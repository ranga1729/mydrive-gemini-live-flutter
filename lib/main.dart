/// MyDrive — Flutter Voice + Text Client for Gemini Live FastAPI Backend
///
/// ── pubspec.yaml dependencies ────────────────────────────────────────────────
/// dependencies:
///   flutter:
///     sdk: flutter
///   record: ^6.2.0
///   flutter_pcm_sound: ^3.3.3
///   web_socket_channel: ^3.0.1
///   permission_handler: ^12.0.1
///
/// ── Android — AndroidManifest.xml ────────────────────────────────────────────
///   <uses-permission android:name="android.permission.RECORD_AUDIO"/>
///   <uses-permission android:name="android.permission.INTERNET"/>
///
/// ── iOS — Info.plist ──────────────────────────────────────────────────────────
///   <key>NSMicrophoneUsageDescription</key>
///   <string>MyDrive needs the microphone to send voice messages.</string>
///
/// ══════════════════════════════════════════════════════════════════════════════
/// ROOT CAUSE ANALYSIS (from screenshot evidence)
/// ══════════════════════════════════════════════════════════════════════════════
///
/// BUG 1 — CRITICAL: Shared _onFrame for both WebSocket channels
///   Both /ws/chat (voice) and /ws/text (text) fed all frames into a single
///   _onFrame() handler that mutated the same shared state (_streamingBubbleIdx,
///   _streamingText, _streamer, _status, watchdog).
///   A stale "ready" or "processing" frame from the idle text channel could
///   arrive while voice was mid-turn and silently corrupt streaming state.
///   Audio binary frames from voice cancelled the shared watchdog, which was
///   also protecting the text channel.
///   FIX: Split into _onVoiceFrame() and _onTextFrame(). Each channel has its
///        own streaming state (_voiceStreamingIdx / _textStreamingIdx).
///        Binary PCM is only accepted from the voice channel.
///
/// BUG 2 — Watchdog started twice per voice turn
///   _stopRecording() called _startWatchdog(). Then the backend echoed
///   {"status":"processing"} which hit _onFrame → _startWatchdog() again,
///   resetting the 30 s clock and masking the real failure.
///   FIX: Watchdog is started exactly ONCE per turn — in _stopRecording() or
///        _sendText(). The backend echo of "processing" only updates the UI
///        label; it never touches the watchdog.
///
/// BUG 3 — Fragile 44-byte WAV header strip
///   The `record` package (pcm16bits) writes a standard WAV container, but
///   optional metadata chunks (LIST, INFO, fact …) can push the "data" payload
///   well beyond byte 44. Sending the wrong bytes to Gemini produces garbage
///   transcription ("He" instead of "Hello") and the backend returns nothing.
///   FIX: _extractPcmFromWav() walks the RIFF sub-chunk list to locate the
///        "data" tag and returns exactly those bytes. Falls back to raw
///        passthrough if the file is not a WAV.
///
/// BUG 4 — Watchdog fired while audio was actively playing
///   The watchdog started at send-time was never extended by incoming audio
///   chunks. A backend response with a long audio payload could be cut off
///   by the 30 s wall mid-playback.
///   FIX: First binary audio chunk from voice channel cancels the watchdog.
///        The streamer being alive is proof the backend responded.

library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:flutter/material.dart';
import 'package:flutter_pcm_sound/flutter_pcm_sound.dart';
import 'package:permission_handler/permission_handler.dart';
import 'package:record/record.dart';
import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

// ════════════════════════════════════════════════════════════════════════════════
// Configuration
// ════════════════════════════════════════════════════════════════════════════════

const String _kBaseHost =
    'mydrive-gemini-live-cueufbg0avdtg3de.canadacentral-01.azurewebsites.net';
const String _kVoiceUrl = 'wss://$_kBaseHost/ws/chat';
const String _kTextUrl  = 'wss://$_kBaseHost/ws/text';

const int _kOutputSampleRate = 24000;
const int _kInputSampleRate  = 16000;

const Duration _kWatchdogTimeout = Duration(seconds: 30);

/// ~100 ms of 16-bit 16 kHz mono audio.
const int _kMinRecordingBytes = 3200;

// ════════════════════════════════════════════════════════════════════════════════
// Entry point
// ════════════════════════════════════════════════════════════════════════════════

void main() => runApp(const MyDriveApp());

class MyDriveApp extends StatelessWidget {
  const MyDriveApp({super.key});

  @override
  Widget build(BuildContext context) => MaterialApp(
    title: 'MyDrive Assistant',
    debugShowCheckedModeBanner: false,
    theme: _theme(),
    home: const ChatPage(),
  );

  ThemeData _theme() => ThemeData(
    useMaterial3: true,
    brightness: Brightness.dark,
    colorScheme: ColorScheme.fromSeed(
      seedColor: const Color(0xFF6C63FF),
      brightness: Brightness.dark,
    ).copyWith(
      surface: const Color(0xFF0A0A0F),
      surfaceContainerHigh: const Color(0xFF16161F),
      surfaceContainerHighest: const Color(0xFF1E1B2E),
    ),
  );
}

// ════════════════════════════════════════════════════════════════════════════════
// Domain models
// ════════════════════════════════════════════════════════════════════════════════

enum _Role  { user, assistant }
enum _Input { voice, text }

sealed class ChatItem {
  final DateTime time;
  const ChatItem({required this.time});
}

final class BubbleItem extends ChatItem {
  final _Role   role;
  final _Input? input;
  final String  text;
  const BubbleItem({
    required super.time,
    required this.role,
    required this.text,
    this.input,
  });
  BubbleItem copyWith({String? text}) => BubbleItem(
    time: time, role: role, input: input, text: text ?? this.text,
  );
}

final class ToolItem extends ChatItem {
  final String               tool;
  final Map<String, dynamic> args;
  final Map<String, dynamic> result;
  const ToolItem({
    required super.time,
    required this.tool,
    required this.args,
    required this.result,
  });
}

enum _Status { idle, micOpening, listening, processing, speaking, error }

// ════════════════════════════════════════════════════════════════════════════════
// _AudioStreamer — flutter_pcm_sound v3.3.3 "One-Pedal Driving"
//
// Confirmed v3.3.3 API:
//   FlutterPcmSound.setup(sampleRate, channelCount)
//   FlutterPcmSound.setFeedThreshold(numSamples)
//   FlutterPcmSound.setFeedCallback(fn)    ← static method, NOT a setter
//   FlutterPcmSound.start()               ← convenience; triggers onFeed(0)
//   FlutterPcmSound.feed(PcmArrayInt16)   ← playing = feeding
//   FlutterPcmSound.release()             ← call only in dispose()
//
// PLAY = keep calling feed(). STOP = stop calling feed().
// No play() / stop() methods exist on this class.
// ════════════════════════════════════════════════════════════════════════════════

class _AudioStreamer {
  _AudioStreamer({
    required this.onPlaybackStarted,
    required this.onPlaybackStopped,
  });

  final VoidCallback onPlaybackStarted;
  final VoidCallback onPlaybackStopped;

  final List<Uint8List> _pending = [];
  bool _isSetup   = false;
  bool _isPlaying = false;
  bool _stopped   = false;
  bool turnDone   = false;

  bool get isPlaying => _isPlaying;

  Future<void> setup() async {
    if (_isSetup) return;
    try {
      await FlutterPcmSound.setup(
        sampleRate: _kOutputSampleRate, channelCount: 1,
      );
      await FlutterPcmSound.setFeedThreshold(_kOutputSampleRate ~/ 4);
      FlutterPcmSound.setFeedCallback(_onFeedNeeded);
      _isSetup = true;
    } catch (e) {
      debugPrint('[_AudioStreamer.setup] $e');
    }
  }

  void pushChunk(Uint8List pcmBytes) {
    if (_stopped) return;
    _pending.add(pcmBytes);
    if (!_isPlaying) _startPlaying();
  }

  Future<void> _startPlaying() async {
    if (_isPlaying || _stopped) return;
    await setup();
    _isPlaying = true;
    onPlaybackStarted();
    try {
      FlutterPcmSound.start();
    } catch (e) {
      debugPrint('[_AudioStreamer._startPlaying] $e');
    }
  }

  void _onFeedNeeded(int remainingFrames) {
    if (_stopped) {
      if (_isPlaying) { _isPlaying = false; onPlaybackStopped(); }
      return;
    }
    if (remainingFrames == 0 && _pending.isEmpty && turnDone) {
      _isPlaying = false;
      turnDone   = false;
      onPlaybackStopped();
      return;
    }
    _feedNext();
  }

  void _feedNext() {
    if (_stopped || _pending.isEmpty) return;
    final chunk = _pending.removeAt(0);
    try {
      final bd      = ByteData.sublistView(chunk);
      final samples = List<int>.generate(
        chunk.lengthInBytes ~/ 2,
            (i) => bd.getInt16(i * 2, Endian.little),
      );
      FlutterPcmSound.feed(PcmArrayInt16.fromList(samples));
    } catch (e) {
      debugPrint('[_AudioStreamer._feedNext] $e');
    }
  }

  void stopImmediately() {
    _pending.clear();
    turnDone = false;
    _stopped = true;
    if (_isPlaying) { _isPlaying = false; onPlaybackStopped(); }
  }

  void prepareForNextTurn() {
    _stopped  = false;
    _pending.clear();
    turnDone  = false;
  }

  Future<void> dispose() async {
    _pending.clear();
    _stopped = true;
    _isPlaying = false;
    try { await FlutterPcmSound.release(); } catch (_) {}
  }
}

// ════════════════════════════════════════════════════════════════════════════════
// WAV → raw PCM extractor
//
// The standard WAV container wraps raw PCM in RIFF chunks. The audio payload
// lives in the "data" sub-chunk which is NOT always at byte offset 44 — optional
// chunks (LIST, INFO, fact, etc.) can appear before it.
// We walk sub-chunks to find "data" and return exactly those bytes.
// ════════════════════════════════════════════════════════════════════════════════

Uint8List _extractPcmFromWav(Uint8List bytes) {
  if (bytes.length < 20) return bytes;
  // Check for "RIFF" magic.
  if (bytes[0] != 0x52 || bytes[1] != 0x49 ||
      bytes[2] != 0x46 || bytes[3] != 0x46) {
    return bytes; // not a WAV — assume raw PCM already
  }
  final bd     = ByteData.sublistView(bytes);
  int   offset = 12; // skip "RIFF????WAVE"
  while (offset + 8 <= bytes.length) {
    final id   = String.fromCharCodes(bytes.sublist(offset, offset + 4));
    final size = bd.getUint32(offset + 4, Endian.little);
    if (id == 'data') {
      final start = offset + 8;
      final end   = (start + size).clamp(0, bytes.length);
      return bytes.sublist(start, end);
    }
    offset += 8 + size + (size & 1); // sub-chunks are word-aligned
  }
  // "data" not found — last-resort fallback.
  return bytes.length > 44 ? bytes.sublist(44) : bytes;
}

// ════════════════════════════════════════════════════════════════════════════════
// Chat page
// ════════════════════════════════════════════════════════════════════════════════

class ChatPage extends StatefulWidget {
  const ChatPage({super.key});
  @override
  State<ChatPage> createState() => _ChatPageState();
}

class _ChatPageState extends State<ChatPage> with TickerProviderStateMixin {

  // ── Voice WebSocket (/ws/chat) ──────────────────────────────────────────────
  WebSocketChannel?            _voiceCh;
  StreamSubscription<dynamic>? _voiceSub;
  int _voiceBackoffSec = 1;

  // ── Text WebSocket (/ws/text) ───────────────────────────────────────────────
  WebSocketChannel?            _textCh;
  StreamSubscription<dynamic>? _textSub;
  int _textBackoffSec = 1;

  // ── Voice-channel streaming state ───────────────────────────────────────────
  int?   _voiceStreamingIdx;
  String _voiceStreamingText = '';
  int?   _voicePlaceholderIdx;

  // ── Text-channel streaming state ────────────────────────────────────────────
  int?   _textStreamingIdx;
  String _textStreamingText = '';

  // ── Tracks which channel owns the current audio turn ─────────────────────────
  // 'voice' | 'text' | null
  String? _activeTurnChannel;

  // ── Tool cards — buffered during a turn, shown after audio finishes ──────────
  final List<ToolItem> _pendingTools = [];

  // ── Recording ────────────────────────────────────────────────────────────────
  final AudioRecorder _recorder    = AudioRecorder();
  bool                _isRecording = false;

  // ── Playback — shared by both channels ───────────────────────────────────────
  late final _AudioStreamer _streamer = _AudioStreamer(
    onPlaybackStarted: () => _setStatus(_Status.speaking),
    onPlaybackStopped: _onTurnFinished,
  );

  // ── Watchdog ─────────────────────────────────────────────────────────────────
  Timer? _watchdog;

  // ── UI state ─────────────────────────────────────────────────────────────────
  bool                _isTextMode = false;
  _Status             _status     = _Status.idle;
  final List<ChatItem>   _items   = [];
  final ScrollController _scroll  = ScrollController();

  final TextEditingController _textCtrl  = TextEditingController();
  final FocusNode             _textFocus = FocusNode();

  late final AnimationController _pulseCtrl = AnimationController(
    vsync: this, duration: const Duration(milliseconds: 850),
  )..repeat(reverse: true);

  late final Animation<double> _pulseAnim =
  Tween<double>(begin: 1.0, end: 1.20).animate(
    CurvedAnimation(parent: _pulseCtrl, curve: Curves.easeInOut),
  );

  late final AnimationController _dotsCtrl = AnimationController(
    vsync: this, duration: const Duration(milliseconds: 1200),
  )..repeat();

  // ── Lifecycle ─────────────────────────────────────────────────────────────────

  @override
  void initState() {
    super.initState();
    _streamer.setup();
    _connectVoice();
    _connectText();
  }

  @override
  void dispose() {
    _watchdog?.cancel();
    _voiceSub?.cancel();
    _voiceCh?.sink.close();
    _textSub?.cancel();
    _textCh?.sink.close();
    _recorder.dispose();
    _streamer.dispose();
    _pulseCtrl.dispose();
    _dotsCtrl.dispose();
    _scroll.dispose();
    _textCtrl.dispose();
    _textFocus.dispose();
    super.dispose();
  }

  // ════════════════════════════════════════════════════════════════════════════
  // WebSocket connections
  // ════════════════════════════════════════════════════════════════════════════

  void _connectVoice() {
    try {
      _voiceSub?.cancel();
      _voiceCh = IOWebSocketChannel.connect(Uri.parse(_kVoiceUrl));
      _voiceSub = _voiceCh!.stream.listen(
        _onVoiceFrame,
        onError: (_) => _scheduleVoiceReconnect(),
        onDone:  ()  => _scheduleVoiceReconnect(),
      );
    } catch (_) {
      _scheduleVoiceReconnect();
    }
  }

  void _scheduleVoiceReconnect() {
    final d = _voiceBackoffSec;
    _voiceBackoffSec = (_voiceBackoffSec * 2).clamp(1, 16);
    Future.delayed(Duration(seconds: d), _connectVoice);
  }

  void _connectText() {
    try {
      _textSub?.cancel();
      _textCh = IOWebSocketChannel.connect(Uri.parse(_kTextUrl));
      _textSub = _textCh!.stream.listen(
        _onTextFrame,
        onError: (_) => _scheduleTextReconnect(),
        onDone:  ()  => _scheduleTextReconnect(),
      );
    } catch (_) {
      _scheduleTextReconnect();
    }
  }

  void _scheduleTextReconnect() {
    final d = _textBackoffSec;
    _textBackoffSec = (_textBackoffSec * 2).clamp(1, 16);
    Future.delayed(Duration(seconds: d), _connectText);
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Watchdog — started exactly ONCE per turn, from the send site
  // ════════════════════════════════════════════════════════════════════════════

  void _startWatchdog() {
    _watchdog?.cancel();
    _watchdog = Timer(_kWatchdogTimeout, () {
      if (_status == _Status.processing || _status == _Status.listening ||
          _status == _Status.micOpening) {
        debugPrint('[watchdog] timeout');
        _streamer.stopImmediately();
        _streamer.prepareForNextTurn();
        _voiceStreamingIdx  = null;
        _voiceStreamingText = '';
        _textStreamingIdx   = null;
        _textStreamingText  = '';
        _setStatus(_Status.idle);
        _addItem(BubbleItem(
          time: DateTime.now(),
          role: _Role.assistant,
          text: '⚠️ Response timed out. Please try again.',
        ));
      }
    });
  }

  void _cancelWatchdog() {
    _watchdog?.cancel();
    _watchdog = null;
  }

  // ════════════════════════════════════════════════════════════════════════════
  // VOICE channel frame handler  (/ws/chat)
  //
  // Receives:
  //   • List<int>                        — raw 24 kHz PCM audio
  //   • {"status": "ready|processing|done|interrupted|error"}
  //   • {"type": "gemini_transcript", "text": "..."}
  //   • {"type": "user_transcript",   "text": "..."}
  //   • {"type": "tool_call", "tool": "...", "args": {...}, "result": {...}}
  // ════════════════════════════════════════════════════════════════════════════

  void _onVoiceFrame(dynamic raw) {
    // ── Binary = raw PCM audio from Gemini ────────────────────────────────────
    if (raw is List<int>) {
      _cancelWatchdog(); // first audio = backend is alive, watchdog not needed
      _streamer.pushChunk(Uint8List.fromList(raw));
      return;
    }
    if (raw is! String) return;

    late final Map<String, dynamic> msg;
    try { msg = jsonDecode(raw) as Map<String, dynamic>; } catch (_) { return; }

    final type = msg['type'] as String?;
    if (type != null) {
      switch (type) {
        case 'gemini_transcript':
          final chunk = (msg['text'] as String?) ?? '';
          if (chunk.isNotEmpty) _appendVoiceTranscript(chunk);
        case 'user_transcript':
          final t = (msg['text'] as String?) ?? '';
          if (t.isNotEmpty) _resolveVoicePlaceholder(t);
        case 'tool_call':
          _pendingTools.add(ToolItem(
            time:   DateTime.now(),
            tool:   (msg['tool']   as String?) ?? '',
            args:   Map<String, dynamic>.from((msg['args']   as Map?) ?? {}),
            result: Map<String, dynamic>.from((msg['result'] as Map?) ?? {}),
          ));
      }
      return;
    }

    switch (msg['status'] as String?) {
      case 'ready':
        _voiceBackoffSec = 1;
        if (_status == _Status.error) _setStatus(_Status.idle);

      case 'processing':
      // Backend echo — only update the UI label.
      // Watchdog was already started in _stopRecording(); do NOT restart it.
        if (_status != _Status.processing) _setStatus(_Status.processing);

      case 'done':
        _cancelWatchdog();
        _streamer.turnDone = true;
        if (!_streamer.isPlaying) {
          _streamer.turnDone = false;
          _onVoiceTurnFinished();
        }

      case 'interrupted':
        _cancelWatchdog();
        _pendingTools.clear();
        _streamer.stopImmediately();
        _streamer.prepareForNextTurn();
        _voiceStreamingIdx  = null;
        _voiceStreamingText = '';
        _setStatus(_Status.idle);

      case 'error':
        _cancelWatchdog();
        _setStatus(_Status.error);
        _addItem(BubbleItem(
          time: DateTime.now(), role: _Role.assistant,
          text: '❌ ${(msg['message'] as String?) ?? 'Unknown error'}',
        ));
    }
  }

  // ════════════════════════════════════════════════════════════════════════════
  // TEXT channel frame handler  (/ws/text)
  //
  // Receives:
  //   • {"status": "ready|processing|done|interrupted|error"}
  //   • {"type": "gemini_transcript", "text": "..."}
  //   • {"type": "tool_call", ...}
  //   NOTE: text channel never sends binary or user_transcript.
  // ════════════════════════════════════════════════════════════════════════════

  void _onTextFrame(dynamic raw) {
    // ── Binary = raw 24 kHz PCM audio from the text session ──────────────────
    // /ws/text uses response_modalities=["AUDIO"] and streams binary PCM
    // frames identically to /ws/chat. Feed them into the shared streamer.
    if (raw is List<int>) {
      debugPrint('[text] binary audio chunk: ${raw.length} bytes');
      _cancelWatchdog(); // first audio = backend alive
      final bytes = raw is Uint8List ? raw : Uint8List.fromList(raw);
      _streamer.pushChunk(bytes);
      return;
    }

    if (raw is! String) return;

    late final Map<String, dynamic> msg;
    try { msg = jsonDecode(raw) as Map<String, dynamic>; } catch (_) { return; }

    final type = msg['type'] as String?;
    if (type != null) {
      switch (type) {
        case 'gemini_transcript':
          final chunk = (msg['text'] as String?) ?? '';
          if (chunk.isNotEmpty) _appendTextTranscript(chunk);
        case 'tool_call':
          _pendingTools.add(ToolItem(
            time:   DateTime.now(),
            tool:   (msg['tool']   as String?) ?? '',
            args:   Map<String, dynamic>.from((msg['args']   as Map?) ?? {}),
            result: Map<String, dynamic>.from((msg['result'] as Map?) ?? {}),
          ));
        default:
          break;
      }
      return;
    }

    switch (msg['status'] as String?) {
      case 'ready':
        _textBackoffSec = 1;
        if (_status == _Status.error) _setStatus(_Status.idle);

      case 'processing':
      // Backend echo — only update label. Watchdog started in _sendText().
        if (_status != _Status.processing) _setStatus(_Status.processing);

      case 'done':
        _cancelWatchdog();
        debugPrint('[text] done received — isPlaying=${_streamer.isPlaying}');
        _streamer.turnDone = true;
        if (!_streamer.isPlaying) {
          // No audio chunks arrived (text-only response or all arrived before
          // _startPlaying was called). Finish the turn immediately.
          _streamer.turnDone = false;
          _onTextTurnFinished();
        }
    // If isPlaying==true, the streamer drains naturally and fires
    // _onTurnFinished() → _onTextTurnFinished() via _activeTurnChannel.

      case 'interrupted':
        _cancelWatchdog();
        _pendingTools.clear();
        _streamer.stopImmediately();
        _streamer.prepareForNextTurn();
        _textStreamingIdx  = null;
        _textStreamingText = '';
        _setStatus(_Status.idle);

      case 'error':
        _cancelWatchdog();
        _setStatus(_Status.error);
        _addItem(BubbleItem(
          time: DateTime.now(), role: _Role.assistant,
          text: '❌ ${(msg['message'] as String?) ?? 'Unknown error'}',
        ));
    }
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Text turn finished (mirrors _onVoiceTurnFinished, called by streamer)
  // ════════════════════════════════════════════════════════════════════════════

  void _onTextTurnFinished() {
    if (!mounted) return;
    _cancelWatchdog();
    _streamer.prepareForNextTurn();
    if (_pendingTools.isNotEmpty) {
      setState(() { _items.addAll(_pendingTools); _pendingTools.clear(); });
      _scrollToBottom();
    }
    _textStreamingIdx  = null;
    _textStreamingText = '';
    _setStatus(_Status.idle);
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Transcript streaming helpers
  // ════════════════════════════════════════════════════════════════════════════

  void _appendVoiceTranscript(String chunk) {
    _voiceStreamingText += chunk;
    if (!mounted) return;
    setState(() {
      if (_voiceStreamingIdx == null) {
        _voiceStreamingIdx = _items.length;
        _items.add(BubbleItem(
          time: DateTime.now(), role: _Role.assistant, text: _voiceStreamingText,
        ));
      } else {
        final idx = _voiceStreamingIdx!;
        if (idx < _items.length && _items[idx] is BubbleItem) {
          _items[idx] = (_items[idx] as BubbleItem).copyWith(text: _voiceStreamingText);
        }
      }
    });
    _scrollToBottom();
  }

  void _appendTextTranscript(String chunk) {
    _textStreamingText += chunk;
    if (!mounted) return;
    setState(() {
      if (_textStreamingIdx == null) {
        _textStreamingIdx = _items.length;
        _items.add(BubbleItem(
          time: DateTime.now(), role: _Role.assistant, text: _textStreamingText,
        ));
      } else {
        final idx = _textStreamingIdx!;
        if (idx < _items.length && _items[idx] is BubbleItem) {
          _items[idx] = (_items[idx] as BubbleItem).copyWith(text: _textStreamingText);
        }
      }
    });
    _scrollToBottom();
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Voice placeholder
  // ════════════════════════════════════════════════════════════════════════════

  void _addVoicePlaceholder() {
    if (!mounted) return;
    setState(() {
      _voicePlaceholderIdx = _items.length;
      _items.add(BubbleItem(
        time: DateTime.now(), role: _Role.user,
        input: _Input.voice, text: '🎙️ …',
      ));
    });
    _scrollToBottom();
  }

  void _resolveVoicePlaceholder(String transcript) {
    if (_voicePlaceholderIdx == null) return;
    final idx = _voicePlaceholderIdx!;
    _voicePlaceholderIdx = null;
    if (!mounted || idx >= _items.length) return;
    setState(() {
      _items[idx] = (_items[idx] as BubbleItem).copyWith(text: '"$transcript"');
    });
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Turn finished dispatcher — called by _AudioStreamer.onPlaybackStopped
  // Routes to the correct channel's cleanup based on who owns the turn.
  // ════════════════════════════════════════════════════════════════════════════

  void _onTurnFinished() {
    if (_activeTurnChannel == 'text') {
      _onTextTurnFinished();
    } else {
      _onVoiceTurnFinished();
    }
    _activeTurnChannel = null;
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Voice turn finished
  // ════════════════════════════════════════════════════════════════════════════

  void _onVoiceTurnFinished() {
    if (!mounted) return;
    _cancelWatchdog();
    _streamer.prepareForNextTurn();
    if (_pendingTools.isNotEmpty) {
      setState(() { _items.addAll(_pendingTools); _pendingTools.clear(); });
      _scrollToBottom();
    }
    _voiceStreamingIdx  = null;
    _voiceStreamingText = '';
    _setStatus(_Status.idle);
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Voice recording
  // ════════════════════════════════════════════════════════════════════════════

  Future<void> _startRecording() async {
    if (_isBusy || _isRecording) return;

    final perm = await Permission.microphone.request();
    if (!perm.isGranted) {
      _addItem(BubbleItem(
        time: DateTime.now(), role: _Role.assistant,
        text: '⚠️ Microphone permission denied.',
      ));
      return;
    }

    // Show a "mic opening" state immediately so the user knows to wait a beat
    // before speaking. The OS audio session takes ~200–500 ms to open on
    // Android/iOS. If the user speaks before start() resolves, that audio is
    // lost. We set _isRecording = false here and only flip it to true AFTER
    // start() fully completes, so _stopRecording() is a no-op if the user
    // releases the button before the session is ready.
    if (mounted) setState(() { _status = _Status.micOpening; });

    final path =
        '${Directory.systemTemp.path}/mydrive_${DateTime.now().millisecondsSinceEpoch}.wav';
    try {
      await _recorder.start(
        const RecordConfig(
          encoder:     AudioEncoder.pcm16bits,
          sampleRate:  _kInputSampleRate,
          numChannels: 1,
        ),
        path: path,
      );
      // Audio session is now confirmed open. ONLY NOW set the listening state.
      if (mounted) setState(() { _isRecording = true; _status = _Status.listening; });
    } catch (e) {
      debugPrint('[_startRecording] $e');
      if (mounted) _setStatus(_Status.idle);
      _addItem(BubbleItem(
        time: DateTime.now(), role: _Role.assistant,
        text: '⚠️ Could not start recording: $e',
      ));
    }
  }

  Future<void> _stopRecording() async {
    if (!_isRecording) return;

    String? path;
    try { path = await _recorder.stop(); }
    catch (e) { debugPrint('[_stopRecording] stop error: $e'); }

    if (mounted) setState(() => _isRecording = false);

    if (path == null || _voiceCh == null) { _setStatus(_Status.idle); return; }

    final file = File(path);
    if (!await file.exists())             { _setStatus(_Status.idle); return; }

    // On Android/iOS the audio session may not have fully flushed the WAV to
    // disk by the time recorder.stop() returns — especially on short recordings.
    // Poll file size for up to 500 ms until it stabilises.
    int prevSize = -1;
    for (int i = 0; i < 10; i++) {
      final size = await file.length().catchError((_) => 0);
      if (size > 0 && size == prevSize) break;
      prevSize = size;
      await Future.delayed(const Duration(milliseconds: 50));
    }

    final rawBytes = await file.readAsBytes();
    file.delete().ignore();

    if (rawBytes.isEmpty) { _setStatus(_Status.idle); return; }

    // Properly walk RIFF sub-chunks to extract the raw PCM payload.
    final Uint8List pcmBytes = _extractPcmFromWav(rawBytes);

    if (pcmBytes.length < _kMinRecordingBytes) {
      debugPrint('[_stopRecording] too short (${pcmBytes.length} B) — skip');
      _setStatus(_Status.idle);
      return;
    }

    _addVoicePlaceholder();
    _voiceStreamingIdx  = null;
    _voiceStreamingText = '';

    try {
      _voiceCh!.sink.add(pcmBytes);
      _voiceCh!.sink.add('END_OF_SPEECH');
      _activeTurnChannel = 'voice';
      _setStatus(_Status.processing);
      _startWatchdog(); // started exactly ONCE here
    } catch (e) {
      debugPrint('[_stopRecording] sink error: $e');
      _setStatus(_Status.error);
    }
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Text sending
  // ════════════════════════════════════════════════════════════════════════════

  void _sendText() {
    final text = _textCtrl.text.trim();
    if (text.isEmpty) return;

    if (_textCh == null) {
      _addItem(BubbleItem(
        time: DateTime.now(), role: _Role.assistant,
        text: '⚠️ Text channel not connected.',
      ));
      return;
    }

    _textCtrl.clear();
    _textFocus.unfocus();

    _addItem(BubbleItem(
      time: DateTime.now(), role: _Role.user, input: _Input.text, text: text,
    ));
    _textStreamingIdx  = null;
    _textStreamingText = '';

    try {
      _textCh!.sink.add(jsonEncode({'type': 'message', 'text': text}));
      _activeTurnChannel = 'text';
      _setStatus(_Status.processing);
      _startWatchdog(); // started exactly ONCE here
    } catch (e) {
      debugPrint('[_sendText] sink error: $e');
      _setStatus(_Status.error);
    }
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Helpers
  // ════════════════════════════════════════════════════════════════════════════

  bool get _isBusy =>
      _status == _Status.processing ||
          _status == _Status.speaking   ||
          _status == _Status.micOpening;

  void _setStatus(_Status s) {
    if (mounted) setState(() => _status = s);
  }

  void _addItem(ChatItem item) {
    if (!mounted) return;
    setState(() => _items.add(item));
    _scrollToBottom();
  }

  void _scrollToBottom() {
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (_scroll.hasClients && _scroll.positions.isNotEmpty) {
        _scroll.animateTo(
          _scroll.position.maxScrollExtent,
          duration: const Duration(milliseconds: 280),
          curve: Curves.easeOut,
        );
      }
    });
  }

  String get _statusLabel => switch (_status) {
    _Status.idle       => 'Ready',
    _Status.micOpening => 'Opening mic…',
    _Status.listening  => 'Listening…',
    _Status.processing => 'Thinking…',
    _Status.speaking   => 'Speaking…',
    _Status.error      => 'Connection error',
  };

  Color get _statusColor => switch (_status) {
    _Status.micOpening => const Color(0xFF00B4D8),
    _Status.listening  => const Color(0xFF00E5A0),
    _Status.processing => const Color(0xFFFFB830),
    _Status.speaking   => const Color(0xFF6C63FF),
    _Status.error      => const Color(0xFFFF4D6D),
    _Status.idle       => const Color(0xFF5A5A72),
  };

  // ════════════════════════════════════════════════════════════════════════════
  // Build
  // ════════════════════════════════════════════════════════════════════════════

  @override
  Widget build(BuildContext context) {
    final cs = Theme.of(context).colorScheme;
    return Scaffold(
      backgroundColor: cs.surface,
      resizeToAvoidBottomInset: true,
      appBar: _AppBar(
        statusLabel:  _statusLabel,
        statusColor:  _statusColor,
        isTextMode:   _isTextMode,
        onModeToggle: (v) => setState(() {
          _isTextMode = v;
          if (v) {
            Future.delayed(const Duration(milliseconds: 80), _textFocus.requestFocus);
          } else {
            _textFocus.unfocus();
          }
        }),
      ),
      body: Column(
        children: [
          Expanded(child: _buildList(cs)),
          _StatusDots(status: _status, controller: _dotsCtrl),
          _isTextMode ? _buildTextInput(cs) : _buildVoiceInput(cs),
        ],
      ),
    );
  }

  Widget _buildList(ColorScheme cs) {
    if (_items.isEmpty) {
      return Center(
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(Icons.record_voice_over_rounded,
                size: 56, color: cs.onSurface.withValues(alpha: 0.08)),
            const SizedBox(height: 14),
            Text(
              _isTextMode
                  ? 'Type a message below to start'
                  : 'Hold the mic to speak',
              style: TextStyle(
                  color: cs.onSurface.withValues(alpha: 0.25), fontSize: 14),
            ),
          ],
        ),
      );
    }
    return ListView.builder(
      controller: _scroll,
      padding: const EdgeInsets.fromLTRB(12, 16, 12, 8),
      itemCount: _items.length,
      itemBuilder: (_, i) => switch (_items[i]) {
        BubbleItem b => _BubbleWidget(bubble: b),
        ToolItem   t => _ToolCardWidget(item: t),
      },
    );
  }

  Widget _buildVoiceInput(ColorScheme cs) => Container(
    padding: EdgeInsets.only(
      top: 20,
      bottom: MediaQuery.of(context).padding.bottom + 24,
    ),
    decoration: BoxDecoration(
      color: const Color(0xFF111118),
      border: Border(
          top: BorderSide(color: cs.outlineVariant.withValues(alpha: 0.3))),
    ),
    child: Column(
      mainAxisSize: MainAxisSize.min,
      children: [
        GestureDetector(
          onLongPressStart: _isBusy ? null : (_) => _startRecording(),
          onLongPressEnd:   _isBusy ? null : (_) => _stopRecording(),
          child: AnimatedBuilder(
            animation: _pulseAnim,
            builder: (_, child) => Transform.scale(
              scale: _isRecording ? _pulseAnim.value : 1.0,
              child: child,
            ),
            child: AnimatedContainer(
              duration: const Duration(milliseconds: 200),
              width: 72, height: 72,
              decoration: BoxDecoration(
                shape: BoxShape.circle,
                color: _isBusy
                    ? const Color(0xFF2A2A38)
                    : _status == _Status.micOpening
                    ? const Color(0xFF00B4D8).withValues(alpha: 0.5)
                    : _isRecording
                    ? const Color(0xFFFF4D6D)
                    : const Color(0xFF6C63FF),
                boxShadow: _isBusy ? [] : [
                  BoxShadow(
                    color: (_status == _Status.micOpening
                        ? const Color(0xFF00B4D8)
                        : _isRecording
                        ? const Color(0xFFFF4D6D)
                        : const Color(0xFF6C63FF)).withValues(alpha: 0.48),
                    blurRadius: 24, spreadRadius: 3,
                  ),
                ],
              ),
              child: Icon(
                _isBusy
                    ? Icons.hourglass_empty_rounded
                    : _status == _Status.micOpening
                    ? Icons.mic_none_rounded   // hollow mic = not yet open
                    : _isRecording
                    ? Icons.stop_rounded
                    : Icons.mic_rounded,
                color: _isBusy ? const Color(0xFF5A5A72) : Colors.white,
                size: 32,
              ),
            ),
          ),
        ),
        const SizedBox(height: 10),
        Text(
          _isBusy
              ? _statusLabel
              : _status == _Status.micOpening
              ? 'Opening mic…'
              : _isRecording ? 'Release to send' : 'Hold to speak',
          style: TextStyle(
              color: cs.onSurface.withValues(alpha: 0.38), fontSize: 13),
        ),
      ],
    ),
  );

  Widget _buildTextInput(ColorScheme cs) {
    final canSend = !_isBusy && !_isRecording;
    return Container(
      padding: EdgeInsets.fromLTRB(
        12, 10, 12,
        MediaQuery.of(context).viewInsets.bottom +
            MediaQuery.of(context).padding.bottom + 10,
      ),
      decoration: BoxDecoration(
        color: const Color(0xFF111118),
        border: Border(
            top: BorderSide(color: cs.outlineVariant.withValues(alpha: 0.3))),
      ),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.end,
        children: [
          Expanded(
            child: TextField(
              controller:      _textCtrl,
              focusNode:       _textFocus,
              minLines: 1,     maxLines: 5,
              keyboardType:    TextInputType.multiline,
              textInputAction: TextInputAction.newline,
              style: const TextStyle(fontSize: 14.5, color: Colors.white),
              decoration: InputDecoration(
                hintText:  'Type a message…',
                hintStyle: TextStyle(color: cs.onSurface.withValues(alpha: 0.35)),
                filled:    true,
                fillColor: const Color(0xFF16161F),
                contentPadding: const EdgeInsets.symmetric(horizontal: 16, vertical: 11),
                border: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(22),
                  borderSide:   BorderSide.none,
                ),
              ),
              onSubmitted: (_) { if (canSend) _sendText(); },
            ),
          ),
          const SizedBox(width: 8),
          AnimatedOpacity(
            opacity:  canSend ? 1.0 : 0.35,
            duration: const Duration(milliseconds: 200),
            child: GestureDetector(
              onTap: canSend ? _sendText : null,
              child: Container(
                width: 46, height: 46,
                decoration: BoxDecoration(
                  shape:     BoxShape.circle,
                  color:     const Color(0xFF6C63FF),
                  boxShadow: canSend ? [
                    BoxShadow(
                      color:        const Color(0xFF6C63FF).withValues(alpha: 0.42),
                      blurRadius:   14, spreadRadius: 2,
                    ),
                  ] : [],
                ),
                child: const Icon(Icons.send_rounded, color: Colors.white, size: 20),
              ),
            ),
          ),
        ],
      ),
    );
  }
}

// ════════════════════════════════════════════════════════════════════════════════
// App bar
// ════════════════════════════════════════════════════════════════════════════════

class _AppBar extends StatelessWidget implements PreferredSizeWidget {
  final String             statusLabel;
  final Color              statusColor;
  final bool               isTextMode;
  final ValueChanged<bool> onModeToggle;
  const _AppBar({
    required this.statusLabel, required this.statusColor,
    required this.isTextMode,  required this.onModeToggle,
  });
  @override
  Size get preferredSize => const Size.fromHeight(kToolbarHeight);

  @override
  Widget build(BuildContext context) => AppBar(
    backgroundColor:  const Color(0xFF111118),
    surfaceTintColor: Colors.transparent,
    title: Row(
      children: [
        Container(
          width: 36, height: 36,
          decoration: BoxDecoration(
            borderRadius: BorderRadius.circular(10),
            gradient: const LinearGradient(
              colors: [Color(0xFF6C63FF), Color(0xFFFF6B9D)],
              begin: Alignment.topLeft, end: Alignment.bottomRight,
            ),
            boxShadow: const [BoxShadow(color: Color(0x666C63FF), blurRadius: 12)],
          ),
          child: const Center(child: Text('🚗', style: TextStyle(fontSize: 18))),
        ),
        const SizedBox(width: 10),
        Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text('MyDrive',
                style: TextStyle(fontSize: 16, fontWeight: FontWeight.w700, color: Colors.white)),
            Row(
              children: [
                Container(
                  width: 6, height: 6,
                  margin: const EdgeInsets.only(right: 5),
                  decoration: BoxDecoration(shape: BoxShape.circle, color: statusColor),
                ),
                Text(statusLabel, style: TextStyle(fontSize: 11, color: statusColor)),
              ],
            ),
          ],
        ),
      ],
    ),
    actions: [
      Padding(
        padding: const EdgeInsets.only(right: 12),
        child: _ModeToggle(isTextMode: isTextMode, onToggle: onModeToggle),
      ),
    ],
  );
}

// ════════════════════════════════════════════════════════════════════════════════
// Status dots
// ════════════════════════════════════════════════════════════════════════════════

class _StatusDots extends StatelessWidget {
  final _Status             status;
  final AnimationController controller;
  const _StatusDots({required this.status, required this.controller});

  @override
  Widget build(BuildContext context) {
    final show = status == _Status.processing || status == _Status.speaking ||
        status == _Status.micOpening;
    return AnimatedSize(
      duration: const Duration(milliseconds: 200),
      child: show
          ? Padding(
        padding: const EdgeInsets.symmetric(vertical: 6),
        child: Row(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            _Dot(index: 0, controller: controller),
            _Dot(index: 1, controller: controller),
            _Dot(index: 2, controller: controller),
            const SizedBox(width: 8),
            Text(
              status == _Status.processing
                  ? 'MyDrive AI is thinking…'
                  : status == _Status.micOpening
                  ? 'Opening microphone…'
                  : 'Speaking…',
              style: const TextStyle(color: Color(0xFF5A5A72), fontSize: 12),
            ),
          ],
        ),
      )
          : const SizedBox.shrink(),
    );
  }
}

class _Dot extends StatelessWidget {
  final int                index;
  final AnimationController controller;
  const _Dot({required this.index, required this.controller});

  @override
  Widget build(BuildContext context) {
    final begin = (index * 0.2).clamp(0.0, 1.0);
    final end   = (begin + 0.4).clamp(0.0, 1.0);
    final anim  = CurvedAnimation(
      parent: controller,
      curve:  Interval(begin, end, curve: Curves.easeInOut),
    );
    return AnimatedBuilder(
      animation: anim,
      builder: (_, __) => Opacity(
        opacity: 0.3 + anim.value * 0.7,
        child: Container(
          margin: const EdgeInsets.symmetric(horizontal: 3),
          width: 6, height: 6,
          decoration: const BoxDecoration(
            color: Color(0xFF6C63FF), shape: BoxShape.circle,
          ),
        ),
      ),
    );
  }
}

// ════════════════════════════════════════════════════════════════════════════════
// Chat bubble
// ════════════════════════════════════════════════════════════════════════════════

class _BubbleWidget extends StatelessWidget {
  final BubbleItem bubble;
  const _BubbleWidget({required this.bubble});

  @override
  Widget build(BuildContext context) {
    final isUser = bubble.role == _Role.user;
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 4),
      child: Align(
        alignment: isUser ? Alignment.centerRight : Alignment.centerLeft,
        child: ConstrainedBox(
          constraints: BoxConstraints(
              maxWidth: MediaQuery.of(context).size.width * 0.76),
          child: Column(
            crossAxisAlignment:
            isUser ? CrossAxisAlignment.end : CrossAxisAlignment.start,
            children: [
              Container(
                padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 10),
                decoration: BoxDecoration(
                  color: isUser
                      ? const Color(0xFF1E1B3A)
                      : const Color(0xFF131320),
                  border: Border.all(
                    color: isUser
                        ? const Color(0xFF6C63FF).withValues(alpha: 0.25)
                        : const Color(0xFF22222E),
                  ),
                  borderRadius: BorderRadius.only(
                    topLeft:     const Radius.circular(16),
                    topRight:    const Radius.circular(16),
                    bottomLeft:  Radius.circular(isUser ? 16 : 3),
                    bottomRight: Radius.circular(isUser ? 3 : 16),
                  ),
                ),
                child: Text(
                  bubble.text,
                  style: TextStyle(
                    fontSize: 14.5, height: 1.5,
                    color: isUser
                        ? Colors.white
                        : Colors.white.withValues(alpha: 0.85),
                    fontStyle: isUser ? FontStyle.normal : FontStyle.italic,
                  ),
                ),
              ),
              const SizedBox(height: 3),
              Row(
                mainAxisSize: MainAxisSize.min,
                children: [
                  Icon(_metaIcon(isUser, bubble.input),
                      size: 10, color: const Color(0xFF5A5A72)),
                  const SizedBox(width: 3),
                  Text(_metaLabel(isUser, bubble.input),
                      style: const TextStyle(
                          fontSize: 10, color: Color(0xFF5A5A72))),
                  Text(_fmtTime(bubble.time),
                      style: const TextStyle(
                          fontSize: 10, color: Color(0xFF5A5A72))),
                ],
              ),
            ],
          ),
        ),
      ),
    );
  }

  IconData _metaIcon(bool isUser, _Input? input) {
    if (!isUser) return Icons.mic_none_rounded;
    return input == _Input.voice ? Icons.mic_rounded : Icons.keyboard_rounded;
  }

  String _metaLabel(bool isUser, _Input? input) {
    if (!isUser) return 'MyDrive AI · ';
    return input == _Input.voice ? 'Voice · ' : 'Text · ';
  }

  String _fmtTime(DateTime t) =>
      '${t.hour.toString().padLeft(2, '0')}:'
          '${t.minute.toString().padLeft(2, '0')}:'
          '${t.second.toString().padLeft(2, '0')}';
}

// ════════════════════════════════════════════════════════════════════════════════
// Tool card
// ════════════════════════════════════════════════════════════════════════════════

class _ToolCardWidget extends StatelessWidget {
  final ToolItem item;
  const _ToolCardWidget({required this.item});

  static const _meta =
  <String, ({String icon, String label, Color color})>{
    'request_roadside_assistance': (
    icon: '🔧', label: 'Roadside Assistance', color: Color(0xFFFFB830),
    ),
    'request_tow_truck': (
    icon: '🚛', label: 'Tow Truck Dispatched', color: Color(0xFFFF4D6D),
    ),
    'search_spare_parts': (
    icon: '🔩', label: 'Spare Parts Search', color: Color(0xFF6C63FF),
    ),
    'book_garage_service': (
    icon: '🏪', label: 'Garage Booking', color: Color(0xFF00E5A0),
    ),
  };

  @override
  Widget build(BuildContext context) {
    final m     = _meta[item.tool];
    final color = m?.color ?? const Color(0xFF5A5A72);
    final icon  = m?.icon  ?? '⚙️';
    final label = m?.label ?? item.tool;

    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 6),
      child: Align(
        alignment: Alignment.centerLeft,
        child: ConstrainedBox(
          constraints: BoxConstraints(
              maxWidth: MediaQuery.of(context).size.width * 0.92),
          child: Container(
            decoration: BoxDecoration(
              color:  const Color(0xFF131320),
              border: Border.all(color: color.withValues(alpha: 0.32)),
              borderRadius: const BorderRadius.only(
                topLeft:     Radius.circular(16),
                topRight:    Radius.circular(16),
                bottomRight: Radius.circular(16),
                bottomLeft:  Radius.circular(3),
              ),
            ),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                _CardHeader(icon: icon, label: label, color: color),
                Padding(
                  padding: const EdgeInsets.all(12),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      if (item.args.isNotEmpty) ...[
                        const _SectionLabel('Parameters'),
                        const SizedBox(height: 5),
                        ...item.args.entries.map(
                              (e) => _KVRow(k: e.key, v: e.value.toString()),
                        ),
                        const SizedBox(height: 10),
                      ],
                      if (item.result.isNotEmpty) ...[
                        const _SectionLabel('Result'),
                        const SizedBox(height: 5),
                        Container(
                          padding: const EdgeInsets.all(10),
                          decoration: BoxDecoration(
                            color:        const Color(0xFF16161F),
                            borderRadius: BorderRadius.circular(8),
                            border: Border.all(color: const Color(0xFF22222E)),
                          ),
                          child: Column(
                            children: item.result.entries
                                .map((e) => _KVRow(
                              k: e.key, v: e.value.toString(), mono: true,
                            ))
                                .toList(),
                          ),
                        ),
                      ],
                    ],
                  ),
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}

class _CardHeader extends StatelessWidget {
  final String icon, label;
  final Color  color;
  const _CardHeader({required this.icon, required this.label, required this.color});

  @override
  Widget build(BuildContext context) => Container(
    padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 9),
    decoration: BoxDecoration(
      color: color.withValues(alpha: 0.07),
      borderRadius: const BorderRadius.only(
        topLeft: Radius.circular(15), topRight: Radius.circular(15),
      ),
      border: Border(bottom: BorderSide(color: color.withValues(alpha: 0.18))),
    ),
    child: Row(
      children: [
        Text(icon, style: const TextStyle(fontSize: 15)),
        const SizedBox(width: 8),
        Expanded(
          child: Text(label,
              style: TextStyle(
                  fontSize: 12, fontWeight: FontWeight.w700,
                  color: color, letterSpacing: 0.3)),
        ),
        Container(
          padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 2),
          decoration: BoxDecoration(
            color:        const Color(0xFF00E5A0).withValues(alpha: 0.10),
            borderRadius: BorderRadius.circular(20),
            border: Border.all(
                color: const Color(0xFF00E5A0).withValues(alpha: 0.28)),
          ),
          child: const Row(
            mainAxisSize: MainAxisSize.min,
            children: [
              _PulseDot(),
              SizedBox(width: 5),
              Text('Triggered',
                  style: TextStyle(
                      fontSize: 10, color: Color(0xFF00E5A0),
                      fontWeight: FontWeight.w600)),
            ],
          ),
        ),
      ],
    ),
  );
}

class _PulseDot extends StatelessWidget {
  const _PulseDot();
  @override
  Widget build(BuildContext context) => TweenAnimationBuilder<double>(
    tween:    Tween(begin: 0.3, end: 1.0),
    duration: const Duration(milliseconds: 700),
    builder:  (_, v, child) => Opacity(opacity: v, child: child),
    child: Container(
      width: 5, height: 5,
      decoration: const BoxDecoration(
          shape: BoxShape.circle, color: Color(0xFF00E5A0)),
    ),
  );
}

class _SectionLabel extends StatelessWidget {
  final String text;
  const _SectionLabel(this.text);
  @override
  Widget build(BuildContext context) => Text(
    text.toUpperCase(),
    style: const TextStyle(
      fontSize: 9.5, color: Color(0xFF5A5A72),
      letterSpacing: 0.8, fontWeight: FontWeight.w600,
    ),
  );
}

class _KVRow extends StatelessWidget {
  final String k, v;
  final bool   mono;
  const _KVRow({required this.k, required this.v, this.mono = false});

  @override
  Widget build(BuildContext context) => Padding(
    padding: const EdgeInsets.symmetric(vertical: 2),
    child: Row(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        SizedBox(
          width: 110,
          child: Text(k, style: TextStyle(
            fontSize: mono ? 11 : 13, color: const Color(0xFF5A5A72),
            fontFamily: mono ? 'monospace' : null,
          )),
        ),
        Expanded(
          child: Text(v, style: TextStyle(
            fontSize: mono ? 11 : 13, color: Colors.white,
            fontFamily: mono ? 'monospace' : null,
          )),
        ),
      ],
    ),
  );
}

// ════════════════════════════════════════════════════════════════════════════════
// Mode toggle
// ════════════════════════════════════════════════════════════════════════════════

class _ModeToggle extends StatelessWidget {
  final bool               isTextMode;
  final ValueChanged<bool> onToggle;
  const _ModeToggle({required this.isTextMode, required this.onToggle});

  @override
  Widget build(BuildContext context) => Container(
    padding: const EdgeInsets.all(3),
    decoration: BoxDecoration(
      color:        const Color(0xFF16161F),
      borderRadius: BorderRadius.circular(20),
      border:       Border.all(color: const Color(0xFF22222E)),
    ),
    child: Row(
      mainAxisSize: MainAxisSize.min,
      children: [
        _Tab(label: 'Voice', icon: Icons.mic_rounded,
            active: !isTextMode, onTap: () => onToggle(false)),
        _Tab(label: 'Text',  icon: Icons.keyboard_rounded,
            active:  isTextMode, onTap: () => onToggle(true)),
      ],
    ),
  );
}

class _Tab extends StatelessWidget {
  final String       label;
  final IconData     icon;
  final bool         active;
  final VoidCallback onTap;
  const _Tab({required this.label, required this.icon,
    required this.active, required this.onTap});

  @override
  Widget build(BuildContext context) => GestureDetector(
    onTap: onTap,
    child: AnimatedContainer(
      duration: const Duration(milliseconds: 180),
      padding:  const EdgeInsets.symmetric(horizontal: 10, vertical: 5),
      decoration: BoxDecoration(
        color:        active ? const Color(0xFF6C63FF) : Colors.transparent,
        borderRadius: BorderRadius.circular(17),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(icon, size: 13,
              color: active ? Colors.white : const Color(0xFF5A5A72)),
          const SizedBox(width: 4),
          Text(label, style: TextStyle(
            fontSize: 11, fontWeight: FontWeight.w600,
            color: active ? Colors.white : const Color(0xFF5A5A72),
          )),
        ],
      ),
    ),
  );
}