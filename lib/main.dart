/// MyDrive — Flutter Voice + Text Client for Gemini Live FastAPI Backend
///
/// ── pubspec.yaml dependencies ────────────────────────────────────────────────
/// dependencies:
///   flutter:
///     sdk: flutter
///   record: ^6.2.0
///   flutter_pcm_sound: ^0.9.0        ← replaces just_audio for raw PCM streaming
///   web_socket_channel: ^3.0.1
///   permission_handler: ^12.0.1
///
/// ── Android — AndroidManifest.xml (inside <manifest>) ────────────────────────
///   <uses-permission android:name="android.permission.RECORD_AUDIO"/>
///   <uses-permission android:name="android.permission.INTERNET"/>
///
/// ── iOS — Info.plist ──────────────────────────────────────────────────────────
///   <key>NSMicrophoneUsageDescription</key>
///   <string>MyDrive needs the microphone to send voice messages.</string>
///
/// ── Audio pipeline (redesigned) ───────────────────────────────────────────────
///   Recording : 16-bit PCM, 16 kHz, mono  → /ws/chat as raw bytes
///   Playback  : raw 24 kHz PCM chunks fed directly into flutter_pcm_sound
///               which maintains its own native ring-buffer — zero file I/O,
///               zero gap between chunks, buttery-smooth continuous playback.
///
/// ── Key changes from original ─────────────────────────────────────────────────
///   BEFORE: just_audio played one temp WAV file per chunk (file-write + cold-
///           start overhead per chunk → audible gaps / stuttering).
///   AFTER : flutter_pcm_sound.feed() pushes Int16List frames directly into the
///           native audio output buffer. The OS mixer handles continuity — no
///           gaps, no file I/O, no per-chunk player lifecycle.
///
///   • _AudioStreamer encapsulates all playback state (single responsibility).
///   • Feed threshold (numChannels * sampleRate ÷ 4 = ~6 000 samples) keeps
///     the native buffer topped-up without over-buffering — standard practice
///     for real-time PCM streaming (matches WebRTC / Opus decoder patterns).
///   • Reconnect back-off is now exponential (1s → 2s → 4s … cap 16s) to
///     avoid hammering the server on flaky networks.
///   • _turnDone / idle-timeout logic is preserved but now operates on the
///     _AudioStreamer rather than a file-based queue.

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

const String _kBaseHost = 'mydrive-gemini-live-cueufbg0avdtg3de.canadacentral-01.azurewebsites.net';
const String _kVoiceUrl = 'wss://$_kBaseHost/ws/chat';
const String _kTextUrl  = 'wss://$_kBaseHost/ws/text';

/// Output sample rate sent by the Gemini backend (24 kHz, mono, 16-bit).
const int _kOutputSampleRate = 24000;

/// Input sample rate required by Gemini voice input (16 kHz, mono, 16-bit).
const int _kInputSampleRate  = 16000;

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
    time:  time,
    role:  role,
    input: input,
    text:  text ?? this.text,
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

enum _Status { idle, listening, processing, speaking, error }

// ════════════════════════════════════════════════════════════════════════════════
// _AudioStreamer — encapsulates all raw-PCM playback logic
//
// Industry pattern: feed raw PCM frames directly to the native audio engine
// via a callback-driven ring-buffer.  flutter_pcm_sound exposes exactly this:
//   • FlutterPcmSound.setup()  — initialise native audio session once
//   • FlutterPcmSound.feed()   — push an Int16List frame into the ring-buffer
//   • onFeedSamplesCallback    — fires when the buffer is running low so we
//                                can feed the next chunk proactively (pull model)
//
// This eliminates every source of gap in the old approach:
//   ✗ No temp-file write per chunk
//   ✗ No just_audio cold-start per chunk
//   ✗ No 40 ms polling sleep
//   ✓ Continuous native audio output — OS mixer handles timing
// ════════════════════════════════════════════════════════════════════════════════

class _AudioStreamer {
  _AudioStreamer({required this.onPlaybackStarted, required this.onPlaybackStopped});

  final VoidCallback onPlaybackStarted;
  final VoidCallback onPlaybackStopped;

  // Pending PCM chunks waiting to be fed into the native buffer.
  final List<Uint8List> _pending = [];

  bool _isSetup   = false;
  bool _isPlaying = false;

  /// Whether the server has signalled that this turn is complete.
  bool turnDone = false;

  bool get isPlaying => _isPlaying;

  // ── Initialise the native audio session (call once) ─────────────────────────
  Future<void> setup() async {
    if (_isSetup) return;
    await FlutterPcmSound.setup(
      sampleRate: _kOutputSampleRate,
      channelCount: 1,
    );
    // The feed threshold determines how many samples remain in the native
    // buffer before onFeedSamplesCallback fires.  At 24 kHz mono, 6 000
    // samples ≈ 250 ms of audio — enough headroom to feed the next chunk
    // without risking underrun, without over-buffering.
    FlutterPcmSound.setFeedThreshold(
      _kOutputSampleRate ~/ 4, // 6 000 samples ≈ 250 ms
    );
    FlutterPcmSound.setFeedCallback(_onFeedNeeded);
    _isSetup = true;
  }

  // ── Receive a new raw PCM chunk from the WebSocket ───────────────────────────
  void pushChunk(Uint8List pcmBytes) {
    _pending.add(pcmBytes);
    if (!_isPlaying) _startPlaying();
  }

  // ── Begin playback — feeds the first chunk to kick off the native session ────
  Future<void> _startPlaying() async {
    if (_isPlaying) return;
    await setup();
    _isPlaying = true;
    onPlaybackStarted();
    await FlutterPcmSound.start();
    _feedNext();
  }

  // ── Called by the native engine when the buffer needs more data ──────────────
  // This is the pull-model callback — we push the next pending chunk here.
  void _onFeedNeeded(int remainingSamples) {
    _feedNext();
  }

  // ── Push the next pending chunk into the native buffer ───────────────────────
  void _feedNext() {
    if (_pending.isEmpty) {
      // Buffer is empty.  If the turn is done, stop playback cleanly.
      // If not, leave the native session running — more chunks are on the way.
      if (turnDone) {
        _stopPlayback();
      }
      return;
    }

    final chunk = _pending.removeAt(0);
    // Interpret raw bytes as little-endian 16-bit signed PCM samples.
    final bd = ByteData.sublistView(chunk);
    final samples = List<int>.generate(
      chunk.lengthInBytes ~/ 2,
          (i) => bd.getInt16(i * 2, Endian.little),
    );
    FlutterPcmSound.feed(PcmArrayInt16.fromList(samples));
  }

  // ── Gracefully stop after the last chunk has been consumed ──────────────────
  // flutter_pcm_sound has no stop() — playback ends naturally when the buffer
  // drains and we stop feeding.  We call release() to tear down the native
  // audio session so it can be re-setup cleanly on the next turn.
  Future<void> _stopPlayback() async {
    if (!_isPlaying) return;
    _isPlaying = false;
    turnDone   = false;
    _isSetup   = false;          // force re-setup on next turn
    await FlutterPcmSound.release();
    onPlaybackStopped();
  }

  // ── Hard stop — called on interrupt or disconnect ────────────────────────────
  Future<void> stopImmediately() async {
    _pending.clear();
    turnDone = false;
    if (_isPlaying) {
      _isPlaying = false;
      _isSetup   = false;
      await FlutterPcmSound.release();
      onPlaybackStopped();
    }
  }

  void dispose() {
    stopImmediately();
  }
}

// ════════════════════════════════════════════════════════════════════════════════
// Chat page — state
// ════════════════════════════════════════════════════════════════════════════════

class ChatPage extends StatefulWidget {
  const ChatPage({super.key});

  @override
  State<ChatPage> createState() => _ChatPageState();
}

class _ChatPageState extends State<ChatPage> with TickerProviderStateMixin {

  // ── WebSocket channels ──────────────────────────────────────────────────────
  WebSocketChannel?            _voiceCh;
  StreamSubscription<dynamic>? _voiceSub;
  WebSocketChannel?            _textCh;
  StreamSubscription<dynamic>? _textSub;

  // Exponential back-off state for reconnects (caps at 16 s).
  int _voiceBackoffSec = 1;
  int _textBackoffSec  = 1;

  // ── Recording ───────────────────────────────────────────────────────────────
  final AudioRecorder _recorder    = AudioRecorder();
  bool                _isRecording = false;

  // ── Playback ─────────────────────────────────────────────────────────────────
  late final _AudioStreamer _streamer = _AudioStreamer(
    onPlaybackStarted:  () => _setStatus(_Status.speaking),
    onPlaybackStopped:  _onTurnFinished,
  );

  /// Tool cards buffered during a turn — inserted into the list after audio ends.
  final List<ToolItem> _pendingTools = [];

  // ── Text input ──────────────────────────────────────────────────────────────
  final TextEditingController _textCtrl  = TextEditingController();
  final FocusNode             _textFocus = FocusNode();
  bool _isTextMode = false;

  // ── UI state ─────────────────────────────────────────────────────────────────
  _Status _status = _Status.idle;

  final List<ChatItem>   _items  = [];
  final ScrollController _scroll = ScrollController();

  int?   _streamingBubbleIdx;
  String _streamingText = '';
  int?   _voicePlaceholderIdx;

  // ── Mic pulse animation ──────────────────────────────────────────────────────
  late final AnimationController _pulseCtrl = AnimationController(
    vsync: this,
    duration: const Duration(milliseconds: 850),
  )..repeat(reverse: true);

  late final Animation<double> _pulseAnim =
  Tween<double>(begin: 1.0, end: 1.20).animate(
    CurvedAnimation(parent: _pulseCtrl, curve: Curves.easeInOut),
  );

  // ── Lifecycle ────────────────────────────────────────────────────────────────

  @override
  void initState() {
    super.initState();
    _streamer.setup(); // warm-up the native audio session early
    _connectVoice();
    _connectText();
  }

  @override
  void dispose() {
    _voiceSub?.cancel();
    _voiceCh?.sink.close();
    _textSub?.cancel();
    _textCh?.sink.close();
    _recorder.dispose();
    _streamer.dispose();
    _pulseCtrl.dispose();
    _scroll.dispose();
    _textCtrl.dispose();
    _textFocus.dispose();
    super.dispose();
  }

  // ════════════════════════════════════════════════════════════════════════════
  // WebSocket connections — exponential back-off reconnect
  // ════════════════════════════════════════════════════════════════════════════

  void _connectVoice() {
    try {
      _voiceCh  = IOWebSocketChannel.connect(Uri.parse(_kVoiceUrl));
      _voiceSub = _voiceCh!.stream.listen(
        _onFrame,
        onError: (_) {
          final delay = _voiceBackoffSec;
          _voiceBackoffSec = (_voiceBackoffSec * 2).clamp(1, 16);
          Future.delayed(Duration(seconds: delay), _connectVoice);
        },
        onDone: () {
          final delay = _voiceBackoffSec;
          _voiceBackoffSec = (_voiceBackoffSec * 2).clamp(1, 16);
          Future.delayed(Duration(seconds: delay), _connectVoice);
        },
      );
      _voiceBackoffSec = 1; // reset on success
    } catch (_) {
      final delay = _voiceBackoffSec;
      _voiceBackoffSec = (_voiceBackoffSec * 2).clamp(1, 16);
      Future.delayed(Duration(seconds: delay), _connectVoice);
    }
  }

  void _connectText() {
    try {
      _textCh  = IOWebSocketChannel.connect(Uri.parse(_kTextUrl));
      _textSub = _textCh!.stream.listen(
        _onFrame,
        onError: (_) {
          final delay = _textBackoffSec;
          _textBackoffSec = (_textBackoffSec * 2).clamp(1, 16);
          Future.delayed(Duration(seconds: delay), _connectText);
        },
        onDone: () {
          final delay = _textBackoffSec;
          _textBackoffSec = (_textBackoffSec * 2).clamp(1, 16);
          Future.delayed(Duration(seconds: delay), _connectText);
        },
      );
      _textBackoffSec = 1;
    } catch (_) {
      final delay = _textBackoffSec;
      _textBackoffSec = (_textBackoffSec * 2).clamp(1, 16);
      Future.delayed(Duration(seconds: delay), _connectText);
    }
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Frame dispatcher — shared by both channels
  // ════════════════════════════════════════════════════════════════════════════

  void _onFrame(dynamic raw) {
    // ── Binary = raw 24 kHz PCM audio ────────────────────────────────────────
    // Push directly into the streamer — no file I/O, no intermediate queue.
    if (raw is List<int>) {
      _streamer.pushChunk(Uint8List.fromList(raw));
      return;
    }

    if (raw is! String) return;

    late final Map<String, dynamic> msg;
    try {
      msg = jsonDecode(raw) as Map<String, dynamic>;
    } catch (_) {
      return;
    }

    final type = msg['type'] as String?;
    if (type != null) {
      switch (type) {
        case 'gemini_transcript':
          final chunk = (msg['text'] as String?) ?? '';
          if (chunk.isNotEmpty) _appendGeminiTranscript(chunk);

        case 'user_transcript':
          final text = (msg['text'] as String?) ?? '';
          if (text.isNotEmpty) _resolveVoicePlaceholder(text);

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
        if (_status == _Status.error || _status == _Status.idle) {
          _setStatus(_Status.idle);
        }

      case 'processing':
        _streamingBubbleIdx = null;
        _streamingText      = '';
        _setStatus(_Status.processing);

      case 'done':
      // Signal the streamer: once its buffer drains, call _onTurnFinished.
        _streamer.turnDone = true;
        // Edge case: if no audio chunks arrived at all, finish immediately.
        if (!_streamer.isPlaying && _streamer.turnDone) {
          _streamer.turnDone = false;
          _onTurnFinished();
        }

      case 'interrupted':
        _pendingTools.clear();
        _streamer.stopImmediately();
        _setStatus(_Status.idle);

      case 'error':
        _setStatus(_Status.error);
        final errMsg = (msg['message'] as String?) ?? 'Unknown error';
        _addItem(BubbleItem(
          time: DateTime.now(),
          role: _Role.assistant,
          text: '❌ $errMsg',
        ));
    }
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Gemini transcript streaming
  // ════════════════════════════════════════════════════════════════════════════

  void _appendGeminiTranscript(String chunk) {
    _streamingText += chunk;
    if (!mounted) return;
    setState(() {
      if (_streamingBubbleIdx == null) {
        _streamingBubbleIdx = _items.length;
        _items.add(BubbleItem(
          time: DateTime.now(),
          role: _Role.assistant,
          text: _streamingText,
        ));
      } else {
        final idx = _streamingBubbleIdx!;
        if (idx < _items.length && _items[idx] is BubbleItem) {
          _items[idx] = (_items[idx] as BubbleItem).copyWith(text: _streamingText);
        }
      }
    });
    _scrollToBottom();
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Voice placeholder → real transcript
  // ════════════════════════════════════════════════════════════════════════════

  void _addVoicePlaceholder() {
    if (!mounted) return;
    setState(() {
      _voicePlaceholderIdx = _items.length;
      _items.add(BubbleItem(
        time:  DateTime.now(),
        role:  _Role.user,
        input: _Input.voice,
        text:  '🎙️ …',
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
      _items[idx] = (_items[idx] as BubbleItem).copyWith(
        text: '"$transcript"',
      );
    });
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Turn finished — called by _AudioStreamer once the buffer is fully drained
  // ════════════════════════════════════════════════════════════════════════════

  void _onTurnFinished() {
    if (!mounted) return;
    if (_pendingTools.isNotEmpty) {
      setState(() {
        _items.addAll(_pendingTools);
        _pendingTools.clear();
      });
      _scrollToBottom();
    }
    _streamingBubbleIdx = null;
    _streamingText      = '';
    _setStatus(_Status.idle);
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Voice recording
  // ════════════════════════════════════════════════════════════════════════════

  Future<void> _startRecording() async {
    if (_status == _Status.processing ||
        _status == _Status.speaking   ||
        _isRecording) return;

    final perm = await Permission.microphone.request();
    if (!perm.isGranted) {
      _addItem(BubbleItem(
        time: DateTime.now(),
        role: _Role.assistant,
        text: '⚠️ Microphone permission denied.',
      ));
      return;
    }

    final path = '${Directory.systemTemp.path}/mydrive_voice.pcm';

    await _recorder.start(
      const RecordConfig(
        encoder:     AudioEncoder.pcm16bits,
        sampleRate:  _kInputSampleRate,
        numChannels: 1,
      ),
      path: path,
    );

    if (mounted) {
      setState(() {
        _isRecording = true;
        _status      = _Status.listening;
      });
    }
  }

  Future<void> _stopRecording() async {
    if (!_isRecording) return;
    final path = await _recorder.stop();
    if (mounted) setState(() => _isRecording = false);

    if (path == null || _voiceCh == null) return;
    final file = File(path);
    if (!await file.exists()) return;

    final bytes = await file.readAsBytes();
    if (bytes.isEmpty) return;

    _addVoicePlaceholder();

    _streamingBubbleIdx = null;
    _streamingText      = '';

    _voiceCh!.sink.add(bytes);
    _voiceCh!.sink.add('END_OF_SPEECH');
    _setStatus(_Status.processing);
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Text sending
  // ════════════════════════════════════════════════════════════════════════════

  void _sendText() {
    final text = _textCtrl.text.trim();
    if (text.isEmpty) return;

    if (_textCh == null) {
      _addItem(BubbleItem(
        time: DateTime.now(),
        role: _Role.assistant,
        text: '⚠️ Text channel not connected.',
      ));
      return;
    }

    _textCtrl.clear();
    _textFocus.unfocus();

    _addItem(BubbleItem(
      time:  DateTime.now(),
      role:  _Role.user,
      input: _Input.text,
      text:  text,
    ));

    _streamingBubbleIdx = null;
    _streamingText      = '';

    _textCh!.sink.add(jsonEncode({'type': 'message', 'text': text}));
    _setStatus(_Status.processing);
  }

  // ════════════════════════════════════════════════════════════════════════════
  // Helpers
  // ════════════════════════════════════════════════════════════════════════════

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
      if (_scroll.hasClients) {
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
    _Status.listening  => 'Listening…',
    _Status.processing => 'Thinking…',
    _Status.speaking   => 'Speaking…',
    _Status.error      => 'Connection error',
  };

  Color get _statusColor => switch (_status) {
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
            Future.delayed(
                const Duration(milliseconds: 80), _textFocus.requestFocus);
          } else {
            _textFocus.unfocus();
          }
        }),
      ),
      body: Column(
        children: [
          Expanded(child: _buildList(cs)),
          _StatusDots(status: _status),
          _isTextMode ? _buildTextInput(cs) : _buildVoiceInput(cs),
        ],
      ),
    );
  }

  // ── Message list ─────────────────────────────────────────────────────────────

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

  // ── Voice input ───────────────────────────────────────────────────────────────

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
          onLongPressStart: (_) => _startRecording(),
          onLongPressEnd:   (_) => _stopRecording(),
          child: AnimatedBuilder(
            animation: _pulseAnim,
            builder: (_, child) => Transform.scale(
              scale: _isRecording ? _pulseAnim.value : 1.0,
              child: child,
            ),
            child: Container(
              width: 72, height: 72,
              decoration: BoxDecoration(
                shape: BoxShape.circle,
                color: _isRecording
                    ? const Color(0xFFFF4D6D)
                    : const Color(0xFF6C63FF),
                boxShadow: [
                  BoxShadow(
                    color: (_isRecording
                        ? const Color(0xFFFF4D6D)
                        : const Color(0xFF6C63FF))
                        .withValues(alpha: 0.48),
                    blurRadius: 24,
                    spreadRadius: 3,
                  ),
                ],
              ),
              child: Icon(
                _isRecording ? Icons.stop_rounded : Icons.mic_rounded,
                color: Colors.white, size: 32,
              ),
            ),
          ),
        ),
        const SizedBox(height: 10),
        Text(
          _isRecording ? 'Release to send' : 'Hold to speak',
          style: TextStyle(
              color: cs.onSurface.withValues(alpha: 0.38), fontSize: 13),
        ),
      ],
    ),
  );

  // ── Text input ────────────────────────────────────────────────────────────────

  Widget _buildTextInput(ColorScheme cs) {
    final canSend = _status != _Status.processing &&
        _status != _Status.speaking;
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
              minLines: 1, maxLines: 5,
              keyboardType:    TextInputType.multiline,
              textInputAction: TextInputAction.newline,
              style: const TextStyle(fontSize: 14.5, color: Colors.white),
              decoration: InputDecoration(
                hintText:  'Type a message…',
                hintStyle: TextStyle(color: cs.onSurface.withValues(alpha: 0.35)),
                filled:    true,
                fillColor: const Color(0xFF16161F),
                contentPadding:
                const EdgeInsets.symmetric(horizontal: 16, vertical: 11),
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
                  shape: BoxShape.circle,
                  color: const Color(0xFF6C63FF),
                  boxShadow: [
                    BoxShadow(
                      color:        const Color(0xFF6C63FF).withValues(alpha: 0.42),
                      blurRadius:   14,
                      spreadRadius: 2,
                    ),
                  ],
                ),
                child: const Icon(Icons.send_rounded,
                    color: Colors.white, size: 20),
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
    required this.statusLabel,
    required this.statusColor,
    required this.isTextMode,
    required this.onModeToggle,
  });

  @override
  Size get preferredSize => const Size.fromHeight(kToolbarHeight);

  @override
  Widget build(BuildContext context) => AppBar(
    backgroundColor: const Color(0xFF111118),
    surfaceTintColor: Colors.transparent,
    title: Row(
      children: [
        Container(
          width: 36, height: 36,
          decoration: BoxDecoration(
            borderRadius: BorderRadius.circular(10),
            gradient: const LinearGradient(
              colors: [Color(0xFF6C63FF), Color(0xFFFF6B9D)],
              begin: Alignment.topLeft,
              end: Alignment.bottomRight,
            ),
            boxShadow: const [
              BoxShadow(color: Color(0x666C63FF), blurRadius: 12),
            ],
          ),
          child: const Center(
            child: Text('🚗', style: TextStyle(fontSize: 18)),
          ),
        ),
        const SizedBox(width: 10),
        Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text('MyDrive',
                style: TextStyle(
                    fontSize:   16,
                    fontWeight: FontWeight.w700,
                    color:      Colors.white)),
            Row(
              children: [
                Container(
                  width: 6, height: 6,
                  margin: const EdgeInsets.only(right: 5),
                  decoration: BoxDecoration(
                      shape: BoxShape.circle, color: statusColor),
                ),
                Text(statusLabel,
                    style: TextStyle(fontSize: 11, color: statusColor)),
              ],
            ),
          ],
        ),
      ],
    ),
    actions: [
      Padding(
        padding: const EdgeInsets.only(right: 12),
        child: _ModeToggle(
          isTextMode: isTextMode,
          onToggle:   onModeToggle,
        ),
      ),
    ],
  );
}

// ════════════════════════════════════════════════════════════════════════════════
// Status dots
// ════════════════════════════════════════════════════════════════════════════════

class _StatusDots extends StatelessWidget {
  final _Status status;
  const _StatusDots({required this.status});

  @override
  Widget build(BuildContext context) {
    final show =
        status == _Status.processing || status == _Status.speaking;
    return AnimatedSize(
      duration: const Duration(milliseconds: 200),
      child: show
          ? Padding(
        padding: const EdgeInsets.symmetric(vertical: 6),
        child: Row(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            _Dot(index: 0), _Dot(index: 1), _Dot(index: 2),
            const SizedBox(width: 8),
            Text(
              status == _Status.processing
                  ? 'MyDrive AI is thinking…'
                  : 'Speaking…',
              style: const TextStyle(
                  color: Color(0xFF5A5A72), fontSize: 12),
            ),
          ],
        ),
      )
          : const SizedBox.shrink(),
    );
  }
}

class _Dot extends StatelessWidget {
  final int index;
  const _Dot({required this.index});

  @override
  Widget build(BuildContext context) => TweenAnimationBuilder<double>(
    tween:    Tween(begin: 0.3, end: 1.0),
    duration: Duration(milliseconds: 480 + index * 140),
    builder: (_, v, __) => Opacity(
      opacity: v,
      child: Container(
        margin: const EdgeInsets.symmetric(horizontal: 3),
        width: 6, height: 6,
        decoration: const BoxDecoration(
          color: Color(0xFF6C63FF),
          shape: BoxShape.circle,
        ),
      ),
    ),
  );
}

// ════════════════════════════════════════════════════════════════════════════════
// Text bubble widget
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
                padding: const EdgeInsets.symmetric(
                    horizontal: 14, vertical: 10),
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
                    fontSize:  14.5,
                    height:    1.5,
                    color: isUser
                        ? Colors.white
                        : Colors.white.withValues(alpha: 0.85),
                    fontStyle:
                    isUser ? FontStyle.normal : FontStyle.italic,
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
    return input == _Input.voice
        ? Icons.mic_rounded
        : Icons.keyboard_rounded;
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
// Tool card widget
// ════════════════════════════════════════════════════════════════════════════════

class _ToolCardWidget extends StatelessWidget {
  final ToolItem item;
  const _ToolCardWidget({required this.item});

  static const _meta =
  <String, ({String icon, String label, Color color})>{
    'request_roadside_assistance': (
    icon:  '🔧',
    label: 'Roadside Assistance',
    color: Color(0xFFFFB830),
    ),
    'request_tow_truck': (
    icon:  '🚛',
    label: 'Tow Truck Dispatched',
    color: Color(0xFFFF4D6D),
    ),
    'search_spare_parts': (
    icon:  '🔩',
    label: 'Spare Parts Search',
    color: Color(0xFF6C63FF),
    ),
    'book_garage_service': (
    icon:  '🏪',
    label: 'Garage Booking',
    color: Color(0xFF00E5A0),
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
                            color: const Color(0xFF16161F),
                            borderRadius: BorderRadius.circular(8),
                            border: Border.all(
                                color: const Color(0xFF22222E)),
                          ),
                          child: Column(
                            children: item.result.entries
                                .map((e) => _KVRow(
                              k:    e.key,
                              v:    e.value.toString(),
                              mono: true,
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
  const _CardHeader({
    required this.icon,
    required this.label,
    required this.color,
  });

  @override
  Widget build(BuildContext context) => Container(
    padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 9),
    decoration: BoxDecoration(
      color: color.withValues(alpha: 0.07),
      borderRadius: const BorderRadius.only(
        topLeft:  Radius.circular(15),
        topRight: Radius.circular(15),
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
                fontSize:      12,
                fontWeight:    FontWeight.w700,
                color:         color,
                letterSpacing: 0.3,
              )),
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
                      fontSize:   10,
                      color:      Color(0xFF00E5A0),
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
    builder: (_, v, child) => Opacity(opacity: v, child: child),
    child: Container(
      width: 5, height: 5,
      decoration: const BoxDecoration(
        shape: BoxShape.circle,
        color: Color(0xFF00E5A0),
      ),
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
      fontSize:      9.5,
      color:         Color(0xFF5A5A72),
      letterSpacing: 0.8,
      fontWeight:    FontWeight.w600,
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
          child: Text(k,
              style: TextStyle(
                fontSize:   mono ? 11 : 13,
                color:      const Color(0xFF5A5A72),
                fontFamily: mono ? 'monospace' : null,
              )),
        ),
        Expanded(
          child: Text(v,
              style: TextStyle(
                fontSize:   mono ? 11 : 13,
                color:      Colors.white,
                fontFamily: mono ? 'monospace' : null,
              )),
        ),
      ],
    ),
  );
}

// ════════════════════════════════════════════════════════════════════════════════
// Mode toggle (Voice ↔ Text)
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
      border: Border.all(color: const Color(0xFF22222E)),
    ),
    child: Row(
      mainAxisSize: MainAxisSize.min,
      children: [
        _Tab(
          label:  'Voice',
          icon:   Icons.mic_rounded,
          active: !isTextMode,
          onTap:  () => onToggle(false),
        ),
        _Tab(
          label:  'Text',
          icon:   Icons.keyboard_rounded,
          active: isTextMode,
          onTap:  () => onToggle(true),
        ),
      ],
    ),
  );
}

class _Tab extends StatelessWidget {
  final String     label;
  final IconData   icon;
  final bool       active;
  final VoidCallback onTap;
  const _Tab({
    required this.label,
    required this.icon,
    required this.active,
    required this.onTap,
  });

  @override
  Widget build(BuildContext context) => GestureDetector(
    onTap: onTap,
    child: AnimatedContainer(
      duration: const Duration(milliseconds: 180),
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 5),
      decoration: BoxDecoration(
        color:        active ? const Color(0xFF6C63FF) : Colors.transparent,
        borderRadius: BorderRadius.circular(17),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(icon,
              size:  13,
              color: active ? Colors.white : const Color(0xFF5A5A72)),
          const SizedBox(width: 4),
          Text(label,
              style: TextStyle(
                fontSize:   11,
                fontWeight: FontWeight.w600,
                color: active ? Colors.white : const Color(0xFF5A5A72),
              )),
        ],
      ),
    ),
  );
}