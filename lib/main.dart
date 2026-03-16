// lib/main.dart
// MyDrive Assistant — Flutter Client (Unified WebSocket API)
// ============================================================
// Architecture:
//   Single WebSocket connection to /ws/chat?session_id=<id>
//   - Outbound JSON: text_input, voice_start, voice_end, set_speaker, interrupt
//   - Outbound binary: raw PCM 16kHz 16-bit (streamed in real-time, no buffering)
//   - Inbound JSON: session_ready, user_transcript, assistant_text, tool_call,
//                   turn_complete, speaker_mode_updated, error, session_ended
//   - Inbound binary: raw PCM 24kHz (when speaker mode is on)
//
// Streaming audio design (v2)
// ───────────────────────────
// Each PCM chunk from the microphone is sent to the server as a binary
// WebSocket frame immediately — there is NO local accumulation.
//
// Signal sequence:
//   1. User taps mic  → send JSON {"type":"voice_start"}
//                       server sends activity_start to Gemini,
//                       spawns a background receive task
//   2. Mic is open    → each PCM chunk sent as raw binary immediately
//                       server forwards each to Gemini via send_realtime_input
//   3. User taps again→ send JSON {"type":"voice_end"}
//                       server sends activity_end to Gemini,
//                       awaits the full response
//
// This means Gemini starts processing the user's first words while they are
// still speaking — the utterance duration is no longer on the critical path.
//
// Dependencies (pubspec.yaml):
//   record: ^6.2.0
//   web_socket_channel: ^3.0.1
//   permission_handler: ^12.0.1
//   flutter_pcm_sound: ^3.3.3
//   uuid: ^4.0.0
//
// Android permissions: RECORD_AUDIO, INTERNET, MODIFY_AUDIO_SETTINGS
// iOS: NSMicrophoneUsageDescription

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:record/record.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:permission_handler/permission_handler.dart';
import 'package:flutter_pcm_sound/flutter_pcm_sound.dart';
import 'package:uuid/uuid.dart';

// ── Backend ────────────────────────────────────────────────────────────────────
const String _kHost          = 'mydrive-gemini-live-cueufbg0avdtg3de.canadacentral-01.azurewebsites.net';
const int    _kOutSampleRate = 24000; // Gemini output: 24 kHz PCM
const int    _kInSampleRate  = 16000; // Gemini input:  16 kHz PCM
const int    _kFeedThreshold = 4800;  // ~200 ms worth of frames at 24 kHz

// ── Palette ────────────────────────────────────────────────────────────────────
const _kBg      = Color(0xFF0A0A0F);
const _kSurface = Color(0xFF111118);
const _kPanel   = Color(0xFF16161F);
const _kBorder  = Color(0xFF22222E);
const _kAccent  = Color(0xFF6C63FF);
const _kAccent2 = Color(0xFFFF6B9D);
const _kGreen   = Color(0xFF00E5A0);
const _kAmber   = Color(0xFFFFB830);
const _kRed     = Color(0xFFFF4D6D);
const _kText    = Color(0xFFE8E8F0);
const _kMuted   = Color(0xFF5A5A72);
const _kUserBg  = Color(0xFF1E1B3A);
const _kAiBg    = Color(0xFF131320);

// ── Models ─────────────────────────────────────────────────────────────────────
enum MessageRole { user, ai }
enum MessageKind { text, voice, toolCall }
enum AppStatus   { disconnected, connecting, ready, listening, processing, speaking, error }

class ChatMessage {
  final String id;
  final MessageRole role;
  final MessageKind kind;
  String text;
  Map<String, dynamic>? toolData;
  ChatMessage({
    required this.id,
    required this.role,
    required this.kind,
    required this.text,
    this.toolData,
  });
}

// ── Entry point ────────────────────────────────────────────────────────────────
void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  SystemChrome.setSystemUIOverlayStyle(const SystemUiOverlayStyle(
    statusBarColor: Colors.transparent,
    statusBarIconBrightness: Brightness.light,
  ));
  await FlutterPcmSound.setup(sampleRate: _kOutSampleRate, channelCount: 1);
  await FlutterPcmSound.setFeedThreshold(_kFeedThreshold);
  runApp(const MyDriveApp());
}

class MyDriveApp extends StatelessWidget {
  const MyDriveApp({super.key});
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'MyDrive Assistant',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        brightness: Brightness.dark,
        scaffoldBackgroundColor: _kBg,
        colorScheme: const ColorScheme.dark(
          primary: _kAccent,
          secondary: _kAccent2,
          surface: _kSurface,
        ),
        textTheme: const TextTheme(bodyMedium: TextStyle(color: _kText)),
      ),
      home: const ChatScreen(),
    );
  }
}

// ── Chat screen ────────────────────────────────────────────────────────────────
class ChatScreen extends StatefulWidget {
  const ChatScreen({super.key});
  @override
  State<ChatScreen> createState() => _ChatScreenState();
}

class _ChatScreenState extends State<ChatScreen> with TickerProviderStateMixin {
  // ── Session & messages ───────────────────────────────────────────────────────
  String _sessionId = const Uuid().v4();
  final List<ChatMessage> _messages = [];
  AppStatus _status     = AppStatus.disconnected;
  bool _isRecording     = false;
  bool _showThinking    = false;
  bool _speakerMode     = false;

  // ── Controllers ──────────────────────────────────────────────────────────────
  final ScrollController      _scrollCtrl = ScrollController();
  final TextEditingController _textCtrl   = TextEditingController();
  final FocusNode             _textFocus  = FocusNode();

  // ── WebSocket ─────────────────────────────────────────────────────────────────
  WebSocketChannel?   _ws;
  StreamSubscription? _wsSub;

  // ── Audio recording ──────────────────────────────────────────────────────────
  final AudioRecorder _recorder = AudioRecorder();
  StreamSubscription<Uint8List>? _recordSub;

  // ── Audio playback ───────────────────────────────────────────────────────────
  final List<Uint8List> _audioQueue = [];
  bool _isPlaying = false;

  // ── Turn tracking ─────────────────────────────────────────────────────────────
  // The user voice bubble and the AI reply bubble both stream in as fragments.
  // We track the in-progress bubble ID for each role so fragments append to the
  // right bubble. Both IDs are cleared on turn_complete and on new input.
  bool    _inTurn                    = false;
  String? _currentAiMessageId;
  String? _currentUserVoiceMessageId;

  // ── Mic animation ─────────────────────────────────────────────────────────────
  late AnimationController _pulseCtrl;
  late Animation<double>   _pulseAnim;

  // ── Init / dispose ────────────────────────────────────────────────────────────
  @override
  void initState() {
    super.initState();
    _pulseCtrl = AnimationController(vsync: this, duration: const Duration(milliseconds: 800));
    _pulseAnim = Tween<double>(begin: 1.0, end: 1.18)
        .animate(CurvedAnimation(parent: _pulseCtrl, curve: Curves.easeInOut));
    _pulseCtrl.stop();

    FlutterPcmSound.setFeedCallback(_onPcmFeedRequest);
    _connect();
  }

  @override
  void dispose() {
    _pulseCtrl.dispose();
    _scrollCtrl.dispose();
    _textCtrl.dispose();
    _textFocus.dispose();
    _disconnect();
    _recorder.dispose();
    FlutterPcmSound.release();
    super.dispose();
  }

  // ── Connection ────────────────────────────────────────────────────────────────
  void _connect() {
    _setStatus(AppStatus.connecting);
    final uri = Uri.parse('wss://$_kHost/ws/chat?session_id=$_sessionId');
    _ws = WebSocketChannel.connect(uri);
    _wsSub = _ws!.stream.listen(
      _onMessage,
      onDone:  () => _setStatus(AppStatus.disconnected),
      onError: (_) => _setStatus(AppStatus.error),
    );
  }

  void _disconnect() {
    _wsSub?.cancel();
    _ws?.sink.close();
    _ws = null;
  }

  // ── Incoming message handler ──────────────────────────────────────────────────
  void _onMessage(dynamic raw) {
    if (raw is Uint8List) {
      _enqueueAudio(raw);
    } else if (raw is List<int>) {
      _enqueueAudio(Uint8List.fromList(raw));
    } else if (raw is String) {
      _handleJson(raw);
    }
  }

  // ── JSON frame handler ────────────────────────────────────────────────────────
  void _handleJson(String raw) {
    Map<String, dynamic> data;
    try {
      data = jsonDecode(raw) as Map<String, dynamic>;
    } catch (_) {
      return;
    }

    final type = data['type'] as String?;

    switch (type) {
      case 'session_ready':
        _setStatus(AppStatus.ready);
        setState(() => _speakerMode = data['speaker_mode'] as bool? ?? false);
        break;

      case 'user_transcript':
      // Input transcription arrives as multiple partial fragments during a turn.
      // Append each fragment to the same bubble until the turn resets.
        final fragment = data['text'] as String? ?? '';
        if (fragment.isEmpty) return;
        setState(() {
          if (_currentUserVoiceMessageId != null) {
            final idx = _messages.indexWhere((m) => m.id == _currentUserVoiceMessageId);
            if (idx != -1) {
              final cur = _messages[idx].text;
              final needSpace = cur.isNotEmpty && !cur.endsWith(' ') && !fragment.startsWith(' ');
              _messages[idx].text = needSpace ? '$cur $fragment' : '$cur$fragment';
            } else {
              // bubble removed unexpectedly — create a new one
              _currentUserVoiceMessageId = _uuid();
              _messages.add(ChatMessage(
                id: _currentUserVoiceMessageId!, role: MessageRole.user,
                kind: MessageKind.voice, text: fragment,
              ));
            }
          } else {
            _currentUserVoiceMessageId = _uuid();
            _messages.add(ChatMessage(
              id: _currentUserVoiceMessageId!, role: MessageRole.user,
              kind: MessageKind.voice, text: fragment,
            ));
          }
        });
        _scrollToBottom();
        break;

      case 'assistant_text':
      // Output transcription arrives as multiple partial fragments.
      // Do NOT trim — Gemini uses leading spaces as natural word separators.
        final fragment = data['text'] as String? ?? '';
        if (fragment.isEmpty) return;
        setState(() {
          _showThinking = false;
          if (_inTurn && _currentAiMessageId != null) {
            final idx = _messages.indexWhere((m) => m.id == _currentAiMessageId);
            if (idx != -1) {
              final cur = _messages[idx].text;
              final needSpace = cur.isNotEmpty && !cur.endsWith(' ') && !fragment.startsWith(' ');
              _messages[idx].text = needSpace ? '$cur $fragment' : '$cur$fragment';
            } else {
              _currentAiMessageId = _uuid();
              _messages.add(ChatMessage(
                id: _currentAiMessageId!, role: MessageRole.ai,
                kind: MessageKind.text, text: fragment,
              ));
            }
          } else {
            _inTurn = true;
            _currentAiMessageId = _uuid();
            _messages.add(ChatMessage(
              id: _currentAiMessageId!, role: MessageRole.ai,
              kind: MessageKind.text, text: fragment,
            ));
          }
        });
        _scrollToBottom();
        break;

      case 'tool_call':
        _addMessage(ChatMessage(
          id: _uuid(), role: MessageRole.ai, kind: MessageKind.toolCall,
          text: data['tool'] as String? ?? 'tool',
          toolData: Map<String, dynamic>.from(data),
        ));
        break;

      case 'turn_complete':
        setState(() {
          _inTurn                    = false;
          _currentAiMessageId        = null;
          _currentUserVoiceMessageId = null;
          _showThinking              = false;
        });
        if (_status == AppStatus.processing) _setStatus(AppStatus.ready);
        break;

      case 'speaker_mode_updated':
        setState(() => _speakerMode = data['enabled'] as bool? ?? false);
        break;

      case 'error':
        _setStatus(AppStatus.error);
        _addMessage(ChatMessage(
          id: _uuid(), role: MessageRole.ai, kind: MessageKind.text,
          text: '⚠️ ${data['message'] ?? 'Unknown error'}',
        ));
        break;

      case 'session_ended':
        _setStatus(AppStatus.disconnected);
        break;

      default:
        break;
    }
  }

  // ── PCM playback ──────────────────────────────────────────────────────────────
  void _enqueueAudio(Uint8List pcm) {
    _audioQueue.add(pcm);
    if (!_isPlaying) _startPlayback();
  }

  void _startPlayback() {
    if (_isPlaying || _audioQueue.isEmpty) return;
    _isPlaying = true;
    _setStatus(AppStatus.speaking);
    _feedNext();
  }

  void _feedNext() {
    if (_audioQueue.isEmpty) {
      _isPlaying = false;
      if (mounted && _status == AppStatus.speaking) _setStatus(AppStatus.ready);
      return;
    }
    final chunk   = _audioQueue.removeAt(0);
    final samples = _bytesToInt16(chunk);
    FlutterPcmSound.feed(PcmArrayInt16.fromList(samples));
  }

  void _onPcmFeedRequest(int remainingFrames) {
    if (remainingFrames == 0) _feedNext();
  }

  List<int> _bytesToInt16(Uint8List bytes) {
    final bd  = bytes.buffer.asByteData();
    final out = <int>[];
    for (var i = 0; i + 1 < bytes.length; i += 2) {
      out.add(bd.getInt16(i, Endian.little));
    }
    return out;
  }

  // ── Voice recording ───────────────────────────────────────────────────────────
  //
  // STREAMING DESIGN — NO LOCAL BUFFERING
  // ──────────────────────────────────────
  // _startRecording:
  //   1. Request mic permission.
  //   2. Send {"type":"voice_start"} — server sends activity_start to Gemini
  //      and spawns a background response-consumer task.
  //   3. Reset turn-tracking IDs so incoming transcripts get fresh bubbles.
  //   4. Open the PCM stream; each chunk is sent as a raw binary WebSocket
  //      frame immediately — the server forwards it to Gemini without buffering.
  //
  // _stopRecording:
  //   1. Stop the mic stream so no more binary frames are sent.
  //   2. Send {"type":"voice_end"} — server sends activity_end to Gemini and
  //      awaits the full response from its background task.
  //
  Future<void> _startRecording() async {
    if (_isRecording) return;

    final perm = await Permission.microphone.request();
    if (!perm.isGranted) {
      _addMessage(ChatMessage(
        id: _uuid(), role: MessageRole.ai, kind: MessageKind.text,
        text: '⚠️ Microphone permission denied.',
      ));
      return;
    }

    if (_ws == null) _connect();

    // Signal the server that the user's voice turn is starting.
    // The server will immediately send activity_start to Gemini and launch
    // its background response-consumer — so Gemini is ready before the first
    // audio chunk even arrives.
    _sendJson({'type': 'voice_start'});

    // Clear turn state so the first user_transcript fragment creates a fresh
    // bubble, not one left over from a previous turn.
    setState(() {
      _inTurn                    = false;
      _currentAiMessageId        = null;
      _currentUserVoiceMessageId = null;
      _showThinking              = false;
    });

    final stream = await _recorder.startStream(const RecordConfig(
      encoder:       AudioEncoder.pcm16bits,
      sampleRate:    _kInSampleRate,
      numChannels:   1,
      echoCancel:    true,
      noiseSuppress: true,
      autoGain:      true,
    ));

    // Stream each chunk directly to the WebSocket — zero local buffering.
    // The server enqueues each chunk as an AUDIO_CHUNK frame and the
    // session_runner forwards it to Gemini via send_realtime_input immediately.
    _recordSub = stream.listen((Uint8List chunk) {
      _ws?.sink.add(chunk);
    });

    setState(() {
      _isRecording  = true;
      _showThinking = false;
    });
    _pulseCtrl.repeat(reverse: true);
    _setStatus(AppStatus.listening);
  }

  Future<void> _stopRecording() async {
    if (!_isRecording) return;

    // Stop the mic first so no more binary frames are emitted.
    await _recordSub?.cancel();
    _recordSub = null;
    await _recorder.stop();

    // Signal the server that the user has finished speaking.
    // The server sends activity_end to Gemini, which then generates a response.
    _sendJson({'type': 'voice_end'});

    setState(() {
      _isRecording  = false;
      _showThinking = true;
    });
    _pulseCtrl
      ..stop()
      ..reset();
    _setStatus(AppStatus.processing);
  }

  // ── Text sending ──────────────────────────────────────────────────────────────
  void _sendText() {
    final text = _textCtrl.text.trim();
    if (text.isEmpty) return;
    if (_ws == null) _connect();

    _addMessage(ChatMessage(
      id: _uuid(), role: MessageRole.user, kind: MessageKind.text, text: text,
    ));

    _textCtrl.clear();
    setState(() {
      _showThinking              = true;
      _inTurn                    = false;
      _currentAiMessageId        = null;
      _currentUserVoiceMessageId = null;
    });

    _sendJson({'type': 'text_input', 'text': text});

    _setStatus(AppStatus.processing);
  }

  // ── Speaker toggle ────────────────────────────────────────────────────────────
  void _toggleSpeaker() {
    _sendJson({'type': 'set_speaker', 'enabled': !_speakerMode});
    setState(() => _speakerMode = !_speakerMode); // optimistic
  }

  // ── Interrupt ─────────────────────────────────────────────────────────────────
  void _sendInterrupt() {
    _sendJson({'type': 'interrupt'});
    _audioQueue.clear();
    setState(() {
      _showThinking              = false;
      _inTurn                    = false;
      _currentAiMessageId        = null;
      _currentUserVoiceMessageId = null;
    });
  }

  // ── New chat ──────────────────────────────────────────────────────────────────
  void _newChat() {
    _disconnect();
    _audioQueue.clear();
    _isPlaying = false;
    setState(() {
      _sessionId                 = const Uuid().v4();
      _messages.clear();
      _showThinking              = false;
      _isRecording               = false;
      _inTurn                    = false;
      _currentAiMessageId        = null;
      _currentUserVoiceMessageId = null;
      _speakerMode               = false;
    });
    _connect();
  }

  // ── Helpers ───────────────────────────────────────────────────────────────────
  void _setStatus(AppStatus s) { if (mounted) setState(() => _status = s); }

  void _addMessage(ChatMessage msg) {
    setState(() => _messages.add(msg));
    _scrollToBottom();
  }

  void _scrollToBottom() {
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (_scrollCtrl.hasClients) {
        _scrollCtrl.animateTo(
          _scrollCtrl.position.maxScrollExtent,
          duration: const Duration(milliseconds: 280),
          curve: Curves.easeOut,
        );
      }
    });
  }

  void _sendJson(Map<String, dynamic> json) {
    _ws?.sink.add(jsonEncode(json));
  }

  String _uuid() => const Uuid().v4();

  (String, Color) get _statusInfo => switch (_status) {
    AppStatus.disconnected => ('Disconnected', _kMuted),
    AppStatus.connecting   => ('Connecting…',  _kAmber),
    AppStatus.ready        => ('Ready',         _kGreen),
    AppStatus.listening    => ('Listening…',    _kAccent2),
    AppStatus.processing   => ('Thinking…',     _kAmber),
    AppStatus.speaking     => ('Speaking…',     _kAccent),
    AppStatus.error        => ('Error',          _kRed),
  };

  // ── Build ──────────────────────────────────────────────────────────────────────
  @override
  Widget build(BuildContext context) {
    final (statusLabel, statusColor) = _statusInfo;
    return Scaffold(
      backgroundColor: _kBg,
      body: SafeArea(
        child: Column(children: [
          _buildHeader(statusLabel, statusColor),
          const Divider(color: _kBorder, height: 1, thickness: 1),
          Expanded(child: _buildMessageList()),
          const Divider(color: _kBorder, height: 1, thickness: 1),
          _buildInputBar(),
        ]),
      ),
    );
  }

  Widget _buildHeader(String statusLabel, Color statusColor) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
      child: Row(children: [
        Container(
          width: 40, height: 40,
          decoration: BoxDecoration(
            gradient: const LinearGradient(
              colors: [_kAccent, _kAccent2],
              begin: Alignment.topLeft,
              end: Alignment.bottomRight,
            ),
            borderRadius: BorderRadius.circular(12),
            boxShadow: [BoxShadow(color: _kAccent.withValues(alpha: 0.35), blurRadius: 16)],
          ),
          child: const Center(child: Text('🚗', style: TextStyle(fontSize: 20))),
        ),
        const SizedBox(width: 12),
        Expanded(
          child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
            const Text(
              'MyDrive Assistant',
              style: TextStyle(color: _kText, fontSize: 16, fontWeight: FontWeight.w800, letterSpacing: -0.3),
            ),
            const SizedBox(height: 3),
            _StatusPill(label: statusLabel, color: statusColor),
          ]),
        ),
        // Speaker toggle
        GestureDetector(
          onTap: _toggleSpeaker,
          child: Container(
            padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 6),
            decoration: BoxDecoration(
              color: _speakerMode ? _kAccent.withValues(alpha: 0.2) : _kPanel,
              borderRadius: BorderRadius.circular(20),
              border: Border.all(color: _speakerMode ? _kAccent : _kBorder),
            ),
            child: Row(
              mainAxisSize: MainAxisSize.min,
              children: [
                Icon(
                  _speakerMode ? Icons.volume_up : Icons.volume_off,
                  color: _speakerMode ? _kAccent : _kMuted,
                  size: 16,
                ),
                const SizedBox(width: 4),
                Text(
                  _speakerMode ? 'ON' : 'OFF',
                  style: TextStyle(
                    color: _speakerMode ? _kAccent : _kMuted,
                    fontSize: 12, fontWeight: FontWeight.w600,
                  ),
                ),
              ],
            ),
          ),
        ),
        const SizedBox(width: 8),
        // Interrupt
        GestureDetector(
          onTap: _sendInterrupt,
          child: Container(
            padding: const EdgeInsets.all(8),
            decoration: BoxDecoration(
              color: _kPanel,
              borderRadius: BorderRadius.circular(20),
              border: Border.all(color: _kBorder),
            ),
            child: const Icon(Icons.stop_rounded, color: _kRed, size: 18),
          ),
        ),
        const SizedBox(width: 8),
        // New chat
        TextButton(
          onPressed: _newChat,
          style: TextButton.styleFrom(
            padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 8),
            backgroundColor: _kPanel,
            side: const BorderSide(color: _kBorder),
            shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(20)),
          ),
          child: const Text('New',
              style: TextStyle(color: _kMuted, fontSize: 12, fontWeight: FontWeight.w600)),
        ),
      ]),
    );
  }

  Widget _buildMessageList() {
    final count = _messages.length + (_showThinking ? 1 : 0);

    if (count == 0) {
      return Center(
        child: Column(mainAxisSize: MainAxisSize.min, children: [
          Container(
            width: 64, height: 64,
            decoration: BoxDecoration(
              gradient: const LinearGradient(
                colors: [_kAccent, _kAccent2],
                begin: Alignment.topLeft, end: Alignment.bottomRight,
              ),
              borderRadius: BorderRadius.circular(20),
              boxShadow: [BoxShadow(color: _kAccent.withValues(alpha: 0.25), blurRadius: 24)],
            ),
            child: const Center(child: Text('🚗', style: TextStyle(fontSize: 32))),
          ),
          const SizedBox(height: 16),
          const Text('MyDrive Assistant',
              style: TextStyle(color: _kText, fontSize: 18, fontWeight: FontWeight.w800)),
          const SizedBox(height: 6),
          const Text('Hold mic to speak, or type a message',
              style: TextStyle(color: _kMuted, fontSize: 13)),
        ]),
      );
    }

    return ListView.builder(
      controller: _scrollCtrl,
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
      itemCount: count,
      itemBuilder: (context, i) {
        if (_showThinking && i == count - 1) return const _ThinkingBubble();
        return _MessageBubble(message: _messages[i]);
      },
    );
  }

  Widget _buildInputBar() {
    return Container(
      color: _kSurface,
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 10),
      child: Row(crossAxisAlignment: CrossAxisAlignment.end, children: [
        // Mic button — tap once to start, tap again to stop
        GestureDetector(
          onLongPressStart: (_) => _startRecording(),
          onLongPressEnd:   (_) => _stopRecording(),
          onTapDown:        (_) => _startRecording(),
          onTapUp:          (_) => _stopRecording(),
          child: AnimatedBuilder(
            animation: _pulseAnim,
            builder: (_, child) => Transform.scale(
              scale: _isRecording ? _pulseAnim.value : 1.0,
              child: child,
            ),
            child: Container(
              width: 46, height: 46,
              decoration: BoxDecoration(
                shape: BoxShape.circle,
                color: _isRecording ? _kRed : _kAccent,
                boxShadow: [BoxShadow(
                  color: (_isRecording ? _kRed : _kAccent).withValues(alpha: 0.4),
                  blurRadius: _isRecording ? 20 : 10,
                  spreadRadius: 1,
                )],
              ),
              child: Icon(
                _isRecording ? Icons.stop_rounded : Icons.mic_rounded,
                color: Colors.white, size: 22,
              ),
            ),
          ),
        ),
        const SizedBox(width: 10),
        Expanded(
          child: ConstrainedBox(
            constraints: const BoxConstraints(maxHeight: 120),
            child: TextField(
              controller: _textCtrl,
              focusNode: _textFocus,
              maxLines: null,
              style: const TextStyle(color: _kText, fontSize: 15),
              onSubmitted: (_) => _sendText(),
              decoration: InputDecoration(
                hintText: 'Type a message…',
                hintStyle: TextStyle(color: _kMuted.withValues(alpha: 0.7), fontSize: 15),
                filled: true,
                fillColor: _kPanel,
                contentPadding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
                border: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(22),
                  borderSide: const BorderSide(color: _kBorder),
                ),
                enabledBorder: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(22),
                  borderSide: const BorderSide(color: _kBorder),
                ),
                focusedBorder: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(22),
                  borderSide: const BorderSide(color: _kAccent, width: 1.5),
                ),
              ),
            ),
          ),
        ),
        const SizedBox(width: 10),
        GestureDetector(
          onTap: _sendText,
          child: Container(
            width: 46, height: 46,
            decoration: BoxDecoration(
              shape: BoxShape.circle,
              color: _kAccent,
              boxShadow: [BoxShadow(
                color: _kAccent.withValues(alpha: 0.35), blurRadius: 12, spreadRadius: 1,
              )],
            ),
            child: const Icon(Icons.send_rounded, color: Colors.white, size: 20),
          ),
        ),
      ]),
    );
  }
}

// ── Status pill ────────────────────────────────────────────────────────────────
class _StatusPill extends StatefulWidget {
  final String label;
  final Color  color;
  const _StatusPill({required this.label, required this.color});
  @override
  State<_StatusPill> createState() => _StatusPillState();
}

class _StatusPillState extends State<_StatusPill> with SingleTickerProviderStateMixin {
  late AnimationController _blink;
  @override
  void initState() {
    super.initState();
    _blink = AnimationController(vsync: this, duration: const Duration(milliseconds: 700))
      ..repeat(reverse: true);
  }
  @override
  void dispose() { _blink.dispose(); super.dispose(); }
  @override
  Widget build(BuildContext context) {
    return Row(mainAxisSize: MainAxisSize.min, children: [
      FadeTransition(
        opacity: _blink,
        child: Container(
          width: 6, height: 6,
          decoration: BoxDecoration(
            shape: BoxShape.circle,
            color: widget.color,
            boxShadow: [BoxShadow(color: widget.color.withValues(alpha: 0.6), blurRadius: 4)],
          ),
        ),
      ),
      const SizedBox(width: 5),
      Text(widget.label,
          style: TextStyle(color: widget.color, fontSize: 11, letterSpacing: 0.3)),
    ]);
  }
}

// ── Thinking bubble ────────────────────────────────────────────────────────────
class _ThinkingBubble extends StatefulWidget {
  const _ThinkingBubble();
  @override
  State<_ThinkingBubble> createState() => _ThinkingBubbleState();
}

class _ThinkingBubbleState extends State<_ThinkingBubble> with SingleTickerProviderStateMixin {
  late AnimationController _ctrl;
  @override
  void initState() {
    super.initState();
    _ctrl = AnimationController(vsync: this, duration: const Duration(milliseconds: 1200))
      ..repeat();
  }
  @override
  void dispose() { _ctrl.dispose(); super.dispose(); }

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 12),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.start,
        children: [
          Container(
            padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 14),
            decoration: BoxDecoration(
              color: _kAiBg,
              borderRadius: const BorderRadius.only(
                topLeft: Radius.circular(14), topRight: Radius.circular(14),
                bottomRight: Radius.circular(14), bottomLeft: Radius.circular(4),
              ),
              border: Border.all(color: _kBorder),
            ),
            child: Row(
              mainAxisSize: MainAxisSize.min,
              children: List.generate(3, (i) {
                return AnimatedBuilder(
                  animation: _ctrl,
                  builder: (_, __) {
                    final t  = ((_ctrl.value * 3) - i).clamp(0.0, 1.0);
                    final dy = -5.0 * (t < 0.5 ? t * 2 : (1 - t) * 2);
                    return Transform.translate(
                      offset: Offset(0, dy),
                      child: Container(
                        width: 7, height: 7,
                        margin: const EdgeInsets.symmetric(horizontal: 3),
                        decoration: const BoxDecoration(shape: BoxShape.circle, color: _kMuted),
                      ),
                    );
                  },
                );
              }),
            ),
          ),
        ],
      ),
    );
  }
}

// ── Message bubble ─────────────────────────────────────────────────────────────
class _MessageBubble extends StatelessWidget {
  final ChatMessage message;
  const _MessageBubble({required this.message});

  bool get _isUser => message.role == MessageRole.user;

  @override
  Widget build(BuildContext context) {
    return TweenAnimationBuilder<double>(
      tween: Tween(begin: 0.0, end: 1.0),
      duration: const Duration(milliseconds: 250),
      curve: Curves.easeOut,
      builder: (_, v, child) => Opacity(
        opacity: v,
        child: Transform.translate(offset: Offset(0, 8 * (1 - v)), child: child),
      ),
      child: Padding(
        padding: const EdgeInsets.only(bottom: 12),
        child: Column(
          crossAxisAlignment: _isUser ? CrossAxisAlignment.end : CrossAxisAlignment.start,
          children: [
            if (message.kind == MessageKind.toolCall)
              _ToolCallCard(data: message.toolData ?? {})
            else
              _buildBubble(context),
            const SizedBox(height: 4),
            if (_isUser) _buildMeta(),
          ],
        ),
      ),
    );
  }

  Widget _buildBubble(BuildContext context) {
    return ConstrainedBox(
      constraints: BoxConstraints(maxWidth: MediaQuery.of(context).size.width * 0.75),
      child: Container(
        padding: const EdgeInsets.symmetric(horizontal: 15, vertical: 11),
        decoration: BoxDecoration(
          color: _isUser ? _kUserBg : _kAiBg,
          borderRadius: BorderRadius.only(
            topLeft:     const Radius.circular(14),
            topRight:    const Radius.circular(14),
            bottomLeft:  _isUser ? const Radius.circular(14) : const Radius.circular(4),
            bottomRight: _isUser ? const Radius.circular(4)  : const Radius.circular(14),
          ),
          border: Border.all(color: _isUser ? _kAccent.withValues(alpha: 0.25) : _kBorder),
        ),
        child: Text(message.text,
            style: const TextStyle(color: _kText, fontSize: 15, height: 1.55)),
      ),
    );
  }

  Widget _buildMeta() {
    final isVoice = message.kind == MessageKind.voice;
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 4),
      child: Row(mainAxisSize: MainAxisSize.min, children: [
        Icon(isVoice ? Icons.mic_rounded : Icons.keyboard_rounded, size: 10, color: _kMuted),
        const SizedBox(width: 4),
        Text(isVoice ? 'Voice' : 'Text',
            style: const TextStyle(color: _kMuted, fontSize: 10)),
      ]),
    );
  }
}

// ── Tool call card ─────────────────────────────────────────────────────────────
class _ToolCallCard extends StatelessWidget {
  final Map<String, dynamic> data;
  const _ToolCallCard({required this.data});

  static const _meta = {
    'request_roadside_assistance': (label: '🔧 Roadside Assistance', color: _kAmber,  bg: Color(0x0FFFB830)),
    'request_tow_truck':           (label: '🚛 Tow Truck',           color: _kRed,    bg: Color(0x0FFF4D6D)),
    'search_spare_parts':          (label: '🔩 Spare Parts Search',  color: _kAccent, bg: Color(0x0F6C63FF)),
    'book_garage_service':         (label: '🏪 Garage Service',      color: _kGreen,  bg: Color(0x0F00E5A0)),
  };

  @override
  Widget build(BuildContext context) {
    final toolName = data['tool']   as String? ?? '';
    final args     = data['args']   as Map<String, dynamic>? ?? {};
    final result   = data['result'] as Map<String, dynamic>? ?? {};
    final m        = _meta[toolName];
    final color    = m?.color ?? _kMuted;
    final bg       = m?.bg    ?? const Color(0x0F5A5A72);
    final label    = m?.label ?? toolName;

    return Container(
      constraints: const BoxConstraints(maxWidth: 340),
      decoration: BoxDecoration(
        color: _kAiBg,
        borderRadius: const BorderRadius.only(
          topLeft: Radius.circular(14), topRight: Radius.circular(14),
          bottomRight: Radius.circular(14), bottomLeft: Radius.circular(4),
        ),
        border: Border.all(color: color.withValues(alpha: 0.3)),
      ),
      child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
        Container(
          padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 10),
          decoration: BoxDecoration(
            color: bg,
            borderRadius: const BorderRadius.only(
              topLeft: Radius.circular(14), topRight: Radius.circular(14),
            ),
          ),
          child: Row(children: [
            Expanded(child: Text(label,
                style: TextStyle(color: color, fontSize: 12, fontWeight: FontWeight.w700))),
            Container(
              padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 3),
              decoration: BoxDecoration(
                color: _kGreen.withValues(alpha: 0.1),
                borderRadius: BorderRadius.circular(20),
                border: Border.all(color: _kGreen.withValues(alpha: 0.3)),
              ),
              child: const Row(mainAxisSize: MainAxisSize.min, children: [
                Icon(Icons.check_circle, size: 8, color: _kGreen),
                SizedBox(width: 4),
                Text('triggered', style: TextStyle(color: _kGreen, fontSize: 9)),
              ]),
            ),
          ]),
        ),
        Padding(
          padding: const EdgeInsets.all(12),
          child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
            if (args.isNotEmpty) ...[
              _kv('args', _fmt(args), color),
              const SizedBox(height: 6),
            ],
            if (result.isNotEmpty) _kv('result', _fmt(result), _kGreen),
          ]),
        ),
      ]),
    );
  }

  Widget _kv(String label, String value, Color labelColor) {
    return RichText(text: TextSpan(children: [
      TextSpan(text: '$label: ',
          style: TextStyle(color: labelColor, fontSize: 12, fontWeight: FontWeight.w600)),
      TextSpan(text: value,
          style: const TextStyle(color: _kMuted, fontSize: 12)),
    ]));
  }

  String _fmt(Map<String, dynamic> m) =>
      m.entries.map((e) => '${e.key}: ${e.value}').join(', ');
}