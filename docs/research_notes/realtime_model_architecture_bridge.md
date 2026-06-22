# Realtime model architecture for Sinomo

## Question

What model architecture should Sinomo consider for the realtime part of the system, and how should slower LLMs be bridged with fast audio/video models?

## Short answer

Sinomo should not put a slow frontier LLM directly on the realtime audio/video path. The realtime part should be a small set of always-on streaming models and policy heads that maintain hot state, detect salience, decide timing, and produce compact event packets. Slower LLMs should consume those packets asynchronously through the event/world-state layer, then return plans, answers, memory updates, or action proposals.

The practical architecture is:

```text
Live audio/video/screen
  -> fast streaming perception models
  -> event + hot-state blackboard
  -> trigger / turn-taking policy
  -> realtime speech or UI feedback
  -> interactive LLM for ordinary turns
  -> slow LLM / planner for deeper reasoning jobs
  -> event log, memory, actions, and revised user-facing outputs
```

The bridge is the event contract, not a shared giant context window.

## Recommended architecture

### 1. Fast streaming perception layer

This layer is always on and causal. It should run even while another model is answering, using a tool, or planning.

Audio responsibilities:

- VAD and endpointing.
- Wake/attention cues.
- Streaming ASR or speech-token model.
- Speaker diarization when useful.
- Prosody and interruption cues.
- Audio event detection for non-speech events.

Video/screen responsibilities:

- Frame sampling and scene-change detection.
- OCR and UI element detection for screen use.
- Object/action/event detection for camera streams.
- Short rolling video embeddings.
- Salience and change detection.

Good model families to consider:

- Streaming ASR: Whisper-style streaming wrappers, NVIDIA Riva/Parakeet-style ASR, Conformer/Emformer-family streaming ASR.
- Low-latency speech-to-speech: OpenAI Realtime API, Gemini Live API, Moshi-style full-duplex speech models, Qwen2.5-Omni/MiniCPM-o/VITA-style open omni models.
- Streaming video understanding: VideoLLM-online, Flash-VStream, VideoStreaming, OmniLive-style memory-based video models.
- Utility vision: OCR, CLIP/SigLIP embeddings, YOLO/RT-DETR-style detectors, UI-specific detectors, small temporal action heads.

This layer should emit structured observations, not prose.

Example events:

```json
{
  "kind": "speech.partial",
  "span_ms": [12140, 12880],
  "payload": {
    "text": "wait stop",
    "is_final": false
  },
  "confidence": 0.91,
  "salience": 0.96,
  "routing": {
    "latency_class": "reactive"
  }
}
```

```json
{
  "kind": "vision.screen.change",
  "span_ms": [41200, 41800],
  "payload": {
    "summary": "modal dialog appeared",
    "ocr": ["Delete project?", "Cancel", "Delete"],
    "entities": ["ui.dialog.confirmation", "action.delete"]
  },
  "confidence": 0.87,
  "salience": 0.93,
  "routing": {
    "latency_class": "interactive"
  }
}
```

### 2. Hot-state blackboard

Maintain a compact, mutable state packet that fast models and controllers can update without waiting for the event database.

Suggested contents:

- Last 2-10 seconds of VAD/ASR state.
- Last few visual observations and screen OCR deltas.
- Current speaker, user activity, and whether Sinomo is speaking.
- Current task and active action target.
- Interruptibility and cancellation state.
- Top salient entities and unresolved questions.

This should be local and in-memory first. Durable event logging can happen slightly behind the hot path.

### 3. Trigger and turn-taking policy

The trigger model decides when Sinomo should act, speak, stay silent, interrupt, or escalate. This should be separate from the model that writes the content.

Start with rules plus a tiny learned policy later.

Inputs:

- Audio activity, speech partials/finals, and interruption cues.
- Visual changes and safety events.
- User state and current output state.
- Confidence, salience, urgency, and novelty.
- Whether a slow job is pending.

Outputs:

- `stay_silent`
- `continue_listening`
- `interrupt_output`
- `acknowledge_now`
- `ask_clarification`
- `enqueue_interactive_turn`
- `enqueue_deliberation_job`
- `raise_alert`

This is the key bridge point: the fast path can decide timing in 50-150 ms, while content can arrive later from a larger model.

### 4. Interactive loop

The interactive loop handles normal user-facing behavior. It should receive bounded context packets assembled from hot state, recent event summaries, and targeted retrieval.

Recommended latency target: 300 ms to 2 seconds, depending on modality and backend.

This loop can use:

- A hosted realtime model for speech-native interaction.
- A faster text LLM with ASR/TTS around it.
- A local small/medium LLM for routing and short responses.
- Tool calls with strict depth and timeout limits.

The interactive loop should produce:

- Short answers.
- Clarifying questions.
- Immediate action proposals.
- Tool/action requests.
- Deliberation jobs when deeper reasoning is needed.

### 5. Slow deliberative loop

The slow loop should be a job system. It is not part of the realtime critical path.

Use it for:

- Deeper planning.
- Ambiguity resolution.
- Cross-source research.
- Long task decomposition.
- Memory consolidation.
- Candidate generation and evaluation.
- Multi-step tool workflows.

The slow loop subscribes to selected events and context packets, works asynchronously, and emits results back into the event stream.

Important design rule: slow results are revisions, continuations, or plans. They should not block listening, barge-in, safety handling, or short acknowledgements.

## The bridge pattern

### Event packets instead of raw context

Slow LLMs should not be fed raw continuous streams. They should receive compact packets:

```json
{
  "packet_kind": "interactive_context",
  "session_id": "sess_...",
  "time": "2026-04-24T18:00:00Z",
  "recent_user_speech": "Can you check whether that error is serious?",
  "current_screen": {
    "summary": "Rust compiler error in terminal",
    "ocr": ["error[E0308]: mismatched types", "expected String, found &str"]
  },
  "active_task": "debug local build",
  "salient_events": ["evt_...", "evt_..."],
  "evidence_refs": ["frame://...", "audio://..."],
  "allowed_actions": ["read_file", "run_tests", "explain"],
  "latency_budget_ms": 1200
}
```

For slow jobs:

```json
{
  "packet_kind": "deliberation_job",
  "goal": "diagnose the build failure and propose a fix",
  "trigger_event_id": "evt_...",
  "context_refs": ["ctx_...", "mem_..."],
  "evidence_refs": ["terminal_snapshot://..."],
  "deadline": null,
  "may_use_tools": true,
  "return_channel": "event_stream"
}
```

### Provisional responses

The fast/interactive layer can produce a provisional response while a slow model works.

Examples:

- "I see it. Checking the error now."
- "That looks like a confirmation dialog. I will wait before acting."
- "I can answer briefly now, and I am also doing the deeper pass."

The slow loop can later emit:

- a correction,
- a fuller answer,
- an action plan,
- or a memory update.

### Cancellation and supersession

Every slow job should be cancellable or supersedable by new realtime events.

Examples:

- User says "never mind".
- Screen changes and the old plan no longer applies.
- Safety policy blocks the planned action.
- A newer transcript corrects an earlier partial.

This requires job records to track:

- trigger event,
- context version,
- active evidence refs,
- cancellation token,
- superseded_by event/job,
- final disposition.

### Evidence handles

The LLM should be able to request evidence when needed, but should not automatically receive all raw media.

Evidence handle examples:

- `audio://node/clip_id`
- `frame://node/session/frame_id`
- `screen://node/snapshot_id`
- `event://evt_id`
- `memory://record_id`

This lets Sinomo keep raw data local and expose only the right amount of context.

## Candidate model stack options

### Option A: Hosted realtime front-end plus Sinomo runtime

Use a hosted realtime speech/video-capable model for low-latency conversation, while Sinomo owns event logging, trigger policy, memory, tool execution, and slow reasoning orchestration.

Good for:

- Fast MVP.
- Natural speech interaction.
- Barge-in and full-duplex behavior.
- Avoiding early local model-serving complexity.

Risk:

- Less control over internals.
- Cost and privacy concerns.
- Vendor-specific API behavior.

Likely stack:

- OpenAI Realtime API or Gemini Live API for realtime voice/video session.
- Sinomo gateway mirrors session events into the local event log.
- Fast local perception still runs for safety, UI state, and action grounding.
- Slow LLM/planner can be remote or local.

### Option B: Local modular realtime stack

Use separate local models for VAD, ASR, vision, OCR, object detection, embeddings, TTS, and a small policy model. Use a local or remote LLM only for interactive/deliberative content.

Good for:

- Privacy and self-hosting.
- Fine-grained control.
- Incremental improvement.
- Works well with Sinomo's event architecture.

Risk:

- More integration work.
- Speech naturalness and turn-taking may lag hosted realtime systems.

Likely stack:

- Silero/WebRTC VAD.
- Streaming ASR such as Riva/Parakeet/Whisper-wrapper.
- Piper/XTTS/other local TTS or hosted TTS.
- OCR + screen parser.
- Small detector/embedding models.
- Local small LLM for routing.
- Larger slow LLM for planning and synthesis.

### Option C: Open omni model as the realtime core

Use an open audio/video/text model such as Moshi-style, Qwen2.5-Omni-style, MiniCPM-o-style, VITA-style, or OmniLive-style systems for the realtime interface.

Good for:

- Research alignment with end-to-end omni assistants.
- Potentially lower latency than ASR -> LLM -> TTS.
- Better handling of paralinguistic speech cues.

Risk:

- Open omni systems can be harder to deploy reliably.
- Tool/action grounding usually still needs external orchestration.
- Video understanding may be weaker than specialized perception modules.

Likely role:

- Use the omni model as one realtime participant, not the whole brain.
- Keep Sinomo's event layer, trigger policy, memory, and action sandbox outside it.

## Recommended path for Sinomo

Start with a hybrid of Option A and Option B:

1. Build the event bridge and hot-state packet contract first.
2. Use modular local fast perception for safety, screen state, and structured events.
3. Use a hosted realtime model for speech UX if speed matters immediately.
4. Route ordinary turns through the agent runtime context assembler.
5. Route hard/slow work to an async deliberation service.
6. Later, test open omni models behind the same `sinomo-voice` / `sinomo-fusion` interfaces.

This avoids betting Sinomo on one model vendor or one end-to-end model architecture.

## Model responsibilities by latency

| Layer | Latency target | Model type | Should do | Should not do |
|---|---:|---|---|---|
| VAD / interruption | 10-50 ms | rules/tiny model | speech activity, barge-in, endpointing | semantic reasoning |
| Reactive trigger | 50-150 ms | rules/small classifier | speak/wait/interrupt/escalate | long-form generation |
| Streaming perception | 50-500 ms | ASR, OCR, detector, video encoder | observations and salience | final task planning |
| Realtime speech UX | 150-800 ms | realtime speech/omni model | acknowledgements, natural turn-taking | unbounded tool workflows |
| Interactive agent | 300 ms-2 s | fast LLM/VLM | answer, clarify, short actions | long branch search |
| Deliberative agent | 2 s+ | stronger LLM/planner | planning, research, synthesis, memory | hot-path safety or barge-in |

## Implementation implications for Sinomo crates

Add the bridge contract early in `sinomo-core`:

- `PerceptionEvent`
- `HotStateSnapshot`
- `ContextPacket`
- `TriggerDecision`
- `AgentSession`
- `TurnContext`
- `DeliberationJob`
- `ModelCapability`
- `EvidenceRef`
- `JobCancellation`

Suggested service responsibilities:

- `sinomo-gateway`: normalize streams and timestamps.
- `sinomo-audio`: VAD/ASR/prosody/audio events.
- `sinomo-video`: frame sampling/OCR/vision events.
- `sinomo-fusion`: align audio, video, transcript, and screen events.
- Future trigger policy service: timing policy and interrupt handling.
- `sinomo-agent`: package-driven agent loop over assembled turn context.
- `sinomo-deliberation`: async slow LLM jobs.
- `sinomo-voice`: realtime speech session, TTS, barge-in.
- `sinomo-events`: canonical log and replay.

## Evaluation targets

Measure this architecture on:

- Input-to-event latency.
- Speech interruption latency.
- False interrupt rate.
- Missed urgent event rate.
- Time to first acknowledgement.
- Time to useful answer.
- Slow-job supersession/cancellation correctness.
- Event replay fidelity.
- Quality of compressed context packets vs raw transcript/frame context.
- Whether perception continues during slow reasoning.

## Core sources

- OpenAI Realtime API documentation: https://platform.openai.com/docs/guides/realtime
- Google Gemini Live API documentation: https://ai.google.dev/gemini-api/docs/live
- Moshi technical report / project: https://kyutai.org/Moshi.pdf and https://github.com/kyutai-labs/moshi
- GPT-4o system card: https://openai.com/index/gpt-4o-system-card/
- VideoLLM-online / LIVE: https://github.com/showlab/VideoLLM-online
- Flash-VStream: https://arxiv.org/abs/2406.08085
- StreamingBench: https://arxiv.org/abs/2411.03628
- OmniLive: https://arxiv.org/abs/2412.09596
- StreamingLLM: https://openreview.net/forum?id=NG7sS51zVF
- Mamba: https://openreview.net/forum?id=AL1fq05o7H
