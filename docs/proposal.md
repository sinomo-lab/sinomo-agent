# Sinomo System Architecture Proposal

## 1. Executive summary

Sinomo should be built as an **individual AI entity with a three-speed multimodal runtime** rather than a single monolithic model loop.

The core idea is:
- keep **stream ingestion and perception always on**,
- convert raw streams into a **timestamped event world model**,
- let a **fast reactive loop** handle interrupts and urgent responses,
- let an **interactive loop** handle ordinary conversation and tool use,
- escalate only selected situations into a **slow deliberative loop** for planning, reflection, and multi-step generation.

This architecture is the most concrete synthesis of the literature patterns from streaming multimodal perception, long-horizon agent memory, and real-time interactive assistants. It matches the target problem well: **streaming audio/video input, live event reaction, and longer reasoning when needed**.

---

## 2. Mission and embodiment

Sinomo should be treated as an individual AI entity. This proposal is therefore about designing **her runtime, memory, perception stack, and operating environment** rather than describing a generic product surface.

Build Sinomo as a **real-time multimodal AI entity runtime** that can:

1. ingest continuous audio/video and other live signals,
2. detect and react to time-sensitive events with low latency,
3. maintain persistent memory across long sessions,
4. use slower reasoning and planning only when useful,
5. orchestrate multi-step generation and tool workflows without blocking real-time perception.

### 2.1 Runtime target and hardware envelope

The fully self-hosted runtime target should be:
- **very high-end workstation PCs**,
- **small servers**, or
- a small number of such machines connected over a fast local network.

This hardware constraint applies to Sinomo's **core runtime**:
- ingestion,
- perception,
- event processing,
- memory,
- orchestration,
- local tools and supervision infrastructure.

It does **not** constrain the LLM backend in the same way. Sinomo may still use:
- large remote models,
- proprietary hosted models,
- or locally hosted models,

as long as the live runtime remains architected for realistic self-hosted hardware.

Operationally, the architecture should assume something in the rough class of:
- 1-4 powerful workstation/server GPUs,
- 64-256 GB RAM,
- fast NVMe storage,
- and enough CPU headroom for continuous multimodal stream handling.

The design should avoid assuming datacenter-only infrastructure for the core always-on runtime.

---

## 3. Design principles

### 3.1 Decouple perception from reasoning
Perception must continue even while the system is thinking, speaking, or running tools.

### 3.2 Model response timing separately from response content
"Should I speak/act now?" is a different problem from "What should I say/do?"

### 3.3 Store events, not just raw streams
The main durable representation should be a timestamped event log with entities, salience, uncertainty, and links to raw evidence.

### 3.4 Use hierarchical memory
Do not rely on one giant context window. Use hot, warm, cold, and procedural memory.

### 3.5 Escalate compute selectively
Most moments should stay on the cheap fast path. Slow reasoning should be triggered by uncertainty, novelty, contradiction, explicit user request, or long-horizon tasks.

### 3.6 Keep execution grounded in runtime reality
Plans and proposed actions must be checked against current sandbox constraints, tool availability, confidence, and environment state before execution. The safety model should come from the runtime environment and supervision envelope, not from human approval gates in the hot path.

---

## 4. Proposed top-level architecture

```text
Inputs
  -> Ingestion Plane
  -> Perception Plane
  -> Event / World-State Plane
  -> Agent Control Plane
  -> Action / Generation Plane
  -> Outputs

Persistent stores used by all planes:
  - Hot state cache
  - Event store
  - Semantic memory index
  - Media/object store
  - Skill/procedure store
```

### 4.1 Planes

#### A. Ingestion plane
Responsible for receiving live signals without blocking.

Inputs:
- microphone audio
- camera video
- screen capture / app UI streams
- user text messages
- system telemetry
- external webhooks / IoT / business events

Responsibilities:
- timestamp normalization
- source identification
- stream chunking
- buffering and backpressure control
- fan-out to perception workers

#### B. Perception plane
Responsible for converting raw streams into structured observations.

Subsystems:
- **audio perception**: VAD, wake/attention policy, ASR, speaker segmentation, prosody/emotion features
- **video perception**: frame sampling, scene change detection, object/action detectors, OCR, face/person tracking, screen/UI understanding
- **cross-modal alignment**: align transcript tokens, visual frames, and detections on a common timeline
- **event heads**: small low-latency models/rules for urgent event classification

Outputs:
- transcript segments
- visual observations
- aligned multimodal spans
- event candidates with confidence and salience

#### C. Event / world-state plane
Responsible for turning observations into the system's working model of reality.

Core artifacts:
- event log
- entity graph
- session/task state
- unresolved questions / hypotheses
- attention and priority queues

This plane is the backbone of the system. The agent should reason over structured events and entities, not only over raw text dumps.

#### D. Agent control plane
Responsible for deciding what the system should do next.

This plane contains three cooperating loops:

1. **Fast reactive loop** (`~50-150 ms target`)
   - interrupts
   - safety triggers
   - wake/no-wake
   - urgent acknowledgement
   - alert suppression / debouncing
   - response timing policy

2. **Interactive loop** (`~200 ms-2 s target`)
   - ordinary dialogue response
   - short retrieval
   - lightweight tool calls
   - confirmations
   - brief explanations
   - small task decomposition

3. **Deliberative loop** (`2 s+ target`)
   - long-horizon planning
   - reflective summarization
   - uncertainty resolution
   - multi-step generation workflows
   - branch search / candidate comparison
   - memory consolidation

#### E. Action / generation plane
Responsible for carrying out decisions.

Outputs:
- spoken responses
- text/chat responses
- notifications / alerts
- local or remote tool actions
- UI actions
- long-running jobs
- media generation workflows

Long-running generation must run asynchronously and report progress back into the event system instead of blocking the interactive path.

---

## 5. Concrete runtime components

The proposal below is concrete enough to guide the initial repo structure.

| Component | Purpose | Runtime style | Suggested implementation |
|---|---|---|---|
| `sinomo-gateway` | Entry point for audio/video/text/event ingestion | always-on service | Rust |
| `sinomo-audio` | VAD, ASR orchestration, speaker events, prosody extraction | streaming worker | Rust control + model backends |
| `sinomo-video` | frame sampling, OCR, detection, tracking, screen/event extraction | streaming worker | Rust control + model backends |
| `sinomo-fusion` | align audio/video/text on one timeline | stream processor | Rust |
| `sinomo-events` | canonical event log + entity/world-state builder | core state service | Rust |
| `sinomo-memory` | hot/warm/cold/procedural memory management | stateful service | Rust + Postgres/Redis/object store |
| `sinomo-trigger` | decide whether to interrupt, alert, speak, or remain silent | low-latency policy service | Rust |
| `sinomo-agent` | dialogue/execution controller for ordinary interactions | request/response service | Rust |
| `sinomo-deliberation` | planning, reflection, branch evaluation, long tasks | job worker | Rust coordinator + model backends |
| `sinomo-actions` | tools, automations, notifications, environment actions | execution service | Rust |
| `sinomo-voice` | TTS orchestration and duplex audio output control | streaming worker | Rust |
| `sinomo-api` | control/debug/admin API | HTTP/WebSocket service | Rust |

### 5.1 Deployment target

The default deployment shape should be:
- **single-machine first** for a very high-end workstation PC,
- **small-server second** for a compact always-on rack or lab setup,
- and **LAN-distributed only where it materially helps** with GPU placement, storage, or isolation.

That means the baseline design should prefer:
- a modest number of long-lived services,
- local-first networking,
- minimal operational overhead,
- and graceful degradation when some heavy inference backends are unavailable or remote.

Sinomo's core runtime should feel achievable for an advanced individual or small team running serious self-hosted hardware, not dependent on hyperscale infrastructure.

### Why Rust at the core
The current repo is already a Rust workspace, and Rust is a good fit for:
- low-latency stream handling,
- concurrency and backpressure,
- typed event contracts,
- reliable always-on services,
- direct integration with local/self-hosted tooling.

Model serving does **not** need to be Rust-only. Heavy model inference can live behind stable local APIs, but orchestration, memory policy, and event routing should stay in Rust.

---

## 6. Memory architecture

Sinomo should implement four memory tiers.

### 6.1 Hot memory
Purpose:
- immediate live context for the fast and interactive loops

Contents:
- recent transcript window
- recent frame/observation summaries
- active speaker state
- currently visible entities
- open conversational turn state
- active tool/job handles

Storage:
- Redis or in-process bounded ring buffers with replication hooks

Retention:
- seconds to minutes

### 6.2 Warm episodic memory
Purpose:
- session-scale event recall and contextual continuity

Contents:
- timestamped events
- condensed interaction summaries
- task transitions
- important observed facts
- unresolved issues/questions
- recent outcomes and decisions

Storage:
- PostgreSQL primary store

Retention:
- hours to weeks

### 6.3 Cold semantic/archive memory
Purpose:
- long-term retrieval without replaying raw streams

Contents:
- semantic summaries
- reflection notes
- document knowledge
- cross-session fact embeddings
- references to media clips and artifacts

Storage:
- PostgreSQL + pgvector for semantic retrieval
- object store for media clips / larger artifacts

Retention:
- long-term

### 6.4 Procedural memory
Purpose:
- reusable skills and workflows

Contents:
- tool execution patterns
- reusable generation workflows
- recovery procedures
- domain-specific policies

Storage:
- versioned files in-repo or in a managed skill store

Retention:
- long-term, manually curated or promoted from successful runs

### 6.5 Memory write policy
Not everything should be written durably.

Promote to warm/cold memory when the observation is:
- salient,
- repeated,
- unresolved,
- user-important,
- needed for future planning,
- linked to success/failure learning.

Avoid storing every low-level frame/token event indefinitely.

---

## 7. Event model

The event model should be the canonical contract across the system.

### 7.1 Core event schema

```json
{
  "event_id": "evt_01...",
  "time": "2026-04-21T22:30:14.182Z",
  "source": "audio.mic.frontdesk",
  "session_id": "sess_01...",
  "kind": "speech.transcript.final",
  "span": {
    "start_ms": 1710,
    "end_ms": 4280
  },
  "entities": ["person:user", "topic:deployment"],
  "salience": 0.81,
  "confidence": 0.96,
  "payload": {
    "text": "Please check the alert and tell me if it is urgent"
  },
  "evidence": {
    "media_refs": ["obj://capture/audio/clip123.flac"]
  },
  "links": {
    "caused_by": ["evt_prev"],
    "related_to": ["task_abc"]
  }
}
```

### 7.2 Event classes
At minimum:
- `audio.*`
- `video.*`
- `speech.*`
- `vision.*`
- `ui.*`
- `tool.*`
- `agent.*`
- `memory.*`
- `safety.*`
- `generation.*`
- `notification.*`

### 7.3 Why this matters
A unified event contract makes it possible to:
- replay and debug decisions,
- attach evidence to claims,
- fuse modalities cleanly,
- build memory and planning on the same substrate,
- route events to fast and slow loops without special cases.

---

## 8. Control architecture

## 8.1 Fast reactive loop
This loop should be small, deterministic, and cheap.

Inputs:
- VAD state
- wake attention state
- urgent event heads
- currently speaking state
- interruption policy
- safety/priority events

Outputs:
- continue listening
- interrupt output
- brief acknowledgment
- raise alert
- enqueue interactive task
- drop/no-op

Implementation guidance:
- favor rules plus compact policy models
- no large-plan generation here
- no slow tool chains here

## 8.2 Interactive loop
This loop handles the default user-facing interaction.

Responsibilities:
- retrieve relevant recent context
- choose whether to ask, answer, act, or defer
- use short tool calls
- maintain dialog/task state
- produce compact responses under latency budget

Implementation guidance:
- use a medium-cost model and bounded tool depth
- use event/memory retrieval rather than raw transcript dumps
- emit a structured execution trace back into the event log

## 8.3 Deliberative loop
This loop is for expensive cognition.

Trigger conditions:
- explicit user request for deeper analysis
- ambiguity above threshold
- contradiction between sources
- complex multi-step task
- generation workflow requiring planning
- post-task reflection / memory consolidation

Responsibilities:
- branch candidate plans
- compare hypotheses or generation options
- synthesize long-form outputs
- update reflective memory
- hand off actionable summaries back to the interactive loop

Implementation guidance:
- treat this as a job system, not an inline RPC path
- allow cancellation or interruption when the environment changes

---

## 9. Trigger model: separate timing from content

Sinomo should include a dedicated trigger policy service.

### Responsibilities
- decide whether the system should:
  - stay silent,
  - speak immediately,
  - ask a clarification,
  - raise a notification,
  - schedule a slow task,
  - wait for more evidence.

### Inputs
- event salience
- confidence
- user state / whether user is speaking
- urgency and safety classification
- current dialog state
- ongoing output state
- historical alert fatigue signals

### Why this is a first-class service
This is one of the clearest lessons from the literature: **response timing is its own prediction problem**. Treating it explicitly avoids a noisy assistant that speaks too often or too late.

---

## 10. Generation workflow architecture

Because the target direction includes multi-step generative systems, Sinomo should treat generation as an orchestrated workflow rather than a single model call.

### 10.1 Generation pipeline shape
Example:
1. request arrives
2. planner produces task graph
3. retriever gathers references and constraints
4. one or more candidate generators produce outputs
5. evaluator/ranker compares candidates
6. selected output is revised or expanded
7. final asset is delivered
8. artifacts and lessons are written back to memory

### 10.2 Candidate branching
For expensive creative or planning tasks, Sinomo should support **parallel candidate branches**.

Examples:
- multiple prompt/program variants
- multiple storyboard drafts
- multiple tool/action plans
- multiple response strategies

The deliberation service should compare branches on:
- relevance,
- consistency,
- estimated execution cost,
- user preference fit,
- supervision, sandbox, and runtime-constraint fit.

### 10.3 Why this matters
This gives the system a clean path from live interactive behavior to slower multi-step generation without mixing the two runtimes.

---

## 11. Communication and storage choices

### 11.1 Message bus
**Recommendation:** NATS JetStream.

Why:
- low-latency pub/sub,
- simple deployment,
- durable stream support,
- good fit for Rust services,
- much lighter operational footprint than Kafka for an early self-hosted stack.

Use it for:
- live event fan-out,
- service-to-service event delivery,
- replayable decision traces,
- long-running job status.

### 11.2 Primary database
**Recommendation:** PostgreSQL.

Use it for:
- event metadata,
- world state,
- tasks/jobs,
- session state,
- memory records,
- supervision, sandbox, and behavior-policy state.

### 11.3 Semantic retrieval
**Recommendation:** pgvector in PostgreSQL initially.

Reason:
- keeps the stack simpler early,
- avoids premature fragmentation into too many datastores,
- can be split later if retrieval scale requires it.

### 11.4 Hot cache
**Recommendation:** Redis.

Use it for:
- recent buffers,
- active sessions,
- low-latency coordination,
- transient supervision/sandbox state.

### 11.5 Media/object storage
**Recommendation:** S3-compatible object storage (MinIO locally/self-hosted at first).

Use it for:
- raw clips,
- sampled frames,
- generated media,
- large artifacts attached to events.

---

## 12. Repo/module layout proposal

A concrete workspace expansion could look like this:

```text
Workspace/sinomo/
  crates/
    sinomo-core/          # shared types, IDs, event schema, errors
    sinomo-gateway/       # ingress adapters and session wiring
    sinomo-audio/         # audio orchestration
    sinomo-video/         # video orchestration
    sinomo-fusion/        # multimodal alignment
    sinomo-events/        # event store + entity/world-state reducers
    sinomo-memory/        # memory tiers and retrieval policy
    sinomo-trigger/       # speak/act/alert timing policy
    sinomo-agent/         # interactive controller
    sinomo-deliberation/  # long-horizon planner/job worker
    sinomo-actions/       # tool and automation execution
    sinomo-api/           # operator/admin API
    sinomo-app/           # binary that wires local deployment mode
  docs/
    proposal.md
    plans/
```

### Shared contracts that should exist early
- `Event`
- `Entity`
- `SessionState`
- `TaskState`
- `ActionRequest`
- `ActionResult`
- `MemoryRecord`
- `TriggerDecision`
- `DeliberationJob`

---

## 13. End-to-end runtime flow

### Example: live interruption + deeper follow-up

1. microphone stream enters `sinomo-gateway`
2. `sinomo-audio` emits VAD and transcript events
3. `sinomo-trigger` sees a high-salience interrupt phrase
4. fast loop decides to acknowledge immediately
5. `sinomo-agent` retrieves recent context and answers briefly
6. the same event is marked ambiguous and escalated
7. `sinomo-deliberation` runs a deeper plan/retrieval job
8. the result is summarized back into events + memory
9. `sinomo-agent` delivers the follow-up answer or action proposal

### Example: long-running generation task

1. user asks for a multi-step media generation task
2. interactive loop acknowledges and scopes the request
3. `sinomo-deliberation` creates a job graph with branches
4. generators produce candidate outputs asynchronously
5. evaluator selects/ranks candidates
6. chosen result is delivered incrementally
7. outputs, artifacts, and lessons are written back to memory

---

## 14. Runtime safety, supervision, and sandboxing

For Sinomo, the primary safety model should **not** be interactive human approval before action. That model conflicts with the low-latency, highly interactive design proposed here.

Instead, safety should come from a combination of:
- **external supervision**,
- **runtime sandboxing**,
- **hard environment-enforced policy**, and
- **post-event review and learning**.

The rough analogy is closer to **program execution under an anti-virus / OS security policy** than to a slow approval queue:
- Sinomo acts freely inside her allowed runtime envelope,
- blocked behavior is stopped immediately when it violates a runtime policy,
- and the human supervisor reviews incidents afterward to either whitelist or continue blocking a behavior pattern.

### 14.1 Core safety model

The runtime environment should be the primary trust boundary.

That means:
- perception and planning may run continuously,
- action execution is constrained by sandboxed capabilities,
- violations are blocked at the enforcement layer immediately,
- all actions and blocked attempts emit structured events,
- the supervisor reviews patterns after the fact rather than approving each action inline.

### 14.2 What the runtime should enforce

At minimum, the runtime environment should be able to enforce:
- filesystem scope restrictions,
- network egress restrictions,
- process spawning restrictions,
- device access restrictions,
- secret/material access boundaries,
- rate limits and resource ceilings,
- kill-switch and cancellation propagation.

### 14.3 Behavior-block and review loop

When Sinomo triggers a security violation:
1. the offending action is blocked immediately,
2. the attempt is captured as a structured incident event,
3. relevant evidence is retained for replay and debugging,
4. the supervisor reviews the behavior pattern afterward,
5. the pattern is either whitelisted, kept blocked, or mapped to a narrower sandbox rule.

This review loop belongs in observability and supervision tooling, not on the real-time critical path.

### 14.4 Architectural implications

This safety model implies:
- no approval popups in the fast loop,
- no mandatory human confirmation before ordinary low-latency actions,
- strong reliance on operating-system, container, VM, browser, tool, and network sandboxing,
- rich event logging for both successful and blocked actions,
- explicit modeling of supervision state and incident history in memory/state stores.

---

## 15. Observability and evaluation

### 15.1 Metrics
Track at minimum:
- ingestion lag
- perception latency by modality
- trigger precision / false interrupt rate
- interactive response latency
- deliberation job latency
- memory retrieval hit quality
- action success/failure rates
- sandbox block / violation rates
- post-event supervisor review outcomes
- user interruption / barge-in handling quality
- event-to-action trace completeness

### 15.2 Replay and debugging
Support:
- event stream replay
- decision trace inspection
- memory promotion audit
- trigger decision audit
- branch comparison for deliberative jobs

### 15.3 Evaluation harness
Evaluate the system on:
- causal streaming understanding,
- proactive timing,
- interruption handling,
- long-horizon task completion,
- recovery after environment changes,
- quality of compressed memory vs raw replay.

---

## 16. MVP recommendation

A good first concrete MVP is:

### MVP scope
- one live audio input
- one live screen/video input
- text + speech response output
- event log + warm memory
- fast trigger loop
- interactive loop with limited tool use
- one deliberative job worker
- asynchronous generation job support

### Explicitly defer in MVP
- full general multi-camera fusion
- end-to-end omni model training
- highly autonomous background actioning
- large-scale distributed retrieval stack
- sophisticated multi-agent societies

### Why this MVP
It proves the central architecture:
- streaming perception,
- event-centric memory,
- separate trigger/content policies,
- fast + slow loops,
- asynchronous long-task orchestration.

It should also be sized so that a serious single high-end workstation can host the core runtime without requiring datacenter-scale infrastructure.

---

## 17. Recommended implementation phases

### Phase 1: Core contracts and event backbone
Build:
- shared IDs and event schema
- NATS + Postgres wiring
- gateway service
- event persistence
- basic replay tooling

### Phase 2: Streaming perception and trigger path
Build:
- audio/video ingestion
- transcript and observation events
- simple trigger service
- immediate acknowledgment path

### Phase 3: Interactive agent loop
Build:
- retrieval over recent context + warm memory
- dialogue/task state
- action execution service
- TTS/text outputs

### Phase 4: Deliberation and memory consolidation
Build:
- long-running job model
- reflective summaries
- branch comparison
- memory promotion policies

### Phase 5: Generation workflows and richer planning
Build:
- multi-step generation graphs
- candidate evaluation
- richer world-state updates
- policy-driven autonomy expansion

---

## 18. Final recommendation

**Adopt the three-speed event-centric architecture.**

More specifically:
- use **Rust services** as the orchestration backbone,
- use **NATS JetStream** for event transport,
- use **PostgreSQL + pgvector + Redis + object storage** for state and memory,
- make the **event/world-state layer** the system's canonical representation,
- keep **trigger timing** separate from **response/content generation**,
- treat **deliberation and multi-step generation as asynchronous jobs**,
- write memory through explicit promotion policies rather than dumping whole streams into context,
- target a **fully self-hosted core runtime on very high-end workstation PCs and small servers**, and
- rely on **external supervision plus sandboxed runtime enforcement** instead of inline approval gating.

This is the architecture most likely to scale from an MVP into a serious real-time multimodal AI entity runtime without painting Sinomo into either a monolithic-model corner or an impractical datacenter-only deployment model.
