# Sinomo Architecture

## Overview

Sinomo is a distributed, real-time multimodal AI entity runtime built around continuous perception, an event-centric world model, and three cooperating control loops. The system should keep ingestion and perception running at all times, even when slower reasoning, tool use, or generation workflows are in progress.

The foundation is a network of cooperating nodes. Each node may have one or more devices attached, and each node can host perception, memory, control, and action components close to where the relevant signal or action happens. A single workstation deployment should be a special case of the same node graph, not a separate architecture.

The initial architecture favors a Rust workspace with small, focused crates that can run together on one machine first, then distribute across desktops, phones, small servers, and LAN devices as workload, privacy, latency, and hardware needs grow.

## Core Principles

1. **Perception is always on.** Audio, video, screen, text, telemetry, and external events are ingested continuously without waiting for reasoning or generation to finish.
2. **Events are the canonical substrate.** Raw streams are converted into timestamped, structured events with confidence, salience, entities, evidence, and links.
3. **Timing and content are separate decisions.** The system decides when to speak, interrupt, alert, or stay silent independently from what it should say or do.
4. **Reasoning runs at multiple speeds.** Fast reactive decisions, normal interactive responses, and slow deliberative work are handled by separate loops.
5. **Memory is hierarchical.** Hot, warm, cold, and procedural memory are managed explicitly instead of relying on a single large context window.
6. **Safety is enforced by the runtime.** Sandboxes, policy enforcement, resource limits, and observability guard actions without putting human approval on the hot path.
7. **Locality is a first-class policy.** Computation should happen as close as practical to the device, signal, data, or action it affects.
8. **Distribution is native.** Nodes, devices, sessions, events, world-state, and memory must be identity-aware and synchronization-aware from the beginning.
9. **Workspaces are distributed objects.** Agent workspaces, staging areas, and artifacts should be local-first but addressable across nodes through explicit ownership, replication, and write policies.

## Distributed Foundation

Sinomo's world view is fundamentally a network of nodes. A node is a trusted runtime participant that can host services, store state, process events, and execute actions. A device is a concrete input/output or action surface associated with a node.

Examples:

- A desktop node with screen capture, keyboard/mouse control, local files, GPU inference, and desktop app actions.
- A mobile node with microphone, camera, local wake detection, lightweight perception, notifications, and touch UI.
- A home server node with durable storage, memory consolidation, scheduled jobs, and heavier models.
- A dedicated sensor node with cameras, microphones, or IoT events.

The architecture should support a user speaking to Sinomo on a mobile device while Sinomo executes a low-latency action on the desktop. In that flow, mobile audio can be processed locally into transcript, embeddings, or intent-like events, then routed to the desktop node where a local fast reactive model decides and acts against the desktop environment.

This differs from a remote-control model where a centralized agent owns all cognition and devices are thin clients. Sinomo should support distributed cognition and distributed action, with placement chosen by latency, capability, privacy, cost, and current node health.

### Node Model

A node represents a runtime location.

Node responsibilities:

- Host one or more Sinomo services.
- Own local device adapters.
- Advertise capabilities and resource limits.
- Maintain local hot state.
- Participate in event routing and state synchronization.
- Enforce local sandbox and action policy.
- Execute actions against local devices and environments.

Node examples:

- `node.desktop.main`
- `node.mobile.phone`
- `node.server.home`
- `node.sensor.office`

### Device Model

A device represents a concrete source or action target associated with a node.

Device examples:

- `device.mobile.phone.mic`
- `device.mobile.phone.camera`
- `device.desktop.main.screen`
- `device.desktop.main.keyboard`
- `device.desktop.main.filesystem`
- `device.server.home.gpu`

Events must be device-aware. Every event should identify the producing node, producing device when applicable, and intended action target when applicable.

### Distributed State Model

State should be split by locality and durability.

| State | Locality | Sync behavior |
|---|---|---|
| Hot state | Local node first | Replicate only selected summaries, active routing state, and handoff metadata |
| Session state | Multi-node | Synchronize session timeline, participants, active devices, and current task state |
| Event log | Partitioned by origin node/session | Replicate durable event envelopes and evidence references according to policy |
| World-state | Federated | Merge entity, task, environment, and attention updates from multiple nodes |
| Warm memory | Durable home node plus caches | Replicate relevant summaries and retrieval hints to participating nodes |
| Cold memory | Durable storage node first | Retrieve remotely or cache selected shards |
| Procedural memory | Versioned and portable | Sync approved skills, policies, and action recipes by node capability |
| Workspace files | Local-first by object policy | Route writes to owners, replicate selected objects, and confirm distributed writes according to policy |

The system should avoid requiring all raw data to move through a central process. Raw media can remain local when possible; downstream nodes can receive derived events, embeddings, summaries, commands, or evidence handles.

### Locality Policy

The runtime should choose placement using explicit policy, not hardcoded assumptions.

Placement factors:

- Device proximity.
- Action target location.
- Latency budget.
- Available models and accelerators.
- Battery, thermal, and bandwidth limits.
- Privacy and data residency constraints.
- Current node health.
- User preference and supervision policy.

Examples:

- Mobile node performs VAD, wake detection, lightweight ASR, and immediate UI feedback.
- Desktop node performs screen understanding and desktop action execution.
- Server node performs memory consolidation, long deliberation, and large retrieval jobs.
- Any node can request help from another node when local confidence, capability, or resources are insufficient.

### Session Handoff

Sessions should not belong to a single device. A session is a distributed activity with participating nodes and devices.

Session responsibilities:

- Track active user interaction surfaces.
- Track active action targets.
- Preserve conversational continuity across devices.
- Route commands to the node closest to the action target.
- Allow a node to join, leave, sleep, reconnect, or fail without losing the session timeline.

For example, when a user speaks on mobile and asks Sinomo to change something on the desktop, the mobile node can publish a speech/intent event into the session. The desktop node can subscribe because it owns the relevant screen and input devices, then execute or negotiate the action locally.

### Distributed Event Routing

Event routing should support both local processing and cross-node propagation.

Routing rules should consider:

- Event family and salience.
- Producing node and device.
- Session membership.
- Target entity, task, device, or node.
- Privacy classification.
- Required latency.
- Subscriber capabilities.

The same event contract should work for local and distributed cases. Distribution should add routing metadata, not create a separate remote event type.

### Distributed Filesystem

Sinomo should include a distributed filesystem component for agent workspaces, staging areas, generated artifacts, coding tasks, and handoff between nodes.

The DFS should be local-first. A file object can live primarily on one node, be replicated to several nodes, or require distributed write confirmation. The policy is attached to the object, directory, workspace, or mount, not hardcoded by file type.

Core use cases:

- Agent coding workspaces and repository checkouts.
- Task staging directories.
- Generated artifacts and intermediate files.
- Local evidence bundles that can be selectively shared.
- Cross-device handoff where one node edits, another builds/tests, and another reviews.
- Recovery after node sleep, disconnect, or restart.

The DFS is not meant to replace Git, PostgreSQL, or local filesystems. It provides a policy-aware workspace layer above local storage and below agent actions.

#### Filesystem Object Model

Every DFS object should have stable identity and provenance.

Object metadata:

- `object_id`
- `workspace_id`
- `path`
- `kind`: file, directory, symlink, artifact, mount, or tombstone.
- `owner_node_id`
- `current_locations`
- `content_hash`
- `version`
- `policy`
- `created_by_event_id`
- `last_modified_by_event_id`
- `retention`

Paths are user-facing workspace paths. Object IDs are the stable synchronization identity. Renames and moves should update path metadata without losing object history.

#### Filesystem Policies

Initial object policies:

| Policy | Write behavior | Read behavior | Typical use |
|---|---|---|---|
| `local-owned` | Writes route to the owning node | Remote reads use cached copies or fetch from owner | Local checkouts, device-private staging, sensitive evidence |
| `replicated-confirmed` | Writes commit after configured replicas confirm | Reads can use any confirmed replica | Shared workspaces, important generated artifacts |
| `replicated-eventual` | Writes commit locally and replicate asynchronously | Reads may tolerate stale replicas with version metadata | Large artifacts, low-risk generated outputs |
| `append-only` | New versions or chunks only; no in-place overwrite | Reads use immutable versions | Logs, evidence bundles, audit traces |
| `ephemeral-local` | Writes stay local and may be discarded | Local reads only unless explicitly exported | Scratch space, temporary build products |

Policy should also include:

- Minimum replica count.
- Preferred nodes.
- Allowed nodes or trust domains.
- Conflict strategy.
- Encryption requirement.
- Retention and garbage collection.
- Maximum object size.
- Whether raw content may leave the owner node.

#### Write Routing

Write routing must respect object policy.

Rules:

- `local-owned` writes are routed to `owner_node_id`; remote nodes submit write requests instead of mutating cached copies.
- `replicated-confirmed` writes are coordinated by the owner or elected coordinator and are visible after required confirmations.
- `replicated-eventual` writes are locally accepted with version metadata and reconciled later.
- `append-only` writes create new immutable versions or chunks.
- `ephemeral-local` writes are never synchronized unless promoted by an explicit event.

All write operations should emit structured `dfs.*` events with node, workspace, object, version, policy, and outcome metadata.

#### Conflict Handling

The DFS should prefer explicit conflict objects over silent overwrite.

Conflict strategy options:

- Reject concurrent writes.
- Route all writes through owner.
- Last-writer-wins for low-risk objects.
- Create branch/conflict versions.
- Merge through a domain-specific resolver.

For coding workspaces, conflict versions should be visible to the agent and can be reconciled using ordinary code review, diff, and test workflows.

#### Workspace Mounts

A workspace is a named view over DFS objects.

Workspace examples:

- `workspace.user.default`
- `workspace.task.codegen.2026-04-24`
- `workspace.repo.sinomo-agent`
- `workspace.staging.media-job-01`

Mounts may point to:

- A local directory.
- A Git checkout.
- A task staging area.
- A replicated DFS namespace.
- A read-only evidence bundle.

Mount metadata should define policy inheritance, sandbox scope, allowed actions, and cleanup behavior.

## Runtime Planes

```text
Inputs
  -> Distributed Foundation Plane
  -> Ingestion Plane
  -> Perception Plane
  -> Event / World-State Plane
  -> Agent Control Plane
  -> Action / Generation Plane
  -> Outputs

Shared stores:
  - Hot state cache
  - Event store
  - Semantic memory tables/indexes
  - Local media/evidence stores
  - Distributed workspace filesystem
  - Skill/procedure store
```

### Distributed Foundation Plane

Provides the runtime substrate for nodes, devices, sessions, routing, and synchronization.

Responsibilities:

- Node identity and registration.
- Device identity and capability advertisement.
- Session membership and handoff.
- Local and cross-node event routing.
- State synchronization policies.
- Distributed workspace and file-object policies.
- Capability discovery.
- Trust, authentication, and node-local sandbox policy boundaries.

Primary crates: `sinomo-node`, `sinomo-device`, `sinomo-network`, `sinomo-sync`, `sinomo-dfs`.

### Ingestion Plane

Receives live signals and normalizes them into timestamped chunks.

Responsibilities:

- Accept microphone, camera, screen, text, telemetry, webhook, and local system events.
- Normalize source IDs and timestamps.
- Attach node and device identity.
- Chunk streams into bounded units.
- Apply buffering, backpressure, and fan-out.
- Preserve raw evidence references for later replay.

Primary crate: `sinomo-gateway`.

### Perception Plane

Turns raw stream chunks into structured observations.

Responsibilities:

- Audio perception: VAD, ASR orchestration, speaker segmentation, prosody features.
- Video perception: frame sampling, scene changes, OCR, object/action detection, UI understanding.
- Cross-modal alignment: align transcript tokens, visual observations, and event candidates on a shared timeline.
- Event heads: low-latency rules or compact models for urgent event classification.

Primary crates: `sinomo-audio`, `sinomo-video`, `sinomo-fusion`.

### Event / World-State Plane

Builds the system's working model of reality.

Responsibilities:

- Persist canonical events.
- Maintain entity graph and session state.
- Track active tasks, hypotheses, unresolved questions, and attention queues.
- Provide replayable decision traces.
- Feed relevant context to memory and control loops.
- Merge state updates from participating nodes.
- Preserve local-first operation when other nodes are unavailable.

Primary crates: `sinomo-events`, `sinomo-memory`, `sinomo-core`.

### Agent Control Plane

Decides what Sinomo should do next. This plane is split into three loops:

| Loop | Target latency | Purpose |
|---|---:|---|
| Fast reactive loop | 50-150 ms | Interrupts, safety triggers, wake/no-wake, alert suppression, immediate acknowledgements |
| Interactive loop | 200 ms-2 s | Dialogue, short retrieval, lightweight tool calls, task state, compact responses |
| Deliberative loop | 2 s+ | Planning, reflection, branch comparison, memory consolidation, long-running generation |

Primary crates: `sinomo-trigger`, `sinomo-agent`, `sinomo-deliberation`.

### Action / Generation Plane

Executes decisions and emits outputs.

Responsibilities:

- Spoken and text responses.
- Notifications and alerts.
- Local and remote tool actions.
- UI actions.
- Long-running generation jobs.
- Artifact delivery and progress events.

Primary crates: `sinomo-actions`, `sinomo-voice`, `sinomo-api`.

## Initial Workspace Layout

The repository should evolve from the current single binary into a Rust workspace with shared contracts and focused service crates.

```text
sinomo-agent/
  Cargo.toml
  Cargo.lock
  README.md
  docs/
    architecture.md
    proposal.md
    plans/
    research_notes/
  crates/
    sinomo-core/          # Shared IDs, event schema, domain types, errors
    sinomo-node/          # Node identity, local runtime metadata, capability model
    sinomo-device/        # Device registry, source/action target metadata
    sinomo-network/       # Peer discovery, transport, authentication, routing
    sinomo-sync/          # Session, world-state, event, and memory synchronization
    sinomo-dfs/           # Local-first distributed workspace filesystem
    sinomo-gateway/       # Ingress adapters and session wiring
    sinomo-audio/         # Audio orchestration and audio event production
    sinomo-video/         # Video/screen orchestration and visual event production
    sinomo-fusion/        # Cross-modal alignment on a shared timeline
    sinomo-events/        # Event persistence, replay, entity/world-state reducers
    sinomo-memory/        # Hot/warm/cold/procedural memory and retrieval policy
    sinomo-trigger/       # Timing policy for speak/act/alert/silence decisions
    sinomo-agent/         # Interactive controller and dialogue/task loop
    sinomo-deliberation/  # Long-running planning, reflection, and generation jobs
    sinomo-actions/       # Tool, automation, notification, and environment actions
    sinomo-voice/         # TTS orchestration and duplex audio output control
    sinomo-api/           # Operator/debug/admin HTTP and WebSocket API
    sinomo-app/           # Local deployment binary that wires services together
```

The current `crates/sinomo` binary can either be renamed to `sinomo-app` or kept temporarily as the bootstrap binary while the crate layout is introduced.

## Crate Responsibilities

### `sinomo-core`

Shared domain contracts used by all other crates.

Early types:

- `Event`
- `EventId`
- `NodeId`
- `DeviceId`
- `Entity`
- `EntityId`
- `SessionId`
- `SessionState`
- `TaskState`
- `ActionRequest`
- `ActionResult`
- `MemoryRecord`
- `TriggerDecision`
- `DeliberationJob`
- `NodeCapability`
- `DeviceCapability`
- `RoutingHint`
- `SyncPolicy`
- `WorkspaceId`
- `DfsObjectId`
- `DfsObjectPolicy`
- `DfsVersion`
- `DfsWriteRequest`
- `DfsWriteReceipt`

This crate should avoid service-specific dependencies. It owns stable schemas, IDs, serialization, error types, and shared time/source abstractions.

### `sinomo-node`

Node identity and runtime metadata.

Responsibilities:

- Create and load stable node identity.
- Describe node role, locality, trust domain, and resource envelope.
- Advertise hosted services and models.
- Track node health, load, and availability.
- Expose local runtime policy boundaries.

### `sinomo-device`

Device registry and metadata.

Responsibilities:

- Register devices attached to a node.
- Describe device sources and action targets.
- Track device permissions, availability, and current state.
- Map device events to stable `DeviceId` values.
- Provide capability filters for routing and placement.

### `sinomo-network`

Peer discovery, transport, and cross-node routing.

Responsibilities:

- Discover known or nearby nodes.
- Authenticate node-to-node communication.
- Route events, commands, and state updates.
- Support local network operation first.
- Provide transport abstractions over direct peer links, home-node sync APIs, or future protocols.

### `sinomo-sync`

Distributed session, event, world-state, and memory synchronization.

Responsibilities:

- Track session participants across nodes.
- Replicate selected event envelopes.
- Merge world-state updates.
- Coordinate handoff between devices.
- Apply sync policies for privacy, bandwidth, durability, and latency.
- Recover after node sleep, disconnect, or restart.

### `sinomo-dfs`

Local-first distributed filesystem for agent workspaces and task staging.

Responsibilities:

- Manage workspaces, mounts, paths, object IDs, and versions.
- Store file/object metadata in PostgreSQL.
- Store content on local node filesystems.
- Apply object-level ownership, replication, retention, and write policies.
- Route writes to owning nodes when required.
- Coordinate confirmed replication for selected objects.
- Emit `dfs.*` events for writes, reads, conflicts, replication, promotion, and cleanup.
- Provide workspace snapshots to action, coding, and generation workflows.

### `sinomo-gateway`

Always-on ingress service.

Responsibilities:

- Own input adapters.
- Normalize timestamps and source IDs.
- Attach node/device provenance.
- Create stream chunks.
- Apply backpressure policies.
- Publish raw input and chunk events.

### `sinomo-audio`

Audio perception worker.

Responsibilities:

- Voice activity detection.
- ASR backend orchestration.
- Speaker segmentation.
- Prosody and audio quality metadata.
- Speech event emission.

### `sinomo-video`

Video, screen, and UI perception worker.

Responsibilities:

- Frame sampling.
- Scene change detection.
- OCR and UI parsing.
- Object, person, action, and screen-state observations.
- Visual event emission.

### `sinomo-fusion`

Cross-modal alignment worker.

Responsibilities:

- Align audio, transcript, video, screen, and UI observations on a common timeline.
- Produce multimodal spans.
- Link related observations and event candidates.
- Feed fused observations into the event/world-state plane.

### `sinomo-events`

Canonical event service.

Responsibilities:

- Validate event envelopes.
- Persist event metadata.
- Publish durable streams.
- Support replay and trace inspection.
- Run world-state reducers that update entities, sessions, tasks, and attention queues.

### `sinomo-memory`

Memory service and retrieval policy.

Responsibilities:

- Maintain hot context and active state.
- Promote salient events into warm episodic memory.
- Maintain semantic/cold memory records and embeddings.
- Manage procedural memory references.
- Return bounded context packages to control loops.

### `sinomo-trigger`

Fast timing policy service.

Responsibilities:

- Decide whether to stay silent, speak, interrupt, alert, or schedule deeper work.
- Debounce noisy alerts.
- Respect speaking/listening state.
- Use salience, urgency, confidence, user state, and alert fatigue signals.

### `sinomo-agent`

Interactive controller.

Responsibilities:

- Handle normal dialogue turns.
- Retrieve recent context and warm memory.
- Maintain task state.
- Run bounded tool calls.
- Emit structured traces and action requests.

### `sinomo-deliberation`

Slow planning and reflection worker.

Responsibilities:

- Run long-horizon planning jobs.
- Compare branches and candidate outputs.
- Resolve uncertainty and contradictions.
- Consolidate memory.
- Orchestrate multi-step generation workflows.

### `sinomo-actions`

Action execution service.

Responsibilities:

- Execute tools and automations inside the runtime policy envelope.
- Emit action success, failure, progress, and sandbox-block events.
- Provide cancellation and kill-switch propagation for long-running work.

### `sinomo-voice`

Speech output service.

Responsibilities:

- TTS orchestration.
- Duplex audio output control.
- Barge-in handling.
- Output state events for trigger policy.

### `sinomo-api`

Operator and integration API.

Responsibilities:

- HTTP/WebSocket control endpoints.
- Debug and replay views.
- Health checks.
- Configuration and supervision interfaces.

### `sinomo-app`

Local deployment binary.

Responsibilities:

- Load configuration.
- Wire services for single-machine mode.
- Start background workers.
- Provide graceful shutdown.

## Event Model

Events are the canonical contract between services.

```json
{
  "event_id": "evt_01...",
  "time": "2026-04-21T22:30:14.182Z",
  "origin_node_id": "node_mobile_phone",
  "origin_device_id": "device_mobile_phone_mic",
  "target_node_id": "node_desktop_main",
  "target_device_id": "device_desktop_main_screen",
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
  },
  "routing": {
    "scope": "session",
    "latency_class": "interactive",
    "privacy": "derived",
    "hints": ["target:desktop", "requires:screen-control"]
  }
}
```

Initial event families:

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
- `dfs.*`
- `workspace.*`

The event envelope should live in `sinomo-core`; persistence, replay, and reducers should live in `sinomo-events`.

Distributed event fields:

- `origin_node_id`: node that produced the event.
- `origin_device_id`: device that produced the event, when applicable.
- `target_node_id`: preferred or required node for handling the event, when known.
- `target_device_id`: preferred or required action target, when known.
- `routing.scope`: local, session, trusted-network, or durable-global.
- `routing.latency_class`: reactive, interactive, background, or archive.
- `routing.privacy`: raw, derived, private, redacted, or public.
- `routing.hints`: capability and placement hints for routers and schedulers.

Filesystem-related events should include workspace ID, object ID, path, object policy, version, owner node, content hash when available, and write receipt metadata when the operation modifies state.

## Memory Architecture

| Tier | Purpose | Expected storage |
|---|---|---|
| Hot memory | Immediate live context, active speaker state, recent observations, open jobs | In-process bounded buffers with selective PostgreSQL checkpoints |
| Warm episodic memory | Session-scale continuity, important events, task transitions, unresolved questions | PostgreSQL |
| Cold semantic memory | Long-term summaries, embeddings, cross-session facts, document knowledge | PostgreSQL |
| Procedural memory | Reusable workflows, skills, recovery procedures, policies | Versioned files or managed skill store |

Memory writes should be selective. Events should be promoted when they are salient, repeated, unresolved, user-important, needed for future planning, or linked to task success/failure.

## Storage, Routing, and Sync

The only required external stateful dependency should be PostgreSQL. Early deployments should avoid requiring message brokers, distributed caches, dedicated search/vector databases, or separate object storage services.

Application services may still use in-process channels, local files, direct peer connections, HTTP/WebSocket APIs, or operating-system primitives, but PostgreSQL is the durable coordination point for event logging, state management, jobs, and memory.

### PostgreSQL

Use PostgreSQL as the primary durable state store.

Use cases:

- Event log and replay.
- Session and task state.
- Entity/world-state tables.
- Memory records.
- DFS workspace, object, version, and policy metadata.
- Job queues and long-running task state.
- Node/device registry.
- Routing inbox/outbox tables.
- Supervision and policy state.
- Semantic retrieval metadata and embedding storage.

In distributed deployments, PostgreSQL should initially live on one durable home node or small server node. Other nodes should cache local hot state and selected warm state. The architecture should not require mobile or lightweight nodes to host the full durable database.

PostgreSQL should be used as a durable event and coordination substrate, not as the real-time hot path for every local decision. Fast reactive loops should use local in-memory state and append durable events asynchronously when possible.

### Event Routing Without a Broker

Without a broker dependency, cross-node routing should use a small application-level routing layer backed by PostgreSQL durability.

Recommended pattern:

- Local services communicate through in-process queues for hot paths.
- Each node writes durable events to an append-only PostgreSQL event log when connected.
- Cross-node deliveries are represented in routing inbox/outbox tables.
- Workers use polling with leases for durable work and delivery.
- PostgreSQL `LISTEN`/`NOTIFY` can be used as an optimization for wakeups, not as the durable source of truth.
- Disconnected nodes buffer selected events locally and sync when reconnected.
- Direct peer links or home-node sync APIs can move events between devices while PostgreSQL remains the durable ledger.

### Distributed Workspace Filesystem

Use PostgreSQL for DFS metadata and local node filesystems for content.

PostgreSQL tables should track:

- Workspaces and mounts.
- Objects, paths, and tombstones.
- Versions and content hashes.
- Object policies and inherited directory policies.
- Node locations and replica state.
- Write intents, leases, receipts, and conflicts.
- Retention and cleanup state.

Content should remain on local node storage by default. Replication copies content between nodes according to object policy, while PostgreSQL records what should exist and what has been confirmed.

The DFS should expose a small API to the rest of the runtime:

- Resolve workspace path to object metadata.
- Read object from local cache or owner node.
- Submit write request.
- Confirm write receipt.
- Create snapshot for a task.
- Promote ephemeral object to durable workspace object.
- Replicate or pin object to selected nodes.
- Garbage collect unneeded local copies.

For coding tasks, the DFS should support repository workspaces that can be mounted as ordinary local directories on the executing node. Git remains the source-control layer; the DFS manages workspace locality, staging, replication, and handoff between nodes.

### Semantic Retrieval

Store embeddings and semantic memory metadata in PostgreSQL from the beginning. The first implementation can use simple SQL filtering plus in-process ranking over bounded candidate sets.

An optional PostgreSQL extension can be introduced later if vector search quality or scale requires it, but it should not be required by the initial architecture.

### Hot State

Hot state should stay local to each node.

Use cases:

- Recent transcript windows.
- Recent frame and UI observations.
- Active speaker state.
- Current output state.
- Active tool/job handles.
- Short-lived routing decisions.

Storage:

- In-process bounded buffers.
- Local ephemeral files when needed.
- Selective PostgreSQL checkpoints for recovery or handoff.

A future distributed cache should be introduced only behind a `sinomo-memory` abstraction rather than leaking into core contracts.

### Media and Evidence Storage

Avoid requiring separate object storage in the base architecture. Store media and large evidence locally on the node that captured or generated it, with PostgreSQL holding metadata, hashes, retention policy, and evidence references.

Use cases:

- Raw audio clips.
- Sampled frames.
- Screen captures.
- Generated media.
- Evidence attached to events.

Evidence references must include enough locality information to support local-only evidence, replicated evidence, and redacted evidence handles. A future object store can be added as an implementation detail when scale requires it, but it should not be part of the initial dependency set.

## Synchronization Strategy

Sinomo should synchronize intent, state, and evidence at different levels of detail.

### Event Replication

Replicate durable event envelopes according to routing and privacy policy.

Default behavior:

- Reactive local events stay local unless they affect a shared session or remote action target.
- Session events replicate to active session participants.
- Raw media references replicate only as evidence handles unless policy allows raw transfer.
- Summaries, embeddings, and derived observations can move more freely than raw streams.
- Blocked actions and safety incidents replicate to the supervision home node.
- DFS metadata events replicate according to workspace/object policy.

### World-State Merge

World-state should be federated. Each node can maintain local reducers for the entities and devices it observes, while `sinomo-sync` merges updates into a shared session view.

Merge guidance:

- Prefer append-only event history over destructive updates.
- Track provenance for every state assertion.
- Represent uncertainty and conflicts explicitly.
- Let newer local device state override stale remote state for that device.
- Use deterministic reducers for replayable state where possible.
- Use CRDT-style structures for simple shared collections when concurrent edits are expected.

### Memory Synchronization

Memory should flow by usefulness and policy rather than full replication.

Examples:

- Mobile keeps hot context for the current conversation and cached user preferences.
- Desktop keeps hot context for screen/UI state and local action history.
- Server keeps durable warm/cold memory and performs consolidation.
- Participating nodes receive compact context packets relevant to their current tasks.

Memory records should include origin, evidence links, promotion reason, visibility policy, and allowed replication scope.

### Workspace Synchronization

Workspace synchronization should be policy-driven per object or subtree.

Default behavior:

- Local scratch files stay on the node that created them.
- Durable workspace metadata is recorded in PostgreSQL.
- `local-owned` objects route writes to the owner node.
- `replicated-confirmed` objects require configured write receipts before becoming the visible current version.
- `replicated-eventual` objects may be visible immediately with pending replica state.
- Conflict objects are surfaced as first-class workspace entries.

The synchronization layer should keep enough metadata for an agent to know whether a file is local, stale, pending replication, conflicted, or unavailable because another node is offline.

### Capability Discovery

Nodes should advertise capabilities in a way that planning and routing can use.

Capability examples:

- `input.audio.vad`
- `input.audio.asr.local`
- `input.video.screen_ocr`
- `action.desktop.keyboard_mouse`
- `action.mobile.notification`
- `model.embedding.local`
- `model.reactive.local`
- `storage.memory.durable`
- `storage.object.local`
- `storage.workspace.local`
- `storage.workspace.replicated`
- `dfs.policy.local_owned`
- `dfs.policy.replicated_confirmed`

Capability discovery allows Sinomo to defer exact placement decisions while keeping the architecture ready for local execution.

## Runtime Flows

### Live Interruption

1. `sinomo-gateway` receives microphone audio.
2. `sinomo-audio` emits VAD and transcript events.
3. `sinomo-trigger` sees a high-salience interruption cue.
4. The fast reactive loop decides whether to interrupt, acknowledge, alert, or wait.
5. `sinomo-agent` retrieves recent context and produces a short response.
6. If ambiguity remains, `sinomo-deliberation` receives a follow-up job.
7. The deliberative result is written back as events and memory.
8. `sinomo-agent` delivers the follow-up when ready.

### Mobile-to-Desktop Action

1. The user speaks to the mobile frontend.
2. The mobile node runs VAD, wake policy, and lightweight audio perception locally.
3. The mobile node publishes a session event with transcript, embeddings, intent hints, and mobile device provenance.
4. `sinomo-network` routes the event to the desktop node because the desktop owns the target screen/action devices.
5. The desktop node retrieves local screen/UI state and relevant shared session context.
6. A local fast reactive or interactive controller decides whether the action is safe and executable.
7. `sinomo-actions` executes the desktop action locally.
8. Action results, screen changes, and any blocked-policy events are published back into the session timeline.
9. The mobile node receives a compact status update for user feedback.

### Long-Running Generation

1. The user asks for a multi-step generation task.
2. `sinomo-agent` acknowledges and scopes the request.
3. `sinomo-deliberation` creates a job graph.
4. Candidate branches run asynchronously.
5. Evaluators compare candidates.
6. `sinomo-actions` or specialized generation backends produce artifacts.
7. Progress and outputs are emitted as events.
8. Results and lessons are promoted into memory.

### Distributed Coding Workspace

1. A coding task creates or selects a DFS workspace.
2. The workspace mounts a repository checkout owned by the desktop node.
3. A mobile node can inspect files through DFS reads or summarized context.
4. Write requests for `local-owned` repository files route to the desktop node.
5. The desktop node applies edits locally, runs formatting/tests, and emits `dfs.write.committed`, `tool.*`, and `agent.*` events.
6. Important generated artifacts use `replicated-confirmed` policy so another node can review or continue work.
7. Scratch build outputs remain `ephemeral-local` and are garbage collected by policy.
8. If a node disconnects mid-task, PostgreSQL metadata and DFS version state allow another capable node to resume from the latest confirmed workspace state.

## Safety and Supervision

Sinomo should act freely inside explicitly enforced runtime envelopes. Human approval should not sit on the low-latency path.

Each node is its own enforcement boundary. A remote node can request or route an action, but the node that owns the target device must make the final local policy check before execution.

The runtime must enforce:

- Filesystem scope restrictions.
- DFS workspace and object-policy restrictions.
- Network egress restrictions.
- Process spawning restrictions.
- Device access restrictions.
- Secret/material access boundaries.
- Rate limits and resource ceilings.
- Kill-switch and cancellation propagation.

Blocked actions should produce structured incident events with evidence, policy details, and replay references. Supervisors can review patterns after the fact and adjust sandbox rules or policy state.

Distributed safety requirements:

- A node must not execute actions solely because another node requested them.
- Action authority is local to the node/device that will be affected.
- DFS write authority must follow the target object's policy and owner node.
- Capability advertisements must distinguish observable capability from permitted capability.
- Secrets and raw sensitive data should remain on the owning node unless policy explicitly allows transfer.
- Incident events should include origin node, requesting node, target node, target device, and policy decision.
- Emergency stop and cancellation must propagate across active session participants.

## Observability

Track at minimum:

- Ingestion lag.
- Perception latency by modality.
- Trigger precision and false interrupt rate.
- Interactive response latency.
- Deliberation job latency.
- Memory retrieval quality.
- Action success/failure rates.
- Sandbox block rates.
- Replay trace completeness.

The system should support event replay, decision trace inspection, memory promotion audits, trigger decision audits, and branch comparison for deliberative jobs.

## MVP Scope

The first useful MVP should prove the event-centric, three-speed runtime without overbuilding the full future system.

Build first:

- `sinomo-core` event and domain contracts.
- `sinomo-node`, `sinomo-device`, `sinomo-network`, `sinomo-sync`, and `sinomo-dfs` foundation contracts.
- `sinomo-events` persistence and replay.
- DFS metadata tables and local content storage for one-node and simulated two-node workspaces.
- `sinomo-gateway` for text plus one audio input and one screen/video input.
- Basic `sinomo-audio` and `sinomo-video` event producers.
- `sinomo-trigger` with rule-based timing decisions.
- `sinomo-agent` interactive loop with bounded retrieval and limited tool use.
- One `sinomo-deliberation` worker for asynchronous jobs.
- Minimal `sinomo-api` health, debug, and event inspection endpoints.
- A two-node local development mode that simulates mobile-to-desktop routing on one machine.
- A coding workspace simulation where writes route to the owning node.

Defer:

- General multi-camera fusion.
- End-to-end omni-model training.
- Highly autonomous background actioning.
- Large-scale distributed retrieval infrastructure.
- Sophisticated multi-agent societies.
- Fully automatic placement optimization.
- Complex cross-internet federation.

## Implementation Phases

### Phase 1: Contracts and Event Backbone

- Create `sinomo-core`.
- Define shared IDs, node/device IDs, event envelope, domain types, routing metadata, and serialization.
- Create `sinomo-node` and `sinomo-device`.
- Define node/device capability contracts.
- Create `sinomo-events`.
- Add event validation, persistence interfaces, and replay scaffolding.
- Add a basic `sinomo-app` binary to wire local services.

### Phase 2: Distributed Runtime Skeleton

- Create `sinomo-network`.
- Create `sinomo-sync`.
- Create `sinomo-dfs`.
- Add local peer registry and static node configuration.
- Support routing events between simulated nodes in one process or one workstation.
- Add session membership and basic handoff metadata.
- Add workspace/object metadata, policy definitions, and local content storage.
- Support owner-routed writes in simulated nodes.

### Phase 3: Ingestion and Perception Skeletons

- Create `sinomo-gateway`.
- Add input adapter traits.
- Create `sinomo-audio`, `sinomo-video`, and `sinomo-fusion` skeletons.
- Emit synthetic and fixture-backed perception events for testing.
- Attach node/device provenance to all produced events.

### Phase 4: Trigger and Interactive Loop

- Create `sinomo-trigger`.
- Implement rule-based trigger decisions.
- Create `sinomo-agent`.
- Add bounded context retrieval and simple action requests.
- Route action requests to the node that owns the target device.

### Phase 5: Memory and Deliberation

- Create `sinomo-memory`.
- Implement hot and warm memory policies.
- Create `sinomo-deliberation`.
- Add async job model, cancellation, summaries, and memory consolidation hooks.
- Add context packet synchronization between participating nodes.
- Connect task context to DFS workspace snapshots.

### Phase 6: Actions, Voice, API, and Supervision

- Create `sinomo-actions`.
- Add sandbox-aware action execution events.
- Create `sinomo-voice`.
- Create `sinomo-api`.
- Add health checks, replay endpoints, policy incident views, and operator controls.
- Add distributed incident traces across requesting and executing nodes.
- Add DFS policy violation events and workspace audit views.

## Near-Term Repository Setup

The next structural step should be to add the crates in dependency order:

1. `sinomo-core`
2. `sinomo-node`
3. `sinomo-device`
4. `sinomo-events`
5. `sinomo-network`
6. `sinomo-sync`
7. `sinomo-dfs`
8. `sinomo-gateway`
9. `sinomo-trigger`
10. `sinomo-agent`
11. `sinomo-memory`
12. perception/action/support crates as skeletons
13. `sinomo-app`

Start with compileable crate skeletons, shared workspace dependencies, and lightweight tests around the event schema, node/device provenance, routing metadata, and DFS object policy behavior. Avoid introducing external services beyond PostgreSQL until the contracts and in-process distributed flow are stable.
