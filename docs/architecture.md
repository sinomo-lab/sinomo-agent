# Sinomo Architecture

## Overview

Sinomo is a distributed, real-time multimodal AI entity runtime built around continuous perception, an event-centric world model, and three cooperating control loops. The system should keep ingestion and perception running at all times, even when slower reasoning, tool use, or generation workflows are in progress.

The foundation is a network of cooperating nodes. Each node may have one or more devices attached, and each node can host perception, memory, control, and action components close to where the relevant signal or action happens. A single workstation deployment should be a special case of the same node graph, not a separate architecture.

The initial architecture favors a Rust workspace with small, focused crates that can run together on one machine first, then distribute across desktops, phones, small servers, and LAN devices as workload, privacy, latency, and hardware needs grow.

Self-evolution is treated as controlled live mutation: Sinomo should be able to change running behavior autonomously without bringing down the system. The detailed design for sandboxed mutation, evaluation, hot-swap, progressive rollout, and rollback lives in [self-evolution-architecture.md](self-evolution-architecture.md).

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
10. **Evolution is live and continuity-preserving.** Self-evolution must move through scoped workspaces, evaluation gates, signed artifacts, node-local policy checks, hot-swap, progressive rollout, and rollback without requiring human approval or stopping the system.

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

### Local Daemon / Service Mode

The initial local service entry point is `sinomo daemon`. It is intentionally small: a foreground service command plus CLI controls that can be wrapped by systemd, launchd, a shell supervisor, or a future Sinomo-owned service manager.

- `sinomo daemon run` runs the daemon in the foreground.
- `sinomo daemon start` launches a detached local background process.
- `sinomo daemon status` probes both the data-directory lock and the listener.
- `sinomo daemon stop` asks the running daemon to shut down.
- `sinomo daemon shadow run|start|status|stop` controls the shadow daemon on `127.0.0.1:21354` by default.

The daemon listens on `127.0.0.1:21353` by default. The bind host and port can be overridden with `--host` and `--port`, but the default should remain stable so local tools can discover the service without a separate registry.

Each daemon instance owns `$SINOMO_DATA_DIR/daemon/daemon.lock` and writes `$SINOMO_DATA_DIR/daemon/status.json`. The lock prevents duplicate local daemons for the same data directory. The status file records the PID, bind address, start time, data directory, and lock path so CLI commands can distinguish three cases: running and reachable, locked but not reachable, or not running. This probe path is the first local control-plane primitive for long-running background work such as scheduled evolution campaigns.

### Self-Rollout Source and Shadow Daemon

The first self-rollout path treats Sinomo's own source as durable node-local state. `sinomo rollout init-source` creates `$SINOMO_DATA_DIR/source/sinomo` by cloning the current repository unless a different `--repo` is supplied. When `sinomo evolve propose` does not receive an explicit `--repo` or configured repo, it uses this data-directory source repository so approved self-evolution work lands in Sinomo's maintained source rather than the operator's checkout.

Approved evolution decisions commit the candidate worktree and fast-forward the source repository. Rollout is then an explicit local operation:

- `sinomo rollout status` prints the data-directory source, installed main binary, installed shadow binary, and rollout state paths.
- `sinomo rollout apply` builds `$SINOMO_DATA_DIR/source/sinomo` with `cargo build -p sinomo`.
- The build log is written to `$SINOMO_DATA_DIR/rollout/build.log`.
- The produced binary is copied atomically to `$SINOMO_DATA_DIR/bin/sinomo-main` and `$SINOMO_DATA_DIR/bin/sinomo-shadow`.
- The main daemon is stopped, the shadow daemon is restarted from the installed shadow binary, and the shadow daemon starts the installed main binary and verifies it becomes healthy.

The shadow daemon owns separate state under `$SINOMO_DATA_DIR/shadow-daemon/` so it does not contend with the main daemon's lock. Its steady-state responsibility is intentionally narrow: it supervises the main daemon process that it launched. If that process exits with a crash-class exit code or crash-class Unix signal, the shadow daemon records `$SINOMO_DATA_DIR/rollout/last-main-exit.json` and restarts the main daemon. If the main daemon exits cleanly or is terminated/killed by operator action, the shadow daemon records the event and does not restart it. This preserves a difference between real crashes and intentional intervention.

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

## Self-Evolution Foundation

Sinomo should support R1-ready live self-evolution from the beginning. The runtime should treat evolution as an autonomous software delivery loop that consumes events, traces, incidents, scorecards, workspaces, and policy state, then mutates running behavior through versioned artifacts, hot-swap, rollout events, and automatic rollback.

The detailed design lives in [self-evolution-architecture.md](self-evolution-architecture.md). This architecture reserves the foundation contracts and trust boundaries needed to support it.

### Trust Rings

The system should separate immutable foundation code from evolvable modules.

| Ring | Contents | Evolution rule |
|---|---|---|
| R0 continuity root | Event envelope, ID contracts, artifact verifier, policy verifier, rollback loader, emergency stop | Autonomous mutation only through dual-root/standby handoff and strongest gates |
| R1 trusted runtime | Node supervisor, event store, DFS metadata, sandbox host, module loader, rollout controller | Autonomous live mutation with redundancy, shadowing, local health checks, and rollback |
| R2 trusted services | Perception, memory, trigger, agent, deliberation, action services | Autonomous live mutation after evaluation and rollout gates |
| R3 sandboxed modules | Skills, tools, evaluators, adapters, prompts, planners, local policies | Primary auto-evolution target |
| R4 disposable workspaces | Mutation branches, test sandboxes, generated fixtures, traces | Fully automated but isolated |
| R5 model providers | Local or remote LLMs used for proposal, implementation, review, critique | No authority; produce artifacts or judgments only |

R0 should remain tiny and continuity-focused. R1 should be boring in behavior but mutable in deployment: it must be designed for autonomous replacement through redundant supervisors, shadow instances, atomic desired-state changes, and fast rollback.

The crate graph now makes this trust-ring cut explicit. Each workspace crate declares `[package.metadata.sinomo] ring = "..."` in its `Cargo.toml`, and `sinomo-runtime` is the R0/R1 boundary facade. R0/R1 crates must not depend on R2 services or application crates. R2 services and application crates must cross the foundation boundary through the `sinomo-runtime` facade instead of depending directly on R0/R1 crates.

Initial crate ownership:

| Group | Crates |
|---|---|
| R0 | `sinomo-core` |
| R1 | `sinomo-api`, `sinomo-device`, `sinomo-dfs`, `sinomo-eval`, `sinomo-events`, `sinomo-local`, `sinomo-module`, `sinomo-network`, `sinomo-node`, `sinomo-policy`, `sinomo-registry`, `sinomo-rollout`, `sinomo-sandbox`, `sinomo-supervision`, `sinomo-sync` |
| R0/R1 boundary | `sinomo-runtime` |
| R2 | `sinomo-actions`, `sinomo-agent`, `sinomo-audio`, `sinomo-deliberation`, `sinomo-evolution`, `sinomo-fusion`, `sinomo-gateway`, `sinomo-lm`, `sinomo-memory`, `sinomo-trigger`, `sinomo-video`, `sinomo-voice` |
| Application/tooling | `sinomo`, `sinomo-app`, `sinomo-cli` |

### Evolution Delivery Model

Self-evolution should flow through explicit artifacts:

```text
events / metrics / incidents / traces / workspaces
  -> evolution job
  -> scoped mutation workspace
  -> candidate bundle
  -> sandbox build and test
  -> evaluation scorecard
  -> policy decision
  -> signed artifact registry entry
  -> node-local load check
  -> shadow / hot-swap / canary / promotion
  -> rollback or stable version
```

No coding worker or LLM should be able to directly overwrite a live module, trusted runtime crate, registry entry, policy verifier, or rollback path. Live mutation happens only when the autonomous evolution controller publishes a verified desired-state change that node-local loaders can accept or reject under local policy.

### Module Capsules

Every evolvable module should eventually be packaged as a capsule.

Capsule contents:

- Module manifest.
- Code artifact or prompt/config artifact.
- Capability imports and exports.
- Resource limits.
- API version and compatibility constraints.
- Source workspace and source revision.
- Content hash and signature.
- Evaluation scorecard references.
- Provenance and attestation records.
- Rollout and rollback policy.

The manifest is part of the artifact. A candidate cannot grant itself new powers by editing code; capability requests are manifest changes and must pass policy.

### R1 Support Requirements

The initial foundation should support these R1 requirements even if their first implementation is skeletal:

- Stable trust ring metadata on modules, jobs, policies, and artifacts.
- Module manifest and artifact identity types in `sinomo-core`.
- An explicit `sinomo-runtime` facade that aggregates only R0/R1 crates and gives R2 services a stable foundation import boundary.
- Workspace architecture tests that reject missing ring metadata, R0/R1 dependencies on R2/application crates, and direct R2/application dependencies on R0/R1 crates outside `sinomo-runtime`.
- A policy decision boundary that candidate code cannot bypass.
- An artifact registry model backed by PostgreSQL metadata and DFS/local content storage.
- Node-local module loader state with current, previous, shadow, and candidate versions.
- Hot-swap state with activation, drain, quiesce, and rollback checkpoints.
- Rollout desired-state records that can be evaluated per node.
- Rollback events that point to known-good artifacts.
- Evaluation scorecards that are machine-readable and replayable.
- Sandboxed mutation workspaces using DFS snapshots and object policies.
- Event families for `evolution.*`, `module.*`, `artifact.*`, `eval.*`, and `rollout.*`.

R1 services may be implemented as ordinary Rust crates at first, but their contracts should assume they are trusted runtime components that must support autonomous live replacement without taking down the node.

## Runtime Planes

```text
Inputs
  -> Distributed Foundation Plane
  -> Ingestion Plane
  -> Perception Plane
  -> Event / World-State Plane
  -> Agent Control Plane
  -> Evolution / Delivery Plane
  -> Action / Generation Plane
  -> Outputs

Shared stores:
  - Hot state cache
  - Event store
  - Semantic memory tables/indexes
  - Local media/evidence stores
  - Distributed workspace filesystem
  - Artifact registry metadata
  - Evaluation scorecards
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

### Evolution / Delivery Plane

Controls self-evolution as a gated delivery pipeline.

Responsibilities:

- Detect improvement opportunities from events, metrics, incidents, user corrections, replay failures, and stale scorecards.
- Create scoped evolution jobs with target ring, allowed write set, required gates, and rollback policy.
- Prepare DFS mutation workspaces and candidate bundles.
- Run sandboxed builds, tests, replay suites, and scorecards.
- Verify manifests, capability diffs, artifacts, signatures, and provenance.
- Register approved artifacts.
- Coordinate shadow, hot-swap, canary, rolling promotion, and rollback across nodes.
- Support autonomous R1 mutation with stronger gates than R2/R3 and continuity checks before activation.

Primary crates: `sinomo-evolution`, `sinomo-eval`, `sinomo-sandbox`, `sinomo-registry`, `sinomo-policy`, `sinomo-rollout`, `sinomo-module`.

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
    sinomo-policy/        # Trusted policy decision contracts and verifier
    sinomo-registry/      # Module artifact registry and provenance metadata
    sinomo-module/        # Module manifests, interfaces, and node-local loader state
    sinomo-sandbox/       # Sandboxed candidate execution and test jobs
    sinomo-eval/          # Evaluation definitions, replay suites, and scorecards
    sinomo-evolution/     # Evolution job orchestration
    sinomo-rollout/       # Progressive rollout and rollback controller
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
- `TrustRing`
- `ModuleId`
- `ArtifactId`
- `ModuleManifest`
- `ArtifactManifest`
- `ArtifactProvenance`
- `CapabilityDiff`
- `PolicyDecision`
- `EvolutionJob`
- `EvaluationScorecard`
- `RolloutPlan`
- `RollbackPolicy`

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

### `sinomo-policy`

Trusted policy decision boundary.

Responsibilities:

- Evaluate structured policy inputs for actions, DFS writes, module loads, artifact promotion, and rollout.
- Separate policy decisions from candidate or evolvable code.
- Represent allow, deny, defer, quarantine, require-more-evidence, and emergency-stop decisions.
- Treat new filesystem, network, process, secret, device, policy, registry, verifier, sandbox, or rollback capabilities as explicit review points.

### `sinomo-registry`

Module artifact registry and provenance metadata.

Responsibilities:

- Track module artifacts, manifests, versions, hashes, signatures, source revisions, compatibility constraints, and rollout state.
- Store provenance and attestation metadata.
- Expose immutable artifact references to node-local loaders.
- Keep previous stable artifact references available for rollback.

### `sinomo-module`

Module interface and node-local loader state.

Responsibilities:

- Define module manifests and API compatibility contracts.
- Track stable, previous, shadow, candidate, and rolled-back versions per node.
- Verify manifests and artifact compatibility before load.
- Support activation, drain, quiesce, hot-swap, and rollback state transitions.
- Emit module load, unload, shadow decision, crash, timeout, and policy-blocked events.

### `sinomo-sandbox`

Sandbox host for candidate builds, tests, and R3 modules.

Responsibilities:

- Run candidate jobs in constrained process/container sandboxes initially.
- Provide a path toward Wasm Component Model modules for R3 capsules.
- Enforce CPU, memory, wall-clock, filesystem, network, secret, and device limits.
- Normalize logs and artifacts before they are reviewed by models or operators.

### `sinomo-eval`

Evaluation definitions and scorecards.

Responsibilities:

- Run static checks, unit tests, integration tests, replay tests, private regression suites, adversarial probes, shadow analysis, and canary analysis.
- Produce machine-readable scorecards.
- Keep generated tests as evidence, not sole authority.
- Build private rotating evals from Sinomo's own traces and failure cases.

### `sinomo-evolution`

Evolution job orchestrator.

Responsibilities:

- Detect improvement opportunities.
- Scope jobs with target module, trust ring, objective, allowed write set, required gates, and rollback policy.
- Coordinate proposal, implementation, autonomous review, build, evaluation, policy decision, artifact registration, rollout, rollback, and archival.
- Emit `evolution.*` events for every state transition.

### `sinomo-rollout`

Progressive delivery and rollback controller.

Responsibilities:

- Represent desired artifact state by module, node, role, trust domain, device class, session, or opt-in group.
- Support shadow, hot-swap, canary, blue/green, rolling promotion, pause, and rollback.
- Stop promotion when scorecards, incidents, latency, crash rate, or user corrections regress.
- Keep rollback to known-good artifacts fast and replayable.

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
- `evolution.*`
- `module.*`
- `artifact.*`
- `eval.*`
- `rollout.*`

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

Evolution-related events should include trust ring, target module, artifact ID, source workspace, policy decision, scorecard ID, rollout plan, affected nodes, and rollback target when applicable.

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
- Module manifests, artifact registry metadata, provenance, signatures, and attestations.
- Evolution jobs, evaluation scorecards, policy decisions, rollout desired state, and rollback records.
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

### Artifact Registry and Provenance

Use PostgreSQL for registry metadata and DFS/local content storage for artifact bytes.

PostgreSQL tables should track:

- Module manifests and API versions.
- Artifact IDs, content hashes, signatures, and compatibility constraints.
- Source workspace, source revision, and mutation job.
- Build, test, eval, and policy attestations.
- Current, previous, candidate, shadow, draining, and rollback artifact state per node.
- Rollout plans, rollout progress, rollback records, and affected nodes.

Artifact content should be immutable. A rollback should point to a known-good artifact rather than mutating the failed artifact in place.

The registry is R1 infrastructure. Candidate code may produce a candidate bundle, but only trusted registry and policy paths can make it loadable.

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
- Evolution, artifact, module, eval, and rollout events replicate to the registry/supervision home node and to affected rollout nodes.

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

### Artifact and Rollout Synchronization

Artifact and rollout synchronization should be desired-state based.

Default behavior:

- The registry records approved artifacts and desired rollout state.
- Nodes do not blindly load new artifacts; they verify signature, manifest compatibility, local policy, resource limits, and trust ring.
- Disconnected nodes keep running their last stable approved artifact.
- Reconnected nodes reconcile desired state through the same checks as online nodes.
- Shadow and canary decisions emit events that feed scorecards before promotion advances.
- Rollbacks are signed desired-state events pointing to known-good artifacts.
- R1 hot-swap uses a standby candidate instance, health probes, state handoff, activation checkpoint, and rollback checkpoint before replacing the active instance.

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
- `module.loader.local`
- `module.runtime.wasm`
- `module.runtime.process_sandbox`
- `evolution.eval.replay`
- `evolution.rollout.canary`
- `registry.artifact.durable`

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

### R3 Module Evolution With R1 Rollout

1. `sinomo-evolution` detects an opportunity from incidents, metrics, replay failures, or user corrections.
2. It creates an `EvolutionJob` with target ring, target module, objective, allowed DFS workspace paths, required gates, and rollback policy.
3. A mutation worker edits only an isolated DFS snapshot or branch workspace.
4. `sinomo-sandbox` builds and tests the candidate under resource and capability limits.
5. `sinomo-eval` runs scorecards, including replay/private regression suites.
6. `sinomo-policy` approves, denies, quarantines, or requests more autonomous evidence based on the manifest diff, capability diff, scorecard, and target ring.
7. `sinomo-registry` records the immutable artifact, provenance, and approved rollout plan.
8. A node-local loader verifies the artifact and runs it in shadow mode beside the stable module.
9. `sinomo-rollout` advances canary or rolls back based on module events, scorecards, incidents, and user correction signals.

### R1 Live Foundation Mutation

1. An improvement opportunity targets an R1 component such as the event store, DFS metadata layer, sandbox host, policy verifier, module loader, or rollout controller.
2. `sinomo-evolution` creates a candidate workspace with a narrower allowed write set, stronger replay requirements, and explicit continuity tests.
3. The candidate must produce build logs, test logs, replay evidence, scorecards, manifest diffs, provenance, rollback notes, and a state-handoff plan.
4. `sinomo-sandbox` runs the candidate against synthetic and replayed control-plane workloads before any live activation.
5. The node starts a standby R1 candidate beside the active R1 component and feeds it shadow traffic where possible.
6. The loader performs an activation checkpoint, drains compatible in-flight work, swaps traffic to the candidate, and keeps the previous version hot.
7. Rollout remains node-local-authority-first; each node can reject, quarantine, activate, or roll back the candidate based on local health and policy.
8. If the candidate regresses, the rollback loader restores the previous artifact without stopping unrelated services.

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
- Module capability imports and resource limits.
- Artifact signature, manifest, and provenance verification.
- Rollout policy, shadow/canary limits, and rollback triggers.
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

Self-evolution safety requirements:

- R0 and R1 changes are autonomous but require the strongest evidence, redundant execution, and continuity checks.
- Candidate code cannot approve its own artifact, modify policy decisions, alter registry trust, bypass the sandbox host, or replace rollback logic.
- New filesystem, network, process, secret, device, policy, registry, verifier, sandbox, or rollback capabilities require explicit autonomous policy review and additional evidence gates.
- R3 modules should run in sandboxed module boundaries when feasible, with Wasm Component Model as the preferred long-term interface.
- Node-local loaders must verify artifact signature, manifest compatibility, capability grants, trust ring, and local resource limits before loading.
- Shadow mode should be available before canary for any module whose behavior can be compared without taking action.
- Rollback must remain available even when the failed artifact caused crashes, timeouts, or policy incidents.
- A failed self-mutation must degrade to the last stable artifact or a reduced-capability safe mode rather than bringing down the node.

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
- Module crash, timeout, and policy-block rates.
- Evaluation gate pass/fail and scorecard deltas.
- Artifact promotion, rollback, and node adoption rates.
- Shadow/canary regressions and user correction spikes.

The system should support event replay, decision trace inspection, memory promotion audits, trigger decision audits, and branch comparison for deliberative jobs.

For self-evolution, observability must also support artifact provenance inspection, manifest diff review, capability diff review, evaluation scorecard comparison, rollout timeline inspection, and rollback evidence review.

## MVP Scope

The first useful MVP should prove the event-centric, three-speed runtime without overbuilding the full future system.

Build first:

- `sinomo-core` event and domain contracts.
- `sinomo-node`, `sinomo-device`, `sinomo-network`, `sinomo-sync`, and `sinomo-dfs` foundation contracts.
- R0/R1 trust ring contracts, module manifests, artifact IDs, policy decisions, evaluation scorecards, and rollout plan placeholders.
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
- An autonomous live evolution harness shape: scoped candidate workspace, scorecard record, policy decision, registry metadata, shadow activation, hot-swap state, and automatic rollback.

Defer:

- General multi-camera fusion.
- End-to-end omni-model training.
- Highly autonomous background actioning.
- Large-scale distributed retrieval infrastructure.
- Sophisticated multi-agent societies.
- Fully automatic placement optimization.
- Complex cross-internet federation.
- Cross-fleet high-speed R1 rollout.
- Mutation of the continuity root without a standby/dual-root mechanism.
- Full Wasm component conversion for all modules.

## Implementation Phases

### Phase 1: Contracts and Event Backbone

- Create `sinomo-core`.
- Define shared IDs, node/device IDs, event envelope, domain types, routing metadata, trust rings, module/artifact IDs, policy decisions, scorecards, rollout placeholders, and serialization.
- Create `sinomo-node` and `sinomo-device`.
- Define node/device capability contracts.
- Create `sinomo-events`.
- Add event validation, persistence interfaces, and replay scaffolding.
- Add a basic `sinomo-app` binary to wire local services.

### Phase 2: Distributed Runtime Skeleton

- Create `sinomo-network`.
- Create `sinomo-sync`.
- Create `sinomo-dfs`.
- Create skeletons for `sinomo-policy`, `sinomo-registry`, and `sinomo-module`.
- Add local peer registry and static node configuration.
- Support routing events between simulated nodes in one process or one workstation.
- Add session membership and basic handoff metadata.
- Add workspace/object metadata, policy definitions, and local content storage.
- Support owner-routed writes in simulated nodes.
- Add manifest, artifact, policy, and loader-state metadata with no dynamic loading yet.

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
- Add module policy violation events and artifact/rollout audit views.

### Phase 7: Self-Evolution Harness

- Create `sinomo-evolution`.
- Create `sinomo-eval`.
- Create `sinomo-sandbox`.
- Create `sinomo-rollout`.
- Add autonomous evolution jobs scoped to DFS snapshots.
- Add local command-based scorecards and replay fixture references.
- Add registry candidate records and provenance metadata.
- Add policy decisions that allow, deny, defer, quarantine, or request more evidence.
- Add shadow, hot-swap, canary, and rollback state models with automatic promotion for low-risk modules and guarded R1 trials.

## Near-Term Repository Setup

The next structural step should be to add the crates in dependency order:

1. `sinomo-core`
2. `sinomo-node`
3. `sinomo-device`
4. `sinomo-events`
5. `sinomo-network`
6. `sinomo-sync`
7. `sinomo-dfs`
8. `sinomo-policy`
9. `sinomo-registry`
10. `sinomo-module`
11. `sinomo-gateway`
12. `sinomo-trigger`
13. `sinomo-agent`
14. `sinomo-memory`
15. `sinomo-eval`
16. `sinomo-sandbox`
17. `sinomo-evolution`
18. `sinomo-rollout`
19. perception/action/support crates as skeletons
20. `sinomo-app`

Start with compileable crate skeletons, shared workspace dependencies, and lightweight tests around the event schema, node/device provenance, routing metadata, DFS object policy behavior, trust-ring classification, module manifest validation, policy decision shapes, and rollout state transitions. Avoid introducing external services beyond PostgreSQL until the contracts and in-process distributed flow are stable.
