# Sinomo Architecture

## Overview

Sinomo is a distributed, real-time multimodal AI entity runtime built around continuous perception, an event-centric world model, and three cooperating control loops. The system should keep ingestion and perception running at all times, even when slower reasoning, tool use, or generation workflows are in progress.

The foundation is a network of cooperating nodes. Each node may have one or more devices attached, and each node can host perception, memory, control, and action components close to where the relevant signal or action happens. A single workstation deployment should be a special case of the same node graph, not a separate architecture.

The architecture favors a Rust workspace with small, focused crates that can run together on one machine and distribute across desktops, phones, small servers, and LAN devices as workload, privacy, latency, and hardware needs require.

The detailed cluster bootstrap, discovery, roaming transport, membership, and distributed KV design lives in [clustering-architecture.md](clustering-architecture.md). The detailed distributed filesystem design lives in [dfs-architecture.md](dfs-architecture.md). Runtime logging, metrics, tracing, and cluster observation are described in [observability-architecture.md](observability-architecture.md). Long-horizon agent memory, memory storage, consolidation, and multi-agent memory governance are described in [memory-architecture.md](memory-architecture.md). The SIFS-backed WASM sandbox design lives in the sandbox crate's [architecture](../crates/sinomo-sandbox/docs/architecture.md). The binary communication and serialization format is specified in [sinomo-object-format.md](sinomo-object-format.md). Rollout operator procedures and examples are documented in [rollout-operations.md](rollout-operations.md). The user-facing main agent, local core model, async deliberation, cluster-aware tooling, background agent, and training-data flow are described in [agent-system-architecture.md](agent-system-architecture.md).

Self-evolution is treated as controlled live mutation: Sinomo should be able to change running behavior autonomously without bringing down the system. The detailed design for sandboxed mutation, evaluation, hot-swap, progressive rollout, and rollback lives in [self-evolution-architecture.md](self-evolution-architecture.md).

## Core Principles

1. **Perception is always on.** Audio, video, screen, text, telemetry, and external events are ingested continuously without waiting for reasoning or generation to finish.
2. **Events are the canonical substrate.** Raw streams are converted into timestamped, structured events with confidence, salience, entities, evidence, and links.
3. **Timing and content are separate decisions.** The system decides when to speak, interrupt, alert, or stay silent independently from what it should say or do.
4. **Reasoning runs at multiple speeds.** Fast reactive decisions, normal interactive responses, and slow deliberative work are handled by separate loops.
5. **Memory is hierarchical.** Hot, warm, cold, and procedural memory are managed explicitly instead of relying on a single large context window.
6. **Safety is enforced by the runtime.** Sandboxes, policy enforcement, resource limits, and observability guard actions without putting human approval on the hot path.
7. **Locality is a first-class policy.** Computation should happen as close as practical to the device, signal, data, or action it affects.
8. **Distribution is native.** Nodes, devices, internal model sessions, events, world-state, and memory must be identity-aware and synchronization-aware from the beginning.
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

The local service entry point is `sinomo daemon`. It is intentionally small: a foreground service command plus CLI controls that can be wrapped by systemd, launchd, a shell supervisor, or a Sinomo-owned service manager.

- `sinomo daemon run` runs the daemon in the foreground.
- `sinomo daemon start` launches a detached local background process.
- `sinomo daemon status` probes both the data-directory lock and the listener.
- `sinomo daemon stop` asks the running daemon to shut down.
- `sinomo daemon shadow run|start|status|stop` controls the shadow daemon on `127.0.0.1:21354` by default.

The daemon listens on `127.0.0.1:21353` by default. The bind host and port can be overridden with `--host` and `--port`, but the default should remain stable so local tools can discover the service without a separate registry.

Each daemon instance owns its process lock and writes a binary Sinomo status object. The main daemon also owns `$SINOMO_DATA_DIR/daemon/data-owner.lock`, which is the exclusive data-directory ownership token for normal node operation. The shadow daemon owns only its separate `$SINOMO_DATA_DIR/shadow-daemon/` process lock and status file. This lets shadow remain reachable for supervision and rollout without competing with main for cached cluster/DFS state. The status object records the PID, daemon version, target OS/architecture, executable path, bind address, start time, data directory, and lock path so CLI commands can distinguish three cases: running and reachable, locked but not reachable, or not running.

The main daemon is the local cluster member. The shadow daemon is no longer recorded as a normal `/cluster/members/<node_id>` participant. Instead, shadow endpoints are folded into the main node's advertised endpoint set so remote nodes can still reach the installation when main is down and shadow is listening. Shadow status uses the main node identity with `process = daemon-shadow`; it is a supervision and routing endpoint, not a KV quorum participant, scheduler target, DFS owner, or normal workload placement target.

Daemon lifecycle is local infrastructure and is allowed before cluster registration. This lets `sinomo install` prepare a machine and start both daemons before the operator runs `sinomo cluster init` or `sinomo cluster join`. A data directory with no cluster state is a pre-bootstrap or recovery condition, not a normal lifecycle mode. In that condition, daemon status reports no cluster registration and the daemon does not publish cluster KV heartbeats. After `cluster init` or `cluster join` creates local cluster state, the running daemon updates its status registration and begins publishing member and node-status records without requiring a restart.

The default `sinomo` command is daemon-first. When the local main daemon is running and registered with a cluster, the CLI treats that daemon as the local cluster runtime for the cluster-state guard and starts the TUI session under the daemon's cluster node identity. If no daemon is reachable, the CLI falls back to the local cluster state and reports bootstrap guidance when neither path exists. Explicit lifecycle commands such as `sinomo daemon run`, `sinomo daemon start`, and `sinomo install` remain opt-in process-management operations.

Node operation is cluster-based. A machine that starts alone is represented as a single-node cluster, and adoption into another cluster is handled as cluster migration. `sinomo cluster migrate plan <invite>` validates the source cluster and target invite without mutating local state. `sinomo cluster join <invite>` on an initialized solo node uses the single-node migration path, executed by the daemon through `/cluster/control/join`: the daemon writes a migration record, quarantines old cluster state/KV/Raft storage under `$SINOMO_DATA_DIR/cluster/migrations/<migration_id>/source/`, moves existing DFS metadata under the import namespace `/sinomo/imports/<source_cluster_id>/...`, and records the target-cluster join with the preserved local node identity. Target cluster KV wins after the join; source cluster KV is kept as evidence/import source only, cluster-level config is skipped, and only node-local `/config/nodes/<source_node_id>/...` config may be reapplied to target KV as normal KV operations. Shared DFS data import is a separate policy-governed operation. `sinomo cluster leave` is the reverse lifecycle edge for an installed node: it leaves the current cluster by quarantining old cluster state/KV/Raft/watch storage under `$SINOMO_DATA_DIR/cluster/migrations/<leave_id>/leave-source/`, then creates a fresh single-node cluster with the preserved local node identity. The left cluster's KV remains archived evidence and is not imported into the new single-node cluster.

User-facing node operations are cluster-centered and daemon-owned. Normal cluster operations — `cluster init`, `join`, `invite`, `rename`, `role`, `remove`, and `leave` — reach the daemon through one typed daemon API client shared by the CLI and the app, backed by the `/cluster/control/{init,join,invite,rename,role,remove,leave}` object-RPC routes that `sinomo-cluster` registers; they require a running daemon. Documented debug/repair commands stay direct against local state: `cluster doctor`, `cluster connectivity`, `cluster repair-members`, `cluster cleanup-legacy-shadow-members`, `cluster state-authority-role`, `cluster migrate` planning, the `debug *` commands, and local daemon/install/rollout lifecycle. Local degraded-mode helpers remain internal fallback plumbing for commands that can read local state when the daemon or quorum is unavailable.

The daemon hosts an internal cluster KV service adapter. The service is not exposed as a public API; Sinomo components use binary object RPC routes to read snapshots, submit KV transactions, range over prefixes, and create/drain local prefix watches. `sinomo-kv` owns KV storage semantics, Raft participant state, leader/election state, term/vote/log/snapshot behavior, and durable KV files under `$SINOMO_DATA_DIR/cluster/kv-raft/<node_id>/` plus the materialized snapshot at `$SINOMO_DATA_DIR/cluster/kv-snapshot.sobj`. `sinomo-network` owns the cluster network and routing layer: routes, endpoint selection, connectivity graph interpretation, relay decisions, and peer transport selection. The two crates do not know each other: `sinomo-network` has no dependency on `sinomo-kv`, and `sinomo-kv` has no dependency on `sinomo-network` (the workspace boundary test enforces both directions). `sinomo-cluster` is the bridge. It owns the transport-coupled control plane — membership, invites, joins, member/state/connectivity/status publishing, and the KV/Raft replication drivers that move consensus traffic over network-selected routes — and it implements the network crate's `ClusterDirectory` provider by reading cluster KV (`install_kv_cluster_directory()`), so routing and endpoint selection read cluster state (members, endpoints, node statuses, connectivity records) only through that registered directory. Connectivity records remain network-domain facts stored in KV: `sinomo-cluster` publishes and serves them, and `sinomo-network` interprets them for routing. No crate absorbs another's domain rules. The daemon supplies host-local authentication, process identity, and peer transport callbacks for those crates. Cluster init, daemon registration, and join registration store cluster identity, Raft role config, member records, endpoint records, capability policy, invitations, and rollout desired/node state under the `/cluster/...` keyspace, so cluster status and peer views can report the KV-backed view rather than only local state. Cluster maintenance sends Raft append heartbeats from the current leader, asks authorized voters for votes when the leader is unreachable, persists a new leader only after quorum, clears local leadership when quorum is lost, and fans out committed watch events with signed daemon messages. Daemons also publish runtime heartbeats under `/cluster/node-status/<node_id>`. Those records include online observation time, daemon version, platform, PID, control address, executable path, and rollout state summary.

`sinomo cluster health` reads the KV-backed member and node-status records, then probes reachable daemon endpoints for a fresh live view. The report shows each cluster member with online/offline/unknown health, version, platform, PID, address, last-seen time, and rollout status. Shadow reachability is represented through the main member's alternate daemon endpoints rather than a separate member row. `sinomo cluster status` includes online/offline counts from the same health rows. A node is treated as offline when it has a probeable endpoint but its last online heartbeat is stale; nodes without a usable endpoint remain unknown rather than falsely offline.

Each local daemon has an Ed25519 identity under `$SINOMO_DATA_DIR/identity/`. The main daemon identity is the cluster member identity. The shadow daemon retains a local identity for signed supervision/routing traffic, but it does not publish a separate membership record. Daemon control traffic, including loopback traffic, is signed with `sinomo.daemon-auth.v1`; the daemon verifies the signature, timestamp, and signer public key against local identity or authorized cluster membership before executing control and KV commands.

`sinomo install` promotes the current binary into the local data directory and wires both daemons into local boot startup:

- `sinomo install [--host <host>] [--port <port>] [--shadow-port <port>] [--no-start]`
- `sinomo uninstall [--no-stop]`

Install creates `$SINOMO_DATA_DIR`, copies the current executable to `$SINOMO_DATA_DIR/bin/sinomo`, exposes that binary as `~/.local/bin/sinomo` on Unix-like systems, deploys the current checkout into the managed source repo, builds installable native-dylib modules from that source, activates their content-addressed loader state, writes a boot entry for the main daemon and a boot entry for the shadow daemon, and starts both daemons unless `--no-start` is set. When an installed binary already exists and the command is run from a different executable, `sinomo install` acts as an update command: it stops the installed shadow daemon and main daemon, waits for their locks to clear, replaces the installed binary, rewrites boot entries, refreshes the user-bin symlink, and restarts both daemons unless `--no-start` is set. It also restarts already-running installed daemons when their bind host or port does not match the requested install options. The install default bind host is `0.0.0.0` so a freshly installed node can accept LAN cluster traffic; explicit operators can use `--host 127.0.0.1` for local-only installs. Daemon status reports both the bind address and the local control address; a wildcard bind uses `127.0.0.1:<port>` as the local CLI control address while still listening on the wildcard bind.

On Windows the boot entries are per-user Startup `.cmd` files because service registration commonly requires elevated privileges. On Unix-like systems the boot entries are systemd user service files when a user systemd config directory is available. `sinomo uninstall` removes only those boot entries and optionally stops the daemons; it intentionally retains `$SINOMO_DATA_DIR` and the installed binary so cluster identity, KV state, and local metadata survive reinstall.

### Self-Rollout Source and Shadow Daemon

Self-rollout treats Sinomo's own source as durable node-local state. `sinomo rollout init-source` creates `$SINOMO_DATA_DIR/source/sinomo` from the current repository unless a different `--repo` is supplied. The managed source is a local git repository with external remotes removed, so rollout and self-evolution do not depend on GitHub, GitLab, or any upstream service outside the Sinomo cluster. Imports merge external checkout changes into the managed source instead of replacing it, so self-evolution commits made inside the managed source are preserved. When `sinomo evolve propose` does not receive an explicit `--repo` or configured repo, it uses this data-directory source repository so approved self-evolution work lands in Sinomo's maintained source rather than the operator's checkout.

Approved evolution decisions commit the candidate worktree and fast-forward the source repository. Rollout is then an explicit local operation:

- `sinomo rollout status` prints the data-directory source, installed main binary, installed shadow binary, and rollout state paths.
- `sinomo rollout import-source [--repo <path>]` merges an external git checkout into the managed source and removes external remotes afterward.
- `sinomo rollout export-source --repo <path>` merges managed source changes, including self-evolution commits, back into an external git checkout.
- `sinomo rollout apply-external [--repo <path>]` imports external changes, then builds and applies the managed source.
- `sinomo rollout apply` builds `$SINOMO_DATA_DIR/source/sinomo` with `cargo build -p sinomo` plus the installable native-dylib module packages.
- `sinomo rollout clear-status [--target <host:port>]` removes stale rollout status without deleting pending rollout artifacts.
- `sinomo rollout cluster [--repo <path> | --binary <path>] [--target <host:port>] [--remote-build] [--remote-repo <path>]` supports development clusters where each node has been installed with `sinomo install`. Compatible targets can receive a binary built by the initiating CLI. Cross-platform repo rollouts use remote build: the target daemon builds from its own managed source and stages candidate bytes under its rollout directory. `--remote-repo` points at the target node's local checkout; when omitted, the daemon uses `$SINOMO_DATA_DIR/source/sinomo`.
- The build log is written to `$SINOMO_DATA_DIR/rollout/build.log`.
- The produced binary is copied atomically to stable `$SINOMO_DATA_DIR/bin/sinomo` and `$SINOMO_DATA_DIR/bin/sinomo-shadow` executables.
- Native-dylib module artifacts are validated through their descriptors, copied into content-addressed `$SINOMO_DATA_DIR/module/cache/native-dylib/...` paths, and activated by node-local loader state before the candidate daemon handoff.
- The main daemon is stopped, the shadow daemon is restarted from the installed shadow binary, and the shadow daemon starts the installed main binary and verifies it becomes healthy.

The shadow daemon owns separate state under `$SINOMO_DATA_DIR/shadow-daemon/` so it does not contend with the main daemon's lock. Its steady-state responsibility is intentionally narrow: it supervises the main daemon process that it launched. If that process exits with a crash-class exit code or crash-class Unix signal, the shadow daemon records `$SINOMO_DATA_DIR/rollout/last-main-exit.sobj` and restarts the main daemon. If the main daemon exits cleanly or is terminated/killed by operator action, the shadow daemon records the event and does not restart it. This preserves a difference between real crashes and intentional intervention.

Cluster rollout supports both supervised and main-only activation. The running main daemon stages candidate main and shadow binaries, replies to the rollout request, and exits. If a local shadow daemon is running, the existing shadow stays alive, switches only the supervised main to the candidate, and waits for the main daemon to answer health probes. If the candidate cannot execute or exits before becoming healthy, the shadow records rollout failure in `$SINOMO_DATA_DIR/rollout/state.sobj`, restores the previous main binary, and keeps supervising. Only after the candidate main is healthy does the main daemon finalize `$SINOMO_DATA_DIR/rollout/pending-shadow-rollout.sobj` by replacing the shadow daemon. If no local shadow daemon is running, the main daemon starts a staged self-restart candidate before it exits; that candidate installs itself as the stable main binary and records rollout success without requiring shadow participation. If candidate shadow startup fails after a supervised main activation, the healthy main daemon attempts to restart the previous shadow binary and records the recovery result.

Development cluster rollout may target an explicit `--target <host:port>` or cluster metadata endpoints. `sinomo install` deploys the source checkout into the managed source repo at `~/.sinomo/source/sinomo`, merging the external checkout on subsequent installs so daemon-backed remote build works without a separate `rollout init-source` step. The remote daemon stages candidate bytes under its rollout directory, then activates them into stable `bin/sinomo` and `bin/sinomo-shadow` executables one process at a time so firewall rules can target stable binary paths while Windows file locking is respected. Binary rollout is platform-specific: daemon status advertises target OS/architecture, the rollout client skips incompatible or unknown remote platforms for binary pushes, and the target daemon rejects mismatched artifacts before stopping its running processes. Compatible binary-push rollout carries matching native-dylib module bytes in the rollout request. Remote-build rollout builds and activates module artifacts on the target from the same managed source used for the daemon binary. When the rollout source is a repo and a remote target is incompatible or unknown, the CLI uses daemon-backed remote build from the target's managed source repo instead of shipping the local binary. Target rollout activation and build do not require git remotes or internet access.

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
| Chat/channel state | Multi-node | Synchronize user-visible timelines, participants, active devices, and current conversation/task state |
| Model session state | Local node first, with durable traces | Preserve the internal model-turn timeline, context projection, compaction state, and handoff metadata |
| Event log | Partitioned by origin node/internal session | Replicate durable event envelopes and evidence references according to policy |
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

### Chats, Channels, and Model Sessions

Chats and channels are the user-facing collaboration surfaces. They own
participants, visible timelines, active user interaction surfaces, and
cross-device continuity.

Model sessions are internal execution units used by `sinomo-agent` to drive
model turns. A model session owns the model-facing projection transcript,
context assembly state, compaction state, queued steering/follow-up input, and
turn traces. It is not the participant model, and it should not be treated as a
user-visible conversation object.

Within one model session, everything the model sees is a context-assembly
decision. The memory system may contribute retrieved blocks, but it does not own
same-session persistence, maintain the active projection, or decide what
same-session information remains visible. Same-session continuity is context
management.

Memory retrieval becomes authoritative only at boundaries: crossing model
sessions, resuming older work, moving between chats/channels, or bringing
durable task, workspace, user, or cluster memory into a fresh context.

Chat/channel responsibilities:

- Track participants and user-visible conversation state.
- Track active user interaction surfaces.
- Track active action targets.
- Preserve conversational continuity across devices.
- Route commands to the node closest to the action target.
- Allow a node to join, leave, sleep, reconnect, or fail without losing the chat
  or channel timeline.

For example, when a user speaks on mobile and asks Sinomo to change something
on the desktop, the mobile node can publish a speech/intent event into the chat
or channel timeline. A desktop-side model session can be created or resumed to
handle that turn because the desktop owns the relevant screen and input devices.

### Distributed Event Routing

Event routing should support both local processing and cross-node propagation.

Routing rules should consider:

- Event family and salience.
- Producing node and device.
- Chat/channel membership or internal model-session lineage.
- Target entity, task, device, or node.
- Privacy classification.
- Required latency.
- Subscriber capabilities.

The same event contract should work for local and distributed cases. Distribution should add routing metadata, not create a separate remote event type.

### Distributed Filesystem

Sinomo should include a distributed filesystem component for agent workspaces, staging areas, generated artifacts, coding tasks, and handoff between nodes. Its default exposed filesystem is SIFS, the cluster root filesystem described in [SIFS](sifs.md).

The DFS should be local-first. A file object can live primarily on one node, be replicated to several nodes, or require distributed write confirmation. The policy is attached to the object, directory, workspace, or mount, not hardcoded by file type.

SIFS is mixed-architecture: `/sinomo/nodes/<node_id>/files` exposes a node's local files as a `local-owned` subtree, while `/users` and `/services` are distributed DFS subtrees for cluster-shared user and service state.

Core use cases:

- Agent coding workspaces and repository checkouts.
- Task staging directories.
- Generated artifacts and intermediate files.
- Local evidence bundles that can be selectively shared.
- Cross-device handoff where one node edits, another builds/tests, and another reviews.
- Recovery after node sleep, disconnect, or restart.

The DFS is not meant to replace Git, optional external databases, or local filesystems. It provides a policy-aware workspace layer above local storage and below agent actions.

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
- `replicated-eventual` writes are locally accepted with version metadata and reconciled asynchronously.
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

The crate graph now makes this trust-ring cut explicit. Each workspace crate declares `[package.metadata.sinomo] ring = "..."` in its `Cargo.toml`, and two facade crates pin the cut: `sinomo-core` is the R0 boundary facade (it aggregates the minimal set of crates that defines the cluster and adds no behavior) and `sinomo-runtime` is the R1 boundary facade (pure re-exports over `sinomo-core` plus the R1 crates). R0 crates depend only on R0 crates. R1 crates cross the R0 boundary through `sinomo-core`, R2 services and application crates cross the foundation boundary through `sinomo-runtime`, and R0/R1 crates must not depend on R2 services or application crates. `crates/sinomo-core/tests/workspace_ring_boundaries.rs` enforces these rules, including the decoupling rule inside R0: `sinomo-network` must not depend on `sinomo-kv` or `sinomo-cluster`, `sinomo-kv` must not depend on `sinomo-network` or `sinomo-cluster`, and `sinomo-cluster` is the only bridge between them.

Self-evolution is intentionally above this foundation. `sinomo-evolution` is an R2 service that owns evolution jobs, mutation packets, candidate bundles, approval records, and campaign state. R0/R1 crates expose generic primitives such as events, artifacts, scorecards, registry evidence, rollout plans, sandbox workloads, and module loader state; they do not know whether those primitives were produced by self-evolution, an operator, or another delivery system.

Current crate ownership:

| Group | Crates |
|---|---|
| R0 | `sinomo-base`, `sinomo-object`, `sinomo-storage`, `sinomo-local`, `sinomo-node`, `sinomo-events`, `sinomo-network`, `sinomo-kv`, `sinomo-cluster`, `sinomo-service` |
| R0 boundary | `sinomo-core` |
| R1 | `sinomo-daemon`, `sinomo-dfs`, `sinomo-dfs-cloud-files-api`, `sinomo-module`, `sinomo-module-api`, `sinomo-registry`, `sinomo-rollout`, `sinomo-policy`, `sinomo-eval`, `sinomo-config`, `sinomo-observe`, `sinomo-device`, `sinomo-supervision`, `sinomo-cli-registry`, `sinomo-test`, the `sinomo-sandbox*` crates, and the native module packages under `modules/` |
| R1 boundary | `sinomo-runtime` |
| R2 | `sinomo-actions`, `sinomo-agent`, `sinomo-audio`, `sinomo-deliberation`, `sinomo-evolution`, `sinomo-fusion`, `sinomo-gateway`, `sinomo-lm`, `sinomo-memory`, `sinomo-models`, `sinomo-pytorch`, `sinomo-video`, `sinomo-voice` |
| Application/tooling | `sinomo`, `sinomo-app`, `sinomo-cli`, `sinomo-dfs-fuse`, `sinomo-testdir` |

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

### R1 Support Contracts

The foundation plane must provide these R1 contracts:

- Stable trust ring metadata on modules, jobs, policies, and artifacts.
- Module manifest and artifact identity types in `sinomo-base`, re-exported at the `sinomo-core` root.
- An explicit `sinomo-runtime` facade that aggregates only R0/R1 crates and gives R2 services a stable foundation import boundary.
- Workspace architecture tests that reject missing ring metadata, R0/R1 dependencies on R2/application crates, and direct R2/application dependencies on R0/R1 crates outside `sinomo-runtime`.
- A policy decision boundary that candidate code cannot bypass.
- An artifact registry model backed by Sinomo durable metadata and DFS/local content storage, with optional PostgreSQL deployment backends.
- Node-local module loader state with current, previous, shadow, and candidate versions.
- Hot-swap state with activation, drain, quiesce, and rollback checkpoints.
- Rollout desired-state records that can be evaluated per node.
- Rollback events that point to known-good artifacts.
- Evaluation scorecards that are machine-readable and replayable.
- Sandboxed mutation workspaces using DFS snapshots and object policies.
- Event families for `evolution.*`, `module.*`, `artifact.*`, `eval.*`, and `rollout.*`.

R1 services are trusted runtime components. Their contracts must support
autonomous live replacement without taking down the node.

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

Provides the runtime substrate for nodes, devices, chats/channels, internal
model-session handoff, routing, and synchronization.

Responsibilities:

- Node identity and registration.
- Device identity and capability advertisement.
- Chat/channel membership and model-session handoff.
- Local and cross-node event routing.
- State synchronization policies.
- Distributed workspace and file-object policies.
- Capability discovery.
- Trust, authentication, and node-local sandbox policy boundaries.

Primary crates: `sinomo-node`, `sinomo-device`, `sinomo-network`, `sinomo-cluster`, `sinomo-service`, `sinomo-dfs`.

### Ingestion Plane

Receives live signals and normalizes them into timestamped chunks.

Responsibilities:

- Accept microphone, camera, screen, text, telemetry, webhook, and local system events.
- Normalize source IDs and timestamps.
- Attach node and device identity.
- Chunk streams into bounded units.
- Apply buffering, backpressure, and fan-out.
- Preserve raw evidence references for replay.

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
- Maintain entity graph, chat/channel state, and internal model-session state.
- Track active tasks, hypotheses, unresolved questions, and attention queues.
- Provide replayable decision traces.
- Feed relevant context to memory and control loops.
- Merge state updates from participating nodes.
- Preserve local-first operation when other nodes are unavailable.

Primary crates: `sinomo-events`, `sinomo-memory`, `sinomo-base`.

### Agent Control Plane

Decides what Sinomo should do next. This plane is split into three loops:

| Loop | Target latency | Purpose |
|---|---:|---|
| Fast reactive loop | 50-150 ms | Interrupts, safety triggers, wake/no-wake, alert suppression, immediate acknowledgements |
| Interactive loop | 200 ms-2 s | Dialogue, short retrieval, lightweight tool calls, task state, compact responses |
| Deliberative loop | 2 s+ | Planning, reflection, branch comparison, memory consolidation, long-running generation |

Primary crates: `sinomo-agent`, `sinomo-deliberation`.

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

Primary crates: `sinomo-actions`, `sinomo-voice`.

## Workspace Crate Layout

The repository is organized as a Rust workspace with shared contracts and
focused service crates.

```text
sinomo-agent/
  Cargo.toml
  Cargo.lock
  README.md
  docs/
    architecture.md
    proposal.md
    research_notes/
  crates/
    # R0 cluster foundation
    sinomo-base/          # Shared IDs, event envelope, domain types, scalars
    sinomo-object/        # Sinomo Object (SOBJ) wire codec
    sinomo-storage/       # Storage layout and durable object schema contracts
    sinomo-local/         # Local data-directory layout
    sinomo-node/          # Node identity, local runtime metadata, capability model
    sinomo-events/        # Event persistence, replay, entity/world-state reducers
    sinomo-network/       # Pure networking library: object RPC, routing, transports, network modules
    sinomo-kv/            # Cluster KV storage and Raft consensus core (transport-free)
    sinomo-cluster/       # Cluster control plane: membership, invites, joins, replication drivers
    sinomo-service/       # Service registration, discovery, routing target selection
    sinomo-core/          # R0 boundary facade aggregating the cluster foundation crates
    # R1 trusted runtime
    sinomo-daemon/        # Local daemon: runtime registry, cluster operator, control routes
    sinomo-device/        # Device registry, source/action target metadata
    sinomo-dfs/           # Local-first distributed workspace filesystem
    sinomo-dfs-cloud-files-api/ # Stable Cloud Files DFS module ABI
    sinomo-policy/        # Trusted policy decision contracts and verifier
    sinomo-registry/      # Module artifact registry and provenance metadata
    sinomo-module/        # Module lifecycle, registry, routing, native loading, and node-local loader state
    sinomo-module-api/    # Stable native dynamic-library module ABI
    sinomo-sandbox/       # Sandbox contracts and sandboxed candidate execution
    sinomo-sandbox-simple/ # Minimal sandbox backend
    sinomo-sandbox-wasix-api/ # Stable WASIX sandbox module ABI
    sinomo-sandbox-wasix-host/ # WASIX sandbox host loader
    sinomo-eval/          # Evaluation definitions, replay suites, and scorecards
    sinomo-rollout/       # Progressive rollout and rollback controller
    sinomo-config/        # Local and replicated configuration
    sinomo-observe/       # Operational events, logs, and cluster observation
    sinomo-supervision/   # Supervision and incident contracts
    sinomo-cli-registry/  # CLI command registry contracts
    sinomo-test/          # Shared test harness utilities
    sinomo-runtime/       # R1 boundary facade over sinomo-core plus the R1 crates
    # R2 trusted services
    sinomo-gateway/       # Ingress adapters and session wiring
    sinomo-audio/         # Audio orchestration and audio event production
    sinomo-video/         # Video/screen orchestration and visual event production
    sinomo-fusion/        # Cross-modal alignment on a shared timeline
    sinomo-memory/        # Hot/warm/cold/procedural memory and retrieval policy
    sinomo-agent/         # Interactive controller and dialogue/task loop
    sinomo-deliberation/  # Long-running planning, reflection, and generation jobs
    sinomo-actions/       # Tool, automation, notification, and environment actions
    sinomo-voice/         # TTS orchestration and duplex audio output control
    sinomo-lm/            # Language-model provider clients
    sinomo-models/        # Model catalog and provider configuration
    sinomo-pytorch/       # PyTorch-backed inference workers
    sinomo-evolution/     # Evolution job orchestration
    # Application / tooling
    sinomo/               # CLI binary entry point (wraps sinomo_cli::run)
    sinomo-cli/           # CLI surface: commands and TUI
    sinomo-app/           # Graphical app
    sinomo-dfs-fuse/      # FUSE mount for DFS
    sinomo-testdir/       # Test data-directory tooling
  modules/
    benchmark/sinomo-benchmarks/ # Benchmark harness package (not a default build member)
    dfs/cloud-files/      # Windows Cloud Files DFS integration native-dylib module
    remote/management/    # Self-contained trusted native-dylib shell, filesystem, and system-info module
    sandbox/wasix/        # WASIX sandbox runtime native-dylib module
    network/iroh/         # Iroh roaming transport native-dylib module (lands in stage C of the crate-boundary refactor)
```

## Crate Responsibilities

### `sinomo-base`

Shared domain contracts used by all other crates. It sits at the bottom of the R0 ring; crates outside R0 consume it through the `sinomo-core` facade, which re-exports the base contracts at its root.

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
- `EvaluationScorecard`
- `RolloutPlan`
- `RollbackPolicy`

This crate should avoid service-specific dependencies. It owns stable schemas, IDs, serialization, error types, and shared time/source abstractions.

### `sinomo-core`

The R0 boundary facade: the minimal set of crates that defines the cluster.

Responsibilities:

- Aggregate exactly the R0 crates and add no behavior of its own.
- Re-export the `sinomo-base` contracts at the crate root so `sinomo_core::...` paths keep working.
- Expose the other R0 crates as modules: `sinomo_core::{object, storage, local, node, events, cluster, network, kv, service}`.
- Give R1 crates one stable import surface so the trust-ring cut stays visible in the Cargo graph.

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

Pure networking library: object RPC, packet routing, transports, and the network-module system. Like a web framework, it owns the request/response mechanism while domain crates register their routes into it and own the business logic behind them.

See [Sinomo Network](network-architecture.md) for the communication boundary, wire-format rules, routing algorithm, and the network-module system.

Responsibilities:

- Own the object RPC router, route registry, middleware, and service catalogs for cluster communication.
- Implement the canonical packet routing algorithm: endpoint selection, connectivity-graph interpretation, relay decisions, retries.
- Provide peer transports (loopback, LAN TCP, roaming) and LAN discovery.
- Sign and verify daemon control transport (`sinomo.daemon-auth.v1`).
- Own the Sinomo Object wire codec dependency for cluster framing.
- Host the runtime network-module registry so transports can be added, enabled, and disabled as modules, including non-TCP/IP transports.
- Read cluster state (members, endpoints, node statuses, connectivity records) only through the `ClusterDirectory` provider registered by `sinomo-cluster`; the crate has no dependency on `sinomo-kv` or `sinomo-cluster`.

### `sinomo-kv`

Cluster KV storage and Raft consensus core. Transport-free: it never opens a socket and has no dependency on `sinomo-network` or `sinomo-cluster`.

Responsibilities:

- Own KV storage semantics: models, WAL, snapshots, revisions, transactions, leases, and prefix watches.
- Own Raft participant state: term/vote/log/snapshot behavior and the election/replication state machines.
- Own durable KV files under `$SINOMO_DATA_DIR/cluster/kv-raft/<node_id>/` and the materialized snapshot at `$SINOMO_DATA_DIR/cluster/kv-snapshot.sobj`.
- Define the `ClusterKvRaft*Host` traits that transport owners implement; hosts build their own error type through the traits' associated `kv_error` constructor rather than a `From<KvError>` bound.

### `sinomo-cluster`

Cluster control plane: the only bridge between `sinomo-kv` and `sinomo-network`.

Responsibilities:

- Own membership, invites, join registration, and member mutation drivers.
- Publish member, state, connectivity, network-info, and node-status records to cluster KV.
- Own the KV/Raft replication, election, quorum, and committed-watch fanout drivers that move consensus traffic over network-selected routes.
- Own cluster health rows and doctor findings.
- Own cluster bootstrap-state I/O for `$SINOMO_DATA_DIR/cluster/state.sobj`.
- Implement the network crate's `ClusterDirectory` provider on top of cluster KV (`install_kv_cluster_directory()`), so routing and endpoint selection see cluster state without a network-to-KV dependency.
- Register the `/cluster/control/{init,join,invite,rename,role,remove,leave}` object-RPC routes for daemon-mediated cluster operations.

### `sinomo-service`

Service registration, discovery, and generic routing target selection.

Responsibilities:

- Own `Service` and `ServiceRegistry`: per-service object-RPC route ownership with a host-agnostic async dispatch seam that works in-process or behind the daemon.
- Publish one `ServiceInstanceRecord` per locally registered service under `/cluster/services/<service>/<node_id>`; the daemon refreshes the records on its maintenance tick.
- Discover service instances by reading that prefix from a cluster-KV snapshot or `ClusterAccess`.
- Select routing targets with `ServiceTargetSelector` and the `LocalFirst`/`LowestCost`/`RoundRobin`/`All` policies, consulting the network layer's `ClusterDirectory` for reachability instead of owning routing rules.

### `sinomo-dfs`

Local-first distributed filesystem for agent workspaces and task staging.

Responsibilities:

- Manage workspaces, mounts, paths, object IDs, and versions.
- Store file/object metadata in Sinomo durable storage, with optional external database backends.
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
- Verify content hashes before loading native dynamic-library module artifacts.
- Check ABI, interface, module identity, and platform descriptors.
- Invoke native dynamic-library modules through the generic `sinomo-module-api`
  C/SOBJ boundary.

### `sinomo-module-api`

Stable native dynamic-library module ABI.

Responsibilities:

- Define C-compatible byte buffer, status, descriptor, host callback, and
  invocation envelope types for trusted native dynamic-library modules.
- Carry optional self-described module surfaces, including object-RPC routes and
  embedded declarative resources, so the host can expose a dylib without
  module-specific code in the main binary.
- Define generic SOBJ request and response envelopes for native module
  invocation.
- Stay independent of dynamic loading, sandbox execution, Wasm runtimes, and
  high-level services.

### `sinomo-sandbox`

SIFS-backed WASM sandbox host for agent tool execution, candidate builds, tests, and R3 modules.

Responsibilities:

- Maintain sandbox instances and command workspaces under `/services/sandbox`.
- Run pinned real WASI command toolchains through the sandbox host without ambient host filesystem or network access.
- Persist stdout, stderr, command records, checkpoints, and changed workspace files through SIFS so work can resume after daemon restart or node handoff.
- Apply cluster-scoped sandbox policy for mounts, command capabilities, toolchain trust, resource budgets, concurrency, and mediated HTTP fetches.
- Provide a path toward Wasm Component Model modules for R3 capsules and richer toolchain packages.
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

Memory service, storage, retrieval policy, and consolidation. The detailed
design lives in [memory-architecture.md](memory-architecture.md).

Responsibilities:

- Maintain hot context and active state.
- Promote salient events into warm episodic memory.
- Maintain semantic/cold memory records and embeddings.
- Manage procedural memory references.
- Return bounded context packages to control loops.
- Preserve memory provenance, retrieval traces, and policy decisions.

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
  "chat_id": "chat_01...",
  "channel_id": "channel_ops",
  "internal_session_id": "sess_01...",
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
    "scope": "chat",
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

The event envelope lives in `sinomo-base` (re-exported at the `sinomo-core` root); persistence, replay, and reducers live in `sinomo-events`.

Distributed event fields:

- `origin_node_id`: node that produced the event.
- `origin_device_id`: device that produced the event, when applicable.
- `target_node_id`: preferred or required node for handling the event, when known.
- `target_device_id`: preferred or required action target, when known.
- `routing.scope`: local, chat, channel, trusted-network, or durable-global.
- `routing.latency_class`: reactive, interactive, background, or archive.
- `routing.privacy`: raw, derived, private, redacted, or public.
- `routing.hints`: capability and placement hints for routers and schedulers.

Filesystem-related events should include workspace ID, object ID, path, object policy, version, owner node, content hash when available, and write receipt metadata when the operation modifies state.

Evolution-related events should include trust ring, target module, artifact ID, source workspace, policy decision, scorecard ID, rollout plan, affected nodes, and rollback target when applicable.

## Memory Architecture

The detailed design lives in [memory-architecture.md](memory-architecture.md).
Sinomo memory is hierarchical and explicit: hot in-process state, immutable
episodic `MemoryRecord`s, derived semantic memory, and versioned procedural
memory. Memory is exposed to model turns through bounded context providers, not
hidden mutable state.

Memory storage uses Sinomo-native durable storage: append-only SOBJ memory
logs, compact snapshots, materialized indexes, and scope-partitioned SIFS
replication plans. PostgreSQL, vector databases, graph databases, and other
external services are optional provider backends rather than requirements of
the base memory architecture.

Memory writes should be selective. Events should be promoted when they are
salient, repeated, unresolved, user-important, needed for future planning,
linked to task success/failure, or required for safety and auditability.

Memory evaluation is layered. `sinomo-memory` tests cover deterministic store,
scope, replay, supersession, consolidation, and replication invariants. Cheap
cluster sanity checks should exercise configured memory visibility without
expensive provider calls. Expensive live/model evaluation belongs in
`sinomo-benchmarks`; the optional `agent-memory-quality` plan runs
`modules/benchmark/sinomo-benchmarks/agent/run_memory_eval.py` to score
retrieval quality, model usefulness, governance/scope behavior, and
cache-friendly context stability against real models.

## Storage, Routing, and Sync

The base runtime should avoid required external stateful dependencies. Early
deployments should avoid requiring message brokers, distributed caches,
dedicated search/vector databases, PostgreSQL, or separate object storage
services.

Application services may still use in-process channels, local files, direct peer
connections, HTTP/WebSocket APIs, operating-system primitives, or optional
external databases. Durable coordination should be owned by Sinomo services and
storage contracts first, with optional external stores hidden behind crate-level
abstractions.

### PostgreSQL

PostgreSQL is an optional deployment backend, not a required base-runtime
dependency.

Possible use cases:

- Event log and replay.
- Internal model-session and task state.
- Entity/world-state tables.
- Optional memory indexes or archives behind `sinomo-memory` provider traits.
- DFS workspace, object, version, and policy metadata.
- Module manifests, artifact registry metadata, provenance, signatures, and attestations.
- Evolution jobs, evaluation scorecards, policy decisions, rollout desired state, and rollback records.
- Job queues and long-running task state.
- Node/device registry.
- Routing inbox/outbox tables.
- Supervision and policy state.
- Semantic retrieval metadata and embedding storage.

If a deployment uses PostgreSQL, it should live on one durable home node or
small server node. Other nodes should cache local hot state and selected warm
state. The architecture must not require mobile or lightweight nodes to host a
full durable database.

PostgreSQL should not be the real-time hot path for every local decision. Fast
reactive loops should use local in-memory state and append durable events
asynchronously when possible.

### Event Routing Without a Broker

Without a broker dependency, cross-node routing should use a small application-level routing layer backed by Sinomo durable storage.

Recommended pattern:

- Local services communicate through in-process queues for hot paths.
- Each node writes durable events to an append-only event log when connected.
- Cross-node deliveries are represented in routing inbox/outbox records.
- Workers use polling with leases for durable work and delivery.
- PostgreSQL `LISTEN`/`NOTIFY` or equivalent backend-specific notifications can be used as an optimization for wakeups, not as the durable source of truth.
- Disconnected nodes buffer selected events locally and sync when reconnected.
- Direct peer links or home-node sync APIs can move events between devices while Sinomo durable storage remains the durable ledger.

### Distributed Workspace Filesystem

DFS metadata is owned by `sinomo-dfs`; content stays on local node filesystems by default.

Durable DFS metadata should track:

- Workspaces and mounts.
- Objects, paths, and tombstones.
- Versions and content hashes.
- Object policies and inherited directory policies.
- Node locations and replica state.
- Write intents, leases, receipts, and conflicts.
- Retention and cleanup state.

Content should remain on local node storage by default. Replication copies content between nodes according to object policy, while durable metadata records what should exist and what has been confirmed.

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

Use Sinomo durable storage for registry metadata and DFS/local content storage for artifact bytes.

Registry metadata should track:

- Module manifests and API versions.
- Artifact IDs, content hashes, signatures, and compatibility constraints.
- Source workspace, source revision, and mutation job.
- Build, test, eval, and policy attestations.
- Current, previous, candidate, shadow, draining, and rollback artifact state per node.
- Rollout plans, rollout progress, rollback records, and affected nodes.

Artifact content should be immutable. A rollback should point to a known-good artifact rather than mutating the failed artifact in place.

The registry is R1 infrastructure. Higher-level systems may produce candidate bundles or other evidence packages, but only trusted registry and policy paths can convert generic artifact evidence and signed release material into something loadable.

### Semantic Retrieval

Semantic retrieval is owned by `sinomo-memory`; see
[memory-architecture.md](memory-architecture.md). Retrieval uses local
materialized indexes plus in-process ranking over bounded candidate sets.

Embedding, graph, full-text, PostgreSQL, or vector-store providers are optional
backends. They must be rebuildable from source memory records and must not be
required by the base architecture.

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
- Selective durable checkpoints for recovery or handoff.

Distributed caches belong behind `sinomo-memory` abstractions rather than
leaking into core contracts.

### Media and Evidence Storage

Avoid requiring separate object storage in the base architecture. Store media and large evidence locally on the node that captured or generated it, with Sinomo durable metadata holding hashes, retention policy, and evidence references.

Use cases:

- Raw audio clips.
- Sampled frames.
- Screen captures.
- Generated media.
- Evidence attached to events.

Evidence references must include enough locality information to support local-only evidence, replicated evidence, and redacted evidence handles. Object stores are optional implementation details for deployments that need them, not part of the required dependency set.

## Synchronization Strategy

Sinomo should synchronize intent, state, and evidence at different levels of detail.

### Event Replication

Replicate durable event envelopes according to routing and privacy policy.

Default behavior:

- Reactive local events stay local unless they affect a shared chat/channel or remote action target.
- Chat/channel events replicate to active chat/channel participants.
- Internal model-session events replicate only as needed for traces, handoff, or recovery.
- Raw media references replicate only as evidence handles unless policy allows raw transfer.
- Summaries, embeddings, and derived observations can move more freely than raw streams.
- Blocked actions and safety incidents replicate to the supervision home node.
- DFS metadata events replicate according to workspace/object policy.
- Evolution, artifact, module, eval, and rollout events replicate to the registry/supervision home node and to affected rollout nodes.

### World-State Merge

World-state should be federated. Each node can maintain local reducers for the entities and devices it observes. Future chat/channel synchronization should merge updates into shared user-visible views once that design is concrete.

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
- Durable workspace metadata is recorded in Sinomo durable storage.
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
3. A trigger policy sees a high-salience interruption cue.
4. The fast reactive loop decides whether to interrupt, acknowledge, alert, or wait.
5. `sinomo-agent` retrieves recent context and produces a short response.
6. If ambiguity remains, `sinomo-deliberation` receives a follow-up job.
7. The deliberative result is written back as events and memory.
8. `sinomo-agent` delivers the follow-up when ready.

### Mobile-to-Desktop Action

1. The user speaks to the mobile frontend.
2. The mobile node runs VAD, wake policy, and lightweight audio perception locally.
3. The mobile node publishes a chat/channel event with transcript, embeddings, intent hints, and mobile device provenance.
4. `sinomo-network` routes the event to the desktop node because the desktop owns the target screen/action devices.
5. The desktop node retrieves local screen/UI state and relevant shared chat/channel context.
6. A local fast reactive or interactive controller decides whether the action is safe and executable.
7. `sinomo-actions` executes the desktop action locally.
8. Action results, screen changes, and any blocked-policy events are published back into the chat/channel timeline.
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
8. If a node disconnects mid-task, durable metadata and DFS version state allow another capable node to resume from the latest confirmed workspace state.

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
- Emergency stop and cancellation must propagate across active chat/channel participants and any backing model sessions.

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
