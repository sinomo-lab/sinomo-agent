# Sinomo Architecture

This document is the developer overview of Sinomo's runtime architecture. It
defines the system boundaries and invariants that other design documents refine.
It is not a command reference, implementation inventory, or delivery plan.

Readers new to the project should start with the
[system overview](system-overview.md). Operators should use the
[user guide](user/README.md), and developers should use the
[developer guide](developer/README.md). Future work and historical proposals
live under [plans/](plans/README.md).

## System Goals

Sinomo is a local-first, distributed runtime for a continuously operating AI
system. It combines live perception, structured events, bounded model turns,
durable memory, situated action, and controlled software evolution.

The architecture follows these principles:

1. **Perception remains available.** Ingestion and perception do not wait for a
   model turn, tool call, or long-running job to finish.
2. **Events are the shared contract.** Services exchange typed events with
   time, provenance, evidence, and routing metadata.
3. **Timing and content are separate decisions.** Fast policy decides whether
   and when to react; model-backed work decides what to say or do.
4. **Reasoning runs at different speeds.** Reactive, interactive, and
   deliberative work have separate latency and cancellation expectations.
5. **Locality is explicit.** Placement accounts for device ownership, latency,
   privacy, availability, and resource constraints.
6. **State has an owner.** Control state, event history, memory, workspace
   content, and artifacts use different consistency and replication policies.
7. **Authority is enforced by the runtime.** Remote requests, model output, and
   candidate code cannot bypass local policy or trust boundaries.
8. **Distribution is part of the base model.** A single machine is a one-node
   cluster, not a separate architecture.

## System Shape

```text
Inputs
  -> ingestion and perception
  -> events and world state
  -> reactive, interactive, and deliberative control
  -> actions and generated output

Shared foundation
  -> node identity and cluster control
  -> network routing and service discovery
  -> SIFS workspaces and durable storage
  -> memory, policy, observation, and controlled evolution
```

Each node can host services, models, storage, devices, and action adapters. Work
is placed near the data or device it affects when possible. Nodes exchange
derived events and evidence references instead of centralizing every raw input.

The event and world-state layer is the center of the runtime. Perception emits
events; agents, memory, policy, actions, supervision, and evolution consume and
emit them. This gives live execution, replay, and audit a common substrate.

## Runtime Entities

| Entity | Role |
|---|---|
| Cluster | Trust domain and replicated control plane for a set of nodes. |
| Node | Runtime location with an identity, services, storage, and capabilities. |
| Device | Concrete input or action surface owned by a node. |
| Chat or channel | User-visible conversation and collaboration timeline. |
| Model session | Internal execution state for model turns, context, tools, and compaction. |
| Service | Discoverable runtime capability hosted by one or more nodes. |
| Event | Typed, timestamped statement with provenance, evidence, and routing policy. |
| Workspace object | Versioned file or artifact governed by a DFS placement policy. |
| Build job | Cluster-scheduled conversion of authorized inputs into an immutable target-compatible artifact. |
| Module artifact | Versioned behavior that can be evaluated, activated, and rolled back. |

Chats and channels are user-facing. A model session is an internal execution
unit and must not become the source of truth for participants or visible
conversation history. Durable memory crosses session, chat, channel, and task
boundaries; context assembly controls what one model turn sees.

## Execution Model

Sinomo separates control work by latency and effect. The ranges below are
design targets used to shape scheduling and cancellation, not measured
cross-platform service-level guarantees:

| Loop | Design target | Responsibility |
|---|---:|---|
| Reactive | 50-150 ms | Wake decisions, interruption, immediate safety checks, and acknowledgements. |
| Interactive | 200 ms-2 s | Dialogue, bounded retrieval, short tool calls, and task updates. |
| Deliberative | 2 s and longer | Planning, branch comparison, consolidation, and long-running generation. |

The reactive path stays small and bounded. Interactive work is cancellable and
user-facing. Deliberative work runs as asynchronous tasks that can be steered,
superseded, or cancelled when newer events change the situation.

Placement uses explicit policy rather than a fixed central coordinator. Relevant
factors include device ownership, latency, model and accelerator availability,
battery and bandwidth, data residency, current node health, and action
authority. A remote node may propose an action, but the node that owns the
affected device or data performs the final authorization.

### Node-local resource conditions

Native application hosts publish battery, thermal, metering, and OS data-policy
conditions through the process-local `sinomo-device` resource monitor. These
observations are advisory scheduling inputs: they are not persisted, written to
cluster KV, or accepted as authority for a distributed mutation. Embedded
daemons subscribe to the same monitor, so a condition change is an exact wake
edge rather than another filesystem or control-plane polling loop.

Resource restrictions may defer passive discovery, observation, DFS placement,
repair, and replica catch-up. Foreground requests, local recovery, explicit
operator recovery, pins, W/N replication requirements, and final authority
checks retain their normal typed paths. DFS publishes only coarse metering and
resource-pressure evidence; exact battery and thermal readings remain local.
Every daemon samples physical DFS capacity at startup and after coalesced,
committed storage-dirty edges. Lease expiry, idle time, and resource callbacks
consume cached evidence and never become filesystem polling triggers; request,
maintenance, and reconciliation lanes consume the same cached projection.
The mobile embedded product profile also omits native build and host-filesystem
projection workers; those capabilities remain discoverable from suitable remote
nodes instead of creating idle toolchain, watcher, lease, or mount state on the
phone.
Android lifecycle, window-inset, network-capability, Data Saver, battery,
power-saver, and thermal changes arrive through framework callbacks registered
only while the Activity is resumed. The app therefore retains no visible-state
JNI polling timer for those signals, and a background Activity retains no
condition listeners. Hiding the Activity immediately projects Restricted mode
to its embedded daemon; a fresh framework-driven condition sample is required
to lift that restriction after the Activity returns.

## Distributed Foundation

### Cluster Control

The cluster provides identity, membership, compact replicated configuration,
service discovery, health, and placement facts. Its KV is a strongly ordered
control plane, not a general content database. Large files, media, model
weights, event archives, and high-cardinality filesystem metadata stay outside
the cluster KV.

KV convergence is fully reactive and action-owned. Exact commits own their
replication fanout, configuration operations own lifecycle reconciliation,
storage mutations own threshold compaction, detected gaps own targeted
recovery, and changed member or endpoint sources own publication. There is no
generic KV maintenance sweep, and observational status, health, app, or CLI
reads do not schedule control-plane work.

The daemon's resident applied Raft state is the canonical KV view. Its shared
materialized file is a derived observational checkpoint that may lag any
ordinary commit and cannot authorize DFS topology, placement, or destructive
work. Correctness-sensitive clients request bounded ReadIndex projections from
the daemon and reuse the resulting exact-key or prefix scope within that
validated generation; they do not cold-replay the daemon-owned Raft node.

Cluster discovery finds possible routes; it never grants authority. Membership
and signed identity establish trust. A node without cluster state is in a
bootstrap or recovery condition and may perform only local setup, credentials,
help, and cluster lifecycle operations.

Host and device exposure is separate node-local consent. Each node keeps its
own fail-closed permission document outside cluster KV and the remotely writable
configuration service. Module installation, activation, and route discovery do
not edit that document. Remote management, unsandboxed agent commands, and
unbounded computer control must be granted manually on the receiving node and
re-checked at the final host-effect boundary; device-facing work additionally
requires an exposure grant for the specific resource and operation. There is no
remote permission-management RPC. After remote management is granted, its shell
is intentionally equivalent to local administrator access, including the
ability to change ordinary node configuration.

One installation contributes one normal cluster member. The main daemon owns
cluster participation. The shadow daemon is a supervision and recovery endpoint
and does not vote, own DFS data, or accept normal workload placement.

See [clustering architecture](clustering-architecture.md) for membership,
consensus, identity, and failure semantics.

### Network Boundary

Cluster communication is composed from independent contracts. Direct transports
establish adjacent-peer identity once. Connectivity tracking publishes fresh,
directed evidence without granting membership. Routing uses the authorized
directory and connectivity snapshots to establish target-bound duplex streams,
including through relays. Routed records are plaintext by default; endpoint
callers may explicitly enable AEAD. Object RPC carries length-framed SOBJ
requests and responses without per-call signatures, encryption, or content
digests and does not know endpoints or node paths. Domain crates own target
selection, typed values, content verification, service methods, authorization,
idempotency, and side effects.

Connectivity contains direct observed edges only. Routing owns transitive
reachability and must realize an authorized graph path as a direct or relayed
stream with established endpoint identity; graph reachability is not a substitute for a usable
data path. Every authorized node pair in the same current routable connected
component must be able to communicate through routing.

Direct-link, connectivity, routing, and object-RPC mechanisms remain internal
layers of the single R0 `sinomo-network` crate. Their separation does not change
the workspace crate structure.

The network and KV cores have no production dependency on one another.
`sinomo-cluster` bridges them by moving consensus and membership traffic over
network-selected routes and by projecting committed cluster facts through the
network directory interface. This separation is enforced by workspace boundary
tests.

See [network architecture](network-architecture.md) for the layer contracts,
routed-stream guarantees, and transport behavior, and
[Sinomo Object format](sinomo-object-format.md) for the binary wire
representation.

### Filesystem and Content

SIFS is the default cluster filesystem. Its current durable policies are
local-owned, replicated-eventual, and replicated-confirmed. Append-only and
ephemeral-local are reserved modes tracked in the
[DFS hardening plan](plans/dfs-policy-and-authority-hardening.md). Policy belongs
to an object or subtree, so sensitive local files and shared artifacts can use
the same namespace without using the same placement rules.

DFS is a built-in filesystem-provider module which is enabled by default but
may be disabled on an individual node. Only nodes with an active DFS module
publish its service routes and run SIFS bootstrap, placement reconciliation,
metadata replication, and repair. Filesystem consumers discover providers by
capability and select a node; once capability records exist, cluster membership
alone does not make a node a DFS storage target. The destination still
re-authorizes every selected route. The node-local toggle is
`sinomo module native-rust disable module.dfs` (or `enable`); Settings → Modules
exposes the same enable/disable override for a selected node. A change is
applied when that node's daemon next constructs its active registry.

The cluster KV stores compact filesystem configuration and shard placement.
DFS metadata shards own paths, versions, intents, receipts, namespace
transactions, journals, and repair state. Content stores own immutable bytes
and manifests. DFS writes serialize through the relevant owner or metadata
primary and produce one authoritative visible history. A healthy publication
is acknowledged by the complete configured metadata shard without touching
Raft. If only a DFS recovery quorum is reachable, cluster KV may commit one
bounded, operation-specific permit and head watermark for the exact assignment
and epoch. This deliberately reuses the cluster's existing Raft consensus to
fence failover; it does not put DFS metadata or content in KV.

See [DFS architecture](dfs-architecture.md), the [SIFS layout](sifs.md), the
[direct NFSv3 adapter](nfs-architecture.md), and the
[Windows Cloud Files architecture](cloud-files-architecture.md) for host
filesystem presentation and its durability boundaries.

### Build and Artifact Distribution

Binary and module compilation is a cluster service, not a step that every
rollout target must perform. Rollout groups targets by explicit compatibility
requirements and asks the build service to resolve an immutable artifact for
each group. The service reuses verified artifacts first, coalesces matching
jobs, and otherwise schedules a compatible worker using fresh capability,
resource, queue, route, and cache-locality evidence.

Builders publish content-addressed artifacts and provenance. They do not choose
rollout lanes, change desired state, or activate their output. Rollout remains
the only cluster authority for distribution intent, staging, activation,
promotion, and rollback, and each target performs final authorization and
compatibility checks. See
[build service architecture](build-service-architecture.md).

The current first slice queries build capabilities directly from reachable
daemons, retains artifacts in each builder's local cache, and streams selected
bytes through the rollout client. Shared registry/DFS lookup and cluster-wide
leases remain follow-on coordination work.

## State and Data

State is separated by authority, durability, and replication cost:

| State | Source of truth | Normal distribution |
|---|---|---|
| Hot interaction state | Owning process or node | Local, with bounded handoff summaries. |
| Cluster configuration | Raft-backed cluster KV | Replicated to control-plane participants. |
| Events and traces | Append-oriented durable stores | Routed by scope, privacy, and retention policy. |
| Long-horizon memory | Versioned source/overlay files plus rebuildable indexes | Source and overlay files follow scope policy; derived indexes replicate by usefulness. |
| Workspace metadata | DFS metadata authority | Sharded and replicated by filesystem policy. |
| Workspace content | Node-local content stores | Copied or cached according to object policy. |
| Build requests and job coordination | Cluster build service with compact control-plane records | Queued, coalesced, leased, and observed by build identity. |
| Built binary and module artifacts | Trusted registry, DFS, and authorized caches | Immutable content distributed by target compatibility and demand. |
| Module and rollout desired state | Trusted rollout services | Distributed to affected nodes and applied only after local verification. |

The base runtime must not require PostgreSQL, a message broker, a distributed
cache, a vector database, or external object storage. Deployments may add those
behind subsystem interfaces, but external services cannot become hidden sources
of authority for base-runtime behavior.

The [storage architecture](storage-architecture.md) defines ownership and
inspection of files in the local Sinomo data directory. The
[memory architecture](memory-architecture.md) defines indexed-file memory,
automatic maintenance, retrieval, overlays, and multi-agent scope.

## Event Contract

Every cross-service event carries enough information to answer:

- what happened and when;
- which node and device produced it;
- which chat, channel, model session, task, or workspace it belongs to;
- what evidence supports it;
- how confident and salient it is, when applicable;
- where it may be routed and retained;
- which earlier events or actions caused it.

Event families use dotted namespaces such as `speech.*`, `vision.*`, `agent.*`,
`tool.*`, `memory.*`, `dfs.*`, `safety.*`, `module.*`, `eval.*`, and
`rollout.*`. Distribution adds routing metadata; it does not create a separate
remote event type.

Raw evidence remains local unless policy allows transfer. Other nodes can
receive derived observations, summaries, embeddings, commands, or evidence
handles. Blocked actions and policy incidents are events too, which keeps audit
and replay on the same path as successful work.

Finite RPC bodies drain and validate their terminal framing if an application
abandons them. Indefinitely lived event subscriptions instead opt into
cancel-on-drop: owner cancellation resets only that logical mux stream and
releases its server producer, without closing the shared carrier or leaving a
background keepalive drain.

## Agent and Memory Boundary

The agent runtime owns provider-neutral model requests, context assembly, tool
orchestration, session persistence, cancellation, steering, and compaction.
Providers translate that request into a model-specific protocol; they do not
own tool authority or durable session state.

Tools execute through runtime registries and policy boundaries. A prompt can
describe desired behavior but cannot grant filesystem, process, network,
device, secret, or rollout capabilities. Tool results retain durable evidence
even when the model-facing projection is compacted or redacted.

Memory is explicit context, not invisible mutable model state. Versioned source
files and append-only overlay files preserve evidence and scope. Derived
indexes and summaries are rebuildable. Retrieval records why each evidence
block was selected and context assembly applies a bounded budget for the
current turn.

See [agent system architecture](agent-system-architecture.md) and
[memory architecture](memory-architecture.md).

## Trust Rings and Crate Boundaries

Trust rings constrain both dependency direction and mutation risk:

| Ring | Purpose | Dependency rule |
|---|---|---|
| R0 | Minimal cluster foundation: base contracts, object format, local storage, node/events, network, KV, cluster, and service discovery. | R0 crates depend only on R0 crates. |
| R0 boundary | `sinomo-core`, the facade over the complete R0 set. | Aggregates R0 without adding service behavior. |
| R1 | Trusted runtime: daemon, DFS, build, policy, modules, registry, rollout, observation, config, evaluation, sandboxes, and support APIs. | Crosses into R0 through `sinomo-core`. |
| R1 boundary | `sinomo-runtime`, the facade over R0 and R1. | Aggregates trusted runtime surfaces without owning application behavior. |
| R2 | Agent, memory, models, perception, action, and evolution services. | Depends on the foundation through `sinomo-runtime`. |
| Application | CLI, GUI, mounts, and test applications. | May depend on R2 services directly; accesses R0 and R1 through `sinomo-runtime`. |

The package metadata in each `Cargo.toml` and the workspace ring-boundary test
are authoritative. ABI packages have narrow reviewed exceptions for the object
codec. New R1 code should use `sinomo_core::...`; R2 code uses
`sinomo_runtime::...` for the trusted foundation. Applications may import R2
services directly but must also use `sinomo_runtime::...` for R0 and R1
surfaces.

Self-evolution also uses an extended `TrustRing` risk classification through R5
for candidate artifacts, disposable workspaces, and model providers. Those
runtime risk classes are not Cargo dependency-ring metadata; R0 through R2 are
the only conceptual overlap. See
[self-evolution architecture](self-evolution-architecture.md#evolution-risk-rings).

The network/KV split is load-bearing: `sinomo-network` has no production
dependency on `sinomo-kv` or `sinomo-cluster`, and `sinomo-kv` has no production
dependency on networking or cluster policy. `sinomo-cluster` is the only R0
bridge between them. Dev-only test-support dependencies do not change that
production boundary.

## Modules and Controlled Evolution

A module is a lifecycle unit, independent of whether its backend is static
Rust, a native library, a declarative resource, a sandboxed process, or a Wasm
component. The lifecycle covers identity, compatibility, evaluation,
activation, observation, replacement, and rollback.

Runtime activation is component-based. Every implementation supplies a
`ComponentDescriptor` with versioned service offers and requirements and is
materialized through `ComponentFactory`. A mounted `ComponentScope` owns all
services, workers, readiness edges, and reverse-order compensations. The
reconciler keeps components pending until dependencies exist, preserves
unchanged scopes across atomic service-registry generations, and removes a
component's routes before unwinding its effects.

`ComponentCatalog` is the single composition input for factories, module
registrations, config definitions, and storage contributions. A host must not
maintain separate per-surface built-in lists. The shared product profile builds
the common catalog used by standalone CLI and embedded app hosts; each may add
process-specific component overlays before handing the catalog to the daemon.
The daemon supplies only host infrastructure components and adapters.

External providers are selected by interface offer, never by provider crate or
hardcoded module ID in the main runtime. WASIX implements the generic sandbox
executor interface; Cloud Files implements the generic host-filesystem provider
interface and keeps its private protocol and durable recovery state inside its
external package. Default external artifacts are enumerated only by verified
signed distribution profiles, not by scanning a source checkout.

Inter-component commands and queries resolve through the live service registry
by service interface and version. `ServiceRequirement` declares the caller
capabilities and exact operations a component may exercise; each `ServiceOffer`
maps operations to concrete routes and their authority. The component service
client intersects those declarations with live route policy before dispatch.
Providers may expose a `component`-scope route which is callable only through
this host path and is denied to every daemon signer. Local fan-out preserves
provider failures independently so aggregators can return useful partial data.
Cluster calls remain ordinary discovered object RPC and require an explicit
host transport; local resolution never silently implies remote delivery.

Inter-component facts use declared, typed event publications and subscriptions,
not middleware-style callback chains. The host overwrites publisher provenance,
uses bounded best-effort delivery with explicit lag, and ties receivers to the
owning component scope. Local transient delivery is built in. Durable append
and cluster forwarding require explicit host adapters and fail closed when no
adapter exists; the event API does not promise global order, waterfall mutation,
or bail semantics across nodes.

Observation extensions use the same service rule. Independently built modules
depend only on `sinomo-observe-contract` and expose the versioned
`sinomo.observe.metric-exporter` pull interface. `sinomo-observe` calls every
matching local provider, validates bounded batches, injects trusted component
identity labels, and returns partial-success status. External modules never link
the recorder, exporter registry, or any process-global `sinomo-observe` state.

Native-dylib route materialization belongs to `sinomo-module`. It owns request
staging, cancellation, content-addressed loading, permission callbacks, ABI
invocation, response validation, and route metadata. The daemon contributes a
narrow request-authority preparer for host-owned fields; it does not implement
the native invocation engine.

Service-catalog publication is a lifecycle-owned component which observes the
live registry generation and independently retries cluster-KV reconciliation.
Generic daemon connectivity maintenance has no service-registry field or
publication branch. Component events use host-installed durable storage and an
authenticated cluster ingress route; the original event id and publisher
metadata survive retries, and the receiving node verifies the signed origin.

Sandbox and DFS control services are component-owned. DFS bulk data, metadata
authority, and placement-sensitive operations retain their typed host and peer
transport contracts; generic component events are never substituted for these
correctness- or throughput-critical paths.

`sinomo-module::ActiveNativeComponent` owns the common native-provider binding:
it resolves exactly one active component from a `ServiceRequirement`, verifies
and loads its content-addressed artifact, and invokes typed SOBJ operations.
Domain-owned clients in `sinomo-sandbox` and `sinomo-dfs` translate their own
interfaces onto that binding. Parallel per-interface host-loader crates and
runtime-facade re-exports are forbidden.

Candidate changes move through isolated workspaces, tests and replay,
machine-readable scorecards, trusted policy, immutable artifact registration,
shadow or canary execution, and rollback. Candidate code cannot approve itself,
change trust policy, bypass the sandbox host, or replace recovery logic.

Build services turn approved source inputs into immutable, target-compatible
binary and module artifacts. They are supply services, not rollout authorities:
an artifact is preferably built once per build identity and compatible target
group, cached, and distributed to eligible nodes. Producing an artifact cannot
make it desired state or authorize activation.

Each node independently verifies compatibility, signature, capability grants,
resource limits, and local policy before activation. A disconnected node keeps
its last approved version until it can reconcile desired state safely.

See [module lifecycle architecture](module-lifecycle-architecture.md),
[self-evolution architecture](self-evolution-architecture.md), and the user
[rollout guide](user/rollout-operations.md).

## Failure and Recovery

Failure behavior follows ownership boundaries:

- A minority cluster partition cannot commit control-plane writes.
- A disconnected node continues local work allowed by cached state and policy.
- Durable events, model sessions, DFS intents, and namespace jobs can resume
  after process restart.
- Route loss triggers bounded alternate-path selection without changing node
  identity or trust.
- Replicated content repairs after reconnect according to object policy.
- Failed module or daemon rollout returns to a known-good artifact when the
  supervisor can do so safely.
- Cancellation propagates across active local and remote work by operation
  identity.

Recovery never silently creates a new cluster identity, grants membership, or
publishes divergent DFS history. Destructive recovery requires an explicit
operator or policy-authorized transition.

## Observability

Runtime behavior is observable through structured logs, metrics, traces, and
domain events. Instrumentation must not sit in the correctness path for packet
forwarding, storage commits, policy checks, or cancellation.

At minimum, operators and developers need visibility into ingestion lag,
response latency, routing attempts, quorum and replication state, DFS repair,
tool and action outcomes, policy blocks, model usage, rollout state, and
artifact provenance. Cluster builds additionally expose queue pressure, cache
resolution, worker selection evidence, resource snapshots, retries, and build
outcomes. See [observability architecture](observability-architecture.md).

## Architectural Invariants

Changes to the runtime must preserve these properties:

1. Ingestion and perception remain independent from slow model and tool work.
2. Cross-service state changes are attributable to typed events or explicit
   control-plane transactions.
3. Discovery and reachability never imply trust.
4. Remote requests are re-authorized at the node that owns the affected
   capability, device, secret, or data.
5. The cluster KV contains compact configuration and consensus fences, not
   content, DFS journals, or high-cardinality filesystem metadata.
6. Normal DFS operation has one authoritative visible history.
7. Memory retrieval is scoped, attributable, and bounded by context assembly.
8. Candidate code cannot approve, authorize, or make itself unrecoverable.
9. Producing a binary or module artifact never grants authority to activate,
   promote, or roll it out.
10. Lower trust rings never depend on higher-ring services.
11. External backends remain replaceable and cannot become undeclared sources
    of authority.
12. Rollout, cluster membership, and service discovery never imply node-local
    consent to remote host or device access.

Detailed contracts belong in the subsystem documents linked from the
[documentation index](README.md). Delivery sequencing and unfinished work
belong under [plans/](plans/README.md), not in this overview.
