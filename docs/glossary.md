# Sinomo Glossary

This glossary defines terms used across the user, developer, and research
documentation. Specialist architecture documents may refine a definition for
their subsystem.

## A

**Agent**
A model-backed runtime participant with a package, instructions, tools, skills,
context policy, and durable session state.

**Artifact**
Immutable, identified output such as a binary or module library. An artifact's
existence does not imply that it is approved or active.

**Authoritative read**
A read performed with current quorum/leadership evidence. Failure does not
silently fall back to a possibly stale local value.

**Authority member**
A cluster member that participates in replicated-state consensus. This is
separate from the descriptive node role and from device or host permissions.

## B

**Build identity**
The inputs and target contract that determine whether an existing build
artifact can be reused for a request.

## C

**Chat or channel**
A user-visible conversation or collaboration timeline. It is distinct from a
model session.

**Cluster**
A trust domain containing one or more authorized Sinomo nodes and a replicated
control plane. One machine can be a complete one-node cluster.

**Cluster KV**
The small Raft-backed key-value control plane used for ordered distributed
facts. It is not a general content or filesystem-metadata database.

**Committed read**
A local view that has applied a known committed log prefix. It can be useful
while disconnected but may be older than the current cluster authority.

**Component**
The runtime activation unit behind a module. A component declares versioned
service offers and requirements; its mounted scope owns the services, workers,
and compensations that are removed together when it is unloaded.

**Content store**
A node-local store for immutable file bytes and manifests used by DFS.

## D

**Daemon**
The long-running process that hosts cluster participation and runtime services.
The main daemon owns normal work; the shadow daemon supervises recovery.

**Data directory**
The persistent local root selected by `SINOMO_DATA_DIR`, defaulting to
`~/.sinomo`. It contains identity, configuration, credentials, runtime state,
content, logs, managed source, and recovery evidence owned by different
subsystems.

**Desired state**
An authorized statement of what version or configuration should be active.
Artifacts, discovery results, and build output are not desired state by
themselves.

**Device exposure**
A node-local permission allowing a named resource and operation. Exposure is
fail-closed and cannot be enabled remotely.

**DFS**
The distributed filesystem layer that owns shared namespace metadata, versions,
placement, replication, receipts, intents, and repair.

## E

**Endpoint**
A concrete address and transport hint for reaching a daemon or service.
Endpoints are observations; identity checks establish which node answered.

**Event**
A typed, timestamped statement about something that happened, with provenance,
causality, evidence, scope, and routing metadata as applicable.

**Extension point**
A typed, versioned seam declared by the subsystem that owns a surface. Modules
contribute bounded data or logical operations to it; the owner composes them
deterministically and keeps final authority.

**Evidence**
Durable material that supports an event, model response, evaluation, policy
decision, or operation. Evidence may be retained separately from its bounded
model-facing projection.

## I

**Installation**
Sinomo software operating with one persistent data directory on a machine. It
can run in the foreground or through managed boot integration and contributes
one normal cluster node.

**Installed deployment**
The binaries, managed source, native modules, boot entries, and daemon lifecycle
maintained by the shared installer and rollout for an installation. Initial
installation registers and starts the main and shadow daemons by default;
legacy operator deployments can use `sinomo install --runtime-service`.

## M

**Main daemon**
The daemon that votes when configured as an authority member and hosts normal
cluster, DFS, service, and workload responsibilities.

**Memory root**
A registered local directory of Markdown or text sources that indexed-file
memory can scan into immutable index generations.

**Metadata authority**
The DFS owner allowed to serialize and publish visible namespace or object
history for a shard and epoch.

**Model provider**
An adapter to a local or external language/realtime model system. Providers do
not own Sinomo's tool authority or durable sessions.

**Model session**
Internal state for turns, context, tool calls, cancellation, steering,
compaction, and resume. It is not the user-visible conversation source of
truth.

**Module**
A versioned behavior lifecycle unit. A module can use several implementation
backends while sharing installation, activation, observation, and rollback
semantics.

## N

**Node**
One runtime location with a signed identity, services, storage, models, devices,
and capabilities. A node's display name or role does not grant authority.

## O

**Object RPC**
Sinomo's typed local and routed command mechanism, supporting value and
streaming bodies. Domain crates own request meaning; the network layer owns
transport and routing.

## P

**Placement**
The decision about where work, metadata, or content should live based on
authority, locality, health, privacy, resources, and policy.

**Policy**
Runtime-enforced rules that admit, constrain, or reject actions. Prompt text and
remote requests cannot override policy.

## R

**Replica**
A member or storage location retaining committed state or content without
necessarily owning current publication authority.

**Rollout**
The controlled process that selects targets, stages an artifact, activates it,
observes health, promotes it, or restores a previous version.

**Route**
A direct or relayed network path selected for a request. A route proves a
candidate delivery path, not trust or authority.

## S

**Service**
A discoverable typed capability hosted by a node or process.

**Shadow daemon**
A local recovery and supervision process. It does not represent another node,
vote, own normal DFS data, or accept ordinary workload placement.

**SIFS**
Sinomo's default shared filesystem, implemented through DFS with per-object or
per-subtree placement policy.

**Skill**
A portable instruction and resource package that can be imported, versioned,
activated by scope, and included in an agent's behavior.

**SOBJ**
Sinomo Object, the schema-aware binary representation used for wire objects and
internal structured state. See the [format specification](sinomo-object-format.md).

## T

**Trust ring**
A workspace dependency and mutation-risk boundary. R0 is the minimal cluster
foundation, R1 is the trusted runtime, R2 contains higher-level agent services,
and applications sit outside those library rings.

## W

**Workspace**
The explicit files and tools available to an agent or developer task. In the
runtime, workspaces should be VFS/SIFS-backed and remain separate from internal
session state.
