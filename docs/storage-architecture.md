# Sinomo Storage Architecture

This document defines how Sinomo records ownership and format expectations for
files in the local data directory. It describes the storage contract, not a
catalogue of every current path.

## Purpose

Every durable local file must have an owning subsystem. The storage registry
makes that ownership inspectable so diagnostics and agents can answer:

- which subsystem owns a path;
- which trust ring may read or write it;
- whether the path is an exact contract or an intentionally open subtree;
- which encoding and object schema apply;
- whether an unrecognized file is expected or requires investigation.

`sinomo-storage` owns this vocabulary and the registry mechanics. It does not
own the complete data-directory layout.

## Ownership

The crate that creates or interprets durable state registers its own paths.
Lower-ring layout is composed first, then trusted runtime and application
layers add their contracts through the appropriate facade. This preserves the
trust-ring dependency direction while producing one inspectable layout.
Product components register their paths through their component factories; a
component that is disabled in a host profile still contributes its paths
through a storage-only contributor, so its retained state stays covered.

Registration stays next to the owning subsystem. A central crate must not need
to understand every daemon, DFS, agent, provider, or evolution file in order to
validate the layout.

An exact file contract records a relative path, owner, encoding, and schema when
applicable. An escape path declares an open subtree whose children cannot be
enumerated in advance, such as a user workspace or content-addressed store.
Every escape requires a reason and an owner; it is not a blanket exemption from
inspection.

## Filesystem Access Policy

DFS backend filesystem access is deployment policy. Ordinary DFS operations
preserve existing directory permissions and ACLs, including group sharing and
setgid directories. New backend directories and staging files use the process
umask and inherited filesystem ACLs; opening recovery state does not enforce
owner-only modes. Operators manage access to the data directory through their
deployment. This is separate from logical DFS file permissions and from
explicitly private storage for credentials. Exclusive creation, recovery locks,
symlink validation, and durable synchronization still apply.

## Format Policy

Internal runtime state is binary-first. SOBJ documents and packed binary logs
provide schema-aware, bounded formats for state that people are not expected to
edit directly.

The configuration system is SOBJ-native across the module ABI, object RPC, and
cluster KV. JSON configuration files are a human-friendly local storage and
interface projection of that contract; they do not define the distributed
schema or become a second source of authority. Modules publish detailed field
schemas for editable settings, while raw JSON editing remains a compatibility
fallback for schema-less legacy definitions and explicitly dynamic JSON
fields. Read-only operational configuration documents may use the same schema
and projection without granting write authority.

Human-readable formats are appropriate for:

- user-managed configuration;
- CLI or service output;
- logs, reports, and benchmark artifacts intended for review;
- external tools that require a specific text format;
- workspace content owned by users or agents.

The choice between binary and text follows the file's audience and contract,
not convenience in one call site. Secrets remain node-local and private even
when their transport or provider requires JSON.

## Storage Migrations

`sinomo-storage::migration` provides durable job progress and a reusable bounded
copy, verification, and publication state machine. The owning module supplies
the transformation, supported old readers, current writer, concurrency checks,
and atomic publication. Jobs and staging output live under that module's
registered storage paths; the shared library does not become their data owner.

Node-local jobs use checksummed atomic SOBJ checkpoints and process-shared file
locks. Replicated owners can supply a `MigrationCheckpoint` backed by their
existing authoritative transactions. Output must be durable before progress,
and each batch must be idempotent across the output/checkpoint crash window.
Source generation and algorithm identities fence retries. Cancellation leaves
durable progress available for the next executor.

DFS uses the job store for bounded compaction of its inline content-binding
logs and the checksummed stream codec for current metadata snapshots.
Conversion of retained older DFS formats belongs to `sinomo-dfs-migration`,
which DFS calls as a bounded storage-preparation provider at cold open; see
[DFS metadata authority](dfs-architecture.md#metadata-authority). Other modules
reuse the mechanism through the trust-ring facades and their own lifecycle
hooks. See the [migration integration guide](developer/storage-migrations.md)
for the contracts and interruption tests.

## Layout Categories

The local data directory contains several kinds of state with different
ownership rules:

| Category | Contract |
|---|---|
| Runtime identity and daemon state | Exact files or tightly owned daemon subtrees. |
| Human-managed configuration | Explicit configuration documents under the shared config hierarchy. |
| Credentials and tokens | Node-local private files owned by the provider or authentication subsystem. |
| DFS metadata and content | Owned dynamic subtrees registered by DFS. |
| Workspaces and exposed files | Reasoned escape paths because contents are intentionally arbitrary. |
| Evolution jobs and review artifacts | Higher-ring subtrees intended for operator and agent inspection. |
| Build inputs, managed source, workspaces, caches, and job evidence | Build-service-owned paths with isolated workspace and immutable cache rules. |
| Installed or staged binaries | Install and rollout-owned paths with explicit activation lifecycle rules. |

Cluster-wide configuration belongs in cluster KV or a shared configuration
document according to the owning subsystem. A local file must not quietly become
an alternate source of authority for distributed settings.

## Build, Artifact, and Activation Storage

Build storage and activation storage have different owners. The build service
owns the reviewed managed checkout, immutable source bundles, isolated per-job
workspaces, fetched dependency material, compiler caches, build logs, and
locally retained immutable outputs. Rollout and the module loader own staged
candidates, installed binaries, active module references, and recovery state. A
build workspace or cache path is never an activation path.

Immutable source bundles, binary and module artifacts, and manifests are
content addressed when cached. The initial build service publishes them to a
verified node-local cache and streams requested artifacts to rollout. A shared
registry or DFS content source may later serve the same immutable identity; it
must not rewrite bytes under an existing identity. Rollout, not the builder,
copies a selected artifact into target staging and installed locations.

The initial queue, job status, and artifact cache are node-local under
`$SINOMO_DATA_DIR/build/`; live RPCs provide scheduling evidence. When
cluster-wide advertisements, leases, or artifact references are added, cluster
KV stores only that compact coordination. It never stores source trees,
dependency archives, artifact bytes, full build logs, or compiler caches.
Temporary workspaces are recoverable or safely disposable after a worker
failure; durable job state and published artifacts remain independently
inspectable.

Windows Cloud Files state is split deliberately between the generic SIFS mount
supervisor and the native provider. The supervisor owns mount consent,
provider-neutral status, and the process-wide lease (`daemon/sifs-mount.lock`,
registered by `sinomo-daemon`). The provider owns its registration journal and
receipt, local mutation queue and immutable blobs, namespace manifest and
cursors, and recovery authority under `module/runtime/module.dfs.cloud-files/`,
inside the `module/runtime` escape path registered by `sinomo-module`. None of
this is user-editable configuration. The ownership and lifecycle rules are
defined in the [Windows Cloud Files architecture](cloud-files-architecture.md);
unknown or apparently stale files are evidence to inspect, never cleanup
candidates for a generic storage walker.

## Verification

Verification can check an explicit set of paths or walk a data directory. A
path is covered only when it matches an exact file contract or falls under a
registered escape path. The report separates covered and unregistered files so
operators can investigate drift without losing access to an otherwise usable
node.

Install and cluster diagnostics use the composed runtime layout. Unregistered
paths are health warnings rather than automatic deletion targets or startup
blockers. The owning subsystem decides whether a legacy path may be migrated;
the registry only reports coverage and schema expectations.

Storage verification does not validate domain semantics. A covered Raft log,
DFS manifest, or provider credential can still be corrupt or unauthorized; the
owning subsystem performs those checks.

## Invariants

1. Each durable path has one clear owner.
2. Higher-ring registration does not create lower-ring dependencies.
3. Escape paths are narrow, documented, and owned.
4. Internal binary documents declare their schema or encoding contract.
5. Human-editable configuration is distinguishable from generated runtime
   state.
6. Unregistered files are reported, never silently adopted or deleted.
7. Local storage cannot override cluster authority without an explicit
   subsystem rule.
8. Build workspaces and caches cannot serve as installed or active locations.
9. Immutable build inputs and artifacts are never overwritten under an existing
   content identity.
10. Cluster coordination stores references to build content, not the content
    itself.

The [architecture overview](architecture.md#state-and-data) explains how local
storage relates to cluster KV, events, memory, and DFS. The
[SIFS document](sifs.md) describes the cluster filesystem rather than the local
data-directory registry.
