# Windows Cloud Files Architecture

The Windows Cloud Files module projects a SIFS subtree into an NTFS directory
through the Windows Cloud Filter API (CFAPI). It is a trusted R1 native dynamic
module, not a second filesystem implementation: DFS remains authoritative for
namespace, object versions, policy, and remote mutation outcomes. NTFS owns the
local projected tree and cache, while the provider hydrates immutable object
versions and reconciles local and remote changes.

This document defines the current production boundary. The
[DFS architecture](dfs-architecture.md), [SIFS contract](sifs.md), and
[module lifecycle](module-lifecycle-architecture.md) remain authoritative for
their respective layers. Source-level ownership and test commands are recorded
in the [module maintainer guide](../modules/dfs/cloud-files/README.md).

## Scope

Cloud Files currently provides:

- a supervised Windows sync root for one configured SIFS subtree;
- paged file and directory placeholder population;
- exact-version range hydration on demand;
- durable local file and namespace writeback to DFS;
- durable application of committed remote namespace changes to NTFS;
- reactive per-item writeback and conflict status in File Explorer;
- restart-safe CFAPI registration and state-preserving disable/re-enable;
- fail-closed provider selection and native registration-ownership checks
  across module-loader rollout;
- live status and bounded attention summaries through the SIFS mount
  supervisor.

It does not replace DFS metadata, cache a second authoritative namespace in
cluster KV, or grant an arbitrary DLL permission to take over an existing sync
root. Files and directories are projected. Mount objects project as
directories; symlinks, hardlinks, artifacts, and tombstones remain represented
in reconciliation state but are not synthesized as file-like placeholders.

## Layer Ownership

```text
Generic SIFS mount supervisor (node-local consent and process lease)
        |
        | typed SOBJ invocation and lifecycle events
        v
Explorer and applications <-> Windows Cloud Files native module
                             (R1, in the daemon process)
                                      |
                                      | DFS reads and mutations
                                      v
                     Mounted SIFS service and routed DFS object RPC
                                      |
                                      v
                     DFS metadata authorities and content stores
```

| Component | Responsibility |
|---|---|
| Cloud Files private protocol | Versioned provider implementation records live in `modules/dfs/cloud-files/src/protocol.rs` and are not part of the runtime facade. |
| Generic host interface | `sinomo.host-filesystem-provider/v1` is the only contract known by the main system. |
| SIFS mount supervisor | Reads node-local mount consent, owns the process-wide mount lease, and calls the DFS-owned typed client. Provider selection and content-addressed loading use `sinomo-module`'s generic active-component binding. It does not decode Cloud Files state. |
| Native module | Registers and connects CFAPI, validates physical identity, serves callbacks, and owns registration, unregister, compensation, and recovery state. |
| Writable worker | Converts NTFS notifications and DFS namespace events into bounded, restart-safe work. |
| Mounted SIFS service | Resolves the configured SIFS root and performs canonical DFS reads and mutations through the normal routed service path. |
| Windows/NTFS | Owns ordinary local directory behavior, placeholder state, cached hydrated ranges, and change notifications. |

The native module executes in the daemon process and is trusted native code.
The module ABI isolates build and rollout, not memory or security. The host
loads only an immutable, content-addressed component offering
`sinomo.host-filesystem-provider/v1`. Cloud Files identity and private protocol
versions are interpreted only inside the external module.

## Read and Population Paths

### Placeholder population

Windows issues `FETCH_PLACEHOLDERS` when an application enumerates a projected
directory. The provider obtains a bounded DFS directory page, creates or
updates only the required placeholders, and returns the page result to CFAPI.
Materialized-directory audit intent is durable so an empty page and an
already-matching page are still recoverable facts rather than invisible work.

Each file placeholder carries a bounded, versioned identity containing the
mount, mounted SIFS root, absolute SIFS path, DFS object ID, object version,
and optional content hash. This identity fences a later hydration request to
the version that created the placeholder.

### Data hydration

Windows issues `FETCH_DATA` for a byte range that is not resident locally. The
provider decodes the placeholder identity, resolves that exact DFS object
version, verifies identity and content evidence, and transfers only the
requested range to CFAPI. A path that now names another object or version is a
stale identity error; the provider does not silently hydrate whichever object
currently occupies the path.

CFAPI cancellation is scoped to the request. Disconnecting the provider and
unregistering the sync root are separate lifecycle operations.

## Local Changes to DFS

Windows mounts must be writable and require `--sync-local-changes`; read-only
and unsynchronized writable modes fail closed. NTFS records the user's write
first. `ReadDirectoryChangesW` then wakes the worker; it is an accelerator, not
the sole source of truth. Startup and notification-overflow recovery use
bounded scans to reconstruct missed changes.

The writeback order is:

1. Read a stable local image and capture exact DFS source/destination
   preconditions.
2. Store immutable content in the module-owned blob staging area.
3. Append a caller-stable operation and its evidence to the durable local
   queue before sending it.
4. Apply the canonical DFS mutation through the mounted SIFS service.
5. Reconcile an ambiguous transport outcome before any retry.
6. Persist `RemoteApplied` before performing the idempotent local placeholder
   completion.
7. Mark the operation complete only after that local effect succeeds.

A changed DFS precondition becomes retained conflict evidence; it is not
resolved by overwriting either side. The status surface reports bounded
attention records and aggregate overflow counts.

The queue protects work once capture begins. It does not make an application
write a remote DFS commit at the instant the NTFS write returns. Status and
conflict handling must therefore distinguish local persistence from confirmed
remote publication.

## File Explorer Status

Windows remains the owner of Explorer-visible state. CFAPI supplies the normal
availability, hydration, pinning, and in-sync badges. After the registration
reaches its durable `Ready` boundary, the provider also registers a bounded
custom-property vocabulary and pushes exceptional or transitional state with
`StorageProviderItemProperties.SetAsync`:

| Durable provider state | Explorer status |
|---|---|
| queued local operation | Waiting to upload |
| canonical DFS mutation in flight or remotely applied but not locally completed | Uploading |
| ambiguous or retryable outcome | Retrying |
| retained local/remote divergence | Conflict |
| terminal operation or worker failure | Sync failed |
| completed or no outstanding evidence | no custom property; use the standard CFAPI in-sync status |

Deletes project their transient state onto the containing directory because
the deleted item no longer exists. Renames project onto the destination. If
several retained facts affect one path, the most severe state wins.

This is a push-only projection over in-memory representations of already
durable queue and reconciliation state. Explorer never loads a Sinomo COM
server and cannot call DFS, networking, or the queue while rendering a folder.
The projector compares the desired state with its last attempted state, so an
unchanged worker pass makes no shell call and settled idle makes no status
write. Shell-property registration or publication failure is non-authoritative:
the mount continues with standard CFAPI badges and logs the ancillary failure
without starting a retry loop.

## Remote Changes to NTFS

The module subscribes to the daemon's DFS namespace event stream. A wake reads
bounded, per-shard feed batches after durable cursors. For each effect, the
reconciler:

1. persists the effect intent;
2. applies the idempotent placeholder effect;
3. persists the manifest and no-echo state; and
4. advances the shard cursor only after every effect in the batch is durable.

Dirty or locally divergent paths become conflicts instead of being overwritten
by a remote update. Gaps, startup, notification overflow, and explicitly
authorized recovery use bounded authoritative scans. The durable manifest is a
reconciliation index, not a competing DFS namespace authority.

The clustered provider shares its node identity with the resident daemon, but
does not own that daemon's primary namespace. Namespace requests and exact-path
lookups therefore use the daemon even when the metadata primary is this same
node. A private metadata image must not turn a feed poll or stat into a stale
local-primary read. Standalone operation and response delivery retain loopback.

A namespace notification owns all returned shard batches and any remaining feed
pages. The worker retains unconsumed batches across bounded passes and parks only
after that notification's backlog is settled. A full scan discards cached feed
inputs and re-fetches from its new durable cursor boundary.
The root subscription accepts coalesced namespace invalidations with no exact
path list; these notifications wake feed replay without granting mutation
authority.

A path-local conflict does not advance that path's manifest identity. This
retains the last successfully projected object/version as bounded ownership
evidence while unrelated scan entries and cursors continue. Older conflicts
which were persisted after prematurely removing that entry may recover it from
the checksummed live manifest journal; that replay history is usable only while
the matching durable conflict remains present.

Absence after downtime is never deletion evidence. Live `ReadDirectoryChangesW`
removals are the only local delete authority; identity-backed startup state can
still recover creates, replacements, and renames.

Older evidence is retired only by explicitly versioned, one-shot cleanup fences
that each complete against an authoritative scan before the fence is durable:

- the conflict-history fences (v1, with v2 adding the
  `/services/real-cluster/` verification subtrees) collapse duplicate terminal
  conflicts, retaining the newest non-test diagnostic, and retire terminal
  deletes beneath narrowly recognized test paths without replaying them. A
  historical test upsert conflict is acknowledged only when the current scan
  still contains the path and the Windows adapter proves the ordinary local
  entry against the conflict's exact type, size, and complete content hash
  while excluding writers.
- the startup-absence fence (v4, which replaced an incomplete v3) retires
  deterministic `startup-delete` conflicts and matching legacy
  absence/raced-generation diagnostics across every bounded scan page of the
  pre-fence cohort. The same scan reclassifies any current ordinary or dirty
  local entry with its actual evidence.

Outside those recognized verification subtrees, cleanup never touches
`sinomo-agent`, sandbox-instance, service, node, or user paths. Any failed
proof, pending operation, or conflict created after a durable fence remains
visible and actionable.

Authoritative scan deltas also recheck terminal delete conflicts without using
the historical classifier. They retire one only when an explicit removal delta
proves the path absent or Windows proves an exact in-sync placeholder for the
current authoritative identity. Failed, dirty, ordinary-file, or incomplete
proofs retain the original conflict and do not block unrelated paths.

## Reactive-at-Rest Contract

Once startup, earlier retries, and crash recovery are settled, the Cloud Files
layer has no periodic scan, heartbeat, cancellation poll, or mount-config poll.
Its worker waits on:

- CFAPI work caused by access beneath the sync root;
- `ReadDirectoryChangesW` notification beneath the sync root;
- a committed DFS namespace event beneath the mounted SIFS root;
- an explicit configuration, lifecycle, recovery, or shutdown signal; or
- the deadline of a retry created by earlier work.

With none of those causes present, the Cloud Files layer owns no scheduled disk
I/O. Startup reconciliation and finite catch-up may perform work without a new
Explorer access because they complete an earlier durable obligation. Other
daemon services, such as cluster membership or Raft, have separate lifecycle
contracts and are not part of this Cloud Files idle guarantee.

One wake performs bounded work and yields. A notification storm cannot turn a
single worker pass into an unbounded foreground scan.

## Registration and Durable Ownership

The native registration journal records `Prepared`, `Registered`,
`StateOwned`, and `Ready` phases together with the exact sync-root file ID,
registration nonce, provider identity, mounted SIFS root, and interface. A
matching readiness receipt proves the `Ready` cut. Recovery classifies exact
current and deterministic temporary successors; unknown, reordered, or
noncanonical evidence fails closed.

The generic host owns only the process-wide SIFS mount lease
(`daemon/sifs-mount.lock`), node-local mount consent in `storage.sifs-mount`,
and provider-neutral status in `storage.sifs-mount-status`. It keeps no staged
registration, unregister intent, provider pin, or recovery marker. These are
separate authorities on purpose: the host reasons only about consent and
provider selection, while the native module alone interprets its registration,
queue, namespace, cursor-recovery, and CFAPI evidence.

The host does not pin a provider. Each activation resolves the single active
component offering `sinomo.host-filesystem-provider/v1`; zero or several
candidates fail closed, and Windows keeps a loaded provider image mapped until
process exit. A provider adopts an existing registration only after its
journal and receipt match the observed provider name, sync-root file ID, and
nonce-bound root identity; a tampered or mismatched journal fails closed.
Without a valid journal, activation requires an empty sync root and absent
writable state, so retained state is never reinterpreted as a fresh mount.

Provider-owned registration, queue, manifest, compensation, and recovery state
live under `$SINOMO_DATA_DIR/module/runtime/module.dfs.cloud-files/`, inside
the `module/runtime` subtree that `sinomo-module` registers as an owned escape
path. Exact filenames and schemas stay beside their owner. Neither location is
a user-editable configuration surface.

Routine daemon shutdown, rollout replacement, transient worker failure, and a
same-root provider restart disconnect the active CFAPI callback connection but
leave the sync root registered. `CfUnregisterSyncRoot` is not a stronger form
of disconnect: Windows converts fully hydrated placeholders into ordinary
files while traversing an unregistered tree. The supervisor therefore invokes
state-preserving unregister only for an explicit disable or configured
mount/root identity change that intentionally relinquishes that registration.
Re-enable may recover those ordinary files only when their type and complete
content still match the provider's prior durable manifest; retained local
operations and mismatches remain conflicts.

Mount-open transfers recovered DFS repairs in pages bounded by the existing
mounted-mutation handoff limit. Its original Armed marker remains durable until
every repair page has been persisted and acknowledged by the daemon. Only then
does startup complete that marker and expose callbacks. A crash or lost page
reply retains full recovery ownership, so a large repair backlog neither drops
repairs nor turns into repeated failed mount activation and metadata scans.

Activation compensation follows the same ownership rule. If an activation
created a fresh registration and then failed before `Ready`, it removes that
registration. If the registration predated the attempt, a transient DFS,
network, or worker startup failure verifies and retains it in `StateOwned` for
retry; it must not call unregister as rollback.

## Disable, Re-enable, and Cursor Recovery

Disabling a healthy mount, or changing its mount point or SIFS root, is state
preserving. The supervisor signals cancellation, joins the active provider
invocation, and then asks the active provider for a state-preserving stop. The
native module unregisters the sync root under its registration lease and
retains exact durable state ownership; it releases ownership only when
writable state was never established. During a cursor-recovery cycle it
records `PreserveAuthorized` before CFAPI unregister effects and
`PreservedStateOwned` only after the new-cycle state-owned journal is durable;
re-enable re-arms that marker before CFAPI registration or writable worker
effects.

Cursor recovery exists for one narrow legacy state: valid provider-owned
namespace cursors with no remaining writable queue or other mutable evidence.
Inspection is read-only. Adoption requires the exact returned token and an
explicit acknowledgement that authoritative full reconciliation may rebuild
the namespace state. It is not a generic repair command.

The native module retains these inspect/apply operations and the commit
protocol below, but the generic host supplies no cursor-recovery context. The
supervisor rejects `sinomo dfs mount --recover-cursor-only-state` because
provider recovery belongs to the external component, and an activation that
finds a retained native marker without matching context fails closed.

Recovery completion is a provider-owned commit protocol:

| Durable cut | Meaning |
|---|---|
| Native `Ready` | Reconciliation is terminal for the authorized cycle. |
| Native `NamespaceFinalized` | The exact namespace-finalization proof is bound while ordinary writeback remains parked. |
| Native `CompletionAuthorized` | Completion is irreversible; a concurrent disable must finish forward rather than return to cursor-preserving unregister. |
| Ordinary `Ready` registration journal | Cursor-recovery provenance has been cleared from the registration journal; the receipt is unchanged. |
| Native marker absent | Native completion authority was cleared only after the normalized journal was durable. The ordinary journal is now restart authority. |

Every cross-file transition uses a deterministic successor and validates the
predecessor/successor pair before effects. A crash may leave either the current
file or its exact temporary successor; replay promotes only a causally valid
cut. Operators must not delete these files to force progress.

If disabled consent is observed after `CompletionAuthorized`, the provider
finishes normalization and latches cancellation. The provider checks cancellation
before completing and releasing parked population/writeback. A later ordinary
disable uses `PreserveState` with no stale cursor context.

## Rollout and Compatibility

The generic host interface and native provider are one compatibility set. The
supervisor always invokes the currently active provider component; there is no
host-side pin to the artifact that created a registration. A successor
provider adopts durable state only through the native journal, receipt, and
versioned state readers, which fail closed on mismatch. The generic supervisor
does not unregister owned state because loader selection changed; a routine
restart only disconnects.

Older interface and serialized-state readers exist only where the owning code
defines an explicit migration. They are not permission for an older provider to
decode a v3 preserve or recovery request. Changes to operation names, typed
payloads, durable phases, or file topology require compatibility tests and a
reviewed migration or fail-closed rejection.

## Operations

Windows mounts are supervised by the main daemon; foreground Cloud Files mounts
are intentionally unsupported. The native module declares the editable
`storage.sifs-mount` setting and read-only `storage.sifs-mount-status` setting
through the common module settings surface. In the graphical app, open
**Settings > Storage**: its **Mount settings** card sets enablement, the
**Sync root**, the SIFS root to project, and **Durable writable cloud sync**.
The **Modules** page lists Cloud Files as managed in Storage rather than
editing it there. The Storage page writes the same node-local configuration
consumed by the mount supervisor; it is not a parallel Cloud Files
configuration store.

The current supervisor owns one mount mapping per node. A mapping associates
one SIFS subtree with one absolute host filesystem location. The equivalent
command for a writable mapping is:

```powershell
sinomo dfs mount 'D:\sinomo' --root / --background --sync-local-changes
```

Inspect the live supervisor rather than inferring state from the config file:

```powershell
sinomo dfs mount --status --json
```

The status includes the platform and phase, provider registration and
connection state, worker readiness, pending/retrying/failed operation counts,
conflict and attention summaries, provider provenance, and restart count.
While cooperative reconciliation remains pending, its detail also identifies
the authoritative namespace scan or local startup audit. Worker readiness
proves activation contracts and the first durable reconciliation quantum;
it does not imply that the remaining audit has finished.
**Settings > Storage** also shows the supervisor's reported state in its
**SIFS mount** card.

Disable through the supervisor so registration ownership is preserved:

```powershell
sinomo dfs mount --disable --json
```

Do not manually unregister the sync root, replace the projected directory, or
delete module runtime files while the provider owns the mount. Provider-specific
recovery is intentionally absent from the generic mount API. A Cloud Files
recovery tool must be versioned and shipped with the provider that owns the
private evidence.

## Security and Failure Rules

- Mount configuration is node-local consent. Cluster membership or a remote
  configuration fact cannot enable host filesystem exposure.
- Existing registration and recovery paths acquire the existing lease and
  validate physical sync-root file identity; they do not recreate missing
  authority as if it were a fresh mount.
- Placeholder hydration is exact-version, not path-latest.
- Ambiguous DFS mutations reconcile by stable operation identity before retry.
- A provider cannot adopt or replace an existing registration without
  validating its native journal against the observed sync root.
- Unknown files, extra deterministic temporaries, changed fingerprints, and
  impossible phase orderings fail closed without cleanup effects.
- A conflict or failed cleanup retains evidence for review. It is not converted
  into a best-effort success.
- Explorer decoration is a one-way, equality-cached projection from durable
  provider state into Windows-owned metadata. It must never become an
  authority or an Explorer-to-provider query path.

## Verification Boundary

Pure queue, reconciliation, registration, and protocol cuts are exercised by
Rust tests. Windows GNU cross-compilation proves the module, generic host, and
CLI test targets continue to build:

```sh
cargo test -p sinomo-dfs-cloud-files
cargo test -p sinomo-dfs-cloud-files -p sinomo-cli \
  --target x86_64-pc-windows-gnu --no-run
```

That is not CFAPI runtime evidence. Changes to registration, callbacks,
notifications, placeholder effects, Explorer behavior, idle I/O, or
cancellation require focused execution on Windows. A release acceptance should
include mount, hydration, local writeback, remote change, disable/re-enable,
restart/crash-cut, conflict, and settled-idle observations with the exact build
and data directory recorded.
