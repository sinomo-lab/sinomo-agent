# Node Health, Device Monitoring, and DFS Storage Locations

Status: partially implemented. DFS storage-health sampling, the aggregate node
advertisement, and host resource evidence have landed; general node health,
additional content locations, storage management, and the app surfaces remain
proposed. Last reviewed 2026-10-13.

This plan adds cached node and device health observation plus safe management of
additional node-local DFS content locations. It extends the current
[observability architecture](../observability-architecture.md),
[DFS architecture](../dfs-architecture.md), and
[storage ownership model](../storage-architecture.md); it does not replace
their current contracts.

## Goal and Completion Criteria

The work is complete when:

- the app can distinguish cluster liveness, health freshness, degraded
  resources, unavailable devices, and missing authorization without conflating
  them;
- every daemon maintains a bounded, cached health snapshot without sampling
  hardware or scanning DFS storage in request handlers;
- authorized operators can inspect, preview, add, drain, and retire additional
  DFS content locations on local or remote nodes;
- a remote mutation requires both voter/admin authority and the target node's
  locally enabled `remote-management` permission;
- every configured content location is bound to a stable volume identity and a
  Sinomo ownership marker before it stores data;
- DFS reads, writes, restart recovery, accounting, scrub, and reconciliation
  work across multiple locations on one node;
- multiple locations on one node never count as multiple replicas or failure
  zones;
- draining or losing a location cannot delete the last durable copy of an
  object; and
- rolling upgrades and downgrades cannot advertise content as healthy when an
  older daemon cannot open every location that holds it.

The first release targets fixed local filesystems. Network, removable, and
operator-supplied encrypted filesystems remain explicit follow-up work.

## Current Baseline

The repository already contains most of the control-plane building blocks, but
not the multi-location content-store abstraction.

- `sinomo-cluster` publishes node liveness, process identity, version, and
  endpoint observations. These records are not a resource-telemetry stream.
- `sinomo-device` models profiles, capabilities, inventory, and coarse
  availability. It does not model sampled device health, freshness, severity,
  or recovery state. Its process-local `NodeResourceMonitor` carries only
  advisory resource mode and network metering supplied by the host.
- the native remote-management module can observe CPU, load, memory, disks,
  network interfaces, and processes. Its collector is request-scoped and must
  not become the app's polling implementation.
- `sinomo-observe` already provides the cluster observation plane and cached
  exporter boundary. It is the appropriate place for sanitized health
  snapshots and metrics.
- node-local remote-management permission defaults to deny and cannot be
  granted remotely. The receiving daemon rechecks it before host effects.
- DFS constructs one concrete `LocalContentStore`, normally at
  `$SINOMO_DATA_DIR/dfs/content`.
- DFS advertisements and placement receipts are keyed by `NodeId`; placement
  policy selects nodes using replica counts, reachability, zones, and the
  cached storage, path, and resource evidence described below.
- the placement preview, reconciliation jobs, retry state, and activity history
  provide the right foundation for inter-node repair and for presenting
  node-local relocation activity.

## Implementation Status

Landed with network-aware DFS placement and mobile resource evidence
(2026-08):

- `sinomo-api` defines the DFS observation vocabulary: `DfsHealthState`
  (`Healthy`, `Degraded`, `Critical`, `Unknown`) and a separate
  `DfsObservationFreshness` (`Fresh`, `Stale`, `Offline`, `Unauthorized`,
  `Unsupported`, `NeverSampled`).
- `sinomo-dfs` defines `DfsContentLocationConfig` (string `location_id`,
  label, root, `Active`/`Draining`/`Detached` lifecycle, reserved free bytes
  defaulting to 5 GiB, optional DFS byte cap, allocation weight, and
  configuration generation), per-location observations, and
  `DfsNodeHealthSnapshot`.
- Each DFS daemon host owns one `DfsDeviceHealthMonitor` per data directory and
  serves immutable cached snapshots. It samples capacity on a five-minute
  safety interval or an explicit storage-environment dirty signal, never in a
  request handler, and applies reserve-recovery hysteresis.
- The monitor derives one aggregate `DfsNodeAdvertisement` per node: content
  admission, a quantized capacity weight, storage health and freshness, usable
  capacity, reserve, location count, network metering, and resource pressure.
  Admission stops immediately on an unsafe transition; recovery and weight
  changes need two consecutive matching samples.
- Placement and source ranking use a cached `DfsPlacementSnapshot`. Fresh
  evidence of a critical or non-admitting node, or of an unreachable path,
  makes a candidate ineligible; candidates then rank by health, path safety,
  resource pressure, and path performance. Placement previews show this
  evidence per node.
- The Android mobile runtime publishes a `NodeResourceSnapshot` (resource mode
  and network metering derived from battery, power-saver, thermal, and network
  state); Linux and Windows DFS hosts sample OS network metering. DFS uses this
  advisory evidence to rank placement and to defer or bound background repair,
  placement reconciliation, and metadata catch-up.

Not implemented; the names used for these in the rest of this plan are
proposals:

- only the implicit `default` location at `$SINOMO_DATA_DIR/dfs/content` is
  configured. There is no surface for additional locations, ownership marker,
  stable volume identity, or `ContentStoreSet`, and DFS still constructs one
  `LocalContentStore`;
- the host-wide `NodeHealthSnapshot` (CPU, memory, devices), its
  `DeviceHealthMonitor`, an observation exporter, and health transition events;
- a distinct `DfsContentLocationId`, the `dfs.content-locations.v1` capability,
  `NodeManagementClient` and its routes, preview/apply, and drain and
  relocation jobs; and
- the Settings node-management page and storage-location UI.

Of the delivery phases below, Phase 0 and Phase 1 are delivered only for the
DFS storage subset, and Phase 2 only for the aggregate advertisement. Phases 3
to 5 have not started.

## Load-Bearing Invariants

### One node remains one replica

Object placement continues to select replica `NodeId`s. A selected node then
chooses one healthy local content location:

```text
object placement policy
    selects NodeIds using W/N, reachability, and zones
                         |
                         v
                  selected node
                         |
                         v
             local content-store allocator
                selects one healthy pool
```

Two disks on the same node improve capacity and local operability, but they do
not provide two DFS receipts, two placement zones, or independent node failure
domains. A receipt remains durable only after one selected local location has
synced the object.

### Observation is not authority

Cluster liveness, cached health observation, and detailed host inspection are
separate signals:

1. cluster health remains the source for member and process liveness;
2. the observation plane exposes sanitized cached health and storage summaries;
3. raw paths, processes, interfaces, device evidence, and storage mutation stay
   behind target-node remote-management authorization.

Remote management being disabled means that detailed inspection is
unauthorized. It does not mean that the node or device is offline.

### The target node authorizes and validates every effect

Transport-selected target identity is authoritative. A node identifier inside
a request body is never sufficient authorization. Preview and apply run on the
target, and apply rechecks authority, local permission, configuration revision,
volume identity, and path safety immediately before mutation.

The node-local `remote-management` grant remains outside remotely writable
configuration and can never be enabled by the workflow described here.

### Configuration does not activate an arbitrary path

Desired storage-location configuration can use the authoritative typed config
service, but a generic config write must not activate an unvalidated
filesystem path. DFS owns semantic preflight, volume binding, layout
initialization, and lifecycle transitions.

### Retirement is drain-first

Removing configuration never deletes stored bytes. The normal lifecycle is
`Active -> Draining -> Detached -> Forgotten`. Destructive cleanup becomes
available only after retained objects are verified at safe destinations.

## Target Health Model

Keep host health in an R1 contract, close to `sinomo-device` and
`sinomo-observe`, rather than expanding the compact R0 cluster status record
with volatile metrics. A representative snapshot (proposed; the landed
`DfsNodeHealthSnapshot` covers only DFS content locations, metering, and
resource pressure) is:

```text
NodeHealthSnapshot
  node_id
  sequence
  observed_at
  summary
  resource observations
  attached device observations
  DFS content-location observations
  typed issues and collector errors
```

Each resource, device, and content location carries:

- a stable identifier;
- `Healthy`, `Degraded`, `Critical`, or `Unknown` health;
- the last successful observation time;
- typed reason codes and a concise human explanation;
- relevant bounded measurements, such as utilization or capacity; and
- partial-failure evidence without invalidating the rest of the snapshot.

`Fresh`, `Stale`, `Offline`, `Unauthorized`, `Unsupported`, and `Never sampled`
are observation states. They should not be folded into the resource-health
enum. The app derives staleness from the sample time and derives offline state
from cluster liveness.

### Daemon-owned sampler

Factor the existing native system observer into a persistent daemon-owned
host monitor (proposed `DeviceHealthMonitor`). It retains platform collector
state across samples and publishes immutable cached snapshots through an
`RwLock`, `ArcSwap`, or an equivalent bounded mechanism, as the landed
DFS-only `DfsDeviceHealthMonitor` already does.

Initial sampling cadence:

- CPU, memory, load, and inexpensive capacity counters every 15 to 30 seconds;
- device and mounted-volume inventory every one to five minutes;
- DFS content accounting from maintained counters, not repeated full-store
  scans; and
- immediate refresh after a known inventory or storage lifecycle transition.

Use hysteresis for low-space, high-utilization, and recovery transitions.
Transient CPU pressure is diagnostic and must not cause replica movement.
Missing, read-only, identity-mismatched, or reserve-exhausted storage can make
a location ineligible for new content.

The shadow daemon does not own a second sampler or a second DFS store registry.

### Publication and events

Observation exporters return cached state in O(1). They must not sleep,
enumerate devices, walk content directories, or refresh the platform collector
inside a request.

Publish only the sanitized fields needed for ordinary cluster views. Detailed
processes, network interfaces, raw paths, and device evidence remain available
through the existing privileged remote-management boundary.

Emit debounced transition events rather than one event per sample (none are
emitted yet):

- `node.health.changed`;
- `device.inventory.changed`;
- `device.health.changed`;
- `dfs.storage.location.changed`; and
- `dfs.storage.operation.changed`.

Events contain stable identifiers, revision, state, and reason codes. They do
not contain raw host paths or unredacted device evidence. A lagged subscriber
reloads the durable or cached snapshot rather than relying on replaying every
transition.

## DFS Content-Location Model

Add a node-local location identity (proposed `DfsContentLocationId`) and a
desired configuration record with at least the fields below.
`DfsContentLocationConfig` already carries the ID string, label, root,
lifecycle, quota, reserve, weight, and generation; volume identity and tags
remain to be added.

- stable ID and operator-visible label;
- canonical root path;
- expected stable volume identity;
- enabled and lifecycle state;
- quota or maximum DFS bytes;
- reserved free bytes;
- deterministic allocation weight;
- optional filesystem, media, or operator tags; and
- configuration generation.

The existing `$SINOMO_DATA_DIR/dfs/content` location becomes the implicit
legacy/default location. Existing installations continue to work without
moving their data.

### Stable volume identity and ownership marker

A pathname alone is not durable identity. Use the strongest stable
cross-platform identity available, such as a Unix device/mount identity or a
Windows volume GUID/serial, alongside reparse-safe canonical-path checks.

Every location contains a marker with:

- cluster ID;
- node ID;
- content-location ID;
- layout schema version; and
- configuration generation.

Activation acquires an exclusive location lock and proves create, write,
fsync, rename, and readback behavior before the location accepts content. A
different disk later appearing at the same mount path is not automatically
adopted.

### `ContentStoreSet` (proposed)

Replace the service's concrete single-store field with a multi-location facade
while retaining a one-location constructor for compatibility and focused test
migration. Each member can remain a complete self-describing
`LocalContentStore` with its own blobs, objects, and staging directories.

The facade owns:

- deterministic weighted allocation among active writable locations;
- an object-to-location locator that is cached and rebuildable;
- bounded fallback lookup across locations after index loss or restart;
- per-location capacity, quota, reserve, scrub, and health state;
- same-filesystem staging and commit for each object write; and
- immutable registry snapshots for in-flight readers while locations change.

One object version is committed wholly within one location. Receipt durability
is unchanged: the node emits a receipt only after the chosen location has
completed the required sync. A location ID may be attached to receipt evidence
for diagnostics, but placement correctness continues to use the node receipt.

### Aggregate node advertisement

Continue publishing one DFS advertisement per node:

- `accepts_content` is true when at least one active location is writable and
  above its reserve;
- capacity weight is derived from configured usable capacity, quantized, and
  hysteretic rather than publishing constantly changing free bytes;
- free capacity and detailed location state remain observation data; and
- zone remains a node-level placement property.

Add a `dfs.content-locations.v1` capability gate. An older daemon must not
advertise the node as healthy when current content exists only in locations it
cannot open.

## Management API and Workflow

The app should use a focused node-management client (proposed
`NodeManagementClient`), not add host administration to `SifsBrowser`. The
implementation can use the existing target-aware daemon object RPC path while
keeping transport-selected target identity authoritative.

Suggested route families:

- `/daemon/node/health/snapshot`;
- `/daemon/node/health/refresh`;
- `/daemon/dfs/storage/locations/list`;
- `/daemon/dfs/storage/mutation-preview`;
- `/daemon/dfs/storage/mutation-apply-v1`;
- `/daemon/dfs/storage/operation`; and
- `/daemon/dfs/storage/activity`.

Storage mutations are typed operations:

- `Add`;
- `UpdatePolicy`;
- `Reactivate`;
- `BeginDrain`;
- `CancelDrain`; and
- `Forget`.

### Preview and apply

Preview returns observed filesystem, capacity, volume identity, expected
impact, warnings, blockers, and an opaque short-lived token bound to:

- target node identity;
- configuration revision;
- canonical path and volume identity;
- validated impact fence; and
- expiry.

Apply requires that token plus an idempotency key. This is necessary because a
remote retry may not know whether the first request was applied. Apply repeats
all security-sensitive checks and rejects a stale preview rather than silently
replanning.

### Authorization

Local callers use the normal local-administration boundary. A nonlocal caller
requires voter/admin authority plus the target's locally granted
`remote-management` permission. The target rechecks permission at apply and
before each later destructive host effect.

Permission to manage a node does not grant microphone capture, camera access,
or other device-resource operations. Device resource-exposure policy remains a
separate authorization domain.

### Target-side validation

Reject:

- relative paths, symlinks, reparse points, and mount-swap races;
- filesystem roots;
- nested, duplicate, or overlapping content locations;
- overlap with KV/Raft, DFS metadata, staging, configuration, credentials,
  daemon state, permission state, or SIFS exposure roots;
- nonempty directories without a matching Sinomo ownership marker;
- unsupported or read-only filesystems;
- network or removable media without explicit future opt-in;
- insufficient reserve or invalid quota arithmetic; and
- volume identity changes between preview and apply.

Validation and initialization occur on the target node. Remote shell or
symlink-based relocation is never a supported management path.

## Drain and Reconciliation

Beginning a drain immediately prevents new allocations to the location. A
durable node-local relocation job then copies and verifies every retained
object to another healthy local location.

Because the replica remains on the same node, a successful local relocation
does not change placement receipts. When no safe local destination exists, the
job asks the existing placement reconciler to establish another node replica
before any source deletion. It must never infer safety merely from desired
configuration.

`Forget` becomes available only when the location is empty or every retained
live version is proven durable elsewhere. Unexpected drive loss is a failed or
unavailable location, not an implicit forget. Reattaching the expected volume
can resume recovery from its marker and durable job state.

Extend the Reconciliation Activity center with an explicit local-storage job
kind showing:

- initializing, draining, retrying, failed, completed, or cancelled state;
- affected objects and bytes;
- copied, verified, and remaining counts;
- copies added or removed at the cluster level when applicable;
- retry timing and last error; and
- source and destination location labels, with raw paths shown only to an
  authorized viewer.

## App Experience

### Settings: node management

Expand **Settings -> This device** into the primary node-management page:

- a node header with liveness, observation freshness, version, and role;
- summary cards for CPU, memory, attached devices, and DFS storage;
- a keyed virtual device list grouped by input, output, compute, storage, and
  other;
- a storage-location table showing label, state, used/free/total capacity, DFS
  bytes, reserve, volume type, last observation, and explanation; and
- add, edit, drain, resume, and forget actions gated by current authority and
  lifecycle state.

Use stable row IDs, retained virtual collections, per-row rebuild boundaries,
last-good snapshots, explicit stale presentation, request generations, and
draft state outside structural snapshots.

### Cluster node detail

Show a compact redacted health and capacity summary with two actions:

- **Open node health**;
- **Manage storage**.

When remote management is disabled or denied, leave the controls discoverable
but disabled with the exact reason. Do not label an unauthorized node offline.

### Add-location flow

1. Choose the node; for a remote target, label the path as being on that node.
2. Choose an observed fixed volume or enter a path.
3. Set label, reserve, quota, and allocation weight.
4. Preview on the target.
5. Review filesystem, volume identity, capacity, warnings, blockers, and
   configuration revision.
6. Apply the preview token.
7. Follow `Validating -> Initializing -> Active` through operation activity.

### Files placement integration

The Files placement editor continues to configure object and subtree placement,
not disks. Candidate rows may show pleasant aggregate evidence such as
`2.4 TB free - 3 locations - accepting content` and link storage-pressure or
insufficient-capacity explanations to node management.

## Delivery Plan

### Phase 0: contracts and safety foundation

Deliver:

- versioned health, location, operation, and error contracts;
- stable platform device and volume identity adapters;
- marker and lock format;
- capability and configuration-generation model;
- path, overlap, filesystem, quota, and reserve validation; and
- fake platform, volume, and clock providers for deterministic tests.

Exit criteria:

- model round trips and compatibility tests pass;
- redacted and privileged views have explicit field-level tests;
- preview/apply rejects path and volume identity changes; and
- mixed-version nodes cannot claim support accidentally.

### Phase 1: read-only health vertical slice

Deliver:

- persistent daemon sampler and cached snapshots;
- sanitized observation exporter and privileged detailed snapshot;
- health transition events;
- Settings and cluster-detail health surfaces; and
- correct fresh, stale, unauthorized, unsupported, offline, and never-sampled
  presentation.

Exit criteria:

- requests perform no sampling sleeps or unbounded filesystem scans;
- partial collector failure preserves last-good independent fields;
- UI refresh and daemon rebinds do not regress to empty or misleading state;
  and
- sampling remains bounded while the app is closed.

### Phase 2: local multi-location DFS

Deliver:

- `ContentStoreSet` with legacy one-location compatibility;
- default location plus one additional fixed local location;
- multi-location allocation, reads, restart recovery, scrub, quota, and reserve
  behavior;
- aggregate node advertisement; and
- capability-gated startup and downgrade checks.

Exit criteria:

- adding a second location does not increase replica or zone counts;
- writes survive restart and locator rebuild;
- a full, read-only, missing, or identity-mismatched location is handled
  without corrupting healthy stores; and
- crash tests cover staging, commit, sync, and registry replacement.

### Phase 3: remote management and storage UI

Deliver:

- inventory, preview, apply, status, and activity routes;
- idempotent mutation and optimistic concurrency;
- target-side permission rechecks and audit events;
- add-location and policy-editing UI; and
- clear unsupported, denied, stale-preview, and volume-changed recovery paths.

Exit criteria:

- a normal member cannot mutate a remote node;
- a voter with target-local permission can preview and apply;
- permission cannot be enabled remotely;
- revocation between preview and apply rejects the effect; and
- duplicate apply requests return the same operation rather than creating a
  second location.

### Phase 4: drain, relocation, and placement integration

Deliver:

- durable local relocation jobs;
- safe detach and forget gates;
- Reconciliation Activity center integration;
- capacity and writable-state placement explanations; and
- cross-node repair fallback when no safe local destination exists.

Exit criteria:

- the last durable object copy is never removed;
- restart during drain resumes idempotently;
- unplug and reattach of the expected volume recovers safely;
- a different volume at the same path is rejected; and
- node advertisements converge without high-frequency KV churn.

### Phase 5: advanced locations and history

Only after the fixed-local design passes real-cluster gates, consider:

- removable and network filesystems;
- encrypted or externally managed volume classes;
- SSD/HDD/tier tags and local allocator policy;
- locally retained longer-term health history; and
- explicit resource-scoped grants narrower than general remote management.

## Verification Strategy

### Unit and property tests

- stable device and volume identities;
- freshness, hysteresis, and partial-snapshot rules;
- redaction and authorization projections;
- path canonicalization, overlap, and mount-swap detection;
- deterministic weighted allocation;
- reserve and quota arithmetic;
- receipt count invariance across any number of local locations; and
- idempotent preview/apply and relocation state transitions.

### Integration tests

- restart and crash during write, activation, registry swap, and drain;
- full disk, read-only filesystem, drive disappearance, and wrong replacement
  volume;
- duplicate content across locations and locator rebuild;
- permission revocation during an operation;
- mixed old and new daemon versions;
- UI reconnect, stale response suppression, large device/location lists,
  keyboard navigation, large text, and narrow layouts; and
- health event loss followed by snapshot reload.

### Real-cluster gate

Use at least three nodes and more than one supported operating system when
available. Prove that:

- an extra location increases usable node capacity but not W/N receipts;
- confirmed and eventual writes preserve their existing placement guarantees;
- local drain relocates within the node when possible;
- cross-node reconciliation establishes safety before the last local copy is
  removed;
- remote permission and rolling-version behavior match the contract; and
- `cluster health`, `cluster doctor`, `dfs status --cluster --json`, placement
  activity, and the final worktree are clean before rollout completion.

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Symlink, reparse-point, or mount-swap attack | Bind preview to canonical path and stable volume identity; recheck at apply and open files relative to a verified directory handle where supported. |
| Treating disks as independent replicas | Keep policies, receipts, advertisements, and zones keyed by `NodeId`; location IDs are node-local evidence only. |
| Deleting the last copy during drain | Require verified local relocation or successful inter-node reconciliation before deletion. |
| High-churn or spoofable capacity input | Publish dampened aggregate eligibility; keep raw capacity in cached observations. |
| Request-time telemetry or directory scans | Maintain daemon-owned snapshots and accounting counters; make exporters O(1). |
| Leaking host topology | Redact cluster summaries and events; expose raw paths only through authorized target-node management. |
| Old daemon opens only the legacy root | Capability-gate activation and startup; reject unsafe downgrade while nonlegacy locations contain unique content. |
| Remote retry applies twice | Require expiring preview tokens, config generations, and idempotency keys. |
| Revocation during long-running drain | Recheck before new destructive effects and leave the durable job in a safe resumable state. |

## Recommended Initial Slice and Effort

Start with Phase 0 and Phase 1, then prove one additional fixed local location
without remote mutation or drain. This establishes health semantics, platform
identity, and the multi-store durability boundary before the most privileged
workflow is exposed.

A health-only vertical slice is roughly one to two engineer-weeks. Safe
cross-platform multi-location DFS with remote administration, restart recovery,
and drain semantics is approximately five to eight engineer-weeks, depending
on the supported platform and filesystem matrix.
