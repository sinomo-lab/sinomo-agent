# Sinomo Build Service Architecture

## Scope

This document defines the cluster service that turns authorized immutable
source inputs into Sinomo binary and module artifacts. It describes the authority
boundary, scheduling contract, artifact flow, and failure behavior. Command
syntax and implementation inventories belong elsewhere.

Building and rollout are separate operations. The build service may compile,
package, verify, cache, and publish an artifact. It cannot select rollout lanes,
change desired state, activate an artifact, promote a candidate, or approve its
own output. Those decisions remain with rollout policy and the target node.

The first implementation delivers authenticated live capability RPCs, a
node-local durable queue, exact target-aware Cargo execution, a verified
node-local artifact cache, and CLI scheduling/distribution. Expiring
advertisements and leases in cluster KV, shared registry/DFS artifact lookup,
signing, and historical reliability estimates are the next distributed
coordination layer described by this document; operators should not assume
those pieces are already active.

```text
authorized source + build requirements
                |
                v
       cluster build service
       (cache, queue, schedule)
                |
                v
       immutable artifact store
                |
                v
          rollout authority
       (stage, activate, observe,
          promote, roll back)
```

## Build Identity

A build request identifies all inputs that can affect the output. At minimum it
includes the authorized source revision or tree, dependency lock state, artifact
kind, target identity, build profile and features, toolchain identity, and
applicable build policy. The canonical digest of those inputs is the build key.

Target identity is explicit. Operating system and architecture are necessary
but may not be sufficient: ABI, target triple, minimum CPU features, loader or
runtime compatibility, and toolchain requirements are included when they
affect whether an artifact can run. A scheduler must not infer compatibility
from a node name, device role, or loose operating-system label.

Successful output is an immutable binary or module artifact plus a manifest
that records its content digest, build key, target requirements, builder
identity, source and toolchain provenance, verification evidence, and signer.
Mutable aliases or rollout channels may point to an artifact, but they do not
change its bytes or identity.

## Cluster Service and Worker Advertisements

Build workers are authenticated cluster services. A worker advertises:

- the target identities and toolchains it can build, including supported
  cross-compilation paths;
- available build slots and relevant CPU, memory, and disk headroom;
- current queue pressure and load;
- cache and artifact locality useful for placement;
- an expiry time for the observation.

Durable service registration makes a worker discoverable. It does not prove
that the worker is currently reachable, has free capacity, or still supports a
toolchain. Scheduling therefore uses fresh, expiring advertisements and route
health. Cluster membership alone never makes a node an eligible builder.

Advertisements are placement evidence, not resource reservations. Before
dispatch, the scheduler acquires a short-lived lease for a compatible worker
slot. If the reservation races with another job, expires, or the worker becomes
unreachable, the scheduler selects another eligible worker.

## Cache, Queue, and Scheduling

The scheduler resolves work in this order:

1. Group requested rollout targets by compatible target identity.
2. Look for a verified artifact matching each build key in the registry,
   distributed content store, and authorized caches.
3. Coalesce concurrent requests for the same build key into one queued or
   running job.
4. Filter workers by trust, policy, fresh liveness, target and toolchain
   support, route availability, and sufficient resource headroom.
5. Rank eligible workers by estimated time to a usable artifact, including
   queue delay, expected build duration, cache locality, transfer cost,
   resource pressure, and recent reliability.
6. Reserve a worker slot with a lease and dispatch the job. Re-rank on failure
   or saturation.

Selection is not local-first. A lightly loaded server that can build and
deliver a Raspberry Pi artifact in minutes should be preferred over compiling
for hours on the target, subject to compatibility, trust, policy, and transfer
cost.

Queued job state is durable enough to survive scheduler restart and supports
deduplication, cancellation, and retry. The compiler process itself need not be
resumable. Cancellation stops work when no authorized consumer still needs the
build; it does not remove an already published immutable artifact.

## Build and Distribution Flow

A worker materializes the identified source and dependencies in an isolated
workspace, verifies the request and policy, performs the build and required
checks, packages the output, and publishes the artifact and manifest. A job is
successful only after the published bytes can be retrieved and independently
verified by content digest.

Artifacts are distributed from the best authorized source, not necessarily the
worker that produced them. Registry, DFS, peer, and node-local caches may all
serve immutable bytes. The receiver verifies digest, signature, provenance,
and target compatibility before staging. Target-independent modules can be
reused across target groups; target-specific modules follow the same grouping
rules as binaries.

Cluster KV contains only compact coordination data such as worker
advertisements, build requests and status, leases, and artifact references.
Source bundles, dependency payloads, artifact bytes, full logs, and other
unbounded evidence live in their owning content or evidence stores.

## Rollout Handoff and Authority

Rollout declares the intended source, artifacts, target set, and policy. The
build service returns verified artifact references for each compatible target
group. Rollout then owns distribution intent, lane selection, target admission,
staging, activation, health gates, promotion, and rollback.

A single operator command may orchestrate build resolution followed by rollout,
but the phases retain separate state and authority. Artifact resolution for a
rollout wave completes before that wave begins staging, so compiler work never
runs as part of target activation.

Every target re-authorizes the request and performs final compatibility and
integrity checks before activation. A builder cannot write node-local loader
state or installed binary state merely because it produced the artifact. A
build result also cannot approve itself or become cluster desired state without
an authorized rollout transition.

Cache-first resolution is the normal path. Rollout should distribute an
existing verified artifact whenever its build key and target contract match,
rather than rebuilding it on every node or once per target.

## Failure and Fallback

Failure handling preserves compatibility and authority:

- a rejected or corrupt cache entry is quarantined and the request returns to
  normal resolution;
- a lost worker lease requeues the job and selects another eligible worker;
- a failed build records evidence against that build key without activating or
  promoting partial output;
- a disconnected target may receive the completed artifact after reconnecting;
- when no compatible worker is available, the job remains visibly queued or
  fails with an explicit capability blocker.

Fallback prefers another compatible cluster worker, then waits for declared
capacity or toolchain support. Building on the rollout target is allowed only
when that node advertises the required build capability and policy explicitly
permits it. Fallback never relaxes target compatibility, provenance,
verification, or activation policy.

## Security and Observability

Build requests and worker traffic are authenticated and authorized. Workers
use isolated workspaces and scoped credentials, and cannot turn source input
into ambient filesystem, network, secret, or cluster authority. Publishing
records the builder and evidence so downstream policy can distinguish trusted,
reproducible, reviewed, and exceptional builds.

Operators need visibility into queue depth, coalesced requests, cache hits and
misses, worker eligibility and selection reasons, the resource snapshot used
for placement, lease and retry history, build and transfer duration, outcome,
artifact digest, and provenance. Build telemetry remains distinct from rollout
activation and health telemetry so a successful build cannot be mistaken for a
successful rollout.

## Architectural Invariants

1. Building produces immutable artifacts; it never grants activation or
   promotion authority.
2. A build key captures every declared input that can affect artifact identity
   or compatibility.
3. Target compatibility is explicit and independently verified by the
   receiving node.
4. A matching verified artifact is reused before new compilation is scheduled.
5. Concurrent requests for one build key do not intentionally create duplicate
   builds.
6. Builder selection uses fresh capability and resource evidence plus a
   lease; membership and durable discovery are insufficient.
7. Scheduling minimizes time to a usable artifact without weakening trust,
   policy, or compatibility.
8. Cluster KV stores compact coordination and references, never artifact bytes
   or unbounded build output.
9. Worker loss can delay or retry a build but cannot leave a partial artifact
   eligible for rollout.
10. Rollout and the receiving node retain sole authority over staging,
    activation, promotion, and rollback.

The [clustering architecture](clustering-architecture.md) defines discovery,
health, and leases. The
[module lifecycle architecture](module-lifecycle-architecture.md) defines
artifact and activation state. The
[storage architecture](storage-architecture.md) defines local path ownership,
and the operator workflow is in the [rollout guide](user/rollout-operations.md).
