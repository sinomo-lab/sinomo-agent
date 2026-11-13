---
name: sinomo-distributed-protocols
description: Design, change, or diagnose Sinomo networking, KV/Raft, membership, DFS consistency, replicated state, or SOBJ protocol behavior. Use for authority, routing, retry, fencing, durability, recovery, and wire-contract changes.
---

# Sinomo distributed protocols

Work from the repository root. Select the affected contract before changing its
implementation; plans and old experiment reports are not the current protocol.
Architecture supplies purpose, ownership and required behavior. Obtain exact
ACK rules, state transitions and lock ordering from source/tests and dedicated
protocol references; use [sinomo-documentation](../sinomo-documentation/SKILL.md)
when recording a change instead of appending its mechanics to architecture.

For repeated DFS failures, use the [DFS debugging skill](../sinomo-dfs-debugging/SKILL.md)
to trace runtime ownership and wait dependencies, and the
[DFS validation skill](../sinomo-dfs-validation/SKILL.md) for fixture, retained-state
and failure-matrix work. They provide concrete diagnosis procedures and do not
replace the required protocol behavior.

## Locate the contract

| Concern | Read first | Implementation entry |
|---|---|---|
| Direct links, connectivity, routed streams, object RPC | [Network architecture](../../../docs/network-architecture.md), [data-path reference](../../../docs/developer/network-data-path.md) | [sinomo-network](../../../crates/sinomo-network/src/lib.rs) |
| Elections, membership, committed state and authoritative reads | [Clustering architecture](../../../docs/clustering-architecture.md) | [sinomo-kv](../../../crates/sinomo-kv/src/lib.rs), [sinomo-cluster](../../../crates/sinomo-cluster/src/lib.rs) |
| Metadata authority, writes, namespace operations, repair | [DFS architecture](../../../docs/dfs-architecture.md) | [sinomo-dfs](../../../crates/sinomo-dfs/src/lib.rs) |
| Encoding or compatibility | [SOBJ format](../../../docs/sinomo-object-format.md) and the owning domain's types | Follow the actual request/response and persisted-record codecs |

Trace both the sender and receiver, including the direct RPC and routed-message
paths when both exist. Locate the durable write, acknowledgement, replay, and
restart paths; a success response alone does not identify the commit boundary.

## Preserve Sinomo's distinctions

- Discovery and connectivity provide routes, not membership or effect authority.
  Validate authenticated source, destination, filesystem/shard, and epoch at the
  owning boundary. Graph reachability must become a usable routed stream.
- Keep networking and KV independent; `sinomo-cluster` is their bridge. Direct
  transports establish peer identity, object RPC owns framing, and domains own
  authorization, operation meaning, idempotency, and content verification.
- Resident applied Raft state and bounded authoritative projections authorize
  control-plane work. The materialized KV file is an observational checkpoint.
  A locally equal value cannot substitute for a quorum-backed write result.
- Cluster KV holds compact control records and fences. DFS paths, journals,
  intents, receipts, namespace transactions, and bytes stay in DFS-owned stores.
- Content receipt threshold `W` is distinct from metadata publication authority.
  Current DFS publication requires the complete configured metadata participant
  proof. Missing participants leave it pending/unavailable; there is no ordinary
  per-operation Raft permit, KV floor or watermark fallback. The resulting
  `MetadataCommitOutcomeUnknown` is an unknown outcome of a durable prepare,
  not a failure, and does not imply majority admission.
  This is a known implementation mismatch with the user's original outage/W/N
  requirement, not a product constraint to preserve indefinitely. Restore that
  behavior with a safe protocol; do not merely lower a count, revive a removed
  permit path, or redefine the requirement to match this implementation.
- A committed shard reconfiguration is not activation. Retiring-primary freeze,
  exact prefix evidence, both assignment majorities, and both primary
  acknowledgements govern the reviewed implementation's handoff. Trace its
  sender/receiver and tests before changing a threshold or rebind shortcut;
  these mechanisms must still satisfy the original outage requirement.
- Replica ingest is a narrow permit, not author authority. Validate the complete
  journal/snapshot payload, including embedded records, before partial mutation.
  A later sequence number alone is not proof of the correct history.
- Preserve stable operation identity across retries. Distinguish rejection,
  committed outcome, and unknown/possibly-applied delivery; reconcile ambiguous
  results before initiating another mutation. Cancellation does not undo an
  already durable effect, so retain ownership of its follow-up work.
- Scheduling signals, resource observations, metrics, and cached projections
  cannot grant authority or weaken durability and pin requirements. Observation
  reads do not become generic KV/DFS maintenance polling triggers.

When fixing existing behavior, prefer tracing and repairing its owning mechanism
before adding another system. This preference does not override the architecture,
correctness or availability requirements above.

For a wire or persistence change, decide whether mixed-version operation is
required. Use explicit versions/capability gates for new semantics that old peers
must reject. An additive default is safe only if ignoring it preserves behavior.
For an accepted breaking change, update every in-tree caller and fixture instead
of adding an unneeded compatibility layer. Keep typed recovery decisions separate
from diagnostic text; isolate required legacy interpretation at the boundary.

## Match evidence to the changed boundary

Use the [testing matrix](../../../docs/developer/testing.md) and
[formal guide](../../../docs/formal/cluster-verification.md). Exercise the relevant
failure edges: duplicate or lost replies, stale authority, partitions, partial
persistence, restart, cancellation after commit, and mixed-version decoding.
For DFS changes, also run `bash scripts/ci/check-dfs-kv-boundary.sh`; the
[lint README](../../../lints/sinomo-architecture/README.md) defines the narrow
KV boundary markers. Do not duplicate consensus or connectivity logic to avoid it.

For formal work, find the actual case ID in
[check-quint.sh](../../../scripts/formal/check-quint.sh); its model, contract,
check, and scenario entries have different roles. For example, a KV change may
use the following Bash commands, while a DFS change needs its own matching case:

```bash
QUINT_CASE_FILTER=raft-kv QUINT_REQUIRE_WITNESSES=1 \
  bash scripts/formal/check-quint.sh coverage
QUINT_CASE_FILTER=raft-kv bash scripts/formal/check-quint.sh verify-fast
```

Record model classification, checked bounds, seed/samples, required witnesses,
and the Rust behavior it refines. Do not present an abstract or target-hardening
model as proof of implemented behavior. Use real Docker transport/process/disk
scenarios when those boundaries change; an in-process test does not establish
them. Follow the [Docker guide](../../../docker/README.md) and the
[Docker verification skill](../sinomo-docker-verification/SKILL.md) for that lane.

Explain the resulting authority and durable-state transitions, compatibility
decision, evidence obtained, and any remaining unverified boundary.
