# DFS Policy and Authority Hardening

Status: partially implemented; remaining items are proposed. Last reviewed
2026-10-13.

The current DFS architecture supports local-owned, replicated-eventual, and
replicated-confirmed workflows with deterministic candidate selection and
shard-local metadata authority. This plan tracks behavior that is intentionally
not described as implemented in the [DFS architecture](../dfs-architecture.md).

## Authority During Quorum Loss

Implemented. A primary publishes only after every configured metadata-shard
replica acknowledges the exact prepare, or after the DFS recovery quorum is
fenced by an operation-bound cluster-Raft permit; without KV quorum,
partial-shard publication fails or waits. Shard handoff installs a durable
retiring-primary write freeze before a successor can activate. The contract is
defined in [Consistency Invariants](../dfs-architecture.md#consistency-invariants)
and covered by the formal models.

## Complete Reserved Object Modes

Make append-only and ephemeral-local modes complete service policies rather than
only enum and routing vocabulary. Define their write, promotion, replication,
recovery, quota, and garbage-collection semantics before advertising them as
durable workflows.

## Structured Policy

Extend policy beyond minimum and desired replica counts. The durable policy
model should cover:

- eligible and forbidden nodes or trust domains;
- raw-content egress and encryption requirements;
- stale-read and repair behavior;
- retention and version limits;
- object and chunk size limits;
- inherited subtree overrides.

Required and forbidden node lists, a minimum zone count, inherited subtree
placement rules, and file-history retention rules exist today; trust domains,
raw-content egress and encryption, stale-read and repair behavior, and size
limits remain open. Policy must be inspectable at the authority that commits
metadata and at every node that stores or serves bytes.

## Placement Inputs

Placement snapshots already rank storage health, capacity, path performance,
network metering and coarse resource pressure, and placement policy supports
required and forbidden nodes and a minimum zone count. Integrate session
locality, privacy, and trust-domain eligibility into the same deterministic
placement. The result must remain stable for the same authoritative input
snapshot and explain why each replica or cache target was selected or rejected.
