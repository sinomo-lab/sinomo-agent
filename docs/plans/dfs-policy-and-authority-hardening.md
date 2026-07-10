# DFS Policy and Authority Hardening

Status: proposed follow-up work. Last reviewed 2026-07-09.

The current DFS architecture supports local-owned, replicated-eventual, and
replicated-confirmed workflows with deterministic candidate selection and
shard-local metadata authority. This plan tracks behavior that is intentionally
not described as implemented in the [DFS architecture](../dfs-architecture.md).

## Authority During Quorum Loss

Define and enforce the relationship between the latest cached shard assignment
and current cluster quorum. Today a shard primary may continue shard-local
metadata commits from its latest valid assignment even when KV is read-only.

The hardening work must choose an explicit contract, fence stale authority, and
add conformance and formal tests. Options include a renewable authority lease,
a quorum-fresh epoch proof for visible commits, or a deliberately documented
degraded mode with bounded risk.

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

Policy must be inspectable at the authority that commits metadata and at every
node that stores or serves bytes.

## Placement Inputs

Integrate health, storage capacity, endpoint quality, session locality, battery,
bandwidth, privacy, and trust-domain eligibility into deterministic placement.
The result must remain stable for the same authoritative input snapshot and
explain why each replica or cache target was selected or rejected.
