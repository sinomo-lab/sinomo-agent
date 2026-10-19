# DFS Policy and Authority Hardening

Status: remaining follow-up work under the current DFS authority contract.
Last reviewed 2026-10-13.

The current DFS architecture supports local-owned, replicated-eventual, and
replicated-confirmed workflows with deterministic candidate selection and
shard-local metadata authority. This plan tracks behavior that is intentionally
not described as implemented in the [DFS architecture](../dfs-architecture.md).

## Authority During Quorum Loss

The selected contract uses existing coherent configuration projections,
actual shard participants, and DFS-owned durable writer and handoff fences.
Ordinary metadata publication and sync require the complete assigned
participant proof. Missing evidence leaves the operation pending or
unavailable without lowering counts or issuing a cluster-KV metadata permit.
Content receipt W/N remains a separate policy.

Cluster KV retains its ordinary atomic transactions for configuration and
genuine cluster-wide guards. It does not acquire caller bindings, retry
sessions, a DFS operation ledger, or new Raft machinery. Authority handoff
must preserve the exact retiring prefix and durable generation exclusion;
whole-root installation must also fence retained old backend handles.
Conformance, restart, partition, and retained-handle tests must demonstrate
these obligations on the final implementation. Superseded KV-assisted
metadata fallback designs are not requirements for future work.

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
