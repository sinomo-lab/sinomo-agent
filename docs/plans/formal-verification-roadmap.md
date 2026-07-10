# Formal Verification Roadmap

Status: active backlog. Last reviewed 2026-07-09.

This plan tracks the next bounded verification slices. Current models,
properties, and execution instructions live in the
[cluster formal verification guide](../formal/cluster-verification.md).

New models should consume established lower-layer guarantees rather than
re-modeling routing, authority, content validation, and retry semantics in one
large state machine.

## Network

1. **Route-cache invalidation.** Model directory versions, local connectivity
   marks, and transport-module snapshots. Prove that a route plan is not reused
   after an input affecting its validity changes.
2. **Timed retry conformance.** Add deterministic virtual-time Rust scenarios
   for late responses, fallback after a primary timeout, delivery duplicates,
   packet loss, and identity-mismatch suppression.
3. **Purpose-specific endpoint selection.** Refine selection for rollout, Raft,
   member status, and data traffic while preserving the common authorization
   and reachability rules.
4. **Stream retry budgets.** Extend delivery models with streaming capability
   and raw-response constraints. Prove that fallback cannot expand the caller's
   deadline or select an incompatible transport.

## Integrated System

1. **Projection contracts.** Map guarantees from focused membership, routing,
   retry, and DFS authority specs into the shared system model through explicit
   assume/guarantee boundaries.
2. **Directory and cache races.** Model directory snapshot versions, route
   cache marks, and in-flight plans. Prove that membership removal, identity
   mismatch, and endpoint-generation changes invalidate affected routes before
   new work is authorized.
3. **Bug-trace harnesses.** Encode fragile networking scenarios as small smoke
   modules and retain both random exploration and fixed-seed replay.

## DFS

1. **Anti-entropy progress.** Under explicit fairness assumptions, prove that a
   missing desired replica eventually obtains a durable receipt or remains
   visibly suppressed because authority was lost.
2. **Sharded namespace recovery.** Model the failure window where the target
   shard transaction commits but its routing index does not. Decide and prove
   whether reopen repairs it automatically or explicit retry owns recovery.
3. **Chunked patch semantics.** Prove that patch commit validates base version,
   base content, offset, and final content hash.
4. **Range-read assembly.** Model source retry, stale-source rejection, and
   chunk assembly. Prove that completed reads match current metadata and that
   mismatched responses cannot populate local state.
5. **Replica placement.** Prove that duplicate candidates, owner normalization,
   and policy widening preserve the configured commit and desired replica
   counts.
6. **Authority handoff.** Prove that the old primary becomes quiescent and the
   new primary eventually rediscovers and schedules committed repair work.
7. **POSIX conformance.** Replay finite namespace, link, cross-shard, and
   visibility scenarios against the Rust DFS service, then deepen directory,
   by-id lookup, and aborted-move lock-release models.
