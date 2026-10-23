# Review the mechanism before adding another state owner

Read this when a DFS fix proposes new synchronization, retry bookkeeping,
compatibility, cached state, persisted identity or a generic API extension. The
goal is a smaller correct algorithm that retains the user's behavior. This is
not a ban on necessary state and not a request for another review/approval loop.

The [development lessons](../../../../docs/developer/dfs-development-lessons.md)
explain the failure patterns independently of any investigation history. Resolve
the implementation entry points below against the working source with CodeGraph.

## 1. Separate the requirement from the constraints your patch introduced

Write the original positive case and the safety negative beside the failing
case. For DFS: W qualifying durable content receipts, N eventual replicas,
supported surviving-node operation, and no divergent visible history. A metadata
implementation must provide the safe authority needed for that behavior; it
cannot exempt itself from the end-to-end availability requirement.

Compare the patch's preconditions with the baseline. Added requirements for every
peer, a healthy unrelated provider projection, a drained global queue or an old
primary's response are observable restrictions even if introduced as guards.
For each restriction ask: is it inherent in the original requirement, or only
needed to make the newly chosen implementation safe?

Consider a change that removes an alternate authorization path and retains only
full-participant confirmation. The change may eliminate one safety hazard while
making a missing peer mandatory. A lock test changed from partial-participation
success to denial until another node returns verifies that narrower behavior;
it does not prove the original availability requirement.

Keep a surviving-node positive and an unsafe competing-history negative independent
of the patch. A present guard may protect a real hazard: replace the responsible
algorithm with its proof rather than deleting that guard in isolation. Report the
positive requirement as unmet until it actually works.

## 2. Determine whether the caller needs an effect, retry or observation

Before changing retry semantics, follow the failing call to its intended effect.
Classify it explicitly:

| Caller needs | Existing operation to seek | Avoid |
|---|---|---|
| First authorized mutation | Normal transactional/domain mutation API | Inventing recovery authority from an old record |
| Resolution of an uncertain mutation | Same operation/body and authoritative outcome; exact domain continuation where valid | New ID, modified predicates or a second outcome ledger |
| Confirmation that an already committed mutation happened | Authoritative outcome observation, exact identity/digest, current guarded domain state | Resubmitting the mutation to obtain a success-shaped response |
| Refresh of a derived view | Existing owner's snapshot/delta/cursor boundary | A new independent journal or polling reconciliation owner |

A retained transaction may contain an unchanged mutation and caller comparisons,
while host-added authorization comparisons differ on a later submission. Before
adding caller/envelope binding, persistence and replay APIs, determine whether the
caller is actually asking for another effect. If it only needs confirmation,
observe the existing committed outcome and validate the exact original command
and current domain scope. Audit sibling callers for the same receipt-only
resubmission mistake.

Current entry points:

```sh
codegraph explore 'observe_original_native_commit evaluate_original_native_commit validate_original_command_preimage placement_outcome_evaluated'
codegraph explore 'kv_request_outcome_authoritative_async replayed_routed_write_outcome_if_known recover_prepared_write_for_retry'
```

- [handoff.rs](../../../../crates/sinomo-dfs-migration/src/authority/handoff.rs)
  observes the original native commit; its comment explicitly excludes first
  effects. `validate_original_command_preimage` matches the original bare body or
  the historical physical body with captured authorization compares.
- [placement custody](../../../../crates/sinomo-dfs-migration/src/authority/placement_work/custody.rs)
  uses `placement_outcome_evaluated` and typed `Committed`, `RejectedBeforeCommit`,
  `Unknown`/`Expired` outcomes. Unknown is not success or a license to resubmit.
- [service_commit.rs](../../../../crates/sinomo-dfs/src/service_commit.rs) and
  [service_routed_write.rs](../../../../crates/sinomo-dfs/src/service_routed_write.rs)
  already own same-operation DFS continuation and result replay.

Test observation with a real committed request whose reply was lost: exact
outcome is recovered, no second mutation/command is submitted, changed original
body is rejected, and current authority is still checked. Historical preimage
matching stays in migration; do not make it a new general-purpose retry protocol.

## 3. Prove a legacy shape had a producer before supporting it

An optional field or decoder default can admit record shapes that no real writer
produces. For example, a permit type can allow missing cluster, node and runtime
identities while every ordinary constructor supplies them. Negative fixtures with
missing identities do not establish a compatibility obligation for anonymous
permits. Building drain routes and recovery state for them expands the supported
algorithm without a demonstrated producer.

For the suspect record, perform a producer census:

1. Establish the supported writer versions and the schemas they can produce.
2. Enumerate constructors **and serialization/persistence callers**, including
   alternate real ingress paths. Read defaults, not just the type declaration.
3. Separate normal writes, actual saved operator DATA, import adapters, negative
   tests and manually assembled fixtures. A decoder accepting a shape is not a
   producer; a malformed saved row is not automatically supported history.
4. Name the version/artifact and write path that could emit each supported shape.
   If none is found, report that evidence and preserve/reject the invalid bytes.
5. Remove obsolete runtime paths together with their dedicated producers,
   consumers, DTOs, routes, watchers, timers and tests. Before removing low-reference
   code, provide caller/reachability, behavior, compatibility and ownership evidence
   plus the exact edit list. An unindexed dynamic route is still a possible caller.

Use CodeGraph to locate the record, actual constructors and persistence callers;
then inspect current references and deliberately malformed fixtures separately:

```sh
codegraph explore 'DfsPlacementAuthorityPermitRecord constructors persistence and consumers'
rg -n 'DfsPlacementAuthorityPermitRecord|holder_cluster_id|holder_node_id|holder_session_id' crates/sinomo-dfs crates/sinomo-dfs-migration
```

Malformed-input handling should preserve and reject the original record. It does
not need a new grant, deletion protocol or permanent Pending state. Test each
missing identity separately from the valid fully identified recovery path.

Sinomo is unreleased with no external users in this task's context. Do not add
parallel wire versions, old writer branches or indefinite codec negotiation just
because an internal interface changes. Actual retained operator data can require
a bounded converter; inventory that obligation explicitly. No-user status does
not authorize deleting that data, and data preservation does not imply runtime
compatibility with every historical or synthetic shape.

## 4. Make one small table of state owners and eliminate duplicate responsibility

For the failing path only, fill in the following. This is task-local reasoning,
not a subsystem to implement:

```text
fact | authoritative owner | durable source | derived copies
     | mutation/invalidation edge | waiters | lifetime/restart rule
     | proposed add/remove/reuse | exact files/callers
```

Useful starting classifications:

| Fact | Existing owner/source | Redundant mechanism to question |
|---|---|---|
| Route, liveness and delivery uncertainty | `sinomo-network` routing/transfer and typed delivery result | DFS retry/health state that reinterprets the same transport failure |
| Cluster configuration and committed transaction outcome | Existing KV API and retained coherent configuration projection | DFS-specific KV session, second operation ledger, per-operation mirror of namespace history |
| Prepared body and semantic terminal result | Native DFS prepared-operation journal and exact outcome | Another pending/committed database requiring reconciliation with that journal |
| Native store's lifetime | Component/data-root owner, physical writer and bound storage generation | Independently reopening the same store because a provider advertisement changed |
| Repair obligation | Committed intent/version; existing durable repair queue for scheduling | A second queue/cursor that can advance independently or reconstruct an uncertain send as new |
| Completion delivery | Existing detached-send owner/inbox and original request | Wrapper success state inferred from enqueue/event emission |
| Snapshot freshness | Owning materialized generation and exact-prefix refresh | Private cursor/cache that misses installation and triggers another repair loop |

Do not delete state merely because it is derived. A restart cursor may avoid
expensive rescans; a receipt may be evidence that cannot be reconstructed. State
its purpose and how it remains subordinate to the owner. If two records claim
the same truth, choose the owner and correct the update/invalidation path before
adding a third record to keep them synchronized.

Consider a backend that installs a snapshot while a private requester retains
its old cursor. Connect installation observation to exact-prefix refresh and
retire stale cached state. This does not prove every cache is unnecessary; it
identifies the coherence edge to inspect before adding another repair loop.

A process-wide `Notify` using `notify_one()` can also let one embedded runtime
root consume another root's wake. Scope that wake to the existing per-root host
instead of creating a second scheduler. Test real roots with interleaved wakes,
not a new test-only scheduling implementation.

## 5. Prefer a removed wait edge to another lock

Write `A owns X → waits for B; B needs X` with actual operations and resources.
Include queued exclusive writers: they can block readers before holding X.
For each edge try these existing-path corrections before designing new state:

- Release admission before a remote operation, retaining the existing durable
  continuation and revalidating only what can change across the release.
- Let exact replica ingest consume its existing shard/store proof rather than
  join unrelated writer-configuration rebuild.
- Route required work through normal transport instead of applying an optional
  maintenance exclusion.
- Scope a wake/lifetime to its existing owner rather than add a global registry.
- Reuse validation while the same exact source cut and physical custody remain
  held; revalidate after either changes. A cached Boolean is not custody.

If a new durable fact is genuinely needed, show the crash/lost-reply case that
the old state cannot represent, give it one owner, and enumerate every writer,
reader, invalidation and recovery edge. Count added authority sources, persistent
records, independent schedulers and serial remote waits as well as LOC. This
reveals a “small” guard that creates a large lifecycle obligation.

After an unsuccessful patch, examine its new dependencies first. Use the prior
source and the same representative workload where practical; remove or revise
the failed hypothesis instead of layering another workaround on it. Preserve
unrelated work and historical evidence. Do not broadly revert someone else's
changes or discard accepted operation state to obtain a clean baseline.

## 6. Close the simplification against behavior, not implementation agreement

A reviewable repair names the old owner defect, the retained requirement, the
exact add/remove/reuse list and a causal control. Its validation includes both
the previously successful degraded case and rejection of an unsafe competing
history. New tests that only construct the new state and exercise its new reader
do not demonstrate that the state was necessary.

Check that removed mechanisms no longer have callers/routes/maintenance entries
and that one canonical path handles direct/routed, retry and restart variants.
When a change must grow beyond the debug task, present the proven reason and
smallest design option while continuing independent work. Do not turn that into
a routine permission question for every edit, or implement speculative extension
points “for later” in an unreleased subsystem.
