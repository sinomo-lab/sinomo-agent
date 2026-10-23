# DFS implementation and diagnostic entry points

Resolve symbols against the working source with CodeGraph, then use exact `rg`
for tests and literals. These paths describe the
implementation, not current deployed behavior.

They also do not override the original W/N and outage requirement. In particular,
the all-participant restriction below is a known implementation mismatch. Use
[mechanism review](mechanism-review.md) before turning existing checks into new
requirements, copying managed state or designing compatibility paths.

## Publication and exact recovery

```sh
codegraph explore 'authorize_prepared_publication_from_record authorize_metadata_shard_operation commit_prepared_operation_owned'
codegraph explore 'replayed_routed_write_outcome_if_known recover_prepared_write_for_retry'
```

| Boundary | Source | Inspect |
|---|---|---|
| Routed write/retry | [service_routed_write.rs](../../../../crates/sinomo-dfs/src/service_routed_write.rs) | `replayed_routed_write_outcome_if_known`, original requester/request/body and exact outcome; no new write identity on ambiguity |
| Write completion/recovery | [service_commit.rs](../../../../crates/sinomo-dfs/src/service_commit.rs) | `recover_prepared_write_for_retry`, recorded base/target, receipt policy, unchanged payload |
| Sealed payload and capability | [publication_guard.rs](../../../../crates/sinomo-dfs/src/publication_guard.rs) | `authorize_prepared_publication_from_record`, `DfsPublicationGuard`; actual writer, original predicates, source generation and freeze |
| Participant collection | [service_replica.rs](../../../../crates/sinomo-dfs/src/service_replica.rs) | `authorize_metadata_shard_operation`; required set is primary union configured replicas, not W or KV voters |
| Durable kernel | [prepared_operation.rs](../../../../crates/sinomo-dfs/src/metadata_backend/prepared_operation.rs) | `commit_prepared_operation_owned`; consumes guard, validates sealed effects, persists exact commit |
| Configuration coherence | [metadata_authority_runtime.rs](../../../../crates/sinomo-dfs/src/metadata_authority_runtime.rs), [write_configuration_projection.rs](../../../../crates/sinomo-dfs/src/write_configuration_projection.rs) | Filesystem/shard proof versus full writer configuration; physical storage generation versus observation generation |
| Physical writer | [metadata_lock.rs](../../../../crates/sinomo-dfs/src/metadata_lock.rs) | Lock acquisition/release, source fence and observational try-lock; an Arc/backend clone alone is not held authority |

V7 prepares retain complete bodies and original comparisons. Commit is digest
bound. Prepares are hidden but can reserve paths/objects; “overlaps active
prepare” can therefore be an unresolved previous operation. Inspect current-epoch
recovery before overlapping publication. Use the native control
`native_explicit_retry_resumes_ready_intent_after_overlapping_prepare` in
[authority tests](../../../../crates/sinomo-dfs/src/tests/authority.rs).

Do not infer terminal state from the presence of a PutIntent commit alone: that
can commit an intent without publishing its target content version. Inspect
qualifying receipts, target visibility and the exact original request outcome.

## Admission, ingress and responses

```sh
codegraph explore 'DfsMutationScheduler admit warm_daemon_dfs_replica_authority_for_peer_snapshot_before'
codegraph explore 'DfsRuntimeMessageCacheEntry replica_admission_handle DfsDetachedSendRuntime'
```

| Boundary | Source | Inspect |
|---|---|---|
| Scheduler/gates | [runtime_admission.rs](../../../../crates/sinomo-dfs/src/runtime_admission.rs) | Global/exact scopes, shard gates, waiter order, foreground preemption and cancellation |
| Host preflight/admission | [message_host.rs](../../../../crates/sinomo-dfs/src/runtime_host/message_host.rs) | Replica-specific proof versus general writer rebuild; admission held while awaiting refresh; renewal of preemption after Pending |
| Direct/routed data plane | [data_plane.rs](../../../../crates/sinomo-dfs/src/runtime_host/data_plane.rs) | Both carriers reaching the same exact replica path, authenticated source/target and dispatch ownership |
| Background initialization | [publish_host.rs](../../../../crates/sinomo-dfs/src/runtime_host/publish_host.rs) | Queued global refresh blocking ACKs on another shard; preemption before admission |
| Resident lanes/detached owner | [runtime_message.rs](../../../../crates/sinomo-dfs/src/runtime_message.rs) | `DfsRuntimeMessageCacheEntry`, `DfsDetachedSendRuntime`, committed effects, active worker reset, runtime shutdown/drain |
| Outbox and response policy | [node_transport.rs](../../../../crates/sinomo-dfs/src/node_transport.rs) | `DeferredDfsPeerSend::eligible_for_target_policy`, FetchBlob, completed-response identity, original delivery state |
| Metadata publication work | [metadata_replication.rs](../../../../crates/sinomo-dfs/src/metadata_replication.rs) | `has_required_replication_work`, exact suffix/snapshot work versus optional verification and advisory filters |

Useful native regressions (resolve full test names before `--exact`):

- [replica_progress_boundaries.rs](../../../../crates/sinomo-dfs/src/runtime_host/tests/replica_progress_boundaries.rs):
  `required_replica_wait_preempts_a_later_maintenance_turn_after_shard_release`,
  `cancelled_replica_wait_does_not_preempt_a_later_maintenance_turn`,
  `routed_authority_probe_does_not_wait_for_general_request_view`,
  `routed_authority_ingress_preserves_source_target_and_epoch_checks`,
  `required_blob_fetch_progresses_while_advisory_maintenance_is_parked`.
- [replica_storage_scope.rs](../../../../crates/sinomo-dfs/src/runtime_host/tests/predispatch_authority_retry/replica_storage_scope.rs):
  `current_replica_does_not_join_unrelated_writer_configuration_rebuild`,
  `new_replica_prepare_does_not_join_unrelated_writer_configuration_rebuild`.
- [repair.rs](../../../../crates/sinomo-dfs/src/tests/repair.rs):
  `native_completed_response_progresses_while_advisory_maintenance_is_parked`.
- In metadata replication: `native_required_journal_suffix_is_not_parked_by_an_execution_hint`
  and `native_required_journal_suffix_is_not_parked_by_stale_connectivity`.

These tests are examples of real ownership/schedule controls, not a substitute
for selecting affected targets or a claim that their old passes cover new code.

## Preparation, repair and handoff

| Concern | Entry points | Key distinction |
|---|---|---|
| Bounded preparation | [source_adoption/preparation.rs](../../../../crates/sinomo-dfs/src/source_adoption/preparation.rs), [source_adoption.rs](../../../../crates/sinomo-dfs/src/source_adoption.rs) | Pending with zero progress on contention versus lost slot/cursor; 256 is a batch boundary, not proof that startup is corrupt |
| Repair production/execution | [service_repair.rs](../../../../crates/sinomo-dfs/src/service_repair.rs), [repair_queue_store.rs](../../../../crates/sinomo-dfs/src/repair_queue_store.rs) | Committed intent is source; queue is scheduling state; producer capture is retained until its checkpoint is acknowledged |
| Maintenance resume | [maintenance_runtime.rs](../../../../crates/sinomo-dfs/src/runtime_host/maintenance_runtime.rs), [maintenance progress](../../../../docs/developer/dfs-maintenance-progress.md) | Cancellation retains bounded progress; returned phase error may need dependency phases again; incomplete never means reconciliation ready |
| Metadata handoff | [service_sync.rs](../../../../crates/sinomo-dfs/src/service_sync.rs), [DFS architecture](../../../../docs/dfs-architecture.md) | Proposed configuration, retiring freeze, exact prefix, activation and successor binding are distinct |
| Historical formats | [sinomo-dfs-migration](../../../../crates/sinomo-dfs-migration/src/lib.rs), [storage migrations](../../../../docs/developer/storage-migrations.md) | Current core is strict; decoder selection and old-format normalization belong in migration |

Inspect the actual repair-custody handoff API and tests before assuming pending
transfers are carried by a configuration change.
Do not reconstruct pending transfers from desired replica counts: original
request IDs, delivery uncertainty, attempts and retry ownership matter.

## Native fixture and observation tools

[DfsTestRuntime](../../../../crates/sinomo-dfs/src/test_runtime.rs) supplies shared
embedded production runtime and transport. Use its current methods rather than
per-test transport implementations:

| Need | Existing facility |
|---|---|
| Actual nodes/lifecycle | `start_cluster`, `start_single_node`, `metadata_primary_service`, `stop_node`, `reopen_node`, `close` |
| Configuration stall/gap | `pause_next_configuration_rebuild`, `lose_configuration_delta_and_pause_rebuild` |
| Controlled work boundary | `pause_replica_materialization`, `pause_deferred_response`, `pause_source_preparation` |
| Actual RPC evidence | `rpc_observations`, `rpc_observation_reader`, namespace captures; bounded captures expose omission information where supported |
| Control-plane cost | `metadata_primary_service_with_control_plane_probe`, `native_kv_request_hook` |

Inspect implementations before assuming `stop_node` simulates abrupt process
death; controlled shutdown and a process kill have different semantics. Close
the fixture through its lifecycle after work drains and export captures first.
Use the [validation recipes](../../sinomo-dfs-validation/references/recipes.md)
for executable discovery, archives and Docker cases.

Existing observation tools are `scripts/debug/capture-performance.py`,
`scripts/debug/summarize-network-timeline.py`, `docker/profile-dfs-writes.py` and
`docker/profile-idle.py`. Apply the [performance skill](../../../../skills/sinomo-performance-analysis/SKILL.md)
when reporting performance; include unavailable measurement/coverage explicitly.
