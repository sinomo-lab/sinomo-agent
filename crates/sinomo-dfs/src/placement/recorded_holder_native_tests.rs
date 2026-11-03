//! Native recorded-placement replay and retained scheduling controls.
use super::*;
use crate::service_write::{DfsDistributedWriteOptions, DfsWriteDispatch, DfsWriteRepair};
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcPhase};
use crate::*;
use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};
use std::sync::Arc;
use std::time::Instant;

const PATH: &str = "/recorded-holder-native.bin";
const BYTES: &[u8] = b"real native immutable placement holder content";
type Service = NetworkedDfsService<DfsNodeTransport>;

struct GateCleanup(Arc<tokio::sync::Semaphore>);
impl Drop for GateCleanup {
    fn drop(&mut self) {
        self.0.close();
    }
}

fn evidence(native: &DfsTestRuntime, node: &NodeId, label: &str, value: serde_json::Value) {
    let count = native.checkpoint_rpc_observation_window().unwrap();
    if count > 0 {
        native
            .export_latest_rpc_observation_checkpoint(
                &native
                    .data_dir(node)
                    .root()
                    .join(format!("{label}-rpc.json")),
            )
            .unwrap();
    }
    std::fs::write(
        native.data_dir(node).root().join(format!("{label}.json")),
        serde_json::to_vec_pretty(&serde_json::json!({
            "checkpointed_rpc_records":count,"facts":value
        }))
        .unwrap(),
    )
    .unwrap();
}

fn base(name: &str) -> (DfsTestRuntime, Service, Vec<NodeId>, DfsObjectMetadata) {
    let (native, mut service, root) =
        crate::test_support::current_native_metadata_service(name, 3, PATH).unwrap();
    let nodes = DfsTestRuntime::ordered_node_ids(name, 3);
    assert_eq!(service.node_id(), &root.primary_node_id);
    let stat = service
        .posix_write_file(
            PATH,
            BYTES,
            DfsWriteOptions::replicated_eventual(vec![root.primary_node_id.clone()]),
        )
        .unwrap();
    let object = service
        .current_read_object_metadata_distributed(&stat.object_id)
        .unwrap();
    assert_eq!(object.current_locations, vec![root.primary_node_id]);
    (native, service, nodes, object)
}

fn proposal(
    native: &DfsTestRuntime,
    service: &mut Service,
) -> (DfsFileSystemConfig, DfsPlacementReconciliationJob) {
    let expected = service.current_placement_file_system_config().unwrap();
    let candidates = service.placement_content_candidates().unwrap();
    assert_eq!(candidates.len(), 3);
    let observer = native.client(&expected.owner_node_id).unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let snapshot = loop {
        if let Some(snapshot) = observer.placement_snapshot_cache.snapshot()
            && snapshot.primary_node_id == expected.owner_node_id
            && !snapshot.nodes.is_empty()
            && time::OffsetDateTime::now_utc() <= snapshot.expires_at
        {
            break (*snapshot).clone();
        }
        assert!(
            Instant::now() < deadline,
            "actual native snapshot is not ready"
        );
        std::thread::yield_now();
    };
    drop(observer);
    let desired = placement_config_with_override(
        &expected,
        PATH,
        DfsPlacementScope::SelfOnly,
        Some(DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap()),
    )
    .unwrap();
    let job = crate::control_plane::build_placement_reconciliation_job(
        sinomo_core::Id::generate().to_string(),
        &desired,
        PATH.into(),
        DfsPlacementScope::SelfOnly,
        BTreeSet::new(),
        candidates,
        Some(snapshot),
        dfs_now_unix_nanos(),
    )
    .unwrap();
    (expected, job)
}

fn plan(
    job: &DfsPlacementReconciliationJob,
    object: &DfsObjectMetadata,
) -> (
    DfsResolvedPlacementPolicy,
    DfsReplicationPolicy,
    DfsReplicaPlacementExplanation,
) {
    let resolved = resolve_placement_policy(
        &job.accepted_config,
        &object.path,
        object.policy.clone(),
        job.candidate_snapshot.len(),
    )
    .unwrap();
    reconciliation_placement_for_object_with_resolved_policy(
        &job.accepted_config,
        job,
        object,
        &[],
        resolved,
    )
    .unwrap()
}

fn accept(
    native: &DfsTestRuntime,
    expected: &DfsFileSystemConfig,
    job: &DfsPlacementReconciliationJob,
) {
    let desired = placement_config_with_override(
        expected,
        PATH,
        DfsPlacementScope::SelfOnly,
        Some(DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap()),
    )
    .unwrap();
    native.shared_control_plane().unwrap().with_client(|kv| {
        assert!(
            DfsControlPlaneStore::new(kv.as_mut())
                .compare_and_put_placement_update(expected, &desired, job)
                .unwrap()
        );
    });
}

fn reconcile(
    service: &mut Service,
    primary: &NodeId,
    job: &DfsPlacementReconciliationJob,
    object: &DfsObjectMetadata,
    replication: DfsReplicationPolicy,
    targets: Vec<NodeId>,
) {
    service.refresh_control_plane_latest().unwrap();
    service
        .route_namespace_operation(
            primary.clone(),
            DfsNamespaceOperation::ReconcilePlacement {
                job_id: job.job_id.clone(),
                path: object.path.clone(),
                object_id: object.object_id.clone(),
                expected_version: object.version.clone(),
                object_policy: object.policy.clone(),
                replication_policy: replication,
                desired_replica_node_ids: targets,
            },
        )
        .unwrap();
}

/// The placement owner recorded in `node`'s durable metadata store.
fn actual_record(
    native: &DfsTestRuntime,
    node: &NodeId,
    job: &DfsPlacementReconciliationJob,
    object: &DfsObjectMetadata,
) -> DfsWriteIntentRecord {
    native
        .node_metadata(node)
        .unwrap()
        .get_intent(
            &object.object_id,
            &format!(
                "placement:{}:{}:{}",
                job.job_id, object.object_id, object.version
            ),
        )
        .unwrap()
        .unwrap()
}

fn pending_case(
    name: &str,
) -> (
    DfsTestRuntime,
    Service,
    Vec<NodeId>,
    DfsObjectMetadata,
    DfsPlacementReconciliationJob,
    DfsWriteIntentRecord,
    Box<dyn Send>,
) {
    let (native, mut service, nodes, object) = base(name);
    let primary = service.node_id().clone();
    let hold: Box<dyn Send> = Box::new(
        native
            .deny_background_replica_before_claim(&primary)
            .unwrap(),
    );
    let (expected, job) = proposal(&native, &mut service);
    let (_, replication, placement) = plan(&job, &object);
    accept(&native, &expected, &job);
    reconcile(
        &mut service,
        &primary,
        &job,
        &object,
        replication,
        placement.plan.desired_replica_nodes,
    );
    let record = actual_record(&native, &primary, &job, &object);
    assert_eq!(record.status, DfsWriteIntentStatus::Committed);
    assert_eq!(
        record.object_metadata.as_ref().unwrap().current_locations,
        vec![primary]
    );
    (native, service, nodes, object, job, record, hold)
}

fn finish_missing(
    native: DfsTestRuntime,
    primary: &NodeId,
    record: &DfsWriteIntentRecord,
) -> (DfsTestRuntime, DfsRepairQueueView) {
    let primary = primary.clone();
    let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
        native.data_dir(&primary),
    ));
    // The runtime moves into the maintenance worker below; observe the
    // primary's durable store through the same inspection view it exposes.
    let metadata_root = crate::default_dfs_metadata_shards_root(native.data_dir(&primary));
    let shards = native.metadata_shards().unwrap();
    let primary_store = || {
        crate::local_state::open_local_sharded_metadata_backend_for_inspection_at_node(
            metadata_root.clone(),
            shards.clone(),
            &primary,
        )
        .unwrap()
    };
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let tick_primary = primary.clone();
    let worker_queue = queue.clone();
    let intent_id = record.intent_id.clone();
    let reached = std::sync::atomic::AtomicBool::new(false);
    let (native, outcomes, observed) = std::thread::scope(|scope| {
        let reached = &reached;
        let worker = scope.spawn(move || {
            // A clean-MAX Set invalidates the existing observed page. Its next
            // normal pass consumes the new token. The pass that dispatches the
            // missing target returns before its reply lands, and a later pass
            // closes that completed custody; automatic passes may carry any of
            // these steps. Keep running ordinary passes until the goal lands,
            // letting each in-flight transfer settle first. No call supplies
            // DATA.
            let mut outcomes = Vec::new();
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() || reached.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }
                outcomes.push(native.maintenance_tick(&tick_primary, remaining));
                while !reached.load(std::sync::atomic::Ordering::Acquire)
                    && Instant::now() < deadline
                    && worker_queue.view().is_ok_and(|view| {
                        view.records
                            .iter()
                            .any(|row| row.intent_id == intent_id && row.in_flight)
                    })
                {
                    std::thread::yield_now();
                }
            }
            (native, outcomes)
        });
        let observed = loop {
            let receipts = primary_store()
                .receipts_for_version(&record.object_id, &record.target_version)
                .unwrap();
            let cut = queue.view().unwrap();
            let goal = record.desired_replica_node_ids.iter().all(|target| {
                receipts.iter().any(|receipt| {
                    &receipt.replica_node_id == target
                        && receipt.content_hash == record.content_hash
                })
            }) && !cut
                .records
                .iter()
                .any(|row| row.intent_id == record.intent_id);
            let now = Instant::now();
            if goal || now >= deadline {
                reached.store(true, std::sync::atomic::Ordering::Release);
                break (goal && now < deadline, receipts, cut);
            }
            std::thread::yield_now();
        };
        let (native, outcomes) = worker.join().unwrap();
        (native, outcomes, observed)
    });
    evidence(
        &native,
        &primary,
        "recorded-holder-finish",
        serde_json::json!({
            "observed_in_budget":observed.0,"receipts":observed.1,
            "queue_head":observed.2.head_sequence,"queue":observed.2.records,
            "ordinary_ticks":format!("{outcomes:#?}"),"original":record,
            "qualification":"actual native receipt and queue goal; native ordinary ticks retain full outcomes independently"
        }),
    );
    assert!(
        observed.0,
        "the actual native original must reach its recorded desired replicas"
    );
    (native, observed.2)
}

fn closed_original(
    native: &mut DfsTestRuntime,
    primary: &NodeId,
    record: &DfsWriteIntentRecord,
    target: &NodeId,
    cut: &DfsRepairQueueView,
) {
    let path = default_dfs_repair_attempt_snapshot_path(native.data_dir(primary));
    native.stop_node(primary).unwrap();
    let history = crate::repair_queue_store::tests::native_queue_history(&path);
    let originals = history
        .iter()
        .flat_map(|(_, ops)| ops)
        .filter_map(|operation| {
            let crate::repair_queue_store::DfsRepairQueueOperation::RepairCustody { change } =
                operation
            else {
                return None;
            };
            let crate::repair_queue_store::RepairCustodyChange::Enroll { birth } = change.as_ref()
            else {
                return None;
            };
            let DfsNetworkMessage::RepairReplica(command) = &birth.original.message else {
                return None;
            };
            (command.intent_id == record.intent_id
                && command.object_id == record.object_id
                && command.version == record.target_version
                && Some(&command.content_hash) == record.content_hash.as_ref()
                && &birth.original.source_node_id == primary
                && &birth.original.target_node_id == target)
                .then(|| birth.as_ref().clone())
        })
        .collect::<Vec<_>>();
    let queue = LocalDfsRepairQueueStore::new(&path);
    let valid = originals.iter().all(|birth| {
        let closed_in_cut = history.iter().any(|(seq, operations)| *seq <= cut.head_sequence
            && operations.iter().any(|operation| matches!(operation,
                crate::repair_queue_store::DfsRepairQueueOperation::RepairCustody { change }
                if matches!(change.as_ref(), crate::repair_queue_store::RepairCustodyChange::Close { subject, .. }
                    if subject == &birth.root.subject))));
        closed_in_cut && queue.closed_repair_birth(&birth.root.subject).unwrap().is_some()
    });
    evidence(
        native,
        primary,
        "recorded-holder-closed",
        serde_json::json!({
            "history":history,"actual_originals":originals,"all_closed_before_cut":valid,"cut":cut.head_sequence
        }),
    );
    assert_eq!(
        originals.len(),
        1,
        "one actual immutable missing-target Repair Original"
    );
    assert!(valid);
}

#[test]
fn native_recorded_holder_cut_survives_real_late_ordinary_repair() {
    let (native, mut service, nodes, initial) = base("native-recorded-holder-growth");
    let primary = service.node_id().clone();
    let operation = format!("holder-growth-{}", sinomo_core::Id::generate());
    let version =
        crate::intent::stable_write_target_version(&primary, &initial.object_id, &operation);
    let (expected, job) = proposal(&native, &mut service);
    // This is a prospective pure preview, not a row installed in native DATA.
    // The normal stable-write producer must return this exact version below.
    let mut prospective = initial.clone();
    prospective.version = version.clone();
    let (_, replication, placement) = plan(&job, &prospective);
    let targets = placement.plan.desired_replica_nodes;
    assert_eq!(targets.len(), 2);
    assert!(targets.contains(&primary));
    let target = targets
        .iter()
        .find(|node| **node != primary)
        .unwrap()
        .clone();
    let extra = nodes
        .iter()
        .find(|node| !targets.contains(node))
        .unwrap()
        .clone();
    assert!(
        placement
            .decisions
            .iter()
            .any(|decision| decision.node_id == extra && decision.eligible)
    );
    let managed_id = format!("placement:{}:{}:{}", job.job_id, initial.object_id, version);
    let intent_digest = *blake3::hash(managed_id.as_bytes()).as_bytes();
    let target_digest = *blake3::hash(target.as_str().as_bytes()).as_bytes();
    let hold_target = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
        native.data_dir(&primary),
        Arc::new(move |point, _, _| {
            if let crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeReplicaTransferClaim {
                intent_id_digest: actual_intent,
                target_id_digest: actual_target,
                ..
            } = point
                && actual_intent == intent_digest
                && actual_target == target_digest
            {
                return Err(DfsError::Cancelled {
                    operation: "hold real managed missing-target claim",
                });
            }
            Ok(())
        }),
    );
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    native.install_fault(FaultRule {
        source: Some(primary.clone()),
        target: Some(extra.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 1,
        action: FaultAction::PauseAfterDispatch(Arc::clone(&gate)),
    });
    let mut writer = native.client(&primary).unwrap();
    // Use the normal manifest producer with real target-version bytes at A.
    // No metadata or receipt is inserted to make A count as a durable holder.
    let request = DfsWriteRequest {
        file_system_id: initial.file_system_id.clone(),
        object_id: initial.object_id.clone(),
        target_version: version.clone(),
        requesting_node_id: primary.clone(),
    };
    // Candidate lists are compatibility hints; the actual primary replans
    // them. Constrain only this OLD ordinary write's desired placement so C
    // is genuinely selected. The later managed job remains entirely unpinned.
    let mut original_policy = DfsReplicationPolicy::new(1, 2).unwrap();
    original_policy.required_node_ids = vec![extra.clone()];
    let original_candidates = vec![primary.clone(), extra.clone()];
    let admitted_candidates = writer.placement_content_candidates().unwrap();
    let admitted_plan = writer
        .plan_distributed_eventual_replicas(
            &request,
            original_candidates.clone(),
            original_policy.clone(),
        )
        .unwrap();
    evidence(
        &native,
        &primary,
        "recorded-holder-source-plan",
        serde_json::json!({
            "old_ordinary_policy":original_policy,"actual_admitted_plan":admitted_plan,
            "actual_candidates":admitted_candidates,"managed_job":job,
            "managed_target":target,"extra_target":extra,
            "qualification":"actual producer planning against its normal native projection; required C belongs only to the old ordinary write, while the accepted managed proposal has no pins or required nodes"
        }),
    );
    assert!(
        admitted_candidates
            .iter()
            .any(|candidate| candidate.node_id == extra)
    );
    assert_eq!(
        admitted_plan
            .desired_replica_nodes
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([primary.clone(), extra.clone()])
    );
    let manifest = writer
        .content()
        .put_object_version(
            request.object_id.as_str(),
            request.target_version.as_str(),
            BYTES,
        )
        .unwrap();
    let original_base = initial.version.clone();
    let worker_outcome = Arc::new(std::sync::Mutex::new(None));
    let captured_outcome = Arc::clone(&worker_outcome);
    let scheduler =
        crate::runtime_host::daemon_dfs_mutation_scheduler(native.data_dir(&primary)).unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let (record, grown, write_result) = std::thread::scope(|scope| {
        let _cleanup = GateCleanup(Arc::clone(&gate));
        let worker = scope.spawn(move || {
            let result = writer.write_distributed_manifest_with_candidates(
                request,
                Some(original_base),
                manifest.content_hash,
                manifest.size_bytes,
                original_candidates,
                original_policy,
                DfsDistributedWriteOptions {
                    publication: DfsWritePublication::Current,
                    repair: DfsWriteRepair::Background,
                    dispatch: DfsWriteDispatch::AwaitResponse,
                    operation_id: Some(operation),
                    ..Default::default()
                },
            );
            *captured_outcome.lock().unwrap() = Some(format!("{result:#?}"));
            result
        });
        let reached_receiver = loop {
            let reached = native.rpc_observations().iter().any(|call| {
                call.source == primary
                    && call.target == extra
                    && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                    && call.phase == RpcPhase::AfterDispatch
            });
            if reached || Instant::now() >= deadline {
                break reached;
            }
            std::thread::yield_now();
        };
        let actual_before = service.current_read_object_metadata_distributed(&initial.object_id);
        let actual_history = native
            .node_metadata(&primary)
            .unwrap()
            .intents_for_object(&initial.object_id);
        evidence(
            &native,
            &primary,
            "recorded-holder-admitted-extra",
            serde_json::json!({
                "receiver_reached":reached_receiver,
                "managed_target":target,"extra_target":extra,
                "actual_worker_outcome":*worker_outcome.lock().unwrap(),
                "actual_before":format!("{actual_before:#?}"),
                "actual_intent_history":format!("{actual_history:#?}"),
                "source_global_held":scheduler.global_is_exclusively_held(),"prospective":prospective,"job":job
            }),
        );
        assert!(
            reached_receiver,
            "actual late ordinary repair did not reach receiver completion"
        );
        let ordinary = actual_history
            .unwrap()
            .into_iter()
            .filter(|record| {
                record.target_version == version && record.status == DfsWriteIntentStatus::Committed
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ordinary.len(),
            1,
            "the actual ordinary source has one committed original"
        );
        assert_eq!(ordinary[0].policy.min_commit_replicas, 1);
        assert_eq!(ordinary[0].policy.desired_replicas, 2);
        assert_eq!(ordinary[0].policy.required_node_ids, vec![extra.clone()]);
        assert_eq!(
            ordinary[0]
                .desired_replica_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([primary.clone(), extra.clone()])
        );
        // Main serves the committed repair promptly, so a maintenance pass,
        // possibly the one carrying this late repair, can still be finishing
        // its GLOBAL turn when the receiver completes. Wait for that turn to
        // end while the gate still holds the reply: a source that kept GLOBAL
        // across its reply hold never releases it before the gate opens below.
        let global_released = loop {
            if !scheduler.global_is_exclusively_held() {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            std::thread::yield_now();
        };
        let reply_held = !native.rpc_observations().iter().any(|call| {
            call.source == primary
                && call.target == extra
                && call.route == DFS_DATA_REPAIR_REPLICA_ROUTE
                && matches!(
                    call.phase,
                    RpcPhase::Completed | RpcPhase::EndedWithoutResponse
                )
        });
        assert!(
            global_released && reply_held,
            "normal source Main must release GLOBAL before reply hold"
        );
        let before = actual_before.unwrap();
        assert_eq!(before.version, version);
        assert_eq!(before.current_locations, vec![primary.clone()]);
        let receiver = native.client(&extra).unwrap();
        assert_eq!(
            receiver
                .content()
                .read_object_version(initial.object_id.as_str(), version.as_str())
                .unwrap()
                .1,
            BYTES
        );
        drop(receiver);
        accept(&native, &expected, &job);
        reconcile(
            &mut service,
            &primary,
            &job,
            &before,
            replication.clone(),
            targets.clone(),
        );
        let record = actual_record(&native, &primary, &job, &before);
        assert_eq!(record.desired_replica_node_ids, targets);
        assert_eq!(
            record.object_metadata.as_ref().unwrap().current_locations,
            vec![primary.clone()]
        );
        gate.add_permits(1);
        let grown = loop {
            let object = service
                .current_read_object_metadata_distributed(&initial.object_id)
                .unwrap();
            if object.current_locations.contains(&extra) {
                break object;
            }
            assert!(
                Instant::now() < deadline,
                "real delayed reply did not publish its verified same-version holder"
            );
            std::thread::yield_now();
        };
        let write_result = worker.join().unwrap();
        (record, grown, write_result)
    });
    write_result.unwrap();
    assert_eq!(grown.version, version);
    assert_eq!(grown.content_hash, initial.content_hash);
    let (_, _, drift) = plan(&job, &grown);
    evidence(
        &native,
        &primary,
        "recorded-holder-growth",
        serde_json::json!({
            "original":record,"grown":grown,"live_holder_replan":drift,"accepted_job":job
        }),
    );
    assert_ne!(
        drift.plan.desired_replica_nodes, targets,
        "the real holder growth must discriminate the old reranking bug"
    );
    reconcile(
        &mut service,
        &primary,
        &job,
        &grown,
        replication,
        targets.clone(),
    );
    assert_eq!(actual_record(&native, &primary, &job, &grown), record);
    drop(hold_target);
    native.clear_faults();
    let (mut native, cut) = finish_missing(native, &primary, &record);
    let receiver = native.client(&target).unwrap();
    assert_eq!(
        receiver
            .content()
            .read_object_version(initial.object_id.as_str(), version.as_str())
            .unwrap()
            .1,
        BYTES
    );
    assert_eq!(actual_record(&native, &primary, &job, &grown), record);
    drop(receiver);
    drop(service);
    closed_original(&mut native, &primary, &record, &target, &cut);
    native.close().unwrap();
}

#[test]
fn native_clean_max_rearms_the_same_real_missing_target() {
    let (native, service, _, object, job, record, hold) = pending_case("native-recorded-clean-max");
    let primary = service.node_id().clone();
    // Restoring and parking committed repair custody is Main-side work on the
    // store that owns the original; the delegating caller keeps no store.
    drop(service);
    let mut service = native.metadata_primary_service(PATH).unwrap();
    assert_eq!(service.node_id(), &primary);
    let path = default_dfs_repair_attempt_snapshot_path(native.data_dir(&primary));
    let queue = LocalDfsRepairQueueStore::new(&path);
    let (before, observation) = queue.view_for_maintenance().unwrap();
    let row = before
        .records
        .iter()
        .find(|row| row.intent_id == record.intent_id)
        .unwrap()
        .clone();
    assert_eq!(row.attempts, 0);
    assert!(row.transfer_request_id.is_none() && !row.in_flight);
    service
        .restore_pending_write_for_intent_if_missing(&record.intent_id, &object.object_id)
        .unwrap();
    service.restore_repair_attempt_records_merging(std::slice::from_ref(&row));
    service.park_committed_repairs(&BTreeSet::from([record.intent_id.clone()]));
    queue
        .reconcile_observed(&observation, &service.durable_repair_queue_records())
        .unwrap();
    let parked = queue.view().unwrap();
    let parked_row = parked
        .records
        .iter()
        .find(|row| row.intent_id == record.intent_id)
        .unwrap();
    assert!(crate::service_repair::unstarted_parked_placement_repair(
        parked_row
    ));
    evidence(
        &native,
        &primary,
        "recorded-clean-max-before",
        serde_json::json!({
            "actual_before":row,"actual_parked":parked_row,"owner":record,
            "qualification":"production parking helper and ordinary observed queue writer on a real native birth; retained-scheduling component control, not a re-created old ranking-bug producer"
        }),
    );
    drop(hold);
    let (mut native, cut) = finish_missing(native, &primary, &record);
    assert_eq!(actual_record(&native, &primary, &job, &object), record);
    let target = row.missing_replica_node_id;
    let receiver = native.client(&target).unwrap();
    assert_eq!(
        receiver
            .content()
            .read_object_version(object.object_id.as_str(), object.version.as_str())
            .unwrap()
            .1,
        BYTES
    );
    drop(receiver);
    drop(service);
    closed_original(&mut native, &primary, &record, &target, &cut);
    native.close().unwrap();
}

#[test]
fn native_capture_rejects_missing_or_inconsistent_original_data() {
    let (native, service, nodes, object, job, record, hold) =
        pending_case("native-recorded-capture-negative");
    let primary = service.node_id().clone();
    // Placement planning reads the recorded original from the store that
    // owns it; the delegating caller keeps no store.
    drop(service);
    let service = native.metadata_primary_service(PATH).unwrap();
    assert_eq!(service.node_id(), &primary);
    let before = native
        .node_metadata(&primary)
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    assert_eq!(
        recorded_placement_ranking_object(&object, &record.intent_id, &record).unwrap(),
        object
    );
    let mut rejected = Vec::new();
    for alteration in 0..4 {
        let mut bad = record.clone();
        match alteration {
            0 => bad.object_metadata = None,
            1 => bad.object_metadata.as_mut().unwrap().version = DfsVersion::generate(),
            2 => {
                bad.object_metadata.as_mut().unwrap().content_hash =
                    Some("blake3:wrong-negative-input".into())
            }
            3 => bad
                .object_metadata
                .as_mut()
                .unwrap()
                .path
                .push_str("-different"),
            _ => unreachable!(),
        }
        rejected.push(recorded_placement_ranking_object(&object, &record.intent_id, &bad).is_err());
    }
    let pin = nodes
        .iter()
        .find(|node| !record.desired_replica_node_ids.contains(node))
        .unwrap();
    let (_, _, pinned) = service
        .reconciliation_placement_for_object_with_pins(
            &job.accepted_config,
            &job,
            &object,
            std::slice::from_ref(pin),
        )
        .unwrap();
    evidence(
        &native,
        &primary,
        "recorded-capture-negative",
        serde_json::json!({
            "actual_original":record,"all_negative_clones_rejected":rejected,"counterfactual_pin_plan":pinned,
            "qualification":"missing/inconsistent descriptors and pin argument are explicitly negative DATA cloned from actual native inputs; no forged row or authoritative pin is persisted"
        }),
    );
    assert!(rejected.iter().all(|rejected| *rejected));
    assert!(pinned.plan.desired_replica_nodes.contains(pin));
    assert_ne!(
        pinned.plan.desired_replica_nodes,
        record.desired_replica_node_ids
    );
    assert_eq!(
        native
            .node_metadata(&primary)
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
    assert_eq!(actual_record(&native, &primary, &job, &object), record);
    drop(hold);
    drop(service);
    native.close().unwrap();
}

#[test]
fn native_stale_policy_and_uncertain_scheduling_remain_fenced() {
    let (native, service, _, object, job, record, hold) =
        pending_case("native-recorded-stale-negative");
    let primary = service.node_id().clone();
    // Restoring and parking committed repair custody and the authority check
    // are Main-side work on the owning store.
    drop(service);
    let mut service = native.metadata_primary_service(PATH).unwrap();
    assert_eq!(service.node_id(), &primary);
    let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
        native.data_dir(&primary),
    ));
    let row = queue
        .view()
        .unwrap()
        .records
        .into_iter()
        .find(|row| row.intent_id == record.intent_id)
        .unwrap();
    service
        .restore_pending_write_for_intent_if_missing(&record.intent_id, &object.object_id)
        .unwrap();
    service.restore_repair_attempt_records_merging(std::slice::from_ref(&row));
    service.park_committed_repairs(&BTreeSet::from([record.intent_id.clone()]));
    let clean = service
        .durable_repair_queue_records()
        .into_iter()
        .find(|row| row.intent_id == record.intent_id)
        .unwrap();
    assert!(crate::service_repair::unstarted_parked_placement_repair(
        &clean
    ));
    let mut negatives = Vec::new();
    for alteration in 0..4 {
        let mut bad = clean.clone();
        match alteration {
            0 => bad.transfer_request_id = Some("negative-named-original".into()),
            1 => bad.transfer_delivery_state = None,
            2 => bad.in_flight = true,
            3 => {
                bad.attempts = 1;
                bad.transfer_delivery_state =
                    Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied);
            }
            _ => unreachable!(),
        }
        negatives.push(!crate::service_repair::unstarted_parked_placement_repair(
            &bad,
        ));
    }
    let changed = native
        .apply_placement_policy(
            &primary,
            PATH,
            DfsPlacementScope::SelfOnly,
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap(),
        )
        .unwrap();
    let authoritative = service
        .placement_intent_remains_authoritative(&record.intent_id)
        .unwrap();
    evidence(
        &native,
        &primary,
        "recorded-stale-negative",
        serde_json::json!({
            "actual_original":record,"actual_new_job":changed,"old_authoritative":authoritative,
            "all_uncertain_clones_rejected":negatives,"actual_queue":queue.view().unwrap().records,
            "qualification":"real current policy Apply fences old job; named/unknown/in-flight clones are explicitly assembled negative selector inputs, never native evidence or persisted custody"
        }),
    );
    assert!(!authoritative);
    assert!(negatives.iter().all(|rejected| *rejected));
    assert_eq!(actual_record(&native, &primary, &job, &object), record);
    drop(hold);
    drop(service);
    native.close().unwrap();
}

#[path = "recorded_holder_native_tests/retained_owner.rs"]
mod retained_owner;
