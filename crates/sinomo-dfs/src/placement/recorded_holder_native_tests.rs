//! Native recorded-placement replay and retained scheduling controls.
use super::*;
use crate::test_runtime::DfsTestRuntime;
use crate::*;
use sinomo_core::DfsObjectPolicy;
use std::time::Instant;

const PATH: &str = "/recorded-holder-native.bin";
const BYTES: &[u8] = b"real native immutable placement holder content";
type Service = NetworkedDfsService<DfsNodeTransport>;

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
    // PATH is a root-shard object, whose primary is the filesystem owner.
    reconciliation_placement_for_object_with_resolved_policy(
        job,
        object,
        &[],
        resolved,
        &job.accepted_config.owner_node_id,
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
                .is_some()
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

#[test]
fn planning_a_page_read_before_the_same_jobs_policy_update_reuses_its_committed_holders() {
    let (native, mut client, _, before) = base("placement-page-before-policy-update");
    let primary = client.node_id().clone();
    let policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    let job = native
        .apply_placement_policy(&primary, PATH, DfsPlacementScope::Subtree, policy)
        .unwrap();
    let (resolved, replication, placement) = plan(&job, &before);
    client.refresh_control_plane_latest().unwrap();
    client
        .route_namespace_operation(
            primary.clone(),
            DfsNamespaceOperation::ReconcilePlacement {
                job_id: job.job_id.clone(),
                path: before.path.clone(),
                object_id: before.object_id.clone(),
                expected_version: before.version.clone(),
                object_policy: resolved.policy.consistency,
                replication_policy: replication,
                desired_replica_node_ids: placement.plan.desired_replica_nodes.clone(),
            },
        )
        .unwrap();
    let record = actual_record(&native, &primary, &job, &before);
    let captured = record.object_metadata.as_ref().unwrap();
    assert_eq!(captured.version, before.version);
    assert_ne!(captured.policy, before.policy);
    drop(client);

    let owner = native.metadata_primary_service(PATH).unwrap();
    let (_, _, replay) = owner
        .reconciliation_placement_for_object_with_pins(
            &job.accepted_config,
            &native.metadata_shards().unwrap(),
            &job,
            &before,
            &[],
        )
        .unwrap();
    assert_eq!(
        replay.plan.desired_replica_nodes,
        placement.plan.desired_replica_nodes
    );
    drop(owner);
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
        recorded_placement_ranking_object(&object, &record.intent_id, &record, &object.policy)
            .unwrap(),
        object
    );
    let mut rejected = Vec::new();
    for alteration in 0..5 {
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
            4 => bad.object_metadata.as_mut().unwrap().policy = DfsObjectPolicy::LocalOwned,
            _ => unreachable!(),
        }
        rejected.push(
            recorded_placement_ranking_object(&object, &record.intent_id, &bad, &object.policy)
                .is_err(),
        );
    }
    let pin = nodes
        .iter()
        .find(|node| !record.desired_replica_node_ids.contains(node))
        .unwrap();
    let (_, _, pinned) = service
        .reconciliation_placement_for_object_with_pins(
            &job.accepted_config,
            &native.metadata_shards().unwrap(),
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
