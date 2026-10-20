use super::*;
use crate::test_runtime::{DfsTestRuntime, RpcPhase};
use crate::{SharedDfsControlPlaneKvClient, sifs_file_system_id};

fn native_placement_job(
    config: DfsFileSystemConfig,
    path: &str,
    candidates: Vec<DfsNodeAdvertisement>,
    id: String,
) -> DfsPlacementReconciliationJob {
    DfsPlacementReconciliationJob {
        schema_version: DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
        job_id: id,
        file_system_id: sifs_file_system_id(),
        path: path.into(),
        scope: DfsPlacementScope::Subtree,
        unavailable_node_ids: BTreeSet::new(),
        candidate_snapshot: candidates,
        placement_snapshot: None,
        adaptive: false,
        adaptive_budget: None,
        processed_content_bytes: 0,
        accepted_config: config,
        state: DfsPlacementReconciliationState::Pending,
        cursor: None,
        total_object_count: 0,
        scanned_object_count: 0,
        matched_object_count: 0,
        updated_object_count: 0,
        scheduled_repair_count: 0,
        completed_repair_count: 0,
        pending_repair_count: 0,
        revision: 0,
        created_at_unix_nanos: dfs_now_unix_nanos(),
        updated_at_unix_nanos: dfs_now_unix_nanos(),
        last_error: None,
    }
}

struct NativePlacementFixture {
    runtime: DfsTestRuntime,
    service: NetworkedDfsService<crate::DfsNodeTransport>,
    control: SharedDfsControlPlaneKvClient<Box<dyn DfsControlPlaneKvClient>>,
    objects: Vec<DfsObjectMetadata>,
    job: DfsPlacementReconciliationJob,
    primary: NodeId,
}
impl NativePlacementFixture {
    fn new(label: &str, remote: bool, count: usize) -> Self {
        Self::with_nodes(label, remote, count, if remote { 2 } else { 1 })
    }
    fn with_nodes(label: &str, remote: bool, count: usize, total: usize) -> Self {
        Self::with_nodes_and_initial_job(label, remote, count, total, true)
    }
    fn with_nodes_and_initial_job(
        label: &str,
        remote: bool,
        count: usize,
        total: usize,
        publish_initial_job: bool,
    ) -> Self {
        let nodes = DfsTestRuntime::ordered_node_ids(label, total);
        let root_primary = nodes[0].clone();
        let mut runtime = DfsTestRuntime::new(label, root_primary.clone()).unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let primary = if remote {
            nodes[1].clone()
        } else {
            root_primary.clone()
        };
        if remote {
            runtime
                .reassign_metadata_shard(
                    &sifs_file_system_id(),
                    "services",
                    primary.clone(),
                    vec![root_primary.clone()],
                )
                .unwrap();
        }
        let control = runtime.shared_control_plane().unwrap();
        let config = control.with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut())
                .get_file_system_config(&sifs_file_system_id())
                .unwrap()
                .unwrap()
        });
        let path = "/services/native-placement";
        let mut policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
        policy.required_node_ids = vec![primary.clone()];
        let desired =
            placement_config_with_override(&config, path, DfsPlacementScope::Subtree, Some(policy))
                .unwrap();
        assert!(control.with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut())
                .compare_and_put_file_system_config(Some(&config), &desired)
                .unwrap()
        }));
        let mut writer = runtime.client(&primary).unwrap();
        writer.posix_mkdir_all(path).unwrap();
        let mut objects = Vec::new();
        for index in 0..count {
            let name = format!("{path}/object-{index:03}.bin");
            writer
                .posix_write_file(
                    &name,
                    &vec![index as u8; 4096],
                    crate::DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
                )
                .unwrap();
            let stat = writer.posix_stat(&name).unwrap();
            objects.push(
                writer
                    .current_read_object_metadata(&stat.object_id)
                    .unwrap(),
            );
        }
        if remote {
            // A client facade never replays journal replies into Main's store.
            // Certify the actual authored cut on both metadata participants
            // before opening the replica view used by physical page checks.
            let mut source = runtime.metadata_primary_service(path).unwrap();
            let target = source
                .capture_metadata_quorum_sync_target(objects.last().unwrap())
                .unwrap();
            let certificate = source
                .sync_metadata_shard_to_quorum(
                    &target,
                    sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                )
                .unwrap();
            assert_eq!(
                certificate
                    .acknowledged_node_ids
                    .into_iter()
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([primary.clone(), root_primary.clone()]),
                "both actual metadata participants must acknowledge the authored cut"
            );
        }
        let mut service = runtime.client(&root_primary).unwrap();
        if remote {
            // The ordinary lookup remains part of the fixture, but its reply
            // cannot substitute for the physical acknowledgement above.
            let synced = service
                .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                    sifs_file_system_id(),
                    primary.clone(),
                    DfsNamespaceOperation::SyncPath { path: path.into() },
                    crate::DFS_ROUTED_NAMESPACE_OP_TIMEOUT,
                    true,
                    false,
                )
                .unwrap();
            assert_eq!(synced.result, crate::DfsNamespaceResult::Applied);
            assert!(
                !synced.metadata_journal_has_more,
                "the fixture's bounded native writes must fit the ordinary sync response"
            );
            let backend = service.metadata_backend().unwrap();
            for object in &objects {
                assert_eq!(
                    backend.get_object(&object.object_id).unwrap().as_ref(),
                    Some(object),
                    "placement starts from the actual received durable row"
                );
            }
            let directory = backend
                .get_object_by_path(&sifs_file_system_id(), path)
                .unwrap()
                .unwrap();
            assert_eq!(directory.kind, DfsObjectKind::Directory);
            assert_eq!(
                backend
                    .object_page_cursor_for_object(&directory)
                    .unwrap()
                    .object_id,
                directory.object_id
            );
        }
        let candidates = service.placement_content_candidates().unwrap();
        assert!(!candidates.is_empty());
        let job = native_placement_job(
            desired.clone(),
            path,
            candidates,
            format!("native-noop-{label}"),
        );
        if publish_initial_job {
            assert!(control.with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .compare_and_put_placement_update(&desired, &desired, &job)
                    .unwrap()
            }));
        }
        service.refresh_control_plane_latest().unwrap();
        Self {
            runtime,
            service,
            control,
            objects,
            job,
            primary,
        }
    }
    fn namespace_calls(&self) -> usize {
        self.runtime
            .rpc_observations()
            .iter()
            .filter(|call| {
                call.target == self.primary
                    && call.route == crate::DFS_DATA_NAMESPACE_ROUTE
                    && call.phase == RpcPhase::BeforeDispatch
            })
            .count()
    }
    fn close(self) {
        drop(self.service);
        self.runtime.close().unwrap();
    }
}

#[test]
fn native_placement_keeps_accepted_candidates_and_fences_later_policy() {
    let mut case =
        NativePlacementFixture::with_nodes("native-accepted-placement-candidates", false, 2, 3);
    let nodes = DfsTestRuntime::ordered_node_ids("native-accepted-placement-candidates", 3);
    let original_config = case.job.accepted_config.clone();
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    policy.required_node_ids = nodes[..2].to_vec();
    let accepted = placement_config_with_override(
        &original_config,
        &case.job.path,
        DfsPlacementScope::Subtree,
        Some(policy),
    )
    .unwrap();
    let all_candidates = case.service.placement_content_candidates().unwrap();
    assert!(nodes.iter().all(|node| {
        all_candidates
            .iter()
            .any(|candidate| &candidate.node_id == node)
    }));
    let candidates = all_candidates
        .into_iter()
        .filter(|candidate| nodes[..2].contains(&candidate.node_id))
        .collect();
    let job = native_placement_job(
        accepted.clone(),
        &case.job.path,
        candidates,
        "native-accepted-two-candidates".into(),
    );
    assert!(case.control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .compare_and_put_placement_update(&original_config, &accepted, &job)
            .unwrap()
    }));
    case.service.refresh_control_plane_latest().unwrap();
    let object = case.objects[0].clone();
    let (resolved, replication, placement) =
        reconciliation_placement_for_object(&accepted, &job, &object).unwrap();
    assert_eq!(placement.plan.desired_replica_nodes, nodes[..2]);
    assert!(!placement.plan.desired_replica_nodes.contains(&nodes[2]));
    case.service
        .reconcile_object_placement_local(
            &job.job_id,
            &object.path,
            &object.object_id,
            &object.version,
            resolved.policy.consistency,
            replication,
            &placement.plan.desired_replica_nodes,
        )
        .unwrap();
    let delayed = case.objects[1].clone();
    let (delayed_policy, delayed_replication, delayed_plan) =
        reconciliation_placement_for_object(&accepted, &job, &delayed).unwrap();
    let mut new_policy =
        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    new_policy.required_node_ids = vec![nodes[0].clone()];
    new_policy.forbidden_node_ids = vec![nodes[1].clone()];
    let current = placement_config_with_override(
        &accepted,
        &job.path,
        DfsPlacementScope::Subtree,
        Some(new_policy),
    )
    .unwrap();
    let candidates = case.service.placement_content_candidates().unwrap();
    let newer_job = native_placement_job(
        current.clone(),
        &job.path,
        candidates,
        "native-newer-placement-policy".into(),
    );
    assert!(case.control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .compare_and_put_placement_update(&accepted, &current, &newer_job)
            .unwrap()
    }));
    let before = case
        .service
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    case.service
        .reconcile_object_placement_local(
            &job.job_id,
            &delayed.path,
            &delayed.object_id,
            &delayed.version,
            delayed_policy.policy.consistency,
            delayed_replication,
            &delayed_plan.plan.desired_replica_nodes,
        )
        .unwrap();
    assert_eq!(
        case.service
            .current_read_object_metadata(&delayed.object_id)
            .unwrap(),
        delayed
    );
    assert_eq!(
        case.service
            .metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
    case.close();
}

#[test]
fn native_placement_noop_preserves_exact_visible_generation_and_journal() {
    let mut case = NativePlacementFixture::new("native-placement-noop", false, 1);
    let object = case.objects[0].clone();
    let before = case
        .service
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let (resolved, replication, placement) =
        reconciliation_placement_for_object(&case.job.accepted_config, &case.job, &object).unwrap();
    for _ in 0..2 {
        case.service
            .reconcile_object_placement_local(
                &case.job.job_id,
                &object.path,
                &object.object_id,
                &object.version,
                resolved.policy.consistency.clone(),
                replication.clone(),
                &placement.plan.desired_replica_nodes,
            )
            .unwrap();
        assert_eq!(
            case.service
                .current_read_object_metadata(&object.object_id)
                .unwrap(),
            object
        );
        assert_eq!(
            case.service
                .metadata_backend()
                .unwrap()
                .journal_last_sequence()
                .unwrap(),
            before
        );
    }
    case.close();
}

#[test]
fn native_local_noop_uses_actual_page_authority_and_rechecks_generation() {
    let mut case = NativePlacementFixture::new("native-local-noop-authority", false, 1);
    let original = case.objects[0].clone();
    let config = case.job.accepted_config.clone();
    let authority = case
        .service
        .prepare_placement_page_authority(&config)
        .unwrap();
    assert!(
        authority.is_some(),
        "normal native ReadIndex must certify the page"
    );
    assert!(matches!(
        case.service
            .placement_noop_check(&original, false, authority.as_ref())
            .unwrap(),
        PlacementNoopCheck::CertifiedLocal
    ));
    case.service
        .posix_write_file(
            &original.path,
            b"actual newer content",
            crate::DfsWriteOptions::replicated_eventual(vec![case.primary.clone()]),
        )
        .unwrap();
    assert!(matches!(
        case.service
            .placement_noop_check(&original, false, authority.as_ref())
            .unwrap(),
        PlacementNoopCheck::RequiresReconciliation
    ));
    case.close();
}

#[test]
fn native_local_placement_preserves_actual_content_owner_and_bytes() {
    let nodes = DfsTestRuntime::ordered_node_ids("native-local-placement-owner", 2);
    let mut runtime =
        DfsTestRuntime::new("native-local-placement-owner", nodes[0].clone()).unwrap();
    runtime.define_node(nodes[1].clone()).unwrap();
    runtime.start_nodes().unwrap();
    let path = format!("{}/owned.bin", crate::sifs_node_files_path(&nodes[1]));
    let bytes = vec![0x5a; 4096];
    let mut owner = runtime.client(&nodes[1]).unwrap();
    let stat = owner
        .posix_write_file(
            &path,
            &bytes,
            crate::DfsWriteOptions::replicated_eventual(vec![nodes[1].clone()]),
        )
        .unwrap();
    let object = owner.current_read_object_metadata(&stat.object_id).unwrap();
    assert_eq!(object.owner_node_id, nodes[1]);
    let mut primary = runtime.metadata_primary_service(&path).unwrap();
    assert_eq!(primary.node_id(), &nodes[0]);
    let cached_before = primary
        .content()
        .has_blob(object.content_hash.as_ref().unwrap())
        .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let config = control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .get_file_system_config(&sifs_file_system_id())
            .unwrap()
            .unwrap()
    });
    let candidates = primary.placement_content_candidates().unwrap();
    let mut job = native_placement_job(
        config.clone(),
        &path,
        candidates,
        "actual-local-owner-policy".into(),
    );
    job.scope = DfsPlacementScope::SelfOnly;
    assert!(control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .compare_and_put_placement_update(&config, &config, &job)
            .unwrap()
    }));
    let (resolved, replication, placement) =
        reconciliation_placement_for_object(&config, &job, &object).unwrap();
    assert_eq!(resolved.policy.consistency, DfsObjectPolicy::LocalOwned);
    assert_eq!(replication.min_commit_replicas, 1);
    assert_eq!(replication.desired_replicas, 1);
    primary
        .reconcile_object_placement_local(
            &job.job_id,
            &path,
            &object.object_id,
            &object.version,
            resolved.policy.consistency,
            replication,
            &placement.plan.desired_replica_nodes,
        )
        .unwrap();
    assert_eq!(
        primary
            .content()
            .has_blob(object.content_hash.as_ref().unwrap())
            .unwrap(),
        cached_before,
        "local-owner reconciliation must not add a cache copy; the initial routed write may already have cached its input"
    );
    assert_eq!(
        primary
            .current_read_object_metadata(&object.object_id)
            .unwrap(),
        object,
        "the actual metadata primary must preserve the owner's canonical locations and generation"
    );
    assert_eq!(owner.posix_read_file(&path).unwrap(), bytes);
    assert_eq!(
        owner
            .current_read_object_metadata(&object.object_id)
            .unwrap(),
        object
    );
    drop(primary);
    drop(owner);
    runtime.close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn native_remote_noop_requires_real_primary_and_rejects_changed_generation() {
    let mut case = NativePlacementFixture::new("native-remote-placement-noop", true, 1);
    let original = case.objects[0].clone();
    let config = case.job.accepted_config.clone();
    let mut selected = case.job.clone();
    selected.path = original.path.clone();
    selected.scope = DfsPlacementScope::SelfOnly;
    let budget = DfsAdaptivePlacementBudget {
        max_objects_per_tick: 1,
        max_bytes_per_tick: u64::MAX,
        max_millis_per_tick: 5_000,
    };
    let prepared = case
        .service
        .prepare_placement_reconciliation_objects(&config, &selected, &budget, Instant::now(), None)
        .unwrap();
    assert_eq!(prepared.len(), 1);
    assert!(matches!(&prepared[0].plan.as_ref().unwrap().noop_check,
        PlacementNoopCheck::LookupRemote(shard) if shard.primary_node_id == case.primary));
    let before = case.namespace_calls();
    let proofs = case
        .service
        .read_remote_placement_noop_proofs(&config, &prepared, Instant::now(), 5_000)
        .await
        .unwrap();
    assert_eq!(proofs.objects[&original.object_id].0, original);
    assert!(
        case.namespace_calls() > before,
        "the real remote namespace receiver must supply freshness"
    );
    let mut owner = case.runtime.client(&case.primary).unwrap();
    owner
        .posix_write_file(
            &original.path,
            b"new actual native generation",
            crate::DfsWriteOptions::replicated_eventual(vec![case.primary.clone()]),
        )
        .unwrap();
    let stale = case
        .service
        .read_remote_placement_noop_proofs(&config, &prepared, Instant::now(), 5_000)
        .await
        .unwrap();
    assert!(
        !stale.objects.contains_key(&original.object_id),
        "the captured old generation cannot authorize a no-op"
    );
    drop(owner);
    case.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn native_remote_noop_delivery_failure_produces_no_authority() {
    use crate::test_runtime::{FaultAction, FaultRule};
    let mut case = NativePlacementFixture::new("native-placement-noop-failure", true, 1);
    let object = case.objects[0].clone();
    let config = case.job.accepted_config.clone();
    let mut selected = case.job.clone();
    selected.path = object.path.clone();
    selected.scope = DfsPlacementScope::SelfOnly;
    let budget = DfsAdaptivePlacementBudget {
        max_objects_per_tick: 1,
        max_bytes_per_tick: u64::MAX,
        max_millis_per_tick: 5_000,
    };
    let prepared = case
        .service
        .prepare_placement_reconciliation_objects(&config, &selected, &budget, Instant::now(), None)
        .unwrap();
    case.runtime.install_fault(FaultRule {
        source: Some(case.service.node_id().clone()),
        target: Some(case.primary.clone()),
        route: Some(crate::DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 1,
        action: FaultAction::BudgetExhausted,
    });
    assert!(
        case.service
            .read_remote_placement_noop_proofs(&config, &prepared, Instant::now(), 5_000)
            .await
            .is_err()
    );
    case.runtime.clear_faults();
    let recovered = case
        .service
        .read_remote_placement_noop_proofs(&config, &prepared, Instant::now(), 5_000)
        .await
        .unwrap();
    assert_eq!(recovered.objects[&object.object_id].0, object);
    case.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn native_remote_noop_pages_resume_at_exact_backend_cursor_without_mutation() {
    let mut case = NativePlacementFixture::new("native-remote-noop-pages", true, 4);
    let config = case.job.accepted_config.clone();
    let directory = case.service.posix_stat(&case.job.path).unwrap();
    let directory = case
        .service
        .current_read_object_metadata(&directory.object_id)
        .unwrap();
    let mut selected = case.job.clone();
    selected.state = DfsPlacementReconciliationState::Scanning;
    selected.cursor = Some(
        case.service
            .metadata_backend()
            .unwrap()
            .object_page_cursor_for_object(&directory)
            .unwrap(),
    );
    let before = case
        .service
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let calls = case.namespace_calls();
    case.service
        .advance_placement_reconciliation_page_async(&config, &mut selected, 3, None)
        .await
        .unwrap();
    assert_eq!(selected.scanned_object_count, 3);
    assert_eq!(selected.updated_object_count, 0);
    assert_eq!(
        selected.cursor.as_ref().unwrap().object_id,
        case.objects[2].object_id
    );
    assert_eq!(
        case.namespace_calls() - calls,
        1,
        "one genuine primary LookupPaths supplies this page"
    );
    case.service
        .advance_placement_reconciliation_page_async(&config, &mut selected, 3, None)
        .await
        .unwrap();
    assert_eq!(selected.scanned_object_count, 4);
    assert!(selected.cursor.is_none());
    assert_eq!(case.namespace_calls() - calls, 2);
    assert_eq!(selected.pending_repair_count, 0);
    assert_eq!(
        case.service
            .metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
    for object in &case.objects {
        assert_eq!(
            case.service
                .current_read_object_metadata(&object.object_id)
                .unwrap(),
            *object
        );
    }
    case.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn native_remote_noop_proof_binds_complete_config_assignment_and_row_once() {
    let mut case = NativePlacementFixture::new("native-remote-noop-proof-bindings", true, 2);
    let original = case.objects[0].clone();
    let config = case.job.accepted_config.clone();
    let mut selected = case.job.clone();
    selected.path = original.path.clone();
    selected.scope = DfsPlacementScope::SelfOnly;
    let budget = placement_page_budget(&selected, 1);
    let prepared = case
        .service
        .prepare_placement_reconciliation_objects(&config, &selected, &budget, Instant::now(), None)
        .unwrap();
    let shard = case
        .service
        .select_metadata_shard_for_object(&original)
        .unwrap()
        .unwrap();
    for changed in [
        "configuration",
        "primary",
        "epoch",
        "object",
        "filesystem",
        "path",
        "version",
        "locations",
    ] {
        let mut proofs = case
            .service
            .read_remote_placement_noop_proofs(&config, &prepared, Instant::now(), 5_000)
            .await
            .unwrap();
        assert_eq!(
            proofs.objects[&original.object_id].0, original,
            "positive evidence comes from the actual receiver"
        );
        let mut expected_config = config.clone();
        let mut expected_shard = shard.clone();
        let mut expected_object = original.clone();
        match changed {
            "configuration" => expected_config.owner_node_id = case.primary.clone(),
            "primary" => expected_shard.primary_node_id = case.service.node_id().clone(),
            "epoch" => expected_shard.epoch += 1,
            "object" => expected_object.object_id = case.objects[1].object_id.clone(),
            "filesystem" => expected_object.file_system_id = DfsFileSystemId::generate(),
            "path" => expected_object.path = case.objects[1].path.clone(),
            "version" => expected_object.version = case.objects[1].version.clone(),
            "locations" => expected_object.current_locations.clear(),
            _ => unreachable!(),
        }
        assert!(
            !proofs.take_matches(&expected_config, &expected_object, &expected_shard),
            "captured proof cannot authorize altered {changed}"
        );
    }
    let mut proofs = case
        .service
        .read_remote_placement_noop_proofs(&config, &prepared, Instant::now(), 5_000)
        .await
        .unwrap();
    assert!(proofs.take_matches(&config, &original, &shard));
    assert!(
        !proofs.take_matches(&config, &original, &shard),
        "the exact receiver evidence is invocation-owned and consumed once"
    );
    case.close();
}

#[test]
fn native_placement_preserves_actual_retained_durability_and_repair_owner_after_gc() {
    native_placement_retirement(true, false);
}

#[test]
fn native_completed_placement_does_not_resurrect_superseded_repairs_after_reopen() {
    native_placement_retirement(false, false);
}

#[test]
fn native_placement_replay_after_handoff_preserves_original_committed_obligation() {
    native_placement_retirement(true, true);
}

#[test]
fn native_unfinished_placement_repair_survives_primary_handoff() {
    native_unfinished_placement_repair(true);
}

#[test]
fn native_unfinished_placement_repair_progresses_with_same_primary() {
    native_unfinished_placement_repair(false);
}

fn native_unfinished_placement_repair(handoff: bool) {
    let label = if handoff {
        "native-unfinished-placement-handoff"
    } else {
        "native-unfinished-placement-same-owner"
    };
    let mut case = NativePlacementFixture::with_nodes_and_initial_job(label, false, 1, 3, false);
    let nodes = DfsTestRuntime::ordered_node_ids(label, 3);
    let old_primary = case.primary.clone();
    let promoted = if handoff {
        nodes[2].clone()
    } else {
        old_primary.clone()
    };
    let replica = nodes[1].clone();
    let object = case.objects[0].clone();
    // Hold the actual old owner's content producer before claim. Metadata
    // publication and the real handoff remain live; no receipt is supplied.
    let held = case
        .runtime
        .deny_background_replica_before_claim(&old_primary)
        .unwrap();
    let original = case.job.accepted_config.clone();
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
    policy.required_node_ids = vec![old_primary.clone(), replica.clone()];
    let desired = placement_config_with_override(
        &original,
        &case.job.path,
        DfsPlacementScope::Subtree,
        Some(policy),
    )
    .unwrap();
    let job = native_placement_job(
        desired.clone(),
        &case.job.path,
        case.job.candidate_snapshot.clone(),
        format!("unfinished-{label}"),
    );
    assert!(case.control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .compare_and_put_placement_update(&original, &desired, &job)
            .unwrap()
    }));
    let (resolved, replication, placement) =
        reconciliation_placement_for_object(&desired, &job, &object).unwrap();
    let operation = DfsNamespaceOperation::ReconcilePlacement {
        job_id: job.job_id.clone(),
        path: object.path.clone(),
        object_id: object.object_id.clone(),
        expected_version: object.version.clone(),
        object_policy: resolved.policy.consistency,
        replication_policy: replication,
        desired_replica_node_ids: placement.plan.desired_replica_nodes,
    };
    case.service.refresh_control_plane_latest().unwrap();
    case.service
        .route_namespace_operation(old_primary.clone(), operation.clone())
        .expect("the actual original primary commits its placement obligation");
    let intent_id = format!(
        "placement:{}:{}:{}",
        job.job_id, object.object_id, object.version
    );
    let original_record = {
        let owner = case.runtime.metadata_primary_service(&object.path).unwrap();
        let backend = owner.metadata_backend().unwrap();
        let record = backend
            .get_intent(&object.object_id, &intent_id)
            .unwrap()
            .expect("real committed placement intent");
        assert_eq!(record.status, crate::DfsWriteIntentStatus::Committed);
        assert!(
            backend
                .receipts_for_version(&object.object_id, &object.version)
                .unwrap()
                .iter()
                .all(|receipt| receipt.replica_node_id != replica),
            "the handoff must begin with a genuine missing target"
        );
        record
    };
    if handoff {
        case.runtime
            .reassign_metadata_shard(
                &object.file_system_id,
                "services",
                promoted.clone(),
                vec![old_primary.clone(), replica.clone()],
            )
            .unwrap();
    }
    drop(held);
    case.service.refresh_control_plane_latest().unwrap();
    let replay = case
        .service
        .route_namespace_operation(promoted.clone(), operation);
    let after = {
        let owner = case.runtime.metadata_primary_service(&object.path).unwrap();
        assert_eq!(owner.node_id(), &promoted);
        owner
            .metadata_backend()
            .unwrap()
            .get_intent(&object.object_id, &intent_id)
            .unwrap()
            .unwrap()
    };
    let deadline = Instant::now() + crate::DEFAULT_DFS_POSIX_SYNC_TIMEOUT;
    let mut completed = false;
    let mut maintenance = Vec::new();
    while replay.is_ok() && Instant::now() < deadline {
        // The native fixture exposes the registered Main's ordinary bounded
        // maintenance call explicitly. Observational reads do not drive it.
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        let remaining = remaining.min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
        let observation = case.runtime.maintenance_tick(&promoted, remaining).unwrap();
        maintenance.push(serde_json::json!({
            "outcome": format!("{:?}", observation.outcome),
            "queue_before": format!("{:?}", observation.queue_before),
            "queue_after": format!("{:?}", observation.queue_after),
            "omitted_kv_events": observation.omitted_kv_event_count,
        }));
        if observation.outcome.is_err() {
            break;
        }
        let owner = case.runtime.metadata_primary_service(&object.path).unwrap();
        let receipts = owner
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&object.object_id, &object.version)
            .unwrap();
        if receipts.iter().any(|receipt| {
            receipt.replica_node_id == replica && receipt.content_hash == object.content_hash
        }) {
            let target = case.runtime.client(&replica).unwrap();
            let actual = target
                .content()
                .read_object_version(object.object_id.as_str(), object.version.as_str())
                .unwrap();
            assert_eq!(actual.1, vec![0; 4096]);
            completed = true;
            break;
        }
        drop(owner);
        std::thread::sleep(Duration::from_millis(10));
    }
    let calls = case.runtime.rpc_observations();
    let selected: Vec<_> = calls
        .iter()
        .filter(|call| call.route.starts_with("/dfs/"))
        .map(|call| {
            serde_json::json!({
                "source": call.source, "target": call.target, "call_id": call.call_id,
                "route": call.route, "phase": format!("{:?}", call.phase),
                "origin": format!("{:?}", call.origin), "outcome_error": call.outcome_error,
                "intent": call.dfs_intent_id, "request": call.dfs_request_id,
                "object": call.dfs_object_id, "version": call.dfs_version,
            })
        })
        .collect();
    std::fs::write(
        case.runtime
            .data_dir(&promoted)
            .root()
            .join("boundary-diagnostic-unfinished-placement.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "original": original_record, "after": after, "replay": format!("{replay:?}"),
            "old_primary": old_primary, "new_primary": promoted, "replica": replica, "handoff": handoff,
            "completed": completed, "maintenance": maintenance,
            "all_rpc_observation_count": calls.len(), "selected_dfs_rpc_observations": selected,
            "qualification": "Actual fresh native fixture; selected DFS RPCs only. Full call count states omitted other routes. No supplied success, receipt or replacement authority."
        }))
        .unwrap(),
    )
    .unwrap();
    eprintln!(
        "unfinished-placement-handoff original={original_record:?} replay={replay:?} after={after:?} completed={completed} maintenance={maintenance:?}"
    );
    case.close();
    replay.expect("the new actual primary must resume the exact unfinished placement");
    assert_eq!(
        after, original_record,
        "handoff cannot rewrite the committed placement identity or captured body"
    );
    assert!(completed, "the real missing target must become durable");
}

fn native_placement_retirement(retain: bool, replay_after_handoff: bool) {
    let label = if replay_after_handoff {
        "native-placement-replay-handoff"
    } else if retain {
        "native-placement-retained-grant"
    } else {
        "native-placement-retirement"
    };
    let mut case = NativePlacementFixture::with_nodes(label, false, 1, 2);
    let nodes = DfsTestRuntime::ordered_node_ids(label, 2);
    let original = case.job.accepted_config.clone();
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).unwrap();
    policy.required_node_ids = nodes.clone();
    let two_copies = placement_config_with_override(
        &original,
        &case.job.path,
        DfsPlacementScope::Subtree,
        Some(policy),
    )
    .unwrap();
    assert!(case.control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .compare_and_put_file_system_config(Some(&original), &two_copies)
            .unwrap()
    }));
    case.service.refresh_control_plane_latest().unwrap();
    let path = case.objects[0].path.clone();
    case.service
        .posix_write_file(
            &path,
            b"actual W2 retained bytes",
            crate::DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: false,
            },
        )
        .unwrap();
    let stat = case.service.posix_stat(&path).unwrap();
    let object = case
        .service
        .current_read_object_metadata(&stat.object_id)
        .unwrap();
    let mut version = case
        .service
        .metadata_backend()
        .unwrap()
        .get_version(&object.object_id, &object.version)
        .unwrap()
        .unwrap();
    let actual_durability = version
        .content_durability
        .clone()
        .expect("normal W2 publication records actual durability");
    assert_eq!(actual_durability.policy.min_commit_replicas, 2);
    assert_eq!(actual_durability.policy.desired_replicas, 2);
    assert_eq!(actual_durability.desired_replica_node_ids.len(), 2);
    let actual_nodes = actual_durability
        .desired_replica_node_ids
        .iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(actual_nodes.len(), 2);
    assert_eq!(actual_nodes, nodes.iter().collect::<BTreeSet<_>>());
    let original_intents = case
        .service
        .metadata_backend()
        .unwrap()
        .write_intents_page(None, 128)
        .unwrap()
        .intents;
    let original_intent = original_intents
        .iter()
        .find(|intent| {
            intent.object_id == object.object_id
                && intent.target_version == object.version
                && intent.status == crate::DfsWriteIntentStatus::Committed
        })
        .expect("actual completed W2 native write retains its retry identity")
        .intent_id
        .clone();
    if retain {
        crate::test_support::install_retention_grant(
            &mut version,
            "org.example.placement-retention",
            &object,
            dfs_now_unix_nanos(),
            Some(crate::DfsOrderedRetentionGrant {
                order: 1,
                granted_at_unix_nanos: dfs_now_unix_nanos(),
                minimum: crate::DfsRetentionMinimum {
                    keep_last: 1,
                    keep_for_seconds: 0,
                },
            }),
        );
        assert!(
            case.service
                .publish_object_metadata_effects(
                    &object,
                    vec![crate::DfsPreparedMetadataEffect::PutVersion { version }]
                )
                .unwrap()
        );
    }
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap();
    policy.required_node_ids = vec![case.primary.clone()];
    let one_copy = placement_config_with_override(
        &two_copies,
        &case.job.path,
        DfsPlacementScope::Subtree,
        Some(policy),
    )
    .unwrap();
    let job = native_placement_job(
        one_copy.clone(),
        &case.job.path,
        case.service.placement_content_candidates().unwrap(),
        "native-retained-copy-placement".into(),
    );
    assert!(case.control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .compare_and_put_placement_update(&two_copies, &one_copy, &job)
            .unwrap()
    }));
    case.service.refresh_control_plane_latest().unwrap();
    let (resolved, replication, placement) =
        reconciliation_placement_for_object(&one_copy, &job, &object).unwrap();
    case.service
        .reconcile_object_placement_local(
            &job.job_id,
            &object.path,
            &object.object_id,
            &object.version,
            resolved.policy.consistency,
            replication,
            &placement.plan.desired_replica_nodes,
        )
        .unwrap();
    if replay_after_handoff {
        let intent_id = format!(
            "placement:{}:{}:{}",
            job.job_id, object.object_id, object.version
        );
        let recorded = case
            .service
            .metadata_backend()
            .unwrap()
            .get_intent(&object.object_id, &intent_id)
            .unwrap()
            .unwrap();
        let promoted = nodes[1].clone();
        case.runtime
            .reassign_metadata_shard(
                &object.file_system_id,
                "services",
                promoted.clone(),
                vec![case.primary.clone()],
            )
            .unwrap();
        let (resolved, replication, placement) =
            reconciliation_placement_for_object(&one_copy, &job, &object).unwrap();
        // Let the real serving Main own admission; a direct Core invocation
        // can race the normal reconciler's live placement transaction.
        case.service.refresh_control_plane_latest().unwrap();
        case.service
            .route_namespace_operation(
                promoted.clone(),
                DfsNamespaceOperation::ReconcilePlacement {
                    job_id: job.job_id.clone(),
                    path: object.path.clone(),
                    object_id: object.object_id.clone(),
                    expected_version: object.version.clone(),
                    object_policy: resolved.policy.consistency,
                    replication_policy: replication,
                    desired_replica_node_ids: placement.plan.desired_replica_nodes,
                },
            )
            .expect("the actual new owner must replay the same immutable placement obligation");
        let owner = case.runtime.metadata_primary_service(&path).unwrap();
        assert_eq!(owner.node_id(), &promoted);
        assert_eq!(
            owner
                .metadata_backend()
                .unwrap()
                .get_intent(&object.object_id, &intent_id)
                .unwrap()
                .unwrap(),
            recorded
        );
        assert_eq!(
            owner
                .current_read_object_metadata(&object.object_id)
                .unwrap()
                .current_locations,
            object.current_locations
        );
        drop(owner);
        case.close();
        return;
    }
    assert_eq!(
        case.service
            .current_read_object_metadata(&object.object_id)
            .unwrap()
            .current_locations,
        if retain {
            object.current_locations.clone()
        } else {
            vec![case.primary.clone()]
        },
        "only independently retained copies survive current placement pruning"
    );
    if !retain {
        assert!(
            case.service
                .metadata_backend()
                .unwrap()
                .get_intent_receipt(&object.object_id, &original_intent)
                .unwrap()
                .is_some(),
            "the exact original request remains a receipt, never executable old repair work"
        );
    }
    case.service
        .gc_metadata_history(crate::DfsMetadataHistoryGcOptions::delete_eligible(
            Duration::ZERO,
            0,
        ))
        .unwrap();
    let retained = case
        .service
        .metadata_backend()
        .unwrap()
        .get_version(&object.object_id, &object.version)
        .unwrap()
        .unwrap();
    assert_eq!(
        retained.content_durability.as_ref(),
        Some(&actual_durability)
    );
    if retain {
        assert!(
            retained.retention["org.example.placement-retention"]
                .grant
                .is_some()
        );
        let owners = case
            .service
            .metadata_backend()
            .unwrap()
            .write_intents_page(None, 128)
            .unwrap()
            .intents;
        let owner = owners
            .iter()
            .find(|intent| {
                intent.object_id == object.object_id && intent.publication.is_retained_repair()
            })
            .expect("genuine retained repair owner survives current placement and GC");
        assert_eq!(owner.policy.min_commit_replicas, 2);
        assert_eq!(owner.policy.desired_replicas, 2);
        assert_eq!(
            owner.desired_replica_node_ids,
            actual_durability.desired_replica_node_ids
        );
    } else {
        assert!(retained.retention.is_empty());
        case.service.hydrate_from_metadata_backend().unwrap();
        assert!(
            !case
                .service
                .coordinator
                .repair_jobs()
                .iter()
                .any(|repair| repair.object_id == object.object_id)
        );
        let primary = case.primary.clone();
        drop(case.service);
        case.runtime.stop_node(&primary).unwrap();
        case.runtime.reopen_node(primary.clone()).unwrap();
        case.service = case.runtime.metadata_primary_service(&path).unwrap();
        assert!(
            !case
                .service
                .coordinator
                .repair_jobs()
                .iter()
                .any(|repair| repair.object_id == object.object_id),
            "reopen cannot recreate the retired second-copy obligation"
        );
        assert_eq!(
            case.service
                .current_read_object_metadata(&object.object_id)
                .unwrap()
                .current_locations,
            vec![primary]
        );
    }
    let replica = case.runtime.client(&nodes[1]).unwrap();
    assert_eq!(
        replica
            .content()
            .read_object_version(object.object_id.as_str(), object.version.as_str())
            .unwrap()
            .1,
        b"actual W2 retained bytes"
    );
    drop(replica);
    case.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn native_remote_response_validator_rejects_ambiguous_or_foreign_captured_rows() {
    let mut case = NativePlacementFixture::new("native-remote-noop-malformed-rows", true, 3);
    let config = case.job.accepted_config.clone();
    let directory = case.service.posix_stat(&case.job.path).unwrap();
    let directory = case
        .service
        .current_read_object_metadata(&directory.object_id)
        .unwrap();
    let mut selected = case.job.clone();
    selected.cursor = Some(
        case.service
            .metadata_backend()
            .unwrap()
            .object_page_cursor_for_object(&directory)
            .unwrap(),
    );
    let prepared = case
        .service
        .prepare_placement_reconciliation_objects(
            &config,
            &selected,
            &placement_page_budget(&selected, 3),
            Instant::now(),
            None,
        )
        .unwrap();
    let proofs = case
        .service
        .read_remote_placement_noop_proofs(&config, &prepared, Instant::now(), 5_000)
        .await
        .unwrap();
    let captured: Vec<_> = case
        .objects
        .iter()
        .map(|object| proofs.objects[&object.object_id].0.clone())
        .collect();
    let shards: Vec<_> = case
        .objects
        .iter()
        .map(|object| proofs.objects[&object.object_id].1.clone())
        .collect();
    let expected: Vec<_> = case.objects.iter().zip(&shards).collect();
    assert_eq!(
        matching_placement_noop_rows(&expected, captured.clone()).len(),
        3
    );
    for invalid in [
        "version",
        "missing",
        "duplicate-id",
        "duplicate-path",
        "object-id",
        "filesystem",
    ] {
        let mut response = captured.clone();
        match invalid {
            "version" => response[1].version = DfsVersion::generate(),
            "missing" => {
                response.remove(1);
            }
            "duplicate-id" => response.push(response[1].clone()),
            "duplicate-path" => {
                let mut row = response[1].clone();
                row.object_id = DfsObjectId::generate();
                response.push(row);
            }
            "object-id" => response[1].object_id = case.objects[2].object_id.clone(),
            "filesystem" => response[1].file_system_id = DfsFileSystemId::generate(),
            _ => unreachable!(),
        }
        let accepted = matching_placement_noop_rows(&expected, response);
        assert!(
            !accepted
                .iter()
                .any(|(object, _)| object.object_id == case.objects[1].object_id),
            "altered {invalid} must not certify the requested immutable row"
        );
        assert!(
            accepted
                .iter()
                .any(|(object, _)| object == &case.objects[0]),
            "a malformed neighbor must not replace another row's identity"
        );
    }
    case.close();
}

#[test]
fn native_local_owner_rejects_substitution_missing_owner_and_policy_conversion() {
    let nodes = DfsTestRuntime::ordered_node_ids("native-local-placement-negative", 2);
    let mut runtime =
        DfsTestRuntime::new("native-local-placement-negative", nodes[0].clone()).unwrap();
    runtime.define_node(nodes[1].clone()).unwrap();
    runtime.start_nodes().unwrap();
    let path = format!("{}/negative.bin", crate::sifs_node_files_path(&nodes[1]));
    let mut owner = runtime.client(&nodes[1]).unwrap();
    let stat = owner
        .posix_write_file(
            &path,
            b"actual local-owner bytes",
            crate::DfsWriteOptions::replicated_eventual(vec![nodes[1].clone()]),
        )
        .unwrap();
    let object = owner.current_read_object_metadata(&stat.object_id).unwrap();
    assert_eq!(object.owner_node_id, nodes[1]);
    assert_eq!(object.policy, DfsObjectPolicy::LocalOwned);
    assert!(validate_local_placement_owner(&object, &DfsObjectPolicy::LocalOwned).is_ok());
    let mut missing_owner = object.clone();
    missing_owner
        .current_locations
        .retain(|node| node != &missing_owner.owner_node_id);
    assert!(matches!(
        validate_local_placement_owner(&missing_owner, &DfsObjectPolicy::LocalOwned),
        Err(DfsError::Distributed { .. })
    ));
    for target in [DfsObjectPolicy::EphemeralLocal] {
        assert!(matches!(
            validate_local_placement_owner(&object, &target),
            Err(DfsError::InvalidReplicationPolicy { .. })
        ));
    }
    let mut primary = runtime.client(&nodes[0]).unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let config = control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .get_file_system_config(&sifs_file_system_id())
            .unwrap()
            .unwrap()
    });
    let mut job = native_placement_job(
        config.clone(),
        &path,
        primary.placement_content_candidates().unwrap(),
        "native-local-owner-negative-job".into(),
    );
    job.scope = DfsPlacementScope::SelfOnly;
    assert!(control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .compare_and_put_placement_update(&config, &config, &job)
            .unwrap()
    }));
    let before = primary
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    assert!(matches!(
        primary.reconcile_object_placement_local(
            &job.job_id,
            &path,
            &object.object_id,
            &object.version,
            DfsObjectPolicy::LocalOwned,
            DfsReplicationPolicy::new(1, 1).unwrap(),
            &[nodes[0].clone()]
        ),
        Err(DfsError::Distributed { .. })
    ));
    assert_eq!(
        primary
            .metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
    assert_eq!(
        owner
            .current_read_object_metadata(&object.object_id)
            .unwrap(),
        object
    );
    assert_eq!(
        owner.posix_read_file(&path).unwrap(),
        b"actual local-owner bytes"
    );
    drop(primary);
    drop(owner);
    runtime.close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn native_remote_page_cancellation_preserves_durable_cursor_and_replays_once() {
    use crate::test_runtime::{FaultAction, FaultRule};
    // The measured SelfOnly job below must be the actual selected work, not
    // a second publication behind the fixture's unrelated Subtree proposal.
    let mut case = NativePlacementFixture::with_nodes_and_initial_job(
        "native-placement-page-cancellation",
        true,
        1,
        2,
        false,
    );
    let object = case.objects[0].clone();
    let config = case.job.accepted_config.clone();
    let scheduler = crate::runtime_host::daemon_dfs_mutation_scheduler(
        case.runtime.data_dir(case.service.node_id()),
    )
    .unwrap();
    // Retain the ACTUAL owner admission while testing this read-only page so
    // another component maintenance invocation cannot checkpoint its job.
    let admission = tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.admit(&crate::DfsMessageExecutionScope::GlobalExclusive),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        case.control.with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut())
                .placement_reconciliation(&case.job.file_system_id, &case.job.job_id)
                .unwrap()
                .is_none()
        }),
        "the unrelated initial proposal was never published"
    );
    let mut job = native_placement_job(
        config.clone(),
        &object.path,
        case.service.placement_content_candidates().unwrap(),
        "native-cancellable-noop-page".into(),
    );
    job.scope = DfsPlacementScope::SelfOnly;
    assert!(case.control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .compare_and_put_placement_update(&config, &config, &job)
            .unwrap()
    }));
    let read_job = || {
        case.control.with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut())
                .placement_reconciliation(&job.file_system_id, &job.job_id)
                .unwrap()
                .unwrap()
        })
    };
    let durable_before = read_job();
    let before = case
        .service
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    for _ in 0..3 {
        let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let first_call = case
            .runtime
            .rpc_observations()
            .iter()
            .map(|call| call.call_id)
            .max()
            .unwrap_or(0);
        case.runtime.install_fault(FaultRule {
            source: Some(case.service.node_id().clone()),
            target: Some(case.primary.clone()),
            route: Some(crate::DFS_DATA_NAMESPACE_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 1,
            action: FaultAction::PauseBeforeDispatch(gate.clone()),
        });
        let mut page = Box::pin(case.service.run_placement_reconciliation_bounded_async(1));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    result = page.as_mut() => panic!("paused native page unexpectedly completed: {result:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(2)) => {}
                }
                if case.runtime.rpc_observations().iter().any(|call| call.call_id > first_call && call.target == case.primary && call.route == crate::DFS_DATA_NAMESPACE_ROUTE && call.phase == RpcPhase::BeforeDispatch) { break; }
            }
        }).await.unwrap();
        drop(page);
        case.runtime.clear_faults();
        gate.add_permits(1);
        assert_eq!(
            read_job(),
            durable_before,
            "cancelled native lookup cannot checkpoint a page"
        );
        assert_eq!(
            case.service
                .current_read_object_metadata(&object.object_id)
                .unwrap(),
            object
        );
    }
    let scanning = case
        .service
        .run_placement_reconciliation_bounded_async(1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(scanning.state, DfsPlacementReconciliationState::Repairing);
    assert_eq!(
        scanning.scanned_object_count, 1,
        "cancelled tentative passes are not counted"
    );
    assert!(scanning.cursor.is_none());
    let completed = case
        .service
        .run_placement_reconciliation_bounded_async(1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.state, DfsPlacementReconciliationState::Completed);
    assert_eq!(completed.pending_repair_count, 0);
    assert_eq!(
        case.service
            .metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
    drop(admission);
    case.close();
}

#[test]
fn native_served_placement_yields_actual_global_owner_to_foreground_query() {
    use crate::test_runtime::{FaultAction, FaultRule};
    let mut case = NativePlacementFixture::new("native-placement-foreground-preemption", true, 1);
    let root_node = case.service.node_id().clone();
    let scheduler =
        crate::runtime_host::daemon_dfs_mutation_scheduler(case.runtime.data_dir(&root_node))
            .unwrap();
    let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    case.runtime.install_fault(FaultRule {
        source: Some(root_node.clone()),
        target: Some(case.primary.clone()),
        route: Some(crate::DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 1,
        action: FaultAction::PauseBeforeDispatch(gate.clone()),
    });
    let original_job = case.control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .placement_reconciliation(&case.job.file_system_id, &case.job.job_id)
            .unwrap()
            .unwrap()
    });
    let handle = case.runtime.runtime_handle();
    let root_shard = case.runtime.root_shard().unwrap();
    let first_call = case
        .runtime
        .rpc_observations()
        .iter()
        .map(|call| call.call_id)
        .max()
        .unwrap_or(0);
    let observe = case.runtime.rpc_observation_reader();
    let native = case.runtime;
    let native = std::thread::scope(|scope| {
        let worker_node = root_node.clone();
        let worker = scope.spawn(move || {
            let outcome = native.maintenance_tick(&worker_node, Duration::from_secs(5));
            (native, outcome)
        });
        handle.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if observe().iter().any(|call| call.call_id > first_call && call.target == case.primary && call.route == crate::DFS_DATA_NAMESPACE_ROUTE && call.phase == RpcPhase::BeforeDispatch) { break; }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }).await.expect("ordinary served maintenance reached its actual remote no-op lookup");
            assert!(scheduler.global_is_exclusively_held());
            scheduler.request_foreground_admission();
            let foreground = tokio::time::timeout(Duration::from_millis(500), scheduler.admit(&crate::DfsMessageExecutionScope::ExactShard { file_system_id: root_shard.file_system_id.clone(), shard_id: crate::DfsMetadataShardId::new(&root_shard.shard_id) })).await.expect("foreground query must acquire the normal owner before stalled RPC release").unwrap();
            let backend = case.service.metadata_backend_mut().unwrap();
            backend.begin_consistent_read_snapshot().unwrap();
            let query = crate::DfsMetadataShardQuery { file_system_id: root_shard.file_system_id.clone(), shard_id: crate::DfsMetadataShardId::new(&root_shard.shard_id), epoch: root_shard.epoch, selection: crate::DfsMetadataSelection::RetainedRevisions { namespace: Some("module.dfs.history".into()), path: Some("/unrelated".into()), object_id: None, after: None, limit: 128 } };
            let result = backend.query_file_history_on_shard(&query).unwrap();
            backend.end_consistent_read_snapshot();
            assert!(matches!(result, crate::DfsMetadataQueryResult::RetainedRevisions { entries, .. } if entries.is_empty()));
            assert!(observe().iter().any(|call| call.call_id > first_call && call.route == crate::DFS_DATA_NAMESPACE_ROUTE && call.phase == RpcPhase::EndedWithoutResponse), "foreground receives owner only after the actual pending lookup is cancelled");
            let after = case.control.with_client(|kv| DfsControlPlaneStore::new(kv.as_mut()).placement_reconciliation(&case.job.file_system_id, &case.job.job_id).unwrap().unwrap());
            assert_eq!(after.cursor, original_job.cursor, "a cancelled remote page cannot checkpoint its unconfirmed row");
            drop(foreground);
        });
        gate.add_permits(1);
        let (native, outcome) = worker.join().unwrap();
        outcome.unwrap();
        native
    });
    case.runtime = native;
    case.runtime.clear_faults();
    case.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn native_routed_noop_revalidates_live_permit_without_namespace_journal_churn() {
    let mut case = NativePlacementFixture::new("native-routed-placement-noop", false, 1);
    let object = case.objects[0].clone();
    let config = case.job.accepted_config.clone();
    let (resolved, replication, placement) =
        reconciliation_placement_for_object(&config, &case.job, &object).unwrap();
    let operation = DfsNamespaceOperation::ReconcilePlacement {
        job_id: case.job.job_id.clone(),
        path: object.path.clone(),
        object_id: object.object_id.clone(),
        expected_version: object.version.clone(),
        object_policy: resolved.policy.consistency,
        replication_policy: replication,
        desired_replica_node_ids: placement.plan.desired_replica_nodes,
    };
    let before = case
        .service
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    for request in ["native-routed-noop-1", "native-routed-noop-2"] {
        let result = case
            .service
            .route_placement_namespace_operation_to_metadata_primary_with_request_id_and_options(
                request.into(),
                object.file_system_id.clone(),
                case.primary.clone(),
                operation.clone(),
                Duration::from_secs(5),
                true,
                true,
            )
            .await
            .unwrap();
        assert_eq!(result.result, crate::DfsNamespaceResult::Applied);
        assert!(
            case.service
                .metadata_backend()
                .unwrap()
                .get_namespace_request(case.service.node_id(), request)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            case.service
                .metadata_backend()
                .unwrap()
                .journal_last_sequence()
                .unwrap(),
            before
        );
    }
    let (_, _, mut permit) = case
        .service
        .acquire_authoritative_placement_reconciliation_for_path(
            &case.job.file_system_id,
            &case.job.job_id,
            &object.path,
        )
        .unwrap()
        .unwrap();
    let rejected = case
        .service
        .route_placement_namespace_operation_to_metadata_primary_with_request_id_and_options(
            "native-routed-noop-2".into(),
            object.file_system_id.clone(),
            case.primary.clone(),
            operation,
            Duration::from_secs(5),
            true,
            true,
        )
        .await
        .unwrap();
    assert!(
        matches!(rejected.result, crate::DfsNamespaceResult::Rejected { .. }),
        "an old no-op reply cannot authorize through a currently held real consistency permit"
    );
    case.service
        .release_placement_authority_permit(&mut permit)
        .unwrap();
    assert_eq!(
        case.service
            .metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap(),
        before
    );
    case.close();
}

#[test]
fn native_served_maintenance_skips_parked_job_for_tail_and_retires_superseded_head() {
    let (runtime, nodes, jobs, gate, _, _) =
        super::native_repair_authority_tests::native_managed_work(
            "native-parked-placement-tail",
            &[("/", DfsPlacementScope::Subtree)],
        );
    let control = runtime.shared_control_plane().unwrap();
    let fs = runtime.file_system().file_system_id;
    let head = control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .placement_reconciliation(&fs, &jobs[0])
            .unwrap()
            .unwrap()
    });
    assert_eq!(head.state, DfsPlacementReconciliationState::Repairing);
    assert!(head.cursor.is_none());
    assert!(head.pending_repair_count > 0);
    let queue = crate::LocalDfsRepairQueueStore::new(
        crate::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&nodes[0])),
    );
    assert!(!queue.view().unwrap().records.is_empty());
    let mut client = runtime.client(&nodes[0]).unwrap();
    let config = control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .get_file_system_config(&fs)
            .unwrap()
            .unwrap()
    });
    let mut tail = native_placement_job(
        config.clone(),
        "/absent-native-placement-tail",
        client.placement_content_candidates().unwrap(),
        sinomo_core::Id::generate().to_string(),
    );
    tail.scope = DfsPlacementScope::SelfOnly;
    // Capture the actual committed rows before publishing Pending. Automatic
    // Main and explicit bounded turns may both advance this same job; a later
    // query must not erase evidence that Repairing was durably published first.
    let observed = std::sync::Arc::new(std::sync::Mutex::new(
        Vec::<DfsPlacementReconciliationJob>::new(),
    ));
    let omitted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let captured = std::sync::Arc::clone(&observed);
    let overflow = std::sync::Arc::clone(&omitted);
    let tail_key = crate::control_plane::placement_reconciliation_key(&fs, &tail.job_id);
    let subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_deltas(
        &sinomo_core::network::ClusterPaths::new(runtime.data_dir(&nodes[0])),
        move |delta| {
            for event in delta.events.iter().filter(|event| event.key == tail_key) {
                if let sinomo_core::kv::KvEventKind::Put { value } = &event.kind {
                    let row =
                        sinomo_core::kv::decode_kv_value::<DfsPlacementReconciliationJob>(value)
                            .expect("actual typed committed placement row");
                    let mut rows = captured.lock().unwrap();
                    if rows.len() < 16 {
                        rows.push(row);
                    } else {
                        overflow.fetch_add(1, std::sync::atomic::Ordering::Release);
                    }
                }
            }
        },
    );
    assert!(control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .compare_and_put_placement_update(&config, &config, &tail)
            .unwrap()
    }));
    // An empty scan still checkpoints Repairing before its verification pass
    // can complete. Drive both real phases within the original total budget.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut previous_revision = tail.revision;
    for expected in [
        DfsPlacementReconciliationState::Repairing,
        DfsPlacementReconciliationState::Completed,
    ] {
        let advanced = loop {
            assert_eq!(omitted.load(std::sync::atomic::Ordering::Acquire), 0);
            let advanced = observed
                .lock()
                .unwrap()
                .iter()
                .find(|row| row.state == expected && row.revision > previous_revision)
                .cloned();
            let parked = control.with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&fs, &head.job_id)
                    .unwrap()
                    .unwrap()
            });
            assert_eq!(
                parked, head,
                "pending durable repair work parks the head's object pass"
            );
            if let Some(advanced) = advanced {
                break advanced;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "tail must finish both actual phases within the original five-second budget; observed={:?}",
                observed.lock().unwrap()
            );
            let result = runtime.maintenance_tick(&nodes[0], remaining).unwrap();
            assert!(
                result.outcome.is_ok(),
                "ordinary served owner: {:?}",
                result.outcome
            );
            assert_eq!(result.omitted_kv_event_count, 0);
            std::thread::sleep(
                Duration::from_millis(1)
                    .min(deadline.saturating_duration_since(std::time::Instant::now())),
            );
        };
        assert!(advanced.revision > previous_revision);
        previous_revision = advanced.revision;
        assert_eq!(advanced.state, expected);
        assert_eq!(advanced.matched_object_count, 0);
        assert_eq!(advanced.pending_repair_count, 0);
    }
    assert!(std::time::Instant::now() < deadline);
    assert_eq!(omitted.load(std::sync::atomic::Ordering::Acquire), 0);
    let completed = control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .placement_reconciliation(&fs, &tail.job_id)
            .unwrap()
            .unwrap()
    });
    assert_eq!(completed.state, DfsPlacementReconciliationState::Completed);
    assert_eq!(completed.revision, previous_revision);
    assert_eq!(completed.matched_object_count, 0);
    assert_eq!(completed.pending_repair_count, 0);
    drop(subscription);
    let mut current = config.clone();
    upsert_placement_rule(
        &mut current.subtree_policies,
        normalize_placement_rule(
            "/native-managed-0",
            DfsPlacementScope::SelfOnly,
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap(),
        )
        .unwrap(),
    );
    assert!(control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .compare_and_put_file_system_config(Some(&config), &current)
            .unwrap()
    }));
    for _ in 0..8 {
        let result = runtime
            .maintenance_tick(&nodes[0], Duration::from_secs(5))
            .unwrap();
        assert_eq!(result.omitted_kv_event_count, 0);
        let retired = control.with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut())
                .placement_reconciliation(&fs, &head.job_id)
                .unwrap()
                .unwrap()
        });
        if retired.state.is_terminal() {
            assert!(
                retired
                    .last_error
                    .as_deref()
                    .unwrap()
                    .contains("superseded")
            );
            break;
        }
    }
    assert!(
        control
            .with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .placement_reconciliation(&fs, &head.job_id)
                .unwrap()
                .unwrap())
            .state
            .is_terminal()
    );
    drop(client);
    gate.add_permits(64);
    runtime.clear_faults();
    runtime.close().unwrap();
}
