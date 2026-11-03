use super::*;
use crate::DfsMetadataBackend as _;
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcPhase};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn native_first_page_checkpoints_scanning_before_dispatch_and_survives_coalescing() {
    let label = "native-placement-start-checkpoint";
    let nodes = DfsTestRuntime::ordered_node_ids(label, 2);
    let coordinator = nodes[0].clone();
    let primary = nodes[1].clone();
    let mut runtime = DfsTestRuntime::new(label, coordinator.clone()).unwrap();
    runtime.define_node(primary.clone()).unwrap();
    runtime.start_nodes().unwrap();
    let assignment = runtime
        .reassign_metadata_shard(
            &crate::sifs_file_system_id(),
            "services",
            primary.clone(),
            vec![coordinator.clone()],
        )
        .unwrap();
    let directory = "/services/native-start-checkpoint";
    let path = format!("{directory}/file.bin");
    let mut writer = runtime.client(&primary).unwrap();
    writer.posix_mkdir_all(directory).unwrap();
    let written = writer
        .posix_write_file(
            &path,
            b"first-page ownership survives cancellation before remote dispatch",
            crate::DfsWriteOptions {
                policy: DfsReplicationPolicy::new(1, 2).unwrap(),
                replica_candidates: nodes.clone(),
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    drop(writer);
    // W1 acknowledges the original write before N2 necessarily settles. Read
    // actual durable receipts without acquiring a competing browser effect
    // lane; any continuation belongs to the registered Main maintenance owner.
    let settlement_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let expected_nodes = nodes.iter().cloned().collect::<BTreeSet<_>>();
    let shard_id = crate::DfsMetadataShardId::new(&assignment.shard_id);
    let object = loop {
        assert!(
            Instant::now() < settlement_deadline,
            "original W1/N2 write did not reach its actual N2 setup prerequisite"
        );
        let inspect = crate::LocalSobjMetadataBackend::open_for_inspection(
            shard_id.clone(),
            crate::local_dfs_metadata_shard_path(runtime.data_dir(&primary), &shard_id),
        )
        .unwrap();
        let object = inspect.get_object(&written.object_id).unwrap().unwrap();
        assert_eq!(object.version, written.version);
        assert_eq!(object.content_hash, written.content_hash);
        let version = inspect
            .get_version(&written.object_id, &written.version)
            .unwrap()
            .unwrap();
        let policy = version.content_durability.unwrap().policy;
        assert_eq!(
            (policy.min_commit_replicas, policy.desired_replicas),
            (1, 2)
        );
        let receipts = inspect
            .receipts_for_version(&written.object_id, &written.version)
            .unwrap();
        drop(inspect);
        let received = receipts
            .iter()
            .filter(|row| {
                row.receipt.file_system_id == written.file_system_id
                    && row.receipt.object_id == written.object_id
                    && row.receipt.committed_version == written.version
                    && row.content_hash == written.content_hash
            })
            .map(|row| row.replica_node_id.clone())
            .collect::<BTreeSet<_>>();
        if received == expected_nodes
            && object
                .current_locations
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
                == expected_nodes
        {
            assert!(
                Instant::now() < settlement_deadline,
                "N2 observed after the original setup deadline is not readiness"
            );
            break object;
        }
        let remaining = settlement_deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "original N2 setup deadline exhausted");
        let maintenance =
            tokio::task::block_in_place(|| runtime.maintenance_tick(&primary, remaining)).unwrap();
        assert!(
            maintenance.outcome.is_ok(),
            "actual Main continuation of original write failed: {:?}",
            maintenance.outcome
        );
        tokio::task::yield_now().await;
    };
    // Use the real authored cut and physical acknowledgements, not a routed
    // reply treated as a substitute for replica publication.
    let mut source = runtime.metadata_primary_service(&path).unwrap();
    let target = source.capture_metadata_quorum_sync_target(&object).unwrap();
    let certificate = source
        .sync_metadata_shard_to_quorum(&target, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert_eq!(
        certificate
            .acknowledged_node_ids
            .into_iter()
            .collect::<BTreeSet<_>>(),
        nodes.iter().cloned().collect::<BTreeSet<_>>()
    );
    drop(source);
    let mut service = runtime.client(&coordinator).unwrap();
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_object(&object.object_id)
            .unwrap(),
        Some(object.clone())
    );
    let config = service.current_placement_file_system_config().unwrap();
    let observation_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let (candidates, snapshot) = loop {
        let candidates = service.placement_content_candidates().unwrap();
        if candidates
            .iter()
            .map(|node| node.node_id.clone())
            .collect::<BTreeSet<_>>()
            == nodes.iter().cloned().collect::<BTreeSet<_>>()
            && let Some(snapshot) = service.placement_snapshot_cache.snapshot()
            && snapshot.primary_node_id == coordinator
            && !snapshot.nodes.is_empty()
            && time::OffsetDateTime::now_utc() <= snapshot.expires_at
        {
            break (candidates, (*snapshot).clone());
        }
        assert!(
            Instant::now() < observation_deadline,
            "actual native placement observation unavailable"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    let control = runtime.shared_control_plane().unwrap();
    let scheduler =
        crate::runtime_host::daemon_dfs_mutation_scheduler(runtime.data_dir(&coordinator)).unwrap();
    // Hold the existing owner admission so automatic maintenance cannot advance
    // the measured job while the explicit native page is being cancelled.
    let admission = tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.admit(&crate::DfsMessageExecutionScope::GlobalExclusive),
    )
    .await
    .unwrap()
    .unwrap();
    let make_job = || {
        crate::control_plane::build_adaptive_placement_reconciliation_job(
            sinomo_core::Id::generate().to_string(),
            &config,
            path.clone(),
            DfsPlacementScope::SelfOnly,
            BTreeSet::new(),
            candidates.clone(),
            snapshot.clone(),
            crate::DfsAdaptivePlacementBudget::default(),
            0,
            dfs_now_unix_nanos(),
        )
        .unwrap()
    };
    let job = make_job();
    assert!(control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .compare_and_put_placement_update(&config, &config, &job)
            .unwrap()
    }));
    let read_original = || {
        control.with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut())
                .placement_reconciliation(&job.file_system_id, &job.job_id)
                .unwrap()
                .unwrap()
        })
    };
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let first_call = runtime
        .rpc_observations()
        .iter()
        .map(|call| call.call_id)
        .max()
        .unwrap_or(0);
    runtime.install_fault(FaultRule {
        source: Some(coordinator.clone()),
        target: Some(primary.clone()),
        route: Some(crate::DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 1,
        action: FaultAction::PauseBeforeDispatch(gate.clone()),
    });
    let mut page = Box::pin(service.run_placement_reconciliation_bounded_async(1));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                result = page.as_mut() => panic!("paused native page unexpectedly completed: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(2)) => {}
            }
            if runtime.rpc_observations().iter().any(|call| {
                call.call_id > first_call
                    && call.source == coordinator
                    && call.target == primary
                    && call.route == crate::DFS_DATA_NAMESPACE_ROUTE
                    && call.phase == RpcPhase::BeforeDispatch
            }) {
                break;
            }
        }
    })
    .await
    .unwrap();
    let at_dispatch = read_original();
    drop(page);
    runtime.clear_faults();
    gate.add_permits(1);
    let after_cancel = read_original();

    // A second admitted fixture job exercises the ordinary coalescer using the
    // same captured real config, roster and snapshot. This is a cancellation
    // control, not evidence of a new production observation or quiet-window activity.
    let newer = make_job();
    assert!(newer.created_at_unix_nanos > job.created_at_unix_nanos);
    assert!(control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .compare_and_put_placement_update(&config, &config, &newer)
            .unwrap()
    }));
    let selected = control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .next_placement_reconciliation(&job.file_system_id, &config)
            .unwrap()
    });
    let after_coalescing = read_original();
    // The before implementation may already have terminalized this exact ID.
    // Never revive it or run the replacement as if it recovered the original.
    let recovered = if selected
        .as_ref()
        .is_some_and(|next| next.job_id == job.job_id)
        && after_coalescing.state == DfsPlacementReconciliationState::Scanning
    {
        let scanned = service
            .run_placement_reconciliation_bounded_async(1)
            .await
            .unwrap()
            .unwrap();
        let completed = service
            .run_placement_reconciliation_bounded_async(1)
            .await
            .unwrap()
            .unwrap();
        Some((scanned, completed))
    } else {
        None
    };
    let final_object = service
        .current_read_object_metadata(&object.object_id)
        .unwrap();
    drop(admission);
    drop(service);
    drop(control);
    runtime.close().unwrap();

    eprintln!(
        "NATIVE_START_CHECKPOINT job={} dispatch_state={:?} dispatch_revision={} cancel_state={:?} coalesced_state={:?} selected={:?}",
        job.job_id,
        at_dispatch.state,
        at_dispatch.revision,
        after_cancel.state,
        after_coalescing.state,
        selected.as_ref().map(|next| &next.job_id)
    );
    assert_eq!(
        at_dispatch.state,
        DfsPlacementReconciliationState::Scanning,
        "Scanning must already be durable before the first actual remote dispatch"
    );
    assert_eq!(at_dispatch.revision, job.revision + 1);
    assert_eq!(at_dispatch.scanned_object_count, 0);
    assert!(at_dispatch.cursor.is_none());
    assert_eq!(
        after_cancel, at_dispatch,
        "cancellation must preserve the start checkpoint"
    );
    assert_eq!(
        after_coalescing, at_dispatch,
        "new Pending work cannot retire the started job"
    );
    assert_eq!(selected.unwrap().job_id, job.job_id);
    let (scanned, completed) =
        recovered.expect("the original cancelled job must remain executable");
    assert_eq!(scanned.job_id, job.job_id);
    assert_eq!(scanned.scanned_object_count, 1);
    assert!(scanned.last_error.is_none());
    assert_eq!(completed.job_id, job.job_id);
    assert_eq!(completed.state, DfsPlacementReconciliationState::Completed);
    assert_eq!(completed.pending_repair_count, 0);
    assert_eq!(final_object, object);
}
