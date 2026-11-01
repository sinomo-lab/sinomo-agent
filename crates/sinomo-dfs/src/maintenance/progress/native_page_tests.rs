//! Real native inventory/cursor control. No repair row, receipt, intent or ACK
//! is seeded. The finite current fixture earns both coprime page counts first.
use super::*;
use crate::test_runtime::{FaultAction, FaultRule, RpcPhase};
use crate::*;
use std::time::Instant;

fn intent_page_count(service: &NetworkedDfsService<DfsNodeTransport>, limit: usize) -> usize {
    let backend = service.metadata_backend().unwrap();
    let mut cursor = None;
    for count in 1..=128 {
        let page = backend
            .write_intent_maintenance_page(cursor.as_ref(), limit)
            .unwrap();
        if page.next_cursor.is_none() {
            return count;
        }
        cursor = page.next_cursor;
    }
    panic!("actual finite native intent inventory exceeded setup bound");
}
fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

fn native_case(name: &str, new_namespace_input: bool) {
    let root_path = format!("/services/{name}");
    let (native, mut writer, shard) =
        crate::test_support::current_native_metadata_service(name, 3, &root_path).unwrap();
    let local = shard.primary_node_id.clone();
    assert_eq!(writer.node_id(), &local);
    let data_dir = native.data_dir(&local).clone();
    // Registered Main ingress owns the ordinary write's repair capture and
    // physical queue checkpoint. No effect admission is held during SDK calls.
    writer.posix_mkdir_all(&root_path).unwrap();
    let job_root = format!("{root_path}/later-namespace");
    writer.posix_mkdir_all(&format!("{job_root}/a/b")).unwrap();
    // Select the existing command-less retained-cache preflight negative cut
    // after ordinary warmup. All SDK producers below must still complete and
    // earn the physical queue; any blocked foreground path is setup failure.
    let background = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
        &data_dir,
        Arc::new(|point, command, _| {
            if matches!(
                point,
                crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight
            ) && command.is_none()
            {
                return Err(DfsError::Cancelled {
                    operation: "negative competing maintenance before finite native page sweep",
                });
            }
            Ok(())
        }),
    );
    let peers = shard.replica_node_ids.clone();
    assert_eq!(peers.len(), 2);
    // Only real content mutation ingress fails. Metadata peers remain live;
    // actual native W1 commits must create the missing-target queue themselves.
    for peer in &peers {
        for route in [DFS_DATA_STAGE_BLOB_ROUTE, DFS_DATA_REPAIR_REPLICA_ROUTE] {
            native.install_fault(FaultRule {
                source: Some(local.clone()),
                target: Some(peer.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: None,
                remaining_hits: 32,
                action: FaultAction::TransientRouteFailureBeforeDispatch,
            });
        }
    }
    let candidates = std::iter::once(local.clone())
        .chain(peers.iter().cloned())
        .collect::<Vec<_>>();
    let mut written = Vec::new();
    for index in 0..9 {
        let path = format!("{root_path}/file-{index}.bin");
        let stat = writer
            .posix_write_file(
                &path,
                &[index as u8; 31],
                DfsWriteOptions {
                    policy: DfsReplicationPolicy::new(1, 3).unwrap(),
                    replica_candidates: candidates.clone(),
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        written.push(stat);
    }
    drop(writer);
    // Only the finite maintenance owner uses a direct effect opener. The SDK
    // has already completed its real Main checkpoint and is gone before this
    // current provider acquires exact native admission.
    let mut core = native.metadata_primary_service(&root_path).unwrap();
    assert_eq!(core.node_id(), &local);
    let execution = core
        .acquire_prepared_execution(&DfsPreparedOperationScope {
            file_system_id: shard.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(&shard.shard_id),
            owner_epoch: shard.epoch,
            operation_id: format!("native-bounded-phase-admission:{name}"),
        })
        .unwrap()
        .expect("real current fixture effect admission");
    let mut subjects = Vec::new();
    let mut receipt_facts = Vec::new();
    for stat in written {
        let intent = core
            .metadata_backend()
            .unwrap()
            .intents_for_object(&stat.object_id)
            .unwrap()
            .into_iter()
            .find(|intent| {
                intent.status == DfsWriteIntentStatus::Committed
                    && intent.target_version == stat.version
            })
            .unwrap();
        assert_eq!(intent.policy.min_commit_replicas, 1);
        assert_eq!(intent.desired_replica_node_ids.len(), 3);
        let receipts = core
            .metadata_backend()
            .unwrap()
            .receipts_for_version_page(
                &intent.object_id,
                &intent.target_version,
                None,
                crate::file_history_query::HISTORY_PAGE_LIMIT,
            )
            .unwrap();
        assert!(receipts.next_cursor.is_none(), "finite actual receipt cut");
        receipt_facts.push(serde_json::json!({"intent":&intent,"receipts":&receipts.receipts}));
        subjects.push(intent);
    }
    native
        .assert_fault_budgets()
        .expect("SETUP: real producer traffic must remain within each bounded content fault");
    let queue = LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(&data_dir));
    let before = queue.view().unwrap();
    // Observe the SAME queue through its ordinary page owner, including any
    // existing pending-restart rule. This never checkpoints scan completion or
    // manually resets a cursor. The sum bound requires its actual first page.
    let (first_page, _) = queue.maintenance_page(2, &[], None).unwrap();
    let rpc_facts = native
        .rpc_observations()
        .into_iter()
        .filter(|call| {
            call.source == local
                && matches!(
                    call.route.as_str(),
                    DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                        | DFS_DATA_STAGE_BLOB_ROUTE
                        | DFS_DATA_REPAIR_REPLICA_ROUTE
                )
        })
        .map(|call| format!("{call:?}"))
        .collect::<Vec<_>>();
    let setup_facts = serde_json::json!({"actual_head":before.head_sequence,
        "actual_records":&before.records,"actual_first_page":&first_page.records,
        "actual_first_page_has_more":first_page.has_more,"actual_subject_receipts":receipt_facts,
        "actual_rpc":rpc_facts,
        "qualification":"normal registered SDK write owner completed before effect admission; physical queue is observed, never seeded"});
    eprintln!("BOUNDED_NATIVE_SETUP name={name} facts={setup_facts}");
    let _ = std::fs::write(
        data_dir.root().join("bounded-phase-setup.json"),
        serde_json::to_vec_pretty(&setup_facts).unwrap(),
    );
    assert_eq!(
        before.records.len(),
        18,
        "actual nine W1/N3 producers each leave two missing replicas"
    );
    assert_eq!(
        first_page.records.as_slice(),
        &before.records[..2],
        "SETUP: actual finite repair sweep must start at the native ordered beginning"
    );
    assert!(first_page.has_more, "SETUP: actual bounded repair page");
    assert!(
        before
            .records
            .iter()
            .all(|row| subjects
                .iter()
                .any(|intent| intent.intent_id == row.intent_id
                    && intent.object_id == row.object_id
                    && intent.target_version == row.version
                    && intent.content_hash.as_deref() == Some(row.content_hash.as_str())))
    );
    let mut options = dfs_resident_routine_maintenance_tick_options();
    options.write_intent_max_records = 2;
    options.repair_max_jobs = 2;
    options.publish_metadata = true;
    let intent_pages = intent_page_count(&core, options.write_intent_max_records);
    let repair_pages = before.records.len().div_ceil(options.repair_max_jobs);
    assert!(
        intent_pages > 1 && repair_pages > 1 && gcd(intent_pages, repair_pages) == 1,
        "native setup must earn coprime finite inventories: {intent_pages}/{repair_pages}"
    );
    let turn_bound = intent_pages + repair_pages - 1;
    assert!(intent_pages * repair_pages > turn_bound);
    let mut host = LocalDfsMaintenanceHost::new(data_dir.clone());
    let mut publisher = LocalDfsMetadataJournalPublisherHost::new(data_dir.clone());
    let (_, peer_transport) = native.peer_transports(&local).unwrap();
    let mut transport = AsyncDfsMetadataNodeTransport::new(
        peers.clone(),
        async_dfs_metadata_journal_batch_sender_from_peer_transport(
            &data_dir,
            Arc::clone(&peer_transport),
        ),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
    );
    let exclusions = DfsMaintenanceExecutionExclusions::new(peers.iter().cloned());
    let started = Instant::now();
    let previous = core.set_transport_operation_deadline(Some(
        started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    ));
    let mut reports = Vec::new();
    let mut eof_seen = false;
    let mut completed = false;
    let mut job: Option<DfsNamespaceJobRecord> = None;
    let mut new_job_advanced_before_repair_eof = false;
    let mut partial_count = 0;
    for turn_index in 0..=turn_bound {
        let result = crate::test_runtime::executor().block_on(
            super::super::run_dfs_maintenance_turn_with_async_host_and_execution_exclusions(
                &mut core,
                &mut host,
                &data_dir,
                sifs_file_system_id(),
                &local,
                vec![shard.clone()],
                &mut publisher,
                &mut transport,
                &exclusions,
                None,
                options,
                true,
            ),
        );
        // Preserve actual diagnostics before any business assertion; a failure
        // in this finite owner must not become an invented completion/sample.
        reports.push(format!("{result:?}"));
        eprintln!(
            "BOUNDED_NATIVE_PAGE name={name} turn={turn_index} intent_pages={intent_pages} repair_pages={repair_pages} result={result:?}"
        );
        let _ = std::fs::write(
            data_dir.root().join("bounded-phase-progress.json"),
            serde_json::to_vec_pretty(&serde_json::json!({"intent_pages":intent_pages,
                "repair_pages":repair_pages,"turn_bound":turn_bound,"reports":reports}))
            .unwrap(),
        );
        native
            .assert_fault_budgets()
            .expect("SETUP: actual page traffic must not exhaust the bounded content faults");
        let report = result
            .expect("actual current native phase call")
            .expect("no busy source in owned finite fixture");
        let more = dfs_maintenance_report_reached_limit(&report);
        let cursor = core.write_intent_maintenance_scan.cursor().unwrap();
        if eof_seen {
            assert!(
                cursor.is_none(),
                "a completed intent inventory must stay at EOF while the repair sweep continues"
            );
        } else if cursor.is_none() {
            eof_seen = true;
        }
        if let Some(before_job) = job.as_ref() {
            let after_job = core.namespace_job(&before_job.job_id).unwrap().unwrap();
            new_job_advanced_before_repair_eof |= report.repair_page_checkpoint.has_more
                && (after_job.status != before_job.status
                    || after_job.processed_object_count > before_job.processed_object_count
                    || after_job.pending_frames != before_job.pending_frames);
        }
        if new_namespace_input && job.is_none() && report.repair_budget_exhausted {
            let accepted = core
                .posix_clear_dir_recursive_bounded(&job_root, 0)
                .unwrap();
            assert_eq!(accepted.status, DfsNamespaceJobStatus::Running);
            job = Some(accepted);
        }
        if more {
            partial_count += 1;
        }
        if !core.maintenance_progress.has_unfinished_turn().unwrap() && !more {
            completed = true;
            break;
        }
        assert!(
            turn_index < turn_bound,
            "independent bounded cursors must not wrap until joint EOF"
        );
    }
    core.set_transport_operation_deadline(previous);
    let after = queue.view().unwrap();
    let evidence = serde_json::json!({"intent_pages":intent_pages,"repair_pages":repair_pages,
        "old_joint_cycle":intent_pages*repair_pages,"turn_bound":turn_bound,"reports":reports,
        "actual_before":{"head_sequence":before.head_sequence,"records":&before.records},
        "actual_after":{"head_sequence":after.head_sequence,"records":&after.records},
        "completed":completed,"partial_count":partial_count,
        "new_namespace_input":job,"new_job_advanced_before_repair_eof":new_job_advanced_before_repair_eof,
        "qualification":"real native W1/N3 write producers, actual default queue and actual Current effect admission; finite inventory/cursor control, not Docker performance acceptance"});
    std::fs::write(
        data_dir.root().join("bounded-phase-pages.json"),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
    assert_eq!(
        after.records.len(),
        before.records.len(),
        "finishing an inventory cannot retire missing replicas or carrier DATA"
    );
    assert!(completed && eof_seen && partial_count > 0);
    assert!(reports.len() <= turn_bound);
    assert!(
        !new_namespace_input || new_job_advanced_before_repair_eof,
        "new real namespace work must progress while the repair sweep still has pages"
    );
    assert!(
        after
            .records
            .iter()
            .all(|row| before
                .records
                .iter()
                .any(|prior| prior.intent_id == row.intent_id
                    && prior.object_id == row.object_id
                    && prior.version == row.version
                    && prior.content_hash == row.content_hash
                    && prior.missing_replica_node_id == row.missing_replica_node_id))
    );
    assert!(
        !native
            .rpc_observations()
            .iter()
            .any(|call| peers.contains(&call.target)
                && matches!(
                    call.route.as_str(),
                    DFS_DATA_STAGE_BLOB_ROUTE | DFS_DATA_REPAIR_REPLICA_ROUTE
                )
                && call.phase == RpcPhase::Decoded),
        "negative content routes cannot create a target ACK"
    );
    drop(transport);
    drop(publisher);
    drop(host);
    drop(queue);
    drop(execution);
    drop(core);
    drop(background);
    native.close().unwrap();
}

#[test]
fn native_coprime_intent_and_repair_pages_finish_without_rewrapping() {
    native_case("native-coprime-maintenance-pages", false);
}
#[test]
fn native_new_namespace_work_progresses_during_unfinished_repair_sweep() {
    native_case("native-page-new-namespace-input", true);
}
