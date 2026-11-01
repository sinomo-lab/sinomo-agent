use super::*;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

type ReadResult = (std::result::Result<DfsStat, DfsPosixError>, Option<Instant>);

struct OwnedStatRead {
    gate: Arc<Semaphore>,
    task: Option<std::thread::JoinHandle<ReadResult>>,
}
impl OwnedStatRead {
    fn join(&mut self) -> ReadResult {
        self.task
            .take()
            .expect("one owned stat call")
            .join()
            .unwrap()
    }
}
impl Drop for OwnedStatRead {
    fn drop(&mut self) {
        // Unwind releases the real completed response and joins the same caller;
        // no replacement request, successful response or authority is supplied.
        self.gate.close();
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
    }
}

fn actual_result(frame: &DfsRoutedMessage) -> &DfsMetadataQueryResult {
    let DfsNetworkMessage::NamespaceResponse(response) = &frame.message else {
        panic!("actual stat capture must be a namespace response")
    };
    let Some(DfsPosixCoordinationResult::MetadataQuery(query)) =
        &response.posix_coordination_result
    else {
        panic!("actual stat capture must carry its typed query")
    };
    &query.result
}

fn initial_pages(frames: &[DfsRoutedMessage]) -> Vec<&DfsStatProjectionPage> {
    frames
        .iter()
        .filter_map(|frame| match actual_result(frame) {
            DfsMetadataQueryResult::StatProjectionV1 { page }
                if !page.request.fence_only && page.request.expected_prefix.is_none() =>
            {
                Some(page.as_ref())
            }
            _ => None,
        })
        .collect()
}

fn native_cut(
    runtime: &DfsTestRuntime,
    shard: &DfsMetadataShardConfig,
    after: u64,
) -> (DfsMetadataPrefixProof, Vec<DfsPreparedOperationRecord>) {
    let observer = runtime.metadata_primary_service("/services").unwrap();
    assert_eq!(observer.node_id, shard.primary_node_id);
    let backend = observer.metadata_backend.as_ref().unwrap();
    let id = DfsMetadataShardId::new(shard.shard_id.clone());
    // This existing read-only export owns tables and proof from ONE live
    // materialized image. Never ask a stat pin for journal/writer custody.
    let snapshot = backend.export_shard_table_snapshot(&id).unwrap().unwrap();
    assert!(snapshot.prefix_proof.is_well_formed());
    assert_eq!(
        snapshot.materialized_through_sequence,
        snapshot.prefix_proof.journal_sequence
    );
    assert_eq!(
        snapshot.semantic_state_fingerprint,
        crate::metadata_records::dfs_metadata_table_snapshot_state_fingerprint(&snapshot).unwrap()
    );
    let mut records = Vec::new();
    for record in &snapshot.tables.prepared_operations_v7 {
        let Some(commit) = record.committed_entry.as_ref().filter(|commit| {
            commit.sequence > after && commit.sequence <= snapshot.materialized_through_sequence
        }) else {
            continue;
        };
        record.validate().unwrap();
        assert_eq!(record.status, DfsPreparedOperationStatus::Committed);
        assert_eq!(record.digest.file_system_id, shard.file_system_id);
        assert_eq!(record.digest.shard_id, id);
        assert_eq!(record.digest.owner_epoch, shard.epoch);
        assert_eq!(commit.digest, record.digest);
        assert_eq!(commit.shard_id, id);
        records.push(record.clone());
    }
    (snapshot.prefix_proof, records)
}

fn run_real_prefix_advances(name: &'static str, advances: usize) {
    assert!(matches!(
        advances,
        1 | super::super::super::POSIX_ROUTED_SYNC_RETRY_ROUNDS
    ));
    let mut cluster = SplitAuthorities::new(name);
    let target = "/stat-native-concurrency-target";
    let parent = "/services/stat-native-concurrency-writes";
    cluster.caller.posix_mkdir_all(parent).unwrap();
    cluster
        .caller
        .posix_write_file(
            target,
            b"the same immutable stat target",
            DfsWriteOptions::replicated_eventual(DfsTestRuntime::ordered_node_ids(name, 3)),
        )
        .unwrap();
    let baseline = cluster.caller.posix_stat_distributed(target).unwrap();
    assert_eq!(baseline.link_count, 1);
    let mut writer = cluster
        .runtime
        .client(&cluster.services.primary_node_id)
        .unwrap();
    let mut reader = cluster.runtime.client(&cluster.caller.node_id).unwrap();
    let captured = Arc::new(Mutex::new(Vec::new()));
    let gate = Arc::new(Semaphore::new(0));
    cluster.runtime.install_fault(FaultRule {
        source: Some(reader.node_id.clone()),
        target: Some(cluster.services.primary_node_id.clone()),
        route: Some(DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: advances,
        action: FaultAction::PauseStatProjectionReplyAfterDispatch {
            object_id: baseline.object_id.clone(),
            gate: Arc::clone(&gate),
            captured: Arc::clone(&captured),
        },
    });
    let action_started = Instant::now();
    let original_deadline = action_started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    reader.set_transport_operation_deadline(Some(original_deadline));
    let mut read = OwnedStatRead {
        gate: Arc::clone(&gate),
        task: Some(std::thread::spawn(move || {
            let result = reader.posix_stat_distributed(target);
            (result, reader.transport.operation_deadline())
        })),
    };
    let mut publications = Vec::new();
    for round in 0..advances {
        let page = loop {
            {
                let frames = captured.lock().unwrap();
                if let Some(page) = initial_pages(&frames).get(round) {
                    break DfsStatProjectionPage::clone(page);
                }
            }
            assert!(
                Instant::now() < original_deadline,
                "actual initial page prerequisite unreached"
            );
            assert!(
                !read.task.as_ref().unwrap().is_finished(),
                "stat ended before actual page pause"
            );
            std::thread::sleep(Duration::from_millis(2));
        };
        assert_eq!(page.request.object_id, baseline.object_id);
        assert_eq!(page.request.object_version, baseline.version);
        assert!(
            page.object.is_none(),
            "Services must supply genuine foreign-scope absence"
        );
        let (before, _) = native_cut(
            &cluster.runtime,
            &cluster.services,
            page.prefix.journal_sequence,
        );
        assert!(before.journal_sequence >= page.prefix.journal_sequence);
        let path = format!("{parent}/actual-publication-{round}");
        // The ordinary admitted Main producer authors this unique namespace
        // operation. It is not a direct table append, fake reply or stamp edit.
        let made = writer.posix_mkdir(&path).unwrap();
        let (after, records) =
            native_cut(&cluster.runtime, &cluster.services, before.journal_sequence);
        assert!(after.journal_sequence > before.journal_sequence);
        let committed = records
            .iter()
            .map(|record| record.digest.clone())
            .collect::<Vec<_>>();
        assert!(
            !committed.is_empty(),
            "real same-shard native commit premise required"
        );
        publications.push(serde_json::json!({
            "round": round + 1, "paused_page": page, "native_before": before,
            "native_after": after, "ordinary_mkdir_path": path,
            "ordinary_mkdir_result": format!("{made:?}"),
            "actual_native_committed_digests": committed, "actual_native_committed_records": records,
        }));
        assert!(!gate.is_closed());
        gate.add_permits(1);
    }
    let (result, restored_deadline) = read.join();
    let frames = captured.lock().unwrap().clone();
    let pages = initial_pages(&frames);
    for frame in &frames {
        assert_eq!(frame.source_node_id, cluster.services.primary_node_id);
        assert_eq!(frame.target_node_id, cluster.caller.node_id);
        let DfsNetworkMessage::NamespaceResponse(reply) = &frame.message else {
            unreachable!()
        };
        let Some(DfsPosixCoordinationResult::MetadataQuery(query)) =
            &reply.posix_coordination_result
        else {
            unreachable!()
        };
        assert_eq!(query.file_system_id, cluster.services.file_system_id);
        assert_eq!(query.shard_id.0, cluster.services.shard_id);
        assert_eq!(query.epoch, cluster.services.epoch);
    }
    let changed = frames
        .iter()
        .filter_map(|frame| match actual_result(frame) {
            DfsMetadataQueryResult::StatProjectionChangedV1 {
                request,
                current_prefix,
            } => Some((request.as_ref(), current_prefix)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let evidence = serde_json::json!({
        "qualification": "actual normal3 split-owner native read-concurrency control; no Docker or production-fix attribution",
        "advances": advances, "existing_read_rounds": super::super::super::POSIX_ROUTED_SYNC_RETRY_ROUNDS,
        "action_elapsed_us": action_started.elapsed().as_micros(),
        "original_deadline_restored": restored_deadline == Some(original_deadline),
        "actual_publications": publications, "actual_receiver_responses": frames,
        "read_result": format!("{result:?}"),
    });
    let export = std::fs::write(
        cluster
            .runtime
            .data_dir(&cluster.caller.node_id)
            .root()
            .join("stat-native-prefix-concurrency.json"),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    );
    if let Err(error) = &export {
        eprintln!("stat concurrency evidence export failed: {error}");
    }
    assert_eq!(restored_deadline, Some(original_deadline));
    assert_eq!(
        changed.len(),
        advances,
        "each real advancement must cause its typed refusal"
    );
    assert!(pages.len() >= advances);
    for (index, (request, current)) in changed.iter().enumerate() {
        assert!(request.fence_only);
        assert!(current.is_well_formed());
        assert_eq!(request.expected_prefix.as_ref(), Some(&pages[index].prefix));
        assert!(current.journal_sequence > pages[index].prefix.journal_sequence);
    }
    if advances == 1 {
        let stat =
            result.expect("existing read rounds must recover one genuine intervening publication");
        assert_eq!(stat.object_id, baseline.object_id);
        assert_eq!(stat.version, baseline.version);
        assert_eq!(stat.content_hash, baseline.content_hash);
        assert_eq!(stat.link_count, 1);
        assert_eq!(pages.len(), 2);
    } else {
        assert_eq!(
            pages.len(),
            advances,
            "no extra read rounds may be invented"
        );
        assert!(
            matches!(
                result,
                Err(DfsPosixError::Dfs(DfsError::MetadataRejection {
                    code: DfsMetadataRejectionCode::PrefixMismatch,
                    ..
                }))
            ),
            "unchanged read rounds must refuse an unstabilized native cut"
        );
    }
    export.expect("stat concurrency evidence retained after semantic assertions");
    drop(read);
    cluster.runtime.clear_faults();
    drop(writer);
    cluster.close();
}

#[test]
fn native_stat_one_real_prefix_advance_uses_existing_read_rounds() {
    run_real_prefix_advances("stat-one-real-native-prefix-advance", 1);
}

#[test]
fn native_stat_four_real_prefix_advances_preserve_bounded_refusal() {
    run_real_prefix_advances(
        "stat-four-real-native-prefix-advances",
        super::super::super::POSIX_ROUTED_SYNC_RETRY_ROUNDS,
    );
}
