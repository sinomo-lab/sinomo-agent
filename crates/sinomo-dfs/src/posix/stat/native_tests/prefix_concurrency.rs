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

// Hold an actual completed owner read while ordinary publications advance its
// shard. A single-owner stat must return that captured record without requiring
// a quiet interval or asking an unrelated authority for aliases.
fn concurrent_owner_writes(name: &str, no_links: bool, writes: usize, target_link: bool) {
    let mut cluster = SplitAuthorities::new(name);
    let target = "/services/maintained-count-stat";
    let parent = "/services/maintained-count-writes";
    cluster.caller.posix_mkdir_all(parent).unwrap();
    cluster
        .caller
        .posix_write_file(
            target,
            b"stable target",
            DfsWriteOptions::replicated_eventual(DfsTestRuntime::ordered_node_ids(name, 3)),
        )
        .unwrap();
    let baseline = cluster.caller.posix_stat_distributed(target).unwrap();
    let root_reads = cluster
        .runtime
        .capture_namespace_requests(&cluster.root.primary_node_id, 32)
        .unwrap();
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
        remaining_hits: 1,
        action: FaultAction::PauseStatMetadataReplyAfterDispatch {
            object_id: baseline.object_id.clone(),
            gate: Arc::clone(&gate),
            captured: Arc::clone(&captured),
        },
    });
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    reader.set_transport_operation_deadline(Some(deadline));
    let mut read = OwnedStatRead {
        gate: Arc::clone(&gate),
        task: Some(std::thread::spawn(move || {
            let result = if no_links {
                reader.posix_open_beneath_no_links_distributed("/", target)
            } else {
                reader.posix_stat_distributed(target)
            };
            (result, reader.transport.operation_deadline())
        })),
    };
    loop {
        if !captured.lock().unwrap().is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "actual owner read was not reached"
        );
        assert!(
            !read.task.as_ref().unwrap().is_finished(),
            "read ended before pause"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    let cut = || {
        let service = cluster.runtime.metadata_primary_service(target).unwrap();
        service
            .metadata_backend
            .as_ref()
            .unwrap()
            .export_shard_table_snapshot(&DfsMetadataShardId::new(
                cluster.services.shard_id.clone(),
            ))
            .unwrap()
            .unwrap()
            .prefix_proof
    };
    let before = cut();
    for index in 0..writes {
        if target_link {
            writer
                .posix_hardlink(target, &format!("{parent}/alias-{index}"))
                .unwrap();
        } else {
            writer
                .posix_mkdir(&format!("{parent}/publication-{index}"))
                .unwrap();
        }
    }
    let after = cut();
    assert!(
        after.journal_sequence > before.journal_sequence,
        "native writes must actually publish"
    );
    gate.add_permits(1);
    let (result, restored) = read.join();
    let stat = result.expect("adjacent publications cannot invalidate a captured inode");
    assert_eq!(restored, Some(deadline));
    assert_eq!(stat.object_id, baseline.object_id);
    assert_eq!(stat.version, baseline.version);
    assert_eq!(stat.link_count, 1);
    let frames = captured.lock().unwrap();
    assert_eq!(frames.len(), 1, "no post-read fence or retry");
    assert!(matches!(
        actual_result(&frames[0]),
        DfsMetadataQueryResult::StatAtPathV2 { .. } | DfsMetadataQueryResult::StatObjectV1 { .. }
    ));
    for frame in root_reads.records() {
        if let DfsNetworkMessage::NamespaceRequest(request) = &frame.message {
            assert!(
                !matches!(&request.operation,
                DfsNamespaceOperation::MetadataQuery { query }
                    if matches!(query.selection, DfsMetadataSelection::StatProjectionV1 { .. }
                        | DfsMetadataSelection::StatAtPathV1 { .. })),
                "ordinary stat must not collect a global projection"
            );
        }
    }
    drop(frames);
    drop(root_reads);
    drop(writer);
    drop(read);
    if target_link {
        assert_eq!(
            cluster
                .caller
                .posix_stat_distributed(target)
                .unwrap()
                .link_count,
            1 + writes as u64
        );
    }
    cluster.close();
}

#[test]
fn native_stat_survives_one_adjacent_publication_without_revalidation() {
    concurrent_owner_writes("count-stat-one-publication", false, 1, false);
}
#[test]
fn native_stat_survives_four_adjacent_publications_without_revalidation() {
    concurrent_owner_writes("count-stat-four-publications", false, 4, false);
}
#[test]
fn native_no_links_stat_survives_one_adjacent_publication_without_revalidation() {
    concurrent_owner_writes("count-no-links-one-publication", true, 1, false);
}
#[test]
fn native_no_links_stat_survives_four_adjacent_publications_without_revalidation() {
    concurrent_owner_writes("count-no-links-four-publications", true, 4, false);
}

#[test]
fn native_stat_returns_captured_inode_and_next_read_observes_acknowledged_link() {
    concurrent_owner_writes("count-stat-target-link", false, 1, true);
}
