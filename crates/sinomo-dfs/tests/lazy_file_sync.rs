use sinomo_dfs::{
    DfsPosixSyncKind, DfsReplicationPolicy, DfsWriteOptions, test_support::DfsTestRuntime,
};

#[test]
fn native_file_sync_certifies_actual_receipts_and_rejects_another_content_hash() {
    let nodes = DfsTestRuntime::ordered_node_ids("native-file-sync-evidence", 3);
    let mut runtime = DfsTestRuntime::new("native-file-sync-evidence", nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let path = "/native-file-sync-evidence.bin";
    let mut service = runtime.client(&nodes[0]).unwrap();
    service
        .posix_write_file(
            path,
            b"actual durable bytes",
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: nodes,
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let mut target = service
        .posix_capture_file_sync_target(path, DfsPosixSyncKind::File)
        .unwrap();
    let certificate = service
        .posix_sync_file(path, DfsPosixSyncKind::File)
        .unwrap();
    assert_eq!(certificate.content_evidence.required_receipts, 2);
    assert!(certificate.content_evidence.receipt_node_ids.len() >= 2);
    target.content_hash = "0".repeat(64);
    assert!(
        service
            .posix_validate_file_sync_target_local(&target)
            .is_err()
    );
    drop(service);
    runtime.close().unwrap();
}
