use sinomo_dfs::{DfsReplicationPolicy, DfsWriteOptions, test_support::DfsTestRuntime};

#[test]
fn stale_conditional_replace_cannot_advance_native_metadata() {
    let nodes = DfsTestRuntime::ordered_node_ids("native-stale-writer", 3);
    let mut runtime = DfsTestRuntime::new("native-stale-writer", nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let path = "/native-stale-writer.bin";
    let mut writer = runtime.client(&nodes[0]).unwrap();
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(2, 2).unwrap(),
        replica_candidates: nodes.clone(),
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    let base = writer
        .posix_write_file(path, b"base", options.clone())
        .unwrap();
    let winner = writer
        .posix_write_file(path, b"winner", options.clone())
        .unwrap();
    let before = writer
        .metadata_backend()
        .unwrap()
        .metadata_summary()
        .unwrap();
    assert!(
        writer
            .posix_replace_file_if_version(
                path,
                &base.object_id,
                &base.version,
                b"stale replacement",
                options
            )
            .is_err()
    );
    assert_eq!(writer.posix_stat(path).unwrap().version, winner.version);
    assert_eq!(writer.posix_read_file(path).unwrap(), b"winner");
    let after = writer
        .metadata_backend()
        .unwrap()
        .metadata_summary()
        .unwrap();
    assert_eq!(after.version_count, before.version_count);
    drop(writer);
    runtime.stop_node(&nodes[0]).unwrap();
    runtime.reopen_node(nodes[0].clone()).unwrap();
    let mut recovered = runtime.client(&nodes[0]).unwrap();
    assert_eq!(recovered.posix_stat(path).unwrap().version, winner.version);
    assert_eq!(recovered.posix_read_file(path).unwrap(), b"winner");
    drop(recovered);
    runtime.close().unwrap();
}
