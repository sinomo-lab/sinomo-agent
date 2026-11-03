use sinomo_dfs::{
    DfsMetadataBackend, DfsReplicationPolicy, DfsWriteOptions, test_support::DfsTestRuntime,
};

#[test]
fn metadata_changes_preserve_the_committed_data_generation_after_restart() {
    let nodes = DfsTestRuntime::ordered_node_ids("native-publication-consistency", 3);
    let mut runtime =
        DfsTestRuntime::new("native-publication-consistency", nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let path = "/native-publication-consistency.bin";
    let mut writer = runtime.client(&nodes[0]).unwrap();
    let options = DfsWriteOptions {
        policy: DfsReplicationPolicy::new(2, 2).unwrap(),
        replica_candidates: nodes.clone(),
        wait_for_commit: true,
        foreground_replica_repair: true,
    };
    writer
        .posix_write_file(path, b"previous", options.clone())
        .unwrap();
    let winner = writer
        .posix_write_file(path, b"committed winner", options)
        .unwrap();
    // The owning primary's durable store; the delegating writer keeps none.
    let primary = runtime.root_shard().unwrap().primary_node_id;
    let recorded = runtime
        .node_metadata(&primary)
        .unwrap()
        .get_version(&winner.object_id, &winner.version)
        .unwrap()
        .unwrap();
    let changed = writer.posix_set_mode(path, 0o600).unwrap();
    assert_eq!(changed.object_id, winner.object_id);
    assert_ne!(changed.version, winner.version);
    let metadata_version = runtime
        .node_metadata(&primary)
        .unwrap()
        .get_version(&changed.object_id, &changed.version)
        .unwrap()
        .unwrap();
    assert_eq!(
        metadata_version.content_durability,
        recorded.content_durability
    );
    assert_eq!(metadata_version.content_hash, recorded.content_hash);
    assert_eq!(
        writer.posix_read_file_distributed(path).unwrap(),
        b"committed winner"
    );
    drop(writer);
    runtime.stop_node(&nodes[0]).unwrap();
    runtime.reopen_node(nodes[0].clone()).unwrap();
    let mut recovered = runtime.client(&nodes[0]).unwrap();
    let actual = recovered.posix_stat_distributed(path).unwrap();
    assert_eq!(actual.version, changed.version);
    assert_eq!(actual.mode, 0o600);
    assert_eq!(
        recovered.posix_read_file_distributed(path).unwrap(),
        b"committed winner"
    );
    drop(recovered);
    runtime.close().unwrap();
}
