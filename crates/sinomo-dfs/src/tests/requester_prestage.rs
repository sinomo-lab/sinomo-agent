//! Requester durability through the shared embedded daemon execution path.
use super::*;

#[test]
fn native_remote_write_retains_its_actual_canonical_binding_on_the_requester() {
    let nodes =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-requester-binding", 3);
    let mut runtime =
        crate::test_runtime::DfsTestRuntime::new("native-requester-binding", nodes[0].clone())
            .unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let configs = runtime.metadata_shards().unwrap();
    let assignment = crate::control_plane::select_metadata_shard_config_for_path(
        &sifs_file_system_id(),
        &configs,
        "/users/requester-native.bin",
    )
    .unwrap()
    .unwrap();
    let requester = nodes
        .iter()
        .find(|node| **node != assignment.primary_node_id)
        .unwrap()
        .clone();
    let mut client = runtime.client(&requester).unwrap();
    let bytes = vec![0x6d; 1024 * 1024 + 33];
    let stat = client
        .posix_write_file(
            "/users/requester-native.bin",
            &bytes,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: nodes,
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    client
        .posix_sync_file("/users/requester-native.bin", DfsPosixSyncKind::File)
        .unwrap();
    assert!(stat.current_locations.contains(&requester));
    let (manifest, stored) = client
        .content
        .read_object_version(stat.object_id.as_str(), stat.version.as_str())
        .unwrap();
    assert_eq!(stored, bytes);
    assert_eq!(Some(manifest.content_hash), stat.content_hash);
    drop(client);
    runtime.stop_node(&requester).unwrap();
    runtime.reopen_node(requester.clone()).unwrap();
    let mut reopened = runtime.client(&requester).unwrap();
    // The reopened requester still holds its canonical local copy, and the
    // caller reads the committed bytes through the distributed read.
    let (manifest, stored) = reopened
        .content
        .read_object_version(stat.object_id.as_str(), stat.version.as_str())
        .unwrap();
    assert_eq!(stored, bytes);
    assert_eq!(Some(manifest.content_hash), stat.content_hash);
    assert_eq!(
        reopened
            .posix_read_file_distributed("/users/requester-native.bin")
            .unwrap(),
        bytes
    );
    drop(reopened);
    runtime.close().unwrap();
}
