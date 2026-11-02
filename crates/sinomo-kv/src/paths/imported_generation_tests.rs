//! Imported read models stay materialized data, never native Raft state.
use super::*;

fn native(root: &Path, count: usize) -> (ClusterKvPaths, NodeId, KvSnapshot) {
    let paths = ClusterKvPaths::new(root);
    let node = NodeId::generate();
    let store = paths.raft_store();
    store.set_leader_compact(&node, 1).unwrap();
    let mut last = None;
    for _ in 0..count {
        let result = store
            .append_command_with_response_and_publish(
                &node,
                KvCommand::Op(KvOp::Put {
                    key: "/generation/native-value".into(),
                    value: b"same".to_vec(),
                    lease_id: None,
                }),
                |_| false,
            )
            .unwrap();
        assert!(result.committed_request.log_index > 0);
        last = Some(Arc::unwrap_or_clone(result.snapshot));
    }
    (paths, node, last.unwrap())
}

#[test]
fn newer_imported_read_model_leaves_native_raft_state_unchanged() {
    let root = sinomo_testdir::TestDir::new("native-import-generation-rich-memo");
    let (paths, node, current) = native(&root.join("current"), 64);
    let (_source, _source_node, incoming) = native(&root.join("incoming-source"), 65);
    assert_eq!(incoming.revision, current.revision);
    assert!(current.committed_requests.len() >= 64);
    assert!(
        kv_snapshot_materialized_generation(&incoming)
            > kv_snapshot_materialized_generation(&current)
    );
    // A plain snapshot has no Raft index or term. The materialized importer
    // publishes it without installing its unanchored state into native Raft.
    let (wrote, _) = write_imported_cluster_kv_snapshot_if_changed_with_observation_report(
        &paths,
        &incoming,
        |_| false,
    )
    .unwrap();
    assert!(wrote);
    assert_eq!(paths.local_cache().cached_snapshot().unwrap(), incoming);
    let unchanged = read_cluster_kv_current_snapshot_for_node(&paths, node.clone()).unwrap();
    assert_eq!(unchanged.entries, current.entries);
    assert_eq!(unchanged.committed_requests, current.committed_requests);
    assert_eq!(unchanged.expired_requests, current.expired_requests);
}

#[test]
fn empty_idle_native_owner_preserves_materialized_only_view() {
    let root = sinomo_testdir::TestDir::new("native-empty-owner-import-generation");
    let (_source_paths, _source_node, imported) = native(&root.join("actual-source"), 4);
    let paths = ClusterKvPaths::new(root.join("materialized-only"));
    let node = NodeId::generate();
    write_imported_cluster_kv_snapshot_if_changed_with_observation_report(
        &paths,
        &imported,
        |_| false,
    )
    .unwrap();
    // The normal read initializer may cache an empty native owner while only
    // the separate materialized imported snapshot contains committed DATA.
    paths.locked_initialize_raft_resident(&node).unwrap();
    let wal = paths.raft_wal_paths(&node);
    let durable_before = crate::wal::raft_view_identity_for_read(&wal).unwrap();
    let empty_native = crate::raft::read_resident_raft_local_snapshot(&wal, &node)
        .expect("normal initializer retained an idle native owner")
        .unwrap();
    let current = read_cluster_kv_current_snapshot_for_node(&paths, node.clone()).unwrap();
    let durable_after = crate::wal::raft_view_identity_for_read(&wal).unwrap();
    assert!(!durable_before.0 && !durable_after.0);
    assert_eq!(
        kv_snapshot_materialized_generation(&empty_native.value),
        (KvRevision::ZERO, 0)
    );
    assert_ne!(
        kv_snapshot_materialized_generation(&empty_native.value),
        kv_snapshot_materialized_generation(&imported)
    );
    assert_eq!(paths.local_cache().cached_snapshot().unwrap(), imported);
    assert_eq!(
        kv_snapshot_materialized_generation(&current),
        kv_snapshot_materialized_generation(&imported),
        "the current reader keeps materialized DATA over empty native state"
    );
    assert_eq!(current.entries, imported.entries);
}
