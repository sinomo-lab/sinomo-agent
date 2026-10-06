use super::*;
use sinomo_core::network::{InProcessNetwork, InProcessTransport};
use std::path::Path;

fn content_hash(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

fn temp_root(name: &str) -> sinomo_testdir::TestDir {
    sinomo_testdir::TestDir::new(&format!("sinomo-dfs-history-{name}"))
}

fn networked_sifs_with_backend(
    root: &Path,
    node_id: NodeId,
) -> NetworkedDfsService<InProcessTransport> {
    let network = InProcessNetwork::new();
    let content = LocalContentStore::new(root.join("content")).unwrap();
    let metadata = open_root_local_metadata_backend(root.join("metadata")).unwrap();
    let mut dfs = NetworkedDfsService::new(network.add_node(node_id), content)
        .with_metadata_backend(metadata)
        .unwrap();
    dfs.bootstrap_distributed_sifs().unwrap();
    dfs
}

fn history_identity(stat: &DfsStat) -> DfsFileVersionIdentity {
    DfsFileVersionIdentity {
        file_system_id: stat.file_system_id.clone(),
        object_id: stat.object_id.clone(),
        version: stat.version.clone(),
    }
}

fn capture_history<T: DfsMessageTransport>(dfs: &mut NetworkedDfsService<T>, stat: &DfsStat) {
    dfs.history()
        .file_history_pin(DfsFileHistoryPinRequest {
            path: stat.path.clone(),
            identity: history_identity(stat),
            operation: DfsFileHistoryPinOperation::Capture {
                pin_id: "checkpoint".into(),
            },
            operation_id: format!("capture-{}", stat.version),
        })
        .unwrap();
}

#[test]
fn legacy_reference_names_resembling_qualified_keys_stay_raw_in_history() {
    let root = temp_root("legacy-qualified-looking-reference");
    let owner = NodeId::generate();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let saved = dfs
        .posix_write_file(
            "/services/legacy-reference-name",
            b"saved",
            DfsWriteOptions::replicated_eventual(vec![owner]),
        )
        .unwrap();
    capture_history(&mut dfs, &saved);
    let raw_name = format!("ref/{HISTORY_MODULE_ID}/legacy-leaf");
    dfs.metadata_backend_mut()
        .unwrap()
        .put_version_pin(&DfsMetadataVersionPinRecord {
            file_system_id: saved.file_system_id.clone(),
            object_id: saved.object_id.clone(),
            version: saved.version.clone(),
            namespace: None,
            pin_id: raw_name.clone(),
            created_at: time::OffsetDateTime::now_utc(),
        })
        .unwrap();
    let page = dfs
        .list_file_history(DfsFileHistoryListRequest {
            file_system_id: saved.file_system_id,
            path: Some(saved.path),
            object_id: Some(saved.object_id),
            after: None,
            limit: 128,
        })
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert!(page.entries[0].pins.contains(&raw_name));
    assert!(!page.entries[0].pins.iter().any(|pin| pin == "legacy-leaf"));
}

#[test]
fn revision_metadata_preserves_live_object_repin_without_authorizing_reads() {
    let root = temp_root("metadata-only-repin");
    let owner = NodeId::generate();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    let path = "/services/metadata-only-repin";
    let saved = dfs
        .posix_write_file(path, b"saved", options.clone())
        .unwrap();
    capture_history(&mut dfs, &saved);
    dfs.posix_write_file(path, b"current", options.clone())
        .unwrap();
    let identity = history_identity(&saved);
    let release = |id: &str, operation: &str| DfsFileHistoryPinRequest {
        path: path.into(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Unpin { pin_id: id.into() },
        operation_id: operation.into(),
    };
    dfs.file_history_pin(release("checkpoint", "release-metadata-only"))
        .unwrap();
    let metadata = dfs.revision_metadata(&identity).unwrap().unwrap();
    assert!(decode_history_capture(&metadata.version).unwrap().is_some());
    assert!(dfs.resolve_retained_revision(&identity).unwrap().is_none());
    assert!(matches!(
        dfs.read_file_history(DfsFileHistoryReadRequest {
            identity: identity.clone(),
            offset: 0,
            max_bytes: 64,
        })
        .unwrap(),
        DfsFileHistoryReadOutcome::NotRetained { .. }
    ));
    assert!(
        dfs.read_file_version_content(&identity, &content_hash(b"saved"), Some(5))
            .is_err(),
        "metadata inspection must not authorize an unreferenced historical read"
    );

    let repin = DfsFileHistoryPinRequest {
        path: path.into(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Pin {
            pin_id: "repin".into(),
        },
        operation_id: "live-object-repin".into(),
    };
    dfs.file_history_pin(repin.clone()).unwrap();
    assert!(matches!(
        dfs.read_file_history(DfsFileHistoryReadRequest {
            identity: identity.clone(),
            offset: 0,
            max_bytes: 64,
        })
        .unwrap(),
        DfsFileHistoryReadOutcome::Found { .. }
    ));
    dfs.posix_unlink(path).unwrap();
    dfs.posix_write_file(path, b"different object", options)
        .unwrap();
    dfs.file_history_pin(release("repin", "release-after-unlink"))
        .unwrap();
    let metadata = dfs.revision_metadata(&identity).unwrap().unwrap();
    assert_eq!(metadata.current_object.kind, DfsObjectKind::Tombstone);
    assert!(decode_history_capture(&metadata.version).unwrap().is_some());
    let mut fresh_dead_source = repin;
    fresh_dead_source.operation_id = "new-pin-after-source-death".into();
    assert!(
        dfs.file_history_pin(fresh_dead_source).is_err(),
        "an expired structural capture must not revive a tombstoned source"
    );
    assert!(
        dfs.metadata_backend()
            .unwrap()
            .version_pins_for_object(&identity.object_id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn exact_version_reads_require_a_live_grant_and_matching_content_identity() {
    let root = temp_root("exact-version-authority");
    let owner = NodeId::generate();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    let original = dfs
        .posix_write_file("/services/granted-file", b"original", options.clone())
        .unwrap();
    let identity = history_identity(&original);
    capture_history(&mut dfs, &original);
    dfs.posix_write_file("/services/granted-file", b"replacement", options)
        .unwrap();

    let hash = content_hash(b"original");
    assert_eq!(
        dfs.history()
            .read_file_version_content(&identity, &hash, Some(8))
            .unwrap(),
        b"original"
    );
    assert!(
        dfs.history()
            .read_file_version_content(&identity, &content_hash(b"replacement"), None)
            .is_err()
    );
    assert!(
        dfs.history()
            .read_file_version_content(&identity, &hash, Some(7))
            .is_err()
    );

    dfs.history()
        .file_history_pin(DfsFileHistoryPinRequest {
            path: original.path,
            identity: identity.clone(),
            operation: DfsFileHistoryPinOperation::Unpin {
                pin_id: "checkpoint".into(),
            },
            operation_id: "release-exact-version-grant".into(),
        })
        .unwrap();
    assert!(
        dfs.history()
            .read_file_version_content(&identity, &hash, Some(8))
            .is_err(),
        "leftover immutable bytes must not authorize a historical read"
    );
}

#[test]
fn file_history_reads_restore_and_deleted_recovery_keep_one_current_view() {
    let root = temp_root("history-restore-and-recovery");
    let owner = NodeId::from_legacy_named("history-restore-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    let path = "/services/history-document";
    let original = dfs
        .posix_write_file(path, b"original\n", options.clone())
        .unwrap();
    let identity = history_identity(&original);
    assert!(matches!(
        dfs.history()
            .read_file_history(DfsFileHistoryReadRequest {
                identity: identity.clone(),
                offset: 0,
                max_bytes: 64
            })
            .unwrap(),
        DfsFileHistoryReadOutcome::NotRetained { .. }
    ));
    capture_history(&mut dfs, &original);
    let edited = dfs
        .posix_write_file(path, b"edited\n", options.clone())
        .unwrap();
    let page = dfs
        .history()
        .list_file_history(DfsFileHistoryListRequest {
            file_system_id: identity.file_system_id.clone(),
            path: Some(path.into()),
            object_id: None,
            after: None,
            limit: 128,
        })
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert!(!page.entries[0].is_current);
    let read = dfs
        .history()
        .read_file_history(DfsFileHistoryReadRequest {
            identity: identity.clone(),
            offset: 2,
            max_bytes: 3,
        })
        .unwrap();
    let DfsFileHistoryReadOutcome::Found { data } = read else {
        panic!("retained bytes must be readable");
    };
    assert_eq!(data.bytes, b"igi");
    assert_eq!(dfs.posix_stat(path).unwrap().version, edited.version);
    let restored = dfs
        .history()
        .restore_file_history(
            DfsFileHistoryRestoreRequest {
                source: identity.clone(),
                destination_path: path.into(),
                expected_destination: Some(history_identity(&edited)),
                operation_id: "restore-history-document".into(),
            },
            options.clone(),
        )
        .unwrap();
    assert_ne!(restored.committed.version, identity.version);
    assert_ne!(restored.committed.version, edited.version);
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"original\n");
    assert!(
        dfs.history()
            .restore_file_history(
                DfsFileHistoryRestoreRequest {
                    source: identity.clone(),
                    destination_path: path.into(),
                    expected_destination: Some(history_identity(&edited)),
                    operation_id: "stale-restore".into()
                },
                options.clone()
            )
            .is_err()
    );
    assert_eq!(
        dfs.posix_stat(path).unwrap().version,
        restored.committed.version
    );
    dfs.posix_unlink(path).unwrap();
    let recovered = dfs
        .history()
        .restore_file_history(
            DfsFileHistoryRestoreRequest {
                source: identity.clone(),
                destination_path: path.into(),
                expected_destination: None,
                operation_id: "recover-history-document".into(),
            },
            options,
        )
        .unwrap();
    assert_ne!(recovered.committed.object_id, identity.object_id);
    assert_eq!(
        dfs.posix_stat(path).unwrap().object_id,
        recovered.committed.object_id,
        "releasing an old version pin must not re-publish its deleted object's tombstone"
    );
    assert_eq!(dfs.posix_read_file(path).unwrap(), b"original\n");
    assert!(
        dfs.conflicts_for_object(&identity.object_id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn file_history_cursor_cannot_be_reused_for_another_query() {
    let root = temp_root("history-query-cursor");
    let owner = NodeId::from_legacy_named("history-cursor-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner]);
    for path in ["/services/history-a", "/services/history-b"] {
        let stat = dfs
            .posix_write_file(path, b"content", options.clone())
            .unwrap();
        capture_history(&mut dfs, &stat);
    }
    let mut request = DfsFileHistoryListRequest {
        file_system_id: sifs_file_system_id(),
        path: Some("/services".into()),
        object_id: None,
        after: None,
        limit: 1,
    };
    let page = dfs.history().list_file_history(request.clone()).unwrap();
    request.after = Some(page.next_cursor.expect("bounded scan has another page"));
    request.path = Some("/users".into());
    assert!(
        dfs.history()
            .list_file_history(request)
            .unwrap_err()
            .to_string()
            .contains("another query")
    );
}

#[test]
fn file_history_path_query_keeps_captured_current_reused_and_unlinked_families() {
    let root = temp_root("history-indexed-path-families");
    let owner = NodeId::from_legacy_named("history-indexed-owner").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, owner.clone());
    let options = DfsWriteOptions::replicated_eventual(vec![owner.clone()]);
    let original_path = "/services/history-indexed-original";
    let moved_path = "/services/history-indexed-moved";
    let original = dfs
        .posix_write_file(original_path, b"first", options.clone())
        .unwrap();
    capture_history(&mut dfs, &original);
    dfs.posix_rename(original_path, moved_path).unwrap();
    let moved = dfs.posix_stat(moved_path).unwrap();
    capture_history(&mut dfs, &moved);
    let reused = dfs
        .posix_write_file(original_path, b"second family", options)
        .unwrap();
    assert_ne!(reused.object_id, original.object_id);
    capture_history(&mut dfs, &reused);
    dfs.posix_unlink(moved_path).unwrap();
    drop(dfs);
    let mut dfs = networked_sifs_with_backend(&root, owner);
    let list = |dfs: &mut NetworkedDfsService<InProcessTransport>, path: &str| {
        let mut after = None;
        let mut entries = Vec::new();
        for _ in 0..8 {
            let page = dfs
                .list_file_history(DfsFileHistoryListRequest {
                    file_system_id: sifs_file_system_id(),
                    path: Some(path.into()),
                    object_id: None,
                    after,
                    limit: 1,
                })
                .unwrap();
            entries.extend(page.entries);
            after = page.next_cursor;
            if after.is_none() {
                return entries;
            }
        }
        panic!("indexed path query did not terminate within its matching family pages");
    };
    let identities = |entries: Vec<DfsFileHistoryEntry>| {
        entries
            .into_iter()
            .map(|entry| (entry.identity.object_id, entry.identity.version))
            .collect::<std::collections::BTreeSet<_>>()
    };
    let old = (original.object_id.clone(), original.version.clone());
    let renamed = (moved.object_id.clone(), moved.version.clone());
    let replacement = (reused.object_id.clone(), reused.version.clone());
    assert_eq!(
        identities(list(&mut dfs, original_path)),
        std::collections::BTreeSet::from([old.clone(), replacement.clone()])
    );
    assert_eq!(
        identities(list(&mut dfs, moved_path)),
        std::collections::BTreeSet::from([old.clone(), renamed.clone()])
    );
    for stat in [&original, &moved] {
        dfs.file_history_pin(DfsFileHistoryPinRequest {
            path: moved_path.into(),
            identity: history_identity(stat),
            operation: DfsFileHistoryPinOperation::Unpin {
                pin_id: "checkpoint".into(),
            },
            operation_id: format!("release-indexed-{}", stat.version),
        })
        .unwrap();
    }
    assert!(
        list(&mut dfs, moved_path).is_empty(),
        "an index candidate cannot revive a released reference"
    );
    assert_eq!(
        identities(list(&mut dfs, original_path)),
        std::collections::BTreeSet::from([replacement])
    );
}
