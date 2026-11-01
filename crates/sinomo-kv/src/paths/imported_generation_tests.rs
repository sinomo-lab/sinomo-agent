//! Generation comparison borrows the existing native owner and retained memo.
use super::*;

fn save(root: &sinomo_testdir::TestDir, name: &str, value: serde_json::Value) {
    std::fs::write(root.join(name), serde_json::to_vec_pretty(&value).unwrap()).unwrap();
}
fn native(root: &Path, count: usize) -> (ClusterKvPaths, NodeId, KvSnapshot, KvSnapshot) {
    let paths = ClusterKvPaths::new(root);
    let node = NodeId::generate();
    let store = paths.raft_store();
    store.set_leader_compact(&node, 1).unwrap();
    let mut first = None;
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
        let snapshot = Arc::unwrap_or_clone(result.snapshot);
        first.get_or_insert_with(|| snapshot.clone());
        last = Some(snapshot);
    }
    (paths, node, first.unwrap(), last.unwrap())
}
fn legacy(paths: &ClusterKvPaths, node: &NodeId, incoming: &KvSnapshot) -> bool {
    match read_cluster_kv_current_snapshot_for_node(paths, node.clone()) {
        Ok(local) => {
            kv_snapshot_materialized_generation(incoming)
                > kv_snapshot_materialized_generation(&local)
        }
        Err(_) => true,
    }
}

#[test]
fn native_import_comparison_preserves_no_effect_generation_without_full_snapshot() {
    let root = sinomo_testdir::TestDir::new("native-import-generation-rich-memo");
    let (paths, node, first, current) = native(&root.join("current"), 64);
    let (_source, _source_node, _, incoming) = native(&root.join("incoming-source"), 65);
    assert_eq!(first.revision, current.revision);
    assert_eq!(incoming.revision, current.revision);
    assert!(current.committed_requests.len() >= 64);
    assert!(
        kv_snapshot_materialized_generation(&incoming)
            > kv_snapshot_materialized_generation(&current)
    );
    let expected = [&first, &current, &incoming].map(|snapshot| {
        kv_snapshot_materialized_generation(snapshot)
            > kv_snapshot_materialized_generation(&current)
    });
    // A newer imported read model is not the local native comparison source.
    // Publish the actual source snapshot through the existing materialized
    // importer, without installing its unanchored state into native Raft.
    write_imported_cluster_kv_snapshot_if_changed_with_observation_report(
        &paths,
        &incoming,
        |_| false,
    )
    .unwrap();
    let before = crate::in_process::complete_snapshot_materializations_for_test();
    let actual = [&first, &current, &incoming]
        .map(|snapshot| paths.imported_snapshot_advances_current_for_node(&node, snapshot));
    let after = crate::in_process::complete_snapshot_materializations_for_test();
    let unchanged = read_cluster_kv_current_snapshot_for_node(&paths, node.clone()).unwrap();
    save(
        &root,
        "native-import-generation-outcome.json",
        serde_json::json!({
            "qualification":"real native request producers on two disposable roots; incoming plain snapshot is comparison DATA, not a Raft adoption/authority certificate; current-thread snapshot count covers only the3 production comparator calls",
            "first_generation":kv_snapshot_materialized_generation(&first),
            "current_generation":kv_snapshot_materialized_generation(&current),
            "incoming_generation":kv_snapshot_materialized_generation(&incoming),
            "actual":actual,"expected":expected,"snapshot_count_before":before,"snapshot_count_after":after,
            "current_full_memo_before":current.committed_requests,"current_full_memo_after":unchanged.committed_requests,
            "expired_before":current.expired_requests,"expired_after":unchanged.expired_requests,
        }),
    );
    assert_eq!(actual, expected);
    assert_eq!(expected, [false, false, true]);
    assert_eq!(unchanged.entries, current.entries);
    assert_eq!(unchanged.committed_requests, current.committed_requests);
    assert_eq!(unchanged.expired_requests, current.expired_requests);
    assert_eq!(
        after - before,
        0,
        "generation comparison must not materialize unrelated entries/events/request memo"
    );
}

#[test]
fn native_import_generation_keeps_cold_busy_role_identity_and_unknown_fallbacks() {
    let root = sinomo_testdir::TestDir::new("native-import-generation-fallbacks");
    let (paths, node, _, current) = native(&root.join("current"), 4);
    let mut observed = Vec::new();
    assert!(crate::raft::evict_resident_raft_node_for_test(
        &paths.raft_wal_paths(&node)
    ));
    let expected = legacy(&paths, &node, &current);
    // The reference read may seed a resident owner. Test the actual cold branch
    // independently rather than letting the reference warm the candidate.
    crate::raft::evict_resident_raft_node_for_test(&paths.raft_wal_paths(&node));
    let actual = paths.imported_snapshot_advances_current_for_node(&node, &current);
    observed.push(serde_json::json!({"phase":"cold","actual":actual,"legacy":expected}));
    save(
        &root,
        "native-import-generation-fallbacks-partial.json",
        serde_json::json!({"observed":observed}),
    );
    assert_eq!(actual, expected);
    let mut holder = paths.local_raft_command_storage(node.clone(), |_| false);
    holder.read_index_state().unwrap();
    let expected = legacy(&paths, &node, &current);
    let actual = paths.imported_snapshot_advances_current_for_node(&node, &current);
    observed.push(serde_json::json!({"phase":"checked-out","actual":actual,"legacy":expected}));
    save(
        &root,
        "native-import-generation-fallbacks-partial.json",
        serde_json::json!({"observed":observed}),
    );
    assert_eq!(actual, expected);
    drop(holder);
    struct ClearRole(PathBuf);
    impl Drop for ClearRole {
        fn drop(&mut self) {
            crate::raft::clear_process_local_raft_access_denial_for_test(&self.0);
        }
    }
    let role = ClearRole(paths.raft.clone());
    crate::raft::deny_process_local_raft_access_for_root(role.0.clone()).unwrap();
    let expected = legacy(&paths, &node, &current);
    let actual = paths.imported_snapshot_advances_current_for_node(&node, &current);
    observed.push(serde_json::json!({"phase":"Shadow-denied","actual":actual,"legacy":expected}));
    save(
        &root,
        "native-import-generation-fallbacks-partial.json",
        serde_json::json!({"observed":observed}),
    );
    assert_eq!(actual, expected);
    drop(role);
    // Existing comparison is advisory: preserve its exact legacy result even
    // while a higher-term authority floor is pending, and never clear that floor.
    paths.locked_initialize_raft_resident(&node).unwrap();
    let term_store = paths.raft_store();
    term_store.publish_pending_higher_term(&node, 2).unwrap();
    let floor_before = term_store.pending_higher_term(&node).unwrap();
    let expected = legacy(&paths, &node, &current);
    let actual = paths.imported_snapshot_advances_current_for_node(&node, &current);
    let floor_after = term_store.pending_higher_term(&node).unwrap();
    observed.push(
        serde_json::json!({"phase":"pending-term-floor","actual":actual,"legacy":expected,
        "pending_before":floor_before,"pending_after":floor_after}),
    );
    save(
        &root,
        "native-import-generation-fallbacks-partial.json",
        serde_json::json!({"observed":observed}),
    );
    assert_eq!(actual, expected);
    assert_eq!(floor_before, Some(2));
    assert_eq!(floor_after, floor_before);
    term_store.observe_term_compact(&node, 2).unwrap();
    drop(term_store);
    let wrong = NodeId::generate();
    let expected = legacy(&paths, &wrong, &current);
    let actual = paths.imported_snapshot_advances_current_for_node(&wrong, &current);
    observed.push(serde_json::json!({"phase":"different-node","actual":actual,"legacy":expected}));
    save(
        &root,
        "native-import-generation-fallbacks-partial.json",
        serde_json::json!({"observed":observed}),
    );
    assert_eq!(actual, expected);
    let missing = ClusterKvPaths::new(root.join("no-current-source"));
    let expected = legacy(&missing, &node, &current);
    let actual = missing.imported_snapshot_advances_current_for_node(&node, &current);
    observed
        .push(serde_json::json!({"phase":"Unknown-no-source","actual":actual,"legacy":expected}));
    assert!(
        actual && expected,
        "unknown comparison retains the old attempt-import result, never NoEffect"
    );
    // Independent root: no floor is cleared merely to replace physical DATA.
    let (paths, node, _, old) = native(&root.join("replacement"), 4);
    // Produce a replacement independently so creating it cannot invalidate the
    // old target slot before the actual comparison observes physical replacement.
    let replacement_paths = ClusterKvPaths::new(root.join("replacement-producer"));
    let store = replacement_paths.raft_store();
    store.set_leader_compact(&node, 1).unwrap();
    let replacement = store
        .append_command_with_response_and_publish(
            &node,
            KvCommand::Op(KvOp::Put {
                key: "/generation/replacement".into(),
                value: b"different".to_vec(),
                lease_id: None,
            }),
            |_| false,
        )
        .unwrap()
        .snapshot;
    drop(store);
    assert!(crate::raft::evict_resident_raft_node_for_test(
        &replacement_paths.raft_wal_paths(&node)
    ));
    let replacement_identity =
        crate::wal::raft_view_identity_for_read(&replacement_paths.raft_wal_paths(&node)).unwrap();
    std::fs::rename(&paths.raft, root.join("retained-original-raft")).unwrap();
    std::fs::rename(&replacement_paths.raft, &paths.raft).unwrap();
    let installed_identity =
        crate::wal::raft_view_identity_for_read(&paths.raft_wal_paths(&node)).unwrap();
    assert_eq!(replacement_identity, installed_identity);
    assert_eq!(replacement.revision, old.revision);
    assert!(
        kv_snapshot_materialized_generation(&old)
            > kv_snapshot_materialized_generation(&replacement)
    );
    // Exercise the candidate first: the reference must not invalidate the
    // previous physical identity on the candidate's behalf.
    let actual = paths.imported_snapshot_advances_current_for_node(&node, &old);
    let expected = legacy(&paths, &node, &old);
    observed.push(
        serde_json::json!({"phase":"physical-replacement","actual":actual,"legacy":expected,
        "old_generation":kv_snapshot_materialized_generation(&old),
        "actual_generation":kv_snapshot_materialized_generation(&replacement),
        "replacement_native_identity":format!("{replacement_identity:?}"),
        "installed_native_identity":format!("{installed_identity:?}")}),
    );
    save(
        &root,
        "native-import-generation-fallbacks.json",
        serde_json::json!({
            "qualification":"same old fallback policy; boolean comparison does not grant native/source/consensus authority",
            "observed":observed,
        }),
    );
    save(
        &root,
        "native-import-generation-fallbacks-partial.json",
        serde_json::json!({"observed":observed}),
    );
    assert_eq!(actual, expected);
    assert!(
        actual,
        "physical identity replacement cannot reuse the retained old memo generation"
    );
}

#[test]
fn native_empty_idle_owner_preserves_materialized_only_generation() {
    let root = sinomo_testdir::TestDir::new("native-empty-owner-import-generation");
    let (source_paths, _source_node, _, imported) = native(&root.join("actual-source"), 4);
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
    let materialized_before = paths.local_cache().cached_snapshot().unwrap();
    let reference = legacy(&paths, &node, &imported);
    let actual = paths.imported_snapshot_advances_current_for_node(&node, &imported);
    let materialized_after = paths.local_cache().cached_snapshot().unwrap();
    let durable_after = crate::wal::raft_view_identity_for_read(&wal).unwrap();
    save(
        &root,
        "native-empty-owner-import-generation.json",
        serde_json::json!({
            "qualification":"genuine normal empty resident initializer plus unchanged imported snapshot from a different genuine native producer; no durable native DATA was invented or removed",
            "source_root":source_paths.root,"durable_before":durable_before.0,"durable_after":durable_after.0,
            "empty_native_generation":kv_snapshot_materialized_generation(&empty_native.value),
            "materialized_generation":kv_snapshot_materialized_generation(&materialized_before),
            "incoming_generation":kv_snapshot_materialized_generation(&imported),
            "actual":actual,"legacy":reference,
            "materialized_before":materialized_before,"materialized_after":materialized_after,
        }),
    );
    assert!(!durable_before.0 && !durable_after.0);
    assert_eq!(
        kv_snapshot_materialized_generation(&empty_native.value),
        (KvRevision::ZERO, 0)
    );
    assert_ne!(
        kv_snapshot_materialized_generation(&empty_native.value),
        kv_snapshot_materialized_generation(&imported)
    );
    assert_eq!(materialized_before, imported);
    assert_eq!(materialized_after, materialized_before);
    assert!(
        !reference,
        "the old reader compares against materialized DATA, not empty native state"
    );
    assert_eq!(actual, reference);
}

#[test]
fn retained_generation_index_matches_snapshot_after_expiry_and_restore_data() {
    let root = sinomo_testdir::TestDir::new("generation-index-expiry-data");
    let mut store = InProcessKvStore::new();
    let record = |index: u64, bytes: usize| {
        let request = KvCommandRequest::generated(KvCommand::Barrier);
        KvCommittedRequestRecord {
            request_id: request.request_id.clone(),
            command_digest: request.digest().unwrap(),
            term: 1,
            log_index: index,
            revision: KvRevision::ZERO,
            txn_response: (bytes > 0).then(|| KvTxnResponse {
                succeeded: true,
                revision: KvRevision::ZERO,
                responses: vec![KvOpResponse::Delete {
                    key: "x".repeat(bytes),
                    deleted: false,
                }],
            }),
        }
    };
    let mut cuts = Vec::new();
    for index in [7, 31, 2] {
        store.record_committed_request(record(index, 0)).unwrap();
        let snapshot = store.try_snapshot().unwrap();
        assert_eq!(
            store.materialized_generation(),
            kv_snapshot_materialized_generation(&snapshot)
        );
        cuts.push(snapshot);
    }
    let before = store.try_snapshot().unwrap();
    let restored = InProcessKvStore::restore(before.clone()).unwrap();
    assert_eq!(
        restored.materialized_generation(),
        kv_snapshot_materialized_generation(&before)
    );
    // Existing supported oversized-memo expiry is a DATA invariant case, not
    // a fabricated native commit/receipt or a new durable state producer.
    let oversized = record(999, MAX_COMMITTED_KV_REQUEST_BYTES.saturating_add(1));
    let id = oversized.request_id.clone();
    store.record_committed_request(oversized).unwrap();
    let expired = store.try_snapshot().unwrap();
    let actual = store.materialized_generation();
    let reopened = InProcessKvStore::restore(expired.clone()).unwrap();
    save(
        &root,
        "generation-index-expiry.json",
        serde_json::json!({
            "qualification":"DATA-only existing store/index/serialization equivalence; no authority/certificate/native success is supplied",
            "cuts":cuts,"before_expiry":before,"after_expiry":expired,"actual_generation":actual,
            "restored_generation":reopened.materialized_generation(),
        }),
    );
    assert!(store.request_id_expired(&id));
    assert_eq!(actual, kv_snapshot_materialized_generation(&expired));
    assert_eq!(reopened.materialized_generation(), actual);
    assert!(expired.committed_requests.is_empty());
    assert_eq!(
        actual.1, 0,
        "the exact compatibility generation excludes expired memo, not max-ever/applied index"
    );
}
