use super::*;

#[test]
fn native_stat_pin_keeps_its_exact_proof_after_read_cache_eviction() {
    let name = "native-stat-pinned-proof";
    let node = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
    let mut runtime = crate::test_runtime::DfsTestRuntime::new(name, node.clone()).unwrap();
    runtime.start_nodes().unwrap();
    let mut writer = runtime.client(&node).unwrap();
    writer.posix_mkdir("/stat-proof-before").unwrap();
    let shard = DfsMetadataShardId::new("root");
    let root = crate::local_dfs_metadata_shard_path(runtime.data_dir(&node), &shard);
    let mut reader = LocalSobjMetadataBackend::open_for_inspection(shard, root).unwrap();
    let before = reader.materialized_through_sequence;
    writer.posix_mkdir("/stat-proof-after").unwrap();
    let expected = writer.posix_stat_distributed("/stat-proof-after").unwrap();
    let cache = Arc::clone(&reader.read_snapshot_cache);
    let captured = Arc::new(Mutex::new(None));
    let capture = Arc::clone(&captured);
    crate::metadata_read_snapshot::install_metadata_read_snapshot_post_read_hook(move || {
        // Evict only the optional reader cache after the actual shared table
        // cut has been selected. Durable DATA and writer ownership stay intact.
        *capture.lock().unwrap() = cache.lock().unwrap().take();
    });
    let pinned = reader.pin_stat_read_snapshot();
    crate::metadata_read_snapshot::clear_metadata_read_snapshot_post_read_hook();
    let cut = captured.lock().unwrap().take();
    let selected = reader.stat_read_snapshots.last().map(|snapshot| {
        (
            snapshot.prefix.clone(),
            snapshot
                .tables
                .as_ref()
                .and_then(|tables| tables.objects.get(&expected.object_id))
                .cloned(),
            snapshot.tables.clone(),
        )
    });
    if pinned.is_ok() {
        reader.unpin_stat_read_snapshot();
    }
    let fresh = reader.pin_stat_read_snapshot();
    let fresh_prefix = reader
        .stat_read_snapshots
        .last()
        .map(|snapshot| snapshot.prefix.clone());
    if fresh.is_ok() {
        reader.unpin_stat_read_snapshot();
    }
    // Corrupt only this reader's proof cache, never the native writer's data.
    // Neither absent proof nor a proof for another sequence may become stat.
    let mut rejected = Vec::new();
    for missing in [true, false] {
        let original = {
            let mut slot = reader.read_snapshot_cache.lock().unwrap();
            let entry = slot.as_mut().expect("fresh shared snapshot is cached");
            let original = entry.prefix_proof.clone();
            if missing {
                entry.prefix_proof = None;
            } else {
                entry.prefix_proof.as_mut().unwrap().journal_sequence += 1;
            }
            original
        };
        let result = reader.pin_stat_read_snapshot();
        if result.is_ok() {
            reader.unpin_stat_read_snapshot();
        }
        rejected.push(result.is_err());
        reader
            .read_snapshot_cache
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .prefix_proof = original;
    }
    let same_table = selected
        .as_ref()
        .zip(cut.as_ref())
        .is_some_and(|((_, _, selected), cut)| {
            selected
                .as_ref()
                .is_some_and(|tables| Arc::ptr_eq(tables, &cut.tables))
        });
    std::fs::write(
        runtime
            .data_dir(&node)
            .root()
            .join("boundary-diagnostic-stat-cut.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "before_sequence": before,
            "captured_sequence": cut.as_ref().and_then(|cut| cut.materialized_through_sequence),
            "captured_prefix": cut.as_ref().and_then(|cut| cut.prefix_proof.as_ref()),
            "pin_result": format!("{pinned:?}"),
            "selected_prefix": selected.as_ref().map(|(prefix, _, _)| prefix),
            "selected_object": selected.as_ref().and_then(|(_, object, _)| object.as_ref()),
            "same_table_arc": same_table,
            "fresh_result": format!("{fresh:?}"),
            "fresh_prefix": fresh_prefix,
            "invalid_proofs_rejected": rejected,
            "qualification": "Actual native writer; read-only inspection handle. Only optional read cache was evicted or invalidated; no data, ownership fence, source or authority was replaced."
        }))
        .unwrap(),
    )
    .unwrap();
    drop(reader);
    drop(writer);
    runtime.close().unwrap();
    let cut = cut.expect("the actual newer native table used a shared read cut");
    assert!(cut.materialized_through_sequence.unwrap() > before);
    pinned.expect("a valid pinned table must retain its own exact prefix after cache eviction");
    let (prefix, object, _) = selected.expect("successful stat retains its cut");
    assert_eq!(Some(&prefix), cut.prefix_proof.as_ref());
    assert_eq!(
        prefix.journal_sequence,
        cut.materialized_through_sequence.unwrap()
    );
    assert!(same_table);
    let object = object.expect("the real committed new object is in the pinned table");
    assert_eq!(object.object_id, expected.object_id);
    assert_eq!(object.version, expected.version);
    assert_eq!(object.path, "/stat-proof-after");
    fresh.expect("subsequent fresh stat remains available");
    assert_eq!(fresh_prefix.as_ref(), cut.prefix_proof.as_ref());
    assert_eq!(rejected, [true, true]);
}
