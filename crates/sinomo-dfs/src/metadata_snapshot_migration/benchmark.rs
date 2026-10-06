//! Opt-in migration scale workload. Never registered as a development test.
use super::*;
use crate::metadata_table_store::{
    encode_metadata_table_snapshot, read_metadata_table_checkpoint_state, read_metadata_table_state,
};
use crate::{DfsMetadataShardId, DfsObjectKind, DfsObjectMetadata};
use sinomo_core::{DfsObjectPolicy, Id};
use std::time::{Duration, Instant};

fn object(index: usize) -> DfsObjectMetadata {
    let owner = Id::from_legacy_named("migration-owner").unwrap();
    DfsObjectMetadata {
        object_id: Id::from_legacy_named(format!("migration-object-{index}")).unwrap(),
        file_system_id: Id::from_legacy_named("migration-fs").unwrap(),
        path: format!("/file-{index}"),
        kind: DfsObjectKind::File,
        tombstone_provenance: None,
        owner_node_id: owner.clone(),
        current_locations: vec![owner],
        content_hash: Some(format!("blake3:{}", blake3::hash(b"content").to_hex())),
        content_size_bytes: Some(7),
        link_target_object_id: None,
        mode: 0o600,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: Id::from_legacy_named(format!("migration-version-{index}")).unwrap(),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    }
}

fn migration<'a>(root: &'a Path, state: &'a DfsMetadataTableState) -> SnapshotMigration<'a> {
    SnapshotMigration::new(
        root,
        &state.tables,
        state.materialized_through_sequence.unwrap(),
        state.prefix_proof.as_ref().unwrap(),
        state.snapshot_fingerprint.clone(),
        u32::from(state.snapshot_format_version.unwrap()),
    )
    .unwrap()
}

/// Runs the opt-in physical metadata migration benchmark in a fresh directory.
/// Panics if a migration invariant fails; returns and persists measured results.
pub fn run(root: &Path, count: usize) -> serde_json::Value {
    assert!((1_000..=2_000_000).contains(&count));
    assert!(
        !root.exists(),
        "benchmark must use a fresh output directory"
    );
    fs::create_dir_all(root).unwrap();
    let started = Instant::now();
    let mut tables = DfsMetadataTables::default();
    let first = object(0);
    tables.apply(DfsMetadataTableOp::PutFileSystem {
        file_system: crate::DfsFileSystem {
            metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
            file_system_id: first.file_system_id.clone(),
            display_name: "scale".into(),
            owner_node_id: first.owner_node_id.clone(),
            subtree_policies: Vec::new(),
            retention_policies: Default::default(),
            module_metadata: Default::default(),
        },
    });
    for index in 0..count {
        tables.apply(DfsMetadataTableOp::PutObject {
            object: object(index),
        });
    }
    let fingerprint =
        crate::metadata_table_store::snapshot_semantic_fingerprint(&tables, Some(1)).unwrap();
    let proof = crate::metadata_records::dfs_metadata_prefix_proof_seed(
        &DfsMetadataShardId::new("root"),
        1,
        &fingerprint,
    );
    let old = encode_metadata_table_snapshot(&tables, Some(1), Some(&proof)).unwrap();
    let old_bytes = old.len();
    crate::metadata_io::write_file_atomic(&metadata_table_snapshot_path(root), &old).unwrap();
    drop(old);
    drop(tables);
    crate::metadata_table_apply::request_metadata_table_format_migration(root).unwrap();
    let state = read_metadata_table_checkpoint_state(root).unwrap();
    let fixture_ms = started.elapsed().as_millis();
    let budget = MigrationBudget {
        max_records: 512,
        max_bytes: 2 * 1024 * 1024,
        max_duration: Duration::from_millis(100),
    };
    let mut driver = migration(root, &state);
    let first_batch_started = Instant::now();
    driver.advance(budget, &AtomicBool::new(false)).unwrap();
    let first_batch_duration = first_batch_started.elapsed();
    let first_progress = driver.store().read().unwrap().unwrap();
    assert!(first_progress.copied_records > 0 && first_progress.copied_records <= 512);
    let stage_len = fs::metadata(&driver.stage).unwrap().len();
    drop(driver);
    drop(state);
    let resume_started = Instant::now();
    let state = read_metadata_table_checkpoint_state(root).unwrap();
    let source_reload_ms = resume_started.elapsed().as_millis();
    let migration_started = Instant::now();
    let mut batches = 1;
    let mut maximum_batch_records = first_progress.copied_records;
    let mut maximum_batch_bytes = first_progress.copied_bytes;
    let mut previous = first_progress;
    let mut driver = migration(root, &state);
    assert_eq!(fs::metadata(&driver.stage).unwrap().len(), stage_len);
    loop {
        let outcome = driver.advance(budget, &AtomicBool::new(false)).unwrap();
        let current = driver.store().read().unwrap().unwrap();
        let records = current.copied_records - previous.copied_records;
        let bytes = current.copied_bytes - previous.copied_bytes;
        maximum_batch_records = maximum_batch_records.max(records);
        maximum_batch_bytes = maximum_batch_bytes.max(bytes);
        assert!(records <= 512 && bytes <= 2 * 1024 * 1024);
        previous = current;
        batches += 1;
        if matches!(outcome, MigrationAdvance::Complete(_)) {
            break;
        }
    }
    let migration_ms = (first_batch_duration + migration_started.elapsed()).as_millis();
    drop(driver);
    drop(state);
    let read_started = Instant::now();
    let current = read_metadata_table_state(root).unwrap();
    assert_eq!(current.tables.objects.len(), count);
    assert_eq!(current.tables.objects[&object(0).object_id], object(0));
    assert_eq!(
        current.tables.objects[&object(count - 1).object_id],
        object(count - 1)
    );
    assert_eq!(current.prefix_proof, Some(proof));
    let result = serde_json::json!({"objects": count, "copied_records": previous.copied_records,
        "verified_records": previous.verified_records, "legacy_snapshot_bytes": old_bytes,
        "current_snapshot_bytes": fs::metadata(metadata_table_snapshot_path(root)).unwrap().len(),
        "fixture_ms": fixture_ms, "source_reload_ms": source_reload_ms, "migration_ms": migration_ms,
        "current_read_ms": read_started.elapsed().as_millis(), "batches": batches,
        "max_batch_records": maximum_batch_records, "max_batch_bytes": maximum_batch_bytes,
        "passed": true, "profile": "physical metadata-record migration; not full filesystem or content throughput"});
    fs::write(
        root.join("scale-result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    result
}
