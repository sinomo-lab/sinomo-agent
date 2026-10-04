//! Pins regression tests.

use super::*;

#[test]
fn durable_version_pins_survive_replay_and_snapshot_until_the_last_consumer_unpins() {
    let root = temp_root("metadata-version-pins-lifecycle");
    let shard_id = DfsMetadataShardId::new("root");
    let source_root = root.join("source");
    let replica_root = root.join("replica");
    let mut source = LocalSobjMetadataBackend::new(shard_id.clone(), &source_root).expect("source");
    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root).expect("replica");
    let old_version = version("version_pin_old");
    let current_version = version("version_pin_current");
    let current = object(current_version.clone());
    let old_record =
        object_version_record(&current, old_version.clone(), OffsetDateTime::UNIX_EPOCH);
    let current_record = object_version_record(
        &current,
        current_version,
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
    );
    source.put_file_system(&file_system()).expect("filesystem");
    source.put_object(&current).expect("current object");
    source.put_version(&old_record).expect("old version");
    source
        .put_version(&current_record)
        .expect("current version");

    let pin_a = DfsMetadataVersionPinRecord {
        namespace: None,
        file_system_id: current.file_system_id.clone(),
        object_id: current.object_id.clone(),
        version: old_version.clone(),
        pin_id: "checkpoint-a".to_string(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    let pin_b = DfsMetadataVersionPinRecord {
        pin_id: "checkpoint-b".to_string(),
        ..pin_a.clone()
    };
    assert!(
        source
            .put_version_pin(&pin_a)
            .expect("first consumer pin")
            .is_some()
    );
    let sequence_after_first_pin = source.journal_last_sequence();
    assert_eq!(
        source.put_version_pin(&pin_a).expect("exact pin retry"),
        None,
        "an exact consumer retry must not grow the journal"
    );
    assert_eq!(source.journal_last_sequence(), sequence_after_first_pin);
    assert!(
        source
            .put_version_pin(&pin_b)
            .expect("second consumer pin")
            .is_some()
    );

    let initial = source
        .journal_batch_since_limited(0, 32, usize::MAX)
        .expect("pin journal read")
        .expect("pin journal batch");
    replica
        .apply_journal_batch(&initial)
        .expect("replicate version pins");
    drop(replica);
    let replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), &replica_root).expect("reopen replica");
    assert_eq!(
        replica
            .version_pins_for_object(&current.object_id)
            .expect("replayed pins"),
        vec![pin_a.clone(), pin_b.clone()]
    );
    drop(replica);

    let projection = source
        .metadata_history_gc_projection(
            DfsMetadataHistoryGcOptions::dry_run(Duration::ZERO, 1)
                .with_now_unix_nanos(2_000_000_000),
        )
        .expect("pinned GC projection");
    assert_eq!(projection.retained_durable_pin_count, 2);
    assert_eq!(
        projection.eligible_version_count, 0,
        "a durable consumer pin must root history older than the keep-latest window"
    );

    assert!(
        source
            .remove_version_pin(
                &pin_a.file_system_id,
                &pin_a.object_id,
                &pin_a.version,
                &pin_a.pin_id,
            )
            .expect("remove first consumer")
            .is_some()
    );
    assert_eq!(
        source
            .metadata_history_gc_projection(
                DfsMetadataHistoryGcOptions::dry_run(Duration::ZERO, 1)
                    .with_now_unix_nanos(2_000_000_000),
            )
            .expect("single-pin GC projection")
            .eligible_version_count,
        0,
        "one remaining consumer must keep the shared version rooted"
    );

    source
        .compact_metadata_table_log_if_needed(0)
        .expect("publish pin snapshot");
    drop(source);
    let mut source =
        LocalSobjMetadataBackend::new(shard_id.clone(), &source_root).expect("reopen source");
    assert_eq!(
        source
            .version_pins_for_object(&current.object_id)
            .expect("snapshot pins"),
        vec![pin_b.clone()]
    );
    assert!(
        source
            .get_version(&current.object_id, &old_version)
            .expect("snapshot-pinned version")
            .is_some()
    );

    assert!(
        source
            .remove_version_pin(
                &pin_b.file_system_id,
                &pin_b.object_id,
                &pin_b.version,
                &pin_b.pin_id,
            )
            .expect("remove last consumer")
            .is_some()
    );
    let report = source
        .gc_metadata_history_authoritative(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_now_unix_nanos(2_000_000_000),
        )
        .expect("collect newly unpinned version");
    assert_eq!(report.removed_version_count, 1);
    assert_eq!(
        source
            .get_version(&current.object_id, &old_version)
            .expect("unrooted old version"),
        None
    );
    drop(source);

    let source =
        LocalSobjMetadataBackend::new(shard_id, &source_root).expect("reopen collected source");
    assert!(
        source
            .version_pins_for_object(&current.object_id)
            .expect("pins after final unpin")
            .is_empty()
    );
    assert_eq!(
        source
            .get_version(&current.object_id, &old_version)
            .expect("collected version after restart"),
        None
    );

    cleanup(&root);
}

#[test]
fn version_pin_admission_fails_closed_for_missing_or_mismatched_versions() {
    let root = temp_root("metadata-version-pin-admission");
    let shard_id = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id, &root).expect("metadata backend");
    let current_version = version("version_pin_admission_current");
    let current = object(current_version.clone());
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .put_version(&object_version_record(
            &current,
            current_version,
            OffsetDateTime::UNIX_EPOCH,
        ))
        .expect("current version");

    let missing = DfsMetadataVersionPinRecord {
        namespace: None,
        file_system_id: current.file_system_id.clone(),
        object_id: current.object_id.clone(),
        version: version("version_pin_missing"),
        pin_id: "checkpoint-missing".to_string(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    assert!(
        backend
            .put_version_pin(&missing)
            .expect_err("missing metadata version must reject the pin")
            .to_string()
            .contains("missing metadata version")
    );

    let mismatched = DfsMetadataVersionPinRecord {
        file_system_id: DfsFileSystemId::from_legacy_named("other-fs").expect("filesystem id"),
        version: current.version.clone(),
        pin_id: "checkpoint-wrong-filesystem".to_string(),
        ..missing
    };
    assert!(
        backend
            .put_version_pin(&mismatched)
            .expect_err("cross-filesystem pin must be rejected")
            .to_string()
            .contains("filesystem")
    );
    assert!(
        backend
            .version_pins_for_object(&current.object_id)
            .expect("pins after rejected admission")
            .is_empty()
    );

    cleanup(&root);
}

#[test]
fn sharded_version_pin_remove_preflights_all_transfer_owners() {
    let root = temp_root("sharded-pin-transfer-remove-preflight");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "archive",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/archive".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let pinned_version = version("version_sharded_preflight_pinned");
    let target_version = version("version_sharded_preflight_target");
    let mut source_object = object(pinned_version.clone());
    source_object.path = "/docs/readme.md".to_string();
    let pinned_record = object_version_record(
        &source_object,
        pinned_version.clone(),
        OffsetDateTime::UNIX_EPOCH,
    );
    backend.put_object(&source_object).expect("source object");
    backend.put_version(&pinned_record).expect("source version");
    let pin = DfsMetadataVersionPinRecord {
        namespace: None,
        file_system_id: source_object.file_system_id.clone(),
        object_id: source_object.object_id.clone(),
        version: pinned_version,
        pin_id: "checkpoint-sharded-preflight".to_string(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    backend.put_version_pin(&pin).expect("source pin");

    let mut moved = source_object.clone();
    moved.path = "/archive/readme.md".to_string();
    moved.version = target_version.clone();
    let target_record = object_version_record(&moved, target_version, OffsetDateTime::UNIX_EPOCH);
    assert!(
        backend
            .reconcile_cross_shard_pin_transfer(
                "transfer-sharded-preflight",
                &moved,
                &source_object.path,
                &[pinned_record, target_record],
                std::slice::from_ref(&pin),
            )
            .expect("target reconcile")
    );

    assert!(
        backend
            .remove_version_pin(
                &pin.file_system_id,
                &pin.object_id,
                &pin.version,
                &pin.pin_id,
            )
            .expect_err("all-shard preflight must reject before source mutation")
            .to_string()
            .contains("active cross-shard transfer")
    );
    for shard_id in ["root", "archive"] {
        assert_eq!(
            backend
                .shard_backend(&DfsMetadataShardId::new(shard_id))
                .expect("physical shard")
                .version_pins_for_object(&pin.object_id)
                .expect("physical pin"),
            vec![pin.clone()],
            "failed preflight must leave shard {shard_id} unchanged"
        );
    }

    backend
        .finalize_cross_shard_pin_transfer(
            "transfer-sharded-preflight",
            &moved,
            &source_object.path,
        )
        .expect("finalize target provenance");
    assert!(
        backend
            .remove_version_pin(
                &pin.file_system_id,
                &pin.object_id,
                &pin.version,
                &pin.pin_id,
            )
            .expect("remove after finalize")
            .is_some()
    );
    for shard_id in ["root", "archive"] {
        assert!(
            backend
                .shard_backend(&DfsMetadataShardId::new(shard_id))
                .expect("physical shard")
                .version_pins_for_object(&pin.object_id)
                .expect("physical pins after finalize")
                .is_empty()
        );
    }

    cleanup(&root);
}

#[test]
fn replicated_snapshot_rejects_dangling_or_filesystem_mismatched_version_pins() {
    let root = temp_root("metadata-version-pin-snapshot-validation");
    let shard_id = DfsMetadataShardId::new("root");
    let mut source =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source")).expect("source");
    let current_version = version("version_pin_snapshot_current");
    let current = object(current_version.clone());
    source.put_file_system(&file_system()).expect("filesystem");
    source.put_object(&current).expect("object");
    source
        .put_version(&object_version_record(
            &current,
            current_version.clone(),
            OffsetDateTime::UNIX_EPOCH,
        ))
        .expect("version");
    let sequence = source.journal_last_sequence();
    let snapshot = source.export_table_snapshot();

    let mut dangling = snapshot.clone();
    dangling
        .tables
        .as_mut()
        .expect("structured snapshot")
        .version_pins
        .push(DfsMetadataVersionPinRecord {
            namespace: None,
            file_system_id: current.file_system_id.clone(),
            object_id: current.object_id.clone(),
            version: version("version_pin_snapshot_missing"),
            pin_id: "checkpoint-dangling".to_string(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        });
    dangling.semantic_state_fingerprint = Some(
        dfs_metadata_table_snapshot_state_fingerprint(&dangling)
            .expect("dangling snapshot fingerprint"),
    );
    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("replica")).expect("replica");
    assert!(
        replica
            .replace_with_replicated_snapshot(shard_id.clone(), sequence, dangling)
            .expect_err("dangling snapshot pin must reject")
            .to_string()
            .contains("missing metadata version")
    );

    let mut mismatched = snapshot;
    mismatched
        .tables
        .as_mut()
        .expect("structured snapshot")
        .version_pins
        .push(DfsMetadataVersionPinRecord {
            namespace: None,
            file_system_id: DfsFileSystemId::from_legacy_named("other-fs").expect("filesystem id"),
            object_id: current.object_id.clone(),
            version: current_version,
            pin_id: "checkpoint-mismatched".to_string(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        });
    mismatched.semantic_state_fingerprint = Some(
        dfs_metadata_table_snapshot_state_fingerprint(&mismatched)
            .expect("mismatched snapshot fingerprint"),
    );
    assert!(
        replica
            .replace_with_replicated_snapshot(shard_id, sequence, mismatched)
            .expect_err("filesystem-mismatched snapshot pin must reject")
            .to_string()
            .contains("metadata version has filesystem")
    );

    cleanup(&root);
}
