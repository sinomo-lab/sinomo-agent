//! Cross shard regression tests.

use super::*;

#[test]
fn sharded_replay_converges_cross_shard_move_independent_of_batch_order() {
    let users_leader_root = temp_root("sharded_replay_move_users_leader");
    let services_leader_root = temp_root("sharded_replay_move_services_leader");
    let first_replica_root = temp_root("sharded_replay_move_first_replica");
    let second_replica_root = temp_root("sharded_replay_move_second_replica");
    let file_system = file_system();
    let mut source = object(version("version_1"));
    source.path = "/users/replay-move-src.txt".to_string();
    source.changed_at_unix_nanos = 1;
    let mut moved = source.clone();
    moved.path = "/services/replay-move-dst.txt".to_string();
    moved.version = version("version_2");
    moved.changed_at_unix_nanos = 2;
    let mut source_eviction = source.clone();
    source_eviction.kind = DfsObjectKind::Tombstone;
    source_eviction.version = moved.version.clone();
    source_eviction.changed_at_unix_nanos = moved.changed_at_unix_nanos;

    let mut users_leader =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("users"), &users_leader_root)
            .expect("users leader");
    users_leader
        .put_file_system(&file_system)
        .expect("users filesystem");
    users_leader.put_object(&source).expect("source object");
    users_leader
        .put_object(&source_eviction)
        .expect("source eviction");
    users_leader
        .put_object(&source)
        .expect("simulated stale location rewrite");
    let mut users_subscription = users_leader.subscribe(0);
    let users_batch = users_leader
        .poll_subscription(&mut users_subscription)
        .expect("users batch");

    let mut services_leader =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("services"), &services_leader_root)
            .expect("services leader");
    services_leader
        .put_file_system(&file_system)
        .expect("services filesystem");
    services_leader
        .put_object(&moved)
        .expect("destination object");
    let mut services_subscription = services_leader.subscribe(0);
    let services_batch = services_leader
        .poll_subscription(&mut services_subscription)
        .expect("services batch");

    let configs = || {
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
            shard_config(
                "users",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/users".to_string(),
                },
            ),
        ]
    };
    for (replica_root, services_first) in [
        (first_replica_root.path(), true),
        (second_replica_root.path(), false),
    ] {
        let mut replica =
            LocalShardedMetadataBackend::new(replica_root, configs()).expect("replica backend");
        // Each source stream begins at its true genesis, including the
        // filesystem record. Do not invent a high starting sequence or a
        // locally authored sequence-1 entry: either would be an invalid
        // exact-prefix fixture and must be rejected as a gap/fork.
        let ordered_batches = if services_first {
            [&services_batch, &users_batch]
        } else {
            [&users_batch, &services_batch]
        };
        for batch in ordered_batches {
            replica.apply_journal_batch(batch).expect("replay batch");
        }
        for batch in ordered_batches {
            assert_eq!(
                replica
                    .apply_journal_batch(batch)
                    .expect("idempotent replay"),
                0
            );
        }
        assert_eq!(
            replica
                .read_object_shard_index(&source.object_id)
                .expect("reconciled index")
                .map(|index| index.shard_id),
            Some(DfsMetadataShardId::new("services"))
        );
        assert_eq!(
            replica
                .get_object(&source.object_id)
                .expect("current object"),
            Some(moved.clone())
        );
        assert_eq!(
            replica
                .get_object_by_path(&file_system.file_system_id, &source.path)
                .expect("source path"),
            None
        );
        assert_eq!(
            replica
                .get_object_by_path(&file_system.file_system_id, &moved.path)
                .expect("destination path"),
            Some(moved.clone())
        );

        drop(replica);
        let reopened =
            LocalShardedMetadataBackend::new(replica_root, configs()).expect("reopened replica");
        assert_eq!(
            reopened
                .get_object(&source.object_id)
                .expect("reopened object"),
            Some(moved.clone())
        );
        assert_eq!(
            reopened
                .get_object_by_path(&file_system.file_system_id, &source.path)
                .expect("reopened source path"),
            None
        );
    }

    cleanup(&users_leader_root);
    cleanup(&services_leader_root);
    cleanup(&first_replica_root);
    cleanup(&second_replica_root);
}

#[test]
fn sharded_backend_pages_and_counts_hardlinks_across_shards() {
    let root = temp_root("sharded_backend_pages_hardlinks");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let file_system = file_system();
    backend.put_file_system(&file_system).expect("filesystem");

    let target = object(version("version_1"));
    let mut root_alias = object(version("version_1"));
    root_alias.object_id =
        DfsObjectId::from_legacy_named("obj_sharded_root_alias").expect("root alias");
    root_alias.path = "/z-root.alias".to_string();
    root_alias.kind = DfsObjectKind::Hardlink;
    root_alias.content_hash = None;
    root_alias.link_target_object_id = Some(target.object_id.clone());
    let mut service_alias = root_alias.clone();
    service_alias.object_id =
        DfsObjectId::from_legacy_named("obj_sharded_service_alias").expect("service alias");
    service_alias.path = "/services/a-service.alias".to_string();

    for object in [&target, &root_alias, &service_alias] {
        backend.put_object(object).expect("object");
    }

    assert_eq!(
        backend
            .hardlink_count_to_object(&file_system.file_system_id, &target.object_id)
            .expect("hardlink count"),
        2
    );
    assert_eq!(
        backend
            .first_hardlink_to_object(&file_system.file_system_id, &target.object_id)
            .expect("first hardlink")
            .expect("first hardlink")
            .path,
        "/services/a-service.alias"
    );
    assert_eq!(
        backend
            .first_hardlink_to_object_outside_prefix(
                &file_system.file_system_id,
                &target.object_id,
                "/services",
            )
            .expect("outside prefix")
            .expect("outside prefix")
            .path,
        "/z-root.alias"
    );

    let mut cursor = None;
    let mut paged_ids = BTreeSet::new();
    let mut saw_hardlink_shard_cursor = false;
    loop {
        let page = backend
            .hardlinks_to_object_page(
                &file_system.file_system_id,
                &target.object_id,
                cursor.as_ref(),
                1,
            )
            .expect("hardlink page");
        paged_ids.extend(page.objects.into_iter().map(|object| object.object_id));
        if page
            .next_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.shard_id.is_some())
        {
            saw_hardlink_shard_cursor = true;
        }
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    assert_eq!(
        paged_ids,
        [root_alias.object_id, service_alias.object_id]
            .into_iter()
            .collect::<BTreeSet<_>>()
    );
    assert!(
        saw_hardlink_shard_cursor,
        "sharded hardlink pages should carry shard cursors for bounded continuation"
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_keeps_source_object_when_cross_shard_target_write_fails() {
    let root = temp_root("sharded_backend_cross_shard_target_failure");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let file_system = file_system();
    let object = object(version("version_1"));
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("root object");
    block_shard_journal_writes(&root, "services");

    let mut moved = object.clone();
    moved.path = "/services/readme.md".to_string();
    moved.version = version("version_2");
    let error = backend
        .put_object(&moved)
        .expect_err("target shard journal failure should reject move");

    assert!(
        matches!(error, MetadataBackendError::Io { .. }),
        "target journal append failure should be surfaced as I/O: {error:?}"
    );
    assert_eq!(
        backend
            .get_object_by_path(&file_system.file_system_id, &object.path)
            .expect("source path")
            .as_ref()
            .map(|object| (&object.path, &object.version, &object.kind)),
        Some((&object.path, &object.version, &DfsObjectKind::File)),
        "failed target write must not tombstone or move the source object"
    );
    assert_eq!(
        backend
            .read_object_shard_index(&object.object_id)
            .expect("object shard index")
            .as_ref()
            .map(|index| (&index.path, &index.shard_id)),
        Some((&object.path, &DfsMetadataShardId::new("root"))),
        "failed target write must leave the object shard index on the source shard"
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_keeps_source_namespace_when_cross_shard_target_transaction_fails() {
    let root = temp_root("sharded_backend_cross_shard_transaction_failure");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let file_system = file_system();
    let object = object(version("version_1"));
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("root object");
    block_shard_journal_writes(&root, "services");

    let mut moved = object.clone();
    moved.path = "/services/readme.md".to_string();
    moved.version = version("version_2");
    let error = backend
        .apply_namespace_transaction(&DfsNamespaceTransaction {
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
            transaction_id: "move-to-services".to_string(),
            file_system_id: file_system.file_system_id.clone(),
            objects: vec![moved],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::now_utc(),
        })
        .expect_err("target shard journal failure should reject namespace move");

    assert!(
        matches!(error, MetadataBackendError::Io { .. }),
        "target namespace journal append failure should be surfaced as I/O: {error:?}"
    );
    assert_eq!(
        backend
            .get_object_by_path(&file_system.file_system_id, &object.path)
            .expect("source path")
            .as_ref()
            .map(|object| (&object.path, &object.version, &object.kind)),
        Some((&object.path, &object.version, &DfsObjectKind::File)),
        "failed target namespace transaction must keep the source object visible"
    );
    assert_eq!(
        backend
            .read_object_shard_index(&object.object_id)
            .expect("object shard index")
            .as_ref()
            .map(|index| (&index.path, &index.shard_id)),
        Some((&object.path, &DfsMetadataShardId::new("root"))),
        "failed target namespace transaction must leave the object shard index on the source shard"
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_hides_stale_source_path_when_cross_shard_tombstone_fails() {
    let root = temp_root("sharded_backend_cross_shard_tombstone_failure");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let file_system = file_system();
    let object = object(version("version_1"));
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("root object");
    block_shard_journal_writes(&root, "root");

    let mut moved = object.clone();
    moved.path = "/services/readme.md".to_string();
    moved.version = version("version_2");
    let error = backend
        .put_object(&moved)
        .expect_err("source tombstone journal failure should be surfaced");

    assert!(
        matches!(error, MetadataBackendError::Io { .. }),
        "source tombstone journal failure should be surfaced as I/O: {error:?}"
    );
    assert_eq!(
        backend
            .get_object(&object.object_id)
            .expect("object by id")
            .as_ref()
            .map(|object| (&object.path, &object.version)),
        Some((&moved.path, &moved.version)),
        "object id lookups should use the target shard after the target write and index update"
    );
    assert_eq!(
        backend
            .get_object_by_path(&file_system.file_system_id, &object.path)
            .expect("old source path"),
        None,
        "a stale non-tombstoned source object must not remain visible once the index points at the target shard"
    );
    assert_eq!(
        backend
            .get_object_by_path(&file_system.file_system_id, &moved.path)
            .expect("target path")
            .as_ref()
            .map(|object| (&object.path, &object.version)),
        Some((&moved.path, &moved.version))
    );

    cleanup(&root);
}

#[test]
fn local_backend_lists_hardlinks_from_target_index() {
    let root = temp_root("local_backend_lists_hardlinks_from_target_index");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let target = object(version("version_1"));
    let mut alias_a = object(version("version_1"));
    alias_a.object_id = DfsObjectId::from_legacy_named("obj_alias_a").expect("object");
    alias_a.path = "/docs/readme.alias-a".to_string();
    alias_a.kind = DfsObjectKind::Hardlink;
    alias_a.content_hash = None;
    alias_a.link_target_object_id = Some(target.object_id.clone());
    let mut alias_b = alias_a.clone();
    alias_b.object_id = DfsObjectId::from_legacy_named("obj_alias_b").expect("object");
    alias_b.path = "/docs/readme.alias-b".to_string();
    let mut alias_c = alias_a.clone();
    alias_c.object_id = DfsObjectId::from_legacy_named("obj_alias_c").expect("object");
    alias_c.path = "/archive/readme.alias-c".to_string();
    let mut unrelated = object(version("version_1"));
    unrelated.object_id = DfsObjectId::from_legacy_named("obj_unrelated").expect("object");
    unrelated.path = "/docs/unrelated".to_string();

    for object in [&target, &alias_b, &unrelated, &alias_a, &alias_c] {
        backend.put_object(object).expect("object");
    }
    let mut stale_alias = alias_b.clone();
    stale_alias.kind = DfsObjectKind::Tombstone;
    stale_alias.link_target_object_id = None;
    backend
        .put_object(&stale_alias)
        .expect("tombstone stale alias");

    let hardlinks = backend
        .hardlinks_to_object(&file_system_id(), &target.object_id)
        .expect("hardlinks");
    assert_eq!(
        hardlinks
            .iter()
            .map(|object| object.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/archive/readme.alias-c", "/docs/readme.alias-a"]
    );
    assert_eq!(
        backend
            .hardlink_count_to_object(&file_system_id(), &target.object_id)
            .expect("hardlink count"),
        2
    );
    assert_eq!(
        backend
            .first_hardlink_to_object(&file_system_id(), &target.object_id)
            .expect("first hardlink")
            .expect("first hardlink")
            .path,
        "/archive/readme.alias-c"
    );
    assert_eq!(
        backend
            .first_hardlink_to_object_outside_prefix(
                &file_system_id(),
                &target.object_id,
                "/archive",
            )
            .expect("outside prefix")
            .expect("outside prefix")
            .path,
        "/docs/readme.alias-a"
    );

    let first_page = backend
        .hardlinks_to_object_page(&file_system_id(), &target.object_id, None, 1)
        .expect("first page");
    assert_eq!(
        first_page
            .objects
            .iter()
            .map(|object| object.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/archive/readme.alias-c"]
    );
    let second_page = backend
        .hardlinks_to_object_page(
            &file_system_id(),
            &target.object_id,
            first_page.next_cursor.as_ref(),
            1,
        )
        .expect("second page");
    assert_eq!(
        second_page
            .objects
            .iter()
            .map(|object| object.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/docs/readme.alias-a"]
    );

    cleanup(&root);
}

#[test]
fn local_backend_removes_stale_hardlink_target_index_entries() {
    let root = temp_root("local_backend_removes_stale_hardlink_target_index_entries");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let target_a = object(version("version_1"));
    let mut target_b = object(version("version_1"));
    target_b.object_id = DfsObjectId::from_legacy_named("obj_target_b").expect("object");
    target_b.path = "/docs/target-b.md".to_string();
    let mut alias = object(version("version_1"));
    alias.object_id = DfsObjectId::from_legacy_named("obj_alias").expect("object");
    alias.path = "/docs/readme.alias".to_string();
    alias.kind = DfsObjectKind::Hardlink;
    alias.content_hash = None;
    alias.link_target_object_id = Some(target_a.object_id.clone());

    for object in [&target_a, &target_b, &alias] {
        backend.put_object(object).expect("object");
    }
    assert!(table_hardlink_exists(
        &backend,
        &target_a.object_id,
        &alias.object_id
    ));

    alias.link_target_object_id = Some(target_b.object_id.clone());
    backend.put_object(&alias).expect("retarget alias");
    assert!(
        !table_hardlink_exists(&backend, &target_a.object_id, &alias.object_id),
        "retargeting a hardlink should remove its old target index entry"
    );
    assert!(table_hardlink_exists(
        &backend,
        &target_b.object_id,
        &alias.object_id
    ));

    alias.kind = DfsObjectKind::Tombstone;
    alias.link_target_object_id = None;
    backend.put_object(&alias).expect("tombstone alias");
    assert!(
        !table_hardlink_exists(&backend, &target_b.object_id, &alias.object_id),
        "tombstoning a hardlink should remove its target index entry"
    );
    assert!(
        backend
            .hardlinks_to_object(&file_system_id(), &target_b.object_id)
            .expect("hardlinks")
            .is_empty()
    );

    cleanup(&root);
}

#[test]
fn metadata_writer_adopts_rotated_live_generation_without_prepare_reference_replay() {
    let root = temp_root("local_backend_writer_adopts_rotated_generation");
    let shard_id = DfsMetadataShardId::new("root");
    let current = object(version("version_rotated_writer_base"));
    let mut committed = current.clone();
    committed.version = version("version_rotated_writer_committed");
    committed.mode = 0o600;
    let prepare = namespace_prepare(
        "operation-rotated-writer",
        &shard_id.0,
        &current,
        committed.clone(),
    );
    let mut backend = LocalSobjMetadataBackend::new(shard_id, &root).expect("backend");
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV1);
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .prepare_namespace_transaction(&prepare)
        .expect("durable prepare");
    backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("compact commit");
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("committed object"),
        Some(committed.clone())
    );
    {
        let _lock = backend.metadata_write_lock().expect("metadata writer lock");
        assert!(
            crate::metadata_table_store::rotate_metadata_table_log_for_checkpoint(&root, 0)
                .expect("rotate table generation")
        );
    }
    assert!(metadata_table_checkpoint_log_path(&root).is_file());

    crate::metadata_table_store::reset_metadata_table_journal_reference_resolutions();
    let mut updated_file_system = file_system();
    updated_file_system.display_name = "updated after checkpoint rotation".to_string();
    backend
        .put_file_system(&updated_file_system)
        .expect("write after representation-only rotation");

    let active_table_log_len = fs::metadata(metadata_table_path(&root))
        .expect("active table log metadata")
        .len();
    assert_eq!(
        backend.table_log_offset, active_table_log_len,
        "checkpoint adoption must rebase active-log accounting before the first new append"
    );
    assert_eq!(
        backend.table_log_journal_reference_replay_cost_bytes, 0,
        "the new active suffix contains no journal-reference replay debt"
    );
    assert_eq!(
        backend.table_log_unpriced_namespace_prepare_references, 0,
        "the new active suffix contains no unpriced namespace prepares"
    );
    assert_eq!(
        crate::metadata_table_store::metadata_table_journal_reference_resolutions(),
        0,
        "the writer already materialized the exact rotated generation and must not decode its namespace prepares again"
    );
    assert_eq!(
        backend
            .get_file_system(&updated_file_system.file_system_id)
            .expect("updated filesystem"),
        Some(updated_file_system)
    );
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("preserved committed object"),
        Some(committed),
        "adopting the rotated representation must preserve the materialized namespace"
    );

    cleanup(&root);
}

#[test]
fn cross_shard_pin_transfer_is_atomic_and_fences_owned_pins_until_finalize() {
    let root = temp_root("cross-shard-pin-transfer-fence");
    let shard_id = DfsMetadataShardId::new("target");
    let mut backend = LocalSobjMetadataBackend::new(shard_id, &root).expect("target backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let pinned_version = version("version_transfer_pinned");
    let target_version = version("version_transfer_target");
    let mut moved = object(target_version.clone());
    moved.path = "/archive/readme.md".to_string();
    let pinned_record =
        object_version_record(&moved, pinned_version.clone(), OffsetDateTime::UNIX_EPOCH);
    let target_record = object_version_record(
        &moved,
        target_version,
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
    );
    let pin = DfsMetadataVersionPinRecord {
        namespace: None,
        file_system_id: moved.file_system_id.clone(),
        object_id: moved.object_id.clone(),
        version: pinned_version,
        pin_id: "checkpoint-transfer".to_string(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };

    let before = backend.journal_last_sequence();
    assert!(
        backend
            .reconcile_cross_shard_pin_transfer(
                "transfer-fence",
                &moved,
                "/docs/readme.md",
                &[pinned_record, target_record],
                std::slice::from_ref(&pin),
            )
            .expect("atomic transfer reconcile")
    );
    assert_eq!(
        backend.journal_last_sequence(),
        before + 1,
        "versions, owned pins, and provenance share one journal mutation"
    );
    assert_eq!(
        backend
            .cross_shard_pin_transfers_for_object(&moved.object_id)
            .expect("transfer scan")[0]
            .copied_pins,
        vec![pin.clone()]
    );
    assert!(
        backend
            .put_version_pin(&pin)
            .expect_err("exact duplicate put must remain fenced")
            .to_string()
            .contains("active cross-shard transfer")
    );
    assert!(
        backend
            .remove_version_pin(
                &pin.file_system_id,
                &pin.object_id,
                &pin.version,
                &pin.pin_id,
            )
            .expect_err("owned pin removal must remain fenced")
            .to_string()
            .contains("active cross-shard transfer")
    );

    backend
        .finalize_cross_shard_pin_transfer("transfer-fence", &moved, "/docs/readme.md")
        .expect("finalize transfer");
    assert!(
        backend
            .cross_shard_pin_transfers_for_object(&moved.object_id)
            .expect("finalized transfer scan")
            .is_empty()
    );
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

    cleanup(&root);
}

#[test]
fn cross_shard_pin_transfer_preserves_preexisting_identical_target_pin() {
    let root = temp_root("cross-shard-pin-transfer-independent");
    let mut backend = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("target"), &root)
        .expect("target backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let pinned_version = version("version_transfer_independent");
    let target_version = version("version_transfer_independent_target");
    let mut moved = object(target_version.clone());
    moved.path = "/archive/readme.md".to_string();
    let pinned_record =
        object_version_record(&moved, pinned_version.clone(), OffsetDateTime::UNIX_EPOCH);
    let target_record = object_version_record(&moved, target_version, OffsetDateTime::UNIX_EPOCH);
    backend
        .put_version(&pinned_record)
        .expect("pre-existing pinned version");
    let independent_pin = DfsMetadataVersionPinRecord {
        namespace: None,
        file_system_id: moved.file_system_id.clone(),
        object_id: moved.object_id.clone(),
        version: pinned_version,
        pin_id: "checkpoint-independent".to_string(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    backend
        .put_version_pin(&independent_pin)
        .expect("pre-existing independent pin");

    assert!(
        backend
            .reconcile_cross_shard_pin_transfer(
                "transfer-independent",
                &moved,
                "/docs/readme.md",
                &[pinned_record, target_record],
                std::slice::from_ref(&independent_pin),
            )
            .expect("reconcile independent pin")
    );
    let transfers = backend
        .cross_shard_pin_transfers_for_object(&moved.object_id)
        .expect("transfer scan");
    assert_eq!(transfers.len(), 1);
    assert!(
        transfers[0].copied_pins.is_empty(),
        "an exact pre-existing target pin must not become transfer-owned"
    );
    assert_eq!(transfers[0].source_pins, vec![independent_pin.clone()]);
    assert!(
        backend
            .remove_version_pin(
                &independent_pin.file_system_id,
                &independent_pin.object_id,
                &independent_pin.version,
                &independent_pin.pin_id,
            )
            .expect_err("acknowledged independent pin must remain fenced")
            .to_string()
            .contains("active cross-shard transfer")
    );
    backend
        .finalize_cross_shard_pin_transfer("transfer-independent", &moved, "/docs/readme.md")
        .expect("finalize independent transfer");
    assert_eq!(
        backend
            .version_pins_for_object(&moved.object_id)
            .expect("independent pin remains"),
        vec![independent_pin]
    );

    cleanup(&root);
}

#[test]
fn cross_shard_source_eviction_replays_as_one_atomic_pin_and_index_transition() {
    let root = temp_root("cross-shard-source-eviction-atomic");
    let shard_id = DfsMetadataShardId::new("source");
    let mut source =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source")).expect("source");
    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("replica")).expect("replica");
    source.put_file_system(&file_system()).expect("filesystem");

    let pinned_version = version("version_source_eviction_pinned");
    let mut hardlink = object(pinned_version.clone());
    hardlink.kind = DfsObjectKind::Hardlink;
    hardlink.link_target_object_id = Some(
        DfsObjectId::from_legacy_named("obj_source_eviction_target").expect("hardlink target id"),
    );
    let pinned_record = object_version_record(
        &hardlink,
        pinned_version.clone(),
        OffsetDateTime::UNIX_EPOCH,
    );
    source.put_object(&hardlink).expect("hardlink source");
    source.put_version(&pinned_record).expect("pinned version");
    let pin = DfsMetadataVersionPinRecord {
        namespace: None,
        file_system_id: hardlink.file_system_id.clone(),
        object_id: hardlink.object_id.clone(),
        version: pinned_version,
        pin_id: "checkpoint-source-eviction".to_string(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    source.put_version_pin(&pin).expect("source pin");

    let initial = source
        .journal_batch_since_limited(0, 32, usize::MAX)
        .expect("initial journal")
        .expect("initial batch");
    replica.apply_journal_batch(&initial).expect("seed replica");
    let fenced_sequence = source.journal_last_sequence();
    let mut moved = hardlink.clone();
    moved.path = "/archive/readme.md".to_string();
    moved.version = version("version_source_eviction_target");
    moved.changed_at_unix_nanos = 1;
    source
        .put_cross_shard_tombstone_if_unchanged(
            &hardlink,
            &moved,
            &shard_id,
            &DfsMetadataShardId::new("target"),
            fenced_sequence,
            Some("transfer-source-eviction"),
        )
        .expect("atomic source eviction");

    let eviction = source
        .journal_batch_since_limited(initial.next_after_sequence, 8, usize::MAX)
        .expect("eviction journal")
        .expect("eviction batch");
    assert_eq!(eviction.entries.len(), 1);
    assert!(matches!(
        eviction.entries[0].mutation,
        DfsMetadataMutation::FinalizeCrossShardSourceEviction { .. }
    ));
    assert_eq!(
        replica
            .get_object(&hardlink.object_id)
            .expect("replica pre-eviction object"),
        Some(hardlink.clone())
    );
    assert_eq!(
        replica
            .version_pins_for_object(&hardlink.object_id)
            .expect("replica pre-eviction pins"),
        vec![pin]
    );
    replica
        .apply_journal_batch(&eviction)
        .expect("apply atomic eviction");
    let tombstone = replica
        .get_object(&hardlink.object_id)
        .expect("replica post-eviction object")
        .expect("source tombstone");
    assert_eq!(tombstone.kind, DfsObjectKind::Tombstone);
    assert!(matches!(
        tombstone.tombstone_provenance,
        Some(crate::DfsTombstoneProvenance::SourceEviction {
            pin_transfer_id: Some(ref transfer_id),
            ..
        }) if transfer_id == "transfer-source-eviction"
    ));
    assert!(
        replica
            .version_pins_for_object(&hardlink.object_id)
            .expect("replica post-eviction pins")
            .is_empty()
    );
    assert!(
        replica
            .hardlinks_to_object(
                &hardlink.file_system_id,
                hardlink
                    .link_target_object_id
                    .as_ref()
                    .expect("hardlink target"),
            )
            .expect("post-eviction hardlink index")
            .is_empty()
    );

    drop(replica);
    let reopened =
        LocalSobjMetadataBackend::new(shard_id, root.join("replica")).expect("reopen replica");
    assert!(
        reopened
            .version_pins_for_object(&hardlink.object_id)
            .expect("reopened pins")
            .is_empty()
    );
    assert_eq!(
        reopened
            .get_object(&hardlink.object_id)
            .expect("reopened tombstone")
            .expect("reopened object")
            .kind,
        DfsObjectKind::Tombstone
    );

    cleanup(&root);
}

#[test]
fn replicated_snapshot_rejects_invalid_cross_shard_pin_transfer_ownership() {
    let root = temp_root("cross-shard-pin-transfer-snapshot-validation");
    let shard_id = DfsMetadataShardId::new("root");
    let mut source =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("source")).expect("source");
    let current_version = version("version_transfer_snapshot_current");
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
    let owned_pin = DfsMetadataVersionPinRecord {
        namespace: None,
        file_system_id: current.file_system_id.clone(),
        object_id: current.object_id.clone(),
        version: current_version.clone(),
        pin_id: "checkpoint-transfer-snapshot".to_string(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    let transfer = DfsCrossShardPinTransferRecord {
        transfer_id: "transfer-snapshot".to_string(),
        file_system_id: current.file_system_id.clone(),
        object_id: current.object_id.clone(),
        source_path: "docs/readme.md".to_string(),
        target_path: "archive/readme.md".to_string(),
        target_version: current_version.clone(),
        source_pins: vec![owned_pin.clone()],
        copied_pins: vec![owned_pin.clone()],
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::UNIX_EPOCH,
    };
    let mut replica =
        LocalSobjMetadataBackend::new(shard_id.clone(), root.join("replica")).expect("replica");

    let mut missing_owned_pin = snapshot.clone();
    missing_owned_pin
        .tables
        .as_mut()
        .expect("structured snapshot")
        .cross_shard_pin_transfers
        .push(transfer.clone());
    missing_owned_pin.semantic_state_fingerprint = Some(
        dfs_metadata_table_snapshot_state_fingerprint(&missing_owned_pin)
            .expect("missing-owned-pin fingerprint"),
    );
    assert!(
        replica
            .replace_with_replicated_snapshot(shard_id.clone(), sequence, missing_owned_pin,)
            .expect_err("missing transfer-owned pin must reject")
            .to_string()
            .contains("missing its owned pin")
    );

    let mut overlapping_owner = snapshot.clone();
    let overlapping_tables = overlapping_owner
        .tables
        .as_mut()
        .expect("structured snapshot");
    overlapping_tables.version_pins.push(owned_pin.clone());
    overlapping_tables
        .cross_shard_pin_transfers
        .push(transfer.clone());
    overlapping_tables
        .cross_shard_pin_transfers
        .push(DfsCrossShardPinTransferRecord {
            transfer_id: "transfer-snapshot-other".to_string(),
            ..transfer.clone()
        });
    overlapping_owner.semantic_state_fingerprint = Some(
        dfs_metadata_table_snapshot_state_fingerprint(&overlapping_owner)
            .expect("overlapping-owner fingerprint"),
    );
    assert!(
        replica
            .replace_with_replicated_snapshot(shard_id.clone(), sequence, overlapping_owner,)
            .expect_err("overlapping transfer ownership must reject")
            .to_string()
            .contains("another active cross-shard pin transfer")
    );

    let mut missing_target_version = snapshot;
    let missing_target_tables = missing_target_version
        .tables
        .as_mut()
        .expect("structured snapshot");
    missing_target_tables.version_pins.push(owned_pin);
    missing_target_tables
        .cross_shard_pin_transfers
        .push(DfsCrossShardPinTransferRecord {
            target_version: version("version_transfer_snapshot_missing_target"),
            ..transfer
        });
    missing_target_version.semantic_state_fingerprint = Some(
        dfs_metadata_table_snapshot_state_fingerprint(&missing_target_version)
            .expect("missing-target-version fingerprint"),
    );
    assert!(
        replica
            .replace_with_replicated_snapshot(shard_id, sequence, missing_target_version)
            .expect_err("missing transfer target version must reject")
            .to_string()
            .contains("missing target version")
    );

    cleanup(&root);
}
