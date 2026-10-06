//! Reads regression tests.

use super::*;

#[test]
fn sharded_backend_moves_object_between_path_shards_without_scanning_on_read() {
    let root = temp_root("sharded_backend_moves_object_between_path_shards");
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
            shard_config(
                "users",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/users".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let file_system = file_system();
    let mut object = object(version("version_1"));
    object.path = "/services/readme.md".to_string();
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&object).expect("services object");

    let mut moved = object.clone();
    moved.path = "/users/readme.md".to_string();
    moved.version = version("version_2");
    backend.put_object(&moved).expect("moved object");

    assert_eq!(
        backend
            .get_object(&object.object_id)
            .expect("get moved object")
            .as_ref()
            .map(|object| object.path.as_str()),
        Some("/users/readme.md")
    );
    assert_eq!(
        backend
            .objects_for_file_system(&file_system.file_system_id)
            .expect("objects")
            .into_iter()
            .map(|object| object.path)
            .collect::<Vec<_>>(),
        vec!["/users/readme.md".to_string()]
    );
    assert_eq!(
        backend
            .get_object_by_path(&file_system.file_system_id, "/services/readme.md")
            .expect("old path")
            .map(|object| object.kind),
        None,
        "source-eviction storage evidence is not a logical namespace tombstone"
    );

    cleanup(&root);
}

#[test]
fn sharded_path_and_hydration_reads_ignore_stale_or_missing_replica_index() {
    let root = temp_root("sharded_path_stale_replica_index");
    let file_system = file_system();
    let mut services = object(version("version_services_anchor"));
    services.object_id = DfsObjectId::from_legacy_named("obj_services_anchor").expect("object id");
    services.path = "/services".to_string();
    services.kind = DfsObjectKind::Directory;
    services.content_hash = None;
    services.content_size_bytes = None;

    {
        let mut root_only = LocalShardedMetadataBackend::new(
            &root,
            vec![shard_config("root", DfsMetadataShardAssignment::Root)],
        )
        .expect("root backend");
        root_only.put_file_system(&file_system).expect("filesystem");
        root_only.put_object(&services).expect("root anchor");
    }

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
    backend
        .migrate_objects_to_configured_shards(&file_system.file_system_id, 8)
        .expect("migrate anchor");
    let root_shard_id = DfsMetadataShardId::new("root");
    let services_shard_id = DfsMetadataShardId::new("services");
    assert_eq!(
        backend
            .shard_backend(&root_shard_id)
            .expect("root shard")
            .get_object(&services.object_id)
            .expect("root row")
            .expect("source tombstone")
            .kind,
        DfsObjectKind::Tombstone
    );
    assert_eq!(
        backend
            .shard_backend(&services_shard_id)
            .expect("services shard")
            .get_object(&services.object_id)
            .expect("services row"),
        Some(services.clone())
    );

    for replica_index in [Some(root_shard_id.clone()), None] {
        match replica_index {
            Some(ref shard_id) => backend
                .write_object_shard_index(&services, shard_id)
                .expect("stale replica index"),
            None => std::fs::remove_file(backend.object_shard_index_path(&services.object_id))
                .expect("remove replica index"),
        }
        assert_eq!(
            backend
                .get_object_by_path(&file_system.file_system_id, &services.path)
                .expect("path lookup"),
            Some(services.clone()),
            "path-selected live row must not depend on a derived replica index"
        );
        assert_eq!(
            backend.get_object(&services.object_id).expect("id lookup"),
            Some(services.clone()),
            "by-id lookup must not select the retired source tombstone"
        );
        assert_eq!(
            backend
                .objects_for_file_system(&file_system.file_system_id)
                .expect("hydration scan"),
            vec![services.clone()],
            "merged hydration must expose only the routed live row"
        );
    }

    backend
        .write_object_shard_index(&services, &root_shard_id)
        .expect("stale index before refresh");
    backend
        .refresh_metadata_shard_durable_view(&services_shard_id)
        .expect("refresh services view");
    assert_eq!(
        backend
            .read_object_shard_index(&services.object_id)
            .expect("repaired index")
            .map(|index| index.shard_id),
        Some(services_shard_id),
        "durable-view refresh must repair derived placement after visibility is established"
    );

    cleanup(&root);
}

#[test]
fn sharded_backend_skips_stale_source_when_target_shard_already_has_object() {
    let root = temp_root("sharded_backend_skips_stale_source_target_object");
    let file_system = file_system();
    let mut stale = object(version("version_1"));
    stale.path = "/services/readme.md".to_string();
    let mut current = stale.clone();
    current.content_hash = Some("blake3:current-object".to_string());
    current.content_size_bytes = Some(7);
    let stale_version = DfsObjectVersionRecord {
        content_durability: None,
        metadata_format: crate::DfsRevisionMetadataFormat::Legacy,
        retention: Default::default(),
        module_metadata: Default::default(),
        file_system_id: stale.file_system_id.clone(),
        object_id: stale.object_id.clone(),
        version: stale.version.clone(),
        content_hash: Some("blake3:stale-version".to_string()),
        size_bytes: Some(13),
        locations: stale.current_locations.clone(),
        created_by_event_id: stale.created_by_event_id.clone(),
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    };
    let current_version = DfsObjectVersionRecord {
        content_durability: None,
        content_hash: current.content_hash.clone(),
        size_bytes: current.content_size_bytes,
        ..stale_version.clone()
    };
    {
        let mut root_only = LocalShardedMetadataBackend::new(
            &root,
            vec![shard_config("root", DfsMetadataShardAssignment::Root)],
        )
        .expect("root-only sharded backend");
        root_only.put_file_system(&file_system).expect("filesystem");
        root_only.put_object(&stale).expect("stale root object");
        root_only
            .put_version(&stale_version)
            .expect("stale root version");
    }

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
    {
        let services = backend
            .shard_backend_mut(&DfsMetadataShardId::new("services"))
            .expect("services shard");
        services
            .put_file_system(&file_system)
            .expect("services filesystem");
        services
            .put_object(&current)
            .expect("current services object");
        services
            .put_version(&current_version)
            .expect("current services version");
    }

    let report = backend
        .migrate_objects_to_configured_shards(&file_system.file_system_id, 16)
        .expect("migration skips stale root copy");

    assert_eq!(
        report,
        DfsMetadataShardMigrationReport {
            file_system_id: file_system.file_system_id.clone(),
            scanned_objects: 1,
            indexed_objects: 1,
            migrated_objects: 0,
            skipped_stale_sources: 1,
            reached_limit: false,
        }
    );
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("current object"),
        Some(current.clone())
    );
    assert_eq!(
        backend
            .get_object_by_path(&file_system.file_system_id, &current.path)
            .expect("current path"),
        Some(current.clone())
    );
    assert_eq!(
        backend
            .read_object_shard_index(&current.object_id)
            .expect("object index")
            .as_ref()
            .map(|index| (&index.path, &index.shard_id)),
        Some((
            &normalize_path(&current.path),
            &DfsMetadataShardId::new("services")
        ))
    );
    assert_eq!(
        backend
            .get_version(&current.object_id, &current.version)
            .expect("current version"),
        Some(current_version)
    );

    cleanup(&root);
}

#[test]
fn local_backend_readers_see_table_batches_from_other_open_handles() {
    let root = temp_root("local_backend_readers_see_table_batches_from_other_open_handles");
    let mut first = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("first backend");
    let second = LocalSobjMetadataBackend::new(DfsMetadataShardId::new("shard-root"), &root)
        .expect("second backend");
    let file_system = file_system();
    let object = object(version("version_1"));

    first.put_file_system(&file_system).expect("filesystem");
    first.put_object(&object).expect("object");

    assert_eq!(
        second
            .get_file_system(&file_system.file_system_id)
            .expect("filesystem from second handle"),
        Some(file_system.clone())
    );
    assert_eq!(
        second
            .get_object(&object.object_id)
            .expect("object from second handle"),
        Some(object.clone())
    );
    assert_eq!(
        second
            .objects_for_file_system(&file_system.file_system_id)
            .expect("objects from second handle"),
        vec![object]
    );

    cleanup(&root);
}
