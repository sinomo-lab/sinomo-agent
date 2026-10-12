//! Retention regression tests.

use super::*;
use sinomo_core::object::serde_ext as sinomo_object_serde;

#[test]
fn native_lifecycle_catalog_keeps_current_content_and_rejects_replayed_authority() {
    let node = NodeId::generate();
    let (runtime, mut dfs) = native_sifs_service("native-lifecycle-catalog", node.clone());
    dfs.posix_write_file(
        "/gc-current",
        b"old",
        DfsWriteOptions::replicated_eventual(vec![node.clone()]),
    )
    .unwrap();
    let current = dfs
        .posix_write_file(
            "/gc-current",
            b"current",
            DfsWriteOptions::replicated_eventual(vec![node.clone()]),
        )
        .unwrap();
    let mut control = runtime.shared_control_plane().unwrap();
    let fs = runtime.file_system().file_system_id;
    let config = control
        .with_client(|kv| kv.get(&file_system_config_key(&fs)))
        .unwrap()
        .unwrap()
        .decode::<DfsFileSystemConfig>()
        .unwrap();
    let shards = control
        .with_client(|kv| kv.range_prefix(&metadata_shards_prefix(&fs)))
        .unwrap()
        .into_iter()
        .map(|entry| entry.decode::<DfsMetadataShardConfig>().unwrap())
        .collect::<Vec<_>>();
    let root = runtime.data_dir(&node).dfs_dir().join("content");
    let catalog = dfs
        .refresh_content_root_catalog(std::slice::from_ref(&config), &shards, &[], &root, &[], 32)
        .unwrap();
    let validation = catalog
        .validate_for_collection_with_head_watermark_policy(
            std::slice::from_ref(&config),
            &shards,
            &[],
            &[],
            false,
            dfs_now_unix_nanos(),
            Duration::from_secs(30),
        )
        .unwrap();
    assert!(validation.is_collection_authorized());
    let replayed: DfsContentRootCatalogCollectionValidation = sinomo_object_serde::from_bytes(
        &sinomo_object_serde::to_bytes(&validation, 1, sinomo_core::object::LengthMode::U32)
            .unwrap(),
    )
    .unwrap();
    assert!(!replayed.is_collection_authorized());
    let denied = dfs
        .run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
            content_root_catalog_validation: Some(replayed),
            version_gc: Some(ContentStoreVersionGcOptions::delete_eligible(
                Duration::ZERO,
                1,
            )),
            orphan_gc: Some(ContentStoreGcOptions::delete_eligible(Duration::ZERO)),
            ..Default::default()
        })
        .unwrap();
    assert!(denied.content_gc_metadata_protection_unavailable);
    assert!(denied.version_gc.is_none() && denied.orphan_gc.is_none());
    let admitted = dfs
        .run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
            scrub_content: true,
            content_root_catalog_validation: Some(validation),
            content_reservation_reconciliation: Some(
                ContentStoreReservationReconciliationOptions::bounded(
                    64,
                    Duration::ZERO,
                    dfs_now_unix_nanos(),
                ),
            ),
            version_gc: Some(ContentStoreVersionGcOptions::delete_eligible(
                Duration::ZERO,
                1,
            )),
            orphan_gc: Some(ContentStoreGcOptions::delete_eligible(Duration::ZERO)),
            ..Default::default()
        })
        .unwrap();
    assert!(admitted.content_scrub.is_some());
    assert!(admitted.version_gc.is_some() && admitted.orphan_gc.is_some());
    assert_eq!(dfs.posix_read_file("/gc-current").unwrap(), b"current");
    assert!(
        dfs.content
            .read_manifest(current.object_id.as_str(), current.version.as_str())
            .is_ok()
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn lifecycle_maintenance_reports_skipped_tombstones_without_backend() {
    let root = temp_root("lifecycle-maintenance-no-backend");
    let node_id = NodeId::from_legacy_named("node_lifecycle_no_backend").expect("node");
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node_id),
        LocalContentStore::new(root.join("content")).expect("content"),
    );

    let report = dfs
        .run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
            tombstone_compaction: Some(DfsTombstoneCompactionOptions::compact_eligible(
                std::time::Duration::from_nanos(0),
            )),
            ..DfsLifecycleMaintenanceOptions::default()
        })
        .expect("lifecycle maintenance without backend");

    assert!(!report.metadata_backend_configured);
    assert!(report.tombstone_compaction_requested);
    assert!(report.tombstone_compaction.is_none());
    assert!(report.content_scrub.is_none());
    assert!(report.version_gc.is_none());
    assert!(report.orphan_gc.is_none());

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn lifecycle_shared_gc_runs_once_without_file_system_records() {
    let root = temp_root("lifecycle-shared-gc-without-file-systems");
    let node_id = NodeId::from_legacy_named("node_lifecycle_empty_catalog").expect("node");
    let metadata =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.join("metadata"))
            .expect("metadata backend");
    assert!(
        metadata
            .list_file_systems()
            .expect("empty filesystem catalog")
            .is_empty()
    );
    let mut dfs = NetworkedDfsService::new(
        crate::test_runtime::DfsTestRuntime::local_transport(node_id),
        LocalContentStore::new(root.join("content")).expect("content store"),
    )
    .with_metadata_backend(metadata)
    .expect("attach metadata backend");
    let mut host = RecordingMaintenanceHost::default();

    let reports = run_dfs_lifecycle_maintenance_with_host(
        &mut dfs,
        &mut host,
        DfsMaintenanceRunOptions {
            cleanup_stale_staging: true,
            stale_staging_min_age: Duration::ZERO,
            compact_tombstones: true,
            tombstone_compaction_min_age: Duration::ZERO,
            collect_metadata_history: true,
            metadata_history_gc_min_age: Duration::ZERO,
            collect_content_versions: true,
            content_version_gc_min_age: Duration::ZERO,
            collect_orphan_content: true,
            content_orphan_gc_min_age: Duration::ZERO,
            ..DfsMaintenanceRunOptions::default()
        },
    )
    .expect("shared lifecycle maintenance");

    assert_eq!(
        reports.len(),
        1,
        "shared maintenance must execute exactly once without filesystem-scoped anti-entropy work"
    );
    let report = &reports[0];
    assert!(report.stale_staging_gc.is_some());
    assert!(report.metadata_history_gc.is_some());
    assert!(report.version_gc.is_none());
    assert!(report.orphan_gc.is_none());
    assert!(report.content_gc_metadata_protection_unavailable);
    assert!(report.tombstone_compaction.is_some());
    assert!(report.anti_entropy.is_none());
    assert!(host.stored.is_empty());
    assert!(host.cleared.is_empty());

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn stat_prefers_backend_tombstone_over_stale_memory_file() {
    let root = temp_root("stat-prefers-backend-tombstone");
    let owner = NodeId::from_legacy_named("node_stat_tombstone_owner").expect("owner");
    let object_id = DfsObjectId::from_legacy_named("obj_stat_tombstone").expect("object");
    let path = "/stat-tombstone.txt";
    let (_runtime, mut dfs) = native_sifs_service(
        "stat_prefers_backend_tombstone_over_stale_memory_file",
        owner.clone(),
    );
    let mut tombstone = file_metadata(owner.clone());
    tombstone.object_id = object_id.clone();
    tombstone.path = path.to_string();
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.version = DfsVersion::from_legacy_named("version_2").expect("version");
    tombstone.content_hash = None;
    tombstone.content_size_bytes = None;
    tombstone.current_locations = vec![owner.clone()];
    dfs.put_visible_object_in_metadata_backend(&tombstone)
        .expect("backend tombstone");

    let mut stale_memory = tombstone.clone();
    stale_memory.kind = DfsObjectKind::File;
    stale_memory.version = DfsVersion::from_legacy_named("version_3").expect("version");
    stale_memory.content_hash = Some("blake3:stale-memory-file".to_string());
    stale_memory.content_size_bytes = Some(12);
    dfs.metadata
        .add_object(stale_memory)
        .expect("stale memory file");

    let error = dfs
        .posix_stat_distributed(path)
        .expect_err("backend tombstone must hide stale memory file");

    assert!(matches!(error, DfsPosixError::NotFound { .. }));
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn tombstone_tracking_ignores_stale_live_path_metadata() {
    let root = temp_root("tombstone-ignores-stale-live");
    let owner = NodeId::from_legacy_named("node_tombstone_stale_live").expect("owner");
    let path = "/tombstone-stale-live.txt";
    let (_runtime, mut dfs) = native_sifs_service(
        "tombstone_tracking_ignores_stale_live_path_metadata",
        owner.clone(),
    );
    dfs.tombstones_by_metadata_path.clear();
    let stale_version = DfsVersion::new("00000000000000000000000001").expect("version");
    let tombstone_version = DfsVersion::new("00000000000000000000000002").expect("version");
    let fresh_version = DfsVersion::new("00000000000000000000000003").expect("version");

    let mut tombstone = file_metadata(owner.clone());
    tombstone.object_id =
        DfsObjectId::from_legacy_named("obj_tombstone_stale_live_delete").expect("object");
    tombstone.path = path.to_string();
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.version = tombstone_version;
    dfs.track_metadata_tombstone(&tombstone);

    let key = (sifs_file_system_id(), metadata_path(path));
    assert!(
        dfs.tombstones_by_metadata_path.contains_key(&key),
        "newer tombstone should be tracked for the deleted path"
    );

    let mut stale_live = file_metadata(owner.clone());
    stale_live.object_id =
        DfsObjectId::from_legacy_named("obj_tombstone_stale_live_old").expect("object");
    stale_live.path = path.to_string();
    stale_live.version = stale_version;
    dfs.track_hydrated_metadata_tombstone(&stale_live);
    assert!(
        dfs.tombstones_by_metadata_path.contains_key(&key),
        "older live metadata must not clear a newer tombstone"
    );

    let mut fresh_live = stale_live;
    fresh_live.object_id =
        DfsObjectId::from_legacy_named("obj_tombstone_stale_live_fresh").expect("object");
    fresh_live.version = fresh_version;
    dfs.track_hydrated_metadata_tombstone(&fresh_live);
    assert!(
        !dfs.tombstones_by_metadata_path.contains_key(&key),
        "newer live metadata should clear the older tombstone"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn hydrated_tombstone_tracking_is_incremental_and_live_path_aware() {
    let root = temp_root("hydrate-incremental-tombstones");
    let node_id = NodeId::from_legacy_named("node_hydrate_tombstones").expect("node");
    let (_runtime, mut dfs) = native_sifs_service(
        "hydrated_tombstone_tracking_is_incremental_and_live_path_aware",
        node_id.clone(),
    );
    dfs.metadata = DfsMetadataStore::new();
    dfs.metadata.add_file_system(file_system(node_id.clone()));
    dfs.tombstones_by_metadata_path.clear();

    let mut live_first = file_metadata(node_id.clone());
    live_first.object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_live_first").expect("object");
    live_first.path = "/users/hydrate/live-first.txt".to_string();
    dfs.metadata
        .add_object(live_first.clone())
        .expect("live object");

    let mut late_tombstone = file_metadata(node_id.clone());
    late_tombstone.object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_late_tombstone").expect("object");
    late_tombstone.kind = DfsObjectKind::Tombstone;
    late_tombstone.path = live_first.path.clone();
    dfs.track_hydrated_metadata_tombstone(&late_tombstone);
    assert!(
        !dfs.tombstones_by_metadata_path
            .contains_key(&(sifs_file_system_id(), metadata_path(&late_tombstone.path))),
        "hydration must not retain a tombstone for a path already owned by a live object"
    );

    let mut early_tombstone = file_metadata(node_id.clone());
    early_tombstone.object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_early_tombstone").expect("object");
    early_tombstone.kind = DfsObjectKind::Tombstone;
    early_tombstone.path = "/users/hydrate/live-after-tombstone.txt".to_string();
    dfs.track_hydrated_metadata_tombstone(&early_tombstone);
    assert!(
        dfs.tombstones_by_metadata_path
            .contains_key(&(sifs_file_system_id(), metadata_path(&early_tombstone.path))),
        "a tombstone should be tracked until hydration sees a live object for the path"
    );

    let mut live_later = file_metadata(node_id);
    live_later.object_id =
        DfsObjectId::from_legacy_named("obj_hydrate_live_later").expect("object");
    live_later.path = early_tombstone.path.clone();
    dfs.track_hydrated_metadata_tombstone(&live_later);
    assert!(
        !dfs.tombstones_by_metadata_path
            .contains_key(&(sifs_file_system_id(), metadata_path(&early_tombstone.path))),
        "a later live object should clear a previously tracked hydrated tombstone"
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn restart_preserves_current_create_after_compacted_tombstone() {
    let node = NodeId::generate();
    let (mut runtime, mut service) =
        native_sifs_service("native-compacted-path-reuse", node.clone());
    let path = "/services/reused-path";
    let options = DfsWriteOptions::replicated_eventual(vec![node.clone()]);
    let old = service
        .posix_write_file(path, b"old bytes", options.clone())
        .unwrap();
    service.posix_unlink(path).unwrap();
    service
        .run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
            tombstone_compaction: Some(
                DfsTombstoneCompactionOptions::compact_eligible(Duration::ZERO)
                    .with_now_unix_nanos(u64::MAX),
            ),
            ..Default::default()
        })
        .unwrap();
    let current = service
        .posix_write_file(path, b"replacement bytes", options)
        .unwrap();
    assert_ne!(old.object_id, current.object_id);
    drop(service);
    runtime.stop_node(&node).unwrap();
    runtime.reopen_node(node.clone()).unwrap();
    let mut service = runtime.client(&node).unwrap();
    assert_eq!(
        service.posix_stat(path).unwrap().object_id,
        current.object_id
    );
    assert_eq!(service.posix_read_file(path).unwrap(), b"replacement bytes");
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn restart_does_not_resurrect_deleted_generation_after_path_reuse() {
    let node = NodeId::generate();
    let (mut runtime, mut service) =
        native_sifs_service("native-no-resurrect-deleted", node.clone());
    let path = "/services/deleted-path";
    let options = DfsWriteOptions::replicated_eventual(vec![node.clone()]);
    let old = service
        .posix_write_file(path, b"stale bytes", options.clone())
        .unwrap();
    service.posix_unlink(path).unwrap();
    let replacement = service
        .posix_write_file(path, b"new bytes", options)
        .unwrap();
    service.posix_unlink(path).unwrap();
    service
        .run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
            tombstone_compaction: Some(
                DfsTombstoneCompactionOptions::compact_eligible(Duration::ZERO)
                    .with_now_unix_nanos(u64::MAX),
            ),
            ..Default::default()
        })
        .unwrap();
    drop(service);
    runtime.stop_node(&node).unwrap();
    runtime.reopen_node(node.clone()).unwrap();
    let mut service = runtime.client(&node).unwrap();
    assert!(matches!(
        service.posix_stat(path),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(matches!(
        service.posix_read_file(path),
        Err(DfsPosixError::NotFound { .. })
    ));
    for object in [old, replacement] {
        if let Some(head) = service
            .metadata_backend()
            .unwrap()
            .get_object(&object.object_id)
            .unwrap()
        {
            assert_eq!(head.kind, DfsObjectKind::Tombstone);
        }
    }
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn sifs_posix_rename_records_old_path_tombstone() {
    let node_id = NodeId::from_legacy_named("node_sifs_rename_tombstone").expect("node");
    let old_path = "/services/sandbox/instances/sandbox_rename/work/old.txt";
    let new_path = "/services/sandbox/instances/sandbox_rename/work/new.txt";

    let (mut runtime, mut dfs) = native_sifs_service(
        "sifs_posix_rename_records_old_path_tombstone",
        node_id.clone(),
    );
    dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_rename/work")
        .expect("mkdir");
    dfs.posix_write_file(
        old_path,
        b"content",
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write old");
    let old_version = dfs.posix_stat(old_path).expect("old stat").version;
    dfs.posix_rename(old_path, new_path).expect("rename");

    assert!(matches!(
        dfs.posix_stat(old_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(dfs.posix_read_file(new_path).expect("read new"), b"content");
    let tombstone = dfs
        .metadata
        .objects
        .values()
        .find(|object| object.path == old_path && object.kind == DfsObjectKind::Tombstone)
        .expect("old path tombstone");
    assert_ne!(tombstone.version, old_version);

    let reloaded = runtime.client(&node_id).unwrap();
    assert!(matches!(
        reloaded.posix_stat(old_path),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert_eq!(
        reloaded
            .posix_read_file(new_path)
            .expect("reloaded read new"),
        b"content"
    );
}

#[test]
fn sifs_posix_bulk_overwrite_rejects_tombstoned_parent() {
    let root = temp_root("sifs-posix-bulk-overwrite-tombstoned-parent");
    let node_id =
        NodeId::from_legacy_named("node_sifs_bulk_overwrite_tombstoned_parent").expect("node");
    let work = "/services/sandbox/instances/sandbox_bulk_overwrite_tombstoned_parent/work";
    let path = "/services/sandbox/instances/sandbox_bulk_overwrite_tombstoned_parent/work/file.txt";
    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_bulk_overwrite_rejects_tombstoned_parent",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        path,
        b"initial",
        DfsWriteOptions::replicated_eventual(vec![node_id]),
    )
    .expect("write initial");
    dfs.posix_unlink(path).expect("unlink child");
    dfs.posix_rmdir(work).expect("tombstone parent");

    assert!(
        matches!(
            dfs.posix_write_files_local_eventual_with_known_parents([(
                path,
                b"must not recreate below tombstone".as_slice(),
            )]),
            Err(DfsPosixError::MissingParent { path: missing }) if missing == work
        ),
        "bulk overwrite must not create a child below a tombstoned parent"
    );
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_list_dir_page_prefers_newer_durable_child_over_stale_memory_tombstone() {
    let root = temp_root("sifs-posix-list-page-stale-memory-tombstone");
    let node_id = NodeId::from_legacy_named("node_sifs_list_page_stale_tombstone").expect("node");
    let dir_path = "/services/sandbox/instances/sandbox_list_page_stale_tombstone/work";

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_list_dir_page_prefers_newer_durable_child_over_stale_memory_tombstone",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(dir_path).expect("mkdir");
    let mut persisted = Vec::new();
    for index in 0..3 {
        persisted.push(
            dfs.posix_write_file(
                &format!("{dir_path}/file-{index}.txt"),
                b"content",
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("write"),
        );
    }

    let mut stale_tombstone = dfs
        .metadata()
        .object(&persisted[1].object_id)
        .expect("persisted object metadata")
        .clone();
    stale_tombstone.kind = DfsObjectKind::Tombstone;
    stale_tombstone.version = DfsVersion::generate();
    stale_tombstone.content_hash = None;
    stale_tombstone.content_size_bytes = None;
    dfs.track_metadata_tombstone(&stale_tombstone);

    let mut durable_live = stale_tombstone.clone();
    durable_live.kind = DfsObjectKind::File;
    durable_live.version = DfsVersion::generate();
    durable_live.content_hash = Some(content_hash(b"newer durable live child"));
    durable_live.content_size_bytes = Some(b"newer durable live child".len() as u64);
    let mut writer = _runtime.client(&node_id).unwrap();
    let written = writer
        .posix_write_file(
            &durable_live.path,
            b"newer durable live child",
            DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
        )
        .expect("normal durable live publication");
    durable_live = writer
        .metadata_backend
        .as_ref()
        .unwrap()
        .get_object(&written.object_id)
        .unwrap()
        .unwrap();
    drop(writer);

    assert!(
        dfs.transient_metadata_object_ids.is_empty(),
        "a stale observed tombstone is not an active transient namespace overlay"
    );
    let mut cursor = None;
    let mut entries = Vec::new();
    loop {
        let page = dfs
            .posix_list_dir_page(dir_path, cursor.as_ref(), 1)
            .expect("list page");
        assert!(page.entries.len() <= 1);
        if let Some(next_cursor) = page.next_cursor.as_ref() {
            assert!(
                next_cursor.dentry.is_some(),
                "stale non-transient tombstones must retain bounded backend pagination"
            );
        }
        entries.extend(page.entries);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }

    entries.sort_by(|left, right| left.name.cmp(&right.name));
    assert_eq!(entries.len(), 3);
    let durable_entry = entries
        .iter()
        .find(|entry| entry.path == durable_live.path)
        .expect("newer durable child must suppress the stale in-memory tombstone");
    assert_eq!(durable_entry.kind, DfsObjectKind::File);
    assert_eq!(durable_entry.object_id, durable_live.object_id);

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn cold_lazy_sync_path_includes_durable_direct_child_tombstones() {
    let owner =
        crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-cold-tombstone", 1).remove(0);
    let (mut runtime, mut writer) = native_sifs_service("native-cold-tombstone", owner.clone());
    let directory = "/services/cold-sync-dir";
    let path = "/services/cold-sync-dir/deleted.txt";
    writer.posix_mkdir_all(directory).unwrap();
    let original = writer
        .posix_write_file(
            path,
            b"removed bytes",
            DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
        )
        .unwrap();
    writer.posix_unlink(path).unwrap();
    let source = writer
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&original.file_system_id, path)
        .unwrap()
        .unwrap();
    assert_eq!(source.kind, DfsObjectKind::Tombstone);
    drop(writer);
    runtime.stop_node(&owner).unwrap();
    runtime.reopen_node(owner.clone()).unwrap();
    let mut reader = runtime.client(&owner).unwrap();
    assert!(matches!(
        reader.posix_stat_distributed(path),
        Err(DfsPosixError::NotFound { .. })
    ));
    let actual = reader
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&original.file_system_id, path)
        .unwrap()
        .unwrap();
    assert_eq!(actual, source);
    assert!(reader.posix_list_dir(directory).unwrap().is_empty());
    drop(reader);
    runtime.close().unwrap();
}

#[test]
fn sifs_cluster_posix_unlink_replicates_tombstone_to_observers() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-unlink", "/users");
    let path = "/users/unlink-me.txt";
    let bytes = b"bytes to be unlinked".to_vec();

    sifs_cluster_primary_posix_write(&mut cluster, path, &bytes);
    sifs_cluster_publish_metadata_until(&mut cluster, "pre-unlink catch-up", |cluster| {
        cluster.observer.posix_stat(path).is_ok()
    });

    cluster.primary.posix_unlink(path).expect("primary unlink");
    assert!(matches!(
        cluster.primary.posix_stat(path),
        Err(DfsPosixError::NotFound { .. })
    ));

    sifs_cluster_publish_metadata_until(&mut cluster, "post-unlink catch-up", |cluster| {
        matches!(
            cluster.observer.posix_stat(path),
            Err(DfsPosixError::NotFound { .. })
        )
    });
    assert!(matches!(
        cluster.observer.posix_read_file(path),
        Err(DfsPosixError::NotFound { .. })
    ));
    let entries = cluster
        .observer
        .posix_list_dir("/users")
        .expect("observer list after unlink");
    assert!(
        !entries.iter().any(|entry| entry.path == path),
        "observer must not list an unlinked path: {entries:?}"
    );

    drop(cluster);
}

#[test]
fn routed_mkdir_all_prefers_live_path_shard_over_late_source_tombstone() {
    let mut cluster = sifs_two_primary_cluster("native-mkdir-after-source-eviction");
    cluster
        .replica
        .posix_mkdir_all("/users/moved-tree")
        .unwrap();
    let old = cluster.replica.posix_stat("/users/moved-tree").unwrap();
    cluster
        .replica
        .posix_rename("/users/moved-tree", "/services/moved-tree")
        .unwrap();
    let eviction = cluster
        .users_primary
        .metadata_backend()
        .unwrap()
        .get_object_on_shard(&DfsMetadataShardId::new("users"), &old.object_id)
        .unwrap()
        .unwrap();
    assert_eq!(eviction.kind, DfsObjectKind::Tombstone);
    cluster
        .replica
        .posix_mkdir_all("/services/moved-tree/a/b")
        .unwrap();
    assert_eq!(
        cluster
            .replica
            .posix_stat("/services/moved-tree")
            .unwrap()
            .object_id,
        old.object_id
    );
    assert_eq!(
        cluster
            .replica
            .posix_stat("/services/moved-tree/a/b")
            .unwrap()
            .kind,
        DfsObjectKind::Directory
    );
    assert!(matches!(
        cluster.replica.posix_stat("/users/moved-tree"),
        Err(DfsPosixError::NotFound { .. })
    ));
    cluster.close();
}

#[test]
fn sifs_replica_distributed_stat_prefers_primary_tombstone_over_stale_local_hit() {
    let mut cluster = sifs_posix_cluster("sifs-replica-stat-primary-tombstone", "/users");
    let path = "/users/stat-stale-live.txt";
    let bytes = b"writer-local stale entry should lose to primary tombstone".to_vec();

    sifs_cluster_routed_posix_write(&mut cluster, path, &bytes);
    assert_eq!(
        cluster.writer.posix_read_file(path).expect("writer read"),
        bytes
    );
    assert!(cluster.writer.posix_stat(path).is_ok());

    cluster.primary.posix_unlink(path).expect("primary unlink");
    assert!(matches!(
        cluster.primary.posix_stat(path),
        Err(DfsPosixError::NotFound { .. })
    ));
    assert!(
        cluster.writer.posix_stat(path).is_ok(),
        "test setup must leave a stale local live entry on the writer"
    );

    let SifsPosixCluster {
        root,
        mut primary,
        mut writer,
        _runtime,
        ..
    } = cluster;
    let stop_primary = Arc::new(AtomicBool::new(false));
    let primary_stop = Arc::clone(&stop_primary);
    let primary_thread = std::thread::spawn(move || {
        while !primary_stop.load(Ordering::SeqCst) {
            primary.process_incoming().expect("primary pump");
            std::thread::sleep(Duration::from_millis(1));
        }
        primary
    });

    let distributed_stat = writer.posix_stat_distributed(path);
    stop_primary.store(true, Ordering::SeqCst);
    primary_thread.join().expect("primary thread");
    assert!(
        matches!(distributed_stat, Err(DfsPosixError::NotFound { .. })),
        "distributed stat must prefer the shard primary tombstone over a stale replica-local hit, got {distributed_stat:?}"
    );

    drop(_runtime);
}
