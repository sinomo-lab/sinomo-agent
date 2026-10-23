use super::*;

fn copy_files(source: &std::path::Path, target: &std::path::Path) {
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let destination = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_files(&entry.path(), &destination);
        } else {
            assert!(entry.file_type().unwrap().is_file());
            let bytes = std::fs::read(entry.path()).unwrap();
            std::fs::write(&destination, &bytes).unwrap();
            assert_eq!(std::fs::read(destination).unwrap(), bytes);
        }
    }
}

fn checkpoint_copy(name: &str) -> (sinomo_testdir::TestDir, LocalSobjMetadataBackend) {
    let (runtime, mut service, config) = native_case(name, 1);
    directory(&mut service, "/services/checkpoint");
    for n in 0..32 {
        service
            .posix_write_file(
                &format!("/services/checkpoint/file-{n}"),
                b"actual native content",
                crate::DfsWriteOptions::replicated_eventual(vec![service.node_id().clone()]),
            )
            .unwrap();
    }
    let shard = DfsMetadataShardId::new("services");
    let source =
        crate::local_dfs_metadata_shard_path(runtime.data_dir(&config.primary_node_id), &shard);
    let original = LocalSobjMetadataBackend::open_for_inspection(shard.clone(), &source).unwrap();
    let copy = sinomo_testdir::TestDir::new(&format!("{name}-copy"));
    {
        // Copy only this disposable native fixture under its real source fence.
        let _custody = original.metadata_write_lock().unwrap();
        copy_files(&source, copy.path());
    }
    drop((original, service));
    runtime.close().unwrap();
    let mut backend = LocalSobjMetadataBackend::open_for_inspection(shard, copy.path()).unwrap();
    assert_eq!(backend.try_materialize_resident_journal().unwrap(), Some(0));
    assert!(backend.read_snapshot_cache.lock().unwrap().is_none());
    let before = metadata_table_storage_generation(copy.path()).unwrap();
    // A separate storage control forces a real checkpoint at this small native
    // cut. Production thresholds and both original 700-create tests are unchanged.
    crate::metadata_table_apply::schedule_metadata_table_checkpoint_if_needed(
        copy.path(),
        &backend.tables,
        backend.table_log_offset.saturating_sub(1),
        backend.table_log_offset,
        Some(&backend.read_snapshot_cache),
    )
    .unwrap();
    crate::metadata_table_apply::wait_for_metadata_table_checkpoint_workers_for_test(copy.path());
    {
        let cache = backend.read_snapshot_cache.lock().unwrap();
        let cache = cache.as_ref().unwrap();
        assert_eq!(&*cache.tables, &backend.tables);
        assert_eq!(cache.prefix_proof, backend.prefix_proof);
        assert_ne!(cache.storage_generation.as_ref(), Some(&before));
        assert_eq!(
            cache.storage_generation.as_ref(),
            Some(&metadata_table_storage_generation(copy.path()).unwrap())
        );
    }
    (copy, backend)
}

#[test]
fn native_checkpoint_hands_its_exact_image_to_the_existing_resident_reader() {
    let (_root, mut backend) = checkpoint_copy("native-checkpoint-reader-image");
    let expected = backend.tables.clone();
    reset_metadata_durable_table_reloads();
    assert_eq!(backend.try_materialize_resident_journal().unwrap(), Some(0));
    assert_eq!(backend.tables, expected);
    assert_eq!(
        metadata_durable_table_reloads(),
        0,
        "the installed checkpoint image was already decoded and validated by its actual producer"
    );
}

#[test]
fn native_checkpoint_image_cannot_cover_a_later_corrupt_physical_source() {
    let (root, mut backend) = checkpoint_copy("native-checkpoint-reader-corrupt-source");
    {
        let _custody = backend.metadata_write_lock().unwrap();
        let path = metadata_table_snapshot_path(root.path());
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
    }
    assert!(backend.try_materialize_resident_journal().is_err());
}
