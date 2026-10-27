use super::*;

fn file_system_id() -> DfsFileSystemId {
    DfsFileSystemId::from_legacy_named("sifs").expect("valid filesystem id")
}

fn object_id() -> DfsObjectId {
    DfsObjectId::from_legacy_named("obj_readme").expect("valid object id")
}

mod prepared_operation;
mod stat_snapshot;
mod selected_recovery_fence;

fn assert_resident_snapshot_pending_preserves_fence<G>(
    mut backend: LocalSobjMetadataBackend,
    fence: G,
    lock_path: &std::path::Path,
) -> LocalSobjMetadataBackend {
    let original_prefix = backend.prefix_proof.clone();
    let (send, receive) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let snapshot = backend.try_begin_resident_read_snapshot();
        let replay = backend.try_materialize_resident_journal();
        send.send((snapshot, replay, backend)).unwrap();
    });
    // This is the existing metadata namespace confirmation window. Always
    // release and join the worker before asserting, including the old blocking
    // failure, so a regression cannot leave an owned test thread behind.
    let observed = receive.recv_timeout(std::time::Duration::from_secs(1));
    let contender = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(lock_path)
        .unwrap();
    let still_fenced = fs2::FileExt::try_lock_exclusive(&contender)
        .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock);
    drop(contender);
    drop(fence);
    worker.join().unwrap();
    let (snapshot, replay, backend) =
        observed.expect("resident read must yield within the unchanged confirmation window");
    assert!(
        !snapshot.unwrap(),
        "a busy physical source is not an acquired snapshot"
    );
    assert_eq!(replay.unwrap(), None, "busy replay cannot claim completion");
    assert!(
        still_fenced,
        "a failed attempt must not release the actual source owner"
    );
    assert_eq!(backend.consistent_read_snapshot_depth, 0);
    assert_eq!(backend.prefix_proof, original_prefix);
    backend
}

#[test]
fn resident_snapshot_yields_to_local_source_fence_and_recovers_exact_view() {
    use crate::DfsMetadataBackend;
    let root = sinomo_testdir::TestDir::new("resident-local-source-fence");
    let shard = DfsMetadataShardId::new("root");
    let backend = LocalSobjMetadataBackend::new(shard.clone(), root.path()).unwrap();
    let original = backend.prefix_proof.clone();
    let lock_path = root.path().join("journal/write.lock");
    let fence =
        crate::metadata_lock::metadata_write_lock(root.path().to_owned(), lock_path.clone())
            .unwrap();
    let mut backend = assert_resident_snapshot_pending_preserves_fence(backend, fence, &lock_path);
    assert!(backend.try_begin_resident_read_snapshot().unwrap());
    assert_eq!(Some(backend.metadata_prefix_proof().unwrap()), original);
    backend.end_consistent_read_snapshot();
    assert_eq!(backend.consistent_read_snapshot_depth, 0);
    drop(backend);
    let mut reopened = LocalSobjMetadataBackend::new(shard, root.path()).unwrap();
    assert_eq!(reopened.prefix_proof, original);
    assert_eq!(
        reopened.try_materialize_resident_journal().unwrap(),
        Some(0)
    );
}

#[test]
fn resident_snapshot_preserves_external_fence_nested_cut_and_corruption_errors() {
    use crate::DfsMetadataBackend;
    use std::io::Write;
    let root = sinomo_testdir::TestDir::new("resident-physical-source-fence");
    let shard = DfsMetadataShardId::new("root");
    let backend = LocalSobjMetadataBackend::new(shard.clone(), root.path()).unwrap();
    let lock_path = root.path().join("journal/write.lock");
    let fence = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    fs2::FileExt::lock_exclusive(&fence).unwrap();
    let mut backend = assert_resident_snapshot_pending_preserves_fence(backend, fence, &lock_path);
    assert!(backend.try_begin_resident_read_snapshot().unwrap());
    let captured = backend.metadata_prefix_proof().unwrap();
    let later_writer = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    fs2::FileExt::lock_exclusive(&later_writer).unwrap();
    assert!(
        backend.try_begin_resident_read_snapshot().unwrap(),
        "nested reads retain their already-acquired exact cut"
    );
    assert_eq!(backend.metadata_prefix_proof().unwrap(), captured);
    backend.end_consistent_read_snapshot();
    backend.end_consistent_read_snapshot();
    assert!(!backend.try_begin_resident_read_snapshot().unwrap());
    drop(later_writer);
    // Only actual lock contention defers. Stable malformed source data remains
    // an error, never Pending or an empty successful view.
    let snapshot = root.path().join("records/tables/snapshot.sobj");
    let mut bytes = std::fs::read(&snapshot).unwrap();
    assert!(!bytes.is_empty());
    bytes[0] ^= 0xff;
    std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&snapshot)
        .unwrap()
        .write_all(&bytes)
        .unwrap();
    assert!(backend.try_begin_resident_read_snapshot().is_err());
    assert_eq!(backend.consistent_read_snapshot_depth, 0);
}

fn resident_checkpoint_read_image(
    name: &str,
) -> (sinomo_testdir::TestDir, LocalSobjMetadataBackend) {
    let root = sinomo_testdir::TestDir::new(name);
    let backend =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.path()).unwrap();
    let path = metadata_table_snapshot_path(root.path());
    let replacement = path.with_extension("validated-replacement");
    std::fs::copy(&path, &replacement).unwrap();
    std::fs::rename(&replacement, &path).unwrap();
    assert_ne!(
        backend.table_storage_generation.as_ref(),
        Some(&metadata_table_storage_generation(root.path()).unwrap())
    );
    let view = backend.current_tables_snapshot().unwrap();
    assert_eq!(&*view, &backend.tables);
    drop(view);
    {
        let cache = backend.read_snapshot_cache.lock().unwrap();
        let cache = cache
            .as_ref()
            .expect("actual full decode of the replaced checkpoint");
        assert!(cache.snapshot_format_version.is_some());
        assert_eq!(
            cache.materialized_through_sequence,
            Some(backend.materialized_through_sequence)
        );
        assert_eq!(cache.prefix_proof, backend.prefix_proof);
        assert_eq!(
            cache.storage_generation.as_ref(),
            Some(&metadata_table_storage_generation(root.path()).unwrap())
        );
    }
    (root, backend)
}

#[test]
fn resident_replay_reuses_verified_same_prefix_checkpoint_image() {
    let (_root, mut backend) = resident_checkpoint_read_image("resident-same-prefix-read-image");
    let prefix = backend.prefix_proof.clone();
    reset_metadata_durable_table_reloads();
    assert_eq!(backend.try_materialize_resident_journal().unwrap(), Some(0));
    assert_eq!(backend.prefix_proof, prefix);
    assert_eq!(
        metadata_durable_table_reloads(),
        0,
        "an already validated exact physical checkpoint must not be decoded again just because its sequence is equal"
    );
}

#[test]
fn resident_read_image_cannot_cover_changed_physical_checkpoint() {
    use std::io::Write;
    let (root, mut backend) = resident_checkpoint_read_image("resident-read-image-physical-change");
    let path = metadata_table_snapshot_path(root.path());
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 0xff;
    std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap()
        .write_all(&bytes)
        .unwrap();
    assert!(
        backend.try_materialize_resident_journal().is_err(),
        "a cached image from before source corruption cannot certify the changed generation"
    );
}

#[test]
fn resident_read_image_different_prefix_requires_fresh_source_reload() {
    let (root, mut backend) = resident_checkpoint_read_image("resident-read-image-foreign-prefix");
    let foreign = sinomo_testdir::TestDir::new("resident-read-image-foreign-source");
    let other =
        LocalSobjMetadataBackend::new(DfsMetadataShardId::new("foreign-shard"), foreign.path())
            .unwrap();
    assert_ne!(backend.prefix_proof, other.prefix_proof);
    let replacement = root.path().join("foreign-checkpoint.sobj");
    std::fs::copy(metadata_table_snapshot_path(foreign.path()), &replacement).unwrap();
    std::fs::rename(replacement, metadata_table_snapshot_path(root.path())).unwrap();
    // This is a raw storage reload, not an authenticated replica install.
    // The valid changed checkpoint must take the original full source path;
    // equality of the sequence alone cannot select the new cache fast path.
    drop(backend.current_tables_snapshot().unwrap());
    {
        let cache = backend.read_snapshot_cache.lock().unwrap();
        let cache = cache.as_ref().unwrap();
        assert_eq!(
            cache.materialized_through_sequence,
            Some(backend.materialized_through_sequence)
        );
        assert_ne!(cache.prefix_proof, backend.prefix_proof);
    }
    reset_metadata_durable_table_reloads();
    assert_eq!(backend.try_materialize_resident_journal().unwrap(), Some(0));
    assert_eq!(metadata_durable_table_reloads(), 1);
    assert_eq!(backend.prefix_proof, other.prefix_proof);
}
