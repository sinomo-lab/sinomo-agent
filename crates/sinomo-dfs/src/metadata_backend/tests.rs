use super::*;

fn file_system_id() -> DfsFileSystemId {
    DfsFileSystemId::from_legacy_named("sifs").expect("valid filesystem id")
}

fn object_id() -> DfsObjectId {
    DfsObjectId::from_legacy_named("obj_readme").expect("valid object id")
}

mod prepared_operation;

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
