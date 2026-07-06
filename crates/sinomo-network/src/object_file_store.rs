use crate::error::NetworkError;
use crate::object::SinomoObjectCodec;
use serde::Serialize;
use sinomo_storage::durable_file;
use std::fs;
use std::path::PathBuf;
use std::sync::LazyLock;

#[cfg(any(test, feature = "test-support"))]
use std::collections::HashSet;
#[cfg(any(test, feature = "test-support"))]
use std::sync::Mutex;
#[cfg(any(test, feature = "test-support"))]
use std::time::SystemTime;

use crate::io_observer;

const FILE_CACHE_CAPACITY: usize = 4096;

pub(crate) const MAX_OBJECT_FILE_READ_BYTES: u64 = durable_file::DEFAULT_OBJECT_FILE_MAX_READ_BYTES;

/// Cache of last-written object files. The cluster directory can be shared by the
/// main and shadow daemons, so the fast-path skip is guarded by a `stat`: an entry
/// is trusted only when the file's length, mtime, and inode still match what we
/// wrote, so a write by another process is detected and falls back to a real read
/// + compare.
static LAST_WRITTEN_OBJECT_FILES: LazyLock<durable_file::ObjectFileWriteMarkCache> =
    LazyLock::new(|| durable_file::ObjectFileWriteMarkCache::new(FILE_CACHE_CAPACITY));

pub fn read_object_file<T: serde::de::DeserializeOwned>(path: &PathBuf) -> Result<T, NetworkError> {
    let bytes = read_object_file_bytes(path)?;
    if let Some(observer) = io_observer() {
        observer.on_disk_read(bytes.len() as u64);
    }
    SinomoObjectCodec::default().decode(&bytes)
}

pub fn read_optional_object_file<T: serde::de::DeserializeOwned>(
    path: &PathBuf,
) -> Result<Option<T>, NetworkError> {
    let Some(bytes) = read_optional_object_file_bytes(path)? else {
        return Ok(None);
    };
    if let Some(observer) = io_observer() {
        observer.on_disk_read(bytes.len() as u64);
    }
    SinomoObjectCodec::default().decode(&bytes).map(Some)
}

fn read_optional_object_file_bytes(path: &PathBuf) -> Result<Option<Vec<u8>>, NetworkError> {
    durable_file::read_optional_object_file_bytes(path, MAX_OBJECT_FILE_READ_BYTES)
        .map_err(network_durable_file_error)
}

fn read_object_file_bytes(path: &PathBuf) -> Result<Vec<u8>, NetworkError> {
    durable_file::read_object_file_bytes(path, MAX_OBJECT_FILE_READ_BYTES)
        .map_err(network_durable_file_error)
}

pub fn remove_optional_file(path: &PathBuf) -> Result<bool, NetworkError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(NetworkError::StorageIo {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

pub fn write_object_file<T: Serialize>(path: &PathBuf, value: &T) -> Result<(), NetworkError> {
    let desired = SinomoObjectCodec::default().encode(value)?;
    write_encoded_object_file(path, &desired)
}

pub fn write_object_file_with_semantic_skip<T, F>(
    path: &PathBuf,
    value: &T,
    stable_eq: F,
) -> Result<(), NetworkError>
where
    T: Serialize + serde::de::DeserializeOwned,
    F: FnOnce(&T) -> bool,
{
    let desired = SinomoObjectCodec::default().encode(value)?;
    if fs::metadata(path)
        .map(|metadata| metadata.len() == desired.len() as u64)
        .unwrap_or(false)
        && let Ok(Some(existing)) = read_optional_object_file::<T>(path)
        && stable_eq(&existing)
    {
        record_object_file_semantic_skip();
        return Ok(());
    }
    write_encoded_object_file(path, &desired)
}

fn write_encoded_object_file(path: &PathBuf, desired: &[u8]) -> Result<(), NetworkError> {
    let result = durable_file::write_object_file_bytes_if_changed(
        path,
        desired,
        &LAST_WRITTEN_OBJECT_FILES,
        durable_file::DurableObjectFileOptions::new("sobj", "tmp"),
    )
    .map_err(network_durable_file_error)?;
    if let (Some(observer), Some(read_bytes)) = (io_observer(), result.compare_read_bytes) {
        observer.on_disk_read(read_bytes);
    }
    if let Some(observer) = io_observer() {
        if result.changed {
            observer.on_object_file_actual_write();
            observer.on_disk_write(desired.len() as u64);
        } else {
            observer.on_object_file_skipped_write();
        }
    }
    Ok(())
}

pub(crate) fn record_object_file_semantic_skip() {
    if let Some(observer) = io_observer() {
        observer.on_object_file_skipped_write();
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn write_object_file_compare_read_bytes_for_test<T: Serialize>(
    path: &PathBuf,
    value: &T,
) -> Result<Option<u64>, NetworkError> {
    let desired = SinomoObjectCodec::default().encode(value)?;
    durable_file::write_object_file_bytes_if_changed(
        path,
        &desired,
        &LAST_WRITTEN_OBJECT_FILES,
        durable_file::DurableObjectFileOptions::new("sobj", "tmp"),
    )
    .map(|result| result.compare_read_bytes)
    .map_err(network_durable_file_error)
}

/// Write a fully encoded object and publish it only if `path` is absent.
///
/// The normal path stages the object in a same-directory temp file, fsyncs it,
/// then publishes it with `hard_link`, which gives atomic no-clobber semantics.
/// Filesystems without hard-link support fall back to a metadata check followed
/// by `rename`; callers must serialize writers for the same `path` so that
/// fallback cannot replace a concurrently created winner.
// Unconditional since stage C: dylib network modules reuse this helper
// through the sinomo-core facade.
pub fn write_object_file_create_new_serialized<T: Serialize>(
    path: &PathBuf,
    value: &T,
) -> Result<bool, NetworkError> {
    let desired = SinomoObjectCodec::default().encode(value)?;
    let result = durable_file::write_object_file_bytes_create_new_serialized(
        path,
        &desired,
        &LAST_WRITTEN_OBJECT_FILES,
        durable_file::DurableObjectFileOptions::new("sobj", "create-new-tmp"),
        |temp_path, path| create_new_link_or_unsupported_for_test(temp_path, &path.to_path_buf()),
    )
    .map_err(network_durable_file_error)?;
    if let Some(observer) = io_observer() {
        if result.changed {
            observer.on_object_file_actual_write();
            observer.on_disk_write(desired.len() as u64);
        } else {
            observer.on_object_file_skipped_write();
        }
    }
    Ok(result.changed)
}

// Unconditional since stage C alongside its only caller above; the injected
// failure branch stays test-gated.
fn create_new_link_or_unsupported_for_test(
    temp_path: &std::path::Path,
    path: &PathBuf,
) -> std::io::Result<()> {
    #[cfg(any(test, feature = "test-support"))]
    if write_object_file_create_new_forces_unsupported_link_for_test(path) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "injected unsupported hard link",
        ));
    }
    fs::hard_link(temp_path, path)
}

fn network_durable_file_error(error: durable_file::DurableFileError) -> NetworkError {
    match error {
        durable_file::DurableFileError::ResourceExhausted {
            operation,
            requested_bytes,
            ..
        } => NetworkError::ResourceExhausted {
            operation,
            requested_bytes,
        },
        error => NetworkError::StorageIo {
            path: error.path().to_string(),
            detail: error.detail().to_string(),
        },
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn object_file_write_mark_cache_capacity_for_test() -> usize {
    LAST_WRITTEN_OBJECT_FILES.capacity()
}

#[cfg(any(test, feature = "test-support"))]
pub fn object_file_write_mark_cache_contains_for_test(path: &PathBuf) -> bool {
    LAST_WRITTEN_OBJECT_FILES.contains(path)
}

#[cfg(any(test, feature = "test-support"))]
pub fn record_object_file_write_mark_for_test(path: &PathBuf) {
    LAST_WRITTEN_OBJECT_FILES.record_mark(
        path,
        blake3::hash(b"stable"),
        6,
        Some(SystemTime::UNIX_EPOCH),
        1,
    );
}

#[cfg(any(test, feature = "test-support"))]
pub fn refresh_object_file_write_mark_for_test(path: &PathBuf) -> bool {
    LAST_WRITTEN_OBJECT_FILES.refresh(path)
}

#[cfg(any(test, feature = "test-support"))]
pub fn remove_object_file_write_mark_entries_under_root_for_test(root: &std::path::Path) {
    LAST_WRITTEN_OBJECT_FILES.retain_not_under_root(root);
}

#[cfg(any(test, feature = "test-support"))]
static CREATE_NEW_OBJECT_FILE_FORCE_UNSUPPORTED_LINK_FOR_TEST: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

#[cfg(any(test, feature = "test-support"))]
pub fn force_write_object_file_create_new_unsupported_link_for_test(path: &PathBuf) {
    if let Ok(mut paths) = CREATE_NEW_OBJECT_FILE_FORCE_UNSUPPORTED_LINK_FOR_TEST.lock() {
        paths.insert(path.clone());
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn clear_write_object_file_create_new_unsupported_link_for_test(path: &PathBuf) {
    if let Ok(mut paths) = CREATE_NEW_OBJECT_FILE_FORCE_UNSUPPORTED_LINK_FOR_TEST.lock() {
        paths.remove(path);
    }
}

#[cfg(any(test, feature = "test-support"))]
fn write_object_file_create_new_forces_unsupported_link_for_test(path: &PathBuf) -> bool {
    CREATE_NEW_OBJECT_FILE_FORCE_UNSUPPORTED_LINK_FOR_TEST
        .lock()
        .ok()
        .is_some_and(|paths| paths.contains(path))
}
