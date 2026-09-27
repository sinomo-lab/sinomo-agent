//! Durable SOBJ and filesystem IO helpers for DFS metadata backends.

use std::fs;
use std::io;
#[cfg(test)]
use std::io::Write as _;
use std::path::{Path, PathBuf};
#[cfg(windows)]
use std::time::{Duration, Instant};

use serde::{Serialize, de::DeserializeOwned};
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_core::storage::durable_file;

use crate::metadata_error::{MetadataBackendError, Result};

pub(crate) fn create_dir_all(path: PathBuf) -> Result<()> {
    fs::create_dir_all(&path).map_err(|source| MetadataBackendError::Io { path, source })
}

pub(crate) fn remove_file_if_exists(path: &Path) -> Result<()> {
    durable_file::remove_file_if_exists_and_sync_parent(path)
        .map(|_| ())
        .map_err(metadata_durable_file_error)
}

pub(crate) fn read_dir(path: &Path) -> Result<Vec<fs::DirEntry>> {
    let mut entries = Vec::new();
    let reader = fs::read_dir(path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })?;
    for entry in reader {
        entries.push(entry.map_err(|source| MetadataBackendError::Io {
            path: path.to_owned(),
            source,
        })?);
    }
    Ok(entries)
}

fn read_file(path: &Path) -> Result<Vec<u8>> {
    read_file_bytes(path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })
}

fn read_file_bytes(path: &Path) -> io::Result<Vec<u8>> {
    #[cfg(not(windows))]
    {
        fs::read(path)
    }

    #[cfg(windows)]
    {
        const ERROR_SHARING_VIOLATION: i32 = 32;
        const ERROR_LOCK_VIOLATION: i32 = 33;
        const RETRY_INITIAL_DELAY: Duration = Duration::from_millis(2);
        const RETRY_MAX_DELAY: Duration = Duration::from_millis(40);
        const RETRY_TIMEOUT: Duration = Duration::from_secs(2);

        let started = Instant::now();
        let mut delay = RETRY_INITIAL_DELAY;
        loop {
            match fs::read(path) {
                Ok(bytes) => return Ok(bytes),
                Err(source)
                    if matches!(
                        source.raw_os_error(),
                        Some(ERROR_SHARING_VIOLATION) | Some(ERROR_LOCK_VIOLATION)
                    ) =>
                {
                    let Some(remaining) = RETRY_TIMEOUT.checked_sub(started.elapsed()) else {
                        return Err(source);
                    };
                    if remaining.is_zero() {
                        return Err(source);
                    }
                    // ReplaceFileW can briefly deny a new open while another
                    // process atomically publishes the next cursor value. The
                    // old or new file remains intact, so wait for that bounded
                    // publication window instead of failing the DFS operation.
                    std::thread::sleep(delay.min(remaining));
                    delay = (delay * 2).min(RETRY_MAX_DELAY);
                }
                Err(source) => return Err(source),
            }
        }
    }
}

pub(crate) fn read_sobj<T>(path: &Path) -> Result<T>
where
    T: DeserializeOwned,
{
    sinomo_object_serde::from_bytes(&read_file(path)?).map_err(|source| {
        MetadataBackendError::Serde(format!("failed to decode {}: {source}", path.display()))
    })
}

pub(crate) fn read_sobj_optional<T>(path: &Path) -> Result<Option<T>>
where
    T: DeserializeOwned,
{
    if !path.exists() {
        return Ok(None);
    }
    read_sobj(path).map(Some)
}

pub(crate) fn read_sobj_tree<T>(path: &Path) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    read_sobj_tree_into(path, &mut records)?;
    Ok(records)
}

fn read_sobj_tree_into<T>(path: &Path, records: &mut Vec<T>) -> Result<()>
where
    T: DeserializeOwned,
{
    for entry in read_dir(path)? {
        let path = entry.path();
        if path.is_dir() {
            read_sobj_tree_into(&path, records)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("sobj") {
            records.push(read_sobj(&path)?);
        }
    }
    Ok(())
}

pub(crate) fn write_sobj_atomic<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize,
{
    write_sobj_atomic_if_changed(path, value).map(|_| ())
}

/// Atomic visibility only. The caller must recover a lost/malformed hint from
/// independently durable journal or table data before granting authority.
pub(crate) fn write_sobj_hint_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = encode_sobj(value)?;
    durable_file::write_file_atomic_hint(
        path,
        &bytes,
        durable_file::DurableAtomicFileOptions::new("hint", "hint"),
    )
    .map_err(metadata_durable_file_error)
}

pub(crate) fn write_sobj_atomic_if_changed<T>(path: &Path, value: &T) -> Result<bool>
where
    T: Serialize,
{
    let bytes = encode_sobj(value)?;
    if path.is_file() {
        match read_file_bytes(path) {
            Ok(existing) if existing == bytes => return Ok(false),
            Ok(_) => {}
            Err(source) if source.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(MetadataBackendError::Io {
                    path: path.to_owned(),
                    source,
                });
            }
        }
    }
    write_file_atomic(path, &bytes)?;
    Ok(true)
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt as _;

    #[test]
    fn metadata_read_retries_a_transient_sharing_violation() {
        let root = sinomo_testdir::TestDir::new("dfs-metadata-read-sharing-violation");
        let path = root.join("cursor.sobj");
        fs::write(&path, b"durable cursor").expect("write cursor");

        let cursor_handle = OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(&path)
            .expect("open cursor without sharing");
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            drop(cursor_handle);
        });

        let bytes = read_file_bytes(&path).expect("read after transient sharing violation");
        release.join().expect("release cursor handle");
        assert_eq!(bytes, b"durable cursor");
    }
}

pub(crate) fn encode_sobj<T>(value: &T) -> Result<Vec<u8>>
where
    T: Serialize,
{
    sinomo_object_serde::to_bytes(value, 1, LengthMode::U32)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))
}

pub(crate) fn write_sobj_files_create_new_relaxed(files: Vec<(PathBuf, Vec<u8>)>) -> Result<()> {
    durable_file::write_files_create_new_relaxed(
        files,
        durable_file::DurableAtomicFileOptions::new("tmp", "tmp"),
    )
    .map_err(metadata_durable_file_error)
}

pub(crate) fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    durable_file::write_file_atomic(
        path,
        bytes,
        durable_file::DurableAtomicFileOptions::new("tmp", "tmp").sync_all(),
    )
    .map_err(metadata_durable_file_error)
}

/// One fully written and synced replacement that has not entered the durable
/// namespace yet.
///
/// Staging deliberately does not sync the parent directory: a crash before
/// installation may discard the private staging name, while a surviving
/// orphan is never observable through the destination. Installation performs
/// the atomic replacement and parent-directory sync as one final operation.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct StagedFileReplacement {
    staged_path: PathBuf,
    destination_path: PathBuf,
    installed: bool,
}

#[cfg(test)]
impl StagedFileReplacement {
    pub(crate) fn install(mut self) -> Result<()> {
        durable_file::install_staged_file(&self.staged_path, &self.destination_path, true)
            .map_err(metadata_durable_file_error)?;
        self.installed = true;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.staged_path
    }
}

#[cfg(test)]
impl Drop for StagedFileReplacement {
    fn drop(&mut self) {
        if !self.installed {
            let _ = fs::remove_file(&self.staged_path);
        }
    }
}

/// Write and sync a private, same-directory replacement without publishing it.
///
/// The caller can do arbitrarily expensive encoding and IO before entering a
/// higher-level writer critical section, then call [`StagedFileReplacement::install`]
/// after revalidating the generation it intends to replace.
#[cfg(test)]
pub(crate) fn stage_file_replacement(
    destination_path: &Path,
    bytes: &[u8],
    label: &str,
) -> Result<StagedFileReplacement> {
    let parent = destination_path.parent().unwrap_or_else(|| Path::new("."));
    create_dir_all(parent.to_path_buf())?;
    let destination_name = destination_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("metadata");
    let staged_path = parent.join(format!(
        ".{destination_name}.{label}.{}.stage",
        sinomo_core::Id::generate()
    ));
    let mut file = durable_file::create_private_staged_file(&staged_path)
        .map_err(metadata_durable_file_error)?;
    if let Err(source) = file.write_all(bytes).and_then(|()| file.sync_all()) {
        let _ = fs::remove_file(&staged_path);
        return Err(MetadataBackendError::Io {
            path: staged_path,
            source,
        });
    }
    Ok(StagedFileReplacement {
        staged_path,
        destination_path: destination_path.to_path_buf(),
        installed: false,
    })
}

pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    durable_file::sync_directory(path).map_err(metadata_durable_file_error)
}

pub(crate) fn metadata_durable_file_error(
    error: durable_file::DurableFileError,
) -> MetadataBackendError {
    MetadataBackendError::Io {
        path: PathBuf::from(error.path()),
        source: io::Error::new(error.kind(), error.detail().to_string()),
    }
}
/// Stable Windows file identity. Unlike timestamps, this distinguishes an
/// atomic replacement even when NTFS preserves the old creation time.
#[cfg(windows)]
pub(crate) fn windows_file_identity(file: &std::fs::File) -> Option<(u32, u64)> {
    use std::mem::MaybeUninit;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };

    let mut information = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
    // SAFETY: the file owns a live handle and the output has the exact Win32
    // structure size. The structure is read only after the call succeeds.
    if unsafe {
        GetFileInformationByHandle(file.as_raw_handle() as HANDLE, information.as_mut_ptr())
    } == 0
    {
        return None;
    }
    // SAFETY: the successful call initialized the complete structure.
    let information = unsafe { information.assume_init() };
    let index =
        (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow);
    Some((information.dwVolumeSerialNumber, index))
}

#[cfg(windows)]
pub(crate) fn windows_path_identity(path: &std::path::Path) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    let (volume, index) = windows_file_identity(&file)?;
    Some(format!("windows:{volume}:{index}"))
}
