//! Durable SOBJ and filesystem IO helpers for DFS metadata backends.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

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
    fs::read(path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })
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

pub(crate) fn write_sobj_atomic_if_changed<T>(path: &Path, value: &T) -> Result<bool>
where
    T: Serialize,
{
    let bytes = encode_sobj(value)?;
    if path.is_file() {
        match fs::read(path) {
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
