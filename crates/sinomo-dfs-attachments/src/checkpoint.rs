//! Module-owned checkpoints using the shared durable filesystem primitives.

use serde::{Serialize, de::DeserializeOwned};
use sinomo_core::object::{LengthMode, serde_ext};
use sinomo_core::storage::durable_file::{self, DurableAtomicFileOptions};
use sinomo_dfs::MetadataBackendError;
use std::path::{Path, PathBuf};

const MAX_CHECKPOINT_BYTES: u64 = 16 * 1024 * 1024;

pub(crate) fn read_sobj_optional<T: DeserializeOwned>(
    path: &Path,
) -> Result<Option<T>, MetadataBackendError> {
    durable_file::read_optional_object_file_bytes(path, MAX_CHECKPOINT_BYTES)
        .map_err(durable_error)?
        .map(|bytes| {
            serde_ext::from_bytes(&bytes).map_err(|source| {
                MetadataBackendError::Serde(format!(
                    "failed to decode {}: {source}",
                    path.display()
                ))
            })
        })
        .transpose()
}

pub(crate) fn write_sobj_atomic<T: Serialize>(
    path: &Path,
    value: &T,
) -> Result<(), MetadataBackendError> {
    let bytes = serde_ext::to_bytes(value, 1, LengthMode::U32)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    if bytes.len() as u64 > MAX_CHECKPOINT_BYTES {
        return Err(MetadataBackendError::Serde(
            "attachment maintenance checkpoint exceeds bounds".into(),
        ));
    }
    if durable_file::read_optional_object_file_bytes(path, MAX_CHECKPOINT_BYTES)
        .map_err(durable_error)?
        .as_deref()
        == Some(bytes.as_slice())
    {
        return Ok(());
    }
    durable_file::write_file_atomic(
        path,
        &bytes,
        DurableAtomicFileOptions::new("tmp", "tmp").sync_all(),
    )
    .map_err(durable_error)
}

fn durable_error(error: durable_file::DurableFileError) -> MetadataBackendError {
    MetadataBackendError::Io {
        path: PathBuf::from(error.path()),
        source: std::io::Error::new(error.kind(), error.detail().to_owned()),
    }
}
