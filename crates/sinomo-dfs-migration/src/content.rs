//! Incremental conversion of retired content layouts into the sole native layout.
//! The migration owner drives conversion; cold-open hooks only enqueue or inspect.
mod incremental;
mod legacy_stage;
mod legacy_blob;
pub(crate) use legacy_stage::{LegacyContentJobReference, LegacyPublishedV3};
mod snapshot_cursor;
mod original_v3;
#[cfg(test)]
pub(crate) use original_v3::write_original_v3_fixture_data;
pub(crate) use original_v3::{OriginalV3Converter, OriginalV3Budget, OriginalV3Selection, OriginalV3Step, OriginalV3UnitResult, OriginalV3SourceIdentity, probe_original_v3};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_core::object::{LengthMode, serde_ext};
use sinomo_core::storage::durable_file;
use sinomo_dfs::{
    ContentManifest, ContentStoreReservationIdentity, DfsStoragePreparation, LocalContentStore,
    MetadataBackendError,
};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

type Result<T> = std::result::Result<T, MetadataBackendError>;
const CONVERSION: &str = ".content-conversion-v2";
const MARKER: &str = "content-reservations/00/batch-format-v1.sobj";
// Progress/receipt DATA is bounded independently from source payload streams.
const CONTROL_RECORD_LIMIT: u64 = 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
struct BlobSource {
    hash: String,
    path: PathBuf,
    offset: u64,
    size: u64,
}
fn error(detail: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(format!("content conversion: {detail}"))
}
fn io(path: &Path, source: std::io::Error) -> MetadataBackendError {
    MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    }
}
fn read<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = durable_file::read_object_file_bytes(path, CONTROL_RECORD_LIMIT).map_err(error)?;
    serde_ext::from_bytes(&bytes).map_err(error)
}
fn write<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_ext::to_bytes(value, 1, LengthMode::U32).map_err(error)?;
    durable_file::write_file_atomic(
        path,
        &bytes,
        durable_file::DurableAtomicFileOptions::new("conversion", "tmp").sync_all(),
    )
    .map_err(error)
}
fn lock(path: &Path) -> Result<File> {
    if let Some(parent) = path.parent() {
        durable_file::ensure_directory(parent).map_err(error)?;
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|source| io(path, source))?;
    file.try_lock_exclusive()
        .map_err(|source| io(path, source))?;
    Ok(file)
}
/// Source-only generation discovery. Current core does not interpret these
/// historical marker values; an absent marker remains the legacy loose path.
pub(crate) fn source_format(root: &Path) -> Result<Option<u32>> {
    let path = root.join(MARKER);
    match fs::metadata(&path) {
        Err(failure) if failure.kind()==std::io::ErrorKind::NotFound=>return Ok(None),
        Err(failure)=>return Err(io(&path,failure)),
        Ok(_)=>{},
    }
    let bytes = durable_file::read_object_file_bytes(&path, 64 * 1024).map_err(error)?;
    let marker: serde_json::Value = serde_ext::from_bytes(&bytes).map_err(error)?;
    let version=marker.get("format_version").and_then(|value|value.as_u64())
        .ok_or_else(||error("existing content marker has no valid source generation"))?;
    Ok(Some(u32::try_from(version).map_err(error)?))
}
pub(crate) fn current(root: &Path) -> Result<bool> {
    let marker_path = root.join(MARKER);
    match fs::metadata(&marker_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(io(&marker_path,error)),
        Ok(_) => {}
    }
    let bytes=durable_file::read_object_file_bytes(&marker_path,64*1024).map_err(error)?;
    let marker:serde_json::Value=serde_ext::from_bytes(&bytes).map_err(error)?;
    if marker.get("format_version").and_then(|v|v.as_u64()) != Some(LocalContentStore::FORMAT_VERSION as u64) {
        return Ok(false);
    }
    LocalContentStore::open_for_inspection(root).validate_current_layout().map_err(error)?;
    Ok(true)
}

pub use incremental::ContentConversionInspection;
pub(crate) use incremental::ContentConverter;

#[cfg(test)]
mod tests;
