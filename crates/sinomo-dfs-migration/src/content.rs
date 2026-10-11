//! Incremental conversion of retired content layouts into the sole native layout.
//! DFS never invokes old readers: its cold-open provider runs this converter.
mod incremental;
mod snapshot_cursor;

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
const RECORD_LIMIT: u64 = u32::MAX as u64;

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
    let bytes = durable_file::read_object_file_bytes(path, RECORD_LIMIT).map_err(error)?;
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
pub(crate) fn current(root: &Path) -> Result<bool> {
    if !root.join(MARKER).exists() {
        return Ok(false);
    }
    let marker: serde_json::Value = read(&root.join(MARKER))?;
    Ok(
        marker.get("format_version").and_then(|v| v.as_u64()) == Some(3)
            && marker
                .get("content_binding_layout")
                .and_then(|v| v.as_str())
                == Some("inline-bindings-v2"),
    )
}
fn copy_blob(root: &Path, target: &Path, blob: &BlobSource) -> Result<()> {
    let hex = blob
        .hash
        .strip_prefix("blake3:")
        .filter(|hex| {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
        .ok_or_else(|| error("invalid source blob identity"))?;
    let destination = target.join("blobs").join(&hex[..2]).join(hex);
    let source = root.join(&blob.path);
    let mut input = File::open(&source).map_err(|e| io(&source, e))?;
    let len = input.metadata().map_err(|e| io(&source, e))?.len();
    if blob
        .offset
        .checked_add(blob.size)
        .is_none_or(|end| end > len)
    {
        return Err(error("source blob locator exceeds file"));
    }
    input
        .seek(SeekFrom::Start(blob.offset))
        .map_err(|e| io(&source, e))?;
    durable_file::ensure_directory(destination.parent().unwrap()).map_err(error)?;
    let temporary = destination.with_extension("conversion-tmp");
    let mut output = File::create(&temporary).map_err(|e| io(&temporary, e))?;
    let mut remaining = blob.size;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0; 64 * 1024];
    while remaining != 0 {
        let count = usize::try_from(remaining.min(buffer.len() as u64)).map_err(error)?;
        input
            .read_exact(&mut buffer[..count])
            .map_err(|e| io(&source, e))?;
        hasher.update(&buffer[..count]);
        output
            .write_all(&buffer[..count])
            .map_err(|e| io(&temporary, e))?;
        remaining -= count as u64;
    }
    if hasher.finalize().to_hex().as_str() != hex {
        return Err(error(format!(
            "source content hash mismatch for {}",
            blob.hash
        )));
    }
    let synced = durable_file::sync_private_staged_file(&temporary, output).map_err(error)?;
    match durable_file::install_synced_private_staged_file(&synced, &destination) {
        Ok(()) => {}
        Err(_) if destination.is_file() => {
            let (hash, size) =
                durable_file::file_blake3_hash_and_size(&destination).map_err(error)?;
            if hash.to_hex().as_str() != hex || size != blob.size {
                return Err(error("candidate blob conflicts with source"));
            }
            durable_file::remove_file_if_exists_and_sync_parent(&temporary).map_err(error)?;
        }
        Err(e) => return Err(error(e)),
    }
    Ok(())
}
fn install_blob(root: &Path, target: &Path, blob: &BlobSource) -> Result<()> {
    let hex = blob
        .hash
        .strip_prefix("blake3:")
        .ok_or_else(|| error("invalid source hash"))?;
    let relative = Path::new("blobs").join(&hex[..2]).join(hex);
    let source = target.join(&relative);
    let destination = root.join(&relative);
    if destination.exists() {
        let (hash, size) = durable_file::file_blake3_hash_and_size(&destination).map_err(error)?;
        if hash.to_hex().as_str() != hex || size != blob.size {
            return Err(error("existing file blob conflicts with converted content"));
        }
    } else {
        durable_file::ensure_directory(&root.join("blobs")).map_err(error)?;
        durable_file::sync_directory(root).map_err(error)?;
        durable_file::ensure_directory(destination.parent().unwrap()).map_err(error)?;
        fs::hard_link(&source, &destination).map_err(|e| io(&destination, e))?;
        durable_file::sync_directory(destination.parent().unwrap()).map_err(error)?;
        durable_file::sync_directory(&root.join("blobs")).map_err(error)?;
    }
    Ok(())
}
pub use incremental::{ContentConversionInspection, ContentConverter};

#[cfg(test)]
mod tests;
