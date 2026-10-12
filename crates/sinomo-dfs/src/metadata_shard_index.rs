//! Rebuildable object-to-metadata-shard routing index.
//!
//! Sharded metadata uses this small local index to route object-id lookups
//! after path-prefix shard placement changes. The sharded backend owns routing
//! policy; this module owns the index record shape and atomic file updates.
//! New rows may use relaxed publication after their canonical records are
//! durable. Missing or malformed hints never replace canonical ownership.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_io::{
    encode_sobj, read_sobj, remove_file_if_exists, write_sobj_atomic,
    write_sobj_files_create_new_relaxed,
};
use crate::metadata_keys::{key_segment, normalize_path};
use crate::{DfsFileSystemId, DfsMetadataShardId, DfsObjectId, DfsObjectMetadata};

#[cfg(test)]
thread_local! {
    static OBJECT_SHARD_INDEX_WRITE_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_object_shard_index_write_count() {
    OBJECT_SHARD_INDEX_WRITE_COUNT.set(0);
}

#[cfg(test)]
pub(crate) fn object_shard_index_write_count() -> usize {
    OBJECT_SHARD_INDEX_WRITE_COUNT.get()
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsObjectShardIndexRecord {
    pub(crate) object_id: DfsObjectId,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) path: String,
    pub(crate) shard_id: DfsMetadataShardId,
}

pub(crate) fn object_shard_index_path(root: &Path, object_id: &DfsObjectId) -> PathBuf {
    root.join("shard-index")
        .join("objects")
        .join(format!("{}.sobj", key_segment(object_id)))
}

pub(crate) fn read_object_shard_index(
    root: &Path,
    object_id: &DfsObjectId,
) -> Result<Option<DfsObjectShardIndexRecord>> {
    read_object_shard_index_for_rebuild(root, object_id).map(|(record, _)| record)
}

/// Read a disposable index before updating it. The second result bit
/// distinguishes a missing file from a present record whose payload cannot be
/// decoded, so callers can atomically heal only the latter.
pub(crate) fn read_object_shard_index_for_rebuild(
    root: &Path,
    object_id: &DfsObjectId,
) -> Result<(Option<DfsObjectShardIndexRecord>, bool)> {
    let path = object_shard_index_path(root, object_id);
    // This is a disposable routing hint, not an ownership record. Read it
    // directly so a permission error or non-directory parent cannot masquerade
    // as absence through Path::exists. Only a missing file or invalid payload
    // permits callers to recover from canonical shard state.
    match read_sobj::<DfsObjectShardIndexRecord>(&path) {
        Ok(index) if index.object_id == *object_id => Ok((Some(index), false)),
        Ok(_) | Err(MetadataBackendError::Serde(_)) => Ok((None, true)),
        Err(MetadataBackendError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok((None, false))
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn write_object_shard_index(
    root: &Path,
    object: &DfsObjectMetadata,
    shard_id: &DfsMetadataShardId,
) -> Result<()> {
    #[cfg(test)]
    OBJECT_SHARD_INDEX_WRITE_COUNT.set(OBJECT_SHARD_INDEX_WRITE_COUNT.get().saturating_add(1));
    write_sobj_atomic(
        &object_shard_index_path(root, &object.object_id),
        &object_shard_index_record(object, shard_id),
    )
}

pub(crate) fn write_new_object_shard_indexes<'a>(
    root: &Path,
    placements: impl IntoIterator<Item = (&'a DfsObjectMetadata, &'a DfsMetadataShardId)>,
) -> Result<()> {
    let files = placements
        .into_iter()
        .map(|(object, shard_id)| {
            Ok((
                object_shard_index_path(root, &object.object_id),
                encode_sobj(&object_shard_index_record(object, shard_id))?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    write_sobj_files_create_new_relaxed(files)
}

pub(crate) fn remove_object_shard_index(root: &Path, object_id: &DfsObjectId) -> Result<()> {
    remove_file_if_exists(&object_shard_index_path(root, object_id))
}

pub(crate) fn object_shard_index_record(
    object: &DfsObjectMetadata,
    shard_id: &DfsMetadataShardId,
) -> DfsObjectShardIndexRecord {
    DfsObjectShardIndexRecord {
        object_id: object.object_id.clone(),
        file_system_id: object.file_system_id.clone(),
        path: normalize_path(&object.path),
        shard_id: shard_id.clone(),
    }
}
