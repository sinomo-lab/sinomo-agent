//! Durable object-to-metadata-shard index.
//!
//! Sharded metadata uses this small local index to route object-id lookups
//! after path-prefix shard placement changes. The sharded backend owns routing
//! policy; this module owns the index record shape and durable file updates.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::metadata_backend::Result;
use crate::metadata_io::{
    encode_sobj, read_sobj_optional, remove_file_if_exists, write_sobj_atomic,
    write_sobj_files_create_new_relaxed,
};
use crate::metadata_keys::{key_segment, normalize_path};
use crate::{DfsFileSystemId, DfsMetadataShardId, DfsObjectId, DfsObjectMetadata};

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
    read_sobj_optional(&object_shard_index_path(root, object_id))
}

pub(crate) fn write_object_shard_index(
    root: &Path,
    object: &DfsObjectMetadata,
    shard_id: &DfsMetadataShardId,
) -> Result<()> {
    write_sobj_atomic(
        &object_shard_index_path(root, &object.object_id),
        &object_shard_index_record(object, shard_id),
    )
}

pub(crate) fn write_content_commit_object_shard_index(
    root: &Path,
    object: &DfsObjectMetadata,
    shard_id: &DfsMetadataShardId,
) -> Result<()> {
    let path = object_shard_index_path(root, &object.object_id);
    if let Some(index) = read_object_shard_index(root, &object.object_id)? {
        let normalized_path = normalize_path(&object.path);
        if index.file_system_id == object.file_system_id
            && index.path == normalized_path
            && index.shard_id == *shard_id
        {
            return Ok(());
        }
        return write_object_shard_index(root, object, shard_id);
    }
    write_sobj_files_create_new_relaxed(vec![(
        path,
        encode_sobj(&object_shard_index_record(object, shard_id))?,
    )])
}

pub(crate) fn write_transaction_object_shard_indexes(
    root: &Path,
    objects: &[DfsObjectMetadata],
    shard_id: &DfsMetadataShardId,
) -> Result<()> {
    let mut new_files = Vec::new();
    let mut existing_objects = Vec::new();
    for object in objects {
        let path = object_shard_index_path(root, &object.object_id);
        if path.is_file() {
            existing_objects.push(object);
        } else {
            new_files.push((
                path,
                encode_sobj(&object_shard_index_record(object, shard_id))?,
            ));
        }
    }
    write_sobj_files_create_new_relaxed(new_files)?;
    for object in existing_objects {
        write_object_shard_index(root, object, shard_id)?;
    }
    Ok(())
}

pub(crate) fn remove_object_shard_index(root: &Path, object_id: &DfsObjectId) -> Result<()> {
    remove_file_if_exists(&object_shard_index_path(root, object_id))
}

fn object_shard_index_record(
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
