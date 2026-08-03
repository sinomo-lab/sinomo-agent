//! Durable object-to-metadata-shard index.
//!
//! Sharded metadata uses this small local index to route object-id lookups
//! after path-prefix shard placement changes. The sharded backend owns routing
//! policy; this module owns the index record shape and durable file updates.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_io::{
    encode_sobj, read_sobj_optional, remove_file_if_exists, write_sobj_atomic,
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
    read_sobj_optional(&object_shard_index_path(root, object_id))
}

/// Read a disposable index before updating it. The second result bit
/// distinguishes a missing file from a present record whose payload cannot be
/// decoded, so callers can atomically heal only the latter.
pub(crate) fn read_object_shard_index_for_rebuild(
    root: &Path,
    object_id: &DfsObjectId,
) -> Result<(Option<DfsObjectShardIndexRecord>, bool)> {
    let path = object_shard_index_path(root, object_id);
    match read_object_shard_index(root, object_id) {
        Ok(index) => Ok((index, false)),
        Err(MetadataBackendError::Serde(_)) if path.is_file() => Ok((None, true)),
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

pub(crate) fn object_shard_index_matches(
    root: &Path,
    object: &DfsObjectMetadata,
    shard_id: &DfsMetadataShardId,
) -> Result<bool> {
    let desired = object_shard_index_record(object, shard_id);
    let (current, rebuild_existing) = read_object_shard_index_for_rebuild(root, &object.object_id)?;
    Ok(!rebuild_existing && current.as_ref() == Some(&desired))
}

pub(crate) fn write_content_commit_object_shard_index(
    root: &Path,
    object: &DfsObjectMetadata,
    shard_id: &DfsMetadataShardId,
) -> Result<()> {
    let path = object_shard_index_path(root, &object.object_id);
    let (index, rebuild_existing) = read_object_shard_index_for_rebuild(root, &object.object_id)?;
    if let Some(index) = index {
        let normalized_path = normalize_path(&object.path);
        if index.file_system_id == object.file_system_id
            && index.path == normalized_path
            && index.shard_id == *shard_id
        {
            return Ok(());
        }
        return write_object_shard_index(root, object, shard_id);
    }
    if rebuild_existing {
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
    let records = objects
        .iter()
        .map(|object| object_shard_index_record(object, shard_id))
        .collect::<Vec<_>>();
    write_object_shard_index_records(root, &records)
}

/// Persist a batch of lightweight routing identities without retaining or
/// cloning their complete object metadata bodies.
pub(crate) fn write_object_shard_index_records(
    root: &Path,
    records: &[DfsObjectShardIndexRecord],
) -> Result<()> {
    let mut new_objects = Vec::new();
    let mut existing_objects = Vec::new();
    for desired in records {
        let (current, rebuild_existing) =
            read_object_shard_index_for_rebuild(root, &desired.object_id)?;
        match current {
            Some(current) if &current == desired => {}
            Some(_) => existing_objects.push(desired),
            None if rebuild_existing => existing_objects.push(desired),
            None => new_objects.push(desired),
        }
    }
    let new_files = new_objects
        .into_iter()
        .map(|record| {
            Ok((
                object_shard_index_path(root, &record.object_id),
                encode_sobj(record)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    write_sobj_files_create_new_relaxed(new_files)?;
    for record in existing_objects {
        #[cfg(test)]
        OBJECT_SHARD_INDEX_WRITE_COUNT.set(OBJECT_SHARD_INDEX_WRITE_COUNT.get().saturating_add(1));
        write_sobj_atomic(&object_shard_index_path(root, &record.object_id), record)?;
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DfsObjectKind, DfsVersion};
    use sinomo_core::{DfsObjectPolicy, node::NodeId};
    use std::fs;

    fn object(index: usize) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named(format!("batch-index-object-{index}"))
                .expect("object id"),
            file_system_id: crate::sifs_file_system_id(),
            path: format!("/services/batch-index-{index}"),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: NodeId::from_legacy_named("batch-index-owner").expect("owner"),
            current_locations: Vec::new(),
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: DfsVersion::from_legacy_named(format!("batch-index-version-{index}"))
                .expect("version"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    #[test]
    fn transaction_index_writer_batches_new_records_and_skips_exact_existing_records() {
        let root = sinomo_testdir::TestDir::new("dfs-transaction-object-index-batch");
        let shard_id = DfsMetadataShardId::new("services");
        let mut objects = (0..64).map(object).collect::<Vec<_>>();

        write_transaction_object_shard_indexes(&root, &objects, &shard_id)
            .expect("write new indexes");
        for object in &objects {
            assert_eq!(
                read_object_shard_index(&root, &object.object_id).expect("read index"),
                Some(object_shard_index_record(object, &shard_id))
            );
        }

        reset_object_shard_index_write_count();
        write_transaction_object_shard_indexes(&root, &objects, &shard_id)
            .expect("repeat exact indexes");
        assert_eq!(
            object_shard_index_write_count(),
            0,
            "an overwrite whose path and shard are unchanged must not republish index files"
        );

        objects[0].path = "/services/moved-batch-index-0".to_string();
        write_transaction_object_shard_indexes(&root, &objects, &shard_id)
            .expect("replace moved index");
        assert_eq!(
            object_shard_index_write_count(),
            1,
            "only the genuinely changed index needs an atomic replacement"
        );

        fs::write(
            object_shard_index_path(&root, &objects[1].object_id),
            b"corrupt derived index",
        )
        .expect("corrupt rebuildable index");
        reset_object_shard_index_write_count();
        write_transaction_object_shard_indexes(&root, &objects, &shard_id)
            .expect("heal corrupt derived index after durable commit");
        assert_eq!(
            read_object_shard_index(&root, &objects[1].object_id).expect("read healed index"),
            Some(object_shard_index_record(&objects[1], &shard_id))
        );
        assert_eq!(
            object_shard_index_write_count(),
            1,
            "a corrupt derived index is atomically rebuilt without rewriting exact peers"
        );
    }
}
