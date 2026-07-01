//! Cross-shard metadata migration helpers.
//!
//! The sharded backend owns routing decisions, while this module owns the
//! mechanics of copying an object's durable metadata family between shard
//! backends and leaving an eviction tombstone in the source shard.

use crate::metadata_backend::{DfsMetadataBackend, MetadataBackendError, Result};
use crate::metadata_receipt::receipt_for_replica_from_backend;
use crate::metadata_records::{
    DfsConflictRecord, DfsMetadataShardId, DfsObjectVersionRecord, DfsReplicaReceiptRecord,
    DfsWriteIntentRecord,
};
use crate::{DfsFileSystem, DfsObjectId, DfsObjectKind, DfsObjectMetadata};

#[derive(Clone, Debug)]
pub(crate) struct DfsObjectShardMigrationBundle {
    pub(crate) object: DfsObjectMetadata,
    pub(crate) versions: Vec<DfsObjectVersionRecord>,
    pub(crate) receipts: Vec<DfsReplicaReceiptRecord>,
    pub(crate) intents: Vec<DfsWriteIntentRecord>,
    pub(crate) conflicts: Vec<DfsConflictRecord>,
}

pub(crate) fn collect_object_shard_migration_bundle<B: DfsMetadataBackend + ?Sized>(
    source: &B,
    object_id: &DfsObjectId,
    page_size: usize,
) -> Result<DfsObjectShardMigrationBundle> {
    let object =
        source
            .get_object(object_id)?
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            })?;
    let mut versions = Vec::new();
    let mut version_cursor = None;
    loop {
        let page =
            source.versions_for_object_page(object_id, version_cursor.as_ref(), page_size)?;
        versions.extend(page.versions);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        version_cursor = Some(next_cursor);
    }
    let mut receipts = Vec::new();
    for version in &versions {
        let mut receipt_cursor = None;
        loop {
            let page = source.receipts_for_version_page(
                object_id,
                &version.version,
                receipt_cursor.as_ref(),
                page_size,
            )?;
            receipts.extend(page.receipts);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            receipt_cursor = Some(next_cursor);
        }
    }
    let mut intents = Vec::new();
    let mut intent_cursor = None;
    loop {
        let page = source.intents_for_object_page(object_id, intent_cursor.as_ref(), page_size)?;
        intents.extend(page.intents);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        intent_cursor = Some(next_cursor);
    }
    let mut conflicts = Vec::new();
    let mut conflict_cursor = None;
    loop {
        let page =
            source.conflicts_for_object_page(object_id, conflict_cursor.as_ref(), page_size)?;
        conflicts.extend(page.conflicts);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        conflict_cursor = Some(next_cursor);
    }
    Ok(DfsObjectShardMigrationBundle {
        object,
        versions,
        receipts,
        intents,
        conflicts,
    })
}

pub(crate) fn install_object_shard_migration_bundle<B: DfsMetadataBackend + ?Sized>(
    target: &mut B,
    file_system: &DfsFileSystem,
    bundle: &DfsObjectShardMigrationBundle,
    target_shard_id: &DfsMetadataShardId,
    page_size: usize,
) -> Result<()> {
    match target.get_file_system(&file_system.file_system_id)? {
        Some(existing) if existing != *file_system => {
            return Err(MetadataBackendError::Serde(format!(
                "target metadata shard {} has conflicting filesystem {} during migration",
                target_shard_id.0, file_system.file_system_id
            )));
        }
        Some(_) => {}
        None => {
            target.put_file_system(file_system)?;
        }
    }

    for version in &bundle.versions {
        match target.get_version(&version.object_id, &version.version)? {
            Some(existing) if existing != *version => {
                return Err(MetadataBackendError::Serde(format!(
                    "target metadata shard {} has conflicting version {} for object {} during migration",
                    target_shard_id.0, version.version, version.object_id
                )));
            }
            Some(_) => {}
            None => {
                target.put_version(version)?;
            }
        }
    }
    for receipt in &bundle.receipts {
        if let Some(existing) = receipt_for_replica_from_backend(
            &*target,
            &receipt.receipt.object_id,
            &receipt.receipt.committed_version,
            &receipt.replica_node_id,
            page_size,
        )? {
            if &existing != receipt {
                return Err(MetadataBackendError::Serde(format!(
                    "target metadata shard {} has conflicting receipt for object {} version {} replica {} during migration",
                    target_shard_id.0,
                    receipt.receipt.object_id,
                    receipt.receipt.committed_version,
                    receipt.replica_node_id
                )));
            }
        } else {
            target.put_receipt(receipt)?;
        }
    }
    for intent in &bundle.intents {
        match target.get_intent(&intent.object_id, &intent.intent_id)? {
            Some(existing) if existing != *intent => {
                return Err(MetadataBackendError::Serde(format!(
                    "target metadata shard {} has conflicting intent {} for object {} during migration",
                    target_shard_id.0, intent.intent_id, intent.object_id
                )));
            }
            Some(_) => {}
            None => {
                target.put_intent(intent)?;
            }
        }
    }
    for conflict in &bundle.conflicts {
        match target.get_conflict(&conflict.object_id, &conflict.conflict_id)? {
            Some(existing) if existing != *conflict => {
                return Err(MetadataBackendError::Serde(format!(
                    "target metadata shard {} has conflicting conflict {} for object {} during migration",
                    target_shard_id.0, conflict.conflict_id, conflict.object_id
                )));
            }
            Some(_) => {}
            None => {
                target.put_conflict(conflict)?;
            }
        }
    }
    match target.get_object(&bundle.object.object_id)? {
        Some(existing) if existing != bundle.object => {
            return Err(MetadataBackendError::Serde(format!(
                "target metadata shard {} has conflicting object {} during migration",
                target_shard_id.0, bundle.object.object_id
            )));
        }
        Some(_) => {}
        None => {
            target.put_object(&bundle.object)?;
        }
    }
    Ok(())
}

pub(crate) fn put_cross_shard_tombstone<B: DfsMetadataBackend + ?Sized>(
    source: &mut B,
    object: &DfsObjectMetadata,
    source_shard_id: &DfsMetadataShardId,
    target_shard_id: &DfsMetadataShardId,
) -> Result<()> {
    if source_shard_id == target_shard_id {
        return Ok(());
    }
    let Some(previous) = source.get_object(&object.object_id)? else {
        return Ok(());
    };
    if previous.kind == DfsObjectKind::Tombstone {
        return Ok(());
    }
    let mut tombstone = previous;
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.version = object.version.clone();
    tombstone.changed_at_unix_nanos = object.changed_at_unix_nanos;
    tombstone.last_modified_by_event_id = object.last_modified_by_event_id.clone();
    source.put_object(&tombstone)?;
    Ok(())
}
