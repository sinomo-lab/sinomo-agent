//! Cross-shard metadata migration helpers.
//!
//! The sharded backend owns routing decisions, while this module owns the
//! mechanics of copying an object's durable metadata family between shard
//! backends and leaving an eviction tombstone in the source shard.

use crate::advisory_lock::DfsAdvisoryLockStateRecord;
use crate::metadata_api::DfsMetadataBackend;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_pages::collect_metadata_pages;
use crate::metadata_receipt::receipt_for_replica_from_backend;
use crate::metadata_records::{
    DfsConflictRecord, DfsMetadataShardId, DfsNamespaceTransaction, DfsObjectVersionRecord,
    DfsReplicaReceiptRecord, DfsWriteIntentRecord,
};
use crate::{DfsFileSystem, DfsObjectId, DfsObjectKind, DfsObjectMetadata};

#[derive(Clone, Debug)]
pub(crate) struct DfsObjectShardMigrationBundle {
    pub(crate) object: DfsObjectMetadata,
    pub(crate) versions: Vec<DfsObjectVersionRecord>,
    pub(crate) receipts: Vec<DfsReplicaReceiptRecord>,
    pub(crate) intents: Vec<DfsWriteIntentRecord>,
    pub(crate) conflicts: Vec<DfsConflictRecord>,
    pub(crate) advisory_lock_state: Option<DfsAdvisoryLockStateRecord>,
}

#[derive(Clone, Debug)]
pub(crate) struct DfsObjectMetadataFamily {
    pub(crate) versions: Vec<DfsObjectVersionRecord>,
    pub(crate) receipts: Vec<DfsReplicaReceiptRecord>,
    pub(crate) intents: Vec<DfsWriteIntentRecord>,
    pub(crate) conflicts: Vec<DfsConflictRecord>,
    pub(crate) advisory_lock_state: Option<DfsAdvisoryLockStateRecord>,
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
    let family = collect_object_metadata_family(source, object_id, page_size, None)?;
    Ok(DfsObjectShardMigrationBundle {
        object,
        versions: family.versions,
        receipts: family.receipts,
        intents: family.intents,
        conflicts: family.conflicts,
        advisory_lock_state: family.advisory_lock_state,
    })
}

/// Collect the durable metadata records attached to one object.
///
/// When `version_history_limit` is set, only the newest version records are
/// retained and receipts are collected for that bounded version set. Intents
/// and conflicts remain unbounded because they carry recovery state independent
/// of visible version history.
pub(crate) fn collect_object_metadata_family<B: DfsMetadataBackend + ?Sized>(
    source: &B,
    object_id: &DfsObjectId,
    page_size: usize,
    version_history_limit: Option<usize>,
) -> Result<DfsObjectMetadataFamily> {
    let versions = collect_metadata_pages(|cursor| {
        let page = source.versions_for_object_page(object_id, cursor, page_size)?;
        Ok((page.versions, page.next_cursor))
    })?;
    let mut versions = versions;
    if let Some(limit) = version_history_limit {
        versions.sort_by(|left, right| right.created_at.cmp(&left.created_at));
        versions.truncate(limit);
    }
    let mut receipts = Vec::new();
    for version in &versions {
        receipts.extend(collect_metadata_pages(|cursor| {
            let page =
                source.receipts_for_version_page(object_id, &version.version, cursor, page_size)?;
            Ok((page.receipts, page.next_cursor))
        })?);
    }
    let intents = collect_metadata_pages(|cursor| {
        let page = source.intents_for_object_page(object_id, cursor, page_size)?;
        Ok((page.intents, page.next_cursor))
    })?;
    let conflicts = collect_metadata_pages(|cursor| {
        let page = source.conflicts_for_object_page(object_id, cursor, page_size)?;
        Ok((page.conflicts, page.next_cursor))
    })?;
    let advisory_lock_state = source
        .get_object(object_id)?
        .map(|object| source.get_advisory_lock_state(&object.file_system_id, object_id))
        .transpose()?
        .flatten();
    Ok(DfsObjectMetadataFamily {
        versions,
        receipts,
        intents,
        conflicts,
        advisory_lock_state,
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
        Some(_) | None => {}
    }
    let advisory_lock_states = bundle
        .advisory_lock_state
        .iter()
        .cloned()
        .map(|mut state| {
            state.authority_rebind_required = true;
            state
        })
        .collect();
    target.apply_namespace_transaction(&DfsNamespaceTransaction {
        transaction_id: format!(
            "migrate-object:{}:{}",
            bundle.object.object_id, target_shard_id.0
        ),
        file_system_id: bundle.object.file_system_id.clone(),
        objects: vec![bundle.object.clone()],
        versions: Vec::new(),
        advisory_lock_states,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    })?;
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
