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
use crate::{DfsFileSystem, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsTombstoneProvenance};

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
        Some(existing)
            if !crate::placement::dfs_file_system_semantically_eq(&existing, file_system) =>
        {
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
        Some(existing)
            if existing != bundle.object
                && !migration_object_generation_supersedes(&bundle.object, &existing) =>
        {
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

fn migration_object_generation_supersedes(
    incoming: &DfsObjectMetadata,
    existing: &DfsObjectMetadata,
) -> bool {
    let incoming_generation = (incoming.changed_at_unix_nanos, &incoming.version);
    let existing_generation = (existing.changed_at_unix_nanos, &existing.version);
    incoming.file_system_id == existing.file_system_id
        && (incoming_generation > existing_generation
            || (incoming_generation == existing_generation
                && incoming.kind != DfsObjectKind::Tombstone
                && existing.kind == DfsObjectKind::Tombstone))
}

pub(crate) fn put_cross_shard_tombstone<B: DfsMetadataBackend + ?Sized>(
    source: &mut B,
    object: &DfsObjectMetadata,
    source_shard_id: &DfsMetadataShardId,
    target_shard_id: &DfsMetadataShardId,
) -> Result<bool> {
    if source_shard_id == target_shard_id {
        return Ok(false);
    }
    let Some(previous) = source.get_object(&object.object_id)? else {
        return Ok(false);
    };
    let Some(tombstone) =
        cross_shard_eviction_tombstone(&previous, object, source_shard_id, target_shard_id)
    else {
        return Ok(false);
    };
    source.put_object(&tombstone)?;
    Ok(true)
}

pub(crate) fn cross_shard_eviction_tombstone(
    previous: &DfsObjectMetadata,
    object: &DfsObjectMetadata,
    source_shard_id: &DfsMetadataShardId,
    target_shard_id: &DfsMetadataShardId,
) -> Option<DfsObjectMetadata> {
    if source_shard_id == target_shard_id || previous.kind == DfsObjectKind::Tombstone {
        return None;
    }
    let previous_changed_at = previous.changed_at_unix_nanos;
    let mut tombstone = previous.clone();
    tombstone.kind = DfsObjectKind::Tombstone;
    tombstone.tombstone_provenance = Some(DfsTombstoneProvenance::SourceEviction {
        source_shard_id: source_shard_id.clone(),
        target_shard_id: target_shard_id.clone(),
    });
    tombstone.version = object.version.clone();
    // The source record must be later than the live record it replaces and at
    // least as new as the destination frame that triggered the eviction. A
    // stale caller may carry the original generation (for example while
    // paging a directory move), so fence historical namespace transactions
    // here instead of relying on every migration caller to advance ctime.
    tombstone.changed_at_unix_nanos = previous_changed_at
        .saturating_add(1)
        .max(object.changed_at_unix_nanos);
    tombstone.last_modified_by_event_id = object.last_modified_by_event_id.clone();
    Some(tombstone)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace::{directory_object, distributed_sifs};
    use crate::{DfsMetadataBackend, DfsMetadataMutation, LocalSobjMetadataBackend};
    use sinomo_core::node::NodeId;

    #[test]
    fn migration_accepts_reordered_semantically_equal_filesystem_without_rejournaling_it() {
        let root = sinomo_testdir::TestDir::new("dfs-migration-policy-order-noop");
        let target_shard_id = DfsMetadataShardId::new("target");
        let node = NodeId::from_legacy_named("node_migration_policy_order_noop").expect("node");
        let file_system = distributed_sifs(node.clone());
        let mut reordered = file_system.clone();
        reordered.subtree_policies.reverse();
        let object = directory_object("/users/migrated", node);
        let bundle = DfsObjectShardMigrationBundle {
            object,
            versions: Vec::new(),
            receipts: Vec::new(),
            intents: Vec::new(),
            conflicts: Vec::new(),
            advisory_lock_state: None,
        };
        let mut target =
            LocalSobjMetadataBackend::new(target_shard_id.clone(), root.path()).expect("target");
        let initial_sequence = target
            .put_file_system(&file_system)
            .expect("seed filesystem")
            .sequence;

        install_object_shard_migration_bundle(
            &mut target,
            &reordered,
            &bundle,
            &target_shard_id,
            32,
        )
        .expect("semantic filesystem match");

        let entries = target
            .journal_since_limited(initial_sequence, 32)
            .expect("migration journal");
        assert!(
            entries
                .iter()
                .all(|entry| !matches!(&entry.mutation, DfsMetadataMutation::PutFileSystem { .. })),
            "migration may author object state but must not rewrite an equivalent filesystem"
        );
        assert_eq!(
            target
                .get_file_system(&file_system.file_system_id)
                .expect("filesystem lookup"),
            Some(file_system),
            "semantic conflict checks must not rewrite the target's durable representation"
        );
    }

    #[test]
    fn source_eviction_tombstone_survives_namespace_recovery_with_stale_move_generation() {
        let root = sinomo_testdir::TestDir::new("dfs-source-eviction-generation");
        let source_shard_id = DfsMetadataShardId::new("source");
        let target_shard_id = DfsMetadataShardId::new("target");
        let node = NodeId::from_legacy_named("node_source_eviction_generation").expect("node");
        let file_system = distributed_sifs(node.clone());
        let mut source_object = directory_object("/services/source-child", node);
        source_object.changed_at_unix_nanos = 42;
        let historical_transaction = DfsNamespaceTransaction {
            transaction_id: "namespace-source-child-create".to_string(),
            file_system_id: source_object.file_system_id.clone(),
            objects: vec![source_object.clone()],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: time::OffsetDateTime::UNIX_EPOCH,
        };

        let mut source =
            LocalSobjMetadataBackend::new(source_shard_id.clone(), root.path()).expect("source");
        source
            .put_file_system(&file_system)
            .expect("source filesystem");
        source
            .apply_namespace_transaction(&historical_transaction)
            .expect("historical directory create");

        // Directory scans used to change only the path, leaving this frame at
        // the exact generation recorded by the historical create transaction.
        let mut stale_moved_object = source_object.clone();
        stale_moved_object.path = "/users/source-child".to_string();
        put_cross_shard_tombstone(
            &mut source,
            &stale_moved_object,
            &source_shard_id,
            &target_shard_id,
        )
        .expect("source eviction tombstone");
        let durable_tombstone = source
            .get_object(&source_object.object_id)
            .expect("source object lookup")
            .expect("source tombstone");
        assert_eq!(durable_tombstone.kind, DfsObjectKind::Tombstone);
        assert!(
            durable_tombstone.changed_at_unix_nanos > source_object.changed_at_unix_nanos,
            "source eviction must fence the historical create generation"
        );
        drop(source);

        let reopened =
            LocalSobjMetadataBackend::new(source_shard_id, root.path()).expect("reopened source");
        let recovered = reopened
            .get_object(&source_object.object_id)
            .expect("recovered object lookup")
            .expect("recovered tombstone");
        assert_eq!(
            recovered.kind,
            DfsObjectKind::Tombstone,
            "namespace recovery must not resurrect the evicted directory"
        );
        assert_eq!(
            reopened
                .get_object_by_path(&source_object.file_system_id, &source_object.path)
                .expect("recovered path lookup")
                .map(|object| object.kind),
            Some(DfsObjectKind::Tombstone)
        );
    }
}
