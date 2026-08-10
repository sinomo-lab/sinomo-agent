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
    DfsConflictRecord, DfsCrossShardPinTransferRecord, DfsCrossShardSourceFamily,
    DfsMetadataShardId, DfsMetadataVersionPinRecord, DfsNamespaceTransaction,
    DfsObjectVersionRecord, DfsReplicaReceiptRecord, DfsWriteIntentReceipt,
    DfsWriteIntentReceiptRetentionState, DfsWriteIntentRecord,
};
use crate::{DfsFileSystem, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsTombstoneProvenance};
use std::collections::BTreeSet;

#[derive(Clone, Debug)]
pub(crate) struct DfsObjectShardMigrationBundle {
    pub(crate) object: DfsObjectMetadata,
    /// Exact source journal head held stable while this family was captured.
    pub(crate) source_journal_sequence: u64,
    pub(crate) versions: Vec<DfsObjectVersionRecord>,
    pub(crate) version_pins: Vec<DfsMetadataVersionPinRecord>,
    pub(crate) pin_transfers: Vec<DfsCrossShardPinTransferRecord>,
    pub(crate) receipts: Vec<DfsReplicaReceiptRecord>,
    pub(crate) intents: Vec<DfsWriteIntentRecord>,
    pub(crate) intent_receipts: Vec<DfsWriteIntentReceipt>,
    pub(crate) intent_receipt_retention: DfsWriteIntentReceiptRetentionState,
    pub(crate) conflicts: Vec<DfsConflictRecord>,
    pub(crate) advisory_lock_state: Option<DfsAdvisoryLockStateRecord>,
}

impl From<DfsObjectShardMigrationBundle> for DfsCrossShardSourceFamily {
    fn from(bundle: DfsObjectShardMigrationBundle) -> Self {
        Self {
            source_object: bundle.object,
            source_journal_sequence: bundle.source_journal_sequence,
            versions: bundle.versions,
            version_pins: bundle.version_pins,
            pin_transfers: bundle.pin_transfers,
            receipts: bundle.receipts,
            intents: bundle.intents,
            intent_receipts: bundle.intent_receipts,
            intent_receipt_retention: bundle.intent_receipt_retention,
            conflicts: bundle.conflicts,
            advisory_lock_state: bundle.advisory_lock_state,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DfsObjectMetadataFamily {
    pub(crate) versions: Vec<DfsObjectVersionRecord>,
    pub(crate) version_pins: Vec<DfsMetadataVersionPinRecord>,
    pub(crate) pin_transfers: Vec<DfsCrossShardPinTransferRecord>,
    pub(crate) receipts: Vec<DfsReplicaReceiptRecord>,
    pub(crate) intents: Vec<DfsWriteIntentRecord>,
    pub(crate) intent_receipts: Vec<DfsWriteIntentReceipt>,
    pub(crate) intent_receipt_retention: DfsWriteIntentReceiptRetentionState,
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
        source_journal_sequence: source.journal_last_sequence()?,
        versions: family.versions,
        version_pins: family.version_pins,
        pin_transfers: family.pin_transfers,
        receipts: family.receipts,
        intents: family.intents,
        intent_receipts: family.intent_receipts,
        intent_receipt_retention: family.intent_receipt_retention,
        conflicts: family.conflicts,
        advisory_lock_state: family.advisory_lock_state,
    })
}

/// Collect the durable metadata records attached to one object.
///
/// When `version_history_limit` is set, the newest version records plus every
/// durably pinned version are retained, and receipts are collected for that
/// bounded/protected version set. Full intents, compact intent receipts, and
/// conflicts remain unbounded because they carry recovery or exact-retry state
/// independent of visible history.
pub(crate) fn collect_object_metadata_family<B: DfsMetadataBackend + ?Sized>(
    source: &B,
    object_id: &DfsObjectId,
    page_size: usize,
    version_history_limit: Option<usize>,
) -> Result<DfsObjectMetadataFamily> {
    let version_pins = source.version_pins_for_object(object_id)?;
    let pin_transfers = source.cross_shard_pin_transfers_for_object(object_id)?;
    let mut versions = collect_metadata_pages(|cursor| {
        let page = source.versions_for_object_page(object_id, cursor, page_size)?;
        Ok((page.versions, page.next_cursor))
    })?;
    for pin in &version_pins {
        let Some(version) = versions.iter().find(|record| record.version == pin.version) else {
            return Err(MetadataBackendError::Serde(format!(
                "durable version pin {} for object {} references missing version {} during metadata-family collection",
                pin.pin_id, object_id, pin.version
            )));
        };
        if version.file_system_id != pin.file_system_id {
            return Err(MetadataBackendError::Serde(format!(
                "durable version pin {} for object {} version {} has filesystem {}, expected {}",
                pin.pin_id, object_id, pin.version, pin.file_system_id, version.file_system_id
            )));
        }
    }
    if let Some(limit) = version_history_limit {
        versions.sort_by(|left, right| right.created_at.cmp(&left.created_at));
        let retained_versions = versions
            .iter()
            .take(limit)
            .map(|record| record.version.clone())
            .chain(version_pins.iter().map(|pin| pin.version.clone()))
            .collect::<BTreeSet<_>>();
        versions.retain(|record| retained_versions.contains(&record.version));
    }
    let mut receipts = Vec::new();
    for version in &versions {
        receipts.extend(collect_metadata_pages(|cursor| {
            let page =
                source.receipts_for_version_page(object_id, &version.version, cursor, page_size)?;
            Ok((page.receipts, page.next_cursor))
        })?);
    }
    let intent_history = source.object_write_intent_history(object_id)?;
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
        version_pins,
        pin_transfers,
        receipts,
        intents: intent_history.intents,
        intent_receipts: intent_history.receipts,
        intent_receipt_retention: intent_history.retention,
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
    let existing_pin_transfers =
        target.cross_shard_pin_transfers_for_object(&bundle.object.object_id)?;
    for transfer in existing_pin_transfers {
        target.remove_cross_shard_pin_transfer_for_authoritative_migration(
            &transfer.file_system_id,
            &transfer.object_id,
            &transfer.transfer_id,
        )?;
    }
    let existing_version_pins = target.version_pins_for_object(&bundle.object.object_id)?;
    let desired_version_pins = bundle
        .version_pins
        .iter()
        .map(|pin| ((pin.version.clone(), pin.pin_id.clone()), pin))
        .collect::<std::collections::BTreeMap<_, _>>();
    for existing in &existing_version_pins {
        let key = (existing.version.clone(), existing.pin_id.clone());
        if desired_version_pins
            .get(&key)
            .is_some_and(|desired| *desired == existing)
        {
            continue;
        }
        target.remove_version_pin(
            &existing.file_system_id,
            &existing.object_id,
            &existing.version,
            &existing.pin_id,
        )?;
    }
    for transfer in &bundle.pin_transfers {
        target.install_cross_shard_pin_transfer_for_authoritative_migration(
            transfer,
            &bundle.versions,
        )?;
    }
    let transfer_fenced_pin_keys = bundle
        .pin_transfers
        .iter()
        .flat_map(|transfer| transfer.source_pins.iter())
        .map(|pin| (pin.version.clone(), pin.pin_id.clone()))
        .collect::<BTreeSet<_>>();
    for pin in &bundle.version_pins {
        if !transfer_fenced_pin_keys.contains(&(pin.version.clone(), pin.pin_id.clone())) {
            target.put_version_pin(pin)?;
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
        if let Some(receipt) = target.get_intent_receipt(&intent.object_id, &intent.intent_id)? {
            if !receipt.matches_intent(intent) {
                return Err(MetadataBackendError::Serde(format!(
                    "target metadata shard {} has conflicting compact intent receipt {} for object {} during migration",
                    target_shard_id.0, intent.intent_id, intent.object_id
                )));
            }
            continue;
        }
        match target.get_intent(&intent.object_id, &intent.intent_id)? {
            Some(existing) if existing != *intent => {
                return Err(MetadataBackendError::Serde(format!(
                    "target metadata shard {} has conflicting intent {} for object {} during migration",
                    target_shard_id.0, intent.intent_id, intent.object_id
                )));
            }
            Some(_) => {}
            None => {
                target.put_migrated_intent_for_object(Some(&bundle.object), intent)?;
            }
        }
    }
    for receipt in &bundle.intent_receipts {
        match target.get_intent_receipt(&receipt.object_id, &receipt.intent_id)? {
            Some(existing) if existing != *receipt => {
                return Err(MetadataBackendError::Serde(format!(
                    "target metadata shard {} has conflicting compact intent receipt {} for object {} during migration",
                    target_shard_id.0, receipt.intent_id, receipt.object_id
                )));
            }
            Some(_) => {}
            None => {
                if let Some(existing) = target.get_intent(&receipt.object_id, &receipt.intent_id)?
                    && !receipt.matches_intent(&existing)
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "target metadata shard {} has conflicting full intent {} for object {} during migration",
                        target_shard_id.0, receipt.intent_id, receipt.object_id
                    )));
                }
                target.put_intent_receipt_for_object(Some(&bundle.object), receipt)?;
            }
        }
    }
    if !bundle.intent_receipt_retention.is_empty() {
        target.merge_intent_receipt_retention_for_object(
            Some(&bundle.object),
            &bundle.object.file_system_id,
            &bundle.object.object_id,
            &bundle.intent_receipt_retention,
        )?;
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
        cross_shard_eviction_tombstone(&previous, object, source_shard_id, target_shard_id, None)
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
    pin_transfer_id: Option<&str>,
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
        pin_transfer_id: pin_transfer_id.map(str::to_string),
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
    use crate::{
        DfsMetadataBackend, DfsMetadataMutation, DfsReplicationPolicy, DfsWriteIntentStatus,
        LocalSobjMetadataBackend, object_version_record,
    };
    use sinomo_core::node::NodeId;
    use time::OffsetDateTime;

    fn terminal_intent(
        object: &DfsObjectMetadata,
        coordinator_node_id: NodeId,
        counter: u64,
    ) -> DfsWriteIntentRecord {
        DfsWriteIntentRecord {
            intent_id: format!(
                "dfs_intent_{}_migration_{counter}",
                coordinator_node_id.as_str()
            ),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: None,
            target_version: object.version.clone(),
            coordinator_node_id,
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_node_ids: Vec::new(),
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Aborted,
            content_hash: object.content_hash.clone(),
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        }
    }

    fn seed_migration_source(
        label: &str,
    ) -> (
        sinomo_testdir::TestDir,
        LocalSobjMetadataBackend,
        crate::DfsFileSystem,
        DfsObjectMetadata,
    ) {
        let root = sinomo_testdir::TestDir::new(label);
        let node = NodeId::from_legacy_named(format!("node_{label}")).expect("node");
        let file_system = distributed_sifs(node.clone());
        let path = format!("/users/{label}");
        let object = directory_object(&path, node);
        let mut source = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new(format!("source-{label}")),
            root.path().join("source"),
        )
        .expect("source");
        source
            .put_file_system(&file_system)
            .expect("source filesystem");
        source
            .put_object_and_version(&object, &object_version_record(&object, None))
            .expect("source object");
        (root, source, file_system, object)
    }

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
            source_journal_sequence: 0,
            versions: Vec::new(),
            version_pins: Vec::new(),
            pin_transfers: Vec::new(),
            receipts: Vec::new(),
            intents: Vec::new(),
            intent_receipts: Vec::new(),
            intent_receipt_retention: DfsWriteIntentReceiptRetentionState::default(),
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
    fn migration_preserves_durable_pins_and_versions_beyond_the_history_cap() {
        let (root, mut source, file_system, object) =
            seed_migration_source("migration-version-pin");
        let old_version =
            crate::DfsVersion::from_legacy_named("version_migration_pinned_old").expect("version");
        let mut old_object = object.clone();
        old_object.version = old_version.clone();
        old_object.content_hash = Some("blake3:migration-pinned-old".to_string());
        let old_record = object_version_record(&old_object, None);
        source.put_version(&old_record).expect("old version");
        let pin = DfsMetadataVersionPinRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: old_version.clone(),
            pin_id: "checkpoint-migration".to_string(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        source
            .put_version_pin(&pin)
            .expect("durable source pin")
            .expect("new source pin");

        let bounded = collect_object_metadata_family(&source, &object.object_id, 16, Some(0))
            .expect("bounded family");
        assert_eq!(bounded.version_pins, vec![pin.clone()]);
        assert_eq!(
            bounded
                .versions
                .iter()
                .map(|record| record.version.clone())
                .collect::<Vec<_>>(),
            vec![old_version.clone()],
            "pinned history must survive even a zero-version migration cap"
        );

        let bundle =
            collect_object_shard_migration_bundle(&source, &object.object_id, 16).expect("bundle");
        let target_shard_id = DfsMetadataShardId::new("target-version-pin");
        let target_root = root.path().join("target");
        let mut target =
            LocalSobjMetadataBackend::new(target_shard_id.clone(), &target_root).expect("target");
        install_object_shard_migration_bundle(
            &mut target,
            &file_system,
            &bundle,
            &target_shard_id,
            16,
        )
        .expect("install pinned bundle");
        assert_eq!(
            target
                .version_pins_for_object(&object.object_id)
                .expect("target pins"),
            vec![pin]
        );
        assert_eq!(
            target
                .get_version(&object.object_id, &old_version)
                .expect("target pinned version"),
            Some(old_record)
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

    #[test]
    fn migration_preserves_compact_intent_receipt_without_rehydrating_full_body() {
        let (root, mut source, file_system, object) =
            seed_migration_source("migration_compact_intent_receipt");
        let coordinator =
            NodeId::from_legacy_named("node_migration_receipt_coordinator").expect("node");
        let intent = terminal_intent(&object, coordinator, 7);
        source.put_intent(&intent).expect("source full intent");
        let receipt = DfsWriteIntentReceipt::from_intent(&intent, OffsetDateTime::UNIX_EPOCH)
            .expect("terminal receipt");
        source
            .put_intent_receipt_for_object(Some(&object), &receipt)
            .expect("compact source intent");

        let bundle =
            collect_object_shard_migration_bundle(&source, &object.object_id, 16).expect("bundle");
        assert!(bundle.intents.is_empty());
        assert_eq!(bundle.intent_receipts, vec![receipt.clone()]);

        let target_root = root.path().join("target");
        let target_shard_id = DfsMetadataShardId::new("target-compact-receipt");
        let mut target =
            LocalSobjMetadataBackend::new(target_shard_id.clone(), &target_root).expect("target");
        install_object_shard_migration_bundle(
            &mut target,
            &file_system,
            &bundle,
            &target_shard_id,
            16,
        )
        .expect("install bundle");
        drop(target);

        let target =
            LocalSobjMetadataBackend::new(target_shard_id, &target_root).expect("reopen target");
        let history = target
            .object_write_intent_history(&object.object_id)
            .expect("target history");
        assert!(history.intents.is_empty());
        assert_eq!(history.receipts, vec![receipt]);
    }

    #[test]
    fn migration_transfers_evicted_intent_floor_and_rejects_stale_retry() {
        let (root, mut source, file_system, object) =
            seed_migration_source("migration_evicted_intent_floor");
        let coordinator =
            NodeId::from_legacy_named("node_migration_floor_coordinator").expect("node");
        let stale_intent = terminal_intent(&object, coordinator, 11);
        let receipt = DfsWriteIntentReceipt::from_intent(&stale_intent, OffsetDateTime::UNIX_EPOCH)
            .expect("terminal receipt");
        source
            .put_intent_receipt_for_object(Some(&object), &receipt)
            .expect("source receipt");
        source
            .apply_migrated_object_family_gc_mutation(&DfsMetadataMutation::remove_intent_receipt(
                object.file_system_id.clone(),
                object.object_id.clone(),
                receipt.intent_id.clone(),
                DfsWriteIntentReceiptRetentionState::eviction_delta(&receipt),
            ))
            .expect("evict source receipt");

        let bundle =
            collect_object_shard_migration_bundle(&source, &object.object_id, 16).expect("bundle");
        assert!(bundle.intents.is_empty());
        assert!(bundle.intent_receipts.is_empty());
        assert!(!bundle.intent_receipt_retention.is_empty());

        let target_shard_id = DfsMetadataShardId::new("target-evicted-floor");
        let mut target =
            LocalSobjMetadataBackend::new(target_shard_id.clone(), root.path().join("target"))
                .expect("target");
        install_object_shard_migration_bundle(
            &mut target,
            &file_system,
            &bundle,
            &target_shard_id,
            16,
        )
        .expect("install bundle");

        let error = target
            .put_intent(&stale_intent)
            .expect_err("evicted retry must stay fenced after migration");
        assert!(error.to_string().contains("idempotency history expired"));
    }

    #[test]
    fn migration_retention_merge_preserves_a_higher_target_floor() {
        let (root, mut source, file_system, object) =
            seed_migration_source("migration_higher_target_floor");
        let coordinator = NodeId::from_legacy_named("node_migration_higher_floor").expect("node");
        let source_receipt = DfsWriteIntentReceipt::from_intent(
            &terminal_intent(&object, coordinator.clone(), 20),
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect("source receipt");
        source
            .merge_intent_receipt_retention_for_object(
                Some(&object),
                &object.file_system_id,
                &object.object_id,
                &DfsWriteIntentReceiptRetentionState::eviction_delta(&source_receipt),
            )
            .expect("source floor");
        let migrated_full_intent = terminal_intent(&object, coordinator.clone(), 30);
        source
            .put_intent(&migrated_full_intent)
            .expect("source full intent above its floor");
        let bundle =
            collect_object_shard_migration_bundle(&source, &object.object_id, 16).expect("bundle");

        let target_shard_id = DfsMetadataShardId::new("target-higher-floor");
        let mut target =
            LocalSobjMetadataBackend::new(target_shard_id.clone(), root.path().join("target"))
                .expect("target");
        target
            .put_file_system(&file_system)
            .expect("target filesystem");
        let target_receipt = DfsWriteIntentReceipt::from_intent(
            &terminal_intent(&object, coordinator.clone(), 50),
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect("target receipt");
        target
            .merge_intent_receipt_retention_for_object(
                Some(&object),
                &object.file_system_id,
                &object.object_id,
                &DfsWriteIntentReceiptRetentionState::eviction_delta(&target_receipt),
            )
            .expect("higher target floor");

        install_object_shard_migration_bundle(
            &mut target,
            &file_system,
            &bundle,
            &target_shard_id,
            16,
        )
        .expect("install bundle");
        let retention = target
            .intent_receipt_retention_for_object(&object.object_id)
            .expect("merged retention");
        assert_eq!(
            retention
                .max_evicted_counter_by_object_and_coordinator
                .get(&object.object_id)
                .and_then(|floors| floors.get(&coordinator)),
            Some(&50),
            "a lower source floor must not lower the destination fence"
        );
        assert_eq!(
            target
                .object_write_intent_history(&object.object_id)
                .expect("migrated target history")
                .intents,
            vec![migrated_full_intent],
            "authoritative source history must install below an already-higher target floor"
        );

        let unrelated = directory_object(
            "/users/migration-higher-target-floor-unrelated",
            coordinator.clone(),
        );
        target
            .put_object_and_version(&unrelated, &object_version_record(&unrelated, None))
            .expect("unrelated target object");
        target
            .put_intent(&terminal_intent(&unrelated, coordinator, 10))
            .expect("moved-object floor must not poison an unrelated intent");
    }
}
