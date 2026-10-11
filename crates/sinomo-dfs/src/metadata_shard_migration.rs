//! Cross-shard metadata migration helpers.
//!
//! The sharded backend owns routing decisions, while this module owns the
//! mechanics of copying an object's durable metadata family between shard
//! backends and leaving an eviction tombstone in the source shard.

use crate::DfsVersion;
use crate::advisory_lock::DfsAdvisoryLockStateRecord;
use crate::metadata_api::DfsMetadataBackend;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_pages::collect_metadata_pages;
use crate::metadata_receipt::receipt_for_replica_from_backend;
use crate::metadata_records::{
    DfsCrossShardPinTransferRecord, DfsCrossShardSourceFamily, DfsMetadataShardId,
    DfsMetadataVersionPinRecord, DfsNamespaceTransaction, DfsObjectVersionRecord,
    DfsReplicaReceiptRecord, DfsWriteIntentReceipt, DfsWriteIntentReceiptRetentionState,
    DfsWriteIntentRecord,
};
use crate::{DfsFileSystem, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsTombstoneProvenance};
use std::collections::BTreeMap;
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
    pub(crate) content_reference_outcomes: Vec<crate::DfsNamespaceTransactionReceipt>,
    pub(crate) content_reference_outcome_retention:
        Option<crate::DfsContentReferenceOutcomeRetention>,
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
            content_reference_outcomes: bundle.content_reference_outcomes,
            content_reference_outcome_retention: bundle.content_reference_outcome_retention,
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
    pub(crate) content_reference_outcomes: Vec<crate::DfsNamespaceTransactionReceipt>,
    pub(crate) content_reference_outcome_retention:
        Option<crate::DfsContentReferenceOutcomeRetention>,
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
        content_reference_outcomes: family.content_reference_outcomes,
        content_reference_outcome_retention: family.content_reference_outcome_retention,
        advisory_lock_state: family.advisory_lock_state,
    })
}

/// Collect the durable metadata records attached to one object.
///
/// When `version_history_limit` is set, the newest version records plus every
/// durably pinned version are retained, and receipts are collected for that
/// bounded/protected version set. Full intents, compact intent receipts, and
/// exact outcomes remain bounded by their current retention receipts
/// independent of visible history.
pub(crate) fn collect_object_metadata_family<B: DfsMetadataBackend + ?Sized>(
    source: &B,
    object_id: &DfsObjectId,
    page_size: usize,
    version_history_limit: Option<usize>,
) -> Result<DfsObjectMetadataFamily> {
    let object =
        source
            .get_object(object_id)?
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            })?;
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
        let mut high_water_versions = BTreeMap::<String, (u64, DfsVersion)>::new();
        for version in &versions {
            for (namespace, state) in &version.retention {
                if state.order == 0 {
                    continue;
                }
                let entry = high_water_versions
                    .entry(namespace.clone())
                    .or_insert((0, version.version.clone()));
                if state.order > entry.0 {
                    *entry = (state.order, version.version.clone());
                }
            }
        }
        versions.sort_by_key(|record| std::cmp::Reverse(record.created_at));
        let retained_versions = versions
            .iter()
            .take(limit)
            .map(|record| record.version.clone())
            .chain(version_pins.iter().map(|pin| pin.version.clone()))
            .chain(
                high_water_versions
                    .into_values()
                    .map(|(_, version)| version),
            )
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
    let content_reference_outcomes = collect_metadata_pages(|cursor| {
        let page = source.content_reference_outcomes_for_object_page(
            &object.file_system_id,
            object_id,
            cursor.map(String::as_str),
            page_size.min(128),
        )?;
        Ok((page.outcomes, page.next_cursor))
    })?;
    let content_reference_outcome_retention =
        source.content_reference_outcome_retention(&object.file_system_id, object_id)?;
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
        content_reference_outcomes,
        content_reference_outcome_retention,
        advisory_lock_state,
    })
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
