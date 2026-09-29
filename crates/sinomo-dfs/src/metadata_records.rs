//! Durable DFS metadata record and page schemas.
//!
//! These types are the wire/storage-facing metadata API. Backend modules own
//! persistence and coordination; this module owns the data shapes that callers
//! see through the crate-root re-exports.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use time::OffsetDateTime;

use crate::advisory_lock::DfsAdvisoryLockStateRecord;
use crate::metadata_api::DfsMetadataBackend;
use crate::metadata_keys::{normalize_node_locations, object_inode_id};
use crate::metadata_prepare_codec::{
    DfsNamespacePrepareCodecError, DfsPackedNamespacePrepareV1, DfsPackedNamespacePrepareV2,
    validate_namespace_prepare_file_system_scope,
};
use crate::{
    DfsConflictReadView, DfsError, DfsFileSystem, DfsFileSystemId, DfsNamespaceObjectPrecondition,
    DfsNamespaceOperation, DfsNamespaceResult, DfsObjectId, DfsObjectKind, DfsObjectMetadata,
    DfsPosixCoordinationResult, DfsReplicationPolicy, DfsVersion, normalize_current_locations,
};

#[cfg(test)]
std::thread_local! {
    static NAMESPACE_PREPARE_PAYLOAD_VALIDATION_PASSES: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

#[cfg(test)]
pub(crate) fn reset_namespace_prepare_payload_validation_passes() {
    NAMESPACE_PREPARE_PAYLOAD_VALIDATION_PASSES.with(|passes| passes.set(0));
}

#[cfg(test)]
pub(crate) fn namespace_prepare_payload_validation_passes() -> usize {
    NAMESPACE_PREPARE_PAYLOAD_VALIDATION_PASSES.with(std::cell::Cell::get)
}

#[cfg(test)]
fn record_namespace_prepare_payload_validation_pass() {
    NAMESPACE_PREPARE_PAYLOAD_VALIDATION_PASSES.with(|passes| {
        passes.set(passes.get().saturating_add(1));
    });
}

pub use sinomo_api::dfs::metadata_records::DfsContentDurability;
pub use sinomo_api::dfs::metadata_records::DfsMetadataShardId;

pub use sinomo_api::dfs::metadata_records::DfsMetadataShardStatus;

pub use sinomo_api::dfs::metadata_records::DfsMetadataReplicaLagStatus;

pub use sinomo_api::dfs::metadata_records::DfsMetadataReplayCursorStatus;

/// Replica-local durable floor established before acknowledging an exact
/// metadata prefix. Snapshot repair may not move the shard below this sequence
/// or install a different prefix at the same sequence.
///
/// A materialized checkpoint probe records `state_fingerprint`; ordinary
/// durable journal replication records `durable_prefix_proof` without forcing
/// rebuildable table expansion onto the ACK path. A later checkpoint probe may
/// enrich the same-sequence floor with its semantic fingerprint.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataPrefixAckFloor {
    pub shard_id: DfsMetadataShardId,
    pub journal_sequence: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub state_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durable_prefix_proof: Option<DfsMetadataPrefixProof>,
}

/// Replica-local durable floor established before acknowledging one exact
/// metadata authority operation.
///
/// Unlike an exact prefix checkpoint, this floor deliberately carries no
/// whole-shard fingerprint: the receiver has already re-read and verified the
/// exact hidden operation named by the probe. Snapshot repair must therefore
/// advance strictly past this sequence before it may replace local state.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataAuthorityAckFloor {
    pub shard_id: DfsMetadataShardId,
    pub journal_sequence: u64,
    /// Exact durable journal prefix acknowledged at this floor. Legacy and
    /// fully-materialized acknowledgements omit it; the materialized table
    /// prefix remains their authoritative proof.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durable_prefix_proof: Option<DfsMetadataPrefixProof>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardMigrationReport {
    pub file_system_id: DfsFileSystemId,
    pub scanned_objects: usize,
    pub indexed_objects: usize,
    pub migrated_objects: usize,
    pub skipped_stale_sources: usize,
    pub reached_limit: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct DfsInodeId(pub String);

impl DfsInodeId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct DfsDirFragmentId(pub String);

impl DfsDirFragmentId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

/// Materialized namespace entry for a path in a DFS filesystem.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPathRecord {
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    pub object_id: DfsObjectId,
    pub inode_id: DfsInodeId,
    pub parent_inode_id: DfsInodeId,
    pub dirfrag_id: DfsDirFragmentId,
    pub name: String,
    pub name_hash: u64,
    pub kind: DfsObjectKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsInodeRecord {
    pub inode_id: DfsInodeId,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub kind: DfsObjectKind,
    pub owner_node_id: NodeId,
    pub policy: sinomo_core::DfsObjectPolicy,
    pub visible_version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub current_locations: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified_by_event_id: Option<EventId>,
}

impl From<&DfsObjectMetadata> for DfsInodeRecord {
    fn from(object: &DfsObjectMetadata) -> Self {
        let mut current_locations = object.current_locations.clone();
        normalize_node_locations(&mut current_locations);
        Self {
            inode_id: object_inode_id(&object.object_id),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            kind: object.kind.clone(),
            owner_node_id: object.owner_node_id.clone(),
            policy: object.policy.clone(),
            visible_version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            current_locations,
            created_by_event_id: object.created_by_event_id.clone(),
            last_modified_by_event_id: object.last_modified_by_event_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsDirFragmentRecord {
    pub file_system_id: DfsFileSystemId,
    pub dir_inode_id: DfsInodeId,
    pub dirfrag_id: DfsDirFragmentId,
    pub hash_start_inclusive: u64,
    pub hash_end_exclusive: u64,
    pub entry_count: u64,
    pub split_after_count: u64,
}

pub use sinomo_api::dfs::metadata_records::DfsObjectVersionRecord;

/// Durable ownership of one immutable metadata version by an independent
/// checkpoint or retention consumer.
///
/// `pin_id` is the consumer-scoped identity: different consumers may pin the
/// same version without one consumer's removal releasing another's root.
/// DFS path pins and content-placement pins are separate contracts and never
/// create this record implicitly.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
pub struct DfsMetadataVersionPinRecord {
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub pin_id: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

/// Durable target-shard provenance for pins copied during one deterministic
/// cross-shard object handoff.
///
/// A retry may observe a newer source pin set after destination publication.
/// Retaining the exact previously copied records lets it remove only stale
/// transfer copies while preserving pins created independently on the target.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsCrossShardPinTransferRecord {
    pub transfer_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub source_path: String,
    pub target_path: String,
    pub target_version: DfsVersion,
    /// Complete source pin set acknowledged by the destination. Every key is
    /// fenced against generic mutation until finalization.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_pins: Vec<DfsMetadataVersionPinRecord>,
    /// Subset of `source_pins` inserted by this transfer and therefore safe to
    /// remove when a retry observes that the source released the pin.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub copied_pins: Vec<DfsMetadataVersionPinRecord>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

/// Exact source-shard generation and pin snapshot used to fence a
/// cross-primary handoff.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsCrossShardVersionPinFence {
    pub source_object: DfsObjectMetadata,
    pub source_journal_sequence: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub version_pins: Vec<DfsMetadataVersionPinRecord>,
}

/// Complete physical metadata family captured from the shard selected by a
/// cross-shard move's old path.
///
/// This deliberately does not follow the object-to-shard authority index:
/// after destination publication that index names the target, while crash
/// recovery still needs the exact source family to reconcile the target and
/// finish the fenced source eviction.
#[derive(Clone, Debug)]
pub struct DfsCrossShardSourceFamily {
    pub source_object: DfsObjectMetadata,
    pub source_journal_sequence: u64,
    pub versions: Vec<DfsObjectVersionRecord>,
    pub version_pins: Vec<DfsMetadataVersionPinRecord>,
    pub pin_transfers: Vec<DfsCrossShardPinTransferRecord>,
    pub receipts: Vec<DfsReplicaReceiptRecord>,
    pub intents: Vec<DfsWriteIntentRecord>,
    pub intent_receipts: Vec<DfsWriteIntentReceipt>,
    pub intent_receipt_retention: DfsWriteIntentReceiptRetentionState,
    pub conflicts: Vec<DfsConflictRecord>,
    pub advisory_lock_state: Option<DfsAdvisoryLockStateRecord>,
}

pub use sinomo_api::dfs::metadata_records::DfsReplicaReceiptRecord;

pub use sinomo_api::dfs::metadata_records::DfsWriteIntentRecord;

/// Compact terminal write identity retained after recovery no longer needs the
/// full object body or mutable diagnostics from a write intent.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteIntentReceipt {
    #[serde(
        default,
        skip_serializing_if = "crate::DfsWritePublication::is_current"
    )]
    pub publication: crate::DfsWritePublication,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_version: Option<DfsVersion>,
    pub target_version: DfsVersion,
    pub coordinator_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_order_token: Option<String>,
    #[serde(default)]
    pub owner_epoch: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub desired_replica_node_ids: Vec<NodeId>,
    pub policy: DfsReplicationPolicy,
    pub status: DfsWriteIntentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default)]
    pub touch_parent_on_create: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_predecessor_object_id: Option<DfsObjectId>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub terminal_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub retained_at: OffsetDateTime,
}

impl DfsWriteIntentReceipt {
    pub(crate) fn from_intent(
        intent: &DfsWriteIntentRecord,
        retained_at: OffsetDateTime,
    ) -> Option<Self> {
        if intent.publication.is_retained_repair()
            || matches!(
                intent.status,
                DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
            )
        {
            return None;
        }
        Some(Self {
            intent_id: intent.intent_id.clone(),
            publication: intent.publication.clone(),
            file_system_id: intent.file_system_id.clone(),
            object_id: intent.object_id.clone(),
            base_version: intent.base_version.clone(),
            target_version: intent.target_version.clone(),
            coordinator_node_id: intent.coordinator_node_id.clone(),
            operation_id: intent.operation_id.clone(),
            operation_order_token: intent.operation_order_token.clone(),
            owner_epoch: intent.owner_epoch,
            desired_replica_node_ids: intent.desired_replica_node_ids.clone(),
            policy: intent.policy.clone(),
            status: intent.status.clone(),
            content_hash: intent.content_hash.clone(),
            touch_parent_on_create: intent.touch_parent_on_create,
            create_predecessor_object_id: intent.create_predecessor_object_id.clone(),
            created_at: intent.created_at,
            terminal_at: intent.updated_at.unwrap_or(intent.created_at),
            retained_at,
        })
    }

    pub(crate) fn to_intent_record(&self) -> DfsWriteIntentRecord {
        DfsWriteIntentRecord {
            publication: self.publication.clone(),
            intent_id: self.intent_id.clone(),
            file_system_id: self.file_system_id.clone(),
            object_id: self.object_id.clone(),
            base_version: self.base_version.clone(),
            target_version: self.target_version.clone(),
            coordinator_node_id: self.coordinator_node_id.clone(),
            operation_id: self.operation_id.clone(),
            operation_order_token: self.operation_order_token.clone(),
            owner_epoch: self.owner_epoch,
            desired_replica_node_ids: self.desired_replica_node_ids.clone(),
            policy: self.policy.clone(),
            status: self.status.clone(),
            content_hash: self.content_hash.clone(),
            object_metadata: None,
            touch_parent_on_create: self.touch_parent_on_create,
            create_predecessor_object_id: self.create_predecessor_object_id.clone(),
            created_at: self.created_at,
            updated_at: Some(self.terminal_at),
        }
    }

    pub(crate) fn matches_intent(&self, intent: &DfsWriteIntentRecord) -> bool {
        Self::from_intent(intent, self.retained_at).as_ref() == Some(self)
    }
}

/// Durable high-water marks for compact intent receipts evicted by metadata
/// history GC. Generated intent IDs end in a monotonically increasing counter;
/// a retry at or below the object/coordinator-specific floor is expired rather
/// than admitted as a fresh write after its exact receipt has been collected.
///
/// The object scope is essential during shard migration: a floor learned for
/// one moved object must not reject an unrelated lower-counter intent which was
/// legitimately owned by the destination shard.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteIntentReceiptRetentionState {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub max_evicted_counter_by_object_and_coordinator: BTreeMap<DfsObjectId, BTreeMap<NodeId, u64>>,
    /// High-water marks for current caller-owned stable operation ids. ULIDs
    /// retain lexical time order, so one object/coordinator floor replaces an
    /// unbounded set of exact derived intent ids.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub max_evicted_operation_order_token_by_object_and_coordinator:
        BTreeMap<DfsObjectId, BTreeMap<NodeId, String>>,
    /// Exact opaque intent ids evicted by current writers.
    ///
    /// Rolling-upgrade compatibility for exact opaque ids retained by older
    /// nodes. New nodes never add to this map. Conservative merges promote an
    /// oversized legacy set to the per-object opaque marker below.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub evicted_opaque_intent_ids_by_object: BTreeMap<DfsObjectId, BTreeSet<String>>,
    /// Conservative rolling-upgrade marker written by older nodes. Once set,
    /// the exact opaque ids which were evicted are unknowable, so all opaque
    /// retries for that object remain reconciliation-required.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub opaque_intent_history_evicted_object_ids: BTreeSet<DfsObjectId>,
}

impl DfsWriteIntentReceiptRetentionState {
    const LEGACY_EXACT_OPAQUE_IDS_PER_OBJECT_MAX: usize = 128;

    pub(crate) fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    pub(crate) fn eviction_delta(receipt: &DfsWriteIntentReceipt) -> Self {
        let mut state = Self::default();
        if let Some(token) = receipt.operation_order_token.as_ref() {
            state
                .max_evicted_operation_order_token_by_object_and_coordinator
                .entry(receipt.object_id.clone())
                .or_default()
                .insert(receipt.coordinator_node_id.clone(), token.clone());
        } else if let Some(counter) = write_intent_id_counter(&receipt.intent_id) {
            state
                .max_evicted_counter_by_object_and_coordinator
                .entry(receipt.object_id.clone())
                .or_default()
                .insert(receipt.coordinator_node_id.clone(), counter);
        } else {
            state
                .opaque_intent_history_evicted_object_ids
                .insert(receipt.object_id.clone());
        }
        state
    }

    pub(crate) fn merge_conservative(&mut self, other: Self) {
        for (object_id, floors) in other.max_evicted_counter_by_object_and_coordinator {
            let current_floors = self
                .max_evicted_counter_by_object_and_coordinator
                .entry(object_id)
                .or_default();
            for (node_id, floor) in floors {
                let current = current_floors.entry(node_id).or_insert(0);
                *current = (*current).max(floor);
            }
        }
        for (object_id, floors) in other.max_evicted_operation_order_token_by_object_and_coordinator
        {
            let current_floors = self
                .max_evicted_operation_order_token_by_object_and_coordinator
                .entry(object_id)
                .or_default();
            for (node_id, floor) in floors {
                let current = current_floors.entry(node_id).or_default();
                if floor > *current {
                    *current = floor;
                }
            }
        }
        for (object_id, intent_ids) in other.evicted_opaque_intent_ids_by_object {
            if self
                .opaque_intent_history_evicted_object_ids
                .contains(&object_id)
            {
                continue;
            }
            let retained = self
                .evicted_opaque_intent_ids_by_object
                .entry(object_id.clone())
                .or_default();
            retained.extend(intent_ids);
            if retained.len() > Self::LEGACY_EXACT_OPAQUE_IDS_PER_OBJECT_MAX {
                self.evicted_opaque_intent_ids_by_object.remove(&object_id);
                self.opaque_intent_history_evicted_object_ids
                    .insert(object_id);
            }
        }
        self.opaque_intent_history_evicted_object_ids
            .extend(other.opaque_intent_history_evicted_object_ids);
        for object_id in &self.opaque_intent_history_evicted_object_ids {
            self.evicted_opaque_intent_ids_by_object.remove(object_id);
        }
    }

    pub(crate) fn history_expired(&self, intent: &DfsWriteIntentRecord) -> bool {
        if let Some(token) = intent.operation_order_token.as_ref() {
            return self
                .max_evicted_operation_order_token_by_object_and_coordinator
                .get(&intent.object_id)
                .and_then(|floors| floors.get(&intent.coordinator_node_id))
                .is_some_and(|floor| token.as_str() <= floor.as_str());
        }
        match write_intent_id_counter(&intent.intent_id) {
            Some(counter) => self
                .max_evicted_counter_by_object_and_coordinator
                .get(&intent.object_id)
                .and_then(|floors| floors.get(&intent.coordinator_node_id))
                .is_some_and(|floor| counter <= *floor),
            None => {
                self.opaque_intent_history_evicted_object_ids
                    .contains(&intent.object_id)
                    || self
                        .evicted_opaque_intent_ids_by_object
                        .get(&intent.object_id)
                        .is_some_and(|intent_ids| intent_ids.contains(&intent.intent_id))
            }
        }
    }

    pub(crate) fn operation_history_expired(
        &self,
        object_id: &DfsObjectId,
        coordinator_node_id: &NodeId,
        intent_id: &str,
        operation_order_token: Option<&str>,
    ) -> bool {
        if let Some(token) = operation_order_token {
            return self
                .max_evicted_operation_order_token_by_object_and_coordinator
                .get(object_id)
                .and_then(|floors| floors.get(coordinator_node_id))
                .is_some_and(|floor| token <= floor.as_str());
        }
        self.opaque_intent_history_evicted_object_ids
            .contains(object_id)
            || self
                .evicted_opaque_intent_ids_by_object
                .get(object_id)
                .is_some_and(|intent_ids| intent_ids.contains(intent_id))
    }

    pub(crate) fn for_object(&self, object_id: &DfsObjectId) -> Self {
        let mut state = Self::default();
        if let Some(floors) = self
            .max_evicted_counter_by_object_and_coordinator
            .get(object_id)
        {
            state
                .max_evicted_counter_by_object_and_coordinator
                .insert(object_id.clone(), floors.clone());
        }
        if let Some(floors) = self
            .max_evicted_operation_order_token_by_object_and_coordinator
            .get(object_id)
        {
            state
                .max_evicted_operation_order_token_by_object_and_coordinator
                .insert(object_id.clone(), floors.clone());
        }
        if self
            .opaque_intent_history_evicted_object_ids
            .contains(object_id)
        {
            state
                .opaque_intent_history_evicted_object_ids
                .insert(object_id.clone());
        }
        if let Some(intent_ids) = self.evicted_opaque_intent_ids_by_object.get(object_id) {
            state
                .evicted_opaque_intent_ids_by_object
                .insert(object_id.clone(), intent_ids.clone());
        }
        state
    }

    pub(crate) fn max_evicted_counter(&self) -> u64 {
        self.max_evicted_counter_by_object_and_coordinator
            .values()
            .flat_map(|floors| floors.values().copied())
            .max()
            .unwrap_or(0)
    }
}

/// Exact write-intent history attached to one object during an authoritative
/// shard move.
///
/// Compact receipts remain distinct from full recovery records so migration
/// cannot accidentally expand a bounded receipt back into a full intent body.
/// The retention state is filtered to this object so a destination inherits
/// the source's exact high-water marks without fencing unrelated histories.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsObjectWriteIntentHistory {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub intents: Vec<DfsWriteIntentRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub receipts: Vec<DfsWriteIntentReceipt>,
    #[serde(
        default,
        skip_serializing_if = "DfsWriteIntentReceiptRetentionState::is_empty"
    )]
    pub retention: DfsWriteIntentReceiptRetentionState,
}

fn write_intent_id_counter(intent_id: &str) -> Option<u64> {
    intent_id.rsplit('_').next()?.parse().ok()
}

pub use sinomo_api::dfs::metadata_records::DfsWriteIntentStatus;

pub use sinomo_api::dfs::metadata_records::DfsConflictRecord;

pub use sinomo_api::dfs::metadata_records::DfsConflictStatus;

/// Append-only metadata mutation as stored in a shard journal.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataJournalEntry {
    pub shard_id: DfsMetadataShardId,
    pub sequence: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub mutation: DfsMetadataMutation,
}

/// Versioned, content-addressed identity of one exact contiguous metadata
/// journal prefix.
///
/// This is deliberately DFS-owned state. It never enters cluster KV: the
/// materialized metadata table log carries the checkpoint alongside the table
/// mutations that made the prefix durable.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataPrefixProof {
    pub version: u16,
    pub journal_sequence: u64,
    pub digest: String,
}

pub(crate) const DFS_METADATA_PREFIX_PROOF_VERSION: u16 = 1;
pub(crate) const DFS_METADATA_PREFIX_PROOF_EXACT_FRAME_VERSION: u16 = 2;
const DFS_METADATA_PREFIX_PROOF_DIGEST_PREFIX: &str = "dfs-metadata-prefix-v1:blake3:";
const DFS_METADATA_PREFIX_PROOF_EXACT_FRAME_DIGEST_PREFIX: &str = "dfs-metadata-prefix-v2:blake3:";

impl DfsMetadataPrefixProof {
    pub(crate) fn from_digest(journal_sequence: u64, digest: impl Into<String>) -> Option<Self> {
        let digest = digest.into();
        [
            DFS_METADATA_PREFIX_PROOF_VERSION,
            DFS_METADATA_PREFIX_PROOF_EXACT_FRAME_VERSION,
        ]
        .into_iter()
        .find_map(|version| {
            let proof = Self {
                version,
                journal_sequence,
                digest: digest.clone(),
            };
            proof.is_well_formed().then_some(proof)
        })
    }

    pub(crate) fn is_well_formed(&self) -> bool {
        let digest = match self.version {
            DFS_METADATA_PREFIX_PROOF_VERSION => self
                .digest
                .strip_prefix(DFS_METADATA_PREFIX_PROOF_DIGEST_PREFIX),
            DFS_METADATA_PREFIX_PROOF_EXACT_FRAME_VERSION => self
                .digest
                .strip_prefix(DFS_METADATA_PREFIX_PROOF_EXACT_FRAME_DIGEST_PREFIX),
            _ => None,
        };
        digest.is_some_and(|digest| {
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    }
}

fn metadata_prefix_digest(parts: &[&[u8]]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo-dfs-metadata-prefix-proof\0v1\0");
    for part in parts {
        hasher.update(&(part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    format!(
        "{DFS_METADATA_PREFIX_PROOF_DIGEST_PREFIX}{}",
        hasher.finalize().to_hex()
    )
}

fn metadata_exact_frame_prefix_digest(parts: &[&[u8]]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo-dfs-metadata-prefix-proof\0v2\0");
    for part in parts {
        hasher.update(&(part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    format!(
        "{DFS_METADATA_PREFIX_PROOF_EXACT_FRAME_DIGEST_PREFIX}{}",
        hasher.finalize().to_hex()
    )
}

/// Establish an exact chain checkpoint from a complete semantic shard image.
/// This is the one-time compatibility path for metadata created before prefix
/// proofs existed; normal writes extend the checkpoint with O(1) work.
pub(crate) fn dfs_metadata_prefix_proof_seed(
    shard_id: &DfsMetadataShardId,
    journal_sequence: u64,
    semantic_state_fingerprint: &str,
) -> DfsMetadataPrefixProof {
    let sequence = journal_sequence.to_be_bytes();
    DfsMetadataPrefixProof {
        version: DFS_METADATA_PREFIX_PROOF_VERSION,
        journal_sequence,
        digest: metadata_prefix_digest(&[
            b"semantic-checkpoint",
            shard_id.0.as_bytes(),
            &sequence,
            semantic_state_fingerprint.as_bytes(),
        ]),
    }
}

/// Extend a legacy V1 prefix with one payload-complete journal entry.
///
/// New writes use the exact-frame V2 helper below. This encoder remains in
/// production only so a restarted authority can authenticate a durable V1
/// handoff tail before checkpointing it at the current head.
pub(crate) fn dfs_metadata_prefix_proof_after(
    previous: &DfsMetadataPrefixProof,
    entry: &DfsMetadataJournalEntry,
) -> Result<DfsMetadataPrefixProof, serde_json::Error> {
    let encoded_entry = serde_json::to_vec(entry)?;
    let sequence = entry.sequence.to_be_bytes();
    Ok(DfsMetadataPrefixProof {
        version: DFS_METADATA_PREFIX_PROOF_VERSION,
        journal_sequence: entry.sequence,
        digest: metadata_prefix_digest(&[
            b"journal-entry",
            previous.digest.as_bytes(),
            &sequence,
            &encoded_entry,
        ]),
    })
}

/// Extend an exact prefix from the immutable bytes of one encoded journal
/// frame.
///
/// This is a format-compatible successor to [`dfs_metadata_prefix_proof_after`].
/// The legacy helper deliberately continues hashing canonical JSON and
/// producing a version-1 proof. New journal code can instead encode its SOBJ
/// frame once, append those exact bytes, and pass the same bytes here. The
/// previous version and sequence are included explicitly so the first V2
/// extension can safely start from an existing V1 semantic checkpoint.
pub(crate) fn dfs_metadata_prefix_proof_v2_after_bytes(
    previous: &DfsMetadataPrefixProof,
    journal_sequence: u64,
    exact_frame_bytes: &[u8],
) -> DfsMetadataPrefixProof {
    let previous_version = previous.version.to_be_bytes();
    let previous_sequence = previous.journal_sequence.to_be_bytes();
    let journal_sequence_bytes = journal_sequence.to_be_bytes();
    DfsMetadataPrefixProof {
        version: DFS_METADATA_PREFIX_PROOF_EXACT_FRAME_VERSION,
        journal_sequence,
        digest: metadata_exact_frame_prefix_digest(&[
            b"journal-frame",
            &previous_version,
            &previous_sequence,
            previous.digest.as_bytes(),
            &journal_sequence_bytes,
            exact_frame_bytes,
        ]),
    }
}

/// Mutation payload stored in the metadata journal.
fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DfsMetadataMutation {
    PutFileSystem {
        file_system_id: DfsFileSystemId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file_system: Option<DfsFileSystem>,
    },
    PutObject {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        object: Option<DfsObjectMetadata>,
    },
    PutVersion {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version_record: Option<DfsObjectVersionRecord>,
    },
    /// One explicit capture and its first named pin are a single durable effect.
    CaptureFileVersion {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        record: Box<DfsObjectVersionRecord>,
        pin: DfsMetadataVersionPinRecord,
    },
    RemoveVersion {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
    },
    PutVersionPin {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        pin_id: String,
        pin: DfsMetadataVersionPinRecord,
    },
    RemoveVersionPin {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        pin_id: String,
    },
    ReconcileCrossShardPinTransfer {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        transfer_id: String,
        #[serde(default, skip_serializing_if = "is_false")]
        authoritative_migration: bool,
        versions: Vec<DfsObjectVersionRecord>,
        transfer: DfsCrossShardPinTransferRecord,
    },
    RemoveCrossShardPinTransfer {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        transfer_id: String,
    },
    FinalizeCrossShardSourceEviction {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        tombstone: DfsObjectMetadata,
        removed_pins: Vec<DfsMetadataVersionPinRecord>,
        removed_pin_transfers: Vec<DfsCrossShardPinTransferRecord>,
    },
    PutReceipt {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        replica_node_id: NodeId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        receipt: Option<DfsReplicaReceiptRecord>,
    },
    RemoveReceipt {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        replica_node_id: NodeId,
    },
    PutIntent {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        intent_id: String,
        status: DfsWriteIntentStatus,
        /// Authoritative shard migration may install a durable source record
        /// below a destination's conservative eviction floor. Ordinary retries
        /// must leave this false and remain fenced by that floor.
        #[serde(default, skip_serializing_if = "is_false")]
        authoritative_migration: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        intent: Option<DfsWriteIntentRecord>,
    },
    /// Atomic full-body to terminal-receipt transition.
    PutIntentReceipt {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        intent_id: String,
        receipt: DfsWriteIntentReceipt,
    },
    /// Collect internal maintenance ownership without creating user retry evidence.
    RemoveRetainedRepairOwner {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        intent_id: String,
    },
    RemoveIntentReceipt {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        intent_id: String,
        retention: DfsWriteIntentReceiptRetentionState,
    },
    /// Merge a source shard's conservative intent-receipt eviction floors into
    /// the destination while moving an object's authoritative metadata family.
    MergeIntentReceiptRetention {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        retention: DfsWriteIntentReceiptRetentionState,
    },
    CommitVisibleVersion {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        previous_version: DfsVersion,
        committed_version: DfsVersion,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operation_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        committed_object: Option<DfsObjectMetadata>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        visible_version: Option<DfsObjectVersionRecord>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        required_replica_node_ids: Vec<NodeId>,
    },
    RemoveVisibleCommit {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
    },
    PutConflict {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        conflict_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conflict: Option<DfsConflictRecord>,
    },
    PutAdvisoryLockState {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        state: Option<DfsAdvisoryLockStateRecord>,
    },
    ApplyNamespaceTransaction {
        file_system_id: DfsFileSystemId,
        transaction_id: String,
        object_count: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operation_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        transaction: Option<DfsNamespaceTransaction>,
    },
    /// Fixed-size idempotency evidence for a namespace transaction whose
    /// materialized objects are already captured by an authoritative table
    /// snapshot.
    PutNamespaceTransactionReceipt {
        file_system_id: DfsFileSystemId,
        transaction_id: String,
        receipt: DfsNamespaceTransactionReceipt,
    },
    RemoveNamespaceTransactionReceipt {
        file_system_id: DfsFileSystemId,
        transaction_id: String,
        retention: DfsNamespaceReceiptRetentionState,
    },
    PutNamespacePrepare {
        file_system_id: DfsFileSystemId,
        operation_id: String,
        status: DfsNamespacePrepareStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prepare: Option<Box<DfsNamespacePrepareRecord>>,
        /// Packed authoritative body selected only by a capability-fenced
        /// shard configuration. Exactly one prepare representation may be
        /// present. Older readers ignore this additive field, observe the
        /// missing canonical body, and fail replay closed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        packed_prepare_v1: Option<Box<DfsPackedNamespacePrepareV1>>,
        /// Indexed, column-oriented body selected only after every assigned
        /// metadata member advertises packed-v2 decoder support.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        packed_prepare_v2: Option<Box<DfsPackedNamespacePrepareV2>>,
    },
    /// Fenced commit of the complete Prepared body already durable earlier in
    /// this shard's journal. Replayers must resolve the referenced prepare and
    /// reject a missing, mismatched, or aborted dependency.
    CommitNamespacePrepare {
        file_system_id: DfsFileSystemId,
        operation_id: String,
        operation_digest: String,
        owner_epoch: u64,
        /// Sorted, deduplicated object identities needed by replica service
        /// caches and sharded placement indexes after the referenced
        /// transaction becomes visible. This remains much smaller than the
        /// prepared transaction body while making post-apply reconciliation
        /// self-contained.
        object_ids: Vec<DfsObjectId>,
    },
    /// Compact terminal prepare evidence. Prepared records never use this
    /// mutation because recovery still needs their complete transaction.
    PutNamespacePrepareReceipt {
        file_system_id: DfsFileSystemId,
        operation_id: String,
        receipt: DfsNamespacePrepareReceipt,
    },
    RemoveNamespacePrepareReceipt {
        file_system_id: DfsFileSystemId,
        operation_id: String,
        retention: DfsNamespaceReceiptRetentionState,
    },
    PutNamespaceJob {
        file_system_id: DfsFileSystemId,
        job_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        job: Option<DfsNamespaceJobRecord>,
    },
    /// Compact terminal evidence retained after a namespace job's resumable
    /// frames and progress bookkeeping have been collected.
    PutNamespaceJobReceipt {
        file_system_id: DfsFileSystemId,
        job_id: String,
        receipt: DfsNamespaceJobReceipt,
    },
    RemoveNamespaceJobReceipt {
        file_system_id: DfsFileSystemId,
        job_id: String,
        retention: DfsNamespaceReceiptRetentionState,
    },
    PutNamespaceRequest {
        file_system_id: DfsFileSystemId,
        requester_node_id: NodeId,
        request_id: String,
        request: Box<DfsNamespaceRequestRecord>,
    },
    /// Compact canonical response retained after a completed request no longer
    /// needs its operation or recovery diagnostics.
    PutNamespaceRequestReceipt {
        file_system_id: DfsFileSystemId,
        requester_node_id: NodeId,
        request_id: String,
        receipt: Box<DfsNamespaceRequestReceipt>,
    },
    RemoveNamespaceRequestReceipt {
        file_system_id: DfsFileSystemId,
        requester_node_id: NodeId,
        request_id: String,
        retention: DfsNamespaceReceiptRetentionState,
    },
}

/// Borrowed view of the one authoritative namespace-prepare representation in
/// a journal mutation.
///
/// Construction validates the selected representation exactly once. The
/// outer fields remain part of the view because packed payloads intentionally
/// omit them; later scope and identity checks can therefore stay allocation
/// free without rescanning the complete packed body.
#[derive(Clone, Copy, Debug)]
pub(crate) enum DfsNamespacePreparePayloadRef<'a> {
    Canonical {
        file_system_id: &'a DfsFileSystemId,
        operation_id: &'a str,
        status: &'a DfsNamespacePrepareStatus,
        prepare: &'a DfsNamespacePrepareRecord,
    },
    PackedV1 {
        file_system_id: &'a DfsFileSystemId,
        operation_id: &'a str,
        status: &'a DfsNamespacePrepareStatus,
        prepare: &'a DfsPackedNamespacePrepareV1,
    },
    PackedV2 {
        file_system_id: &'a DfsFileSystemId,
        operation_id: &'a str,
        status: &'a DfsNamespacePrepareStatus,
        prepare: &'a DfsPackedNamespacePrepareV2,
    },
}

impl<'a> DfsNamespacePreparePayloadRef<'a> {
    #[cfg(test)]
    pub(crate) fn into_canonical(
        self,
    ) -> Result<Cow<'a, DfsNamespacePrepareRecord>, DfsNamespacePrepareCodecError> {
        match self {
            Self::Canonical { prepare, .. } => Ok(Cow::Borrowed(prepare)),
            Self::PackedV1 {
                file_system_id,
                operation_id,
                status,
                prepare,
            } => Ok(Cow::Owned(prepare.unpack_validated(
                file_system_id,
                operation_id,
                status.clone(),
            ))),
            Self::PackedV2 {
                file_system_id,
                operation_id,
                status,
                prepare,
            } => Ok(Cow::Owned(prepare.unpack_validated(
                file_system_id,
                operation_id,
                status.clone(),
            ))),
        }
    }

    /// Materialize one canonical, normalized table record.
    ///
    /// Packed payloads already require an owned canonical allocation at the
    /// table boundary. Their validated unpack path materializes and normalizes
    /// that allocation once, without rescanning it here.
    pub(crate) fn into_normalized_canonical(
        self,
    ) -> Result<Cow<'a, DfsNamespacePrepareRecord>, DfsNamespacePrepareCodecError> {
        match self {
            Self::Canonical { prepare, .. } => {
                Ok(Cow::Owned(normalized_namespace_prepare(prepare)))
            }
            Self::PackedV1 {
                file_system_id,
                operation_id,
                status,
                prepare,
            } => Ok(Cow::Owned(prepare.unpack_validated(
                file_system_id,
                operation_id,
                status.clone(),
            ))),
            Self::PackedV2 {
                file_system_id,
                operation_id,
                status,
                prepare,
            } => Ok(Cow::Owned(prepare.unpack_validated(
                file_system_id,
                operation_id,
                status.clone(),
            ))),
        }
    }

    pub(crate) fn validate_file_system_scope(
        &self,
        expected: &DfsFileSystemId,
    ) -> Result<(), DfsNamespacePrepareCodecError> {
        let file_system_id = match self {
            Self::Canonical { file_system_id, .. }
            | Self::PackedV1 { file_system_id, .. }
            | Self::PackedV2 { file_system_id, .. } => file_system_id,
        };
        require_prepare_outer_file_system(file_system_id, expected)
    }

    pub(crate) fn matches_prepare_identity(&self, expected: &DfsNamespacePrepareRecord) -> bool {
        match self {
            Self::Canonical {
                file_system_id,
                operation_id,
                status,
                prepare,
            } => {
                *file_system_id == &expected.file_system_id
                    && *operation_id == expected.operation_id
                    && *status == &expected.status
                    && prepare.file_system_id == expected.file_system_id
                    && prepare.operation_id == expected.operation_id
                    && prepare.status == expected.status
                    && prepare.shard_id == expected.shard_id
                    && prepare.owner_epoch == expected.owner_epoch
                    && prepare.operation_digest == expected.operation_digest
            }
            Self::PackedV1 {
                file_system_id,
                operation_id,
                status,
                prepare,
            } => {
                *file_system_id == &expected.file_system_id
                    && *operation_id == expected.operation_id
                    && *status == &expected.status
                    && prepare.matches_prepare_identity(
                        &expected.shard_id,
                        expected.owner_epoch,
                        &expected.operation_digest,
                    )
            }
            Self::PackedV2 {
                file_system_id,
                operation_id,
                status,
                prepare,
            } => {
                *file_system_id == &expected.file_system_id
                    && *operation_id == expected.operation_id
                    && *status == &expected.status
                    && prepare.matches_prepare_identity(
                        &expected.shard_id,
                        expected.owner_epoch,
                        &expected.operation_digest,
                    )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn matches_authority(
        &self,
        file_system_id: &DfsFileSystemId,
        operation_id: &str,
        status: &DfsNamespacePrepareStatus,
        shard_id: &DfsMetadataShardId,
        owner_epoch: u64,
        operation_digest: &str,
    ) -> bool {
        match self {
            Self::Canonical {
                file_system_id: outer_file_system_id,
                operation_id: outer_operation_id,
                status: outer_status,
                prepare,
            } => {
                *outer_file_system_id == file_system_id
                    && *outer_operation_id == operation_id
                    && *outer_status == status
                    && prepare.file_system_id == *file_system_id
                    && prepare.operation_id == operation_id
                    && prepare.status == *status
                    && prepare.shard_id == *shard_id
                    && prepare.owner_epoch == owner_epoch
                    && prepare.operation_digest == operation_digest
            }
            Self::PackedV1 {
                file_system_id: outer_file_system_id,
                operation_id: outer_operation_id,
                status: outer_status,
                prepare,
            } => {
                *outer_file_system_id == file_system_id
                    && *outer_operation_id == operation_id
                    && *outer_status == status
                    && prepare.matches_prepare_identity(shard_id, owner_epoch, operation_digest)
            }
            Self::PackedV2 {
                file_system_id: outer_file_system_id,
                operation_id: outer_operation_id,
                status: outer_status,
                prepare,
            } => {
                *outer_file_system_id == file_system_id
                    && *outer_operation_id == operation_id
                    && *outer_status == status
                    && prepare.matches_prepare_identity(shard_id, owner_epoch, operation_digest)
            }
        }
    }
}

fn require_prepare_outer_file_system(
    actual: &DfsFileSystemId,
    expected: &DfsFileSystemId,
) -> Result<(), DfsNamespacePrepareCodecError> {
    if actual == expected {
        return Ok(());
    }
    Err(DfsNamespacePrepareCodecError::CrossFileSystem {
        field: "outer mutation".to_string(),
        expected: expected.to_string(),
        actual: actual.to_string(),
    })
}

impl DfsMetadataMutation {
    /// Filesystem named by the mutation routing envelope. Embedded records
    /// still require independent validation before replica ingestion.
    pub fn file_system_id(&self) -> &DfsFileSystemId {
        match self {
            DfsMetadataMutation::PutFileSystem { file_system_id, .. }
            | DfsMetadataMutation::PutObject { file_system_id, .. }
            | DfsMetadataMutation::PutVersion { file_system_id, .. }
            | DfsMetadataMutation::CaptureFileVersion { file_system_id, .. }
            | DfsMetadataMutation::RemoveVersion { file_system_id, .. }
            | DfsMetadataMutation::PutVersionPin { file_system_id, .. }
            | DfsMetadataMutation::RemoveVersionPin { file_system_id, .. }
            | DfsMetadataMutation::ReconcileCrossShardPinTransfer { file_system_id, .. }
            | DfsMetadataMutation::RemoveCrossShardPinTransfer { file_system_id, .. }
            | DfsMetadataMutation::FinalizeCrossShardSourceEviction { file_system_id, .. }
            | DfsMetadataMutation::PutReceipt { file_system_id, .. }
            | DfsMetadataMutation::RemoveReceipt { file_system_id, .. }
            | DfsMetadataMutation::PutIntent { file_system_id, .. }
            | DfsMetadataMutation::PutIntentReceipt { file_system_id, .. }
            | DfsMetadataMutation::RemoveIntentReceipt { file_system_id, .. }
            | DfsMetadataMutation::RemoveRetainedRepairOwner { file_system_id, .. }
            | DfsMetadataMutation::MergeIntentReceiptRetention { file_system_id, .. }
            | DfsMetadataMutation::CommitVisibleVersion { file_system_id, .. }
            | DfsMetadataMutation::RemoveVisibleCommit { file_system_id, .. }
            | DfsMetadataMutation::PutConflict { file_system_id, .. }
            | DfsMetadataMutation::PutAdvisoryLockState { file_system_id, .. }
            | DfsMetadataMutation::ApplyNamespaceTransaction { file_system_id, .. }
            | DfsMetadataMutation::PutNamespaceTransactionReceipt { file_system_id, .. }
            | DfsMetadataMutation::RemoveNamespaceTransactionReceipt { file_system_id, .. }
            | DfsMetadataMutation::PutNamespacePrepare { file_system_id, .. }
            | DfsMetadataMutation::CommitNamespacePrepare { file_system_id, .. }
            | DfsMetadataMutation::PutNamespacePrepareReceipt { file_system_id, .. }
            | DfsMetadataMutation::RemoveNamespacePrepareReceipt { file_system_id, .. }
            | DfsMetadataMutation::PutNamespaceJob { file_system_id, .. }
            | DfsMetadataMutation::PutNamespaceJobReceipt { file_system_id, .. }
            | DfsMetadataMutation::RemoveNamespaceJobReceipt { file_system_id, .. }
            | DfsMetadataMutation::PutNamespaceRequest { file_system_id, .. }
            | DfsMetadataMutation::PutNamespaceRequestReceipt { file_system_id, .. }
            | DfsMetadataMutation::RemoveNamespaceRequestReceipt { file_system_id, .. } => {
                file_system_id
            }
        }
    }

    /// Whether applying this mutation can change the object row that determines
    /// visible path-to-shard placement.
    ///
    /// Object-local receipts, versions, intents, pins, conflicts, and locks all
    /// carry an object identity for recovery or GC, but do not publish a new
    /// object row. Keeping that distinction here lets both replica index repair
    /// and the service mirror avoid an all-shard object reload for those rows.
    pub fn changes_visible_object_placement(&self) -> bool {
        matches!(
            self,
            Self::PutObject { .. }
                | Self::FinalizeCrossShardSourceEviction { .. }
                | Self::CommitVisibleVersion { .. }
                | Self::ApplyNamespaceTransaction {
                    transaction: Some(_),
                    ..
                }
                | Self::PutNamespacePrepare {
                    status: DfsNamespacePrepareStatus::Committed,
                    ..
                }
                | Self::CommitNamespacePrepare { .. }
        )
    }

    /// Paths carried directly by a mutation that can change the visible
    /// namespace. Compact prepare-commit records intentionally return no path;
    /// callers must use a conservative root notification for those records.
    pub fn visible_namespace_paths(&self) -> Vec<&str> {
        match self {
            Self::PutObject { path, .. } => vec![path.as_str()],
            Self::FinalizeCrossShardSourceEviction { tombstone, .. } => {
                vec![tombstone.path.as_str()]
            }
            Self::CommitVisibleVersion {
                committed_object: Some(object),
                ..
            } => vec![object.path.as_str()],
            Self::ApplyNamespaceTransaction {
                transaction: Some(transaction),
                ..
            } => transaction
                .objects
                .iter()
                .map(|object| object.path.as_str())
                .collect(),
            _ => Vec::new(),
        }
    }

    fn visit_affected_object_ids(&self, mut visit: impl FnMut(&DfsObjectId)) {
        match self {
            Self::PutFileSystem { .. }
            | Self::PutNamespaceJob { .. }
            | Self::PutNamespaceJobReceipt { .. }
            | Self::RemoveNamespaceJobReceipt { .. }
            | Self::PutNamespaceRequest { .. }
            | Self::PutNamespaceRequestReceipt { .. }
            | Self::PutNamespaceTransactionReceipt { .. }
            | Self::RemoveNamespaceTransactionReceipt { .. }
            | Self::PutNamespacePrepareReceipt { .. }
            | Self::RemoveNamespacePrepareReceipt { .. }
            | Self::RemoveNamespaceRequestReceipt { .. } => {}
            Self::PutObject { object_id, .. }
            | Self::PutVersion { object_id, .. }
            | Self::CaptureFileVersion { object_id, .. }
            | Self::RemoveVersion { object_id, .. }
            | Self::PutVersionPin { object_id, .. }
            | Self::RemoveVersionPin { object_id, .. }
            | Self::ReconcileCrossShardPinTransfer { object_id, .. }
            | Self::RemoveCrossShardPinTransfer { object_id, .. }
            | Self::FinalizeCrossShardSourceEviction { object_id, .. }
            | Self::PutReceipt { object_id, .. }
            | Self::RemoveReceipt { object_id, .. }
            | Self::PutIntent { object_id, .. }
            | Self::PutIntentReceipt { object_id, .. }
            | Self::RemoveIntentReceipt { object_id, .. }
            | Self::RemoveRetainedRepairOwner { object_id, .. }
            | Self::MergeIntentReceiptRetention { object_id, .. }
            | Self::CommitVisibleVersion { object_id, .. }
            | Self::RemoveVisibleCommit { object_id, .. }
            | Self::PutConflict { object_id, .. }
            | Self::PutAdvisoryLockState { object_id, .. } => {
                visit(object_id);
            }
            Self::ApplyNamespaceTransaction {
                transaction: Some(transaction),
                ..
            } => {
                for object in &transaction.objects {
                    visit(&object.object_id);
                }
                for version in &transaction.versions {
                    visit(&version.object_id);
                }
                for state in &transaction.advisory_lock_states {
                    visit(&state.object_id);
                }
            }
            Self::ApplyNamespaceTransaction {
                transaction: None, ..
            } => {}
            Self::PutNamespacePrepare { .. } => match self.namespace_prepare_payload_ref() {
                Ok(Some(DfsNamespacePreparePayloadRef::Canonical { prepare, .. })) => {
                    for expected in &prepare.expected_object_versions {
                        visit(&expected.object_id);
                    }
                    for object in &prepare.transaction.objects {
                        visit(&object.object_id);
                    }
                }
                Ok(Some(DfsNamespacePreparePayloadRef::PackedV1 { prepare, .. })) => {
                    prepare.visit_affected_object_ids(visit);
                }
                Ok(Some(DfsNamespacePreparePayloadRef::PackedV2 { prepare, .. })) => {
                    prepare.visit_affected_object_ids_validated(visit);
                }
                // Missing, dual, unsupported, or malformed payloads are rejected
                // by replay. Do not let a pre-validation routing/index visitor
                // report a partial object set for such an entry.
                Ok(None) | Err(_) => {}
            },
            Self::CommitNamespacePrepare { object_ids, .. } => {
                for object_id in object_ids {
                    visit(object_id);
                }
            }
        }
    }

    pub(crate) fn affected_object_ids(&self) -> BTreeSet<DfsObjectId> {
        let mut object_ids = BTreeSet::new();
        self.visit_affected_object_ids(|object_id| {
            object_ids.insert(object_id.clone());
        });
        object_ids
    }

    /// Return the next sorted page of object IDs touched by this one durable
    /// journal entry without materializing an arbitrarily large transaction's
    /// complete ID set. The `has_more` bit is exact and lets a durable caller
    /// checkpoint progress inside one entry rather than rejecting a valid
    /// transaction that is larger than its per-pass work budget.
    pub(crate) fn affected_object_ids_page(
        &self,
        after_object_id: Option<&DfsObjectId>,
        max_entries: usize,
    ) -> (Vec<DfsObjectId>, bool) {
        if max_entries == 0 {
            let mut has_more = false;
            self.visit_affected_object_ids(|object_id| {
                if after_object_id.is_none_or(|after| object_id > after) {
                    has_more = true;
                }
            });
            return (Vec::new(), has_more);
        }

        let mut object_ids = BTreeSet::new();
        let mut has_more = false;
        self.visit_affected_object_ids(|object_id| {
            if after_object_id.is_some_and(|after| object_id <= after) {
                return;
            }
            if object_ids.insert(object_id.clone()) && object_ids.len() > max_entries {
                object_ids.pop_last();
                has_more = true;
            }
        });
        (object_ids.into_iter().collect(), has_more)
    }

    pub(crate) fn affects_object_id(&self, expected: &DfsObjectId) -> bool {
        let mut affected = false;
        self.visit_affected_object_ids(|object_id| {
            affected |= object_id == expected;
        });
        affected
    }

    pub(crate) fn put_file_system(file_system: &DfsFileSystem) -> Self {
        Self::PutFileSystem {
            file_system_id: file_system.file_system_id.clone(),
            file_system: Some(file_system.clone()),
        }
    }

    pub(crate) fn put_object(object: &DfsObjectMetadata) -> Self {
        Self::PutObject {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: object.path.clone(),
            object: Some(object.clone()),
        }
    }

    pub(crate) fn put_version(version: &DfsObjectVersionRecord) -> Self {
        Self::PutVersion {
            file_system_id: version.file_system_id.clone(),
            object_id: version.object_id.clone(),
            version: version.version.clone(),
            version_record: Some(version.clone()),
        }
    }

    pub(crate) fn remove_version(
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
    ) -> Self {
        Self::RemoveVersion {
            file_system_id,
            object_id,
            version,
        }
    }

    pub(crate) fn put_version_pin(pin: &DfsMetadataVersionPinRecord) -> Self {
        Self::PutVersionPin {
            file_system_id: pin.file_system_id.clone(),
            object_id: pin.object_id.clone(),
            version: pin.version.clone(),
            pin_id: pin.pin_id.clone(),
            pin: pin.clone(),
        }
    }

    pub(crate) fn remove_version_pin(
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        pin_id: String,
    ) -> Self {
        Self::RemoveVersionPin {
            file_system_id,
            object_id,
            version,
            pin_id,
        }
    }

    pub(crate) fn reconcile_cross_shard_pin_transfer(
        transfer: &DfsCrossShardPinTransferRecord,
        versions: Vec<DfsObjectVersionRecord>,
        authoritative_migration: bool,
    ) -> Self {
        Self::ReconcileCrossShardPinTransfer {
            file_system_id: transfer.file_system_id.clone(),
            object_id: transfer.object_id.clone(),
            transfer_id: transfer.transfer_id.clone(),
            authoritative_migration,
            versions,
            transfer: transfer.clone(),
        }
    }

    pub(crate) fn remove_cross_shard_pin_transfer(
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        transfer_id: String,
    ) -> Self {
        Self::RemoveCrossShardPinTransfer {
            file_system_id,
            object_id,
            transfer_id,
        }
    }

    pub(crate) fn finalize_cross_shard_source_eviction(
        tombstone: &DfsObjectMetadata,
        removed_pins: Vec<DfsMetadataVersionPinRecord>,
        removed_pin_transfers: Vec<DfsCrossShardPinTransferRecord>,
    ) -> Self {
        Self::FinalizeCrossShardSourceEviction {
            file_system_id: tombstone.file_system_id.clone(),
            object_id: tombstone.object_id.clone(),
            tombstone: tombstone.clone(),
            removed_pins,
            removed_pin_transfers,
        }
    }

    pub(crate) fn put_receipt(receipt: &DfsReplicaReceiptRecord) -> Self {
        Self::PutReceipt {
            file_system_id: receipt.receipt.file_system_id.clone(),
            object_id: receipt.receipt.object_id.clone(),
            version: receipt.receipt.committed_version.clone(),
            replica_node_id: receipt.replica_node_id.clone(),
            receipt: Some(receipt.clone()),
        }
    }

    pub(crate) fn put_intent(intent: &DfsWriteIntentRecord) -> Self {
        Self::PutIntent {
            file_system_id: intent.file_system_id.clone(),
            object_id: intent.object_id.clone(),
            intent_id: intent.intent_id.clone(),
            status: intent.status.clone(),
            authoritative_migration: false,
            intent: Some(intent.clone()),
        }
    }

    pub(crate) fn put_migrated_intent(intent: &DfsWriteIntentRecord) -> Self {
        Self::PutIntent {
            file_system_id: intent.file_system_id.clone(),
            object_id: intent.object_id.clone(),
            intent_id: intent.intent_id.clone(),
            status: intent.status.clone(),
            authoritative_migration: true,
            intent: Some(intent.clone()),
        }
    }

    pub(crate) fn put_intent_receipt(receipt: &DfsWriteIntentReceipt) -> Self {
        Self::PutIntentReceipt {
            file_system_id: receipt.file_system_id.clone(),
            object_id: receipt.object_id.clone(),
            intent_id: receipt.intent_id.clone(),
            receipt: receipt.clone(),
        }
    }

    pub(crate) fn remove_intent_receipt(
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        intent_id: String,
        retention: DfsWriteIntentReceiptRetentionState,
    ) -> Self {
        Self::RemoveIntentReceipt {
            file_system_id,
            object_id,
            intent_id,
            retention,
        }
    }

    pub(crate) fn merge_intent_receipt_retention(
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        retention: DfsWriteIntentReceiptRetentionState,
    ) -> Self {
        Self::MergeIntentReceiptRetention {
            file_system_id,
            object_id,
            retention,
        }
    }

    pub(crate) fn put_advisory_lock_state(state: &DfsAdvisoryLockStateRecord) -> Self {
        Self::PutAdvisoryLockState {
            file_system_id: state.file_system_id.clone(),
            object_id: state.object_id.clone(),
            state: Some(state.clone()),
        }
    }

    pub(crate) fn put_namespace_job(job: &DfsNamespaceJobRecord) -> Self {
        Self::PutNamespaceJob {
            file_system_id: job.file_system_id.clone(),
            job_id: job.job_id.clone(),
            job: Some(job.clone()),
        }
    }

    pub(crate) fn put_namespace_job_receipt(receipt: &DfsNamespaceJobReceipt) -> Self {
        Self::PutNamespaceJobReceipt {
            file_system_id: receipt.file_system_id.clone(),
            job_id: receipt.job_id.clone(),
            receipt: receipt.clone(),
        }
    }

    pub(crate) fn put_namespace_request(request: &DfsNamespaceRequestRecord) -> Self {
        Self::PutNamespaceRequest {
            file_system_id: request.file_system_id.clone(),
            requester_node_id: request.requester_node_id.clone(),
            request_id: request.request_id.clone(),
            request: Box::new(request.clone()),
        }
    }

    pub(crate) fn put_namespace_request_receipt(receipt: &DfsNamespaceRequestReceipt) -> Self {
        Self::PutNamespaceRequestReceipt {
            file_system_id: receipt.file_system_id.clone(),
            requester_node_id: receipt.requester_node_id.clone(),
            request_id: receipt.request_id.clone(),
            receipt: Box::new(receipt.clone()),
        }
    }

    pub(crate) fn put_namespace_prepare(prepare: &DfsNamespacePrepareRecord) -> Self {
        Self::PutNamespacePrepare {
            file_system_id: prepare.file_system_id.clone(),
            operation_id: prepare.operation_id.clone(),
            status: prepare.status.clone(),
            prepare: Some(Box::new(prepare.clone())),
            packed_prepare_v1: None,
            packed_prepare_v2: None,
        }
    }

    pub(crate) fn put_namespace_prepare_packed_v1(
        prepare: &DfsNamespacePrepareRecord,
    ) -> Result<Self, DfsNamespacePrepareCodecError> {
        Ok(Self::PutNamespacePrepare {
            file_system_id: prepare.file_system_id.clone(),
            operation_id: prepare.operation_id.clone(),
            status: prepare.status.clone(),
            prepare: None,
            packed_prepare_v1: Some(Box::new(DfsPackedNamespacePrepareV1::pack(prepare)?)),
            packed_prepare_v2: None,
        })
    }

    pub(crate) fn put_namespace_prepare_packed_v2(
        prepare: &DfsNamespacePrepareRecord,
    ) -> Result<Self, DfsNamespacePrepareCodecError> {
        Ok(Self::PutNamespacePrepare {
            file_system_id: prepare.file_system_id.clone(),
            operation_id: prepare.operation_id.clone(),
            status: prepare.status.clone(),
            prepare: None,
            packed_prepare_v1: None,
            packed_prepare_v2: Some(Box::new(DfsPackedNamespacePrepareV2::pack(prepare)?)),
        })
    }

    pub(crate) fn put_namespace_prepare_packed_v3(
        prepare: &DfsNamespacePrepareRecord,
    ) -> Result<Self, DfsNamespacePrepareCodecError> {
        Ok(Self::PutNamespacePrepare {
            file_system_id: prepare.file_system_id.clone(),
            operation_id: prepare.operation_id.clone(),
            status: prepare.status.clone(),
            prepare: None,
            packed_prepare_v1: None,
            packed_prepare_v2: Some(Box::new(DfsPackedNamespacePrepareV2::pack_v3(prepare)?)),
        })
    }

    pub(crate) fn namespace_prepare_payload_ref(
        &self,
    ) -> Result<Option<DfsNamespacePreparePayloadRef<'_>>, DfsNamespacePrepareCodecError> {
        let Self::PutNamespacePrepare {
            file_system_id,
            operation_id,
            status,
            prepare,
            packed_prepare_v1,
            packed_prepare_v2,
        } = self
        else {
            return Ok(None);
        };
        let representation_count = usize::from(prepare.is_some())
            .saturating_add(usize::from(packed_prepare_v1.is_some()))
            .saturating_add(usize::from(packed_prepare_v2.is_some()));
        if representation_count > 1 {
            return Err(DfsNamespacePrepareCodecError::AmbiguousPayload);
        }
        if let Some(prepare) = prepare.as_ref() {
            #[cfg(test)]
            record_namespace_prepare_payload_validation_pass();
            validate_namespace_prepare_file_system_scope(prepare, file_system_id)?;
            return Ok(Some(DfsNamespacePreparePayloadRef::Canonical {
                file_system_id,
                operation_id,
                status,
                prepare,
            }));
        }
        if let Some(prepare) = packed_prepare_v1.as_ref() {
            #[cfg(test)]
            record_namespace_prepare_payload_validation_pass();
            prepare.validate_file_system_scope(file_system_id)?;
            return Ok(Some(DfsNamespacePreparePayloadRef::PackedV1 {
                file_system_id,
                operation_id,
                status,
                prepare,
            }));
        }
        if let Some(prepare) = packed_prepare_v2.as_ref() {
            #[cfg(test)]
            record_namespace_prepare_payload_validation_pass();
            prepare.validate_file_system_scope(file_system_id)?;
            return Ok(Some(DfsNamespacePreparePayloadRef::PackedV2 {
                file_system_id,
                operation_id,
                status,
                prepare,
            }));
        }
        Ok(None)
    }

    /// Reborrow a namespace-prepare representation whose full codec and
    /// filesystem validation was already completed against this immutable
    /// mutation.
    ///
    /// This is used only to carry a validation proof across a write-lock or
    /// table-planning boundary. Callers must not use it for unvalidated wire
    /// input.
    pub(crate) fn namespace_prepare_payload_ref_prevalidated(
        &self,
    ) -> Option<DfsNamespacePreparePayloadRef<'_>> {
        let Self::PutNamespacePrepare {
            file_system_id,
            operation_id,
            status,
            prepare,
            packed_prepare_v1,
            packed_prepare_v2,
        } = self
        else {
            return None;
        };
        match (
            prepare.as_ref(),
            packed_prepare_v1.as_ref(),
            packed_prepare_v2.as_ref(),
        ) {
            (Some(prepare), None, None) => Some(DfsNamespacePreparePayloadRef::Canonical {
                file_system_id,
                operation_id,
                status,
                prepare,
            }),
            (None, Some(prepare), None) => Some(DfsNamespacePreparePayloadRef::PackedV1 {
                file_system_id,
                operation_id,
                status,
                prepare,
            }),
            (None, None, Some(prepare)) => Some(DfsNamespacePreparePayloadRef::PackedV2 {
                file_system_id,
                operation_id,
                status,
                prepare,
            }),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn namespace_prepare_payload(
        &self,
    ) -> Result<Option<Cow<'_, DfsNamespacePrepareRecord>>, DfsNamespacePrepareCodecError> {
        self.namespace_prepare_payload_ref()?
            .map(DfsNamespacePreparePayloadRef::into_canonical)
            .transpose()
    }

    pub(crate) fn commit_namespace_prepare(prepare: &DfsNamespacePrepareRecord) -> Self {
        let mut object_ids = prepare
            .expected_object_versions
            .iter()
            .map(|expected| expected.object_id.clone())
            // Receipt validation fences the filesystem but intentionally does
            // not require every receipt object to have a version expectation.
            // Keep those independently valid objects in the compact commit's
            // affected-object set.
            .chain(
                prepare
                    .content_receipts
                    .iter()
                    .map(|receipt| receipt.receipt.object_id.clone()),
            )
            .collect::<Vec<_>>();
        object_ids.sort();
        object_ids.dedup();
        Self::CommitNamespacePrepare {
            file_system_id: prepare.file_system_id.clone(),
            operation_id: prepare.operation_id.clone(),
            operation_digest: prepare.operation_digest.clone(),
            owner_epoch: prepare.owner_epoch,
            object_ids,
        }
    }

    pub(crate) fn put_namespace_prepare_receipt(receipt: &DfsNamespacePrepareReceipt) -> Self {
        Self::PutNamespacePrepareReceipt {
            file_system_id: receipt.file_system_id.clone(),
            operation_id: receipt.operation_id.clone(),
            receipt: receipt.clone(),
        }
    }

    pub(crate) fn put_namespace_transaction_receipt(
        receipt: &DfsNamespaceTransactionReceipt,
    ) -> Self {
        Self::PutNamespaceTransactionReceipt {
            file_system_id: receipt.file_system_id.clone(),
            transaction_id: receipt.transaction_id.clone(),
            receipt: receipt.clone(),
        }
    }

    pub(crate) fn commit_visible_version(commit: &DfsVisibleVersionCommitRecord) -> Self {
        Self::CommitVisibleVersion {
            file_system_id: commit.committed_object.file_system_id.clone(),
            object_id: commit.committed_object.object_id.clone(),
            previous_version: commit.expected_version.clone(),
            committed_version: commit.committed_object.version.clone(),
            operation_id: commit.operation_id.clone(),
            committed_object: Some(commit.committed_object.clone()),
            visible_version: Some(commit.visible_version.clone()),
            required_replica_node_ids: commit.required_replica_node_ids.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataSubscription {
    pub shard_id: DfsMetadataShardId,
    pub next_after_sequence: u64,
}

impl DfsMetadataSubscription {
    pub fn new(shard_id: DfsMetadataShardId, next_after_sequence: u64) -> Self {
        Self {
            shard_id,
            next_after_sequence,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataJournalBatch {
    pub shard_id: DfsMetadataShardId,
    pub entries: Vec<DfsMetadataJournalEntry>,
    pub next_after_sequence: u64,
}

/// Exact immutable SOBJ bytes for one journal entry carried by versioned
/// authority and ordinary replica-catch-up paths. `sequence` is duplicated
/// outside the bytes so bounds, ordering, and response correlation can be
/// checked before decoding a large payload; receivers must still verify that
/// the decoded entry matches it.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataJournalFrameRecord {
    pub sequence: u64,
    pub exact_bytes: Bytes,
}

/// Atomic namespace batch used to publish related object and version records.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsFileVersionPinMutation {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub add: Vec<DfsMetadataVersionPinRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remove: Vec<DfsMetadataVersionPinRecord>,
}

impl DfsFileVersionPinMutation {
    pub(crate) fn validate_scope(&self, file_system_id: &DfsFileSystemId) -> Result<(), String> {
        if self.add.len().saturating_add(self.remove.len()) > 4096 {
            return Err("file history pin batch exceeds 4096 entries".into());
        }
        let mut identities = BTreeSet::new();
        for pin in self.add.iter().chain(&self.remove) {
            if &pin.file_system_id != file_system_id
                || pin.pin_id.trim().is_empty()
                || pin.pin_id.len() > 256
                || pin.pin_id.contains('\0')
                || !identities.insert((&pin.object_id, &pin.version, &pin.pin_id))
            {
                return Err("invalid, duplicate, or cross-filesystem history pin".into());
            }
        }
        Ok(())
    }
}

/// Atomic namespace batch used to publish related object and version records.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceTransaction {
    pub transaction_id: String,
    pub file_system_id: DfsFileSystemId,
    pub objects: Vec<DfsObjectMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub versions: Vec<DfsObjectVersionRecord>,
    /// Explicit history pins share the namespace prepare/publication protocol.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_version_pins: Option<DfsFileVersionPinMutation>,
    /// Exact lock aggregates installed atomically with migrated objects.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub advisory_lock_states: Vec<DfsAdvisoryLockStateRecord>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

/// Fixed-size identity retained once a namespace transaction is known to be
/// completely represented by a durable materialized snapshot.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceTransactionReceipt {
    pub transaction_id: String,
    pub file_system_id: DfsFileSystemId,
    pub transaction_digest: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    /// Start of this terminal identity's bounded exact-retry window.
    #[serde(with = "time::serde::rfc3339")]
    pub retained_at: OffsetDateTime,
}

impl DfsNamespaceTransactionReceipt {
    pub(crate) fn from_transaction(
        transaction: &DfsNamespaceTransaction,
        retained_at: OffsetDateTime,
    ) -> Result<Self, serde_json::Error> {
        let mut hasher = blake3::Hasher::new();
        serde_json::to_writer(Blake3HashWriter(&mut hasher), transaction)?;
        Ok(Self {
            transaction_id: transaction.transaction_id.clone(),
            file_system_id: transaction.file_system_id.clone(),
            transaction_digest: format!("blake3:{}", hasher.finalize().to_hex()),
            created_at: transaction.created_at,
            retained_at,
        })
    }

    pub(crate) fn matches_transaction(
        &self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<bool, serde_json::Error> {
        Ok(self == &Self::from_transaction(transaction, self.retained_at)?)
    }
}

/// Streaming sink used for stable JSON identities without allocating another
/// complete copy of a bulk namespace transaction.
struct Blake3HashWriter<'a>(&'a mut blake3::Hasher);

impl std::io::Write for Blake3HashWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Expected visible version for one object in a hidden namespace prepare.
///
/// `None` means the object must not exist when the prepare is installed and
/// again when it is committed.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct DfsNamespaceObjectVersionExpectation {
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<DfsVersion>,
}

/// Expected occupant for one normalized path in a hidden namespace prepare.
///
/// `None` means no live object or durable tombstone may occupy the path.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct DfsNamespacePathOccupantExpectation {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_object_id: Option<DfsObjectId>,
}

/// Lifecycle of a durable namespace prepare.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNamespacePrepareStatus {
    Prepared,
    Committed,
    Aborted,
}

const NAMESPACE_PREPARE_JOURNAL_PROVENANCE_PREFIX: &str = "namespace-journal-provenance-v1:";

/// Build a prepare key that remains unique per transaction while carrying a
/// reversible caller-owned operation id for semantic journal projection.
///
/// The prepare key is internal metadata identity, so it cannot be replaced by
/// the caller id directly: one distributed operation may commit one prepare
/// on each of several shards. Length framing keeps arbitrary non-control UTF-8
/// operation ids unambiguous without changing the durable prepare schema.
pub(crate) fn namespace_prepare_operation_id_with_journal_provenance(
    journal_operation_id: &str,
    transaction_id: &str,
) -> String {
    let transaction_digest = blake3::hash(transaction_id.as_bytes()).to_hex();
    format!(
        "{NAMESPACE_PREPARE_JOURNAL_PROVENANCE_PREFIX}{}:{journal_operation_id}:{}",
        journal_operation_id.len(),
        &transaction_digest.as_str()[..24],
    )
}

/// Recover caller-owned feed provenance from an internal prepare key created
/// by [`namespace_prepare_operation_id_with_journal_provenance`].
pub(crate) fn namespace_prepare_journal_operation_id(operation_id: &str) -> Option<&str> {
    let framed = operation_id.strip_prefix(NAMESPACE_PREPARE_JOURNAL_PROVENANCE_PREFIX)?;
    let (length, framed) = framed.split_once(':')?;
    let length = length.parse::<usize>().ok()?;
    if length == 0 || framed.len() <= length || !framed.is_char_boundary(length) {
        return None;
    }
    let (journal_operation_id, suffix) = framed.split_at(length);
    let digest = suffix.strip_prefix(':')?;
    if digest.len() != 24 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some(journal_operation_id)
}

/// Durable, hidden namespace transaction prepared on one metadata shard.
///
/// A prepared record reserves its object/path footprint without changing the
/// visible namespace. Committing revalidates these exact expectations under
/// the metadata write lock and atomically materializes `transaction` together
/// with the `Committed` status.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespacePrepareRecord {
    pub operation_id: String,
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    #[serde(default)]
    pub owner_epoch: u64,
    pub operation_digest: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expected_object_versions: Vec<DfsNamespaceObjectVersionExpectation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expected_path_occupants: Vec<DfsNamespacePathOccupantExpectation>,
    /// Authenticated durable-content receipts materialized atomically with the
    /// visible namespace transaction. Namespace-only operations leave this
    /// empty; typed content commits must provide one receipt per visible file
    /// version before the prepare can commit. Pre-receipt journal entries omit
    /// this field and replay with an empty receipt set.
    #[serde(default)]
    pub content_receipts: Vec<DfsReplicaReceiptRecord>,
    pub transaction: DfsNamespaceTransaction,
    /// Terminal routed-request result materialized atomically with the visible
    /// namespace transaction. Standalone and write-intent prepares leave this
    /// empty; request-owned namespace prepares use it to close the crash window
    /// between applying metadata and recording the canonical RPC outcome.
    /// Legacy prepares predate routed requests and omit this field; request
    /// validation still rejects new request-owned prepares without a result.
    #[serde(default)]
    pub completed_request: Option<DfsNamespaceRequestRecord>,
    /// Compact terminal routed-request evidence for operations whose semantic
    /// body is already fully represented by this prepare's transaction and
    /// content receipts. New fused local-eventual batches use this attachment
    /// instead of repeating the full operation in `completed_request`.
    ///
    /// Legacy prepares omit this field. A request-owned prepare may carry one
    /// completion representation, never both.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_request_completion: Option<DfsNamespaceRequestCompletion>,
    pub status: DfsNamespacePrepareStatus,
}

/// Terminal namespace-prepare identity retained after the complete hidden
/// transaction is no longer needed for recovery.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespacePrepareReceipt {
    pub operation_id: String,
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub owner_epoch: u64,
    pub operation_digest: String,
    pub transaction_id: String,
    pub status: DfsNamespacePrepareStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    /// Start of this terminal identity's bounded exact-retry window.
    #[serde(with = "time::serde::rfc3339")]
    pub retained_at: OffsetDateTime,
}

impl DfsNamespacePrepareReceipt {
    pub(crate) fn from_prepare(
        prepare: &DfsNamespacePrepareRecord,
        retained_at: OffsetDateTime,
    ) -> Option<Self> {
        (prepare.status != DfsNamespacePrepareStatus::Prepared).then(|| Self {
            operation_id: prepare.operation_id.clone(),
            file_system_id: prepare.file_system_id.clone(),
            shard_id: prepare.shard_id.clone(),
            owner_epoch: prepare.owner_epoch,
            operation_digest: prepare.operation_digest.clone(),
            transaction_id: prepare.transaction.transaction_id.clone(),
            status: prepare.status.clone(),
            created_at: prepare.transaction.created_at,
            retained_at,
        })
    }

    pub(crate) fn matches_prepare_identity(&self, prepare: &DfsNamespacePrepareRecord) -> bool {
        self.operation_id == prepare.operation_id
            && self.file_system_id == prepare.file_system_id
            && self.shard_id == prepare.shard_id
            && self.owner_epoch == prepare.owner_epoch
            && self.operation_digest == prepare.operation_digest
            && self.transaction_id == prepare.transaction.transaction_id
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNamespaceRequestStatus {
    Pending,
    Completed,
}

/// Durable, autonomously recoverable admission record for one routed namespace
/// mutation.
///
/// `Pending` contains the exact semantic operation and authority shard needed
/// to resume it. Namespace transactions may atomically advance this record to
/// `Completed`; jobs and cross-shard sagas are resumed by maintenance until
/// they can record their canonical terminal result. Exact retries never create
/// a second request identity.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceRequestRecord {
    pub file_system_id: DfsFileSystemId,
    pub requester_node_id: NodeId,
    pub request_id: String,
    pub operation_id: String,
    pub authority_shard_id: DfsMetadataShardId,
    pub operation: DfsNamespaceOperation,
    pub command_digest: String,
    pub status: DfsNamespaceRequestStatus,
    pub result: Option<DfsNamespaceResult>,
    pub posix_coordination_result: Option<DfsPosixCoordinationResult>,
    pub recovery_attempts: u32,
    pub last_recovery_error: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

/// Compact terminal result attached to a request-owned namespace prepare.
///
/// The prepare's transaction and content receipts already contain the full
/// durable mutation. This record retains only the identity and canonical
/// response needed to index a hidden prepare, validate exact retries, and
/// materialize a bounded request receipt when the prepare commits.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceRequestCompletion {
    pub file_system_id: DfsFileSystemId,
    pub requester_node_id: NodeId,
    pub request_id: String,
    pub operation_id: String,
    pub authority_shard_id: DfsMetadataShardId,
    pub command_digest: String,
    /// Exact local-eventual transaction whose full body is carried by the
    /// owning namespace prepare.
    pub transaction_id: String,
    pub result: DfsNamespaceResult,
    pub posix_coordination_result: Option<DfsPosixCoordinationResult>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub completed_at: OffsetDateTime,
}

impl DfsNamespaceRequestCompletion {
    pub(crate) fn from_completed_request(request: &DfsNamespaceRequestRecord) -> Option<Self> {
        if request.status != DfsNamespaceRequestStatus::Completed {
            return None;
        }
        Some(Self {
            file_system_id: request.file_system_id.clone(),
            requester_node_id: request.requester_node_id.clone(),
            request_id: request.request_id.clone(),
            operation_id: request.operation_id.clone(),
            authority_shard_id: request.authority_shard_id.clone(),
            command_digest: request.command_digest.clone(),
            transaction_id: match &request.operation {
                DfsNamespaceOperation::CommitLocalEventualBatch { transaction, .. } => {
                    transaction.transaction_id.clone()
                }
                _ => return None,
            },
            result: request.result.clone()?,
            posix_coordination_result: request.posix_coordination_result.clone(),
            created_at: request.created_at,
            completed_at: request.updated_at,
        })
    }

    pub(crate) fn request_key(&self) -> (NodeId, String) {
        (self.requester_node_id.clone(), self.request_id.clone())
    }

    pub(crate) fn matches_completed_request(&self, request: &DfsNamespaceRequestRecord) -> bool {
        Self::from_completed_request(request).as_ref() == Some(self)
    }
}

/// Compact, canonical response retained for exact retries after a completed
/// namespace request's operation and recovery bookkeeping have been folded
/// into an authoritative materialized snapshot.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceRequestReceipt {
    pub file_system_id: DfsFileSystemId,
    pub requester_node_id: NodeId,
    pub request_id: String,
    pub operation_id: String,
    pub authority_shard_id: DfsMetadataShardId,
    pub command_digest: String,
    pub result: DfsNamespaceResult,
    pub posix_coordination_result: Option<DfsPosixCoordinationResult>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    /// Start of this terminal identity's bounded exact-retry window.
    #[serde(with = "time::serde::rfc3339")]
    pub retained_at: OffsetDateTime,
}

impl DfsNamespaceRequestReceipt {
    pub(crate) fn from_request(
        request: &DfsNamespaceRequestRecord,
        retained_at: OffsetDateTime,
    ) -> Option<Self> {
        if request.status != DfsNamespaceRequestStatus::Completed {
            return None;
        }
        Some(Self {
            file_system_id: request.file_system_id.clone(),
            requester_node_id: request.requester_node_id.clone(),
            request_id: request.request_id.clone(),
            operation_id: request.operation_id.clone(),
            authority_shard_id: request.authority_shard_id.clone(),
            command_digest: request.command_digest.clone(),
            result: request.result.clone()?,
            posix_coordination_result: request.posix_coordination_result.clone(),
            created_at: request.created_at,
            retained_at,
        })
    }

    pub(crate) fn matches_completed_request(&self, request: &DfsNamespaceRequestRecord) -> bool {
        Self::from_request(request, self.retained_at).as_ref() == Some(self)
    }

    pub(crate) fn from_completion(completion: &DfsNamespaceRequestCompletion) -> Self {
        Self {
            file_system_id: completion.file_system_id.clone(),
            requester_node_id: completion.requester_node_id.clone(),
            request_id: completion.request_id.clone(),
            operation_id: completion.operation_id.clone(),
            authority_shard_id: completion.authority_shard_id.clone(),
            command_digest: completion.command_digest.clone(),
            result: completion.result.clone(),
            posix_coordination_result: completion.posix_coordination_result.clone(),
            created_at: completion.created_at,
            retained_at: completion.completed_at,
        }
    }

    pub(crate) fn matches_completion(&self, completion: &DfsNamespaceRequestCompletion) -> bool {
        &Self::from_completion(completion) == self
    }
}

/// Durable high-water marks for terminal idempotency receipts evicted by a
/// bounded table checkpoint.
///
/// Full pending/prepared records are never covered by these floors. A command
/// at or below a floor is explicitly expired/unknown instead of being admitted
/// as a fresh mutation after its exact receipt has left the bounded window.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceReceiptRetentionState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_floor: Option<DfsNamespaceReceiptOrderKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepare_floor: Option<DfsNamespaceReceiptOrderKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_floor: Option<DfsNamespaceReceiptOrderKey>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub max_evicted_request_order_token_by_requester: BTreeMap<NodeId, String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub opaque_request_history_evicted_requester_node_ids: BTreeSet<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct DfsNamespaceReceiptOrderKey {
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub identity: String,
}

impl DfsNamespaceReceiptRetentionState {
    pub(crate) fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    pub(crate) fn merge_conservative(&mut self, other: Self) {
        self.transaction_floor = match (self.transaction_floor.take(), other.transaction_floor) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (Some(key), None) | (None, Some(key)) => Some(key),
            (None, None) => None,
        };
        self.prepare_floor = match (self.prepare_floor.take(), other.prepare_floor) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (Some(key), None) | (None, Some(key)) => Some(key),
            (None, None) => None,
        };
        self.job_floor = match (self.job_floor.take(), other.job_floor) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (Some(key), None) | (None, Some(key)) => Some(key),
            (None, None) => None,
        };
        for (requester_node_id, floor) in other.max_evicted_request_order_token_by_requester {
            let current = self
                .max_evicted_request_order_token_by_requester
                .entry(requester_node_id)
                .or_default();
            *current = std::mem::take(current).max(floor);
        }
        self.opaque_request_history_evicted_requester_node_ids
            .extend(other.opaque_request_history_evicted_requester_node_ids);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for pending routed namespace-request recovery.
pub struct DfsPendingNamespaceRequestPageCursor {
    /// Last recovery-attempt timestamp at the end of the current page.
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    /// Authenticated requester at the end of the current page.
    pub requester_node_id: NodeId,
    /// Request id at the end of the current page.
    pub request_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of durable namespace requests which still require recovery.
pub struct DfsPendingNamespaceRequestPage {
    /// Pending requests in oldest-attempt-first order.
    pub requests: Vec<DfsNamespaceRequestRecord>,
    /// Cursor to pass to the next page request, if more pending requests remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsPendingNamespaceRequestPageCursor>,
}

pub use sinomo_api::dfs::metadata_records::DfsObjectPageCursor;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of object metadata records.
pub struct DfsObjectPage {
    /// Objects returned by this page.
    pub objects: Vec<DfsObjectMetadata>,
    /// Cursor to pass to the next page request, if more objects remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsObjectPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for conflict records belonging to one object.
pub struct DfsConflictPageCursor {
    /// Conflict id at the end of the current page.
    pub conflict_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of conflicts for one object.
pub struct DfsConflictPage {
    /// Conflict records returned by this page.
    pub conflicts: Vec<DfsConflictRecord>,
    /// Cursor to pass to the next page request, if more conflicts remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsConflictPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for version records belonging to one object.
pub struct DfsObjectVersionPageCursor {
    /// Version id at the end of the current page.
    pub version: DfsVersion,
    /// Metadata shard to resume from when the page came from a sharded backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of version records for one object.
pub struct DfsObjectVersionPage {
    /// Versions returned by this page.
    pub versions: Vec<DfsObjectVersionRecord>,
    /// Cursor to pass to the next page request, if more versions remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsObjectVersionPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for durable version pins belonging to one object.
pub struct DfsMetadataVersionPinPageCursor {
    /// Version id at the end of the current page.
    pub version: DfsVersion,
    /// Independent consumer identity at the end of the current page.
    pub pin_id: String,
    /// Metadata shard to resume from when the page came from a sharded backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of durable version pins belonging to one object.
pub struct DfsMetadataVersionPinPage {
    /// Pins returned by this page in `(version, pin_id)` order.
    pub pins: Vec<DfsMetadataVersionPinRecord>,
    /// Cursor to pass to the next page request, if more pins remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsMetadataVersionPinPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for replica receipt records belonging to one object version.
pub struct DfsReplicaReceiptPageCursor {
    /// Replica node id at the end of the current page.
    pub replica_node_id: NodeId,
    /// Metadata shard to resume from when the page came from a sharded backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of replica receipt records for one object version.
pub struct DfsReplicaReceiptPage {
    /// Receipts returned by this page.
    pub receipts: Vec<DfsReplicaReceiptRecord>,
    /// Cursor to pass to the next page request, if more receipts remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsReplicaReceiptPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for write intent records belonging to one object.
pub struct DfsWriteIntentPageCursor {
    /// Intent id at the end of the current page.
    pub intent_id: String,
    /// Metadata shard to resume from when the page came from a sharded backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of write intent records for one object or object version.
pub struct DfsWriteIntentPage {
    /// Write intents returned by this page.
    pub intents: Vec<DfsWriteIntentRecord>,
    /// Cursor to pass to the next page request, if more intents remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsWriteIntentPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for global write-intent scans.
pub struct DfsWriteIntentScanCursor {
    /// Intent creation time at the end of the current page.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    /// Intent id at the end of the current page.
    pub intent_id: String,
    /// Object id at the end of the current page.
    pub object_id: DfsObjectId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of write intents from a global metadata scan.
pub struct DfsWriteIntentScanPage {
    /// Write intents returned by this page.
    pub intents: Vec<DfsWriteIntentRecord>,
    /// Cursor to pass to the next scan request, if more intents remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsWriteIntentScanCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for global object-version scans.
pub struct DfsObjectVersionScanCursor {
    /// Object id at the end of the current page.
    pub object_id: DfsObjectId,
    /// Version id at the end of the current page.
    pub version: DfsVersion,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of object versions from a global metadata scan.
pub struct DfsObjectVersionScanPage {
    /// Version records returned by this page.
    pub versions: Vec<DfsObjectVersionRecord>,
    /// Cursor to pass to the next scan request, if more versions remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsObjectVersionScanCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for global replica-receipt scans.
pub struct DfsReplicaReceiptScanCursor {
    /// Object id at the end of the current page.
    pub object_id: DfsObjectId,
    /// Version id at the end of the current page.
    pub version: DfsVersion,
    /// Replica node id at the end of the current page.
    pub replica_node_id: NodeId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of replica receipts from a global metadata scan.
pub struct DfsReplicaReceiptScanPage {
    /// Replica receipts returned by this page.
    pub receipts: Vec<DfsReplicaReceiptRecord>,
    /// Cursor to pass to the next scan request, if more receipts remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsReplicaReceiptScanCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for global conflict scans.
pub struct DfsConflictScanCursor {
    /// Object id at the end of the current page.
    pub object_id: DfsObjectId,
    /// Conflict id at the end of the current page.
    pub conflict_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of conflicts from a global metadata scan.
pub struct DfsConflictScanPage {
    /// Conflicts returned by this page.
    pub conflicts: Vec<DfsConflictRecord>,
    /// Cursor to pass to the next scan request, if more conflicts remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsConflictScanCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for direct child object pages under one directory.
pub struct DfsDirectoryPageCursor {
    /// Child path at the end of the current page.
    pub path: String,
    /// Child object id at the end of the current page.
    pub object_id: DfsObjectId,
    /// Dentry cursor used by backends with fragmented directory indexes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dentry: Option<DfsDentryPageCursor>,
    /// Metadata shard to resume from when the page came from a sharded backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Directory-entry cursor within one directory fragment.
pub struct DfsDentryPageCursor {
    /// Directory fragment id at the end of the current page.
    pub dirfrag_id: DfsDirFragmentId,
    /// Dentry name hash at the end of the current page.
    pub name_hash: u64,
    /// Dentry name at the end of the current page.
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of direct child objects under one directory.
pub struct DfsDirectoryPage {
    /// Direct children returned by this page.
    pub objects: Vec<DfsObjectMetadata>,
    /// Cursor to pass to the next page request, if more children remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsDirectoryPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Cursor for namespace background job scans.
pub struct DfsNamespaceJobPageCursor {
    /// Job id at the end of the current page.
    pub job_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
/// Page of namespace background jobs.
pub struct DfsNamespaceJobPage {
    /// Namespace jobs returned by this page.
    pub jobs: Vec<DfsNamespaceJobRecord>,
    /// Cursor to pass to the next page request, if more jobs remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsNamespaceJobPageCursor>,
}

/// Options for compacting tombstone metadata into retained path markers.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsTombstoneCompactionOptions {
    pub dry_run: bool,
    pub min_tombstone_age_nanos: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_compactions: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_unix_nanos: Option<u64>,
}

impl DfsTombstoneCompactionOptions {
    pub fn dry_run(min_tombstone_age: Duration) -> Self {
        Self {
            dry_run: true,
            min_tombstone_age_nanos: duration_nanos_saturating(min_tombstone_age),
            max_compactions: None,
            now_unix_nanos: None,
        }
    }

    pub fn compact_eligible(min_tombstone_age: Duration) -> Self {
        Self {
            dry_run: false,
            min_tombstone_age_nanos: duration_nanos_saturating(min_tombstone_age),
            max_compactions: None,
            now_unix_nanos: None,
        }
    }

    pub fn with_max_compactions(mut self, max_compactions: u64) -> Self {
        self.max_compactions = Some(max_compactions);
        self
    }

    pub fn with_now_unix_nanos(mut self, now_unix_nanos: u64) -> Self {
        self.now_unix_nanos = Some(now_unix_nanos);
        self
    }
}

/// Counts produced by a tombstone compaction run.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsTombstoneCompactionReport {
    pub dry_run: bool,
    pub min_tombstone_age_nanos: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_compactions: Option<u64>,
    pub scanned_tombstone_count: u64,
    pub eligible_tombstone_count: u64,
    pub compacted_tombstone_count: u64,
    pub retained_young_tombstone_count: u64,
    /// Earliest wall-clock instant at which a currently young tombstone can
    /// become retention-eligible. This lets reactive schedulers arm one exact
    /// deadline instead of polling the whole metadata set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_eligible_at_unix_nanos: Option<u64>,
    pub retained_referenced_tombstone_count: u64,
    pub retained_limit_tombstone_count: u64,
    pub removed_object_record_count: u64,
    pub removed_inode_record_count: u64,
    pub removed_file_system_object_record_count: u64,
    pub preserved_path_marker_count: u64,
    pub compacted_marker_count: u64,
    pub max_compactions_reached: bool,
}

/// Aggregate shard metadata counters used by status and inspection callers.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataSummary {
    pub file_system_count: usize,
    pub object_count: usize,
    pub version_count: usize,
    pub receipt_count: usize,
    pub intent_count: usize,
    pub pending_write_count: usize,
    pub open_conflict_count: usize,
    pub pending_repair_count: usize,
    pub pending_namespace_job_count: usize,
    pub failed_namespace_job_count: usize,
    /// Unique effective `(filesystem, root path)` cross-shard namespace locks
    /// retained by running durable namespace jobs until they become terminal.
    #[serde(default)]
    pub cross_shard_namespace_lock_count: usize,
}

/// Exact durable routed-request identity which originated a namespace job.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileRestoreJobState {
    pub request: crate::DfsFileHistoryRestoreRequest,
    pub requester_node_id: NodeId,
    pub options: crate::DfsWriteOptions,
    pub write_operation_id: String,
    pub planned: crate::DfsFileVersionIdentity,
    pub source_pin: Option<crate::DfsFileHistoryPinRequest>,
    pub source_pin_applied: bool,
    pub content_hash: Option<String>,
    pub content_size_bytes: Option<u64>,
    pub result: Option<DfsFileRestoreJobResult>,
    pub cleanup_complete: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum DfsFileRestoreJobResult {
    Applied {
        response: crate::DfsFileHistoryRestoreResponse,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileRestoreJobReceipt {
    pub request: crate::DfsFileHistoryRestoreRequest,
    pub requester_node_id: NodeId,
    pub result: DfsFileRestoreJobResult,
}

/// Exact durable routed-request identity which originated a namespace job.
///
/// Jobs created outside routed request execution, and jobs persisted by older
/// versions, omit this identity. Recovery may use the legacy path/time match
/// for those rows, but new routed jobs can be tied to one exact admission.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct DfsNamespaceJobOrigin {
    pub requester_node_id: NodeId,
    pub request_id: String,
    pub operation_id: String,
}

impl DfsNamespaceJobOrigin {
    pub(crate) fn from_request(request: &DfsNamespaceRequestRecord) -> Self {
        Self {
            requester_node_id: request.requester_node_id.clone(),
            request_id: request.request_id.clone(),
            operation_id: request.operation_id.clone(),
        }
    }

    pub(crate) fn matches_request(&self, request: &DfsNamespaceRequestRecord) -> bool {
        self == &Self::from_request(request)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceJobRecord {
    pub job_id: String,
    pub file_system_id: DfsFileSystemId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<DfsNamespaceJobOrigin>,
    pub kind: DfsNamespaceJobKind,
    pub status: DfsNamespaceJobStatus,
    pub root_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_frames: Vec<DfsNamespaceJobFrame>,
    pub processed_object_count: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "time::serde::rfc3339::option")]
    pub completed_at: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

/// Compact terminal evidence for a collected namespace job.
///
/// Pending frames and progress counters intentionally remain only on the full
/// job record while it can still run. This receipt preserves enough identity
/// for exact routed-request recovery and conservative legacy path matching.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNamespaceJobReceipt {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_restore: Option<Box<DfsFileRestoreJobReceipt>>,
    pub job_id: String,
    pub file_system_id: DfsFileSystemId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<DfsNamespaceJobOrigin>,
    pub kind: DfsNamespaceJobKind,
    pub status: DfsNamespaceJobStatus,
    pub root_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_path: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub terminal_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub retained_at: OffsetDateTime,
}

impl DfsNamespaceJobReceipt {
    pub(crate) fn from_job(
        job: &DfsNamespaceJobRecord,
        retained_at: OffsetDateTime,
    ) -> Option<Self> {
        if job.status == DfsNamespaceJobStatus::Running {
            return None;
        }
        Some(Self {
            job_id: job.job_id.clone(),
            file_restore: job.pending_frames.iter().find_map(|frame| match frame {
                DfsNamespaceJobFrame::FileHistoryRestore { state } => {
                    state.result.clone().map(|result| {
                        Box::new(DfsFileRestoreJobReceipt {
                            request: state.request.clone(),
                            requester_node_id: state.requester_node_id.clone(),
                            result,
                        })
                    })
                }
                _ => None,
            }),
            file_system_id: job.file_system_id.clone(),
            origin: job.origin.clone(),
            kind: job.kind.clone(),
            status: job.status.clone(),
            root_path: job.root_path.clone(),
            target_path: job.target_path.clone(),
            created_at: job.created_at,
            terminal_at: job.completed_at.unwrap_or(job.updated_at),
            retained_at,
        })
    }

    pub(crate) fn matches_terminal_job(&self, job: &DfsNamespaceJobRecord) -> bool {
        Self::from_job(job, self.retained_at).as_ref() == Some(self)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNamespaceJobKind {
    FileHistoryRestore,
    ClearDirectoryRecursive,
    RenameDirectory,
    RenameObjectAcrossShardPrimaries,
    HardlinkAcrossShardPrimaries,
    /// Source-side coordinator for removing or moving a hardlink alias whose
    /// canonical inode is owned by another metadata authority.
    HardlinkAliasMutationAcrossShardPrimaries,
    /// Target-side durable read/write fence held while the alias authority
    /// commits its exact namespace generation.
    HardlinkTargetFenceAcrossShardPrimary,
    RenameDirectoryAcrossShardPrimaries,
}

impl DfsNamespaceJobKind {
    pub(crate) fn holds_cross_shard_namespace_lock_while_running(&self) -> bool {
        matches!(
            self,
            Self::RenameObjectAcrossShardPrimaries
                | Self::HardlinkAcrossShardPrimaries
                | Self::HardlinkAliasMutationAcrossShardPrimaries
                | Self::HardlinkTargetFenceAcrossShardPrimary
                | Self::RenameDirectoryAcrossShardPrimaries
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNamespaceJobStatus {
    Running,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DfsNamespaceJobFrame {
    FileHistoryRestore {
        state: Box<DfsFileRestoreJobState>,
    },
    ScanDirectory {
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<DfsDirectoryPageCursor>,
    },
    CompleteDirectory {
        path: String,
    },
    RenameDirectoryStart {
        source_object_id: DfsObjectId,
    },
    RenameDirectoryScan {
        source_object_id: DfsObjectId,
        source_path: String,
        target_path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<DfsDirectoryPageCursor>,
    },
    RenameDirectoryComplete {
        source_object_id: DfsObjectId,
        source_path: String,
        target_path: String,
    },
    /// Cross-shard-primary move: ask the destination-shard primary to adopt
    /// the moved object record at its new path.
    CrossShardAdopt {
        moved_object: DfsObjectMetadata,
        old_path: String,
        destination_primary_node_id: NodeId,
    },
    /// Cross-shard-primary move: locally tombstone the old path, evict the
    /// moved record from the source shard, and touch the source parent.
    CrossShardComplete {
        moved_object: DfsObjectMetadata,
        old_path: String,
    },
    /// Cross-shard-primary hardlink: ask the link-path shard primary to adopt
    /// the alias record.
    CrossShardHardlinkAdopt {
        alias: DfsObjectMetadata,
        target_object_id: DfsObjectId,
        destination_primary_node_id: NodeId,
    },
    /// Cross-shard-primary hardlink: locally record the target's link-count
    /// change (ctime bump).
    CrossShardHardlinkComplete {
        target_object_id: DfsObjectId,
    },
    /// Install the target authority's durable fence before exposing any alias
    /// mutation. The exact target generations are carried by the fence job.
    CrossShardHardlinkTargetFenceInstall {
        fence: Box<DfsNamespaceJobRecord>,
        source_path: String,
    },
    /// Commit the exact alias-shard transaction while both authorities reject
    /// reads and mutations for the affected paths.
    CrossShardHardlinkAliasCommit {
        transaction: DfsNamespaceTransaction,
    },
    /// Commit the pre-minted canonical generation and release its durable
    /// fence. Retrying this frame is exact and idempotent.
    CrossShardHardlinkTargetFenceCommit {
        fence_job_id: String,
        source_path: String,
        expected_target: DfsNamespaceObjectPrecondition,
        committed_target: DfsObjectMetadata,
    },
    /// Marker retained inside a target-side fence job. Maintenance must never
    /// execute it independently; only the authenticated source coordinator can
    /// commit or release this reservation.
    HardlinkTargetFenceReservation {
        source_path: String,
        expected_target: DfsNamespaceObjectPrecondition,
        committed_target: DfsObjectMetadata,
    },
    /// Cross-shard-primary directory rename: move the root directory record
    /// (destination primary adopts it, source primary tombstones the old
    /// path and evicts the record).
    CrossShardDirStart {
        source_object_id: DfsObjectId,
        source_root: String,
        target_root: String,
    },
    /// Cross-shard-primary directory rename: page the children of one moved
    /// directory, adopting each at its target path and evicting the source
    /// record; subdirectories push their own scan frames (depth first).
    CrossShardDirScan {
        source_path: String,
        target_path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<DfsDirectoryPageCursor>,
    },
    /// Cross-shard-primary directory rename: record the source parent's
    /// mtime change once the whole subtree has moved.
    CrossShardDirFinish {
        source_root: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsVisibleVersionCommitRecord {
    pub(crate) expected_version: DfsVersion,
    pub(crate) committed_object: DfsObjectMetadata,
    pub(crate) visible_version: DfsObjectVersionRecord,
    pub(crate) required_replica_node_ids: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) operation_id: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub(crate) created_at: OffsetDateTime,
}

/// Compact materialized metadata table snapshot.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataTableSnapshot {
    pub(crate) format_version: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) materialized_through_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) tables: Option<Box<DfsMetadataTableSnapshotTables>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) ops: Vec<DfsMetadataTableOp>,
    /// Exact semantic identity of the snapshot payload, excluding these proof
    /// fields. Snapshot install validates it before adopting `prefix_proof`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) semantic_state_fingerprint: Option<String>,
    /// Journal-chain head carried across table compaction and snapshot repair.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) prefix_proof: Option<DfsMetadataPrefixProof>,
}

/// Return a content-addressed identity for one complete materialized shard
/// checkpoint. The materialization sequence is intentionally part of the
/// digest: a durability proof is for the exact state at one exact journal
/// head, not merely for a set of currently equivalent namespace records.
pub(crate) fn dfs_metadata_table_snapshot_state_fingerprint(
    snapshot: &DfsMetadataTableSnapshot,
) -> Result<String, serde_json::Error> {
    #[cfg(test)]
    DFS_METADATA_FULL_SNAPSHOT_FINGERPRINT_CALLS.set(
        DFS_METADATA_FULL_SNAPSHOT_FINGERPRINT_CALLS
            .get()
            .saturating_add(1),
    );
    // Keep the historical snapshot encoding stable and avoid a circular hash:
    // proof/checksum fields never contribute to the semantic state digest.
    let mut semantic_snapshot = snapshot.clone();
    semantic_snapshot.semantic_state_fingerprint = None;
    semantic_snapshot.prefix_proof = None;
    let encoded = serde_json::to_vec(&semantic_snapshot)?;
    Ok(format!("blake3:{}", blake3::hash(&encoded).to_hex()))
}

#[cfg(test)]
thread_local! {
    static DFS_METADATA_FULL_SNAPSHOT_FINGERPRINT_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_dfs_metadata_full_snapshot_fingerprint_calls() {
    DFS_METADATA_FULL_SNAPSHOT_FINGERPRINT_CALLS.set(0);
}

#[cfg(test)]
pub(crate) fn dfs_metadata_full_snapshot_fingerprint_calls() -> usize {
    DFS_METADATA_FULL_SNAPSHOT_FINGERPRINT_CALLS.get()
}

pub(crate) fn dfs_metadata_table_snapshot_matches_checkpoint(
    snapshot: &DfsMetadataTableSnapshot,
    journal_sequence: u64,
    expected_fingerprint: &str,
) -> Result<bool, serde_json::Error> {
    if let Some(expected_proof) =
        DfsMetadataPrefixProof::from_digest(journal_sequence, expected_fingerprint.to_string())
    {
        return Ok(snapshot
            .prefix_proof
            .as_ref()
            .is_some_and(|proof| proof.is_well_formed() && proof == &expected_proof));
    }
    Ok(dfs_metadata_table_snapshot_state_fingerprint(snapshot)? == expected_fingerprint)
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsMetadataTableSnapshotTables {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) file_systems: Vec<DfsFileSystem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) objects: Vec<DfsObjectMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) file_system_objects: Vec<DfsPathRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) paths: Vec<DfsPathRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) inodes: Vec<DfsInodeRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) dirfrags: Vec<DfsDirFragmentRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) dentries: Vec<DfsPathRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) hardlinks: Vec<DfsHardlinkIndexSnapshotRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) compacted_tombstones: Vec<DfsObjectMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) versions: Vec<DfsObjectVersionRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) version_pins: Vec<DfsMetadataVersionPinRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) cross_shard_pin_transfers: Vec<DfsCrossShardPinTransferRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) receipts: Vec<DfsReplicaReceiptRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) intents: Vec<DfsWriteIntentRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) intent_receipts: Vec<DfsWriteIntentReceipt>,
    #[serde(
        default,
        skip_serializing_if = "DfsWriteIntentReceiptRetentionState::is_empty"
    )]
    pub(crate) intent_receipt_retention: DfsWriteIntentReceiptRetentionState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) conflicts: Vec<DfsConflictRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) advisory_lock_states: Vec<DfsAdvisoryLockStateRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) visible_commits: Vec<DfsVisibleCommitSnapshotRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) namespace_transactions: Vec<DfsNamespaceTransaction>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) namespace_transaction_receipts: Vec<DfsNamespaceTransactionReceipt>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) namespace_prepares: Vec<DfsNamespacePrepareRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) namespace_prepare_receipts: Vec<DfsNamespacePrepareReceipt>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) namespace_jobs: Vec<DfsNamespaceJobRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) namespace_job_receipts: Vec<DfsNamespaceJobReceipt>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) namespace_request_receipts: Vec<DfsNamespaceRequestReceipt>,
    #[serde(
        default,
        skip_serializing_if = "DfsNamespaceReceiptRetentionState::is_empty"
    )]
    pub(crate) namespace_receipt_retention: DfsNamespaceReceiptRetentionState,
    /// Snapshots written before routed namespace requests omit this table.
    #[serde(default)]
    pub(crate) namespace_requests: Vec<DfsNamespaceRequestRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsHardlinkIndexSnapshotRecord {
    pub(crate) target_object_id: DfsObjectId,
    pub(crate) hardlink_object_id: DfsObjectId,
    pub(crate) path_record: DfsPathRecord,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsVisibleCommitSnapshotRecord {
    pub(crate) object_id: DfsObjectId,
    pub(crate) version: DfsVersion,
    pub(crate) commit: DfsVisibleVersionCommitRecord,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsMetadataTableBatch {
    pub(crate) sequence: u64,
    pub(crate) ops: Vec<DfsMetadataTableOp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) prefix_proof: Option<DfsMetadataPrefixProof>,
    /// Estimated heap/decode work required to expand authoritative journal
    /// references in this rebuildable table batch.
    ///
    /// This is deliberately an estimate rather than a second encoding of the
    /// referenced payload. Older batches decode as zero; replay treats a
    /// zero-cost journal reference as unpriced and checkpoints it
    /// conservatively.
    #[serde(default, skip_serializing_if = "metadata_table_replay_cost_is_zero")]
    pub(crate) journal_reference_replay_cost_bytes: u64,
}

fn metadata_table_replay_cost_is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub(crate) enum DfsMetadataTableOp {
    PutFileSystem {
        file_system: DfsFileSystem,
    },
    PutObject {
        object: DfsObjectMetadata,
    },
    RemoveObject {
        object_id: DfsObjectId,
    },
    PutFileSystemObject {
        path_record: DfsPathRecord,
    },
    RemoveFileSystemObject {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
    },
    PutPath {
        path_record: DfsPathRecord,
    },
    RemovePath {
        file_system_id: DfsFileSystemId,
        path: String,
    },
    PutInode {
        inode: DfsInodeRecord,
    },
    RemoveInode {
        inode_id: DfsInodeId,
    },
    PutDirFragment {
        dirfrag: DfsDirFragmentRecord,
    },
    PutDentry {
        path_record: DfsPathRecord,
    },
    RemoveDentry {
        parent_inode_id: DfsInodeId,
        dirfrag_id: DfsDirFragmentId,
        name_hash: u64,
        name: String,
    },
    PutHardlink {
        target_object_id: DfsObjectId,
        hardlink_object_id: DfsObjectId,
        path_record: DfsPathRecord,
    },
    RemoveHardlink {
        target_object_id: DfsObjectId,
        hardlink_object_id: DfsObjectId,
    },
    PutCompactedTombstone {
        object: DfsObjectMetadata,
    },
    RemoveCompactedTombstonePath {
        file_system_id: DfsFileSystemId,
        path: String,
    },
    PutVersion {
        version: DfsObjectVersionRecord,
    },
    RemoveVersion {
        object_id: DfsObjectId,
        version: DfsVersion,
    },
    PutVersionPin {
        pin: DfsMetadataVersionPinRecord,
    },
    RemoveVersionPin {
        object_id: DfsObjectId,
        version: DfsVersion,
        pin_id: String,
    },
    ReconcileCrossShardPinTransfer {
        versions: Vec<DfsObjectVersionRecord>,
        transfer: DfsCrossShardPinTransferRecord,
    },
    RemoveCrossShardPinTransfer {
        object_id: DfsObjectId,
        transfer_id: String,
    },
    FinalizeCrossShardSourceEviction {
        tombstone: DfsObjectMetadata,
        removed_pins: Vec<DfsMetadataVersionPinRecord>,
        removed_pin_transfers: Vec<DfsCrossShardPinTransferRecord>,
    },
    PutReceipt {
        receipt: DfsReplicaReceiptRecord,
    },
    RemoveReceipt {
        object_id: DfsObjectId,
        version: DfsVersion,
        replica_node_id: NodeId,
    },
    PutIntent {
        intent: DfsWriteIntentRecord,
    },
    RemoveRetainedRepairOwner {
        object_id: DfsObjectId,
        intent_id: String,
    },
    PutIntentReceipt {
        receipt: DfsWriteIntentReceipt,
    },
    RemoveIntentReceipt {
        object_id: DfsObjectId,
        intent_id: String,
        retention: DfsWriteIntentReceiptRetentionState,
    },
    MergeIntentReceiptRetention {
        retention: DfsWriteIntentReceiptRetentionState,
    },
    PutConflict {
        conflict: DfsConflictRecord,
    },
    PutAdvisoryLockState {
        state: DfsAdvisoryLockStateRecord,
    },
    RemoveConflict {
        object_id: DfsObjectId,
        conflict_id: String,
    },
    PutVisibleCommit {
        object_id: DfsObjectId,
        version: DfsVersion,
        commit: DfsVisibleVersionCommitRecord,
    },
    RemoveVisibleCommit {
        object_id: DfsObjectId,
        version: DfsVersion,
    },
    PutNamespaceTransaction {
        transaction: DfsNamespaceTransaction,
    },
    PutNamespaceTransactionReceipt {
        receipt: DfsNamespaceTransactionReceipt,
    },
    RemoveNamespaceTransactionReceipt {
        transaction_id: String,
        retention: DfsNamespaceReceiptRetentionState,
    },
    PutNamespacePrepare {
        prepare: Box<DfsNamespacePrepareRecord>,
    },
    /// Rebuildable materialized-table reference to the authoritative
    /// `PutNamespacePrepare` journal entry at `journal_sequence`.
    ///
    /// The full prepare body remains in the journal and in structured table
    /// snapshots. Table-log replay must resolve and validate this reference
    /// before applying it; it is never meaningful as a standalone table
    /// mutation.
    PutNamespacePrepareJournalReference {
        shard_id: DfsMetadataShardId,
        journal_sequence: u64,
        file_system_id: DfsFileSystemId,
        operation_id: String,
        operation_digest: String,
        owner_epoch: u64,
        status: DfsNamespacePrepareStatus,
    },
    /// Compact materialized-table transition for a prepare whose full semantic
    /// body is already present in the preceding table state. Replaying this
    /// marker expands the transaction and terminal request evidence directly
    /// into the in-memory indexes without serializing thousands of derived ops.
    CommitNamespacePrepare {
        operation_id: String,
        operation_digest: String,
        owner_epoch: u64,
        /// Deterministic terminal-window start copied from the authoritative
        /// commit journal entry. Legacy table markers omit it and fall back to
        /// the prepared transaction timestamp.
        #[serde(
            default,
            with = "time::serde::rfc3339::option",
            skip_serializing_if = "Option::is_none"
        )]
        retained_at: Option<OffsetDateTime>,
    },
    PutNamespacePrepareReceipt {
        receipt: DfsNamespacePrepareReceipt,
    },
    RemoveNamespacePrepareReceipt {
        operation_id: String,
        retention: DfsNamespaceReceiptRetentionState,
    },
    PutNamespaceJob {
        job: DfsNamespaceJobRecord,
    },
    PutNamespaceJobReceipt {
        receipt: DfsNamespaceJobReceipt,
    },
    RemoveNamespaceJobReceipt {
        job_id: String,
        retention: DfsNamespaceReceiptRetentionState,
    },
    PutNamespaceRequest {
        request: Box<DfsNamespaceRequestRecord>,
    },
    PutNamespaceRequestReceipt {
        receipt: Box<DfsNamespaceRequestReceipt>,
    },
    RemoveNamespaceRequestReceipt {
        requester_node_id: NodeId,
        request_id: String,
        retention: DfsNamespaceReceiptRetentionState,
    },
}

impl DfsMetadataTableOp {
    pub(crate) fn put_namespace_prepare(prepare: DfsNamespacePrepareRecord) -> Self {
        Self::PutNamespacePrepare {
            prepare: Box::new(prepare),
        }
    }

    pub(crate) fn put_namespace_request(request: DfsNamespaceRequestRecord) -> Self {
        Self::PutNamespaceRequest {
            request: Box::new(request),
        }
    }

    pub(crate) fn put_namespace_request_receipt(receipt: DfsNamespaceRequestReceipt) -> Self {
        Self::PutNamespaceRequestReceipt {
            receipt: Box::new(receipt),
        }
    }
}

impl DfsTombstoneCompactionReport {
    pub(crate) fn for_options(options: &DfsTombstoneCompactionOptions) -> Self {
        Self {
            dry_run: options.dry_run,
            min_tombstone_age_nanos: options.min_tombstone_age_nanos,
            max_compactions: options.max_compactions,
            ..Self::default()
        }
    }

    pub(crate) fn merge(&mut self, other: DfsTombstoneCompactionReport) {
        self.scanned_tombstone_count = self
            .scanned_tombstone_count
            .saturating_add(other.scanned_tombstone_count);
        self.eligible_tombstone_count = self
            .eligible_tombstone_count
            .saturating_add(other.eligible_tombstone_count);
        self.compacted_tombstone_count = self
            .compacted_tombstone_count
            .saturating_add(other.compacted_tombstone_count);
        self.retained_young_tombstone_count = self
            .retained_young_tombstone_count
            .saturating_add(other.retained_young_tombstone_count);
        self.next_eligible_at_unix_nanos = self
            .next_eligible_at_unix_nanos
            .into_iter()
            .chain(other.next_eligible_at_unix_nanos)
            .min();
        self.retained_referenced_tombstone_count = self
            .retained_referenced_tombstone_count
            .saturating_add(other.retained_referenced_tombstone_count);
        self.retained_limit_tombstone_count = self
            .retained_limit_tombstone_count
            .saturating_add(other.retained_limit_tombstone_count);
        self.removed_object_record_count = self
            .removed_object_record_count
            .saturating_add(other.removed_object_record_count);
        self.removed_inode_record_count = self
            .removed_inode_record_count
            .saturating_add(other.removed_inode_record_count);
        self.removed_file_system_object_record_count = self
            .removed_file_system_object_record_count
            .saturating_add(other.removed_file_system_object_record_count);
        self.preserved_path_marker_count = self
            .preserved_path_marker_count
            .saturating_add(other.preserved_path_marker_count);
        self.compacted_marker_count = self
            .compacted_marker_count
            .saturating_add(other.compacted_marker_count);
        self.max_compactions_reached |= other.max_compactions_reached;
    }
}

fn duration_nanos_saturating(duration: Duration) -> u64 {
    duration.as_nanos().try_into().unwrap_or(u64::MAX)
}

pub(crate) fn metadata_journal_batch_file_system_id(
    batch: &DfsMetadataJournalBatch,
) -> Option<DfsFileSystemId> {
    batch
        .entries
        .first()
        .map(|entry| entry.mutation.file_system_id().clone())
}

pub(crate) fn read_conflicts_reason(conflicts: Vec<DfsConflictReadView>) -> String {
    conflicts
        .into_iter()
        .map(|conflict| format!("{}: {}", conflict.conflict_id, conflict.reason))
        .collect::<Vec<_>>()
        .join("; ")
}

pub(crate) fn dfs_now_unix_nanos() -> u64 {
    OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .try_into()
        .unwrap_or(1_000_000_000)
}

pub(crate) fn object_version_record(
    object: &DfsObjectMetadata,
    size_bytes: Option<u64>,
) -> DfsObjectVersionRecord {
    let mut locations = object.current_locations.clone();
    normalize_current_locations(&mut locations);
    DfsObjectVersionRecord {
        content_durability: None,
        history_order: 0,
        retained: None,
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone(),
        size_bytes: size_bytes.or(object.content_size_bytes),
        locations,
        created_by_event_id: object.last_modified_by_event_id.clone(),
        created_at: OffsetDateTime::now_utc(),
        visible: true,
    }
}

pub(crate) fn normalized_object_metadata(object: &DfsObjectMetadata) -> DfsObjectMetadata {
    let mut object = object.clone();
    normalize_object_metadata_in_place(&mut object);
    object
}

fn normalize_object_metadata_in_place(object: &mut DfsObjectMetadata) {
    normalize_node_locations(&mut object.current_locations);
    if object.kind != crate::DfsObjectKind::Tombstone {
        object.tombstone_provenance = None;
    }
}

pub(crate) fn normalized_version_record(
    version: &DfsObjectVersionRecord,
) -> DfsObjectVersionRecord {
    let mut version = version.clone();
    normalize_node_locations(&mut version.locations);
    version
}

pub(crate) fn normalized_namespace_transaction(
    transaction: &DfsNamespaceTransaction,
) -> DfsNamespaceTransaction {
    let mut transaction = transaction.clone();
    normalize_namespace_transaction_in_place(&mut transaction);
    transaction
}

pub(crate) fn normalize_namespace_transaction_in_place(transaction: &mut DfsNamespaceTransaction) {
    for object in &mut transaction.objects {
        normalize_object_metadata_in_place(object);
    }
    for version in &mut transaction.versions {
        normalize_node_locations(&mut version.locations);
    }
    for state in &mut transaction.advisory_lock_states {
        state.authority_rebind_required = true;
    }
}

pub(crate) fn normalized_namespace_prepare(
    prepare: &DfsNamespacePrepareRecord,
) -> DfsNamespacePrepareRecord {
    let mut prepare = prepare.clone();
    normalize_namespace_prepare_in_place(&mut prepare);
    prepare
}

/// Canonicalize an owned namespace prepare without cloning its transaction.
///
/// Large local-eventual batches already transfer ownership of their prepare to
/// the metadata authority. Keeping the in-place form separate lets that hot
/// path compute the canonical digest, journal payload, and materialized table
/// state from one object graph instead of cloning thousands of objects and
/// versions solely to normalize them.
pub(crate) fn normalize_namespace_prepare_in_place(prepare: &mut DfsNamespacePrepareRecord) {
    normalize_namespace_transaction_in_place(&mut prepare.transaction);
    for expected in &mut prepare.expected_path_occupants {
        expected.path = crate::metadata_keys::normalize_path(&expected.path);
    }
    prepare.expected_object_versions.sort();
    prepare.expected_path_occupants.sort();
    prepare.content_receipts.sort_by(|left, right| {
        (
            &left.receipt.object_id,
            &left.receipt.committed_version,
            &left.replica_node_id,
        )
            .cmp(&(
                &right.receipt.object_id,
                &right.receipt.committed_version,
                &right.replica_node_id,
            ))
    });
}

pub(crate) fn put_visible_version_record_if_changed(
    backend: &mut dyn DfsMetadataBackend,
    mut version_record: DfsObjectVersionRecord,
) -> Result<bool, DfsError> {
    if let Some(existing) =
        backend.get_version(&version_record.object_id, &version_record.version)?
    {
        version_record.created_at = existing.created_at;
        version_record.retained = existing.retained.clone();
        version_record.content_durability = existing.content_durability.clone();
        version_record.history_order = existing.history_order;
        if existing == version_record {
            return Ok(false);
        }
    }
    backend.put_version(&version_record)?;
    Ok(true)
}

pub(crate) fn remote_read_rejection_prunes_location(reason: &str) -> bool {
    reason.contains("is not a current location for DFS object")
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::DfsWriteReceipt;

    #[test]
    fn metadata_mutation_stack_footprint_stays_bounded() {
        // Journal mutations cross several by-value Result boundaries. Keeping
        // large namespace bodies behind pointers prevents every small journal
        // or table operation from reserving space for the largest variant.
        assert!(
            std::mem::size_of::<DfsMetadataMutation>() <= 1024,
            "DfsMetadataMutation grew to {} bytes",
            std::mem::size_of::<DfsMetadataMutation>()
        );
        assert!(
            std::mem::size_of::<DfsMetadataJournalEntry>() <= 4 * 1024,
            "DfsMetadataJournalEntry grew to {} bytes",
            std::mem::size_of::<DfsMetadataJournalEntry>()
        );
        assert!(
            std::mem::size_of::<DfsMetadataTableOp>() <= 1024,
            "DfsMetadataTableOp grew to {} bytes",
            std::mem::size_of::<DfsMetadataTableOp>()
        );
        assert!(
            std::mem::size_of::<DfsMetadataTableSnapshot>() <= 256,
            "DfsMetadataTableSnapshot grew to {} bytes",
            std::mem::size_of::<DfsMetadataTableSnapshot>()
        );
        assert!(
            std::mem::size_of::<crate::DfsMetadataSnapshotInstallCommand>() <= 512,
            "DfsMetadataSnapshotInstallCommand grew to {} bytes",
            std::mem::size_of::<crate::DfsMetadataSnapshotInstallCommand>()
        );
    }

    fn prefix_test_entry(sequence: u64) -> DfsMetadataJournalEntry {
        DfsMetadataJournalEntry {
            shard_id: DfsMetadataShardId::new("services"),
            sequence,
            created_at: OffsetDateTime::UNIX_EPOCH,
            mutation: DfsMetadataMutation::PutFileSystem {
                file_system_id: DfsFileSystemId::from_legacy_named("sifs-prefix-proof-test")
                    .expect("filesystem id"),
                file_system: None,
            },
        }
    }

    #[test]
    fn legacy_prefix_proof_encoding_remains_v1_json_compatible() {
        let seed = dfs_metadata_prefix_proof_seed(
            &DfsMetadataShardId::new("services"),
            0,
            "semantic-state",
        );
        let entry = prefix_test_entry(1);
        let proof = dfs_metadata_prefix_proof_after(&seed, &entry).expect("legacy proof");

        assert_eq!(proof.version, DFS_METADATA_PREFIX_PROOF_VERSION);
        assert!(proof.is_well_formed());
        assert_eq!(
            proof.digest,
            "dfs-metadata-prefix-v1:blake3:9a12f641231be2f7a1986dd2282b4b5e59820e230b267b631507b77c5cf762ae"
        );
        assert_eq!(
            serde_json::from_value::<DfsMetadataPrefixProof>(
                serde_json::to_value(&proof).expect("encode legacy proof")
            )
            .expect("decode legacy proof"),
            proof
        );
    }

    #[test]
    fn exact_frame_prefix_proof_is_byte_sensitive_and_chains_from_v1() {
        let seed = dfs_metadata_prefix_proof_seed(
            &DfsMetadataShardId::new("services"),
            0,
            "semantic-state",
        );
        let first_bytes = b"exact-sobj-frame";
        let first = dfs_metadata_prefix_proof_v2_after_bytes(&seed, 1, first_bytes);
        let same = dfs_metadata_prefix_proof_v2_after_bytes(&seed, 1, first_bytes);
        let changed = dfs_metadata_prefix_proof_v2_after_bytes(&seed, 1, b"exact-sobj-framf");

        assert_eq!(first, same);
        assert_ne!(first.digest, changed.digest);
        assert_eq!(first.version, DFS_METADATA_PREFIX_PROOF_EXACT_FRAME_VERSION);
        assert!(first.is_well_formed());

        let second = dfs_metadata_prefix_proof_v2_after_bytes(&first, 2, b"next-frame");
        assert_eq!(second.journal_sequence, 2);
        assert!(second.is_well_formed());

        let mut wrong_prefix = second.clone();
        wrong_prefix.digest =
            wrong_prefix
                .digest
                .replacen("dfs-metadata-prefix-v2", "dfs-metadata-prefix-v1", 1);
        assert!(!wrong_prefix.is_well_formed());
    }

    #[test]
    fn namespace_prepare_key_round_trips_exact_journal_operation_provenance() {
        let caller_operation_id = "cf:client-ümlaut_01k00000000000000000000000";
        let first = namespace_prepare_operation_id_with_journal_provenance(
            caller_operation_id,
            "transaction-one",
        );
        let same = namespace_prepare_operation_id_with_journal_provenance(
            caller_operation_id,
            "transaction-one",
        );
        let other = namespace_prepare_operation_id_with_journal_provenance(
            caller_operation_id,
            "transaction-two",
        );

        assert_eq!(first, same);
        assert_ne!(first, other);
        assert_eq!(
            namespace_prepare_journal_operation_id(&first),
            Some(caller_operation_id)
        );
        assert_eq!(
            namespace_prepare_journal_operation_id("namespace-journal-provenance-v1:5:short"),
            None
        );
    }

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("sifs").expect("valid filesystem id")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::from_legacy_named("obj_mutation").expect("valid object id")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("valid version")
    }

    fn object(object_version: DfsVersion) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: object_id(),
            file_system_id: file_system_id(),
            path: "/docs/mutation.md".to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("blake3:mutation".to_string()),
            content_size_bytes: Some(32),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            version: object_version,
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    #[test]
    fn opaque_write_operation_eviction_fails_closed_without_poisoning_sortable_operations() {
        let object_id = object_id();
        let receipt = DfsWriteIntentReceipt {
            publication: Default::default(),
            intent_id: "dfs_write_op_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            file_system_id: file_system_id(),
            object_id: object_id.clone(),
            base_version: Some(version("version_receipt_base")),
            target_version: version("version_receipt_target"),
            coordinator_node_id: node("node_receipt_writer"),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 3,
            desired_replica_node_ids: vec![node("node_receipt_writer")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Committed,
            content_hash: Some("blake3:receipt".to_string()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            terminal_at: OffsetDateTime::UNIX_EPOCH,
            retained_at: OffsetDateTime::UNIX_EPOCH,
        };
        let state = DfsWriteIntentReceiptRetentionState::eviction_delta(&receipt);
        assert!(state.operation_history_expired(
            &object_id,
            &receipt.coordinator_node_id,
            &receipt.intent_id,
            None,
        ));
        assert!(state.operation_history_expired(
            &object_id,
            &receipt.coordinator_node_id,
            "dfs_write_op_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            None,
        ));
        assert!(
            !state.operation_history_expired(
                &object_id,
                &receipt.coordinator_node_id,
                "dfs_write_op_01j00000000000000000000000",
                Some("01j00000000000000000000000"),
            ),
            "legacy opaque eviction must not poison new sortable operation ids"
        );
        assert!(
            state
                .max_evicted_counter_by_object_and_coordinator
                .is_empty(),
            "stable operation ids must not pollute the generated counter floor"
        );
        assert_eq!(
            serde_json::from_slice::<DfsWriteIntentReceiptRetentionState>(
                &serde_json::to_vec(&state).expect("encode retention")
            )
            .expect("decode retention"),
            state
        );

        let legacy: DfsWriteIntentReceiptRetentionState =
            serde_json::from_value(serde_json::json!({
                "opaque_intent_history_evicted_object_ids": [object_id.clone()]
            }))
            .expect("decode rolling-upgrade retention");
        assert!(legacy.operation_history_expired(
            &object_id,
            &node("node_receipt_writer"),
            "dfs_write_op_any_unknown_legacy_id",
            None,
        ));
        assert!(!legacy.operation_history_expired(
            &object_id,
            &node("node_receipt_writer"),
            "dfs_write_op_01j00000000000000000000000",
            Some("01j00000000000000000000000"),
        ));
    }

    #[test]
    fn sortable_write_receipt_high_churn_reopens_as_one_bounded_floor() {
        let object_id = object_id();
        let coordinator = node("node_sortable_receipt_writer");
        let mut state = DfsWriteIntentReceiptRetentionState::default();
        for index in 0..4096_u64 {
            let token = format!("00000000000000000000{index:06}");
            let receipt = DfsWriteIntentReceipt {
                publication: Default::default(),
                intent_id: format!("dfs_write_op_{token}"),
                file_system_id: file_system_id(),
                object_id: object_id.clone(),
                base_version: None,
                target_version: version("version_sortable_receipt_target"),
                coordinator_node_id: coordinator.clone(),
                operation_id: Some(format!("cf_{token}")),
                operation_order_token: Some(token),
                owner_epoch: 1,
                desired_replica_node_ids: vec![coordinator.clone()],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                status: DfsWriteIntentStatus::Committed,
                content_hash: Some("blake3:sortable-receipt".to_string()),
                touch_parent_on_create: false,
                create_predecessor_object_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                terminal_at: OffsetDateTime::UNIX_EPOCH,
                retained_at: OffsetDateTime::UNIX_EPOCH,
            };
            state.merge_conservative(DfsWriteIntentReceiptRetentionState::eviction_delta(
                &receipt,
            ));
        }
        assert!(state.evicted_opaque_intent_ids_by_object.is_empty());
        assert!(state.opaque_intent_history_evicted_object_ids.is_empty());
        assert_eq!(
            state
                .max_evicted_operation_order_token_by_object_and_coordinator
                .get(&object_id)
                .map(BTreeMap::len),
            Some(1),
        );

        let reopened: DfsWriteIntentReceiptRetentionState =
            serde_json::from_slice(&serde_json::to_vec(&state).expect("persist sortable floor"))
                .expect("reopen sortable floor");
        let floor = format!("00000000000000000000{:06}", 4095);
        assert!(reopened.operation_history_expired(
            &object_id,
            &coordinator,
            &format!("dfs_write_op_{floor}"),
            Some(&floor),
        ));
        let next = format!("00000000000000000000{:06}", 4096);
        assert!(!reopened.operation_history_expired(
            &object_id,
            &coordinator,
            &format!("dfs_write_op_{next}"),
            Some(&next),
        ));
    }

    fn version_record(object: &DfsObjectMetadata) -> DfsObjectVersionRecord {
        DfsObjectVersionRecord {
            content_durability: None,
            history_order: 0,
            retained: None,
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes: object.content_size_bytes,
            locations: object.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        }
    }

    #[test]
    fn namespace_prepare_normalization_is_idempotent_and_does_not_mutate_input() {
        let mut object = object(version("version_normalize_prepare"));
        object.current_locations = vec![
            node("node_z"),
            node("node_a"),
            node("node_z"),
            node("node_owner"),
        ];
        let mut version_record = version_record(&object);
        version_record.locations = object.current_locations.clone();
        let prepare = DfsNamespacePrepareRecord {
            operation_id: "operation-normalize-prepare".to_string(),
            file_system_id: file_system_id(),
            shard_id: DfsMetadataShardId::new("root"),
            owner_epoch: 1,
            operation_digest: "blake3:normalize-prepare".to_string(),
            expected_object_versions: vec![DfsNamespaceObjectVersionExpectation {
                object_id: object.object_id.clone(),
                expected_version: None,
            }],
            expected_path_occupants: vec![
                DfsNamespacePathOccupantExpectation {
                    path: "/docs//z".to_string(),
                    expected_object_id: None,
                },
                DfsNamespacePathOccupantExpectation {
                    path: "/docs/./a".to_string(),
                    expected_object_id: None,
                },
            ],
            content_receipts: Vec::new(),
            transaction: DfsNamespaceTransaction {
                file_version_pins: None,
                transaction_id: "transaction-normalize-prepare".to_string(),
                file_system_id: file_system_id(),
                objects: vec![object],
                versions: vec![version_record],
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            },
            completed_request: None,
            completed_request_completion: None,
            status: DfsNamespacePrepareStatus::Prepared,
        };
        let original = prepare.clone();

        let normalized = normalized_namespace_prepare(&prepare);
        assert_eq!(prepare, original, "normalization must preserve its input");
        assert_eq!(
            normalized_namespace_prepare(&normalized),
            normalized,
            "normalization must be idempotent"
        );
        let expected_locations = vec![node("node_z"), node("node_a"), node("node_owner")];
        assert_eq!(
            normalized.transaction.objects[0].current_locations,
            expected_locations
        );
        assert_eq!(
            normalized.transaction.versions[0].locations,
            expected_locations
        );
        assert_eq!(
            normalized
                .expected_path_occupants
                .iter()
                .map(|expected| expected.path.as_str())
                .collect::<Vec<_>>(),
            vec!["docs/./a", "docs//z"]
        );
    }

    #[test]
    fn mutation_constructors_copy_identity_fields_from_payloads() {
        let object = object(version("version_mutation_1"));
        let version_record = version_record(&object);
        let receipt = DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                committed_version: object.version.clone(),
                owner_node_id: object.owner_node_id.clone(),
                event_id: EventId::generate(),
            },
            replica_node_id: node("node_replica"),
            content_hash: object.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
        };
        let intent = DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: "intent_mutation".to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: None,
            target_version: object.version.clone(),
            coordinator_node_id: node("node_owner"),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 0,
            desired_replica_node_ids: vec![node("node_replica")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Staging,
            content_hash: object.content_hash.clone(),
            object_metadata: Some(object.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: None,
        };

        assert_eq!(
            DfsMetadataMutation::put_object(&object),
            DfsMetadataMutation::PutObject {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                path: object.path.clone(),
                object: Some(object.clone()),
            }
        );
        assert_eq!(
            DfsMetadataMutation::put_version(&version_record),
            DfsMetadataMutation::PutVersion {
                file_system_id: version_record.file_system_id.clone(),
                object_id: version_record.object_id.clone(),
                version: version_record.version.clone(),
                version_record: Some(version_record.clone()),
            }
        );
        assert_eq!(
            DfsMetadataMutation::put_receipt(&receipt),
            DfsMetadataMutation::PutReceipt {
                file_system_id: receipt.receipt.file_system_id.clone(),
                object_id: receipt.receipt.object_id.clone(),
                version: receipt.receipt.committed_version.clone(),
                replica_node_id: receipt.replica_node_id.clone(),
                receipt: Some(receipt.clone()),
            }
        );
        assert_eq!(
            DfsMetadataMutation::put_intent(&intent),
            DfsMetadataMutation::PutIntent {
                file_system_id: intent.file_system_id.clone(),
                object_id: intent.object_id.clone(),
                intent_id: intent.intent_id.clone(),
                status: intent.status.clone(),
                authoritative_migration: false,
                intent: Some(intent),
            }
        );
    }

    #[test]
    fn legacy_write_intent_without_parent_touch_field_defaults_to_raw_semantics() {
        let object = object(version("version_legacy_intent"));
        let intent = DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: "intent_legacy_parent_touch".to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: None,
            target_version: object.version.clone(),
            coordinator_node_id: node("node_owner"),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 0,
            desired_replica_node_ids: vec![node("node_replica")],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::ReceiptsPending,
            content_hash: object.content_hash.clone(),
            object_metadata: Some(object),
            touch_parent_on_create: true,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: None,
        };
        let mut encoded = serde_json::to_value(intent).expect("serialize intent");
        encoded
            .as_object_mut()
            .expect("intent object")
            .remove("touch_parent_on_create");

        let decoded: DfsWriteIntentRecord =
            serde_json::from_value(encoded).expect("deserialize legacy intent");
        assert!(
            !decoded.touch_parent_on_create,
            "legacy raw write intents must not invent POSIX parent effects"
        );
    }

    #[test]
    fn namespace_prepare_defaults_legacy_atomic_commit_fields_but_serializes_them_explicitly() {
        let prepare = DfsNamespacePrepareRecord {
            operation_id: "legacy-namespace-prepare".to_string(),
            file_system_id: file_system_id(),
            shard_id: DfsMetadataShardId::new("shard-root"),
            owner_epoch: 1,
            operation_digest: "blake3:legacy".to_string(),
            expected_object_versions: Vec::new(),
            expected_path_occupants: Vec::new(),
            content_receipts: Vec::new(),
            transaction: DfsNamespaceTransaction {
                file_version_pins: None,
                transaction_id: "legacy-namespace-transaction".to_string(),
                file_system_id: file_system_id(),
                objects: Vec::new(),
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            },
            completed_request: None,
            completed_request_completion: None,
            status: DfsNamespacePrepareStatus::Prepared,
        };
        let mut encoded = serde_json::to_value(&prepare).expect("serialize namespace prepare");
        let encoded = encoded.as_object_mut().expect("namespace prepare object");
        assert_eq!(
            encoded.get("content_receipts"),
            Some(&serde_json::json!([]))
        );
        assert_eq!(
            encoded.get("completed_request"),
            Some(&serde_json::Value::Null)
        );
        assert!(
            encoded.get("completed_request_completion").is_none(),
            "an absent compact completion should not expand legacy prepare payloads"
        );

        let mut legacy = encoded.clone();
        legacy.remove("content_receipts");
        legacy.remove("completed_request");
        legacy.remove("completed_request_completion");
        let decoded = serde_json::from_value::<DfsNamespacePrepareRecord>(
            serde_json::Value::Object(legacy.clone()),
        )
        .expect("deserialize legacy namespace prepare");
        assert!(decoded.content_receipts.is_empty());
        assert!(decoded.completed_request.is_none());
        assert!(decoded.completed_request_completion.is_none());

        let mut legacy_entry = serde_json::to_value(DfsMetadataJournalEntry {
            shard_id: prepare.shard_id.clone(),
            sequence: 1,
            created_at: OffsetDateTime::UNIX_EPOCH,
            mutation: DfsMetadataMutation::put_namespace_prepare(&prepare),
        })
        .expect("serialize namespace prepare journal entry");
        let legacy_entry_prepare = legacy_entry
            .get_mut("mutation")
            .and_then(|mutation| mutation.get_mut("prepare"))
            .and_then(serde_json::Value::as_object_mut)
            .expect("namespace prepare journal payload");
        legacy_entry_prepare.remove("content_receipts");
        legacy_entry_prepare.remove("completed_request");
        legacy_entry_prepare.remove("completed_request_completion");
        let legacy_bytes = sinomo_core::object::serde_ext::to_bytes(
            &legacy_entry,
            1,
            sinomo_core::object::LengthMode::U32,
        )
        .expect("encode legacy namespace prepare journal entry as SOBJ");
        let decoded: DfsMetadataJournalEntry =
            sinomo_core::object::serde_ext::from_bytes(&legacy_bytes)
                .expect("decode legacy namespace prepare journal entry from SOBJ");
        let DfsMetadataMutation::PutNamespacePrepare {
            prepare: Some(decoded),
            ..
        } = decoded.mutation
        else {
            panic!("decoded journal entry should contain namespace prepare payload");
        };
        assert!(decoded.content_receipts.is_empty());
        assert!(decoded.completed_request.is_none());
        assert!(decoded.completed_request_completion.is_none());
    }

    #[test]
    fn ambiguous_namespace_prepare_payload_has_no_prevalidation_affected_object_ids() {
        let prepared_object = object(version("version_ambiguous_prepare"));
        let prepare = DfsNamespacePrepareRecord {
            operation_id: "ambiguous-namespace-prepare".to_string(),
            file_system_id: file_system_id(),
            shard_id: DfsMetadataShardId::new("shard-root"),
            owner_epoch: 1,
            operation_digest: "blake3:ambiguous-namespace-prepare".to_string(),
            expected_object_versions: Vec::new(),
            expected_path_occupants: Vec::new(),
            content_receipts: Vec::new(),
            transaction: DfsNamespaceTransaction {
                file_version_pins: None,
                transaction_id: "ambiguous-namespace-transaction".to_string(),
                file_system_id: file_system_id(),
                objects: vec![prepared_object],
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            },
            completed_request: None,
            completed_request_completion: None,
            status: DfsNamespacePrepareStatus::Prepared,
        };
        let packed = match DfsMetadataMutation::put_namespace_prepare_packed_v1(&prepare)
            .expect("pack namespace prepare")
        {
            DfsMetadataMutation::PutNamespacePrepare {
                packed_prepare_v1: Some(packed),
                ..
            } => packed,
            _ => panic!("packed namespace prepare constructor returned another mutation"),
        };
        let ambiguous = match DfsMetadataMutation::put_namespace_prepare(&prepare) {
            DfsMetadataMutation::PutNamespacePrepare {
                file_system_id,
                operation_id,
                status,
                prepare,
                ..
            } => DfsMetadataMutation::PutNamespacePrepare {
                file_system_id,
                operation_id,
                status,
                prepare,
                packed_prepare_v1: Some(packed),
                packed_prepare_v2: None,
            },
            _ => panic!("namespace prepare constructor returned another mutation"),
        };

        assert!(matches!(
            ambiguous.namespace_prepare_payload_ref(),
            Err(DfsNamespacePrepareCodecError::AmbiguousPayload)
        ));
        assert!(
            ambiguous.affected_object_ids().is_empty(),
            "a dual payload must not publish a partial routing/index object set before replay rejects it"
        );
    }

    #[test]
    fn metadata_table_snapshot_defaults_legacy_namespace_requests_but_serializes_it_explicitly() {
        let decoded: DfsMetadataTableSnapshotTables = serde_json::from_value(serde_json::json!({}))
            .expect("deserialize legacy metadata table snapshot");
        assert!(decoded.namespace_requests.is_empty());
        assert!(decoded.namespace_job_receipts.is_empty());
        assert_eq!(
            serde_json::to_value(decoded)
                .expect("serialize metadata table snapshot")
                .get("namespace_requests"),
            Some(&serde_json::json!([]))
        );
    }

    #[test]
    fn namespace_job_origin_is_backward_compatible_and_terminal_receipt_is_compact() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let request = DfsNamespaceRequestRecord {
            file_system_id: file_system_id(),
            requester_node_id: node("node_namespace_job_requester"),
            request_id: "dfs_net_req_namespace_job_origin".to_string(),
            operation_id: "req-namespace-job-origin".to_string(),
            authority_shard_id: DfsMetadataShardId::new("users"),
            operation: DfsNamespaceOperation::Rename {
                from: "/users/source".to_string(),
                to: "/services/target".to_string(),
            },
            command_digest: "blake3:namespace-job-origin".to_string(),
            status: DfsNamespaceRequestStatus::Pending,
            result: None,
            posix_coordination_result: None,
            recovery_attempts: 0,
            last_recovery_error: None,
            created_at: now,
            updated_at: now,
        };
        let origin = DfsNamespaceJobOrigin::from_request(&request);
        assert!(origin.matches_request(&request));
        let job = DfsNamespaceJobRecord {
            job_id: "job-namespace-origin".to_string(),
            file_system_id: file_system_id(),
            origin: Some(origin.clone()),
            kind: DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
            status: DfsNamespaceJobStatus::Completed,
            root_path: "/users/source".to_string(),
            target_path: Some("/services/target".to_string()),
            pending_frames: vec![DfsNamespaceJobFrame::ScanDirectory {
                path: "/users/source".to_string(),
                cursor: None,
            }],
            processed_object_count: 7,
            created_at: now,
            updated_at: now,
            completed_at: Some(now),
            failure_reason: None,
        };
        let receipt =
            DfsNamespaceJobReceipt::from_job(&job, now).expect("terminal namespace job receipt");
        assert_eq!(receipt.origin, Some(origin));
        assert!(receipt.matches_terminal_job(&job));
        let encoded_receipt = serde_json::to_value(&receipt).expect("encode job receipt");
        assert!(encoded_receipt.get("pending_frames").is_none());
        assert!(encoded_receipt.get("processed_object_count").is_none());

        let mut legacy = serde_json::to_value(job).expect("encode namespace job");
        legacy.as_object_mut().expect("job object").remove("origin");
        let decoded: DfsNamespaceJobRecord =
            serde_json::from_value(legacy).expect("decode pre-origin namespace job");
        assert!(decoded.origin.is_none());
    }

    #[test]
    fn visible_commit_mutation_constructor_uses_commit_record_fields() {
        let committed_object = object(version("version_mutation_2"));
        let visible_version = version_record(&committed_object);
        let commit = DfsVisibleVersionCommitRecord {
            expected_version: version("version_mutation_1"),
            committed_object: committed_object.clone(),
            visible_version: visible_version.clone(),
            required_replica_node_ids: vec![node("node_replica")],
            operation_id: Some("stable-visible-op".to_string()),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };

        assert_eq!(
            DfsMetadataMutation::commit_visible_version(&commit),
            DfsMetadataMutation::CommitVisibleVersion {
                file_system_id: committed_object.file_system_id,
                object_id: committed_object.object_id,
                previous_version: commit.expected_version,
                committed_version: committed_object.version,
                operation_id: Some("stable-visible-op".to_string()),
                committed_object: Some(commit.committed_object),
                visible_version: Some(visible_version),
                required_replica_node_ids: commit.required_replica_node_ids,
            }
        );
    }

    #[test]
    fn namespace_transaction_affected_object_ids_page_covers_512_ids_in_two_pages() {
        let mut objects = Vec::with_capacity(512);
        for index in (0..512).rev() {
            let mut object = object(version("version_paged_namespace_transaction"));
            object.object_id =
                DfsObjectId::from_legacy_named(format!("obj_paged_namespace_{index:04}"))
                    .expect("valid object id");
            object.path = format!("/docs/paged-namespace-{index:04}.md");
            objects.push(object);
        }
        let transaction = DfsNamespaceTransaction {
            file_version_pins: None,
            transaction_id: "transaction_paged_namespace_512".to_string(),
            file_system_id: file_system_id(),
            objects,
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        let mutation = DfsMetadataMutation::ApplyNamespaceTransaction {
            file_system_id: transaction.file_system_id.clone(),
            transaction_id: transaction.transaction_id.clone(),
            object_count: transaction.objects.len(),
            operation_id: None,
            transaction: Some(transaction),
        };
        let expected = mutation
            .affected_object_ids()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(expected.len(), 512);

        let (first_page, first_has_more) = mutation.affected_object_ids_page(None, 256);
        assert_eq!(first_page, expected[..256]);
        assert!(first_has_more);
        assert_eq!(
            mutation.affected_object_ids_page(None, 256),
            (first_page.clone(), true),
            "paging the same durable entry must be deterministic"
        );

        let (second_page, second_has_more) =
            mutation.affected_object_ids_page(first_page.last(), 256);
        assert_eq!(second_page, expected[256..]);
        assert!(!second_has_more);

        let mut paged = first_page;
        paged.extend(second_page);
        assert_eq!(
            paged, expected,
            "paging must neither lose nor duplicate an object ID"
        );
        assert_eq!(paged.into_iter().collect::<BTreeSet<_>>().len(), 512);
    }

    #[test]
    fn zero_object_mutation_has_no_affected_object_id_page() {
        let mutation = DfsMetadataMutation::ApplyNamespaceTransaction {
            file_system_id: file_system_id(),
            transaction_id: "transaction_empty_namespace".to_string(),
            object_count: 0,
            operation_id: None,
            transaction: Some(DfsNamespaceTransaction {
                file_version_pins: None,
                transaction_id: "transaction_empty_namespace".to_string(),
                file_system_id: file_system_id(),
                objects: Vec::new(),
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            }),
        };

        assert_eq!(
            mutation.affected_object_ids_page(None, 256),
            (Vec::new(), false)
        );
        assert_eq!(
            mutation.affected_object_ids_page(None, 0),
            (Vec::new(), false)
        );
    }
}
