//! Durable DFS metadata record and page schemas.
//!
//! These types are the wire/storage-facing metadata API. Backend modules own
//! persistence and coordination; this module owns the data shapes that callers
//! see through the crate-root re-exports.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use time::OffsetDateTime;

use crate::advisory_lock::DfsAdvisoryLockStateRecord;
use crate::metadata_keys::{normalize_node_locations, object_inode_id};
use crate::{
    DfsFileSystem, DfsFileSystemId, DfsNamespaceObjectPrecondition, DfsNamespaceOperation,
    DfsNamespaceResult, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsPosixCoordinationResult,
    DfsReplicationPolicy, DfsRevisionReference, DfsVersion, normalize_current_locations,
};

pub use sinomo_api::dfs::metadata_records::DfsContentDurability;
pub use sinomo_api::dfs::metadata_records::DfsMetadataShardId;

/// Bounded read-only evidence from the owning physical snapshot codec.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataSnapshotReaderFloorEvidence {
    pub format_version: u16,
    pub materialized_through_sequence: Option<u64>,
    pub generation: String,
}

/// Generic authoritative retained-content projection. Feature providers decode
/// only their own version namespace; the core supplies identity and grant roots.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsRetainedRevisionDescriptor {
    pub version: DfsObjectVersionRecord,
    pub current_object: DfsObjectMetadata,
    pub references: Vec<DfsMetadataVersionPinRecord>,
}

/// One exact anchored revision row. This metadata projection carries no
/// reference-completeness evidence and grants no immutable-content access.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsRevisionMetadataDescriptor {
    pub version: DfsObjectVersionRecord,
    pub current_object: DfsObjectMetadata,
}

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
#[serde(deny_unknown_fields)]
pub struct DfsMetadataPrefixAckFloor {
    pub shard_id: DfsMetadataShardId,
    pub journal_sequence: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub state_fingerprint: String,
    pub durable_prefix_proof: DfsMetadataPrefixProof,
}

/// Replica-local durable floor established before acknowledging one exact
/// metadata authority operation.
///
/// Unlike an exact prefix checkpoint, this floor deliberately carries no
/// whole-shard fingerprint: the receiver has already re-read and verified the
/// exact hidden operation named by the probe. Snapshot repair must therefore
/// advance strictly past this sequence before it may replace local state.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataAuthorityAckFloor {
    pub shard_id: DfsMetadataShardId,
    pub journal_sequence: u64,
    /// Exact durable journal prefix acknowledged by the actual receiver.
    pub durable_prefix_proof: DfsMetadataPrefixProof,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, Hash, PartialEq, Ord, PartialOrd)]
#[serde(deny_unknown_fields)]
pub struct DfsInodeId(pub String);

impl DfsInodeId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, Hash, PartialEq, Ord, PartialOrd)]
#[serde(deny_unknown_fields)]
pub struct DfsDirFragmentId(pub String);

impl DfsDirFragmentId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

/// Materialized namespace entry for a path in a DFS filesystem.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct DfsMetadataVersionPinRecord {
    pub namespace: String,
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[derive(Clone, Debug, Serialize, Deserialize)]
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
    pub content_reference_outcomes: Vec<DfsNamespaceTransactionReceipt>,
    pub content_reference_outcome_retention: Option<DfsContentReferenceOutcomeRetention>,
    pub advisory_lock_state: Option<DfsAdvisoryLockStateRecord>,
}

pub use sinomo_api::dfs::metadata_records::DfsReplicaReceiptRecord;

pub use sinomo_api::dfs::metadata_records::DfsWriteIntentRecord;

/// Compact terminal write identity retained after recovery no longer needs the
/// full object body or mutable diagnostics from a write intent.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsWriteIntentReceipt {
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
    pub owner_epoch: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub desired_replica_node_ids: Vec<NodeId>,
    pub policy: DfsReplicationPolicy,
    pub status: DfsWriteIntentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
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
#[serde(deny_unknown_fields)]
pub struct DfsWriteIntentReceiptRetentionState {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub max_evicted_counter_by_object_and_coordinator: BTreeMap<DfsObjectId, BTreeMap<NodeId, u64>>,
    /// High-water marks for current caller-owned stable operation ids. ULIDs
    /// retain lexical time order, so one object/coordinator floor replaces an
    /// unbounded set of exact derived intent ids.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub max_evicted_operation_order_token_by_object_and_coordinator:
        BTreeMap<DfsObjectId, BTreeMap<NodeId, String>>,
    /// Opaque operation history has no sortable boundary. Once collected,
    /// later retries for the object require explicit reconciliation.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub opaque_intent_history_evicted_object_ids: BTreeSet<DfsObjectId>,
}

impl DfsWriteIntentReceiptRetentionState {
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
        self.opaque_intent_history_evicted_object_ids
            .extend(other.opaque_intent_history_evicted_object_ids);
    }

    pub(crate) fn operation_history_expired(
        &self,
        object_id: &DfsObjectId,
        coordinator_node_id: &NodeId,
        _intent_id: &str,
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
#[serde(deny_unknown_fields)]
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

/// Append-only metadata mutation as stored in a shard journal.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct DfsMetadataPrefixProof {
    pub version: u16,
    pub journal_sequence: u64,
    pub digest: String,
}

pub(crate) const DFS_METADATA_PREFIX_PROOF_EXACT_FRAME_VERSION: u16 = 2;
const DFS_METADATA_PREFIX_PROOF_EXACT_FRAME_DIGEST_PREFIX: &str = "dfs-metadata-prefix-v2:blake3:";

impl DfsMetadataPrefixProof {
    pub(crate) fn from_digest(journal_sequence: u64, digest: impl Into<String>) -> Option<Self> {
        let proof = Self {
            version: DFS_METADATA_PREFIX_PROOF_EXACT_FRAME_VERSION,
            journal_sequence,
            digest: digest.into(),
        };
        proof.is_well_formed().then_some(proof)
    }
    pub(crate) fn is_well_formed(&self) -> bool {
        self.version == DFS_METADATA_PREFIX_PROOF_EXACT_FRAME_VERSION
            && self
                .digest
                .strip_prefix(DFS_METADATA_PREFIX_PROOF_EXACT_FRAME_DIGEST_PREFIX)
                .is_some_and(|digest| {
                    digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit())
                })
    }
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

/// Genesis is valid only for a genuinely new empty current shard. Existing
/// storage must arrive with an externally certified exact-frame import cut.
pub(crate) fn dfs_metadata_prefix_proof_genesis(
    shard: &DfsMetadataShardId,
) -> DfsMetadataPrefixProof {
    DfsMetadataPrefixProof {
        version: DFS_METADATA_PREFIX_PROOF_EXACT_FRAME_VERSION,
        journal_sequence: 0,
        digest: metadata_exact_frame_prefix_digest(&[
            b"current-empty-shard-v7",
            shard.0.as_bytes(),
        ]),
    }
}

/// Extend the current exact-frame chain from its certified prior cut.
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

/// A shard writer's semantic routing projection for one canonical object write.
/// This describes only namespace identity; it never suppresses journal replay,
/// visible-object refresh, authority checks, or canonical persistence.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsObjectRoutingChange {
    Changed,
    Unchanged,
    #[serde(other)]
    Unknown,
}

/// Current journal protocol: every semantic effect is sealed in Prepare;
/// Commit carries only its exact already-durable operation digest.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsMetadataMutation {
    PrepareOperationV7 {
        file_system_id: DfsFileSystemId,
        operation_id: String,
        record: Box<DfsPreparedOperationRecord>,
    },
    CommitPreparedOperationV7 {
        file_system_id: DfsFileSystemId,
        operation_id: String,
        digest: DfsPreparedOperationDigest,
    },
}
impl DfsMetadataMutation {
    pub fn file_system_id(&self) -> &DfsFileSystemId {
        match self {
            Self::PrepareOperationV7 { file_system_id, .. }
            | Self::CommitPreparedOperationV7 { file_system_id, .. } => file_system_id,
        }
    }
    pub fn changes_visible_object_placement(&self) -> bool {
        matches!(self, Self::CommitPreparedOperationV7 { .. })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct DfsMetadataJournalFrameRecord {
    pub sequence: u64,
    pub exact_bytes: Bytes,
}

/// Exact pre-publication evidence for one module metadata update. None
/// requires that revision to be absent; comparison happens under the writer.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsRevisionMetadataExpectation {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub expected: Option<Box<DfsObjectVersionRecord>>,
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
            crate::validate_metadata_namespace(&pin.namespace)?;
            if !pin.pin_id.starts_with(&format!("ref/{}/", pin.namespace)) {
                return Err("content reference key has another namespace".into());
            }
            if &pin.file_system_id != file_system_id
                || pin.pin_id.trim().is_empty()
                || pin.pin_id.len() > 1024
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
#[serde(deny_unknown_fields)]
pub struct DfsNamespaceTransaction {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_reference_identity: Option<DfsRevisionReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contribution_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extension_expectations: Vec<DfsRevisionMetadataExpectation>,
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
#[serde(deny_unknown_fields)]
pub struct DfsNamespaceTransactionReceipt {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_reference_identity: Option<DfsRevisionReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contribution_digest: Option<String>,
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
    pub(crate) fn validate_content_reference_annotation(&self) -> Result<(), String> {
        if let Some(identity) = &self.content_reference_identity {
            validate_content_reference_outcome_identity(
                identity,
                &self.file_system_id,
                &self.transaction_id,
                self.contribution_digest.as_deref(),
            )?;
            if self
                .transaction_digest
                .strip_prefix("blake3:")
                .is_none_or(|hash| {
                    hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
            {
                return Err("invalid reference outcome transaction digest".into());
            }
        }
        Ok(())
    }

    pub(crate) fn same_outcome_identity(&self, other: &Self) -> bool {
        self.content_reference_identity == other.content_reference_identity
            && self.contribution_digest == other.contribution_digest
            && self.transaction_id == other.transaction_id
            && self.file_system_id == other.file_system_id
            && self.transaction_digest == other.transaction_digest
            && self.created_at == other.created_at
    }
    pub(crate) fn from_transaction(
        transaction: &DfsNamespaceTransaction,
        retained_at: OffsetDateTime,
    ) -> Result<Self, serde_json::Error> {
        let mut hasher = blake3::Hasher::new();
        serde_json::to_writer(Blake3HashWriter(&mut hasher), transaction)?;
        Ok(Self {
            content_reference_identity: transaction.content_reference_identity.clone(),
            contribution_digest: transaction.contribution_digest.clone(),
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

impl DfsNamespaceTransaction {
    pub(crate) fn validate_content_reference_annotation(&self) -> Result<(), String> {
        if let Some(identity) = &self.content_reference_identity {
            validate_content_reference_outcome_identity(
                identity,
                &self.file_system_id,
                &self.transaction_id,
                self.contribution_digest.as_deref(),
            )?;
            if !self.objects.is_empty()
                || !self.advisory_lock_states.is_empty()
                || self.versions.iter().any(|version| {
                    version.file_system_id != identity.file_system_id
                        || version.object_id != identity.object_id
                        || version.version != identity.version
                })
                || self.file_version_pins.as_ref().is_some_and(|pins| {
                    pins.add.iter().chain(&pins.remove).any(|pin| {
                        pin.file_system_id != identity.file_system_id
                            || pin.object_id != identity.object_id
                            || pin.version != identity.version
                    })
                })
            {
                return Err("reference outcome annotation has foreign mutation effects".into());
            }
        }
        Ok(())
    }
}

pub(crate) fn validate_content_reference_outcome_identity(
    identity: &DfsRevisionReference,
    file_system_id: &DfsFileSystemId,
    transaction_id: &str,
    digest: Option<&str>,
) -> Result<(), String> {
    let operation = transaction_id
        .strip_prefix("file-history:")
        .ok_or("reference outcome annotation has no compatible operation identity")?;
    if identity.file_system_id != *file_system_id
        || operation.trim().is_empty()
        || operation.len() > 256
        || operation.contains('\0')
        || digest.is_none_or(|hash| {
            hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    {
        return Err("invalid reference outcome identity or contribution digest".into());
    }
    Ok(())
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
#[serde(deny_unknown_fields)]
pub struct DfsNamespaceObjectVersionExpectation {
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<DfsVersion>,
}

/// Expected occupant for one normalized path in a hidden namespace prepare.
///
/// `None` means no live object or durable tombstone may occupy the path.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(deny_unknown_fields)]
pub struct DfsNamespacePathOccupantExpectation {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_object_id: Option<DfsObjectId>,
}

/// Sole current prepared publication lifecycle.
mod prepared_effects;
pub use prepared_effects::DfsPreparedMetadataEffect;
mod prepared_operation;
pub use prepared_operation::{
    DFS_PREPARED_OPERATION_PROTOCOL_VERSION, DfsPreparedOperationCommitReference,
    DfsPreparedOperationCommitResult, DfsPreparedOperationDigest, DfsPreparedOperationEviction,
    DfsPreparedOperationPayload, DfsPreparedOperationRecord, DfsPreparedOperationScope,
    DfsPreparedOperationStatus,
};
pub(crate) use prepared_operation::{
    compound_prepared_operation_id, with_prepared_payload_validation,
};
#[cfg(test)]
pub(crate) use prepared_operation::{
    prepared_payload_digest_count, reset_prepared_payload_digest_count,
};

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
#[serde(deny_unknown_fields)]
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

impl DfsNamespaceRequestRecord {
    pub(crate) fn request_key(&self) -> (NodeId, String) {
        (self.requester_node_id.clone(), self.request_id.clone())
    }

    pub(crate) fn matches_immutable_payload(&self, other: &Self) -> bool {
        self.file_system_id == other.file_system_id
            && self.requester_node_id == other.requester_node_id
            && self.request_id == other.request_id
            && self.operation_id == other.operation_id
            && self.authority_shard_id == other.authority_shard_id
            && self.operation == other.operation
            && self.command_digest == other.command_digest
            && self.created_at == other.created_at
    }

    /// Recovery bookkeeping may change without changing a completed outcome.
    pub(crate) fn matches_completed_outcome(&self, other: &Self) -> bool {
        self.matches_immutable_payload(other)
            && self.status == DfsNamespaceRequestStatus::Completed
            && other.status == DfsNamespaceRequestStatus::Completed
            && self.result == other.result
            && self.posix_coordination_result == other.posix_coordination_result
    }
}

/// Compact terminal result attached to a request-owned namespace prepare.
///
/// The prepare's transaction and content receipts already contain the full
/// durable mutation. This record retains only the identity and canonical
/// response needed to index a hidden prepare, validate exact retries, and
/// materialize a bounded request receipt when the prepare commits.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct DfsNamespaceReceiptRetentionState {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub content_reference_outcome_retention:
        BTreeMap<DfsObjectId, DfsContentReferenceOutcomeRetention>,
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
#[serde(deny_unknown_fields)]
pub struct DfsNamespaceReceiptOrderKey {
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub identity: String,
}

/// Object-scoped reference retry expiry, carried with the metadata family.
/// A missing identity behind a floor is unknown, never a fresh operation.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsContentReferenceOutcomeRetention {
    pub file_system_id: DfsFileSystemId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_evicted_operation_order_token: Option<String>,
    #[serde(default)]
    pub opaque_operation_history_evicted: bool,
}

impl DfsContentReferenceOutcomeRetention {
    pub(crate) fn validate_file_system_scope(
        &self,
        expected: &DfsFileSystemId,
    ) -> Result<(), String> {
        if self.file_system_id != *expected
            || self
                .max_evicted_operation_order_token
                .as_ref()
                .is_some_and(|token| {
                    crate::intent::stable_operation_order_token(token).as_ref() != Some(token)
                })
        {
            return Err("invalid reference outcome expiry filesystem or order token".into());
        }
        Ok(())
    }
    pub(crate) fn merge_conservative(&mut self, other: &Self) {
        if self.file_system_id != other.file_system_id {
            // A conflicting identity cannot authorize a fresh operation.
            self.opaque_operation_history_evicted = true;
        }
        if other.max_evicted_operation_order_token > self.max_evicted_operation_order_token {
            self.max_evicted_operation_order_token =
                other.max_evicted_operation_order_token.clone();
        }
        self.opaque_operation_history_evicted |= other.opaque_operation_history_evicted;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsContentReferenceOutcomePage {
    pub outcomes: Vec<DfsNamespaceTransactionReceipt>,
    pub next_cursor: Option<String>,
}

impl DfsNamespaceReceiptRetentionState {
    pub(crate) fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    pub(crate) fn merge_conservative(&mut self, other: Self) {
        for (object_id, retention) in &other.content_reference_outcome_retention {
            self.content_reference_outcome_retention
                .entry(object_id.clone())
                .and_modify(|existing| existing.merge_conservative(retention))
                .or_insert_with(|| retention.clone());
        }
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct DfsMetadataSummary {
    pub file_system_count: usize,
    pub object_count: usize,
    pub version_count: usize,
    pub receipt_count: usize,
    pub intent_count: usize,
    pub pending_write_count: usize,
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
#[serde(deny_unknown_fields)]
pub struct DfsContentCopyJobState {
    pub request: crate::DfsContentCopyRequest,
    pub requester_node_id: NodeId,
    pub options: crate::DfsWriteOptions,
    pub write_operation_id: String,
    pub planned: crate::DfsRevisionReference,
    pub source_reference: Option<crate::DfsContentReferenceRequest>,
    pub source_reference_applied: bool,
    #[serde(deserialize_with = "deserialize_content_copy_source_release")]
    pub source_release: Option<crate::DfsContentReferenceRequest>,
    pub content_hash: Option<String>,
    pub content_size_bytes: Option<u64>,
    pub result: Option<DfsContentCopyJobResult>,
    pub cleanup_complete: bool,
}

fn deserialize_content_copy_source_release<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<crate::DfsContentReferenceRequest>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<crate::DfsContentReferenceRequest>::deserialize(deserializer)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum DfsContentCopyJobResult {
    Applied {
        response: crate::DfsContentCopyResponse,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsContentCopyJobReceipt {
    pub request: crate::DfsContentCopyRequest,
    pub requester_node_id: NodeId,
    pub result: DfsContentCopyJobResult,
}

/// Exact durable routed-request identity which originated a namespace job.
///
/// Routed jobs bind one exact admission. Locally authored jobs may omit this
/// identity; an unrelated pending request cannot become their recovery owner.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
mod hardlink_fence;
pub use hardlink_fence::DfsHardlinkTargetFence;

/// Compact terminal evidence for a collected namespace job.
///
/// Pending frames and progress counters intentionally remain only on the full
/// job record while it can still run. This receipt preserves enough identity
/// for exact routed-request recovery.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsNamespaceJobReceipt {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_copy: Option<Box<DfsContentCopyJobReceipt>>,
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
            content_copy: job.pending_frames.iter().find_map(|frame| match frame {
                DfsNamespaceJobFrame::ContentCopy { state } => state.result.clone().map(|result| {
                    Box::new(DfsContentCopyJobReceipt {
                        request: state.request.clone(),
                        requester_node_id: state.requester_node_id.clone(),
                        result,
                    })
                }),
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
    ContentCopy,
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
    ContentCopy {
        state: Box<DfsContentCopyJobState>,
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
        fence: Box<DfsHardlinkTargetFence>,
        source_path: String,
    },
    /// Commit the exact alias-shard transaction while both authorities reject
    /// reads and mutations for the affected paths.
    #[serde(rename = "cross-shard-hardlink-alias-commit-with-read-set-v1")]
    CrossShardHardlinkAliasCommit {
        transaction: DfsNamespaceTransaction,
        /// Original physical scope and complete comparisons, captured before
        /// the first target phase. None is an observed absence, never a default.
        shard_id: DfsMetadataShardId,
        expected_object_versions: Vec<DfsNamespaceObjectVersionExpectation>,
        expected_path_occupants: Vec<DfsNamespacePathOccupantExpectation>,
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

/// Compact identity of the SAME current native record-stream image carried
/// as a snapshot RPC attachment. It contains data identity, not authority.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataSnapshotImageDescriptor {
    pub format_version: u16,
    pub materialized_through_sequence: u64,
    pub semantic_state_fingerprint: String,
    pub prefix_proof: DfsMetadataPrefixProof,
    pub total_size_bytes: u64,
    pub stream_record_count: u64,
    pub stream_digest: String,
}

impl DfsMetadataSnapshotImageDescriptor {
    pub(crate) fn validate(&self) -> std::result::Result<(), crate::MetadataBackendError> {
        if self.format_version
            != crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION
            || !self.prefix_proof.is_well_formed()
            || self.materialized_through_sequence != self.prefix_proof.journal_sequence
            || self.total_size_bytes < 40
            || self.stream_digest.len() != 64
            || !self.stream_digest.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(crate::MetadataBackendError::Serde(
                "invalid current snapshot image descriptor".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn matches_checkpoint(&self, sequence: u64, fingerprint: &str) -> bool {
        self.prefix_proof.journal_sequence == sequence
            && self.prefix_proof.is_well_formed()
            && self.prefix_proof.digest == fingerprint
    }
}

/// Compact materialized metadata table snapshot.
#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
pub struct DfsMetadataTableSnapshot {
    pub format_version: u16,
    pub materialized_through_sequence: u64,
    pub tables: Box<DfsMetadataTableSnapshotTables>,
    pub semantic_state_fingerprint: String,
    pub prefix_proof: DfsMetadataPrefixProof,
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
    #[derive(Serialize)]
    struct SemanticSnapshot<'a> {
        format_version: u16,
        materialized_through_sequence: u64,
        tables: &'a DfsMetadataTableSnapshotTables,
    }
    let encoded = serde_json::to_vec(&SemanticSnapshot {
        format_version: snapshot.format_version,
        materialized_through_sequence: snapshot.materialized_through_sequence,
        tables: &snapshot.tables,
    })?;
    Ok(format!("blake3:{}", blake3::hash(&encoded).to_hex()))
}

#[cfg(test)]
thread_local! {
    static DFS_METADATA_FULL_SNAPSHOT_FINGERPRINT_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Clone, Debug, Default, Serialize, Eq, PartialEq)]
pub struct DfsMetadataTableSnapshotTables {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub file_systems: Vec<DfsFileSystem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub objects: Vec<DfsObjectMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub file_system_objects: Vec<DfsPathRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<DfsPathRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inodes: Vec<DfsInodeRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dirfrags: Vec<DfsDirFragmentRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dentries: Vec<DfsPathRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hardlinks: Vec<DfsHardlinkIndexSnapshotRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compacted_tombstones: Vec<DfsObjectMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub versions: Vec<DfsObjectVersionRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub version_pins: Vec<DfsMetadataVersionPinRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cross_shard_pin_transfers: Vec<DfsCrossShardPinTransferRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub receipts: Vec<DfsReplicaReceiptRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub intents: Vec<DfsWriteIntentRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub intent_receipts: Vec<DfsWriteIntentReceipt>,
    #[serde(
        default,
        skip_serializing_if = "DfsWriteIntentReceiptRetentionState::is_empty"
    )]
    pub intent_receipt_retention: DfsWriteIntentReceiptRetentionState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub advisory_lock_states: Vec<DfsAdvisoryLockStateRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub namespace_transaction_receipts: Vec<DfsNamespaceTransactionReceipt>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prepared_operations_v7: Vec<DfsPreparedOperationRecord>,
    pub prepared_operation_evictions: Vec<DfsPreparedOperationEviction>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub namespace_jobs: Vec<DfsNamespaceJobRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub namespace_job_receipts: Vec<DfsNamespaceJobReceipt>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub namespace_request_receipts: Vec<DfsNamespaceRequestReceipt>,
    #[serde(
        default,
        skip_serializing_if = "DfsNamespaceReceiptRetentionState::is_empty"
    )]
    pub namespace_receipt_retention: DfsNamespaceReceiptRetentionState,
    pub namespace_requests: Vec<DfsNamespaceRequestRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsHardlinkIndexSnapshotRecord {
    pub target_object_id: DfsObjectId,
    pub hardlink_object_id: DfsObjectId,
    pub path_record: DfsPathRecord,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DfsMetadataTableBatch {
    pub(crate) sequence: u64,
    pub(crate) ops: Vec<DfsMetadataTableOp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) prefix_proof: Option<DfsMetadataPrefixProof>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DfsMetadataTableOp {
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

    PutAdvisoryLockState {
        state: DfsAdvisoryLockStateRecord,
    },

    MergeContentReferenceOutcomeRetention {
        object_id: DfsObjectId,
        retention: DfsContentReferenceOutcomeRetention,
    },
    PutNamespaceTransactionReceipt {
        receipt: DfsNamespaceTransactionReceipt,
    },
    RemoveNamespaceTransactionReceipt {
        transaction_id: String,
        retention: DfsNamespaceReceiptRetentionState,
    },

    PutPreparedOperationV7 {
        record: Box<DfsPreparedOperationRecord>,
    },
    CommitPreparedOperationV7 {
        digest: DfsPreparedOperationDigest,
        committed_entry: Box<DfsPreparedOperationCommitReference>,
    },
    RemovePreparedOperation {
        digest: DfsPreparedOperationDigest,
    },
    ResolvePreparedOperation {
        digest: DfsPreparedOperationDigest,
        status: DfsPreparedOperationStatus,
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
        retention: Default::default(),
        module_metadata: Default::default(),
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

pub(crate) fn normalize_namespace_prepare_parts(
    transaction: &mut DfsNamespaceTransaction,
    expected_object_versions: &mut [DfsNamespaceObjectVersionExpectation],
    expected_path_occupants: &mut [DfsNamespacePathOccupantExpectation],
    content_receipts: &mut [DfsReplicaReceiptRecord],
) {
    normalize_namespace_transaction_in_place(transaction);
    for expected in expected_path_occupants.iter_mut() {
        expected.path = crate::metadata_keys::normalize_path(&expected.path);
    }
    expected_object_versions.sort();
    expected_path_occupants.sort();
    content_receipts.sort_by(|left, right| {
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

pub(crate) fn remote_read_rejection_prunes_location(reason: &str) -> bool {
    reason.contains("is not a current location for DFS object")
}
