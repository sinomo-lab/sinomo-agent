//! Compact, versioned storage codec for authoritative namespace prepares.
//!
//! The journal may store this body only when the shard's quorum-committed
//! configuration selects a packed representation. Replicas decode every
//! retained representation, while snapshots and in-memory tables keep using
//! [`DfsNamespacePrepareRecord`].

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use serde::{Deserialize, Serialize};
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt};
use thiserror::Error;
use time::{OffsetDateTime, UtcOffset};

use crate::{
    DfsAdvisoryLockStateRecord, DfsFileSystemId, DfsMetadataShardId,
    DfsNamespaceObjectVersionExpectation, DfsNamespacePathOccupantExpectation,
    DfsNamespacePrepareRecord, DfsNamespacePrepareStatus, DfsNamespaceRequestCompletion,
    DfsNamespaceRequestRecord, DfsNamespaceTransaction, DfsObjectId, DfsObjectKind,
    DfsObjectMetadata, DfsObjectVersionRecord, DfsReplicaReceiptRecord, DfsTombstoneProvenance,
    DfsVersion,
};

pub(crate) const DFS_PACKED_NAMESPACE_PREPARE_V1: u8 = 1;
pub(crate) const DFS_PACKED_NAMESPACE_PREPARE_V2: u8 = 2;
pub(crate) const DFS_PACKED_NAMESPACE_PREPARE_V3: u8 = 3;
// Decode-only compatibility for development history data. Writers use V3.
const LEGACY_HISTORY_PREPARE_V4: u8 = 4;
const LEGACY_HISTORY_PREPARE_V5: u8 = 5;
fn history_order_is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum DfsNamespacePrepareCodecError {
    #[error("invalid content durability: {0}")]
    InvalidContentDurability(String),
    #[error("invalid retained file version: {0}")]
    InvalidRetainedVersion(String),
    #[error("unsupported packed namespace prepare version {0}")]
    UnsupportedVersion(u8),
    #[error("namespace prepare journal mutation contains both canonical and packed payloads")]
    AmbiguousPayload,
    #[error("namespace prepare record {field} belongs to filesystem {actual}, expected {expected}")]
    CrossFileSystem {
        field: String,
        expected: String,
        actual: String,
    },
    #[error(
        "packed namespace prepare dictionary {dictionary} exceeds the u32 index space ({len} entries)"
    )]
    DictionaryTooLarge {
        dictionary: &'static str,
        len: usize,
    },
    #[error("packed namespace prepare dictionary {dictionary} repeats entry {index}")]
    DuplicateDictionaryValue {
        dictionary: &'static str,
        index: usize,
    },
    #[error(
        "packed namespace prepare {table} column {column} has {actual} rows, expected {expected}"
    )]
    ColumnLength {
        table: &'static str,
        column: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error(
        "packed namespace prepare {field}[{row}] references {dictionary}[{index}], but the dictionary has {len} entries"
    )]
    InvalidDictionaryIndex {
        field: &'static str,
        row: usize,
        dictionary: &'static str,
        index: u32,
        len: usize,
    },
    #[error("packed namespace prepare {field}[{row}] has unsupported enum tag {tag}")]
    InvalidEnumTag {
        field: &'static str,
        row: usize,
        tag: u8,
    },
    #[error(
        "packed namespace prepare {field}[{row}] has invalid time {unix_nanos}ns offset {offset_seconds}s"
    )]
    InvalidTime {
        field: &'static str,
        row: usize,
        unix_nanos: i128,
        offset_seconds: i32,
    },
    #[error(
        "packed namespace prepare sparse column {field} is not strictly row-ordered at entry {entry}"
    )]
    NonCanonicalSparseRows { field: &'static str, entry: usize },
}

/// Packed body stored beside the existing outer mutation identity.
///
/// The enclosing mutation already carries filesystem, operation, and status,
/// so v1 intentionally omits those repeated values. Short serialized field
/// names reduce SOBJ's per-row map-key cost without adding a second hand-written
/// binary format.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsPackedNamespacePrepareV1 {
    #[serde(rename = "f")]
    format_version: u8,
    #[serde(rename = "s")]
    shard_id: DfsMetadataShardId,
    #[serde(rename = "e")]
    owner_epoch: u64,
    #[serde(rename = "d")]
    operation_digest: String,
    #[serde(rename = "o", default, skip_serializing_if = "Vec::is_empty")]
    expected_object_versions: Vec<PackedObjectVersionExpectationV1>,
    #[serde(rename = "p", default, skip_serializing_if = "Vec::is_empty")]
    expected_path_occupants: Vec<PackedPathOccupantExpectationV1>,
    #[serde(rename = "r", default, skip_serializing_if = "Vec::is_empty")]
    content_receipts: Vec<PackedReplicaReceiptV1>,
    #[serde(rename = "t")]
    transaction: PackedNamespaceTransactionV1,
    #[serde(rename = "q", default, skip_serializing_if = "Option::is_none")]
    completed_request: Option<DfsNamespaceRequestRecord>,
    #[serde(rename = "c", default, skip_serializing_if = "Option::is_none")]
    completed_request_completion: Option<DfsNamespaceRequestCompletion>,
}

impl DfsPackedNamespacePrepareV1 {
    pub(crate) fn format_is_supported(&self) -> bool {
        self.format_version == DFS_PACKED_NAMESPACE_PREPARE_V1
    }

    fn require_supported_format(&self) -> Result<(), DfsNamespacePrepareCodecError> {
        if self.format_is_supported() {
            return Ok(());
        }
        Err(DfsNamespacePrepareCodecError::UnsupportedVersion(
            self.format_version,
        ))
    }

    pub(crate) fn shard_id(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }

    pub(crate) fn owner_epoch(&self) -> u64 {
        self.owner_epoch
    }

    pub(crate) fn operation_digest(&self) -> &str {
        &self.operation_digest
    }

    /// Validate the fields that remain filesystem-qualified in packed-v1.
    ///
    /// Objects, versions, expectations, and receipts inherit the enclosing
    /// mutation's filesystem when unpacked. Advisory-lock state and completed
    /// request evidence retain their complete wire records, so they must be
    /// checked without expanding the packed body.
    pub(crate) fn validate_file_system_scope(
        &self,
        expected: &DfsFileSystemId,
    ) -> Result<(), DfsNamespacePrepareCodecError> {
        self.require_supported_format()?;
        if let Some(pins) = &self.transaction.file_version_pins {
            pins.validate_scope(expected)
                .map_err(DfsNamespacePrepareCodecError::InvalidRetainedVersion)?;
        }
        for version in &self.transaction.versions {
            if version.content_durability.is_some() {
                return Err(DfsNamespacePrepareCodecError::InvalidContentDurability(
                    "content durability requires packed-v3".into(),
                ));
            }
            if let Some(retained) = &version.retained {
                retained
                    .validate_for_version(&version.unpack(expected))
                    .map_err(DfsNamespacePrepareCodecError::InvalidRetainedVersion)?;
            }
        }
        for (index, state) in self.transaction.advisory_lock_states.iter().enumerate() {
            require_indexed_file_system(
                "transaction.advisory_lock_states",
                index,
                expected,
                &state.file_system_id,
            )?;
        }
        if let Some(request) = &self.completed_request {
            require_file_system("completed_request", expected, &request.file_system_id)?;
        }
        if let Some(completion) = &self.completed_request_completion {
            require_file_system(
                "completed_request_completion",
                expected,
                &completion.file_system_id,
            )?;
        }
        Ok(())
    }

    /// Compare the identity retained inside packed-v1 without reconstructing
    /// the canonical prepare. Filesystem, operation, and status are carried by
    /// the enclosing mutation and are fenced by the caller.
    pub(crate) fn matches_prepare_identity(
        &self,
        shard_id: &DfsMetadataShardId,
        owner_epoch: u64,
        operation_digest: &str,
    ) -> bool {
        self.shard_id() == shard_id
            && self.owner_epoch() == owner_epoch
            && self.operation_digest() == operation_digest
    }

    pub(crate) fn pack(
        prepare: &DfsNamespacePrepareRecord,
    ) -> Result<Self, DfsNamespacePrepareCodecError> {
        if prepare
            .transaction
            .versions
            .iter()
            .any(|version| version.content_durability.is_some())
        {
            return Err(DfsNamespacePrepareCodecError::InvalidContentDurability(
                "content durability requires packed-v3".into(),
            ));
        }
        validate_namespace_prepare_file_system_scope(prepare, &prepare.file_system_id)?;
        Ok(Self {
            format_version: DFS_PACKED_NAMESPACE_PREPARE_V1,
            shard_id: prepare.shard_id.clone(),
            owner_epoch: prepare.owner_epoch,
            operation_digest: prepare.operation_digest.clone(),
            expected_object_versions: prepare
                .expected_object_versions
                .iter()
                .map(PackedObjectVersionExpectationV1::from)
                .collect(),
            expected_path_occupants: prepare
                .expected_path_occupants
                .iter()
                .map(PackedPathOccupantExpectationV1::from)
                .collect(),
            content_receipts: prepare
                .content_receipts
                .iter()
                .map(PackedReplicaReceiptV1::from)
                .collect(),
            transaction: PackedNamespaceTransactionV1::from(&prepare.transaction),
            completed_request: prepare.completed_request.clone(),
            completed_request_completion: prepare.completed_request_completion.clone(),
        })
    }

    #[cfg(test)]
    pub(crate) fn unpack(
        &self,
        file_system_id: &DfsFileSystemId,
        operation_id: &str,
        status: DfsNamespacePrepareStatus,
    ) -> Result<DfsNamespacePrepareRecord, DfsNamespacePrepareCodecError> {
        self.validate_file_system_scope(file_system_id)?;
        Ok(self.unpack_validated(file_system_id, operation_id, status))
    }

    /// Materialize a payload whose format, indices, and filesystem scope were
    /// already validated by its enclosing payload view.
    pub(crate) fn unpack_validated(
        &self,
        file_system_id: &DfsFileSystemId,
        operation_id: &str,
        status: DfsNamespacePrepareStatus,
    ) -> DfsNamespacePrepareRecord {
        let mut prepare = DfsNamespacePrepareRecord {
            operation_id: operation_id.to_string(),
            file_system_id: file_system_id.clone(),
            shard_id: self.shard_id.clone(),
            owner_epoch: self.owner_epoch,
            operation_digest: self.operation_digest.clone(),
            expected_object_versions: self
                .expected_object_versions
                .iter()
                .map(DfsNamespaceObjectVersionExpectation::from)
                .collect(),
            expected_path_occupants: self
                .expected_path_occupants
                .iter()
                .map(DfsNamespacePathOccupantExpectation::from)
                .collect(),
            content_receipts: self
                .content_receipts
                .iter()
                .map(|receipt| receipt.unpack(file_system_id))
                .collect(),
            transaction: self.transaction.unpack(file_system_id),
            completed_request: self.completed_request.clone(),
            completed_request_completion: self.completed_request_completion.clone(),
            status,
        };
        normalize_unpacked_canonical_in_place(&mut prepare);
        prepare
    }

    pub(crate) fn visit_affected_object_ids(&self, mut visit: impl FnMut(&DfsObjectId)) {
        for expected in &self.expected_object_versions {
            visit(&expected.object_id);
        }
        for object in &self.transaction.objects {
            visit(&object.object_id);
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedObjectVersionExpectationV1 {
    #[serde(rename = "o")]
    object_id: DfsObjectId,
    #[serde(rename = "v", default, skip_serializing_if = "Option::is_none")]
    expected_version: Option<DfsVersion>,
}

impl From<&DfsNamespaceObjectVersionExpectation> for PackedObjectVersionExpectationV1 {
    fn from(value: &DfsNamespaceObjectVersionExpectation) -> Self {
        Self {
            object_id: value.object_id.clone(),
            expected_version: value.expected_version.clone(),
        }
    }
}

impl From<&PackedObjectVersionExpectationV1> for DfsNamespaceObjectVersionExpectation {
    fn from(value: &PackedObjectVersionExpectationV1) -> Self {
        Self {
            object_id: value.object_id.clone(),
            expected_version: value.expected_version.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedPathOccupantExpectationV1 {
    #[serde(rename = "p")]
    path: String,
    #[serde(rename = "o", default, skip_serializing_if = "Option::is_none")]
    expected_object_id: Option<DfsObjectId>,
}

impl From<&DfsNamespacePathOccupantExpectation> for PackedPathOccupantExpectationV1 {
    fn from(value: &DfsNamespacePathOccupantExpectation) -> Self {
        Self {
            path: value.path.clone(),
            expected_object_id: value.expected_object_id.clone(),
        }
    }
}

impl From<&PackedPathOccupantExpectationV1> for DfsNamespacePathOccupantExpectation {
    fn from(value: &PackedPathOccupantExpectationV1) -> Self {
        Self {
            path: value.path.clone(),
            expected_object_id: value.expected_object_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedNamespaceTransactionV1 {
    #[serde(rename = "i")]
    transaction_id: String,
    #[serde(rename = "o")]
    objects: Vec<PackedObjectMetadataV1>,
    #[serde(rename = "v", default, skip_serializing_if = "Vec::is_empty")]
    versions: Vec<PackedObjectVersionV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    file_version_pins: Option<crate::DfsFileVersionPinMutation>,
    #[serde(rename = "l", default, skip_serializing_if = "Vec::is_empty")]
    advisory_lock_states: Vec<DfsAdvisoryLockStateRecord>,
    #[serde(rename = "t", with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
}

impl From<&DfsNamespaceTransaction> for PackedNamespaceTransactionV1 {
    fn from(value: &DfsNamespaceTransaction) -> Self {
        Self {
            transaction_id: value.transaction_id.clone(),
            objects: value
                .objects
                .iter()
                .map(PackedObjectMetadataV1::from)
                .collect(),
            versions: value
                .versions
                .iter()
                .map(PackedObjectVersionV1::from)
                .collect(),
            advisory_lock_states: value.advisory_lock_states.clone(),
            file_version_pins: value.file_version_pins.clone(),
            created_at: value.created_at,
        }
    }
}

impl PackedNamespaceTransactionV1 {
    fn unpack(&self, file_system_id: &DfsFileSystemId) -> DfsNamespaceTransaction {
        DfsNamespaceTransaction {
            file_version_pins: self.file_version_pins.clone(),
            transaction_id: self.transaction_id.clone(),
            file_system_id: file_system_id.clone(),
            objects: self
                .objects
                .iter()
                .map(|object| object.unpack(file_system_id))
                .collect(),
            versions: self
                .versions
                .iter()
                .map(|version| version.unpack(file_system_id))
                .collect(),
            advisory_lock_states: self.advisory_lock_states.clone(),
            created_at: self.created_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedObjectMetadataV1 {
    #[serde(rename = "o")]
    object_id: DfsObjectId,
    #[serde(rename = "p")]
    path: String,
    #[serde(rename = "k")]
    kind: DfsObjectKind,
    #[serde(rename = "t", default, skip_serializing_if = "Option::is_none")]
    tombstone_provenance: Option<DfsTombstoneProvenance>,
    #[serde(rename = "n")]
    owner_node_id: NodeId,
    #[serde(rename = "l", default, skip_serializing_if = "Vec::is_empty")]
    current_locations: Vec<NodeId>,
    #[serde(rename = "h", default, skip_serializing_if = "Option::is_none")]
    content_hash: Option<String>,
    #[serde(rename = "z", default, skip_serializing_if = "Option::is_none")]
    content_size_bytes: Option<u64>,
    #[serde(rename = "a", default, skip_serializing_if = "Option::is_none")]
    link_target_object_id: Option<DfsObjectId>,
    #[serde(rename = "m", default, skip_serializing_if = "is_zero_u32")]
    mode: u32,
    #[serde(rename = "u", default, skip_serializing_if = "is_zero_u32")]
    uid: u32,
    #[serde(rename = "g", default, skip_serializing_if = "is_zero_u32")]
    gid: u32,
    #[serde(rename = "aa", default, skip_serializing_if = "is_zero_u64")]
    accessed_at_unix_nanos: u64,
    #[serde(rename = "mm", default, skip_serializing_if = "is_zero_u64")]
    modified_at_unix_nanos: u64,
    #[serde(rename = "cc", default, skip_serializing_if = "is_zero_u64")]
    created_at_unix_nanos: u64,
    #[serde(rename = "xx", default, skip_serializing_if = "is_zero_u64")]
    changed_at_unix_nanos: u64,
    #[serde(rename = "v")]
    version: DfsVersion,
    #[serde(rename = "y")]
    policy: DfsObjectPolicy,
    #[serde(rename = "c", default, skip_serializing_if = "Option::is_none")]
    created_by_event_id: Option<EventId>,
    #[serde(rename = "e", default, skip_serializing_if = "Option::is_none")]
    last_modified_by_event_id: Option<EventId>,
}

impl From<&DfsObjectMetadata> for PackedObjectMetadataV1 {
    fn from(value: &DfsObjectMetadata) -> Self {
        Self {
            object_id: value.object_id.clone(),
            path: value.path.clone(),
            kind: value.kind.clone(),
            tombstone_provenance: value.tombstone_provenance.clone(),
            owner_node_id: value.owner_node_id.clone(),
            current_locations: value.current_locations.clone(),
            content_hash: value.content_hash.clone(),
            content_size_bytes: value.content_size_bytes,
            link_target_object_id: value.link_target_object_id.clone(),
            mode: value.mode,
            uid: value.uid,
            gid: value.gid,
            accessed_at_unix_nanos: value.accessed_at_unix_nanos,
            modified_at_unix_nanos: value.modified_at_unix_nanos,
            created_at_unix_nanos: value.created_at_unix_nanos,
            changed_at_unix_nanos: value.changed_at_unix_nanos,
            version: value.version.clone(),
            policy: value.policy.clone(),
            created_by_event_id: value.created_by_event_id.clone(),
            last_modified_by_event_id: value.last_modified_by_event_id.clone(),
        }
    }
}

impl PackedObjectMetadataV1 {
    fn unpack(&self, file_system_id: &DfsFileSystemId) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: self.object_id.clone(),
            file_system_id: file_system_id.clone(),
            path: self.path.clone(),
            kind: self.kind.clone(),
            tombstone_provenance: self.tombstone_provenance.clone(),
            owner_node_id: self.owner_node_id.clone(),
            current_locations: self.current_locations.clone(),
            content_hash: self.content_hash.clone(),
            content_size_bytes: self.content_size_bytes,
            link_target_object_id: self.link_target_object_id.clone(),
            mode: self.mode,
            uid: self.uid,
            gid: self.gid,
            accessed_at_unix_nanos: self.accessed_at_unix_nanos,
            modified_at_unix_nanos: self.modified_at_unix_nanos,
            created_at_unix_nanos: self.created_at_unix_nanos,
            changed_at_unix_nanos: self.changed_at_unix_nanos,
            version: self.version.clone(),
            policy: self.policy.clone(),
            created_by_event_id: self.created_by_event_id.clone(),
            last_modified_by_event_id: self.last_modified_by_event_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedObjectVersionV1 {
    #[serde(rename = "g", default, skip_serializing_if = "history_order_is_zero")]
    history_order: u64,
    #[serde(rename = "o")]
    object_id: DfsObjectId,
    #[serde(rename = "v")]
    version: DfsVersion,
    #[serde(rename = "h", default, skip_serializing_if = "Option::is_none")]
    content_hash: Option<String>,
    #[serde(rename = "z", default, skip_serializing_if = "Option::is_none")]
    size_bytes: Option<u64>,
    #[serde(rename = "l", default, skip_serializing_if = "Vec::is_empty")]
    locations: Vec<NodeId>,
    #[serde(rename = "e", default, skip_serializing_if = "Option::is_none")]
    created_by_event_id: Option<EventId>,
    #[serde(rename = "t", with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(rename = "b")]
    visible: bool,
    #[serde(rename = "r", default, skip_serializing_if = "Option::is_none")]
    retained: Option<Box<crate::DfsRetainedFileVersion>>,
    #[serde(rename = "d", default, skip_serializing_if = "Option::is_none")]
    content_durability: Option<Box<crate::DfsContentDurability>>,
}

impl From<&DfsObjectVersionRecord> for PackedObjectVersionV1 {
    fn from(value: &DfsObjectVersionRecord) -> Self {
        Self {
            history_order: value.history_order,
            object_id: value.object_id.clone(),
            version: value.version.clone(),
            content_hash: value.content_hash.clone(),
            size_bytes: value.size_bytes,
            locations: value.locations.clone(),
            created_by_event_id: value.created_by_event_id.clone(),
            created_at: value.created_at,
            visible: value.visible,
            retained: value.retained.clone(),
            content_durability: value.content_durability.clone(),
        }
    }
}

impl PackedObjectVersionV1 {
    fn unpack(&self, file_system_id: &DfsFileSystemId) -> DfsObjectVersionRecord {
        DfsObjectVersionRecord {
            content_durability: self.content_durability.clone(),
            history_order: self.history_order,
            retained: self.retained.clone(),
            file_system_id: file_system_id.clone(),
            object_id: self.object_id.clone(),
            version: self.version.clone(),
            content_hash: self.content_hash.clone(),
            size_bytes: self.size_bytes,
            locations: self.locations.clone(),
            created_by_event_id: self.created_by_event_id.clone(),
            created_at: self.created_at,
            visible: self.visible,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedReplicaReceiptV1 {
    #[serde(rename = "o")]
    object_id: DfsObjectId,
    #[serde(rename = "v")]
    committed_version: DfsVersion,
    #[serde(rename = "n")]
    owner_node_id: NodeId,
    #[serde(rename = "e")]
    event_id: EventId,
    #[serde(rename = "r")]
    replica_node_id: NodeId,
    #[serde(rename = "h", default, skip_serializing_if = "Option::is_none")]
    content_hash: Option<String>,
    #[serde(rename = "t", with = "time::serde::rfc3339")]
    received_at: OffsetDateTime,
}

impl From<&DfsReplicaReceiptRecord> for PackedReplicaReceiptV1 {
    fn from(value: &DfsReplicaReceiptRecord) -> Self {
        Self {
            object_id: value.receipt.object_id.clone(),
            committed_version: value.receipt.committed_version.clone(),
            owner_node_id: value.receipt.owner_node_id.clone(),
            event_id: value.receipt.event_id.clone(),
            replica_node_id: value.replica_node_id.clone(),
            content_hash: value.content_hash.clone(),
            received_at: value.received_at,
        }
    }
}

impl PackedReplicaReceiptV1 {
    fn unpack(&self, file_system_id: &DfsFileSystemId) -> DfsReplicaReceiptRecord {
        DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: file_system_id.clone(),
                object_id: self.object_id.clone(),
                committed_version: self.committed_version.clone(),
                owner_node_id: self.owner_node_id.clone(),
                event_id: self.event_id.clone(),
            },
            replica_node_id: self.replica_node_id.clone(),
            content_hash: self.content_hash.clone(),
            received_at: self.received_at,
        }
    }
}

/// Indexed, column-oriented namespace prepare body.
///
/// V2 interns every identity that is repeated across the prepare's object,
/// version, expectation, and receipt families. Dense scalar fields remain in
/// typed columns, while tombstone provenance is sparse. The representation is
/// deliberately still SOBJ/Serde: the quorum capability fence selects the
/// positional enum-tag contract, and malformed dictionary references fail
/// before any canonical record is materialized.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsPackedNamespacePrepareV2 {
    #[serde(rename = "f")]
    format_version: u8,
    #[serde(rename = "s")]
    shard_id: DfsMetadataShardId,
    #[serde(rename = "e")]
    owner_epoch: u64,
    #[serde(rename = "g")]
    operation_digest: String,
    #[serde(rename = "d")]
    dictionaries: PackedDictionariesV2,
    #[serde(rename = "x")]
    expected_object_versions: PackedObjectVersionExpectationsV2,
    #[serde(rename = "p")]
    expected_path_occupants: PackedPathOccupantExpectationsV2,
    #[serde(rename = "r")]
    content_receipts: PackedReplicaReceiptsV2,
    #[serde(rename = "i")]
    transaction_id: String,
    #[serde(rename = "o")]
    objects: PackedObjectsV2,
    #[serde(rename = "v")]
    versions: PackedObjectVersionsV2,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    file_version_pins: Option<crate::DfsFileVersionPinMutation>,
    #[serde(rename = "l", default, skip_serializing_if = "Vec::is_empty")]
    advisory_lock_states: Vec<DfsAdvisoryLockStateRecord>,
    #[serde(rename = "t")]
    transaction_created_at: PackedTimeV2,
    #[serde(rename = "q", default, skip_serializing_if = "Option::is_none")]
    completed_request: Option<DfsNamespaceRequestRecord>,
    #[serde(rename = "c", default, skip_serializing_if = "Option::is_none")]
    completed_request_completion: Option<DfsNamespaceRequestCompletion>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedDictionariesV2 {
    #[serde(rename = "o", default, skip_serializing_if = "Vec::is_empty")]
    object_ids: Vec<DfsObjectId>,
    #[serde(rename = "p", default, skip_serializing_if = "Vec::is_empty")]
    paths: Vec<String>,
    #[serde(rename = "v", default, skip_serializing_if = "Vec::is_empty")]
    versions: Vec<DfsVersion>,
    #[serde(rename = "h", default, skip_serializing_if = "Vec::is_empty")]
    hashes: Vec<String>,
    #[serde(rename = "n", default, skip_serializing_if = "Vec::is_empty")]
    nodes: Vec<NodeId>,
    #[serde(rename = "e", default, skip_serializing_if = "Vec::is_empty")]
    events: Vec<EventId>,
    #[serde(rename = "l")]
    location_sets: PackedLocationSetsV2,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedLocationSetsV2 {
    /// One leading zero plus the exclusive end offset of every set.
    #[serde(rename = "o")]
    offsets: Vec<u32>,
    #[serde(rename = "n", default, skip_serializing_if = "Vec::is_empty")]
    nodes: Vec<u32>,
}

impl PackedLocationSetsV2 {
    fn empty() -> Self {
        Self {
            offsets: vec![0],
            nodes: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    fn get(&self, index: usize) -> Option<&[u32]> {
        let start = usize::try_from(*self.offsets.get(index)?).ok()?;
        let end = usize::try_from(*self.offsets.get(index + 1)?).ok()?;
        self.nodes.get(start..end)
    }

    fn validate(&self, node_dictionary_len: usize) -> Result<(), DfsNamespacePrepareCodecError> {
        if self.offsets.first() != Some(&0)
            || self.offsets.last().copied().map(|value| value as usize) != Some(self.nodes.len())
            || self.offsets.windows(2).any(|pair| pair[0] > pair[1])
        {
            return Err(DfsNamespacePrepareCodecError::NonCanonicalSparseRows {
                field: "location_sets.offsets",
                entry: self.offsets.len(),
            });
        }
        let mut unique = HashSet::with_capacity(self.len());
        for set_index in 0..self.len() {
            let set = self
                .get(set_index)
                .expect("validated location-set offset bounds");
            if !unique.insert(set) {
                return Err(DfsNamespacePrepareCodecError::DuplicateDictionaryValue {
                    dictionary: "location_sets",
                    index: set_index,
                });
            }
            for (node_row, node) in set.iter().enumerate() {
                validate_index(
                    *node,
                    node_dictionary_len,
                    "location_sets.nodes",
                    node_row,
                    "nodes",
                )?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedObjectVersionExpectationsV2 {
    #[serde(rename = "o", default, skip_serializing_if = "Vec::is_empty")]
    object_ids: Vec<u32>,
    #[serde(
        rename = "v",
        default,
        skip_serializing_if = "PackedOptionalColumnV2::is_empty"
    )]
    expected_versions: PackedOptionalColumnV2<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedPathOccupantExpectationsV2 {
    #[serde(rename = "p", default, skip_serializing_if = "Vec::is_empty")]
    paths: Vec<u32>,
    #[serde(
        rename = "o",
        default,
        skip_serializing_if = "PackedOptionalColumnV2::is_empty"
    )]
    expected_object_ids: PackedOptionalColumnV2<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedObjectsV2 {
    #[serde(rename = "o", default, skip_serializing_if = "Vec::is_empty")]
    object_ids: Vec<u32>,
    #[serde(rename = "p", default, skip_serializing_if = "Vec::is_empty")]
    paths: Vec<u32>,
    #[serde(rename = "k", default, skip_serializing_if = "Vec::is_empty")]
    kinds: Vec<u8>,
    #[serde(rename = "n", default, skip_serializing_if = "Vec::is_empty")]
    owner_nodes: Vec<u32>,
    #[serde(rename = "l", default, skip_serializing_if = "Vec::is_empty")]
    location_sets: Vec<u32>,
    #[serde(rename = "h")]
    content_hashes: PackedOptionalColumnV2<u32>,
    #[serde(rename = "z")]
    content_sizes: PackedOptionalColumnV2<u64>,
    #[serde(rename = "a")]
    link_targets: PackedOptionalColumnV2<u32>,
    #[serde(rename = "m", default, skip_serializing_if = "Vec::is_empty")]
    modes: Vec<u32>,
    #[serde(rename = "u", default, skip_serializing_if = "Vec::is_empty")]
    uids: Vec<u32>,
    #[serde(rename = "g", default, skip_serializing_if = "Vec::is_empty")]
    gids: Vec<u32>,
    #[serde(rename = "aa", default, skip_serializing_if = "Vec::is_empty")]
    accessed_at_unix_nanos: Vec<u64>,
    #[serde(rename = "mm", default, skip_serializing_if = "Vec::is_empty")]
    modified_at_unix_nanos: Vec<u64>,
    #[serde(rename = "cc", default, skip_serializing_if = "Vec::is_empty")]
    created_at_unix_nanos: Vec<u64>,
    #[serde(rename = "xx", default, skip_serializing_if = "Vec::is_empty")]
    changed_at_unix_nanos: Vec<u64>,
    #[serde(rename = "v", default, skip_serializing_if = "Vec::is_empty")]
    versions: Vec<u32>,
    #[serde(rename = "y", default, skip_serializing_if = "Vec::is_empty")]
    policies: Vec<u8>,
    #[serde(rename = "c")]
    created_by_events: PackedOptionalColumnV2<u32>,
    #[serde(rename = "e")]
    last_modified_by_events: PackedOptionalColumnV2<u32>,
    #[serde(rename = "t", default, skip_serializing_if = "Vec::is_empty")]
    tombstone_provenance: Vec<PackedSparseTombstoneProvenanceV2>,
}

/// Dense values plus a compact missing-value bitmap. SOBJ arrays are typed,
/// so a `Vec<Option<T>>` cannot mix null and primitive elements.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedOptionalColumnV2<T> {
    #[serde(rename = "v", default, skip_serializing_if = "Vec::is_empty")]
    values: Vec<T>,
    /// Empty means every row is present. Otherwise one bit per missing row.
    #[serde(rename = "m", default, skip_serializing_if = "Vec::is_empty")]
    missing: Vec<u64>,
}

impl<T> Default for PackedOptionalColumnV2<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            missing: Vec::new(),
        }
    }
}

impl<T> PackedOptionalColumnV2<T>
where
    T: Copy + Default,
{
    fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
            missing: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn push(&mut self, value: Option<T>) {
        let row = self.values.len();
        self.values.push(value.unwrap_or_default());
        if value.is_none() {
            self.missing.resize(bitmap_word_len(row + 1), 0);
            self.missing[row / 64] |= 1_u64 << (row % 64);
        } else if !self.missing.is_empty() {
            self.missing.resize(bitmap_word_len(row + 1), 0);
        }
    }

    fn get(&self, row: usize) -> Option<T> {
        let value = *self.values.get(row)?;
        let missing = self
            .missing
            .get(row / 64)
            .is_some_and(|word| word & (1_u64 << (row % 64)) != 0);
        (!missing).then_some(value)
    }

    fn iter(&self) -> impl Iterator<Item = Option<T>> + '_ {
        (0..self.len()).map(|row| self.get(row))
    }

    fn validate_shape(
        &self,
        table: &'static str,
        column: &'static str,
        expected: usize,
    ) -> Result<(), DfsNamespacePrepareCodecError> {
        validate_column_length(table, column, expected, self.values.len())?;
        if self.missing.is_empty() {
            return Ok(());
        }
        validate_column_length(
            table,
            "optional_bitmap_words",
            bitmap_word_len(expected),
            self.missing.len(),
        )?;
        if let Some(last) = self.missing.last()
            && !expected.is_multiple_of(64)
            && *last >> (expected % 64) != 0
        {
            return Err(DfsNamespacePrepareCodecError::NonCanonicalSparseRows {
                field: column,
                entry: expected,
            });
        }
        Ok(())
    }
}

fn bitmap_word_len(rows: usize) -> usize {
    rows.saturating_add(63) / 64
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedSparseTombstoneProvenanceV2 {
    #[serde(rename = "r")]
    row: u32,
    #[serde(rename = "p")]
    provenance: DfsTombstoneProvenance,
}

// SOBJ lists are homogeneous; an integer/object tuple is not encodable.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedSparseRecord<T> {
    #[serde(rename = "i")]
    row: usize,
    #[serde(rename = "v")]
    value: T,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedObjectVersionsV2 {
    #[serde(rename = "g", default, skip_serializing_if = "Vec::is_empty")]
    history_orders: Vec<(usize, u64)>,
    #[serde(rename = "o", default, skip_serializing_if = "Vec::is_empty")]
    object_ids: Vec<u32>,
    #[serde(rename = "v", default, skip_serializing_if = "Vec::is_empty")]
    versions: Vec<u32>,
    #[serde(rename = "h")]
    content_hashes: PackedOptionalColumnV2<u32>,
    #[serde(rename = "z")]
    sizes: PackedOptionalColumnV2<u64>,
    #[serde(rename = "l", default, skip_serializing_if = "Vec::is_empty")]
    location_sets: Vec<u32>,
    #[serde(rename = "e")]
    created_by_events: PackedOptionalColumnV2<u32>,
    #[serde(rename = "t", default, skip_serializing_if = "Vec::is_empty")]
    created_at: Vec<PackedTimeV2>,
    #[serde(rename = "b", default, skip_serializing_if = "Vec::is_empty")]
    visible: Vec<bool>,
    #[serde(rename = "r", default, skip_serializing_if = "Vec::is_empty")]
    retained: Vec<PackedSparseRecord<Box<crate::DfsRetainedFileVersion>>>,
    #[serde(rename = "d", default, skip_serializing_if = "Vec::is_empty")]
    content_durability: Vec<PackedSparseRecord<Box<crate::DfsContentDurability>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedReplicaReceiptsV2 {
    #[serde(rename = "o", default, skip_serializing_if = "Vec::is_empty")]
    object_ids: Vec<u32>,
    #[serde(rename = "v", default, skip_serializing_if = "Vec::is_empty")]
    versions: Vec<u32>,
    #[serde(rename = "n", default, skip_serializing_if = "Vec::is_empty")]
    owner_nodes: Vec<u32>,
    #[serde(rename = "e", default, skip_serializing_if = "Vec::is_empty")]
    events: Vec<u32>,
    #[serde(rename = "r", default, skip_serializing_if = "Vec::is_empty")]
    replica_nodes: Vec<u32>,
    #[serde(rename = "h")]
    content_hashes: PackedOptionalColumnV2<u32>,
    #[serde(rename = "t", default, skip_serializing_if = "Vec::is_empty")]
    received_at: Vec<PackedTimeV2>,
}

/// Exact instant and display offset without RFC3339 string amplification.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PackedTimeV2 {
    #[serde(rename = "s")]
    unix_seconds: i64,
    #[serde(rename = "n")]
    nanosecond: u32,
    #[serde(rename = "o", default, skip_serializing_if = "is_zero_i32")]
    offset_seconds: i32,
}

impl From<OffsetDateTime> for PackedTimeV2 {
    fn from(value: OffsetDateTime) -> Self {
        Self {
            unix_seconds: value.unix_timestamp(),
            nanosecond: value.nanosecond(),
            offset_seconds: value.offset().whole_seconds(),
        }
    }
}

struct BorrowedInterner<'a, T> {
    dictionary: &'static str,
    indices: HashMap<&'a T, u32>,
    values: Vec<T>,
}

impl<'a, T> BorrowedInterner<'a, T>
where
    T: Clone + Eq + Hash,
{
    fn new(dictionary: &'static str) -> Self {
        Self {
            dictionary,
            indices: HashMap::new(),
            values: Vec::new(),
        }
    }

    fn intern(&mut self, value: &'a T) -> Result<u32, DfsNamespacePrepareCodecError> {
        if let Some(index) = self.indices.get(value).copied() {
            return Ok(index);
        }
        let index = u32::try_from(self.values.len()).map_err(|_| {
            DfsNamespacePrepareCodecError::DictionaryTooLarge {
                dictionary: self.dictionary,
                len: self.values.len(),
            }
        })?;
        self.indices.insert(value, index);
        self.values.push(value.clone());
        Ok(index)
    }
}

struct BorrowedDictionariesV2<'a> {
    object_ids: BorrowedInterner<'a, DfsObjectId>,
    paths: BorrowedInterner<'a, String>,
    versions: BorrowedInterner<'a, DfsVersion>,
    hashes: BorrowedInterner<'a, String>,
    nodes: BorrowedInterner<'a, NodeId>,
    events: BorrowedInterner<'a, EventId>,
    location_indices: HashMap<&'a [NodeId], u32>,
    location_sets: PackedLocationSetsV2,
}

impl<'a> BorrowedDictionariesV2<'a> {
    fn new() -> Self {
        Self {
            object_ids: BorrowedInterner::new("object_ids"),
            paths: BorrowedInterner::new("paths"),
            versions: BorrowedInterner::new("versions"),
            hashes: BorrowedInterner::new("hashes"),
            nodes: BorrowedInterner::new("nodes"),
            events: BorrowedInterner::new("events"),
            location_indices: HashMap::new(),
            location_sets: PackedLocationSetsV2::empty(),
        }
    }

    fn intern_locations(
        &mut self,
        locations: &'a [NodeId],
    ) -> Result<u32, DfsNamespacePrepareCodecError> {
        if let Some(index) = self.location_indices.get(locations).copied() {
            return Ok(index);
        }
        let index = u32::try_from(self.location_sets.len()).map_err(|_| {
            DfsNamespacePrepareCodecError::DictionaryTooLarge {
                dictionary: "location_sets",
                len: self.location_sets.len(),
            }
        })?;
        let end = self
            .location_sets
            .nodes
            .len()
            .checked_add(locations.len())
            .ok_or(DfsNamespacePrepareCodecError::DictionaryTooLarge {
                dictionary: "location_nodes",
                len: usize::MAX,
            })?;
        let end =
            u32::try_from(end).map_err(|_| DfsNamespacePrepareCodecError::DictionaryTooLarge {
                dictionary: "location_nodes",
                len: end,
            })?;
        for node in locations {
            self.location_sets.nodes.push(self.nodes.intern(node)?);
        }
        self.location_sets.offsets.push(end);
        self.location_indices.insert(locations, index);
        Ok(index)
    }

    fn finish(self) -> PackedDictionariesV2 {
        PackedDictionariesV2 {
            object_ids: self.object_ids.values,
            paths: self.paths.values,
            versions: self.versions.values,
            hashes: self.hashes.values,
            nodes: self.nodes.values,
            events: self.events.values,
            location_sets: self.location_sets,
        }
    }
}

impl DfsPackedNamespacePrepareV2 {
    pub(crate) fn format_is_supported(&self) -> bool {
        matches!(
            self.format_version,
            DFS_PACKED_NAMESPACE_PREPARE_V2
                | DFS_PACKED_NAMESPACE_PREPARE_V3
                | LEGACY_HISTORY_PREPARE_V4
                | LEGACY_HISTORY_PREPARE_V5
        )
    }

    pub(crate) fn pack_v3(
        prepare: &DfsNamespacePrepareRecord,
    ) -> Result<Self, DfsNamespacePrepareCodecError> {
        Self::pack_with_format(prepare, DFS_PACKED_NAMESPACE_PREPARE_V3)
    }

    fn require_supported_format(&self) -> Result<(), DfsNamespacePrepareCodecError> {
        if self.format_is_supported() {
            return Ok(());
        }
        Err(DfsNamespacePrepareCodecError::UnsupportedVersion(
            self.format_version,
        ))
    }

    pub(crate) fn shard_id(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }

    pub(crate) fn owner_epoch(&self) -> u64 {
        self.owner_epoch
    }

    pub(crate) fn operation_digest(&self) -> &str {
        &self.operation_digest
    }

    pub(crate) fn matches_prepare_identity(
        &self,
        shard_id: &DfsMetadataShardId,
        owner_epoch: u64,
        operation_digest: &str,
    ) -> bool {
        self.shard_id() == shard_id
            && self.owner_epoch() == owner_epoch
            && self.operation_digest() == operation_digest
    }

    pub(crate) fn pack(
        prepare: &DfsNamespacePrepareRecord,
    ) -> Result<Self, DfsNamespacePrepareCodecError> {
        Self::pack_with_format(prepare, DFS_PACKED_NAMESPACE_PREPARE_V2)
    }

    fn pack_with_format(
        prepare: &DfsNamespacePrepareRecord,
        format_version: u8,
    ) -> Result<Self, DfsNamespacePrepareCodecError> {
        if format_version != DFS_PACKED_NAMESPACE_PREPARE_V3
            && prepare
                .transaction
                .versions
                .iter()
                .any(|version| version.content_durability.is_some())
        {
            return Err(DfsNamespacePrepareCodecError::InvalidContentDurability(
                "content durability requires packed-v3".into(),
            ));
        }
        let file_system_id = &prepare.file_system_id;
        require_file_system(
            "transaction",
            file_system_id,
            &prepare.transaction.file_system_id,
        )?;

        let mut dictionaries = BorrowedDictionariesV2::new();
        let mut objects = PackedObjectsV2 {
            object_ids: Vec::with_capacity(prepare.transaction.objects.len()),
            paths: Vec::with_capacity(prepare.transaction.objects.len()),
            kinds: Vec::with_capacity(prepare.transaction.objects.len()),
            owner_nodes: Vec::with_capacity(prepare.transaction.objects.len()),
            location_sets: Vec::with_capacity(prepare.transaction.objects.len()),
            content_hashes: PackedOptionalColumnV2::with_capacity(
                prepare.transaction.objects.len(),
            ),
            content_sizes: PackedOptionalColumnV2::with_capacity(prepare.transaction.objects.len()),
            link_targets: PackedOptionalColumnV2::with_capacity(prepare.transaction.objects.len()),
            modes: Vec::with_capacity(prepare.transaction.objects.len()),
            uids: Vec::with_capacity(prepare.transaction.objects.len()),
            gids: Vec::with_capacity(prepare.transaction.objects.len()),
            accessed_at_unix_nanos: Vec::with_capacity(prepare.transaction.objects.len()),
            modified_at_unix_nanos: Vec::with_capacity(prepare.transaction.objects.len()),
            created_at_unix_nanos: Vec::with_capacity(prepare.transaction.objects.len()),
            changed_at_unix_nanos: Vec::with_capacity(prepare.transaction.objects.len()),
            versions: Vec::with_capacity(prepare.transaction.objects.len()),
            policies: Vec::with_capacity(prepare.transaction.objects.len()),
            created_by_events: PackedOptionalColumnV2::with_capacity(
                prepare.transaction.objects.len(),
            ),
            last_modified_by_events: PackedOptionalColumnV2::with_capacity(
                prepare.transaction.objects.len(),
            ),
            tombstone_provenance: Vec::new(),
        };
        for (row, object) in prepare.transaction.objects.iter().enumerate() {
            require_indexed_file_system(
                "transaction.objects",
                row,
                file_system_id,
                &object.file_system_id,
            )?;
            objects
                .object_ids
                .push(dictionaries.object_ids.intern(&object.object_id)?);
            objects.paths.push(dictionaries.paths.intern(&object.path)?);
            objects.kinds.push(pack_object_kind(&object.kind));
            objects
                .owner_nodes
                .push(dictionaries.nodes.intern(&object.owner_node_id)?);
            objects
                .location_sets
                .push(dictionaries.intern_locations(&object.current_locations)?);
            objects.content_hashes.push(intern_optional(
                &mut dictionaries.hashes,
                object.content_hash.as_ref(),
            )?);
            objects.content_sizes.push(object.content_size_bytes);
            objects.link_targets.push(intern_optional(
                &mut dictionaries.object_ids,
                object.link_target_object_id.as_ref(),
            )?);
            objects.modes.push(object.mode);
            objects.uids.push(object.uid);
            objects.gids.push(object.gid);
            objects
                .accessed_at_unix_nanos
                .push(object.accessed_at_unix_nanos);
            objects
                .modified_at_unix_nanos
                .push(object.modified_at_unix_nanos);
            objects
                .created_at_unix_nanos
                .push(object.created_at_unix_nanos);
            objects
                .changed_at_unix_nanos
                .push(object.changed_at_unix_nanos);
            objects
                .versions
                .push(dictionaries.versions.intern(&object.version)?);
            objects.policies.push(pack_object_policy(&object.policy));
            objects.created_by_events.push(intern_optional(
                &mut dictionaries.events,
                object.created_by_event_id.as_ref(),
            )?);
            objects.last_modified_by_events.push(intern_optional(
                &mut dictionaries.events,
                object.last_modified_by_event_id.as_ref(),
            )?);
            if let Some(provenance) = &object.tombstone_provenance {
                let row = u32::try_from(row).map_err(|_| {
                    DfsNamespacePrepareCodecError::DictionaryTooLarge {
                        dictionary: "object_rows",
                        len: prepare.transaction.objects.len(),
                    }
                })?;
                objects
                    .tombstone_provenance
                    .push(PackedSparseTombstoneProvenanceV2 {
                        row,
                        provenance: provenance.clone(),
                    });
            }
        }

        let mut versions = PackedObjectVersionsV2 {
            history_orders: Vec::new(),
            object_ids: Vec::with_capacity(prepare.transaction.versions.len()),
            versions: Vec::with_capacity(prepare.transaction.versions.len()),
            content_hashes: PackedOptionalColumnV2::with_capacity(
                prepare.transaction.versions.len(),
            ),
            sizes: PackedOptionalColumnV2::with_capacity(prepare.transaction.versions.len()),
            location_sets: Vec::with_capacity(prepare.transaction.versions.len()),
            created_by_events: PackedOptionalColumnV2::with_capacity(
                prepare.transaction.versions.len(),
            ),
            created_at: Vec::with_capacity(prepare.transaction.versions.len()),
            visible: Vec::with_capacity(prepare.transaction.versions.len()),
            retained: Vec::new(),
            content_durability: Vec::new(),
        };
        for (row, version) in prepare.transaction.versions.iter().enumerate() {
            require_indexed_file_system(
                "transaction.versions",
                row,
                file_system_id,
                &version.file_system_id,
            )?;
            versions
                .object_ids
                .push(dictionaries.object_ids.intern(&version.object_id)?);
            versions
                .versions
                .push(dictionaries.versions.intern(&version.version)?);
            versions.content_hashes.push(intern_optional(
                &mut dictionaries.hashes,
                version.content_hash.as_ref(),
            )?);
            versions.sizes.push(version.size_bytes);
            versions
                .location_sets
                .push(dictionaries.intern_locations(&version.locations)?);
            versions.created_by_events.push(intern_optional(
                &mut dictionaries.events,
                version.created_by_event_id.as_ref(),
            )?);
            versions.created_at.push(version.created_at.into());
            versions.visible.push(version.visible);
            if version.history_order != 0 {
                versions.history_orders.push((row, version.history_order));
            }
            if let Some(retained) = &version.retained {
                versions.retained.push(PackedSparseRecord {
                    row,
                    value: retained.clone(),
                });
            }
            if let Some(plan) = &version.content_durability {
                versions.content_durability.push(PackedSparseRecord {
                    row,
                    value: plan.clone(),
                });
            }
        }

        let mut expected_object_versions = PackedObjectVersionExpectationsV2 {
            object_ids: Vec::with_capacity(prepare.expected_object_versions.len()),
            expected_versions: PackedOptionalColumnV2::with_capacity(
                prepare.expected_object_versions.len(),
            ),
        };
        for expected in &prepare.expected_object_versions {
            expected_object_versions
                .object_ids
                .push(dictionaries.object_ids.intern(&expected.object_id)?);
            expected_object_versions
                .expected_versions
                .push(intern_optional(
                    &mut dictionaries.versions,
                    expected.expected_version.as_ref(),
                )?);
        }

        let mut expected_path_occupants = PackedPathOccupantExpectationsV2 {
            paths: Vec::with_capacity(prepare.expected_path_occupants.len()),
            expected_object_ids: PackedOptionalColumnV2::with_capacity(
                prepare.expected_path_occupants.len(),
            ),
        };
        for expected in &prepare.expected_path_occupants {
            expected_path_occupants
                .paths
                .push(dictionaries.paths.intern(&expected.path)?);
            expected_path_occupants
                .expected_object_ids
                .push(intern_optional(
                    &mut dictionaries.object_ids,
                    expected.expected_object_id.as_ref(),
                )?);
        }

        let mut content_receipts = PackedReplicaReceiptsV2 {
            object_ids: Vec::with_capacity(prepare.content_receipts.len()),
            versions: Vec::with_capacity(prepare.content_receipts.len()),
            owner_nodes: Vec::with_capacity(prepare.content_receipts.len()),
            events: Vec::with_capacity(prepare.content_receipts.len()),
            replica_nodes: Vec::with_capacity(prepare.content_receipts.len()),
            content_hashes: PackedOptionalColumnV2::with_capacity(prepare.content_receipts.len()),
            received_at: Vec::with_capacity(prepare.content_receipts.len()),
        };
        for (row, receipt) in prepare.content_receipts.iter().enumerate() {
            require_indexed_file_system(
                "content_receipts",
                row,
                file_system_id,
                &receipt.receipt.file_system_id,
            )?;
            content_receipts
                .object_ids
                .push(dictionaries.object_ids.intern(&receipt.receipt.object_id)?);
            content_receipts.versions.push(
                dictionaries
                    .versions
                    .intern(&receipt.receipt.committed_version)?,
            );
            content_receipts
                .owner_nodes
                .push(dictionaries.nodes.intern(&receipt.receipt.owner_node_id)?);
            content_receipts
                .events
                .push(dictionaries.events.intern(&receipt.receipt.event_id)?);
            content_receipts
                .replica_nodes
                .push(dictionaries.nodes.intern(&receipt.replica_node_id)?);
            content_receipts.content_hashes.push(intern_optional(
                &mut dictionaries.hashes,
                receipt.content_hash.as_ref(),
            )?);
            content_receipts
                .received_at
                .push(receipt.received_at.into());
        }

        let advisory_lock_states = prepare
            .transaction
            .advisory_lock_states
            .iter()
            .enumerate()
            .map(|(row, state)| {
                require_indexed_file_system(
                    "transaction.advisory_lock_states",
                    row,
                    file_system_id,
                    &state.file_system_id,
                )?;
                Ok(state.clone())
            })
            .collect::<Result<Vec<_>, DfsNamespacePrepareCodecError>>()?;
        if let Some(request) = &prepare.completed_request {
            require_file_system("completed_request", file_system_id, &request.file_system_id)?;
        }
        if let Some(completion) = &prepare.completed_request_completion {
            require_file_system(
                "completed_request_completion",
                file_system_id,
                &completion.file_system_id,
            )?;
        }

        Ok(Self {
            format_version,
            shard_id: prepare.shard_id.clone(),
            owner_epoch: prepare.owner_epoch,
            operation_digest: prepare.operation_digest.clone(),
            dictionaries: dictionaries.finish(),
            expected_object_versions,
            expected_path_occupants,
            content_receipts,
            transaction_id: prepare.transaction.transaction_id.clone(),
            objects,
            versions,
            advisory_lock_states,
            transaction_created_at: prepare.transaction.created_at.into(),
            file_version_pins: prepare.transaction.file_version_pins.clone(),
            completed_request: prepare.completed_request.clone(),
            completed_request_completion: prepare.completed_request_completion.clone(),
        })
    }

    pub(crate) fn validate_file_system_scope(
        &self,
        expected: &DfsFileSystemId,
    ) -> Result<(), DfsNamespacePrepareCodecError> {
        self.validate_structure()?;
        if let Some(pins) = &self.file_version_pins {
            pins.validate_scope(expected)
                .map_err(DfsNamespacePrepareCodecError::InvalidRetainedVersion)?;
        }
        for entry in &self.versions.retained {
            let row = &entry.row;
            let retained = &entry.value;
            let version = DfsObjectVersionRecord {
                content_durability: self
                    .versions
                    .content_durability
                    .binary_search_by_key(row, |entry| entry.row)
                    .ok()
                    .map(|index| self.versions.content_durability[index].value.clone()),
                history_order: self
                    .versions
                    .history_orders
                    .binary_search_by_key(row, |(row, _)| *row)
                    .ok()
                    .map(|index| self.versions.history_orders[index].1)
                    .unwrap_or(0),
                file_system_id: expected.clone(),
                object_id: self.object_id_at(
                    self.versions.object_ids[*row],
                    "versions.object_ids",
                    *row,
                ),
                version: self.version_at(self.versions.versions[*row], "versions.versions", *row),
                content_hash: self
                    .versions
                    .content_hashes
                    .get(*row)
                    .map(|index| self.hash_at(index, "versions.content_hashes", *row)),
                size_bytes: self.versions.sizes.get(*row),
                locations: Vec::new(),
                created_by_event_id: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: self.versions.visible[*row],
                retained: None,
            };
            retained
                .validate_for_version(&version)
                .map_err(DfsNamespacePrepareCodecError::InvalidRetainedVersion)?;
        }
        for (index, state) in self.advisory_lock_states.iter().enumerate() {
            require_indexed_file_system(
                "advisory_lock_states",
                index,
                expected,
                &state.file_system_id,
            )?;
        }
        if let Some(request) = &self.completed_request {
            require_file_system("completed_request", expected, &request.file_system_id)?;
        }
        if let Some(completion) = &self.completed_request_completion {
            require_file_system(
                "completed_request_completion",
                expected,
                &completion.file_system_id,
            )?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn unpack(
        &self,
        file_system_id: &DfsFileSystemId,
        operation_id: &str,
        status: DfsNamespacePrepareStatus,
    ) -> Result<DfsNamespacePrepareRecord, DfsNamespacePrepareCodecError> {
        self.validate_file_system_scope(file_system_id)?;
        Ok(self.unpack_validated(file_system_id, operation_id, status))
    }

    /// Materialize a payload whose format, indices, and filesystem scope were
    /// already validated by its enclosing payload view.
    pub(crate) fn unpack_validated(
        &self,
        file_system_id: &DfsFileSystemId,
        operation_id: &str,
        status: DfsNamespacePrepareStatus,
    ) -> DfsNamespacePrepareRecord {
        let expected_object_versions = self
            .expected_object_versions
            .object_ids
            .iter()
            .zip(self.expected_object_versions.expected_versions.iter())
            .map(
                |(object_id, expected_version)| DfsNamespaceObjectVersionExpectation {
                    object_id: dictionary_value(
                        &self.dictionaries.object_ids,
                        *object_id,
                        "expected_object_versions.object_ids",
                        0,
                        "object_ids",
                    )
                    .clone(),
                    expected_version: expected_version.map(|index| {
                        dictionary_value(
                            &self.dictionaries.versions,
                            index,
                            "expected_object_versions.expected_versions",
                            0,
                            "versions",
                        )
                        .clone()
                    }),
                },
            )
            .collect();
        let expected_path_occupants = self
            .expected_path_occupants
            .paths
            .iter()
            .zip(self.expected_path_occupants.expected_object_ids.iter())
            .map(
                |(path, expected_object_id)| DfsNamespacePathOccupantExpectation {
                    path: dictionary_value(
                        &self.dictionaries.paths,
                        *path,
                        "expected_path_occupants.paths",
                        0,
                        "paths",
                    )
                    .clone(),
                    expected_object_id: expected_object_id.map(|index| {
                        dictionary_value(
                            &self.dictionaries.object_ids,
                            index,
                            "expected_path_occupants.expected_object_ids",
                            0,
                            "object_ids",
                        )
                        .clone()
                    }),
                },
            )
            .collect();

        let content_receipts = (0..self.content_receipts.object_ids.len())
            .map(|row| DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file_system_id.clone(),
                    object_id: self.object_id_at(
                        self.content_receipts.object_ids[row],
                        "content_receipts.object_ids",
                        row,
                    ),
                    committed_version: self.version_at(
                        self.content_receipts.versions[row],
                        "content_receipts.versions",
                        row,
                    ),
                    owner_node_id: self.node_at(
                        self.content_receipts.owner_nodes[row],
                        "content_receipts.owner_nodes",
                        row,
                    ),
                    event_id: self.event_at(
                        self.content_receipts.events[row],
                        "content_receipts.events",
                        row,
                    ),
                },
                replica_node_id: self.node_at(
                    self.content_receipts.replica_nodes[row],
                    "content_receipts.replica_nodes",
                    row,
                ),
                content_hash: self
                    .content_receipts
                    .content_hashes
                    .get(row)
                    .map(|index| self.hash_at(index, "content_receipts.content_hashes", row)),
                received_at: unpack_time(
                    self.content_receipts.received_at[row],
                    "content_receipts.received_at",
                    row,
                )
                .expect("validated packed-v2 receipt time"),
            })
            .collect();

        let mut tombstones = self.objects.tombstone_provenance.iter().peekable();
        let objects = (0..self.objects.object_ids.len())
            .map(|row| {
                let tombstone_provenance = tombstones
                    .next_if(|entry| entry.row as usize == row)
                    .map(|entry| entry.provenance.clone());
                DfsObjectMetadata {
                    object_id: self.object_id_at(
                        self.objects.object_ids[row],
                        "objects.object_ids",
                        row,
                    ),
                    file_system_id: file_system_id.clone(),
                    path: self.path_at(self.objects.paths[row], "objects.paths", row),
                    kind: unpack_object_kind(self.objects.kinds[row], "objects.kinds", row)
                        .expect("validated packed-v2 object kind"),
                    tombstone_provenance,
                    owner_node_id: self.node_at(
                        self.objects.owner_nodes[row],
                        "objects.owner_nodes",
                        row,
                    ),
                    current_locations: self.location_set_at(
                        self.objects.location_sets[row],
                        "objects.location_sets",
                        row,
                    ),
                    content_hash: self
                        .objects
                        .content_hashes
                        .get(row)
                        .map(|index| self.hash_at(index, "objects.content_hashes", row)),
                    content_size_bytes: self.objects.content_sizes.get(row),
                    link_target_object_id: self
                        .objects
                        .link_targets
                        .get(row)
                        .map(|index| self.object_id_at(index, "objects.link_targets", row)),
                    mode: self.objects.modes[row],
                    uid: self.objects.uids[row],
                    gid: self.objects.gids[row],
                    accessed_at_unix_nanos: self.objects.accessed_at_unix_nanos[row],
                    modified_at_unix_nanos: self.objects.modified_at_unix_nanos[row],
                    created_at_unix_nanos: self.objects.created_at_unix_nanos[row],
                    changed_at_unix_nanos: self.objects.changed_at_unix_nanos[row],
                    version: self.version_at(self.objects.versions[row], "objects.versions", row),
                    policy: unpack_object_policy(
                        self.objects.policies[row],
                        "objects.policies",
                        row,
                    )
                    .expect("validated packed-v2 object policy"),
                    created_by_event_id: self
                        .objects
                        .created_by_events
                        .get(row)
                        .map(|index| self.event_at(index, "objects.created_by_events", row)),
                    last_modified_by_event_id: self
                        .objects
                        .last_modified_by_events
                        .get(row)
                        .map(|index| self.event_at(index, "objects.last_modified_by_events", row)),
                }
            })
            .collect();

        let versions = (0..self.versions.object_ids.len())
            .map(|row| DfsObjectVersionRecord {
                content_durability: self
                    .versions
                    .content_durability
                    .binary_search_by_key(&row, |entry| entry.row)
                    .ok()
                    .map(|index| self.versions.content_durability[index].value.clone()),
                history_order: self
                    .versions
                    .history_orders
                    .binary_search_by_key(&row, |(row, _)| *row)
                    .ok()
                    .map(|index| self.versions.history_orders[index].1)
                    .unwrap_or(0),
                retained: self
                    .versions
                    .retained
                    .binary_search_by_key(&row, |entry| entry.row)
                    .ok()
                    .map(|index| self.versions.retained[index].value.clone()),
                file_system_id: file_system_id.clone(),
                object_id: self.object_id_at(
                    self.versions.object_ids[row],
                    "versions.object_ids",
                    row,
                ),
                version: self.version_at(self.versions.versions[row], "versions.versions", row),
                content_hash: self
                    .versions
                    .content_hashes
                    .get(row)
                    .map(|index| self.hash_at(index, "versions.content_hashes", row)),
                size_bytes: self.versions.sizes.get(row),
                locations: self.location_set_at(
                    self.versions.location_sets[row],
                    "versions.location_sets",
                    row,
                ),
                created_by_event_id: self
                    .versions
                    .created_by_events
                    .get(row)
                    .map(|index| self.event_at(index, "versions.created_by_events", row)),
                created_at: unpack_time(self.versions.created_at[row], "versions.created_at", row)
                    .expect("validated packed-v2 version time"),
                visible: self.versions.visible[row],
            })
            .collect();

        let mut prepare = DfsNamespacePrepareRecord {
            operation_id: operation_id.to_string(),
            file_system_id: file_system_id.clone(),
            shard_id: self.shard_id.clone(),
            owner_epoch: self.owner_epoch,
            operation_digest: self.operation_digest.clone(),
            expected_object_versions,
            expected_path_occupants,
            content_receipts,
            transaction: DfsNamespaceTransaction {
                file_version_pins: self.file_version_pins.clone(),
                transaction_id: self.transaction_id.clone(),
                file_system_id: file_system_id.clone(),
                objects,
                versions,
                advisory_lock_states: self.advisory_lock_states.clone(),
                created_at: unpack_time(self.transaction_created_at, "transaction_created_at", 0)
                    .expect("validated packed-v2 transaction time"),
            },
            completed_request: self.completed_request.clone(),
            completed_request_completion: self.completed_request_completion.clone(),
            status,
        };
        normalize_unpacked_canonical_in_place(&mut prepare);
        prepare
    }

    #[cfg(test)]
    pub(crate) fn visit_affected_object_ids(
        &self,
        visit: impl FnMut(&DfsObjectId),
    ) -> Result<(), DfsNamespacePrepareCodecError> {
        self.validate_structure()?;
        self.visit_affected_object_ids_validated(visit);
        Ok(())
    }

    /// Visit object identities after the enclosing payload view has validated
    /// every dictionary index and column shape.
    pub(crate) fn visit_affected_object_ids_validated(&self, mut visit: impl FnMut(&DfsObjectId)) {
        for (row, index) in self.expected_object_versions.object_ids.iter().enumerate() {
            visit(dictionary_value(
                &self.dictionaries.object_ids,
                *index,
                "expected_object_versions.object_ids",
                row,
                "object_ids",
            ));
        }
        for (row, index) in self.objects.object_ids.iter().enumerate() {
            visit(dictionary_value(
                &self.dictionaries.object_ids,
                *index,
                "objects.object_ids",
                row,
                "object_ids",
            ));
        }
    }

    fn object_id_at(&self, index: u32, field: &'static str, row: usize) -> DfsObjectId {
        dictionary_value(
            &self.dictionaries.object_ids,
            index,
            field,
            row,
            "object_ids",
        )
        .clone()
    }

    fn path_at(&self, index: u32, field: &'static str, row: usize) -> String {
        dictionary_value(&self.dictionaries.paths, index, field, row, "paths").clone()
    }

    fn version_at(&self, index: u32, field: &'static str, row: usize) -> DfsVersion {
        dictionary_value(&self.dictionaries.versions, index, field, row, "versions").clone()
    }

    fn hash_at(&self, index: u32, field: &'static str, row: usize) -> String {
        dictionary_value(&self.dictionaries.hashes, index, field, row, "hashes").clone()
    }

    fn node_at(&self, index: u32, field: &'static str, row: usize) -> NodeId {
        dictionary_value(&self.dictionaries.nodes, index, field, row, "nodes").clone()
    }

    fn event_at(&self, index: u32, field: &'static str, row: usize) -> EventId {
        dictionary_value(&self.dictionaries.events, index, field, row, "events").clone()
    }

    fn location_set_at(&self, index: u32, field: &'static str, row: usize) -> Vec<NodeId> {
        let _ = (field, row);
        self.dictionaries
            .location_sets
            .get(index as usize)
            .expect("packed-v2 location-set reference was validated")
            .iter()
            .map(|node| self.node_at(*node, "location_sets.nodes", index as usize))
            .collect()
    }

    fn validate_structure(&self) -> Result<(), DfsNamespacePrepareCodecError> {
        self.require_supported_format()?;
        validate_unique_dictionary("object_ids", &self.dictionaries.object_ids)?;
        validate_unique_dictionary("paths", &self.dictionaries.paths)?;
        validate_unique_dictionary("versions", &self.dictionaries.versions)?;
        validate_unique_dictionary("hashes", &self.dictionaries.hashes)?;
        validate_unique_dictionary("nodes", &self.dictionaries.nodes)?;
        validate_unique_dictionary("events", &self.dictionaries.events)?;
        self.dictionaries
            .location_sets
            .validate(self.dictionaries.nodes.len())?;

        let expectation_rows = self.expected_object_versions.object_ids.len();
        self.expected_object_versions
            .expected_versions
            .validate_shape(
                "expected_object_versions",
                "expected_versions",
                expectation_rows,
            )?;
        for row in 0..expectation_rows {
            validate_index(
                self.expected_object_versions.object_ids[row],
                self.dictionaries.object_ids.len(),
                "expected_object_versions.object_ids",
                row,
                "object_ids",
            )?;
            validate_optional_index(
                self.expected_object_versions.expected_versions.get(row),
                self.dictionaries.versions.len(),
                "expected_object_versions.expected_versions",
                row,
                "versions",
            )?;
        }

        let path_rows = self.expected_path_occupants.paths.len();
        self.expected_path_occupants
            .expected_object_ids
            .validate_shape("expected_path_occupants", "expected_object_ids", path_rows)?;
        for row in 0..path_rows {
            validate_index(
                self.expected_path_occupants.paths[row],
                self.dictionaries.paths.len(),
                "expected_path_occupants.paths",
                row,
                "paths",
            )?;
            validate_optional_index(
                self.expected_path_occupants.expected_object_ids.get(row),
                self.dictionaries.object_ids.len(),
                "expected_path_occupants.expected_object_ids",
                row,
                "object_ids",
            )?;
        }

        self.validate_object_columns()?;
        self.validate_version_columns()?;
        self.validate_receipt_columns()?;
        unpack_time(self.transaction_created_at, "transaction_created_at", 0)?;
        Ok(())
    }

    fn validate_object_columns(&self) -> Result<(), DfsNamespacePrepareCodecError> {
        let rows = self.objects.object_ids.len();
        self.objects
            .content_hashes
            .validate_shape("objects", "content_hashes", rows)?;
        self.objects
            .content_sizes
            .validate_shape("objects", "content_sizes", rows)?;
        self.objects
            .link_targets
            .validate_shape("objects", "link_targets", rows)?;
        self.objects
            .created_by_events
            .validate_shape("objects", "created_by_events", rows)?;
        self.objects.last_modified_by_events.validate_shape(
            "objects",
            "last_modified_by_events",
            rows,
        )?;
        for (column, actual) in [
            ("paths", self.objects.paths.len()),
            ("kinds", self.objects.kinds.len()),
            ("owner_nodes", self.objects.owner_nodes.len()),
            ("location_sets", self.objects.location_sets.len()),
            ("content_hashes", self.objects.content_hashes.len()),
            ("content_sizes", self.objects.content_sizes.len()),
            ("link_targets", self.objects.link_targets.len()),
            ("modes", self.objects.modes.len()),
            ("uids", self.objects.uids.len()),
            ("gids", self.objects.gids.len()),
            (
                "accessed_at_unix_nanos",
                self.objects.accessed_at_unix_nanos.len(),
            ),
            (
                "modified_at_unix_nanos",
                self.objects.modified_at_unix_nanos.len(),
            ),
            (
                "created_at_unix_nanos",
                self.objects.created_at_unix_nanos.len(),
            ),
            (
                "changed_at_unix_nanos",
                self.objects.changed_at_unix_nanos.len(),
            ),
            ("versions", self.objects.versions.len()),
            ("policies", self.objects.policies.len()),
            ("created_by_events", self.objects.created_by_events.len()),
            (
                "last_modified_by_events",
                self.objects.last_modified_by_events.len(),
            ),
        ] {
            validate_column_length("objects", column, rows, actual)?;
        }
        let mut previous_sparse_row = None;
        for (entry, provenance) in self.objects.tombstone_provenance.iter().enumerate() {
            if previous_sparse_row.is_some_and(|previous| previous >= provenance.row) {
                return Err(DfsNamespacePrepareCodecError::NonCanonicalSparseRows {
                    field: "objects.tombstone_provenance",
                    entry,
                });
            }
            validate_index(
                provenance.row,
                rows,
                "objects.tombstone_provenance",
                entry,
                "object_rows",
            )?;
            previous_sparse_row = Some(provenance.row);
        }
        for row in 0..rows {
            validate_index(
                self.objects.object_ids[row],
                self.dictionaries.object_ids.len(),
                "objects.object_ids",
                row,
                "object_ids",
            )?;
            validate_index(
                self.objects.paths[row],
                self.dictionaries.paths.len(),
                "objects.paths",
                row,
                "paths",
            )?;
            unpack_object_kind(self.objects.kinds[row], "objects.kinds", row)?;
            validate_index(
                self.objects.owner_nodes[row],
                self.dictionaries.nodes.len(),
                "objects.owner_nodes",
                row,
                "nodes",
            )?;
            validate_index(
                self.objects.location_sets[row],
                self.dictionaries.location_sets.len(),
                "objects.location_sets",
                row,
                "location_sets",
            )?;
            validate_optional_index(
                self.objects.content_hashes.get(row),
                self.dictionaries.hashes.len(),
                "objects.content_hashes",
                row,
                "hashes",
            )?;
            validate_optional_index(
                self.objects.link_targets.get(row),
                self.dictionaries.object_ids.len(),
                "objects.link_targets",
                row,
                "object_ids",
            )?;
            validate_index(
                self.objects.versions[row],
                self.dictionaries.versions.len(),
                "objects.versions",
                row,
                "versions",
            )?;
            unpack_object_policy(self.objects.policies[row], "objects.policies", row)?;
            validate_optional_index(
                self.objects.created_by_events.get(row),
                self.dictionaries.events.len(),
                "objects.created_by_events",
                row,
                "events",
            )?;
            validate_optional_index(
                self.objects.last_modified_by_events.get(row),
                self.dictionaries.events.len(),
                "objects.last_modified_by_events",
                row,
                "events",
            )?;
        }
        Ok(())
    }

    fn validate_version_columns(&self) -> Result<(), DfsNamespacePrepareCodecError> {
        let rows = self.versions.object_ids.len();
        if !matches!(
            self.format_version,
            DFS_PACKED_NAMESPACE_PREPARE_V3 | LEGACY_HISTORY_PREPARE_V5
        ) && !self.versions.content_durability.is_empty()
        {
            return Err(DfsNamespacePrepareCodecError::InvalidContentDurability(
                "content durability requires packed-v3".into(),
            ));
        }
        for (index, (row, order)) in self.versions.history_orders.iter().enumerate() {
            if *row >= rows
                || *order == 0
                || (index > 0 && self.versions.history_orders[index - 1].0 >= *row)
            {
                return Err(DfsNamespacePrepareCodecError::NonCanonicalSparseRows {
                    field: "versions.history_orders",
                    entry: index,
                });
            }
        }
        for (index, entry) in self.versions.retained.iter().enumerate() {
            let row = &entry.row;
            if *row >= rows || (index > 0 && self.versions.retained[index - 1].row >= *row) {
                return Err(DfsNamespacePrepareCodecError::NonCanonicalSparseRows {
                    field: "versions.retained",
                    entry: index,
                });
            }
        }
        self.versions
            .content_hashes
            .validate_shape("versions", "content_hashes", rows)?;
        self.versions
            .sizes
            .validate_shape("versions", "sizes", rows)?;
        self.versions
            .created_by_events
            .validate_shape("versions", "created_by_events", rows)?;
        for (column, actual) in [
            ("versions", self.versions.versions.len()),
            ("content_hashes", self.versions.content_hashes.len()),
            ("sizes", self.versions.sizes.len()),
            ("location_sets", self.versions.location_sets.len()),
            ("created_by_events", self.versions.created_by_events.len()),
            ("created_at", self.versions.created_at.len()),
            ("visible", self.versions.visible.len()),
        ] {
            validate_column_length("versions", column, rows, actual)?;
        }
        for row in 0..rows {
            validate_index(
                self.versions.object_ids[row],
                self.dictionaries.object_ids.len(),
                "versions.object_ids",
                row,
                "object_ids",
            )?;
            validate_index(
                self.versions.versions[row],
                self.dictionaries.versions.len(),
                "versions.versions",
                row,
                "versions",
            )?;
            validate_optional_index(
                self.versions.content_hashes.get(row),
                self.dictionaries.hashes.len(),
                "versions.content_hashes",
                row,
                "hashes",
            )?;
            validate_index(
                self.versions.location_sets[row],
                self.dictionaries.location_sets.len(),
                "versions.location_sets",
                row,
                "location_sets",
            )?;
            validate_optional_index(
                self.versions.created_by_events.get(row),
                self.dictionaries.events.len(),
                "versions.created_by_events",
                row,
                "events",
            )?;
            unpack_time(self.versions.created_at[row], "versions.created_at", row)?;
        }
        for (index, entry) in self.versions.content_durability.iter().enumerate() {
            let row = &entry.row;
            let plan = &entry.value;
            if *row >= rows
                || (index > 0 && self.versions.content_durability[index - 1].row >= *row)
            {
                return Err(DfsNamespacePrepareCodecError::NonCanonicalSparseRows {
                    field: "versions.content_durability",
                    entry: index,
                });
            }
            crate::validate_replication_plan(&plan.desired_replica_node_ids, &plan.policy)
                .map_err(|error| {
                    DfsNamespacePrepareCodecError::InvalidContentDurability(error.to_string())
                })?;
            if self.versions.content_hashes.get(*row).is_none()
                || self.versions.sizes.get(*row).is_none()
            {
                return Err(DfsNamespacePrepareCodecError::InvalidContentDurability(
                    "content durability requires immutable content identity".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_receipt_columns(&self) -> Result<(), DfsNamespacePrepareCodecError> {
        let rows = self.content_receipts.object_ids.len();
        self.content_receipts.content_hashes.validate_shape(
            "content_receipts",
            "content_hashes",
            rows,
        )?;
        for (column, actual) in [
            ("versions", self.content_receipts.versions.len()),
            ("owner_nodes", self.content_receipts.owner_nodes.len()),
            ("events", self.content_receipts.events.len()),
            ("replica_nodes", self.content_receipts.replica_nodes.len()),
            ("content_hashes", self.content_receipts.content_hashes.len()),
            ("received_at", self.content_receipts.received_at.len()),
        ] {
            validate_column_length("content_receipts", column, rows, actual)?;
        }
        for row in 0..rows {
            validate_index(
                self.content_receipts.object_ids[row],
                self.dictionaries.object_ids.len(),
                "content_receipts.object_ids",
                row,
                "object_ids",
            )?;
            validate_index(
                self.content_receipts.versions[row],
                self.dictionaries.versions.len(),
                "content_receipts.versions",
                row,
                "versions",
            )?;
            validate_index(
                self.content_receipts.owner_nodes[row],
                self.dictionaries.nodes.len(),
                "content_receipts.owner_nodes",
                row,
                "nodes",
            )?;
            validate_index(
                self.content_receipts.events[row],
                self.dictionaries.events.len(),
                "content_receipts.events",
                row,
                "events",
            )?;
            validate_index(
                self.content_receipts.replica_nodes[row],
                self.dictionaries.nodes.len(),
                "content_receipts.replica_nodes",
                row,
                "nodes",
            )?;
            validate_optional_index(
                self.content_receipts.content_hashes.get(row),
                self.dictionaries.hashes.len(),
                "content_receipts.content_hashes",
                row,
                "hashes",
            )?;
            unpack_time(
                self.content_receipts.received_at[row],
                "content_receipts.received_at",
                row,
            )?;
        }
        Ok(())
    }
}

fn intern_optional<'a, T>(
    interner: &mut BorrowedInterner<'a, T>,
    value: Option<&'a T>,
) -> Result<Option<u32>, DfsNamespacePrepareCodecError>
where
    T: Clone + Eq + Hash,
{
    value.map(|value| interner.intern(value)).transpose()
}

fn validate_unique_dictionary<T>(
    dictionary: &'static str,
    values: &[T],
) -> Result<(), DfsNamespacePrepareCodecError>
where
    T: Eq + Hash,
{
    let mut unique = HashSet::with_capacity(values.len());
    for (index, value) in values.iter().enumerate() {
        if !unique.insert(value) {
            return Err(DfsNamespacePrepareCodecError::DuplicateDictionaryValue {
                dictionary,
                index,
            });
        }
    }
    Ok(())
}

fn validate_column_length(
    table: &'static str,
    column: &'static str,
    expected: usize,
    actual: usize,
) -> Result<(), DfsNamespacePrepareCodecError> {
    if expected == actual {
        return Ok(());
    }
    Err(DfsNamespacePrepareCodecError::ColumnLength {
        table,
        column,
        expected,
        actual,
    })
}

fn validate_index(
    index: u32,
    len: usize,
    field: &'static str,
    row: usize,
    dictionary: &'static str,
) -> Result<(), DfsNamespacePrepareCodecError> {
    if (index as usize) < len {
        return Ok(());
    }
    Err(DfsNamespacePrepareCodecError::InvalidDictionaryIndex {
        field,
        row,
        dictionary,
        index,
        len,
    })
}

fn validate_optional_index(
    index: Option<u32>,
    len: usize,
    field: &'static str,
    row: usize,
    dictionary: &'static str,
) -> Result<(), DfsNamespacePrepareCodecError> {
    index.map_or(Ok(()), |index| {
        validate_index(index, len, field, row, dictionary)
    })
}

fn dictionary_value<'a, T>(
    values: &'a [T],
    index: u32,
    _field: &'static str,
    _row: usize,
    _dictionary: &'static str,
) -> &'a T {
    values
        .get(index as usize)
        .expect("packed-v2 dictionary reference was validated")
}

fn pack_object_kind(kind: &DfsObjectKind) -> u8 {
    match kind {
        DfsObjectKind::File => 0,
        DfsObjectKind::Directory => 1,
        DfsObjectKind::Symlink => 2,
        DfsObjectKind::Hardlink => 3,
        DfsObjectKind::Artifact => 4,
        DfsObjectKind::Mount => 5,
        DfsObjectKind::Tombstone => 6,
    }
}

fn unpack_object_kind(
    tag: u8,
    field: &'static str,
    row: usize,
) -> Result<DfsObjectKind, DfsNamespacePrepareCodecError> {
    match tag {
        0 => Ok(DfsObjectKind::File),
        1 => Ok(DfsObjectKind::Directory),
        2 => Ok(DfsObjectKind::Symlink),
        3 => Ok(DfsObjectKind::Hardlink),
        4 => Ok(DfsObjectKind::Artifact),
        5 => Ok(DfsObjectKind::Mount),
        6 => Ok(DfsObjectKind::Tombstone),
        tag => Err(DfsNamespacePrepareCodecError::InvalidEnumTag { field, row, tag }),
    }
}

fn pack_object_policy(policy: &DfsObjectPolicy) -> u8 {
    match policy {
        DfsObjectPolicy::LocalOwned => 0,
        DfsObjectPolicy::ReplicatedConfirmed => 1,
        DfsObjectPolicy::ReplicatedEventual => 2,
        DfsObjectPolicy::AppendOnly => 3,
        DfsObjectPolicy::EphemeralLocal => 4,
    }
}

fn unpack_object_policy(
    tag: u8,
    field: &'static str,
    row: usize,
) -> Result<DfsObjectPolicy, DfsNamespacePrepareCodecError> {
    match tag {
        0 => Ok(DfsObjectPolicy::LocalOwned),
        1 => Ok(DfsObjectPolicy::ReplicatedConfirmed),
        2 => Ok(DfsObjectPolicy::ReplicatedEventual),
        3 => Ok(DfsObjectPolicy::AppendOnly),
        4 => Ok(DfsObjectPolicy::EphemeralLocal),
        tag => Err(DfsNamespacePrepareCodecError::InvalidEnumTag { field, row, tag }),
    }
}

fn unpack_time(
    packed: PackedTimeV2,
    field: &'static str,
    row: usize,
) -> Result<OffsetDateTime, DfsNamespacePrepareCodecError> {
    let invalid = || DfsNamespacePrepareCodecError::InvalidTime {
        field,
        row,
        unix_nanos: i128::from(packed.unix_seconds)
            .saturating_mul(1_000_000_000)
            .saturating_add(i128::from(packed.nanosecond)),
        offset_seconds: packed.offset_seconds,
    };
    let value = OffsetDateTime::from_unix_timestamp(packed.unix_seconds)
        .map_err(|_| invalid())?
        .replace_nanosecond(packed.nanosecond)
        .map_err(|_| invalid())?;
    let offset = UtcOffset::from_whole_seconds(packed.offset_seconds).map_err(|_| invalid())?;
    Ok(value.to_offset(offset))
}

/// Packed authors receive an already-normalized prepare from the backend.
/// Reuse the authoritative normalizer while expanding untrusted journal bytes
/// so replay preserves every canonical ordering choice made by the primary.
fn normalize_unpacked_canonical_in_place(prepare: &mut DfsNamespacePrepareRecord) {
    crate::metadata_records::normalize_namespace_prepare_in_place(prepare);
}

pub(crate) fn validate_namespace_prepare_file_system_scope(
    prepare: &DfsNamespacePrepareRecord,
    expected: &DfsFileSystemId,
) -> Result<(), DfsNamespacePrepareCodecError> {
    require_file_system("prepare", expected, &prepare.file_system_id)?;
    require_file_system("transaction", expected, &prepare.transaction.file_system_id)?;
    if let Some(pins) = &prepare.transaction.file_version_pins {
        pins.validate_scope(expected)
            .map_err(DfsNamespacePrepareCodecError::InvalidRetainedVersion)?;
    }
    for (index, object) in prepare.transaction.objects.iter().enumerate() {
        require_indexed_file_system(
            "transaction.objects",
            index,
            expected,
            &object.file_system_id,
        )?;
    }
    for (index, version) in prepare.transaction.versions.iter().enumerate() {
        if let Some(plan) = &version.content_durability {
            crate::validate_replication_plan(&plan.desired_replica_node_ids, &plan.policy)
                .map_err(|error| {
                    DfsNamespacePrepareCodecError::InvalidContentDurability(error.to_string())
                })?;
            if version.content_hash.is_none() || version.size_bytes.is_none() {
                return Err(DfsNamespacePrepareCodecError::InvalidContentDurability(
                    "content durability requires immutable content identity".into(),
                ));
            }
        }
        if let Some(retained) = &version.retained {
            retained
                .validate_for_version(version)
                .map_err(DfsNamespacePrepareCodecError::InvalidRetainedVersion)?;
        }
        require_indexed_file_system(
            "transaction.versions",
            index,
            expected,
            &version.file_system_id,
        )?;
    }
    for (index, lock) in prepare.transaction.advisory_lock_states.iter().enumerate() {
        require_indexed_file_system(
            "transaction.advisory_lock_states",
            index,
            expected,
            &lock.file_system_id,
        )?;
    }
    for (index, receipt) in prepare.content_receipts.iter().enumerate() {
        require_indexed_file_system(
            "content_receipts",
            index,
            expected,
            &receipt.receipt.file_system_id,
        )?;
    }
    if let Some(request) = &prepare.completed_request {
        require_file_system("completed_request", expected, &request.file_system_id)?;
    }
    if let Some(completion) = &prepare.completed_request_completion {
        require_file_system(
            "completed_request_completion",
            expected,
            &completion.file_system_id,
        )?;
    }
    Ok(())
}

fn require_indexed_file_system(
    field: &str,
    index: usize,
    expected: &DfsFileSystemId,
    actual: &DfsFileSystemId,
) -> Result<(), DfsNamespacePrepareCodecError> {
    if actual == expected {
        return Ok(());
    }
    Err(DfsNamespacePrepareCodecError::CrossFileSystem {
        field: format!("{field}[{index}]"),
        expected: expected.to_string(),
        actual: actual.to_string(),
    })
}

fn require_file_system(
    field: &str,
    expected: &DfsFileSystemId,
    actual: &DfsFileSystemId,
) -> Result<(), DfsNamespacePrepareCodecError> {
    if expected == actual {
        return Ok(());
    }
    Err(DfsNamespacePrepareCodecError::CrossFileSystem {
        field: field.to_string(),
        expected: expected.to_string(),
        actual: actual.to_string(),
    })
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn is_zero_i32(value: &i32) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DfsNamespaceResult;
    use serde::Serialize;
    use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};

    fn id(value: impl Into<String>) -> sinomo_core::Id {
        sinomo_core::Id::from_legacy_named(value.into()).expect("valid typed id")
    }

    fn fixture_prepare(record_count: usize) -> DfsNamespacePrepareRecord {
        let file_system_id = id("sifs-packed-codec");
        let owner_node_id = id("node-packed-owner");
        let replica_node_id = id("node-packed-replica");
        let base_version = id("version-packed-base");
        let mut objects = Vec::with_capacity(record_count);
        let mut versions = Vec::with_capacity(record_count);
        let mut expectations = Vec::with_capacity(record_count);
        let mut path_expectations = Vec::with_capacity(record_count);
        let mut receipts = Vec::with_capacity(record_count);
        for index in 0..record_count {
            let object_id = id(format!("obj_packed_codec_{index}"));
            let version = id(format!("version_packed_codec_{index}"));
            let event_id = id(format!("evt_packed_codec_{index}"));
            let path = format!("/services/packed-codec/{index}");
            let content_hash = format!("blake3:{index:064x}");
            let mut object = DfsObjectMetadata {
                object_id: object_id.clone(),
                file_system_id: file_system_id.clone(),
                path: path.clone(),
                kind: DfsObjectKind::File,
                tombstone_provenance: None,
                owner_node_id: owner_node_id.clone(),
                current_locations: vec![replica_node_id.clone()],
                content_hash: Some(content_hash.clone()),
                content_size_bytes: Some(4096),
                link_target_object_id: None,
                mode: 0o100644,
                uid: 1000,
                gid: 1000,
                accessed_at_unix_nanos: 1,
                modified_at_unix_nanos: 2,
                created_at_unix_nanos: 3,
                changed_at_unix_nanos: 4,
                version: version.clone(),
                policy: DfsObjectPolicy::ReplicatedEventual,
                created_by_event_id: Some(event_id.clone()),
                last_modified_by_event_id: Some(event_id.clone()),
            };
            if index % 2 == 1 {
                object.kind = DfsObjectKind::Tombstone;
                object.tombstone_provenance = Some(DfsTombstoneProvenance::SourceEviction {
                    source_shard_id: DfsMetadataShardId::new("shard-packed-source"),
                    target_shard_id: DfsMetadataShardId::new("shard-packed-codec"),
                    pin_transfer_id: Some(format!("pin-packed-{index}")),
                });
                object.current_locations.clear();
                object.content_hash = None;
                object.content_size_bytes = None;
                object.link_target_object_id = Some(id(format!("obj_packed_target_{index}")));
                object.mode = 0;
                object.uid = 0;
                object.gid = 0;
                object.accessed_at_unix_nanos = 0;
                object.modified_at_unix_nanos = 0;
                object.created_at_unix_nanos = 0;
                object.changed_at_unix_nanos = 0;
                object.created_by_event_id = None;
                object.last_modified_by_event_id = None;
            }
            objects.push(object);
            versions.push(DfsObjectVersionRecord {
                content_durability: None,
                history_order: 0,
                retained: None,
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some(content_hash.clone()),
                size_bytes: Some(4096),
                locations: vec![replica_node_id.clone()],
                created_by_event_id: Some(event_id.clone()),
                created_at: OffsetDateTime::UNIX_EPOCH,
                visible: true,
            });
            expectations.push(DfsNamespaceObjectVersionExpectation {
                object_id: object_id.clone(),
                expected_version: Some(base_version.clone()),
            });
            path_expectations.push(DfsNamespacePathOccupantExpectation {
                path,
                expected_object_id: None,
            });
            receipts.push(DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: file_system_id.clone(),
                    object_id,
                    committed_version: version,
                    owner_node_id: owner_node_id.clone(),
                    event_id,
                },
                replica_node_id: replica_node_id.clone(),
                content_hash: Some(content_hash),
                received_at: OffsetDateTime::UNIX_EPOCH,
            });
        }
        DfsNamespacePrepareRecord {
            operation_id: "operation-packed-codec".to_string(),
            file_system_id: file_system_id.clone(),
            shard_id: DfsMetadataShardId::new("shard-packed-codec"),
            owner_epoch: 7,
            operation_digest: format!("blake3:{}", "a".repeat(64)),
            expected_object_versions: expectations,
            expected_path_occupants: path_expectations,
            content_receipts: receipts,
            transaction: DfsNamespaceTransaction {
                file_version_pins: None,
                transaction_id: "transaction-packed-codec".to_string(),
                file_system_id: file_system_id.clone(),
                objects,
                versions,
                advisory_lock_states: vec![DfsAdvisoryLockStateRecord::empty(
                    file_system_id.clone(),
                    id("obj_packed_lock"),
                    7,
                )],
                created_at: OffsetDateTime::UNIX_EPOCH,
            },
            completed_request: None,
            completed_request_completion: Some(DfsNamespaceRequestCompletion {
                file_system_id,
                requester_node_id: id("node-packed-requester"),
                request_id: "request-packed-codec".to_string(),
                operation_id: "operation-packed-codec".to_string(),
                authority_shard_id: DfsMetadataShardId::new("shard-packed-codec"),
                command_digest: format!("blake3:{}", "b".repeat(64)),
                transaction_id: "transaction-packed-codec".to_string(),
                result: DfsNamespaceResult::Applied,
                posix_coordination_result: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                completed_at: OffsetDateTime::UNIX_EPOCH,
            }),
            status: DfsNamespacePrepareStatus::Prepared,
        }
    }

    #[test]
    fn content_durability_round_trips_only_in_packed_v3_and_rejects_invalid_plans() {
        let mut prepare =
            crate::metadata_records::normalized_namespace_prepare(&fixture_prepare(2));
        prepare.transaction.versions[0].content_durability =
            Some(Box::new(crate::DfsContentDurability {
                current_policy_granted_at_unix_nanos: Some(42),
                source_version: prepare.transaction.versions[0].version.clone(),
                coordinator_node_id: id("durability-coordinator"),
                owner_epoch: 7,
                desired_replica_node_ids: vec![id("durability-a"), id("durability-b")],
                policy: crate::DfsReplicationPolicy::new(1, 2).unwrap(),
            }));
        assert!(DfsPackedNamespacePrepareV1::pack(&prepare).is_err());
        assert!(DfsPackedNamespacePrepareV2::pack(&prepare).is_err());
        let packed = DfsPackedNamespacePrepareV2::pack_v3(&prepare).unwrap();
        assert_eq!(packed.format_version, 3);
        let bytes = sinomo_object_serde::to_bytes(&packed, 1, LengthMode::U32).unwrap();
        let decoded: DfsPackedNamespacePrepareV2 = sinomo_object_serde::from_bytes(&bytes).unwrap();
        assert_eq!(
            decoded
                .unpack(
                    &prepare.file_system_id,
                    &prepare.operation_id,
                    prepare.status.clone()
                )
                .unwrap(),
            prepare
        );
        let mut legacy = packed.clone();
        legacy.format_version = LEGACY_HISTORY_PREPARE_V5;
        assert_eq!(
            legacy
                .unpack(
                    &prepare.file_system_id,
                    &prepare.operation_id,
                    prepare.status.clone()
                )
                .unwrap(),
            prepare
        );
        let mut old_contract = packed.clone();
        old_contract.format_version = DFS_PACKED_NAMESPACE_PREPARE_V2;
        assert!(
            old_contract
                .validate_file_system_scope(&prepare.file_system_id)
                .is_err()
        );
        let mut malformed = packed.clone();
        malformed.versions.content_durability[0]
            .value
            .policy
            .min_commit_replicas = 3;
        assert!(
            malformed
                .validate_file_system_scope(&prepare.file_system_id)
                .is_err()
        );
        let mut malformed = packed;
        malformed.versions.content_durability[0].row = usize::MAX;
        assert!(
            malformed
                .validate_file_system_scope(&prepare.file_system_id)
                .is_err()
        );
    }

    #[test]
    fn file_history_survives_packed_prepare_and_rejects_nested_identity_changes() {
        let mut prepare =
            crate::metadata_records::normalized_namespace_prepare(&fixture_prepare(2));
        prepare.transaction.versions[0].retained = Some(Box::new(crate::DfsRetainedFileVersion {
            object: Box::new(prepare.transaction.objects[0].clone()),
            captured_at_unix_nanos: 10,
            source: crate::DfsFileVersionSource::Checkpoint {
                operation_id: "capture-codec".into(),
            },
            policy_capture: None,
        }));
        let v1 = DfsPackedNamespacePrepareV1::pack(&prepare).unwrap();
        let v2 = DfsPackedNamespacePrepareV2::pack(&prepare).unwrap();
        let encoded_v1 = sinomo_object_serde::to_bytes(&v1, 1, LengthMode::U32).unwrap();
        let v1: DfsPackedNamespacePrepareV1 = sinomo_object_serde::from_bytes(&encoded_v1).unwrap();
        let encoded_v2 = sinomo_object_serde::to_bytes(&v2, 1, LengthMode::U32).unwrap();
        let v2: DfsPackedNamespacePrepareV2 = sinomo_object_serde::from_bytes(&encoded_v2).unwrap();
        let legacy_row = serde_json::to_value((
            0usize,
            prepare.transaction.versions[0].retained.as_ref().unwrap(),
        ))
        .unwrap();
        let decoded_legacy: PackedSparseRecord<Box<crate::DfsRetainedFileVersion>> =
            serde_json::from_value(legacy_row).unwrap();
        assert_eq!(decoded_legacy, v2.versions.retained[0]);
        assert_eq!(
            v1.unpack(
                &prepare.file_system_id,
                &prepare.operation_id,
                prepare.status.clone()
            )
            .unwrap(),
            prepare
        );
        assert_eq!(
            v2.unpack(
                &prepare.file_system_id,
                &prepare.operation_id,
                prepare.status.clone()
            )
            .unwrap(),
            prepare
        );
        let mut bad_v1 = v1;
        bad_v1.transaction.versions[0]
            .retained
            .as_mut()
            .unwrap()
            .object
            .file_system_id = id("foreign-fs");
        assert!(
            bad_v1
                .validate_file_system_scope(&prepare.file_system_id)
                .is_err()
        );
        let mut bad_v2 = v2;
        bad_v2.versions.retained[0].value.object.content_hash = Some("blake3:forged".into());
        assert!(
            bad_v2
                .validate_file_system_scope(&prepare.file_system_id)
                .is_err()
        );
    }

    #[test]
    fn packed_prepare_v1_round_trips_exact_canonical_record_through_sobj() {
        let prepare = crate::metadata_records::normalized_namespace_prepare(&fixture_prepare(2));
        let packed = DfsPackedNamespacePrepareV1::pack(&prepare).expect("pack prepare");
        let bytes = sinomo_object_serde::to_bytes(&packed, 1, LengthMode::U32)
            .expect("encode packed prepare");
        let decoded: DfsPackedNamespacePrepareV1 =
            sinomo_object_serde::from_bytes(&bytes).expect("decode packed prepare");
        let unpacked = decoded
            .unpack(
                &prepare.file_system_id,
                &prepare.operation_id,
                prepare.status.clone(),
            )
            .expect("unpack prepare");
        assert_eq!(unpacked, prepare);
    }

    #[test]
    fn packed_prepare_v2_round_trips_normalized_canonical_record_through_sobj() {
        let prepare = crate::metadata_records::normalized_namespace_prepare(&fixture_prepare(8));
        let packed = DfsPackedNamespacePrepareV2::pack(&prepare).expect("pack prepare");
        let bytes = sinomo_object_serde::to_bytes(&packed, 1, LengthMode::U32)
            .expect("encode packed prepare");
        let decoded: DfsPackedNamespacePrepareV2 =
            sinomo_object_serde::from_bytes(&bytes).expect("decode packed prepare");
        let unpacked = decoded
            .unpack(
                &prepare.file_system_id,
                &prepare.operation_id,
                prepare.status.clone(),
            )
            .expect("unpack prepare");
        assert_eq!(unpacked, prepare);
    }

    #[test]
    fn packed_prepare_v1_and_v2_replay_preserve_authoritative_location_order_and_digest() {
        let mut prepare = fixture_prepare(1);
        let mut locations: [NodeId; 2] =
            [id("node-packed-location-a"), id("node-packed-location-b")];
        locations.sort();
        let authoritative_order = vec![locations[1].clone(), locations[0].clone()];
        prepare.transaction.objects[0].current_locations = vec![
            authoritative_order[0].clone(),
            authoritative_order[1].clone(),
            authoritative_order[0].clone(),
        ];
        prepare.transaction.versions[0].locations = vec![
            authoritative_order[0].clone(),
            authoritative_order[1].clone(),
            authoritative_order[0].clone(),
        ];
        crate::metadata_records::normalize_namespace_prepare_in_place(&mut prepare);

        assert_eq!(
            prepare.transaction.objects[0].current_locations,
            authoritative_order
        );
        assert_eq!(
            prepare.transaction.versions[0].locations,
            authoritative_order
        );
        let primary_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest_normalized(&prepare)
                .expect("primary prepare digest");

        let replayed_v1 = DfsPackedNamespacePrepareV1::pack(&prepare)
            .expect("pack v1 prepare")
            .unpack(
                &prepare.file_system_id,
                &prepare.operation_id,
                prepare.status.clone(),
            )
            .expect("unpack v1 prepare");
        assert_eq!(replayed_v1, prepare);
        assert_eq!(
            crate::service_replica::metadata_namespace_prepare_payload_digest_normalized(
                &replayed_v1,
            )
            .expect("v1 replay prepare digest"),
            primary_digest
        );

        let replayed_v2 = DfsPackedNamespacePrepareV2::pack(&prepare)
            .expect("pack v2 prepare")
            .unpack(
                &prepare.file_system_id,
                &prepare.operation_id,
                prepare.status.clone(),
            )
            .expect("unpack v2 prepare");
        assert_eq!(replayed_v2, prepare);
        assert_eq!(
            crate::service_replica::metadata_namespace_prepare_payload_digest_normalized(
                &replayed_v2,
            )
            .expect("v2 replay prepare digest"),
            primary_digest
        );
    }

    #[test]
    fn packed_prepare_v2_normalizes_untrusted_noncanonical_rows_once_while_unpacking() {
        let mut prepare = fixture_prepare(3);
        let first_location = id("node-packed-location-a");
        let second_location = id("node-packed-location-b");
        prepare.transaction.objects[0].current_locations = vec![
            second_location.clone(),
            first_location.clone(),
            second_location.clone(),
        ];
        prepare.transaction.versions[0].locations =
            vec![second_location.clone(), first_location, second_location];
        prepare.expected_path_occupants[0].path = "//services//packed-codec///0".to_string();
        prepare.expected_object_versions.reverse();
        prepare.expected_path_occupants.reverse();
        prepare.content_receipts.reverse();

        let expected = crate::metadata_records::normalized_namespace_prepare(&prepare);
        let packed =
            DfsPackedNamespacePrepareV2::pack(&prepare).expect("pack noncanonical prepare");
        let bytes = sinomo_object_serde::to_bytes(&packed, 1, LengthMode::U32)
            .expect("encode noncanonical packed prepare");
        let decoded: DfsPackedNamespacePrepareV2 =
            sinomo_object_serde::from_bytes(&bytes).expect("decode noncanonical packed prepare");
        assert_eq!(
            decoded
                .unpack(
                    &prepare.file_system_id,
                    &prepare.operation_id,
                    prepare.status.clone(),
                )
                .expect("normalize decoded prepare"),
            expected
        );
    }

    #[test]
    fn packed_prepare_v2_is_deterministic_and_visits_borrowed_object_ids() {
        let prepare = crate::metadata_records::normalized_namespace_prepare(&fixture_prepare(16));
        let first = DfsPackedNamespacePrepareV2::pack(&prepare).expect("first pack");
        let second = DfsPackedNamespacePrepareV2::pack(&prepare).expect("second pack");
        assert_eq!(first, second);
        assert_eq!(
            sinomo_object_serde::to_bytes(&first, 1, LengthMode::U32).expect("first encode"),
            sinomo_object_serde::to_bytes(&second, 1, LengthMode::U32).expect("second encode")
        );

        let mut visited = Vec::new();
        first
            .visit_affected_object_ids(|object_id| visited.push(object_id.clone()))
            .expect("visit packed object IDs");
        let expected = prepare
            .expected_object_versions
            .iter()
            .map(|expected| expected.object_id.clone())
            .chain(
                prepare
                    .transaction
                    .objects
                    .iter()
                    .map(|object| object.object_id.clone()),
            )
            .collect::<Vec<_>>();
        assert_eq!(visited, expected);
    }

    #[test]
    fn packed_prepare_v2_preserves_arbitrary_cross_family_relationships() {
        let mut prepare = fixture_prepare(3);
        let external_object = id("obj_packed_external_occupant");
        prepare
            .expected_object_versions
            .push(DfsNamespaceObjectVersionExpectation {
                object_id: external_object.clone(),
                expected_version: Some(id("version_packed_external_base")),
            });
        prepare
            .expected_path_occupants
            .push(DfsNamespacePathOccupantExpectation {
                path: "/services/packed-codec/external".to_string(),
                expected_object_id: Some(external_object),
            });
        let first_object = prepare.transaction.objects[0].object_id.clone();
        prepare.transaction.versions.push(DfsObjectVersionRecord {
            content_durability: None,
            history_order: 0,
            retained: None,
            file_system_id: prepare.file_system_id.clone(),
            object_id: first_object,
            version: id("version_packed_historical_extra"),
            content_hash: None,
            size_bytes: None,
            locations: Vec::new(),
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH + time::Duration::nanoseconds(17),
            visible: false,
        });
        let prepare = crate::metadata_records::normalized_namespace_prepare(&prepare);
        let packed = DfsPackedNamespacePrepareV2::pack(&prepare).expect("pack prepare");
        assert_eq!(
            packed
                .unpack(
                    &prepare.file_system_id,
                    &prepare.operation_id,
                    prepare.status.clone(),
                )
                .expect("unpack prepare"),
            prepare
        );
    }

    #[test]
    fn packed_prepare_v2_rejects_malformed_columns_indices_and_dictionaries() {
        let prepare = crate::metadata_records::normalized_namespace_prepare(&fixture_prepare(2));

        let mut bad_column =
            DfsPackedNamespacePrepareV2::pack(&prepare).expect("pack bad-column fixture");
        bad_column.objects.paths.pop();
        assert!(matches!(
            bad_column.validate_file_system_scope(&prepare.file_system_id),
            Err(DfsNamespacePrepareCodecError::ColumnLength {
                table: "objects",
                column: "paths",
                ..
            })
        ));

        let mut bad_index =
            DfsPackedNamespacePrepareV2::pack(&prepare).expect("pack bad-index fixture");
        bad_index.objects.object_ids[0] = u32::MAX;
        assert!(matches!(
            bad_index.validate_file_system_scope(&prepare.file_system_id),
            Err(DfsNamespacePrepareCodecError::InvalidDictionaryIndex {
                field: "objects.object_ids",
                row: 0,
                ..
            })
        ));

        let mut duplicate =
            DfsPackedNamespacePrepareV2::pack(&prepare).expect("pack duplicate fixture");
        duplicate
            .dictionaries
            .paths
            .push(duplicate.dictionaries.paths[0].clone());
        assert!(matches!(
            duplicate.validate_file_system_scope(&prepare.file_system_id),
            Err(DfsNamespacePrepareCodecError::DuplicateDictionaryValue {
                dictionary: "paths",
                ..
            })
        ));

        let mut bad_tag =
            DfsPackedNamespacePrepareV2::pack(&prepare).expect("pack bad-tag fixture");
        bad_tag.objects.kinds[0] = u8::MAX;
        assert!(matches!(
            bad_tag.validate_file_system_scope(&prepare.file_system_id),
            Err(DfsNamespacePrepareCodecError::InvalidEnumTag {
                field: "objects.kinds",
                row: 0,
                ..
            })
        ));
    }

    #[test]
    fn packed_prepare_v1_rejects_unsupported_version() {
        let prepare = fixture_prepare(1);
        let mut packed = DfsPackedNamespacePrepareV1::pack(&prepare).expect("pack prepare");
        packed.format_version = DFS_PACKED_NAMESPACE_PREPARE_V1.saturating_add(1);
        assert_eq!(
            packed
                .validate_file_system_scope(&prepare.file_system_id)
                .expect_err("borrowed validation must reject unsupported format"),
            DfsNamespacePrepareCodecError::UnsupportedVersion(2)
        );
        assert_eq!(
            packed
                .unpack(
                    &prepare.file_system_id,
                    &prepare.operation_id,
                    prepare.status.clone(),
                )
                .expect_err("unsupported format must fail"),
            DfsNamespacePrepareCodecError::UnsupportedVersion(2)
        );
    }

    #[test]
    fn legacy_history_prepares_are_readable_but_reencoded_only_as_v3() {
        let prepare = crate::metadata_records::normalized_namespace_prepare(&fixture_prepare(1));
        for format_version in [4, 5] {
            let mut packed = DfsPackedNamespacePrepareV2::pack_v3(&prepare).unwrap();
            packed.format_version = format_version;
            let bytes = sinomo_object_serde::to_bytes(&packed, 1, LengthMode::U32).unwrap();
            let decoded: DfsPackedNamespacePrepareV2 =
                sinomo_object_serde::from_bytes(&bytes).unwrap();
            let canonical = decoded
                .unpack(
                    &prepare.file_system_id,
                    &prepare.operation_id,
                    prepare.status.clone(),
                )
                .unwrap();
            assert_eq!(canonical, prepare);
            assert_eq!(
                DfsPackedNamespacePrepareV2::pack_v3(&canonical)
                    .unwrap()
                    .format_version,
                3
            );
        }
        let mut unknown = DfsPackedNamespacePrepareV2::pack_v3(&prepare).unwrap();
        unknown.format_version = 6;
        assert_eq!(
            unknown.validate_file_system_scope(&prepare.file_system_id),
            Err(DfsNamespacePrepareCodecError::UnsupportedVersion(6))
        );
    }

    #[derive(Serialize)]
    struct PackedPrepareWithUnknownField<'a> {
        #[serde(rename = "f")]
        format_version: u8,
        #[serde(rename = "s")]
        shard_id: &'a DfsMetadataShardId,
        #[serde(rename = "e")]
        owner_epoch: u64,
        #[serde(rename = "d")]
        operation_digest: &'a str,
        #[serde(rename = "o")]
        expected_object_versions: &'a [PackedObjectVersionExpectationV1],
        #[serde(rename = "p")]
        expected_path_occupants: &'a [PackedPathOccupantExpectationV1],
        #[serde(rename = "r")]
        content_receipts: &'a [PackedReplicaReceiptV1],
        #[serde(rename = "t")]
        transaction: &'a PackedNamespaceTransactionV1,
        #[serde(rename = "q")]
        completed_request: &'a Option<DfsNamespaceRequestRecord>,
        #[serde(rename = "c")]
        completed_request_completion: &'a Option<DfsNamespaceRequestCompletion>,
        #[serde(rename = "x")]
        unknown: u8,
    }

    #[test]
    fn packed_prepare_v1_rejects_unknown_sobj_fields() {
        let prepare = fixture_prepare(2);
        let packed = DfsPackedNamespacePrepareV1::pack(&prepare).expect("pack prepare");
        let malformed = PackedPrepareWithUnknownField {
            format_version: packed.format_version,
            shard_id: &packed.shard_id,
            owner_epoch: packed.owner_epoch,
            operation_digest: &packed.operation_digest,
            expected_object_versions: &packed.expected_object_versions,
            expected_path_occupants: &packed.expected_path_occupants,
            content_receipts: &packed.content_receipts,
            transaction: &packed.transaction,
            completed_request: &packed.completed_request,
            completed_request_completion: &packed.completed_request_completion,
            unknown: 1,
        };
        let bytes = sinomo_object_serde::to_bytes(&malformed, 1, LengthMode::U32)
            .expect("encode malformed packed prepare");
        sinomo_object_serde::from_bytes::<DfsPackedNamespacePrepareV1>(&bytes)
            .expect_err("unknown packed field must fail closed");
    }

    #[test]
    fn packed_prepare_v1_rejects_cross_file_system_records() {
        let mut prepare = fixture_prepare(1);
        prepare.transaction.objects[0].file_system_id = id("sifs-packed-codec-other");
        let error =
            DfsPackedNamespacePrepareV1::pack(&prepare).expect_err("cross-filesystem object");
        assert!(matches!(
            error,
            DfsNamespacePrepareCodecError::CrossFileSystem { ref field, .. }
                if field == "transaction.objects[0]"
        ));
    }

    #[test]
    fn packed_prepare_v1_borrowed_validation_rejects_retained_cross_file_system_record() {
        let prepare = fixture_prepare(1);
        let mut packed = DfsPackedNamespacePrepareV1::pack(&prepare).expect("pack prepare");
        packed.transaction.advisory_lock_states[0].file_system_id = id("sifs-packed-codec-other");
        let error = packed
            .validate_file_system_scope(&prepare.file_system_id)
            .expect_err("cross-filesystem retained record");
        assert!(matches!(
            error,
            DfsNamespacePrepareCodecError::CrossFileSystem { ref field, .. }
                if field == "transaction.advisory_lock_states[0]"
        ));
    }

    #[derive(Serialize)]
    struct LegacyPrepareEnvelope<'a> {
        file_system_id: &'a DfsFileSystemId,
        operation_id: &'a str,
        status: &'a DfsNamespacePrepareStatus,
        prepare: &'a DfsNamespacePrepareRecord,
    }

    #[derive(Serialize)]
    struct PackedPrepareEnvelope<'a> {
        file_system_id: &'a DfsFileSystemId,
        operation_id: &'a str,
        status: &'a DfsNamespacePrepareStatus,
        packed_prepare_v1: &'a DfsPackedNamespacePrepareV1,
    }

    #[derive(Serialize)]
    struct PackedPrepareV2Envelope<'a> {
        file_system_id: &'a DfsFileSystemId,
        operation_id: &'a str,
        status: &'a DfsNamespacePrepareStatus,
        packed_prepare_v2: &'a DfsPackedNamespacePrepareV2,
    }

    #[test]
    fn packed_prepare_v2_materially_reduces_v1_and_legacy_sobj_for_4096_records() {
        let prepare = crate::metadata_records::normalized_namespace_prepare(&fixture_prepare(4096));
        let packed = DfsPackedNamespacePrepareV1::pack(&prepare).expect("pack prepare");
        let packed_v2 = DfsPackedNamespacePrepareV2::pack(&prepare).expect("pack v2 prepare");
        let legacy = LegacyPrepareEnvelope {
            file_system_id: &prepare.file_system_id,
            operation_id: &prepare.operation_id,
            status: &prepare.status,
            prepare: &prepare,
        };
        let packed_envelope = PackedPrepareEnvelope {
            file_system_id: &prepare.file_system_id,
            operation_id: &prepare.operation_id,
            status: &prepare.status,
            packed_prepare_v1: &packed,
        };
        let packed_v2_envelope = PackedPrepareV2Envelope {
            file_system_id: &prepare.file_system_id,
            operation_id: &prepare.operation_id,
            status: &prepare.status,
            packed_prepare_v2: &packed_v2,
        };
        let legacy_bytes = sinomo_object_serde::to_bytes(&legacy, 1, LengthMode::U32)
            .expect("encode legacy prepare");
        let packed_bytes = sinomo_object_serde::to_bytes(&packed_envelope, 1, LengthMode::U32)
            .expect("encode packed prepare");
        let packed_v2_bytes =
            sinomo_object_serde::to_bytes(&packed_v2_envelope, 1, LengthMode::U32)
                .expect("encode packed v2 prepare");
        eprintln!(
            "namespace prepare SOBJ bytes: legacy={} packed_v1={} packed_v2={} v1_reduction={:.1}% v2_from_v1={:.1}%",
            legacy_bytes.len(),
            packed_bytes.len(),
            packed_v2_bytes.len(),
            100.0 * (legacy_bytes.len() - packed_bytes.len()) as f64 / legacy_bytes.len() as f64,
            100.0 * (packed_bytes.len() - packed_v2_bytes.len()) as f64 / packed_bytes.len() as f64,
        );
        assert!(
            packed_bytes.len().saturating_mul(4) <= legacy_bytes.len().saturating_mul(3),
            "packed v1 must save at least 25%: legacy={} packed={}",
            legacy_bytes.len(),
            packed_bytes.len()
        );
        assert!(
            packed_v2_bytes.len().saturating_mul(4) <= packed_bytes.len().saturating_mul(3),
            "packed v2 must save at least 25% beyond v1: v1={} v2={}",
            packed_bytes.len(),
            packed_v2_bytes.len()
        );
    }
}
