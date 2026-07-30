//! Compact, versioned storage codec for authoritative namespace prepares.
//!
//! The journal may store this body only when the shard's quorum-committed
//! configuration selects packed-v1. Replicas decode both representations,
//! while snapshots and in-memory tables keep using
//! [`DfsNamespacePrepareRecord`].

use serde::{Deserialize, Serialize};
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt};
use thiserror::Error;
use time::OffsetDateTime;

use crate::{
    DfsAdvisoryLockStateRecord, DfsFileSystemId, DfsMetadataShardId,
    DfsNamespaceObjectVersionExpectation, DfsNamespacePathOccupantExpectation,
    DfsNamespacePrepareRecord, DfsNamespacePrepareStatus, DfsNamespaceRequestCompletion,
    DfsNamespaceRequestRecord, DfsNamespaceTransaction, DfsObjectId, DfsObjectKind,
    DfsObjectMetadata, DfsObjectVersionRecord, DfsReplicaReceiptRecord, DfsTombstoneProvenance,
    DfsVersion,
};

pub(crate) const DFS_PACKED_NAMESPACE_PREPARE_V1: u8 = 1;

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum DfsNamespacePrepareCodecError {
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

    pub(crate) fn unpack(
        &self,
        file_system_id: &DfsFileSystemId,
        operation_id: &str,
        status: DfsNamespacePrepareStatus,
    ) -> Result<DfsNamespacePrepareRecord, DfsNamespacePrepareCodecError> {
        self.validate_file_system_scope(file_system_id)?;
        Ok(DfsNamespacePrepareRecord {
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
        })
    }

    pub(crate) fn visit_affected_object_ids(&self, mut visit: impl FnMut(&DfsObjectId)) {
        for expected in &self.expected_object_versions {
            visit(&expected.object_id);
        }
        for object in &self.transaction.objects {
            visit(&object.object_id);
        }
    }

    pub(crate) fn record_count(&self) -> usize {
        self.transaction
            .objects
            .len()
            .saturating_add(self.transaction.versions.len())
            .saturating_add(self.transaction.advisory_lock_states.len())
            .saturating_add(self.expected_object_versions.len())
            .saturating_add(self.expected_path_occupants.len())
            .saturating_add(self.content_receipts.len())
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
            created_at: value.created_at,
        }
    }
}

impl PackedNamespaceTransactionV1 {
    fn unpack(&self, file_system_id: &DfsFileSystemId) -> DfsNamespaceTransaction {
        DfsNamespaceTransaction {
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
}

impl From<&DfsObjectVersionRecord> for PackedObjectVersionV1 {
    fn from(value: &DfsObjectVersionRecord) -> Self {
        Self {
            object_id: value.object_id.clone(),
            version: value.version.clone(),
            content_hash: value.content_hash.clone(),
            size_bytes: value.size_bytes,
            locations: value.locations.clone(),
            created_by_event_id: value.created_by_event_id.clone(),
            created_at: value.created_at,
            visible: value.visible,
        }
    }
}

impl PackedObjectVersionV1 {
    fn unpack(&self, file_system_id: &DfsFileSystemId) -> DfsObjectVersionRecord {
        DfsObjectVersionRecord {
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

pub(crate) fn validate_namespace_prepare_file_system_scope(
    prepare: &DfsNamespacePrepareRecord,
    expected: &DfsFileSystemId,
) -> Result<(), DfsNamespacePrepareCodecError> {
    require_file_system("prepare", expected, &prepare.file_system_id)?;
    require_file_system("transaction", expected, &prepare.transaction.file_system_id)?;
    for (index, object) in prepare.transaction.objects.iter().enumerate() {
        require_indexed_file_system(
            "transaction.objects",
            index,
            expected,
            &object.file_system_id,
        )?;
    }
    for (index, version) in prepare.transaction.versions.iter().enumerate() {
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
    fn packed_prepare_v1_round_trips_exact_canonical_record_through_sobj() {
        let prepare = fixture_prepare(2);
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

    #[test]
    fn packed_prepare_v1_materially_reduces_actual_sobj_for_4096_records() {
        let prepare = fixture_prepare(4096);
        let packed = DfsPackedNamespacePrepareV1::pack(&prepare).expect("pack prepare");
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
        let legacy_bytes = sinomo_object_serde::to_bytes(&legacy, 1, LengthMode::U32)
            .expect("encode legacy prepare");
        let packed_bytes = sinomo_object_serde::to_bytes(&packed_envelope, 1, LengthMode::U32)
            .expect("encode packed prepare");
        eprintln!(
            "namespace prepare SOBJ bytes: legacy={} packed={} reduction={:.1}%",
            legacy_bytes.len(),
            packed_bytes.len(),
            100.0 * (legacy_bytes.len() - packed_bytes.len()) as f64 / legacy_bytes.len() as f64
        );
        assert!(
            packed_bytes.len().saturating_mul(4) <= legacy_bytes.len().saturating_mul(3),
            "packed v1 must save at least 25%: legacy={} packed={}",
            legacy_bytes.len(),
            packed_bytes.len()
        );
    }
}
