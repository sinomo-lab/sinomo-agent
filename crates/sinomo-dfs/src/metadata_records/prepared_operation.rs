//! Complete, immutable v7 publication bodies. Legacy journal decoders remain
//! outside this protocol; a v7 commit names only an already durable body.

use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;
use std::sync::Arc;

use super::{
    DfsNamespaceObjectVersionExpectation, DfsNamespacePathOccupantExpectation,
    DfsNamespaceRequestCompletion, DfsNamespaceRequestRecord, DfsNamespaceTransaction,
    DfsWriteIntentRecord,
};
use crate::metadata_error::{MetadataBackendError, Result};
use crate::{DfsFileSystemId, DfsMetadataShardId, DfsReplicaReceiptRecord};

#[cfg(test)]
thread_local! {
    static PREPARED_PAYLOAD_DIGEST_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
#[cfg(test)]
pub(crate) fn reset_prepared_payload_digest_count() {
    PREPARED_PAYLOAD_DIGEST_COUNT.with(|count| count.set(0));
}
#[cfg(test)]
pub(crate) fn prepared_payload_digest_count() -> usize {
    PREPARED_PAYLOAD_DIGEST_COUNT.with(|count| count.get())
}

pub const DFS_PREPARED_OPERATION_PROTOCOL_VERSION: u8 = 7;
const PREPARED_OPERATION_DIGEST_DOMAIN: &[u8] = b"sinomo.dfs.prepared-operation\0v7\0";

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPreparedOperationStatus {
    Prepared,
    Committed,
    Aborted,
    Superseded,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsPreparedOperationScope {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub owner_epoch: u64,
    pub operation_id: String,
}

/// All conditional publication data is sealed before metadata acknowledgement.
/// Terminal intent and routed outcome are part of the same immutable body.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsPreparedOperationPayload {
    pub protocol_version: u8,
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub owner_epoch: u64,
    pub operation_id: String,
    /// Caller-owned journal causality, captured before hashing the complete body.
    #[serde(deserialize_with = "deserialize_journal_operation_id")]
    pub journal_operation_id: Option<String>,
    #[serde(deserialize_with = "deserialize_journal_sequence_precondition")]
    pub expected_journal_sequence: Option<u64>,
    pub expected_object_versions: Vec<DfsNamespaceObjectVersionExpectation>,
    pub expected_path_occupants: Vec<DfsNamespacePathOccupantExpectation>,
    pub transaction: DfsNamespaceTransaction,
    pub metadata_effects: Vec<super::DfsPreparedMetadataEffect>,
    pub content_receipts: Vec<DfsReplicaReceiptRecord>,
    pub required_replica_node_ids: Vec<NodeId>,
    pub prepared_intent: Option<DfsWriteIntentRecord>,
    pub terminal_intent: Option<DfsWriteIntentRecord>,
    pub completed_request: Option<DfsNamespaceRequestRecord>,
    pub completed_request_completion: Option<DfsNamespaceRequestCompletion>,
}

/// The digest binds the canonical payload and its actual durable prepare slot.
/// This is evidence identity, never publication authorization.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsPreparedOperationDigest {
    pub protocol_version: u8,
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub owner_epoch: u64,
    pub operation_id: String,
    pub payload_digest: String,
    pub prepare_sequence: u64,
}

/// Result of the actual guarded kernel invocation, not inferred from heads.
/// Exact retained replay introduces no durable publication or materialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsPreparedOperationCommitResult {
    pub entry: super::DfsMetadataJournalEntry,
    pub published: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsPreparedOperationRecord {
    pub digest: DfsPreparedOperationDigest,
    pub payload: Arc<DfsPreparedOperationPayload>,
    pub status: DfsPreparedOperationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_entry: Option<Box<DfsPreparedOperationCommitReference>>,
}

/// Exact compact terminal identity retained when the immutable body is collected.
/// It may be retired only with certified authority-epoch retirement, never time.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsPreparedOperationEviction {
    /// Original committed generation-bound birth evidence, never reconstructed
    /// from mutable filesystem rows after the immutable body is collected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub genesis_storage_activation: Option<crate::DfsMetadataStorageActivation>,
    pub digest: DfsPreparedOperationDigest,
    pub status: DfsPreparedOperationStatus,
    pub committed_entry: Option<Box<DfsPreparedOperationCommitReference>>,
}

/// Exact terminal journal identity. A commit reference cannot recursively
/// contain another preparation or its complete publication payload.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsPreparedOperationCommitReference {
    pub shard_id: DfsMetadataShardId,
    pub sequence: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
    pub digest: DfsPreparedOperationDigest,
}
impl DfsPreparedOperationCommitReference {
    pub fn from_journal_entry(entry: &super::DfsMetadataJournalEntry) -> Result<Self> {
        let super::DfsMetadataMutation::CommitPreparedOperationV7 {
            file_system_id,
            operation_id,
            digest,
        } = &entry.mutation
        else {
            return Err(MetadataBackendError::Serde(
                "terminal reference requires a commit entry".into(),
            ));
        };
        if file_system_id != &digest.file_system_id || operation_id != &digest.operation_id {
            return Err(MetadataBackendError::Serde(
                "terminal reference changed its commit scope".into(),
            ));
        }
        let reference = Self {
            shard_id: entry.shard_id.clone(),
            sequence: entry.sequence,
            created_at: entry.created_at,
            digest: digest.clone(),
        };
        reference.validate(digest)?;
        Ok(reference)
    }
    pub fn validate(&self, digest: &DfsPreparedOperationDigest) -> Result<()> {
        if self.digest != *digest
            || self.shard_id != digest.shard_id
            || digest.protocol_version != DFS_PREPARED_OPERATION_PROTOCOL_VERSION
            || digest.prepare_sequence == 0
            || self.sequence <= digest.prepare_sequence
        {
            return Err(MetadataBackendError::Serde(
                "terminal reference changed its original commit identity".into(),
            ));
        }
        Ok(())
    }
    pub fn journal_entry(&self) -> super::DfsMetadataJournalEntry {
        super::DfsMetadataJournalEntry {
            shard_id: self.shard_id.clone(),
            sequence: self.sequence,
            created_at: self.created_at,
            mutation: super::DfsMetadataMutation::CommitPreparedOperationV7 {
                file_system_id: self.digest.file_system_id.clone(),
                operation_id: self.digest.operation_id.clone(),
                digest: self.digest.clone(),
            },
        }
    }
}
impl DfsPreparedOperationEviction {
    /// Derive compact terminal DATA from its exact validated immutable body.
    /// This constructor supplies no native publication or storage authority.
    pub fn from_terminal_record(record: &DfsPreparedOperationRecord) -> Result<Self> {
        record.validate()?;
        let genesis_storage_activation = if record.status == DfsPreparedOperationStatus::Committed
            && is_exact_genesis_digest(&record.digest) {
            let mut filesystems = record.metadata_effects.iter().filter_map(|effect| match effect {
                crate::DfsPreparedMetadataEffect::PutFileSystem { file_system } => Some(file_system),
                _ => None,
            });
            let file_system = filesystems.next().ok_or_else(|| MetadataBackendError::Serde(
                "committed genesis has no original filesystem binding".into()))?;
            if filesystems.next().is_some() || file_system.file_system_id != record.digest.file_system_id {
                return Err(MetadataBackendError::Serde("committed genesis has contradictory filesystem bindings".into()));
            }
            match &file_system.metadata_storage {
                None => None,
                Some(state @ crate::DfsMetadataStorageState::Active { .. }) => {
                    state.validate_at_catalog_generation(file_system.catalog_generation)
                        .map_err(MetadataBackendError::Serde)?;
                    state.active_descriptor()
                }
                Some(crate::DfsMetadataStorageState::PendingTransition { .. }) =>
                    return Err(MetadataBackendError::Serde("committed genesis cannot bind a pending storage transition".into())),
            }
        } else { None };
        let terminal = Self { genesis_storage_activation, digest: record.digest.clone(),
            status: record.status, committed_entry: record.committed_entry.clone() };
        terminal.validate()?;
        Ok(terminal)
    }

    pub fn validate_against_record(&self, record: &DfsPreparedOperationRecord) -> Result<()> {
        if *self != Self::from_terminal_record(record)? {
            return Err(MetadataBackendError::Serde("terminal eviction changes its original body or generation witness".into()));
        }
        Ok(())
    }
    pub fn validate(&self) -> Result<()> {
        if self.digest.protocol_version != DFS_PREPARED_OPERATION_PROTOCOL_VERSION
            || self.digest.prepare_sequence == 0
            || self.digest.operation_id.trim().is_empty()
            || !self
                .digest
                .payload_digest
                .strip_prefix("dfs-prepared-operation-v7:blake3:")
                .is_some_and(|digest| {
                    digest.len() == 64
                        && digest
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
            || self.status == DfsPreparedOperationStatus::Prepared
            || (self.status == DfsPreparedOperationStatus::Committed)
                != self.committed_entry.is_some()
        {
            return Err(MetadataBackendError::Serde(
                "terminal eviction identity is malformed".into(),
            ));
        }
        if let Some(entry) = &self.committed_entry {
            entry.validate(&self.digest)?;
        }
        if let Some(activation) = &self.genesis_storage_activation {
            if self.status != DfsPreparedOperationStatus::Committed || !is_exact_genesis_digest(&self.digest) {
                return Err(MetadataBackendError::Serde("generation witness does not name committed exact genesis".into()));
            }
            activation.validate().map_err(MetadataBackendError::Serde)?;
        }
        Ok(())
    }
}

fn is_exact_genesis_digest(digest: &DfsPreparedOperationDigest) -> bool {
    digest.owner_epoch == 1 && digest.operation_id == format!("genesis:{}:{}:1", digest.file_system_id, digest.shard_id.0)
}

pub(crate) fn compound_prepared_operation_id(
    journal_operation_id: &str,
    transaction_id: &str,
) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(b"sinomo.dfs.compound-operation\0v7\0");
    for value in [journal_operation_id, transaction_id] {
        hash.update(&(value.len() as u64).to_le_bytes());
        hash.update(value.as_bytes());
    }
    format!("compound-operation-v7:blake3:{}", hash.finalize().to_hex())
}
fn deserialize_journal_operation_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    Option::<String>::deserialize(deserializer)
}

fn deserialize_journal_sequence_precondition<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<u64>, D::Error> {
    Option::<u64>::deserialize(deserializer)
}

// A replay owns its decoded table image until it returns. Keep only one
// successful immutable-body observation in that scope, never an authority,
// terminal-state result or strong payload reference. Outside replay every
// public validation still hashes normally and leaves Arc uniqueness unchanged.
struct PreparedPayloadValidation {
    last: Option<(std::sync::Weak<DfsPreparedOperationPayload>, String)>,
}
thread_local! {
    static PREPARED_PAYLOAD_VALIDATION: std::cell::RefCell<Option<PreparedPayloadValidation>> = const { std::cell::RefCell::new(None) };
}

pub(crate) fn with_prepared_payload_validation<R>(read: impl FnOnce() -> R) -> R {
    struct Restore(Option<PreparedPayloadValidation>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let previous = self.0.take();
            let _ = PREPARED_PAYLOAD_VALIDATION.try_with(|slot| slot.replace(previous));
        }
    }
    let Ok(previous) = PREPARED_PAYLOAD_VALIDATION
        .try_with(|slot| slot.replace(Some(PreparedPayloadValidation { last: None })))
    else {
        return read();
    };
    let _restore = Restore(previous);
    read()
}

fn canonical_digest_in_validation_scope(
    payload: &Arc<DfsPreparedOperationPayload>,
) -> Result<String> {
    let cached = PREPARED_PAYLOAD_VALIDATION
        .try_with(|slot| {
            let held = slot.borrow();
            let (owner, digest) = held.as_ref()?.last.as_ref()?;
            let owner = owner.upgrade()?;
            Arc::ptr_eq(&owner, payload).then(|| digest.clone())
        })
        .ok()
        .flatten();
    if let Some(digest) = cached {
        return Ok(digest);
    }
    let digest = payload.canonical_digest()?;
    let _ = PREPARED_PAYLOAD_VALIDATION.try_with(|slot| {
        if let Some(scope) = slot.borrow_mut().as_mut() {
            // Payload fields contain owned immutable values. Safe mutation of
            // this Arc either clones its body or dissociates Weak observers;
            // either action requires fresh validation for the new allocation.
            scope.last = Some((Arc::downgrade(payload), digest.clone()));
        }
    });
    Ok(digest)
}

impl DfsPreparedOperationPayload {
    #[allow(clippy::too_many_arguments)]
    pub fn for_namespace_transaction(
        scope: DfsPreparedOperationScope,
        expected_object_versions: Vec<DfsNamespaceObjectVersionExpectation>,
        expected_path_occupants: Vec<DfsNamespacePathOccupantExpectation>,
        transaction: DfsNamespaceTransaction,
        content_receipts: Vec<DfsReplicaReceiptRecord>,
        prepared_intent: Option<DfsWriteIntentRecord>,
        terminal_intent: Option<DfsWriteIntentRecord>,
        required_replica_node_ids: Vec<NodeId>,
        completed_request: Option<DfsNamespaceRequestRecord>,
        completed_request_completion: Option<DfsNamespaceRequestCompletion>,
    ) -> Result<Self> {
        let mut payload = Self {
            protocol_version: DFS_PREPARED_OPERATION_PROTOCOL_VERSION,
            file_system_id: scope.file_system_id,
            shard_id: scope.shard_id,
            owner_epoch: scope.owner_epoch,
            operation_id: scope.operation_id,
            journal_operation_id: None,
            expected_journal_sequence: None,
            expected_object_versions,
            expected_path_occupants,
            transaction,
            metadata_effects: Vec::new(),
            content_receipts,
            required_replica_node_ids,
            prepared_intent,
            terminal_intent,
            completed_request,
            completed_request_completion,
        };
        payload.normalize();
        payload.validate()?;
        Ok(payload)
    }

    pub fn for_metadata_effects(
        scope: DfsPreparedOperationScope,
        expected_object_versions: Vec<DfsNamespaceObjectVersionExpectation>,
        expected_path_occupants: Vec<DfsNamespacePathOccupantExpectation>,
        metadata_effects: Vec<super::DfsPreparedMetadataEffect>,
        captured_at: time::OffsetDateTime,
    ) -> Result<Self> {
        let transaction = DfsNamespaceTransaction {
            transaction_id: format!("metadata:{}", scope.operation_id),
            file_system_id: scope.file_system_id.clone(),
            objects: Vec::new(),
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: captured_at,
            content_reference_identity: None,
            extension_expectations: Vec::new(),
            contribution_digest: None,
            file_version_pins: None,
        };
        let mut payload = Self::for_namespace_transaction(
            scope,
            expected_object_versions,
            expected_path_occupants,
            transaction,
            Vec::new(),
            None,
            None,
            Vec::new(),
            None,
            None,
        )?;
        payload.metadata_effects = metadata_effects;
        payload.validate()?;
        Ok(payload)
    }

    pub fn validate(&self) -> Result<()> {
        if self.protocol_version != DFS_PREPARED_OPERATION_PROTOCOL_VERSION
            || self.operation_id.trim().is_empty()
            || self.operation_id.contains('\0')
            || self
                .journal_operation_id
                .as_ref()
                .is_some_and(|id| id.trim().is_empty() || id.chars().any(char::is_control))
            || self.transaction.file_system_id != self.file_system_id
            || self.completed_request.is_some() && self.completed_request_completion.is_some()
            || self.prepared_intent.is_some() != self.terminal_intent.is_some()
        {
            return Err(MetadataBackendError::Serde(
                "prepared operation version, scope, or terminal shape is invalid".into(),
            ));
        }
        if self
            .transaction
            .objects
            .iter()
            .any(|object| object.file_system_id != self.file_system_id)
            || self
                .transaction
                .versions
                .iter()
                .any(|version| version.file_system_id != self.file_system_id)
            || self
                .content_receipts
                .iter()
                .any(|receipt| receipt.receipt.file_system_id != self.file_system_id)
        {
            return Err(MetadataBackendError::Serde(
                "prepared operation contains a foreign filesystem effect".into(),
            ));
        }
        if let (Some(prepared), Some(terminal)) = (&self.prepared_intent, &self.terminal_intent) {
            if prepared.file_system_id != self.file_system_id
                || terminal.file_system_id != self.file_system_id
                || prepared.owner_epoch != self.owner_epoch
                || terminal.owner_epoch != self.owner_epoch
                || prepared.intent_id != terminal.intent_id
                || prepared.object_id != terminal.object_id
                || prepared.target_version != terminal.target_version
                || terminal.status != super::DfsWriteIntentStatus::Committed
            {
                return Err(MetadataBackendError::Serde(
                    "prepared operation terminal intent is not bound to its preparation".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn normalize(&mut self) {
        super::normalize_namespace_prepare_parts(
            &mut self.transaction,
            &mut self.expected_object_versions,
            &mut self.expected_path_occupants,
            &mut self.content_receipts,
        );
        self.required_replica_node_ids.sort();
        self.required_replica_node_ids.dedup();
    }

    pub fn canonical_digest(&self) -> Result<String> {
        self.validate()?;
        #[cfg(test)]
        PREPARED_PAYLOAD_DIGEST_COUNT.with(|count| count.set(count.get() + 1));
        let mut hasher = blake3::Hasher::new();
        hasher.update(PREPARED_OPERATION_DIGEST_DOMAIN);
        serde_json::to_writer(&mut hasher, self)
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
        Ok(format!(
            "dfs-prepared-operation-v7:blake3:{}",
            hasher.finalize().to_hex()
        ))
    }
}

impl DfsPreparedOperationDigest {
    /// Compute the exact current payload identity as DATA. This does not
    /// authorize preparation, publication, source recovery or storage activation.
    pub fn at_sequence(
        payload: &DfsPreparedOperationPayload,
        prepare_sequence: u64,
    ) -> Result<Self> {
        if prepare_sequence == 0 {
            return Err(MetadataBackendError::Serde(
                "prepared operation sequence is zero".into(),
            ));
        }
        Ok(Self::bind_canonical_digest(
            payload,
            prepare_sequence,
            payload.canonical_digest()?,
        ))
    }

    fn bind_canonical_digest(
        payload: &DfsPreparedOperationPayload,
        prepare_sequence: u64,
        payload_digest: String,
    ) -> Self {
        Self {
            protocol_version: payload.protocol_version,
            file_system_id: payload.file_system_id.clone(),
            shard_id: payload.shard_id.clone(),
            owner_epoch: payload.owner_epoch,
            operation_id: payload.operation_id.clone(),
            payload_digest,
            prepare_sequence,
        }
    }

    fn validate_shared_payload(&self, payload: &Arc<DfsPreparedOperationPayload>) -> Result<()> {
        if self.prepare_sequence == 0
            || *self
                != Self::bind_canonical_digest(
                    payload,
                    self.prepare_sequence,
                    canonical_digest_in_validation_scope(payload)?,
                )
        {
            return Err(MetadataBackendError::Serde(
                "prepared operation digest or durable scope mismatch".into(),
            ));
        }
        Ok(())
    }

    pub fn validate_payload(&self, payload: &DfsPreparedOperationPayload) -> Result<()> {
        if self.prepare_sequence == 0 || *self != Self::at_sequence(payload, self.prepare_sequence)?
        {
            return Err(MetadataBackendError::Serde(
                "prepared operation digest or durable scope mismatch".into(),
            ));
        }
        Ok(())
    }
}

impl DfsPreparedOperationRecord {
    pub fn validate(&self) -> Result<()> {
        self.digest.validate_shared_payload(&self.payload)?;
        if self
            .payload
            .expected_journal_sequence
            .is_some_and(|sequence| sequence.checked_add(1) != Some(self.digest.prepare_sequence))
        {
            return Err(MetadataBackendError::Serde(
                "prepared operation journal-cut precondition does not bind its actual slot".into(),
            ));
        }

        if let Some(entry) = &self.committed_entry {
            entry.validate(&self.digest)?;
        }
        if (self.status == DfsPreparedOperationStatus::Committed) != self.committed_entry.is_some()
        {
            return Err(MetadataBackendError::Serde(
                "prepared operation terminal status has no exact commit reference".into(),
            ));
        }
        Ok(())
    }
}

impl std::ops::Deref for DfsPreparedOperationRecord {
    type Target = DfsPreparedOperationPayload;
    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}
