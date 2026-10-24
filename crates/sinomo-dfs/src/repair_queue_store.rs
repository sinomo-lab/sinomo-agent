//! Incremental durable storage for the node-local DFS repair queue.
//!
//! The metadata journal remains the authority for write intents and receipts.
//! This store is a bounded restart accelerator: foreground commits append only
//! their repair-queue delta, while maintenance occasionally folds the framed
//! log into an atomic snapshot.

use crate::DfsError;
use crate::maintenance::DfsRepairAttemptRecord;
#[cfg(test)]
use crate::metadata_io::write_sobj_atomic;
use crate::metadata_io::{encode_sobj, metadata_durable_file_error, write_file_atomic};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_core::storage::durable_file;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read as _, Seek as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

const DFS_REPAIR_QUEUE_LOG_MAX_RECORD_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_DFS_REPAIR_QUEUE_COMPACTION_LOG_BYTES: u64 = 4 * 1024 * 1024;
const SELECTED_ORIGINAL_FIXTURE_MARKER: &str = "selected-original-data-fixture-v2.sobj";

#[cfg(test)]
thread_local! {
    // Actual content sync has completed; neither owner mutex is held here.
    static BEFORE_IMPORTED_EXECUTOR_APPEND: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectedOriginalFixtureMarker {
    version: u32,
    queue_incarnation: String,
}

/// Representation admission for one fresh physical fixture. Not a handoff,
/// executor, fsync acknowledgement or serving-floor capability.
#[derive(Clone, Debug)]
pub(crate) struct SelectedOriginalFixtureIdentity {
    root: PathBuf,
    snapshot_path: PathBuf,
    marker: SelectedOriginalFixtureMarker,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(not(unix))]
    created: std::time::SystemTime,
}

impl SelectedOriginalFixtureIdentity {
    pub(crate) fn root(&self) -> &Path { &self.root }
    pub(crate) fn snapshot_path(&self) -> &Path { &self.snapshot_path }

    /// A separately admitted shard staging directory, never another queue.
    /// The native fixture owns its parent before calling this constructor.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn admit_staging_root(&self, root: PathBuf) -> Result<Self, DfsError> {
        self.check()?;
        fs::create_dir(&root).map_err(|error| fingerprint_error(&root, error))?;
        let metadata = fs::symlink_metadata(&root).map_err(|error| fingerprint_error(&root, error))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(custody_error("selected staging root is not a fresh physical directory"));
        }
        write_file_atomic(&root.join(SELECTED_ORIGINAL_FIXTURE_MARKER), &encode_sobj(&self.marker)?)?;
        if let Some(parent) = root.parent() {
            durable_file::sync_directory(parent).map_err(metadata_durable_file_error)?;
        }
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt as _;
        let identity = Self {
            root, snapshot_path: self.snapshot_path.clone(), marker: self.marker.clone(),
            #[cfg(unix)] device: metadata.dev(),
            #[cfg(unix)] inode: metadata.ino(),
            #[cfg(not(unix))] created: metadata.created().map_err(|error| custody_error(&error.to_string()))?,
        };
        identity.check()?;
        Ok(identity)
    }

    pub(crate) fn reject_default_root(root: &Path) -> Result<(), DfsError> {
        match fs::symlink_metadata(root.join(SELECTED_ORIGINAL_FIXTURE_MARKER)) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(fingerprint_error(root, error)),
            Ok(_) => Err(custody_error("default reader rejects selected original physical layout")),
        }
    }

    pub(crate) fn check(&self) -> Result<(), DfsError> {
        let metadata = fs::symlink_metadata(&self.root).map_err(|error| fingerprint_error(&self.root, error))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(custody_error("selected original fixture is not its admitted directory"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            if (metadata.dev(), metadata.ino()) != (self.device, self.inode) {
                return Err(custody_error("selected original fixture was physically replaced"));
            }
        }
        #[cfg(not(unix))]
        if metadata.created().map_err(|error| fingerprint_error(&self.root, error))? != self.created {
            return Err(custody_error("selected original fixture was physically replaced"));
        }
        let path = self.root.join(SELECTED_ORIGINAL_FIXTURE_MARKER);
        let before = fs::symlink_metadata(&path).map_err(|error| fingerprint_error(&path, error))?;
        if !before.is_file() || before.file_type().is_symlink() || before.len() > 4096 {
            return Err(custody_error("selected original fixture marker is unavailable or unsupported"));
        }
        let marker: SelectedOriginalFixtureMarker = crate::metadata_io::read_sobj(&path)?;
        let after = fs::symlink_metadata(&path).map_err(|error| fingerprint_error(&path, error))?;
        if marker != self.marker || after.file_type().is_symlink() || !same_file_observation(&before, &after) {
            return Err(custody_error("selected original fixture layout/incarnation changed"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct DfsRepairQueueKey {
    pub intent_id: String,
    pub missing_replica_node_id: NodeId,
}

impl DfsRepairQueueKey {
    pub fn from_record(record: &DfsRepairAttemptRecord) -> Self {
        Self {
            intent_id: record.intent_id.clone(),
            missing_replica_node_id: record.missing_replica_node_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DfsRepairQueueOperation {
    /// Insert a newly committed repair without regressing existing retry state.
    EnqueueIfMissing { record: DfsRepairAttemptRecord },
    /// Replace the mutable retry state for one existing repair identity.
    Set { record: DfsRepairAttemptRecord },
    /// Remove a repair only after authoritative metadata has settled it.
    Remove { key: DfsRepairQueueKey },
    /// Versioned custody uses this SAME sequenced log. Old complete-frame
    /// decoders reject this variant instead of dropping unknown body fields.
    RepairCustody { change: Box<RepairCustodyChange> },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepairOriginalBirth {
    pub(crate) original: crate::DfsRoutedMessage,
    pub(crate) attachment: crate::DfsRpcAttachmentDescriptor,
    pub(crate) root: crate::content_store::RepairPayloadRoot,
    pub(crate) observed: Vec<crate::DfsRepairReplicaResponse>,
    pub(crate) attempts: Vec<RepairCarrierAttempt>,
    pub(crate) closed_at_sequence: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepairCarrierAttempt {
    pub(crate) sequence: u64,
    frame_id: u64,
    message_id: u64,
    delivery_state: Option<sinomo_core::network::NetworkDeliveryState>,
    delivery_observed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RepairCustodyChange {
    Initialize {
        incarnation: String,
    },
    /// Immutable foreign-origin DATA, excluded from all executable indexes.
    ImportOriginalData {
        material: Box<RepairOriginalImportMaterial>,
        installer_queue_incarnation: String,
    },
    /// A local content hold associated with an imported origin. This remains
    /// DATA: it grants no carrier, completion, application or release permit.
    AssociateImportedExecutor {
        binding: Box<ImportedRepairExecutorBinding>,
    },
    Enroll {
        birth: Box<RepairOriginalBirth>,
    },
    Attempt {
        subject: crate::content_store::RepairPayloadSubject,
        frame_id: u64,
        message_id: u64,
    },
    Delivery {
        subject: crate::content_store::RepairPayloadSubject,
        frame_id: u64,
        message_id: u64,
        delivery_state: Option<sinomo_core::network::NetworkDeliveryState>,
    },
    Observe {
        subject: crate::content_store::RepairPayloadSubject,
        response: crate::DfsRepairReplicaResponse,
    },
    Close {
        subject: crate::content_store::RepairPayloadSubject,
        observed_digest: String,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepairCustodySnapshot {
    incarnation: String,
    births: Vec<RepairOriginalBirth>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    imported_original_data: Vec<ImportedRepairOriginalData>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    imported_executor_associations: Vec<ImportedRepairExecutorAssociation>,
}

/// Exact captured material. This contains no executor or root-release permit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepairOriginalImportMaterial {
    pub(crate) cut: crate::DfsRepairHandoffCut,
    pub(crate) birth: RepairOriginalBirth,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ImportedRepairOriginalData {
    pub(crate) material: RepairOriginalImportMaterial,
    installer_queue_incarnation: String,
    installed_at_sequence: u64,
}

/// Immutable A origin and B-local physical retention are deliberately separate.
/// In particular, `bound_assignment` is historical DATA, not current authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportedRepairExecutorBinding {
    origin: crate::content_store::RepairPayloadSubject,
    original_command_digest: String,
    captured_birth_digest: String,
    imported_at_sequence: u64,
    installer_queue_incarnation: String,
    executor_node_id: NodeId,
    bound_assignment: crate::DfsMetadataShardConfig,
    local_root: crate::content_store::RepairPayloadRoot,
}

/// Synced disabled association. It is intentionally absent from every native
/// birth/attempt index and cannot construct a ClosedRepairBirth.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ImportedRepairExecutorAssociation {
    binding: ImportedRepairExecutorBinding,
    associated_at_sequence: u64,
}

impl ImportedRepairExecutorAssociation {
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn local_root(&self) -> &crate::content_store::RepairPayloadRoot {
        &self.binding.local_root
    }
}

impl RepairOriginalImportMaterial {
    fn wire_key(&self) -> (String, String) {
        (self.birth.original.source_node_id.to_string(), self.birth.root.subject.request_id.clone())
    }

    pub(crate) fn validate(&self) -> Result<(), DfsError> {
        self.cut.validate_selected_original_data()?;
        let selected = self.cut.selected_original_data.as_ref()
            .ok_or_else(|| custody_error("import has no explicit selected-original coverage"))?;
        self.birth.validate(&selected.source_queue_incarnation)?;
        let crate::DfsNetworkMessage::RepairReplica(command) = &self.birth.original.message else {
            return Err(custody_error("selected original is not Repair"));
        };
        let authority = command.authority_shard.as_ref().ok_or_else(|| custody_error("selected original lacks its immutable authority stamp"))?;
        if self.cut.version != 2
            || self.birth.closed_at_sequence.is_some()
            || self.birth.original.source_node_id != selected.producer
            || self.birth.root.subject.request_id != selected.request_id
            || self.birth.root.original_command_digest != selected.original_digest
            || self.birth.captured_data_digest()? != selected.captured_birth_digest
            || command.file_system_id != self.cut.handoff.retiring_shard.file_system_id
            || authority.file_system_id != command.file_system_id
            || authority.shard_id != self.cut.handoff.retiring_shard.shard_id
            || authority.primary_node_id != selected.producer
            || authority.epoch > self.cut.handoff.retiring_shard.epoch
            || self.birth.attempts.iter().any(|attempt| attempt.sequence == 0 || attempt.sequence > self.cut.queue_sequence)
        {
            return Err(custody_error("imported original differs from its exact source cut"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct RepairCustodyIndex {
    incarnation: String,
    births: BTreeMap<crate::content_store::RepairPayloadSubject, RepairOriginalBirth>,
    imported_original_data: BTreeMap<(String, String), ImportedRepairOriginalData>,
    imported_executor_associations: BTreeMap<(String, String), ImportedRepairExecutorAssociation>,
    // Conflict index only. Never consulted as runnable/native original work.
    associated_slots: BTreeMap<DfsRepairQueueKey, (String, String)>,
    slots: BTreeMap<DfsRepairQueueKey, crate::content_store::RepairPayloadSubject>,
    closed_requests:
        BTreeMap<(DfsRepairQueueKey, String), crate::content_store::RepairPayloadSubject>,
    observed_pending: std::collections::BTreeSet<crate::content_store::RepairPayloadSubject>,
}

/// A synced result observation is not a local-application/closure proof.
#[derive(Clone)]
pub(crate) struct RepairObservedResult {
    subject: crate::content_store::RepairPayloadSubject,
    response_digest: String,
}

/// Constructed only from an irreversible, synchronized closed birth. Private
/// fields and no Deserialize prevent a caller from asserting local application.
pub(crate) struct ClosedRepairBirth {
    root: crate::content_store::RepairPayloadRoot,
    closed_at_sequence: u64,
}

impl ClosedRepairBirth {
    pub(crate) fn root(&self) -> &crate::content_store::RepairPayloadRoot {
        &self.root
    }
}

pub(crate) fn original_repair_digest(
    message: &crate::DfsRoutedMessage,
    attachment: &crate::DfsRpcAttachmentDescriptor,
) -> Result<String, DfsError> {
    let crate::DfsNetworkMessage::RepairReplica(command) = &message.message else {
        return Err(custody_error(
            "original custody requires a RepairReplica command",
        ));
    };
    // Carrier frame/message IDs are attempt envelopes. Preserve the original
    // envelope as data, while retries retain this complete semantic head.
    #[derive(Serialize)]
    struct OriginalRepairDigestInput<'a> {
        producer: &'a NodeId,
        target: &'a NodeId,
        command: &'a crate::DfsRepairReplicaCommand,
        attachment: &'a crate::DfsRpcAttachmentDescriptor,
    }
    let bytes = sinomo_object_serde::to_bytes(
        &OriginalRepairDigestInput {
            producer: &message.source_node_id,
            target: &message.target_node_id,
            command,
            attachment,
        },
        1,
        LengthMode::U32,
    )
    .map_err(|error| custody_error(&format!("encode original Repair head: {error}")))?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"sinomo.dfs.original-repair\0v2\0");
    hash.update(&bytes);
    Ok(hash.finalize().to_hex().to_string())
}

fn repair_result_digest(response: &crate::DfsRepairReplicaResponse) -> Result<String, DfsError> {
    let bytes = sinomo_object_serde::to_bytes(response, 1, LengthMode::U32)
        .map_err(|error| custody_error(&format!("encode original Repair result: {error}")))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn custody_error(message: &str) -> DfsError {
    DfsError::Metadata {
        message: message.to_owned(),
    }
}

pub(crate) fn repair_subject(
    incarnation: &str,
    message: &crate::DfsRoutedMessage,
) -> Result<crate::content_store::RepairPayloadSubject, DfsError> {
    let crate::DfsNetworkMessage::RepairReplica(command) = &message.message else {
        return Err(custody_error("original custody requires RepairReplica"));
    };
    Ok(crate::content_store::RepairPayloadSubject {
        queue_incarnation: incarnation.to_owned(),
        producer: message.source_node_id.to_string(),
        target: message.target_node_id.to_string(),
        intent_id: command.intent_id.clone(),
        request_id: command.request_id.clone(),
    })
}

fn closed_repair_proof(
    index: &RepairCustodyIndex,
    subject: &crate::content_store::RepairPayloadSubject,
) -> Result<ClosedRepairBirth, DfsError> {
    let birth = index
        .births
        .get(subject)
        .ok_or_else(|| custody_error("closed proof has no original birth"))?;
    birth.validate(&index.incarnation)?;
    let sequence = birth
        .closed_at_sequence
        .ok_or_else(|| custody_error("original Repair is not durably closed"))?;
    Ok(ClosedRepairBirth {
        root: birth.root.clone(),
        closed_at_sequence: sequence,
    })
}

impl RepairOriginalBirth {
    pub(crate) fn captured_data_digest(&self) -> Result<String, DfsError> {
        #[derive(Serialize)]
        struct Captured<'a> { domain: &'static str, birth: &'a RepairOriginalBirth }
        Ok(blake3::hash(&encode_sobj(&Captured {
            domain: "sinomo.dfs.selected-original-full-birth.v2",
            birth: self,
        })?).to_hex().to_string())
    }
    fn key(&self) -> Result<DfsRepairQueueKey, DfsError> {
        let crate::DfsNetworkMessage::RepairReplica(command) = &self.original.message else {
            return Err(custody_error("stored original is not RepairReplica"));
        };
        Ok(DfsRepairQueueKey {
            intent_id: command.intent_id.clone(),
            missing_replica_node_id: self.original.target_node_id.clone(),
        })
    }

    pub(crate) fn validate(&self, incarnation: &str) -> Result<(), DfsError> {
        let crate::DfsNetworkMessage::RepairReplica(command) = &self.original.message else {
            return Err(custody_error("stored original is not RepairReplica"));
        };
        let subject = &self.root.subject;
        if incarnation.is_empty()
            || subject.queue_incarnation != incarnation
            || subject.producer != self.original.source_node_id.to_string()
            || subject.target != self.original.target_node_id.to_string()
            || subject.intent_id != command.intent_id
            || subject.request_id != command.request_id
            || !command.background_repair
            || !command.bytes.is_empty()
            || command.authority_shard.is_none()
            || self.attachment.content_hash.as_deref() != Some(command.content_hash.as_str())
            || self.attachment.total_size_bytes != self.root.size_bytes()
            || self.root.content_hash() != command.content_hash
            || original_repair_digest(&self.original, &self.attachment)?
                != self.root.original_command_digest
        {
            return Err(custody_error(
                "original Repair head, body, scope or queue incarnation differs",
            ));
        }
        if let crate::content_store::RepairPayloadSource::Manifest(manifest) = &self.root.source
            && (manifest.object_id != command.object_id.as_str()
                || manifest.version != command.version.as_str())
        {
            return Err(custody_error(
                "original Repair manifest belongs to another exact version",
            ));
        }
        for response in &self.observed {
            self.validate_response(response)?;
        }
        if self
            .closed_at_sequence
            .is_some_and(|sequence| sequence == 0)
            || (self.closed_at_sequence.is_some()
                && !self.observed.iter().any(|response| {
                    matches!(
                        response.result,
                        crate::DfsBlobTransferResult::Committed { .. }
                    )
                }))
        {
            return Err(custody_error(
                "closed Repair birth lacks its exact committed response",
            ));
        }
        Ok(())
    }

    fn validate_response(
        &self,
        response: &crate::DfsRepairReplicaResponse,
    ) -> Result<(), DfsError> {
        let crate::DfsNetworkMessage::RepairReplica(command) = &self.original.message else {
            return Err(custody_error("stored original is not RepairReplica"));
        };
        let valid_hash = match &response.result {
            crate::DfsBlobTransferResult::Durable { content_hash }
            | crate::DfsBlobTransferResult::Committed { content_hash } => {
                content_hash == &command.content_hash
            }
            crate::DfsBlobTransferResult::Rejected { .. } => true,
        };
        if response.request_id != command.request_id
            || response.intent_id != command.intent_id
            || response.file_system_id != command.file_system_id
            || response.object_id != command.object_id
            || response.version != command.version
            || response.replica_node_id != self.original.target_node_id
            || !valid_hash
        {
            return Err(custody_error(
                "observed result differs from its complete original Repair",
            ));
        }
        Ok(())
    }
}

impl RepairCustodyIndex {
    fn from_snapshot(
        snapshot: Option<RepairCustodySnapshot>,
        floor: u64,
        selected_reader: bool,
    ) -> Result<Self, DfsError> {
        let Some(snapshot) = snapshot else {
            return Ok(Self::default());
        };
        if !selected_reader
            && (!snapshot.imported_original_data.is_empty()
                || !snapshot.imported_executor_associations.is_empty())
        {
            return Err(custody_error("default queue reader rejects selected original DATA"));
        }
        if snapshot.incarnation.is_empty() {
            return Err(custody_error("custody snapshot has no queue incarnation"));
        }
        let mut result = Self {
            incarnation: snapshot.incarnation,
            ..Self::default()
        };
        for birth in snapshot.births {
            birth.validate(&result.incarnation)?;
            if birth
                .closed_at_sequence
                .is_some_and(|sequence| sequence > floor)
            {
                return Err(custody_error(
                    "closed Repair birth exceeds its snapshot prefix",
                ));
            }
            let subject = birth.root.subject.clone();
            if result.births.contains_key(&subject) {
                return Err(custody_error("duplicate original Repair birth"));
            }
            if birth.closed_at_sequence.is_none()
                && result.slots.insert(birth.key()?, subject.clone()).is_some()
            {
                return Err(custody_error(
                    "multiple unresolved original Repairs own one queue slot",
                ));
            }
            if birth.closed_at_sequence.is_some() {
                result
                    .closed_requests
                    .insert((birth.key()?, subject.request_id.clone()), subject.clone());
            } else if birth.observed.iter().any(|response| {
                matches!(
                    response.result,
                    crate::DfsBlobTransferResult::Committed { .. }
                )
            }) {
                result.observed_pending.insert(subject.clone());
            }
            result.births.insert(subject, birth);
        }
        for imported in snapshot.imported_original_data {
            result.validate_import(&imported.material, &imported.installer_queue_incarnation)?;
            if imported.installed_at_sequence == 0 || imported.installed_at_sequence > floor {
                return Err(custody_error("original DATA installation exceeds its local snapshot prefix"));
            }
            if result.imported_original_data.insert(imported.material.wire_key(), imported).is_some() {
                return Err(custody_error("duplicate imported original DATA in snapshot"));
            }
        }
        for association in snapshot.imported_executor_associations {
            result.validate_executor_binding(&association.binding)?;
            if association.associated_at_sequence <= association.binding.imported_at_sequence
                || association.associated_at_sequence > floor
            {
                return Err(custody_error(
                    "executor association exceeds its exact snapshot prefix",
                ));
            }
            let wire_key = (
                association.binding.origin.producer.clone(),
                association.binding.origin.request_id.clone(),
            );
            let imported = &result.imported_original_data[&wire_key];
            result
                .associated_slots
                .insert(imported.material.birth.key()?, wire_key.clone());
            if result
                .imported_executor_associations
                .insert(wire_key, association)
                .is_some()
            {
                return Err(custody_error(
                    "duplicate imported executor association in snapshot",
                ));
            }
        }
        Ok(result)
    }

    fn snapshot(&self) -> Option<RepairCustodySnapshot> {
        (!self.incarnation.is_empty()).then(|| RepairCustodySnapshot {
            incarnation: self.incarnation.clone(),
            births: self.births.values().cloned().collect(),
            imported_original_data: self.imported_original_data.values().cloned().collect(),
            imported_executor_associations: self
                .imported_executor_associations
                .values()
                .cloned()
                .collect(),
        })
    }

    fn validate_executor_binding(
        &self,
        binding: &ImportedRepairExecutorBinding,
    ) -> Result<(), DfsError> {
        let wire_key = (
            binding.origin.producer.clone(),
            binding.origin.request_id.clone(),
        );
        let imported = self.imported_original_data.get(&wire_key).ok_or_else(|| {
            custody_error("executor association has no exact installed original DATA")
        })?;
        self.validate_import(&imported.material, &binding.installer_queue_incarnation)?;
        let birth = &imported.material.birth;
        let desired = &imported.material.cut.handoff.desired_shard;
        if imported.installed_at_sequence != binding.imported_at_sequence
            || imported.installer_queue_incarnation != binding.installer_queue_incarnation
            || birth.root.subject != binding.origin
            || birth.root.original_command_digest != binding.original_command_digest
            || birth.captured_data_digest()? != binding.captured_birth_digest
            || binding.executor_node_id == birth.original.source_node_id
            || desired.primary_node_id != binding.executor_node_id
            || desired != &binding.bound_assignment
            || binding.local_root.subject != birth.root.subject
            || binding.local_root.original_command_digest != birth.root.original_command_digest
            || binding.local_root.source != birth.root.source
            || binding.local_root.store_incarnation.is_empty()
            || binding.local_root.store_incarnation == birth.root.store_incarnation
        {
            return Err(custody_error(
                "executor association changed its imported origin or local retention",
            ));
        }
        let key = birth.key()?;
        if self.slots.contains_key(&key)
            || self
                .associated_slots
                .get(&key)
                .is_some_and(|existing| existing != &wire_key)
            || self
                .imported_executor_associations
                .get(&wire_key)
                .is_some_and(|existing| existing.binding != *binding)
        {
            return Err(custody_error(
                "executor association conflicts with unresolved slot ownership",
            ));
        }
        Ok(())
    }

    fn validate_import(&self, material: &RepairOriginalImportMaterial, installer: &str) -> Result<(), DfsError> {
        material.validate()?;
        if self.incarnation.is_empty() || installer != self.incarnation
            || material.birth.root.subject.queue_incarnation == self.incarnation
        {
            return Err(custody_error("foreign original DATA cannot replace or impersonate the local queue"));
        }
        let key = material.wire_key();
        if self.births.values().any(|birth| {
            (birth.original.source_node_id.to_string(), birth.root.subject.request_id.clone()) == key
        }) {
            return Err(custody_error("import conflicts with an existing native original identity"));
        }
        if self.imported_original_data.get(&key).is_some_and(|existing| {
            existing.material != *material || existing.installer_queue_incarnation != installer
        }) {
            return Err(custody_error("import changed immutable original material, history or transfer"));
        }
        Ok(())
    }

    fn validate_change(&self, change: &RepairCustodyChange) -> Result<(), DfsError> {
        match change {
            RepairCustodyChange::AssociateImportedExecutor { binding } => {
                self.validate_executor_binding(binding)?;
            }
            RepairCustodyChange::ImportOriginalData { material, installer_queue_incarnation } => {
                self.validate_import(material, installer_queue_incarnation)?;
            }
            RepairCustodyChange::Initialize { incarnation } => {
                if incarnation.is_empty()
                    || (!self.incarnation.is_empty() && self.incarnation != *incarnation)
                {
                    return Err(custody_error(
                        "queue custody incarnation cannot be replaced",
                    ));
                }
            }
            RepairCustodyChange::Enroll { birth } => {
                birth.validate(&self.incarnation)?;
                if self.associated_slots.contains_key(&birth.key()?) {
                    return Err(custody_error("native enrollment cannot replace a disabled imported executor association"));
                }
                if self.imported_original_data.contains_key(&(birth.original.source_node_id.to_string(), birth.root.subject.request_id.clone())) {
                    return Err(custody_error("native enrollment cannot replace an imported original identity"));
                }
                if birth.closed_at_sequence.is_some()
                    || !birth.observed.is_empty()
                    || !birth.attempts.is_empty()
                {
                    return Err(custody_error(
                        "new original Repair already claims completion",
                    ));
                }
                if let Some(old) = self.births.get(&birth.root.subject) {
                    if old.closed_at_sequence.is_some() || old.root != birth.root {
                        return Err(custody_error(
                            "original Repair birth changed or attempted reopening",
                        ));
                    }
                }
                if self
                    .slots
                    .get(&birth.key()?)
                    .is_some_and(|subject| subject != &birth.root.subject)
                {
                    return Err(custody_error(
                        "unresolved original Repair cannot change request identity",
                    ));
                }
            }
            RepairCustodyChange::Observe { subject, response } => {
                self.births
                    .get(subject)
                    .ok_or_else(|| custody_error("result has no original Repair birth"))?
                    .validate_response(response)?;
            }
            RepairCustodyChange::Attempt { subject, .. } => {
                if self
                    .births
                    .get(subject)
                    .is_none_or(|birth| birth.closed_at_sequence.is_some())
                {
                    return Err(custody_error(
                        "attempt cannot reopen a missing or closed Repair birth",
                    ));
                }
            }
            RepairCustodyChange::Delivery {
                subject,
                frame_id,
                message_id,
                ..
            } => {
                if self.births.get(subject).is_none_or(|birth| {
                    !birth.attempts.iter().any(|attempt| {
                        attempt.frame_id == *frame_id && attempt.message_id == *message_id
                    })
                }) {
                    return Err(custody_error(
                        "delivery has no exact original carrier attempt",
                    ));
                }
            }
            RepairCustodyChange::Close {
                subject,
                observed_digest,
            } => {
                let birth = self
                    .births
                    .get(subject)
                    .ok_or_else(|| custody_error("closure has no original Repair birth"))?;
                if !birth.observed.iter().any(|response| {
                    matches!(
                        response.result,
                        crate::DfsBlobTransferResult::Committed { .. }
                    ) && repair_result_digest(response)
                        .is_ok_and(|digest| digest == *observed_digest)
                }) {
                    return Err(custody_error(
                        "Repair closure has no exact observed Committed result",
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_enrollment_records(
        &self,
        records: &BTreeMap<DfsRepairQueueKey, DfsRepairAttemptRecord>,
        operations: &[DfsRepairQueueOperation],
    ) -> Result<(), DfsError> {
        for operation in operations {
            let DfsRepairQueueOperation::RepairCustody { change } = operation else {
                continue;
            };
            let RepairCustodyChange::Enroll { birth } = change.as_ref() else {
                continue;
            };
            if self.births.contains_key(&birth.root.subject) {
                continue;
            }
            let key = birth.key()?;
            let mut effective = records.get(&key);
            for operation in operations {
                match operation {
                    DfsRepairQueueOperation::Set { record }
                        if DfsRepairQueueKey::from_record(record) == key =>
                    {
                        effective = Some(record)
                    }
                    DfsRepairQueueOperation::EnqueueIfMissing { record }
                        if DfsRepairQueueKey::from_record(record) == key && effective.is_none() =>
                    {
                        effective = Some(record)
                    }
                    DfsRepairQueueOperation::Remove { key: removed } if removed == &key => {
                        effective = None
                    }
                    _ => {}
                }
            }
            if effective.is_some_and(|record| {
                record.in_flight
                    || (record.completed_transfer_request_id.is_none()
                        && record.transfer_delivery_state
                            != Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent))
                    || record.completed_transfer_request_id.as_deref()
                        == Some(birth.root.subject.request_id.as_str())
            }) {
                return Err(custody_error(
                    "first custody enrollment cannot adopt newly uncovered legacy work or reuse a terminal ID",
                ));
            }
        }
        Ok(())
    }

    fn validate_scheduling_changes(
        &self,
        operations: &[DfsRepairQueueOperation],
    ) -> Result<(), DfsError> {
        for operation in operations {
            if let DfsRepairQueueOperation::Remove { key } = operation {
                if self.slots.contains_key(key) {
                    return Err(custody_error(
                        "ordinary removal cannot retire an unresolved original Repair",
                    ));
                }
                continue;
            }
            let record = match operation {
                DfsRepairQueueOperation::Set { record }
                | DfsRepairQueueOperation::EnqueueIfMissing { record } => record,
                _ => continue,
            };
            let key = DfsRepairQueueKey::from_record(record);
            if let Some(subject) = self.slots.get(&key) {
                let birth = &self.births[subject];
                let crate::DfsNetworkMessage::RepairReplica(command) = &birth.original.message
                else {
                    return Err(custody_error("stored original is not Repair"));
                };
                if record.object_id != command.object_id
                    || record.version != command.version
                    || record.content_hash != command.content_hash
                    || record.transfer_kind.is_some_and(|kind| {
                        kind != sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair
                    })
                    || record
                        .transfer_request_id
                        .as_deref()
                        .is_some_and(|id| id != command.request_id)
                    || record
                        .completed_transfer_request_id
                        .as_deref()
                        .is_some_and(|id| id != command.request_id)
                {
                    return Err(custody_error(
                        "scheduling rewrite conflicts with its unresolved original Repair",
                    ));
                }
            }
            // A retired original ID is never a new executable request, even if
            // ordinary removal/replacement erased its scheduling projection.
            if let Some(request) = record.transfer_request_id.as_deref() {
                if self
                    .closed_requests
                    .contains_key(&(key.clone(), request.to_owned()))
                {
                    return Err(custody_error(
                        "scheduling rewrite attempted closed original resurrection",
                    ));
                }
            }
        }
        Ok(())
    }

    fn apply_change(
        &mut self,
        change: &RepairCustodyChange,
        sequence: u64,
    ) -> Result<(), DfsError> {
        self.validate_change(change)?;
        match change {
            RepairCustodyChange::AssociateImportedExecutor { binding } => {
                if sequence <= binding.imported_at_sequence {
                    return Err(custody_error(
                        "executor association does not follow its exact import",
                    ));
                }
                let wire_key = (
                    binding.origin.producer.clone(),
                    binding.origin.request_id.clone(),
                );
                let imported = &self.imported_original_data[&wire_key];
                self.associated_slots
                    .insert(imported.material.birth.key()?, wire_key.clone());
                self.imported_executor_associations
                    .entry(wire_key)
                    .or_insert_with(|| ImportedRepairExecutorAssociation {
                        binding: *binding.clone(),
                        associated_at_sequence: sequence,
                    });
            }
            RepairCustodyChange::ImportOriginalData { material, installer_queue_incarnation } => {
                self.imported_original_data.entry(material.wire_key()).or_insert_with(|| ImportedRepairOriginalData {
                    material: *material.clone(),
                    installer_queue_incarnation: installer_queue_incarnation.clone(),
                    installed_at_sequence: sequence,
                });
            }
            RepairCustodyChange::Initialize { incarnation } => {
                self.incarnation = incarnation.clone()
            }
            RepairCustodyChange::Enroll { birth } => {
                self.slots.insert(birth.key()?, birth.root.subject.clone());
                self.births
                    .entry(birth.root.subject.clone())
                    .or_insert_with(|| {
                        let mut birth = *birth.clone();
                        for attempt in &mut birth.attempts {
                            attempt.sequence = sequence;
                        }
                        birth
                    });
            }
            RepairCustodyChange::Observe { subject, response } => {
                let birth = self
                    .births
                    .get_mut(subject)
                    .expect("validated original birth");
                if !birth.observed.contains(response) {
                    birth.observed.push(response.clone());
                }
                if birth.closed_at_sequence.is_none()
                    && matches!(
                        response.result,
                        crate::DfsBlobTransferResult::Committed { .. }
                    )
                {
                    self.observed_pending.insert(subject.clone());
                }
            }
            RepairCustodyChange::Attempt {
                subject,
                frame_id,
                message_id,
            } => {
                let birth = self
                    .births
                    .get_mut(subject)
                    .expect("validated original birth");
                birth.attempts.push(RepairCarrierAttempt {
                    sequence,
                    frame_id: *frame_id,
                    message_id: *message_id,
                    delivery_state: None,
                    delivery_observed: false,
                });
            }
            RepairCustodyChange::Delivery {
                subject,
                frame_id,
                message_id,
                delivery_state,
            } => {
                let birth = self
                    .births
                    .get_mut(subject)
                    .expect("validated original birth");
                let attempt = birth
                    .attempts
                    .iter_mut()
                    .rev()
                    .find(|attempt| {
                        attempt.frame_id == *frame_id && attempt.message_id == *message_id
                    })
                    .expect("validated carrier attempt");
                // A later NotSent observation cannot clear uncertainty from a
                // different attempt; each original envelope retains its fact.
                attempt.delivery_state = if attempt.delivery_observed {
                    crate::service_repair::join_transfer_delivery(
                        attempt.delivery_state,
                        *delivery_state,
                    )
                } else {
                    *delivery_state
                };
                attempt.delivery_observed = true;
            }
            RepairCustodyChange::Close { subject, .. } => {
                let birth = self
                    .births
                    .get_mut(subject)
                    .expect("validated original birth");
                birth.closed_at_sequence.get_or_insert(sequence);
                let key = birth.key()?;
                self.closed_requests
                    .insert((key.clone(), subject.request_id.clone()), subject.clone());
                self.observed_pending.remove(subject);
                if self.slots.get(&key) == Some(subject) {
                    self.slots.remove(&key);
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct DfsRepairQueueLogBatch {
    sequence: u64,
    operations: Vec<DfsRepairQueueOperation>,
}

/// Current restart checkpoint for the sequenced repair accelerator.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DfsRepairQueueSnapshotFile {
    records: Vec<DfsRepairAttemptRecord>,
    applied_through_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    custody: Option<RepairCustodySnapshot>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsRepairQueueView {
    pub head_sequence: u64,
    pub records: Vec<DfsRepairAttemptRecord>,
}

/// Bounded operator projection from an already-materialized repair queue.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsRepairQueueStatusView {
    pub head_sequence: u64,
    pub record_count: usize,
    pub records: Vec<DfsRepairAttemptRecord>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsRepairQueueMaintenancePage {
    pub total_record_count: usize,
    pub records: Vec<DfsRepairAttemptRecord>,
    /// Same-tick owner captures merged with exact existing retry state.
    pub enrollment_records: Vec<DfsRepairAttemptRecord>,
    pub has_more: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct DfsRepairQueuePageObservation {
    queue: DfsRepairQueueObservation,
    advances_scan: bool,
    after: Option<DfsRepairQueueKey>,
    records: Vec<DfsRepairAttemptRecord>,
}

impl DfsRepairQueuePageObservation {
    pub(crate) fn record_count(&self) -> usize {
        self.records.len()
    }
}

#[derive(Clone, Debug)]
pub(crate) enum DfsRepairQueuePageReconcileOutcome {
    Applied {
        removed_target_count: usize,
        operation_count: usize,
        has_more: bool,
    },
    GenerationChanged {
        current_head_sequence: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DfsRepairQueueBoundedStatus {
    Hydrated(DfsRepairQueueStatusView),
    NotHydrated,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DfsRepairQueueReconcileOutcome {
    Applied {
        head_sequence: u64,
        operation_count: usize,
    },
    GenerationChanged {
        current_head_sequence: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsRepairQueueCompactionOptions {
    pub max_log_bytes: u64,
    pub force: bool,
}

impl Default for DfsRepairQueueCompactionOptions {
    fn default() -> Self {
        Self {
            max_log_bytes: DEFAULT_DFS_REPAIR_QUEUE_COMPACTION_LOG_BYTES,
            force: false,
        }
    }
}

impl DfsRepairQueueCompactionOptions {
    pub fn force() -> Self {
        Self {
            max_log_bytes: 0,
            force: true,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsRepairQueueCompactionReport {
    pub compacted: bool,
    pub applied_through_sequence: u64,
    pub retained_record_count: usize,
    pub log_bytes_before: u64,
    pub log_bytes_after: u64,
}

/// Process-local, store-bound observation; never serialized as authority.
#[derive(Clone, Debug)]
pub(crate) struct DfsRepairQueueObservation {
    owner: Arc<()>,
    revision: u64,
    pub(crate) head_sequence: u64,
}

pub(crate) enum DfsRepairQueueObservedReconcileOutcome {
    Applied(DfsRepairQueueObservation),
    GenerationChanged { current_head_sequence: u64 },
}

#[derive(Clone, Debug)]
struct RepairQueueFileFingerprint {
    present: bool,
    bytes: u64,
    hasher: blake3::Hasher,
}

impl RepairQueueFileFingerprint {
    fn absent() -> Self {
        Self {
            present: false,
            bytes: 0,
            hasher: blake3::Hasher::new(),
        }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(bytes);
        Self {
            present: true,
            bytes: bytes.len() as u64,
            hasher,
        }
    }

    fn append_frame(&mut self, bytes: &[u8]) {
        self.present = true;
        self.hasher.update(&(bytes.len() as u32).to_le_bytes());
        self.hasher.update(bytes);
        self.bytes = self.bytes.saturating_add(4 + bytes.len() as u64);
    }
}

impl PartialEq for RepairQueueFileFingerprint {
    fn eq(&self, other: &Self) -> bool {
        self.present == other.present
            && self.bytes == other.bytes
            && self.hasher.finalize() == other.hasher.finalize()
    }
}
impl Eq for RepairQueueFileFingerprint {}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RepairQueueDiskFingerprint {
    snapshot: RepairQueueFileFingerprint,
    log: RepairQueueFileFingerprint,
}

/// Test diagnostic DATA only: no hydration, recovery or durability capability.
#[cfg(test)]
#[derive(Debug, Serialize)]
pub(crate) struct RepairOriginalDiagnostic {
    pub(crate) birth: Option<RepairOriginalBirth>,
    pub(crate) facts: serde_json::Value,
}

#[cfg(test)]
impl RepairOriginalDiagnostic {
    fn unavailable(mut self, status: &str, detail: impl ToString) -> Self {
        self.facts["status"] = status.into();
        self.facts["error"] = detail.to_string().into();
        self
    }
}

#[cfg(test)]
fn diagnostic_queue_metadata(path: &Path) -> Result<Option<fs::Metadata>, (&'static str, String)> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => Err((
            "NonRegular",
            format!("{} is not a regular non-symlink file", path.display()),
        )),
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(("IoError", format!("{}: {error}", path.display()))),
    }
}

#[cfg(test)]
fn diagnostic_queue_identity(metadata: &Option<fs::Metadata>) -> serde_json::Value {
    let Some(metadata) = metadata else {
        return serde_json::json!({"present": false});
    };
    #[cfg(unix)]
    let physical = {
        use std::os::unix::fs::MetadataExt as _;
        serde_json::json!({"device": metadata.dev(), "inode": metadata.ino(),
            "ctime_seconds": metadata.ctime(), "ctime_nanos": metadata.ctime_nsec()})
    };
    #[cfg(not(unix))]
    let physical = serde_json::json!({"unavailable": "Unix device/inode/ctime"});
    serde_json::json!({"present": true, "bytes": metadata.len(),
        "modified": format!("{:?}", metadata.modified()), "physical": physical})
}

#[cfg(test)]
fn same_diagnostic_queue_file(left: &Option<fs::Metadata>, right: &Option<fs::Metadata>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => same_file_observation(left, right),
        _ => false,
    }
}

/// Bounded, read-only fingerprint for these two existing queue files. This does
/// not parse a second queue, certify a tail, create a path or acquire a writer.
#[cfg(test)]
fn diagnostic_queue_fingerprint(
    path: &Path,
    expected: &Option<fs::Metadata>,
    remaining: &mut u64,
    bytes_read: &mut u64,
) -> Result<RepairQueueFileFingerprint, (&'static str, String)> {
    let current = diagnostic_queue_metadata(path)?;
    if !same_diagnostic_queue_file(expected, &current) {
        return Err(("Changed", format!("{} changed before read", path.display())));
    }
    let Some(before) = current else {
        return Ok(RepairQueueFileFingerprint::absent());
    };
    let cost = before
        .len()
        .checked_add(1)
        .ok_or_else(|| ("TooLarge", "length overflow".into()))?;
    if cost > *remaining {
        return Err((
            "TooLarge",
            format!(
                "{} exceeds remaining diagnostic byte budget",
                path.display()
            ),
        ));
    }
    // Charge the EOF/growth probe even when it returns zero bytes.
    *remaining -= cost;
    let mut file =
        File::open(path).map_err(|error| ("IoError", format!("{}: {error}", path.display())))?;
    let opened = file
        .metadata()
        .map_err(|error| ("IoError", error.to_string()))?;
    if !same_file_observation(&before, &opened) {
        return Err((
            "Changed",
            format!("{} replaced while opening", path.display()),
        ));
    }
    let mut fingerprint = RepairQueueFileFingerprint::from_bytes(&[]);
    let mut buffer = [0_u8; 64 * 1024];
    while fingerprint.bytes < before.len() {
        let limit = (before.len() - fingerprint.bytes).min(buffer.len() as u64) as usize;
        let read = file
            .read(&mut buffer[..limit])
            .map_err(|error| ("IoError", error.to_string()))?;
        if read == 0 {
            return Err(("Changed", format!("{} shrank during read", path.display())));
        }
        *bytes_read += read as u64;
        fingerprint.hasher.update(&buffer[..read]);
        fingerprint.bytes += read as u64;
    }
    let mut extra = [0_u8; 1];
    let read = file
        .read(&mut extra)
        .map_err(|error| ("IoError", error.to_string()))?;
    *bytes_read += read as u64;
    if read != 0 {
        return Err(("Changed", format!("{} grew during read", path.display())));
    }
    let after = file
        .metadata()
        .map_err(|error| ("IoError", error.to_string()))?;
    let current = diagnostic_queue_metadata(path)?;
    if !same_file_observation(&opened, &after)
        || !same_diagnostic_queue_file(&Some(after), &current)
    {
        return Err(("Changed", format!("{} changed during read", path.display())));
    }
    Ok(fingerprint)
}

fn fingerprint_error(path: &Path, detail: impl std::fmt::Display) -> DfsError {
    DfsError::Metadata {
        message: format!(
            "could not fingerprint DFS repair queue {}: {detail}",
            path.display()
        ),
    }
}

pub(crate) fn same_file_observation(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    if !left.is_file()
        || !right.is_file()
        || left.len() != right.len()
        || left.modified().ok() != right.modified().ok()
    {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if (left.dev(), left.ino(), left.ctime(), left.ctime_nsec())
            != (right.dev(), right.ino(), right.ctime(), right.ctime_nsec())
        {
            return false;
        }
    }
    true
}

/// Fixed-memory, exact-byte check. Metadata only detects a read racing a
/// replacement; cache equality always requires the content digest as well.
fn fingerprint_repair_queue_file(path: &Path) -> Result<RepairQueueFileFingerprint, DfsError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RepairQueueFileFingerprint::absent());
        }
        Err(error) => return Err(fingerprint_error(path, error)),
    };
    let before = file
        .metadata()
        .map_err(|error| fingerprint_error(path, error))?;
    if !before.is_file() {
        return Err(fingerprint_error(path, "not a regular file"));
    }
    let mut fingerprint = RepairQueueFileFingerprint::from_bytes(&[]);
    let mut buffer = [0_u8; 64 * 1024];
    while fingerprint.bytes < before.len() {
        let limit = (before.len() - fingerprint.bytes).min(buffer.len() as u64) as usize;
        let read = file
            .read(&mut buffer[..limit])
            .map_err(|error| fingerprint_error(path, error))?;
        if read == 0 {
            return Err(fingerprint_error(
                path,
                "file shrank during fingerprint read",
            ));
        }
        fingerprint.hasher.update(&buffer[..read]);
        fingerprint.bytes += read as u64;
    }
    let mut extra = [0_u8; 1];
    if file
        .read(&mut extra)
        .map_err(|error| fingerprint_error(path, error))?
        != 0
    {
        return Err(fingerprint_error(path, "file grew during fingerprint read"));
    }
    let after = file
        .metadata()
        .map_err(|error| fingerprint_error(path, error))?;
    let current = fs::metadata(path).map_err(|error| fingerprint_error(path, error))?;
    if !same_file_observation(&before, &after) || !same_file_observation(&after, &current) {
        return Err(fingerprint_error(
            path,
            "file changed during fingerprint read",
        ));
    }
    Ok(fingerprint)
}

#[derive(Debug)]
struct DfsRepairQueueState {
    snapshot_path: PathBuf,
    active_log_path: PathBuf,
    loaded: bool,
    owner: Arc<()>,
    revision: u64,
    fingerprint: Option<RepairQueueDiskFingerprint>,
    snapshot_floor: u64,
    head_sequence: u64,
    active_log_bytes: u64,
    valid_active_log_bytes: u64,
    /// An append sync returned an error, so replayed bytes must be synced (or
    /// a torn suffix truncated and synced) before a retry may acknowledge an
    /// idempotent no-op.
    resync_on_load: bool,
    records: BTreeMap<DfsRepairQueueKey, DfsRepairAttemptRecord>,
    custody: RepairCustodyIndex,
    selected_fixture: Option<SelectedOriginalFixtureIdentity>,
    selected_fixture_initializing: bool,
    writer_held: bool,
    custody_recovery_cursor: Option<crate::content_store::RepairPayloadSubject>,
    // Process-local position only; persisted records retain all recovery work.
    maintenance_cursor: Option<DfsRepairQueueKey>,
    maintenance_restart_pending: bool,
    maintenance_revisit_pending: bool,
}

impl DfsRepairQueueState {
    fn new(snapshot_path: PathBuf) -> Self {
        let active_log_path = repair_queue_active_log_path(&snapshot_path);
        Self {
            snapshot_path,
            active_log_path,
            loaded: false,
            owner: Arc::new(()),
            revision: 0,
            fingerprint: None,
            snapshot_floor: 0,
            head_sequence: 0,
            active_log_bytes: 0,
            valid_active_log_bytes: 0,
            resync_on_load: false,
            records: BTreeMap::new(),
            custody: RepairCustodyIndex::default(),
            selected_fixture: None,
            selected_fixture_initializing: false,
            writer_held: false,
            custody_recovery_cursor: None,
            maintenance_cursor: None,
            maintenance_restart_pending: false,
            maintenance_revisit_pending: false,
        }
    }

    fn observation(&self) -> DfsRepairQueueObservation {
        DfsRepairQueueObservation {
            owner: Arc::clone(&self.owner),
            revision: self.revision,
            head_sequence: self.head_sequence,
        }
    }

    fn next_revision(&self) -> Result<u64, DfsError> {
        self.revision
            .checked_add(1)
            .ok_or_else(|| DfsError::Metadata {
                message: "DFS repair queue resident revision overflow".to_string(),
            })
    }

    fn disk_fingerprint(&self) -> Result<RepairQueueDiskFingerprint, DfsError> {
        Ok(RepairQueueDiskFingerprint {
            snapshot: fingerprint_repair_queue_file(&self.snapshot_path)?,
            log: fingerprint_repair_queue_file(&self.active_log_path)?,
        })
    }

    fn refresh_from_disk(&mut self) -> Result<(), DfsError> {
        if self.loaded && !self.resync_on_load {
            match self.disk_fingerprint() {
                Ok(current) if self.fingerprint.as_ref() == Some(&current) => return Ok(()),
                Ok(_) => {}
                Err(error) => {
                    self.loaded = false;
                    self.fingerprint = None;
                    return Err(error);
                }
            }
        }
        self.loaded = false;
        self.maintenance_restart_pending = true;
        self.ensure_loaded()
    }

    fn ensure_loaded(&mut self) -> Result<(), DfsError> {
        if let Some(identity) = &self.selected_fixture { identity.check()?; }
        if self.loaded && !self.resync_on_load {
            return Ok(());
        }
        if self.resync_on_load && !self.writer_held {
            return self.with_writer_lock(|state| state.ensure_loaded());
        }
        self.loaded = false;
        self.fingerprint = None;
        let revision = self.next_revision()?;
        #[cfg(test)]
        if let Some(hook) = REPAIR_QUEUE_BEFORE_REPLAY_HOOK.take() {
            hook();
        }
        #[cfg(test)]
        REPAIR_QUEUE_FULL_REPLAYS.set(REPAIR_QUEUE_FULL_REPLAYS.get().saturating_add(1));
        let (snapshot, snapshot_fingerprint) =
            read_repair_queue_snapshot_with_fingerprint(&self.snapshot_path)?;
        let mut records = records_by_key(snapshot.records)?;
        let mut custody =
            RepairCustodyIndex::from_snapshot(snapshot.custody, snapshot.applied_through_sequence, self.selected_fixture.is_some())?;
        let replay = replay_repair_queue_log(
            &self.active_log_path,
            snapshot.applied_through_sequence,
            &mut records,
            &mut custody,
            self.selected_fixture.is_some(),
        )?;
        if let Some(identity) = &self.selected_fixture {
            if custody.incarnation != identity.marker.queue_incarnation
                && !(self.selected_fixture_initializing && custody.incarnation.is_empty() && replay.head_sequence == 0)
            {
                return Err(custody_error("admitted selected fixture lost or changed its original queue incarnation"));
            }
        }
        #[cfg(test)]
        if let Some(hook) = REPAIR_QUEUE_AFTER_REPLAY_HOOK.take() {
            hook();
        }
        let consumed = RepairQueueDiskFingerprint {
            snapshot: snapshot_fingerprint,
            log: replay.fingerprint.clone(),
        };
        let after = self.disk_fingerprint()?;
        if consumed != after {
            return Err(DfsError::Metadata {
                message: "DFS repair queue changed while loading its snapshot/log generation"
                    .to_string(),
            });
        }
        self.snapshot_floor = snapshot.applied_through_sequence;
        self.head_sequence = replay.head_sequence;
        self.records = records;
        self.custody = custody;
        self.active_log_bytes = replay.file_bytes;
        self.valid_active_log_bytes = replay.valid_bytes;
        if self.resync_on_load {
            self.resync_replayed_log()?;
            self.resync_on_load = false;
            // A repaired tail is a different physical generation. Reload it
            // only after the required sync succeeded, including no-op retries.
            return self.ensure_loaded();
        }
        self.revision = revision;
        self.fingerprint = Some(consumed);
        self.loaded = true;
        Ok(())
    }

    fn append_operations(
        &mut self,
        operations: Vec<DfsRepairQueueOperation>,
    ) -> Result<(u64, usize), DfsError> {
        self.with_writer_lock(move |state| state.append_operations_held(operations))
    }

    fn with_writer_lock<R>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<R, DfsError>,
    ) -> Result<R, DfsError> {
        if self.writer_held {
            return operation(self);
        }
        if let Some(identity) = &self.selected_fixture { identity.check()?; }
        if let Some(parent) = self.snapshot_path.parent() {
            fs::create_dir_all(parent).map_err(|error| fingerprint_error(parent, error))?;
        }
        let path = self.snapshot_path.with_extension("writer-lock");
        let lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| fingerprint_error(&path, error))?;
        FileExt::lock_exclusive(&lock).map_err(|error| fingerprint_error(&path, error))?;
        if let Some(identity) = &self.selected_fixture { identity.check()?; }
        self.writer_held = true;
        let result = operation(self);
        self.writer_held = false;
        // File drop also releases this lock on error/unwind. No queue lock is
        // retained when the content owner is entered by the caller.
        drop(lock);
        result
    }

    fn append_operations_held(
        &mut self,
        operations: Vec<DfsRepairQueueOperation>,
    ) -> Result<(u64, usize), DfsError> {
        self.ensure_loaded()?;
        if operations.is_empty() {
            return Ok((self.head_sequence, 0));
        }

        validate_repair_queue_operations(&self.records, &operations)?;
        self.custody.validate_scheduling_changes(&operations)?;
        self.custody
            .validate_enrollment_records(&self.records, &operations)?;
        let changes = operations
            .iter()
            .filter_map(|operation| match operation {
                DfsRepairQueueOperation::RepairCustody { change } => Some(change.as_ref()),
                _ => None,
            })
            .collect::<Vec<_>>();
        if changes.len() > 1 {
            return Err(custody_error(
                "custody updates require one indivisible framed transition",
            ));
        }
        for change in changes {
            if matches!(
                change,
                RepairCustodyChange::ImportOriginalData { .. }
                    | RepairCustodyChange::AssociateImportedExecutor { .. }
            ) && self.selected_fixture.is_none()
            {
                return Err(custody_error("selected original DATA requires its admitted physical fixture"));
            }
            self.custody.validate_change(change)?;
        }
        if self.fingerprint.as_ref() != Some(&self.disk_fingerprint()?) {
            self.loaded = false;
            return Err(custody_error(
                "repair queue changed before its locked append; retry the same observation",
            ));
        }
        // Retry DATA observations survive our own custody-only appends. Close
        // always fences them, even if its scheduling row was already absent.
        let changes_retry_view = operations.iter().any(|operation| !matches!(operation, DfsRepairQueueOperation::RepairCustody { change } if !matches!(change.as_ref(), RepairCustodyChange::Close { .. })));
        let revision = if changes_retry_view {
            self.next_revision()?
        } else {
            self.revision
        };
        self.repair_torn_tail_before_append()?;

        let sequence = self
            .head_sequence
            .checked_add(1)
            .ok_or_else(|| DfsError::Metadata {
                message: "DFS repair queue log sequence overflow".to_string(),
            })?;
        let batch = DfsRepairQueueLogBatch {
            sequence,
            operations,
        };
        let bytes =
            sinomo_object_serde::to_bytes(&batch, 1, LengthMode::U32).map_err(|source| {
                DfsError::Serialization {
                    message: format!("could not encode DFS repair queue batch: {source}"),
                }
            })?;
        if bytes.len() > DFS_REPAIR_QUEUE_LOG_MAX_RECORD_BYTES {
            return Err(DfsError::Metadata {
                message: format!(
                    "DFS repair queue batch is too large: {} bytes exceeds {}",
                    bytes.len(),
                    DFS_REPAIR_QUEUE_LOG_MAX_RECORD_BYTES
                ),
            });
        }

        let append_result = (|| {
            let mut append = durable_file::open_file_for_append(&self.active_log_path)
                .map_err(metadata_durable_file_error)
                .map_err(DfsError::from)?;
            durable_file::append_length_prefixed_records(
                &mut append.file,
                &self.active_log_path,
                [bytes.as_slice()],
                durable_file::DurableRecordAppendOptions::new(
                    durable_file::DurableLengthEndian::Little,
                )
                .sync(durable_file::DurableFileSync::All),
            )
            .map_err(metadata_durable_file_error)
            .map_err(DfsError::from)
        })();
        let written = match append_result {
            Ok(written) => written,
            Err(error) => {
                // A failed sync can leave either no frame, a torn frame, or a
                // complete but uncertain frame. Force the next call to replay
                // and synchronize/truncate that tail before it can acknowledge
                // a no-op retry.
                self.loaded = false;
                self.resync_on_load = true;
                return Err(error);
            }
        };

        let rewind_scan = self.maintenance_cursor.as_ref().is_some_and(|cursor| {
            batch.operations.iter().any(|op| match op {
                DfsRepairQueueOperation::EnqueueIfMissing { record }
                | DfsRepairQueueOperation::Set { record } => {
                    DfsRepairQueueKey::from_record(record) <= *cursor
                }
                DfsRepairQueueOperation::Remove { .. } => false,
                DfsRepairQueueOperation::RepairCustody { .. } => false,
            })
        });
        if let Err(error) = apply_repair_queue_operations(&mut self.records, &batch.operations) {
            // Validation above makes this unreachable unless the operation
            // semantics change. Reload from the durable frame rather than
            // retaining a cache that disagrees with disk.
            self.loaded = false;
            return Err(error);
        }
        for operation in &batch.operations {
            if let DfsRepairQueueOperation::RepairCustody { change } = operation {
                if let Err(error) = self.custody.apply_change(change, sequence) {
                    self.loaded = false;
                    return Err(error);
                }
            }
        }
        // Advance only the expected bytes we wrote. Reading an arbitrary
        // post-append disk hash here could silently adopt an external suffix.
        if let Some(fingerprint) = self.fingerprint.as_mut() {
            fingerprint.log.append_frame(&bytes);
        }
        if rewind_scan {
            self.maintenance_revisit_pending = true;
        }
        self.revision = revision;
        self.head_sequence = sequence;
        self.active_log_bytes = self.active_log_bytes.saturating_add(written);
        self.valid_active_log_bytes = self.active_log_bytes;
        Ok((sequence, batch.operations.len()))
    }

    fn repair_torn_tail_before_append(&mut self) -> Result<(), DfsError> {
        if self.valid_active_log_bytes >= self.active_log_bytes {
            return Ok(());
        }
        durable_file::truncate_file_and_sync(
            &self.active_log_path,
            self.valid_active_log_bytes,
            durable_file::DurableFileSync::All,
        )
        .map_err(metadata_durable_file_error)
        .map_err(DfsError::from)?;
        self.active_log_bytes = self.valid_active_log_bytes;
        self.fingerprint = Some(self.disk_fingerprint()?);
        Ok(())
    }

    fn resync_replayed_log(&mut self) -> Result<(), DfsError> {
        #[cfg(test)]
        if REPAIR_QUEUE_FAIL_RESYNC.replace(false) {
            return Err(DfsError::Metadata {
                message: "injected repair queue resync failure".into(),
            });
        }
        if self.valid_active_log_bytes < self.active_log_bytes {
            durable_file::truncate_file_and_sync(
                &self.active_log_path,
                self.valid_active_log_bytes,
                durable_file::DurableFileSync::All,
            )
            .map_err(metadata_durable_file_error)
            .map_err(DfsError::from)?;
            self.active_log_bytes = self.valid_active_log_bytes;
            return Ok(());
        }

        let mut append = durable_file::open_file_for_append(&self.active_log_path)
            .map_err(metadata_durable_file_error)
            .map_err(DfsError::from)?;
        durable_file::append_length_prefixed_records(
            &mut append.file,
            &self.active_log_path,
            std::iter::empty::<&[u8]>(),
            durable_file::DurableRecordAppendOptions::new(
                durable_file::DurableLengthEndian::Little,
            )
            .sync(durable_file::DurableFileSync::All),
        )
        .map_err(metadata_durable_file_error)
        .map_err(DfsError::from)?;
        Ok(())
    }

    /// Selected DATA installation only. Reading a replayed import is not a
    /// renewed durability acknowledgement; certify its files and namespace
    /// under the same cross-process writer before advancing the install cursor.
    fn sync_selected_import(&mut self, owned_root: &Path) -> Result<(), DfsError> {
        let identity = self.selected_fixture.as_ref()
            .ok_or_else(|| custody_error("selected DATA sync has no admitted physical fixture"))?.clone();
        identity.check()?;
        if !self.writer_held || identity.root() != owned_root || !self.snapshot_path.starts_with(owned_root)
            || self.snapshot_path.components().any(|part| matches!(part, std::path::Component::ParentDir))
        {
            return Err(custody_error("selected DATA sync has no exact owned queue namespace"));
        }
        self.repair_torn_tail_before_append()?;
        let generation = self.disk_fingerprint()?;
        if self.fingerprint.as_ref() != Some(&generation) {
            return Err(custody_error("queue changed before selected DATA synchronization"));
        }
        fn same_directory(left: &fs::Metadata, right: &fs::Metadata) -> bool {
            if !left.is_dir() || !right.is_dir() || left.file_type().is_symlink() || right.file_type().is_symlink() { return false; }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt as _;
                if (left.dev(), left.ino()) != (right.dev(), right.ino()) { return false; }
            }
            #[cfg(not(unix))]
            if left.created().ok().is_none() || left.created().ok() != right.created().ok() { return false; }
            true
        }
        let mut namespaces = Vec::new();
        let mut directory = self.snapshot_path.parent().ok_or_else(|| custody_error("selected DATA has no namespace"))?;
        loop {
            let metadata = fs::symlink_metadata(directory).map_err(|error| fingerprint_error(directory, error))?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() { return Err(custody_error("selected namespace is not an owned directory")); }
            namespaces.push((directory.to_owned(), metadata));
            if directory == owned_root {
                if let Some(parent) = directory.parent() {
                    let metadata = fs::symlink_metadata(parent).map_err(|error| fingerprint_error(parent, error))?;
                    if !metadata.is_dir() || metadata.file_type().is_symlink() { return Err(custody_error("admitted root has no stable parent")); }
                    namespaces.push((parent.to_owned(), metadata));
                }
                break;
            }
            directory = directory.parent().filter(|parent| parent.starts_with(owned_root))
                .ok_or_else(|| custody_error("selected namespace escaped its owner"))?;
        }
        let mut files = Vec::new();
        for path in [&self.snapshot_path, &self.active_log_path] {
            let metadata = match fs::symlink_metadata(path) {
                Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => Some(metadata),
                Ok(_) => return Err(custody_error("selected DATA file is not a regular owned file")),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(fingerprint_error(path, error)),
            };
            files.push((path.clone(), metadata));
        }
        for (path, before) in &files {
            let Some(before) = before else { continue; };
            let mut file = fs::OpenOptions::new().read(true).write(true).open(path).map_err(|error| fingerprint_error(path, error))?;
            if !same_file_observation(before, &file.metadata().map_err(|error| fingerprint_error(path, error))?) {
                return Err(custody_error("selected file was replaced before its barrier"));
            }
            durable_file::sync_open_file(&mut file, path, durable_file::DurableFileSync::All).map_err(metadata_durable_file_error)?;
        }
        #[cfg(test)]
        if let Some(hook) = REPAIR_QUEUE_SELECTED_IMPORT_BARRIER_HOOK.take() { hook(); }
        for (directory, _) in &namespaces {
            #[cfg(test)]
            if REPAIR_QUEUE_FAIL_SELECTED_NAMESPACE.replace(false) { return Err(custody_error("injected selected namespace barrier failure")); }
            durable_file::sync_directory(directory).map_err(metadata_durable_file_error)?;
        }
        // Keep identities AND absence states through the entire barrier;
        // equal bytes in a replacement cannot renew this certificate.
        for (path, before) in &files {
            let after = match fs::symlink_metadata(path) {
                Ok(metadata) => Some(metadata),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(fingerprint_error(path, error)),
            };
            match (before, &after) {
                (None, None) => {}
                (Some(before), Some(after)) if !after.file_type().is_symlink() && same_file_observation(before, after) => {}
                _ => return Err(custody_error("selected file generation changed across synchronization")),
            }
        }
        for (directory, before) in &namespaces {
            let after = fs::symlink_metadata(directory).map_err(|error| fingerprint_error(directory, error))?;
            if !same_directory(before, &after) { return Err(custody_error("selected namespace was physically replaced")); }
        }
        identity.check()?;
        if self.disk_fingerprint()? != generation { return Err(custody_error("queue changed during selected DATA synchronization")); }
        Ok(())
    }
}

static DFS_REPAIR_QUEUE_STORES: OnceLock<
    Mutex<BTreeMap<PathBuf, Arc<Mutex<DfsRepairQueueState>>>>,
> = OnceLock::new();

#[cfg(test)]
thread_local! {
    static SELECTED_FIXTURE_BEFORE_RESERVATION_HOOK: std::cell::Cell<Option<Box<dyn FnOnce()>>> = const { std::cell::Cell::new(None) };
    static SELECTED_FIXTURE_AFTER_RESERVATION_HOOK: std::cell::Cell<Option<Box<dyn FnOnce()>>> = const { std::cell::Cell::new(None) };
    static REPAIR_QUEUE_BEFORE_REPLAY_HOOK: std::cell::Cell<Option<Box<dyn FnOnce()>>> = const { std::cell::Cell::new(None) };
    static REPAIR_QUEUE_AFTER_REPLAY_HOOK: std::cell::Cell<Option<Box<dyn FnOnce()>>> = const { std::cell::Cell::new(None) };
    pub(crate) static REPAIR_QUEUE_SELECTED_IMPORT_BARRIER_HOOK: std::cell::Cell<Option<Box<dyn FnOnce()>>> = const { std::cell::Cell::new(None) };
    pub(crate) static REPAIR_QUEUE_FAIL_SELECTED_NAMESPACE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static REPAIR_QUEUE_FAIL_RESYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static REPAIR_QUEUE_FULL_REPLAYS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static BOUNDED_STATUS_RECORD_VISITS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[inline]
fn record_bounded_status_record_visit() {
    #[cfg(test)]
    BOUNDED_STATUS_RECORD_VISITS.set(BOUNDED_STATUS_RECORD_VISITS.get().saturating_add(1));
}

/// Cloneable handle to the process-local materialized repair queue.
///
/// Handles for the same snapshot path share one sequence allocator and map, so
/// daemon foreground and maintenance paths cannot issue conflicting frames.
#[derive(Clone, Debug)]
pub struct LocalDfsRepairQueueStore {
    inner: Arc<Mutex<DfsRepairQueueState>>,
    selected_fixture: Option<SelectedOriginalFixtureIdentity>,
}

impl LocalDfsRepairQueueStore {
    pub(crate) fn validate_selected_original_fixture(&self, identity: &SelectedOriginalFixtureIdentity) -> Result<(), DfsError> {
        identity.check()?;
        let own = self.selected_fixture.as_ref().ok_or_else(|| custody_error("selected DATA cannot use an ordinary queue handle"))?;
        own.check()?;
        if own.root != identity.root || own.marker != identity.marker || own.snapshot_path != identity.snapshot_path {
            return Err(custody_error("selected DATA fixture and queue belong to different physical owners"));
        }
        let state = self.lock_state()?;
        if state.snapshot_path != identity.snapshot_path {
            return Err(custody_error("selected DATA queue is outside its admitted fixture"));
        }
        Ok(())
    }
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn new_selected_original_fixture(
        root: impl Into<PathBuf>,
    ) -> Result<(Self, SelectedOriginalFixtureIdentity), DfsError> {
        let root = root.into();
        let snapshot_path = root.join("repair.sobj");
        Self::new_selected_original_fixture_at(root, snapshot_path)
    }

    /// Explicit native fixture selects the SAME standard queue filename.
    /// Ordinary `new` never inherits representation admission from this path.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn new_selected_original_fixture_at(
        root: PathBuf,
        snapshot_path: PathBuf,
    ) -> Result<(Self, SelectedOriginalFixtureIdentity), DfsError> {
        if snapshot_path.parent() != Some(root.as_path())
            || snapshot_path.components().any(|part| matches!(part, std::path::Component::ParentDir))
        {
            return Err(custody_error("selected queue is outside its exact admitted directory"));
        }
        match fs::symlink_metadata(&root) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(fingerprint_error(&root, error)),
            Ok(_) => return Err(custody_error("fresh selected fixture cannot adopt an existing directory")),
        }
        #[cfg(test)]
        if let Some(hook) = SELECTED_FIXTURE_BEFORE_RESERVATION_HOOK.take() { hook(); }
        let mut reserved = DfsRepairQueueState::new(snapshot_path.clone());
        reserved.selected_fixture_initializing = true;
        let inner = Arc::new(Mutex::new(reserved));
        {
            use std::collections::btree_map::Entry;
            let mut stores = DFS_REPAIR_QUEUE_STORES.get_or_init(|| Mutex::new(BTreeMap::new())).lock()
                .map_err(|_| custody_error("queue registry mutex poisoned"))?;
            match stores.entry(snapshot_path.clone()) {
                Entry::Occupied(_) => return Err(custody_error("fresh selected fixture cannot adopt an existing queue owner")),
                Entry::Vacant(entry) => { entry.insert(Arc::clone(&inner)); }
            }
        }
        // Filesystem work occurs outside the global registry lock. A default
        // handle seeing our reservation is rejected until/after initialization.
        #[cfg(test)]
        if let Some(hook) = SELECTED_FIXTURE_AFTER_RESERVATION_HOOK.take() { hook(); }
        let initialized = (|| {
            fs::create_dir(&root).map_err(|error| fingerprint_error(&root, error))?;
            let metadata = fs::symlink_metadata(&root).map_err(|error| fingerprint_error(&root, error))?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() { return Err(custody_error("fresh selected fixture changed before admission")); }
            let marker = SelectedOriginalFixtureMarker { version: 2, queue_incarnation: sinomo_core::Id::generate().to_string() };
            write_file_atomic(&root.join(SELECTED_ORIGINAL_FIXTURE_MARKER), &encode_sobj(&marker)?)?;
            if let Some(parent) = root.parent() { durable_file::sync_directory(parent).map_err(metadata_durable_file_error)?; }
            #[cfg(unix)]
            use std::os::unix::fs::MetadataExt as _;
            let identity = SelectedOriginalFixtureIdentity {
                root: root.clone(), snapshot_path: snapshot_path.clone(), marker,
                #[cfg(unix)] device: metadata.dev(),
                #[cfg(unix)] inode: metadata.ino(),
                #[cfg(not(unix))] created: metadata.created().map_err(|error| fingerprint_error(&snapshot_path, error))?,
            };
            identity.check()?;
            let store = Self { inner: Arc::clone(&inner), selected_fixture: Some(identity.clone()) };
            {
                let mut state = inner.lock().map_err(|_| custody_error("fixture queue mutex poisoned"))?;
                state.selected_fixture = Some(identity.clone());
                let initialized = state.with_writer_lock(|state| {
                    state.append_operations_held(vec![DfsRepairQueueOperation::RepairCustody {
                        change: Box::new(RepairCustodyChange::Initialize { incarnation: identity.marker.queue_incarnation.clone() }),
                    }])?;
                    state.sync_selected_import(identity.root())
                });
                state.selected_fixture_initializing = false;
                initialized?;
            }
            Ok((store, identity))
        })();
        if initialized.is_err() {
            let mut stores = DFS_REPAIR_QUEUE_STORES.get().expect("registered reservation").lock()
                .map_err(|_| custody_error("queue registry mutex poisoned"))?;
            if stores.get(&snapshot_path).is_some_and(|existing| Arc::ptr_eq(existing, &inner)) { stores.remove(&snapshot_path); }
        }
        initialized
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn reopen_selected_original_fixture(identity: &SelectedOriginalFixtureIdentity) -> Result<Self, DfsError> {
        identity.check()?;
        let mut store = Self::new(identity.snapshot_path.clone());
        store.selected_fixture = Some(identity.clone());
        {
            let mut state = store.inner.lock().map_err(|_| custody_error("fixture queue mutex poisoned"))?;
            if state.selected_fixture.as_ref().is_some_and(|expected| expected.root != identity.root || expected.marker != identity.marker) {
                return Err(custody_error("selected fixture reopen conflicts with another owner"));
            }
            state.selected_fixture = Some(identity.clone());
            state.refresh_from_disk()?;
            if state.custody.incarnation != identity.marker.queue_incarnation {
                return Err(custody_error("selected fixture lost its actual original queue incarnation"));
            }
        }
        Ok(store)
    }

    pub(crate) fn custody_incarnation(&self) -> Result<String, DfsError> {
        let mut state = self.lock_state()?;
        state.with_writer_lock(|state| {
            state.refresh_from_disk()?;
            if state.custody.incarnation.is_empty() {
                state.append_operations(vec![DfsRepairQueueOperation::RepairCustody {
                    change: Box::new(RepairCustodyChange::Initialize {
                        incarnation: sinomo_core::Id::generate().to_string(),
                    }),
                }])?;
            }
            Ok(state.custody.incarnation.clone())
        })
    }

    pub(crate) fn observed_original_repair_page(
        &self,
        limit: usize,
    ) -> Result<Vec<RepairOriginalBirth>, DfsError> {
        use std::ops::Bound::{Excluded, Unbounded};
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        let limit = limit.min(crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let after = state.custody_recovery_cursor.clone();
        let mut subjects = state
            .custody
            .observed_pending
            .range((after.as_ref().map_or(Unbounded, Excluded), Unbounded))
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        if subjects.is_empty() && after.is_some() {
            subjects = state
                .custody
                .observed_pending
                .iter()
                .take(limit)
                .cloned()
                .collect();
        }
        state.custody_recovery_cursor = subjects.last().cloned();
        Ok(subjects
            .iter()
            .map(|subject| state.custody.births[subject].clone())
            .collect())
    }

    /// Inspect only an existing, matched resident projection. In particular,
    /// do not initialize a handle or run the ordinary refresh/recovery lookup.
    #[cfg(test)]
    pub(crate) fn observe_registered_original_for_slot(
        snapshot_path: &Path,
        key: &DfsRepairQueueKey,
        max_total_read_bytes: u64,
    ) -> RepairOriginalDiagnostic {
        Self::observe_registered_original_for_slot_with_recheck(
            snapshot_path,
            key,
            max_total_read_bytes,
            || {},
        )
    }

    #[cfg(test)]
    fn observe_registered_original_for_slot_with_recheck(
        snapshot_path: &Path,
        key: &DfsRepairQueueKey,
        max_total_read_bytes: u64,
        before_recheck: impl FnOnce(),
    ) -> RepairOriginalDiagnostic {
        let log_path = repair_queue_active_log_path(snapshot_path);
        let budget =
            max_total_read_bytes.min(crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT as u64);
        let mut diagnostic = RepairOriginalDiagnostic {
            birth: None,
            facts: serde_json::json!({
                "status": "Unavailable", "snapshot_path": snapshot_path, "log_path": log_path,
                "key": key, "per_pass_budget_bytes": budget, "bytes_read": 0,
                "before": {"unavailable": "not observed"},
                "after": {"unavailable": "not observed"},
                "qualification": "resident DATA matching readable bytes; no ACK, durability, Terminal, current-holder or executor authority; per-call, not an atomic queue/content cut",
                "absence_scope": "no indexed open birth only; closed/foreign/orphan originals not enumerated",
                "historical_physical_identity": "unavailable; a byte-identical replacement before observation cannot be excluded"
            }),
        };
        let Some(stores) = DFS_REPAIR_QUEUE_STORES.get() else {
            return diagnostic.unavailable("Unregistered", "resident registry does not exist");
        };
        let inner = match stores.try_lock() {
            Ok(stores) => match stores.get(snapshot_path).cloned() {
                Some(inner) => inner,
                None => {
                    return diagnostic.unavailable("Unregistered", "no existing resident entry");
                }
            },
            Err(std::sync::TryLockError::WouldBlock) => {
                return diagnostic.unavailable("Busy", "registry busy");
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return diagnostic.unavailable("Poisoned", "registry poisoned");
            }
        };
        let state = match inner.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => {
                return diagnostic.unavailable("Busy", "resident owner busy");
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return diagnostic.unavailable("Poisoned", "resident owner poisoned");
            }
        };
        diagnostic.facts["resident"] = serde_json::json!({
            "loaded": state.loaded, "resync_pending": state.resync_on_load,
            "writer_active": state.writer_held, "head_sequence": state.head_sequence,
            "revision": state.revision, "active_log_bytes": state.active_log_bytes,
            "valid_log_bytes": state.valid_active_log_bytes,
            "existing_incarnation": (!state.custody.incarnation.is_empty()).then_some(&state.custody.incarnation)
        });
        if state.resync_on_load {
            return diagnostic.unavailable(
                "Uncertain",
                "resident append requires owner resync; diagnostic deferred",
            );
        }
        if state.writer_held {
            return diagnostic.unavailable("WriterActive", "writer active; diagnostic deferred");
        }
        if !state.loaded {
            return diagnostic
                .unavailable("Cold", "resident DATA not loaded; no hydration attempted");
        }
        let Some(expected) = state.fingerprint.as_ref() else {
            return diagnostic.unavailable("Uncertain", "resident fingerprint unavailable");
        };
        if state.valid_active_log_bytes < state.active_log_bytes {
            return diagnostic.unavailable(
                "Torn",
                "resident replay observed an incomplete suffix; no repair attempted",
            );
        }
        let before = match (
            diagnostic_queue_metadata(snapshot_path),
            diagnostic_queue_metadata(&log_path),
        ) {
            (Ok(snapshot), Ok(log)) => (snapshot, log),
            (Err((kind, error)), _) | (_, Err((kind, error))) => {
                return diagnostic.unavailable(kind, error);
            }
        };
        diagnostic.facts["before"] = serde_json::json!({
            "snapshot": diagnostic_queue_identity(&before.0), "log": diagnostic_queue_identity(&before.1)
        });
        let mut bytes_read = 0;
        let mut pass = || {
            let mut remaining = budget;
            let result = (|| {
                Ok::<_, (&'static str, String)>(RepairQueueDiskFingerprint {
                    snapshot: diagnostic_queue_fingerprint(
                        snapshot_path,
                        &before.0,
                        &mut remaining,
                        &mut bytes_read,
                    )?,
                    log: diagnostic_queue_fingerprint(
                        &log_path,
                        &before.1,
                        &mut remaining,
                        &mut bytes_read,
                    )?,
                })
            })();
            result
        };
        let first = match pass() {
            Ok(fingerprint) => fingerprint,
            Err((kind, error)) => {
                drop(pass);
                diagnostic.facts["bytes_read"] = bytes_read.into();
                return diagnostic.unavailable(kind, error);
            }
        };
        diagnostic.facts["first_fingerprint"] = serde_json::json!({
            "snapshot": {"present": first.snapshot.present, "bytes": first.snapshot.bytes,
                "blake3": first.snapshot.hasher.finalize().to_hex().to_string()},
            "log": {"present": first.log.present, "bytes": first.log.bytes,
                "blake3": first.log.hasher.finalize().to_hex().to_string()}
        });
        before_recheck();
        let second = pass();
        drop(pass);
        diagnostic.facts["bytes_read"] = bytes_read.into();
        let second = match second {
            Ok(fingerprint) => fingerprint,
            Err((kind, error)) => return diagnostic.unavailable(kind, error),
        };
        diagnostic.facts["second_fingerprint"] = serde_json::json!({
            "snapshot": {"present": second.snapshot.present, "bytes": second.snapshot.bytes,
                "blake3": second.snapshot.hasher.finalize().to_hex().to_string()},
            "log": {"present": second.log.present, "bytes": second.log.bytes,
                "blake3": second.log.hasher.finalize().to_hex().to_string()}
        });
        let after = match (
            diagnostic_queue_metadata(snapshot_path),
            diagnostic_queue_metadata(&log_path),
        ) {
            (Ok(snapshot), Ok(log)) => (snapshot, log),
            (Err((kind, error)), _) | (_, Err((kind, error))) => {
                return diagnostic.unavailable(kind, error);
            }
        };
        diagnostic.facts["after"] = serde_json::json!({
            "snapshot": diagnostic_queue_identity(&after.0), "log": diagnostic_queue_identity(&after.1)
        });
        if !same_diagnostic_queue_file(&before.0, &after.0)
            || !same_diagnostic_queue_file(&before.1, &after.1)
            || first != second
        {
            return diagnostic.unavailable(
                "Changed",
                "queue physical identity or bytes changed during observation",
            );
        }
        if &first != expected {
            return diagnostic.unavailable(
                "Dirty",
                "resident DATA differs from current readable bytes; no reload attempted",
            );
        }
        diagnostic.birth = state
            .custody
            .slots
            .get(key)
            .and_then(|subject| state.custody.births.get(subject))
            .cloned();
        diagnostic.facts["record"] = serde_json::json!(state.records.get(key));
        diagnostic.facts["status"] = "ObservedResident".into();
        diagnostic
    }

    pub(crate) fn original_repair_for_slot(
        &self,
        intent_id: &str,
        target: &NodeId,
    ) -> Result<Option<RepairOriginalBirth>, DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        let key = DfsRepairQueueKey {
            intent_id: intent_id.to_owned(),
            missing_replica_node_id: target.clone(),
        };
        Ok(state
            .custody
            .slots
            .get(&key)
            .and_then(|subject| state.custody.births.get(subject))
            .cloned())
    }

    pub(crate) fn original_repair(
        &self,
        message: &crate::DfsRoutedMessage,
    ) -> Result<Option<RepairOriginalBirth>, DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        let subject = repair_subject(&state.custody.incarnation, message)?;
        Ok(state.custody.births.get(&subject).cloned())
    }

    pub(crate) fn legacy_repair_is_uncovered(
        &self,
        intent_id: &str,
        target: &NodeId,
    ) -> Result<bool, DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        let key = DfsRepairQueueKey {
            intent_id: intent_id.to_owned(),
            missing_replica_node_id: target.clone(),
        };
        Ok(state.records.get(&key).is_some_and(|record| {
            record.completed_transfer_request_id.is_none()
                && (record.in_flight
                    || record.transfer_delivery_state
                        != Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent))
        }))
    }

    pub(crate) fn enroll_original_repair(
        &self,
        message: &crate::DfsRoutedMessage,
        attachment: &crate::DfsRpcAttachmentDescriptor,
        payload: &crate::content_store::RetainedRepairPayload,
    ) -> Result<RepairOriginalBirth, DfsError> {
        let mut state = self.lock_state()?;
        state.with_writer_lock(|state| {
            state.refresh_from_disk()?;
            let birth = RepairOriginalBirth {
                original: message.clone(),
                attachment: attachment.clone(),
                root: payload.root().clone(),
                observed: Vec::new(),
                attempts: Vec::new(),
                closed_at_sequence: None,
            };
            let crate::DfsNetworkMessage::RepairReplica(command) = &message.message else {
                return Err(custody_error("enrollment requires Repair"));
            };
            let record = DfsRepairAttemptRecord {
                intent_id: command.intent_id.clone(),
                object_id: command.object_id.clone(),
                version: command.version.clone(),
                content_hash: command.content_hash.clone(),
                missing_replica_node_id: message.target_node_id.clone(),
                attempts: 0,
                in_flight: false,
                last_attempt_unix_nanos: 0,
                next_retry_unix_nanos: 0,
                transfer_request_id: None,
                last_error: None,
                transfer_delivery_state: Some(
                    sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                ),
                completed_transfer_request_id: None,
                transfer_kind: Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair),
            };
            let key = DfsRepairQueueKey::from_record(&record);
            let mut operations = Vec::with_capacity(2);
            if let Some(existing) = state.records.get(&key) {
                // Preserve the identity check formerly supplied by Enqueue,
                // without invalidating a same-pass DATA token for a no-op.
                validate_stable_repair_identity(existing, &record)?;
            } else {
                operations.push(DfsRepairQueueOperation::EnqueueIfMissing { record });
            }
            operations.push(DfsRepairQueueOperation::RepairCustody {
                change: Box::new(RepairCustodyChange::Enroll {
                    birth: Box::new(birth.clone()),
                }),
            });
            state.append_operations(operations)?;
            Ok(state
                .custody
                .births
                .get(&birth.root.subject)
                .expect("synced original enrollment")
                .clone())
        })
    }

    pub(crate) fn record_original_repair_attempt(
        &self,
        message: &crate::DfsRoutedMessage,
    ) -> Result<(), DfsError> {
        let mut state = self.lock_state()?;
        state.with_writer_lock(|state| {
            state.refresh_from_disk()?;
            let subject = repair_subject(&state.custody.incarnation, message)?;
            let birth = state
                .custody
                .births
                .get(&subject)
                .ok_or_else(|| custody_error("carrier has no complete original Repair"))?;
            if original_repair_digest(message, &birth.attachment)?
                != birth.root.original_command_digest
            {
                return Err(custody_error("carrier changed its original Repair head"));
            }
            state.append_operations(vec![DfsRepairQueueOperation::RepairCustody {
                change: Box::new(RepairCustodyChange::Attempt {
                    subject,
                    frame_id: message.frame_id,
                    message_id: message.message_id,
                }),
            }])?;
            Ok(())
        })
    }

    pub(crate) fn validate_original_repair_dispatch(
        &self,
        message: &crate::DfsRoutedMessage,
    ) -> Result<RepairOriginalBirth, DfsError> {
        let birth = self.original_repair(message)?.ok_or_else(|| {
            custody_error("native Repair dispatch lacks its complete original body")
        })?;
        if birth.closed_at_sequence.is_some()
            || !birth.observed.is_empty()
            || original_repair_digest(message, &birth.attachment)?
                != birth.root.original_command_digest
            || !birth.attempts.iter().any(|attempt| {
                attempt.frame_id == message.frame_id && attempt.message_id == message.message_id
            })
        {
            return Err(custody_error(
                "native Repair dispatch differs from its open original/attempt or awaits result application",
            ));
        }
        Ok(birth)
    }

    pub(crate) fn record_original_repair_delivery(
        &self,
        message: &crate::DfsRoutedMessage,
        delivery_state: Option<sinomo_core::network::NetworkDeliveryState>,
    ) -> Result<(), DfsError> {
        let mut state = self.lock_state()?;
        state.with_writer_lock(|state| {
            state.refresh_from_disk()?;
            let subject = repair_subject(&state.custody.incarnation, message)?;
            let Some(birth) = state.custody.births.get(&subject) else {
                return Ok(());
            };
            if original_repair_digest(message, &birth.attachment)?
                != birth.root.original_command_digest
            {
                return Err(custody_error(
                    "carrier outcome changed its original Repair head",
                ));
            }
            state.append_operations(vec![DfsRepairQueueOperation::RepairCustody {
                change: Box::new(RepairCustodyChange::Delivery {
                    subject,
                    frame_id: message.frame_id,
                    message_id: message.message_id,
                    delivery_state,
                }),
            }])?;
            Ok(())
        })
    }

    pub(crate) fn original_repair_for_result(
        &self,
        producer: &NodeId,
        response: &crate::DfsRepairReplicaResponse,
    ) -> Result<Option<RepairOriginalBirth>, DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        let subject = crate::content_store::RepairPayloadSubject {
            queue_incarnation: state.custody.incarnation.clone(),
            producer: producer.to_string(),
            target: response.replica_node_id.to_string(),
            intent_id: response.intent_id.clone(),
            request_id: response.request_id.clone(),
        };
        let Some(birth) = state.custody.births.get(&subject) else {
            return Ok(None);
        };
        birth.validate_response(response)?;
        Ok(Some(birth.clone()))
    }

    pub(crate) fn observe_original_repair_result(
        &self,
        producer: &NodeId,
        response: &crate::DfsRepairReplicaResponse,
    ) -> Result<Option<RepairObservedResult>, DfsError> {
        let mut state = self.lock_state()?;
        state.with_writer_lock(|state| {
            state.refresh_from_disk()?;
            let subject = crate::content_store::RepairPayloadSubject {
                queue_incarnation: state.custody.incarnation.clone(),
                producer: producer.to_string(),
                target: response.replica_node_id.to_string(),
                intent_id: response.intent_id.clone(),
                request_id: response.request_id.clone(),
            };
            let Some(birth) = state.custody.births.get(&subject) else {
                return Ok(None);
            };
            birth.validate_response(response)?;
            if !birth.observed.contains(response) {
                state.append_operations(vec![DfsRepairQueueOperation::RepairCustody {
                    change: Box::new(RepairCustodyChange::Observe {
                        subject: subject.clone(),
                        response: response.clone(),
                    }),
                }])?;
            }
            Ok(Some(RepairObservedResult {
                subject,
                response_digest: repair_result_digest(response)?,
            }))
        })
    }

    pub(crate) fn close_applied_original_repair(
        &self,
        applied: &crate::service_repair::AppliedOriginalRepair,
    ) -> Result<ClosedRepairBirth, DfsError> {
        let ticket = applied.observed();
        let mut state = self.lock_state()?;
        state.with_writer_lock(|state| {
            state.refresh_from_disk()?;
            let birth = state
                .custody
                .births
                .get(&ticket.subject)
                .ok_or_else(|| custody_error("applied result lost its original birth"))?;
            state.custody.validate_change(&RepairCustodyChange::Close {
                subject: ticket.subject.clone(),
                observed_digest: ticket.response_digest.clone(),
            })?;
            if birth.closed_at_sequence.is_some() {
                return closed_repair_proof(&state.custody, &ticket.subject);
            }
            let key = birth.key()?;
            let mut operations = Vec::new();
            if let Some(record) = state.records.get(&key) {
                let mut settled = record.clone();
                settled.in_flight = false;
                settled.transfer_request_id = None;
                settled.completed_transfer_request_id = Some(ticket.subject.request_id.clone());
                settled.next_retry_unix_nanos = 0;
                operations.push(DfsRepairQueueOperation::Set { record: settled });
            }
            operations.push(DfsRepairQueueOperation::RepairCustody {
                change: Box::new(RepairCustodyChange::Close {
                    subject: ticket.subject.clone(),
                    observed_digest: ticket.response_digest.clone(),
                }),
            });
            state.append_operations(operations)?;
            closed_repair_proof(&state.custody, &ticket.subject)
        })
    }

    pub(crate) fn closed_repair_birth(
        &self,
        subject: &crate::content_store::RepairPayloadSubject,
    ) -> Result<Option<ClosedRepairBirth>, DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        if state
            .custody
            .births
            .get(subject)
            .is_none_or(|birth| birth.closed_at_sequence.is_none())
        {
            return Ok(None);
        }
        Ok(Some(closed_repair_proof(&state.custody, subject)?))
    }

    /// Bounded DATA capture. The handoff owner separately retains the native
    /// shard admission and carrier drain exclusion; sequence changes reject
    /// a capture rather than combining queue generations.
    pub(crate) fn handoff_page(
        &self,
        after: Option<&DfsRepairQueueKey>,
    ) -> Result<(u64, Vec<DfsRepairAttemptRecord>, bool), DfsError> {
        use std::ops::Bound::{Excluded, Unbounded};
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        let mut rows = state
            .records
            .range((after.map_or(Unbounded, Excluded), Unbounded));
        let page = rows
            .by_ref()
            .take(crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT)
            .map(|(_, row)| row.clone())
            .collect();
        Ok((state.head_sequence, page, rows.next().is_some()))
    }

    pub(crate) fn selected_original_handoff_data(
        &self,
        producer: &NodeId,
        request_id: &str,
    ) -> Result<(u64, RepairOriginalBirth), DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        let mut matches = state.custody.births.values().filter(|birth| {
            birth.original.source_node_id == *producer && birth.root.subject.request_id == request_id
        });
        let birth = matches.next().ok_or_else(|| custody_error("selected origin has no genuine retained birth"))?.clone();
        if matches.next().is_some() || birth.closed_at_sequence.is_some() {
            return Err(custody_error("selected origin is ambiguous or already closed"));
        }
        birth.validate(&state.custody.incarnation)?;
        Ok((state.head_sequence, birth))
    }

    /// Explicit DATA inspection, never consulted by ordinary executor lookups.
    pub(crate) fn installed_original_handoff_data(
        &self,
        producer: &NodeId,
        request_id: &str,
    ) -> Result<Option<ImportedRepairOriginalData>, DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        Ok(state.custody.imported_original_data.get(&(producer.to_string(), request_id.to_owned())).cloned())
    }

    /// Read disabled DATA through the existing queue recovery path. The value
    /// grants no current content, native original/attempt or completion permit.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn imported_executor_association(
        &self,
        producer: &NodeId,
        request_id: &str,
    ) -> Result<Option<ImportedRepairExecutorAssociation>, DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        Ok(state
            .custody
            .imported_executor_associations
            .get(&(producer.to_string(), request_id.to_owned()))
            .cloned())
    }

    /// Join actual local content retention to one exact installed foreign
    /// original. The caller separately proves current native B admission. No
    /// association is inserted into an executable/native-birth index.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn associate_imported_executor_disabled(
        &self,
        imported: &ImportedRepairOriginalData,
        executor: &NodeId,
        assignment: &crate::DfsMetadataShardConfig,
        content: &crate::LocalContentStore,
    ) -> Result<ImportedRepairExecutorAssociation, DfsError> {
        let identity = self.selected_fixture.as_ref().ok_or_else(|| {
            custody_error("disabled executor association requires an explicit selected queue")
        })?;
        self.validate_selected_original_fixture(identity)?;
        imported.material.validate()?;
        let wire_key = imported.material.wire_key();
        {
            let mut state = self.lock_state()?;
            state.refresh_from_disk()?;
            if state.custody.imported_original_data.get(&wire_key) != Some(imported)
                || assignment != &imported.material.cut.handoff.desired_shard
                || &assignment.primary_node_id != executor
                || executor == &imported.material.birth.original.source_node_id
            {
                return Err(custody_error(
                    "disabled association does not name its exact installed handoff/executor",
                ));
            }
        }
        let birth = &imported.material.birth;
        let crate::DfsNetworkMessage::RepairReplica(command) = &birth.original.message else {
            return Err(custody_error(
                "executor association requires an imported Repair",
            ));
        };
        // Reject a different local representation before retaining it. The
        // existing capture may fall back to Blob, which is not evidence for a
        // Manifest original in this deliberately narrow first slice.
        let source = match content
            .open_object_version_stream(command.object_id.as_str(), command.version.as_str())
        {
            Ok((manifest, _reader)) => {
                crate::content_store::RepairPayloadSource::Manifest(manifest)
            }
            Err(error) => {
                let crate::content_store::RepairPayloadSource::Blob {
                    content_hash,
                    size_bytes,
                } = &birth.root.source
                else {
                    return Err(error.into());
                };
                let reader = content.open_blob_stream(content_hash)?;
                if reader.size_bytes() != *size_bytes {
                    return Err(custody_error(
                        "local original payload has another exact size",
                    ));
                }
                birth.root.source.clone()
            }
        };
        if source != birth.root.source {
            return Err(custody_error(
                "local original payload differs from its sealed source representation",
            ));
        }
        let retained = content.capture_repair_payload(
            birth.root.subject.clone(),
            birth.root.original_command_digest.clone(),
            command.object_id.as_str(),
            command.version.as_str(),
            &command.content_hash,
            birth.attachment.total_size_bytes,
        )?;
        let binding = ImportedRepairExecutorBinding {
            origin: birth.root.subject.clone(),
            original_command_digest: birth.root.original_command_digest.clone(),
            captured_birth_digest: birth.captured_data_digest()?,
            imported_at_sequence: imported.installed_at_sequence,
            installer_queue_incarnation: imported.installer_queue_incarnation.clone(),
            executor_node_id: executor.clone(),
            bound_assignment: assignment.clone(),
            local_root: retained.root().clone(),
        };
        content.validate_repair_payload_root(retained.root())?;
        #[cfg(test)]
        BEFORE_IMPORTED_EXECUTOR_APPEND.with(|slot| {
            if let Some(callback) = slot.borrow_mut().take() {
                callback();
            }
        });
        // Retain the owned reader through this writer append, but never enter
        // the content owner while holding the queue state/writer lock.
        let association = {
            let mut state = self.lock_state()?;
            state.with_writer_lock(|state| {
                state.refresh_from_disk()?;
                if state.custody.imported_original_data.get(&wire_key) != Some(imported) {
                    return Err(custody_error(
                        "original import changed before disabled association",
                    ));
                }
                state.custody.validate_executor_binding(&binding)?;
                if !state
                    .custody
                    .imported_executor_associations
                    .contains_key(&wire_key)
                {
                    state.append_operations_held(vec![DfsRepairQueueOperation::RepairCustody {
                        change: Box::new(RepairCustodyChange::AssociateImportedExecutor {
                            binding: Box::new(binding.clone()),
                        }),
                    }])?;
                }
                if let Err(error) = state.sync_selected_import(identity.root()) {
                    state.loaded = false;
                    state.resync_on_load = true;
                    return Err(error);
                }
                Ok(state.custody.imported_executor_associations[&wire_key].clone())
            })?
        };
        // A replacement or failed final qualification retains historical DATA;
        // it must never turn the stored root into a current-use capability.
        content.validate_repair_payload_root(retained.root())?;
        Ok(association)
    }

    /// Import into the SAME queue/log/fsync path, without replacing any other
    /// producer. A crash before the page receipt can replay exact rows only.
    pub(crate) fn install_handoff_records(
        &self,
        records: &[DfsRepairAttemptRecord],
    ) -> Result<(), DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        for record in records {
            if state
                .records
                .get(&DfsRepairQueueKey::from_record(record))
                .is_some_and(|existing| existing != record)
            {
                return Err(DfsError::Metadata {
                    message: "repair handoff conflicts with existing original producer custody"
                        .into(),
                });
            }
        }
        state.append_operations(
            records
                .iter()
                .cloned()
                .map(|record| DfsRepairQueueOperation::EnqueueIfMissing { record })
                .collect(),
        )?;
        Ok(())
    }

    /// The handoff owner authenticates the cut and actual B physical owner.
    /// This method persists DATA only; native original/dispatch/result indexes
    /// deliberately never consult `imported_original_data`.
    pub(crate) fn install_original_handoff_data(
        &self,
        material: &RepairOriginalImportMaterial,
        owned_root: &Path,
    ) -> Result<u64, DfsError> {
        self.install_original_handoff_page(material, &[], owned_root)
    }

    pub(crate) fn install_original_handoff_page(
        &self,
        material: &RepairOriginalImportMaterial,
        records: &[DfsRepairAttemptRecord],
        owned_root: &Path,
    ) -> Result<u64, DfsError> {
        material.validate()?;
        let identity = self.selected_fixture.as_ref().ok_or_else(|| custody_error("original DATA import rejects ordinary queue handles before effects"))?;
        self.validate_selected_original_fixture(identity)?;
        if identity.root() != owned_root {
            return Err(custody_error("original DATA fixture and requested namespace differ"));
        }
        let mut state = self.lock_state()?;
        let root_metadata = fs::symlink_metadata(owned_root)
            .map_err(|error| fingerprint_error(owned_root, error))?;
        if !state.snapshot_path.starts_with(owned_root)
            || state.snapshot_path.components().any(|part| matches!(part, std::path::Component::ParentDir))
            || !root_metadata.is_dir() || root_metadata.file_type().is_symlink()
        {
            return Err(custody_error("original DATA install has no exact owned namespace"));
        }
        state.with_writer_lock(|state| {
            state.refresh_from_disk()?;
            let expected = state.selected_fixture.as_ref().ok_or_else(|| custody_error("original DATA import requires its admitted selected reader"))?;
            if state.custody.incarnation != expected.marker.queue_incarnation {
                return Err(custody_error("original DATA import cannot initialize a replacement queue incarnation"));
            }
            let installer = state.custody.incarnation.clone();
            state.custody.validate_import(material, &installer)?;
            let key = material.wire_key();
            let installed = state.custody.imported_original_data.get(&key).map(|existing| existing.installed_at_sequence);
            let mut operations = Vec::new();
            for record in records {
                match state.records.get(&DfsRepairQueueKey::from_record(record)) {
                    Some(existing) if existing != record => return Err(custody_error("selected handoff conflicts with existing retry DATA")),
                    Some(_) => {}
                    None => operations.push(DfsRepairQueueOperation::EnqueueIfMissing { record: record.clone() }),
                }
            }
            if installed.is_none() {
                operations.push(DfsRepairQueueOperation::RepairCustody {
                    change: Box::new(RepairCustodyChange::ImportOriginalData {
                        material: Box::new(material.clone()),
                        installer_queue_incarnation: installer,
                    }),
                });
            }
            // One existing writer exclusion and one frame bind the validated
            // page rows to its sole custody transition. Conflicts precede both.
            let appended_sequence = state.append_operations_held(operations)?.0;
            let sequence = installed.unwrap_or(appended_sequence);
            if let Err(error) = state.sync_selected_import(owned_root) {
                state.loaded = false;
                state.resync_on_load = true;
                return Err(error);
            }
            Ok(sequence)
        })
    }

    pub fn new(snapshot_path: impl Into<PathBuf>) -> Self {
        let snapshot_path = snapshot_path.into();
        let mut stores = DFS_REPAIR_QUEUE_STORES
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let inner = stores
            .entry(snapshot_path.clone())
            .or_insert_with(|| Arc::new(Mutex::new(DfsRepairQueueState::new(snapshot_path))))
            .clone();
        Self { inner, selected_fixture: None }
    }

    /// Make the next access rebuild from the durable snapshot and active log.
    ///
    /// Tests use this to force cold recovery. Production maintenance uses
    /// exact content-fingerprint refresh instead of unconditional replay.
    #[cfg(test)]
    pub(crate) fn reload_from_disk_on_next_access(&self) {
        if let Ok(mut state) = self.inner.lock() {
            state.loaded = false;
        }
    }

    pub fn view(&self) -> Result<DfsRepairQueueView, DfsError> {
        let mut state = self.lock_state()?;
        state.ensure_loaded()?;
        Ok(DfsRepairQueueView {
            head_sequence: state.head_sequence,
            records: state.records.values().cloned().collect(),
        })
    }

    /// Read actual typed rows for one owner's targets at the same current cut.
    /// Missing keys supply no delivery or settlement evidence.
    pub(crate) fn records_for_targets(
        &self,
        intent_id: &str,
        targets: &[NodeId],
    ) -> Result<Vec<DfsRepairAttemptRecord>, DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        Ok(targets
            .iter()
            .filter_map(|target| {
                state
                    .records
                    .get(&DfsRepairQueueKey {
                        intent_id: intent_id.to_owned(),
                        missing_replica_node_id: target.clone(),
                    })
                    .cloned()
            })
            .collect())
    }

    /// Observe only actual transfer targets under the SAME maintenance token.
    /// Delivery completion does not acknowledge a scan or create absent rows.
    pub(crate) fn observe_transfer_targets(
        &self,
        intent_id: &str,
        targets: &[NodeId],
    ) -> Result<(Vec<DfsRepairAttemptRecord>, DfsRepairQueuePageObservation), DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        let records = targets
            .iter()
            .filter_map(|target| {
                state
                    .records
                    .get(&DfsRepairQueueKey {
                        intent_id: intent_id.to_owned(),
                        missing_replica_node_id: target.clone(),
                    })
                    .cloned()
            })
            .collect::<Vec<_>>();
        let observation = DfsRepairQueuePageObservation {
            queue: state.observation(),
            advances_scan: false,
            after: state.maintenance_cursor.clone(),
            records: records.clone(),
        };
        Ok((records, observation))
    }

    /// Set one SAME fully observed transfer through the existing page checkpoint.
    /// Concurrent changes and removals win; missing rows supply no new custody.
    pub(crate) fn checkpoint_transfer_record(
        &self,
        before: &DfsRepairAttemptRecord,
        desired: &DfsRepairAttemptRecord,
    ) -> Result<bool, DfsError> {
        validate_stable_repair_identity(before, desired)?;
        if DfsRepairQueueKey::from_record(before) != DfsRepairQueueKey::from_record(desired)
            || before.transfer_kind.is_none()
            || before.transfer_kind != desired.transfer_kind
            || before.transfer_request_id.is_none()
            || before.completed_transfer_request_id.is_some()
        {
            return Ok(false);
        }
        let (records, observation) = self.observe_transfer_targets(
            &before.intent_id,
            std::slice::from_ref(&before.missing_replica_node_id),
        )?;
        if records.as_slice() != std::slice::from_ref(before) {
            return Ok(false);
        }
        Ok(matches!(
            self.reconcile_maintenance_page(
                &observation,
                0,
                std::slice::from_ref(before),
                std::slice::from_ref(desired),
                &[],
                &[]
            )?,
            DfsRepairQueuePageReconcileOutcome::Applied { .. }
        ))
    }

    /// A validated live owner may wake only its exact settled observations.
    /// This changes scheduling DATA through the existing Set/log/fsync family;
    /// it grants no transfer authority and never creates a missing queue row.
    pub(crate) fn wake_observed_settled_records(
        &self,
        observed: &[DfsRepairAttemptRecord],
        next_retry_unix_nanos: u64,
    ) -> Result<Vec<DfsRepairAttemptRecord>, DfsError> {
        self.wake_observed_scheduling_records_if(
            observed,
            next_retry_unix_nanos,
            &crate::repair::retained_repair_schedule_can_wake,
        )
    }

    /// The same observed Set/log CAS with an explicit scheduling-only selector.
    pub(crate) fn wake_observed_scheduling_records_if(
        &self,
        observed: &[DfsRepairAttemptRecord],
        next_retry_unix_nanos: u64,
        eligible: &dyn Fn(&DfsRepairAttemptRecord) -> bool,
    ) -> Result<Vec<DfsRepairAttemptRecord>, DfsError> {
        if observed.is_empty() {
            return Ok(Vec::new());
        }
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        let mut before = BTreeMap::new();
        for record in observed {
            let key = DfsRepairQueueKey::from_record(record);
            if let Some(previous) = before.insert(key, record)
                && previous != record
            {
                return Err(DfsError::Metadata {
                    message: "retained scheduling wake has conflicting exact observations".into(),
                });
            }
        }
        let mut operations = Vec::new();
        for (key, record) in &before {
            if state.records.get(key) == Some(*record) && eligible(record) {
                let mut updated = (*record).clone();
                updated.next_retry_unix_nanos = next_retry_unix_nanos;
                if updated != **record {
                    operations.push(DfsRepairQueueOperation::Set { record: updated });
                }
            }
        }
        state.append_operations(operations)?;
        // Return the actual current rows, including a newer concurrent claim.
        // Removed rows stay absent; the activation cannot re-enroll them.
        Ok(before
            .keys()
            .filter_map(|key| state.records.get(key).cloned())
            .collect())
    }

    /// Maintenance observes external changes through exact streamed content
    /// fingerprints while retaining the hot decoded queue when bytes match.
    pub(crate) fn view_for_maintenance(
        &self,
    ) -> Result<(DfsRepairQueueView, DfsRepairQueueObservation), DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        Ok((
            DfsRepairQueueView {
                head_sequence: state.head_sequence,
                records: state.records.values().cloned().collect(),
            },
            state.observation(),
        ))
    }

    /// Observe one bounded window without cloning unrelated queue records.
    /// Total cardinality remains available to placement eligibility.
    pub(crate) fn maintenance_page(
        &self,
        max_records: usize,
        captured_records: &[DfsRepairAttemptRecord],
        expected: Option<&DfsRepairQueuePageObservation>,
    ) -> Result<(DfsRepairQueueMaintenancePage, DfsRepairQueuePageObservation), DfsError> {
        use std::ops::Bound;
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        if let Some(expected) = expected {
            if !Arc::ptr_eq(&state.owner, &expected.queue.owner)
                || state.revision != expected.queue.revision
                || state.maintenance_cursor != expected.after
            {
                return Err(DfsError::Metadata {
                    message: "repair queue generation changed before same-tick enrollment".into(),
                });
            }
        }
        if max_records > 0 && state.maintenance_restart_pending {
            state.maintenance_cursor = None;
            state.maintenance_restart_pending = false;
            state.maintenance_revisit_pending = false;
        }
        let after = state.maintenance_cursor.clone();
        let lower = after.as_ref().map_or(Bound::Unbounded, Bound::Excluded);
        let mut entries = state.records.range((lower, Bound::Unbounded));
        let records = entries
            .by_ref()
            .take(max_records)
            .map(|(_, record)| record.clone())
            .collect::<Vec<_>>();
        let has_more = entries.next().is_some();
        let observed = DfsRepairQueuePageObservation {
            queue: state.observation(),
            advances_scan: max_records > 0,
            after,
            records: records.clone(),
        };
        let mut selected = records;
        let mut enrollment_records = Vec::with_capacity(captured_records.len());
        let mut enrollment_remaining = false;
        let mut seen = std::collections::BTreeSet::new();
        for captured in captured_records {
            let key = DfsRepairQueueKey::from_record(captured);
            if !seen.insert(key.clone()) {
                continue;
            }
            if let Some(existing) = state.records.get(&key) {
                validate_stable_repair_identity(existing, captured)?;
                enrollment_records.push(existing.clone());
            } else {
                enrollment_records.push(captured.clone());
                if selected.len() < max_records {
                    selected.push(captured.clone());
                } else {
                    enrollment_remaining = true;
                }
            }
        }
        let page = DfsRepairQueueMaintenancePage {
            total_record_count: state.records.len(),
            records: selected,
            enrollment_records,
            has_more: has_more || enrollment_remaining,
        };
        Ok((page, observed))
    }

    /// Commit only observed work and owner-captured additions through the
    /// existing durable queue log. Cursor movement acknowledges scanning,
    /// never effect authority; it is atomic with the successful queue delta.
    pub(crate) fn reconcile_maintenance_page(
        &self,
        expected: &DfsRepairQueuePageObservation,
        examined_records: usize,
        reconciled_before: &[DfsRepairAttemptRecord],
        desired_records: &[DfsRepairAttemptRecord],
        retired_intents: &[DfsRepairAttemptRecord],
        enqueued_records: &[DfsRepairAttemptRecord],
    ) -> Result<DfsRepairQueuePageReconcileOutcome, DfsError> {
        use std::ops::Bound;
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        if !Arc::ptr_eq(&state.owner, &expected.queue.owner)
            || state.revision != expected.queue.revision
            || state.maintenance_cursor != expected.after
        {
            return Ok(DfsRepairQueuePageReconcileOutcome::GenerationChanged {
                current_head_sequence: state.head_sequence,
            });
        }
        if examined_records > expected.records.len() {
            return Err(DfsError::Metadata {
                message: "repair page examined beyond its observation".into(),
            });
        }
        let mut retired = BTreeMap::new();
        for record in retired_intents {
            if let Some(previous) = retired.insert(record.intent_id.clone(), record) {
                validate_stable_repair_intent_identity(previous, record)?;
            }
        }
        let is_retired = |record: &DfsRepairAttemptRecord| -> Result<bool, DfsError> {
            let Some(retired) = retired.get(&record.intent_id) else {
                return Ok(false);
            };
            validate_stable_repair_intent_identity(retired, record)?;
            Ok(true)
        };
        let mut desired = BTreeMap::new();
        for record in desired_records {
            if !is_retired(record)? {
                desired.insert(DfsRepairQueueKey::from_record(record), record);
            }
        }
        let mut before = BTreeMap::new();
        for record in expected
            .records
            .iter()
            .take(examined_records)
            .chain(reconciled_before)
        {
            let key = DfsRepairQueueKey::from_record(record);
            if let Some(existing) = state.records.get(&key) {
                validate_stable_repair_identity(existing, record)?;
            }
            before.insert(key, record);
        }
        let mut operations = BTreeMap::new();
        // Intent retirement examines its indexed targets, including siblings
        // outside this candidate window. Retain unresolved transfer custody;
        // every removal still binds the exact retired object/version/hash.
        for record in retired.values() {
            let lower = DfsRepairQueueKey {
                intent_id: record.intent_id.clone(),
                missing_replica_node_id: NodeId::new("00000000000000000000000000")
                    .expect("minimum ULID"),
            };
            for (key, existing) in state
                .records
                .range(lower..)
                .take_while(|(key, _)| key.intent_id == record.intent_id)
            {
                validate_stable_repair_intent_identity(record, existing)?;
                if crate::service_repair::repair_transfer_custody_unresolved(existing)
                    || state.custody.slots.contains_key(key)
                {
                    // A terminal scheduling checkpoint is not V2 application
                    // closure. Preserve that birth while retiring unrelated rows.
                    continue;
                }
                operations.insert(
                    key.clone(),
                    DfsRepairQueueOperation::Remove { key: key.clone() },
                );
            }
        }
        for (key, old) in &before {
            if is_retired(old)?
                && (state.custody.slots.contains_key(key)
                    || state
                        .records
                        .get(key)
                        .is_some_and(crate::service_repair::repair_transfer_custody_unresolved))
            {
                continue;
            }
            match desired.get(key) {
                Some(record) => {
                    validate_stable_repair_identity(old, record)?;
                    if state.records.get(key) != Some(*record) {
                        operations.insert(
                            key.clone(),
                            DfsRepairQueueOperation::Set {
                                record: (*record).clone(),
                            },
                        );
                    }
                }
                None if state.records.contains_key(key)
                    && !state.custody.slots.contains_key(key) =>
                {
                    operations.insert(
                        key.clone(),
                        DfsRepairQueueOperation::Remove { key: key.clone() },
                    );
                }
                None => {}
            }
        }
        // New work created by earlier phases participates in this exact
        // checkpoint. Captured old work cannot re-enqueue a retired identity.
        let mut rewind = false;
        let next_cursor = expected
            .records
            .get(examined_records.wrapping_sub(1))
            .map(DfsRepairQueueKey::from_record)
            .or_else(|| expected.after.clone());
        for captured in enqueued_records {
            if is_retired(captured)? {
                continue;
            }
            let key = DfsRepairQueueKey::from_record(captured);
            let record = desired.get(&key).copied().unwrap_or(captured);
            validate_stable_repair_identity(captured, record)?;
            if let Some(existing) = state.records.get(&key) {
                validate_stable_repair_identity(existing, record)?;
                continue;
            }
            rewind |= next_cursor.as_ref().is_some_and(|cursor| &key <= cursor);
            operations
                .entry(key)
                .or_insert_with(|| DfsRepairQueueOperation::EnqueueIfMissing {
                    record: record.clone(),
                });
        }
        let new_work = operations.iter().any(|(key, operation)| {
            !state.records.contains_key(key)
                && !matches!(operation, DfsRepairQueueOperation::Remove { .. })
        });
        let removed_target_count = operations
            .values()
            .filter(|op| matches!(op, DfsRepairQueueOperation::Remove { .. }))
            .count();
        let (_, operation_count) = state.append_operations(operations.into_values().collect())?;
        if !expected.advances_scan {
            // A completion-only delta cannot reopen an already completed
            // parked sweep. Only newly enrolled work requests continuation.
            return Ok(DfsRepairQueuePageReconcileOutcome::Applied {
                removed_target_count,
                operation_count,
                has_more: new_work,
            });
        }
        state.maintenance_revisit_pending |= rewind;
        let lower = next_cursor
            .as_ref()
            .map_or(Bound::Unbounded, Bound::Excluded);
        let suffix_remaining = state
            .records
            .range((lower, Bound::Unbounded))
            .next()
            .is_some();
        let wrap_remaining =
            !suffix_remaining && state.maintenance_revisit_pending && !state.records.is_empty();
        let has_more = suffix_remaining || wrap_remaining;
        state.maintenance_cursor = if suffix_remaining { next_cursor } else { None };
        if !suffix_remaining {
            state.maintenance_revisit_pending = false;
        }
        Ok(DfsRepairQueuePageReconcileOutcome::Applied {
            removed_target_count,
            operation_count,
            has_more,
        })
    }

    pub(crate) fn reconcile_observed(
        &self,
        expected: &DfsRepairQueueObservation,
        desired_records: &[DfsRepairAttemptRecord],
    ) -> Result<DfsRepairQueueObservedReconcileOutcome, DfsError> {
        let mut state = self.lock_state()?;
        state.refresh_from_disk()?;
        if !Arc::ptr_eq(&state.owner, &expected.owner) || state.revision != expected.revision {
            return Ok(DfsRepairQueueObservedReconcileOutcome::GenerationChanged {
                current_head_sequence: state.head_sequence,
            });
        }
        let operations = reconcile_repair_queue_operations(&state.records, desired_records)?;
        state.append_operations(operations)?;
        Ok(DfsRepairQueueObservedReconcileOutcome::Applied(
            state.observation(),
        ))
    }

    /// Read queue cardinality plus a bounded deterministic sample without
    /// loading/replaying durable state or cloning the complete queue. If the
    /// process-local generation has never been materialized, status is
    /// explicitly unavailable rather than turning an operator probe into a
    /// cold recovery pass.
    pub fn bounded_status_view(
        &self,
        max_records: usize,
    ) -> Result<DfsRepairQueueBoundedStatus, DfsError> {
        let state = match self.inner.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(DfsError::Metadata {
                    message: "bounded DFS repair queue status unavailable while the resident queue is being updated"
                        .to_string(),
                });
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(DfsError::Metadata {
                    message: "bounded DFS repair queue status lock is poisoned".to_string(),
                });
            }
        };
        if !state.loaded {
            return Ok(DfsRepairQueueBoundedStatus::NotHydrated);
        }
        let records = state
            .records
            .values()
            .take(max_records)
            .inspect(|_| record_bounded_status_record_visit())
            .cloned()
            .collect();
        Ok(DfsRepairQueueBoundedStatus::Hydrated(
            DfsRepairQueueStatusView {
                head_sequence: state.head_sequence,
                record_count: state.records.len(),
                records,
            },
        ))
    }

    pub fn append_enqueues(
        &self,
        records: impl IntoIterator<Item = DfsRepairAttemptRecord>,
    ) -> Result<usize, DfsError> {
        let mut state = self.lock_state()?;
        state.ensure_loaded()?;
        let mut operations = Vec::new();
        let mut queued = BTreeMap::new();
        for record in records {
            let key = DfsRepairQueueKey::from_record(&record);
            if let Some(existing) = queued.get(&key) {
                validate_stable_repair_identity(existing, &record)?;
                continue;
            }
            queued.insert(key.clone(), record.clone());
            if let Some(existing) = state.records.get(&key) {
                validate_stable_repair_identity(existing, &record)?;
                continue;
            }
            operations.push(DfsRepairQueueOperation::EnqueueIfMissing { record });
        }
        let (_, operation_count) = state.append_operations(operations)?;
        Ok(operation_count)
    }

    pub fn append_operations(
        &self,
        operations: Vec<DfsRepairQueueOperation>,
    ) -> Result<usize, DfsError> {
        if operations
            .iter()
            .any(|operation| matches!(operation, DfsRepairQueueOperation::RepairCustody { .. }))
        {
            return Err(custody_error(
                "custody transitions require their dedicated original/result/application owner",
            ));
        }
        let mut state = self.lock_state()?;
        let (_, operation_count) = state.append_operations(operations)?;
        Ok(operation_count)
    }

    pub fn reconcile(
        &self,
        expected_head_sequence: u64,
        desired_records: &[DfsRepairAttemptRecord],
    ) -> Result<DfsRepairQueueReconcileOutcome, DfsError> {
        let mut state = self.lock_state()?;
        state.ensure_loaded()?;
        if state.head_sequence != expected_head_sequence {
            return Ok(DfsRepairQueueReconcileOutcome::GenerationChanged {
                current_head_sequence: state.head_sequence,
            });
        }
        let operations = reconcile_repair_queue_operations(&state.records, desired_records)?;
        let (head_sequence, operation_count) = state.append_operations(operations)?;
        Ok(DfsRepairQueueReconcileOutcome::Applied {
            head_sequence,
            operation_count,
        })
    }

    /// Reconcile a complete desired queue through the normal sequenced log.
    pub fn replace_records(
        &self,
        desired_records: &[DfsRepairAttemptRecord],
    ) -> Result<usize, DfsError> {
        let mut state = self.lock_state()?;
        state.ensure_loaded()?;
        let operations = reconcile_repair_queue_operations(&state.records, desired_records)?;
        let (_, operation_count) = state.append_operations(operations)?;
        Ok(operation_count)
    }

    pub fn compact_if_needed(
        &self,
        options: DfsRepairQueueCompactionOptions,
    ) -> Result<DfsRepairQueueCompactionReport, DfsError> {
        let mut state = self.lock_state()?;
        state.with_writer_lock(|state| {
            state.refresh_from_disk()?;
            let log_bytes_before = state.active_log_bytes;
            if state.head_sequence == 0
                || (!options.force && log_bytes_before < options.max_log_bytes)
            {
                return Ok(DfsRepairQueueCompactionReport {
                    compacted: false,
                    applied_through_sequence: state.snapshot_floor,
                    retained_record_count: state.records.len(),
                    log_bytes_before,
                    log_bytes_after: log_bytes_before,
                });
            }

            let snapshot = DfsRepairQueueSnapshotFile {
                records: state.records.values().cloned().collect(),
                applied_through_sequence: state.head_sequence,
                custody: state.custody.snapshot(),
            };
            let snapshot_bytes = encode_sobj(&snapshot)?;
            if let Err(error) = write_file_atomic(&state.snapshot_path, &snapshot_bytes) {
                state.loaded = false;
                return Err(error.into());
            }
            let log_present = state.active_log_path.exists();
            if log_present
                && let Err(error) = durable_file::truncate_file_and_sync(
                    &state.active_log_path,
                    0,
                    durable_file::DurableFileSync::All,
                )
            {
                // The published snapshot floor makes the retained old log safe to
                // replay. Reload it on retry instead of trusting cached lengths.
                state.loaded = false;
                return Err(DfsError::from(metadata_durable_file_error(error)));
            }
            // Owned compaction preserves the exact logical queue, so existing
            // maintenance observations remain valid across this physical rewrite.
            state.fingerprint = Some(RepairQueueDiskFingerprint {
                snapshot: RepairQueueFileFingerprint::from_bytes(&snapshot_bytes),
                log: if log_present {
                    RepairQueueFileFingerprint::from_bytes(&[])
                } else {
                    RepairQueueFileFingerprint::absent()
                },
            });
            state.snapshot_floor = state.head_sequence;
            state.active_log_bytes = 0;
            state.valid_active_log_bytes = 0;
            Ok(DfsRepairQueueCompactionReport {
                compacted: true,
                applied_through_sequence: state.head_sequence,
                retained_record_count: state.records.len(),
                log_bytes_before,
                log_bytes_after: 0,
            })
        })
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, DfsRepairQueueState>, DfsError> {
        let state = self.inner.lock().map_err(|_| DfsError::Metadata {
            message: "DFS repair queue store lock poisoned".to_string(),
        })?;
        if state.selected_fixture_initializing && self.selected_fixture.is_none() {
            return Err(custody_error("default queue handle rejects pending selected fixture reservation"));
        }
        match (&self.selected_fixture, &state.selected_fixture) {
            (Some(identity), Some(expected)) if identity.root == expected.root && identity.marker == expected.marker && identity.snapshot_path == expected.snapshot_path => identity.check()?,
            (None, None) => {
                if let Some(parent) = state.snapshot_path.parent() {
                    match fs::symlink_metadata(parent.join(SELECTED_ORIGINAL_FIXTURE_MARKER)) {
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(fingerprint_error(parent, error)),
                        Ok(_) => return Err(custody_error("default queue handle rejects selected original fixture")),
                    }
                }
            }
            _ => return Err(custody_error("queue handle has a different selected original reader")),
        }
        Ok(state)
    }

    #[cfg(test)]
    pub(crate) fn evict_for_test(snapshot_path: &Path) {
        if let Some(stores) = DFS_REPAIR_QUEUE_STORES.get() {
            stores
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(snapshot_path);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DfsRepairQueueReplayReport {
    head_sequence: u64,
    file_bytes: u64,
    valid_bytes: u64,
    fingerprint: RepairQueueFileFingerprint,
}

fn replay_repair_queue_log(
    path: &Path,
    snapshot_floor: u64,
    records: &mut BTreeMap<DfsRepairQueueKey, DfsRepairAttemptRecord>,
    custody: &mut RepairCustodyIndex,
    selected_reader: bool,
) -> Result<DfsRepairQueueReplayReport, DfsError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DfsRepairQueueReplayReport {
                head_sequence: snapshot_floor,
                file_bytes: 0,
                valid_bytes: 0,
                fingerprint: RepairQueueFileFingerprint::absent(),
            });
        }
        Err(source) => {
            return Err(DfsError::Metadata {
                message: format!(
                    "could not open DFS repair queue log {}: {source}",
                    path.display()
                ),
            });
        }
    };
    let file_bytes = file
        .metadata()
        .map_err(|source| DfsError::Metadata {
            message: format!(
                "could not stat DFS repair queue log {}: {source}",
                path.display()
            ),
        })?
        .len();
    let mut fingerprint = RepairQueueFileFingerprint::from_bytes(&[]);
    let mut head_sequence = snapshot_floor;
    let mut expected_sequence = snapshot_floor.saturating_add(1);
    let valid_bytes = loop {
        let record_start = file
            .stream_position()
            .map_err(|source| DfsError::Metadata {
                message: format!(
                    "could not seek DFS repair queue log {}: {source}",
                    path.display()
                ),
            })?;
        let Some(record) = durable_file::read_length_prefixed_record(
            &mut file,
            path,
            durable_file::DurableRecordReadOptions::new(durable_file::DurableLengthEndian::Little)
                .max_record_bytes(DFS_REPAIR_QUEUE_LOG_MAX_RECORD_BYTES),
        )
        .map_err(metadata_durable_file_error)
        .map_err(DfsError::from)?
        else {
            // The shared framing reader rewinds to the incomplete record's
            // start. Hash its remaining physical bytes too: they do not apply
            // queue operations, but they belong to this exact generation.
            let mut buffer = [0_u8; 64 * 1024];
            while fingerprint.bytes < file_bytes {
                let limit = (file_bytes - fingerprint.bytes).min(buffer.len() as u64) as usize;
                file.read_exact(&mut buffer[..limit])
                    .map_err(|error| fingerprint_error(path, error))?;
                fingerprint.hasher.update(&buffer[..limit]);
                fingerprint.bytes += limit as u64;
            }
            break record_start;
        };
        if record.record_start != fingerprint.bytes || record.record_end > file_bytes {
            return Err(fingerprint_error(path, "log changed during framed replay"));
        }
        // U32 little-endian framing has one exact representation. Hash that
        // prefix and the very payload buffer decoded below, including frames
        // covered by the snapshot floor. A later re-open cannot stand in for
        // these consumed bytes (A -> B -> A would otherwise adopt B under A).
        fingerprint.append_frame(&record.bytes);
        let batch: DfsRepairQueueLogBatch = sinomo_object_serde::from_bytes(&record.bytes)
            .map_err(|source| DfsError::Serialization {
                message: format!(
                    "could not decode DFS repair queue log {} at byte {}: {source}",
                    path.display(),
                    record.record_start
                ),
            })?;
        if !selected_reader
            && batch.operations.iter().any(|operation| matches!(operation,
                DfsRepairQueueOperation::RepairCustody { change } if matches!(change.as_ref(),
                    RepairCustodyChange::ImportOriginalData { .. }
                        | RepairCustodyChange::AssociateImportedExecutor { .. })))
        {
            return Err(custody_error("default queue replay rejects selected original DATA"));
        }
        if batch.sequence <= snapshot_floor {
            if custody.incarnation.is_empty()
                && batch.operations.iter().any(|operation| {
                    matches!(operation, DfsRepairQueueOperation::RepairCustody { .. })
                })
            {
                return Err(custody_error(
                    "snapshot discarded its versioned original Repair custody",
                ));
            }
            continue;
        }
        if batch.sequence != expected_sequence {
            return Err(DfsError::Metadata {
                message: format!(
                    "DFS repair queue log {} sequence gap: expected {}, found {}",
                    path.display(),
                    expected_sequence,
                    batch.sequence
                ),
            });
        }
        custody.validate_scheduling_changes(&batch.operations)?;
        custody.validate_enrollment_records(records, &batch.operations)?;
        apply_repair_queue_operations(records, &batch.operations)?;
        for operation in &batch.operations {
            if let DfsRepairQueueOperation::RepairCustody { change } = operation {
                custody.apply_change(change, batch.sequence)?;
            }
        }
        head_sequence = batch.sequence;
        expected_sequence = batch
            .sequence
            .checked_add(1)
            .ok_or_else(|| DfsError::Metadata {
                message: "DFS repair queue log sequence overflow".to_string(),
            })?;
    };
    Ok(DfsRepairQueueReplayReport {
        head_sequence,
        file_bytes,
        valid_bytes,
        fingerprint,
    })
}

fn apply_repair_queue_operations(
    records: &mut BTreeMap<DfsRepairQueueKey, DfsRepairAttemptRecord>,
    operations: &[DfsRepairQueueOperation],
) -> Result<(), DfsError> {
    for operation in operations {
        match operation {
            DfsRepairQueueOperation::EnqueueIfMissing { record } => {
                let key = DfsRepairQueueKey::from_record(record);
                if let Some(existing) = records.get(&key) {
                    validate_stable_repair_identity(existing, record)?;
                } else {
                    records.insert(key, record.clone());
                }
            }
            DfsRepairQueueOperation::Set { record } => {
                let key = DfsRepairQueueKey::from_record(record);
                if let Some(existing) = records.get(&key) {
                    validate_stable_repair_identity(existing, record)?;
                }
                records.insert(key, record.clone());
            }
            DfsRepairQueueOperation::Remove { key } => {
                records.remove(key);
            }
            DfsRepairQueueOperation::RepairCustody { .. } => {}
        }
    }
    Ok(())
}

/// Validate a batch without cloning the entire accumulated queue. Foreground
/// writes normally touch only one or two repair identities, so this keeps
/// append cost proportional to the delta instead of the queue size.
fn validate_repair_queue_operations(
    records: &BTreeMap<DfsRepairQueueKey, DfsRepairAttemptRecord>,
    operations: &[DfsRepairQueueOperation],
) -> Result<(), DfsError> {
    let mut touched: BTreeMap<DfsRepairQueueKey, Option<DfsRepairAttemptRecord>> = BTreeMap::new();
    for operation in operations {
        if matches!(operation, DfsRepairQueueOperation::RepairCustody { .. }) {
            continue;
        }
        let (key, candidate, removes) = match operation {
            DfsRepairQueueOperation::EnqueueIfMissing { record } => {
                (DfsRepairQueueKey::from_record(record), Some(record), false)
            }
            DfsRepairQueueOperation::Set { record } => {
                (DfsRepairQueueKey::from_record(record), Some(record), false)
            }
            DfsRepairQueueOperation::Remove { key } => (key.clone(), None, true),
            DfsRepairQueueOperation::RepairCustody { .. } => {
                unreachable!("custody transition handled separately")
            }
        };
        let current = touched
            .entry(key.clone())
            .or_insert_with(|| records.get(&key).cloned());
        if removes {
            *current = None;
            continue;
        }
        let candidate = candidate.expect("non-remove repair queue operation has a record");
        if let Some(existing) = current.as_ref() {
            validate_stable_repair_identity(existing, candidate)?;
        }
        match operation {
            DfsRepairQueueOperation::EnqueueIfMissing { .. } if current.is_some() => {}
            _ => *current = Some(candidate.clone()),
        }
    }
    Ok(())
}

fn validate_stable_repair_intent_identity(
    expected: &DfsRepairAttemptRecord,
    actual: &DfsRepairAttemptRecord,
) -> Result<(), DfsError> {
    if expected.intent_id != actual.intent_id
        || expected.object_id != actual.object_id
        || expected.version != actual.version
        || expected.content_hash != actual.content_hash
    {
        return Err(DfsError::Metadata {
            message: "repair retirement identity changed".into(),
        });
    }
    Ok(())
}

fn validate_stable_repair_identity(
    existing: &DfsRepairAttemptRecord,
    candidate: &DfsRepairAttemptRecord,
) -> Result<(), DfsError> {
    if existing.object_id == candidate.object_id
        && existing.version == candidate.version
        && existing.content_hash == candidate.content_hash
    {
        return Ok(());
    }
    Err(DfsError::Metadata {
        message: format!(
            "DFS repair queue identity {} for target {} changed object, version, or content hash",
            existing.intent_id, existing.missing_replica_node_id
        ),
    })
}

fn reconcile_repair_queue_operations(
    current: &BTreeMap<DfsRepairQueueKey, DfsRepairAttemptRecord>,
    desired_records: &[DfsRepairAttemptRecord],
) -> Result<Vec<DfsRepairQueueOperation>, DfsError> {
    let desired = records_by_key(desired_records.to_vec())?;
    let mut operations = Vec::new();
    for (key, record) in &desired {
        match current.get(key) {
            Some(existing) if existing == record => {}
            Some(existing) => {
                validate_stable_repair_identity(existing, record)?;
                operations.push(DfsRepairQueueOperation::Set {
                    record: record.clone(),
                });
            }
            None => operations.push(DfsRepairQueueOperation::Set {
                record: record.clone(),
            }),
        }
    }
    for key in current.keys() {
        if !desired.contains_key(key) {
            operations.push(DfsRepairQueueOperation::Remove { key: key.clone() });
        }
    }
    Ok(operations)
}

fn records_by_key(
    records: Vec<DfsRepairAttemptRecord>,
) -> Result<BTreeMap<DfsRepairQueueKey, DfsRepairAttemptRecord>, DfsError> {
    let mut indexed = BTreeMap::new();
    for record in records {
        let key = DfsRepairQueueKey::from_record(&record);
        if let Some(existing) = indexed.get(&key) {
            validate_stable_repair_identity(existing, &record)?;
        }
        indexed.insert(key, record);
    }
    Ok(indexed)
}

#[cfg(test)]
fn read_repair_queue_snapshot(path: &Path) -> Result<DfsRepairQueueSnapshotFile, DfsError> {
    read_repair_queue_snapshot_with_fingerprint(path).map(|(snapshot, _)| snapshot)
}

fn read_repair_queue_snapshot_with_fingerprint(
    path: &Path,
) -> Result<(DfsRepairQueueSnapshotFile, RepairQueueFileFingerprint), DfsError> {
    match fs::read(path) {
        Ok(bytes) => {
            let snapshot = sinomo_object_serde::from_bytes(&bytes).map_err(|source| {
                DfsError::Serialization {
                    message: format!(
                        "DFS repair queue snapshot object error at {}: {source}",
                        path.display()
                    ),
                }
            })?;
            Ok((snapshot, RepairQueueFileFingerprint::from_bytes(&bytes)))
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok((
            DfsRepairQueueSnapshotFile::default(),
            RepairQueueFileFingerprint::absent(),
        )),
        Err(source) => Err(DfsError::Metadata {
            message: format!(
                "could not read DFS repair queue snapshot {}: {source}",
                path.display()
            ),
        }),
    }
}

fn repair_queue_active_log_path(snapshot_path: &Path) -> PathBuf {
    let mut file_name = snapshot_path
        .file_stem()
        .unwrap_or_else(|| std::ffi::OsStr::new("repair-attempts"))
        .to_os_string();
    file_name.push(".active.slog");
    snapshot_path.with_file_name(file_name)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{DfsObjectId, DfsVersion};
    use std::fs::OpenOptions;
    use std::io::Write as _;

    mod imported_executor_association_tests {
        use super::*;

        struct Fixture {
            source_queue: LocalDfsRepairQueueStore,
            source_content: crate::LocalContentStore,
            queue: LocalDfsRepairQueueStore,
            identity: SelectedOriginalFixtureIdentity,
            content: crate::LocalContentStore,
            original: crate::DfsRoutedMessage,
            imported: ImportedRepairOriginalData,
            assignment: crate::DfsMetadataShardConfig,
        }

        // Real queue/content writers, export, stage and install. The supplied
        // handoff/configuration VALUES test local DATA persistence only; this
        // helper does not impersonate native cluster authority or execution.
        fn fixture(root: &Path) -> Fixture {
            let (source_queue, source_content, original, attachment, retained, source_identity) =
                selected_original_fixture(&root.join("source"));
            source_queue
                .enroll_original_repair(&original, &attachment, &retained)
                .unwrap();
            source_queue
                .record_original_repair_attempt(&original)
                .unwrap();
            source_queue
                .record_original_repair_delivery(
                    &original,
                    Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied),
                )
                .unwrap();
            let crate::DfsNetworkMessage::RepairReplica(command) = &original.message else {
                panic!("Repair fixture");
            };
            let retiring = command.authority_shard.clone().unwrap();
            let assignment = crate::DfsMetadataShardConfig {
                primary_node_id: NodeId::from_legacy_named("disabled-import-executor").unwrap(),
                replica_node_ids: vec![retiring.primary_node_id.clone()],
                epoch: retiring.epoch + 1,
                ..retiring.clone()
            };
            let handoff = crate::DfsMetadataShardMembershipHandoff {
                cause: crate::DfsMetadataShardHandoffCause::Membership,
                handoff_id: "disabled-import-association".into(),
                desired_shard: assignment.clone(),
                required_prepared_sequence: 17,
                required_head_watermark: None,
                retiring_primary_fence: Some(crate::DfsMetadataShardMembershipHandoffFence {
                    handoff_id: "disabled-import-association".into(),
                    primary_node_id: retiring.primary_node_id.clone(),
                    journal_sequence: 17,
                    state_fingerprint: "local-association-persistence-only".into(),
                }),
                retiring_shard: retiring,
            };
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            let export = crate::repair_handoff::RepairHandoffExport::capture_selected_original(
                &source_identity,
                &handoff,
                &source_queue,
                &command.request_id,
                |_| Ok(true),
                deadline,
            )
            .unwrap();
            let (queue, identity) =
                LocalDfsRepairQueueStore::new_selected_original_fixture(root.join("destination"))
                    .unwrap();
            for ordinal in 0..export.cut.page_count {
                crate::repair_handoff::receive_selected_original_data(
                    &identity,
                    &export.page(ordinal).unwrap(),
                    deadline,
                )
                .unwrap();
            }
            assert!(
                crate::repair_handoff::install_selected_original_data(
                    &identity,
                    &export.cut,
                    &queue,
                    deadline
                )
                .unwrap()
            );
            let imported = queue
                .installed_original_handoff_data(&original.source_node_id, &command.request_id)
                .unwrap()
                .unwrap();
            let content = crate::LocalContentStore::new(identity.root().join("content")).unwrap();
            let bytes = source_content
                .read_object_version(command.object_id.as_str(), command.version.as_str())
                .unwrap()
                .1;
            content
                .put_object_version(command.object_id.as_str(), command.version.as_str(), &bytes)
                .unwrap();
            Fixture {
                source_queue,
                source_content,
                queue,
                identity,
                content,
                original,
                imported,
                assignment,
            }
        }

        fn associate(f: &Fixture) -> ImportedRepairExecutorAssociation {
            f.queue
                .associate_imported_executor_disabled(
                    &f.imported,
                    &f.assignment.primary_node_id,
                    &f.assignment,
                    &f.content,
                )
                .unwrap()
        }

        #[test]
        fn imported_executor_retains_actual_b_content_without_enabling_native_execution() {
            let root = sinomo_testdir::TestDir::new("imported-executor-disabled");
            let f = fixture(root.path());
            let before = f.queue.view().unwrap().records;
            let association = associate(&f);
            let birth = &f.imported.material.birth;
            assert_eq!(association.local_root().subject, birth.root.subject);
            assert_eq!(association.local_root().source, birth.root.source);
            assert_ne!(
                association.local_root().store_incarnation,
                birth.root.store_incarnation
            );
            f.content
                .inspect_repair_payload_root(association.local_root())
                .unwrap();
            let mut reader = f
                .content
                .open_retained_repair_payload(association.local_root())
                .unwrap();
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut reader, &mut bytes).unwrap();
            let crate::DfsNetworkMessage::RepairReplica(command) = &f.original.message else {
                panic!("Repair fixture");
            };
            assert_eq!(
                bytes,
                f.source_content
                    .read_object_version(command.object_id.as_str(), command.version.as_str())
                    .unwrap()
                    .1
            );
            assert_eq!(
                f.source_queue
                    .original_repair(&f.original)
                    .unwrap()
                    .unwrap(),
                *birth
            );
            assert_eq!(
                f.queue
                    .installed_original_handoff_data(
                        &f.original.source_node_id,
                        &command.request_id
                    )
                    .unwrap()
                    .unwrap(),
                f.imported
            );
            assert_eq!(f.queue.view().unwrap().records, before);
            assert!(
                f.queue
                    .original_repair_for_slot(&command.intent_id, &f.original.target_node_id)
                    .unwrap()
                    .is_none()
            );
            let mut carrier = f.original.clone();
            carrier.source_node_id = f.assignment.primary_node_id.clone();
            carrier.frame_id += 1;
            carrier.message_id += 1;
            assert!(f.queue.original_repair(&carrier).unwrap().is_none());
            assert!(f.queue.record_original_repair_attempt(&carrier).is_err());
            assert!(f.queue.validate_original_repair_dispatch(&carrier).is_err());
            let response = original_response(&f.original, true);
            assert!(
                f.queue
                    .observe_original_repair_result(&carrier.source_node_id, &response)
                    .unwrap()
                    .is_none()
            );
            assert!(
                f.queue
                    .closed_repair_birth(&birth.root.subject)
                    .unwrap()
                    .is_none()
            );

            // Even an ordinary fresh native birth cannot turn this slot into
            // a new semantic request. This retained root remains a safe orphan.
            let mut native = carrier;
            if let crate::DfsNetworkMessage::RepairReplica(command) = &mut native.message {
                command.authority_shard = Some(f.assignment.clone());
                command.request_id.push_str("-must-not-replace");
            }
            let payload = f
                .content
                .capture_repair_payload(
                    repair_subject(&f.queue.custody_incarnation().unwrap(), &native).unwrap(),
                    original_repair_digest(&native, &birth.attachment).unwrap(),
                    command.object_id.as_str(),
                    command.version.as_str(),
                    &command.content_hash,
                    birth.attachment.total_size_bytes,
                )
                .unwrap();
            assert!(
                f.queue
                    .enroll_original_repair(&native, &birth.attachment, &payload)
                    .is_err()
            );
            assert_eq!(
                f.queue
                    .imported_executor_association(&f.original.source_node_id, &command.request_id)
                    .unwrap(),
                Some(association)
            );
        }

        #[test]
        fn imported_executor_duplicate_lost_reply_reopen_and_compaction_keep_one_association() {
            let root = sinomo_testdir::TestDir::new("imported-executor-reopen");
            let mut f = fixture(root.path());
            let first = associate(&f); // Drop the caller reply; the queue is already synced.
            let sequence = f.queue.view().unwrap().head_sequence;
            assert_eq!(associate(&f), first);
            assert_eq!(f.queue.view().unwrap().head_sequence, sequence);
            LocalDfsRepairQueueStore::evict_for_test(f.identity.snapshot_path());
            f.queue =
                LocalDfsRepairQueueStore::reopen_selected_original_fixture(&f.identity).unwrap();
            assert_eq!(associate(&f), first);
            f.queue
                .compact_if_needed(DfsRepairQueueCompactionOptions::force())
                .unwrap();
            LocalDfsRepairQueueStore::evict_for_test(f.identity.snapshot_path());
            f.queue =
                LocalDfsRepairQueueStore::reopen_selected_original_fixture(&f.identity).unwrap();
            assert_eq!(associate(&f), first);
            assert_eq!(f.queue.view().unwrap().head_sequence, sequence);
            assert_eq!(
                first.binding.origin.request_id,
                f.imported.material.birth.root.subject.request_id
            );
        }

        #[test]
        fn imported_executor_conflicting_origin_assignment_and_physical_root_do_not_append() {
            let root = sinomo_testdir::TestDir::new("imported-executor-conflicts");
            let f = fixture(root.path());
            let sequence = f.queue.view().unwrap().head_sequence;
            let mut changed = f.imported.clone();
            changed.installed_at_sequence += 1;
            assert!(
                f.queue
                    .associate_imported_executor_disabled(
                        &changed,
                        &f.assignment.primary_node_id,
                        &f.assignment,
                        &f.content
                    )
                    .is_err()
            );
            let mut changed = f.imported.clone();
            if let crate::DfsNetworkMessage::RepairReplica(command) =
                &mut changed.material.birth.original.message
            {
                command.expected_base_version =
                    Some(DfsVersion::from_legacy_named("invented-base").unwrap());
            }
            assert!(
                f.queue
                    .associate_imported_executor_disabled(
                        &changed,
                        &f.assignment.primary_node_id,
                        &f.assignment,
                        &f.content
                    )
                    .is_err()
            );
            let mut changed_assignment = f.assignment.clone();
            changed_assignment.epoch += 1;
            assert!(
                f.queue
                    .associate_imported_executor_disabled(
                        &f.imported,
                        &changed_assignment.primary_node_id,
                        &changed_assignment,
                        &f.content
                    )
                    .is_err()
            );
            assert!(
                f.queue
                    .associate_imported_executor_disabled(
                        &f.imported,
                        &f.assignment.primary_node_id,
                        &f.assignment,
                        &f.source_content
                    )
                    .is_err(),
                "A physical store cannot become B retention"
            );
            assert_eq!(f.queue.view().unwrap().head_sequence, sequence);
            assert!(
                f.queue
                    .imported_executor_association(
                        &f.original.source_node_id,
                        &f.imported.material.birth.root.subject.request_id
                    )
                    .unwrap()
                    .is_none()
            );
        }

        #[test]
        fn imported_executor_requires_exact_local_version_not_blob_fallback() {
            let root = sinomo_testdir::TestDir::new("imported-executor-missing-version");
            let f = fixture(root.path());
            let missing = crate::LocalContentStore::new(root.join("missing-content")).unwrap();
            let sequence = f.queue.view().unwrap().head_sequence;
            assert!(
                f.queue
                    .associate_imported_executor_disabled(
                        &f.imported,
                        &f.assignment.primary_node_id,
                        &f.assignment,
                        &missing
                    )
                    .is_err()
            );
            let crate::DfsNetworkMessage::RepairReplica(command) = &f.original.message else {
                panic!("Repair fixture");
            };
            let bytes = f
                .source_content
                .read_object_version(command.object_id.as_str(), command.version.as_str())
                .unwrap()
                .1;
            missing
                .put_object_version_blob_only(
                    command.object_id.as_str(),
                    command.version.as_str(),
                    &bytes,
                )
                .unwrap();
            assert!(
                f.queue
                    .associate_imported_executor_disabled(
                        &f.imported,
                        &f.assignment.primary_node_id,
                        &f.assignment,
                        &missing
                    )
                    .is_err()
            );
            assert!(
                missing
                    .retained_repair_for_slot(
                        &f.original.source_node_id.to_string(),
                        &f.original.target_node_id.to_string(),
                        &command.intent_id
                    )
                    .unwrap()
                    .is_none()
            );
            assert_eq!(f.queue.view().unwrap().head_sequence, sequence);
            assert!(
                f.queue
                    .imported_executor_association(&f.original.source_node_id, &command.request_id)
                    .unwrap()
                    .is_none()
            );
        }

        #[test]
        fn imported_executor_interruption_after_content_sync_retains_orphan_then_reuses_it() {
            let root = sinomo_testdir::TestDir::new("imported-executor-content-crash-cut");
            let mut f = fixture(root.path());
            let sequence = f.queue.view().unwrap().head_sequence;
            BEFORE_IMPORTED_EXECUTOR_APPEND.with(|slot| {
                *slot.borrow_mut() =
                    Some(Box::new(|| panic!("stop after actual content root sync")))
            });
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| associate(&f))).is_err()
            );
            let crate::DfsNetworkMessage::RepairReplica(command) = &f.original.message else {
                panic!("Repair fixture");
            };
            let retained = f
                .content
                .retained_repair_for_slot(
                    &f.original.source_node_id.to_string(),
                    &f.original.target_node_id.to_string(),
                    &command.intent_id,
                )
                .unwrap()
                .unwrap();
            f.content.inspect_repair_payload_root(&retained).unwrap();
            assert_eq!(f.queue.view().unwrap().head_sequence, sequence);
            assert!(
                f.queue
                    .imported_executor_association(&f.original.source_node_id, &command.request_id)
                    .unwrap()
                    .is_none()
            );
            LocalDfsRepairQueueStore::evict_for_test(f.identity.snapshot_path());
            f.queue =
                LocalDfsRepairQueueStore::reopen_selected_original_fixture(&f.identity).unwrap();
            assert_eq!(*associate(&f).local_root(), retained);
        }

        #[test]
        fn imported_executor_physical_replacement_cannot_renew_stored_association() {
            let root = sinomo_testdir::TestDir::new("imported-executor-store-replaced");
            let f = fixture(root.path());
            let association = associate(&f);
            let sequence = f.queue.view().unwrap().head_sequence;
            let content_root = f.identity.root().join("content");
            fs::rename(&content_root, root.join("retained-old-content")).unwrap();
            fs::create_dir(&content_root).unwrap();
            assert!(
                f.queue
                    .associate_imported_executor_disabled(
                        &f.imported,
                        &f.assignment.primary_node_id,
                        &f.assignment,
                        &f.content,
                    )
                    .is_err()
            );
            assert_eq!(f.queue.view().unwrap().head_sequence, sequence);
            assert_eq!(
                f.queue
                    .imported_executor_association(
                        &f.original.source_node_id,
                        &f.imported.material.birth.root.subject.request_id,
                    )
                    .unwrap(),
                Some(association),
                "historical DATA remains, without renewed physical authority"
            );
        }

        #[test]
        fn imported_executor_snapshot_validates_origin_and_default_reader_rejects_without_mutation()
        {
            let root = sinomo_testdir::TestDir::new("imported-executor-snapshot");
            let f = fixture(root.path());
            associate(&f);
            f.queue
                .compact_if_needed(DfsRepairQueueCompactionOptions::force())
                .unwrap();
            let snapshot = read_repair_queue_snapshot(f.identity.snapshot_path()).unwrap();
            let floor = snapshot.applied_through_sequence;
            let original = snapshot.custody.clone().unwrap();
            let mut broken = original.clone();
            broken.imported_executor_associations[0]
                .binding
                .local_root
                .store_incarnation = f.imported.material.birth.root.store_incarnation.clone();
            assert!(RepairCustodyIndex::from_snapshot(Some(broken), floor, true).is_err());
            let mut broken = original.clone();
            broken.imported_executor_associations[0].associated_at_sequence = broken
                .imported_executor_associations[0]
                .binding
                .imported_at_sequence;
            assert!(RepairCustodyIndex::from_snapshot(Some(broken), floor, true).is_err());
            assert!(RepairCustodyIndex::from_snapshot(Some(original), floor, false).is_err());

            // Actual current codec bytes under an ordinary path, with no
            // selected marker: fail before repair/append, never silently omit.
            let ordinary_path = root.join("ordinary.sobj");
            let bytes = encode_sobj(&snapshot).unwrap();
            write_file_atomic(&ordinary_path, &bytes).unwrap();
            let ordinary = LocalDfsRepairQueueStore::new(&ordinary_path);
            assert!(ordinary.view().is_err());
            assert!(ordinary.view().is_err());
            assert_eq!(fs::read(&ordinary_path).unwrap(), bytes);
            assert!(!repair_queue_active_log_path(&ordinary_path).exists());
        }
    }

    // These use the actual file writers/replay. They test local persistence,
    // not receiver idempotency, process/partition recovery or survivor custody.
    fn original_fixture(
        root: &Path,
    ) -> (
        LocalDfsRepairQueueStore,
        crate::LocalContentStore,
        crate::DfsRoutedMessage,
        crate::DfsRpcAttachmentDescriptor,
        crate::content_store::RetainedRepairPayload,
    ) {
        let store = LocalDfsRepairQueueStore::new(root.join("repair.sobj"));
        original_fixture_from_store(root, store)
    }

    pub(crate) fn selected_original_fixture(root: &Path) -> (LocalDfsRepairQueueStore, crate::LocalContentStore, crate::DfsRoutedMessage, crate::DfsRpcAttachmentDescriptor, crate::content_store::RetainedRepairPayload, SelectedOriginalFixtureIdentity) {
        let (store, identity) = LocalDfsRepairQueueStore::new_selected_original_fixture(root).unwrap();
        let (store, content, message, descriptor, payload) = original_fixture_from_store(root, store);
        (store, content, message, descriptor, payload, identity)
    }

    fn original_fixture_from_store(root: &Path, store: LocalDfsRepairQueueStore) -> (LocalDfsRepairQueueStore, crate::LocalContentStore, crate::DfsRoutedMessage, crate::DfsRpcAttachmentDescriptor, crate::content_store::RetainedRepairPayload) {
        let incarnation = store.custody_incarnation().unwrap();
        let content = crate::LocalContentStore::new(root.join("content")).unwrap();
        let object = DfsObjectId::from_legacy_named("custody-object").unwrap();
        let version = DfsVersion::from_legacy_named("custody-version").unwrap();
        let bytes = b"complete original attachment";
        let manifest = content
            .put_object_version(object.as_str(), version.as_str(), bytes)
            .unwrap();
        let producer = NodeId::from_legacy_named("custody-producer").unwrap();
        let target = NodeId::from_legacy_named("custody-receiver").unwrap();
        let command = crate::DfsRepairReplicaCommand {
            request_id: "original-request".into(),
            background_repair: true,
            intent_id: "original-intent".into(),
            file_system_id: crate::sifs_file_system_id(),
            object_id: object,
            expected_base_version: None,
            version,
            content_hash: manifest.content_hash.clone(),
            bytes: Vec::new(),
            object_metadata: None,
            authority_shard: Some(crate::DfsMetadataShardConfig {
                file_system_id: crate::sifs_file_system_id(),
                shard_id: "root".into(),
                primary_node_id: producer.clone(),
                replica_node_ids: vec![target.clone()],
                epoch: 7,
                assignment: crate::DfsMetadataShardAssignment::Root,
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            }),
        };
        let message = crate::DfsRoutedMessage {
            source_node_id: producer,
            target_node_id: target,
            frame_id: 11,
            message_id: 12,
            message: crate::DfsNetworkMessage::RepairReplica(command),
        };
        let descriptor = crate::DfsRpcAttachmentDescriptor {
            total_size_bytes: bytes.len() as u64,
            content_hash: Some(manifest.content_hash),
        };
        let payload = content
            .capture_repair_payload(
                repair_subject(&incarnation, &message).unwrap(),
                original_repair_digest(&message, &descriptor).unwrap(),
                manifest.object_id.as_str(),
                manifest.version.as_str(),
                descriptor.content_hash.as_deref().unwrap(),
                descriptor.total_size_bytes,
            )
            .unwrap();
        (store, content, message, descriptor, payload)
    }

    fn original_response(
        message: &crate::DfsRoutedMessage,
        committed: bool,
    ) -> crate::DfsRepairReplicaResponse {
        let crate::DfsNetworkMessage::RepairReplica(command) = &message.message else {
            panic!("Repair fixture");
        };
        crate::DfsRepairReplicaResponse {
            request_id: command.request_id.clone(),
            intent_id: command.intent_id.clone(),
            file_system_id: command.file_system_id.clone(),
            object_id: command.object_id.clone(),
            version: command.version.clone(),
            replica_node_id: message.target_node_id.clone(),
            result: if committed {
                crate::DfsBlobTransferResult::Committed {
                    content_hash: command.content_hash.clone(),
                }
            } else {
                crate::DfsBlobTransferResult::Durable {
                    content_hash: command.content_hash.clone(),
                }
            },
        }
    }

    // Include all resident fields and queue-side file identities/content. Reads
    // may change atime, so it is deliberately not an unchanged-file assertion.
    fn diagnostic_fixture_state(store: &LocalDfsRepairQueueStore) -> (String, serde_json::Value) {
        let state = store.inner.lock().unwrap();
        let resident = format!("{state:?}");
        let snapshot = state.snapshot_path.clone();
        let log = state.active_log_path.clone();
        drop(state);
        let files: Vec<_> = [&snapshot, &log, &snapshot.with_extension("writer-lock")].into_iter()
            .map(|path| {
                let metadata = fs::symlink_metadata(path).ok();
                let content = match metadata.as_ref() {
                    Some(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                        serde_json::json!(fs::read(path).map(|bytes| blake3::hash(&bytes).to_hex().to_string()).map_err(|error| error.to_string()))
                    }
                    _ => serde_json::json!({"unavailable": "not a regular existing file"}),
                };
                serde_json::json!({"path": path, "identity": diagnostic_queue_identity(&metadata), "content": content})
            }).collect();
        let mut names: Vec<_> = fs::read_dir(snapshot.parent().unwrap())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        names.sort();
        (
            resident,
            serde_json::json!({"files": files, "directory_entries": names}),
        )
    }

    fn diagnostic_key(message: &crate::DfsRoutedMessage) -> DfsRepairQueueKey {
        let crate::DfsNetworkMessage::RepairReplica(command) = &message.message else {
            panic!("Repair fixture");
        };
        DfsRepairQueueKey {
            intent_id: command.intent_id.clone(),
            missing_replica_node_id: message.target_node_id.clone(),
        }
    }

    #[test]
    fn nonrecovering_original_observation_preserves_exact_birth_and_files() {
        let root = sinomo_testdir::TestDir::new("dfs-observe-original-birth");
        let (store, _, message, descriptor, payload) = original_fixture(&root);
        let birth = store
            .enroll_original_repair(&message, &descriptor, &payload)
            .unwrap();
        let before = diagnostic_fixture_state(&store);
        let replay_count = REPAIR_QUEUE_FULL_REPLAYS.get();
        let observation = LocalDfsRepairQueueStore::observe_registered_original_for_slot(
            &root.join("repair.sobj"),
            &diagnostic_key(&message),
            4 * 1024 * 1024,
        );
        assert_eq!(observation.facts["status"], "ObservedResident");
        assert_eq!(observation.birth, Some(birth));
        assert_eq!(
            observation.facts["before"]["snapshot"]["present"], false,
            "an expected absent snapshot is valid before compaction"
        );
        assert_eq!(REPAIR_QUEUE_FULL_REPLAYS.get(), replay_count);
        assert_eq!(diagnostic_fixture_state(&store), before);
    }

    #[test]
    fn nonrecovering_original_observation_leaves_legacy_rows_uncovered() {
        let root = sinomo_testdir::TestDir::new("dfs-observe-legacy-row");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let row = record("legacy", "legacy-target", 2);
        store.append_enqueues([row.clone()]).unwrap();
        let before = diagnostic_fixture_state(&store);
        let observation = LocalDfsRepairQueueStore::observe_registered_original_for_slot(
            &path,
            &DfsRepairQueueKey::from_record(&row),
            4 * 1024 * 1024,
        );
        assert_eq!(observation.facts["status"], "ObservedResident");
        assert!(observation.birth.is_none());
        assert_eq!(observation.facts["record"], serde_json::json!(row));
        assert!(observation.facts["resident"]["existing_incarnation"].is_null());
        assert_eq!(diagnostic_fixture_state(&store), before);
    }

    #[test]
    fn nonrecovering_original_observation_matches_snapshot_and_log_with_combined_bound() {
        let root = sinomo_testdir::TestDir::new("dfs-observe-snapshot-log");
        let path = root.join("repair.sobj");
        let log = repair_queue_active_log_path(&path);
        let (store, _, message, descriptor, payload) = original_fixture(&root);
        let birth = store
            .enroll_original_repair(&message, &descriptor, &payload)
            .unwrap();
        store
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        store.record_original_repair_attempt(&message).unwrap();
        let before = diagnostic_fixture_state(&store);
        let observation = LocalDfsRepairQueueStore::observe_registered_original_for_slot(
            &path,
            &diagnostic_key(&message),
            4 * 1024 * 1024,
        );
        assert_eq!(observation.facts["status"], "ObservedResident");
        assert_eq!(observation.facts["before"]["snapshot"]["present"], true);
        assert_eq!(observation.facts["before"]["log"]["present"], true);
        let captured = observation.birth.unwrap();
        assert_eq!(captured.original, message);
        assert_eq!(captured.attachment, descriptor);
        assert_eq!(captured.root, birth.root);
        assert_eq!(captured.attempts.len(), 1);
        let snapshot_cost = fs::metadata(&path).unwrap().len() + 1;
        let log_cost = fs::metadata(&log).unwrap().len() + 1;
        let budget = snapshot_cost.max(log_cost);
        assert!(budget < snapshot_cost + log_cost);
        let bounded = LocalDfsRepairQueueStore::observe_registered_original_for_slot(
            &path,
            &diagnostic_key(&captured.original),
            budget,
        );
        assert_eq!(
            bounded.facts["status"], "TooLarge",
            "both files share one pass budget"
        );
        assert!(bounded.birth.is_none());
        assert_eq!(diagnostic_fixture_state(&store), before);
    }

    #[test]
    fn nonrecovering_original_observation_preserves_persisted_resync_uncertainty() {
        let root = sinomo_testdir::TestDir::new("dfs-observe-resync-payload");
        let path = root.join("repair.sobj");
        let log = repair_queue_active_log_path(&path);
        let (store, _, message, descriptor, payload) = original_fixture(&root);
        store
            .enroll_original_repair(&message, &descriptor, &payload)
            .unwrap();
        store.record_original_repair_attempt(&message).unwrap();
        let complete = fs::read(&log).unwrap();
        for torn in [false, true] {
            let mut bytes = complete.clone();
            if torn {
                bytes.extend_from_slice(&42_u32.to_le_bytes());
                bytes.push(1);
            }
            fs::write(&log, &bytes).unwrap();
            {
                let mut state = store.inner.lock().unwrap();
                state.resync_on_load = true;
            }
            let before = diagnostic_fixture_state(&store);
            REPAIR_QUEUE_FAIL_RESYNC.set(true);
            let observation = LocalDfsRepairQueueStore::observe_registered_original_for_slot(
                &path,
                &diagnostic_key(&message),
                4 * 1024 * 1024,
            );
            let resync_hook_unconsumed = REPAIR_QUEUE_FAIL_RESYNC.replace(false);
            assert_eq!(observation.facts["status"], "Uncertain");
            assert!(observation.birth.is_none());
            assert!(resync_hook_unconsumed);
            assert_eq!(diagnostic_fixture_state(&store), before);
            assert_eq!(fs::read(&log).unwrap(), bytes);
        }
    }

    #[test]
    fn nonrecovering_original_observation_defers_cold_busy_and_uncertain() {
        let root = sinomo_testdir::TestDir::new("dfs-observe-unavailable");
        let path = root.join("repair.sobj");
        let key = DfsRepairQueueKey::from_record(&record("cold", "target", 0));
        let inspect =
            || LocalDfsRepairQueueStore::observe_registered_original_for_slot(&path, &key, 4096);
        assert_eq!(inspect().facts["status"], "Unregistered");
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
        let store = LocalDfsRepairQueueStore::new(&path);
        let before = diagnostic_fixture_state(&store);
        assert_eq!(inspect().facts["status"], "Cold");
        {
            let _held = store.inner.lock().unwrap();
            assert_eq!(inspect().facts["status"], "Busy");
        }
        assert_eq!(diagnostic_fixture_state(&store), before);
        {
            let mut state = store.inner.lock().unwrap();
            state.resync_on_load = true;
        }
        let before = diagnostic_fixture_state(&store);
        REPAIR_QUEUE_FAIL_RESYNC.set(true);
        let observation = inspect();
        let resync_hook_unconsumed = REPAIR_QUEUE_FAIL_RESYNC.replace(false);
        assert_eq!(observation.facts["status"], "Uncertain");
        assert!(resync_hook_unconsumed);
        assert_eq!(diagnostic_fixture_state(&store), before);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    }

    #[test]
    fn nonrecovering_original_observation_does_not_repair_torn_or_dirty_bytes() {
        let root = sinomo_testdir::TestDir::new("dfs-observe-torn-dirty");
        let path = root.join("repair.sobj");
        let log = repair_queue_active_log_path(&path);
        let store = LocalDfsRepairQueueStore::new(&path);
        let row = record("torn", "target", 0);
        store.append_enqueues([row.clone()]).unwrap();
        let key = DfsRepairQueueKey::from_record(&row);
        let original = fs::read(&log).unwrap();
        let mut dirty = original.clone();
        let last = dirty.len() - 1;
        dirty[last] ^= 1;
        fs::write(&log, &dirty).unwrap();
        let before = diagnostic_fixture_state(&store);
        let observation =
            LocalDfsRepairQueueStore::observe_registered_original_for_slot(&path, &key, 4096);
        assert_eq!(observation.facts["status"], "Dirty");
        assert_eq!(diagnostic_fixture_state(&store), before);
        let mut torn = original;
        torn.extend_from_slice(&42_u32.to_le_bytes());
        torn.push(1);
        fs::write(&log, &torn).unwrap();
        assert_eq!(
            LocalDfsRepairQueueStore::observe_registered_original_for_slot(&path, &key, 4096).facts
                ["status"],
            "Dirty"
        );
        // Ordinary setup replay observes the torn prefix; the diagnostic must
        // subsequently preserve that resident state and every suffix byte.
        store.view_for_maintenance().unwrap();
        let before = diagnostic_fixture_state(&store);
        for _ in 0..2 {
            let observation =
                LocalDfsRepairQueueStore::observe_registered_original_for_slot(&path, &key, 4096);
            assert_eq!(observation.facts["status"], "Torn");
            assert!(observation.birth.is_none());
            assert_eq!(diagnostic_fixture_state(&store), before);
        }
        assert_eq!(fs::read(&log).unwrap(), torn);
    }

    #[test]
    fn nonrecovering_original_observation_bounds_reads_and_rejects_file_races() {
        let root = sinomo_testdir::TestDir::new("dfs-observe-file-race");
        let path = root.join("repair.sobj");
        let log = repair_queue_active_log_path(&path);
        let store = LocalDfsRepairQueueStore::new(&path);
        let row = record("race", "target", 0);
        store.append_enqueues([row.clone()]).unwrap();
        let key = DfsRepairQueueKey::from_record(&row);
        let before = diagnostic_fixture_state(&store);
        let observation =
            LocalDfsRepairQueueStore::observe_registered_original_for_slot(&path, &key, 1);
        assert_eq!(observation.facts["status"], "TooLarge");
        assert_eq!(observation.facts["bytes_read"], 0);
        assert_eq!(diagnostic_fixture_state(&store), before);
        let original = fs::read(&log).unwrap();
        for appearance in [false, true] {
            let replacement = root.join("replacement");
            fs::write(&replacement, &original).unwrap();
            let observation =
                LocalDfsRepairQueueStore::observe_registered_original_for_slot_with_recheck(
                    &path,
                    &key,
                    4096,
                    || {
                        if appearance {
                            fs::write(&path, []).unwrap();
                        } else {
                            fs::rename(&replacement, &log).unwrap();
                        }
                    },
                );
            assert_eq!(observation.facts["status"], "Changed");
            assert!(observation.birth.is_none());
            assert_eq!(fs::read(&log).unwrap(), original);
            if appearance {
                fs::remove_file(&path).unwrap();
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn nonrecovering_original_observation_rejects_symlink_nonregular_and_io_errors() {
        let root = sinomo_testdir::TestDir::new("dfs-observe-file-errors");
        let directory = root.join("queue");
        fs::create_dir(&directory).unwrap();
        let path = directory.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let row = record("error", "target", 0);
        store.append_enqueues([row.clone()]).unwrap();
        let key = DfsRepairQueueKey::from_record(&row);
        let inspect =
            || LocalDfsRepairQueueStore::observe_registered_original_for_slot(&path, &key, 4096);
        std::os::unix::fs::symlink(repair_queue_active_log_path(&path), &path).unwrap();
        let before = diagnostic_fixture_state(&store);
        assert_eq!(inspect().facts["status"], "NonRegular");
        assert_eq!(diagnostic_fixture_state(&store), before);
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        let before = diagnostic_fixture_state(&store);
        assert_eq!(inspect().facts["status"], "NonRegular");
        assert_eq!(diagnostic_fixture_state(&store), before);
        fs::remove_dir(&path).unwrap();
        let resident = format!("{:?}", store.inner.lock().unwrap());
        let retained = root.join("retained");
        fs::rename(&directory, &retained).unwrap();
        std::os::unix::fs::symlink("queue", &directory).unwrap();
        let observation = inspect();
        fs::remove_file(&directory).unwrap();
        fs::rename(&retained, &directory).unwrap();
        assert_eq!(observation.facts["status"], "IoError");
        assert!(observation.birth.is_none());
        assert_eq!(format!("{:?}", store.inner.lock().unwrap()), resident);
    }

    #[test]
    fn original_repair_preserves_body_through_replace_compact_reopen() {
        let root = sinomo_testdir::TestDir::new("dfs-original-repair-rewrite");
        let (store, content, message, descriptor, payload) = original_fixture(&root);
        let birth = store
            .enroll_original_repair(&message, &descriptor, &payload)
            .unwrap();
        store.record_original_repair_attempt(&message).unwrap();
        assert!(
            store.replace_records(&[]).is_err(),
            "ordinary replacement cannot erase runnable unresolved custody"
        );
        let current = store.view().unwrap().records;
        store.replace_records(&current).unwrap();
        store
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        let reopened = reopen(&root.join("repair.sobj"));
        let retained = reopened
            .validate_original_repair_dispatch(&message)
            .unwrap();
        assert_eq!(retained.original, birth.original);
        assert_eq!(retained.attachment, descriptor);
        assert_eq!(retained.root, birth.root);
        assert_eq!(retained.attempts.len(), 1);
        content.inspect_repair_payload_root(&retained.root).unwrap();
    }

    #[test]
    fn original_repair_rejects_changed_head_and_new_identity_in_open_slot() {
        let root = sinomo_testdir::TestDir::new("dfs-original-repair-alias");
        let (store, _, message, descriptor, payload) = original_fixture(&root);
        store
            .enroll_original_repair(&message, &descriptor, &payload)
            .unwrap();
        let mut changed = message.clone();
        if let crate::DfsNetworkMessage::RepairReplica(command) = &mut changed.message {
            command.expected_base_version =
                Some(DfsVersion::from_legacy_named("changed-base").unwrap());
        }
        assert!(
            store
                .enroll_original_repair(&changed, &descriptor, &payload)
                .is_err()
        );
        assert!(store.record_original_repair_attempt(&changed).is_err());
        if let crate::DfsNetworkMessage::RepairReplica(command) = &mut changed.message {
            command.request_id = "replacement-request".into();
        }
        assert!(
            store
                .enroll_original_repair(&changed, &descriptor, &payload)
                .is_err()
        );
        assert_eq!(
            store.original_repair(&message).unwrap().unwrap().original,
            message
        );
    }

    #[test]
    fn original_repair_later_not_sent_does_not_clear_older_unknown_attempt() {
        use sinomo_core::network::NetworkDeliveryState;
        let root = sinomo_testdir::TestDir::new("dfs-original-repair-attempts");
        let (store, _, message, descriptor, payload) = original_fixture(&root);
        store
            .enroll_original_repair(&message, &descriptor, &payload)
            .unwrap();
        store.record_original_repair_attempt(&message).unwrap();
        store
            .record_original_repair_delivery(&message, Some(NetworkDeliveryState::PossiblyApplied))
            .unwrap();
        let mut retry = message.clone();
        retry.frame_id += 1;
        retry.message_id += 1;
        store.record_original_repair_attempt(&retry).unwrap();
        store
            .record_original_repair_delivery(&retry, Some(NetworkDeliveryState::DefinitelyNotSent))
            .unwrap();
        let reopened = reopen(&root.join("repair.sobj"));
        let birth = reopened.original_repair(&message).unwrap().unwrap();
        assert_eq!(birth.attempts.len(), 2);
        assert_eq!(
            birth.attempts[0].delivery_state,
            Some(NetworkDeliveryState::PossiblyApplied)
        );
        assert_eq!(
            birth.attempts[1].delivery_state,
            Some(NetworkDeliveryState::DefinitelyNotSent)
        );
        assert!(birth.closed_at_sequence.is_none());
    }

    #[test]
    fn original_repair_observed_result_survives_restart_without_application_claim() {
        let root = sinomo_testdir::TestDir::new("dfs-original-repair-observed");
        let (store, content, message, descriptor, payload) = original_fixture(&root);
        let birth = store
            .enroll_original_repair(&message, &descriptor, &payload)
            .unwrap();
        store.record_original_repair_attempt(&message).unwrap();
        let response = original_response(&message, true);
        store
            .observe_original_repair_result(&message.source_node_id, &response)
            .unwrap()
            .unwrap();
        store
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        let reopened = reopen(&root.join("repair.sobj"));
        let retained = reopened.original_repair(&message).unwrap().unwrap();
        assert_eq!(retained.observed, vec![response]);
        assert!(retained.closed_at_sequence.is_none());
        assert!(
            reopened
                .closed_repair_birth(&birth.root.subject)
                .unwrap()
                .is_none()
        );
        assert!(
            reopened
                .validate_original_repair_dispatch(&message)
                .is_err()
        );
        content.inspect_repair_payload_root(&retained.root).unwrap();
    }

    #[test]
    fn original_repair_rejects_uncorrelated_result_and_unowned_closure() {
        let root = sinomo_testdir::TestDir::new("dfs-original-repair-result-correlation");
        let (store, _, message, descriptor, payload) = original_fixture(&root);
        let birth = store
            .enroll_original_repair(&message, &descriptor, &payload)
            .unwrap();
        let mut wrong = original_response(&message, true);
        wrong.version = DfsVersion::from_legacy_named("another-version").unwrap();
        assert!(
            store
                .observe_original_repair_result(&message.source_node_id, &wrong)
                .is_err()
        );
        let progress = original_response(&message, false);
        store
            .observe_original_repair_result(&message.source_node_id, &progress)
            .unwrap();
        let committed = original_response(&message, true);
        store
            .observe_original_repair_result(&message.source_node_id, &committed)
            .unwrap();
        let change = RepairCustodyChange::Close {
            subject: birth.root.subject,
            observed_digest: repair_result_digest(&committed).unwrap(),
        };
        assert!(
            store
                .append_operations(vec![DfsRepairQueueOperation::RepairCustody {
                    change: Box::new(change)
                }])
                .is_err(),
            "generic public append cannot mint application authority"
        );
    }

    #[test]
    fn original_repair_custody_only_appends_preserve_retry_data_observation() {
        let root = sinomo_testdir::TestDir::new("dfs-original-repair-data-token");
        let (store, _, message, descriptor, payload) = original_fixture(&root);
        store
            .enroll_original_repair(&message, &descriptor, &payload)
            .unwrap();
        let (view, observed) = store.view_for_maintenance().unwrap();
        store.record_original_repair_attempt(&message).unwrap();
        assert!(matches!(
            store.reconcile_observed(&observed, &view.records).unwrap(),
            DfsRepairQueueObservedReconcileOutcome::Applied(_)
        ));
        assert!(
            matches!(
                store.reconcile(view.head_sequence, &view.records).unwrap(),
                DfsRepairQueueReconcileOutcome::GenerationChanged { .. }
            ),
            "public exact-head comparison still uses combined history"
        );
    }

    #[test]
    fn original_repair_stale_removal_and_changed_retry_row_are_rejected() {
        let root = sinomo_testdir::TestDir::new("dfs-original-repair-stale-remove");
        let (store, _, message, descriptor, payload) = original_fixture(&root);
        store
            .enroll_original_repair(&message, &descriptor, &payload)
            .unwrap();
        let (view, observed) = store.view_for_maintenance().unwrap();
        store.record_original_repair_attempt(&message).unwrap();
        assert!(store.reconcile_observed(&observed, &[]).is_err());
        let mut changed = view.records[0].clone();
        changed.transfer_request_id = Some("new-id".into());
        assert!(store.replace_records(&[changed]).is_err());
        assert_eq!(store.view().unwrap().records, view.records);
        let mut retry = view.records[0].clone();
        retry.attempts += 1;
        store.replace_records(&[retry]).unwrap();
        assert!(matches!(
            store.reconcile_observed(&observed, &view.records).unwrap(),
            DfsRepairQueueObservedReconcileOutcome::GenerationChanged { .. }
        ));
    }

    #[test]
    fn original_repair_external_reload_fences_old_retry_data_observation() {
        let root = sinomo_testdir::TestDir::new("dfs-original-repair-external-reload");
        let (store, _, message, descriptor, payload) = original_fixture(&root);
        store
            .enroll_original_repair(&message, &descriptor, &payload)
            .unwrap();
        let (view, observed) = store.view_for_maintenance().unwrap();
        let sequence = view.head_sequence + 1;
        let bytes = encode_sobj(&DfsRepairQueueLogBatch {
            sequence,
            operations: vec![DfsRepairQueueOperation::RepairCustody {
                change: Box::new(RepairCustodyChange::Attempt {
                    subject: payload.root().subject.clone(),
                    frame_id: 99,
                    message_id: 100,
                }),
            }],
        })
        .unwrap();
        let mut file = OpenOptions::new()
            .append(true)
            .open(repair_queue_active_log_path(&root.join("repair.sobj")))
            .unwrap();
        file.write_all(&(bytes.len() as u32).to_le_bytes()).unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert!(matches!(
            store.reconcile_observed(&observed, &view.records).unwrap(),
            DfsRepairQueueObservedReconcileOutcome::GenerationChanged { .. }
        ));
    }

    #[test]
    fn original_repair_callback_rejects_uncovered_legacy_without_inventing_body() {
        let root = sinomo_testdir::TestDir::new("dfs-original-repair-uncovered-callback");
        let (_, _, message, _, _) = original_fixture(&root);
        let data_dir = sinomo_core::local::SinomoDataDir::new(root.join("actual-native-root"));
        let path = crate::default_dfs_repair_attempt_snapshot_path(&data_dir);
        assert!(!path.exists());
        assert!(
            crate::runtime_message::checkpoint_detached_replica_delivery(
                &data_dir,
                crate::node_transport::DfsBackgroundReplicaDeliveryEvent::BeforeDispatch {
                    message
                }
            )
            .is_err()
        );
        assert!(
            !path.exists(),
            "observational rejection cannot enroll an invented original"
        );
    }

    #[test]
    fn original_repair_enrollment_rechecks_new_legacy_uncertainty_under_writer() {
        let root = sinomo_testdir::TestDir::new("dfs-original-repair-stale-enrollment");
        let (store, content, message, descriptor, payload) = original_fixture(&root);
        assert!(
            !store
                .legacy_repair_is_uncovered("original-intent", &message.target_node_id)
                .unwrap()
        );
        let crate::DfsNetworkMessage::RepairReplica(command) = &message.message else {
            unreachable!()
        };
        let mut legacy = record("legacy", "custody-receiver", 1);
        legacy.intent_id = command.intent_id.clone();
        legacy.object_id = command.object_id.clone();
        legacy.version = command.version.clone();
        legacy.content_hash = command.content_hash.clone();
        legacy.in_flight = true;
        legacy.transfer_kind = Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair);
        legacy.transfer_request_id = Some("old-uncertain-request".into());
        legacy.transfer_delivery_state =
            Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied);
        store.append_enqueues([legacy.clone()]).unwrap();
        assert!(
            store
                .enroll_original_repair(&message, &descriptor, &payload)
                .is_err()
        );
        assert!(store.original_repair(&message).unwrap().is_none());
        assert_eq!(store.view().unwrap().records, vec![legacy]);
        content.inspect_repair_payload_root(payload.root()).unwrap();
    }

    #[test]
    fn original_repair_enrolls_existing_explicit_not_sent_birth() {
        let root = sinomo_testdir::TestDir::new("dfs-original-repair-explicit-fresh");
        let (store, _, message, descriptor, payload) = original_fixture(&root);
        let crate::DfsNetworkMessage::RepairReplica(command) = &message.message else {
            unreachable!()
        };
        let mut fresh = record("fresh", "custody-receiver", 0);
        fresh.intent_id = command.intent_id.clone();
        fresh.object_id = command.object_id.clone();
        fresh.version = command.version.clone();
        fresh.content_hash = command.content_hash.clone();
        fresh.transfer_kind = Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair);
        fresh.transfer_delivery_state =
            Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent);
        store.append_enqueues([fresh.clone()]).unwrap();
        let (page, token) = store.maintenance_page(8, &[], None).unwrap();
        assert_eq!(page.records, vec![fresh.clone()]);
        let physical_head = store.view().unwrap().head_sequence;
        store
            .enroll_original_repair(&message, &descriptor, &payload)
            .unwrap();
        store.record_original_repair_attempt(&message).unwrap();
        assert_eq!(store.view().unwrap().records, vec![fresh.clone()],
            "original custody must not rewrite the existing retry DATA");
        assert!(store.view().unwrap().head_sequence > physical_head,
            "custody still advances the exact physical history");
        let mut claimed = fresh;
        claimed.attempts += 1;
        claimed.in_flight = true;
        claimed.transfer_request_id = Some(command.request_id.clone());
        claimed.transfer_delivery_state =
            Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied);
        assert!(matches!(store.reconcile_maintenance_page(
            &token, page.records.len(), &page.records,
            std::slice::from_ref(&claimed), &[], &[]).unwrap(),
            DfsRepairQueuePageReconcileOutcome::Applied { .. }),
            "the actual pre-enrollment page/cursor token must checkpoint its named claim");
        assert_eq!(store.view().unwrap().records, vec![claimed]);
        store.validate_original_repair_dispatch(&message).unwrap();
    }

    #[test]
    fn original_repair_enrollment_invalidates_new_data_and_rejects_existing_identity_conflict() {
        let root = sinomo_testdir::TestDir::new("dfs-original-repair-new-data-token");
        let (store, _, message, descriptor, payload) = original_fixture(&root);
        let (page, token) = store.maintenance_page(8, &[], None).unwrap();
        assert!(page.records.is_empty());
        store.enroll_original_repair(&message, &descriptor, &payload).unwrap();
        assert!(matches!(store.reconcile_maintenance_page(
            &token, 0, &[], &[], &[], &[]).unwrap(),
            DfsRepairQueuePageReconcileOutcome::GenerationChanged { .. }),
            "a real scheduling insertion must invalidate the older DATA observation");

        let root = sinomo_testdir::TestDir::new("dfs-original-repair-conflicting-data-token");
        let (store, _, message, descriptor, payload) = original_fixture(&root);
        let crate::DfsNetworkMessage::RepairReplica(command) = &message.message else {
            unreachable!()
        };
        let mut conflict = record("conflict", "custody-receiver", 0);
        conflict.intent_id = command.intent_id.clone();
        conflict.object_id = DfsObjectId::from_legacy_named("different-original-object").unwrap();
        conflict.version = command.version.clone();
        conflict.content_hash = command.content_hash.clone();
        conflict.transfer_kind = Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair);
        conflict.transfer_delivery_state =
            Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent);
        store.append_enqueues([conflict.clone()]).unwrap();
        let (page, token) = store.maintenance_page(8, &[], None).unwrap();
        assert!(store.enroll_original_repair(&message, &descriptor, &payload).is_err());
        assert!(store.original_repair(&message).unwrap().is_none());
        assert_eq!(store.view().unwrap().records, vec![conflict]);
        assert!(matches!(store.reconcile_maintenance_page(
            &token, page.records.len(), &page.records, &page.records, &[], &[]).unwrap(),
            DfsRepairQueuePageReconcileOutcome::Applied { .. }),
            "rejected enrollment must leave the original row and DATA token intact");
    }

    #[test]
    fn original_repair_zero_attempts_and_missing_ids_do_not_prove_not_sent() {
        let root = sinomo_testdir::TestDir::new("dfs-original-repair-anonymous-unknown");
        let (store, _, message, descriptor, payload) = original_fixture(&root);
        let crate::DfsNetworkMessage::RepairReplica(command) = &message.message else {
            unreachable!()
        };
        let mut unknown = record("unknown", "custody-receiver", 0);
        unknown.intent_id = command.intent_id.clone();
        unknown.object_id = command.object_id.clone();
        unknown.version = command.version.clone();
        unknown.content_hash = command.content_hash.clone();
        unknown.transfer_kind = Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair);
        assert!(
            unknown.transfer_request_id.is_none()
                && unknown.completed_transfer_request_id.is_none()
                && unknown.transfer_delivery_state.is_none()
                && !unknown.in_flight
        );
        store.append_enqueues([unknown]).unwrap();
        assert!(
            store
                .enroll_original_repair(&message, &descriptor, &payload)
                .is_err()
        );
    }

    #[test]
    fn original_repair_retirement_preserves_unclosed_birth_and_retires_unrelated_row() {
        let root = sinomo_testdir::TestDir::new("dfs-original-repair-retirement");
        let (store, content, message, descriptor, payload) = original_fixture(&root);
        let birth = store
            .enroll_original_repair(&message, &descriptor, &payload)
            .unwrap();
        let response = original_response(&message, true);
        store
            .observe_original_repair_result(&message.source_node_id, &response)
            .unwrap();
        // Assembled DATA crash cut: the old terminal scheduling checkpoint
        // exists, but no native application/ClosedRepairBirth is asserted.
        let mut original = store.view().unwrap().records.remove(0);
        original.in_flight = false;
        original.transfer_request_id = None;
        original.completed_transfer_request_id = Some("original-request".into());
        let mut other = record("other-settled", "unrelated-target", 1);
        other.transfer_kind = Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair);
        other.completed_transfer_request_id = Some("other-completed".into());
        store
            .append_operations(vec![
                DfsRepairQueueOperation::Set {
                    record: original.clone(),
                },
                DfsRepairQueueOperation::EnqueueIfMissing {
                    record: other.clone(),
                },
            ])
            .unwrap();
        let (page, token) = store.maintenance_page(8, &[], None).unwrap();
        assert!(matches!(
            store
                .reconcile_maintenance_page(
                    &token,
                    page.records.len(),
                    &page.records,
                    &[],
                    &[original.clone(), other],
                    &[]
                )
                .unwrap(),
            DfsRepairQueuePageReconcileOutcome::Applied {
                removed_target_count: 1,
                ..
            }
        ));
        assert_eq!(store.view().unwrap().records, vec![original]);
        let retained = store.original_repair(&message).unwrap().unwrap();
        assert_eq!(retained.observed, vec![response]);
        assert!(retained.closed_at_sequence.is_none());
        assert!(
            store
                .closed_repair_birth(&birth.root.subject)
                .unwrap()
                .is_none()
        );
        content.inspect_repair_payload_root(&retained.root).unwrap();
    }

    fn record(name: &str, target: &str, attempts: u32) -> DfsRepairAttemptRecord {
        DfsRepairAttemptRecord {
            transfer_delivery_state: None,
            completed_transfer_request_id: None,
            transfer_kind: None,
            intent_id: format!("intent-{name}"),
            object_id: DfsObjectId::from_legacy_named(format!("object-{name}")).expect("object"),
            version: DfsVersion::from_legacy_named(format!("version-{name}")).expect("version"),
            content_hash: format!("blake3:{name}"),
            missing_replica_node_id: NodeId::from_legacy_named(target).expect("target"),
            attempts,
            in_flight: false,
            last_attempt_unix_nanos: u64::from(attempts),
            next_retry_unix_nanos: u64::from(attempts).saturating_add(10),
            transfer_request_id: None,
            last_error: (attempts > 0).then(|| "retry".to_string()),
        }
    }

    fn reopen(snapshot_path: &Path) -> LocalDfsRepairQueueStore {
        LocalDfsRepairQueueStore::evict_for_test(snapshot_path);
        LocalDfsRepairQueueStore::new(snapshot_path)
    }

    fn append_external_batch(path: &Path, sequence: u64, records: Vec<DfsRepairAttemptRecord>) {
        let bytes = encode_sobj(&DfsRepairQueueLogBatch {
            sequence,
            operations: records
                .into_iter()
                .map(|record| DfsRepairQueueOperation::EnqueueIfMissing { record })
                .collect(),
        })
        .unwrap();
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        durable_file::append_length_prefixed_records(
            &mut file,
            path,
            [bytes.as_slice()],
            durable_file::DurableRecordAppendOptions::new(
                durable_file::DurableLengthEndian::Little,
            )
            .sync(durable_file::DurableFileSync::All),
        )
        .unwrap();
    }

    #[test]
    fn repair_queue_page_rejection_preserves_records_and_next_observable_page() {
        let root = sinomo_testdir::TestDir::new("repair-page-atomic-rejection");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let alpha = record("alpha", "node-alpha", 3);
        let beta = record("beta", "node-beta", 4);
        let gamma = record("gamma", "node-gamma", 5);
        store
            .append_enqueues([alpha.clone(), beta.clone(), gamma.clone()])
            .unwrap();
        let (first, first_token) = store.maintenance_page(1, &[], None).unwrap();
        assert_eq!(first.records, vec![alpha.clone()]);
        assert!(matches!(
            store
                .reconcile_maintenance_page(&first_token, 1, &[], &first.records, &[], &[])
                .unwrap(),
            DfsRepairQueuePageReconcileOutcome::Applied {
                has_more: true,
                operation_count: 0,
                ..
            }
        ));
        let (page, stale_token) = store.maintenance_page(1, &[], None).unwrap();
        assert_eq!(page.records, vec![beta.clone()]);
        let delta = record("delta", "node-delta", 0);
        LocalDfsRepairQueueStore::new(&path)
            .append_enqueues([delta.clone()])
            .unwrap();
        let exact = store.view().unwrap();
        let unwanted = record("unwanted", "node-unwanted", 0);
        assert!(matches!(
            store
                .reconcile_maintenance_page(
                    &stale_token,
                    1,
                    &page.records,
                    &[],
                    &[beta.clone()],
                    &[unwanted.clone()]
                )
                .unwrap(),
            DfsRepairQueuePageReconcileOutcome::GenerationChanged { .. }
        ));
        assert_eq!(
            store.view().unwrap(),
            exact,
            "stale observation applies neither removal nor enqueue"
        );
        let (same, token) = store.maintenance_page(1, &[], None).unwrap();
        assert_eq!(
            same.records, page.records,
            "rejected reconciliation cannot advance past beta"
        );
        let mut incompatible = beta.clone();
        incompatible.version = DfsVersion::from_legacy_named("different-page-version").unwrap();
        assert!(
            store
                .reconcile_maintenance_page(
                    &token,
                    1,
                    &same.records,
                    &[incompatible],
                    &[alpha.clone()],
                    &[unwanted]
                )
                .is_err()
        );
        assert_eq!(
            store.view().unwrap(),
            exact,
            "a later identity error cannot partially apply the earlier retirement"
        );
        assert_eq!(
            store.maintenance_page(1, &[], None).unwrap().0.records,
            same.records
        );
        // The failed operation neither consumed nor forged the genuine token.
        assert!(matches!(
            store
                .reconcile_maintenance_page(&token, 1, &same.records, &same.records, &[], &[])
                .unwrap(),
            DfsRepairQueuePageReconcileOutcome::Applied {
                has_more: true,
                operation_count: 0,
                ..
            }
        ));
        assert_eq!(
            LocalDfsRepairQueueStore::new(&path)
                .maintenance_page(1, &[], None)
                .unwrap()
                .0
                .records,
            vec![delta]
        );
        assert_eq!(store.view().unwrap(), exact);
    }

    #[test]
    fn repair_queue_page_token_cannot_mutate_a_reopened_owner() {
        let root = sinomo_testdir::TestDir::new("repair-page-owner-identity");
        let path = root.join("repair.sobj");
        let original = LocalDfsRepairQueueStore::new(&path);
        let alpha = record("alpha", "node-alpha", 2);
        let beta = record("beta", "node-beta", 3);
        original
            .append_enqueues([alpha.clone(), beta.clone()])
            .unwrap();
        let (page, token) = original.maintenance_page(1, &[], None).unwrap();
        let reopened = reopen(&path);
        let exact = reopened.view().unwrap();
        assert!(matches!(
            reopened
                .reconcile_maintenance_page(
                    &token,
                    1,
                    &page.records,
                    &[],
                    &[alpha.clone()],
                    &[record("extra", "node-extra", 0)]
                )
                .unwrap(),
            DfsRepairQueuePageReconcileOutcome::GenerationChanged { .. }
        ));
        assert_eq!(reopened.view().unwrap(), exact);
        assert_eq!(
            reopened.maintenance_page(1, &[], None).unwrap().0.records,
            vec![alpha]
        );
    }

    #[test]
    fn repair_queue_completion_only_delta_does_not_reopen_finished_parked_sweep() {
        let root = sinomo_testdir::TestDir::new("repair-page-parked-followup");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let mut parked = vec![
            record("alpha", "node-alpha", 3),
            record("beta", "node-beta", 4),
        ];
        for record in &mut parked {
            record.next_retry_unix_nanos = u64::MAX - 1;
        }
        store.append_enqueues(parked.clone()).unwrap();
        let (page, token) = store.maintenance_page(2, &[], None).unwrap();
        assert_eq!(page.total_record_count, 2);
        assert!(matches!(
            store
                .reconcile_maintenance_page(&token, 2, &page.records, &page.records, &[], &[])
                .unwrap(),
            DfsRepairQueuePageReconcileOutcome::Applied {
                has_more: false,
                operation_count: 0,
                ..
            }
        ));
        let exact = store.view().unwrap();
        for _ in 0..3 {
            let (delta, observed) = LocalDfsRepairQueueStore::new(&path)
                .maintenance_page(0, &parked, None)
                .unwrap();
            assert!(delta.records.is_empty());
            assert_eq!(
                delta.total_record_count, 2,
                "empty delta window cannot erase total eligibility count"
            );
            assert!(
                matches!(
                    store
                        .reconcile_maintenance_page(
                            &observed,
                            0,
                            &parked,
                            &parked,
                            &[],
                            &delta.enrollment_records
                        )
                        .unwrap(),
                    DfsRepairQueuePageReconcileOutcome::Applied {
                        has_more: false,
                        operation_count: 0,
                        ..
                    }
                ),
                "unrelated completion with no delta must not manufacture another sweep"
            );
            assert_eq!(store.view().unwrap(), exact);
        }
        let ready = record("new-ready", "node-ready", 0);
        let (delta, observed) = store
            .maintenance_page(0, std::slice::from_ref(&ready), None)
            .unwrap();
        assert!(
            matches!(
                store
                    .reconcile_maintenance_page(
                        &observed,
                        0,
                        &[],
                        &[],
                        &[],
                        &delta.enrollment_records
                    )
                    .unwrap(),
                DfsRepairQueuePageReconcileOutcome::Applied {
                    has_more: true,
                    operation_count: 1,
                    ..
                }
            ),
            "a genuinely new owned repair retains immediate continuation"
        );
        assert!(store.view().unwrap().records.contains(&ready));
    }

    #[test]
    fn repair_queue_threshold_compaction_preserves_page_token_suffix_and_revisit() {
        use crate::maintenance::{
            DfsMaintenanceHost, LocalDfsMaintenanceHost, default_dfs_repair_attempt_snapshot_path,
        };
        let root = sinomo_testdir::TestDir::new("repair-page-threshold-cursor");
        let data_dir = sinomo_core::local::SinomoDataDir::new(root.path());
        let path = default_dfs_repair_attempt_snapshot_path(&data_dir);
        let log = repair_queue_active_log_path(&path);
        let store = LocalDfsRepairQueueStore::new(&path);
        // Real framed bytes cross the existing production threshold. No forced
        // compaction and no private cursor mutation substitutes for the owner.
        let records = (0..8)
            .map(|index| {
                let mut record = record(&format!("b{index}"), "node-target", 3);
                record.transfer_kind =
                    Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair);
                record.transfer_delivery_state =
                    Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent);
                record.last_error = Some("x".repeat(600_000));
                record
            })
            .collect::<Vec<_>>();
        store.append_enqueues(records.clone()).unwrap();
        assert!(fs::metadata(&log).unwrap().len() >= DEFAULT_DFS_REPAIR_QUEUE_COMPACTION_LOG_BYTES);
        let mut host = LocalDfsMaintenanceHost::new(data_dir.clone());
        let page = host.load_repair_maintenance_page(1).unwrap();
        assert_eq!(page.records, records[..1]);
        let keys = std::collections::BTreeSet::from([(
            records[0].intent_id.clone(),
            records[0].missing_replica_node_id.clone(),
        )]);
        let checkpoint = host
            .checkpoint_repair_maintenance_page(&page, &keys, &[], &page.records)
            .unwrap();
        assert!(checkpoint.has_more);
        assert_eq!(checkpoint.removed_target_count, 1);
        assert_eq!(
            fs::metadata(&log).unwrap().len(),
            0,
            "normal page checkpoint retains threshold-based log compaction"
        );
        assert_eq!(store.view().unwrap().records, records[1..]);
        let mut early = record("a0", "node-earlier", 0);
        early.transfer_kind = Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair);
        early.transfer_delivery_state =
            Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent);
        store.append_enqueues([early.clone()]).unwrap();
        let (next, next_token) = store.maintenance_page(1, &[], None).unwrap();
        assert_eq!(
            next.records,
            records[1..2],
            "earlier arrivals do not restart the old suffix"
        );
        let exact = store.view().unwrap();
        let bytes = fs::metadata(&log).unwrap().len();
        assert!(
            !store
                .compact_if_needed(DfsRepairQueueCompactionOptions {
                    max_log_bytes: bytes + 1,
                    force: false
                })
                .unwrap()
                .compacted
        );
        assert!(
            store
                .compact_if_needed(DfsRepairQueueCompactionOptions {
                    max_log_bytes: bytes,
                    force: false
                })
                .unwrap()
                .compacted
        );
        assert_eq!(
            store.view().unwrap(),
            exact,
            "physical rewrite preserves exact logical queue/head"
        );
        let same_owner = LocalDfsRepairQueueStore::new(&path);
        assert_eq!(
            same_owner.maintenance_page(1, &[], None).unwrap().0.records,
            next.records
        );
        assert!(
            matches!(
                same_owner
                    .reconcile_maintenance_page(
                        &next_token,
                        1,
                        &next.records,
                        &[],
                        &next.records,
                        &[]
                    )
                    .unwrap(),
                DfsRepairQueuePageReconcileOutcome::Applied { has_more: true, .. }
            ),
            "the real pre-compaction observation remains valid"
        );
        for expected in &records[2..] {
            let fresh_host_handle = LocalDfsRepairQueueStore::new(&path);
            let (page, observed) = fresh_host_handle.maintenance_page(1, &[], None).unwrap();
            assert_eq!(page.records, vec![expected.clone()]);
            assert!(matches!(
                fresh_host_handle
                    .reconcile_maintenance_page(
                        &observed,
                        1,
                        &page.records,
                        &[],
                        &page.records,
                        &[]
                    )
                    .unwrap(),
                DfsRepairQueuePageReconcileOutcome::Applied { has_more: true, .. }
            ));
        }
        assert_eq!(
            store.maintenance_page(1, &[], None).unwrap().0.records,
            vec![early.clone()],
            "the earlier arrival is revisited after the old tail completes"
        );
        let logical_remaining = store.view().unwrap();
        let cold = reopen(&path);
        assert_eq!(
            cold.view().unwrap(),
            logical_remaining,
            "cold recovery preserves completed removals and remaining work"
        );
        let (last, observed) = cold.maintenance_page(1, &[], None).unwrap();
        assert_eq!(last.records, vec![early]);
        assert!(matches!(
            cold.reconcile_maintenance_page(&observed, 1, &last.records, &[], &last.records, &[])
                .unwrap(),
            DfsRepairQueuePageReconcileOutcome::Applied {
                has_more: false,
                ..
            }
        ));
        assert!(reopen(&path).view().unwrap().records.is_empty());
    }

    #[test]
    fn repair_queue_maintenance_hosts_reuse_exact_fingerprint_and_preserve_backoff() {
        use crate::maintenance::{
            DfsMaintenanceHost, LocalDfsMaintenanceHost, default_dfs_repair_attempt_snapshot_path,
        };
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-hosts");
        let data_dir = sinomo_core::local::SinomoDataDir::new(root.path());
        let path = default_dfs_repair_attempt_snapshot_path(&data_dir);
        let store = LocalDfsRepairQueueStore::new(&path);
        let queued = record("alpha", "node-alpha", 3);
        store.append_enqueues([queued.clone()]).unwrap();
        REPAIR_QUEUE_FULL_REPLAYS.set(0);
        for _ in 0..4 {
            let mut host = LocalDfsMaintenanceHost::new(data_dir.clone());
            let records = host.load_repair_attempt_records().unwrap();
            assert_eq!(records, vec![queued.clone()]);
            host.store_repair_attempt_records(&records).unwrap();
        }
        assert_eq!(
            REPAIR_QUEUE_FULL_REPLAYS.get(),
            0,
            "unchanged maintenance generations must not decode historical batches"
        );
        let other = LocalDfsRepairQueueStore::new(&path);
        other
            .append_enqueues([record("beta", "node-beta", 0)])
            .unwrap();
        let mut host = LocalDfsMaintenanceHost::new(data_dir);
        let records = host.load_repair_attempt_records().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], queued);
        assert_eq!(
            REPAIR_QUEUE_FULL_REPLAYS.get(),
            0,
            "owned append must extend the expected fingerprint without replay"
        );
    }

    #[test]
    fn repair_queue_refresh_observes_external_append_and_rejects_stale_token() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-append");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let alpha = record("alpha", "node-alpha", 3);
        let beta = record("beta", "node-beta", 1);
        store.append_enqueues([alpha.clone()]).unwrap();
        let (_, observed) = store.view_for_maintenance().unwrap();
        append_external_batch(&repair_queue_active_log_path(&path), 2, vec![beta.clone()]);
        assert!(matches!(
            store.reconcile_observed(&observed, &[]).unwrap(),
            DfsRepairQueueObservedReconcileOutcome::GenerationChanged {
                current_head_sequence: 2
            }
        ));
        assert_eq!(
            store.view_for_maintenance().unwrap().0.records,
            vec![alpha, beta]
        );
    }

    #[test]
    fn repair_queue_refresh_same_size_same_head_snapshot_replacement_preserves_retry() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-same-head");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let queued = record("alpha", "node-alpha", 3);
        store.append_enqueues([queued.clone()]).unwrap();
        store
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        let (_, observed) = store.view_for_maintenance().unwrap();
        let before = fs::read(&path).unwrap();
        let mut snapshot = read_repair_queue_snapshot(&path).unwrap();
        snapshot.records[0].last_error = Some("later".into());
        let replacement = encode_sobj(&snapshot).unwrap();
        assert_eq!(before.len(), replacement.len());
        assert_ne!(before, replacement);
        write_file_atomic(&path, &replacement).unwrap();
        assert!(matches!(
            store.reconcile_observed(&observed, &[]).unwrap(),
            DfsRepairQueueObservedReconcileOutcome::GenerationChanged {
                current_head_sequence: 1
            }
        ));
        assert_eq!(
            store.view_for_maintenance().unwrap().0.records,
            snapshot.records
        );
    }

    #[test]
    fn repair_queue_owned_append_does_not_adopt_external_prefix_bytes() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-owned-append");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let mut alpha = record("alpha", "node-alpha", 3);
        store.append_enqueues([alpha.clone()]).unwrap();
        alpha.last_error = Some("later".into());
        let bytes = encode_sobj(&DfsRepairQueueLogBatch {
            sequence: 1,
            operations: vec![DfsRepairQueueOperation::EnqueueIfMissing {
                record: alpha.clone(),
            }],
        })
        .unwrap();
        let mut framed = (bytes.len() as u32).to_le_bytes().to_vec();
        framed.extend(bytes);
        let log = repair_queue_active_log_path(&path);
        assert_eq!(fs::metadata(&log).unwrap().len(), framed.len() as u64);
        write_file_atomic(&log, &framed).unwrap();
        let beta = record("beta", "node-beta", 0);
        store.append_enqueues([beta.clone()]).unwrap();
        REPAIR_QUEUE_FULL_REPLAYS.set(0);
        assert_eq!(
            store.view_for_maintenance().unwrap().0.records,
            vec![alpha, beta]
        );
        assert_eq!(
            REPAIR_QUEUE_FULL_REPLAYS.get(),
            1,
            "own append must hash its known prefix, not silently adopt a changed earlier frame"
        );
    }

    #[test]
    fn repair_queue_refresh_torn_suffix_and_compaction_keep_pending_records() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-torn");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let alpha = record("alpha", "node-alpha", 3);
        store.append_enqueues([alpha.clone()]).unwrap();
        let log = repair_queue_active_log_path(&path);
        OpenOptions::new()
            .append(true)
            .open(&log)
            .unwrap()
            .write_all(&[0xff, 0x01])
            .unwrap();
        assert_eq!(
            store.view_for_maintenance().unwrap().0.records,
            vec![alpha.clone()]
        );
        let beta = record("beta", "node-beta", 0);
        store.append_enqueues([beta.clone()]).unwrap();
        let (view, token) = store.view_for_maintenance().unwrap();
        assert_eq!(view.records, vec![alpha, beta]);
        store
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        REPAIR_QUEUE_FULL_REPLAYS.set(0);
        assert!(matches!(
            store.reconcile_observed(&token, &view.records).unwrap(),
            DfsRepairQueueObservedReconcileOutcome::Applied(_)
        ));
        assert_eq!(
            REPAIR_QUEUE_FULL_REPLAYS.get(),
            0,
            "owned compaction publishes its exact expected bytes without a replay"
        );
        assert_eq!(
            reopen(&path).view_for_maintenance().unwrap().0.records,
            view.records
        );
    }

    #[test]
    fn repair_queue_refresh_corruption_cannot_reuse_resident_authority() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-corrupt");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        store
            .append_enqueues([record("alpha", "node-alpha", 1)])
            .unwrap();
        let log = repair_queue_active_log_path(&path);
        let mut bytes = fs::read(&log).unwrap();
        bytes[4..].fill(0xff);
        write_file_atomic(&log, &bytes).unwrap();
        assert!(store.view_for_maintenance().is_err());
        assert!(matches!(
            store.bounded_status_view(1).unwrap(),
            DfsRepairQueueBoundedStatus::NotHydrated
        ));
        assert!(
            store
                .append_enqueues([record("alpha", "node-alpha", 0)])
                .is_err()
        );
    }

    #[test]
    fn repair_queue_observation_cannot_authorize_another_store_or_new_resident_owner() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-token-owner");
        let path = root.join("repair.sobj");
        let first = LocalDfsRepairQueueStore::new(&path);
        first
            .append_enqueues([record("alpha", "node-alpha", 1)])
            .unwrap();
        let (_, token) = first.view_for_maintenance().unwrap();
        let other = LocalDfsRepairQueueStore::new(root.join("other.sobj"));
        other
            .append_enqueues([record("alpha", "node-alpha", 1)])
            .unwrap();
        assert!(matches!(
            other.reconcile_observed(&token, &[]).unwrap(),
            DfsRepairQueueObservedReconcileOutcome::GenerationChanged { .. }
        ));
        let reopened = reopen(&path);
        assert!(matches!(
            reopened.reconcile_observed(&token, &[]).unwrap(),
            DfsRepairQueueObservedReconcileOutcome::GenerationChanged { .. }
        ));
        assert_eq!(reopened.view().unwrap().records.len(), 1);
    }

    #[test]
    fn repair_queue_refresh_rejects_aba_substitution_of_decoded_snapshot_or_log() {
        for snapshot_case in [false, true] {
            let root = sinomo_testdir::TestDir::new(if snapshot_case {
                "dfs-repair-refresh-snapshot-aba"
            } else {
                "dfs-repair-refresh-log-aba"
            });
            let path = root.join("repair.sobj");
            let store = LocalDfsRepairQueueStore::new(&path);
            let original = record("alpha", "node-alpha", 3);
            store.append_enqueues([original.clone()]).unwrap();
            if snapshot_case {
                store
                    .compact_if_needed(DfsRepairQueueCompactionOptions::force())
                    .unwrap();
            }
            let changed_path = if snapshot_case {
                path.clone()
            } else {
                repair_queue_active_log_path(&path)
            };
            let original_bytes = fs::read(&changed_path).unwrap();
            let mut substituted = original.clone();
            substituted.last_error = Some("later".into());
            let substituted_bytes = if snapshot_case {
                encode_sobj(&DfsRepairQueueSnapshotFile {
                    records: vec![substituted],
                    applied_through_sequence: 1,
                    custody: None,
                })
                .unwrap()
            } else {
                let bytes = encode_sobj(&DfsRepairQueueLogBatch {
                    sequence: 1,
                    operations: vec![DfsRepairQueueOperation::EnqueueIfMissing {
                        record: substituted,
                    }],
                })
                .unwrap();
                let mut framed = (bytes.len() as u32).to_le_bytes().to_vec();
                framed.extend(bytes);
                framed
            };
            assert_eq!(original_bytes.len(), substituted_bytes.len());
            let original_fingerprint = store.inner.lock().unwrap().disk_fingerprint().unwrap();
            store.reload_from_disk_on_next_access();
            let before_path = changed_path.clone();
            REPAIR_QUEUE_BEFORE_REPLAY_HOOK.set(Some(Box::new(move || {
                write_file_atomic(&before_path, &substituted_bytes).unwrap();
            })));
            REPAIR_QUEUE_AFTER_REPLAY_HOOK.set(Some(Box::new(move || {
                write_file_atomic(&changed_path, &original_bytes).unwrap();
            })));
            let error = store
                .view_for_maintenance()
                .expect_err("B's decoded queue cannot be sealed by A's restored bytes");
            assert!(error.to_string().contains("changed while loading"));
            let state = store.inner.lock().unwrap();
            assert!(!state.loaded);
            assert!(state.fingerprint.is_none());
            assert_eq!(
                state.disk_fingerprint().unwrap(),
                original_fingerprint,
                "A is restored despite the rejected B replay"
            );
            drop(state);
            assert_eq!(
                store.view_for_maintenance().unwrap().0.records,
                vec![original]
            );
        }
    }

    #[test]
    fn repair_queue_refresh_fingerprint_includes_covered_frames_and_torn_body() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-consumed-bytes");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let queued = record("alpha", "node-alpha", 3);
        store.append_enqueues([queued.clone()]).unwrap();
        let log = repair_queue_active_log_path(&path);
        let mut covered_and_torn = fs::read(&log).unwrap();
        store
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .unwrap();
        // The complete old frame is covered by snapshot floor 1. The following
        // body is incomplete; both still contribute to the raw fingerprint.
        covered_and_torn.extend_from_slice(&77_u32.to_le_bytes());
        covered_and_torn.extend_from_slice(&[1, 2, 3]);
        write_file_atomic(&log, &covered_and_torn).unwrap();
        assert_eq!(
            store.view_for_maintenance().unwrap().0.records,
            vec![queued.clone()]
        );
        REPAIR_QUEUE_FULL_REPLAYS.set(0);
        assert_eq!(
            store.view_for_maintenance().unwrap().0.records,
            vec![queued]
        );
        assert_eq!(
            REPAIR_QUEUE_FULL_REPLAYS.get(),
            0,
            "the complete consumed byte generation must match on the next pass"
        );
        let state = store.inner.lock().unwrap();
        assert_eq!(
            state.fingerprint.as_ref(),
            Some(&state.disk_fingerprint().unwrap())
        );
        assert!(state.valid_active_log_bytes < state.active_log_bytes);
    }

    #[test]
    fn repair_queue_refresh_rejects_generation_changed_during_load() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-load-race");
        let path = root.join("repair.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        store
            .append_enqueues([record("alpha", "node-alpha", 1)])
            .unwrap();
        store.reload_from_disk_on_next_access();
        let log = repair_queue_active_log_path(&path);
        REPAIR_QUEUE_AFTER_REPLAY_HOOK.set(Some(Box::new(move || {
            append_external_batch(&log, 2, vec![record("beta", "node-beta", 0)]);
        })));
        assert!(store.view_for_maintenance().is_err());
        assert!(matches!(
            store.bounded_status_view(1).unwrap(),
            DfsRepairQueueBoundedStatus::NotHydrated
        ));
        assert_eq!(store.view_for_maintenance().unwrap().0.records.len(), 2);
    }

    #[test]
    fn repair_queue_refresh_never_bypasses_failed_resync_for_noop_retry() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-refresh-resync-failure");
        let store = LocalDfsRepairQueueStore::new(root.join("repair.sobj"));
        let queued = record("alpha", "node-alpha", 2);
        store.append_enqueues([queued.clone()]).unwrap();
        {
            let mut state = store.inner.lock().unwrap();
            state.loaded = false;
            state.resync_on_load = true;
        }
        REPAIR_QUEUE_FAIL_RESYNC.set(true);
        assert!(store.append_enqueues([queued.clone()]).is_err());
        {
            let state = store.inner.lock().unwrap();
            assert!(!state.loaded);
            assert!(state.resync_on_load);
        }
        assert_eq!(store.append_enqueues([queued.clone()]).unwrap(), 0);
        assert_eq!(
            store.view_for_maintenance().unwrap().0.records,
            vec![queued]
        );
        assert!(!store.inner.lock().unwrap().resync_on_load);
    }

    // Queue scheduling DATA and actual Set/reopen only: no transport,
    // publication owner, quorum, healthy response or effect permission.
    fn settled_parked_record(name: &str, target: &str) -> DfsRepairAttemptRecord {
        let mut row = record(name, target, 3);
        row.transfer_kind = Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair);
        row.transfer_delivery_state =
            Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent);
        row.next_retry_unix_nanos = u64::MAX;
        row
    }

    #[test]
    fn retained_activation_wake_persists_only_scheduling_and_reopens() {
        let root = sinomo_testdir::TestDir::new("retained-wake-persist-DATA");
        let path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let parked = settled_parked_record("parked", "node-parked-DATA");
        let mut sibling = settled_parked_record("sibling", "node-sibling-DATA");
        sibling.next_retry_unix_nanos = 60_000;
        store
            .append_enqueues([parked.clone(), sibling.clone()])
            .unwrap();
        let mut expected = parked.clone();
        expected.next_retry_unix_nanos = 42;
        let updated = store.wake_observed_settled_records(&[parked], 42).unwrap();
        assert_eq!(updated, vec![expected.clone()]);
        let reopened = reopen(&path).view().unwrap();
        assert_eq!(reopened.head_sequence, 2);
        let actual = reopened
            .records
            .iter()
            .find(|row| row.intent_id == expected.intent_id)
            .unwrap();
        assert_eq!(
            actual, &expected,
            "all custody/identity/diagnostic fields remain unchanged"
        );
        assert_eq!(
            reopened
                .records
                .iter()
                .find(|row| row.intent_id == sibling.intent_id),
            Some(&sibling)
        );
    }

    #[test]
    fn retained_activation_wake_preserves_newer_named_and_unknown_claims() {
        let root = sinomo_testdir::TestDir::new("retained-wake-concurrent-DATA");
        for unknown_kind in [false, true] {
            let path = root.join(if unknown_kind {
                "unknown.sobj"
            } else {
                "named.sobj"
            });
            let store = LocalDfsRepairQueueStore::new(&path);
            let observed = settled_parked_record("concurrent", "node-concurrent-DATA");
            store.append_enqueues([observed.clone()]).unwrap();
            let mut current = observed.clone();
            if unknown_kind {
                current.transfer_kind = None;
                current.completed_transfer_request_id = Some("prior-DATA-completion".into());
            } else {
                current.transfer_request_id = Some("newer-named-DATA-claim".into());
                current.transfer_delivery_state = None;
                current.in_flight = true;
            }
            store
                .append_operations(vec![DfsRepairQueueOperation::Set {
                    record: current.clone(),
                }])
                .unwrap();
            assert_eq!(
                store
                    .wake_observed_settled_records(&[observed], 42)
                    .unwrap(),
                vec![current.clone()],
                "stale whole-row observations return the newer current custody"
            );
            let actual = reopen(&path).view().unwrap();
            assert_eq!(
                actual.head_sequence, 2,
                "rejected wake appends no scheduling frame"
            );
            assert_eq!(actual.records, vec![current]);
        }
    }

    #[test]
    fn retained_activation_wake_keeps_removed_target_absent() {
        let root = sinomo_testdir::TestDir::new("retained-wake-removed-DATA");
        let path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&path);
        let observed = settled_parked_record("removed", "node-removed-DATA");
        store.append_enqueues([observed.clone()]).unwrap();
        store
            .append_operations(vec![DfsRepairQueueOperation::Remove {
                key: DfsRepairQueueKey::from_record(&observed),
            }])
            .unwrap();
        assert!(
            store
                .wake_observed_settled_records(&[observed], 42)
                .unwrap()
                .is_empty()
        );
        let actual = reopen(&path).view().unwrap();
        assert_eq!(actual.head_sequence, 2);
        assert!(
            actual.records.is_empty(),
            "wake cannot re-enroll a removed target"
        );
    }

    #[test]
    fn retained_activation_wake_preserves_unknown_and_blocked_scheduling() {
        let root = sinomo_testdir::TestDir::new("retained-wake-ineligible-DATA");
        for fault in [
            "null-kind",
            "unknown-delivery",
            "named",
            "in-flight",
            "lost-authority",
            "connectivity",
            "existing-backoff",
        ] {
            let path = root.join(format!("{fault}.sobj"));
            let store = LocalDfsRepairQueueStore::new(&path);
            let mut observed = settled_parked_record(fault, "node-ineligible-DATA");
            match fault {
                "null-kind" => {
                    observed.transfer_kind = None;
                    observed.completed_transfer_request_id =
                        Some("historical-DATA-completion".into());
                }
                "unknown-delivery" => observed.transfer_delivery_state = None,
                "named" => observed.transfer_request_id = Some("retained-DATA-claim".into()),
                "in-flight" => observed.in_flight = true,
                "lost-authority" => {
                    observed.last_error =
                        Some("source is not current metadata shard primary".into())
                }
                "connectivity" => {
                    observed.last_error = Some(
                        crate::maintenance::DFS_REPAIR_EXECUTION_BLOCKED_BY_CONNECTIVITY.into(),
                    )
                }
                "existing-backoff" => observed.next_retry_unix_nanos = 60_000,
                _ => unreachable!(),
            }
            store.append_enqueues([observed.clone()]).unwrap();
            assert!(!crate::repair::retained_repair_schedule_can_wake(&observed));
            assert_eq!(
                store
                    .wake_observed_settled_records(&[observed.clone()], 42)
                    .unwrap(),
                vec![observed.clone()]
            );
            let actual = reopen(&path).view().unwrap();
            assert_eq!(actual.head_sequence, 1, "ineligible wake appends no frame");
            assert_eq!(actual.records, vec![observed]);
        }
    }

    #[test]
    fn transfer_checkpoint_preserves_newer_claims_removal_and_unknown_kind() {
        use sinomo_api::dfs::maintenance::DfsRepairTransferKind;
        use sinomo_core::network::NetworkDeliveryState;
        let root = sinomo_testdir::TestDir::new("transfer-checkpoint-CAS-DATA");
        for fault in ["exact", "changed", "removed", "null-kind", "wrong-key"] {
            let path = root.join(format!("{fault}.sobj"));
            let store = LocalDfsRepairQueueStore::new(&path);
            let mut before = record("claim", "transfer-target-DATA", 1);
            before.transfer_kind = Some(DfsRepairTransferKind::Repair);
            before.transfer_request_id = Some("actual-DATA-request".into());
            before.transfer_delivery_state = Some(NetworkDeliveryState::PossiblyApplied);
            before.in_flight = true;
            if fault == "null-kind" {
                before.transfer_kind = None;
            }
            store.append_enqueues([before.clone()]).unwrap();
            let (rows, observed) = store
                .observe_transfer_targets(
                    &before.intent_id,
                    std::slice::from_ref(&before.missing_replica_node_id),
                )
                .unwrap();
            assert_eq!(rows, vec![before.clone()]);
            assert!(!observed.advances_scan);
            let mut current = before.clone();
            if fault == "changed" {
                current.transfer_request_id = Some("newer-DATA-request".into());
                current.transfer_delivery_state = None;
                store
                    .append_operations(vec![DfsRepairQueueOperation::Set {
                        record: current.clone(),
                    }])
                    .unwrap();
            } else if fault == "removed" {
                store
                    .append_operations(vec![DfsRepairQueueOperation::Remove {
                        key: DfsRepairQueueKey::from_record(&before),
                    }])
                    .unwrap();
            }
            let head = store.view().unwrap().head_sequence;
            let mut desired = before.clone();
            desired.in_flight = false;
            desired.transfer_request_id = None;
            desired.completed_transfer_request_id = Some("actual-DATA-request".into());
            if fault == "wrong-key" {
                desired.intent_id = "foreign-DATA-intent".into();
            }
            assert_eq!(
                store.checkpoint_transfer_record(&before, &desired).unwrap(),
                fault == "exact"
            );
            let actual = reopen(&path).view().unwrap();
            if fault == "exact" {
                assert_eq!(actual.records, vec![desired]);
                assert_eq!(actual.head_sequence, head + 1);
            } else if fault == "removed" {
                assert!(
                    actual.records.is_empty(),
                    "completion cannot re-enroll a removal"
                );
                assert_eq!(actual.head_sequence, head);
            } else {
                assert_eq!(actual.records, vec![current]);
                assert_eq!(actual.head_sequence, head, "rejected Set appends no frame");
            }
        }
        // Pure queue DATA below proves indexed retirement and CAS behavior,
        // never a transport ACK, content receipt, or publication authority.
        for custody in ["unnamed", "named", "null-kind", "stage"] {
            let path = root.join(format!("retirement-{custody}.sobj"));
            let store = LocalDfsRepairQueueStore::new(&path);
            let mut settled = record("retirement", "settled-target-DATA", 2);
            settled.transfer_kind = Some(DfsRepairTransferKind::Repair);
            settled.transfer_delivery_state = Some(NetworkDeliveryState::DefinitelyNotSent);
            let mut unresolved = settled.clone();
            unresolved.missing_replica_node_id =
                NodeId::from_legacy_named("unresolved-target-DATA").unwrap();
            unresolved.transfer_delivery_state = None;
            if custody == "named" {
                unresolved.transfer_request_id = Some("retirement-DATA-request".into());
                unresolved.transfer_delivery_state = Some(NetworkDeliveryState::PossiblyApplied);
                unresolved.in_flight = true;
            } else if custody == "null-kind" {
                unresolved.transfer_kind = None;
                unresolved.transfer_delivery_state = Some(NetworkDeliveryState::DefinitelyNotSent);
            } else if custody == "stage" {
                unresolved.transfer_kind = Some(DfsRepairTransferKind::Stage);
                unresolved.transfer_request_id = Some("stage-DATA-request".into());
                unresolved.transfer_delivery_state = Some(NetworkDeliveryState::PossiblyApplied);
                unresolved.in_flight = true;
            }
            store
                .append_enqueues([settled.clone(), unresolved.clone()])
                .unwrap();
            let log = repair_queue_active_log_path(&path);
            let original_bytes = fs::read(&log).unwrap();
            // The unknown sibling is outside this observed transfer window.
            // Whole-intent retirement must still protect its actual custody.
            let (rows, observed) = store
                .observe_transfer_targets(
                    &settled.intent_id,
                    std::slice::from_ref(&settled.missing_replica_node_id),
                )
                .unwrap();
            assert_eq!(rows, vec![settled.clone()]);
            assert!(matches!(
                store
                    .reconcile_maintenance_page(
                        &observed,
                        1,
                        &rows,
                        &[],
                        std::slice::from_ref(&settled),
                        &[],
                    )
                    .unwrap(),
                DfsRepairQueuePageReconcileOutcome::Applied {
                    removed_target_count: 1,
                    operation_count: 1,
                    ..
                }
            ));
            assert!(fs::read(&log).unwrap().starts_with(&original_bytes));
            let cold = reopen(&path);
            assert_eq!(cold.view().unwrap().records, vec![unresolved.clone()]);
            // An examined unknown row also survives the before/desired loop.
            // Stale captures cannot recreate either retired or absent siblings.
            let (rows, observed) = cold
                .observe_transfer_targets(
                    &unresolved.intent_id,
                    std::slice::from_ref(&unresolved.missing_replica_node_id),
                )
                .unwrap();
            let mut absent = unresolved.clone();
            absent.missing_replica_node_id =
                NodeId::from_legacy_named("absent-retired-target-DATA").unwrap();
            let exact = cold.view().unwrap();
            let bytes = fs::read(&log).unwrap();
            assert!(matches!(
                cold.reconcile_maintenance_page(
                    &observed,
                    1,
                    &rows,
                    &[],
                    std::slice::from_ref(&settled),
                    &[settled.clone(), absent],
                )
                .unwrap(),
                DfsRepairQueuePageReconcileOutcome::Applied {
                    removed_target_count: 0,
                    operation_count: 0,
                    ..
                }
            ));
            assert_eq!(cold.view().unwrap(), exact);
            assert_eq!(fs::read(&log).unwrap(), bytes);
            let (rows, observed) = cold
                .observe_transfer_targets(
                    &unresolved.intent_id,
                    std::slice::from_ref(&unresolved.missing_replica_node_id),
                )
                .unwrap();
            let mut newer = unresolved.clone();
            newer.last_error = Some("newer actual queue DATA".into());
            LocalDfsRepairQueueStore::new(&path)
                .append_operations(vec![DfsRepairQueueOperation::Set {
                    record: newer.clone(),
                }])
                .unwrap();
            let exact = cold.view().unwrap();
            let bytes = fs::read(&log).unwrap();
            assert!(matches!(
                cold.reconcile_maintenance_page(
                    &observed,
                    1,
                    &rows,
                    &[],
                    std::slice::from_ref(&settled),
                    &[],
                )
                .unwrap(),
                DfsRepairQueuePageReconcileOutcome::GenerationChanged { .. }
            ));
            assert_eq!(cold.view().unwrap(), exact);
            assert_eq!(fs::read(&log).unwrap(), bytes);
            let (rows, observed) = cold
                .observe_transfer_targets(
                    &newer.intent_id,
                    std::slice::from_ref(&newer.missing_replica_node_id),
                )
                .unwrap();
            let mut foreign_retirement = settled.clone();
            foreign_retirement.version =
                DfsVersion::from_legacy_named("foreign-retirement-version-DATA").unwrap();
            assert!(
                cold.reconcile_maintenance_page(
                    &observed,
                    1,
                    &rows,
                    &[],
                    &[foreign_retirement],
                    &[],
                )
                .is_err()
            );
            assert_eq!(cold.view().unwrap(), exact);
            assert_eq!(fs::read(&log).unwrap(), bytes);
            assert_eq!(reopen(&path).view().unwrap().records, vec![newer]);
        }
    }

    #[test]
    fn connectivity_wake_persists_only_exact_settled_rows_and_preserves_concurrent_custody() {
        let root = sinomo_testdir::TestDir::new("connectivity-wake-CAS-DATA");
        let wake = &crate::repair::connectivity_repair_schedule_can_wake;
        for fault in [
            "settled",
            "null-kind",
            "unknown",
            "possibly-applied",
            "named",
            "inflight",
            "changed",
            "removed",
        ] {
            let path = root.join(format!("{fault}.sobj"));
            let store = LocalDfsRepairQueueStore::new(&path);
            let mut observed = settled_parked_record(fault, "node-connectivity-wake-DATA");
            observed.last_error =
                Some(crate::maintenance::DFS_REPAIR_EXECUTION_BLOCKED_BY_CONNECTIVITY.into());
            match fault {
                "null-kind" => observed.transfer_kind = None,
                "unknown" => observed.transfer_delivery_state = None,
                "possibly-applied" => {
                    observed.transfer_delivery_state =
                        Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied)
                }
                "named" => {
                    observed.transfer_request_id = Some("actual-unresolved-DATA-claim".into())
                }
                "inflight" => observed.in_flight = true,
                _ => {}
            }
            observed.completed_transfer_request_id = None;
            store.append_enqueues([observed.clone()]).unwrap();
            let mut current = observed.clone();
            if fault == "changed" {
                current.transfer_request_id = Some("newer-DATA-claim".into());
                current.transfer_delivery_state = None;
                current.in_flight = true;
                store
                    .append_operations(vec![DfsRepairQueueOperation::Set {
                        record: current.clone(),
                    }])
                    .unwrap();
            } else if fault == "removed" {
                store
                    .append_operations(vec![DfsRepairQueueOperation::Remove {
                        key: DfsRepairQueueKey::from_record(&observed),
                    }])
                    .unwrap();
            }
            let head = store.view().unwrap().head_sequence;
            let rows = store
                .wake_observed_scheduling_records_if(&[observed.clone()], 42, wake)
                .unwrap();
            let actual = reopen(&path).view().unwrap();
            if fault == "settled" {
                current.next_retry_unix_nanos = 42;
                assert_eq!(rows, vec![current.clone()]);
                assert_eq!(actual.records, vec![current]);
                assert_eq!(
                    actual.head_sequence,
                    head + 1,
                    "one real Set frame durably wakes scheduling only"
                );
            } else if fault == "removed" {
                assert!(rows.is_empty() && actual.records.is_empty());
                assert_eq!(actual.head_sequence, head, "a removed row cannot re-enroll");
            } else {
                assert_eq!(rows, vec![current.clone()]);
                assert_eq!(actual.records, vec![current]);
                assert_eq!(
                    actual.head_sequence, head,
                    "uncertain or changed custody appends no wake frame"
                );
            }
        }
    }

    #[test]
    fn repair_queue_replays_enqueue_set_and_remove_in_order() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-replay");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let alpha = record("alpha", "node-alpha", 0);
        let beta = record("beta", "node-beta", 0);
        assert_eq!(
            store
                .append_enqueues([alpha.clone(), beta.clone()])
                .expect("enqueue"),
            2
        );
        let mut retried_alpha = alpha.clone();
        retried_alpha.attempts = 3;
        retried_alpha.last_error = Some("backoff".to_string());
        store
            .append_operations(vec![
                DfsRepairQueueOperation::Set {
                    record: retried_alpha.clone(),
                },
                DfsRepairQueueOperation::Remove {
                    key: DfsRepairQueueKey::from_record(&beta),
                },
            ])
            .expect("update and remove");

        let reopened = reopen(&snapshot_path);
        let view = reopened.view().expect("replay");
        assert_eq!(view.head_sequence, 2);
        assert_eq!(view.records, vec![retried_alpha]);
    }

    #[test]
    fn repair_queue_enqueue_if_missing_preserves_existing_backoff() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-preserve-backoff");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let existing = record("alpha", "node-alpha", 4);
        store
            .append_enqueues([existing.clone()])
            .expect("initial enqueue");
        assert_eq!(
            store
                .append_enqueues([record("alpha", "node-alpha", 0)])
                .expect("idempotent enqueue"),
            0
        );
        let view = reopen(&snapshot_path).view().expect("replay");
        assert_eq!(view.head_sequence, 1);
        assert_eq!(view.records, vec![existing]);
    }

    #[test]
    fn bounded_status_samples_large_resident_queue_without_replay_or_full_clone() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-bounded-status");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let mut records = (0..4096)
            .map(|index| {
                record(
                    &format!("bounded-{index:04}"),
                    &format!("node-bounded-{index:04}"),
                    0,
                )
            })
            .collect::<Vec<_>>();
        let sentinel = record("zzzz-sentinel", "node-zzzz-sentinel", 0);
        records.push(sentinel.clone());
        store.append_enqueues(records).expect("seed large queue");

        BOUNDED_STATUS_RECORD_VISITS.set(0);
        let DfsRepairQueueBoundedStatus::Hydrated(view) =
            store.bounded_status_view(32).expect("bounded hot view")
        else {
            panic!("materialized queue must return a hydrated bounded view");
        };
        assert_eq!(view.record_count, 4097);
        assert_eq!(view.records.len(), 32);
        assert!(!view.records.contains(&sentinel));
        assert_eq!(
            BOUNDED_STATUS_RECORD_VISITS.get(),
            32,
            "status may visit and clone only the requested sample, not the complete resident queue"
        );

        let cold = reopen(&snapshot_path);
        fs::write(&snapshot_path, b"not a repair queue snapshot")
            .expect("install disk replay sentinel");
        let cold_status = cold
            .bounded_status_view(32)
            .expect("cold bounded status must not replay disk");
        assert_eq!(cold_status, DfsRepairQueueBoundedStatus::NotHydrated);
    }

    #[test]
    fn repair_queue_enqueue_rejects_conflicting_duplicate_batch_identity() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-conflicting-batch");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let original = record("alpha", "node-alpha", 0);
        let mut conflicting = original.clone();
        conflicting.content_hash = "blake3:conflicting".to_string();

        assert!(
            store
                .append_enqueues([original, conflicting])
                .expect_err("conflicting duplicate identity must fail")
                .to_string()
                .contains("changed object, version, or content hash")
        );
        assert!(store.view().expect("unchanged queue").records.is_empty());
    }

    #[test]
    fn repair_queue_custom_snapshot_paths_have_distinct_active_logs() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-distinct-logs");
        let first = root.join("first.sobj");
        let second = root.join("second.sobj");

        assert_ne!(
            repair_queue_active_log_path(&first),
            repair_queue_active_log_path(&second)
        );
        assert_eq!(
            repair_queue_active_log_path(&first),
            root.join("first.active.slog")
        );
    }

    #[test]
    fn repair_queue_resyncs_uncertain_complete_frame_before_noop_retry() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-resync-noop");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let queued = record("alpha", "node-alpha", 0);
        store
            .append_enqueues([queued.clone()])
            .expect("initial enqueue");
        {
            let mut state = store.inner.lock().expect("queue state");
            // Model append_length_prefixed_records having written a complete
            // frame before its sync returned an error.
            state.loaded = false;
            state.resync_on_load = true;
        }

        assert_eq!(
            store
                .append_enqueues([queued.clone()])
                .expect("idempotent retry after resync"),
            0
        );
        let state = store.inner.lock().expect("queue state");
        assert!(state.loaded);
        assert!(!state.resync_on_load);
        assert_eq!(
            state.records.values().cloned().collect::<Vec<_>>(),
            vec![queued]
        );
    }

    #[test]
    fn repair_queue_reconcile_rejects_stale_head_without_losing_enqueue() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-cas");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let stale = store.view().expect("empty view");
        let queued = record("alpha", "node-alpha", 0);
        store
            .append_enqueues([queued.clone()])
            .expect("concurrent enqueue");

        assert_eq!(
            store.reconcile(stale.head_sequence, &[]).expect("CAS"),
            DfsRepairQueueReconcileOutcome::GenerationChanged {
                current_head_sequence: 1,
            }
        );
        assert_eq!(store.view().expect("current view").records, vec![queued]);
    }

    #[test]
    fn repair_queue_torn_tail_is_truncated_before_later_append() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-torn-tail");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let alpha = record("alpha", "node-alpha", 0);
        store
            .append_enqueues([alpha.clone()])
            .expect("first enqueue");
        let active_log_path = repair_queue_active_log_path(&snapshot_path);
        let valid_len = fs::metadata(&active_log_path).expect("valid log").len();
        drop(store);
        LocalDfsRepairQueueStore::evict_for_test(&snapshot_path);
        let mut file = OpenOptions::new()
            .append(true)
            .open(&active_log_path)
            .expect("open log");
        file.write_all(&64_u32.to_le_bytes()).expect("length");
        file.write_all(b"torn").expect("torn bytes");
        file.sync_all().expect("sync torn bytes");
        drop(file);

        let reopened = LocalDfsRepairQueueStore::new(&snapshot_path);
        assert_eq!(
            reopened.view().expect("read valid prefix").records,
            vec![alpha]
        );
        let beta = record("beta", "node-beta", 0);
        reopened
            .append_enqueues([beta.clone()])
            .expect("append after repair");
        assert!(fs::metadata(&active_log_path).expect("repaired log").len() > valid_len);
        assert_eq!(
            reopen(&snapshot_path)
                .view()
                .expect("replay repaired log")
                .records,
            vec![record("alpha", "node-alpha", 0), beta]
        );
    }

    #[test]
    fn repair_queue_empty_snapshot_floor_prevents_enqueue_resurrection() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-empty-compaction");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let queued = record("alpha", "node-alpha", 0);
        store.append_enqueues([queued.clone()]).expect("enqueue");
        store
            .append_operations(vec![DfsRepairQueueOperation::Remove {
                key: DfsRepairQueueKey::from_record(&queued),
            }])
            .expect("remove");
        let active_log_path = repair_queue_active_log_path(&snapshot_path);
        let old_log = fs::read(&active_log_path).expect("old log");
        let report = store
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .expect("compact empty queue");
        assert!(report.compacted);
        assert_eq!(report.applied_through_sequence, 2);
        let snapshot = read_repair_queue_snapshot(&snapshot_path).expect("snapshot");
        assert_eq!(snapshot.applied_through_sequence, 2);
        assert!(snapshot.records.is_empty());

        // Model a crash after snapshot publication but before old-log pruning.
        fs::write(&active_log_path, old_log).expect("restore retained old log");
        let view = reopen(&snapshot_path).view().expect("replay floor");
        assert_eq!(view.head_sequence, 2);
        assert!(view.records.is_empty());
    }

    #[test]
    fn repair_queue_compaction_preserves_later_active_suffix() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-compaction-suffix");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let alpha = record("alpha", "node-alpha", 0);
        let beta = record("beta", "node-beta", 0);
        store
            .append_enqueues([alpha.clone()])
            .expect("enqueue before compaction");
        store
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .expect("compact");
        store
            .append_enqueues([beta.clone()])
            .expect("enqueue suffix");

        let view = reopen(&snapshot_path)
            .view()
            .expect("replay snapshot and suffix");
        assert_eq!(view.head_sequence, 2);
        assert_eq!(view.records, vec![alpha, beta]);
    }

    #[test]
    fn repair_queue_explicit_reload_observes_external_snapshot_corruption() {
        let root = sinomo_testdir::TestDir::new("dfs-repair-queue-explicit-reload");
        let snapshot_path = root.join("repair-attempts.sobj");
        let store = LocalDfsRepairQueueStore::new(&snapshot_path);
        let queued = record("alpha", "node-alpha", 0);
        store
            .append_enqueues([queued.clone()])
            .expect("enqueue before compaction");
        store
            .compact_if_needed(DfsRepairQueueCompactionOptions::force())
            .expect("compact queue");
        fs::write(&snapshot_path, b"invalid repair queue snapshot")
            .expect("replace snapshot externally");

        assert_eq!(
            store.view().expect("hot view remains materialized").records,
            vec![queued]
        );
        store.reload_from_disk_on_next_access();
        assert!(
            store.view().is_err(),
            "an explicit maintenance reload must observe external corruption"
        );
    }
    #[test]
    fn selected_original_data_captures_recovery_birth_without_a_retry_projection() {
        let root = sinomo_testdir::TestDir::new("selected-original-no-retry-row");
        let (source, _content, original, attachment, payload, identity) = selected_original_fixture(&root.join("source"));
        source.enroll_original_repair(&original, &attachment, &payload).unwrap();
        source.record_original_repair_attempt(&original).unwrap();
        source.compact_if_needed(DfsRepairQueueCompactionOptions::force()).unwrap();
        let path = identity.root().join("repair.sobj");
        let mut retained = read_repair_queue_snapshot(&path).unwrap();
        // Construct a retained recovery snapshot, not an allowed ordinary
        // Remove transition: current removal must keep rejecting open births.
        retained.records.clear();
        write_file_atomic(&path, &encode_sobj(&retained).unwrap()).unwrap();
        drop(source); LocalDfsRepairQueueStore::evict_for_test(&path);
        let source = LocalDfsRepairQueueStore::reopen_selected_original_fixture(&identity).unwrap();
        assert!(source.view().unwrap().records.is_empty());
        let crate::DfsNetworkMessage::RepairReplica(command) = &original.message else { panic!("Repair fixture"); };
        let retiring = command.authority_shard.clone().unwrap();
        let desired = crate::DfsMetadataShardConfig {
            primary_node_id: NodeId::from_legacy_named("selected-original-successor").unwrap(),
            replica_node_ids: vec![retiring.primary_node_id.clone()],
            epoch: retiring.epoch + 1,
            ..retiring.clone()
        };
        let handoff = crate::DfsMetadataShardMembershipHandoff {
            cause: crate::DfsMetadataShardHandoffCause::Membership,
            handoff_id: "selected-original-without-row".into(),
            desired_shard: desired,
            required_prepared_sequence: 17,
            required_head_watermark: None,
            retiring_primary_fence: Some(crate::DfsMetadataShardMembershipHandoffFence {
                handoff_id: "selected-original-without-row".into(),
                primary_node_id: retiring.primary_node_id.clone(),
                journal_sequence: 17,
                state_fingerprint: "local-persistence-fixture-only".into(),
            }),
            retiring_shard: retiring,
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let export = crate::repair_handoff::RepairHandoffExport::capture_selected_original(&identity, &handoff, &source, &command.request_id, |_| Ok(true), deadline).unwrap();
        assert_eq!(export.cut.record_count, 0);
        assert_eq!(export.cut.page_count, 1);
        let (destination, destination_identity) = LocalDfsRepairQueueStore::new_selected_original_fixture(root.join("destination")).unwrap();
        crate::repair_handoff::receive_selected_original_data(&destination_identity, &export.page(0).unwrap(), deadline).unwrap();
        assert!(crate::repair_handoff::install_selected_original_data(&destination_identity, &export.cut, &destination, deadline).unwrap());
        let imported = destination.installed_original_handoff_data(&original.source_node_id, &command.request_id).unwrap().unwrap();
        assert_eq!(imported.material.birth.original, original);
        assert_eq!(imported.material.birth.attempts.len(), 1);
        assert!(destination.view().unwrap().records.is_empty());
        assert!(destination.original_repair_for_slot(&command.intent_id, &original.target_node_id).unwrap().is_none());
    }

    #[test]
    fn selected_original_data_missing_incarnation_cannot_be_reinitialized_on_reopen() {
        let root = sinomo_testdir::TestDir::new("selected-original-lost-incarnation");
        let (store, identity) = LocalDfsRepairQueueStore::new_selected_original_fixture(root.join("queue")).unwrap();
        let snapshot = identity.root().join("repair.sobj");
        let log = repair_queue_active_log_path(&snapshot);
        let marker_path = identity.root().join(SELECTED_ORIGINAL_FIXTURE_MARKER);
        let marker = fs::read(&marker_path).unwrap();
        drop(store); LocalDfsRepairQueueStore::evict_for_test(&snapshot);
        fs::write(&log, []).unwrap();
        assert!(LocalDfsRepairQueueStore::reopen_selected_original_fixture(&identity).is_err());
        assert!(fs::read(&log).unwrap().is_empty());
        assert_eq!(fs::read(&marker_path).unwrap(), marker);
        assert!(!snapshot.exists());
    }

    #[test]
    fn selected_original_data_fresh_registry_reservation_never_adopts_a_racing_ordinary_owner() {
        let root = sinomo_testdir::TestDir::new("selected-original-registry-race");
        let directory = root.join("candidate");
        let path = directory.join("repair.sobj");
        let observed = Arc::new(Mutex::new(None));
        let saved = Arc::clone(&observed);
        let registered_path = path.clone();
        SELECTED_FIXTURE_BEFORE_RESERVATION_HOOK.set(Some(Box::new(move || {
            let ordinary = LocalDfsRepairQueueStore::new(registered_path);
            *saved.lock().unwrap() = Some(ordinary);
        })));
        assert!(LocalDfsRepairQueueStore::new_selected_original_fixture(&directory).is_err());
        assert!(SELECTED_FIXTURE_BEFORE_RESERVATION_HOOK.take().is_none());
        assert!(!directory.exists(), "rejected fresh creation performs no filesystem adoption");
        let ordinary = observed.lock().unwrap().take().unwrap();
        assert!(ordinary.view().unwrap().records.is_empty());
        ordinary.append_enqueues([record("original-ordinary-owner", "original-target", 0)]).unwrap();
        assert_eq!(ordinary.view().unwrap().records.len(), 1);
        assert!(!directory.join(SELECTED_ORIGINAL_FIXTURE_MARKER).exists());
    }

    #[test]
    fn selected_original_data_pending_reservation_rejects_default_handle_before_filesystem_work() {
        let root = sinomo_testdir::TestDir::new("selected-original-pending-owner");
        let directory = root.join("candidate");
        let path = directory.join("repair.sobj");
        let observed = Arc::new(Mutex::new(None));
        let saved = Arc::clone(&observed);
        let pending_directory = directory.clone();
        SELECTED_FIXTURE_AFTER_RESERVATION_HOOK.set(Some(Box::new(move || {
            let ordinary = LocalDfsRepairQueueStore::new(path);
            assert!(ordinary.view().is_err());
            assert!(!pending_directory.exists());
            *saved.lock().unwrap() = Some(ordinary);
        })));
        let (selected, identity) = LocalDfsRepairQueueStore::new_selected_original_fixture(&directory).unwrap();
        assert!(SELECTED_FIXTURE_AFTER_RESERVATION_HOOK.take().is_none());
        assert_eq!(selected.custody_incarnation().unwrap(), identity.marker.queue_incarnation);
        assert!(observed.lock().unwrap().as_ref().unwrap().view().is_err());
    }

}
