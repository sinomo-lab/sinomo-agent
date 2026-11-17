//! Metadata backend API surface.
//!
//! This module owns the public metadata backend contract and default scan
//! helpers. Concrete local storage lives in `metadata_backend`.

use std::collections::{BTreeMap, BTreeSet};

use sinomo_core::node::NodeId;

use crate::advisory_lock::DfsAdvisoryLockStateRecord;
use crate::content_root_catalog::{
    DfsContentRootCatalogIncompleteReason, DfsContentRootCatalogPage,
    DfsContentRootCatalogPageRequest,
};
use crate::control_plane::DfsMetadataShardConfig;
use crate::intent::write_intent_status_is_active;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_hardlink::{object_is_hardlink_to_target, sort_hardlinks_by_path};
use crate::metadata_history_gc::{
    DfsMetadataContentProtection, DfsMetadataHistoryGcOptions, DfsMetadataHistoryGcReport,
};
use crate::metadata_journal::bounded_metadata_journal_batch;
use crate::metadata_keys::{normalize_posix_path, write_intent_page_cursor};
use crate::metadata_limits::OBJECT_LOCAL_METADATA_PAGE_SIZE;
use crate::metadata_pages::{
    collect_filtered_cursor_page, collect_filtered_metadata_pages, collect_metadata_pages,
    walk_metadata_pages,
};
use crate::metadata_records::*;
use crate::metadata_summary::count_pending_repairs_for_object;
use crate::posix_path::posix_path_is_at_or_under_prefix;
use crate::{
    DfsFileSystem, DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsVersion,
};

/// Explicit native-fixture representation context. This grants neither transfer
/// authorization nor serving/execution authority and cannot be deserialized.
/// Ordinary native constructors never create or infer one from disk or a page.
#[derive(Debug)]
pub struct DfsSelectedOriginalDataLayout {
    data_root: std::path::PathBuf,
    data_identity: (u64, u64),
    owner_identity: (u64, u64),
    routing_layout: crate::selected_fixture::SelectedOriginalFixtureIdentity,
    stages: std::sync::Mutex<BTreeMap<(DfsFileSystemId, crate::DfsMetadataShardId), crate::selected_fixture::SelectedOriginalFixtureIdentity>>,
}

fn selected_layout_rejection(message: impl Into<String>) -> MetadataBackendError {
    MetadataBackendError::Rejected {
        code: crate::DfsMetadataRejectionCode::Other,
        message: message.into(),
    }
}

impl DfsSelectedOriginalDataLayout {
    /// Directory entries may change length/timestamps/link count without
    /// replacing the physical directory. Only stable platform identity and
    /// physical directory type bind these three observations to the held handle.
    fn same_directory_observations(
        before: &std::fs::Metadata,
        opened: &std::fs::Metadata,
        after: &std::fs::Metadata,
    ) -> bool {
        if [before, opened, after].iter().any(|metadata| {
            !metadata.is_dir() || metadata.file_type().is_symlink()
        }) {
            return false;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            let identity = (opened.dev(), opened.ino());
            (before.dev(), before.ino()) == identity
                && (after.dev(), after.ino()) == identity
        }
        #[cfg(not(unix))]
        {
            // No timestamp/path fallback for unavailable directory identity.
            false
        }
    }

    fn directory_identity(path: &std::path::Path) -> Result<(u64, u64)> {
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|error| selected_layout_rejection(error.to_string()))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(selected_layout_rejection("selected native root is not a physical directory"));
        }
        let file = std::fs::File::open(path)
            .map_err(|error| selected_layout_rejection(error.to_string()))?;
        let opened = file.metadata()
            .map_err(|error| selected_layout_rejection(error.to_string()))?;
        let after = std::fs::symlink_metadata(path)
            .map_err(|error| selected_layout_rejection(error.to_string()))?;
        if !Self::same_directory_observations(&metadata, &opened, &after) {
            return Err(selected_layout_rejection("selected native directory changed while opening its physical identity"));
        }
        crate::runtime_host::storage_file_identity(&file)
            .map_err(|error| selected_layout_rejection(error.to_string()))
    }

    fn named_owner_identity(data_root: &std::path::Path) -> Result<(u64, u64)> {
        let path = data_root.join("daemon/data-owner.lock");
        let before = std::fs::symlink_metadata(&path)
            .map_err(|error| selected_layout_rejection(error.to_string()))?;
        if !before.is_file() || before.file_type().is_symlink() {
            return Err(selected_layout_rejection("selected named owner is not a physical file"));
        }
        let file = std::fs::File::open(&path)
            .map_err(|error| selected_layout_rejection(error.to_string()))?;
        let opened = file.metadata().map_err(|error| selected_layout_rejection(error.to_string()))?;
        let after = std::fs::symlink_metadata(&path)
            .map_err(|error| selected_layout_rejection(error.to_string()))?;
        if after.file_type().is_symlink()
            || !crate::selected_fixture::same_file_observation(&before, &opened)
            || !crate::selected_fixture::same_file_observation(&opened, &after)
        { return Err(selected_layout_rejection("selected named owner changed while opening")); }
        crate::runtime_host::storage_file_identity(&file)
            .map_err(|error| selected_layout_rejection(error.to_string()))
    }

    /// Recheck physical identity only; the caller independently retains native
    /// DATA ownership and the ordinary current authority/admission proof.
    pub(crate) fn check(&self) -> Result<()> {
        if Self::directory_identity(&self.data_root)? != self.data_identity {
            return Err(selected_layout_rejection("selected native DATA root was replaced"));
        }
        if Self::named_owner_identity(&self.data_root)? != self.owner_identity
        {
            return Err(selected_layout_rejection("selected native owner file was replaced"));
        }
        self.routing_layout.check().map_err(|error| selected_layout_rejection(error.to_string()))
    }

    pub(crate) fn check_owner(&self, owner: &crate::runtime_host::DfsDataOwnerLock) -> Result<()> {
        self.check()?;
        if owner.physical_data_root() != self.data_root
            || owner.storage_owner_file_identity()
                .map_err(|error| selected_layout_rejection(error.to_string()))? != self.owner_identity
        {
            return Err(selected_layout_rejection("selected layout belongs to another retained DATA owner"));
        }
        Ok(())
    }

    pub(crate) fn check_routing_root(&self, root: &std::path::Path) -> Result<()> {
        self.check()?;
        if self.routing_layout.root() != root {
            return Err(selected_layout_rejection("selected router names another metadata root"));
        }
        Ok(())
    }

    pub(crate) fn stage(
        &self,
        filesystem: &DfsFileSystemId,
        shard: &crate::DfsMetadataShardId,
        root: &std::path::Path,
    ) -> Result<crate::selected_fixture::SelectedOriginalFixtureIdentity> {
        self.check()?;
        let stages = self.stages.lock()
            .map_err(|_| selected_layout_rejection("selected native stage registry poisoned"))?;
        let stage = stages.get(&(filesystem.clone(), shard.clone()))
            .ok_or_else(|| selected_layout_rejection("backend has no explicitly admitted selected stage"))?;
        if stage.root() != root {
            return Err(selected_layout_rejection("selected backend names another physical stage"));
        }
        stage.check().map_err(|error| selected_layout_rejection(error.to_string()))?;
        Ok(stage.clone())
    }

    /// Only an explicit fresh native fixture creates its expected stage before
    /// the backend's first preparation/recovery read. Reopen reuses this entry.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn admit_stage(
        &self,
        filesystem: &DfsFileSystemId,
        shard: &crate::DfsMetadataShardId,
        root: &std::path::Path,
    ) -> Result<()> {
        self.check()?;
        let data_dir = sinomo_core::local::SinomoDataDir::new(&self.data_root);
        let expected = crate::default_dfs_metadata_shards_root(&data_dir)
            .join(crate::metadata_keys::key_segment(&shard.0));
        if expected != root {
            return Err(selected_layout_rejection("selected shard root is outside its exact native layout"));
        }
        let mut stages = self.stages.lock()
            .map_err(|_| selected_layout_rejection("selected native stage registry poisoned"))?;
        let key = (filesystem.clone(), shard.clone());
        if let Some(stage) = stages.get(&key) {
            stage.check().map_err(|error| selected_layout_rejection(error.to_string()))?;
            if stage.root() != root {
                return Err(selected_layout_rejection("selected shard root changed across reopen"));
            }
            return Ok(());
        }
        if stages.values().any(|stage| stage.root() == root) {
            return Err(selected_layout_rejection("selected physical shard already belongs to another filesystem"));
        }
        let parent = root.parent().ok_or_else(|| selected_layout_rejection("selected stage has no parent"))?;
        std::fs::create_dir_all(parent).map_err(|error| selected_layout_rejection(error.to_string()))?;
        let stage = self.routing_layout.admit_staging_root(root.to_path_buf())
            .map_err(|error| selected_layout_rejection(error.to_string()))?;
        stages.insert(key, stage);
        Ok(())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn fresh(owner: &crate::runtime_host::DfsDataOwnerLock) -> Result<std::sync::Arc<Self>> {
        let data_root = owner.physical_data_root().to_path_buf();
        let data_identity = Self::directory_identity(&data_root)?;
        let owner_identity = owner.storage_owner_file_identity()
            .map_err(|error| selected_layout_rejection(error.to_string()))?;
        if Self::named_owner_identity(&data_root)? != owner_identity
        {
            return Err(selected_layout_rejection("fresh selected fixture lost its actual named DATA lock"));
        }
        let data_dir = sinomo_core::local::SinomoDataDir::new(&data_root);
        let metadata = crate::default_dfs_metadata_shards_root(&data_dir);
        if metadata.exists() && std::fs::read_dir(&metadata)
            .map_err(|error| selected_layout_rejection(error.to_string()))?.next().is_some()
        {
            return Err(selected_layout_rejection("fresh selected native fixture cannot adopt existing metadata"));
        }
        let parent = metadata.parent().ok_or_else(|| selected_layout_rejection("selected metadata root has no namespace"))?;
        std::fs::create_dir_all(parent).map_err(|error| selected_layout_rejection(error.to_string()))?;
        Self::directory_identity(parent)?;
        if metadata.exists() {
            std::fs::remove_dir(&metadata).map_err(|error| selected_layout_rejection(error.to_string()))?;
        }
        let routing_layout = crate::selected_fixture::SelectedOriginalFixtureIdentity::create(metadata)
            .map_err(|error| selected_layout_rejection(error.to_string()))?;
        sinomo_core::storage::durable_file::sync_directory(&data_root)
            .map_err(|error| selected_layout_rejection(error.to_string()))?;
        if let Some(parent) = data_root.parent() {
            sinomo_core::storage::durable_file::sync_directory(parent)
                .map_err(|error| selected_layout_rejection(error.to_string()))?;
        }
        let context = std::sync::Arc::new(Self {
            data_identity,
            owner_identity,
            data_root, routing_layout,
            stages: std::sync::Mutex::new(BTreeMap::new()),
        });
        context.check_owner(owner)?;
        Ok(context)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn reopen(&self, owner: &crate::runtime_host::DfsDataOwnerLock) -> Result<()> {
        self.check_owner(owner)?;
        for stage in self.stages.lock()
            .map_err(|_| selected_layout_rejection("selected native stage registry poisoned"))?.values()
        {
            stage.check().map_err(|error| selected_layout_rejection(error.to_string()))?;
        }
        Ok(())
    }

}

/// Durable shard-local freeze installed while a same-primary membership
/// handoff carries the retiring authority's exact state.
///
/// This bounded marker is local coordination state, not file metadata. A
/// pending marker rejects every authoritative mutation. After activation, only
/// the backend instance explicitly rebound to the marker's exact active epoch
/// may mutate; independently opened stale instances remain fenced. Read,
/// snapshot, prefix-proof, and replication-cursor operations remain usable.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(
    into = "DfsMetadataShardWriteFreezeWire",
    try_from = "DfsMetadataShardWriteFreezeWire"
)]
pub struct DfsMetadataShardWriteFreeze {
    pub handoff_id: String,
    pub retiring_epoch: u64,
    /// Active authority generation installed after the handoff commits.
    ///
    /// `None` is a pending write freeze. `Some(epoch)` remains durable after
    /// activation so already-open backend instances bound to the retiring
    /// generation cannot resume writes merely because another process
    /// completed the handoff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activated_epoch: Option<u64>,
    pub journal_sequence: u64,
    pub state_fingerprint: String,
    /// Exact durable descendant from which a freshly opened lane can resume
    /// validating the prefix after older journal segments are checkpointed.
    ///
    /// New markers start at the handoff prefix. Checkpoint publication
    /// advances this proof before pruning the journal history needed to
    /// reconstruct it. Current markers always carry the exact proof.
    pub rebind_prefix_proof: crate::DfsMetadataPrefixProof,
}

/// How a write-set member's journal relates to a prefix in a new epoch's
/// history (see `metadata_backend::alignment`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DfsMetadataReplicaAlignment {
    /// The member already acknowledged past the prefix at that epoch.
    Following { head: crate::DfsMetadataPrefixProof },
    /// The member now holds exactly the prefix; `dropped` records after it,
    /// never acknowledged at the new epoch, were removed.
    Aligned {
        head: crate::DfsMetadataPrefixProof,
        dropped: u64,
    },
    /// The member's journal ends before the prefix; the new primary checks
    /// that head against its own journal.
    Behind { head: crate::DfsMetadataPrefixProof },
}

impl DfsMetadataShardWriteFreeze {
    /// Whether a shard on the write-set protocol whose record is at
    /// `record_epoch` has moved past this marker, so it fences nothing.
    ///
    /// An activated marker below the record's epoch belongs to an earlier
    /// generation: authorship at the record's epoch is fenced by the
    /// publication provider, the provider-validated initial epoch binding and
    /// the bound epoch of each prepare. A pending marker whose handoff the
    /// record moved past and whose completion the record does not name
    /// (`completed_handoff_id`, from the handoff proof key) can never
    /// activate: a takeover deleted that handoff. A marker at the record's
    /// epoch, or one for a handoff that completed, is current and is kept.
    pub fn is_obsolete_for_write_set_record(
        &self,
        record_epoch: u64,
        completed_handoff_id: Option<&str>,
    ) -> bool {
        match self.activated_epoch {
            Some(activated_epoch) => activated_epoch < record_epoch,
            None => {
                self.retiring_epoch < record_epoch
                    && completed_handoff_id != Some(self.handoff_id.as_str())
            }
        }
    }
}

// The current producer emits the unwrapped cooperative marker. Historical
// repair-custody envelopes are decoded only by the migration owner.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum DfsMetadataShardWriteFreezeWire {
    // A local recovery fence envelope is never a cooperative marker. Its
    // recognition fails the read closed, even for a malformed or mixed payload
    // that another envelope would otherwise accept.
    LocalRecoveryV1 {
        selected_local_recovery_fence_v1: LocalRecoveryFenceTag,
    },
    SelectedOriginalDataV2 {
        selected_original_data_v2: DfsMetadataShardWriteFreezePayload,
    },
    Current(DfsMetadataShardWriteFreezePayload),
}

struct LocalRecoveryFenceTag;
impl<'de> serde::Deserialize<'de> for LocalRecoveryFenceTag {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let _ = <serde::de::IgnoredAny as serde::Deserialize>::deserialize(deserializer)?;
        Ok(Self)
    }
}
impl serde::Serialize for LocalRecoveryFenceTag {
    fn serialize<S: serde::Serializer>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("local recovery recognition cannot serialize a cooperative marker"))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DfsMetadataShardWriteFreezePayload {
    handoff_id: String,
    retiring_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    activated_epoch: Option<u64>,
    journal_sequence: u64,
    state_fingerprint: String,
    rebind_prefix_proof: crate::DfsMetadataPrefixProof,
}

impl From<DfsMetadataShardWriteFreeze> for DfsMetadataShardWriteFreezeWire {
    fn from(value: DfsMetadataShardWriteFreeze) -> Self {
        Self::Current(DfsMetadataShardWriteFreezePayload {
            handoff_id: value.handoff_id,
            retiring_epoch: value.retiring_epoch,
            activated_epoch: value.activated_epoch,
            journal_sequence: value.journal_sequence,
            state_fingerprint: value.state_fingerprint,
            rebind_prefix_proof: value.rebind_prefix_proof,
        })
    }
}

/// Explicit selected-owner codec. The ordinary Deserialize above deliberately
/// rejects this envelope; callers cannot select this reader from wire contents.
pub(crate) fn read_handoff_freeze_with_layout(
    path: &std::path::Path,
    layout: Option<&DfsSelectedOriginalDataLayout>,
) -> Result<Option<DfsMetadataShardWriteFreeze>> {
    let Some(layout) = layout else {
        reject_default_handoff_layout(path)?;
        return crate::metadata_io::read_sobj_optional(path);
    };
    check_handoff_layout_path(layout, path)?;
    let result: Option<DfsMetadataShardWriteFreeze> = crate::metadata_io::read_sobj_optional(path)
        .map_err(|error| selected_layout_rejection(error.to_string()))?;
    check_handoff_layout_path(layout, path)?;
    Ok(result)
}

pub(crate) fn write_handoff_freeze_with_layout(
    path: &std::path::Path,
    freeze: &DfsMetadataShardWriteFreeze,
    layout: Option<&DfsSelectedOriginalDataLayout>,
) -> Result<()> {
    if let Some(layout) = layout {
        check_handoff_layout_path(layout, path)?;
    } else {
        reject_default_handoff_layout(path)?;
    }
    crate::metadata_io::write_sobj_atomic(path, freeze)?;
    if let Some(layout) = layout { check_handoff_layout_path(layout, path)?; }
    Ok(())
}

pub(crate) fn validate_handoff_layout_root(layout: &DfsSelectedOriginalDataLayout, root: &std::path::Path) -> Result<()> {
    check_handoff_layout_path(layout, &root.join("journal/membership-handoff-write-freeze.sobj"))
}

fn reject_default_handoff_layout(path: &std::path::Path) -> Result<()> {
    let root = path.parent().and_then(std::path::Path::parent)
        .ok_or_else(|| selected_layout_rejection("handoff freeze has no shard root"))?;
    crate::selected_fixture::SelectedOriginalFixtureIdentity::reject_default_root(root)
        .map_err(|error| selected_layout_rejection(error.to_string()))
}

fn check_handoff_layout_path(layout: &DfsSelectedOriginalDataLayout, path: &std::path::Path) -> Result<()> {
    layout.check()?;
    let root = path.parent().and_then(std::path::Path::parent)
        .ok_or_else(|| selected_layout_rejection("selected freeze has no shard root"))?;
    if path != root.join("journal/membership-handoff-write-freeze.sobj") {
        return Err(selected_layout_rejection("selected freeze names another marker path"));
    }
    let stages = layout.stages.lock()
        .map_err(|_| selected_layout_rejection("selected native stage registry poisoned"))?;
    let stage = stages.values().find(|stage| stage.root() == root)
        .ok_or_else(|| selected_layout_rejection("selected freeze has no admitted physical stage"))?;
    stage.check().map_err(|error| selected_layout_rejection(error.to_string()))
}

impl TryFrom<DfsMetadataShardWriteFreezeWire> for DfsMetadataShardWriteFreeze {
    type Error = &'static str;
    fn try_from(wire: DfsMetadataShardWriteFreezeWire) -> std::result::Result<Self, Self::Error> {
        let payload = match wire {
            DfsMetadataShardWriteFreezeWire::LocalRecoveryV1 { .. } => {
                return Err("selected local recovery fence is not a cooperative handoff");
            }
            DfsMetadataShardWriteFreezeWire::SelectedOriginalDataV2 {
                selected_original_data_v2: payload,
            }
            | DfsMetadataShardWriteFreezeWire::Current(payload) => payload,
        };
        Ok(Self {
            handoff_id: payload.handoff_id,
            retiring_epoch: payload.retiring_epoch,
            activated_epoch: payload.activated_epoch,
            journal_sequence: payload.journal_sequence,
            state_fingerprint: payload.state_fingerprint,
            rebind_prefix_proof: payload.rebind_prefix_proof,
        })
    }
}

/// Capability for one replica ingest from an already validated active source.
///
/// The metadata replication boundary mints this only after matching the
/// request's filesystem, shard, source node, and authority epoch to the exact
/// committed shard assignment. Before taking its write lock, the durable
/// backend rechecks the filesystem on every journal payload or on the complete
/// canonical snapshot state. It then rechecks the shard and, when an author
/// fence exists, the activated epoch while holding its write lock. Keeping
/// construction crate-private prevents external backend callers from bypassing
/// that source-validation boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataReplicaIngestPermit {
    namespace_catalog: Option<crate::DfsMetadataNamespaceCatalog>,
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    source_node_id: NodeId,
    source_epoch: u64,
    pending_primary_handoff_id: Option<String>,
    /// Switch epoch of a write-set shard; `None` under the all-member rule.
    floor_epoch_scope: Option<u64>,
    /// The epoch start the receiver's own write-set record names for the
    /// source epoch, when that epoch began after the shard switched.
    epoch_start: Option<crate::DfsMetadataPrefixProof>,
}

/// Verify the immutable original preparation independently of the current cut.
/// A digest-only eviction or a revoked body cannot supply publication evidence.
pub(crate) fn validated_metadata_authority_operation_digest(
    record: &DfsPreparedOperationRecord,
    operation: &crate::DfsMetadataAuthorityOperation,
) -> Result<DfsPreparedOperationDigest> {
    record.validate()?;
    let proof = &record.digest;
    if !matches!(
        record.status,
        DfsPreparedOperationStatus::Prepared | DfsPreparedOperationStatus::Committed
    ) || !matches!(operation, crate::DfsMetadataAuthorityOperation::PreparedOperation {
            operation_id, file_system_id, shard_id, owner_epoch, prepare_sequence, payload_digest
        } if operation_id == &proof.operation_id && file_system_id == &proof.file_system_id
            && shard_id == &proof.shard_id && *owner_epoch == proof.owner_epoch
            && *prepare_sequence == proof.prepare_sequence && payload_digest == &proof.payload_digest)
    {
        return Err(MetadataBackendError::Serde(
            "authority operation differs from its complete live preparation".into(),
        ));
    }
    Ok(proof.clone())
}

/// Result of one replica journal apply plus any proof computed from entries
/// that became durable during that exact call.
#[doc(hidden)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsMetadataReplicaApplyReport {
    applied_entries: usize,
    namespace_authority_proof: Option<DfsPreparedOperationDigest>,
    durable_prefix_proof: Option<DfsMetadataPrefixProof>,
    materialization_pending: bool,
}

impl DfsMetadataReplicaApplyReport {
    pub(crate) fn with_durable_ingest(
        applied_entries: usize,
        namespace_authority_proof: Option<DfsPreparedOperationDigest>,
        durable_prefix_proof: DfsMetadataPrefixProof,
        materialization_pending: bool,
    ) -> Self {
        Self {
            applied_entries,
            namespace_authority_proof,
            durable_prefix_proof: Some(durable_prefix_proof),
            materialization_pending,
        }
    }

    pub(crate) fn applied_entries(&self) -> usize {
        self.applied_entries
    }

    pub(crate) fn durable_prefix_proof(&self) -> Option<&DfsMetadataPrefixProof> {
        self.durable_prefix_proof.as_ref()
    }

    pub(crate) fn materialization_pending(&self) -> bool {
        self.materialization_pending
    }

    pub(crate) fn namespace_authority_proof_for(
        &self,
        shard: &DfsMetadataShardConfig,
        operation: &crate::DfsMetadataAuthorityOperation,
    ) -> Option<&DfsPreparedOperationDigest> {
        self.namespace_authority_proof
            .as_ref()
            .filter(|proof| matches!(operation, crate::DfsMetadataAuthorityOperation::PreparedOperation {operation_id,file_system_id,shard_id,owner_epoch,prepare_sequence,payload_digest} if operation_id==&proof.operation_id && file_system_id==&proof.file_system_id && shard_id==&proof.shard_id && *owner_epoch==proof.owner_epoch && *prepare_sequence==proof.prepare_sequence && payload_digest==&proof.payload_digest && proof.file_system_id==shard.file_system_id && proof.shard_id.0==shard.shard_id && proof.owner_epoch==shard.epoch))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataHistoryEffectsPlan {
    pub report: DfsMetadataHistoryGcReport,
    pub effects_by_scope:
        BTreeMap<(DfsFileSystemId, DfsMetadataShardId), Vec<crate::DfsPreparedMetadataEffect>>,
}

/// One bounded durable view used to admit an idempotent namespace request.
///
/// Admission needs several related indexes. Reading each index through an
/// independently refreshed shard snapshot can decode the same checkpoint
/// repeatedly and, more importantly, can observe different durable
/// generations. Durable sharded backends return all evidence from one stable
/// table image per retained shard.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsNamespaceRequestAdmissionSnapshot {
    pub request: Option<DfsNamespaceRequestRecord>,
    pub receipt: Option<DfsNamespaceRequestReceipt>,
    pub prepared: Option<DfsPreparedOperationRecord>,
    pub history_expired: bool,
}

/// Applied outcomes and hidden reservations from one exact physical table view.
/// A read must not combine pre-commit absence with post-commit reservation
/// removal and then incorrectly certify that an operation was never applied.
#[derive(Clone, Debug)]
pub struct DfsWriteOperationAdmissionSnapshot {
    pub intent: Option<DfsWriteIntentRecord>,
    pub target_size_bytes: Option<u64>,
    pub prepared: Vec<DfsPreparedOperationRecord>,
    pub retention: DfsWriteIntentReceiptRetentionState,
}

impl DfsMetadataReplicaIngestPermit {
    /// Mint evidence only after the caller has validated all four values
    /// against the same exact committed shard assignment.
    pub(crate) fn new_validated(
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
        source_node_id: NodeId,
        source_epoch: u64,
    ) -> Self {
        Self {
            file_system_id,
            shard_id,
            source_node_id,
            source_epoch,
            namespace_catalog: None,
            pending_primary_handoff_id: None,
            floor_epoch_scope: None,
            epoch_start: None,
        }
    }

    /// Mint an exact, proof-bound repair permit for the promoted replica of a
    /// still-pending primary handoff. The caller must validate the committed
    /// retiring assignment, durable fence, successor identity, and snapshot
    /// checkpoint before constructing this capability.
    pub(crate) fn new_validated_pending_primary_handoff(
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
        source_node_id: NodeId,
        source_epoch: u64,
        handoff_id: String,
    ) -> Self {
        Self {
            file_system_id,
            shard_id,
            source_node_id,
            source_epoch,
            namespace_catalog: None,
            pending_primary_handoff_id: Some(handoff_id),
            floor_epoch_scope: None,
            epoch_start: None,
        }
    }

    /// Scope acknowledgement floors to epochs for a write-set shard. The
    /// value is the switch epoch from the same validated shard record.
    pub(crate) fn with_namespace_catalog(mut self, catalog: crate::DfsMetadataNamespaceCatalog) -> Self {
        self.namespace_catalog = Some(catalog);
        self
    }
    pub(crate) fn validate_counted_snapshot(&self, tables: &crate::metadata_tables::DfsMetadataTables, generation: Option<&crate::DfsMetadataGenerationIdentity>) -> Result<()> {
        let catalog = self.namespace_catalog.as_ref().filter(|catalog|
            catalog.matches_ingress_scope(&self.file_system_id, &self.shard_id, &self.source_node_id, self.source_epoch))
            .ok_or_else(|| MetadataBackendError::Serde(
                "snapshot admission lacks its exact captured complete namespace catalog".into(),
            ))?;
        let generation = generation.ok_or_else(|| MetadataBackendError::Serde("current snapshot lacks mandatory generation identity".into()))?;
        if generation.shard_id != self.shard_id {
            return Err(MetadataBackendError::Serde("current snapshot generation belongs to another physical shard".into()));
        }
        catalog.validate_generation(generation)?;
        crate::storage_generation::validate_native_genesis_snapshot(generation, tables)?;
        catalog.validate_snapshot(&self.shard_id, tables)
    }
    pub(crate) fn snapshot_generation_envelope(&self, identity: &crate::DfsMetadataGenerationIdentity, tables: &crate::metadata_tables::DfsMetadataTables) -> Result<crate::storage_generation::GenerationEnvelope> {
        self.validate_counted_snapshot(tables, Some(identity))?;
        let catalog = self.namespace_catalog.as_ref().expect("validated complete catalog");
        Ok(crate::storage_generation::GenerationEnvelope {
            format_version: crate::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            identity: identity.clone(),
            state: crate::storage_generation::GenerationState::Active {
                activation_digest: catalog.generation_activation_digest(identity, tables)?,
                standalone_seal: None,
            },
        })
    }

    pub(crate) fn with_floor_epoch_scope(mut self, scope: Option<u64>) -> Self {
        self.floor_epoch_scope = scope;
        self
    }

    pub(crate) fn floor_epoch_scope(&self) -> Option<u64> {
        self.floor_epoch_scope
    }

    /// Bind the epoch start of the receiver's own record, so the epoch's
    /// first batch aligns the receiver's journal before it is ingested.
    pub(crate) fn with_epoch_start(mut self, epoch_start: Option<crate::DfsMetadataPrefixProof>) -> Self {
        self.epoch_start = epoch_start;
        self
    }

    pub(crate) fn epoch_start(&self) -> Option<&crate::DfsMetadataPrefixProof> {
        self.epoch_start.as_ref()
    }

    pub(crate) fn snapshot_replaces_imported_prefix(
        &self, original: &DfsMetadataPrefixProof, snapshot: &crate::DfsMetadataSnapshotImageDescriptor,
    ) -> bool {
        snapshot.generation.shard_id == self.shard_id
            && self.namespace_catalog.as_ref().is_some_and(|catalog|
                catalog.matches_ingress_scope(&self.file_system_id, &self.shard_id,
                    &self.source_node_id, self.source_epoch)
                    && catalog.snapshot_replaces_imported_prefix(original, snapshot))
    }

    /// Epoch to record in a floor acknowledged under this permit: the source
    /// epoch on a write-set shard, `None` under the all-member rule.
    pub(crate) fn floor_authority_epoch(&self) -> Option<u64> {
        self.floor_epoch_scope.map(|_| self.source_epoch)
    }

    /// Keep `floor` when it binds this ingest, drop it when it belongs to an
    /// earlier epoch, and refuse the ingest when it belongs to a later one.
    pub(crate) fn binding_ack_floor<T>(
        &self,
        floor: Option<T>,
        floor_epoch: impl Fn(&T) -> Option<u64>,
    ) -> Result<Option<T>> {
        let Some(floor) = floor else {
            return Ok(None);
        };
        match crate::metadata_records::dfs_metadata_ack_floor_relation(
            self.floor_epoch_scope,
            floor_epoch(&floor),
            self.source_epoch,
        ) {
            crate::metadata_records::DfsMetadataAckFloorRelation::Binds => Ok(Some(floor)),
            crate::metadata_records::DfsMetadataAckFloorRelation::EarlierEpoch => Ok(None),
            crate::metadata_records::DfsMetadataAckFloorRelation::LaterEpoch => {
                Err(MetadataBackendError::SourceEpochFenced {
                    shard_id: self.shard_id.clone(),
                    source_epoch: self.source_epoch,
                    fenced_epoch: floor_epoch(&floor).or(self.floor_epoch_scope).unwrap_or_default(),
                })
            }
        }
    }

    pub(crate) fn file_system_id(&self) -> &DfsFileSystemId {
        &self.file_system_id
    }

    pub(crate) fn shard_id(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }

    pub(crate) fn source_node_id(&self) -> &NodeId {
        &self.source_node_id
    }

    pub(crate) fn source_epoch(&self) -> u64 {
        self.source_epoch
    }

    pub(crate) fn pending_primary_handoff_id(&self) -> Option<&str> {
        self.pending_primary_handoff_id.as_deref()
    }

    pub(crate) fn admits_write_freeze(&self, freeze: &DfsMetadataShardWriteFreeze) -> bool {
        freeze
            .activated_epoch
            .is_some_and(|activated_epoch| self.source_epoch >= activated_epoch)
            || (freeze.activated_epoch.is_none()
                && self.pending_primary_handoff_id() == Some(freeze.handoff_id.as_str())
                && self.source_epoch == freeze.retiring_epoch)
    }
}

fn write_set_member_state_unsupported() -> MetadataBackendError {
    MetadataBackendError::Serde("metadata backend has no write-set member state".into())
}

/// One object's slice of the global `(object, version)` scan, read through
/// the object-local version index. Scan cursors keep their global meaning, so
/// a cursor for an earlier object restarts this object and a later one ends it.
fn object_version_records_page<B: DfsMetadataBackend + ?Sized>(
    backend: &B,
    object_id: &DfsObjectId,
    after: Option<&DfsObjectVersionScanCursor>,
    max_records: usize,
) -> Result<DfsObjectVersionScanPage> {
    let after = match after {
        Some(after) if &after.object_id == object_id => Some(DfsObjectVersionPageCursor {
            version: after.version.clone(),
            shard_id: None,
        }),
        Some(after) if &after.object_id > object_id => {
            return Ok(DfsObjectVersionScanPage {
                versions: Vec::new(),
                next_cursor: None,
            });
        }
        _ => None,
    };
    let page = backend.versions_for_object_page(object_id, after.as_ref(), max_records)?;
    Ok(DfsObjectVersionScanPage {
        versions: page.versions,
        next_cursor: page.next_cursor.map(|cursor| DfsObjectVersionScanCursor {
            object_id: object_id.clone(),
            version: cursor.version,
        }),
    })
}

pub(crate) fn namespace_prepare_expectations_from_lookups(
    transaction: &DfsNamespaceTransaction,
    mut object_lookup: impl FnMut(&DfsObjectId) -> Result<Option<DfsObjectMetadata>>,
    mut path_lookup: impl FnMut(&DfsFileSystemId, &str) -> Result<Option<DfsObjectMetadata>>,
) -> Result<(
    Vec<DfsNamespaceObjectVersionExpectation>,
    Vec<DfsNamespacePathOccupantExpectation>,
    BTreeSet<String>,
)> {
    let mut object_ids = transaction
        .objects
        .iter()
        .map(|object| object.object_id.clone())
        .chain(
            transaction
                .versions
                .iter()
                .map(|version| version.object_id.clone()),
        )
        .chain(
            transaction
                .content_reference_identity
                .iter()
                .map(|identity| identity.object_id.clone()),
        )
        .chain(
            transaction
                .advisory_lock_states
                .iter()
                .map(|state| state.object_id.clone()),
        )
        .collect::<BTreeSet<_>>();
    let mut paths = transaction
        .objects
        .iter()
        .map(|object| object.path.clone())
        .collect::<BTreeSet<_>>();
    for object_id in &object_ids {
        if let Some(object) = object_lookup(object_id)? {
            paths.insert(object.path);
        }
    }
    let mut expected_path_occupants = Vec::with_capacity(paths.len());
    for path in &paths {
        let occupant = path_lookup(&transaction.file_system_id, path)?;
        if let Some(occupant) = occupant.as_ref() {
            object_ids.insert(occupant.object_id.clone());
        }
        expected_path_occupants.push(DfsNamespacePathOccupantExpectation {
            path: path.clone(),
            expected_object_id: occupant.map(|object| object.object_id),
        });
    }
    let mut expected_object_versions = Vec::with_capacity(object_ids.len());
    for object_id in object_ids {
        let current = object_lookup(&object_id)?;
        expected_object_versions.push(DfsNamespaceObjectVersionExpectation {
            object_id,
            expected_version: current.map(|object| object.version),
        });
    }
    Ok((expected_object_versions, expected_path_occupants, paths))
}

/// Durable DFS metadata store interface.
///
/// Implementations own authoritative filesystem/object metadata, object
/// versions, write intents, replica receipts, conflict records, namespace
/// transactions, and metadata journals. The trait intentionally exposes paged
/// scans as primitives so service and operator code can walk large namespaces
/// without materializing every record class at once.
#[derive(Clone, Debug)]
pub struct DfsCrossShardPinTransferPlan {
    pub effects: Vec<crate::DfsPreparedMetadataEffect>,
    pub active: bool,
}
#[derive(Clone, Debug)]
pub struct DfsForeignShardSourceEvictionPlan {
    pub physical_source_shard: DfsMetadataShardId,
    pub expected_objects: Vec<DfsNamespaceObjectVersionExpectation>,
    pub expected_paths: Vec<DfsNamespacePathOccupantExpectation>,
    pub expected_journal_sequence: u64,
    pub effects: Vec<crate::DfsPreparedMetadataEffect>,
}

/// One page of a shard's derived repair index: each current repair need with
/// what the node's repair executor dispatches.
#[doc(hidden)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsRepairIndexPage {
    pub(crate) needs: Vec<(
        crate::metadata_tables::DfsRepairNeedKey,
        crate::metadata_tables::DfsRepairNeed,
    )>,
}

pub trait DfsMetadataBackend: std::fmt::Debug + Send {
    /// Complete current representation admission only after the original
    /// native genesis committed. This creates no publication permission.
    fn activate_native_genesis_storage_generation(&mut self, _shard: &crate::DfsMetadataShardConfig) -> Result<()> {
        Err(MetadataBackendError::Serde("backend does not own native genesis generation activation".into()))
    }
    #[cfg(test)]
    fn retain_native_prefix_history(
        &mut self, _round:&crate::node_transport::OwnedNativeMetadataPrefixRound,
        _provider:&crate::publication_guard::DfsPublicationExecutionProvider,
        _execution:&crate::publication_guard::DfsPublicationExecutionOwners,
        _local:&NodeId, _deadline:std::time::Instant,
    )->Result<crate::metadata_backend::native_prefix_history::NativePrefixHistoryIntake> {
        Err(MetadataBackendError::Serde("backend does not own native prefix historical intake".into()))
    }
    #[cfg(test)]
    fn read_native_prefix_history(&self,_shard:&crate::DfsMetadataShardId)
        ->Result<Option<crate::metadata_backend::native_prefix_history::NativePrefixHistoryRecord>> {
        Err(MetadataBackendError::Serde("backend does not own native prefix historical DATA".into()))
    }

    /// Representation admission of this exact handle, never mutation or
    /// handoff authorization. Ordinary backends retain their default reader.
    fn selected_original_data_layout(
        &self,
        _file_system_id: &DfsFileSystemId,
        _shard_id: &crate::DfsMetadataShardId,
    ) -> Result<Option<std::sync::Arc<DfsSelectedOriginalDataLayout>>> {
        Ok(None)
    }
    fn plan_cross_shard_pin_transfer(
        &self,
        transfer_id: &str,
        moved_object: &DfsObjectMetadata,
        source_path: &str,
        incoming_versions: &[DfsObjectVersionRecord],
        incoming_pins: &[DfsMetadataVersionPinRecord],
    ) -> Result<DfsCrossShardPinTransferPlan>;
    fn plan_foreign_shard_source_eviction(
        &self,
        moved_object: &DfsObjectMetadata,
        old_path: &str,
        captured_source_family: &DfsCrossShardSourceFamily,
        transfer_id: Option<&str>,
        target_shard_id: &DfsMetadataShardId,
    ) -> Result<DfsForeignShardSourceEvictionPlan>;
    /// Bounded lookup in the existing operation-ID index; no journal recovery.
    fn prepared_operation_family_page(
        &self, shard_id: &DfsMetadataShardId, prefix: &str, after: Option<&str>, max_entries: usize,
    ) -> Result<(Vec<crate::DfsPreparedOperationRecord>, Option<String>)> {
        let _ = (shard_id, prefix, after, max_entries);
        Err(MetadataBackendError::Serde("backend has no bounded prepared-family index".into()))
    }
    /// Observe the immutable record only at the supplied exact current cut.
    /// None means the cut cannot be corroborated; Some(None) is proven absence.
    fn prepared_operation_record_at_prefix(
        &self, shard_id: &DfsMetadataShardId, prefix: &DfsMetadataPrefixProof, operation_id: &str,
    ) -> Result<Option<Option<crate::DfsPreparedOperationRecord>>> {
        let _ = (shard_id, prefix, operation_id);
        Ok(None)
    }

    fn prepared_operation_record(
        &self,
        operation_id: &str,
    ) -> Result<Option<crate::DfsPreparedOperationRecord>>;
    /// Observe the filesystem and original operation from one physical shard.
    /// These are read inputs; publication still requires the native writer guard.
    fn prepared_operation_read_context(
        &self,
        shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
        operation_id: &str,
    ) -> Result<(
        Option<DfsFileSystem>,
        Option<crate::DfsPreparedOperationRecord>,
    )> {
        Ok((
            self.get_file_system_on_shard(shard_id, file_system_id)?,
            self.prepared_operation_record(operation_id)?,
        ))
    }
    fn list_prepared_operations(&self) -> Result<Vec<crate::DfsPreparedOperationRecord>>;
    /// Read only the prepared bodies reserving this operation's footprint.
    /// This grants no authority to resolve them; the native author must use
    /// the existing guarded recovery path with the original body/readset.
    fn prepared_operation_reservations(
        &self,
        payload: &crate::DfsPreparedOperationPayload,
    ) -> Result<Vec<crate::DfsPreparedOperationRecord>> {
        let (objects, paths) = crate::metadata_tables::prepared_payload_reservation_keys(payload);
        Ok(self
            .list_prepared_operations()?
            .into_iter()
            .filter(|record| {
                if record.status != crate::DfsPreparedOperationStatus::Prepared
                    || record.file_system_id != payload.file_system_id
                    || record.shard_id != payload.shard_id
                    || record.operation_id == payload.operation_id
                {
                    return false;
                }
                let footprint = crate::metadata_tables::namespace_prepare_footprint(record);
                !objects.is_disjoint(footprint.object_ids())
                    || !paths.is_disjoint(footprint.normalized_paths())
            })
            .collect())
    }

    fn prepared_operation_for_request(
        &self,
        requester_node_id: &sinomo_core::node::NodeId,
        request_id: &str,
    ) -> Result<Option<crate::DfsPreparedOperationRecord>>;

    /// Shared observation-only view of already-held native storage. This performs
    /// no IO, opens no backend and conveys no writer or publication authority.
    #[cfg(any(test, feature = "test-support"))]
    fn native_held_snapshot_for_scope(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
    ) -> Option<crate::NativeHeldMetadataSnapshotV1>;

    /// Persist one complete immutable v7 operation. Publication effects remain
    /// hidden until its exact digest is committed with an owned writer guard.
    fn prepare_operation(
        &mut self,
        _payload: crate::metadata_records::DfsPreparedOperationPayload,
        _source: Option<&crate::source_adoption::DfsSourceAdoptionWitness>,
    ) -> Result<crate::metadata_records::DfsPreparedOperationDigest>;

    fn prepared_operation_digest(
        &self,
        _operation_id: &str,
    ) -> Result<Option<crate::metadata_records::DfsPreparedOperationDigest>>;

    fn prepared_operation_payload(
        &self,
        _operation_id: &str,
    ) -> Result<Option<std::sync::Arc<crate::metadata_records::DfsPreparedOperationPayload>>>;

    /// Resolve immutable content bound to one exact durable prepare identity.
    /// A resident body may outlive its current record or authority generation:
    /// returning it proves neither current status nor permission to publish.
    /// Callers needing those facts must use the ordinary current-state APIs.
    fn prepared_operation_payload_for_digest(
        &self,
        digest: &DfsPreparedOperationDigest,
    ) -> Result<Option<std::sync::Arc<DfsPreparedOperationPayload>>> {
        let Some(record) = self.prepared_operation_record(&digest.operation_id)? else {
            return Ok(None);
        };
        record.validate()?;
        if record.digest != *digest {
            return Err(MetadataBackendError::Serde(
                "immutable prepared body differs from the requested durable identity".into(),
            ));
        }
        Ok(Some(record.payload))
    }

    /// Read-only exact terminal evidence. No acknowledgement round or new
    /// publication marker is required for an already materialized commit.
    fn committed_prepared_operation(
        &self,
        _digest: &crate::metadata_records::DfsPreparedOperationDigest,
    ) -> Result<Option<DfsMetadataJournalEntry>>;

    /// Physical owner identity is an index key, never authority.
    fn publication_owner_scope(
        &self,
        _file_system_id: &DfsFileSystemId,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<crate::publication_guard::DfsPublicationOwnerScope>;

    /// Acquire and retain the actual writer lock before final authority reads.
    fn publication_writer_session(
        &mut self,
        _file_system_id: &DfsFileSystemId,
        _shard_id: &DfsMetadataShardId,
        _digest: &crate::metadata_records::DfsPreparedOperationDigest,
        _source: Option<&crate::DfsSourceAdoptionWitness>,
        provider: Option<&crate::publication_guard::DfsPublicationExecutionProvider>,
        execution: Option<&crate::publication_guard::DfsPublicationExecutionOwners>,
        clustered: Option<&crate::DfsMetadataShardConfig>,
    ) -> Result<crate::publication_guard::DfsPublicationWriterSession>;

    fn publication_writer_file_system(
        &self,
        _file_system_id: &DfsFileSystemId,
        _session: &crate::publication_guard::DfsPublicationWriterSession,
    ) -> Result<Option<DfsFileSystem>>;

    fn publication_writer_object(
        &self,
        _file_system_id: &DfsFileSystemId,
        _object_id: &DfsObjectId,
        _session: &crate::publication_guard::DfsPublicationWriterSession,
    ) -> Result<Option<DfsObjectMetadata>>;

    /// Consume actual authorization and publish only the previously sealed body.
    fn commit_prepared_operation(
        &mut self,
        _digest: &crate::metadata_records::DfsPreparedOperationDigest,
        _guard: crate::publication_guard::DfsPublicationGuard,
    ) -> Result<crate::metadata_records::DfsPreparedOperationCommitResult>;

    /// Pin one coherent in-memory metadata generation for a bounded bulk read.
    ///
    /// Full-catalog hydration calls many paging and point-read methods through
    /// this trait. Durable local backends may refresh once at the start of the
    /// scope and then serve those reads from that generation instead of
    /// polling the same snapshot, log, and checkpoint files for every record.
    /// Implementations must resume ordinary freshness checks after the
    /// matching end call.
    fn begin_consistent_read_snapshot(&mut self) -> Result<()> {
        Ok(())
    }

    /// End one bounded coherent-read scope opened above.
    fn end_consistent_read_snapshot(&mut self) {}

    /// Pin exactly one physical authority scope. Unlike a path lookup, this
    /// must not refresh ancestor fallbacks or unrelated shard backlogs.
    fn has_physical_read_scope(&self, _shard_id: &DfsMetadataShardId) -> bool {
        false
    }

    fn begin_consistent_read_snapshot_on_shard(
        &mut self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        Err(MetadataBackendError::Serde(
            "metadata backend does not support exact-shard read snapshots".into(),
        ))
    }

    fn end_consistent_read_snapshot_on_shard(&mut self, _shard_id: &DfsMetadataShardId) {}

    /// Pin an existing materialized read view without acquiring a writer or
    /// running journal recovery. Unsupported backends remain unavailable.
    fn begin_stat_read_snapshot(&mut self) -> Result<()> {
        Err(MetadataBackendError::Serde(
            "backend has no stat read-view capability".into(),
        ))
    }
    fn end_stat_read_snapshot(&mut self) {}
    fn begin_stat_read_snapshot_on_shard(&mut self, _id: &DfsMetadataShardId) -> Result<()> {
        Err(MetadataBackendError::Serde(
            "backend has no exact stat read-view capability".into(),
        ))
    }
    fn end_stat_read_snapshot_on_shard(&mut self, _id: &DfsMetadataShardId) {}
    fn begin_stat_read_snapshot_on_shards(&mut self, ids: &[DfsMetadataShardId]) -> Result<()> {
        for (index, id) in ids.iter().enumerate() {
            if let Err(error) = self.begin_stat_read_snapshot_on_shard(id) {
                for pinned in ids[..index].iter().rev() {
                    self.end_stat_read_snapshot_on_shard(pinned);
                }
                return Err(error);
            }
        }
        Ok(())
    }
    fn end_stat_read_snapshot_on_shards(&mut self, ids: &[DfsMetadataShardId]) {
        for id in ids.iter().rev() {
            self.end_stat_read_snapshot_on_shard(id);
        }
    }
    fn begin_stat_read_snapshot_for_path(
        &mut self,
        _fs: &DfsFileSystemId,
        _path: &str,
    ) -> Result<()> {
        self.begin_stat_read_snapshot()
    }
    fn end_stat_read_snapshot_for_path(&mut self, _fs: &DfsFileSystemId, _path: &str) {
        self.end_stat_read_snapshot();
    }

    /// Pin the metadata generation needed to serve one exact namespace path.
    ///
    /// The default is deliberately conservative for unsharded and third-party
    /// backends. A sharded router can override this to refresh only the active
    /// path shard and any retained routing fallbacks instead of reopening every
    /// unrelated shard for a point lookup.
    fn begin_consistent_read_snapshot_for_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<()> {
        let _ = (file_system_id, path);
        self.begin_consistent_read_snapshot()
    }

    /// End a path-scoped coherent-read scope opened above.
    fn end_consistent_read_snapshot_for_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) {
        let _ = (file_system_id, path);
        self.end_consistent_read_snapshot();
    }

    /// Fetch a filesystem record by id.
    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>>;
    fn get_file_system_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsFileSystem>>;

    /// List all filesystem records known to this backend.
    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>>;

    /// Fetch an object record by object id.
    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>>;

    /// Fetch an object record from one exact metadata shard.
    ///
    /// Namespace prepares use this point read after selecting their immutable
    /// request-scoped shard. A sharded router must not resolve the object
    /// through its global object index here: during a cross-shard adoption the
    /// same object id is intentionally still visible on the source shard, but
    /// destination preconditions describe only destination-shard state.
    fn get_object_on_shard(
        &self,
        _shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>>;

    /// Fetch the durable whole-file advisory-lock aggregate for an object.
    fn get_advisory_lock_state(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsAdvisoryLockStateRecord>>;

    /// Fetch the visible object at a normalized filesystem path.
    fn get_object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>>;

    /// Fetch the visible object at a path from one exact metadata shard.
    ///
    /// Single-shard backends delegate to their normal lookup. Sharded routers
    /// override this so prepare expectations cannot accidentally span shard
    /// authorities while a namespace saga is moving an object.
    fn get_object_by_path_on_shard(
        &self,
        _shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>>;

    /// Capture every object-version and path-occupant precondition for one
    /// namespace prepare on an exact shard.
    ///
    /// Object paths, path occupants and occupant versions must observe one
    /// immutable table generation. Prepare and commit revalidate these exact
    /// predicates under the publication writer lock.
    fn namespace_prepare_expectations(
        &self,
        shard_id: &DfsMetadataShardId,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<(
        Vec<DfsNamespaceObjectVersionExpectation>,
        Vec<DfsNamespacePathOccupantExpectation>,
        BTreeSet<String>,
    )>;

    /// Compatibility helper that drains all object pages for a filesystem.
    fn objects_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        collect_metadata_pages(|cursor| {
            let page = self.objects_for_file_system_page(
                file_system_id,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.objects, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of objects for a filesystem.
    fn objects_for_file_system_page(
        &self,
        file_system_id: &DfsFileSystemId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage>;

    /// Return the subtree root and descendants using a path-index range.
    /// The backend must retain its ordinary shard-aware continuation contract.
    fn objects_for_subtree_page(
        &self,
        file_system_id: &DfsFileSystemId,
        subtree_path: &str,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage>;

    /// List durable direct-child tombstones below a directory. Backends with a
    /// path index should override this so targeted namespace synchronization
    /// does not scan the entire filesystem.
    fn child_tombstones_for_directory(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let directory_path = normalize_posix_path(directory_path);
        let prefix = if directory_path == "/" {
            "/".to_string()
        } else {
            format!("{}/", directory_path.trim_end_matches('/'))
        };
        collect_filtered_metadata_pages(
            |cursor| {
                let page = self.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |object| {
                let object_path = normalize_posix_path(&object.path);
                let rest = object_path.strip_prefix(&prefix)?;
                (object.kind == DfsObjectKind::Tombstone && !rest.is_empty() && !rest.contains('/'))
                    .then_some(object)
            },
        )
    }

    /// Count visible objects in a filesystem without requiring callers to
    /// materialize every record.
    fn count_objects_for_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<usize> {
        const COUNT_PAGE_SIZE: usize = 4096;
        let mut count = 0usize;
        walk_metadata_pages(|cursor| {
            let page =
                self.objects_for_file_system_page(file_system_id, cursor, COUNT_PAGE_SIZE)?;
            count = count.saturating_add(page.objects.len());
            Ok(page.next_cursor)
        })?;
        Ok(count)
    }

    /// Build the page cursor that resumes after `object`.
    fn object_page_cursor_for_object(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsObjectPageCursor> {
        Ok(DfsObjectPageCursor {
            path: object.path.clone(),
            object_id: object.object_id.clone(),
            shard_id: None,
        })
    }

    /// Build the directory-order cursor for an object that is visible through
    /// an in-memory overlay. Indexed backends override this so callers can
    /// merge a bounded transient set into durable pages without abandoning the
    /// backend's directory index.
    fn directory_page_cursor_for_object(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsDirectoryPageCursor>;

    /// Return one indexed directory page together with each entry's exact
    /// resume cursor. Overlay-aware callers use these cursors directly instead
    /// of re-reading backend state once per durable object.
    /// Entries follow ascending cursor order: shard, dentry fragment/hash/name,
    /// then path and object id. A resumed page must continue strictly after its
    /// indexed cursor, so concatenating pages preserves this order.
    fn child_objects_for_directory_page_entries(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<(
        Vec<(DfsObjectMetadata, DfsDirectoryPageCursor)>,
        Option<DfsDirectoryPageCursor>,
    )>;

    /// Compatibility helper that lists direct children below a directory path.
    fn child_objects_for_directory(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let directory_path = normalize_posix_path(directory_path);
        let prefix = if directory_path == "/" {
            "/".to_string()
        } else {
            format!("{}/", directory_path.trim_end_matches('/'))
        };
        let mut children = collect_filtered_metadata_pages(
            |cursor| {
                let page = self.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |object| {
                let object_path = normalize_posix_path(&object.path);
                if object.kind == DfsObjectKind::Tombstone
                    || object_path == directory_path
                    || !object_path.starts_with(&prefix)
                {
                    return None;
                }
                let rest = object_path.trim_start_matches(&prefix);
                (!rest.is_empty() && !rest.contains('/')).then_some(object)
            },
        )?;
        children.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(children)
    }

    /// Return one cursor-bounded page of direct directory children.
    fn child_objects_for_directory_page(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        let directory_path = normalize_posix_path(directory_path);
        let prefix = if directory_path == "/" {
            "/".to_string()
        } else {
            format!("{}/", directory_path.trim_end_matches('/'))
        };
        let object_cursor = after.map(|cursor| DfsObjectPageCursor {
            path: cursor.path.clone(),
            object_id: cursor.object_id.clone(),
            shard_id: cursor.shard_id.clone(),
        });
        let (objects, next_cursor) = collect_filtered_cursor_page(
            object_cursor,
            max_entries,
            |cursor| {
                let page = self.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |object| {
                let object_path = normalize_posix_path(&object.path);
                if object.kind == DfsObjectKind::Tombstone
                    || object_path == directory_path
                    || !object_path.starts_with(&prefix)
                {
                    return Ok(None);
                }
                let rest = object_path.trim_start_matches(&prefix);
                if rest.is_empty() || rest.contains('/') {
                    return Ok(None);
                }
                let object_cursor = self.object_page_cursor_for_object(&object)?;
                let cursor = DfsDirectoryPageCursor {
                    path: object_cursor.path,
                    object_id: object_cursor.object_id,
                    dentry: None,
                    shard_id: object_cursor.shard_id,
                };
                Ok(Some((object, cursor)))
            },
        )?;
        Ok(DfsDirectoryPage {
            objects,
            next_cursor,
        })
    }

    /// Compatibility helper that lists all hardlink records targeting an object.
    fn hardlinks_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut hardlinks = collect_filtered_metadata_pages(
            |cursor| {
                let page = self.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |object| {
                object_is_hardlink_to_target(&object, file_system_id, target_object_id)
                    .then_some(object)
            },
        )?;
        sort_hardlinks_by_path(&mut hardlinks);
        Ok(hardlinks)
    }

    /// Return one cursor-bounded page of hardlink records for a target object.
    fn hardlinks_to_object_page(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        let (hardlinks, next_cursor) = collect_filtered_cursor_page(
            after.cloned(),
            max_entries,
            |cursor| {
                let page = self.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |object| {
                if !object_is_hardlink_to_target(&object, file_system_id, target_object_id) {
                    return Ok(None);
                }
                let cursor = self.object_page_cursor_for_object(&object)?;
                Ok(Some((object, cursor)))
            },
        )?;
        Ok(DfsObjectPage {
            objects: hardlinks,
            next_cursor,
        })
    }

    /// Count hardlink records targeting an object.
    fn hardlink_count_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<usize> {
        let mut count = 0usize;
        walk_metadata_pages(|cursor| {
            let page =
                self.hardlinks_to_object_page(file_system_id, target_object_id, cursor, 1024)?;
            count = count.saturating_add(page.objects.len());
            Ok(page.next_cursor)
        })?;
        Ok(count)
    }

    /// Return the first hardlink record targeting an object, if one exists.
    fn first_hardlink_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        Ok(self
            .hardlinks_to_object_page(file_system_id, target_object_id, None, 1)?
            .objects
            .into_iter()
            .next())
    }

    /// Return the first hardlink target outside `prefix`, used by namespace
    /// cleanup to avoid removing still-referenced content.
    fn first_hardlink_to_object_outside_prefix(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let prefix = normalize_posix_path(prefix);
        let mut found = None;
        walk_metadata_pages(|cursor| {
            let page =
                self.hardlinks_to_object_page(file_system_id, target_object_id, cursor, 1024)?;
            for object in page.objects {
                if !posix_path_is_at_or_under_prefix(&object.path, &prefix) {
                    found = Some(object);
                    return Ok(None);
                }
            }
            Ok(page.next_cursor)
        })?;
        Ok(found)
    }

    /// List all durable version pins attached to one object. The object-family
    /// migration path uses this to preserve checkpoint roots with authority.
    ///
    /// Backends that do not implement durable version pins must fail closed:
    /// silently returning an empty set could let migration or GC discard a
    /// version still protected by an implementation-specific checkpoint.
    fn version_pins_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsMetadataVersionPinRecord>> {
        let _ = object_id;
        Err(MetadataBackendError::Serde(
            "metadata backend does not support durable version pin scans".to_string(),
        ))
    }

    /// Return one bounded page of durable pins in `(version, pin_id)` order.
    ///
    /// Migration code must use this page API instead of first materializing
    /// the complete pin set so its per-pass record budget remains meaningful.
    fn version_pins_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsMetadataVersionPinPageCursor>,
        max_entries: usize,
    ) -> Result<DfsMetadataVersionPinPage> {
        let _ = (object_id, after, max_entries);
        Err(MetadataBackendError::Serde(
            "metadata backend does not support bounded durable version pin scans".to_string(),
        ))
    }

    /// Inspect current prepared-reference owners through the object index.
    /// The second result is false when the bounded scan is incomplete.
    fn prepared_content_reference_protection(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        max_prepares: usize,
    ) -> Result<(bool, bool)>;

    /// Return active target-side pin-transfer provenance for one object.
    fn cross_shard_pin_transfers_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsCrossShardPinTransferRecord>> {
        let _ = object_id;
        Err(MetadataBackendError::Serde(
            "metadata backend does not support cross-shard pin-transfer scans".to_string(),
        ))
    }

    /// Capture the exact source-authority pin set and its journal head under
    /// one shard writer lock. The destination payload and later source
    /// eviction fence must use this same capture.
    fn capture_cross_shard_source_pin_fence(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsCrossShardVersionPinFence> {
        let _ = object_id;
        Err(MetadataBackendError::Serde(
            "metadata backend does not support cross-shard source pin fences".to_string(),
        ))
    }

    /// Capture the complete physical source family from the shard selected by
    /// `source_path`, without following an object authority index that may
    /// already point at the destination after a crash.
    fn capture_cross_shard_source_family(
        &mut self,
        object_id: &DfsObjectId,
        source_path: &str,
    ) -> Result<DfsCrossShardSourceFamily> {
        let _ = (object_id, source_path);
        Err(MetadataBackendError::Serde(
            "metadata backend does not support path-anchored cross-shard source capture"
                .to_string(),
        ))
    }

    /// Capture only the actual source family under its physical writer and
    /// handoff fences. The Actor must separately verify current source authority.
    fn capture_source_adoption_source(
        &mut self,
        object_id: &DfsObjectId,
        source_path: &str,
    ) -> Result<crate::metadata_backend::DfsSourceAdoptionSourceCapture>;

    /// Capture the actual original Running job under its physical writer and
    /// handoff fences. Remote job rows or source families cannot create custody.
    fn capture_source_adoption_job(
        &mut self,
        job_id: &str,
    ) -> Result<crate::metadata_backend::DfsSourceAdoptionJobCapture>;

    /// Capture the physical source family while retaining its existing writer
    /// and handoff fences. This is owned source custody, not author permission.
    fn capture_source_adoption(
        &mut self,
        object_id: &DfsObjectId,
        source_path: &str,
        job_id: &str,
    ) -> Result<crate::metadata_backend::DfsSourceAdoptionCapture>;

    /// Fetch one object-version record by object id and version id.
    fn get_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Option<DfsObjectVersionRecord>>;

    /// Fetch object/version pairs in caller-provided order.
    fn get_object_version_pairs(
        &self,
        keys: &[(DfsObjectId, DfsVersion)],
    ) -> Result<Vec<(Option<DfsObjectMetadata>, Option<DfsObjectVersionRecord>)>> {
        keys.iter()
            .map(|(object_id, version)| {
                Ok((
                    self.get_object(object_id)?,
                    self.get_version(object_id, version)?,
                ))
            })
            .collect()
    }

    /// Bounded candidates for a generic retained-revision projection. Indexed
    /// backends may skip unrelated rows, but must include grants whose captured
    /// path or current object path is within `path_prefix`, in global
    /// `(object, version)` order. Candidates do not prove a live reference.
    /// The default preserves compatibility with backends without an index.
    fn retained_revision_records_page(
        &self,
        namespace: Option<&str>,
        file_system_id: &DfsFileSystemId,
        object_id: Option<&DfsObjectId>,
        path_prefix: Option<&str>,
        after: Option<&DfsObjectVersionScanCursor>,
        max_records: usize,
    ) -> Result<DfsObjectVersionScanPage> {
        let _ = (namespace, file_system_id, path_prefix);
        match object_id {
            Some(object_id) => object_version_records_page(self, object_id, after, max_records),
            None => self.version_records_page(after, max_records),
        }
    }

    /// Bounded generic grant projection over immutable version candidates.
    /// A filtered page may be empty; its cursor advances over candidates.
    /// Namespace filtering never combines distinct objects at a path.
    fn retained_revision_descriptors_page(
        &self,
        namespace: Option<&str>,
        file_system_id: &DfsFileSystemId,
        object_id: Option<&DfsObjectId>,
        path_prefix: Option<&str>,
        after: Option<&DfsObjectVersionScanCursor>,
        max_records: usize,
    ) -> Result<(
        Vec<crate::DfsRetainedRevisionDescriptor>,
        Option<DfsObjectVersionScanCursor>,
    )> {
        if max_records == 0 || max_records > 128 {
            return Err(MetadataBackendError::Serde(
                "retained revision page requires 1..=128 records".into(),
            ));
        }
        if let Some(namespace) = namespace {
            crate::validate_metadata_namespace(namespace).map_err(MetadataBackendError::Serde)?;
        }
        let page = self.retained_revision_records_page(
            namespace,
            file_system_id,
            object_id,
            path_prefix,
            after,
            max_records,
        )?;
        let mut entries = Vec::new();
        let mut pins_by_object = std::collections::BTreeMap::new();
        let mut current_by_object = std::collections::BTreeMap::new();
        let mut orders_by_object = std::collections::BTreeMap::<
            DfsObjectId,
            std::collections::BTreeMap<String, u64>,
        >::new();
        let now = crate::dfs_now_unix_nanos();
        for version in page.versions {
            if &version.file_system_id != file_system_id
                || object_id.is_some_and(|id| id != &version.object_id)
            {
                continue;
            }
            if !version.retention.iter().any(|(owner, state)| {
                namespace.is_none_or(|namespace| namespace == owner.as_str())
                    && state.grant.is_some()
            }) {
                continue;
            }
            crate::file_history::validate_file_version_record(&version)?;
            if !current_by_object.contains_key(&version.object_id) {
                current_by_object.insert(
                    version.object_id.clone(),
                    self.get_object(&version.object_id)?,
                );
            }
            let Some(current) = current_by_object
                .get(&version.object_id)
                .and_then(Option::as_ref)
                .filter(|object| object.tombstone_provenance.is_none())
            else {
                continue;
            };
            if let Some(prefix) = path_prefix {
                let matches = |path: &str| {
                    let path = crate::metadata_keys::normalize_posix_path(path);
                    prefix == "/"
                        || path == prefix
                        || path
                            .strip_prefix(prefix)
                            .is_some_and(|suffix| suffix.starts_with('/'))
                };
                if !matches(&current.path)
                    && !version.retention.iter().any(|(owner, state)| {
                        namespace.is_none_or(|namespace| namespace == owner.as_str())
                            && state
                                .grant
                                .as_ref()
                                .is_some_and(|grant| matches(&grant.object.path))
                    })
                {
                    continue;
                }
            }
            if !pins_by_object.contains_key(&version.object_id) {
                pins_by_object.insert(
                    version.object_id.clone(),
                    self.version_pins_for_object(&version.object_id)?,
                );
            }
            let references = pins_by_object[&version.object_id]
                .iter()
                .filter(|pin| pin.version == version.version)
                .cloned()
                .collect::<Vec<_>>();
            let has_reference = references
                .iter()
                .any(|pin| namespace.is_none_or(|owner| pin.namespace == owner));
            if !crate::file_history::retained_namespace_is_live(
                &version,
                namespace,
                has_reference,
                now,
                || {
                    if let Some(orders) = orders_by_object.get(&version.object_id) {
                        return Ok(orders.clone());
                    }
                    let orders = crate::file_history::retention_orders_from_backend(
                        self,
                        &version.object_id,
                    )?;
                    orders_by_object.insert(version.object_id.clone(), orders.clone());
                    Ok(orders)
                },
            )? {
                continue;
            }
            entries.push(crate::DfsRetainedRevisionDescriptor {
                version,
                current_object: current.clone(),
                references,
            });
        }
        Ok((entries, page.next_cursor))
    }

    fn query_file_history_on_shard(
        &self,
        query: &crate::DfsMetadataShardQuery,
    ) -> Result<crate::DfsMetadataQueryResult> {
        let _ = query;
        Err(MetadataBackendError::Serde(
            "backend does not support shard-scoped file history".into(),
        ))
    }

    /// Serve `count-contributions-v1` from the queried physical shard's
    /// tables, attributing paths with `catalog`. Only a backend holding that
    /// shard answers; the caller has already bound its current primary.
    fn count_contributions_on_shard(
        &self,
        query: &crate::DfsMetadataShardQuery,
        catalog: &[crate::DfsMetadataShardConfig],
    ) -> Result<crate::DfsMetadataQueryResult> {
        let _ = (query, catalog);
        Err(MetadataBackendError::Serde(
            "backend does not support shard count contributions".into(),
        ))
    }

    /// Compatibility helper that drains all versions for one object.
    fn versions_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsObjectVersionRecord>> {
        collect_metadata_pages(|cursor| {
            let page =
                self.versions_for_object_page(object_id, cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.versions, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of versions for one object.
    fn versions_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsObjectVersionPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionPage>;

    /// Compatibility helper that drains all receipts for one object version.
    fn receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.receipts_for_version_page(
                object_id,
                version,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.receipts, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of receipts for one object version.
    fn receipts_for_version_page(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        after: Option<&DfsReplicaReceiptPageCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptPage>;

    /// Current generation of one receipt slot: the present receipt's, the
    /// withdrawal tombstone's, or zero for a slot that was never withdrawn.
    fn receipt_slot_generation(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<u64>;

    /// Inspect the active object/version intent index for a placement-owned
    /// repair contract. Compacted retry history is not a live obligation.
    fn has_current_placement_repair_owner(&self, object: &DfsObjectMetadata) -> Result<bool>;

    /// At most `max_entries` needs of one local shard's derived repair index,
    /// in `(object, version, missing replica)` order after `after`. A shard
    /// this backend does not hold has no needs.
    #[doc(hidden)]
    fn repair_index_page(
        &self,
        shard_id: &DfsMetadataShardId,
        after: Option<&(DfsObjectId, DfsVersion, NodeId)>,
        max_entries: usize,
    ) -> Result<DfsRepairIndexPage>;

    /// The current needs among `keys` in one local shard's repair index.
    #[doc(hidden)]
    fn repair_index_needs(
        &self,
        shard_id: &DfsMetadataShardId,
        keys: &[(DfsObjectId, DfsVersion, NodeId)],
    ) -> Result<DfsRepairIndexPage>;

    /// Every current need of `objects` in one local shard's repair index.
    #[doc(hidden)]
    fn repair_index_objects(
        &self,
        shard_id: &DfsMetadataShardId,
        objects: &std::collections::BTreeSet<DfsObjectId>,
    ) -> Result<DfsRepairIndexPage>;

    /// The number of owned needs in one local shard's repair index, as the
    /// index maintains it.
    #[doc(hidden)]
    fn repair_index_owned_count(&self, shard_id: &DfsMetadataShardId) -> Result<usize>;

    /// Actual unit-test scan counters from already initialized physical children.
    /// These memory-only observations never open a backend or grant authority.
    #[cfg(test)]
    fn test_receipt_scan_counts_by_shard(&self) -> BTreeMap<DfsMetadataShardId, (usize, usize)>;
    #[cfg(test)]
    fn reset_test_receipt_scan_counts_by_shard(&self);

    /// Return full intents, compact receipts, and conservative eviction floors
    /// from one shard-local metadata snapshot.
    fn object_write_intent_history(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectWriteIntentHistory>;

    /// Fetch the exact compact representation for one intent, if compaction
    /// has replaced its full body.
    fn get_intent_receipt(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentReceipt>> {
        Ok(self
            .object_write_intent_history(object_id)?
            .receipts
            .into_iter()
            .find(|receipt| receipt.intent_id == intent_id))
    }

    /// Return the conservative receipt-eviction floor owned by the object's
    /// current metadata shard.
    fn intent_receipt_retention_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<DfsWriteIntentReceiptRetentionState> {
        Ok(self.object_write_intent_history(object_id)?.retention)
    }

    /// Fetch a write intent by object id and intent id.
    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>>;

    /// Observe one write identity and its object's indexed reservations without
    /// opening unrelated shards or granting publication authority.
    fn write_operation_admission_snapshot(
        &self,
        shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<DfsWriteOperationAdmissionSnapshot>;

    /// Fetch one exact write-intent key from one physical local metadata
    /// shard. Unlike [`Self::get_intent`], this never follows an object index
    /// or merges historical shard copies. Bounded recovery uses it to prove
    /// that an active intent has exactly one durable shard provenance before
    /// making an invisible create visible.
    fn get_intent_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>>;

    /// Fetch one exact receipt key from one physical local metadata shard.
    /// This is the receipt counterpart to [`Self::get_intent_on_shard`] and
    /// avoids relying on a visible-object routing index during create recovery.
    fn get_receipt_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsReplicaReceiptRecord>>;

    /// Highest generated intent counter which must not be reused after compact
    /// receipts have been evicted.
    fn write_intent_counter_floor(&self) -> Result<u64> {
        Ok(0)
    }

    /// Compatibility helper that drains all intents for one object.
    fn intents_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsWriteIntentRecord>> {
        collect_metadata_pages(|cursor| {
            let page =
                self.intents_for_object_page(object_id, cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.intents, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of intents for one object.
    fn intents_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage>;

    /// Return one cursor-bounded page of intents for one object and target
    /// version.
    fn intents_for_object_version_page(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        let (intents, next_cursor) = collect_filtered_cursor_page(
            after.cloned(),
            max_entries,
            |cursor| {
                let page = self.intents_for_object_page(
                    object_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.intents, page.next_cursor))
            },
            |intent| {
                if intent.target_version != *target_version {
                    return Ok(None);
                }
                let cursor = write_intent_page_cursor(&intent);
                Ok(Some((intent, cursor)))
            },
        )?;
        Ok(DfsWriteIntentPage {
            intents,
            next_cursor,
        })
    }

    /// Return one cursor-bounded global page of write intents.
    fn write_intents_page(
        &self,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentScanPage>;

    /// Spend a raw-record budget in at most one physical shard, before active
    /// or ownership filtering. A continuation is advisory scan progress only.
    /// Public inventory paging cannot provide this work bound after filtering,
    /// so unsupported backends must not silently fall back to that API.
    fn write_intent_maintenance_page(
        &self,
        _after: Option<&crate::DfsWriteIntentMaintenanceCursor>,
        _max_records: usize,
    ) -> Result<crate::DfsWriteIntentMaintenancePage> {
        Err(MetadataBackendError::Serde(
            "metadata backend does not support physically bounded intent maintenance".into(),
        ))
    }

    /// Apply the existing merged-scan visibility predicate to one physically
    /// observed active row. This is separate from bounded enumeration because
    /// ownership point lookups may cross shard boundaries. Recovery must still
    /// perform its normal exact authority and durable-record rechecks.
    fn write_intent_maintenance_candidate_is_listable(
        &self,
        _shard_id: &DfsMetadataShardId,
        _intent: &DfsWriteIntentRecord,
    ) -> Result<bool> {
        Err(MetadataBackendError::Serde(
            "metadata backend does not support physical intent visibility checks".into(),
        ))
    }

    /// Return one cursor-bounded page of durable write intents from one exact
    /// local metadata shard.
    ///
    /// Recovery uses this shard-scoped view to retain intent provenance across
    /// authority movement. It must include historical duplicates which the
    /// merged global scan deliberately filters from ordinary callers.
    fn write_intents_on_shard_page(
        &self,
        shard_id: &DfsMetadataShardId,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<Option<DfsWriteIntentScanPage>>;

    /// Return one cursor-bounded global page of object-version records.
    fn version_records_page(
        &self,
        after: Option<&DfsObjectVersionScanCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionScanPage>;

    /// Return one cursor-bounded page of every object-version record one
    /// physical shard store holds, listed or not, visible or not. `None` when
    /// this backend does not hold `shard_id`.
    fn version_records_on_shard_page(
        &self,
        _shard_id: &DfsMetadataShardId,
        _after: Option<&DfsObjectVersionScanCursor>,
        _max_entries: usize,
    ) -> Result<Option<DfsObjectVersionScanPage>> {
        Ok(None)
    }

    /// Return one cursor-bounded global page of replica receipts.
    fn receipt_records_page(
        &self,
        after: Option<&DfsReplicaReceiptScanCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptScanPage>;

    /// Return one cursor-bounded global page of conflict records.

    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_versions(&self) -> Result<Vec<DfsObjectVersionRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.version_records_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.versions, page.next_cursor))
        })
    }
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_receipts(&self) -> Result<Vec<DfsReplicaReceiptRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.receipt_records_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.receipts, page.next_cursor))
        })
    }
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.
    fn all_intents(&self) -> Result<Vec<DfsWriteIntentRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.write_intents_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.intents, page.next_cursor))
        })
    }
    /// Compatibility helper for callers that still need a full materialized
    /// list. Implementations should expose the paged scan APIs as primitives.

    /// Add one object's versions, receipts, intents, conflicts, and repair
    /// count into an in-progress summary.
    fn accumulate_metadata_summary_for_object(
        &self,
        summary: &mut DfsMetadataSummary,
        object: &DfsObjectMetadata,
    ) -> Result<()> {
        summary.object_count = summary.object_count.saturating_add(1);

        let mut object_intents = Vec::new();
        walk_metadata_pages(|intent_cursor| {
            let intent_page = self.intents_for_object_page(
                &object.object_id,
                intent_cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            summary.intent_count = summary
                .intent_count
                .saturating_add(intent_page.intents.len());
            summary.pending_write_count = summary.pending_write_count.saturating_add(
                intent_page
                    .intents
                    .iter()
                    .filter(|intent| write_intent_status_is_active(&intent.status))
                    .count(),
            );
            for intent in intent_page.intents {
                if self
                    .get_intent_receipt(&intent.object_id, &intent.intent_id)?
                    .is_none()
                {
                    object_intents.push(intent);
                }
            }
            Ok(intent_page.next_cursor)
        })?;

        let mut receipts_by_version = BTreeMap::<DfsVersion, Vec<DfsReplicaReceiptRecord>>::new();
        let mut repair_versions = BTreeMap::new();
        walk_metadata_pages(|version_cursor| {
            let version_page = self.versions_for_object_page(
                &object.object_id,
                version_cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            summary.version_count = summary
                .version_count
                .saturating_add(version_page.versions.len());
            for version in version_page.versions {
                repair_versions.insert(version.version.clone(), version.clone());
                walk_metadata_pages(|receipt_cursor| {
                    let receipt_page = self.receipts_for_version_page(
                        &object.object_id,
                        &version.version,
                        receipt_cursor,
                        OBJECT_LOCAL_METADATA_PAGE_SIZE,
                    )?;
                    summary.receipt_count = summary
                        .receipt_count
                        .saturating_add(receipt_page.receipts.len());
                    receipts_by_version
                        .entry(version.version.clone())
                        .or_default()
                        .extend(receipt_page.receipts);
                    Ok(receipt_page.next_cursor)
                })?;
            }
            Ok(version_page.next_cursor)
        })?;

        summary.pending_repair_count =
            summary
                .pending_repair_count
                .saturating_add(count_pending_repairs_for_object(
                    object,
                    &object_intents,
                    &receipts_by_version,
                    &repair_versions,
                    &self
                        .version_pins_for_object(&object.object_id)?
                        .into_iter()
                        .map(|pin| pin.version)
                        .collect(),
                ));
        Ok(())
    }

    /// Add all object-level counts for one filesystem into an in-progress
    /// summary.
    fn accumulate_metadata_summary_for_file_system(
        &self,
        summary: &mut DfsMetadataSummary,
        file_system_id: &DfsFileSystemId,
    ) -> Result<()> {
        walk_metadata_pages(|object_cursor| {
            let page = self.objects_for_file_system_page(
                file_system_id,
                object_cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            for object in page.objects {
                self.accumulate_metadata_summary_for_object(summary, &object)?;
            }
            Ok(page.next_cursor)
        })?;
        Ok(())
    }

    /// Add namespace-job counts into an in-progress summary, optionally scoped
    /// to one filesystem.
    fn accumulate_namespace_job_summary(
        &self,
        summary: &mut DfsMetadataSummary,
        file_system_scope: Option<&DfsFileSystemId>,
    ) -> Result<()> {
        walk_metadata_pages(|cursor| {
            let page = self.list_namespace_jobs_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            for job in page.jobs {
                if file_system_scope
                    .is_some_and(|file_system_id| job.file_system_id != *file_system_id)
                {
                    continue;
                }
                match job.status {
                    DfsNamespaceJobStatus::Running => {
                        summary.pending_namespace_job_count =
                            summary.pending_namespace_job_count.saturating_add(1);
                    }
                    DfsNamespaceJobStatus::Failed => {
                        summary.failed_namespace_job_count =
                            summary.failed_namespace_job_count.saturating_add(1);
                    }
                    DfsNamespaceJobStatus::Completed => {}
                }
            }
            Ok(page.next_cursor)
        })?;
        Ok(())
    }

    /// Compute a metadata summary across every filesystem visible to the
    /// backend.
    fn metadata_summary(&self) -> Result<DfsMetadataSummary> {
        let filesystems = self.list_file_systems()?;
        let mut summary = DfsMetadataSummary {
            file_system_count: filesystems.len(),
            ..DfsMetadataSummary::default()
        };
        for file_system in &filesystems {
            self.accumulate_metadata_summary_for_file_system(
                &mut summary,
                &file_system.file_system_id,
            )?;
        }
        self.accumulate_namespace_job_summary(&mut summary, None)?;
        Ok(summary)
    }

    /// Compute the operational metadata counts owned by this node's current
    /// primary shards.
    ///
    /// A non-sharded backend is wholly local, so the default is the complete
    /// summary. Topology-aware inspection backends override this to exclude
    /// replica-only backlog without reopening a second subset of shard stores.
    fn metadata_summary_for_local_primary_shards(&self) -> Result<DfsMetadataSummary> {
        self.metadata_summary()
    }

    /// Compute the complete and local-primary operational summaries from one
    /// coherent table observation.
    ///
    /// The default backend is wholly local, so both projections are identical.
    /// Sharded backends override this to avoid rescanning primary tables after
    /// building the complete operator summary.
    fn metadata_status_summaries(&self) -> Result<(DfsMetadataSummary, DfsMetadataSummary)> {
        let summary = self.metadata_summary()?;
        Ok((summary.clone(), summary))
    }

    /// Compute a metadata summary for one filesystem, returning `None` when
    /// the filesystem is unknown.
    fn metadata_summary_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsMetadataSummary>> {
        let Some(_) = self.get_file_system(file_system_id)? else {
            return Ok(None);
        };
        let mut summary = DfsMetadataSummary {
            file_system_count: 1,
            ..DfsMetadataSummary::default()
        };
        self.accumulate_metadata_summary_for_file_system(&mut summary, file_system_id)?;
        self.accumulate_namespace_job_summary(&mut summary, Some(file_system_id))?;
        Ok(Some(summary))
    }

    /// Fetch a recorded namespace transaction by id when the backend supports
    /// transaction recovery.

    /// Exact compact outcome identity retained after a transaction body is
    /// checkpointed. Backends without durable receipts fail closed for callers
    /// which require replayable module contribution effects.
    fn get_namespace_transaction_receipt(
        &self,
        transaction_id: &str,
    ) -> Result<Option<crate::DfsNamespaceTransactionReceipt>>;

    /// One bounded object-indexed page of durable reference outcome witnesses.
    fn content_reference_outcomes_for_object_page(
        &self,
        _file_system_id: &DfsFileSystemId,
        _object_id: &DfsObjectId,
        _after: Option<&str>,
        _max_entries: usize,
    ) -> Result<crate::DfsContentReferenceOutcomePage>;

    fn content_reference_outcome_retention(
        &self,
        _file_system_id: &DfsFileSystemId,
        _object_id: &DfsObjectId,
    ) -> Result<Option<crate::DfsContentReferenceOutcomeRetention>>;

    fn content_reference_outcome_history_expired(
        &self,
        identity: &crate::DfsRevisionReference,
        operation_id: &str,
    ) -> Result<bool> {
        let Some(retention) = self
            .content_reference_outcome_retention(&identity.file_system_id, &identity.object_id)?
        else {
            return Ok(false);
        };
        Ok(
            match crate::intent::stable_operation_order_token(operation_id) {
                Some(token) => retention
                    .max_evicted_operation_order_token
                    .as_ref()
                    .is_some_and(|floor| token <= *floor),
                None => retention.opaque_operation_history_evicted,
            },
        )
    }

    /// Project retention-eligible tombstones without taking a write lock.
    ///
    /// Reactive maintenance schedulers use the returned due count and next
    /// eligibility deadline to avoid periodic full passes on idle metadata.
    fn tombstone_compaction_projection(
        &self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        Ok(DfsTombstoneCompactionReport::for_options(&options))
    }

    /// Compact eligible tombstone records according to backend policy.
    fn compact_tombstones(
        &mut self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        let _ = options;
        Ok(DfsTombstoneCompactionReport::default())
    }

    /// Project retention-eligible metadata history and its next deadline
    /// without authoring journal entries.
    fn metadata_history_gc_projection(
        &self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<DfsMetadataHistoryGcReport> {
        Ok(DfsMetadataHistoryGcReport::for_options(&options))
    }

    /// Append and materialize a bounded authoritative metadata-history GC
    /// batch. Implementations must journal every observable removal.
    fn plan_metadata_history_gc(
        &mut self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<DfsMetadataHistoryEffectsPlan>;

    /// Return one bounded, stable page of shard-local content reachability
    /// roots from an exact durable metadata cut.
    ///
    /// Clustered callers validate primary authority and topology before
    /// invoking this method. Backends which cannot expose an authoritative
    /// shard image fail closed with a structured, rootless page.
    fn content_root_catalog_page(
        &mut self,
        request: &DfsContentRootCatalogPageRequest,
    ) -> Result<DfsContentRootCatalogPage> {
        request.validate()?;
        Ok(DfsContentRootCatalogPage::incomplete_for_request(
            request,
            DfsContentRootCatalogIncompleteReason::MetadataBackendUnavailable,
            crate::metadata_tombstone::metadata_now_unix_nanos(),
        ))
    }

    /// Return the exact metadata roots which local version-manifest and blob
    /// reclamation must preserve.
    fn metadata_content_protection(&self) -> Result<DfsMetadataContentProtection>;

    /// Fetch a durable namespace request by authenticated requester and id.
    fn get_namespace_request(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespaceRequestRecord>>;

    /// Fetch the compact canonical outcome retained after a completed request
    /// has been folded into an authoritative metadata snapshot.
    fn get_namespace_request_receipt(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespaceRequestReceipt>> {
        let _ = (requester_node_id, request_id);
        Ok(None)
    }

    /// Return whether a missing request identity is at or below this shard's
    /// durable idempotency-retention floor.
    ///
    /// Callers must reject such an identity as expired/unknown. Treating it as
    /// new could reapply an operation after its exact terminal receipt was
    /// evicted from the bounded checkpoint window.
    fn namespace_request_history_expired_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<bool> {
        let _ = (shard_id, requester_node_id, request_id);
        Ok(false)
    }

    /// Read all durable idempotency evidence needed to admit one namespace
    /// request from a single coherent metadata generation.
    ///
    /// Inspect each retained shard once and read all admission indexes from
    /// the same immutable table generation.
    fn namespace_request_admission_snapshot(
        &mut self,
        shard_id: &DfsMetadataShardId,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<DfsNamespaceRequestAdmissionSnapshot>;

    /// Return durable routed namespace requests for bounded autonomous
    /// inspection. Recovery should use `list_pending_namespace_requests_page`
    /// so terminal idempotency records are never cloned or filtered.
    ///
    /// Completed records remain complete until the next checkpoint folds them
    /// into bounded canonical receipts. Pending records are never pruned.
    fn list_namespace_requests(&self) -> Result<Vec<DfsNamespaceRequestRecord>>;

    /// Return one bounded, oldest-attempt-first page of pending namespace
    /// requests. Concrete durable backends serve this from a derived status
    /// index rather than scanning retained terminal idempotency records.
    fn list_pending_namespace_requests_page(
        &self,
        after: Option<&DfsPendingNamespaceRequestPageCursor>,
        max_entries: usize,
    ) -> Result<DfsPendingNamespaceRequestPage>;

    /// Fetch a namespace maintenance job by id.
    /// Exact active parent-write participants on the physical owner. This is
    /// an indexed view of existing namespace job rows, not new authority.
    fn parent_count_reservations(
        &self, shard_id: &DfsMetadataShardId, file_system_id: &DfsFileSystemId, object_id: &DfsObjectId,
    ) -> Result<Vec<DfsNamespaceJobRecord>> {
        let _ = (shard_id, file_system_id, object_id);
        Err(MetadataBackendError::Serde("backend has no indexed parent participant view".into()))
    }

    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>>;

    /// Fetch compact terminal namespace-job evidence by durable job id.
    fn get_namespace_job_receipt(&self, job_id: &str) -> Result<Option<DfsNamespaceJobReceipt>>;

    /// Find completed job evidence for an exact durable routed request.
    ///
    /// Match the actual durable origin identity; diagnostic path/time values
    /// do not replace routed request provenance.
    fn completed_namespace_job_receipt_for_paths_since(
        &self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: &str,
        kinds: &[DfsNamespaceJobKind],
        created_at: time::OffsetDateTime,
        origin: &DfsNamespaceJobOrigin,
    ) -> Result<Option<DfsNamespaceJobReceipt>>;

    /// Collect all namespace jobs through the bounded paging primitive.
    fn list_namespace_jobs(&self) -> Result<Vec<DfsNamespaceJobRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.list_namespace_jobs_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.jobs, page.next_cursor))
        })
    }

    /// Return one cursor-bounded page of namespace jobs.
    fn list_namespace_jobs_page(
        &self,
        after: Option<&DfsNamespaceJobPageCursor>,
        max_entries: usize,
    ) -> Result<DfsNamespaceJobPage> {
        let _ = after;
        let _ = max_entries;
        Ok(DfsNamespaceJobPage {
            jobs: Vec::new(),
            next_cursor: None,
        })
    }

    /// Read all journal entries after `after_sequence`.
    fn journal_since(&self, after_sequence: u64) -> Result<Vec<DfsMetadataJournalEntry>>;

    /// Highest journal sequence known across this backend.
    fn journal_last_sequence(&self) -> Result<u64> {
        Ok(self
            .metadata_shard_statuses()?
            .into_iter()
            .map(|status| status.journal_last_sequence)
            .max()
            .unwrap_or(0))
    }

    /// Export a table snapshot for replication or repair.
    fn export_table_snapshot(&self) -> Result<DfsMetadataTableSnapshot>;

    /// Snapshot of one shard's tables, for replica snapshot repair. Backends
    /// that cannot scope an export to a single shard return `None` and the
    /// repair is skipped.
    /// Capture the sole current native record image. This is DATA only and
    /// cannot mint the replica permit consumed by install.
    fn export_shard_snapshot_image(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<crate::DfsMetadataSnapshotImage>>;

    fn export_shard_table_snapshot(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataTableSnapshot>> {
        let _ = shard_id;
        Ok(None)
    }

    /// Return the O(1), durable identity of one shard's exact materialized
    /// journal prefix. Implementations must return `None` instead of deriving
    /// a partial or sequence-only identity.
    fn metadata_prefix_proof(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataPrefixProof>>;

    /// Observe authorized journal ingest and snapshot replacement across local
    /// backend handles. Notifications are hints, not substitutes for reading
    /// durable state; a requester retains its subscription between operations.
    fn subscribe_metadata_replica_progress(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<crate::DfsMetadataReplicaObserver>> {
        Ok(None)
    }

    /// Refresh and pin exactly one shard after observed durable replica progress.
    /// Return its materialized exact prefix only if it equals or verifiably
    /// extends the observed prefix. `Some` must be paired with `end` on every
    /// path; its job pages and prefix come from that same pinned generation.
    fn begin_replica_projection_read(
        &mut self,
        _installed: &crate::DfsMetadataReplicaObservation,
    ) -> Result<Option<DfsMetadataPrefixProof>> {
        Ok(None)
    }

    fn end_replica_projection_read(&mut self, _shard_id: &DfsMetadataShardId) {}

    /// One bounded job page from the shard pinned by `begin_replica_projection_read`.
    fn replica_projection_namespace_jobs_page(
        &self,
        _shard_id: &DfsMetadataShardId,
        _after: Option<&DfsNamespaceJobPageCursor>,
        _max_entries: usize,
    ) -> Result<DfsNamespaceJobPage> {
        Err(MetadataBackendError::Serde(
            "metadata backend does not support exact-shard snapshot job pages".into(),
        ))
    }

    /// Refresh one shard's in-memory metadata view from its durable tables and
    /// journal before a cold-path exact-prefix attestation. Implementations
    /// with no cached durable state may retain the no-op default.
    fn refresh_metadata_shard_durable_view(
        &mut self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        Ok(())
    }

    /// Refresh one shard only when its cached durable generation changed.
    ///
    /// The default preserves compatibility for backends that cannot compare a
    /// cached generation cheaply. Sharded local backends use this cold-path
    /// variant during service rebind so an unchanged activated marker does not
    /// rebuild the complete derived object-to-shard index on every open.
    fn refresh_metadata_shard_durable_view_if_changed(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        self.refresh_metadata_shard_durable_view(shard_id)?;
        Ok(true)
    }

    /// Refresh only the owned shard's table/journal view before an exact
    /// prefix attestation or durability-head read. Derived cross-shard object
    /// indexes and unrelated shard generations must remain untouched.
    fn refresh_metadata_shard_attestation_view(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        self.refresh_metadata_change_feed_view(shard_id)
    }

    /// Refresh the exact shard view needed by its change feed. This does not
    /// require rebuilding cross-shard object indexes from a full snapshot.
    fn refresh_metadata_change_feed_view(&mut self, shard_id: &DfsMetadataShardId) -> Result<()> {
        self.refresh_metadata_shard_durable_view(shard_id)
    }

    /// Return the durable membership-handoff write freeze for one shard.
    fn metadata_shard_write_freeze(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        Ok(None)
    }

    /// Whether this backend instance is currently fenced from authoritative
    /// shard mutations. An activated durable marker still fences an
    /// independently opened lane until exact completion evidence rebinds that
    /// process-local instance.
    fn metadata_shard_authoritative_writes_frozen(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        Ok(false)
    }

    /// The instance-local fence without a cached topology/primary-role filter.
    /// Mutation admission uses this only for a shard stamped as locally owned,
    /// then verifies the current assignment and exact completion proof. A
    /// former replica's stale routing catalog must not hide its own fence.
    fn metadata_shard_instance_writes_frozen(&self, shard_id: &DfsMetadataShardId) -> Result<bool> {
        self.metadata_shard_authoritative_writes_frozen(shard_id)
    }

    /// Atomically freeze authoritative mutations and capture the exact current
    /// shard state under the backend's cross-process write lock.
    fn freeze_metadata_shard_for_membership_handoff(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _handoff_id: &str,
        _retiring_epoch: u64,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        Ok(None)
    }

    /// Durably mark the exact matching freeze as activated without binding
    /// this backend instance to the successor authority generation. This lets
    /// a retiring instance record control-plane completion while remaining
    /// fenced. A fresh authority observer must separately call
    /// [`clear_metadata_shard_membership_handoff_freeze`](Self::clear_metadata_shard_membership_handoff_freeze)
    /// after proving the active assignment and completion evidence.
    fn mark_metadata_shard_membership_handoff_activated(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _handoff_id: &str,
        _activated_epoch: u64,
    ) -> Result<bool> {
        Ok(false)
    }

    /// Advance the exact matching freeze to a durable active generation after
    /// the desired assignment is active. Implementations must install the
    /// complete active shard configuration before clearing the pending freeze
    /// or running append-producing recovery. The calling backend instance is
    /// rebound to that exact generation; independently opened stale instances
    /// stay fenced until they separately prove and observe it. The exact
    /// prefix checked by the caller is revalidated under the refreshed writer
    /// before epoch binding, marker changes, rebind checkpointing or explicit recovery.
    fn clear_metadata_shard_membership_handoff_freeze(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _handoff_id: &str,
        _active_shard: &DfsMetadataShardConfig,
        _checked_prefix: &DfsMetadataPrefixProof,
    ) -> Result<bool> {
        Ok(false)
    }

    /// On a shard on the write-set protocol whose record is at
    /// `record_epoch`, remove this shard's handoff freeze marker when the
    /// record has moved past it
    /// ([`DfsMetadataShardWriteFreeze::is_obsolete_for_write_set_record`]),
    /// and unbind the instance from any earlier authority epoch. Returns
    /// whether a marker was removed.
    fn clear_obsolete_metadata_shard_handoff_freeze(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _record_epoch: u64,
        _completed_handoff_id: Option<&str>,
    ) -> Result<bool> {
        Ok(false)
    }

    /// Align this write-set member with `target`, a prefix of the permit
    /// epoch's history whose start is `epoch_start`, under the shard writer
    /// lock (see `metadata_backend::alignment`).
    fn align_metadata_replica_to_prefix(
        &mut self,
        _permit: &DfsMetadataReplicaIngestPermit,
        _target: &DfsMetadataPrefixProof,
        _epoch_start: &DfsMetadataPrefixProof,
    ) -> Result<DfsMetadataReplicaAlignment> {
        Err(write_set_member_state_unsupported())
    }

    /// This store's exact prefix proof at `sequence`, from its table
    /// checkpoint through its retained journal; `None` when it can no longer
    /// reconstruct it.
    fn historical_metadata_prefix_proof(
        &self,
        _shard_id: &DfsMetadataShardId,
        _sequence: u64,
    ) -> Result<Option<DfsMetadataPrefixProof>> {
        Ok(None)
    }

    /// Read at most `max_entries` journal entries after `after_sequence`.
    fn journal_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut entries = self.journal_since(after_sequence)?;
        entries.truncate(max_entries);
        Ok(entries)
    }

    /// Build a bounded journal batch after `after_sequence`.
    fn journal_batch_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        let entries = self.journal_since_limited(after_sequence, max_entries)?;
        bounded_metadata_journal_batch(
            DfsMetadataShardId::new("default"),
            after_sequence,
            entries,
            max_entries,
            max_encoded_bytes,
        )
    }

    /// Build a bounded journal batch for one shard. Single-shard backends may
    /// delegate to `journal_batch_since_limited`.
    fn journal_batch_for_shard_since_limited(
        &self,
        _shard_id: &DfsMetadataShardId,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        self.journal_batch_since_limited(after_sequence, max_entries, max_encoded_bytes)
    }

    /// Return the exact immutable SOBJ frames for a previously selected
    /// journal batch. These must be the bytes already stored in its
    /// authoritative journal; decoded records cannot be re-encoded as evidence.
    fn journal_frames_for_batch(
        &self,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<Vec<DfsMetadataJournalFrameRecord>>;

    /// Share fresh, already verified foreground ACKs with other local sender
    /// handles. This is a non-durable scheduling hint, never publication authority.
    fn observe_metadata_replication_cursors(
        &self,
        _shard_id: &DfsMetadataShardId,
        _cursors: &[(NodeId, u64)],
    ) {
    }

    /// Highest known delivered prefix, combining the saved checkpoint with
    /// verified in-process ACK hints. This is a suffix-selection cursor, not
    /// fresh evidence of receiver durability or publication authority.
    fn metadata_replication_cursor(
        &self,
        _target_node_id: &NodeId,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<u64>;

    /// Checkpoint the next journal sequence to send to a target for a shard.
    /// This derived hint may be lost on crash; it cannot certify receiver
    /// durability or authorize publication without a fresh exact acknowledgement.
    fn put_metadata_replication_cursor(
        &mut self,
        _target_node_id: &NodeId,
        _shard_id: &DfsMetadataShardId,
        _next_after_sequence: u64,
    ) -> Result<()>;

    /// Return the replica-local exact-checkpoint floor persisted before a
    /// successful prefix acknowledgement.
    fn metadata_prefix_ack_floor(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataPrefixAckFloor>>;

    /// Persist a monotonic exact-checkpoint floor before acknowledging the
    /// corresponding prefix probe.
    fn put_metadata_prefix_ack_floor(&mut self, _floor: &DfsMetadataPrefixAckFloor) -> Result<()>;

    /// Return the replica-local floor persisted before an exact ordinary-write
    /// authority acknowledgement.
    fn metadata_authority_ack_floor(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataAuthorityAckFloor>>;

    /// Persist a monotonic ordinary-write authority floor before returning the
    /// corresponding request nonce.
    fn put_metadata_authority_ack_floor(
        &mut self,
        floor: &DfsMetadataAuthorityAckFloor,
    ) -> Result<()>;

    /// Durable member-local state of a write-set shard, if any.
    fn write_set_member_state(
        &self,
        _shard_id: &DfsMetadataShardId,
    ) -> Result<Option<crate::DfsMetadataWriteSetMemberState>> {
        Err(write_set_member_state_unsupported())
    }

    /// Durably promise to accept nothing more at `cut`'s epoch and return the
    /// exact epoch start, under the shard's writer lock.
    fn promise_metadata_takeover(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _cut: &crate::DfsMetadataShardRecordCut,
        _swap_request_id: &sinomo_core::kv::KvRequestId,
    ) -> Result<crate::DfsMetadataPrefixProof> {
        Err(write_set_member_state_unsupported())
    }

    /// End a settled takeover promise.
    fn settle_metadata_takeover_promise(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _swap_request_id: &sinomo_core::kv::KvRequestId,
        _end: crate::DfsMetadataTakeoverPromiseEnd,
    ) -> Result<()> {
        Err(write_set_member_state_unsupported())
    }

    /// Drop the acceptance fence once the member's record view has reached it.
    fn retire_write_set_member_fence(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _observed_epoch: u64,
    ) -> Result<()> {
        Err(write_set_member_state_unsupported())
    }

    /// Durably require a member's acknowledgement before its add is submitted.
    fn record_pending_write_set_add(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _cut: &crate::DfsMetadataShardRecordCut,
        _node_id: &NodeId,
        _evidence: &crate::DfsMetadataMemberAckView,
    ) -> Result<()> {
        Err(write_set_member_state_unsupported())
    }

    /// Drop pending adds the record `current` has moved past.
    fn release_pending_write_set_adds(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _current: &crate::DfsMetadataShardRecordCut,
    ) -> Result<Vec<NodeId>> {
        Err(write_set_member_state_unsupported())
    }

    /// The same release for a caller that already holds the shard writer
    /// lock through `writer`, such as the publication fence.
    fn release_superseded_pending_write_set_adds_held(
        &mut self,
        _shard_id: &DfsMetadataShardId,
        _shard: &crate::DfsMetadataShardConfig,
        _write_set: Option<&crate::DfsMetadataShardWriteSet>,
        _writer: &crate::publication_guard::DfsPublicationWriterSession,
    ) -> Result<Vec<NodeId>> {
        Err(write_set_member_state_unsupported())
    }

    /// Return the durable replay cursor for journal entries originating from
    /// `source_shard_id`. Local durable backends require this to match their
    /// logical shard identity.
    fn metadata_replay_cursor(&self, source_shard_id: &DfsMetadataShardId) -> Result<u64> {
        Ok(self
            .metadata_shard_statuses()?
            .into_iter()
            .flat_map(|status| status.replay_cursors)
            .find(|cursor| cursor.source_shard_id == *source_shard_id)
            .map(|cursor| cursor.replayed_through_sequence)
            .unwrap_or(0))
    }

    /// Return per-shard metadata journal and replay status.
    fn metadata_shard_statuses(&self) -> Result<Vec<DfsMetadataShardStatus>>;

    /// Resolve the durable shard which owns one namespace path when this
    /// backend can prove that routing identity.
    ///
    /// Unsharded backends inherit the single status identity. Sharded routers
    /// override this with their canonical path routing table. Returning `None`
    /// is conservative: callers retain an unscoped guard which exact-shard
    /// replacement cannot discard without positive ownership evidence.
    fn metadata_shard_id_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsMetadataShardId>> {
        let _ = (file_system_id, path);
        let statuses = self.metadata_shard_statuses()?;
        Ok((statuses.len() == 1).then(|| statuses[0].shard_id.clone()))
    }

    /// Persist the original replica frames under their validated source permit.
    /// Durability acknowledgement does not require the rebuildable tables to
    /// reach the same cut; authoring and handoff retain their refresh fences.
    fn persist_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
    ) -> Result<DfsMetadataReplicaApplyReport>;

    /// Persist the complete current source cut and independently verify its
    /// named original preparation, including the full body and actual slot.
    /// Later frames remain part of the exact prefix obligation. A delayed cut
    /// must still extend to any later locally acknowledged durable history.
    fn persist_authority_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
        expected_target_prefix: &DfsMetadataPrefixProof,
        expected_operation: &crate::DfsMetadataAuthorityOperation,
    ) -> Result<DfsMetadataReplicaApplyReport>;

    /// Materialize any journal suffix previously admitted at the durable ACK
    /// boundary. Promotion, snapshots, and ordinary authoritative writes must
    /// continue to use their existing synchronous refresh fences.
    fn materialize_pending_replica_journal(&mut self) -> Result<usize> {
        Ok(0)
    }

    /// Replace local metadata tables using exact source evidence validated by
    /// the metadata replication boundary. The local durable implementation
    /// preserves any activated author fence and never binds authority through
    /// this path.
    fn replace_with_replicated_snapshot_with_replica_permit(
        &mut self,
        _permit: &DfsMetadataReplicaIngestPermit,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: crate::DfsMetadataSnapshotImage,
    ) -> Result<bool>;

    /// Create a subscription cursor for journal polling.
    fn subscribe(&self, after_sequence: u64) -> DfsMetadataSubscription;

    /// Poll journal entries for a subscription.
    fn poll_subscription(
        &self,
        subscription: &mut DfsMetadataSubscription,
    ) -> Result<DfsMetadataJournalBatch>;
}

#[cfg(test)]
mod selected_native_layout_tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    fn layout(name: &str) -> (sinomo_testdir::TestDir, sinomo_core::local::SinomoDataDir,
        crate::runtime_host::DfsDataOwnerLock, Arc<DfsSelectedOriginalDataLayout>) {
        let root = sinomo_testdir::TestDir::new(name);
        let data = sinomo_core::local::SinomoDataDir::new(root.path());
        let mut owner = crate::runtime_host::DfsDataOwnerLock::acquire(
            &crate::runtime_host::DfsRuntimePaths::new(&data)).unwrap();
        let selected = owner.admit_selected_original_fixture(None).unwrap();
        (root, data, owner, selected)
    }

    fn files(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        fn visit(root: &Path, at: &Path, result: &mut Vec<(PathBuf, Vec<u8>)>) {
            for entry in std::fs::read_dir(at).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() { visit(root, &path, result); }
                else { result.push((path.strip_prefix(root).unwrap().to_path_buf(), std::fs::read(path).unwrap())); }
            }
        }
        let mut result = Vec::new(); visit(root, root, &mut result);
        result.sort_by(|a,b| a.0.cmp(&b.0)); result
    }

    #[cfg(unix)]
    #[test]
    fn selected_native_directory_identity_accepts_genuine_directory() {
        let root = sinomo_testdir::TestDir::new("selected-directory-genuine");
        let before = std::fs::symlink_metadata(root.path()).unwrap();
        let file = std::fs::File::open(root.path()).unwrap();
        let opened = file.metadata().unwrap();
        let after = std::fs::symlink_metadata(root.path()).unwrap();
        assert!(!crate::selected_fixture::same_file_observation(&before, &opened),
            "the unchanged regular-file predicate rejects directories");
        assert!(DfsSelectedOriginalDataLayout::same_directory_observations(&before, &opened, &after));
        assert_eq!(DfsSelectedOriginalDataLayout::directory_identity(root.path()).unwrap(),
            crate::runtime_host::storage_file_identity(&file).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn selected_native_directory_identity_accepts_child_entry_metadata_changes() {
        let root = sinomo_testdir::TestDir::new("selected-directory-child-changes");
        let before = std::fs::symlink_metadata(root.path()).unwrap();
        let file = std::fs::File::open(root.path()).unwrap();
        let opened = file.metadata().unwrap();
        let expected = crate::runtime_host::storage_file_identity(&file).unwrap();
        let child = root.path().join("child");
        std::fs::write(&child, b"ordinary directory entry").unwrap();
        // Ensure a visible incidental timestamp difference without a sleep or
        // depending on the filesystem's clock granularity.
        file.set_times(std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH)).unwrap();
        let after = std::fs::symlink_metadata(root.path()).unwrap();
        assert_ne!(before.modified().unwrap(), after.modified().unwrap());
        assert!(DfsSelectedOriginalDataLayout::same_directory_observations(&before, &opened, &after));
        assert_eq!(DfsSelectedOriginalDataLayout::directory_identity(root.path()).unwrap(), expected);
        std::fs::remove_file(child).unwrap();
        let after_remove = std::fs::symlink_metadata(root.path()).unwrap();
        assert!(DfsSelectedOriginalDataLayout::same_directory_observations(&before, &opened, &after_remove));
        assert_eq!(DfsSelectedOriginalDataLayout::directory_identity(root.path()).unwrap(), expected);
    }

    #[cfg(unix)]
    #[test]
    fn selected_native_directory_identity_rejects_replacement_after_open() {
        let root = sinomo_testdir::TestDir::new("selected-directory-after-open");
        let data = sinomo_core::local::SinomoDataDir::new(root.path().join("data"));
        let mut owner = crate::runtime_host::DfsDataOwnerLock::acquire(
            &crate::runtime_host::DfsRuntimePaths::new(&data)).unwrap();
        let selected = owner.admit_selected_original_fixture(None).unwrap();
        let path = data.root();
        let before = std::fs::symlink_metadata(&path).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let opened = file.metadata().unwrap();
        let expected = crate::runtime_host::storage_file_identity(&file).unwrap();
        std::fs::rename(&path, root.path().join("retained-data")).unwrap();
        std::fs::create_dir(&path).unwrap();
        let after = std::fs::symlink_metadata(&path).unwrap();
        assert!(!DfsSelectedOriginalDataLayout::same_directory_observations(&before, &opened, &after));
        assert_ne!(DfsSelectedOriginalDataLayout::directory_identity(&path).unwrap(), expected);
        let error = selected.check().unwrap_err();
        assert!(error.to_string().contains("selected native DATA root was replaced"), "{error}");
        assert!(std::fs::read_dir(&path).unwrap().next().is_none(),
            "the rejected replacement was not initialized or repaired");
        // Retained directory and descriptor stay live through the assertions,
        // preventing inode reuse from obscuring the replacement control.
        drop(file);
        drop(selected);
        drop(owner);
    }

    #[cfg(unix)]
    #[test]
    fn selected_native_directory_identity_rejects_replacement_before_open() {
        let root = sinomo_testdir::TestDir::new("selected-directory-before-open");
        let path = root.path().join("named");
        std::fs::create_dir(&path).unwrap();
        let before = std::fs::symlink_metadata(&path).unwrap();
        let retained_file = std::fs::File::open(&path).unwrap();
        std::fs::rename(&path, root.path().join("retained")).unwrap();
        std::fs::create_dir(&path).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let opened = file.metadata().unwrap();
        let after = std::fs::symlink_metadata(&path).unwrap();
        assert_ne!(crate::runtime_host::storage_file_identity(&retained_file).unwrap(),
            crate::runtime_host::storage_file_identity(&file).unwrap());
        assert!(!DfsSelectedOriginalDataLayout::same_directory_observations(&before, &opened, &after),
            "opened and after agree, but the original named directory was replaced");
    }

    #[cfg(unix)]
    #[test]
    fn selected_native_directory_identity_rejects_file_and_symlink_alias() {
        let root = sinomo_testdir::TestDir::new("selected-directory-alias");
        let named = root.path().join("real-directory");
        std::fs::create_dir(&named).unwrap();
        let before = std::fs::symlink_metadata(&named).unwrap();
        let file = std::fs::File::open(&named).unwrap();
        let opened = file.metadata().unwrap();
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(&named, &alias).unwrap();
        let alias_metadata = std::fs::symlink_metadata(&alias).unwrap();
        assert!(DfsSelectedOriginalDataLayout::directory_identity(&alias).is_err());
        assert!(!DfsSelectedOriginalDataLayout::same_directory_observations(&before, &opened, &alias_metadata));
        let ordinary_file = root.path().join("regular-file");
        std::fs::write(&ordinary_file, b"not a directory").unwrap();
        assert!(DfsSelectedOriginalDataLayout::directory_identity(&ordinary_file).is_err());
        assert_eq!(std::fs::read(ordinary_file).unwrap(), b"not a directory");
        assert_eq!(std::fs::read_link(alias).unwrap(), named);
    }

    #[test]
    fn selected_native_v1_freeze_default_readers_and_writers_reject_before_mutation() {
        let (_root, data, _owner, layout) = layout("selected-native-default-v1-freeze");
        let fs = crate::sifs_file_system_id();
        let shard = crate::DfsMetadataShardId::new("selected-native-shard");
        let root = crate::local_dfs_metadata_shard_path(&data, &shard);
        layout.admit_stage(&fs, &shard, &root).unwrap();
        crate::test_runtime::copy_native_genesis_data_into_selected(&root, &shard.0, &fs, &layout);
        let mut backend = crate::LocalSobjMetadataBackend::new_with_selected_layout(
            fs.clone(), shard.clone(), root.clone(), Arc::clone(&layout)).unwrap();
        let freeze = backend.freeze_metadata_shard_for_membership_handoff(&shard, "selected-native-v1", 7).unwrap().unwrap();
        drop(backend);
        let path = root.join("journal/membership-handoff-write-freeze.sobj");
        let before = files(&data.root());
        assert_eq!(read_handoff_freeze_with_layout(&path, Some(&layout)).unwrap(), Some(freeze.clone()));
        assert!(read_handoff_freeze_with_layout(&path, None).is_err());
        assert!(write_handoff_freeze_with_layout(&path, &freeze, None).is_err());
        assert!(crate::read_local_dfs_metadata_shard_write_freeze(&data, &shard).is_err());
        assert!(crate::LocalSobjMetadataBackend::new(shard.clone(), &root).is_err());
        assert!(crate::LocalSobjMetadataBackend::open_for_inspection(shard.clone(), &root).is_err());
        assert!(crate::LocalSobjMetadataBackend::open_for_replica_repair(shard, &root).is_err());
        assert_eq!(files(&data.root()), before, "unsupported reader does not recover, repair or rewrite even the initial V1 marker");
    }

    #[test]
    fn selected_native_open_rejects_stage_replaced_by_preparation_before_recovery() {
        let (_root, data, _owner, layout) = layout("selected-native-preparation-replacement");
        let fs = DfsFileSystemId::from_legacy_named("selected-native-preparation-fs").unwrap();
        let shard = crate::DfsMetadataShardId::new("selected-native-preparation-shard");
        let stage = crate::local_dfs_metadata_shard_path(&data, &shard);
        layout.admit_stage(&fs, &shard, &stage).unwrap();
        let expected_stage = stage.clone();
        let retained = stage.with_extension("retained");
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = Arc::clone(&called);
        let registration = crate::register_dfs_storage_preparation_provider(data.root().to_owned(),
            Arc::new(move |root, scope, _| {
                assert_eq!(root, expected_stage);
                assert!(matches!(scope, crate::DfsStoragePreparationScope::Metadata(_)));
                assert!(!observed.swap(true, std::sync::atomic::Ordering::SeqCst));
                std::fs::rename(root, &retained).unwrap();
                std::fs::create_dir(root).unwrap();
                std::fs::write(root.join("replacement-sentinel"), b"new physical owner").unwrap();
                Ok(crate::DfsStoragePreparation::Ready)
            }), None).unwrap();
        assert!(crate::LocalSobjMetadataBackend::new_with_selected_layout(fs, shard, stage.clone(), layout).is_err());
        assert!(called.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(files(&stage), vec![(PathBuf::from("replacement-sentinel"), b"new physical owner".to_vec())]);
        drop(registration);
    }

    #[cfg(unix)]
    #[test]
    fn selected_native_owner_alias_and_replaced_stage_never_rebind_layout() {
        let (_root, data, _owner, layout) = layout("selected-native-owner-identity");
        let owner_path = data.root().join("daemon/data-owner.lock");
        let retained_owner = owner_path.with_extension("retained");
        std::fs::rename(&owner_path, &retained_owner).unwrap();
        std::os::unix::fs::symlink(&retained_owner, &owner_path).unwrap();
        assert!(layout.check().is_err(), "a symlink to the still-locked old inode is not the actual named owner");
        std::fs::remove_file(&owner_path).unwrap();
        std::fs::rename(&retained_owner, &owner_path).unwrap();
        layout.check().unwrap();
        let fs = DfsFileSystemId::from_legacy_named("selected-native-owner-fs").unwrap();
        let shard = crate::DfsMetadataShardId::new("selected-native-owner-shard");
        let stage = crate::local_dfs_metadata_shard_path(&data, &shard);
        layout.admit_stage(&fs, &shard, &stage).unwrap();
        std::fs::rename(&stage, stage.with_extension("retained")).unwrap();
        std::fs::create_dir(&stage).unwrap();
        assert!(layout.stage(&fs, &shard, &stage).is_err());
        assert!(layout.admit_stage(&fs, &shard, &stage).is_err(), "reopen cannot adopt a replacement");
        assert!(files(&stage).is_empty());
    }
}

#[cfg(test)]
mod removed_repair_custody_marker_tests {
    use super::*;

    /// The marker body written before repair custody was removed: the same
    /// fields plus a custody cut.
    #[derive(serde::Serialize)]
    struct OldPayload {
        handoff_id: String,
        retiring_epoch: u64,
        journal_sequence: u64,
        state_fingerprint: String,
        rebind_prefix_proof: crate::DfsMetadataPrefixProof,
        repair_custody: OldCut,
    }

    #[derive(serde::Serialize)]
    struct OldCut {
        version: u32,
        handoff_id: String,
        page_count: u64,
        records: Vec<String>,
    }

    #[derive(serde::Serialize)]
    struct CustodyV1 {
        repair_custody_v1: OldPayload,
    }

    #[derive(serde::Serialize)]
    struct SelectedOriginalDataV2 {
        selected_original_data_v2: OldPayload,
    }

    fn old_payload() -> OldPayload {
        OldPayload {
            handoff_id: "handoff-before-removal".into(),
            retiring_epoch: 3,
            journal_sequence: 17,
            state_fingerprint: "fingerprint".into(),
            rebind_prefix_proof: crate::DfsMetadataPrefixProof {
                version: 2,
                journal_sequence: 17,
                digest: format!("dfs-metadata-prefix-v2:blake3:{}", "a".repeat(64)),
            },
            repair_custody: OldCut {
                version: 1,
                handoff_id: "handoff-before-removal".into(),
                page_count: 2,
                records: vec!["row".into()],
            },
        }
    }

    #[test]
    fn historical_custody_markers_require_migration_and_remain_unchanged() {
        let root = sinomo_testdir::TestDir::new("removed-custody-freeze-marker");
        for (name, bytes) in [
            ("unwrapped", crate::metadata_io::encode_sobj(&old_payload()).unwrap()),
            (
                "custody-v1",
                crate::metadata_io::encode_sobj(&CustodyV1 {
                    repair_custody_v1: old_payload(),
                })
                .unwrap(),
            ),
            (
                "selected-v2",
                crate::metadata_io::encode_sobj(&SelectedOriginalDataV2 {
                    selected_original_data_v2: old_payload(),
                })
                .unwrap(),
            ),
        ] {
            let path = root
                .path()
                .join(name)
                .join("journal/membership-handoff-write-freeze.sobj");
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            assert!(read_handoff_freeze_with_layout(&path, None).is_err(), "{name}");
            assert_eq!(std::fs::read(&path).unwrap(), bytes, "retained historical marker is not rewritten");
        }
    }
}
