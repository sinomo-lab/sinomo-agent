//! Owned, resumable metadata conversion. Existing visibility is imported only
//! after the native host certifies the exact source/current cut.
pub(crate) mod ack_floor;
mod legacy_projection;
mod prior_native;
mod recovery;
mod retained_content;
pub(crate) mod scope_mapping;
pub(crate) mod sobj_cursor;
pub(crate) mod source;
mod source_content;
pub(crate) mod source_document;
pub use source_content::{
    MetadataSourceObjectWitness, MetadataSourceVersionEvidence, lookup_metadata_source_version,
    lookup_owned_metadata_source_version,
};
mod source_owner;
pub(crate) use source_owner::validate_standalone_source_owner_evidence;
pub(crate) mod source_ancestry;
mod source_proof;
mod wire_json;

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinomo_dfs::{
    DfsFileSystemId, DfsMetadataPrefixProof, DfsMetadataShardConfig, DfsMetadataShardId,
    DfsMetadataShardMembershipHandoffProof, DfsNativeTxnCommit, DfsStoragePreparation,
    MetadataBackendError,
};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

type Result<T> = std::result::Result<T, MetadataBackendError>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataImportProposalFacts {
    pub source_root: PathBuf,
    pub source_identity: String,
    pub source_snapshot_fingerprint: String,
    pub source_exact_prefix: Option<DfsMetadataPrefixProof>,
    pub current_image_fingerprint: String,
    pub staged_root: PathBuf,
    pub scope_shard: DfsMetadataShardId,
    pub file_system_ids: Vec<DfsFileSystemId>,
    pub expected_assignments: Vec<DfsMetadataShardConfig>,
    pub target_exact_prefix: Option<DfsMetadataPrefixProof>,
    pub preserved_pending_operations: Vec<Value>,
    pub captured_floor_and_watermark_entries: Vec<Value>,
    pub source_cut: MetadataSourceCut,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeMetadataImportProposal {
    #[serde(flatten)]
    pub facts: MetadataImportProposalFacts,
    pub stage_request: sinomo_core::kv::KvTxnRequest,
    pub expected_handoff: sinomo_dfs::DfsMetadataShardMembershipHandoff,
    pub activation_request: Option<sinomo_core::kv::KvTxnRequest>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StandaloneSourceOwnerEvidence {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub owner_node_id: sinomo_core::node::NodeId,
    pub owner_epoch: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StandaloneMetadataImportProposal {
    #[serde(flatten)]
    pub facts: MetadataImportProposalFacts,
    pub owner_binding: String,
    pub source_owner_evidence: Vec<StandaloneSourceOwnerEvidence>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "authority", content = "proposal", rename_all = "kebab-case")]
pub enum MetadataImportProposal {
    Native(NativeMetadataImportProposal),
    Standalone(StandaloneMetadataImportProposal),
}
#[derive(Clone, Debug, Serialize)]
pub struct MetadataImportCertificateFacts {
    pub(crate) proposal_fingerprint: String,
    pub(crate) current_image_fingerprint: String,
    pub(crate) target_exact_prefix: DfsMetadataPrefixProof,
}
#[derive(Clone, Debug, Serialize)]
pub struct NativeMetadataImportCertificate {
    #[serde(flatten)]
    pub(crate) facts: MetadataImportCertificateFacts,
    pub(crate) stage_commit: DfsNativeTxnCommit,
    pub(crate) activation_commit: DfsNativeTxnCommit,
    pub(crate) joint_handoff_proof: DfsMetadataShardMembershipHandoffProof,
    pub(crate) source_retirements:
        Vec<crate::authority::source_retirement::NativeSourceRetirementCertificate>,
    pub(crate) data_reservation_retirements:
        Vec<crate::authority::source_retirement::NativeDataReservationRetirementCertificate>,
}
#[derive(Clone, Serialize)]
pub struct StandaloneMetadataImportCertificate {
    #[serde(flatten)]
    pub(crate) facts: MetadataImportCertificateFacts,
    #[serde(skip)]
    pub(crate) owner: Arc<crate::authority::standalone::StandaloneStorageImportCapability>,
}
impl std::fmt::Debug for StandaloneMetadataImportCertificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StandaloneMetadataImportCertificate")
            .field("facts", &self.facts)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "authority", content = "certificate", rename_all = "kebab-case")]
pub enum MetadataImportCertificate {
    Native(NativeMetadataImportCertificate),
    Standalone(StandaloneMetadataImportCertificate),
}
/// Exact original control-plane reservation retained as conversion DATA.
/// Its old fence/ACK fields remain in `entry.value`; this never grants author
/// permission. Fresh native import guards must compare that complete entry.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct OriginalMetadataHandoffReservation {
    pub entry: sinomo_core::kv::KvEntry,
    pub retiring_shard: DfsMetadataShardConfig,
    pub desired_shard: DfsMetadataShardConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataSourceCut {
    pub source_root: PathBuf,
    pub source_identity: String,
    pub scope_shard: DfsMetadataShardId,
    pub file_system_ids: Vec<DfsFileSystemId>,
    pub source_snapshot_fingerprint: String,
    /// Actual independently reread source SnapshotHeader; never target format.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_format_version: Option<u16>,
    pub source_exact_prefix: Option<DfsMetadataPrefixProof>,
    /// Conversion projection digest; distinct from the native image identity.
    pub converted_image_fingerprint: String,
    pub current_image_fingerprint: String,
    pub target_exact_prefix: DfsMetadataPrefixProof,
    pub source_journal_sequence: u64,
    pub source_state_fingerprint: String,
    pub projection_assignments: Vec<DfsMetadataShardConfig>,
    #[serde(default)]
    pub original_handoff: Option<OriginalMetadataHandoffReservation>,
    pub staged_root: PathBuf,
    pub pending_operations: Vec<Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MetadataSourceContentLocator {
    pub object_id: String,
    pub version: String,
    pub content_version: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataSourceContentWorkScope {
    pub source_root: PathBuf,
    pub source_identity: String,
    pub source_snapshot_fingerprint: String,
    pub source_journal_sequence: u64,
    pub file_system_ids: Vec<DfsFileSystemId>,
    pub scope_shard: DfsMetadataShardId,
    pub projection_assignments: Vec<DfsMetadataShardConfig>,
    #[serde(default)]
    pub original_handoff: Option<OriginalMetadataHandoffReservation>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataCapturedKvEntry {
    pub key: String,
    pub entry: Option<sinomo_core::kv::KvEntry>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataImportPlanFacts {
    pub expected_assignments: Vec<DfsMetadataShardConfig>,
    pub desired_assignments: Vec<DfsMetadataShardConfig>,
    pub target_exact_prefix: DfsMetadataPrefixProof,
    pub current_image_fingerprint: String,
    pub pending_operation_mapping: Vec<Value>,
    pub combined_layout_source_fences: Vec<Value>,
    pub source_cut: MetadataSourceCut,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeMetadataImportPlan {
    #[serde(flatten)]
    pub facts: MetadataImportPlanFacts,
    pub captured_entries: Vec<MetadataCapturedKvEntry>,
    pub captured_authority: sinomo_core::kv::KvAuthorityProof,
    pub stage_request: sinomo_core::kv::KvTxnRequest,
    pub expected_handoff: sinomo_dfs::DfsMetadataShardMembershipHandoff,
    pub activation_request: Option<sinomo_core::kv::KvTxnRequest>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StandaloneMetadataImportPlan {
    #[serde(flatten)]
    pub facts: MetadataImportPlanFacts,
    pub owner_binding: String,
    pub source_owner_evidence: Vec<StandaloneSourceOwnerEvidence>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "authority", content = "plan", rename_all = "kebab-case")]
pub enum MetadataImportPlan {
    Native(NativeMetadataImportPlan),
    Standalone(StandaloneMetadataImportPlan),
}
macro_rules! import_facts {
    ($facts:ty, $outer:ident, $native:ty, $standalone:ty) => {
        impl std::ops::Deref for $native {
            type Target = $facts;
            fn deref(&self) -> &Self::Target {
                &self.facts
            }
        }
        impl std::ops::Deref for $standalone {
            type Target = $facts;
            fn deref(&self) -> &Self::Target {
                &self.facts
            }
        }
        impl std::ops::Deref for $outer {
            type Target = $facts;
            fn deref(&self) -> &Self::Target {
                match self {
                    Self::Native(value) => &value.facts,
                    Self::Standalone(value) => &value.facts,
                }
            }
        }
    };
}
import_facts!(
    MetadataImportProposalFacts,
    MetadataImportProposal,
    NativeMetadataImportProposal,
    StandaloneMetadataImportProposal
);
import_facts!(
    MetadataImportPlanFacts,
    MetadataImportPlan,
    NativeMetadataImportPlan,
    StandaloneMetadataImportPlan
);
import_facts!(
    MetadataImportCertificateFacts,
    MetadataImportCertificate,
    NativeMetadataImportCertificate,
    StandaloneMetadataImportCertificate
);
impl MetadataImportProposal {
    pub(crate) fn native(&self) -> Option<&NativeMetadataImportProposal> {
        match self {
            Self::Native(value) => Some(value),
            Self::Standalone(_) => None,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataImportAuthorityKind {
    Native,
    Standalone,
}
// Addition to existing metadata.rs; no independent persistence family.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(deny_unknown_fields)]
pub(crate) struct MetadataDataReservationRef {
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) original_shard_id: DfsMetadataShardId,
    pub(crate) capsule_digest: String,
}
pub(crate) fn data_reservation_refs(
    source: &MetadataSourceCut,
) -> Result<Vec<MetadataDataReservationRef>> {
    let mut result = std::collections::BTreeSet::new();
    for row in &source.pending_operations {
        if row["disposition"].as_str() != Some("data-after-original-reservation-retirement") {
            continue;
        }
        if matches!(
            row["capsule_kind"].as_str(),
            Some("placement-job-v0" | "placement-job-v1")
        ) {
            let reservation =
                crate::authority::placement_job_source::OriginalPlacementJob::verify_capsule(row)?;
            if !result.insert(reservation) {
                return Err(error(
                    "placement DATA reservation repeats exact original scope",
                ));
            }
            continue;
        }
        let original = row
            .get("source_row")
            .ok_or_else(|| error("current work reference lacks original capsule"))?;
        crate::legacy_wire::verify_original_namespace_request(original)?;
        if original["status"].as_str() != Some("pending") {
            return Err(error(
                "data reservation has no actual pending source request",
            ));
        }
        let actual = scope_mapping::source_capsule_digest(original)?;
        if row["capsule_digest"].as_str() != Some(actual.as_str()) {
            return Err(error("original data capsule digest changed"));
        }
        let scope = &row["original_source_scope"];
        let reservation = MetadataDataReservationRef {
            file_system_id: serde_json::from_value(scope["file_system_id"].clone())
                .map_err(error)?,
            original_shard_id: serde_json::from_value(scope["shard_id"].clone()).map_err(error)?,
            capsule_digest: actual,
        };
        let declared = crate::legacy_wire::normalize_source_leaf(serde_json::json!({
            "file_system_id": original["file_system_id"],
            "authority_shard_id": original["authority_shard_id"],
        }))?;
        if declared["file_system_id"]
            != serde_json::to_value(&reservation.file_system_id).map_err(error)?
            || declared["authority_shard_id"]
                != serde_json::to_value(&reservation.original_shard_id).map_err(error)?
            || !result.insert(reservation)
        {
            return Err(error(
                "data reservation changes or repeats its exact ORIGINAL scope",
            ));
        }
    }
    Ok(result.into_iter().collect())
}

pub trait MetadataImportAuthority: Send + Sync {
    fn queue_original_topology_after_install(
        &self,
        source: &MetadataSourceCut,
        certificate: &MetadataImportCertificate,
        installed_root: &Path,
    ) -> Result<()>;
    fn queue_published_original_topology(
        &self,
        source: &MetadataSourceCut,
        installed_root: &Path,
    ) -> Result<()>;
    fn original_handoff_reservation(
        &self,
        shard: &DfsMetadataShardId,
        file_system_ids: &[DfsFileSystemId],
    ) -> Result<Option<OriginalMetadataHandoffReservation>>;

    fn resume_imported_namespace_jobs(
        &self,
        source: &MetadataSourceCut,
        certificate: &MetadataImportCertificate,
        installed_root: &Path,
        jobs: &[sinomo_dfs::DfsNamespaceJobRecord],
    ) -> Result<()>;
    fn kind(&self) -> MetadataImportAuthorityKind;
    fn validate_owned_source(
        &self,
        root: &Path,
        scope: &DfsMetadataShardId,
        fs_ids: &[DfsFileSystemId],
    ) -> Result<()>;
    fn pending_original_source_scopes(
        &self,
        source: &MetadataSourceCut,
    ) -> Result<Vec<(DfsFileSystemId, DfsMetadataShardId)>>;
    fn fulfill_retained_content(
        &self,
        source: &MetadataSourceContentWorkScope,
        object: &sinomo_dfs::DfsObjectMetadata,
        version: &sinomo_dfs::DfsObjectVersionRecord,
        locator: &MetadataSourceContentLocator,
        policy: sinomo_dfs::DfsReplicationPolicy,
        targets: Vec<sinomo_core::node::NodeId>,
    ) -> Result<crate::authority::content_work::MetadataImportedRetainedContent>;
    fn source_policy_candidate_count(
        &self,
        source: &MetadataSourceContentWorkScope,
    ) -> Result<usize>;
    fn current_retained_content_placement(
        &self,
        source: &MetadataSourceContentWorkScope,
        object: &sinomo_dfs::DfsObjectMetadata,
        version: &sinomo_dfs::DfsObjectVersionRecord,
    ) -> Result<(
        sinomo_dfs::DfsReplicationPolicy,
        Vec<sinomo_core::node::NodeId>,
    )>;
    fn local_node_id(&self) -> Result<sinomo_core::node::NodeId>;
    fn received(
        &self,
        source: &MetadataSourceContentWorkScope,
        source_exact_prefix: Option<&DfsMetadataPrefixProof>,
    ) -> Result<Option<(MetadataImportProposal, MetadataImportCertificate)>>;
    fn assignments(
        &self,
        file_system_ids: &[DfsFileSystemId],
    ) -> Result<Vec<DfsMetadataShardConfig>>;
    fn plan(&self, source: &MetadataSourceCut) -> Result<MetadataImportPlan>;
    fn certify(&self, proposal: &MetadataImportProposal) -> Result<MetadataImportCertificate>;
    fn certify_recovered(
        &self,
        proposal: &MetadataImportProposal,
        actual_source_root: &Path,
        resolved_image_root: &Path,
        provenance_plan_root: &Path,
    ) -> Result<MetadataImportCertificate>;
    fn received_recovered(
        &self,
        proposal: &MetadataImportProposal,
        actual_source_root: &Path,
        resolved_image_root: &Path,
        provenance_plan_root: &Path,
    ) -> Result<Option<MetadataImportCertificate>>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Phase {
    Source,
    JournalTail,
    Build,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    source: source::SourceCursor,
    events: sinomo_core::storage::migration::stream::StreamCursor,
    phase: Phase,
    tail_after: u64,
    current_import_candidate_count: Option<usize>,
    // The immutable full body remains in the validated active DATA goal.
    // This required nullable field distinguishes current cursors from retained
    // MIG-only cursors which still contain the original complete capsules.
    data_goal_digest: Option<String>,
}

/// Retained bounded MIG progress DATA, never a core/runtime wire variant.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CapsuleProgress {
    source: source::SourceCursor,
    events: sinomo_core::storage::migration::stream::StreamCursor,
    phase: Phase,
    tail_after: u64,
    current_import_candidate_count: Option<usize>,
    #[serde(default)]
    data_reservation_capsules: Vec<Value>,
}

fn progress_for_data_goal(value: Value, goal: Option<&DataGoalStage>) -> Result<Progress> {
    let goal_digest = goal.map(|goal| goal.digest.clone());
    let progress = if value.get("data_goal_digest").is_some() {
        serde_json::from_value::<Progress>(value).map_err(error)?
    } else {
        let retained: CapsuleProgress = serde_json::from_value(value).map_err(error)?;
        let expected = goal.map_or(&[][..], |goal| goal.capsules.as_slice());
        if retained.data_reservation_capsules != expected {
            return Err(error(
                "retained conversion cursor changes its exact DATA goal body",
            ));
        }
        Progress {
            source: retained.source,
            events: retained.events,
            phase: retained.phase,
            tail_after: retained.tail_after,
            current_import_candidate_count: retained.current_import_candidate_count,
            data_goal_digest: goal_digest.clone(),
        }
    };
    if progress.data_goal_digest != goal_digest {
        return Err(error(
            "durable conversion cursor belongs to another DATA goal",
        ));
    }
    Ok(progress)
}
/// Immutable source facts captured before target partitioning/materialization.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct CapturedSourceCut {
    source_identity: String,
    source_snapshot_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_format_version: Option<u16>,
    source_exact_prefix: Option<DfsMetadataPrefixProof>,
    source_journal_sequence: u64,
    source_state_fingerprint: String,
    file_system_ids: Vec<DfsFileSystemId>,
    projection_assignments: Vec<DfsMetadataShardConfig>,
    #[serde(default)]
    original_handoff: Option<OriginalMetadataHandoffReservation>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
enum ConversionEvent {
    Source {
        record: source::SourceRecord,
        point: source::SourceCursor,
    },
    Tail(Value),
}
struct Session {
    _source_lock: Arc<File>,
    source_root: PathBuf,
    destination_root: PathBuf,
    staged_root: PathBuf,
    inventory: source::SourceInventory,
    progress: Progress,
    data_goal: Option<DataGoalStage>,
    replay_cursor: sinomo_core::storage::migration::stream::StreamCursor,
    replayed_events: u64,
    replay_source: source::SourceCursor,
    replay_tail_after: Option<u64>,
    replay_validated: bool,
    current_import_policy_bound: bool,
    projection: legacy_projection::LegacyProjection,
    header: serde_json::Map<String, Value>,
    materialized_through: u64,
    source_head: u64,
    source_prefix: Option<DfsMetadataPrefixProof>,
    source_record_point: Option<source::SourceCursor>,
    source_current_membership: Option<Arc<source_content::MetadataSourceCurrentMembership>>,
    verified_source_points: BTreeMap<Vec<u8>, source::VerifiedRecordPoint>,
    source_content_membership: BTreeMap<
        (
            DfsFileSystemId,
            sinomo_dfs::DfsObjectId,
            sinomo_dfs::DfsVersion,
        ),
        source_content::MetadataSourceVersionMembership,
    >,
    source_object_membership: BTreeMap<
        (DfsFileSystemId, sinomo_dfs::DfsObjectId),
        source_content::MetadataSourceVersionMembership,
    >,
    retained_content_work: BTreeMap<
        (
            DfsFileSystemId,
            sinomo_dfs::DfsObjectId,
            sinomo_dfs::DfsVersion,
        ),
        retained_content::WorkRecord,
    >,
    confirmed_source_content_bindings: std::collections::BTreeSet<(
        DfsFileSystemId,
        sinomo_dfs::DfsObjectId,
        sinomo_dfs::DfsVersion,
    )>,
    journal_sequences: std::collections::BTreeSet<u64>,
    journal_frame_digests: BTreeMap<u64, String>,
}
enum InstalledCompletion {
    Certified(MetadataSourceCut, MetadataImportCertificate, PathBuf),
    Published(MetadataSourceCut, PathBuf),
}
struct RootSlot {
    shard: DfsMetadataShardId,
    session: Mutex<Option<Session>>,
}
impl RootSlot {
    fn lock_session(&self) -> std::sync::MutexGuard<'_, Option<Session>> {
        match self.session.lock() {
            Ok(session) => session,
            Err(poisoned) => {
                let mut session = poisoned.into_inner();
                session.take();
                self.session.clear_poison();
                session
            }
        }
    }
}
/// A component-owned controller; sessions retain the physical source lock,
/// source cut and conversion cursor across Pending and cancellation boundaries.
#[derive(Default)]
pub struct MetadataConverter {
    roots: Mutex<BTreeMap<PathBuf, Arc<RootSlot>>>,
    authority: Option<Arc<dyn MetadataImportAuthority>>,
}
impl MetadataConverter {
    pub fn with_authority(authority: Arc<dyn MetadataImportAuthority>) -> Self {
        Self {
            roots: Mutex::new(BTreeMap::new()),
            authority: Some(authority),
        }
    }
    pub fn prepare(
        &self,
        root: &Path,
        shard: &DfsMetadataShardId,
        budget: usize,
    ) -> Result<DfsStoragePreparation> {
        self.prepare_with_source(root, root, shard, budget)
    }
    pub fn prepare_with_source(
        &self,
        destination: &Path,
        source: &Path,
        shard: &DfsMetadataShardId,
        budget: usize,
    ) -> Result<DfsStoragePreparation> {
        self.prepare_source(destination, source, shard, budget, None, None)
    }

    /// A captured old reservation can coexist with a current local image. Its
    /// retirement still requires the ordinary certified StorageImport path.
    pub(crate) fn prepare_original_reservation(
        &self,
        destination: &Path,
        source: &Path,
        reservation: &OriginalMetadataHandoffReservation,
        budget: usize,
    ) -> Result<DfsStoragePreparation> {
        self.prepare_source(
            destination,
            source,
            &DfsMetadataShardId(reservation.retiring_shard.shard_id.clone()),
            budget,
            Some(reservation),
            None,
        )
    }

    /// Convert per-actual-scope archived DATA through the same bounded source
    /// replay/image/install controller. Capsules carry no current permission.
    pub(crate) fn prepare_metadata_data_reservations(
        &self,
        destination: &Path,
        source: &Path,
        shard: &DfsMetadataShardId,
        capsules: &[Value],
        budget: usize,
    ) -> Result<DfsStoragePreparation> {
        if capsules.is_empty() {
            return Err(error("metadata DATA goal requires actual bound capsules"));
        }
        self.prepare_source(destination, source, shard, budget, None, Some(capsules))
    }

    fn prepare_source(
        &self,
        destination: &Path,
        source: &Path,
        shard: &DfsMetadataShardId,
        budget: usize,
        original_reservation: Option<&OriginalMetadataHandoffReservation>,
        data_capsules: Option<&[Value]>,
    ) -> Result<DfsStoragePreparation> {
        if budget == 0 {
            return Err(error("metadata conversion budget must be positive"));
        }
        let destination = canonical_destination(destination)?;
        // No filesystem work runs under the shared directory mutex. A stable
        // slot also serializes waiting aliases across errors and final rename.
        let slot = {
            let mut roots = self.roots.lock().unwrap_or_else(|error| error.into_inner());
            Arc::clone(roots.entry(destination.clone()).or_insert_with(|| {
                Arc::new(RootSlot {
                    shard: shard.clone(),
                    session: Mutex::new(None),
                })
            }))
        };
        if slot.shard != *shard {
            return Err(error(
                "one physical metadata destination was requested under different shard identities",
            ));
        }
        let mut session = slot.lock_session();
        // Resolve both persisted stage roles once under this destination's
        // custody. The exact validated DATA is local to this bounded call.
        let goals = (|| {
            let original = read_data_goal(
                &unversioned_conversion_stage_base_path(&destination, shard)?,
                &destination,
                shard,
            )?;
            let current = read_data_goal(
                &conversion_stage_base_path(&destination, shard)?,
                &destination,
                shard,
            )?
            .or_else(|| original.clone());
            Ok::<_, MetadataBackendError>((original, current))
        })();
        let (original_goal, mut data_goal) = match goals {
            Ok(goals) => goals,
            Err(error) => {
                // These reads previously ran inside the inner prepare call,
                // whose error path drops this same warm source cursor.
                session.take();
                return Err(error);
            }
        };
        let mut waiting_for_data_goal = false;
        if let Some(capsules) = data_capsules {
            let (bound, reservations) = validated_placement_capsules(capsules, shard)?;
            let ids = reservations
                .iter()
                .map(|reservation| reservation.file_system_id.clone())
                .collect::<std::collections::BTreeSet<_>>();
            self.authority
                .as_ref()
                .ok_or_else(|| error("DATA goal requires actual native owner"))?
                .validate_owned_source(source, shard, &ids.into_iter().collect::<Vec<_>>())?;
            if !select_data_goal(
                &destination,
                shard,
                bound,
                session.is_some(),
                &mut data_goal,
            )? {
                // Drive the previous goal through the SAME controller/cursor.
                // Completing it cannot report Ready for this requested new goal.
                waiting_for_data_goal = true;
            }
        }
        let replayed_events_before = session.as_ref().map(|state| state.replayed_events);
        tracing::trace!(
            target: "sinomo.dfs.perf",
            converter = ?(self as *const Self),
            slot = ?Arc::as_ptr(&slot),
            destination = %destination.display(),
            source = %source.display(),
            shard = %shard.0,
            budget,
            data_goal_digest = ?data_goal.as_ref().map(|goal| goal.digest.as_str()),
            waiting_for_data_goal,
            warm_session = session.is_some(),
            replayed_events = ?replayed_events_before,
            durable_events = ?session.as_ref().map(|state| state.progress.events.records),
            phase = ?session.as_ref().map(|state| &state.progress.phase),
            "migration bounded metadata prepare started"
        );
        let mut completion = None;
        let result = self.prepare_with_source_inner(
            &destination,
            source,
            shard,
            budget,
            &mut session,
            original_reservation,
            data_goal.as_ref(),
            original_goal.as_ref(),
            &mut completion,
        );
        tracing::trace!(
            target: "sinomo.dfs.perf",
            converter = ?(self as *const Self),
            slot = ?Arc::as_ptr(&slot),
            destination = %destination.display(),
            shard = %shard.0,
            data_goal_digest = ?data_goal.as_ref().map(|goal| goal.digest.as_str()),
            replayed_events_before = ?replayed_events_before,
            replayed_events_after = ?session.as_ref().map(|state| state.replayed_events),
            durable_events = ?session.as_ref().map(|state| state.progress.events.records),
            phase = ?session.as_ref().map(|state| &state.progress.phase),
            replay_validated = ?session.as_ref().map(|state| state.replay_validated),
            retained_content_work = ?session.as_ref().map(|state| state.retained_content_work.len()),
            confirmed_source_bindings = ?session.as_ref().map(|state| state.confirmed_source_content_bindings.len()),
            outcome = ?result,
            reset_session = result.is_err() || matches!(&result, Ok(DfsStoragePreparation::Ready)),
            "migration bounded metadata prepare returned"
        );
        if result.is_err() || matches!(&result, Ok(DfsStoragePreparation::Ready)) {
            // Reset under the SAME per-root lock. Queued aliases can only
            // reconstruct the last durable cursor, never reuse partial state.
            session.take();
        }
        drop(session);
        if matches!(&result, Ok(DfsStoragePreparation::Ready)) {
            if let Some(completion) = completion {
                let authority = self
                    .authority
                    .as_ref()
                    .ok_or_else(|| error("completed topology needs actual owner"))?;
                match completion {
                    InstalledCompletion::Certified(source, certificate, root) => authority
                        .queue_original_topology_after_install(&source, &certificate, &root)?,
                    InstalledCompletion::Published(source, root) => {
                        authority.queue_published_original_topology(&source, &root)?
                    }
                }
            }
        }
        if waiting_for_data_goal && matches!(&result, Ok(DfsStoragePreparation::Ready)) {
            Ok(DfsStoragePreparation::Pending {
                processed_records: 0,
            })
        } else {
            result
        }
    }
    fn prepare_with_source_inner(
        &self,
        destination: &Path,
        source: &Path,
        shard: &DfsMetadataShardId,
        budget: usize,
        warm_session: &mut Option<Session>,
        original_reservation: Option<&OriginalMetadataHandoffReservation>,
        data_goal: Option<&DataGoalStage>,
        original_goal: Option<&DataGoalStage>,
        completion: &mut Option<InstalledCompletion>,
    ) -> Result<DfsStoragePreparation> {
        if budget == 0 {
            return Err(error("metadata conversion budget must be positive"));
        }
        let previous = stage_path_for_goal(
            unversioned_conversion_stage_base_path(destination, shard)?,
            original_goal,
        );
        if let Some(interrupted) =
            recovery::resolve_interrupted_installation(destination, &previous)?
        {
            let authority = self
                .authority
                .as_ref()
                .ok_or_else(|| error("prior installation requires actual native authority"))?;
            warm_session.take();
            let session = open_session_at_stage(
                destination,
                &interrupted.actual_source,
                shard,
                previous,
                original_goal.cloned(),
            )?;
            validate_retained_source_identity(
                &interrupted.actual_source,
                &interrupted.source_identity,
                &interrupted.source_snapshot_fingerprint,
            )?;
            let certificate = if let Some(certificate) = authority.received_recovered(
                &interrupted.proposal,
                &interrupted.actual_source,
                &interrupted.resolved_image_root,
                &interrupted.provenance_plan_root,
            )? {
                certificate
            } else {
                let local = authority.local_node_id()?;
                if interrupted.proposal.native().is_some_and(|native| {
                    native.expected_handoff.retiring_shard.primary_node_id != local
                }) {
                    return Ok(DfsStoragePreparation::Pending {
                        processed_records: 0,
                    });
                }
                authority.certify_recovered(
                    &interrupted.proposal,
                    &interrupted.actual_source,
                    &interrupted.resolved_image_root,
                    &interrupted.provenance_plan_root,
                )?
            };
            validate_certificate(&interrupted.proposal, &certificate)?;
            install_certified_stage(
                &session,
                &interrupted.proposal,
                &certificate,
                authority.as_ref(),
            )?;
            // The ORIGINAL native exchange physically finished. Its exact8
            // image is still migration DATA; the next bounded call imports9.
            // No current readiness or job execution is inferred from this step.
            return Ok(DfsStoragePreparation::Pending {
                processed_records: 1,
            });
        }
        let conversion_stage =
            stage_path_for_goal(conversion_stage_base_path(destination, shard)?, data_goal);
        if let Some(interrupted) =
            recovery::resolve_interrupted_installation(destination, &conversion_stage)?
        {
            let authority = self
                .authority
                .as_ref()
                .ok_or_else(|| error("interrupted import requires actual native authority"))?;
            warm_session.take();
            let session = open_session_at_stage(
                destination,
                &interrupted.actual_source,
                shard,
                conversion_stage.clone(),
                data_goal.cloned(),
            )?;
            validate_retained_source_identity(
                &interrupted.actual_source,
                &interrupted.source_identity,
                &interrupted.source_snapshot_fingerprint,
            )?;
            let certificate = if let Some(certificate) = authority.received_recovered(
                &interrupted.proposal,
                &interrupted.actual_source,
                &interrupted.resolved_image_root,
                &interrupted.provenance_plan_root,
            )? {
                certificate
            } else {
                let local = authority.local_node_id()?;
                if interrupted.proposal.native().is_some_and(|native| {
                    native.expected_handoff.retiring_shard.primary_node_id != local
                }) {
                    return Ok(DfsStoragePreparation::Pending {
                        processed_records: 0,
                    });
                }
                authority.certify_recovered(
                    &interrupted.proposal,
                    &interrupted.actual_source,
                    &interrupted.resolved_image_root,
                    &interrupted.provenance_plan_root,
                )?
            };
            validate_certificate(&interrupted.proposal, &certificate)?;
            install_certified_stage(
                &session,
                &interrupted.proposal,
                &certificate,
                authority.as_ref(),
            )?;
            *completion = Some(InstalledCompletion::Certified(
                interrupted.proposal.source_cut.clone(),
                certificate,
                destination.to_owned(),
            ));
            return Ok(DfsStoragePreparation::Ready);
        }
        let goal_capsules = data_goal.map_or(&[][..], |goal| goal.capsules.as_slice());
        if original_reservation.is_none()
            && current_root(destination)?
            && (goal_capsules.is_empty()
                || published_data_goal_matches(destination, shard, goal_capsules)?)
        {
            let marker = destination.join("metadata-import-v7.json");
            if marker.is_file() {
                let raw: Value = read_json(&marker)?;
                let cut: MetadataSourceCut = serde_json::from_value(
                    raw.get("source_cut")
                        .cloned()
                        .ok_or_else(|| error("current import marker lacks original cut"))?,
                )
                .map_err(error)?;
                if cut.scope_shard != *shard {
                    return Err(error("published cut has another scope"));
                }
                if cut.original_handoff.is_some()
                    || cut
                        .pending_operations
                        .iter()
                        .any(|row| row.get("prior_native_topology").is_some())
                {
                    *completion = Some(InstalledCompletion::Published(cut, destination.to_owned()));
                }
            }
            return Ok(DfsStoragePreparation::Ready);
        }
        let discovered = if (original_reservation.is_some() || !goal_capsules.is_empty())
            && current_root(destination)?
        {
            Some(destination.to_owned())
        } else {
            discover_metadata_source(destination, source)?
        };
        let Some(source) = discovered else {
            return Ok(
                if original_reservation.is_some() || !goal_capsules.is_empty() {
                    // Absence cannot certify an original reservation. A genuine
                    // receiver image/certificate must resolve this physical scope.
                    DfsStoragePreparation::Pending {
                        processed_records: 0,
                    }
                } else {
                    DfsStoragePreparation::Ready
                },
            );
        };
        let authority = self.authority.as_ref().ok_or_else(|| {
            error("retained metadata conversion requires actual native import authority")
        })?;
        if warm_session.is_none() {
            *warm_session = Some(open_session_at_stage(
                destination,
                &source,
                shard,
                conversion_stage,
                data_goal.cloned(),
            )?);
        }
        let session = warm_session
            .as_mut()
            .expect("initialized per-root conversion session");
        if session.data_goal.as_ref() != data_goal
            || session.progress.data_goal_digest != data_goal.map(|goal| goal.digest.clone())
        {
            return Err(error("warm source cursor belongs to another DATA goal"));
        }
        if session.source_root != source {
            session.inventory.verify(&source)?;
            register_source_fence(&source, &session._source_lock)?;
            session.source_root = source.clone();
        }
        if !session.current_import_policy_bound {
            // This is a NEW current-policy decoder context, not evidence of
            // historical source W/N. Original source rows stay byte-exact.
            let context = MetadataSourceContentWorkScope {
                source_root: session.source_root.clone(),
                source_identity: session.inventory.fingerprint.clone(),
                source_snapshot_fingerprint: session
                    .inventory
                    .snapshot_fingerprint()
                    .unwrap_or(&session.inventory.fingerprint)
                    .to_owned(),
                source_journal_sequence: session.source_head,
                file_system_ids: Vec::new(),
                scope_shard: shard.clone(),
                projection_assignments: Vec::new(),
                original_handoff: None,
            };
            let count = authority.source_policy_candidate_count(&context)?;
            if count == 0 {
                // The normal host may still be publishing its first sampled
                // advertisement. Retain this same source/controller without
                // binding fabricated eligibility or current policy defaults.
                return Ok(DfsStoragePreparation::Pending {
                    processed_records: 0,
                });
            }
            if session
                .progress
                .current_import_candidate_count
                .is_some_and(|old| old.clamp(1, 2) != count.clamp(1, 2))
            {
                return Err(error(
                    "current import policy defaults changed during immutable conversion",
                ));
            }
            session.projection.bind_current_import_policy(count)?;
            session.progress.current_import_candidate_count = Some(count);
            session.current_import_policy_bound = true;
            save_progress(session)?;
        }
        let mut processed = 0;
        // Restart reconstruction is bounded by the same request budget. Warm
        // sessions retain the ordinary current projection in memory.
        while processed < budget && session.replayed_events < session.progress.events.records {
            let mut reader =
                sinomo_core::storage::migration::stream::MigrationStreamReader::resume(
                    &session.staged_root.join("events.slog"),
                    &session.replay_cursor,
                )
                .map_err(error)?;
            if reader.header != event_header(session)? {
                return Err(error(
                    "durable conversion event stream changed its source header",
                ));
            }
            let bytes = reader
                .next_record(64 * 1024 * 1024)
                .map_err(error)?
                .ok_or_else(|| error("durable conversion event stream ended before its cursor"))?;
            let event: ConversionEvent =
                sinomo_core::object::serde_ext::from_bytes(&bytes).map_err(error)?;
            validate_replayed_event(session, &event)?;
            apply_event(session, &event)?;
            session.replay_cursor = reader.cursor().clone();
            session.replayed_events += 1;
            processed += 1;
        }
        if session.replayed_events < session.progress.events.records {
            return Ok(DfsStoragePreparation::Pending {
                processed_records: processed,
            });
        }
        if !session.replay_validated {
            if !matches!(session.progress.phase, Phase::Source)
                && source::next(&session.source_root, &mut session.replay_source)?.is_some()
            {
                return Err(error("conversion event ledger omits actual source rows"));
            }
            if serde_json::to_value(session.progress.source.point()).map_err(error)?
                != serde_json::to_value(session.replay_source.point()).map_err(error)?
            {
                return Err(error(
                    "saved source cursor does not follow verified actual source events",
                ));
            }
            session.replay_validated = true;
        }
        while processed < budget {
            match session.progress.phase {
                Phase::Source => {
                    let root = session.source_root.clone();
                    let mut cursor = std::mem::take(&mut session.progress.source);
                    let point = cursor.point();
                    if let Some(record) = source::next(&root, &mut cursor)? {
                        append_event(session, ConversionEvent::Source { record, point })?;
                        session.progress.source = cursor;
                        save_progress(&session)?;
                        processed += 1;
                    } else {
                        session.progress.source = cursor;
                        validate_source_snapshot(&session)?;
                        session.progress.tail_after = session.materialized_through;
                        session.progress.phase = Phase::JournalTail;
                        save_progress(&session)?;
                    }
                }
                Phase::JournalTail => {
                    let next = session
                        .journal_sequences
                        .range((
                            std::ops::Bound::Excluded(session.progress.tail_after),
                            std::ops::Bound::Unbounded,
                        ))
                        .next()
                        .copied();
                    if let Some(sequence) = next {
                        if sequence
                            != session
                                .progress
                                .tail_after
                                .checked_add(1)
                                .ok_or_else(|| error("source journal sequence overflow"))?
                        {
                            return Err(error(
                                "retained source journal has an uncovered sequence gap",
                            ));
                        }
                        let entry = read_verified_journal_evidence(
                            &session.staged_root,
                            sequence,
                            &session.journal_frame_digests,
                        )?;
                        append_event(session, ConversionEvent::Tail(entry))?;
                        session.progress.tail_after = sequence;
                        save_progress(&session)?;
                        processed += 1;
                    } else {
                        if session.source_head > session.progress.tail_after {
                            return Err(error("retained journal head names missing source data"));
                        }
                        session.inventory.verify(&session.source_root)?;
                        session.progress.phase = Phase::Build;
                        save_progress(&session)?;
                    }
                }
                Phase::Build => break,
            }
        }
        if !matches!(session.progress.phase, Phase::Build) {
            return Ok(DfsStoragePreparation::Pending {
                processed_records: processed,
            });
        }
        let ids = session.projection.source_file_system_ids()?;
        authority.validate_owned_source(&session.source_root, shard, &ids)?;
        let authority_kind = authority.kind();
        let retained_cut_path = session.staged_root.join("source-cut.json");
        let captured_path = session.staged_root.join("captured-source-cut.json");
        let assignments = if captured_path.is_file() {
            let captured: CapturedSourceCut = read_json(&captured_path)?;
            captured.projection_assignments
        } else if retained_cut_path.is_file() {
            let retained: MetadataSourceCut = read_json(&retained_cut_path)?;
            if retained.source_identity != session.inventory.fingerprint {
                return Err(error("retained blueprint names another source cut"));
            }
            retained.projection_assignments
        } else {
            authority.assignments(&ids)?
        };
        if authority_kind == MetadataImportAuthorityKind::Native
            && !assignments.iter().any(|cfg| cfg.shard_id == shard.0)
        {
            return Err(error(
                "conversion target is absent from the captured native assignments",
            ));
        }
        let original_handoff = if captured_path.is_file() {
            read_json::<CapturedSourceCut>(&captured_path)?.original_handoff
        } else if retained_cut_path.is_file() {
            read_json::<MetadataSourceCut>(&retained_cut_path)?.original_handoff
        } else {
            authority.original_handoff_reservation(shard, &ids)?
        };
        if let Some(expected) = original_reservation {
            if original_handoff.as_ref() != Some(expected) {
                return Err(error(
                    "captured source does not retain the exact requested original reservation",
                ));
            }
        }
        let actual_source_format = session
            .header
            .get("format_version")
            .map(|value| {
                let format = value
                    .as_u64()
                    .ok_or_else(|| error("actual source header format is not u64"))?;
                u16::try_from(format).map_err(error)
            })
            .transpose()?;
        let captured = CapturedSourceCut {
            source_identity: session.inventory.fingerprint.clone(),
            source_snapshot_fingerprint: session
                .inventory
                .snapshot_fingerprint()
                .unwrap_or(&session.inventory.fingerprint)
                .to_owned(),
            source_format_version: {
                let recorded = if captured_path.is_file() {
                    read_json::<CapturedSourceCut>(&captured_path)?.source_format_version
                } else if retained_cut_path.is_file() {
                    read_json::<MetadataSourceCut>(&retained_cut_path)?.source_format_version
                } else {
                    actual_source_format
                };
                // Older None remains hash-compatible; a recorded Some must be
                // the exact independently reread actual source header fact.
                if recorded.is_some() && recorded != actual_source_format {
                    return Err(error(
                        "captured source format differs from actual verified header",
                    ));
                }
                recorded
            },
            source_exact_prefix: session.source_prefix.clone(),
            source_journal_sequence: session.source_head.max(session.materialized_through),
            source_state_fingerprint: source_state_fingerprint(&session)?,
            file_system_ids: ids.clone(),
            projection_assignments: assignments.clone(),
            original_handoff: original_handoff.clone(),
        };
        if captured_path.is_file() {
            let retained: CapturedSourceCut = read_json(&captured_path)?;
            if retained != captured {
                return Err(error(
                    "source facts changed before target partition/materialization",
                ));
            }
        } else {
            write_json(&captured_path, &captured)?;
        }
        let work_scope = MetadataSourceContentWorkScope {
            source_root: session.source_root.clone(),
            source_identity: captured.source_identity.clone(),
            source_snapshot_fingerprint: captured.source_snapshot_fingerprint.clone(),
            source_journal_sequence: captured.source_journal_sequence,
            file_system_ids: captured.file_system_ids.clone(),
            scope_shard: shard.clone(),
            projection_assignments: captured.projection_assignments.clone(),
            original_handoff: captured.original_handoff.clone(),
        };
        session.projection.bind_scope(shard.clone())?;
        let stage = session.staged_root.clone();
        let digests = &session.journal_frame_digests;
        session
            .projection
            .resolve_deferred(&|seq| read_verified_journal_document(&stage, seq, digests))?;
        if session
            .source_current_membership
            .as_ref()
            .is_none_or(|membership| !membership.matches_scope(&work_scope))
        {
            let membership = source_content::capture_current_membership(session, &work_scope)?;
            register_source_current_membership(&session.source_root, &membership)?;
            session.source_current_membership = Some(membership);
            source_content::publish(session, &captured)?;
        }
        if let Some((proposal, certificate)) =
            authority.received(&work_scope, captured.source_exact_prefix.as_ref())?
        {
            validate_certificate(&proposal, &certificate)?;
            require_placement_goal_refs(&proposal.source_cut, &goal_capsules)?;
            session.inventory.verify(&session.source_root)?;
            install_certified_stage(session, &proposal, &certificate, authority.as_ref())?;
            *completion = Some(InstalledCompletion::Certified(
                proposal.source_cut.clone(),
                certificate,
                destination.to_owned(),
            ));
            return Ok(DfsStoragePreparation::Ready);
        }
        let local = authority.local_node_id()?;
        if authority_kind == MetadataImportAuthorityKind::Native
            && !assignments
                .iter()
                .any(|cfg| cfg.shard_id == shard.0 && cfg.primary_node_id == local)
        {
            return Ok(DfsStoragePreparation::Pending {
                processed_records: processed,
            });
        }
        let (mut tables, mut pending) = match authority_kind {
            MetadataImportAuthorityKind::Native => {
                partition_projection(session, shard, &assignments)?
            }
            MetadataImportAuthorityKind::Standalone => {
                if !assignments.is_empty() {
                    return Err(error(
                        "explicit standalone import cannot carry native assignments",
                    ));
                }
                let stage = session.staged_root.clone();
                let digests = &session.journal_frame_digests;
                session.projection.resolve_deferred(&|seq| {
                    read_verified_journal_document(&stage, seq, digests)
                })?;
                let mut tables = session.projection.current_tables()?;
                let mut pending = session.projection.pending_operations()?;
                let (mappings, unresolved) = scope_mapping::resolve_standalone_adoption_work(
                    &mut tables,
                    &mut pending,
                    session.projection.required_native_work(),
                    shard,
                    &local,
                )?;
                pending.extend(mappings);
                for work in unresolved {
                    pending.push(serde_json::json!({"disposition":"requires-native-row-work",
                        "family":work.family,"reason":work.reason,"source_row":work.source_row}));
                }
                for job in &tables.namespace_jobs {
                    if job.status == sinomo_dfs::DfsNamespaceJobStatus::Running {
                        pending.push(
                            serde_json::json!({"disposition":"restored-current-job-data",
                            "current_job_data":job,"current_effects_authored":false}),
                        );
                    }
                }
                (tables, pending)
            }
        };
        let mut remaining = budget.saturating_sub(processed);
        if !retained_content::fulfill(
            session,
            &work_scope,
            authority.as_ref(),
            &mut tables,
            &mut remaining,
        )? {
            return Ok(DfsStoragePreparation::Pending {
                processed_records: budget - remaining,
            });
        }
        processed = budget - remaining;
        if session.header.get("format_version").and_then(Value::as_u64) == Some(8) {
            if let Some(topology) = prior_native::topology_data(&session.source_root, shard)? {
                pending.push(topology);
            }
        }
        pending.extend(
            session
                .data_goal
                .as_ref()
                .into_iter()
                .flat_map(|goal| goal.capsules.iter().cloned()),
        );
        let (identity, converted_fingerprint, pending_mapping) =
            build_native_image(&session, shard, tables, pending)?;
        let mut cut = MetadataSourceCut {
            source_root: session.source_root.clone(),
            source_identity: captured.source_identity,
            scope_shard: shard.clone(),
            file_system_ids: captured.file_system_ids,
            source_snapshot_fingerprint: captured.source_snapshot_fingerprint,
            source_format_version: captured.source_format_version,
            source_exact_prefix: captured.source_exact_prefix,
            converted_image_fingerprint: converted_fingerprint,
            current_image_fingerprint: identity.state_fingerprint.clone(),
            target_exact_prefix: identity.prefix_proof.clone(),
            source_journal_sequence: captured.source_journal_sequence,
            source_state_fingerprint: captured.source_state_fingerprint,
            projection_assignments: captured.projection_assignments,
            staged_root: session.staged_root.join("current-image"),
            pending_operations: pending_mapping,
            original_handoff: captured.original_handoff,
        };
        require_placement_goal_refs(&cut, goal_capsules)?;
        let cut_path = session.staged_root.join("source-cut.json");
        if cut_path.is_file() {
            let retained: MetadataSourceCut = read_json(&cut_path)?;
            if retained.source_identity != session.inventory.fingerprint
                || retained.current_image_fingerprint != identity.state_fingerprint
                || retained.target_exact_prefix != identity.prefix_proof
            {
                return Err(error(
                    "retained import blueprint differs from the immutable source/candidate",
                ));
            }
            require_placement_goal_refs(&retained, &goal_capsules)?;
            cut = retained;
        } else {
            write_json(&cut_path, &cut)?;
        }
        let dependencies = authority.pending_original_source_scopes(&cut)?;
        if !dependencies.is_empty() {
            for (fs, original) in dependencies {
                if !cut
                    .projection_assignments
                    .iter()
                    .any(|config| config.file_system_id == fs && config.shard_id == original.0)
                {
                    return Err(error(
                        "original source scope is absent from actual captured assignments",
                    ));
                }
                // The provider's normal metadata store layout is indexed by
                // the actual configured shard ID, never an inferred label.
                if std::path::Path::new(&original.0).components().count() != 1
                    || matches!(
                        std::path::Path::new(&original.0).components().next(),
                        Some(std::path::Component::ParentDir | std::path::Component::CurDir)
                    )
                {
                    return Err(error(
                        "configured original scope is not an owned metadata path segment",
                    ));
                }
                let original_root = destination
                    .parent()
                    .ok_or_else(|| error("metadata scope has no owning store root"))?
                    .join(&original.0);
                let key = canonical_destination(&original_root)?;
                let busy = self
                    .roots
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(&key)
                    .is_some_and(|slot| match slot.session.try_lock() {
                        Ok(_) => false,
                        Err(std::sync::TryLockError::WouldBlock) => true,
                        Err(std::sync::TryLockError::Poisoned(_)) => false,
                    });
                if !busy {
                    let _ = self.prepare_with_source(
                        &original_root,
                        &session.source_root,
                        &original,
                        budget,
                    )?;
                }
            }
            return Ok(DfsStoragePreparation::Pending {
                processed_records: processed,
            });
        }
        let plan = authority.plan(&cut)?;
        require_placement_goal_refs(&plan.source_cut, &goal_capsules)?;
        if plan.current_image_fingerprint != cut.current_image_fingerprint
            || plan.target_exact_prefix != cut.target_exact_prefix
            || plan.expected_assignments != cut.projection_assignments
        {
            return Err(error(
                "native import plan changes the completed candidate identity or routing cut",
            ));
        }
        let facts = MetadataImportProposalFacts {
            source_root: cut.source_root.clone(),
            source_identity: cut.source_identity.clone(),
            source_snapshot_fingerprint: cut.source_snapshot_fingerprint.clone(),
            source_exact_prefix: cut.source_exact_prefix.clone(),
            current_image_fingerprint: cut.current_image_fingerprint.clone(),
            staged_root: cut.staged_root.clone(),
            scope_shard: shard.clone(),
            file_system_ids: cut.file_system_ids.clone(),
            expected_assignments: plan.expected_assignments.clone(),
            target_exact_prefix: Some(plan.target_exact_prefix.clone()),
            preserved_pending_operations: plan.pending_operation_mapping.clone(),
            captured_floor_and_watermark_entries: match &plan {
                MetadataImportPlan::Native(native) => native
                    .captured_entries
                    .iter()
                    .map(serde_json::to_value)
                    .collect::<std::result::Result<_, _>>()
                    .map_err(error)?,
                MetadataImportPlan::Standalone(_) => Vec::new(),
            },
            source_cut: plan.source_cut.clone(),
        };
        let proposal = match plan {
            MetadataImportPlan::Native(native) => {
                MetadataImportProposal::Native(NativeMetadataImportProposal {
                    facts,
                    stage_request: native.stage_request,
                    expected_handoff: native.expected_handoff,
                    activation_request: native.activation_request,
                })
            }
            MetadataImportPlan::Standalone(local) => {
                MetadataImportProposal::Standalone(StandaloneMetadataImportProposal {
                    facts,
                    owner_binding: local.owner_binding,
                    source_owner_evidence: local.source_owner_evidence,
                })
            }
        };
        write_json(&session.staged_root.join("import-proposal.json"), &proposal)?;
        let certificate = authority.certify(&proposal)?;
        validate_certificate(&proposal, &certificate)?;
        session.inventory.verify(&session.source_root)?;
        install_certified_stage(&session, &proposal, &certificate, authority.as_ref())?;
        *completion = Some(InstalledCompletion::Certified(
            proposal.source_cut.clone(),
            certificate,
            destination.to_owned(),
        ));
        Ok(DfsStoragePreparation::Ready)
    }
}
struct SourceCustody {
    fence: std::sync::Weak<File>,
    memberships: BTreeMap<
        DfsMetadataShardId,
        std::sync::Weak<source_content::MetadataSourceCurrentMembership>,
    >,
}
static SOURCE_FENCES: std::sync::OnceLock<Mutex<BTreeMap<PathBuf, SourceCustody>>> =
    std::sync::OnceLock::new();
fn register_source_fence(root: &Path, file: &Arc<File>) -> Result<()> {
    let key = root.canonicalize().map_err(error)?;
    SOURCE_FENCES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry(key)
        .and_modify(|custody| custody.fence = Arc::downgrade(file))
        .or_insert_with(|| SourceCustody {
            fence: Arc::downgrade(file),
            memberships: BTreeMap::new(),
        });
    Ok(())
}
fn register_source_current_membership(
    root: &Path,
    membership: &Arc<source_content::MetadataSourceCurrentMembership>,
) -> Result<()> {
    let key = root.canonicalize().map_err(error)?;
    let mut custody = SOURCE_FENCES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let source = custody
        .get_mut(&key)
        .ok_or_else(|| error("actual current source membership lacks its owned writer fence"))?;
    if source.fence.upgrade().is_none() {
        return Err(error(
            "actual current source membership outlived writer custody",
        ));
    }
    source.memberships.retain(|_, view| view.strong_count() > 0);
    source
        .memberships
        .insert(membership.scope_shard().clone(), Arc::downgrade(membership));
    Ok(())
}
fn source_current_membership(
    root: &Path,
    scope: &MetadataSourceContentWorkScope,
) -> Result<Arc<source_content::MetadataSourceCurrentMembership>> {
    let key = root.canonicalize().map_err(error)?;
    let membership = SOURCE_FENCES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&key)
        .and_then(|source| source.memberships.get(&scope.scope_shard))
        .and_then(std::sync::Weak::upgrade)
        .ok_or_else(|| {
            error("actual source membership is unavailable before owned reconstruction")
        })?;
    if !membership.matches_scope(scope) {
        return Err(error(
            "owned current source membership describes a different immutable cut",
        ));
    }
    Ok(membership)
}
/// The real completed image location belonging to this owned source cut. A
/// caller cannot choose a stage path or create a plan through this accessor.
pub fn owned_metadata_source_stage(source: &MetadataSourceContentWorkScope) -> Result<PathBuf> {
    let membership = source_current_membership(&source.source_root, source)?;
    let cut: MetadataSourceCut = read_json(&membership.stage_root().join("source-cut.json"))?;
    if cut.source_identity != source.source_identity
        || cut.source_snapshot_fingerprint != source.source_snapshot_fingerprint
        || cut.source_journal_sequence != source.source_journal_sequence
        || cut.scope_shard != source.scope_shard
        || cut.file_system_ids != source.file_system_ids
        || cut.projection_assignments != source.projection_assignments
        || cut.staged_root != membership.stage_root().join("current-image")
    {
        return Err(error(
            "completed native stage does not belong to the actual owned source cut",
        ));
    }
    Ok(cut.staged_root)
}
/// Retain the actual converter writer fence across a self or peer stage RPC.
/// The registry carries only weak custody; it cannot manufacture ownership.
pub(crate) fn retain_source_fence(root: &Path) -> Result<Arc<File>> {
    let key = root.canonicalize().map_err(error)?;
    if let Some(file) = SOURCE_FENCES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
        .and_then(|custody| custody.fence.upgrade())
    {
        return Ok(file);
    }
    let path = root.join("journal/write.lock");
    fs::create_dir_all(
        path.parent()
            .ok_or_else(|| error("source writer fence has no parent"))?,
    )
    .map_err(error)?;
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(error)?;
    match file.try_lock_exclusive() {
        Ok(()) => {}
        Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => {
            return Err(MetadataBackendError::StoragePreparationPending {
                root: key,
                processed_records: 0,
            });
        }
        Err(source) => return Err(error(source)),
    }
    let file = Arc::new(file);
    register_source_fence(root, &file)?;
    Ok(file)
}
fn error(e: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(e.to_string())
}
fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    serde_json::from_slice(&fs::read(path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })?)
    .map_err(error)
}
fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    sinomo_core::storage::durable_file::write_file_atomic(
        path,
        &serde_json::to_vec(value).map_err(error)?,
        sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("import", "tmp")
            .sync_all(),
    )
    .map_err(error)
}
/// Prior native generations keep their own exact clocks while being imported.
/// This classifier is conversion DATA, never admission to the current DFS reader.
pub(crate) fn native_source_format(format: Option<u16>) -> bool {
    matches!(format, Some(8)) || format == Some(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION)
}
pub(crate) fn native_source_root(root: &Path, format: Option<u16>) -> Result<bool> {
    let Some(format) = format.filter(|format| native_source_format(Some(*format))) else {
        return Ok(false);
    };
    if format == sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION {
        current_root(root)
    } else {
        metadata_root_at_format(root, format)
    }
}
pub(crate) fn inspect_import_candidate(
    root: &Path,
) -> Result<sinomo_dfs::DfsMetadataCandidateIdentity> {
    if native_source_root(root, Some(8))? {
        prior_native::inspect(root)
    } else {
        sinomo_dfs::inspect_metadata_candidate(root)
    }
}
pub(crate) fn decode_import_payload_data(
    value: Value,
) -> Result<sinomo_dfs::DfsPreparedOperationPayload> {
    prior_native::payload_data(value)
}
pub(crate) fn current_root(root: &Path) -> Result<bool> {
    let snapshot_present = root.join("records/tables/snapshot.sobj").exists();
    let snapshot_current = snapshot_present
        && metadata_root_at_format(root, sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION)?;
    // A normal new replica can materialize current journal/table frames before
    // its first checkpoint snapshot. Read the actual declaration; never try a
    // legacy decoder after a failed current reader or infer health from a path.
    let mut current = false;
    let mut legacy = false;
    for relative in [
        "records/tables/materialized.checkpoint.slog",
        "records/tables/materialized.slog",
    ] {
        let path = root.join(relative);
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(MetadataBackendError::Io { path, source }),
        };
        let length = file.metadata().map_err(error)?.len();
        let record = sinomo_core::storage::durable_file::read_length_prefixed_record(
            &mut file,
            &path,
            sinomo_core::storage::durable_file::DurableRecordReadOptions::new(
                sinomo_core::storage::durable_file::DurableLengthEndian::Little,
            ),
        )
        .map_err(error)?;
        let Some(record) = record else {
            if length != 0 && !snapshot_current {
                return Err(error("metadata table log declaration is incomplete"));
            }
            continue;
        };
        if record.bytes.len() < 6 || &record.bytes[..4] != sinomo_core::object::MAGIC {
            return Err(error("metadata table log declaration is invalid"));
        }
        if u16::from_le_bytes([record.bytes[4], record.bytes[5]])
            == sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION
        {
            current = true;
        } else {
            legacy = true;
        }
    }
    if current && legacy
        || snapshot_present && !snapshot_current && current
        || snapshot_current && legacy
    {
        return Err(error(
            "metadata snapshot/log roles declare different storage generations",
        ));
    }
    if !current && !snapshot_current {
        return Ok(false);
    }
    let first_prefix = sinomo_dfs::read_current_metadata_table_log_prefix(root)?;
    Ok(snapshot_current || first_prefix.is_some())
}
pub(crate) fn metadata_root_at_format(root: &Path, format: u16) -> Result<bool> {
    let path = root.join("records/tables/snapshot.sobj");
    if !path.exists() {
        return Ok(false);
    }
    if !sinomo_core::storage::migration::stream::is_stream(&path).map_err(error)? {
        return Ok(false);
    }
    let bytes = sinomo_core::storage::migration::stream::read_header(&path).map_err(error)?;
    let header: Value = sinomo_core::object::serde_ext::from_bytes(&bytes).map_err(error)?;
    if header.get("format_version").and_then(Value::as_u64) != Some(u64::from(format)) {
        return Ok(false);
    }
    let proof: DfsMetadataPrefixProof = serde_json::from_value(
        header
            .get("prefix_proof")
            .cloned()
            .ok_or_else(|| error("current metadata header has no prefix"))?,
    )
    .map_err(error)?;
    source_proof::validate(&proof)?;
    if proof.version != 2
        || header
            .get("materialized_through_sequence")
            .and_then(Value::as_u64)
            != Some(proof.journal_sequence)
        || header
            .get("semantic_state_fingerprint")
            .and_then(Value::as_str)
            .is_none()
    {
        return Err(error("invalid current metadata checkpoint header"));
    }
    // Normal DFS performs the full native framing/table validation at mount.
    Ok(true)
}
/// Source discovery never treats a retained combined root as an empty shard.
/// A completed import records where its still-retained original image lives,
/// allowing other logical shards to convert on demand after root activation.
pub fn discover_metadata_source(
    destination: &Path,
    default_source: &Path,
) -> Result<Option<PathBuf>> {
    if current_root(destination)? {
        return Ok(None);
    }
    if source::has_migration_source_data(destination)? {
        return Ok(Some(destination.to_owned()));
    }
    let imported = default_source.join("metadata-import-v7.json");
    if imported.is_file() {
        let record: Value = read_json(&imported)?;
        let retained = record
            .get("retained_source_root")
            .and_then(Value::as_str)
            .ok_or_else(|| error("current import has no retained source identity"))?;
        let retained = PathBuf::from(retained);
        let parent = default_source
            .parent()
            .ok_or_else(|| error("metadata source has no owning parent"))?
            .canonicalize()
            .map_err(error)?;
        let actual = retained.canonicalize().map_err(error)?;
        if !actual.starts_with(&parent) || actual == destination {
            return Err(error(
                "retained metadata source escapes the owned storage root",
            ));
        }
        return Ok(Some(actual));
    }
    if !default_source.exists() || current_root(default_source)? {
        return Ok(None);
    }
    if source::has_migration_source_data(default_source)? {
        return Ok(Some(default_source.to_owned()));
    }
    Ok(None)
}
#[cfg(test)]
fn open_session(destination: &Path, source: &Path, shard: &DfsMetadataShardId) -> Result<Session> {
    let goal = active_data_goal(destination, shard)?;
    open_session_at_stage(
        destination,
        source,
        shard,
        stage_path_for_goal(
            conversion_stage_base_path(destination, shard)?,
            goal.as_ref(),
        ),
        goal,
    )
}
fn open_session_at_stage(
    destination: &Path,
    source: &Path,
    shard: &DfsMetadataShardId,
    staged_root: PathBuf,
    goal: Option<DataGoalStage>,
) -> Result<Session> {
    let lock = retain_source_fence(source)?;
    let inventory = source::discover(source)?;
    fs::create_dir_all(&staged_root).map_err(error)?;
    let progress_path = staged_root.join("progress.json");
    if let Some(goal) = goal.as_ref() {
        if goal.destination != destination
            || goal.shard != *shard
            || staged_root.file_name().and_then(|name| name.to_str()) != Some(goal.digest.as_str())
            || staged_root
                .parent()
                .and_then(Path::file_name)
                .and_then(|name| name.to_str())
                != Some("data-goals")
        {
            return Err(error("conversion cursor path names another DATA goal"));
        }
    }
    let progress = if progress_path.exists() {
        let progress = progress_for_data_goal(read_json(&progress_path)?, goal.as_ref())?;
        if progress.source.inventory.as_ref() != Some(&inventory) {
            return Err(error("resumed conversion names a different source cut"));
        }
        progress
    } else {
        Progress {
            source: source::SourceCursor::new(inventory.clone()),
            events: Default::default(),
            phase: Phase::Source,
            tail_after: 0,
            current_import_candidate_count: None,
            data_goal_digest: goal.as_ref().map(|goal| goal.digest.clone()),
        }
    };
    let replay_source = source::SourceCursor::new(inventory.clone());
    let session = Session {
        _source_lock: lock,
        source_root: source.to_owned(),
        destination_root: destination.to_owned(),
        staged_root,
        inventory,
        progress,
        data_goal: goal,
        replay_cursor: Default::default(),
        replayed_events: 0,
        replay_source,
        replay_tail_after: None,
        replay_validated: false,
        current_import_policy_bound: false,
        projection: Default::default(),
        header: Default::default(),
        materialized_through: 0,
        source_head: 0,
        source_prefix: None,
        source_record_point: None,
        source_current_membership: None,
        verified_source_points: BTreeMap::new(),
        source_content_membership: BTreeMap::new(),
        source_object_membership: BTreeMap::new(),
        retained_content_work: BTreeMap::new(),
        confirmed_source_content_bindings: Default::default(),
        journal_sequences: Default::default(),
        journal_frame_digests: BTreeMap::new(),
    };
    save_progress(&session)?;
    Ok(session)
}
fn unversioned_conversion_stage_base_path(
    destination: &Path,
    shard: &DfsMetadataShardId,
) -> Result<PathBuf> {
    let parent = destination
        .parent()
        .ok_or_else(|| error("metadata destination has no parent"))?;
    let key = blake3::hash(&serde_json::to_vec(&(destination, shard)).map_err(error)?)
        .to_hex()
        .to_string();
    Ok(parent.join(".metadata-conversion").join(key))
}
fn unversioned_conversion_stage_path(
    destination: &Path,
    shard: &DfsMetadataShardId,
) -> Result<PathBuf> {
    let base = unversioned_conversion_stage_base_path(destination, shard)?;
    Ok(stage_path_for_goal(
        base.clone(),
        read_data_goal(&base, destination, shard)?.as_ref(),
    ))
}
fn conversion_stage_base_path(destination: &Path, shard: &DfsMetadataShardId) -> Result<PathBuf> {
    let base = unversioned_conversion_stage_base_path(destination, shard)?;
    // Completed source replay/candidate receipts belong to their target schema.
    // Preserve them while a new generation imports the actual live source root.
    Ok(base.join(format!(
        "target-format-{}",
        sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION
    )))
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct DataGoalStage {
    destination: PathBuf,
    shard: DfsMetadataShardId,
    capsules: Vec<Value>,
    digest: String,
}
fn validated_placement_capsules(
    capsules: &[Value],
    shard: &DfsMetadataShardId,
) -> Result<(
    Vec<Value>,
    std::collections::BTreeSet<MetadataDataReservationRef>,
)> {
    let mut rows = BTreeMap::new();
    for row in capsules {
        let reservation =
            crate::authority::placement_job_source::OriginalPlacementJob::verify_capsule(row)?;
        if reservation.original_shard_id != *shard
            || rows.insert(reservation, row.clone()).is_some()
        {
            return Err(error(
                "DATA goal crosses or repeats its actual source scope",
            ));
        }
    }
    if rows.is_empty() {
        return Err(error("DATA goal has no original reservation"));
    }
    let reservations = rows.keys().cloned().collect();
    Ok((rows.into_values().collect(), reservations))
}
pub(crate) fn data_goal_digest(
    destination: &Path,
    shard: &DfsMetadataShardId,
    capsules: &[Value],
) -> Result<String> {
    Ok(blake3::hash(
        &serde_json::to_vec(&("metadata-data-goal-v1", destination, shard, capsules))
            .map_err(error)?,
    )
    .to_hex()
    .to_string())
}
fn active_data_goal(
    destination: &Path,
    shard: &DfsMetadataShardId,
) -> Result<Option<DataGoalStage>> {
    let base = conversion_stage_base_path(destination, shard)?;
    if let Some(goal) = read_data_goal(&base, destination, shard)? {
        return Ok(Some(goal));
    }
    // Exact retained DATA reservations remain inputs, even when their target
    // generation changes. The old pointer/bytes are never rewritten here.
    read_data_goal(
        &unversioned_conversion_stage_base_path(destination, shard)?,
        destination,
        shard,
    )
}
fn read_data_goal(
    base: &Path,
    destination: &Path,
    shard: &DfsMetadataShardId,
) -> Result<Option<DataGoalStage>> {
    let path = base.join("active-data-goal.json");
    if !path.exists() {
        return Ok(None);
    }
    let goal: DataGoalStage = read_json(&path)?;
    validate_data_goal(&goal, destination, shard)?;
    Ok(Some(goal))
}
fn validate_data_goal(
    goal: &DataGoalStage,
    destination: &Path,
    shard: &DfsMetadataShardId,
) -> Result<()> {
    let (verified, _) = validated_placement_capsules(&goal.capsules, shard)?;
    if goal.destination != destination
        || goal.shard != *shard
        || verified != goal.capsules
        || goal.digest != data_goal_digest(destination, shard, &verified)?
    {
        return Err(error(
            "DATA stage role changes exact destination/scope/capsules",
        ));
    }
    Ok(())
}
fn conversion_stage_path(destination: &Path, shard: &DfsMetadataShardId) -> Result<PathBuf> {
    let base = conversion_stage_base_path(destination, shard)?;
    Ok(stage_path_for_goal(
        base,
        active_data_goal(destination, shard)?.as_ref(),
    ))
}
fn stage_path_for_goal(base: PathBuf, goal: Option<&DataGoalStage>) -> PathBuf {
    match goal {
        Some(goal) => base.join("data-goals").join(&goal.digest),
        None => base,
    }
}
fn placement_goal_refs(
    capsules: &[Value],
) -> Result<std::collections::BTreeSet<MetadataDataReservationRef>> {
    capsules
        .iter()
        .map(crate::authority::placement_job_source::OriginalPlacementJob::verify_capsule)
        .collect()
}
fn require_placement_goal_refs(cut: &MetadataSourceCut, capsules: &[Value]) -> Result<()> {
    let actual = cut
        .pending_operations
        .iter()
        .filter(|row| {
            matches!(
                row["capsule_kind"].as_str(),
                Some("placement-job-v0" | "placement-job-v1")
            )
        })
        .map(crate::authority::placement_job_source::OriginalPlacementJob::verify_capsule)
        .collect::<Result<Vec<_>>>()?;
    let expected = placement_goal_refs(capsules)?;
    if actual.len() != expected.len()
        || actual
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            != expected
    {
        return Err(error("source cut changes exact DATA goal reservation set"));
    }
    Ok(())
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataImportMarker {
    format_version: u32,
    retained_source_root: PathBuf,
    source_identity: String,
    source_snapshot_fingerprint: String,
    authority_binding: String,
    source_cut: MetadataSourceCut,
    native_image: sinomo_dfs::DfsMetadataCandidateIdentity,
    /// A physical installation fact, written only after native certification,
    /// exact destination-image verification and imported-work registration.
    #[serde(default)]
    installation_complete: bool,
}

fn read_import_marker(
    destination: &Path,
    shard: &DfsMetadataShardId,
) -> Result<Option<MetadataImportMarker>> {
    let destination = canonical_destination(destination)?;
    let path = destination.join("metadata-import-v7.json");
    if !path.is_file() {
        return Ok(None);
    }
    let marker: MetadataImportMarker = read_json(&path)?;
    let cut = &marker.source_cut;
    source_proof::validate(&marker.native_image.prefix_proof)?;
    if marker.format_version != 7
        || cut.scope_shard != *shard
        || marker.authority_binding.is_empty()
        || marker.native_image.prefix_proof != cut.target_exact_prefix
        || marker.native_image.state_fingerprint != cut.current_image_fingerprint
        || marker.source_identity != cut.source_identity
        || marker.source_snapshot_fingerprint != cut.source_snapshot_fingerprint
    {
        return Err(error(
            "installed marker changes its complete native cut/image/source binding",
        ));
    }
    let expected_parent = destination
        .parent()
        .ok_or_else(|| error("installed DATA destination has no owner"))?
        .join(".metadata-retained");
    if cut.source_root == destination
        && marker.retained_source_root.parent() != Some(expected_parent.as_path())
        || cut.source_root != destination && marker.retained_source_root != cut.source_root
    {
        return Err(error(
            "installed marker changes its actual retained source role",
        ));
    }
    Ok(Some(marker))
}

fn published_import_marker(
    destination: &Path,
    shard: &DfsMetadataShardId,
) -> Result<Option<MetadataImportMarker>> {
    if !current_root(destination)? {
        return Ok(None);
    }
    let Some(marker) = read_import_marker(destination, shard)? else {
        return Ok(None);
    };
    if !marker.installation_complete {
        return Ok(None);
    }
    validate_retained_source_identity(
        &marker.retained_source_root,
        &marker.source_identity,
        &marker.source_snapshot_fingerprint,
    )?;
    // The initial native_image is the SAME installed identity. Normal later
    // checkpoints may replace snapshot bytes; only exact journal ancestry to
    // that captured anchor can admit the current read-only prefix.
    let Some(current) = sinomo_dfs::inspect_metadata_prefix_descendant(
        destination,
        shard,
        &marker.native_image.prefix_proof,
    )
    .map_err(error)?
    else {
        return Err(MetadataBackendError::StoragePreparationPending {
            root: destination.to_owned(),
            processed_records: 0,
        });
    };
    if current == marker.native_image.prefix_proof {
        // Equal-prefix replacement must preserve the actual native logical
        // image too. Checkpoint layout bytes may change without a new prefix.
        let actual = inspect_import_candidate(destination)?;
        if actual.prefix_proof != marker.native_image.prefix_proof
            || actual.state_fingerprint != marker.native_image.state_fingerprint
        {
            return Err(error(
                "installed native image changed at the same exact prefix",
            ));
        }
    }
    Ok(Some(marker))
}

/// Native activation does not finish the local candidate exchange. This
/// representation check supplements the caller's actual native receipt and
/// reservation validation; marker DATA can never supply author authority.
pub(crate) fn installed_native_data_goal_matches(
    destination: &Path,
    plan: &NativeMetadataImportPlan,
) -> Result<bool> {
    let Some(marker) = published_import_marker(destination, &plan.source_cut.scope_shard)? else {
        return Ok(false);
    };
    if marker.authority_binding != plan.expected_handoff.handoff_id
        || serde_json::to_value(&marker.source_cut).map_err(error)?
            != serde_json::to_value(&plan.source_cut).map_err(error)?
        || marker.native_image.prefix_proof != plan.target_exact_prefix
        || marker.native_image.state_fingerprint != plan.current_image_fingerprint
    {
        return Err(error(
            "installed DATA goal differs from its actual native plan",
        ));
    }
    Ok(true)
}

fn published_data_goal_matches(
    destination: &Path,
    shard: &DfsMetadataShardId,
    capsules: &[Value],
) -> Result<bool> {
    let Some(marker) = published_import_marker(destination, shard)? else {
        return Ok(false);
    };
    let actual = marker
        .source_cut
        .pending_operations
        .iter()
        .filter(|row| {
            matches!(
                row["capsule_kind"].as_str(),
                Some("placement-job-v0" | "placement-job-v1")
            )
        })
        .map(crate::authority::placement_job_source::OriginalPlacementJob::verify_capsule)
        .collect::<Result<Vec<_>>>()?;
    let expected = placement_goal_refs(capsules)?;
    Ok(actual.len() == expected.len()
        && actual
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            == expected)
}

fn complete_installed_import_marker(
    destination: &Path,
    proposal: &MetadataImportProposal,
    identity: &sinomo_dfs::DfsMetadataCandidateIdentity,
    installation_binding: &str,
) -> Result<()> {
    let mut marker = read_import_marker(destination, &proposal.scope_shard)?
        .ok_or_else(|| error("verified installed image has no retained marker"))?;
    if marker.authority_binding != installation_binding
        || marker.native_image != *identity
        || serde_json::to_value(&marker.source_cut).map_err(error)?
            != serde_json::to_value(&proposal.source_cut).map_err(error)?
    {
        return Err(error(
            "verified installation marker changed before completion",
        ));
    }
    marker.installation_complete = true;
    write_json(&destination.join("metadata-import-v7.json"), &marker)
}
fn select_data_goal(
    destination: &Path,
    shard: &DfsMetadataShardId,
    capsules: Vec<Value>,
    warm: bool,
    active: &mut Option<DataGoalStage>,
) -> Result<bool> {
    let digest = data_goal_digest(destination, shard, &capsules)?;
    if let Some(active) = active.as_ref() {
        if active.digest == digest {
            return Ok(true);
        }
    }
    if warm {
        return Ok(false);
    }
    let previous = stage_path_for_goal(
        conversion_stage_base_path(destination, shard)?,
        active.as_ref(),
    );
    // Never displace an unfinished exchange or partial bounded source replay.
    if previous.join("installation.json").exists() {
        return Ok(false);
    }
    if previous.join("progress.json").exists() {
        if !previous.join("source-cut.json").is_file() {
            return Ok(false); // partial bounded replay must resume the old goal
        }
        let marker = destination.join("metadata-import-v7.json");
        if !current_root(destination)? || !marker.is_file() {
            return Ok(false);
        }
        let row: Value = read_json(&marker)?;
        let cut: MetadataSourceCut =
            serde_json::from_value(row["source_cut"].clone()).map_err(error)?;
        let retained: MetadataSourceCut = read_json(&previous.join("source-cut.json"))?;
        if cut.scope_shard != *shard
            || cut.staged_root != previous.join("current-image")
            || serde_json::to_value(&cut).map_err(error)?
                != serde_json::to_value(&retained).map_err(error)?
            || previous.join("current-image").exists()
        {
            return Ok(false);
        }
    }
    let base = conversion_stage_base_path(destination, shard)?;
    fs::create_dir_all(&base).map_err(error)?;
    let goal = DataGoalStage {
        destination: destination.to_owned(),
        shard: shard.clone(),
        capsules,
        digest,
    };
    write_json(&base.join("active-data-goal.json"), &goal)?;
    // Install the new call-local selection only after the same durable write.
    *active = Some(goal);
    Ok(true)
}

/// Locate provenance only through the controller's actual destination/stage
/// roles. A returned blueprint is data; the caller must replay its native
/// requests and compare the live exact handoff before using it.
pub(crate) fn retained_native_import_plan(
    destination: &Path,
    file_system_id: &DfsFileSystemId,
    shard: &DfsMetadataShardId,
    handoff_id: &str,
) -> Result<Option<(NativeMetadataImportPlan, PathBuf)>> {
    let destination = canonical_destination(destination)?;
    let stage = conversion_stage_path(&destination, shard)?;
    let previous = unversioned_conversion_stage_path(&destination, shard)?;
    let roots = [
        stage.join("current-image"),
        previous.join("current-image"),
        destination,
    ];
    let mut retained: Option<(NativeMetadataImportPlan, PathBuf)> = None;
    for root in roots {
        let path = root.join("native-import-plan.json");
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(MetadataBackendError::Io { path, source }),
        };
        let plan: NativeMetadataImportPlan = serde_json::from_slice(&bytes).map_err(error)?;
        if plan.expected_handoff.handoff_id != handoff_id {
            continue;
        }
        if plan.source_cut.scope_shard != *shard
            || plan.expected_handoff.retiring_shard.file_system_id != *file_system_id
            || plan.expected_handoff.retiring_shard.shard_id != shard.0
            || plan.expected_handoff.desired_shard.file_system_id != *file_system_id
            || plan.expected_handoff.desired_shard.shard_id != shard.0
        {
            return Err(error(
                "retained native plan differs from the requested actual scope",
            ));
        }
        if let Some((previous, _)) = &retained {
            if serde_json::to_value(previous).map_err(error)?
                != serde_json::to_value(&plan).map_err(error)?
            {
                return Err(error(
                    "one live handoff has conflicting retained native blueprints",
                ));
            }
        }
        retained = Some((plan, root));
    }
    Ok(retained)
}

/// Existing filesystem ancestry supplies one physical slot key even before
/// the destination directory exists. No data directory is created here.
fn canonical_destination(destination: &Path) -> Result<PathBuf> {
    let absolute = if destination.is_absolute() {
        destination.to_owned()
    } else {
        std::env::current_dir().map_err(error)?.join(destination)
    };
    let mut ancestor = absolute;
    let mut missing = Vec::new();
    loop {
        match fs::metadata(&ancestor) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => return Err(error("metadata destination ancestry is not a directory")),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                if fs::symlink_metadata(&ancestor).is_ok() {
                    return Err(error(
                        "metadata destination contains an unresolved symbolic link",
                    ));
                }
                let name = ancestor
                    .file_name()
                    .ok_or_else(|| {
                        error("metadata destination has no resolvable existing ancestor")
                    })?
                    .to_owned();
                missing.push(name);
                if !ancestor.pop() {
                    return Err(error("metadata destination escapes its existing ancestry"));
                }
            }
            Err(source) => {
                return Err(MetadataBackendError::Io {
                    path: ancestor,
                    source,
                });
            }
        }
    }
    let mut resolved = ancestor.canonicalize().map_err(error)?;
    for name in missing.into_iter().rev() {
        resolved.push(name);
    }
    Ok(resolved)
}
fn save_progress(session: &Session) -> Result<()> {
    write_json(
        &session.staged_root.join("progress.json"),
        &session.progress,
    )
}
fn event_header(session: &Session) -> Result<Vec<u8>> {
    #[derive(Serialize)]
    struct Header<'a> {
        schema_version: u16,
        source_fingerprint: &'a str,
        destination_root: &'a Path,
    }
    sinomo_core::object::serde_ext::to_bytes(
        &Header {
            schema_version: 7,
            source_fingerprint: &session.inventory.fingerprint,
            destination_root: &session.destination_root,
        },
        7,
        sinomo_core::object::LengthMode::U32,
    )
    .map_err(error)
}
fn append_event(session: &mut Session, event: ConversionEvent) -> Result<()> {
    let header = event_header(session)?;
    let mut writer = sinomo_core::storage::migration::stream::MigrationStreamWriter::resume(
        &session.staged_root.join("events.slog"),
        &header,
        &session.progress.events,
    )
    .map_err(error)?;
    let bytes =
        sinomo_core::object::serde_ext::to_bytes(&event, 7, sinomo_core::object::LengthMode::U32)
            .map_err(error)?;
    writer.append(&bytes).map_err(error)?;
    let durable = writer.checkpoint().map_err(error)?;
    apply_event(session, &event)?;
    session.progress.events = durable.clone();
    session.replay_cursor = durable;
    session.replayed_events += 1;
    Ok(())
}
fn validate_replayed_event(session: &mut Session, event: &ConversionEvent) -> Result<()> {
    match event {
        ConversionEvent::Source { point, record } => {
            if session.replay_tail_after.is_some()
                || serde_json::to_value(point.point()).map_err(error)?
                    != serde_json::to_value(session.replay_source.point()).map_err(error)?
            {
                return Err(error(
                    "staged source event changes actual decoder order or position",
                ));
            }
            let (verified_point, actual) =
                source::next_verified(&session.source_root, &mut session.replay_source)?
                    .ok_or_else(|| {
                        error("staged source event extends beyond the actual immutable source")
                    })?;
            if serde_json::to_value(&actual).map_err(error)?
                != serde_json::to_value(record).map_err(error)?
            {
                return Err(error(
                    "staged source event differs from independently decoded source bytes",
                ));
            }
            source_content::cache_verified_point(session, verified_point, &actual)?;
        }
        ConversionEvent::Tail(entry) => {
            if session.replay_tail_after.is_none()
                && source::next(&session.source_root, &mut session.replay_source)?.is_some()
            {
                return Err(error(
                    "staged journal replay begins before complete actual source discovery",
                ));
            }
            let after = session
                .replay_tail_after
                .unwrap_or(session.materialized_through);
            let sequence = entry
                .get("sequence")
                .and_then(Value::as_u64)
                .ok_or_else(|| error("staged journal replay has no actual sequence"))?;
            if after.checked_add(1) != Some(sequence)
                || read_verified_journal_evidence(
                    &session.staged_root,
                    sequence,
                    &session.journal_frame_digests,
                )? != *entry
            {
                return Err(error(
                    "staged journal replay changes its exact source frame or contiguous cut",
                ));
            }
            session.replay_tail_after = Some(sequence);
        }
    }
    Ok(())
}
fn evidence_path(stage: &Path, sequence: u64) -> PathBuf {
    stage
        .join("journal-evidence")
        .join(format!("{sequence:020}.sobj"))
}
fn read_verified_journal_evidence(
    stage: &Path,
    sequence: u64,
    digests: &BTreeMap<u64, String>,
) -> Result<Value> {
    read_verified_journal_document(stage, sequence, digests).map(|(entry, _)| entry)
}
pub(crate) fn read_verified_journal_document(
    stage: &Path,
    sequence: u64,
    digests: &BTreeMap<u64, String>,
) -> Result<(Value, source_document::SourceRawDocument)> {
    let bytes = fs::read(evidence_path(stage, sequence)).map_err(error)?;
    if digests
        .get(&sequence)
        .is_none_or(|expected| *expected != blake3::hash(&bytes).to_hex().as_str())
    {
        return Err(error(
            "retained staged journal bytes are not the actual captured source frame",
        ));
    }
    let entry = source_document::decode_original_document(&bytes)?;
    Ok((entry, source_document::SourceRawDocument::row(bytes)))
}
fn store_journal_evidence(session: &mut Session, entry: &Value, bytes: &[u8]) -> Result<()> {
    let sequence = entry
        .get("sequence")
        .and_then(Value::as_u64)
        .ok_or_else(|| error("retained journal frame has no sequence"))?;
    let path = evidence_path(&session.staged_root, sequence);
    if path.exists() {
        if fs::read(&path).map_err(error)? != bytes {
            return Err(error(
                "retained journal has conflicting exact frames at the same slot",
            ));
        }
    } else {
        fs::create_dir_all(
            path.parent()
                .ok_or_else(|| error("journal evidence has no parent"))?,
        )
        .map_err(error)?;
        sinomo_core::storage::durable_file::write_file_atomic(
            &path,
            bytes,
            sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("source", "tmp")
                .sync_all(),
        )
        .map_err(error)?;
    }
    session.journal_sequences.insert(sequence);
    let digest = blake3::hash(bytes).to_hex().to_string();
    if session
        .journal_frame_digests
        .insert(sequence, digest.clone())
        .is_some_and(|old| old != digest)
    {
        return Err(error("actual source journal forks a retained frame digest"));
    }
    session.source_head = session.source_head.max(sequence);
    Ok(())
}
fn apply_event(session: &mut Session, event: &ConversionEvent) -> Result<()> {
    session.source_record_point = match event {
        ConversionEvent::Source { point, .. } => Some(point.point()),
        ConversionEvent::Tail(_) => None,
    };
    if let ConversionEvent::Source { record, .. } = event {
        source_content::capture(session, record)?;
    }
    match event {
        ConversionEvent::Source {
            record: source::SourceRecord::SnapshotHeader(value),
            ..
        } => {
            for (key, value) in value
                .as_object()
                .ok_or_else(|| error("retained snapshot header is not a record"))?
            {
                session.header.insert(key.clone(), value.clone());
            }
            if let Some(sequence) = session
                .header
                .get("materialized_through_sequence")
                .and_then(Value::as_u64)
            {
                session.materialized_through = sequence;
                session.source_head = session.source_head.max(sequence);
            }
            if let Some(value) = session.header.get("prefix_proof").filter(|v| !v.is_null()) {
                let proof = serde_json::from_value::<DfsMetadataPrefixProof>(value.clone())
                    .map_err(error)?;
                source_proof::validate(&proof)?;
                if proof.journal_sequence != session.materialized_through {
                    return Err(error(
                        "retained snapshot proof differs from its materialization cut",
                    ));
                }
                session.source_prefix = Some(proof);
                source_ancestry::store_anchor(
                    &session.staged_root,
                    session.source_prefix.as_ref().expect("captured proof"),
                )?;
            }
        }
        ConversionEvent::Source {
            record:
                source::SourceRecord::SnapshotRow {
                    table,
                    row,
                    raw_document,
                },
            ..
        } => session.projection.push_snapshot_row_from_source(
            table,
            row.clone(),
            &raw_document.context()?,
        )?,
        ConversionEvent::Source {
            record:
                source::SourceRecord::LooseRow {
                    family,
                    row,
                    raw_document,
                },
            ..
        } => {
            let family = match family.as_str() {
                "filesystems" => "file_systems",
                "filesystem-objects" => "file_system_objects",
                "visible-commits" => "visible_commits",
                "namespace-transactions" => "namespace_transactions",
                other => other,
            };
            session.projection.push_snapshot_row_from_source(
                &family.replace('-', "_"),
                row.clone(),
                &raw_document.context()?,
            )?;
        }
        ConversionEvent::Source {
            record: source::SourceRecord::JournalFrame { entry, exact_bytes },
            ..
        } => store_journal_evidence(session, entry, exact_bytes)?,
        ConversionEvent::Source {
            record:
                source::SourceRecord::TableBatch {
                    batch,
                    raw_document,
                },
            ..
        } => {
            let sequence = batch
                .get("sequence")
                .and_then(Value::as_u64)
                .ok_or_else(|| error("retained table batch has no sequence"))?;
            let prefix = batch
                .get("prefix_proof")
                .filter(|v| !v.is_null())
                .map(|value| {
                    serde_json::from_value::<DfsMetadataPrefixProof>(value.clone()).map_err(error)
                })
                .transpose()?;
            if let Some(proof) = &prefix {
                source_proof::validate(proof)?;
                if proof.journal_sequence != sequence {
                    return Err(error("retained table batch proof differs from its cut"));
                }
                source_ancestry::store_anchor(&session.staged_root, proof)?;
            }
            let ops = batch
                .get("ops")
                .and_then(Value::as_array)
                .ok_or_else(|| error("retained table batch has no operation list"))?;
            if sequence > session.materialized_through {
                let stage = session.staged_root.clone();
                let digests = &session.journal_frame_digests;
                let source_context = raw_document.context()?;
                for (index, op) in ops.iter().enumerate() {
                    session.projection.apply_table_op_from_source(
                        op.clone(),
                        &source_context,
                        &format!("/ops/{index}"),
                        &|seq| read_verified_journal_document(&stage, seq, digests),
                    )?;
                }
                session.materialized_through = sequence;
                session.source_head = session.source_head.max(sequence);
                session.source_prefix = prefix;
            }
        }
        ConversionEvent::Source {
            record: source::SourceRecord::Evidence { relative, value },
            ..
        } => {
            let directory = session.staged_root.join("source-evidence");
            fs::create_dir_all(&directory).map_err(error)?;
            let key = blake3::hash(relative.to_string_lossy().as_bytes())
                .to_hex()
                .to_string();
            write_json(&directory.join(format!("{key}.json")), &(relative, value))?;
        }
        ConversionEvent::Tail(entry) => {
            let sequence = entry
                .get("sequence")
                .and_then(Value::as_u64)
                .ok_or_else(|| error("retained tail has no sequence"))?;
            let (recorded_entry, raw_document) = read_verified_journal_document(
                &session.staged_root,
                sequence,
                &session.journal_frame_digests,
            )?;
            if recorded_entry != *entry {
                return Err(error(
                    "retained tail differs from its original verified source document",
                ));
            }
            session
                .projection
                .apply_journal_from_source(recorded_entry, &raw_document.context()?)?;
            if let Some(proof) = session.source_prefix.as_ref() {
                let bytes =
                    fs::read(evidence_path(&session.staged_root, sequence)).map_err(error)?;
                let anchor = source_ancestry::retained_anchor(&session.staged_root, sequence)?;
                let extended = source_proof::extend_as(
                    proof,
                    sequence,
                    &bytes,
                    anchor
                        .as_ref()
                        .map_or(proof.version, |anchor| anchor.version),
                )?;
                if anchor.as_ref().is_some_and(|anchor| anchor != &extended) {
                    return Err(error("retained source tail forks its actual prefix anchor"));
                }
                session.source_prefix = Some(extended);
            }
            session.source_head = session.source_head.max(sequence);
        }
    }
    Ok(())
}
fn validate_source_snapshot(session: &Session) -> Result<()> {
    if let Some(expected) = session
        .header
        .get("semantic_state_fingerprint")
        .and_then(Value::as_str)
    {
        let path = session.source_root.join("records/tables/snapshot.sobj");
        if !sinomo_core::storage::migration::stream::is_stream(&path).map_err(error)?
            && wire_json::snapshot_fingerprint(&path)? != expected
        {
            return Err(error("retained checkpoint semantic checksum mismatch"));
        }
    }
    Ok(())
}
fn source_state_fingerprint(session: &Session) -> Result<String> {
    if let Some(prefix) = &session.source_prefix {
        return Ok(prefix.digest.clone());
    }
    if let Some(fingerprint) = session
        .header
        .get("semantic_state_fingerprint")
        .and_then(Value::as_str)
    {
        return Ok(fingerprint.to_owned());
    }
    // Old proof-less sources bind the complete captured image; this remains
    // converter evidence until real native joint activation certifies it.
    Ok(session.inventory.fingerprint.clone())
}
/// Independently decoded immutable source DATA; it grants no authority.
/// An expected identity is checked before verifying the same inventory, exactly
/// as in ordinary source validation. Inspection never creates a source grant.
pub(crate) fn read_retained_source_identity(
    actual_root: &Path,
    expected: Option<(&str, &str)>,
) -> Result<(String, String)> {
    let inventory = source::discover(actual_root)?;
    let snapshot = inventory
        .snapshot_fingerprint()
        .unwrap_or(&inventory.fingerprint);
    if expected.is_some_and(|(identity, expected_snapshot)| {
        inventory.fingerprint != identity || snapshot != expected_snapshot
    }) {
        return Err(error(
            "actual retained source differs from its immutable captured identity",
        ));
    }
    inventory.verify(actual_root)?;
    Ok((inventory.fingerprint.clone(), snapshot.to_owned()))
}
pub(crate) fn validate_retained_source_identity(
    actual_root: &Path,
    expected_source_identity: &str,
    expected_snapshot_fingerprint: &str,
) -> Result<()> {
    read_retained_source_identity(
        actual_root,
        Some((expected_source_identity, expected_snapshot_fingerprint)),
    )
    .map(|_| ())
}

fn partition_projection(
    session: &mut Session,
    shard: &DfsMetadataShardId,
    configs: &[DfsMetadataShardConfig],
) -> Result<(sinomo_dfs::DfsMetadataTableSnapshotTables, Vec<Value>)> {
    let stage = session.staged_root.clone();
    let digests = &session.journal_frame_digests;
    session
        .projection
        .resolve_deferred(&|seq| read_verified_journal_document(&stage, seq, digests))?;
    let targets = configs
        .iter()
        .filter(|config| config.shard_id == shard.0)
        .collect::<Vec<_>>();
    if targets.len() != 1 {
        return Err(error(
            "native source mapping requires one exact filesystem/shard target",
        ));
    }
    let mut tables = session.projection.current_tables()?;
    let mut pending = session.projection.pending_operations()?;
    let (work_mappings, unresolved) = scope_mapping::resolve_adoption_work(
        &mut tables,
        &mut pending,
        session.projection.required_native_work(),
        configs,
        targets[0],
    )?;
    let mut mapped = scope_mapping::map_scope(tables, pending, configs, targets[0])?;
    mapped.operation_mappings.extend(work_mappings);
    mapped.required_native_work.extend(unresolved);
    // These are source DATA in the certified image; normal resume establishes
    // current writer custody after that image is installed.
    for job in &mapped.tables.namespace_jobs {
        if job.status == sinomo_dfs::DfsNamespaceJobStatus::Running {
            mapped.operation_mappings.push(serde_json::json!({
                "disposition":"restored-current-job-data", "current_job_data":job,
                "current_effects_authored":false,
            }));
        }
    }
    write_json(
        &stage.join("scope-mapping-provenance.json"),
        &serde_json::json!({
            "row_provenance":mapped.row_provenance,"required_source_retirements":mapped.required_source_retirements,
            "required_native_work":mapped.required_native_work,"retained_non_target_row_count":mapped.retained_non_target_row_count
        }),
    )?;
    let mut operations = mapped.operation_mappings;
    for work in mapped.required_native_work {
        operations.push(serde_json::json!({"disposition":"requires-native-row-work","family":work.family,"reason":work.reason,"source_row":work.source_row}));
    }
    Ok((mapped.tables, operations))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BuiltImage {
    source_fingerprint: String,
    projection_fingerprint: String,
    identity: sinomo_dfs::DfsMetadataCandidateIdentity,
    pending_mapping: Vec<Value>,
}
fn build_native_image(
    session: &Session,
    shard: &DfsMetadataShardId,
    mut tables: sinomo_dfs::DfsMetadataTableSnapshotTables,
    pending: Vec<Value>,
) -> Result<(sinomo_dfs::DfsMetadataCandidateIdentity, String, Vec<Value>)> {
    let projection_fingerprint = format!(
        "blake3:{}",
        blake3::hash(&serde_json::to_vec(&(&tables, &pending)).map_err(error)?).to_hex()
    );
    let receipt_path = session.staged_root.join("built-image.json");
    let root = session.staged_root.join("current-image");
    if receipt_path.is_file() {
        let built: BuiltImage = read_json(&receipt_path)?;
        if built.source_fingerprint != session.inventory.fingerprint
            || built.projection_fingerprint != projection_fingerprint
        {
            return Err(error(
                "completed native candidate belongs to another conversion cut",
            ));
        }
        let identity = sinomo_dfs::inspect_metadata_candidate(&root)?;
        if identity != built.identity {
            return Err(error(
                "completed native candidate changed before activation",
            ));
        }
        return Ok((identity, projection_fingerprint, built.pending_mapping));
    }
    let writer = sinomo_dfs::DfsMetadataCandidateWriter::open(&root)?;
    // A crash before the durable build receipt leaves only an unmounted,
    // exclusively owned candidate. Rebuild that candidate from retained data.
    for entry in fs::read_dir(&root).map_err(error)? {
        let entry = entry.map_err(error)?;
        let name = entry.file_name();
        if matches!(
            name.to_str(),
            Some("metadata-candidate-v7.json" | "metadata-candidate.lock")
        ) {
            continue;
        }
        if entry.file_type().map_err(error)?.is_dir() {
            fs::remove_dir_all(entry.path()).map_err(error)?;
        } else {
            fs::remove_file(entry.path()).map_err(error)?;
        }
    }
    let source_sequence = session.source_head.max(session.materialized_through);
    let mut proof = source_proof::conversion_base(
        source_sequence,
        &session.inventory.fingerprint,
        &projection_fingerprint,
    );
    let mut mapping = Vec::new();
    for mut source in pending {
        if source
            .get("import_terminal_identity_only")
            .and_then(Value::as_bool)
            == Some(true)
            || matches!(
                source.get("disposition").and_then(Value::as_str),
                Some(
                    "requires-original-source-retirement"
                        | "requires-original-source-scope"
                        | "requires-native-row-work"
                        | "data-after-original-reservation-retirement"
                        | "restored-current-job-data"
                        | "preserved-original-terminal-data"
                )
            )
        {
            source["current_effects_authored"] = Value::Bool(false);
            mapping.push(source);
            continue;
        }

        let payload: sinomo_dfs::DfsPreparedOperationPayload = serde_json::from_value(
            source
                .get("payload")
                .cloned()
                .ok_or_else(|| error("converted operation lacks its current body"))?,
        )
        .map_err(error)?;
        if payload.shard_id != *shard {
            return Err(error(
                "converted operation crosses the physical candidate shard",
            ));
        }
        let status: sinomo_dfs::DfsPreparedOperationStatus = serde_json::from_value(
            source
                .get("status")
                .cloned()
                .ok_or_else(|| error("converted operation lacks its exact source status"))?,
        )
        .map_err(error)?;
        let retired = source
            .get("retired_reservation")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let compact = source
            .get("compact_terminal")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let terminal_at = source
            .get("terminal_at")
            .filter(|v| !v.is_null())
            .map(|value| serde_json::from_value::<String>(value.clone()).map_err(error))
            .transpose()?
            .map(|value| {
                time::OffsetDateTime::parse(&value, &time::format_description::well_known::Rfc3339)
                    .map_err(error)
            })
            .transpose()?
            .unwrap_or(payload.transaction.created_at);
        let (mut record, after_prepare) = writer.prepare(payload, &proof)?;
        proof = after_prepare;
        if retired {
            record.status = sinomo_dfs::DfsPreparedOperationStatus::Superseded;
        } else if status == sinomo_dfs::DfsPreparedOperationStatus::Committed {
            let (committed, after_commit) = writer.commit(record, &proof, terminal_at)?;
            record = committed;
            proof = after_commit;
        } else {
            record.status = status;
        }
        record.validate()?;
        source
            .as_object_mut()
            .ok_or_else(|| error("operation mapping is not a record"))?
            .insert(
                "current_digest".into(),
                serde_json::to_value(&record.digest).map_err(error)?,
            );
        if compact && !retired && status != sinomo_dfs::DfsPreparedOperationStatus::Prepared {
            tables
                .prepared_operation_evictions
                .push(sinomo_dfs::DfsPreparedOperationEviction {
                    digest: record.digest,
                    status: record.status,
                    committed_entry: record.committed_entry,
                });
        } else {
            tables.prepared_operations_v7.push(record);
        }
        mapping.push(source);
    }
    let identity = writer.write(tables, proof)?;
    drop(writer);
    let built = BuiltImage {
        source_fingerprint: session.inventory.fingerprint.clone(),
        projection_fingerprint: projection_fingerprint.clone(),
        identity: identity.clone(),
        pending_mapping: mapping.clone(),
    };
    write_json(&receipt_path, &built)?;
    Ok((identity, projection_fingerprint, mapping))
}

fn validate_certificate(
    proposal: &MetadataImportProposal,
    certificate: &MetadataImportCertificate,
) -> Result<()> {
    let expected = blake3::hash(
        &serde_json::to_vec(proposal).map_err(|e| MetadataBackendError::Serde(e.to_string()))?,
    )
    .to_hex()
    .to_string();
    if certificate.proposal_fingerprint != expected
        || certificate.current_image_fingerprint != proposal.current_image_fingerprint
    {
        return Err(MetadataBackendError::Serde(
            "native import certificate names another source/image".into(),
        ));
    }
    if (matches!(proposal, MetadataImportProposal::Native(_))
        && proposal.expected_assignments.is_empty())
        || matches!(
            (proposal, certificate),
            (
                MetadataImportProposal::Native(_),
                MetadataImportCertificate::Standalone(_)
            ) | (
                MetadataImportProposal::Standalone(_),
                MetadataImportCertificate::Native(_)
            )
        )
        || proposal.target_exact_prefix.as_ref() != Some(&certificate.target_exact_prefix)
    {
        return Err(MetadataBackendError::Serde(
            "native import lacks exact assignment and target-prefix binding".into(),
        ));
    }
    if let MetadataImportCertificate::Native(seal) = certificate {
        let expected = data_reservation_refs(&proposal.source_cut)?;
        let actual = seal
            .data_reservation_retirements
            .iter()
            .map(|certificate| certificate.reservation.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if actual.len() != seal.data_reservation_retirements.len()
            || expected
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
                != actual
        {
            return Err(error(
                "native certificate changes exact SourceCut DATA reservations",
            ));
        }
        let source_digest = blake3::hash(&serde_json::to_vec(&proposal.source_cut).map_err(error)?)
            .to_hex()
            .to_string();
        for retired in &seal.data_reservation_retirements {
            if retired.reservation.original_shard_id == proposal.scope_shard
                && retired.original_source_cut_digest != source_digest
            {
                return Err(error(
                    "DATA retirement names another actual current source cut",
                ));
            }
        }
    }
    Ok(())
}
pub(crate) fn imported_namespace_jobs(
    source: &MetadataSourceCut,
) -> Result<Vec<sinomo_dfs::DfsNamespaceJobRecord>> {
    let mut jobs = BTreeMap::new();
    for row in &source.pending_operations {
        let mut values = Vec::new();
        if let Some(job) = row.get("current_job_data") {
            values.push(job);
        }
        if let Some(values_from_parent) = row.get("current_jobs").and_then(Value::as_array) {
            values.extend(values_from_parent);
        }
        for value in values {
            let job: sinomo_dfs::DfsNamespaceJobRecord =
                serde_json::from_value(value.clone()).map_err(error)?;
            if job.status != sinomo_dfs::DfsNamespaceJobStatus::Running {
                return Err(error(
                    "imported work mapping names a non-running current job",
                ));
            }
            let key = (job.file_system_id.clone(), job.job_id.clone());
            if let Some(previous) = jobs.insert(key, job.clone()) {
                if previous != job {
                    return Err(error(
                        "imported work mappings disagree on complete job DATA",
                    ));
                }
            }
        }
    }
    Ok(jobs.into_values().collect())
}

fn archive_prior_stage_file(stage: &Path, original: &Path, name: &str) -> Result<()> {
    if !original.is_file() {
        return Ok(());
    }
    let bytes = fs::read(original).map_err(error)?;
    let archive = stage.join(name);
    if archive.is_file() {
        if fs::read(&archive).map_err(error)? != bytes {
            return Err(error("original staging archive changed"));
        }
    } else {
        sinomo_core::storage::durable_file::write_file_atomic(
            &archive,
            &bytes,
            sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("import", "tmp")
                .sync_all(),
        )
        .map_err(error)?;
    }
    Ok(())
}
fn complete_installation_intent(stage: &Path, prior: bool) -> Result<()> {
    let intent = stage.join("installation.json");
    if prior {
        archive_prior_stage_file(stage, &intent, "completed-original-installation-v8.json")?;
    }
    sinomo_core::storage::durable_file::remove_file_if_exists_and_sync_parent(&intent)
        .map(|_| ())
        .map_err(error)
}
fn install_certified_stage(
    session: &Session,
    proposal: &MetadataImportProposal,
    certificate: &MetadataImportCertificate,
    authority: &dyn MetadataImportAuthority,
) -> Result<()> {
    validate_certificate(proposal, certificate)?;
    let image_root = if proposal.staged_root.is_dir() {
        proposal.staged_root.as_path()
    } else {
        session.destination_root.as_path()
    };
    let prior = native_source_root(image_root, Some(8))?;
    let jobs = if prior {
        Vec::new()
    } else {
        imported_namespace_jobs(&proposal.source_cut)?
    };
    if proposal.preserved_pending_operations.iter().any(|row| {
        matches!(
            row.get("disposition").and_then(Value::as_str),
            Some("requires-native-row-work" | "requires-original-source-scope")
        )
    }) {
        return Err(MetadataBackendError::StoragePreparationPending {
            root: session.destination_root.clone(),
            processed_records: 0,
        });
    }
    let identity = inspect_import_candidate(image_root)?;
    if identity.state_fingerprint != certificate.current_image_fingerprint
        || identity.prefix_proof != certificate.target_exact_prefix
    {
        return Err(error(
            "certified current candidate changed before publication",
        ));
    }
    let installation_binding = match (proposal, certificate) {
        (MetadataImportProposal::Native(native), MetadataImportCertificate::Native(seal)) => {
            let proof = &seal.joint_handoff_proof;
            if proof.handoff_id != native.expected_handoff.handoff_id
                || proof.retiring_shard != native.expected_handoff.retiring_shard
                || proof.desired_shard != native.expected_handoff.desired_shard
                || proof.cause != native.expected_handoff.cause
                || proof.desired_shard.shard_id != proposal.scope_shard.0
                || proof.desired_shard.epoch <= proof.retiring_shard.epoch
            {
                return Err(error(
                    "sealed import names a different actual handoff/epoch",
                ));
            }
            for mapping in &proposal.preserved_pending_operations {
                if mapping.get("disposition").and_then(Value::as_str)
                    != Some("requires-original-source-retirement")
                    && mapping.get("retired_reservation").and_then(Value::as_bool) != Some(true)
                {
                    continue;
                }
                let payload = decode_import_payload_data(
                    mapping
                        .get("source_payload")
                        .unwrap_or(&mapping["payload"])
                        .clone(),
                )?;
                let scope = mapping.get("source_scope");
                let fs: DfsFileSystemId = scope
                    .and_then(|value| value.get("file_system_id"))
                    .map(|value| serde_json::from_value(value.clone()).map_err(error))
                    .transpose()?
                    .unwrap_or(payload.file_system_id.clone());
                let shard: DfsMetadataShardId = scope
                    .and_then(|value| value.get("shard_id"))
                    .map(|value| serde_json::from_value(value.clone()).map_err(error))
                    .transpose()?
                    .unwrap_or(payload.shard_id.clone());
                let epoch = scope
                    .and_then(|value| value.get("owner_epoch"))
                    .and_then(Value::as_u64)
                    .unwrap_or(payload.owner_epoch);
                let digest = mapping
                    .get("source_operation_digest")
                    .and_then(Value::as_str)
                    .ok_or_else(|| error("source retirement capsule lacks exact source digest"))?;
                if !seal.source_retirements.iter().any(|retired| {
                    retired.file_system_id == fs
                        && retired.shard_id == shard
                        && retired.owner_epoch == epoch
                        && retired.operation_id == payload.operation_id
                        && retired.source_operation_digest == digest
                        && retired.joint_handoff_proof.retiring_shard.file_system_id == fs
                        && retired.joint_handoff_proof.retiring_shard.shard_id == shard.0
                        && retired.joint_handoff_proof.desired_shard.epoch > epoch
                }) {
                    return Err(error(
                        "source capsule has no actual exact ORIGINAL-scope retirement seal",
                    ));
                }
            }
            for reservation in data_reservation_refs(&proposal.source_cut)? {
                if !seal.data_reservation_retirements.iter().any(|retired| {
                    retired.reservation == reservation
                        && !retired.original_source_cut_digest.is_empty()
                        && retired.joint_handoff_proof.retiring_shard.file_system_id
                            == reservation.file_system_id
                        && retired.joint_handoff_proof.retiring_shard.shard_id
                            == reservation.original_shard_id.0
                        && retired.joint_handoff_proof.desired_shard.epoch
                            > retired.joint_handoff_proof.retiring_shard.epoch
                }) {
                    return Err(error(
                        "original DATA reservation has no actual exact retirement seal",
                    ));
                }
            }
            proof.handoff_id.clone()
        }
        (
            MetadataImportProposal::Standalone(local),
            MetadataImportCertificate::Standalone(seal),
        ) => {
            if local.owner_binding != seal.owner.binding() {
                return Err(error("standalone import changed its actual owner binding"));
            }
            seal.owner
                .validate_import(local, &session.source_root, image_root)?;
            for fs in &proposal.file_system_ids {
                let floors = ack_floor::acknowledged_source_floors_for_root(
                    &session.source_root,
                    fs,
                    proposal.source_cut.source_format_version,
                    &proposal.source_cut.scope_shard,
                )?;
                let mut reader = source_ancestry::source_ancestry_reader(
                    &proposal.source_cut,
                    &session.source_root,
                    &floors,
                )?;
                while reader
                    .next_chunk(source_ancestry::MAX_SOURCE_ANCESTRY_CHUNK_BYTES)?
                    .is_some()
                {}
            }
            local.owner_binding.clone()
        }
        _ => return Err(error("import certificate uses a different authority mode")),
    };
    for fs in &proposal.file_system_ids {
        let floors = ack_floor::acknowledged_source_floors_for_root(
            &session.source_root,
            fs,
            proposal.source_cut.source_format_version,
            &proposal.source_cut.scope_shard,
        )?;
        // The native seal follows authenticated receiver-ledger verification.
        // Recheck these immutable LOCAL files; primary evidence paths do not
        // identify receiver-local files.
        for floor in floors {
            if floor.journal_sequence > proposal.source_cut.source_journal_sequence
                || floor.journal_sequence == proposal.source_cut.source_journal_sequence
                    && ((!floor.state_fingerprint.is_empty()
                        && floor.state_fingerprint != proposal.source_cut.source_state_fingerprint)
                        || floor.durable_prefix_proof.as_ref().is_some_and(|proof| {
                            proposal.source_cut.source_exact_prefix.as_ref() != Some(proof)
                        }))
            {
                return Err(error(
                    "local retained floor advanced or forked before certified installation",
                ));
            }
        }
    }
    session.inventory.verify(&session.source_root)?;
    if image_root == session.destination_root {
        // The image already crossed the rename boundary. Acceptance still
        // follows a newly obtained native seal and exact retained-source check.
        // Recertification completed under actual retained-source custody.
        // Remove the durable intent only now, so later cold opens can take
        // the ordinary current-root path without repeating installation.
        if !prior {
            authority.resume_imported_namespace_jobs(
                &proposal.source_cut,
                certificate,
                &session.destination_root,
                &jobs,
            )?;
        }
        if !prior {
            complete_installed_import_marker(
                &session.destination_root,
                proposal,
                &identity,
                &installation_binding,
            )?;
        }
        complete_installation_intent(&session.staged_root, prior)?;
        return Ok(());
    }
    let parent = session
        .destination_root
        .parent()
        .ok_or_else(|| error("metadata target has no owning parent"))?;
    fs::create_dir_all(parent).map_err(error)?;
    let key = blake3::hash(
        &serde_json::to_vec(&(&session.destination_root, &certificate.proposal_fingerprint))
            .map_err(error)?,
    )
    .to_hex()
    .to_string();
    let intent = session.staged_root.join("installation.json");
    let retained = if prior && intent.is_file() {
        let original: Value = read_json(&intent)?;
        let retained = PathBuf::from(
            original["retained_source_root"]
                .as_str()
                .ok_or_else(|| error("original installation omits its retained role"))?,
        );
        if retained.parent() != Some(parent.join(".metadata-retained").as_path()) {
            return Err(error(
                "original installation retained role escapes its owning parent",
            ));
        }
        retained
    } else {
        parent.join(".metadata-retained").join(&key)
    };
    let state = serde_json::json!({"format_version":7,"proposal":proposal,"source_identity":session.inventory.fingerprint,"source_snapshot_fingerprint":session.inventory.snapshot_fingerprint().unwrap_or(&session.inventory.fingerprint),"source_root":session.source_root,"destination_root":session.destination_root,"candidate_root":proposal.staged_root,"retained_source_root":retained,"native_image":identity});
    if intent.is_file() {
        let original: Value = read_json(&intent)?;
        if original.get("format_version") != state.get("format_version")
            || original.get("proposal") != state.get("proposal")
            || original.get("destination_root") != state.get("destination_root")
            || original.get("candidate_root") != state.get("candidate_root")
            || original.get("native_image") != state.get("native_image")
            || original.get("retained_source_root") != state.get("retained_source_root")
        {
            return Err(error(
                "existing installation intent changes its exact proposal or physical roles",
            ));
        }
    } else {
        write_json(&intent, &state)?;
    }
    // These records are recovery/provenance data. They never deserialize into
    // authority; every interrupted retry returns to the native certifier.
    let candidate_marker = proposal.staged_root.join("metadata-import-v7.json");
    let source_retained = if session.source_root == session.destination_root {
        retained.clone()
    } else {
        session.source_root.clone()
    };
    if prior {
        archive_prior_stage_file(
            &session.staged_root,
            &candidate_marker,
            "completed-original-candidate-marker-v8.json",
        )?;
        archive_prior_stage_file(
            &session.staged_root,
            &proposal.staged_root.join("metadata-candidate-v7.json"),
            "completed-original-candidate-stamp-v8.json",
        )?;
    }
    write_json(
        &candidate_marker,
        &serde_json::json!({"format_version":7,"retained_source_root":source_retained,"source_identity":session.inventory.fingerprint,"source_snapshot_fingerprint":session.inventory.snapshot_fingerprint().unwrap_or(&session.inventory.fingerprint),"authority_binding":installation_binding,"source_cut":proposal.source_cut,"native_image":identity}),
    )?;
    sinomo_core::storage::durable_file::remove_file_if_exists_and_sync_parent(
        &proposal.staged_root.join("metadata-candidate-v7.json"),
    )
    .map_err(error)?;
    if session.destination_root.exists() {
        let qualified_data_exchange = match (proposal, certificate) {
            (MetadataImportProposal::Native(native), MetadataImportCertificate::Native(seal)) => {
                native_source_format(proposal.source_cut.source_format_version)
                    && !data_reservation_refs(&proposal.source_cut)?.is_empty()
                    && session.source_root == session.destination_root
                    && matches!(
                        &seal.joint_handoff_proof.cause,
                        sinomo_dfs::DfsMetadataShardHandoffCause::StorageImport { .. }
                    )
                    && proposal
                        .source_cut
                        .projection_assignments
                        .iter()
                        .any(|assignment| assignment == &native.expected_handoff.retiring_shard)
            }
            _ => false,
        };
        if current_root(&session.destination_root)?
            && !(proposal.source_cut.original_handoff.is_some()
                && session.source_root == session.destination_root)
            && !qualified_data_exchange
        {
            return Err(error(
                "current destination was published by a different import",
            ));
        }
        if retained.exists() {
            return Err(error(
                "retained source archive already exists before source exchange",
            ));
        }
        fs::create_dir_all(
            retained
                .parent()
                .ok_or_else(|| error("retained source has no parent"))?,
        )
        .map_err(error)?;
        fs::rename(&session.destination_root, &retained).map_err(error)?;
        sinomo_core::storage::durable_file::sync_directory(parent).map_err(error)?;
        sinomo_core::storage::durable_file::sync_directory(
            retained
                .parent()
                .ok_or_else(|| error("retained source has no parent"))?,
        )
        .map_err(error)?;
        if session.source_root == session.destination_root {
            SOURCE_FENCES
                .get_or_init(Mutex::default)
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&session.source_root);
            register_source_fence(&retained, &session._source_lock)?;
        }
    }
    fs::rename(&proposal.staged_root, &session.destination_root).map_err(error)?;
    sinomo_core::storage::durable_file::sync_directory(parent).map_err(error)?;
    let stage_parent = proposal
        .staged_root
        .parent()
        .ok_or_else(|| error("candidate has no parent"))?;
    if stage_parent != parent {
        sinomo_core::storage::durable_file::sync_directory(stage_parent).map_err(error)?;
    }
    let installed = inspect_import_candidate(&session.destination_root)?;
    if installed != identity {
        return Err(error(
            "installed current metadata image differs from its actual native certificate",
        ));
    }
    if !prior {
        authority.resume_imported_namespace_jobs(
            &proposal.source_cut,
            certificate,
            &session.destination_root,
            &jobs,
        )?;
    }
    if !prior {
        complete_installed_import_marker(
            &session.destination_root,
            proposal,
            &identity,
            &installation_binding,
        )?;
    }
    complete_installation_intent(&session.staged_root, prior)?;
    Ok(())
}

#[cfg(test)]
mod current_layout_data_tests {
    use super::*;

    fn copy_data(from: &Path, into: &Path) {
        fs::create_dir_all(into).unwrap();
        for entry in fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = into.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_data(&entry.path(), &target);
            } else {
                assert!(entry.file_type().unwrap().is_file());
                fs::copy(entry.path(), target).unwrap();
            }
        }
    }

    #[test]
    #[ignore = "requires actual saved current bootstrap metadata tree"]
    fn actual_current_log_only_bootstrap_is_current_and_preserves_invalid_data() {
        let saved = PathBuf::from(
            std::env::var("SINOMO_DFS_CURRENT_BOOTSTRAP_METADATA_ROOT")
                .expect("genuine saved current root/services/users DATA required"),
        );
        let copied = sinomo_testdir::TestDir::new("current-log-bootstrap-data");
        for scope in ["root", "services", "users"] {
            let root = copied.join(scope);
            copy_data(&saved.join(scope), &root);
            assert!(!root.join("records/tables/snapshot.sobj").exists());
            assert!(current_root(&root).unwrap());
            assert!(discover_metadata_source(&root, &root).unwrap().is_none());
            let expected = sinomo_dfs::read_current_metadata_table_log_prefix(&root)
                .unwrap()
                .expect("actual complete native9 log prefix");
            let normal = sinomo_dfs::LocalSobjMetadataBackend::open_for_inspection(
                DfsMetadataShardId::new(scope),
                root.clone(),
            )
            .unwrap();
            assert!(
                normal.export_table_snapshot().materialized_through_sequence
                    >= expected.journal_sequence
            );
            drop(normal);

            let active = root.join("records/tables/materialized.slog");
            let checkpoint = root.join("records/tables/materialized.checkpoint.slog");
            let original = fs::read(&active).unwrap();
            fs::rename(&active, &checkpoint).unwrap();
            fs::write(&active, []).unwrap();
            assert!(
                current_root(&root).unwrap(),
                "checkpoint-only role remains current"
            );
            assert_eq!(
                sinomo_dfs::read_current_metadata_table_log_prefix(&root).unwrap(),
                Some(expected)
            );
            fs::rename(&checkpoint, &active).unwrap();

            fs::write(&active, &original[..9]).unwrap();
            assert!(
                current_root(&root).is_err(),
                "incomplete first declaration cannot become an empty legacy source"
            );
            assert_eq!(fs::read(&active).unwrap(), original[..9]);
            let mut corrupt = original.clone();
            // The actual first batch begins after its four-byte record length.
            corrupt[4] ^= 1;
            fs::write(&active, &corrupt).unwrap();
            assert!(current_root(&root).is_err());
            assert_eq!(fs::read(&active).unwrap(), corrupt);
            let mut legacy = original.clone();
            legacy[8..10].copy_from_slice(&8_u16.to_le_bytes());
            fs::write(&active, &legacy).unwrap();
            assert!(
                !current_root(&root).unwrap(),
                "declared old data stays in migration only"
            );
            assert!(sinomo_dfs::read_current_metadata_table_log_prefix(&root).is_err());
            assert_eq!(fs::read(&active).unwrap(), legacy);
            fs::write(&active, &original).unwrap();
            let mut torn_suffix = original.clone();
            torn_suffix.extend_from_slice(&[1, 0]);
            fs::write(&active, &torn_suffix).unwrap();
            assert!(current_root(&root).unwrap());
            assert_eq!(fs::read(&active).unwrap(), torn_suffix);
            fs::write(&active, &original).unwrap();
            // Classification reads only the declaration. A later repeated
            // complete actual batch must still fail normal full replay.
            let first_len = u32::from_le_bytes(original[..4].try_into().unwrap()) as usize;
            let mut repeated = original.clone();
            repeated.extend_from_slice(&original[..4 + first_len]);
            fs::write(&active, &repeated).unwrap();
            assert!(current_root(&root).unwrap());
            assert!(
                sinomo_dfs::LocalSobjMetadataBackend::open_for_inspection(
                    DfsMetadataShardId::new(scope),
                    &root
                )
                .is_err()
            );
            assert_eq!(fs::read(&active).unwrap(), repeated);
            fs::write(&active, &original).unwrap();
        }
        // Obtain zero cursor bytes from the ordinary current backend, using
        // the existing shared executor. Copy only that real bookkeeping DATA;
        // the destination never receives a fabricated prefix or publication.
        let name = "current-empty-cursor-layout";
        let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, 1);
        let fixture =
            sinomo_dfs::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
        let handle = fixture.runtime_handle();
        let _entered = handle.enter();
        let initialized = copied.join("initialized/users");
        let backend = sinomo_dfs::LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("users"),
            &initialized,
        )
        .unwrap();
        let cursor =
            fs::read(initialized.join("records/local-materialization/cursor.sobj")).unwrap();
        let actual: Value = sinomo_core::object::serde_ext::from_bytes(&cursor).unwrap();
        assert_eq!(actual["last_sequence"], serde_json::json!(0));
        assert_eq!(actual["shard_id"], serde_json::json!("users"));
        drop(backend);
        let native_snapshot = initialized.join("records/tables/snapshot.sobj");
        let snapshot_bytes = fs::read(&native_snapshot).unwrap();
        let mixed = copied.join("mixed/users");
        fs::create_dir_all(mixed.join("records/tables")).unwrap();
        let mixed_snapshot = mixed.join("records/tables/snapshot.sobj");
        fs::write(&mixed_snapshot, &snapshot_bytes).unwrap();
        let mixed_log = mixed.join("records/tables/materialized.slog");
        let mut declared_old_log =
            fs::read(saved.join("users/records/tables/materialized.slog")).unwrap();
        declared_old_log[8..10].copy_from_slice(&8_u16.to_le_bytes());
        fs::write(&mixed_log, &declared_old_log).unwrap();
        assert!(
            current_root(&mixed).is_err(),
            "current snapshot cannot hide an old declared log"
        );
        assert_eq!(fs::read(&mixed_snapshot).unwrap(), snapshot_bytes);
        assert_eq!(fs::read(&mixed_log).unwrap(), declared_old_log);
        // A negative declaration mutation of a real current snapshot cannot
        // admit its unchanged current log as an original legacy source.
        let header_bytes =
            sinomo_core::storage::migration::stream::read_header(&native_snapshot).unwrap();
        let mut header: Value = sinomo_core::object::serde_ext::from_bytes(&header_bytes).unwrap();
        header["format_version"] = serde_json::json!(8);
        let old_header = sinomo_core::object::serde_ext::to_bytes(
            &header,
            8,
            sinomo_core::object::LengthMode::U32,
        )
        .unwrap();
        fs::remove_file(&mixed_snapshot).unwrap();
        drop(
            sinomo_core::storage::migration::stream::MigrationStreamWriter::resume(
                &mixed_snapshot,
                &old_header,
                &Default::default(),
            )
            .unwrap(),
        );
        let declared_old_snapshot = fs::read(&mixed_snapshot).unwrap();
        fs::copy(
            saved.join("users/records/tables/materialized.slog"),
            &mixed_log,
        )
        .unwrap();
        assert!(
            current_root(&mixed).is_err(),
            "old snapshot cannot hide a current declared log"
        );
        assert_eq!(fs::read(&mixed_snapshot).unwrap(), declared_old_snapshot);
        let foreign_current = copied.join("initialized/foreign");
        copy_data(&saved.join("users"), &foreign_current);
        let actual_log =
            fs::read(foreign_current.join("records/tables/materialized.slog")).unwrap();
        assert!(
            sinomo_dfs::LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new("foreign"),
                &foreign_current
            )
            .is_err()
        );
        assert_eq!(
            fs::read(foreign_current.join("records/tables/materialized.slog")).unwrap(),
            actual_log
        );
        drop(_entered);
        fixture.close().unwrap();
        let empty = copied.join("bookkeeping/users");
        let cursor_path = empty.join("records/local-materialization/cursor.sobj");
        fs::create_dir_all(cursor_path.parent().unwrap()).unwrap();
        fs::write(&cursor_path, &cursor).unwrap();
        assert!(!source::has_migration_source_data(&empty).unwrap());
        assert!(discover_metadata_source(&empty, &empty).unwrap().is_none());
        assert_eq!(fs::read(&cursor_path).unwrap(), cursor);
        let foreign = copied.join("bookkeeping/foreign");
        copy_data(&empty, &foreign);
        assert!(source::has_migration_source_data(&foreign).unwrap());
        assert_eq!(
            discover_metadata_source(&foreign, &foreign).unwrap(),
            Some(foreign)
        );
        let advanced =
            fs::read(saved.join("users/records/local-materialization/cursor.sobj")).unwrap();
        let actual: Value = sinomo_core::object::serde_ext::from_bytes(&advanced).unwrap();
        assert!(actual["last_sequence"].as_u64().unwrap() > 0);
        fs::write(&cursor_path, &advanced).unwrap();
        assert!(source::has_migration_source_data(&empty).unwrap());
        fs::write(&cursor_path, &cursor).unwrap();
        let ack = "coordination/metadata-prefix-ack-floors/users.sobj";
        let ack_path = empty.join(ack);
        fs::create_dir_all(ack_path.parent().unwrap()).unwrap();
        fs::copy(saved.join("users").join(ack), &ack_path).unwrap();
        assert!(source::has_migration_source_data(&empty).unwrap());
        assert_eq!(
            fs::read(ack_path).unwrap(),
            fs::read(saved.join("users").join(ack)).unwrap()
        );
    }
}

#[cfg(test)]
mod recovery_session_tests {
    use super::*;

    #[test]
    fn prior_storage_generation_is_discovered_without_current_admission() {
        use sinomo_core::object::{LengthMode, serde_ext};
        use sinomo_core::storage::migration::stream::MigrationStreamWriter;
        let dir = sinomo_testdir::TestDir::new("metadata-prior-generation-discovery");
        let root = dir.join("root");
        let path = root.join("records/tables/snapshot.sobj");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        // A structural negative input: no prefix/rows/authority are fabricated.
        // Generation 8 is rejected before the current header fields are decoded.
        let header =
            serde_ext::to_bytes(&serde_json::json!({"format_version":8}), 8, LengthMode::U32)
                .unwrap();
        drop(MigrationStreamWriter::resume(&path, &header, &Default::default()).unwrap());
        let original = fs::read(&path).unwrap();
        assert_eq!(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION, 9);
        assert!(!current_root(&root).unwrap());
        assert_eq!(discover_metadata_source(&root, &root).unwrap(), Some(root));
        assert_eq!(fs::read(path).unwrap(), original);
    }

    #[test]
    fn new_target_generation_preserves_prior_ledger_and_pending_exchange() {
        let dir = sinomo_testdir::TestDir::new("metadata-prior-generation-ledger");
        let destination = dir.join("root");
        let shard = DfsMetadataShardId::new("root");
        let previous = unversioned_conversion_stage_base_path(&destination, &shard).unwrap();
        fs::create_dir_all(&previous).unwrap();
        let progress = previous.join("progress.json");
        fs::write(&progress, b"retained source replay ledger").unwrap();
        let current = conversion_stage_base_path(&destination, &shard).unwrap();
        assert_eq!(current, previous.join("target-format-9"));
        assert_eq!(
            fs::read(&progress).unwrap(),
            b"retained source replay ledger"
        );
        let installation = previous.join("installation.json");
        fs::write(&installation, b"unfinished actual exchange sentinel").unwrap();
        assert_eq!(
            unversioned_conversion_stage_path(&destination, &shard).unwrap(),
            previous
        );
        assert_eq!(
            fs::read(installation).unwrap(),
            b"unfinished actual exchange sentinel"
        );
        assert_eq!(
            fs::read(progress).unwrap(),
            b"retained source replay ledger"
        );
    }

    #[test]
    fn actual_source_writer_lock_contention_is_typed_pending_without_capture() {
        let root = sinomo_testdir::TestDir::new("metadata-actual-source-writer-busy");
        let source = root.path().join("source");
        fs::create_dir_all(source.join("journal")).unwrap();
        let lock_path = source.join("journal/write.lock");
        let actual_writer = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        actual_writer.lock_exclusive().unwrap();
        let result = retain_source_fence(&source);
        assert!(matches!(result,
            Err(MetadataBackendError::StoragePreparationPending { root: pending_root, processed_records: 0 })
            if pending_root == source.canonicalize().unwrap()));
        drop(actual_writer);
        let captured = retain_source_fence(&source).unwrap();
        let competitor = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        assert_eq!(
            competitor.try_lock_exclusive().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        drop(captured);
        competitor.try_lock_exclusive().unwrap();
    }

    #[test]
    fn source_writer_open_error_is_not_reported_as_pending() {
        let root = sinomo_testdir::TestDir::new("metadata-source-writer-nonlock-error");
        let source = root.path().join("source");
        fs::create_dir_all(source.join("journal/write.lock")).unwrap();
        let result = retain_source_fence(&source);
        assert!(
            matches!(result, Err(MetadataBackendError::Serde(_))),
            "an actual open error stays an error, never a lock/recovery retry grant"
        );
    }

    #[test]
    fn poisoned_projection_is_discarded_before_releasing_source_custody() {
        let root = sinomo_testdir::TestDir::new("metadata-conversion-poisoned-session");
        let source = root.path().join("source");
        fs::create_dir_all(&source).unwrap();
        let destination = root.path().join("destination");
        let shard = DfsMetadataShardId::new("current");
        let slot = Arc::new(RootSlot {
            shard: shard.clone(),
            session: Mutex::new(Some(open_session(&destination, &source, &shard).unwrap())),
        });
        let queued = Arc::clone(&slot);
        assert!(
            std::thread::spawn(move || {
                let mut state = queued.lock_session();
                state.as_mut().unwrap().replayed_events = 11;
                panic!("unwind after partial scratch projection");
            })
            .join()
            .is_err()
        );
        let lock_path = source.join("journal/write.lock");
        let competing = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        assert!(
            competing.try_lock_exclusive().is_err(),
            "source custody must survive until the poisoned state is discarded"
        );
        let state = slot.lock_session();
        assert!(
            state.is_none(),
            "a queued caller must not receive the partial projection"
        );
        assert!(!slot.session.is_poisoned());
        competing.try_lock_exclusive().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn concurrent_directory_aliases_share_one_stable_slot() {
        let root = sinomo_testdir::TestDir::new("metadata-conversion-alias-slot");
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(root.path(), &alias).unwrap();
        let destination = root.path().join("new-current-root");
        let aliased = alias.join("new-current-root");
        assert_eq!(
            canonical_destination(&destination).unwrap(),
            canonical_destination(&aliased).unwrap()
        );
        let converter = Arc::new(MetadataConverter::default());
        let first = Arc::clone(&converter);
        let shard = DfsMetadataShardId::new("current");
        let first_shard = shard.clone();
        let worker = std::thread::spawn(move || first.prepare(&destination, &first_shard, 1));
        // An empty storage path needs no conversion. This is not publication
        // authorization; normal DFS genesis still requires its native guard.
        assert_eq!(
            converter.prepare(&aliased, &shard, 1).unwrap(),
            DfsStoragePreparation::Ready
        );
        assert_eq!(
            worker.join().unwrap().unwrap(),
            DfsStoragePreparation::Ready
        );
        let slots = converter.roots.lock().unwrap();
        assert_eq!(slots.len(), 1);
        assert!(slots.values().next().unwrap().lock_session().is_none());
    }
}

#[cfg(test)]
mod data_goal_stage_tests {
    use super::*;
    fn capsule(shard: &str) -> Value {
        let fs = sinomo_core::Id::from_legacy_named("old-placement-source-fs").unwrap();
        let original = serde_json::json!({"job_id":"old-job","file_system_id":"old-placement-source-fs",
            "path":"/services","scope":"subtree","state":"repairing",
            "total_object_count":1,"scanned_object_count":3,"matched_object_count":2,"updated_object_count":1,
            "scheduled_repair_count":1,"completed_repair_count":0,"pending_repair_count":4,
            "revision":9,"created_at_unix_nanos":42,"updated_at_unix_nanos":7});
        let bytes = sinomo_core::object::serde_ext::to_bytes(
            &original,
            1,
            sinomo_core::object::LengthMode::U32,
        )
        .unwrap();
        let dir = sinomo_testdir::TestDir::new("metadata-data-goal-original-sobj");
        let path = dir.join("original.sobj");
        std::fs::write(&path, &bytes).unwrap();
        let entry = sinomo_core::kv::KvEntry {
            key: "/dfs/filesystems/old-placement-source-fs/placement-reconciliations/old-job"
                .into(),
            value: std::fs::read(path).unwrap(),
            create_revision: sinomo_core::kv::KvRevision(8),
            modify_revision: sinomo_core::kv::KvRevision(12),
            version: sinomo_core::kv::KvVersion(2),
            lease_id: Some(sinomo_core::kv::LeaseId(17)),
        };
        // Hash the exact typed source entry, including its serialization order.
        // A JSON Value map would reorder fields before computing this digest.
        let capsule_digest = blake3::hash(
            &serde_json::to_vec(&("original-placement-reservation-v0", &entry, &fs, shard))
                .unwrap(),
        )
        .to_hex()
        .to_string();
        let source_entry_digest =
            blake3::hash(&serde_json::to_vec(&("original-placement-job-v0", &entry)).unwrap())
                .to_hex()
                .to_string();
        serde_json::json!({"capsule_kind":"placement-job-v0",
            "capsule_digest":capsule_digest,
            "source_entry_digest":source_entry_digest,
            "source_row":entry,"original_source_scope":{"file_system_id":fs,"shard_id":shard},
            "disposition":"data-after-original-reservation-retirement","required_native_work":"current-placement-reissue"})
    }
    fn retained_progress_data(goal: &DataGoalStage) -> Value {
        // Cursor DATA only; no metadata image, certificate, writer or authority
        // is installed by these private persisted-codec controls.
        serde_json::json!({
            "source": source::SourceCursor::default(),
            "events": sinomo_core::storage::migration::stream::StreamCursor::default(),
            "phase": "Source",
            "tail_after": 37,
            "current_import_candidate_count": 4,
            "data_reservation_capsules": goal.capsules,
        })
    }

    #[test]
    fn retained_progress_import_preserves_cursor_and_restarts_by_exact_goal_digest() {
        let dir = sinomo_testdir::TestDir::new("metadata-progress-goal-reference");
        let destination = dir.join("root");
        let shard = DfsMetadataShardId::new("root");
        let (capsules, _) = validated_placement_capsules(&[capsule("root")], &shard).unwrap();
        assert!(
            select_data_goal(
                &destination,
                &shard,
                capsules,
                false,
                &mut active_data_goal(&destination, &shard).unwrap()
            )
            .unwrap()
        );
        let goal = active_data_goal(&destination, &shard).unwrap().unwrap();
        let retained = retained_progress_data(&goal);
        let imported = progress_for_data_goal(retained.clone(), Some(&goal)).unwrap();
        let current = serde_json::to_value(&imported).unwrap();
        for key in [
            "source",
            "events",
            "phase",
            "tail_after",
            "current_import_candidate_count",
        ] {
            assert_eq!(current[key], retained[key], "{key}");
        }
        assert_eq!(current["data_goal_digest"], goal.digest);
        assert!(current.get("data_reservation_capsules").is_none());
        let restarted = progress_for_data_goal(current.clone(), Some(&goal)).unwrap();
        assert_eq!(serde_json::to_value(restarted).unwrap(), current);
    }

    #[test]
    fn progress_goal_import_rejects_changed_capsules_and_mixed_or_foreign_references() {
        let dir = sinomo_testdir::TestDir::new("metadata-progress-goal-negative");
        let destination = dir.join("root");
        let shard = DfsMetadataShardId::new("root");
        let (capsules, _) = validated_placement_capsules(&[capsule("root")], &shard).unwrap();
        assert!(
            select_data_goal(
                &destination,
                &shard,
                capsules,
                false,
                &mut active_data_goal(&destination, &shard).unwrap()
            )
            .unwrap()
        );
        let goal = active_data_goal(&destination, &shard).unwrap().unwrap();
        let retained = retained_progress_data(&goal);
        let mut changed = retained.clone();
        changed["data_reservation_capsules"][0]["source_row"]["modify_revision"] =
            serde_json::json!(99);
        assert!(progress_for_data_goal(changed, Some(&goal)).is_err());
        assert!(progress_for_data_goal(retained.clone(), None).is_err());
        let imported = progress_for_data_goal(retained.clone(), Some(&goal)).unwrap();
        let current = serde_json::to_value(imported).unwrap();
        let mut changed = current.clone();
        changed["data_goal_digest"] = serde_json::json!("another-goal");
        assert!(progress_for_data_goal(changed, Some(&goal)).is_err());
        let mut changed = current.clone();
        changed["data_reservation_capsules"] = retained["data_reservation_capsules"].clone();
        assert!(progress_for_data_goal(changed, Some(&goal)).is_err());
        let mut changed = retained.clone();
        changed["foreign_field"] = serde_json::json!(true);
        assert!(progress_for_data_goal(changed, Some(&goal)).is_err());

        let mut changed_goal = goal.clone();
        changed_goal.destination = dir.join("another-root");
        assert!(validate_data_goal(&changed_goal, &destination, &shard).is_err());
        let mut changed_goal = goal.clone();
        changed_goal.shard = DfsMetadataShardId::new("other");
        assert!(validate_data_goal(&changed_goal, &destination, &shard).is_err());
        let mut changed_goal = goal.clone();
        changed_goal.digest = "another-goal".into();
        assert!(validate_data_goal(&changed_goal, &destination, &shard).is_err());
        let mut changed_goal = goal.clone();
        changed_goal.capsules[0]["source_row"]["modify_revision"] = serde_json::json!(99);
        assert!(validate_data_goal(&changed_goal, &destination, &shard).is_err());
    }

    #[test]
    fn progress_without_data_goal_retains_required_null_reference_and_rejects_capsules() {
        let retained = serde_json::json!({
            "source": source::SourceCursor::default(),
            "events": sinomo_core::storage::migration::stream::StreamCursor::default(),
            "phase": "Source",
            "tail_after": 0,
            "current_import_candidate_count": null,
        });
        let imported = progress_for_data_goal(retained, None).unwrap();
        let current = serde_json::to_value(imported).unwrap();
        assert_eq!(current["data_goal_digest"], Value::Null);
        assert!(current.get("data_reservation_capsules").is_none());
        assert!(progress_for_data_goal(current, None).is_ok());
    }

    #[test]
    #[ignore = "requires exact retained progress and active goal DATA manifest; no native authority"]
    fn actual_retained_progress_goal_pairs_import_without_changing_any_cursor_fact() {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Pair {
            node: u32,
            progress: PathBuf,
            goal: PathBuf,
            progress_blake3: String,
            goal_blake3: String,
            destination: PathBuf,
            shard: DfsMetadataShardId,
            goal_digest: String,
        }
        let path = std::env::var_os("SINOMO_MIGRATION_PROGRESS_GOAL_MANIFEST")
            .expect("actual retained progress/goal manifest");
        let pairs: Vec<Pair> = read_json(Path::new(&path)).unwrap();
        assert_eq!(pairs.len(), 6, "the actual retained six owner DATA pairs");
        for pair in pairs {
            let goal_bytes = fs::read(&pair.goal).unwrap();
            assert_eq!(
                blake3::hash(&goal_bytes).to_hex().as_str(),
                pair.goal_blake3
            );
            let goal: DataGoalStage = serde_json::from_slice(&goal_bytes).unwrap();
            validate_data_goal(&goal, &pair.destination, &pair.shard).unwrap();
            assert_eq!(goal.digest, pair.goal_digest);
            let progress_bytes = fs::read(&pair.progress).unwrap();
            assert_eq!(
                blake3::hash(&progress_bytes).to_hex().as_str(),
                pair.progress_blake3
            );
            let retained: Value = serde_json::from_slice(&progress_bytes).unwrap();
            let imported = progress_for_data_goal(retained.clone(), Some(&goal)).unwrap();
            let current = serde_json::to_value(imported).unwrap();
            assert!(current.get("data_reservation_capsules").is_none());
            for key in [
                "source",
                "events",
                "phase",
                "tail_after",
                "current_import_candidate_count",
            ] {
                assert_eq!(current[key], retained[key], "node{}:{key}", pair.node);
            }
            assert_eq!(current["data_goal_digest"], goal.digest);
            let restarted = progress_for_data_goal(current.clone(), Some(&goal)).unwrap();
            assert_eq!(serde_json::to_value(restarted).unwrap(), current);
            let mut changed = retained.clone();
            if let Some(capsules) = changed
                .get_mut("data_reservation_capsules")
                .and_then(Value::as_array_mut)
            {
                capsules.pop();
            } else {
                changed["data_goal_digest"] = serde_json::json!("different-goal");
            }
            assert!(progress_for_data_goal(changed, Some(&goal)).is_err());
        }
    }

    #[test]
    fn bound_original_sobj_data_goal_restarts_and_rejects_tampering() {
        let dir = sinomo_testdir::TestDir::new("metadata-data-goal-role");
        let destination = dir.join("root");
        let shard = DfsMetadataShardId::new("root");
        let row = capsule("root");
        let (rows, _) = validated_placement_capsules(&[row.clone()], &shard).unwrap();
        assert!(
            select_data_goal(
                &destination,
                &shard,
                rows.clone(),
                false,
                &mut active_data_goal(&destination, &shard).unwrap()
            )
            .unwrap()
        );
        let first = conversion_stage_path(&destination, &shard).unwrap();
        assert_eq!(
            active_data_goal(&destination, &shard)
                .unwrap()
                .unwrap()
                .capsules,
            rows
        );
        assert!(
            select_data_goal(
                &destination,
                &shard,
                rows,
                true,
                &mut active_data_goal(&destination, &shard).unwrap()
            )
            .unwrap()
        );
        assert_eq!(conversion_stage_path(&destination, &shard).unwrap(), first);
        assert!(validated_placement_capsules(&[row.clone(), row.clone()], &shard).is_err());
        assert!(validated_placement_capsules(&[capsule("other")], &shard).is_err());
        let pointer = conversion_stage_base_path(&destination, &shard)
            .unwrap()
            .join("active-data-goal.json");
        let mut value: Value = read_json(&pointer).unwrap();
        value["capsules"][0]["source_row"]["modify_revision"] = Value::from(99);
        write_json(&pointer, &value).unwrap();
        assert!(active_data_goal(&destination, &shard).is_err());
    }
    #[test]
    fn new_data_goal_never_discards_unfinished_installation() {
        let dir = sinomo_testdir::TestDir::new("metadata-data-goal-pending-install");
        let destination = dir.join("root");
        let shard = DfsMetadataShardId::new("root");
        let base = conversion_stage_base_path(&destination, &shard).unwrap();
        std::fs::create_dir_all(&base).unwrap();
        let intent = base.join("installation.json");
        std::fs::write(&intent, b"actual pending installation sentinel").unwrap();
        let (rows, _) = validated_placement_capsules(&[capsule("root")], &shard).unwrap();
        assert!(
            !select_data_goal(
                &destination,
                &shard,
                rows,
                false,
                &mut active_data_goal(&destination, &shard).unwrap()
            )
            .unwrap()
        );
        assert_eq!(
            std::fs::read(intent).unwrap(),
            b"actual pending installation sentinel"
        );
        assert!(!base.join("active-data-goal.json").exists());
    }
}

#[cfg(test)]
mod native_source_fenced_inspection_tests {
    use super::*;

    #[test]
    fn current_prefix_ancestry_reads_actual_frames_while_its_source_writer_is_retained() {
        use sinomo_dfs::test_support::DfsTestRuntime;
        let name = "native-source-fenced-prefix-inspection";
        let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
        let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
        native.start_nodes().unwrap();
        let mut client = native.client(&node).unwrap();
        let shard = DfsMetadataShardId::new("services");
        let root = native.data_dir(&node).dfs_dir().join("metadata/services");
        let physical_prefix = || {
            let current = sinomo_dfs::LocalSobjMetadataBackend::open_for_inspection(
                shard.clone(),
                root.clone(),
            )
            .unwrap();
            sinomo_dfs::DfsMetadataBackend::metadata_prefix_proof(&current, &shard)
                .unwrap()
                .unwrap()
        };
        client
            .posix_write_file(
                "/services/source-fenced-inspection",
                b"first",
                sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
        let anchor = physical_prefix();
        client
            .posix_write_file(
                "/services/source-fenced-inspection",
                b"second",
                sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
        let current = physical_prefix();
        assert!(current.journal_sequence > anchor.journal_sequence);
        // SAME converter primitive, actual file and Main-generated frame chain.
        // The inspection must not try to acquire this OS writer a second time.
        let fence = retain_source_fence(&root).unwrap();
        assert_eq!(
            sinomo_dfs::inspect_metadata_prefix_descendant(&root, &shard, &anchor).unwrap(),
            Some(current.clone()),
        );
        let mut fork = anchor.clone();
        fork.digest = format!(
            "dfs-metadata-prefix-v2:blake3:{}",
            blake3::hash(b"foreign anchor").to_hex()
        );
        assert!(
            sinomo_dfs::inspect_metadata_prefix_descendant(&root, &shard, &fork)
                .unwrap()
                .is_none()
        );
        assert!(
            !matches!(
                sinomo_dfs::inspect_metadata_prefix_descendant(
                    &root,
                    &DfsMetadataShardId::new("foreign-shard"),
                    &anchor,
                ),
                Ok(Some(_))
            ),
            "a foreign shard cannot establish current ancestry"
        );
        let segments = root.join("journal/segments");
        let segment = std::fs::read_dir(&segments)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("slog"))
            .expect("actual normal journal segment");
        let hidden = segment.with_extension("source-test-held");
        std::fs::rename(&segment, &hidden).unwrap();
        let missing = sinomo_dfs::inspect_metadata_prefix_descendant(&root, &shard, &anchor);
        std::fs::rename(&hidden, &segment).unwrap();
        assert!(
            !matches!(missing, Ok(Some(_))),
            "missing actual suffix bytes cannot prove ancestry"
        );
        assert_eq!(
            sinomo_dfs::inspect_metadata_prefix_descendant(&root, &shard, &anchor).unwrap(),
            Some(current),
        );
        drop(fence);
        drop(client);
        native.close().unwrap();
    }
}
