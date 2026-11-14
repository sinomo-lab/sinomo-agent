//! Small DFS control-plane records stored in `sinomo-kv`.
//!
//! This module intentionally does not store per-file or per-object metadata.
//! `sinomo-kv` is for compact, cluster-wide configuration such as DfsFileSystem
//! roots and metadata-shard placement. File/object namespace metadata belongs
//! in the DFS metadata service and its sharded local stores.

use crate::DfsFileSystemId;
use crate::namespace::{SIFS_SERVICES_PATH, SIFS_USERS_PATH};
use crate::placement::{
    DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION, placement_config_remains_authoritative_for_job,
    placement_reconciliation_activity_page_from_summaries, scoped_placement_config_for_job,
};
use crate::{
    DfsAdaptivePlacementObservationState, DfsPlacementReconciliationActivityCursor,
    DfsPlacementReconciliationActivityPage, DfsPlacementReconciliationActivityStatus,
    DfsPlacementReconciliationActivitySummary, DfsPlacementReconciliationJob, sifs_file_system_id,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT;
use sinomo_core::kv::{
    InProcessKvStore, KvCompare, KvEntry, KvError, KvOp, KvRequestId, KvRequestOutcome, KvRevision,
    KvTxn, KvTxnRequest, KvTxnResponse,
};
use sinomo_core::node::{
    ClusterCapabilityPolicyRecord, DFS_METADATA_PREPARED_OPERATION_V7_CAPABILITY, NodeId,
    cluster_capability_policy_key, cluster_member_key,
};
use sinomo_core::{ClusterMemberRecord, ClusterMemberState, DfsObjectPolicy};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

mod write_set;
pub use write_set::*;

const DFS_KV_PREFIX: &str = "/dfs";
const METADATA_HEAD_WATERMARK_PUBLISH_RETRIES: usize = 4;
const METADATA_SHARD_AUTHORITY_READ_RETRIES: usize = 4;
const METADATA_MEMBERSHIP_HANDOFF_RETRIES: usize = 4;
const PLACEMENT_RECONCILIATION_QUEUE_SCHEMA_VERSION: u32 = 1;
const PLACEMENT_RECONCILIATION_ACTIVITY_INDEX_SCHEMA_VERSION: u32 = 1;
const PLACEMENT_RECONCILIATION_ACTIVITY_RECORD_SCHEMA_VERSION: u32 = 1;
const PLACEMENT_RECONCILIATION_QUEUE_CAS_RETRIES: usize = 8;
const MAX_ACTIVE_PLACEMENT_RECONCILIATIONS: usize = 1_024;
const MAX_RECENT_PLACEMENT_RECONCILIATION_ACTIVITIES: usize = 256;
const FILE_SYSTEM_CONFIG_CAS_RETRIES: usize = 8;
const PLACEMENT_AUTHORITY_RECOVERY_CAS_RETRIES: usize = 4;

/// The exact native R0 response, retained without a second authority read.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNativeTxnCommit {
    pub request: sinomo_core::kv::KvTxnRequest,
    pub result: sinomo_core::kv::ClusterKvTxnCompactCommitResult,
}

fn metadata_operation_authority_view_error_is_retryable(error: &KvError) -> bool {
    match error {
        KvError::Busy { .. }
        | KvError::LeaderOffline { .. }
        | KvError::QuorumUnavailable { .. }
        | KvError::CommittedViewUnavailable { .. }
        | KvError::AuthorityUnavailable { .. } => true,
        // Cluster-backed clients retain the typed cluster error inside the KV
        // filesystem adapter. This exact marker is emitted when ReadIndex
        // reached quorum but the resident Raft generation changed before the
        // projection was consumed. Retrying the same exact-key authority read
        // is safe: the read has no effect.
        KvError::Io { detail, .. } => {
            detail.contains("resident Raft view changed after ReadIndex quorum")
                && detail.contains("retry the read")
        }
        _ => false,
    }
}

fn metadata_shard_authority_read_deadline(operation_deadline: Option<Instant>) -> Instant {
    let started = Instant::now();
    let authority_deadline = started
        .checked_add(DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT)
        .unwrap_or(started);
    operation_deadline
        .map(|deadline| deadline.min(authority_deadline))
        .unwrap_or(authority_deadline)
}

fn metadata_shard_authority_deadline_error() -> KvError {
    KvError::AuthorityUnavailable {
        detail: "metadata shard authority read deadline elapsed".to_string(),
    }
}

/// Exact phrase required by the deliberately exceptional placement-authority
/// break-glass path. Callers must treat the matching value as an operator
/// acknowledgement, not as a generic force flag.
pub const DFS_PLACEMENT_AUTHORITY_BREAK_GLASS_ACKNOWLEDGEMENT: &str =
    "all-prior-placement-writers-are-stopped-or-fenced";

/// One exclusion latch coupling a v1 reconciliation authority check to one
/// bounded metadata mutation.
///
/// This record deliberately has no wall-clock deadline or expiring KV lease.
/// Metadata writes do not yet carry a fencing token, so allowing a stalled
/// holder to expire would let a newer Apply commit while the old holder can
/// still mutate metadata. Apply and other filesystem-config writers therefore
/// compare the latch missing in the same Raft transaction that changes policy.
///
/// Every new holder is bound to both its node and process session. This lets a
/// fresh `NetworkedDfsService` in the same daemon reclaim a release whose KV
/// response was lost, while a restarted daemon may recover only its own prior
/// process after acquiring the exclusive local data-owner lock. There is
/// intentionally no time-based or cross-node automatic recovery path.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct DfsPlacementAuthorityPermitRecord {
    job_id: String,
    holder_id: String,
    holder_cluster_id: String,
    holder_node_id: NodeId,
    holder_session_id: String,
}

#[derive(Debug)]
pub(crate) struct DfsPlacementAuthorityPermit {
    file_system_id: DfsFileSystemId,
    record: DfsPlacementAuthorityPermitRecord,
    released: bool,
    #[cfg(test)]
    observation: Option<crate::test_runtime::PlacementAuthorityCallObservation>,
}

/// Opaque daemon-incarnation identity embedded in every new placement permit.
/// Cluster and process identity are supplied by the control-plane host; the
/// DFS service supplies the local node id at acquisition time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsPlacementAuthorityProcessIdentity {
    pub cluster_id: String,
    pub process_session_id: String,
}

pub use sinomo_api::dfs::control_plane::DfsPlacementAuthorityPermitStatus;

pub use sinomo_api::dfs::control_plane::DfsPlacementAuthorityRecoveryRefusal;

pub use sinomo_api::dfs::control_plane::DfsPlacementAuthorityRecoveryOutcome;

#[derive(Clone, Debug, Eq, PartialEq)]
struct DfsPlacementAuthorityPendingRelease {
    file_system_id: DfsFileSystemId,
    record: DfsPlacementAuthorityPermitRecord,
}

static PENDING_PLACEMENT_AUTHORITY_RELEASES: OnceLock<
    Mutex<Vec<DfsPlacementAuthorityPendingRelease>>,
> = OnceLock::new();
static IN_PROCESS_PLACEMENT_AUTHORITY_SESSION_ID: OnceLock<String> = OnceLock::new();

fn pending_placement_authority_releases() -> &'static Mutex<Vec<DfsPlacementAuthorityPendingRelease>>
{
    PENDING_PLACEMENT_AUTHORITY_RELEASES.get_or_init(|| Mutex::new(Vec::new()))
}

fn with_pending_placement_authority_releases<R>(
    action: impl FnOnce(&mut Vec<DfsPlacementAuthorityPendingRelease>) -> R,
) -> R {
    let mut pending = pending_placement_authority_releases()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    action(&mut pending)
}

fn queue_placement_authority_release(permit: &DfsPlacementAuthorityPermit) {
    queue_placement_authority_record_release(&permit.file_system_id, &permit.record);
}

fn queue_placement_authority_record_release(
    file_system_id: &DfsFileSystemId,
    record: &DfsPlacementAuthorityPermitRecord,
) {
    let pending = DfsPlacementAuthorityPendingRelease {
        file_system_id: file_system_id.clone(),
        record: record.clone(),
    };
    with_pending_placement_authority_releases(|releases| {
        if !releases.contains(&pending) {
            releases.push(pending);
        }
    });
}

fn placement_authority_release_is_pending(
    file_system_id: &DfsFileSystemId,
    record: &DfsPlacementAuthorityPermitRecord,
) -> bool {
    with_pending_placement_authority_releases(|releases| {
        releases
            .iter()
            .any(|pending| pending.file_system_id == *file_system_id && pending.record == *record)
    })
}

fn clear_pending_placement_authority_release(
    file_system_id: &DfsFileSystemId,
    record: &DfsPlacementAuthorityPermitRecord,
) {
    with_pending_placement_authority_releases(|releases| {
        releases.retain(|pending| {
            pending.file_system_id != *file_system_id || pending.record != *record
        });
    });
}

impl Drop for DfsPlacementAuthorityPermit {
    fn drop(&mut self) {
        if !self.released {
            // A panic or early unwind after acquisition is positive evidence
            // that this bounded mutation's stack has exited. Preserve the
            // exact handle process-wide so a fresh service can finish release.
            queue_placement_authority_release(self);
        }
    }
}

#[cfg(test)]
impl DfsPlacementAuthorityPermit {
    pub(crate) fn with_native_observation(
        mut self,
        mut observation: Option<crate::test_runtime::PlacementAuthorityCallObservation>,
        state: crate::DfsPlacementReconciliationState,
    ) -> Self {
        if let Some(observation) = observation.as_mut() {
            observation.acquired(&self.record.holder_id, state);
        }
        self.observation = observation;
        self
    }
    pub(crate) fn observe_native_release(&mut self, succeeded: bool) {
        if let Some(observation) = self.observation.as_mut() {
            observation.released(succeeded);
        }
    }
}

/// Compact scheduling state for placement jobs whose full, immutable planning
/// snapshots remain in their per-job KV records. Keeping only active ids here
/// lets maintenance ticks avoid loading an ever-growing terminal history while
/// preserving those records for delayed-intent authority checks and audit.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsPlacementReconciliationQueue {
    schema_version: u32,
    job_ids: Vec<String>,
}

impl Default for DfsPlacementReconciliationQueue {
    fn default() -> Self {
        Self {
            schema_version: PLACEMENT_RECONCILIATION_QUEUE_SCHEMA_VERSION,
            job_ids: Vec::new(),
        }
    }
}

/// Bounded newest-first activity identity index. Full execution snapshots stay
/// in their existing per-job records, while the separately keyed compact
/// records let the operator activity surface avoid decoding candidate and
/// accepted-config snapshots on every poll.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsPlacementReconciliationActivityIndex {
    schema_version: u32,
    recent: Vec<DfsPlacementReconciliationActivityCursor>,
    #[serde(default)]
    partial: bool,
    #[serde(default)]
    history_truncated: bool,
}

impl Default for DfsPlacementReconciliationActivityIndex {
    fn default() -> Self {
        Self {
            schema_version: PLACEMENT_RECONCILIATION_ACTIVITY_INDEX_SCHEMA_VERSION,
            recent: Vec::new(),
            // The control-plane store cannot prove that every writer in the
            // cluster has crossed this schema boundary. Keep completeness
            // monotonic and conservative: an old writer can create and finish
            // an unindexed job before upgrading, and that historical gap must
            // not disappear merely because current capabilities later look
            // homogeneous.
            partial: true,
            history_truncated: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsPlacementReconciliationActivityRecord {
    schema_version: u32,
    activity: DfsPlacementReconciliationActivitySummary,
}

impl From<&DfsPlacementReconciliationJob> for DfsPlacementReconciliationActivityRecord {
    fn from(job: &DfsPlacementReconciliationJob) -> Self {
        Self {
            schema_version: PLACEMENT_RECONCILIATION_ACTIVITY_RECORD_SCHEMA_VERSION,
            activity: DfsPlacementReconciliationActivitySummary::from(job),
        }
    }
}

pub fn file_system_config_key(file_system_id: &DfsFileSystemId) -> String {
    format!("{DFS_KV_PREFIX}/filesystems/{file_system_id}/config")
}

pub fn metadata_shards_prefix(file_system_id: &DfsFileSystemId) -> String {
    format!("{DFS_KV_PREFIX}/filesystems/{file_system_id}/metadata-shards")
}

pub fn placement_reconciliations_prefix(file_system_id: &DfsFileSystemId) -> String {
    format!("{DFS_KV_PREFIX}/filesystems/{file_system_id}/placement-reconciliations-v1/")
}

pub fn placement_reconciliation_key(file_system_id: &DfsFileSystemId, job_id: &str) -> String {
    format!(
        "{}{}",
        placement_reconciliations_prefix(file_system_id),
        normalize_segment(job_id)
    )
}

pub(crate) fn placement_reconciliation_queue_key(file_system_id: &DfsFileSystemId) -> String {
    format!("{DFS_KV_PREFIX}/filesystems/{file_system_id}/placement-reconciliation-queue-v1")
}

fn adaptive_placement_observation_key(file_system_id: &DfsFileSystemId) -> String {
    format!("{DFS_KV_PREFIX}/filesystems/{file_system_id}/adaptive-placement-observation-v1")
}

/// Bounded cluster-control projection for recent placement-job activity.
///
/// A filesystem's first config transaction creates the empty index alongside
/// the config. Its presence therefore distinguishes a filesystem with no
/// post-schema activity from an older filesystem whose unindexed history may
/// be incomplete; per-path and per-version metadata never enters this key.
pub fn placement_reconciliation_activity_index_key(file_system_id: &DfsFileSystemId) -> String {
    format!(
        "{DFS_KV_PREFIX}/filesystems/{file_system_id}/placement-reconciliation-activity-index-v1"
    )
}

fn placement_reconciliation_activity_record_key(
    file_system_id: &DfsFileSystemId,
    job_id: &str,
) -> String {
    format!(
        "{DFS_KV_PREFIX}/filesystems/{file_system_id}/placement-reconciliation-activity-v1/{}",
        normalize_segment(job_id)
    )
}

/// Exact non-expiring placement-authority permit key for one filesystem.
///
/// Daemon-side committed-snapshot reactors use this key to recover a dead
/// process's exact permit when it becomes observable, without polling the
/// complete DFS control plane.
pub fn placement_authority_permit_key(file_system_id: &DfsFileSystemId) -> String {
    format!("{DFS_KV_PREFIX}/filesystems/{file_system_id}/placement-authority-permit-v1")
}

fn authoritative_placement_permit_entry(
    key: &str,
    observed_revision: KvRevision,
    mut entries: Vec<Option<KvEntry>>,
) -> Result<Option<(KvEntry, DfsPlacementAuthorityPermitRecord)>, KvError> {
    if entries.len() != 1 {
        return Err(KvError::Serialization {
            detail: "placement permit authority did not return exactly one requested key".into(),
        });
    }
    let Some(entry) = entries.pop().flatten() else {
        tracing::trace!(target: "sinomo.dfs.perf", key,
            observed_revision = observed_revision.0, disposition = "absent",
            "placement-permit-authoritative-recheck");
        return Ok(None);
    };
    if entry.key != key {
        return Err(KvError::Serialization {
            detail: "placement permit authority returned another key".into(),
        });
    }
    let record: DfsPlacementAuthorityPermitRecord = entry.decode()?;
    tracing::trace!(target: "sinomo.dfs.perf", key,
        observed_revision = observed_revision.0, current_revision = entry.modify_revision.0,
        holder_node = %record.holder_node_id, job_id = %record.job_id, disposition = "present",
        "placement-permit-authoritative-recheck");
    Ok(Some((entry, record)))
}

pub(crate) fn placement_authority_permit_value_requires_local_process_recovery(
    value: &[u8],
    local_node_id: &NodeId,
    process: &DfsPlacementAuthorityProcessIdentity,
) -> Result<bool, KvError> {
    let record: DfsPlacementAuthorityPermitRecord = sinomo_core::kv::decode_kv_value(value)?;
    Ok(record.holder_cluster_id == process.cluster_id
        && &record.holder_node_id == local_node_id
        && record.holder_session_id != process.process_session_id)
}

fn placement_authority_permit_recovery_token(entry: &KvEntry) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo-dfs-placement-authority-permit-v1\0");
    hasher.update(&entry.modify_revision.0.to_be_bytes());
    hasher.update(&entry.value);
    format!(
        "v1:{}:{}",
        entry.modify_revision.0,
        hasher.finalize().to_hex()
    )
}

fn placement_authority_permit_status_from_entry(
    file_system_id: &DfsFileSystemId,
    entry: &KvEntry,
) -> Result<DfsPlacementAuthorityPermitStatus, KvError> {
    let record = entry.decode::<DfsPlacementAuthorityPermitRecord>()?;
    Ok(DfsPlacementAuthorityPermitStatus {
        file_system_id: file_system_id.clone(),
        job_id: record.job_id,
        holder_cluster_id: record.holder_cluster_id,
        holder_node_id: record.holder_node_id,
        recovery_token: placement_authority_permit_recovery_token(entry),
    })
}

fn placement_authority_busy_path(
    file_system_id: &DfsFileSystemId,
    record: &DfsPlacementAuthorityPermitRecord,
) -> String {
    let holder = format!(
        "held by node {} in cluster {}, daemon session {}",
        record.holder_node_id, record.holder_cluster_id, record.holder_session_id
    );
    format!(
        "{}; active placement metadata mutation permit {holder} cannot be auto-expired; restart the owning daemon for guarded local-session recovery; a permanently removed node requires operator intervention after member revocation",
        placement_authority_permit_key(file_system_id),
    )
}

/// Merge a generic filesystem publication without granting it policy-edit
/// authority. Once a config exists, its complete managed-rule set remains
/// authoritative; generic callers may update base fields but cannot create,
/// replace, or resurrect placement rules. Adding known generated defaults is
/// reserved for [`merge_file_system_config_defaults`], while explicit user
/// edits belong exclusively to the placement Apply transaction.
fn merge_file_system_config_publication(
    current: Option<&DfsFileSystemConfig>,
    proposed: &DfsFileSystemConfig,
) -> DfsFileSystemConfig {
    let Some(current) = current else {
        return proposed.clone();
    };
    let mut merged = proposed.clone();
    merged.subtree_policies = current.subtree_policies.clone();
    merged.retention_policies = current.retention_policies.clone();
    merged.module_metadata = current.module_metadata.clone();
    merged.namespace_count_activation = current.namespace_count_activation.clone();
    merged
}

fn validate_ordinary_namespace_count_activation(
    expected: Option<&DfsFileSystemConfig>, desired: &DfsFileSystemConfig,
) -> Result<(), KvError> {
    if expected.and_then(|config| config.namespace_count_activation.as_ref())
        != desired.namespace_count_activation.as_ref()
    {
        return Err(KvError::Serialization {
            detail: "namespace-count activation is authored only by the native complete-cohort maintenance transaction".into(),
        });
    }
    if let Some(activation) = &desired.namespace_count_activation {
        activation.validate().map_err(|detail| KvError::Serialization { detail })?;
    }
    Ok(())
}

fn merge_file_system_config_defaults(
    current: Option<&DfsFileSystemConfig>,
    defaults: &DfsFileSystemConfig,
) -> DfsFileSystemConfig {
    let Some(current) = current else {
        return defaults.clone();
    };
    let mut merged = current.clone();
    for rule in &defaults.subtree_policies {
        if !merged.subtree_policies.iter().any(|existing| {
            existing.path_prefix == rule.path_prefix && existing.scope == rule.scope
        }) {
            merged.subtree_policies.push(rule.clone());
        }
    }
    merged
}

fn validate_v1_placement_reconciliation_job(
    job: &DfsPlacementReconciliationJob,
) -> Result<(), KvError> {
    if job.schema_version != DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION {
        return Err(KvError::Serialization {
            detail: format!(
                "placement reconciliation job {} uses schema version {}, expected {}",
                job.job_id, job.schema_version, DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION
            ),
        });
    }
    let accepted_config = &job.accepted_config;
    for rule in &accepted_config.subtree_policies {
        crate::placement::placement_policy_from_rule(rule).map_err(|error| {
            KvError::Serialization {
                detail: format!(
                    "placement reconciliation job {} has invalid policy at {}: {error}",
                    job.job_id, rule.path_prefix
                ),
            }
        })?;
    }
    if accepted_config.file_system_id != job.file_system_id {
        return Err(KvError::Serialization {
            detail: format!(
                "placement reconciliation job {} snapshot belongs to filesystem {} instead of {}",
                job.job_id, accepted_config.file_system_id, job.file_system_id
            ),
        });
    }
    if job.candidate_snapshot.is_empty() {
        return Err(KvError::Serialization {
            detail: format!(
                "placement reconciliation job {} is missing its accepted candidate snapshot",
                job.job_id
            ),
        });
    }
    if job.adaptive {
        if job.placement_snapshot.is_none() {
            return Err(KvError::Serialization {
                detail: format!(
                    "adaptive placement reconciliation job {} is missing its placement snapshot",
                    job.job_id
                ),
            });
        }
        let Some(budget) = job.adaptive_budget.as_ref() else {
            return Err(KvError::Serialization {
                detail: format!(
                    "adaptive placement reconciliation job {} is missing its execution budget",
                    job.job_id
                ),
            });
        };
        if budget.max_objects_per_tick == 0
            || budget.max_bytes_per_tick == 0
            || budget.max_millis_per_tick == 0
        {
            return Err(KvError::Serialization {
                detail: format!(
                    "adaptive placement reconciliation job {} has a zero execution budget",
                    job.job_id
                ),
            });
        }
    }
    Ok(())
}

/// Build the same immutable reconciliation input consumed by normal placement
/// publication. This function owns no KV, queue, samples, admission or authority.
#[allow(clippy::too_many_arguments)]
pub fn build_placement_reconciliation_job(
    job_id: String,
    config: &DfsFileSystemConfig,
    path: String,
    scope: crate::DfsPlacementScope,
    unavailable_node_ids: BTreeSet<NodeId>,
    candidate_snapshot: Vec<crate::DfsNodeAdvertisement>,
    placement_snapshot: Option<crate::DfsPlacementSnapshot>,
    created_at_unix_nanos: u64,
) -> Result<DfsPlacementReconciliationJob, KvError> {
    let mut job = DfsPlacementReconciliationJob {
        schema_version: DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
        job_id,
        file_system_id: config.file_system_id.clone(),
        path,
        scope,
        unavailable_node_ids,
        candidate_snapshot,
        placement_snapshot,
        adaptive: false,
        adaptive_budget: None,
        processed_content_bytes: 0,
        accepted_config: config.clone(),
        state: crate::DfsPlacementReconciliationState::Pending,
        cursor: None,
        total_object_count: 0,
        scanned_object_count: 0,
        matched_object_count: 0,
        updated_object_count: 0,
        scheduled_repair_count: 0,
        completed_repair_count: 0,
        pending_repair_count: 0,
        revision: 0,
        created_at_unix_nanos,
        updated_at_unix_nanos: created_at_unix_nanos,
        last_error: None,
    };
    job.accepted_config = scoped_placement_config_for_job(config, &job);
    validate_v1_placement_reconciliation_job(&job)?;
    Ok(job)
}

/// The normal adaptive producer and source conversion share this same job
/// factory. All observations/configuration are supplied current DATA; budgets
/// remain exact per-tick constraints and spent bytes remain cumulative DATA.
#[allow(clippy::too_many_arguments)]
pub fn build_adaptive_placement_reconciliation_job(
    job_id: String,
    config: &DfsFileSystemConfig,
    path: String,
    scope: crate::DfsPlacementScope,
    unavailable_node_ids: BTreeSet<NodeId>,
    candidate_snapshot: Vec<crate::DfsNodeAdvertisement>,
    placement_snapshot: crate::DfsPlacementSnapshot,
    budget: crate::DfsAdaptivePlacementBudget,
    processed_content_bytes: u64,
    created_at_unix_nanos: u64,
) -> Result<DfsPlacementReconciliationJob, KvError> {
    let mut job = build_placement_reconciliation_job(
        job_id,
        config,
        path,
        scope,
        unavailable_node_ids,
        candidate_snapshot,
        Some(placement_snapshot),
        created_at_unix_nanos,
    )?;
    job.adaptive = true;
    job.adaptive_budget = Some(budget);
    job.processed_content_bytes = processed_content_bytes;
    validate_v1_placement_reconciliation_job(&job)?;
    Ok(job)
}

fn validate_placement_reconciliation_queue(
    file_system_id: &DfsFileSystemId,
    queue: &DfsPlacementReconciliationQueue,
) -> Result<(), KvError> {
    if queue.schema_version != PLACEMENT_RECONCILIATION_QUEUE_SCHEMA_VERSION {
        return Err(KvError::Serialization {
            detail: format!(
                "placement reconciliation queue for {file_system_id} uses schema version {}, expected {}",
                queue.schema_version, PLACEMENT_RECONCILIATION_QUEUE_SCHEMA_VERSION
            ),
        });
    }
    if queue.job_ids.len() > MAX_ACTIVE_PLACEMENT_RECONCILIATIONS {
        return Err(KvError::RangeLimit {
            prefix: placement_reconciliation_queue_key(file_system_id),
            max_entries: MAX_ACTIVE_PLACEMENT_RECONCILIATIONS,
        });
    }
    let unique_job_ids = queue.job_ids.iter().collect::<BTreeSet<_>>();
    if unique_job_ids.len() != queue.job_ids.len()
        || queue.job_ids.iter().any(|job_id| job_id.is_empty())
    {
        return Err(KvError::Serialization {
            detail: format!(
                "placement reconciliation queue for {file_system_id} contains empty or duplicate job ids"
            ),
        });
    }
    Ok(())
}

/// Classify the executor's compact active queue from the same sorted authority
/// projection used for root-primary ownership. Full job history remains durable
/// for recovery and audit; it is read only for the existing pre-queue fallback.
pub(crate) fn placement_reconciliation_pending_work_from_authority_entries(
    file_system_id: &DfsFileSystemId,
    authority_entries: &[KvEntry],
    pending_repair_count: usize,
) -> Result<usize, KvError> {
    let queue_key = placement_reconciliation_queue_key(file_system_id);
    let queue_entry = authority_entries
        .binary_search_by(|entry| entry.key.as_str().cmp(&queue_key))
        .ok()
        .map(|index| &authority_entries[index]);
    let Some(queue_entry) = queue_entry else {
        // Older state has not yet installed the executor's active queue. Keep
        // its existing history-based wake until the executor initializes it.
        let prefix = placement_reconciliations_prefix(file_system_id);
        let jobs = authority_entries
            .iter()
            .filter(|entry| entry.key.starts_with(&prefix))
            .map(|entry| entry.decode::<DfsPlacementReconciliationJob>())
            .collect::<Result<Vec<_>, _>>()?;
        return placement_reconciliation_projected_eligibility_count(
            file_system_id,
            &jobs,
            authority_entries,
            pending_repair_count,
        );
    };
    let queue = placement_reconciliation_queue_from_entry(file_system_id, queue_entry)?;
    let mut jobs = Vec::with_capacity(queue.job_ids.len());
    let mut queue_repair_pending = false;
    for (position, job_id) in queue.job_ids.iter().enumerate() {
        let key = placement_reconciliation_key(file_system_id, job_id);
        let entry = authority_entries
            .binary_search_by(|entry| entry.key.as_str().cmp(&key))
            .ok()
            .map(|index| &authority_entries[index]);
        let Some(entry) = entry else {
            // The executor can repair only the leading stale prefix. A stale
            // tail behind a waiting repair does not own another runnable tick.
            queue_repair_pending |= position == 0;
            continue;
        };
        let job = entry.decode::<DfsPlacementReconciliationJob>()?;
        validate_v1_placement_reconciliation_job(&job)?;
        if job.state.is_terminal() {
            queue_repair_pending |= position == 0;
        }
        jobs.push(job);
    }
    Ok(placement_reconciliation_projected_eligibility_count(
        file_system_id,
        &jobs,
        authority_entries,
        pending_repair_count,
    )? + usize::from(queue_repair_pending))
}

fn placement_reconciliation_projected_eligibility_count(
    file_system_id: &DfsFileSystemId,
    jobs: &[DfsPlacementReconciliationJob],
    authority_entries: &[KvEntry],
    pending_repair_count: usize,
) -> Result<usize, KvError> {
    let needs_config = jobs.iter().any(|job| {
        job.state == crate::DfsPlacementReconciliationState::Repairing
            && !crate::maintenance::dfs_placement_reconciliation_job_can_advance(
                job,
                pending_repair_count,
                None,
            )
    });
    let current_config = if needs_config {
        let key = file_system_config_key(file_system_id);
        let config = authority_entries
            .binary_search_by(|entry| entry.key.as_str().cmp(&key))
            .ok()
            .map(|index| authority_entries[index].decode::<DfsFileSystemConfig>())
            .transpose()?;
        if config
            .as_ref()
            .is_some_and(|config| config.file_system_id != *file_system_id)
        {
            return Err(KvError::Serialization {
                detail: format!(
                    "projected placement configuration does not belong to filesystem {file_system_id}"
                ),
            });
        }
        config
    } else {
        None
    };
    Ok(
        crate::maintenance::dfs_actionable_placement_reconciliation_count_with_config(
            jobs,
            pending_repair_count,
            current_config.as_ref(),
        ),
    )
}

fn placement_reconciliation_queue_from_entry(
    file_system_id: &DfsFileSystemId,
    entry: &KvEntry,
) -> Result<DfsPlacementReconciliationQueue, KvError> {
    let queue = entry.decode::<DfsPlacementReconciliationQueue>()?;
    validate_placement_reconciliation_queue(file_system_id, &queue)?;
    Ok(queue)
}

fn sort_placement_reconciliation_activity_cursors(
    cursors: &mut [DfsPlacementReconciliationActivityCursor],
) {
    cursors.sort_by(|left, right| {
        right
            .created_at_unix_nanos
            .cmp(&left.created_at_unix_nanos)
            .then_with(|| right.job_id.cmp(&left.job_id))
    });
}

fn validate_placement_reconciliation_activity_index(
    file_system_id: &DfsFileSystemId,
    index: &DfsPlacementReconciliationActivityIndex,
) -> Result<(), KvError> {
    if index.schema_version != PLACEMENT_RECONCILIATION_ACTIVITY_INDEX_SCHEMA_VERSION {
        return Err(KvError::Serialization {
            detail: format!(
                "placement reconciliation activity index for {file_system_id} uses schema version {}, expected {}",
                index.schema_version, PLACEMENT_RECONCILIATION_ACTIVITY_INDEX_SCHEMA_VERSION
            ),
        });
    }
    if index.recent.len() > MAX_RECENT_PLACEMENT_RECONCILIATION_ACTIVITIES {
        return Err(KvError::RangeLimit {
            prefix: placement_reconciliation_activity_index_key(file_system_id),
            max_entries: MAX_RECENT_PLACEMENT_RECONCILIATION_ACTIVITIES,
        });
    }
    if index
        .recent
        .iter()
        .any(|cursor| cursor.job_id.trim().is_empty())
        || index
            .recent
            .iter()
            .map(|cursor| cursor.job_id.as_str())
            .collect::<BTreeSet<_>>()
            .len()
            != index.recent.len()
    {
        return Err(KvError::Serialization {
            detail: format!(
                "placement reconciliation activity index for {file_system_id} contains empty or duplicate job ids"
            ),
        });
    }
    let mut sorted = index.recent.clone();
    sort_placement_reconciliation_activity_cursors(&mut sorted);
    if sorted != index.recent {
        return Err(KvError::Serialization {
            detail: format!(
                "placement reconciliation activity index for {file_system_id} is not newest-first"
            ),
        });
    }
    Ok(())
}

fn validate_placement_reconciliation_activity_record(
    file_system_id: &DfsFileSystemId,
    job_id: &str,
    record: &DfsPlacementReconciliationActivityRecord,
) -> Result<(), KvError> {
    if record.schema_version != PLACEMENT_RECONCILIATION_ACTIVITY_RECORD_SCHEMA_VERSION {
        return Err(KvError::Serialization {
            detail: format!(
                "placement reconciliation activity record {job_id} uses schema version {}, expected {}",
                record.schema_version, PLACEMENT_RECONCILIATION_ACTIVITY_RECORD_SCHEMA_VERSION
            ),
        });
    }
    if record.activity.file_system_id != *file_system_id || record.activity.job_id != job_id {
        return Err(KvError::Serialization {
            detail: format!(
                "placement reconciliation activity record {job_id} does not match its filesystem or job key"
            ),
        });
    }
    Ok(())
}

fn include_placement_reconciliation_activity(
    index: &mut DfsPlacementReconciliationActivityIndex,
    job: &DfsPlacementReconciliationJob,
) -> Result<(bool, Vec<String>), KvError> {
    let before = index.clone();
    let cursor = DfsPlacementReconciliationActivityCursor {
        created_at_unix_nanos: job.created_at_unix_nanos,
        job_id: job.job_id.clone(),
    };
    if let Some(existing) = index
        .recent
        .iter()
        .find(|existing| existing.job_id == cursor.job_id)
    {
        if existing != &cursor {
            return Err(KvError::Serialization {
                detail: format!(
                    "placement reconciliation activity job {} changed its immutable creation cursor",
                    job.job_id
                ),
            });
        }
        return Ok((false, Vec::new()));
    }
    index.recent.push(cursor);
    sort_placement_reconciliation_activity_cursors(&mut index.recent);
    let mut evicted_job_ids = Vec::new();
    if index.recent.len() > MAX_RECENT_PLACEMENT_RECONCILIATION_ACTIVITIES {
        evicted_job_ids.extend(
            index.recent[MAX_RECENT_PLACEMENT_RECONCILIATION_ACTIVITIES..]
                .iter()
                .map(|cursor| cursor.job_id.clone()),
        );
        index
            .recent
            .truncate(MAX_RECENT_PLACEMENT_RECONCILIATION_ACTIVITIES);
        index.history_truncated = true;
    }
    validate_placement_reconciliation_activity_index(&job.file_system_id, index)?;
    Ok((index != &before, evicted_job_ids))
}

fn validate_placement_reconciliation_snapshot_identity(
    expected: &DfsPlacementReconciliationJob,
    desired: &DfsPlacementReconciliationJob,
) -> Result<(), KvError> {
    if expected.schema_version != desired.schema_version
        || expected.job_id != desired.job_id
        || expected.file_system_id != desired.file_system_id
        || expected.path != desired.path
        || expected.scope != desired.scope
        || expected.unavailable_node_ids != desired.unavailable_node_ids
        || expected.candidate_snapshot != desired.candidate_snapshot
        || expected.accepted_config != desired.accepted_config
        || expected.created_at_unix_nanos != desired.created_at_unix_nanos
    {
        return Err(KvError::Serialization {
            detail: format!(
                "placement reconciliation job {} cannot change its accepted policy or topology snapshots",
                expected.job_id
            ),
        });
    }
    Ok(())
}

pub fn metadata_shard_key(file_system_id: &DfsFileSystemId, shard_id: &str) -> String {
    format!(
        "{}/{}",
        metadata_shards_prefix(file_system_id),
        normalize_segment(shard_id)
    )
}

/// Compact Raft-backed write barrier used to prove that a metadata-shard
/// assignment was current at a particular point in the cluster KV log.
///
/// This lives outside `metadata-shards/` so shard-prefix scans continue to
/// contain only [`DfsMetadataShardConfig`] values.
pub fn metadata_shard_authority_barrier_key(
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
) -> String {
    format!(
        "{DFS_KV_PREFIX}/filesystems/{file_system_id}/metadata-authority-barriers/{}",
        normalize_segment(shard_id)
    )
}

/// Bounded quorum-committed floor that primary transfer must preserve.
///
/// This is deliberately separate from [`metadata_shard_authority_barrier_key`]:
/// ordinary authority checks overwrite that generic barrier, while a committed
/// floor stays in force for every later primary transfer of the shard.
pub fn metadata_shard_operation_floor_key(
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
) -> String {
    format!(
        "{DFS_KV_PREFIX}/filesystems/{file_system_id}/metadata-operation-floors/{}",
        normalize_segment(shard_id)
    )
}

/// One bounded, quorum-committed fence for a membership or primary-role handoff.
///
/// The active shard configuration remains at [`metadata_shard_key`] until the
/// retiring assignment has transferred an exact DFS metadata state to every
/// member of both assignments. Keeping this outside `metadata-shards/` ensures
/// ordinary placement scans continue to decode only shard configurations.
pub fn metadata_shard_membership_handoff_key(
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
) -> String {
    format!(
        "{DFS_KV_PREFIX}/filesystems/{file_system_id}/metadata-membership-handoffs/{}",
        normalize_segment(shard_id)
    )
}

/// Last completed membership-handoff proof for one shard.
///
/// This is bounded audit/failover evidence. A later handoff overwrites it; DFS
/// file and object metadata never enters cluster KV.
pub fn metadata_shard_membership_handoff_proof_key(
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
) -> String {
    format!(
        "{DFS_KV_PREFIX}/filesystems/{file_system_id}/metadata-membership-handoff-proofs/{}",
        normalize_segment(shard_id)
    )
}

/// Quorum-committed durable head for one metadata-shard authority epoch.
pub fn metadata_shard_head_watermark_key(
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
) -> String {
    format!(
        "{DFS_KV_PREFIX}/filesystems/{file_system_id}/metadata-head-watermarks/{}",
        normalize_segment(shard_id)
    )
}

pub use sinomo_api::dfs::control_plane::DfsFileSystemConfig;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataShardConfig {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: String,
    pub primary_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replica_node_ids: Vec<NodeId>,
    pub epoch: u64,
    pub assignment: DfsMetadataShardAssignment,
    pub default_policy: DfsObjectPolicy,
}

/// Highest DFS metadata prefix known to be durable on a configured recovery
/// quorum while the exact shard assignment and authority epoch were current.
///
/// This is a compact consensus fence, not storage for the journal or metadata
/// tables identified by the sequence and fingerprint.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardHeadWatermark {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: String,
    pub primary_node_id: NodeId,
    pub epoch: u64,
    pub journal_sequence: u64,
    #[serde(default)]
    pub state_fingerprint: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acknowledged_node_ids: Vec<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsMetadataShardOperationFloor {
    operation_digest: String,
    prepared_sequence: u64,
    authority_epoch: u64,
    permit_id: String,
}

/// The current handoff protocol also certifies storage representation changes.
/// The DFS runtime compares opaque source identity; legacy decoding stays in
/// the migration component. None of these values grants author permission.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsMetadataShardHandoffCause {
    Membership,
    StorageImport { cut: DfsMetadataStorageImportCut },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataStorageImportCut {
    pub source_cut_digest: String,
    pub source_journal_sequence: u64,
    pub source_state_fingerprint: String,
    pub target_prefix: crate::DfsMetadataPrefixProof,
    pub converted_image_fingerprint: String,
    pub pending_mapping_digest: String,
}

/// Quorum-committed fence separating a retiring metadata assignment from a
/// desired assignment or primary role.
///
/// While this record exists, the retiring configuration remains stored at the
/// normal shard key but may not authorize a new canonical publication. A data
/// plane coordinator must catch up a joint quorum of the retiring and desired
/// members and then provide [`DfsMetadataShardMembershipHandoffProof`].
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardMembershipHandoff {
    pub cause: DfsMetadataShardHandoffCause,
    pub handoff_id: String,
    pub retiring_shard: DfsMetadataShardConfig,
    pub desired_shard: DfsMetadataShardConfig,
    /// Highest partial-shard publication or sync prefix committed through the
    /// cluster Raft log when this handoff began.
    pub required_prepared_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_head_watermark: Option<DfsMetadataShardHeadWatermark>,
    /// Exact retiring-primary state captured only after that primary has
    /// observed this durable pending handoff under its publication guard.
    /// A quorum-side `begin` may precede this acknowledgement while the old
    /// primary is partitioned, so activation must never infer a fence merely
    /// from the existence of the handoff record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retiring_primary_fence: Option<DfsMetadataShardMembershipHandoffFence>,
}

/// Retiring-primary acknowledgement that closes the cross-process
/// validate-to-publish race for a metadata handoff.
///
/// This is bounded authority evidence, not DFS file metadata. The retiring
/// primary captures it under the same process-wide publication guard held by
/// canonical commits, after refreshing and verifying the durable handoff.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardMembershipHandoffFence {
    pub handoff_id: String,
    pub primary_node_id: NodeId,
    pub journal_sequence: u64,
    pub state_fingerprint: String,
}

/// Exact DFS-state evidence consumed to activate a fenced reconfiguration.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardMembershipHandoffProof {
    pub cause: DfsMetadataShardHandoffCause,
    pub handoff_id: String,
    pub retiring_shard: DfsMetadataShardConfig,
    pub desired_shard: DfsMetadataShardConfig,
    pub journal_sequence: u64,
    pub state_fingerprint: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acknowledged_node_ids: Vec<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsMetadataShardAssignment {
    Root,
    PathPrefix {
        prefix: String,
    },
    NameHashRange {
        start_inclusive: u64,
        end_exclusive: u64,
    },
}

/// One bounded prefix projection paired with the revision and completeness
/// certificate supplied by its authority path.
///
/// `complete` is deliberately explicit: callers which fence destructive work
/// must reject a truncated projection even when every returned entry is valid.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsAuthoritativePrefixProjection {
    entries: Vec<KvEntry>,
    revision: KvRevision,
    complete: bool,
}

impl DfsAuthoritativePrefixProjection {
    pub fn complete(
        prefix: &str,
        entries: Vec<KvEntry>,
        revision: KvRevision,
    ) -> Result<Self, KvError> {
        if entries.len() > sinomo_core::kv::CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_ENTRIES {
            return Err(KvError::RangeLimit {
                prefix: prefix.to_string(),
                max_entries: sinomo_core::kv::CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_ENTRIES,
            });
        }
        let mut payload_bytes = 0_usize;
        let mut previous_key: Option<&str> = None;
        for entry in &entries {
            payload_bytes = payload_bytes
                .saturating_add(entry.key.len())
                .saturating_add(entry.value.len());
            if payload_bytes > sinomo_core::kv::CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_PAYLOAD_BYTES {
                return Err(KvError::RangePayloadLimit {
                    prefix: prefix.to_string(),
                    observed_bytes: payload_bytes,
                    max_bytes: sinomo_core::kv::CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
                });
            }
            if !entry.key.starts_with(prefix) {
                return Err(KvError::Serialization {
                    detail: format!(
                        "authoritative prefix {prefix:?} returned out-of-scope key {:?}",
                        entry.key,
                    ),
                });
            }
            if previous_key.is_some_and(|previous| previous >= entry.key.as_str()) {
                return Err(KvError::Serialization {
                    detail: format!(
                        "authoritative prefix {prefix:?} did not return strictly ordered keys"
                    ),
                });
            }
            if entry.create_revision > entry.modify_revision || entry.modify_revision > revision {
                return Err(KvError::Serialization {
                    detail: format!(
                        "authoritative prefix entry {:?} has revisions {}..{} outside proof revision {}",
                        entry.key, entry.create_revision.0, entry.modify_revision.0, revision.0,
                    ),
                });
            }
            previous_key = Some(entry.key.as_str());
        }
        Ok(Self {
            entries,
            revision,
            complete: true,
        })
    }

    pub fn entries(&self) -> &[KvEntry] {
        &self.entries
    }

    pub fn revision(&self) -> KvRevision {
        self.revision
    }

    pub fn is_complete(&self) -> bool {
        self.complete
    }

    pub fn into_parts(self) -> (Vec<KvEntry>, KvRevision, bool) {
        (self.entries, self.revision, self.complete)
    }
}

#[derive(Clone, Debug)]
pub enum DfsWriteConfigurationReadiness {
    Ready(Option<std::sync::Arc<crate::DfsWriteConfigurationProjection>>),
    Pending,
}

pub trait DfsControlPlaneKvClient: std::fmt::Debug + Send {
    /// Borrow this same client's native asynchronous transport, when supported.
    /// Maintenance uses it while retaining its existing admission/generation
    /// owner so a pending RPC can yield to foreground admission.
    fn async_client(&mut self) -> Option<&mut dyn AsyncDfsControlPlaneKvClient> {
        None
    }

    /// The host's published configuration snapshot, after its native floor
    /// check, for one operation. Callers use this same value for shard
    /// routing, filesystem policy, members and providers. `None` means the
    /// client has no published configuration and keeps its ordinary reads.
    /// This method must never initiate a KV read or hydrate a resident backend.
    /// sinomo:kv-configuration: Borrow the host-owned published configuration snapshot after its native authority floor check.
    fn configuration_snapshot(
        &self,
    ) -> Result<Option<std::sync::Arc<crate::DfsConfigurationSnapshot>>, KvError> {
        Ok(None)
    }

    /// One already-published, coherent configuration for ordinary write policy
    /// and provider selection. Hosts which do not own this projection retain
    /// the existing configuration-read behavior through the compatibility seam.
    /// This method must never initiate a KV read or hydrate a resident backend.
    /// sinomo:kv-configuration: Consume the host-owned configuration projection with its native authority floor.
    fn write_configuration_projection(
        &self,
    ) -> Result<Option<std::sync::Arc<crate::DfsWriteConfigurationProjection>>, KvError> {
        Ok(self
            .configuration_snapshot()?
            .and_then(|snapshot| snapshot.write_configuration().cloned()))
    }

    /// Typed rejection before any parent or write intent is prepared. This
    /// retained-only check never starts a read or classifies diagnostic text.
    /// Borrow the host's published configuration or report pending recovery.
    /// This method must not initiate a KV read or hydrate a backend.
    /// sinomo:kv-configuration: Inspect existing configuration readiness and its native authority floor without fetching KV state.
    fn write_configuration_readiness(&self) -> Result<DfsWriteConfigurationReadiness, KvError> {
        self.write_configuration_projection()
            .map(DfsWriteConfigurationReadiness::Ready)
    }

    fn refresh_latest(&mut self) -> Result<(), KvError> {
        Ok(())
    }

    /// Identify a host process which is authorized to execute placement
    /// reconciliation. Generic clients return `None`; the daemon supplies a
    /// cluster-bound process session and the in-process test store supplies an
    /// isolated embedded session.
    fn placement_authority_process_identity(&self) -> Option<DfsPlacementAuthorityProcessIdentity> {
        None
    }

    /// Borrow only a snapshot already published by the normal host environment.
    /// Source restoration waits without sampling, opening DFS or creating a
    /// cache/registry when this retained observation is unavailable.
    #[cfg(feature = "storage-conversion")]
    fn storage_import_placement_snapshot(&self) -> Option<Arc<crate::DfsPlacementSnapshot>> {
        None
    }

    /// Confirm that this client is backed by the host's current exclusive
    /// ownership proof before recovering a permit from an earlier process.
    ///
    /// The identity fields alone are deliberately insufficient: they are wire
    /// data and can be constructed by any caller. Production hosts override
    /// this method only while they retain their OS-backed ownership lock.
    fn authorizes_prior_process_placement_authority_recovery(
        &self,
        _local_node_id: &NodeId,
        _current_process: &DfsPlacementAuthorityProcessIdentity,
    ) -> bool {
        false
    }

    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError>;

    /// Read a small set of exact keys from one coherent client view.
    ///
    /// The default is sufficient for clients whose `&self` reads are backed by
    /// one immutable or exclusively-owned snapshot. Clients that issue a fresh
    /// authority RPC for each point read must override this method and pin all
    /// returned entries to one authoritative revision.
    fn get_consistent(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>, KvError> {
        keys.iter().map(|key| self.get(key)).collect()
    }

    /// Establish one authoritative view for an exact, bounded key set.
    ///
    /// The default preserves compatibility with snapshot-backed clients by
    /// refreshing once and then projecting every key from that one view.
    /// Clients with a native exact-key-set ReadIndex route override this method
    /// so an operation fence never imports the complete KV snapshot first.
    fn get_authoritative_consistent(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        self.refresh_latest()?;
        self.get_consistent(keys)
    }

    /// Establish one authoritative exact-key-set view before the caller's
    /// absolute operation deadline.
    ///
    /// Snapshot-backed compatibility clients complete locally and may rely on
    /// this preflight. Clients which can block on transport must override this
    /// method and enforce `deadline` throughout the underlying request.
    fn get_authoritative_consistent_before(
        &mut self,
        keys: &[String],
        deadline: Instant,
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        if Instant::now() >= deadline {
            return Err(metadata_shard_authority_deadline_error());
        }
        self.get_authoritative_consistent(keys)
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError>;

    /// Optional decoded shard projection from the same current configuration
    /// certificate used by `range_prefix`. This is routing evidence, not a
    /// publication fence. `None` retains the ordinary configuration-read path.
    fn cached_metadata_shards(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<Vec<DfsMetadataShardConfig>>, KvError> {
        match self.configuration_snapshot()? {
            Some(snapshot) => snapshot.metadata_shards(file_system_id),
            None => Ok(None),
        }
    }

    /// Establish one authoritative, complete view of a bounded prefix.
    ///
    /// Unlike [`Self::range_prefix`], this method is an authority capability:
    /// callers may use a successful result to fence destructive work. Generic
    /// observational clients deliberately fail closed instead of promoting a
    /// local/materialized projection to cluster authority. Daemon and mounted
    /// clients override this with their bounded ReadIndex prefix route.
    fn range_prefix_authoritative(
        &mut self,
        prefix: &str,
    ) -> Result<DfsAuthoritativePrefixProjection, KvError> {
        Err(KvError::AuthorityUnavailable {
            detail: format!(
                "control-plane client cannot establish authoritative prefix {prefix:?}"
            ),
        })
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError>;
    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError>;

    /// The default preserves ordinary transaction behavior but supplies no
    /// native authority certificate. It cannot authorize a V7 recovery commit.
    fn txn_with_native_authority(
        &mut self,
        txn: KvTxn,
    ) -> Result<(KvTxnResponse, Option<DfsNativeTxnCommit>), KvError> {
        Ok((self.txn(txn)?, None))
    }

    /// Commit one transaction under the caller's stable request identity.
    /// Resubmitting the identical request returns its committed response or
    /// commits it once, which settles an outcome the caller did not learn.
    /// Clients without a native stable-identity commit path fail closed.
    fn txn_request(&mut self, request: KvTxnRequest) -> Result<KvTxnResponse, KvError> {
        Err(KvError::AuthorityUnavailable {
            detail: format!(
                "control-plane client cannot commit stable request {:?}",
                request.request_id
            ),
        })
    }

    /// The authoritative retained outcome of one stable request identity.
    /// This never executes the request. Clients without a native outcome
    /// reader fail closed.
    fn request_outcome(&mut self, request_id: &KvRequestId) -> Result<KvRequestOutcome, KvError> {
        Err(KvError::AuthorityUnavailable {
            detail: format!("control-plane client cannot read the outcome of {request_id:?}"),
        })
    }

    /// Compare a control-plane value through the client's linearizable write
    /// path. The fresh barrier write is intentional: a compare-only or
    /// same-value transaction can be answered by the cluster's semantic
    /// no-effect fast path without committing a new Raft entry.
    fn linearizable_compare_value(
        &mut self,
        key: String,
        expected_value: Vec<u8>,
        barrier_key: String,
    ) -> Result<bool, KvError> {
        let barrier_value =
            sinomo_core::kv::encode_kv_value(&sinomo_core::Id::generate().to_string())?;
        let response = self.txn(KvTxn {
            compares: vec![KvCompare::ValueEquals {
                key,
                value: expected_value,
            }],
            success: vec![KvOp::Put {
                key: barrier_key,
                value: barrier_value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })?;
        Ok(response.succeeded)
    }
}

pub type DfsControlPlaneKvFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, KvError>> + Send + 'a>>;

pub trait AsyncDfsControlPlaneKvClient: std::fmt::Debug + Send {
    fn refresh_latest_async<'a>(&'a mut self) -> DfsControlPlaneKvFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn get_async<'a>(&'a self, key: &'a str) -> DfsControlPlaneKvFuture<'a, Option<KvEntry>>;

    /// Async counterpart of [`DfsControlPlaneKvClient::get_consistent`].
    fn get_consistent_async<'a>(
        &'a self,
        keys: Vec<String>,
    ) -> DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>>;

    /// Async counterpart of
    /// [`DfsControlPlaneKvClient::get_authoritative_consistent`].
    fn get_authoritative_consistent_async<'a>(
        &'a mut self,
        keys: Vec<String>,
    ) -> DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
        Box::pin(async move {
            self.refresh_latest_async().await?;
            self.get_consistent_async(keys).await
        })
    }

    /// Async counterpart of
    /// [`DfsControlPlaneKvClient::get_authoritative_consistent_before`].
    fn get_authoritative_consistent_before_async<'a>(
        &'a mut self,
        keys: Vec<String>,
        deadline: Instant,
    ) -> DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
        if Instant::now() >= deadline {
            return Box::pin(async { Err(metadata_shard_authority_deadline_error()) });
        }
        self.get_authoritative_consistent_async(keys)
    }

    fn range_prefix_async<'a>(
        &'a self,
        prefix: &'a str,
    ) -> DfsControlPlaneKvFuture<'a, Vec<KvEntry>>;

    fn put_async<'a>(
        &'a mut self,
        key: String,
        value: Vec<u8>,
    ) -> DfsControlPlaneKvFuture<'a, KvRevision>;

    fn txn_async<'a>(&'a mut self, txn: KvTxn) -> DfsControlPlaneKvFuture<'a, KvTxnResponse>;

    /// Async counterpart of [`DfsControlPlaneKvClient::txn_request`].
    fn txn_request_async<'a>(
        &'a mut self,
        request: KvTxnRequest,
    ) -> DfsControlPlaneKvFuture<'a, KvTxnResponse> {
        Box::pin(async move {
            Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "control-plane client cannot commit stable request {:?}",
                    request.request_id
                ),
            })
        })
    }

    /// Async counterpart of [`DfsControlPlaneKvClient::request_outcome`].
    fn request_outcome_async<'a>(
        &'a mut self,
        request_id: KvRequestId,
    ) -> DfsControlPlaneKvFuture<'a, KvRequestOutcome> {
        Box::pin(async move {
            Err(KvError::AuthorityUnavailable {
                detail: format!("control-plane client cannot read the outcome of {request_id:?}"),
            })
        })
    }

    fn linearizable_compare_value_async<'a>(
        &'a mut self,
        key: String,
        expected_value: Vec<u8>,
        barrier_key: String,
    ) -> DfsControlPlaneKvFuture<'a, bool> {
        Box::pin(async move {
            let barrier_value =
                sinomo_core::kv::encode_kv_value(&sinomo_core::Id::generate().to_string())?;
            let response = self
                .txn_async(KvTxn {
                    compares: vec![KvCompare::ValueEquals {
                        key,
                        value: expected_value,
                    }],
                    success: vec![KvOp::Put {
                        key: barrier_key,
                        value: barrier_value,
                        lease_id: None,
                    }],
                    failure: Vec::new(),
                })
                .await?;
            Ok(response.succeeded)
        })
    }
}

#[derive(Debug)]
pub struct SharedDfsControlPlaneKvClient<C> {
    inner: Arc<Mutex<C>>,
}

impl<C> Clone for SharedDfsControlPlaneKvClient<C> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<C> SharedDfsControlPlaneKvClient<C> {
    pub fn new(client: C) -> Self {
        Self {
            inner: Arc::new(Mutex::new(client)),
        }
    }

    pub fn with_client<R>(&self, f: impl FnOnce(&mut C) -> R) -> R {
        let mut client = self.inner.lock().expect("control-plane KV client lock");
        f(&mut client)
    }
}

impl<C> DfsControlPlaneKvClient for SharedDfsControlPlaneKvClient<C>
where
    C: DfsControlPlaneKvClient + Send,
{
    fn refresh_latest(&mut self) -> Result<(), KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".to_string(),
            })?
            .refresh_latest()
    }

    fn placement_authority_process_identity(&self) -> Option<DfsPlacementAuthorityProcessIdentity> {
        self.inner
            .lock()
            .ok()
            .and_then(|client| client.placement_authority_process_identity())
    }

    fn authorizes_prior_process_placement_authority_recovery(
        &self,
        local_node_id: &NodeId,
        current_process: &DfsPlacementAuthorityProcessIdentity,
    ) -> bool {
        self.inner.lock().is_ok_and(|client| {
            client.authorizes_prior_process_placement_authority_recovery(
                local_node_id,
                current_process,
            )
        })
    }

    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".to_string(),
            })?
            .get(key)
    }

    fn get_consistent(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>, KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".to_string(),
            })?
            .get_consistent(keys)
    }

    fn get_authoritative_consistent(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".to_string(),
            })?
            .get_authoritative_consistent(keys)
    }

    fn get_authoritative_consistent_before(
        &mut self,
        keys: &[String],
        deadline: Instant,
    ) -> Result<Vec<Option<KvEntry>>, KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".to_string(),
            })?
            .get_authoritative_consistent_before(keys, deadline)
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".to_string(),
            })?
            .range_prefix(prefix)
    }

    fn cached_metadata_shards(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<Vec<DfsMetadataShardConfig>>, KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".to_string(),
            })?
            .cached_metadata_shards(file_system_id)
    }

    fn range_prefix_authoritative(
        &mut self,
        prefix: &str,
    ) -> Result<DfsAuthoritativePrefixProjection, KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".to_string(),
            })?
            .range_prefix_authoritative(prefix)
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".to_string(),
            })?
            .put(key, value)
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".to_string(),
            })?
            .txn(txn)
    }
    fn txn_with_native_authority(
        &mut self,
        txn: KvTxn,
    ) -> Result<(KvTxnResponse, Option<DfsNativeTxnCommit>), KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".into(),
            })?
            .txn_with_native_authority(txn)
    }

    fn txn_request(&mut self, request: KvTxnRequest) -> Result<KvTxnResponse, KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".into(),
            })?
            .txn_request(request)
    }

    fn request_outcome(&mut self, request_id: &KvRequestId) -> Result<KvRequestOutcome, KvError> {
        self.inner
            .lock()
            .map_err(|_| KvError::Serialization {
                detail: "control-plane KV client lock poisoned".into(),
            })?
            .request_outcome(request_id)
    }
}

impl DfsControlPlaneKvClient for InProcessKvStore {
    fn async_client(&mut self) -> Option<&mut dyn AsyncDfsControlPlaneKvClient> {
        Some(self)
    }

    fn placement_authority_process_identity(&self) -> Option<DfsPlacementAuthorityProcessIdentity> {
        Some(DfsPlacementAuthorityProcessIdentity {
            cluster_id: "in-process".to_string(),
            process_session_id: IN_PROCESS_PLACEMENT_AUTHORITY_SESSION_ID
                .get_or_init(|| sinomo_core::Id::generate().to_string())
                .clone(),
        })
    }

    fn authorizes_prior_process_placement_authority_recovery(
        &self,
        _local_node_id: &NodeId,
        _current_process: &DfsPlacementAuthorityProcessIdentity,
    ) -> bool {
        // An in-process store has no external daemon lifetime. It is used by
        // embedded callers and tests which own the complete store directly.
        true
    }

    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        Ok(self.get(key).cloned())
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.range_prefix(prefix)
    }

    fn range_prefix_authoritative(
        &mut self,
        prefix: &str,
    ) -> Result<DfsAuthoritativePrefixProjection, KvError> {
        DfsAuthoritativePrefixProjection::complete(
            prefix,
            self.range_prefix(prefix)?,
            self.revision(),
        )
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        InProcessKvStore::put(self, key, value, None)
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        InProcessKvStore::txn(self, txn)
    }
}

impl AsyncDfsControlPlaneKvClient for InProcessKvStore {
    fn get_async<'a>(&'a self, key: &'a str) -> DfsControlPlaneKvFuture<'a, Option<KvEntry>> {
        Box::pin(async move { DfsControlPlaneKvClient::get(self, key) })
    }

    fn get_consistent_async<'a>(
        &'a self,
        keys: Vec<String>,
    ) -> DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
        let result = DfsControlPlaneKvClient::get_consistent(self, &keys);
        Box::pin(async move { result })
    }

    fn range_prefix_async<'a>(
        &'a self,
        prefix: &'a str,
    ) -> DfsControlPlaneKvFuture<'a, Vec<KvEntry>> {
        Box::pin(async move { DfsControlPlaneKvClient::range_prefix(self, prefix) })
    }

    fn put_async<'a>(
        &'a mut self,
        key: String,
        value: Vec<u8>,
    ) -> DfsControlPlaneKvFuture<'a, KvRevision> {
        Box::pin(async move { InProcessKvStore::put(self, key, value, None) })
    }

    fn txn_async<'a>(&'a mut self, txn: KvTxn) -> DfsControlPlaneKvFuture<'a, KvTxnResponse> {
        Box::pin(async move { InProcessKvStore::txn(self, txn) })
    }
}

// Both transports publish exactly the same validated progress transaction.
// Native I/O belongs to their wrappers; this builder owns no KV/transport state.
fn placement_reconciliation_progress_transaction(
    desired: &DfsPlacementReconciliationJob,
    current_entry: KvEntry,
    queue_entry: Option<KvEntry>,
    activity_index_entry: Option<KvEntry>,
    mut compares: Vec<KvCompare>,
) -> Result<KvTxn, KvError> {
    let key = placement_reconciliation_key(&desired.file_system_id, &desired.job_id);
    let queue_key = placement_reconciliation_queue_key(&desired.file_system_id);
    let mut queue = queue_entry
        .as_ref()
        .map(KvEntry::decode::<DfsPlacementReconciliationQueue>)
        .transpose()?;
    if let Some(queue) = queue.as_ref() {
        validate_placement_reconciliation_queue(&desired.file_system_id, queue)?;
    }
    let queue_len_before = queue.as_ref().map_or(0, |queue| queue.job_ids.len());
    if desired.state.is_terminal()
        && let Some(queue) = queue.as_mut()
    {
        queue.job_ids.retain(|job_id| job_id != &desired.job_id);
    }
    let queue_changed = queue
        .as_ref()
        .is_some_and(|queue| queue.job_ids.len() != queue_len_before);
    compares.push(KvCompare::ValueEquals {
        key: key.clone(),
        value: current_entry.value,
    });
    let activity_record_key =
        placement_reconciliation_activity_record_key(&desired.file_system_id, &desired.job_id);
    let activity_record = DfsPlacementReconciliationActivityRecord::from(desired);
    let mut success = vec![
        KvOp::put_serde(key, desired, None)?,
        KvOp::put_serde(activity_record_key, &activity_record, None)?,
    ];
    let mut activity_index = placement_activity_index_from_entry(
        &desired.file_system_id,
        activity_index_entry.as_ref(),
    )?;
    let (activity_index_update, evicted_activity_job_ids) =
        placement_activity_index_update(desired, activity_index_entry, &mut activity_index)?;
    if let Some((compare, operation)) = activity_index_update {
        compares.push(compare);
        success.push(operation);
    }
    let evicted_terminal_activity_job_ids = queue
        .as_ref()
        .map(|queue| {
            evicted_activity_job_ids
                .into_iter()
                .filter(|job_id| !queue.job_ids.contains(job_id))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if (queue_changed || !evicted_terminal_activity_job_ids.is_empty())
        && let Some(queue_entry) = queue_entry
    {
        compares.push(KvCompare::ValueEquals {
            key: queue_key.clone(),
            value: queue_entry.value,
        });
    }
    if queue_changed && let Some(queue) = queue.as_ref() {
        success.push(KvOp::put_serde(queue_key, queue, None)?);
    }
    for evicted_job_id in evicted_terminal_activity_job_ids {
        success.push(KvOp::Delete {
            key: placement_reconciliation_activity_record_key(
                &desired.file_system_id,
                &evicted_job_id,
            ),
        });
    }
    Ok(KvTxn {
        compares,
        success,
        failure: Vec::new(),
    })
}

fn validate_placement_reconciliation_progress(
    expected: &DfsPlacementReconciliationJob,
    desired: &DfsPlacementReconciliationJob,
) -> Result<(), KvError> {
    validate_v1_placement_reconciliation_job(expected)?;
    validate_v1_placement_reconciliation_job(desired)?;
    validate_placement_reconciliation_snapshot_identity(expected, desired)
}

// A placement edit keeps the reviewed config and job identity fixed across
// definite CAS rejection. Only the queue/activity publication image is rebuilt.
struct PlacementUpdatePublication<'a> {
    expected: &'a DfsFileSystemConfig,
    desired: &'a DfsFileSystemConfig,
    job: DfsPlacementReconciliationJob,
    keys: Vec<String>,
}

struct PlacementUpdateInputs {
    config: KvEntry,
    queue: Option<KvEntry>,
    activity: Option<KvEntry>,
    permit: Option<KvEntry>,
}

impl<'a> PlacementUpdatePublication<'a> {
    fn new(
        expected: &'a DfsFileSystemConfig,
        desired: &'a DfsFileSystemConfig,
        job: &DfsPlacementReconciliationJob,
    ) -> Result<Self, KvError> {
        validate_ordinary_namespace_count_activation(Some(expected), desired)?;
        let mut durable_job = job.clone();
        durable_job.accepted_config = scoped_placement_config_for_job(desired, job);
        validate_v1_placement_reconciliation_job(&durable_job)?;
        let keys = vec![
            file_system_config_key(&expected.file_system_id),
            placement_reconciliation_key(&job.file_system_id, &job.job_id),
            placement_reconciliation_activity_record_key(&job.file_system_id, &job.job_id),
            placement_reconciliation_queue_key(&job.file_system_id),
            placement_reconciliation_activity_index_key(&job.file_system_id),
            placement_authority_permit_key(&job.file_system_id),
        ];
        Ok(Self {
            expected,
            desired,
            job: durable_job,
            keys,
        })
    }

    fn inputs(
        &self,
        entries: Vec<Option<KvEntry>>,
    ) -> Result<Option<PlacementUpdateInputs>, KvError> {
        let [config, job, activity_record, queue, activity, permit]: [Option<KvEntry>; 6] =
            entries.try_into().map_err(|_| KvError::Serialization {
                detail: "placement update exact-key projection has the wrong length".into(),
            })?;
        let Some(config) = config else {
            return Ok(None);
        };
        if config.decode::<DfsFileSystemConfig>()? != *self.expected
            || job.is_some()
            || activity_record.is_some()
        {
            return Ok(None);
        }
        if let Some(entry) = permit.as_ref() {
            let record = entry.decode::<DfsPlacementAuthorityPermitRecord>()?;
            if !placement_authority_release_is_pending(&self.job.file_system_id, &record) {
                return Err(KvError::Busy {
                    path: placement_authority_busy_path(&self.job.file_system_id, &record),
                });
            }
        }
        Ok(Some(PlacementUpdateInputs {
            config,
            queue,
            activity,
            permit,
        }))
    }

    fn queued_job_keys(
        &self,
        inputs: &PlacementUpdateInputs,
    ) -> Result<Option<Vec<String>>, KvError> {
        let Some(entry) = inputs.queue.as_ref() else {
            return Ok(None);
        };
        let queue = entry.decode::<DfsPlacementReconciliationQueue>()?;
        validate_placement_reconciliation_queue(&self.job.file_system_id, &queue)?;
        Ok(Some(
            queue
                .job_ids
                .iter()
                .map(|id| placement_reconciliation_key(&self.job.file_system_id, id))
                .collect(),
        ))
    }

    fn transaction(
        &self,
        inputs: PlacementUpdateInputs,
        job_entries: Vec<KvEntry>,
        permit_compare: KvCompare,
    ) -> Result<KvTxn, KvError> {
        let mut jobs = job_entries
            .into_iter()
            .map(|entry| entry.decode::<DfsPlacementReconciliationJob>())
            .collect::<Result<Vec<_>, _>>()?;
        for job in &jobs {
            validate_v1_placement_reconciliation_job(job)?;
        }
        let queue = match inputs.queue.as_ref() {
            Some(entry) => entry.decode::<DfsPlacementReconciliationQueue>()?,
            None => DfsPlacementReconciliationQueue {
                schema_version: PLACEMENT_RECONCILIATION_QUEUE_SCHEMA_VERSION,
                job_ids: Vec::new(),
            },
        };
        let jobs = jobs
            .into_iter()
            .map(|job| (job.job_id.clone(), job))
            .collect::<BTreeMap<_, _>>();
        let queue = compact_placement_queue_from_jobs(
            &self.job.file_system_id,
            queue,
            &jobs,
            self.desired,
            Some(&self.job.job_id),
        )?;
        let mut index = placement_activity_index_from_entry(
            &self.job.file_system_id,
            inputs.activity.as_ref(),
        )?;
        let (activity_index_update, evicted_activity_job_ids) =
            placement_activity_index_update(&self.job, inputs.activity, &mut index)?;
        let compare_entry = |key: String, entry: Option<KvEntry>| match entry {
            Some(entry) => KvCompare::ValueEquals {
                key,
                value: entry.value,
            },
            None => KvCompare::Missing { key },
        };
        let mut compares = vec![
            compare_entry(self.keys[0].clone(), Some(inputs.config)),
            KvCompare::Missing {
                key: self.keys[1].clone(),
            },
            KvCompare::Missing {
                key: self.keys[2].clone(),
            },
            compare_entry(self.keys[3].clone(), inputs.queue),
            permit_compare,
        ];
        let mut success = vec![
            KvOp::put_serde(self.keys[0].clone(), self.desired, None)?,
            KvOp::put_serde(self.keys[1].clone(), &self.job, None)?,
            KvOp::put_serde(
                self.keys[2].clone(),
                &DfsPlacementReconciliationActivityRecord::from(&self.job),
                None,
            )?,
            KvOp::put_serde(self.keys[3].clone(), &queue, None)?,
        ];
        if let Some((compare, operation)) = activity_index_update {
            compares.push(compare);
            success.push(operation);
        }
        for evicted_id in evicted_activity_job_ids {
            if !queue.job_ids.contains(&evicted_id) {
                success.push(KvOp::Delete {
                    key: placement_reconciliation_activity_record_key(
                        &self.job.file_system_id,
                        &evicted_id,
                    ),
                });
            }
        }
        Ok(KvTxn {
            compares,
            success,
            failure: Vec::new(),
        })
    }
}

/// Pure current publication planning for the owned storage converter. Captured
/// entries are DATA; only the caller's real native commit can establish effect
/// authority. No reader, recovery owner or queue engine is created here.
#[cfg(feature = "storage-conversion")]
pub struct DfsPlacementUpdatePlan<'a> {
    publication: PlacementUpdatePublication<'a>,
}
#[cfg(feature = "storage-conversion")]
impl<'a> DfsPlacementUpdatePlan<'a> {
    pub fn new(
        expected: &'a DfsFileSystemConfig,
        desired: &'a DfsFileSystemConfig,
        job: &DfsPlacementReconciliationJob,
    ) -> Result<Self, KvError> {
        if expected.file_system_id != desired.file_system_id
            || job.file_system_id != expected.file_system_id
        {
            return Err(KvError::Serialization {
                detail: "placement input plan changes its filesystem identity".into(),
            });
        }
        Ok(Self {
            publication: PlacementUpdatePublication::new(expected, desired, job)?,
        })
    }
    /// Config, new job, new activity, queue, activity index and permit, in order.
    pub fn keys(&self) -> &[String] {
        &self.publication.keys
    }
    fn checked_inputs(
        &self,
        entries: Vec<Option<KvEntry>>,
    ) -> Result<Option<PlacementUpdateInputs>, KvError> {
        if entries.len() != self.keys().len()
            || entries
                .iter()
                .zip(self.keys())
                .any(|(entry, key)| entry.as_ref().is_some_and(|entry| &entry.key != key))
        {
            return Err(KvError::Serialization {
                detail: "placement input plan has incomplete or differently scoped native entries"
                    .into(),
            });
        }
        if entries.last().is_some_and(Option::is_some) {
            return Err(KvError::Busy {
                path: self.keys()[5].clone(),
            });
        }
        self.publication.inputs(entries)
    }
    pub fn queued_job_keys(
        &self,
        entries: Vec<Option<KvEntry>>,
    ) -> Result<Option<Vec<String>>, KvError> {
        let Some(inputs) = self.checked_inputs(entries)? else {
            return Ok(None);
        };
        Ok(Some(
            self.publication
                .queued_job_keys(&inputs)?
                .unwrap_or_default(),
        ))
    }
    /// Build the existing normal transaction from complete ordered read images.
    /// Missing queued rows are explicit; present rows cannot be omitted or renamed.
    pub fn transaction(
        &self,
        entries: Vec<Option<KvEntry>>,
        queued_entries: Vec<Option<KvEntry>>,
    ) -> Result<Option<KvTxn>, KvError> {
        let Some(inputs) = self.checked_inputs(entries)? else {
            return Ok(None);
        };
        let keys = self
            .publication
            .queued_job_keys(&inputs)?
            .unwrap_or_default();
        if queued_entries.len() != keys.len() {
            return Err(KvError::Serialization {
                detail: "placement input plan omitted a queued native identity".into(),
            });
        }
        let mut jobs = Vec::with_capacity(queued_entries.len());
        for (key, entry) in keys.iter().zip(queued_entries) {
            if let Some(entry) = entry {
                let job = entry.decode::<DfsPlacementReconciliationJob>()?;
                validate_v1_placement_reconciliation_job(&job)?;
                if entry.key != *key
                    || job.file_system_id != self.publication.job.file_system_id
                    || placement_reconciliation_key(&job.file_system_id, &job.job_id) != *key
                {
                    return Err(KvError::Serialization {
                        detail: "placement input plan changed a queued job's exact identity".into(),
                    });
                }
                jobs.push(entry);
            }
        }
        self.publication
            .transaction(
                inputs,
                jobs,
                KvCompare::Missing {
                    key: self.keys()[5].clone(),
                },
            )
            .map(Some)
    }
}

fn compact_placement_queue_from_jobs(
    file_system_id: &DfsFileSystemId,
    queue: DfsPlacementReconciliationQueue,
    jobs: &BTreeMap<String, DfsPlacementReconciliationJob>,
    desired_config: &DfsFileSystemConfig,
    new_job_id: Option<&str>,
) -> Result<DfsPlacementReconciliationQueue, KvError> {
    let mut authoritative = Vec::new();
    let mut superseded = Vec::new();
    for job_id in queue.job_ids {
        let Some(job) = jobs.get(&job_id) else {
            continue;
        };
        validate_v1_placement_reconciliation_job(job)?;
        if job.state.is_terminal() {
            continue;
        }
        let target = if placement_config_remains_authoritative_for_job(
            &job.accepted_config,
            desired_config,
            job,
        ) {
            &mut authoritative
        } else {
            &mut superseded
        };
        target.push(job_id);
    }
    if let Some(job_id) = new_job_id {
        if authoritative.len().saturating_add(superseded.len())
            >= MAX_ACTIVE_PLACEMENT_RECONCILIATIONS
        {
            return Err(KvError::RangeLimit {
                prefix: placement_reconciliation_queue_key(file_system_id),
                max_entries: MAX_ACTIVE_PLACEMENT_RECONCILIATIONS,
            });
        }
        authoritative.push(job_id.to_string());
    }
    authoritative.extend(superseded);
    let queue = DfsPlacementReconciliationQueue {
        schema_version: PLACEMENT_RECONCILIATION_QUEUE_SCHEMA_VERSION,
        job_ids: authoritative,
    };
    validate_placement_reconciliation_queue(file_system_id, &queue)?;
    Ok(queue)
}

fn placement_activity_index_from_entry(
    file_system_id: &DfsFileSystemId,
    entry: Option<&KvEntry>,
) -> Result<DfsPlacementReconciliationActivityIndex, KvError> {
    let index = match entry {
        Some(entry) => entry.decode::<DfsPlacementReconciliationActivityIndex>()?,
        None => DfsPlacementReconciliationActivityIndex {
            partial: true,
            history_truncated: true,
            ..DfsPlacementReconciliationActivityIndex::default()
        },
    };
    validate_placement_reconciliation_activity_index(file_system_id, &index)?;
    Ok(index)
}

fn placement_activity_index_update(
    job: &DfsPlacementReconciliationJob,
    entry: Option<KvEntry>,
    index: &mut DfsPlacementReconciliationActivityIndex,
) -> Result<(Option<(KvCompare, KvOp)>, Vec<String>), KvError> {
    let key = placement_reconciliation_activity_index_key(&job.file_system_id);
    let (changed, evicted_job_ids) = include_placement_reconciliation_activity(index, job)?;
    if !changed {
        return Ok((None, evicted_job_ids));
    }
    let compare = match entry {
        Some(entry) => KvCompare::ValueEquals {
            key: key.clone(),
            value: entry.value,
        },
        None => KvCompare::Missing { key: key.clone() },
    };
    Ok((
        Some((compare, KvOp::put_serde(key, index, None)?)),
        evicted_job_ids,
    ))
}

pub struct DfsControlPlaneStore<'a> {
    pub(crate) kv: &'a mut dyn DfsControlPlaneKvClient,
}

impl<'a> DfsControlPlaneStore<'a> {
    pub fn new(kv: &'a mut dyn DfsControlPlaneKvClient) -> Self {
        Self { kv }
    }

    pub fn put_file_system_config(&mut self, config: &DfsFileSystemConfig) -> Result<(), KvError> {
        for _ in 0..FILE_SYSTEM_CONFIG_CAS_RETRIES {
            let current = self.get_file_system_config(&config.file_system_id)?;
            let merged = merge_file_system_config_publication(current.as_ref(), config);
            if current.as_ref() == Some(&merged) {
                return Ok(());
            }
            if self.compare_and_put_file_system_config(current.as_ref(), &merged)? {
                return Ok(());
            }
        }
        Err(KvError::Busy {
            path: format!(
                "{} changed concurrently during generic filesystem publication; retry",
                file_system_config_key(&config.file_system_id)
            ),
        })
    }

    /// Idempotently seed filesystem defaults without changing any existing
    /// owner/display metadata or managed placement rule. This is the recurring
    /// bootstrap publication path; missing generated rules are appended with
    /// an exact config CAS so it cannot regress a concurrent owner or policy
    /// update.
    pub fn ensure_file_system_config_defaults(
        &mut self,
        defaults: &DfsFileSystemConfig,
    ) -> Result<(), KvError> {
        for _ in 0..FILE_SYSTEM_CONFIG_CAS_RETRIES {
            let current = self.get_file_system_config(&defaults.file_system_id)?;
            let merged = merge_file_system_config_defaults(current.as_ref(), defaults);
            if current.as_ref() == Some(&merged) {
                return Ok(());
            }
            if self.compare_and_put_file_system_config(current.as_ref(), &merged)? {
                return Ok(());
            }
        }
        Err(KvError::Busy {
            path: format!(
                "{} changed concurrently while ensuring filesystem defaults; retry",
                file_system_config_key(&defaults.file_system_id)
            ),
        })
    }

    /// sinomo:kv-configuration: Read filesystem policy through the existing retained configuration projection.
    pub fn get_file_system_config(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsFileSystemConfig>, KvError> {
        self.get_serde(&file_system_config_key(file_system_id))
    }

    /// Compare-and-set a filesystem config while participating in the same
    /// placement-authority latch as Apply. Callers which merge a freshly read
    /// config (such as recurring default-cluster publication) must use this
    /// form so a concurrent user Apply cannot be overwritten by their stale
    /// snapshot.
    /// sinomo:kv-consistency: Serialize explicit filesystem policy changes with placement authority and the expected configuration value.
    pub fn compare_and_put_file_system_config(
        &mut self,
        expected: Option<&DfsFileSystemConfig>,
        desired: &DfsFileSystemConfig,
    ) -> Result<bool, KvError> {
        if expected.is_some_and(|config| config.file_system_id != desired.file_system_id) {
            return Err(KvError::Serialization {
                detail: "filesystem config compare-and-put cannot change the filesystem id"
                    .to_string(),
            });
        }
        validate_ordinary_namespace_count_activation(expected, desired)?;
        let key = file_system_config_key(&desired.file_system_id);
        let current_entry = self.kv.get(&key)?;
        let creating = expected.is_none() && current_entry.is_none();
        let config_compare = match (expected, current_entry) {
            (None, None) => KvCompare::Missing { key: key.clone() },
            (Some(expected), Some(entry)) => {
                if entry.decode::<DfsFileSystemConfig>()? != *expected {
                    return Ok(false);
                }
                KvCompare::ValueEquals {
                    key: key.clone(),
                    value: entry.value,
                }
            }
            _ => return Ok(false),
        };
        let permit_compare =
            self.placement_authority_permit_missing_compare(&desired.file_system_id)?;
        let mut compares = vec![config_compare, permit_compare];
        crate::validate_retention_policies(&desired.retention_policies)
            .map_err(|detail| KvError::Serialization { detail })?;
        crate::validate_module_metadata_map(&desired.module_metadata)
            .map_err(|detail| KvError::Serialization { detail })?;
        if !desired.retention_policies.is_empty() || !desired.module_metadata.is_empty() {
            let entries = self.kv.range_prefix(&format!(
                "{}/",
                metadata_shards_prefix(&desired.file_system_id)
            ))?;
            if entries.is_empty() {
                return Err(KvError::Serialization {
                    detail: "retention metadata requires configured current metadata shards".into(),
                });
            }
            for entry in entries {
                let shard: DfsMetadataShardConfig = entry.decode()?;
                if shard.file_system_id != desired.file_system_id {
                    return Err(KvError::Serialization { detail: "namespaced retention metadata requires current support on every assigned metadata shard".into() });
                }
                compares.push(KvCompare::ValueEquals {
                    key: entry.key,
                    value: entry.value,
                });
                compares.extend(metadata_prepared_operation_admission_compares(
                    self.kv, &shard,
                )?);
            }
        }
        let mut success = vec![KvOp::put_serde(key, desired, None)?];
        if creating {
            let activity_key = placement_reconciliation_activity_index_key(&desired.file_system_id);
            if self.kv.get(&activity_key)?.is_none() {
                compares.push(KvCompare::Missing {
                    key: activity_key.clone(),
                });
                success.push(KvOp::put_serde(
                    activity_key,
                    &DfsPlacementReconciliationActivityIndex::default(),
                    None,
                )?);
            }
        }
        Ok(self
            .kv
            .txn(KvTxn {
                compares,
                success,
                failure: Vec::new(),
            })?
            .succeeded)
    }

    /// sinomo:kv-consistency: Verify a suspected live placement holder with an exact-key authority read; a cached released holder cannot fence current configuration forever.
    fn placement_authority_permit_missing_compare(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<KvCompare, KvError> {
        let key = placement_authority_permit_key(file_system_id);
        let deadline = metadata_shard_authority_read_deadline(None);
        for _ in 0..PLACEMENT_AUTHORITY_RECOVERY_CAS_RETRIES {
            let Some(mut entry) = self.kv.get(&key)? else {
                return Ok(KvCompare::Missing { key });
            };
            let mut record = entry.decode::<DfsPlacementAuthorityPermitRecord>()?;
            if !placement_authority_release_is_pending(file_system_id, &record) {
                // A normal client's immutable projection may retain a holder
                // whose exact release already committed elsewhere. Recheck
                // only this key, within one existing authority-read budget.
                // Missing still becomes a Missing compare in the real CAS;
                // no timeout, hint, or read result releases a live holder.
                let entries = self
                    .kv
                    .get_authoritative_consistent_before(&[key.clone()], deadline)?;
                let Some(current) =
                    authoritative_placement_permit_entry(&key, entry.modify_revision, entries)?
                else {
                    return Ok(KvCompare::Missing { key });
                };
                (entry, record) = current;
                if !placement_authority_release_is_pending(file_system_id, &record) {
                    return Err(KvError::Busy {
                        path: placement_authority_busy_path(file_system_id, &record),
                    });
                }
            }
            let response = self.kv.txn(KvTxn {
                compares: vec![KvCompare::ValueEquals {
                    key: key.clone(),
                    value: entry.value,
                }],
                success: vec![KvOp::Delete { key: key.clone() }],
                failure: Vec::new(),
            })?;
            if response.succeeded {
                clear_pending_placement_authority_release(file_system_id, &record);
                return Ok(KvCompare::Missing { key });
            }
        }
        Err(KvError::Busy {
            path: format!(
                "{} changed repeatedly during exact pending-release recovery; retry",
                placement_authority_permit_key(file_system_id)
            ),
        })
    }

    /// Storage conversion uses the same exact pending-release and actual-host
    /// prior-process recovery as ordinary placement. DATA cannot grant custody.
    /// sinomo:kv-consistency: Recover only the exact normal placement latch before a source-import retirement CAS.
    #[cfg(feature = "storage-conversion")]
    pub fn prepare_storage_import_placement_retirement(
        &mut self,
        file_system_id: &DfsFileSystemId,
        local_node_id: &NodeId,
    ) -> Result<KvCompare, KvError> {
        // First reconcile the normal exact in-process release handle.
        match self.placement_authority_permit_missing_compare(file_system_id) {
            Ok(compare) => return Ok(compare),
            Err(KvError::Busy { .. }) => {}
            Err(failed) => return Err(failed),
        }
        self.kv.refresh_latest()?;
        if let Some(process) = self.kv.placement_authority_process_identity() {
            self.recover_prior_process_placement_authority_permit_for_local_node(
                file_system_id,
                local_node_id,
                &process,
            )?;
        }
        // A current/foreign/unidentified live holder remains typed Busy.
        self.placement_authority_permit_missing_compare(file_system_id)
    }

    /// Inspect the current placement mutation permit without exposing its
    /// random holder id or process-session id. The returned recovery token is
    /// bound to the exact encoded value and KV generation.
    pub fn placement_authority_permit_status(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsPlacementAuthorityPermitStatus>, KvError> {
        let Some(entry) = self
            .kv
            .get(&placement_authority_permit_key(file_system_id))?
        else {
            return Ok(None);
        };
        placement_authority_permit_status_from_entry(file_system_id, &entry).map(Some)
    }

    /// Clear a permit owned by a fully identified, removed node in this exact
    /// cluster. Absence from membership is checked in the same quorum
    /// transaction as the exact permit delete; an explicitly `Removed` record
    /// is accepted only under an exact-value membership compare. Offline,
    /// suspect, pending, active, local, and foreign holders fail
    /// closed.
    ///
    /// Membership removal does not fence an already-running metadata writer,
    /// so the operator must additionally acknowledge that the old owner has
    /// been stopped or externally fenced.
    /// sinomo:kv-consistency: Recover only an exact placement permit whose owning node was globally removed from membership.
    pub fn recover_removed_node_placement_authority_permit(
        &mut self,
        file_system_id: &DfsFileSystemId,
        current_cluster_id: &str,
        local_node_id: &NodeId,
        expected_recovery_token: &str,
        acknowledge_owner_stopped: bool,
    ) -> Result<DfsPlacementAuthorityRecoveryOutcome, KvError> {
        if !acknowledge_owner_stopped {
            return Ok(DfsPlacementAuthorityRecoveryOutcome::Refused {
                reason: DfsPlacementAuthorityRecoveryRefusal::OwnerStoppedAcknowledgementRequired,
            });
        }
        let permit_key = placement_authority_permit_key(file_system_id);
        let Some(permit_entry) = self.kv.get(&permit_key)? else {
            return Ok(DfsPlacementAuthorityRecoveryOutcome::Missing);
        };
        if placement_authority_permit_recovery_token(&permit_entry) != expected_recovery_token {
            return Ok(DfsPlacementAuthorityRecoveryOutcome::Changed);
        }
        let record = permit_entry.decode::<DfsPlacementAuthorityPermitRecord>()?;
        let holder_cluster_id = record.holder_cluster_id.as_str();
        let holder_node_id = &record.holder_node_id;
        if holder_cluster_id != current_cluster_id {
            return Ok(DfsPlacementAuthorityRecoveryOutcome::Refused {
                reason: DfsPlacementAuthorityRecoveryRefusal::ForeignCluster,
            });
        }
        if holder_node_id == local_node_id {
            return Ok(DfsPlacementAuthorityRecoveryOutcome::Refused {
                reason: DfsPlacementAuthorityRecoveryRefusal::CurrentLocalNode,
            });
        }

        let member_key = cluster_member_key(holder_node_id);
        let member_compare = match self.kv.get(&member_key)? {
            None => KvCompare::Missing {
                key: member_key.clone(),
            },
            Some(member_entry) => {
                let member = member_entry.decode::<ClusterMemberRecord>()?;
                if member.node_id != *holder_node_id
                    || member.cluster_id.to_string() != current_cluster_id
                {
                    return Ok(DfsPlacementAuthorityRecoveryOutcome::Refused {
                        reason:
                            DfsPlacementAuthorityRecoveryRefusal::InvalidRemovedMembershipEvidence,
                    });
                }
                if member.state != ClusterMemberState::Removed {
                    return Ok(DfsPlacementAuthorityRecoveryOutcome::Refused {
                        reason: DfsPlacementAuthorityRecoveryRefusal::HolderMembershipPresent {
                            state: member.state,
                        },
                    });
                }
                KvCompare::ValueEquals {
                    key: member_key.clone(),
                    value: member_entry.value,
                }
            }
        };

        let response = self.kv.txn(KvTxn {
            compares: vec![
                KvCompare::ValueEquals {
                    key: permit_key.clone(),
                    value: permit_entry.value,
                },
                KvCompare::ModifyRevisionEquals {
                    key: permit_key.clone(),
                    revision: permit_entry.modify_revision,
                },
                member_compare,
            ],
            success: vec![KvOp::Delete { key: permit_key }],
            failure: Vec::new(),
        })?;
        if !response.succeeded {
            return Ok(DfsPlacementAuthorityRecoveryOutcome::Changed);
        }
        clear_pending_placement_authority_release(file_system_id, &record);
        Ok(DfsPlacementAuthorityRecoveryOutcome::Cleared)
    }

    /// Deliberately exceptional recovery for a foreign-cluster
    /// permit whose owner cannot be proven through current membership.
    /// The exact acknowledgement phrase and exact-generation token are both
    /// required. Fully identified same-cluster holders must use the normal
    /// removed-node recovery path and can never cross this escape hatch.
    /// sinomo:kv-consistency: Recover an explicitly authorized abandoned placement permit with exact global compare-and-swap fencing.
    pub fn break_glass_recover_placement_authority_permit(
        &mut self,
        file_system_id: &DfsFileSystemId,
        current_cluster_id: &str,
        expected_recovery_token: &str,
        acknowledgement: &str,
    ) -> Result<DfsPlacementAuthorityRecoveryOutcome, KvError> {
        if acknowledgement != DFS_PLACEMENT_AUTHORITY_BREAK_GLASS_ACKNOWLEDGEMENT {
            return Ok(DfsPlacementAuthorityRecoveryOutcome::Refused {
                reason: DfsPlacementAuthorityRecoveryRefusal::BreakGlassAcknowledgementRequired,
            });
        }
        let permit_key = placement_authority_permit_key(file_system_id);
        let Some(permit_entry) = self.kv.get(&permit_key)? else {
            return Ok(DfsPlacementAuthorityRecoveryOutcome::Missing);
        };
        if placement_authority_permit_recovery_token(&permit_entry) != expected_recovery_token {
            return Ok(DfsPlacementAuthorityRecoveryOutcome::Changed);
        }
        let record = permit_entry.decode::<DfsPlacementAuthorityPermitRecord>()?;
        if record.holder_cluster_id == current_cluster_id {
            return Ok(DfsPlacementAuthorityRecoveryOutcome::Refused {
                reason: DfsPlacementAuthorityRecoveryRefusal::NormalRecoveryRequired,
            });
        }

        let response = self.kv.txn(KvTxn {
            compares: vec![
                KvCompare::ValueEquals {
                    key: permit_key.clone(),
                    value: permit_entry.value,
                },
                KvCompare::ModifyRevisionEquals {
                    key: permit_key.clone(),
                    revision: permit_entry.modify_revision,
                },
            ],
            success: vec![KvOp::Delete { key: permit_key }],
            failure: Vec::new(),
        })?;
        if !response.succeeded {
            return Ok(DfsPlacementAuthorityRecoveryOutcome::Changed);
        }
        clear_pending_placement_authority_release(file_system_id, &record);
        Ok(DfsPlacementAuthorityRecoveryOutcome::Cleared)
    }

    /// Recover a permit belonging to an earlier process on this exact node.
    ///
    /// The caller must hold the node's exclusive daemon and data-owner locks,
    /// and must refresh its control-plane view before invoking this method.
    /// Those locks prove the prior local process is gone. Unknown legacy
    /// current-process holders and records owned by another node are
    /// deliberately left intact. The delete itself compares the exact encoded
    /// record, so it cannot remove a concurrently replaced holder.
    pub fn recover_prior_process_placement_authority_permit_for_local_node(
        &mut self,
        file_system_id: &DfsFileSystemId,
        local_node_id: &NodeId,
        current_process: &DfsPlacementAuthorityProcessIdentity,
    ) -> Result<bool, KvError> {
        if !self
            .kv
            .authorizes_prior_process_placement_authority_recovery(local_node_id, current_process)
        {
            return Err(KvError::Busy {
                path: format!(
                    "{} recovery requires the host's live exclusive data-owner capability",
                    placement_authority_permit_key(file_system_id)
                ),
            });
        }
        let key = placement_authority_permit_key(file_system_id);
        let Some(entry) = self.kv.get(&key)? else {
            return Ok(false);
        };
        let record = entry.decode::<DfsPlacementAuthorityPermitRecord>()?;
        if record.holder_cluster_id != current_process.cluster_id
            || &record.holder_node_id != local_node_id
            || record.holder_session_id == current_process.process_session_id
        {
            return Ok(false);
        }
        Ok(self
            .kv
            .txn(KvTxn {
                compares: vec![KvCompare::ValueEquals {
                    key: key.clone(),
                    value: entry.value,
                }],
                success: vec![KvOp::Delete { key }],
                failure: Vec::new(),
            })?
            .succeeded)
    }

    /// Acquire the exact v1 job/config generation through the quorum-backed
    /// transaction path and keep Apply excluded until the caller releases the
    /// returned permit. The preliminary reads preserve encoding compatibility;
    /// the transaction compares the exact stored bytes, not only decoded
    /// values, and writes a fresh non-expiring holder record so it cannot take
    /// the semantic no-effect shortcut.
    /// sinomo:kv-consistency: Serialize placement mutations with concurrent policy Apply using the existing global permit.
    pub(crate) fn try_acquire_placement_authority_permit(
        &mut self,
        expected_job: &DfsPlacementReconciliationJob,
        expected_config: &DfsFileSystemConfig,
        holder_node_id: &NodeId,
        holder_process: &DfsPlacementAuthorityProcessIdentity,
    ) -> Result<Option<DfsPlacementAuthorityPermit>, KvError> {
        validate_v1_placement_reconciliation_job(expected_job)?;
        let job_key =
            placement_reconciliation_key(&expected_job.file_system_id, &expected_job.job_id);
        let config_key = file_system_config_key(&expected_job.file_system_id);
        let Some(job_entry) = self.kv.get(&job_key)? else {
            return Ok(None);
        };
        if job_entry.decode::<DfsPlacementReconciliationJob>()? != *expected_job {
            return Ok(None);
        }
        let Some(config_entry) = self.kv.get(&config_key)? else {
            return Ok(None);
        };
        if config_entry.decode::<DfsFileSystemConfig>()? != *expected_config {
            return Ok(None);
        }

        let holder_id = sinomo_core::Id::generate().to_string();
        let record = DfsPlacementAuthorityPermitRecord {
            job_id: expected_job.job_id.clone(),
            holder_id,
            holder_cluster_id: holder_process.cluster_id.clone(),
            holder_node_id: holder_node_id.clone(),
            holder_session_id: holder_process.process_session_id.clone(),
        };
        let permit_key = placement_authority_permit_key(&expected_job.file_system_id);
        let permit_compare =
            match self.placement_authority_permit_missing_compare(&expected_job.file_system_id) {
                Ok(value) => value,
                Err(KvError::Busy { .. }) => return Ok(None),
                Err(error) => return Err(error),
            };
        let permit_put = KvOp::put_serde(permit_key, &record, None)?;
        // Arm the non-cloneable RAII permit before entering the transaction.
        // If the client panics or returns an error after quorum committed the
        // Put, Drop queues this exact record before the unwind can strand it.
        let mut permit = DfsPlacementAuthorityPermit {
            file_system_id: expected_job.file_system_id.clone(),
            record,
            released: false,
            #[cfg(test)]
            observation: None,
        };
        let response = self.kv.txn(KvTxn {
            compares: vec![
                KvCompare::ValueEquals {
                    key: job_key,
                    value: job_entry.value,
                },
                KvCompare::ValueEquals {
                    key: config_key,
                    value: config_entry.value,
                },
                permit_compare,
            ],
            success: vec![permit_put],
            failure: Vec::new(),
        })?;
        if !response.succeeded {
            // The authoritative compare rejected the Put, so Drop must not
            // queue a record that was never installed.
            permit.released = true;
            return Ok(None);
        }
        Ok(Some(permit))
    }

    /// sinomo:kv-consistency: Release only the exact globally owned placement permit after its mutation completes.
    pub(crate) fn release_placement_authority_permit(
        &mut self,
        permit: &mut DfsPlacementAuthorityPermit,
    ) -> Result<(), KvError> {
        // Queue the exact handle before contacting KV. This positive evidence
        // that the bounded mutation stack has exited survives ephemeral
        // service destruction and lets a later service finish an ambiguous or
        // failed release without inferring liveness from a timeout.
        if !permit.released {
            queue_placement_authority_release(permit);
        }
        let key = placement_authority_permit_key(&permit.file_system_id);
        let value = sinomo_core::kv::encode_kv_value(&permit.record)?;
        // A false compare means the exact holder is already gone. Never delete
        // a different holder's permit from a delayed finally/retry path.
        self.kv.txn(KvTxn {
            compares: vec![KvCompare::ValueEquals {
                key: key.clone(),
                value,
            }],
            success: vec![KvOp::Delete { key }],
            failure: Vec::new(),
        })?;
        clear_pending_placement_authority_release(&permit.file_system_id, &permit.record);
        permit.released = true;
        Ok(())
    }

    fn v1_placement_reconciliation(
        &self,
        file_system_id: &DfsFileSystemId,
        job_id: &str,
    ) -> Result<Option<DfsPlacementReconciliationJob>, KvError> {
        self.get_serde(&placement_reconciliation_key(file_system_id, job_id))
    }

    fn placement_reconciliation_queue_snapshot(
        &self,
        file_system_id: &DfsFileSystemId,
        _current_config: &DfsFileSystemConfig,
    ) -> Result<(Option<KvEntry>, DfsPlacementReconciliationQueue), KvError> {
        let key = placement_reconciliation_queue_key(file_system_id);
        let Some(entry) = self.kv.get(&key)? else {
            return Ok((
                None,
                DfsPlacementReconciliationQueue {
                    schema_version: PLACEMENT_RECONCILIATION_QUEUE_SCHEMA_VERSION,
                    job_ids: Vec::new(),
                },
            ));
        };
        let queue = placement_reconciliation_queue_from_entry(file_system_id, &entry)?;
        Ok((Some(entry), queue))
    }

    fn placement_reconciliation_activity_index_snapshot(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<(Option<KvEntry>, DfsPlacementReconciliationActivityIndex), KvError> {
        let entry = self
            .kv
            .get(&placement_reconciliation_activity_index_key(file_system_id))?;
        let index = placement_activity_index_from_entry(file_system_id, entry.as_ref())?;
        Ok((entry, index))
    }

    /// Atomically publish a placement-rule edit and its durable reconciliation
    /// job. The compare prevents two Files clients from silently losing one
    /// another's policy changes.
    /// sinomo:kv-consistency: Atomically publish an explicit placement policy update against the expected configuration and mutation permit.
    pub fn compare_and_put_placement_update(
        &mut self,
        expected: &DfsFileSystemConfig,
        desired: &DfsFileSystemConfig,
        job: &DfsPlacementReconciliationJob,
    ) -> Result<bool, KvError> {
        let publication = PlacementUpdatePublication::new(expected, desired, job)?;
        for attempt in 0..PLACEMENT_RECONCILIATION_QUEUE_CAS_RETRIES {
            // Only a definite rejection owns a new consistency read. Keep the
            // original approved configuration, candidate plan and job identity.
            let entries = if attempt == 0 {
                self.kv.get_consistent(&publication.keys)?
            } else {
                self.kv.get_authoritative_consistent(&publication.keys)?
            };
            let Some(inputs) = publication.inputs(entries)? else {
                return Ok(false);
            };
            let job_entries = match publication.queued_job_keys(&inputs)? {
                Some(keys) => {
                    let entries = if attempt == 0 || keys.is_empty() {
                        self.kv.get_consistent(&keys)?
                    } else {
                        self.kv.get_authoritative_consistent(&keys)?
                    };
                    entries.into_iter().flatten().collect()
                }
                None => Vec::new(),
            };
            let permit_compare = if inputs.permit.is_some() {
                self.placement_authority_permit_missing_compare(&job.file_system_id)?
            } else {
                KvCompare::Missing {
                    key: publication.keys[5].clone(),
                }
            };
            let txn = publication.transaction(inputs, job_entries, permit_compare)?;
            if self.kv.txn(txn)?.succeeded {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Load the compact, primary-owned debounce record for adaptive placement.
    /// Object metadata and health samples stay out of cluster KV; only the
    /// semantic snapshot identity and sustained-observation counters live here
    /// so a metadata-primary failover does not reset hysteresis.
    pub fn adaptive_placement_observation(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsAdaptivePlacementObservationState>, KvError> {
        self.get_serde(&adaptive_placement_observation_key(file_system_id))
    }

    pub fn compare_and_put_adaptive_placement_observation(
        &mut self,
        file_system_id: &DfsFileSystemId,
        expected: Option<&DfsAdaptivePlacementObservationState>,
        desired: &DfsAdaptivePlacementObservationState,
    ) -> Result<bool, KvError> {
        let key = adaptive_placement_observation_key(file_system_id);
        let current_entry = self.kv.get(&key)?;
        let current = current_entry.as_ref().map(KvEntry::decode).transpose()?;
        if current.as_ref() != expected {
            return Ok(false);
        }
        let compare = current_entry.map_or_else(
            || KvCompare::Missing { key: key.clone() },
            |entry| KvCompare::ValueEquals {
                key: key.clone(),
                value: entry.value,
            },
        );
        Ok(self
            .kv
            .txn(KvTxn {
                compares: vec![compare],
                success: vec![KvOp::put_serde(key, desired, None)?],
                failure: Vec::new(),
            })?
            .succeeded)
    }

    /// sinomo:kv-configuration: Read the committed placement configuration reconciliation plan and its control-plane progress.
    pub fn placement_reconciliation(
        &self,
        file_system_id: &DfsFileSystemId,
        job_id: &str,
    ) -> Result<Option<DfsPlacementReconciliationJob>, KvError> {
        self.v1_placement_reconciliation(file_system_id, job_id)
    }

    pub(crate) fn has_v1_placement_reconciliation_state(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<bool, KvError> {
        if self
            .kv
            .get(&placement_reconciliation_queue_key(file_system_id))?
            .is_some()
        {
            return Ok(true);
        }
        Ok(!self
            .kv
            .range_prefix(&placement_reconciliations_prefix(file_system_id))?
            .is_empty())
    }

    pub fn placement_reconciliations(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsPlacementReconciliationJob>, KvError> {
        let mut jobs = self
            .kv
            .range_prefix(&placement_reconciliations_prefix(file_system_id))?
            .into_iter()
            .map(|entry| entry.decode::<DfsPlacementReconciliationJob>())
            .collect::<Result<Vec<_>, _>>()?;
        jobs.sort_by(|left: &DfsPlacementReconciliationJob, right| {
            left.created_at_unix_nanos
                .cmp(&right.created_at_unix_nanos)
                .then_with(|| left.job_id.cmp(&right.job_id))
        });
        Ok(jobs)
    }

    /// Read a bounded operator activity window without scanning terminal job
    /// history. The retained recent index is unioned with every id in the
    /// independently bounded active queue, so a long-running job remains
    /// discoverable even after newer terminal activity fills the recent cap.
    ///
    /// Older daemons do not publish compact activity records. Those active
    /// jobs are point-read from their execution record and `partial` remains
    /// set; unindexed terminal history is deliberately not range-scanned.
    /// sinomo:kv-configuration: Inspect placement configuration job activity through its bounded control-plane index.
    pub fn placement_reconciliation_activity(
        &self,
        file_system_id: &DfsFileSystemId,
        statuses: &[DfsPlacementReconciliationActivityStatus],
        path_prefix: Option<&str>,
        after: Option<&DfsPlacementReconciliationActivityCursor>,
        limit: usize,
    ) -> Result<DfsPlacementReconciliationActivityPage, KvError> {
        if after.is_some_and(|cursor| cursor.job_id.trim().is_empty()) {
            return Err(KvError::Serialization {
                detail: "placement reconciliation activity cursor has an empty job id".to_string(),
            });
        }
        let (index_entry, index) =
            self.placement_reconciliation_activity_index_snapshot(file_system_id)?;
        let mut partial = index.partial || index_entry.is_none();
        let mut history_truncated = index.history_truncated || index_entry.is_none();
        let indexed_creation_times = index
            .recent
            .iter()
            .map(|cursor| (cursor.job_id.clone(), cursor.created_at_unix_nanos))
            .collect::<BTreeMap<_, _>>();
        let mut job_ids = index
            .recent
            .iter()
            .map(|cursor| cursor.job_id.clone())
            .collect::<BTreeSet<_>>();

        let include_active_queue = statuses.is_empty()
            || statuses.iter().any(|status| {
                matches!(
                    status,
                    DfsPlacementReconciliationActivityStatus::Running
                        | DfsPlacementReconciliationActivityStatus::Retrying
                )
            });
        if include_active_queue {
            let queue_key = placement_reconciliation_queue_key(file_system_id);
            if let Some(entry) = self.kv.get(&queue_key)? {
                let queue = entry.decode::<DfsPlacementReconciliationQueue>()?;
                validate_placement_reconciliation_queue(file_system_id, &queue)?;
                job_ids.extend(queue.job_ids);
            }
        }

        let mut activities = Vec::with_capacity(job_ids.len());
        for job_id in job_ids {
            let activity_key =
                placement_reconciliation_activity_record_key(file_system_id, &job_id);
            let activity = match self.kv.get(&activity_key)? {
                Some(entry) => {
                    let record = entry.decode::<DfsPlacementReconciliationActivityRecord>()?;
                    validate_placement_reconciliation_activity_record(
                        file_system_id,
                        &job_id,
                        &record,
                    )?;
                    record.activity
                }
                None => {
                    partial = true;
                    history_truncated = true;
                    continue;
                }
            };
            if indexed_creation_times
                .get(&job_id)
                .is_some_and(|created_at| *created_at != activity.created_at_unix_nanos)
            {
                return Err(KvError::Serialization {
                    detail: format!(
                        "placement reconciliation activity job {job_id} does not match its indexed creation cursor"
                    ),
                });
            }
            activities.push(activity);
        }

        let mut page = placement_reconciliation_activity_page_from_summaries(
            activities,
            statuses,
            path_prefix,
            after,
            limit,
        );
        page.partial = partial;
        page.history_truncated = history_truncated;
        Ok(page)
    }

    /// Return the next executable placement job from its compact active queue.
    /// Adaptive coalescing inspects
    /// only its bounded active identities, never terminal history or DFS objects.
    pub fn next_placement_reconciliation(
        &mut self,
        file_system_id: &DfsFileSystemId,
        current_config: &DfsFileSystemConfig,
    ) -> Result<Option<DfsPlacementReconciliationJob>, KvError> {
        self.next_placement_reconciliation_with_eligibility(file_system_id, current_config, |_| {
            true
        })
    }

    /// Select from the existing bounded queue in its established order. Parked
    /// jobs own no object pass; stale accepted configurations still reach their
    /// normal terminalization before eligibility can postpone them.
    pub(crate) fn next_placement_reconciliation_with_eligibility(
        &mut self,
        file_system_id: &DfsFileSystemId,
        current_config: &DfsFileSystemConfig,
        mut can_advance: impl FnMut(&DfsPlacementReconciliationJob) -> bool,
    ) -> Result<Option<DfsPlacementReconciliationJob>, KvError> {
        let queue_key = placement_reconciliation_queue_key(file_system_id);
        for _ in 0..PLACEMENT_RECONCILIATION_QUEUE_CAS_RETRIES {
            let (queue_entry, queue) =
                self.placement_reconciliation_queue_snapshot(file_system_id, current_config)?;
            let Some(queue_entry) = queue_entry else {
                let response = self.kv.txn(KvTxn {
                    compares: vec![KvCompare::Missing {
                        key: queue_key.clone(),
                    }],
                    success: vec![KvOp::put_serde(queue_key.clone(), &queue, None)?],
                    failure: Vec::new(),
                })?;
                if response.succeeded && queue.job_ids.is_empty() {
                    return Ok(None);
                }
                continue;
            };
            if let Some(retired_job_id) =
                self.coalesce_pending_adaptive_placement(file_system_id, current_config, &queue)?
                && queue.job_ids.first() == Some(&retired_job_id)
            {
                continue;
            }
            if queue.job_ids.is_empty() {
                return Ok(None);
            }
            let mut repair_leading_entry = false;
            for (position, candidate_id) in queue.job_ids.iter().enumerate() {
                let job = self.v1_placement_reconciliation(file_system_id, candidate_id)?;
                if let Some(job) = job {
                    validate_v1_placement_reconciliation_job(&job)?;
                    if !job.state.is_terminal() {
                        if can_advance(&job) {
                            return Ok(Some(job));
                        }
                        continue;
                    }
                }
                if position == 0 {
                    repair_leading_entry = true;
                    break;
                }
                // A stale tail behind a parked job owns no cleanup tick, as
                // in the scheduler's existing leading-prefix contract.
            }
            if !repair_leading_entry {
                return Ok(None);
            }

            // Missing/terminal ids can only result from interrupted migration
            // or an older writer. Repair them with a queue-value compare so a
            // concurrent Apply cannot be overwritten.
            let mut repaired = queue;
            repaired.job_ids.remove(0);
            let response = self.kv.txn(KvTxn {
                compares: vec![KvCompare::ValueEquals {
                    key: queue_key.clone(),
                    value: queue_entry.value,
                }],
                success: vec![KvOp::put_serde(queue_key.clone(), &repaired, None)?],
                failure: Vec::new(),
            })?;
            if response.succeeded && repaired.job_ids.is_empty() {
                return Ok(None);
            }
        }
        Err(KvError::Busy { path: queue_key })
    }

    /// Collapse one obsolete, unstarted automatic observation per bounded
    /// queue advance. Explicit work and jobs with durable progress retain ownership.
    fn coalesce_pending_adaptive_placement(
        &mut self,
        file_system_id: &DfsFileSystemId,
        current_config: &DfsFileSystemConfig,
        queue: &DfsPlacementReconciliationQueue,
    ) -> Result<Option<String>, KvError> {
        if queue.job_ids.len() < 2 {
            return Ok(None);
        }
        let config_key = file_system_config_key(file_system_id);
        let Some(config_entry) = self.kv.get(&config_key)? else {
            return Ok(None);
        };
        if config_entry.decode::<DfsFileSystemConfig>()? != *current_config {
            return Ok(None);
        }
        // The queue prioritizes authoritative configurations, not creation
        // time. Load it once and select replacements before considering stale
        // jobs, which may appear on either side of their replacement.
        let mut jobs = Vec::with_capacity(queue.job_ids.len());
        let mut newest = BTreeMap::<(String, bool), usize>::new();
        for job_id in &queue.job_ids {
            let key = placement_reconciliation_key(file_system_id, job_id);
            let Some(entry) = self.kv.get(&key)? else {
                continue;
            };
            let job = entry.decode::<DfsPlacementReconciliationJob>()?;
            validate_v1_placement_reconciliation_job(&job)?;
            if !job.adaptive || job.state.is_terminal() {
                continue;
            }
            let scope = (
                job.path.clone(),
                job.scope == crate::DfsPlacementScope::Subtree,
            );
            let index = jobs.len();
            jobs.push((entry, job));
            let job = &jobs[index].1;
            if placement_config_remains_authoritative_for_job(
                &job.accepted_config,
                current_config,
                job,
            ) && newest.get(&scope).is_none_or(|previous| {
                jobs[*previous].1.created_at_unix_nanos < job.created_at_unix_nanos
            }) {
                newest.insert(scope, index);
            }
        }
        let candidate = jobs.iter().find_map(|(_, job)| {
            let scope = (
                job.path.clone(),
                job.scope == crate::DfsPlacementScope::Subtree,
            );
            let (newest_entry, replacement) = &jobs[*newest.get(&scope)?];
            (job.state == crate::DfsPlacementReconciliationState::Pending
                && job.cursor.is_none()
                && job.scanned_object_count == 0
                && job.created_at_unix_nanos < replacement.created_at_unix_nanos)
                .then_some((job, newest_entry, &replacement.job_id))
        });
        let Some((job, newest_entry, replacement_id)) = candidate else {
            return Ok(None);
        };
        let mut retired = job.clone();
        retired.state = crate::DfsPlacementReconciliationState::Failed;
        retired.revision = retired.revision.saturating_add(1);
        retired.updated_at_unix_nanos = crate::dfs_now_unix_nanos();
        retired.last_error = Some(format!(
            "{}{replacement_id}",
            sinomo_api::dfs::placement::PLACEMENT_RECONCILIATION_SUPERSEDED_ADAPTIVE_PREFIX
        ));
        // Pending may contain effects from a cancelled first page. Use the
        // normal failed-job fence, and compare the replacement and config
        // in the same transaction before revoking any delayed old effects.
        let guards = vec![
            KvCompare::ValueEquals {
                key: config_key,
                value: config_entry.value,
            },
            KvCompare::ValueEquals {
                key: newest_entry.key.clone(),
                value: newest_entry.value.clone(),
            },
        ];
        match self.compare_and_put_placement_reconciliation_with_guards(job, &retired, guards) {
            Ok(true) => Ok(Some(job.job_id.clone())),
            Ok(false) | Err(KvError::Busy { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub fn compare_and_put_placement_reconciliation(
        &mut self,
        expected: &DfsPlacementReconciliationJob,
        desired: &DfsPlacementReconciliationJob,
    ) -> Result<bool, KvError> {
        self.compare_and_put_placement_reconciliation_with_guards(expected, desired, Vec::new())
    }

    pub(crate) fn compare_and_put_placement_reconciliation_with_guards(
        &mut self,
        expected: &DfsPlacementReconciliationJob,
        desired: &DfsPlacementReconciliationJob,
        compares: Vec<KvCompare>,
    ) -> Result<bool, KvError> {
        validate_placement_reconciliation_progress(expected, desired)?;
        let key = placement_reconciliation_key(&expected.file_system_id, &expected.job_id);
        let Some(current_entry) = self.kv.get(&key)? else {
            return Ok(false);
        };
        if current_entry.decode::<DfsPlacementReconciliationJob>()? != *expected {
            return Ok(false);
        }
        let queue_entry = self
            .kv
            .get(&placement_reconciliation_queue_key(&desired.file_system_id))?;
        let activity_index_entry = self.kv.get(&placement_reconciliation_activity_index_key(
            &desired.file_system_id,
        ))?;
        let mut txn = placement_reconciliation_progress_transaction(
            desired,
            current_entry,
            queue_entry,
            activity_index_entry,
            compares,
        )?;
        if desired.state == crate::DfsPlacementReconciliationState::Failed {
            txn.compares
                .push(self.placement_authority_permit_missing_compare(&desired.file_system_id)?);
        }
        Ok(self.kv.txn(txn)?.succeeded)
    }

    pub fn put_metadata_shard(&mut self, shard: &DfsMetadataShardConfig) -> Result<(), KvError> {
        let key = metadata_shard_key(&shard.file_system_id, &shard.shard_id);
        for _ in 0..METADATA_MEMBERSHIP_HANDOFF_RETRIES {
            let current_entry = self.kv.get(&key)?;
            if let Some(entry) = current_entry.as_ref() {
                let current = entry.decode::<DfsMetadataShardConfig>()?;
                validate_raw_metadata_shard_write(&current, shard)?;
                if current == *shard {
                    return Ok(());
                }
            }
            let mut compares = vec![match current_entry.as_ref() {
                Some(entry) => KvCompare::ValueEquals {
                    key: key.clone(),
                    value: entry.value.clone(),
                },
                None => KvCompare::Missing { key: key.clone() },
            }];
            if current_entry.is_none() {
                compares.extend(metadata_prepared_operation_admission_compares(
                    self.kv, shard,
                )?);
            }
            if self
                .kv
                .txn(KvTxn {
                    compares,
                    success: vec![KvOp::put_serde(key.clone(), shard, None)?],
                    failure: Vec::new(),
                })?
                .succeeded
            {
                return Ok(());
            }
        }
        Err(KvError::Busy { path: key })
    }

    /// Create an absent shard or idempotently replace the exact same value.
    ///
    /// A changed assignment or primary role is deliberately *not* activated here.
    /// It must use [`Self::begin_metadata_shard_membership_handoff`] followed by
    /// [`Self::complete_metadata_shard_membership_handoff`] so a committed
    /// reconfiguration decision cannot outrun the retiring DFS metadata state.
    pub fn compare_and_put_metadata_shard(
        &mut self,
        expected: Option<&DfsMetadataShardConfig>,
        desired: &DfsMetadataShardConfig,
    ) -> Result<bool, KvError> {
        validate_metadata_shard_cas_identity(expected, desired)?;
        let key = metadata_shard_key(&desired.file_system_id, &desired.shard_id);
        let current_entry = self.kv.get(&key)?;
        let shard_compare = match (expected, current_entry.as_ref()) {
            (None, None) => KvCompare::Missing { key: key.clone() },
            (Some(expected), Some(entry))
                if entry.decode::<DfsMetadataShardConfig>()? == *expected =>
            {
                KvCompare::ValueEquals {
                    key: key.clone(),
                    value: entry.value.clone(),
                }
            }
            _ => return Ok(false),
        };
        let mut compares = vec![shard_compare];
        if let Some(expected) = expected
            && expected != desired
        {
            validate_metadata_shard_cas_reconfiguration(expected, desired)?;
            return Ok(false);
        }
        if expected.is_none() {
            compares.extend([
                KvCompare::Missing {
                    key: metadata_shard_membership_handoff_key(
                        &desired.file_system_id,
                        &desired.shard_id,
                    ),
                },
                KvCompare::Missing {
                    key: metadata_shard_operation_floor_key(
                        &desired.file_system_id,
                        &desired.shard_id,
                    ),
                },
                KvCompare::Missing {
                    key: metadata_shard_head_watermark_key(
                        &desired.file_system_id,
                        &desired.shard_id,
                    ),
                },
            ]);
            compares.extend(metadata_prepared_operation_admission_compares(
                self.kv, desired,
            )?);
        }
        Ok(self
            .kv
            .txn(KvTxn {
                compares,
                success: vec![KvOp::put_serde(key, desired, None)?],
                failure: Vec::new(),
            })?
            .succeeded)
    }

    /// Request a topology-preserving primary transfer. This creates a handoff
    /// intent only; the retiring-primary fence, replica prefix proof, joint
    /// acknowledgement and activation protocol still own the authority change.
    pub fn request_metadata_shard_primary_handoff(
        &mut self,
        retiring: &DfsMetadataShardConfig,
        new_primary: &NodeId,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        let desired = metadata_shard_primary_handoff_target(retiring, new_primary)?;
        self.begin_metadata_shard_membership_handoff(retiring, &desired)
    }

    /// Record a pending membership or primary-role decision before catch-up.
    ///
    /// The shard configuration itself is intentionally left unchanged. The
    /// returned record binds the retiring and desired assignments to a fresh
    /// id and to the highest quorum-permitted prepare sequence observed in the
    /// same Raft transaction. Because a quorum-side caller may commit this
    /// while the retiring primary is partitioned, this record alone is not the
    /// retiring-primary publication fence; that is attached later by
    /// [`Self::acknowledge_metadata_shard_membership_handoff_fence`].
    pub fn begin_metadata_shard_membership_handoff(
        &mut self,
        retiring: &DfsMetadataShardConfig,
        desired: &DfsMetadataShardConfig,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        self.begin_metadata_shard_handoff_with_cause(
            retiring,
            desired,
            DfsMetadataShardHandoffCause::Membership,
        )
    }

    pub fn begin_metadata_shard_handoff_with_cause(
        &mut self,
        retiring: &DfsMetadataShardConfig,
        desired: &DfsMetadataShardConfig,
        cause: DfsMetadataShardHandoffCause,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        validate_metadata_shard_cas_identity(Some(retiring), desired)?;
        if retiring == desired {
            return Err(KvError::Serialization {
                detail: format!(
                    "metadata shard {} membership handoff does not change the assignment",
                    retiring.shard_id
                ),
            });
        }
        validate_metadata_shard_handoff_cause(retiring, desired, &cause)?;
        let shard_key = metadata_shard_key(&retiring.file_system_id, &retiring.shard_id);
        let floor_key =
            metadata_shard_operation_floor_key(&retiring.file_system_id, &retiring.shard_id);
        let watermark_key =
            metadata_shard_head_watermark_key(&retiring.file_system_id, &retiring.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&retiring.file_system_id, &retiring.shard_id);
        for _ in 0..METADATA_MEMBERSHIP_HANDOFF_RETRIES {
            let Some(shard_entry) = self.kv.get(&shard_key)? else {
                return Ok(None);
            };
            if shard_entry.decode::<DfsMetadataShardConfig>()? != *retiring {
                return Ok(None);
            }
            if let Some(entry) = self.kv.get(&handoff_key)? {
                let handoff = entry.decode::<DfsMetadataShardMembershipHandoff>()?;
                return Ok((handoff.retiring_shard == *retiring
                    && handoff.desired_shard == *desired
                    && handoff.cause == cause)
                    .then_some(handoff));
            }
            let write_set_entry = self.kv.get(&metadata_shard_write_set_key(
                &retiring.file_system_id,
                &retiring.shard_id,
            ))?;
            let (write_set_compare, _) = metadata_handoff_write_set_ops(
                retiring,
                desired,
                &cause,
                write_set_entry.as_ref(),
                None,
            )?;

            let floor_entry = self.kv.get(&floor_key)?;
            let floor_sequence = match floor_entry.as_ref() {
                Some(entry) => {
                    let floor = entry.decode::<DfsMetadataShardOperationFloor>()?;
                    if floor.authority_epoch > retiring.epoch {
                        return Ok(None);
                    }
                    floor.prepared_sequence
                }
                None => 0,
            };
            let watermark_entry = self.kv.get(&watermark_key)?;
            let required_head_watermark = watermark_entry
                .as_ref()
                .map(|entry| {
                    normalize_metadata_shard_head_watermark(
                        retiring,
                        &entry.decode::<DfsMetadataShardHeadWatermark>()?,
                    )
                })
                .transpose()?;
            let required_prepared_sequence = required_head_watermark
                .as_ref()
                .map(|watermark| watermark.journal_sequence)
                .unwrap_or(0)
                .max(floor_sequence);
            let handoff = DfsMetadataShardMembershipHandoff {
                cause: cause.clone(),
                handoff_id: match &cause {
                    DfsMetadataShardHandoffCause::Membership => sinomo_core::Id::generate(),
                    DfsMetadataShardHandoffCause::StorageImport { cut } => {
                        sinomo_core::Id::derived(
                            "dfs-storage-import-handoff-v7",
                            serde_json::to_vec(&(retiring, desired, cut)).map_err(|e| {
                                KvError::Serialization {
                                    detail: e.to_string(),
                                }
                            })?,
                        )
                    }
                }
                .to_string(),
                retiring_shard: retiring.clone(),
                desired_shard: desired.clone(),
                required_prepared_sequence,
                required_head_watermark,
                retiring_primary_fence: None,
            };
            let floor_compare = match floor_entry {
                Some(entry) => KvCompare::ValueEquals {
                    key: floor_key.clone(),
                    value: entry.value,
                },
                None => KvCompare::Missing {
                    key: floor_key.clone(),
                },
            };
            let watermark_compare = match watermark_entry {
                Some(entry) => KvCompare::ValueEquals {
                    key: watermark_key.clone(),
                    value: entry.value,
                },
                None => KvCompare::Missing {
                    key: watermark_key.clone(),
                },
            };
            let mut compares = vec![
                KvCompare::ValueEquals {
                    key: shard_key.clone(),
                    value: shard_entry.value,
                },
                floor_compare,
                watermark_compare,
                KvCompare::Missing {
                    key: handoff_key.clone(),
                },
                write_set_compare,
            ];
            compares.extend(metadata_prepared_operation_admission_compares(
                self.kv, desired,
            )?);
            let response = self.kv.txn(KvTxn {
                compares,
                success: vec![KvOp::put_serde(handoff_key.clone(), &handoff, None)?],
                failure: Vec::new(),
            })?;
            if response.succeeded {
                return Ok(Some(handoff));
            }
        }
        Ok(None)
    }

    /// Withdraw an exact handoff that has not yet acquired its retiring-primary
    /// fence. The active assignment is unchanged until activation, so this is
    /// the safe escape hatch when the retiring primary discovers durable work
    /// that must be resolved before a replacement fence can be captured.
    /// sinomo:kv-consistency: Cancel only the exact unfenced global handoff proposal without undoing an activated or durably fenced transition.
    pub fn cancel_unfenced_metadata_shard_membership_handoff(
        &mut self,
        expected_handoff: &DfsMetadataShardMembershipHandoff,
    ) -> Result<bool, KvError> {
        let retiring = &expected_handoff.retiring_shard;
        let shard_key = metadata_shard_key(&retiring.file_system_id, &retiring.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&retiring.file_system_id, &retiring.shard_id);
        let Some(shard_entry) = self.kv.get(&shard_key)? else {
            return Ok(false);
        };
        if shard_entry.decode::<DfsMetadataShardConfig>()? != *retiring {
            return Ok(false);
        }
        let Some(handoff_entry) = self.kv.get(&handoff_key)? else {
            return Ok(false);
        };
        let handoff = handoff_entry.decode::<DfsMetadataShardMembershipHandoff>()?;
        if !metadata_shard_membership_handoff_identity_matches(&handoff, expected_handoff)
            || handoff.retiring_primary_fence.is_some()
        {
            return Ok(false);
        }
        Ok(self
            .kv
            .txn(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key: shard_key,
                        value: shard_entry.value,
                    },
                    KvCompare::ValueEquals {
                        key: handoff_key.clone(),
                        value: handoff_entry.value,
                    },
                ],
                success: vec![KvOp::Delete { key: handoff_key }],
                failure: Vec::new(),
            })?
            .succeeded)
    }

    /// Attach the exact post-fence retiring-primary head to a pending handoff.
    ///
    /// The caller must be the retiring-primary service and must capture
    /// `fence` while holding its process-wide publication guard after a fresh
    /// control-plane read. This transaction makes that observation durable and
    /// immutable before any data-plane catch-up can activate the desired set.
    /// sinomo:kv-consistency: Publish the retiring primary durable fence into the exact globally ordered handoff proposal.
    pub fn acknowledge_metadata_shard_membership_handoff_fence(
        &mut self,
        expected_handoff: &DfsMetadataShardMembershipHandoff,
        fence: &DfsMetadataShardMembershipHandoffFence,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        let retiring = &expected_handoff.retiring_shard;
        let shard_key = metadata_shard_key(&retiring.file_system_id, &retiring.shard_id);
        let floor_key =
            metadata_shard_operation_floor_key(&retiring.file_system_id, &retiring.shard_id);
        let watermark_key =
            metadata_shard_head_watermark_key(&retiring.file_system_id, &retiring.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&retiring.file_system_id, &retiring.shard_id);

        let Some(shard_entry) = self.kv.get(&shard_key)? else {
            return Ok(None);
        };
        if shard_entry.decode::<DfsMetadataShardConfig>()? != *retiring {
            return Ok(None);
        }
        let Some(handoff_entry) = self.kv.get(&handoff_key)? else {
            return Ok(None);
        };
        let handoff = handoff_entry.decode::<DfsMetadataShardMembershipHandoff>()?;
        if !metadata_shard_membership_handoff_identity_matches(&handoff, expected_handoff) {
            return Ok(None);
        }
        if let Some(existing) = handoff.retiring_primary_fence.as_ref() {
            return Ok((existing == fence).then_some(handoff));
        }
        // A write-set shard admits only a primary rotation; the retiring
        // primary never fences any other handoff, so it cannot activate.
        let write_set_entry = self.kv.get(&metadata_shard_write_set_key(
            &retiring.file_system_id,
            &retiring.shard_id,
        ))?;
        if metadata_handoff_write_set_ops(
            retiring,
            &handoff.desired_shard,
            &handoff.cause,
            write_set_entry.as_ref(),
            None,
        )
        .is_err()
        {
            return Ok(None);
        }

        let floor_entry = self.kv.get(&floor_key)?;
        let current_floor = floor_entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardOperationFloor>())
            .transpose()?;
        let fence = normalize_metadata_shard_membership_handoff_fence(
            &handoff,
            fence,
            current_floor.as_ref(),
        )?;
        let watermark_entry = self.kv.get(&watermark_key)?;
        if !metadata_shard_membership_handoff_watermark_is_exact(
            &handoff,
            watermark_entry.as_ref(),
        )? {
            return Ok(None);
        }
        let floor_compare = match floor_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: floor_key,
                value: entry.value,
            },
            None => KvCompare::Missing { key: floor_key },
        };
        let watermark_compare = match watermark_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: watermark_key,
                value: entry.value,
            },
            None => KvCompare::Missing { key: watermark_key },
        };
        let mut acknowledged = handoff;
        acknowledged.retiring_primary_fence = Some(fence);
        Ok(self
            .kv
            .txn(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key: shard_key,
                        value: shard_entry.value,
                    },
                    KvCompare::ValueEquals {
                        key: handoff_key.clone(),
                        value: handoff_entry.value,
                    },
                    floor_compare,
                    watermark_compare,
                ],
                success: vec![KvOp::put_serde(handoff_key, &acknowledged, None)?],
                failure: Vec::new(),
            })?
            .succeeded
            .then_some(acknowledged))
    }

    /// Activate a desired assignment or primary role only after exact retiring
    /// state has been acknowledged by a joint quorum of both configurations.
    /// sinomo:kv-consistency: Atomically activate configuration after durable retiring-primary fencing and joint assignment proof.
    pub fn complete_metadata_shard_membership_handoff(
        &mut self,
        retiring: &DfsMetadataShardConfig,
        desired: &DfsMetadataShardConfig,
        proof: &DfsMetadataShardMembershipHandoffProof,
    ) -> Result<bool, KvError> {
        validate_metadata_shard_cas_identity(Some(retiring), desired)?;

        let shard_key = metadata_shard_key(&retiring.file_system_id, &retiring.shard_id);
        let floor_key =
            metadata_shard_operation_floor_key(&retiring.file_system_id, &retiring.shard_id);
        let watermark_key =
            metadata_shard_head_watermark_key(&retiring.file_system_id, &retiring.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&retiring.file_system_id, &retiring.shard_id);
        let proof_key = metadata_shard_membership_handoff_proof_key(
            &retiring.file_system_id,
            &retiring.shard_id,
        );

        let Some(shard_entry) = self.kv.get(&shard_key)? else {
            return Ok(false);
        };
        if shard_entry.decode::<DfsMetadataShardConfig>()? != *retiring {
            return Ok(false);
        }
        let Some(handoff_entry) = self.kv.get(&handoff_key)? else {
            return Ok(false);
        };
        let handoff = handoff_entry.decode::<DfsMetadataShardMembershipHandoff>()?;
        validate_metadata_shard_handoff_cause(retiring, desired, &handoff.cause)?;
        if handoff.retiring_shard != *retiring || handoff.desired_shard != *desired {
            return Ok(false);
        }
        let write_set_entry = self.kv.get(&metadata_shard_write_set_key(
            &retiring.file_system_id,
            &retiring.shard_id,
        ))?;
        let floor_entry = self.kv.get(&floor_key)?;
        let current_floor = floor_entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardOperationFloor>())
            .transpose()?;
        let proof = normalize_metadata_shard_membership_handoff_proof(
            &handoff,
            proof,
            current_floor.as_ref(),
        )?;
        let activated_watermark =
            metadata_shard_membership_handoff_activated_watermark(&handoff, &proof)?;
        let (write_set_compare, write_set_put) = metadata_handoff_write_set_ops(
            retiring,
            desired,
            &handoff.cause,
            write_set_entry.as_ref(),
            Some(&proof),
        )?;
        let watermark_entry = self.kv.get(&watermark_key)?;
        if !metadata_shard_membership_handoff_watermark_is_exact(
            &handoff,
            watermark_entry.as_ref(),
        )? {
            return Ok(false);
        }
        let floor_compare = match floor_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: floor_key,
                value: entry.value,
            },
            None => KvCompare::Missing { key: floor_key },
        };
        let watermark_compare = match watermark_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: watermark_key.clone(),
                value: entry.value,
            },
            None => KvCompare::Missing {
                key: watermark_key.clone(),
            },
        };
        let mut compares = vec![
            KvCompare::ValueEquals {
                key: shard_key.clone(),
                value: shard_entry.value,
            },
            KvCompare::ValueEquals {
                key: handoff_key.clone(),
                value: handoff_entry.value,
            },
            floor_compare,
            watermark_compare,
            write_set_compare,
        ];
        compares.extend(metadata_prepared_operation_admission_compares(
            self.kv, desired,
        )?);
        Ok(self
            .kv
            .txn(KvTxn {
                compares,
                success: vec![
                    KvOp::put_serde(shard_key, desired, None)?,
                    KvOp::put_serde(watermark_key, &activated_watermark, None)?,
                    KvOp::put_serde(proof_key, &proof, None)?,
                    KvOp::Delete { key: handoff_key },
                ]
                .into_iter()
                .chain(write_set_put)
                .collect(),
                failure: Vec::new(),
            })?
            .succeeded)
    }

    /// Read only the active assignment and pending handoff for one retiring
    /// shard under its caller-held publication fence and original deadline.
    /// sinomo:kv-consistency: Fence retiring-shard publication against a concurrently completed metadata handoff using one exact authoritative key set.
    pub(crate) fn retiring_metadata_shard_handoff_view_before(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
        deadline: Instant,
    ) -> Result<
        (
            Option<DfsMetadataShardConfig>,
            Option<DfsMetadataShardMembershipHandoff>,
        ),
        crate::DfsError,
    > {
        let keys = [
            metadata_shard_key(file_system_id, shard_id),
            metadata_shard_membership_handoff_key(file_system_id, shard_id),
        ];
        let entries = self
            .kv
            .get_authoritative_consistent_before(&keys, deadline)?;
        if entries.len() != keys.len()
            || entries
                .iter()
                .zip(&keys)
                .any(|(entry, key)| entry.as_ref().is_some_and(|entry| &entry.key != key))
        {
            return Err(crate::DfsError::Metadata {
                message: "handoff authority read returned a differently scoped key set".into(),
            });
        }
        let active = entries[0]
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardConfig>())
            .transpose()?;
        let pending = entries[1]
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoff>())
            .transpose()?;
        Ok((active, pending))
    }

    /// sinomo:kv-configuration: Read the configured handoff proposal; this does not authorize activation.
    pub fn get_metadata_shard_membership_handoff(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        self.get_serde(&metadata_shard_membership_handoff_key(
            file_system_id,
            shard_id,
        ))
    }

    /// sinomo:kv-configuration: Read the existing committed configuration activation certificate.
    pub fn get_metadata_shard_membership_handoff_proof(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardMembershipHandoffProof>, KvError> {
        self.get_serde(&metadata_shard_membership_handoff_proof_key(
            file_system_id,
            shard_id,
        ))
    }

    /// sinomo:kv-configuration: Read the assigned shard through the existing retained configuration projection.
    pub fn get_metadata_shard(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        self.get_serde(&metadata_shard_key(file_system_id, shard_id))
    }

    pub fn get_metadata_shard_head_watermark(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardHeadWatermark>, KvError> {
        self.get_serde(&metadata_shard_head_watermark_key(file_system_id, shard_id))
    }

    /// Publish a quorum-durable metadata head only while `expected_shard` is
    /// still the exact committed shard assignment.
    ///
    /// The previous watermark participates in the compare so concurrent syncs
    /// cannot regress the durable head. A fresh authority-barrier write keeps
    /// repeated identical publication on the Raft quorum path instead of the
    /// semantic no-effect fast path.
    /// sinomo:kv-consistency: Preserve the exact degraded recovery watermark across globally ordered shard handoffs.
    pub fn publish_metadata_shard_head_watermark(
        &mut self,
        expected_shard: &DfsMetadataShardConfig,
        watermark: &DfsMetadataShardHeadWatermark,
    ) -> Result<bool, KvError> {
        let watermark = normalize_metadata_shard_head_watermark(expected_shard, watermark)?;
        let shard_key =
            metadata_shard_key(&expected_shard.file_system_id, &expected_shard.shard_id);
        let watermark_key = metadata_shard_head_watermark_key(
            &expected_shard.file_system_id,
            &expected_shard.shard_id,
        );
        let handoff_key = metadata_shard_membership_handoff_key(
            &expected_shard.file_system_id,
            &expected_shard.shard_id,
        );

        for _ in 0..METADATA_HEAD_WATERMARK_PUBLISH_RETRIES {
            let Some(shard_entry) = self.kv.get(&shard_key)? else {
                return Ok(false);
            };
            if shard_entry.decode::<DfsMetadataShardConfig>()? != *expected_shard {
                return Ok(false);
            }
            if self.kv.get(&handoff_key)?.is_some() {
                return Ok(false);
            }

            let previous_entry = self.kv.get(&watermark_key)?;
            let published = merged_metadata_shard_head_watermark(
                expected_shard,
                previous_entry.as_ref(),
                &watermark,
            )?;
            let compares = vec![
                KvCompare::ValueEquals {
                    key: shard_key.clone(),
                    value: shard_entry.value,
                },
                KvCompare::Missing {
                    key: handoff_key.clone(),
                },
                match previous_entry.as_ref() {
                    Some(entry) => KvCompare::ValueEquals {
                        key: watermark_key.clone(),
                        value: entry.value.clone(),
                    },
                    None => KvCompare::Missing {
                        key: watermark_key.clone(),
                    },
                },
            ];
            let barrier_value =
                sinomo_core::kv::encode_kv_value(&sinomo_core::Id::generate().to_string())?;
            let response = self.kv.txn(KvTxn {
                compares,
                success: vec![
                    KvOp::put_serde(watermark_key.clone(), &published, None)?,
                    KvOp::Put {
                        key: metadata_shard_authority_barrier_key(
                            &expected_shard.file_system_id,
                            &expected_shard.shard_id,
                        ),
                        value: barrier_value,
                        lease_id: None,
                    },
                ],
                failure: Vec::new(),
            })?;
            if response.succeeded {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// sinomo:kv-configuration: Resolve namespace routing from the existing retained shard configuration projection.
    pub fn list_metadata_shards(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsMetadataShardConfig>, KvError> {
        if let Some(shards) = self.kv.cached_metadata_shards(file_system_id)? {
            return Ok(shards);
        }
        let entries = tracing::trace_span!(target: "sinomo.perf", "dfs.control_plane.shards",
            stage = "dfs.control_plane.shards", phase = "read_prefix")
        .in_scope(|| {
            self.kv
                .range_prefix(&format!("{}/", metadata_shards_prefix(file_system_id)))
        })?;
        let mut shards: Vec<DfsMetadataShardConfig> =
            tracing::trace_span!(target: "sinomo.perf", "dfs.control_plane.shards",
                stage = "dfs.control_plane.shards", phase = "decode", entries = entries.len())
            .in_scope(|| {
                entries
                    .into_iter()
                    .map(|entry| entry.decode())
                    .collect::<Result<Vec<_>, _>>()
            })?;
        let _sort = tracing::trace_span!(target: "sinomo.perf", "dfs.control_plane.shards",
            stage = "dfs.control_plane.shards", phase = "sort")
        .entered();
        shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        Ok(shards)
    }

    pub fn select_metadata_shard_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        select_metadata_shard_config_for_path(
            file_system_id,
            &self.list_metadata_shards(file_system_id)?,
            path,
        )
    }

    pub fn select_metadata_shard_for_path_strict_default_sifs_split(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        select_metadata_shard_config_for_path_strict_default_sifs_split(
            file_system_id,
            &self.list_metadata_shards(file_system_id)?,
            path,
        )
    }

    /// Prove through a Raft-backed write barrier that `expected` is still the
    /// exact committed metadata-shard assignment.
    pub fn certify_metadata_shard_authority(
        &mut self,
        expected: &DfsMetadataShardConfig,
    ) -> Result<bool, KvError> {
        let key = metadata_shard_key(&expected.file_system_id, &expected.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&expected.file_system_id, &expected.shard_id);
        let Some(entry) = self.kv.get(&key)? else {
            return Ok(false);
        };
        if entry.decode::<DfsMetadataShardConfig>()? != *expected {
            return Ok(false);
        }
        if self.kv.get(&handoff_key)?.is_some() {
            return Ok(false);
        }
        Ok(self
            .kv
            .txn(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key,
                        value: entry.value,
                    },
                    KvCompare::Missing { key: handoff_key },
                ],
                success: vec![KvOp::Put {
                    key: metadata_shard_authority_barrier_key(
                        &expected.file_system_id,
                        &expected.shard_id,
                    ),
                    value: sinomo_core::kv::encode_kv_value(
                        &sinomo_core::Id::generate().to_string(),
                    )?,
                    lease_id: None,
                }],
                failure: Vec::new(),
            })?
            .succeeded)
    }

    /// Refresh cluster configuration and verify that `expected` is still the
    /// active assignment with no membership handoff pending.
    ///
    /// This check deliberately reads only DFS configuration from cluster KV.
    /// The caller already holds typed full-shard or cluster-quorum
    /// authorization; this final read and the shared publication guard order
    /// an in-flight commit before a subsequently observed handoff fence.
    pub fn metadata_shard_authority_is_current(
        &mut self,
        expected: &DfsMetadataShardConfig,
    ) -> Result<bool, KvError> {
        self.metadata_shard_authority_is_current_before(expected, None)
    }

    /// Validate one shard assignment before the caller's existing absolute
    /// operation deadline. When no caller deadline exists, the normal
    /// control-plane Raft budget bounds the complete retry sequence.
    /// sinomo:kv-consistency: Fence publication against a concurrent committed ownership or membership transition.
    pub fn metadata_shard_authority_is_current_before(
        &mut self,
        expected: &DfsMetadataShardConfig,
        operation_deadline: Option<Instant>,
    ) -> Result<bool, KvError> {
        let key = metadata_shard_key(&expected.file_system_id, &expected.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&expected.file_system_id, &expected.shard_id);
        let requested_keys = [key, handoff_key];
        let deadline = metadata_shard_authority_read_deadline(operation_deadline);
        for attempt in 0..METADATA_SHARD_AUTHORITY_READ_RETRIES {
            if Instant::now() >= deadline {
                return Err(metadata_shard_authority_deadline_error());
            }
            match self
                .kv
                .get_authoritative_consistent_before(&requested_keys, deadline)
            {
                Ok(entries) => {
                    return metadata_shard_authority_entries_are_current(
                        expected,
                        &requested_keys,
                        entries,
                    );
                }
                Err(error)
                    if metadata_operation_authority_view_error_is_retryable(&error)
                        && attempt + 1 < METADATA_SHARD_AUTHORITY_READ_RETRIES =>
                {
                    // Every authoritative exact-set call starts a fresh client
                    // generation but inherits this one absolute deadline.
                    // Replay only transient authority-view failures; semantic
                    // rejection and storage/codec errors fail closed.
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("metadata shard authority retry loop always returns")
    }

    /// Certify one bounded placement page through the existing ReadIndex
    /// boundary. The returned comparisons are mandatory at its progress CAS;
    /// they never authorize object-side mutation or survive another page.
    /// sinomo:kv-consistency: Batch exact configuration and shard/handoff checks for one bounded reconciliation page, then fence its checkpoint in Raft.
    pub(crate) fn placement_page_authority_compares_before(
        &mut self,
        expected_config: &DfsFileSystemConfig,
        expected_shards: &[DfsMetadataShardConfig],
        operation_deadline: Option<Instant>,
    ) -> Result<Option<Vec<KvCompare>>, KvError> {
        let config_key = file_system_config_key(&expected_config.file_system_id);
        let mut keys = vec![config_key];
        for shard in expected_shards {
            if shard.file_system_id != expected_config.file_system_id {
                return Err(KvError::Serialization {
                    detail: "placement page contains a foreign filesystem shard".into(),
                });
            }
            keys.push(metadata_shard_key(&shard.file_system_id, &shard.shard_id));
            keys.push(metadata_shard_membership_handoff_key(
                &shard.file_system_id,
                &shard.shard_id,
            ));
        }
        let deadline = metadata_shard_authority_read_deadline(operation_deadline);
        for attempt in 0..METADATA_SHARD_AUTHORITY_READ_RETRIES {
            if Instant::now() >= deadline {
                return Err(metadata_shard_authority_deadline_error());
            }
            let entries = match self.kv.get_authoritative_consistent_before(&keys, deadline) {
                Ok(entries) => entries,
                Err(error)
                    if metadata_operation_authority_view_error_is_retryable(&error)
                        && attempt + 1 < METADATA_SHARD_AUTHORITY_READ_RETRIES =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };
            if entries.len() != keys.len()
                || keys
                    .iter()
                    .zip(&entries)
                    .any(|(key, entry)| entry.as_ref().is_some_and(|entry| entry.key != *key))
            {
                return Err(KvError::Serialization {
                    detail: "placement page authority returned an invalid exact-key projection"
                        .into(),
                });
            }
            let Some(config_entry) = entries[0].as_ref() else {
                return Ok(None);
            };
            if config_entry.decode::<DfsFileSystemConfig>()? != *expected_config {
                return Ok(None);
            }
            for (index, shard) in expected_shards.iter().enumerate() {
                let offset = 1 + index * 2;
                if !metadata_shard_authority_entries_are_current(
                    shard,
                    &keys[offset..offset + 2],
                    entries[offset..offset + 2].to_vec(),
                )? {
                    return Ok(None);
                }
            }
            return Ok(Some(
                keys.into_iter()
                    .zip(entries)
                    .map(|(key, entry)| match entry {
                        Some(entry) => KvCompare::ValueEquals {
                            key,
                            value: entry.value,
                        },
                        None => KvCompare::Missing { key },
                    })
                    .collect(),
            ));
        }
        unreachable!("placement page authority retry loop always returns")
    }

    /// Return the unchanged shard for an idempotent request, but fail closed
    /// for an actual primary change.
    ///
    /// Cluster KV cannot certify the current DFS journal head because healthy
    /// full-shard writes intentionally do not update a Raft watermark. A future
    /// proof-bearing data-plane handoff must freeze the retiring primary and
    /// prove that exact prefix on the promoted replica before this surface can
    /// activate a different primary.
    /// sinomo:kv-consistency: Reject unfenced primary transfers using the current global assignment and pending handoff state.
    pub fn transfer_metadata_shard_primary(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
        expected_epoch: u64,
        new_primary_node_id: NodeId,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        let shard_key = metadata_shard_key(file_system_id, shard_id);
        let handoff_key = metadata_shard_membership_handoff_key(file_system_id, shard_id);
        if self.kv.get(&handoff_key)?.is_some() {
            return Ok(None);
        }
        let Some(shard_entry) = self.kv.get(&shard_key)? else {
            return Ok(None);
        };
        let current: DfsMetadataShardConfig = shard_entry.decode()?;
        if current.epoch != expected_epoch {
            return Ok(None);
        }
        if current.primary_node_id == new_primary_node_id {
            return Ok(Some(current));
        }
        reject_kv_only_metadata_shard_primary_transfer(&current, &new_primary_node_id)
    }

    #[cfg(test)]
    fn put_serde<T>(&mut self, key: &str, value: &T) -> Result<(), KvError>
    where
        T: Serialize,
    {
        self.kv
            .put(key.to_string(), sinomo_core::kv::encode_kv_value(value)?)?;
        Ok(())
    }

    fn get_serde<T>(&self, key: &str) -> Result<Option<T>, KvError>
    where
        T: DeserializeOwned,
    {
        let Some(entry) = self.kv.get(key)? else {
            return Ok(None);
        };
        entry.decode().map(Some)
    }
}

fn metadata_shard_authority_entries_are_current(
    expected: &DfsMetadataShardConfig,
    requested_keys: &[String],
    entries: Vec<Option<KvEntry>>,
) -> Result<bool, KvError> {
    if entries.len() != requested_keys.len() {
        return Err(KvError::Serialization {
            detail: format!(
                "consistent metadata-shard authority read returned {} entries for {} exact keys",
                entries.len(),
                requested_keys.len(),
            ),
        });
    }
    for (requested_key, entry) in requested_keys.iter().zip(&entries) {
        if entry
            .as_ref()
            .is_some_and(|entry| entry.key != *requested_key)
        {
            return Err(KvError::Serialization {
                detail: format!(
                    "consistent metadata-shard authority read returned an out-of-order entry for exact key {requested_key:?}"
                ),
            });
        }
    }
    let mut entries = entries.into_iter();
    let Some(entry) = entries.next().flatten() else {
        return Ok(false);
    };
    if entry.decode::<DfsMetadataShardConfig>()? != *expected {
        return Ok(false);
    }
    if entries.next().flatten().is_some() {
        return Ok(false);
    }
    Ok(true)
}

pub struct DfsControlPlaneAsyncStore<'a> {
    pub(crate) kv: &'a mut dyn AsyncDfsControlPlaneKvClient,
}

impl<'a> DfsControlPlaneAsyncStore<'a> {
    pub fn new(kv: &'a mut dyn AsyncDfsControlPlaneKvClient) -> Self {
        Self { kv }
    }

    pub(crate) async fn compare_and_put_placement_reconciliation_with_guards(
        &mut self,
        expected: &DfsPlacementReconciliationJob,
        desired: &DfsPlacementReconciliationJob,
        compares: Vec<KvCompare>,
    ) -> Result<bool, KvError> {
        validate_placement_reconciliation_progress(expected, desired)?;
        let key = placement_reconciliation_key(&expected.file_system_id, &expected.job_id);
        let Some(current_entry) = self.kv.get_async(&key).await? else {
            return Ok(false);
        };
        if current_entry.decode::<DfsPlacementReconciliationJob>()? != *expected {
            return Ok(false);
        }
        let queue_entry = self
            .kv
            .get_async(&placement_reconciliation_queue_key(&desired.file_system_id))
            .await?;
        let activity_index_entry = self
            .kv
            .get_async(&placement_reconciliation_activity_index_key(
                &desired.file_system_id,
            ))
            .await?;
        let mut txn = placement_reconciliation_progress_transaction(
            desired,
            current_entry,
            queue_entry,
            activity_index_entry,
            compares,
        )?;
        if desired.state == crate::DfsPlacementReconciliationState::Failed {
            txn.compares.push(
                self.placement_authority_permit_missing_compare(&desired.file_system_id)
                    .await?,
            );
        }
        Ok(self.kv.txn_async(txn).await?.succeeded)
    }

    pub async fn put_file_system_config(
        &mut self,
        config: &DfsFileSystemConfig,
    ) -> Result<(), KvError> {
        for _ in 0..FILE_SYSTEM_CONFIG_CAS_RETRIES {
            let current = self.get_file_system_config(&config.file_system_id).await?;
            let merged = merge_file_system_config_publication(current.as_ref(), config);
            if current.as_ref() == Some(&merged) {
                return Ok(());
            }
            if self
                .compare_and_put_file_system_config(current.as_ref(), &merged)
                .await?
            {
                return Ok(());
            }
        }
        Err(KvError::Busy {
            path: format!(
                "{} changed concurrently during generic filesystem publication; retry",
                file_system_config_key(&config.file_system_id)
            ),
        })
    }

    pub async fn ensure_file_system_config_defaults(
        &mut self,
        defaults: &DfsFileSystemConfig,
    ) -> Result<(), KvError> {
        for _ in 0..FILE_SYSTEM_CONFIG_CAS_RETRIES {
            let current = self
                .get_file_system_config(&defaults.file_system_id)
                .await?;
            let merged = merge_file_system_config_defaults(current.as_ref(), defaults);
            if current.as_ref() == Some(&merged) {
                return Ok(());
            }
            if self
                .compare_and_put_file_system_config(current.as_ref(), &merged)
                .await?
            {
                return Ok(());
            }
        }
        Err(KvError::Busy {
            path: format!(
                "{} changed concurrently while ensuring filesystem defaults; retry",
                file_system_config_key(&defaults.file_system_id)
            ),
        })
    }

    pub async fn get_file_system_config(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsFileSystemConfig>, KvError> {
        let key = file_system_config_key(file_system_id);
        let Some(entry) = self.kv.get_async(&key).await? else {
            return Ok(None);
        };
        entry.decode().map(Some)
    }

    pub async fn compare_and_put_file_system_config(
        &mut self,
        expected: Option<&DfsFileSystemConfig>,
        desired: &DfsFileSystemConfig,
    ) -> Result<bool, KvError> {
        if expected.is_some_and(|config| config.file_system_id != desired.file_system_id) {
            return Err(KvError::Serialization {
                detail: "filesystem config compare-and-put cannot change the filesystem id"
                    .to_string(),
            });
        }
        validate_ordinary_namespace_count_activation(expected, desired)?;
        let key = file_system_config_key(&desired.file_system_id);
        let current_entry = self.kv.get_async(&key).await?;
        let creating = expected.is_none() && current_entry.is_none();
        let config_compare = match (expected, current_entry) {
            (None, None) => KvCompare::Missing { key: key.clone() },
            (Some(expected), Some(entry)) => {
                if entry.decode::<DfsFileSystemConfig>()? != *expected {
                    return Ok(false);
                }
                KvCompare::ValueEquals {
                    key: key.clone(),
                    value: entry.value,
                }
            }
            _ => return Ok(false),
        };
        let permit_compare = self
            .placement_authority_permit_missing_compare(&desired.file_system_id)
            .await?;
        let mut compares = vec![config_compare, permit_compare];
        crate::validate_retention_policies(&desired.retention_policies)
            .map_err(|detail| KvError::Serialization { detail })?;
        crate::validate_module_metadata_map(&desired.module_metadata)
            .map_err(|detail| KvError::Serialization { detail })?;
        if !desired.retention_policies.is_empty() || !desired.module_metadata.is_empty() {
            let entries = self
                .kv
                .range_prefix_async(&format!(
                    "{}/",
                    metadata_shards_prefix(&desired.file_system_id)
                ))
                .await?;
            if entries.is_empty() {
                return Err(KvError::Serialization {
                    detail: "retention metadata requires configured current metadata shards".into(),
                });
            }
            for entry in entries {
                let shard: DfsMetadataShardConfig = entry.decode()?;
                if shard.file_system_id != desired.file_system_id {
                    return Err(KvError::Serialization { detail: "namespaced retention metadata requires current support on every assigned metadata shard".into() });
                }
                compares.push(KvCompare::ValueEquals {
                    key: entry.key,
                    value: entry.value,
                });
                compares.extend(
                    metadata_prepared_operation_admission_compares_async(self.kv, &shard).await?,
                );
            }
        }
        let mut success = vec![KvOp::put_serde(key, desired, None)?];
        if creating {
            let activity_key = placement_reconciliation_activity_index_key(&desired.file_system_id);
            if self.kv.get_async(&activity_key).await?.is_none() {
                compares.push(KvCompare::Missing {
                    key: activity_key.clone(),
                });
                success.push(KvOp::put_serde(
                    activity_key,
                    &DfsPlacementReconciliationActivityIndex::default(),
                    None,
                )?);
            }
        }
        Ok(self
            .kv
            .txn_async(KvTxn {
                compares,
                success,
                failure: Vec::new(),
            })
            .await?
            .succeeded)
    }

    /// sinomo:kv-consistency: Atomically publish the reviewed placement configuration and stable job, rebuilding bookkeeping only after a definite CAS rejection.
    pub async fn compare_and_put_placement_update(
        &mut self,
        expected: &DfsFileSystemConfig,
        desired: &DfsFileSystemConfig,
        job: &DfsPlacementReconciliationJob,
    ) -> Result<bool, KvError> {
        let publication = PlacementUpdatePublication::new(expected, desired, job)?;
        for attempt in 0..PLACEMENT_RECONCILIATION_QUEUE_CAS_RETRIES {
            let entries = if attempt == 0 {
                self.kv
                    .get_consistent_async(publication.keys.clone())
                    .await?
            } else {
                self.kv
                    .get_authoritative_consistent_async(publication.keys.clone())
                    .await?
            };
            let Some(inputs) = publication.inputs(entries)? else {
                return Ok(false);
            };
            let job_entries = match publication.queued_job_keys(&inputs)? {
                Some(keys) => {
                    let entries = if attempt == 0 || keys.is_empty() {
                        self.kv.get_consistent_async(keys).await?
                    } else {
                        self.kv.get_authoritative_consistent_async(keys).await?
                    };
                    entries.into_iter().flatten().collect()
                }
                None => Vec::new(),
            };
            let permit_compare = if inputs.permit.is_some() {
                self.placement_authority_permit_missing_compare(&job.file_system_id)
                    .await?
            } else {
                KvCompare::Missing {
                    key: publication.keys[5].clone(),
                }
            };
            let txn = publication.transaction(inputs, job_entries, permit_compare)?;
            if self.kv.txn_async(txn).await?.succeeded {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// sinomo:kv-consistency: Async exact-key recheck of a suspected live placement holder, preserving the same CAS and recovery bounds.
    async fn placement_authority_permit_missing_compare(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<KvCompare, KvError> {
        let key = placement_authority_permit_key(file_system_id);
        let deadline = metadata_shard_authority_read_deadline(None);
        for _ in 0..PLACEMENT_AUTHORITY_RECOVERY_CAS_RETRIES {
            let Some(mut entry) = self.kv.get_async(&key).await? else {
                return Ok(KvCompare::Missing { key });
            };
            let mut record = entry.decode::<DfsPlacementAuthorityPermitRecord>()?;
            if !placement_authority_release_is_pending(file_system_id, &record) {
                let entries = self
                    .kv
                    .get_authoritative_consistent_before_async(vec![key.clone()], deadline)
                    .await?;
                let Some(current) =
                    authoritative_placement_permit_entry(&key, entry.modify_revision, entries)?
                else {
                    return Ok(KvCompare::Missing { key });
                };
                (entry, record) = current;
                if !placement_authority_release_is_pending(file_system_id, &record) {
                    return Err(KvError::Busy {
                        path: placement_authority_busy_path(file_system_id, &record),
                    });
                }
            }
            let response = self
                .kv
                .txn_async(KvTxn {
                    compares: vec![KvCompare::ValueEquals {
                        key: key.clone(),
                        value: entry.value,
                    }],
                    success: vec![KvOp::Delete { key: key.clone() }],
                    failure: Vec::new(),
                })
                .await?;
            if response.succeeded {
                clear_pending_placement_authority_release(file_system_id, &record);
                return Ok(KvCompare::Missing { key });
            }
        }
        Err(KvError::Busy {
            path: format!(
                "{} changed repeatedly during exact pending-release recovery; retry",
                placement_authority_permit_key(file_system_id)
            ),
        })
    }

    pub async fn put_metadata_shard(
        &mut self,
        shard: &DfsMetadataShardConfig,
    ) -> Result<(), KvError> {
        let key = metadata_shard_key(&shard.file_system_id, &shard.shard_id);
        for _ in 0..METADATA_MEMBERSHIP_HANDOFF_RETRIES {
            let current_entry = self.kv.get_async(&key).await?;
            if let Some(entry) = current_entry.as_ref() {
                let current = entry.decode::<DfsMetadataShardConfig>()?;
                validate_raw_metadata_shard_write(&current, shard)?;
                if current == *shard {
                    return Ok(());
                }
            }
            let mut compares = vec![match current_entry.as_ref() {
                Some(entry) => KvCompare::ValueEquals {
                    key: key.clone(),
                    value: entry.value.clone(),
                },
                None => KvCompare::Missing { key: key.clone() },
            }];
            if current_entry.is_none() {
                compares.extend(
                    metadata_prepared_operation_admission_compares_async(self.kv, shard).await?,
                );
            }
            if self
                .kv
                .txn_async(KvTxn {
                    compares,
                    success: vec![KvOp::put_serde(key.clone(), shard, None)?],
                    failure: Vec::new(),
                })
                .await?
                .succeeded
            {
                return Ok(());
            }
        }
        Err(KvError::Busy { path: key })
    }

    /// Async create/idempotent compare-and-put. Changed assignments or primary
    /// roles require the explicit two-phase handoff API.
    pub async fn compare_and_put_metadata_shard(
        &mut self,
        expected: Option<&DfsMetadataShardConfig>,
        desired: &DfsMetadataShardConfig,
    ) -> Result<bool, KvError> {
        validate_metadata_shard_cas_identity(expected, desired)?;
        let key = metadata_shard_key(&desired.file_system_id, &desired.shard_id);
        let current_entry = self.kv.get_async(&key).await?;
        let shard_compare = match (expected, current_entry.as_ref()) {
            (None, None) => KvCompare::Missing { key: key.clone() },
            (Some(expected), Some(entry))
                if entry.decode::<DfsMetadataShardConfig>()? == *expected =>
            {
                KvCompare::ValueEquals {
                    key: key.clone(),
                    value: entry.value.clone(),
                }
            }
            _ => return Ok(false),
        };
        let mut compares = vec![shard_compare];
        if let Some(expected) = expected
            && expected != desired
        {
            validate_metadata_shard_cas_reconfiguration(expected, desired)?;
            return Ok(false);
        }
        if expected.is_none() {
            compares.extend([
                KvCompare::Missing {
                    key: metadata_shard_membership_handoff_key(
                        &desired.file_system_id,
                        &desired.shard_id,
                    ),
                },
                KvCompare::Missing {
                    key: metadata_shard_operation_floor_key(
                        &desired.file_system_id,
                        &desired.shard_id,
                    ),
                },
                KvCompare::Missing {
                    key: metadata_shard_head_watermark_key(
                        &desired.file_system_id,
                        &desired.shard_id,
                    ),
                },
            ]);
            compares.extend(
                metadata_prepared_operation_admission_compares_async(self.kv, desired).await?,
            );
        }
        Ok(self
            .kv
            .txn_async(KvTxn {
                compares,
                success: vec![KvOp::put_serde(key, desired, None)?],
                failure: Vec::new(),
            })
            .await?
            .succeeded)
    }

    pub async fn request_metadata_shard_primary_handoff(
        &mut self,
        retiring: &DfsMetadataShardConfig,
        new_primary: &NodeId,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        let desired = metadata_shard_primary_handoff_target(retiring, new_primary)?;
        self.begin_metadata_shard_membership_handoff(retiring, &desired)
            .await
    }

    pub async fn begin_metadata_shard_membership_handoff(
        &mut self,
        retiring: &DfsMetadataShardConfig,
        desired: &DfsMetadataShardConfig,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        self.begin_metadata_shard_handoff_with_cause(
            retiring,
            desired,
            DfsMetadataShardHandoffCause::Membership,
        )
        .await
    }

    pub async fn begin_metadata_shard_handoff_with_cause(
        &mut self,
        retiring: &DfsMetadataShardConfig,
        desired: &DfsMetadataShardConfig,
        cause: DfsMetadataShardHandoffCause,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        validate_metadata_shard_cas_identity(Some(retiring), desired)?;
        if retiring == desired {
            return Err(KvError::Serialization {
                detail: format!(
                    "metadata shard {} membership handoff does not change the assignment",
                    retiring.shard_id
                ),
            });
        }
        validate_metadata_shard_handoff_cause(retiring, desired, &cause)?;
        let shard_key = metadata_shard_key(&retiring.file_system_id, &retiring.shard_id);
        let floor_key =
            metadata_shard_operation_floor_key(&retiring.file_system_id, &retiring.shard_id);
        let watermark_key =
            metadata_shard_head_watermark_key(&retiring.file_system_id, &retiring.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&retiring.file_system_id, &retiring.shard_id);
        for _ in 0..METADATA_MEMBERSHIP_HANDOFF_RETRIES {
            let Some(shard_entry) = self.kv.get_async(&shard_key).await? else {
                return Ok(None);
            };
            if shard_entry.decode::<DfsMetadataShardConfig>()? != *retiring {
                return Ok(None);
            }
            if let Some(entry) = self.kv.get_async(&handoff_key).await? {
                let handoff = entry.decode::<DfsMetadataShardMembershipHandoff>()?;
                return Ok((handoff.retiring_shard == *retiring
                    && handoff.desired_shard == *desired
                    && handoff.cause == cause)
                    .then_some(handoff));
            }
            let write_set_entry = self
                .kv
                .get_async(&metadata_shard_write_set_key(
                    &retiring.file_system_id,
                    &retiring.shard_id,
                ))
                .await?;
            let (write_set_compare, _) = metadata_handoff_write_set_ops(
                retiring,
                desired,
                &cause,
                write_set_entry.as_ref(),
                None,
            )?;

            let floor_entry = self.kv.get_async(&floor_key).await?;
            let floor_sequence = match floor_entry.as_ref() {
                Some(entry) => {
                    let floor = entry.decode::<DfsMetadataShardOperationFloor>()?;
                    if floor.authority_epoch > retiring.epoch {
                        return Ok(None);
                    }
                    floor.prepared_sequence
                }
                None => 0,
            };
            let watermark_entry = self.kv.get_async(&watermark_key).await?;
            let required_head_watermark = watermark_entry
                .as_ref()
                .map(|entry| {
                    normalize_metadata_shard_head_watermark(
                        retiring,
                        &entry.decode::<DfsMetadataShardHeadWatermark>()?,
                    )
                })
                .transpose()?;
            let required_prepared_sequence = required_head_watermark
                .as_ref()
                .map(|watermark| watermark.journal_sequence)
                .unwrap_or(0)
                .max(floor_sequence);
            let handoff = DfsMetadataShardMembershipHandoff {
                cause: cause.clone(),
                handoff_id: match &cause {
                    DfsMetadataShardHandoffCause::Membership => sinomo_core::Id::generate(),
                    DfsMetadataShardHandoffCause::StorageImport { cut } => {
                        sinomo_core::Id::derived(
                            "dfs-storage-import-handoff-v7",
                            serde_json::to_vec(&(retiring, desired, cut)).map_err(|e| {
                                KvError::Serialization {
                                    detail: e.to_string(),
                                }
                            })?,
                        )
                    }
                }
                .to_string(),
                retiring_shard: retiring.clone(),
                desired_shard: desired.clone(),
                required_prepared_sequence,
                required_head_watermark,
                retiring_primary_fence: None,
            };
            let floor_compare = match floor_entry {
                Some(entry) => KvCompare::ValueEquals {
                    key: floor_key.clone(),
                    value: entry.value,
                },
                None => KvCompare::Missing {
                    key: floor_key.clone(),
                },
            };
            let watermark_compare = match watermark_entry {
                Some(entry) => KvCompare::ValueEquals {
                    key: watermark_key.clone(),
                    value: entry.value,
                },
                None => KvCompare::Missing {
                    key: watermark_key.clone(),
                },
            };
            let mut compares = vec![
                KvCompare::ValueEquals {
                    key: shard_key.clone(),
                    value: shard_entry.value,
                },
                floor_compare,
                watermark_compare,
                KvCompare::Missing {
                    key: handoff_key.clone(),
                },
                write_set_compare,
            ];
            compares.extend(
                metadata_prepared_operation_admission_compares_async(self.kv, desired).await?,
            );
            let response = self
                .kv
                .txn_async(KvTxn {
                    compares,
                    success: vec![KvOp::put_serde(handoff_key.clone(), &handoff, None)?],
                    failure: Vec::new(),
                })
                .await?;
            if response.succeeded {
                return Ok(Some(handoff));
            }
        }
        Ok(None)
    }

    pub async fn acknowledge_metadata_shard_membership_handoff_fence(
        &mut self,
        expected_handoff: &DfsMetadataShardMembershipHandoff,
        fence: &DfsMetadataShardMembershipHandoffFence,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        let retiring = &expected_handoff.retiring_shard;
        let shard_key = metadata_shard_key(&retiring.file_system_id, &retiring.shard_id);
        let floor_key =
            metadata_shard_operation_floor_key(&retiring.file_system_id, &retiring.shard_id);
        let watermark_key =
            metadata_shard_head_watermark_key(&retiring.file_system_id, &retiring.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&retiring.file_system_id, &retiring.shard_id);

        let Some(shard_entry) = self.kv.get_async(&shard_key).await? else {
            return Ok(None);
        };
        if shard_entry.decode::<DfsMetadataShardConfig>()? != *retiring {
            return Ok(None);
        }
        let Some(handoff_entry) = self.kv.get_async(&handoff_key).await? else {
            return Ok(None);
        };
        let handoff = handoff_entry.decode::<DfsMetadataShardMembershipHandoff>()?;
        if !metadata_shard_membership_handoff_identity_matches(&handoff, expected_handoff) {
            return Ok(None);
        }
        if let Some(existing) = handoff.retiring_primary_fence.as_ref() {
            return Ok((existing == fence).then_some(handoff));
        }
        let write_set_entry = self
            .kv
            .get_async(&metadata_shard_write_set_key(
                &retiring.file_system_id,
                &retiring.shard_id,
            ))
            .await?;
        if metadata_handoff_write_set_ops(
            retiring,
            &handoff.desired_shard,
            &handoff.cause,
            write_set_entry.as_ref(),
            None,
        )
        .is_err()
        {
            return Ok(None);
        }

        let floor_entry = self.kv.get_async(&floor_key).await?;
        let current_floor = floor_entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardOperationFloor>())
            .transpose()?;
        let fence = normalize_metadata_shard_membership_handoff_fence(
            &handoff,
            fence,
            current_floor.as_ref(),
        )?;
        let watermark_entry = self.kv.get_async(&watermark_key).await?;
        if !metadata_shard_membership_handoff_watermark_is_exact(
            &handoff,
            watermark_entry.as_ref(),
        )? {
            return Ok(None);
        }
        let floor_compare = match floor_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: floor_key,
                value: entry.value,
            },
            None => KvCompare::Missing { key: floor_key },
        };
        let watermark_compare = match watermark_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: watermark_key,
                value: entry.value,
            },
            None => KvCompare::Missing { key: watermark_key },
        };
        let mut acknowledged = handoff;
        acknowledged.retiring_primary_fence = Some(fence);
        Ok(self
            .kv
            .txn_async(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key: shard_key,
                        value: shard_entry.value,
                    },
                    KvCompare::ValueEquals {
                        key: handoff_key.clone(),
                        value: handoff_entry.value,
                    },
                    floor_compare,
                    watermark_compare,
                ],
                success: vec![KvOp::put_serde(handoff_key, &acknowledged, None)?],
                failure: Vec::new(),
            })
            .await?
            .succeeded
            .then_some(acknowledged))
    }

    pub async fn complete_metadata_shard_membership_handoff(
        &mut self,
        retiring: &DfsMetadataShardConfig,
        desired: &DfsMetadataShardConfig,
        proof: &DfsMetadataShardMembershipHandoffProof,
    ) -> Result<bool, KvError> {
        validate_metadata_shard_cas_identity(Some(retiring), desired)?;

        let shard_key = metadata_shard_key(&retiring.file_system_id, &retiring.shard_id);
        let floor_key =
            metadata_shard_operation_floor_key(&retiring.file_system_id, &retiring.shard_id);
        let watermark_key =
            metadata_shard_head_watermark_key(&retiring.file_system_id, &retiring.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&retiring.file_system_id, &retiring.shard_id);
        let proof_key = metadata_shard_membership_handoff_proof_key(
            &retiring.file_system_id,
            &retiring.shard_id,
        );

        let Some(shard_entry) = self.kv.get_async(&shard_key).await? else {
            return Ok(false);
        };
        if shard_entry.decode::<DfsMetadataShardConfig>()? != *retiring {
            return Ok(false);
        }
        let Some(handoff_entry) = self.kv.get_async(&handoff_key).await? else {
            return Ok(false);
        };
        let handoff = handoff_entry.decode::<DfsMetadataShardMembershipHandoff>()?;
        validate_metadata_shard_handoff_cause(retiring, desired, &handoff.cause)?;
        if handoff.retiring_shard != *retiring || handoff.desired_shard != *desired {
            return Ok(false);
        }
        let write_set_entry = self
            .kv
            .get_async(&metadata_shard_write_set_key(
                &retiring.file_system_id,
                &retiring.shard_id,
            ))
            .await?;
        let floor_entry = self.kv.get_async(&floor_key).await?;
        let current_floor = floor_entry
            .as_ref()
            .map(|entry| entry.decode::<DfsMetadataShardOperationFloor>())
            .transpose()?;
        let proof = normalize_metadata_shard_membership_handoff_proof(
            &handoff,
            proof,
            current_floor.as_ref(),
        )?;
        let activated_watermark =
            metadata_shard_membership_handoff_activated_watermark(&handoff, &proof)?;
        let (write_set_compare, write_set_put) = metadata_handoff_write_set_ops(
            retiring,
            desired,
            &handoff.cause,
            write_set_entry.as_ref(),
            Some(&proof),
        )?;
        let watermark_entry = self.kv.get_async(&watermark_key).await?;
        if !metadata_shard_membership_handoff_watermark_is_exact(
            &handoff,
            watermark_entry.as_ref(),
        )? {
            return Ok(false);
        }
        let floor_compare = match floor_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: floor_key,
                value: entry.value,
            },
            None => KvCompare::Missing { key: floor_key },
        };
        let watermark_compare = match watermark_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: watermark_key.clone(),
                value: entry.value,
            },
            None => KvCompare::Missing {
                key: watermark_key.clone(),
            },
        };
        let mut compares = vec![
            KvCompare::ValueEquals {
                key: shard_key.clone(),
                value: shard_entry.value,
            },
            KvCompare::ValueEquals {
                key: handoff_key.clone(),
                value: handoff_entry.value,
            },
            floor_compare,
            watermark_compare,
            write_set_compare,
        ];
        compares
            .extend(metadata_prepared_operation_admission_compares_async(self.kv, desired).await?);
        Ok(self
            .kv
            .txn_async(KvTxn {
                compares,
                success: vec![
                    KvOp::put_serde(shard_key, desired, None)?,
                    KvOp::put_serde(watermark_key, &activated_watermark, None)?,
                    KvOp::put_serde(proof_key, &proof, None)?,
                    KvOp::Delete { key: handoff_key },
                ]
                .into_iter()
                .chain(write_set_put)
                .collect(),
                failure: Vec::new(),
            })
            .await?
            .succeeded)
    }

    pub async fn get_metadata_shard_membership_handoff(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardMembershipHandoff>, KvError> {
        self.get_serde(&metadata_shard_membership_handoff_key(
            file_system_id,
            shard_id,
        ))
        .await
    }

    pub async fn get_metadata_shard_membership_handoff_proof(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardMembershipHandoffProof>, KvError> {
        self.get_serde(&metadata_shard_membership_handoff_proof_key(
            file_system_id,
            shard_id,
        ))
        .await
    }

    pub async fn get_metadata_shard(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        self.get_serde(&metadata_shard_key(file_system_id, shard_id))
            .await
    }

    pub async fn get_metadata_shard_head_watermark(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Option<DfsMetadataShardHeadWatermark>, KvError> {
        let key = metadata_shard_head_watermark_key(file_system_id, shard_id);
        let Some(entry) = self.kv.get_async(&key).await? else {
            return Ok(None);
        };
        entry.decode().map(Some)
    }

    /// Async exact-authority publication of a quorum-durable metadata head.
    pub async fn publish_metadata_shard_head_watermark(
        &mut self,
        expected_shard: &DfsMetadataShardConfig,
        watermark: &DfsMetadataShardHeadWatermark,
    ) -> Result<bool, KvError> {
        let watermark = normalize_metadata_shard_head_watermark(expected_shard, watermark)?;
        let shard_key =
            metadata_shard_key(&expected_shard.file_system_id, &expected_shard.shard_id);
        let watermark_key = metadata_shard_head_watermark_key(
            &expected_shard.file_system_id,
            &expected_shard.shard_id,
        );
        let handoff_key = metadata_shard_membership_handoff_key(
            &expected_shard.file_system_id,
            &expected_shard.shard_id,
        );

        for _ in 0..METADATA_HEAD_WATERMARK_PUBLISH_RETRIES {
            let Some(shard_entry) = self.kv.get_async(&shard_key).await? else {
                return Ok(false);
            };
            if shard_entry.decode::<DfsMetadataShardConfig>()? != *expected_shard {
                return Ok(false);
            }
            if self.kv.get_async(&handoff_key).await?.is_some() {
                return Ok(false);
            }

            let previous_entry = self.kv.get_async(&watermark_key).await?;
            let published = merged_metadata_shard_head_watermark(
                expected_shard,
                previous_entry.as_ref(),
                &watermark,
            )?;
            let compares = vec![
                KvCompare::ValueEquals {
                    key: shard_key.clone(),
                    value: shard_entry.value,
                },
                KvCompare::Missing {
                    key: handoff_key.clone(),
                },
                match previous_entry.as_ref() {
                    Some(entry) => KvCompare::ValueEquals {
                        key: watermark_key.clone(),
                        value: entry.value.clone(),
                    },
                    None => KvCompare::Missing {
                        key: watermark_key.clone(),
                    },
                },
            ];
            let barrier_value =
                sinomo_core::kv::encode_kv_value(&sinomo_core::Id::generate().to_string())?;
            let response = self
                .kv
                .txn_async(KvTxn {
                    compares,
                    success: vec![
                        KvOp::put_serde(watermark_key.clone(), &published, None)?,
                        KvOp::Put {
                            key: metadata_shard_authority_barrier_key(
                                &expected_shard.file_system_id,
                                &expected_shard.shard_id,
                            ),
                            value: barrier_value,
                            lease_id: None,
                        },
                    ],
                    failure: Vec::new(),
                })
                .await?;
            if response.succeeded {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn list_metadata_shards(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsMetadataShardConfig>, KvError> {
        let mut shards: Vec<DfsMetadataShardConfig> = self
            .kv
            .range_prefix_async(&format!("{}/", metadata_shards_prefix(file_system_id)))
            .await?
            .into_iter()
            .map(|entry| entry.decode())
            .collect::<Result<Vec<_>, _>>()?;
        shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        Ok(shards)
    }

    pub async fn select_metadata_shard_for_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        select_metadata_shard_config_for_path(
            file_system_id,
            &mut self.list_metadata_shards(file_system_id).await?,
            path,
        )
    }

    pub async fn select_metadata_shard_for_path_strict_default_sifs_split(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        select_metadata_shard_config_for_path_strict_default_sifs_split(
            file_system_id,
            &mut self.list_metadata_shards(file_system_id).await?,
            path,
        )
    }

    /// Async Raft-backed proof that `expected` remains the exact committed
    /// metadata-shard assignment.
    pub async fn certify_metadata_shard_authority(
        &mut self,
        expected: &DfsMetadataShardConfig,
    ) -> Result<bool, KvError> {
        let key = metadata_shard_key(&expected.file_system_id, &expected.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&expected.file_system_id, &expected.shard_id);
        let Some(entry) = self.kv.get_async(&key).await? else {
            return Ok(false);
        };
        if entry.decode::<DfsMetadataShardConfig>()? != *expected {
            return Ok(false);
        }
        if self.kv.get_async(&handoff_key).await?.is_some() {
            return Ok(false);
        }
        Ok(self
            .kv
            .txn_async(KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key,
                        value: entry.value,
                    },
                    KvCompare::Missing { key: handoff_key },
                ],
                success: vec![KvOp::Put {
                    key: metadata_shard_authority_barrier_key(
                        &expected.file_system_id,
                        &expected.shard_id,
                    ),
                    value: sinomo_core::kv::encode_kv_value(
                        &sinomo_core::Id::generate().to_string(),
                    )?,
                    lease_id: None,
                }],
                failure: Vec::new(),
            })
            .await?
            .succeeded)
    }

    /// Async configuration refresh and exact-assignment validation.
    pub async fn metadata_shard_authority_is_current(
        &mut self,
        expected: &DfsMetadataShardConfig,
    ) -> Result<bool, KvError> {
        self.metadata_shard_authority_is_current_before(expected, None)
            .await
    }

    /// Async deadline-preserving counterpart of
    /// [`DfsControlPlaneStore::metadata_shard_authority_is_current_before`].
    pub async fn metadata_shard_authority_is_current_before(
        &mut self,
        expected: &DfsMetadataShardConfig,
        operation_deadline: Option<Instant>,
    ) -> Result<bool, KvError> {
        let key = metadata_shard_key(&expected.file_system_id, &expected.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&expected.file_system_id, &expected.shard_id);
        let requested_keys = vec![key, handoff_key];
        let deadline = metadata_shard_authority_read_deadline(operation_deadline);
        for attempt in 0..METADATA_SHARD_AUTHORITY_READ_RETRIES {
            if Instant::now() >= deadline {
                return Err(metadata_shard_authority_deadline_error());
            }
            match self
                .kv
                .get_authoritative_consistent_before_async(requested_keys.clone(), deadline)
                .await
            {
                Ok(entries) => {
                    return metadata_shard_authority_entries_are_current(
                        expected,
                        &requested_keys,
                        entries,
                    );
                }
                Err(error)
                    if metadata_operation_authority_view_error_is_retryable(&error)
                        && attempt + 1 < METADATA_SHARD_AUTHORITY_READ_RETRIES =>
                {
                    // Match the synchronous operation-bound retry contract:
                    // every fresh exact-key-set attempt consumes the remaining
                    // portion of the same absolute deadline.
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("async metadata shard authority retry loop always returns")
    }

    /// Async counterpart of [`DfsControlPlaneStore::transfer_metadata_shard_primary`].
    /// Actual primary changes fail closed until a proof-bearing DFS data-plane
    /// handoff supplies the retiring-primary fence and promoted-replica proof.
    pub async fn transfer_metadata_shard_primary(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
        expected_epoch: u64,
        new_primary_node_id: NodeId,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        let shard_key = metadata_shard_key(file_system_id, shard_id);
        let handoff_key = metadata_shard_membership_handoff_key(file_system_id, shard_id);
        if self.kv.get_async(&handoff_key).await?.is_some() {
            return Ok(None);
        }
        let Some(shard_entry) = self.kv.get_async(&shard_key).await? else {
            return Ok(None);
        };
        let current: DfsMetadataShardConfig = shard_entry.decode()?;
        if current.epoch != expected_epoch {
            return Ok(None);
        }
        if current.primary_node_id == new_primary_node_id {
            return Ok(Some(current));
        }
        reject_kv_only_metadata_shard_primary_transfer(&current, &new_primary_node_id)
    }

    async fn get_serde<T>(&mut self, key: &str) -> Result<Option<T>, KvError>
    where
        T: DeserializeOwned,
    {
        let Some(entry) = self.kv.get_async(key).await? else {
            return Ok(None);
        };
        entry.decode().map(Some)
    }
}

fn validate_metadata_shard_cas_identity(
    expected: Option<&DfsMetadataShardConfig>,
    desired: &DfsMetadataShardConfig,
) -> Result<(), KvError> {
    if expected.is_some_and(|expected| {
        expected.file_system_id != desired.file_system_id || expected.shard_id != desired.shard_id
    }) {
        return Err(KvError::Serialization {
            detail: "metadata-shard compare-and-put cannot change shard identity".to_string(),
        });
    }
    Ok(())
}

/// Validate a proof-carrying metadata handoff configuration delta.
///
/// A handoff is either a same-primary membership or persisted-codec upgrade, or
/// a pure role rotation over the exact existing member ring. Assignment and
/// policy changes are separate administrative decisions. Mixing a primary move
/// with a membership change would make the promoted-prefix proof ambiguous, so
/// it is rejected.
///
/// Packed-v1 activation is intentionally one-way for now. A rollback also
/// needs a canonical snapshot/replay floor beyond every packed record before an
/// older binary may reopen the shard. The current handoff proof does not carry
/// that stronger evidence, so accepting a packed-to-legacy transition would
/// advertise a downgrade that is not actually restart-safe.
fn validate_metadata_shard_handoff_cause(
    retiring: &DfsMetadataShardConfig,
    desired: &DfsMetadataShardConfig,
    cause: &DfsMetadataShardHandoffCause,
) -> Result<(), KvError> {
    match cause {
        DfsMetadataShardHandoffCause::Membership => {
            validate_metadata_shard_cas_reconfiguration(retiring, desired)
        }
        DfsMetadataShardHandoffCause::StorageImport { cut } => {
            let mut next = retiring.clone();
            next.epoch = retiring
                .epoch
                .checked_add(1)
                .ok_or_else(|| KvError::Serialization {
                    detail: "storage import owner epoch is exhausted".into(),
                })?;
            let digest = |value: &str| {
                value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
            };
            if *desired != next
                || !digest(&cut.source_cut_digest)
                || !digest(&cut.pending_mapping_digest)
                || cut.source_state_fingerprint.is_empty()
                || cut.converted_image_fingerprint.is_empty()
                || cut.target_prefix.version != 2
                || !cut.target_prefix.is_well_formed()
                || cut.target_prefix.journal_sequence < cut.source_journal_sequence
            {
                return Err(KvError::Serialization { detail: "storage import requires an exact next epoch, unchanged assignment and complete source/image/pending cut".into() });
            }
            Ok(())
        }
    }
}

fn validate_metadata_shard_cas_reconfiguration(
    expected: &DfsMetadataShardConfig,
    desired: &DfsMetadataShardConfig,
) -> Result<(), KvError> {
    if expected.assignment != desired.assignment
        || expected.default_policy != desired.default_policy
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff cannot change assignment or default policy",
                expected.shard_id
            ),
        });
    }
    let Some(next_epoch) = expected.epoch.checked_add(1) else {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} authority epoch is exhausted",
                expected.shard_id
            ),
        });
    };
    if desired.epoch != next_epoch {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} reconfiguration must advance epoch {} to {}",
                expected.shard_id, expected.epoch, next_epoch
            ),
        });
    }

    if expected.primary_node_id == desired.primary_node_id {
        let membership_changed = metadata_shard_configured_node_ids(expected)
            != metadata_shard_configured_node_ids(desired);
        if !membership_changed {
            return Err(KvError::Serialization {
                detail: format!(
                    "metadata shard {} handoff must change the configured replica set",
                    expected.shard_id
                ),
            });
        }
        return Ok(());
    }

    let Some(expected_replicas) =
        topology_preserving_metadata_shard_replicas(expected, &desired.primary_node_id)
    else {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} primary handoff target {} must be an existing replica",
                expected.shard_id, desired.primary_node_id
            ),
        });
    };
    if desired.replica_node_ids != expected_replicas {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} primary handoff must preserve the exact configured member ring; mixed primary and membership changes are not allowed",
                expected.shard_id
            ),
        });
    }
    Ok(())
}

fn validate_raw_metadata_shard_write(
    current: &DfsMetadataShardConfig,
    desired: &DfsMetadataShardConfig,
) -> Result<(), KvError> {
    if current != desired {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} reconfiguration requires its native membership handoff",
                current.shard_id
            ),
        });
    }
    Ok(())
}

#[tracing::instrument(
    target = "sinomo.perf",
    level = "trace",
    name = "dfs.metadata.codec_admission",
    skip_all,
    fields(stage = "dfs.metadata.codec_admission")
)]
fn metadata_prepared_operation_admission_compares(
    kv: &(impl DfsControlPlaneKvClient + ?Sized),
    desired: &DfsMetadataShardConfig,
) -> Result<Vec<KvCompare>, KvError> {
    let nodes = metadata_shard_configured_node_ids(desired);
    let keys = nodes
        .iter()
        .flat_map(|node| {
            [
                cluster_member_key(node),
                cluster_capability_policy_key(node),
            ]
        })
        .collect::<Vec<_>>();
    let trace_operation = if crate::service_trace::dfs_perf_trace_enabled() {
        format!(
            "codec:{}:{}:{}:{}",
            desired.file_system_id, desired.shard_id, desired.epoch, desired.primary_node_id
        )
    } else {
        String::new()
    };
    let mut timing = crate::service_trace::DfsPhaseTimer::new(&trace_operation);
    let entries = tracing::trace_span!(target: "sinomo.perf", "dfs.metadata.codec_read",
        stage = "dfs.metadata.codec_read", phase = "members-and-policies")
    .in_scope(|| kv.get_consistent(&keys))?;
    timing.mark("codec-member-policy-read-returned");
    if entries.len() != keys.len() {
        return Err(KvError::Serialization {
            detail: "metadata participant read set changed its exact key count".into(),
        });
    }
    let mut cluster_id = None;
    let mut compares = Vec::new();
    for (index, node_id) in nodes.into_iter().enumerate() {
        let _member = tracing::trace_span!(target: "sinomo.perf", "dfs.metadata.codec_member",
            stage = "dfs.metadata.codec_member", peer_node_id = %node_id)
        .entered();
        let member_key = keys[2 * index].clone();
        let policy_key = keys[2 * index + 1].clone();
        let member_entry = entries[2 * index].clone().ok_or_else(|| KvError::Serialization {
            detail: format!(
                "metadata shard {} cannot activate {} namespace prepares: assigned node {} has no authoritative cluster member capability record",
                desired.shard_id, "prepared-v7", node_id,
            ),
        })?;
        let policy_entry = entries[2 * index + 1].clone();
        if member_entry.key != member_key
            || policy_entry
                .as_ref()
                .is_some_and(|entry| entry.key != policy_key)
        {
            return Err(KvError::Serialization {
                detail: "metadata participant read set changed its exact key scope".into(),
            });
        }
        let _validation =
            tracing::trace_span!(target: "sinomo.perf", "dfs.metadata.codec_validate",
            stage = "dfs.metadata.codec_validate")
            .entered();
        compares.extend(metadata_prepared_operation_member_compares(
            desired,
            &node_id,
            member_key,
            member_entry,
            policy_key,
            policy_entry,
            &mut cluster_id,
            DFS_METADATA_PREPARED_OPERATION_V7_CAPABILITY,
        )?);
    }
    timing.mark("codec-member-policy-validation-returned");
    Ok(compares)
}

async fn metadata_prepared_operation_admission_compares_async(
    kv: &mut (impl AsyncDfsControlPlaneKvClient + ?Sized),
    desired: &DfsMetadataShardConfig,
) -> Result<Vec<KvCompare>, KvError> {
    let nodes = metadata_shard_configured_node_ids(desired);
    let keys = nodes
        .iter()
        .flat_map(|node| {
            [
                cluster_member_key(node),
                cluster_capability_policy_key(node),
            ]
        })
        .collect::<Vec<_>>();
    let trace_operation = if crate::service_trace::dfs_perf_trace_enabled() {
        format!(
            "codec:{}:{}:{}:{}",
            desired.file_system_id, desired.shard_id, desired.epoch, desired.primary_node_id
        )
    } else {
        String::new()
    };
    let mut timing = crate::service_trace::DfsPhaseTimer::new(&trace_operation);
    let entries = tracing::Instrument::instrument(
        kv.get_consistent_async(keys.clone()),
        tracing::trace_span!(target: "sinomo.perf", "dfs.metadata.codec_read",
            stage = "dfs.metadata.codec_read", phase = "members-and-policies"),
    )
    .await?;
    timing.mark("codec-member-policy-read-returned");
    if entries.len() != keys.len() {
        return Err(KvError::Serialization {
            detail: "metadata participant read set changed its exact key count".into(),
        });
    }
    let mut cluster_id = None;
    let mut compares = Vec::new();
    for (index, node_id) in nodes.into_iter().enumerate() {
        let member_key = keys[2 * index].clone();
        let policy_key = keys[2 * index + 1].clone();
        let member_entry = entries[2 * index].clone().ok_or_else(|| KvError::Serialization {
            detail: format!(
                "metadata shard {} cannot activate {} namespace prepares: assigned node {} has no authoritative cluster member capability record",
                desired.shard_id, "prepared-v7", node_id,
            ),
        })?;
        let policy_entry = entries[2 * index + 1].clone();
        if member_entry.key != member_key
            || policy_entry
                .as_ref()
                .is_some_and(|entry| entry.key != policy_key)
        {
            return Err(KvError::Serialization {
                detail: "metadata participant read set changed its exact key scope".into(),
            });
        }
        compares.extend(metadata_prepared_operation_member_compares(
            desired,
            &node_id,
            member_key,
            member_entry,
            policy_key,
            policy_entry,
            &mut cluster_id,
            DFS_METADATA_PREPARED_OPERATION_V7_CAPABILITY,
        )?);
    }
    timing.mark("codec-member-policy-validation-returned");
    Ok(compares)
}

fn metadata_prepared_operation_member_compares(
    desired: &DfsMetadataShardConfig,
    node_id: &NodeId,
    member_key: String,
    member_entry: KvEntry,
    policy_key: String,
    policy_entry: Option<KvEntry>,
    cluster_id: &mut Option<sinomo_core::Id>,
    required_capability: &str,
) -> Result<Vec<KvCompare>, KvError> {
    let member = member_entry.decode::<ClusterMemberRecord>()?;
    if member.node_id != *node_id || !member.state.is_authorized() {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} cannot activate {} namespace prepares: assigned node {} is not an authorized cluster member",
                desired.shard_id, "prepared-v7", node_id,
            ),
        });
    }
    let policy = policy_entry
        .as_ref()
        .map(|entry| entry.decode::<ClusterCapabilityPolicyRecord>())
        .transpose()?;
    if policy
        .as_ref()
        .is_some_and(|policy| policy.node_id != *node_id || policy.cluster_id != member.cluster_id)
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} cannot activate {} namespace prepares: assigned node {} has an inconsistent authoritative capability policy",
                desired.shard_id, "prepared-v7", node_id,
            ),
        });
    }
    let effective_capabilities = policy
        .as_ref()
        .map(|policy| policy.capabilities.as_slice())
        .unwrap_or(member.capabilities.as_slice());
    if !effective_capabilities
        .iter()
        .any(|capability| capability.as_str() == required_capability)
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} cannot activate {} namespace prepares: assigned node {} effective capability policy does not advertise {}",
                desired.shard_id, "prepared-v7", node_id, required_capability,
            ),
        });
    }
    // A capability policy grants permission; it cannot make an older assigned
    // binary understand the maintained-count publication contract.
    let count_capability = sinomo_core::node::DFS_METADATA_NAMESPACE_COUNTS_V1_CAPABILITY;
    if !member.capabilities.iter().any(|capability| capability.as_str() == count_capability)
        || !effective_capabilities.iter().any(|capability| capability.as_str() == count_capability)
    {
        return Err(KvError::Serialization {
            detail: format!("metadata shard {} assigned node {} does not advertise and permit {}",
                desired.shard_id, node_id, count_capability),
        });
    }
    if cluster_id
        .as_ref()
        .is_some_and(|cluster_id| *cluster_id != member.cluster_id)
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} cannot activate {} namespace prepares across inconsistent cluster member records",
                desired.shard_id, "prepared-v7",
            ),
        });
    }
    *cluster_id = Some(member.cluster_id);
    Ok(vec![
        KvCompare::ValueEquals {
            key: member_key,
            value: member_entry.value,
        },
        match policy_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: policy_key,
                value: entry.value,
            },
            None => KvCompare::Missing { key: policy_key },
        },
    ])
}

fn metadata_shard_configured_node_ids(shard: &DfsMetadataShardConfig) -> BTreeSet<NodeId> {
    std::iter::once(shard.primary_node_id.clone())
        .chain(shard.replica_node_ids.iter().cloned())
        .collect()
}

/// Whether exact-prefix acknowledgements form a safe membership-handoff
/// certificate.
///
/// Requiring a majority of both assignments is the DFS equivalent of joint
/// consensus: the retiring generation cannot be skipped, while the desired
/// generation is durable before activation. Requiring both primaries closes
/// the authority-transfer edges explicitly. In particular, a primary rotation
/// cannot activate without the promoted node's handoff-bound marker, and an
/// isolated retiring primary can never be inferred away by a quorum-side
/// reconfiguration. Reconfiguration validation separately limits each handoff
/// to either a same-primary membership change or a same-ring primary rotation;
/// mixed primary and membership changes never reach this predicate.
pub(crate) fn metadata_shard_membership_handoff_acknowledgements_are_safe(
    retiring: &DfsMetadataShardConfig,
    desired: &DfsMetadataShardConfig,
    acknowledged_node_ids: &BTreeSet<NodeId>,
) -> bool {
    let retiring_node_ids = metadata_shard_configured_node_ids(retiring);
    let desired_node_ids = metadata_shard_configured_node_ids(desired);
    let configured_union = retiring_node_ids
        .union(&desired_node_ids)
        .cloned()
        .collect::<BTreeSet<_>>();
    let retiring_quorum = retiring_node_ids.len() / 2 + 1;
    let desired_quorum = desired_node_ids.len() / 2 + 1;

    acknowledged_node_ids.is_subset(&configured_union)
        && acknowledged_node_ids.contains(&retiring.primary_node_id)
        && acknowledged_node_ids.contains(&desired.primary_node_id)
        && acknowledged_node_ids
            .intersection(&retiring_node_ids)
            .count()
            >= retiring_quorum
        && acknowledged_node_ids
            .intersection(&desired_node_ids)
            .count()
            >= desired_quorum
}

fn metadata_shard_membership_handoff_watermark_is_exact(
    handoff: &DfsMetadataShardMembershipHandoff,
    current_entry: Option<&KvEntry>,
) -> Result<bool, KvError> {
    match (&handoff.required_head_watermark, current_entry) {
        (None, None) => Ok(true),
        (Some(required), Some(entry)) => Ok(normalize_metadata_shard_head_watermark(
            &handoff.retiring_shard,
            &entry.decode::<DfsMetadataShardHeadWatermark>()?,
        )? == *required),
        _ => Ok(false),
    }
}

fn metadata_shard_membership_handoff_activated_watermark(
    handoff: &DfsMetadataShardMembershipHandoff,
    proof: &DfsMetadataShardMembershipHandoffProof,
) -> Result<DfsMetadataShardHeadWatermark, KvError> {
    let desired_node_ids = metadata_shard_configured_node_ids(&handoff.desired_shard);
    normalize_metadata_shard_head_watermark(
        &handoff.desired_shard,
        &DfsMetadataShardHeadWatermark {
            file_system_id: handoff.desired_shard.file_system_id.clone(),
            shard_id: handoff.desired_shard.shard_id.clone(),
            primary_node_id: handoff.desired_shard.primary_node_id.clone(),
            epoch: handoff.desired_shard.epoch,
            journal_sequence: proof.journal_sequence,
            state_fingerprint: proof.state_fingerprint.clone(),
            acknowledged_node_ids: proof
                .acknowledged_node_ids
                .iter()
                .filter(|node_id| desired_node_ids.contains(*node_id))
                .cloned()
                .collect(),
        },
    )
}

fn metadata_shard_membership_handoff_identity_matches(
    current: &DfsMetadataShardMembershipHandoff,
    expected: &DfsMetadataShardMembershipHandoff,
) -> bool {
    current.handoff_id == expected.handoff_id
        && current.retiring_shard == expected.retiring_shard
        && current.desired_shard == expected.desired_shard
        && current.required_prepared_sequence == expected.required_prepared_sequence
        && current.required_head_watermark == expected.required_head_watermark
}

fn normalize_metadata_shard_membership_handoff_fence(
    handoff: &DfsMetadataShardMembershipHandoff,
    fence: &DfsMetadataShardMembershipHandoffFence,
    current_floor: Option<&DfsMetadataShardOperationFloor>,
) -> Result<DfsMetadataShardMembershipHandoffFence, KvError> {
    if fence.handoff_id != handoff.handoff_id
        || fence.primary_node_id != handoff.retiring_shard.primary_node_id
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} retiring-primary fence is not bound to handoff {} and primary {}",
                handoff.retiring_shard.shard_id,
                handoff.handoff_id,
                handoff.retiring_shard.primary_node_id
            ),
        });
    }
    if fence.state_fingerprint.is_empty() {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} retiring-primary fence has no exact state fingerprint",
                handoff.retiring_shard.shard_id
            ),
        });
    }
    if current_floor.is_some_and(|floor| floor.authority_epoch > handoff.retiring_shard.epoch) {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} retiring-primary fence was superseded by a newer authority epoch",
                handoff.retiring_shard.shard_id
            ),
        });
    }
    let required_sequence = handoff.required_prepared_sequence.max(
        current_floor
            .map(|floor| floor.prepared_sequence)
            .unwrap_or(0),
    );
    if fence.journal_sequence < required_sequence {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} retiring-primary fence sequence {} is below required prepared floor {}",
                handoff.retiring_shard.shard_id, fence.journal_sequence, required_sequence
            ),
        });
    }
    if let Some(required_head) = handoff.required_head_watermark.as_ref()
        && fence.journal_sequence == required_head.journal_sequence
        && fence.state_fingerprint != required_head.state_fingerprint
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} retiring-primary fence at sequence {} does not match its required exact head",
                handoff.retiring_shard.shard_id, fence.journal_sequence
            ),
        });
    }
    Ok(fence.clone())
}

fn normalize_metadata_shard_membership_handoff_proof(
    handoff: &DfsMetadataShardMembershipHandoff,
    proof: &DfsMetadataShardMembershipHandoffProof,
    current_floor: Option<&DfsMetadataShardOperationFloor>,
) -> Result<DfsMetadataShardMembershipHandoffProof, KvError> {
    if handoff.handoff_id.is_empty()
        || proof.handoff_id != handoff.handoff_id
        || proof.cause != handoff.cause
        || proof.retiring_shard != handoff.retiring_shard
        || proof.desired_shard != handoff.desired_shard
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff proof is not bound to the pending retiring and desired assignments",
                handoff.retiring_shard.shard_id
            ),
        });
    }
    if proof.state_fingerprint.is_empty() {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff proof has no exact state fingerprint",
                handoff.retiring_shard.shard_id
            ),
        });
    }
    let Some(retiring_fence) = handoff.retiring_primary_fence.as_ref() else {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff has no retiring-primary fence acknowledgement",
                handoff.retiring_shard.shard_id
            ),
        });
    };
    let expected_target = match &handoff.cause {
        DfsMetadataShardHandoffCause::Membership => (
            retiring_fence.journal_sequence,
            retiring_fence.state_fingerprint.as_str(),
        ),
        DfsMetadataShardHandoffCause::StorageImport { cut } => {
            if retiring_fence.journal_sequence != cut.source_journal_sequence
                || retiring_fence.state_fingerprint != cut.source_state_fingerprint
            {
                return Err(KvError::Serialization {
                    detail: "storage import retiring fence differs from its captured source cut"
                        .into(),
                });
            }
            (
                cut.target_prefix.journal_sequence,
                cut.converted_image_fingerprint.as_str(),
            )
        }
    };
    if proof.journal_sequence != expected_target.0 || proof.state_fingerprint != expected_target.1 {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff proof does not carry the exact retiring-primary fenced head",
                handoff.retiring_shard.shard_id
            ),
        });
    }
    if current_floor.is_some_and(|floor| floor.authority_epoch > handoff.retiring_shard.epoch) {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff was superseded by authority epoch {}",
                handoff.retiring_shard.shard_id,
                current_floor
                    .expect("checked current floor")
                    .authority_epoch
            ),
        });
    }
    let required_prepared_sequence = current_floor
        .map(|floor| floor.prepared_sequence)
        .unwrap_or(0)
        .max(handoff.required_prepared_sequence);
    if proof.journal_sequence < required_prepared_sequence {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff proof sequence {} is below required prepared floor {}",
                handoff.retiring_shard.shard_id, proof.journal_sequence, required_prepared_sequence
            ),
        });
    }
    if matches!(handoff.cause, DfsMetadataShardHandoffCause::Membership)
        && let Some(required_head_watermark) = handoff.required_head_watermark.as_ref()
        && proof.journal_sequence == required_head_watermark.journal_sequence
        && proof.state_fingerprint != required_head_watermark.state_fingerprint
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff proof at retiring head sequence {} does not match its exact state fingerprint",
                handoff.retiring_shard.shard_id, proof.journal_sequence
            ),
        });
    }
    let retiring_node_ids = metadata_shard_configured_node_ids(&handoff.retiring_shard);
    let desired_node_ids = metadata_shard_configured_node_ids(&handoff.desired_shard);
    let configured_node_ids = retiring_node_ids
        .union(&desired_node_ids)
        .cloned()
        .collect::<BTreeSet<_>>();
    let acknowledged_node_ids = proof
        .acknowledged_node_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if !metadata_shard_membership_handoff_acknowledgements_are_safe(
        &handoff.retiring_shard,
        &handoff.desired_shard,
        &acknowledged_node_ids,
    ) {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} membership handoff proof acknowledges {:?}; safe activation requires both primaries and a majority of both retiring {:?} and desired {:?} assignments (configured union {:?})",
                handoff.retiring_shard.shard_id,
                acknowledged_node_ids,
                retiring_node_ids,
                desired_node_ids,
                configured_node_ids,
            ),
        });
    }

    Ok(DfsMetadataShardMembershipHandoffProof {
        cause: proof.cause.clone(),
        handoff_id: proof.handoff_id.clone(),
        retiring_shard: proof.retiring_shard.clone(),
        desired_shard: proof.desired_shard.clone(),
        journal_sequence: proof.journal_sequence,
        state_fingerprint: proof.state_fingerprint.clone(),
        acknowledged_node_ids: acknowledged_node_ids.into_iter().collect(),
    })
}

/// Rotate the existing ordered metadata membership around `new_primary` while
/// preserving exactly the same unique configured nodes.
///
/// Keeping the old primary in the replica set is load-bearing: merely replacing
/// `primary_node_id` leaves the new primary duplicated in `replica_node_ids` and
/// silently shrinks the effective quorum after replica normalization.
pub(crate) fn topology_preserving_metadata_shard_replicas(
    current: &DfsMetadataShardConfig,
    new_primary: &NodeId,
) -> Option<Vec<NodeId>> {
    let mut members = vec![current.primary_node_id.clone()];
    for node_id in &current.replica_node_ids {
        if !members.contains(node_id) {
            members.push(node_id.clone());
        }
    }
    let primary_index = members.iter().position(|node_id| node_id == new_primary)?;
    if members.len() < 2 {
        return None;
    }
    Some(
        (1..members.len())
            .map(|offset| members[(primary_index + offset) % members.len()].clone())
            .collect(),
    )
}

fn metadata_shard_primary_handoff_target(
    retiring: &DfsMetadataShardConfig,
    new_primary: &NodeId,
) -> Result<DfsMetadataShardConfig, KvError> {
    if &retiring.primary_node_id == new_primary {
        return Err(KvError::Serialization {
            detail: "handoff target is already primary".into(),
        });
    }
    let replica_node_ids = topology_preserving_metadata_shard_replicas(retiring, new_primary)
        .ok_or_else(|| KvError::Serialization {
            detail: "handoff target must be an existing metadata replica".into(),
        })?;
    let epoch = retiring
        .epoch
        .checked_add(1)
        .ok_or_else(|| KvError::Serialization {
            detail: format!("metadata shard {} epoch exhausted", retiring.shard_id),
        })?;
    Ok(DfsMetadataShardConfig {
        primary_node_id: new_primary.clone(),
        replica_node_ids,
        epoch,
        ..retiring.clone()
    })
}

/// Reject a primary change that has only control-plane state available.
///
/// Healthy full-shard writes intentionally do not publish their metadata head
/// to cluster KV. Consequently even a valid, quorum-acknowledged KV watermark
/// can lag the retiring primary's durable DFS journal and cannot safely fence a
/// promotion. Primary movement must be completed by a data-plane handoff that
/// freezes the retiring primary under its publication guard and proves the
/// exact resulting prefix on the promoted replica.
fn reject_kv_only_metadata_shard_primary_transfer(
    current: &DfsMetadataShardConfig,
    new_primary: &NodeId,
) -> Result<Option<DfsMetadataShardConfig>, KvError> {
    if topology_preserving_metadata_shard_replicas(current, new_primary).is_none() {
        return Ok(None);
    }
    if current.epoch == u64::MAX {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} authority epoch is exhausted",
                current.shard_id
            ),
        });
    }
    Err(KvError::Serialization {
        detail: format!(
            "metadata shard {} primary transfer from {} to {} requires a current operation-bound DFS prefix fence and exact promoted-replica acknowledgement; a cluster-KV head watermark cannot prove the latest healthy full-shard write",
            current.shard_id, current.primary_node_id, new_primary
        ),
    })
}

fn normalize_metadata_shard_head_watermark(
    expected_shard: &DfsMetadataShardConfig,
    watermark: &DfsMetadataShardHeadWatermark,
) -> Result<DfsMetadataShardHeadWatermark, KvError> {
    if watermark.file_system_id != expected_shard.file_system_id
        || watermark.shard_id != expected_shard.shard_id
        || watermark.primary_node_id != expected_shard.primary_node_id
        || watermark.epoch != expected_shard.epoch
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata-head watermark authority does not match shard {} epoch {}",
                expected_shard.shard_id, expected_shard.epoch
            ),
        });
    }

    let configured_node_ids = std::iter::once(expected_shard.primary_node_id.clone())
        .chain(expected_shard.replica_node_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    let acknowledged_node_ids = watermark
        .acknowledged_node_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if !acknowledged_node_ids.contains(&expected_shard.primary_node_id) {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata-head watermark for shard {} does not acknowledge primary {}",
                expected_shard.shard_id, expected_shard.primary_node_id
            ),
        });
    }
    if let Some(unconfigured) = acknowledged_node_ids
        .iter()
        .find(|node_id| !configured_node_ids.contains(*node_id))
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata-head watermark for shard {} acknowledges unconfigured node {}",
                expected_shard.shard_id, unconfigured
            ),
        });
    }
    let quorum_size = configured_node_ids.len() / 2 + 1;
    if acknowledged_node_ids.len() < quorum_size {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata-head watermark for shard {} has {} acknowledgements; quorum is {}",
                expected_shard.shard_id,
                acknowledged_node_ids.len(),
                quorum_size
            ),
        });
    }
    if watermark.state_fingerprint.is_empty() {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata-head watermark for shard {} has no exact state fingerprint",
                expected_shard.shard_id
            ),
        });
    }

    Ok(DfsMetadataShardHeadWatermark {
        file_system_id: watermark.file_system_id.clone(),
        shard_id: watermark.shard_id.clone(),
        primary_node_id: watermark.primary_node_id.clone(),
        epoch: watermark.epoch,
        journal_sequence: watermark.journal_sequence,
        state_fingerprint: watermark.state_fingerprint.clone(),
        acknowledged_node_ids: acknowledged_node_ids.into_iter().collect(),
    })
}

fn merged_metadata_shard_head_watermark(
    expected_shard: &DfsMetadataShardConfig,
    previous_entry: Option<&KvEntry>,
    candidate: &DfsMetadataShardHeadWatermark,
) -> Result<DfsMetadataShardHeadWatermark, KvError> {
    let Some(previous_entry) = previous_entry else {
        return Ok(candidate.clone());
    };
    let previous = previous_entry.decode::<DfsMetadataShardHeadWatermark>()?;
    if previous.file_system_id != candidate.file_system_id
        || previous.shard_id != candidate.shard_id
        || previous.primary_node_id != candidate.primary_node_id
        || previous.epoch != candidate.epoch
    {
        return Ok(candidate.clone());
    }
    let Ok(mut previous) = normalize_metadata_shard_head_watermark(expected_shard, &previous)
    else {
        return Ok(candidate.clone());
    };

    match previous.journal_sequence.cmp(&candidate.journal_sequence) {
        std::cmp::Ordering::Greater => Ok(previous),
        std::cmp::Ordering::Less => Ok(candidate.clone()),
        std::cmp::Ordering::Equal => {
            if previous.state_fingerprint != candidate.state_fingerprint {
                // The candidate itself already carries a complete fresh
                // quorum. Never union acknowledgements for different states
                // that happen to share a journal sequence.
                return Ok(candidate.clone());
            }
            previous
                .acknowledged_node_ids
                .extend(candidate.acknowledged_node_ids.iter().cloned());
            normalize_metadata_shard_head_watermark(expected_shard, &previous)
        }
    }
}

fn normalize_segment(segment: &str) -> String {
    segment.trim_matches('/').replace('/', "_")
}

pub fn select_metadata_shard_config_for_path(
    file_system_id: &DfsFileSystemId,
    shards: &[DfsMetadataShardConfig],
    path: &str,
) -> Result<Option<DfsMetadataShardConfig>, KvError> {
    select_metadata_shard_config_for_path_with_options(file_system_id, shards, path, false)
}

/// Select one metadata shard from an already-authoritative catalog while
/// rejecting a partial default SIFS split.
///
/// This pure selector is useful to validate an immutable catalog snapshot
/// without re-inserting its shard records into another KV store. Re-insertion
/// would repeat activation validation and would also require copying unrelated
/// authority evidence, such as member capability records.
pub fn select_metadata_shard_config_for_path_strict_default_sifs_split(
    file_system_id: &DfsFileSystemId,
    shards: &[DfsMetadataShardConfig],
    path: &str,
) -> Result<Option<DfsMetadataShardConfig>, KvError> {
    select_metadata_shard_config_for_path_with_options(file_system_id, shards, path, true)
}

/// Immutable path-routing index for one metadata topology generation.
///
/// Control-plane helpers above intentionally accept arbitrary slices and are
/// convenient for one-off validation. Long-lived metadata backends should
/// compile the catalog once: prefix routing then costs O(path depth), while
/// hash-range routing is a binary search over disjoint winner intervals.
#[derive(Clone, Debug)]
pub(crate) struct DfsMetadataShardRoutingIndex {
    file_system_id: DfsFileSystemId,
    root_shard_id: Option<String>,
    prefix_root: MetadataPathPrefixNode,
    hash_intervals: Vec<MetadataHashRoutingInterval>,
    saw_default_sifs_split_shard: bool,
}

#[derive(Clone, Debug, Default)]
struct MetadataPathPrefixNode {
    shard_id: Option<String>,
    children: BTreeMap<String, MetadataPathPrefixNode>,
}

#[derive(Clone, Debug)]
struct MetadataHashRoutingInterval {
    start_inclusive: u128,
    end_exclusive: u128,
    shard_id: String,
}

#[derive(Clone, Debug)]
struct MetadataHashRoutingCandidate {
    start_inclusive: u128,
    end_exclusive: u128,
    width: u128,
    shard_id: String,
}

impl DfsMetadataShardRoutingIndex {
    pub(crate) fn new(
        file_system_id: DfsFileSystemId,
        shards: &[DfsMetadataShardConfig],
    ) -> Result<Self, KvError> {
        let mut root_shard_id = None::<String>;
        let mut prefix_root = MetadataPathPrefixNode::default();
        let mut hash_candidates = Vec::new();
        let mut saw_default_sifs_split_shard = false;
        let hash_space_end = u128::from(u64::MAX) + 1;

        for shard in shards
            .iter()
            .filter(|shard| shard.file_system_id == file_system_id)
        {
            match &shard.assignment {
                DfsMetadataShardAssignment::Root => {
                    if root_shard_id
                        .as_ref()
                        .is_none_or(|current| shard.shard_id.as_str() < current.as_str())
                    {
                        root_shard_id = Some(shard.shard_id.clone());
                    }
                }
                DfsMetadataShardAssignment::PathPrefix { prefix } => {
                    let prefix = normalize_metadata_selection_path(prefix)?;
                    if matches!(prefix.as_str(), SIFS_SERVICES_PATH | SIFS_USERS_PATH) {
                        saw_default_sifs_split_shard = true;
                    }
                    let mut node = &mut prefix_root;
                    for component in prefix
                        .trim_start_matches('/')
                        .split('/')
                        .filter(|component| !component.is_empty())
                    {
                        node = node.children.entry(component.to_string()).or_default();
                    }
                    if node
                        .shard_id
                        .as_ref()
                        .is_none_or(|current| shard.shard_id.as_str() < current.as_str())
                    {
                        node.shard_id = Some(shard.shard_id.clone());
                    }
                }
                DfsMetadataShardAssignment::NameHashRange {
                    start_inclusive,
                    end_exclusive,
                } => {
                    if start_inclusive == end_exclusive {
                        continue;
                    }
                    let width = metadata_hash_range_width(*start_inclusive, *end_exclusive);
                    let start = u128::from(*start_inclusive);
                    let end = u128::from(*end_exclusive);
                    let mut push_candidate = |start_inclusive: u128, end_exclusive: u128| {
                        if start_inclusive < end_exclusive {
                            hash_candidates.push(MetadataHashRoutingCandidate {
                                start_inclusive,
                                end_exclusive,
                                width,
                                shard_id: shard.shard_id.clone(),
                            });
                        }
                    };
                    if start_inclusive < end_exclusive {
                        push_candidate(start, end);
                    } else {
                        push_candidate(start, hash_space_end);
                        push_candidate(0, end);
                    }
                }
            }
        }

        let mut endpoints = Vec::with_capacity(hash_candidates.len().saturating_mul(2) + 2);
        endpoints.push(0);
        endpoints.push(hash_space_end);
        for candidate in &hash_candidates {
            endpoints.push(candidate.start_inclusive);
            endpoints.push(candidate.end_exclusive);
        }
        endpoints.sort_unstable();
        endpoints.dedup();

        let mut hash_intervals = Vec::<MetadataHashRoutingInterval>::new();
        for endpoints in endpoints.windows(2) {
            let start_inclusive = endpoints[0];
            let end_exclusive = endpoints[1];
            let winner = hash_candidates
                .iter()
                .filter(|candidate| {
                    candidate.start_inclusive <= start_inclusive
                        && start_inclusive < candidate.end_exclusive
                })
                .min_by(|left, right| {
                    left.width
                        .cmp(&right.width)
                        .then_with(|| left.shard_id.cmp(&right.shard_id))
                });
            let Some(winner) = winner else {
                continue;
            };
            if let Some(previous) = hash_intervals.last_mut()
                && previous.end_exclusive == start_inclusive
                && previous.shard_id == winner.shard_id
            {
                previous.end_exclusive = end_exclusive;
            } else {
                hash_intervals.push(MetadataHashRoutingInterval {
                    start_inclusive,
                    end_exclusive,
                    shard_id: winner.shard_id.clone(),
                });
            }
        }

        Ok(Self {
            file_system_id,
            root_shard_id,
            prefix_root,
            hash_intervals,
            saw_default_sifs_split_shard,
        })
    }

    pub(crate) fn select_shard_id(
        &self,
        path: &str,
        reject_incomplete_default_sifs_split: bool,
    ) -> Result<Option<&str>, KvError> {
        let path = normalize_metadata_selection_path(path)?;
        let mut prefix_match = self.prefix_root.shard_id.as_deref();
        let mut node = &self.prefix_root;
        for component in path
            .trim_start_matches('/')
            .split('/')
            .filter(|component| !component.is_empty())
        {
            let Some(next) = node.children.get(component) else {
                break;
            };
            node = next;
            if node.shard_id.is_some() {
                prefix_match = node.shard_id.as_deref();
            }
        }

        let path_hash = u128::from(metadata_name_hash(&path));
        let hash_match = self
            .hash_intervals
            .partition_point(|interval| interval.start_inclusive <= path_hash)
            .checked_sub(1)
            .and_then(|index| self.hash_intervals.get(index))
            .filter(|interval| path_hash < interval.end_exclusive)
            .map(|interval| interval.shard_id.as_str());

        if reject_incomplete_default_sifs_split
            && prefix_match.is_none()
            && hash_match.is_none()
            && self.root_shard_id.is_some()
            && self.file_system_id == sifs_file_system_id()
            && self.saw_default_sifs_split_shard
            && let Some((shard_id, prefix)) = default_sifs_split_shard_for_path(&path)
        {
            return Err(KvError::Busy {
                path: format!(
                    "metadata shard config incomplete for {path}: missing default {shard_id} shard for {prefix}"
                ),
            });
        }

        Ok(prefix_match
            .or(hash_match)
            .or(self.root_shard_id.as_deref()))
    }
}

fn select_metadata_shard_config_for_path_with_options(
    file_system_id: &DfsFileSystemId,
    shards: &[DfsMetadataShardConfig],
    path: &str,
    reject_incomplete_default_sifs_split: bool,
) -> Result<Option<DfsMetadataShardConfig>, KvError> {
    let path = normalize_metadata_selection_path(path)?;
    let path_hash = metadata_name_hash(&path);
    let mut root = None::<&DfsMetadataShardConfig>;
    let mut prefix_match = None::<(usize, String, &DfsMetadataShardConfig)>;
    let mut hash_match = None::<(u128, String, &DfsMetadataShardConfig)>;
    let mut saw_default_sifs_split_shard = false;

    for shard in shards {
        match &shard.assignment {
            DfsMetadataShardAssignment::Root => {
                if root
                    .as_ref()
                    .is_none_or(|current| shard.shard_id < current.shard_id)
                {
                    root = Some(shard);
                }
            }
            DfsMetadataShardAssignment::PathPrefix { prefix } => {
                let prefix = normalize_metadata_selection_path(prefix)?;
                if matches!(prefix.as_str(), SIFS_SERVICES_PATH | SIFS_USERS_PATH) {
                    saw_default_sifs_split_shard = true;
                }
                if metadata_path_matches_prefix(&path, &prefix) {
                    let candidate = (prefix.len(), shard.shard_id.clone(), shard);
                    if prefix_match.as_ref().is_none_or(|current| {
                        candidate.0 > current.0
                            || (candidate.0 == current.0 && candidate.1 < current.1)
                    }) {
                        prefix_match = Some(candidate);
                    }
                }
            }
            DfsMetadataShardAssignment::NameHashRange {
                start_inclusive,
                end_exclusive,
            } => {
                if metadata_hash_in_range(path_hash, *start_inclusive, *end_exclusive) {
                    let candidate = (
                        metadata_hash_range_width(*start_inclusive, *end_exclusive),
                        shard.shard_id.clone(),
                        shard,
                    );
                    if hash_match.as_ref().is_none_or(|current| {
                        candidate.0 < current.0
                            || (candidate.0 == current.0 && candidate.1 < current.1)
                    }) {
                        hash_match = Some(candidate);
                    }
                }
            }
        }
    }

    if reject_incomplete_default_sifs_split
        && prefix_match.is_none()
        && hash_match.is_none()
        && root.is_some()
        && file_system_id == &sifs_file_system_id()
        && saw_default_sifs_split_shard
        && let Some((shard_id, prefix)) = default_sifs_split_shard_for_path(&path)
    {
        return Err(KvError::Busy {
            path: format!(
                "metadata shard config incomplete for {path}: missing default {shard_id} shard for {prefix}"
            ),
        });
    }

    Ok(prefix_match
        .map(|(_, _, shard)| shard)
        .or_else(|| hash_match.map(|(_, _, shard)| shard))
        .or(root)
        .cloned())
}

fn default_sifs_split_shard_for_path(path: &str) -> Option<(&'static str, &'static str)> {
    if metadata_path_matches_prefix(path, SIFS_SERVICES_PATH) {
        Some(("services", SIFS_SERVICES_PATH))
    } else if metadata_path_matches_prefix(path, SIFS_USERS_PATH) {
        Some(("users", SIFS_USERS_PATH))
    } else {
        None
    }
}

fn normalize_metadata_selection_path(path: &str) -> Result<String, KvError> {
    if path.bytes().any(|byte| byte == 0) {
        return Err(KvError::Serialization {
            detail: format!("metadata shard path contains a nul byte: {path:?}"),
        });
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            value => parts.push(value),
        }
    }
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn metadata_path_matches_prefix(path: &str, prefix: &str) -> bool {
    prefix == "/"
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn metadata_name_hash(path: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in path.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn metadata_hash_in_range(hash: u64, start_inclusive: u64, end_exclusive: u64) -> bool {
    if start_inclusive == end_exclusive {
        return false;
    }
    if start_inclusive < end_exclusive {
        hash >= start_inclusive && hash < end_exclusive
    } else {
        hash >= start_inclusive || hash < end_exclusive
    }
}

fn metadata_hash_range_width(start_inclusive: u64, end_exclusive: u64) -> u128 {
    if start_inclusive <= end_exclusive {
        u128::from(end_exclusive - start_inclusive)
    } else {
        (u128::from(u64::MAX) - u128::from(start_inclusive) + 1) + u128::from(end_exclusive)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn placement_job_factory_rejects_invalid_structural_policies_for_manual_and_adaptive_data() {
        let owner = NodeId::generate();
        let candidate = crate::DfsNodeAdvertisement::content(owner.clone());
        let snapshot = crate::DfsPlacementSnapshot::empty(owner.clone());
        let budget = crate::DfsAdaptivePlacementBudget::default();
        for (w, n, valid) in [
            (1, 1, true),
            (1, 3, true),
            (2, 2, true),
            (2, 1, false),
            (0, 1, false),
            (1, 0, false),
        ] {
            let config = DfsFileSystemConfig {
                namespace_count_activation: None,
                file_system_id: DfsFileSystemId::generate(),
                display_name: "structural-policy-DATA".into(),
                owner_node_id: owner.clone(),
                retention_policies: Default::default(),
                module_metadata: Default::default(),
                subtree_policies: vec![crate::DfsSubtreePolicy::new(
                    "/data",
                    DfsObjectPolicy::ReplicatedEventual,
                    w,
                    n,
                )],
            };
            let manual = build_placement_reconciliation_job(
                format!("manual-W{w}-N{n}"),
                &config,
                "/data".into(),
                crate::DfsPlacementScope::Subtree,
                BTreeSet::new(),
                vec![candidate.clone()],
                None,
                1,
            );
            let adaptive = build_adaptive_placement_reconciliation_job(
                format!("adaptive-W{w}-N{n}"),
                &config,
                "/data".into(),
                crate::DfsPlacementScope::Subtree,
                BTreeSet::new(),
                vec![candidate.clone()],
                snapshot.clone(),
                budget.clone(),
                7,
                1,
            );
            assert_eq!(manual.is_ok(), valid, "manual W{w}/N{n}");
            assert_eq!(adaptive.is_ok(), valid, "adaptive W{w}/N{n}");
            if valid {
                let manual = manual.unwrap();
                let adaptive = adaptive.unwrap();
                assert_eq!(manual.accepted_config, config);
                assert_eq!(adaptive.accepted_config, config);
                assert_eq!(adaptive.adaptive_budget, Some(budget.clone()));
                assert_eq!(adaptive.processed_content_bytes, 7);
                assert_eq!(adaptive.placement_snapshot, Some(snapshot.clone()));
                assert!(adaptive.adaptive);
            } else {
                assert!(
                    matches!(manual, Err(KvError::Serialization { detail }) if detail.contains("invalid policy"))
                );
                assert!(
                    matches!(adaptive, Err(KvError::Serialization { detail }) if detail.contains("invalid policy"))
                );
            }
        }
    }

    use crate::{DfsFileSystem, DfsSubtreePolicy};
    use crate::{DfsNodeAdvertisement, DfsPlacementReconciliationState, DfsPlacementScope};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A client whose generic refresh and point-read paths are deliberately
    /// unusable. Metadata authority validation can succeed only by selecting
    /// the single native exact-key-set operation.
    #[derive(Debug)]
    struct ExactAuthorityOnlyKv {
        inner: InProcessKvStore,
        refresh_calls: AtomicUsize,
        coherent_observation_calls: AtomicUsize,
        authoritative_exact_set_calls: AtomicUsize,
        requested_exact_sets: Mutex<Vec<Vec<String>>>,
        authoritative_deadlines: Mutex<Vec<Instant>>,
        authoritative_errors: Mutex<VecDeque<KvError>>,
    }

    impl ExactAuthorityOnlyKv {
        fn new(inner: InProcessKvStore) -> Self {
            Self {
                inner,
                refresh_calls: AtomicUsize::new(0),
                coherent_observation_calls: AtomicUsize::new(0),
                authoritative_exact_set_calls: AtomicUsize::new(0),
                requested_exact_sets: Mutex::new(Vec::new()),
                authoritative_deadlines: Mutex::new(Vec::new()),
                authoritative_errors: Mutex::new(VecDeque::new()),
            }
        }

        fn reject_full_refresh(&self) -> KvError {
            KvError::Io {
                path: "test full snapshot refresh".to_string(),
                detail: "metadata authority validation must not import a full snapshot".to_string(),
            }
        }

        fn authoritative_exact_set(
            &self,
            keys: &[String],
        ) -> Result<Vec<Option<KvEntry>>, KvError> {
            self.authoritative_exact_set_calls
                .fetch_add(1, Ordering::SeqCst);
            self.requested_exact_sets
                .lock()
                .expect("exact authority requests")
                .push(keys.to_vec());
            if let Some(error) = self
                .authoritative_errors
                .lock()
                .expect("scripted exact authority errors")
                .pop_front()
            {
                return Err(error);
            }
            Ok(keys
                .iter()
                .map(|key| self.inner.get(key).cloned())
                .collect())
        }
    }

    impl DfsControlPlaneKvClient for ExactAuthorityOnlyKv {
        fn refresh_latest(&mut self) -> Result<(), KvError> {
            self.refresh_calls.fetch_add(1, Ordering::SeqCst);
            Err(self.reject_full_refresh())
        }

        fn get(&self, _key: &str) -> Result<Option<KvEntry>, KvError> {
            Err(self.reject_full_refresh())
        }

        fn get_consistent(&self, _keys: &[String]) -> Result<Vec<Option<KvEntry>>, KvError> {
            self.coherent_observation_calls
                .fetch_add(1, Ordering::SeqCst);
            Err(self.reject_full_refresh())
        }

        fn get_authoritative_consistent(
            &mut self,
            keys: &[String],
        ) -> Result<Vec<Option<KvEntry>>, KvError> {
            self.authoritative_exact_set(keys)
        }

        fn get_authoritative_consistent_before(
            &mut self,
            keys: &[String],
            deadline: Instant,
        ) -> Result<Vec<Option<KvEntry>>, KvError> {
            self.authoritative_deadlines
                .lock()
                .expect("exact authority deadlines")
                .push(deadline);
            self.authoritative_exact_set(keys)
        }

        fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
            self.inner.range_prefix(prefix)
        }

        fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
            self.inner.put(key, value, None)
        }

        fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
            self.inner.txn(txn)
        }
    }

    impl AsyncDfsControlPlaneKvClient for ExactAuthorityOnlyKv {
        fn refresh_latest_async<'a>(&'a mut self) -> DfsControlPlaneKvFuture<'a, ()> {
            self.refresh_calls.fetch_add(1, Ordering::SeqCst);
            let result = Err(self.reject_full_refresh());
            Box::pin(async move { result })
        }

        fn get_async<'a>(&'a self, _key: &'a str) -> DfsControlPlaneKvFuture<'a, Option<KvEntry>> {
            let result = Err(self.reject_full_refresh());
            Box::pin(async move { result })
        }

        fn get_consistent_async<'a>(
            &'a self,
            _keys: Vec<String>,
        ) -> DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
            self.coherent_observation_calls
                .fetch_add(1, Ordering::SeqCst);
            let result = Err(self.reject_full_refresh());
            Box::pin(async move { result })
        }

        fn get_authoritative_consistent_async<'a>(
            &'a mut self,
            keys: Vec<String>,
        ) -> DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
            let result = self.authoritative_exact_set(&keys);
            Box::pin(async move { result })
        }

        fn get_authoritative_consistent_before_async<'a>(
            &'a mut self,
            keys: Vec<String>,
            deadline: Instant,
        ) -> DfsControlPlaneKvFuture<'a, Vec<Option<KvEntry>>> {
            self.authoritative_deadlines
                .lock()
                .expect("exact authority deadlines")
                .push(deadline);
            let result = self.authoritative_exact_set(&keys);
            Box::pin(async move { result })
        }

        fn range_prefix_async<'a>(
            &'a self,
            prefix: &'a str,
        ) -> DfsControlPlaneKvFuture<'a, Vec<KvEntry>> {
            let result = self.inner.range_prefix(prefix);
            Box::pin(async move { result })
        }

        fn put_async<'a>(
            &'a mut self,
            key: String,
            value: Vec<u8>,
        ) -> DfsControlPlaneKvFuture<'a, KvRevision> {
            let result = self.inner.put(key, value, None);
            Box::pin(async move { result })
        }

        fn txn_async<'a>(&'a mut self, txn: KvTxn) -> DfsControlPlaneKvFuture<'a, KvTxnResponse> {
            let result = self.inner.txn(txn);
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct RejectPlacementHistoryRangeKv {
        inner: InProcessKvStore,
    }

    impl DfsControlPlaneKvClient for RejectPlacementHistoryRangeKv {
        fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
            Ok(self.inner.get(key).cloned())
        }

        fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
            if prefix.starts_with("/dfs/filesystems/")
                && prefix.ends_with("/placement-reconciliations-v1/")
            {
                return Err(KvError::Serialization {
                    detail: "placement history range must not be loaded after queue initialization"
                        .to_string(),
                });
            }
            self.inner.range_prefix(prefix)
        }

        fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
            self.inner.put(key, value, None)
        }

        fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
            self.inner.txn(txn)
        }
    }

    #[derive(Debug)]
    struct StaleObservedAuthoritativeTxnKv {
        observed: InProcessKvStore,
        authoritative: InProcessKvStore,
    }

    impl DfsControlPlaneKvClient for StaleObservedAuthoritativeTxnKv {
        fn authorizes_prior_process_placement_authority_recovery(
            &self,
            _local_node_id: &NodeId,
            _current_process: &DfsPlacementAuthorityProcessIdentity,
        ) -> bool {
            true
        }

        fn refresh_latest(&mut self) -> Result<(), KvError> {
            self.observed = InProcessKvStore::restore(self.authoritative.try_snapshot()?)?;
            Ok(())
        }

        fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
            Ok(self.observed.get(key).cloned())
        }

        fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
            self.observed.range_prefix(prefix)
        }

        fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
            self.authoritative.put(key, value, None)
        }

        fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
            self.authoritative.txn(txn)
        }
    }

    #[derive(Debug)]
    struct FailPlacementPermitDeleteKv {
        inner: InProcessKvStore,
        remaining_delete_failures: usize,
        apply_acquisition_then_fail: bool,
    }

    impl DfsControlPlaneKvClient for FailPlacementPermitDeleteKv {
        fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
            Ok(self.inner.get(key).cloned())
        }

        fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
            self.inner.range_prefix(prefix)
        }

        fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
            self.inner.put(key, value, None)
        }

        fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
            let puts_placement_permit = txn.success.iter().any(|operation| {
                matches!(operation, KvOp::Put { key, .. } if key.ends_with("/placement-authority-permit-v1"))
            });
            if puts_placement_permit && self.apply_acquisition_then_fail {
                self.apply_acquisition_then_fail = false;
                let response = self.inner.txn(txn)?;
                assert!(response.succeeded, "injected acquisition must commit");
                return Err(KvError::Io {
                    path: "test-placement-authority-permit".to_string(),
                    detail: "injected ambiguous permit acquisition".to_string(),
                });
            }
            let deletes_placement_permit = txn.success.iter().any(|operation| {
                matches!(operation, KvOp::Delete { key } if key.ends_with("/placement-authority-permit-v1"))
            });
            if deletes_placement_permit && self.remaining_delete_failures > 0 {
                self.remaining_delete_failures -= 1;
                return Err(KvError::Io {
                    path: "test-placement-authority-permit".to_string(),
                    detail: "injected permit release failure".to_string(),
                });
            }
            self.inner.txn(txn)
        }
    }

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("sifs").expect("valid filesystem id")
    }

    fn placement_authority_test_config(
        owner: &NodeId,
        desired_replicas: usize,
    ) -> DfsFileSystemConfig {
        DfsFileSystemConfig {
            namespace_count_activation: None,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: vec![DfsSubtreePolicy {
                path_prefix: "/users/team".to_string(),
                policy: DfsObjectPolicy::ReplicatedEventual,
                scope: DfsPlacementScope::Subtree,
                min_commit_replicas: 1,
                desired_replicas: desired_replicas,
                required_node_ids: Vec::new(),
                forbidden_node_ids: Vec::new(),
                min_distinct_zones: 0,
            }],
        }
    }

    fn placement_authority_test_job(
        job_id: &str,
        file_system_id: &DfsFileSystemId,
        owner: &NodeId,
        created_at_unix_nanos: u64,
    ) -> DfsPlacementReconciliationJob {
        DfsPlacementReconciliationJob {
            schema_version: DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
            job_id: job_id.to_string(),
            file_system_id: file_system_id.clone(),
            path: "/users/team".to_string(),
            scope: DfsPlacementScope::Subtree,
            unavailable_node_ids: BTreeSet::new(),
            candidate_snapshot: vec![DfsNodeAdvertisement::content(owner.clone())],
            placement_snapshot: None,
            adaptive: false,
            adaptive_budget: None,
            processed_content_bytes: 0,
            accepted_config: placement_authority_test_config(owner, 1),
            state: DfsPlacementReconciliationState::Pending,
            cursor: None,
            total_object_count: 0,
            scanned_object_count: 0,
            matched_object_count: 0,
            updated_object_count: 0,
            scheduled_repair_count: 0,
            completed_repair_count: 0,
            pending_repair_count: 0,
            revision: 0,
            created_at_unix_nanos,
            updated_at_unix_nanos: created_at_unix_nanos,
            last_error: None,
        }
    }

    fn reactive_placement_projection_for_test(
        kv: &InProcessKvStore,
        local_node: &NodeId,
    ) -> Result<crate::DfsPendingMaintenanceWork, crate::DfsError> {
        let root = sinomo_testdir::TestDir::new("reactive-placement-projection");
        crate::dfs_pending_reactive_maintenance_work_from_authority_entries(
            &sinomo_core::local::SinomoDataDir::new(root.path()),
            local_node,
            &kv.range_prefix(DFS_KV_PREFIX).unwrap(),
            1,
        )
    }

    fn seed_reactive_placement_root_for_test(kv: &mut InProcessKvStore, owner: &NodeId) {
        let shard = DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        kv.put(
            metadata_shard_key(&shard.file_system_id, &shard.shard_id),
            sinomo_core::kv::encode_kv_value(&shard).unwrap(),
            None,
        )
        .unwrap();
    }

    #[test]
    fn reactive_placement_nonowner_does_not_decode_remote_job_history() {
        let owner = node("reactive-remote-owner");
        let replica = node("reactive-remote-replica");
        let mut kv = InProcessKvStore::new();
        seed_reactive_placement_root_for_test(&mut kv, &owner);
        kv.put(
            placement_reconciliation_key(&crate::sifs_file_system_id(), "retained-remote"),
            vec![0xff],
            None,
        )
        .unwrap();
        let work = reactive_placement_projection_for_test(&kv, &replica)
            .expect("remote job history is not this node's runnable work");
        assert_eq!(work.actionable_placement_reconciliation_count, 0);
        assert!(!work.should_trigger_background_maintenance());
        assert!(reactive_placement_projection_for_test(&kv, &owner).is_err());
    }

    #[tokio::test]
    async fn placement_progress_async_preserves_sync_guards_and_terminal_queue() {
        let owner = node("async-progress-owner");
        let config = placement_authority_test_config(&owner, 1);
        let mut seed = InProcessKvStore::new();
        let expected = {
            let mut store = DfsControlPlaneStore::new(&mut seed);
            store.put_file_system_config(&config).unwrap();
            let job =
                placement_authority_test_job("async-progress", &config.file_system_id, &owner, 1);
            assert!(
                store
                    .compare_and_put_placement_update(&config, &config, &job)
                    .unwrap()
            );
            store
                .placement_reconciliation(&config.file_system_id, &job.job_id)
                .unwrap()
                .unwrap()
        };
        let before = seed.try_snapshot().unwrap();
        let guard_key = file_system_config_key(&config.file_system_id);
        let guard_value = seed.get(&guard_key).unwrap().value.clone();
        for state in [
            DfsPlacementReconciliationState::Completed,
            DfsPlacementReconciliationState::Failed,
        ] {
            for guard_matches in [true, false] {
                let mut synchronous = InProcessKvStore::restore(before.clone()).unwrap();
                let mut asynchronous = InProcessKvStore::restore(before.clone()).unwrap();
                let mut desired = expected.clone();
                desired.state = state;
                desired.revision += 1;
                desired.updated_at_unix_nanos += 1;
                let guards = vec![KvCompare::ValueEquals {
                    key: guard_key.clone(),
                    value: if guard_matches {
                        guard_value.clone()
                    } else {
                        b"stale-configuration".to_vec()
                    },
                }];
                let sync_result = DfsControlPlaneStore::new(&mut synchronous)
                    .compare_and_put_placement_reconciliation_with_guards(
                        &expected,
                        &desired,
                        guards.clone(),
                    )
                    .unwrap();
                let async_result = DfsControlPlaneAsyncStore::new(&mut asynchronous)
                    .compare_and_put_placement_reconciliation_with_guards(
                        &expected, &desired, guards,
                    )
                    .await
                    .unwrap();
                assert_eq!(sync_result, guard_matches);
                assert_eq!(async_result, guard_matches);
                assert_eq!(
                    synchronous.try_snapshot().unwrap(),
                    asynchronous.try_snapshot().unwrap()
                );
                if !guard_matches {
                    assert_eq!(asynchronous.try_snapshot().unwrap(), before);
                    continue;
                }
                let queue: DfsPlacementReconciliationQueue = asynchronous
                    .get(&placement_reconciliation_queue_key(&config.file_system_id))
                    .unwrap()
                    .decode()
                    .unwrap();
                assert!(queue.job_ids.is_empty());
                let actual: DfsPlacementReconciliationJob = asynchronous
                    .get(&placement_reconciliation_key(
                        &config.file_system_id,
                        &expected.job_id,
                    ))
                    .unwrap()
                    .decode()
                    .unwrap();
                assert_eq!(actual, desired);
                assert!(
                    !DfsControlPlaneAsyncStore::new(&mut asynchronous)
                        .compare_and_put_placement_reconciliation_with_guards(
                            &expected,
                            &desired,
                            Vec::new()
                        )
                        .await
                        .unwrap(),
                    "old durable progress cannot overwrite the terminal job"
                );
            }
        }
        let mut invalid = expected.clone();
        invalid.candidate_snapshot.clear();
        let mut asynchronous = InProcessKvStore::restore(before.clone()).unwrap();
        assert!(
            DfsControlPlaneAsyncStore::new(&mut asynchronous)
                .compare_and_put_placement_reconciliation_with_guards(
                    &expected,
                    &invalid,
                    Vec::new()
                )
                .await
                .is_err()
        );
        assert_eq!(asynchronous.try_snapshot().unwrap(), before);
    }

    fn placement_authority_test_process_identity() -> DfsPlacementAuthorityProcessIdentity {
        DfsPlacementAuthorityProcessIdentity {
            cluster_id: "placement-test-cluster".to_string(),
            process_session_id: "placement-test-process-session".to_string(),
        }
    }

    #[test]
    fn adaptive_coalescing_handles_config_prioritized_queue_order() {
        let mut kv = InProcessKvStore::new();
        let owner = node("adaptive-priority-owner");
        let old_config = placement_authority_test_config(&owner, 1);
        let current_config = placement_authority_test_config(&owner, 2);
        let mut store = DfsControlPlaneStore::new(&mut kv);
        store.put_file_system_config(&old_config).unwrap();
        let mut old =
            placement_authority_test_job("older-stale", &old_config.file_system_id, &owner, 1);
        old.adaptive = true;
        old.placement_snapshot = Some(crate::DfsPlacementSnapshot::empty(owner.clone()));
        old.adaptive_budget = Some(Default::default());
        assert!(
            store
                .compare_and_put_placement_update(&old_config, &old_config, &old)
                .unwrap()
        );
        let mut newest = old.clone();
        newest.job_id = "newer-current".into();
        newest.created_at_unix_nanos = 2;
        newest.updated_at_unix_nanos = 2;
        newest
            .candidate_snapshot
            .push(DfsNodeAdvertisement::content(node(
                "adaptive-priority-peer",
            )));
        assert!(
            store
                .compare_and_put_placement_update(&old_config, &current_config, &newest)
                .unwrap()
        );
        let (_, queue) = store
            .placement_reconciliation_queue_snapshot(
                &current_config.file_system_id,
                &current_config,
            )
            .unwrap();
        assert_eq!(
            queue.job_ids,
            [newest.job_id.clone(), old.job_id.clone()],
            "configuration priority deliberately differs from chronological order"
        );
        let next = store
            .next_placement_reconciliation(&current_config.file_system_id, &current_config)
            .unwrap()
            .unwrap();
        assert_eq!(next.job_id, newest.job_id);
        let obsolete = store
            .placement_reconciliation(&current_config.file_system_id, &old.job_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            obsolete.state,
            crate::DfsPlacementReconciliationState::Failed
        );
        assert!(obsolete.last_error.unwrap().contains(&newest.job_id));
    }

    #[test]
    fn adaptive_queue_coalescing_preserves_explicit_progress_and_mutation_fences() {
        let mut kv = InProcessKvStore::new();
        let owner = node("adaptive-coalesce-owner");
        let config = placement_authority_test_config(&owner, 1);
        let mut store = DfsControlPlaneStore::new(&mut kv);
        store.put_file_system_config(&config).unwrap();
        for (index, name) in ["explicit", "active", "obsolete", "newest", "other-scope"]
            .into_iter()
            .enumerate()
        {
            let mut job = placement_authority_test_job(
                name,
                &config.file_system_id,
                &owner,
                index as u64 + 1,
            );
            job.adaptive = name != "explicit";
            if job.adaptive {
                job.placement_snapshot = Some(crate::DfsPlacementSnapshot::empty(owner.clone()));
                job.adaptive_budget = Some(Default::default());
            }
            if name == "active" {
                job.state = crate::DfsPlacementReconciliationState::Scanning;
                job.scanned_object_count = 1;
                job.cursor = Some(crate::DfsObjectPageCursor {
                    path: "/users/team/a".into(),
                    object_id: crate::DfsObjectId::from_legacy_named("coalesce-progress").unwrap(),
                    shard_id: None,
                });
            }
            if name == "other-scope" {
                job.path = "/services".into();
            }
            assert!(
                store
                    .compare_and_put_placement_update(&config, &config, &job)
                    .unwrap()
            );
        }
        let explicit = store
            .placement_reconciliation(&config.file_system_id, "explicit")
            .unwrap()
            .unwrap();
        let obsolete = store
            .placement_reconciliation(&config.file_system_id, "obsolete")
            .unwrap()
            .unwrap();
        let mut permit = store
            .try_acquire_placement_authority_permit(
                &explicit,
                &config,
                &owner,
                &placement_authority_test_process_identity(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .next_placement_reconciliation(&config.file_system_id, &config)
                .unwrap(),
            Some(explicit.clone())
        );
        assert_eq!(
            store
                .placement_reconciliation(&config.file_system_id, "obsolete")
                .unwrap()
                .unwrap()
                .state,
            crate::DfsPlacementReconciliationState::Pending
        );
        store
            .release_placement_authority_permit(&mut permit)
            .unwrap();
        assert_eq!(
            store
                .next_placement_reconciliation(&config.file_system_id, &config)
                .unwrap(),
            Some(explicit.clone())
        );
        let retired = store
            .placement_reconciliation(&config.file_system_id, "obsolete")
            .unwrap()
            .unwrap();
        assert_eq!(
            retired.state,
            crate::DfsPlacementReconciliationState::Failed
        );
        assert!(retired.last_error.as_ref().unwrap().contains("newest"));
        assert!(
            store
                .try_acquire_placement_authority_permit(
                    &obsolete,
                    &config,
                    &owner,
                    &placement_authority_test_process_identity(),
                )
                .unwrap()
                .is_none(),
            "delayed effects of the coalesced job remain fenced"
        );
        for name in ["newest", "other-scope"] {
            assert_eq!(
                store
                    .placement_reconciliation(&config.file_system_id, name)
                    .unwrap()
                    .unwrap()
                    .state,
                crate::DfsPlacementReconciliationState::Pending
            );
        }
        assert_eq!(
            store
                .placement_reconciliation(&config.file_system_id, "active")
                .unwrap()
                .unwrap()
                .scanned_object_count,
            1
        );
        assert_eq!(
            store
                .placement_reconciliation(&config.file_system_id, "explicit")
                .unwrap(),
            Some(explicit)
        );
    }

    #[test]
    fn adaptive_queue_selects_replacement_after_retiring_obsolete_heads() {
        let mut kv = InProcessKvStore::new();
        let owner = node("adaptive-head-owner");
        let config = placement_authority_test_config(&owner, 1);
        let mut store = DfsControlPlaneStore::new(&mut kv);
        store.put_file_system_config(&config).unwrap();
        for (index, name) in ["first", "second", "latest"].into_iter().enumerate() {
            let mut job = placement_authority_test_job(
                name,
                &config.file_system_id,
                &owner,
                index as u64 + 1,
            );
            job.adaptive = true;
            job.placement_snapshot = Some(crate::DfsPlacementSnapshot::empty(owner.clone()));
            job.adaptive_budget = Some(Default::default());
            assert!(
                store
                    .compare_and_put_placement_update(&config, &config, &job)
                    .unwrap()
            );
        }
        assert_eq!(
            store
                .next_placement_reconciliation(&config.file_system_id, &config)
                .unwrap()
                .unwrap()
                .job_id,
            "latest"
        );
        for name in ["first", "second"] {
            assert_eq!(
                store
                    .placement_reconciliation(&config.file_system_id, name)
                    .unwrap()
                    .unwrap()
                    .state,
                crate::DfsPlacementReconciliationState::Failed
            );
        }
    }

    #[test]
    fn placement_permit_recovery_is_scoped_to_prior_local_process() {
        let process = placement_authority_test_process_identity();
        let local_node_id = node("placement-permit-process-match");
        let record = |holder_node_id, holder_session_id| DfsPlacementAuthorityPermitRecord {
            job_id: "job".to_string(),
            holder_id: "holder".to_string(),
            holder_cluster_id: process.cluster_id.clone(),
            holder_node_id: holder_node_id,
            holder_session_id: holder_session_id,
        };
        let current = sinomo_core::kv::encode_kv_value(&record(
            local_node_id.clone(),
            process.process_session_id.clone(),
        ))
        .expect("encode current permit");
        let prior = sinomo_core::kv::encode_kv_value(&record(
            local_node_id.clone(),
            "prior-session".to_string(),
        ))
        .expect("encode prior permit");
        let remote = sinomo_core::kv::encode_kv_value(&record(
            node("placement-permit-other-node"),
            "remote-session".to_string(),
        ))
        .expect("encode remote permit");

        assert!(
            !placement_authority_permit_value_requires_local_process_recovery(
                &current,
                &local_node_id,
                &process,
            )
            .expect("ignore current permit")
        );
        assert!(
            placement_authority_permit_value_requires_local_process_recovery(
                &prior,
                &local_node_id,
                &process,
            )
            .expect("recover prior local permit")
        );
        assert!(
            !placement_authority_permit_value_requires_local_process_recovery(
                &remote,
                &local_node_id,
                &process,
            )
            .expect("ignore remote permit")
        );
    }

    fn placement_authority_operator_test_cluster_id() -> sinomo_core::Id {
        sinomo_core::Id::from_legacy_named("placement-operator-cluster").expect("valid cluster id")
    }

    fn placement_authority_operator_test_record(
        cluster_id: Option<&sinomo_core::Id>,
        holder_node_id: Option<NodeId>,
        holder_session_id: Option<&str>,
    ) -> DfsPlacementAuthorityPermitRecord {
        DfsPlacementAuthorityPermitRecord {
            job_id: "placement-operator-recovery-job".to_string(),
            holder_id: sinomo_core::Id::generate().to_string(),
            holder_cluster_id: cluster_id.expect("identified cluster").to_string(),
            holder_node_id: holder_node_id.expect("identified node"),
            holder_session_id: holder_session_id
                .expect("identified process session")
                .to_string(),
        }
    }

    fn put_placement_authority_operator_test_record(
        kv: &mut InProcessKvStore,
        file_system_id: &DfsFileSystemId,
        record: &DfsPlacementAuthorityPermitRecord,
    ) {
        kv.put_serde(placement_authority_permit_key(file_system_id), record, None)
            .expect("put placement authority permit");
    }

    fn placement_authority_operator_test_member(
        cluster_id: &sinomo_core::Id,
        node_id: &NodeId,
        state: ClusterMemberState,
    ) -> ClusterMemberRecord {
        let mut member = ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            node_id.clone(),
            "placement recovery holder",
            "placement-recovery-holder-public-key",
            time::OffsetDateTime::UNIX_EPOCH,
        );
        member.state = state;
        member
    }

    fn copy_current_prepare_member_fixture_data<'a>(
        kv: &mut InProcessKvStore,
        node_ids: impl IntoIterator<Item = &'a NodeId>,
    ) {
        copy_native_member_fixture_data(kv, node_ids.into_iter().cloned().collect());
    }

    #[cfg(feature = "storage-conversion")]
    #[test]
    fn converter_placement_plan_is_the_normal_native_publication_transaction() {
        let (native, service, shard) = crate::test_support::current_native_metadata_service(
            "native-placement-input-plan",
            1,
            "/",
        )
        .unwrap();
        let control = native.shared_control_plane().unwrap();
        let candidates = control
            .with_client(|kv| DfsControlPlaneStore::new(kv.as_mut()).cluster_view())
            .unwrap()
            .content_nodes;
        assert!(
            !candidates.is_empty(),
            "the mounted normal content advertisement must exist"
        );
        let config = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut()).get_file_system_config(&shard.file_system_id)
            })
            .unwrap()
            .unwrap();
        // Complete the normal empty-queue initialization before capturing the
        // publication DATA baseline. The same maintenance producer can do this
        // concurrently; its legitimate CAS retry is not another publication.
        control.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv.as_mut());
            assert!(
                store
                    .next_placement_reconciliation(&shard.file_system_id, &config)
                    .unwrap()
                    .is_none()
            );
            let entry = store
                .kv
                .get(&placement_reconciliation_queue_key(&shard.file_system_id))
                .unwrap()
                .unwrap();
            let queue: DfsPlacementReconciliationQueue = entry.decode().unwrap();
            assert_eq!(
                queue.schema_version,
                PLACEMENT_RECONCILIATION_QUEUE_SCHEMA_VERSION
            );
            assert!(queue.job_ids.is_empty());
        });
        let now = crate::dfs_now_unix_nanos();
        let job = build_placement_reconciliation_job(
            sinomo_core::Id::generate().to_string(),
            &config,
            "/services".into(),
            crate::DfsPlacementScope::SelfOnly,
            BTreeSet::new(),
            candidates,
            None,
            now,
        )
        .unwrap();
        let plan = DfsPlacementUpdatePlan::new(&config, &config, &job).unwrap();
        let entries = control
            .with_client(|kv| kv.get_consistent(plan.keys()))
            .unwrap();
        let queued_keys = plan.queued_job_keys(entries.clone()).unwrap().unwrap();
        let queued = control
            .with_client(|kv| kv.get_consistent(&queued_keys))
            .unwrap();
        let expected = plan.transaction(entries.clone(), queued).unwrap().unwrap();
        let submitted = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let copied = std::sync::Arc::clone(&submitted);
        let job_key = placement_reconciliation_key(&job.file_system_id, &job.job_id);
        let hook = native
            .native_kv_request_hook(&shard.primary_node_id, move |request| {
                if request.txn.success.iter().any(|op| {
                    matches!(op,
                KvOp::Put { key, .. } if key == &job_key)
                }) {
                    copied.lock().unwrap().push(request.txn.clone());
                }
                Ok(())
            })
            .unwrap();
        assert!(
            control
                .with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                    .compare_and_put_placement_update(&config, &config, &job))
                .unwrap()
        );
        assert_eq!(submitted.lock().unwrap().as_slice(), &[expected]);
        drop(hook);
        let mut wrong_scope = entries.clone();
        wrong_scope[0].as_mut().unwrap().key.push_str("-unrelated");
        assert!(plan.transaction(wrong_scope, Vec::new()).is_err());
        let mut present_permit = entries;
        let mut actual_entry = present_permit[0].as_ref().unwrap().clone();
        actual_entry.key = plan.keys()[5].clone();
        present_permit[5] = Some(actual_entry);
        assert!(
            plan.transaction(present_permit, Vec::new()).is_err(),
            "data planning cannot relax the normal Missing permit predicate"
        );
        drop(service);
        drop(control);
        native.close().unwrap();
    }

    #[test]
    fn current_placement_job_requires_exact_config_and_candidate_fields() {
        let owner = node("required-current-placement-fields");
        let config = placement_authority_test_config(&owner, 1);
        let candidates = vec![DfsNodeAdvertisement::content(owner)];
        let job = build_placement_reconciliation_job(
            "current-required-fields".into(),
            &config,
            "/users/team".into(),
            DfsPlacementScope::Subtree,
            BTreeSet::new(),
            candidates,
            None,
            42,
        )
        .unwrap();
        assert_eq!(
            job.accepted_config,
            scoped_placement_config_for_job(&config, &job)
        );
        assert_eq!(job.state, DfsPlacementReconciliationState::Pending);
        let encoded = serde_json::to_value(&job).unwrap();
        for field in ["schema_version", "accepted_config", "candidate_snapshot"] {
            let mut missing = encoded.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<DfsPlacementReconciliationJob>(missing).is_err(),
                "current durable input must not restore a missing {field}"
            );
        }
        let mut unqualified = encoded;
        unqualified
            .as_object_mut()
            .unwrap()
            .insert("legacy_holder".into(), serde_json::json!("old"));
        assert!(serde_json::from_value::<DfsPlacementReconciliationJob>(unqualified).is_err());
        assert!(
            build_placement_reconciliation_job(
                "empty-current-candidates".into(),
                &config,
                "/users/team".into(),
                DfsPlacementScope::Subtree,
                BTreeSet::new(),
                Vec::new(),
                None,
                42,
            )
            .is_err(),
            "current work requires actual accepted candidate data"
        );
    }

    #[test]
    fn placement_policy_and_reconciliation_job_commit_atomically() {
        let mut kv = InProcessKvStore::new();
        let file_system_id = file_system_id();
        let expected = DfsFileSystemConfig {
            namespace_count_activation: None,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: file_system_id.clone(),
            display_name: "sifs".to_string(),
            owner_node_id: node("placement-owner"),
            subtree_policies: Vec::new(),
        };
        let mut desired = expected.clone();
        desired.subtree_policies.push(DfsSubtreePolicy {
            path_prefix: "/users/team".to_string(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            scope: DfsPlacementScope::Subtree,
            min_commit_replicas: 1,
            desired_replicas: 3,
            required_node_ids: vec![node("placement-required")],
            forbidden_node_ids: Vec::new(),
            min_distinct_zones: 2,
        });
        desired.subtree_policies.push(DfsSubtreePolicy {
            path_prefix: "/elsewhere".to_string(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            scope: DfsPlacementScope::Subtree,
            min_commit_replicas: 1,
            desired_replicas: 1,
            required_node_ids: Vec::new(),
            forbidden_node_ids: Vec::new(),
            min_distinct_zones: 0,
        });
        let job = DfsPlacementReconciliationJob {
            schema_version: DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
            job_id: "placement-job".to_string(),
            file_system_id: file_system_id.clone(),
            path: "/users/team".to_string(),
            scope: DfsPlacementScope::Subtree,
            unavailable_node_ids: BTreeSet::new(),
            candidate_snapshot: vec![DfsNodeAdvertisement::content(node("placement-owner"))],
            placement_snapshot: None,
            adaptive: false,
            adaptive_budget: None,
            processed_content_bytes: 0,
            accepted_config: desired.clone(),
            state: DfsPlacementReconciliationState::Pending,
            cursor: None,
            total_object_count: 0,
            scanned_object_count: 0,
            matched_object_count: 0,
            updated_object_count: 0,
            scheduled_repair_count: 0,
            completed_repair_count: 0,
            pending_repair_count: 0,
            revision: 0,
            created_at_unix_nanos: 1,
            updated_at_unix_nanos: 1,
            last_error: None,
        };
        let mut store = DfsControlPlaneStore::new(&mut kv);
        store
            .put_file_system_config(&expected)
            .expect("put filesystem");
        assert!(
            store
                .compare_and_put_placement_update(&expected, &desired, &job)
                .expect("commit placement")
        );
        assert_eq!(
            store
                .get_file_system_config(&file_system_id)
                .expect("get filesystem"),
            Some(desired.clone())
        );
        let mut durable_job = job.clone();
        durable_job.accepted_config = scoped_placement_config_for_job(&desired, &job);
        assert_eq!(
            durable_job
                .accepted_config
                .subtree_policies
                .iter()
                .map(|rule| rule.path_prefix.as_str())
                .collect::<Vec<_>>(),
            vec!["/users/team"]
        );
        assert_eq!(
            store
                .placement_reconciliation(&file_system_id, &job.job_id)
                .expect("get job"),
            Some(durable_job.clone())
        );
        let activity = store
            .placement_reconciliation_activity(&file_system_id, &[], None, None, 50)
            .expect("read compact placement activity");
        assert_eq!(activity.activities.len(), 1);
        assert_eq!(activity.activities[0].job_id, job.job_id);
        assert_eq!(
            activity.activities[0].status,
            DfsPlacementReconciliationActivityStatus::Running
        );
        assert!(
            activity.partial,
            "completeness stays monotonic until a future explicit migration proves old history"
        );
        assert!(!activity.history_truncated);
        let compact_record = store
            .get_serde::<DfsPlacementReconciliationActivityRecord>(
                &placement_reconciliation_activity_record_key(&file_system_id, &job.job_id),
            )
            .expect("read compact activity record")
            .expect("compact activity record");
        assert_eq!(compact_record.activity.revision, durable_job.revision);
        assert!(
            placement_reconciliation_key(&file_system_id, &job.job_id)
                .starts_with(&placement_reconciliations_prefix(&file_system_id))
        );

        let mut stripped = durable_job.clone();
        stripped.candidate_snapshot.clear();
        assert!(matches!(
            store.compare_and_put_placement_reconciliation(&durable_job, &stripped),
            Err(KvError::Serialization { detail })
                if detail.contains("candidate snapshot")
        ));
        assert_eq!(
            store
                .placement_reconciliation(&file_system_id, &job.job_id)
                .expect("job after rejected snapshot strip"),
            Some(durable_job.clone())
        );
        assert!(
            !store
                .compare_and_put_placement_update(&expected, &desired, &job)
                .expect("reject stale update")
        );

        let mut completed = durable_job.clone();
        completed.state = DfsPlacementReconciliationState::Completed;
        completed.revision = 1;
        completed.updated_at_unix_nanos = 2;
        assert!(
            store
                .compare_and_put_placement_reconciliation(&durable_job, &completed)
                .expect("complete placement job")
        );
        let queue_key = placement_reconciliation_queue_key(&file_system_id);
        let queue = store
            .get_serde::<DfsPlacementReconciliationQueue>(&queue_key)
            .expect("read active queue")
            .expect("active queue");
        assert!(queue.job_ids.is_empty());
        assert_eq!(
            store
                .placement_reconciliation(&file_system_id, &job.job_id)
                .expect("retained terminal job"),
            Some(completed.clone())
        );
        let completed_activity = store
            .placement_reconciliation_activity(&file_system_id, &[], None, None, 50)
            .expect("read completed activity");
        assert_eq!(completed_activity.activities.len(), 1);
        assert_eq!(completed_activity.activities[0].revision, 1);
        assert_eq!(
            completed_activity.activities[0].status,
            DfsPlacementReconciliationActivityStatus::Completed
        );

        // Simulate an interrupted/older writer leaving stale queue ids. The
        // scheduler repairs both without falling back to a history scan.
        store
            .put_serde(
                &queue_key,
                &DfsPlacementReconciliationQueue {
                    schema_version: PLACEMENT_RECONCILIATION_QUEUE_SCHEMA_VERSION,
                    job_ids: vec!["missing-job".to_string(), completed.job_id.clone()],
                },
            )
            .expect("inject stale queue ids");
        let mut kv = RejectPlacementHistoryRangeKv { inner: kv };
        let mut store = DfsControlPlaneStore::new(&mut kv);
        assert_eq!(
            store
                .next_placement_reconciliation(&file_system_id, &desired)
                .expect("repair stale queue"),
            None
        );
        assert!(
            store
                .get_serde::<DfsPlacementReconciliationQueue>(&queue_key)
                .expect("read repaired queue")
                .expect("repaired queue")
                .job_ids
                .is_empty()
        );
        let activity = store
            .placement_reconciliation_activity(&file_system_id, &[], None, None, 50)
            .expect("activity must not scan placement history");
        assert_eq!(activity.activities.len(), 1);
        assert_eq!(activity.activities[0].job_id, completed.job_id);
    }

    #[test]
    fn placement_activity_missing_current_record_is_bounded_and_explicitly_partial() {
        let (mut data, actual) =
            native_placement_activity_fixture_data("native-activity-negative-gap");
        let before = DfsControlPlaneStore::new(&mut data)
            .placement_reconciliation_activity(&actual.file_system_id, &[], None, None, 10)
            .unwrap();
        assert!(
            before
                .activities
                .iter()
                .any(|row| row.job_id == actual.job_id)
        );
        // Negative omission of a compact record produced by the SAME normal
        // Apply. Full job DATA is not a replacement healthy activity producer.
        data.txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Delete {
                key: placement_reconciliation_activity_record_key(
                    &actual.file_system_id,
                    &actual.job_id,
                ),
            }],
            failure: Vec::new(),
        })
        .unwrap();
        let mut data = RejectPlacementHistoryRangeKv { inner: data };
        let page = DfsControlPlaneStore::new(&mut data)
            .placement_reconciliation_activity(&actual.file_system_id, &[], None, None, 10)
            .unwrap();
        assert!(page.activities.is_empty());
        assert!(page.partial);
        assert!(page.history_truncated);
    }

    #[test]
    fn placement_activity_uses_actual_compact_record_without_full_job_scan() {
        let (mut data, actual) =
            native_placement_activity_fixture_data("native-activity-without-full-row");
        let before = DfsControlPlaneStore::new(&mut data)
            .placement_reconciliation_activity(&actual.file_system_id, &[], None, None, 10)
            .unwrap();
        assert!(
            before
                .activities
                .iter()
                .any(|row| row.job_id == actual.job_id)
        );
        // Lose only the full execution DATA row. The actual native compact
        // record stays unchanged; no completion state or summary is invented.
        data.txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Delete {
                key: placement_reconciliation_key(&actual.file_system_id, &actual.job_id),
            }],
            failure: Vec::new(),
        })
        .unwrap();
        let mut data = RejectPlacementHistoryRangeKv { inner: data };
        let page = DfsControlPlaneStore::new(&mut data)
            .placement_reconciliation_activity(&actual.file_system_id, &[], None, None, 10)
            .unwrap();
        assert_eq!(page, before);
    }

    #[test]
    fn placement_activity_old_writer_gap_remains_partial_after_new_writer_bootstrap() {
        let file_system_id = file_system_id();
        let owner = node("placement-activity-monotonic-partial-owner");
        let base = DfsFileSystemConfig {
            namespace_count_activation: None,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: file_system_id.clone(),
            display_name: "sifs".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        };
        let desired = placement_authority_test_config(&owner, 1);
        let mut old_terminal = placement_authority_test_job(
            "placement-activity-unindexed-terminal",
            &file_system_id,
            &owner,
            1,
        );
        old_terminal.accepted_config = scoped_placement_config_for_job(&base, &old_terminal);
        old_terminal.state = DfsPlacementReconciliationState::Completed;
        let new_job = placement_authority_test_job(
            "placement-activity-post-upgrade",
            &file_system_id,
            &owner,
            2,
        );

        let mut kv = InProcessKvStore::new();
        kv.put_serde(file_system_config_key(&file_system_id), &base, None)
            .expect("put pre-index config");
        kv.put_serde(
            placement_reconciliation_key(&file_system_id, &old_terminal.job_id),
            &old_terminal,
            None,
        )
        .expect("put unindexed terminal job");
        kv.put_serde(
            placement_reconciliation_queue_key(&file_system_id),
            &DfsPlacementReconciliationQueue::default(),
            None,
        )
        .expect("put pre-index empty queue");

        let mut store = DfsControlPlaneStore::new(&mut kv);
        assert!(
            store
                .compare_and_put_placement_update(&base, &desired, &new_job)
                .expect("first post-upgrade Apply")
        );
        let page = store
            .placement_reconciliation_activity(&file_system_id, &[], None, None, 10)
            .expect("post-upgrade activity");
        assert_eq!(page.activities.len(), 1);
        assert_eq!(page.activities[0].job_id, new_job.job_id);
        assert!(
            page.partial,
            "current writers becoming capable cannot erase the durable possibility of an older terminal gap"
        );
    }

    #[test]
    fn placement_activity_unions_old_active_jobs_with_the_capped_recent_index() {
        let file_system_id = file_system_id();
        let owner = node("placement-activity-cap-owner");
        let mut kv = InProcessKvStore::new();
        let mut index = DfsPlacementReconciliationActivityIndex {
            history_truncated: true,
            ..DfsPlacementReconciliationActivityIndex::default()
        };

        for offset in 0..MAX_RECENT_PLACEMENT_RECONCILIATION_ACTIVITIES {
            let mut job = placement_authority_test_job(
                &format!("placement-activity-recent-{offset:04}"),
                &file_system_id,
                &owner,
                1_000 + offset as u64,
            );
            job.state = DfsPlacementReconciliationState::Completed;
            let record = DfsPlacementReconciliationActivityRecord::from(&job);
            index.recent.push(record.activity.cursor());
            kv.put_serde(
                placement_reconciliation_activity_record_key(&file_system_id, &job.job_id),
                &record,
                None,
            )
            .expect("put compact recent activity");
        }
        sort_placement_reconciliation_activity_cursors(&mut index.recent);
        kv.put_serde(
            placement_reconciliation_activity_index_key(&file_system_id),
            &index,
            None,
        )
        .expect("put capped recent index");

        let mut active = placement_authority_test_job(
            "placement-activity-long-running",
            &file_system_id,
            &owner,
            1,
        );
        let active_config = placement_authority_test_config(&owner, 1);
        active.accepted_config = scoped_placement_config_for_job(&active_config, &active);
        kv.put_serde(
            placement_reconciliation_key(&file_system_id, &active.job_id),
            &active,
            None,
        )
        .expect("put full active execution record");
        kv.put_serde(
            placement_reconciliation_activity_record_key(&file_system_id, &active.job_id),
            &DfsPlacementReconciliationActivityRecord::from(&active),
            None,
        )
        .expect("put compact active activity");
        kv.put_serde(
            placement_reconciliation_queue_key(&file_system_id),
            &DfsPlacementReconciliationQueue {
                schema_version: PLACEMENT_RECONCILIATION_QUEUE_SCHEMA_VERSION,
                job_ids: vec![active.job_id.clone()],
            },
            None,
        )
        .expect("put active queue");

        let mut kv = RejectPlacementHistoryRangeKv { inner: kv };
        let store = DfsControlPlaneStore::new(&mut kv);
        let active_page = store
            .placement_reconciliation_activity(
                &file_system_id,
                &[DfsPlacementReconciliationActivityStatus::Running],
                None,
                None,
                10,
            )
            .expect("active activity outside recent cap");
        assert_eq!(active_page.activities.len(), 1);
        assert_eq!(active_page.activities[0].job_id, active.job_id);
        assert!(active_page.partial);
        assert!(active_page.history_truncated);

        let first_completed = store
            .placement_reconciliation_activity(
                &file_system_id,
                &[DfsPlacementReconciliationActivityStatus::Completed],
                None,
                None,
                10,
            )
            .expect("first completed activity page");
        assert_eq!(first_completed.activities.len(), 10);
        assert!(first_completed.next_cursor.is_some());
        let second_completed = store
            .placement_reconciliation_activity(
                &file_system_id,
                &[DfsPlacementReconciliationActivityStatus::Completed],
                None,
                first_completed.next_cursor.as_ref(),
                10,
            )
            .expect("second completed activity page");
        assert_eq!(second_completed.activities.len(), 10);
        assert!(
            second_completed.activities[0].created_at_unix_nanos
                < first_completed.activities[9].created_at_unix_nanos
        );
    }

    #[test]
    fn placement_activity_index_retention_is_bounded() {
        let file_system_id = file_system_id();
        let owner = node("placement-activity-retention-owner");
        let mut index = DfsPlacementReconciliationActivityIndex::default();
        for offset in 0..=MAX_RECENT_PLACEMENT_RECONCILIATION_ACTIVITIES {
            let job = placement_authority_test_job(
                &format!("placement-activity-retention-{offset:04}"),
                &file_system_id,
                &owner,
                offset as u64,
            );
            include_placement_reconciliation_activity(&mut index, &job)
                .expect("include bounded activity identity");
        }
        assert_eq!(
            index.recent.len(),
            MAX_RECENT_PLACEMENT_RECONCILIATION_ACTIVITIES
        );
        assert!(index.history_truncated);
        assert_eq!(
            index.recent[0].created_at_unix_nanos,
            MAX_RECENT_PLACEMENT_RECONCILIATION_ACTIVITIES as u64
        );
        assert_eq!(index.recent.last().unwrap().created_at_unix_nanos, 1);
    }

    #[test]
    fn placement_activity_retention_deletes_only_evicted_terminal_records() {
        let file_system_id = file_system_id();
        let owner = node("placement-activity-gc-owner");
        let config = placement_authority_test_config(&owner, 1);
        let mut kv = InProcessKvStore::new();
        let mut index = DfsPlacementReconciliationActivityIndex::default();
        let mut oldest_terminal_job_id = String::new();
        for offset in 0..MAX_RECENT_PLACEMENT_RECONCILIATION_ACTIVITIES {
            let mut job = placement_authority_test_job(
                &format!("placement-activity-gc-terminal-{offset:04}"),
                &file_system_id,
                &owner,
                1_000 + offset as u64,
            );
            job.state = DfsPlacementReconciliationState::Completed;
            let record = DfsPlacementReconciliationActivityRecord::from(&job);
            if offset == 0 {
                oldest_terminal_job_id = job.job_id.clone();
            }
            index.recent.push(record.activity.cursor());
            kv.put_serde(
                placement_reconciliation_activity_record_key(&file_system_id, &job.job_id),
                &record,
                None,
            )
            .expect("put terminal compact record");
        }
        sort_placement_reconciliation_activity_cursors(&mut index.recent);
        kv.put_serde(
            placement_reconciliation_activity_index_key(&file_system_id),
            &index,
            None,
        )
        .expect("put full activity index");

        let mut newest_active = placement_authority_test_job(
            "placement-activity-gc-newest-active",
            &file_system_id,
            &owner,
            2_000,
        );
        newest_active.accepted_config = scoped_placement_config_for_job(&config, &newest_active);
        kv.put_serde(
            placement_reconciliation_key(&file_system_id, &newest_active.job_id),
            &newest_active,
            None,
        )
        .expect("put newest active job");
        kv.put_serde(
            placement_reconciliation_queue_key(&file_system_id),
            &DfsPlacementReconciliationQueue {
                schema_version: PLACEMENT_RECONCILIATION_QUEUE_SCHEMA_VERSION,
                job_ids: vec![newest_active.job_id.clone()],
            },
            None,
        )
        .expect("put newest active queue");
        let mut newest_progress = newest_active.clone();
        newest_progress.state = DfsPlacementReconciliationState::Scanning;
        newest_progress.revision = 1;
        newest_progress.updated_at_unix_nanos = 2_001;
        let mut store = DfsControlPlaneStore::new(&mut kv);
        assert!(
            store
                .compare_and_put_placement_reconciliation(&newest_active, &newest_progress)
                .expect("index newest active job")
        );
        assert!(
            store
                .kv
                .get(&placement_reconciliation_activity_record_key(
                    &file_system_id,
                    &oldest_terminal_job_id,
                ))
                .expect("read evicted terminal record")
                .is_none(),
            "an evicted terminal compact record must not grow a second unbounded namespace"
        );

        let mut oldest_active = placement_authority_test_job(
            "placement-activity-gc-oldest-active",
            &file_system_id,
            &owner,
            1,
        );
        oldest_active.accepted_config = scoped_placement_config_for_job(&config, &oldest_active);
        store
            .put_serde(
                &placement_reconciliation_key(&file_system_id, &oldest_active.job_id),
                &oldest_active,
            )
            .expect("put oldest active job");
        store
            .put_serde(
                &placement_reconciliation_queue_key(&file_system_id),
                &DfsPlacementReconciliationQueue {
                    schema_version: PLACEMENT_RECONCILIATION_QUEUE_SCHEMA_VERSION,
                    job_ids: vec![newest_progress.job_id.clone(), oldest_active.job_id.clone()],
                },
            )
            .expect("put both active jobs");
        let mut oldest_progress = oldest_active.clone();
        oldest_progress.state = DfsPlacementReconciliationState::Scanning;
        oldest_progress.revision = 1;
        oldest_progress.updated_at_unix_nanos = 2;
        assert!(
            store
                .compare_and_put_placement_reconciliation(&oldest_active, &oldest_progress)
                .expect("advance evicted active job")
        );
        assert!(
            store
                .kv
                .get(&placement_reconciliation_activity_record_key(
                    &file_system_id,
                    &oldest_progress.job_id,
                ))
                .expect("read active compact record")
                .is_some(),
            "an active job remains observable even when its creation cursor falls outside the recent cap"
        );
        let mut oldest_completed = oldest_progress.clone();
        oldest_completed.state = DfsPlacementReconciliationState::Completed;
        oldest_completed.revision = 2;
        oldest_completed.updated_at_unix_nanos = 3;
        assert!(
            store
                .compare_and_put_placement_reconciliation(&oldest_progress, &oldest_completed)
                .expect("terminalize evicted active job")
        );
        assert!(
            store
                .kv
                .get(&placement_reconciliation_activity_record_key(
                    &file_system_id,
                    &oldest_completed.job_id,
                ))
                .expect("read terminalized evicted activity")
                .is_none(),
            "once the evicted job leaves the active queue its compact record is deleted atomically"
        );
        let queue = store
            .get_serde::<DfsPlacementReconciliationQueue>(&placement_reconciliation_queue_key(
                &file_system_id,
            ))
            .expect("read final active queue")
            .expect("final active queue");
        assert!(!queue.job_ids.contains(&oldest_completed.job_id));
    }

    #[test]
    fn generic_publication_preserves_policy_and_defaults_preserve_base_fields() {
        let mut kv = InProcessKvStore::new();
        let original_owner = node("placement-publication-original-owner");
        let updated_owner = node("placement-publication-updated-owner");
        let mut custom = placement_authority_test_config(&original_owner, 2);
        custom.display_name = "custom sifs".to_string();
        let mut store = DfsControlPlaneStore::new(&mut kv);
        store
            .put_file_system_config(&custom)
            .expect("initial custom config");

        store
            .put_file_system_config(&DfsFileSystemConfig {
                namespace_count_activation: None,
                retention_policies: Default::default(),
                module_metadata: Default::default(),
                file_system_id: custom.file_system_id.clone(),
                display_name: "renamed sifs".to_string(),
                owner_node_id: updated_owner.clone(),
                subtree_policies: Vec::new(),
            })
            .expect("generic owner/display update");
        let published = store
            .get_file_system_config(&custom.file_system_id)
            .expect("published config")
            .expect("published config exists");
        assert_eq!(published.owner_node_id, updated_owner);
        assert_eq!(published.display_name, "renamed sifs");
        assert_eq!(published.subtree_policies, custom.subtree_policies);

        let defaults = DfsFileSystemConfig {
            namespace_count_activation: None,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: custom.file_system_id.clone(),
            display_name: "bootstrap default".to_string(),
            owner_node_id: original_owner,
            subtree_policies: vec![DfsSubtreePolicy {
                path_prefix: "/services".to_string(),
                policy: DfsObjectPolicy::ReplicatedEventual,
                scope: DfsPlacementScope::Subtree,
                min_commit_replicas: 1,
                desired_replicas: 1,
                required_node_ids: Vec::new(),
                forbidden_node_ids: Vec::new(),
                min_distinct_zones: 0,
            }],
        };
        store
            .ensure_file_system_config_defaults(&defaults)
            .expect("first defaults publication");
        store
            .ensure_file_system_config_defaults(&defaults)
            .expect("recurring defaults publication");
        let merged = store
            .get_file_system_config(&custom.file_system_id)
            .expect("merged config")
            .expect("merged config exists");
        assert_eq!(merged.owner_node_id, updated_owner);
        assert_eq!(merged.display_name, "renamed sifs");
        assert_eq!(merged.subtree_policies[0], custom.subtree_policies[0]);
        assert!(merged.subtree_policies.iter().any(|rule| {
            rule.path_prefix == "/services" && rule.scope == DfsPlacementScope::Subtree
        }));
    }

    #[test]
    fn generic_publication_does_not_resurrect_deleted_policy() {
        let mut kv = InProcessKvStore::new();
        let owner = node("placement-publication-deleted-policy-owner");
        let deleted = DfsFileSystemConfig {
            namespace_count_activation: None,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: file_system_id(),
            display_name: "sifs after placement deletion".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        };
        let mut store = DfsControlPlaneStore::new(&mut kv);
        store
            .put_file_system_config(&deleted)
            .expect("seed authoritative post-Apply config");

        let mut stale_publication = placement_authority_test_config(&owner, 2);
        stale_publication.display_name = "generic publisher base-field update".to_string();
        assert!(
            !stale_publication.subtree_policies.is_empty(),
            "test setup must carry the placement rule deleted by Apply"
        );
        store
            .put_file_system_config(&stale_publication)
            .expect("generic publication");

        let published = store
            .get_file_system_config(&deleted.file_system_id)
            .expect("published config")
            .expect("published config exists");
        assert_eq!(
            published.display_name, stale_publication.display_name,
            "generic publication may still update non-policy fields"
        );
        assert!(
            published.subtree_policies.is_empty(),
            "generic publication must not resurrect a policy deleted by Apply"
        );
    }

    #[test]
    fn placement_apply_retries_stale_queue_without_losing_background_work() {
        let owner = node("placement-queue-race-owner");
        let base = placement_authority_test_config(&owner, 1);
        let desired = placement_authority_test_config(&owner, 2);
        let proposal = placement_authority_test_job(
            "placement-queue-race-proposal",
            &base.file_system_id,
            &owner,
            2,
        );
        let mut background = placement_authority_test_job(
            "placement-queue-race-background",
            &base.file_system_id,
            &owner,
            1,
        );
        background.path = "/services".into();
        let mut authoritative = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut authoritative)
            .put_file_system_config(&base)
            .expect("base config");
        let observed = InProcessKvStore::restore(authoritative.try_snapshot().expect("snapshot"))
            .expect("observed pre-maintenance snapshot");
        assert!(
            DfsControlPlaneStore::new(&mut authoritative)
                .compare_and_put_placement_update(&base, &base, &background)
                .expect("enqueue unrelated background work")
        );
        let mut client = StaleObservedAuthoritativeTxnKv {
            observed,
            authoritative,
        };
        // The first queue CAS is definitely rejected against the stale
        // observed image. The store refreshes bookkeeping and retries this
        // same approved proposal internally, without requiring another Apply.
        assert!(
            DfsControlPlaneStore::new(&mut client)
                .compare_and_put_placement_update(&base, &desired, &proposal)
                .expect("retry rejected queue compare with the same proposal identity")
        );
        let store = DfsControlPlaneStore::new(&mut client.authoritative);
        assert_eq!(
            store.get_file_system_config(&base.file_system_id).unwrap(),
            Some(desired.clone())
        );
        let (_, queue) = store
            .placement_reconciliation_queue_snapshot(&base.file_system_id, &desired)
            .expect("durable queue");
        assert_eq!(
            queue.job_ids,
            vec![background.job_id, proposal.job_id.clone()]
        );
        let durable = store
            .placement_reconciliation(&base.file_system_id, &proposal.job_id)
            .unwrap()
            .expect("published proposal");
        assert_eq!(durable.job_id, proposal.job_id);
        assert_eq!(
            durable.accepted_config,
            scoped_placement_config_for_job(&desired, &proposal)
        );
    }

    #[test]
    fn placement_apply_cannot_cross_a_held_metadata_mutation_permit() {
        let mut kv = InProcessKvStore::new();
        let owner = node("placement-permit-owner");
        let base = DfsFileSystemConfig {
            namespace_count_activation: None,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        };
        let first_config = placement_authority_test_config(&owner, 2);
        let first_job =
            placement_authority_test_job("placement-permit-first", &base.file_system_id, &owner, 1);
        let second_config = placement_authority_test_config(&owner, 1);
        let second_job = placement_authority_test_job(
            "placement-permit-second",
            &base.file_system_id,
            &owner,
            2,
        );
        let mut store = DfsControlPlaneStore::new(&mut kv);
        store.put_file_system_config(&base).expect("base config");
        assert!(
            store
                .compare_and_put_placement_update(&base, &first_config, &first_job)
                .expect("first Apply")
        );
        let durable_first = store
            .placement_reconciliation(&base.file_system_id, &first_job.job_id)
            .expect("first job read")
            .expect("first job");
        let mut permit = store
            .try_acquire_placement_authority_permit(
                &durable_first,
                &first_config,
                &owner,
                &placement_authority_test_process_identity(),
            )
            .expect("acquire first permit")
            .expect("first permit");

        let mut bootstrap_defaults = first_config.clone();
        bootstrap_defaults.subtree_policies.push(DfsSubtreePolicy {
            path_prefix: "/services".to_string(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            scope: DfsPlacementScope::Subtree,
            min_commit_replicas: 1,
            desired_replicas: 1,
            required_node_ids: Vec::new(),
            forbidden_node_ids: Vec::new(),
            min_distinct_zones: 0,
        });
        assert!(matches!(
            store.ensure_file_system_config_defaults(&bootstrap_defaults),
            Err(KvError::Busy { path })
                if path.contains(&placement_authority_permit_key(&base.file_system_id))
        ));

        assert!(matches!(
            store.compare_and_put_placement_update(
                &first_config,
                &second_config,
                &second_job,
            ),
            Err(KvError::Busy { path })
                if path.contains(&placement_authority_permit_key(&base.file_system_id))
        ));
        assert_eq!(
            store
                .get_file_system_config(&base.file_system_id)
                .expect("config while permit held"),
            Some(first_config.clone())
        );

        store
            .release_placement_authority_permit(&mut permit)
            .expect("release first permit");
        assert!(
            store
                .compare_and_put_placement_update(&first_config, &second_config, &second_job,)
                .expect("second Apply after release")
        );
        assert!(
            store
                .try_acquire_placement_authority_permit(
                    &durable_first,
                    &first_config,
                    &owner,
                    &placement_authority_test_process_identity(),
                )
                .expect("stale generation acquire")
                .is_none(),
            "an old job/config generation must not acquire after replacement Apply"
        );
    }

    #[test]
    fn stale_observed_generation_cannot_pass_the_authoritative_permit_transaction() {
        let owner = node("placement-stale-observed-owner");
        let base = DfsFileSystemConfig {
            namespace_count_activation: None,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        };
        let first_config = placement_authority_test_config(&owner, 2);
        let first_job = placement_authority_test_job(
            "placement-stale-observed-first",
            &base.file_system_id,
            &owner,
            1,
        );
        let second_config = placement_authority_test_config(&owner, 1);
        let second_job = placement_authority_test_job(
            "placement-stale-observed-second",
            &base.file_system_id,
            &owner,
            2,
        );
        let mut authoritative = InProcessKvStore::new();
        let durable_first = {
            let mut store = DfsControlPlaneStore::new(&mut authoritative);
            store.put_file_system_config(&base).expect("base config");
            assert!(
                store
                    .compare_and_put_placement_update(&base, &first_config, &first_job)
                    .expect("first Apply")
            );
            store
                .placement_reconciliation(&base.file_system_id, &first_job.job_id)
                .expect("first job read")
                .expect("first job")
        };
        let observed = InProcessKvStore::restore(
            authoritative
                .try_snapshot()
                .expect("stale observed snapshot"),
        )
        .expect("restore stale observed snapshot");
        {
            let mut store = DfsControlPlaneStore::new(&mut authoritative);
            assert!(
                store
                    .compare_and_put_placement_update(&first_config, &second_config, &second_job,)
                    .expect("replacement Apply")
            );
        }

        let mut client = StaleObservedAuthoritativeTxnKv {
            observed,
            authoritative,
        };
        let mut store = DfsControlPlaneStore::new(&mut client);
        assert!(
            store
                .try_acquire_placement_authority_permit(
                    &durable_first,
                    &first_config,
                    &owner,
                    &placement_authority_test_process_identity(),
                )
                .expect("stale observed permit attempt")
                .is_none(),
            "the exact job/config compare must execute against authoritative Raft state"
        );
        store.kv.refresh_latest().expect("refresh observed state");
        assert_eq!(
            store
                .get_file_system_config(&base.file_system_id)
                .expect("refreshed config"),
            Some(second_config)
        );
    }

    #[test]
    fn active_placement_permit_never_auto_expires_and_stale_release_is_harmless() {
        let mut kv = InProcessKvStore::new();
        let owner = node("placement-permanent-permit-owner");
        let base = DfsFileSystemConfig {
            namespace_count_activation: None,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        };
        let first_config = placement_authority_test_config(&owner, 2);
        let first_job = placement_authority_test_job(
            "placement-permanent-first",
            &base.file_system_id,
            &owner,
            1,
        );
        let second_config = placement_authority_test_config(&owner, 1);
        let second_job = placement_authority_test_job(
            "placement-permanent-second",
            &base.file_system_id,
            &owner,
            2,
        );
        let mut store = DfsControlPlaneStore::new(&mut kv);
        store.put_file_system_config(&base).expect("base config");
        assert!(
            store
                .compare_and_put_placement_update(&base, &first_config, &first_job)
                .expect("first Apply")
        );
        let durable_first = store
            .placement_reconciliation(&base.file_system_id, &first_job.job_id)
            .expect("first job read")
            .expect("first job");
        let mut first_permit = store
            .try_acquire_placement_authority_permit(
                &durable_first,
                &first_config,
                &owner,
                &placement_authority_test_process_identity(),
            )
            .expect("acquire permanent permit")
            .expect("permanent permit");
        let permit_key = placement_authority_permit_key(&base.file_system_id);
        let permit_entry = store
            .kv
            .get(&permit_key)
            .expect("permit read")
            .expect("permit");
        assert_eq!(
            permit_entry.lease_id, None,
            "the latch must not auto-expire"
        );
        assert!(matches!(
            store.compare_and_put_placement_update(&first_config, &second_config, &second_job),
            Err(KvError::Busy { path })
                if path.contains(&placement_authority_permit_key(&base.file_system_id))
                    && path.contains("cannot be auto-expired")
        ));

        store
            .release_placement_authority_permit(&mut first_permit)
            .expect("release first permit");
        assert!(
            store
                .compare_and_put_placement_update(&first_config, &second_config, &second_job)
                .expect("Apply after exact release")
        );
        let durable_second = store
            .placement_reconciliation(&base.file_system_id, &second_job.job_id)
            .expect("second job read")
            .expect("second job");
        let mut new_permit = store
            .try_acquire_placement_authority_permit(
                &durable_second,
                &second_config,
                &owner,
                &placement_authority_test_process_identity(),
            )
            .expect("acquire new permit")
            .expect("new permit");
        let new_entry = store
            .kv
            .get(&permit_key)
            .expect("new permit read")
            .expect("new permit entry")
            .clone();

        // A delayed retry from the prior holder compares its exact value and
        // therefore cannot delete the replacement holder.
        store
            .release_placement_authority_permit(&mut first_permit)
            .expect("stale release is harmless");
        assert_eq!(
            store
                .kv
                .get(&permit_key)
                .expect("permit after stale release"),
            Some(new_entry)
        );
        store
            .release_placement_authority_permit(&mut new_permit)
            .expect("release new permit");
    }

    #[test]
    fn placement_failed_release_survives_ephemeral_store_and_fresh_store_finishes_exact_delete() {
        let owner = node("placement-release-recovery-owner");
        let base = DfsFileSystemConfig {
            namespace_count_activation: None,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        };
        let accepted = placement_authority_test_config(&owner, 2);
        let job = placement_authority_test_job(
            "placement-release-recovery",
            &base.file_system_id,
            &owner,
            1,
        );
        let mut client = FailPlacementPermitDeleteKv {
            inner: InProcessKvStore::new(),
            remaining_delete_failures: 0,
            apply_acquisition_then_fail: false,
        };
        let mut store = DfsControlPlaneStore::new(&mut client);
        store.put_file_system_config(&base).expect("base config");
        assert!(
            store
                .compare_and_put_placement_update(&base, &accepted, &job)
                .expect("Apply")
        );
        let durable_job = store
            .placement_reconciliation(&base.file_system_id, &job.job_id)
            .expect("job read")
            .expect("durable job");
        let mut permit = store
            .try_acquire_placement_authority_permit(
                &durable_job,
                &accepted,
                &owner,
                &placement_authority_test_process_identity(),
            )
            .expect("permit acquisition")
            .expect("permit");
        let permit_record = permit.record.clone();

        client.remaining_delete_failures = 1;
        let mut store = DfsControlPlaneStore::new(&mut client);
        assert!(matches!(
            store.release_placement_authority_permit(&mut permit),
            Err(KvError::Io { detail, .. }) if detail.contains("injected")
        ));
        assert!(placement_authority_release_is_pending(
            &base.file_system_id,
            &permit_record
        ));

        // Model the maintenance tick's ephemeral store and service being
        // destroyed after release exhaustion. The process-wide exact handle
        // remains recoverable by a new store instance.
        drop(permit);
        let mut store = DfsControlPlaneStore::new(&mut client);
        let mut republished = accepted.clone();
        republished.display_name = "sifs after release recovery".to_string();
        store
            .put_file_system_config(&republished)
            .expect("fresh store recovers exact pending release");
        assert!(
            store
                .kv
                .get(&placement_authority_permit_key(&base.file_system_id))
                .expect("permit read")
                .is_none()
        );
        assert!(!placement_authority_release_is_pending(
            &base.file_system_id,
            &permit_record
        ));
    }

    #[test]
    fn placement_ambiguous_acquisition_is_queued_before_any_metadata_mutation() {
        let owner = node("placement-ambiguous-acquisition-owner");
        let base = DfsFileSystemConfig {
            namespace_count_activation: None,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        };
        let accepted = placement_authority_test_config(&owner, 2);
        let job = placement_authority_test_job(
            "placement-ambiguous-acquisition",
            &base.file_system_id,
            &owner,
            1,
        );
        let mut client = FailPlacementPermitDeleteKv {
            inner: InProcessKvStore::new(),
            remaining_delete_failures: 0,
            apply_acquisition_then_fail: false,
        };
        let mut store = DfsControlPlaneStore::new(&mut client);
        store.put_file_system_config(&base).expect("base config");
        assert!(
            store
                .compare_and_put_placement_update(&base, &accepted, &job)
                .expect("Apply")
        );
        let durable_job = store
            .placement_reconciliation(&base.file_system_id, &job.job_id)
            .expect("job read")
            .expect("durable job");

        client.apply_acquisition_then_fail = true;
        let mut store = DfsControlPlaneStore::new(&mut client);
        assert!(matches!(
            store.try_acquire_placement_authority_permit(
                &durable_job,
                &accepted,
                &owner,
                &placement_authority_test_process_identity(),
            ),
            Err(KvError::Io { detail, .. }) if detail.contains("ambiguous permit acquisition")
        ));
        let permit_record = client
            .inner
            .get(&placement_authority_permit_key(&base.file_system_id))
            .expect("committed ambiguous permit")
            .decode::<DfsPlacementAuthorityPermitRecord>()
            .expect("permit decoding");
        assert!(placement_authority_release_is_pending(
            &base.file_system_id,
            &permit_record
        ));

        // No metadata action was entered, so a fresh store may clear only the
        // exact ambiguously committed record and proceed with config work.
        let mut store = DfsControlPlaneStore::new(&mut client);
        let mut republished = accepted.clone();
        republished.display_name = "sifs after ambiguous acquisition".to_string();
        store
            .put_file_system_config(&republished)
            .expect("fresh store clears ambiguous acquisition");
        assert!(
            store
                .kv
                .get(&placement_authority_permit_key(&base.file_system_id))
                .expect("permit read")
                .is_none()
        );
        assert!(!placement_authority_release_is_pending(
            &base.file_system_id,
            &permit_record
        ));
    }

    #[test]
    fn placement_prior_process_recovery_requires_host_ownership_capability() {
        let file_system_id = file_system_id();
        let owner = node("placement-recovery-unauthorized-owner");
        let current_process = DfsPlacementAuthorityProcessIdentity {
            cluster_id: "placement-recovery-unauthorized-cluster".to_string(),
            process_session_id: "placement-recovery-unauthorized-current-session".to_string(),
        };
        let record = DfsPlacementAuthorityPermitRecord {
            job_id: "placement-recovery-unauthorized-job".to_string(),
            holder_id: "placement-recovery-unauthorized-holder".to_string(),
            holder_cluster_id: current_process.cluster_id.clone(),
            holder_node_id: owner.clone(),
            holder_session_id: "placement-recovery-unauthorized-prior-session".to_string(),
        };
        let key = placement_authority_permit_key(&file_system_id);
        let mut client = RejectPlacementHistoryRangeKv {
            inner: InProcessKvStore::new(),
        };
        client
            .inner
            .put(
                key.clone(),
                sinomo_core::kv::encode_kv_value(&record).expect("permit encoding"),
                None,
            )
            .expect("permit write");

        let error = DfsControlPlaneStore::new(&mut client)
            .recover_prior_process_placement_authority_permit_for_local_node(
                &file_system_id,
                &owner,
                &current_process,
            )
            .expect_err("generic clients cannot forge daemon ownership");
        assert!(
            matches!(error, KvError::Busy { path } if path.contains("live exclusive data-owner capability"))
        );
        assert!(client.inner.get(&key).is_some());
    }

    #[test]
    fn placement_prior_process_recovery_requires_exact_cluster_node_and_new_session() {
        let mut kv = InProcessKvStore::new();
        let owner = node("placement-prior-process-owner");
        let base = DfsFileSystemConfig {
            namespace_count_activation: None,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        };
        let accepted = placement_authority_test_config(&owner, 2);
        let job = placement_authority_test_job(
            "placement-prior-process",
            &base.file_system_id,
            &owner,
            1,
        );
        let prior_process = DfsPlacementAuthorityProcessIdentity {
            cluster_id: "placement-recovery-cluster".to_string(),
            process_session_id: "placement-prior-process-session".to_string(),
        };
        let current_process = DfsPlacementAuthorityProcessIdentity {
            cluster_id: prior_process.cluster_id.clone(),
            process_session_id: "placement-current-process-session".to_string(),
        };
        let mut store = DfsControlPlaneStore::new(&mut kv);
        store.put_file_system_config(&base).expect("base config");
        assert!(
            store
                .compare_and_put_placement_update(&base, &accepted, &job)
                .expect("Apply")
        );
        let durable_job = store
            .placement_reconciliation(&base.file_system_id, &job.job_id)
            .expect("job read")
            .expect("durable job");
        let permit = store
            .try_acquire_placement_authority_permit(&durable_job, &accepted, &owner, &prior_process)
            .expect("permit acquisition")
            .expect("permit");
        // Simulate the old process disappearing without running Drop. The
        // daemon-side exclusive lock is the external proof used by recovery.
        std::mem::forget(permit);

        assert!(
            !store
                .recover_prior_process_placement_authority_permit_for_local_node(
                    &base.file_system_id,
                    &owner,
                    &prior_process,
                )
                .expect("current-session recovery is rejected")
        );
        assert!(
            !store
                .recover_prior_process_placement_authority_permit_for_local_node(
                    &base.file_system_id,
                    &node("placement-foreign-node"),
                    &current_process,
                )
                .expect("foreign-node recovery is rejected")
        );
        let foreign_cluster = DfsPlacementAuthorityProcessIdentity {
            cluster_id: "placement-foreign-cluster".to_string(),
            process_session_id: current_process.process_session_id.clone(),
        };
        assert!(
            !store
                .recover_prior_process_placement_authority_permit_for_local_node(
                    &base.file_system_id,
                    &owner,
                    &foreign_cluster,
                )
                .expect("foreign-cluster recovery is rejected")
        );
        assert!(
            store
                .recover_prior_process_placement_authority_permit_for_local_node(
                    &base.file_system_id,
                    &owner,
                    &current_process,
                )
                .expect("guarded prior-process recovery")
        );
        assert!(
            store
                .kv
                .get(&placement_authority_permit_key(&base.file_system_id))
                .expect("permit read")
                .is_none()
        );
    }

    #[test]
    fn placement_operator_inspection_is_sanitized_and_missing_member_clear_is_guarded() {
        let file_system_id = file_system_id();
        let cluster_id = placement_authority_operator_test_cluster_id();
        let holder_node_id = node("placement-operator-missing-holder");
        let local_node_id = node("placement-operator-local-node");
        let record = placement_authority_operator_test_record(
            Some(&cluster_id),
            Some(holder_node_id.clone()),
            Some("private-holder-session"),
        );
        let mut kv = InProcessKvStore::new();
        put_placement_authority_operator_test_record(&mut kv, &file_system_id, &record);
        let mut store = DfsControlPlaneStore::new(&mut kv);
        let status = store
            .placement_authority_permit_status(&file_system_id)
            .expect("inspect permit")
            .expect("permit status");
        assert_eq!(status.holder_cluster_id, cluster_id.to_string());
        assert_eq!(status.holder_node_id, holder_node_id);
        assert!(status.recovery_token.starts_with("v1:"));
        let encoded = serde_json::to_string(&status).expect("status json");
        assert!(!encoded.contains("private-holder-session"));
        assert!(!encoded.contains(&record.holder_id));

        assert_eq!(
            store
                .recover_removed_node_placement_authority_permit(
                    &file_system_id,
                    &cluster_id.to_string(),
                    &local_node_id,
                    &status.recovery_token,
                    false,
                )
                .expect("reject missing acknowledgement"),
            DfsPlacementAuthorityRecoveryOutcome::Refused {
                reason: DfsPlacementAuthorityRecoveryRefusal::OwnerStoppedAcknowledgementRequired,
            }
        );
        assert_eq!(
            store
                .recover_removed_node_placement_authority_permit(
                    &file_system_id,
                    &cluster_id.to_string(),
                    &local_node_id,
                    &status.recovery_token,
                    true,
                )
                .expect("clear removed holder"),
            DfsPlacementAuthorityRecoveryOutcome::Cleared
        );
        assert_eq!(
            store
                .placement_authority_permit_status(&file_system_id)
                .expect("inspect cleared permit"),
            None
        );
    }

    #[test]
    fn placement_operator_clear_accepts_only_exact_removed_membership_evidence() {
        let file_system_id = file_system_id();
        let cluster_id = placement_authority_operator_test_cluster_id();
        let local_node_id = node("placement-operator-membership-local");
        let holder_node_id = node("placement-operator-membership-holder");

        for state in [
            ClusterMemberState::Active,
            ClusterMemberState::Offline,
            ClusterMemberState::Pending,
            ClusterMemberState::Suspect,
        ] {
            let record = placement_authority_operator_test_record(
                Some(&cluster_id),
                Some(holder_node_id.clone()),
                Some("placement-membership-session"),
            );
            let mut kv = InProcessKvStore::new();
            put_placement_authority_operator_test_record(&mut kv, &file_system_id, &record);
            kv.put_serde(
                cluster_member_key(&holder_node_id),
                &placement_authority_operator_test_member(
                    &cluster_id,
                    &holder_node_id,
                    state.clone(),
                ),
                None,
            )
            .expect("put member");
            let mut store = DfsControlPlaneStore::new(&mut kv);
            let status = store
                .placement_authority_permit_status(&file_system_id)
                .expect("inspect permit")
                .expect("permit");
            assert_eq!(
                store
                    .recover_removed_node_placement_authority_permit(
                        &file_system_id,
                        &cluster_id.to_string(),
                        &local_node_id,
                        &status.recovery_token,
                        true,
                    )
                    .expect("refuse present member"),
                DfsPlacementAuthorityRecoveryOutcome::Refused {
                    reason: DfsPlacementAuthorityRecoveryRefusal::HolderMembershipPresent { state },
                }
            );
            assert!(
                store
                    .placement_authority_permit_status(&file_system_id)
                    .expect("permit remains")
                    .is_some()
            );
        }

        let record = placement_authority_operator_test_record(
            Some(&cluster_id),
            Some(holder_node_id.clone()),
            Some("placement-removed-session"),
        );
        let mut kv = InProcessKvStore::new();
        put_placement_authority_operator_test_record(&mut kv, &file_system_id, &record);
        kv.put_serde(
            cluster_member_key(&holder_node_id),
            &placement_authority_operator_test_member(
                &cluster_id,
                &holder_node_id,
                ClusterMemberState::Removed,
            ),
            None,
        )
        .expect("put removed member");
        let mut store = DfsControlPlaneStore::new(&mut kv);
        let token = store
            .placement_authority_permit_status(&file_system_id)
            .expect("inspect removed permit")
            .expect("removed permit")
            .recovery_token;
        assert_eq!(
            store
                .recover_removed_node_placement_authority_permit(
                    &file_system_id,
                    &cluster_id.to_string(),
                    &local_node_id,
                    &token,
                    true,
                )
                .expect("clear explicitly removed member"),
            DfsPlacementAuthorityRecoveryOutcome::Cleared
        );
    }

    #[test]
    fn placement_operator_break_glass_is_explicit_and_limited_to_foreign_holders() {
        let file_system_id = file_system_id();
        let current_cluster_id = placement_authority_operator_test_cluster_id();
        let foreign_cluster_id =
            sinomo_core::Id::from_legacy_named("placement-operator-foreign-cluster")
                .expect("valid foreign cluster id");
        let holder_node_id = node("placement-operator-break-glass-holder");
        let local_node_id = node("placement-operator-break-glass-local");

        let cases = [placement_authority_operator_test_record(
            Some(&foreign_cluster_id),
            Some(holder_node_id.clone()),
            Some("foreign-session"),
        )];
        for record in cases {
            let mut kv = InProcessKvStore::new();
            put_placement_authority_operator_test_record(&mut kv, &file_system_id, &record);
            let mut store = DfsControlPlaneStore::new(&mut kv);
            let status = store
                .placement_authority_permit_status(&file_system_id)
                .expect("inspect break-glass permit")
                .expect("break-glass permit");
            let normal_reason = DfsPlacementAuthorityRecoveryRefusal::ForeignCluster;
            assert_eq!(
                store
                    .recover_removed_node_placement_authority_permit(
                        &file_system_id,
                        &current_cluster_id.to_string(),
                        &local_node_id,
                        &status.recovery_token,
                        true,
                    )
                    .expect("normal recovery refusal"),
                DfsPlacementAuthorityRecoveryOutcome::Refused {
                    reason: normal_reason,
                }
            );
            assert_eq!(
                store
                    .break_glass_recover_placement_authority_permit(
                        &file_system_id,
                        &current_cluster_id.to_string(),
                        &status.recovery_token,
                        "yes",
                    )
                    .expect("reject weak acknowledgement"),
                DfsPlacementAuthorityRecoveryOutcome::Refused {
                    reason: DfsPlacementAuthorityRecoveryRefusal::BreakGlassAcknowledgementRequired,
                }
            );
            assert_eq!(
                store
                    .break_glass_recover_placement_authority_permit(
                        &file_system_id,
                        &current_cluster_id.to_string(),
                        &status.recovery_token,
                        DFS_PLACEMENT_AUTHORITY_BREAK_GLASS_ACKNOWLEDGEMENT,
                    )
                    .expect("break-glass clear"),
                DfsPlacementAuthorityRecoveryOutcome::Cleared
            );
        }

        let same_cluster = placement_authority_operator_test_record(
            Some(&current_cluster_id),
            Some(holder_node_id),
            Some("same-cluster-session"),
        );
        let mut kv = InProcessKvStore::new();
        put_placement_authority_operator_test_record(&mut kv, &file_system_id, &same_cluster);
        let mut store = DfsControlPlaneStore::new(&mut kv);
        let token = store
            .placement_authority_permit_status(&file_system_id)
            .expect("inspect same-cluster permit")
            .expect("same-cluster permit")
            .recovery_token;
        assert_eq!(
            store
                .break_glass_recover_placement_authority_permit(
                    &file_system_id,
                    &current_cluster_id.to_string(),
                    &token,
                    DFS_PLACEMENT_AUTHORITY_BREAK_GLASS_ACKNOWLEDGEMENT,
                )
                .expect("same-cluster break-glass refusal"),
            DfsPlacementAuthorityRecoveryOutcome::Refused {
                reason: DfsPlacementAuthorityRecoveryRefusal::NormalRecoveryRequired,
            }
        );
    }

    #[test]
    fn placement_operator_recovery_exact_cas_preserves_replacement_and_readded_member() {
        let file_system_id = file_system_id();
        let cluster_id = placement_authority_operator_test_cluster_id();
        let holder_node_id = node("placement-operator-cas-holder");
        let local_node_id = node("placement-operator-cas-local");
        let old = placement_authority_operator_test_record(
            Some(&cluster_id),
            Some(holder_node_id.clone()),
            Some("old-session"),
        );
        let replacement = placement_authority_operator_test_record(
            Some(&cluster_id),
            Some(holder_node_id.clone()),
            Some("replacement-session"),
        );
        let key = placement_authority_permit_key(&file_system_id);
        let mut observed = InProcessKvStore::new();
        put_placement_authority_operator_test_record(&mut observed, &file_system_id, &old);
        let mut authoritative = InProcessKvStore::new();
        put_placement_authority_operator_test_record(
            &mut authoritative,
            &file_system_id,
            &replacement,
        );
        let mut client = StaleObservedAuthoritativeTxnKv {
            observed,
            authoritative,
        };
        let mut store = DfsControlPlaneStore::new(&mut client);
        let token = store
            .placement_authority_permit_status(&file_system_id)
            .expect("inspect stale permit")
            .expect("stale permit")
            .recovery_token;
        assert_eq!(
            store
                .recover_removed_node_placement_authority_permit(
                    &file_system_id,
                    &cluster_id.to_string(),
                    &local_node_id,
                    &token,
                    true,
                )
                .expect("replacement compare"),
            DfsPlacementAuthorityRecoveryOutcome::Changed
        );
        assert_eq!(
            client
                .authoritative
                .get(&key)
                .expect("replacement remains")
                .decode::<DfsPlacementAuthorityPermitRecord>()
                .expect("decode replacement"),
            replacement
        );

        let mut observed = InProcessKvStore::new();
        put_placement_authority_operator_test_record(&mut observed, &file_system_id, &old);
        let mut authoritative =
            InProcessKvStore::restore(observed.try_snapshot().expect("snapshot observed permit"))
                .expect("restore authoritative permit");
        authoritative
            .put_serde(
                cluster_member_key(&holder_node_id),
                &placement_authority_operator_test_member(
                    &cluster_id,
                    &holder_node_id,
                    ClusterMemberState::Active,
                ),
                None,
            )
            .expect("re-add holder membership");
        let mut client = StaleObservedAuthoritativeTxnKv {
            observed,
            authoritative,
        };
        let mut store = DfsControlPlaneStore::new(&mut client);
        let token = store
            .placement_authority_permit_status(&file_system_id)
            .expect("inspect before membership race")
            .expect("permit before membership race")
            .recovery_token;
        assert_eq!(
            store
                .recover_removed_node_placement_authority_permit(
                    &file_system_id,
                    &cluster_id.to_string(),
                    &local_node_id,
                    &token,
                    true,
                )
                .expect("membership race compare"),
            DfsPlacementAuthorityRecoveryOutcome::Changed
        );
        assert!(client.authoritative.get(&key).is_some());
    }

    #[test]
    fn key_helpers_use_control_plane_namespace() {
        let file_system_id = file_system_id();

        assert_eq!(
            file_system_config_key(&file_system_id),
            format!("/dfs/filesystems/{file_system_id}/config")
        );
        assert_eq!(
            metadata_shard_key(&file_system_id, "root/frag"),
            format!("/dfs/filesystems/{file_system_id}/metadata-shards/root_frag")
        );
        assert_eq!(
            metadata_shard_membership_handoff_key(&file_system_id, "root/frag"),
            format!("/dfs/filesystems/{file_system_id}/metadata-membership-handoffs/root_frag")
        );
        assert_eq!(
            metadata_shard_membership_handoff_proof_key(&file_system_id, "root/frag"),
            format!(
                "/dfs/filesystems/{file_system_id}/metadata-membership-handoff-proofs/root_frag"
            )
        );
    }

    #[test]
    fn store_puts_and_gets_only_control_plane_records() {
        let node_owner = node("node_owner");
        let node_replica = node("node_replica");
        let mut kv = InProcessKvStore::new();
        // These names are structural assignment DATA; admitted member bytes
        // come unchanged from their actual ordinary native source owner.
        copy_native_member_fixture_data(&mut kv, vec![node("node_owner"), node("node_replica")]);
        let file_system = DfsFileSystem {
            namespace_count_activation: None,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: file_system_id(),
            display_name: "repo".to_string(),
            owner_node_id: node_owner.clone(),
            subtree_policies: vec![DfsSubtreePolicy::new(
                ".",
                DfsObjectPolicy::ReplicatedEventual,
                1,
                2,
            )],
        };
        let file_system_config = DfsFileSystemConfig::from(&file_system);
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node_owner.clone(),
            replica_node_ids: vec![node_replica],
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };

        let mut control = DfsControlPlaneStore::new(&mut kv);
        control
            .put_file_system_config(&file_system_config)
            .expect("put filesystem config");
        control
            .put_metadata_shard(&shard)
            .expect("put shard config");

        assert_eq!(
            control
                .get_file_system_config(&file_system.file_system_id)
                .expect("get filesystem config"),
            Some(file_system_config)
        );
        assert_eq!(
            control
                .get_metadata_shard(&file_system.file_system_id, "root")
                .expect("get shard config"),
            Some(shard)
        );
    }

    #[tokio::test]
    async fn async_raw_metadata_shard_put_rejects_unproven_reconfiguration() {
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id(),
            shard_id: "root".into(),
            primary_node_id: node("node_async_raw_current_owner"),
            replica_node_ids: vec![node("node_async_raw_current_replica")],
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut data = InProcessKvStore::new();
        copy_native_member_fixture_data(
            &mut data,
            std::iter::once(shard.primary_node_id.clone())
                .chain(shard.replica_node_ids.clone())
                .collect(),
        );
        DfsControlPlaneAsyncStore::new(&mut data)
            .put_metadata_shard(&shard)
            .await
            .unwrap();
        let altered_membership = DfsMetadataShardConfig {
            replica_node_ids: vec![node("negative-raw-reconfiguration-outsider")],
            epoch: shard.epoch + 1,
            ..shard.clone()
        };
        let altered_epoch = DfsMetadataShardConfig {
            epoch: shard.epoch + 1,
            ..shard.clone()
        };
        for changed in [&altered_membership, &altered_epoch] {
            assert!(
                matches!(DfsControlPlaneAsyncStore::new(&mut data).put_metadata_shard(changed).await,
                Err(KvError::Serialization { detail }) if detail.contains("native membership handoff"))
            );
        }
        assert_eq!(
            DfsControlPlaneAsyncStore::new(&mut data)
                .get_metadata_shard(&shard.file_system_id, &shard.shard_id)
                .await
                .unwrap(),
            Some(shard)
        );
    }

    #[tokio::test]
    async fn async_initial_metadata_shard_requires_current_member_capabilities() {
        let packed = DfsMetadataShardConfig {
            file_system_id: file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: node("node_async_initial_packed_owner"),
            replica_node_ids: vec![node("node_async_initial_packed_replica")],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut raw_kv = InProcessKvStore::new();
        assert!(matches!(
            DfsControlPlaneAsyncStore::new(&mut raw_kv)
                .put_metadata_shard(&packed)
                .await,
            Err(KvError::Serialization { detail })
                if detail.contains("authoritative cluster member capability record")
        ));
        copy_current_prepare_member_fixture_data(
            &mut raw_kv,
            std::iter::once(&packed.primary_node_id).chain(packed.replica_node_ids.iter()),
        );
        DfsControlPlaneAsyncStore::new(&mut raw_kv)
            .put_metadata_shard(&packed)
            .await
            .expect("capability-proved async raw create");

        let mut cas_kv = InProcessKvStore::new();
        assert!(matches!(
            DfsControlPlaneAsyncStore::new(&mut cas_kv)
                .compare_and_put_metadata_shard(None, &packed)
                .await,
            Err(KvError::Serialization { detail })
                if detail.contains("authoritative cluster member capability record")
        ));
        copy_current_prepare_member_fixture_data(
            &mut cas_kv,
            std::iter::once(&packed.primary_node_id).chain(packed.replica_node_ids.iter()),
        );
        assert!(
            DfsControlPlaneAsyncStore::new(&mut cas_kv)
                .compare_and_put_metadata_shard(None, &packed)
                .await
                .expect("capability-proved async CAS create")
        );
    }

    #[test]
    fn metadata_shard_primary_transfer_preserves_epoch_compare_and_idempotence() {
        let name = "native-cp-transfer-negative-compare";
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
        let (mut data, shard) = native_control_plane_fixture_data(name, nodes);
        let promoted = shard.replica_node_ids[0].clone();
        assert!(
            DfsControlPlaneStore::new(&mut data)
                .transfer_metadata_shard_primary(
                    &shard.file_system_id,
                    &shard.shard_id,
                    shard.epoch.saturating_sub(1),
                    promoted.clone()
                )
                .unwrap()
                .is_none()
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut data)
                .transfer_metadata_shard_primary(
                    &shard.file_system_id,
                    &shard.shard_id,
                    shard.epoch,
                    shard.primary_node_id.clone()
                )
                .unwrap(),
            Some(shard.clone())
        );
        assert!(
            matches!(DfsControlPlaneStore::new(&mut data).transfer_metadata_shard_primary(
            &shard.file_system_id, &shard.shard_id, shard.epoch, promoted),
            Err(KvError::Serialization { detail }) if detail.contains("current operation-bound DFS prefix fence"))
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut data)
                .get_metadata_shard(&shard.file_system_id, &shard.shard_id)
                .unwrap(),
            Some(shard)
        );
    }

    #[test]
    fn metadata_shard_primary_transfer_rejects_epoch_exhaustion() {
        let name = "native-cp-transfer-negative-exhaustion";
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
        let (mut data, mut shard) = native_control_plane_fixture_data(name, nodes);
        let promoted = shard.replica_node_ids[0].clone();
        // Negative schema input on the captured row; no live assignment is
        // published at the impossible exhausted test epoch.
        shard.epoch = u64::MAX;
        data.put_serde(
            metadata_shard_key(&shard.file_system_id, &shard.shard_id),
            &shard,
            None,
        )
        .unwrap();
        assert!(
            matches!(DfsControlPlaneStore::new(&mut data).transfer_metadata_shard_primary(
            &shard.file_system_id, &shard.shard_id, u64::MAX, promoted),
            Err(KvError::Serialization { detail }) if detail.contains("epoch is exhausted"))
        );
    }

    #[tokio::test]
    async fn async_metadata_shard_primary_transfer_requires_dfs_handoff() {
        let name = "native-cp-async-transfer-negative";
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
        let (mut data, shard) = native_control_plane_fixture_data(name, nodes);
        assert!(
            matches!(DfsControlPlaneAsyncStore::new(&mut data).transfer_metadata_shard_primary(
            &shard.file_system_id, &shard.shard_id, shard.epoch, shard.replica_node_ids[0].clone()).await,
            Err(KvError::Serialization { detail }) if detail.contains("current operation-bound DFS prefix fence"))
        );
        assert_eq!(
            DfsControlPlaneAsyncStore::new(&mut data)
                .get_metadata_shard(&shard.file_system_id, &shard.shard_id)
                .await
                .unwrap(),
            Some(shard)
        );
    }

    #[test]
    fn metadata_shard_compare_and_put_rejects_stale_expected_value() {
        let (mut data, before, after) =
            native_control_plane_handoff_fixture_data("native-cp-stale-cas-data");
        assert!(
            !DfsControlPlaneStore::new(&mut data)
                .compare_and_put_metadata_shard(Some(&before), &after)
                .unwrap()
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut data)
                .get_metadata_shard(&after.file_system_id, &after.shard_id)
                .unwrap(),
            Some(after)
        );
    }

    #[test]
    fn metadata_shard_compare_and_put_parks_direct_primary_change() {
        let name = "native-cp-unproven-role-change-negative";
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
        let (mut data, current) = native_control_plane_fixture_data(name, nodes);
        let changed = DfsMetadataShardConfig {
            primary_node_id: current.replica_node_ids[0].clone(),
            replica_node_ids: vec![current.primary_node_id.clone()],
            epoch: current.epoch + 1,
            ..current.clone()
        };
        assert!(
            !DfsControlPlaneStore::new(&mut data)
                .compare_and_put_metadata_shard(Some(&current), &changed)
                .unwrap()
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut data)
                .get_metadata_shard(&current.file_system_id, &current.shard_id)
                .unwrap(),
            Some(current)
        );
    }

    #[test]
    fn primary_handoff_request_is_idempotent_and_does_not_activate_without_proof() {
        let (runtime, requester, current) =
            crate::test_support::current_native_metadata_service(
                "native-cp-idempotent-role-request",
                3,
                "/",
            )
            .unwrap();
        drop(requester);
        // The shard fence guard belongs to the store-backed owning service.
        let mut source = runtime.metadata_primary_service("/").unwrap();
        // SAME normal source serialization gate: pause background freeze while
        // observing the request producer. This guard grants no publication.
        let gate = source
            .metadata_shard_fence_guard(&current.file_system_id, &current.shard_id)
            .unwrap();
        let control = source.control_plane.as_mut().unwrap();
        let mut store = DfsControlPlaneStore::new(control.as_mut());
        let prior_proof = store
            .get_metadata_shard_membership_handoff_proof(&current.file_system_id, &current.shard_id)
            .unwrap();
        assert!(
            store
                .request_metadata_shard_primary_handoff(&current, &node("native-cp-outsider"))
                .is_err()
        );
        let first = store
            .request_metadata_shard_primary_handoff(&current, &current.replica_node_ids[0])
            .unwrap()
            .unwrap();
        let retry = store
            .request_metadata_shard_primary_handoff(&current, &current.replica_node_ids[0])
            .unwrap()
            .unwrap();
        assert_eq!(first, retry);
        assert_eq!(
            first.desired_shard,
            metadata_shard_primary_handoff_target(&current, &current.replica_node_ids[0],).unwrap()
        );
        assert_eq!(first.desired_shard.epoch, current.epoch + 1);
        assert!(first.retiring_primary_fence.is_none());
        assert_eq!(
            store
                .get_metadata_shard(&current.file_system_id, &current.shard_id)
                .unwrap(),
            Some(current.clone())
        );
        let observed_proof = store
            .get_metadata_shard_membership_handoff_proof(&current.file_system_id, &current.shard_id)
            .unwrap();
        assert_eq!(
            observed_proof, prior_proof,
            "the new unfenced request preserves the complete earlier native proof"
        );
        assert!(
            observed_proof
                .as_ref()
                .is_none_or(|proof| proof.handoff_id != first.handoff_id),
            "the new request has no exact proof or activation at this held source cut"
        );
        assert!(
            store
                .request_metadata_shard_primary_handoff(&current, &current.replica_node_ids[1])
                .unwrap()
                .is_none()
        );
        let mut stale = current.clone();
        stale.epoch += 1;
        assert!(
            store
                .request_metadata_shard_primary_handoff(&stale, &current.replica_node_ids[0])
                .unwrap()
                .is_none()
        );
        let mut exhausted = current.clone();
        exhausted.epoch = u64::MAX;
        assert!(
            store
                .request_metadata_shard_primary_handoff(&exhausted, &current.replica_node_ids[0])
                .is_err()
        );
        assert_eq!(
            store
                .get_metadata_shard(&current.file_system_id, &current.shard_id)
                .unwrap(),
            Some(current)
        );
        drop(gate);
        drop(source);
        // All request-only assertions above were observed while the real source
        // fence prevented activation. Finish that same requested transition
        // before shutting down its native owner and background reconciliation.
        let desired = first.desired_shard;
        let activated = runtime
            .reassign_metadata_shard(
                &desired.file_system_id,
                &desired.shard_id,
                desired.primary_node_id.clone(),
                desired.replica_node_ids.clone(),
            )
            .unwrap();
        assert_eq!(activated, desired);
        runtime.close().unwrap();
    }

    #[test]
    fn metadata_shard_primary_role_handoff_requires_exact_existing_member_rotation() {
        let (runtime, requester, current) =
            crate::test_support::current_native_metadata_service(
                "native-cp-exact-role-rotation",
                3,
                "/",
            )
            .unwrap();
        drop(requester);
        // The shard fence guard belongs to the store-backed owning service.
        let mut source = runtime.metadata_primary_service("/").unwrap();
        let desired =
            metadata_shard_primary_handoff_target(&current, &current.replica_node_ids[0]).unwrap();
        let mut wrong_order = desired.clone();
        wrong_order.replica_node_ids.reverse();
        let gate = source
            .metadata_shard_fence_guard(&current.file_system_id, &current.shard_id)
            .unwrap();
        let control = source.control_plane.as_mut().unwrap();
        let mut store = DfsControlPlaneStore::new(control.as_mut());
        for invalid in [
            DfsMetadataShardConfig {
                primary_node_id: node("native-cp-role-outsider"),
                ..desired.clone()
            },
            DfsMetadataShardConfig {
                replica_node_ids: vec![current.primary_node_id.clone()],
                ..desired.clone()
            },
            wrong_order,
        ] {
            assert!(matches!(
                store.begin_metadata_shard_membership_handoff(&current, &invalid),
                Err(KvError::Serialization { .. })
            ));
        }
        let handoff = store
            .begin_metadata_shard_membership_handoff(&current, &desired)
            .unwrap()
            .unwrap();
        assert_eq!(handoff.retiring_shard, current);
        assert_eq!(handoff.desired_shard, desired);
        assert_eq!(
            metadata_shard_configured_node_ids(&handoff.retiring_shard),
            metadata_shard_configured_node_ids(&handoff.desired_shard)
        );
        assert_eq!(
            store
                .get_metadata_shard(&current.file_system_id, &current.shard_id)
                .unwrap(),
            Some(current)
        );
        drop(gate);
        drop(source);
        // All request-only assertions above were observed while the real source
        // fence prevented activation. Finish that same requested transition
        // before shutting down its native owner and background reconciliation.
        let desired = handoff.desired_shard;
        let activated = runtime
            .reassign_metadata_shard(
                &desired.file_system_id,
                &desired.shard_id,
                desired.primary_node_id.clone(),
                desired.replica_node_ids.clone(),
            )
            .unwrap();
        assert_eq!(activated, desired);
        runtime.close().unwrap();
    }

    #[test]
    fn metadata_shard_primary_handoff_joint_quorum_tolerates_one_offline_replica() {
        let (mut runtime, mut requester, current) =
            crate::test_support::current_native_metadata_service(
                "native-cp-offline-joint-handoff",
                4,
                "/",
            )
            .unwrap();
        assert_eq!(metadata_shard_configured_node_ids(&current).len(), 4);
        assert_eq!(current.replica_node_ids.len(), 3);
        requester
            .posix_mkdir("/native-cp-before-offline-joint")
            .unwrap();
        let before = requester
            .posix_stat_distributed("/native-cp-before-offline-joint")
            .unwrap();
        drop(requester);
        // Fencing and handoff reconciliation belong to the store-backed owner.
        let mut source = runtime.metadata_primary_service("/").unwrap();
        let promoted = current.replica_node_ids[0].clone();
        let offline = current.replica_node_ids.last().unwrap().clone();
        assert_ne!(offline, promoted);
        assert_ne!(offline, current.primary_node_id);
        runtime.stop_node(&offline).unwrap();
        let desired = metadata_shard_primary_handoff_target(&current, &promoted).unwrap();
        let handoff_key =
            metadata_shard_membership_handoff_key(&current.file_system_id, &current.shard_id);
        let proof_key =
            metadata_shard_membership_handoff_proof_key(&current.file_system_id, &current.shard_id);
        let floor_key =
            metadata_shard_operation_floor_key(&current.file_system_id, &current.shard_id);
        let captured = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&captured);
        let capture = runtime
            .native_kv_request_hook(&current.primary_node_id, move |request| {
                let proof = request.txn.success.iter().find_map(|op| match op {
                    KvOp::Put { key, value, .. } if key == &proof_key => Some(value),
                    _ => None,
                });
                let pending = request
                    .txn
                    .compares
                    .iter()
                    .find_map(|compare| match compare {
                        KvCompare::ValueEquals { key, value } if key == &handoff_key => Some(value),
                        _ => None,
                    });
                if let (Some(proof), Some(pending)) = (proof, pending) {
                    let proof: DfsMetadataShardMembershipHandoffProof =
                        sinomo_core::object::serde_ext::from_bytes(proof)
                            .expect("actual normal activation proof request");
                    let pending: DfsMetadataShardMembershipHandoff =
                        sinomo_core::object::serde_ext::from_bytes(pending)
                            .expect("same actual transaction's fenced pending compare");
                    let floor = request
                        .txn
                        .compares
                        .iter()
                        .find_map(|compare| match compare {
                            KvCompare::ValueEquals { key, value } if key == &floor_key => Some(
                                sinomo_core::object::serde_ext::from_bytes::<
                                    DfsMetadataShardOperationFloor,
                                >(value)
                                .expect("same actual transaction's floor compare"),
                            ),
                            _ => None,
                        });
                    let mut seen = seen.lock().unwrap();
                    assert!(seen.len() < 32, "bounded actual handoff capture");
                    seen.push((pending, proof, floor));
                }
                Ok(())
            })
            .unwrap();
        let gate = source
            .metadata_shard_fence_guard(&current.file_system_id, &current.shard_id)
            .unwrap();
        let requested = DfsControlPlaneStore::new(source.control_plane.as_mut().unwrap().as_mut())
            .request_metadata_shard_primary_handoff(&current, &promoted)
            .unwrap()
            .unwrap();
        assert_eq!(requested.desired_shard, desired);
        drop(gate);
        let configurations = runtime.metadata_shards().unwrap();
        source
            .reconcile_pending_metadata_shard_membership_handoffs(
                &configurations,
                std::time::Duration::from_secs(5),
            )
            .unwrap();
        let store = DfsControlPlaneStore::new(source.control_plane.as_mut().unwrap().as_mut());
        let active = store
            .get_metadata_shard(&current.file_system_id, &current.shard_id)
            .unwrap()
            .unwrap();
        let proof = store
            .get_metadata_shard_membership_handoff_proof(&current.file_system_id, &current.shard_id)
            .unwrap()
            .unwrap();
        assert_eq!(active, desired);
        assert_eq!(proof.retiring_shard, current);
        assert_eq!(proof.desired_shard, desired);
        assert!(!proof.acknowledged_node_ids.contains(&offline));
        assert_eq!(proof.acknowledged_node_ids.len(), 3);
        assert!(
            proof
                .acknowledged_node_ids
                .contains(&current.primary_node_id)
        );
        assert!(proof.acknowledged_node_ids.contains(&promoted));
        assert!(
            store
                .get_metadata_shard_membership_handoff(&current.file_system_id, &current.shard_id)
                .unwrap()
                .is_none()
        );
        // Negative-only validation from the SAME successful producer request.
        // This copied proof can never activate an assignment or create an ACK.
        let (pending, original, floor) = captured
            .lock()
            .unwrap()
            .iter()
            .find(|(_, actual, _)| actual == &proof)
            .unwrap()
            .clone();
        assert_eq!(original, proof);
        let mut missing_promoted = original;
        missing_promoted
            .acknowledged_node_ids
            .retain(|node| node != &promoted);
        assert!(matches!(normalize_metadata_shard_membership_handoff_proof(
            &pending, &missing_promoted, floor.as_ref(),
        ), Err(KvError::Serialization { detail }) if detail.contains("both primaries")));
        let mut reader = runtime.client(&promoted).unwrap();
        let after = reader
            .posix_stat_distributed("/native-cp-before-offline-joint")
            .unwrap();
        assert_eq!(
            (after.object_id, after.version),
            (before.object_id, before.version)
        );
        drop(reader);
        drop(capture);
        drop(source);
        runtime.close().unwrap();
    }

    #[test]
    fn async_metadata_shard_primary_role_handoff_accepts_exact_rotation() {
        let (runtime, requester, current) = crate::test_support::current_native_metadata_service(
            "native-cp-async-role-rotation",
            2,
            "/",
        )
        .unwrap();
        drop(requester);
        // The shard fence guard belongs to the store-backed owning service.
        let mut source = runtime.metadata_primary_service("/").unwrap();
        let desired =
            metadata_shard_primary_handoff_target(&current, &current.replica_node_ids[0]).unwrap();
        let gate = source
            .metadata_shard_fence_guard(&current.file_system_id, &current.shard_id)
            .unwrap();
        let mut control = source.control_plane.take().unwrap();
        // Drive the actual native async CP API on the existing shared executor.
        let handoff = runtime.runtime_handle().block_on(async {
            DfsControlPlaneAsyncStore::new(
                control.async_client().expect("native async CP capability"),
            )
            .request_metadata_shard_primary_handoff(&current, &desired.primary_node_id)
            .await
            .unwrap()
            .unwrap()
        });
        assert_eq!(handoff.retiring_shard, current);
        assert_eq!(handoff.desired_shard, desired);
        assert_eq!(
            DfsControlPlaneStore::new(control.as_mut())
                .get_metadata_shard(&current.file_system_id, &current.shard_id,)
                .unwrap(),
            Some(current)
        );
        drop(gate);
        drop(control);
        drop(source);
        runtime.close().unwrap();
    }

    /// Immutable schema/comparison DATA captured from a completed ordinary
    /// native owner. This copied store has no live native transaction proof,
    /// publication capability or peer ACK and cannot certify V7 publication.
    fn native_control_plane_fixture_data(
        name: &str,
        nodes: Vec<NodeId>,
    ) -> (InProcessKvStore, DfsMetadataShardConfig) {
        assert!(!nodes.is_empty());
        let name = name.to_string();
        // Some callers run on a current-thread Tokio test executor. Native Main
        // startup uses its existing shared multi-thread executor independently.
        std::thread::spawn(move || {
            let mut runtime =
                crate::test_runtime::DfsTestRuntime::new(&name, nodes[0].clone()).unwrap();
            for node in &nodes[1..] {
                runtime.define_node(node.clone()).unwrap();
            }
            runtime.start_nodes().unwrap();
            let shard = runtime.root_shard().unwrap();
            // This fixture captures control-plane DATA only. Borrow the real
            // registered Core client directly; opening a DFS mutation service
            // would add an unrelated, concurrently refreshing projection gate.
            let context = runtime.component_context(&nodes[0]).unwrap();
            assert!(
                crate::runtime_host::registered_component_cluster(&context)
                    .unwrap()
                    .is_some()
            );
            let paths = sinomo_core::network::ClusterPaths::new(runtime.data_dir(&nodes[0]));
            let mut source = crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(
                &paths,
                &runtime.runtime_handle(),
            );
            let mut keys = nodes
                .iter()
                .flat_map(|node| {
                    [
                        cluster_member_key(node),
                        cluster_capability_policy_key(node),
                    ]
                })
                .collect::<Vec<_>>();
            keys.push(metadata_shard_key(&shard.file_system_id, &shard.shard_id));
            let entries = source.get_authoritative_consistent(&keys).unwrap();
            assert_eq!(entries.len(), keys.len());
            let mut data = InProcessKvStore::new();
            for (key, entry) in keys.into_iter().zip(entries) {
                if let Some(entry) = entry {
                    assert_eq!(entry.key, key);
                    data.put(entry.key, entry.value, None).unwrap();
                }
            }
            for node in &nodes {
                let member = data
                    .get(&cluster_member_key(node))
                    .unwrap()
                    .decode::<ClusterMemberRecord>()
                    .unwrap();
                assert_eq!(member.node_id, *node);
                assert!(member.state.is_authorized());
                // Assert produced capabilities verbatim; never add a healthy
                // capability to satisfy the gate under test.
                assert!(
                    member
                        .capabilities
                        .iter()
                        .any(|capability| capability.as_str()
                            == DFS_METADATA_PREPARED_OPERATION_V7_CAPABILITY)
                );
            }
            assert_eq!(
                data.get(&metadata_shard_key(&shard.file_system_id, &shard.shard_id))
                    .unwrap()
                    .decode::<DfsMetadataShardConfig>()
                    .unwrap(),
                shard
            );
            drop(source);
            drop(context);
            runtime.close().unwrap();
            (data, shard)
        })
        .join()
        .expect("actual native control-plane DATA producer")
    }

    #[test]
    fn ordinary_filesystem_updates_preserve_and_cannot_author_count_activation() {
        // Configuration DATA only; these comparisons grant no maintenance CAS.
        let mut current = DfsFileSystemConfig::from(&crate::DfsFileSystem::sifs(node("count-config-owner")));
        current.namespace_count_activation = Some(crate::DfsNamespaceCountActivation {
            schema_version: crate::DFS_NAMESPACE_COUNT_ACTIVATION_SCHEMA_VERSION,
            initialization_id: "original-complete-cut".into(), cohort_id: "original-cohort".into(),
            activation_digest: format!("blake3:{}", "a".repeat(64)),
        });
        let mut ordinary = current.clone();
        ordinary.namespace_count_activation = None;
        ordinary.display_name = "renamed".into();
        let merged = merge_file_system_config_publication(Some(&current), &ordinary);
        assert_eq!(merged.namespace_count_activation, current.namespace_count_activation);
        assert_eq!(merged.display_name, "renamed");
        assert_eq!(merge_file_system_config_defaults(Some(&current), &ordinary).namespace_count_activation,
            current.namespace_count_activation);
        assert!(validate_ordinary_namespace_count_activation(Some(&current), &merged).is_ok());
        assert!(validate_ordinary_namespace_count_activation(None, &current).is_err());
        assert!(validate_ordinary_namespace_count_activation(Some(&current), &ordinary).is_err());
        let mut replacement = current.clone();
        replacement.namespace_count_activation.as_mut().unwrap().initialization_id = "another-cut".into();
        assert!(validate_ordinary_namespace_count_activation(Some(&current), &replacement).is_err());
    }

    fn copy_native_member_fixture_data(kv: &mut InProcessKvStore, nodes: Vec<NodeId>) {
        let name = format!("native-cp-member-data-{}", nodes[0]);
        let (captured, _) = native_control_plane_fixture_data(&name, nodes.clone());
        for node in nodes {
            for key in [
                cluster_member_key(&node),
                cluster_capability_policy_key(&node),
            ] {
                if let Some(entry) = captured.get(&key) {
                    kv.put(entry.key.clone(), entry.value.clone(), None)
                        .unwrap();
                }
            }
        }
    }

    fn native_control_plane_handoff_fixture_data(
        name: &str,
    ) -> (
        InProcessKvStore,
        DfsMetadataShardConfig,
        DfsMetadataShardConfig,
    ) {
        let name = name.to_string();
        std::thread::spawn(move || {
            let (runtime, nodes) =
                crate::test_runtime::DfsTestRuntime::start_cluster(&name, 2).unwrap();
            let before = runtime.root_shard().unwrap();
            let promoted = nodes
                .iter()
                .find(|node| **node != before.primary_node_id)
                .unwrap()
                .clone();
            let after = runtime
                .reassign_metadata_shard(
                    &before.file_system_id,
                    &before.shard_id,
                    promoted,
                    vec![before.primary_node_id.clone()],
                )
                .unwrap();
            let source = runtime.shared_control_plane().unwrap();
            let key = metadata_shard_key(&after.file_system_id, &after.shard_id);
            let entry = source
                .with_client(|client| client.get_authoritative_consistent(&[key.clone()]))
                .unwrap()
                .pop()
                .unwrap()
                .unwrap();
            assert_eq!(entry.decode::<DfsMetadataShardConfig>().unwrap(), after);
            assert_ne!(before.primary_node_id, after.primary_node_id);
            let mut data = InProcessKvStore::new();
            data.put(entry.key, entry.value, None).unwrap();
            drop(source);
            runtime.close().unwrap();
            (data, before, after)
        })
        .join()
        .expect("actual current handoff source DATA")
    }

    fn native_placement_activity_fixture_data(
        name: &str,
    ) -> (InProcessKvStore, DfsPlacementReconciliationJob) {
        let name = name.to_string();
        std::thread::spawn(move || {
            let (runtime, nodes) =
                crate::test_runtime::DfsTestRuntime::start_cluster(&name, 1).unwrap();
            let mut policy =
                crate::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1).unwrap();
            policy.required_node_ids = nodes.clone();
            let produced = runtime
                .apply_placement_policy(
                    &nodes[0],
                    "/services/native-activity-data",
                    crate::DfsPlacementScope::Subtree,
                    policy,
                )
                .unwrap();
            let source = runtime.shared_control_plane().unwrap();
            let keys = [
                file_system_config_key(&produced.file_system_id),
                metadata_shard_key(&produced.file_system_id, "root"),
                placement_reconciliation_key(&produced.file_system_id, &produced.job_id),
                placement_reconciliation_queue_key(&produced.file_system_id),
                placement_reconciliation_activity_index_key(&produced.file_system_id),
                placement_reconciliation_activity_record_key(
                    &produced.file_system_id,
                    &produced.job_id,
                ),
            ];
            let entries = source
                .with_client(|client| client.get_authoritative_consistent(&keys))
                .unwrap();
            assert_eq!(entries.len(), keys.len());
            let mut data = InProcessKvStore::new();
            for (key, entry) in keys.into_iter().zip(entries) {
                if let Some(entry) = entry {
                    assert_eq!(entry.key, key);
                    data.put(entry.key, entry.value, None).unwrap();
                }
            }
            let actual = data
                .get(&placement_reconciliation_key(
                    &produced.file_system_id,
                    &produced.job_id,
                ))
                .unwrap()
                .decode::<DfsPlacementReconciliationJob>()
                .unwrap();
            assert_eq!(actual.job_id, produced.job_id);
            assert!(
                data.get(&placement_reconciliation_activity_record_key(
                    &actual.file_system_id,
                    &actual.job_id
                ))
                .is_some()
            );
            drop(source);
            runtime.close().unwrap();
            (data, actual)
        })
        .join()
        .expect("actual normal placement Apply activity DATA")
    }

    fn exact_authority_test_fixture(
        test_name: &str,
    ) -> (ExactAuthorityOnlyKv, DfsMetadataShardConfig) {
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(test_name, 1);
        let (inner, shard) = native_control_plane_fixture_data(test_name, nodes);
        (ExactAuthorityOnlyKv::new(inner), shard)
    }

    fn assert_one_exact_authority_read(
        client: &ExactAuthorityOnlyKv,
        shard: &DfsMetadataShardConfig,
    ) {
        assert_eq!(client.refresh_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            client.coherent_observation_calls.load(Ordering::SeqCst),
            0,
            "authority validation must not fall back to cached point reads"
        );
        assert_eq!(
            client.authoritative_exact_set_calls.load(Ordering::SeqCst),
            1,
            "one operation fence must issue exactly one exact-key-set read"
        );
        assert_eq!(
            client
                .requested_exact_sets
                .lock()
                .expect("exact authority requests")
                .as_slice(),
            &[vec![
                metadata_shard_key(&shard.file_system_id, &shard.shard_id),
                metadata_shard_membership_handoff_key(&shard.file_system_id, &shard.shard_id,),
            ]]
        );
    }

    #[test]
    fn metadata_shard_authority_uses_one_exact_set_without_full_snapshot_refresh() {
        let (mut client, shard) = exact_authority_test_fixture("sync-exact-authority");
        assert!(
            DfsControlPlaneStore::new(&mut client)
                .metadata_shard_authority_is_current(&shard)
                .expect("exact operation-bound authority validation")
        );
        assert_one_exact_authority_read(&client, &shard);
    }

    #[test]
    fn metadata_shard_authority_retries_transient_exact_set_failures() {
        let (mut client, shard) = exact_authority_test_fixture("sync-exact-authority-retry");
        client
            .authoritative_errors
            .lock()
            .expect("scripted exact authority errors")
            .extend([
                KvError::AuthorityUnavailable {
                    detail: "temporary exact authority loss".to_string(),
                },
                KvError::Io {
                    path: "test-cluster-authoritative-view".to_string(),
                    detail: "resident Raft view changed after ReadIndex quorum; retry the read"
                        .to_string(),
                },
            ]);

        assert!(
            DfsControlPlaneStore::new(&mut client)
                .metadata_shard_authority_is_current(&shard)
                .expect("transient exact authority failures must settle")
        );
        assert_eq!(
            client.authoritative_exact_set_calls.load(Ordering::SeqCst),
            3
        );
        assert_eq!(client.refresh_calls.load(Ordering::SeqCst), 0);
        assert_eq!(client.coherent_observation_calls.load(Ordering::SeqCst), 0);
        let deadlines = client
            .authoritative_deadlines
            .lock()
            .expect("exact authority deadlines");
        assert_eq!(deadlines.len(), 3);
        assert!(deadlines.iter().all(|deadline| *deadline == deadlines[0]));
    }

    #[test]
    fn metadata_shard_authority_expired_deadline_starts_no_exact_read() {
        let (mut client, shard) = exact_authority_test_fixture("sync-exact-authority-expired");
        let expired = Instant::now()
            .checked_sub(std::time::Duration::from_millis(1))
            .expect("expired authority deadline");

        assert!(matches!(
            DfsControlPlaneStore::new(&mut client)
                .metadata_shard_authority_is_current_before(&shard, Some(expired)),
            Err(KvError::AuthorityUnavailable { detail })
                if detail.contains("deadline elapsed")
        ));
        assert_eq!(
            client.authoritative_exact_set_calls.load(Ordering::SeqCst),
            0
        );
        assert!(
            client
                .authoritative_deadlines
                .lock()
                .expect("exact authority deadlines")
                .is_empty()
        );
    }

    #[test]
    fn metadata_shard_authority_semantic_rejection_does_not_retry() {
        let (mut stale_client, stale_shard) =
            exact_authority_test_fixture("sync-exact-authority-stale");
        let successor = DfsMetadataShardConfig {
            epoch: stale_shard.epoch.saturating_add(1),
            ..stale_shard.clone()
        };
        stale_client
            .inner
            .put_serde(
                metadata_shard_key(&stale_shard.file_system_id, &stale_shard.shard_id),
                &successor,
                None,
            )
            .expect("negatively alter captured assignment DATA");
        assert!(
            !DfsControlPlaneStore::new(&mut stale_client)
                .metadata_shard_authority_is_current(&stale_shard)
                .expect("stale assignment is a semantic rejection")
        );
        assert_eq!(
            stale_client
                .authoritative_exact_set_calls
                .load(Ordering::SeqCst),
            1
        );

        let (mut handoff_client, handoff_shard) =
            exact_authority_test_fixture("sync-exact-authority-handoff");
        handoff_client
            .inner
            .put(
                metadata_shard_membership_handoff_key(
                    &handoff_shard.file_system_id,
                    &handoff_shard.shard_id,
                ),
                vec![1],
                None,
            )
            .expect("install handoff marker");
        assert!(
            !DfsControlPlaneStore::new(&mut handoff_client)
                .metadata_shard_authority_is_current(&handoff_shard)
                .expect("pending handoff is a semantic rejection")
        );
        assert_eq!(
            handoff_client
                .authoritative_exact_set_calls
                .load(Ordering::SeqCst),
            1
        );
    }

    #[test]
    fn metadata_shard_authority_does_not_retry_terminal_io() {
        let (mut client, shard) = exact_authority_test_fixture("sync-exact-authority-terminal-io");
        let terminal = KvError::Io {
            path: "test-disk".to_string(),
            detail: "permission denied".to_string(),
        };
        client
            .authoritative_errors
            .lock()
            .expect("scripted exact authority errors")
            .push_back(terminal.clone());

        assert_eq!(
            DfsControlPlaneStore::new(&mut client)
                .metadata_shard_authority_is_current(&shard)
                .expect_err("ordinary IO must remain terminal"),
            terminal
        );
        assert_eq!(
            client.authoritative_exact_set_calls.load(Ordering::SeqCst),
            1
        );
    }

    #[test]
    fn metadata_shard_authority_bounds_transient_exact_set_retries() {
        let (mut client, shard) = exact_authority_test_fixture("sync-exact-authority-exhausted");
        client
            .authoritative_errors
            .lock()
            .expect("scripted exact authority errors")
            .extend((0..METADATA_SHARD_AUTHORITY_READ_RETRIES).map(|_| {
                KvError::AuthorityUnavailable {
                    detail: "temporary exact authority loss".to_string(),
                }
            }));

        assert!(matches!(
            DfsControlPlaneStore::new(&mut client).metadata_shard_authority_is_current(&shard),
            Err(KvError::AuthorityUnavailable { .. })
        ));
        assert_eq!(
            client.authoritative_exact_set_calls.load(Ordering::SeqCst),
            METADATA_SHARD_AUTHORITY_READ_RETRIES
        );
    }

    #[tokio::test]
    async fn async_metadata_shard_authority_uses_one_exact_set_without_full_snapshot_refresh() {
        let (mut client, shard) = exact_authority_test_fixture("async-exact-authority");
        assert!(
            DfsControlPlaneAsyncStore::new(&mut client)
                .metadata_shard_authority_is_current(&shard)
                .await
                .expect("async exact operation-bound authority validation")
        );
        assert_one_exact_authority_read(&client, &shard);
    }

    #[tokio::test]
    async fn async_metadata_shard_authority_retries_transient_exact_set_failure() {
        let (mut client, shard) = exact_authority_test_fixture("async-exact-authority-retry");
        client
            .authoritative_errors
            .lock()
            .expect("scripted exact authority errors")
            .push_back(KvError::AuthorityUnavailable {
                detail: "temporary async exact authority loss".to_string(),
            });

        assert!(
            DfsControlPlaneAsyncStore::new(&mut client)
                .metadata_shard_authority_is_current(&shard)
                .await
                .expect("async transient exact authority failure must settle")
        );
        assert_eq!(
            client.authoritative_exact_set_calls.load(Ordering::SeqCst),
            2
        );
        assert_eq!(client.refresh_calls.load(Ordering::SeqCst), 0);
        assert_eq!(client.coherent_observation_calls.load(Ordering::SeqCst), 0);
        let deadlines = client
            .authoritative_deadlines
            .lock()
            .expect("exact authority deadlines");
        assert_eq!(deadlines.len(), 2);
        assert!(deadlines.iter().all(|deadline| *deadline == deadlines[0]));
    }

    #[test]
    fn metadata_shard_authority_check_rejects_stale_assignment_without_writing_kv() {
        let (mut data, before, _after) =
            native_control_plane_handoff_fixture_data("native-cp-stale-read-data");
        let revision = data.revision();
        assert!(
            !DfsControlPlaneStore::new(&mut data)
                .metadata_shard_authority_is_current(&before)
                .unwrap()
        );
        assert_eq!(data.revision(), revision);
    }

    struct NativeFloorFixture {
        runtime: crate::test_support::DfsTestRuntime,
        service: crate::NetworkedDfsService<crate::DfsNodeTransport>,
        shard: DfsMetadataShardConfig,
        control: Box<dyn DfsControlPlaneKvClient>,
        observed_create_command: Arc<Mutex<Option<crate::DfsMessageCommand>>>,
    }
    impl NativeFloorFixture {
        fn new(name: &str) -> Self {
            Self::with_node_count(name, 3)
        }
        fn with_node_count(name: &str, node_count: usize) -> Self {
            let (runtime, _) = crate::test_support::DfsTestRuntime::start_cluster(name, node_count)
                .expect("start actual native floor owners");
            let shard = runtime.root_shard().expect("actual root assignment");
            for path in [
                "/native-floor-a.bin",
                "/native-floor-b.bin",
                "/native-floor-c.bin",
            ] {
                let mut policy =
                    crate::DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1)
                        .unwrap();
                policy.required_node_ids = vec![shard.primary_node_id.clone()];
                runtime
                    .apply_placement_policy(
                        &shard.primary_node_id,
                        path,
                        DfsPlacementScope::SelfOnly,
                        policy,
                    )
                    .expect("normal W1 placement Apply before writer open");
            }
            let service = runtime.client(&shard.primary_node_id).unwrap();
            // This actual peer's CP remains healthy while the source owner's
            // floor read is denied. It supplies the same genuine Raft producer.
            let mut peer = runtime.client(&shard.replica_node_ids[0]).unwrap();
            let inner = peer
                .control_plane
                .take()
                .expect("actual peer control plane");
            drop(peer);
            Self {
                runtime,
                service,
                shard,
                control: inner,
                observed_create_command: Arc::new(Mutex::new(None)),
            }
        }
        fn deny_last_metadata_participant(&self) {
            let unavailable = self.shard.replica_node_ids.last().unwrap().clone();
            for route in [
                crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
                crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
            ] {
                self.runtime
                    .install_fault(crate::test_support::DfsTestFaultRule {
                        source: Some(self.shard.primary_node_id.clone()),
                        target: Some(unavailable.clone()),
                        route: Some(route.into()),
                        call_id: None,
                        origin: None,
                        remaining_hits: 32,
                        action: crate::test_support::DfsTestFaultAction::BudgetExhausted,
                    });
            }
        }
        fn floor_key(&self) -> String {
            metadata_shard_operation_floor_key(&self.shard.file_system_id, &self.shard.shard_id)
        }
        /// The primary node's durable store; the delegating writer keeps none.
        fn primary_store(&self) -> Box<dyn crate::DfsMetadataBackend> {
            Box::new(
                self.runtime
                    .node_metadata(&self.shard.primary_node_id)
                    .unwrap(),
            )
        }
        fn floor(&mut self) -> DfsMetadataShardOperationFloor {
            let key = self.floor_key();
            let entries = self.control.get_authoritative_consistent(&[key]).unwrap();
            let [Some(entry)] = entries.as_slice() else {
                panic!("actual authoritative floor baseline must contain its exact native permit");
            };
            entry.decode().unwrap()
        }
        fn capture_create_on_main(
            &self,
            path: &str,
            operation_id: &str,
            captured: Arc<Mutex<Option<crate::DfsPreparedOperationPayload>>>,
            before_prepare: Option<Box<dyn FnOnce() + Send>>,
        ) -> crate::runtime_host::DaemonDfsPreDispatchTestHookGuard {
            let expected_path = path.to_string();
            let expected_operation = operation_id.to_string();
            let expected_shard = self.shard.clone();
            let executions = std::sync::atomic::AtomicUsize::new(0);
            let before_prepare = Mutex::new(before_prepare);
            let observed_command = Arc::clone(&self.observed_create_command);
            let hook: Arc<crate::runtime_host::DaemonDfsPreDispatchTestHook> =
                Arc::new(move |point, command, _| {
                    if !matches!(
                        point,
                        crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeExecute { .. }
                    ) {
                        return Ok(());
                    }
                    let Some(command) = command else {
                        return Ok(());
                    };
                    let crate::DfsNetworkMessage::ExclusiveCreateRequest(write) =
                        &command.routed_message.message
                    else {
                        return Ok(());
                    };
                    if write.request_id != expected_operation {
                        return Ok(());
                    }
                    assert_eq!(executions.fetch_add(1, Ordering::SeqCst), 0);
                    // One actual normal command, correlated before any mutation.
                    observed_command.lock().unwrap().replace(command.clone());
                    assert_eq!(
                        command.routed_message.source_node_id,
                        expected_shard.primary_node_id
                    );
                    assert_eq!(
                        command.routed_message.target_node_id,
                        expected_shard.primary_node_id
                    );
                    assert_eq!(
                        write.request.requesting_node_id,
                        expected_shard.primary_node_id
                    );
                    assert_eq!(write.request.file_system_id, expected_shard.file_system_id);
                    assert_eq!(write.authority_shard.as_ref(), Some(&expected_shard));
                    assert_eq!(write.object_metadata.as_ref().unwrap().path, expected_path);
                    let object_id = write.request.object_id.clone();
                    let target_version = write.request.target_version.clone();
                    let operation_id = expected_operation.clone();
                    let path = expected_path.clone();
                    let capture = Arc::clone(&captured);
                    let trigger = before_prepare.lock().unwrap().take();
                    crate::service_commit::BEFORE_CREATE_PREPARE.with(|hook| {
                        assert!(hook.borrow().is_none());
                        *hook.borrow_mut() = Some(Box::new(move |payload| {
                            let intent = payload.prepared_intent.as_ref().unwrap();
                            assert_eq!(intent.operation_id.as_deref(), Some(operation_id.as_str()));
                            assert_eq!(intent.object_id, object_id);
                            assert_eq!(intent.target_version, target_version);
                            assert!(payload.transaction.objects.iter().any(
                                |object| object.object_id == object_id
                                    && object.version == target_version
                                    && object.path == path
                            ));
                            let mut captured = capture.lock().unwrap();
                            assert!(captured.is_none());
                            *captured = Some(payload.clone());
                            if let Some(trigger) = trigger {
                                trigger();
                            }
                        }));
                    });
                    Ok(())
                });
            crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
                self.runtime.data_dir(&self.shard.primary_node_id),
                hook,
            )
        }
        fn write(&mut self, path: &str) -> crate::DfsPreparedOperationRecord {
            let captured = Arc::new(Mutex::new(None));
            let operation_id = format!("cf_{}", sinomo_core::Id::generate());
            let capture =
                self.capture_create_on_main(path, &operation_id, Arc::clone(&captured), None);
            let created = self
                .service
                .posix_create_file_with_contents_and_attrs_and_operation_id(
                    path,
                    &[],
                    crate::DfsWriteOptions::replicated_eventual(vec![
                        self.shard.primary_node_id.clone(),
                    ]),
                    crate::DfsPosixCreateAttrs::new(0o640, 6101, 6102),
                    &operation_id,
                );
            drop(capture);
            created.expect("actual full-participant native root create");
            let payload = captured
                .lock()
                .unwrap()
                .take()
                .expect("actual normal create payload");
            let record = self.record_for_payload(path, &payload);
            assert_eq!(record.status, crate::DfsPreparedOperationStatus::Committed);
            assert!(
                self.primary_store()
                    .committed_prepared_operation(&record.digest)
                    .unwrap()
                    .is_some(),
                "actual producer must retain its exact terminal commit witness"
            );
            record
        }
        // Handoff must honor any operation floor already committed for the
        // shard. Seed one bound to this exact native prepare.
        fn seed_floor(&mut self, record: &crate::DfsPreparedOperationRecord) {
            let key = self.floor_key();
            let floor = DfsMetadataShardOperationFloor {
                operation_digest: record.digest.payload_digest.clone(),
                prepared_sequence: record.digest.prepare_sequence,
                authority_epoch: record.digest.owner_epoch,
                permit_id: sinomo_core::Id::generate().to_string(),
            };
            let response = self
                .control
                .txn(KvTxn {
                    compares: vec![KvCompare::Missing { key: key.clone() }],
                    success: vec![KvOp::put_serde(key, &floor, None).unwrap()],
                    failure: Vec::new(),
                })
                .unwrap();
            assert!(response.succeeded, "seed the shard's first operation floor");
        }
        fn floor_record(
            &mut self,
        ) -> (
            DfsMetadataShardOperationFloor,
            crate::DfsPreparedOperationRecord,
        ) {
            let floor = self.floor();
            assert!(floor.prepared_sequence > 0);
            assert!(!floor.permit_id.is_empty());
            let shard_id = crate::DfsMetadataShardId::new(self.shard.shard_id.clone());
            let store = self.primary_store();
            let backend: &dyn crate::DfsMetadataBackend = &*store;
            let batch = backend
                .journal_batch_for_shard_since_limited(
                    &shard_id,
                    floor.prepared_sequence - 1,
                    1,
                    crate::DfsMetadataJournalReplicationOptions::default().batch_byte_limit,
                )
                .unwrap()
                .expect("actual floor-bound native prepare frame");
            assert_eq!(batch.shard_id, shard_id);
            let [entry] = batch.entries.as_slice() else {
                panic!("floor lookup must select its one exact native journal slot");
            };
            assert_eq!(entry.sequence, floor.prepared_sequence);
            let crate::DfsMetadataMutation::PrepareOperationV7 {
                file_system_id,
                operation_id,
                record: original,
            } = &entry.mutation
            else {
                panic!("actual operation floor must bind a native prepare body");
            };
            original.validate().unwrap();
            assert_eq!(operation_id, &original.digest.operation_id);
            assert_eq!(file_system_id, &self.shard.file_system_id);
            assert_eq!(original.digest.file_system_id, self.shard.file_system_id);
            assert_eq!(original.digest.shard_id, shard_id);
            assert_eq!(original.digest.owner_epoch, self.shard.epoch);
            assert_eq!(original.digest.owner_epoch, floor.authority_epoch);
            assert_eq!(original.digest.payload_digest, floor.operation_digest);
            assert_eq!(original.digest.prepare_sequence, floor.prepared_sequence);
            let record = backend
                .prepared_operation_record(&original.digest.operation_id)
                .unwrap()
                .expect("actual retained floor-bound operation");
            record.validate().unwrap();
            assert_eq!(record.digest, original.digest);
            assert_eq!(record.payload, original.payload);
            assert_eq!(record.status, crate::DfsPreparedOperationStatus::Committed);
            assert!(
                backend
                    .committed_prepared_operation(&record.digest)
                    .unwrap()
                    .is_some()
            );
            (floor, record)
        }
        fn record_for_payload(
            &self,
            path: &str,
            captured: &crate::DfsPreparedOperationPayload,
        ) -> crate::DfsPreparedOperationRecord {
            captured.validate().unwrap();
            assert_eq!(captured.file_system_id, self.shard.file_system_id);
            assert_eq!(captured.shard_id.0, self.shard.shard_id);
            assert_eq!(captured.owner_epoch, self.shard.epoch);
            assert!(
                captured
                    .transaction
                    .objects
                    .iter()
                    .any(|object| object.path == path)
            );
            // The normal listing intentionally contains only pending prepares.
            // Select this producer's exact immutable identity, including its
            // committed row, without inventing a record from an observed head.
            let record = self
                .primary_store()
                .prepared_operation_record(&captured.operation_id)
                .unwrap()
                .expect("actual operation-bound prepared body");
            record.validate().unwrap();
            assert_eq!(
                record.digest.payload_digest,
                captured.canonical_digest().unwrap()
            );
            let mut normalized = captured.clone();
            normalized.normalize();
            assert_eq!(record.payload.as_ref(), &normalized);
            record
        }
        fn prepare_without_floor(
            &mut self,
            path: &str,
        ) -> (
            crate::DfsPreparedOperationRecord,
            crate::test_support::DfsTestNativeConfigurationFault,
        ) {
            self.deny_last_metadata_participant();
            let fault = self
                .runtime
                .metadata_operation_floor_fault(&self.shard.primary_node_id)
                .unwrap();
            let trigger = fault.operation_floor_denial_trigger(self.floor_key());
            let captured = Arc::new(Mutex::new(None));
            let operation_id = format!("cf_{}", sinomo_core::Id::generate());
            let capture = self.capture_create_on_main(
                path,
                &operation_id,
                Arc::clone(&captured),
                Some(Box::new(trigger)),
            );
            let created = self
                .service
                .posix_create_file_with_contents_and_attrs_and_operation_id(
                    path,
                    &[],
                    crate::DfsWriteOptions::replicated_eventual(vec![
                        self.shard.primary_node_id.clone(),
                    ]),
                    crate::DfsPosixCreateAttrs::new(0o640, 6101, 6102),
                    &operation_id,
                );
            drop(capture);
            let error = created.unwrap_err();
            assert!(
                error.to_string().contains("recovery quorum unavailable"),
                "{error}"
            );
            assert_eq!(
                fault.denied_operation_floor_reads(),
                0,
                "normal full-participant publication must not fall back to an implicit KV floor read"
            );
            let payload = captured
                .lock()
                .unwrap()
                .take()
                .expect("actual denied-create payload");
            let record = self.record_for_payload(path, &payload);
            assert_eq!(record.status, crate::DfsPreparedOperationStatus::Prepared);
            let pending = self.primary_store().list_prepared_operations().unwrap();
            assert!(
                pending
                    .iter()
                    .all(
                        |candidate| !candidate.metadata_effects.iter().any(|effect| matches!(
                            effect,
                            crate::DfsPreparedMetadataEffect::ResolvePreparedOperation {
                                digest,
                                status: crate::DfsPreparedOperationStatus::Aborted,
                            } if digest == &record.digest
                        ))
                    ),
                "an authority-read failure must not prepare an automatic abort of the sealed write"
            );
            assert!(
                self.primary_store()
                    .get_object_by_path(&self.shard.file_system_id, path)
                    .unwrap()
                    .is_none()
            );
            (record, fault)
        }
        fn recover_and_close(mut self, path: &str, record: &crate::DfsPreparedOperationRecord) {
            let expected = record
                .transaction
                .objects
                .iter()
                .find(|object| object.path == path)
                .unwrap();
            drop(self.control);
            drop(self.service);
            self.runtime.clear_faults();
            self.runtime.stop_node(&self.shard.primary_node_id).unwrap();
            self.runtime
                .reopen_node(self.shard.primary_node_id.clone())
                .unwrap();
            let outcome = self
                .runtime
                .maintenance_tick(
                    &self.shard.primary_node_id,
                    std::time::Duration::from_secs(5),
                )
                .unwrap()
                .outcome
                .unwrap();
            assert!(
                matches!(outcome, crate::DfsMaintenanceTickOutcome::Completed { .. }),
                "the actual Main recovery invocation remains deferred: {outcome:?}"
            );
            let mut recovered = self.runtime.client(&self.shard.primary_node_id).unwrap();
            let recovered_store: Box<dyn crate::DfsMetadataBackend> = Box::new(
                self.runtime
                    .node_metadata(&self.shard.primary_node_id)
                    .unwrap(),
            );
            let actual = recovered_store
                .prepared_operation_record(&record.digest.operation_id)
                .unwrap()
                .expect("normal Main restart must retain this exact prepared operation");
            actual.validate().unwrap();
            assert_eq!(actual.digest, record.digest);
            assert_eq!(actual.payload, record.payload);
            assert_eq!(actual.status, crate::DfsPreparedOperationStatus::Committed);
            assert!(
                recovered_store
                    .committed_prepared_operation(&record.digest)
                    .unwrap()
                    .is_some()
            );
            drop(recovered_store);
            let stat = recovered.posix_stat_distributed(path).unwrap();
            assert_eq!(stat.object_id, expected.object_id);
            assert_eq!(stat.version, expected.version);
            assert_eq!(
                recovered.posix_read_file_distributed(path).unwrap(),
                Vec::<u8>::new()
            );
            drop(recovered);
            self.runtime.close().unwrap();
        }
    }

    #[derive(Debug)]
    struct NativeParticipantReadSetObservation {
        inner: Box<dyn DfsControlPlaneKvClient>,
        scalar_reads: AtomicUsize,
        consistent_reads: AtomicUsize,
        negative_response: AtomicUsize,
    }
    impl DfsControlPlaneKvClient for NativeParticipantReadSetObservation {
        fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
            self.scalar_reads.fetch_add(1, Ordering::SeqCst);
            self.inner.get(key)
        }
        fn get_consistent(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>, KvError> {
            self.consistent_reads.fetch_add(1, Ordering::SeqCst);
            let mut actual = self.inner.get_consistent(keys)?;
            // Negative response DATA only, after the real native reader returns.
            match self.negative_response.load(Ordering::SeqCst) {
                1 => {
                    actual.pop();
                }
                2 => {
                    actual[0].as_mut().unwrap().key = "/unrelated-participant".into();
                }
                3 => {
                    let entry = actual[0].as_mut().unwrap();
                    let mut member = entry.decode::<ClusterMemberRecord>()?;
                    member.capabilities.retain(|capability| capability.as_str()
                        != sinomo_core::node::DFS_METADATA_NAMESPACE_COUNTS_V1_CAPABILITY);
                    entry.value = sinomo_core::kv::encode_kv_value(&member)?;
                }
                _ => {}
            }
            Ok(actual)
        }
        fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
            self.inner.range_prefix(prefix)
        }
        fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
            self.inner.put(key, value)
        }
        fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
            self.inner.txn(txn)
        }
        fn txn_with_native_authority(
            &mut self,
            txn: KvTxn,
        ) -> Result<(KvTxnResponse, Option<DfsNativeTxnCommit>), KvError> {
            self.inner.txn_with_native_authority(txn)
        }
    }

    #[test]
    fn metadata_participant_admission_uses_one_actual_native_read_set_and_rejects_bad_scope() {
        let fixture = NativeFloorFixture::new("native-member-policy-read-set");
        let shard = fixture.shard.clone();
        let observer = NativeParticipantReadSetObservation {
            inner: fixture.control,
            scalar_reads: AtomicUsize::new(0),
            consistent_reads: AtomicUsize::new(0),
            negative_response: AtomicUsize::new(0),
        };
        let compares = metadata_prepared_operation_admission_compares(&observer, &shard).unwrap();
        assert_eq!(
            compares.len(),
            2 * metadata_shard_configured_node_ids(&shard).len()
        );
        assert_eq!(observer.scalar_reads.load(Ordering::SeqCst), 0);
        assert_eq!(observer.consistent_reads.load(Ordering::SeqCst), 1);
        observer.negative_response.store(1, Ordering::SeqCst);
        assert!(matches!(
            metadata_prepared_operation_admission_compares(&observer, &shard),
            Err(KvError::Serialization { detail }) if detail.contains("exact key count")
        ));
        observer.negative_response.store(2, Ordering::SeqCst);
        assert!(matches!(
            metadata_prepared_operation_admission_compares(&observer, &shard),
            Err(KvError::Serialization { detail }) if detail.contains("exact key scope")
        ));
        observer.negative_response.store(3, Ordering::SeqCst);
        assert!(matches!(metadata_prepared_operation_admission_compares(&observer, &shard),
            Err(KvError::Serialization { detail }) if detail.contains("namespace-counts.v1")),
            "an existing assignment and an effective policy cannot supply missing binary support");
        assert_eq!(observer.scalar_reads.load(Ordering::SeqCst), 0);
        assert_eq!(observer.consistent_reads.load(Ordering::SeqCst), 4);
        drop(observer);
        drop(fixture.service);
        fixture.runtime.clear_faults();
        fixture.runtime.close().unwrap();
    }

    #[test]
    fn metadata_operation_floor_foreign_target_collision_preserves_the_actual_prepare() {
        let mut fixture = NativeFloorFixture::new("native-floor-foreign-target-collision");
        let (original, fault) = fixture.prepare_without_floor("/native-floor-b.bin");
        let mut command = fixture
            .observed_create_command
            .lock()
            .unwrap()
            .take()
            .expect("exact normal Main command was observed");
        let foreign = fixture.shard.replica_node_ids[0].clone();
        let collision_id = format!("foreign_{}", sinomo_core::Id::generate());
        let crate::DfsNetworkMessage::ExclusiveCreateRequest(mut write) =
            command.routed_message.message
        else {
            panic!("actual exclusive create command");
        };
        write.request_id = collision_id.clone();
        write.request.requesting_node_id = foreign.clone();
        write.requester_blob_pre_staged = false;
        write.exclusive_create = false;
        command.routed_message.source_node_id = foreign.clone();
        command.routed_message.message = crate::DfsNetworkMessage::DistributedWriteRequest(write);
        let shard_id = crate::DfsMetadataShardId::new(fixture.shard.shard_id.clone());
        let journal = crate::LocalSobjMetadataBackend::open_for_inspection(
            shard_id.clone(),
            crate::default_dfs_metadata_shards_root(
                fixture.runtime.data_dir(&fixture.shard.primary_node_id),
            )
            .join(crate::metadata_keys::key_segment(&shard_id.0)),
        )
        .unwrap();
        let limit = crate::DfsMetadataJournalReplicationOptions::default().batch_entry_limit;
        let before = journal.journal_since_limited(0, limit).unwrap();
        assert!(
            before.len() < limit,
            "complete bounded journal before collision"
        );
        let response = fixture
            .runtime
            .rpc_request(
                &foreign,
                &fixture.shard.primary_node_id,
                crate::dfs_message_rpc_request(&command).unwrap(),
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .expect("foreign collision must return its normal rejection");
        let response = crate::decode_dfs_message_rpc_response(&response).unwrap();
        assert!(response.outbound_messages.iter().any(|routed| matches!(
            &routed.message,
            crate::DfsNetworkMessage::DistributedWriteResponse(reply)
                if reply.request_id == collision_id && matches!(
                    &reply.result, crate::DfsDistributedWriteResult::Rejected { reason, .. }
                        if reason.contains(&original.prepared_intent.as_ref().unwrap().intent_id)
                )
        )), "original pending claim must reject the unrelated target collision");
        let retained = fixture
            .primary_store()
            .prepared_operation_record(&original.digest.operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(retained, original);
        let after = journal.journal_since_limited(0, limit).unwrap();
        assert!(
            after.len() < limit,
            "complete bounded journal after collision"
        );
        assert_eq!(
            after, before,
            "foreign request cannot author abort or other native frames"
        );
        drop(fault);
        drop(fixture.service);
        drop(fixture.control);
        fixture.runtime.clear_faults();
        fixture.runtime.close().unwrap();
    }

    #[test]
    fn metadata_authority_view_retry_accepts_only_the_readindex_generation_race() {
        assert!(metadata_operation_authority_view_error_is_retryable(
            &KvError::Io {
                path: "test-cluster-authoritative-view".into(),
                detail: "resident Raft view changed after ReadIndex quorum; retry the read".into(),
            }
        ));
        for detail in ["permission denied", "permission denied; retry the read"] {
            assert!(!metadata_operation_authority_view_error_is_retryable(
                &KvError::Io {
                    path: "test-disk".into(),
                    detail: detail.into(),
                }
            ));
        }
    }

    #[test]
    fn degraded_operation_floor_is_carried_into_membership_handoff() {
        let mut fixture =
            NativeFloorFixture::with_node_count("native-floor-membership-expansion", 4);
        assert_eq!(metadata_shard_configured_node_ids(&fixture.shard).len(), 4);
        assert_eq!(fixture.shard.replica_node_ids.len(), 3);
        fixture.runtime.clear_faults();
        let added = fixture.shard.replica_node_ids.last().unwrap().clone();
        let retained = fixture.shard.replica_node_ids[..2].to_vec();
        // Make the three-member source through a real completed native handoff;
        // the fourth mounted member remains a genuine expansion target.
        fixture.shard = fixture
            .runtime
            .reassign_metadata_shard(
                &fixture.shard.file_system_id,
                &fixture.shard.shard_id,
                fixture.shard.primary_node_id.clone(),
                retained,
            )
            .unwrap();
        assert_eq!(metadata_shard_configured_node_ids(&fixture.shard).len(), 3);
        assert_eq!(fixture.shard.replica_node_ids.len(), 2);
        assert!(!metadata_shard_configured_node_ids(&fixture.shard).contains(&added));
        fixture.service = fixture
            .runtime
            .client(&fixture.shard.primary_node_id)
            .unwrap();
        let created = fixture.write("/native-floor-a.bin");
        fixture.seed_floor(&created);
        fixture.deny_last_metadata_participant();
        let (floor, _) = fixture.floor_record();
        let mut desired = fixture.shard.clone();
        desired.replica_node_ids.push(added);
        desired.replica_node_ids.sort();
        desired.epoch += 1;
        // The shard fence guard belongs to the store-backed owning service.
        let fence_owner = fixture.runtime.metadata_primary_service("/").unwrap();
        let gate = fence_owner
            .metadata_shard_fence_guard(&fixture.shard.file_system_id, &fixture.shard.shard_id)
            .unwrap();
        let handoff = DfsControlPlaneStore::new(fixture.control.as_mut())
            .begin_metadata_shard_membership_handoff(&fixture.shard, &desired)
            .unwrap()
            .unwrap();
        assert_eq!(handoff.required_prepared_sequence, floor.prepared_sequence);
        let shard_id = crate::DfsMetadataShardId::new(fixture.shard.shard_id.clone());
        let backend = fence_owner.metadata_backend().unwrap();
        let sequence = backend
            .metadata_shard_statuses()
            .unwrap()
            .into_iter()
            .find(|status| status.shard_id == shard_id)
            .unwrap()
            .journal_last_sequence;
        let fingerprint =
            crate::service_sync::metadata_shard_state_fingerprint_at(backend, &shard_id, sequence)
                .unwrap();
        // A negative below-floor candidate uses the actual source fingerprint;
        // it cannot establish a fence or healthy acknowledgement.
        let stale_fence = DfsMetadataShardMembershipHandoffFence {
            handoff_id: handoff.handoff_id.clone(),
            primary_node_id: fixture.shard.primary_node_id.clone(),
            journal_sequence: floor.prepared_sequence - 1,
            state_fingerprint: fingerprint,
        };
        assert!(matches!(DfsControlPlaneStore::new(fixture.control.as_mut())
            .acknowledge_metadata_shard_membership_handoff_fence(&handoff, &stale_fence),
            Err(KvError::Serialization { detail }) if detail.contains("below required prepared floor")));
        drop(gate);
        drop(fence_owner);
        fixture.runtime.clear_faults();
        let active = fixture
            .runtime
            .reassign_metadata_shard(
                &desired.file_system_id,
                &desired.shard_id,
                desired.primary_node_id.clone(),
                desired.replica_node_ids.clone(),
            )
            .unwrap();
        assert_eq!(active, desired);
        let proof = DfsControlPlaneStore::new(fixture.control.as_mut())
            .get_metadata_shard_membership_handoff_proof(&active.file_system_id, &active.shard_id)
            .unwrap()
            .unwrap();
        assert_eq!(proof.retiring_shard, fixture.shard);
        assert_eq!(proof.desired_shard, active);
        assert!(proof.journal_sequence >= floor.prepared_sequence);
        assert!(metadata_shard_membership_handoff_acknowledgements_are_safe(
            &proof.retiring_shard,
            &proof.desired_shard,
            &proof.acknowledged_node_ids.iter().cloned().collect(),
        ));
        drop(fixture.control);
        drop(fixture.service);
        fixture.runtime.close().unwrap();
    }

    #[test]
    fn degraded_head_watermark_is_exact_and_monotonic() {
        let mut fixture = NativeFloorFixture::new("native-cp-sync-watermark");
        let unavailable = fixture.shard.replica_node_ids.last().unwrap().clone();
        let first = fixture.write("/native-floor-a.bin");
        let first_object = first
            .transaction
            .objects
            .iter()
            .find(|object| object.path == "/native-floor-a.bin")
            .unwrap();
        let mut barrier_owner = fixture.runtime.metadata_primary_service("/").unwrap();
        let first_target = barrier_owner
            .capture_metadata_quorum_sync_target(first_object)
            .unwrap();
        let first_certificate = barrier_owner
            .sync_metadata_shard_to_quorum(&first_target, std::time::Duration::from_secs(5))
            .unwrap();
        let watermark = |certificate: &crate::DfsMetadataQuorumSyncCertificate| {
            let target = &certificate.target;
            assert_eq!(
                certificate.acknowledged_node_ids.len(),
                1 + target.replica_node_ids.len()
            );
            DfsMetadataShardHeadWatermark {
                file_system_id: target.file_system_id.clone(),
                shard_id: target.shard_id.0.clone(),
                primary_node_id: target.primary_node_id.clone(),
                epoch: target.authority_epoch.unwrap(),
                journal_sequence: target.journal_sequence,
                state_fingerprint: target.state_fingerprint.clone(),
                acknowledged_node_ids: certificate.acknowledged_node_ids.clone(),
            }
        };
        // This compatibility control record consumes an actual full native
        // certificate; it is not an implicit fallback for partial DFS Sync.
        assert!(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .publish_metadata_shard_head_watermark(
                    &fixture.shard,
                    &watermark(&first_certificate)
                )
                .unwrap()
        );
        let low = DfsControlPlaneStore::new(fixture.control.as_mut())
            .get_metadata_shard_head_watermark(
                &fixture.shard.file_system_id,
                &fixture.shard.shard_id,
            )
            .unwrap()
            .unwrap();
        assert_eq!(low.journal_sequence, first_target.journal_sequence);
        assert_eq!(low.state_fingerprint, first_target.state_fingerprint);
        assert_eq!(
            low.acknowledged_node_ids,
            first_certificate.acknowledged_node_ids
        );
        drop(barrier_owner);
        let second = fixture.write("/native-floor-b.bin");
        let mut barrier_owner = fixture.runtime.metadata_primary_service("/").unwrap();
        let second_object = second
            .transaction
            .objects
            .iter()
            .find(|object| object.path == "/native-floor-b.bin")
            .unwrap();
        let second_target = barrier_owner
            .capture_metadata_quorum_sync_target(second_object)
            .unwrap();
        fixture.runtime.stop_node(&unavailable).unwrap();
        assert!(
            barrier_owner
                .sync_metadata_shard_to_quorum(
                    &second_target,
                    std::time::Duration::from_millis(100)
                )
                .is_err(),
            "a previously certified head cannot satisfy a new full-participant barrier while a member is stopped"
        );
        assert_eq!(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .get_metadata_shard_head_watermark(
                    &fixture.shard.file_system_id,
                    &fixture.shard.shard_id
                )
                .unwrap(),
            Some(low.clone())
        );
        fixture.runtime.reopen_node(unavailable).unwrap();
        let second_certificate = barrier_owner
            .sync_metadata_shard_to_quorum(&second_target, std::time::Duration::from_secs(5))
            .unwrap();
        assert!(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .publish_metadata_shard_head_watermark(
                    &fixture.shard,
                    &watermark(&second_certificate)
                )
                .unwrap()
        );
        let high = DfsControlPlaneStore::new(fixture.control.as_mut())
            .get_metadata_shard_head_watermark(
                &fixture.shard.file_system_id,
                &fixture.shard.shard_id,
            )
            .unwrap()
            .unwrap();
        assert!(high.journal_sequence > low.journal_sequence);
        assert_eq!(high.journal_sequence, second_target.journal_sequence);
        assert_eq!(high.state_fingerprint, second_target.state_fingerprint);
        assert_eq!(
            high.acknowledged_node_ids,
            second_certificate.acknowledged_node_ids
        );
        assert!(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .publish_metadata_shard_head_watermark(&fixture.shard, &low)
                .unwrap()
        );
        assert_eq!(
            DfsControlPlaneStore::new(fixture.control.as_mut())
                .get_metadata_shard_head_watermark(
                    &fixture.shard.file_system_id,
                    &fixture.shard.shard_id
                )
                .unwrap(),
            Some(high)
        );
        drop(barrier_owner);
        fixture.recover_and_close("/native-floor-b.bin", &second);
    }

    #[test]
    fn metadata_shard_selection_prefers_longest_path_prefix() {
        let node_owner = node("node_shard_select_owner");
        let node_replica = node("node_shard_select_replica");
        let mut kv = InProcessKvStore::new();
        // These names are structural assignment DATA; admitted member bytes
        // come unchanged from their actual ordinary native source owner.
        copy_native_member_fixture_data(
            &mut kv,
            vec![
                node("node_shard_select_owner"),
                node("node_shard_select_replica"),
            ],
        );
        let file_system_id = file_system_id();
        let mut control = DfsControlPlaneStore::new(&mut kv);
        for shard in [
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: node_owner.clone(),
                replica_node_ids: vec![node_replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: node_owner.clone(),
                replica_node_ids: vec![node_replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "services-sandbox".to_string(),
                primary_node_id: node_owner.clone(),
                replica_node_ids: vec![node_replica],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services/sandbox".to_string(),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
        ] {
            control.put_metadata_shard(&shard).expect("put shard");
        }

        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, "/services/sandbox/instances/one")
                .expect("select sandbox")
                .map(|shard| shard.shard_id),
            Some("services-sandbox".to_string())
        );
        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, "/services/api")
                .expect("select services")
                .map(|shard| shard.shard_id),
            Some("services".to_string())
        );
        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, "/services2/api")
                .expect("select root")
                .map(|shard| shard.shard_id),
            Some("root".to_string())
        );
    }

    #[test]
    fn metadata_shard_selection_rejects_partial_default_sifs_split_view() {
        let node_root = node("node_shard_partial_root");
        let node_services = node("node_shard_partial_services");
        let node_users = node("node_shard_partial_users");
        let mut kv = InProcessKvStore::new();
        // These names are structural assignment DATA; admitted member bytes
        // come unchanged from their actual ordinary native source owner.
        copy_native_member_fixture_data(
            &mut kv,
            vec![
                node("node_shard_partial_root"),
                node("node_shard_partial_services"),
                node("node_shard_partial_users"),
            ],
        );
        let file_system_id = sifs_file_system_id();
        let mut control = DfsControlPlaneStore::new(&mut kv);
        let root_shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node_root.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        control
            .put_metadata_shard(&root_shard)
            .expect("put root shard");
        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, "/services/api")
                .expect("root-only SIFS view remains usable")
                .map(|shard| shard.shard_id),
            Some("root".to_string())
        );

        let users_shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "users".to_string(),
            primary_node_id: node_users,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_USERS_PATH.to_string(),
            },
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        control
            .put_metadata_shard(&users_shard)
            .expect("put users shard");
        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, "/services/api")
                .expect("generic selector keeps custom partial SIFS views usable")
                .map(|shard| shard.shard_id),
            Some("root".to_string())
        );
        let error = control
            .select_metadata_shard_for_path_strict_default_sifs_split(
                &file_system_id,
                "/services/api",
            )
            .expect_err("partial split view must not route /services to root");
        assert!(
            matches!(&error, KvError::Busy { path } if path.contains("missing default services shard")),
            "unexpected error: {error:?}"
        );
        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, "/users/alice")
                .expect("visible users shard remains selected")
                .map(|shard| shard.shard_id),
            Some("users".to_string())
        );

        let services_shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "services".to_string(),
            primary_node_id: node_services,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: SIFS_SERVICES_PATH.to_string(),
            },
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        control
            .put_metadata_shard(&services_shard)
            .expect("put services shard");
        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, "/services/api")
                .expect("complete split view selects services")
                .map(|shard| shard.shard_id),
            Some("services".to_string())
        );
    }

    #[test]
    fn metadata_shard_selection_uses_hash_range_before_root() {
        let node_owner = node("node_shard_hash_owner");
        let node_replica = node("node_shard_hash_replica");
        let mut kv = InProcessKvStore::new();
        // These names are structural assignment DATA; admitted member bytes
        // come unchanged from their actual ordinary native source owner.
        copy_native_member_fixture_data(
            &mut kv,
            vec![
                node("node_shard_hash_owner"),
                node("node_shard_hash_replica"),
            ],
        );
        let file_system_id = file_system_id();
        let selected_path = "/users/alice/project";
        let hash = metadata_name_hash(selected_path);
        let (start_inclusive, end_exclusive) = if hash == u64::MAX {
            (hash, 0)
        } else {
            (hash, hash + 1)
        };
        let mut control = DfsControlPlaneStore::new(&mut kv);
        for shard in [
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: node_owner.clone(),
                replica_node_ids: vec![node_replica.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "hash-selected".to_string(),
                primary_node_id: node_owner,
                replica_node_ids: vec![node_replica],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::NameHashRange {
                    start_inclusive,
                    end_exclusive,
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
        ] {
            control.put_metadata_shard(&shard).expect("put shard");
        }

        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, selected_path)
                .expect("select hash")
                .map(|shard| shard.shard_id),
            Some("hash-selected".to_string())
        );
    }

    #[test]
    fn metadata_shard_selection_prefers_narrower_hash_range() {
        let node_owner = node("node_shard_hash_width_owner");
        let mut kv = InProcessKvStore::new();
        // These names are structural assignment DATA; admitted member bytes
        // come unchanged from their actual ordinary native source owner.
        copy_native_member_fixture_data(&mut kv, vec![node("node_shard_hash_width_owner")]);
        let file_system_id = file_system_id();
        let selected_path = "/users/bob/project";
        let hash = metadata_name_hash(selected_path);
        let mut control = DfsControlPlaneStore::new(&mut kv);
        for shard in [
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "aaa-broad".to_string(),
                primary_node_id: node_owner.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::NameHashRange {
                    start_inclusive: hash.wrapping_sub(100),
                    end_exclusive: hash.wrapping_add(101),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "zzz-narrow".to_string(),
                primary_node_id: node_owner.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::NameHashRange {
                    start_inclusive: hash,
                    end_exclusive: hash.wrapping_add(1),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
        ] {
            control.put_metadata_shard(&shard).expect("put shard");
        }

        assert_eq!(
            control
                .select_metadata_shard_for_path(&file_system_id, selected_path)
                .expect("select hash")
                .map(|shard| shard.shard_id),
            Some("zzz-narrow".to_string())
        );
    }

    #[test]
    fn compiled_metadata_shard_routing_matches_reference_selector() {
        let file_system_id = file_system_id();
        let owner = node("node_compiled_shard_router");
        let selected_path = "/hashed/wrapped";
        let selected_hash = metadata_name_hash(selected_path);
        let shards = vec![
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: owner.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: owner.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "services-deep".to_string(),
                primary_node_id: owner.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services/deep".to_string(),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "hash-wide".to_string(),
                primary_node_id: owner.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::NameHashRange {
                    start_inclusive: selected_hash.wrapping_sub(100),
                    end_exclusive: selected_hash.wrapping_add(101),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "hash-narrow".to_string(),
                primary_node_id: owner,
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::NameHashRange {
                    start_inclusive: selected_hash,
                    end_exclusive: selected_hash.wrapping_add(1),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
        ];
        let compiled = DfsMetadataShardRoutingIndex::new(file_system_id.clone(), &shards)
            .expect("compile routing index");

        for path in [
            "/",
            "/services",
            "/services/api",
            "/services/deep/task",
            "/services2",
            selected_path,
            "/other/path",
            "/a/../services/deep/normalized",
        ] {
            let reference = select_metadata_shard_config_for_path(&file_system_id, &shards, path)
                .expect("reference selection")
                .map(|shard| shard.shard_id);
            let indexed = compiled
                .select_shard_id(path, false)
                .expect("indexed selection")
                .map(str::to_string);
            assert_eq!(indexed, reference, "routing mismatch for {path}");
        }
    }

    #[test]
    fn metadata_shard_selection_rejects_nul_paths() {
        let node_owner = node("node_shard_invalid_owner");
        let mut kv = InProcessKvStore::new();
        // These names are structural assignment DATA; admitted member bytes
        // come unchanged from their actual ordinary native source owner.
        copy_native_member_fixture_data(&mut kv, vec![node("node_shard_invalid_owner")]);
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node_owner,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control.put_metadata_shard(&shard).expect("put shard");

        assert!(matches!(
            control.select_metadata_shard_for_path(&file_system_id, "/bad\0path"),
            Err(KvError::Serialization { .. })
        ));
    }
    mod placement_update_retry;
}

/// Observation-only decoding of the SAME retained current control-row bytes.
/// No private floor type or authority constructor crosses this module.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn decode_native_control_observation(
    entry: &KvEntry,
) -> Result<Option<(&'static str, serde_json::Value)>, KvError> {
    decode_native_control_value_observation(&entry.key, &entry.value)
}

/// The same current decoder at explicit observation export. Request values
/// have no durable Entry revisions; preserve their actual key and bytes only.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn decode_native_control_value_observation(
    key: &str,
    bytes: &[u8],
) -> Result<Option<(&'static str, serde_json::Value)>, KvError> {
    let parts = key.split('/').collect::<Vec<_>>();
    if parts.len() != 6
        || parts[0] != ""
        || parts[1] != "dfs"
        || parts[2] != "filesystems"
        || parts[3].is_empty()
        || parts[5].is_empty()
    {
        return Ok(None);
    }
    fn value<T: Serialize>(value: &T) -> Result<serde_json::Value, KvError> {
        serde_json::to_value(value).map_err(|error| KvError::Serialization {
            detail: error.to_string(),
        })
    }
    Ok(match parts[4] {
        "metadata-shards" => Some((
            "DfsMetadataShardConfig",
            value(&sinomo_core::kv::decode_kv_value::<DfsMetadataShardConfig>(
                bytes,
            )?)?,
        )),
        "metadata-membership-handoffs" => Some((
            "DfsMetadataShardMembershipHandoff",
            value(&sinomo_core::kv::decode_kv_value::<
                DfsMetadataShardMembershipHandoff,
            >(bytes)?)?,
        )),
        "metadata-membership-handoff-proofs" => Some((
            "DfsMetadataShardMembershipHandoffProof",
            value(&sinomo_core::kv::decode_kv_value::<
                DfsMetadataShardMembershipHandoffProof,
            >(bytes)?)?,
        )),
        "metadata-operation-floors" => Some((
            "DfsMetadataShardOperationFloor",
            value(&sinomo_core::kv::decode_kv_value::<
                DfsMetadataShardOperationFloor,
            >(bytes)?)?,
        )),
        "metadata-head-watermarks" => Some((
            "DfsMetadataShardHeadWatermark",
            value(&sinomo_core::kv::decode_kv_value::<
                DfsMetadataShardHeadWatermark,
            >(bytes)?)?,
        )),
        _ => None,
    })
}
