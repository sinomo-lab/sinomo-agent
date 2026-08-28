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
    DfsAdaptivePlacementObservationState, DfsFileSystem, DfsPlacementReconciliationActivityCursor,
    DfsPlacementReconciliationActivityPage, DfsPlacementReconciliationActivityStatus,
    DfsPlacementReconciliationActivitySummary, DfsPlacementReconciliationJob, DfsSubtreePolicy,
    sifs_file_system_id,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT;
use sinomo_core::kv::{
    InProcessKvStore, KvCompare, KvEntry, KvError, KvOp, KvRevision, KvTxn, KvTxnResponse,
};
use sinomo_core::node::{
    ClusterCapabilityPolicyRecord, DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY,
    DFS_METADATA_PACKED_NAMESPACE_PREPARE_V2_CAPABILITY, NodeId, cluster_capability_policy_key,
    cluster_member_key,
};
use sinomo_core::{ClusterMemberRecord, ClusterMemberState, DfsObjectPolicy};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

const DFS_KV_PREFIX: &str = "/dfs";
const METADATA_HEAD_WATERMARK_PUBLISH_RETRIES: usize = 4;
const METADATA_OPERATION_FLOOR_PUBLISH_RETRIES: usize = 4;
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

#[derive(Debug, Eq, PartialEq)]
enum MetadataOperationPermitAttempt {
    Certified(String),
    Rejected,
    RetryAfterCompareMiss,
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
        // projection was consumed. Retrying the same operation-bound floor is
        // safe: its digest and prepared sequence are compared in Raft below.
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
struct DfsPlacementAuthorityPermitRecord {
    job_id: String,
    holder_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    holder_cluster_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    holder_node_id: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    holder_session_id: Option<String>,
}

#[derive(Debug)]
pub(crate) struct DfsPlacementAuthorityPermit {
    file_system_id: DfsFileSystemId,
    record: DfsPlacementAuthorityPermitRecord,
    released: bool,
}

/// Opaque daemon-incarnation identity embedded in every new placement permit.
/// Cluster and process identity are supplied by the control-plane host; the
/// DFS service supplies the local node id at acquisition time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsPlacementAuthorityProcessIdentity {
    pub cluster_id: String,
    pub process_session_id: String,
}

/// Sanitized classification of the identity carried by a placement permit.
/// Process-session and random holder ids remain private to the authority
/// implementation; operators receive only the fields needed to establish the
/// recovery class plus an opaque exact-generation token.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPlacementAuthorityPermitHolderKind {
    Identified,
    Partial,
    Legacy,
}

/// Read-only operator view of a non-expiring placement mutation permit.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementAuthorityPermitStatus {
    pub file_system_id: DfsFileSystemId,
    pub job_id: String,
    pub holder_kind: DfsPlacementAuthorityPermitHolderKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub holder_cluster_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub holder_node_id: Option<NodeId>,
    /// Opaque token bound to both the exact encoded permit and its KV
    /// generation. Recovery must echo this value unchanged.
    pub recovery_token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPlacementAuthorityRecoveryRefusal {
    OwnerStoppedAcknowledgementRequired,
    CurrentLocalNode,
    IncompleteHolderIdentity,
    ForeignCluster,
    HolderMembershipPresent { state: ClusterMemberState },
    InvalidRemovedMembershipEvidence,
    BreakGlassAcknowledgementRequired,
    NormalRecoveryRequired,
}

/// Outcome of an operator-authorized placement permit recovery attempt.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum DfsPlacementAuthorityRecoveryOutcome {
    Missing,
    Cleared,
    Changed,
    Refused {
        reason: DfsPlacementAuthorityRecoveryRefusal,
    },
}

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

fn placement_reconciliation_queue_key(file_system_id: &DfsFileSystemId) -> String {
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

fn placement_authority_permit_holder_kind(
    record: &DfsPlacementAuthorityPermitRecord,
) -> DfsPlacementAuthorityPermitHolderKind {
    match (
        &record.holder_cluster_id,
        &record.holder_node_id,
        &record.holder_session_id,
    ) {
        (Some(_), Some(_), Some(_)) => DfsPlacementAuthorityPermitHolderKind::Identified,
        (None, None, None) => DfsPlacementAuthorityPermitHolderKind::Legacy,
        _ => DfsPlacementAuthorityPermitHolderKind::Partial,
    }
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
    let holder_kind = placement_authority_permit_holder_kind(&record);
    Ok(DfsPlacementAuthorityPermitStatus {
        file_system_id: file_system_id.clone(),
        job_id: record.job_id,
        holder_kind,
        holder_cluster_id: record.holder_cluster_id,
        holder_node_id: record.holder_node_id,
        recovery_token: placement_authority_permit_recovery_token(entry),
    })
}

fn placement_authority_busy_path(
    file_system_id: &DfsFileSystemId,
    record: &DfsPlacementAuthorityPermitRecord,
) -> String {
    let holder = match (
        &record.holder_cluster_id,
        &record.holder_node_id,
        &record.holder_session_id,
    ) {
        (Some(cluster_id), Some(node_id), Some(session_id)) => {
            format!("held by node {node_id} in cluster {cluster_id}, daemon session {session_id}")
        }
        _ => "held by an unknown legacy session".to_string(),
    };
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
    merged
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

fn legacy_placement_reconciliations_prefix(file_system_id: &DfsFileSystemId) -> String {
    format!("{DFS_KV_PREFIX}/filesystems/{file_system_id}/placement-reconciliations/")
}

fn legacy_placement_reconciliation_key(file_system_id: &DfsFileSystemId, job_id: &str) -> String {
    format!(
        "{}{}",
        legacy_placement_reconciliations_prefix(file_system_id),
        normalize_segment(job_id)
    )
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
    let Some(accepted_config) = job.accepted_config.as_ref() else {
        return Err(KvError::Serialization {
            detail: format!(
                "placement reconciliation job {} is missing its accepted config snapshot",
                job.job_id
            ),
        });
    };
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
/// ordinary authority checks overwrite that generic barrier, while an
/// operation permit must remain effective until a later prepared operation
/// advances this single per-shard floor.
fn metadata_shard_operation_floor_key(file_system_id: &DfsFileSystemId, shard_id: &str) -> String {
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileSystemConfig {
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree_policies: Vec<DfsSubtreePolicy>,
}

impl From<&DfsFileSystem> for DfsFileSystemConfig {
    fn from(file_system: &DfsFileSystem) -> Self {
        Self {
            file_system_id: file_system.file_system_id.clone(),
            display_name: file_system.display_name.clone(),
            owner_node_id: file_system.owner_node_id.clone(),
            subtree_policies: file_system.subtree_policies.clone(),
        }
    }
}

impl From<DfsFileSystemConfig> for DfsFileSystem {
    fn from(config: DfsFileSystemConfig) -> Self {
        Self {
            file_system_id: config.file_system_id,
            display_name: config.display_name,
            owner_node_id: config.owner_node_id,
            subtree_policies: config.subtree_policies,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsMetadataNamespacePrepareCodec {
    /// Canonical full [`crate::DfsNamespacePrepareRecord`] journal payload.
    #[default]
    Legacy,
    /// Packed-v1 authoritative journal payload. This may be selected only
    /// after every assigned metadata member advertises decoder support.
    /// Activation is one-way until a future rollback protocol proves a
    /// legacy-readable snapshot and replay floor beyond all packed records.
    PackedV1,
    /// Indexed packed-v2 authoritative journal payload. This capability is
    /// cumulative with every earlier codec and is selected only after every
    /// assigned metadata member advertises the matching decoder capability.
    PackedV2,
}

impl DfsMetadataNamespacePrepareCodec {
    pub(crate) const fn rank(self) -> u8 {
        match self {
            Self::Legacy => 0,
            Self::PackedV1 => 1,
            Self::PackedV2 => 2,
        }
    }

    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::PackedV1 => "packed-v1",
            Self::PackedV2 => "packed-v2",
        }
    }

    pub(crate) const fn required_capability(self) -> Option<&'static str> {
        match self {
            Self::Legacy => None,
            Self::PackedV1 => Some(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY),
            Self::PackedV2 => Some(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V2_CAPABILITY),
        }
    }

    pub(crate) const fn is_packed(self) -> bool {
        !matches!(self, Self::Legacy)
    }
}

fn metadata_namespace_prepare_codec_is_legacy(codec: &DfsMetadataNamespacePrepareCodec) -> bool {
    *codec == DfsMetadataNamespacePrepareCodec::Legacy
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardConfig {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: String,
    pub primary_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replica_node_ids: Vec<NodeId>,
    pub epoch: u64,
    pub assignment: DfsMetadataShardAssignment,
    #[serde(
        default,
        skip_serializing_if = "metadata_namespace_prepare_codec_is_legacy"
    )]
    pub namespace_prepare_codec: DfsMetadataNamespacePrepareCodec,
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

/// Quorum-committed fence separating a retiring metadata assignment from a
/// desired assignment or primary role.
///
/// While this record exists, the retiring configuration remains stored at the
/// normal shard key but may not authorize a new canonical publication. A data
/// plane coordinator must catch up a joint quorum of the retiring and desired
/// members and then provide [`DfsMetadataShardMembershipHandoffProof`].
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataShardMembershipHandoff {
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
        if entries.len() > sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES {
            return Err(KvError::RangeLimit {
                prefix: prefix.to_string(),
                max_entries: sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
            });
        }
        let mut payload_bytes = 0_usize;
        let mut previous_key: Option<&str> = None;
        for entry in &entries {
            payload_bytes = payload_bytes
                .saturating_add(entry.key.len())
                .saturating_add(entry.value.len());
            if payload_bytes > sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES {
                return Err(KvError::RangePayloadLimit {
                    prefix: prefix.to_string(),
                    observed_bytes: payload_bytes,
                    max_bytes: sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
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

pub trait DfsControlPlaneKvClient: std::fmt::Debug + Send {
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

#[derive(Clone, Debug)]
pub struct SharedDfsControlPlaneKvClient<C> {
    inner: Arc<Mutex<C>>,
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
}

impl DfsControlPlaneKvClient for InProcessKvStore {
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

    fn placement_authority_permit_missing_compare(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<KvCompare, KvError> {
        let key = placement_authority_permit_key(file_system_id);
        for _ in 0..PLACEMENT_AUTHORITY_RECOVERY_CAS_RETRIES {
            let Some(entry) = self.kv.get(&key)? else {
                return Ok(KvCompare::Missing { key });
            };
            let record = entry.decode::<DfsPlacementAuthorityPermitRecord>()?;
            if !placement_authority_release_is_pending(file_system_id, &record) {
                return Err(KvError::Busy {
                    path: placement_authority_busy_path(file_system_id, &record),
                });
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
    /// suspect, pending, active, local, foreign, and partial holders fail
    /// closed.
    ///
    /// Membership removal does not fence an already-running metadata writer,
    /// so the operator must additionally acknowledge that the old owner has
    /// been stopped or externally fenced.
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
        let (Some(holder_cluster_id), Some(holder_node_id), Some(_holder_session_id)) = (
            record.holder_cluster_id.as_deref(),
            record.holder_node_id.as_ref(),
            record.holder_session_id.as_deref(),
        ) else {
            return Ok(DfsPlacementAuthorityRecoveryOutcome::Refused {
                reason: DfsPlacementAuthorityRecoveryRefusal::IncompleteHolderIdentity,
            });
        };
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

    /// Deliberately exceptional recovery for a foreign-cluster or incomplete
    /// legacy permit whose owner cannot be proven through current membership.
    /// The exact acknowledgement phrase and exact-generation token are both
    /// required. Fully identified same-cluster holders must use the normal
    /// removed-node recovery path and can never cross this escape hatch.
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
        let fully_identified_same_cluster = matches!(
            (
                record.holder_cluster_id.as_deref(),
                record.holder_node_id.as_ref(),
                record.holder_session_id.as_deref(),
            ),
            (Some(cluster_id), Some(_), Some(_)) if cluster_id == current_cluster_id
        );
        if fully_identified_same_cluster {
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
    /// records, current-process holders, and records owned by another node are
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
        if record.holder_cluster_id.as_deref() != Some(current_process.cluster_id.as_str())
            || record.holder_node_id.as_ref() != Some(local_node_id)
            || record
                .holder_session_id
                .as_deref()
                .is_none_or(|session_id| session_id == current_process.process_session_id)
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
            holder_cluster_id: Some(holder_process.cluster_id.clone()),
            holder_node_id: Some(holder_node_id.clone()),
            holder_session_id: Some(holder_process.process_session_id.clone()),
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

    fn initial_placement_reconciliation_queue(
        &self,
        file_system_id: &DfsFileSystemId,
        current_config: &DfsFileSystemConfig,
    ) -> Result<DfsPlacementReconciliationQueue, KvError> {
        let mut jobs = self
            .kv
            .range_prefix(&placement_reconciliations_prefix(file_system_id))?
            .into_iter()
            .map(|entry| entry.decode::<DfsPlacementReconciliationJob>())
            .collect::<Result<Vec<_>, _>>()?;
        for job in &jobs {
            validate_v1_placement_reconciliation_job(job)?;
        }
        jobs.retain(|job| !job.state.is_terminal());
        jobs.sort_by(|left, right| {
            left.created_at_unix_nanos
                .cmp(&right.created_at_unix_nanos)
                .then_with(|| left.job_id.cmp(&right.job_id))
        });
        if jobs.len() > MAX_ACTIVE_PLACEMENT_RECONCILIATIONS {
            return Err(KvError::RangeLimit {
                prefix: placement_reconciliations_prefix(file_system_id),
                max_entries: MAX_ACTIVE_PLACEMENT_RECONCILIATIONS,
            });
        }

        // Run jobs that are still authoritative before stale work. Repeated
        // same-path Apply operations therefore put the newest useful job at
        // the head without starving older jobs for unrelated scopes.
        let (authoritative, superseded): (Vec<_>, Vec<_>) = jobs.into_iter().partition(|job| {
            job.accepted_config.as_ref().is_some_and(|accepted| {
                placement_config_remains_authoritative_for_job(accepted, current_config, job)
            })
        });
        let job_ids = authoritative
            .into_iter()
            .chain(superseded)
            .map(|job| job.job_id)
            .collect();
        Ok(DfsPlacementReconciliationQueue {
            schema_version: PLACEMENT_RECONCILIATION_QUEUE_SCHEMA_VERSION,
            job_ids,
        })
    }

    fn placement_reconciliation_queue_snapshot(
        &self,
        file_system_id: &DfsFileSystemId,
        current_config: &DfsFileSystemConfig,
    ) -> Result<(Option<KvEntry>, DfsPlacementReconciliationQueue), KvError> {
        let key = placement_reconciliation_queue_key(file_system_id);
        let Some(entry) = self.kv.get(&key)? else {
            return self
                .initial_placement_reconciliation_queue(file_system_id, current_config)
                .map(|queue| (None, queue));
        };
        let queue = entry.decode::<DfsPlacementReconciliationQueue>()?;
        validate_placement_reconciliation_queue(file_system_id, &queue)?;
        Ok((Some(entry), queue))
    }

    fn placement_reconciliation_activity_index_snapshot(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<(Option<KvEntry>, DfsPlacementReconciliationActivityIndex), KvError> {
        let key = placement_reconciliation_activity_index_key(file_system_id);
        let Some(entry) = self.kv.get(&key)? else {
            // An existing filesystem with no index predates this projection.
            // Do not scan terminal history to migrate it on an operator read;
            // seed bounded state as jobs are next created or advanced and keep
            // the compatibility gap explicit on every page.
            let index = DfsPlacementReconciliationActivityIndex {
                partial: true,
                history_truncated: true,
                ..DfsPlacementReconciliationActivityIndex::default()
            };
            return Ok((None, index));
        };
        let index = entry.decode::<DfsPlacementReconciliationActivityIndex>()?;
        validate_placement_reconciliation_activity_index(file_system_id, &index)?;
        Ok((Some(entry), index))
    }

    fn placement_reconciliation_activity_index_update(
        &self,
        job: &DfsPlacementReconciliationJob,
    ) -> Result<(Option<(KvCompare, KvOp)>, Vec<String>), KvError> {
        let key = placement_reconciliation_activity_index_key(&job.file_system_id);
        let (entry, mut index) =
            self.placement_reconciliation_activity_index_snapshot(&job.file_system_id)?;
        let (changed, evicted_job_ids) =
            include_placement_reconciliation_activity(&mut index, job)?;
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
            Some((compare, KvOp::put_serde(key, &index, None)?)),
            evicted_job_ids,
        ))
    }

    fn compact_and_prioritize_placement_reconciliation_queue(
        &self,
        file_system_id: &DfsFileSystemId,
        queue: DfsPlacementReconciliationQueue,
        desired_config: &DfsFileSystemConfig,
        new_job_id: Option<&str>,
    ) -> Result<DfsPlacementReconciliationQueue, KvError> {
        let mut authoritative = Vec::new();
        let mut superseded = Vec::new();
        for job_id in queue.job_ids {
            let Some(job) = self.v1_placement_reconciliation(file_system_id, &job_id)? else {
                continue;
            };
            validate_v1_placement_reconciliation_job(&job)?;
            if job.state.is_terminal() {
                continue;
            }
            let target = if job.accepted_config.as_ref().is_some_and(|accepted| {
                placement_config_remains_authoritative_for_job(accepted, desired_config, &job)
            }) {
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

    /// Atomically publish a placement-rule edit and its durable reconciliation
    /// job. The compare prevents two Files clients from silently losing one
    /// another's policy changes.
    pub fn compare_and_put_placement_update(
        &mut self,
        expected: &DfsFileSystemConfig,
        desired: &DfsFileSystemConfig,
        job: &DfsPlacementReconciliationJob,
    ) -> Result<bool, KvError> {
        let config_key = file_system_config_key(&expected.file_system_id);
        let Some(current_entry) = self.kv.get(&config_key)? else {
            return Ok(false);
        };
        if current_entry.decode::<DfsFileSystemConfig>()? != *expected {
            return Ok(false);
        }
        let mut durable_job = job.clone();
        // Bind the reconciliation to exactly the config committed by this KV
        // transaction even if a future caller forgets to populate the public
        // snapshot field before calling the store.
        durable_job.accepted_config = Some(scoped_placement_config_for_job(desired, job));
        validate_v1_placement_reconciliation_job(&durable_job)?;
        let job_key = placement_reconciliation_key(&job.file_system_id, &job.job_id);
        let legacy_job_key = legacy_placement_reconciliation_key(&job.file_system_id, &job.job_id);
        let queue_key = placement_reconciliation_queue_key(&job.file_system_id);
        let (queue_entry, queue) =
            self.placement_reconciliation_queue_snapshot(&job.file_system_id, desired)?;
        let queue = self.compact_and_prioritize_placement_reconciliation_queue(
            &job.file_system_id,
            queue,
            desired,
            Some(&job.job_id),
        )?;
        let queue_compare = match queue_entry {
            Some(entry) => KvCompare::ValueEquals {
                key: queue_key.clone(),
                value: entry.value,
            },
            None => KvCompare::Missing {
                key: queue_key.clone(),
            },
        };
        let permit_compare =
            self.placement_authority_permit_missing_compare(&job.file_system_id)?;
        let activity_record_key =
            placement_reconciliation_activity_record_key(&job.file_system_id, &job.job_id);
        let activity_record = DfsPlacementReconciliationActivityRecord::from(&durable_job);
        let (activity_index_update, evicted_activity_job_ids) =
            self.placement_reconciliation_activity_index_update(&durable_job)?;
        let mut compares = vec![
            KvCompare::ValueEquals {
                key: config_key.clone(),
                value: current_entry.value,
            },
            KvCompare::Missing {
                key: job_key.clone(),
            },
            KvCompare::Missing {
                key: legacy_job_key,
            },
            KvCompare::Missing {
                key: activity_record_key.clone(),
            },
            queue_compare,
            permit_compare,
        ];
        let mut success = vec![
            KvOp::put_serde(config_key.clone(), desired, None)?,
            KvOp::put_serde(job_key.clone(), &durable_job, None)?,
            KvOp::put_serde(activity_record_key, &activity_record, None)?,
            KvOp::put_serde(queue_key.clone(), &queue, None)?,
        ];
        if let Some((compare, operation)) = activity_index_update {
            compares.push(compare);
            success.push(operation);
        }
        for evicted_job_id in evicted_activity_job_ids {
            if !queue.job_ids.contains(&evicted_job_id) {
                success.push(KvOp::Delete {
                    key: placement_reconciliation_activity_record_key(
                        &job.file_system_id,
                        &evicted_job_id,
                    ),
                });
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

    pub fn placement_reconciliation(
        &self,
        file_system_id: &DfsFileSystemId,
        job_id: &str,
    ) -> Result<Option<DfsPlacementReconciliationJob>, KvError> {
        if let Some(job) = self.v1_placement_reconciliation(file_system_id, job_id)? {
            return Ok(Some(job));
        }
        self.get_serde(&legacy_placement_reconciliation_key(file_system_id, job_id))
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
        let v1_job_ids = jobs
            .iter()
            .map(|job| job.job_id.clone())
            .collect::<BTreeSet<_>>();
        jobs.extend(
            self.kv
                .range_prefix(&legacy_placement_reconciliations_prefix(file_system_id))?
                .into_iter()
                .map(|entry| entry.decode::<DfsPlacementReconciliationJob>())
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|job| !v1_job_ids.contains(&job.job_id)),
        );
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
            } else if index_entry.is_none() {
                // A pre-queue/pre-index daemon may still have legacy terminal
                // or active jobs. Reporting the compatibility gap is safer
                // than an unbounded history scan on every UI poll.
                partial = true;
                history_truncated = true;
            }
        }

        let mut activities = Vec::with_capacity(job_ids.len());
        for job_id in job_ids {
            let activity_key =
                placement_reconciliation_activity_record_key(file_system_id, &job_id);
            let (mut activity, from_compact_record) = match self.kv.get(&activity_key)? {
                Some(entry) => {
                    let record = entry.decode::<DfsPlacementReconciliationActivityRecord>()?;
                    validate_placement_reconciliation_activity_record(
                        file_system_id,
                        &job_id,
                        &record,
                    )?;
                    (record.activity, true)
                }
                None => {
                    partial = true;
                    let Some(job) = self.v1_placement_reconciliation(file_system_id, &job_id)?
                    else {
                        history_truncated = true;
                        continue;
                    };
                    validate_v1_placement_reconciliation_job(&job)?;
                    if job.file_system_id != *file_system_id || job.job_id != job_id {
                        return Err(KvError::Serialization {
                            detail: format!(
                                "placement reconciliation job {job_id} does not match its filesystem or job key"
                            ),
                        });
                    }
                    (DfsPlacementReconciliationActivitySummary::from(&job), false)
                }
            };
            if from_compact_record && !activity.phase.is_terminal() {
                // A rolling-upgrade writer can advance the full v1 job without
                // knowing about compact activity records. Reconcile every
                // projected nonterminal against that one bounded point read so
                // stale `Running` cannot survive a terminal transition.
                let Some(job) = self.v1_placement_reconciliation(file_system_id, &job_id)? else {
                    partial = true;
                    history_truncated = true;
                    continue;
                };
                validate_v1_placement_reconciliation_job(&job)?;
                if job.file_system_id != *file_system_id || job.job_id != job_id {
                    return Err(KvError::Serialization {
                        detail: format!(
                            "placement reconciliation job {job_id} does not match its filesystem or job key"
                        ),
                    });
                }
                let current = DfsPlacementReconciliationActivitySummary::from(&job);
                if current.revision < activity.revision {
                    return Err(KvError::Serialization {
                        detail: format!(
                            "placement reconciliation activity record {job_id} is newer than its durable job"
                        ),
                    });
                }
                if current != activity {
                    activity = current;
                    partial = true;
                }
            }
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

    /// Return the next executable v1 placement job without loading terminal
    /// history. A missing queue is initialized once from the v1 namespace;
    /// afterward ticks use one compact queue read and one point lookup.
    pub fn next_placement_reconciliation(
        &mut self,
        file_system_id: &DfsFileSystemId,
        current_config: &DfsFileSystemConfig,
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
            let Some(job_id) = queue.job_ids.first() else {
                return Ok(None);
            };
            if let Some(job) = self.v1_placement_reconciliation(file_system_id, job_id)? {
                validate_v1_placement_reconciliation_job(&job)?;
                if !job.state.is_terminal() {
                    return Ok(Some(job));
                }
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

    pub fn compare_and_put_placement_reconciliation(
        &mut self,
        expected: &DfsPlacementReconciliationJob,
        desired: &DfsPlacementReconciliationJob,
    ) -> Result<bool, KvError> {
        validate_v1_placement_reconciliation_job(expected)?;
        validate_v1_placement_reconciliation_job(desired)?;
        validate_placement_reconciliation_snapshot_identity(expected, desired)?;
        let key = placement_reconciliation_key(&expected.file_system_id, &expected.job_id);
        let Some(current_entry) = self.kv.get(&key)? else {
            return Ok(false);
        };
        if current_entry.decode::<DfsPlacementReconciliationJob>()? != *expected {
            return Ok(false);
        }
        let queue_key = placement_reconciliation_queue_key(&desired.file_system_id);
        let queue_entry = self.kv.get(&queue_key)?;
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
        let mut compares = vec![KvCompare::ValueEquals {
            key: key.clone(),
            value: current_entry.value,
        }];
        let activity_record_key =
            placement_reconciliation_activity_record_key(&desired.file_system_id, &desired.job_id);
        let activity_record = DfsPlacementReconciliationActivityRecord::from(desired);
        let mut success = vec![
            KvOp::put_serde(key, desired, None)?,
            KvOp::put_serde(activity_record_key, &activity_record, None)?,
        ];
        let (activity_index_update, evicted_activity_job_ids) =
            self.placement_reconciliation_activity_index_update(desired)?;
        if let Some((compare, operation)) = activity_index_update {
            compares.push(compare);
            success.push(operation);
        }
        if desired.state == crate::DfsPlacementReconciliationState::Failed {
            // `Failed` revokes authority for delayed operations and receipts;
            // order that transition after any currently permitted one-object
            // mutation just like a replacement Apply.
            compares
                .push(self.placement_authority_permit_missing_compare(&desired.file_system_id)?);
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
        Ok(self
            .kv
            .txn(KvTxn {
                compares,
                success,
                failure: Vec::new(),
            })?
            .succeeded)
    }

    pub fn put_metadata_shard(&mut self, shard: &DfsMetadataShardConfig) -> Result<(), KvError> {
        let key = metadata_shard_key(&shard.file_system_id, &shard.shard_id);
        for _ in 0..METADATA_MEMBERSHIP_HANDOFF_RETRIES {
            let current_entry = self.kv.get(&key)?;
            if let Some(entry) = current_entry.as_ref() {
                let current = entry.decode::<DfsMetadataShardConfig>()?;
                validate_raw_metadata_namespace_prepare_codec_write(&current, shard)?;
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
                compares.extend(metadata_namespace_prepare_codec_admission_compares(
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
            compares.extend(metadata_namespace_prepare_codec_admission_compares(
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
        validate_metadata_shard_cas_identity(Some(retiring), desired)?;
        if retiring == desired {
            return Err(KvError::Serialization {
                detail: format!(
                    "metadata shard {} membership handoff does not change the assignment",
                    retiring.shard_id
                ),
            });
        }
        validate_metadata_shard_cas_reconfiguration(retiring, desired)?;
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
                    && handoff.desired_shard == *desired)
                    .then_some(handoff));
            }

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
                handoff_id: sinomo_core::Id::generate().to_string(),
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
            ];
            compares.extend(metadata_namespace_prepare_codec_admission_compares(
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
    pub fn complete_metadata_shard_membership_handoff(
        &mut self,
        retiring: &DfsMetadataShardConfig,
        desired: &DfsMetadataShardConfig,
        proof: &DfsMetadataShardMembershipHandoffProof,
    ) -> Result<bool, KvError> {
        validate_metadata_shard_cas_identity(Some(retiring), desired)?;
        validate_metadata_shard_cas_reconfiguration(retiring, desired)?;
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
        if handoff.retiring_shard != *retiring || handoff.desired_shard != *desired {
            return Ok(false);
        }
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
        ];
        compares.extend(metadata_namespace_prepare_codec_admission_compares(
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
                ],
                failure: Vec::new(),
            })?
            .succeeded)
    }

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

    pub fn list_metadata_shards(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsMetadataShardConfig>, KvError> {
        let mut shards: Vec<DfsMetadataShardConfig> = self
            .kv
            .range_prefix(&format!("{}/", metadata_shards_prefix(file_system_id)))?
            .into_iter()
            .map(|entry| entry.decode())
            .collect::<Result<Vec<_>, _>>()?;
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

    /// Obtain an operation-bound quorum permit while the exact shard
    /// assignment is still committed. This is the partition fallback for a
    /// canonical DFS mutation. The fixed per-shard record also advances the
    /// minimum prepared journal sequence a future primary must have replayed;
    /// it is bounded consensus state, not per-file metadata or an operation
    /// log.
    pub fn certify_metadata_shard_operation(
        &mut self,
        expected: &DfsMetadataShardConfig,
        operation_digest: &str,
        prepared_sequence: u64,
    ) -> Result<Option<String>, KvError> {
        if operation_digest.is_empty() || prepared_sequence == 0 {
            return Ok(None);
        }
        let shard_key = metadata_shard_key(&expected.file_system_id, &expected.shard_id);
        let floor_key =
            metadata_shard_operation_floor_key(&expected.file_system_id, &expected.shard_id);
        let handoff_key =
            metadata_shard_membership_handoff_key(&expected.file_system_id, &expected.shard_id);
        for attempt in 0..METADATA_OPERATION_FLOOR_PUBLISH_RETRIES {
            let retryable_error = match self.certify_metadata_shard_operation_once(
                expected,
                operation_digest,
                prepared_sequence,
                &shard_key,
                &floor_key,
                &handoff_key,
            ) {
                Ok(MetadataOperationPermitAttempt::Certified(permit_id)) => {
                    return Ok(Some(permit_id));
                }
                Ok(MetadataOperationPermitAttempt::Rejected) => return Ok(None),
                Ok(MetadataOperationPermitAttempt::RetryAfterCompareMiss) => None,
                Err(error) if metadata_operation_authority_view_error_is_retryable(&error) => {
                    Some(error)
                }
                Err(error) => return Err(error),
            };
            if attempt + 1 == METADATA_OPERATION_FLOOR_PUBLISH_RETRIES {
                return match retryable_error {
                    Some(error) => Err(error),
                    None => Ok(None),
                };
            }
            // Reads may come from a lagging materialized projection while the
            // transaction compare executes against authoritative Raft. A
            // ReadIndex generation race is equally observational: refresh and
            // replay the same digest/prepare sequence instead of leaking a
            // commit-ready DFS intent to the caller as a permanent failure.
            if let Err(error) = self.kv.refresh_latest() {
                if metadata_operation_authority_view_error_is_retryable(&error) {
                    continue;
                }
                return Err(error);
            }
        }
        unreachable!("metadata operation floor retry loop always returns")
    }

    fn certify_metadata_shard_operation_once(
        &mut self,
        expected: &DfsMetadataShardConfig,
        operation_digest: &str,
        prepared_sequence: u64,
        shard_key: &str,
        floor_key: &str,
        handoff_key: &str,
    ) -> Result<MetadataOperationPermitAttempt, KvError> {
        let Some(shard_entry) = self.kv.get(shard_key)? else {
            return Ok(MetadataOperationPermitAttempt::Rejected);
        };
        if shard_entry.decode::<DfsMetadataShardConfig>()? != *expected {
            return Ok(MetadataOperationPermitAttempt::Rejected);
        }
        if self.kv.get(handoff_key)?.is_some() {
            return Ok(MetadataOperationPermitAttempt::Rejected);
        }

        let floor_entry = self.kv.get(floor_key)?;
        if let Some(previous_entry) = floor_entry.as_ref() {
            let previous = previous_entry.decode::<DfsMetadataShardOperationFloor>()?;
            if previous.authority_epoch == expected.epoch
                && previous.prepared_sequence == prepared_sequence
                && previous.operation_digest == operation_digest
                && !previous.permit_id.is_empty()
            {
                // The floor may come from an observational projection. Reuse
                // its permit only after an authoritative transaction verifies
                // the assignment, exact floor bytes, and missing handoff.
                let response = self.kv.txn(KvTxn {
                    compares: vec![
                        KvCompare::ValueEquals {
                            key: shard_key.to_string(),
                            value: shard_entry.value.clone(),
                        },
                        KvCompare::ValueEquals {
                            key: floor_key.to_string(),
                            value: previous_entry.value.clone(),
                        },
                        KvCompare::Missing {
                            key: handoff_key.to_string(),
                        },
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
                })?;
                return if response.succeeded {
                    Ok(MetadataOperationPermitAttempt::Certified(
                        previous.permit_id,
                    ))
                } else {
                    Ok(MetadataOperationPermitAttempt::RetryAfterCompareMiss)
                };
            }
            if previous.authority_epoch > expected.epoch
                || previous.prepared_sequence > prepared_sequence
                || (previous.prepared_sequence == prepared_sequence
                    && previous.operation_digest != operation_digest)
            {
                return Ok(MetadataOperationPermitAttempt::Rejected);
            }
        }

        let permit_id = sinomo_core::Id::generate().to_string();
        let floor = DfsMetadataShardOperationFloor {
            operation_digest: operation_digest.to_string(),
            prepared_sequence,
            authority_epoch: expected.epoch,
            permit_id: permit_id.clone(),
        };
        let floor_compare = match floor_entry.as_ref() {
            Some(entry) => KvCompare::ValueEquals {
                key: floor_key.to_string(),
                value: entry.value.clone(),
            },
            None => KvCompare::Missing {
                key: floor_key.to_string(),
            },
        };
        let response = self.kv.txn(KvTxn {
            compares: vec![
                KvCompare::ValueEquals {
                    key: shard_key.to_string(),
                    value: shard_entry.value,
                },
                floor_compare,
                KvCompare::Missing {
                    key: handoff_key.to_string(),
                },
            ],
            success: vec![KvOp::put_serde(floor_key.to_string(), &floor, None)?],
            failure: Vec::new(),
        })?;
        if response.succeeded {
            Ok(MetadataOperationPermitAttempt::Certified(permit_id))
        } else {
            Ok(MetadataOperationPermitAttempt::RetryAfterCompareMiss)
        }
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

    /// Return the unchanged shard for an idempotent request, but fail closed
    /// for an actual primary change.
    ///
    /// Cluster KV cannot certify the current DFS journal head because healthy
    /// full-shard writes intentionally do not update a Raft watermark. A future
    /// proof-bearing data-plane handoff must freeze the retiring primary and
    /// prove that exact prefix on the promoted replica before this surface can
    /// activate a different primary.
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

    async fn placement_authority_permit_missing_compare(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<KvCompare, KvError> {
        let key = placement_authority_permit_key(file_system_id);
        for _ in 0..PLACEMENT_AUTHORITY_RECOVERY_CAS_RETRIES {
            let Some(entry) = self.kv.get_async(&key).await? else {
                return Ok(KvCompare::Missing { key });
            };
            let record = entry.decode::<DfsPlacementAuthorityPermitRecord>()?;
            if !placement_authority_release_is_pending(file_system_id, &record) {
                return Err(KvError::Busy {
                    path: placement_authority_busy_path(file_system_id, &record),
                });
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
                validate_raw_metadata_namespace_prepare_codec_write(&current, shard)?;
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
                    metadata_namespace_prepare_codec_admission_compares_async(self.kv, shard)
                        .await?,
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
            compares.extend(
                metadata_namespace_prepare_codec_admission_compares_async(self.kv, desired).await?,
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

    pub async fn begin_metadata_shard_membership_handoff(
        &mut self,
        retiring: &DfsMetadataShardConfig,
        desired: &DfsMetadataShardConfig,
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
        validate_metadata_shard_cas_reconfiguration(retiring, desired)?;
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
                    && handoff.desired_shard == *desired)
                    .then_some(handoff));
            }

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
                handoff_id: sinomo_core::Id::generate().to_string(),
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
            ];
            compares.extend(
                metadata_namespace_prepare_codec_admission_compares_async(self.kv, desired).await?,
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
        validate_metadata_shard_cas_reconfiguration(retiring, desired)?;
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
        if handoff.retiring_shard != *retiring || handoff.desired_shard != *desired {
            return Ok(false);
        }
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
        ];
        compares.extend(
            metadata_namespace_prepare_codec_admission_compares_async(self.kv, desired).await?,
        );
        Ok(self
            .kv
            .txn_async(KvTxn {
                compares,
                success: vec![
                    KvOp::put_serde(shard_key, desired, None)?,
                    KvOp::put_serde(watermark_key, &activated_watermark, None)?,
                    KvOp::put_serde(proof_key, &proof, None)?,
                    KvOp::Delete { key: handoff_key },
                ],
                failure: Vec::new(),
            })
            .await?
            .succeeded)
    }

    pub async fn get_metadata_shard_membership_handoff(
        &self,
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
        &self,
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
        &self,
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
        &self,
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
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        select_metadata_shard_config_for_path(
            file_system_id,
            &self.list_metadata_shards(file_system_id).await?,
            path,
        )
    }

    pub async fn select_metadata_shard_for_path_strict_default_sifs_split(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsMetadataShardConfig>, KvError> {
        select_metadata_shard_config_for_path_strict_default_sifs_split(
            file_system_id,
            &self.list_metadata_shards(file_system_id).await?,
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

    async fn get_serde<T>(&self, key: &str) -> Result<Option<T>, KvError>
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
    validate_metadata_namespace_prepare_codec_transition(expected, desired)?;

    if expected.primary_node_id == desired.primary_node_id {
        let membership_changed = metadata_shard_configured_node_ids(expected)
            != metadata_shard_configured_node_ids(desired);
        let codec_changed = expected.namespace_prepare_codec != desired.namespace_prepare_codec;
        if !membership_changed && !codec_changed {
            return Err(KvError::Serialization {
                detail: format!(
                    "metadata shard {} handoff must change the configured replica set or namespace prepare codec",
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

fn validate_metadata_namespace_prepare_codec_transition(
    expected: &DfsMetadataShardConfig,
    desired: &DfsMetadataShardConfig,
) -> Result<(), KvError> {
    if desired.namespace_prepare_codec.rank() < expected.namespace_prepare_codec.rank() {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} cannot downgrade namespace prepare codec {} to {} without a compatible snapshot, including a legacy-readable snapshot when required, and replay-floor proof",
                expected.shard_id,
                expected.namespace_prepare_codec.label(),
                desired.namespace_prepare_codec.label(),
            ),
        });
    }
    Ok(())
}

fn validate_raw_metadata_namespace_prepare_codec_write(
    current: &DfsMetadataShardConfig,
    desired: &DfsMetadataShardConfig,
) -> Result<(), KvError> {
    validate_metadata_namespace_prepare_codec_transition(current, desired)?;
    if current.namespace_prepare_codec != desired.namespace_prepare_codec
        || (current.namespace_prepare_codec.is_packed() && current != desired)
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} cannot activate or reconfigure {} namespace prepares through a raw put; use the capability-validated metadata membership handoff",
                current.shard_id,
                desired.namespace_prepare_codec.label(),
            ),
        });
    }
    Ok(())
}

fn metadata_namespace_prepare_codec_admission_compares(
    kv: &(impl DfsControlPlaneKvClient + ?Sized),
    desired: &DfsMetadataShardConfig,
) -> Result<Vec<KvCompare>, KvError> {
    let Some(required_capability) = desired.namespace_prepare_codec.required_capability() else {
        return Ok(Vec::new());
    };
    let mut cluster_id = None;
    let mut compares = Vec::new();
    for node_id in metadata_shard_configured_node_ids(desired) {
        let member_key = cluster_member_key(&node_id);
        let member_entry =
            kv.get(&member_key)?
                .ok_or_else(|| KvError::Serialization {
                    detail: format!(
                        "metadata shard {} cannot activate {} namespace prepares: assigned node {} has no authoritative cluster member capability record",
                        desired.shard_id,
                        desired.namespace_prepare_codec.label(),
                        node_id,
                    ),
                })?;
        let policy_key = cluster_capability_policy_key(&node_id);
        let policy_entry = kv.get(&policy_key)?;
        compares.extend(metadata_namespace_prepare_codec_member_compares(
            desired,
            &node_id,
            member_key,
            member_entry,
            policy_key,
            policy_entry,
            &mut cluster_id,
            required_capability,
        )?);
    }
    Ok(compares)
}

async fn metadata_namespace_prepare_codec_admission_compares_async(
    kv: &mut (impl AsyncDfsControlPlaneKvClient + ?Sized),
    desired: &DfsMetadataShardConfig,
) -> Result<Vec<KvCompare>, KvError> {
    let Some(required_capability) = desired.namespace_prepare_codec.required_capability() else {
        return Ok(Vec::new());
    };
    let mut cluster_id = None;
    let mut compares = Vec::new();
    for node_id in metadata_shard_configured_node_ids(desired) {
        let member_key = cluster_member_key(&node_id);
        let member_entry = kv
            .get_async(&member_key)
            .await?
            .ok_or_else(|| KvError::Serialization {
                detail: format!(
                    "metadata shard {} cannot activate {} namespace prepares: assigned node {} has no authoritative cluster member capability record",
                    desired.shard_id,
                    desired.namespace_prepare_codec.label(),
                    node_id,
                ),
            })?;
        let policy_key = cluster_capability_policy_key(&node_id);
        let policy_entry = kv.get_async(&policy_key).await?;
        compares.extend(metadata_namespace_prepare_codec_member_compares(
            desired,
            &node_id,
            member_key,
            member_entry,
            policy_key,
            policy_entry,
            &mut cluster_id,
            required_capability,
        )?);
    }
    Ok(compares)
}

fn metadata_namespace_prepare_codec_member_compares(
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
                desired.shard_id,
                desired.namespace_prepare_codec.label(),
                node_id,
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
                desired.shard_id,
                desired.namespace_prepare_codec.label(),
                node_id,
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
                desired.shard_id,
                desired.namespace_prepare_codec.label(),
                node_id,
                required_capability,
            ),
        });
    }
    if cluster_id
        .as_ref()
        .is_some_and(|cluster_id| *cluster_id != member.cluster_id)
    {
        return Err(KvError::Serialization {
            detail: format!(
                "metadata shard {} cannot activate {} namespace prepares across inconsistent cluster member records",
                desired.shard_id,
                desired.namespace_prepare_codec.label(),
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
    if proof.journal_sequence != retiring_fence.journal_sequence
        || proof.state_fingerprint != retiring_fence.state_fingerprint
    {
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
    if let Some(required_head_watermark) = handoff.required_head_watermark.as_ref()
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

pub(crate) fn select_metadata_shard_config_for_path(
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
            if prefix == placement_reconciliations_prefix(&file_system_id()) {
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
    enum PackedCapabilityRecordKind {
        Member,
        Policy,
    }

    #[derive(Debug)]
    struct DropPackedCapabilityBeforeTxnKv {
        inner: InProcessKvStore,
        record_key: String,
        record_kind: PackedCapabilityRecordKind,
        drop_before_next_txn: bool,
    }

    impl DfsControlPlaneKvClient for DropPackedCapabilityBeforeTxnKv {
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
            if self.drop_before_next_txn {
                match self.record_kind {
                    PackedCapabilityRecordKind::Member => {
                        let mut member = self
                            .inner
                            .get(&self.record_key)
                            .expect("member capability record")
                            .decode::<ClusterMemberRecord>()
                            .expect("decode member capability record");
                        member.capabilities.retain(|capability| {
                            capability.as_str()
                                != DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY
                        });
                        self.inner
                            .put_serde(self.record_key.clone(), &member, None)
                            .expect("drop packed prepare member capability");
                    }
                    PackedCapabilityRecordKind::Policy => {
                        let mut policy = self
                            .inner
                            .get(&self.record_key)
                            .expect("capability policy record")
                            .decode::<ClusterCapabilityPolicyRecord>()
                            .expect("decode capability policy record");
                        policy.capabilities.retain(|capability| {
                            capability.as_str()
                                != DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY
                        });
                        self.inner
                            .put_serde(self.record_key.clone(), &policy, None)
                            .expect("drop packed prepare policy capability");
                    }
                }
                self.drop_before_next_txn = false;
            }
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
    struct RetryAuthoritativeViewKv {
        inner: InProcessKvStore,
        remaining_retryable_get_failures: AtomicUsize,
        refresh_calls: AtomicUsize,
    }

    impl DfsControlPlaneKvClient for RetryAuthoritativeViewKv {
        fn refresh_latest(&mut self) -> Result<(), KvError> {
            self.refresh_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
            if self
                .remaining_retryable_get_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    (remaining > 0).then(|| remaining - 1)
                })
                .is_ok()
            {
                return Err(KvError::Io {
                    path: "test-cluster-authoritative-view".to_string(),
                    detail: "resident Raft view changed after ReadIndex quorum; retry the read"
                        .to_string(),
                });
            }
            Ok(self.inner.get(key).cloned())
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
            file_system_id: file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: vec![DfsSubtreePolicy {
                path_prefix: "/users/team".to_string(),
                policy: DfsObjectPolicy::ReplicatedEventual,
                scope: DfsPlacementScope::Subtree,
                min_commit_replicas: Some(1),
                desired_replicas: Some(desired_replicas),
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
            accepted_config: None,
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

    fn placement_authority_test_process_identity() -> DfsPlacementAuthorityProcessIdentity {
        DfsPlacementAuthorityProcessIdentity {
            cluster_id: "placement-test-cluster".to_string(),
            process_session_id: "placement-test-process-session".to_string(),
        }
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
            holder_cluster_id: cluster_id.map(ToString::to_string),
            holder_node_id,
            holder_session_id: holder_session_id.map(ToString::to_string),
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

    fn put_packed_namespace_prepare_capable_members<'a>(
        kv: &mut InProcessKvStore,
        node_ids: impl IntoIterator<Item = &'a NodeId>,
    ) {
        let cluster_id = sinomo_core::Id::from_legacy_named("packed-prepare-capability-cluster")
            .expect("cluster id");
        let capability =
            sinomo_core::CapabilityName::new(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY)
                .expect("static capability");
        for node_id in node_ids {
            let mut member = ClusterMemberRecord::local_non_voting(
                cluster_id.clone(),
                node_id.clone(),
                "packed prepare capable member",
                format!("{node_id}-public-key"),
                time::OffsetDateTime::UNIX_EPOCH,
            );
            member.state = ClusterMemberState::Active;
            member.capabilities.push(capability.clone());
            kv.put_serde(cluster_member_key(node_id), &member, None)
                .expect("put packed prepare capability member");
        }
    }

    fn add_packed_v2_namespace_prepare_capability<'a>(
        kv: &mut InProcessKvStore,
        node_ids: impl IntoIterator<Item = &'a NodeId>,
    ) {
        let capability =
            sinomo_core::CapabilityName::new(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V2_CAPABILITY)
                .expect("static capability");
        for node_id in node_ids {
            let key = cluster_member_key(node_id);
            let mut member = kv
                .get(&key)
                .expect("cluster member capability record")
                .decode::<ClusterMemberRecord>()
                .expect("decode cluster member capability record");
            if !member.capabilities.contains(&capability) {
                member.capabilities.push(capability.clone());
            }
            kv.put_serde(key, &member, None)
                .expect("add packed-v2 prepare capability");
        }
    }

    fn put_packed_namespace_prepare_capability_policy(
        kv: &mut InProcessKvStore,
        node_id: &NodeId,
        capable: bool,
    ) {
        let member = kv
            .get(&cluster_member_key(node_id))
            .expect("cluster member")
            .decode::<ClusterMemberRecord>()
            .expect("decode cluster member");
        let mut policy = ClusterCapabilityPolicyRecord::from(&member);
        if !capable {
            policy.capabilities.retain(|capability| {
                capability.as_str() != DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY
            });
        }
        kv.put_serde(cluster_capability_policy_key(node_id), &policy, None)
            .expect("put packed prepare capability policy");
    }

    #[test]
    fn placement_policy_and_reconciliation_job_commit_atomically() {
        let mut kv = InProcessKvStore::new();
        let file_system_id = file_system_id();
        let expected = DfsFileSystemConfig {
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
            min_commit_replicas: Some(1),
            desired_replicas: Some(3),
            required_node_ids: vec![node("placement-required")],
            forbidden_node_ids: Vec::new(),
            min_distinct_zones: 2,
        });
        desired.subtree_policies.push(DfsSubtreePolicy {
            path_prefix: "/elsewhere".to_string(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            scope: DfsPlacementScope::Subtree,
            min_commit_replicas: Some(1),
            desired_replicas: Some(1),
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
            accepted_config: None,
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
        durable_job.accepted_config = Some(scoped_placement_config_for_job(&desired, &job));
        assert_eq!(
            durable_job
                .accepted_config
                .as_ref()
                .expect("accepted config")
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
        assert!(
            store
                .kv
                .range_prefix(&legacy_placement_reconciliations_prefix(&file_system_id))
                .expect("legacy placement prefix")
                .is_empty(),
            "a legacy worker must not be able to list a v1 reconciliation job"
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
    fn placement_activity_rolling_upgrade_fallback_is_bounded_and_explicitly_partial() {
        let file_system_id = file_system_id();
        let owner = node("placement-activity-fallback-owner");
        let config = placement_authority_test_config(&owner, 1);
        let mut job = placement_authority_test_job(
            "placement-activity-fallback-job",
            &file_system_id,
            &owner,
            7,
        );
        job.accepted_config = Some(scoped_placement_config_for_job(&config, &job));

        // Model a v1 daemon from before compact activity records. It has the
        // bounded active queue and full job, but no activity index/record.
        let mut kv = InProcessKvStore::new();
        kv.put_serde(file_system_config_key(&file_system_id), &config, None)
            .expect("put legacy-compatible config");
        kv.put_serde(
            placement_reconciliation_key(&file_system_id, &job.job_id),
            &job,
            None,
        )
        .expect("put legacy-compatible active job");
        kv.put_serde(
            placement_reconciliation_queue_key(&file_system_id),
            &DfsPlacementReconciliationQueue {
                schema_version: PLACEMENT_RECONCILIATION_QUEUE_SCHEMA_VERSION,
                job_ids: vec![job.job_id.clone()],
            },
            None,
        )
        .expect("put active queue");

        let mut kv = RejectPlacementHistoryRangeKv { inner: kv };
        let store = DfsControlPlaneStore::new(&mut kv);
        let page = store
            .placement_reconciliation_activity(
                &file_system_id,
                &[DfsPlacementReconciliationActivityStatus::Running],
                Some("/users"),
                None,
                10,
            )
            .expect("bounded rolling-upgrade activity");
        assert_eq!(page.activities.len(), 1);
        assert_eq!(page.activities[0].job_id, job.job_id);
        assert!(page.partial);
        assert!(page.history_truncated);
    }

    #[test]
    fn placement_activity_prefers_a_newer_full_job_from_an_older_writer() {
        let file_system_id = file_system_id();
        let owner = node("placement-activity-mixed-writer-owner");
        let base = DfsFileSystemConfig {
            file_system_id: file_system_id.clone(),
            display_name: "sifs".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
        };
        let desired = placement_authority_test_config(&owner, 1);
        let job = placement_authority_test_job(
            "placement-activity-mixed-writer-job",
            &file_system_id,
            &owner,
            9,
        );
        let mut kv = InProcessKvStore::new();
        let mut store = DfsControlPlaneStore::new(&mut kv);
        store.put_file_system_config(&base).expect("base config");
        assert!(
            store
                .compare_and_put_placement_update(&base, &desired, &job)
                .expect("new-writer Apply")
        );
        let durable = store
            .placement_reconciliation(&file_system_id, &job.job_id)
            .expect("durable job read")
            .expect("durable job");

        // Model an older v1 daemon: it knows the full job and active queue but
        // not the compact activity record introduced by this change.
        let mut completed = durable;
        completed.state = DfsPlacementReconciliationState::Completed;
        completed.revision = 1;
        completed.updated_at_unix_nanos = 10;
        kv.put_serde(
            placement_reconciliation_key(&file_system_id, &completed.job_id),
            &completed,
            None,
        )
        .expect("older writer completes full job");
        kv.put_serde(
            placement_reconciliation_queue_key(&file_system_id),
            &DfsPlacementReconciliationQueue::default(),
            None,
        )
        .expect("older writer removes active queue id");

        let mut kv = RejectPlacementHistoryRangeKv { inner: kv };
        let store = DfsControlPlaneStore::new(&mut kv);
        let page = store
            .placement_reconciliation_activity(&file_system_id, &[], None, None, 10)
            .expect("mixed-writer activity");
        assert_eq!(page.activities.len(), 1);
        assert_eq!(page.activities[0].revision, 1);
        assert_eq!(
            page.activities[0].status,
            DfsPlacementReconciliationActivityStatus::Completed
        );
        assert!(page.partial, "the compact projection was stale");
    }

    #[test]
    fn placement_activity_old_writer_gap_remains_partial_after_new_writer_bootstrap() {
        let file_system_id = file_system_id();
        let owner = node("placement-activity-monotonic-partial-owner");
        let base = DfsFileSystemConfig {
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
        old_terminal.accepted_config = Some(scoped_placement_config_for_job(&base, &old_terminal));
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
        active.accepted_config = Some(scoped_placement_config_for_job(&active_config, &active));
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
        newest_active.accepted_config =
            Some(scoped_placement_config_for_job(&config, &newest_active));
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
        oldest_active.accepted_config =
            Some(scoped_placement_config_for_job(&config, &oldest_active));
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
            file_system_id: custom.file_system_id.clone(),
            display_name: "bootstrap default".to_string(),
            owner_node_id: original_owner,
            subtree_policies: vec![DfsSubtreePolicy {
                path_prefix: "/services".to_string(),
                policy: DfsObjectPolicy::ReplicatedEventual,
                scope: DfsPlacementScope::Subtree,
                min_commit_replicas: Some(1),
                desired_replicas: Some(1),
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
    fn placement_apply_cannot_cross_a_held_metadata_mutation_permit() {
        let mut kv = InProcessKvStore::new();
        let owner = node("placement-permit-owner");
        let base = DfsFileSystemConfig {
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
            min_commit_replicas: Some(1),
            desired_replicas: Some(1),
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
            holder_cluster_id: Some(current_process.cluster_id.clone()),
            holder_node_id: Some(owner.clone()),
            holder_session_id: Some("placement-recovery-unauthorized-prior-session".to_string()),
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
    fn placement_prior_process_recovery_exact_cas_preserves_replacement_and_legacy_records() {
        let file_system_id = file_system_id();
        let owner = node("placement-recovery-cas-owner");
        let current_process = DfsPlacementAuthorityProcessIdentity {
            cluster_id: "placement-recovery-cas-cluster".to_string(),
            process_session_id: "placement-recovery-cas-current-session".to_string(),
        };
        let old = DfsPlacementAuthorityPermitRecord {
            job_id: "placement-recovery-cas-old-job".to_string(),
            holder_id: "placement-recovery-cas-old-holder".to_string(),
            holder_cluster_id: Some(current_process.cluster_id.clone()),
            holder_node_id: Some(owner.clone()),
            holder_session_id: Some("placement-recovery-cas-prior-session".to_string()),
        };
        let replacement = DfsPlacementAuthorityPermitRecord {
            job_id: "placement-recovery-cas-new-job".to_string(),
            holder_id: "placement-recovery-cas-new-holder".to_string(),
            holder_cluster_id: Some(current_process.cluster_id.clone()),
            holder_node_id: Some(owner.clone()),
            holder_session_id: Some(current_process.process_session_id.clone()),
        };
        let key = placement_authority_permit_key(&file_system_id);
        let mut observed = InProcessKvStore::new();
        observed
            .put(
                key.clone(),
                sinomo_core::kv::encode_kv_value(&old).expect("old record encoding"),
                None,
            )
            .expect("observed old record");
        let mut authoritative = InProcessKvStore::new();
        authoritative
            .put(
                key.clone(),
                sinomo_core::kv::encode_kv_value(&replacement)
                    .expect("replacement record encoding"),
                None,
            )
            .expect("authoritative replacement");
        let mut client = StaleObservedAuthoritativeTxnKv {
            observed,
            authoritative,
        };
        let mut store = DfsControlPlaneStore::new(&mut client);
        assert!(
            !store
                .recover_prior_process_placement_authority_permit_for_local_node(
                    &file_system_id,
                    &owner,
                    &current_process,
                )
                .expect("stale recovery compare")
        );
        assert_eq!(
            client
                .authoritative
                .get(&key)
                .expect("replacement remains")
                .decode::<DfsPlacementAuthorityPermitRecord>()
                .expect("replacement decoding"),
            replacement
        );

        let legacy = DfsPlacementAuthorityPermitRecord {
            job_id: "placement-recovery-legacy-job".to_string(),
            holder_id: "placement-recovery-legacy-holder".to_string(),
            holder_cluster_id: None,
            holder_node_id: None,
            holder_session_id: None,
        };
        let mut kv = InProcessKvStore::new();
        kv.put(
            key.clone(),
            sinomo_core::kv::encode_kv_value(&legacy).expect("legacy record encoding"),
            None,
        )
        .expect("legacy record");
        let mut store = DfsControlPlaneStore::new(&mut kv);
        assert!(
            !store
                .recover_prior_process_placement_authority_permit_for_local_node(
                    &file_system_id,
                    &owner,
                    &current_process,
                )
                .expect("legacy recovery is rejected")
        );
        assert!(store.kv.get(&key).expect("legacy record read").is_some());
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
        assert_eq!(
            status.holder_kind,
            DfsPlacementAuthorityPermitHolderKind::Identified
        );
        assert_eq!(status.holder_cluster_id, Some(cluster_id.to_string()));
        assert_eq!(status.holder_node_id, Some(holder_node_id));
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
    fn placement_operator_break_glass_is_explicit_and_limited_to_foreign_or_incomplete_holders() {
        let file_system_id = file_system_id();
        let current_cluster_id = placement_authority_operator_test_cluster_id();
        let foreign_cluster_id =
            sinomo_core::Id::from_legacy_named("placement-operator-foreign-cluster")
                .expect("valid foreign cluster id");
        let holder_node_id = node("placement-operator-break-glass-holder");
        let local_node_id = node("placement-operator-break-glass-local");

        let cases = [
            placement_authority_operator_test_record(
                Some(&foreign_cluster_id),
                Some(holder_node_id.clone()),
                Some("foreign-session"),
            ),
            placement_authority_operator_test_record(
                Some(&current_cluster_id),
                Some(holder_node_id.clone()),
                None,
            ),
            placement_authority_operator_test_record(None, None, None),
        ];
        for record in cases {
            let mut kv = InProcessKvStore::new();
            put_placement_authority_operator_test_record(&mut kv, &file_system_id, &record);
            let mut store = DfsControlPlaneStore::new(&mut kv);
            let status = store
                .placement_authority_permit_status(&file_system_id)
                .expect("inspect break-glass permit")
                .expect("break-glass permit");
            let normal_reason = if record.holder_cluster_id.as_deref()
                == Some(current_cluster_id.to_string().as_str())
            {
                DfsPlacementAuthorityRecoveryRefusal::IncompleteHolderIdentity
            } else if record.holder_cluster_id.is_some() {
                DfsPlacementAuthorityRecoveryRefusal::ForeignCluster
            } else {
                DfsPlacementAuthorityRecoveryRefusal::IncompleteHolderIdentity
            };
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

    fn membership_handoff_proof(
        handoff: &DfsMetadataShardMembershipHandoff,
        journal_sequence: u64,
    ) -> DfsMetadataShardMembershipHandoffProof {
        let retiring_node_ids = metadata_shard_configured_node_ids(&handoff.retiring_shard);
        let desired_node_ids = metadata_shard_configured_node_ids(&handoff.desired_shard);
        let acknowledged_node_ids = retiring_node_ids
            .union(&desired_node_ids)
            .cloned()
            .collect();
        let state_fingerprint = handoff
            .retiring_primary_fence
            .as_ref()
            .filter(|fence| fence.journal_sequence == journal_sequence)
            .map(|fence| fence.state_fingerprint.clone())
            .unwrap_or_else(|| format!("blake3:membership-handoff-{journal_sequence}"));
        DfsMetadataShardMembershipHandoffProof {
            handoff_id: handoff.handoff_id.clone(),
            retiring_shard: handoff.retiring_shard.clone(),
            desired_shard: handoff.desired_shard.clone(),
            journal_sequence,
            state_fingerprint,
            acknowledged_node_ids,
        }
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
        let file_system = DfsFileSystem {
            file_system_id: file_system_id(),
            display_name: "repo".to_string(),
            owner_node_id: node_owner.clone(),
            subtree_policies: vec![DfsSubtreePolicy::legacy(
                ".",
                DfsObjectPolicy::ReplicatedEventual,
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
            namespace_prepare_codec: Default::default(),
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

    #[test]
    fn raw_metadata_shard_put_rejects_unproven_packed_downgrade() {
        let file_system_id = file_system_id();
        let packed = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node("node_raw_codec_owner"),
            replica_node_ids: vec![node("node_raw_codec_replica")],
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV1,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let legacy = DfsMetadataShardConfig {
            epoch: packed.epoch + 1,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::Legacy,
            ..packed.clone()
        };
        let mut kv = InProcessKvStore::new();
        put_packed_namespace_prepare_capable_members(
            &mut kv,
            std::iter::once(&packed.primary_node_id).chain(packed.replica_node_ids.iter()),
        );
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&packed)
            .expect("put packed shard");

        let changed_packed = DfsMetadataShardConfig {
            replica_node_ids: vec![node("node_raw_codec_uncapable_replica")],
            epoch: packed.epoch + 1,
            ..packed.clone()
        };
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).put_metadata_shard(&changed_packed),
            Err(KvError::Serialization { detail })
                if detail.contains("capability-validated")
                    && detail.contains("membership handoff")
        ));
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).put_metadata_shard(&legacy),
            Err(KvError::Serialization { detail })
                if detail.contains("legacy-readable snapshot")
                    && detail.contains("replay-floor proof")
        ));
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read retained packed shard"),
            Some(packed)
        );
    }

    #[test]
    fn raw_metadata_shard_put_rejects_unproven_packed_activation() {
        let file_system_id = file_system_id();
        let legacy = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node("node_raw_activation_owner"),
            replica_node_ids: vec![node("node_raw_activation_replica")],
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::Legacy,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let packed = DfsMetadataShardConfig {
            epoch: legacy.epoch + 1,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV1,
            ..legacy.clone()
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&legacy)
            .expect("put legacy shard");

        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).put_metadata_shard(&packed),
            Err(KvError::Serialization { detail })
                if detail.contains("capability-validated")
                    && detail.contains("membership handoff")
        ));
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv)
                .begin_metadata_shard_membership_handoff(&legacy, &packed),
            Err(KvError::Serialization { detail })
                if detail.contains("authoritative cluster member capability record")
        ));
        put_packed_namespace_prepare_capable_members(
            &mut kv,
            std::iter::once(&packed.primary_node_id).chain(packed.replica_node_ids.iter()),
        );
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .begin_metadata_shard_membership_handoff(&legacy, &packed)
                .expect("capability-proved packed handoff")
                .is_some()
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read retained legacy shard"),
            Some(legacy)
        );
    }

    #[test]
    fn metadata_namespace_prepare_codec_transitions_are_monotonic_through_v2() {
        let legacy = DfsMetadataShardConfig {
            file_system_id: file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: node("node_codec_transition_owner"),
            replica_node_ids: vec![node("node_codec_transition_replica")],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::Legacy,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let packed_v1 = DfsMetadataShardConfig {
            epoch: 2,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV1,
            ..legacy.clone()
        };
        let packed_v2 = DfsMetadataShardConfig {
            epoch: 3,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV2,
            ..legacy.clone()
        };

        validate_metadata_namespace_prepare_codec_transition(&legacy, &packed_v1)
            .expect("legacy to packed-v1 upgrade");
        validate_metadata_namespace_prepare_codec_transition(&packed_v1, &packed_v2)
            .expect("packed-v1 to packed-v2 upgrade");
        validate_metadata_namespace_prepare_codec_transition(&legacy, &packed_v2)
            .expect("a fully capable new generation may activate packed-v2 directly");
        for desired in [&packed_v1, &legacy] {
            assert!(matches!(
                validate_metadata_namespace_prepare_codec_transition(&packed_v2, desired),
                Err(KvError::Serialization { detail })
                    if detail.contains("cannot downgrade")
                        && detail.contains("packed-v2")
                        && detail.contains(desired.namespace_prepare_codec.label())
            ));
        }
    }

    #[test]
    fn initial_packed_v2_metadata_shard_requires_its_exact_capability() {
        let packed_v2 = DfsMetadataShardConfig {
            file_system_id: file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: node("node_initial_packed_v2_owner"),
            replica_node_ids: vec![node("node_initial_packed_v2_replica")],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV2,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let configured_node_ids = std::iter::once(&packed_v2.primary_node_id)
            .chain(packed_v2.replica_node_ids.iter())
            .cloned()
            .collect::<Vec<_>>();
        let mut kv = InProcessKvStore::new();
        put_packed_namespace_prepare_capable_members(&mut kv, configured_node_ids.iter());

        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).put_metadata_shard(&packed_v2),
            Err(KvError::Serialization { detail })
                if detail.contains("packed-v2")
                    && detail.contains(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V2_CAPABILITY)
        ));

        add_packed_v2_namespace_prepare_capability(&mut kv, configured_node_ids.iter());
        let denied_node_id = &packed_v2.replica_node_ids[0];
        let denied_member = kv
            .get(&cluster_member_key(denied_node_id))
            .expect("packed-v2 member")
            .decode::<ClusterMemberRecord>()
            .expect("decode packed-v2 member");
        let mut denied_policy = ClusterCapabilityPolicyRecord::from(&denied_member);
        denied_policy.capabilities.retain(|capability| {
            capability.as_str() != DFS_METADATA_PACKED_NAMESPACE_PREPARE_V2_CAPABILITY
        });
        kv.put_serde(
            cluster_capability_policy_key(denied_node_id),
            &denied_policy,
            None,
        )
        .expect("put packed-v2 denying policy");
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).put_metadata_shard(&packed_v2),
            Err(KvError::Serialization { detail })
                if detail.contains("effective capability policy")
                    && detail.contains(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V2_CAPABILITY)
        ));

        let allowed_policy = ClusterCapabilityPolicyRecord::from(&denied_member);
        kv.put_serde(
            cluster_capability_policy_key(denied_node_id),
            &allowed_policy,
            None,
        )
        .expect("put packed-v2 allowing policy");
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&packed_v2)
            .expect("capability-proved packed-v2 create");
    }

    #[test]
    fn initial_packed_metadata_shard_requires_authoritative_member_capabilities() {
        let packed = DfsMetadataShardConfig {
            file_system_id: file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: node("node_initial_packed_owner"),
            replica_node_ids: vec![node("node_initial_packed_replica")],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV1,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut raw_kv = InProcessKvStore::new();
        assert!(matches!(
            DfsControlPlaneStore::new(&mut raw_kv).put_metadata_shard(&packed),
            Err(KvError::Serialization { detail })
                if detail.contains("authoritative cluster member capability record")
        ));
        put_packed_namespace_prepare_capable_members(
            &mut raw_kv,
            std::iter::once(&packed.primary_node_id).chain(packed.replica_node_ids.iter()),
        );
        put_packed_namespace_prepare_capability_policy(
            &mut raw_kv,
            &packed.replica_node_ids[0],
            false,
        );
        assert!(matches!(
            DfsControlPlaneStore::new(&mut raw_kv).put_metadata_shard(&packed),
            Err(KvError::Serialization { detail })
                if detail.contains("effective capability policy")
        ));
        put_packed_namespace_prepare_capability_policy(
            &mut raw_kv,
            &packed.replica_node_ids[0],
            true,
        );
        DfsControlPlaneStore::new(&mut raw_kv)
            .put_metadata_shard(&packed)
            .expect("capability-proved raw create");

        let mut cas_kv = InProcessKvStore::new();
        assert!(matches!(
            DfsControlPlaneStore::new(&mut cas_kv)
                .compare_and_put_metadata_shard(None, &packed),
            Err(KvError::Serialization { detail })
                if detail.contains("authoritative cluster member capability record")
        ));
        put_packed_namespace_prepare_capable_members(
            &mut cas_kv,
            std::iter::once(&packed.primary_node_id).chain(packed.replica_node_ids.iter()),
        );
        assert!(
            DfsControlPlaneStore::new(&mut cas_kv)
                .compare_and_put_metadata_shard(None, &packed)
                .expect("capability-proved CAS create")
        );
    }

    #[test]
    fn initial_packed_metadata_shard_fences_concurrent_capability_loss() {
        let packed = DfsMetadataShardConfig {
            file_system_id: file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: node("node_racing_packed_owner"),
            replica_node_ids: vec![node("node_racing_packed_replica")],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV1,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut inner = InProcessKvStore::new();
        put_packed_namespace_prepare_capable_members(
            &mut inner,
            std::iter::once(&packed.primary_node_id).chain(packed.replica_node_ids.iter()),
        );
        let replica_member_key = cluster_member_key(&packed.replica_node_ids[0]);
        let mut kv = DropPackedCapabilityBeforeTxnKv {
            inner,
            record_key: replica_member_key,
            record_kind: PackedCapabilityRecordKind::Member,
            drop_before_next_txn: true,
        };

        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).put_metadata_shard(&packed),
            Err(KvError::Serialization { detail })
                if detail.contains(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY)
        ));
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&packed.file_system_id, &packed.shard_id)
                .expect("read fenced shard")
                .is_none(),
            "the member capability compare must fence the shard create transaction"
        );
    }

    #[test]
    fn initial_packed_metadata_shard_fences_concurrent_policy_capability_loss() {
        let packed = DfsMetadataShardConfig {
            file_system_id: file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: node("node_racing_packed_policy_owner"),
            replica_node_ids: vec![node("node_racing_packed_policy_replica")],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV1,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut inner = InProcessKvStore::new();
        put_packed_namespace_prepare_capable_members(
            &mut inner,
            std::iter::once(&packed.primary_node_id).chain(packed.replica_node_ids.iter()),
        );
        put_packed_namespace_prepare_capability_policy(
            &mut inner,
            &packed.replica_node_ids[0],
            true,
        );
        let replica_policy_key = cluster_capability_policy_key(&packed.replica_node_ids[0]);
        let mut kv = DropPackedCapabilityBeforeTxnKv {
            inner,
            record_key: replica_policy_key,
            record_kind: PackedCapabilityRecordKind::Policy,
            drop_before_next_txn: true,
        };

        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).put_metadata_shard(&packed),
            Err(KvError::Serialization { detail })
                if detail.contains("effective capability policy")
        ));
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&packed.file_system_id, &packed.shard_id)
                .expect("read policy-fenced shard")
                .is_none(),
            "the capability-policy compare must fence the shard create transaction"
        );
    }

    #[tokio::test]
    async fn async_raw_metadata_shard_put_rejects_unproven_packed_downgrade() {
        let file_system_id = file_system_id();
        let packed = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node("node_async_raw_codec_owner"),
            replica_node_ids: vec![node("node_async_raw_codec_replica")],
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV1,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let legacy = DfsMetadataShardConfig {
            epoch: packed.epoch + 1,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::Legacy,
            ..packed.clone()
        };
        let mut kv = InProcessKvStore::new();
        put_packed_namespace_prepare_capable_members(
            &mut kv,
            std::iter::once(&packed.primary_node_id).chain(packed.replica_node_ids.iter()),
        );
        DfsControlPlaneAsyncStore::new(&mut kv)
            .put_metadata_shard(&packed)
            .await
            .expect("put packed shard");

        let changed_packed = DfsMetadataShardConfig {
            replica_node_ids: vec![node("node_async_raw_codec_uncapable_replica")],
            epoch: packed.epoch + 1,
            ..packed.clone()
        };
        assert!(matches!(
            DfsControlPlaneAsyncStore::new(&mut kv)
                .put_metadata_shard(&changed_packed)
                .await,
            Err(KvError::Serialization { detail })
                if detail.contains("capability-validated")
                    && detail.contains("membership handoff")
        ));
        assert!(matches!(
            DfsControlPlaneAsyncStore::new(&mut kv)
                .put_metadata_shard(&legacy)
                .await,
            Err(KvError::Serialization { detail })
                if detail.contains("legacy-readable snapshot")
                    && detail.contains("replay-floor proof")
        ));
        assert_eq!(
            DfsControlPlaneAsyncStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .await
                .expect("read retained packed shard"),
            Some(packed)
        );
    }

    #[tokio::test]
    async fn async_raw_metadata_shard_put_rejects_unproven_packed_activation() {
        let file_system_id = file_system_id();
        let legacy = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node("node_async_raw_activation_owner"),
            replica_node_ids: vec![node("node_async_raw_activation_replica")],
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::Legacy,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let packed = DfsMetadataShardConfig {
            epoch: legacy.epoch + 1,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV1,
            ..legacy.clone()
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneAsyncStore::new(&mut kv)
            .put_metadata_shard(&legacy)
            .await
            .expect("put legacy shard");

        assert!(matches!(
            DfsControlPlaneAsyncStore::new(&mut kv)
                .put_metadata_shard(&packed)
                .await,
            Err(KvError::Serialization { detail })
                if detail.contains("capability-validated")
                    && detail.contains("membership handoff")
        ));
        assert!(matches!(
            DfsControlPlaneAsyncStore::new(&mut kv)
                .begin_metadata_shard_membership_handoff(&legacy, &packed)
                .await,
            Err(KvError::Serialization { detail })
                if detail.contains("authoritative cluster member capability record")
        ));
        put_packed_namespace_prepare_capable_members(
            &mut kv,
            std::iter::once(&packed.primary_node_id).chain(packed.replica_node_ids.iter()),
        );
        assert!(
            DfsControlPlaneAsyncStore::new(&mut kv)
                .begin_metadata_shard_membership_handoff(&legacy, &packed)
                .await
                .expect("capability-proved async packed handoff")
                .is_some()
        );
        assert_eq!(
            DfsControlPlaneAsyncStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .await
                .expect("read retained legacy shard"),
            Some(legacy)
        );
    }

    #[tokio::test]
    async fn async_initial_packed_metadata_shard_requires_authoritative_member_capabilities() {
        let packed = DfsMetadataShardConfig {
            file_system_id: file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: node("node_async_initial_packed_owner"),
            replica_node_ids: vec![node("node_async_initial_packed_replica")],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::PackedV1,
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
        put_packed_namespace_prepare_capable_members(
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
        put_packed_namespace_prepare_capable_members(
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
        let node_owner = node("node_owner");
        let node_replica = node("node_replica");
        let mut kv = InProcessKvStore::new();
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node_owner.clone(),
            replica_node_ids: vec![node_replica.clone()],
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control.put_metadata_shard(&shard).expect("put shard");
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .transfer_metadata_shard_primary(&file_system_id, "root", 6, node_replica.clone(),)
                .expect("stale transfer")
                .is_none()
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .transfer_metadata_shard_primary(&file_system_id, "root", 7, node_owner)
                .expect("idempotent transfer"),
            Some(shard.clone())
        );
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).transfer_metadata_shard_primary(
                &file_system_id,
                "root",
                7,
                node_replica,
            ),
            Err(KvError::Serialization { detail })
                if detail.contains("current operation-bound DFS prefix fence")
        ));
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read unchanged shard"),
            Some(shard)
        );
    }

    #[test]
    fn metadata_shard_primary_transfer_rejects_epoch_exhaustion() {
        let owner = node("node_transfer_exhausted_owner");
        let replica = node("node_transfer_exhausted_replica");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner,
            replica_node_ids: vec![replica.clone()],
            epoch: u64::MAX,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&shard)
            .expect("put shard");

        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).transfer_metadata_shard_primary(
                &file_system_id,
                "root",
                u64::MAX,
                replica,
            ),
            Err(KvError::Serialization { detail }) if detail.contains("epoch is exhausted")
        ));
    }

    #[tokio::test]
    async fn async_metadata_shard_primary_transfer_requires_dfs_handoff() {
        let owner = node("node_transfer_async_owner");
        let replica = node("node_transfer_async_replica");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replica.clone()],
            epoch: 12,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&shard)
            .expect("put shard");

        assert!(matches!(
            DfsControlPlaneAsyncStore::new(&mut kv)
                .transfer_metadata_shard_primary(
                    &file_system_id,
                    "root",
                    shard.epoch,
                    replica,
                )
                .await,
            Err(KvError::Serialization { detail })
                if detail.contains("current operation-bound DFS prefix fence")
        ));
        assert_eq!(
            DfsControlPlaneAsyncStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .await
                .expect("read unchanged async shard"),
            Some(shard)
        );
    }

    #[test]
    fn metadata_shard_compare_and_put_rejects_stale_expected_value() {
        let owner = node("node_shard_cas_owner");
        let replacement = node("node_shard_cas_replacement");
        let file_system_id = file_system_id();
        let initial = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replacement.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            primary_node_id: replacement.clone(),
            epoch: 2,
            ..initial.clone()
        };
        let mut kv = InProcessKvStore::new();
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .compare_and_put_metadata_shard(None, &initial)
                .expect("create missing shard")
        );
        // Install the concurrent successor directly as fixture state. The
        // public KV-only primary-transfer surface deliberately cannot create
        // this transition without a proof-bearing DFS handoff.
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&desired)
            .expect("install concurrent successor fixture");

        assert!(
            !DfsControlPlaneStore::new(&mut kv)
                .compare_and_put_metadata_shard(Some(&initial), &desired)
                .expect("stale compare")
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read current shard")
                .expect("shard")
                .primary_node_id,
            replacement
        );
    }

    #[test]
    fn metadata_shard_compare_and_put_parks_direct_primary_change() {
        let owner = node("node_shard_direct_primary_owner");
        let replacement = node("node_shard_direct_primary_replacement");
        let file_system_id = file_system_id();
        let current = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replacement.clone()],
            epoch: 40,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            primary_node_id: replacement,
            replica_node_ids: vec![owner],
            epoch: 41,
            ..current.clone()
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&current)
            .expect("put current shard");

        assert!(
            !DfsControlPlaneStore::new(&mut kv)
                .compare_and_put_metadata_shard(Some(&current), &desired)
                .expect("bare primary-role activation must fail closed")
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .expect("read unchanged shard"),
            Some(current)
        );
    }

    #[test]
    fn metadata_shard_primary_role_handoff_requires_exact_existing_member_rotation() {
        let owner = node("node_primary_role_owner");
        let promoted = node("node_primary_role_promoted");
        let other_replica = node("node_primary_role_other_replica");
        let outsider = node("node_primary_role_outsider");
        let file_system_id = file_system_id();
        let current = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![promoted.clone(), other_replica.clone()],
            epoch: 44,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            primary_node_id: promoted.clone(),
            replica_node_ids: vec![other_replica.clone(), owner.clone()],
            epoch: current.epoch + 1,
            ..current.clone()
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&current)
            .expect("put current shard");

        for invalid in [
            DfsMetadataShardConfig {
                primary_node_id: outsider,
                ..desired.clone()
            },
            DfsMetadataShardConfig {
                replica_node_ids: vec![owner.clone()],
                ..desired.clone()
            },
            DfsMetadataShardConfig {
                replica_node_ids: vec![owner.clone(), other_replica],
                ..desired.clone()
            },
        ] {
            assert!(matches!(
                DfsControlPlaneStore::new(&mut kv)
                    .begin_metadata_shard_membership_handoff(&current, &invalid),
                Err(KvError::Serialization { .. })
            ));
        }

        let handoff = DfsControlPlaneStore::new(&mut kv)
            .begin_metadata_shard_membership_handoff(&current, &desired)
            .expect("begin exact role-only handoff")
            .expect("current shard still active");
        assert_eq!(handoff.retiring_shard, current);
        assert_eq!(handoff.desired_shard, desired);
        assert_eq!(
            metadata_shard_configured_node_ids(&handoff.retiring_shard),
            metadata_shard_configured_node_ids(&handoff.desired_shard)
        );
    }

    #[test]
    fn metadata_shard_primary_handoff_joint_quorum_tolerates_one_offline_replica() {
        let owner = node("node_primary_joint_owner");
        let promoted = node("node_primary_joint_promoted");
        let retained = node("node_primary_joint_retained");
        let offline = node("node_primary_joint_offline");
        let file_system_id = file_system_id();
        let current = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![promoted.clone(), retained.clone(), offline.clone()],
            epoch: 46,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            primary_node_id: promoted.clone(),
            replica_node_ids: vec![retained.clone(), offline.clone(), owner.clone()],
            epoch: current.epoch + 1,
            ..current.clone()
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&current)
            .expect("put current shard");
        let handoff = DfsControlPlaneStore::new(&mut kv)
            .begin_metadata_shard_membership_handoff(&current, &desired)
            .expect("begin primary handoff")
            .expect("current shard remains exact");
        let retiring_fence = DfsMetadataShardMembershipHandoffFence {
            handoff_id: handoff.handoff_id.clone(),
            primary_node_id: owner.clone(),
            journal_sequence: 12,
            state_fingerprint: "blake3:primary-joint-12".to_string(),
        };
        let handoff = DfsControlPlaneStore::new(&mut kv)
            .acknowledge_metadata_shard_membership_handoff_fence(&handoff, &retiring_fence)
            .expect("acknowledge retiring fence")
            .expect("handoff remains pending");

        let mut missing_promoted = membership_handoff_proof(&handoff, 12);
        missing_promoted.acknowledged_node_ids =
            vec![owner.clone(), retained.clone(), offline.clone()];
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv).complete_metadata_shard_membership_handoff(
                &current,
                &desired,
                &missing_promoted,
            ),
            Err(KvError::Serialization { detail }) if detail.contains("both primaries")
        ));

        let mut proof = membership_handoff_proof(&handoff, 12);
        proof.acknowledged_node_ids = vec![owner.clone(), promoted.clone(), retained.clone()];
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .complete_metadata_shard_membership_handoff(&current, &desired, &proof)
                .expect("joint quorum activates without the offline replica")
        );
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .get_metadata_shard(&file_system_id, "root")
                .expect("active shard"),
            Some(desired)
        );
    }

    #[tokio::test]
    async fn async_metadata_shard_primary_role_handoff_accepts_exact_rotation() {
        let owner = node("node_primary_role_async_owner");
        let promoted = node("node_primary_role_async_promoted");
        let file_system_id = file_system_id();
        let current = DfsMetadataShardConfig {
            file_system_id,
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![promoted.clone()],
            epoch: 45,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            primary_node_id: promoted,
            replica_node_ids: vec![owner],
            epoch: current.epoch + 1,
            ..current.clone()
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneAsyncStore::new(&mut kv)
            .put_metadata_shard(&current)
            .await
            .expect("put current shard");

        let handoff = DfsControlPlaneAsyncStore::new(&mut kv)
            .begin_metadata_shard_membership_handoff(&current, &desired)
            .await
            .expect("begin exact async role-only handoff")
            .expect("current shard still active");
        assert_eq!(handoff.retiring_shard, current);
        assert_eq!(handoff.desired_shard, desired);
    }

    fn exact_authority_test_fixture(
        test_name: &str,
    ) -> (ExactAuthorityOnlyKv, DfsMetadataShardConfig) {
        let owner = node(&format!("{test_name}-owner"));
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: owner,
            replica_node_ids: Vec::new(),
            epoch: 19,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut inner = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut inner)
            .put_metadata_shard(&shard)
            .expect("put exact-authority shard");
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
        DfsControlPlaneStore::new(&mut stale_client.inner)
            .put_metadata_shard(&successor)
            .expect("install successor shard");
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
        let owner = node("node_authority_stale_owner");
        let replacement = node("node_authority_stale_replacement");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner,
            replica_node_ids: vec![replacement.clone()],
            epoch: 4,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut kv = InProcessKvStore::new();
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control.put_metadata_shard(&shard).expect("put shard");
        let transferred = DfsMetadataShardConfig {
            primary_node_id: replacement,
            replica_node_ids: vec![shard.primary_node_id.clone()],
            epoch: shard.epoch + 1,
            ..shard.clone()
        };
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&transferred)
            .expect("install successor fixture");

        let revision_before = kv.revision();
        assert!(
            !DfsControlPlaneStore::new(&mut kv)
                .metadata_shard_authority_is_current(&shard)
                .expect("stale authority check")
        );
        assert_eq!(kv.revision(), revision_before);
    }

    #[test]
    fn metadata_operation_floor_refreshes_after_stale_observed_cas() {
        let owner = node("node_operation_floor_refresh_owner");
        let replica = node("node_operation_floor_refresh_replica");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner,
            replica_node_ids: vec![replica],
            epoch: 73,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let floor_key = metadata_shard_operation_floor_key(&file_system_id, &shard.shard_id);
        let stale_floor = DfsMetadataShardOperationFloor {
            operation_digest: "blake3:stale-observed-floor".to_string(),
            prepared_sequence: 20268,
            authority_epoch: 71,
            permit_id: "stale-observed-permit".to_string(),
        };
        let authoritative_floor = DfsMetadataShardOperationFloor {
            operation_digest: "blake3:authoritative-predecessor-floor".to_string(),
            prepared_sequence: 20274,
            authority_epoch: shard.epoch,
            permit_id: "authoritative-predecessor-permit".to_string(),
        };

        let mut observed = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut observed)
            .put_metadata_shard(&shard)
            .expect("put observed shard");
        observed
            .put_serde(floor_key.clone(), &stale_floor, None)
            .expect("put stale observed floor");
        let mut authoritative = InProcessKvStore::restore(
            observed
                .try_snapshot()
                .expect("snapshot stale observed state"),
        )
        .expect("restore authoritative state");
        authoritative
            .put_serde(floor_key.clone(), &authoritative_floor, None)
            .expect("advance authoritative floor");

        let mut client = StaleObservedAuthoritativeTxnKv {
            observed,
            authoritative,
        };
        let requested_digest = "blake3:requested-after-refresh";
        let requested_sequence = authoritative_floor.prepared_sequence + 1;
        let permit_id = DfsControlPlaneStore::new(&mut client)
            .certify_metadata_shard_operation(&shard, requested_digest, requested_sequence)
            .expect("certify after refreshing stale floor")
            .expect("operation permit");
        let committed = client
            .authoritative
            .get(&floor_key)
            .expect("committed operation floor")
            .decode::<DfsMetadataShardOperationFloor>()
            .expect("decode committed operation floor");
        assert_eq!(committed.operation_digest, requested_digest);
        assert_eq!(committed.prepared_sequence, requested_sequence);
        assert_eq!(committed.authority_epoch, shard.epoch);
        assert_eq!(committed.permit_id, permit_id);
    }

    #[test]
    fn metadata_operation_floor_retries_readindex_generation_race() {
        let owner = node("node_operation_floor_readindex_retry_owner");
        let replica = node("node_operation_floor_readindex_retry_replica");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner,
            replica_node_ids: vec![replica],
            epoch: 74,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut inner = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut inner)
            .put_metadata_shard(&shard)
            .expect("put metadata shard");
        let mut client = RetryAuthoritativeViewKv {
            inner,
            remaining_retryable_get_failures: AtomicUsize::new(1),
            refresh_calls: AtomicUsize::new(0),
        };

        let digest = "blake3:readindex-generation-retry";
        let sequence = 20452;
        let permit_id = DfsControlPlaneStore::new(&mut client)
            .certify_metadata_shard_operation(&shard, digest, sequence)
            .expect("retry transient authoritative view")
            .expect("operation permit");

        assert_eq!(client.refresh_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            client
                .remaining_retryable_get_failures
                .load(Ordering::SeqCst),
            0
        );
        let floor = client
            .inner
            .get(&metadata_shard_operation_floor_key(
                &file_system_id,
                &shard.shard_id,
            ))
            .expect("operation floor")
            .decode::<DfsMetadataShardOperationFloor>()
            .expect("decode operation floor");
        assert_eq!(floor.operation_digest, digest);
        assert_eq!(floor.prepared_sequence, sequence);
        assert_eq!(floor.authority_epoch, shard.epoch);
        assert_eq!(floor.permit_id, permit_id);

        assert!(!metadata_operation_authority_view_error_is_retryable(
            &KvError::Io {
                path: "test-disk".to_string(),
                detail: "permission denied".to_string(),
            }
        ));
        assert!(!metadata_operation_authority_view_error_is_retryable(
            &KvError::Io {
                path: "test-disk".to_string(),
                detail: "permission denied; retry the read".to_string(),
            }
        ));
    }

    #[test]
    fn metadata_operation_floor_exact_replay_reuses_authoritative_permit() {
        let owner = node("node_operation_floor_replay_owner");
        let replica = node("node_operation_floor_replay_replica");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner,
            replica_node_ids: vec![replica],
            epoch: 73,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let floor_key = metadata_shard_operation_floor_key(&file_system_id, &shard.shard_id);
        let stale_floor = DfsMetadataShardOperationFloor {
            operation_digest: "blake3:stale-replay-floor".to_string(),
            prepared_sequence: 20268,
            authority_epoch: 71,
            permit_id: "stale-replay-permit".to_string(),
        };
        let exact_floor = DfsMetadataShardOperationFloor {
            operation_digest: "blake3:exact-replayed-operation".to_string(),
            prepared_sequence: 20274,
            authority_epoch: shard.epoch,
            permit_id: "existing-authoritative-permit".to_string(),
        };

        let mut observed = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut observed)
            .put_metadata_shard(&shard)
            .expect("put observed shard");
        observed
            .put_serde(floor_key.clone(), &stale_floor, None)
            .expect("put stale observed floor");
        let mut authoritative = InProcessKvStore::restore(
            observed
                .try_snapshot()
                .expect("snapshot stale observed state"),
        )
        .expect("restore authoritative state");
        authoritative
            .put_serde(floor_key.clone(), &exact_floor, None)
            .expect("install exact authoritative floor");
        let barrier_key = metadata_shard_authority_barrier_key(&file_system_id, &shard.shard_id);

        let mut client = StaleObservedAuthoritativeTxnKv {
            observed,
            authoritative,
        };
        let permit_id = DfsControlPlaneStore::new(&mut client)
            .certify_metadata_shard_operation(
                &shard,
                &exact_floor.operation_digest,
                exact_floor.prepared_sequence,
            )
            .expect("certify exact replay after refreshing stale floor")
            .expect("existing operation permit");
        assert_eq!(permit_id, exact_floor.permit_id);
        assert!(
            client.authoritative.get(&barrier_key).is_some(),
            "exact replay must be revalidated through an authoritative barrier"
        );
        assert_eq!(
            client
                .authoritative
                .get(&floor_key)
                .expect("exact operation floor")
                .decode::<DfsMetadataShardOperationFloor>()
                .expect("decode exact operation floor"),
            exact_floor
        );
    }

    #[test]
    fn metadata_operation_floor_stale_exact_replay_cannot_bypass_authoritative_handoff() {
        let owner = node("node_operation_floor_handoff_owner");
        let replica = node("node_operation_floor_handoff_replica");
        let replacement = node("node_operation_floor_handoff_replacement");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner,
            replica_node_ids: vec![replica],
            epoch: 73,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            replica_node_ids: vec![replacement],
            epoch: shard.epoch + 1,
            ..shard.clone()
        };
        let floor_key = metadata_shard_operation_floor_key(&file_system_id, &shard.shard_id);
        let handoff_key = metadata_shard_membership_handoff_key(&file_system_id, &shard.shard_id);
        let barrier_key = metadata_shard_authority_barrier_key(&file_system_id, &shard.shard_id);
        let exact_floor = DfsMetadataShardOperationFloor {
            operation_digest: "blake3:exact-before-authoritative-handoff".to_string(),
            prepared_sequence: 20274,
            authority_epoch: shard.epoch,
            permit_id: "permit-before-authoritative-handoff".to_string(),
        };

        let mut observed = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut observed)
            .put_metadata_shard(&shard)
            .expect("put observed shard");
        observed
            .put_serde(floor_key.clone(), &exact_floor, None)
            .expect("put observed exact floor");
        let mut authoritative = InProcessKvStore::restore(
            observed
                .try_snapshot()
                .expect("snapshot stale observed state"),
        )
        .expect("restore authoritative state");
        authoritative
            .put_serde(
                handoff_key,
                &DfsMetadataShardMembershipHandoff {
                    handoff_id: "authoritative-handoff".to_string(),
                    retiring_shard: shard.clone(),
                    desired_shard: desired,
                    required_prepared_sequence: exact_floor.prepared_sequence,
                    required_head_watermark: None,
                    retiring_primary_fence: None,
                },
                None,
            )
            .expect("install authoritative handoff");

        let mut client = StaleObservedAuthoritativeTxnKv {
            observed,
            authoritative,
        };
        assert!(
            DfsControlPlaneStore::new(&mut client)
                .certify_metadata_shard_operation(
                    &shard,
                    &exact_floor.operation_digest,
                    exact_floor.prepared_sequence,
                )
                .expect("reject stale exact replay against authoritative handoff")
                .is_none()
        );
        assert!(client.authoritative.get(&barrier_key).is_none());
        assert_eq!(
            client
                .authoritative
                .get(&floor_key)
                .expect("operation floor remains")
                .decode::<DfsMetadataShardOperationFloor>()
                .expect("decode operation floor"),
            exact_floor
        );
    }

    #[test]
    fn degraded_operation_floor_is_carried_into_membership_handoff() {
        let owner = node("node_operation_floor_owner");
        let retained = node("node_operation_floor_retained");
        let added = node("node_operation_floor_added");
        let file_system_id = file_system_id();
        let current = DfsMetadataShardConfig {
            file_system_id,
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![retained.clone()],
            epoch: 9,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            replica_node_ids: vec![retained, added],
            epoch: current.epoch + 1,
            ..current.clone()
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&current)
            .expect("put current shard");

        let permit = DfsControlPlaneStore::new(&mut kv)
            .certify_metadata_shard_operation(&current, "blake3:prepared-6", 6)
            .expect("certify degraded operation")
            .expect("operation permit");
        assert!(!permit.is_empty());
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .certify_metadata_shard_operation(&current, "blake3:different-6", 6)
                .expect("reject same-sequence alternate operation")
                .is_none()
        );

        let handoff = DfsControlPlaneStore::new(&mut kv)
            .begin_metadata_shard_membership_handoff(&current, &desired)
            .expect("begin membership handoff")
            .expect("exact current assignment");
        assert_eq!(handoff.required_prepared_sequence, 6);
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .certify_metadata_shard_operation(&current, "blake3:after-handoff", 7)
                .expect("pending handoff rejects new degraded permit")
                .is_none()
        );

        let stale_fence = DfsMetadataShardMembershipHandoffFence {
            handoff_id: handoff.handoff_id.clone(),
            primary_node_id: owner.clone(),
            journal_sequence: 5,
            state_fingerprint: "blake3:state-5".to_string(),
        };
        assert!(matches!(
            DfsControlPlaneStore::new(&mut kv)
                .acknowledge_metadata_shard_membership_handoff_fence(&handoff, &stale_fence),
            Err(KvError::Serialization { detail }) if detail.contains("below required prepared floor")
        ));

        let current_fence = DfsMetadataShardMembershipHandoffFence {
            journal_sequence: 6,
            state_fingerprint: "blake3:state-6".to_string(),
            ..stale_fence
        };
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .acknowledge_metadata_shard_membership_handoff_fence(&handoff, &current_fence)
                .expect("acknowledge current operation floor")
                .is_some()
        );
    }

    #[test]
    fn degraded_head_watermark_is_exact_and_monotonic() {
        let owner = node("node_head_watermark_owner");
        let replica = node("node_head_watermark_replica");
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: owner.clone(),
            replica_node_ids: vec![replica.clone()],
            epoch: 4,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut kv = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut kv)
            .put_metadata_shard(&shard)
            .expect("put shard");
        let high = DfsMetadataShardHeadWatermark {
            file_system_id: file_system_id.clone(),
            shard_id: shard.shard_id.clone(),
            primary_node_id: owner.clone(),
            epoch: shard.epoch,
            journal_sequence: 8,
            state_fingerprint: "blake3:state-8".to_string(),
            acknowledged_node_ids: vec![owner.clone(), replica.clone()],
        };
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .publish_metadata_shard_head_watermark(&shard, &high)
                .expect("publish high watermark")
        );
        let low = DfsMetadataShardHeadWatermark {
            journal_sequence: 7,
            state_fingerprint: "blake3:state-7".to_string(),
            ..high.clone()
        };
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .publish_metadata_shard_head_watermark(&shard, &low)
                .expect("lower candidate is a fenced no-regression write")
        );
        let stored = DfsControlPlaneStore::new(&mut kv)
            .get_metadata_shard_head_watermark(&file_system_id, "root")
            .expect("read watermark")
            .expect("stored watermark");
        assert_eq!(stored.journal_sequence, high.journal_sequence);
        assert_eq!(stored.state_fingerprint, high.state_fingerprint);
        assert_eq!(
            stored
                .acknowledged_node_ids
                .into_iter()
                .collect::<BTreeSet<_>>(),
            high.acknowledged_node_ids
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn metadata_shard_selection_prefers_longest_path_prefix() {
        let node_owner = node("node_shard_select_owner");
        let node_replica = node("node_shard_select_replica");
        let mut kv = InProcessKvStore::new();
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
                namespace_prepare_codec: Default::default(),
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
                namespace_prepare_codec: Default::default(),
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
                namespace_prepare_codec: Default::default(),
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
        let file_system_id = sifs_file_system_id();
        let mut control = DfsControlPlaneStore::new(&mut kv);
        let root_shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node_root.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
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
            namespace_prepare_codec: Default::default(),
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
            namespace_prepare_codec: Default::default(),
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
                namespace_prepare_codec: Default::default(),
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
                namespace_prepare_codec: Default::default(),
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
                namespace_prepare_codec: Default::default(),
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
                namespace_prepare_codec: Default::default(),
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
                namespace_prepare_codec: Default::default(),
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
                namespace_prepare_codec: Default::default(),
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
                namespace_prepare_codec: Default::default(),
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
                namespace_prepare_codec: Default::default(),
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
                namespace_prepare_codec: Default::default(),
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
        let file_system_id = file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: node_owner,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut control = DfsControlPlaneStore::new(&mut kv);
        control.put_metadata_shard(&shard).expect("put shard");

        assert!(matches!(
            control.select_metadata_shard_for_path(&file_system_id, "/bad\0path"),
            Err(KvError::Serialization { .. })
        ));
    }
}
