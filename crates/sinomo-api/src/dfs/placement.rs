//! Shared DFS wire values and validation; no storage or service runtime.

use crate::dfs::*;
use serde::Deserialize;
use serde::Serialize;
use sinomo_core::DfsObjectPolicy;
use sinomo_core::node::NodeId;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub const DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION: u32 = 1;

pub const DEFAULT_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE: usize = 50;

#[doc(hidden)]
pub const PLACEMENT_RECONCILIATION_SUPERSEDED_ERROR: &str =
    "superseded because a newer placement configuration is authoritative";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementPolicy {
    pub consistency: DfsObjectPolicy,
    pub min_commit_replicas: usize,
    pub desired_replicas: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_node_ids: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forbidden_node_ids: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub min_distinct_zones: usize,
}

impl DfsPlacementPolicy {
    pub fn new(
        consistency: DfsObjectPolicy,
        min_commit_replicas: usize,
        desired_replicas: usize,
    ) -> Result<Self, DfsDistributedError> {
        let policy = Self {
            consistency,
            min_commit_replicas,
            desired_replicas,
            required_node_ids: Vec::new(),
            forbidden_node_ids: Vec::new(),
            min_distinct_zones: 0,
        };
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<(), DfsDistributedError> {
        if matches!(
            self.consistency,
            DfsObjectPolicy::LocalOwned | DfsObjectPolicy::EphemeralLocal
        ) {
            if self.min_commit_replicas != 1
                || self.desired_replicas != 1
                || !self.required_node_ids.is_empty()
                || !self.forbidden_node_ids.is_empty()
                || self.min_distinct_zones != 0
            {
                return Err(DfsDistributedError::InvalidPolicy {
                    reason: "local placement requires W=1, N=1, and no replica constraints"
                        .to_string(),
                });
            }
            return Ok(());
        }
        if !matches!(
            self.consistency,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) {
            return Err(DfsDistributedError::InvalidPolicy {
                reason: "append-only placement is not a complete durable policy".to_string(),
            });
        }
        if self.consistency == DfsObjectPolicy::ReplicatedConfirmed
            && self.min_commit_replicas != self.desired_replicas
        {
            return Err(DfsDistributedError::InvalidPolicy {
                reason: "replicated-confirmed requires W == N".to_string(),
            });
        }
        self.replication_policy(BTreeMap::new()).map(drop)
    }

    pub fn replication_policy(
        &self,
        node_zones: BTreeMap<NodeId, String>,
    ) -> Result<DfsReplicationPolicy, DfsDistributedError> {
        DfsReplicationPolicy::new(self.min_commit_replicas, self.desired_replicas)?
            .with_placement_constraints(
                self.required_node_ids.clone(),
                self.forbidden_node_ids.clone(),
                self.min_distinct_zones,
                node_zones,
            )
    }

    pub fn into_rule(self, path: String, scope: DfsPlacementScope) -> DfsSubtreePolicy {
        DfsSubtreePolicy {
            path_prefix: path,
            policy: self.consistency,
            scope,
            min_commit_replicas: Some(self.min_commit_replicas),
            desired_replicas: Some(self.desired_replicas),
            required_node_ids: self.required_node_ids,
            forbidden_node_ids: self.forbidden_node_ids,
            min_distinct_zones: self.min_distinct_zones,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsExactPlacementPolicy {
    pub scope: DfsPlacementScope,
    pub policy: DfsPlacementPolicy,
}

/// The policy an editor should present for one placement scope.
///
/// `exact` distinguishes a rule stored directly on the selected path from the
/// fallback that would apply if that scope has no exact rule. In particular,
/// a missing `Subtree` rule never inherits a same-path `SelfOnly` rule, since
/// that rule cannot affect descendants.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementScopeResolution {
    pub scope: DfsPlacementScope,
    pub policy: DfsPlacementPolicy,
    #[serde(default)]
    pub exact: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_scope: Option<DfsPlacementScope>,
    #[serde(default)]
    pub inherited: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub explanation: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsResolvedPlacementPolicy {
    pub path: String,
    pub policy: DfsPlacementPolicy,
    /// Every managed rule attached directly to `path`, independent of which
    /// rule wins resolution for the path object itself. This lets editors keep
    /// coexisting SelfOnly and Subtree rules distinct.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exact_policies: Vec<DfsExactPlacementPolicy>,
    /// Effective editor values for `SelfOnly` and `Subtree`, including the
    /// correct fallback when the corresponding exact rule is absent.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scope_resolutions: Vec<DfsPlacementScopeResolution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_scope: Option<DfsPlacementScope>,
    pub inherited: bool,
    pub explanation: String,
}

/// Old previews predate either reachability-aware aggregate impact (v1) or an
/// exact preview token (v2), so they are not safe inputs for an Apply decision.
pub const DFS_PLACEMENT_PREVIEW_SCHEMA_VERSION: u32 = 2;

pub const DFS_PLACEMENT_PREVIEW_TOKEN_SCHEMA_VERSION: u32 = 1;

#[doc(hidden)]
pub const DFS_PLACEMENT_PREVIEW_TOKEN_DIGEST_PREFIX: &str = "dfs-placement-preview-v1:blake3:";

/// Opaque identity of the exact policy, candidate, reachability, metadata, and
/// impact snapshot represented by a placement preview.
///
/// Apply accepts this value only on the versioned, fail-closed update route.
/// Callers must not interpret or construct the digest themselves.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementPreviewToken {
    #[serde(default)]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub digest: String,
}

impl DfsPlacementPreviewToken {
    pub fn is_well_formed(&self) -> bool {
        self.schema_version == DFS_PLACEMENT_PREVIEW_TOKEN_SCHEMA_VERSION
            && self
                .digest
                .strip_prefix(DFS_PLACEMENT_PREVIEW_TOKEN_DIGEST_PREFIX)
                .is_some_and(|digest| {
                    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementPreview {
    /// Capability/schema version for safety-critical preview fields. Missing
    /// values deserialize as zero and must be treated as legacy/unsafe.
    #[serde(default)]
    pub schema_version: u32,
    /// Exact server-side preview snapshot required by the v2 Apply route.
    /// Missing values deserialize as unsafe so an old daemon fails closed.
    #[serde(default)]
    pub preview_token: DfsPlacementPreviewToken,
    pub resolved: DfsResolvedPlacementPolicy,
    pub placement: DfsReplicaPlacementExplanation,
    pub current_replica_node_ids: Vec<NodeId>,
    pub added_replica_node_ids: Vec<NodeId>,
    pub removed_replica_node_ids: Vec<NodeId>,
    /// Stable, operator-facing projection of every advertised, current, or
    /// policy-constrained node relevant to this preview.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<DfsPlacementNodePreview>,
    /// Environmental constraints that prevent the current proposal from being
    /// applied. Invalid policies still fail before a preview is returned.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocking_diagnostics: Vec<DfsPlacementBlockingDiagnostic>,
    /// Aggregate dry-run impact. The single-object planner fills the replica
    /// deltas it knows; subtree-aware callers can replace these defaults with
    /// totals from a metadata scan.
    #[serde(default)]
    pub impact: DfsPlacementImpact,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl DfsPlacementPreview {
    /// Whether this payload includes the reachability, diagnostics, and impact
    /// guarantees required to make an Apply decision.
    pub fn supports_safe_apply(&self) -> bool {
        self.schema_version >= DFS_PLACEMENT_PREVIEW_SCHEMA_VERSION
            && self.preview_token.is_well_formed()
    }

    pub fn is_satisfiable(&self) -> bool {
        self.supports_safe_apply() && self.blocking_diagnostics.is_empty()
    }

    /// Snapshot the explicitly unavailable candidates represented by this
    /// preview. Persisting this set with an accepted reconciliation job keeps
    /// its strict planner aligned with the plan the operator approved.
    pub fn unavailable_node_ids(&self) -> BTreeSet<NodeId> {
        self.nodes
            .iter()
            .filter(|node| node.reachability == DfsPlacementNodeReachability::Unreachable)
            .map(|node| node.node_id.clone())
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPlacementNodeReachability {
    #[default]
    Unknown,
    Local,
    Reachable,
    Unreachable,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementNodePreview {
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    #[serde(default)]
    pub capacity_weight: u32,
    #[serde(default)]
    pub accepts_content: bool,
    #[serde(default)]
    pub reachability: DfsPlacementNodeReachability,
    #[serde(default)]
    pub current_replica: bool,
    #[serde(default)]
    pub desired_replica: bool,
    #[serde(default)]
    pub preferred_for_commit: bool,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub forbidden: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_health: Option<crate::dfs::DfsHealthState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_freshness: Option<crate::dfs::DfsObservationFreshness>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usable_capacity_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reserved_free_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_state: Option<crate::dfs::DfsPlacementPathState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_latency_micros: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_throughput_bytes_per_second: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_metering: Option<crate::dfs::DfsNetworkMetering>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation_freshness: Option<crate::dfs::DfsObservationFreshness>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub explanation: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "code", rename_all = "kebab-case")]
pub enum DfsPlacementBlockingDiagnostic {
    InsufficientCandidates {
        available: usize,
        required: usize,
    },
    RequiredNodeUnavailable {
        node_id: NodeId,
    },
    DesiredNodeUnavailable {
        node_id: NodeId,
    },
    InsufficientZones {
        available: usize,
        required: usize,
    },
    /// At least one authorized main daemon can still run the legacy
    /// reconciliation protocol. Apply stays disabled until every listed node
    /// advertises the snapshot-bound v1 capability.
    IncompatibleDaemonVersion {
        node_ids: Vec<NodeId>,
    },
    /// An environmental failure in the policy resolved for this exact object
    /// or subtree. The wrapper preserves the source rule, so a nested override
    /// cannot be mistaken for a failure in the rule currently being edited.
    ScopedPolicy {
        path: String,
        scope: DfsPlacementScope,
        diagnostic: Box<DfsPlacementBlockingDiagnostic>,
    },
}

impl DfsPlacementBlockingDiagnostic {
    pub fn message(&self) -> String {
        match self {
            Self::InsufficientCandidates {
                available,
                required,
            } => format!(
                "only {available} eligible content nodes are available but N={required} is required"
            ),
            Self::RequiredNodeUnavailable { node_id } => {
                format!("required node {node_id} is unavailable or is not accepting SIFS content")
            }
            Self::DesiredNodeUnavailable { node_id } => {
                format!("desired replica node {node_id} is currently unreachable")
            }
            Self::InsufficientZones {
                available,
                required,
            } => {
                format!(
                    "the replica plan can represent at most {available} zones but {required} are required"
                )
            }
            Self::IncompatibleDaemonVersion { node_ids } => format!(
                "upgrade or restart these Sinomo daemons before applying placement: {}",
                node_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::ScopedPolicy {
                path,
                scope,
                diagnostic,
            } => {
                let scope = match scope {
                    DfsPlacementScope::SelfOnly => "object",
                    DfsPlacementScope::Subtree => "subtree",
                };
                format!(
                    "the {scope} placement policy at {path} is unsatisfiable: {}",
                    diagnostic.message()
                )
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementImpact {
    #[serde(default)]
    pub affected_object_count: u64,
    #[serde(default)]
    pub affected_bytes: u64,
    #[serde(default)]
    pub unknown_size_object_count: u64,
    #[serde(default)]
    pub copies_added: u64,
    #[serde(default)]
    pub copies_removed: u64,
    /// Boundary-independent digest of every non-tombstone metadata record in
    /// the selected scope. Object digests are XOR-combined, so bounded pages
    /// can be merged without making the token depend on page boundaries.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub metadata_snapshot_digest: String,
    /// False when one or more policies required best-effort planning, so copy
    /// additions/removals are useful estimates rather than authoritative
    /// reconciliation totals.
    #[serde(default = "placement_impact_complete_default")]
    pub complete: bool,
}

const fn placement_impact_complete_default() -> bool {
    true
}

impl Default for DfsPlacementImpact {
    fn default() -> Self {
        Self {
            affected_object_count: 0,
            affected_bytes: 0,
            unknown_size_object_count: 0,
            copies_added: 0,
            copies_removed: 0,
            metadata_snapshot_digest: String::new(),
            complete: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPlacementReconciliationState {
    Pending,
    Scanning,
    Repairing,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct DfsAdaptivePlacementBudget {
    pub max_objects_per_tick: usize,
    pub max_bytes_per_tick: u64,
    pub max_millis_per_tick: u64,
}

impl Default for DfsAdaptivePlacementBudget {
    fn default() -> Self {
        Self {
            max_objects_per_tick: 64,
            max_bytes_per_tick: 512 * 1024 * 1024,
            max_millis_per_tick: 250,
        }
    }
}

impl DfsPlacementReconciliationState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementReconciliationJob {
    /// Version of the durable job contract. Version 1 jobs live in a sibling
    /// KV namespace that legacy daemons do not scan or CAS.
    #[serde(default)]
    pub schema_version: u32,
    pub job_id: String,
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    pub scope: DfsPlacementScope,
    /// Explicitly unavailable candidates excluded by the accepted preview.
    /// This is a planning snapshot, not a live health record: subsequent
    /// reconciliation passes must not silently select one of these nodes.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub unavailable_node_ids: BTreeSet<NodeId>,
    /// Exact content-node advertisements used by the accepted preview. Replica
    /// selection must remain stable even when another node observes a newer or
    /// temporarily different cluster view while this job is running.
    ///
    /// An empty snapshot identifies a legacy/read-only job and is never
    /// accepted by the v1 writer or runner.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidate_snapshot: Vec<DfsNodeAdvertisement>,
    /// Primary-owned health/network evidence used for adaptive jobs. Manual
    /// policy jobs without this field retain their exact legacy planner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement_snapshot: Option<crate::dfs::DfsPlacementSnapshot>,
    #[serde(default)]
    pub adaptive: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adaptive_budget: Option<DfsAdaptivePlacementBudget>,
    #[serde(default)]
    pub processed_content_bytes: u64,
    /// Exact filesystem config accepted atomically with this job. Legacy jobs
    /// deserialize with no snapshot but remain read-only; v1 writers persist
    /// it so stale jobs can be superseded safely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_config: Option<DfsFileSystemConfig>,
    pub state: DfsPlacementReconciliationState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<DfsObjectPageCursor>,
    pub total_object_count: usize,
    pub scanned_object_count: usize,
    pub matched_object_count: usize,
    pub updated_object_count: usize,
    pub scheduled_repair_count: usize,
    pub completed_repair_count: usize,
    pub pending_repair_count: usize,
    pub revision: u64,
    pub created_at_unix_nanos: u64,
    pub updated_at_unix_nanos: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl DfsPlacementReconciliationJob {
    pub fn supports_snapshot_reconciliation(&self) -> bool {
        self.schema_version == DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION
            && self.accepted_config.is_some()
            && !self.candidate_snapshot.is_empty()
    }

    pub fn progress_fraction(&self) -> (usize, usize) {
        let scan_total = self.total_object_count;
        let repair_total = self.scheduled_repair_count;
        let total = scan_total.saturating_add(repair_total);
        if total == 0 {
            return if self.state == DfsPlacementReconciliationState::Completed {
                (1, 1)
            } else {
                (0, 1)
            };
        }
        let completed_scan = if matches!(
            self.state,
            DfsPlacementReconciliationState::Repairing | DfsPlacementReconciliationState::Completed
        ) {
            scan_total
        } else {
            self.scanned_object_count.min(scan_total)
        };
        let completed =
            completed_scan.saturating_add(self.completed_repair_count.min(repair_total));
        (completed, total)
    }

    pub fn activity_status(&self) -> DfsPlacementReconciliationActivityStatus {
        match self.state {
            DfsPlacementReconciliationState::Completed => {
                DfsPlacementReconciliationActivityStatus::Completed
            }
            DfsPlacementReconciliationState::Failed
                if self.last_error.as_deref()
                    == Some(PLACEMENT_RECONCILIATION_SUPERSEDED_ERROR) =>
            {
                DfsPlacementReconciliationActivityStatus::Superseded
            }
            DfsPlacementReconciliationState::Failed => {
                DfsPlacementReconciliationActivityStatus::Failed
            }
            _ if self.last_error.is_some() => DfsPlacementReconciliationActivityStatus::Retrying,
            _ => DfsPlacementReconciliationActivityStatus::Running,
        }
    }
}

/// Operator-facing status for placement reconciliation activity. `Retrying` is
/// derived from a non-terminal job's durable `last_error`: successful pages
/// clear that field, while transient failures remain queued for the next
/// maintenance tick.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPlacementReconciliationActivityStatus {
    Running,
    Retrying,
    Failed,
    Completed,
    /// The job became obsolete after a newer overlapping placement policy was
    /// accepted. This is an expected control-plane outcome, not an execution
    /// failure; the durable execution enum remains `Failed` for compatibility.
    Superseded,
}

/// Stable newest-first activity cursor. Creation time and job id are immutable
/// job identity fields, so progress updates cannot move an item between pages.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementReconciliationActivityCursor {
    pub created_at_unix_nanos: u64,
    pub job_id: String,
}

/// Compact activity projection that deliberately omits the accepted config and
/// candidate snapshots retained by the durable execution record.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementReconciliationActivitySummary {
    pub job_id: String,
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    pub scope: DfsPlacementScope,
    pub status: DfsPlacementReconciliationActivityStatus,
    pub phase: DfsPlacementReconciliationState,
    pub progress_completed: usize,
    pub progress_total: usize,
    pub total_object_count: usize,
    pub scanned_object_count: usize,
    pub matched_object_count: usize,
    pub updated_object_count: usize,
    pub scheduled_repair_count: usize,
    pub completed_repair_count: usize,
    pub pending_repair_count: usize,
    pub revision: u64,
    pub created_at_unix_nanos: u64,
    pub updated_at_unix_nanos: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl DfsPlacementReconciliationActivitySummary {
    pub fn cursor(&self) -> DfsPlacementReconciliationActivityCursor {
        DfsPlacementReconciliationActivityCursor {
            created_at_unix_nanos: self.created_at_unix_nanos,
            job_id: self.job_id.clone(),
        }
    }
}

impl From<&DfsPlacementReconciliationJob> for DfsPlacementReconciliationActivitySummary {
    fn from(job: &DfsPlacementReconciliationJob) -> Self {
        let (progress_completed, progress_total) = job.progress_fraction();
        Self {
            job_id: job.job_id.clone(),
            file_system_id: job.file_system_id.clone(),
            path: job.path.clone(),
            scope: job.scope,
            status: job.activity_status(),
            phase: job.state,
            progress_completed,
            progress_total,
            total_object_count: job.total_object_count,
            scanned_object_count: job.scanned_object_count,
            matched_object_count: job.matched_object_count,
            updated_object_count: job.updated_object_count,
            scheduled_repair_count: job.scheduled_repair_count,
            completed_repair_count: job.completed_repair_count,
            pending_repair_count: job.pending_repair_count,
            revision: job.revision,
            created_at_unix_nanos: job.created_at_unix_nanos,
            updated_at_unix_nanos: job.updated_at_unix_nanos,
            last_error: job.last_error.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementReconciliationActivityPage {
    pub activities: Vec<DfsPlacementReconciliationActivitySummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsPlacementReconciliationActivityCursor>,
    /// Some durable jobs may predate the compact activity projection or may
    /// have been written by an older daemon. Active jobs are still unioned
    /// from the bounded execution queue, but terminal history can have gaps.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub partial: bool,
    /// Older terminal activity has fallen outside the retained recent index.
    /// `next_cursor` pages only through the records that remain available.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub history_truncated: bool,
}

fn is_zero_usize(value: &usize) -> bool {
    *value == 0
}

pub const MAX_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE: usize = 100;
