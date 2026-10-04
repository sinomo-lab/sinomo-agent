//! Durable, explainable DFS placement policy and reconciliation records.

use crate::control_plane::DfsPlacementAuthorityPermit;
use crate::distributed::explain_replicas_best_effort;
use crate::posix_path::normalize_posix_path;
use crate::{
    AsyncDfsControlPlaneKvClient, DfsControlPlaneKvClient, DfsControlPlaneStore,
    DfsDistributedError, DfsError, DfsFileSystem, DfsFileSystemConfig, DfsFileSystemId,
    DfsMessageCommittedWork, DfsMessageTransport, DfsMetadataShardConfig, DfsNamespaceOperation,
    DfsNodeAdvertisement, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsObjectPage,
    DfsObjectPageCursor, DfsPlacementScope, DfsReplicaCandidate, DfsReplicaPlacementExplanation,
    DfsReplicaPlacementRequest, DfsReplicationPolicy, DfsStat, DfsSubtreePolicy, DfsVersion,
    DfsWriteIntentState, DistributedWriteIntent, NetworkedDfsService, dfs_now_unix_nanos,
    explain_replicas, explain_replicas_with_placement_snapshot, validate_replication_plan,
};
use serde::{Deserialize, Serialize};
use sinomo_core::kv::KvCompare;
use sinomo_core::node::{
    DFS_PLACEMENT_RECONCILIATION_V1_CAPABILITY, NodeId, cluster_member_is_legacy_shadow_daemon,
};
use sinomo_core::{ClusterBootstrapState, DfsObjectPolicy};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

const PLACEMENT_IMPACT_PAGE_SIZE: usize = 256;
const PLACEMENT_AUTHORITY_ACQUIRE_RETRIES: usize = 4;
const PLACEMENT_AUTHORITY_RELEASE_RETRIES: usize = 4;
pub use sinomo_api::dfs::MAX_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE;
pub use sinomo_api::dfs::placement::DEFAULT_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE;
pub use sinomo_api::dfs::placement::DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION;
use sinomo_api::dfs::placement::PLACEMENT_RECONCILIATION_SUPERSEDED_ERROR;
const ADAPTIVE_PLACEMENT_MIN_CONSECUTIVE_OBSERVATIONS: u32 = 3;
const ADAPTIVE_PLACEMENT_MIN_SUSTAINED_NANOS: u64 = 120_000_000_000;

fn advance_adaptive_placement_observation(
    current: Option<&DfsAdaptivePlacementObservationState>,
    snapshot_digest: &str,
    snapshot_generation: u64,
    now_unix_nanos: u64,
) -> (DfsAdaptivePlacementObservationState, bool) {
    let mut desired = current.cloned().unwrap_or_default();
    if desired.pending_snapshot_digest != snapshot_digest {
        desired.pending_snapshot_digest = snapshot_digest.to_string();
        desired.pending_snapshot_generation = snapshot_generation;
        desired.first_observed_at_unix_nanos = now_unix_nanos;
        desired.consecutive_observations = 1;
    } else {
        desired.pending_snapshot_generation = snapshot_generation;
        desired.consecutive_observations = desired
            .consecutive_observations
            .saturating_add(1)
            .min(ADAPTIVE_PLACEMENT_MIN_CONSECUTIVE_OBSERVATIONS);
    }
    let sustained = desired.consecutive_observations
        >= ADAPTIVE_PLACEMENT_MIN_CONSECUTIVE_OBSERVATIONS
        && now_unix_nanos.saturating_sub(desired.first_observed_at_unix_nanos)
            >= ADAPTIVE_PLACEMENT_MIN_SUSTAINED_NANOS
        && desired.last_enqueued_snapshot_digest != snapshot_digest;
    (desired, sustained)
}

// Every maintenance page is time-bounded, including explicit operator jobs.
// Adaptive jobs additionally retain their configured object/byte limits.
fn placement_page_budget(
    job: &DfsPlacementReconciliationJob,
    max_objects: usize,
) -> DfsAdaptivePlacementBudget {
    let mut budget = job
        .adaptive_budget
        .clone()
        .filter(|_| job.adaptive)
        .unwrap_or_else(|| DfsAdaptivePlacementBudget {
            max_objects_per_tick: max_objects,
            max_bytes_per_tick: u64::MAX,
            ..Default::default()
        });
    budget.max_objects_per_tick = budget.max_objects_per_tick.min(max_objects).max(1);
    budget
}

fn finish_placement_reconciliation_page(
    job: &mut DfsPlacementReconciliationJob,
    pass_pending_repairs: usize,
) {
    match job.state {
        DfsPlacementReconciliationState::Scanning if job.cursor.is_none() => {
            job.total_object_count = job.scanned_object_count;
            job.state = DfsPlacementReconciliationState::Repairing;
            job.pending_repair_count = 0;
        }
        DfsPlacementReconciliationState::Repairing => {
            // Checkpoint this subtotal with the exact continuation. Otherwise
            // the final page can report completion while an earlier page still
            // has missing replicas. A new pass resets the subtotal at entry.
            job.pending_repair_count = pass_pending_repairs;
            if job.cursor.is_none() {
                job.completed_repair_count = job
                    .scheduled_repair_count
                    .saturating_sub(job.pending_repair_count);
                if job.pending_repair_count == 0 {
                    job.state = DfsPlacementReconciliationState::Completed;
                }
            }
        }
        _ => {}
    }
}

enum PlacementNoopCheck {
    CertifiedLocal,
    LookupRemote(crate::DfsMetadataShardConfig),
    RequiresReconciliation,
}

pub(crate) struct PlacementPageAuthority {
    pub(crate) config: DfsFileSystemConfig,
    pub(crate) shards: Vec<crate::DfsMetadataShardConfig>,
    pub(crate) compares: Vec<KvCompare>,
}

enum PlacementIntentAuthority {
    Ordinary,
    Managed(DfsPlacementAuthorityPermit),
    Stale,
}

struct PreparedPlacementObject {
    object: DfsObjectMetadata,
    next_cursor: Option<DfsObjectPageCursor>,
    plan: Option<PreparedPlacementObjectPlan>,
}

struct PreparedPlacementObjectPlan {
    resolved: DfsResolvedPlacementPolicy,
    replication: DfsReplicationPolicy,
    desired_nodes: Vec<NodeId>,
    missing_before: usize,
    content_bearing: bool,
    transfer_bytes: u64,
    change_needed: bool,
    noop_check: PlacementNoopCheck,
}

// One invocation-owned response proof, never a persistent object/cache entry.
// Its exact expected config, object and assignment cannot authorize another row.
struct PlacementRemoteNoopProofs {
    config: DfsFileSystemConfig,
    objects: BTreeMap<DfsObjectId, (DfsObjectMetadata, DfsMetadataShardConfig)>,
}

impl PlacementRemoteNoopProofs {
    fn take_matches(
        &mut self,
        config: &DfsFileSystemConfig,
        object: &DfsObjectMetadata,
        shard: &DfsMetadataShardConfig,
    ) -> bool {
        self.objects
            .remove(&object.object_id)
            .is_some_and(|(expected, expected_shard)| {
                self.config == *config && expected == *object && expected_shard == *shard
            })
    }
}

pub use sinomo_api::dfs::placement::DfsPlacementPolicy;

pub use sinomo_api::dfs::placement::DfsExactPlacementPolicy;

pub use sinomo_api::dfs::placement::DfsPlacementScopeResolution;

pub use sinomo_api::dfs::placement::DfsResolvedPlacementPolicy;

pub fn normalize_placement_rule(
    path: &str,
    scope: DfsPlacementScope,
    policy: DfsPlacementPolicy,
) -> Result<DfsSubtreePolicy, DfsDistributedError> {
    policy.validate()?;
    let path = normalize_posix_path(path).map_err(|error| DfsDistributedError::InvalidPolicy {
        reason: error.to_string(),
    })?;
    Ok(policy.into_rule(path, scope))
}

pub fn upsert_placement_rule(
    rules: &mut Vec<DfsSubtreePolicy>,
    rule: DfsSubtreePolicy,
) -> Option<DfsSubtreePolicy> {
    let previous = if let Some(existing) = rules
        .iter_mut()
        .find(|existing| existing.path_prefix == rule.path_prefix && existing.scope == rule.scope)
    {
        Some(std::mem::replace(existing, rule))
    } else {
        rules.push(rule);
        None
    };
    canonicalize_placement_rule_order(rules);
    previous
}

/// Canonicalize only the ordering that placement resolution proves irrelevant.
///
/// Different `(path_prefix, scope)` identities have explicit path-depth and
/// scope precedence, so their input order cannot select a winner. Duplicate
/// identities are different: legacy data can contain them and the current
/// exact/tied lookup behavior observes their relative order. `sort_by` is
/// stable, which gives us a deterministic inter-identity order without
/// changing that duplicate-rule precedence.
fn canonicalize_placement_rule_order(rules: &mut [DfsSubtreePolicy]) {
    rules.sort_by(|left, right| {
        left.path_prefix
            .cmp(&right.path_prefix)
            .then_with(|| placement_scope_rank(left.scope).cmp(&placement_scope_rank(right.scope)))
    });
}

fn canonical_subtree_policies(rules: &[DfsSubtreePolicy]) -> Vec<DfsSubtreePolicy> {
    let mut canonical = rules.to_vec();
    for rule in &mut canonical {
        // Placement constraints are sets: `DfsReplicationPolicy` sorts and
        // deduplicates both lists before validation or planning. Mirror that
        // established consumption semantics when deciding whether metadata
        // changed, without rewriting the durable rule itself.
        rule.required_node_ids.sort();
        rule.required_node_ids.dedup();
        rule.forbidden_node_ids.sort();
        rule.forbidden_node_ids.dedup();
    }
    canonicalize_placement_rule_order(&mut canonical);
    canonical
}

/// Compare filesystem metadata using placement semantics rather than the
/// incidental order in which distinct subtree rules were discovered.
///
/// This deliberately does not normalize path aliases, legacy count defaults,
/// or the relative order of duplicate `(path_prefix, scope)` rules. Those can
/// affect current resolution. Required/forbidden node lists are canonicalized
/// as sets because the placement planner already does so before use.
pub(crate) fn dfs_file_system_semantically_eq(left: &DfsFileSystem, right: &DfsFileSystem) -> bool {
    left == right
        || (left.file_system_id == right.file_system_id
            && left.display_name == right.display_name
            && left.owner_node_id == right.owner_node_id
            && left.retention_policies == right.retention_policies
            && left.module_metadata == right.module_metadata
            && canonical_subtree_policies(&left.subtree_policies)
                == canonical_subtree_policies(&right.subtree_policies))
}

pub(crate) fn dfs_file_system_config_semantically_eq(
    left: &DfsFileSystemConfig,
    right: &DfsFileSystemConfig,
) -> bool {
    left == right
        || (left.file_system_id == right.file_system_id
            && left.display_name == right.display_name
            && left.owner_node_id == right.owner_node_id
            && left.retention_policies == right.retention_policies
            && left.module_metadata == right.module_metadata
            && canonical_subtree_policies(&left.subtree_policies)
                == canonical_subtree_policies(&right.subtree_policies))
}

pub fn remove_placement_rule(
    rules: &mut Vec<DfsSubtreePolicy>,
    path: &str,
    scope: DfsPlacementScope,
) -> Result<Option<DfsSubtreePolicy>, DfsDistributedError> {
    let path = normalize_posix_path(path).map_err(|error| DfsDistributedError::InvalidPolicy {
        reason: error.to_string(),
    })?;
    let index = rules
        .iter()
        .position(|rule| rule.path_prefix == path && rule.scope == scope);
    Ok(index.map(|index| rules.remove(index)))
}

pub fn resolve_placement_policy(
    config: &DfsFileSystemConfig,
    path: &str,
    object_policy: DfsObjectPolicy,
    legacy_desired_replicas: usize,
) -> Result<DfsResolvedPlacementPolicy, DfsDistributedError> {
    let path = normalize_posix_path(path).map_err(|error| DfsDistributedError::InvalidPolicy {
        reason: error.to_string(),
    })?;
    let mut exact_policies = config
        .subtree_policies
        .iter()
        .filter(|rule| rule.path_prefix == path)
        .map(|rule| {
            placement_policy_from_rule(rule, legacy_desired_replicas).map(|policy| {
                DfsExactPlacementPolicy {
                    scope: rule.scope,
                    policy,
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    exact_policies.sort_by_key(|policy| placement_scope_rank(policy.scope));
    let scope_resolutions = [DfsPlacementScope::SelfOnly, DfsPlacementScope::Subtree]
        .into_iter()
        .map(|scope| {
            resolve_placement_policy_for_scope(
                config,
                &path,
                object_policy.clone(),
                legacy_desired_replicas,
                scope,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let rule = config
        .subtree_policies
        .iter()
        .filter(|rule| placement_rule_matches(rule, &path))
        .max_by(|left, right| {
            placement_path_depth(&left.path_prefix)
                .cmp(&placement_path_depth(&right.path_prefix))
                .then_with(|| {
                    placement_scope_rank(left.scope).cmp(&placement_scope_rank(right.scope))
                })
        });
    let Some(rule) = rule else {
        let policy = legacy_placement_policy(object_policy, legacy_desired_replicas)?;
        return Ok(DfsResolvedPlacementPolicy {
            path,
            policy,
            exact_policies,
            scope_resolutions,
            source_path: None,
            source_scope: None,
            inherited: false,
            explanation: "no managed override matched; using the object's durable policy"
                .to_string(),
        });
    };
    let policy = placement_policy_from_rule(rule, legacy_desired_replicas)?;
    let inherited = rule.path_prefix != path;
    let explanation = if inherited {
        format!(
            "inherited the {:?} rule at {}",
            rule.scope, rule.path_prefix
        )
    } else {
        format!("matched the {:?} rule on this path", rule.scope)
    };
    Ok(DfsResolvedPlacementPolicy {
        path,
        policy,
        exact_policies,
        scope_resolutions,
        source_path: Some(rule.path_prefix.clone()),
        source_scope: Some(rule.scope),
        inherited,
        explanation,
    })
}

fn resolve_placement_policy_for_scope(
    config: &DfsFileSystemConfig,
    path: &str,
    object_policy: DfsObjectPolicy,
    legacy_desired_replicas: usize,
    scope: DfsPlacementScope,
) -> Result<DfsPlacementScopeResolution, DfsDistributedError> {
    if let Some(rule) = config
        .subtree_policies
        .iter()
        .find(|rule| rule.path_prefix == path && rule.scope == scope)
    {
        return Ok(DfsPlacementScopeResolution {
            scope,
            policy: placement_policy_from_rule(rule, legacy_desired_replicas)?,
            exact: true,
            source_path: Some(path.to_string()),
            source_scope: Some(scope),
            inherited: false,
            explanation: format!("matched the {scope:?} rule on this path"),
        });
    }

    // A missing SelfOnly rule falls back to whatever governs the path object,
    // including a same-path Subtree rule. A missing Subtree rule instead falls
    // back only through ancestor Subtree rules; a same-path SelfOnly rule has
    // no meaning for descendants.
    let fallback_rule = config
        .subtree_policies
        .iter()
        .filter(|rule| match scope {
            DfsPlacementScope::SelfOnly => placement_rule_matches(rule, path),
            DfsPlacementScope::Subtree => {
                rule.scope == DfsPlacementScope::Subtree
                    && rule.path_prefix != path
                    && path_is_within(path, &rule.path_prefix)
            }
        })
        .max_by(|left, right| {
            placement_path_depth(&left.path_prefix)
                .cmp(&placement_path_depth(&right.path_prefix))
                .then_with(|| {
                    placement_scope_rank(left.scope).cmp(&placement_scope_rank(right.scope))
                })
        });

    let Some(rule) = fallback_rule else {
        return Ok(DfsPlacementScopeResolution {
            scope,
            policy: legacy_placement_policy(object_policy, legacy_desired_replicas)?,
            exact: false,
            source_path: None,
            source_scope: None,
            inherited: true,
            explanation: "no managed fallback matched; using the object's durable policy"
                .to_string(),
        });
    };
    Ok(DfsPlacementScopeResolution {
        scope,
        policy: placement_policy_from_rule(rule, legacy_desired_replicas)?,
        exact: false,
        source_path: Some(rule.path_prefix.clone()),
        source_scope: Some(rule.scope),
        inherited: true,
        explanation: format!(
            "falls back to the {:?} rule at {}",
            rule.scope, rule.path_prefix
        ),
    })
}

fn placement_policy_from_rule(
    rule: &DfsSubtreePolicy,
    legacy_desired_replicas: usize,
) -> Result<DfsPlacementPolicy, DfsDistributedError> {
    let desired_replicas = rule.desired_replicas.unwrap_or_else(|| {
        // Legacy SIFS local rules omit counts. Their only valid default is one
        // replica, regardless of the cluster's candidate count; explicit
        // counts and constraints still pass through policy validation below.
        if matches!(
            rule.policy,
            DfsObjectPolicy::LocalOwned | DfsObjectPolicy::EphemeralLocal
        ) {
            1
        } else {
            legacy_desired_replicas.clamp(1, 2)
        }
    });
    let min_commit_replicas = rule.min_commit_replicas.unwrap_or_else(|| {
        if rule.policy == DfsObjectPolicy::ReplicatedConfirmed {
            desired_replicas
        } else {
            1
        }
    });
    let policy = DfsPlacementPolicy {
        consistency: rule.policy.clone(),
        min_commit_replicas,
        desired_replicas,
        required_node_ids: rule.required_node_ids.clone(),
        forbidden_node_ids: rule.forbidden_node_ids.clone(),
        min_distinct_zones: rule.min_distinct_zones,
    };
    policy.validate()?;
    Ok(policy)
}

pub fn placement_rule_matches(rule: &DfsSubtreePolicy, path: &str) -> bool {
    match rule.scope {
        DfsPlacementScope::SelfOnly => rule.path_prefix == path,
        DfsPlacementScope::Subtree => path_is_within(path, &rule.path_prefix),
    }
}

fn path_is_within(path: &str, root: &str) -> bool {
    root == "/"
        || path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn placement_path_depth(path: &str) -> usize {
    path.split('/').filter(|part| !part.is_empty()).count()
}

fn placement_scope_rank(scope: DfsPlacementScope) -> u8 {
    match scope {
        DfsPlacementScope::Subtree => 0,
        DfsPlacementScope::SelfOnly => 1,
    }
}

fn legacy_placement_policy(
    policy: DfsObjectPolicy,
    desired_replicas: usize,
) -> Result<DfsPlacementPolicy, DfsDistributedError> {
    let desired_replicas = if matches!(
        policy,
        DfsObjectPolicy::LocalOwned | DfsObjectPolicy::EphemeralLocal
    ) {
        1
    } else {
        desired_replicas.clamp(1, 2)
    };
    let min_commit_replicas = if policy == DfsObjectPolicy::ReplicatedConfirmed {
        desired_replicas
    } else {
        1
    };
    DfsPlacementPolicy::new(policy, min_commit_replicas, desired_replicas)
}

pub use sinomo_api::dfs::placement::DFS_PLACEMENT_PREVIEW_SCHEMA_VERSION;
use sinomo_api::dfs::placement::DFS_PLACEMENT_PREVIEW_TOKEN_DIGEST_PREFIX;
pub use sinomo_api::dfs::placement::DFS_PLACEMENT_PREVIEW_TOKEN_SCHEMA_VERSION;
const DFS_PLACEMENT_METADATA_SNAPSHOT_DIGEST_PREFIX: &str =
    "dfs-placement-metadata-set-v1:blake3-xor:";

pub use sinomo_api::dfs::placement::DfsPlacementPreviewToken;

/// Exact selected-object metadata used to fence a preview. Subtree previews
/// additionally carry a bounded-memory digest of every matching metadata
/// record in [`DfsPlacementImpact::metadata_snapshot_digest`].
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementPreviewObjectFence {
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<DfsObjectKind>,
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default)]
    pub changed_at_unix_nanos: u64,
    pub policy: DfsObjectPolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub current_locations: Vec<NodeId>,
}

impl From<&DfsStat> for DfsPlacementPreviewObjectFence {
    fn from(stat: &DfsStat) -> Self {
        let mut current_locations = stat.current_locations.clone();
        current_locations.sort();
        current_locations.dedup();
        Self {
            file_system_id: stat.file_system_id.clone(),
            object_id: stat.object_id.clone(),
            path: stat.path.clone(),
            kind: Some(stat.kind.clone()),
            version: stat.version.clone(),
            content_hash: stat.content_hash.clone(),
            size_bytes: stat.size_bytes,
            changed_at_unix_nanos: stat.changed_at_unix_nanos,
            policy: stat.policy.clone(),
            current_locations,
        }
    }
}

pub use sinomo_api::dfs::placement::DfsPlacementPreview;

#[derive(Serialize)]
struct DfsPlacementPreviewCandidateTokenMaterial {
    node_id: NodeId,
    capacity_weight: u32,
    accepts_content: bool,
    zone: Option<String>,
}

// Bind authorization to the recomputed decision, not its changing diagnostic
// measurements. The selector remains the sole owner of eligibility and ranking.
#[derive(Serialize)]
struct DfsPlacementDecisionAuthorization<'a> {
    node_id: &'a NodeId,
    zone: &'a Option<String>,
    eligible: bool,
    selected: bool,
    preferred_for_commit: bool,
}

impl<'a> From<&'a crate::DfsReplicaPlacementDecision> for DfsPlacementDecisionAuthorization<'a> {
    fn from(decision: &'a crate::DfsReplicaPlacementDecision) -> Self {
        Self {
            node_id: &decision.node_id,
            zone: &decision.zone,
            eligible: decision.eligible,
            selected: decision.selected,
            preferred_for_commit: decision.preferred_for_commit,
        }
    }
}

#[derive(Serialize)]
struct DfsPlacementNodeAuthorization<'a> {
    node_id: &'a NodeId,
    zone: &'a Option<String>,
    capacity_weight: u32,
    accepts_content: bool,
    reachability: DfsPlacementNodeReachability,
    current_replica: bool,
    desired_replica: bool,
    preferred_for_commit: bool,
    required: bool,
    forbidden: bool,
}

#[derive(Serialize)]
struct DfsPlacementPreviewAuthorization<'a> {
    schema_version: u32,
    resolved: &'a DfsResolvedPlacementPolicy,
    plan: &'a crate::DfsReplicaPlacementPlan,
    represented_zone_count: usize,
    decisions: Vec<DfsPlacementDecisionAuthorization<'a>>,
    current_replica_node_ids: &'a [NodeId],
    added_replica_node_ids: &'a [NodeId],
    removed_replica_node_ids: &'a [NodeId],
    nodes: Vec<DfsPlacementNodeAuthorization<'a>>,
    blocking_diagnostics: &'a [DfsPlacementBlockingDiagnostic],
    impact: &'a DfsPlacementImpact,
}

impl<'a> From<&'a DfsPlacementPreview> for DfsPlacementPreviewAuthorization<'a> {
    fn from(preview: &'a DfsPlacementPreview) -> Self {
        Self {
            schema_version: preview.schema_version,
            resolved: &preview.resolved,
            plan: &preview.placement.plan,
            represented_zone_count: preview.placement.represented_zone_count,
            decisions: preview
                .placement
                .decisions
                .iter()
                .map(DfsPlacementDecisionAuthorization::from)
                .collect(),
            current_replica_node_ids: &preview.current_replica_node_ids,
            added_replica_node_ids: &preview.added_replica_node_ids,
            removed_replica_node_ids: &preview.removed_replica_node_ids,
            nodes: preview
                .nodes
                .iter()
                .map(|node| DfsPlacementNodeAuthorization {
                    node_id: &node.node_id,
                    zone: &node.zone,
                    capacity_weight: node.capacity_weight,
                    accepts_content: node.accepts_content,
                    reachability: node.reachability,
                    current_replica: node.current_replica,
                    desired_replica: node.desired_replica,
                    preferred_for_commit: node.preferred_for_commit,
                    required: node.required,
                    forbidden: node.forbidden,
                })
                .collect(),
            blocking_diagnostics: &preview.blocking_diagnostics,
            impact: &preview.impact,
        }
    }
}

#[derive(Serialize)]
struct DfsPlacementPreviewTokenMaterial<'a> {
    domain: &'static str,
    base_config: &'a DfsFileSystemConfig,
    desired_config: &'a DfsFileSystemConfig,
    path: &'a str,
    scope: DfsPlacementScope,
    object: &'a DfsPlacementPreviewObjectFence,
    candidates: Vec<DfsPlacementPreviewCandidateTokenMaterial>,
    unavailable_node_ids: &'a BTreeSet<NodeId>,
    preview: DfsPlacementPreviewAuthorization<'a>,
}

/// Seal a fully enriched preview to its authorization inputs and exact metadata
/// snapshot. Diagnostic sampling counters and raw performance/capacity readings
/// are excluded; the recomputed selected plan and eligibility are still fenced.
/// Call this only after aggregate impact, final
/// connectivity, and compatibility diagnostics have been attached.
pub fn bind_placement_preview_token(
    preview: &mut DfsPlacementPreview,
    base_config: &DfsFileSystemConfig,
    desired_config: &DfsFileSystemConfig,
    path: &str,
    scope: DfsPlacementScope,
    object: &DfsPlacementPreviewObjectFence,
    candidates: &[DfsNodeAdvertisement],
    unavailable_node_ids: &BTreeSet<NodeId>,
) -> Result<DfsPlacementPreviewToken, DfsError> {
    preview.schema_version = DFS_PLACEMENT_PREVIEW_SCHEMA_VERSION;
    preview.preview_token = DfsPlacementPreviewToken::default();
    let token = compute_placement_preview_token(
        preview,
        base_config,
        desired_config,
        path,
        scope,
        object,
        candidates,
        unavailable_node_ids,
    )?;
    preview.preview_token = token.clone();
    Ok(token)
}

#[allow(clippy::too_many_arguments)]
fn compute_placement_preview_token(
    preview: &DfsPlacementPreview,
    base_config: &DfsFileSystemConfig,
    desired_config: &DfsFileSystemConfig,
    path: &str,
    scope: DfsPlacementScope,
    object: &DfsPlacementPreviewObjectFence,
    candidates: &[DfsNodeAdvertisement],
    unavailable_node_ids: &BTreeSet<NodeId>,
) -> Result<DfsPlacementPreviewToken, DfsError> {
    let mut candidates = candidates
        .iter()
        .map(|candidate| DfsPlacementPreviewCandidateTokenMaterial {
            node_id: candidate.node_id.clone(),
            capacity_weight: candidate.capacity_weight,
            accepts_content: candidate.accepts_content,
            zone: candidate.zone.clone(),
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then_with(|| left.accepts_content.cmp(&right.accepts_content))
            .then_with(|| left.capacity_weight.cmp(&right.capacity_weight))
            .then_with(|| left.zone.cmp(&right.zone))
    });
    let material = DfsPlacementPreviewTokenMaterial {
        domain: "sinomo-dfs-placement-preview-authorization-v1",
        base_config,
        desired_config,
        path,
        scope,
        object,
        candidates,
        unavailable_node_ids,
        preview: DfsPlacementPreviewAuthorization::from(preview),
    };
    let bytes = serde_json::to_vec(&material).map_err(|error| DfsError::Serialization {
        message: format!("cannot encode placement preview token material: {error}"),
    })?;
    Ok(DfsPlacementPreviewToken {
        schema_version: DFS_PLACEMENT_PREVIEW_TOKEN_SCHEMA_VERSION,
        digest: format!(
            "{DFS_PLACEMENT_PREVIEW_TOKEN_DIGEST_PREFIX}{}",
            blake3::hash(&bytes).to_hex()
        ),
    })
}

pub use sinomo_api::dfs::placement::DfsPlacementNodeReachability;

pub use sinomo_api::dfs::placement::DfsPlacementNodePreview;

pub use sinomo_api::dfs::placement::DfsPlacementBlockingDiagnostic;

/// Return the rolling-upgrade blocker for snapshot-bound placement jobs.
///
/// This intentionally fails closed for authorized records with no daemon
/// capabilities: an older or not-yet-refreshed main daemon must not be allowed
/// to receive a v1 routed reconciliation operation. Explicit legacy shadow
/// records are excluded because they never host DFS maintenance or metadata.
pub fn placement_reconciliation_compatibility_diagnostic(
    state: &ClusterBootstrapState,
) -> Option<DfsPlacementBlockingDiagnostic> {
    let mut node_ids = state
        .members
        .iter()
        .filter(|member| {
            member.state.is_authorized()
                && !cluster_member_is_legacy_shadow_daemon(member)
                && !member.capabilities.iter().any(|capability| {
                    capability.as_str() == DFS_PLACEMENT_RECONCILIATION_V1_CAPABILITY
                })
        })
        .map(|member| member.node_id.clone())
        .collect::<Vec<_>>();
    node_ids.sort();
    node_ids.dedup();
    (!node_ids.is_empty())
        .then_some(DfsPlacementBlockingDiagnostic::IncompatibleDaemonVersion { node_ids })
}

pub use sinomo_api::dfs::placement::DfsPlacementImpact;

/// A single bounded metadata page of placement-impact analysis. `impact` is a
/// subtotal for this page only; callers can release their service lock and
/// resume from `next_cursor` without retaining the page's object metadata.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementImpactPage {
    pub impact: DfsPlacementImpact,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocking_diagnostics: Vec<DfsPlacementBlockingDiagnostic>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsObjectPageCursor>,
}

fn placement_config_with_override(
    config: &DfsFileSystemConfig,
    path: &str,
    scope: DfsPlacementScope,
    override_policy: Option<DfsPlacementPolicy>,
) -> Result<DfsFileSystemConfig, DfsDistributedError> {
    let mut preview_config = config.clone();
    if let Some(policy) = override_policy {
        let rule = normalize_placement_rule(path, scope, policy)?;
        upsert_placement_rule(&mut preview_config.subtree_policies, rule);
    }
    Ok(preview_config)
}

/// Preview managed placement using the filesystem owner as the deterministic
/// planning seed. `observer_node_id` is used only to mark the local table row.
#[allow(clippy::too_many_arguments)]
pub fn preview_placement(
    config: &DfsFileSystemConfig,
    path: &str,
    object_id: DfsObjectId,
    version: DfsVersion,
    object_policy: DfsObjectPolicy,
    observer_node_id: NodeId,
    current_replica_node_ids: Vec<NodeId>,
    candidates: Vec<DfsNodeAdvertisement>,
    override_policy: Option<DfsPlacementPolicy>,
    override_scope: DfsPlacementScope,
) -> Result<DfsPlacementPreview, DfsDistributedError> {
    preview_placement_with_unavailable_nodes(
        config,
        path,
        object_id,
        version,
        object_policy,
        observer_node_id,
        current_replica_node_ids,
        candidates,
        override_policy,
        override_scope,
        &BTreeSet::new(),
    )
}

/// Preview placement while excluding nodes that the caller has positively
/// identified as unavailable. Excluded advertisements remain in `nodes` for
/// operator context, but cannot satisfy N, W, required-node, or zone checks.
///
/// Managed placement is seeded by the filesystem owner, making the desired set
/// stable regardless of which daemon serves the preview. The explicit node id
/// identifies only the observer used for the table's `Local` reachability.
#[allow(clippy::too_many_arguments)]
pub fn preview_placement_with_unavailable_nodes(
    config: &DfsFileSystemConfig,
    path: &str,
    object_id: DfsObjectId,
    version: DfsVersion,
    object_policy: DfsObjectPolicy,
    observer_node_id: NodeId,
    current_replica_node_ids: Vec<NodeId>,
    candidates: Vec<DfsNodeAdvertisement>,
    override_policy: Option<DfsPlacementPolicy>,
    override_scope: DfsPlacementScope,
    unavailable_node_ids: &BTreeSet<NodeId>,
) -> Result<DfsPlacementPreview, DfsDistributedError> {
    preview_placement_with_snapshot_and_unavailable_nodes(
        config,
        path,
        object_id,
        version,
        object_policy,
        observer_node_id,
        current_replica_node_ids,
        candidates,
        override_policy,
        override_scope,
        None,
        &[],
        unavailable_node_ids,
    )
}

/// Preview placement from the same primary-owned environmental snapshot used
/// by new writes and adaptive reconciliation. Manual content pins are included
/// in the desired set, so an accepted preview cannot later disagree with the
/// durable reconciler about additive retention constraints.
#[allow(clippy::too_many_arguments)]
pub fn preview_placement_with_snapshot_and_unavailable_nodes(
    config: &DfsFileSystemConfig,
    path: &str,
    object_id: DfsObjectId,
    version: DfsVersion,
    object_policy: DfsObjectPolicy,
    observer_node_id: NodeId,
    current_replica_node_ids: Vec<NodeId>,
    candidates: Vec<DfsNodeAdvertisement>,
    override_policy: Option<DfsPlacementPolicy>,
    override_scope: DfsPlacementScope,
    placement_snapshot: Option<&crate::DfsPlacementSnapshot>,
    pinned_replica_node_ids: &[NodeId],
    unavailable_node_ids: &BTreeSet<NodeId>,
) -> Result<DfsPlacementPreview, DfsDistributedError> {
    let normalized_path =
        normalize_posix_path(path).map_err(|error| DfsDistributedError::InvalidPolicy {
            reason: error.to_string(),
        })?;
    let preview_config =
        placement_config_with_override(config, path, override_scope, override_policy)?;
    let mut object_fence_locations = current_replica_node_ids.clone();
    object_fence_locations.sort();
    object_fence_locations.dedup();
    let object_fence = DfsPlacementPreviewObjectFence {
        file_system_id: config.file_system_id.clone(),
        object_id: object_id.clone(),
        path: normalized_path.clone(),
        kind: None,
        version: version.clone(),
        content_hash: None,
        size_bytes: None,
        changed_at_unix_nanos: 0,
        policy: object_policy.clone(),
        current_locations: object_fence_locations,
    };
    let current_object_policy = object_policy.clone();
    let exact_scoped_policy = preview_config
        .subtree_policies
        .iter()
        .find(|rule| rule.path_prefix == normalized_path && rule.scope == override_scope)
        .map(|rule| placement_policy_from_rule(rule, candidates.len()))
        .transpose()?;
    let resolved =
        resolve_placement_policy(&preview_config, path, object_policy, candidates.len())?;
    let (replication, replica_candidates, mut blocking_diagnostics) =
        placement_inputs(&resolved.policy, &candidates, unavailable_node_ids)?;

    // A same-path SelfOnly rule legitimately wins resolution for the path
    // object itself. It must not, however, hide an unsatisfiable Subtree rule
    // that the operator is proposing for descendants.
    if let Some(exact_scoped_policy) = exact_scoped_policy
        && (resolved.source_path.as_deref() != Some(normalized_path.as_str())
            || resolved.source_scope != Some(override_scope)
            || resolved.policy != exact_scoped_policy)
    {
        let (_, _, proposed_diagnostics) =
            placement_inputs(&exact_scoped_policy, &candidates, unavailable_node_ids)?;
        extend_unique_diagnostics(
            &mut blocking_diagnostics,
            scoped_diagnostics(&normalized_path, override_scope, proposed_diagnostics),
        );
    }
    let placement_request = DfsReplicaPlacementRequest {
        file_system_id: config.file_system_id.clone(),
        object_id,
        target_version: version,
        coordinator_node_id: config.owner_node_id.clone(),
        policy: replication,
        candidates: replica_candidates,
    };
    let placement = if blocking_diagnostics.is_empty() {
        match placement_snapshot {
            Some(snapshot) => crate::explain_replicas_with_placement_snapshot(
                &placement_request,
                snapshot,
                &current_replica_node_ids,
                pinned_replica_node_ids,
            )?,
            None => explain_replicas(&placement_request)?,
        }
    } else {
        explain_replicas_best_effort(&placement_request)?
    };
    let current = current_replica_node_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let desired = placement
        .plan
        .desired_replica_nodes
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let added_replica_node_ids: Vec<NodeId> = desired.difference(&current).cloned().collect();
    let removed_replica_node_ids: Vec<NodeId> = current.difference(&desired).cloned().collect();
    let nodes = placement_node_previews(
        &candidates,
        &observer_node_id,
        unavailable_node_ids,
        &current,
        &resolved.policy,
        &placement,
    );
    let impact = DfsPlacementImpact {
        affected_object_count: if current_object_policy != resolved.policy.consistency
            || !added_replica_node_ids.is_empty()
            || !removed_replica_node_ids.is_empty()
        {
            1
        } else {
            0
        },
        affected_bytes: 0,
        unknown_size_object_count: 0,
        copies_added: added_replica_node_ids.len().try_into().unwrap_or(u64::MAX),
        copies_removed: removed_replica_node_ids
            .len()
            .try_into()
            .unwrap_or(u64::MAX),
        metadata_snapshot_digest: String::new(),
        complete: blocking_diagnostics.is_empty(),
    };
    let mut warnings = Vec::new();
    if resolved.policy.min_commit_replicas < resolved.policy.desired_replicas {
        warnings.push(format!(
            "writes become visible at W={} while repair continues toward N={}",
            resolved.policy.min_commit_replicas, resolved.policy.desired_replicas
        ));
    }
    let mut preview = DfsPlacementPreview {
        schema_version: DFS_PLACEMENT_PREVIEW_SCHEMA_VERSION,
        preview_token: DfsPlacementPreviewToken::default(),
        resolved,
        placement,
        current_replica_node_ids,
        added_replica_node_ids,
        removed_replica_node_ids,
        nodes,
        blocking_diagnostics,
        impact,
        warnings,
    };
    if let Err(error) = bind_placement_preview_token(
        &mut preview,
        config,
        &preview_config,
        &normalized_path,
        override_scope,
        &object_fence,
        &candidates,
        unavailable_node_ids,
    ) {
        preview.warnings.push(format!(
            "this preview could not be sealed for Apply and must be refreshed: {error}"
        ));
    }
    Ok(preview)
}

fn placement_inputs(
    policy: &DfsPlacementPolicy,
    advertisements: &[DfsNodeAdvertisement],
    unavailable_node_ids: &BTreeSet<NodeId>,
) -> Result<
    (
        DfsReplicationPolicy,
        Vec<DfsReplicaCandidate>,
        Vec<DfsPlacementBlockingDiagnostic>,
    ),
    DfsDistributedError,
> {
    let node_zones = advertisements
        .iter()
        .filter_map(|candidate| {
            candidate
                .zone
                .clone()
                .map(|zone| (candidate.node_id.clone(), zone))
        })
        .collect::<BTreeMap<_, _>>();
    let replication = policy.replication_policy(node_zones)?;
    let replica_candidates = advertisements
        .iter()
        .filter(|candidate| {
            candidate.accepts_content && !unavailable_node_ids.contains(&candidate.node_id)
        })
        .map(|candidate| {
            DfsReplicaCandidate::weighted(
                candidate.node_id.clone(),
                candidate.capacity_weight.max(1),
            )
            .map(|candidate_plan| candidate_plan.with_zone(candidate.zone.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let blocking_diagnostics = placement_blocking_diagnostics(&replication, &replica_candidates);
    Ok((replication, replica_candidates, blocking_diagnostics))
}

fn scoped_diagnostics(
    path: &str,
    scope: DfsPlacementScope,
    diagnostics: Vec<DfsPlacementBlockingDiagnostic>,
) -> Vec<DfsPlacementBlockingDiagnostic> {
    diagnostics
        .into_iter()
        .map(|diagnostic| DfsPlacementBlockingDiagnostic::ScopedPolicy {
            path: path.to_string(),
            scope,
            diagnostic: Box::new(diagnostic),
        })
        .collect()
}

fn extend_unique_diagnostics(
    target: &mut Vec<DfsPlacementBlockingDiagnostic>,
    diagnostics: impl IntoIterator<Item = DfsPlacementBlockingDiagnostic>,
) {
    for diagnostic in diagnostics {
        if !target.contains(&diagnostic) {
            target.push(diagnostic);
        }
    }
}

fn extend_preview_diagnostics(
    preview: &mut DfsPlacementPreview,
    diagnostics: impl IntoIterator<Item = DfsPlacementBlockingDiagnostic>,
) {
    let anchor_source = preview
        .resolved
        .source_path
        .clone()
        .zip(preview.resolved.source_scope)
        .unwrap_or_else(|| (preview.resolved.path.clone(), DfsPlacementScope::SelfOnly));
    for diagnostic in diagnostics {
        let duplicates_anchor = matches!(
            &diagnostic,
            DfsPlacementBlockingDiagnostic::ScopedPolicy {
                path,
                scope,
                diagnostic,
            } if path == &anchor_source.0
                && scope == &anchor_source.1
                && preview
                    .blocking_diagnostics
                    .iter()
                    .any(|existing| existing == diagnostic.as_ref())
        );
        if !duplicates_anchor && !preview.blocking_diagnostics.contains(&diagnostic) {
            preview.blocking_diagnostics.push(diagnostic);
        }
    }
}

fn placement_blocking_diagnostics(
    policy: &DfsReplicationPolicy,
    candidates: &[DfsReplicaCandidate],
) -> Vec<DfsPlacementBlockingDiagnostic> {
    let eligible = candidates
        .iter()
        .filter(|candidate| !policy.forbidden_node_ids.contains(&candidate.node_id))
        .collect::<Vec<_>>();
    let eligible_node_ids = eligible
        .iter()
        .map(|candidate| &candidate.node_id)
        .collect::<BTreeSet<_>>();
    let mut diagnostics = Vec::new();
    if eligible_node_ids.len() < policy.desired_replicas {
        diagnostics.push(DfsPlacementBlockingDiagnostic::InsufficientCandidates {
            available: eligible_node_ids.len(),
            required: policy.desired_replicas,
        });
    }
    for node_id in &policy.required_node_ids {
        if !eligible_node_ids.contains(node_id) {
            diagnostics.push(DfsPlacementBlockingDiagnostic::RequiredNodeUnavailable {
                node_id: node_id.clone(),
            });
        }
    }
    if policy.min_distinct_zones > 0 {
        let required_node_ids = policy.required_node_ids.iter().collect::<BTreeSet<_>>();
        let required_zones = eligible
            .iter()
            .filter(|candidate| required_node_ids.contains(&candidate.node_id))
            .filter_map(|candidate| {
                candidate
                    .zone
                    .as_ref()
                    .or_else(|| policy.node_zones.get(&candidate.node_id))
            })
            .collect::<BTreeSet<_>>();
        let additional_zones = eligible
            .iter()
            .filter(|candidate| !required_node_ids.contains(&candidate.node_id))
            .filter_map(|candidate| {
                candidate
                    .zone
                    .as_ref()
                    .or_else(|| policy.node_zones.get(&candidate.node_id))
            })
            .filter(|zone| !required_zones.contains(zone))
            .collect::<BTreeSet<_>>();
        let eligible_required_count = eligible
            .iter()
            .filter(|candidate| required_node_ids.contains(&candidate.node_id))
            .count();
        let remaining_slots = policy
            .desired_replicas
            .saturating_sub(eligible_required_count);
        let available = required_zones
            .len()
            .saturating_add(additional_zones.len().min(remaining_slots));
        if available < policy.min_distinct_zones {
            diagnostics.push(DfsPlacementBlockingDiagnostic::InsufficientZones {
                available,
                required: policy.min_distinct_zones,
            });
        }
    }
    diagnostics
}

fn placement_node_previews(
    advertisements: &[DfsNodeAdvertisement],
    observer_node_id: &NodeId,
    unavailable_node_ids: &BTreeSet<NodeId>,
    current_replica_node_ids: &BTreeSet<NodeId>,
    policy: &DfsPlacementPolicy,
    placement: &DfsReplicaPlacementExplanation,
) -> Vec<DfsPlacementNodePreview> {
    let advertisements_by_node = advertisements
        .iter()
        .map(|advertisement| (advertisement.node_id.clone(), advertisement))
        .collect::<BTreeMap<_, _>>();
    let decisions_by_node = placement
        .decisions
        .iter()
        .map(|decision| (decision.node_id.clone(), decision))
        .collect::<BTreeMap<_, _>>();
    let mut node_ids = advertisements_by_node
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    node_ids.extend(unavailable_node_ids.iter().cloned());
    node_ids.extend(current_replica_node_ids.iter().cloned());
    node_ids.extend(policy.required_node_ids.iter().cloned());
    node_ids.extend(policy.forbidden_node_ids.iter().cloned());
    node_ids.extend(placement.plan.desired_replica_nodes.iter().cloned());

    node_ids
        .into_iter()
        .map(|node_id| {
            let advertisement = advertisements_by_node.get(&node_id).copied();
            let decision = decisions_by_node.get(&node_id).copied();
            let accepts_content = advertisement.is_some_and(|value| value.accepts_content);
            let required = policy.required_node_ids.contains(&node_id);
            let forbidden = policy.forbidden_node_ids.contains(&node_id);
            let current_replica = current_replica_node_ids.contains(&node_id);
            let explanation = if let Some(decision) = decision {
                decision.reason.clone()
            } else if unavailable_node_ids.contains(&node_id) {
                "advertised for content but currently unavailable".to_string()
            } else if required && !accepts_content {
                "required by policy but unavailable as a content candidate".to_string()
            } else if forbidden {
                "rejected: forbidden by policy".to_string()
            } else if !accepts_content {
                if current_replica {
                    "current replica, but no content-service advertisement is available".to_string()
                } else {
                    "not advertised as an eligible content node".to_string()
                }
            } else {
                "eligible content node".to_string()
            };
            DfsPlacementNodePreview {
                node_id: node_id.clone(),
                zone: advertisement
                    .and_then(|value| value.zone.clone())
                    .or_else(|| decision.and_then(|value| value.zone.clone())),
                capacity_weight: advertisement
                    .map(|value| value.capacity_weight.max(1))
                    .unwrap_or(0),
                accepts_content,
                reachability: if unavailable_node_ids.contains(&node_id) {
                    DfsPlacementNodeReachability::Unreachable
                } else if &node_id == observer_node_id {
                    DfsPlacementNodeReachability::Local
                } else {
                    DfsPlacementNodeReachability::Unknown
                },
                current_replica,
                desired_replica: placement.plan.desired_replica_nodes.contains(&node_id),
                preferred_for_commit: placement.plan.preferred_commit_nodes.contains(&node_id),
                required,
                forbidden,
                storage_health: decision
                    .and_then(|value| value.storage_health)
                    .or_else(|| advertisement.map(|value| value.storage_health)),
                storage_freshness: decision
                    .and_then(|value| value.storage_freshness)
                    .or_else(|| advertisement.map(|value| value.storage_freshness)),
                usable_capacity_bytes: decision
                    .and_then(|value| value.usable_capacity_bytes)
                    .or_else(|| advertisement.and_then(|value| value.usable_capacity_bytes)),
                reserved_free_bytes: advertisement.and_then(|value| value.reserved_free_bytes),
                path_state: decision.and_then(|value| value.path_state),
                path_latency_micros: decision.and_then(|value| value.path_latency_micros),
                path_throughput_bytes_per_second: decision
                    .and_then(|value| value.path_throughput_bytes_per_second),
                path_metering: decision
                    .and_then(|value| value.path_metering)
                    .or_else(|| advertisement.map(|value| value.network_metering)),
                observation_freshness: decision.and_then(|value| value.observation_freshness),
                explanation,
            }
        })
        .collect()
}

struct ObjectPlacementAnalysis {
    resolved: DfsResolvedPlacementPolicy,
    placement: DfsReplicaPlacementExplanation,
    blocking_diagnostics: Vec<DfsPlacementBlockingDiagnostic>,
}

fn analyze_object_placement(
    config: &DfsFileSystemConfig,
    object: &DfsObjectMetadata,
    candidates: &[DfsNodeAdvertisement],
    unavailable_node_ids: &BTreeSet<NodeId>,
    snapshot: Option<&crate::DfsPlacementSnapshot>,
    pinned_node_ids: &[NodeId],
) -> Result<ObjectPlacementAnalysis, DfsDistributedError> {
    let resolved = resolve_placement_policy(
        config,
        &object.path,
        object.policy.clone(),
        candidates.len(),
    )?;
    let (replication, replica_candidates, blocking_diagnostics) =
        placement_inputs(&resolved.policy, candidates, unavailable_node_ids)?;
    let request = DfsReplicaPlacementRequest {
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        target_version: object.version.clone(),
        coordinator_node_id: config.owner_node_id.clone(),
        policy: replication,
        candidates: replica_candidates,
    };
    let placement = if blocking_diagnostics.is_empty() {
        explain_object_placement(
            &request,
            object,
            &resolved.policy.consistency,
            snapshot,
            pinned_node_ids,
        )?
    } else {
        explain_replicas_best_effort(&request)?
    };
    Ok(ObjectPlacementAnalysis {
        resolved,
        placement,
        blocking_diagnostics,
    })
}

fn resolved_diagnostic_scope(
    resolved: &DfsResolvedPlacementPolicy,
    object_path: &str,
) -> (String, DfsPlacementScope) {
    match (&resolved.source_path, resolved.source_scope) {
        (Some(path), Some(scope)) => (path.clone(), scope),
        _ => (object_path.to_string(), DfsPlacementScope::SelfOnly),
    }
}

pub use sinomo_api::dfs::placement::DfsPlacementReconciliationState;

pub use sinomo_api::dfs::placement::DfsAdaptivePlacementBudget;

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct DfsAdaptivePlacementObservationState {
    pub pending_snapshot_digest: String,
    pub pending_snapshot_generation: u64,
    pub first_observed_at_unix_nanos: u64,
    pub consecutive_observations: u32,
    pub last_enqueued_snapshot_digest: String,
    pub last_enqueued_snapshot_generation: u64,
}

pub use sinomo_api::dfs::placement::DfsPlacementReconciliationJob;

pub use sinomo_api::dfs::placement::DfsPlacementReconciliationActivityStatus;

pub use sinomo_api::dfs::placement::DfsPlacementReconciliationActivityCursor;

pub use sinomo_api::dfs::placement::DfsPlacementReconciliationActivitySummary;

pub use sinomo_api::dfs::placement::DfsPlacementReconciliationActivityPage;

/// Build a stable, compact activity page from durable reconciliation records.
/// The caller owns retrieval; this helper bounds the returned wire projection.
pub fn placement_reconciliation_activity_page<'a>(
    jobs: impl IntoIterator<Item = &'a DfsPlacementReconciliationJob>,
    statuses: &[DfsPlacementReconciliationActivityStatus],
    path_prefix: Option<&str>,
    after: Option<&DfsPlacementReconciliationActivityCursor>,
    limit: usize,
) -> DfsPlacementReconciliationActivityPage {
    placement_reconciliation_activity_page_from_summaries(
        jobs.into_iter()
            .map(DfsPlacementReconciliationActivitySummary::from),
        statuses,
        path_prefix,
        after,
        limit,
    )
}

pub(crate) fn placement_reconciliation_activity_page_from_summaries(
    activities: impl IntoIterator<Item = DfsPlacementReconciliationActivitySummary>,
    statuses: &[DfsPlacementReconciliationActivityStatus],
    path_prefix: Option<&str>,
    after: Option<&DfsPlacementReconciliationActivityCursor>,
    limit: usize,
) -> DfsPlacementReconciliationActivityPage {
    let mut activities = activities
        .into_iter()
        .map(|mut activity| {
            activity.status = DfsPlacementReconciliationActivityStatus::from_execution(
                activity.phase,
                activity.last_error.as_deref(),
            );
            activity
        })
        .filter(|activity| {
            (statuses.is_empty() || statuses.contains(&activity.status))
                && path_prefix.is_none_or(|prefix| path_is_within(&activity.path, prefix))
        })
        .filter(|activity| {
            after.is_none_or(|cursor| {
                activity.created_at_unix_nanos < cursor.created_at_unix_nanos
                    || (activity.created_at_unix_nanos == cursor.created_at_unix_nanos
                        && activity.job_id < cursor.job_id)
            })
        })
        .collect::<Vec<_>>();
    activities.sort_by(|left, right| {
        right
            .created_at_unix_nanos
            .cmp(&left.created_at_unix_nanos)
            .then_with(|| right.job_id.cmp(&left.job_id))
    });
    let limit = limit.clamp(1, MAX_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE);
    let has_more = activities.len() > limit;
    activities.truncate(limit);
    let next_cursor = has_more
        .then(|| {
            activities
                .last()
                .map(DfsPlacementReconciliationActivitySummary::cursor)
        })
        .flatten();
    DfsPlacementReconciliationActivityPage {
        activities,
        next_cursor,
        partial: false,
        history_truncated: false,
    }
}

pub fn publish_dfs_content_advertisement_if_needed(
    control_plane: &mut dyn DfsControlPlaneKvClient,
    local_node_id: &NodeId,
    mut desired: DfsNodeAdvertisement,
) -> Result<bool, DfsError> {
    if desired.node_id != *local_node_id {
        return Err(DfsError::Distributed {
            message: format!(
                "local content advertisement names node {} but service owns {}",
                desired.node_id, local_node_id
            ),
        });
    }
    desired.accepts_metadata = false;
    let mut store = DfsControlPlaneStore::new(control_plane);
    if let Some(current) = store.content_service(local_node_id)?
        && dfs_content_advertisement_is_current(&current, &desired)
    {
        return Ok(false);
    }
    store.advertise_content_service(&desired)?;
    Ok(true)
}

pub async fn publish_dfs_content_advertisement_if_needed_async(
    control_plane: &mut dyn AsyncDfsControlPlaneKvClient,
    local_node_id: &NodeId,
    mut desired: DfsNodeAdvertisement,
) -> Result<bool, DfsError> {
    if desired.node_id != *local_node_id {
        return Err(DfsError::Distributed {
            message: format!(
                "local content advertisement names node {} but service owns {}",
                desired.node_id, local_node_id
            ),
        });
    }
    desired.accepts_metadata = false;
    let key = crate::content_service_key(local_node_id);
    if let Some(current) = control_plane
        .get_async(&key)
        .await?
        .map(|entry| entry.decode::<DfsNodeAdvertisement>())
        .transpose()?
        && dfs_content_advertisement_is_current(&current, &desired)
    {
        return Ok(false);
    }
    publish_dfs_content_advertisement_async(control_plane, local_node_id, desired).await?;
    Ok(true)
}

pub async fn publish_dfs_content_advertisement_async(
    control_plane: &mut dyn AsyncDfsControlPlaneKvClient,
    local_node_id: &NodeId,
    mut desired: DfsNodeAdvertisement,
) -> Result<(), DfsError> {
    if desired.node_id != *local_node_id {
        return Err(DfsError::Distributed {
            message: format!(
                "local content advertisement names node {} but service owns {}",
                desired.node_id, local_node_id
            ),
        });
    }
    desired.accepts_metadata = false;
    control_plane
        .put_async(
            crate::content_service_key(local_node_id),
            sinomo_core::kv::encode_kv_value(&desired)?,
        )
        .await?;
    Ok(())
}

fn dfs_content_advertisement_is_current(
    current: &DfsNodeAdvertisement,
    desired: &DfsNodeAdvertisement,
) -> bool {
    let lease_is_fresh = desired.expires_at.is_none()
        || current.expires_at.is_some_and(|expires_at| {
            expires_at > time::OffsetDateTime::now_utc() + time::Duration::seconds(30)
        });
    lease_is_fresh
        && current.node_id == desired.node_id
        && current.accepts_content == desired.accepts_content
        && current.capacity_weight == desired.capacity_weight
        && current.zone == desired.zone
        && current.storage_health == desired.storage_health
        && current.storage_freshness == desired.storage_freshness
        && current.content_location_count == desired.content_location_count
        && current.network_metering == desired.network_metering
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn current_placement_file_system_config(&mut self) -> Result<DfsFileSystemConfig, DfsError> {
        let file_system_id = crate::sifs_file_system_id();
        if let Some(config) = self
            .namespace_execution
            .context()
            .filter(|context| context.file_system_id == file_system_id)
            .and_then(|context| context.placement_config.clone())
        {
            return Ok(config);
        }
        let config = if let Some(control_plane) = self.control_plane.as_mut()
            && let Some(config) = DfsControlPlaneStore::new(control_plane.as_mut())
                .get_file_system_config(&file_system_id)?
        {
            config
        } else {
            self.metadata
                .filesystems
                .get(&file_system_id)
                .map(DfsFileSystemConfig::from)
                .ok_or_else(|| DfsError::UnknownFileSystem {
                    file_system_id: file_system_id.clone(),
                })?
        };
        if let Some(context) = self
            .namespace_execution
            .context_mut()
            .filter(|context| context.file_system_id == file_system_id)
        {
            context.placement_config = Some(config.clone());
        }
        Ok(config)
    }

    pub fn resolved_placement_policy_for_path(
        &mut self,
        path: &str,
        object_policy: DfsObjectPolicy,
        legacy_desired_replicas: usize,
    ) -> Result<DfsResolvedPlacementPolicy, DfsError> {
        let config = self.current_placement_file_system_config()?;
        resolve_placement_policy(&config, path, object_policy, legacy_desired_replicas)
            .map_err(DfsError::from)
    }

    pub fn placement_content_candidates(&mut self) -> Result<Vec<DfsNodeAdvertisement>, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(vec![DfsNodeAdvertisement::content(self.node_id.clone())]);
        };
        let mut candidates = DfsControlPlaneStore::new(control_plane.as_mut())
            .cluster_view()?
            .content_nodes;
        if !candidates
            .iter()
            .any(|candidate| candidate.node_id == self.node_id)
        {
            candidates.push(DfsNodeAdvertisement::content(self.node_id.clone()));
        }
        candidates.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        candidates.dedup_by(|left, right| left.node_id == right.node_id);
        Ok(candidates)
    }

    /// Refresh this node's compact content advertisement only when its
    /// hysteretic semantics changed or its lease is near expiry.
    pub fn publish_local_content_advertisement_if_needed(
        &mut self,
        desired: DfsNodeAdvertisement,
    ) -> Result<bool, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(false);
        };
        publish_dfs_content_advertisement_if_needed(control_plane.as_mut(), &self.node_id, desired)
    }

    /// Preview one placement proposal and replace the single-object impact
    /// defaults with authoritative totals for the requested scope. Callers that
    /// already have a stat can pass its immutable identity and location fields;
    /// candidate discovery and the aggregate metadata scan stay inside DFS.
    #[allow(clippy::too_many_arguments)]
    pub fn preview_placement_with_impact(
        &mut self,
        config: &DfsFileSystemConfig,
        path: &str,
        object_id: DfsObjectId,
        version: DfsVersion,
        object_policy: DfsObjectPolicy,
        current_replica_node_ids: Vec<NodeId>,
        override_policy: Option<DfsPlacementPolicy>,
        scope: DfsPlacementScope,
    ) -> Result<DfsPlacementPreview, DfsError> {
        self.preview_placement_with_impact_and_unavailable_nodes(
            config,
            path,
            object_id,
            version,
            object_policy,
            current_replica_node_ids,
            override_policy,
            scope,
            &BTreeSet::new(),
        )
    }

    /// The reachability-aware form of [`Self::preview_placement_with_impact`].
    /// Explicitly unavailable nodes remain visible in the node projection but
    /// are excluded from every object plan and aggregate feasibility check.
    #[allow(clippy::too_many_arguments)]
    pub fn preview_placement_with_impact_and_unavailable_nodes(
        &mut self,
        config: &DfsFileSystemConfig,
        path: &str,
        object_id: DfsObjectId,
        version: DfsVersion,
        object_policy: DfsObjectPolicy,
        current_replica_node_ids: Vec<NodeId>,
        override_policy: Option<DfsPlacementPolicy>,
        scope: DfsPlacementScope,
        unavailable_node_ids: &BTreeSet<NodeId>,
    ) -> Result<DfsPlacementPreview, DfsError> {
        let preview_config =
            placement_config_with_override(config, path, scope, override_policy.clone())
                .map_err(DfsError::from)?;
        let mut fence_locations = current_replica_node_ids.clone();
        fence_locations.sort();
        fence_locations.dedup();
        let object_fence = DfsPlacementPreviewObjectFence {
            file_system_id: config.file_system_id.clone(),
            object_id: object_id.clone(),
            path: normalize_posix_path(path).map_err(|error| DfsError::Distributed {
                message: error.to_string(),
            })?,
            kind: None,
            version: version.clone(),
            content_hash: None,
            size_bytes: None,
            changed_at_unix_nanos: 0,
            policy: object_policy.clone(),
            current_locations: fence_locations,
        };
        let candidates = self.placement_content_candidates()?;
        let mut preview = preview_placement_with_unavailable_nodes(
            config,
            path,
            object_id,
            version,
            object_policy,
            self.node_id.clone(),
            current_replica_node_ids,
            candidates.clone(),
            override_policy,
            scope,
            unavailable_node_ids,
        )
        .map_err(DfsError::from)?;
        let impact = self.placement_impact_scan_for_config(
            &preview_config,
            path,
            scope,
            &candidates,
            unavailable_node_ids,
        )?;
        preview.impact = impact.impact;
        extend_preview_diagnostics(&mut preview, impact.blocking_diagnostics);
        if !preview.blocking_diagnostics.is_empty() {
            preview.impact.complete = false;
        }
        bind_placement_preview_token(
            &mut preview,
            config,
            &preview_config,
            path,
            scope,
            &object_fence,
            &candidates,
            unavailable_node_ids,
        )?;
        Ok(preview)
    }

    /// Compute bounded-memory aggregate impact for an already-materialized
    /// placement configuration. A matching non-tombstone object contributes to
    /// the affected totals only when its durable consistency or replica set
    /// would change; copy totals mirror the reconciliation plan. Use the paged
    /// form when the caller also needs structured diagnostics explaining an
    /// incomplete total.
    pub fn placement_impact_for_config(
        &self,
        config: &DfsFileSystemConfig,
        path: &str,
        scope: DfsPlacementScope,
        candidates: &[DfsNodeAdvertisement],
    ) -> Result<DfsPlacementImpact, DfsError> {
        self.placement_impact_for_config_with_unavailable_nodes(
            config,
            path,
            scope,
            candidates,
            &BTreeSet::new(),
        )
    }

    /// Whole-scan convenience API for callers that already have a stable
    /// candidate/reachability snapshot.
    pub fn placement_impact_for_config_with_unavailable_nodes(
        &self,
        config: &DfsFileSystemConfig,
        path: &str,
        scope: DfsPlacementScope,
        candidates: &[DfsNodeAdvertisement],
        unavailable_node_ids: &BTreeSet<NodeId>,
    ) -> Result<DfsPlacementImpact, DfsError> {
        Ok(self
            .placement_impact_scan_for_config(
                config,
                path,
                scope,
                candidates,
                unavailable_node_ids,
            )?
            .impact)
    }

    fn placement_impact_scan_for_config(
        &self,
        config: &DfsFileSystemConfig,
        path: &str,
        scope: DfsPlacementScope,
        candidates: &[DfsNodeAdvertisement],
        unavailable_node_ids: &BTreeSet<NodeId>,
    ) -> Result<DfsPlacementImpactPage, DfsError> {
        let mut aggregate = DfsPlacementImpactPage {
            impact: DfsPlacementImpact::default(),
            blocking_diagnostics: Vec::new(),
            next_cursor: None,
        };
        let mut cursor = None;
        loop {
            let page = self.placement_impact_page_for_config_with_unavailable_nodes(
                config,
                path,
                scope,
                candidates,
                cursor.as_ref(),
                PLACEMENT_IMPACT_PAGE_SIZE,
                unavailable_node_ids,
            )?;
            merge_placement_impact(&mut aggregate.impact, &page.impact);
            extend_unique_diagnostics(
                &mut aggregate.blocking_diagnostics,
                page.blocking_diagnostics,
            );
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(aggregate)
    }

    /// Analyze at most one metadata page. This is the lock-friendly primitive
    /// for daemon/CLI callers: retain the candidate snapshot, release the DFS
    /// service between calls, and resume from `next_cursor`.
    #[allow(clippy::too_many_arguments)]
    pub fn placement_impact_page_for_config(
        &self,
        config: &DfsFileSystemConfig,
        path: &str,
        scope: DfsPlacementScope,
        candidates: &[DfsNodeAdvertisement],
        cursor: Option<&DfsObjectPageCursor>,
        max_objects: usize,
    ) -> Result<DfsPlacementImpactPage, DfsError> {
        self.placement_impact_page_for_config_with_unavailable_nodes(
            config,
            path,
            scope,
            candidates,
            cursor,
            max_objects,
            &BTreeSet::new(),
        )
    }

    /// Reachability-aware one-page impact scan.
    #[allow(clippy::too_many_arguments)]
    pub fn placement_impact_page_for_config_with_unavailable_nodes(
        &self,
        config: &DfsFileSystemConfig,
        path: &str,
        scope: DfsPlacementScope,
        candidates: &[DfsNodeAdvertisement],
        cursor: Option<&DfsObjectPageCursor>,
        max_objects: usize,
        unavailable_node_ids: &BTreeSet<NodeId>,
    ) -> Result<DfsPlacementImpactPage, DfsError> {
        self.placement_impact_page_for_config_with_snapshot_and_unavailable_nodes(
            config,
            path,
            scope,
            candidates,
            cursor,
            max_objects,
            unavailable_node_ids,
            None,
        )
    }

    /// Analyze the same bounded page using the exact snapshot selected for
    /// Apply and its durable reconciliation job. Metadata and actual per-object
    /// decisions share one digest; diagnostics alone never grant authorization.
    #[allow(clippy::too_many_arguments)]
    pub fn placement_impact_page_for_config_with_snapshot_and_unavailable_nodes(
        &self,
        config: &DfsFileSystemConfig,
        path: &str,
        scope: DfsPlacementScope,
        candidates: &[DfsNodeAdvertisement],
        cursor: Option<&DfsObjectPageCursor>,
        max_objects: usize,
        unavailable_node_ids: &BTreeSet<NodeId>,
        snapshot: Option<&crate::DfsPlacementSnapshot>,
    ) -> Result<DfsPlacementImpactPage, DfsError> {
        let path = normalize_posix_path(path).map_err(|error| DfsError::Distributed {
            message: error.to_string(),
        })?;
        let mut impact = DfsPlacementImpact::default();
        let mut blocking_diagnostics = Vec::new();
        let page = self.placement_object_page_for_file_system(
            &config.file_system_id,
            cursor,
            max_objects,
        )?;
        let mut next_cursor = page.next_cursor;
        let mut metadata_snapshot_xor = [0_u8; 32];
        let mut metadata_snapshot_has_objects = false;
        for object in page.objects {
            if object.kind == DfsObjectKind::Tombstone
                || !placement_scope_matches(scope, &path, &object.path)
            {
                continue;
            }

            let mut pinned_node_ids = self.content_pin_nodes(&object.object_id);
            pinned_node_ids.sort();
            pinned_node_ids.dedup();
            let analysis = analyze_object_placement(
                config,
                &object,
                candidates,
                unavailable_node_ids,
                snapshot,
                &pinned_node_ids,
            )
            .map_err(DfsError::from)?;
            let object_digest =
                placement_metadata_object_digest(&object, &analysis, &pinned_node_ids)?;
            for (aggregate, byte) in metadata_snapshot_xor.iter_mut().zip(object_digest) {
                *aggregate ^= byte;
            }
            metadata_snapshot_has_objects = true;
            if !analysis.blocking_diagnostics.is_empty() {
                impact.complete = false;
                let (diagnostic_path, diagnostic_scope) =
                    resolved_diagnostic_scope(&analysis.resolved, &object.path);
                extend_unique_diagnostics(
                    &mut blocking_diagnostics,
                    scoped_diagnostics(
                        &diagnostic_path,
                        diagnostic_scope,
                        analysis.blocking_diagnostics,
                    ),
                );
            }
            let content_bearing = matches!(
                object.kind,
                DfsObjectKind::File | DfsObjectKind::Symlink | DfsObjectKind::Artifact
            ) && object.content_hash.is_some();
            let (copies_added, copies_removed) = if content_bearing {
                let desired = analysis
                    .placement
                    .plan
                    .desired_replica_nodes
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                let current = object
                    .current_locations
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>();
                (
                    desired
                        .difference(&current)
                        .count()
                        .try_into()
                        .unwrap_or(u64::MAX),
                    current
                        .difference(&desired)
                        .count()
                        .try_into()
                        .unwrap_or(u64::MAX),
                )
            } else {
                (0, 0)
            };
            let affected = object.policy != analysis.resolved.policy.consistency
                || copies_added > 0
                || copies_removed > 0;
            if affected {
                impact.affected_object_count = impact.affected_object_count.saturating_add(1);
                if content_bearing {
                    match object.content_size_bytes {
                        Some(bytes) => {
                            impact.affected_bytes = impact.affected_bytes.saturating_add(bytes);
                        }
                        None => {
                            impact.unknown_size_object_count =
                                impact.unknown_size_object_count.saturating_add(1);
                        }
                    }
                }
                impact.copies_added = impact.copies_added.saturating_add(copies_added);
                impact.copies_removed = impact.copies_removed.saturating_add(copies_removed);
            }
            if scope == DfsPlacementScope::SelfOnly {
                next_cursor = None;
                break;
            }
        }
        if metadata_snapshot_has_objects {
            impact.metadata_snapshot_digest =
                placement_metadata_snapshot_digest(metadata_snapshot_xor);
        }
        Ok(DfsPlacementImpactPage {
            impact,
            blocking_diagnostics,
            next_cursor,
        })
    }

    pub fn managed_replication_policy_for_path(
        &mut self,
        path: &str,
        object_policy: DfsObjectPolicy,
        legacy_desired_replicas: usize,
    ) -> Result<
        (
            DfsResolvedPlacementPolicy,
            Vec<DfsNodeAdvertisement>,
            DfsReplicationPolicy,
        ),
        DfsError,
    > {
        let resolved =
            self.resolved_placement_policy_for_path(path, object_policy, legacy_desired_replicas)?;
        let candidates = self.placement_content_candidates()?;
        let node_zones = candidates
            .iter()
            .filter_map(|candidate| {
                candidate
                    .zone
                    .clone()
                    .map(|zone| (candidate.node_id.clone(), zone))
            })
            .collect();
        let replication = resolved.policy.replication_policy(node_zones)?;
        Ok((resolved, candidates, replication))
    }

    /// Policy-only counterpart for write options. Keep the full zone map even
    /// when no minimum is configured: that map participates in durable operation
    /// identity, and this lookup must not rewrite an older operation's policy.
    pub(crate) fn managed_write_replication_policy_for_path(
        &mut self,
        path: &str,
        object_policy: DfsObjectPolicy,
        legacy_desired_replicas: usize,
    ) -> Result<DfsReplicationPolicy, DfsError> {
        let configuration = self
            .control_plane
            .as_deref()
            .map(|control| control.write_configuration_projection())
            .transpose()?
            .flatten();
        self.managed_write_replication_policy_from_configuration(
            path,
            object_policy,
            legacy_desired_replicas,
            configuration.as_deref(),
        )
    }

    pub(crate) fn managed_write_replication_policy_from_configuration(
        &mut self,
        path: &str,
        object_policy: DfsObjectPolicy,
        legacy_desired_replicas: usize,
        configuration: Option<&crate::DfsWriteConfigurationProjection>,
    ) -> Result<DfsReplicationPolicy, DfsError> {
        let trace_started = crate::dfs_perf_trace_enabled().then(Instant::now);
        let resolved = match configuration {
            Some(configuration) => {
                let file_system_id = crate::sifs_file_system_id();
                let config = configuration
                    .file_system_config(&file_system_id)?
                    .or_else(|| {
                        self.metadata
                            .filesystems
                            .get(&file_system_id)
                            .map(DfsFileSystemConfig::from)
                    })
                    .ok_or_else(|| DfsError::UnknownFileSystem { file_system_id })?;
                resolve_placement_policy(&config, path, object_policy, legacy_desired_replicas)?
            }
            None => self.resolved_placement_policy_for_path(
                path,
                object_policy,
                legacy_desired_replicas,
            )?,
        };
        let filesystem_policy_us = trace_started.map_or(0, |start| start.elapsed().as_micros());
        let zone_started = trace_started.map(|_| Instant::now());
        let mut candidates = match configuration {
            Some(configuration) => configuration.content_advertisements()?,
            None => match self.control_plane.as_mut() {
                Some(control_plane) => {
                    DfsControlPlaneStore::new(control_plane.as_mut()).content_services()?
                }
                None => Vec::new(),
            },
        };
        if !candidates
            .iter()
            .any(|candidate| candidate.node_id == self.node_id)
        {
            candidates.push(DfsNodeAdvertisement::content(self.node_id.clone()));
        }
        // Match placement_content_candidates exactly, including duplicate order
        // and the local fallback, while avoiding unrelated metadata advertisements.
        candidates.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        candidates.dedup_by(|left, right| left.node_id == right.node_id);
        let node_zones = candidates
            .into_iter()
            .filter_map(|candidate| candidate.zone.map(|zone| (candidate.node_id, zone)))
            .collect();
        let zone_read_us = zone_started.map_or(0, |start| start.elapsed().as_micros());
        let conversion_started = trace_started.map(|_| Instant::now());
        let policy = resolved.policy.replication_policy(node_zones)?;
        if let Some(started) = trace_started {
            crate::dfs_perf_trace(format_args!(
                "dfs-write-policy node={} path={} filesystem_policy_us={} zone_read_us={} policy_conversion_us={} zone_required={} total_us={}",
                self.node_id,
                path,
                filesystem_policy_us,
                zone_read_us,
                conversion_started.map_or(0, |start| start.elapsed().as_micros()),
                policy.min_distinct_zones > 0,
                started.elapsed().as_micros(),
            ));
        }
        Ok(policy)
    }

    pub(crate) fn apply_managed_policy_to_new_object(
        &mut self,
        object: &mut DfsObjectMetadata,
        legacy_desired_replicas: usize,
    ) -> Result<(), DfsError> {
        let resolved = self.resolved_placement_policy_for_path(
            &object.path,
            object.policy.clone(),
            legacy_desired_replicas,
        )?;
        object.policy = resolved.policy.consistency;
        Ok(())
    }

    pub(crate) fn reconcile_object_placement_local(
        &mut self,
        job_id: &str,
        path: &str,
        object_id: &DfsObjectId,
        expected_version: &DfsVersion,
        object_policy: DfsObjectPolicy,
        replication_policy: DfsReplicationPolicy,
        desired_replica_node_ids: &[NodeId],
    ) -> Result<(), DfsError> {
        self.with_authoritative_placement_reconciliation(
            &crate::sifs_file_system_id(),
            job_id,
            Some(path),
            1,
            |service, job, accepted_config| {
                service.reconcile_object_placement_under_authority(
                    job,
                    accepted_config,
                    path,
                    object_id,
                    expected_version,
                    object_policy,
                    replication_policy,
                    desired_replica_node_ids,
                    None,
                )
            },
        )?;
        Ok(())
    }

    /// sinomo:kv-consistency: Inspect bounded placement decisions during repair recovery; each replacement revalidates the current configuration and acquires its existing mutation permit.
    pub(crate) fn placement_plans_for_repair(
        &mut self,
    ) -> Result<Vec<DfsPlacementReconciliationJob>, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(Vec::new());
        };
        let store = DfsControlPlaneStore::new(control_plane.as_mut());
        let fs = crate::sifs_file_system_id();
        let Some(config) = store.get_file_system_config(&fs)? else {
            return Ok(Vec::new());
        };
        let page = store.placement_reconciliation_activity(
            &fs,
            &[
                crate::DfsPlacementReconciliationActivityStatus::Completed,
                crate::DfsPlacementReconciliationActivityStatus::Running,
                crate::DfsPlacementReconciliationActivityStatus::Retrying,
            ],
            None,
            None,
            MAX_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE,
        )?;
        let mut plans = Vec::new();
        for activity in page.activities {
            let Some(job) = store.placement_reconciliation(&fs, &activity.job_id)? else {
                continue;
            };
            if job.state != DfsPlacementReconciliationState::Failed
                && job.supports_snapshot_reconciliation()
                && job.accepted_config.as_ref().is_some_and(|accepted| {
                    placement_config_remains_authoritative_for_job(accepted, &config, &job)
                })
            {
                let covers_filesystem = job.path == "/" && job.scope == DfsPlacementScope::Subtree;
                plans.push(job);
                if covers_filesystem {
                    break;
                }
            }
        }
        // Ordering selects a candidate, not authority. The exact record and
        // configuration are checked again by the existing permit transaction.
        plans.sort_by(|a, b| {
            b.created_at_unix_nanos
                .cmp(&a.created_at_unix_nanos)
                .then_with(|| b.job_id.cmp(&a.job_id))
        });
        Ok(plans)
    }

    pub(crate) fn repair_may_need_placement_recovery(
        &self,
        repair: &crate::DistributedRepairJob,
        observed_config: Option<&DfsFileSystemConfig>,
    ) -> Result<bool, DfsError> {
        if self.control_plane.is_none() {
            return Ok(false);
        }
        let Some(intent) = self.coordinator.intent(&repair.intent_id) else {
            return Ok(false);
        };
        if !intent.publication.is_current() {
            return Ok(false);
        }
        if placement_reconciliation_job_id_from_intent_id(&repair.intent_id).is_some() {
            return Ok(true);
        }
        let object = self.current_read_object_metadata(&repair.object_id)?;
        // Managed placement applies only to the absolute POSIX namespace.
        // Legacy relative object paths have no matching placement job.
        if !object.path.starts_with('/') || object.version != repair.version {
            return Ok(false);
        }
        if let Some(config) =
            observed_config.filter(|config| config.file_system_id == object.file_system_id)
        {
            let resolved = resolve_placement_policy(
                config,
                &object.path,
                object.policy.clone(),
                intent.policy.desired_replicas,
            )?;
            if resolved.policy.min_commit_replicas != intent.policy.min_commit_replicas
                || resolved.policy.desired_replicas != intent.policy.desired_replicas
            {
                return Ok(true);
            }
        }
        self.metadata_backend
            .as_ref()
            .map(|backend| backend.has_current_placement_repair_owner(&object))
            .transpose()
            .map(|owner| owner.unwrap_or(false))
            .map_err(DfsError::from)
    }

    pub(crate) fn superseding_repair_plan<'a>(
        &self,
        repair: &crate::DistributedRepairJob,
        plans: &'a [DfsPlacementReconciliationJob],
    ) -> Result<Option<(DfsObjectMetadata, &'a DfsPlacementReconciliationJob)>, DfsError> {
        let Some(intent) = self.coordinator.intent(&repair.intent_id) else {
            return Ok(None);
        };
        if !intent.publication.is_current() {
            return Ok(None);
        }
        let object = self.current_read_object_metadata(&repair.object_id)?;
        if object.version != repair.version {
            return Ok(None);
        }
        let Some(plan) = plans.iter().find(|plan| {
            plan.file_system_id == object.file_system_id
                && placement_job_matches(plan, &object.path)
        }) else {
            metrics::counter!("sinomo.dfs.background.items", "worker" => "repair", "kind" => "replacement_plan_unavailable").increment(1);
            return Ok(None);
        };
        if plan.state != DfsPlacementReconciliationState::Completed {
            metrics::counter!("sinomo.dfs.background.items", "worker" => "repair", "kind" => "replacement_plan_active").increment(1);
            // An accepted running plan owns its transition and progress.
            // Recovery must not replay an older completed plan over it.
            return Ok(None);
        }
        let original_placement = placement_reconciliation_job_id_from_intent_id(&repair.intent_id);
        if original_placement == Some(plan.job_id.as_str()) {
            metrics::counter!("sinomo.dfs.background.items", "worker" => "repair", "kind" => "replacement_owns_current").increment(1);
            return Ok(None);
        }
        let (_, _, replacement) = reconciliation_placement_for_object_with_pins(
            plan.accepted_config
                .as_ref()
                .expect("validated placement plan"),
            plan,
            &object,
            &self.content_pin_nodes(&object.object_id),
        )?;
        if original_placement.is_none()
            && replacement
                .plan
                .desired_replica_nodes
                .contains(&repair.missing_replica_node_id)
        {
            metrics::counter!("sinomo.dfs.background.items", "worker" => "repair", "kind" => "replacement_target_current").increment(1);
            return Ok(None);
        }
        Ok(Some((object, plan)))
    }

    /// Retire a bounded group under the existing job-scoped permit. Each
    /// object's mutation still validates its version, pins and accepted plan.
    /// The permit is released before the repair executor sends any blob RPC.
    pub(crate) fn replace_superseded_repair_obligations(
        &mut self,
        queued: &[crate::DistributedRepairJob],
        plans: &[DfsPlacementReconciliationJob],
        limit: usize,
        observed_config: Option<&DfsFileSystemConfig>,
        allowed: impl Fn(&crate::DistributedRepairJob) -> bool,
    ) -> Result<usize, DfsError> {
        self.replace_superseded_repair_obligations_recording(
            queued,
            plans,
            limit,
            observed_config,
            allowed,
            &mut Vec::new(),
        )
    }

    pub(crate) fn replace_superseded_repair_obligations_recording(
        &mut self,
        queued: &[crate::DistributedRepairJob],
        plans: &[DfsPlacementReconciliationJob],
        limit: usize,
        observed_config: Option<&DfsFileSystemConfig>,
        allowed: impl Fn(&crate::DistributedRepairJob) -> bool,
        retired_records: &mut Vec<crate::DfsRepairAttemptRecord>,
    ) -> Result<usize, DfsError> {
        let Some(first) = queued.first().filter(|first| limit > 0 && allowed(first)) else {
            return Ok(0);
        };
        let Some((object, plan)) = self.superseding_repair_plan(first, plans)? else {
            return Ok(0);
        };
        let file_system_id = object.file_system_id.clone();
        let first_path = object.path.clone();
        let mut seen = BTreeSet::from([(object.object_id.clone(), object.version.clone())]);
        let mut batch = vec![(first.intent_id.clone(), object)];
        // Sparse or interleaved jobs must not turn one bounded pass into a
        // second scan of the entire recovery queue.
        for repair in queued.iter().skip(1).take(limit.saturating_mul(4).min(128)) {
            if batch.len() >= limit {
                break;
            }
            if !allowed(repair)
                || seen.contains(&(repair.object_id.clone(), repair.version.clone()))
                || !self.repair_retry_is_due(&repair.intent_id, &repair.missing_replica_node_id)
            {
                continue;
            }
            self.restore_pending_write_for_repair_job(repair)?;
            let Some(pending) = self.writes.pending_writes.get(&repair.intent_id).cloned() else {
                continue;
            };
            if !self.pending_repair_is_locally_authoritative(&pending)?
                || !self.pending_repair_targets_current_content(repair, &pending)?
                || !self.repair_may_need_placement_recovery(repair, observed_config)?
            {
                continue;
            }
            let Some((object, selected)) = self.superseding_repair_plan(repair, plans)? else {
                continue;
            };
            if selected.job_id != plan.job_id || object.file_system_id != file_system_id {
                continue;
            }
            seen.insert((object.object_id.clone(), object.version.clone()));
            batch.push((repair.intent_id.clone(), object));
        }
        let replaced = self
            .with_authoritative_placement_reconciliation(
                &file_system_id,
                &plan.job_id,
                Some(&first_path),
                batch.len(),
                |service, current_plan, accepted_config| {
                    if current_plan.state != DfsPlacementReconciliationState::Completed {
                        return Ok(0);
                    }
                    let mut replaced = 0;
                    for (old_intent_id, object) in batch {
                        // The permit covers the job, not just its first path.
                        if !placement_job_matches(current_plan, &object.path) {
                            continue;
                        }
                        let (resolved, replication, placement) =
                            reconciliation_placement_for_object_with_pins(
                                accepted_config,
                                current_plan,
                                &object,
                                &service.content_pin_nodes(&object.object_id),
                            )?;
                        let retirement_identity =
                            queued
                                .iter()
                                .find(|repair| repair.intent_id == old_intent_id)
                                .and_then(|repair| {
                                    service.writes.pending_writes.get(&old_intent_id).map(
                                        |pending| {
                                            crate::service_repair::retired_repair_identity(
                                                repair, pending,
                                            )
                                        },
                                    )
                                });
                        service.reconcile_object_placement_under_authority(
                            current_plan,
                            accepted_config,
                            &object.path,
                            &object.object_id,
                            &object.version,
                            resolved.policy.consistency,
                            replication,
                            &placement.plan.desired_replica_nodes,
                            Some(&old_intent_id),
                        )?;
                        let retired = service
                            .metadata_backend
                            .as_ref()
                            .map(|backend| {
                                backend.get_intent_receipt(&object.object_id, &old_intent_id)
                            })
                            .transpose()?
                            .flatten()
                            .is_some();
                        if retired {
                            if let Some(record) = retirement_identity {
                                retired_records.push(record);
                            }
                            service.retire_committed_repair(&old_intent_id);
                            replaced += 1;
                        }
                    }
                    Ok(replaced)
                },
            )?
            .unwrap_or(0);
        if replaced > 0 {
            metrics::counter!("sinomo.dfs.background.items", "worker" => "repair", "kind" => "superseded_obligation").increment(replaced as u64);
        }
        Ok(replaced)
    }

    #[allow(clippy::too_many_arguments)]
    fn reconcile_object_placement_under_authority(
        &mut self,
        job: &DfsPlacementReconciliationJob,
        accepted_config: &DfsFileSystemConfig,
        path: &str,
        object_id: &DfsObjectId,
        expected_version: &DfsVersion,
        object_policy: DfsObjectPolicy,
        replication_policy: DfsReplicationPolicy,
        desired_replica_node_ids: &[NodeId],
        replaced_repair_intent: Option<&str>,
    ) -> Result<(), DfsError> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        validate_replication_plan(desired_replica_node_ids, &replication_policy)?;
        let mut object = self.current_read_object_metadata(object_id)?;
        if object.path != path || object.version != *expected_version {
            // A concurrent rename or write made this scan result stale. The
            // next reconciliation page/pass will see the authoritative state.
            return Ok(());
        }
        let pinned_node_ids = self.content_pin_nodes(object_id);
        let (expected_resolved, expected_replication, expected_placement) =
            reconciliation_placement_for_object_with_pins(
                accepted_config,
                job,
                &object,
                &pinned_node_ids,
            )?;
        if object_policy != expected_resolved.policy.consistency
            || replication_policy != expected_replication
            || desired_replica_node_ids != expected_placement.plan.desired_replica_nodes
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "placement reconciliation payload for {path} no longer matches authoritative job {}",
                    job.job_id
                ),
            });
        }
        if !self.is_local_metadata_primary_for_object(&object)? {
            return Err(DfsError::Distributed {
                message: format!(
                    "placement reconciliation for {} reached a non-primary node",
                    object.path
                ),
            });
        }
        let intent_id = format!(
            "placement:{}:{}:{}",
            job.job_id,
            object_id.as_str(),
            expected_version.as_str()
        );
        if self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.get_intent_receipt(object_id, &intent_id))
            .transpose()?
            .flatten()
            .is_some()
        {
            // A later placement has durably finished this exact operation.
            // Its retry receipt cannot regain current repair ownership.
            return Ok(());
        }
        if matches!(
            object_policy,
            DfsObjectPolicy::LocalOwned | DfsObjectPolicy::EphemeralLocal
        ) {
            // The exact current job/plan and local metadata authority were
            // checked above. A local-policy scan is observational: keep the
            // owner's copy and any existing read caches/pins, and never enter
            // generic replica creation or pruning. Missing owner knowledge is
            // not authority to substitute another node.
            if matches!(
                object.kind,
                DfsObjectKind::File | DfsObjectKind::Symlink | DfsObjectKind::Artifact
            ) && object.content_hash.is_some()
                && !object.current_locations.contains(&object.owner_node_id)
            {
                return Err(DfsError::Distributed {
                    message: "local placement requires its existing content owner; generic replica repair cannot restore that ownership"
                        .to_string(),
                });
            }
            return Ok(());
        }
        let policy_changed = object.policy != object_policy;
        if policy_changed {
            object.policy = object_policy;
            self.add_object_metadata(object.clone())?;
            self.record_message_committed_work(DfsMessageCommittedWork {
                publish_metadata: true,
                ..Default::default()
            });
        }
        if !matches!(
            object.kind,
            DfsObjectKind::File | DfsObjectKind::Symlink | DfsObjectKind::Artifact
        ) {
            return Ok(());
        }
        let Some(content_hash) = object.content_hash.clone() else {
            return Ok(());
        };
        let desired = desired_replica_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let current = object
            .current_locations
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let retained_nodes =
            if self.retained_file_content_is_live(object_id, expected_version, &content_hash)? {
                let backend = self
                    .metadata_backend
                    .as_ref()
                    .expect("retained version has metadata");
                let version = backend
                    .get_version(object_id, expected_version)?
                    .ok_or_else(|| DfsError::Metadata {
                        message: "retained placement version disappeared".into(),
                    })?;
                let durability = match version.content_durability.clone() {
                    Some(plan) => Some(plan),
                    None => {
                        crate::content_durability::recover_from_backend(&version, backend.as_ref())?
                    }
                }
                .ok_or_else(|| DfsError::Metadata {
                    message: "retained placement version has no durable repair plan".into(),
                })?;
                durability
                    .desired_replica_node_ids
                    .into_iter()
                    .collect::<BTreeSet<_>>()
            } else {
                BTreeSet::new()
            };
        let previous_placement_locations = current.difference(&desired).collect::<Vec<_>>();
        let replaces_current_locations = !previous_placement_locations.is_empty();
        let location_removals = previous_placement_locations
            .into_iter()
            .filter(|node_id| {
                !retained_nodes.contains(*node_id)
                    && !self
                        .content_pin_node_ids
                        .get(object_id)
                        .is_some_and(|pinned| pinned.contains(*node_id))
            })
            .cloned()
            .collect::<Vec<_>>();
        if desired.is_subset(&current)
            && !replaces_current_locations
            && !policy_changed
            && replaced_repair_intent.is_none()
        {
            // The owner received an observational placement check. There is
            // no location, policy or repair-ownership mutation to publish.
            return Ok(());
        }
        let owner_epoch = self.local_metadata_owner_epoch_for_object(&object)?;
        let intent = DistributedWriteIntent {
            publication: Default::default(),
            intent_id: intent_id.clone(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: Some(object.version.clone()),
            target_version: object.version.clone(),
            requesting_node_id: self.node_id.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch,
            desired_replica_nodes: desired_replica_node_ids.to_vec(),
            policy: replication_policy,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            state: DfsWriteIntentState::Committed,
        };
        let durable_receipts = desired_replica_node_ids
            .iter()
            .filter(|node_id| current.contains(*node_id))
            .cloned()
            .collect::<Vec<_>>();
        self.put_metadata_intent_and_receipts(&intent, &content_hash, &durable_receipts)?;
        // A placement change replaces the current repair obligation as well
        // as locations. Leaving the original full write executable recreates
        // its removed targets on restart. Preserve its exact retry identity
        // and independent retained-history owner using the existing ledger.
        if self.metadata_backend.is_some() {
            loop {
                let retired = self
                    .metadata_backend
                    .as_mut()
                    .unwrap()
                    .compact_replaced_placement_intents(&object, &intent_id, 128)?;
                if retired.is_empty() {
                    break;
                }
                self.record_message_committed_work(DfsMessageCommittedWork {
                    publish_metadata: true,
                    repair_maintenance: true,
                    ..Default::default()
                });
                let count = retired.len();
                for old_id in retired {
                    self.retire_committed_repair(&old_id);
                }
                if count < 128 {
                    break;
                }
            }
        }
        self.coordinator
            .restore_intent_deferring_repair_rebuild(intent)?;
        for node_id in durable_receipts {
            self.coordinator
                .record_durable_receipt(&intent_id, node_id)?;
        }
        if !retained_nodes.is_empty() {
            self.activate_file_history_repair_roots(BTreeSet::from([(
                object_id.clone(),
                expected_version.clone(),
            )]))?;
        }
        if desired.is_subset(&current) {
            for node_id in location_removals {
                self.mark_remote_read_location_missing(
                    object_id,
                    expected_version,
                    &Some(content_hash.clone()),
                    &node_id,
                )?;
            }
            self.cleanup_committed_pending_write_if_repaired(&intent_id);
            return Ok(());
        }
        // The exact current job, plan and local author were validated above.
        // Restore only its missing targets; broad hydration must not substitute
        // for the durable queue handoff, including an idempotent request replay.
        self.coordinator
            .set_committed_repair_active(&intent_id, true);
        self.restore_pending_write_for_intent_if_missing(&intent_id, object_id)?;
        if self.coordinator.capture_active_repair_jobs(&intent_id) {
            self.record_message_committed_work(DfsMessageCommittedWork {
                repair_maintenance: true,
                ..Default::default()
            });
        }
        Ok(())
    }

    fn observe_adaptive_placement_and_enqueue(
        &mut self,
        config: &DfsFileSystemConfig,
    ) -> Result<bool, DfsError> {
        let Some(snapshot) = self.placement_snapshot_cache.snapshot() else {
            return Ok(false);
        };
        let now = time::OffsetDateTime::now_utc();
        // Only the elected root metadata primary may turn local observations
        // into cluster-wide placement intent. Never move data from an expired
        // snapshot; expiry degrades source ranking to unknown instead.
        if snapshot.primary_node_id != self.node_id
            || snapshot.nodes.is_empty()
            || now > snapshot.expires_at
        {
            return Ok(false);
        }

        let digest = snapshot.semantic_digest();
        let now_nanos = dfs_now_unix_nanos();
        let current = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(false);
            };
            DfsControlPlaneStore::new(control_plane.as_mut())
                .adaptive_placement_observation(&config.file_system_id)?
        };
        if current
            .as_ref()
            .is_some_and(|state| state.last_enqueued_snapshot_digest == digest)
        {
            return Ok(false);
        }
        let (desired, sustained) = advance_adaptive_placement_observation(
            current.as_ref(),
            &digest,
            snapshot.generation,
            now_nanos,
        );
        let observation_updated = if current.as_ref() == Some(&desired) {
            true
        } else {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(false);
            };
            DfsControlPlaneStore::new(control_plane.as_mut())
                .compare_and_put_adaptive_placement_observation(
                    &config.file_system_id,
                    current.as_ref(),
                    &desired,
                )?
        };
        if !observation_updated || !sustained {
            return Ok(false);
        }

        let mut candidate_snapshot = self.placement_content_candidates()?;
        for node in &snapshot.nodes {
            if candidate_snapshot
                .iter()
                .any(|candidate| candidate.node_id == node.node_id)
            {
                continue;
            }
            candidate_snapshot.push(DfsNodeAdvertisement {
                node_id: node.node_id.clone(),
                capacity_weight: node.capacity_weight.max(1),
                accepts_metadata: false,
                accepts_content: node.accepts_new_content,
                zone: node.zone.clone(),
                storage_health: node.storage_health,
                storage_freshness: node.storage_freshness,
                usable_capacity_bytes: node.usable_capacity_bytes,
                reserved_free_bytes: node.reserved_free_bytes,
                content_location_count: node.content_location_count,
                network_metering: node.network_metering,
                resource_pressure: node.resource_pressure,
                advertisement_generation: snapshot.generation,
                updated_at: node.observed_at,
                expires_at: Some(node.expires_at),
            });
        }
        candidate_snapshot.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        candidate_snapshot.dedup_by(|left, right| left.node_id == right.node_id);

        let digest_segment = digest
            .strip_prefix("blake3:")
            .unwrap_or(&digest)
            .chars()
            .take(20)
            .collect::<String>();
        let job_id = format!("adaptive-{}-{digest_segment}", snapshot.generation);
        let budget = DfsAdaptivePlacementBudget::default();
        let job = DfsPlacementReconciliationJob {
            schema_version: DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
            job_id: job_id.clone(),
            file_system_id: config.file_system_id.clone(),
            path: "/".to_string(),
            scope: DfsPlacementScope::Subtree,
            unavailable_node_ids: BTreeSet::new(),
            candidate_snapshot,
            placement_snapshot: Some((*snapshot).clone()),
            adaptive: true,
            adaptive_budget: Some(budget),
            processed_content_bytes: 0,
            accepted_config: Some(config.clone()),
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
            created_at_unix_nanos: now_nanos,
            updated_at_unix_nanos: now_nanos,
            last_error: None,
        };
        let enqueued = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(false);
            };
            let mut store = DfsControlPlaneStore::new(control_plane.as_mut());
            if store
                .placement_reconciliation(&config.file_system_id, &job_id)?
                .is_some()
            {
                true
            } else {
                store.compare_and_put_placement_update(config, config, &job)?
            }
        };
        if !enqueued {
            return Ok(false);
        }

        let observed = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(true);
            };
            DfsControlPlaneStore::new(control_plane.as_mut())
                .adaptive_placement_observation(&config.file_system_id)?
        };
        if let Some(mut recorded) = observed.clone()
            && recorded.pending_snapshot_digest == digest
        {
            recorded.last_enqueued_snapshot_digest = digest;
            recorded.last_enqueued_snapshot_generation = snapshot.generation;
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(true);
            };
            let _ = DfsControlPlaneStore::new(control_plane.as_mut())
                .compare_and_put_adaptive_placement_observation(
                    &config.file_system_id,
                    observed.as_ref(),
                    &recorded,
                )?;
        }
        Ok(true)
    }

    /// Advance at most one durable placement job by a bounded object page.
    /// Progress is compare-and-swapped in cluster KV, so concurrent daemon
    /// ticks may help without losing a cursor or double-counting work.
    pub fn run_placement_reconciliation_bounded(
        &mut self,
        max_objects: usize,
    ) -> Result<Option<DfsPlacementReconciliationJob>, DfsError> {
        self.run_placement_reconciliation_bounded_with_eligibility(max_objects, None)
    }

    pub(crate) fn run_placement_reconciliation_bounded_with_pending_repairs(
        &mut self,
        max_objects: usize,
        pending_repair_count: usize,
    ) -> Result<Option<DfsPlacementReconciliationJob>, DfsError> {
        self.run_placement_reconciliation_bounded_with_eligibility(
            max_objects,
            Some(pending_repair_count),
        )
    }

    fn run_placement_reconciliation_bounded_with_eligibility(
        &mut self,
        max_objects: usize,
        pending_repair_count: Option<usize>,
    ) -> Result<Option<DfsPlacementReconciliationJob>, DfsError> {
        let file_system_id = crate::sifs_file_system_id();
        let (current_config, coordinator_node_id) = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(None);
            };
            let store = DfsControlPlaneStore::new(control_plane.as_mut());
            // A maintenance tick may run before the default filesystem has
            // been published to cluster KV. No managed placement job can be
            // accepted without that config, so placement is simply idle; it
            // must not prevent unrelated namespace or anti-entropy work.
            let Some(config) = store.get_file_system_config(&file_system_id)? else {
                if store.has_v1_placement_reconciliation_state(&file_system_id)? {
                    return Err(DfsError::Distributed {
                        message: format!(
                            "placement reconciliation state exists without filesystem config {file_system_id}"
                        ),
                    });
                }
                return Ok(None);
            };
            let coordinator_node_id = store
                .select_metadata_shard_for_path_strict_default_sifs_split(&file_system_id, "/")?
                .map(|shard| shard.primary_node_id);
            (config, coordinator_node_id)
        };
        // Advancing a job performs durable object-side effects before the
        // progress CAS. Without one deterministic runner, every daemon can
        // race the same revision and create the same repair intent under a
        // different coordinator. The filesystem root primary is a
        // failover-capable, cluster-wide job coordinator; using one global
        // primary also means a placement update can wake the same node that
        // owns the Files API in the default cluster. Individual objects are
        // still routed to their own metadata primaries below.
        if coordinator_node_id
            .as_ref()
            .is_some_and(|node_id| node_id != &self.node_id)
        {
            return Ok(None);
        }
        self.observe_adaptive_placement_and_enqueue(&current_config)?;
        let expected = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(None);
            };
            let mut store = DfsControlPlaneStore::new(control_plane.as_mut());
            let Some(job) = store.next_placement_reconciliation_with_eligibility(
                &file_system_id,
                &current_config,
                |job| {
                    pending_repair_count.is_none_or(|count| {
                        crate::maintenance::dfs_placement_reconciliation_job_can_advance(
                            job,
                            count,
                            Some(&current_config),
                        )
                    })
                },
            )?
            else {
                return Ok(None);
            };
            job
        };
        let mut desired = expected.clone();
        if expected.accepted_config.as_ref().is_some_and(|accepted| {
            !placement_config_remains_authoritative_for_job(accepted, &current_config, &expected)
        }) {
            mark_placement_reconciliation_superseded(&mut desired);
            desired.revision = desired.revision.saturating_add(1);
            desired.updated_at_unix_nanos = dfs_now_unix_nanos();
            let updated = {
                let Some(control_plane) = self.control_plane.as_mut() else {
                    return Ok(None);
                };
                DfsControlPlaneStore::new(control_plane.as_mut())
                    .compare_and_put_placement_reconciliation(&expected, &desired)?
            };
            return Ok(updated.then_some(desired));
        }
        let config = expected
            .accepted_config
            .clone()
            .expect("executable placement jobs have an accepted config snapshot");
        let page_authority = self.prepare_placement_page_authority(&current_config)?;
        let result = self.advance_placement_reconciliation_page(
            &config,
            &mut desired,
            max_objects.max(1),
            page_authority.as_ref(),
        );
        // Placement config and DFS object metadata live in separate durable
        // planes, so there is no safe crash-recoverable cross-plane lock in
        // this slice. Re-read after every bounded page: if an overlapping
        // config became authoritative during the page, terminalize the stale
        // job immediately. The newer durable job then converges any
        // idempotent stale-page effects instead of letting the oldest job
        // starve the queue indefinitely.
        let post_page_config = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(None);
            };
            DfsControlPlaneStore::new(control_plane.as_mut())
                .get_file_system_config(&file_system_id)?
                .ok_or_else(|| DfsError::UnknownFileSystem {
                    file_system_id: file_system_id.clone(),
                })?
        };
        let superseded_during_page = expected.accepted_config.as_ref().is_some_and(|accepted| {
            !placement_config_remains_authoritative_for_job(accepted, &post_page_config, &expected)
        });
        desired.revision = desired.revision.saturating_add(1);
        desired.updated_at_unix_nanos = dfs_now_unix_nanos();
        if superseded_during_page {
            mark_placement_reconciliation_superseded(&mut desired);
        } else {
            match result {
                Ok(()) => desired.last_error = None,
                Err(error) => {
                    if matches!(error, DfsError::InvalidReplicationPolicy { .. }) {
                        desired.state = DfsPlacementReconciliationState::Failed;
                        desired.cursor = None;
                    }
                    desired.last_error = Some(error.to_string());
                }
            }
        }
        let updated = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(None);
            };
            DfsControlPlaneStore::new(control_plane.as_mut())
                .compare_and_put_placement_reconciliation_with_guards(
                    &expected,
                    &desired,
                    if superseded_during_page {
                        Vec::new()
                    } else {
                        page_authority.map_or_else(Vec::new, |proof| proof.compares)
                    },
                )?
        };
        Ok(updated.then_some(desired))
    }

    #[tracing::instrument(target = "sinomo.perf", level = "trace", name = "dfs.placement.page",
        skip_all, fields(stage = "dfs.placement.page", job_id = %job.job_id, path = %job.path,
        phase = ?job.state, scanned_objects = tracing::field::Empty,
        content_bytes = tracing::field::Empty, outcome = "error"))]
    fn advance_placement_reconciliation_page(
        &mut self,
        config: &DfsFileSystemConfig,
        job: &mut DfsPlacementReconciliationJob,
        max_objects: usize,
        page_authority: Option<&PlacementPageAuthority>,
    ) -> Result<(), DfsError> {
        if job.state == DfsPlacementReconciliationState::Pending {
            // Discover the scope's size while processing it; a separate
            // filesystem inventory would duplicate work before every job.
            job.total_object_count = 0;
            job.state = DfsPlacementReconciliationState::Scanning;
            job.cursor = None;
        }
        let budget = placement_page_budget(job, max_objects);
        let started = Instant::now();
        let mut processed_objects = 0usize;
        let mut processed_bytes = 0u64;
        let mut pass_pending_repairs =
            if job.state == DfsPlacementReconciliationState::Repairing && job.cursor.is_none() {
                0
            } else {
                job.pending_repair_count
            };
        loop {
            if processed_objects >= budget.max_objects_per_tick {
                break;
            }
            if processed_objects > 0
                && (processed_bytes >= budget.max_bytes_per_tick
                    || started.elapsed() >= Duration::from_millis(budget.max_millis_per_tick))
            {
                break;
            }
            // Preserve the backend's exact continuation after each completed
            // object. A slow object may exceed the time budget once to ensure
            // progress, but another must not start after the budget is spent.
            let page = self.placement_object_page(job, 1)?;
            for object in &page.objects {
                processed_objects = processed_objects.saturating_add(1);
                if job.state == DfsPlacementReconciliationState::Scanning {
                    job.scanned_object_count = job.scanned_object_count.saturating_add(1);
                }
                if object.kind == DfsObjectKind::Tombstone
                    || !placement_job_matches(job, &object.path)
                {
                    continue;
                }
                if job.state == DfsPlacementReconciliationState::Scanning {
                    job.matched_object_count = job.matched_object_count.saturating_add(1);
                }
                let pinned_node_ids = self.content_pin_nodes(&object.object_id);
                let (resolved, replication, placement) =
                    reconciliation_placement_for_object_with_pins(
                        config,
                        job,
                        object,
                        &pinned_node_ids,
                    )?;
                let desired_nodes = placement.plan.desired_replica_nodes;
                let content_bearing = matches!(
                    object.kind,
                    DfsObjectKind::File | DfsObjectKind::Symlink | DfsObjectKind::Artifact
                ) && object.content_hash.is_some();
                let missing_before = if content_bearing {
                    desired_nodes
                        .iter()
                        .filter(|node_id| !object.current_locations.contains(node_id))
                        .count()
                } else {
                    0
                };
                let obsolete_before = if content_bearing {
                    object
                        .current_locations
                        .iter()
                        .filter(|node_id| !desired_nodes.contains(node_id))
                        .count()
                } else {
                    0
                };
                let object_transfer_bytes = object
                    .content_size_bytes
                    .unwrap_or_default()
                    .saturating_mul(u64::try_from(missing_before).unwrap_or(u64::MAX));
                processed_bytes = processed_bytes.saturating_add(object_transfer_bytes);
                job.processed_content_bytes = job
                    .processed_content_bytes
                    .saturating_add(object_transfer_bytes);
                let placement_change_needed = object.policy != resolved.policy.consistency
                    || missing_before > 0
                    || obsolete_before > 0;
                if job.state == DfsPlacementReconciliationState::Scanning && placement_change_needed
                {
                    job.updated_object_count = job.updated_object_count.saturating_add(1);
                    job.scheduled_repair_count =
                        job.scheduled_repair_count.saturating_add(missing_before);
                }
                let confirmed_noop = match self.placement_noop_check(
                    object,
                    placement_change_needed,
                    page_authority,
                )? {
                    PlacementNoopCheck::CertifiedLocal => true,
                    PlacementNoopCheck::LookupRemote(primary) => {
                        let objects = self.route_namespace_lookup_paths_to_metadata_primary(
                            object.file_system_id.clone(),
                            primary.primary_node_id,
                            vec![object.path.clone()],
                            crate::DFS_ROUTED_NAMESPACE_OP_TIMEOUT,
                        )?;
                        objects.as_slice() == std::slice::from_ref(object)
                    }
                    PlacementNoopCheck::RequiresReconciliation => false,
                };
                if !confirmed_noop {
                    self.route_placement_reconciliation_object(
                        job,
                        object,
                        resolved.policy.consistency,
                        replication,
                        desired_nodes.clone(),
                    )?;
                }
                if job.state == DfsPlacementReconciliationState::Repairing && content_bearing {
                    let current = self.current_read_object_metadata(&object.object_id)?;
                    pass_pending_repairs = pass_pending_repairs.saturating_add(
                        desired_nodes
                            .iter()
                            .filter(|node_id| !current.current_locations.contains(node_id))
                            .count(),
                    );
                }
            }
            job.cursor = page.next_cursor;
            if job.cursor.is_none() || page.objects.is_empty() {
                break;
            }
        }
        finish_placement_reconciliation_page(job, pass_pending_repairs);
        let span = tracing::Span::current();
        span.record("scanned_objects", processed_objects);
        span.record("content_bytes", processed_bytes);
        span.record("outcome", "ok");
        metrics::counter!("sinomo.dfs.background.items", "worker" => "placement", "kind" => "scanned")
            .increment(processed_objects as u64);
        Ok(())
    }

    /// One exact-key ReadIndex proof belongs to this bounded page. Every
    /// object is still reread, actual mutations keep their own effect fences,
    /// and the proof's comparisons must accompany the final progress CAS.
    /// sinomo:kv-consistency: Certify bounded placement-page configuration and shard authority with one shared ReadIndex operation.
    fn prepare_placement_page_authority(
        &mut self,
        current_config: &DfsFileSystemConfig,
    ) -> Result<Option<PlacementPageAuthority>, DfsError> {
        let deadline = self.transport.operation_deadline();
        let Some(kv) = self.control_plane.as_mut() else {
            return Ok(None);
        };
        let mut store = DfsControlPlaneStore::new(kv.as_mut());
        let shards = store.list_metadata_shards(&current_config.file_system_id)?;
        let Some(compares) =
            store.placement_page_authority_compares_before(current_config, &shards, deadline)?
        else {
            return Err(DfsError::Distributed { message: "placement page configuration or metadata authority changed before its bounded read".into() });
        };
        Ok(Some(PlacementPageAuthority {
            config: current_config.clone(),
            shards,
            compares,
        }))
    }

    /// A page proof certifies control state, never an unchanged object. Reread
    /// local metadata or use the ordinary authoritative remote lookup. Changed
    /// objects retain the existing placement mutation permit and effect fence.
    fn placement_noop_check(
        &mut self,
        object: &DfsObjectMetadata,
        placement_change_needed: bool,
        page_authority: Option<&PlacementPageAuthority>,
    ) -> Result<PlacementNoopCheck, DfsError> {
        if placement_change_needed {
            return Ok(PlacementNoopCheck::RequiresReconciliation);
        }
        let shard = self.select_metadata_shard_for_object(object)?;
        if let Some(proof) = page_authority {
            if proof.config.file_system_id != object.file_system_id
                || shard
                    .as_ref()
                    .is_none_or(|shard| !proof.shards.contains(shard))
            {
                return Ok(PlacementNoopCheck::RequiresReconciliation);
            }
            if let Some(shard) = shard.as_ref()
                && shard.primary_node_id == self.node_id
            {
                return Ok(
                    if self.current_read_object_metadata(&object.object_id)? == *object {
                        PlacementNoopCheck::CertifiedLocal
                    } else {
                        PlacementNoopCheck::RequiresReconciliation
                    },
                );
            }
        } else if self.is_local_metadata_primary_for_object(object)? {
            return Ok(
                if self
                    .certified_local_metadata_owner_shard_for_object(object)?
                    .is_some()
                    && self.current_read_object_metadata(&object.object_id)? == *object
                {
                    PlacementNoopCheck::CertifiedLocal
                } else {
                    PlacementNoopCheck::RequiresReconciliation
                },
            );
        }
        Ok(shard
            .filter(|shard| shard.primary_node_id != self.node_id)
            .map(PlacementNoopCheck::LookupRemote)
            .unwrap_or(PlacementNoopCheck::RequiresReconciliation))
    }

    fn placement_object_page(
        &self,
        job: &DfsPlacementReconciliationJob,
        max_objects: usize,
    ) -> Result<DfsObjectPage, DfsError> {
        if let Some(backend) = self.metadata_backend.as_ref() {
            return match job.scope {
                DfsPlacementScope::SelfOnly => Ok(DfsObjectPage {
                    objects: if max_objects == 0 {
                        Vec::new()
                    } else {
                        backend
                            .get_object_by_path(&job.file_system_id, &job.path)?
                            .into_iter()
                            .collect()
                    },
                    next_cursor: None,
                }),
                DfsPlacementScope::Subtree => backend
                    .objects_for_subtree_page(
                        &job.file_system_id,
                        &job.path,
                        job.cursor.as_ref(),
                        max_objects,
                    )
                    .map_err(DfsError::from),
            };
        }
        // The backend-free test/embedded service has no durable path index.
        // Retain its bounded cursor semantics while selecting only this scope.
        let mut objects = self
            .metadata
            .objects
            .values()
            .filter(|object| {
                object.file_system_id == job.file_system_id
                    && placement_job_matches(job, &object.path)
            })
            .filter(|object| {
                job.cursor.as_ref().is_none_or(|after| {
                    (&object.path, &object.object_id) > (&after.path, &after.object_id)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        objects.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        let next_cursor = (max_objects > 0 && objects.len() > max_objects).then(|| {
            let object = &objects[max_objects - 1];
            DfsObjectPageCursor {
                path: object.path.clone(),
                object_id: object.object_id.clone(),
                shard_id: None,
            }
        });
        objects.truncate(max_objects);
        Ok(DfsObjectPage {
            objects,
            next_cursor,
        })
    }

    fn placement_object_page_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
        cursor: Option<&DfsObjectPageCursor>,
        max_objects: usize,
    ) -> Result<DfsObjectPage, DfsError> {
        if let Some(backend) = self.metadata_backend.as_ref() {
            return backend
                .objects_for_file_system_page(file_system_id, cursor, max_objects)
                .map_err(DfsError::from);
        }
        if max_objects == 0 {
            return Ok(DfsObjectPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }

        // The in-memory store is keyed by object id rather than path. Keep only
        // the smallest page plus one lookahead while scanning so preview and
        // reconciliation memory remain bounded by the requested page size.
        let mut page_objects = Vec::with_capacity(max_objects.saturating_add(1));
        for object in self.metadata.objects.values().filter(|object| {
            object.file_system_id == *file_system_id
                && cursor.is_none_or(|cursor| {
                    object.path > cursor.path
                        || (object.path == cursor.path && object.object_id > cursor.object_id)
                })
        }) {
            let index = page_objects
                .binary_search_by(|candidate: &DfsObjectMetadata| {
                    candidate
                        .path
                        .cmp(&object.path)
                        .then_with(|| candidate.object_id.cmp(&object.object_id))
                })
                .unwrap_or_else(|index| index);
            if index <= max_objects {
                page_objects.insert(index, object.clone());
                if page_objects.len() > max_objects.saturating_add(1) {
                    page_objects.pop();
                }
            }
        }
        let has_more = page_objects.len() > max_objects;
        page_objects.truncate(max_objects);
        let next_cursor = has_more.then(|| {
            let last = page_objects.last().expect("non-empty bounded page");
            DfsObjectPageCursor {
                path: last.path.clone(),
                object_id: last.object_id.clone(),
                shard_id: None,
            }
        });
        Ok(DfsObjectPage {
            objects: page_objects,
            next_cursor,
        })
    }

    fn route_placement_reconciliation_object(
        &mut self,
        job: &DfsPlacementReconciliationJob,
        object: &DfsObjectMetadata,
        object_policy: DfsObjectPolicy,
        replication_policy: DfsReplicationPolicy,
        desired_replica_node_ids: Vec<NodeId>,
    ) -> Result<(), DfsError> {
        let operation = DfsNamespaceOperation::ReconcilePlacement {
            job_id: job.job_id.clone(),
            path: object.path.clone(),
            object_id: object.object_id.clone(),
            expected_version: object.version.clone(),
            object_policy,
            replication_policy,
            desired_replica_node_ids,
        };
        let shard = self.select_metadata_shard_for_object(object)?;
        if let Some(shard) = shard
            && shard.primary_node_id != self.node_id
        {
            return self
                .route_namespace_operation(shard.primary_node_id, operation)
                .map_err(|error| DfsError::Distributed {
                    message: error.to_string(),
                });
        }
        self.execute_namespace_operation_local(&operation)
            .map_err(|error| DfsError::Distributed {
                message: error.to_string(),
            })
    }
}

fn mark_placement_reconciliation_superseded(job: &mut DfsPlacementReconciliationJob) {
    // Keep the wire enum backward-compatible for mixed-version clients:
    // `Failed` is terminal in every schema version, while `last_error`
    // distinguishes supersession from an execution failure.
    job.state = DfsPlacementReconciliationState::Failed;
    job.cursor = None;
    job.last_error = Some(PLACEMENT_RECONCILIATION_SUPERSEDED_ERROR.to_string());
}

#[cfg(test)]
fn reconciliation_placement_for_object(
    config: &DfsFileSystemConfig,
    job: &DfsPlacementReconciliationJob,
    object: &DfsObjectMetadata,
) -> Result<
    (
        DfsResolvedPlacementPolicy,
        DfsReplicationPolicy,
        DfsReplicaPlacementExplanation,
    ),
    DfsError,
> {
    reconciliation_placement_for_object_with_pins(config, job, object, &[])
}

fn reconciliation_placement_for_object_with_pins(
    config: &DfsFileSystemConfig,
    job: &DfsPlacementReconciliationJob,
    object: &DfsObjectMetadata,
    pinned_node_ids: &[NodeId],
) -> Result<
    (
        DfsResolvedPlacementPolicy,
        DfsReplicationPolicy,
        DfsReplicaPlacementExplanation,
    ),
    DfsError,
> {
    if job.candidate_snapshot.is_empty() {
        return Err(DfsError::Distributed {
            message: format!(
                "placement reconciliation job {} has no accepted candidate snapshot",
                job.job_id
            ),
        });
    }
    let candidates = &job.candidate_snapshot;
    let resolved = resolve_placement_policy(
        config,
        &object.path,
        object.policy.clone(),
        candidates.len(),
    )
    .map_err(placement_reconciliation_policy_error)?;
    let (replication, replica_candidates) = if matches!(
        resolved.policy.consistency,
        DfsObjectPolicy::LocalOwned | DfsObjectPolicy::EphemeralLocal
    ) {
        (
            resolved
                .policy
                .replication_policy(BTreeMap::new())
                .map_err(placement_reconciliation_policy_error)?,
            Vec::new(),
        )
    } else {
        let (replication, candidates, _) =
            placement_inputs(&resolved.policy, candidates, &job.unavailable_node_ids)
                .map_err(placement_reconciliation_policy_error)?;
        (replication, candidates)
    };
    let request = DfsReplicaPlacementRequest {
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        target_version: object.version.clone(),
        coordinator_node_id: config.owner_node_id.clone(),
        policy: replication.clone(),
        candidates: replica_candidates,
    };
    let placement = explain_object_placement(
        &request,
        object,
        &resolved.policy.consistency,
        job.placement_snapshot.as_ref(),
        pinned_node_ids,
    )
    .map_err(placement_reconciliation_policy_error)?;
    Ok((resolved, replication, placement))
}

fn explain_object_placement(
    request: &DfsReplicaPlacementRequest,
    object: &DfsObjectMetadata,
    consistency: &DfsObjectPolicy,
    snapshot: Option<&crate::DfsPlacementSnapshot>,
    pinned_node_ids: &[NodeId],
) -> Result<DfsReplicaPlacementExplanation, DfsDistributedError> {
    if matches!(
        consistency,
        DfsObjectPolicy::LocalOwned | DfsObjectPolicy::EphemeralLocal
    ) {
        if object.policy != *consistency {
            return Err(DfsDistributedError::InvalidPolicy {
                reason:
                    "placement reconciliation cannot convert an existing object into a local policy"
                        .into(),
            });
        }
        // One shared local-owner rule serves preview and execution. A current
        // snapshot can rank remote nodes, but cannot substitute a content owner.
        return explain_replicas(&DfsReplicaPlacementRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version: object.version.clone(),
            coordinator_node_id: object.owner_node_id.clone(),
            policy: request.policy.clone(),
            candidates: vec![DfsReplicaCandidate::new(object.owner_node_id.clone())],
        });
    }
    match snapshot {
        Some(snapshot) => explain_replicas_with_placement_snapshot(
            request,
            snapshot,
            &object.current_locations,
            pinned_node_ids,
        ),
        None => explain_replicas(request),
    }
}

fn placement_reconciliation_policy_error(error: DfsDistributedError) -> DfsError {
    match error {
        DfsDistributedError::InvalidPolicy { reason }
        | DfsDistributedError::InvalidReplicaSet { reason } => {
            DfsError::InvalidReplicationPolicy { message: reason }
        }
        error => DfsError::from(error),
    }
}

fn placement_metadata_object_digest(
    object: &DfsObjectMetadata,
    analysis: &ObjectPlacementAnalysis,
    pinned_node_ids: &[NodeId],
) -> Result<[u8; 32], DfsError> {
    // The page already computes each object's actual decision. Bind it in the
    // same boundary-independent digest so descendant plans cannot change while
    // the root plan and aggregate counts happen to remain unchanged.
    #[derive(Serialize)]
    struct ObjectAuthorization<'a> {
        object: &'a DfsObjectMetadata,
        resolved: &'a DfsResolvedPlacementPolicy,
        pinned_node_ids: &'a [NodeId],
        plan: &'a crate::DfsReplicaPlacementPlan,
        decisions: Vec<DfsPlacementDecisionAuthorization<'a>>,
    }
    let authorization = ObjectAuthorization {
        object,
        resolved: &analysis.resolved,
        pinned_node_ids,
        plan: &analysis.placement.plan,
        decisions: analysis
            .placement
            .decisions
            .iter()
            .map(DfsPlacementDecisionAuthorization::from)
            .collect(),
    };
    let bytes = serde_json::to_vec(&authorization).map_err(|error| DfsError::Serialization {
        message: format!("cannot encode placement metadata snapshot: {error}"),
    })?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo-dfs-placement-object-authorization\0v1\0");
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(&bytes);
    Ok(*hasher.finalize().as_bytes())
}

fn placement_metadata_snapshot_digest(bytes: [u8; 32]) -> String {
    format!(
        "{DFS_PLACEMENT_METADATA_SNAPSHOT_DIGEST_PREFIX}{}",
        blake3::Hash::from_bytes(bytes).to_hex()
    )
}

fn placement_metadata_snapshot_digest_bytes(value: &str) -> Option<[u8; 32]> {
    let hex = value.strip_prefix(DFS_PLACEMENT_METADATA_SNAPSHOT_DIGEST_PREFIX)?;
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let mut bytes = [0_u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}

/// Merge one bounded impact page into an aggregate while preserving its
/// boundary-independent metadata snapshot identity.
pub fn merge_placement_impact(target: &mut DfsPlacementImpact, subtotal: &DfsPlacementImpact) {
    target.affected_object_count = target
        .affected_object_count
        .saturating_add(subtotal.affected_object_count);
    target.affected_bytes = target
        .affected_bytes
        .saturating_add(subtotal.affected_bytes);
    target.unknown_size_object_count = target
        .unknown_size_object_count
        .saturating_add(subtotal.unknown_size_object_count);
    target.copies_added = target.copies_added.saturating_add(subtotal.copies_added);
    target.copies_removed = target
        .copies_removed
        .saturating_add(subtotal.copies_removed);
    match (
        placement_metadata_snapshot_digest_bytes(&target.metadata_snapshot_digest),
        placement_metadata_snapshot_digest_bytes(&subtotal.metadata_snapshot_digest),
    ) {
        (Some(mut target_bytes), Some(subtotal_bytes)) => {
            for (target, subtotal) in target_bytes.iter_mut().zip(subtotal_bytes) {
                *target ^= subtotal;
            }
            target.metadata_snapshot_digest = placement_metadata_snapshot_digest(target_bytes);
        }
        (None, Some(_)) if target.metadata_snapshot_digest.is_empty() => {
            target.metadata_snapshot_digest = subtotal.metadata_snapshot_digest.clone();
        }
        _ => {}
    }
    target.complete &= subtotal.complete;
}

fn placement_job_matches(job: &DfsPlacementReconciliationJob, path: &str) -> bool {
    placement_scope_matches(job.scope, &job.path, path)
}

pub(crate) fn placement_reconciliation_job_id_from_intent_id(intent_id: &str) -> Option<&str> {
    let remainder = intent_id.strip_prefix("placement:")?;
    let (job_id, _) = remainder.split_once(':')?;
    (!job_id.is_empty()).then_some(job_id)
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn authoritative_placement_reconciliation_for_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        job_id: &str,
        path: Option<&str>,
    ) -> Result<
        Option<(
            DfsPlacementReconciliationJob,
            DfsFileSystemConfig,
            DfsFileSystemConfig,
        )>,
        DfsError,
    > {
        let Some(control_plane) = self.control_plane.as_mut() else {
            // Managed placement jobs are cluster-owned. Fail closed if their
            // authority cannot be checked; ordinary writes never enter here.
            return Err(DfsError::Distributed {
                message: format!(
                    "cannot verify placement reconciliation job {job_id}: control plane unavailable"
                ),
            });
        };
        let store = DfsControlPlaneStore::new(control_plane.as_mut());
        let Some(job) = store.placement_reconciliation(file_system_id, job_id)? else {
            return Err(DfsError::Distributed {
                message: format!("cannot verify missing placement reconciliation job {job_id}"),
            });
        };
        if !job.supports_snapshot_reconciliation() {
            return Err(DfsError::Distributed {
                message: format!(
                    "cannot verify placement reconciliation job {job_id}: unsupported or incomplete durable schema"
                ),
            });
        }
        if job.file_system_id != *file_system_id
            || job.state == DfsPlacementReconciliationState::Failed
            || path.is_some_and(|path| !placement_job_matches(&job, path))
        {
            return Ok(None);
        }
        let Some(accepted_config) = job.accepted_config.clone() else {
            return Err(DfsError::Distributed {
                message: format!(
                    "cannot verify placement reconciliation job {job_id} without its accepted config"
                ),
            });
        };
        if job.candidate_snapshot.is_empty() {
            return Err(DfsError::Distributed {
                message: format!(
                    "cannot verify placement reconciliation job {job_id} without its accepted candidate snapshot"
                ),
            });
        }
        let current_config = store
            .get_file_system_config(file_system_id)?
            .ok_or_else(|| DfsError::UnknownFileSystem {
                file_system_id: file_system_id.clone(),
            })?;
        if !placement_config_remains_authoritative_for_job(&accepted_config, &current_config, &job)
        {
            return Ok(None);
        }
        Ok(Some((job, accepted_config, current_config)))
    }

    fn acquire_authoritative_placement_reconciliation_for_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        job_id: &str,
        path: &str,
    ) -> Result<
        Option<(
            DfsPlacementReconciliationJob,
            DfsFileSystemConfig,
            DfsPlacementAuthorityPermit,
        )>,
        DfsError,
    > {
        self.acquire_authoritative_placement_reconciliation(file_system_id, job_id, Some(path))
    }

    /// sinomo:kv-consistency: Fence an existing placement reconciliation against concurrent policy Apply before granting its mutation permit.
    fn acquire_authoritative_placement_reconciliation(
        &mut self,
        file_system_id: &DfsFileSystemId,
        job_id: &str,
        path: Option<&str>,
    ) -> Result<
        Option<(
            DfsPlacementReconciliationJob,
            DfsFileSystemConfig,
            DfsPlacementAuthorityPermit,
        )>,
        DfsError,
    > {
        for attempt in 0..PLACEMENT_AUTHORITY_ACQUIRE_RETRIES {
            let Some((job, accepted_config, current_config)) =
                self.authoritative_placement_reconciliation_for_path(file_system_id, job_id, path)?
            else {
                return Ok(None);
            };
            let permit = {
                let Some(control_plane) = self.control_plane.as_mut() else {
                    return Err(DfsError::Distributed {
                        message: format!(
                            "cannot acquire placement reconciliation job {job_id}: control plane unavailable"
                        ),
                    });
                };
                let holder_process = control_plane
                    .placement_authority_process_identity()
                    .ok_or_else(|| DfsError::Distributed {
                        message: format!(
                            "cannot acquire placement reconciliation job {job_id}: this control-plane host has no daemon placement-authority session"
                        ),
                    })?;
                DfsControlPlaneStore::new(control_plane.as_mut())
                    .try_acquire_placement_authority_permit(
                        &job,
                        &current_config,
                        &self.node_id,
                        &holder_process,
                    )?
            };
            if let Some(permit) = permit {
                return Ok(Some((job, accepted_config, permit)));
            }
            if attempt + 1 < PLACEMENT_AUTHORITY_ACQUIRE_RETRIES {
                let Some(control_plane) = self.control_plane.as_mut() else {
                    return Err(DfsError::Distributed {
                        message: format!(
                            "cannot refresh placement reconciliation job {job_id}: control plane unavailable"
                        ),
                    });
                };
                control_plane.refresh_latest()?;
            }
        }
        Err(DfsError::Distributed {
            message: format!(
                "placement reconciliation job {job_id} is busy behind an active metadata mutation permit; automatic expiry is unsafe without metadata fencing; restart the owning daemon for guarded recovery, while a permanently removed node requires operator intervention after member revocation"
            ),
        })
    }

    fn release_placement_authority_permit(
        &mut self,
        permit: &mut DfsPlacementAuthorityPermit,
    ) -> Result<(), DfsError> {
        let mut last_error = None;
        for attempt in 0..PLACEMENT_AUTHORITY_RELEASE_RETRIES {
            let result = match self.control_plane.as_mut() {
                Some(control_plane) => DfsControlPlaneStore::new(control_plane.as_mut())
                    .release_placement_authority_permit(permit)
                    .map_err(DfsError::from),
                None => Err(DfsError::Distributed {
                    message: "control plane unavailable".to_string(),
                }),
            };
            match result {
                Ok(()) => return Ok(()),
                Err(error) => {
                    last_error = Some(error);
                    if attempt + 1 < PLACEMENT_AUTHORITY_RELEASE_RETRIES {
                        let _ = self.refresh_control_plane_latest();
                    }
                }
            }
        }
        Err(DfsError::Distributed {
            message: format!(
                "placement authority permit release failed after {PLACEMENT_AUTHORITY_RELEASE_RETRIES} attempts; the exact permit is retained by the daemon process for safe retry from a fresh service instance: {}",
                last_error.expect("release loop records an error")
            ),
        })
    }

    pub(crate) fn with_authoritative_placement_reconciliation<R>(
        &mut self,
        file_system_id: &DfsFileSystemId,
        job_id: &str,
        path: Option<&str>,
        item_count: usize,
        action: impl FnOnce(
            &mut Self,
            &DfsPlacementReconciliationJob,
            &DfsFileSystemConfig,
        ) -> Result<R, DfsError>,
    ) -> Result<Option<R>, DfsError> {
        self.with_authoritative_placement_reconciliation_completion(
            file_system_id,
            job_id,
            path,
            item_count,
            action,
        )?
        .map(|(value, release)| release.map(|()| value))
        .transpose()
    }

    pub(crate) fn with_authoritative_placement_reconciliation_completion<R>(
        &mut self,
        file_system_id: &DfsFileSystemId,
        job_id: &str,
        path: Option<&str>,
        item_count: usize,
        action: impl FnOnce(
            &mut Self,
            &DfsPlacementReconciliationJob,
            &DfsFileSystemConfig,
        ) -> Result<R, DfsError>,
    ) -> Result<Option<(R, Result<(), DfsError>)>, DfsError> {
        let acquired = {
            let span = tracing::trace_span!(target: "sinomo.perf", "dfs.repair.authority.acquire",
                stage = "dfs.repair.authority.acquire", job_id, file_system_id = %file_system_id,
                item_count, outcome = "error", status = tracing::field::Empty);
            let _entered = span.enter();
            let result =
                self.acquire_authoritative_placement_reconciliation(file_system_id, job_id, path);
            span.record("outcome", if result.is_ok() { "ok" } else { "error" });
            span.record(
                "status",
                match &result {
                    Ok(Some(_)) => "acquired",
                    Ok(None) => "stale",
                    Err(_) => "error",
                },
            );
            result
        }?;
        let Some((job, accepted_config, mut permit)) = acquired else {
            return Ok(None);
        };
        let action_result = action(self, &job, &accepted_config);
        let release_result = {
            let span = tracing::trace_span!(target: "sinomo.perf", "dfs.repair.authority.release",
                stage = "dfs.repair.authority.release", job_id, file_system_id = %file_system_id,
                item_count, outcome = "error", status = tracing::field::Empty);
            let _entered = span.enter();
            let result = self.release_placement_authority_permit(&mut permit);
            span.record("outcome", if result.is_ok() { "ok" } else { "error" });
            span.record(
                "status",
                if result.is_ok() {
                    "released"
                } else {
                    "pending-release"
                },
            );
            result
        };
        // The owner may checkpoint completed retirement even when release
        // remains in exact pending custody. The ordinary wrapper preserves
        // its existing error contract and never returns permission on error.
        action_result.map(|value| Some((value, release_result)))
    }

    fn placement_intent_authority(
        &mut self,
        intent_id: &str,
    ) -> Result<PlacementIntentAuthority, DfsError> {
        self.placement_intent_authority_recording(intent_id, &mut false)
    }

    fn placement_intent_authority_recording(
        &mut self,
        intent_id: &str,
        retired: &mut bool,
    ) -> Result<PlacementIntentAuthority, DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Ok(PlacementIntentAuthority::Stale);
        };
        if matches!(
            intent.state,
            DfsWriteIntentState::Rejected
                | DfsWriteIntentState::Conflict
                | DfsWriteIntentState::Aborted
        ) {
            return Ok(PlacementIntentAuthority::Stale);
        }
        let Some(job_id) = placement_reconciliation_job_id_from_intent_id(intent_id) else {
            if intent_id.starts_with("placement:") {
                return Err(DfsError::Distributed {
                    message: format!("malformed reserved placement intent id: {intent_id}"),
                });
            }
            return Ok(PlacementIntentAuthority::Ordinary);
        };
        let object = self.current_read_object_metadata(&intent.object_id)?;
        let path = object.path.clone();
        let mut authority = self.acquire_authoritative_placement_reconciliation_for_path(
            &intent.file_system_id,
            job_id,
            &path,
        )?;
        let authoritative_result = authority
            .as_ref()
            .map(|(job, config, _)| {
                self.managed_placement_intent_matches_job(&intent, &object, job, config)
            })
            .transpose();
        let authoritative = match authoritative_result {
            Ok(authoritative) => authoritative.unwrap_or(false),
            Err(error) => {
                if let Some((_, _, permit)) = authority.as_mut() {
                    let _ = self.release_placement_authority_permit(permit);
                }
                return Err(error);
            }
        };
        if authoritative {
            let (_, _, permit) = authority.expect("authoritative placement has a permit");
            return Ok(PlacementIntentAuthority::Managed(permit));
        }
        let mut held_permit = authority.map(|(_, _, permit)| permit);

        let abort_result = self.retire_stale_placement_intents(std::slice::from_ref(&intent));
        if abort_result.is_ok() && intent.state == DfsWriteIntentState::Committed {
            *retired = true;
        }
        let release_result = held_permit
            .as_mut()
            .map(|permit| self.release_placement_authority_permit(permit))
            .transpose();
        match (abort_result, release_result) {
            (Err(error), _) => return Err(error),
            (Ok(()), Err(error)) => return Err(error),
            (Ok(()), Ok(_)) => {}
        }
        Ok(PlacementIntentAuthority::Stale)
    }

    /// Shared scalar/group stale semantics. A committed publication is never
    /// rewritten as Aborted; only its derived execution ownership is retired.
    /// The caller retains any exact placement permit through these transitions.
    pub(crate) fn retire_stale_placement_intents(
        &mut self,
        intents: &[DistributedWriteIntent],
    ) -> Result<(), DfsError> {
        let mut committed = BTreeSet::new();
        for intent in intents {
            if intent.state == DfsWriteIntentState::Committed {
                committed.insert(intent.intent_id.clone());
                continue;
            }
            let content_hash = self
                .writes
                .pending_writes
                .get(&intent.intent_id)
                .map(|pending| pending.content_hash.clone())
                .or_else(|| {
                    self.current_read_object_metadata(&intent.object_id)
                        .ok()
                        .and_then(|object| object.content_hash)
                });
            let mut aborted = intent.clone();
            aborted.state = DfsWriteIntentState::Aborted;
            self.put_metadata_intent(&aborted, content_hash.as_deref())?;
            self.coordinator.restore_intent(aborted)?;
            self.remove_pending_write_state(&intent.intent_id);
        }
        self.retire_committed_repairs(&committed);
        Ok(())
    }

    /// Validate one intent while the caller owns the existing exact job permit.
    /// Scalar checks and bounded repair batches share these payload/pin rules.
    #[tracing::instrument(target = "sinomo.perf", level = "trace",
        name = "dfs.repair.authority.validate", skip_all,
        fields(stage = "dfs.repair.authority.validate", intent_id = %intent.intent_id,
            job_id = %job.job_id, file_system_id = %intent.file_system_id, item_count = 1,
            outcome = "error", status = tracing::field::Empty))]
    pub(crate) fn managed_placement_intent_matches_job(
        &self,
        intent: &DistributedWriteIntent,
        object: &DfsObjectMetadata,
        job: &DfsPlacementReconciliationJob,
        config: &DfsFileSystemConfig,
    ) -> Result<bool, DfsError> {
        if placement_reconciliation_job_id_from_intent_id(&intent.intent_id)
            != Some(job.job_id.as_str())
            || job.file_system_id != intent.file_system_id
            || object.object_id != intent.object_id
            || object.file_system_id != intent.file_system_id
            || !placement_job_matches(job, &object.path)
        {
            tracing::Span::current().record("outcome", "ok");
            tracing::Span::current().record("status", "mismatch");
            return Ok(false);
        }
        let pinned_node_ids = self.content_pin_nodes(&object.object_id);
        let (resolved, expected_replication, expected_placement) =
            reconciliation_placement_for_object_with_pins(config, job, object, &pinned_node_ids)
                .inspect_err(|_| {
                    tracing::Span::current().record("outcome", "error");
                })?;
        let permits_generic_repair = !matches!(
            resolved.policy.consistency,
            DfsObjectPolicy::LocalOwned | DfsObjectPolicy::EphemeralLocal
        );
        if !permits_generic_repair {
            tracing::info!(
                target: "sinomo.dfs.placement",
                intent_id = %intent.intent_id, object_id = %object.object_id,
                policy = ?resolved.policy.consistency, reason = "owner-local-policy",
                "fenced legacy managed replica repair; existing content ownership is preserved"
            );
        }
        let matches = permits_generic_repair
            && intent.policy == expected_replication
            && intent.desired_replica_nodes == expected_placement.plan.desired_replica_nodes;
        tracing::Span::current().record("outcome", "ok");
        tracing::Span::current().record("status", if matches { "valid" } else { "mismatch" });
        Ok(matches)
    }

    /// Fence delayed receipts and repair retries belonging to a managed
    /// placement job. Placement intent ids already durably encode their job id
    /// (`placement:<job-id>:...`), including across metadata-shard migration,
    /// so this is backward-compatible with existing intent records.
    pub(crate) fn placement_intent_remains_authoritative(
        &mut self,
        intent_id: &str,
    ) -> Result<bool, DfsError> {
        self.placement_intent_remains_authoritative_recording(intent_id, &mut false)
    }

    pub(crate) fn placement_intent_remains_authoritative_recording(
        &mut self,
        intent_id: &str,
        retired: &mut bool,
    ) -> Result<bool, DfsError> {
        match self.placement_intent_authority_recording(intent_id, retired)? {
            PlacementIntentAuthority::Ordinary => Ok(true),
            PlacementIntentAuthority::Stale => Ok(false),
            PlacementIntentAuthority::Managed(mut permit) => {
                self.release_placement_authority_permit(&mut permit)?;
                Ok(true)
            }
        }
    }

    /// Run one bounded placement-derived mutation while holding the exact v1
    /// job/config permit. Ordinary foreground intents do not touch cluster KV.
    /// The explicit finally-style release keeps every success and error path
    /// from silently stranding the latch. Transient release failures retain the
    /// exact handle in the daemon-wide pending-release registry, including
    /// across an unwind or ephemeral service replacement. A process crash
    /// requires guarded prior-session recovery because automatic expiry is
    /// unsafe without a metadata fencing token.
    pub(crate) fn with_placement_intent_authority<R>(
        &mut self,
        intent_id: &str,
        action: impl FnOnce(&mut Self) -> Result<R, DfsError>,
    ) -> Result<Option<R>, DfsError> {
        match self.placement_intent_authority(intent_id)? {
            PlacementIntentAuthority::Ordinary => action(self).map(Some),
            PlacementIntentAuthority::Stale => Ok(None),
            PlacementIntentAuthority::Managed(mut permit) => {
                let action_result = action(self);
                let release_result = self.release_placement_authority_permit(&mut permit);
                match (action_result, release_result) {
                    (Ok(value), Ok(())) => Ok(Some(value)),
                    (Err(error), _) => Err(error),
                    (Ok(_), Err(error)) => Err(error),
                }
            }
        }
    }
}

pub(crate) fn placement_config_remains_authoritative_for_job(
    accepted: &DfsFileSystemConfig,
    current: &DfsFileSystemConfig,
    job: &DfsPlacementReconciliationJob,
) -> bool {
    accepted.file_system_id == current.file_system_id
        && accepted.owner_node_id == current.owner_node_id
        && placement_rules_affecting_job(accepted, job)
            == placement_rules_affecting_job(current, job)
}

fn placement_rules_affecting_job(
    config: &DfsFileSystemConfig,
    job: &DfsPlacementReconciliationJob,
) -> Vec<DfsSubtreePolicy> {
    let mut rules = config
        .subtree_policies
        .iter()
        .filter(|rule| match job.scope {
            DfsPlacementScope::SelfOnly => match rule.scope {
                DfsPlacementScope::SelfOnly => rule.path_prefix == job.path,
                DfsPlacementScope::Subtree => path_is_within(&job.path, &rule.path_prefix),
            },
            DfsPlacementScope::Subtree => match rule.scope {
                DfsPlacementScope::SelfOnly => path_is_within(&rule.path_prefix, &job.path),
                DfsPlacementScope::Subtree => {
                    path_is_within(&job.path, &rule.path_prefix)
                        || path_is_within(&rule.path_prefix, &job.path)
                }
            },
        })
        .cloned()
        .collect::<Vec<_>>();
    rules.sort_by(|left, right| {
        left.path_prefix
            .cmp(&right.path_prefix)
            .then_with(|| placement_scope_rank(left.scope).cmp(&placement_scope_rank(right.scope)))
    });
    rules
}

pub(crate) fn scoped_placement_config_for_job(
    config: &DfsFileSystemConfig,
    job: &DfsPlacementReconciliationJob,
) -> DfsFileSystemConfig {
    DfsFileSystemConfig {
        metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
        retention_policies: Default::default(),
        module_metadata: Default::default(),
        file_system_id: config.file_system_id.clone(),
        display_name: config.display_name.clone(),
        owner_node_id: config.owner_node_id.clone(),
        subtree_policies: placement_rules_affecting_job(config, job),
    }
}

fn placement_scope_matches(scope: DfsPlacementScope, root: &str, path: &str) -> bool {
    match scope {
        DfsPlacementScope::SelfOnly => path == root,
        DfsPlacementScope::Subtree => path_is_within(path, root),
    }
}

// Remote placement work must yield so foreground requests can preempt
// maintenance before its global admission is released. Synchronous callers
// retain their existing execution path.
impl<T> NetworkedDfsService<T>
where
    T: crate::AsyncDfsMessageTransport + crate::DfsTransportIdentity + DfsMessageTransport,
{
    fn prepare_placement_reconciliation_objects(
        &mut self,
        config: &DfsFileSystemConfig,
        job: &DfsPlacementReconciliationJob,
        budget: &DfsAdaptivePlacementBudget,
        started: Instant,
        authority: Option<&PlacementPageAuthority>,
    ) -> Result<Vec<PreparedPlacementObject>, DfsError> {
        let page = self.placement_object_page(job, budget.max_objects_per_tick)?;
        let count = page.objects.len();
        let mut prepared = Vec::with_capacity(count);
        let mut transfer_bytes = 0u64;
        for (index, object) in page.objects.into_iter().enumerate() {
            if !prepared.is_empty()
                && (transfer_bytes >= budget.max_bytes_per_tick
                    || started.elapsed() >= Duration::from_millis(budget.max_millis_per_tick))
            {
                break;
            }
            let next_cursor = if index + 1 == count {
                page.next_cursor.clone()
            } else if let Some(backend) = self.metadata_backend.as_ref() {
                Some(backend.object_page_cursor_for_object(&object)?)
            } else {
                Some(DfsObjectPageCursor {
                    path: object.path.clone(),
                    object_id: object.object_id.clone(),
                    shard_id: None,
                })
            };
            let plan = if object.kind == DfsObjectKind::Tombstone
                || !placement_job_matches(job, &object.path)
            {
                None
            } else {
                let pins = self.content_pin_nodes(&object.object_id);
                let (resolved, replication, placement) =
                    reconciliation_placement_for_object_with_pins(config, job, &object, &pins)?;
                let desired_nodes = placement.plan.desired_replica_nodes;
                let content_bearing = matches!(
                    object.kind,
                    DfsObjectKind::File | DfsObjectKind::Symlink | DfsObjectKind::Artifact
                ) && object.content_hash.is_some();
                let missing_before = if content_bearing {
                    desired_nodes
                        .iter()
                        .filter(|node| !object.current_locations.contains(node))
                        .count()
                } else {
                    0
                };
                let obsolete_before = content_bearing
                    && object
                        .current_locations
                        .iter()
                        .any(|node| !desired_nodes.contains(node));
                let change_needed = object.policy != resolved.policy.consistency
                    || missing_before > 0
                    || obsolete_before;
                let transfer_bytes = object
                    .content_size_bytes
                    .unwrap_or_default()
                    .saturating_mul(u64::try_from(missing_before).unwrap_or(u64::MAX));
                let noop_check = self.placement_noop_check(&object, change_needed, authority)?;
                Some(PreparedPlacementObjectPlan {
                    resolved,
                    replication,
                    desired_nodes,
                    missing_before,
                    content_bearing,
                    transfer_bytes,
                    change_needed,
                    noop_check,
                })
            };
            transfer_bytes =
                transfer_bytes.saturating_add(plan.as_ref().map_or(0, |plan| plan.transfer_bytes));
            prepared.push(PreparedPlacementObject {
                object,
                next_cursor,
                plan,
            });
        }
        Ok(prepared)
    }

    async fn read_remote_placement_noop_proofs(
        &mut self,
        config: &DfsFileSystemConfig,
        objects: &[PreparedPlacementObject],
        started: Instant,
        max_millis: u64,
    ) -> Result<PlacementRemoteNoopProofs, DfsError> {
        const MAX_LOOKUP_PATHS: usize = 64;
        let mut by_primary =
            BTreeMap::<NodeId, Vec<(&DfsObjectMetadata, &DfsMetadataShardConfig)>>::new();
        let mut primary_order = Vec::new();
        for object in objects {
            if let Some(plan) = object.plan.as_ref()
                && let PlacementNoopCheck::LookupRemote(shard) = &plan.noop_check
            {
                if !by_primary.contains_key(&shard.primary_node_id) {
                    primary_order.push(shard.primary_node_id.clone());
                }
                by_primary
                    .entry(shard.primary_node_id.clone())
                    .or_default()
                    .push((&object.object, shard));
            }
        }
        let mut proofs = PlacementRemoteNoopProofs {
            config: config.clone(),
            objects: BTreeMap::new(),
        };
        let mut requests = 0usize;
        for primary in primary_order {
            let expected = by_primary.remove(&primary).expect("prepared primary group");
            for batch in expected.chunks(MAX_LOOKUP_PATHS) {
                if requests > 0 && started.elapsed() >= Duration::from_millis(max_millis) {
                    return Ok(proofs);
                }
                requests += 1;
                let request_id = self.next_request_id();
                let response = self.route_placement_namespace_operation_to_metadata_primary_with_request_id_and_options(
                    request_id, config.file_system_id.clone(), primary.clone(),
                    DfsNamespaceOperation::LookupPaths { paths: batch.iter().map(|(object, _)| object.path.clone()).collect() },
                    crate::DFS_ROUTED_NAMESPACE_OP_TIMEOUT, false, false,
                ).await?;
                if response.result != crate::DfsNamespaceResult::Applied {
                    continue;
                }
                // Duplicate or extra rows cannot certify another requested object.
                // Every accepted proof matches the full expected immutable row.
                let mut returned = BTreeMap::<DfsObjectId, Option<DfsObjectMetadata>>::new();
                let mut returned_paths = BTreeMap::<String, usize>::new();
                for object in response.synced_path_objects {
                    *returned_paths.entry(object.path.clone()).or_default() += 1;
                    if let Some(previous) = returned.get_mut(&object.object_id) {
                        *previous = None;
                    } else {
                        returned.insert(object.object_id.clone(), Some(object));
                    }
                }
                for (object, shard) in batch {
                    if returned.get(&object.object_id).and_then(Option::as_ref) == Some(*object)
                        && returned_paths.get(&object.path) == Some(&1)
                        && self.select_metadata_shard_for_object(object)?.as_ref() == Some(*shard)
                    {
                        proofs.objects.insert(
                            object.object_id.clone(),
                            ((*object).clone(), (*shard).clone()),
                        );
                    }
                }
            }
        }
        Ok(proofs)
    }

    #[cfg(test)]
    pub(crate) async fn run_placement_reconciliation_bounded_async(
        &mut self,
        max_objects: usize,
    ) -> Result<Option<DfsPlacementReconciliationJob>, DfsError> {
        self.run_placement_reconciliation_bounded_async_with_eligibility(max_objects, None)
            .await
    }

    pub(crate) async fn run_placement_reconciliation_bounded_async_with_pending_repairs(
        &mut self,
        max_objects: usize,
        pending_repair_count: usize,
    ) -> Result<Option<DfsPlacementReconciliationJob>, DfsError> {
        self.run_placement_reconciliation_bounded_async_with_eligibility(
            max_objects,
            Some(pending_repair_count),
        )
        .await
    }

    async fn run_placement_reconciliation_bounded_async_with_eligibility(
        &mut self,
        max_objects: usize,
        pending_repair_count: Option<usize>,
    ) -> Result<Option<DfsPlacementReconciliationJob>, DfsError> {
        let file_system_id = crate::sifs_file_system_id();
        let (current_config, coordinator_node_id) = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(None);
            };
            let store = DfsControlPlaneStore::new(control_plane.as_mut());
            // A maintenance tick may run before the default filesystem has
            // been published to cluster KV. No managed placement job can be
            // accepted without that config, so placement is simply idle; it
            // must not prevent unrelated namespace or anti-entropy work.
            let Some(config) = store.get_file_system_config(&file_system_id)? else {
                if store.has_v1_placement_reconciliation_state(&file_system_id)? {
                    return Err(DfsError::Distributed {
                        message: format!(
                            "placement reconciliation state exists without filesystem config {file_system_id}"
                        ),
                    });
                }
                return Ok(None);
            };
            let coordinator_node_id = store
                .select_metadata_shard_for_path_strict_default_sifs_split(&file_system_id, "/")?
                .map(|shard| shard.primary_node_id);
            (config, coordinator_node_id)
        };
        // Advancing a job performs durable object-side effects before the
        // progress CAS. Without one deterministic runner, every daemon can
        // race the same revision and create the same repair intent under a
        // different coordinator. The filesystem root primary is a
        // failover-capable, cluster-wide job coordinator; using one global
        // primary also means a placement update can wake the same node that
        // owns the Files API in the default cluster. Individual objects are
        // still routed to their own metadata primaries below.
        if coordinator_node_id
            .as_ref()
            .is_some_and(|node_id| node_id != &self.node_id)
        {
            return Ok(None);
        }
        self.observe_adaptive_placement_and_enqueue(&current_config)?;
        let expected = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(None);
            };
            let mut store = DfsControlPlaneStore::new(control_plane.as_mut());
            let Some(job) = store.next_placement_reconciliation_with_eligibility(
                &file_system_id,
                &current_config,
                |job| {
                    pending_repair_count.is_none_or(|count| {
                        crate::maintenance::dfs_placement_reconciliation_job_can_advance(
                            job,
                            count,
                            Some(&current_config),
                        )
                    })
                },
            )?
            else {
                return Ok(None);
            };
            job
        };
        let mut desired = expected.clone();
        if expected.accepted_config.as_ref().is_some_and(|accepted| {
            !placement_config_remains_authoritative_for_job(accepted, &current_config, &expected)
        }) {
            mark_placement_reconciliation_superseded(&mut desired);
            desired.revision = desired.revision.saturating_add(1);
            desired.updated_at_unix_nanos = dfs_now_unix_nanos();
            let updated = {
                let Some(control_plane) = self.control_plane.as_mut() else {
                    return Ok(None);
                };
                DfsControlPlaneStore::new(control_plane.as_mut())
                    .compare_and_put_placement_reconciliation(&expected, &desired)?
            };
            return Ok(updated.then_some(desired));
        }
        let config = expected
            .accepted_config
            .clone()
            .expect("executable placement jobs have an accepted config snapshot");
        // Cancelling this future does not undo earlier remote effects. Until
        // the post-page CAS below, durable progress remains at the previous
        // page and a later pass revalidates it with fresh normal request IDs.
        let page_authority = self.prepare_placement_page_authority(&current_config)?;
        let result = self
            .advance_placement_reconciliation_page_async(
                &config,
                &mut desired,
                max_objects.max(1),
                page_authority.as_ref(),
            )
            .await;
        // Finish this remote page's poll before any progress publication I/O.
        // Foreground cancellation keeps the prior durable cursor; completed
        // remote effects are revalidated idempotently by the next owner.
        tokio::task::yield_now().await;
        // Placement config and DFS object metadata live in separate durable
        // planes, so there is no safe crash-recoverable cross-plane lock in
        // this slice. Re-read after every bounded page: if an overlapping
        // config became authoritative during the page, terminalize the stale
        // job immediately. The newer durable job then converges any
        // idempotent stale-page effects instead of letting the oldest job
        // starve the queue indefinitely.
        let post_page_config = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(None);
            };
            let config = match control_plane.async_client() {
                Some(client) => {
                    crate::DfsControlPlaneAsyncStore::new(client)
                        .get_file_system_config(&file_system_id)
                        .await?
                }
                None => DfsControlPlaneStore::new(control_plane.as_mut())
                    .get_file_system_config(&file_system_id)?,
            };
            config.ok_or_else(|| DfsError::UnknownFileSystem {
                file_system_id: file_system_id.clone(),
            })?
        };
        let superseded_during_page = expected.accepted_config.as_ref().is_some_and(|accepted| {
            !placement_config_remains_authoritative_for_job(accepted, &post_page_config, &expected)
        });
        desired.revision = desired.revision.saturating_add(1);
        desired.updated_at_unix_nanos = dfs_now_unix_nanos();
        if superseded_during_page {
            mark_placement_reconciliation_superseded(&mut desired);
        } else {
            match result {
                Ok(()) => desired.last_error = None,
                Err(error) => {
                    if matches!(error, DfsError::InvalidReplicationPolicy { .. }) {
                        desired.state = DfsPlacementReconciliationState::Failed;
                        desired.cursor = None;
                    }
                    desired.last_error = Some(error.to_string());
                }
            }
        }
        let updated = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(None);
            };
            let guards = if superseded_during_page {
                Vec::new()
            } else {
                page_authority
                    .as_ref()
                    .map_or_else(Vec::new, |proof| proof.compares.clone())
            };
            match control_plane.async_client() {
                Some(client) => {
                    crate::DfsControlPlaneAsyncStore::new(client)
                        .compare_and_put_placement_reconciliation_with_guards(
                            &expected, &desired, guards,
                        )
                        .await?
                }
                None => DfsControlPlaneStore::new(control_plane.as_mut())
                    .compare_and_put_placement_reconciliation_with_guards(
                        &expected, &desired, guards,
                    )?,
            }
        };
        Ok(updated.then_some(desired))
    }

    #[tracing::instrument(target = "sinomo.perf", level = "trace", name = "dfs.placement.page",
        skip_all, fields(stage = "dfs.placement.page", job_id = %job.job_id, path = %job.path,
        phase = ?job.state, scanned_objects = tracing::field::Empty,
        content_bytes = tracing::field::Empty, outcome = "error"))]
    async fn advance_placement_reconciliation_page_async(
        &mut self,
        config: &DfsFileSystemConfig,
        job: &mut DfsPlacementReconciliationJob,
        max_objects: usize,
        authority: Option<&PlacementPageAuthority>,
    ) -> Result<(), DfsError> {
        if job.state == DfsPlacementReconciliationState::Pending {
            job.total_object_count = 0;
            job.state = DfsPlacementReconciliationState::Scanning;
            job.cursor = None;
        }
        let budget = placement_page_budget(job, max_objects);
        let started = Instant::now();
        let mut processed_objects = 0usize;
        let mut processed_bytes = 0u64;
        let mut pass_pending_repairs =
            if job.state == DfsPlacementReconciliationState::Repairing && job.cursor.is_none() {
                0
            } else {
                job.pending_repair_count
            };
        // Plan one bounded page once, then share its independently authorized
        // primary reads. No speculative page or durable proof cache is retained.
        let objects = self
            .prepare_placement_reconciliation_objects(config, job, &budget, started, authority)?;
        let mut remote_proofs = self
            .read_remote_placement_noop_proofs(
                config,
                &objects,
                started,
                budget.max_millis_per_tick,
            )
            .await?;
        if objects.is_empty() {
            job.cursor = None;
        }
        for prepared in objects {
            if processed_objects > 0 {
                tokio::task::yield_now().await;
            }
            if processed_objects >= budget.max_objects_per_tick {
                break;
            }
            if processed_objects > 0
                && (processed_bytes >= budget.max_bytes_per_tick
                    || started.elapsed() >= Duration::from_millis(budget.max_millis_per_tick))
            {
                break;
            }
            let object = &prepared.object;
            processed_objects = processed_objects.saturating_add(1);
            if job.state == DfsPlacementReconciliationState::Scanning {
                job.scanned_object_count = job.scanned_object_count.saturating_add(1);
            }
            let Some(plan) = prepared.plan else {
                job.cursor = prepared.next_cursor;
                continue;
            };
            if job.state == DfsPlacementReconciliationState::Scanning {
                job.matched_object_count = job.matched_object_count.saturating_add(1);
            }
            processed_bytes = processed_bytes.saturating_add(plan.transfer_bytes);
            job.processed_content_bytes = job
                .processed_content_bytes
                .saturating_add(plan.transfer_bytes);
            if job.state == DfsPlacementReconciliationState::Scanning && plan.change_needed {
                job.updated_object_count = job.updated_object_count.saturating_add(1);
                job.scheduled_repair_count = job
                    .scheduled_repair_count
                    .saturating_add(plan.missing_before);
            }
            let confirmed_noop = match &plan.noop_check {
                PlacementNoopCheck::CertifiedLocal => {
                    // Preparation can precede another object's remote await.
                    // Preserve the ordinary exact-row check at consumption.
                    self.current_read_object_metadata(&object.object_id)? == *object
                }
                PlacementNoopCheck::LookupRemote(expected_shard) => {
                    self.select_metadata_shard_for_object(object)?.as_ref() == Some(expected_shard)
                        && remote_proofs.take_matches(config, object, expected_shard)
                }
                PlacementNoopCheck::RequiresReconciliation => false,
            };
            let current = if confirmed_noop {
                Some(object.clone())
            } else {
                self.route_placement_reconciliation_object_async(
                    job,
                    object,
                    plan.resolved.policy.consistency,
                    plan.replication,
                    plan.desired_nodes.clone(),
                )
                .await?
            };
            if job.state == DfsPlacementReconciliationState::Repairing
                && plan.content_bearing
                && let Some(current) = current
                && placement_job_matches(job, &current.path)
                && current.kind != DfsObjectKind::Tombstone
            {
                pass_pending_repairs = pass_pending_repairs.saturating_add(
                    plan.desired_nodes
                        .iter()
                        .filter(|node| !current.current_locations.contains(node))
                        .count(),
                );
            }
            // Exact backend cursor only after this object's actual completion.
            // Errors/cancellation preserve the preceding object's durable cut.
            job.cursor = prepared.next_cursor;
        }
        finish_placement_reconciliation_page(job, pass_pending_repairs);
        let span = tracing::Span::current();
        span.record("scanned_objects", processed_objects);
        span.record("content_bytes", processed_bytes);
        span.record("outcome", "ok");
        metrics::counter!("sinomo.dfs.background.items", "worker" => "placement", "kind" => "scanned")
            .increment(processed_objects as u64);
        Ok(())
    }

    async fn route_placement_reconciliation_object_async(
        &mut self,
        job: &DfsPlacementReconciliationJob,
        object: &DfsObjectMetadata,
        object_policy: DfsObjectPolicy,
        replication_policy: DfsReplicationPolicy,
        desired_replica_node_ids: Vec<NodeId>,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        let operation = DfsNamespaceOperation::ReconcilePlacement {
            job_id: job.job_id.clone(),
            path: object.path.clone(),
            object_id: object.object_id.clone(),
            expected_version: object.version.clone(),
            object_policy,
            replication_policy,
            desired_replica_node_ids,
        };
        let shard = self.select_metadata_shard_for_object(object)?;
        if let Some(shard) = shard
            && shard.primary_node_id != self.node_id
        {
            // As on the synchronous path, a later maintenance invocation gets
            // a fresh monotonic request token. This invocation's redirects keep
            // this ID and immutable command. Cross-invocation convergence is owned by
            // job/config/version checks and deterministic placement repair IDs.
            let operation_id = self.next_request_id();
            return self
                .route_placement_namespace_operation_with_request_id_and_result_async(
                    operation_id,
                    shard.primary_node_id,
                    operation,
                )
                .await
                .map(|objects| {
                    objects
                        .into_iter()
                        .find(|current| current.object_id == object.object_id)
                })
                .map_err(|error| match error {
                    crate::DfsPosixError::Dfs(error) => error,
                    other => DfsError::Distributed {
                        message: other.to_string(),
                    },
                });
        }
        self.execute_namespace_operation_local(&operation)
            .map_err(|error| DfsError::Distributed {
                message: error.to_string(),
            })?;
        self.current_read_object_metadata(&object.object_id)
            .map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DfsBlobTransferResult, DfsFileSystem, DfsMetadataBackend, DfsMetadataShardAssignment,
        DfsMetadataShardConfig, DfsRepairReplicaResponse, DfsRoutedCall, DfsRoutedMessage,
        DfsTransportIdentity, DfsWriteIntentStatus, LocalContentStore, NetworkedDfsServiceEvent,
        SharedDfsControlPlaneKvClient, open_root_local_metadata_backend, sifs_file_system_id,
    };
    use sinomo_core::kv::InProcessKvStore;
    use sinomo_core::network::NetworkError;

    #[derive(Clone, Debug)]
    struct PreviewTransport {
        local_node_id: NodeId,
    }

    impl DfsTransportIdentity for PreviewTransport {
        fn local_node_id(&self) -> &NodeId {
            &self.local_node_id
        }
    }

    impl DfsMessageTransport for PreviewTransport {
        fn send_routed_call(
            &mut self,
            target_node_id: &NodeId,
            _call: DfsRoutedCall,
        ) -> Result<(), NetworkError> {
            Err(NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            })
        }

        fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
            Ok(Vec::new())
        }
    }

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("node")
    }

    #[test]
    fn adaptive_placement_requires_sustained_change_and_enqueues_once() {
        let digest = "blake3:changed";
        let start = 10;
        let (first, ready) = advance_adaptive_placement_observation(None, digest, 7, start);
        assert!(!ready);
        let (second, ready) = advance_adaptive_placement_observation(
            Some(&first),
            digest,
            7,
            start + ADAPTIVE_PLACEMENT_MIN_SUSTAINED_NANOS,
        );
        assert!(!ready);
        let (mut third, ready) = advance_adaptive_placement_observation(
            Some(&second),
            digest,
            7,
            start + ADAPTIVE_PLACEMENT_MIN_SUSTAINED_NANOS + 1,
        );
        assert!(ready);
        third.last_enqueued_snapshot_digest = digest.to_string();
        let (_, ready) = advance_adaptive_placement_observation(
            Some(&third),
            digest,
            7,
            start + ADAPTIVE_PLACEMENT_MIN_SUSTAINED_NANOS + 2,
        );
        assert!(!ready);
    }

    #[test]
    fn async_content_advertisement_publication_is_lease_gated() {
        let local = node("async-content-advertisement");
        let now = time::OffsetDateTime::now_utc();
        let mut desired = DfsNodeAdvertisement::content(local.clone());
        desired.storage_health = crate::DfsHealthState::Healthy;
        desired.storage_freshness = crate::DfsObservationFreshness::Fresh;
        desired.expires_at = Some(now + time::Duration::seconds(90));
        let mut kv = InProcessKvStore::new();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        runtime.block_on(async {
            assert!(
                publish_dfs_content_advertisement_if_needed_async(
                    &mut kv,
                    &local,
                    desired.clone(),
                )
                .await
                .expect("first publication")
            );
            assert!(
                !publish_dfs_content_advertisement_if_needed_async(&mut kv, &local, desired,)
                    .await
                    .expect("lease-gated publication")
            );
        });
    }

    fn advertisement(node_id: &str, zone: &str) -> DfsNodeAdvertisement {
        DfsNodeAdvertisement {
            node_id: node(node_id),
            capacity_weight: 1,
            accepts_metadata: false,
            accepts_content: true,
            zone: Some(zone.to_string()),
            storage_health: crate::DfsHealthState::Unknown,
            storage_freshness: crate::DfsObservationFreshness::NeverSampled,
            usable_capacity_bytes: None,
            reserved_free_bytes: None,
            content_location_count: 1,
            network_metering: crate::DfsNetworkMetering::Unknown,
            resource_pressure: crate::DfsResourcePressure::Normal,
            advertisement_generation: 0,
            updated_at: time::OffsetDateTime::now_utc(),
            expires_at: None,
        }
    }

    fn placement_config(owner_node_id: NodeId) -> DfsFileSystemConfig {
        DfsFileSystemConfig {
            metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: sifs_file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id,
            subtree_policies: Vec::new(),
        }
    }

    fn reconciliation_job(
        path: &str,
        scope: DfsPlacementScope,
        unavailable_node_ids: BTreeSet<NodeId>,
    ) -> DfsPlacementReconciliationJob {
        DfsPlacementReconciliationJob {
            schema_version: DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION,
            job_id: "placement-test-job".to_string(),
            file_system_id: sifs_file_system_id(),
            path: path.to_string(),
            scope,
            unavailable_node_ids,
            candidate_snapshot: vec![advertisement("node-a", "east")],
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
        }
    }

    fn placement_object(
        suffix: &str,
        path: &str,
        kind: DfsObjectKind,
        owner_node_id: NodeId,
        current_locations: Vec<NodeId>,
        content_size_bytes: Option<u64>,
    ) -> DfsObjectMetadata {
        let content_hash = matches!(
            kind,
            DfsObjectKind::File | DfsObjectKind::Symlink | DfsObjectKind::Artifact
        )
        .then(|| format!("hash-{suffix}"));
        DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named(format!("object-{suffix}")).expect("object"),
            file_system_id: sifs_file_system_id(),
            path: path.to_string(),
            kind,
            tombstone_provenance: None,
            owner_node_id,
            current_locations,
            content_hash,
            content_size_bytes,
            link_target_object_id: None,
            mode: 0,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: DfsVersion::from_legacy_named(format!("version-{suffix}")).expect("version"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn preview_with(
        config: &DfsFileSystemConfig,
        policy: DfsPlacementPolicy,
        candidates: Vec<DfsNodeAdvertisement>,
    ) -> Result<DfsPlacementPreview, DfsDistributedError> {
        preview_placement(
            config,
            "/users/team/report.md",
            DfsObjectId::from_legacy_named("preview-object").expect("object"),
            DfsVersion::from_legacy_named("preview-version").expect("version"),
            DfsObjectPolicy::ReplicatedEventual,
            node("node-a"),
            vec![node("node-a")],
            candidates,
            Some(policy),
            DfsPlacementScope::SelfOnly,
        )
    }

    #[test]
    fn filesystem_semantics_ignore_distinct_subtree_rule_discovery_order() {
        let owner = node("owner");
        let subtree = DfsSubtreePolicy::legacy("/users/team", DfsObjectPolicy::ReplicatedEventual);
        let self_only = DfsSubtreePolicy {
            path_prefix: "/users/team".to_string(),
            policy: DfsObjectPolicy::ReplicatedConfirmed,
            scope: DfsPlacementScope::SelfOnly,
            min_commit_replicas: Some(2),
            desired_replicas: Some(2),
            required_node_ids: Vec::new(),
            forbidden_node_ids: Vec::new(),
            min_distinct_zones: 0,
        };
        let services = DfsSubtreePolicy::legacy("/services", DfsObjectPolicy::ReplicatedEventual);
        let first = DfsFileSystem {
            metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: sifs_file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: vec![subtree, self_only, services],
        };
        let mut reordered = first.clone();
        reordered.subtree_policies.reverse();

        assert!(dfs_file_system_semantically_eq(&first, &reordered));

        let first_config = DfsFileSystemConfig::from(&first);
        let reordered_config = DfsFileSystemConfig::from(&reordered);
        assert!(dfs_file_system_config_semantically_eq(
            &first_config,
            &reordered_config
        ));
        for path in ["/users/team", "/users/team/report.md", "/services/api"] {
            assert_eq!(
                resolve_placement_policy(
                    &first_config,
                    path,
                    DfsObjectPolicy::ReplicatedEventual,
                    2,
                )
                .expect("first policy"),
                resolve_placement_policy(
                    &reordered_config,
                    path,
                    DfsObjectPolicy::ReplicatedEventual,
                    2,
                )
                .expect("reordered policy"),
                "distinct rule discovery order must not change resolution for {path}"
            );
        }
    }

    #[test]
    fn filesystem_semantics_preserve_duplicate_rule_precedence() {
        let owner = node("owner");
        let eventual = DfsSubtreePolicy::legacy("/users/team", DfsObjectPolicy::ReplicatedEventual);
        let confirmed =
            DfsSubtreePolicy::legacy("/users/team", DfsObjectPolicy::ReplicatedConfirmed);
        let first = DfsFileSystem {
            metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: sifs_file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: vec![eventual.clone(), confirmed.clone()],
        };
        let reversed = DfsFileSystem {
            subtree_policies: vec![confirmed, eventual],
            ..first.clone()
        };

        assert!(
            !dfs_file_system_semantically_eq(&first, &reversed),
            "duplicate path/scope order remains durable precedence state"
        );
        let first_policy = resolve_placement_policy(
            &DfsFileSystemConfig::from(&first),
            "/users/team/report.md",
            DfsObjectPolicy::ReplicatedEventual,
            2,
        )
        .expect("first duplicate ordering");
        let reversed_policy = resolve_placement_policy(
            &DfsFileSystemConfig::from(&reversed),
            "/users/team/report.md",
            DfsObjectPolicy::ReplicatedEventual,
            2,
        )
        .expect("reversed duplicate ordering");
        assert_ne!(
            first_policy.policy.consistency, reversed_policy.policy.consistency,
            "the fixture must prove why a stable canonical sort preserves duplicate order"
        );
    }

    #[test]
    fn filesystem_semantics_treat_replica_constraints_as_sets() {
        let owner = node("owner");
        let first_rule = DfsSubtreePolicy {
            path_prefix: "/users/team".to_string(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            scope: DfsPlacementScope::Subtree,
            min_commit_replicas: Some(1),
            desired_replicas: Some(3),
            required_node_ids: vec![node("node-b"), node("node-a"), node("node-a")],
            forbidden_node_ids: vec![node("node-d"), node("node-c")],
            min_distinct_zones: 0,
        };
        let reordered_rule = DfsSubtreePolicy {
            required_node_ids: vec![node("node-a"), node("node-b")],
            forbidden_node_ids: vec![node("node-c"), node("node-d"), node("node-d")],
            ..first_rule.clone()
        };
        let first = DfsFileSystem {
            metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: sifs_file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: vec![first_rule.clone()],
        };
        let reordered = DfsFileSystem {
            subtree_policies: vec![reordered_rule.clone()],
            ..first.clone()
        };

        assert!(dfs_file_system_semantically_eq(&first, &reordered));
        assert_eq!(
            placement_policy_from_rule(&first_rule, 2)
                .expect("first policy")
                .replication_policy(BTreeMap::new())
                .expect("first replication policy"),
            placement_policy_from_rule(&reordered_rule, 2)
                .expect("reordered policy")
                .replication_policy(BTreeMap::new())
                .expect("reordered replication policy"),
            "replica constraints are normalized as sets by the planner"
        );
    }

    #[test]
    fn filesystem_semantics_do_not_assume_path_aliases_are_equivalent() {
        let owner = node("owner");
        let canonical = DfsFileSystem {
            metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: sifs_file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: vec![DfsSubtreePolicy::legacy(
                "/users/team",
                DfsObjectPolicy::ReplicatedConfirmed,
            )],
        };
        let aliased = DfsFileSystem {
            subtree_policies: vec![DfsSubtreePolicy::legacy(
                "/users//team",
                DfsObjectPolicy::ReplicatedConfirmed,
            )],
            ..canonical.clone()
        };

        assert!(
            !dfs_file_system_semantically_eq(&canonical, &aliased),
            "legacy stored paths remain observable until they are explicitly migrated"
        );
        let canonical_policy = resolve_placement_policy(
            &DfsFileSystemConfig::from(&canonical),
            "/users/team/report.md",
            DfsObjectPolicy::ReplicatedEventual,
            2,
        )
        .expect("canonical policy");
        let aliased_policy = resolve_placement_policy(
            &DfsFileSystemConfig::from(&aliased),
            "/users/team/report.md",
            DfsObjectPolicy::ReplicatedEventual,
            2,
        )
        .expect("aliased policy");
        assert_ne!(
            canonical_policy.policy.consistency, aliased_policy.policy.consistency,
            "the resolver normalizes the lookup path, not historical stored rule paths"
        );
    }

    #[test]
    fn self_rule_overrides_same_path_subtree_without_affecting_children() {
        let mut config = DfsFileSystemConfig {
            metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("fs"),
            display_name: "sifs".to_string(),
            owner_node_id: node("owner"),
            subtree_policies: Vec::new(),
        };
        upsert_placement_rule(
            &mut config.subtree_policies,
            normalize_placement_rule(
                "/users/team",
                DfsPlacementScope::Subtree,
                DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 3)
                    .expect("subtree"),
            )
            .expect("rule"),
        );
        upsert_placement_rule(
            &mut config.subtree_policies,
            normalize_placement_rule(
                "/users/team",
                DfsPlacementScope::SelfOnly,
                DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).expect("self"),
            )
            .expect("rule"),
        );

        let selected = resolve_placement_policy(
            &config,
            "/users/team",
            DfsObjectPolicy::ReplicatedEventual,
            2,
        )
        .expect("selected");
        assert_eq!(
            selected.policy.consistency,
            DfsObjectPolicy::ReplicatedConfirmed
        );
        assert!(!selected.inherited);
        assert_eq!(selected.exact_policies.len(), 2);
        assert_eq!(selected.exact_policies[0].scope, DfsPlacementScope::Subtree);
        assert_eq!(selected.exact_policies[0].policy.desired_replicas, 3);
        assert_eq!(
            selected.exact_policies[1].scope,
            DfsPlacementScope::SelfOnly
        );
        assert_eq!(
            selected.exact_policies[1].policy.consistency,
            DfsObjectPolicy::ReplicatedConfirmed
        );

        let child = resolve_placement_policy(
            &config,
            "/users/team/report.md",
            DfsObjectPolicy::ReplicatedEventual,
            2,
        )
        .expect("child");
        assert_eq!(child.policy.desired_replicas, 3);
        assert!(child.inherited);
        assert!(child.exact_policies.is_empty());
    }

    #[test]
    fn missing_subtree_scope_falls_back_to_ancestor_not_same_path_self_rule() {
        let mut config = placement_config(node("owner"));
        for (path, scope, consistency, replicas) in [
            (
                "/users",
                DfsPlacementScope::Subtree,
                DfsObjectPolicy::ReplicatedEventual,
                2,
            ),
            (
                "/users/team",
                DfsPlacementScope::SelfOnly,
                DfsObjectPolicy::ReplicatedConfirmed,
                1,
            ),
        ] {
            upsert_placement_rule(
                &mut config.subtree_policies,
                normalize_placement_rule(
                    path,
                    scope,
                    DfsPlacementPolicy::new(consistency, 1, replicas).expect("policy"),
                )
                .expect("rule"),
            );
        }

        let resolved = resolve_placement_policy(
            &config,
            "/users/team",
            DfsObjectPolicy::ReplicatedEventual,
            1,
        )
        .expect("resolved policy");
        assert_eq!(
            resolved.policy.consistency,
            DfsObjectPolicy::ReplicatedConfirmed
        );
        let subtree = resolved
            .scope_resolutions
            .iter()
            .find(|resolution| resolution.scope == DfsPlacementScope::Subtree)
            .expect("subtree scope resolution");
        assert!(!subtree.exact);
        assert_eq!(subtree.source_path.as_deref(), Some("/users"));
        assert_eq!(subtree.source_scope, Some(DfsPlacementScope::Subtree));
        assert_eq!(subtree.policy.desired_replicas, 2);
        assert_eq!(
            subtree.policy.consistency,
            DfsObjectPolicy::ReplicatedEventual
        );
    }

    #[test]
    fn preview_honors_required_forbidden_and_zone_spread() {
        let fs = DfsFileSystemId::from_legacy_named("sifs").expect("fs");
        let config = DfsFileSystemConfig {
            metadata_format: crate::DfsRevisionMetadataFormat::NamespacedV6,
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: fs,
            display_name: "sifs".to_string(),
            owner_node_id: node("node-a"),
            subtree_policies: Vec::new(),
        };
        let mut policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 2, 3).expect("policy");
        policy.required_node_ids = vec![node("node-c")];
        policy.forbidden_node_ids = vec![node("node-d")];
        policy.min_distinct_zones = 2;
        let advertisements = [
            ("node-a", "east"),
            ("node-b", "east"),
            ("node-c", "west"),
            ("node-d", "north"),
        ]
        .into_iter()
        .map(|(node_id, zone)| DfsNodeAdvertisement {
            node_id: node(node_id),
            capacity_weight: 1,
            accepts_metadata: false,
            accepts_content: true,
            zone: Some(zone.to_string()),
            storage_health: crate::DfsHealthState::Unknown,
            storage_freshness: crate::DfsObservationFreshness::NeverSampled,
            usable_capacity_bytes: None,
            reserved_free_bytes: None,
            content_location_count: 1,
            network_metering: crate::DfsNetworkMetering::Unknown,
            resource_pressure: crate::DfsResourcePressure::Normal,
            advertisement_generation: 0,
            updated_at: time::OffsetDateTime::now_utc(),
            expires_at: None,
        })
        .collect();
        let preview = preview_placement(
            &config,
            "/users/team/report.md",
            DfsObjectId::from_legacy_named("object").expect("object"),
            DfsVersion::from_legacy_named("version").expect("version"),
            DfsObjectPolicy::ReplicatedEventual,
            node("node-a"),
            vec![node("node-a")],
            advertisements,
            Some(policy),
            DfsPlacementScope::SelfOnly,
        )
        .expect("preview");
        assert!(preview.is_satisfiable());
        assert!(
            preview
                .placement
                .plan
                .desired_replica_nodes
                .contains(&node("node-c"))
        );
        assert!(
            !preview
                .placement
                .plan
                .desired_replica_nodes
                .contains(&node("node-d"))
        );
        assert!(preview.placement.represented_zone_count >= 2);
        let required = preview
            .nodes
            .iter()
            .find(|candidate| candidate.node_id == node("node-c"))
            .expect("required node row");
        assert_eq!(required.zone.as_deref(), Some("west"));
        assert_eq!(required.capacity_weight, 1);
        assert!(required.accepts_content);
        assert!(required.required);
        assert!(required.desired_replica);
        assert!(required.explanation.contains("required"));

        let forbidden = preview
            .nodes
            .iter()
            .find(|candidate| candidate.node_id == node("node-d"))
            .expect("forbidden node row");
        assert!(forbidden.forbidden);
        assert!(!forbidden.desired_replica);
        assert!(forbidden.explanation.contains("forbidden"));

        let local = preview
            .nodes
            .iter()
            .find(|candidate| candidate.node_id == node("node-a"))
            .expect("local node row");
        assert_eq!(local.reachability, DfsPlacementNodeReachability::Local);
        assert!(local.current_replica);
    }

    #[test]
    fn preview_returns_best_effort_nodes_for_candidate_shortage() {
        let config = placement_config(node("node-a"));
        let policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 3).expect("policy");
        let preview = preview_with(
            &config,
            policy,
            vec![
                advertisement("node-a", "east"),
                advertisement("node-b", "west"),
            ],
        )
        .expect("best-effort preview");

        assert!(!preview.is_satisfiable());
        assert_eq!(preview.placement.plan.desired_replica_nodes.len(), 2);
        assert_eq!(preview.nodes.len(), 2);
        assert!(!preview.impact.complete);
        assert!(preview.blocking_diagnostics.contains(
            &DfsPlacementBlockingDiagnostic::InsufficientCandidates {
                available: 2,
                required: 3,
            }
        ));
    }

    #[test]
    fn preview_excludes_unavailable_optional_candidate_but_keeps_its_row() {
        let config = placement_config(node("node-a"));
        let policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).expect("policy");
        let candidates = vec![
            advertisement("node-a", "east"),
            advertisement("node-b", "west"),
            advertisement("node-c", "north"),
        ];
        let unavailable = BTreeSet::from([node("node-b")]);
        let preview = preview_placement_with_unavailable_nodes(
            &config,
            "/users/team/report.md",
            DfsObjectId::from_legacy_named("unavailable-object").expect("object"),
            DfsVersion::from_legacy_named("unavailable-version").expect("version"),
            DfsObjectPolicy::ReplicatedEventual,
            node("node-a"),
            vec![node("node-a")],
            candidates,
            Some(policy),
            DfsPlacementScope::SelfOnly,
            &unavailable,
        )
        .expect("preview");

        assert!(preview.is_satisfiable());
        assert_eq!(
            preview.placement.plan.desired_replica_nodes,
            vec![node("node-a"), node("node-c")]
        );
        let unavailable = preview
            .nodes
            .iter()
            .find(|candidate| candidate.node_id == node("node-b"))
            .expect("unavailable row retained");
        assert!(unavailable.accepts_content);
        assert_eq!(
            unavailable.reachability,
            DfsPlacementNodeReachability::Unreachable
        );
        assert!(!unavailable.desired_replica);
        assert!(unavailable.explanation.contains("unavailable"));
    }

    #[test]
    fn reconciliation_preserves_preview_unavailable_candidate_snapshot() {
        let owner = node("node-a");
        let path = "/users/team/report.md";
        let candidates = vec![
            advertisement("node-a", "east"),
            advertisement("node-b", "west"),
            advertisement("node-c", "north"),
        ];
        let policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2).expect("policy");
        let mut config = placement_config(owner.clone());
        upsert_placement_rule(
            &mut config.subtree_policies,
            normalize_placement_rule(path, DfsPlacementScope::SelfOnly, policy)
                .expect("placement rule"),
        );
        let object = placement_object(
            "reconciliation-unavailable",
            path,
            DfsObjectKind::File,
            owner.clone(),
            vec![owner.clone()],
            Some(64),
        );

        let baseline = preview_placement_with_unavailable_nodes(
            &config,
            path,
            object.object_id.clone(),
            object.version.clone(),
            object.policy.clone(),
            owner.clone(),
            object.current_locations.clone(),
            candidates.clone(),
            None,
            DfsPlacementScope::SelfOnly,
            &BTreeSet::new(),
        )
        .expect("baseline preview");
        let unavailable_node_id = baseline
            .placement
            .plan
            .desired_replica_nodes
            .iter()
            .find(|node_id| **node_id != owner)
            .cloned()
            .expect("baseline remote replica");
        let unavailable_node_ids = BTreeSet::from([unavailable_node_id.clone()]);
        let preview = preview_placement_with_unavailable_nodes(
            &config,
            path,
            object.object_id.clone(),
            object.version.clone(),
            object.policy.clone(),
            owner,
            object.current_locations.clone(),
            candidates.clone(),
            None,
            DfsPlacementScope::SelfOnly,
            &unavailable_node_ids,
        )
        .expect("filtered preview");
        assert!(preview.is_satisfiable());
        assert_eq!(preview.unavailable_node_ids(), unavailable_node_ids);
        assert!(
            !preview
                .placement
                .plan
                .desired_replica_nodes
                .contains(&unavailable_node_id)
        );

        let mut job = reconciliation_job(
            path,
            DfsPlacementScope::SelfOnly,
            preview.unavailable_node_ids(),
        );
        job.candidate_snapshot = candidates.clone();
        let (_, _, reconciliation) = reconciliation_placement_for_object(&config, &job, &object)
            .expect("reconciliation placement");
        assert_eq!(
            reconciliation.plan.desired_replica_nodes, preview.placement.plan.desired_replica_nodes,
            "the applied repair plan must preserve the preview's unavailable-node exclusion"
        );

        let mut legacy_job = reconciliation_job(path, DfsPlacementScope::SelfOnly, BTreeSet::new());
        legacy_job.candidate_snapshot = candidates;
        let (_, _, legacy_reconciliation) =
            reconciliation_placement_for_object(&config, &legacy_job, &object)
                .expect("legacy reconciliation placement");
        assert!(
            legacy_reconciliation
                .plan
                .desired_replica_nodes
                .contains(&unavailable_node_id),
            "the fixture must exercise the old divergence, not an incidental equivalent plan"
        );
    }

    #[test]
    fn reconciliation_uses_accepted_candidates_when_live_view_differs() {
        let owner = node("node-a");
        let job_path = "/team";
        let path = "/team/object.bin";
        let base = placement_config(owner.clone());
        let mut desired = base.clone();
        upsert_placement_rule(
            &mut desired.subtree_policies,
            normalize_placement_rule(
                job_path,
                DfsPlacementScope::Subtree,
                DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).expect("policy"),
            )
            .expect("rule"),
        );
        let accepted_candidates = vec![
            advertisement("node-a", "east"),
            advertisement("node-b", "west"),
        ];
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        let durable_job = control_plane.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv);
            store.put_file_system_config(&base).expect("base config");
            store
                .put_metadata_shard(&DfsMetadataShardConfig {
                    file_system_id: base.file_system_id.clone(),
                    shard_id: "root".to_string(),
                    primary_node_id: owner.clone(),
                    replica_node_ids: Vec::new(),
                    epoch: 1,
                    assignment: DfsMetadataShardAssignment::Root,
                    namespace_prepare_codec: Default::default(),
                    default_policy: DfsObjectPolicy::ReplicatedEventual,
                })
                .expect("root metadata shard");
            let mut job = reconciliation_job(job_path, DfsPlacementScope::Subtree, BTreeSet::new());
            job.job_id = "placement-stable-candidates".to_string();
            job.accepted_config = Some(desired.clone());
            job.candidate_snapshot = accepted_candidates.clone();
            assert!(
                store
                    .compare_and_put_placement_update(&base, &desired, &job)
                    .expect("placement update")
            );
            // The metadata primary now sees a different live candidate set.
            // This must not change the plan accepted by the previewing node.
            store
                .advertise_content_service(&advertisement("node-a", "east"))
                .expect("advertise local");
            store
                .advertise_content_service(&advertisement("node-c", "north"))
                .expect("advertise changed remote");
            store
                .placement_reconciliation(&job.file_system_id, &job.job_id)
                .expect("read durable job")
                .expect("durable job")
        });

        let object = placement_object(
            "stable-candidates",
            path,
            DfsObjectKind::File,
            owner.clone(),
            vec![owner.clone()],
            Some(64),
        );
        let root = sinomo_testdir::TestDir::new("dfs-placement-stable-candidates");
        let content = LocalContentStore::new(root.join("content")).expect("content store");
        let mut metadata =
            open_root_local_metadata_backend(root.join("metadata")).expect("metadata backend");
        metadata
            .put_file_system(&DfsFileSystem::from(desired))
            .expect("filesystem metadata");
        let mut service = NetworkedDfsService::new(
            PreviewTransport {
                local_node_id: owner,
            },
            content,
        )
        .with_control_plane(control_plane)
        .with_metadata_backend(metadata)
        .expect("attach metadata backend");
        service
            .add_object_metadata(object.clone())
            .expect("object metadata");
        let delayed_object = placement_object(
            "delayed-stable-candidates",
            "/team/delayed.bin",
            DfsObjectKind::File,
            node("node-a"),
            vec![node("node-a")],
            Some(32),
        );
        service
            .add_object_metadata(delayed_object.clone())
            .expect("delayed object metadata");
        let live_node_ids = service
            .placement_content_candidates()
            .expect("live candidates")
            .into_iter()
            .map(|candidate| candidate.node_id)
            .collect::<Vec<_>>();
        assert_eq!(live_node_ids, vec![node("node-a"), node("node-c")]);

        let accepted_config = durable_job
            .accepted_config
            .as_ref()
            .expect("accepted config");
        let (resolved, replication, placement) =
            reconciliation_placement_for_object(accepted_config, &durable_job, &object)
                .expect("accepted plan");
        assert_eq!(
            placement.plan.desired_replica_nodes,
            vec![node("node-a"), node("node-b")]
        );
        service
            .reconcile_object_placement_local(
                &durable_job.job_id,
                path,
                &object.object_id,
                &object.version,
                resolved.policy.consistency,
                replication,
                &placement.plan.desired_replica_nodes,
            )
            .expect("reconcile accepted plan");
        let intent_id = format!(
            "placement:{}:{}:{}",
            durable_job.job_id, object.object_id, object.version
        );
        let intent = service
            .coordinator
            .intent(&intent_id)
            .expect("placement intent");
        assert_eq!(
            intent.desired_replica_nodes,
            vec![node("node-a"), node("node-b")]
        );

        let (delayed_resolved, delayed_replication, delayed_placement) =
            reconciliation_placement_for_object(accepted_config, &durable_job, &delayed_object)
                .expect("delayed accepted plan");
        let newer_config = {
            let mut config = accepted_config.clone();
            let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1)
                .expect("newer policy");
            policy.forbidden_node_ids = vec![node("node-b")];
            upsert_placement_rule(
                &mut config.subtree_policies,
                normalize_placement_rule(job_path, DfsPlacementScope::Subtree, policy)
                    .expect("newer rule"),
            );
            config
        };
        let control_plane = service.control_plane.as_mut().expect("control plane");
        let mut store = DfsControlPlaneStore::new(control_plane.as_mut());
        let mut newer_job =
            reconciliation_job(job_path, DfsPlacementScope::Subtree, BTreeSet::new());
        newer_job.job_id = "placement-newer-policy".to_string();
        newer_job.created_at_unix_nanos = 2;
        newer_job.updated_at_unix_nanos = 2;
        newer_job.accepted_config = Some(newer_config.clone());
        newer_job.candidate_snapshot = vec![
            advertisement("node-a", "east"),
            advertisement("node-c", "north"),
        ];
        assert!(
            store
                .compare_and_put_placement_update(accepted_config, &newer_config, &newer_job)
                .expect("newer placement update")
        );

        // A routed operation computed before the newer atomic update must not
        // mutate metadata or create a placement intent after it arrives.
        service
            .reconcile_object_placement_local(
                &durable_job.job_id,
                &delayed_object.path,
                &delayed_object.object_id,
                &delayed_object.version,
                delayed_resolved.policy.consistency,
                delayed_replication,
                &delayed_placement.plan.desired_replica_nodes,
            )
            .expect("ignore delayed stale operation");
        let delayed_current = service
            .current_read_object_metadata(&delayed_object.object_id)
            .expect("delayed object");
        assert_eq!(delayed_current.policy, DfsObjectPolicy::ReplicatedEventual);
        let delayed_intent_id = format!(
            "placement:{}:{}:{}",
            durable_job.job_id, delayed_object.object_id, delayed_object.version
        );
        assert!(service.coordinator.intent(&delayed_intent_id).is_none());

        // A successful repair response that was already in flight for the old
        // policy is fenced before a durable receipt or location can be added.
        let content_hash = object.content_hash.clone().expect("content hash");
        let event = service
            .handle_repair_replica_response(
                node("node-b"),
                DfsRepairReplicaResponse {
                    request_id: "late-placement-repair".to_string(),
                    intent_id: intent_id.clone(),
                    file_system_id: object.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    replica_node_id: node("node-b"),
                    result: DfsBlobTransferResult::Durable {
                        content_hash: content_hash.clone(),
                    },
                },
            )
            .expect("late repair response");
        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::RepairRejected { .. }
        ));
        assert_eq!(
            service
                .coordinator
                .intent(&intent_id)
                .map(|intent| intent.state.clone()),
            Some(DfsWriteIntentState::Committed)
        );
        assert!(
            service
                .coordinator
                .repair_jobs()
                .iter()
                .all(|job| job.intent_id != intent_id)
        );
        assert!(
            service
                .backend_receipt_for_replica(&object.object_id, &object.version, &node("node-b"))
                .expect("receipt lookup")
                .is_none()
        );
        assert!(
            !service
                .current_read_object_metadata(&object.object_id)
                .expect("current object")
                .current_locations
                .contains(&node("node-b"))
        );
        let durable_intent = service
            .metadata_backend
            .as_ref()
            .expect("metadata backend")
            .get_intent(&object.object_id, &intent_id)
            .expect("intent lookup")
            .expect("durable intent");
        assert_eq!(durable_intent.status, DfsWriteIntentStatus::Committed);
    }

    #[test]
    fn legacy_reconciliation_is_read_only_and_never_scanned() {
        let owner = node("node-a");
        let base = placement_config(owner.clone());
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .put_file_system_config(&base)
                .expect("base config");
            let mut job = reconciliation_job("/team", DfsPlacementScope::Subtree, BTreeSet::new());
            job.job_id = "placement-legacy-snapshots".to_string();
            let mut legacy = serde_json::to_value(&job).expect("legacy job value");
            let object = legacy.as_object_mut().expect("legacy job object");
            object.remove("schema_version");
            object.remove("unavailable_node_ids");
            object.remove("candidate_snapshot");
            object.remove("placement_snapshot");
            object.remove("adaptive");
            object.remove("adaptive_budget");
            object.remove("processed_content_bytes");
            object.remove("accepted_config");
            let key = format!(
                "/dfs/filesystems/{}/placement-reconciliations/{}",
                job.file_system_id, job.job_id
            );
            InProcessKvStore::put_serde(kv, key, &legacy, None).expect("seed legacy job");
        });

        let root = sinomo_testdir::TestDir::new("dfs-placement-legacy-snapshots");
        let content = LocalContentStore::new(root.join("content")).expect("content store");
        let mut service = NetworkedDfsService::new(
            PreviewTransport {
                local_node_id: owner,
            },
            content,
        )
        .with_control_plane(control_plane.clone());
        assert!(
            service
                .run_placement_reconciliation_bounded(1)
                .expect("legacy scan gate")
                .is_none(),
            "a new runner must not migrate or execute a snapshotless legacy job"
        );
        control_plane.with_client(|kv| {
            let legacy = DfsControlPlaneStore::new(kv)
                .placement_reconciliation(&sifs_file_system_id(), "placement-legacy-snapshots")
                .expect("legacy lookup")
                .expect("legacy job");
            assert_eq!(legacy.schema_version, 0);
            assert_eq!(legacy.revision, 0);
            assert!(legacy.accepted_config.is_none());
            assert!(legacy.candidate_snapshot.is_empty());
        });
    }

    #[test]
    fn placement_reconciliation_is_idle_before_filesystem_config_publication() {
        let owner = node("node-a");
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        let root = sinomo_testdir::TestDir::new("dfs-placement-before-config-publication");
        let content = LocalContentStore::new(root.join("content")).expect("content store");
        let mut service = NetworkedDfsService::new(
            PreviewTransport {
                local_node_id: owner,
            },
            content,
        )
        .with_control_plane(control_plane.clone());

        assert!(
            service
                .run_placement_reconciliation_bounded(1)
                .expect("pre-bootstrap placement scan")
                .is_none(),
            "placement must stay idle until its filesystem config is published"
        );

        control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .put_file_system_config(&placement_config(node("node-a")))
                .expect("publish filesystem config");
        });
        assert!(
            service
                .run_placement_reconciliation_bounded(1)
                .expect("post-bootstrap placement scan")
                .is_none(),
            "a later config publication must make placement maintenance usable"
        );
    }

    #[test]
    fn placement_reconciliation_rejects_orphaned_v1_state_without_a_config() {
        let owner = node("node-a");
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        control_plane.with_client(|kv| {
            InProcessKvStore::put_serde(
                kv,
                crate::placement_reconciliation_key(&sifs_file_system_id(), "orphaned-job"),
                &"orphaned-v1-placement-state",
                None,
            )
            .expect("seed orphaned v1 state");
        });
        let root = sinomo_testdir::TestDir::new("dfs-placement-orphaned-v1-state");
        let content = LocalContentStore::new(root.join("content")).expect("content store");
        let mut service = NetworkedDfsService::new(
            PreviewTransport {
                local_node_id: owner,
            },
            content,
        )
        .with_control_plane(control_plane);

        let error = service
            .run_placement_reconciliation_bounded(1)
            .expect_err("orphaned managed state must not look like clean bootstrap");
        assert!(
            error
                .to_string()
                .contains("state exists without filesystem config")
        );
    }

    #[test]
    fn placement_preview_blocks_daemons_without_v1_capability() {
        let now = time::OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::Id::from_legacy_named("placement-cluster").expect("cluster id");
        let local_node_id = node("node-new");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "placement cluster",
            local_node_id.clone(),
            "new node",
            "new-public-key",
            now,
        );
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        assert!(placement_reconciliation_compatibility_diagnostic(&state).is_none());

        let mut legacy = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            node("node-old"),
            "old node",
            "old-public-key",
            now,
        );
        legacy.capabilities = vec![
            sinomo_core::CapabilityName::new(sinomo_core::node::DAEMON_MAIN_RUN_CAPABILITY)
                .expect("main capability"),
        ];
        legacy.allowed_capabilities = legacy.capabilities.clone();
        state.members.push(legacy);

        let diagnostic = placement_reconciliation_compatibility_diagnostic(&state)
            .expect("legacy daemon blocker");
        assert_eq!(
            diagnostic,
            DfsPlacementBlockingDiagnostic::IncompatibleDaemonVersion {
                node_ids: vec![node("node-old")],
            }
        );
        assert!(diagnostic.message().contains("upgrade or restart"));

        let mut shadow = sinomo_core::ClusterMemberRecord::local_non_voting(
            cluster_id,
            node("node-shadow"),
            "shadow node",
            "shadow-public-key",
            now,
        );
        shadow.capabilities = sinomo_core::node::shadow_daemon_capabilities();
        shadow.allowed_capabilities = shadow.capabilities.clone();
        state.members.push(shadow);
        assert_eq!(
            placement_reconciliation_compatibility_diagnostic(&state),
            Some(diagnostic),
            "a legacy shadow process does not host placement reconciliation"
        );
    }

    #[test]
    fn placement_namespace_operation_uses_a_legacy_unknown_v1_wire_tag() {
        #[allow(dead_code)]
        #[derive(Deserialize)]
        #[serde(rename_all = "kebab-case")]
        enum LegacyNamespaceOperation {
            ReconcilePlacement(serde_json::Value),
        }

        let operation = DfsNamespaceOperation::ReconcilePlacement {
            job_id: "placement-v1-wire".to_string(),
            path: "/team/object.bin".to_string(),
            object_id: DfsObjectId::from_legacy_named("placement-v1-object").expect("object"),
            expected_version: DfsVersion::from_legacy_named("placement-v1-version")
                .expect("version"),
            object_policy: DfsObjectPolicy::ReplicatedEventual,
            replication_policy: DfsReplicationPolicy::new(1, 1).expect("replication"),
            desired_replica_node_ids: vec![node("node-new")],
        };
        let value = serde_json::to_value(&operation).expect("serialize operation");
        assert!(value.get("reconcile-placement-v1").is_some());
        assert!(
            serde_json::from_value::<LegacyNamespaceOperation>(value).is_err(),
            "a legacy metadata primary must reject the v1 operation before execution"
        );
    }

    #[test]
    fn managed_preview_uses_file_system_owner_as_stable_seed() {
        let config = placement_config(node("node-b"));
        let policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).expect("policy");
        let candidates = vec![
            advertisement("node-a", "east"),
            advertisement("node-b", "west"),
            advertisement("node-c", "north"),
        ];
        let preview_from = |observer: &str| {
            preview_placement(
                &config,
                "/stable.bin",
                DfsObjectId::from_legacy_named("stable-object").expect("object"),
                DfsVersion::from_legacy_named("stable-version").expect("version"),
                DfsObjectPolicy::ReplicatedEventual,
                node(observer),
                vec![node("node-a")],
                candidates.clone(),
                Some(policy.clone()),
                DfsPlacementScope::SelfOnly,
            )
            .expect("stable preview")
        };

        let from_a = preview_from("node-a");
        let from_c = preview_from("node-c");
        assert_eq!(
            from_a.placement.plan.desired_replica_nodes,
            from_c.placement.plan.desired_replica_nodes
        );
        assert!(
            from_a
                .placement
                .plan
                .desired_replica_nodes
                .contains(&config.owner_node_id)
        );
        assert_eq!(
            from_a
                .nodes
                .iter()
                .find(|candidate| candidate.node_id == node("node-a"))
                .expect("observer a")
                .reachability,
            DfsPlacementNodeReachability::Local
        );
        assert_eq!(
            from_c
                .nodes
                .iter()
                .find(|candidate| candidate.node_id == node("node-c"))
                .expect("observer c")
                .reachability,
            DfsPlacementNodeReachability::Local
        );
    }

    #[test]
    fn same_path_self_rule_cannot_mask_unsatisfiable_subtree_proposal() {
        let mut config = placement_config(node("node-a"));
        for (scope, replicas) in [
            (DfsPlacementScope::Subtree, 3),
            (DfsPlacementScope::SelfOnly, 1),
        ] {
            upsert_placement_rule(
                &mut config.subtree_policies,
                normalize_placement_rule(
                    "/team",
                    scope,
                    DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, replicas)
                        .expect("policy"),
                )
                .expect("rule"),
            );
        }

        // CLI/daemon pass an already-materialized desired config and None here.
        let preview = preview_placement(
            &config,
            "/team",
            DfsObjectId::from_legacy_named("team-object").expect("object"),
            DfsVersion::from_legacy_named("team-version").expect("version"),
            DfsObjectPolicy::ReplicatedEventual,
            node("node-a"),
            Vec::new(),
            vec![
                advertisement("node-a", "east"),
                advertisement("node-b", "west"),
            ],
            None,
            DfsPlacementScope::Subtree,
        )
        .expect("best-effort preview");

        assert_eq!(
            preview.resolved.source_scope,
            Some(DfsPlacementScope::SelfOnly)
        );
        assert!(!preview.is_satisfiable());
        assert!(!preview.impact.complete);
        assert!(preview.blocking_diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic,
                DfsPlacementBlockingDiagnostic::ScopedPolicy {
                    path,
                    scope: DfsPlacementScope::Subtree,
                    diagnostic,
                } if path == "/team" && matches!(
                    diagnostic.as_ref(),
                    DfsPlacementBlockingDiagnostic::InsufficientCandidates {
                        available: 2,
                        required: 3,
                    }
                )
            )
        }));
    }

    #[test]
    fn preview_reports_unavailable_required_node_and_keeps_its_row() {
        let config = placement_config(node("node-a"));
        let mut policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).expect("policy");
        policy.required_node_ids = vec![node("node-c")];
        let preview = preview_with(
            &config,
            policy,
            vec![
                advertisement("node-a", "east"),
                advertisement("node-b", "west"),
            ],
        )
        .expect("best-effort preview");

        assert!(preview.blocking_diagnostics.contains(
            &DfsPlacementBlockingDiagnostic::RequiredNodeUnavailable {
                node_id: node("node-c"),
            }
        ));
        let required = preview
            .nodes
            .iter()
            .find(|candidate| candidate.node_id == node("node-c"))
            .expect("synthetic required node row");
        assert!(required.required);
        assert!(!required.accepts_content);
        assert!(!required.desired_replica);
        assert_eq!(required.capacity_weight, 0);
        assert!(required.explanation.contains("unavailable"));
    }

    #[test]
    fn preview_reports_insufficient_zones_without_losing_candidates() {
        let config = placement_config(node("node-a"));
        let mut policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).expect("policy");
        policy.min_distinct_zones = 2;
        let preview = preview_with(
            &config,
            policy,
            vec![
                advertisement("node-a", "east"),
                advertisement("node-b", "east"),
            ],
        )
        .expect("best-effort preview");

        assert_eq!(preview.nodes.len(), 2);
        assert_eq!(preview.placement.plan.desired_replica_nodes.len(), 2);
        assert!(preview.blocking_diagnostics.contains(
            &DfsPlacementBlockingDiagnostic::InsufficientZones {
                available: 1,
                required: 2,
            }
        ));
    }

    #[test]
    fn preview_reports_zone_shortage_when_required_nodes_consume_replica_slots() {
        let config = placement_config(node("node-a"));
        let mut policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).expect("policy");
        policy.required_node_ids = vec![node("node-a"), node("node-b")];
        policy.min_distinct_zones = 2;
        let preview = preview_with(
            &config,
            policy,
            vec![
                advertisement("node-a", "east"),
                advertisement("node-b", "east"),
                advertisement("node-c", "west"),
            ],
        )
        .expect("best-effort preview");

        assert!(!preview.is_satisfiable());
        assert_eq!(
            preview.placement.plan.desired_replica_nodes,
            vec![node("node-a"), node("node-b")]
        );
        assert_eq!(preview.placement.represented_zone_count, 1);
        assert!(preview.blocking_diagnostics.contains(
            &DfsPlacementBlockingDiagnostic::InsufficientZones {
                available: 1,
                required: 2,
            }
        ));
        assert_eq!(preview.nodes.len(), 3);
    }

    #[test]
    fn preview_still_rejects_structurally_invalid_policy() {
        let config = placement_config(node("node-a"));
        let invalid = DfsPlacementPolicy {
            consistency: DfsObjectPolicy::ReplicatedEventual,
            min_commit_replicas: 2,
            desired_replicas: 1,
            required_node_ids: Vec::new(),
            forbidden_node_ids: Vec::new(),
            min_distinct_zones: 0,
        };
        let error = preview_with(&config, invalid, vec![advertisement("node-a", "east")])
            .expect_err("invalid policy");
        assert!(matches!(error, DfsDistributedError::InvalidPolicy { .. }));
    }

    #[test]
    fn placement_preview_new_fields_default_when_deserializing_old_payloads() {
        let config = placement_config(node("node-a"));
        let policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).expect("policy");
        let preview =
            preview_with(&config, policy, vec![advertisement("node-a", "east")]).expect("preview");
        let mut value = serde_json::to_value(preview).expect("serialize preview");
        let object = value.as_object_mut().expect("preview object");
        object.remove("schema_version");
        object.remove("preview_token");
        object.remove("nodes");
        object.remove("blocking_diagnostics");
        object.remove("impact");
        let resolved = object
            .get_mut("resolved")
            .and_then(serde_json::Value::as_object_mut)
            .expect("resolved placement");
        resolved.remove("exact_policies");
        resolved.remove("scope_resolutions");

        let decoded: DfsPlacementPreview =
            serde_json::from_value(value).expect("deserialize old preview");
        assert_eq!(decoded.schema_version, 0);
        assert!(!decoded.supports_safe_apply());
        assert!(decoded.nodes.is_empty());
        assert!(!decoded.is_satisfiable());
        assert_eq!(decoded.impact, DfsPlacementImpact::default());
        assert!(decoded.resolved.exact_policies.is_empty());
        assert!(decoded.resolved.scope_resolutions.is_empty());

        let mut legacy_impact = serde_json::to_value(DfsPlacementImpact::default())
            .expect("serialize placement impact");
        legacy_impact
            .as_object_mut()
            .expect("impact object")
            .remove("complete");
        let decoded: DfsPlacementImpact =
            serde_json::from_value(legacy_impact).expect("deserialize legacy impact");
        assert!(decoded.complete);
    }

    #[test]
    fn placement_preview_token_ignores_heartbeats_but_fences_semantic_candidates() {
        let config = placement_config(node("node-a"));
        let policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).expect("policy");
        let candidates = vec![
            advertisement("node-a", "east"),
            advertisement("node-b", "west"),
        ];
        let first = preview_with(&config, policy.clone(), candidates.clone()).expect("preview");
        assert!(first.preview_token.is_well_formed());
        assert!(first.supports_safe_apply());

        let mut heartbeat_only = candidates.clone();
        for candidate in &mut heartbeat_only {
            candidate.updated_at += time::Duration::minutes(5);
        }
        let heartbeat =
            preview_with(&config, policy.clone(), heartbeat_only).expect("heartbeat preview");
        assert_eq!(first.preview_token, heartbeat.preview_token);

        let mut changed_candidates = candidates;
        changed_candidates[1].capacity_weight = 2;
        let changed =
            preview_with(&config, policy, changed_candidates).expect("changed candidate preview");
        assert_ne!(first.preview_token, changed.preview_token);
    }

    fn authorization_preview_fixture() -> (
        DfsFileSystemConfig,
        DfsFileSystemConfig,
        DfsPlacementPreviewObjectFence,
        Vec<DfsNodeAdvertisement>,
        DfsPlacementPreview,
    ) {
        let config = placement_config(node("node-a"));
        let policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
        let candidates = vec![
            advertisement("node-a", "east"),
            advertisement("node-b", "west"),
            advertisement("node-c", "central"),
        ];
        let preview = preview_with(&config, policy.clone(), candidates.clone()).unwrap();
        let desired = placement_config_with_override(
            &config,
            "/users/team/report.md",
            DfsPlacementScope::SelfOnly,
            Some(policy),
        )
        .unwrap();
        let object = DfsPlacementPreviewObjectFence {
            file_system_id: config.file_system_id.clone(),
            object_id: DfsObjectId::from_legacy_named("preview-object").unwrap(),
            path: "/users/team/report.md".into(),
            kind: None,
            version: DfsVersion::from_legacy_named("preview-version").unwrap(),
            content_hash: None,
            size_bytes: None,
            changed_at_unix_nanos: 0,
            policy: DfsObjectPolicy::ReplicatedEventual,
            current_locations: vec![node("node-a")],
        };
        (config, desired, object, candidates, preview)
    }

    #[test]
    fn placement_preview_token_ignores_diagnostic_telemetry_with_the_same_authorized_plan() {
        let (config, desired, object, candidates, preview) = authorization_preview_fixture();
        let mut changed_candidates = candidates.clone();
        for candidate in &mut changed_candidates {
            candidate.updated_at += time::Duration::seconds(2);
            candidate.advertisement_generation += 1;
            candidate.usable_capacity_bytes = Some(123_456);
            candidate.reserved_free_bytes = Some(65_536);
        }
        // Recompute through the existing selector; diagnostic updates cannot
        // replace candidate eligibility or the actual chosen replication plan.
        let policy = preview.resolved.policy.clone();
        let mut changed = preview_with(&config, policy, changed_candidates.clone()).unwrap();
        assert_eq!(changed.placement.plan, preview.placement.plan);
        assert_ne!(changed.nodes, preview.nodes);
        changed.placement.snapshot_generation = Some(9_999);
        changed.placement.snapshot_observed_at = Some(time::OffsetDateTime::now_utc());
        for decision in &mut changed.placement.decisions {
            decision.path_latency_micros = Some(999);
            decision.path_throughput_bytes_per_second = Some(1_234);
            decision.usable_capacity_bytes = Some(123_456);
            decision.storage_freshness = Some(crate::DfsObservationFreshness::Stale);
            decision.observation_freshness = Some(crate::DfsObservationFreshness::Stale);
            decision.reason = "display-only sampled cost changed".into();
        }
        for node in &mut changed.nodes {
            node.path_latency_micros = Some(999);
            node.path_throughput_bytes_per_second = Some(1_234);
            node.storage_freshness = Some(crate::DfsObservationFreshness::Stale);
            node.observation_freshness = Some(crate::DfsObservationFreshness::Stale);
            node.explanation = "display-only sampled cost changed".into();
        }
        let token = bind_placement_preview_token(
            &mut changed,
            &config,
            &desired,
            &object.path,
            DfsPlacementScope::SelfOnly,
            &object,
            &changed_candidates,
            &BTreeSet::new(),
        )
        .unwrap();
        assert_eq!(token, preview.preview_token);
    }

    #[test]
    fn placement_preview_token_fences_selected_plan_eligibility_and_membership() {
        let (config, desired, object, candidates, preview) = authorization_preview_fixture();
        let token_for = |mut changed: DfsPlacementPreview, candidates: &[DfsNodeAdvertisement]| {
            bind_placement_preview_token(
                &mut changed,
                &config,
                &desired,
                &object.path,
                DfsPlacementScope::SelfOnly,
                &object,
                candidates,
                &BTreeSet::new(),
            )
            .unwrap()
        };
        let mut selected = preview.clone();
        selected.placement.plan.desired_replica_nodes.swap(0, 1);
        assert_ne!(token_for(selected, &candidates), preview.preview_token);
        let mut preferred = preview.clone();
        preferred.placement.plan.preferred_commit_nodes = vec![node("node-c")];
        assert_ne!(token_for(preferred, &candidates), preview.preview_token);
        let mut eligibility = preview.clone();
        eligibility.placement.decisions[0].eligible = false;
        assert_ne!(token_for(eligibility, &candidates), preview.preview_token);
        let mut membership = candidates.clone();
        membership[0].accepts_content = false;
        assert_ne!(
            token_for(preview.clone(), &membership),
            preview.preview_token
        );
        let mut incompatible = preview.clone();
        incompatible.blocking_diagnostics.push(
            DfsPlacementBlockingDiagnostic::IncompatibleDaemonVersion {
                node_ids: vec![node("node-c")],
            },
        );
        assert_ne!(token_for(incompatible, &candidates), preview.preview_token);
    }

    #[test]
    fn placement_preview_token_fences_reviewed_configuration_and_subtree_object_changes() {
        let (config, desired, object, candidates, preview) = authorization_preview_fixture();
        let token_for = |mut changed: DfsPlacementPreview,
                         config: &DfsFileSystemConfig,
                         desired: &DfsFileSystemConfig,
                         object: &DfsPlacementPreviewObjectFence| {
            bind_placement_preview_token(
                &mut changed,
                config,
                desired,
                &object.path,
                DfsPlacementScope::SelfOnly,
                object,
                &candidates,
                &BTreeSet::new(),
            )
            .unwrap()
        };
        let mut changed_config = config.clone();
        changed_config.owner_node_id = node("new-config-owner");
        assert_ne!(
            token_for(preview.clone(), &changed_config, &desired, &object),
            preview.preview_token
        );
        let mut changed_policy = desired.clone();
        changed_policy.subtree_policies[0].min_commit_replicas = Some(2);
        assert_ne!(
            token_for(preview.clone(), &config, &changed_policy, &object),
            preview.preview_token
        );
        let mut changed_object = object.clone();
        changed_object.version = DfsVersion::from_legacy_named("changed-preview-version").unwrap();
        assert_ne!(
            token_for(preview.clone(), &config, &desired, &changed_object),
            preview.preview_token
        );
        let mut changed_impact = preview.clone();
        changed_impact.impact.metadata_snapshot_digest =
            "different-subtree-metadata-snapshot".into();
        assert_ne!(
            token_for(changed_impact, &config, &desired, &object),
            preview.preview_token
        );
    }

    #[test]
    fn placement_preview_token_fences_descendant_snapshot_plans_with_an_unchanged_root_plan() {
        let root = sinomo_testdir::TestDir::new("preview-descendant-plan-fence");
        let owner = node("node-a");
        let candidates = vec![
            advertisement("node-a", "east"),
            advertisement("node-b", "west"),
            advertisement("node-c", "central"),
            advertisement("node-d", "central"),
        ];
        let mut config = placement_config(owner.clone());
        let mut root_policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
        root_policy.required_node_ids = vec![node("node-a"), node("node-b")];
        upsert_placement_rule(
            &mut config.subtree_policies,
            normalize_placement_rule("/team", DfsPlacementScope::SelfOnly, root_policy).unwrap(),
        );
        let mut subtree_policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
        subtree_policy.forbidden_node_ids = vec![node("node-b")];
        upsert_placement_rule(
            &mut config.subtree_policies,
            normalize_placement_rule("/team", DfsPlacementScope::Subtree, subtree_policy).unwrap(),
        );
        let directory = placement_object(
            "preview-directory",
            "/team",
            DfsObjectKind::Directory,
            owner.clone(),
            vec![owner.clone()],
            None,
        );
        let child = placement_object(
            "preview-child",
            "/team/child.bin",
            DfsObjectKind::File,
            owner.clone(),
            vec![owner.clone()],
            Some(10),
        );
        let mut service = NetworkedDfsService::new(
            PreviewTransport {
                local_node_id: owner.clone(),
            },
            LocalContentStore::new(root.join("content")).unwrap(),
        );
        service
            .add_object_metadata_batch(vec![directory.clone(), child.clone()])
            .unwrap();
        let now = time::OffsetDateTime::now_utc();
        let mut snapshot = crate::DfsPlacementSnapshot::empty(owner.clone());
        snapshot.nodes = candidates
            .iter()
            .map(|candidate| {
                let mut observation =
                    crate::DfsPlacementNodeObservation::from_advertisement(candidate);
                observation.storage_health = crate::DfsHealthState::Healthy;
                observation.storage_freshness = crate::DfsObservationFreshness::Fresh;
                observation.expires_at = now + time::Duration::hours(1);
                observation
            })
            .collect();
        snapshot.paths = candidates
            .iter()
            .map(|candidate| crate::DfsPlacementPathObservation {
                source_node_id: owner.clone(),
                target_node_id: candidate.node_id.clone(),
                state: crate::DfsPlacementPathState::Healthy,
                latency_micros: Some(if candidate.node_id == node("node-d") {
                    1_000
                } else {
                    100
                }),
                delivered_bytes_per_second: Some(1_000_000),
                metering: crate::DfsNetworkMetering::Unmetered,
                observed_at: now,
                expires_at: now + time::Duration::hours(1),
            })
            .collect();
        let fence = DfsPlacementPreviewObjectFence {
            file_system_id: directory.file_system_id.clone(),
            object_id: directory.object_id.clone(),
            path: directory.path.clone(),
            kind: Some(directory.kind.clone()),
            version: directory.version.clone(),
            content_hash: directory.content_hash.clone(),
            size_bytes: directory.content_size_bytes,
            changed_at_unix_nanos: directory.changed_at_unix_nanos,
            policy: directory.policy.clone(),
            current_locations: directory.current_locations.clone(),
        };
        let preview = |snapshot: &crate::DfsPlacementSnapshot| {
            let mut preview = preview_placement_with_snapshot_and_unavailable_nodes(
                &config,
                &directory.path,
                directory.object_id.clone(),
                directory.version.clone(),
                directory.policy.clone(),
                owner.clone(),
                directory.current_locations.clone(),
                candidates.clone(),
                None,
                DfsPlacementScope::Subtree,
                Some(snapshot),
                &[],
                &BTreeSet::new(),
            )
            .unwrap();
            preview.impact = DfsPlacementImpact::default();
            let mut cursor = None;
            loop {
                let page = service
                    .placement_impact_page_for_config_with_snapshot_and_unavailable_nodes(
                        &config,
                        "/team",
                        DfsPlacementScope::Subtree,
                        &candidates,
                        cursor.as_ref(),
                        1,
                        &BTreeSet::new(),
                        Some(snapshot),
                    )
                    .unwrap();
                merge_placement_impact(&mut preview.impact, &page.impact);
                cursor = page.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }
            bind_placement_preview_token(
                &mut preview,
                &config,
                &config,
                "/team",
                DfsPlacementScope::Subtree,
                &fence,
                &candidates,
                &BTreeSet::new(),
            )
            .unwrap();
            preview
        };
        let before = preview(&snapshot);
        assert_eq!(
            before.placement.plan.desired_replica_nodes,
            vec![node("node-a"), node("node-b")]
        );
        let mut changed = snapshot.clone();
        changed.generation += 1;
        for path in &mut changed.paths {
            if path.target_node_id == node("node-c") {
                path.latency_micros = Some(2_000);
            }
        }
        let after = preview(&changed);
        assert_eq!(
            before.placement.plan, after.placement.plan,
            "SelfOnly required [a,b] root stays fixed"
        );
        assert_eq!(before.impact.copies_added, after.impact.copies_added);
        assert_eq!(
            before.impact.affected_object_count,
            after.impact.affected_object_count
        );
        assert_ne!(
            before.impact.metadata_snapshot_digest, after.impact.metadata_snapshot_digest,
            "descendant [a,c] to [a,d] selection must be fenced despite equal counts and root plan"
        );
        assert_ne!(before.preview_token, after.preview_token);
        let mut job = reconciliation_job("/team", DfsPlacementScope::Subtree, BTreeSet::new());
        job.candidate_snapshot = candidates.clone();
        job.placement_snapshot = Some(snapshot.clone());
        let (_, _, child_before) =
            reconciliation_placement_for_object_with_pins(&config, &job, &child, &[]).unwrap();
        job.placement_snapshot = Some(changed);
        let (_, _, child_after) =
            reconciliation_placement_for_object_with_pins(&config, &job, &child, &[]).unwrap();
        assert_eq!(
            child_before.plan.desired_replica_nodes,
            vec![node("node-a"), node("node-c")]
        );
        assert_eq!(
            child_after.plan.desired_replica_nodes,
            vec![node("node-a"), node("node-d")]
        );
        let mut telemetry_only = snapshot;
        telemetry_only.generation += 99;
        telemetry_only.observed_at += time::Duration::seconds(1);
        for path in &mut telemetry_only.paths {
            path.latency_micros = path.latency_micros.map(|latency| latency + 1);
        }
        let unchanged = preview(&telemetry_only);
        assert_eq!(before.placement.plan, unchanged.placement.plan);
        assert_eq!(before.impact, unchanged.impact);
        assert_eq!(
            before.preview_token, unchanged.preview_token,
            "telemetry may change only while every actual scoped plan stays fixed"
        );
    }

    #[test]
    fn legacy_placement_decision_cannot_supply_eligibility_authority() {
        let (_, _, _, _, preview) = authorization_preview_fixture();
        assert!(
            preview
                .placement
                .decisions
                .iter()
                .all(|decision| decision.eligible)
        );
        let mut legacy = serde_json::to_value(&preview.placement.decisions[0]).unwrap();
        legacy.as_object_mut().unwrap().remove("eligible");
        let decoded: crate::DfsReplicaPlacementDecision = serde_json::from_value(legacy).unwrap();
        assert!(
            !decoded.eligible,
            "only a newly computed server decision proves eligibility"
        );
    }

    #[test]
    fn placement_preview_token_fences_final_impact() {
        let config = placement_config(node("node-a"));
        let policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).expect("policy");
        let candidates = vec![advertisement("node-a", "east")];
        let preview = preview_with(&config, policy.clone(), candidates.clone()).expect("preview");
        let desired = placement_config_with_override(
            &config,
            "/users/team/report.md",
            DfsPlacementScope::SelfOnly,
            Some(policy),
        )
        .expect("desired config");
        let object = DfsPlacementPreviewObjectFence {
            file_system_id: config.file_system_id.clone(),
            object_id: DfsObjectId::from_legacy_named("preview-object").expect("object"),
            path: "/users/team/report.md".to_string(),
            kind: None,
            version: DfsVersion::from_legacy_named("preview-version").expect("version"),
            content_hash: None,
            size_bytes: None,
            changed_at_unix_nanos: 0,
            policy: DfsObjectPolicy::ReplicatedEventual,
            current_locations: vec![node("node-a")],
        };
        let unavailable = BTreeSet::new();

        let mut same = preview.clone();
        let same_token = bind_placement_preview_token(
            &mut same,
            &config,
            &desired,
            "/users/team/report.md",
            DfsPlacementScope::SelfOnly,
            &object,
            &candidates,
            &unavailable,
        )
        .expect("rebind preview");
        assert_eq!(preview.preview_token, same_token);

        same.impact.affected_object_count = same.impact.affected_object_count.saturating_add(1);
        let changed_token = bind_placement_preview_token(
            &mut same,
            &config,
            &desired,
            "/users/team/report.md",
            DfsPlacementScope::SelfOnly,
            &object,
            &candidates,
            &unavailable,
        )
        .expect("rebind changed impact");
        assert_ne!(preview.preview_token, changed_token);
    }

    #[test]
    fn reconciliation_job_unavailable_snapshot_defaults_for_legacy_payload() {
        let mut job = reconciliation_job(
            "/legacy",
            DfsPlacementScope::Subtree,
            BTreeSet::from([node("node-b")]),
        );
        job.accepted_config = Some(placement_config(node("node-a")));
        let mut value = serde_json::to_value(job).expect("serialize reconciliation job");
        let object = value.as_object_mut().expect("job object");
        object.remove("schema_version");
        object.remove("unavailable_node_ids");
        object.remove("candidate_snapshot");
        object.remove("placement_snapshot");
        object.remove("adaptive");
        object.remove("adaptive_budget");
        object.remove("processed_content_bytes");
        object.remove("accepted_config");

        let decoded: DfsPlacementReconciliationJob =
            serde_json::from_value(value).expect("deserialize legacy reconciliation job");
        assert_eq!(decoded.schema_version, 0);
        assert!(decoded.unavailable_node_ids.is_empty());
        assert!(decoded.candidate_snapshot.is_empty());
        assert!(decoded.placement_snapshot.is_none());
        assert!(!decoded.adaptive);
        assert!(decoded.adaptive_budget.is_none());
        assert_eq!(decoded.processed_content_bytes, 0);
        assert!(decoded.accepted_config.is_none());
    }

    #[test]
    fn newer_authoritative_reconciliation_runs_before_superseded_backlog() {
        let owner = node("node-a");
        let base = placement_config(owner.clone());
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .put_file_system_config(&base)
                .expect("base config")
        });

        let (first, second) = control_plane.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv);
            let mut first_config = base.clone();
            upsert_placement_rule(
                &mut first_config.subtree_policies,
                normalize_placement_rule(
                    "/team",
                    DfsPlacementScope::Subtree,
                    DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1)
                        .expect("first policy"),
                )
                .expect("first rule"),
            );
            let mut first =
                reconciliation_job("/team", DfsPlacementScope::Subtree, BTreeSet::new());
            first.job_id = "placement-first".to_string();
            first.created_at_unix_nanos = 1;
            first.updated_at_unix_nanos = 1;
            first.accepted_config = Some(first_config.clone());
            assert!(
                store
                    .compare_and_put_placement_update(&base, &first_config, &first)
                    .expect("first update")
            );

            let mut second_config = first_config.clone();
            upsert_placement_rule(
                &mut second_config.subtree_policies,
                normalize_placement_rule(
                    "/team",
                    DfsPlacementScope::Subtree,
                    DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1)
                        .expect("second policy"),
                )
                .expect("second rule"),
            );
            let mut second =
                reconciliation_job("/team", DfsPlacementScope::Subtree, BTreeSet::new());
            second.job_id = "placement-second".to_string();
            second.created_at_unix_nanos = 2;
            second.updated_at_unix_nanos = 2;
            second.accepted_config = Some(second_config.clone());
            assert!(
                store
                    .compare_and_put_placement_update(&first_config, &second_config, &second)
                    .expect("second update")
            );
            (first, second)
        });

        let root = sinomo_testdir::TestDir::new("dfs-placement-superseded");
        let content = LocalContentStore::new(root.join("content")).expect("content store");
        let mut service = NetworkedDfsService::new(
            PreviewTransport {
                local_node_id: owner,
            },
            content,
        )
        .with_control_plane(control_plane.clone());

        let mut newer = service
            .run_placement_reconciliation_bounded(1)
            .expect("newer pass")
            .expect("authoritative job");
        assert_eq!(newer.job_id, second.job_id);
        assert_ne!(newer.state, DfsPlacementReconciliationState::Failed);
        for _ in 0..4 {
            if newer.state.is_terminal() {
                break;
            }
            newer = service
                .run_placement_reconciliation_bounded(1)
                .expect("continue newer job")
                .expect("authoritative job remains queued");
            assert_eq!(newer.job_id, second.job_id);
        }
        assert_eq!(newer.state, DfsPlacementReconciliationState::Completed);

        let superseded = service
            .run_placement_reconciliation_bounded(1)
            .expect("supersede pass")
            .expect("stale job");
        assert_eq!(superseded.job_id, first.job_id);
        assert_eq!(superseded.state, DfsPlacementReconciliationState::Failed);
        assert!(superseded.state.is_terminal());
        assert!(
            superseded
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("superseded"))
        );
    }

    #[test]
    fn unrelated_later_rule_does_not_supersede_older_reconciliation() {
        let owner = node("node-a");
        let base = placement_config(owner.clone());
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        control_plane.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .put_file_system_config(&base)
                .expect("base config")
        });

        let first = control_plane.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv);
            let mut first_config = base.clone();
            upsert_placement_rule(
                &mut first_config.subtree_policies,
                normalize_placement_rule(
                    "/team",
                    DfsPlacementScope::Subtree,
                    DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1)
                        .expect("team policy"),
                )
                .expect("team rule"),
            );
            let mut first =
                reconciliation_job("/team", DfsPlacementScope::Subtree, BTreeSet::new());
            first.job_id = "placement-team".to_string();
            first.created_at_unix_nanos = 1;
            first.updated_at_unix_nanos = 1;
            first.accepted_config = Some(first_config.clone());
            assert!(
                store
                    .compare_and_put_placement_update(&base, &first_config, &first)
                    .expect("team update")
            );

            let mut unrelated_config = first_config.clone();
            upsert_placement_rule(
                &mut unrelated_config.subtree_policies,
                normalize_placement_rule(
                    "/elsewhere",
                    DfsPlacementScope::Subtree,
                    DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1)
                        .expect("unrelated policy"),
                )
                .expect("unrelated rule"),
            );
            let mut unrelated =
                reconciliation_job("/elsewhere", DfsPlacementScope::Subtree, BTreeSet::new());
            unrelated.job_id = "placement-elsewhere".to_string();
            unrelated.created_at_unix_nanos = 2;
            unrelated.updated_at_unix_nanos = 2;
            unrelated.accepted_config = Some(unrelated_config.clone());
            assert!(
                store
                    .compare_and_put_placement_update(&first_config, &unrelated_config, &unrelated,)
                    .expect("unrelated update")
            );
            first
        });

        let root = sinomo_testdir::TestDir::new("dfs-placement-unrelated-update");
        let content = LocalContentStore::new(root.join("content")).expect("content store");
        let mut service = NetworkedDfsService::new(
            PreviewTransport {
                local_node_id: owner,
            },
            content,
        )
        .with_control_plane(control_plane);
        let advanced = service
            .run_placement_reconciliation_bounded(1)
            .expect("reconciliation pass")
            .expect("oldest job");
        assert_eq!(advanced.job_id, first.job_id);
        assert_ne!(advanced.state, DfsPlacementReconciliationState::Failed);
    }

    #[test]
    fn placement_local_rule_adaptive_reconciliation_accepts_default_sifs_with_six_candidates() {
        let owner = node("node-a");
        let config = DfsFileSystemConfig::from(&crate::distributed_sifs(owner.clone()));
        let local_path = crate::sifs_node_files_path(&owner);
        let local_rule = config
            .subtree_policies
            .iter()
            .find(|rule| rule.path_prefix == local_path)
            .expect("default SIFS local rule");
        assert_eq!(local_rule.policy, DfsObjectPolicy::LocalOwned);
        assert_eq!(local_rule.desired_replicas, None);
        assert_eq!(local_rule.min_commit_replicas, None);

        let mut job = reconciliation_job("/", DfsPlacementScope::Subtree, BTreeSet::new());
        job.adaptive = true;
        job.adaptive_budget = Some(DfsAdaptivePlacementBudget::default());
        job.accepted_config = Some(config.clone());
        job.candidate_snapshot = ["node-a", "node-b", "node-c", "node-d", "node-e", "node-f"]
            .into_iter()
            .map(|id| advertisement(id, "local-rule-test-zone"))
            .collect();
        job.placement_snapshot = Some(crate::DfsPlacementSnapshot::empty(owner.clone()));
        assert_eq!(job.candidate_snapshot.len(), 6);

        let local_directory = crate::distributed_sifs_bootstrap_objects(owner.clone())
            .into_iter()
            .find(|object| object.path == local_path)
            .expect("real SIFS bootstrap local directory");
        let mut local_file = placement_object(
            "adaptive-local-rule-child",
            &format!("{local_path}/existing.bin"),
            DfsObjectKind::File,
            owner.clone(),
            vec![owner],
            Some(4096),
        );
        local_file.policy = DfsObjectPolicy::LocalOwned;
        for object in [local_directory, local_file] {
            // This is the runner's actual per-object resolution path. Before
            // the fix, candidates.len()==6 produced N=2 and terminally failed
            // the otherwise valid adaptive job at the local subtree.
            let (resolved, replication, placement) =
                reconciliation_placement_for_object_with_pins(&config, &job, &object, &[])
                    .expect("default local subtree is a valid adaptive scan target");
            assert_eq!(resolved.policy.consistency, DfsObjectPolicy::LocalOwned);
            assert_eq!(resolved.policy.min_commit_replicas, 1);
            assert_eq!(resolved.policy.desired_replicas, 1);
            assert_eq!(replication.min_commit_replicas, 1);
            assert_eq!(replication.desired_replicas, 1);
            assert_eq!(placement.plan.desired_replica_nodes.len(), 1);
        }
    }

    fn local_owner_reconciliation_fixture(
        root: &std::path::Path,
        object_policy: DfsObjectPolicy,
        target_policy: DfsObjectPolicy,
        owner_location_present: bool,
    ) -> (
        NetworkedDfsService<PreviewTransport>,
        DfsPlacementReconciliationJob,
        DfsObjectMetadata,
        LocalContentStore,
    ) {
        let primary = node("node-a");
        let content_owner = node("node-b");
        let path = format!("{}/owned.bin", crate::sifs_node_files_path(&content_owner));
        let mut config = placement_config(primary.clone());
        config.subtree_policies.push(DfsSubtreePolicy::legacy(
            crate::sifs_node_files_path(&content_owner),
            target_policy,
        ));
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        let job = control_plane.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv);
            store
                .put_file_system_config(&config)
                .expect("filesystem config");
            store
                .put_metadata_shard(&DfsMetadataShardConfig {
                    file_system_id: config.file_system_id.clone(),
                    shard_id: "root".to_string(),
                    primary_node_id: primary.clone(),
                    replica_node_ids: Vec::new(),
                    epoch: 1,
                    assignment: DfsMetadataShardAssignment::Root,
                    namespace_prepare_codec: Default::default(),
                    default_policy: DfsObjectPolicy::ReplicatedEventual,
                })
                .expect("metadata primary differs from content owner");
            let mut job = reconciliation_job("/", DfsPlacementScope::Subtree, BTreeSet::new());
            job.adaptive = true;
            job.adaptive_budget = Some(DfsAdaptivePlacementBudget::default());
            job.accepted_config = Some(config.clone());
            job.candidate_snapshot = ["node-a", "node-b", "node-c", "node-d", "node-e", "node-f"]
                .into_iter()
                .map(|id| advertisement(id, "owner-test-zone"))
                .collect();
            job.placement_snapshot = Some(crate::DfsPlacementSnapshot::empty(primary.clone()));
            assert!(
                store
                    .compare_and_put_placement_update(&config, &config, &job)
                    .unwrap()
            );
            store
                .placement_reconciliation(&config.file_system_id, &job.job_id)
                .unwrap()
                .expect("stored job")
        });
        let mut object = placement_object(
            "local-owner-execution",
            &path,
            DfsObjectKind::File,
            content_owner.clone(),
            vec![node("node-c")],
            Some(4096),
        );
        object.policy = object_policy;
        if owner_location_present {
            object.current_locations.insert(0, content_owner);
        }
        object.current_locations.sort();
        let bytes = vec![0x5a; 4096];
        object.content_hash = Some(crate::content_hash(&bytes));
        let owner_content = LocalContentStore::new(root.join("owner-content")).unwrap();
        owner_content
            .put_object_version(object.object_id.as_str(), object.version.as_str(), &bytes)
            .expect("durable content stays at its existing owner");
        let mut metadata = open_root_local_metadata_backend(root.join("metadata")).unwrap();
        metadata
            .put_file_system(&DfsFileSystem::from(config))
            .unwrap();
        let mut service = NetworkedDfsService::new(
            PreviewTransport {
                local_node_id: primary,
            },
            LocalContentStore::new(root.join("primary-content")).unwrap(),
        )
        .with_control_plane(control_plane)
        .with_metadata_backend(metadata)
        .unwrap();
        service.add_object_metadata(object.clone()).unwrap();
        (service, job, object, owner_content)
    }

    fn assert_local_reconciliation_preserved(
        service: &mut NetworkedDfsService<PreviewTransport>,
        job: &DfsPlacementReconciliationJob,
        object: &DfsObjectMetadata,
        owner_content: &LocalContentStore,
    ) {
        assert_eq!(
            service
                .current_read_object_metadata(&object.object_id)
                .unwrap(),
            *object
        );
        let intent = format!(
            "placement:{}:{}:{}",
            job.job_id, object.object_id, object.version
        );
        assert!(
            service.coordinator.intent(&intent).is_none(),
            "no generic placement repair intent"
        );
        assert!(
            !service
                .content()
                .has_blob(object.content_hash.as_ref().unwrap())
                .unwrap()
        );
        assert_eq!(
            owner_content
                .read_object_version(object.object_id.as_str(), object.version.as_str())
                .unwrap()
                .1,
            vec![0x5a; 4096]
        );
    }

    #[test]
    fn placement_local_owner_execution_preserves_non_filesystem_owner_and_cached_copies() {
        for policy in [DfsObjectPolicy::LocalOwned, DfsObjectPolicy::EphemeralLocal] {
            let root = sinomo_testdir::TestDir::new("dfs-placement-local-owner-execution");
            let (mut service, job, object, owner_content) =
                local_owner_reconciliation_fixture(root.path(), policy.clone(), policy, true);
            assert_ne!(object.owner_node_id, service.node_id);
            let (resolved, replication, placement) = reconciliation_placement_for_object_with_pins(
                job.accepted_config.as_ref().unwrap(),
                &job,
                &object,
                &[],
            )
            .expect("local owner is the sole placement target");
            assert_eq!(
                placement.plan.desired_replica_nodes,
                vec![object.owner_node_id.clone()]
            );
            service
                .reconcile_object_placement_local(
                    &job.job_id,
                    &object.path,
                    &object.object_id,
                    &object.version,
                    resolved.policy.consistency,
                    replication,
                    &placement.plan.desired_replica_nodes,
                )
                .expect("actual metadata-primary execution keeps local ownership");
            assert_local_reconciliation_preserved(&mut service, &job, &object, &owner_content);
        }
    }

    #[test]
    fn placement_local_owner_execution_rejects_substitution_and_missing_owner() {
        for policy in [DfsObjectPolicy::LocalOwned, DfsObjectPolicy::EphemeralLocal] {
            for owner_present in [true, false] {
                let root = sinomo_testdir::TestDir::new("dfs-placement-local-owner-rejection");
                let (mut service, job, object, owner_content) = local_owner_reconciliation_fixture(
                    root.path(),
                    policy.clone(),
                    policy.clone(),
                    owner_present,
                );
                let target = if owner_present {
                    service.node_id.clone()
                } else {
                    object.owner_node_id.clone()
                };
                let error = service
                    .reconcile_object_placement_local(
                        &job.job_id,
                        &object.path,
                        &object.object_id,
                        &object.version,
                        policy.clone(),
                        DfsReplicationPolicy::new(1, 1).unwrap(),
                        &[target],
                    )
                    .expect_err(
                        "a local owner cannot be substituted or restored by generic repair",
                    );
                assert!(matches!(error, DfsError::Distributed { .. }));
                assert_local_reconciliation_preserved(&mut service, &job, &object, &owner_content);
            }
        }
    }

    #[test]
    fn placement_local_owner_execution_rejects_unimplemented_policy_conversion_before_effects() {
        for (source, target) in [
            (
                DfsObjectPolicy::ReplicatedEventual,
                DfsObjectPolicy::LocalOwned,
            ),
            (
                DfsObjectPolicy::ReplicatedEventual,
                DfsObjectPolicy::EphemeralLocal,
            ),
            (DfsObjectPolicy::LocalOwned, DfsObjectPolicy::EphemeralLocal),
            (DfsObjectPolicy::EphemeralLocal, DfsObjectPolicy::LocalOwned),
        ] {
            let root = sinomo_testdir::TestDir::new("dfs-placement-local-owner-conversion");
            let (mut service, job, object, owner_content) =
                local_owner_reconciliation_fixture(root.path(), source, target.clone(), true);
            let error = service
                .reconcile_object_placement_local(
                    &job.job_id,
                    &object.path,
                    &object.object_id,
                    &object.version,
                    target,
                    DfsReplicationPolicy::new(1, 1).unwrap(),
                    std::slice::from_ref(&object.owner_node_id),
                )
                .expect_err("policy conversion needs an owner-localization protocol");
            assert!(matches!(error, DfsError::InvalidReplicationPolicy { .. }));
            assert_local_reconciliation_preserved(&mut service, &job, &object, &owner_content);
        }
    }

    #[test]
    fn placement_local_owner_retires_legacy_managed_repair_without_retracting_commit() {
        for policy in [DfsObjectPolicy::LocalOwned, DfsObjectPolicy::EphemeralLocal] {
            let root = sinomo_testdir::TestDir::new("dfs-placement-legacy-local-repair");
            let (mut service, job, object, owner_content) =
                local_owner_reconciliation_fixture(root.path(), policy.clone(), policy, true);
            let intent_id = format!(
                "placement:{}:{}:{}",
                job.job_id, object.object_id, object.version
            );
            service
                .coordinator
                .restore_intent(DistributedWriteIntent {
                    publication: Default::default(),
                    intent_id: intent_id.clone(),
                    file_system_id: object.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                    base_version: Some(object.version.clone()),
                    target_version: object.version.clone(),
                    requesting_node_id: service.node_id.clone(),
                    operation_id: None,
                    operation_order_token: None,
                    owner_epoch: 1,
                    desired_replica_nodes: vec![object.owner_node_id.clone()],
                    policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                    touch_parent_on_create: false,
                    create_predecessor_object_id: None,
                    state: DfsWriteIntentState::Committed,
                })
                .expect("older managed local repair intent");
            assert!(
                !service
                    .placement_intent_remains_authoritative(&intent_id)
                    .expect("local generic repair is no longer authorized")
            );
            let mut ran_repair_action = false;
            assert!(
                service
                    .with_placement_intent_authority(&intent_id, |_| {
                        ran_repair_action = true;
                        Ok(())
                    })
                    .expect("delayed managed repair callback is fenced")
                    .is_none()
            );
            assert!(!ran_repair_action);
            assert_eq!(
                service.coordinator.intent(&intent_id).unwrap().state,
                DfsWriteIntentState::Committed,
                "retire repair, preserve published outcome"
            );
            assert_eq!(
                service
                    .current_read_object_metadata(&object.object_id)
                    .unwrap(),
                object
            );
            assert!(
                !service
                    .content()
                    .has_blob(object.content_hash.as_ref().unwrap())
                    .unwrap()
            );
            assert_eq!(
                owner_content
                    .read_object_version(object.object_id.as_str(), object.version.as_str())
                    .unwrap()
                    .1,
                vec![0x5a; 4096]
            );
        }
    }

    #[derive(Clone, Debug)]
    struct PlacementRouteCounter {
        local_node_id: NodeId,
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        stall: Option<std::sync::Arc<PlacementPeerStall>>,
        responses: std::sync::Arc<std::sync::Mutex<Vec<crate::DfsNamespaceResponse>>>,
        operations: std::sync::Arc<std::sync::Mutex<Vec<DfsNamespaceOperation>>>,
        lookup_response: Option<Vec<DfsObjectMetadata>>,
        reconcile_response: Option<Vec<DfsObjectMetadata>>,
        incoming: Vec<crate::DfsRoutedMessage>,
        repair_authority_probe: Option<RepairAuthorityProbe>,
    }

    #[derive(Debug, Default)]
    struct PlacementPeerStall {
        entered: tokio::sync::Notify,
        released: std::sync::Mutex<bool>,
        release: std::sync::Condvar,
    }

    impl DfsTransportIdentity for PlacementRouteCounter {
        fn local_node_id(&self) -> &NodeId {
            &self.local_node_id
        }
    }

    impl DfsMessageTransport for PlacementRouteCounter {
        fn send_routed_call(
            &mut self,
            target: &NodeId,
            call: DfsRoutedCall,
        ) -> Result<(), NetworkError> {
            if let Some(probe) = &self.repair_authority_probe
                && let crate::DfsNetworkMessage::FetchBlob(command) = &call.message.message
            {
                assert!(
                    !probe.permit_is_present(),
                    "alternate fetch dispatch follows permit release"
                );
                probe.fetches.lock().unwrap().push(command.clone());
                return Ok(());
            }
            if let Some(probe) = &self.repair_authority_probe
                && let crate::DfsNetworkMessage::RepairRetainedBlob(command) = &call.message.message
                && let Some(gate) = probe.retained_checkpoint_gate.lock().unwrap().as_ref()
            {
                assert!(
                    repair_paging_store(&gate.root)
                        .view()
                        .unwrap()
                        .records
                        .iter()
                        .all(|record| record.intent_id != gate.retired_intent_id),
                    "retained network send must not begin while earlier cleanup is uncheckpointed"
                );
                gate.calls.lock().unwrap().push(command.clone());
                return Ok(());
            }
            let routed = call.message;
            match routed.message {
                crate::DfsNetworkMessage::NamespaceResponse(response) => {
                    self.responses.lock().unwrap().push(response);
                    return Ok(());
                }
                crate::DfsNetworkMessage::NamespaceRequest(command) => {
                    self.operations
                        .lock()
                        .unwrap()
                        .push(command.operation.clone());
                    let objects = match &command.operation {
                        DfsNamespaceOperation::LookupPaths { .. } => self.lookup_response.as_ref(),
                        DfsNamespaceOperation::ReconcilePlacement { .. } => {
                            self.reconcile_response.as_ref()
                        }
                        _ => None,
                    };
                    if let Some(objects) = objects {
                        assert!(
                            command.metadata_journal_after_sequences.is_empty(),
                            "background placement cannot require a requester journal cursor"
                        );
                        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        assert!(!command.include_metadata_journal_batches);
                        self.incoming.push(crate::DfsRoutedMessage {
                            source_node_id: target.clone(),
                            target_node_id: self.local_node_id.clone(),
                            frame_id: routed.frame_id,
                            message_id: routed.message_id,
                            message: crate::DfsNetworkMessage::NamespaceResponse(
                                crate::DfsNamespaceResponse {
                                    request_id: command.request_id,
                                    result: crate::DfsNamespaceResult::Applied,
                                    synced_path_object: objects.first().cloned(),
                                    synced_path_objects: objects.clone(),
                                    metadata_journal_batches: Vec::new(),
                                    metadata_journal_has_more: Some(false),
                                    posix_coordination_result: None,
                                },
                            ),
                        });
                        return Ok(());
                    }
                }
                _ => {}
            }
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if let Some(stall) = &self.stall {
                stall.entered.notify_one();
                let guard = stall.released.lock().unwrap();
                let _ = stall
                    .release
                    .wait_timeout_while(guard, Duration::from_secs(2), |released| !*released)
                    .unwrap();
            }
            Err(NetworkError::UnknownNode {
                node_id: target.clone(),
            })
        }
        fn supports_background_replica_dispatch(&self) -> bool {
            self.repair_authority_probe.is_some()
        }

        fn prepare_background_replica_dispatch(
            &mut self,
            requests: Vec<crate::DfsRoutedSend>,
        ) -> Result<(), NetworkError> {
            let probe = self
                .repair_authority_probe
                .as_ref()
                .expect("background probe enabled");
            assert!(
                !probe.permit_is_present(),
                "real permit must be released before dispatch"
            );
            if let Some(failed) = probe.fail_dispatch_intent.lock().unwrap().as_ref()
                && requests.iter().any(|request| matches!(&request.call.message.message, crate::DfsNetworkMessage::RepairReplica(command) if &command.intent_id == failed)) {
                return Err(NetworkError::Io { operation: "injected repair prepare failure".into(), detail: "no requests accepted from this fanout".into() });
            }
            assert!(
                requests.len() <= 2,
                "existing attachment fanout cap remains two"
            );
            for request in requests {
                assert!(
                    request.call.attachment.is_some(),
                    "dispatch owns an immutable content attachment"
                );
                let crate::DfsNetworkMessage::RepairReplica(command) = request.call.message.message
                else {
                    panic!("only current-content repair belongs in background dispatch");
                };
                assert!(command.background_repair);
                probe.dispatched.lock().unwrap().push(command);
            }
            Ok(())
        }

        fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
            Ok(std::mem::take(&mut self.incoming))
        }
        fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
            Ok(Some(self.incoming.len()))
        }
    }

    fn placement_page_noop_fixture(
        root: &std::path::Path,
        remote_metadata_primary: bool,
        change: &str,
    ) -> (
        NetworkedDfsService<PlacementRouteCounter>,
        SharedDfsControlPlaneKvClient<InProcessKvStore>,
        DfsObjectMetadata,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let local = node("node-a");
        let remote = node("node-b");
        let target_policy = if change == "policy" {
            DfsObjectPolicy::ReplicatedConfirmed
        } else {
            DfsObjectPolicy::ReplicatedEventual
        };
        let desired = if change == "missing" { 2 } else { 1 };
        let mut config = placement_config(local.clone());
        config.subtree_policies.push(
            normalize_placement_rule(
                "/team",
                DfsPlacementScope::Subtree,
                DfsPlacementPolicy::new(target_policy, 1, desired).unwrap(),
            )
            .unwrap(),
        );
        let control = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        control.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv);
            store.put_file_system_config(&config).unwrap();
            store
                .put_metadata_shard(&DfsMetadataShardConfig {
                    file_system_id: config.file_system_id.clone(),
                    shard_id: "root".into(),
                    primary_node_id: local.clone(),
                    replica_node_ids: Vec::new(),
                    epoch: 1,
                    assignment: DfsMetadataShardAssignment::Root,
                    namespace_prepare_codec: Default::default(),
                    default_policy: DfsObjectPolicy::ReplicatedEventual,
                })
                .unwrap();
            if remote_metadata_primary {
                store
                    .put_metadata_shard(&DfsMetadataShardConfig {
                        file_system_id: config.file_system_id.clone(),
                        shard_id: "team".into(),
                        primary_node_id: remote.clone(),
                        replica_node_ids: Vec::new(),
                        epoch: 1,
                        assignment: DfsMetadataShardAssignment::PathPrefix {
                            prefix: "/team".into(),
                        },
                        namespace_prepare_codec: Default::default(),
                        default_policy: DfsObjectPolicy::ReplicatedEventual,
                    })
                    .unwrap();
            }
            let mut job = reconciliation_job("/team", DfsPlacementScope::Subtree, BTreeSet::new());
            job.accepted_config = Some(config.clone());
            job.candidate_snapshot = vec![advertisement("node-a", "east")];
            if change == "missing" {
                job.candidate_snapshot.push(advertisement("node-b", "west"));
            }
            assert!(
                store
                    .compare_and_put_placement_update(&config, &config, &job)
                    .unwrap()
            );
        });
        let mut object = placement_object(
            "page-noop",
            "/team/object.bin",
            DfsObjectKind::File,
            local.clone(),
            vec![local.clone()],
            Some(4096),
        );
        if change == "obsolete" {
            object.current_locations.push(remote);
        }
        object.current_locations.sort();
        let bytes = vec![0x37; 4096];
        object.content_hash = Some(crate::content_hash(&bytes));
        let content = LocalContentStore::new(root.join("content")).unwrap();
        content
            .put_object_version(object.object_id.as_str(), object.version.as_str(), &bytes)
            .unwrap();
        let mut metadata = open_root_local_metadata_backend(root.join("metadata"))
            .expect("durable root metadata backend");
        metadata
            .put_file_system(&DfsFileSystem::from(config))
            .expect("persist the accepted filesystem");
        if remote_metadata_primary {
            // Seed the coordinator's durable mirror before attaching cluster
            // authority. The test must still route to refresh this remote row;
            // the coordinator never authors it as the remote shard primary.
            metadata
                .put_object(&object)
                .expect("seed remote object mirror");
        }
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut service = NetworkedDfsService::new(
            PlacementRouteCounter {
                local_node_id: local,
                calls: calls.clone(),
                stall: None,
                responses: Default::default(),
                operations: Default::default(),
                lookup_response: None,
                reconcile_response: None,
                incoming: Vec::new(),
                repair_authority_probe: None,
            },
            content,
        )
        .with_control_plane(control.clone())
        .with_metadata_backend(metadata)
        .expect("attach durable backend under the configured shard authority");
        if !remote_metadata_primary {
            service
                .add_object_metadata(object.clone())
                .expect("author local row through real clustered metadata publication");
        }
        (service, control, object, calls)
    }

    fn placement_permit_event_count(
        control: &SharedDfsControlPlaneKvClient<InProcessKvStore>,
    ) -> usize {
        control.with_client(|kv| {
            kv.snapshot()
                .unwrap()
                .events
                .iter()
                .filter(|event| event.key.contains("/placement-authority-permit-v1"))
                .count()
        })
    }

    #[tokio::test]
    async fn async_placement_uses_authoritative_results_without_requester_replay() {
        for response_includes_current_object in [true, false] {
            let root = sinomo_testdir::TestDir::new("placement-result-only");
            let (mut service, _control, object, calls) =
                placement_page_noop_fixture(root.path(), true, "missing");
            let mut current = object.clone();
            current.current_locations.push(node("node-b"));
            service.transport.reconcile_response = Some(if response_includes_current_object {
                vec![current.clone()]
            } else {
                Vec::new()
            });
            service.transport.lookup_response = Some(vec![current]);
            let scanning = service
                .run_placement_reconciliation_bounded_async(8)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(scanning.state, DfsPlacementReconciliationState::Repairing);
            assert!(scanning.last_error.is_none(), "{:?}", scanning.last_error);
            let completed = service
                .run_placement_reconciliation_bounded_async(8)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(completed.state, DfsPlacementReconciliationState::Completed);
            assert_eq!(completed.pending_repair_count, 0);
            assert_eq!(
                calls.load(std::sync::atomic::Ordering::SeqCst),
                if response_includes_current_object {
                    2
                } else {
                    4
                },
                "old replies use a bounded authoritative lookup fallback"
            );
            assert_eq!(
                service
                    .metadata_backend
                    .as_ref()
                    .unwrap()
                    .get_object(&object.object_id)
                    .unwrap(),
                Some(object),
                "the result proves placement progress without authoring or refreshing the coordinator's replica"
            );
        }
    }

    #[test]
    fn completed_placement_does_not_resurrect_superseded_write_repairs() {
        let root = sinomo_testdir::TestDir::new("placement-recovery-obligation");
        let (mut service, control, object, _) =
            placement_page_noop_fixture(root.path(), false, "obsolete");
        let intent = DistributedWriteIntent {
            publication: Default::default(),
            intent_id: "original-two-copy-write".into(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: Some(object.version.clone()),
            target_version: object.version.clone(),
            requesting_node_id: node("node-a"),
            operation_id: Some("original-write-request".into()),
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_nodes: vec![node("node-a"), node("node-b")],
            policy: DfsReplicationPolicy::new(1, 2).unwrap(),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            state: DfsWriteIntentState::Committed,
        };
        service
            .put_metadata_intent_and_receipts(
                &intent,
                object.content_hash.as_deref().unwrap(),
                &intent.desired_replica_nodes,
            )
            .unwrap();
        service.hydrate_from_metadata_backend().unwrap();
        assert!(service.coordinator.repair_jobs().is_empty());

        let scan = service
            .run_placement_reconciliation_bounded(8)
            .unwrap()
            .unwrap();
        assert!(scan.last_error.is_none(), "{:?}", scan.last_error);
        let completed = service
            .run_placement_reconciliation_bounded(8)
            .unwrap()
            .unwrap();
        assert_eq!(completed.state, DfsPlacementReconciliationState::Completed);
        assert_eq!(
            service
                .current_read_object_metadata(&object.object_id)
                .unwrap()
                .current_locations,
            [node("node-a")],
        );
        drop(service);

        let mut recovered = NetworkedDfsService::new(
            PreviewTransport {
                local_node_id: node("node-a"),
            },
            LocalContentStore::new(root.join("content")).unwrap(),
        )
        .with_control_plane(control)
        .with_metadata_backend(open_root_local_metadata_backend(root.join("metadata")).unwrap())
        .unwrap();
        assert!(
            recovered.coordinator.repair_jobs().is_empty(),
            "restart must not restore the old two-copy write's superseded target: {:?}",
            recovered.coordinator.repair_jobs(),
        );
        assert!(
            recovered
                .metadata_backend
                .as_ref()
                .unwrap()
                .get_intent(&object.object_id, &intent.intent_id)
                .unwrap()
                .is_some(),
            "the original operation's exact retry identity must remain available",
        );
        let replacement_id = format!(
            "placement:{}:{}:{}",
            completed.job_id, object.object_id, object.version
        );
        let backend = recovered.metadata_backend.as_mut().unwrap();
        backend
            .gc_metadata_history(crate::DfsMetadataHistoryGcOptions::delete_eligible(
                Duration::ZERO,
                0,
            ))
            .unwrap();
        assert!(
            backend
                .get_intent_receipt(&object.object_id, &replacement_id)
                .unwrap()
                .is_none(),
            "GC must preserve the current placement repair contract"
        );
        assert_eq!(
            backend
                .get_intent(&object.object_id, &replacement_id)
                .unwrap()
                .unwrap()
                .policy
                .desired_replicas,
            1
        );
        recovered.hydrate_from_metadata_backend().unwrap();
        assert!(recovered.coordinator.repair_jobs().is_empty());
    }

    fn placement_test_committed_intent(
        object: &DfsObjectMetadata,
        id: String,
    ) -> DistributedWriteIntent {
        DistributedWriteIntent {
            publication: Default::default(),
            intent_id: id,
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: Some(object.version.clone()),
            target_version: object.version.clone(),
            requesting_node_id: node("node-a"),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_nodes: vec![node("node-a"), node("node-b")],
            policy: DfsReplicationPolicy::new(1, 2).unwrap(),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            state: DfsWriteIntentState::Committed,
        }
    }

    #[derive(Debug)]
    struct NoPlacementHistoryKv(SharedDfsControlPlaneKvClient<InProcessKvStore>);

    impl DfsControlPlaneKvClient for NoPlacementHistoryKv {
        fn put(
            &mut self,
            key: String,
            value: Vec<u8>,
        ) -> Result<sinomo_core::kv::KvRevision, sinomo_core::kv::KvError> {
            assert!(!key.contains("placement-reconciliation"));
            self.0.put(key, value)
        }
        fn get(
            &self,
            key: &str,
        ) -> Result<Option<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
            assert!(
                !key.contains("placement-reconciliation"),
                "ordinary repair read placement history: {key}"
            );
            self.0.get(key)
        }
        fn range_prefix(
            &self,
            prefix: &str,
        ) -> Result<Vec<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
            assert!(
                !prefix.contains("placement-reconciliation"),
                "ordinary repair scanned placement history"
            );
            self.0.range_prefix(prefix)
        }
        fn txn(
            &mut self,
            txn: sinomo_core::kv::KvTxn,
        ) -> Result<sinomo_core::kv::KvTxnResponse, sinomo_core::kv::KvError> {
            self.0.txn(txn)
        }
    }

    #[test]
    fn ordinary_current_policy_repair_does_not_read_placement_history() {
        let root = sinomo_testdir::TestDir::new("ordinary-repair-control-boundary");
        let (mut service, control, object, calls) =
            placement_page_noop_fixture(root.path(), false, "missing");
        let intent = placement_test_committed_intent(&object, "ordinary-current-write".into());
        service
            .put_metadata_intent_and_receipts(
                &intent,
                object.content_hash.as_deref().unwrap(),
                &object.current_locations,
            )
            .unwrap();
        service.hydrate_from_metadata_backend().unwrap();
        service.control_plane = Some(Box::new(NoPlacementHistoryKv(control)));
        service.retry_pending_repairs_bounded(8).unwrap();
        assert!(
            calls.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "a genuine missing replica still receives a repair attempt"
        );
    }

    #[test]
    fn placement_repair_recovery_does_not_replay_stage_promotions() {
        for managed in [false, true] {
            let root = sinomo_testdir::TestDir::new("placement-promotion-recovery");
            let (mut service, _, object, calls) =
                placement_page_noop_fixture(root.path(), false, "missing");
            let id = if managed {
                format!(
                    "placement:placement-test-job:{}:{}",
                    object.object_id, object.version
                )
            } else {
                "ordinary-staged-write".into()
            };
            let intent = placement_test_committed_intent(&object, id);
            service
                .put_metadata_intent_and_receipts(
                    &intent,
                    object.content_hash.as_deref().unwrap(),
                    &intent.desired_replica_nodes,
                )
                .unwrap();
            calls.store(0, std::sync::atomic::Ordering::SeqCst);
            service
                .hydrate_from_metadata_backend_for_commit_promotion_recovery()
                .unwrap();
            assert_eq!(
                calls.load(std::sync::atomic::Ordering::SeqCst),
                usize::from(!managed),
                "RepairReplica finishes placement copies; ordinary StageBlob writes retain promotion recovery"
            );
        }
    }

    #[test]
    fn placement_retirement_preserves_independent_retained_durability() {
        let root = sinomo_testdir::TestDir::new("placement-retained-durability");
        let (mut service, _, object, _) =
            placement_page_noop_fixture(root.path(), false, "obsolete");
        let intent = placement_test_committed_intent(&object, "retained-original-write".into());
        service
            .put_metadata_intent_and_receipts(
                &intent,
                object.content_hash.as_deref().unwrap(),
                &object.current_locations,
            )
            .unwrap();
        let mut version =
            crate::metadata_records::object_version_record(&object, object.content_size_bytes);
        version.content_durability = Some(crate::content_durability::from_intent(&intent));
        sinomo_dfs_history::install_history_capture(
            &mut version,
            crate::DfsRetainedFileVersion {
                object: Box::new(object.clone()),
                captured_at_unix_nanos: 1,
                source: crate::DfsFileVersionSource::RetentionRule,
                policy_capture: Some(crate::DfsFileVersionPolicyCapture {
                    order: 1,
                    captured_at_unix_nanos: 1,
                    retention: crate::DfsFileHistoryRetention {
                        keep_last: 1,
                        keep_for_seconds: 0,
                    },
                }),
            },
            1,
        )
        .unwrap();
        service
            .metadata_backend
            .as_mut()
            .unwrap()
            .put_version(&version)
            .unwrap();
        service.hydrate_from_metadata_backend().unwrap();
        for _ in 0..2 {
            let job = service
                .run_placement_reconciliation_bounded(8)
                .unwrap()
                .unwrap();
            assert!(job.last_error.is_none(), "{:?}", job.last_error);
        }
        assert_eq!(
            service
                .current_read_object_metadata(&object.object_id)
                .unwrap()
                .current_locations,
            object.current_locations,
            "current placement cannot prune an independently retained copy"
        );
        let backend = service.metadata_backend.as_ref().unwrap();
        assert!(
            backend
                .get_intent_receipt(&object.object_id, &intent.intent_id)
                .unwrap()
                .is_some()
        );
        let owners = backend.write_intents_page(None, 8).unwrap().intents;
        let retained = owners
            .iter()
            .find(|i| i.publication.is_retained_repair())
            .expect("durable retained repair owner");
        assert_eq!(retained.policy.desired_replicas, 2);
        assert_eq!(
            retained.desired_replica_node_ids,
            intent.desired_replica_nodes
        );
    }

    #[tokio::test]
    async fn legacy_repair_conversion_batches_objects_under_one_permit() {
        for asynchronous in [false, true] {
            let root = sinomo_testdir::TestDir::new("legacy-repair-batch-permit");
            let (mut service, control, object, calls) =
                placement_page_noop_fixture(root.path(), false, "none");
            let mut identities = Vec::new();
            for index in 0..3 {
                let mut copy = object.clone();
                if index > 0 {
                    copy.object_id =
                        DfsObjectId::from_legacy_named(format!("legacy-batch-{index}")).unwrap();
                    copy.path = format!("/team/object-{index}.bin");
                    service
                        .content
                        .put_object_version(
                            copy.object_id.as_str(),
                            copy.version.as_str(),
                            &vec![0x37; 4096],
                        )
                        .unwrap();
                    service.add_object_metadata(copy.clone()).unwrap();
                }
                let intent =
                    placement_test_committed_intent(&copy, format!("legacy-batch-write-{index}"));
                service
                    .put_metadata_intent_and_receipts(
                        &intent,
                        copy.content_hash.as_deref().unwrap(),
                        &copy.current_locations,
                    )
                    .unwrap();
                identities.push((copy.object_id, intent.intent_id));
            }
            control.with_client(|kv| {
                let mut store = DfsControlPlaneStore::new(kv);
                let previous = store
                    .placement_reconciliation(&object.file_system_id, "placement-test-job")
                    .unwrap()
                    .unwrap();
                let mut completed = previous.clone();
                completed.state = DfsPlacementReconciliationState::Completed;
                completed.revision += 1;
                assert!(
                    store
                        .compare_and_put_placement_reconciliation(&previous, &completed)
                        .unwrap()
                );
            });
            service.hydrate_from_metadata_backend().unwrap();
            assert_eq!(service.coordinator.repair_jobs().len(), 3);
            let before = placement_permit_event_count(&control);
            let sent = if asynchronous {
                service
                    .retry_pending_repairs_async_bounded_with_attempts_and_execution_exclusions(
                        2,
                        &[],
                        &Default::default(),
                    )
                    .await
                    .unwrap()
            } else {
                service.retry_pending_repairs_bounded(2).unwrap()
            };
            assert!(sent.is_empty());
            assert_eq!(
                placement_permit_event_count(&control) - before,
                2,
                "two conversions share one acquisition and one release"
            );
            assert_eq!(
                service.coordinator.repair_jobs().len(),
                1,
                "the object budget must bound the batch"
            );
            service.hydrate_from_metadata_backend().unwrap();
            assert_eq!(
                service.coordinator.repair_jobs().len(),
                1,
                "retirement remains durable after reconstructing service state"
            );
            service.retry_pending_repairs_bounded(2).unwrap();
            assert!(service.coordinator.repair_jobs().is_empty());
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
            let backend = service.metadata_backend.as_ref().unwrap();
            for (object_id, intent_id) in identities {
                assert!(
                    backend
                        .get_intent_receipt(&object_id, &intent_id)
                        .unwrap()
                        .is_some()
                );
            }
        }
    }

    #[test]
    fn repair_recovery_finishes_legacy_completed_placement_ownership() {
        for config_changes in [false, true] {
            let root = sinomo_testdir::TestDir::new("legacy-placement-recovery");
            let (mut service, control, object, calls) =
                placement_page_noop_fixture(root.path(), false, "none");
            let intent = placement_test_committed_intent(&object, "legacy-two-copy-write".into());
            service
                .put_metadata_intent_and_receipts(
                    &intent,
                    object.content_hash.as_deref().unwrap(),
                    &object.current_locations,
                )
                .unwrap();
            // Model the previous implementation: its placement job completed
            // after pruning the old target, but left the old write executable.
            control.with_client(|kv| {
                let mut store = DfsControlPlaneStore::new(kv);
                let old = store
                    .placement_reconciliation(&object.file_system_id, "placement-test-job")
                    .unwrap()
                    .unwrap();
                let mut completed = old.clone();
                completed.state = DfsPlacementReconciliationState::Completed;
                completed.revision += 1;
                assert!(
                    store
                        .compare_and_put_placement_reconciliation(&old, &completed)
                        .unwrap()
                );
            });
            service.hydrate_from_metadata_backend().unwrap();
            let repairs = service.coordinator.repair_jobs().to_vec();
            assert_eq!(repairs.len(), 1);
            let plans = service.placement_plans_for_repair().unwrap();
            if config_changes {
                control.with_client(|kv| {
                    let mut store = DfsControlPlaneStore::new(kv);
                    let current = store
                        .get_file_system_config(&object.file_system_id)
                        .unwrap()
                        .unwrap();
                    let mut config = current.clone();
                    config.subtree_policies[0] = normalize_placement_rule(
                        "/team",
                        DfsPlacementScope::Subtree,
                        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap(),
                    )
                    .unwrap();
                    assert!(
                        store
                            .compare_and_put_file_system_config(Some(&current), &config)
                            .unwrap()
                    );
                    assert!(!placement_config_remains_authoritative_for_job(
                        plans[0].accepted_config.as_ref().unwrap(),
                        &config,
                        &plans[0],
                    ));
                });
                assert!(
                    service
                        .replace_superseded_repair_obligations(&repairs, &plans, 1, None, |_| true)
                        .unwrap()
                        == 0
                );
                assert!(
                    service
                        .metadata_backend
                        .as_ref()
                        .unwrap()
                        .get_intent_receipt(&object.object_id, &intent.intent_id)
                        .unwrap()
                        .is_none()
                );
            } else {
                assert!(service.retry_pending_repairs_bounded(8).unwrap().is_empty());
                assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
                assert!(service.coordinator.repair_jobs().is_empty());
                service.hydrate_from_metadata_backend().unwrap();
                assert!(
                    service.coordinator.repair_jobs().is_empty(),
                    "completed normalization survives another reconstruction"
                );
                assert_eq!(
                    crate::DfsOperatorInspector::new(
                        service.metadata_backend.as_ref().unwrap().as_ref()
                    )
                    .status()
                    .unwrap()
                    .pending_repair_count,
                    0
                );
            }
        }
    }

    #[test]
    fn placement_policy_mutation_owns_publication_but_completed_verification_does_not() {
        let root = sinomo_testdir::TestDir::new("placement-owned-publication");
        let (mut service, _control, _, _) =
            placement_page_noop_fixture(root.path(), false, "policy");
        service.begin_message_committed_work();
        let scanning = service
            .run_placement_reconciliation_bounded(8)
            .unwrap()
            .unwrap();
        assert!(scanning.last_error.is_none(), "{:?}", scanning.last_error);
        assert!(service.take_message_committed_work().publish_metadata);
        service.begin_message_committed_work();
        let completed = service
            .run_placement_reconciliation_bounded(8)
            .unwrap()
            .unwrap();
        assert_eq!(completed.state, DfsPlacementReconciliationState::Completed);
        assert!(!service.take_message_committed_work().publish_metadata);
    }

    #[test]
    fn placement_page_noop_skips_only_local_permits_and_preserves_remote_freshness_route() {
        for remote in [false, true] {
            let root = sinomo_testdir::TestDir::new("dfs-placement-page-noop");
            let (mut service, control, object, calls) =
                placement_page_noop_fixture(root.path(), remote, "none");
            let permits_before = placement_permit_event_count(&control);
            let scanning = service
                .run_placement_reconciliation_bounded(8)
                .unwrap()
                .unwrap();
            if remote {
                assert!(
                    calls.load(std::sync::atomic::Ordering::SeqCst) > 0,
                    "remote no-op still needs its authoritative response/journal boundary"
                );
                assert!(
                    scanning.last_error.is_some(),
                    "test transport refuses that required route"
                );
                assert_ne!(scanning.state, DfsPlacementReconciliationState::Completed);
                continue;
            }
            assert_eq!(scanning.state, DfsPlacementReconciliationState::Repairing);
            assert!(scanning.last_error.is_none());
            assert_eq!(scanning.scanned_object_count, 1);
            assert_eq!(scanning.updated_object_count, 0);
            let completed = service
                .run_placement_reconciliation_bounded(8)
                .unwrap()
                .unwrap();
            assert_eq!(completed.state, DfsPlacementReconciliationState::Completed);
            assert_eq!(completed.pending_repair_count, 0);
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(placement_permit_event_count(&control), permits_before);
            assert_eq!(
                service
                    .current_read_object_metadata(&object.object_id)
                    .unwrap(),
                object
            );
            let intent = format!(
                "placement:{}:{}:{}",
                completed.job_id, object.object_id, object.version
            );
            assert!(service.coordinator.intent(&intent).is_none());
        }
    }

    #[tokio::test]
    async fn placement_noop_uses_primary_lookup_and_falls_back_for_changed_or_missing_rows() {
        for asynchronous in [false, true] {
            for reply in ["same", "changed", "missing"] {
                let root = sinomo_testdir::TestDir::new("dfs-placement-primary-noop-read");
                let (mut service, control, object, _) =
                    placement_page_noop_fixture(root.path(), true, "none");
                let mut objects = vec![object.clone()];
                if reply == "changed" {
                    objects[0].current_locations.clear();
                }
                if reply == "missing" {
                    objects.clear();
                }
                service.transport.lookup_response = Some(objects);
                let head = service
                    .metadata_backend()
                    .unwrap()
                    .journal_last_sequence()
                    .unwrap();
                let permits = placement_permit_event_count(&control);
                let job = if asynchronous {
                    service
                        .run_placement_reconciliation_bounded_async(8)
                        .await
                        .unwrap()
                        .unwrap()
                } else {
                    service
                        .run_placement_reconciliation_bounded(8)
                        .unwrap()
                        .unwrap()
                };
                let operations = service.transport.operations.lock().unwrap();
                assert!(
                    matches!(&operations[0], DfsNamespaceOperation::LookupPaths { paths } if paths == &vec![object.path.clone()])
                );
                if reply == "same" {
                    assert_eq!(operations.len(), 1, "fresh no-op needs no mutation RPC");
                    assert!(job.last_error.is_none());
                    assert_eq!(placement_permit_event_count(&control), permits);
                } else {
                    assert_eq!(operations.len(), 2);
                    assert!(matches!(
                        operations[1],
                        DfsNamespaceOperation::ReconcilePlacement { .. }
                    ));
                    assert!(
                        job.last_error.is_some(),
                        "the fake mutator refuses the fallback; it must not be skipped"
                    );
                }
                assert_eq!(
                    service
                        .metadata_backend()
                        .unwrap()
                        .journal_last_sequence()
                        .unwrap(),
                    head,
                    "a no-op proof must not replay or overwrite the local mirror"
                );
            }
        }
    }

    #[derive(Debug)]
    struct StaleNoopAuthorityKv {
        inner: SharedDfsControlPlaneKvClient<InProcessKvStore>,
        checks: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    fn placement_remote_batch_objects(
        service: &mut NetworkedDfsService<PlacementRouteCounter>,
        first: &DfsObjectMetadata,
        count: usize,
    ) -> Vec<DfsObjectMetadata> {
        let mut objects = vec![first.clone()];
        for index in 1..count {
            let mut object = first.clone();
            object.object_id =
                DfsObjectId::from_legacy_named(format!("remote-batch-object-{index}")).unwrap();
            object.path = format!("/team/z-batch-{index:03}.bin");
            service
                .metadata_backend
                .as_mut()
                .unwrap()
                .put_object(&object)
                .unwrap();
            objects.push(object);
        }
        objects
    }

    #[tokio::test]
    async fn placement_remote_noop_reads_batch_only_the_bounded_page_and_resume_exactly() {
        let root = sinomo_testdir::TestDir::new("placement-remote-batch-page");
        let (mut service, _control, first, calls) =
            placement_page_noop_fixture(root.path(), true, "none");
        let objects = placement_remote_batch_objects(&mut service, &first, 4);
        service.transport.lookup_response = Some(objects.clone());
        let first_page = service
            .run_placement_reconciliation_bounded_async(3)
            .await
            .unwrap()
            .unwrap();
        assert!(
            first_page.last_error.is_none(),
            "{:?}",
            first_page.last_error
        );
        assert_eq!(first_page.scanned_object_count, 3);
        assert_eq!(
            first_page.cursor.as_ref().unwrap().object_id,
            objects[2].object_id
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(matches!(&service.transport.operations.lock().unwrap()[0],
            DfsNamespaceOperation::LookupPaths { paths } if paths == &objects[..3].iter().map(|object| object.path.clone()).collect::<Vec<_>>()));
        let second_page = service
            .run_placement_reconciliation_bounded_async(3)
            .await
            .unwrap()
            .unwrap();
        assert!(
            second_page.last_error.is_none(),
            "{:?}",
            second_page.last_error
        );
        assert_eq!(second_page.scanned_object_count, 4);
        assert!(second_page.cursor.is_none());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(matches!(&service.transport.operations.lock().unwrap()[1],
            DfsNamespaceOperation::LookupPaths { paths } if paths == &vec![objects[3].path.clone()]));
    }

    #[tokio::test]
    async fn placement_remote_batch_proofs_cannot_authorize_missing_changed_or_ambiguous_objects() {
        for invalid in [
            "version",
            "missing",
            "duplicate-id",
            "duplicate-path",
            "object-id",
            "filesystem",
        ] {
            let root = sinomo_testdir::TestDir::new(&format!("placement-remote-batch-{invalid}"));
            let (mut service, _control, first, _) =
                placement_page_noop_fixture(root.path(), true, "none");
            let objects = placement_remote_batch_objects(&mut service, &first, 3);
            let mut response = objects.clone();
            match invalid {
                "version" => {
                    response[1].version =
                        DfsVersion::from_legacy_named("changed-batch-version").unwrap()
                }
                "missing" => {
                    response.remove(1);
                }
                "duplicate-id" => response.push(response[1].clone()),
                "duplicate-path" => {
                    let mut extra = response[1].clone();
                    extra.object_id =
                        DfsObjectId::from_legacy_named("different-batch-inode").unwrap();
                    response.push(extra);
                }
                "object-id" => response[1].object_id = objects[2].object_id.clone(),
                "filesystem" => {
                    response[1].file_system_id =
                        DfsFileSystemId::from_legacy_named("different-batch-fs").unwrap()
                }
                _ => unreachable!(),
            }
            service.transport.lookup_response = Some(response);
            let job = service
                .run_placement_reconciliation_bounded_async(3)
                .await
                .unwrap()
                .unwrap();
            let operations = service.transport.operations.lock().unwrap();
            assert_eq!(
                operations.len(),
                2,
                "one batch then exact offending-object mutation fallback: {invalid}"
            );
            assert!(
                matches!(&operations[0], DfsNamespaceOperation::LookupPaths { paths } if paths.len() == 3)
            );
            assert!(
                matches!(&operations[1], DfsNamespaceOperation::ReconcilePlacement { object_id, .. }
                if object_id == &objects[1].object_id),
                "another object's proof must not authorize the changed row: {invalid}"
            );
            assert!(
                job.last_error.is_some(),
                "fake mutator refuses the fallback"
            );
            assert_eq!(
                job.cursor.as_ref().unwrap().object_id,
                objects[0].object_id,
                "failed object cannot advance the exact preceding cursor"
            );
        }
    }

    #[tokio::test]
    async fn placement_remote_batch_proofs_bind_expected_config_assignment_and_object() {
        let root = sinomo_testdir::TestDir::new("placement-remote-batch-proof-fences");
        let (mut service, control, first, _) =
            placement_page_noop_fixture(root.path(), true, "none");
        let objects = placement_remote_batch_objects(&mut service, &first, 2);
        service.transport.lookup_response = Some(objects.clone());
        let job = control.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .placement_reconciliation(&sifs_file_system_id(), "placement-test-job")
                .unwrap()
                .unwrap()
        });
        let config = job.accepted_config.clone().unwrap();
        let authority = service.prepare_placement_page_authority(&config).unwrap();
        let started = Instant::now();
        let prepared = service
            .prepare_placement_reconciliation_objects(
                &config,
                &job,
                &placement_page_budget(&job, 2),
                started,
                authority.as_ref(),
            )
            .unwrap();
        let mut proofs = service
            .read_remote_placement_noop_proofs(&config, &prepared, started, 5_000)
            .await
            .unwrap();
        let shard = service
            .select_metadata_shard_for_object(&first)
            .unwrap()
            .unwrap();
        let mut changed_config = config.clone();
        changed_config.owner_node_id = node("new-batch-config-owner");
        assert!(!proofs.take_matches(&changed_config, &first, &shard));
        let mut proofs = service
            .read_remote_placement_noop_proofs(&config, &prepared, Instant::now(), 5_000)
            .await
            .unwrap();
        let mut changed_shard = shard.clone();
        changed_shard.primary_node_id = node("new-batch-primary");
        changed_shard.epoch += 1;
        assert!(!proofs.take_matches(&config, &first, &changed_shard));
        let mut proofs = service
            .read_remote_placement_noop_proofs(&config, &prepared, Instant::now(), 5_000)
            .await
            .unwrap();
        let mut changed_object = first.clone();
        changed_object.version = objects[1].version.clone();
        changed_object.path = objects[1].path.clone();
        assert!(!proofs.take_matches(&config, &changed_object, &shard));
        assert!(proofs.take_matches(&config, &objects[1], &shard));
        assert!(
            !proofs.take_matches(&config, &objects[1], &shard),
            "proof is consumed once within this invocation"
        );
    }

    impl DfsControlPlaneKvClient for StaleNoopAuthorityKv {
        fn get(
            &self,
            key: &str,
        ) -> Result<Option<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
            self.inner.get(key)
        }
        fn range_prefix(
            &self,
            prefix: &str,
        ) -> Result<Vec<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
            self.inner.range_prefix(prefix)
        }
        fn put(
            &mut self,
            key: String,
            value: Vec<u8>,
        ) -> Result<sinomo_core::kv::KvRevision, sinomo_core::kv::KvError> {
            self.inner.put(key, value)
        }
        fn txn(
            &mut self,
            txn: sinomo_core::kv::KvTxn,
        ) -> Result<sinomo_core::kv::KvTxnResponse, sinomo_core::kv::KvError> {
            self.inner.txn(txn)
        }
        fn get_authoritative_consistent(
            &mut self,
            keys: &[String],
        ) -> Result<Vec<Option<sinomo_core::kv::KvEntry>>, sinomo_core::kv::KvError> {
            self.checks
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut entries = self.inner.get_consistent(keys)?;
            for entry in entries.iter_mut().flatten() {
                if entry.key.contains("/metadata-shards/") {
                    let mut shard = entry.decode::<DfsMetadataShardConfig>()?;
                    shard.primary_node_id = node("node-b");
                    shard.epoch += 1;
                    entry.value = sinomo_core::kv::encode_kv_value(&shard)?;
                }
            }
            Ok(entries)
        }
    }

    #[test]
    fn placement_page_noop_requires_certified_local_authority_not_stale_selection() {
        let root = sinomo_testdir::TestDir::new("dfs-placement-page-noop-stale-authority");
        let (service, control, _, calls) = placement_page_noop_fixture(root.path(), false, "none");
        let checks = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut service = service.with_control_plane(StaleNoopAuthorityKv {
            inner: control.clone(),
            checks: checks.clone(),
        });
        let permits_before = placement_permit_event_count(&control);
        let before = control.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .placement_reconciliation(&sifs_file_system_id(), "placement-test-job")
                .unwrap()
                .unwrap()
        });
        let error = service
            .run_placement_reconciliation_bounded(8)
            .expect_err("the fresh page certificate must reject the old observational assignment");
        assert!(
            matches!(error, DfsError::Distributed { .. }),
            "unexpected stale-authority result: {error}"
        );
        assert!(
            checks.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "the observational old local assignment is insufficient"
        );
        let after = control.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .placement_reconciliation(&sifs_file_system_id(), "placement-test-job")
                .unwrap()
                .unwrap()
        });
        assert_eq!(
            after, before,
            "uncertified authority cannot checkpoint any page progress"
        );
        assert_eq!(placement_permit_event_count(&control), permits_before);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn placement_repair_obligations_survive_message_handoff_replay() {
        let root = sinomo_testdir::TestDir::new("dfs-placement-repair-handoff");
        let (mut service, _control, object, _) =
            placement_page_noop_fixture(root.path(), false, "missing");
        let intent_id = format!(
            "placement:placement-test-job:{}:{}",
            object.object_id, object.version
        );
        let mut first_head = None;
        for attempt in 0..3 {
            let rebuilds = service.coordinator.repair_job_rebuild_count;
            service.begin_message_committed_work();
            let job = service
                .run_placement_reconciliation_bounded(8)
                .unwrap()
                .unwrap();
            assert_eq!(job.state, DfsPlacementReconciliationState::Repairing);
            let queued = service.take_message_committed_repair_queue_records();
            assert_eq!(
                queued.len(),
                1,
                "current placement repairs need a durable message handoff, including replay {attempt}"
            );
            assert_eq!(queued[0].intent_id, intent_id);
            assert_eq!(queued[0].missing_replica_node_id, node("node-b"));
            assert_eq!(
                queued[0].content_hash,
                *object.content_hash.as_ref().unwrap()
            );
            assert!(service.take_message_committed_work().repair_maintenance);
            assert_eq!(
                service.coordinator.repair_job_rebuild_count, rebuilds,
                "one placement intent must not rebuild every historical repair"
            );
            let head = service
                .metadata_backend
                .as_ref()
                .unwrap()
                .journal_last_sequence()
                .unwrap();
            if let Some(first_head) = first_head {
                assert_eq!(
                    head, first_head,
                    "re-emitting the handoff must not rewrite settled intent or receipt records"
                );
            } else {
                first_head = Some(head);
            }
            // Losing an earlier volatile handoff must not strand the durable
            // committed intent. A fresh authority-checked poll may reactivate
            // this exact current job after cached work was retired.
            if attempt == 1 {
                service
                    .coordinator
                    .set_committed_repair_active(&intent_id, false);
            }
        }
        service
            .put_metadata_receipt(
                &object.file_system_id,
                &object.object_id,
                &object.version,
                &node("node-b"),
                object.content_hash.as_ref().unwrap(),
            )
            .unwrap();
        service.begin_message_committed_work();
        let completed = service
            .run_placement_reconciliation_bounded(8)
            .unwrap()
            .unwrap();
        assert_eq!(completed.state, DfsPlacementReconciliationState::Completed);
        assert!(
            service
                .take_message_committed_repair_queue_records()
                .is_empty()
        );
        assert!(!service.take_message_committed_work().repair_maintenance);
    }

    #[test]
    fn placement_page_noop_repairing_rechecks_a_concurrent_new_version() {
        let root = sinomo_testdir::TestDir::new("dfs-placement-page-noop-new-version");
        let (mut service, control, object, _) =
            placement_page_noop_fixture(root.path(), false, "none");
        let scanning = service
            .run_placement_reconciliation_bounded(8)
            .unwrap()
            .unwrap();
        assert_eq!(scanning.state, DfsPlacementReconciliationState::Repairing);
        let permits_before = placement_permit_event_count(&control);
        let mut newer = object.clone();
        newer.version = DfsVersion::from_legacy_named("placement-after-noop-new-version").unwrap();
        newer.current_locations = vec![node("node-b")];
        let new_bytes = vec![0x6b; 4096];
        let new_hash = crate::content_hash(&new_bytes);
        assert_ne!(object.content_hash.as_deref(), Some(new_hash.as_str()));
        newer.content_hash = Some(new_hash.clone());
        let remote_content = LocalContentStore::new(root.join("remote-new-version")).unwrap();
        remote_content
            .put_object_version(newer.object_id.as_str(), newer.version.as_str(), &new_bytes)
            .expect("only the remote replica stores the new payload");
        assert!(!service.content().has_blob(&new_hash).unwrap());
        {
            // Model the intervening published metadata replacement through
            // the same original-read-set fence as a real namespace author.
            let _planning = service.namespace_execution.begin_planning();
            assert_eq!(
                service
                    .current_read_object_metadata(&object.object_id)
                    .unwrap(),
                object
            );
            service
                .add_object_metadata(newer.clone())
                .expect("replace the version under its captured original read set");
        }
        assert_eq!(
            service
                .current_read_object_metadata(&newer.object_id)
                .unwrap(),
            newer
        );
        assert!(!service.content().has_blob(&new_hash).unwrap());

        let repairing = service
            .run_placement_reconciliation_bounded(8)
            .unwrap()
            .unwrap();
        assert!(
            repairing.last_error.is_none(),
            "repairing should observe the new version: {:?}",
            repairing.last_error
        );
        assert_eq!(repairing.state, DfsPlacementReconciliationState::Repairing);
        assert_eq!(
            repairing.pending_repair_count, 1,
            "fresh owner view cannot claim stale scan completion"
        );
        assert!(placement_permit_event_count(&control) > permits_before);
        let intent_id = format!(
            "placement:{}:{}:{}",
            repairing.job_id, newer.object_id, newer.version
        );
        let intent = service
            .coordinator
            .intent(&intent_id)
            .expect("new version needs real repair");
        assert_eq!(intent.target_version, newer.version);
        assert_eq!(intent.desired_replica_nodes, vec![node("node-a")]);
        assert!(
            !service.content().has_blob(&new_hash).unwrap(),
            "planning repair is not a local receipt"
        );
        assert_eq!(
            remote_content
                .read_object_version(newer.object_id.as_str(), newer.version.as_str())
                .unwrap()
                .1,
            new_bytes
        );
    }

    #[test]
    fn placement_page_noop_does_not_skip_policy_missing_or_obsolete_changes() {
        for remote in [false, true] {
            for change in ["policy", "missing", "obsolete"] {
                let root = sinomo_testdir::TestDir::new("dfs-placement-page-needs-change");
                let (mut service, control, object, calls) =
                    placement_page_noop_fixture(root.path(), remote, change);
                let permits_before = placement_permit_event_count(&control);
                let progressed = service
                    .run_placement_reconciliation_bounded(8)
                    .unwrap()
                    .unwrap();
                if remote {
                    assert!(
                        calls.load(std::sync::atomic::Ordering::SeqCst) > 0,
                        "changed remote object must still be routed"
                    );
                    assert!(
                        progressed.last_error.is_some(),
                        "test transport rejects the attempted route"
                    );
                } else {
                    assert!(
                        progressed.last_error.is_none(),
                        "local {change} mutation should complete: {:?}",
                        progressed.last_error
                    );
                    assert!(
                        placement_permit_event_count(&control) > permits_before,
                        "real local mutation retains its durable permit"
                    );
                    let current = service
                        .current_read_object_metadata(&object.object_id)
                        .unwrap();
                    match change {
                        "policy" => {
                            assert_eq!(current.policy, DfsObjectPolicy::ReplicatedConfirmed)
                        }
                        "obsolete" => assert_eq!(current.current_locations, vec![node("node-a")]),
                        "missing" => {
                            let intent = format!(
                                "placement:{}:{}:{}",
                                progressed.job_id, object.object_id, object.version
                            );
                            assert_eq!(
                                service
                                    .coordinator
                                    .intent(&intent)
                                    .unwrap()
                                    .desired_replica_nodes
                                    .len(),
                                2
                            );
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }
    }

    #[test]
    fn placement_local_rule_defaults_preserve_explicit_validation_and_replicated_defaults() {
        for consistency in [DfsObjectPolicy::LocalOwned, DfsObjectPolicy::EphemeralLocal] {
            let legacy = DfsSubtreePolicy::legacy("/local", consistency.clone());
            let resolved = placement_policy_from_rule(&legacy, 6).expect("local omitted counts");
            assert_eq!(resolved.consistency, consistency);
            assert_eq!(
                (resolved.min_commit_replicas, resolved.desired_replicas),
                (1, 1)
            );

            let mut explicit = legacy.clone();
            explicit.min_commit_replicas = Some(1);
            explicit.desired_replicas = Some(1);
            assert_eq!(placement_policy_from_rule(&explicit, 6).unwrap(), resolved);

            let mut invalid = Vec::new();
            for count in [0, 2] {
                let mut rule = legacy.clone();
                rule.desired_replicas = Some(count);
                invalid.push(rule);
                let mut rule = legacy.clone();
                rule.min_commit_replicas = Some(count);
                invalid.push(rule);
            }
            let mut required = legacy.clone();
            required.required_node_ids.push(node("node-a"));
            invalid.push(required);
            let mut forbidden = legacy.clone();
            forbidden.forbidden_node_ids.push(node("node-b"));
            invalid.push(forbidden);
            let mut zoned = legacy;
            zoned.min_distinct_zones = 1;
            invalid.push(zoned);
            for rule in invalid {
                assert!(matches!(
                    placement_policy_from_rule(&rule, 6),
                    Err(DfsDistributedError::InvalidPolicy { .. })
                ));
            }
        }
        for (consistency, expected_w) in [
            (DfsObjectPolicy::ReplicatedEventual, 1),
            (DfsObjectPolicy::ReplicatedConfirmed, 2),
        ] {
            let rule = DfsSubtreePolicy::legacy("/replicated", consistency);
            let resolved =
                placement_policy_from_rule(&rule, 6).expect("existing replicated default");
            assert_eq!(
                (resolved.min_commit_replicas, resolved.desired_replicas),
                (expected_w, 2)
            );
        }
    }

    #[test]
    fn structurally_invalid_reconciliation_policy_fails_terminally() {
        let owner = node("node-a");
        let base = placement_config(owner.clone());
        let mut invalid = base.clone();
        invalid.subtree_policies.push(DfsSubtreePolicy {
            path_prefix: "/team".to_string(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            scope: DfsPlacementScope::Subtree,
            min_commit_replicas: Some(2),
            desired_replicas: Some(1),
            required_node_ids: Vec::new(),
            forbidden_node_ids: Vec::new(),
            min_distinct_zones: 0,
        });
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        control_plane.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv);
            store.put_file_system_config(&base).expect("base config");
            let mut job = reconciliation_job("/team", DfsPlacementScope::Subtree, BTreeSet::new());
            job.job_id = "placement-invalid".to_string();
            job.accepted_config = Some(invalid.clone());
            assert!(
                store
                    .compare_and_put_placement_update(&base, &invalid, &job)
                    .expect("publish invalid legacy config")
            );
        });

        let root = sinomo_testdir::TestDir::new("dfs-placement-invalid-job");
        let content = LocalContentStore::new(root.join("content")).expect("content store");
        let mut service = NetworkedDfsService::new(
            PreviewTransport {
                local_node_id: owner.clone(),
            },
            content,
        );
        service
            .add_object_metadata(placement_object(
                "invalid-job-object",
                "/team/object",
                DfsObjectKind::Directory,
                owner,
                Vec::new(),
                None,
            ))
            .expect("object metadata");
        let mut service = service.with_control_plane(control_plane);

        let failed = service
            .run_placement_reconciliation_bounded(8)
            .expect("reconciliation tick")
            .expect("invalid job");
        assert_eq!(failed.state, DfsPlacementReconciliationState::Failed);
        assert!(failed.state.is_terminal());
        assert!(
            failed
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("invalid DFS replication policy"))
        );
    }

    #[test]
    fn aggregate_impact_counts_scope_bytes_and_replica_deltas() {
        let root = sinomo_testdir::TestDir::new("dfs-placement-impact");
        let owner = node("node-a");
        let content = LocalContentStore::new(root.join("content")).expect("content store");
        let mut service = NetworkedDfsService::new(
            PreviewTransport {
                local_node_id: owner.clone(),
            },
            content,
        );
        service
            .add_object_metadata_batch(vec![
                placement_object(
                    "known",
                    "/team/known.bin",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a")],
                    Some(10),
                ),
                placement_object(
                    "unknown",
                    "/team/unknown.bin",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a"), node("node-b")],
                    None,
                ),
                placement_object(
                    "directory",
                    "/team/subdir",
                    DfsObjectKind::Directory,
                    owner.clone(),
                    Vec::new(),
                    None,
                ),
                placement_object(
                    "outside",
                    "/elsewhere/file.bin",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a")],
                    Some(99),
                ),
            ])
            .expect("metadata");
        let config = placement_config(owner);
        let candidates = vec![
            advertisement("node-a", "east"),
            advertisement("node-b", "west"),
        ];

        let subtree = service
            .placement_impact_for_config(&config, "/team", DfsPlacementScope::Subtree, &candidates)
            .expect("subtree impact");
        assert_eq!(
            subtree,
            DfsPlacementImpact {
                affected_object_count: 1,
                affected_bytes: 10,
                unknown_size_object_count: 0,
                copies_added: 1,
                copies_removed: 0,
                metadata_snapshot_digest: subtree.metadata_snapshot_digest.clone(),
                complete: true,
            }
        );

        let single = service
            .placement_impact_for_config(
                &config,
                "/team/known.bin",
                DfsPlacementScope::SelfOnly,
                &candidates,
            )
            .expect("single-object impact");
        assert_eq!(single.affected_object_count, 1);
        assert_eq!(single.affected_bytes, 10);
        assert_eq!(single.copies_added, 1);
    }

    #[test]
    fn aggregate_impact_excludes_objects_preserved_by_nested_override() {
        let root = sinomo_testdir::TestDir::new("dfs-placement-impact-nested");
        let owner = node("node-a");
        let content = LocalContentStore::new(root.join("content")).expect("content store");
        let mut service = NetworkedDfsService::new(
            PreviewTransport {
                local_node_id: owner.clone(),
            },
            content,
        );
        service
            .add_object_metadata_batch(vec![
                placement_object(
                    "changed",
                    "/team/changed.bin",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a")],
                    Some(10),
                ),
                placement_object(
                    "nested-stable",
                    "/team/nested/stable.bin",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a")],
                    Some(20),
                ),
            ])
            .expect("metadata");
        let mut config = placement_config(owner);
        for (path, replicas) in [("/team", 2), ("/team/nested", 1)] {
            upsert_placement_rule(
                &mut config.subtree_policies,
                normalize_placement_rule(
                    path,
                    DfsPlacementScope::Subtree,
                    DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, replicas)
                        .expect("placement policy"),
                )
                .expect("placement rule"),
            );
        }

        let impact = service
            .placement_impact_for_config(
                &config,
                "/team",
                DfsPlacementScope::Subtree,
                &[
                    advertisement("node-a", "east"),
                    advertisement("node-b", "west"),
                ],
            )
            .expect("nested impact");

        assert_eq!(
            impact,
            DfsPlacementImpact {
                affected_object_count: 1,
                affected_bytes: 10,
                unknown_size_object_count: 0,
                copies_added: 1,
                copies_removed: 0,
                metadata_snapshot_digest: impact.metadata_snapshot_digest.clone(),
                complete: true,
            }
        );
    }

    #[test]
    fn aggregate_impact_reports_nested_unsatisfiable_override_and_is_incomplete() {
        let root = sinomo_testdir::TestDir::new("dfs-placement-impact-nested-unsatisfiable");
        let owner = node("node-a");
        let content = LocalContentStore::new(root.join("content")).expect("content store");
        let mut service = NetworkedDfsService::new(
            PreviewTransport {
                local_node_id: owner.clone(),
            },
            content,
        );
        service
            .add_object_metadata_batch(vec![
                placement_object(
                    "parent-valid",
                    "/team/valid.bin",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a")],
                    Some(10),
                ),
                placement_object(
                    "punctuation-sibling",
                    "/team-other",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a")],
                    Some(99),
                ),
                placement_object(
                    "nested-invalid",
                    "/team/nested/invalid.bin",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a")],
                    Some(20),
                ),
            ])
            .expect("metadata");
        let mut config = placement_config(owner);
        for (path, replicas) in [("/team", 1), ("/team/nested", 3)] {
            upsert_placement_rule(
                &mut config.subtree_policies,
                normalize_placement_rule(
                    path,
                    DfsPlacementScope::Subtree,
                    DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, replicas)
                        .expect("policy"),
                )
                .expect("rule"),
            );
        }
        let candidates = vec![
            advertisement("node-a", "east"),
            advertisement("node-b", "west"),
        ];

        let scan = service
            .placement_impact_scan_for_config(
                &config,
                "/team",
                DfsPlacementScope::Subtree,
                &candidates,
                &BTreeSet::new(),
            )
            .expect("aggregate scan");
        assert!(!scan.impact.complete);
        assert!(scan.blocking_diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic,
                DfsPlacementBlockingDiagnostic::ScopedPolicy {
                    path,
                    scope: DfsPlacementScope::Subtree,
                    diagnostic,
                } if path == "/team/nested" && matches!(
                    diagnostic.as_ref(),
                    DfsPlacementBlockingDiagnostic::InsufficientCandidates {
                        available: 2,
                        required: 3,
                    }
                )
            )
        }));

        let whole = service
            .placement_impact_for_config(&config, "/team", DfsPlacementScope::Subtree, &candidates)
            .expect("whole impact");
        assert!(!whole.complete);
    }

    #[test]
    fn aggregate_impact_pages_past_the_first_metadata_chunk() {
        let root = sinomo_testdir::TestDir::new("dfs-placement-impact-pages");
        let owner = node("node-a");
        let content = LocalContentStore::new(root.join("content")).expect("content store");
        let mut service = NetworkedDfsService::new(
            PreviewTransport {
                local_node_id: owner.clone(),
            },
            content,
        );
        let objects = (0..(PLACEMENT_IMPACT_PAGE_SIZE + 17))
            .map(|index| {
                placement_object(
                    &format!("paged-{index:04}"),
                    &format!("/paged/item-{index:04}"),
                    DfsObjectKind::Directory,
                    owner.clone(),
                    Vec::new(),
                    None,
                )
            })
            .collect::<Vec<_>>();
        service
            .add_object_metadata_batch(objects)
            .expect("paged metadata");
        let mut config = placement_config(owner);
        upsert_placement_rule(
            &mut config.subtree_policies,
            normalize_placement_rule(
                "/paged",
                DfsPlacementScope::Subtree,
                DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1)
                    .expect("confirmed policy"),
            )
            .expect("paged rule"),
        );

        let impact = service
            .placement_impact_for_config(
                &config,
                "/paged",
                DfsPlacementScope::Subtree,
                &[advertisement("node-a", "east")],
            )
            .expect("paged impact");
        assert_eq!(
            impact.affected_object_count,
            (PLACEMENT_IMPACT_PAGE_SIZE + 17) as u64
        );
        assert_eq!(impact.affected_bytes, 0);
        assert_eq!(impact.copies_added, 0);

        let first_page = service
            .placement_impact_page_for_config(
                &config,
                "/paged",
                DfsPlacementScope::Subtree,
                &[advertisement("node-a", "east")],
                None,
                31,
            )
            .expect("bounded page");
        assert_eq!(first_page.impact.affected_object_count, 31);
        assert!(first_page.next_cursor.is_some());
        assert!(first_page.blocking_diagnostics.is_empty());
    }

    #[test]
    fn placement_activity_status_distinguishes_retrying_from_terminal_failure() {
        let mut job = reconciliation_job("/team", DfsPlacementScope::Subtree, BTreeSet::new());
        assert_eq!(
            job.activity_status(),
            DfsPlacementReconciliationActivityStatus::Running
        );

        job.state = DfsPlacementReconciliationState::Scanning;
        job.last_error = Some("candidate temporarily unreachable".to_string());
        assert_eq!(
            job.activity_status(),
            DfsPlacementReconciliationActivityStatus::Retrying
        );

        job.state = DfsPlacementReconciliationState::Failed;
        assert_eq!(
            job.activity_status(),
            DfsPlacementReconciliationActivityStatus::Failed
        );
        job.last_error = Some(PLACEMENT_RECONCILIATION_SUPERSEDED_ERROR.to_string());
        assert_eq!(
            job.activity_status(),
            DfsPlacementReconciliationActivityStatus::Superseded,
            "an expected replacement must not be presented as an operational failure"
        );
        job.last_error = Some(format!(
            "{}replacement",
            sinomo_api::dfs::placement::PLACEMENT_RECONCILIATION_SUPERSEDED_ADAPTIVE_PREFIX
        ));
        assert_eq!(
            job.activity_status(),
            DfsPlacementReconciliationActivityStatus::Superseded
        );
        let mut legacy_summary = DfsPlacementReconciliationActivitySummary::from(&job);
        legacy_summary.status = DfsPlacementReconciliationActivityStatus::Failed;
        let page = placement_reconciliation_activity_page_from_summaries(
            [legacy_summary],
            &[DfsPlacementReconciliationActivityStatus::Superseded],
            None,
            None,
            10,
        );
        assert_eq!(
            page.activities.len(),
            1,
            "stored summaries normalize before status filtering"
        );
        assert_eq!(
            page.activities[0].status,
            DfsPlacementReconciliationActivityStatus::Superseded
        );
        job.state = DfsPlacementReconciliationState::Completed;
        assert_eq!(
            job.activity_status(),
            DfsPlacementReconciliationActivityStatus::Completed
        );
    }

    #[test]
    fn placement_activity_page_is_stable_newest_first_and_filterable() {
        let mut oldest =
            reconciliation_job("/team/a", DfsPlacementScope::SelfOnly, BTreeSet::new());
        oldest.job_id = "activity-a".to_string();
        oldest.created_at_unix_nanos = 1;

        let mut tied = oldest.clone();
        tied.job_id = "activity-b".to_string();
        tied.path = "/team/b".to_string();
        tied.last_error = Some("retry later".to_string());

        let mut newest = oldest.clone();
        newest.job_id = "activity-c".to_string();
        newest.path = "/other".to_string();
        newest.created_at_unix_nanos = 2;
        newest.state = DfsPlacementReconciliationState::Completed;

        let jobs = [oldest, newest, tied];
        let first = placement_reconciliation_activity_page(jobs.iter(), &[], None, None, 2);
        assert_eq!(
            first
                .activities
                .iter()
                .map(|activity| activity.job_id.as_str())
                .collect::<Vec<_>>(),
            vec!["activity-c", "activity-b"]
        );
        let second = placement_reconciliation_activity_page(
            jobs.iter(),
            &[],
            None,
            first.next_cursor.as_ref(),
            2,
        );
        assert_eq!(second.activities.len(), 1);
        assert_eq!(second.activities[0].job_id, "activity-a");
        assert!(second.next_cursor.is_none());

        let retrying = placement_reconciliation_activity_page(
            jobs.iter(),
            &[DfsPlacementReconciliationActivityStatus::Retrying],
            Some("/team"),
            None,
            10,
        );
        assert_eq!(retrying.activities.len(), 1);
        assert_eq!(retrying.activities[0].job_id, "activity-b");
        assert_eq!(
            retrying.activities[0].status,
            DfsPlacementReconciliationActivityStatus::Retrying
        );

        let boundary =
            placement_reconciliation_activity_page(jobs.iter(), &[], Some("/tea"), None, 10);
        assert!(boundary.activities.is_empty());
    }

    #[test]
    fn placement_activity_page_enforces_the_wire_limit() {
        let jobs = (0..(MAX_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE + 1))
            .map(|index| {
                let mut job = reconciliation_job(
                    &format!("/activity/{index}"),
                    DfsPlacementScope::SelfOnly,
                    BTreeSet::new(),
                );
                job.job_id = format!("activity-{index:04}");
                job.created_at_unix_nanos = index as u64;
                job
            })
            .collect::<Vec<_>>();
        let page = placement_reconciliation_activity_page(jobs.iter(), &[], None, None, usize::MAX);
        assert_eq!(
            page.activities.len(),
            MAX_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE
        );
        assert!(page.next_cursor.is_some());
    }

    impl crate::AsyncDfsMessageTransport for PlacementRouteCounter {
        fn local_node_id(&self) -> &NodeId {
            &self.local_node_id
        }
        fn send_routed_call_async<'a>(
            &'a mut self,
            target: &'a NodeId,
            call: DfsRoutedCall,
        ) -> crate::DfsMessageTransportFuture<'a, Result<(), NetworkError>> {
            Box::pin(async move {
                if let Some(stall) = &self.stall {
                    self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    stall.entered.notify_one();
                    std::future::pending::<()>().await;
                }
                DfsMessageTransport::send_routed_call(self, target, call)
            })
        }
        fn drain_incoming_messages_async<'a>(
            &'a mut self,
        ) -> crate::DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedMessage>, NetworkError>>
        {
            Box::pin(async move { DfsMessageTransport::drain_incoming_messages(self) })
        }
        fn pending_incoming_count_async<'a>(
            &'a mut self,
        ) -> crate::DfsMessageTransportFuture<'a, Result<Option<usize>, NetworkError>> {
            Box::pin(async move { DfsMessageTransport::pending_incoming_count(self) })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn history_query_admission_preempts_real_placement_rpc() {
        let root = sinomo_testdir::TestDir::new("history-placement-admission");
        let (mut service, control, _object, calls) =
            placement_page_noop_fixture(root.path(), true, "none");
        let stall = std::sync::Arc::new(PlacementPeerStall::default());
        service.transport.stall = Some(stall.clone());
        let data_dir = sinomo_core::local::SinomoDataDir::new(root.path());
        let scheduler = std::sync::Arc::new(crate::DfsMutationScheduler::new());
        let interest = scheduler.begin_maintenance_interest();
        let worker_scheduler = scheduler.clone();
        let runtime = tokio::runtime::Handle::current();
        let worker = tokio::task::spawn_blocking(move || {
            runtime.block_on(async move {
                let _admission = worker_scheduler
                    .admit(&crate::DfsMessageExecutionScope::GlobalExclusive)
                    .await
                    .unwrap();
                let mut host = crate::LocalDfsMaintenanceHost::new(data_dir.clone());
                let mut publisher =
                    crate::LocalDfsMetadataJournalPublisherHost::new(data_dir.clone());
                let mut transport = crate::AsyncDfsMetadataNodeTransport::new(
                    Vec::new(),
                    std::sync::Arc::new(|_, _, _| {
                        Box::pin(async { panic!("publication disabled") })
                    }),
                    std::sync::Arc::new(|_, _, _| {
                        Box::pin(async { panic!("publication disabled") })
                    }),
                );
                let local = service.node_id().clone();
                crate::run_dfs_maintenance_until_foreground_waits(
                    &interest,
                    crate::run_dfs_maintenance_tick_with_async_host(
                        &mut service,
                        &mut host,
                        &data_dir,
                        sifs_file_system_id(),
                        &local,
                        Vec::new(),
                        &mut publisher,
                        &mut transport,
                        crate::DfsMaintenanceTickOptions {
                            publish_metadata: false,
                            ..Default::default()
                        },
                    ),
                )
                .await
            })
        });
        tokio::time::timeout(Duration::from_secs(5), stall.entered.notified())
            .await
            .unwrap();
        // This is the production exact-shard admission used by history queries.
        // The actual maintenance tick must yield its global owner while its
        // peer is stalled; a sync send cannot observe the foreground request.
        scheduler.request_foreground_admission();
        let history = tokio::time::timeout(Duration::from_millis(500), async {
            let _admission = scheduler.admit(&crate::DfsMessageExecutionScope::ExactShard {
                file_system_id: sifs_file_system_id(), shard_id: crate::DfsMetadataShardId::new("root"),
            }).await.unwrap();
            let mut backend = open_root_local_metadata_backend(root.path().join("metadata")).unwrap();
            backend.begin_consistent_read_snapshot().unwrap();
            let query = crate::DfsFileHistoryShardQuery {
                file_system_id: sifs_file_system_id(), shard_id: crate::DfsMetadataShardId::new("root"), epoch: 1,
                selection: crate::DfsMetadataSelection::RetainedRevisions { namespace: Some("module.dfs.history".into()), path: Some("/unrelated".into()), object_id: None, after: None, limit: 128 },
            };
            let result = crate::file_history_query::query_backend_metadata(&backend, &query).unwrap();
            backend.end_consistent_read_snapshot();
            assert!(matches!(result, crate::DfsMetadataQueryResult::RetainedRevisions { entries, .. } if entries.is_empty()));
        }).await;
        *stall.released.lock().unwrap() = true;
        stall.release.notify_all();
        let outcome = worker.await.unwrap();
        assert!(
            history.is_ok(),
            "history query timed out behind a synchronous maintenance peer RPC"
        );
        assert!(matches!(
            outcome,
            crate::DfsMaintenancePreemptionOutcome::ForegroundWaiting
        ));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let job = control.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .placement_reconciliation(&sifs_file_system_id(), "placement-test-job")
                .unwrap()
                .unwrap()
        });
        assert!(
            job.cursor.is_none(),
            "cancelled page cannot claim remote completion"
        );
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn foreground_admission_preempts_native_placement_progress_rpc() {
        let root = sinomo_testdir::TestDir::new("placement-native-progress-preemption");
        let (mut service, original, _object, _calls) =
            placement_page_noop_fixture(root.path(), false, "none");
        let data_dir = sinomo_core::local::SinomoDataDir::new(root.path());
        data_dir.ensure_root_layout().unwrap();
        let paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("placement-native-progress").unwrap(),
            "placement native progress",
            service.node_id().clone(),
            "local",
            "pk-local",
            time::OffsetDateTime::now_utc(),
        );
        let local_member = state.members.first_mut().expect("native local voter");
        local_member.capabilities = sinomo_core::node::main_daemon_capabilities(&local_member.role);
        local_member.allowed_capabilities = local_member.capabilities.clone();
        sinomo_core::cluster::write_cluster_state(&paths, &state).unwrap();
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state).unwrap();
        let fixture_entries = original.with_client(|kv| kv.try_snapshot().unwrap().entries);
        let gate =
            std::sync::Arc::new(crate::runtime_host::NativePlacementProgressRpcGate::default());
        let runtime = tokio::runtime::Handle::current();
        service.control_plane = Some(
            crate::runtime_host::gated_native_placement_control_plane_for_test(
                &paths,
                &runtime,
                fixture_entries,
                gate.clone(),
            ),
        );
        let original_job = {
            let control = service.control_plane.as_mut().unwrap();
            DfsControlPlaneStore::new(control.as_mut())
                .placement_reconciliation(&sifs_file_system_id(), "placement-test-job")
                .unwrap()
                .unwrap()
        };
        let scheduler = std::sync::Arc::new(crate::DfsMutationScheduler::new());
        let interest = scheduler.begin_maintenance_interest();
        let worker_scheduler = scheduler.clone();
        let worker_dir = data_dir.clone();
        let worker = tokio::task::spawn_blocking(move || {
            runtime.block_on(async move {
                let _admission = worker_scheduler
                    .admit(&crate::DfsMessageExecutionScope::GlobalExclusive)
                    .await
                    .unwrap();
                crate::run_dfs_maintenance_until_foreground_waits(&interest, async move {
                    let mut host = crate::LocalDfsMaintenanceHost::new(worker_dir.clone());
                    let mut publisher =
                        crate::LocalDfsMetadataJournalPublisherHost::new(worker_dir.clone());
                    let mut transport = crate::AsyncDfsMetadataNodeTransport::new(
                        Vec::new(),
                        std::sync::Arc::new(|_, _, _| {
                            Box::pin(async { panic!("publication disabled") })
                        }),
                        std::sync::Arc::new(|_, _, _| {
                            Box::pin(async { panic!("publication disabled") })
                        }),
                    );
                    let local = service.node_id().clone();
                    crate::run_dfs_maintenance_tick_with_async_host(
                        &mut service,
                        &mut host,
                        &worker_dir,
                        sifs_file_system_id(),
                        &local,
                        Vec::new(),
                        &mut publisher,
                        &mut transport,
                        crate::DfsMaintenanceTickOptions {
                            publish_metadata: false,
                            ..Default::default()
                        },
                    )
                    .await
                })
                .await
            })
        });
        tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            gate.entered.notified(),
        )
        .await
        .expect("real native progress RPC reached Pending");
        assert!(scheduler.global_is_exclusively_held());
        scheduler.request_foreground_admission();
        let admission = tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            scheduler.admit(&crate::DfsMessageExecutionScope::GlobalExclusive),
        )
        .await
        .expect("foreground admitted before pending RPC was released")
        .unwrap();
        assert!(gate.cancelled.load(std::sync::atomic::Ordering::Acquire));
        assert!(matches!(
            worker.await.unwrap(),
            crate::DfsMaintenancePreemptionOutcome::ForegroundWaiting
        ));
        // The pending future has been destroyed and its complete service dropped
        // before foreground receives ownership. Its uncommitted cursor cannot
        // claim a page merely because the local object work finished.
        use sinomo_core::cluster::ClusterKvPathsExt as _;
        let key = crate::placement_reconciliation_key(&sifs_file_system_id(), "placement-test-job");
        let read = sinomo_core::kv::with_cluster_kv_resident_read_projection_before(
            &paths.kv_paths().raft_wal_paths(&state.local_node_id),
            &state.local_node_id,
            Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            |projection| projection.read_index_entry(&key),
        )
        .expect("resident native owner remains available")
        .expect("native read owner completed after cancelled RPC");
        let retained: DfsPlacementReconciliationJob = read
            .value
            .expect("bounded native point read")
            .expect("original durable placement job")
            .decode()
            .unwrap();
        assert_eq!(retained, original_job);
        drop(admission);
    }

    #[tokio::test]
    async fn explicit_placement_saves_completed_prefix_when_time_slice_expires() {
        use std::future::Future;
        let root = sinomo_testdir::TestDir::new("dfs-placement-time-slice");
        let (mut service, control, first, _) =
            placement_page_noop_fixture(root.path(), false, "none");
        let mut second = first.clone();
        second.object_id = DfsObjectId::from_legacy_named("time-slice-second").unwrap();
        second.path = "/team/second.bin".into();
        service.add_object_metadata(second.clone()).unwrap();
        let mut page = Box::pin(service.run_placement_reconciliation_bounded_async(256));
        let yielded =
            std::future::poll_fn(|cx| std::task::Poll::Ready(page.as_mut().poll(cx).is_pending()))
                .await;
        assert!(yielded);
        // Model a busy executor resuming after the time slice. The next object
        // must wait for a fresh pass; the successful prefix is committed now.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let partial = page.await.unwrap().unwrap();
        assert!(!partial.adaptive);
        assert_eq!(partial.scanned_object_count, 1);
        assert_eq!(partial.cursor.as_ref().unwrap().object_id, first.object_id);
        let persisted = control.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .placement_reconciliation(&sifs_file_system_id(), &partial.job_id)
                .unwrap()
                .unwrap()
        });
        assert_eq!(persisted, partial);
        let resumed = service
            .run_placement_reconciliation_bounded_async(256)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resumed.scanned_object_count, 2);
        assert!(resumed.cursor.is_none());
        assert_eq!(resumed.state, DfsPlacementReconciliationState::Repairing);
    }

    fn parked_placement_head_fixture(
        root: &std::path::Path,
    ) -> (
        NetworkedDfsService<PlacementRouteCounter>,
        SharedDfsControlPlaneKvClient<InProcessKvStore>,
        DfsPlacementReconciliationJob,
        DfsPlacementReconciliationJob,
    ) {
        let (mut service, control, _, _) = placement_page_noop_fixture(root, false, "missing");
        assert_eq!(
            service
                .run_placement_reconciliation_bounded(8)
                .unwrap()
                .unwrap()
                .state,
            DfsPlacementReconciliationState::Repairing
        );
        let head = service
            .run_placement_reconciliation_bounded(8)
            .unwrap()
            .unwrap();
        assert!(head.cursor.is_none());
        assert_eq!(head.pending_repair_count, 1);
        assert!(
            !service.durable_repair_queue_records().is_empty(),
            "fixture owns real missing-replica work, not a synthetic pending count"
        );
        let mut tail = reconciliation_job(
            "/absent-eligible-tail",
            DfsPlacementScope::SelfOnly,
            BTreeSet::new(),
        );
        tail.job_id = "eligible-placement-tail".into();
        tail.created_at_unix_nanos = head.created_at_unix_nanos + 1;
        tail.accepted_config = head.accepted_config.clone();
        tail.candidate_snapshot = head.candidate_snapshot.clone();
        let tail = control.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv);
            let config = store
                .get_file_system_config(&head.file_system_id)
                .unwrap()
                .unwrap();
            assert!(
                store
                    .compare_and_put_placement_update(&config, &config, &tail)
                    .unwrap()
            );
            // Publication records the tail's exact scoped configuration; use
            // that durable value for later progress compares.
            store
                .placement_reconciliation(&tail.file_system_id, &tail.job_id)
                .unwrap()
                .unwrap()
        });
        (service, control, head, tail)
    }

    async fn placement_queue_maintenance_tick(
        service: &mut NetworkedDfsService<PlacementRouteCounter>,
        data_dir: &sinomo_core::local::SinomoDataDir,
        host: &mut crate::LocalDfsMaintenanceHost,
        asynchronous: bool,
    ) -> crate::DfsMaintenanceTickReport {
        let scheduler = std::sync::Arc::new(crate::DfsMutationScheduler::new());
        let _admission = scheduler
            .admit(&crate::DfsMessageExecutionScope::GlobalExclusive)
            .await
            .unwrap();
        let mut publisher = crate::LocalDfsMetadataJournalPublisherHost::new(data_dir.clone());
        let local = service.node_id().clone();
        let options = crate::DfsMaintenanceTickOptions {
            namespace_job_max_jobs: 0,
            conflict_max_records: 0,
            write_intent_max_records: 0,
            placement_reconciliation_max_objects: 8,
            repair_max_jobs: 0,
            publish_metadata: false,
            lifecycle: crate::DfsMaintenanceRunOptions {
                repair_metadata: false,
                cleanup_stale_staging: false,
                ..Default::default()
            },
            ..Default::default()
        };
        if asynchronous {
            let mut transport = crate::AsyncDfsMetadataNodeTransport::new(
                Vec::new(),
                std::sync::Arc::new(|_, _, _| Box::pin(async { panic!("publication disabled") })),
                std::sync::Arc::new(|_, _, _| Box::pin(async { panic!("publication disabled") })),
            );
            crate::run_dfs_maintenance_tick_with_async_host(
                service,
                host,
                data_dir,
                sifs_file_system_id(),
                &local,
                Vec::new(),
                &mut publisher,
                &mut transport,
                options,
            )
            .await
            .unwrap()
        } else {
            let mut transport = crate::DfsMetadataNodeTransport::new(
                Vec::new(),
                std::sync::Arc::new(|_, _, _| panic!("publication disabled")),
                std::sync::Arc::new(|_, _, _| panic!("publication disabled")),
            );
            crate::run_dfs_maintenance_tick_with_host(
                service,
                host,
                sifs_file_system_id(),
                &local,
                Vec::new(),
                &mut publisher,
                &mut transport,
                options,
            )
            .unwrap()
        }
    }

    #[tokio::test]
    async fn maintenance_skips_parked_placement_head_for_eligible_tail_with_real_durable_repairs() {
        use crate::DfsMaintenanceHost;
        for asynchronous in [false, true] {
            let root = sinomo_testdir::TestDir::new("placement-parked-head-eligible-tail");
            let (mut service, control, head, tail) = parked_placement_head_fixture(root.path());
            let data_dir = sinomo_core::local::SinomoDataDir::new(root.path());
            let mut host = crate::LocalDfsMaintenanceHost::new(data_dir.clone());
            let repairs = service.durable_repair_queue_records();
            host.store_repair_attempt_records(&repairs).unwrap();
            let durable = crate::repair_queue_store::LocalDfsRepairQueueStore::new(
                crate::default_dfs_repair_attempt_snapshot_path(&data_dir),
            )
            .view()
            .unwrap();
            assert_eq!(durable.records.len(), repairs.len());
            let report =
                placement_queue_maintenance_tick(&mut service, &data_dir, &mut host, asynchronous)
                    .await;
            assert_eq!(
                report.placement_reconciliation.unwrap().job_id,
                tail.job_id,
                "another routine wake cannot restart a parked head's object pass"
            );
            let after = control.with_client(|kv| {
                DfsControlPlaneStore::new(kv)
                    .placement_reconciliation(&head.file_system_id, &head.job_id)
                    .unwrap()
                    .unwrap()
            });
            assert_eq!(
                after, head,
                "parked head owns no progress CAS or object pass"
            );
        }
    }

    #[tokio::test]
    async fn maintenance_empty_durable_repair_queue_resumes_placement_final_validation() {
        for asynchronous in [false, true] {
            let root = sinomo_testdir::TestDir::new("placement-empty-repair-final-validation");
            let (mut service, _, head, _) = parked_placement_head_fixture(root.path());
            let data_dir = sinomo_core::local::SinomoDataDir::new(root.path());
            let mut host = crate::LocalDfsMaintenanceHost::new(data_dir.clone());
            assert!(
                crate::repair_queue_store::LocalDfsRepairQueueStore::new(
                    crate::default_dfs_repair_attempt_snapshot_path(&data_dir)
                )
                .view()
                .unwrap()
                .records
                .is_empty()
            );
            let report =
                placement_queue_maintenance_tick(&mut service, &data_dir, &mut host, asynchronous)
                    .await;
            let advanced = report.placement_reconciliation.unwrap();
            assert_eq!(advanced.job_id, head.job_id);
            assert!(
                advanced.revision > head.revision,
                "the existing empty-queue eligibility edge owns prompt final validation"
            );
        }
    }

    #[tokio::test]
    async fn maintenance_retires_overlapping_superseded_placement_head_before_skipping_repairs() {
        use crate::DfsMaintenanceHost;
        for asynchronous in [false, true] {
            let root = sinomo_testdir::TestDir::new("placement-stale-parked-overlap");
            let (mut service, control, head, _) = parked_placement_head_fixture(root.path());
            control.with_client(|kv| {
                let mut store = DfsControlPlaneStore::new(kv);
                let old = store
                    .get_file_system_config(&head.file_system_id)
                    .unwrap()
                    .unwrap();
                let mut current = old.clone();
                current.subtree_policies.push(
                    normalize_placement_rule(
                        "/team/object.bin",
                        DfsPlacementScope::SelfOnly,
                        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap(),
                    )
                    .unwrap(),
                );
                assert!(
                    store
                        .compare_and_put_file_system_config(Some(&old), &current)
                        .unwrap()
                );
                assert!(!placement_config_remains_authoritative_for_job(
                    head.accepted_config.as_ref().unwrap(),
                    &current,
                    &head
                ));
            });
            let data_dir = sinomo_core::local::SinomoDataDir::new(root.path());
            let mut host = crate::LocalDfsMaintenanceHost::new(data_dir.clone());
            host.store_repair_attempt_records(&service.durable_repair_queue_records())
                .unwrap();
            let report =
                placement_queue_maintenance_tick(&mut service, &data_dir, &mut host, asynchronous)
                    .await;
            let retired = report.placement_reconciliation.unwrap();
            assert_eq!(retired.job_id, head.job_id);
            assert!(retired.state.is_terminal());
            assert!(
                retired
                    .last_error
                    .as_deref()
                    .unwrap()
                    .contains("superseded")
            );
        }
    }

    #[tokio::test]
    async fn maintenance_repairs_missing_or_terminal_queue_head_before_eligible_tail() {
        use crate::DfsMaintenanceHost;
        use sinomo_core::kv::{KvOp, KvTxn};
        for terminal in [false, true] {
            for asynchronous in [false, true] {
                let root = sinomo_testdir::TestDir::new("placement-stale-queue-prefix");
                let (mut service, control, head, tail) = parked_placement_head_fixture(root.path());
                control.with_client(|kv| {
                    let key =
                        crate::placement_reconciliation_key(&head.file_system_id, &head.job_id);
                    let op = if terminal {
                        let mut done = head.clone();
                        done.state = DfsPlacementReconciliationState::Completed;
                        KvOp::put_serde(key, &done, None).unwrap()
                    } else {
                        KvOp::Delete { key }
                    };
                    kv.txn(KvTxn {
                        compares: Vec::new(),
                        success: vec![op],
                        failure: Vec::new(),
                    })
                    .unwrap();
                });
                let data_dir = sinomo_core::local::SinomoDataDir::new(root.path());
                let mut host = crate::LocalDfsMaintenanceHost::new(data_dir.clone());
                host.store_repair_attempt_records(&service.durable_repair_queue_records())
                    .unwrap();
                let report = placement_queue_maintenance_tick(
                    &mut service,
                    &data_dir,
                    &mut host,
                    asynchronous,
                )
                .await;
                assert_eq!(report.placement_reconciliation.unwrap().job_id, tail.job_id);
            }
        }
    }

    #[tokio::test]
    async fn maintenance_legacy_missing_active_queue_preserves_eligibility_selection() {
        use crate::DfsMaintenanceHost;
        use sinomo_core::kv::{KvOp, KvTxn};
        for asynchronous in [false, true] {
            let root = sinomo_testdir::TestDir::new("placement-legacy-active-queue-init");
            let (mut service, control, head, tail) = parked_placement_head_fixture(root.path());
            control.with_client(|kv| {
                kv.txn(KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::Delete {
                        key: crate::control_plane::placement_reconciliation_queue_key(
                            &head.file_system_id,
                        ),
                    }],
                    failure: Vec::new(),
                })
                .unwrap();
            });
            let data_dir = sinomo_core::local::SinomoDataDir::new(root.path());
            let mut host = crate::LocalDfsMaintenanceHost::new(data_dir.clone());
            host.store_repair_attempt_records(&service.durable_repair_queue_records())
                .unwrap();
            let report =
                placement_queue_maintenance_tick(&mut service, &data_dir, &mut host, asynchronous)
                    .await;
            assert_eq!(
                report.placement_reconciliation.unwrap().job_id,
                tail.job_id,
                "legacy queue initialization cannot bypass the same parked-job predicate"
            );
        }
    }

    #[tokio::test]
    async fn maintenance_all_placement_jobs_parked_ignores_missing_tail_without_object_pass() {
        use crate::DfsMaintenanceHost;
        use sinomo_core::kv::{KvOp, KvTxn};
        for asynchronous in [false, true] {
            let root = sinomo_testdir::TestDir::new("placement-all-parked-missing-tail");
            let (mut service, control, head, tail) = parked_placement_head_fixture(root.path());
            control.with_client(|kv| {
                kv.txn(KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::Delete {
                        key: crate::placement_reconciliation_key(
                            &tail.file_system_id,
                            &tail.job_id,
                        ),
                    }],
                    failure: Vec::new(),
                })
                .unwrap();
            });
            let data_dir = sinomo_core::local::SinomoDataDir::new(root.path());
            let mut host = crate::LocalDfsMaintenanceHost::new(data_dir.clone());
            host.store_repair_attempt_records(&service.durable_repair_queue_records())
                .unwrap();
            let permits_before = placement_permit_event_count(&control);
            let report =
                placement_queue_maintenance_tick(&mut service, &data_dir, &mut host, asynchronous)
                    .await;
            assert!(
                report.placement_reconciliation.is_none(),
                "a stale tail cannot manufacture another object pass for a parked head"
            );
            let after = control.with_client(|kv| {
                DfsControlPlaneStore::new(kv)
                    .placement_reconciliation(&head.file_system_id, &head.job_id)
                    .unwrap()
                    .unwrap()
            });
            assert_eq!(after, head);
            assert_eq!(
                placement_permit_event_count(&control),
                permits_before,
                "no placement object-side mutation permit is acquired"
            );
        }
    }

    #[tokio::test]
    async fn production_classifier_wakes_parked_superseded_placement_for_terminalization() {
        use crate::DfsMaintenanceHost;
        for asynchronous in [false, true] {
            let root = sinomo_testdir::TestDir::new("placement-projected-supersession-wake");
            let (mut service, control, head, tail) = parked_placement_head_fixture(root.path());
            control.with_client(|kv| {
                let mut store = DfsControlPlaneStore::new(kv);
                let mut done = tail.clone();
                done.state = DfsPlacementReconciliationState::Completed;
                done.revision += 1;
                assert!(
                    store
                        .compare_and_put_placement_reconciliation(&tail, &done)
                        .unwrap()
                );
                let old = store
                    .get_file_system_config(&head.file_system_id)
                    .unwrap()
                    .unwrap();
                let mut current = old.clone();
                current.subtree_policies.push(
                    normalize_placement_rule(
                        "/team/object.bin",
                        DfsPlacementScope::SelfOnly,
                        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap(),
                    )
                    .unwrap(),
                );
                assert!(
                    store
                        .compare_and_put_file_system_config(Some(&old), &current)
                        .unwrap()
                );
            });
            let data_dir = sinomo_core::local::SinomoDataDir::new(root.path());
            let mut host = crate::LocalDfsMaintenanceHost::new(data_dir.clone());
            let repairs = service.durable_repair_queue_records();
            assert!(!repairs.is_empty());
            host.store_repair_attempt_records(&repairs).unwrap();
            let entries = control.with_client(|kv| kv.snapshot().unwrap().entries);
            assert_eq!(
                crate::control_plane::placement_reconciliation_pending_work_from_authority_entries(
                    &head.file_system_id,
                    &entries,
                    repairs.len()
                )
                .unwrap(),
                1,
                "cached configuration change owns the exact stale-job terminalization wake"
            );
            let report =
                placement_queue_maintenance_tick(&mut service, &data_dir, &mut host, asynchronous)
                    .await;
            let retired = report.placement_reconciliation.unwrap();
            assert_eq!(retired.job_id, head.job_id);
            assert!(retired.state.is_terminal());
            assert!(
                retired
                    .last_error
                    .as_deref()
                    .unwrap()
                    .contains("superseded")
            );
        }
    }

    #[test]
    fn placement_repair_pages_preserve_pending_repairs_across_continuations() {
        for asynchronous in [false, true] {
            let root = sinomo_testdir::TestDir::new("dfs-placement-repair-page-count");
            let (mut service, control, first, _) =
                placement_page_noop_fixture(root.path(), false, "missing");
            let mut second = first.clone();
            second.object_id = DfsObjectId::from_legacy_named("repair-page-directory").unwrap();
            second.path = "/team/second".into();
            second.kind = DfsObjectKind::Directory;
            second.content_hash = None;
            second.content_size_bytes = None;
            service.add_object_metadata(second).unwrap();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let mut advance = || {
                if asynchronous {
                    runtime.block_on(service.run_placement_reconciliation_bounded_async(1))
                } else {
                    service.run_placement_reconciliation_bounded(1)
                }
                .unwrap()
                .unwrap()
            };
            assert_eq!(advance().state, DfsPlacementReconciliationState::Scanning);
            assert_eq!(advance().state, DfsPlacementReconciliationState::Repairing);
            let partial = advance();
            assert!(partial.cursor.is_some());
            assert_eq!(partial.pending_repair_count, 1);
            let persisted = control.with_client(|kv| {
                DfsControlPlaneStore::new(kv)
                    .placement_reconciliation(&sifs_file_system_id(), &partial.job_id)
                    .unwrap()
                    .unwrap()
            });
            assert_eq!(persisted, partial);
            let end = advance();
            assert!(end.cursor.is_none());
            assert_eq!(end.state, DfsPlacementReconciliationState::Repairing);
            assert_eq!(end.pending_repair_count, 1);
            assert_eq!(end.completed_repair_count, 0);
            let next_pass = advance();
            assert_eq!(
                next_pass.pending_repair_count, 1,
                "restart the count each pass"
            );
        }
    }

    #[test]
    fn async_placement_cancellation_between_local_noops_keeps_page_for_replay() {
        use std::future::Future;
        let root = sinomo_testdir::TestDir::new("dfs-async-placement-page-cancellation");
        let (mut service, control, first, calls) =
            placement_page_noop_fixture(root.path(), false, "none");
        let mut second = first.clone();
        second.object_id = DfsObjectId::from_legacy_named("async-placement-second-object").unwrap();
        second.path = "/team/second.bin".into();
        service
            .content()
            .put_object_version(
                second.object_id.as_str(),
                second.version.as_str(),
                &vec![0x37; 4096],
            )
            .unwrap();
        service.add_object_metadata(second.clone()).unwrap();
        let durable_job = || {
            control.with_client(|kv| {
                DfsControlPlaneStore::new(kv)
                    .placement_reconciliation(&sifs_file_system_id(), "placement-test-job")
                    .unwrap()
                    .unwrap()
            })
        };
        let before = durable_job();
        let permits_before = placement_permit_event_count(&control);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // Demonstrate the progress limit explicitly: repeatedly cancelling at
        // the first cooperative point leaves the same durable page. The intent
        // maintenance cursor does not advance placement's separate job cursor.
        for _ in 0..3 {
            runtime.block_on(async {
                let mut page = Box::pin(service.run_placement_reconciliation_bounded_async(8));
                let yielded = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(page.as_mut().poll(cx).is_pending())
                })
                .await;
                assert!(
                    yielded,
                    "two certified local no-ops must yield between objects"
                );
                drop(page);
            });
            assert_eq!(
                durable_job(),
                before,
                "cancellation cannot claim uncommitted page progress"
            );
            assert_eq!(
                service
                    .current_read_object_metadata(&first.object_id)
                    .unwrap(),
                first
            );
            assert_eq!(
                service
                    .current_read_object_metadata(&second.object_id)
                    .unwrap(),
                second
            );
        }
        let scanning = runtime
            .block_on(service.run_placement_reconciliation_bounded_async(8))
            .unwrap()
            .unwrap();
        assert_eq!(scanning.state, DfsPlacementReconciliationState::Repairing);
        assert_eq!(
            scanning.scanned_object_count, 2,
            "cancelled tentative counters must not be double-counted"
        );
        let completed = runtime
            .block_on(service.run_placement_reconciliation_bounded_async(8))
            .unwrap()
            .unwrap();
        assert_eq!(completed.state, DfsPlacementReconciliationState::Completed);
        assert_eq!(completed.pending_repair_count, 0);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(placement_permit_event_count(&control), permits_before);
    }

    #[test]
    fn async_placement_remote_noop_still_requires_authoritative_route() {
        let root = sinomo_testdir::TestDir::new("dfs-async-placement-remote-noop");
        let (mut service, _control, _object, calls) =
            placement_page_noop_fixture(root.path(), true, "none");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let job = runtime
            .block_on(service.run_placement_reconciliation_bounded_async(8))
            .unwrap()
            .unwrap();
        assert!(
            calls.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "remote no-op cannot skip authority/response freshness"
        );
        assert!(
            job.last_error.is_some(),
            "fixture refuses the required route"
        );
        assert_ne!(job.state, DfsPlacementReconciliationState::Completed);
        assert!(
            job.cursor.is_none(),
            "failed first-page operation cannot advance past that object"
        );
    }

    #[test]
    fn routed_placement_noop_keeps_fresh_authority_without_namespace_journal_churn() {
        let root = sinomo_testdir::TestDir::new("placement-noop-no-journal");
        let (mut service, control, object, _) =
            placement_page_noop_fixture(root.path(), false, "none");
        let job = control.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .placement_reconciliation(&sifs_file_system_id(), "placement-test-job")
                .unwrap()
                .unwrap()
        });
        let (resolved, replication, plan) = reconciliation_placement_for_object(
            job.accepted_config.as_ref().unwrap(),
            &job,
            &object,
        )
        .unwrap();
        let operation = DfsNamespaceOperation::ReconcilePlacement {
            job_id: job.job_id.clone(),
            path: object.path.clone(),
            object_id: object.object_id.clone(),
            expected_version: object.version.clone(),
            object_policy: resolved.policy.consistency,
            replication_policy: replication,
            desired_replica_node_ids: plan.plan.desired_replica_nodes,
        };
        let requester = node("placement-observer");
        let shards = control.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .list_metadata_shards(&sifs_file_system_id())
                .unwrap()
        });
        let head = service
            .metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap();
        let permits = placement_permit_event_count(&control);
        let mut command = crate::DfsNamespaceCommand {
            request_id: "placement-check-1".into(),
            file_system_id: sifs_file_system_id(),
            requesting_node_id: requester.clone(),
            include_metadata_journal_batches: true,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: shards,
            operation,
        };
        for request in ["placement-check-1", "placement-check-2"] {
            command.request_id = request.into();
            service
                .handle_namespace_request(requester.clone(), command.clone())
                .unwrap();
            let response = service.transport.responses.lock().unwrap().pop().unwrap();
            assert_eq!(response.result, crate::DfsNamespaceResult::Applied);
            assert!(
                service
                    .metadata_backend()
                    .unwrap()
                    .get_namespace_request(&requester, request)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                service
                    .metadata_backend()
                    .unwrap()
                    .journal_last_sequence()
                    .unwrap(),
                head
            );
        }
        assert!(
            placement_permit_event_count(&control) > permits,
            "no-op reconciliation still uses the real placement consistency permit"
        );
        let (_, _, mut held_permit) = service
            .acquire_authoritative_placement_reconciliation_for_path(
                &job.file_system_id,
                &job.job_id,
                &object.path,
            )
            .unwrap()
            .unwrap();
        // Even the same invocation id must revalidate its domain-owned permit,
        // rather than replaying an earlier success over a live consistency fence.
        service
            .handle_namespace_request(requester, command)
            .unwrap();
        let response = service.transport.responses.lock().unwrap().pop().unwrap();
        assert!(matches!(
            response.result,
            crate::DfsNamespaceResult::Rejected { .. }
        ));
        service
            .release_placement_authority_permit(&mut held_permit)
            .unwrap();
    }
    #[tokio::test]
    async fn placement_repeat_execution_keeps_one_durable_semantic_repair_intent() {
        let root = sinomo_testdir::TestDir::new("dfs-placement-repeated-semantic-effect");
        let (mut service, control, object, _calls) =
            placement_page_noop_fixture(root.path(), false, "missing");
        let job = control.with_client(|kv| {
            DfsControlPlaneStore::new(kv)
                .placement_reconciliation(&sifs_file_system_id(), "placement-test-job")
                .unwrap()
                .unwrap()
        });
        let (resolved, replication, plan) = reconciliation_placement_for_object(
            job.accepted_config.as_ref().unwrap(),
            &job,
            &object,
        )
        .unwrap();
        let intent_id = format!(
            "placement:{}:{}:{}",
            job.job_id, object.object_id, object.version
        );
        // A later pass after an unknown reply has a new namespace invocation,
        // but repeats this same checked receiver-side desired-state operation.
        service
            .reconcile_object_placement_local(
                &job.job_id,
                &object.path,
                &object.object_id,
                &object.version,
                resolved.policy.consistency.clone(),
                replication.clone(),
                &plan.plan.desired_replica_nodes,
            )
            .unwrap();
        let original = service
            .metadata_backend()
            .unwrap()
            .get_intent(&object.object_id, &intent_id)
            .unwrap()
            .unwrap();
        let coordinator_before = service.coordinator.intent(&intent_id).unwrap().clone();
        let journal_before_replay = service
            .metadata_backend()
            .unwrap()
            .journal_last_sequence()
            .unwrap();
        let receipts_before_replay = service
            .metadata_backend()
            .unwrap()
            .receipts_for_version(&object.object_id, &object.version)
            .unwrap();
        service
            .reconcile_object_placement_local(
                &job.job_id,
                &object.path,
                &object.object_id,
                &object.version,
                resolved.policy.consistency,
                replication,
                &plan.plan.desired_replica_nodes,
            )
            .unwrap();
        let after = service
            .metadata_backend()
            .unwrap()
            .get_intent(&object.object_id, &intent_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            service
                .metadata_backend()
                .unwrap()
                .intents_for_object(&object.object_id)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(after.intent_id, original.intent_id);
        assert_eq!(after.policy, original.policy);
        assert_eq!(
            after.desired_replica_node_ids,
            original.desired_replica_node_ids
        );
        assert_eq!(after.created_at, original.created_at);
        assert_eq!(
            after, original,
            "an identical repair poll keeps the durable intent unchanged"
        );
        assert_eq!(
            service
                .metadata_backend()
                .unwrap()
                .journal_last_sequence()
                .unwrap(),
            journal_before_replay,
            "an identical repair poll must not author a journal entry"
        );
        assert_eq!(
            service
                .metadata_backend()
                .unwrap()
                .receipts_for_version(&object.object_id, &object.version)
                .unwrap(),
            receipts_before_replay,
            "existing receipt provenance must not be refreshed by a poll"
        );
        assert_eq!(
            service.coordinator.intent(&intent_id),
            Some(&coordinator_before)
        );
        let permits_before_restore = placement_permit_event_count(&control);
        service.restore_pending_writes_for_repair_jobs().unwrap();
        assert_eq!(
            placement_permit_event_count(&control),
            permits_before_restore,
            "local restoration must not acquire a placement permit for the backlog"
        );
        assert!(
            service
                .placement_intent_remains_authoritative(&intent_id)
                .unwrap()
        );
        assert!(
            placement_permit_event_count(&control) > permits_before_restore,
            "the execution boundary must retain its real consistency check"
        );
        let mut parked = service.durable_repair_queue_records();
        assert!(!parked.is_empty());
        for record in &mut parked {
            record.next_retry_unix_nanos = u64::MAX - 1;
        }
        let permits_before_parked = placement_permit_event_count(&control);
        assert!(
            service
                .retry_pending_repairs_bounded_with_attempts(8, &parked)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            placement_permit_event_count(&control),
            permits_before_parked
        );
        assert!(
            service
                .retry_pending_repairs_async_bounded_with_attempts(8, &parked)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            placement_permit_event_count(&control),
            permits_before_parked,
            "neither repair executor may acquire permits for backed-off targets"
        );
        assert_eq!(
            service
                .current_read_object_metadata(&object.object_id)
                .unwrap(),
            object
        );
    }
    #[derive(Clone, Debug)]
    struct RepairAuthorityProbe {
        control: SharedDfsControlPlaneKvClient<InProcessKvStore>,
        transactions: std::sync::Arc<std::sync::Mutex<Vec<sinomo_core::kv::KvTxn>>>,
        dispatched: std::sync::Arc<std::sync::Mutex<Vec<crate::DfsRepairReplicaCommand>>>,
        fetches: std::sync::Arc<std::sync::Mutex<Vec<crate::DfsFetchBlobCommand>>>,
        fail_release: std::sync::Arc<std::sync::atomic::AtomicBool>,
        fail_dispatch_intent: std::sync::Arc<std::sync::Mutex<Option<String>>>,
        retained_checkpoint_gate:
            std::sync::Arc<std::sync::Mutex<Option<RepairRetainedCheckpointGate>>>,
    }

    impl RepairAuthorityProbe {
        fn permit_transactions(&self) -> (usize, usize) {
            use sinomo_core::kv::KvOp;
            self.transactions
                .lock()
                .unwrap()
                .iter()
                .flat_map(|txn| &txn.success)
                .fold((0, 0), |(acquires, releases), op| match op {
                    KvOp::Put { key, .. } if key.contains("/placement-authority-permit-v1") => {
                        (acquires + 1, releases)
                    }
                    KvOp::Delete { key } if key.contains("/placement-authority-permit-v1") => {
                        (acquires, releases + 1)
                    }
                    _ => (acquires, releases),
                })
        }

        fn permit_is_present(&self) -> bool {
            self.control.with_client(|kv| {
                DfsControlPlaneStore::new(kv)
                    .placement_authority_permit_status(&sifs_file_system_id())
                    .unwrap()
                    .is_some()
            })
        }
    }

    impl DfsControlPlaneKvClient for RepairAuthorityProbe {
        fn placement_authority_process_identity(
            &self,
        ) -> Option<crate::DfsPlacementAuthorityProcessIdentity> {
            self.control.placement_authority_process_identity()
        }
        fn refresh_latest(&mut self) -> Result<(), sinomo_core::kv::KvError> {
            self.control.refresh_latest()
        }
        fn get(
            &self,
            key: &str,
        ) -> Result<Option<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
            self.control.get(key)
        }
        fn range_prefix(
            &self,
            prefix: &str,
        ) -> Result<Vec<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
            self.control.range_prefix(prefix)
        }
        fn put(
            &mut self,
            key: String,
            value: Vec<u8>,
        ) -> Result<sinomo_core::kv::KvRevision, sinomo_core::kv::KvError> {
            self.control.put(key, value)
        }
        fn txn(
            &mut self,
            txn: sinomo_core::kv::KvTxn,
        ) -> Result<sinomo_core::kv::KvTxnResponse, sinomo_core::kv::KvError> {
            use sinomo_core::kv::KvOp;
            self.transactions.lock().unwrap().push(txn.clone());
            if self.fail_release.load(std::sync::atomic::Ordering::SeqCst)
                && txn.success.iter().any(|op| matches!(op, KvOp::Delete { key } if key.contains("/placement-authority-permit-v1"))) {
                return Err(sinomo_core::kv::KvError::Serialization { detail: "injected exact permit release failure".into() });
            }
            self.control.txn(txn)
        }
    }

    fn repair_authority_fixture(
        root: &std::path::Path,
        groups: &[&str],
        missing_content_index: Option<usize>,
        fanout: bool,
    ) -> (
        NetworkedDfsService<PlacementRouteCounter>,
        RepairAuthorityProbe,
        Vec<DistributedWriteIntent>,
    ) {
        repair_authority_fixture_with_snapshot(root, groups, missing_content_index, fanout, false)
    }

    fn repair_authority_fixture_with_snapshot(
        root: &std::path::Path,
        groups: &[&str],
        missing_content_index: Option<usize>,
        fanout: bool,
        with_placement_snapshot: bool,
    ) -> (
        NetworkedDfsService<PlacementRouteCounter>,
        RepairAuthorityProbe,
        Vec<DistributedWriteIntent>,
    ) {
        let (mut service, control, first, _) = placement_page_noop_fixture(root, false, "missing");
        let mut jobs = BTreeMap::new();
        control.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv);
            let mut config = store
                .get_file_system_config(&sifs_file_system_id())
                .unwrap()
                .unwrap();
            if fanout {
                let old = config.clone();
                config.subtree_policies[0] = normalize_placement_rule(
                    "/team",
                    DfsPlacementScope::Subtree,
                    DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 3).unwrap(),
                )
                .unwrap();
                assert!(
                    store
                        .compare_and_put_file_system_config(Some(&old), &config)
                        .unwrap()
                );
            }
            for &group in groups {
                if group == "ordinary" || jobs.contains_key(group) {
                    continue;
                }
                let mut job =
                    reconciliation_job("/team", DfsPlacementScope::Subtree, BTreeSet::new());
                job.job_id = group.into();
                job.accepted_config = Some(config.clone());
                job.candidate_snapshot = vec![
                    advertisement("node-a", "east"),
                    advertisement("node-b", "west"),
                ];
                if fanout || with_placement_snapshot {
                    job.candidate_snapshot
                        .push(advertisement("node-c", "north"));
                }
                if with_placement_snapshot {
                    let now = time::OffsetDateTime::now_utc();
                    let mut snapshot =
                        crate::DfsPlacementSnapshot::empty(config.owner_node_id.clone());
                    snapshot.expires_at = now + time::Duration::hours(1);
                    snapshot.nodes = job
                        .candidate_snapshot
                        .iter()
                        .map(|candidate| {
                            let mut observed =
                                crate::DfsPlacementNodeObservation::from_advertisement(candidate);
                            observed.storage_health = crate::DfsHealthState::Healthy;
                            observed.storage_freshness = crate::DfsObservationFreshness::Fresh;
                            observed.expires_at = snapshot.expires_at;
                            observed
                        })
                        .collect();
                    snapshot.paths = job
                        .candidate_snapshot
                        .iter()
                        .map(|candidate| crate::DfsPlacementPathObservation {
                            source_node_id: config.owner_node_id.clone(),
                            target_node_id: candidate.node_id.clone(),
                            state: crate::DfsPlacementPathState::Healthy,
                            latency_micros: Some(if candidate.node_id == node("node-c") {
                                1_000
                            } else {
                                100
                            }),
                            delivered_bytes_per_second: Some(1_000_000),
                            metering: crate::DfsNetworkMetering::Unmetered,
                            observed_at: now,
                            expires_at: snapshot.expires_at,
                        })
                        .collect();
                    job.placement_snapshot = Some(snapshot);
                }
                assert!(
                    store
                        .compare_and_put_placement_update(&config, &config, &job)
                        .unwrap()
                );
                jobs.insert(group.to_owned(), (job, config.clone()));
            }
        });
        // The retained configuration used by ordinary repair matches the real accepted policy.
        service.refresh_control_plane_latest().unwrap();
        let mut intents = Vec::new();
        for (index, &group) in groups.iter().enumerate() {
            let mut object = first.clone();
            object.object_id =
                DfsObjectId::from_legacy_named(format!("repair-authority-object-{index}")).unwrap();
            object.path = format!("/team/repair-authority-{index:03}.bin");
            let bytes = vec![index as u8; 4096];
            object.content_hash = Some(crate::content_hash(&bytes));
            if missing_content_index == Some(index) {
                object.current_locations.push(node("node-c"));
            } else {
                service
                    .content()
                    .put_object_version(object.object_id.as_str(), object.version.as_str(), &bytes)
                    .unwrap();
            }
            service.add_object_metadata(object.clone()).unwrap();
            let id = if group == "ordinary" {
                format!("ordinary-repair-{index:03}")
            } else {
                format!("placement:{group}:repair-{index:03}")
            };
            let mut intent = placement_test_committed_intent(&object, id);
            if let Some((job, config)) = jobs.get(group) {
                let (_, policy, placement) =
                    reconciliation_placement_for_object_with_pins(config, job, &object, &[])
                        .unwrap();
                intent.policy = policy;
                intent.desired_replica_nodes = placement.plan.desired_replica_nodes;
            }
            service
                .put_metadata_intent_and_receipts(
                    &intent,
                    object.content_hash.as_deref().unwrap(),
                    &object.current_locations,
                )
                .unwrap();
            intents.push(intent);
        }
        service.hydrate_from_metadata_backend().unwrap();
        assert_eq!(
            service.coordinator.repair_jobs().len(),
            groups.len() * if fanout { 2 } else { 1 }
        );
        let probe = RepairAuthorityProbe {
            control,
            transactions: Default::default(),
            dispatched: Default::default(),
            fetches: Default::default(),
            fail_release: Default::default(),
            fail_dispatch_intent: Default::default(),
            retained_checkpoint_gate: Default::default(),
        };
        service.transport.repair_authority_probe = Some(probe.clone());
        service.control_plane = Some(Box::new(probe.clone()));
        (service, probe, intents)
    }

    #[tokio::test]
    async fn repair_authority_batch_same_job_uses_one_real_permit_before_dispatch() {
        let root = sinomo_testdir::TestDir::new("repair-authority-batch-one-permit");
        let (mut service, probe, intents) =
            repair_authority_fixture(root.path(), &["repair-job"; 4], None, false);
        let scheduled = service
            .retry_pending_repairs_async_bounded_with_attempts(4, &[])
            .await
            .unwrap();
        assert_eq!(scheduled.len(), 4);
        assert_eq!(probe.dispatched.lock().unwrap().len(), 4);
        assert_eq!(
            probe.permit_transactions(),
            (1, 1),
            "four repairs share one actual acquired and released permit"
        );
        assert!(!probe.permit_is_present());
        assert!(intents.iter().all(|intent| {
            service.coordinator.intent(&intent.intent_id).unwrap().state
                == DfsWriteIntentState::Committed
        }));
    }

    #[tokio::test]
    async fn repair_authority_batch_distinct_jobs_keep_separate_real_permits() {
        let root = sinomo_testdir::TestDir::new("repair-authority-batch-distinct-jobs");
        let (mut service, probe, _) = repair_authority_fixture(
            root.path(),
            &["repair-a", "repair-a", "repair-b", "repair-b"],
            None,
            false,
        );
        assert_eq!(
            service
                .retry_pending_repairs_async_bounded_with_attempts(4, &[])
                .await
                .unwrap()
                .len(),
            4
        );
        assert_eq!(probe.permit_transactions(), (2, 2));
        assert_eq!(probe.dispatched.lock().unwrap().len(), 4);
        assert!(!probe.permit_is_present());
    }

    #[tokio::test]
    async fn repair_authority_batch_ordinary_repairs_do_no_permit_kv() {
        let root = sinomo_testdir::TestDir::new("repair-authority-ordinary-zero-kv");
        let (mut service, probe, _) =
            repair_authority_fixture(root.path(), &["ordinary"; 4], None, false);
        assert_eq!(
            service
                .retry_pending_repairs_async_bounded_with_attempts(4, &[])
                .await
                .unwrap()
                .len(),
            4
        );
        assert_eq!(probe.permit_transactions(), (0, 0));
        assert!(
            probe.transactions.lock().unwrap().is_empty(),
            "ordinary retry must not perform any KV transaction"
        );
    }

    #[tokio::test]
    async fn repair_authority_batch_preserves_expired_inflight_ids_and_target_budget() {
        let root = sinomo_testdir::TestDir::new("repair-authority-stable-identities");
        let (mut service, probe, _) =
            repair_authority_fixture(root.path(), &["repair-job"; 3], None, true);
        service.restore_pending_writes_for_repair_jobs().unwrap();
        let mut attempts = service.durable_repair_queue_records();
        assert_eq!(attempts.len(), 6);
        for (index, record) in attempts.iter_mut().enumerate() {
            record.attempts = 1;
            record.in_flight = true;
            record.next_retry_unix_nanos = 0;
            record.transfer_request_id = Some(format!("existing-transfer-{index}"));
        }
        let request_sequence_before = service.next_request_id;
        let scheduled = service
            .retry_pending_repairs_async_bounded_with_attempts(5, &attempts)
            .await
            .unwrap();
        assert_eq!(
            scheduled.len(),
            5,
            "companion fanout consumes the same target budget"
        );
        assert_eq!(
            service.next_request_id, request_sequence_before,
            "restored transfers allocate no replacement identities"
        );
        assert_eq!(probe.permit_transactions(), (1, 1));
        let sent = probe.dispatched.lock().unwrap();
        assert_eq!(sent.len(), 5);
        for command in sent.iter() {
            let scheduled_target = scheduled
                .iter()
                .find(|job| {
                    job.intent_id == command.intent_id
                        && attempts.iter().any(|record| {
                            record.intent_id == job.intent_id
                                && record.missing_replica_node_id == job.missing_replica_node_id
                                && record.transfer_request_id.as_deref()
                                    == Some(command.request_id.as_str())
                        })
                })
                .unwrap();
            assert!(
                attempts
                    .iter()
                    .any(|record| record.intent_id == scheduled_target.intent_id
                        && record.missing_replica_node_id
                            == scheduled_target.missing_replica_node_id
                        && record.transfer_request_id.as_deref()
                            == Some(command.request_id.as_str()))
            );
        }
        assert_eq!(
            service
                .durable_repair_queue_records()
                .iter()
                .filter(|record| record.in_flight)
                .count(),
            5
        );
    }

    #[tokio::test]
    async fn repair_authority_batch_missing_content_keeps_scalar_fetch_and_budget() {
        let root = sinomo_testdir::TestDir::new("repair-authority-ready-prefix-fetch");
        let (mut service, probe, intents) =
            repair_authority_fixture(root.path(), &["repair-job"; 4], Some(2), false);
        let request_sequence_before = service.next_request_id;
        let scheduled = service
            .retry_pending_repairs_async_bounded_with_attempts(3, &[])
            .await
            .unwrap();
        assert_eq!(
            service.next_request_id,
            request_sequence_before + 3,
            "only two repairs and one alternate fetch allocate IDs"
        );
        assert_eq!(scheduled.len(), 2);
        assert_eq!(probe.dispatched.lock().unwrap().len(), 2);
        assert_eq!(probe.fetches.lock().unwrap().len(), 1);
        assert_eq!(
            probe.fetches.lock().unwrap()[0].intent_id,
            intents[2].intent_id
        );
        assert_eq!(
            probe.permit_transactions(),
            (2, 2),
            "one prefix permit plus the scalar fetch permit"
        );
        assert!(
            service
                .writes
                .pending_writes
                .get(&intents[2].intent_id)
                .unwrap()
                .repair_inflight_request_ids
                .is_empty()
        );
        assert!(
            probe
                .dispatched
                .lock()
                .unwrap()
                .iter()
                .all(|command| command.intent_id != intents[3].intent_id)
        );
        assert!(
            service
                .retry_pending_repairs_async_bounded_with_attempts(1, &[])
                .await
                .unwrap()
                .iter()
                .any(|job| job.intent_id == intents[3].intent_id)
        );
    }

    #[tokio::test]
    async fn repair_authority_batch_rechecks_config_pins_and_current_content() {
        for mismatch in ["config", "pins", "version", "path", "desired"] {
            let root = sinomo_testdir::TestDir::new(&format!("repair-authority-reject-{mismatch}"));
            let (mut service, probe, intents) = if mismatch == "pins" {
                repair_authority_fixture_with_snapshot(
                    root.path(),
                    &["repair-job"; 3],
                    None,
                    false,
                    true,
                )
            } else {
                repair_authority_fixture(root.path(), &["repair-job"; 3], None, false)
            };
            let rejected = &intents[2];
            match mismatch {
                "config" => probe.control.with_client(|kv| {
                    let mut store = DfsControlPlaneStore::new(kv);
                    let previous = store
                        .get_file_system_config(&sifs_file_system_id())
                        .unwrap()
                        .unwrap();
                    let mut config = previous.clone();
                    config.subtree_policies[0] = normalize_placement_rule(
                        "/team",
                        DfsPlacementScope::Subtree,
                        DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap(),
                    )
                    .unwrap();
                    assert!(
                        store
                            .compare_and_put_file_system_config(Some(&previous), &config)
                            .unwrap()
                    );
                }),
                "pins" => {
                    let job = probe.control.with_client(|kv| {
                        DfsControlPlaneStore::new(kv)
                            .placement_reconciliation(&sifs_file_system_id(), "repair-job")
                            .unwrap()
                            .unwrap()
                    });
                    let object = service
                        .current_read_object_metadata(&rejected.object_id)
                        .unwrap();
                    let config = job.accepted_config.as_ref().unwrap();
                    assert!(
                        job.placement_snapshot.is_some(),
                        "pin validation requires the accepted snapshot path"
                    );
                    assert_eq!(job.candidate_snapshot.len(), 3);
                    let (_, _, before) =
                        reconciliation_placement_for_object_with_pins(config, &job, &object, &[])
                            .unwrap();
                    assert_eq!(before.plan.desired_replica_nodes.len(), 2);
                    assert_eq!(
                        rejected.desired_replica_nodes, before.plan.desired_replica_nodes,
                        "fixture intent must match the accepted unpinned N2 plan"
                    );
                    let pinned = job
                        .candidate_snapshot
                        .iter()
                        .find(|candidate| {
                            !before
                                .plan
                                .desired_replica_nodes
                                .contains(&candidate.node_id)
                        })
                        .expect("three advertised candidates leave one unselected at N2")
                        .node_id
                        .clone();
                    let data_dir = sinomo_core::local::SinomoDataDir::new(root.path());
                    let pin_root = crate::default_dfs_pin_root(&data_dir);
                    crate::LocalDfsPinStore::new(&pin_root)
                        .add_pin(crate::DfsPinAddRequest {
                            file_system_id: object.file_system_id.clone(),
                            path: object.path.clone(),
                            object_id: Some(object.object_id.clone()),
                            pinned_by_node_id: pinned.clone(),
                            mode: crate::DfsPinMode::Content,
                            created_at: time::OffsetDateTime::now_utc(),
                        })
                        .unwrap();
                    // Reload the durable owner and publish through the same
                    // typed pin snapshot used by browser service construction.
                    service.set_content_pin_snapshot(
                        crate::list_local_dfs_pins(&pin_root, None)
                            .unwrap()
                            .into_iter()
                            .filter(|pin| pin.mode == crate::DfsPinMode::Content)
                            .filter_map(|pin| {
                                pin.object_id
                                    .map(|object_id| (object_id, pin.pinned_by_node_id))
                            }),
                    );
                    let pins = service.content_pin_nodes(&rejected.object_id);
                    assert_eq!(pins, vec![pinned.clone()]);
                    let (_, _, after) =
                        reconciliation_placement_for_object_with_pins(config, &job, &object, &pins)
                            .unwrap();
                    assert!(after.plan.desired_replica_nodes.contains(&pinned));
                    assert_ne!(
                        after.plan.desired_replica_nodes, before.plan.desired_replica_nodes,
                        "the durable advertised pin must change the actual accepted desired plan before retry"
                    );
                }
                "version" | "path" => {
                    // Model an intervening namespace author under the same
                    // original object/path read-set fence as production.
                    let _planning = service.namespace_execution.begin_planning();
                    let mut object = service
                        .current_read_object_metadata(&rejected.object_id)
                        .unwrap();
                    if mismatch == "version" {
                        object.version =
                            DfsVersion::from_legacy_named("later-repair-content").unwrap();
                        object.content_hash = Some(crate::content_hash(b"later content"));
                    } else {
                        object.path = "/outside/repair-authority.bin".into();
                        assert!(
                            service
                                .authoritative_metadata_object_for_path(
                                    &object.file_system_id,
                                    &object.path,
                                )
                                .unwrap()
                                .is_none()
                        );
                    }
                    service.add_object_metadata(object).unwrap();
                }
                "desired" => {
                    let mut intent = rejected.clone();
                    intent.desired_replica_nodes.reverse();
                    let object = service
                        .current_read_object_metadata(&intent.object_id)
                        .unwrap();
                    service
                        .put_metadata_intent(&intent, object.content_hash.as_deref())
                        .unwrap();
                    service.coordinator.restore_intent(intent).unwrap();
                }
                _ => unreachable!(),
            }
            let _result = service
                .retry_pending_repairs_async_bounded_with_attempts(3, &[])
                .await;
            let sent = probe.dispatched.lock().unwrap();
            assert!(
                sent.iter()
                    .all(|command| command.intent_id != rejected.intent_id),
                "{mismatch} cannot authorize the rejected payload"
            );
            if mismatch == "config" {
                assert!(sent.is_empty());
            } else {
                assert_eq!(
                    sent.len(),
                    2,
                    "ready work preceding {mismatch} must keep its normal order"
                );
            }
            assert!(
                !probe.permit_is_present(),
                "{mismatch} must release any acquired permit"
            );
        }
    }

    #[tokio::test]
    async fn repair_authority_batch_release_error_retains_exact_custody_for_fresh_service() {
        let root = sinomo_testdir::TestDir::new("repair-authority-release-custody");
        let (mut service, probe, _) =
            repair_authority_fixture(root.path(), &["repair-job"; 3], None, false);
        probe
            .fail_release
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(
            service
                .retry_pending_repairs_async_bounded_with_attempts(3, &[])
                .await
                .is_err()
        );
        assert!(probe.permit_is_present());
        assert!(
            probe.dispatched.lock().unwrap().is_empty(),
            "no dispatch before successful release"
        );
        let transport = service.transport.clone();
        drop(service);
        probe
            .fail_release
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let mut recovered = NetworkedDfsService::new(
            transport,
            LocalContentStore::new(root.join("content")).unwrap(),
        )
        .with_control_plane(probe.clone())
        .with_metadata_backend(open_root_local_metadata_backend(root.join("metadata")).unwrap())
        .unwrap();
        assert_eq!(
            recovered
                .retry_pending_repairs_async_bounded_with_attempts(3, &[])
                .await
                .unwrap()
                .len(),
            3
        );
        assert!(!probe.permit_is_present());
        assert_eq!(probe.dispatched.lock().unwrap().len(), 3);
    }
    #[tokio::test]
    async fn repair_authority_batch_exclusions_keep_unattempted_targets_parked() {
        let root = sinomo_testdir::TestDir::new("repair-authority-excluded-targets");
        let (mut service, probe, _) =
            repair_authority_fixture(root.path(), &["repair-job"; 3], None, true);
        let excluded = crate::DfsMaintenanceExecutionExclusions::new([node("node-b")]);
        let scheduled = service
            .retry_pending_repairs_async_bounded_with_attempts_and_execution_exclusions(
                3,
                &[],
                &excluded,
            )
            .await
            .unwrap();
        assert_eq!(scheduled.len(), 3);
        assert!(
            scheduled
                .iter()
                .all(|repair| repair.missing_replica_node_id == node("node-c"))
        );
        assert_eq!(probe.permit_transactions(), (1, 1));
        assert!(
            service
                .durable_repair_queue_records()
                .iter()
                .filter(|record| record.missing_replica_node_id == node("node-b"))
                .all(|record| record.attempts == 0 && record.transfer_request_id.is_none())
        );
    }
    // These witnesses deliberately use only APIs present before repair paging.
    // The real queue and forwarded KV operations, rather than elapsed time,
    // establish bounded work and checkpoint ownership.
    #[derive(Clone, Debug)]
    struct RepairRetainedCheckpointGate {
        root: std::path::PathBuf,
        retired_intent_id: String,
        calls: std::sync::Arc<std::sync::Mutex<Vec<crate::DfsStageBlobCommand>>>,
    }

    #[derive(Clone, Debug)]
    struct RepairRetainOnAcquire {
        inner: RepairAuthorityProbe,
        metadata_root: std::path::PathBuf,
        object: DfsObjectMetadata,
        fired: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl DfsControlPlaneKvClient for RepairRetainOnAcquire {
        fn placement_authority_process_identity(
            &self,
        ) -> Option<crate::DfsPlacementAuthorityProcessIdentity> {
            self.inner.placement_authority_process_identity()
        }
        fn refresh_latest(&mut self) -> Result<(), sinomo_core::kv::KvError> {
            self.inner.refresh_latest()
        }
        fn get(
            &self,
            key: &str,
        ) -> Result<Option<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
            self.inner.get(key)
        }
        fn range_prefix(
            &self,
            prefix: &str,
        ) -> Result<Vec<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
            self.inner.range_prefix(prefix)
        }
        fn put(
            &mut self,
            key: String,
            value: Vec<u8>,
        ) -> Result<sinomo_core::kv::KvRevision, sinomo_core::kv::KvError> {
            self.inner.put(key, value)
        }
        fn txn(
            &mut self,
            txn: sinomo_core::kv::KvTxn,
        ) -> Result<sinomo_core::kv::KvTxnResponse, sinomo_core::kv::KvError> {
            let acquire = txn.success.iter().any(|op| matches!(op, sinomo_core::kv::KvOp::Put { key, .. } if key.contains("/placement-authority-permit-v1")));
            let result = self.inner.txn(txn);
            if acquire
                && result.as_ref().is_ok_and(|response| response.succeeded)
                && !self.fired.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                assert!(self.inner.permit_is_present());
                // The visible object stays identical. Use the real atomic
                // retained-version/pin API to change the packet classification
                // AFTER candidate collection and actual permit acquisition.
                let mut backend = open_root_local_metadata_backend(&self.metadata_root).unwrap();
                let mut version = backend
                    .get_version(&self.object.object_id, &self.object.version)
                    .unwrap()
                    .unwrap_or_else(|| {
                        crate::metadata_records::object_version_record(
                            &self.object,
                            self.object.content_size_bytes,
                        )
                    });
                assert!(!version.has_retention_grant());
                sinomo_dfs_history::install_history_capture(
                    &mut version,
                    crate::DfsRetainedFileVersion {
                        object: Box::new(self.object.clone()),
                        captured_at_unix_nanos: crate::dfs_now_unix_nanos(),
                        source: crate::DfsFileVersionSource::Checkpoint {
                            operation_id: "repair-retained-permit-hook".into(),
                        },
                        policy_capture: None,
                    },
                    0,
                )
                .unwrap();
                backend
                    .capture_file_version(
                        &version,
                        &crate::DfsMetadataVersionPinRecord {
                            namespace: None,
                            file_system_id: self.object.file_system_id.clone(),
                            object_id: self.object.object_id.clone(),
                            version: self.object.version.clone(),
                            pin_id: "repair-retained-permit-hook".into(),
                            created_at: time::OffsetDateTime::now_utc(),
                        },
                        Some(&self.object.version),
                    )
                    .unwrap();
                assert_eq!(
                    backend.get_object(&self.object.object_id).unwrap(),
                    Some(self.object.clone())
                );
            }
            result
        }
    }

    #[tokio::test]
    async fn repair_authority_retained_marker_change_cannot_await_before_retirement_checkpoint() {
        let root = sinomo_testdir::TestDir::new("repair-retained-kind-checkpoint");
        let (mut service, probe, mut intents) =
            repair_authority_fixture(root.path(), &["repair-page-job"; 2], None, false);
        repair_paging_complete_fixture_jobs(&probe, Some("repair-page-job"));
        repair_fault_make_stale(&mut service, &mut intents[0]);
        let object = service
            .current_read_object_metadata(&intents[1].object_id)
            .unwrap();
        let records = service.durable_repair_queue_records();
        assert_eq!(records.len(), 2);
        repair_paging_seed_queue(root.path(), &records);
        service = repair_paging_reopen(root.path(), service, &probe);
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        *probe.retained_checkpoint_gate.lock().unwrap() = Some(RepairRetainedCheckpointGate {
            root: root.path().to_path_buf(),
            retired_intent_id: intents[0].intent_id.clone(),
            calls: calls.clone(),
        });
        let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        service.control_plane = Some(Box::new(RepairRetainOnAcquire {
            inner: probe.clone(),
            metadata_root: root.path().join("metadata"),
            object: object.clone(),
            fired: fired.clone(),
        }));
        let result = repair_paging_tick(&mut service, root.path(), 2).await;
        assert!(fired.load(std::sync::atomic::Ordering::SeqCst));
        assert!(
            result.is_err(),
            "changed packet kind returns bounded progress to the checkpoint owner"
        );
        assert!(
            calls.lock().unwrap().is_empty(),
            "grouped processing must not enter the async retained send path"
        );
        assert!(probe.dispatched.lock().unwrap().is_empty());
        assert!(!probe.permit_is_present());
        let remaining = repair_paging_store(root.path()).view().unwrap().records;
        assert_eq!(
            remaining,
            records
                .into_iter()
                .filter(|record| record.intent_id == intents[1].intent_id)
                .collect::<Vec<_>>(),
            "only the completed stale intent is retired; retained task identity and attempt state survive"
        );
        let backend = service.metadata_backend().unwrap();
        assert_eq!(
            backend.get_object(&object.object_id).unwrap(),
            Some(object.clone())
        );
        assert!(
            backend
                .get_version(&object.object_id, &object.version)
                .unwrap()
                .unwrap()
                .has_retention_grant()
        );
        assert!(
            backend
                .version_pins_for_object(&object.object_id)
                .unwrap()
                .iter()
                .any(|pin| pin.pin_id == "repair-retained-permit-hook")
        );
        service = repair_paging_reopen(root.path(), service, &probe);
        let resumed = repair_paging_tick(&mut service, root.path(), 2)
            .await
            .unwrap();
        assert_eq!(
            resumed.scheduled_repairs.len(),
            1,
            "the legitimate retained transfer keeps the existing scalar path"
        );
        let sent = calls.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].intent_id, intents[1].intent_id);
        assert_eq!(sent[0].version, object.version);
        assert_eq!(Some(sent[0].content_hash.clone()), object.content_hash);
        let durable = repair_paging_store(root.path()).view().unwrap().records;
        assert_eq!(durable.len(), 1);
        assert!(durable[0].in_flight);
        assert_eq!(
            durable[0].transfer_request_id.as_deref(),
            Some(sent[0].request_id.as_str())
        );
        assert!(!probe.permit_is_present());
    }

    #[derive(Clone, Debug)]
    struct RepairMutationOnAcquire {
        inner: RepairAuthorityProbe,
        metadata_root: std::path::PathBuf,
        replacement: DfsObjectMetadata,
        fired: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl DfsControlPlaneKvClient for RepairMutationOnAcquire {
        fn placement_authority_process_identity(
            &self,
        ) -> Option<crate::DfsPlacementAuthorityProcessIdentity> {
            self.inner.placement_authority_process_identity()
        }
        fn refresh_latest(&mut self) -> Result<(), sinomo_core::kv::KvError> {
            self.inner.refresh_latest()
        }
        fn get(
            &self,
            key: &str,
        ) -> Result<Option<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
            self.inner.get(key)
        }
        fn range_prefix(
            &self,
            prefix: &str,
        ) -> Result<Vec<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
            self.inner.range_prefix(prefix)
        }
        fn put(
            &mut self,
            key: String,
            value: Vec<u8>,
        ) -> Result<sinomo_core::kv::KvRevision, sinomo_core::kv::KvError> {
            self.inner.put(key, value)
        }
        fn txn(
            &mut self,
            txn: sinomo_core::kv::KvTxn,
        ) -> Result<sinomo_core::kv::KvTxnResponse, sinomo_core::kv::KvError> {
            let acquire = txn.success.iter().any(|op| matches!(op, sinomo_core::kv::KvOp::Put { key, .. } if key.contains("/placement-authority-permit-v1")));
            let result = self.inner.txn(txn);
            if acquire
                && result.as_ref().is_ok_and(|response| response.succeeded)
                && !self.fired.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                assert!(self.inner.permit_is_present());
                // Inject an intervening namespace author through the normal
                // original-object read-set fence, not a raw table overwrite.
                let data_root = self.metadata_root.parent().unwrap();
                let mut author = NetworkedDfsService::new(
                    PreviewTransport {
                        local_node_id: node("node-a"),
                    },
                    LocalContentStore::new(data_root.join("content")).unwrap(),
                )
                .with_control_plane(self.inner.control.clone())
                .with_metadata_backend_lazy(
                    open_root_local_metadata_backend(&self.metadata_root).unwrap(),
                )
                .unwrap();
                let _planning = author.namespace_execution.begin_planning();
                let original = author
                    .current_read_object_metadata(&self.replacement.object_id)
                    .unwrap();
                assert_ne!(original.version, self.replacement.version);
                author
                    .add_object_metadata(self.replacement.clone())
                    .unwrap();
            }
            result
        }
    }

    fn repair_fault_make_stale(
        service: &mut NetworkedDfsService<PlacementRouteCounter>,
        intent: &mut DistributedWriteIntent,
    ) {
        intent.desired_replica_nodes.reverse();
        let object = service
            .current_read_object_metadata(&intent.object_id)
            .unwrap();
        service
            .put_metadata_intent(intent, object.content_hash.as_deref())
            .unwrap();
        service.coordinator.restore_intent(intent.clone()).unwrap();
    }

    #[tokio::test]
    async fn repair_authority_partial_retirement_survives_release_error_and_reopen() {
        let root = sinomo_testdir::TestDir::new("repair-page-release-error-progress");
        let (mut service, probe, mut intents) =
            repair_authority_fixture(root.path(), &["repair-page-job"; 3], None, false);
        repair_paging_complete_fixture_jobs(&probe, Some("repair-page-job"));
        for intent in &mut intents[..2] {
            repair_fault_make_stale(&mut service, intent);
        }
        let history = repair_paging_history(&service, &intents);
        let records = service.durable_repair_queue_records();
        repair_paging_seed_queue(root.path(), &records);
        service = repair_paging_reopen(root.path(), service, &probe);
        probe
            .fail_release
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(
            repair_paging_tick(&mut service, root.path(), 3)
                .await
                .is_err()
        );
        let remaining = repair_paging_store(root.path()).view().unwrap().records;
        assert_eq!(
            remaining.len(),
            1,
            "completed stale retirement is checkpointed even when exact release custody remains pending"
        );
        assert_eq!(remaining[0].intent_id, intents[2].intent_id);
        assert!(
            probe.dispatched.lock().unwrap().is_empty(),
            "failed release cannot hand off any prepared call"
        );
        assert!(probe.permit_is_present());
        assert_eq!(repair_paging_history(&service, &intents), history);
        service = repair_paging_reopen(root.path(), service, &probe);
        assert_eq!(
            repair_paging_store(root.path()).view().unwrap().records,
            remaining
        );
        probe
            .fail_release
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let resumed = repair_paging_tick(&mut service, root.path(), 3)
            .await
            .unwrap();
        assert_eq!(resumed.scheduled_repairs.len(), 1);
        assert!(
            !probe.permit_is_present(),
            "fresh service finishes exact pending release before acquiring its next permit"
        );
        assert!(
            repair_paging_store(root.path())
                .view()
                .unwrap()
                .records
                .iter()
                .all(|record| record.intent_id == intents[2].intent_id)
        );
        assert_eq!(repair_paging_history(&service, &intents), history);
    }

    #[tokio::test]
    async fn repair_authority_partial_retirement_survives_validator_error_and_reopen() {
        let root = sinomo_testdir::TestDir::new("repair-page-validation-error-progress");
        let (mut service, probe, mut intents) = repair_authority_fixture_with_snapshot(
            root.path(),
            &["repair-page-job"; 3],
            None,
            false,
            true,
        );
        repair_paging_complete_fixture_jobs(&probe, Some("repair-page-job"));
        for intent in &mut intents[..2] {
            repair_fault_make_stale(&mut service, intent);
        }
        let history = repair_paging_history(&service, &intents);
        let records = service.durable_repair_queue_records();
        repair_paging_seed_queue(root.path(), &records);
        service = repair_paging_reopen(root.path(), service, &probe);
        service
            .set_content_pin_snapshot([(intents[2].object_id.clone(), node("not-advertised-pin"))]);
        let object = service
            .current_read_object_metadata(&intents[2].object_id)
            .unwrap();
        probe.control.with_client(|kv| {
            let store = DfsControlPlaneStore::new(kv);
            let job = store
                .placement_reconciliation(&sifs_file_system_id(), "repair-page-job")
                .unwrap()
                .unwrap();
            assert!(
                reconciliation_placement_for_object_with_pins(
                    job.accepted_config.as_ref().unwrap(),
                    &job,
                    &object,
                    &service.content_pin_nodes(&object.object_id)
                )
                .is_err(),
                "fixture reaches a real planner error after the two stale members"
            );
        });
        assert!(
            repair_paging_tick(&mut service, root.path(), 3)
                .await
                .is_err()
        );
        let remaining = repair_paging_store(root.path()).view().unwrap().records;
        assert_eq!(
            remaining.len(),
            1,
            "the later validator error cannot discard completed cleanup"
        );
        assert_eq!(remaining[0].intent_id, intents[2].intent_id);
        assert!(!probe.permit_is_present());
        assert!(probe.dispatched.lock().unwrap().is_empty());
        service = repair_paging_reopen(root.path(), service, &probe);
        assert_eq!(
            repair_paging_store(root.path()).view().unwrap().records,
            remaining
        );
        assert_eq!(repair_paging_history(&service, &intents), history);
    }

    #[tokio::test]
    async fn repair_authority_partial_retirement_and_prepared_prefix_survive_dispatch_error() {
        let root = sinomo_testdir::TestDir::new("repair-page-dispatch-error-progress");
        let (mut service, probe, mut intents) =
            repair_authority_fixture(root.path(), &["repair-page-job"; 3], None, false);
        repair_paging_complete_fixture_jobs(&probe, Some("repair-page-job"));
        repair_fault_make_stale(&mut service, &mut intents[1]);
        let history = repair_paging_history(&service, &intents);
        let records = service.durable_repair_queue_records();
        repair_paging_seed_queue(root.path(), &records);
        service = repair_paging_reopen(root.path(), service, &probe);
        *probe.fail_dispatch_intent.lock().unwrap() = Some(intents[2].intent_id.clone());
        assert!(
            repair_paging_tick(&mut service, root.path(), 3)
                .await
                .is_err()
        );
        let remaining = repair_paging_store(root.path()).view().unwrap().records;
        assert_eq!(remaining.len(), 2);
        assert!(
            remaining
                .iter()
                .all(|record| record.intent_id != intents[1].intent_id)
        );
        let prepared = probe.dispatched.lock().unwrap().clone();
        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].intent_id, intents[0].intent_id);
        assert!(
            remaining
                .iter()
                .any(|record| record.intent_id == prepared[0].intent_id
                    && record.in_flight
                    && record.transfer_request_id.as_deref()
                        == Some(prepared[0].request_id.as_str())),
            "every prepared prefix call has durable exact attempt ownership despite a later error"
        );
        let failed = remaining
            .iter()
            .find(|record| record.intent_id == intents[2].intent_id)
            .unwrap();
        assert_eq!(
            failed,
            records
                .iter()
                .find(|record| record.intent_id == intents[2].intent_id)
                .unwrap()
        );
        assert!(!probe.permit_is_present());
        service = repair_paging_reopen(root.path(), service, &probe);
        assert_eq!(
            repair_paging_store(root.path()).view().unwrap().records,
            remaining
        );
        assert_eq!(repair_paging_history(&service, &intents), history);
    }

    #[tokio::test]
    async fn repair_authority_rechecks_changed_content_after_real_permit_acquisition() {
        let root = sinomo_testdir::TestDir::new("repair-page-acquire-content-change");
        let (mut service, probe, intents) =
            repair_authority_fixture(root.path(), &["repair-page-job"; 3], None, false);
        repair_paging_complete_fixture_jobs(&probe, Some("repair-page-job"));
        let mut replacement = service
            .current_read_object_metadata(&intents[0].object_id)
            .unwrap();
        let bytes = b"new authoritative current content";
        replacement.version =
            DfsVersion::from_legacy_named("repair-permit-hook-new-version").unwrap();
        replacement.content_hash = Some(crate::content_hash(bytes));
        replacement.content_size_bytes = Some(bytes.len() as u64);
        service
            .content()
            .put_object_version(
                replacement.object_id.as_str(),
                replacement.version.as_str(),
                bytes,
            )
            .unwrap();
        let records = service.durable_repair_queue_records();
        repair_paging_seed_queue(root.path(), &records);
        service = repair_paging_reopen(root.path(), service, &probe);
        let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        service.control_plane = Some(Box::new(RepairMutationOnAcquire {
            inner: probe.clone(),
            metadata_root: root.path().join("metadata"),
            replacement: replacement.clone(),
            fired: fired.clone(),
        }));
        repair_paging_tick(&mut service, root.path(), 3)
            .await
            .unwrap();
        assert!(fired.load(std::sync::atomic::Ordering::SeqCst));
        assert!(
            probe
                .dispatched
                .lock()
                .unwrap()
                .iter()
                .all(|command| command.intent_id != intents[0].intent_id),
            "candidate checks before acquisition cannot authorize old content after the real permit hook changes metadata"
        );
        assert_eq!(
            service
                .metadata_backend()
                .unwrap()
                .get_object(&replacement.object_id)
                .unwrap(),
            Some(replacement)
        );
        let remaining = repair_paging_store(root.path()).view().unwrap().records;
        assert!(
            remaining
                .iter()
                .all(|record| record.intent_id != intents[0].intent_id)
        );
        assert!(!probe.permit_is_present());
        assert_eq!(
            service
                .metadata_backend()
                .unwrap()
                .get_intent(&intents[0].object_id, &intents[0].intent_id)
                .unwrap()
                .unwrap()
                .status,
            DfsWriteIntentStatus::Committed
        );
    }

    const REPAIR_PAGING_TEST_WORK: usize = 8;
    const REPAIR_PAGING_TEST_MAX_EXAMINED: usize = 16;

    #[derive(Clone, Debug)]
    struct RepairPagingForegroundControl {
        inner: RepairAuthorityProbe,
        scheduler: std::sync::Arc<crate::DfsMutationScheduler>,
        requested: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl DfsControlPlaneKvClient for RepairPagingForegroundControl {
        fn placement_authority_process_identity(
            &self,
        ) -> Option<crate::DfsPlacementAuthorityProcessIdentity> {
            self.inner.placement_authority_process_identity()
        }

        fn refresh_latest(&mut self) -> Result<(), sinomo_core::kv::KvError> {
            self.inner.refresh_latest()
        }

        fn get(
            &self,
            key: &str,
        ) -> Result<Option<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
            self.inner.get(key)
        }

        fn range_prefix(
            &self,
            prefix: &str,
        ) -> Result<Vec<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
            self.inner.range_prefix(prefix)
        }

        fn put(
            &mut self,
            key: String,
            value: Vec<u8>,
        ) -> Result<sinomo_core::kv::KvRevision, sinomo_core::kv::KvError> {
            self.inner.put(key, value)
        }

        fn txn(
            &mut self,
            txn: sinomo_core::kv::KvTxn,
        ) -> Result<sinomo_core::kv::KvTxnResponse, sinomo_core::kv::KvError> {
            let acquires = txn.success.iter().any(|op| {
                matches!(op,
                sinomo_core::kv::KvOp::Put { key, .. }
                    if key.contains("/placement-authority-permit-v1"))
            });
            let result = self.inner.txn(txn);
            if acquires
                && result.as_ref().is_ok_and(|response| response.succeeded)
                && !self
                    .requested
                    .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                assert!(self.scheduler.global_is_exclusively_held());
                self.scheduler.request_foreground_admission();
            }
            result
        }
    }

    fn repair_paging_complete_fixture_jobs(probe: &RepairAuthorityProbe, extra_job: Option<&str>) {
        probe.control.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv);
            for job_id in std::iter::once("placement-test-job").chain(extra_job) {
                let current = store
                    .placement_reconciliation(&sifs_file_system_id(), job_id)
                    .unwrap()
                    .unwrap();
                let mut completed = current.clone();
                completed.state = DfsPlacementReconciliationState::Completed;
                completed.cursor = None;
                completed.revision += 1;
                assert!(
                    store
                        .compare_and_put_placement_reconciliation(&current, &completed)
                        .unwrap()
                );
            }
        });
    }

    fn repair_paging_stale_fixture(
        root: &std::path::Path,
        count: usize,
        fanout: bool,
    ) -> (
        NetworkedDfsService<PlacementRouteCounter>,
        RepairAuthorityProbe,
        Vec<DistributedWriteIntent>,
    ) {
        let (mut service, probe, mut intents) =
            repair_authority_fixture(root, &vec!["repair-page-job"; count], None, fanout);
        repair_paging_complete_fixture_jobs(&probe, Some("repair-page-job"));
        for intent in &mut intents {
            // Keep the accepted job/config current. The persisted intent's old
            // ordered plan is stale, so the real permit validator must retire
            // it without changing its committed outcome or content receipts.
            intent.desired_replica_nodes.reverse();
            let object = service
                .current_read_object_metadata(&intent.object_id)
                .unwrap();
            service
                .put_metadata_intent(intent, object.content_hash.as_deref())
                .unwrap();
            service.coordinator.restore_intent(intent.clone()).unwrap();
        }
        assert_eq!(
            service.durable_repair_queue_records().len(),
            count * if fanout { 2 } else { 1 }
        );
        (service, probe, intents)
    }

    fn repair_paging_store(
        root: &std::path::Path,
    ) -> crate::repair_queue_store::LocalDfsRepairQueueStore {
        let data_dir = sinomo_core::local::SinomoDataDir::new(root);
        crate::repair_queue_store::LocalDfsRepairQueueStore::new(
            crate::default_dfs_repair_attempt_snapshot_path(&data_dir),
        )
    }

    fn repair_paging_seed_queue(root: &std::path::Path, records: &[crate::DfsRepairAttemptRecord]) {
        use crate::DfsMaintenanceHost;
        let mut host =
            crate::LocalDfsMaintenanceHost::new(sinomo_core::local::SinomoDataDir::new(root));
        host.store_repair_attempt_records(records).unwrap();
        assert_eq!(
            repair_paging_store(root).view().unwrap().records.len(),
            records.len()
        );
    }

    fn repair_paging_reopen(
        root: &std::path::Path,
        service: NetworkedDfsService<PlacementRouteCounter>,
        probe: &RepairAuthorityProbe,
    ) -> NetworkedDfsService<PlacementRouteCounter> {
        let transport = service.transport.clone();
        drop(service);
        NetworkedDfsService::new(
            transport,
            LocalContentStore::new(root.join("content")).unwrap(),
        )
        .with_control_plane(probe.clone())
        .with_metadata_backend_lazy(
            open_root_local_metadata_backend(root.join("metadata")).unwrap(),
        )
        .unwrap()
    }

    fn repair_paging_history(
        service: &NetworkedDfsService<PlacementRouteCounter>,
        intents: &[DistributedWriteIntent],
    ) -> Vec<serde_json::Value> {
        let backend = service.metadata_backend().unwrap();
        intents
            .iter()
            .map(|intent| {
                let object = backend.get_object(&intent.object_id).unwrap().unwrap();
                let record = backend
                    .get_intent(&intent.object_id, &intent.intent_id)
                    .unwrap()
                    .unwrap();
                let receipts = ["node-a", "node-b", "node-c"]
                    .into_iter()
                    .map(|replica| {
                        backend
                            .get_receipt_on_shard(
                                &crate::DfsMetadataShardId::new("root"),
                                &intent.object_id,
                                &intent.target_version,
                                &node(replica),
                            )
                            .unwrap()
                    })
                    .collect::<Vec<_>>();
                serde_json::json!({"object": object, "intent": record, "receipts": receipts})
            })
            .collect()
    }

    async fn repair_paging_tick(
        service: &mut NetworkedDfsService<PlacementRouteCounter>,
        root: &std::path::Path,
        max_work: usize,
    ) -> Result<crate::DfsMaintenanceTickReport, DfsError> {
        let data_dir = sinomo_core::local::SinomoDataDir::new(root);
        let mut host = crate::LocalDfsMaintenanceHost::new(data_dir.clone());
        let mut publisher = crate::LocalDfsMetadataJournalPublisherHost::new(data_dir.clone());
        let mut transport = crate::AsyncDfsMetadataNodeTransport::new(
            Vec::new(),
            std::sync::Arc::new(|_, _, _| Box::pin(async { panic!("publication disabled") })),
            std::sync::Arc::new(|_, _, _| Box::pin(async { panic!("publication disabled") })),
        );
        let local = service.node_id().clone();
        crate::run_dfs_maintenance_tick_with_async_host(
            service,
            &mut host,
            &data_dir,
            sifs_file_system_id(),
            &local,
            Vec::new(),
            &mut publisher,
            &mut transport,
            crate::DfsMaintenanceTickOptions {
                namespace_job_max_jobs: 0,
                conflict_max_records: 0,
                write_intent_max_records: 0,
                placement_reconciliation_max_objects: 8,
                repair_max_jobs: max_work,
                publish_metadata: false,
                lifecycle: crate::DfsMaintenanceRunOptions {
                    repair_metadata: false,
                    cleanup_stale_staging: false,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
    }

    #[tokio::test]
    async fn repair_authority_paged_stale_queue_checkpoints_and_continues_without_new_activity() {
        let root = sinomo_testdir::TestDir::new("repair-page-stale-137");
        let (mut service, probe, intents) = repair_paging_stale_fixture(root.path(), 137, false);
        let history = repair_paging_history(&service, &intents);
        let original = service.durable_repair_queue_records();
        repair_paging_seed_queue(root.path(), &original);
        service = repair_paging_reopen(root.path(), service, &probe);
        let scheduler = std::sync::Arc::new(crate::DfsMutationScheduler::new());
        let mut previous = original.len();
        let mut completed = false;
        for _ in 0..original.len() {
            let admission = scheduler
                .admit(&crate::DfsMessageExecutionScope::GlobalExclusive)
                .await
                .unwrap();
            let before_permits = probe.permit_transactions();
            let report = repair_paging_tick(&mut service, root.path(), REPAIR_PAGING_TEST_WORK)
                .await
                .unwrap();
            let remaining = repair_paging_store(root.path()).view().unwrap().records;
            let removed = previous
                .checked_sub(remaining.len())
                .expect("stale cleanup cannot add work");
            assert!(
                removed > 0 && removed <= REPAIR_PAGING_TEST_MAX_EXAMINED,
                "one stale page must checkpoint bounded real work; removed {removed} of {previous}"
            );
            let after_permits = probe.permit_transactions();
            assert_eq!(
                (
                    after_permits.0 - before_permits.0,
                    after_permits.1 - before_permits.1
                ),
                (1, 1),
                "same-job stale retirement shares one actual permit"
            );
            assert!(report.scheduled_repairs.is_empty());
            assert!(probe.dispatched.lock().unwrap().is_empty());
            assert_eq!(
                crate::dfs_maintenance_report_reached_limit(&report),
                !remaining.is_empty(),
                "checkpointed stale progress and its suffix own immediate continuation"
            );
            assert_eq!(repair_paging_history(&service, &intents), history);
            assert!(!probe.permit_is_present());
            drop(admission);
            previous = remaining.len();
            service = repair_paging_reopen(root.path(), service, &probe);
            if remaining.is_empty() {
                completed = true;
                break;
            }
        }
        assert!(
            completed,
            "bounded passes drain the real stale queue without another wake"
        );
        let before_permits = probe.permit_transactions();
        let admission = scheduler
            .admit(&crate::DfsMessageExecutionScope::GlobalExclusive)
            .await
            .unwrap();
        let idle = repair_paging_tick(&mut service, root.path(), REPAIR_PAGING_TEST_WORK)
            .await
            .unwrap();
        assert!(!crate::dfs_maintenance_report_reached_limit(&idle));
        assert!(
            repair_paging_store(root.path())
                .view()
                .unwrap()
                .records
                .is_empty()
        );
        assert_eq!(probe.permit_transactions(), before_permits);
        assert_eq!(repair_paging_history(&service, &intents), history);
        drop(admission);
    }

    #[tokio::test]
    async fn repair_authority_stale_page_checkpoints_before_waiting_foreground_admission() {
        #[derive(Debug)]
        struct GenerationDrop {
            dropped: std::sync::Arc<std::sync::atomic::AtomicBool>,
            scheduler: std::sync::Arc<crate::DfsMutationScheduler>,
            probe: RepairAuthorityProbe,
            owned_at_drop: std::sync::Arc<std::sync::atomic::AtomicBool>,
            permit_free_at_drop: std::sync::Arc<std::sync::atomic::AtomicBool>,
        }
        impl Drop for GenerationDrop {
            fn drop(&mut self) {
                self.owned_at_drop.store(
                    self.scheduler.global_is_exclusively_held(),
                    std::sync::atomic::Ordering::SeqCst,
                );
                self.permit_free_at_drop.store(
                    !self.probe.permit_is_present(),
                    std::sync::atomic::Ordering::SeqCst,
                );
                self.dropped
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let root = sinomo_testdir::TestDir::new("repair-page-foreground-137");
        let (mut service, probe, intents) = repair_paging_stale_fixture(root.path(), 137, false);
        let history = repair_paging_history(&service, &intents);
        let original = service.durable_repair_queue_records();
        repair_paging_seed_queue(root.path(), &original);
        service = repair_paging_reopen(root.path(), service, &probe);
        let reopen_transport = service.transport.clone();
        let scheduler = std::sync::Arc::new(crate::DfsMutationScheduler::new());
        let interest = scheduler.begin_maintenance_interest();
        let admission = scheduler
            .admit(&crate::DfsMessageExecutionScope::GlobalExclusive)
            .await
            .unwrap();
        let requested = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        service.control_plane = Some(Box::new(RepairPagingForegroundControl {
            inner: probe.clone(),
            scheduler: scheduler.clone(),
            requested: requested.clone(),
        }));
        // Register an actual waiter on the same held scheduler. The production
        // foreground edge is issued synchronously by the first real permit CAS.
        let mut foreground =
            Box::pin(scheduler.admit(&crate::DfsMessageExecutionScope::GlobalExclusive));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            std::future::Future::poll(foreground.as_mut(), &mut context),
            std::task::Poll::Pending
        ));
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let owned_at_drop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let permit_free_at_drop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let generation = GenerationDrop {
            dropped: dropped.clone(),
            scheduler: scheduler.clone(),
            probe: probe.clone(),
            owned_at_drop: owned_at_drop.clone(),
            permit_free_at_drop: permit_free_at_drop.clone(),
        };
        let worker_root = root.path().to_path_buf();
        let outcome = crate::run_dfs_maintenance_until_foreground_waits(&interest, async move {
            let _generation = generation;
            repair_paging_tick(&mut service, &worker_root, REPAIR_PAGING_TEST_WORK)
                .await
                .unwrap()
        })
        .await;
        assert!(requested.load(std::sync::atomic::Ordering::SeqCst));
        assert!(matches!(
            outcome,
            crate::DfsMaintenancePreemptionOutcome::ForegroundWaiting
        ));
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
        assert!(
            owned_at_drop.load(std::sync::atomic::Ordering::SeqCst),
            "service future is destroyed before global admission release"
        );
        assert!(
            permit_free_at_drop.load(std::sync::atomic::Ordering::SeqCst),
            "permit is released before cancellation"
        );
        assert!(scheduler.global_is_exclusively_held());
        let remaining = repair_paging_store(root.path()).view().unwrap().records;
        let removed = original.len() - remaining.len();
        assert!(
            removed > 0 && removed <= REPAIR_PAGING_TEST_MAX_EXAMINED,
            "foreground must observe a durable bounded stale prefix, removed {removed}"
        );
        assert_eq!(probe.permit_transactions(), (1, 1));
        assert!(
            remaining.iter().all(|record| original.contains(record)),
            "unvisited queue suffix keeps exact attempt records"
        );
        assert!(probe.dispatched.lock().unwrap().is_empty());
        assert!(!probe.permit_is_present());
        drop(admission);
        let foreground_admission = foreground.await.unwrap();
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!probe.permit_is_present());
        drop(foreground_admission);
        let mut fresh = NetworkedDfsService::new(
            reopen_transport,
            LocalContentStore::new(root.path().join("content")).unwrap(),
        )
        .with_control_plane(probe.clone())
        .with_metadata_backend_lazy(
            open_root_local_metadata_backend(root.path().join("metadata")).unwrap(),
        )
        .unwrap();
        assert_eq!(repair_paging_history(&fresh, &intents), history);
        assert_eq!(
            repair_paging_store(root.path()).view().unwrap().records,
            remaining
        );
        let admission = scheduler
            .admit(&crate::DfsMessageExecutionScope::GlobalExclusive)
            .await
            .unwrap();
        let resumed = repair_paging_tick(&mut fresh, root.path(), REPAIR_PAGING_TEST_WORK)
            .await
            .unwrap();
        let after_resume = repair_paging_store(root.path()).view().unwrap().records;
        assert!(after_resume.len() < remaining.len());
        assert!(
            after_resume.iter().all(|record| remaining.contains(record)),
            "fresh service cannot resurrect the retired prefix"
        );
        assert!(crate::dfs_maintenance_report_reached_limit(&resumed));
        assert_eq!(repair_paging_history(&fresh, &intents), history);
        drop(admission);
    }

    #[tokio::test]
    async fn repair_authority_stale_intent_crossing_page_keeps_unvisited_intent_durable() {
        let root = sinomo_testdir::TestDir::new("repair-page-sibling-boundary");
        let (mut service, probe, intents) = repair_paging_stale_fixture(root.path(), 2, true);
        let history = repair_paging_history(&service, &intents);
        let original = service.durable_repair_queue_records();
        assert_eq!(original.len(), 4);
        repair_paging_seed_queue(root.path(), &original);
        service = repair_paging_reopen(root.path(), service, &probe);
        let report = repair_paging_tick(&mut service, root.path(), 1)
            .await
            .unwrap();
        let remaining = repair_paging_store(root.path()).view().unwrap().records;
        assert_eq!(
            remaining.len(),
            2,
            "one examined stale intent removes all its target siblings, not the next intent"
        );
        assert!(
            remaining
                .iter()
                .all(|record| record.intent_id == intents[1].intent_id)
        );
        assert!(crate::dfs_maintenance_report_reached_limit(&report));
        assert_eq!(probe.permit_transactions(), (1, 1));
        service = repair_paging_reopen(root.path(), service, &probe);
        let final_report = repair_paging_tick(&mut service, root.path(), 1)
            .await
            .unwrap();
        assert!(
            repair_paging_store(root.path())
                .view()
                .unwrap()
                .records
                .is_empty()
        );
        assert!(!crate::dfs_maintenance_report_reached_limit(&final_report));
        assert_eq!(repair_paging_history(&service, &intents), history);
        assert!(probe.dispatched.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn repair_authority_paged_early_insertions_do_not_starve_existing_tail() {
        let root = sinomo_testdir::TestDir::new("repair-page-insert-fairness");
        let (mut service, probe, intents) =
            repair_authority_fixture(root.path(), &["repair-page-job"; 48], None, false);
        repair_paging_complete_fixture_jobs(&probe, Some("repair-page-job"));
        let records = service.durable_repair_queue_records();
        let mut initial = records[32..36].to_vec();
        for record in &mut initial {
            record.attempts = 1;
            record.next_retry_unix_nanos = u64::MAX - 1;
        }
        initial.push(records[47].clone());
        repair_paging_seed_queue(root.path(), &initial);
        service = repair_paging_reopen(root.path(), service, &probe);
        let tail = &intents[47].intent_id;
        for pass in 0..6 {
            if pass > 0 {
                let mut earlier = records[16 - pass].clone();
                earlier.attempts = 1;
                earlier.next_retry_unix_nanos = u64::MAX - 1;
                repair_paging_store(root.path())
                    .append_enqueues([earlier])
                    .unwrap();
            }
            let _ = repair_paging_tick(&mut service, root.path(), 1)
                .await
                .unwrap();
            if probe
                .dispatched
                .lock()
                .unwrap()
                .iter()
                .any(|command| &command.intent_id == tail)
            {
                return;
            }
            service = repair_paging_reopen(root.path(), service, &probe);
        }
        panic!("new earlier keys must not repeatedly rewind ahead of an existing ready tail");
    }

    #[tokio::test]
    async fn repair_authority_empty_old_queue_executes_same_tick_placement_repairs() {
        let root = sinomo_testdir::TestDir::new("repair-page-new-placement-work");
        let (mut service, probe, _) = repair_authority_fixture(root.path(), &[], None, false);
        assert!(
            repair_paging_store(root.path())
                .view()
                .unwrap()
                .records
                .is_empty()
        );
        assert!(service.durable_repair_queue_records().is_empty());
        let report = repair_paging_tick(&mut service, root.path(), REPAIR_PAGING_TEST_WORK)
            .await
            .unwrap();
        assert!(
            !report.scheduled_repairs.is_empty(),
            "placement-created repair work executes when the old observed page was empty"
        );
        assert!(!probe.dispatched.lock().unwrap().is_empty());
        let durable = repair_paging_store(root.path()).view().unwrap().records;
        assert!(!durable.is_empty());
        assert!(
            durable
                .iter()
                .any(|record| record.in_flight && record.attempts > 0)
        );
        service = repair_paging_reopen(root.path(), service, &probe);
        assert_eq!(
            repair_paging_store(root.path()).view().unwrap().records,
            durable
        );
        assert!(
            service.coordinator.repair_jobs().is_empty(),
            "fresh service derives execution only when its durable queue is admitted"
        );
    }
    // This host deliberately implements only the pre-paging snapshot contract.
    // Its persistence is still the real local durable queue owner.
    struct LegacyRepairSnapshotHost {
        inner: crate::LocalDfsMaintenanceHost,
    }

    impl crate::DfsMaintenanceHost for LegacyRepairSnapshotHost {
        fn load_anti_entropy_cursor(
            &mut self,
            file_system_id: &DfsFileSystemId,
        ) -> Result<Option<crate::DfsAntiEntropyCursorRecord>, DfsError> {
            crate::DfsMaintenanceHost::load_anti_entropy_cursor(&mut self.inner, file_system_id)
        }
        fn store_anti_entropy_cursor(
            &mut self,
            cursor: &crate::DfsAntiEntropyCursorRecord,
        ) -> Result<(), DfsError> {
            crate::DfsMaintenanceHost::store_anti_entropy_cursor(&mut self.inner, cursor)
        }
        fn clear_anti_entropy_cursor(
            &mut self,
            file_system_id: &DfsFileSystemId,
        ) -> Result<(), DfsError> {
            crate::DfsMaintenanceHost::clear_anti_entropy_cursor(&mut self.inner, file_system_id)
        }
        fn load_repair_attempt_records(
            &mut self,
        ) -> Result<Vec<crate::DfsRepairAttemptRecord>, DfsError> {
            crate::DfsMaintenanceHost::load_repair_attempt_records(&mut self.inner)
        }
        fn store_repair_attempt_records(
            &mut self,
            records: &[crate::DfsRepairAttemptRecord],
        ) -> Result<(), DfsError> {
            crate::DfsMaintenanceHost::store_repair_attempt_records(&mut self.inner, records)
        }
    }

    async fn repair_legacy_snapshot_tick(
        service: &mut NetworkedDfsService<PlacementRouteCounter>,
        root: &std::path::Path,
        max_work: usize,
    ) -> Result<crate::DfsMaintenanceTickReport, DfsError> {
        let data_dir = sinomo_core::local::SinomoDataDir::new(root);
        let mut host = LegacyRepairSnapshotHost {
            inner: crate::LocalDfsMaintenanceHost::new(data_dir.clone()),
        };
        let mut publisher = crate::LocalDfsMetadataJournalPublisherHost::new(data_dir.clone());
        let mut transport = crate::AsyncDfsMetadataNodeTransport::new(
            Vec::new(),
            std::sync::Arc::new(|_, _, _| Box::pin(async { panic!("publication disabled") })),
            std::sync::Arc::new(|_, _, _| Box::pin(async { panic!("publication disabled") })),
        );
        let local = service.node_id().clone();
        crate::run_dfs_maintenance_tick_with_async_host(
            service,
            &mut host,
            &data_dir,
            sifs_file_system_id(),
            &local,
            Vec::new(),
            &mut publisher,
            &mut transport,
            crate::DfsMaintenanceTickOptions {
                namespace_job_max_jobs: 0,
                conflict_max_records: 0,
                write_intent_max_records: 0,
                placement_reconciliation_max_objects: 8,
                repair_max_jobs: max_work,
                publish_metadata: false,
                lifecycle: crate::DfsMaintenanceRunOptions {
                    repair_metadata: false,
                    cleanup_stale_staging: false,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
    }

    #[tokio::test]
    async fn repair_authority_legacy_zero_budget_preserves_exact_durable_queue() {
        let root = sinomo_testdir::TestDir::new("repair-legacy-zero-budget");
        let (mut service, probe, intents) =
            repair_authority_fixture(root.path(), &["repair-page-job"; 2], None, false);
        repair_paging_complete_fixture_jobs(&probe, Some("repair-page-job"));
        let history = repair_paging_history(&service, &intents);
        let mut original = service.durable_repair_queue_records();
        for (index, record) in original.iter_mut().enumerate() {
            record.attempts = 3;
            record.in_flight = true;
            record.last_attempt_unix_nanos = 100 + index as u64;
            record.next_retry_unix_nanos = u64::MAX - 1;
            record.transfer_request_id = Some(format!("legacy-unresolved-transfer-{index}"));
            record.last_error = Some("preserve this concrete prior-attempt evidence".into());
        }
        repair_paging_seed_queue(root.path(), &original);
        service = repair_paging_reopen(root.path(), service, &probe);
        assert!(service.coordinator.repair_jobs().is_empty());
        let scheduler = std::sync::Arc::new(crate::DfsMutationScheduler::new());
        let admission = scheduler
            .admit(&crate::DfsMessageExecutionScope::GlobalExclusive)
            .await
            .unwrap();
        let report = repair_legacy_snapshot_tick(&mut service, root.path(), 0)
            .await
            .unwrap();
        assert!(report.scheduled_repairs.is_empty());
        assert!(!crate::dfs_maintenance_report_reached_limit(&report));
        assert_eq!(
            repair_paging_store(root.path()).view().unwrap().records,
            original,
            "zero execution budget cannot replace an unloaded durable queue with an empty service projection"
        );
        assert_eq!(probe.permit_transactions(), (0, 0));
        assert!(probe.dispatched.lock().unwrap().is_empty());
        assert_eq!(repair_paging_history(&service, &intents), history);
        drop(admission);
    }

    #[tokio::test]
    async fn repair_authority_legacy_groups_preserve_parked_retry_identity_before_ready_tail() {
        let root = sinomo_testdir::TestDir::new("repair-legacy-parked-groups");
        let (mut service, probe, intents) =
            repair_authority_fixture(root.path(), &["repair-page-job"; 35], None, false);
        repair_paging_complete_fixture_jobs(&probe, Some("repair-page-job"));
        let mut original = service.durable_repair_queue_records();
        assert_eq!(original.len(), 35);
        for (index, record) in original.iter_mut().take(34).enumerate() {
            record.attempts = 3;
            record.in_flight = true;
            record.last_attempt_unix_nanos = 1_000 + index as u64;
            record.next_retry_unix_nanos = u64::MAX - 1;
            record.transfer_request_id = Some(format!("legacy-held-request-{index}"));
            record.last_error = Some(format!("preserve concrete attempt marker {index}"));
        }
        let parked = original[..34].to_vec();
        let tail = intents[34].intent_id.clone();
        repair_paging_seed_queue(root.path(), &original);
        service = repair_paging_reopen(root.path(), service, &probe);
        let scheduler = std::sync::Arc::new(crate::DfsMutationScheduler::new());
        let admission = scheduler
            .admit(&crate::DfsMessageExecutionScope::GlobalExclusive)
            .await
            .unwrap();
        let report = repair_legacy_snapshot_tick(&mut service, root.path(), 1)
            .await
            .unwrap();
        assert_eq!(report.scheduled_repairs.len(), 1);
        assert_eq!(report.scheduled_repairs[0].intent_id, tail);
        let dispatched = probe.dispatched.lock().unwrap();
        assert_eq!(dispatched.len(), 1);
        assert_eq!(
            dispatched[0].intent_id, tail,
            "ordinary future backoff remains effective across all internal groups"
        );
        drop(dispatched);
        let after = repair_paging_store(root.path()).view().unwrap().records;
        assert_eq!(after.len(), original.len());
        for expected in &parked {
            let actual = after
                .iter()
                .find(|record| {
                    record.intent_id == expected.intent_id
                        && record.missing_replica_node_id == expected.missing_replica_node_id
                })
                .unwrap();
            assert_eq!(
                actual, expected,
                "a later group cannot erase an earlier target's exact deadline, in-flight request ID or error witness"
            );
        }
        assert_eq!(probe.permit_transactions(), (1, 1));
        assert!(!probe.permit_is_present());
        drop(admission);
    }
}
