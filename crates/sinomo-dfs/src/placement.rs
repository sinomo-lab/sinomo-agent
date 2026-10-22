//! Durable, explainable DFS placement policy and reconciliation records.

#[cfg(test)]
#[path = "placement/native_noop_tests.rs"]
mod native_noop_tests;
#[cfg(test)]
#[path = "placement/native_repair_authority_tests.rs"]
mod native_repair_authority_tests;

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

fn matching_placement_noop_rows(
    expected: &[(&DfsObjectMetadata, &DfsMetadataShardConfig)],
    rows: Vec<DfsObjectMetadata>,
) -> Vec<(DfsObjectMetadata, DfsMetadataShardConfig)> {
    let mut returned = BTreeMap::<DfsObjectId, Option<DfsObjectMetadata>>::new();
    let mut paths = BTreeMap::<String, usize>::new();
    for row in rows {
        *paths.entry(row.path.clone()).or_default() += 1;
        if let Some(previous) = returned.get_mut(&row.object_id) {
            *previous = None;
        } else {
            returned.insert(row.object_id.clone(), Some(row));
        }
    }
    expected
        .iter()
        .filter_map(|(object, shard)| {
            (returned.get(&object.object_id).and_then(Option::as_ref) == Some(*object)
                && paths.get(&object.path) == Some(&1))
            .then(|| ((*object).clone(), (*shard).clone()))
        })
        .collect()
}

fn validate_local_placement_owner(
    object: &DfsObjectMetadata,
    target_policy: &DfsObjectPolicy,
) -> Result<(), DfsError> {
    let local = |policy: &DfsObjectPolicy| {
        matches!(
            policy,
            DfsObjectPolicy::LocalOwned | DfsObjectPolicy::EphemeralLocal
        )
    };
    if object.policy != *target_policy && local(target_policy) {
        return Err(DfsError::InvalidReplicationPolicy {
            message: "placement policy conversion requires an explicit content-owner localization protocol".into(),
        });
    }
    if local(target_policy)
        && matches!(
            object.kind,
            DfsObjectKind::File | DfsObjectKind::Symlink | DfsObjectKind::Artifact
        )
        && object.content_hash.is_some()
        && !object.current_locations.contains(&object.owner_node_id)
    {
        return Err(DfsError::Distributed {
            message: "local placement requires its existing content owner; generic replica repair cannot restore that ownership".into(),
        });
    }
    Ok(())
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
    default_desired_replicas: usize,
) -> Result<DfsResolvedPlacementPolicy, DfsDistributedError> {
    let path = normalize_posix_path(path).map_err(|error| DfsDistributedError::InvalidPolicy {
        reason: error.to_string(),
    })?;
    let mut exact_policies = config
        .subtree_policies
        .iter()
        .filter(|rule| rule.path_prefix == path)
        .map(|rule| {
            placement_policy_from_rule(rule).map(|policy| DfsExactPlacementPolicy {
                scope: rule.scope,
                policy,
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
                default_desired_replicas,
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
        let policy = default_placement_policy(object_policy, default_desired_replicas)?;
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
    let policy = placement_policy_from_rule(rule)?;
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
    default_desired_replicas: usize,
    scope: DfsPlacementScope,
) -> Result<DfsPlacementScopeResolution, DfsDistributedError> {
    if let Some(rule) = config
        .subtree_policies
        .iter()
        .find(|rule| rule.path_prefix == path && rule.scope == scope)
    {
        return Ok(DfsPlacementScopeResolution {
            scope,
            policy: placement_policy_from_rule(rule)?,
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
            policy: default_placement_policy(object_policy, default_desired_replicas)?,
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
        policy: placement_policy_from_rule(rule)?,
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

pub(crate) fn placement_policy_from_rule(
    rule: &DfsSubtreePolicy,
) -> Result<DfsPlacementPolicy, DfsDistributedError> {
    let policy = DfsPlacementPolicy {
        consistency: rule.policy.clone(),
        min_commit_replicas: rule.min_commit_replicas,
        desired_replicas: rule.desired_replicas,
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

fn default_placement_policy(
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
        .map(|rule| placement_policy_from_rule(rule))
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
        default_desired_replicas: usize,
    ) -> Result<DfsResolvedPlacementPolicy, DfsError> {
        let config = self.current_placement_file_system_config()?;
        resolve_placement_policy(&config, path, object_policy, default_desired_replicas)
            .map_err(DfsError::from)
    }

    pub fn placement_content_candidates(&mut self) -> Result<Vec<DfsNodeAdvertisement>, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(vec![DfsNodeAdvertisement::content(self.node_id.clone())]);
        };
        let mut candidates = match control_plane.write_configuration_projection()? {
            Some(configuration) => configuration.content_advertisements()?,
            // Compatibility callers retain the same content advertisements;
            // metadata offers do not participate in content placement.
            None => DfsControlPlaneStore::new(control_plane.as_mut()).content_services()?,
        };
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
        default_desired_replicas: usize,
    ) -> Result<
        (
            DfsResolvedPlacementPolicy,
            Vec<DfsNodeAdvertisement>,
            DfsReplicationPolicy,
        ),
        DfsError,
    > {
        let resolved =
            self.resolved_placement_policy_for_path(path, object_policy, default_desired_replicas)?;
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
        default_desired_replicas: usize,
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
            default_desired_replicas,
            configuration.as_deref(),
        )
    }

    pub(crate) fn managed_write_replication_policy_from_configuration(
        &mut self,
        path: &str,
        object_policy: DfsObjectPolicy,
        default_desired_replicas: usize,
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
                resolve_placement_policy(&config, path, object_policy, default_desired_replicas)?
            }
            None => self.resolved_placement_policy_for_path(
                path,
                object_policy,
                default_desired_replicas,
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
        default_desired_replicas: usize,
    ) -> Result<(), DfsError> {
        let resolved = self.resolved_placement_policy_for_path(
            &object.path,
            object.policy.clone(),
            default_desired_replicas,
        )?;
        object.policy = resolved.policy.consistency;
        // New creates are already staged in the private namespace projection.
        // Distributed preparation reads that same projection by object ID;
        // retain the managed consistency before it captures the durable body.
        if self
            .transient_metadata_object_ids
            .contains(&object.object_id)
            && self
                .metadata
                .object(&object.object_id)
                .is_some_and(|staged| staged.policy != object.policy)
        {
            self.add_transient_object_metadata(object.clone())?;
        }
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
                && placement_config_remains_authoritative_for_job(
                    &job.accepted_config,
                    &config,
                    &job,
                )
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
            &plan.accepted_config,
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
            if self.restore_pending_write_for_repair_job(repair)?
                == crate::service_commit::RepairProducerCustodyReadiness::Parked
            {
                continue;
            }
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
        validate_local_placement_owner(&object, &object_policy)?;
        if matches!(
            object_policy,
            DfsObjectPolicy::LocalOwned | DfsObjectPolicy::EphemeralLocal
        ) {
            // The exact current job/plan and local metadata authority were
            // checked above. A local-policy scan is observational: keep the
            // owner's copy and any existing read caches/pins, and never enter
            // generic replica creation or pruning. Missing owner knowledge is
            // not authority to substitute another node.
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
                let durability = version
                    .content_durability
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
        let mut intent = DistributedWriteIntent {
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
        if let Some(recorded) = self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.get_intent(object_id, &intent_id))
            .transpose()?
            .flatten()
        {
            let original = crate::intent::distributed_intent_from_record(&recorded);
            let mut current_owner = original.clone();
            current_owner.owner_epoch = intent.owner_epoch;
            current_owner.requesting_node_id = intent.requesting_node_id.clone();
            if original.owner_epoch > intent.owner_epoch
                || (original.owner_epoch == intent.owner_epoch
                    && original.requesting_node_id != intent.requesting_node_id)
                || current_owner != intent
                || recorded.content_hash.as_deref() != Some(content_hash.as_str())
            {
                return Err(DfsError::Metadata {
                    message: "placement replay differs from its original committed obligation"
                        .into(),
                });
            }
            // The current job/plan and actual local primary were validated
            // above. A handoff moves execution authority, not the immutable
            // identity of the already committed placement obligation.
            intent = original;
        }
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
            let mut retirement_cursor = None;
            loop {
                let (retired, next) = self.publish_superseded_placement_intent_receipts(
                    &object,
                    &intent_id,
                    retirement_cursor.as_ref(),
                    128,
                )?;
                if !retired.is_empty() {
                    self.record_message_committed_work(DfsMessageCommittedWork {
                        publish_metadata: true,
                        repair_maintenance: true,
                        ..Default::default()
                    });
                }
                for old_id in retired {
                    self.retire_committed_repair(&old_id);
                }
                retirement_cursor = next;
                if retirement_cursor.is_none() {
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
        if let Some(execution) = self.publication_execution.current() {
            // A handoff replay keeps the original committed metadata, so it
            // does not cross the new-commit enrollment callback. The current
            // primary still needs its first local producer before handing
            // derived work to the queue. Reuse that same enrollment: existing
            // named/unknown custody and retry backoff are never replaced.
            let now = dfs_now_unix_nanos();
            let births = desired_replica_node_ids
                .iter()
                .filter(|target| {
                    self.coordinator
                        .repair_job_for_target(&intent_id, target)
                        .is_some()
                })
                .map(|target| crate::DfsRepairAttemptRecord {
                    intent_id: intent_id.clone(),
                    object_id: object_id.clone(),
                    version: expected_version.clone(),
                    content_hash: content_hash.clone(),
                    missing_replica_node_id: target.clone(),
                    attempts: 0,
                    in_flight: false,
                    last_attempt_unix_nanos: 0,
                    next_retry_unix_nanos: now,
                    transfer_request_id: None,
                    last_error: None,
                    transfer_delivery_state: Some(
                        sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                    ),
                    completed_transfer_request_id: None,
                    transfer_kind: Some(
                        sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair,
                    ),
                })
                .collect::<Vec<_>>();
            crate::service_repair::enroll_prepared_repair_targets(
                &sinomo_core::local::SinomoDataDir::new(execution.data_root()),
                &self.repair_attempt_records,
                &self.writes.pending_writes,
                &births,
            )?;
        }
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
        let job = crate::control_plane::build_adaptive_placement_reconciliation_job(
            job_id.clone(),
            config,
            "/".to_string(),
            DfsPlacementScope::Subtree,
            BTreeSet::new(),
            candidate_snapshot,
            (*snapshot).clone(),
            budget,
            0,
            now_nanos,
        )?;
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
        if !placement_config_remains_authoritative_for_job(
            &expected.accepted_config,
            &current_config,
            &expected,
        ) {
            mark_placement_reconciliation_superseded(&mut desired);
            desired.revision = desired.revision.saturating_add(1);
            desired.updated_at_unix_nanos = dfs_now_unix_nanos();
            let updated = {
                let Some(control_plane) = self.control_plane.as_mut() else {
                    return Ok(None);
                };
                DfsControlPlaneStore::new(control_plane.as_mut())
                    .compare_and_put_placement_reconciliation_with_guards(
                        &expected,
                        &desired,
                        placement_supersession_config_compares(&current_config)?,
                    )?
            };
            return Ok(updated.then_some(desired));
        }
        let config = expected.accepted_config.clone();
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
        let superseded_during_page = !placement_config_remains_authoritative_for_job(
            &expected.accepted_config,
            &post_page_config,
            &expected,
        );
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
                        placement_supersession_config_compares(&post_page_config)?
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

// Supersession classifies against this captured configuration. Keep that same
// configuration in the existing progress CAS so an older read cannot retire a
// job published by a concurrent Apply. Changed configuration owns a fresh pass.
fn placement_supersession_config_compares(
    config: &DfsFileSystemConfig,
) -> Result<Vec<KvCompare>, DfsError> {
    Ok(vec![KvCompare::ValueEquals {
        key: crate::file_system_config_key(&config.file_system_id),
        value: sinomo_core::kv::encode_kv_value(config)?,
    }])
}

fn mark_placement_reconciliation_superseded(job: &mut DfsPlacementReconciliationJob) {
    // The current terminal status plus diagnostic distinguishes supersession
    // from an execution failure in placement activity.
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

/// Exact Current placement DATA identity, not authority or transfer evidence.
pub(crate) fn current_placement_repair_owner_matches_identity(
    owner: &crate::DfsWriteIntentRecord,
) -> bool {
    owner.publication.is_current()
        && owner.status == crate::DfsWriteIntentStatus::Committed
        && owner.base_version.as_ref() == Some(&owner.target_version)
        && owner.operation_id.is_none()
        && placement_reconciliation_job_id_from_intent_id(&owner.intent_id).is_some_and(|job| {
            owner.intent_id
                == format!(
                    "placement:{job}:{}:{}",
                    owner.object_id, owner.target_version
                )
        })
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
        let accepted_config = job.accepted_config.clone();
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

    /// sinomo:kv-configuration: Read the existing native host/process identity; this performs no KV request and grants no placement permit.
    fn current_placement_process_identity(
        &self,
        job_id: &str,
    ) -> Result<crate::DfsPlacementAuthorityProcessIdentity, DfsError> {
        self.control_plane
            .as_ref()
            .and_then(|control| control.placement_authority_process_identity())
            .ok_or_else(|| DfsError::Distributed {
                message: format!(
                    "cannot acquire placement reconciliation job {job_id}: this control-plane host has no daemon placement-authority session"
                ),
            })
    }

    /// sinomo:kv-consistency: Fence an existing placement reconciliation against concurrent policy Apply before granting its mutation permit.
    pub(crate) fn acquire_authoritative_placement_reconciliation(
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
        #[cfg(test)]
        let mut observation =
            self.native_publication_owner_for_observation()
                .and_then(|(root, owner)| {
                    crate::test_runtime::observe_placement_authority_call(
                        &root, &owner, job_id, path,
                    )
                });
        for attempt in 0..PLACEMENT_AUTHORITY_ACQUIRE_RETRIES {
            let Some((job, accepted_config, current_config)) =
                self.authoritative_placement_reconciliation_for_path(file_system_id, job_id, path)?
            else {
                return Ok(None);
            };
            let holder_process = self.current_placement_process_identity(job_id)?;
            let permit = {
                let Some(control_plane) = self.control_plane.as_mut() else {
                    return Err(DfsError::Distributed {
                        message: format!(
                            "cannot acquire placement reconciliation job {job_id}: control plane unavailable"
                        ),
                    });
                };
                DfsControlPlaneStore::new(control_plane.as_mut())
                    .try_acquire_placement_authority_permit(
                        &job,
                        &current_config,
                        &self.node_id,
                        &holder_process,
                    )?
            };
            if let Some(permit) = permit {
                #[cfg(test)]
                let permit = permit.with_native_observation(observation.take(), job.state.clone());
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
            #[cfg(test)]
            permit.observe_native_release(result.is_ok());
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
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Ok(PlacementIntentAuthority::Stale);
        };
        if matches!(
            intent.state,
            DfsWriteIntentState::Rejected | DfsWriteIntentState::Aborted
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
    /// rewritten as Aborted; its execution is withdrawn and actual producer
    /// DATA is parked. Stale plan eligibility is not terminal repair evidence.
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
        self.park_committed_repairs(&committed);
        Ok(())
    }

    /// Validate an intent against the exact job/configuration cut. Eligibility
    /// checks and permit-held effects share these payload/pin rules; this
    /// comparison alone never grants mutation authority.
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

    /// Check committed repair eligibility against the existing job/config view.
    /// This boolean lends no mutation authority: receipt/effect paths retain
    /// their own permit. An already-released permit cannot fence later work.
    /// Uncommitted intents keep the original permit-held retirement path.
    pub(crate) fn placement_intent_remains_authoritative(
        &mut self,
        intent_id: &str,
    ) -> Result<bool, DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Ok(false);
        };
        if intent.state != DfsWriteIntentState::Committed {
            return match self.placement_intent_authority(intent_id)? {
                PlacementIntentAuthority::Ordinary => Ok(true),
                PlacementIntentAuthority::Stale => Ok(false),
                PlacementIntentAuthority::Managed(mut permit) => {
                    self.release_placement_authority_permit(&mut permit)?;
                    Ok(true)
                }
            };
        }
        let Some(job_id) = placement_reconciliation_job_id_from_intent_id(intent_id) else {
            if intent_id.starts_with("placement:") {
                return Err(DfsError::Distributed {
                    message: format!("malformed reserved placement intent id: {intent_id}"),
                });
            }
            return Ok(true);
        };
        let object = self.current_read_object_metadata(&intent.object_id)?;
        let context = self.authoritative_placement_reconciliation_for_path(
            &intent.file_system_id,
            job_id,
            Some(&object.path),
        )?;
        let eligible = if let Some((job, config, _)) = context {
            // Retain the native host/process requirement of the permit path;
            // a configuration reader alone cannot become a repair producer.
            self.current_placement_process_identity(job_id)?;
            self.managed_placement_intent_matches_job(&intent, &object, &job, &config)?
        } else {
            false
        };
        if !eligible {
            self.retire_stale_placement_intents(std::slice::from_ref(&intent))?;
        }
        Ok(eligible)
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
                // Validate complete row identity and ambiguity using the same
                // bounded response validator, then revalidate live assignment.
                for (object, shard) in
                    matching_placement_noop_rows(batch, response.synced_path_objects)
                {
                    if self.select_metadata_shard_for_object(&object)?.as_ref() == Some(&shard) {
                        proofs
                            .objects
                            .insert(object.object_id.clone(), (object, shard));
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
        admitted_metadata_shards: &[crate::DfsMetadataShardConfig],
    ) -> Result<Option<DfsPlacementReconciliationJob>, DfsError> {
        let Some(root) =
            crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                &crate::sifs_file_system_id(),
                admitted_metadata_shards,
                "/",
            )?
        else {
            // Missing admitted routing is deferred work, never placement permission.
            return Ok(None);
        };
        if root.primary_node_id != self.node_id {
            // Reuse the caller's admitted configuration before entering the
            // synchronous current-read path for work owned by another Main.
            return Ok(None);
        }
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
        if !placement_config_remains_authoritative_for_job(
            &expected.accepted_config,
            &current_config,
            &expected,
        ) {
            mark_placement_reconciliation_superseded(&mut desired);
            desired.revision = desired.revision.saturating_add(1);
            desired.updated_at_unix_nanos = dfs_now_unix_nanos();
            let updated = {
                let Some(control_plane) = self.control_plane.as_mut() else {
                    return Ok(None);
                };
                DfsControlPlaneStore::new(control_plane.as_mut())
                    .compare_and_put_placement_reconciliation_with_guards(
                        &expected,
                        &desired,
                        placement_supersession_config_compares(&current_config)?,
                    )?
            };
            return Ok(updated.then_some(desired));
        }
        let config = expected.accepted_config.clone();
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
        let superseded_during_page = !placement_config_remains_authoritative_for_job(
            &expected.accepted_config,
            &post_page_config,
            &expected,
        );
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
                placement_supersession_config_compares(&post_page_config)?
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
        DfsMetadataShardConfig, DfsNodeTransport, DfsRepairReplicaResponse, DfsRoutedCall,
        DfsRoutedMessage, DfsTransportIdentity, DfsWriteIntentStatus, LocalContentStore,
        NetworkedDfsServiceEvent, SharedDfsControlPlaneKvClient, open_root_local_metadata_backend,
        sifs_file_system_id,
    };
    use sinomo_core::kv::InProcessKvStore;
    use sinomo_core::network::NetworkError;

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

    fn native_placement_fixture(
        name: &str,
        count: usize,
    ) -> (
        crate::test_support::DfsTestRuntime,
        NetworkedDfsService<crate::DfsNodeTransport>,
        BTreeMap<&'static str, NodeId>,
    ) {
        let (runtime, service, config) =
            crate::test_support::current_native_metadata_service(name, count, "/services")
                .expect("normal mounted native metadata owner");
        let mut ordered = vec![config.primary_node_id.clone()];
        ordered.extend(
            crate::test_support::DfsTestRuntime::ordered_node_ids(name, count)
                .into_iter()
                .filter(|node| *node != config.primary_node_id),
        );
        let nodes = ["node-a", "node-b", "node-c", "node-d"]
            .into_iter()
            .zip(ordered)
            .collect();
        (runtime, service, nodes)
    }

    fn native_placement_objects(
        service: &mut NetworkedDfsService<crate::DfsNodeTransport>,
        objects: Vec<DfsObjectMetadata>,
    ) -> Vec<DfsObjectMetadata> {
        objects
            .into_iter()
            .map(|planned| {
                let stat = if planned.kind == DfsObjectKind::Directory {
                    service
                        .posix_mkdir_all(&planned.path)
                        .expect("normal current directory producer");
                    service
                        .posix_stat(&planned.path)
                        .expect("actual committed directory stat")
                } else {
                    assert_eq!(planned.kind, DfsObjectKind::File);
                    if let Some((parent, _)) = planned.path.rsplit_once('/') {
                        service
                            .posix_mkdir_all(if parent.is_empty() { "/" } else { parent })
                            .expect("normal parent directory producer before file content");
                    }
                    let candidates = planned.current_locations.clone();
                    let count = candidates.len();
                    assert!(count > 0);
                    // Seed actual bytes and actual W holders; the old synthetic
                    // hash/identity fields are never installed as durable rows.
                    service
                        .posix_write_file(
                            &planned.path,
                            &vec![
                                1;
                                usize::try_from(planned.content_size_bytes.unwrap_or(0)).unwrap()
                            ],
                            crate::DfsWriteOptions {
                                policy: DfsReplicationPolicy::new(count, count).unwrap(),
                                replica_candidates: candidates,
                                wait_for_commit: true,
                                foreground_replica_repair: true,
                            },
                        )
                        .expect("normal current byte/version/receipt producer")
                };
                service
                    .metadata_backend()
                    .unwrap()
                    .get_object(&stat.object_id)
                    .unwrap()
                    .expect("actual committed metadata row")
            })
            .collect()
    }

    fn placement_config(owner_node_id: NodeId) -> DfsFileSystemConfig {
        DfsFileSystemConfig {
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
            accepted_config: placement_config(node("node-a")),
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
        let subtree =
            DfsSubtreePolicy::new("/users/team", DfsObjectPolicy::ReplicatedEventual, 1, 2);
        let self_only = DfsSubtreePolicy {
            path_prefix: "/users/team".to_string(),
            policy: DfsObjectPolicy::ReplicatedConfirmed,
            scope: DfsPlacementScope::SelfOnly,
            min_commit_replicas: 2,
            desired_replicas: 2,
            required_node_ids: Vec::new(),
            forbidden_node_ids: Vec::new(),
            min_distinct_zones: 0,
        };
        let services =
            DfsSubtreePolicy::new("/services", DfsObjectPolicy::ReplicatedEventual, 1, 2);
        let first = DfsFileSystem {
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
        let eventual =
            DfsSubtreePolicy::new("/users/team", DfsObjectPolicy::ReplicatedEventual, 1, 2);
        let confirmed =
            DfsSubtreePolicy::new("/users/team", DfsObjectPolicy::ReplicatedConfirmed, 2, 2);
        let first = DfsFileSystem {
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
            min_commit_replicas: 1,
            desired_replicas: 3,
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
            placement_policy_from_rule(&first_rule)
                .expect("first policy")
                .replication_policy(BTreeMap::new())
                .expect("first replication policy"),
            placement_policy_from_rule(&reordered_rule)
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
            retention_policies: Default::default(),
            module_metadata: Default::default(),
            file_system_id: sifs_file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id: owner.clone(),
            subtree_policies: vec![DfsSubtreePolicy::new(
                "/users/team",
                DfsObjectPolicy::ReplicatedConfirmed,
                2,
                2,
            )],
        };
        let aliased = DfsFileSystem {
            subtree_policies: vec![DfsSubtreePolicy::new(
                "/users//team",
                DfsObjectPolicy::ReplicatedConfirmed,
                2,
                2,
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
    fn placement_reconciliation_is_idle_before_filesystem_config_publication() {
        let owner = node("node-a");
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        let root = sinomo_testdir::TestDir::new("dfs-placement-before-config-publication");
        let content = LocalContentStore::new(root.join("content")).expect("content store");
        let mut service = NetworkedDfsService::new(
            crate::test_runtime::DfsTestRuntime::local_transport(owner),
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
            crate::test_runtime::DfsTestRuntime::local_transport(owner),
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
        changed_policy.subtree_policies[0].min_commit_replicas = 2;
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
        let (_runtime, mut service, nodes) =
            native_placement_fixture("preview-descendant-plan-fence", 4);
        let node = |label: &str| nodes[label].clone();
        let advertisement = |label: &str, zone: &str| {
            let mut candidate = self::advertisement(label, zone);
            candidate.node_id = node(label);
            candidate
        };
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
            normalize_placement_rule("/services/team", DfsPlacementScope::SelfOnly, root_policy)
                .unwrap(),
        );
        let mut subtree_policy =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
        subtree_policy.forbidden_node_ids = vec![node("node-b")];
        upsert_placement_rule(
            &mut config.subtree_policies,
            normalize_placement_rule("/services/team", DfsPlacementScope::Subtree, subtree_policy)
                .unwrap(),
        );
        let directory = placement_object(
            "preview-directory",
            "/services/team",
            DfsObjectKind::Directory,
            owner.clone(),
            vec![owner.clone()],
            None,
        );
        let child = placement_object(
            "preview-child",
            "/services/team/child.bin",
            DfsObjectKind::File,
            owner.clone(),
            vec![owner.clone()],
            Some(10),
        );
        let mut actual = native_placement_objects(&mut service, vec![directory, child]);
        let child = actual.pop().unwrap();
        let directory = actual.pop().unwrap();
        // Scores below are pure planner inputs for token sensitivity only.
        // They are never installed as native observations or job authority.
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
                        "/services/team",
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
                "/services/team",
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
        let mut job = reconciliation_job(
            "/services/team",
            DfsPlacementScope::Subtree,
            BTreeSet::new(),
        );
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
            first.accepted_config = first_config.clone();
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
            second.accepted_config = second_config.clone();
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
            crate::test_runtime::DfsTestRuntime::local_transport(owner),
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
            first.accepted_config = first_config.clone();
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
            unrelated.accepted_config = unrelated_config.clone();
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
            crate::test_runtime::DfsTestRuntime::local_transport(owner),
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
        assert_eq!(local_rule.desired_replicas, 1);
        assert_eq!(local_rule.min_commit_replicas, 1);

        let mut job = reconciliation_job("/", DfsPlacementScope::Subtree, BTreeSet::new());
        job.adaptive = true;
        job.adaptive_budget = Some(DfsAdaptivePlacementBudget::default());
        job.accepted_config = config.clone();
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

    #[test]
    fn placement_local_rule_defaults_preserve_explicit_validation_and_replicated_defaults() {
        for consistency in [DfsObjectPolicy::LocalOwned, DfsObjectPolicy::EphemeralLocal] {
            let current = DfsSubtreePolicy::new("/local", consistency.clone(), 1, 1);
            let resolved = placement_policy_from_rule(&current).expect("explicit local counts");
            assert_eq!(resolved.consistency, consistency);
            assert_eq!(
                (resolved.min_commit_replicas, resolved.desired_replicas),
                (1, 1)
            );

            let mut explicit = current.clone();
            explicit.min_commit_replicas = 1;
            explicit.desired_replicas = 1;
            assert_eq!(placement_policy_from_rule(&explicit).unwrap(), resolved);

            let mut invalid = Vec::new();
            for count in [0, 2] {
                let mut rule = current.clone();
                rule.desired_replicas = count;
                invalid.push(rule);
                let mut rule = current.clone();
                rule.min_commit_replicas = count;
                invalid.push(rule);
            }
            let mut required = current.clone();
            required.required_node_ids.push(node("node-a"));
            invalid.push(required);
            let mut forbidden = current.clone();
            forbidden.forbidden_node_ids.push(node("node-b"));
            invalid.push(forbidden);
            let mut zoned = current;
            zoned.min_distinct_zones = 1;
            invalid.push(zoned);
            for rule in invalid {
                assert!(matches!(
                    placement_policy_from_rule(&rule),
                    Err(DfsDistributedError::InvalidPolicy { .. })
                ));
            }
        }
        for (consistency, expected_w) in [
            (DfsObjectPolicy::ReplicatedEventual, 1),
            (DfsObjectPolicy::ReplicatedConfirmed, 2),
        ] {
            let rule = DfsSubtreePolicy::new("/replicated", consistency, expected_w, 2);
            let resolved = placement_policy_from_rule(&rule).expect("explicit replicated counts");
            assert_eq!(
                (resolved.min_commit_replicas, resolved.desired_replicas),
                (expected_w, 2)
            );
        }
    }

    #[test]
    fn structurally_invalid_reconciliation_policy_is_rejected_before_publication() {
        let (_runtime, mut service, _) =
            native_placement_fixture("native-invalid-placement-policy", 1);
        let base = DfsControlPlaneStore::new(service.control_plane_client_mut().unwrap())
            .get_file_system_config(&sifs_file_system_id())
            .unwrap()
            .unwrap();
        let candidates = service.placement_content_candidates().unwrap();
        let mut invalid = base.clone();
        invalid.subtree_policies.push(DfsSubtreePolicy {
            path_prefix: "/services/team".to_string(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            scope: DfsPlacementScope::Subtree,
            min_commit_replicas: 2,
            desired_replicas: 1,
            required_node_ids: Vec::new(),
            forbidden_node_ids: Vec::new(),
            min_distinct_zones: 0,
        });
        let job_id = "native-rejected-invalid-placement";
        assert!(
            crate::control_plane::build_placement_reconciliation_job(
                job_id.into(),
                &invalid,
                "/services/team".into(),
                DfsPlacementScope::Subtree,
                BTreeSet::new(),
                candidates,
                None,
                crate::dfs_now_unix_nanos(),
            )
            .is_err(),
            "the ordinary builder must reject the invalid policy before any job can be published"
        );
        let mut store = DfsControlPlaneStore::new(service.control_plane_client_mut().unwrap());
        assert_eq!(
            store.get_file_system_config(&base.file_system_id).unwrap(),
            Some(base.clone())
        );
        assert!(
            store
                .placement_reconciliation(&base.file_system_id, job_id)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn aggregate_impact_counts_scope_bytes_and_replica_deltas() {
        let (_runtime, mut service, nodes) =
            native_placement_fixture("aggregate_impact_counts_scope_bytes_and_replica_deltas", 2);
        let node = |label: &str| nodes[label].clone();
        let advertisement = |label: &str, zone: &str| {
            let mut candidate = self::advertisement(label, zone);
            candidate.node_id = node(label);
            candidate
        };
        let owner = node("node-a");
        native_placement_objects(
            &mut service,
            vec![
                placement_object(
                    "known",
                    "/services/team/known.bin",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a")],
                    Some(10),
                ),
                placement_object(
                    "unknown",
                    "/services/team/unknown.bin",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a"), node("node-b")],
                    None,
                ),
                placement_object(
                    "directory",
                    "/services/team/subdir",
                    DfsObjectKind::Directory,
                    owner.clone(),
                    Vec::new(),
                    None,
                ),
                placement_object(
                    "outside",
                    "/services/elsewhere/file.bin",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a")],
                    Some(99),
                ),
            ],
        );
        let config = placement_config(owner);
        let candidates = vec![
            advertisement("node-a", "east"),
            advertisement("node-b", "west"),
        ];

        let subtree = service
            .placement_impact_for_config(
                &config,
                "/services/team",
                DfsPlacementScope::Subtree,
                &candidates,
            )
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
                "/services/team/known.bin",
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
        let (_runtime, mut service, nodes) = native_placement_fixture(
            "aggregate_impact_excludes_objects_preserved_by_nested_override",
            2,
        );
        let node = |label: &str| nodes[label].clone();
        let advertisement = |label: &str, zone: &str| {
            let mut candidate = self::advertisement(label, zone);
            candidate.node_id = node(label);
            candidate
        };
        let owner = node("node-a");
        native_placement_objects(
            &mut service,
            vec![
                placement_object(
                    "changed",
                    "/services/team/changed.bin",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a")],
                    Some(10),
                ),
                placement_object(
                    "nested-stable",
                    "/services/team/nested/stable.bin",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a")],
                    Some(20),
                ),
            ],
        );
        let mut config = placement_config(owner);
        for (path, replicas) in [("/services/team", 2), ("/services/team/nested", 1)] {
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
                "/services/team",
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
        let (_runtime, mut service, nodes) = native_placement_fixture(
            "aggregate_impact_reports_nested_unsatisfiable_override_and_is_incomplete",
            2,
        );
        let node = |label: &str| nodes[label].clone();
        let advertisement = |label: &str, zone: &str| {
            let mut candidate = self::advertisement(label, zone);
            candidate.node_id = node(label);
            candidate
        };
        let owner = node("node-a");
        native_placement_objects(
            &mut service,
            vec![
                placement_object(
                    "parent-valid",
                    "/services/team/valid.bin",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a")],
                    Some(10),
                ),
                placement_object(
                    "punctuation-sibling",
                    "/services/team-other",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a")],
                    Some(99),
                ),
                placement_object(
                    "nested-invalid",
                    "/services/team/nested/invalid.bin",
                    DfsObjectKind::File,
                    owner.clone(),
                    vec![node("node-a")],
                    Some(20),
                ),
            ],
        );
        let mut config = placement_config(owner);
        for (path, replicas) in [("/services/team", 1), ("/services/team/nested", 3)] {
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
                "/services/team",
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
                } if path == "/services/team/nested" && matches!(
                    diagnostic.as_ref(),
                    DfsPlacementBlockingDiagnostic::InsufficientCandidates {
                        available: 2,
                        required: 3,
                    }
                )
            )
        }));

        let whole = service
            .placement_impact_for_config(
                &config,
                "/services/team",
                DfsPlacementScope::Subtree,
                &candidates,
            )
            .expect("whole impact");
        assert!(!whole.complete);
    }

    #[test]
    fn aggregate_impact_pages_past_the_first_metadata_chunk() {
        let (runtime, service, nodes) =
            native_placement_fixture("aggregate_impact_pages_past_the_first_metadata_chunk", 1);
        let node = |label: &str| nodes[label].clone();
        let advertisement = |label: &str, zone: &str| {
            let mut candidate = self::advertisement(label, zone);
            candidate.node_id = node(label);
            candidate
        };
        let owner = node("node-a");
        drop(service);
        let mut service = runtime.metadata_primary_service("/services/paged").unwrap();

        let objects = (0..(PLACEMENT_IMPACT_PAGE_SIZE + 17))
            .map(|index| {
                placement_object(
                    &format!("paged-{index:04}"),
                    &format!("/services/paged/item-{index:04}"),
                    DfsObjectKind::Directory,
                    owner.clone(),
                    Vec::new(),
                    None,
                )
            })
            .collect::<Vec<_>>();
        let objects = native_placement_objects(&mut service, objects);
        assert_eq!(objects.len(), PLACEMENT_IMPACT_PAGE_SIZE + 17);
        let parent = service.posix_stat("/services/paged").unwrap();
        assert_eq!(parent.kind, DfsObjectKind::Directory);
        assert!(objects.iter().all(|object| object.path != parent.path));
        let mut config = placement_config(owner);
        upsert_placement_rule(
            &mut config.subtree_policies,
            normalize_placement_rule(
                "/services/paged",
                DfsPlacementScope::Subtree,
                DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 1, 1)
                    .expect("confirmed policy"),
            )
            .expect("paged rule"),
        );

        let impact = service
            .placement_impact_for_config(
                &config,
                "/services/paged",
                DfsPlacementScope::Subtree,
                &[advertisement("node-a", "east")],
            )
            .expect("paged impact");
        assert_eq!(
            impact.affected_object_count,
            objects.len() as u64 + 1,
            "the subtree includes all 273 children and their actual native parent"
        );
        assert_eq!(impact.affected_bytes, 0);
        assert_eq!(impact.copies_added, 0);

        let first_page = service
            .placement_impact_page_for_config(
                &config,
                "/services/paged",
                DfsPlacementScope::Subtree,
                &[advertisement("node-a", "east")],
                None,
                31,
            )
            .expect("bounded page");
        let raw_first = service
            .placement_object_page_for_file_system(&config.file_system_id, None, 31)
            .unwrap();
        assert_eq!(
            raw_first.objects.len(),
            31,
            "unchanged metadata page budget"
        );
        let outside = raw_first
            .objects
            .iter()
            .filter(|object| {
                object.path != "/services/paged" && !object.path.starts_with("/services/paged/")
            })
            .map(|object| object.path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            outside,
            vec!["/services"],
            "actual bootstrap parent is outside the proposed subtree"
        );
        assert_eq!(first_page.impact.affected_object_count, 30);
        assert!(first_page.next_cursor.is_some());
        assert!(first_page.blocking_diagnostics.is_empty());
        let mut paged_count = first_page.impact.affected_object_count;
        let mut cursor = first_page.next_cursor;
        while let Some(after) = cursor {
            let page = service
                .placement_impact_page_for_config(
                    &config,
                    "/services/paged",
                    DfsPlacementScope::Subtree,
                    &[advertisement("node-a", "east")],
                    Some(&after),
                    31,
                )
                .unwrap();
            assert!(page.impact.affected_object_count <= 31);
            assert!(page.blocking_diagnostics.is_empty());
            paged_count += page.impact.affected_object_count;
            cursor = page.next_cursor;
        }
        assert_eq!(
            paged_count, impact.affected_object_count,
            "every original child and its real parent is covered exactly once"
        );
        drop(service);
        assert_eq!(runtime.observation_dropped_count(), 0);
        runtime.close().unwrap();
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
}

#[cfg(test)]
mod admitted_placement_coordinator_native_tests {
    use super::*;
    use crate::DfsControlPlaneKvClient;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug)]
    struct ObservedNativeReads {
        inner: Box<dyn DfsControlPlaneKvClient>,
        reads: Arc<AtomicUsize>,
    }
    impl DfsControlPlaneKvClient for ObservedNativeReads {
        fn async_client(&mut self) -> Option<&mut dyn crate::AsyncDfsControlPlaneKvClient> {
            self.inner.async_client()
        }
        fn write_configuration_readiness(
            &self,
        ) -> Result<crate::DfsWriteConfigurationReadiness, sinomo_core::kv::KvError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.write_configuration_readiness()
        }
        fn write_configuration_projection(
            &self,
        ) -> Result<Option<Arc<crate::DfsWriteConfigurationProjection>>, sinomo_core::kv::KvError>
        {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.write_configuration_projection()
        }
        fn refresh_latest(&mut self) -> Result<(), sinomo_core::kv::KvError> {
            self.inner.refresh_latest()
        }
        fn get(
            &self,
            key: &str,
        ) -> Result<Option<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.get(key)
        }
        fn range_prefix(
            &self,
            prefix: &str,
        ) -> Result<Vec<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.range_prefix(prefix)
        }
        fn get_consistent(
            &self,
            keys: &[String],
        ) -> Result<Vec<Option<sinomo_core::kv::KvEntry>>, sinomo_core::kv::KvError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.get_consistent(keys)
        }
        fn get_authoritative_consistent(
            &mut self,
            keys: &[String],
        ) -> Result<Vec<Option<sinomo_core::kv::KvEntry>>, sinomo_core::kv::KvError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.get_authoritative_consistent(keys)
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
        fn txn_with_native_authority(
            &mut self,
            txn: sinomo_core::kv::KvTxn,
        ) -> Result<
            (
                sinomo_core::kv::KvTxnResponse,
                Option<crate::DfsNativeTxnCommit>,
            ),
            sinomo_core::kv::KvError,
        > {
            self.inner.txn_with_native_authority(txn)
        }
    }

    #[test]
    fn actual_nonowner_placement_scheduling_uses_admitted_routing_without_control_reads() {
        let (runtime, mut owner, root_shard) =
            crate::test_support::current_native_metadata_service(
                "native-placement-admitted-role",
                2,
                "/",
            )
            .unwrap();
        let shards =
            crate::DfsControlPlaneStore::new(owner.control_plane.as_mut().unwrap().as_mut())
                .list_metadata_shards(&crate::sifs_file_system_id())
                .unwrap();
        let foreign = root_shard.replica_node_ids[0].clone();
        assert_ne!(foreign, root_shard.primary_node_id);
        let mut observer = runtime.client(&foreign).unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let inner = observer.control_plane.take().unwrap();
        observer.control_plane = Some(Box::new(ObservedNativeReads {
            inner,
            reads: Arc::clone(&reads),
        }));
        let context = runtime.component_context(&foreign).unwrap();
        let bound =
            crate::DfsMaintenanceTickOptions::default().placement_reconciliation_max_objects;
        assert!(
            context
                .runtime()
                .unwrap()
                .block_on(
                    observer.run_placement_reconciliation_bounded_async_with_pending_repairs(
                        bound, 0, &shards,
                    ),
                )
                .unwrap()
                .is_none()
        );
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "a real nonowner must not re-read configuration under placement admission"
        );
        // Negative DATA: absent routing cannot turn this node into a coordinator.
        assert!(
            context
                .runtime()
                .unwrap()
                .block_on(
                    observer.run_placement_reconciliation_bounded_async_with_pending_repairs(
                        bound,
                        0,
                        &[],
                    ),
                )
                .unwrap()
                .is_none()
        );
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        drop(context);
        drop(observer);
        let path = "/native-placement-owner-positive";
        let stat = owner
            .posix_write_file(
                path,
                b"actual owner bytes",
                crate::DfsWriteOptions::replicated_eventual(vec![
                    root_shard.primary_node_id.clone(),
                ]),
            )
            .unwrap();
        assert_eq!(owner.posix_read_file(path).unwrap(), b"actual owner bytes");
        let durable = owner
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap()
            .unwrap();
        assert_eq!(durable.version, stat.version);
        let outcome = runtime
            .maintenance_tick(
                &root_shard.primary_node_id,
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap()
            .outcome
            .unwrap();
        assert!(matches!(
            outcome,
            crate::DfsMaintenanceTickOutcome::Completed { .. }
        ));
        drop(owner);
        runtime.close().unwrap();
    }
}
