//! Durable, explainable DFS placement policy and reconciliation records.

use crate::control_plane::DfsPlacementAuthorityPermit;
use crate::distributed::explain_replicas_best_effort;
use crate::posix_path::normalize_posix_path;
use crate::{
    AsyncDfsControlPlaneKvClient, DfsControlPlaneKvClient, DfsControlPlaneStore,
    DfsDistributedError, DfsError, DfsFileSystem, DfsFileSystemConfig, DfsFileSystemId,
    DfsMessageTransport, DfsNamespaceOperation, DfsNodeAdvertisement, DfsObjectId, DfsObjectKind,
    DfsObjectMetadata, DfsObjectPage, DfsObjectPageCursor, DfsPlacementScope, DfsReplicaCandidate,
    DfsReplicaPlacementExplanation, DfsReplicaPlacementRequest, DfsReplicationPolicy, DfsStat,
    DfsSubtreePolicy, DfsVersion, DfsWriteIntentState, DistributedWriteIntent, NetworkedDfsService,
    dfs_now_unix_nanos, explain_replicas, explain_replicas_with_placement_snapshot,
    validate_replication_plan,
};
use serde::{Deserialize, Serialize};
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

enum PlacementIntentAuthority {
    Ordinary,
    Managed(DfsPlacementAuthorityPermit),
    Stale,
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
            && left.file_history == right.file_history
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
            && left.file_history == right.file_history
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
    let desired_replicas = rule
        .desired_replicas
        .unwrap_or_else(|| legacy_desired_replicas.clamp(1, 2));
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
    preview: DfsPlacementPreview,
}

/// Seal a fully enriched preview to the exact inputs and metadata snapshot the
/// operator reviewed. Call this only after aggregate impact, final
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
    let mut preview = preview.clone();
    preview.preview_token = DfsPlacementPreviewToken::default();
    let material = DfsPlacementPreviewTokenMaterial {
        domain: "sinomo-dfs-placement-preview-token-v1",
        base_config,
        desired_config,
        path,
        scope,
        object,
        candidates,
        unavailable_node_ids,
        preview,
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
    desired_replica_node_ids: Vec<NodeId>,
    blocking_diagnostics: Vec<DfsPlacementBlockingDiagnostic>,
}

fn analyze_object_placement(
    config: &DfsFileSystemConfig,
    object: &DfsObjectMetadata,
    candidates: &[DfsNodeAdvertisement],
    unavailable_node_ids: &BTreeSet<NodeId>,
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
    let explanation = if blocking_diagnostics.is_empty() {
        explain_replicas(&request)?
    } else {
        explain_replicas_best_effort(&request)?
    };
    Ok(ObjectPlacementAnalysis {
        resolved,
        desired_replica_node_ids: explanation.plan.desired_replica_nodes,
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

            let object_digest = placement_metadata_object_digest(&object)?;
            for (aggregate, byte) in metadata_snapshot_xor.iter_mut().zip(object_digest) {
                *aggregate ^= byte;
            }
            metadata_snapshot_has_objects = true;

            let analysis =
                analyze_object_placement(config, &object, candidates, unavailable_node_ids)
                    .map_err(DfsError::from)?;
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
                    .desired_replica_node_ids
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
            path,
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
                )
            },
        )?;
        Ok(())
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
        if object.policy != object_policy {
            object.policy = object_policy;
            self.add_object_metadata(object.clone())?;
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
        if desired.is_subset(&current) {
            for node_id in current.difference(&desired).cloned().collect::<Vec<_>>() {
                // A content pin is an additive retention constraint. Adaptive
                // policy may choose a different W/N set, but it must never use
                // metadata cleanup to revoke a replica an operator pinned.
                if self
                    .content_pin_node_ids
                    .get(object_id)
                    .is_some_and(|pinned| pinned.contains(&node_id))
                {
                    continue;
                }
                self.mark_remote_read_location_missing(
                    object_id,
                    expected_version,
                    &Some(content_hash.clone()),
                    &node_id,
                )?;
            }
            return Ok(());
        }

        let intent_id = format!(
            "placement:{}:{}:{}",
            job.job_id,
            object_id.as_str(),
            expected_version.as_str()
        );
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
        self.coordinator.restore_intent(intent)?;
        for node_id in durable_receipts {
            self.coordinator
                .restore_durable_receipt(&intent_id, node_id)?;
        }
        self.restore_pending_write_for_intent_if_missing(&intent_id, object_id)?;
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
            let Some(job) =
                store.next_placement_reconciliation(&file_system_id, &current_config)?
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
        let result =
            self.advance_placement_reconciliation_page(&config, &mut desired, max_objects.max(1));
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
                .compare_and_put_placement_reconciliation(&expected, &desired)?
        };
        Ok(updated.then_some(desired))
    }

    fn advance_placement_reconciliation_page(
        &mut self,
        config: &DfsFileSystemConfig,
        job: &mut DfsPlacementReconciliationJob,
        max_objects: usize,
    ) -> Result<(), DfsError> {
        if job.state == DfsPlacementReconciliationState::Pending {
            job.total_object_count = self
                .metadata_backend
                .as_ref()
                .map(|backend| backend.count_objects_for_file_system(&job.file_system_id))
                .transpose()?
                .unwrap_or_else(|| {
                    self.metadata
                        .objects
                        .values()
                        .filter(|object| object.file_system_id == job.file_system_id)
                        .count()
                });
            job.state = DfsPlacementReconciliationState::Scanning;
            job.cursor = None;
        }
        let adaptive_budget = job.adaptive_budget.clone().filter(|_| job.adaptive);
        let object_limit = adaptive_budget
            .as_ref()
            .map(|budget| max_objects.min(budget.max_objects_per_tick))
            .unwrap_or(max_objects)
            .max(1);
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
            if processed_objects >= object_limit {
                break;
            }
            if processed_objects > 0
                && adaptive_budget.as_ref().is_some_and(|budget| {
                    processed_bytes >= budget.max_bytes_per_tick
                        || started.elapsed() >= Duration::from_millis(budget.max_millis_per_tick)
                })
            {
                break;
            }
            // Adaptive jobs page one object at a time. An object/intent is the
            // smallest crash-safe unit, so an individual oversized or slow
            // object may exceed a byte/time budget once to guarantee progress;
            // no second object starts after that budget is consumed.
            let page_size = if adaptive_budget.is_some() {
                1
            } else {
                object_limit.saturating_sub(processed_objects)
            };
            let page = self.placement_object_page(job, page_size)?;
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
                if job.state == DfsPlacementReconciliationState::Scanning
                    && (object.policy != resolved.policy.consistency
                        || missing_before > 0
                        || obsolete_before > 0)
                {
                    job.updated_object_count = job.updated_object_count.saturating_add(1);
                    job.scheduled_repair_count =
                        job.scheduled_repair_count.saturating_add(missing_before);
                }
                self.route_placement_reconciliation_object(
                    job,
                    object,
                    resolved.policy.consistency,
                    replication,
                    desired_nodes.clone(),
                )?;
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
            if job.cursor.is_none() || page.objects.is_empty() || adaptive_budget.is_none() {
                break;
            }
        }
        if job.cursor.is_none() {
            match job.state {
                DfsPlacementReconciliationState::Scanning => {
                    job.state = DfsPlacementReconciliationState::Repairing;
                    job.pending_repair_count = 0;
                }
                DfsPlacementReconciliationState::Repairing => {
                    job.pending_repair_count = pass_pending_repairs;
                    job.completed_repair_count = job
                        .scheduled_repair_count
                        .saturating_sub(job.pending_repair_count);
                    if job.pending_repair_count == 0 {
                        job.state = DfsPlacementReconciliationState::Completed;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn placement_object_page(
        &self,
        job: &DfsPlacementReconciliationJob,
        max_objects: usize,
    ) -> Result<DfsObjectPage, DfsError> {
        self.placement_object_page_for_file_system(
            &job.file_system_id,
            job.cursor.as_ref(),
            max_objects,
        )
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
    let (replication, replica_candidates, _) =
        placement_inputs(&resolved.policy, candidates, &job.unavailable_node_ids)
            .map_err(placement_reconciliation_policy_error)?;
    let request = DfsReplicaPlacementRequest {
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        target_version: object.version.clone(),
        coordinator_node_id: config.owner_node_id.clone(),
        policy: replication.clone(),
        candidates: replica_candidates,
    };
    let placement = match job.placement_snapshot.as_ref() {
        Some(snapshot) => explain_replicas_with_placement_snapshot(
            &request,
            snapshot,
            &object.current_locations,
            pinned_node_ids,
        )
        .map_err(placement_reconciliation_policy_error)?,
        None => explain_replicas(&request).map_err(placement_reconciliation_policy_error)?,
    };
    Ok((resolved, replication, placement))
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

fn placement_metadata_object_digest(object: &DfsObjectMetadata) -> Result<[u8; 32], DfsError> {
    let bytes = serde_json::to_vec(object).map_err(|error| DfsError::Serialization {
        message: format!("cannot encode placement metadata snapshot: {error}"),
    })?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo-dfs-placement-metadata-object\0v1\0");
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

fn placement_reconciliation_job_id_from_intent_id(intent_id: &str) -> Option<&str> {
    let remainder = intent_id.strip_prefix("placement:")?;
    let (job_id, _) = remainder.split_once(':')?;
    (!job_id.is_empty()).then_some(job_id)
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn authoritative_placement_reconciliation_for_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        job_id: &str,
        path: &str,
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
            || !placement_job_matches(&job, path)
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

    fn with_authoritative_placement_reconciliation<R>(
        &mut self,
        file_system_id: &DfsFileSystemId,
        job_id: &str,
        path: &str,
        action: impl FnOnce(
            &mut Self,
            &DfsPlacementReconciliationJob,
            &DfsFileSystemConfig,
        ) -> Result<R, DfsError>,
    ) -> Result<Option<R>, DfsError> {
        let Some((job, accepted_config, mut permit)) = self
            .acquire_authoritative_placement_reconciliation_for_path(
                file_system_id,
                job_id,
                path,
            )?
        else {
            return Ok(None);
        };
        let action_result = action(self, &job, &accepted_config);
        let release_result = self.release_placement_authority_permit(&mut permit);
        match (action_result, release_result) {
            (Ok(value), Ok(())) => Ok(Some(value)),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
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
        let pinned_node_ids = self.content_pin_nodes(&intent.object_id);
        let path = object.path.clone();
        let mut authority = self.acquire_authoritative_placement_reconciliation_for_path(
            &intent.file_system_id,
            job_id,
            &path,
        )?;
        let authoritative_result = authority
            .as_ref()
            .map(|(job, config, _)| {
                let (_, expected_replication, expected_placement) =
                    reconciliation_placement_for_object_with_pins(
                        config,
                        job,
                        &object,
                        &pinned_node_ids,
                    )?;
                Ok::<_, DfsError>(
                    intent.policy == expected_replication
                        && intent.desired_replica_nodes
                            == expected_placement.plan.desired_replica_nodes,
                )
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

        let content_hash = self
            .writes
            .pending_writes
            .get(intent_id)
            .map(|pending| pending.content_hash.clone())
            .or_else(|| {
                self.current_read_object_metadata(&intent.object_id)
                    .ok()
                    .and_then(|object| object.content_hash)
            });
        // Terminalizing an intent after replacement is safe without the old
        // generation's permit. If the generation is still current but the
        // intent payload is malformed, `held_permit` remains held through this
        // durable abort. Persist it before dropping the in-memory repair state.
        let abort_result = (|| {
            if matches!(intent.state, DfsWriteIntentState::Committed) {
                // Replacing the placement policy retires further repair work;
                // it cannot undo this write's already published outcome.
                self.retire_committed_repair(intent_id);
                return Ok(());
            }
            let mut aborted = intent;
            aborted.state = DfsWriteIntentState::Aborted;
            self.put_metadata_intent(&aborted, content_hash.as_deref())?;
            self.coordinator.restore_intent(aborted)?;
            self.remove_pending_write_state(intent_id);
            Ok::<_, DfsError>(())
        })();
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

    /// Fence delayed receipts and repair retries belonging to a managed
    /// placement job. Placement intent ids already durably encode their job id
    /// (`placement:<job-id>:...`), including across metadata-shard migration,
    /// so this is backward-compatible with existing intent records.
    pub(crate) fn placement_intent_remains_authoritative(
        &mut self,
        intent_id: &str,
    ) -> Result<bool, DfsError> {
        match self.placement_intent_authority(intent_id)? {
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
        file_history: Default::default(),
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
            file_history: Default::default(),
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
            file_history: Default::default(),
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
            file_history: Default::default(),
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
            file_history: Default::default(),
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
            file_history: Default::default(),
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
            file_history: Default::default(),
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
            file_history: Default::default(),
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
