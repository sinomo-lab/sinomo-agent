//! Durable, explainable DFS placement policy and reconciliation records.

use crate::control_plane::DfsPlacementAuthorityPermit;
use crate::distributed::explain_replicas_best_effort;
use crate::posix_path::normalize_posix_path;
use crate::{
    DfsControlPlaneStore, DfsDistributedError, DfsError, DfsFileSystemConfig, DfsFileSystemId,
    DfsMessageTransport, DfsNamespaceOperation, DfsNodeAdvertisement, DfsObjectId, DfsObjectKind,
    DfsObjectMetadata, DfsObjectPage, DfsObjectPageCursor, DfsPlacementScope, DfsReplicaCandidate,
    DfsReplicaPlacementExplanation, DfsReplicaPlacementRequest, DfsReplicationPolicy, DfsStat,
    DfsSubtreePolicy, DfsVersion, DfsWriteIntentState, DistributedWriteIntent, NetworkedDfsService,
    dfs_now_unix_nanos, explain_replicas, validate_replication_plan,
};
use serde::{Deserialize, Serialize};
use sinomo_core::node::{
    DFS_PLACEMENT_RECONCILIATION_V1_CAPABILITY, NodeId, cluster_member_is_legacy_shadow_daemon,
};
use sinomo_core::{ClusterBootstrapState, DfsObjectPolicy};
use std::collections::{BTreeMap, BTreeSet};

const PLACEMENT_IMPACT_PAGE_SIZE: usize = 256;
const PLACEMENT_AUTHORITY_ACQUIRE_RETRIES: usize = 4;
const PLACEMENT_AUTHORITY_RELEASE_RETRIES: usize = 4;
pub const DFS_PLACEMENT_RECONCILIATION_SCHEMA_VERSION: u32 = 1;

enum PlacementIntentAuthority {
    Ordinary,
    Managed(DfsPlacementAuthorityPermit),
    Stale,
}

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

fn is_zero_usize(value: &usize) -> bool {
    *value == 0
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
    if let Some(existing) = rules
        .iter_mut()
        .find(|existing| existing.path_prefix == rule.path_prefix && existing.scope == rule.scope)
    {
        return Some(std::mem::replace(existing, rule));
    }
    rules.push(rule);
    rules.sort_by(|left, right| {
        left.path_prefix
            .cmp(&right.path_prefix)
            .then_with(|| placement_scope_rank(left.scope).cmp(&placement_scope_rank(right.scope)))
    });
    None
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
        .unwrap_or_else(|| legacy_desired_replicas.max(1).min(2));
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
        desired_replicas.max(1).min(2)
    };
    let min_commit_replicas = if policy == DfsObjectPolicy::ReplicatedConfirmed {
        desired_replicas
    } else {
        1
    };
    DfsPlacementPolicy::new(policy, min_commit_replicas, desired_replicas)
}

/// Old previews predate either reachability-aware aggregate impact (v1) or an
/// exact preview token (v2), so they are not safe inputs for an Apply decision.
pub const DFS_PLACEMENT_PREVIEW_SCHEMA_VERSION: u32 = 2;
pub const DFS_PLACEMENT_PREVIEW_TOKEN_SCHEMA_VERSION: u32 = 1;
const DFS_PLACEMENT_PREVIEW_TOKEN_DIGEST_PREFIX: &str = "dfs-placement-preview-v1:blake3:";
const DFS_PLACEMENT_METADATA_SNAPSHOT_DIGEST_PREFIX: &str =
    "dfs-placement-metadata-set-v1:blake3-xor:";

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
        explain_replicas(&placement_request)?
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPlacementReconciliationState {
    Pending,
    Scanning,
    Repairing,
    Completed,
    Failed,
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
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn current_placement_file_system_config(&mut self) -> Result<DfsFileSystemConfig, DfsError> {
        let file_system_id = crate::sifs_file_system_id();
        if let Some(control_plane) = self.control_plane.as_mut()
            && let Some(config) = DfsControlPlaneStore::new(control_plane.as_mut())
                .get_file_system_config(&file_system_id)?
        {
            return Ok(config);
        }
        self.metadata
            .filesystems
            .get(&file_system_id)
            .map(DfsFileSystemConfig::from)
            .ok_or(DfsError::UnknownFileSystem { file_system_id })
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
        validate_replication_plan(desired_replica_node_ids, &replication_policy)?;
        let mut object = self.current_read_object_metadata(object_id)?;
        if object.path != path || object.version != *expected_version {
            // A concurrent rename or write made this scan result stale. The
            // next reconciliation page/pass will see the authoritative state.
            return Ok(());
        }
        let (expected_resolved, expected_replication, expected_placement) =
            reconciliation_placement_for_object(accepted_config, job, &object)?;
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
            intent_id: intent_id.clone(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: Some(object.version.clone()),
            target_version: object.version.clone(),
            requesting_node_id: self.node_id.clone(),
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

    /// Advance at most one durable placement job by a bounded object page.
    /// Progress is compare-and-swapped in cluster KV, so concurrent daemon
    /// ticks may help without losing a cursor or double-counting work.
    pub fn run_placement_reconciliation_bounded(
        &mut self,
        max_objects: usize,
    ) -> Result<Option<DfsPlacementReconciliationJob>, DfsError> {
        let file_system_id = crate::sifs_file_system_id();
        let (expected, current_config, coordinator_node_id) = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(None);
            };
            let mut store = DfsControlPlaneStore::new(control_plane.as_mut());
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
            let Some(job) = store.next_placement_reconciliation(&file_system_id, &config)? else {
                return Ok(None);
            };
            (job, config, coordinator_node_id)
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
        let page = self.placement_object_page(job, max_objects)?;
        let mut pass_pending_repairs =
            if job.state == DfsPlacementReconciliationState::Repairing && job.cursor.is_none() {
                0
            } else {
                job.pending_repair_count
            };
        for object in &page.objects {
            if job.state == DfsPlacementReconciliationState::Scanning {
                job.scanned_object_count = job.scanned_object_count.saturating_add(1);
            }
            if object.kind == DfsObjectKind::Tombstone || !placement_job_matches(job, &object.path)
            {
                continue;
            }
            if job.state == DfsPlacementReconciliationState::Scanning {
                job.matched_object_count = job.matched_object_count.saturating_add(1);
            }
            let (resolved, replication, placement) =
                reconciliation_placement_for_object(config, job, object)?;
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
    job.last_error =
        Some("superseded because a newer placement configuration is authoritative".to_string());
}

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
    let placement = explain_replicas(&DfsReplicaPlacementRequest {
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        target_version: object.version.clone(),
        coordinator_node_id: config.owner_node_id.clone(),
        policy: replication.clone(),
        candidates: replica_candidates,
    })?;
    Ok((resolved, replication, placement))
}

fn placement_reconciliation_policy_error(error: DfsDistributedError) -> DfsError {
    match error {
        DfsDistributedError::InvalidPolicy { reason } => {
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
                    reconciliation_placement_for_object(config, job, &object)?;
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
            .pending_writes
            .get(intent_id)
            .map(|pending| pending.content_hash.clone())
            .or_else(|| {
                self.current_read_object_metadata(&intent.object_id)
                    .ok()
                    .and_then(|object| object.content_hash)
            });
        let mut aborted = intent;
        aborted.state = DfsWriteIntentState::Aborted;
        // Terminalizing an intent after replacement is safe without the old
        // generation's permit. If the generation is still current but the
        // intent payload is malformed, `held_permit` remains held through this
        // durable abort. Persist it before dropping the in-memory repair state.
        let abort_result = (|| {
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

    fn advertisement(node_id: &str, zone: &str) -> DfsNodeAdvertisement {
        DfsNodeAdvertisement {
            node_id: node(node_id),
            capacity_weight: 1,
            accepts_metadata: false,
            accepts_content: true,
            zone: Some(zone.to_string()),
            updated_at: time::OffsetDateTime::now_utc(),
        }
    }

    fn placement_config(owner_node_id: NodeId) -> DfsFileSystemConfig {
        DfsFileSystemConfig {
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
    fn self_rule_overrides_same_path_subtree_without_affecting_children() {
        let mut config = DfsFileSystemConfig {
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
            updated_at: time::OffsetDateTime::now_utc(),
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
            Some(DfsWriteIntentState::Aborted)
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
        assert_eq!(durable_intent.status, DfsWriteIntentStatus::Aborted);
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
        object.remove("accepted_config");

        let decoded: DfsPlacementReconciliationJob =
            serde_json::from_value(value).expect("deserialize legacy reconciliation job");
        assert_eq!(decoded.schema_version, 0);
        assert!(decoded.unavailable_node_ids.is_empty());
        assert!(decoded.candidate_snapshot.is_empty());
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
}
