//! Per-shard W floor and N, and the choice of a shard's metadata members.

use super::super::*;
use crate::placement::{
    default_placement_policy, path_is_within, placement_policy_from_rule, placement_rule_matches,
};
use crate::{DfsPlacementScope, DfsSubtreePolicy};

/// W floor and N for one shard: the highest content W and N among the rules
/// that can apply to the shard's own paths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsMetadataShardBounds {
    pub w_floor: usize,
    pub n: usize,
}

/// Bounds for `shard` under `config`.
///
/// Routing gives a path to the deepest matching prefix assignment, otherwise
/// to the narrowest hash range, otherwise to the root assignment. A prefix
/// shard, and the root as prefix `/`, owns its root and every descendant
/// outside the nested prefix assignments of other shards. The rules that can
/// apply to those paths are the deepest rule matching the root, the deepest
/// subtree rule at or above the root (or the default policy when there is
/// none), and every rule strictly inside the root that is not at or inside a
/// nested assignment. Any path can land on a hash-range shard, so every rule
/// applies to it, plus the default policy.
///
/// The default policy is the replicated-eventual default with
/// `eligible_metadata_members` candidates. Object policies and per-write
/// policies are not derivable from configuration; a commit covers them with
/// [`metadata_commit_write_set_floor`].
pub fn metadata_shard_replication_bounds(
    config: &DfsFileSystemConfig,
    shard: &DfsMetadataShardConfig,
    shards: &[DfsMetadataShardConfig],
    eligible_metadata_members: usize,
) -> Result<DfsMetadataShardBounds, KvError> {
    let invalid = |error: crate::DfsDistributedError| KvError::Serialization {
        detail: format!(
            "metadata shard {} replication bounds: {error}",
            shard.shard_id
        ),
    };
    let default = default_placement_policy(
        DfsObjectPolicy::ReplicatedEventual,
        eligible_metadata_members,
    )
    .map_err(invalid)?;
    let mut bounds = DfsMetadataShardBounds { w_floor: 0, n: 0 };
    fn include(bounds: &mut DfsMetadataShardBounds, w: usize, n: usize) {
        bounds.w_floor = bounds.w_floor.max(w);
        bounds.n = bounds.n.max(n);
    }
    let include_rule = |bounds: &mut DfsMetadataShardBounds, rule: &DfsSubtreePolicy| {
        let policy = placement_policy_from_rule(rule).map_err(invalid)?;
        include(bounds, policy.min_commit_replicas, policy.desired_replicas);
        Ok::<(), KvError>(())
    };
    let root = match &shard.assignment {
        DfsMetadataShardAssignment::Root => Some("/".to_string()),
        DfsMetadataShardAssignment::PathPrefix { prefix } => {
            Some(normalize_metadata_selection_path(prefix)?)
        }
        DfsMetadataShardAssignment::NameHashRange { .. } => None,
    };
    let Some(root) = root else {
        for rule in &config.subtree_policies {
            include_rule(&mut bounds, rule)?;
        }
        include(
            &mut bounds,
            default.min_commit_replicas,
            default.desired_replicas,
        );
        return Ok(clamp(bounds));
    };
    let nested = shards
        .iter()
        .filter(|other| {
            other.file_system_id == shard.file_system_id && other.shard_id != shard.shard_id
        })
        .filter_map(|other| match &other.assignment {
            DfsMetadataShardAssignment::PathPrefix { prefix } => {
                normalize_metadata_selection_path(prefix).ok()
            }
            _ => None,
        })
        .filter(|prefix| prefix != &root && metadata_path_matches_prefix(prefix, &root))
        .collect::<Vec<_>>();
    let depth = |path: &str| path.split('/').filter(|part| !part.is_empty()).count();
    let normalized = |rule: &DfsSubtreePolicy| normalize_metadata_selection_path(&rule.path_prefix);
    let mut governing_root: Option<&DfsSubtreePolicy> = None;
    let mut deepest_subtree: Option<&DfsSubtreePolicy> = None;
    let mut inner = Vec::new();
    for rule in &config.subtree_policies {
        let prefix = normalized(rule)?;
        if placement_rule_matches(
            &DfsSubtreePolicy {
                path_prefix: prefix.clone(),
                ..rule.clone()
            },
            &root,
        ) {
            let deeper = |current: Option<&DfsSubtreePolicy>| {
                current.is_none_or(|current| {
                    let current_depth = depth(&current.path_prefix);
                    depth(&prefix) > current_depth
                        || (depth(&prefix) == current_depth
                            && rule.scope == DfsPlacementScope::SelfOnly
                            && current.scope != DfsPlacementScope::SelfOnly)
                })
            };
            if deeper(governing_root) {
                governing_root = Some(rule);
            }
            if rule.scope == DfsPlacementScope::Subtree
                && deepest_subtree
                    .is_none_or(|current| depth(&prefix) > depth(&current.path_prefix))
            {
                deepest_subtree = Some(rule);
            }
        }
        if prefix != root
            && path_is_within(&prefix, &root)
            && !nested.iter().any(|nested| path_is_within(&prefix, nested))
        {
            inner.push(rule);
        }
    }
    if let Some(rule) = governing_root {
        include_rule(&mut bounds, rule)?;
    }
    match deepest_subtree {
        Some(rule) => include_rule(&mut bounds, rule)?,
        None => include(
            &mut bounds,
            default.min_commit_replicas,
            default.desired_replicas,
        ),
    }
    for rule in inner {
        include_rule(&mut bounds, rule)?;
    }
    Ok(clamp(bounds))
}

fn clamp(bounds: DfsMetadataShardBounds) -> DfsMetadataShardBounds {
    let n = bounds.n.max(1);
    DfsMetadataShardBounds {
        w_floor: bounds.w_floor.clamp(1, n),
        n,
    }
}

/// Write-set size one commit needs: the shard's W floor, raised to the
/// committed intent's own content W. Object and per-write policies can ask
/// for more than the shard's rules, and metadata is never less durable than
/// the content it commits.
pub fn metadata_commit_write_set_floor(
    bounds: &DfsMetadataShardBounds,
    intent_min_commit_replicas: usize,
) -> usize {
    bounds.w_floor.max(intent_min_commit_replicas)
}

/// A switched shard's effective N: the rule-derived N, its W floor, and the
/// largest content W it has accepted for commit. The last part only grows, so
/// the effective N never falls below the W of content committed under the
/// write-set protocol.
pub fn metadata_shard_effective_n(
    bounds: &DfsMetadataShardBounds,
    write_set: Option<&super::DfsMetadataShardWriteSet>,
) -> usize {
    bounds
        .n
        .max(bounds.w_floor)
        .max(write_set.map_or(1, super::DfsMetadataShardWriteSet::accepted_w))
}

/// How a member considered for a shard's copies relates to the shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum DfsMetadataWriteSetCandidateKind {
    /// A configured member that was not recruited as a substitute.
    Original,
    /// A configured member recruited to replace another, at this generation.
    Substitute { recruited_at_generation: u64 },
    /// An eligible member that is not configured.
    Candidate,
}

/// One member considered for a shard's metadata copies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataWriteSetCandidate {
    pub node_id: NodeId,
    pub mobile: bool,
    /// In the current write set, so it holds every committed change.
    pub in_write_set: bool,
    /// The network layer reports a settled path. A ranking input only.
    pub reachable: bool,
    pub kind: DfsMetadataWriteSetCandidateKind,
}

/// Choose the members that keep a shard's metadata copies, the primary first.
///
/// The primary is always kept. Non-mobile members are taken up to N, ranked
/// by reachability (a lost member must not keep its place, or nothing names
/// its replacement), then originals before substitutes and older substitutes
/// before newer ones (the most recent recruit is the one an original's return
/// makes surplus), then write-set membership, a per-shard rendezvous hash that
/// spreads copies across shards, and node id. Mobile members are added only
/// while the non-mobile members, the primary included, are fewer than the W
/// floor, and then only up to the floor.
pub fn select_metadata_shard_members(
    shard: &DfsMetadataShardConfig,
    candidates: &[DfsMetadataWriteSetCandidate],
    bounds: &DfsMetadataShardBounds,
    primary_mobile: bool,
) -> Vec<NodeId> {
    let rank = |candidate: &DfsMetadataWriteSetCandidate| {
        let hash = blake3::Hasher::new()
            .update(b"sinomo-dfs-metadata-member-rank\0")
            .update(shard.file_system_id.to_string().as_bytes())
            .update(b"\0")
            .update(shard.shard_id.as_bytes())
            .update(b"\0")
            .update(candidate.node_id.as_str().as_bytes())
            .finalize();
        (
            !candidate.reachable,
            candidate.kind,
            !candidate.in_write_set,
            *hash.as_bytes(),
            candidate.node_id.clone(),
        )
    };
    let mut others = candidates
        .iter()
        .filter(|candidate| candidate.node_id != shard.primary_node_id)
        .collect::<Vec<_>>();
    others.sort_by_key(|candidate| rank(candidate));
    others.dedup_by(|left, right| left.node_id == right.node_id);
    let mut selected = vec![shard.primary_node_id.clone()];
    let mut non_mobile = usize::from(!primary_mobile);
    for candidate in others.iter().filter(|candidate| !candidate.mobile) {
        if selected.len() >= bounds.n {
            break;
        }
        selected.push(candidate.node_id.clone());
        non_mobile += 1;
    }
    if non_mobile < bounds.w_floor {
        for candidate in others.iter().filter(|candidate| candidate.mobile) {
            if selected.len() >= bounds.w_floor {
                break;
            }
            selected.push(candidate.node_id.clone());
        }
    }
    selected
}
