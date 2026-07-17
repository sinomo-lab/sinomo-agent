//! Durable, explainable DFS placement policy and reconciliation records.

use crate::posix_path::normalize_posix_path;
use crate::{
    DfsControlPlaneStore, DfsDistributedError, DfsError, DfsFileSystemConfig, DfsFileSystemId,
    DfsMessageTransport, DfsNamespaceOperation, DfsNodeAdvertisement, DfsObjectId, DfsObjectKind,
    DfsObjectMetadata, DfsObjectPage, DfsObjectPageCursor, DfsPlacementScope, DfsReplicaCandidate,
    DfsReplicaPlacementExplanation, DfsReplicaPlacementRequest, DfsReplicationPolicy,
    DfsSubtreePolicy, DfsVersion, DfsWriteIntentState, DistributedWriteIntent, NetworkedDfsService,
    dfs_now_unix_nanos, explain_replicas, validate_replication_plan,
};
use serde::{Deserialize, Serialize};
use sinomo_core::DfsObjectPolicy;
use sinomo_core::node::NodeId;
use std::collections::{BTreeMap, BTreeSet};

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
pub struct DfsResolvedPlacementPolicy {
    pub path: String,
    pub policy: DfsPlacementPolicy,
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
            source_path: None,
            source_scope: None,
            inherited: false,
            explanation: "no managed override matched; using the object's durable policy"
                .to_string(),
        });
    };
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
        source_path: Some(rule.path_prefix.clone()),
        source_scope: Some(rule.scope),
        inherited,
        explanation,
    })
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementPreview {
    pub resolved: DfsResolvedPlacementPolicy,
    pub placement: DfsReplicaPlacementExplanation,
    pub current_replica_node_ids: Vec<NodeId>,
    pub added_replica_node_ids: Vec<NodeId>,
    pub removed_replica_node_ids: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[allow(clippy::too_many_arguments)]
pub fn preview_placement(
    config: &DfsFileSystemConfig,
    path: &str,
    object_id: DfsObjectId,
    version: DfsVersion,
    object_policy: DfsObjectPolicy,
    coordinator_node_id: NodeId,
    current_replica_node_ids: Vec<NodeId>,
    candidates: Vec<DfsNodeAdvertisement>,
    override_policy: Option<DfsPlacementPolicy>,
    override_scope: DfsPlacementScope,
) -> Result<DfsPlacementPreview, DfsDistributedError> {
    let mut preview_config = config.clone();
    if let Some(policy) = override_policy {
        let rule = normalize_placement_rule(path, override_scope, policy)?;
        upsert_placement_rule(&mut preview_config.subtree_policies, rule);
    }
    let resolved =
        resolve_placement_policy(&preview_config, path, object_policy, candidates.len())?;
    let node_zones = candidates
        .iter()
        .filter_map(|candidate| {
            candidate
                .zone
                .clone()
                .map(|zone| (candidate.node_id.clone(), zone))
        })
        .collect::<BTreeMap<_, _>>();
    let replication = resolved.policy.replication_policy(node_zones)?;
    let candidates = candidates
        .into_iter()
        .filter(|candidate| candidate.accepts_content)
        .map(|candidate| {
            DfsReplicaCandidate::weighted(candidate.node_id, candidate.capacity_weight.max(1))
                .map(|candidate_plan| candidate_plan.with_zone(candidate.zone))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let placement = explain_replicas(&DfsReplicaPlacementRequest {
        file_system_id: config.file_system_id.clone(),
        object_id,
        target_version: version,
        coordinator_node_id,
        policy: replication,
        candidates,
    })?;
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
    let added_replica_node_ids = desired.difference(&current).cloned().collect();
    let removed_replica_node_ids = current.difference(&desired).cloned().collect();
    let mut warnings = Vec::new();
    if resolved.policy.min_commit_replicas < resolved.policy.desired_replicas {
        warnings.push(format!(
            "writes become visible at W={} while repair continues toward N={}",
            resolved.policy.min_commit_replicas, resolved.policy.desired_replicas
        ));
    }
    Ok(DfsPlacementPreview {
        resolved,
        placement,
        current_replica_node_ids,
        added_replica_node_ids,
        removed_replica_node_ids,
        warnings,
    })
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
    pub job_id: String,
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    pub scope: DfsPlacementScope,
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
        validate_replication_plan(desired_replica_node_ids, &replication_policy)?;
        let mut object = self.current_read_object_metadata(object_id)?;
        if object.path != path || object.version != *expected_version {
            // A concurrent rename or write made this scan result stale. The
            // next reconciliation page/pass will see the authoritative state.
            return Ok(());
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
            "placement:{job_id}:{}:{}",
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
        let (expected, config, coordinator_node_id) = {
            let Some(control_plane) = self.control_plane.as_mut() else {
                return Ok(None);
            };
            let store = DfsControlPlaneStore::new(control_plane.as_mut());
            let Some(job) = store
                .placement_reconciliations(&file_system_id)?
                .into_iter()
                .find(|job| !job.state.is_terminal())
            else {
                return Ok(None);
            };
            let config = store
                .get_file_system_config(&file_system_id)?
                .ok_or_else(|| DfsError::UnknownFileSystem {
                    file_system_id: file_system_id.clone(),
                })?;
            let coordinator_node_id = store
                .select_metadata_shard_for_path_strict_default_sifs_split(&file_system_id, "/")?
                .map(|shard| shard.primary_node_id);
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
        let result =
            self.advance_placement_reconciliation_page(&config, &mut desired, max_objects.max(1));
        desired.revision = desired.revision.saturating_add(1);
        desired.updated_at_unix_nanos = dfs_now_unix_nanos();
        match result {
            Ok(()) => desired.last_error = None,
            Err(error) => desired.last_error = Some(error.to_string()),
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
        let candidates = self.placement_content_candidates()?;
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
            let resolved = resolve_placement_policy(
                config,
                &object.path,
                object.policy.clone(),
                candidates.len(),
            )?;
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
            let desired_nodes = explain_replicas(&DfsReplicaPlacementRequest {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                target_version: object.version.clone(),
                coordinator_node_id: self.node_id.clone(),
                policy: replication.clone(),
                candidates: candidates
                    .iter()
                    .filter(|candidate| candidate.accepts_content)
                    .map(|candidate| {
                        DfsReplicaCandidate::weighted(
                            candidate.node_id.clone(),
                            candidate.capacity_weight.max(1),
                        )
                        .map(|plan| plan.with_zone(candidate.zone.clone()))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            })?
            .plan
            .desired_replica_nodes;
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
        if let Some(backend) = self.metadata_backend.as_ref() {
            return backend
                .objects_for_file_system_page(&job.file_system_id, job.cursor.as_ref(), max_objects)
                .map_err(DfsError::from);
        }
        let mut objects = self
            .metadata
            .objects
            .values()
            .filter(|object| object.file_system_id == job.file_system_id)
            .cloned()
            .collect::<Vec<_>>();
        objects.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        let start = job.cursor.as_ref().map_or(0, |cursor| {
            objects
                .iter()
                .position(|object| {
                    object.path > cursor.path
                        || (object.path == cursor.path && object.object_id > cursor.object_id)
                })
                .unwrap_or(objects.len())
        });
        let end = start.saturating_add(max_objects).min(objects.len());
        let page_objects = objects[start..end].to_vec();
        let next_cursor = (end < objects.len()).then(|| {
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

fn placement_job_matches(job: &DfsPlacementReconciliationJob, path: &str) -> bool {
    match job.scope {
        DfsPlacementScope::SelfOnly => path == job.path,
        DfsPlacementScope::Subtree => path_is_within(path, &job.path),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("node")
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

        let child = resolve_placement_policy(
            &config,
            "/users/team/report.md",
            DfsObjectPolicy::ReplicatedEventual,
            2,
        )
        .expect("child");
        assert_eq!(child.policy.desired_replicas, 3);
        assert!(child.inherited);
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
    }
}
