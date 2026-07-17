//! Distributed eventual object protocol for DFS.

use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicationPolicy {
    pub min_commit_replicas: usize,
    pub desired_replicas: usize,
    /// Nodes that must be part of every deterministic placement plan.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_node_ids: Vec<NodeId>,
    /// Nodes that may not receive durable content for this intent.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forbidden_node_ids: Vec<NodeId>,
    /// Minimum number of distinct advertised zones represented by the plan.
    /// Zero disables the constraint so legacy intents remain valid.
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub min_distinct_zones: usize,
    /// Authoritative placement-input snapshot used to make zone selection
    /// deterministic and explainable for this intent.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_zones: BTreeMap<NodeId, String>,
}

fn is_zero_usize(value: &usize) -> bool {
    *value == 0
}

impl DfsReplicationPolicy {
    pub fn new(
        min_commit_replicas: usize,
        desired_replicas: usize,
    ) -> Result<Self, DfsDistributedError> {
        let policy = Self {
            min_commit_replicas,
            desired_replicas,
            required_node_ids: Vec::new(),
            forbidden_node_ids: Vec::new(),
            min_distinct_zones: 0,
            node_zones: BTreeMap::new(),
        };
        policy.validate()?;
        Ok(policy)
    }

    pub fn confirmed(desired_replicas: usize) -> Result<Self, DfsDistributedError> {
        Self::new(desired_replicas, desired_replicas)
    }

    pub fn is_strict_confirmed(&self) -> bool {
        self.min_commit_replicas == self.desired_replicas
    }

    pub fn with_placement_constraints(
        mut self,
        mut required_node_ids: Vec<NodeId>,
        mut forbidden_node_ids: Vec<NodeId>,
        min_distinct_zones: usize,
        node_zones: BTreeMap<NodeId, String>,
    ) -> Result<Self, DfsDistributedError> {
        required_node_ids.sort();
        required_node_ids.dedup();
        forbidden_node_ids.sort();
        forbidden_node_ids.dedup();
        self.required_node_ids = required_node_ids;
        self.forbidden_node_ids = forbidden_node_ids;
        self.min_distinct_zones = min_distinct_zones;
        self.node_zones = node_zones;
        self.validate()?;
        Ok(self)
    }

    /// Change W/N while retaining node and zone constraints.
    pub fn with_counts(
        &self,
        min_commit_replicas: usize,
        desired_replicas: usize,
    ) -> Result<Self, DfsDistributedError> {
        let mut policy = self.clone();
        policy.min_commit_replicas = min_commit_replicas;
        policy.desired_replicas = desired_replicas;
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<(), DfsDistributedError> {
        if self.min_commit_replicas == 0 {
            return Err(DfsDistributedError::InvalidPolicy {
                reason: "min_commit_replicas must be greater than zero".to_string(),
            });
        }

        if self.desired_replicas == 0 {
            return Err(DfsDistributedError::InvalidPolicy {
                reason: "desired_replicas must be greater than zero".to_string(),
            });
        }

        if self.min_commit_replicas > self.desired_replicas {
            return Err(DfsDistributedError::InvalidPolicy {
                reason: "min_commit_replicas cannot exceed desired_replicas".to_string(),
            });
        }

        if self.required_node_ids.len() > self.desired_replicas {
            return Err(DfsDistributedError::InvalidPolicy {
                reason: format!(
                    "required node count {} cannot exceed desired_replicas {}",
                    self.required_node_ids.len(),
                    self.desired_replicas
                ),
            });
        }

        if let Some(node_id) = self
            .required_node_ids
            .iter()
            .find(|node_id| self.forbidden_node_ids.contains(*node_id))
        {
            return Err(DfsDistributedError::InvalidPolicy {
                reason: format!("node {node_id} cannot be both required and forbidden"),
            });
        }

        if self.min_distinct_zones > self.desired_replicas {
            return Err(DfsDistributedError::InvalidPolicy {
                reason: format!(
                    "min_distinct_zones {} cannot exceed desired_replicas {}",
                    self.min_distinct_zones, self.desired_replicas
                ),
            });
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicaCandidate {
    pub node_id: NodeId,
    pub weight: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
}

impl DfsReplicaCandidate {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            weight: 1,
            zone: None,
        }
    }

    pub fn weighted(node_id: NodeId, weight: u32) -> Result<Self, DfsDistributedError> {
        if weight == 0 {
            return Err(DfsDistributedError::InvalidReplicaSet {
                reason: "replica candidate weight must be greater than zero".to_string(),
            });
        }
        Ok(Self {
            node_id,
            weight,
            zone: None,
        })
    }

    pub fn with_zone(mut self, zone: Option<String>) -> Self {
        self.zone = zone;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicaPlacementRequest {
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub target_version: DfsVersion,
    pub coordinator_node_id: NodeId,
    pub policy: DfsReplicationPolicy,
    pub candidates: Vec<DfsReplicaCandidate>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicaPlacementPlan {
    pub desired_replica_nodes: Vec<NodeId>,
    pub preferred_commit_nodes: Vec<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicaPlacementDecision {
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    pub selected: bool,
    pub preferred_for_commit: bool,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicaPlacementExplanation {
    pub plan: DfsReplicaPlacementPlan,
    pub represented_zone_count: usize,
    pub decisions: Vec<DfsReplicaPlacementDecision>,
}

pub fn plan_replicas(
    request: &DfsReplicaPlacementRequest,
) -> Result<DfsReplicaPlacementPlan, DfsDistributedError> {
    Ok(explain_replicas(request)?.plan)
}

pub fn explain_replicas(
    request: &DfsReplicaPlacementRequest,
) -> Result<DfsReplicaPlacementExplanation, DfsDistributedError> {
    explain_replicas_with_environment_validation(request, true)
}

/// Build the most useful deterministic placement explanation possible without
/// rejecting an otherwise valid policy merely because the current cluster
/// environment cannot satisfy it. This is intentionally crate-private: write
/// paths must continue to use [`explain_replicas`] and its strict validation.
pub(crate) fn explain_replicas_best_effort(
    request: &DfsReplicaPlacementRequest,
) -> Result<DfsReplicaPlacementExplanation, DfsDistributedError> {
    explain_replicas_with_environment_validation(request, false)
}

fn explain_replicas_with_environment_validation(
    request: &DfsReplicaPlacementRequest,
    validate_environment: bool,
) -> Result<DfsReplicaPlacementExplanation, DfsDistributedError> {
    request.policy.validate()?;
    if validate_environment {
        validate_candidate_set(&request.candidates, &request.policy)?;
    } else {
        validate_candidate_shape(&request.candidates)?;
    }

    let mut ranked = request
        .candidates
        .iter()
        .filter(|candidate| {
            !request
                .policy
                .forbidden_node_ids
                .contains(&candidate.node_id)
        })
        .cloned()
        .collect::<Vec<_>>();
    for candidate in &mut ranked {
        if candidate.zone.is_none() {
            candidate.zone = request.policy.node_zones.get(&candidate.node_id).cloned();
        }
    }
    ranked.sort_by(|left, right| {
        let left_is_coordinator = left.node_id == request.coordinator_node_id;
        let right_is_coordinator = right.node_id == request.coordinator_node_id;
        right_is_coordinator
            .cmp(&left_is_coordinator)
            .then_with(|| {
                placement_score(request, right)
                    .cmp(&placement_score(request, left))
                    .then_with(|| left.node_id.cmp(&right.node_id))
            })
    });

    let mut selected = Vec::<DfsReplicaCandidate>::new();
    for candidate in &ranked {
        if request
            .policy
            .required_node_ids
            .contains(&candidate.node_id)
        {
            selected.push(candidate.clone());
        }
    }

    while selected.len() < request.policy.desired_replicas {
        let selected_zones = selected
            .iter()
            .filter_map(|candidate| candidate.zone.as_deref())
            .collect::<BTreeSet<_>>();
        let need_new_zone = selected_zones.len() < request.policy.min_distinct_zones;
        let candidate = ranked.iter().find(|candidate| {
            !selected
                .iter()
                .any(|selected| selected.node_id == candidate.node_id)
                && (!need_new_zone
                    || candidate
                        .zone
                        .as_deref()
                        .is_some_and(|zone| !selected_zones.contains(zone)))
        });
        let candidate = candidate.or_else(|| {
            ranked.iter().find(|candidate| {
                !selected
                    .iter()
                    .any(|selected| selected.node_id == candidate.node_id)
            })
        });
        let Some(candidate) = candidate else {
            break;
        };
        selected.push(candidate.clone());
    }

    let desired_replica_nodes = selected
        .iter()
        .map(|candidate| candidate.node_id.clone())
        .collect::<Vec<_>>();
    let preferred_commit_nodes = desired_replica_nodes
        .iter()
        .take(request.policy.min_commit_replicas)
        .cloned()
        .collect();

    let plan = DfsReplicaPlacementPlan {
        desired_replica_nodes,
        preferred_commit_nodes,
    };
    if validate_environment {
        validate_replication_plan(&plan.desired_replica_nodes, &request.policy)?;
    }

    let represented_zone_count = selected
        .iter()
        .filter_map(|candidate| candidate.zone.as_deref())
        .collect::<BTreeSet<_>>()
        .len();
    let decisions = request
        .candidates
        .iter()
        .map(|candidate| {
            let selected = plan.desired_replica_nodes.contains(&candidate.node_id);
            let preferred_for_commit = plan.preferred_commit_nodes.contains(&candidate.node_id);
            let required = request
                .policy
                .required_node_ids
                .contains(&candidate.node_id);
            let forbidden = request
                .policy
                .forbidden_node_ids
                .contains(&candidate.node_id);
            let zone = candidate
                .zone
                .clone()
                .or_else(|| request.policy.node_zones.get(&candidate.node_id).cloned());
            let reason = if forbidden {
                "rejected: forbidden by policy".to_string()
            } else if required {
                "selected: required by policy".to_string()
            } else if preferred_for_commit {
                "selected: preferred commit replica".to_string()
            } else if selected {
                match zone.as_deref() {
                    Some(zone) => format!("selected: deterministic rank and zone spread ({zone})"),
                    None => "selected: deterministic rank".to_string(),
                }
            } else {
                "eligible but outside the desired replica count".to_string()
            };
            DfsReplicaPlacementDecision {
                node_id: candidate.node_id.clone(),
                zone,
                selected,
                preferred_for_commit,
                reason,
            }
        })
        .collect();
    Ok(DfsReplicaPlacementExplanation {
        plan,
        represented_zone_count,
        decisions,
    })
}

pub fn validate_replication_plan(
    desired_replica_nodes: &[NodeId],
    policy: &DfsReplicationPolicy,
) -> Result<(), DfsDistributedError> {
    policy.validate()?;
    validate_replica_set(desired_replica_nodes, policy)?;
    for required in &policy.required_node_ids {
        if !desired_replica_nodes.contains(required) {
            return Err(DfsDistributedError::InvalidReplicaSet {
                reason: format!("required node {required} is missing from the replica plan"),
            });
        }
    }
    for forbidden in &policy.forbidden_node_ids {
        if desired_replica_nodes.contains(forbidden) {
            return Err(DfsDistributedError::InvalidReplicaSet {
                reason: format!("forbidden node {forbidden} is present in the replica plan"),
            });
        }
    }
    if policy.min_distinct_zones > 0 {
        let represented = desired_replica_nodes
            .iter()
            .filter_map(|node_id| policy.node_zones.get(node_id))
            .collect::<BTreeSet<_>>()
            .len();
        if represented < policy.min_distinct_zones {
            return Err(DfsDistributedError::InvalidReplicaSet {
                reason: format!(
                    "replica plan represents {represented} zones but policy requires {}",
                    policy.min_distinct_zones
                ),
            });
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsWriteIntentState {
    AwaitingReceipts,
    Committed,
    Rejected,
    Conflict,
    Aborted,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsReplicaReceiptState {
    Durable,
    Rejected,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DistributedWriteIntent {
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_version: Option<DfsVersion>,
    pub target_version: DfsVersion,
    pub requesting_node_id: NodeId,
    #[serde(default)]
    pub owner_epoch: u64,
    pub desired_replica_nodes: Vec<NodeId>,
    pub policy: DfsReplicationPolicy,
    #[serde(default)]
    pub touch_parent_on_create: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_predecessor_object_id: Option<DfsObjectId>,
    pub state: DfsWriteIntentState,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DistributedReplicaReceipt {
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub replica_node_id: NodeId,
    pub state: DfsReplicaReceiptState,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DistributedRepairJob {
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub missing_replica_node_id: NodeId,
}

#[derive(Clone, Debug, Default)]
pub struct DistributedEventualCoordinator {
    next_intent_id: u64,
    intents: BTreeMap<String, DistributedWriteIntent>,
    awaiting_intent_id_by_object: BTreeMap<ObjectKey, String>,
    awaiting_intent_ids_by_object_id: BTreeMap<DfsObjectId, BTreeSet<String>>,
    claimed_intent_ids_by_target: BTreeMap<IntentTargetKey, BTreeSet<String>>,
    receipts: BTreeMap<String, BTreeMap<NodeId, DistributedReplicaReceipt>>,
    receipt_count: usize,
    committed_versions: BTreeMap<ObjectKey, DfsVersion>,
    repair_jobs: Vec<DistributedRepairJob>,
}

impl DistributedEventualCoordinator {
    pub fn new() -> Self {
        Self {
            next_intent_id: 1,
            ..Self::default()
        }
    }

    pub fn set_committed_version(
        &mut self,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
    ) {
        self.committed_versions
            .insert(ObjectKey::new(file_system_id, object_id), version);
    }

    pub fn committed_version(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> Option<&DfsVersion> {
        self.committed_versions
            .get(&ObjectKey::from_refs(file_system_id, object_id))
    }

    pub fn create_intent(
        &mut self,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        base_version: Option<DfsVersion>,
        target_version: DfsVersion,
        requesting_node_id: NodeId,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<DistributedWriteIntent, DfsDistributedError> {
        self.create_intent_at_epoch(
            file_system_id,
            object_id,
            base_version,
            target_version,
            requesting_node_id,
            0,
            desired_replica_nodes,
            policy,
        )
    }

    pub fn create_intent_at_epoch(
        &mut self,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        base_version: Option<DfsVersion>,
        target_version: DfsVersion,
        requesting_node_id: NodeId,
        owner_epoch: u64,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<DistributedWriteIntent, DfsDistributedError> {
        self.create_intent_at_epoch_with_parent_touch(
            file_system_id,
            object_id,
            base_version,
            target_version,
            requesting_node_id,
            owner_epoch,
            desired_replica_nodes,
            policy,
            false,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_intent_at_epoch_with_parent_touch(
        &mut self,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        base_version: Option<DfsVersion>,
        target_version: DfsVersion,
        requesting_node_id: NodeId,
        owner_epoch: u64,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        touch_parent_on_create: bool,
        create_predecessor_object_id: Option<DfsObjectId>,
    ) -> Result<DistributedWriteIntent, DfsDistributedError> {
        self.validate_new_intent(
            &file_system_id,
            &object_id,
            base_version.as_ref(),
            &target_version,
            &desired_replica_nodes,
            &policy,
        )?;

        let intent_id = self.next_intent_id(&requesting_node_id, &object_id, &target_version);
        let intent = DistributedWriteIntent {
            intent_id: intent_id.clone(),
            file_system_id,
            object_id,
            base_version,
            target_version,
            requesting_node_id,
            owner_epoch,
            desired_replica_nodes,
            policy,
            touch_parent_on_create,
            create_predecessor_object_id,
            state: DfsWriteIntentState::AwaitingReceipts,
        };
        self.replace_indexed_intent(intent.clone());
        Ok(intent)
    }

    pub fn validate_new_intent(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        base_version: Option<&DfsVersion>,
        target_version: &DfsVersion,
        desired_replica_nodes: &[NodeId],
        policy: &DfsReplicationPolicy,
    ) -> Result<(), DfsDistributedError> {
        validate_replication_plan(desired_replica_nodes, policy)?;

        let key = ObjectKey::from_refs(file_system_id, object_id);
        if let Some(committed_version) = self.committed_versions.get(&key)
            && Some(committed_version) != base_version
        {
            return Err(DfsDistributedError::BaseVersionConflict {
                object_id: object_id.clone(),
                expected_base_version: committed_version.clone(),
                supplied_base_version: base_version.cloned(),
            });
        }

        let target_key = IntentTargetKey::from_refs(file_system_id, object_id, target_version);
        if self
            .claimed_intent_ids_by_target
            .get(&target_key)
            .is_some_and(|intent_ids| !intent_ids.is_empty())
        {
            return Err(DfsDistributedError::DuplicateIntent {
                object_id: object_id.clone(),
                target_version: target_version.clone(),
            });
        }

        if let Some(pending) = self
            .awaiting_intent_id_by_object
            .get(&key)
            .and_then(|intent_id| self.intents.get(intent_id))
            .filter(|intent| matches!(intent.state, DfsWriteIntentState::AwaitingReceipts))
        {
            return Err(DfsDistributedError::PendingIntentConflict {
                object_id: object_id.clone(),
                pending_intent_id: pending.intent_id.clone(),
                pending_target_version: pending.target_version.clone(),
                requested_target_version: target_version.clone(),
            });
        }

        Ok(())
    }

    pub fn intent(&self, intent_id: &str) -> Option<&DistributedWriteIntent> {
        self.intents.get(intent_id)
    }

    pub fn has_awaiting_intent_targeting(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
    ) -> bool {
        self.awaiting_intent_ids_by_object_id
            .get(object_id)
            .into_iter()
            .flatten()
            .filter_map(|intent_id| self.intents.get(intent_id))
            .any(|intent| {
                intent.target_version == *target_version
                    && matches!(intent.state, DfsWriteIntentState::AwaitingReceipts)
            })
    }

    pub fn intent_for_target(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
    ) -> Option<&DistributedWriteIntent> {
        self.intents.values().find(|intent| {
            intent.file_system_id == *file_system_id
                && intent.object_id == *object_id
                && intent.target_version == *target_version
        })
    }

    /// Return the live awaiting/committed claim for an exact write target.
    /// Terminal history deliberately does not participate: an aborted claim
    /// releases the target, and routed replay/cleanup must find a later live
    /// claim rather than the first historical row with the same identity.
    pub fn claimed_intent_for_target(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
    ) -> Option<&DistributedWriteIntent> {
        self.claimed_intent_ids_by_target
            .get(&IntentTargetKey::from_refs(
                file_system_id,
                object_id,
                target_version,
            ))
            .into_iter()
            .flatten()
            .find_map(|intent_id| self.intents.get(intent_id))
    }

    /// Whether any write intent for the object is still awaiting receipts.
    /// Cross-shard moves fence on this: a half-finished write must settle
    /// before its object may change shard authority.
    pub fn has_open_intent_for_object(&self, object_id: &DfsObjectId) -> bool {
        self.awaiting_intent_ids_by_object_id
            .get(object_id)
            .is_some_and(|intent_ids| !intent_ids.is_empty())
    }

    pub fn restore_intent(
        &mut self,
        intent: DistributedWriteIntent,
    ) -> Result<(), DfsDistributedError> {
        self.restore_intent_deferring_repair_rebuild(intent)?;
        self.rebuild_repair_jobs();
        Ok(())
    }

    /// [`restore_intent`](Self::restore_intent) without the repair-job rebuild.
    /// Bulk restore paths (metadata hydration) call this per record and issue
    /// one [`rebuild_repair_jobs_after_bulk_restore`](Self::rebuild_repair_jobs_after_bulk_restore)
    /// at the end — rebuilding per record turned hydration quadratic and burned
    /// seconds of CPU per SIFS open once a degraded cluster accumulated a
    /// backlog of unsettled intents.
    pub(crate) fn restore_intent_deferring_repair_rebuild(
        &mut self,
        intent: DistributedWriteIntent,
    ) -> Result<(), DfsDistributedError> {
        validate_replication_plan(&intent.desired_replica_nodes, &intent.policy)?;
        if let Some(existing) = self.intents.get(&intent.intent_id)
            && (existing.file_system_id != intent.file_system_id
                || existing.object_id != intent.object_id
                || existing.target_version != intent.target_version)
        {
            return Err(DfsDistributedError::DuplicateIntentId {
                intent_id: intent.intent_id.clone(),
                existing_object_id: existing.object_id.clone(),
                new_object_id: intent.object_id.clone(),
            });
        }
        if matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
            self.validate_new_intent(
                &intent.file_system_id,
                &intent.object_id,
                intent.base_version.as_ref(),
                &intent.target_version,
                &intent.desired_replica_nodes,
                &intent.policy,
            )?;
        }
        self.advance_next_intent_id(&intent.intent_id);
        if matches!(intent.state, DfsWriteIntentState::Committed) {
            self.committed_versions.insert(
                ObjectKey::new(intent.file_system_id.clone(), intent.object_id.clone()),
                intent.target_version.clone(),
            );
        }
        self.replace_indexed_intent(intent);
        Ok(())
    }

    pub fn observe_restored_intent_id(&mut self, intent_id: &str) {
        self.advance_next_intent_id(intent_id);
    }

    pub fn restore_durable_receipt(
        &mut self,
        intent_id: &str,
        replica_node_id: NodeId,
    ) -> Result<(), DfsDistributedError> {
        let rebuild =
            self.restore_durable_receipt_deferring_repair_rebuild(intent_id, replica_node_id)?;
        if rebuild {
            self.rebuild_repair_jobs();
        }
        Ok(())
    }

    /// [`restore_durable_receipt`](Self::restore_durable_receipt) without the
    /// repair-job rebuild; returns whether the caller-visible rebuild would
    /// have run (a committed intent's receipt landed). Bulk restore paths call
    /// this per receipt and rebuild once at the end.
    pub(crate) fn restore_durable_receipt_deferring_repair_rebuild(
        &mut self,
        intent_id: &str,
        replica_node_id: NodeId,
    ) -> Result<bool, DfsDistributedError> {
        let intent = self.intents.get(intent_id).cloned().ok_or_else(|| {
            DfsDistributedError::UnknownIntent {
                intent_id: intent_id.to_string(),
            }
        })?;
        if !intent.desired_replica_nodes.contains(&replica_node_id) {
            return Err(DfsDistributedError::UnexpectedReplica {
                intent_id: intent_id.to_string(),
                replica_node_id,
            });
        }
        self.replace_indexed_receipt(DistributedReplicaReceipt {
            intent_id: intent_id.to_string(),
            file_system_id: intent.file_system_id,
            object_id: intent.object_id,
            version: intent.target_version,
            replica_node_id,
            state: DfsReplicaReceiptState::Durable,
        });
        Ok(matches!(intent.state, DfsWriteIntentState::Committed))
    }

    /// Rebuild repair jobs once after a bulk restore that used the deferring
    /// restore variants.
    pub(crate) fn rebuild_repair_jobs_after_bulk_restore(&mut self) {
        self.rebuild_repair_jobs();
    }

    pub fn receipts_for_intent(&self, intent_id: &str) -> Vec<&DistributedReplicaReceipt> {
        self.receipts
            .get(intent_id)
            .into_iter()
            .flat_map(|receipts| receipts.values())
            .collect()
    }

    pub fn durable_receipt_count(&self, intent_id: &str) -> usize {
        self.receipts
            .get(intent_id)
            .into_iter()
            .flat_map(|receipts| receipts.values())
            .filter(|receipt| receipt.state == DfsReplicaReceiptState::Durable)
            .count()
    }

    pub fn is_commit_ready(&self, intent_id: &str) -> Result<bool, DfsDistributedError> {
        let intent =
            self.intents
                .get(intent_id)
                .ok_or_else(|| DfsDistributedError::UnknownIntent {
                    intent_id: intent_id.to_string(),
                })?;

        Ok(matches!(intent.state, DfsWriteIntentState::Committed)
            || self.durable_receipt_count(intent_id) >= intent.policy.min_commit_replicas)
    }

    pub fn record_durable_receipt(
        &mut self,
        intent_id: &str,
        replica_node_id: NodeId,
    ) -> Result<DistributedWriteIntent, DfsDistributedError> {
        let intent = self.intents.get(intent_id).cloned().ok_or_else(|| {
            DfsDistributedError::UnknownIntent {
                intent_id: intent_id.to_string(),
            }
        })?;

        if !intent.desired_replica_nodes.contains(&replica_node_id) {
            return Err(DfsDistributedError::UnexpectedReplica {
                intent_id: intent_id.to_string(),
                replica_node_id,
            });
        }

        if matches!(
            intent.state,
            DfsWriteIntentState::Rejected
                | DfsWriteIntentState::Conflict
                | DfsWriteIntentState::Aborted
        ) {
            return Err(DfsDistributedError::IntentClosed {
                intent_id: intent_id.to_string(),
                state: intent.state,
            });
        }

        let receipt = DistributedReplicaReceipt {
            intent_id: intent_id.to_string(),
            file_system_id: intent.file_system_id.clone(),
            object_id: intent.object_id.clone(),
            version: intent.target_version.clone(),
            replica_node_id: replica_node_id.clone(),
            state: DfsReplicaReceiptState::Durable,
        };
        self.replace_indexed_receipt(receipt);
        // A newly durable receipt can only retire this node's repair job for this
        // intent; it never introduces new missing replicas. Removing exactly that
        // job is the complete update, so a full `rebuild_repair_jobs` scan would
        // recompute the identical result at O(committed intents × replicas) cost.
        self.repair_jobs.retain(|job| {
            job.intent_id != intent_id || job.missing_replica_node_id != replica_node_id
        });

        self.intents
            .get(intent_id)
            .cloned()
            .ok_or_else(|| DfsDistributedError::UnknownIntent {
                intent_id: intent_id.to_string(),
            })
    }

    pub fn repair_jobs(&self) -> &[DistributedRepairJob] {
        &self.repair_jobs
    }

    pub fn intent_count(&self) -> usize {
        self.intents.len()
    }

    pub fn receipt_count(&self) -> usize {
        self.receipt_count
    }

    pub fn committed_version_count(&self) -> usize {
        self.committed_versions.len()
    }

    pub fn abort_intent(
        &mut self,
        intent_id: &str,
    ) -> Result<DistributedWriteIntent, DfsDistributedError> {
        let mut intent = self.intents.get(intent_id).cloned().ok_or_else(|| {
            DfsDistributedError::UnknownIntent {
                intent_id: intent_id.to_string(),
            }
        })?;
        if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
            return Err(DfsDistributedError::IntentClosed {
                intent_id: intent_id.to_string(),
                state: intent.state.clone(),
            });
        }
        intent.state = DfsWriteIntentState::Aborted;
        self.replace_indexed_intent(intent.clone());
        self.repair_jobs.retain(|job| job.intent_id != intent_id);
        Ok(intent)
    }

    pub fn downgrade_intent_policy(
        &mut self,
        intent_id: &str,
        policy: DfsReplicationPolicy,
    ) -> Result<DistributedWriteIntent, DfsDistributedError> {
        policy.validate()?;
        let current = self.intents.get(intent_id).cloned().ok_or_else(|| {
            DfsDistributedError::UnknownIntent {
                intent_id: intent_id.to_string(),
            }
        })?;

        if !matches!(current.state, DfsWriteIntentState::AwaitingReceipts) {
            return Err(DfsDistributedError::IntentClosed {
                intent_id: intent_id.to_string(),
                state: current.state,
            });
        }
        if policy.desired_replicas != current.policy.desired_replicas {
            return Err(DfsDistributedError::InvalidPolicy {
                reason: "first-version policy downgrade must keep desired_replicas unchanged"
                    .to_string(),
            });
        }
        if policy.min_commit_replicas > current.policy.min_commit_replicas {
            return Err(DfsDistributedError::InvalidPolicy {
                reason: "policy downgrade cannot increase min_commit_replicas".to_string(),
            });
        }

        let intent =
            self.intents
                .get_mut(intent_id)
                .ok_or_else(|| DfsDistributedError::UnknownIntent {
                    intent_id: intent_id.to_string(),
                })?;
        intent.policy = policy;

        self.intents
            .get(intent_id)
            .cloned()
            .ok_or_else(|| DfsDistributedError::UnknownIntent {
                intent_id: intent_id.to_string(),
            })
    }

    pub fn finalize_commit_if_ready(
        &mut self,
        intent_id: &str,
    ) -> Result<bool, DfsDistributedError> {
        if !self.is_commit_ready(intent_id)? {
            return Ok(false);
        }
        if self
            .intents
            .get(intent_id)
            .is_some_and(|intent| matches!(intent.state, DfsWriteIntentState::Committed))
        {
            return Ok(true);
        }
        self.commit_intent(intent_id)?;
        Ok(true)
    }

    pub(crate) fn commit_intent(&mut self, intent_id: &str) -> Result<(), DfsDistributedError> {
        let durable_nodes = self.durable_receipt_nodes(intent_id);
        let mut intent = self.intents.get(intent_id).cloned().ok_or_else(|| {
            DfsDistributedError::UnknownIntent {
                intent_id: intent_id.to_string(),
            }
        })?;

        if matches!(intent.state, DfsWriteIntentState::Committed) {
            return Ok(());
        }

        intent.state = DfsWriteIntentState::Committed;
        self.replace_indexed_intent(intent.clone());
        self.committed_versions.insert(
            ObjectKey::new(intent.file_system_id.clone(), intent.object_id.clone()),
            intent.target_version.clone(),
        );

        for missing_replica_node_id in intent
            .desired_replica_nodes
            .iter()
            .filter(|node_id| !durable_nodes.contains(*node_id))
        {
            self.repair_jobs.push(DistributedRepairJob {
                intent_id: intent.intent_id.clone(),
                file_system_id: intent.file_system_id.clone(),
                object_id: intent.object_id.clone(),
                version: intent.target_version.clone(),
                missing_replica_node_id: missing_replica_node_id.clone(),
            });
        }

        Ok(())
    }

    /// Replace the canonical intent row and update both sparse views as one
    /// in-memory transition. The coordinator is rebuilt from durable metadata
    /// on hydration, so these indexes never enter the journal or snapshot
    /// format.
    fn replace_indexed_intent(&mut self, intent: DistributedWriteIntent) {
        if let Some(previous) = self.intents.remove(&intent.intent_id) {
            self.unindex_intent(&previous);
        }
        self.index_intent(&intent);
        self.intents.insert(intent.intent_id.clone(), intent);
    }

    fn index_intent(&mut self, intent: &DistributedWriteIntent) {
        if matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
            self.awaiting_intent_id_by_object.insert(
                ObjectKey::from_refs(&intent.file_system_id, &intent.object_id),
                intent.intent_id.clone(),
            );
            self.awaiting_intent_ids_by_object_id
                .entry(intent.object_id.clone())
                .or_default()
                .insert(intent.intent_id.clone());
        }
        if matches!(
            intent.state,
            DfsWriteIntentState::AwaitingReceipts | DfsWriteIntentState::Committed
        ) {
            self.claimed_intent_ids_by_target
                .entry(IntentTargetKey::from_intent(intent))
                .or_default()
                .insert(intent.intent_id.clone());
        }
    }

    fn unindex_intent(&mut self, intent: &DistributedWriteIntent) {
        if matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
            let object_key = ObjectKey::from_refs(&intent.file_system_id, &intent.object_id);
            if self.awaiting_intent_id_by_object.get(&object_key) == Some(&intent.intent_id) {
                self.awaiting_intent_id_by_object.remove(&object_key);
            }
            let remove_object_entry = self
                .awaiting_intent_ids_by_object_id
                .get_mut(&intent.object_id)
                .is_some_and(|intent_ids| {
                    intent_ids.remove(&intent.intent_id);
                    intent_ids.is_empty()
                });
            if remove_object_entry {
                self.awaiting_intent_ids_by_object_id
                    .remove(&intent.object_id);
            }
        }
        if matches!(
            intent.state,
            DfsWriteIntentState::AwaitingReceipts | DfsWriteIntentState::Committed
        ) {
            let target_key = IntentTargetKey::from_intent(intent);
            let remove_target_entry = self
                .claimed_intent_ids_by_target
                .get_mut(&target_key)
                .is_some_and(|intent_ids| {
                    intent_ids.remove(&intent.intent_id);
                    intent_ids.is_empty()
                });
            if remove_target_entry {
                self.claimed_intent_ids_by_target.remove(&target_key);
            }
        }
    }

    fn replace_indexed_receipt(&mut self, receipt: DistributedReplicaReceipt) {
        let replaced = self
            .receipts
            .entry(receipt.intent_id.clone())
            .or_default()
            .insert(receipt.replica_node_id.clone(), receipt)
            .is_some();
        if !replaced {
            self.receipt_count = self.receipt_count.saturating_add(1);
        }
    }

    fn durable_receipt_nodes(&self, intent_id: &str) -> BTreeSet<NodeId> {
        self.receipts
            .get(intent_id)
            .into_iter()
            .flat_map(|receipts| receipts.iter())
            .filter_map(|(replica_node_id, receipt)| {
                (receipt.state == DfsReplicaReceiptState::Durable)
                    .then_some(replica_node_id.clone())
            })
            .collect()
    }

    fn next_intent_id(
        &mut self,
        requesting_node_id: &NodeId,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
    ) -> String {
        // The hash covers the target version as well as the object: after a
        // cross-shard move a different coordinator mints intents for the same
        // object, and its counter may coincide with the previous
        // coordinator's. Distinct targets must never share an intent id;
        // a collision then only remains possible for genuinely competing
        // writes to one target, which duplicate-intent detection must see.
        let object_hash =
            blake3::hash(format!("{}\0{}", object_id.as_str(), target_version.as_str()).as_bytes())
                .to_hex();
        let object_hash = &object_hash[..16];
        let intent_id = format!(
            "dfs_intent_{}_{}_{}",
            requesting_node_id.as_str(),
            object_hash,
            self.next_intent_id,
        );
        self.next_intent_id += 1;
        intent_id
    }

    fn advance_next_intent_id(&mut self, intent_id: &str) {
        let Some(suffix) = intent_id.rsplit('_').next() else {
            return;
        };
        let Ok(value) = suffix.parse::<u64>() else {
            return;
        };
        self.next_intent_id = self.next_intent_id.max(value.saturating_add(1));
    }

    fn rebuild_repair_jobs(&mut self) {
        self.repair_jobs.clear();
        let mut jobs = Vec::new();
        for intent in self
            .intents
            .values()
            .filter(|intent| matches!(intent.state, DfsWriteIntentState::Committed))
        {
            let durable_receipts = self.receipts.get(intent.intent_id.as_str());
            for missing_replica_node_id in intent.desired_replica_nodes.iter().filter(|node_id| {
                !durable_receipts.is_some_and(|receipts| {
                    receipts
                        .get(*node_id)
                        .is_some_and(|receipt| receipt.state == DfsReplicaReceiptState::Durable)
                })
            }) {
                jobs.push(DistributedRepairJob {
                    intent_id: intent.intent_id.clone(),
                    file_system_id: intent.file_system_id.clone(),
                    object_id: intent.object_id.clone(),
                    version: intent.target_version.clone(),
                    missing_replica_node_id: missing_replica_node_id.clone(),
                });
            }
        }
        self.repair_jobs = jobs;
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ObjectKey {
    file_system_id: DfsFileSystemId,
    object_id: DfsObjectId,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct IntentTargetKey {
    object: ObjectKey,
    target_version: DfsVersion,
}

impl IntentTargetKey {
    fn from_intent(intent: &DistributedWriteIntent) -> Self {
        Self::from_refs(
            &intent.file_system_id,
            &intent.object_id,
            &intent.target_version,
        )
    }

    fn from_refs(
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
    ) -> Self {
        Self {
            object: ObjectKey::from_refs(file_system_id, object_id),
            target_version: target_version.clone(),
        }
    }
}

impl ObjectKey {
    fn new(file_system_id: DfsFileSystemId, object_id: DfsObjectId) -> Self {
        Self {
            file_system_id,
            object_id,
        }
    }

    fn from_refs(file_system_id: &DfsFileSystemId, object_id: &DfsObjectId) -> Self {
        Self {
            file_system_id: file_system_id.clone(),
            object_id: object_id.clone(),
        }
    }
}

fn validate_replica_set(
    desired_replica_nodes: &[NodeId],
    policy: &DfsReplicationPolicy,
) -> Result<(), DfsDistributedError> {
    let unique_nodes = desired_replica_nodes.iter().collect::<BTreeSet<_>>();
    if unique_nodes.len() != desired_replica_nodes.len() {
        return Err(DfsDistributedError::InvalidReplicaSet {
            reason: "desired_replica_nodes contains duplicates".to_string(),
        });
    }

    if desired_replica_nodes.len() != policy.desired_replicas {
        return Err(DfsDistributedError::InvalidReplicaSet {
            reason: "desired_replica_nodes must match desired_replicas".to_string(),
        });
    }

    Ok(())
}

fn validate_candidate_set(
    candidates: &[DfsReplicaCandidate],
    policy: &DfsReplicationPolicy,
) -> Result<(), DfsDistributedError> {
    validate_candidate_shape(candidates)?;
    validate_candidate_environment(candidates, policy)
}

fn validate_candidate_shape(candidates: &[DfsReplicaCandidate]) -> Result<(), DfsDistributedError> {
    let unique_nodes = candidates
        .iter()
        .map(|candidate| &candidate.node_id)
        .collect::<BTreeSet<_>>();
    if unique_nodes.len() != candidates.len() {
        return Err(DfsDistributedError::InvalidReplicaSet {
            reason: "replica candidates contain duplicate nodes".to_string(),
        });
    }
    if candidates.iter().any(|candidate| candidate.weight == 0) {
        return Err(DfsDistributedError::InvalidReplicaSet {
            reason: "replica candidate weight must be greater than zero".to_string(),
        });
    }
    Ok(())
}

fn validate_candidate_environment(
    candidates: &[DfsReplicaCandidate],
    policy: &DfsReplicationPolicy,
) -> Result<(), DfsDistributedError> {
    let eligible = candidates
        .iter()
        .filter(|candidate| !policy.forbidden_node_ids.contains(&candidate.node_id))
        .collect::<Vec<_>>();
    if eligible.len() < policy.desired_replicas {
        return Err(DfsDistributedError::InvalidReplicaSet {
            reason: format!(
                "not enough eligible replica candidates for desired_replicas: have {}, need {}",
                eligible.len(),
                policy.desired_replicas
            ),
        });
    }
    for required in &policy.required_node_ids {
        if !eligible
            .iter()
            .any(|candidate| &candidate.node_id == required)
        {
            return Err(DfsDistributedError::InvalidReplicaSet {
                reason: format!("required node {required} is not an eligible candidate"),
            });
        }
    }
    if policy.min_distinct_zones > 0 {
        let available_zone_count = eligible
            .iter()
            .filter_map(|candidate| {
                candidate
                    .zone
                    .as_ref()
                    .or_else(|| policy.node_zones.get(&candidate.node_id))
            })
            .collect::<BTreeSet<_>>()
            .len();
        if available_zone_count < policy.min_distinct_zones {
            return Err(DfsDistributedError::InvalidReplicaSet {
                reason: format!(
                    "only {available_zone_count} eligible zones are advertised but policy requires {}",
                    policy.min_distinct_zones
                ),
            });
        }
    }
    Ok(())
}

fn placement_score(request: &DfsReplicaPlacementRequest, candidate: &DfsReplicaCandidate) -> u128 {
    let hash_input = format!(
        "{}\0{}\0{}\0{}",
        request.file_system_id, request.object_id, request.target_version, candidate.node_id
    );
    let hash = blake3::hash(hash_input.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hash.as_bytes()[..16]);
    u128::from_be_bytes(bytes).saturating_mul(candidate.weight as u128)
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DfsDistributedError {
    #[error("invalid DFS replication policy: {reason}")]
    InvalidPolicy { reason: String },
    #[error("invalid DFS replica set: {reason}")]
    InvalidReplicaSet { reason: String },
    #[error("unknown DFS write intent: {intent_id}")]
    UnknownIntent { intent_id: String },
    #[error("duplicate DFS write intent for {object_id} at {target_version}")]
    DuplicateIntent {
        object_id: DfsObjectId,
        target_version: DfsVersion,
    },
    #[error(
        "duplicate DFS write intent id {intent_id} is used for both {existing_object_id} and {new_object_id}"
    )]
    DuplicateIntentId {
        intent_id: String,
        existing_object_id: DfsObjectId,
        new_object_id: DfsObjectId,
    },
    #[error(
        "pending DFS write intent {pending_intent_id} for {object_id} at {pending_target_version} conflicts with requested target {requested_target_version}"
    )]
    PendingIntentConflict {
        object_id: DfsObjectId,
        pending_intent_id: String,
        pending_target_version: DfsVersion,
        requested_target_version: DfsVersion,
    },
    #[error(
        "base version conflict for {object_id}: expected {expected_base_version}, supplied {supplied_base_version:?}"
    )]
    BaseVersionConflict {
        object_id: DfsObjectId,
        expected_base_version: DfsVersion,
        supplied_base_version: Option<DfsVersion>,
    },
    #[error("replica {replica_node_id} is not expected for DFS write intent {intent_id}")]
    UnexpectedReplica {
        intent_id: String,
        replica_node_id: NodeId,
    },
    #[error("DFS write intent {intent_id} is closed in state {state:?}")]
    IntentClosed {
        intent_id: String,
        state: DfsWriteIntentState,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("sifs").expect("filesystem")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::from_legacy_named("obj_file").expect("object")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("version")
    }

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("node")
    }

    fn policy(min_commit_replicas: usize, desired_replicas: usize) -> DfsReplicationPolicy {
        DfsReplicationPolicy::new(min_commit_replicas, desired_replicas).expect("policy")
    }

    fn create_intent(
        coordinator: &mut DistributedEventualCoordinator,
        policy: DfsReplicationPolicy,
    ) -> DistributedWriteIntent {
        let file_system_id = file_system_id();
        let object_id = object_id();
        coordinator.set_committed_version(
            file_system_id.clone(),
            object_id.clone(),
            version("version_1"),
        );
        coordinator
            .create_intent(
                file_system_id,
                object_id,
                Some(version("version_1")),
                version("version_2"),
                node("node_writer"),
                vec![node("node_a"), node("node_b"), node("node_c")],
                policy,
            )
            .expect("intent")
    }

    fn assert_intent_indexes_match_history(coordinator: &DistributedEventualCoordinator) {
        let mut awaiting_by_object = BTreeMap::new();
        let mut awaiting_by_object_id: BTreeMap<DfsObjectId, BTreeSet<String>> = BTreeMap::new();
        let mut claimed_by_target: BTreeMap<IntentTargetKey, BTreeSet<String>> = BTreeMap::new();

        for (intent_id, intent) in &coordinator.intents {
            if matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
                assert!(
                    awaiting_by_object
                        .insert(
                            ObjectKey::from_refs(&intent.file_system_id, &intent.object_id),
                            intent_id.clone(),
                        )
                        .is_none(),
                    "reference history contains competing awaiting intents"
                );
                awaiting_by_object_id
                    .entry(intent.object_id.clone())
                    .or_default()
                    .insert(intent_id.clone());
            }
            if matches!(
                intent.state,
                DfsWriteIntentState::AwaitingReceipts | DfsWriteIntentState::Committed
            ) {
                claimed_by_target
                    .entry(IntentTargetKey::from_intent(intent))
                    .or_default()
                    .insert(intent_id.clone());
            }
        }

        assert_eq!(coordinator.awaiting_intent_id_by_object, awaiting_by_object);
        assert_eq!(
            coordinator.awaiting_intent_ids_by_object_id,
            awaiting_by_object_id
        );
        assert_eq!(coordinator.claimed_intent_ids_by_target, claimed_by_target);
    }

    #[test]
    fn sparse_intent_indexes_preserve_validation_and_restore_transitions() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let mut intent = create_intent(&mut coordinator, policy(2, 3));
        assert_intent_indexes_match_history(&coordinator);
        assert!(coordinator.has_open_intent_for_object(&intent.object_id));
        assert!(
            coordinator.has_awaiting_intent_targeting(&intent.object_id, &intent.target_version)
        );
        assert_eq!(
            coordinator
                .claimed_intent_for_target(
                    &intent.file_system_id,
                    &intent.object_id,
                    &intent.target_version,
                )
                .map(|found| found.intent_id.as_str()),
            Some(intent.intent_id.as_str())
        );

        let exact_error = coordinator
            .validate_new_intent(
                &intent.file_system_id,
                &intent.object_id,
                intent.base_version.as_ref(),
                &intent.target_version,
                &intent.desired_replica_nodes,
                &intent.policy,
            )
            .expect_err("exact target must win over the pending-object conflict");
        assert!(matches!(
            exact_error,
            DfsDistributedError::DuplicateIntent { .. }
        ));

        let different_target_error = coordinator
            .validate_new_intent(
                &intent.file_system_id,
                &intent.object_id,
                intent.base_version.as_ref(),
                &version("version_3"),
                &intent.desired_replica_nodes,
                &intent.policy,
            )
            .expect_err("different target must retain pending-object classification");
        assert!(matches!(
            different_target_error,
            DfsDistributedError::PendingIntentConflict { .. }
        ));

        coordinator
            .commit_intent(&intent.intent_id)
            .expect("commit intent");
        assert_intent_indexes_match_history(&coordinator);
        assert!(!coordinator.has_open_intent_for_object(&intent.object_id));
        assert!(
            !coordinator.has_awaiting_intent_targeting(&intent.object_id, &intent.target_version)
        );
        assert!(matches!(
            coordinator.validate_new_intent(
                &intent.file_system_id,
                &intent.object_id,
                Some(&intent.target_version),
                &intent.target_version,
                &intent.desired_replica_nodes,
                &intent.policy,
            ),
            Err(DfsDistributedError::DuplicateIntent { .. })
        ));
        assert_eq!(
            coordinator
                .claimed_intent_for_target(
                    &intent.file_system_id,
                    &intent.object_id,
                    &intent.target_version,
                )
                .map(|found| &found.state),
            Some(&DfsWriteIntentState::Committed)
        );

        intent.state = DfsWriteIntentState::Aborted;
        coordinator
            .restore_intent(intent.clone())
            .expect("replace committed restore with aborted history");
        assert_intent_indexes_match_history(&coordinator);
        assert!(
            coordinator
                .claimed_intent_for_target(
                    &intent.file_system_id,
                    &intent.object_id,
                    &intent.target_version,
                )
                .is_none(),
            "aborted history must not remain a replayable target claim"
        );
        coordinator
            .validate_new_intent(
                &intent.file_system_id,
                &intent.object_id,
                Some(&intent.target_version),
                &intent.target_version,
                &intent.desired_replica_nodes,
                &intent.policy,
            )
            .expect("aborted replacement must release the target claim");

        intent.base_version = Some(intent.target_version.clone());
        intent.state = DfsWriteIntentState::AwaitingReceipts;
        coordinator
            .restore_intent(intent.clone())
            .expect("replace aborted history with awaiting intent");
        assert_intent_indexes_match_history(&coordinator);
        assert!(coordinator.has_open_intent_for_object(&intent.object_id));
    }

    #[test]
    fn sparse_target_claims_keep_all_restored_committed_duplicates() {
        let mut seed = DistributedEventualCoordinator::new();
        let mut first = create_intent(&mut seed, policy(1, 3));
        first.state = DfsWriteIntentState::Committed;
        let mut second = first.clone();
        second.intent_id = "dfs_intent_restored_duplicate_42".to_string();

        let mut coordinator = DistributedEventualCoordinator::new();
        coordinator
            .restore_intent(first.clone())
            .expect("first committed restore");
        coordinator
            .restore_intent(second.clone())
            .expect("second committed restore for the same target");
        assert_intent_indexes_match_history(&coordinator);
        let target_key = IntentTargetKey::from_intent(&first);
        assert_eq!(
            coordinator
                .claimed_intent_ids_by_target
                .get(&target_key)
                .map(BTreeSet::len),
            Some(2)
        );

        first.state = DfsWriteIntentState::Aborted;
        coordinator
            .restore_intent(first.clone())
            .expect("terminalize first restored claim");
        assert_intent_indexes_match_history(&coordinator);
        assert_eq!(
            coordinator
                .claimed_intent_ids_by_target
                .get(&target_key)
                .map(BTreeSet::len),
            Some(1)
        );
        assert!(matches!(
            coordinator.validate_new_intent(
                &second.file_system_id,
                &second.object_id,
                Some(&second.target_version),
                &second.target_version,
                &second.desired_replica_nodes,
                &second.policy,
            ),
            Err(DfsDistributedError::DuplicateIntent { .. })
        ));

        second.state = DfsWriteIntentState::Aborted;
        coordinator
            .restore_intent(second.clone())
            .expect("terminalize final restored claim");
        assert_intent_indexes_match_history(&coordinator);
        assert!(
            !coordinator
                .claimed_intent_ids_by_target
                .contains_key(&target_key)
        );
        coordinator
            .validate_new_intent(
                &second.file_system_id,
                &second.object_id,
                Some(&second.target_version),
                &second.target_version,
                &second.desired_replica_nodes,
                &second.policy,
            )
            .expect("last terminalized claim must release the target");
    }

    #[test]
    fn claimed_target_lookup_skips_older_aborted_history() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let first = create_intent(&mut coordinator, policy(2, 3));
        coordinator
            .abort_intent(&first.intent_id)
            .expect("abort first target claim");
        let second = coordinator
            .create_intent(
                first.file_system_id.clone(),
                first.object_id.clone(),
                first.base_version.clone(),
                first.target_version.clone(),
                first.requesting_node_id.clone(),
                first.desired_replica_nodes.clone(),
                first.policy.clone(),
            )
            .expect("aborted claim releases the exact target");

        assert_ne!(first.intent_id, second.intent_id);
        assert_eq!(coordinator.intent_count(), 2);
        assert_eq!(
            coordinator
                .claimed_intent_for_target(
                    &second.file_system_id,
                    &second.object_id,
                    &second.target_version,
                )
                .map(|intent| intent.intent_id.as_str()),
            Some(second.intent_id.as_str())
        );
        assert_intent_indexes_match_history(&coordinator);
    }

    #[test]
    fn awaiting_index_keeps_filesystems_separate_for_shared_object_ids() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let shared_object_id = object_id();
        let first_file_system = file_system_id();
        let second_file_system =
            DfsFileSystemId::from_legacy_named("other_sifs").expect("filesystem");
        for file_system_id in [first_file_system, second_file_system] {
            coordinator
                .create_intent(
                    file_system_id,
                    shared_object_id.clone(),
                    None,
                    version("version_2"),
                    node("node_writer"),
                    vec![node("node_a")],
                    policy(1, 1),
                )
                .expect("one awaiting intent per filesystem/object key");
        }

        assert_intent_indexes_match_history(&coordinator);
        assert_eq!(
            coordinator
                .awaiting_intent_ids_by_object_id
                .get(&shared_object_id)
                .map(BTreeSet::len),
            Some(2)
        );
        assert!(coordinator.has_open_intent_for_object(&shared_object_id));
    }

    #[test]
    fn receipt_buckets_preserve_order_idempotency_and_intent_isolation() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let first = create_intent(&mut coordinator, policy(2, 3));
        let second = coordinator
            .create_intent(
                file_system_id(),
                DfsObjectId::from_legacy_named("obj_second_file").expect("object"),
                None,
                version("version_second_1"),
                node("node_writer"),
                vec![node("node_a"), node("node_b"), node("node_c")],
                policy(2, 3),
            )
            .expect("second intent");

        coordinator
            .record_durable_receipt(&first.intent_id, node("node_b"))
            .expect("first intent node b receipt");
        coordinator
            .record_durable_receipt(&first.intent_id, node("node_a"))
            .expect("first intent node a receipt");
        coordinator
            .record_durable_receipt(&second.intent_id, node("node_c"))
            .expect("second intent receipt");
        coordinator
            .record_durable_receipt(&first.intent_id, node("node_a"))
            .expect("duplicate receipt");

        let first_nodes = coordinator
            .receipts_for_intent(&first.intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<Vec<_>>();
        assert_eq!(first_nodes, vec![node("node_a"), node("node_b")]);
        assert_eq!(coordinator.durable_receipt_count(&first.intent_id), 2);
        assert_eq!(coordinator.durable_receipt_count(&second.intent_id), 1);
        assert_eq!(coordinator.receipt_count(), 3);
    }

    #[test]
    fn closed_intent_restore_keeps_receipts_but_runtime_record_rejects() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let intent = create_intent(&mut coordinator, policy(2, 3));
        coordinator
            .record_durable_receipt(&intent.intent_id, node("node_a"))
            .expect("initial receipt");
        coordinator
            .abort_intent(&intent.intent_id)
            .expect("abort intent");

        assert!(matches!(
            coordinator.record_durable_receipt(&intent.intent_id, node("node_b")),
            Err(DfsDistributedError::IntentClosed {
                state: DfsWriteIntentState::Aborted,
                ..
            })
        ));
        assert_eq!(coordinator.receipt_count(), 1);

        coordinator
            .restore_durable_receipt(&intent.intent_id, node("node_b"))
            .expect("hydration accepts a retained closed intent receipt");
        assert_eq!(coordinator.receipt_count(), 2);
        assert_eq!(coordinator.durable_receipt_count(&intent.intent_id), 2);
    }

    #[test]
    fn same_id_intent_restore_does_not_prune_historical_receipt_bucket() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let mut intent = create_intent(&mut coordinator, policy(2, 3));
        coordinator
            .record_durable_receipt(&intent.intent_id, node("node_a"))
            .expect("historical receipt");

        intent.state = DfsWriteIntentState::Aborted;
        intent.desired_replica_nodes = vec![node("node_b")];
        intent.policy = policy(1, 1);
        coordinator
            .restore_intent(intent.clone())
            .expect("same-id restore with a changed desired set");

        let receipts = coordinator.receipts_for_intent(&intent.intent_id);
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].replica_node_id, node("node_a"));
        assert_eq!(coordinator.receipt_count(), 1);
    }

    #[test]
    fn placement_planner_prefers_coordinator_and_is_deterministic() {
        let request = DfsReplicaPlacementRequest {
            file_system_id: file_system_id(),
            object_id: object_id(),
            target_version: version("version_2"),
            coordinator_node_id: node("node_b"),
            policy: policy(2, 3),
            candidates: vec![
                DfsReplicaCandidate::new(node("node_a")),
                DfsReplicaCandidate::new(node("node_b")),
                DfsReplicaCandidate::new(node("node_c")),
                DfsReplicaCandidate::new(node("node_d")),
            ],
        };

        let first = plan_replicas(&request).expect("first plan");
        let second = plan_replicas(&request).expect("second plan");

        assert_eq!(first, second);
        assert_eq!(first.desired_replica_nodes.len(), 3);
        assert_eq!(first.desired_replica_nodes[0], node("node_b"));
        assert_eq!(first.preferred_commit_nodes.len(), 2);
        assert!(
            first
                .preferred_commit_nodes
                .iter()
                .all(|node_id| first.desired_replica_nodes.contains(node_id))
        );
    }

    #[test]
    fn intent_ids_include_requesting_node_and_object_to_avoid_collisions() {
        let mut left = DistributedEventualCoordinator::new();
        let mut right = DistributedEventualCoordinator::new();
        let mut same_node = DistributedEventualCoordinator::new();
        let other_object = DfsObjectId::from_legacy_named("obj_other_file").expect("object");

        let left_intent = left
            .create_intent(
                file_system_id(),
                object_id(),
                Some(version("version_1")),
                version("version_2"),
                node("node_left"),
                vec![node("node_left"), node("node_replica")],
                policy(2, 2),
            )
            .expect("left intent");
        let right_intent = right
            .create_intent(
                file_system_id(),
                object_id(),
                Some(version("version_1")),
                version("version_3"),
                node("node_right"),
                vec![node("node_right"), node("node_replica")],
                policy(2, 2),
            )
            .expect("right intent");
        let same_node_other_object = same_node
            .create_intent(
                file_system_id(),
                other_object,
                Some(version("version_1")),
                version("version_2"),
                node("node_left"),
                vec![node("node_left"), node("node_replica")],
                policy(2, 2),
            )
            .expect("same node other object intent");

        assert_ne!(left_intent.intent_id, right_intent.intent_id);
        assert_ne!(left_intent.intent_id, same_node_other_object.intent_id);
        assert!(left_intent.intent_id.contains(node("node_left").as_str()));
        assert!(right_intent.intent_id.contains(node("node_right").as_str()));
    }

    #[test]
    fn commits_after_threshold_receipts() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let intent = create_intent(&mut coordinator, policy(2, 3));

        coordinator
            .record_durable_receipt(&intent.intent_id, node("node_a"))
            .expect("first receipt");
        coordinator
            .record_durable_receipt(&intent.intent_id, node("node_b"))
            .expect("second receipt");
        coordinator
            .commit_intent(&intent.intent_id)
            .expect("commit intent");
        let updated = coordinator
            .intent(&intent.intent_id)
            .expect("intent")
            .clone();

        assert_eq!(updated.state, DfsWriteIntentState::Committed);
        assert_eq!(
            coordinator.committed_version(&file_system_id(), &object_id()),
            Some(&version("version_2"))
        );
    }

    #[test]
    fn does_not_commit_after_too_few_receipts() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let intent = create_intent(&mut coordinator, policy(2, 3));

        let updated = coordinator
            .record_durable_receipt(&intent.intent_id, node("node_a"))
            .expect("first receipt");

        assert_eq!(updated.state, DfsWriteIntentState::AwaitingReceipts);
        assert_eq!(
            coordinator.committed_version(&file_system_id(), &object_id()),
            Some(&version("version_1"))
        );
    }

    #[test]
    fn duplicate_receipts_are_idempotent() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let intent = create_intent(&mut coordinator, policy(2, 3));

        coordinator
            .record_durable_receipt(&intent.intent_id, node("node_a"))
            .expect("first receipt");
        let updated = coordinator
            .record_durable_receipt(&intent.intent_id, node("node_a"))
            .expect("duplicate receipt");

        assert_eq!(updated.state, DfsWriteIntentState::AwaitingReceipts);
        assert_eq!(coordinator.durable_receipt_count(&intent.intent_id), 1);
        assert!(coordinator.repair_jobs().is_empty());
    }

    #[test]
    fn rejects_competing_pending_intent_for_same_object() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let pending = create_intent(&mut coordinator, policy(2, 3));

        let error = coordinator
            .create_intent(
                file_system_id(),
                object_id(),
                Some(version("version_1")),
                version("version_3"),
                node("node_writer"),
                vec![node("node_a"), node("node_b"), node("node_c")],
                policy(2, 3),
            )
            .expect_err("competing pending intent should be rejected");

        assert!(matches!(
            error,
            DfsDistributedError::PendingIntentConflict {
                pending_intent_id,
                pending_target_version,
                requested_target_version,
                ..
            } if pending_intent_id == pending.intent_id
                && pending_target_version == version("version_2")
                && requested_target_version == version("version_3")
        ));
    }

    #[test]
    fn schedules_repair_for_missing_desired_replicas() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let intent = create_intent(&mut coordinator, policy(2, 3));

        coordinator
            .record_durable_receipt(&intent.intent_id, node("node_a"))
            .expect("first receipt");
        coordinator
            .record_durable_receipt(&intent.intent_id, node("node_b"))
            .expect("second receipt");
        coordinator
            .commit_intent(&intent.intent_id)
            .expect("commit intent");

        assert_eq!(
            coordinator.repair_jobs(),
            &[DistributedRepairJob {
                intent_id: intent.intent_id,
                file_system_id: file_system_id(),
                object_id: object_id(),
                version: version("version_2"),
                missing_replica_node_id: node("node_c"),
            }]
        );
    }

    #[test]
    fn repair_receipt_clears_pending_repair_job() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let intent = create_intent(&mut coordinator, policy(1, 3));

        let committed = coordinator
            .record_durable_receipt(&intent.intent_id, node("node_a"))
            .expect("threshold receipt");
        assert_eq!(committed.state, DfsWriteIntentState::AwaitingReceipts);
        coordinator
            .commit_intent(&intent.intent_id)
            .expect("commit intent");
        assert_eq!(
            coordinator.intent(&intent.intent_id).expect("intent").state,
            DfsWriteIntentState::Committed
        );
        assert_eq!(coordinator.repair_jobs().len(), 2);

        coordinator
            .record_durable_receipt(&intent.intent_id, node("node_b"))
            .expect("repair receipt");
        assert_eq!(
            coordinator.repair_jobs(),
            &[DistributedRepairJob {
                intent_id: intent.intent_id,
                file_system_id: file_system_id(),
                object_id: object_id(),
                version: version("version_2"),
                missing_replica_node_id: node("node_c"),
            }]
        );
    }

    #[test]
    fn strict_confirmed_behavior_waits_for_all_desired_replicas() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let intent = create_intent(&mut coordinator, policy(3, 3));

        coordinator
            .record_durable_receipt(&intent.intent_id, node("node_a"))
            .expect("first receipt");
        let updated = coordinator
            .record_durable_receipt(&intent.intent_id, node("node_b"))
            .expect("second receipt");

        assert_eq!(updated.state, DfsWriteIntentState::AwaitingReceipts);
        assert!(coordinator.repair_jobs().is_empty());

        let committed = coordinator
            .record_durable_receipt(&intent.intent_id, node("node_c"))
            .expect("third receipt");

        assert_eq!(committed.state, DfsWriteIntentState::AwaitingReceipts);
        coordinator
            .commit_intent(&intent.intent_id)
            .expect("commit intent");
        assert_eq!(
            coordinator.intent(&intent.intent_id).expect("intent").state,
            DfsWriteIntentState::Committed
        );
        assert!(coordinator.repair_jobs().is_empty());
    }

    #[test]
    fn abort_closes_pending_confirmed_intent_without_advancing_version() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let intent = create_intent(&mut coordinator, policy(3, 3));

        coordinator
            .record_durable_receipt(&intent.intent_id, node("node_a"))
            .expect("first receipt");
        let aborted = coordinator
            .abort_intent(&intent.intent_id)
            .expect("abort intent");

        assert_eq!(aborted.state, DfsWriteIntentState::Aborted);
        assert_eq!(
            coordinator.committed_version(&file_system_id(), &object_id()),
            Some(&version("version_1"))
        );
        assert!(matches!(
            coordinator.record_durable_receipt(&intent.intent_id, node("node_b")),
            Err(DfsDistributedError::IntentClosed {
                state: DfsWriteIntentState::Aborted,
                ..
            })
        ));
    }

    #[test]
    fn confirmed_intent_can_downgrade_to_eventual_threshold() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let intent = create_intent(&mut coordinator, policy(3, 3));

        coordinator
            .record_durable_receipt(&intent.intent_id, node("node_a"))
            .expect("first receipt");
        let downgraded = coordinator
            .downgrade_intent_policy(&intent.intent_id, policy(1, 3))
            .expect("downgrade intent");

        assert_eq!(downgraded.state, DfsWriteIntentState::AwaitingReceipts);
        coordinator
            .commit_intent(&intent.intent_id)
            .expect("commit intent");
        assert_eq!(
            coordinator.intent(&intent.intent_id).expect("intent").state,
            DfsWriteIntentState::Committed
        );
        assert_eq!(
            coordinator.committed_version(&file_system_id(), &object_id()),
            Some(&version("version_2"))
        );
        assert_eq!(coordinator.repair_jobs().len(), 2);
    }

    #[test]
    fn rejects_stale_base_version_when_current_version_is_known() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let file_system_id = file_system_id();
        let object_id = object_id();
        coordinator.set_committed_version(
            file_system_id.clone(),
            object_id.clone(),
            version("version_2"),
        );

        let error = coordinator
            .create_intent(
                file_system_id,
                object_id.clone(),
                Some(version("version_1")),
                version("version_3"),
                node("node_writer"),
                vec![node("node_a"), node("node_b"), node("node_c")],
                policy(2, 3),
            )
            .expect_err("stale base should fail");

        assert!(matches!(
            error,
            DfsDistributedError::BaseVersionConflict {
                object_id: rejected_object_id,
                expected_base_version,
                supplied_base_version: Some(supplied_base_version),
            } if rejected_object_id == object_id
                && expected_base_version == version("version_2")
                && supplied_base_version == version("version_1")
        ));
    }
}
