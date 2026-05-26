//! Distributed eventual object protocol for DFS.

use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
use serde::{Deserialize, Serialize};
use sinomo_node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicationPolicy {
    pub min_commit_replicas: usize,
    pub desired_replicas: usize,
}

impl DfsReplicationPolicy {
    pub fn new(
        min_commit_replicas: usize,
        desired_replicas: usize,
    ) -> Result<Self, DfsDistributedError> {
        let policy = Self {
            min_commit_replicas,
            desired_replicas,
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

    fn validate(&self) -> Result<(), DfsDistributedError> {
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

        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicaCandidate {
    pub node_id: NodeId,
    pub weight: u32,
}

impl DfsReplicaCandidate {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id, weight: 1 }
    }

    pub fn weighted(node_id: NodeId, weight: u32) -> Result<Self, DfsDistributedError> {
        if weight == 0 {
            return Err(DfsDistributedError::InvalidReplicaSet {
                reason: "replica candidate weight must be greater than zero".to_string(),
            });
        }
        Ok(Self { node_id, weight })
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

pub fn plan_replicas(
    request: &DfsReplicaPlacementRequest,
) -> Result<DfsReplicaPlacementPlan, DfsDistributedError> {
    request.policy.validate()?;
    validate_candidate_set(&request.candidates, &request.policy)?;

    let mut ranked = request.candidates.clone();
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

    let desired_replica_nodes: Vec<NodeId> = ranked
        .iter()
        .take(request.policy.desired_replicas)
        .map(|candidate| candidate.node_id.clone())
        .collect();
    let preferred_commit_nodes = desired_replica_nodes
        .iter()
        .take(request.policy.min_commit_replicas)
        .cloned()
        .collect();

    Ok(DfsReplicaPlacementPlan {
        desired_replica_nodes,
        preferred_commit_nodes,
    })
}

pub fn validate_replication_plan(
    desired_replica_nodes: &[NodeId],
    policy: &DfsReplicationPolicy,
) -> Result<(), DfsDistributedError> {
    policy.validate()?;
    validate_replica_set(desired_replica_nodes, policy)
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
    receipts: BTreeMap<(String, NodeId), DistributedReplicaReceipt>,
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
        self.validate_new_intent(
            &file_system_id,
            &object_id,
            base_version.as_ref(),
            &target_version,
            &desired_replica_nodes,
            &policy,
        )?;

        let intent_id = self.next_intent_id(&requesting_node_id);
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
            state: DfsWriteIntentState::AwaitingReceipts,
        };
        self.intents.insert(intent_id, intent.clone());
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

        if self.intents.values().any(|intent| {
            intent.file_system_id == *file_system_id
                && intent.object_id == *object_id
                && intent.target_version == *target_version
                && matches!(
                    intent.state,
                    DfsWriteIntentState::AwaitingReceipts | DfsWriteIntentState::Committed
                )
        }) {
            return Err(DfsDistributedError::DuplicateIntent {
                object_id: object_id.clone(),
                target_version: target_version.clone(),
            });
        }

        if let Some(pending) = self.intents.values().find(|intent| {
            intent.file_system_id == *file_system_id
                && intent.object_id == *object_id
                && matches!(intent.state, DfsWriteIntentState::AwaitingReceipts)
        }) {
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

    pub fn restore_intent(
        &mut self,
        intent: DistributedWriteIntent,
    ) -> Result<(), DfsDistributedError> {
        validate_replication_plan(&intent.desired_replica_nodes, &intent.policy)?;
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
        self.intents.insert(intent.intent_id.clone(), intent);
        self.rebuild_repair_jobs();
        Ok(())
    }

    pub fn restore_durable_receipt(
        &mut self,
        intent_id: &str,
        replica_node_id: NodeId,
    ) -> Result<(), DfsDistributedError> {
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
        self.receipts.insert(
            (intent_id.to_string(), replica_node_id.clone()),
            DistributedReplicaReceipt {
                intent_id: intent_id.to_string(),
                file_system_id: intent.file_system_id,
                object_id: intent.object_id,
                version: intent.target_version,
                replica_node_id,
                state: DfsReplicaReceiptState::Durable,
            },
        );
        if self.is_commit_ready(intent_id)?
            && self
                .intents
                .get(intent_id)
                .is_some_and(|intent| matches!(intent.state, DfsWriteIntentState::AwaitingReceipts))
        {
            self.commit_intent(intent_id)?;
        } else {
            self.rebuild_repair_jobs();
        }
        Ok(())
    }

    pub fn receipts_for_intent(&self, intent_id: &str) -> Vec<&DistributedReplicaReceipt> {
        self.receipts
            .iter()
            .filter_map(|((receipt_intent_id, _), receipt)| {
                (receipt_intent_id == intent_id).then_some(receipt)
            })
            .collect()
    }

    pub fn durable_receipt_count(&self, intent_id: &str) -> usize {
        self.durable_receipt_nodes(intent_id).len()
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
        self.receipts
            .insert((intent_id.to_string(), replica_node_id.clone()), receipt);
        self.repair_jobs.retain(|job| {
            job.intent_id != intent_id || job.missing_replica_node_id != replica_node_id
        });

        if self.is_commit_ready(intent_id)? {
            self.commit_intent(intent_id)?;
        }

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

    pub fn abort_intent(
        &mut self,
        intent_id: &str,
    ) -> Result<DistributedWriteIntent, DfsDistributedError> {
        let intent =
            self.intents
                .get_mut(intent_id)
                .ok_or_else(|| DfsDistributedError::UnknownIntent {
                    intent_id: intent_id.to_string(),
                })?;
        if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
            return Err(DfsDistributedError::IntentClosed {
                intent_id: intent_id.to_string(),
                state: intent.state.clone(),
            });
        }
        intent.state = DfsWriteIntentState::Aborted;
        self.repair_jobs.retain(|job| job.intent_id != intent_id);
        Ok(intent.clone())
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

        let commit_ready = self.durable_receipt_count(intent_id) >= policy.min_commit_replicas;
        let intent =
            self.intents
                .get_mut(intent_id)
                .ok_or_else(|| DfsDistributedError::UnknownIntent {
                    intent_id: intent_id.to_string(),
                })?;
        intent.policy = policy;

        if commit_ready {
            self.commit_intent(intent_id)?;
        }

        self.intents
            .get(intent_id)
            .cloned()
            .ok_or_else(|| DfsDistributedError::UnknownIntent {
                intent_id: intent_id.to_string(),
            })
    }

    fn commit_intent(&mut self, intent_id: &str) -> Result<(), DfsDistributedError> {
        let durable_nodes = self.durable_receipt_nodes(intent_id);
        let intent =
            self.intents
                .get_mut(intent_id)
                .ok_or_else(|| DfsDistributedError::UnknownIntent {
                    intent_id: intent_id.to_string(),
                })?;

        if matches!(intent.state, DfsWriteIntentState::Committed) {
            return Ok(());
        }

        intent.state = DfsWriteIntentState::Committed;
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

    fn durable_receipt_nodes(&self, intent_id: &str) -> BTreeSet<NodeId> {
        self.receipts
            .iter()
            .filter_map(|((receipt_intent_id, replica_node_id), receipt)| {
                (receipt_intent_id == intent_id && receipt.state == DfsReplicaReceiptState::Durable)
                    .then_some(replica_node_id.clone())
            })
            .collect()
    }

    fn next_intent_id(&mut self, requesting_node_id: &NodeId) -> String {
        let intent_id = format!(
            "dfs_intent_{}_{}",
            requesting_node_id.as_str(),
            self.next_intent_id
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
        let committed: Vec<DistributedWriteIntent> = self
            .intents
            .values()
            .filter(|intent| matches!(intent.state, DfsWriteIntentState::Committed))
            .cloned()
            .collect();
        for intent in committed {
            let durable_nodes = self.durable_receipt_nodes(&intent.intent_id);
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
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ObjectKey {
    file_system_id: DfsFileSystemId,
    object_id: DfsObjectId,
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
    if candidates.len() < policy.desired_replicas {
        return Err(DfsDistributedError::InvalidReplicaSet {
            reason: "not enough replica candidates for desired_replicas".to_string(),
        });
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
    fn intent_ids_include_requesting_node_to_avoid_cross_node_collisions() {
        let mut left = DistributedEventualCoordinator::new();
        let mut right = DistributedEventualCoordinator::new();

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

        assert_ne!(left_intent.intent_id, right_intent.intent_id);
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
        let updated = coordinator
            .record_durable_receipt(&intent.intent_id, node("node_b"))
            .expect("second receipt");

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
        assert_eq!(committed.state, DfsWriteIntentState::Committed);
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

        assert_eq!(committed.state, DfsWriteIntentState::Committed);
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

        assert_eq!(downgraded.state, DfsWriteIntentState::Committed);
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
