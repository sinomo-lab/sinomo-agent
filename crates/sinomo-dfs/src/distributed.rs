//! Distributed eventual object protocol for DFS.

use serde::{Deserialize, Serialize};
use sinomo_core::{DfsObjectId, DfsVersion, NodeId, WorkspaceId};
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
#[serde(rename_all = "kebab-case")]
pub enum DfsWriteIntentState {
    AwaitingReceipts,
    Committed,
    Rejected,
    Conflict,
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
    pub workspace_id: WorkspaceId,
    pub object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_version: Option<DfsVersion>,
    pub target_version: DfsVersion,
    pub requesting_node_id: NodeId,
    pub desired_replica_nodes: Vec<NodeId>,
    pub policy: DfsReplicationPolicy,
    pub state: DfsWriteIntentState,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DistributedReplicaReceipt {
    pub intent_id: String,
    pub workspace_id: WorkspaceId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub replica_node_id: NodeId,
    pub state: DfsReplicaReceiptState,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DistributedRepairJob {
    pub intent_id: String,
    pub workspace_id: WorkspaceId,
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
        workspace_id: WorkspaceId,
        object_id: DfsObjectId,
        version: DfsVersion,
    ) {
        self.committed_versions
            .insert(ObjectKey::new(workspace_id, object_id), version);
    }

    pub fn committed_version(
        &self,
        workspace_id: &WorkspaceId,
        object_id: &DfsObjectId,
    ) -> Option<&DfsVersion> {
        self.committed_versions
            .get(&ObjectKey::from_refs(workspace_id, object_id))
    }

    pub fn create_intent(
        &mut self,
        workspace_id: WorkspaceId,
        object_id: DfsObjectId,
        base_version: Option<DfsVersion>,
        target_version: DfsVersion,
        requesting_node_id: NodeId,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<DistributedWriteIntent, DfsDistributedError> {
        policy.validate()?;
        validate_replica_set(&desired_replica_nodes, &policy)?;

        let key = ObjectKey::new(workspace_id.clone(), object_id.clone());
        if let Some(committed_version) = self.committed_versions.get(&key) {
            if Some(committed_version) != base_version.as_ref() {
                return Err(DfsDistributedError::BaseVersionConflict {
                    object_id,
                    expected_base_version: committed_version.clone(),
                    supplied_base_version: base_version,
                });
            }
        }

        if self.intents.values().any(|intent| {
            intent.workspace_id == workspace_id
                && intent.object_id == object_id
                && intent.target_version == target_version
                && matches!(
                    intent.state,
                    DfsWriteIntentState::AwaitingReceipts | DfsWriteIntentState::Committed
                )
        }) {
            return Err(DfsDistributedError::DuplicateIntent {
                object_id,
                target_version,
            });
        }

        let intent_id = self.next_intent_id();
        let intent = DistributedWriteIntent {
            intent_id: intent_id.clone(),
            workspace_id,
            object_id,
            base_version,
            target_version,
            requesting_node_id,
            desired_replica_nodes,
            policy,
            state: DfsWriteIntentState::AwaitingReceipts,
        };
        self.intents.insert(intent_id, intent.clone());
        Ok(intent)
    }

    pub fn intent(&self, intent_id: &str) -> Option<&DistributedWriteIntent> {
        self.intents.get(intent_id)
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
            DfsWriteIntentState::Rejected | DfsWriteIntentState::Conflict
        ) {
            return Err(DfsDistributedError::IntentClosed {
                intent_id: intent_id.to_string(),
                state: intent.state,
            });
        }

        let receipt = DistributedReplicaReceipt {
            intent_id: intent_id.to_string(),
            workspace_id: intent.workspace_id.clone(),
            object_id: intent.object_id.clone(),
            version: intent.target_version.clone(),
            replica_node_id: replica_node_id.clone(),
            state: DfsReplicaReceiptState::Durable,
        };
        self.receipts
            .insert((intent_id.to_string(), replica_node_id), receipt);

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
            ObjectKey::new(intent.workspace_id.clone(), intent.object_id.clone()),
            intent.target_version.clone(),
        );

        for missing_replica_node_id in intent
            .desired_replica_nodes
            .iter()
            .filter(|node_id| !durable_nodes.contains(*node_id))
        {
            self.repair_jobs.push(DistributedRepairJob {
                intent_id: intent.intent_id.clone(),
                workspace_id: intent.workspace_id.clone(),
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

    fn next_intent_id(&mut self) -> String {
        let intent_id = format!("dfs_intent_{}", self.next_intent_id);
        self.next_intent_id += 1;
        intent_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ObjectKey {
    workspace_id: WorkspaceId,
    object_id: DfsObjectId,
}

impl ObjectKey {
    fn new(workspace_id: WorkspaceId, object_id: DfsObjectId) -> Self {
        Self {
            workspace_id,
            object_id,
        }
    }

    fn from_refs(workspace_id: &WorkspaceId, object_id: &DfsObjectId) -> Self {
        Self {
            workspace_id: workspace_id.clone(),
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

    if desired_replica_nodes.len() < policy.desired_replicas {
        return Err(DfsDistributedError::InvalidReplicaSet {
            reason: "desired_replica_nodes is smaller than desired_replicas".to_string(),
        });
    }

    Ok(())
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

    fn workspace_id() -> WorkspaceId {
        WorkspaceId::new("workspace_repo").expect("workspace")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::new("obj_file").expect("object")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::new(value).expect("version")
    }

    fn node(value: &str) -> NodeId {
        NodeId::new(value).expect("node")
    }

    fn policy(min_commit_replicas: usize, desired_replicas: usize) -> DfsReplicationPolicy {
        DfsReplicationPolicy::new(min_commit_replicas, desired_replicas).expect("policy")
    }

    fn create_intent(
        coordinator: &mut DistributedEventualCoordinator,
        policy: DfsReplicationPolicy,
    ) -> DistributedWriteIntent {
        let workspace_id = workspace_id();
        let object_id = object_id();
        coordinator.set_committed_version(
            workspace_id.clone(),
            object_id.clone(),
            version("version_1"),
        );
        coordinator
            .create_intent(
                workspace_id,
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
            coordinator.committed_version(&workspace_id(), &object_id()),
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
            coordinator.committed_version(&workspace_id(), &object_id()),
            Some(&version("version_1"))
        );
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
                workspace_id: workspace_id(),
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
    fn rejects_stale_base_version_when_current_version_is_known() {
        let mut coordinator = DistributedEventualCoordinator::new();
        let workspace_id = workspace_id();
        let object_id = object_id();
        coordinator.set_committed_version(
            workspace_id.clone(),
            object_id.clone(),
            version("version_2"),
        );

        let error = coordinator
            .create_intent(
                workspace_id,
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
