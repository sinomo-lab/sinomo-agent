//! Shared DFS wire values and validation; no storage or service runtime.

use crate::dfs::*;
use serde::Deserialize;
use serde::Serialize;
use sinomo_core::node::NodeId;
use std::collections::BTreeMap;
use thiserror::Error;

/// The one rule for W and N: W never exceeds N. A policy that states more
/// required copies (W) than desired copies (N) desires W copies; W is never
/// lowered. Every replication, placement and subtree policy applies this when
/// it is constructed or decoded, so no policy is refused, weakened or left
/// pending because it stated W > N.
pub const fn normalized_replica_counts(
    min_commit_replicas: usize,
    desired_replicas: usize,
) -> (usize, usize) {
    if desired_replicas < min_commit_replicas {
        (min_commit_replicas, min_commit_replicas)
    } else {
        (min_commit_replicas, desired_replicas)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(from = "DfsReplicationPolicyFields")]
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

/// The decoded fields of a [`DfsReplicationPolicy`], before normalization.
#[derive(Deserialize)]
struct DfsReplicationPolicyFields {
    min_commit_replicas: usize,
    desired_replicas: usize,
    #[serde(default)]
    required_node_ids: Vec<NodeId>,
    #[serde(default)]
    forbidden_node_ids: Vec<NodeId>,
    #[serde(default)]
    min_distinct_zones: usize,
    #[serde(default)]
    node_zones: BTreeMap<NodeId, String>,
}

impl From<DfsReplicationPolicyFields> for DfsReplicationPolicy {
    fn from(fields: DfsReplicationPolicyFields) -> Self {
        let (min_commit_replicas, desired_replicas) =
            normalized_replica_counts(fields.min_commit_replicas, fields.desired_replicas);
        Self {
            min_commit_replicas,
            desired_replicas,
            required_node_ids: fields.required_node_ids,
            forbidden_node_ids: fields.forbidden_node_ids,
            min_distinct_zones: fields.min_distinct_zones,
            node_zones: fields.node_zones,
        }
    }
}

impl DfsReplicationPolicy {
    /// A policy with `min_commit_replicas` required and `desired_replicas`
    /// desired copies; N is raised to W when it states less.
    pub fn new(
        min_commit_replicas: usize,
        desired_replicas: usize,
    ) -> Result<Self, DfsDistributedError> {
        let (min_commit_replicas, desired_replicas) =
            normalized_replica_counts(min_commit_replicas, desired_replicas);
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

    /// Change W/N while retaining node and zone constraints; N is raised to W
    /// when it states less.
    pub fn with_counts(
        &self,
        min_commit_replicas: usize,
        desired_replicas: usize,
    ) -> Result<Self, DfsDistributedError> {
        let (min_commit_replicas, desired_replicas) =
            normalized_replica_counts(min_commit_replicas, desired_replicas);
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

        // Unreachable through construction and decoding, which normalize.
        if self.min_commit_replicas > self.desired_replicas {
            return Err(DfsDistributedError::InvalidPolicy {
                reason: "min_commit_replicas exceeds desired_replicas in a policy that was not normalized"
                    .to_string(),
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
pub struct DfsReplicaPlacementPlan {
    pub desired_replica_nodes: Vec<NodeId>,
    pub preferred_commit_nodes: Vec<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicaPlacementDecision {
    pub node_id: NodeId,
    /// Eligibility computed by the owning selector. This is an observed
    /// decision, never client-provided authority; older previews default false.
    #[serde(default)]
    pub eligible: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    pub selected: bool,
    pub preferred_for_commit: bool,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_health: Option<crate::dfs::DfsHealthState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_freshness: Option<crate::dfs::DfsObservationFreshness>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usable_capacity_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_state: Option<crate::dfs::DfsPlacementPathState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_latency_micros: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_throughput_bytes_per_second: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_metering: Option<crate::dfs::DfsNetworkMetering>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_pressure: Option<crate::dfs::DfsResourcePressure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation_freshness: Option<crate::dfs::DfsObservationFreshness>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReplicaPlacementExplanation {
    pub plan: DfsReplicaPlacementPlan,
    pub represented_zone_count: usize,
    pub decisions: Vec<DfsReplicaPlacementDecision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_generation: Option<u64>,
    #[serde(
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub snapshot_observed_at: Option<time::OffsetDateTime>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsWriteIntentState {
    AwaitingReceipts,
    Committed,
    Rejected,

    Aborted,
    PreconditionFailed {
        failure: DfsWritePreconditionFailure,
    },
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
mod normalization_tests {
    use super::*;

    #[test]
    fn a_policy_stating_w_above_n_desires_w_copies_and_keeps_its_w() {
        assert_eq!(normalized_replica_counts(3, 2), (3, 3));
        assert_eq!(normalized_replica_counts(1, 2), (1, 2));
        assert_eq!(normalized_replica_counts(2, 2), (2, 2));
        let policy = DfsReplicationPolicy::new(3, 2).unwrap();
        assert_eq!((policy.min_commit_replicas, policy.desired_replicas), (3, 3));
        let changed = policy.with_counts(4, 1).unwrap();
        assert_eq!((changed.min_commit_replicas, changed.desired_replicas), (4, 4));
        let decoded: DfsReplicationPolicy = serde_json::from_value(serde_json::json!({
            "min_commit_replicas": 3,
            "desired_replicas": 1,
        }))
        .unwrap();
        assert_eq!((decoded.min_commit_replicas, decoded.desired_replicas), (3, 3));
        // An already consistent policy keeps its exact encoding.
        let plain = DfsReplicationPolicy::new(1, 2).unwrap();
        let encoded = serde_json::to_vec(&plain).unwrap();
        let again: DfsReplicationPolicy = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(again, plain);
        assert_eq!(serde_json::to_vec(&again).unwrap(), encoded);
        // W = 0 is still refused: only W > N is normalized.
        assert!(DfsReplicationPolicy::new(0, 2).is_err());
    }

    #[test]
    fn placement_and_subtree_policies_normalize_on_construction_and_decode() {
        let confirmed = crate::dfs::DfsPlacementPolicy::new(
            sinomo_core::DfsObjectPolicy::ReplicatedConfirmed,
            3,
            2,
        )
        .unwrap();
        assert_eq!((confirmed.min_commit_replicas, confirmed.desired_replicas), (3, 3));
        let decoded: crate::dfs::DfsPlacementPolicy = serde_json::from_value(serde_json::json!({
            "consistency": "replicated-eventual",
            "min_commit_replicas": 2,
            "desired_replicas": 1,
        }))
        .unwrap();
        assert_eq!((decoded.min_commit_replicas, decoded.desired_replicas), (2, 2));
        decoded.validate().unwrap();
        let rule = crate::dfs::DfsSubtreePolicy::new(
            "/team",
            sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            3,
            1,
        );
        assert_eq!((rule.min_commit_replicas, rule.desired_replicas), (3, 3));
        let decoded: crate::dfs::DfsSubtreePolicy = serde_json::from_value(serde_json::json!({
            "path_prefix": "/team",
            "policy": "replicated-eventual",
            "scope": "subtree",
            "min_commit_replicas": 2,
            "desired_replicas": 1,
        }))
        .unwrap();
        assert_eq!((decoded.min_commit_replicas, decoded.desired_replicas), (2, 2));
    }
}
