//! Shared DFS wire values and validation; no storage or service runtime.

use crate::dfs::*;
use serde::Deserialize;
use serde::Serialize;
use sinomo_core::ClusterMemberState;
use sinomo_core::node::NodeId;

/// Sanitized classification of the identity carried by a placement permit.
/// Process-session and random holder ids remain private to the authority
/// implementation; operators receive only the fields needed to establish the
/// recovery class plus an opaque exact-generation token.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPlacementAuthorityPermitHolderKind {
    Identified,
    Partial,
    Legacy,
}

/// Read-only operator view of a non-expiring placement mutation permit.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementAuthorityPermitStatus {
    pub file_system_id: DfsFileSystemId,
    pub job_id: String,
    pub holder_kind: DfsPlacementAuthorityPermitHolderKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub holder_cluster_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub holder_node_id: Option<NodeId>,
    /// Opaque token bound to both the exact encoded permit and its KV
    /// generation. Recovery must echo this value unchanged.
    pub recovery_token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPlacementAuthorityRecoveryRefusal {
    OwnerStoppedAcknowledgementRequired,
    CurrentLocalNode,
    IncompleteHolderIdentity,
    ForeignCluster,
    HolderMembershipPresent { state: ClusterMemberState },
    InvalidRemovedMembershipEvidence,
    BreakGlassAcknowledgementRequired,
    NormalRecoveryRequired,
}

/// Outcome of an operator-authorized placement permit recovery attempt.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum DfsPlacementAuthorityRecoveryOutcome {
    Missing,
    Cleared,
    Changed,
    Refused {
        reason: DfsPlacementAuthorityRecoveryRefusal,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileSystemConfig {
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree_policies: Vec<DfsSubtreePolicy>,
}

impl From<&DfsFileSystem> for DfsFileSystemConfig {
    fn from(file_system: &DfsFileSystem) -> Self {
        Self {
            file_system_id: file_system.file_system_id.clone(),
            display_name: file_system.display_name.clone(),
            owner_node_id: file_system.owner_node_id.clone(),
            subtree_policies: file_system.subtree_policies.clone(),
        }
    }
}

impl From<DfsFileSystemConfig> for DfsFileSystem {
    fn from(config: DfsFileSystemConfig) -> Self {
        Self {
            file_system_id: config.file_system_id,
            display_name: config.display_name,
            owner_node_id: config.owner_node_id,
            subtree_policies: config.subtree_policies,
        }
    }
}
