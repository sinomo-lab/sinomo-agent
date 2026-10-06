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

#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "DfsFileSystemConfigWire")]
pub struct DfsFileSystemConfig {
    #[serde(skip)]
    pub metadata_format: DfsRevisionMetadataFormat,
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree_policies: Vec<DfsSubtreePolicy>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub retention_policies: std::collections::BTreeMap<String, DfsRetentionPolicy>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub module_metadata: std::collections::BTreeMap<String, DfsModuleMetadataValue>,
}

#[derive(Serialize)]
struct DfsFileSystemConfigRef<'a> {
    file_system_id: &'a DfsFileSystemId,
    display_name: &'a String,
    owner_node_id: &'a NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    subtree_policies: &'a Vec<DfsSubtreePolicy>,
    retention_policies: &'a std::collections::BTreeMap<String, DfsRetentionPolicy>,
    module_metadata: &'a std::collections::BTreeMap<String, DfsModuleMetadataValue>,
}

impl Serialize for DfsFileSystemConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.metadata_format == DfsRevisionMetadataFormat::Legacy {
            return crate::dfs::file_history::serialize_legacy_file_system(
                &self.file_system_id,
                &self.display_name,
                &self.owner_node_id,
                &self.subtree_policies,
                &self.retention_policies,
                &self.module_metadata,
                serializer,
            );
        }
        DfsFileSystemConfigRef {
            file_system_id: &self.file_system_id,
            display_name: &self.display_name,
            owner_node_id: &self.owner_node_id,
            subtree_policies: &self.subtree_policies,
            retention_policies: &self.retention_policies,
            module_metadata: &self.module_metadata,
        }
        .serialize(serializer)
    }
}

impl PartialEq for DfsFileSystemConfig {
    fn eq(&self, other: &Self) -> bool {
        self.file_system_id == other.file_system_id
            && self.display_name == other.display_name
            && self.owner_node_id == other.owner_node_id
            && self.subtree_policies == other.subtree_policies
            && self.retention_policies == other.retention_policies
            && self.module_metadata == other.module_metadata
    }
}
impl Eq for DfsFileSystemConfig {}

#[derive(Deserialize)]
struct DfsFileSystemConfigWire {
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree_policies: Vec<DfsSubtreePolicy>,
    #[serde(
        default,
        deserialize_with = "crate::dfs::metadata_records::deserialize_present_value"
    )]
    pub retention_policies: Option<std::collections::BTreeMap<String, DfsRetentionPolicy>>,
    #[serde(
        default,
        deserialize_with = "crate::dfs::metadata_records::deserialize_present_value"
    )]
    pub module_metadata: Option<std::collections::BTreeMap<String, DfsModuleMetadataValue>>,
    #[serde(default)]
    file_history: DfsFileHistoryPolicy,
}

impl TryFrom<DfsFileSystemConfigWire> for DfsFileSystemConfig {
    type Error = String;
    fn try_from(wire: DfsFileSystemConfigWire) -> Result<Self, Self::Error> {
        let metadata_format = if wire.retention_policies.is_some() || wire.module_metadata.is_some()
        {
            DfsRevisionMetadataFormat::NamespacedV6
        } else {
            DfsRevisionMetadataFormat::Legacy
        };
        let mut policies = wire.retention_policies.unwrap_or_default();
        let module_metadata = wire.module_metadata.unwrap_or_default();
        crate::dfs::file_history::migrate_legacy_retention_policy(
            &mut policies,
            wire.file_history,
        )?;
        validate_retention_policies(&policies)?;
        validate_module_metadata_map(&module_metadata)?;
        Ok(Self {
            metadata_format,
            file_system_id: wire.file_system_id,
            display_name: wire.display_name,
            owner_node_id: wire.owner_node_id,
            subtree_policies: wire.subtree_policies,
            retention_policies: policies,
            module_metadata,
        })
    }
}

impl From<&DfsFileSystem> for DfsFileSystemConfig {
    fn from(file_system: &DfsFileSystem) -> Self {
        Self {
            metadata_format: file_system.metadata_format,
            file_system_id: file_system.file_system_id.clone(),
            display_name: file_system.display_name.clone(),
            owner_node_id: file_system.owner_node_id.clone(),
            subtree_policies: file_system.subtree_policies.clone(),
            retention_policies: file_system.retention_policies.clone(),
            module_metadata: file_system.module_metadata.clone(),
        }
    }
}

impl From<DfsFileSystemConfig> for DfsFileSystem {
    fn from(config: DfsFileSystemConfig) -> Self {
        Self {
            metadata_format: config.metadata_format,
            file_system_id: config.file_system_id,
            display_name: config.display_name,
            owner_node_id: config.owner_node_id,
            subtree_policies: config.subtree_policies,
            retention_policies: config.retention_policies,
            module_metadata: config.module_metadata,
        }
    }
}
