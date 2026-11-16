//! Shared DFS wire values and validation; no storage or service runtime.

use crate::dfs::*;
use serde::Deserialize;
use serde::Serialize;
use sinomo_core::ClusterMemberState;
use sinomo_core::node::NodeId;

/// Read-only operator view of a non-expiring placement mutation permit.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsPlacementAuthorityPermitStatus {
    pub file_system_id: DfsFileSystemId,
    pub job_id: String,
    pub holder_cluster_id: String,
    pub holder_node_id: NodeId,
    /// Opaque token bound to both the exact encoded permit and its KV
    /// generation. Recovery must echo this value unchanged.
    pub recovery_token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPlacementAuthorityRecoveryRefusal {
    OwnerStoppedAcknowledgementRequired,
    CurrentLocalNode,
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
#[serde(try_from = "DfsFileSystemConfigWire")]
pub struct DfsFileSystemConfig {
    /// Monotonic generation of this filesystem's complete metadata-shard catalog.
    #[serde(default, skip_serializing_if = "is_zero_catalog_generation")]
    pub catalog_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_storage: Option<DfsMetadataStorageState>,
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree_policies: Vec<DfsSubtreePolicy>,
    pub retention_policies: std::collections::BTreeMap<String, DfsRetentionPolicy>,
    pub module_metadata: std::collections::BTreeMap<String, DfsModuleMetadataValue>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DfsFileSystemConfigWire {
    /// Monotonic generation of this filesystem's complete metadata-shard catalog.
    #[serde(default, skip_serializing_if = "is_zero_catalog_generation")]
    pub catalog_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_storage: Option<DfsMetadataStorageState>,
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree_policies: Vec<DfsSubtreePolicy>,
    pub retention_policies: std::collections::BTreeMap<String, DfsRetentionPolicy>,
    pub module_metadata: std::collections::BTreeMap<String, DfsModuleMetadataValue>,
}

impl TryFrom<DfsFileSystemConfigWire> for DfsFileSystemConfig {
    type Error = String;
    fn try_from(wire: DfsFileSystemConfigWire) -> Result<Self, Self::Error> {
        let policies = wire.retention_policies;
        let module_metadata = wire.module_metadata;
        if let Some(activation) = &wire.metadata_storage {
            activation.validate_at_catalog_generation(wire.catalog_generation)?;
        }
        validate_retention_policies(&policies)?;
        validate_module_metadata_map(&module_metadata)?;
        Ok(Self {
            catalog_generation: wire.catalog_generation,
            metadata_storage: wire.metadata_storage,
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
            catalog_generation: file_system.catalog_generation,
            metadata_storage: file_system.metadata_storage.clone(),
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
            catalog_generation: config.catalog_generation,
            metadata_storage: config.metadata_storage,
            file_system_id: config.file_system_id,
            display_name: config.display_name,
            owner_node_id: config.owner_node_id,
            subtree_policies: config.subtree_policies,
            retention_policies: config.retention_policies,
            module_metadata: config.module_metadata,
        }
    }
}

#[cfg(test)]
mod placement_authority_status_schema_tests {
    use super::DfsPlacementAuthorityPermitStatus;

    #[test]
    fn current_permit_status_requires_both_holder_identities_and_rejects_retired_kind() {
        let value = serde_json::json!({
            "file_system_id": sinomo_core::Id::derived("dfs-permit-status-schema", b"fs").to_string(),
            "job_id": "current-job",
            "holder_cluster_id": sinomo_core::Id::derived("dfs-permit-status-schema", b"cluster").to_string(),
            "holder_node_id": sinomo_core::Id::derived("dfs-permit-status-schema", b"node").to_string(),
            "recovery_token": "opaque-observed-token"
        });
        let current: DfsPlacementAuthorityPermitStatus =
            serde_json::from_value(value.clone()).expect("complete current read-only status");
        assert_eq!(
            serde_json::to_value(current).expect("current status"),
            value
        );
        for field in ["holder_cluster_id", "holder_node_id"] {
            let mut missing = value.clone();
            missing
                .as_object_mut()
                .expect("status object")
                .remove(field);
            assert!(serde_json::from_value::<DfsPlacementAuthorityPermitStatus>(missing).is_err());
            let mut null = value.clone();
            null[field] = serde_json::Value::Null;
            assert!(serde_json::from_value::<DfsPlacementAuthorityPermitStatus>(null).is_err());
        }
        for kind in ["legacy", "partial", "identified"] {
            let mut retired = value.clone();
            retired["holder_kind"] = kind.into();
            assert!(serde_json::from_value::<DfsPlacementAuthorityPermitStatus>(retired).is_err());
        }
    }
}

fn is_zero_catalog_generation(value: &u64) -> bool { *value == 0 }
