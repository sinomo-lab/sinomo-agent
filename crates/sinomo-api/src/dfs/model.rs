//! Shared DFS wire values and validation; no storage or service runtime.

use crate::dfs::*;
use serde::Deserialize;
use serde::Serialize;
use sinomo_core::DfsObjectPolicy;
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;

/// Version of the compact activation record, independently of storage format 11.
pub const DFS_NAMESPACE_COUNT_ACTIVATION_SCHEMA_VERSION: u16 = 1;

/// Published only by the native complete-cohort maintenance CAS. This compact
/// configuration fact is not an inode counter or a local installation flag.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsNamespaceCountActivation {
    pub schema_version: u16,
    pub initialization_id: String,
    pub cohort_id: String,
    pub activation_digest: String,
}
impl DfsNamespaceCountActivation {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != DFS_NAMESPACE_COUNT_ACTIVATION_SCHEMA_VERSION
            || [&self.initialization_id, &self.cohort_id].iter().any(|value|
                value.is_empty() || value.len() > 1024 || value.contains('\0'))
            || !self.activation_digest.strip_prefix("blake3:").is_some_and(|hash|
                hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
        { return Err("invalid namespace-count activation configuration".into()); }
        Ok(())
    }
}

/// A DFS namespace and its default ownership metadata.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(try_from = "DfsFileSystemWire")]
pub struct DfsFileSystem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace_count_activation: Option<DfsNamespaceCountActivation>,
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
struct DfsFileSystemWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace_count_activation: Option<DfsNamespaceCountActivation>,
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree_policies: Vec<DfsSubtreePolicy>,
    pub retention_policies: std::collections::BTreeMap<String, DfsRetentionPolicy>,
    pub module_metadata: std::collections::BTreeMap<String, DfsModuleMetadataValue>,
}

impl TryFrom<DfsFileSystemWire> for DfsFileSystem {
    type Error = String;
    fn try_from(wire: DfsFileSystemWire) -> Result<Self, Self::Error> {
        let policies = wire.retention_policies;
        let module_metadata = wire.module_metadata;
        if let Some(activation) = &wire.namespace_count_activation { activation.validate()?; }
        validate_retention_policies(&policies)?;
        validate_module_metadata_map(&module_metadata)?;
        Ok(Self {
            namespace_count_activation: wire.namespace_count_activation,
            file_system_id: wire.file_system_id,
            display_name: wire.display_name,
            owner_node_id: wire.owner_node_id,
            subtree_policies: wire.subtree_policies,
            retention_policies: policies,
            module_metadata,
        })
    }
}

/// Policy override that applies to objects below a path prefix.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsSubtreePolicy {
    pub path_prefix: String,
    pub policy: DfsObjectPolicy,
    /// Whether the override affects only `path_prefix` or is inherited by all
    /// descendants.
    pub scope: DfsPlacementScope,
    /// Explicit commit and placement replica counts.
    pub min_commit_replicas: usize,
    pub desired_replicas: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_node_ids: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forbidden_node_ids: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub min_distinct_zones: usize,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPlacementScope {
    SelfOnly,
    #[default]
    Subtree,
}

impl DfsSubtreePolicy {
    pub fn new(
        path_prefix: impl Into<String>,
        policy: DfsObjectPolicy,
        min_commit_replicas: usize,
        desired_replicas: usize,
    ) -> Self {
        Self {
            path_prefix: path_prefix.into(),
            policy,
            scope: DfsPlacementScope::Subtree,
            min_commit_replicas,
            desired_replicas,
            required_node_ids: Vec::new(),
            forbidden_node_ids: Vec::new(),
            min_distinct_zones: 0,
        }
    }
}

impl DfsFileSystem {
    pub fn sifs(owner_node_id: NodeId) -> Self {
        Self {
            namespace_count_activation: None,
            file_system_id: sifs_file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id,
            subtree_policies: Vec::new(),
            retention_policies: Default::default(),
            module_metadata: Default::default(),
        }
    }
}

/// Current metadata for one DFS object in a filesystem.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsObjectMetadata {
    pub object_id: DfsObjectId,
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    pub kind: DfsObjectKind,
    /// Authoritative number of namespace links to this canonical object.
    /// Directories include their two implicit links and immediate subdirectories.
    /// Absent on legacy records and hardlink markers; absence is not a count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace_link_count: Option<u64>,
    /// Storage-level provenance for a tombstone that only evicts a physical
    /// source-shard copy after a cross-shard move. `None` means an authoritative
    /// namespace deletion.
    ///
    /// This distinction is part of durable metadata, rather than an inference
    /// from timestamps or paths: a source eviction is intentionally newer than
    /// the source live record, but it must never delete the live object already
    /// installed on `target_shard_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tombstone_provenance: Option<DfsTombstoneProvenance>,
    pub owner_node_id: NodeId,
    pub current_locations: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_target_object_id: Option<DfsObjectId>,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub accessed_at_unix_nanos: u64,
    pub modified_at_unix_nanos: u64,
    pub created_at_unix_nanos: u64,
    pub changed_at_unix_nanos: u64,
    pub version: DfsVersion,
    pub policy: DfsObjectPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified_by_event_id: Option<EventId>,
}

/// Logical kind of a DFS object.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsObjectKind {
    File,
    Directory,
    Symlink,
    Hardlink,
    Artifact,
    Mount,
    Tombstone,
}

impl DfsObjectKind {
    /// Initial count for a newly created namespace object, never a legacy default.
    pub const fn initial_namespace_link_count(&self) -> Option<u64> {
        match self {
            Self::Directory => Some(2),
            Self::Hardlink => None,
            Self::Tombstone => Some(0),
            _ => Some(1),
        }
    }
}

/// Why a durable DFS tombstone exists.
///
/// Ordinary namespace deletes use `None` on
/// [`DfsObjectMetadata::tombstone_provenance`]. Only physical source eviction
/// needs an explicit marker because it is not authoritative deletion evidence.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsTombstoneProvenance {
    SourceEviction {
        source_shard_id: crate::dfs::DfsMetadataShardId,
        target_shard_id: crate::dfs::DfsMetadataShardId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pin_transfer_id: Option<String>,
    },
}

fn is_zero_usize(value: &usize) -> bool {
    *value == 0
}

#[cfg(test)]
mod namespace_count_activation_tests {
    use super::*;

    #[test]
    fn activation_is_preserved_by_both_filesystem_projections_and_legacy_none_is_omitted() {
        let mut filesystem = DfsFileSystem::sifs(NodeId::generate());
        assert!(serde_json::to_value(&filesystem).unwrap().get("namespace_count_activation").is_none());
        let legacy = DfsFileSystemConfig::from(&filesystem);
        assert!(serde_json::to_value(&legacy).unwrap().get("namespace_count_activation").is_none());
        filesystem.namespace_count_activation = Some(DfsNamespaceCountActivation {
            schema_version: DFS_NAMESPACE_COUNT_ACTIVATION_SCHEMA_VERSION,
            initialization_id: "complete-cut".into(), cohort_id: "cohort".into(),
            activation_digest: format!("blake3:{}", "a".repeat(64)),
        });
        let config = DfsFileSystemConfig::from(&filesystem);
        assert_eq!(DfsFileSystem::from(config.clone()), filesystem);
        let value = serde_json::to_value(&config).unwrap();
        assert_eq!(serde_json::from_value::<DfsFileSystemConfig>(value.clone()).unwrap(), config);
        let mut malformed = value;
        malformed["namespace_count_activation"]["schema_version"] = serde_json::json!(11);
        assert!(serde_json::from_value::<DfsFileSystemConfig>(malformed).is_err(),
            "activation record schema1 is distinct from metadata storage format11");
    }
}
