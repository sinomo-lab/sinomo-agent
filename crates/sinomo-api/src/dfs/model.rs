//! Shared DFS wire values and validation; no storage or service runtime.

use crate::dfs::*;
use serde::Deserialize;
use serde::Serialize;
use sinomo_core::DfsObjectPolicy;
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;

/// A DFS namespace and its default ownership metadata.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(try_from = "DfsFileSystemWire")]
pub struct DfsFileSystem {
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
        validate_retention_policies(&policies)?;
        validate_module_metadata_map(&module_metadata)?;
        Ok(Self {
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
#[serde(from = "DfsSubtreePolicyFields")]
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

/// The decoded fields of a [`DfsSubtreePolicy`], before normalization.
#[derive(Deserialize)]
struct DfsSubtreePolicyFields {
    path_prefix: String,
    policy: DfsObjectPolicy,
    scope: DfsPlacementScope,
    min_commit_replicas: usize,
    desired_replicas: usize,
    #[serde(default)]
    required_node_ids: Vec<NodeId>,
    #[serde(default)]
    forbidden_node_ids: Vec<NodeId>,
    #[serde(default)]
    min_distinct_zones: usize,
}

impl From<DfsSubtreePolicyFields> for DfsSubtreePolicy {
    fn from(fields: DfsSubtreePolicyFields) -> Self {
        let (min_commit_replicas, desired_replicas) =
            crate::dfs::normalized_replica_counts(fields.min_commit_replicas, fields.desired_replicas);
        Self {
            path_prefix: fields.path_prefix,
            policy: fields.policy,
            scope: fields.scope,
            min_commit_replicas,
            desired_replicas,
            required_node_ids: fields.required_node_ids,
            forbidden_node_ids: fields.forbidden_node_ids,
            min_distinct_zones: fields.min_distinct_zones,
        }
    }
}

impl DfsSubtreePolicy {
    /// A subtree rule; N is raised to W when it states less.
    pub fn new(
        path_prefix: impl Into<String>,
        policy: DfsObjectPolicy,
        min_commit_replicas: usize,
        desired_replicas: usize,
    ) -> Self {
        let (min_commit_replicas, desired_replicas) =
            crate::dfs::normalized_replica_counts(min_commit_replicas, desired_replicas);
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
