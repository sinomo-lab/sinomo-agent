//! Core DFS metadata, routing, and service-event domain types.
//!
//! This module intentionally owns the in-memory model used by local and
//! networked DFS services. Durable table/page records live in
//! `metadata_backend`; transport envelopes live in `protocol` and `routes`.

use crate::{
    ContentStoreStats, DFS_POLICY_VIOLATION_EVENT_KIND, DfsConflictRecord, DfsConflictStatus,
    DfsError, DfsFileSystemId, DfsObjectId, DfsReadData, DfsReadResult, DfsVersion, DfsWriteResult,
    sifs_file_system_id,
};
use serde::{Deserialize, Serialize};
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
use sinomo_core::{
    Confidence, DfsObjectPolicy, DfsWriteReceipt, DfsWriteRequest, EventEnvelope, EventKind,
    LatencyClass, PrivacyClass, RoutingMetadata, RoutingScope, Salience, Source,
};
use std::collections::{BTreeMap, BTreeSet};
use time::OffsetDateTime;

/// A DFS namespace and its default ownership metadata.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileSystem {
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree_policies: Vec<DfsSubtreePolicy>,
}

/// Policy override that applies to objects below a path prefix.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsSubtreePolicy {
    pub path_prefix: String,
    pub policy: DfsObjectPolicy,
    /// Whether the override affects only `path_prefix` or is inherited by all
    /// descendants. Older records default to subtree behavior.
    #[serde(default, skip_serializing_if = "DfsPlacementScope::is_subtree")]
    pub scope: DfsPlacementScope,
    /// Explicit W and N. Absent values retain the legacy policy defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_commit_replicas: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired_replicas: Option<usize>,
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

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPlacementScope {
    SelfOnly,
    #[default]
    Subtree,
}

impl DfsPlacementScope {
    fn is_subtree(value: &Self) -> bool {
        matches!(value, Self::Subtree)
    }
}

impl DfsSubtreePolicy {
    pub fn legacy(path_prefix: impl Into<String>, policy: DfsObjectPolicy) -> Self {
        Self {
            path_prefix: path_prefix.into(),
            policy,
            scope: DfsPlacementScope::Subtree,
            min_commit_replicas: None,
            desired_replicas: None,
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
        }
    }
}

/// Current metadata for one DFS object in a filesystem.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsObjectMetadata {
    pub object_id: DfsObjectId,
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    pub kind: DfsObjectKind,
    /// Storage-level provenance for a tombstone that only evicts a physical
    /// source-shard copy after a cross-shard move. `None` is the backwards
    /// compatible representation of an authoritative namespace deletion.
    ///
    /// This distinction is part of durable metadata, rather than an inference
    /// from timestamps or paths: a source eviction is intentionally newer than
    /// the source live record, but it must never delete the live object already
    /// installed on `target_shard_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tombstone_provenance: Option<DfsTombstoneProvenance>,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub current_locations: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_target_object_id: Option<DfsObjectId>,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub mode: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub uid: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub gid: u32,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub accessed_at_unix_nanos: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub modified_at_unix_nanos: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub created_at_unix_nanos: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub changed_at_unix_nanos: u64,
    pub version: DfsVersion,
    pub policy: DfsObjectPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified_by_event_id: Option<EventId>,
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
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
/// Ordinary namespace deletes keep the legacy `None` representation on
/// [`DfsObjectMetadata::tombstone_provenance`]. Only physical source eviction
/// needs an explicit marker because it is not authoritative deletion evidence.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsTombstoneProvenance {
    SourceEviction {
        source_shard_id: crate::DfsMetadataShardId,
        target_shard_id: crate::DfsMetadataShardId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pin_transfer_id: Option<String>,
    },
}

/// In-memory record of a write that must be completed or reconciled.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WriteIntent {
    pub request: DfsWriteRequest,
    pub target_path: String,
    pub status: WriteIntentStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

/// Lifecycle state for an in-memory write intent.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum WriteIntentStatus {
    PendingOwner,
    Committed,
    Rejected,
    Conflict,
}

/// Structured policy denial that can be emitted as a supervision event.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPolicyViolation {
    pub requesting_node_id: NodeId,
    pub executing_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_system_id: Option<DfsFileSystemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<DfsObjectId>,
    pub reason: String,
}

impl DfsPolicyViolation {
    pub fn event(&self) -> EventEnvelope {
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            self.executing_node_id.clone(),
            Source::new("dfs.policy").expect("valid source"),
            EventKind::new(DFS_POLICY_VIOLATION_EVENT_KIND).expect("valid event kind"),
            Salience::new(0.9).expect("valid salience"),
            Confidence::new(0.96).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(self.executing_node_id.clone())
        .with_payload(serde_json::json!({
            "requesting_node_id": self.requesting_node_id,
            "executing_node_id": self.executing_node_id,
            "file_system_id": self.file_system_id,
            "object_id": self.object_id,
            "reason": self.reason,
        }))
    }
}

/// In-memory DFS metadata index used by local and networked services.
#[derive(Clone, Debug, Default)]
pub struct DfsMetadataStore {
    pub(crate) filesystems: BTreeMap<DfsFileSystemId, DfsFileSystem>,
    pub(crate) objects: BTreeMap<DfsObjectId, DfsObjectMetadata>,
    /// Index from `(file_system_id, normalized posix path)` to the object ids at
    /// that path, so path lookups avoid scanning every object. A path can hold
    /// more than one object id transiently (e.g. a tombstone alongside its
    /// replacement), so the set is resolved to the first non-tombstone object at
    /// lookup time, matching the previous linear scan exactly.
    pub(crate) objects_by_path: BTreeMap<(DfsFileSystemId, String), BTreeSet<DfsObjectId>>,
    pub(crate) conflicts: BTreeMap<DfsObjectId, BTreeMap<String, DfsConflictRecord>>,
}

impl DfsMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_file_system(&mut self, file_system: DfsFileSystem) {
        self.filesystems
            .insert(file_system.file_system_id.clone(), file_system);
    }

    pub fn add_object(&mut self, mut object: DfsObjectMetadata) -> Result<(), DfsError> {
        if !self.filesystems.contains_key(&object.file_system_id) {
            return Err(DfsError::UnknownFileSystem {
                file_system_id: object.file_system_id,
            });
        }
        normalize_current_locations(&mut object.current_locations);
        let new_key = path_index_key(&object.file_system_id, &object.path);
        let previous_key = self
            .objects
            .get(&object.object_id)
            .and_then(|previous| path_index_key(&previous.file_system_id, &previous.path));
        if previous_key != new_key {
            if let Some(previous_key) = previous_key {
                self.unindex_path(&previous_key, &object.object_id);
            }
            if let Some(new_key) = new_key {
                self.objects_by_path
                    .entry(new_key)
                    .or_default()
                    .insert(object.object_id.clone());
            }
        }
        self.objects.insert(object.object_id.clone(), object);
        Ok(())
    }

    pub(crate) fn remove_object(&mut self, object_id: &DfsObjectId) -> Option<DfsObjectMetadata> {
        let removed = self.objects.remove(object_id)?;
        if let Some(key) = path_index_key(&removed.file_system_id, &removed.path) {
            self.unindex_path(&key, object_id);
        }
        Some(removed)
    }

    fn unindex_path(&mut self, key: &(DfsFileSystemId, String), object_id: &DfsObjectId) {
        if let Some(object_ids) = self.objects_by_path.get_mut(key) {
            object_ids.remove(object_id);
            if object_ids.is_empty() {
                self.objects_by_path.remove(key);
            }
        }
    }

    pub fn object(&self, object_id: &DfsObjectId) -> Option<&DfsObjectMetadata> {
        self.objects.get(object_id)
    }

    /// Resolves the first non-tombstone object at a normalized posix path within
    /// a filesystem, using the path index instead of a full object scan.
    pub fn object_by_normalized_posix_path(
        &self,
        file_system_id: &DfsFileSystemId,
        normalized_path: &str,
    ) -> Option<&DfsObjectMetadata> {
        self.objects_by_path
            .get(&(file_system_id.clone(), normalized_path.to_string()))?
            .iter()
            .filter_map(|object_id| self.objects.get(object_id))
            .find(|object| object.kind != DfsObjectKind::Tombstone)
    }

    pub fn add_conflict(&mut self, conflict: DfsConflictRecord) {
        self.conflicts
            .entry(conflict.object_id.clone())
            .or_default()
            .insert(conflict.conflict_id.clone(), conflict);
    }

    pub fn remove_conflict(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Option<DfsConflictRecord> {
        let conflicts = self.conflicts.get_mut(object_id)?;
        let removed = conflicts.remove(conflict_id);
        if conflicts.is_empty() {
            self.conflicts.remove(object_id);
        }
        removed
    }

    pub fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Vec<DfsConflictRecord> {
        self.conflicts
            .get(object_id)
            .map(|conflicts| conflicts.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn open_conflicts_for_object(&self, object_id: &DfsObjectId) -> Vec<DfsConflictRecord> {
        self.conflicts_for_object(object_id)
            .into_iter()
            .filter(|conflict| conflict.status == DfsConflictStatus::Open)
            .collect()
    }

    pub fn route_read(
        &self,
        object_id: &DfsObjectId,
        requester_node_id: &NodeId,
    ) -> Result<ReadRoute, DfsError> {
        let object = self
            .objects
            .get(object_id)
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: object_id.clone(),
            })?;

        let action = match &object.policy {
            DfsObjectPolicy::LocalOwned => {
                if &object.owner_node_id == requester_node_id {
                    ReadRouteAction::ReadLocally
                } else {
                    ReadRouteAction::RouteToOwner {
                        owner_node_id: object.owner_node_id.clone(),
                    }
                }
            }
            DfsObjectPolicy::ReplicatedConfirmed | DfsObjectPolicy::ReplicatedEventual => {
                if object.current_locations.contains(requester_node_id) {
                    ReadRouteAction::ReadLocally
                } else {
                    ReadRouteAction::RouteToOwner {
                        owner_node_id: object.owner_node_id.clone(),
                    }
                }
            }
            _ => {
                return Err(DfsError::UnsupportedPolicy {
                    policy: object.policy.clone(),
                });
            }
        };

        Ok(ReadRoute {
            object_id: object.object_id.clone(),
            action,
        })
    }

    pub fn route_write(
        &self,
        request: DfsWriteRequest,
        requester_node_id: &NodeId,
    ) -> Result<WriteRoute, DfsError> {
        let object =
            self.objects
                .get(&request.object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: request.object_id.clone(),
                })?;

        let action = match &object.policy {
            DfsObjectPolicy::LocalOwned => {
                if &object.owner_node_id == requester_node_id {
                    WriteRouteAction::CommitLocally
                } else {
                    WriteRouteAction::RouteToOwner {
                        owner_node_id: object.owner_node_id.clone(),
                    }
                }
            }
            DfsObjectPolicy::ReplicatedConfirmed => WriteRouteAction::RequireReplicaReceipts,
            DfsObjectPolicy::ReplicatedEventual => WriteRouteAction::CommitThenReplicate,
            DfsObjectPolicy::AppendOnly => WriteRouteAction::AppendNewVersion,
            DfsObjectPolicy::EphemeralLocal => WriteRouteAction::CommitLocally,
        };

        Ok(WriteRoute {
            request,
            object_id: object.object_id.clone(),
            action,
        })
    }

    pub fn commit_write(
        &mut self,
        receipt: DfsWriteReceipt,
        content_hash: Option<String>,
    ) -> Result<(), DfsError> {
        let object =
            self.objects
                .get_mut(&receipt.object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: receipt.object_id.clone(),
                })?;
        object.version = receipt.committed_version;
        object.last_modified_by_event_id = Some(receipt.event_id);
        object.content_hash = content_hash;
        Ok(())
    }
}

/// Routing decision for a DFS read request.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ReadRoute {
    pub object_id: DfsObjectId,
    pub action: ReadRouteAction,
}

/// Action selected for a DFS read route.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ReadRouteAction {
    ReadLocally,
    RouteToOwner { owner_node_id: NodeId },
}

/// Routing decision for a DFS write request.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WriteRoute {
    pub request: DfsWriteRequest,
    pub object_id: DfsObjectId,
    pub action: WriteRouteAction,
}

/// Action selected for a DFS write route.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum WriteRouteAction {
    CommitLocally,
    RouteToOwner { owner_node_id: NodeId },
    RequireReplicaReceipts,
    CommitThenReplicate,
    AppendNewVersion,
}

/// Result shape returned by the high-level DFS read loop.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsReadOutcome {
    Local {
        object_id: DfsObjectId,
        data: Option<DfsReadData>,
    },
    Routed {
        request_id: String,
        owner_node_id: NodeId,
    },
}

/// Result shape returned by the high-level DFS write loop.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsWriteOutcome {
    Local {
        receipt: DfsWriteReceipt,
    },
    Routed {
        request_id: String,
        owner_node_id: NodeId,
    },
}

/// User-visible service event emitted by basic DFS request handling.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsServiceEvent {
    ReadRequestHandled {
        request_id: String,
        object_id: DfsObjectId,
        requester_node_id: NodeId,
        result: DfsReadResult,
    },
    ReadResponseReceived {
        request_id: String,
        object_id: DfsObjectId,
        result: DfsReadResult,
    },
    WriteRequestHandled {
        request_id: String,
        object_id: DfsObjectId,
        requester_node_id: NodeId,
        result: DfsWriteResult,
    },
    WriteResponseReceived {
        request_id: String,
        object_id: DfsObjectId,
        result: DfsWriteResult,
    },
}

/// Snapshot of local DFS service counters used by status and diagnostics.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperationalMetrics {
    pub node_id: NodeId,
    pub file_system_count: usize,
    pub object_count: usize,
    pub open_conflict_count: usize,
    pub coordinator_intent_count: usize,
    pub coordinator_receipt_count: usize,
    pub coordinator_committed_version_count: usize,
    pub repair_job_count: usize,
    pub repair_attempt_count: usize,
    pub pending_write_count: usize,
    pub pending_read_count: usize,
    pub stage_retry_count: usize,
    pub repair_retry_count: usize,
    pub stage_inflight_count: usize,
    pub repair_inflight_count: usize,
    pub fetch_inflight_count: usize,
    pub fetch_failed_source_count: usize,
    pub active_replica_stage_count: usize,
    pub metadata_replication_cursor_count: usize,
    pub pending_metadata_replication_count: usize,
    pub pending_routed_write_count: usize,
    pub pending_remote_write_response_count: usize,
    pub pending_routed_namespace_op_count: usize,
    pub cross_shard_namespace_lock_count: usize,
    /// Local metadata shards whose current backend instance remains fenced
    /// from authoritative mutations by a membership-handoff marker.
    #[serde(default)]
    pub authoritative_write_frozen_shard_count: usize,
    pub metadata_backend_configured: bool,
    pub content: ContentStoreStats,
}

pub(crate) fn normalize_current_locations(locations: &mut Vec<NodeId>) {
    let mut seen = BTreeSet::new();
    locations.retain(|node_id| seen.insert(node_id.clone()));
}

/// Builds the `(file_system_id, normalized path)` key used to index an object by
/// path, or `None` when the object's path cannot be normalized (such objects are
/// not reachable by path lookup, matching the previous scan's behavior).
pub(crate) fn path_index_key(
    file_system_id: &DfsFileSystemId,
    path: &str,
) -> Option<(DfsFileSystemId, String)> {
    crate::posix_path::normalized_posix_path_key(path)
        .map(|normalized| (file_system_id.clone(), normalized))
}
