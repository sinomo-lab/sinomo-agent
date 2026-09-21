//! Shared DFS wire values and validation; no storage or service runtime.

use crate::dfs::*;
use serde::Deserialize;
use serde::Serialize;
use sinomo_core::node::NodeId;
use std::collections::BTreeSet;

/// A DFS operator inspection request (status / filesystem / path / object / conflicts /
/// repair). Host-agnostic contract: the same request drives in-process and daemon paths.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DaemonDfsInspectionRequest {
    Status,
    FileSystem {
        file_system_id: DfsFileSystemId,
    },
    Path {
        file_system_id: DfsFileSystemId,
        path: String,
    },
    Object {
        object_id: DfsObjectId,
    },
    Conflicts {
        file_system_id: Option<DfsFileSystemId>,
        object_id: Option<DfsObjectId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<DfsOperatorInspectionCursor>,
    },
    ResolveConflicts {
        file_system_id: Option<DfsFileSystemId>,
        object_id: Option<DfsObjectId>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        conflict_ids: Vec<String>,
    },
    ConflictVersions {
        object_id: DfsObjectId,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        conflict_ids: Vec<String>,
    },
    /// Retired metadata-only mutation. Kept for explicit rejection of older
    /// callers; restoration uses DfsFileHistoryRestoreRequest on its own route.
    UseConflictVersion {
        object_id: DfsObjectId,
        version: DfsVersion,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        conflict_ids: Vec<String>,
    },
    Repair {
        file_system_id: Option<DfsFileSystemId>,
        object_id: Option<DfsObjectId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<DfsOperatorInspectionCursor>,
    },
}

/// The response to a [`DaemonDfsInspectionRequest`].
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DaemonDfsInspectionResponse {
    Status {
        status: DfsOperatorStatus,
    },
    FileSystem {
        file_system: Option<DfsOperatorFileSystemView>,
    },
    Path {
        object: Option<DfsOperatorObjectView>,
    },
    Object {
        object: Option<DfsOperatorObjectView>,
    },
    Conflicts {
        conflicts: Vec<DfsConflictRecord>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_cursor: Option<DfsOperatorInspectionCursor>,
    },
    ResolveConflicts {
        report: DfsOperatorConflictResolutionReport,
    },
    ConflictVersions {
        report: Option<DfsOperatorConflictVersionsReport>,
    },
    UseConflictVersion {
        report: DfsOperatorUseConflictVersionReport,
    },
    Repair {
        repairs: Vec<DfsOperatorRepairView>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_cursor: Option<DfsOperatorInspectionCursor>,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum DfsOperatorStatusMetric {
    ObjectCount,
    VersionCount,
    ReceiptCount,
    IntentCount,
    PendingWriteCount,
    OpenConflictCount,
    PendingRepairCount,
    RepairAttemptCount,
    PendingNamespaceJobCount,
    FailedNamespaceJobCount,
    CrossShardNamespaceLockCount,
    MetadataShardJournalSequence,
    MetadataShardReplayCursor,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DfsOperatorStatusOmission {
    /// Historical routing generations can contain both the source and target
    /// copy of an object. A bounded projection cannot choose the authoritative
    /// copy without the durable probes used by the exact read path.
    HistoricalRoutingTopology {
        generation_count: usize,
        /// Every resident shard summary was available and their bounded
        /// cross-shard namespace-lock counts were all zero. Union emptiness is
        /// exact even though historical placement can make a positive summed
        /// cardinality ambiguous.
        #[serde(default, skip_serializing_if = "bool_is_false")]
        cross_shard_namespace_locks_zero_proven: bool,
    },
    /// Status deliberately did not replay a cold node-local repair queue.
    ResidentRepairQueueNotHydrated,
    /// A replica shard which is not authoritative on this node has never been
    /// opened. Bounded status retains its configured topology without opening
    /// it merely to recover replica-local counters and replay cursors.
    NonPrimaryResidentMetadataNotHydrated {
        file_system_id: DfsFileSystemId,
        shard_id: crate::dfs::DfsMetadataShardId,
    },
}

impl DfsOperatorStatusOmission {
    pub fn historical_routing_topology(generation_count: usize) -> Self {
        Self::HistoricalRoutingTopology {
            generation_count,
            cross_shard_namespace_locks_zero_proven: false,
        }
    }

    pub fn historical_routing_topology_with_cross_shard_lock_zero_proof(
        generation_count: usize,
    ) -> Self {
        Self::HistoricalRoutingTopology {
            generation_count,
            cross_shard_namespace_locks_zero_proven: true,
        }
    }

    pub fn resident_repair_queue_not_hydrated() -> Self {
        Self::ResidentRepairQueueNotHydrated
    }

    pub fn non_primary_resident_metadata_not_hydrated(
        file_system_id: DfsFileSystemId,
        shard_id: crate::dfs::DfsMetadataShardId,
    ) -> Self {
        Self::NonPrimaryResidentMetadataNotHydrated {
            file_system_id,
            shard_id,
        }
    }

    pub fn legacy_reason(&self) -> String {
        match self {
            Self::HistoricalRoutingTopology {
                generation_count, ..
            } => format!(
                "{generation_count} historical routing topology generation(s) remain active; metadata counters requiring authoritative visibility were omitted"
            ),
            Self::ResidentRepairQueueNotHydrated =>
                "repair queue observation unavailable: bounded DFS repair queue status unavailable because the resident queue is not hydrated"
                    .to_string(),
            Self::NonPrimaryResidentMetadataNotHydrated {
                file_system_id,
                shard_id,
            } => format!(
                "non-primary resident metadata shard {} for filesystem {} is not hydrated for bounded status; shard counters and replay cursors were omitted",
                shard_id.0, file_system_id
            ),
        }
    }

    pub fn omitted_metrics(&self) -> &'static [DfsOperatorStatusMetric] {
        use DfsOperatorStatusMetric::*;

        match self {
            Self::HistoricalRoutingTopology {
                cross_shard_namespace_locks_zero_proven: true,
                ..
            } => &[
                ObjectCount,
                VersionCount,
                ReceiptCount,
                IntentCount,
                PendingWriteCount,
                OpenConflictCount,
                PendingRepairCount,
                PendingNamespaceJobCount,
                FailedNamespaceJobCount,
            ],
            Self::HistoricalRoutingTopology { .. } => &[
                ObjectCount,
                VersionCount,
                ReceiptCount,
                IntentCount,
                PendingWriteCount,
                OpenConflictCount,
                PendingRepairCount,
                PendingNamespaceJobCount,
                FailedNamespaceJobCount,
                CrossShardNamespaceLockCount,
            ],
            Self::ResidentRepairQueueNotHydrated => &[RepairAttemptCount],
            Self::NonPrimaryResidentMetadataNotHydrated { .. } => &[
                ObjectCount,
                VersionCount,
                ReceiptCount,
                IntentCount,
                CrossShardNamespaceLockCount,
                MetadataShardJournalSequence,
                MetadataShardReplayCursor,
            ],
        }
    }
}

fn bool_is_false(value: &bool) -> bool {
    !*value
}

impl std::fmt::Display for DfsOperatorStatusOmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.legacy_reason())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorStatus {
    /// Node-local physical migrations; counts describe metadata records, not files.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub storage_migrations: Vec<sinomo_core::storage::migration::MigrationJob>,
    /// Human-readable compatibility mirrors for every unavailable observation.
    /// New readers use [`Self::blocking_incomplete_reasons`] so typed bounded
    /// omissions do not masquerade as corruption. Old readers remain
    /// fail-closed because producers continue to mirror omissions here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub incomplete_reasons: Vec<String>,
    /// Deliberate, bounded omissions that keep status from hydrating or
    /// scanning production-sized durable state. These remain operator-visible
    /// but are not themselves evidence of a DFS invariant violation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bounded_omissions: Vec<DfsOperatorStatusOmission>,
    pub file_system_count: usize,
    pub object_count: usize,
    pub version_count: usize,
    pub receipt_count: usize,
    pub intent_count: usize,
    pub pending_write_count: usize,
    pub open_conflict_count: usize,
    pub pending_repair_count: usize,
    #[serde(default)]
    pub repair_attempt_count: usize,
    #[serde(default)]
    pub pending_namespace_job_count: usize,
    #[serde(default)]
    pub failed_namespace_job_count: usize,
    /// Unique effective `(filesystem, root path)` cross-shard namespace locks
    /// retained by running durable jobs. `None` fails closed when an older
    /// daemon or bounded omission cannot provide this incremental metric.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cross_shard_namespace_lock_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_shards: Vec<DfsMetadataShardStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub repair_attempts: Vec<DfsRepairAttemptRecord>,
}

impl DfsOperatorStatus {
    /// Return only incomplete observations which are not exact legacy mirrors
    /// of a typed bounded omission. An old payload has no typed omissions, so
    /// every legacy reason remains blocking. Likewise, an unmatched reason in
    /// a new payload fails closed.
    pub fn blocking_incomplete_reasons(&self) -> Vec<&str> {
        let bounded_legacy_reasons = self
            .bounded_omissions
            .iter()
            .map(DfsOperatorStatusOmission::legacy_reason)
            .collect::<BTreeSet<_>>();
        self.incomplete_reasons
            .iter()
            .map(String::as_str)
            .filter(|reason| !bounded_legacy_reasons.contains(*reason))
            .collect()
    }

    pub fn push_bounded_omission(&mut self, omission: DfsOperatorStatusOmission) {
        if omission
            .omitted_metrics()
            .contains(&DfsOperatorStatusMetric::CrossShardNamespaceLockCount)
        {
            self.cross_shard_namespace_lock_count = None;
        }
        let legacy_reason = omission.legacy_reason();
        if !self
            .incomplete_reasons
            .iter()
            .any(|reason| reason == &legacy_reason)
        {
            self.incomplete_reasons.push(legacy_reason);
        }
        if !self.bounded_omissions.contains(&omission) {
            self.bounded_omissions.push(omission);
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorFileSystemView {
    pub file_system: DfsFileSystem,
    pub object_count: usize,
    pub open_conflict_count: usize,
    pub pending_repair_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorObjectView {
    pub object: DfsObjectMetadata,
    pub versions: Vec<DfsObjectVersionRecord>,
    pub receipts: Vec<DfsReplicaReceiptRecord>,
    pub intents: Vec<DfsWriteIntentRecord>,
    pub conflicts: Vec<DfsConflictRecord>,
    pub repairs: Vec<DfsOperatorRepairView>,
    pub agent_state: DfsAgentObjectState,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorRepairView {
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: Option<String>,
    pub missing_replica_node_id: NodeId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorConflictPage {
    pub conflicts: Vec<DfsConflictRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsOperatorInspectionCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorConflictResolutionReport {
    pub removed: Vec<DfsConflictRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub not_found_conflict_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorConflictVersionsReport {
    pub object: DfsObjectMetadata,
    pub conflicts: Vec<DfsConflictRecord>,
    pub versions: Vec<DfsOperatorConflictVersionView>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorConflictVersionView {
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<NodeId>,
    pub visible: bool,
    pub current: bool,
    pub stored: bool,
    pub roles: Vec<DfsOperatorConflictVersionRole>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorConflictVersionRole {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_id: Option<String>,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorUseConflictVersionReport {
    pub previous_object: DfsObjectMetadata,
    pub committed_object: DfsObjectMetadata,
    pub selected_version: DfsObjectVersionRecord,
    pub changed: bool,
    pub removed: Vec<DfsConflictRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub not_found_conflict_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorRepairPage {
    pub repairs: Vec<DfsOperatorRepairView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsOperatorInspectionCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorInspectionCursor {
    pub file_system_id: DfsFileSystemId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_cursor: Option<DfsObjectPageCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<DfsObjectId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_key: Option<DfsOperatorInspectionItemCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DfsOperatorInspectionItemCursor {
    Conflict {
        conflict_id: String,
    },
    Repair {
        intent_id: String,
        version: DfsVersion,
        missing_replica_node_id: NodeId,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsAgentObjectState {
    Current,
    PendingWrite,
    PendingRepair,
    Conflicted,
    MetadataOnly,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPinMode {
    Metadata,
    Content,
}

impl std::fmt::Display for DfsPinMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Metadata => formatter.write_str("metadata"),
            Self::Content => formatter.write_str("content"),
        }
    }
}
