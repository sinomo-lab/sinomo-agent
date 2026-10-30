//! Shared DFS wire values and validation; no storage or service runtime.

use crate::dfs::*;
use serde::Deserialize;
use serde::Serialize;
use sinomo_core::node::NodeId;

/// A DFS operator inspection request (status / filesystem / path / object /
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

    /// Inspect bounded repair work without mutating metadata.
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

    pub fn reason(&self) -> String {
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

impl std::fmt::Display for DfsOperatorStatusOmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.reason())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorStatus {
    /// Node-local physical migrations; counts describe metadata records, not files.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub storage_migrations: Vec<sinomo_core::storage::migration::MigrationJob>,
    /// Errors that prevent a complete, authoritative observation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub incomplete_reasons: Vec<String>,
    /// Deliberate, bounded omissions that keep status from hydrating or
    /// scanning production-sized durable state. These remain operator-visible
    /// but are not themselves evidence of a DFS invariant violation.
    pub bounded_omissions: Vec<DfsOperatorStatusOmission>,
    pub file_system_count: usize,
    pub object_count: usize,
    pub version_count: usize,
    pub receipt_count: usize,
    pub intent_count: usize,
    pub pending_write_count: usize,
    pub pending_repair_count: usize,
    #[serde(default)]
    pub repair_attempt_count: usize,
    #[serde(default)]
    pub pending_namespace_job_count: usize,
    #[serde(default)]
    pub failed_namespace_job_count: usize,
    /// Unique effective `(filesystem, root path)` cross-shard namespace locks
    /// retained by running durable jobs. `None` fails closed when an bounded
    /// omission cannot provide this incremental metric.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cross_shard_namespace_lock_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_shards: Vec<DfsMetadataShardStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub repair_attempts: Vec<DfsRepairAttemptRecord>,
}

impl DfsOperatorStatus {
    pub fn blocking_incomplete_reasons(&self) -> Vec<&str> {
        self.incomplete_reasons.iter().map(String::as_str).collect()
    }

    pub fn push_bounded_omission(&mut self, omission: DfsOperatorStatusOmission) {
        if omission
            .omitted_metrics()
            .contains(&DfsOperatorStatusMetric::CrossShardNamespaceLockCount)
        {
            self.cross_shard_namespace_lock_count = None;
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
    pub pending_repair_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperatorObjectView {
    pub object: DfsObjectMetadata,
    pub versions: Vec<DfsObjectVersionRecord>,
    pub receipts: Vec<DfsReplicaReceiptRecord>,
    pub intents: Vec<DfsWriteIntentRecord>,
    /// Current content placement projected from its canonical owner or recorded
    /// durability. Missing means unavailable, not an empty or completed goal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_desired_replica_node_ids: Option<Vec<NodeId>>,
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
