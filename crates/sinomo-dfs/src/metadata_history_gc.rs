//! Retention planning and observability for authoritative DFS metadata history.
//!
//! This module is deliberately pure over [`DfsMetadataTables`]. The concrete
//! backends own authority fencing and journal writes; the planner identifies a
//! deterministic, bounded set of records which can be removed without losing
//! current namespace state, pending recovery state, or explicitly retained
//! historical versions.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::intent::write_intent_status_is_active;
use crate::metadata_namespace_transaction::namespace_transaction_recovery_is_complete;
use crate::metadata_receipt::replay_receipt_matches_existing;
use crate::metadata_records::{
    DfsConflictStatus, DfsMetadataShardId, DfsNamespacePrepareStatus, DfsNamespaceRequestStatus,
    DfsObjectVersionRecord, DfsWriteIntentStatus,
};
use crate::metadata_tables::DfsMetadataTables;
use crate::metadata_visible_commit::visible_commit_should_recover;
use crate::protocol::DfsNamespaceDestinationPrecondition;
use crate::{DfsFileSystemId, DfsNamespaceOperation, DfsObjectId, DfsVersion};

/// Stable identity of one immutable metadata version protected from GC.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
pub struct DfsMetadataVersionIdentity {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
}

impl DfsMetadataVersionIdentity {
    pub fn new(object_id: DfsObjectId, version: DfsVersion) -> Self {
        Self { object_id, version }
    }
}

/// Historical-version and recovery-body retention policy.
///
/// The contract is intentionally explicit:
///
/// * current and recovery-rooted versions are retained regardless of age;
/// * at least `keep_latest_versions_per_object` immutable versions are kept;
/// * every record younger than `min_history_age_nanos` remains available;
/// * durable metadata version-pin records are absolute roots;
/// * caller-provided per-run version protections are additional roots.
///
/// Local path/content placement pins do not imply a historical-version pin
/// and never create a durable metadata version-pin record implicitly.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataHistoryGcOptions {
    pub dry_run: bool,
    pub min_history_age_nanos: u64,
    pub keep_latest_versions_per_object: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_removals: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_unix_nanos: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub protected_versions: Vec<DfsMetadataVersionIdentity>,
}

impl DfsMetadataHistoryGcOptions {
    pub fn dry_run(min_history_age: Duration, keep_latest_versions_per_object: u64) -> Self {
        Self {
            dry_run: true,
            min_history_age_nanos: duration_nanos_saturating(min_history_age),
            keep_latest_versions_per_object: keep_latest_versions_per_object.max(1),
            max_removals: None,
            now_unix_nanos: None,
            protected_versions: Vec::new(),
        }
    }

    pub fn delete_eligible(
        min_history_age: Duration,
        keep_latest_versions_per_object: u64,
    ) -> Self {
        Self {
            dry_run: false,
            min_history_age_nanos: duration_nanos_saturating(min_history_age),
            keep_latest_versions_per_object: keep_latest_versions_per_object.max(1),
            max_removals: None,
            now_unix_nanos: None,
            protected_versions: Vec::new(),
        }
    }

    pub fn with_max_removals(mut self, max_removals: u64) -> Self {
        self.max_removals = Some(max_removals);
        self
    }

    pub fn with_now_unix_nanos(mut self, now_unix_nanos: u64) -> Self {
        self.now_unix_nanos = Some(now_unix_nanos);
        self
    }

    pub fn with_protected_versions(
        mut self,
        protected_versions: impl IntoIterator<Item = DfsMetadataVersionIdentity>,
    ) -> Self {
        self.protected_versions = protected_versions.into_iter().collect();
        self.protected_versions.sort();
        self.protected_versions.dedup();
        self
    }
}

/// Approximate serialized footprint and retention age for one materialized
/// metadata table. Derived indexes are intentionally excluded.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataRetainedTableStats {
    pub table: String,
    pub record_count: u64,
    pub estimated_serialized_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_record_unix_nanos: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_record_age_nanos: Option<u64>,
}

/// Per-shard retained metadata footprint captured by a GC projection or run.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataHistoryShardStats {
    pub shard_id: DfsMetadataShardId,
    /// Whether this local shard generation may contribute lifecycle collection
    /// pressure. Replica and handoff-frozen inventories remain observable but
    /// must never schedule authoritative deletion.
    #[serde(default)]
    pub collection_authorized: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_size_bytes: Option<u64>,
    #[serde(default)]
    pub journal_size_bytes: u64,
    #[serde(default)]
    pub journal_segment_count: u64,
    #[serde(default)]
    pub journal_head_sequence: u64,
    pub total_record_count: u64,
    pub estimated_serialized_bytes: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<DfsMetadataRetainedTableStats>,
}

/// Metadata-derived roots which local content GC must never delete.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataContentProtection {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub versions: Vec<DfsMetadataVersionIdentity>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_hashes: Vec<String>,
}

impl DfsMetadataContentProtection {
    pub(crate) fn merge(&mut self, other: Self) {
        self.versions.extend(other.versions);
        self.versions.sort();
        self.versions.dedup();
        self.content_hashes.extend(other.content_hashes);
        self.content_hashes.sort();
        self.content_hashes.dedup();
    }
}

/// Counts produced by one metadata-history projection or authoritative GC run.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataHistoryGcReport {
    pub dry_run: bool,
    pub min_history_age_nanos: u64,
    pub keep_latest_versions_per_object: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_removals: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shards: Vec<DfsMetadataHistoryShardStats>,
    pub protected_version_count: u64,
    pub scanned_version_count: u64,
    pub eligible_version_count: u64,
    pub removed_version_count: u64,
    pub eligible_receipt_count: u64,
    pub removed_receipt_count: u64,
    pub eligible_intent_count: u64,
    pub compacted_intent_count: u64,
    pub eligible_intent_receipt_count: u64,
    pub removed_intent_receipt_count: u64,
    pub eligible_conflict_count: u64,
    pub removed_conflict_count: u64,
    pub eligible_visible_commit_count: u64,
    pub removed_visible_commit_count: u64,
    pub eligible_namespace_job_count: u64,
    pub compacted_namespace_job_count: u64,
    pub eligible_namespace_transaction_count: u64,
    pub compacted_namespace_transaction_count: u64,
    pub eligible_namespace_prepare_count: u64,
    pub compacted_namespace_prepare_count: u64,
    pub eligible_namespace_request_count: u64,
    pub compacted_namespace_request_count: u64,
    pub eligible_namespace_job_receipt_count: u64,
    pub removed_namespace_job_receipt_count: u64,
    pub eligible_namespace_transaction_receipt_count: u64,
    pub removed_namespace_transaction_receipt_count: u64,
    pub eligible_namespace_prepare_receipt_count: u64,
    pub removed_namespace_prepare_receipt_count: u64,
    pub eligible_namespace_request_receipt_count: u64,
    pub removed_namespace_request_receipt_count: u64,
    pub retained_recovery_root_count: u64,
    pub retained_young_record_count: u64,
    pub retained_latest_version_count: u64,
    pub retained_explicit_pin_count: u64,
    #[serde(default)]
    pub retained_durable_pin_count: u64,
    /// Oldest record which the planner found both unrooted and outside the
    /// configured recovery/history window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_eligible_record_unix_nanos: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_eligible_at_unix_nanos: Option<u64>,
    pub max_removals_reached: bool,
}

impl DfsMetadataHistoryGcReport {
    pub fn eligible_record_count(&self) -> u64 {
        self.eligible_version_count
            .saturating_add(self.eligible_receipt_count)
            .saturating_add(self.eligible_intent_count)
            .saturating_add(self.eligible_intent_receipt_count)
            .saturating_add(self.eligible_conflict_count)
            .saturating_add(self.eligible_visible_commit_count)
            .saturating_add(self.eligible_namespace_job_count)
            .saturating_add(self.eligible_namespace_transaction_count)
            .saturating_add(self.eligible_namespace_prepare_count)
            .saturating_add(self.eligible_namespace_request_count)
            .saturating_add(self.eligible_namespace_job_receipt_count)
            .saturating_add(self.eligible_namespace_transaction_receipt_count)
            .saturating_add(self.eligible_namespace_prepare_receipt_count)
            .saturating_add(self.eligible_namespace_request_receipt_count)
    }

    pub fn applied_record_count(&self) -> u64 {
        self.removed_version_count
            .saturating_add(self.removed_receipt_count)
            .saturating_add(self.compacted_intent_count)
            .saturating_add(self.removed_intent_receipt_count)
            .saturating_add(self.removed_conflict_count)
            .saturating_add(self.removed_visible_commit_count)
            .saturating_add(self.compacted_namespace_job_count)
            .saturating_add(self.compacted_namespace_transaction_count)
            .saturating_add(self.compacted_namespace_prepare_count)
            .saturating_add(self.compacted_namespace_request_count)
            .saturating_add(self.removed_namespace_job_receipt_count)
            .saturating_add(self.removed_namespace_transaction_receipt_count)
            .saturating_add(self.removed_namespace_prepare_receipt_count)
            .saturating_add(self.removed_namespace_request_receipt_count)
    }

    pub(crate) fn for_options(options: &DfsMetadataHistoryGcOptions) -> Self {
        Self {
            dry_run: options.dry_run,
            min_history_age_nanos: options.min_history_age_nanos,
            keep_latest_versions_per_object: options.keep_latest_versions_per_object.max(1),
            max_removals: options.max_removals,
            ..Self::default()
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.shards.extend(other.shards);
        self.protected_version_count = self
            .protected_version_count
            .saturating_add(other.protected_version_count);
        self.scanned_version_count = self
            .scanned_version_count
            .saturating_add(other.scanned_version_count);
        self.eligible_version_count = self
            .eligible_version_count
            .saturating_add(other.eligible_version_count);
        self.removed_version_count = self
            .removed_version_count
            .saturating_add(other.removed_version_count);
        self.eligible_receipt_count = self
            .eligible_receipt_count
            .saturating_add(other.eligible_receipt_count);
        self.removed_receipt_count = self
            .removed_receipt_count
            .saturating_add(other.removed_receipt_count);
        self.eligible_intent_count = self
            .eligible_intent_count
            .saturating_add(other.eligible_intent_count);
        self.compacted_intent_count = self
            .compacted_intent_count
            .saturating_add(other.compacted_intent_count);
        self.eligible_intent_receipt_count = self
            .eligible_intent_receipt_count
            .saturating_add(other.eligible_intent_receipt_count);
        self.removed_intent_receipt_count = self
            .removed_intent_receipt_count
            .saturating_add(other.removed_intent_receipt_count);
        self.eligible_conflict_count = self
            .eligible_conflict_count
            .saturating_add(other.eligible_conflict_count);
        self.removed_conflict_count = self
            .removed_conflict_count
            .saturating_add(other.removed_conflict_count);
        self.eligible_visible_commit_count = self
            .eligible_visible_commit_count
            .saturating_add(other.eligible_visible_commit_count);
        self.removed_visible_commit_count = self
            .removed_visible_commit_count
            .saturating_add(other.removed_visible_commit_count);
        self.eligible_namespace_job_count = self
            .eligible_namespace_job_count
            .saturating_add(other.eligible_namespace_job_count);
        self.compacted_namespace_job_count = self
            .compacted_namespace_job_count
            .saturating_add(other.compacted_namespace_job_count);
        self.eligible_namespace_transaction_count = self
            .eligible_namespace_transaction_count
            .saturating_add(other.eligible_namespace_transaction_count);
        self.compacted_namespace_transaction_count = self
            .compacted_namespace_transaction_count
            .saturating_add(other.compacted_namespace_transaction_count);
        self.eligible_namespace_prepare_count = self
            .eligible_namespace_prepare_count
            .saturating_add(other.eligible_namespace_prepare_count);
        self.compacted_namespace_prepare_count = self
            .compacted_namespace_prepare_count
            .saturating_add(other.compacted_namespace_prepare_count);
        self.eligible_namespace_request_count = self
            .eligible_namespace_request_count
            .saturating_add(other.eligible_namespace_request_count);
        self.compacted_namespace_request_count = self
            .compacted_namespace_request_count
            .saturating_add(other.compacted_namespace_request_count);
        self.eligible_namespace_job_receipt_count = self
            .eligible_namespace_job_receipt_count
            .saturating_add(other.eligible_namespace_job_receipt_count);
        self.removed_namespace_job_receipt_count = self
            .removed_namespace_job_receipt_count
            .saturating_add(other.removed_namespace_job_receipt_count);
        self.eligible_namespace_transaction_receipt_count = self
            .eligible_namespace_transaction_receipt_count
            .saturating_add(other.eligible_namespace_transaction_receipt_count);
        self.removed_namespace_transaction_receipt_count = self
            .removed_namespace_transaction_receipt_count
            .saturating_add(other.removed_namespace_transaction_receipt_count);
        self.eligible_namespace_prepare_receipt_count = self
            .eligible_namespace_prepare_receipt_count
            .saturating_add(other.eligible_namespace_prepare_receipt_count);
        self.removed_namespace_prepare_receipt_count = self
            .removed_namespace_prepare_receipt_count
            .saturating_add(other.removed_namespace_prepare_receipt_count);
        self.eligible_namespace_request_receipt_count = self
            .eligible_namespace_request_receipt_count
            .saturating_add(other.eligible_namespace_request_receipt_count);
        self.removed_namespace_request_receipt_count = self
            .removed_namespace_request_receipt_count
            .saturating_add(other.removed_namespace_request_receipt_count);
        self.retained_recovery_root_count = self
            .retained_recovery_root_count
            .saturating_add(other.retained_recovery_root_count);
        self.retained_young_record_count = self
            .retained_young_record_count
            .saturating_add(other.retained_young_record_count);
        self.retained_latest_version_count = self
            .retained_latest_version_count
            .saturating_add(other.retained_latest_version_count);
        self.retained_explicit_pin_count = self
            .retained_explicit_pin_count
            .saturating_add(other.retained_explicit_pin_count);
        self.retained_durable_pin_count = self
            .retained_durable_pin_count
            .saturating_add(other.retained_durable_pin_count);
        self.oldest_eligible_record_unix_nanos = self
            .oldest_eligible_record_unix_nanos
            .into_iter()
            .chain(other.oldest_eligible_record_unix_nanos)
            .min();
        self.next_eligible_at_unix_nanos = self
            .next_eligible_at_unix_nanos
            .into_iter()
            .chain(other.next_eligible_at_unix_nanos)
            .min();
        self.max_removals_reached |= other.max_removals_reached;
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum DfsMetadataHistoryRemoval {
    Receipt {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        replica_node_id: sinomo_core::node::NodeId,
    },
    Version {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
    },
    Intent {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        intent_id: String,
    },
    IntentReceipt {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        intent_id: String,
    },
    Conflict {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        conflict_id: String,
    },
    VisibleCommit {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
    },
    NamespaceJob {
        file_system_id: DfsFileSystemId,
        job_id: String,
    },
    NamespaceTransaction {
        file_system_id: DfsFileSystemId,
        transaction_id: String,
    },
    NamespacePrepare {
        file_system_id: DfsFileSystemId,
        operation_id: String,
    },
    NamespaceRequest {
        file_system_id: DfsFileSystemId,
        requester_node_id: sinomo_core::node::NodeId,
        request_id: String,
    },
    NamespaceJobReceipt {
        file_system_id: DfsFileSystemId,
        job_id: String,
    },
    NamespaceTransactionReceipt {
        file_system_id: DfsFileSystemId,
        transaction_id: String,
    },
    NamespacePrepareReceipt {
        file_system_id: DfsFileSystemId,
        operation_id: String,
    },
    NamespaceRequestReceipt {
        file_system_id: DfsFileSystemId,
        requester_node_id: sinomo_core::node::NodeId,
        request_id: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DfsMetadataHistoryGcPlan {
    pub(crate) report: DfsMetadataHistoryGcReport,
    pub(crate) removals: Vec<DfsMetadataHistoryRemoval>,
    pub(crate) protected_versions: BTreeSet<DfsMetadataVersionIdentity>,
    pub(crate) protected_content_hashes: BTreeSet<String>,
}

#[derive(Clone, Debug)]
struct RemovalCandidate {
    retained_at_unix_nanos: u64,
    removal: DfsMetadataHistoryRemoval,
}

impl RemovalCandidate {
    fn sort_key(&self) -> (u64, &DfsMetadataHistoryRemoval) {
        (self.retained_at_unix_nanos, &self.removal)
    }
}

pub(crate) fn metadata_history_stats_from_tables(
    shard_id: DfsMetadataShardId,
    tables: &DfsMetadataTables,
    snapshot_size_bytes: Option<u64>,
) -> DfsMetadataHistoryShardStats {
    let mut stats = Vec::new();
    push_stats(
        &mut stats,
        "file-systems",
        tables.file_systems.values(),
        |_| None,
    );
    push_stats(&mut stats, "objects", tables.objects.values(), |record| {
        Some(
            [
                record.changed_at_unix_nanos,
                record.modified_at_unix_nanos,
                record.created_at_unix_nanos,
                record.accessed_at_unix_nanos,
            ]
            .into_iter()
            .find(|value| *value > 0)
            .unwrap_or(0),
        )
    });
    push_stats(
        &mut stats,
        "file-system-objects",
        tables.file_system_objects.values(),
        |_| None,
    );
    push_stats(&mut stats, "paths", tables.paths.values(), |_| None);
    push_stats(&mut stats, "inodes", tables.inodes.values(), |_| None);
    push_stats(&mut stats, "dirfrags", tables.dirfrags.values(), |_| None);
    push_stats(&mut stats, "dentries", tables.dentries.values(), |_| None);
    push_stats(&mut stats, "hardlinks", tables.hardlinks.values(), |_| None);
    push_stats(
        &mut stats,
        "compacted-tombstones",
        tables.compacted_tombstones.values(),
        |record| {
            Some(
                [
                    record.changed_at_unix_nanos,
                    record.modified_at_unix_nanos,
                    record.created_at_unix_nanos,
                    record.accessed_at_unix_nanos,
                ]
                .into_iter()
                .find(|value| *value > 0)
                .unwrap_or(0),
            )
        },
    );
    push_stats(&mut stats, "versions", tables.versions.values(), |record| {
        Some(offset_datetime_nanos(record.created_at))
    });
    push_stats(
        &mut stats,
        "version-pins",
        tables.version_pins.values(),
        |record| Some(offset_datetime_nanos(record.created_at)),
    );
    push_stats(
        &mut stats,
        "cross-shard-pin-transfers",
        tables.cross_shard_pin_transfers.values(),
        |record| Some(offset_datetime_nanos(record.created_at)),
    );
    push_stats(&mut stats, "receipts", tables.receipts.values(), |record| {
        Some(offset_datetime_nanos(record.received_at))
    });
    push_stats(&mut stats, "intents", tables.intents.values(), |record| {
        Some(offset_datetime_nanos(
            record.updated_at.unwrap_or(record.created_at),
        ))
    });
    push_stats(
        &mut stats,
        "intent-receipts",
        tables.intent_receipts.values(),
        |record| Some(offset_datetime_nanos(record.retained_at)),
    );
    push_retention_stats(
        &mut stats,
        "intent-receipt-retention",
        &tables.intent_receipt_retention,
        intent_receipt_retention_record_count(&tables.intent_receipt_retention),
    );
    push_stats(
        &mut stats,
        "conflicts",
        tables.conflicts.values(),
        |record| Some(offset_datetime_nanos(record.detected_at)),
    );
    push_stats(
        &mut stats,
        "advisory-lock-states",
        tables.advisory_lock_states.values(),
        |_| None,
    );
    push_stats(
        &mut stats,
        "visible-commits",
        tables.visible_commits.values(),
        |record| Some(offset_datetime_nanos(record.created_at)),
    );
    push_stats(
        &mut stats,
        "namespace-transactions",
        tables.namespace_transactions.values(),
        |record| Some(offset_datetime_nanos(record.created_at)),
    );
    push_stats(
        &mut stats,
        "namespace-transaction-receipts",
        tables.namespace_transaction_receipts.values(),
        |record| Some(offset_datetime_nanos(record.retained_at)),
    );
    push_stats(
        &mut stats,
        "namespace-prepares",
        tables.namespace_prepares.values(),
        |record| Some(offset_datetime_nanos(record.transaction.created_at)),
    );
    push_stats(
        &mut stats,
        "namespace-prepare-receipts",
        tables.namespace_prepare_receipts.values(),
        |record| Some(offset_datetime_nanos(record.retained_at)),
    );
    push_stats(
        &mut stats,
        "namespace-jobs",
        tables.namespace_jobs.values(),
        |record| Some(offset_datetime_nanos(record.updated_at)),
    );
    push_stats(
        &mut stats,
        "namespace-job-receipts",
        tables.namespace_job_receipts.values(),
        |record| Some(offset_datetime_nanos(record.retained_at)),
    );
    push_stats(
        &mut stats,
        "namespace-requests",
        tables.namespace_requests.values(),
        |record| Some(offset_datetime_nanos(record.updated_at)),
    );
    push_stats(
        &mut stats,
        "namespace-request-receipts",
        tables.namespace_request_receipts.values(),
        |record| Some(offset_datetime_nanos(record.retained_at)),
    );
    push_retention_stats(
        &mut stats,
        "namespace-receipt-retention",
        &tables.namespace_receipt_retention,
        namespace_receipt_retention_record_count(&tables.namespace_receipt_retention),
    );

    let total_record_count = stats
        .iter()
        .map(|table| table.record_count)
        .fold(0_u64, u64::saturating_add);
    let estimated_serialized_bytes = stats
        .iter()
        .map(|table| table.estimated_serialized_bytes)
        .fold(0_u64, u64::saturating_add);
    DfsMetadataHistoryShardStats {
        shard_id,
        collection_authorized: true,
        snapshot_size_bytes,
        journal_size_bytes: 0,
        journal_segment_count: 0,
        journal_head_sequence: 0,
        total_record_count,
        estimated_serialized_bytes,
        tables: stats,
    }
}

pub(crate) fn plan_metadata_history_gc(
    shard_id: DfsMetadataShardId,
    tables: &DfsMetadataTables,
    options: &DfsMetadataHistoryGcOptions,
    snapshot_size_bytes: Option<u64>,
) -> DfsMetadataHistoryGcPlan {
    let now = options
        .now_unix_nanos
        .unwrap_or_else(metadata_history_now_unix_nanos);
    let mut report = DfsMetadataHistoryGcReport::for_options(options);
    let mut shard_stats = metadata_history_stats_from_tables(shard_id, tables, snapshot_size_bytes);
    for table in &mut shard_stats.tables {
        table.oldest_record_age_nanos = table
            .oldest_record_unix_nanos
            .map(|oldest| now.saturating_sub(oldest));
    }
    report.shards.push(shard_stats);

    let explicit_pins = options
        .protected_versions
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    report.retained_explicit_pin_count = explicit_pins.len() as u64;
    let mut protected_versions = explicit_pins;
    report.retained_durable_pin_count = tables.version_pins.len() as u64;
    protected_versions.extend(
        tables
            .version_pins
            .values()
            .map(|pin| DfsMetadataVersionIdentity::new(pin.object_id.clone(), pin.version.clone())),
    );
    for transfer in tables.cross_shard_pin_transfers.values() {
        protected_versions.insert(DfsMetadataVersionIdentity::new(
            transfer.object_id.clone(),
            transfer.target_version.clone(),
        ));
        protected_versions.extend(
            transfer
                .source_pins
                .iter()
                .chain(&transfer.copied_pins)
                .map(|pin| {
                    DfsMetadataVersionIdentity::new(pin.object_id.clone(), pin.version.clone())
                }),
        );
    }

    for object in tables.objects.values() {
        protected_versions.insert(DfsMetadataVersionIdentity::new(
            object.object_id.clone(),
            object.version.clone(),
        ));
    }

    for conflict in tables
        .conflicts
        .values()
        .filter(|conflict| conflict.status == DfsConflictStatus::Open)
    {
        for version in conflict
            .base_version
            .iter()
            .chain([&conflict.local_version, &conflict.remote_version])
        {
            protected_versions.insert(DfsMetadataVersionIdentity::new(
                conflict.object_id.clone(),
                version.clone(),
            ));
        }
    }

    for intent in tables.intents.values() {
        let is_recovery_root = intent_is_recovery_root(tables, intent);
        if is_recovery_root && let Some(base_version) = intent.base_version.as_ref() {
            protected_versions.insert(DfsMetadataVersionIdentity::new(
                intent.object_id.clone(),
                base_version.clone(),
            ));
        }
        // A terminal intent selected for compaction in this plan still
        // carries the exact-retry witness until the journaled receipt exists.
        // Root its target for this mark phase; the receipt roots it on the
        // following pass and prevents a same-batch version deletion from
        // making an exact routed retry unreadable.
        if is_recovery_root || !write_intent_status_is_active(&intent.status) {
            protected_versions.insert(DfsMetadataVersionIdentity::new(
                intent.object_id.clone(),
                intent.target_version.clone(),
            ));
        }
    }
    for receipt in tables.intent_receipts.values() {
        protected_versions.insert(DfsMetadataVersionIdentity::new(
            receipt.object_id.clone(),
            receipt.target_version.clone(),
        ));
    }

    for commit in tables.visible_commits.values() {
        let current_is_expected = tables
            .objects
            .get(&commit.committed_object.object_id)
            .is_some_and(|object| object.version == commit.expected_version);
        if current_is_expected || visible_commit_should_recover(tables, commit) {
            protected_versions.insert(DfsMetadataVersionIdentity::new(
                commit.committed_object.object_id.clone(),
                commit.expected_version.clone(),
            ));
            protected_versions.insert(DfsMetadataVersionIdentity::new(
                commit.visible_version.object_id.clone(),
                commit.visible_version.version.clone(),
            ));
        }
    }

    for transaction in tables.namespace_transactions.values() {
        protect_namespace_transaction_versions(&mut protected_versions, transaction);
    }
    for prepare in tables
        .namespace_prepares
        .values()
        .filter(|prepare| namespace_prepare_is_recovery_root(tables, prepare))
    {
        for expected in &prepare.expected_object_versions {
            if let Some(version) = expected.expected_version.as_ref() {
                protected_versions.insert(DfsMetadataVersionIdentity::new(
                    expected.object_id.clone(),
                    version.clone(),
                ));
            }
        }
        protect_namespace_transaction_versions(&mut protected_versions, &prepare.transaction);
        for receipt in &prepare.content_receipts {
            protected_versions.insert(DfsMetadataVersionIdentity::new(
                receipt.receipt.object_id.clone(),
                receipt.receipt.committed_version.clone(),
            ));
        }
    }
    for request in tables
        .namespace_requests
        .values()
        .filter(|request| request.status == DfsNamespaceRequestStatus::Pending)
    {
        protect_namespace_operation_versions(&mut protected_versions, &request.operation);
    }
    for job in tables.namespace_jobs.values() {
        protect_namespace_job_versions(&mut protected_versions, job);
    }

    report.retained_recovery_root_count = protected_versions.len() as u64;

    let keep_latest =
        usize::try_from(options.keep_latest_versions_per_object.max(1)).unwrap_or(usize::MAX);
    let mut versions_by_object = BTreeMap::<DfsObjectId, Vec<&DfsObjectVersionRecord>>::new();
    for version in tables.versions.values() {
        report.scanned_version_count = report.scanned_version_count.saturating_add(1);
        versions_by_object
            .entry(version.object_id.clone())
            .or_default()
            .push(version);
    }
    for versions in versions_by_object.values_mut() {
        versions.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.version.cmp(&left.version))
        });
        for version in versions.iter().take(keep_latest) {
            if protected_versions.insert(DfsMetadataVersionIdentity::new(
                version.object_id.clone(),
                version.version.clone(),
            )) {
                report.retained_latest_version_count =
                    report.retained_latest_version_count.saturating_add(1);
            }
        }
    }

    let mut candidates = Vec::new();
    let mut versions_selected_for_removal = BTreeSet::new();
    let versions_with_receipts = tables
        .receipts
        .keys()
        .map(|(object_id, version, _)| {
            DfsMetadataVersionIdentity::new(object_id.clone(), version.clone())
        })
        .collect::<BTreeSet<_>>();
    for version in tables.versions.values() {
        let identity =
            DfsMetadataVersionIdentity::new(version.object_id.clone(), version.version.clone());
        if protected_versions.contains(&identity) {
            continue;
        }
        if versions_with_receipts.contains(&identity) {
            // Replica receipts are dependent durable evidence. Collect them
            // first and sweep the now-unreferenced version on a later pass,
            // so incremental replay never exposes a receipt whose version was
            // removed by an earlier bounded batch.
            continue;
        }
        let retained_at = offset_datetime_nanos(version.created_at);
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        report.eligible_version_count = report.eligible_version_count.saturating_add(1);
        versions_selected_for_removal.insert(identity);
        candidates.push(RemovalCandidate {
            retained_at_unix_nanos: retained_at,
            removal: DfsMetadataHistoryRemoval::Version {
                file_system_id: version.file_system_id.clone(),
                object_id: version.object_id.clone(),
                version: version.version.clone(),
            },
        });
    }

    for receipt in tables.intent_receipts.values() {
        let retained_at = offset_datetime_nanos(receipt.retained_at);
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        report.eligible_intent_receipt_count =
            report.eligible_intent_receipt_count.saturating_add(1);
        candidates.push(RemovalCandidate {
            retained_at_unix_nanos: retained_at,
            removal: DfsMetadataHistoryRemoval::IntentReceipt {
                file_system_id: receipt.file_system_id.clone(),
                object_id: receipt.object_id.clone(),
                intent_id: receipt.intent_id.clone(),
            },
        });
    }

    for receipt in tables.receipts.values() {
        let identity = DfsMetadataVersionIdentity::new(
            receipt.receipt.object_id.clone(),
            receipt.receipt.committed_version.clone(),
        );
        if protected_versions.contains(&identity) {
            continue;
        }
        let retained_at = offset_datetime_nanos(receipt.received_at);
        if !versions_selected_for_removal.contains(&identity)
            && retain_young(retained_at, now, options.min_history_age_nanos, &mut report)
        {
            continue;
        }
        report.eligible_receipt_count = report.eligible_receipt_count.saturating_add(1);
        candidates.push(RemovalCandidate {
            retained_at_unix_nanos: retained_at,
            removal: DfsMetadataHistoryRemoval::Receipt {
                file_system_id: receipt.receipt.file_system_id.clone(),
                object_id: receipt.receipt.object_id.clone(),
                version: receipt.receipt.committed_version.clone(),
                replica_node_id: receipt.replica_node_id.clone(),
            },
        });
    }

    for intent in tables.intents.values() {
        if intent_is_recovery_root(tables, intent) {
            continue;
        }
        let retained_at = offset_datetime_nanos(intent.updated_at.unwrap_or(intent.created_at));
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        report.eligible_intent_count = report.eligible_intent_count.saturating_add(1);
        candidates.push(RemovalCandidate {
            retained_at_unix_nanos: retained_at,
            removal: DfsMetadataHistoryRemoval::Intent {
                file_system_id: intent.file_system_id.clone(),
                object_id: intent.object_id.clone(),
                intent_id: intent.intent_id.clone(),
            },
        });
    }

    for conflict in tables
        .conflicts
        .values()
        .filter(|conflict| conflict.status != DfsConflictStatus::Open)
    {
        let retained_at = offset_datetime_nanos(conflict.detected_at);
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        report.eligible_conflict_count = report.eligible_conflict_count.saturating_add(1);
        candidates.push(RemovalCandidate {
            retained_at_unix_nanos: retained_at,
            removal: DfsMetadataHistoryRemoval::Conflict {
                file_system_id: conflict.file_system_id.clone(),
                object_id: conflict.object_id.clone(),
                conflict_id: conflict.conflict_id.clone(),
            },
        });
    }

    for ((object_id, version), commit) in &tables.visible_commits {
        let current_is_expected = tables
            .objects
            .get(object_id)
            .is_some_and(|object| object.version == commit.expected_version);
        if current_is_expected || visible_commit_should_recover(tables, commit) {
            continue;
        }
        let retained_at = offset_datetime_nanos(commit.created_at);
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        report.eligible_visible_commit_count =
            report.eligible_visible_commit_count.saturating_add(1);
        candidates.push(RemovalCandidate {
            retained_at_unix_nanos: retained_at,
            removal: DfsMetadataHistoryRemoval::VisibleCommit {
                file_system_id: commit.committed_object.file_system_id.clone(),
                object_id: object_id.clone(),
                version: version.clone(),
            },
        });
    }

    // Namespace jobs are selected here only when no pending durable request
    // can still use the full job as its success witness. The backend converts
    // selected terminal jobs to compact receipts rather than dropping their
    // canonical outcome.
    for job in tables
        .namespace_jobs
        .values()
        .filter(|job| job.status != crate::DfsNamespaceJobStatus::Running)
    {
        if namespace_job_has_pending_request_witness(tables, job) {
            continue;
        }
        let retained_at = offset_datetime_nanos(job.completed_at.unwrap_or(job.updated_at));
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        report.eligible_namespace_job_count = report.eligible_namespace_job_count.saturating_add(1);
        candidates.push(RemovalCandidate {
            retained_at_unix_nanos: retained_at,
            removal: DfsMetadataHistoryRemoval::NamespaceJob {
                file_system_id: job.file_system_id.clone(),
                job_id: job.job_id.clone(),
            },
        });
    }

    for transaction in tables
        .namespace_transactions
        .values()
        .filter(|transaction| namespace_transaction_recovery_is_complete(tables, transaction))
    {
        let retained_at = offset_datetime_nanos(transaction.created_at);
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        report.eligible_namespace_transaction_count = report
            .eligible_namespace_transaction_count
            .saturating_add(1);
        candidates.push(RemovalCandidate {
            retained_at_unix_nanos: retained_at,
            removal: DfsMetadataHistoryRemoval::NamespaceTransaction {
                file_system_id: transaction.file_system_id.clone(),
                transaction_id: transaction.transaction_id.clone(),
            },
        });
    }

    for prepare in tables
        .namespace_prepares
        .values()
        .filter(|prepare| !namespace_prepare_is_recovery_root(tables, prepare))
    {
        let retained_at = offset_datetime_nanos(prepare.transaction.created_at);
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        report.eligible_namespace_prepare_count =
            report.eligible_namespace_prepare_count.saturating_add(1);
        candidates.push(RemovalCandidate {
            retained_at_unix_nanos: retained_at,
            removal: DfsMetadataHistoryRemoval::NamespacePrepare {
                file_system_id: prepare.file_system_id.clone(),
                operation_id: prepare.operation_id.clone(),
            },
        });
    }

    for request in tables
        .namespace_requests
        .values()
        .filter(|request| request.status == DfsNamespaceRequestStatus::Completed)
    {
        let retained_at = offset_datetime_nanos(request.updated_at);
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        report.eligible_namespace_request_count =
            report.eligible_namespace_request_count.saturating_add(1);
        candidates.push(RemovalCandidate {
            retained_at_unix_nanos: retained_at,
            removal: DfsMetadataHistoryRemoval::NamespaceRequest {
                file_system_id: request.file_system_id.clone(),
                requester_node_id: request.requester_node_id.clone(),
                request_id: request.request_id.clone(),
            },
        });
    }

    for receipt in tables.namespace_job_receipts.values() {
        if namespace_job_receipt_has_pending_request_witness(tables, receipt) {
            continue;
        }
        let retained_at = offset_datetime_nanos(receipt.retained_at);
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        report.eligible_namespace_job_receipt_count = report
            .eligible_namespace_job_receipt_count
            .saturating_add(1);
        candidates.push(RemovalCandidate {
            retained_at_unix_nanos: retained_at,
            removal: DfsMetadataHistoryRemoval::NamespaceJobReceipt {
                file_system_id: receipt.file_system_id.clone(),
                job_id: receipt.job_id.clone(),
            },
        });
    }

    for receipt in tables.namespace_transaction_receipts.values() {
        let retained_at = offset_datetime_nanos(receipt.retained_at);
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        report.eligible_namespace_transaction_receipt_count = report
            .eligible_namespace_transaction_receipt_count
            .saturating_add(1);
        candidates.push(RemovalCandidate {
            retained_at_unix_nanos: retained_at,
            removal: DfsMetadataHistoryRemoval::NamespaceTransactionReceipt {
                file_system_id: receipt.file_system_id.clone(),
                transaction_id: receipt.transaction_id.clone(),
            },
        });
    }

    for receipt in tables.namespace_prepare_receipts.values() {
        let retained_at = offset_datetime_nanos(receipt.retained_at);
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        report.eligible_namespace_prepare_receipt_count = report
            .eligible_namespace_prepare_receipt_count
            .saturating_add(1);
        candidates.push(RemovalCandidate {
            retained_at_unix_nanos: retained_at,
            removal: DfsMetadataHistoryRemoval::NamespacePrepareReceipt {
                file_system_id: receipt.file_system_id.clone(),
                operation_id: receipt.operation_id.clone(),
            },
        });
    }

    for receipt in tables.namespace_request_receipts.values() {
        // A full request-owned prepare carries the canonical completion body
        // used to repair an interrupted commit. Keep its exact compact request
        // witness until the prepare itself has compacted; otherwise a bounded
        // pass could evict the receipt first, make the prepare incomplete, and
        // cause restart recovery to resurrect the full request body.
        if namespace_request_receipt_has_prepare_dependency(tables, receipt) {
            continue;
        }
        let retained_at = offset_datetime_nanos(receipt.retained_at);
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        report.eligible_namespace_request_receipt_count = report
            .eligible_namespace_request_receipt_count
            .saturating_add(1);
        candidates.push(RemovalCandidate {
            retained_at_unix_nanos: retained_at,
            removal: DfsMetadataHistoryRemoval::NamespaceRequestReceipt {
                file_system_id: receipt.file_system_id.clone(),
                requester_node_id: receipt.requester_node_id.clone(),
                request_id: receipt.request_id.clone(),
            },
        });
    }

    report.oldest_eligible_record_unix_nanos = candidates
        .iter()
        .map(|candidate| candidate.retained_at_unix_nanos)
        .min();
    candidates.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));
    let max_removals = options
        .max_removals
        .and_then(|limit| usize::try_from(limit).ok())
        .unwrap_or(usize::MAX);
    if !options.dry_run && candidates.len() > max_removals {
        report.max_removals_reached = true;
        candidates.truncate(max_removals);
    }
    let removals = if options.dry_run {
        Vec::new()
    } else {
        candidates
            .into_iter()
            .map(|candidate| candidate.removal)
            .collect::<Vec<_>>()
    };
    for removal in &removals {
        match removal {
            DfsMetadataHistoryRemoval::Receipt { .. } => {
                report.removed_receipt_count = report.removed_receipt_count.saturating_add(1);
            }
            DfsMetadataHistoryRemoval::Version { .. } => {
                report.removed_version_count = report.removed_version_count.saturating_add(1);
            }
            DfsMetadataHistoryRemoval::Intent { .. } => {
                report.compacted_intent_count = report.compacted_intent_count.saturating_add(1);
            }
            DfsMetadataHistoryRemoval::IntentReceipt { .. } => {
                report.removed_intent_receipt_count =
                    report.removed_intent_receipt_count.saturating_add(1);
            }
            DfsMetadataHistoryRemoval::Conflict { .. } => {
                report.removed_conflict_count = report.removed_conflict_count.saturating_add(1);
            }
            DfsMetadataHistoryRemoval::VisibleCommit { .. } => {
                report.removed_visible_commit_count =
                    report.removed_visible_commit_count.saturating_add(1);
            }
            DfsMetadataHistoryRemoval::NamespaceJob { .. } => {
                report.compacted_namespace_job_count =
                    report.compacted_namespace_job_count.saturating_add(1);
            }
            DfsMetadataHistoryRemoval::NamespaceTransaction { .. } => {
                report.compacted_namespace_transaction_count = report
                    .compacted_namespace_transaction_count
                    .saturating_add(1);
            }
            DfsMetadataHistoryRemoval::NamespacePrepare { .. } => {
                report.compacted_namespace_prepare_count =
                    report.compacted_namespace_prepare_count.saturating_add(1);
            }
            DfsMetadataHistoryRemoval::NamespaceRequest { .. } => {
                report.compacted_namespace_request_count =
                    report.compacted_namespace_request_count.saturating_add(1);
            }
            DfsMetadataHistoryRemoval::NamespaceJobReceipt { .. } => {
                report.removed_namespace_job_receipt_count =
                    report.removed_namespace_job_receipt_count.saturating_add(1);
            }
            DfsMetadataHistoryRemoval::NamespaceTransactionReceipt { .. } => {
                report.removed_namespace_transaction_receipt_count = report
                    .removed_namespace_transaction_receipt_count
                    .saturating_add(1);
            }
            DfsMetadataHistoryRemoval::NamespacePrepareReceipt { .. } => {
                report.removed_namespace_prepare_receipt_count = report
                    .removed_namespace_prepare_receipt_count
                    .saturating_add(1);
            }
            DfsMetadataHistoryRemoval::NamespaceRequestReceipt { .. } => {
                report.removed_namespace_request_receipt_count = report
                    .removed_namespace_request_receipt_count
                    .saturating_add(1);
            }
        }
    }

    let protected_content_hashes = protected_versions
        .iter()
        .filter_map(|identity| {
            tables
                .versions
                .get(&(identity.object_id.clone(), identity.version.clone()))
                .and_then(|version| version.content_hash.clone())
        })
        .chain(
            tables
                .objects
                .values()
                .filter_map(|object| object.content_hash.clone()),
        )
        .collect();
    report.protected_version_count = protected_versions.len() as u64;
    DfsMetadataHistoryGcPlan {
        report,
        removals,
        protected_versions,
        protected_content_hashes,
    }
}

pub(crate) fn metadata_content_protection_from_tables(
    tables: &DfsMetadataTables,
) -> DfsMetadataContentProtection {
    let mut versions = tables
        .versions
        .values()
        .map(|version| {
            DfsMetadataVersionIdentity::new(version.object_id.clone(), version.version.clone())
        })
        .collect::<BTreeSet<_>>();
    versions.extend(tables.objects.values().map(|object| {
        DfsMetadataVersionIdentity::new(object.object_id.clone(), object.version.clone())
    }));
    let mut content_hashes = tables
        .versions
        .values()
        .filter_map(|version| version.content_hash.clone())
        .chain(
            tables
                .objects
                .values()
                .filter_map(|object| object.content_hash.clone()),
        )
        .chain(
            tables
                .intents
                .values()
                .filter_map(|intent| intent.content_hash.clone()),
        )
        .chain(
            tables
                .intent_receipts
                .values()
                .filter_map(|receipt| receipt.content_hash.clone()),
        )
        .chain(
            tables
                .receipts
                .values()
                .filter_map(|receipt| receipt.content_hash.clone()),
        )
        .collect::<BTreeSet<_>>();
    for receipt in tables.receipts.values() {
        versions.insert(DfsMetadataVersionIdentity::new(
            receipt.receipt.object_id.clone(),
            receipt.receipt.committed_version.clone(),
        ));
    }
    for receipt in tables.intent_receipts.values() {
        versions.insert(DfsMetadataVersionIdentity::new(
            receipt.object_id.clone(),
            receipt.target_version.clone(),
        ));
    }
    for intent in tables.intents.values().filter(|intent| {
        write_intent_status_is_active(&intent.status) || intent_is_recovery_root(tables, intent)
    }) {
        if let Some(base_version) = intent.base_version.as_ref() {
            versions.insert(DfsMetadataVersionIdentity::new(
                intent.object_id.clone(),
                base_version.clone(),
            ));
        }
        versions.insert(DfsMetadataVersionIdentity::new(
            intent.object_id.clone(),
            intent.target_version.clone(),
        ));
    }
    for conflict in tables.conflicts.values() {
        for version in conflict
            .base_version
            .iter()
            .chain([&conflict.local_version, &conflict.remote_version])
        {
            versions.insert(DfsMetadataVersionIdentity::new(
                conflict.object_id.clone(),
                version.clone(),
            ));
        }
        content_hashes.extend(
            conflict
                .local_content_hash
                .iter()
                .chain(conflict.remote_content_hash.iter())
                .cloned(),
        );
    }
    for commit in tables.visible_commits.values() {
        versions.insert(DfsMetadataVersionIdentity::new(
            commit.visible_version.object_id.clone(),
            commit.visible_version.version.clone(),
        ));
        if let Some(content_hash) = commit.visible_version.content_hash.as_ref() {
            content_hashes.insert(content_hash.clone());
        }
    }
    for transaction in tables.namespace_transactions.values() {
        protect_namespace_transaction_versions(&mut versions, transaction);
        content_hashes.extend(
            transaction
                .objects
                .iter()
                .filter_map(|object| object.content_hash.clone())
                .chain(
                    transaction
                        .versions
                        .iter()
                        .filter_map(|version| version.content_hash.clone()),
                ),
        );
    }
    for prepare in tables.namespace_prepares.values() {
        for expected in &prepare.expected_object_versions {
            if let Some(version) = expected.expected_version.as_ref() {
                versions.insert(DfsMetadataVersionIdentity::new(
                    expected.object_id.clone(),
                    version.clone(),
                ));
            }
        }
        protect_namespace_transaction_versions(&mut versions, &prepare.transaction);
        content_hashes.extend(
            prepare
                .transaction
                .objects
                .iter()
                .filter_map(|object| object.content_hash.clone())
                .chain(
                    prepare
                        .transaction
                        .versions
                        .iter()
                        .filter_map(|version| version.content_hash.clone()),
                ),
        );
        for receipt in &prepare.content_receipts {
            versions.insert(DfsMetadataVersionIdentity::new(
                receipt.receipt.object_id.clone(),
                receipt.receipt.committed_version.clone(),
            ));
            if let Some(content_hash) = receipt.content_hash.as_ref() {
                content_hashes.insert(content_hash.clone());
            }
        }
    }
    for request in tables.namespace_requests.values() {
        protect_namespace_operation_versions(&mut versions, &request.operation);
        protect_namespace_operation_content_hashes(&mut content_hashes, &request.operation);
    }
    for job in tables.namespace_jobs.values() {
        protect_namespace_job_versions(&mut versions, job);
        for frame in &job.pending_frames {
            let content_hash = match frame {
                crate::DfsNamespaceJobFrame::CrossShardAdopt { moved_object, .. }
                | crate::DfsNamespaceJobFrame::CrossShardComplete { moved_object, .. } => {
                    moved_object.content_hash.as_ref()
                }
                crate::DfsNamespaceJobFrame::CrossShardHardlinkAdopt { alias, .. } => {
                    alias.content_hash.as_ref()
                }
                _ => None,
            };
            if let Some(content_hash) = content_hash {
                content_hashes.insert(content_hash.clone());
            }
        }
    }
    DfsMetadataContentProtection {
        versions: versions.into_iter().collect(),
        content_hashes: content_hashes.into_iter().collect(),
    }
}

fn namespace_job_has_pending_request_witness(
    tables: &DfsMetadataTables,
    job: &crate::DfsNamespaceJobRecord,
) -> bool {
    if let Some(origin) = job.origin.as_ref() {
        return tables
            .namespace_requests
            .get(&(origin.requester_node_id.clone(), origin.request_id.clone()))
            .is_some_and(|request| {
                request.status == DfsNamespaceRequestStatus::Pending
                    && origin.matches_request(request)
            });
    }
    // Legacy jobs do not carry an exact request link. Retain them whenever any
    // pending request could plausibly use the path/time witness.
    tables.namespace_requests.values().any(|request| {
        request.status == DfsNamespaceRequestStatus::Pending
            && request.file_system_id == job.file_system_id
            && request.created_at <= job.created_at
            && legacy_namespace_job_matches_operation(
                &job.kind,
                &job.job_id,
                &job.root_path,
                job.target_path.as_deref(),
                &request.operation,
            )
    })
}

fn namespace_job_receipt_has_pending_request_witness(
    tables: &DfsMetadataTables,
    receipt: &crate::DfsNamespaceJobReceipt,
) -> bool {
    if let Some(origin) = receipt.origin.as_ref() {
        return tables
            .namespace_requests
            .get(&(origin.requester_node_id.clone(), origin.request_id.clone()))
            .is_some_and(|request| {
                request.status == DfsNamespaceRequestStatus::Pending
                    && origin.matches_request(request)
            });
    }
    tables.namespace_requests.values().any(|request| {
        request.status == DfsNamespaceRequestStatus::Pending
            && request.file_system_id == receipt.file_system_id
            && request.created_at <= receipt.created_at
            && legacy_namespace_job_matches_operation(
                &receipt.kind,
                &receipt.job_id,
                &receipt.root_path,
                receipt.target_path.as_deref(),
                &request.operation,
            )
    })
}

fn legacy_namespace_job_matches_operation(
    kind: &crate::DfsNamespaceJobKind,
    job_id: &str,
    root_path: &str,
    target_path: Option<&str>,
    operation: &DfsNamespaceOperation,
) -> bool {
    match operation {
        DfsNamespaceOperation::ResumeNamespaceJobBounded {
            job_id: request_job_id,
            ..
        } => request_job_id == job_id,
        DfsNamespaceOperation::ClearDirRecursive { path } => {
            kind == &crate::DfsNamespaceJobKind::ClearDirectoryRecursive && path == root_path
        }
        DfsNamespaceOperation::Hardlink {
            existing_path,
            link_path,
        } => {
            kind == &crate::DfsNamespaceJobKind::HardlinkAcrossShardPrimaries
                && existing_path == root_path
                && target_path == Some(link_path.as_str())
        }
        DfsNamespaceOperation::Unlink { path }
        | DfsNamespaceOperation::ConditionalUnlink { path, .. } => {
            kind == &crate::DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries
                && path == root_path
                && target_path == Some(path.as_str())
        }
        DfsNamespaceOperation::Rename { from, to }
        | DfsNamespaceOperation::ConditionalRename { from, to, .. }
        | DfsNamespaceOperation::RenameDirectoryBounded { from, to, .. } => {
            matches!(
                kind,
                crate::DfsNamespaceJobKind::RenameDirectory
                    | crate::DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries
                    | crate::DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries
                    | crate::DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
            ) && from == root_path
                && target_path == Some(to.as_str())
        }
        _ => false,
    }
}

fn namespace_request_receipt_has_prepare_dependency(
    tables: &DfsMetadataTables,
    receipt: &crate::DfsNamespaceRequestReceipt,
) -> bool {
    tables.namespace_prepares.values().any(|prepare| {
        prepare.status != DfsNamespacePrepareStatus::Aborted
            && (prepare
                .completed_request
                .as_ref()
                .is_some_and(|completion| {
                    completion.requester_node_id == receipt.requester_node_id
                        && completion.request_id == receipt.request_id
                        && receipt.matches_completed_request(completion)
                })
                || prepare
                    .completed_request_completion
                    .as_ref()
                    .is_some_and(|completion| {
                        completion.requester_node_id == receipt.requester_node_id
                            && completion.request_id == receipt.request_id
                            && receipt.matches_completion(completion)
                    }))
    })
}

fn intent_is_recovery_root(
    tables: &DfsMetadataTables,
    intent: &crate::DfsWriteIntentRecord,
) -> bool {
    if write_intent_status_is_active(&intent.status) {
        return true;
    }
    if tables
        .conflicts
        .values()
        .any(|conflict| conflict.created_by_intent_id.as_deref() == Some(intent.intent_id.as_str()))
        || tables
            .namespace_prepares
            .values()
            .any(|prepare| prepare.operation_id == format!("namespace:create:{}", intent.intent_id))
    {
        return true;
    }
    if intent.status != DfsWriteIntentStatus::Committed {
        return false;
    }
    if tables
        .objects
        .get(&intent.object_id)
        .is_none_or(|object| object.version != intent.target_version)
    {
        return false;
    }
    let receipt_nodes = tables
        .receipts
        .iter()
        .filter_map(|((object_id, version, node_id), receipt)| {
            (object_id == &intent.object_id
                && version == &intent.target_version
                && intent
                    .content_hash
                    .as_ref()
                    .is_none_or(|expected| receipt.content_hash.as_ref() == Some(expected)))
            .then_some(node_id)
        })
        .collect::<BTreeSet<_>>();
    intent
        .desired_replica_node_ids
        .iter()
        .any(|node_id| !receipt_nodes.contains(node_id))
}

fn namespace_prepare_is_recovery_root(
    tables: &DfsMetadataTables,
    prepare: &crate::DfsNamespacePrepareRecord,
) -> bool {
    match prepare.status {
        DfsNamespacePrepareStatus::Prepared => true,
        DfsNamespacePrepareStatus::Aborted => false,
        DfsNamespacePrepareStatus::Committed => {
            !namespace_transaction_recovery_is_complete(tables, &prepare.transaction)
                || prepare.content_receipts.iter().any(|receipt| {
                    !tables
                        .receipts
                        .get(&(
                            receipt.receipt.object_id.clone(),
                            receipt.receipt.committed_version.clone(),
                            receipt.replica_node_id.clone(),
                        ))
                        .is_some_and(|existing| replay_receipt_matches_existing(existing, receipt))
                })
                || prepare
                    .completed_request
                    .as_ref()
                    .is_some_and(|completion| {
                        let key = (
                            completion.requester_node_id.clone(),
                            completion.request_id.clone(),
                        );
                        !tables.namespace_requests.get(&key).is_some_and(|request| {
                            request.status == DfsNamespaceRequestStatus::Completed
                                && request.operation_id == completion.operation_id
                                && request.command_digest == completion.command_digest
                                && request.result == completion.result
                                && request.posix_coordination_result
                                    == completion.posix_coordination_result
                        }) && !tables
                            .namespace_request_receipts
                            .get(&key)
                            .is_some_and(|receipt| receipt.matches_completed_request(completion))
                    })
                || prepare
                    .completed_request_completion
                    .as_ref()
                    .is_some_and(|completion| {
                        let key = completion.request_key();
                        !tables
                            .namespace_requests
                            .get(&key)
                            .is_some_and(|request| completion.matches_completed_request(request))
                            && !tables
                                .namespace_request_receipts
                                .get(&key)
                                .is_some_and(|receipt| receipt.matches_completion(completion))
                    })
        }
    }
}

fn protect_namespace_operation_versions(
    protected: &mut BTreeSet<DfsMetadataVersionIdentity>,
    operation: &DfsNamespaceOperation,
) {
    match operation {
        DfsNamespaceOperation::ConditionalUnlink { expected, .. }
        | DfsNamespaceOperation::ConditionalRmdir { expected, .. } => {
            protected.insert(DfsMetadataVersionIdentity::new(
                expected.object_id.clone(),
                expected.version.clone(),
            ));
        }
        DfsNamespaceOperation::ConditionalRename {
            expected_source,
            expected_destination,
            ..
        } => {
            protected.insert(DfsMetadataVersionIdentity::new(
                expected_source.object_id.clone(),
                expected_source.version.clone(),
            ));
            if let DfsNamespaceDestinationPrecondition::Exact(expected) = expected_destination {
                protected.insert(DfsMetadataVersionIdentity::new(
                    expected.object_id.clone(),
                    expected.version.clone(),
                ));
            }
        }
        DfsNamespaceOperation::MkdirAtNoLinks {
            expected_parent_object_id,
            expected_parent_version,
            ..
        } => {
            protected.insert(DfsMetadataVersionIdentity::new(
                expected_parent_object_id.clone(),
                expected_parent_version.clone(),
            ));
        }
        DfsNamespaceOperation::CommitLocalEventualBatch {
            transaction,
            writes,
        } => {
            protect_namespace_transaction_versions(protected, transaction);
            for write in writes {
                if let Some(version) = write.expected_base_version.as_ref() {
                    protected.insert(DfsMetadataVersionIdentity::new(
                        write.receipt.receipt.object_id.clone(),
                        version.clone(),
                    ));
                }
                protected.insert(DfsMetadataVersionIdentity::new(
                    write.receipt.receipt.object_id.clone(),
                    write.receipt.receipt.committed_version.clone(),
                ));
            }
        }
        DfsNamespaceOperation::ReconcilePlacement {
            object_id,
            expected_version,
            ..
        } => {
            protected.insert(DfsMetadataVersionIdentity::new(
                object_id.clone(),
                expected_version.clone(),
            ));
        }
        DfsNamespaceOperation::FileSync { target, .. } => {
            protected.insert(DfsMetadataVersionIdentity::new(
                target.object_id.clone(),
                target.version.clone(),
            ));
        }
        DfsNamespaceOperation::DirectorySync { target, .. } => {
            protected.insert(DfsMetadataVersionIdentity::new(
                target.object_id.clone(),
                target.version.clone(),
            ));
        }
        DfsNamespaceOperation::AdoptObjectAtPath {
            object,
            versions,
            receipts,
            intents,
            intent_receipts,
            conflicts,
            ..
        } => {
            protected.insert(DfsMetadataVersionIdentity::new(
                object.object_id.clone(),
                object.version.clone(),
            ));
            for version in versions {
                protected.insert(DfsMetadataVersionIdentity::new(
                    version.object_id.clone(),
                    version.version.clone(),
                ));
            }
            for receipt in receipts {
                protected.insert(DfsMetadataVersionIdentity::new(
                    receipt.receipt.object_id.clone(),
                    receipt.receipt.committed_version.clone(),
                ));
            }
            for intent in intents {
                if let Some(version) = intent.base_version.as_ref() {
                    protected.insert(DfsMetadataVersionIdentity::new(
                        intent.object_id.clone(),
                        version.clone(),
                    ));
                }
                protected.insert(DfsMetadataVersionIdentity::new(
                    intent.object_id.clone(),
                    intent.target_version.clone(),
                ));
            }
            for receipt in intent_receipts {
                protected.insert(DfsMetadataVersionIdentity::new(
                    receipt.object_id.clone(),
                    receipt.target_version.clone(),
                ));
            }
            for conflict in conflicts {
                for version in conflict
                    .base_version
                    .iter()
                    .chain([&conflict.local_version, &conflict.remote_version])
                {
                    protected.insert(DfsMetadataVersionIdentity::new(
                        conflict.object_id.clone(),
                        version.clone(),
                    ));
                }
            }
        }
        DfsNamespaceOperation::AdoptHardlinkAlias { alias, .. } => {
            protected.insert(DfsMetadataVersionIdentity::new(
                alias.object_id.clone(),
                alias.version.clone(),
            ));
        }
        _ => {}
    }
}

fn protect_namespace_operation_content_hashes(
    protected: &mut BTreeSet<String>,
    operation: &DfsNamespaceOperation,
) {
    match operation {
        DfsNamespaceOperation::CommitLocalEventualBatch {
            transaction,
            writes,
        } => {
            protected.extend(
                transaction
                    .objects
                    .iter()
                    .filter_map(|object| object.content_hash.clone())
                    .chain(
                        transaction
                            .versions
                            .iter()
                            .filter_map(|version| version.content_hash.clone()),
                    )
                    .chain(
                        writes
                            .iter()
                            .filter_map(|write| write.receipt.content_hash.clone()),
                    ),
            );
        }
        DfsNamespaceOperation::FileSync { target, .. } => {
            protected.insert(target.content_hash.clone());
        }
        DfsNamespaceOperation::AdoptObjectAtPath {
            object,
            versions,
            receipts,
            intents,
            intent_receipts,
            conflicts,
            ..
        } => {
            protected.extend(
                object
                    .content_hash
                    .iter()
                    .cloned()
                    .chain(
                        versions
                            .iter()
                            .filter_map(|version| version.content_hash.clone()),
                    )
                    .chain(
                        receipts
                            .iter()
                            .filter_map(|receipt| receipt.content_hash.clone()),
                    )
                    .chain(
                        intents
                            .iter()
                            .filter_map(|intent| intent.content_hash.clone()),
                    )
                    .chain(
                        intent_receipts
                            .iter()
                            .filter_map(|receipt| receipt.content_hash.clone()),
                    )
                    .chain(conflicts.iter().flat_map(|conflict| {
                        conflict
                            .local_content_hash
                            .iter()
                            .chain(conflict.remote_content_hash.iter())
                            .cloned()
                    })),
            );
        }
        DfsNamespaceOperation::AdoptHardlinkAlias { alias, .. } => {
            protected.extend(alias.content_hash.iter().cloned());
        }
        _ => {}
    }
}

fn protect_namespace_job_versions(
    protected: &mut BTreeSet<DfsMetadataVersionIdentity>,
    job: &crate::DfsNamespaceJobRecord,
) {
    for frame in &job.pending_frames {
        let object = match frame {
            crate::DfsNamespaceJobFrame::CrossShardAdopt { moved_object, .. }
            | crate::DfsNamespaceJobFrame::CrossShardComplete { moved_object, .. } => {
                Some(moved_object)
            }
            crate::DfsNamespaceJobFrame::CrossShardHardlinkAdopt { alias, .. } => Some(alias),
            _ => None,
        };
        if let Some(object) = object {
            protected.insert(DfsMetadataVersionIdentity::new(
                object.object_id.clone(),
                object.version.clone(),
            ));
        }
    }
}

fn protect_namespace_transaction_versions(
    protected: &mut BTreeSet<DfsMetadataVersionIdentity>,
    transaction: &crate::DfsNamespaceTransaction,
) {
    for object in &transaction.objects {
        protected.insert(DfsMetadataVersionIdentity::new(
            object.object_id.clone(),
            object.version.clone(),
        ));
    }
    for version in &transaction.versions {
        protected.insert(DfsMetadataVersionIdentity::new(
            version.object_id.clone(),
            version.version.clone(),
        ));
    }
}

fn retain_young(
    retained_at_unix_nanos: u64,
    now_unix_nanos: u64,
    min_age_nanos: u64,
    report: &mut DfsMetadataHistoryGcReport,
) -> bool {
    if now_unix_nanos.saturating_sub(retained_at_unix_nanos) >= min_age_nanos {
        return false;
    }
    report.retained_young_record_count = report.retained_young_record_count.saturating_add(1);
    let eligible_at = retained_at_unix_nanos.saturating_add(min_age_nanos);
    report.next_eligible_at_unix_nanos = Some(
        report
            .next_eligible_at_unix_nanos
            .map(|current| current.min(eligible_at))
            .unwrap_or(eligible_at),
    );
    true
}

fn push_stats<'a, T: Serialize + 'a>(
    stats: &mut Vec<DfsMetadataRetainedTableStats>,
    table: &str,
    records: impl Iterator<Item = &'a T>,
    mut timestamp: impl FnMut(&T) -> Option<u64>,
) {
    let mut table_stats = DfsMetadataRetainedTableStats {
        table: table.to_string(),
        ..DfsMetadataRetainedTableStats::default()
    };
    for record in records {
        table_stats.record_count = table_stats.record_count.saturating_add(1);
        table_stats.estimated_serialized_bytes =
            table_stats.estimated_serialized_bytes.saturating_add(
                serde_json::to_vec(record)
                    .map(|encoded| encoded.len() as u64)
                    .unwrap_or(0),
            );
        table_stats.oldest_record_unix_nanos = table_stats
            .oldest_record_unix_nanos
            .into_iter()
            .chain(timestamp(record))
            .min();
    }
    stats.push(table_stats);
}

fn push_retention_stats<T: Serialize>(
    stats: &mut Vec<DfsMetadataRetainedTableStats>,
    table: &str,
    retention: &T,
    record_count: u64,
) {
    let estimated_serialized_bytes = if record_count == 0 {
        0
    } else {
        serde_json::to_vec(retention)
            .map(|encoded| usize_to_u64_saturating(encoded.len()))
            .unwrap_or(0)
    };
    stats.push(DfsMetadataRetainedTableStats {
        table: table.to_string(),
        record_count,
        estimated_serialized_bytes,
        oldest_record_unix_nanos: None,
        oldest_record_age_nanos: None,
    });
}

fn intent_receipt_retention_record_count(
    retention: &crate::metadata_records::DfsWriteIntentReceiptRetentionState,
) -> u64 {
    let generated_floor_count = retention
        .max_evicted_counter_by_object_and_coordinator
        .values()
        .map(|floors| usize_to_u64_saturating(floors.len()))
        .fold(0_u64, u64::saturating_add);
    let exact_opaque_id_count = retention
        .evicted_opaque_intent_ids_by_object
        .values()
        .map(|intent_ids| usize_to_u64_saturating(intent_ids.len()))
        .fold(0_u64, u64::saturating_add);
    generated_floor_count
        .saturating_add(exact_opaque_id_count)
        .saturating_add(usize_to_u64_saturating(
            retention.opaque_intent_history_evicted_object_ids.len(),
        ))
}

fn namespace_receipt_retention_record_count(
    retention: &crate::metadata_records::DfsNamespaceReceiptRetentionState,
) -> u64 {
    u64::from(retention.transaction_floor.is_some())
        .saturating_add(u64::from(retention.prepare_floor.is_some()))
        .saturating_add(u64::from(retention.job_floor.is_some()))
        .saturating_add(usize_to_u64_saturating(
            retention.max_evicted_request_order_token_by_requester.len(),
        ))
        .saturating_add(usize_to_u64_saturating(
            retention
                .opaque_request_history_evicted_requester_node_ids
                .len(),
        ))
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn duration_nanos_saturating(duration: Duration) -> u64 {
    duration.as_nanos().try_into().unwrap_or(u64::MAX)
}

fn offset_datetime_nanos(value: OffsetDateTime) -> u64 {
    value.unix_timestamp_nanos().try_into().unwrap_or(0)
}

fn metadata_history_now_unix_nanos() -> u64 {
    offset_datetime_nanos(OffsetDateTime::now_utc())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DfsConflictRecord, DfsCrossShardPinTransferRecord, DfsMetadataVersionPinRecord,
        DfsNamespaceJobKind, DfsNamespaceJobOrigin, DfsNamespaceJobRecord, DfsNamespaceJobStatus,
        DfsNamespaceRequestRecord, DfsNamespaceResult, DfsObjectKind, DfsObjectMetadata,
        DfsReplicationPolicy, DfsWriteIntentRecord,
    };
    use sinomo_core::{DfsObjectPolicy, node::NodeId};

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("sifs").expect("filesystem id")
    }

    fn object_id(value: &str) -> DfsObjectId {
        DfsObjectId::from_legacy_named(value).expect("object id")
    }

    fn version(value: &str) -> DfsVersion {
        DfsVersion::from_legacy_named(value).expect("version")
    }

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("node id")
    }

    fn timestamp(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("timestamp")
    }

    fn options() -> DfsMetadataHistoryGcOptions {
        DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 1)
            .with_now_unix_nanos(100_000_000_000)
    }

    fn version_record(
        object_id: &DfsObjectId,
        version: &DfsVersion,
        created_at_seconds: i64,
    ) -> DfsObjectVersionRecord {
        DfsObjectVersionRecord {
            file_system_id: file_system_id(),
            object_id: object_id.clone(),
            version: version.clone(),
            content_hash: Some(format!(
                "blake3:{:064x}",
                u64::try_from(created_at_seconds).unwrap_or_default()
            )),
            size_bytes: Some(1),
            locations: vec![node("node_owner")],
            created_by_event_id: None,
            created_at: timestamp(created_at_seconds),
            visible: true,
        }
    }

    fn current_object(object_id: &DfsObjectId, version: &DfsVersion) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system_id(),
            path: format!("/{}", object_id.as_str()),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: node("node_owner"),
            current_locations: vec![node("node_owner")],
            content_hash: Some("blake3:current".to_string()),
            content_size_bytes: Some(1),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn insert_version(
        tables: &mut DfsMetadataTables,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        created_at_seconds: i64,
    ) {
        tables.versions.insert(
            (object_id.clone(), version.clone()),
            version_record(object_id, version, created_at_seconds),
        );
    }

    fn version_is_selected(
        plan: &DfsMetadataHistoryGcPlan,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> bool {
        plan.removals.iter().any(|removal| {
            matches!(
                removal,
                DfsMetadataHistoryRemoval::Version {
                    object_id: selected_object_id,
                    version: selected_version,
                    ..
                } if selected_object_id == object_id && selected_version == version
            )
        })
    }

    fn namespace_job_is_selected(plan: &DfsMetadataHistoryGcPlan, job_id: &str) -> bool {
        plan.removals.iter().any(|removal| {
            matches!(
                removal,
                DfsMetadataHistoryRemoval::NamespaceJob {
                    job_id: selected_job_id,
                    ..
                } if selected_job_id == job_id
            )
        })
    }

    fn namespace_request(
        requester_node_id: NodeId,
        request_id: &str,
        status: DfsNamespaceRequestStatus,
    ) -> DfsNamespaceRequestRecord {
        DfsNamespaceRequestRecord {
            file_system_id: file_system_id(),
            requester_node_id,
            request_id: request_id.to_string(),
            operation_id: format!("operation_{request_id}"),
            authority_shard_id: DfsMetadataShardId::new("shard_namespace"),
            operation: DfsNamespaceOperation::Rename {
                from: "/source".to_string(),
                to: "/target".to_string(),
            },
            command_digest: format!("digest_{request_id}"),
            status: status.clone(),
            result: (status == DfsNamespaceRequestStatus::Completed)
                .then_some(DfsNamespaceResult::Applied),
            posix_coordination_result: None,
            recovery_attempts: 0,
            last_recovery_error: None,
            created_at: timestamp(1),
            updated_at: timestamp(1),
        }
    }

    fn completed_namespace_job(
        job_id: &str,
        origin: Option<DfsNamespaceJobOrigin>,
    ) -> DfsNamespaceJobRecord {
        DfsNamespaceJobRecord {
            job_id: job_id.to_string(),
            file_system_id: file_system_id(),
            origin,
            kind: DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
            status: DfsNamespaceJobStatus::Completed,
            root_path: "/source".to_string(),
            target_path: Some("/target".to_string()),
            pending_frames: Vec::new(),
            processed_object_count: 1,
            created_at: timestamp(2),
            updated_at: timestamp(3),
            completed_at: Some(timestamp(3)),
            failure_reason: None,
        }
    }

    #[test]
    fn metadata_history_stats_count_nested_retention_floors_and_version_pins() {
        let mut tables = DfsMetadataTables::default();
        let first_object = object_id("obj_stats_floor_first");
        let second_object = object_id("obj_stats_floor_second");
        tables
            .intent_receipt_retention
            .max_evicted_counter_by_object_and_coordinator
            .entry(first_object.clone())
            .or_default()
            .extend([
                (node("node_stats_floor_first"), 7),
                (node("node_stats_floor_second"), 9),
            ]);
        tables
            .intent_receipt_retention
            .max_evicted_counter_by_object_and_coordinator
            .entry(second_object.clone())
            .or_default()
            .insert(node("node_stats_floor_third"), 11);
        tables
            .intent_receipt_retention
            .opaque_intent_history_evicted_object_ids
            .extend([
                object_id("obj_stats_opaque_first"),
                object_id("obj_stats_opaque_second"),
            ]);
        tables
            .intent_receipt_retention
            .evicted_opaque_intent_ids_by_object
            .entry(first_object.clone())
            .or_default()
            .extend([
                "dfs_write_op_stats_first".to_string(),
                "dfs_write_op_stats_second".to_string(),
            ]);

        use crate::metadata_records::DfsNamespaceReceiptOrderKey;
        tables.namespace_receipt_retention.transaction_floor = Some(DfsNamespaceReceiptOrderKey {
            created_at: timestamp(1),
            identity: "transaction_floor".to_string(),
        });
        tables.namespace_receipt_retention.prepare_floor = Some(DfsNamespaceReceiptOrderKey {
            created_at: timestamp(2),
            identity: "prepare_floor".to_string(),
        });
        tables.namespace_receipt_retention.job_floor = Some(DfsNamespaceReceiptOrderKey {
            created_at: timestamp(3),
            identity: "job_floor".to_string(),
        });
        tables
            .namespace_receipt_retention
            .max_evicted_request_order_token_by_requester
            .extend([
                (
                    node("node_stats_requester_first"),
                    "request-token-first".to_string(),
                ),
                (
                    node("node_stats_requester_second"),
                    "request-token-second".to_string(),
                ),
            ]);
        tables
            .namespace_receipt_retention
            .opaque_request_history_evicted_requester_node_ids
            .insert(node("node_stats_requester_opaque"));

        let pin = DfsMetadataVersionPinRecord {
            file_system_id: file_system_id(),
            object_id: first_object.clone(),
            version: version("version_stats_pinned"),
            pin_id: "checkpoint:stats".to_string(),
            created_at: timestamp(4),
        };
        tables.version_pins.insert(
            (
                pin.object_id.clone(),
                pin.version.clone(),
                pin.pin_id.clone(),
            ),
            pin,
        );

        let stats = metadata_history_stats_from_tables(
            DfsMetadataShardId::new("shard_stats"),
            &tables,
            None,
        );
        let table = |name: &str| {
            stats
                .tables
                .iter()
                .find(|table| table.table == name)
                .expect("retained table stats")
        };
        let intent_retention = table("intent-receipt-retention");
        assert_eq!(
            intent_retention.record_count, 7,
            "each nested object/coordinator floor, exact opaque id, and legacy opaque object is one retained entry"
        );
        assert_eq!(
            intent_retention.estimated_serialized_bytes,
            serde_json::to_vec(&tables.intent_receipt_retention)
                .expect("serialize intent retention")
                .len() as u64
        );
        let namespace_retention = table("namespace-receipt-retention");
        assert_eq!(
            namespace_retention.record_count, 6,
            "three typed floors, two requester floors, and one opaque requester are retained entries"
        );
        assert_eq!(
            namespace_retention.estimated_serialized_bytes,
            serde_json::to_vec(&tables.namespace_receipt_retention)
                .expect("serialize namespace retention")
                .len() as u64
        );
        let version_pins = table("version-pins");
        assert_eq!(version_pins.record_count, 1);
        assert_eq!(
            version_pins.oldest_record_unix_nanos,
            Some(offset_datetime_nanos(timestamp(4)))
        );
        assert!(stats.collection_authorized);
        assert_eq!(stats.total_record_count, 14);
    }

    #[test]
    fn metadata_history_report_merge_keeps_oldest_authorized_candidate() {
        let mut report = DfsMetadataHistoryGcReport {
            oldest_eligible_record_unix_nanos: Some(20),
            ..Default::default()
        };
        report.merge(DfsMetadataHistoryGcReport {
            oldest_eligible_record_unix_nanos: Some(10),
            ..Default::default()
        });
        assert_eq!(report.oldest_eligible_record_unix_nanos, Some(10));
    }

    #[test]
    fn metadata_history_gc_retains_current_and_selects_old_unrooted_version() {
        let object_id = object_id("obj_history_current");
        let old_version = version("version_history_old");
        let current_version = version("version_history_current");
        let mut tables = DfsMetadataTables::default();
        insert_version(&mut tables, &object_id, &old_version, 1);
        insert_version(&mut tables, &object_id, &current_version, 2);
        tables.objects.insert(
            object_id.clone(),
            current_object(&object_id, &current_version),
        );

        let plan = plan_metadata_history_gc(
            DfsMetadataShardId::new("shard_history"),
            &tables,
            &options(),
            None,
        );

        assert!(version_is_selected(&plan, &object_id, &old_version));
        assert!(!version_is_selected(&plan, &object_id, &current_version));
        assert_eq!(
            plan.report.oldest_eligible_record_unix_nanos,
            Some(offset_datetime_nanos(timestamp(1)))
        );
        assert!(
            plan.protected_versions
                .contains(&DfsMetadataVersionIdentity::new(object_id, current_version))
        );
        assert_eq!(plan.report.eligible_version_count, 1);
        assert_eq!(plan.report.removed_version_count, 1);
    }

    #[test]
    fn metadata_history_gc_roots_active_pin_transfer_versions_and_reports_the_table() {
        let object_id = object_id("obj_active_pin_transfer");
        let target_version = version("version_transfer_target");
        let copied_pin_version = version("version_transfer_copied_pin");
        let unrooted_version = version("version_transfer_unrooted");
        let current_version = version("version_transfer_current");
        let mut tables = DfsMetadataTables::default();
        for (version, created_at) in [
            (&target_version, 1),
            (&copied_pin_version, 2),
            (&unrooted_version, 3),
            (&current_version, 4),
        ] {
            insert_version(&mut tables, &object_id, version, created_at);
        }
        tables.objects.insert(
            object_id.clone(),
            current_object(&object_id, &current_version),
        );
        let copied_pin = DfsMetadataVersionPinRecord {
            file_system_id: file_system_id(),
            object_id: object_id.clone(),
            version: copied_pin_version.clone(),
            pin_id: "checkpoint:active-transfer".to_string(),
            created_at: timestamp(2),
        };
        let acknowledged_pin_version = version("acknowledged-pin");
        tables.versions.insert(
            (object_id.clone(), acknowledged_pin_version.clone()),
            version_record(&object_id, &acknowledged_pin_version, 2),
        );
        let acknowledged_pin = DfsMetadataVersionPinRecord {
            file_system_id: file_system_id(),
            object_id: object_id.clone(),
            version: acknowledged_pin_version.clone(),
            pin_id: "checkpoint:acknowledged-transfer".to_string(),
            created_at: timestamp(3),
        };
        tables.version_pins.insert(
            (
                copied_pin.object_id.clone(),
                copied_pin.version.clone(),
                copied_pin.pin_id.clone(),
            ),
            copied_pin.clone(),
        );
        tables.version_pins.insert(
            (
                acknowledged_pin.object_id.clone(),
                acknowledged_pin.version.clone(),
                acknowledged_pin.pin_id.clone(),
            ),
            acknowledged_pin.clone(),
        );
        let transfer = DfsCrossShardPinTransferRecord {
            transfer_id: "transfer-active".to_string(),
            file_system_id: file_system_id(),
            object_id: object_id.clone(),
            source_path: "source".to_string(),
            target_path: "target".to_string(),
            target_version: target_version.clone(),
            source_pins: vec![acknowledged_pin, copied_pin.clone()],
            copied_pins: vec![copied_pin],
            created_at: timestamp(5),
            updated_at: timestamp(6),
        };
        tables
            .cross_shard_pin_transfers
            .insert((object_id.clone(), transfer.transfer_id.clone()), transfer);

        let plan = plan_metadata_history_gc(
            DfsMetadataShardId::new("shard_active_pin_transfer"),
            &tables,
            &options(),
            None,
        );

        for protected_version in [
            &target_version,
            &copied_pin_version,
            &acknowledged_pin_version,
            &current_version,
        ] {
            assert!(
                !version_is_selected(&plan, &object_id, protected_version),
                "active transfer version {protected_version} must remain rooted"
            );
        }
        assert!(version_is_selected(&plan, &object_id, &unrooted_version));
        let transfer_stats = plan.report.shards[0]
            .tables
            .iter()
            .find(|table| table.table == "cross-shard-pin-transfers")
            .expect("cross-shard transfer table stats");
        assert_eq!(transfer_stats.record_count, 1);
        assert_eq!(
            transfer_stats.oldest_record_unix_nanos,
            Some(offset_datetime_nanos(timestamp(5)))
        );
    }

    #[test]
    fn metadata_history_gc_retains_open_conflict_and_active_intent_version_roots() {
        let mut tables = DfsMetadataTables::default();

        let conflict_object_id = object_id("obj_history_conflict");
        let conflict_base = version("version_conflict_base");
        let conflict_local = version("version_conflict_local");
        let conflict_remote = version("version_conflict_remote");
        let conflict_newest = version("version_conflict_newest");
        for (version, created_at) in [
            (&conflict_base, 1),
            (&conflict_local, 2),
            (&conflict_remote, 3),
            (&conflict_newest, 4),
        ] {
            insert_version(&mut tables, &conflict_object_id, version, created_at);
        }
        let conflict = DfsConflictRecord {
            conflict_id: "conflict_history_open".to_string(),
            file_system_id: file_system_id(),
            object_id: conflict_object_id.clone(),
            path: "/conflict".to_string(),
            base_version: Some(conflict_base.clone()),
            local_version: conflict_local.clone(),
            remote_version: conflict_remote.clone(),
            detected_by_node_id: node("node_detector"),
            reason: "test conflict root".to_string(),
            local_content_hash: None,
            remote_content_hash: None,
            created_by_intent_id: None,
            strategy: None,
            status: DfsConflictStatus::Open,
            detected_at: timestamp(1),
        };
        tables.conflicts.insert(
            (conflict_object_id.clone(), conflict.conflict_id.clone()),
            conflict,
        );

        let intent_object_id = object_id("obj_history_intent");
        let intent_base = version("version_intent_base");
        let intent_target = version("version_intent_target");
        let intent_newest = version("version_intent_newest");
        for (version, created_at) in [(&intent_base, 1), (&intent_target, 2), (&intent_newest, 3)] {
            insert_version(&mut tables, &intent_object_id, version, created_at);
        }
        let intent = DfsWriteIntentRecord {
            intent_id: "intent_history_active_1".to_string(),
            file_system_id: file_system_id(),
            object_id: intent_object_id.clone(),
            base_version: Some(intent_base.clone()),
            target_version: intent_target.clone(),
            coordinator_node_id: node("node_coordinator"),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("replication policy"),
            status: DfsWriteIntentStatus::Staging,
            content_hash: Some("blake3:intent".to_string()),
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: timestamp(1),
            updated_at: None,
        };
        tables
            .intents
            .insert((intent_object_id.clone(), intent.intent_id.clone()), intent);

        let plan = plan_metadata_history_gc(
            DfsMetadataShardId::new("shard_roots"),
            &tables,
            &options(),
            None,
        );

        for (object_id, version) in [
            (&conflict_object_id, &conflict_base),
            (&conflict_object_id, &conflict_local),
            (&conflict_object_id, &conflict_remote),
            (&intent_object_id, &intent_base),
            (&intent_object_id, &intent_target),
        ] {
            let identity = DfsMetadataVersionIdentity::new(object_id.clone(), version.clone());
            assert!(plan.protected_versions.contains(&identity));
            assert!(!version_is_selected(&plan, object_id, version));
        }
        assert!(!plan.removals.iter().any(|removal| matches!(
            removal,
            DfsMetadataHistoryRemoval::Intent { .. } | DfsMetadataHistoryRemoval::Conflict { .. }
        )));
    }

    #[test]
    fn metadata_history_gc_compacts_terminal_intent_before_collecting_its_target() {
        let object_id = object_id("obj_terminal_intent_retry");
        let target_version = version("version_terminal_intent_target");
        let newer_version = version("version_terminal_intent_newer");
        let mut tables = DfsMetadataTables::default();
        insert_version(&mut tables, &object_id, &target_version, 1);
        insert_version(&mut tables, &object_id, &newer_version, 2);
        let intent = DfsWriteIntentRecord {
            intent_id: "intent_terminal_retry_1".to_string(),
            file_system_id: file_system_id(),
            object_id: object_id.clone(),
            base_version: None,
            target_version: target_version.clone(),
            coordinator_node_id: node("node_coordinator"),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_node_ids: vec![node("node_owner")],
            policy: DfsReplicationPolicy::new(1, 1).expect("replication policy"),
            status: DfsWriteIntentStatus::Aborted,
            content_hash: Some("blake3:terminal-retry".to_string()),
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: timestamp(1),
            updated_at: Some(timestamp(2)),
        };
        tables
            .intents
            .insert((object_id.clone(), intent.intent_id.clone()), intent);

        let plan = plan_metadata_history_gc(
            DfsMetadataShardId::new("shard_terminal_intent"),
            &tables,
            &options(),
            None,
        );

        assert!(plan.removals.iter().any(|removal| matches!(
            removal,
            DfsMetadataHistoryRemoval::Intent {
                object_id: selected_object_id,
                intent_id,
                ..
            } if selected_object_id == &object_id && intent_id == "intent_terminal_retry_1"
        )));
        assert!(
            !version_is_selected(&plan, &object_id, &target_version),
            "the full terminal intent must survive as a compact receipt before its exact-retry target can be swept"
        );
        assert!(
            plan.protected_versions
                .contains(&DfsMetadataVersionIdentity::new(object_id, target_version))
        );
    }

    #[test]
    fn metadata_history_gc_collects_replica_receipt_before_its_version() {
        let object_id = object_id("obj_receipt_before_version");
        let old_version = version("version_receipt_before_old");
        let newer_version = version("version_receipt_before_newer");
        let replica = node("node_receipt_before_replica");
        let mut tables = DfsMetadataTables::default();
        insert_version(&mut tables, &object_id, &old_version, 1);
        insert_version(&mut tables, &object_id, &newer_version, 2);
        tables.receipts.insert(
            (object_id.clone(), old_version.clone(), replica.clone()),
            crate::DfsReplicaReceiptRecord {
                receipt: sinomo_core::DfsWriteReceipt {
                    file_system_id: file_system_id(),
                    object_id: object_id.clone(),
                    committed_version: old_version.clone(),
                    owner_node_id: node("node_owner"),
                    event_id: sinomo_core::events::EventId::from_legacy_named(
                        "event_receipt_before_version",
                    )
                    .expect("event id"),
                },
                replica_node_id: replica,
                content_hash: Some("blake3:receipt-before-version".to_string()),
                received_at: timestamp(3),
            },
        );

        let first = plan_metadata_history_gc(
            DfsMetadataShardId::new("shard_receipt_before_version"),
            &tables,
            &options(),
            None,
        );
        assert!(!version_is_selected(&first, &object_id, &old_version));
        assert!(first.removals.iter().any(|removal| matches!(
            removal,
            DfsMetadataHistoryRemoval::Receipt {
                object_id: selected_object_id,
                version: selected_version,
                ..
            } if selected_object_id == &object_id && selected_version == &old_version
        )));

        tables
            .receipts
            .retain(|(record_object_id, record_version, _), _| {
                record_object_id != &object_id || record_version != &old_version
            });
        let second = plan_metadata_history_gc(
            DfsMetadataShardId::new("shard_receipt_before_version"),
            &tables,
            &options(),
            None,
        );
        assert!(version_is_selected(&second, &object_id, &old_version));
    }

    #[test]
    fn metadata_content_protection_includes_recovery_only_hash_roots() {
        let object_id = object_id("obj_content_recovery_roots");
        let local_version = version("version_content_local");
        let remote_version = version("version_content_remote");
        let transaction_version = version("version_content_transaction");
        let mut tables = DfsMetadataTables::default();
        let conflict = DfsConflictRecord {
            conflict_id: "conflict_content_roots".to_string(),
            file_system_id: file_system_id(),
            object_id: object_id.clone(),
            path: "/content-roots".to_string(),
            base_version: None,
            local_version,
            remote_version,
            detected_by_node_id: node("node_detector"),
            reason: "content roots".to_string(),
            local_content_hash: Some("blake3:conflict-local-only".to_string()),
            remote_content_hash: Some("blake3:conflict-remote-only".to_string()),
            created_by_intent_id: None,
            strategy: None,
            status: DfsConflictStatus::Open,
            detected_at: timestamp(1),
        };
        tables
            .conflicts
            .insert((object_id.clone(), conflict.conflict_id.clone()), conflict);
        let mut transaction_object = current_object(&object_id, &version("version_content_object"));
        transaction_object.content_hash = Some("blake3:transaction-object-only".to_string());
        let mut transaction_version_record = version_record(&object_id, &transaction_version, 3);
        transaction_version_record.content_hash =
            Some("blake3:transaction-version-only".to_string());
        let transaction = crate::DfsNamespaceTransaction {
            transaction_id: "transaction_content_roots".to_string(),
            file_system_id: file_system_id(),
            objects: vec![transaction_object],
            versions: vec![transaction_version_record],
            advisory_lock_states: Vec::new(),
            created_at: timestamp(3),
        };
        tables
            .namespace_transactions
            .insert(transaction.transaction_id.clone(), transaction);

        let protection = metadata_content_protection_from_tables(&tables);
        for content_hash in [
            "blake3:conflict-local-only",
            "blake3:conflict-remote-only",
            "blake3:transaction-object-only",
            "blake3:transaction-version-only",
        ] {
            assert!(
                protection
                    .content_hashes
                    .contains(&content_hash.to_string()),
                "missing recovery-only content root {content_hash}"
            );
        }
    }

    #[test]
    fn metadata_history_gc_compacts_owned_job_only_after_exact_request_is_terminal() {
        let owner = namespace_request(
            node("node_namespace_owner"),
            "request_namespace_owner",
            DfsNamespaceRequestStatus::Pending,
        );
        let owner_key = (owner.requester_node_id.clone(), owner.request_id.clone());
        let unrelated = namespace_request(
            node("node_namespace_other"),
            "request_namespace_other",
            DfsNamespaceRequestStatus::Pending,
        );
        let job = completed_namespace_job(
            "job_namespace_owned",
            Some(DfsNamespaceJobOrigin::from_request(&owner)),
        );
        let mut tables = DfsMetadataTables::default();
        tables.namespace_requests.insert(owner_key.clone(), owner);
        tables.namespace_requests.insert(
            (
                unrelated.requester_node_id.clone(),
                unrelated.request_id.clone(),
            ),
            unrelated,
        );
        tables
            .namespace_jobs
            .insert(job.job_id.clone(), job.clone());

        let pending_plan = plan_metadata_history_gc(
            DfsMetadataShardId::new("shard_namespace"),
            &tables,
            &options(),
            None,
        );
        assert!(!namespace_job_is_selected(&pending_plan, &job.job_id));

        let owner = tables
            .namespace_requests
            .get_mut(&owner_key)
            .expect("owner request");
        owner.status = DfsNamespaceRequestStatus::Completed;
        owner.result = Some(DfsNamespaceResult::Applied);
        owner.updated_at = timestamp(4);

        let terminal_plan = plan_metadata_history_gc(
            DfsMetadataShardId::new("shard_namespace"),
            &tables,
            &options(),
            None,
        );
        assert!(
            namespace_job_is_selected(&terminal_plan, &job.job_id),
            "an unrelated path-matching request must not retain an exactly owned job"
        );
    }

    #[test]
    fn metadata_history_gc_conservatively_retains_legacy_job_for_matching_pending_request() {
        let request = namespace_request(
            node("node_namespace_legacy"),
            "request_namespace_legacy",
            DfsNamespaceRequestStatus::Pending,
        );
        let job = completed_namespace_job("job_namespace_legacy", None);
        let mut tables = DfsMetadataTables::default();
        tables.namespace_requests.insert(
            (
                request.requester_node_id.clone(),
                request.request_id.clone(),
            ),
            request,
        );
        tables
            .namespace_jobs
            .insert(job.job_id.clone(), job.clone());

        let plan = plan_metadata_history_gc(
            DfsMetadataShardId::new("shard_namespace"),
            &tables,
            &options(),
            None,
        );

        assert!(!namespace_job_is_selected(&plan, &job.job_id));
        assert_eq!(plan.report.compacted_namespace_job_count, 0);
    }

    #[test]
    fn metadata_history_gc_retains_legacy_clear_and_hardlink_job_evidence_for_pending_requests() {
        let cases = [
            (
                "clear",
                DfsNamespaceJobKind::ClearDirectoryRecursive,
                "/tree",
                None,
                DfsNamespaceOperation::ClearDirRecursive {
                    path: "/tree".to_string(),
                },
            ),
            (
                "hardlink",
                DfsNamespaceJobKind::HardlinkAcrossShardPrimaries,
                "/source",
                Some("/target"),
                DfsNamespaceOperation::Hardlink {
                    existing_path: "/source".to_string(),
                    link_path: "/target".to_string(),
                },
            ),
        ];
        for (case, kind, root_path, target_path, operation) in cases {
            let mut request = namespace_request(
                node(&format!("node_namespace_legacy_{case}")),
                &format!("request_namespace_legacy_{case}"),
                DfsNamespaceRequestStatus::Pending,
            );
            request.operation = operation;
            let mut job = completed_namespace_job(&format!("job_namespace_legacy_{case}"), None);
            job.kind = kind;
            job.root_path = root_path.to_string();
            job.target_path = target_path.map(str::to_string);
            let receipt = crate::DfsNamespaceJobReceipt::from_job(&job, timestamp(4))
                .expect("terminal legacy job receipt");
            let mut tables = DfsMetadataTables::default();
            tables.namespace_requests.insert(
                (
                    request.requester_node_id.clone(),
                    request.request_id.clone(),
                ),
                request,
            );
            tables
                .namespace_jobs
                .insert(job.job_id.clone(), job.clone());
            tables
                .namespace_job_receipts
                .insert(receipt.job_id.clone(), receipt);

            let plan = plan_metadata_history_gc(
                DfsMetadataShardId::new("shard_namespace"),
                &tables,
                &options(),
                None,
            );

            assert!(
                !namespace_job_is_selected(&plan, &job.job_id),
                "legacy {case} job must remain as pending-request evidence"
            );
            assert!(
                !plan.removals.iter().any(|removal| matches!(
                    removal,
                    DfsMetadataHistoryRemoval::NamespaceJobReceipt {
                        job_id: selected_job_id,
                        ..
                    } if selected_job_id == &job.job_id
                )),
                "legacy {case} job receipt must remain as pending-request evidence"
            );
        }
    }
}
