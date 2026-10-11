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
use crate::metadata_records::{
    DfsMetadataShardId, DfsNamespaceRequestStatus, DfsObjectVersionRecord,
    DfsPreparedOperationRecord, DfsPreparedOperationStatus, DfsWriteIntentRecord,
    DfsWriteIntentStatus,
};
use crate::metadata_tables::DfsMetadataTables;
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
    /// Restrict an operator retention pass to these full terminal job bodies.
    /// Age and pending-request protections still apply; receipts are retained.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace_job_ids: Option<Vec<String>>,
}

impl DfsMetadataHistoryGcOptions {
    pub fn dry_run(min_history_age: Duration, keep_latest_versions_per_object: u64) -> Self {
        Self {
            dry_run: true,
            min_history_age_nanos: duration_nanos_saturating(min_history_age),
            keep_latest_versions_per_object,
            max_removals: None,
            now_unix_nanos: None,
            protected_versions: Vec::new(),
            namespace_job_ids: None,
        }
    }

    pub fn delete_eligible(
        min_history_age: Duration,
        keep_latest_versions_per_object: u64,
    ) -> Self {
        Self {
            dry_run: false,
            min_history_age_nanos: duration_nanos_saturating(min_history_age),
            keep_latest_versions_per_object,
            max_removals: None,
            now_unix_nanos: None,
            protected_versions: Vec::new(),
            namespace_job_ids: None,
        }
    }

    pub fn with_max_removals(mut self, max_removals: u64) -> Self {
        self.max_removals = Some(max_removals);
        self
    }

    pub fn with_namespace_job_ids(mut self, jobs: impl IntoIterator<Item = String>) -> Self {
        let mut jobs: Vec<_> = jobs.into_iter().collect();
        jobs.sort();
        jobs.dedup();
        self.namespace_job_ids = Some(jobs);
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
    #[serde(default)]
    pub materialized_retained_repair_owner_count: u64,
    #[serde(default)]
    pub removed_retained_repair_owner_count: u64,
    pub eligible_intent_receipt_count: u64,
    pub removed_intent_receipt_count: u64,
    pub eligible_namespace_job_count: u64,
    pub compacted_namespace_job_count: u64,
    pub eligible_prepared_operation_count: u64,
    pub removed_prepared_operation_count: u64,
    pub eligible_namespace_request_count: u64,
    pub compacted_namespace_request_count: u64,
    pub eligible_namespace_job_receipt_count: u64,
    pub removed_namespace_job_receipt_count: u64,
    pub eligible_namespace_transaction_receipt_count: u64,
    pub removed_namespace_transaction_receipt_count: u64,
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
            .saturating_add(self.eligible_namespace_job_count)
            .saturating_add(self.eligible_prepared_operation_count)
            .saturating_add(self.eligible_namespace_request_count)
            .saturating_add(self.eligible_namespace_job_receipt_count)
            .saturating_add(self.eligible_namespace_transaction_receipt_count)
            .saturating_add(self.eligible_namespace_request_receipt_count)
    }

    pub fn applied_record_count(&self) -> u64 {
        self.removed_version_count
            .saturating_add(self.removed_receipt_count)
            .saturating_add(self.compacted_intent_count)
            .saturating_add(self.materialized_retained_repair_owner_count)
            .saturating_add(self.removed_retained_repair_owner_count)
            .saturating_add(self.removed_intent_receipt_count)
            .saturating_add(self.compacted_namespace_job_count)
            .saturating_add(self.removed_prepared_operation_count)
            .saturating_add(self.compacted_namespace_request_count)
            .saturating_add(self.removed_namespace_job_receipt_count)
            .saturating_add(self.removed_namespace_transaction_receipt_count)
            .saturating_add(self.removed_namespace_request_receipt_count)
    }

    pub(crate) fn for_options(options: &DfsMetadataHistoryGcOptions) -> Self {
        Self {
            dry_run: options.dry_run,
            min_history_age_nanos: options.min_history_age_nanos,
            keep_latest_versions_per_object: options.keep_latest_versions_per_object,
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
        self.materialized_retained_repair_owner_count = self
            .materialized_retained_repair_owner_count
            .saturating_add(other.materialized_retained_repair_owner_count);
        self.removed_retained_repair_owner_count = self
            .removed_retained_repair_owner_count
            .saturating_add(other.removed_retained_repair_owner_count);
        self.eligible_intent_receipt_count = self
            .eligible_intent_receipt_count
            .saturating_add(other.eligible_intent_receipt_count);
        self.removed_intent_receipt_count = self
            .removed_intent_receipt_count
            .saturating_add(other.removed_intent_receipt_count);
        self.eligible_namespace_job_count = self
            .eligible_namespace_job_count
            .saturating_add(other.eligible_namespace_job_count);
        self.compacted_namespace_job_count = self
            .compacted_namespace_job_count
            .saturating_add(other.compacted_namespace_job_count);
        self.eligible_prepared_operation_count = self
            .eligible_prepared_operation_count
            .saturating_add(other.eligible_prepared_operation_count);
        self.removed_prepared_operation_count = self
            .removed_prepared_operation_count
            .saturating_add(other.removed_prepared_operation_count);
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
    NamespaceJob {
        file_system_id: DfsFileSystemId,
        job_id: String,
    },
    PreparedOperation {
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
    NamespaceRequestReceipt {
        file_system_id: DfsFileSystemId,
        requester_node_id: sinomo_core::node::NodeId,
        request_id: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DfsMetadataHistoryGcPlan {
    pub(crate) report: DfsMetadataHistoryGcReport,
    pub(crate) retained_repair_owners: Vec<DfsWriteIntentRecord>,
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
        "advisory-lock-states",
        tables.advisory_lock_states.values(),
        |_| None,
    );
    push_stats(
        &mut stats,
        "namespace-transaction-receipts",
        tables.namespace_transaction_receipts.values(),
        |record| Some(offset_datetime_nanos(record.retained_at)),
    );
    push_stats(
        &mut stats,
        "prepared-operations-v7",
        tables.prepared_operations_v7.values(),
        |record| Some(offset_datetime_nanos(record.transaction.created_at)),
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
) -> Result<DfsMetadataHistoryGcPlan, crate::MetadataBackendError> {
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
    let mut prepared_content_hashes = BTreeSet::new();
    protected_versions.extend(
        crate::file_history::policy_retained_versions(tables, now)
            .into_iter()
            .map(|(object_id, version)| DfsMetadataVersionIdentity::new(object_id, version)),
    );
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

    // Keep the ordinary durable repair owner until N is reached for retained
    // content too. Only explicit roots count; the intent's own grace period
    // must not renew a released pin or expired rule.
    let retained_repair_roots = protected_versions
        .iter()
        .filter(|identity| {
            tables
                .versions
                .get(&(identity.object_id.clone(), identity.version.clone()))
                .is_some_and(|version| version.has_retention_grant())
        })
        .cloned()
        .collect::<BTreeSet<_>>();

    let retained_history_objects = tables
        .versions
        .values()
        .filter(|version| {
            version.has_retention_grant()
                && tables
                    .objects
                    .get(&version.object_id)
                    .is_none_or(|object| object.version != version.version)
        })
        .map(|version| version.object_id.clone())
        .collect::<BTreeSet<_>>();
    for object in tables.objects.values() {
        // A deleted object's counter is needed while any retained generation
        // remains, including an old pin whose newer captures were collected.
        // Once that family is gone the data-free counter must not prevent
        // ordinary tombstone compaction forever.
        if object.kind == crate::DfsObjectKind::Tombstone {
            let has_history = retained_history_objects.contains(&object.object_id);
            if !has_history || object.tombstone_provenance.is_some() {
                continue;
            }
        }
        protected_versions.insert(DfsMetadataVersionIdentity::new(
            object.object_id.clone(),
            object.version.clone(),
        ));
    }

    let restore_intent_roots = restore_job_intent_roots(tables);
    for intent in tables.intents.values() {
        let is_recovery_root = intent_is_recovery_root(
            tables,
            intent,
            &retained_repair_roots,
            &restore_intent_roots,
        );
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
        if is_recovery_root
            || (!write_intent_status_is_active(&intent.status)
                && !intent.publication.is_retained_repair())
        {
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

    for prepare in tables.prepared_operations_v7.values().filter(|prepare| {
        prepare.status == DfsPreparedOperationStatus::Prepared
            || prepared_operation_has_live_dependency(tables, prepare)
    }) {
        for expected in &prepare.expected_object_versions {
            if let Some(version) = expected.expected_version.as_ref() {
                protected_versions.insert(DfsMetadataVersionIdentity::new(
                    expected.object_id.clone(),
                    version.clone(),
                ));
            }
        }
        protect_prepared_operation(
            &mut protected_versions,
            &mut prepared_content_hashes,
            prepare,
        );
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
        usize::try_from(options.keep_latest_versions_per_object).unwrap_or(usize::MAX);
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

    // A live metadata-only successor carries its original data-generation
    // binding. Keep that exact same-object version and its W receipts until
    // every current/retained/recovery root using it is gone. The worklist adds
    // only recorded dependencies of already protected versions; unrelated
    // historical byte generations receive no new root.
    let mut data_generation_roots = protected_versions.iter().cloned().collect::<Vec<_>>();
    while let Some(identity) = data_generation_roots.pop() {
        let Some(version) = tables
            .versions
            .get(&(identity.object_id.clone(), identity.version.clone()))
        else {
            continue;
        };
        let Some(plan) = version.content_durability.as_ref() else {
            continue;
        };
        let source =
            DfsMetadataVersionIdentity::new(version.object_id.clone(), plan.source_version.clone());
        let recorded_source = tables
            .versions
            .get(&(source.object_id.clone(), source.version.clone()))
            .ok_or_else(|| {
                crate::MetadataBackendError::Serde(format!(
                    "protected revision {}/{} has no recorded data-generation witness {}",
                    version.object_id, version.version, plan.source_version
                ))
            })?;
        if recorded_source.file_system_id != version.file_system_id
            || recorded_source.object_id != version.object_id
            || recorded_source.version != plan.source_version
            || recorded_source.content_hash != version.content_hash
            || recorded_source.size_bytes != version.size_bytes
        {
            return Err(crate::MetadataBackendError::Serde(
                "protected revision's recorded data-generation witness changes content identity"
                    .into(),
            ));
        }
        if protected_versions.insert(source.clone()) {
            data_generation_roots.push(source);
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

    let mut owners_before_removal = BTreeMap::new();
    for receipt in tables.intent_receipts.values() {
        let retained_at = offset_datetime_nanos(receipt.retained_at);
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        if let Some(owner) =
            retained_repair_owner_before_compaction(tables, &receipt.to_intent_record())?
        {
            owners_before_removal.insert(
                (receipt.object_id.clone(), receipt.intent_id.clone()),
                owner,
            );
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
        // A repair owner is also the version's durable W/N plan. Keep that
        // plan while the retained metadata exists, without making it a root
        // for an otherwise collectible version.
        if intent.publication.is_retained_repair()
            && tables
                .versions
                .get(&(intent.object_id.clone(), intent.target_version.clone()))
                .is_some_and(|version| version.has_retention_grant())
        {
            continue;
        }
        if intent_is_recovery_root(
            tables,
            intent,
            &retained_repair_roots,
            &restore_intent_roots,
        ) {
            continue;
        }
        let retained_at = offset_datetime_nanos(intent.updated_at.unwrap_or(intent.created_at));
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        if let Some(owner) = retained_repair_owner_before_compaction(tables, intent)? {
            owners_before_removal
                .insert((intent.object_id.clone(), intent.intent_id.clone()), owner);
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

    for prepared in tables.prepared_operations_v7.values().filter(|prepared| {
        prepared.status != DfsPreparedOperationStatus::Prepared
            && !prepared_operation_has_live_dependency(tables, prepared)
    }) {
        let retained_at = prepared
            .committed_entry
            .as_ref()
            .map(|entry| offset_datetime_nanos(entry.created_at))
            .unwrap_or_else(|| offset_datetime_nanos(prepared.transaction.created_at));
        if retain_young(retained_at, now, options.min_history_age_nanos, &mut report) {
            continue;
        }
        report.eligible_prepared_operation_count =
            report.eligible_prepared_operation_count.saturating_add(1);
        candidates.push(RemovalCandidate {
            retained_at_unix_nanos: retained_at,
            removal: DfsMetadataHistoryRemoval::PreparedOperation {
                file_system_id: prepared.file_system_id.clone(),
                operation_id: prepared.operation_id.clone(),
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

    if let Some(jobs) = options.namespace_job_ids.as_ref() {
        candidates.retain(|candidate| {
            matches!(&candidate.removal,
            DfsMetadataHistoryRemoval::NamespaceJob { job_id, .. }
                if jobs.iter().any(|requested| requested == job_id))
        });
        let shards = std::mem::take(&mut report.shards);
        report = DfsMetadataHistoryGcReport::for_options(options);
        report.shards = shards;
        report.eligible_namespace_job_count = candidates.len() as u64;
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
    let mut retained_repair_owners = BTreeMap::new();
    let mut removals = Vec::new();
    if !options.dry_run {
        for candidate in candidates {
            // Creating a prerequisite consumes the same bounded mutation
            // budget. Even a budget of one makes progress: publish the owner
            // now, compact its original witness on the next pass.
            let source = match &candidate.removal {
                DfsMetadataHistoryRemoval::Intent {
                    object_id,
                    intent_id,
                    ..
                }
                | DfsMetadataHistoryRemoval::IntentReceipt {
                    object_id,
                    intent_id,
                    ..
                } => Some((object_id.clone(), intent_id.clone())),
                _ => None,
            };
            if let Some(owner) = source.and_then(|key| owners_before_removal.remove(&key)) {
                let key = (owner.object_id.clone(), owner.intent_id.clone());
                if let Some(previous) = retained_repair_owners.get(&key) {
                    if !crate::file_history::retained_repair_plan_matches(previous, &owner) {
                        return Err(crate::MetadataBackendError::Serde(
                            "retained version has ambiguous archived durability plans".into(),
                        ));
                    }
                } else {
                    if removals.len() + retained_repair_owners.len() >= max_removals {
                        report.max_removals_reached = true;
                        break;
                    }
                    retained_repair_owners.insert(key, owner);
                }
            }
            if removals.len() + retained_repair_owners.len() >= max_removals {
                report.max_removals_reached = true;
                break;
            }
            removals.push(candidate.removal);
        }
    }
    report.materialized_retained_repair_owner_count = retained_repair_owners.len() as u64;
    for removal in &removals {
        match removal {
            DfsMetadataHistoryRemoval::Receipt { .. } => {
                report.removed_receipt_count = report.removed_receipt_count.saturating_add(1);
            }
            DfsMetadataHistoryRemoval::Version { .. } => {
                report.removed_version_count = report.removed_version_count.saturating_add(1);
            }
            DfsMetadataHistoryRemoval::Intent {
                object_id,
                intent_id,
                ..
            } => {
                if tables
                    .intents
                    .get(&(object_id.clone(), intent_id.clone()))
                    .is_some_and(|intent| intent.publication.is_retained_repair())
                {
                    report.removed_retained_repair_owner_count += 1;
                } else {
                    report.compacted_intent_count = report.compacted_intent_count.saturating_add(1);
                }
            }
            DfsMetadataHistoryRemoval::IntentReceipt { .. } => {
                report.removed_intent_receipt_count =
                    report.removed_intent_receipt_count.saturating_add(1);
            }

            DfsMetadataHistoryRemoval::NamespaceJob { .. } => {
                report.compacted_namespace_job_count =
                    report.compacted_namespace_job_count.saturating_add(1);
            }

            DfsMetadataHistoryRemoval::PreparedOperation { .. } => {
                report.removed_prepared_operation_count =
                    report.removed_prepared_operation_count.saturating_add(1);
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
        .chain(prepared_content_hashes)
        .collect();
    report.protected_version_count = protected_versions.len() as u64;
    Ok(DfsMetadataHistoryGcPlan {
        report,
        retained_repair_owners: retained_repair_owners.into_values().collect(),
        removals,
        protected_versions,
        protected_content_hashes,
    })
}

pub(crate) fn retained_repair_owner_before_compaction(
    tables: &DfsMetadataTables,
    source: &DfsWriteIntentRecord,
) -> Result<Option<DfsWriteIntentRecord>, crate::MetadataBackendError> {
    if source.status != DfsWriteIntentStatus::Committed
        || source.publication.is_retained_repair()
        || source.intent_id.starts_with("placement:")
    {
        return Ok(None);
    }
    let Some(version) = tables
        .versions
        .get(&(source.object_id.clone(), source.target_version.clone()))
        .filter(|version| version.has_retention_grant())
    else {
        return Ok(None);
    };
    let owner = crate::file_history::retained_repair_intent(version, Some(source))?;
    let key = (owner.object_id.clone(), owner.intent_id.clone());
    if let Some(existing) = tables.intents.get(&key) {
        if !crate::file_history::retained_repair_plan_matches(existing, &owner) {
            return Err(crate::MetadataBackendError::Serde(
                "retained repair owner differs from its original durability plan".into(),
            ));
        }
        return Ok(None);
    }
    Ok(Some(owner))
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
    // Every retained version is already included above. This additional pass
    // protects staging bases and unfinished current-publication recovery.
    let no_additional_retained_roots = BTreeSet::new();
    let restore_intent_roots = restore_job_intent_roots(tables);
    for intent in tables.intents.values().filter(|intent| {
        write_intent_status_is_active(&intent.status)
            || intent_is_recovery_root(
                tables,
                intent,
                &no_additional_retained_roots,
                &restore_intent_roots,
            )
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
    for prepare in tables.prepared_operations_v7.values().filter(|prepared| {
        prepared.status == DfsPreparedOperationStatus::Prepared
            || prepared_operation_has_live_dependency(tables, prepared)
    }) {
        for expected in &prepare.expected_object_versions {
            if let Some(version) = expected.expected_version.as_ref() {
                versions.insert(DfsMetadataVersionIdentity::new(
                    expected.object_id.clone(),
                    version.clone(),
                ));
            }
        }
        protect_prepared_operation(&mut versions, &mut content_hashes, prepare);
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
        protect_namespace_job_content_hashes(&mut content_hashes, job);
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
    job.origin.as_ref().is_some_and(|origin| {
        tables
            .namespace_requests
            .get(&(origin.requester_node_id.clone(), origin.request_id.clone()))
            .is_some_and(|request| {
                request.status == DfsNamespaceRequestStatus::Pending
                    && origin.matches_request(request)
            })
    })
}

fn namespace_job_receipt_has_pending_request_witness(
    tables: &DfsMetadataTables,
    receipt: &crate::DfsNamespaceJobReceipt,
) -> bool {
    receipt.origin.as_ref().is_some_and(|origin| {
        tables
            .namespace_requests
            .get(&(origin.requester_node_id.clone(), origin.request_id.clone()))
            .is_some_and(|request| {
                request.status == DfsNamespaceRequestStatus::Pending
                    && origin.matches_request(request)
            })
    })
}

fn namespace_request_receipt_has_prepare_dependency(
    tables: &DfsMetadataTables,
    receipt: &crate::DfsNamespaceRequestReceipt,
) -> bool {
    tables.prepared_operations_v7.values().any(|prepare| {
        !matches!(
            prepare.status,
            DfsPreparedOperationStatus::Aborted | DfsPreparedOperationStatus::Superseded
        ) && (prepare
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

fn restore_job_intent_roots(tables: &DfsMetadataTables) -> BTreeSet<(DfsObjectId, String)> {
    tables
        .namespace_jobs
        .values()
        .filter(|job| {
            job.kind == crate::DfsNamespaceJobKind::ContentCopy
                && job.status == crate::DfsNamespaceJobStatus::Running
        })
        .flat_map(|job| job.pending_frames.iter())
        .filter_map(|frame| match frame {
            crate::DfsNamespaceJobFrame::ContentCopy { state } if state.result.is_none() => Some((
                state.planned.object_id.clone(),
                crate::intent::stable_write_intent_id(
                    &state.requester_node_id,
                    &state.planned.object_id,
                    &state.write_operation_id,
                ),
            )),
            _ => None,
        })
        .collect()
}

fn intent_is_recovery_root(
    tables: &DfsMetadataTables,
    intent: &crate::DfsWriteIntentRecord,
    retained_repair_roots: &BTreeSet<DfsMetadataVersionIdentity>,
    restore_intent_roots: &BTreeSet<(DfsObjectId, String)>,
) -> bool {
    if write_intent_status_is_active(&intent.status) {
        return true;
    }
    if tables.prepared_operations_v7.values().any(|prepared| {
        prepared.status == DfsPreparedOperationStatus::Prepared
            && prepared.prepared_intent.as_ref().is_some_and(|owner| {
                owner.object_id == intent.object_id && owner.intent_id == intent.intent_id
            })
    }) {
        return true;
    }
    if intent.status != DfsWriteIntentStatus::Committed {
        return false;
    }
    if intent.publication.is_current()
        && intent.intent_id.starts_with("placement:")
        && let Some(object) = tables.objects.get(&intent.object_id)
        && object.kind != crate::DfsObjectKind::Tombstone
        && matches!(
            object.policy,
            sinomo_core::DfsObjectPolicy::ReplicatedEventual
                | sinomo_core::DfsObjectPolicy::ReplicatedConfirmed
        )
        && object.content_hash.is_some()
        && object.content_hash == intent.content_hash
    {
        let current = tables
            .versions
            .get(&(object.object_id.clone(), object.version.clone()));
        let source = tables
            .versions
            .get(&(intent.object_id.clone(), intent.target_version.clone()));
        if object.version == intent.target_version
            || current
                .and_then(|version| version.content_durability.as_ref())
                .is_some_and(|plan| plan.source_version == intent.target_version)
            || current.zip(source).is_some_and(|(current, source)| {
                crate::content_durability::same_content_lineage(current, source)
            })
            || source.is_none()
        {
            // This record is the current placement's repair contract, not
            // merely write retry history. A newer placement retires it through
            // its ownership transition; GC must not restore the older write's
            // replica plan by forgetting it. Missing lineage evidence remains
            // conservatively protected and grants no new repair authority.
            return true;
        }
    }
    if restore_intent_roots.contains(&(intent.object_id.clone(), intent.intent_id.clone())) {
        return true;
    }
    if !retained_repair_roots.contains(&DfsMetadataVersionIdentity::new(
        intent.object_id.clone(),
        intent.target_version.clone(),
    )) && tables.objects.get(&intent.object_id).is_none_or(|object| {
        object.version != intent.target_version
            && !tables
                .versions
                .get(&(object.object_id.clone(), object.version.clone()))
                .and_then(|version| version.content_durability.as_ref())
                .is_some_and(|plan| {
                    plan.source_version == intent.target_version
                        && object.content_hash == intent.content_hash
                })
    }) {
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

/// Revalidates GC against the actual writer-held current table cut. The GC
/// operation's own removal footprint is excluded; all other live roots remain.
pub(crate) fn version_is_protected_from_history_gc(
    tables: &DfsMetadataTables,
    object_id: &DfsObjectId,
    version: &DfsVersion,
    excluding_operation_id: &str,
) -> bool {
    let identity = DfsMetadataVersionIdentity::new(object_id.clone(), version.clone());
    let now = metadata_history_now_unix_nanos();
    let mut protected = crate::file_history::policy_retained_versions(tables, now)
        .into_iter()
        .map(|(object_id, version)| DfsMetadataVersionIdentity::new(object_id, version))
        .collect::<BTreeSet<_>>();
    protected.extend(
        tables
            .version_pins
            .values()
            .map(|pin| DfsMetadataVersionIdentity::new(pin.object_id.clone(), pin.version.clone())),
    );
    for transfer in tables.cross_shard_pin_transfers.values() {
        protected.insert(DfsMetadataVersionIdentity::new(
            transfer.object_id.clone(),
            transfer.target_version.clone(),
        ));
        protected.extend(
            transfer
                .source_pins
                .iter()
                .chain(&transfer.copied_pins)
                .map(|pin| {
                    DfsMetadataVersionIdentity::new(pin.object_id.clone(), pin.version.clone())
                }),
        );
    }
    let retained_repair_roots = protected
        .iter()
        .filter(|identity| {
            tables
                .versions
                .get(&(identity.object_id.clone(), identity.version.clone()))
                .is_some_and(|version| version.has_retention_grant())
        })
        .cloned()
        .collect();
    for object in tables.objects.values() {
        if object.kind == crate::DfsObjectKind::Tombstone {
            let retained_family = tables.versions.values().any(|version| {
                version.object_id == object.object_id
                    && version.version != object.version
                    && version.has_retention_grant()
            });
            if !retained_family || object.tombstone_provenance.is_some() {
                continue;
            }
        }
        protected.insert(DfsMetadataVersionIdentity::new(
            object.object_id.clone(),
            object.version.clone(),
        ));
    }
    let restore_roots = restore_job_intent_roots(tables);
    for intent in tables.intents.values() {
        let recovery =
            intent_is_recovery_root(tables, intent, &retained_repair_roots, &restore_roots);
        if recovery && let Some(base) = &intent.base_version {
            protected.insert(DfsMetadataVersionIdentity::new(
                intent.object_id.clone(),
                base.clone(),
            ));
        }
        if recovery
            || (!write_intent_status_is_active(&intent.status)
                && !intent.publication.is_retained_repair())
        {
            protected.insert(DfsMetadataVersionIdentity::new(
                intent.object_id.clone(),
                intent.target_version.clone(),
            ));
        }
    }
    for receipt in tables.intent_receipts.values() {
        protected.insert(DfsMetadataVersionIdentity::new(
            receipt.object_id.clone(),
            receipt.target_version.clone(),
        ));
    }
    for prepared in tables.prepared_operations_v7.values().filter(|prepared| {
        prepared.operation_id != excluding_operation_id
            && (prepared.status == DfsPreparedOperationStatus::Prepared
                || prepared_operation_has_live_dependency(tables, prepared))
    }) {
        for expected in &prepared.expected_object_versions {
            if let Some(version) = &expected.expected_version {
                protected.insert(DfsMetadataVersionIdentity::new(
                    expected.object_id.clone(),
                    version.clone(),
                ));
            }
        }
        protect_prepared_operation(&mut protected, &mut BTreeSet::new(), prepared);
        for receipt in &prepared.content_receipts {
            protected.insert(DfsMetadataVersionIdentity::new(
                receipt.receipt.object_id.clone(),
                receipt.receipt.committed_version.clone(),
            ));
        }
    }
    for request in tables.namespace_requests.values().filter(|request| {
        request.status == DfsNamespaceRequestStatus::Pending
            && request.operation_id != excluding_operation_id
    }) {
        protect_namespace_operation_versions(&mut protected, &request.operation);
    }
    for job in tables.namespace_jobs.values() {
        protect_namespace_job_versions(&mut protected, job);
    }
    protected.contains(&identity)
}

fn protect_namespace_job_content_hashes(
    hashes: &mut BTreeSet<String>,
    job: &crate::DfsNamespaceJobRecord,
) {
    for frame in &job.pending_frames {
        let hash = match frame {
            crate::DfsNamespaceJobFrame::CrossShardAdopt { moved_object, .. }
            | crate::DfsNamespaceJobFrame::CrossShardComplete { moved_object, .. } => {
                moved_object.content_hash.as_ref()
            }
            crate::DfsNamespaceJobFrame::CrossShardHardlinkAdopt { alias, .. } => {
                alias.content_hash.as_ref()
            }
            _ => None,
        };
        hashes.extend(hash.cloned());
    }
}

fn protect_prepared_operation(
    versions: &mut BTreeSet<DfsMetadataVersionIdentity>,
    hashes: &mut BTreeSet<String>,
    prepared: &DfsPreparedOperationRecord,
) {
    protect_namespace_transaction_versions(versions, &prepared.transaction);
    hashes.extend(
        prepared
            .transaction
            .objects
            .iter()
            .filter_map(|object| object.content_hash.clone())
            .chain(
                prepared
                    .transaction
                    .versions
                    .iter()
                    .filter_map(|version| version.content_hash.clone()),
            ),
    );
    for intent in prepared
        .prepared_intent
        .iter()
        .chain(prepared.terminal_intent.iter())
    {
        if let Some(base) = intent.base_version.as_ref() {
            versions.insert(DfsMetadataVersionIdentity::new(
                intent.object_id.clone(),
                base.clone(),
            ));
        }
        versions.insert(DfsMetadataVersionIdentity::new(
            intent.object_id.clone(),
            intent.target_version.clone(),
        ));
        hashes.extend(intent.content_hash.iter().cloned());
    }
    for effect in &prepared.metadata_effects {
        use crate::DfsPreparedMetadataEffect as Effect;
        match effect {
            Effect::PutObject { object } => {
                versions.insert(DfsMetadataVersionIdentity::new(
                    object.object_id.clone(),
                    object.version.clone(),
                ));
                hashes.extend(object.content_hash.iter().cloned());
            }
            Effect::PutVersion { version } => {
                versions.insert(DfsMetadataVersionIdentity::new(
                    version.object_id.clone(),
                    version.version.clone(),
                ));
                hashes.extend(version.content_hash.iter().cloned());
            }
            Effect::RemoveVersion { object_id, version }
            | Effect::RemoveVersionPin {
                object_id, version, ..
            }
            | Effect::RemoveReceipt {
                object_id, version, ..
            } => {
                versions.insert(DfsMetadataVersionIdentity::new(
                    object_id.clone(),
                    version.clone(),
                ));
            }
            Effect::PutVersionPin { pin } => {
                versions.insert(DfsMetadataVersionIdentity::new(
                    pin.object_id.clone(),
                    pin.version.clone(),
                ));
            }
            Effect::PutReceipt { receipt } => {
                versions.insert(DfsMetadataVersionIdentity::new(
                    receipt.receipt.object_id.clone(),
                    receipt.receipt.committed_version.clone(),
                ));
                hashes.extend(receipt.content_hash.iter().cloned());
            }
            Effect::PutIntent { intent } => {
                versions.insert(DfsMetadataVersionIdentity::new(
                    intent.object_id.clone(),
                    intent.target_version.clone(),
                ));
                if let Some(base) = &intent.base_version {
                    versions.insert(DfsMetadataVersionIdentity::new(
                        intent.object_id.clone(),
                        base.clone(),
                    ));
                }
                hashes.extend(intent.content_hash.iter().cloned());
            }
            Effect::PutIntentReceipt { receipt } => {
                versions.insert(DfsMetadataVersionIdentity::new(
                    receipt.object_id.clone(),
                    receipt.target_version.clone(),
                ));
                hashes.extend(receipt.content_hash.iter().cloned());
            }
            Effect::ReconcileCrossShardPinTransfer {
                versions: captured,
                transfer,
            } => {
                for version in captured {
                    versions.insert(DfsMetadataVersionIdentity::new(
                        version.object_id.clone(),
                        version.version.clone(),
                    ));
                    hashes.extend(version.content_hash.iter().cloned());
                }
                versions.insert(DfsMetadataVersionIdentity::new(
                    transfer.object_id.clone(),
                    transfer.target_version.clone(),
                ));
                for pin in transfer.source_pins.iter().chain(&transfer.copied_pins) {
                    versions.insert(DfsMetadataVersionIdentity::new(
                        pin.object_id.clone(),
                        pin.version.clone(),
                    ));
                }
            }
            Effect::FinalizeCrossShardSourceEviction {
                tombstone,
                removed_pins,
                removed_pin_transfers,
            } => {
                versions.insert(DfsMetadataVersionIdentity::new(
                    tombstone.object_id.clone(),
                    tombstone.version.clone(),
                ));
                for pin in removed_pins {
                    versions.insert(DfsMetadataVersionIdentity::new(
                        pin.object_id.clone(),
                        pin.version.clone(),
                    ));
                }
                for transfer in removed_pin_transfers {
                    versions.insert(DfsMetadataVersionIdentity::new(
                        transfer.object_id.clone(),
                        transfer.target_version.clone(),
                    ));
                    for pin in transfer.source_pins.iter().chain(&transfer.copied_pins) {
                        versions.insert(DfsMetadataVersionIdentity::new(
                            pin.object_id.clone(),
                            pin.version.clone(),
                        ));
                    }
                }
            }
            Effect::PutNamespaceJob { job } => {
                protect_namespace_job_versions(versions, job);
                protect_namespace_job_content_hashes(hashes, job);
            }
            Effect::PutNamespaceRequest { request } => {
                protect_namespace_operation_versions(versions, &request.operation);
                protect_namespace_operation_content_hashes(hashes, &request.operation);
            }
            _ => {}
        }
    }
}

fn prepared_operation_has_live_dependency(
    tables: &DfsMetadataTables,
    prepared: &DfsPreparedOperationRecord,
) -> bool {
    let matches_request = |request: &crate::DfsNamespaceRequestRecord| {
        request.file_system_id == prepared.file_system_id
            && request.status == DfsNamespaceRequestStatus::Pending
            && (request.operation_id == prepared.operation_id
                || prepared
                    .completed_request
                    .as_ref()
                    .is_some_and(|completion| {
                        completion.requester_node_id == request.requester_node_id
                            && completion.request_id == request.request_id
                    })
                || prepared
                    .completed_request_completion
                    .as_ref()
                    .is_some_and(|completion| {
                        completion.requester_node_id == request.requester_node_id
                            && completion.request_id == request.request_id
                    }))
    };
    tables.namespace_requests.values().any(matches_request)
        || tables.namespace_jobs.values().any(|job| job.file_system_id == prepared.file_system_id
            && job.status == crate::DfsNamespaceJobStatus::Running
            && prepared.metadata_effects.iter().any(|effect| matches!(effect,
                crate::DfsPreparedMetadataEffect::PutNamespaceJob { job: captured } if captured.job_id == job.job_id)))
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
                    ),
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
        if let crate::DfsNamespaceJobFrame::ContentCopy { state } = frame
            && job.status == crate::DfsNamespaceJobStatus::Running
            && state.result.is_none()
        {
            protected.insert(DfsMetadataVersionIdentity::new(
                state.planned.object_id.clone(),
                state.planned.version.clone(),
            ));
            if let Some(expected) = &state.request.expected_destination {
                protected.insert(DfsMetadataVersionIdentity::new(
                    expected.object_id.clone(),
                    expected.version.clone(),
                ));
            }
        }
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
    generated_floor_count.saturating_add(usize_to_u64_saturating(
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
    fn plan_metadata_history_gc(
        shard_id: crate::DfsMetadataShardId,
        tables: &crate::metadata_tables::DfsMetadataTables,
        options: &super::DfsMetadataHistoryGcOptions,
        snapshot_size_bytes: Option<u64>,
    ) -> super::DfsMetadataHistoryGcPlan {
        super::plan_metadata_history_gc(shard_id, tables, options, snapshot_size_bytes)
            .expect("valid GC plan")
    }

    use super::*;
    use crate::{
        DfsCrossShardPinTransferRecord, DfsMetadataVersionPinRecord, DfsNamespaceJobKind,
        DfsNamespaceJobOrigin, DfsNamespaceJobRecord, DfsNamespaceJobStatus,
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
            content_durability: None,
            retention: Default::default(),
            module_metadata: Default::default(),
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

    fn prepared_record(
        operation_id: &str,
        effects: Vec<crate::DfsPreparedMetadataEffect>,
        status: DfsPreparedOperationStatus,
    ) -> DfsPreparedOperationRecord {
        let payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
            crate::DfsPreparedOperationScope {
                file_system_id: file_system_id(),
                shard_id: DfsMetadataShardId::new("shard_roots"),
                owner_epoch: 1,
                operation_id: operation_id.into(),
            },
            Vec::new(),
            Vec::new(),
            effects,
            timestamp(1),
        )
        .unwrap();
        let digest = crate::DfsPreparedOperationDigest::at_sequence(&payload, 1).unwrap();
        let committed_entry = (status == DfsPreparedOperationStatus::Committed).then(|| {
            Box::new(crate::DfsPreparedOperationCommitReference {
                shard_id: digest.shard_id.clone(),
                sequence: 2,
                created_at: timestamp(2),
                digest: digest.clone(),
            })
        });
        DfsPreparedOperationRecord {
            digest,
            payload: std::sync::Arc::new(payload),
            status,
            committed_entry,
        }
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
            namespace: "module.dfs.history".into(),
            file_system_id: file_system_id(),
            object_id: first_object.clone(),
            version: version("version_stats_pinned"),
            pin_id: crate::content_reference_storage_id(
                "module.dfs.history",
                &("checkpoint:stats".to_string()),
            ),
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
            intent_retention.record_count, 5,
            "each nested object/coordinator floor and opaque object marker is one retained entry"
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
        assert_eq!(stats.total_record_count, 12);
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
    fn metadata_history_gc_preserves_pending_body_and_collects_terminal_v7_rows() {
        let mut tables = DfsMetadataTables::default();
        for (id, status) in [
            ("pending", DfsPreparedOperationStatus::Prepared),
            ("committed", DfsPreparedOperationStatus::Committed),
            ("aborted", DfsPreparedOperationStatus::Aborted),
            ("superseded", DfsPreparedOperationStatus::Superseded),
        ] {
            let record = prepared_record(id, Vec::new(), status);
            record.validate().unwrap();
            tables.prepared_operations_v7.insert(id.into(), record);
        }
        let plan = plan_metadata_history_gc(
            DfsMetadataShardId::new("shard_roots"),
            &tables,
            &options(),
            None,
        );
        assert_eq!(plan.report.eligible_prepared_operation_count, 3);
        assert_eq!(plan.report.removed_prepared_operation_count, 3);
        assert!(plan.removals.iter().all(|removal| !matches!(removal, DfsMetadataHistoryRemoval::PreparedOperation { operation_id, .. } if operation_id == "pending")));
        let limited = plan_metadata_history_gc(
            DfsMetadataShardId::new("shard_roots"),
            &tables,
            &options().with_max_removals(1),
            None,
        );
        assert_eq!(limited.removals.len(), 1);
        assert!(limited.report.max_removals_reached);
        let dry = plan_metadata_history_gc(
            DfsMetadataShardId::new("shard_roots"),
            &tables,
            &DfsMetadataHistoryGcOptions::dry_run(Duration::ZERO, 0).with_now_unix_nanos(u64::MAX),
            None,
        );
        assert!(dry.removals.is_empty());
        assert_eq!(dry.report.eligible_prepared_operation_count, 3);
    }

    #[test]
    fn metadata_history_gc_retains_terminal_body_until_exact_pending_request_settles() {
        let mut tables = DfsMetadataTables::default();
        let mut request = namespace_request(
            node("current_request_owner"),
            "current_request",
            DfsNamespaceRequestStatus::Pending,
        );
        request.operation_id = "linked-operation".into();
        let key = (
            request.requester_node_id.clone(),
            request.request_id.clone(),
        );
        tables.namespace_requests.insert(key.clone(), request);
        let record = prepared_record(
            "linked-operation",
            Vec::new(),
            DfsPreparedOperationStatus::Committed,
        );
        tables
            .prepared_operations_v7
            .insert(record.operation_id.clone(), record);
        let before = plan_metadata_history_gc(
            DfsMetadataShardId::new("shard_roots"),
            &tables,
            &options(),
            None,
        );
        assert_eq!(before.report.eligible_prepared_operation_count, 0);
        tables.namespace_requests.get_mut(&key).unwrap().status =
            DfsNamespaceRequestStatus::Completed;
        let after = plan_metadata_history_gc(
            DfsMetadataShardId::new("shard_roots"),
            &tables,
            &options(),
            None,
        );
        assert_eq!(after.report.eligible_prepared_operation_count, 1);
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
            namespace: "module.dfs.history".into(),
            file_system_id: file_system_id(),
            object_id: object_id.clone(),
            version: copied_pin_version.clone(),
            pin_id: crate::content_reference_storage_id(
                "module.dfs.history",
                &("checkpoint:active-transfer".to_string()),
            ),
            created_at: timestamp(2),
        };
        let acknowledged_pin_version = version("acknowledged-pin");
        tables.versions.insert(
            (object_id.clone(), acknowledged_pin_version.clone()),
            version_record(&object_id, &acknowledged_pin_version, 2),
        );
        let acknowledged_pin = DfsMetadataVersionPinRecord {
            namespace: "module.dfs.history".into(),
            file_system_id: file_system_id(),
            object_id: object_id.clone(),
            version: acknowledged_pin_version.clone(),
            pin_id: crate::content_reference_storage_id(
                "module.dfs.history",
                &("checkpoint:acknowledged-transfer".to_string()),
            ),
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
    fn metadata_history_gc_retains_pending_body_and_active_intent_version_roots() {
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
        let prepared = prepared_record(
            "operation-protected-versions",
            [&conflict_base, &conflict_local, &conflict_remote]
                .into_iter()
                .map(|version| crate::DfsPreparedMetadataEffect::PutVersion {
                    version: version_record(&conflict_object_id, version, 1),
                })
                .collect(),
            DfsPreparedOperationStatus::Prepared,
        );
        tables
            .prepared_operations_v7
            .insert(prepared.operation_id.clone(), prepared);

        let intent_object_id = object_id("obj_history_intent");
        let intent_base = version("version_intent_base");
        let intent_target = version("version_intent_target");
        let intent_newest = version("version_intent_newest");
        for (version, created_at) in [(&intent_base, 1), (&intent_target, 2), (&intent_newest, 3)] {
            insert_version(&mut tables, &intent_object_id, version, created_at);
        }
        let intent = DfsWriteIntentRecord {
            publication: Default::default(),
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
            DfsMetadataHistoryRemoval::Intent { .. }
                | DfsMetadataHistoryRemoval::PreparedOperation { .. }
        )));
    }

    #[test]
    fn file_history_gc_keeps_repair_ownership_only_while_an_explicit_root_is_live() {
        let object_id = object_id("retained_repair_root");
        let old_version = version("retained_repair_old");
        let mut tables = DfsMetadataTables::default();
        let mut historical = current_object(&object_id, &old_version);
        historical.content_hash = Some("blake3:retained".into());
        let mut version = crate::metadata_records::object_version_record(&historical, Some(1));
        version.content_durability = Some(Box::new(crate::DfsContentDurability {
            source_version: old_version.clone(),
            coordinator_node_id: node("node_owner"),
            owner_epoch: 0,
            desired_replica_node_ids: vec![node("node_owner"), node("node_offline")],
            policy: DfsReplicationPolicy::new(1, 2).unwrap(),
        }));
        version.retention.insert(
            "vendor.retained".into(),
            crate::DfsRevisionRetentionState {
                order: 0,
                grant: Some(Box::new(crate::DfsRevisionRetentionGrant {
                    object: Box::new(historical.clone()),
                    granted_at_unix_nanos: 1,
                    origin: crate::DfsRetentionGrantOrigin::ExplicitOperation,
                    minimum: None,
                })),
            },
        );
        tables
            .versions
            .insert((object_id.clone(), old_version.clone()), version);
        tables.objects.insert(
            object_id.clone(),
            current_object(&object_id, &self::version("retained_repair_current")),
        );
        let pin = DfsMetadataVersionPinRecord {
            namespace: "module.dfs.history".into(),
            file_system_id: file_system_id(),
            object_id: object_id.clone(),
            version: old_version.clone(),
            pin_id: crate::content_reference_storage_id("module.dfs.history", "checkpoint"),
            created_at: timestamp(1),
        };
        tables.version_pins.insert(
            (object_id.clone(), old_version.clone(), pin.pin_id.clone()),
            pin,
        );
        let intent = DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: "retained-repair".into(),
            file_system_id: file_system_id(),
            object_id: object_id.clone(),
            base_version: None,
            target_version: old_version,
            coordinator_node_id: node("node_owner"),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 0,
            desired_replica_node_ids: vec![node("node_owner"), node("node_offline")],
            policy: DfsReplicationPolicy::new(1, 2).unwrap(),
            status: DfsWriteIntentStatus::Committed,
            content_hash: historical.content_hash,
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: timestamp(1),
            updated_at: Some(timestamp(2)),
        };
        tables
            .intents
            .insert((object_id, intent.intent_id.clone()), intent);
        let options = DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 0)
            .with_now_unix_nanos(u64::MAX);
        let plan =
            plan_metadata_history_gc(DfsMetadataShardId::new("root"), &tables, &options, None);
        assert_eq!(
            plan.report.eligible_intent_count, 0,
            "a historical repair owner must survive until N"
        );
        tables.version_pins.clear();
        let plan =
            plan_metadata_history_gc(DfsMetadataShardId::new("root"), &tables, &options, None);
        assert_eq!(
            plan.report.eligible_intent_count, 1,
            "unpinning must allow the incomplete repair owner to compact"
        );
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
            publication: Default::default(),
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
        let mut transaction_object = current_object(&object_id, &version("version_content_object"));
        transaction_object.content_hash = Some("blake3:transaction-object-only".to_string());
        let mut transaction_version_record = version_record(&object_id, &transaction_version, 3);
        transaction_version_record.content_hash =
            Some("blake3:transaction-version-only".to_string());
        let mut local = version_record(&object_id, &local_version, 1);
        local.content_hash = Some("blake3:conflict-local-only".into());
        let mut remote = version_record(&object_id, &remote_version, 2);
        remote.content_hash = Some("blake3:conflict-remote-only".into());
        let prepared = prepared_record(
            "operation-content-roots",
            vec![
                crate::DfsPreparedMetadataEffect::PutObject {
                    object: transaction_object,
                },
                crate::DfsPreparedMetadataEffect::PutVersion {
                    version: transaction_version_record,
                },
                crate::DfsPreparedMetadataEffect::PutVersion { version: local },
                crate::DfsPreparedMetadataEffect::PutVersion { version: remote },
            ],
            DfsPreparedOperationStatus::Prepared,
        );
        tables
            .prepared_operations_v7
            .insert(prepared.operation_id.clone(), prepared);

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
    fn namespace_job_compaction_scope_preserves_young_running_pending_and_unselected_history() {
        let owner = namespace_request(
            node("scoped-job-owner"),
            "pending-owner",
            DfsNamespaceRequestStatus::Pending,
        );
        let mut tables = DfsMetadataTables::default();
        let completed_owner = namespace_request(
            node("scoped-job-owner"),
            "completed-owner",
            DfsNamespaceRequestStatus::Completed,
        );
        let completed_origin = Some(crate::DfsNamespaceJobOrigin::from_request(&completed_owner));
        tables.namespace_requests.insert(
            (
                completed_owner.requester_node_id.clone(),
                completed_owner.request_id.clone(),
            ),
            completed_owner,
        );
        let mut failed = completed_namespace_job("selected-failed", completed_origin.clone());
        failed.status = crate::DfsNamespaceJobStatus::Failed;
        let unselected = completed_namespace_job("unselected", completed_origin.clone());
        let mut young = completed_namespace_job("young", completed_origin.clone());
        young.completed_at = Some(timestamp(99));
        let mut running = completed_namespace_job("running", completed_origin);
        running.status = crate::DfsNamespaceJobStatus::Running;

        let pending = completed_namespace_job(
            "pending",
            Some(crate::DfsNamespaceJobOrigin::from_request(&owner)),
        );
        tables.namespace_requests.insert(
            (owner.requester_node_id.clone(), owner.request_id.clone()),
            owner,
        );
        for job in [failed.clone(), unselected, young, running, pending] {
            tables.namespace_jobs.insert(job.job_id.clone(), job);
        }
        let scoped = DfsMetadataHistoryGcOptions::delete_eligible(Duration::from_secs(10), 1)
            .with_now_unix_nanos(100_000_000_000)
            .with_namespace_job_ids(
                ["selected-failed", "young", "running", "pending"].map(String::from),
            );
        let plan =
            plan_metadata_history_gc(DfsMetadataShardId::new("scope"), &tables, &scoped, None);
        assert_eq!(plan.removals.len(), 1);
        assert!(namespace_job_is_selected(&plan, &failed.job_id));
        assert_eq!(plan.report.eligible_record_count(), 1);
        let empty = scoped.with_namespace_job_ids(Vec::new());
        let plan =
            plan_metadata_history_gc(DfsMetadataShardId::new("scope"), &tables, &empty, None);
        assert!(
            plan.removals.is_empty(),
            "an empty explicit scope cannot expand into generic history GC"
        );
    }
}
