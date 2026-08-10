//! Sharded DFS metadata backend.
//!
//! This module owns routing across shard-local metadata backends: shard
//! selection, object-shard indexes, cross-shard moves, and merged scans.
//! Durable table and journal storage stays in `metadata_backend`.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock, TryLockError};

use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;

use crate::advisory_lock::{
    DfsAdvisoryLockApplyResult, DfsAdvisoryLockCommand, DfsAdvisoryLockStateRecord,
    DfsAdvisoryLockTransitionContext, prepare_advisory_lock_state_for_migration,
};
use crate::content_root_catalog::{
    DfsContentRootCatalogIncompleteReason, DfsContentRootCatalogPage,
    DfsContentRootCatalogPageRequest, DfsContentRootCatalogShardIdentity,
};
use crate::control_plane::{
    DfsMetadataShardAssignment, DfsMetadataShardConfig, DfsMetadataShardRoutingIndex,
};
use crate::intent::write_intent_status_is_active;
use crate::metadata_api::{
    DfsMetadataBackend, DfsMetadataReplicaApplyReport, DfsMetadataReplicaIngestPermit,
    DfsMetadataShardWriteFreeze, DfsNamespaceRequestAdmissionSnapshot,
    DfsNormalizedNamespacePrepare, DfsObjectLocationUpdate,
};
use crate::metadata_backend::{
    LocalSobjMetadataBackend, ResidentMetadataReplicationCursorProjectionObserver,
};
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_hardlink::sort_hardlinks_by_path;
use crate::metadata_history_gc::{
    DfsMetadataContentProtection, DfsMetadataHistoryGcOptions, DfsMetadataHistoryGcReport,
};
use crate::metadata_io::{create_dir_all, read_sobj_optional, write_sobj_atomic};
use crate::metadata_keys::{
    conflict_scan_cursor, conflict_scan_key, directory_page_cursor_with_shard, key_segment,
    local_directory_page_cursor, local_object_page_cursor, local_object_version_page_cursor,
    local_replica_receipt_page_cursor, local_version_pin_page_cursor,
    local_write_intent_page_cursor, normalize_path, normalize_posix_path,
    object_page_cursor_with_shard, object_page_key, object_version_scan_cursor,
    object_version_scan_key, replica_receipt_scan_cursor, replica_receipt_scan_key,
    write_intent_page_cursor, write_intent_page_cursor_with_shard, write_intent_scan_cursor,
    write_intent_scan_order,
};
use crate::metadata_limits::{OBJECT_LOCAL_METADATA_PAGE_SIZE, SHARD_MIGRATION_PAGE_SIZE};
use crate::metadata_lock::MetadataWriteLock;
use crate::metadata_pages::{
    collect_metadata_pages, finish_cursor_page, finish_sorted_cursor_page, walk_metadata_pages,
};
use crate::metadata_records::*;
#[cfg(test)]
use crate::metadata_shard_index::object_shard_index_path;
use crate::metadata_shard_index::{
    DfsObjectShardIndexRecord, object_shard_index_record, read_object_shard_index,
    read_object_shard_index_for_rebuild, remove_object_shard_index,
    write_content_commit_object_shard_index, write_new_object_shard_indexes,
    write_object_shard_index, write_object_shard_index_records,
    write_transaction_object_shard_indexes,
};
use crate::metadata_shard_migration::{
    DfsObjectShardMigrationBundle,
    install_object_shard_migration_bundle as install_object_shard_migration_bundle_into_backend,
    put_cross_shard_tombstone as put_cross_shard_tombstone_into_backend,
};
use crate::metadata_shard_pages::{
    collect_resumed_sharded_page_entries, collect_sharded_scan_entries,
    object_version_page_with_shard_cursor, replica_receipt_page_with_shard_cursor,
    write_intent_page_with_shard_cursor,
};
use crate::metadata_summary::{
    accumulate_metadata_summary_detail_counts, metadata_summary_for_file_system_from_tables,
    metadata_summary_from_tables,
};
use crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION;
use crate::metadata_tombstone::{metadata_now_unix_nanos, plan_tombstone_compaction};
use crate::posix_path::posix_path_is_at_or_under_prefix;
use crate::{
    DfsFileSystem, DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata,
    DfsOperatorStatusOmission, DfsVersion,
};

fn is_source_eviction_tombstone(object: &DfsObjectMetadata) -> bool {
    object.kind == DfsObjectKind::Tombstone
        && matches!(
            object.tombstone_provenance.as_ref(),
            Some(crate::DfsTombstoneProvenance::SourceEviction { .. })
        )
}

fn journal_batch_visible_object_placement_ids(
    batch: &DfsMetadataJournalBatch,
) -> BTreeSet<DfsObjectId> {
    batch
        .entries
        .iter()
        .filter(|entry| entry.mutation.changes_visible_object_placement())
        .flat_map(|entry| entry.mutation.affected_object_ids())
        .collect()
}

#[cfg(test)]
mod visible_placement_tests {
    use super::*;
    use time::OffsetDateTime;

    fn namespace_prepare_batch(
        status: DfsNamespacePrepareStatus,
    ) -> (DfsMetadataJournalBatch, DfsObjectId) {
        let file_system_id = crate::sifs_file_system_id();
        let shard_id = DfsMetadataShardId::new("root");
        let object_id = DfsObjectId::from_legacy_named("object_hidden_prepare").expect("object id");
        let prepare = DfsNamespacePrepareRecord {
            operation_id: "hidden-prepare".to_string(),
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.clone(),
            owner_epoch: 7,
            operation_digest: "blake3:hidden-prepare".to_string(),
            expected_object_versions: vec![DfsNamespaceObjectVersionExpectation {
                object_id: object_id.clone(),
                expected_version: None,
            }],
            expected_path_occupants: Vec::new(),
            content_receipts: Vec::new(),
            transaction: DfsNamespaceTransaction {
                transaction_id: "hidden-prepare-transaction".to_string(),
                file_system_id: file_system_id.clone(),
                objects: Vec::new(),
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            },
            completed_request: None,
            completed_request_completion: None,
            status: status.clone(),
        };
        (
            DfsMetadataJournalBatch {
                shard_id: shard_id.clone(),
                entries: vec![DfsMetadataJournalEntry {
                    shard_id,
                    sequence: 1,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                    mutation: DfsMetadataMutation::PutNamespacePrepare {
                        file_system_id,
                        operation_id: prepare.operation_id.clone(),
                        status,
                        prepare: Some(Box::new(prepare)),
                        packed_prepare_v1: None,
                        packed_prepare_v2: None,
                    },
                }],
                next_after_sequence: 1,
            },
            object_id,
        )
    }

    #[test]
    fn hidden_namespace_prepares_do_not_schedule_visible_placement_reconciliation() {
        for status in [
            DfsNamespacePrepareStatus::Prepared,
            DfsNamespacePrepareStatus::Aborted,
        ] {
            let (batch, _) = namespace_prepare_batch(status);
            assert!(journal_batch_visible_object_placement_ids(&batch).is_empty());
        }

        let (committed, object_id) = namespace_prepare_batch(DfsNamespacePrepareStatus::Committed);
        assert_eq!(
            journal_batch_visible_object_placement_ids(&committed),
            BTreeSet::from([object_id.clone()]),
            "legacy committed prepares publish their transaction"
        );

        let compact_commit = DfsMetadataJournalBatch {
            shard_id: DfsMetadataShardId::new("root"),
            entries: vec![DfsMetadataJournalEntry {
                shard_id: DfsMetadataShardId::new("root"),
                sequence: 2,
                created_at: OffsetDateTime::UNIX_EPOCH,
                mutation: DfsMetadataMutation::CommitNamespacePrepare {
                    file_system_id: crate::sifs_file_system_id(),
                    operation_id: "hidden-prepare".to_string(),
                    operation_digest: "blake3:hidden-prepare".to_string(),
                    owner_epoch: 7,
                    object_ids: vec![object_id.clone()],
                },
            }],
            next_after_sequence: 2,
        };
        assert_eq!(
            journal_batch_visible_object_placement_ids(&compact_commit),
            BTreeSet::from([object_id]),
            "compact commits make the prepared objects visible"
        );

        let version_only = DfsMetadataMutation::PutVersion {
            file_system_id: crate::sifs_file_system_id(),
            object_id: DfsObjectId::from_legacy_named("version-only-object").expect("object id"),
            version: DfsVersion::from_legacy_named("version-only-version").expect("version"),
            version_record: None,
        };
        assert!(
            !version_only.changes_visible_object_placement(),
            "version history has an object identity but cannot move its visible object row"
        );
    }
}

#[cfg(test)]
mod namespace_prepare_codec_handoff_tests {
    use super::*;
    use crate::control_plane::{
        DfsMetadataNamespacePrepareCodec, DfsMetadataShardMembershipHandoffProof,
    };
    use sinomo_core::DfsObjectPolicy;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use time::OffsetDateTime;

    static NEXT_TEST_ROOT: AtomicU64 = AtomicU64::new(1);

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new() -> Self {
            let unique = NEXT_TEST_ROOT.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "sinomo-dfs-v1-v2-handoff-{}-{unique}",
                std::process::id()
            )))
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn shard_config(codec: DfsMetadataNamespacePrepareCodec, epoch: u64) -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: node("codec-primary"),
            replica_node_ids: vec![node("codec-replica")],
            epoch,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: codec,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        }
    }

    fn file_system() -> DfsFileSystem {
        DfsFileSystem {
            file_system_id: crate::sifs_file_system_id(),
            display_name: "codec handoff".to_string(),
            owner_node_id: node("codec-primary"),
            subtree_policies: Vec::new(),
        }
    }

    fn object(version: &str) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named("codec-handoff-object")
                .expect("valid object id"),
            file_system_id: crate::sifs_file_system_id(),
            path: "services/codec-handoff/object".to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: node("codec-primary"),
            current_locations: vec![node("codec-primary")],
            content_hash: Some(format!("blake3:{version}")),
            content_size_bytes: Some(1),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: DfsVersion::from_legacy_named(version).expect("valid version"),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn prepare(
        operation_id: &str,
        owner_epoch: u64,
        current: &DfsObjectMetadata,
        committed: DfsObjectMetadata,
    ) -> DfsNamespacePrepareRecord {
        let mut prepare = DfsNamespacePrepareRecord {
            operation_id: operation_id.to_string(),
            file_system_id: current.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new("root"),
            owner_epoch,
            operation_digest: String::new(),
            expected_object_versions: vec![DfsNamespaceObjectVersionExpectation {
                object_id: current.object_id.clone(),
                expected_version: Some(current.version.clone()),
            }],
            expected_path_occupants: vec![DfsNamespacePathOccupantExpectation {
                path: current.path.clone(),
                expected_object_id: Some(current.object_id.clone()),
            }],
            content_receipts: Vec::new(),
            transaction: DfsNamespaceTransaction {
                transaction_id: format!("transaction:{operation_id}"),
                file_system_id: current.file_system_id.clone(),
                objects: vec![committed],
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
            },
            completed_request: None,
            completed_request_completion: None,
            status: DfsNamespacePrepareStatus::Prepared,
        };
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                .expect("namespace prepare digest");
        prepare
    }

    fn seed_lagging_replica(
        root: &std::path::Path,
        config: &DfsMetadataShardConfig,
        shard_id: &DfsMetadataShardId,
        entries: &[DfsMetadataJournalEntry],
        head: u64,
    ) {
        let mut replica =
            LocalShardedMetadataBackend::new(root, vec![config.clone()]).expect("lagging replica");
        replica
            .apply_journal_batch(&DfsMetadataJournalBatch {
                shard_id: shard_id.clone(),
                entries: entries.to_vec(),
                next_after_sequence: head,
            })
            .expect("seed pre-upgrade replica prefix");
    }

    fn assert_prepare_status(
        backend: &LocalShardedMetadataBackend,
        operation_id: &str,
        expected: DfsNamespacePrepareStatus,
    ) {
        let full_status = backend
            .get_namespace_prepare(operation_id)
            .expect("namespace prepare lookup")
            .map(|prepare| prepare.status);
        let receipt_status = backend
            .shard_backend(&DfsMetadataShardId::new("root"))
            .expect("root shard")
            .namespace_prepare_receipt_ref(operation_id)
            .map(|receipt| receipt.status.clone());
        assert_eq!(full_status.or(receipt_status), Some(expected));
    }

    #[test]
    fn packed_v1_to_v2_handoff_reopen_preserves_history_and_replica_recovery() {
        let test_root = TestRoot::new();
        let source_root = test_root.0.join("source");
        let catch_up_root = test_root.0.join("catch-up-replica");
        let snapshot_root = test_root.0.join("snapshot-replica");
        let shard_id = DfsMetadataShardId::new("root");
        let local_node_id = node("codec-primary");
        let retiring = shard_config(DfsMetadataNamespacePrepareCodec::PackedV1, 7);
        let mut active = shard_config(DfsMetadataNamespacePrepareCodec::PackedV2, 8);
        active.replica_node_ids = vec![node("codec-successor-replica")];

        let mut source = LocalShardedMetadataBackend::new(&source_root, vec![retiring.clone()])
            .expect("v1 source");
        source.put_file_system(&file_system()).expect("filesystem");
        let initial = object("codec-handoff-v1");
        source.put_object(&initial).expect("initial object");
        let pre_upgrade_entries = source.journal_since(0).expect("pre-upgrade prefix");
        let pre_upgrade_head = pre_upgrade_entries
            .last()
            .expect("pre-upgrade journal head")
            .sequence;
        seed_lagging_replica(
            &catch_up_root,
            &retiring,
            &shard_id,
            &pre_upgrade_entries,
            pre_upgrade_head,
        );
        seed_lagging_replica(
            &snapshot_root,
            &retiring,
            &shard_id,
            &pre_upgrade_entries,
            pre_upgrade_head,
        );

        let v1_committed = object("codec-handoff-v1-committed");
        let v1_prepare = prepare(
            "prepare-before-v2-activation",
            retiring.epoch,
            &initial,
            v1_committed.clone(),
        );
        let v1_entry = source
            .prepare_namespace_transaction(&v1_prepare)
            .expect("packed-v1 prepare");
        assert!(matches!(
            v1_entry.mutation,
            DfsMetadataMutation::PutNamespacePrepare {
                prepare: None,
                packed_prepare_v1: Some(_),
                packed_prepare_v2: None,
                ..
            }
        ));
        source
            .commit_prepared_namespace_transaction(
                &v1_prepare.operation_id,
                &v1_prepare.operation_digest,
                v1_prepare.owner_epoch,
            )
            .expect("commit v1 prepare");

        let handoff_id = "packed-v1-to-v2-activation";
        let freeze = source
            .freeze_metadata_shard_for_membership_handoff(&shard_id, handoff_id, retiring.epoch)
            .expect("freeze v1 generation")
            .expect("configured source shard");
        let proof = DfsMetadataShardMembershipHandoffProof {
            handoff_id: handoff_id.to_string(),
            retiring_shard: retiring,
            desired_shard: active.clone(),
            journal_sequence: freeze.journal_sequence,
            state_fingerprint: freeze.state_fingerprint,
            acknowledged_node_ids: [local_node_id.clone()].into_iter().collect(),
        };
        assert!(
            crate::service_sync::rebind_completed_metadata_shard_membership_handoff(
                &mut source,
                &local_node_id,
                &active,
                None,
                Some(&proof),
            )
            .expect("activate v2 generation")
        );
        assert_prepare_status(
            &source,
            &v1_prepare.operation_id,
            DfsNamespacePrepareStatus::Committed,
        );

        drop(source);
        let mut source = LocalShardedMetadataBackend::new(&source_root, vec![active.clone()])
            .expect("reopen active v2 source");
        assert!(
            source
                .clear_metadata_shard_membership_handoff_freeze(&shard_id, handoff_id, &active,)
                .expect("bind reopened source to the activated v2 generation")
        );
        assert_prepare_status(
            &source,
            &v1_prepare.operation_id,
            DfsNamespacePrepareStatus::Committed,
        );

        let v2_committed = object("codec-handoff-v2-prepared");
        let v2_prepare = prepare(
            "prepare-after-v2-activation",
            active.epoch,
            &v1_committed,
            v2_committed,
        );
        let v2_entry = source
            .prepare_namespace_transaction(&v2_prepare)
            .expect("packed-v2 prepare after reopen");
        assert!(matches!(
            v2_entry.mutation,
            DfsMetadataMutation::PutNamespacePrepare {
                prepare: None,
                packed_prepare_v1: None,
                packed_prepare_v2: Some(_),
                ..
            }
        ));

        let retained_entries = source.journal_since(0).expect("mixed-codec journal");
        assert!(retained_entries.iter().any(|entry| matches!(
            &entry.mutation,
            DfsMetadataMutation::PutNamespacePrepare {
                prepare: None,
                packed_prepare_v1: Some(_),
                packed_prepare_v2: None,
                ..
            }
        )));
        assert!(retained_entries.iter().any(|entry| matches!(
            &entry.mutation,
            DfsMetadataMutation::PutNamespacePrepare {
                prepare: None,
                packed_prepare_v1: None,
                packed_prepare_v2: Some(_),
                ..
            }
        )));

        let source_head = source
            .shard_backend(&shard_id)
            .expect("source shard")
            .journal_last_sequence();
        let source_prefix = DfsMetadataBackend::metadata_prefix_proof(&source, &shard_id)
            .expect("source prefix proof")
            .expect("source shard prefix proof");

        let catch_up_batch = source
            .journal_batch_for_shard_since_limited(&shard_id, pre_upgrade_head, 32, usize::MAX)
            .expect("mixed-codec catch-up batch")
            .expect("mixed-codec catch-up entries");
        assert_eq!(catch_up_batch.next_after_sequence, source_head);
        let mut catch_up = LocalShardedMetadataBackend::new(&catch_up_root, vec![active.clone()])
            .expect("reopen lagging replica on v2 config");
        catch_up
            .apply_journal_batch(&catch_up_batch)
            .expect("incrementally replay v1 and v2 entries");
        assert_eq!(
            DfsMetadataBackend::metadata_prefix_proof(&catch_up, &shard_id)
                .expect("catch-up prefix proof")
                .expect("catch-up shard prefix proof"),
            source_prefix
        );
        assert_prepare_status(
            &catch_up,
            &v1_prepare.operation_id,
            DfsNamespacePrepareStatus::Committed,
        );
        assert_prepare_status(
            &catch_up,
            &v2_prepare.operation_id,
            DfsNamespacePrepareStatus::Prepared,
        );

        let snapshot = source
            .export_shard_table_snapshot(&shard_id)
            .expect("export active snapshot")
            .expect("active shard snapshot");
        let mut snapshot_replica =
            LocalShardedMetadataBackend::new(&snapshot_root, vec![active.clone()])
                .expect("reopen snapshot replica on v2 config");
        snapshot_replica
            .replace_with_replicated_snapshot(shard_id.clone(), source_head, snapshot)
            .expect("install mixed-history authoritative snapshot");
        assert_eq!(
            DfsMetadataBackend::metadata_prefix_proof(&snapshot_replica, &shard_id)
                .expect("snapshot prefix proof")
                .expect("snapshot shard prefix proof"),
            source_prefix
        );
        assert_prepare_status(
            &snapshot_replica,
            &v1_prepare.operation_id,
            DfsNamespacePrepareStatus::Committed,
        );
        assert_prepare_status(
            &snapshot_replica,
            &v2_prepare.operation_id,
            DfsNamespacePrepareStatus::Prepared,
        );

        drop(catch_up);
        drop(snapshot_replica);
        let catch_up = LocalShardedMetadataBackend::new(&catch_up_root, vec![active.clone()])
            .expect("reopen incrementally recovered replica");
        let snapshot_replica = LocalShardedMetadataBackend::new(&snapshot_root, vec![active])
            .expect("reopen snapshot-recovered replica");
        for replica in [&catch_up, &snapshot_replica] {
            assert_prepare_status(
                replica,
                &v1_prepare.operation_id,
                DfsNamespacePrepareStatus::Committed,
            );
            assert_prepare_status(
                replica,
                &v2_prepare.operation_id,
                DfsNamespacePrepareStatus::Prepared,
            );
        }
    }
}

const ROUTING_TOPOLOGY_HISTORY_LIMIT: usize = 4;

#[cfg(test)]
thread_local! {
    static SHARDED_METADATA_OBJECT_ID_LOOKUP_COUNT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static SHARDED_CONSISTENT_READ_SNAPSHOT_SHARDS: std::cell::RefCell<Vec<DfsMetadataShardId>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
pub(crate) fn reset_sharded_metadata_object_id_lookup_count() {
    SHARDED_METADATA_OBJECT_ID_LOOKUP_COUNT.set(0);
}

#[cfg(test)]
pub(crate) fn sharded_metadata_object_id_lookup_count() -> usize {
    SHARDED_METADATA_OBJECT_ID_LOOKUP_COUNT.get()
}

#[cfg(test)]
pub(crate) fn reset_sharded_consistent_read_snapshot_shards() {
    SHARDED_CONSISTENT_READ_SNAPSHOT_SHARDS.with(|shards| shards.borrow_mut().clear());
}

#[cfg(test)]
pub(crate) fn sharded_consistent_read_snapshot_shards() -> Vec<DfsMetadataShardId> {
    SHARDED_CONSISTENT_READ_SNAPSHOT_SHARDS.with(|shards| shards.borrow().clone())
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
struct DfsMetadataRoutingTopologyHistory {
    #[serde(default)]
    active: Vec<DfsMetadataShardConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    fallbacks: Vec<Vec<DfsMetadataShardConfig>>,
}

pub(crate) fn routing_topology_history_path(root: &std::path::Path) -> PathBuf {
    root.join("shard-index").join("routing-topologies.sobj")
}

fn canonicalize_routing_topology(shards: &mut [DfsMetadataShardConfig]) {
    shards.sort_by(|left, right| {
        left.file_system_id
            .cmp(&right.file_system_id)
            .then_with(|| left.shard_id.cmp(&right.shard_id))
    });
}

/// Fields that can change which shard receives a path or object lookup.
/// Authority, replication, codec, and policy changes do not create old routes.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DfsMetadataRoutingShardIdentity {
    file_system_id: DfsFileSystemId,
    shard_id: String,
    assignment: DfsMetadataShardAssignment,
}

fn routing_topology_identity(
    topology: &[DfsMetadataShardConfig],
) -> Vec<DfsMetadataRoutingShardIdentity> {
    let mut identity = topology
        .iter()
        .map(|shard| DfsMetadataRoutingShardIdentity {
            file_system_id: shard.file_system_id.clone(),
            shard_id: shard.shard_id.clone(),
            assignment: shard.assignment.clone(),
        })
        .collect::<Vec<_>>();
    identity.sort_by(|left, right| {
        left.file_system_id
            .cmp(&right.file_system_id)
            .then_with(|| left.shard_id.cmp(&right.shard_id))
    });
    identity
}

fn compile_routing_indexes(
    shards: impl IntoIterator<Item = DfsMetadataShardConfig>,
) -> Result<BTreeMap<DfsFileSystemId, DfsMetadataShardRoutingIndex>> {
    let mut configs_by_file_system =
        BTreeMap::<DfsFileSystemId, Vec<DfsMetadataShardConfig>>::new();
    for shard in shards {
        configs_by_file_system
            .entry(shard.file_system_id.clone())
            .or_default()
            .push(shard);
    }
    configs_by_file_system
        .into_iter()
        .map(|(file_system_id, shards)| {
            DfsMetadataShardRoutingIndex::new(file_system_id.clone(), &shards)
                .map(|index| (file_system_id, index))
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))
        })
        .collect()
}

fn load_routing_topology_history(
    root: &std::path::Path,
    active: &[DfsMetadataShardConfig],
    persist: bool,
) -> Result<DfsMetadataRoutingTopologyHistory> {
    let path = routing_topology_history_path(root);
    let persisted = read_sobj_optional::<DfsMetadataRoutingTopologyHistory>(&path)?;
    let mut history = persisted.clone().unwrap_or_default();
    if history.active.is_empty() {
        history.active = active.to_vec();
        // Upgrade compatibility: old layouts have no routing history. A root
        // assignment is the only unambiguous predecessor for a newly split
        // catalog, so retain that bounded fallback rather than scanning every
        // shard for every create miss.
        let root_fallback = active
            .iter()
            .filter(|shard| matches!(&shard.assignment, DfsMetadataShardAssignment::Root))
            .cloned()
            .collect::<Vec<_>>();
        if !root_fallback.is_empty() && root_fallback.len() < active.len() {
            history.fallbacks.push(root_fallback);
        }
    } else if history.active != active {
        let previous = std::mem::replace(&mut history.active, active.to_vec());
        if routing_topology_identity(&previous) != routing_topology_identity(active) {
            history.fallbacks.insert(0, previous);
        }
    }
    let active_identity = routing_topology_identity(&history.active);
    let mut fallback_identities = Vec::new();
    history.fallbacks.retain(|topology| {
        if topology.is_empty() {
            return false;
        }
        let identity = routing_topology_identity(topology);
        if identity == active_identity || fallback_identities.contains(&identity) {
            return false;
        }
        fallback_identities.push(identity);
        true
    });
    history.fallbacks.truncate(ROUTING_TOPOLOGY_HISTORY_LIMIT);
    if persist && persisted.as_ref() != Some(&history) {
        write_sobj_atomic(&path, &history)?;
    }
    Ok(history)
}

#[cfg(test)]
mod routing_topology_history_tests {
    use super::*;
    use crate::control_plane::DfsMetadataNamespacePrepareCodec;
    use sinomo_core::DfsObjectPolicy;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn root_config() -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: node("routing-history-primary"),
            replica_node_ids: vec![node("routing-history-replica")],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::Legacy,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        }
    }

    fn split_configs() -> Vec<DfsMetadataShardConfig> {
        let root = root_config();
        vec![
            root,
            DfsMetadataShardConfig {
                file_system_id: crate::sifs_file_system_id(),
                shard_id: "services".to_string(),
                primary_node_id: node("routing-history-services-primary"),
                replica_node_ids: vec![node("routing-history-services-replica")],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: crate::SIFS_SERVICES_PATH.to_string(),
                },
                namespace_prepare_codec: DfsMetadataNamespacePrepareCodec::Legacy,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
        ]
    }

    fn change_non_routing_fields(
        topology: &[DfsMetadataShardConfig],
    ) -> Vec<DfsMetadataShardConfig> {
        topology
            .iter()
            .enumerate()
            .map(|(index, shard)| {
                let mut shard = shard.clone();
                shard.primary_node_id = node(&format!("routing-history-new-primary-{index}"));
                shard.replica_node_ids = vec![
                    node(&format!("routing-history-new-replica-{index}")),
                    node(&format!("routing-history-new-replica-{index}-b")),
                ];
                shard.epoch += 10;
                shard.namespace_prepare_codec = DfsMetadataNamespacePrepareCodec::PackedV2;
                shard.default_policy = DfsObjectPolicy::AppendOnly;
                shard
            })
            .collect()
    }

    fn metadata_root(name: &str) -> (sinomo_testdir::TestDir, PathBuf) {
        let root = sinomo_testdir::TestDir::new(name);
        let metadata_root = root.path().join("metadata");
        create_dir_all(metadata_root.join("shard-index")).expect("create shard index root");
        (root, metadata_root)
    }

    #[test]
    fn routing_topology_history_does_not_fallback_for_non_routing_changes() {
        let (_root, metadata_root) = metadata_root("dfs-routing-history-non-routing-change");
        let initial = vec![root_config()];
        let initial_history = load_routing_topology_history(&metadata_root, &initial, true)
            .expect("persist initial routing history");
        assert!(initial_history.fallbacks.is_empty());

        let changed = change_non_routing_fields(&initial);
        let history = load_routing_topology_history(&metadata_root, &changed, true)
            .expect("refresh full active shard configs");

        assert_eq!(history.active, changed);
        assert!(
            history.fallbacks.is_empty(),
            "authority, replica, epoch, codec, and policy changes must not create routing fallbacks"
        );
        assert_eq!(
            read_sobj_optional::<DfsMetadataRoutingTopologyHistory>(
                &routing_topology_history_path(&metadata_root),
            )
            .expect("read refreshed routing history"),
            Some(history),
            "the complete active configs must still be refreshed on disk"
        );
    }

    #[test]
    fn routing_topology_history_compacts_existing_semantic_duplicates() {
        let (_root, metadata_root) = metadata_root("dfs-routing-history-semantic-dedup");
        let active = split_configs();
        let active_duplicate = change_non_routing_fields(&active);
        let root_fallback = vec![root_config()];
        let root_duplicate = change_non_routing_fields(&root_fallback);
        let persisted = DfsMetadataRoutingTopologyHistory {
            active: active.clone(),
            fallbacks: vec![active_duplicate, root_fallback.clone(), root_duplicate],
        };
        let path = routing_topology_history_path(&metadata_root);
        write_sobj_atomic(&path, &persisted).expect("persist duplicate routing history");

        let history = load_routing_topology_history(&metadata_root, &active, true)
            .expect("compact routing history");

        assert_eq!(history.fallbacks, vec![root_fallback]);
        assert_eq!(
            read_sobj_optional::<DfsMetadataRoutingTopologyHistory>(&path)
                .expect("read compacted routing history"),
            Some(history),
            "semantic compaction must be persisted"
        );
    }

    #[test]
    fn routing_topology_history_retains_genuine_root_to_split_change() {
        let (_root, metadata_root) = metadata_root("dfs-routing-history-root-to-split");
        let root_topology = vec![root_config()];
        load_routing_topology_history(&metadata_root, &root_topology, true)
            .expect("persist root routing topology");

        let split = split_configs();
        let history = load_routing_topology_history(&metadata_root, &split, true)
            .expect("persist split routing topology");

        assert_eq!(history.active, split);
        assert_eq!(
            history.fallbacks,
            vec![root_topology],
            "a genuine shard or assignment change must remain routable until migration retirement"
        );
    }
}

/// Local sharded metadata router.
///
/// This backend multiplexes multiple `LocalSobjMetadataBackend` shards using
/// control-plane shard configs and a local object-to-shard index. It owns
/// cross-shard moves, shard-index repair, and merged cursor scans; shard-local
/// durable table and journal storage remain in `LocalSobjMetadataBackend`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalMetadataShardOpenMode {
    Strict,
    ReplicaRepair,
    Inspection,
}

#[derive(Debug)]
struct LocalMetadataShardSlot {
    config: DfsMetadataShardConfig,
    root: PathBuf,
    open_mode: LocalMetadataShardOpenMode,
    initialization: Mutex<()>,
    backend: OnceLock<Box<LocalSobjMetadataBackend>>,
}

/// One resident shard-local backend slot shared by scoped mutation sessions.
///
/// The slot owns exactly one [`LocalSobjMetadataBackend`] instance. Converting
/// a [`LocalShardedMetadataBackend`] moves any already-open instance here; a
/// deferred replica slot is still opened lazily, once, on its first lock.
/// Keeping the backend itself behind the shard lock is important: membership
/// handoff authority is deliberately bound to that exact in-memory instance.
#[derive(Debug)]
struct ResidentLocalMetadataShardSlot {
    config: DfsMetadataShardConfig,
    root: PathBuf,
    open_mode: LocalMetadataShardOpenMode,
    state: Mutex<ResidentLocalMetadataShardState>,
    available: Condvar,
    async_available: tokio::sync::Notify,
    /// Last complete shard-local operator projection. It stays readable while
    /// the backend itself is checked out by a long mutation, so status never
    /// queues behind data-plane ownership. Its topology is mutable because a
    /// global membership-handoff lease can rebind the resident router while
    /// retaining this exact shard backend instance.
    operator_status: RwLock<ResidentLocalMetadataShardOperatorStatus>,
}

#[derive(Debug)]
enum ResidentLocalMetadataShardState {
    Unopened,
    Opening,
    Available(Box<LocalSobjMetadataBackend>),
    CheckedOut,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResidentLocalMetadataShardStatusSnapshot {
    summary: DfsMetadataSummary,
    shard_status: DfsMetadataShardStatus,
    incomplete_reasons: Vec<String>,
}

#[derive(Debug)]
struct ResidentLocalMetadataShardOperatorStatus {
    config: DfsMetadataShardConfig,
    snapshot: Option<ResidentLocalMetadataShardStatusSnapshot>,
}

/// Post-commit bridge from an async replica ACK into bounded resident status.
///
/// It owns neither the shard backend nor its mutation lease. The durable
/// cursor remains authoritative; this observer advances only the monotonic
/// in-memory projection and the matching last-complete operator image.
#[derive(Clone, Debug)]
pub(crate) struct ResidentMetadataReplicationCursorStatusObserver {
    shard_id: DfsMetadataShardId,
    slot: Arc<ResidentLocalMetadataShardSlot>,
    projection: ResidentMetadataReplicationCursorProjectionObserver,
}

impl ResidentMetadataReplicationCursorStatusObserver {
    pub(crate) fn observe(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        sequence: u64,
    ) {
        if shard_id != &self.shard_id {
            debug_assert_eq!(shard_id, &self.shard_id);
            return;
        }

        // Lease restore takes these locks in the same order. That prevents a
        // restore which sampled the old projection from overwriting this
        // newer post-ACK status image.
        let mut operator_status = self
            .slot
            .operator_status
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if !operator_status
            .config
            .replica_node_ids
            .contains(target_node_id)
        {
            // The durable ACK remains authoritative on disk, but a retained
            // predecessor plan cannot repopulate the current generation's
            // in-memory projection or bounded status with a removed target.
            return;
        }
        self.projection.observe(target_node_id, shard_id, sequence);
        let Some(snapshot) = operator_status.snapshot.as_mut() else {
            return;
        };
        let journal_last_sequence = snapshot.shard_status.journal_last_sequence;
        let Some(replica) = snapshot
            .shard_status
            .replica_lag
            .iter_mut()
            .find(|replica| replica.target_node_id == *target_node_id)
        else {
            // A retained plan may finish while a newer resident topology no
            // longer projects this target. The old plan still owns its
            // durable cursor ACK, but it must not invent a status row outside
            // the slot's retained replica set.
            return;
        };
        replica.replicated_through_sequence = replica.replicated_through_sequence.max(sequence);
        replica.lag_entries =
            journal_last_sequence.saturating_sub(replica.replicated_through_sequence);
    }
}

/// One bounded operator image from a resident sharded metadata generation.
///
/// The image is assembled from per-shard projections published when shard
/// leases complete. It never checks out or scans a metadata table generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsResidentMetadataStatusSnapshot {
    pub summary: DfsMetadataSummary,
    pub primary_summary: DfsMetadataSummary,
    pub metadata_shards: Vec<DfsMetadataShardStatus>,
    pub incomplete_reasons: Vec<String>,
    pub bounded_omissions: Vec<DfsOperatorStatusOmission>,
}

pub(crate) fn metadata_shard_topology_status(
    config: &DfsMetadataShardConfig,
) -> DfsMetadataShardStatus {
    DfsMetadataShardStatus {
        shard_id: DfsMetadataShardId::new(config.shard_id.clone()),
        file_system_id: Some(config.file_system_id.clone()),
        primary_node_id: Some(config.primary_node_id.clone()),
        replica_node_ids: config.replica_node_ids.clone(),
        epoch: Some(config.epoch),
        journal_next_sequence: 0,
        journal_last_sequence: 0,
        replica_lag: Vec::new(),
        replay_cursors: Vec::new(),
    }
}

fn non_primary_unhydrated_status_omission(
    local_node_id: Option<&NodeId>,
    config: &DfsMetadataShardConfig,
    shard_id: &DfsMetadataShardId,
) -> Option<DfsOperatorStatusOmission> {
    local_node_id
        .filter(|local_node_id| &config.primary_node_id != *local_node_id)
        .map(|_| {
            DfsOperatorStatusOmission::non_primary_resident_metadata_not_hydrated(
                config.file_system_id.clone(),
                shard_id.clone(),
            )
        })
}

impl ResidentLocalMetadataShardSlot {
    fn from_local_slot(slot: LocalMetadataShardSlot) -> Self {
        let LocalMetadataShardSlot {
            config,
            root,
            open_mode,
            initialization: _,
            backend,
        } = slot;
        let backend = backend.into_inner();
        let status_snapshot = backend
            .as_deref()
            .map(|backend| initialize_resident_metadata_shard_status_snapshot(&config, backend));
        Self {
            config: config.clone(),
            root,
            open_mode,
            state: Mutex::new(match backend {
                Some(backend) => ResidentLocalMetadataShardState::Available(backend),
                None => ResidentLocalMetadataShardState::Unopened,
            }),
            available: Condvar::new(),
            async_available: tokio::sync::Notify::new(),
            operator_status: RwLock::new(ResidentLocalMetadataShardOperatorStatus {
                config,
                snapshot: status_snapshot,
            }),
        }
    }

    fn open_backend(&self) -> Result<Box<LocalSobjMetadataBackend>> {
        open_local_metadata_shard_backend(&self.config, &self.root, self.open_mode).map(Box::new)
    }

    fn try_checkout(self: &Arc<Self>) -> Result<Option<Box<LocalSobjMetadataBackend>>> {
        let mut state = self.state.lock().map_err(|error| {
            MetadataBackendError::Serde(format!(
                "resident metadata shard {} state lock failed: {error}",
                self.config.shard_id
            ))
        })?;
        match &*state {
            ResidentLocalMetadataShardState::Unopened => {
                *state = ResidentLocalMetadataShardState::Opening;
                drop(state);
                let opened = self.open_backend();
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                match opened {
                    Ok(backend) => {
                        let mut operator_status = self
                            .operator_status
                            .write()
                            .unwrap_or_else(|error| error.into_inner());
                        let status_config = operator_status.config.clone();
                        operator_status.snapshot =
                            Some(initialize_resident_metadata_shard_status_snapshot(
                                &status_config,
                                &backend,
                            ));
                        *state = ResidentLocalMetadataShardState::CheckedOut;
                        Ok(Some(backend))
                    }
                    Err(error) => {
                        *state = ResidentLocalMetadataShardState::Unopened;
                        self.available.notify_all();
                        self.async_available.notify_waiters();
                        Err(error)
                    }
                }
            }
            ResidentLocalMetadataShardState::Available(_) => {
                let ResidentLocalMetadataShardState::Available(backend) =
                    std::mem::replace(&mut *state, ResidentLocalMetadataShardState::CheckedOut)
                else {
                    unreachable!("resident metadata shard availability checked above")
                };
                Ok(Some(backend))
            }
            ResidentLocalMetadataShardState::Opening
            | ResidentLocalMetadataShardState::CheckedOut => Ok(None),
        }
    }

    fn checkout(self: &Arc<Self>) -> Result<Box<LocalSobjMetadataBackend>> {
        loop {
            if let Some(backend) = self.try_checkout()? {
                return Ok(backend);
            }
            let mut state = self.state.lock().map_err(|error| {
                MetadataBackendError::Serde(format!(
                    "resident metadata shard {} state lock failed: {error}",
                    self.config.shard_id
                ))
            })?;
            while matches!(
                *state,
                ResidentLocalMetadataShardState::Opening
                    | ResidentLocalMetadataShardState::CheckedOut
            ) {
                state = self.available.wait(state).map_err(|error| {
                    MetadataBackendError::Serde(format!(
                        "resident metadata shard {} availability wait failed: {error}",
                        self.config.shard_id
                    ))
                })?;
            }
        }
    }

    async fn wait_until_checkout_available(&self) -> Result<()> {
        loop {
            // Register before inspecting state so a restore between the check
            // and await cannot strand this waiter. Unlike the blocking
            // checkout, this owns no backend while it waits.
            let notified = self.async_available.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.state.lock().map_err(|error| {
                    MetadataBackendError::Serde(format!(
                        "resident metadata shard {} state lock failed: {error}",
                        self.config.shard_id
                    ))
                })?;
                if matches!(
                    *state,
                    ResidentLocalMetadataShardState::Unopened
                        | ResidentLocalMetadataShardState::Available(_)
                ) {
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    fn restore(&self, backend: Box<LocalSobjMetadataBackend>) {
        self.restore_with_status_config(backend, None);
    }

    fn restore_with_status_config(
        &self,
        backend: Box<LocalSobjMetadataBackend>,
        status_config: Option<&DfsMetadataShardConfig>,
    ) {
        let mut operator_status = self
            .operator_status
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let mut incomplete_reasons = Vec::new();
        if let Some(status_config) = status_config
            && operator_status.config != *status_config
        {
            if operator_status.config.replica_node_ids != status_config.replica_node_ids
                && let Err(error) = backend
                    .reconcile_resident_metadata_replication_cursor_status_projection(
                        &status_config.replica_node_ids,
                        &DfsMetadataShardId::new(status_config.shard_id.clone()),
                    )
            {
                incomplete_reasons.push(format!(
                    "metadata shard {} cursor projection could not be rebound: {error}",
                    status_config.shard_id
                ));
            }
            operator_status.config = status_config.clone();
        }
        let refreshed = resident_metadata_shard_status_snapshot_from_projection(
            &operator_status.config,
            &backend,
            incomplete_reasons,
        );
        operator_status.snapshot = Some(refreshed);
        drop(operator_status);
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        debug_assert!(matches!(
            *state,
            ResidentLocalMetadataShardState::CheckedOut
        ));
        *state = ResidentLocalMetadataShardState::Available(backend);
        self.available.notify_all();
        self.async_available.notify_waiters();
    }

    /// Return a bounded operator projection without waiting for ownership of
    /// the shard backend. A checked-out shard deliberately serves the last
    /// completed generation; an unopened shard has no truthful generation to
    /// serve and is reported as unavailable instead of being hydrated here.
    fn bounded_status_snapshot(&self) -> Result<ResidentLocalMetadataShardStatusSnapshot> {
        let state = match self.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return self.last_status_snapshot(),
            Err(TryLockError::Poisoned(error)) => {
                return Err(MetadataBackendError::Serde(format!(
                    "resident metadata shard {} state lock failed: {error}",
                    self.config.shard_id
                )));
            }
        };
        match &*state {
            ResidentLocalMetadataShardState::Available(_) => self.last_status_snapshot(),
            ResidentLocalMetadataShardState::Opening
            | ResidentLocalMetadataShardState::CheckedOut => self.last_status_snapshot(),
            ResidentLocalMetadataShardState::Unopened => {
                Err(MetadataBackendError::StatusUnhydrated {
                    shard_id: DfsMetadataShardId::new(self.config.shard_id.clone()),
                })
            }
        }
    }

    fn last_status_snapshot(&self) -> Result<ResidentLocalMetadataShardStatusSnapshot> {
        self.operator_status
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot
            .clone()
            .ok_or_else(|| MetadataBackendError::StatusUnavailable {
                reason: format!(
                    "resident metadata shard {} has no completed generation for bounded status",
                    self.config.shard_id
                ),
            })
    }

    fn is_open(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        matches!(
            *state,
            ResidentLocalMetadataShardState::Available(_)
                | ResidentLocalMetadataShardState::CheckedOut
        )
    }
}

#[cfg(test)]
thread_local! {
    static RESIDENT_METADATA_STATUS_DURABLE_CURSOR_READS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_resident_metadata_status_durable_cursor_reads() {
    RESIDENT_METADATA_STATUS_DURABLE_CURSOR_READS.set(0);
}

#[cfg(test)]
fn resident_metadata_status_durable_cursor_reads() -> usize {
    RESIDENT_METADATA_STATUS_DURABLE_CURSOR_READS.get()
}

fn initialize_resident_metadata_shard_status_snapshot(
    config: &DfsMetadataShardConfig,
    backend: &LocalSobjMetadataBackend,
) -> ResidentLocalMetadataShardStatusSnapshot {
    #[cfg(test)]
    RESIDENT_METADATA_STATUS_DURABLE_CURSOR_READS.set(
        RESIDENT_METADATA_STATUS_DURABLE_CURSOR_READS
            .get()
            .saturating_add(1),
    );
    let incomplete_reasons = backend
        .initialize_resident_metadata_cursor_status_projection(
            &config.replica_node_ids,
            &DfsMetadataShardId::new(config.shard_id.clone()),
        )
        .err()
        .map(|error| {
            vec![format!(
                "metadata shard {} cursor projection could not be initialized: {error}",
                config.shard_id
            )]
        })
        .unwrap_or_default();
    resident_metadata_shard_status_snapshot_from_projection(config, backend, incomplete_reasons)
}

fn resident_metadata_shard_status_snapshot_from_projection(
    config: &DfsMetadataShardConfig,
    backend: &LocalSobjMetadataBackend,
    mut incomplete_reasons: Vec<String>,
) -> ResidentLocalMetadataShardStatusSnapshot {
    let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
    let journal_last_sequence = backend.journal_last_sequence();
    let (replica_lag, replay_cursors) = match backend
        .resident_metadata_cursor_status_projection(&config.replica_node_ids, &shard_id)
    {
        Some((replication_cursors, replay_cursors)) => (
            replication_cursors
                .into_iter()
                .map(
                    |(target_node_id, replicated_through_sequence)| DfsMetadataReplicaLagStatus {
                        target_node_id,
                        replicated_through_sequence,
                        lag_entries: journal_last_sequence
                            .saturating_sub(replicated_through_sequence),
                    },
                )
                .collect(),
            replay_cursors,
        ),
        None => {
            if incomplete_reasons.is_empty() {
                incomplete_reasons.push(format!(
                    "metadata shard {} has no complete resident cursor projection",
                    config.shard_id
                ));
            }
            (Vec::new(), Vec::new())
        }
    };
    ResidentLocalMetadataShardStatusSnapshot {
        summary: backend.resident_metadata_status_summary(),
        shard_status: DfsMetadataShardStatus {
            shard_id,
            file_system_id: Some(config.file_system_id.clone()),
            primary_node_id: Some(config.primary_node_id.clone()),
            replica_node_ids: config.replica_node_ids.clone(),
            epoch: Some(config.epoch),
            journal_next_sequence: journal_last_sequence.saturating_add(1),
            journal_last_sequence,
            replica_lag,
            replay_cursors,
        },
        incomplete_reasons,
    }
}

/// A cheap, exact-shard handle into a resident local metadata generation.
///
/// Cloning this value does not clone or reopen metadata. Each clone points to
/// the same shard-local backend and therefore observes the same process-local
/// membership-handoff authority binding.
#[derive(Clone, Debug)]
pub struct ResidentLocalMetadataShardHandle {
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    slot: Arc<ResidentLocalMetadataShardSlot>,
}

impl ResidentLocalMetadataShardHandle {
    pub fn file_system_id(&self) -> &DfsFileSystemId {
        &self.file_system_id
    }

    pub fn shard_id(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }

    /// Check out only this shard's resident backend for one bounded operation.
    /// A competing checkout waits until the returned lease is dropped.
    pub fn checkout(&self) -> Result<ResidentLocalMetadataShardLease> {
        let backend = self.slot.checkout()?;
        Ok(ResidentLocalMetadataShardLease {
            file_system_id: self.file_system_id.clone(),
            shard_id: self.shard_id.clone(),
            slot: Arc::clone(&self.slot),
            backend: Some(backend),
        })
    }

    /// Attempt to check out this shard without waiting for a competing owner.
    ///
    /// `Ok(None)` means the shard is opening or checked out. Callers that hold
    /// another admission or lock can release it before waiting for
    /// [`Self::wait_until_checkout_available`], avoiding cross-lock cycles.
    pub fn try_checkout(&self) -> Result<Option<ResidentLocalMetadataShardLease>> {
        self.slot.try_checkout().map(|backend| {
            backend.map(|backend| ResidentLocalMetadataShardLease {
                file_system_id: self.file_system_id.clone(),
                shard_id: self.shard_id.clone(),
                slot: Arc::clone(&self.slot),
                backend: Some(backend),
            })
        })
    }

    /// Wait asynchronously until a checkout can be retried without retaining
    /// a shard lease or another subsystem's admission guard.
    pub async fn wait_until_checkout_available(&self) -> Result<()> {
        self.slot.wait_until_checkout_available().await
    }

    /// Run one bounded operation against the exact resident backend instance.
    pub fn with_backend<R>(
        &self,
        operation: impl FnOnce(&mut LocalSobjMetadataBackend) -> Result<R>,
    ) -> Result<R> {
        let mut lease = self.checkout()?;
        operation(&mut lease)
    }

    pub fn is_open(&self) -> bool {
        self.slot.is_open()
    }
}

/// Owned, exclusive access to one resident shard-local metadata backend.
///
/// Dropping the lease returns the same backend instance to its resident slot.
pub struct ResidentLocalMetadataShardLease {
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    slot: Arc<ResidentLocalMetadataShardSlot>,
    backend: Option<Box<LocalSobjMetadataBackend>>,
}

impl std::fmt::Debug for ResidentLocalMetadataShardLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResidentLocalMetadataShardLease")
            .field("file_system_id", &self.file_system_id)
            .field("shard_id", &self.shard_id)
            .finish_non_exhaustive()
    }
}

impl ResidentLocalMetadataShardLease {
    pub fn file_system_id(&self) -> &DfsFileSystemId {
        &self.file_system_id
    }

    pub fn shard_id(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }

    pub fn shard_root(&self) -> &Path {
        &self.slot.root
    }

    pub(crate) fn metadata_replication_cursor_status_observer(
        &self,
    ) -> ResidentMetadataReplicationCursorStatusObserver {
        ResidentMetadataReplicationCursorStatusObserver {
            shard_id: self.shard_id.clone(),
            slot: Arc::clone(&self.slot),
            projection: self
                .backend
                .as_ref()
                .expect("resident metadata shard lease retains backend until drop")
                .resident_metadata_replication_cursor_projection_observer(),
        }
    }
}

impl Deref for ResidentLocalMetadataShardLease {
    type Target = LocalSobjMetadataBackend;

    fn deref(&self) -> &Self::Target {
        self.backend
            .as_ref()
            .expect("resident metadata shard lease retains backend until drop")
    }
}

impl DerefMut for ResidentLocalMetadataShardLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.backend
            .as_mut()
            .expect("resident metadata shard lease retains backend until drop")
    }
}

impl Drop for ResidentLocalMetadataShardLease {
    fn drop(&mut self) {
        if let Some(backend) = self.backend.take() {
            self.slot.restore(backend);
        }
    }
}

#[derive(Debug)]
struct ResidentLocalShardedMetadataRouterState {
    root: PathBuf,
    local_node_id: Option<NodeId>,
    shard_configs: BTreeMap<String, DfsMetadataShardConfig>,
    routing_indexes: BTreeMap<DfsFileSystemId, DfsMetadataShardRoutingIndex>,
    fallback_routing_indexes: Vec<BTreeMap<DfsFileSystemId, DfsMetadataShardRoutingIndex>>,
}

#[derive(Debug)]
struct ResidentLocalShardedMetadataBackendInner {
    router: Mutex<ResidentLocalShardedMetadataRouterState>,
    shards: BTreeMap<DfsMetadataShardId, Arc<ResidentLocalMetadataShardSlot>>,
}

/// One resident sharded metadata generation with independent shard locks.
///
/// This is intentionally a pool of the original shard backend instances, not
/// an `Arc<Mutex<LocalShardedMetadataBackend>>`: unrelated shards can execute
/// concurrently, while same-shard calls retain the existing serialization and
/// authority semantics.
#[derive(Clone, Debug)]
pub struct ResidentLocalShardedMetadataBackend {
    inner: Arc<ResidentLocalShardedMetadataBackendInner>,
}

impl ResidentLocalShardedMetadataBackend {
    pub fn shard_ids(&self) -> Vec<DfsMetadataShardId> {
        self.inner.shards.keys().cloned().collect()
    }

    pub fn shard_config(&self, shard_id: &DfsMetadataShardId) -> Result<DfsMetadataShardConfig> {
        self.with_router(|router| router.shard_configs.get(&shard_id.0).cloned())?
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "unknown resident DFS metadata shard: {}",
                    shard_id.0
                ))
            })
    }

    /// Verify that this resident generation is the exact durable router the
    /// caller intends to borrow. This is used at boundaries where cluster KV
    /// may have changed after a resident handle was captured.
    pub fn matches_topology(
        &self,
        metadata_root: &Path,
        shards: &[DfsMetadataShardConfig],
    ) -> Result<bool> {
        let expected = shards
            .iter()
            .map(|shard| (shard.shard_id.clone(), shard.clone()))
            .collect::<BTreeMap<_, _>>();
        if expected.len() != shards.len() {
            return Ok(false);
        }
        self.with_router(|router| router.root == metadata_root && router.shard_configs == expected)
    }

    /// Assemble an operator status image without checking out any shard or
    /// scanning a materialized metadata table. Shards currently owned by a
    /// mutation contribute their last completed generation.
    ///
    /// Historical routing fallbacks mean a namespace move can temporarily
    /// have live copies in both the source and destination shards. Determining
    /// which copy is authoritative requires the durable routing/index probes
    /// used by the exact scanner, so this bounded path omits visibility-based
    /// counters and marks the image incomplete until migration retires the
    /// fallbacks. Status itself must remain bounded and available.
    pub fn bounded_status_snapshot(&self) -> Result<DfsResidentMetadataStatusSnapshot> {
        // Retain the router generation while collecting every slot image.
        // Global handoff restore holds the same lock while rebinding slot
        // status, so an operator snapshot cannot pair predecessor topology
        // with successor lag rows (or vice versa).
        let router = self.inner.router.lock().map_err(|error| {
            MetadataBackendError::Serde(format!(
                "resident DFS metadata router lock failed: {error}"
            ))
        })?;
        let local_node_id = router.local_node_id.clone();
        let shard_configs = router.shard_configs.clone();
        let fallback_count = router.fallback_routing_indexes.len();
        let mut summary = DfsMetadataSummary::default();
        let mut primary_summary = DfsMetadataSummary::default();
        let mut file_system_ids = BTreeSet::new();
        let mut primary_file_system_ids = BTreeSet::new();
        let mut metadata_shards = Vec::with_capacity(shard_configs.len());
        let mut incomplete_reasons = Vec::new();
        let mut bounded_omissions = Vec::new();
        let mut every_configured_shard_summary_available = true;
        for config in shard_configs.values() {
            let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
            let local_primary = local_node_id
                .as_ref()
                .is_none_or(|local_node_id| config.primary_node_id == *local_node_id);
            file_system_ids.insert(config.file_system_id.clone());
            if local_primary {
                primary_file_system_ids.insert(config.file_system_id.clone());
            }
            let Some(slot) = self.inner.shards.get(&shard_id) else {
                every_configured_shard_summary_available = false;
                incomplete_reasons.push(format!(
                    "metadata shard {} has no resident bounded-status slot",
                    shard_id.0
                ));
                metadata_shards.push(metadata_shard_topology_status(config));
                continue;
            };
            let mut snapshot = match slot.bounded_status_snapshot() {
                Ok(snapshot) => snapshot,
                Err(MetadataBackendError::StatusUnhydrated {
                    shard_id: unhydrated_shard_id,
                }) if unhydrated_shard_id == shard_id && !local_primary => {
                    every_configured_shard_summary_available = false;
                    let omission = non_primary_unhydrated_status_omission(
                        local_node_id.as_ref(),
                        config,
                        &shard_id,
                    )
                    .expect("non-primary shard must produce a bounded omission");
                    incomplete_reasons.push(omission.legacy_reason());
                    bounded_omissions.push(omission);
                    metadata_shards.push(metadata_shard_topology_status(config));
                    continue;
                }
                Err(error) => {
                    every_configured_shard_summary_available = false;
                    incomplete_reasons.push(format!(
                        "metadata shard {} status unavailable: {error}",
                        shard_id.0
                    ));
                    metadata_shards.push(metadata_shard_topology_status(config));
                    continue;
                }
            };
            incomplete_reasons.append(&mut snapshot.incomplete_reasons);
            accumulate_metadata_summary_detail_counts(&mut summary, &snapshot.summary);
            if local_primary {
                accumulate_metadata_summary_detail_counts(&mut primary_summary, &snapshot.summary);
            }

            // The router config is the generation-level authority for
            // topology fields. A checked-out shard snapshot can legitimately
            // predate a just-completed topology update.
            snapshot.shard_status.file_system_id = Some(config.file_system_id.clone());
            snapshot.shard_status.primary_node_id = Some(config.primary_node_id.clone());
            snapshot.shard_status.replica_node_ids = config.replica_node_ids.clone();
            snapshot.shard_status.epoch = Some(config.epoch);
            metadata_shards.push(snapshot.shard_status);
        }
        drop(router);
        summary.file_system_count = file_system_ids.len();
        primary_summary.file_system_count = primary_file_system_ids.len();
        if fallback_count > 0 {
            let cross_shard_namespace_locks_zero_proven = every_configured_shard_summary_available
                && summary.cross_shard_namespace_lock_count == 0;
            let file_system_count = summary.file_system_count;
            let primary_file_system_count = primary_summary.file_system_count;
            summary = DfsMetadataSummary {
                file_system_count,
                ..DfsMetadataSummary::default()
            };
            primary_summary = DfsMetadataSummary {
                file_system_count: primary_file_system_count,
                ..DfsMetadataSummary::default()
            };
            let omission = if cross_shard_namespace_locks_zero_proven {
                DfsOperatorStatusOmission::historical_routing_topology_with_cross_shard_lock_zero_proof(
                    fallback_count,
                )
            } else {
                DfsOperatorStatusOmission::historical_routing_topology(fallback_count)
            };
            let legacy_reason = omission.legacy_reason();
            incomplete_reasons.push(legacy_reason.clone());
            bounded_omissions.push(omission);
        }
        incomplete_reasons.sort();
        incomplete_reasons.dedup();
        Ok(DfsResidentMetadataStatusSnapshot {
            summary,
            primary_summary,
            metadata_shards,
            incomplete_reasons,
            bounded_omissions,
        })
    }

    /// Scope a handle to an exact filesystem and shard identity.
    ///
    /// Requiring both identities prevents a wire-derived shard name from
    /// borrowing an unrelated filesystem's resident backend.
    pub fn scope_shard(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<ResidentLocalMetadataShardHandle> {
        let slot = self.inner.shards.get(shard_id).ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "unknown resident DFS metadata shard: {}",
                shard_id.0
            ))
        })?;
        let configured_file_system_id = self
            .with_router(|router| {
                router
                    .shard_configs
                    .get(&shard_id.0)
                    .map(|config| config.file_system_id.clone())
            })?
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "resident DFS metadata shard {} has no routing config",
                    shard_id.0
                ))
            })?;
        if configured_file_system_id != *file_system_id {
            return Err(MetadataBackendError::Serde(format!(
                "resident DFS metadata shard {} belongs to filesystem {}, not {}",
                shard_id.0, configured_file_system_id, file_system_id
            )));
        }
        Ok(ResidentLocalMetadataShardHandle {
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.clone(),
            slot: Arc::clone(slot),
        })
    }

    /// Check out every shard in canonical shard-id order and reconstruct the
    /// original router around those exact instances.
    pub fn checkout_global(&self) -> Result<ResidentLocalShardedMetadataBackendLease> {
        let mut leases = Vec::with_capacity(self.inner.shards.len());
        for (shard_id, slot) in &self.inner.shards {
            let backend = slot.checkout()?;
            leases.push((
                shard_id.clone(),
                ResidentLocalMetadataShardLease {
                    file_system_id: slot.config.file_system_id.clone(),
                    shard_id: shard_id.clone(),
                    slot: Arc::clone(slot),
                    backend: Some(backend),
                },
            ));
        }
        // The first canonical shard also serializes competing global
        // checkouts. Read router state only after every shard is owned so a
        // waiter cannot capture and later restore a predecessor generation.
        let router = self.with_router(|router| ResidentLocalShardedMetadataRouterState {
            root: router.root.clone(),
            local_node_id: router.local_node_id.clone(),
            shard_configs: router.shard_configs.clone(),
            routing_indexes: router.routing_indexes.clone(),
            fallback_routing_indexes: router.fallback_routing_indexes.clone(),
        })?;
        let mut shards = BTreeMap::new();
        for (shard_id, mut lease) in leases {
            let slot = Arc::clone(&lease.slot);
            let backend = lease
                .backend
                .take()
                .expect("new global resident lease owns checked-out shard");
            let config = router
                .shard_configs
                .get(&shard_id.0)
                .cloned()
                .unwrap_or_else(|| slot.config.clone());
            let local_slot = LocalMetadataShardSlot {
                config,
                root: slot.root.clone(),
                open_mode: slot.open_mode,
                initialization: Mutex::new(()),
                backend: OnceLock::new(),
            };
            local_slot
                .backend
                .set(backend)
                .expect("global resident checkout installs exact shard backend");
            shards.insert(shard_id, local_slot);
        }
        Ok(ResidentLocalShardedMetadataBackendLease {
            resident: self.clone(),
            backend: Some(LocalShardedMetadataBackend {
                root: router.root,
                local_node_id: router.local_node_id,
                shard_configs: router.shard_configs,
                routing_indexes: router.routing_indexes,
                fallback_routing_indexes: router.fallback_routing_indexes,
                shards,
            }),
        })
    }

    fn with_router<R>(
        &self,
        operation: impl FnOnce(&ResidentLocalShardedMetadataRouterState) -> R,
    ) -> Result<R> {
        let router = self.inner.router.lock().map_err(|error| {
            MetadataBackendError::Serde(format!(
                "resident DFS metadata router lock failed: {error}"
            ))
        })?;
        Ok(operation(&router))
    }
}

/// Owned, exclusive access to every shard in one resident router generation.
/// Dropping this lease restores the same shard instances and any router state
/// changed by handoff or routing-fallback reconciliation.
pub struct ResidentLocalShardedMetadataBackendLease {
    resident: ResidentLocalShardedMetadataBackend,
    backend: Option<LocalShardedMetadataBackend>,
}

impl std::fmt::Debug for ResidentLocalShardedMetadataBackendLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResidentLocalShardedMetadataBackendLease")
            .field("shard_count", &self.resident.inner.shards.len())
            .finish_non_exhaustive()
    }
}

impl Deref for ResidentLocalShardedMetadataBackendLease {
    type Target = LocalShardedMetadataBackend;

    fn deref(&self) -> &Self::Target {
        self.backend
            .as_ref()
            .expect("resident global metadata lease retains router until drop")
    }
}

impl DerefMut for ResidentLocalShardedMetadataBackendLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.backend
            .as_mut()
            .expect("resident global metadata lease retains router until drop")
    }
}

impl Drop for ResidentLocalShardedMetadataBackendLease {
    fn drop(&mut self) {
        let Some(backend) = self.backend.take() else {
            return;
        };
        let LocalShardedMetadataBackend {
            root,
            local_node_id,
            shard_configs,
            routing_indexes,
            fallback_routing_indexes,
            shards,
        } = backend;
        let mut router = self
            .resident
            .inner
            .router
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for (shard_id, slot) in shards {
            let backend = slot
                .backend
                .into_inner()
                .expect("global resident lease returns every checked-out shard backend");
            let resident_slot = self
                .resident
                .inner
                .shards
                .get(&shard_id)
                .expect("global resident lease returns known shard");
            let status_config = shard_configs
                .get(&shard_id.0)
                .unwrap_or(&resident_slot.config);
            resident_slot.restore_with_status_config(backend, Some(status_config));
        }
        *router = ResidentLocalShardedMetadataRouterState {
            root,
            local_node_id,
            shard_configs,
            routing_indexes,
            fallback_routing_indexes,
        };
    }
}

impl LocalMetadataShardSlot {
    fn new(
        config: DfsMetadataShardConfig,
        root: PathBuf,
        open_mode: LocalMetadataShardOpenMode,
    ) -> Self {
        Self {
            config,
            root,
            open_mode,
            initialization: Mutex::new(()),
            backend: OnceLock::new(),
        }
    }

    fn should_open_eagerly(&self) -> bool {
        self.open_mode != LocalMetadataShardOpenMode::ReplicaRepair
    }

    fn open_backend(&self) -> Result<Box<LocalSobjMetadataBackend>> {
        open_local_metadata_shard_backend(&self.config, &self.root, self.open_mode).map(Box::new)
    }

    fn backend(&self) -> Result<&LocalSobjMetadataBackend> {
        if self.backend.get().is_none() {
            let _initialization = self.initialization.lock().map_err(|error| {
                MetadataBackendError::Serde(format!(
                    "metadata shard {} initialization lock failed: {error}",
                    self.config.shard_id
                ))
            })?;
            if self.backend.get().is_none() {
                let backend = self.open_backend()?;
                self.backend
                    .set(backend)
                    .expect("metadata shard initialization lock admits one backend");
            }
        }
        Ok(self
            .backend
            .get()
            .expect("metadata shard slot initialized above")
            .as_ref())
    }

    fn backend_mut(&mut self) -> Result<&mut LocalSobjMetadataBackend> {
        if self.backend.get().is_none() {
            let backend = self.open_backend()?;
            self.backend
                .set(backend)
                .expect("exclusive metadata shard slot initialization succeeds");
        }
        Ok(self
            .backend
            .get_mut()
            .expect("metadata shard slot initialized above")
            .as_mut())
    }

    fn opened_backend_mut(&mut self) -> Option<&mut LocalSobjMetadataBackend> {
        self.backend.get_mut().map(Box::as_mut)
    }
}

fn open_local_metadata_shard_backend(
    config: &DfsMetadataShardConfig,
    root: &Path,
    open_mode: LocalMetadataShardOpenMode,
) -> Result<LocalSobjMetadataBackend> {
    let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
    let mut backend = match open_mode {
        LocalMetadataShardOpenMode::Strict => {
            LocalSobjMetadataBackend::new(shard_id, root.to_path_buf())?
        }
        LocalMetadataShardOpenMode::ReplicaRepair => {
            match LocalSobjMetadataBackend::new(shard_id.clone(), root.to_path_buf()) {
                Ok(backend) => backend,
                Err(open_error) => LocalSobjMetadataBackend::open_for_replica_repair(
                    shard_id,
                    root.to_path_buf(),
                )
                .map_err(|repair_error| {
                    MetadataBackendError::Serde(format!(
                        "cannot open replica metadata shard {}: {open_error}; repair-only open failed: {repair_error}",
                        config.shard_id
                    ))
                })?,
            }
        }
        LocalMetadataShardOpenMode::Inspection => {
            LocalSobjMetadataBackend::open_for_inspection(shard_id, root.to_path_buf())?
        }
    };
    backend.set_namespace_prepare_codec(config.namespace_prepare_codec);
    Ok(backend)
}

#[derive(Debug)]
pub struct LocalShardedMetadataBackend {
    root: PathBuf,
    local_node_id: Option<NodeId>,
    shard_configs: BTreeMap<String, DfsMetadataShardConfig>,
    routing_indexes: BTreeMap<DfsFileSystemId, DfsMetadataShardRoutingIndex>,
    fallback_routing_indexes: Vec<BTreeMap<DfsFileSystemId, DfsMetadataShardRoutingIndex>>,
    shards: BTreeMap<DfsMetadataShardId, LocalMetadataShardSlot>,
}

impl LocalShardedMetadataBackend {
    /// Open or initialize a local router over the configured metadata shards.
    ///
    /// Each shard config gets its own `LocalSobjMetadataBackend` under `root`.
    /// Cross-shard object placement is tracked in this backend's shard-index
    /// directory, while shard-local tables and journals stay inside each local
    /// shard backend.
    pub fn new(root: impl Into<PathBuf>, shards: Vec<DfsMetadataShardConfig>) -> Result<Self> {
        let root = root.into();
        Self::open_with(root, shards, true, None)
    }

    /// Open configured shards while allowing unreadable non-primary mirrors
    /// to enter the snapshot-repair handshake. A shard primary always retains
    /// strict open/replay semantics.
    pub fn new_with_replica_repair(
        root: impl Into<PathBuf>,
        shards: Vec<DfsMetadataShardConfig>,
        local_node_id: &NodeId,
    ) -> Result<Self> {
        let root = root.into();
        Self::open_with(root, shards, true, Some(local_node_id))
    }

    /// Open a sharded backend for read-only inspection/status.
    ///
    /// Shard-local backends use their inspection open mode, so this path avoids
    /// recovery writes while still reading the durable materialized tables.
    pub fn open_for_inspection(
        root: impl Into<PathBuf>,
        shards: Vec<DfsMetadataShardConfig>,
    ) -> Result<Self> {
        let root = root.into();
        Self::open_with(root, shards, false, None)
    }

    /// Open read-only shard state while retaining the local node identity used
    /// to project primary-only maintenance work. This does not grant write
    /// authority; shard-local backends remain in inspection mode.
    pub fn open_for_inspection_at_node(
        root: impl Into<PathBuf>,
        shards: Vec<DfsMetadataShardConfig>,
        local_node_id: &NodeId,
    ) -> Result<Self> {
        let root = root.into();
        Self::open_with(root, shards, false, Some(local_node_id))
    }

    /// Move this router's exact shard backend instances into a resident pool.
    ///
    /// Already-open shards are moved without decoding or recovery. Deferred
    /// replica-repair slots remain lazy and are opened once by their first
    /// scoped handle. The consumed router cannot subsequently create a second
    /// process-local authority capability for the same pool generation.
    pub fn into_resident(self) -> ResidentLocalShardedMetadataBackend {
        let Self {
            root,
            local_node_id,
            shard_configs,
            routing_indexes,
            fallback_routing_indexes,
            shards,
        } = self;
        let shards = shards
            .into_iter()
            .map(|(shard_id, slot)| {
                (
                    shard_id,
                    Arc::new(ResidentLocalMetadataShardSlot::from_local_slot(slot)),
                )
            })
            .collect();
        ResidentLocalShardedMetadataBackend {
            inner: Arc::new(ResidentLocalShardedMetadataBackendInner {
                router: Mutex::new(ResidentLocalShardedMetadataRouterState {
                    root,
                    local_node_id,
                    shard_configs,
                    routing_indexes,
                    fallback_routing_indexes,
                }),
                shards,
            }),
        }
    }

    fn open_with(
        root: PathBuf,
        mut shards: Vec<DfsMetadataShardConfig>,
        initialize_layout: bool,
        replica_repair_local_node_id: Option<&NodeId>,
    ) -> Result<Self> {
        if shards.is_empty() {
            return Err(MetadataBackendError::Serde(
                "local sharded metadata backend requires at least one shard".to_string(),
            ));
        }
        if initialize_layout {
            create_dir_all(root.join("shard-index").join("objects"))?;
        }
        canonicalize_routing_topology(&mut shards);
        let routing_history = load_routing_topology_history(&root, &shards, initialize_layout)?;
        let mut shard_configs = BTreeMap::new();
        let mut shard_slots = Vec::with_capacity(shards.len());
        for shard in shards {
            let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
            if shard_configs
                .insert(shard.shard_id.clone(), shard.clone())
                .is_some()
            {
                return Err(MetadataBackendError::Serde(format!(
                    "duplicate DFS metadata shard config: {}",
                    shard.shard_id
                )));
            }
            let shard_root = root.join(key_segment(&shard_id.0));
            let open_mode = if !initialize_layout {
                LocalMetadataShardOpenMode::Inspection
            } else if replica_repair_local_node_id
                .is_some_and(|local_node_id| shard.primary_node_id != *local_node_id)
            {
                LocalMetadataShardOpenMode::ReplicaRepair
            } else {
                LocalMetadataShardOpenMode::Strict
            };
            shard_slots.push((
                shard_id,
                LocalMetadataShardSlot::new(shard, shard_root, open_mode),
            ));
        }
        let replica_repair_local_node_id = replica_repair_local_node_id.cloned();
        let (eager_slots, deferred_slots): (Vec<_>, Vec<_>) = shard_slots
            .into_iter()
            .partition(|(_, slot)| slot.should_open_eagerly());
        #[cfg(test)]
        let eager_open_count = eager_slots.len();
        let mut opened = std::thread::scope(|scope| {
            let handles = eager_slots
                .into_iter()
                .map(|(shard_id, slot)| {
                    scope.spawn(move || {
                        let backend = slot.open_backend()?;
                        slot.backend
                            .set(backend)
                            .expect("new metadata shard slot accepts eager backend initialization");
                        Ok((shard_id, slot))
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().map_err(|_| {
                        MetadataBackendError::Serde(
                            "local DFS metadata shard open worker panicked".to_string(),
                        )
                    })?
                })
                .collect::<Result<Vec<_>>>()
        })?;
        opened.extend(deferred_slots);
        #[cfg(test)]
        crate::metadata_backend::record_metadata_backend_open_calls_from_workers(eager_open_count);
        let backends = opened.into_iter().collect::<BTreeMap<_, _>>();
        let routing_indexes = compile_routing_indexes(shard_configs.values().cloned())?;
        let fallback_routing_indexes = routing_history
            .fallbacks
            .iter()
            .map(|topology| compile_routing_indexes(topology.iter().cloned()))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            root,
            local_node_id: replica_repair_local_node_id,
            shard_configs,
            routing_indexes,
            fallback_routing_indexes,
            shards: backends,
        })
    }

    pub(crate) fn shard_ids(&self) -> Vec<DfsMetadataShardId> {
        self.shards.keys().cloned().collect()
    }

    /// Refresh and pin every inspection-only shard without taking writer locks
    /// or running authoritative recovery.
    pub(crate) fn begin_inspection_snapshot(&mut self) -> Result<()> {
        let shard_ids = self.shards.keys().cloned().collect::<Vec<_>>();
        for (index, shard_id) in shard_ids.iter().enumerate() {
            let begin = self
                .shards
                .get_mut(shard_id)
                .expect("collected shard id remains installed")
                .backend_mut()
                .and_then(LocalSobjMetadataBackend::begin_inspection_snapshot);
            if let Err(error) = begin {
                for pinned_shard_id in &shard_ids[..index] {
                    self.shards
                        .get_mut(pinned_shard_id)
                        .expect("pinned inspection shard remains installed")
                        .opened_backend_mut()
                        .expect("pinned inspection shard was opened")
                        .end_inspection_snapshot();
                }
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn end_inspection_snapshot(&mut self) {
        for slot in self.shards.values_mut() {
            if let Some(backend) = slot.opened_backend_mut() {
                backend.end_inspection_snapshot();
            }
        }
    }

    fn shard_is_local_authoritative_primary(&self, shard_id: &DfsMetadataShardId) -> bool {
        self.local_node_id.as_ref().is_none_or(|local_node_id| {
            self.shard_configs
                .get(&shard_id.0)
                .is_some_and(|config| config.primary_node_id == *local_node_id)
        })
    }

    #[cfg(test)]
    pub(crate) fn object_shard_index_path(&self, object_id: &DfsObjectId) -> PathBuf {
        object_shard_index_path(&self.root, object_id)
    }

    pub(crate) fn read_object_shard_index(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectShardIndexRecord>> {
        read_object_shard_index(&self.root, object_id)
    }

    pub(crate) fn write_object_shard_index(
        &self,
        object: &DfsObjectMetadata,
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        write_object_shard_index(&self.root, object, shard_id)
    }

    fn write_content_commit_object_shard_index(
        &self,
        object: &DfsObjectMetadata,
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        write_content_commit_object_shard_index(&self.root, object, shard_id)
    }

    fn write_transaction_object_shard_indexes(
        &self,
        objects: &[DfsObjectMetadata],
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        write_transaction_object_shard_indexes(&self.root, objects, shard_id)
    }

    fn unique_namespace_prepare_shard_id(
        &self,
        operation_id: &str,
        include_terminal_receipt: bool,
    ) -> Result<DfsMetadataShardId> {
        let mut found_shard_id = None;
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            let present = if include_terminal_receipt {
                backend.has_current_namespace_prepare_identity(operation_id)?
            } else {
                backend.has_current_namespace_prepare(operation_id)?
            };
            if present {
                if found_shard_id.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {operation_id} exists on multiple local shards"
                    )));
                }
                found_shard_id = Some(shard_id.clone());
            }
        }
        found_shard_id.ok_or_else(|| {
            MetadataBackendError::Serde(format!("namespace prepare {operation_id} does not exist"))
        })
    }

    fn namespace_prepare_routing_records(
        &self,
        shard_id: &DfsMetadataShardId,
        operation_id: &str,
    ) -> Result<Vec<DfsObjectShardIndexRecord>> {
        let backend = self.shard_backend(shard_id)?;
        if let Some(prepare) = backend.namespace_prepare_ref(operation_id) {
            return Ok(prepare
                .transaction
                .objects
                .iter()
                .map(|object| object_shard_index_record(object, shard_id))
                .collect());
        }
        if backend
            .namespace_prepare_receipt_ref(operation_id)
            .is_some()
        {
            return Ok(Vec::new());
        }
        Err(MetadataBackendError::Serde(format!(
            "namespace prepare {operation_id} disappeared from shard {}",
            shard_id.0
        )))
    }

    fn remove_object_shard_index(&self, object_id: &DfsObjectId) -> Result<()> {
        remove_object_shard_index(&self.root, object_id)
    }

    pub(crate) fn shard_backend(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<&LocalSobjMetadataBackend> {
        self.shards
            .get(shard_id)
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "unknown local DFS metadata shard: {}",
                    shard_id.0
                ))
            })?
            .backend()
    }

    pub(crate) fn shard_backend_mut(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<&mut LocalSobjMetadataBackend> {
        self.shards
            .get_mut(shard_id)
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "unknown local DFS metadata shard: {}",
                    shard_id.0
                ))
            })?
            .backend_mut()
    }

    #[cfg(test)]
    fn opened_shard_ids(&self) -> BTreeSet<DfsMetadataShardId> {
        self.shards
            .iter()
            .filter_map(|(shard_id, slot)| slot.backend.get().map(|_| shard_id.clone()))
            .collect()
    }

    fn require_replica_ingest_permit_for_shard(
        &self,
        permit: &DfsMetadataReplicaIngestPermit,
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        let shard = self.shard_configs.get(&shard_id.0).ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "unknown configured DFS metadata shard: {}",
                shard_id.0
            ))
        })?;
        if permit.file_system_id() != &shard.file_system_id || permit.shard_id() != shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata replica ingest permit for filesystem {} shard {} does not match configured filesystem {} shard {}",
                permit.file_system_id(),
                permit.shard_id().0,
                shard.file_system_id,
                shard.shard_id,
            )));
        }
        Ok(())
    }

    fn configured_shard_ids_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsMetadataShardId>> {
        let shard_ids = self
            .shard_configs
            .values()
            .filter(|shard| shard.file_system_id == *file_system_id)
            .map(|shard| DfsMetadataShardId::new(shard.shard_id.clone()))
            .collect::<Vec<_>>();
        if shard_ids.is_empty() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: file_system_id.clone(),
            });
        }
        Ok(shard_ids)
    }

    fn select_shard_id_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<DfsMetadataShardId> {
        let selected = self
            .routing_indexes
            .get(file_system_id)
            .ok_or_else(|| MetadataBackendError::UnknownFileSystem {
                file_system_id: file_system_id.clone(),
            })?
            .select_shard_id(path, false)
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
        selected.map(DfsMetadataShardId::new).ok_or_else(|| {
            MetadataBackendError::UnknownFileSystem {
                file_system_id: file_system_id.clone(),
            }
        })
    }

    fn fallback_shard_ids_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
        selected_shard_id: &DfsMetadataShardId,
    ) -> Result<Vec<DfsMetadataShardId>> {
        let mut shard_ids = Vec::new();
        let mut seen = BTreeSet::new();
        seen.insert(selected_shard_id.clone());
        for routing_indexes in &self.fallback_routing_indexes {
            let Some(index) = routing_indexes.get(file_system_id) else {
                continue;
            };
            let Some(shard_id) = index
                .select_shard_id(path, false)
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))?
            else {
                continue;
            };
            let shard_id = DfsMetadataShardId::new(shard_id);
            if self.shards.contains_key(&shard_id) && seen.insert(shard_id.clone()) {
                shard_ids.push(shard_id);
            }
        }
        Ok(shard_ids)
    }

    fn consistent_read_shard_ids_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Vec<DfsMetadataShardId>> {
        let selected = self.select_shard_id_for_path(file_system_id, path)?;
        let mut shard_ids = vec![selected.clone()];
        shard_ids.extend(self.fallback_shard_ids_for_path(file_system_id, path, &selected)?);
        if let Some(missing) = shard_ids
            .iter()
            .find(|shard_id| !self.shards.contains_key(*shard_id))
        {
            return Err(MetadataBackendError::Serde(format!(
                "path {path} routed to unavailable local metadata shard {missing:?}"
            )));
        }
        Ok(shard_ids)
    }

    fn begin_consistent_read_snapshot_for_shards(
        &mut self,
        shard_ids: &[DfsMetadataShardId],
    ) -> Result<()> {
        #[cfg(test)]
        SHARDED_CONSISTENT_READ_SNAPSHOT_SHARDS
            .with(|observed| observed.borrow_mut().extend_from_slice(shard_ids));
        for (index, shard_id) in shard_ids.iter().enumerate() {
            let begin = self
                .shards
                .get_mut(shard_id)
                .expect("routed consistent-read shard remains installed")
                .backend_mut()
                .and_then(LocalSobjMetadataBackend::begin_consistent_read_snapshot);
            if let Err(error) = begin {
                for pinned_shard_id in &shard_ids[..index] {
                    self.shards
                        .get_mut(pinned_shard_id)
                        .expect("pinned consistent-read shard remains installed")
                        .opened_backend_mut()
                        .expect("pinned consistent-read shard was opened")
                        .end_consistent_read_snapshot();
                }
                return Err(error);
            }
        }
        Ok(())
    }

    fn end_consistent_read_snapshot_for_shards(&mut self, shard_ids: &[DfsMetadataShardId]) {
        for shard_id in shard_ids {
            self.shards
                .get_mut(shard_id)
                .expect("routed consistent-read shard remains installed")
                .opened_backend_mut()
                .expect("routed consistent-read shard was opened")
                .end_consistent_read_snapshot();
        }
    }

    /// Retire one filesystem from every historical routing generation after
    /// the caller has completed a quiescent, durable migration proof.
    ///
    /// This performs no metadata scan. Persist the narrowed history before
    /// changing the resident indexes so an I/O failure keeps the conservative
    /// fallback routes active in the current process.
    pub(crate) fn retire_routing_fallbacks_for_file_system(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<()> {
        let path = routing_topology_history_path(&self.root);
        if let Some(mut history) = read_sobj_optional::<DfsMetadataRoutingTopologyHistory>(&path)? {
            let mut changed = false;
            for topology in &mut history.fallbacks {
                let previous_len = topology.len();
                topology.retain(|shard| shard.file_system_id != *file_system_id);
                changed |= topology.len() != previous_len;
            }
            let previous_generation_count = history.fallbacks.len();
            history.fallbacks.retain(|topology| !topology.is_empty());
            changed |= history.fallbacks.len() != previous_generation_count;
            if changed {
                write_sobj_atomic(&path, &history)?;
            }
        }

        for indexes in &mut self.fallback_routing_indexes {
            indexes.remove(file_system_id);
        }
        self.fallback_routing_indexes
            .retain(|indexes| !indexes.is_empty());
        Ok(())
    }

    fn shard_id_for_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsMetadataShardId>> {
        if let Some(index) = self.read_object_shard_index(object_id)? {
            if self.object_shard_index_is_corroborated(object_id, &index)? {
                return Ok(Some(index.shard_id));
            }
        }
        // The object-to-shard record is a rebuildable accelerator. A replica
        // may have no record or may retain the source shard after ingesting a
        // cross-shard move. Fall back to physical/routing authority instead of
        // allowing that derived state to select the source eviction tombstone.
        Ok(self
            .authoritative_object_placement(object_id)?
            .map(|(shard_id, _)| shard_id))
    }

    fn object_shard_index_is_corroborated(
        &self,
        object_id: &DfsObjectId,
        index: &DfsObjectShardIndexRecord,
    ) -> Result<bool> {
        if !self.shards.contains_key(&index.shard_id) {
            return Ok(false);
        }
        let Some(object) = self.shard_backend(&index.shard_id)?.get_object(object_id)? else {
            return Ok(false);
        };
        if is_source_eviction_tombstone(&object) {
            return Ok(false);
        }
        Ok(object.file_system_id == index.file_system_id
            && normalize_path(&object.path) == index.path
            && self
                .select_shard_id_for_path(&object.file_system_id, &object.path)
                .is_ok_and(|selected| selected == index.shard_id))
    }

    /// Resolve physical object authority for merged scans without letting a
    /// stale derived index or shard iteration order override a newer routed
    /// record. Only object-less pending creates fall back to the derived index.
    fn shard_id_for_global_scan_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsMetadataShardId>> {
        if let Some((shard_id, _)) = self.authoritative_object_placement(object_id)? {
            return Ok(Some(shard_id));
        }
        let Some(index) = self.read_object_shard_index(object_id)? else {
            return Ok(None);
        };
        if !self.shards.contains_key(&index.shard_id)
            || self.select_shard_id_for_path(&index.file_system_id, &index.path)? != index.shard_id
        {
            return Ok(None);
        }
        Ok(Some(index.shard_id))
    }

    /// Resolve the one shard whose receipt records may participate in restart
    /// recovery for an object/version pair.
    ///
    /// Visible objects use routed/generation-aware physical authority. A
    /// pending create has no visible object yet, so it uses a valid derived
    /// index when present; an index-less metadata replica falls back to the
    /// unique shard that carries its active intent. Ambiguous physical copies
    /// fail closed rather than allowing an arbitrary shard's receipts to
    /// satisfy write quorum.
    fn shard_id_for_receipt_scan_record(
        &self,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<Option<DfsMetadataShardId>> {
        let object_id = &receipt.receipt.object_id;
        let version = &receipt.receipt.committed_version;
        if let Some(shard_id) = self.shard_id_for_global_scan_object(object_id)? {
            return Ok(Some(shard_id));
        }

        let mut active_intent_shard_id = None;
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            let mut has_active_intent = false;
            walk_metadata_pages(|cursor| {
                let page = backend.intents_for_object_version_page(
                    object_id,
                    version,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                has_active_intent = page.intents.iter().any(|intent| {
                    write_intent_status_is_active(&intent.status)
                        && intent.file_system_id == receipt.receipt.file_system_id
                        && intent.object_id == *object_id
                        && intent.target_version == *version
                        && intent.content_hash == receipt.content_hash
                        && intent.object_metadata.as_ref().is_some_and(|object| {
                            object.object_id == intent.object_id
                                && object.file_system_id == intent.file_system_id
                                && self
                                    .select_shard_id_for_path(&object.file_system_id, &object.path)
                                    .is_ok_and(|selected| selected == *shard_id)
                        })
                });
                if has_active_intent {
                    Ok(None)
                } else {
                    Ok(page.next_cursor)
                }
            })?;
            if !has_active_intent {
                continue;
            }
            if active_intent_shard_id.is_some() {
                return Ok(None);
            }
            active_intent_shard_id = Some(shard_id.clone());
        }
        Ok(active_intent_shard_id)
    }

    /// Resolve the newest physical record for one object without trusting the
    /// derived shard index. Journal batches for different shards can arrive in
    /// either order, and a cross-shard move deliberately leaves a same-version
    /// eviction tombstone behind. The moved live record wins that exact tie;
    /// otherwise later generations win deterministically.
    fn authoritative_object_placement(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<(DfsMetadataShardId, DfsObjectMetadata)>> {
        let index = self.read_object_shard_index(object_id)?;
        self.authoritative_object_placement_with_index(object_id, index.as_ref())
    }

    fn authoritative_object_placement_with_index(
        &self,
        object_id: &DfsObjectId,
        index: Option<&DfsObjectShardIndexRecord>,
    ) -> Result<Option<(DfsMetadataShardId, DfsObjectMetadata)>> {
        let mut candidates = Vec::new();
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            let Some(object) = backend.get_object(object_id)? else {
                continue;
            };
            // A source-eviction marker is physical cleanup evidence, never a
            // namespace deletion. This remains true if a later topology
            // generation routes the path back through the old source shard.
            if is_source_eviction_tombstone(&object) {
                continue;
            }
            let routed_here = self
                .select_shard_id_for_path(&object.file_system_id, &object.path)
                .is_ok_and(|selected| selected == *shard_id);
            candidates.push((shard_id.clone(), object, routed_here));
        }
        let require_routed_candidate = candidates.iter().any(|(_, _, routed_here)| *routed_here);
        let mut selected: Option<(DfsMetadataShardId, DfsObjectMetadata)> = None;
        for (shard_id, object, routed_here) in candidates {
            if require_routed_candidate && !routed_here {
                continue;
            }
            let replace = selected
                .as_ref()
                .is_none_or(|(selected_shard, selected_object)| {
                    let generation = (object.changed_at_unix_nanos, &object.version);
                    let selected_generation = (
                        selected_object.changed_at_unix_nanos,
                        &selected_object.version,
                    );
                    generation > selected_generation
                        || (generation == selected_generation
                            && object.kind != DfsObjectKind::Tombstone
                            && selected_object.kind == DfsObjectKind::Tombstone)
                        || (generation == selected_generation
                            && (object.kind == DfsObjectKind::Tombstone)
                                == (selected_object.kind == DfsObjectKind::Tombstone)
                            && index.is_some_and(|index| {
                                index.shard_id == shard_id && index.shard_id != *selected_shard
                            }))
                });
            if replace {
                selected = Some((shard_id, object));
            }
        }
        Ok(selected)
    }

    fn reconcile_object_shard_indexes<'a>(
        &mut self,
        object_ids: impl IntoIterator<Item = &'a DfsObjectId>,
    ) -> Result<()> {
        self.begin_consistent_read_snapshot()?;
        let result = (|| {
            let mut new_indexes = Vec::new();
            for object_id in object_ids {
                let (existing, rebuild_existing) =
                    read_object_shard_index_for_rebuild(&self.root, object_id)?;
                let Some((shard_id, object)) =
                    self.authoritative_object_placement_with_index(object_id, existing.as_ref())?
                else {
                    if existing.is_some() || rebuild_existing {
                        self.remove_object_shard_index(object_id)?;
                    }
                    continue;
                };
                if existing.as_ref().is_some_and(|index| {
                    index.file_system_id == object.file_system_id
                        && index.path == normalize_path(&object.path)
                        && index.shard_id == shard_id
                }) {
                    continue;
                }
                if existing.is_none() && !rebuild_existing {
                    new_indexes.push((object, shard_id));
                } else {
                    self.write_object_shard_index(&object, &shard_id)?;
                }
            }
            write_new_object_shard_indexes(
                &self.root,
                new_indexes
                    .iter()
                    .map(|(object, shard_id)| (object, shard_id)),
            )
        })();
        self.end_consistent_read_snapshot();
        result
    }

    fn refresh_metadata_shard_durable_view_and_reconcile(
        &mut self,
        shard_id: &DfsMetadataShardId,
        reconcile_unchanged: bool,
    ) -> Result<bool> {
        let generation_before = self.shard_backend(shard_id)?.cached_table_generation();
        let mut affected_object_ids = self
            .shard_backend(shard_id)?
            .export_table_snapshot()
            .tables
            .into_iter()
            .flat_map(|tables| tables.objects)
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        self.shard_backend_mut(shard_id)?
            .refresh_metadata_shard_durable_view(shard_id)?;
        let changed = self.shard_backend(shard_id)?.cached_table_generation() != generation_before;
        if !changed && !reconcile_unchanged {
            return Ok(false);
        }
        affected_object_ids.extend(
            self.shard_backend(shard_id)?
                .export_table_snapshot()
                .tables
                .into_iter()
                .flat_map(|tables| tables.objects)
                .map(|object| object.object_id),
        );
        self.reconcile_object_shard_indexes(affected_object_ids.iter())?;
        Ok(changed)
    }

    fn indexed_shard_id_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsMetadataShardId>> {
        Ok(self.read_object_shard_index(object_id)?.and_then(|index| {
            self.shards
                .contains_key(&index.shard_id)
                .then_some(index.shard_id)
        }))
    }

    fn object_is_visible_from_shard(
        &self,
        object: &DfsObjectMetadata,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        if is_source_eviction_tombstone(object) {
            return Ok(false);
        }
        let selected_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        if selected_shard_id == *shard_id {
            if object.kind == DfsObjectKind::Tombstone {
                return Ok(true);
            }
            // A failed source cleanup can leave the old path's live row behind
            // after a move to a different routed path. Hide that residue only
            // when the derived index is corroborated by the complete physical
            // destination record and current routing. A stale replica index
            // that points at a retired shard cannot veto this selected row.
            if let Some(index) = self.read_object_shard_index(&object.object_id)?
                && index.shard_id != *shard_id
                && self.object_shard_index_is_corroborated(&object.object_id, &index)?
            {
                return Ok(false);
            }
            return Ok(true);
        }

        // A current path-selected record is the namespace authority. A live
        // destination therefore hides the historical source record even when
        // the replica-local object index is stale or absent. If a newly
        // activated target has not received the object yet, preserve the old
        // source fallback until migration installs the routed copy.
        if self
            .shard_backend(&selected_shard_id)?
            .get_object_by_path(&object.file_system_id, &object.path)?
            .is_some_and(|selected| !is_source_eviction_tombstone(&selected))
        {
            return Ok(false);
        }
        Ok(self
            .authoritative_object_placement(&object.object_id)?
            .is_some_and(|(authoritative_shard_id, _)| authoritative_shard_id == *shard_id))
    }

    fn object_is_listable_from_shard(
        &self,
        object: &DfsObjectMetadata,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        self.object_is_visible_from_shard(object, shard_id)
    }

    fn intent_is_listable_from_shard(
        &self,
        intent: &DfsWriteIntentRecord,
        backend: &LocalSobjMetadataBackend,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        if write_intent_status_is_active(&intent.status) {
            return self.active_intent_is_listable_from_shard(intent, shard_id);
        }
        if let Some(object) = backend.get_object(&intent.object_id)? {
            return self.object_is_listable_from_shard(&object, shard_id);
        }
        if let Some(object) = intent.object_metadata.as_ref()
            && object.object_id == intent.object_id
            && object.file_system_id == intent.file_system_id
        {
            return self.object_is_listable_from_shard(object, shard_id);
        }
        if let Some(index) = self.read_object_shard_index(&intent.object_id)?
            && self.shards.contains_key(&index.shard_id)
            && index.shard_id != *shard_id
        {
            return Ok(false);
        }
        Ok(true)
    }

    fn active_intent_is_listable_from_shard(
        &self,
        intent: &DfsWriteIntentRecord,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        let Some(index) = self.read_object_shard_index(&intent.object_id)? else {
            return Ok(true);
        };
        if !self.shards.contains_key(&index.shard_id) || index.shard_id == *shard_id {
            return Ok(true);
        }
        let Some(current_object) = self
            .shard_backend(&index.shard_id)?
            .get_object(&intent.object_id)?
        else {
            return Ok(true);
        };
        if current_object.kind == DfsObjectKind::Tombstone {
            return Ok(true);
        }
        Ok(intent
            .base_version
            .as_ref()
            .is_some_and(|base_version| *base_version == current_object.version)
            || intent.target_version == current_object.version)
    }

    fn require_shard_id_for_object(&self, object_id: &DfsObjectId) -> Result<DfsMetadataShardId> {
        self.shard_id_for_object(object_id)?
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            })
    }

    fn shard_id_for_existing_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsMetadataShardId>> {
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            if backend.get_intent(object_id, intent_id)?.is_some() {
                return Ok(Some(shard_id.clone()));
            }
        }
        Ok(None)
    }

    fn shard_id_for_intent_write(
        &self,
        intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataShardId> {
        if let Some(shard_id) =
            self.shard_id_for_existing_intent(&intent.object_id, &intent.intent_id)?
        {
            return Ok(shard_id);
        }
        self.shard_id_for_object(&intent.object_id)?.ok_or_else(|| {
            MetadataBackendError::UnknownObject {
                object_id: intent.object_id.clone(),
            }
        })
    }

    fn shard_id_for_pending_object_record(
        &self,
        object_id: &DfsObjectId,
        file_system_id: &DfsFileSystemId,
        object: Option<&DfsObjectMetadata>,
    ) -> Result<DfsMetadataShardId> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return Ok(shard_id);
        }
        let Some(object) = object else {
            return Err(MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            });
        };
        if object.object_id != *object_id || object.file_system_id != *file_system_id {
            return Err(MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            });
        }
        self.select_shard_id_for_path(file_system_id, &object.path)
    }

    fn first_hardlink_to_object_matching<P>(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        mut matches: P,
    ) -> Result<Option<DfsObjectMetadata>>
    where
        P: FnMut(&DfsObjectMetadata) -> bool,
    {
        let mut best = None::<DfsObjectMetadata>;
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            walk_metadata_pages(|cursor| {
                let page = backend.hardlinks_to_object_page(
                    file_system_id,
                    target_object_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                for object in page.objects {
                    if !self.object_is_listable_from_shard(&object, shard_id)? || !matches(&object)
                    {
                        continue;
                    }
                    if best
                        .as_ref()
                        .is_none_or(|current| object_page_key(&object) < object_page_key(current))
                    {
                        best = Some(object);
                    }
                }
                Ok(page.next_cursor)
            })?;
        }
        Ok(best)
    }

    pub(crate) fn ensure_file_system_on_all_shards(
        &mut self,
        file_system: &DfsFileSystem,
    ) -> Result<usize> {
        let shard_ids = self.configured_shard_ids_for_file_system(&file_system.file_system_id)?;
        let mut written = 0usize;
        for shard_id in shard_ids {
            let backend = self.shard_backend_mut(&shard_id)?;
            if backend
                .get_file_system(&file_system.file_system_id)?
                .as_ref()
                .is_some_and(|current| {
                    crate::placement::dfs_file_system_semantically_eq(current, file_system)
                })
            {
                continue;
            }
            backend.put_file_system(file_system)?;
            written = written.saturating_add(1);
        }
        Ok(written)
    }

    pub(crate) fn migrate_objects_to_configured_shards(
        &mut self,
        file_system_id: &DfsFileSystemId,
        max_migrations: usize,
    ) -> Result<DfsMetadataShardMigrationReport> {
        let file_system = self.get_file_system(file_system_id)?.ok_or_else(|| {
            MetadataBackendError::UnknownFileSystem {
                file_system_id: file_system_id.clone(),
            }
        })?;
        let mut report = DfsMetadataShardMigrationReport {
            file_system_id: file_system_id.clone(),
            scanned_objects: 0,
            indexed_objects: 0,
            migrated_objects: 0,
            skipped_stale_sources: 0,
            reached_limit: false,
        };
        let shard_ids = self.shard_ids();
        let mut visited_object_ids = BTreeSet::<DfsObjectId>::new();
        let mut completed_migration_steps = 0usize;
        for source_shard_id in shard_ids {
            walk_metadata_pages(|cursor| {
                let page = self
                    .shard_backend(&source_shard_id)?
                    .objects_for_file_system_page(
                        file_system_id,
                        cursor,
                        SHARD_MIGRATION_PAGE_SIZE,
                    )?;
                for object in page.objects {
                    let first_visit = visited_object_ids.insert(object.object_id.clone());
                    let existing_index = self.read_object_shard_index(&object.object_id)?;
                    let needs_physical_source_cleanup = existing_index
                        .as_ref()
                        .is_some_and(|index| index.shard_id != source_shard_id);
                    if !first_visit && !needs_physical_source_cleanup {
                        // A migration can install the target into a shard that
                        // this same bounded walk has not visited yet. Do not
                        // count or re-process that physical target copy, while
                        // still allowing a later indexed source copy to pass
                        // through the fenced cleanup branch below.
                        continue;
                    }
                    if completed_migration_steps >= max_migrations {
                        report.reached_limit = true;
                        return Ok(None);
                    }
                    if first_visit {
                        report.scanned_objects = report.scanned_objects.saturating_add(1);
                    }
                    if let Some(index) = existing_index
                        && index.shard_id != source_shard_id
                    {
                        let target_object = self
                            .shard_backend(&index.shard_id)?
                            .get_object(&object.object_id)?;
                        if let Some(target_object) = target_object
                            && target_object.kind != DfsObjectKind::Tombstone
                            && self.select_shard_id_for_path(
                                &target_object.file_system_id,
                                &target_object.path,
                            )? == index.shard_id
                        {
                            // Crash recovery for the narrow index→source
                            // cleanup window. Target install and exact pin-set
                            // reconciliation happen before the index write.
                            // Once indexed, new pins are legitimate target
                            // authority state, so verify the copied source set
                            // is still present but never remove target extras.
                            let (source_lock, bundle) = self.object_shard_migration_bundle(
                                &source_shard_id,
                                &object.object_id,
                            )?;
                            let target_pins = self
                                .shard_backend(&index.shard_id)?
                                .version_pins_for_object(&object.object_id)?;
                            for source_pin in &bundle.version_pins {
                                if !target_pins
                                    .iter()
                                    .any(|target_pin| target_pin == source_pin)
                                {
                                    return Err(MetadataBackendError::Serde(format!(
                                        "indexed target shard {} is missing durable source pin {} for {}/{} during migration recovery",
                                        index.shard_id.0,
                                        source_pin.pin_id,
                                        source_pin.object_id,
                                        source_pin.version
                                    )));
                                }
                            }
                            let eviction = self
                                .shard_backend_mut(&source_shard_id)?
                                .put_cross_shard_tombstone_if_unchanged_while_locked(
                                    &source_lock,
                                    &bundle.object,
                                    &target_object,
                                    &source_shard_id,
                                    &index.shard_id,
                                    bundle.source_journal_sequence,
                                    None,
                                )?;
                            match eviction {
                                crate::metadata_backend::DfsConditionalSourceEviction::Applied {
                                    ..
                                }
                                | crate::metadata_backend::DfsConditionalSourceEviction::AlreadyApplied => {
                                    completed_migration_steps =
                                        completed_migration_steps.saturating_add(1);
                                    report.migrated_objects =
                                        report.migrated_objects.saturating_add(1);
                                    continue;
                                }
                                crate::metadata_backend::DfsConditionalSourceEviction::JournalAdvanced
                                | crate::metadata_backend::DfsConditionalSourceEviction::SourceChanged => {
                                    return Err(MetadataBackendError::Serde(format!(
                                        "source shard {} changed while recovering indexed migration of object {}",
                                        source_shard_id.0, object.object_id
                                    )));
                                }
                            }
                        }
                        report.skipped_stale_sources =
                            report.skipped_stale_sources.saturating_add(1);
                        continue;
                    }
                    let target_shard_id =
                        self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
                    if source_shard_id == target_shard_id {
                        let expected_path = normalize_path(&object.path);
                        let needs_index = self
                            .read_object_shard_index(&object.object_id)?
                            .is_none_or(|index| {
                                index.shard_id != target_shard_id || index.path != expected_path
                            });
                        if needs_index {
                            self.write_object_shard_index(&object, &target_shard_id)?;
                            report.indexed_objects = report.indexed_objects.saturating_add(1);
                        }
                        continue;
                    }
                    if let Some(target_object) = self
                        .shard_backend(&target_shard_id)?
                        .get_object(&object.object_id)?
                        && target_object.kind != DfsObjectKind::Tombstone
                    {
                        let target_object_shard_id = self.select_shard_id_for_path(
                            &target_object.file_system_id,
                            &target_object.path,
                        )?;
                        if target_object_shard_id == target_shard_id {
                            let (source_lock, bundle) = self.object_shard_migration_bundle(
                                &source_shard_id,
                                &object.object_id,
                            )?;
                            let target_pins = self
                                .shard_backend(&target_shard_id)?
                                .version_pins_for_object(&object.object_id)?;
                            for source_pin in &bundle.version_pins {
                                if !target_pins
                                    .iter()
                                    .any(|target_pin| target_pin == source_pin)
                                {
                                    return Err(MetadataBackendError::Serde(format!(
                                        "existing migration target shard {} is missing durable source pin {} for {}/{}",
                                        target_shard_id.0,
                                        source_pin.pin_id,
                                        source_pin.object_id,
                                        source_pin.version
                                    )));
                                }
                            }
                            self.write_object_shard_index(&target_object, &target_shard_id)?;
                            report.indexed_objects = report.indexed_objects.saturating_add(1);
                            let eviction = self
                                .shard_backend_mut(&source_shard_id)?
                                .put_cross_shard_tombstone_if_unchanged_while_locked(
                                    &source_lock,
                                    &bundle.object,
                                    &target_object,
                                    &source_shard_id,
                                    &target_shard_id,
                                    bundle.source_journal_sequence,
                                    None,
                                )?;
                            if !matches!(
                                eviction,
                                crate::metadata_backend::DfsConditionalSourceEviction::Applied {
                                    ..
                                } | crate::metadata_backend::DfsConditionalSourceEviction::AlreadyApplied
                            ) {
                                return Err(MetadataBackendError::Serde(format!(
                                    "source shard {} changed while completing installed migration of object {}",
                                    source_shard_id.0, object.object_id
                                )));
                            }
                            completed_migration_steps = completed_migration_steps.saturating_add(1);
                            report.skipped_stale_sources =
                                report.skipped_stale_sources.saturating_add(1);
                            continue;
                        }
                    }
                    let (source_lock, bundle) =
                        self.object_shard_migration_bundle(&source_shard_id, &object.object_id)?;
                    self.install_object_shard_migration_bundle(
                        &file_system,
                        &bundle,
                        &target_shard_id,
                    )?;
                    self.write_object_shard_index(&bundle.object, &target_shard_id)?;
                    let eviction = self
                        .shard_backend_mut(&source_shard_id)?
                        .put_cross_shard_tombstone_if_unchanged_while_locked(
                            &source_lock,
                            &bundle.object,
                            &bundle.object,
                            &source_shard_id,
                            &target_shard_id,
                            bundle.source_journal_sequence,
                            None,
                        )?;
                    if !matches!(
                        eviction,
                        crate::metadata_backend::DfsConditionalSourceEviction::Applied { .. }
                            | crate::metadata_backend::DfsConditionalSourceEviction::AlreadyApplied
                    ) {
                        return Err(MetadataBackendError::Serde(format!(
                            "source shard {} changed while migrating object {} to shard {}",
                            source_shard_id.0, bundle.object.object_id, target_shard_id.0
                        )));
                    }
                    completed_migration_steps = completed_migration_steps.saturating_add(1);
                    report.migrated_objects = report.migrated_objects.saturating_add(1);
                }
                Ok(page.next_cursor)
            })?;
            if report.reached_limit {
                return Ok(report);
            }
        }
        // A complete scan proves that every live object is now at the active
        // route (or has durable source-eviction evidence). Retire historical
        // routing generations so steady-state negative lookups touch exactly
        // one shard.
        self.retire_routing_fallbacks_for_file_system(file_system_id)?;
        Ok(report)
    }

    fn object_shard_migration_bundle(
        &mut self,
        source_shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
    ) -> Result<(MetadataWriteLock, DfsObjectShardMigrationBundle)> {
        self.shard_backend_mut(source_shard_id)?
            .lock_and_capture_object_shard_migration_bundle(
                object_id,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )
    }

    fn install_object_shard_migration_bundle(
        &mut self,
        file_system: &DfsFileSystem,
        bundle: &DfsObjectShardMigrationBundle,
        target_shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        install_object_shard_migration_bundle_into_backend(
            self.shard_backend_mut(target_shard_id)?,
            file_system,
            bundle,
            target_shard_id,
            OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )
    }

    fn put_cross_shard_tombstone(
        &mut self,
        object: &DfsObjectMetadata,
        source_shard_id: &DfsMetadataShardId,
        target_shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        self.require_unpinned_source_for_unmigrated_move(
            source_shard_id,
            target_shard_id,
            &object.object_id,
            "unfenced source eviction",
        )?;
        put_cross_shard_tombstone_into_backend(
            self.shard_backend_mut(source_shard_id)?,
            object,
            source_shard_id,
            target_shard_id,
        )
        .map(|_| ())
    }

    fn require_unpinned_source_for_unmigrated_move(
        &self,
        source_shard_id: &DfsMetadataShardId,
        target_shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
        operation: &str,
    ) -> Result<()> {
        if source_shard_id == target_shard_id {
            return Ok(());
        }
        let pins = self
            .shard_backend(source_shard_id)?
            .version_pins_for_object(object_id)?;
        if let Some(pin) = pins.first() {
            return Err(MetadataBackendError::Serde(format!(
                "{operation} cannot move pinned object {object_id} from shard {} to shard {} without a fenced full-family transfer; pin {} protects version {}",
                source_shard_id.0, target_shard_id.0, pin.pin_id, pin.version
            )));
        }
        Ok(())
    }

    fn put_object_routed(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry> {
        let object = normalized_object_metadata(object);
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        let source_shard_id = self.shard_id_for_object(&object.object_id)?;
        if let Some(source_shard_id) = source_shard_id.as_ref() {
            self.require_unpinned_source_for_unmigrated_move(
                source_shard_id,
                &target_shard_id,
                &object.object_id,
                "routed object update",
            )?;
        }
        let entry = self
            .shard_backend_mut(&target_shard_id)?
            .put_object(&object)?;
        self.write_object_shard_index(&object, &target_shard_id)?;
        if let Some(source_shard_id) = source_shard_id {
            self.put_cross_shard_tombstone(&object, &source_shard_id, &target_shard_id)?;
        }
        Ok(entry)
    }
}

#[cfg(test)]
mod lazy_replica_slot_tests {
    use super::*;
    use crate::namespace::{directory_object, distributed_sifs};
    use sinomo_core::DfsObjectPolicy;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn configs(local_node_id: &NodeId) -> Vec<DfsMetadataShardConfig> {
        vec![
            DfsMetadataShardConfig {
                file_system_id: crate::sifs_file_system_id(),
                shard_id: "root".to_string(),
                primary_node_id: local_node_id.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: crate::sifs_file_system_id(),
                shard_id: "services".to_string(),
                primary_node_id: node("lazy-slot-remote-primary"),
                replica_node_ids: vec![local_node_id.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: crate::SIFS_SERVICES_PATH.to_string(),
                },
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
        ]
    }

    #[test]
    fn resident_shard_lease_moves_only_backend_ownership() {
        assert!(
            std::mem::size_of::<ResidentLocalMetadataShardLease>() <= 256,
            "resident shard lease grew to {} bytes",
            std::mem::size_of::<ResidentLocalMetadataShardLease>()
        );
    }

    #[test]
    fn replica_repair_router_defers_non_primary_shard_until_first_access() {
        let root = sinomo_testdir::TestDir::new("dfs-lazy-replica-shard-slot");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("lazy-slot-local");
        let root_shard_id = DfsMetadataShardId::new("root");
        let replica_shard_id = DfsMetadataShardId::new("services");
        let mut backend = LocalShardedMetadataBackend::new_with_replica_repair(
            &metadata_root,
            configs(&local_node_id),
            &local_node_id,
        )
        .expect("open node-aware sharded backend");

        assert_eq!(
            backend.opened_shard_ids(),
            BTreeSet::from([root_shard_id.clone()]),
            "only the local authoritative primary should be decoded eagerly"
        );
        backend
            .shard_backend_mut(&replica_shard_id)
            .expect("open lazy replica shard on first access");
        assert_eq!(
            backend.opened_shard_ids(),
            BTreeSet::from([root_shard_id, replica_shard_id])
        );
    }

    #[test]
    fn strict_and_inspection_routers_keep_eager_open_contract() {
        let root = sinomo_testdir::TestDir::new("dfs-eager-compatible-shard-slots");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("eager-slot-local");
        let expected = BTreeSet::from([
            DfsMetadataShardId::new("root"),
            DfsMetadataShardId::new("services"),
        ]);

        let strict = LocalShardedMetadataBackend::new(&metadata_root, configs(&local_node_id))
            .expect("open strict sharded backend");
        assert_eq!(strict.opened_shard_ids(), expected);
        drop(strict);

        let inspection = LocalShardedMetadataBackend::open_for_inspection(
            &metadata_root,
            configs(&local_node_id),
        )
        .expect("open inspection sharded backend");
        assert_eq!(inspection.opened_shard_ids(), expected);
    }

    #[test]
    fn lazy_slots_preserve_distinct_backend_instances_between_routers() {
        let root = sinomo_testdir::TestDir::new("dfs-distinct-lazy-shard-slots");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("distinct-slot-local");
        let replica_shard_id = DfsMetadataShardId::new("services");
        let first = LocalShardedMetadataBackend::new_with_replica_repair(
            &metadata_root,
            configs(&local_node_id),
            &local_node_id,
        )
        .expect("open first router");
        let second = LocalShardedMetadataBackend::new_with_replica_repair(
            &metadata_root,
            configs(&local_node_id),
            &local_node_id,
        )
        .expect("open second router");

        let first_backend = first
            .shard_backend(&replica_shard_id)
            .expect("open first replica slot");
        let second_backend = second
            .shard_backend(&replica_shard_id)
            .expect("open second replica slot");
        assert!(
            !std::ptr::eq(first_backend, second_backend),
            "lazy slots must not merge instance-local authority capabilities"
        );
    }

    #[test]
    fn resident_handles_move_open_backends_and_open_deferred_shards_once() {
        let root = sinomo_testdir::TestDir::new("dfs-resident-shard-open-once");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("resident-open-local");
        let file_system_id = crate::sifs_file_system_id();
        let root_shard_id = DfsMetadataShardId::new("root");
        let replica_shard_id = DfsMetadataShardId::new("services");
        let router = LocalShardedMetadataBackend::new_with_replica_repair(
            &metadata_root,
            configs(&local_node_id),
            &local_node_id,
        )
        .expect("open node-aware sharded backend");
        crate::metadata_backend::reset_metadata_backend_open_calls();

        let resident = router.into_resident();
        let root_handle = resident
            .scope_shard(&file_system_id, &root_shard_id)
            .expect("scope moved primary");
        assert!(root_handle.is_open());
        root_handle
            .with_backend(|backend| backend.metadata_summary().map(|_| ()))
            .expect("read moved primary backend");
        assert_eq!(
            crate::metadata_backend::metadata_backend_open_calls(),
            0,
            "converting and borrowing an eager shard must not reopen it"
        );

        let replica_handle = resident
            .scope_shard(&file_system_id, &replica_shard_id)
            .expect("scope deferred replica");
        assert!(!replica_handle.is_open());
        replica_handle
            .with_backend(|backend| backend.metadata_summary().map(|_| ()))
            .expect("open deferred replica once");
        assert_eq!(crate::metadata_backend::metadata_backend_open_calls(), 1);
        replica_handle
            .clone()
            .with_backend(|backend| backend.metadata_summary().map(|_| ()))
            .expect("reuse deferred replica instance");
        assert_eq!(
            crate::metadata_backend::metadata_backend_open_calls(),
            1,
            "cloned resident handles must reuse the first lazy open"
        );
    }

    #[test]
    fn resident_topology_match_requires_exact_root_and_config_generation() {
        let root = sinomo_testdir::TestDir::new("dfs-resident-topology-identity");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("resident-topology-local");
        let shards = configs(&local_node_id);
        let resident = LocalShardedMetadataBackend::new(&metadata_root, shards.clone())
            .expect("open resident topology")
            .into_resident();

        assert!(
            resident
                .matches_topology(&metadata_root, &shards)
                .expect("matching topology")
        );
        assert!(
            !resident
                .matches_topology(&root.path().join("other-metadata"), &shards)
                .expect("mismatched root")
        );
        let mut advanced = shards.clone();
        advanced[0].epoch = advanced[0].epoch.saturating_add(1);
        assert!(
            !resident
                .matches_topology(&metadata_root, &advanced)
                .expect("mismatched epoch")
        );
        assert!(
            !resident
                .matches_topology(&metadata_root, &shards[1..])
                .expect("missing shard")
        );
    }

    #[test]
    fn bounded_status_does_not_hydrate_a_never_opened_replica() {
        let root = sinomo_testdir::TestDir::new("dfs-bounded-status-unhydrated-replica");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("bounded-status-replica-local");
        let mut shard = configs(&local_node_id)[0].clone();
        shard.primary_node_id = node("bounded-status-replica-primary");
        shard.replica_node_ids = vec![local_node_id.clone()];
        let resident = LocalShardedMetadataBackend::new_with_replica_repair(
            &metadata_root,
            vec![shard.clone()],
            &local_node_id,
        )
        .expect("open deferred resident replica")
        .into_resident();
        crate::metadata_backend::reset_metadata_backend_open_calls();

        let snapshot = resident
            .bounded_status_snapshot()
            .expect("a never-hydrated shard returns bounded partial status");
        let expected_omission =
            DfsOperatorStatusOmission::non_primary_resident_metadata_not_hydrated(
                shard.file_system_id.clone(),
                DfsMetadataShardId::new(&shard.shard_id),
            );
        assert_eq!(snapshot.bounded_omissions, vec![expected_omission.clone()]);
        assert_eq!(
            snapshot.incomplete_reasons,
            vec![expected_omission.legacy_reason()]
        );
        assert!(
            expected_omission
                .omitted_metrics()
                .contains(&crate::DfsOperatorStatusMetric::MetadataShardReplayCursor)
        );
        assert!(
            !expected_omission
                .omitted_metrics()
                .contains(&crate::DfsOperatorStatusMetric::OpenConflictCount),
            "an unopened non-primary shard must not weaken primary-owned debt gates"
        );
        assert_eq!(snapshot.metadata_shards.len(), 1);
        assert_eq!(snapshot.metadata_shards[0].shard_id.0, "root");
        assert_eq!(
            crate::metadata_backend::metadata_backend_open_calls(),
            0,
            "bounded status must report unavailable instead of hydrating metadata"
        );
    }

    #[test]
    fn bounded_status_keeps_an_unopened_local_primary_blocking() {
        let root = sinomo_testdir::TestDir::new("dfs-bounded-status-unhydrated-primary");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("bounded-status-primary-local");
        let shard = configs(&local_node_id)[0].clone();
        let shard_id = DfsMetadataShardId::new(&shard.shard_id);
        let resident = LocalShardedMetadataBackend::new_with_replica_repair(
            &metadata_root,
            vec![shard],
            &local_node_id,
        )
        .expect("open resident primary")
        .into_resident();
        let slot = resident
            .inner
            .shards
            .get(&shard_id)
            .expect("resident primary slot");
        *slot.state.lock().unwrap_or_else(|error| error.into_inner()) =
            ResidentLocalMetadataShardState::Unopened;
        slot.operator_status
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot = None;
        crate::metadata_backend::reset_metadata_backend_open_calls();

        let snapshot = resident
            .bounded_status_snapshot()
            .expect("an unopened local primary returns blocking partial status");
        assert!(snapshot.bounded_omissions.is_empty());
        assert_eq!(snapshot.incomplete_reasons.len(), 1);
        assert!(snapshot.incomplete_reasons[0].contains("status unavailable"));
        assert!(snapshot.incomplete_reasons[0].contains("is not hydrated for bounded status"));
        assert_eq!(snapshot.metadata_shards.len(), 1);
        assert_eq!(snapshot.metadata_shards[0].shard_id, shard_id);
        assert_eq!(
            crate::metadata_backend::metadata_backend_open_calls(),
            0,
            "bounded status must fail closed without hydrating a local primary"
        );
    }

    #[test]
    fn unhydrated_status_is_omitted_only_for_a_non_primary_shard() {
        let local_node_id = node("bounded-status-role-local");
        let shard_id = DfsMetadataShardId::new("root");
        let local_primary = configs(&local_node_id)[0].clone();
        assert_eq!(
            non_primary_unhydrated_status_omission(Some(&local_node_id), &local_primary, &shard_id,),
            None,
            "an unopened local primary must remain a blocking incomplete status"
        );

        let mut non_primary = local_primary;
        non_primary.primary_node_id = node("bounded-status-role-primary");
        assert!(matches!(
            non_primary_unhydrated_status_omission(
                Some(&local_node_id),
                &non_primary,
                &shard_id,
            ),
            Some(DfsOperatorStatusOmission::NonPrimaryResidentMetadataNotHydrated {
                file_system_id,
                shard_id: omitted_shard_id,
            }) if file_system_id == non_primary.file_system_id && omitted_shard_id == shard_id
        ));
        assert_eq!(
            non_primary_unhydrated_status_omission(None, &non_primary, &shard_id),
            None,
            "missing local identity must fail closed"
        );
    }

    #[test]
    fn bounded_status_serves_last_complete_generation_while_shard_is_checked_out() {
        let root = sinomo_testdir::TestDir::new("dfs-bounded-status-checked-out-shard");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("bounded-status-checked-out-local");
        let file_system = distributed_sifs(local_node_id.clone());
        let replica_node_id = node("bounded-status-checked-out-replica");
        let mut shard = configs(&local_node_id)[0].clone();
        shard.replica_node_ids = vec![replica_node_id.clone()];
        let mut backend = LocalShardedMetadataBackend::new(&metadata_root, vec![shard])
            .expect("open status backend");
        backend
            .put_file_system(&file_system)
            .expect("seed status filesystem");
        let resident = backend.into_resident();
        let handle = resident
            .scope_shard(
                &file_system.file_system_id,
                &DfsMetadataShardId::new("root"),
            )
            .expect("scope root shard");
        let initial = resident
            .bounded_status_snapshot()
            .expect("initial bounded status");
        assert_eq!(initial.summary.object_count, 0);

        reset_resident_metadata_status_durable_cursor_reads();
        let mut lease = handle.checkout().expect("hold shard mutation lease");
        let object = directory_object("/users/status-while-mutating", local_node_id);
        lease.put_object(&object).expect("mutate checked-out shard");
        lease
            .put_metadata_replication_cursor(&replica_node_id, &DfsMetadataShardId::new("root"), 1)
            .expect("advance resident replica cursor");
        let started = Instant::now();
        let during = resident
            .bounded_status_snapshot()
            .expect("serve prior complete generation");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "bounded status must never wait for the checked-out shard"
        );
        assert_eq!(during.summary.object_count, 0);

        drop(lease);
        assert_eq!(
            resident_metadata_status_durable_cursor_reads(),
            0,
            "mutating and returning a resident shard lease must not reread durable cursor sidecars"
        );
        let after = resident
            .bounded_status_snapshot()
            .expect("status after mutation release");
        assert_eq!(
            after.summary.object_count, 1,
            "the completed mutation generation must publish on lease return"
        );
        assert_eq!(
            after.metadata_shards[0].replica_lag[0].replicated_through_sequence, 1,
            "cursor operations must advance the in-memory resident projection"
        );
    }

    #[test]
    fn bounded_status_matches_exact_scan_without_running_a_full_scan() {
        let root = sinomo_testdir::TestDir::new("dfs-bounded-status-stable-topology");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("bounded-status-stable-local");
        let file_system = distributed_sifs(local_node_id.clone());
        let shard = configs(&local_node_id)[0].clone();
        let mut backend = LocalShardedMetadataBackend::new(&metadata_root, vec![shard])
            .expect("open stable status backend");
        backend
            .put_file_system(&file_system)
            .expect("seed stable filesystem");
        backend
            .put_object(&directory_object("/users/stable-status", local_node_id))
            .expect("seed stable object");
        let exact = backend.metadata_summary().expect("exact stable summary");
        let resident = backend.into_resident();

        crate::metadata_summary::reset_full_metadata_summary_scans();
        reset_resident_metadata_status_durable_cursor_reads();
        let bounded = resident
            .bounded_status_snapshot()
            .expect("bounded stable summary");
        resident
            .bounded_status_snapshot()
            .expect("repeat bounded stable summary");
        assert_eq!(bounded.summary, exact);
        assert_eq!(
            crate::metadata_summary::full_metadata_summary_scans(),
            0,
            "bounded status must not invoke the materialized-table scanner"
        );
        assert_eq!(
            resident_metadata_status_durable_cursor_reads(),
            0,
            "status must reuse the published cursor projection instead of rereading durable sidecars"
        );
    }

    #[test]
    fn historical_routing_preserves_a_complete_zero_lock_proof() {
        let root = sinomo_testdir::TestDir::new("dfs-bounded-status-routing-zero-locks");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("bounded-status-routing-zero-local");
        let file_system = distributed_sifs(local_node_id.clone());
        let root_shard = configs(&local_node_id)[0].clone();
        {
            let mut initial = LocalShardedMetadataBackend::new(&metadata_root, vec![root_shard])
                .expect("open predecessor topology");
            initial
                .put_file_system(&file_system)
                .expect("seed predecessor filesystem");
            initial
                .put_object(&directory_object(
                    "/services/routing-zero-locks",
                    local_node_id.clone(),
                ))
                .expect("seed predecessor-routed object");
        }

        let resident = LocalShardedMetadataBackend::new(&metadata_root, configs(&local_node_id))
            .expect("open successor topology")
            .into_resident();
        crate::metadata_summary::reset_full_metadata_summary_scans();
        let snapshot = resident
            .bounded_status_snapshot()
            .expect("bounded historical status");
        let historical = snapshot
            .bounded_omissions
            .iter()
            .find(|omission| {
                matches!(
                    omission,
                    DfsOperatorStatusOmission::HistoricalRoutingTopology { .. }
                )
            })
            .expect("historical routing omission");

        assert_eq!(snapshot.summary.cross_shard_namespace_lock_count, 0);
        assert!(
            !historical
                .omitted_metrics()
                .contains(&crate::DfsOperatorStatusMetric::CrossShardNamespaceLockCount),
            "the complete union-empty projection must retain an exact zero lock proof"
        );
        assert_eq!(
            crate::metadata_summary::full_metadata_summary_scans(),
            0,
            "the zero proof must use resident shard summaries only"
        );
    }

    #[test]
    fn historical_routing_zero_lock_proof_requires_every_shard_summary() {
        let root = sinomo_testdir::TestDir::new("dfs-bounded-status-routing-zero-incomplete");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("bounded-status-routing-zero-incomplete-local");
        let root_shard = configs(&local_node_id)[0].clone();
        drop(
            LocalShardedMetadataBackend::new(&metadata_root, vec![root_shard])
                .expect("open predecessor topology"),
        );

        let resident = LocalShardedMetadataBackend::new_with_replica_repair(
            &metadata_root,
            configs(&local_node_id),
            &local_node_id,
        )
        .expect("open successor topology with deferred replica")
        .into_resident();
        crate::metadata_backend::reset_metadata_backend_open_calls();
        let snapshot = resident
            .bounded_status_snapshot()
            .expect("bounded incomplete historical status");
        let historical = snapshot
            .bounded_omissions
            .iter()
            .find(|omission| {
                matches!(
                    omission,
                    DfsOperatorStatusOmission::HistoricalRoutingTopology { .. }
                )
            })
            .expect("historical routing omission");

        assert!(
            historical
                .omitted_metrics()
                .contains(&crate::DfsOperatorStatusMetric::CrossShardNamespaceLockCount),
            "an unhydrated shard must keep the lock projection unknown"
        );
        assert!(snapshot.bounded_omissions.iter().any(|omission| matches!(
            omission,
            DfsOperatorStatusOmission::NonPrimaryResidentMetadataNotHydrated { .. }
        )));
        assert_eq!(
            crate::metadata_backend::metadata_backend_open_calls(),
            0,
            "zero-proof status must not hydrate a missing shard summary"
        );
    }

    #[test]
    fn bounded_status_is_explicitly_unavailable_during_routing_migration() {
        let root = sinomo_testdir::TestDir::new("dfs-bounded-status-routing-migration");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("bounded-status-migration-local");
        let file_system = distributed_sifs(local_node_id.clone());
        let root_shard = configs(&local_node_id)[0].clone();
        {
            let mut initial = LocalShardedMetadataBackend::new(&metadata_root, vec![root_shard])
                .expect("open predecessor topology");
            initial
                .put_file_system(&file_system)
                .expect("seed predecessor filesystem");
            initial
                .put_object(&directory_object(
                    "/services/migrated-status-object",
                    local_node_id.clone(),
                ))
                .expect("seed predecessor-routed object");
        }
        let mut migrating =
            LocalShardedMetadataBackend::new(&metadata_root, configs(&local_node_id))
                .expect("open successor topology");
        let error = migrating
            .metadata_status_summaries()
            .expect_err("routing handoff counts are not exact without durable probes");
        let MetadataBackendError::StatusOmitted { omission } = error else {
            panic!("routing migration must return a typed bounded omission");
        };
        assert!(matches!(
            &omission,
            DfsOperatorStatusOmission::HistoricalRoutingTopology {
                generation_count: 1,
                ..
            }
        ));
        assert!(
            omission
                .omitted_metrics()
                .contains(&crate::DfsOperatorStatusMetric::OpenConflictCount)
        );
        assert!(
            !omission
                .omitted_metrics()
                .contains(&crate::DfsOperatorStatusMetric::CrossShardNamespaceLockCount),
            "a complete cold aggregate also preserves the exact zero lock proof"
        );

        migrating
            .migrate_objects_to_configured_shards(&file_system.file_system_id, 16)
            .expect("migrate predecessor topology");
        let exact = migrating
            .metadata_summary()
            .expect("exact post-migration summary");
        let (bounded, _) = migrating
            .metadata_status_summaries()
            .expect("bounded status resumes after migration proves a stable topology");
        assert_eq!(
            bounded, exact,
            "source-eviction tombstones must not inflate bounded post-migration status"
        );
    }

    #[test]
    fn historical_routing_omits_a_job_only_cross_shard_lock_projection() {
        let root = sinomo_testdir::TestDir::new("dfs-bounded-status-routing-lock-only");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("bounded-status-routing-lock-local");
        let file_system = distributed_sifs(local_node_id.clone());
        let root_shard = configs(&local_node_id)[0].clone();
        {
            let mut initial = LocalShardedMetadataBackend::new(&metadata_root, vec![root_shard])
                .expect("open predecessor topology");
            initial
                .put_file_system(&file_system)
                .expect("seed predecessor filesystem");
            initial
                .put_namespace_job(&DfsNamespaceJobRecord {
                    job_id: "job-routing-lock-only".to_string(),
                    file_system_id: file_system.file_system_id.clone(),
                    origin: None,
                    kind: DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
                    status: DfsNamespaceJobStatus::Running,
                    root_path: "/services/lock-only".to_string(),
                    target_path: Some("/users/lock-only".to_string()),
                    pending_frames: Vec::new(),
                    processed_object_count: 0,
                    created_at: time::OffsetDateTime::UNIX_EPOCH,
                    updated_at: time::OffsetDateTime::UNIX_EPOCH,
                    completed_at: None,
                    failure_reason: None,
                })
                .expect("seed cross-shard namespace lock");
        }

        let migrating = LocalShardedMetadataBackend::new(&metadata_root, configs(&local_node_id))
            .expect("open successor topology");
        let error = migrating
            .metadata_status_summaries()
            .expect_err("historical lock-only status must fail closed");
        let MetadataBackendError::StatusOmitted { omission } = &error else {
            panic!("historical lock-only status must return a typed omission");
        };
        assert!(matches!(
            omission,
            DfsOperatorStatusOmission::HistoricalRoutingTopology {
                generation_count: 1,
                ..
            }
        ));
        assert!(
            omission
                .omitted_metrics()
                .contains(&crate::DfsOperatorStatusMetric::CrossShardNamespaceLockCount),
            "a positive cold aggregate under historical routing must remain unknown"
        );

        let snapshot = migrating
            .into_resident()
            .bounded_status_snapshot()
            .expect("bounded resident historical lock status");
        let historical = snapshot
            .bounded_omissions
            .iter()
            .find(|omission| {
                matches!(
                    omission,
                    DfsOperatorStatusOmission::HistoricalRoutingTopology { .. }
                )
            })
            .expect("historical routing omission");
        assert!(
            historical
                .omitted_metrics()
                .contains(&crate::DfsOperatorStatusMetric::CrossShardNamespaceLockCount),
            "a positive physical sum under historical routing cannot expose an exact effective count"
        );
    }

    #[test]
    fn resident_handles_share_instance_local_handoff_authority() {
        let root = sinomo_testdir::TestDir::new("dfs-resident-shard-handoff-authority");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("resident-handoff-local");
        let file_system_id = crate::sifs_file_system_id();
        let shard_id = DfsMetadataShardId::new("root");
        let mut shard = configs(&local_node_id)
            .into_iter()
            .find(|config| config.shard_id == shard_id.0)
            .expect("root config");
        let resident = LocalShardedMetadataBackend::new(&metadata_root, vec![shard.clone()])
            .expect("open sharded backend")
            .into_resident();
        let first = resident
            .scope_shard(&file_system_id, &shard_id)
            .expect("scope first handle");
        let second = first.clone();

        first
            .with_backend(|backend| {
                backend.freeze_metadata_shard_for_membership_handoff(
                    &shard_id,
                    "resident-handoff",
                    shard.epoch,
                )?;
                shard.epoch += 1;
                backend.clear_metadata_shard_membership_handoff_freeze(
                    &shard_id,
                    "resident-handoff",
                    &shard,
                )?;
                Ok(())
            })
            .expect("bind activated handoff to resident instance");
        assert!(
            !second
                .with_backend(|backend| {
                    backend.metadata_shard_authoritative_writes_frozen(&shard_id)
                })
                .expect("observe authority through cloned handle"),
            "every handle must observe the authority bound to the moved instance"
        );
    }

    #[test]
    fn resident_global_checkout_returns_same_instances_and_router_updates() {
        let root = sinomo_testdir::TestDir::new("dfs-resident-global-checkout");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("resident-global-local");
        let file_system_id = crate::sifs_file_system_id();
        let shard_id = DfsMetadataShardId::new("root");
        let shard = configs(&local_node_id)
            .into_iter()
            .find(|config| config.shard_id == shard_id.0)
            .expect("root config");
        let resident = LocalShardedMetadataBackend::new(&metadata_root, vec![shard.clone()])
            .expect("open sharded backend")
            .into_resident();
        let handle = resident
            .scope_shard(&file_system_id, &shard_id)
            .expect("scope resident shard");
        crate::metadata_backend::reset_metadata_backend_open_calls();

        {
            let mut global = resident.checkout_global().expect("check out global router");
            global
                .freeze_metadata_shard_for_membership_handoff(
                    &shard_id,
                    "resident-global-handoff",
                    shard.epoch,
                )
                .expect("freeze resident shard");
            let mut active = shard.clone();
            active.epoch += 1;
            global
                .clear_metadata_shard_membership_handoff_freeze(
                    &shard_id,
                    "resident-global-handoff",
                    &active,
                )
                .expect("activate resident shard");
        }
        assert_eq!(
            crate::metadata_backend::metadata_backend_open_calls(),
            0,
            "global checkout must reconstruct the router from resident instances"
        );

        assert_eq!(
            resident
                .shard_config(&shard_id)
                .expect("returned router config")
                .epoch,
            shard.epoch + 1,
            "global checkout must return router configuration updates"
        );
        handle
            .with_backend(|backend| {
                assert!(!backend.metadata_shard_authoritative_writes_frozen(&shard_id)?);
                Ok(())
            })
            .expect("returned exact backend authority");
    }

    #[test]
    fn resident_backend_serializes_one_shard_without_blocking_another() {
        let root = sinomo_testdir::TestDir::new("dfs-resident-disjoint-shard-locks");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("resident-lock-local");
        let file_system_id = crate::sifs_file_system_id();
        let resident = LocalShardedMetadataBackend::new(&metadata_root, configs(&local_node_id))
            .expect("open strict sharded backend")
            .into_resident();
        let root_handle = resident
            .scope_shard(&file_system_id, &DfsMetadataShardId::new("root"))
            .expect("scope root shard");
        let services_handle = resident
            .scope_shard(&file_system_id, &DfsMetadataShardId::new("services"))
            .expect("scope services shard");
        let root_guard = root_handle.checkout().expect("check out root shard");

        let (services_tx, services_rx) = mpsc::channel();
        let services_thread = std::thread::spawn(move || {
            let _guard = services_handle
                .checkout()
                .expect("check out disjoint shard");
            services_tx.send(()).expect("signal disjoint lock");
        });
        services_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("disjoint shard must not wait for root shard");
        services_thread.join().expect("disjoint lock thread");

        let (same_tx, same_rx) = mpsc::channel();
        let same_handle = root_handle.clone();
        let same_thread = std::thread::spawn(move || {
            let _guard = same_handle
                .checkout()
                .expect("check out same shard after release");
            same_tx.send(()).expect("signal same-shard lock");
        });
        assert!(
            same_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "same-shard handle must remain serialized"
        );
        drop(root_guard);
        same_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("same shard must advance after release");
        same_thread.join().expect("same-shard lock thread");
    }
}

impl DfsMetadataBackend for LocalShardedMetadataBackend {
    fn metadata_shard_id_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsMetadataShardId>> {
        self.select_shard_id_for_path(file_system_id, path)
            .map(Some)
    }

    fn begin_consistent_read_snapshot(&mut self) -> Result<()> {
        let shard_ids = self.shards.keys().cloned().collect::<Vec<_>>();
        self.begin_consistent_read_snapshot_for_shards(&shard_ids)
    }

    fn end_consistent_read_snapshot(&mut self) {
        let shard_ids = self.shards.keys().cloned().collect::<Vec<_>>();
        self.end_consistent_read_snapshot_for_shards(&shard_ids);
    }

    fn begin_consistent_read_snapshot_for_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<()> {
        let shard_ids = self.consistent_read_shard_ids_for_path(file_system_id, path)?;
        self.begin_consistent_read_snapshot_for_shards(&shard_ids)
    }

    fn end_consistent_read_snapshot_for_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) {
        let Ok(shard_ids) = self.consistent_read_shard_ids_for_path(file_system_id, path) else {
            debug_assert!(false, "path-scoped metadata read lost its routed shard");
            return;
        };
        self.end_consistent_read_snapshot_for_shards(&shard_ids);
    }

    fn put_file_system(&mut self, file_system: &DfsFileSystem) -> Result<DfsMetadataJournalEntry> {
        let shard_ids = self.configured_shard_ids_for_file_system(&file_system.file_system_id)?;
        let mut last_entry = None;
        for shard_id in shard_ids {
            last_entry = Some(
                self.shard_backend_mut(&shard_id)?
                    .put_file_system(file_system)?,
            );
        }
        last_entry.ok_or_else(|| MetadataBackendError::UnknownFileSystem {
            file_system_id: file_system.file_system_id.clone(),
        })
    }

    fn ensure_file_system(
        &mut self,
        file_system: &DfsFileSystem,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let shard_ids = self.configured_shard_ids_for_file_system(&file_system.file_system_id)?;
        let mut last_entry = None;
        for shard_id in shard_ids {
            let backend = self.shard_backend_mut(&shard_id)?;
            if backend
                .get_file_system(&file_system.file_system_id)?
                .as_ref()
                .is_some_and(|current| {
                    crate::placement::dfs_file_system_semantically_eq(current, file_system)
                })
            {
                continue;
            }
            last_entry = Some(backend.put_file_system(file_system)?);
        }
        Ok(last_entry)
    }

    fn ensure_file_system_on_shard(
        &mut self,
        shard_id: &DfsMetadataShardId,
        file_system: &DfsFileSystem,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        self.shard_backend_mut(shard_id)?
            .ensure_file_system(file_system)
    }

    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>> {
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(file_system) = backend.get_file_system(file_system_id)? {
                return Ok(Some(file_system));
            }
        }
        Ok(None)
    }

    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>> {
        let mut file_systems = BTreeMap::<DfsFileSystemId, DfsFileSystem>::new();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            for file_system in backend.list_file_systems()? {
                file_systems.insert(file_system.file_system_id.clone(), file_system);
            }
        }
        Ok(file_systems.into_values().collect())
    }

    fn put_object(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry> {
        self.put_object_routed(object)
    }

    fn add_object_location_if_current(
        &mut self,
        object_id: &DfsObjectId,
        expected_version: &DfsVersion,
        expected_content_hash: &str,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsObjectLocationUpdate>> {
        let Some((shard_id, authoritative_object)) =
            self.authoritative_object_placement(object_id)?
        else {
            return Ok(None);
        };
        if authoritative_object.kind == DfsObjectKind::Tombstone
            || authoritative_object.version != *expected_version
            || authoritative_object.content_hash.as_deref() != Some(expected_content_hash)
        {
            return Ok(None);
        }
        let updated = self
            .shard_backend_mut(&shard_id)?
            .add_object_location_if_current(
                object_id,
                expected_version,
                expected_content_hash,
                replica_node_id,
            )?;
        if let Some(update) = updated.as_ref() {
            self.write_object_shard_index(&update.object, &shard_id)?;
        }
        Ok(updated)
    }

    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>> {
        #[cfg(test)]
        SHARDED_METADATA_OBJECT_ID_LOOKUP_COUNT.set(
            SHARDED_METADATA_OBJECT_ID_LOOKUP_COUNT
                .get()
                .saturating_add(1),
        );
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return self.shard_backend(&shard_id)?.get_object(object_id);
        }
        Ok(None)
    }

    fn get_object_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        self.shard_backend(shard_id)?.get_object(object_id)
    }

    fn get_advisory_lock_state(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsAdvisoryLockStateRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend(&shard_id)?
            .get_advisory_lock_state(file_system_id, object_id)
    }

    fn apply_advisory_lock_command(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        context: DfsAdvisoryLockTransitionContext,
        command: &DfsAdvisoryLockCommand,
    ) -> Result<DfsAdvisoryLockApplyResult> {
        let shard_id = self.require_shard_id_for_object(object_id)?;
        self.shard_backend_mut(&shard_id)?
            .apply_advisory_lock_command(file_system_id, object_id, context, command)
    }

    fn install_advisory_lock_state_for_migration(
        &mut self,
        state: &DfsAdvisoryLockStateRecord,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let shard_id = self.require_shard_id_for_object(&state.object_id)?;
        self.shard_backend_mut(&shard_id)?
            .install_advisory_lock_state_for_migration(state)
    }

    fn get_object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let shard_id = self.select_shard_id_for_path(file_system_id, path)?;
        let object = self
            .shard_backend(&shard_id)?
            .get_object_by_path(file_system_id, path)?;
        if let Some(object) = object {
            if self.object_is_visible_from_shard(&object, &shard_id)? {
                return Ok(Some(object));
            }
            // A non-authoritative physical row on the currently selected
            // shard does not suppress a live fallback. In particular, a
            // source-eviction marker may become path-selected after a later
            // topology change, but it remains storage-only evidence.
        }
        for fallback_shard_id in
            self.fallback_shard_ids_for_path(file_system_id, path, &shard_id)?
        {
            let Some(candidate) = self
                .shard_backend(&fallback_shard_id)?
                .get_object_by_path(file_system_id, path)?
            else {
                continue;
            };
            if self.object_is_visible_from_shard(&candidate, &fallback_shard_id)? {
                return Ok(Some(candidate));
            }
        }
        Ok(None)
    }

    fn get_object_by_path_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        self.shard_backend(shard_id)?
            .get_object_by_path(file_system_id, path)
    }

    fn namespace_prepare_expectations(
        &self,
        shard_id: &DfsMetadataShardId,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<(
        Vec<DfsNamespaceObjectVersionExpectation>,
        Vec<DfsNamespacePathOccupantExpectation>,
        BTreeSet<String>,
    )> {
        self.shard_backend(shard_id)?
            .namespace_prepare_expectations(shard_id, transaction)
    }

    fn objects_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut objects = BTreeMap::<DfsObjectId, DfsObjectMetadata>::new();
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            walk_metadata_pages(|cursor| {
                let page = backend.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                for object in page.objects {
                    if !self.object_is_listable_from_shard(&object, shard_id)? {
                        continue;
                    }
                    objects.insert(object.object_id.clone(), object);
                }
                Ok(page.next_cursor)
            })?;
        }
        let mut objects = objects.into_values().collect::<Vec<_>>();
        objects.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(objects)
    }

    fn objects_for_file_system_page(
        &self,
        file_system_id: &DfsFileSystemId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        if max_entries == 0 {
            return Ok(DfsObjectPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let resume_shard_id = after.and_then(|cursor| cursor.shard_id.as_ref());
        let entries = collect_resumed_sharded_page_entries(
            &self.shards,
            after,
            resume_shard_id,
            max_entries,
            local_object_page_cursor,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.objects_for_file_system_page(file_system_id, cursor, limit)?;
                Ok((page.objects, page.next_cursor))
            },
            |shard_id, _, object| {
                if !self.object_is_listable_from_shard(&object, shard_id)? {
                    return Ok(None);
                }
                let cursor = object_page_cursor_with_shard(&object, shard_id);
                Ok(Some((object, cursor)))
            },
        )?;
        let (objects, next_cursor) = finish_cursor_page(entries, max_entries);
        Ok(DfsObjectPage {
            objects,
            next_cursor,
        })
    }

    fn object_page_cursor_for_object(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsObjectPageCursor> {
        Ok(DfsObjectPageCursor {
            path: object.path.clone(),
            object_id: object.object_id.clone(),
            shard_id: self.shard_id_for_object(&object.object_id)?,
        })
    }

    fn directory_page_cursor_for_object(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsDirectoryPageCursor> {
        let shard_id = self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        let cursor = self
            .shard_backend(&shard_id)?
            .indexed_directory_page_cursor_for_object(object)?;
        Ok(directory_page_cursor_with_shard(cursor, &shard_id))
    }

    fn child_objects_for_directory_page_entries(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<(
        Vec<(DfsObjectMetadata, DfsDirectoryPageCursor)>,
        Option<DfsDirectoryPageCursor>,
    )> {
        if max_entries == 0 {
            return Ok((Vec::new(), None));
        }
        let resume_shard_id = after.and_then(|cursor| cursor.shard_id.as_ref());
        let mut entries = collect_resumed_sharded_page_entries(
            &self.shards,
            after,
            resume_shard_id,
            max_entries,
            local_directory_page_cursor,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.child_objects_for_directory_page_entries(
                    file_system_id,
                    directory_path,
                    cursor,
                    limit,
                )?;
                Ok((page.entries, page.next_cursor))
            },
            |shard_id, _, (object, local_cursor)| {
                if !self.object_is_listable_from_shard(&object, shard_id)? {
                    return Ok(None);
                }
                Ok(Some((
                    object,
                    directory_page_cursor_with_shard(local_cursor, shard_id),
                )))
            },
        )?;
        let has_more = entries.len() > max_entries;
        entries.truncate(max_entries);
        let next_cursor = has_more
            .then(|| entries.last().map(|(_, cursor)| cursor.clone()))
            .flatten();
        Ok((entries, next_cursor))
    }

    fn child_tombstones_for_directory(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut candidate_paths = BTreeSet::new();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            for tombstone in
                backend.child_tombstones_for_directory(file_system_id, directory_path)?
            {
                candidate_paths.insert(normalize_path(&tombstone.path));
            }
        }
        let mut tombstones = Vec::new();
        for path in candidate_paths {
            if let Some(object) = self.get_object_by_path(file_system_id, &path)?
                && object.kind == DfsObjectKind::Tombstone
            {
                tombstones.push(object);
            }
        }
        Ok(tombstones)
    }

    fn child_objects_for_directory_page(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        let (entries, next_cursor) = DfsMetadataBackend::child_objects_for_directory_page_entries(
            self,
            file_system_id,
            directory_path,
            after,
            max_entries,
        )?;
        Ok(DfsDirectoryPage {
            objects: entries.into_iter().map(|(object, _)| object).collect(),
            next_cursor,
        })
    }

    fn hardlinks_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut hardlinks = collect_metadata_pages(|cursor| {
            let page = self.hardlinks_to_object_page(
                file_system_id,
                target_object_id,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.objects, page.next_cursor))
        })?;
        sort_hardlinks_by_path(&mut hardlinks);
        Ok(hardlinks)
    }

    fn hardlinks_to_object_page(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        if max_entries == 0 {
            return Ok(DfsObjectPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let resume_shard_id = after.and_then(|cursor| cursor.shard_id.as_ref());
        let entries = collect_resumed_sharded_page_entries(
            &self.shards,
            after,
            resume_shard_id,
            max_entries,
            local_object_page_cursor,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.hardlinks_to_object_page(
                    file_system_id,
                    target_object_id,
                    cursor,
                    limit,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |shard_id, _, object| {
                if !self.object_is_listable_from_shard(&object, shard_id)? {
                    return Ok(None);
                }
                let cursor = object_page_cursor_with_shard(&object, shard_id);
                Ok(Some((object, cursor)))
            },
        )?;
        let (objects, next_cursor) = finish_cursor_page(entries, max_entries);
        Ok(DfsObjectPage {
            objects,
            next_cursor,
        })
    }

    fn hardlink_count_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<usize> {
        let mut object_ids = BTreeSet::<DfsObjectId>::new();
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            walk_metadata_pages(|cursor| {
                let page = backend.hardlinks_to_object_page(
                    file_system_id,
                    target_object_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                for object in page.objects {
                    if self.object_is_listable_from_shard(&object, shard_id)? {
                        object_ids.insert(object.object_id);
                    }
                }
                Ok(page.next_cursor)
            })?;
        }
        Ok(object_ids.len())
    }

    fn first_hardlink_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        self.first_hardlink_to_object_matching(file_system_id, target_object_id, |_| true)
    }

    fn first_hardlink_to_object_outside_prefix(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let prefix = normalize_posix_path(prefix);
        self.first_hardlink_to_object_matching(file_system_id, target_object_id, |object| {
            !posix_path_is_at_or_under_prefix(&object.path, &prefix)
        })
    }

    fn put_version(&mut self, version: &DfsObjectVersionRecord) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.require_shard_id_for_object(&version.object_id)?;
        self.shard_backend_mut(&shard_id)?.put_version(version)
    }

    fn put_version_pin(
        &mut self,
        pin: &DfsMetadataVersionPinRecord,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let shard_id = self.require_shard_id_for_object(&pin.object_id)?;
        self.shard_backend_mut(&shard_id)?.put_version_pin(pin)
    }

    fn remove_version_pin(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        pin_id: &str,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let mut containing_shards = Vec::new();
        let key = (version.clone(), pin_id.to_string());
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            if let Some(owner) = backend
                .cross_shard_pin_transfers_for_object(object_id)?
                .into_iter()
                .find(|transfer| {
                    transfer
                        .source_pins
                        .iter()
                        .any(|pin| &pin.version == version && pin.pin_id == pin_id)
                })
            {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata version pin {pin_id} for {object_id}/{version} on shard {} is fenced by active cross-shard transfer {}",
                    shard_id.0, owner.transfer_id
                )));
            }
            if let Some(pin) = backend
                .version_pins_for_object(object_id)?
                .into_iter()
                .find(|pin| (pin.version.clone(), pin.pin_id.clone()) == key)
            {
                if &pin.file_system_id != file_system_id {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata version pin {pin_id} on shard {} has filesystem {}, expected {file_system_id}",
                        shard_id.0, pin.file_system_id
                    )));
                }
                containing_shards.push(shard_id.clone());
            }
        }
        let mut first_entry = None;
        for shard_id in containing_shards {
            if let Some(entry) = self.shard_backend_mut(&shard_id)?.remove_version_pin(
                file_system_id,
                object_id,
                version,
                pin_id,
            )? {
                first_entry.get_or_insert(entry);
            }
        }
        Ok(first_entry)
    }

    fn version_pins_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsMetadataVersionPinRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(Vec::new());
        };
        self.shard_backend(&shard_id)?
            .version_pins_for_object(object_id)
    }

    fn version_pins_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsMetadataVersionPinPageCursor>,
        max_entries: usize,
    ) -> Result<DfsMetadataVersionPinPage> {
        if max_entries == 0 {
            return Ok(DfsMetadataVersionPinPage {
                pins: Vec::new(),
                next_cursor: None,
            });
        }
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(DfsMetadataVersionPinPage {
                pins: Vec::new(),
                next_cursor: None,
            });
        };
        let local_after = after.map(local_version_pin_page_cursor);
        let mut page = self
            .shard_backend(&shard_id)?
            .version_pins_for_object_page(object_id, local_after.as_ref(), max_entries)?;
        if let Some(cursor) = page.next_cursor.as_mut() {
            cursor.shard_id = Some(shard_id);
        }
        Ok(page)
    }

    fn install_cross_shard_version_pin_family(
        &mut self,
        object: &DfsObjectMetadata,
        versions: &[DfsObjectVersionRecord],
        pins: &[DfsMetadataVersionPinRecord],
    ) -> Result<()> {
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        for version in versions {
            if version.object_id != object.object_id
                || version.file_system_id != object.file_system_id
            {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin-family install for object {} received unrelated version {}/{}",
                    object.object_id, version.object_id, version.version
                )));
            }
            match self
                .shard_backend(&target_shard_id)?
                .get_version(&version.object_id, &version.version)?
            {
                Some(existing) if existing != *version => {
                    return Err(MetadataBackendError::Serde(format!(
                        "target shard {} has conflicting version {} for pinned object {}",
                        target_shard_id.0, version.version, object.object_id
                    )));
                }
                Some(_) => {}
                None => {
                    self.shard_backend_mut(&target_shard_id)?
                        .put_version(version)?;
                }
            }
        }
        let mut desired = BTreeMap::new();
        for pin in pins {
            if pin.object_id != object.object_id || pin.file_system_id != object.file_system_id {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin-family install for object {} received unrelated pin {} for {}/{}",
                    object.object_id, pin.pin_id, pin.object_id, pin.version
                )));
            }
            desired.insert((pin.version.clone(), pin.pin_id.clone()), pin.clone());
        }
        let target_is_already_authoritative = self
            .shard_id_for_object(&object.object_id)?
            .is_some_and(|shard_id| shard_id == target_shard_id);
        let existing = self
            .shard_backend(&target_shard_id)?
            .version_pins_for_object(&object.object_id)?;
        if !target_is_already_authoritative {
            for pin in &existing {
                let key = (pin.version.clone(), pin.pin_id.clone());
                if desired.get(&key) == Some(pin) {
                    continue;
                }
                self.shard_backend_mut(&target_shard_id)?
                    .remove_version_pin(
                        &pin.file_system_id,
                        &pin.object_id,
                        &pin.version,
                        &pin.pin_id,
                    )?;
            }
        }
        for pin in desired.values() {
            if target_is_already_authoritative
                && existing.iter().any(|current| {
                    current.version == pin.version && current.pin_id == pin.pin_id && current != pin
                })
            {
                return Err(MetadataBackendError::Serde(format!(
                    "authoritative target shard {} has a conflicting new pin {} for {}/{}",
                    target_shard_id.0, pin.pin_id, pin.object_id, pin.version
                )));
            }
            self.shard_backend_mut(&target_shard_id)?
                .put_version_pin(pin)?;
        }
        Ok(())
    }

    fn reconcile_cross_shard_pin_transfer(
        &mut self,
        transfer_id: &str,
        object: &DfsObjectMetadata,
        source_path: &str,
        versions: &[DfsObjectVersionRecord],
        source_pins: &[DfsMetadataVersionPinRecord],
    ) -> Result<bool> {
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        self.shard_backend_mut(&target_shard_id)?
            .reconcile_cross_shard_pin_transfer(
                transfer_id,
                object,
                source_path,
                versions,
                source_pins,
            )
    }

    fn finalize_cross_shard_pin_transfer(
        &mut self,
        transfer_id: &str,
        object: &DfsObjectMetadata,
        source_path: &str,
    ) -> Result<()> {
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        self.shard_backend_mut(&target_shard_id)?
            .finalize_cross_shard_pin_transfer(transfer_id, object, source_path)
    }

    fn cross_shard_pin_transfers_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsCrossShardPinTransferRecord>> {
        let shard_id = self.require_shard_id_for_object(object_id)?;
        self.shard_backend(&shard_id)?
            .cross_shard_pin_transfers_for_object(object_id)
    }

    fn install_cross_shard_pin_transfer_for_authoritative_migration(
        &mut self,
        transfer: &DfsCrossShardPinTransferRecord,
        versions: &[DfsObjectVersionRecord],
    ) -> Result<()> {
        let target_shard_id =
            self.select_shard_id_for_path(&transfer.file_system_id, &transfer.target_path)?;
        self.shard_backend_mut(&target_shard_id)?
            .install_cross_shard_pin_transfer_for_authoritative_migration(transfer, versions)
    }

    fn remove_cross_shard_pin_transfer_for_authoritative_migration(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        transfer_id: &str,
    ) -> Result<()> {
        let shard_ids = self.shard_ids();
        for shard_id in shard_ids {
            self.shard_backend_mut(&shard_id)?
                .remove_cross_shard_pin_transfer_for_authoritative_migration(
                    file_system_id,
                    object_id,
                    transfer_id,
                )?;
        }
        Ok(())
    }

    fn capture_cross_shard_source_pin_fence(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsCrossShardVersionPinFence> {
        let shard_id = self.require_shard_id_for_object(object_id)?;
        self.shard_backend_mut(&shard_id)?
            .capture_cross_shard_source_pin_fence(object_id)
    }

    fn capture_cross_shard_source_family(
        &mut self,
        object_id: &DfsObjectId,
        source_path: &str,
    ) -> Result<DfsCrossShardSourceFamily> {
        let authority_shard_id = self.require_shard_id_for_object(object_id)?;
        let authority_object = self
            .shard_backend(&authority_shard_id)?
            .get_object(object_id)?
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            })?;
        let source_shard_id =
            self.select_shard_id_for_path(&authority_object.file_system_id, source_path)?;
        let (_source_lock, bundle) =
            self.object_shard_migration_bundle(&source_shard_id, object_id)?;
        if bundle.object.file_system_id != authority_object.file_system_id
            || normalize_path(&bundle.object.path) != normalize_path(source_path)
        {
            return Err(MetadataBackendError::Serde(format!(
                "physical source shard {} does not contain object {} at requested path {}",
                source_shard_id.0, object_id, source_path
            )));
        }
        Ok(bundle.into())
    }

    fn put_object_and_version(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let object = normalized_object_metadata(object);
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        let source_shard_id = self.shard_id_for_object(&object.object_id)?;
        if let Some(source_shard_id) = source_shard_id.as_ref() {
            self.require_unpinned_source_for_unmigrated_move(
                source_shard_id,
                &target_shard_id,
                &object.object_id,
                "combined object/version update",
            )?;
        }
        let entries = self
            .shard_backend_mut(&target_shard_id)?
            .put_object_and_version(&object, version)?;
        self.write_object_shard_index(&object, &target_shard_id)?;
        if let Some(source_shard_id) = source_shard_id {
            self.put_cross_shard_tombstone(&object, &source_shard_id, &target_shard_id)?;
        }
        Ok(entries)
    }

    fn put_object_version_intent_and_receipts(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
        intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let object = normalized_object_metadata(object);
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        let source_shard_id = self.shard_id_for_object(&object.object_id)?;
        if let Some(source_shard_id) = source_shard_id.as_ref() {
            self.require_unpinned_source_for_unmigrated_move(
                source_shard_id,
                &target_shard_id,
                &object.object_id,
                "combined visible-version commit",
            )?;
        }
        let entries = self
            .shard_backend_mut(&target_shard_id)?
            .put_object_version_intent_and_receipts(&object, version, intent, receipts)?;
        self.write_content_commit_object_shard_index(&object, &target_shard_id)?;
        if let Some(source_shard_id) = source_shard_id {
            self.put_cross_shard_tombstone(&object, &source_shard_id, &target_shard_id)?;
        }
        Ok(entries)
    }

    fn commit_create_intent_receipts_and_namespace(
        &mut self,
        pending_intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
        transaction: &DfsNamespaceTransaction,
        committed_intent: &DfsWriteIntentRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut shard_ids = BTreeSet::new();
        for object in &transaction.objects {
            shard_ids.insert(self.select_shard_id_for_path(&object.file_system_id, &object.path)?);
        }
        if shard_ids.len() != 1 {
            return Err(MetadataBackendError::Serde(format!(
                "combined create transaction {} spans {} metadata shards",
                transaction.transaction_id,
                shard_ids.len()
            )));
        }
        let shard_id = shard_ids.into_iter().next().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "combined create transaction {} contains no objects",
                transaction.transaction_id
            ))
        })?;
        if !transaction
            .objects
            .iter()
            .any(|object| object.object_id == pending_intent.object_id)
        {
            return Err(MetadataBackendError::Serde(format!(
                "combined create transaction {} is missing intent object {}",
                transaction.transaction_id, pending_intent.object_id
            )));
        }
        let entries = self
            .shard_backend_mut(&shard_id)?
            .commit_create_intent_receipts_and_namespace(
                pending_intent,
                receipts,
                transaction,
                committed_intent,
            )?;
        self.write_transaction_object_shard_indexes(&transaction.objects, &shard_id)?;
        Ok(entries)
    }

    fn get_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Option<DfsObjectVersionRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend(&shard_id)?
            .get_version(object_id, version)
    }

    fn versions_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsObjectVersionRecord>> {
        collect_metadata_pages(|cursor| {
            let page =
                self.versions_for_object_page(object_id, cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.versions, page.next_cursor))
        })
    }

    fn versions_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsObjectVersionPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionPage> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(DfsObjectVersionPage {
                versions: Vec::new(),
                next_cursor: None,
            });
        };
        let local_after = after.map(local_object_version_page_cursor);
        let page = self.shard_backend(&shard_id)?.versions_for_object_page(
            object_id,
            local_after.as_ref(),
            max_entries,
        )?;
        Ok(object_version_page_with_shard_cursor(page, &shard_id))
    }

    fn put_receipt(
        &mut self,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.require_shard_id_for_object(&receipt.receipt.object_id)?;
        self.shard_backend_mut(&shard_id)?.put_receipt(receipt)
    }

    fn remove_receipt(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend_mut(&shard_id)?
            .remove_receipt(object_id, version, replica_node_id)
    }

    fn put_receipt_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.shard_id_for_pending_object_record(
            &receipt.receipt.object_id,
            &receipt.receipt.file_system_id,
            object,
        )?;
        let entry = self.shard_backend_mut(&shard_id)?.put_receipt(receipt)?;
        if let Some(object) = object {
            self.write_object_shard_index(&normalized_object_metadata(object), &shard_id)?;
        }
        Ok(entry)
    }

    fn receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.receipts_for_version_page(
                object_id,
                version,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.receipts, page.next_cursor))
        })
    }

    fn receipts_for_version_page(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        after: Option<&DfsReplicaReceiptPageCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptPage> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(DfsReplicaReceiptPage {
                receipts: Vec::new(),
                next_cursor: None,
            });
        };
        let local_after = after.map(local_replica_receipt_page_cursor);
        let page = self.shard_backend(&shard_id)?.receipts_for_version_page(
            object_id,
            version,
            local_after.as_ref(),
            max_entries,
        )?;
        Ok(replica_receipt_page_with_shard_cursor(page, &shard_id))
    }

    fn put_intent(&mut self, intent: &DfsWriteIntentRecord) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.shard_id_for_intent_write(intent)?;
        self.shard_backend_mut(&shard_id)?.put_intent(intent)
    }

    fn put_intent_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        if let Some(shard_id) =
            self.shard_id_for_existing_intent(&intent.object_id, &intent.intent_id)?
        {
            return self.shard_backend_mut(&shard_id)?.put_intent(intent);
        }
        let shard_id = self.shard_id_for_pending_object_record(
            &intent.object_id,
            &intent.file_system_id,
            object,
        )?;
        let entry = self.shard_backend_mut(&shard_id)?.put_intent(intent)?;
        if let Some(object) = object {
            self.write_object_shard_index(&normalized_object_metadata(object), &shard_id)?;
        }
        Ok(entry)
    }

    fn put_migrated_intent_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = if let Some(object) = object {
            if object.object_id != intent.object_id
                || object.file_system_id != intent.file_system_id
            {
                return Err(MetadataBackendError::Serde(format!(
                    "migrated intent {}/{} destination object identity mismatch",
                    intent.object_id, intent.intent_id
                )));
            }
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?
        } else if let Some(shard_id) =
            self.shard_id_for_existing_intent(&intent.object_id, &intent.intent_id)?
        {
            shard_id
        } else {
            self.shard_id_for_pending_object_record(
                &intent.object_id,
                &intent.file_system_id,
                None,
            )?
        };
        let entry = self
            .shard_backend_mut(&shard_id)?
            .put_migrated_intent_for_object(object, intent)?;
        if let Some(object) = object {
            self.write_object_shard_index(&normalized_object_metadata(object), &shard_id)?;
        }
        Ok(entry)
    }

    fn put_intent_receipt_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        receipt: &DfsWriteIntentReceipt,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = if let Some(object) = object {
            if object.object_id != receipt.object_id
                || object.file_system_id != receipt.file_system_id
            {
                return Err(MetadataBackendError::Serde(format!(
                    "migrated intent receipt {}/{} destination object identity mismatch",
                    receipt.object_id, receipt.intent_id
                )));
            }
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?
        } else if let Some(shard_id) =
            self.shard_id_for_existing_intent(&receipt.object_id, &receipt.intent_id)?
        {
            shard_id
        } else {
            self.shard_id_for_pending_object_record(
                &receipt.object_id,
                &receipt.file_system_id,
                None,
            )?
        };
        let entry = self
            .shard_backend_mut(&shard_id)?
            .put_intent_receipt_for_object(object, receipt)?;
        if let Some(object) = object {
            self.write_object_shard_index(&normalized_object_metadata(object), &shard_id)?;
        }
        Ok(entry)
    }

    fn merge_intent_receipt_retention_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        retention: &DfsWriteIntentReceiptRetentionState,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id =
            self.shard_id_for_pending_object_record(object_id, file_system_id, object)?;
        let entry = self
            .shard_backend_mut(&shard_id)?
            .merge_intent_receipt_retention_for_object(
                object,
                file_system_id,
                object_id,
                retention,
            )?;
        if let Some(object) = object {
            self.write_object_shard_index(&normalized_object_metadata(object), &shard_id)?;
        }
        Ok(entry)
    }

    fn object_write_intent_history(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectWriteIntentHistory> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return self
                .shard_backend(&shard_id)?
                .object_write_intent_history(object_id);
        }

        let mut intents = BTreeMap::<String, DfsWriteIntentRecord>::new();
        let mut receipts = BTreeMap::<String, DfsWriteIntentReceipt>::new();
        let mut retention = DfsWriteIntentReceiptRetentionState::default();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            let history = backend.object_write_intent_history(object_id)?;
            retention.merge_conservative(history.retention);
            for intent in history.intents {
                if let Some(receipt) = receipts.get(&intent.intent_id) {
                    if !receipt.matches_intent(&intent) {
                        return Err(MetadataBackendError::Serde(format!(
                            "object {} has conflicting full and compact intent {} across metadata shards",
                            object_id, intent.intent_id
                        )));
                    }
                    continue;
                }
                if let Some(existing) = intents.get(&intent.intent_id)
                    && existing != &intent
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "object {} has conflicting intent {} across metadata shards",
                        object_id, intent.intent_id
                    )));
                }
                intents.insert(intent.intent_id.clone(), intent);
            }
            for receipt in history.receipts {
                if let Some(intent) = intents.remove(&receipt.intent_id)
                    && !receipt.matches_intent(&intent)
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "object {} has conflicting full and compact intent {} across metadata shards",
                        object_id, receipt.intent_id
                    )));
                }
                if let Some(existing) = receipts.get(&receipt.intent_id)
                    && existing != &receipt
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "object {} has conflicting compact intent receipt {} across metadata shards",
                        object_id, receipt.intent_id
                    )));
                }
                receipts.insert(receipt.intent_id.clone(), receipt);
            }
        }
        Ok(DfsObjectWriteIntentHistory {
            intents: intents.into_values().collect(),
            receipts: receipts.into_values().collect(),
            retention,
        })
    }

    fn get_intent_receipt(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentReceipt>> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return self
                .shard_backend(&shard_id)?
                .get_intent_receipt(object_id, intent_id);
        }
        let mut found = None;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            let Some(receipt) = backend.get_intent_receipt(object_id, intent_id)? else {
                continue;
            };
            if found.as_ref().is_some_and(|existing| existing != &receipt) {
                return Err(MetadataBackendError::Serde(format!(
                    "object {} has conflicting compact intent receipt {} across metadata shards",
                    object_id, intent_id
                )));
            }
            found = Some(receipt);
        }
        Ok(found)
    }

    fn intent_receipt_retention_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<DfsWriteIntentReceiptRetentionState> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return self
                .shard_backend(&shard_id)?
                .intent_receipt_retention_for_object(object_id);
        }
        let mut retention = DfsWriteIntentReceiptRetentionState::default();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            retention.merge_conservative(backend.intent_receipt_retention_for_object(object_id)?);
        }
        Ok(retention)
    }

    fn rebind_prepared_write_intent(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        expected_intent: &DfsWriteIntentRecord,
        rebound_intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self
            .shard_id_for_existing_intent(&expected_intent.object_id, &expected_intent.intent_id)?
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "write intent {} does not exist for object {}",
                    expected_intent.intent_id, expected_intent.object_id
                ))
            })?;
        if let Some(object) = object {
            let selected = self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
            if selected != shard_id {
                return Err(MetadataBackendError::Serde(format!(
                    "write intent {} is durable on shard {}, but object path {} routes to shard {}",
                    expected_intent.intent_id, shard_id.0, object.path, selected.0
                )));
            }
        }
        self.shard_backend_mut(&shard_id)?
            .rebind_prepared_write_intent(object, expected_intent, rebound_intent)
    }

    fn put_intent_and_receipts_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        if let Some(shard_id) =
            self.shard_id_for_existing_intent(&intent.object_id, &intent.intent_id)?
        {
            return self
                .shard_backend_mut(&shard_id)?
                .put_intent_and_receipts_for_object(object, intent, receipts);
        }
        let shard_id = self.shard_id_for_pending_object_record(
            &intent.object_id,
            &intent.file_system_id,
            object,
        )?;
        let entries = self
            .shard_backend_mut(&shard_id)?
            .put_intent_and_receipts_for_object(object, intent, receipts)?;
        if let Some(object) = object {
            self.write_object_shard_index(&normalized_object_metadata(object), &shard_id)?;
        }
        Ok(entries)
    }

    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return self
                .shard_backend(&shard_id)?
                .get_intent(object_id, intent_id);
        }
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(intent) = backend.get_intent(object_id, intent_id)? {
                return Ok(Some(intent));
            }
        }
        Ok(None)
    }

    fn get_intent_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>> {
        let Some(slot) = self.shards.get(shard_id) else {
            return Ok(None);
        };
        slot.backend()?
            .get_intent_on_shard(shard_id, object_id, intent_id)
    }

    fn get_receipt_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsReplicaReceiptRecord>> {
        let Some(slot) = self.shards.get(shard_id) else {
            return Ok(None);
        };
        slot.backend()?
            .get_receipt_on_shard(shard_id, object_id, version, replica_node_id)
    }

    fn write_intent_counter_floor(&self) -> Result<u64> {
        self.shards.values().try_fold(0_u64, |floor, slot| {
            let backend = slot.backend()?;
            Ok(floor.max(backend.write_intent_counter_floor()?))
        })
    }

    fn intents_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsWriteIntentRecord>> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            let backend = self.shard_backend(&shard_id)?;
            return collect_metadata_pages(|cursor| {
                let page = backend.intents_for_object_page(
                    object_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.intents, page.next_cursor))
            });
        }
        let mut intents = BTreeMap::<String, DfsWriteIntentRecord>::new();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            walk_metadata_pages(|cursor| {
                let page = backend.intents_for_object_page(
                    object_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                for intent in page.intents {
                    intents.entry(intent.intent_id.clone()).or_insert(intent);
                }
                Ok(page.next_cursor)
            })?;
        }
        Ok(intents.into_values().collect())
    }

    fn intents_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        if max_entries == 0 {
            return Ok(DfsWriteIntentPage {
                intents: Vec::new(),
                next_cursor: None,
            });
        }
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            let local_after = after.map(local_write_intent_page_cursor);
            let page = self.shard_backend(&shard_id)?.intents_for_object_page(
                object_id,
                local_after.as_ref(),
                max_entries,
            )?;
            return Ok(write_intent_page_with_shard_cursor(page, &shard_id));
        }

        let resume_shard_id = after.and_then(|cursor| cursor.shard_id.as_ref());
        let mut seen_intent_ids = BTreeSet::<String>::new();
        let entries = collect_resumed_sharded_page_entries(
            &self.shards,
            after,
            resume_shard_id,
            max_entries,
            local_write_intent_page_cursor,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.intents_for_object_page(object_id, cursor, limit)?;
                Ok((page.intents, page.next_cursor))
            },
            |shard_id, _, intent| {
                if !seen_intent_ids.insert(intent.intent_id.clone()) {
                    return Ok(None);
                }
                let cursor = write_intent_page_cursor_with_shard(
                    write_intent_page_cursor(&intent),
                    shard_id,
                );
                Ok(Some((intent, cursor)))
            },
        )?;
        let (intents, next_cursor) = finish_cursor_page(entries, max_entries);
        Ok(DfsWriteIntentPage {
            intents,
            next_cursor,
        })
    }

    fn intents_for_object_version_page(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        if max_entries == 0 {
            return Ok(DfsWriteIntentPage {
                intents: Vec::new(),
                next_cursor: None,
            });
        }
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            let local_after = after.map(local_write_intent_page_cursor);
            let page = self
                .shard_backend(&shard_id)?
                .intents_for_object_version_page(
                    object_id,
                    target_version,
                    local_after.as_ref(),
                    max_entries,
                )?;
            return Ok(write_intent_page_with_shard_cursor(page, &shard_id));
        }

        let resume_shard_id = after.and_then(|cursor| cursor.shard_id.as_ref());
        let mut seen_intent_ids = BTreeSet::<String>::new();
        let entries = collect_resumed_sharded_page_entries(
            &self.shards,
            after,
            resume_shard_id,
            max_entries,
            local_write_intent_page_cursor,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.intents_for_object_version_page(
                    object_id,
                    target_version,
                    cursor,
                    limit,
                )?;
                Ok((page.intents, page.next_cursor))
            },
            |shard_id, slot, intent| {
                let backend = slot.backend()?;
                if !self.intent_is_listable_from_shard(&intent, backend, shard_id)? {
                    return Ok(None);
                }
                if !seen_intent_ids.insert(intent.intent_id.clone()) {
                    return Ok(None);
                }
                let cursor = write_intent_page_cursor_with_shard(
                    write_intent_page_cursor(&intent),
                    shard_id,
                );
                Ok(Some((intent, cursor)))
            },
        )?;
        let (intents, next_cursor) = finish_cursor_page(entries, max_entries);
        Ok(DfsWriteIntentPage {
            intents,
            next_cursor,
        })
    }

    fn write_intents_page(
        &self,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentScanPage> {
        if max_entries == 0 {
            return Ok(DfsWriteIntentScanPage {
                intents: Vec::new(),
                next_cursor: None,
            });
        }
        let mut seen = BTreeSet::<(DfsObjectId, String)>::new();
        let entries = collect_sharded_scan_entries(
            &self.shards,
            after,
            max_entries,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.write_intents_page(cursor, limit)?;
                Ok((page.intents, page.next_cursor))
            },
            |shard_id, slot, intent| {
                let backend = slot.backend()?;
                if !self.intent_is_listable_from_shard(&intent, backend, shard_id)? {
                    return Ok(None);
                }
                if !seen.insert((intent.object_id.clone(), intent.intent_id.clone())) {
                    return Ok(None);
                }
                let cursor = write_intent_scan_cursor(&intent);
                Ok(Some((intent, cursor)))
            },
        )?;
        let (intents, next_cursor) =
            finish_sorted_cursor_page(entries, max_entries, write_intent_scan_order);
        Ok(DfsWriteIntentScanPage {
            intents,
            next_cursor,
        })
    }

    fn write_intents_on_shard_page(
        &self,
        shard_id: &DfsMetadataShardId,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<Option<DfsWriteIntentScanPage>> {
        let Some(backend) = self.shards.get(shard_id) else {
            return Ok(None);
        };
        backend
            .backend()?
            .write_intents_on_shard_page(shard_id, after, max_entries)
    }

    fn version_records_page(
        &self,
        after: Option<&DfsObjectVersionScanCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionScanPage> {
        if max_entries == 0 {
            return Ok(DfsObjectVersionScanPage {
                versions: Vec::new(),
                next_cursor: None,
            });
        }
        let mut seen = BTreeSet::<(DfsObjectId, DfsVersion)>::new();
        let entries = collect_sharded_scan_entries(
            &self.shards,
            after,
            max_entries,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.version_records_page(cursor, limit)?;
                Ok((page.versions, page.next_cursor))
            },
            |shard_id, slot, version| {
                let backend = slot.backend()?;
                let Some(object) = backend.get_object(&version.object_id)? else {
                    return Ok(None);
                };
                if !self.object_is_listable_from_shard(&object, shard_id)? {
                    return Ok(None);
                }
                if !seen.insert((version.object_id.clone(), version.version.clone())) {
                    return Ok(None);
                }
                let cursor = object_version_scan_cursor(&version);
                Ok(Some((version, cursor)))
            },
        )?;
        let (versions, next_cursor) =
            finish_sorted_cursor_page(entries, max_entries, |left, right| {
                object_version_scan_key(left).cmp(&object_version_scan_key(right))
            });
        Ok(DfsObjectVersionScanPage {
            versions,
            next_cursor,
        })
    }

    fn receipt_records_page(
        &self,
        after: Option<&DfsReplicaReceiptScanCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptScanPage> {
        if max_entries == 0 {
            return Ok(DfsReplicaReceiptScanPage {
                receipts: Vec::new(),
                next_cursor: None,
            });
        }
        let mut seen = BTreeSet::<(DfsObjectId, DfsVersion, NodeId)>::new();
        let mut authoritative_shards = BTreeMap::<
            (DfsFileSystemId, DfsObjectId, DfsVersion, Option<String>),
            Option<DfsMetadataShardId>,
        >::new();
        let entries = collect_sharded_scan_entries(
            &self.shards,
            after,
            max_entries,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.receipt_records_page(cursor, limit)?;
                Ok((page.receipts, page.next_cursor))
            },
            |shard_id, _, receipt| {
                let receipt_provenance = (
                    receipt.receipt.file_system_id.clone(),
                    receipt.receipt.object_id.clone(),
                    receipt.receipt.committed_version.clone(),
                    receipt.content_hash.clone(),
                );
                let authoritative_shard_id =
                    if let Some(shard_id) = authoritative_shards.get(&receipt_provenance) {
                        shard_id.clone()
                    } else {
                        let shard_id = self.shard_id_for_receipt_scan_record(&receipt)?;
                        authoritative_shards.insert(receipt_provenance, shard_id.clone());
                        shard_id
                    };
                if authoritative_shard_id.as_ref() != Some(shard_id) {
                    return Ok(None);
                }
                let key = (
                    receipt.receipt.object_id.clone(),
                    receipt.receipt.committed_version.clone(),
                    receipt.replica_node_id.clone(),
                );
                if !seen.insert(key) {
                    return Ok(None);
                }
                let cursor = replica_receipt_scan_cursor(&receipt);
                Ok(Some((receipt, cursor)))
            },
        )?;
        let (receipts, next_cursor) =
            finish_sorted_cursor_page(entries, max_entries, |left, right| {
                replica_receipt_scan_key(left).cmp(&replica_receipt_scan_key(right))
            });
        Ok(DfsReplicaReceiptScanPage {
            receipts,
            next_cursor,
        })
    }

    fn conflict_records_page(
        &self,
        after: Option<&DfsConflictScanCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictScanPage> {
        if max_entries == 0 {
            return Ok(DfsConflictScanPage {
                conflicts: Vec::new(),
                next_cursor: None,
            });
        }
        let mut seen = BTreeSet::<(DfsObjectId, String)>::new();
        let mut authoritative_shards = BTreeMap::<DfsObjectId, Option<DfsMetadataShardId>>::new();
        let entries = collect_sharded_scan_entries(
            &self.shards,
            after,
            max_entries,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.conflict_records_page(cursor, limit)?;
                Ok((page.conflicts, page.next_cursor))
            },
            |shard_id, _, conflict| {
                let authoritative_shard_id =
                    if let Some(shard_id) = authoritative_shards.get(&conflict.object_id) {
                        shard_id.clone()
                    } else {
                        let shard_id = self.shard_id_for_global_scan_object(&conflict.object_id)?;
                        authoritative_shards.insert(conflict.object_id.clone(), shard_id.clone());
                        shard_id
                    };
                if authoritative_shard_id.as_ref() != Some(shard_id) {
                    return Ok(None);
                }
                if !seen.insert((conflict.object_id.clone(), conflict.conflict_id.clone())) {
                    return Ok(None);
                }
                let cursor = conflict_scan_cursor(&conflict);
                Ok(Some((conflict, cursor)))
            },
        )?;
        let (conflicts, next_cursor) =
            finish_sorted_cursor_page(entries, max_entries, |left, right| {
                conflict_scan_key(left).cmp(&conflict_scan_key(right))
            });
        Ok(DfsConflictScanPage {
            conflicts,
            next_cursor,
        })
    }

    fn commit_visible_version(
        &mut self,
        expected_version: &DfsVersion,
        committed_object: &DfsObjectMetadata,
        visible_version: &DfsObjectVersionRecord,
        required_replica_node_ids: &[NodeId],
    ) -> Result<DfsMetadataJournalEntry> {
        self.commit_visible_version_with_operation_id(
            expected_version,
            committed_object,
            visible_version,
            required_replica_node_ids,
            None,
        )
    }

    fn commit_visible_version_with_operation_id(
        &mut self,
        expected_version: &DfsVersion,
        committed_object: &DfsObjectMetadata,
        visible_version: &DfsObjectVersionRecord,
        required_replica_node_ids: &[NodeId],
        operation_id: Option<&str>,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.require_shard_id_for_object(&committed_object.object_id)?;
        let entry = self
            .shard_backend_mut(&shard_id)?
            .commit_visible_version_with_operation_id(
                expected_version,
                committed_object,
                visible_version,
                required_replica_node_ids,
                operation_id,
            )?;
        self.write_content_commit_object_shard_index(
            &normalized_object_metadata(committed_object),
            &shard_id,
        )?;
        Ok(entry)
    }

    fn apply_namespace_transaction(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<DfsMetadataJournalEntry> {
        self.apply_namespace_transaction_with_operation_id(transaction, None)
    }

    fn apply_namespace_transaction_with_operation_id(
        &mut self,
        transaction: &DfsNamespaceTransaction,
        operation_id: Option<&str>,
    ) -> Result<DfsMetadataJournalEntry> {
        let mut objects_by_shard = BTreeMap::<DfsMetadataShardId, Vec<DfsObjectMetadata>>::new();
        let mut versions_by_shard =
            BTreeMap::<DfsMetadataShardId, Vec<DfsObjectVersionRecord>>::new();
        let mut advisory_lock_states_by_shard =
            BTreeMap::<DfsMetadataShardId, Vec<DfsAdvisoryLockStateRecord>>::new();
        let mut object_shards = BTreeMap::<DfsObjectId, DfsMetadataShardId>::new();
        let mut cross_shard_tombstones =
            Vec::<(DfsObjectMetadata, DfsMetadataShardId, DfsMetadataShardId)>::new();
        let explicit_lock_state_object_ids = transaction
            .advisory_lock_states
            .iter()
            .map(|state| state.object_id.clone())
            .collect::<BTreeSet<_>>();
        for object in &transaction.objects {
            let object = normalized_object_metadata(object);
            let target_shard_id =
                self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
            object_shards.insert(object.object_id.clone(), target_shard_id.clone());
            if let Some(source_shard_id) = self.indexed_shard_id_for_object(&object.object_id)?
                && source_shard_id != target_shard_id
            {
                let mut source_pins = self
                    .shard_backend(&source_shard_id)?
                    .version_pins_for_object(&object.object_id)?;
                source_pins.sort();
                if source_pins.is_empty() {
                    cross_shard_tombstones.push((
                        object.clone(),
                        source_shard_id.clone(),
                        target_shard_id.clone(),
                    ));
                } else {
                    let mut target_pins = self
                        .shard_backend(&target_shard_id)?
                        .version_pins_for_object(&object.object_id)?;
                    target_pins.sort();
                    if target_pins != source_pins {
                        return Err(MetadataBackendError::Serde(format!(
                            "namespace transaction {} cannot adopt pinned object {} until target shard {} durably contains the exact source pin set from shard {}",
                            transaction.transaction_id,
                            object.object_id,
                            target_shard_id.0,
                            source_shard_id.0
                        )));
                    }
                    // The exact target copy is durable, but source cleanup is
                    // deferred to the caller's journal-head/fingerprint
                    // receipt fence after this transaction publishes the
                    // destination object and index.
                }
                if !explicit_lock_state_object_ids.contains(&object.object_id)
                    && let Some(source_state) = self
                        .shard_backend(&source_shard_id)?
                        .get_advisory_lock_state(&object.file_system_id, &object.object_id)?
                {
                    let (prepared_state, _) = prepare_advisory_lock_state_for_migration(
                        &source_state,
                    )
                    .map_err(|source| {
                        MetadataBackendError::Serde(format!(
                            "prepare advisory-lock state for local shard move of {}: {source}",
                            object.object_id
                        ))
                    })?;
                    advisory_lock_states_by_shard
                        .entry(target_shard_id.clone())
                        .or_default()
                        .push(prepared_state);
                }
            }
            objects_by_shard
                .entry(target_shard_id)
                .or_default()
                .push(object);
        }
        for version in &transaction.versions {
            let version = normalized_version_record(version);
            let shard_id = if let Some(shard_id) = object_shards.get(&version.object_id) {
                shard_id.clone()
            } else {
                self.require_shard_id_for_object(&version.object_id)?
            };
            versions_by_shard.entry(shard_id).or_default().push(version);
        }
        for state in &transaction.advisory_lock_states {
            let mut state = state.clone();
            state.authority_rebind_required = true;
            let shard_id = if let Some(shard_id) = object_shards.get(&state.object_id) {
                shard_id.clone()
            } else {
                self.require_shard_id_for_object(&state.object_id)?
            };
            advisory_lock_states_by_shard
                .entry(shard_id)
                .or_default()
                .push(state);
        }
        let mut shard_ids = objects_by_shard.keys().cloned().collect::<BTreeSet<_>>();
        shard_ids.extend(versions_by_shard.keys().cloned());
        shard_ids.extend(advisory_lock_states_by_shard.keys().cloned());
        let mut last_entry = None;
        for shard_id in shard_ids {
            let objects = objects_by_shard.remove(&shard_id).unwrap_or_default();
            let versions = versions_by_shard.remove(&shard_id).unwrap_or_default();
            let advisory_lock_states = advisory_lock_states_by_shard
                .remove(&shard_id)
                .unwrap_or_default();
            let shard_transaction = DfsNamespaceTransaction {
                transaction_id: format!("{}:{}", transaction.transaction_id, shard_id.0),
                file_system_id: transaction.file_system_id.clone(),
                objects,
                versions,
                advisory_lock_states,
                created_at: transaction.created_at,
            };
            let entry = self
                .shard_backend_mut(&shard_id)?
                .apply_namespace_transaction_with_operation_id(&shard_transaction, operation_id)?;
            self.write_transaction_object_shard_indexes(&shard_transaction.objects, &shard_id)?;
            last_entry = Some(entry);
        }
        for (object, source_shard_id, target_shard_id) in cross_shard_tombstones {
            self.put_cross_shard_tombstone(&object, &source_shard_id, &target_shard_id)?;
        }
        last_entry.ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "namespace transaction {} did not contain any objects, versions, or advisory-lock states",
                transaction.transaction_id
            ))
        })
    }

    fn prepare_namespace_transaction(
        &mut self,
        prepare: &DfsNamespacePrepareRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let mut prepare = prepare.clone();
        self.prepare_namespace_transaction_in_place(&mut prepare)
    }

    fn prepare_normalized_namespace_transaction_in_place(
        &mut self,
        prepare: DfsNormalizedNamespacePrepare<'_>,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = {
            let record = prepare.as_record();
            if !self.shards.contains_key(&record.shard_id) {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {} targets unknown local shard {}",
                    record.operation_id, record.shard_id.0
                )));
            }
            let (configured_epoch, configured_codec) = self
                .shard_configs
                .get(&record.shard_id.0)
                .map(|shard| (shard.epoch, shard.namespace_prepare_codec))
                .ok_or_else(|| {
                    MetadataBackendError::Serde(format!(
                        "namespace prepare {} targets unconfigured local shard {}",
                        record.operation_id, record.shard_id.0
                    ))
                })?;
            if configured_codec.is_packed() && record.owner_epoch != configured_epoch {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {} owner epoch {} does not match configured shard {} epoch {}",
                    record.operation_id, record.owner_epoch, record.shard_id.0, configured_epoch
                )));
            }
            for path in record
                .transaction
                .objects
                .iter()
                .map(|object| object.path.as_str())
                .chain(
                    record
                        .expected_path_occupants
                        .iter()
                        .map(|expected| expected.path.as_str()),
                )
            {
                let selected = self.select_shard_id_for_path(&record.file_system_id, path)?;
                if selected != record.shard_id {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {} path {} routes to shard {}, not prepared shard {}",
                        record.operation_id, path, selected.0, record.shard_id.0
                    )));
                }
            }
            record.shard_id.clone()
        };
        self.shard_backend_mut(&shard_id)?
            .prepare_normalized_namespace_transaction_in_place(prepare)
    }

    fn rebind_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
        rebound_prepare: &DfsNamespacePrepareRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let rebound = normalized_namespace_prepare(rebound_prepare);
        let shard_id = self.unique_namespace_prepare_shard_id(operation_id, false)?;
        if rebound.shard_id != shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} is durable on shard {}, not rebound shard {}",
                shard_id.0, rebound.shard_id.0
            )));
        }
        for path in rebound
            .transaction
            .objects
            .iter()
            .map(|object| object.path.as_str())
            .chain(
                rebound
                    .expected_path_occupants
                    .iter()
                    .map(|expected| expected.path.as_str()),
            )
        {
            let selected = self.select_shard_id_for_path(&rebound.file_system_id, path)?;
            if selected != shard_id {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {operation_id} path {path} routes to shard {}, not durable shard {}",
                    selected.0, shard_id.0
                )));
            }
        }
        self.shard_backend_mut(&shard_id)?
            .rebind_prepared_namespace_transaction(
                operation_id,
                expected_operation_digest,
                expected_owner_epoch,
                &rebound,
            )
    }

    fn commit_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.unique_namespace_prepare_shard_id(operation_id, true)?;
        let routing_records = self.namespace_prepare_routing_records(&shard_id, operation_id)?;
        let root = self.root.clone();
        // The object-to-shard index is a disposable routing projection. Install
        // its exact lightweight rows before the semantic commit so a successful
        // compacting commit can never lose the body required by a later index
        // write. If the journal append fails, these ahead-of-state rows still
        // route to the correct shard and the hidden objects remain invisible.
        write_object_shard_index_records(&root, &routing_records)?;
        let backend = self.shard_backend_mut(&shard_id)?;
        let entry = backend.commit_prepared_namespace_transaction(
            operation_id,
            expected_operation_digest,
            expected_owner_epoch,
        )?;
        Ok(entry)
    }

    fn commit_prepared_namespace_transaction_on_shard(
        &mut self,
        shard_id: &DfsMetadataShardId,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
    ) -> Result<DfsMetadataJournalEntry> {
        let routing_records = self.namespace_prepare_routing_records(shard_id, operation_id)?;
        let root = self.root.clone();
        write_object_shard_index_records(&root, &routing_records)?;
        let backend = self.shard_backend_mut(shard_id)?;
        let entry = backend.commit_prepared_namespace_transaction(
            operation_id,
            expected_operation_digest,
            expected_owner_epoch,
        )?;
        Ok(entry)
    }

    fn abort_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.unique_namespace_prepare_shard_id(operation_id, true)?;
        self.shard_backend_mut(&shard_id)?
            .abort_prepared_namespace_transaction(
                operation_id,
                expected_operation_digest,
                expected_owner_epoch,
            )
    }

    fn evict_object_record_for_foreign_shard_move(
        &mut self,
        moved_object: &DfsObjectMetadata,
        source_path: &str,
    ) -> Result<()> {
        let target_shard_id =
            self.select_shard_id_for_path(&moved_object.file_system_id, &moved_object.path)?;
        let source_shard_id =
            self.select_shard_id_for_path(&moved_object.file_system_id, source_path)?;
        if source_shard_id != target_shard_id {
            self.put_cross_shard_tombstone(moved_object, &source_shard_id, &target_shard_id)?;
        }
        self.write_object_shard_index(moved_object, &target_shard_id)?;
        Ok(())
    }

    fn evict_object_record_for_foreign_shard_move_with_pin_fence(
        &mut self,
        moved_object: &DfsObjectMetadata,
        source_path: &str,
        expected_source_object: &DfsObjectMetadata,
        expected_source_journal_sequence: u64,
        expected_version_pins: &[DfsMetadataVersionPinRecord],
        pin_transfer_id: Option<&str>,
    ) -> Result<()> {
        let target_shard_id =
            self.select_shard_id_for_path(&moved_object.file_system_id, &moved_object.path)?;
        let source_shard_id =
            self.select_shard_id_for_path(&moved_object.file_system_id, source_path)?;
        if source_shard_id == target_shard_id {
            self.write_object_shard_index(moved_object, &target_shard_id)?;
            return Ok(());
        }
        let (source_lock, bundle) =
            self.object_shard_migration_bundle(&source_shard_id, &moved_object.object_id)?;
        if &bundle.object != expected_source_object {
            return Err(MetadataBackendError::Serde(format!(
                "source shard {} object {} generation changed before pinned eviction",
                source_shard_id.0, moved_object.object_id
            )));
        }
        if bundle.source_journal_sequence != expected_source_journal_sequence {
            return Err(MetadataBackendError::Serde(format!(
                "source shard {} advanced from acknowledged sequence {} to {} before pinned object {} eviction",
                source_shard_id.0,
                expected_source_journal_sequence,
                bundle.source_journal_sequence,
                moved_object.object_id
            )));
        }
        let mut current_pins = bundle.version_pins.clone();
        current_pins.sort();
        let mut expected_pins = expected_version_pins.to_vec();
        expected_pins.sort();
        if current_pins != expected_pins {
            return Err(MetadataBackendError::Serde(format!(
                "source shard {} durable pin set changed before object {} eviction",
                source_shard_id.0, moved_object.object_id
            )));
        }
        self.write_object_shard_index(moved_object, &target_shard_id)?;
        let eviction = self
            .shard_backend_mut(&source_shard_id)?
            .put_cross_shard_tombstone_if_unchanged_while_locked(
                &source_lock,
                &bundle.object,
                moved_object,
                &source_shard_id,
                &target_shard_id,
                bundle.source_journal_sequence,
                pin_transfer_id,
            )?;
        match eviction {
            crate::metadata_backend::DfsConditionalSourceEviction::Applied { .. }
            | crate::metadata_backend::DfsConditionalSourceEviction::AlreadyApplied => Ok(()),
            crate::metadata_backend::DfsConditionalSourceEviction::JournalAdvanced => {
                Err(MetadataBackendError::Serde(format!(
                    "source shard {} advanced while evicting pinned object {}",
                    source_shard_id.0, moved_object.object_id
                )))
            }
            crate::metadata_backend::DfsConditionalSourceEviction::SourceChanged => {
                Err(MetadataBackendError::Serde(format!(
                    "source shard {} object {} changed while evicting pinned family",
                    source_shard_id.0, moved_object.object_id
                )))
            }
        }
    }

    fn tombstone_compaction_projection(
        &self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        let mut report = DfsTombstoneCompactionReport::for_options(&options);
        let now = options
            .now_unix_nanos
            .unwrap_or_else(metadata_now_unix_nanos);
        for (shard_id, slot) in &self.shards {
            if !self.shard_is_local_authoritative_primary(shard_id)
                || self.metadata_shard_authoritative_writes_frozen(shard_id)?
            {
                continue;
            }
            let backend = slot.backend()?;
            let tables = backend.current_tables_snapshot()?;
            report.merge(plan_tombstone_compaction(&tables, &options, now).report);
        }
        Ok(report)
    }

    fn metadata_history_gc_projection(
        &self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<DfsMetadataHistoryGcReport> {
        let mut report = DfsMetadataHistoryGcReport::for_options(&options);
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            let mut shard_report = backend.metadata_history_gc_projection(options.clone())?;
            if self.shard_is_local_authoritative_primary(shard_id)
                && !self.metadata_shard_authoritative_writes_frozen(shard_id)?
            {
                report.merge(shard_report);
            } else {
                // Replica and handoff-frozen tables still consume local disk and
                // must remain visible in the per-shard inventory. Do not merge
                // their eligible/deadline counters into schedulable work:
                // authoritative deletion is deliberately fenced on these
                // shards and advertising it would create a reactive busy loop.
                for shard in &mut shard_report.shards {
                    shard.collection_authorized = false;
                }
                report.shards.extend(shard_report.shards);
            }
        }
        Ok(report)
    }

    fn gc_metadata_history(
        &mut self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<DfsMetadataHistoryGcReport> {
        let mut report = DfsMetadataHistoryGcReport::for_options(&options);
        let shard_ids = self.shard_ids();
        for shard_id in shard_ids {
            if !self.shard_is_local_authoritative_primary(&shard_id)
                || self.metadata_shard_authoritative_writes_frozen(&shard_id)?
            {
                continue;
            }
            let mut shard_options = options.clone();
            if !options.dry_run
                && let Some(max_removals) = options.max_removals
            {
                let removed = report.applied_record_count();
                let remaining = max_removals.saturating_sub(removed);
                if remaining == 0 {
                    report.max_removals_reached = true;
                    break;
                }
                shard_options.max_removals = Some(remaining);
            }
            match self
                .shard_backend_mut(&shard_id)?
                .gc_metadata_history_authoritative(shard_options)
            {
                Ok(shard_report) => report.merge(shard_report),
                Err(MetadataBackendError::MembershipHandoffFrozen { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(report)
    }

    fn metadata_content_protection(&self) -> Result<DfsMetadataContentProtection> {
        let mut protection = DfsMetadataContentProtection::default();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            protection.merge(backend.metadata_content_protection()?);
        }
        Ok(protection)
    }

    fn content_root_catalog_page(
        &mut self,
        request: &DfsContentRootCatalogPageRequest,
    ) -> Result<DfsContentRootCatalogPage> {
        request.validate()?;
        let observed_identity = self
            .shard_configs
            .get(&request.identity.shard_id.0)
            .map(DfsContentRootCatalogShardIdentity::from_config);
        let Some(observed_identity) = observed_identity else {
            return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                request,
                DfsContentRootCatalogIncompleteReason::UnknownShard,
                metadata_now_unix_nanos(),
            ));
        };
        if observed_identity != request.identity {
            return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                request,
                DfsContentRootCatalogIncompleteReason::AuthorityChanged {
                    observed: Some(observed_identity),
                },
                metadata_now_unix_nanos(),
            ));
        }
        self.shard_backend_mut(&request.identity.shard_id)?
            .content_root_catalog_page(request)
    }

    fn compact_tombstones(
        &mut self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        let mut report = DfsTombstoneCompactionReport::for_options(&options);
        let shard_ids = self.shard_ids();
        let mut compacted_object_ids = Vec::new();
        for shard_id in shard_ids {
            if !self.shard_is_local_authoritative_primary(&shard_id)
                || self.metadata_shard_authoritative_writes_frozen(&shard_id)?
            {
                // A retiring primary deliberately keeps its local shard
                // frozen after activating the successor generation. That
                // shard remains available for replica ingest, but optional
                // lifecycle mutation must leave it untouched. Independent
                // active shards must still reach metadata publication later
                // in the same maintenance tick.
                continue;
            }
            let mut shard_options = options.clone();
            if !options.dry_run
                && let Some(max_compactions) = options.max_compactions
            {
                let remaining = max_compactions.saturating_sub(report.compacted_tombstone_count);
                if remaining == 0 {
                    report.max_compactions_reached = true;
                    break;
                }
                shard_options.max_compactions = Some(remaining);
            }
            let compacted = self
                .shard_backend_mut(&shard_id)?
                .compact_tombstones_with_ids(shard_options);
            let (shard_report, shard_compacted_object_ids) = match compacted {
                Ok(compacted) => compacted,
                Err(MetadataBackendError::MembershipHandoffFrozen { .. }) => {
                    // A handoff can install its freeze after the read above.
                    // Preserve the same safe skip across that race.
                    continue;
                }
                Err(error) => return Err(error),
            };
            report.merge(shard_report);
            compacted_object_ids.extend(shard_compacted_object_ids);
        }
        for object_id in compacted_object_ids {
            self.remove_object_shard_index(&object_id)?;
        }
        Ok(report)
    }

    fn put_namespace_job(&mut self, job: &DfsNamespaceJobRecord) -> Result<()> {
        let shard_id = self.select_shard_id_for_path(&job.file_system_id, &job.root_path)?;
        self.shard_backend_mut(&shard_id)?.put_namespace_job(job)
    }

    fn put_namespace_job_receipt(&mut self, receipt: &DfsNamespaceJobReceipt) -> Result<()> {
        let shard_id =
            self.select_shard_id_for_path(&receipt.file_system_id, &receipt.root_path)?;
        self.shard_backend_mut(&shard_id)?
            .put_namespace_job_receipt(receipt)
    }

    fn put_namespace_request_on_shard(
        &mut self,
        shard_id: &DfsMetadataShardId,
        request: &DfsNamespaceRequestRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let admission = self.namespace_request_admission_snapshot(
            shard_id,
            &request.requester_node_id,
            &request.request_id,
        )?;
        let durable_shard_id = admission
            .request
            .map(|existing| existing.authority_shard_id)
            .or_else(|| admission.receipt.map(|receipt| receipt.authority_shard_id));
        let history_expired = if durable_shard_id.is_none() && admission.prepared.is_some() {
            // Preserve the legacy write-path rule: a hidden prepare does not
            // itself bypass an already-evicted request identity.
            self.namespace_request_history_expired_on_shard(
                shard_id,
                &request.requester_node_id,
                &request.request_id,
            )?
        } else {
            admission.history_expired
        };
        if durable_shard_id.is_none() && history_expired {
            return Err(MetadataBackendError::Serde(format!(
                "namespace request {}/{} idempotency history expired or is unknown",
                request.requester_node_id, request.request_id
            )));
        }
        let target_shard_id = durable_shard_id.as_ref().unwrap_or(shard_id);
        self.shard_backend_mut(target_shard_id)?
            .put_namespace_request_on_shard(target_shard_id, request)
    }

    fn get_namespace_request(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespaceRequestRecord>> {
        let mut found = None;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(request) = backend.get_namespace_request(requester_node_id, request_id)? {
                if found.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace request {requester_node_id}/{request_id} exists on multiple local shards"
                    )));
                }
                found = Some(request);
            }
        }
        Ok(found)
    }

    fn get_namespace_request_receipt(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespaceRequestReceipt>> {
        let mut found = None;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(receipt) =
                backend.get_namespace_request_receipt(requester_node_id, request_id)?
            {
                if found.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace request receipt {requester_node_id}/{request_id} exists on multiple local shards"
                    )));
                }
                found = Some(receipt);
            }
        }
        Ok(found)
    }

    fn namespace_request_history_expired_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<bool> {
        // Validate the selected current authority, but consult every retained
        // local shard history. A prefix topology change can select a different
        // shard after an exact receipt was evicted; admitting against only the
        // new shard's floor would make the old request fresh again. Current
        // remote primaries are represented here by their local replica state,
        // and same-node topology changes retain both shard histories.
        self.shard_backend(shard_id)?;
        for (history_shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            if backend.namespace_request_history_expired_on_shard(
                history_shard_id,
                requester_node_id,
                request_id,
            )? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn namespace_request_admission_snapshot(
        &mut self,
        shard_id: &DfsMetadataShardId,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<DfsNamespaceRequestAdmissionSnapshot> {
        self.shard_backend(shard_id)?;
        let mut request = None;
        let mut receipt = None;
        let mut prepared = None;
        let mut history_expired = false;
        for (history_shard_id, slot) in &mut self.shards {
            let backend = slot.backend_mut()?;
            let local = backend.namespace_request_admission_snapshot(
                history_shard_id,
                requester_node_id,
                request_id,
            )?;
            if let Some(found) = local.request {
                if request.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace request {requester_node_id}/{request_id} exists on multiple local shards"
                    )));
                }
                request = Some(found);
            }
            if let Some(found) = local.receipt {
                if receipt.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace request receipt {requester_node_id}/{request_id} exists on multiple local shards"
                    )));
                }
                receipt = Some(found);
            }
            if let Some(found) = local.prepared {
                if prepared.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "pending namespace request {requester_node_id}/{request_id} owns prepared transactions on multiple local shards"
                    )));
                }
                prepared = Some(found);
            }
            history_expired |= local.history_expired;
        }
        if request.is_some() || receipt.is_some() || prepared.is_some() {
            history_expired = false;
        }
        Ok(DfsNamespaceRequestAdmissionSnapshot {
            request,
            receipt,
            prepared,
            history_expired,
        })
    }

    fn list_namespace_requests(&self) -> Result<Vec<DfsNamespaceRequestRecord>> {
        let mut requests = BTreeMap::new();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            for request in backend.list_namespace_requests()? {
                let key = (
                    request.requester_node_id.clone(),
                    request.request_id.clone(),
                );
                if requests.insert(key, request).is_some() {
                    return Err(MetadataBackendError::Serde(
                        "namespace request exists on multiple local shards".to_string(),
                    ));
                }
            }
        }
        Ok(requests.into_values().collect())
    }

    fn list_pending_namespace_requests_page(
        &self,
        after: Option<&DfsPendingNamespaceRequestPageCursor>,
        max_entries: usize,
    ) -> Result<DfsPendingNamespaceRequestPage> {
        if max_entries == 0 {
            return Ok(DfsPendingNamespaceRequestPage {
                requests: Vec::new(),
                next_cursor: None,
            });
        }
        let mut requests_by_identity =
            BTreeMap::<(NodeId, String), DfsNamespaceRequestRecord>::new();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            let page = backend
                .list_pending_namespace_requests_page(after, max_entries.saturating_add(1))?;
            for request in page.requests {
                let request_key = (
                    request.requester_node_id.clone(),
                    request.request_id.clone(),
                );
                if requests_by_identity.insert(request_key, request).is_some() {
                    return Err(MetadataBackendError::Serde(
                        "pending namespace request exists on multiple local shards".to_string(),
                    ));
                }
            }
        }
        let entries = requests_by_identity
            .into_values()
            .map(|request| {
                let cursor = DfsPendingNamespaceRequestPageCursor {
                    updated_at: request.updated_at,
                    requester_node_id: request.requester_node_id.clone(),
                    request_id: request.request_id.clone(),
                };
                (request, cursor)
            })
            .collect::<Vec<_>>();
        let (requests, next_cursor) =
            finish_sorted_cursor_page(entries, max_entries, |left, right| {
                (left.updated_at, &left.requester_node_id, &left.request_id).cmp(&(
                    right.updated_at,
                    &right.requester_node_id,
                    &right.request_id,
                ))
            });
        Ok(DfsPendingNamespaceRequestPage {
            requests,
            next_cursor,
        })
    }

    fn get_prepared_namespace_prepare_for_request(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespacePrepareRecord>> {
        let mut found = None;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            let Some(prepare) = backend
                .get_prepared_namespace_prepare_for_request(requester_node_id, request_id)?
            else {
                continue;
            };
            if found.is_some() {
                return Err(MetadataBackendError::Serde(format!(
                    "pending namespace request {requester_node_id}/{request_id} owns prepared transactions on multiple local shards"
                )));
            }
            found = Some(prepare);
        }
        Ok(found)
    }

    fn get_namespace_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Option<DfsNamespaceTransaction>> {
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(transaction) = backend.get_namespace_transaction(transaction_id)? {
                return Ok(Some(transaction));
            }
        }
        Ok(None)
    }

    fn get_namespace_prepare(
        &self,
        operation_id: &str,
    ) -> Result<Option<DfsNamespacePrepareRecord>> {
        let mut found = None;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(prepare) = backend.get_namespace_prepare(operation_id)? {
                if found.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {operation_id} exists on multiple local shards"
                    )));
                }
                found = Some(prepare);
            }
        }
        Ok(found)
    }

    fn list_prepared_namespace_prepares(&self) -> Result<Vec<DfsNamespacePrepareRecord>> {
        let mut prepares = BTreeMap::new();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            for prepare in backend.list_prepared_namespace_prepares()? {
                if prepares
                    .insert(prepare.operation_id.clone(), prepare.clone())
                    .is_some()
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {} exists on multiple local shards",
                        prepare.operation_id
                    )));
                }
            }
        }
        Ok(prepares.into_values().collect())
    }

    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        let mut found = None;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(job) = backend.get_namespace_job(job_id)? {
                if found.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace job {job_id} exists on multiple local shards"
                    )));
                }
                found = Some(job);
            }
        }
        Ok(found)
    }

    fn get_namespace_job_receipt(&self, job_id: &str) -> Result<Option<DfsNamespaceJobReceipt>> {
        let mut found = None;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(receipt) = backend.get_namespace_job_receipt(job_id)? {
                if found.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace job receipt {job_id} exists on multiple local shards"
                    )));
                }
                found = Some(receipt);
            }
        }
        Ok(found)
    }

    fn completed_namespace_job_receipt_for_paths_since(
        &self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: &str,
        kinds: &[DfsNamespaceJobKind],
        created_at: time::OffsetDateTime,
        origin: &DfsNamespaceJobOrigin,
    ) -> Result<Option<DfsNamespaceJobReceipt>> {
        let mut found = None;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            let Some(receipt) = backend.completed_namespace_job_receipt_for_paths_since(
                file_system_id,
                root_path,
                target_path,
                kinds,
                created_at,
                origin,
            )?
            else {
                continue;
            };
            if found.is_some() {
                return Err(MetadataBackendError::Serde(format!(
                    "completed namespace job receipt for {}/{}/{} exists on multiple local shards",
                    origin.requester_node_id, origin.request_id, origin.operation_id
                )));
            }
            found = Some(receipt);
        }
        Ok(found)
    }

    fn list_namespace_jobs(&self) -> Result<Vec<DfsNamespaceJobRecord>> {
        let mut jobs = BTreeMap::<String, DfsNamespaceJobRecord>::new();
        walk_metadata_pages(|cursor| {
            let page = self.list_namespace_jobs_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            for job in page.jobs {
                jobs.insert(job.job_id.clone(), job);
            }
            Ok(page.next_cursor)
        })?;
        Ok(jobs.into_values().collect())
    }

    fn list_namespace_jobs_page(
        &self,
        after: Option<&DfsNamespaceJobPageCursor>,
        max_entries: usize,
    ) -> Result<DfsNamespaceJobPage> {
        if max_entries == 0 {
            return Ok(DfsNamespaceJobPage {
                jobs: Vec::new(),
                next_cursor: None,
            });
        }
        let mut merged = BTreeMap::<String, DfsNamespaceJobRecord>::new();
        let mut shard_has_more = false;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            let page = backend.list_namespace_jobs_page(after, max_entries.saturating_add(1))?;
            shard_has_more |= page.next_cursor.is_some();
            for job in page.jobs {
                merged.insert(job.job_id.clone(), job);
            }
        }
        let mut jobs = merged.into_values().collect::<Vec<_>>();
        let has_more = jobs.len() > max_entries || shard_has_more;
        if jobs.len() > max_entries {
            jobs.truncate(max_entries);
        }
        let next_cursor = if has_more {
            jobs.last().map(|job| DfsNamespaceJobPageCursor {
                job_id: job.job_id.clone(),
            })
        } else {
            None
        };
        Ok(DfsNamespaceJobPage { jobs, next_cursor })
    }

    fn metadata_summary(&self) -> Result<DfsMetadataSummary> {
        let mut summary = DfsMetadataSummary::default();
        let mut file_system_ids = BTreeSet::new();
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            let tables = backend.current_tables_snapshot()?;
            file_system_ids.extend(tables.file_systems.keys().cloned());
            let shard_summary = metadata_summary_from_tables(&tables, |object| {
                self.object_is_listable_from_shard(object, shard_id)
            })?;
            accumulate_metadata_summary_detail_counts(&mut summary, &shard_summary);
        }
        summary.file_system_count = file_system_ids.len();
        Ok(summary)
    }

    fn metadata_summary_for_local_primary_shards(&self) -> Result<DfsMetadataSummary> {
        let mut summary = DfsMetadataSummary::default();
        let mut file_system_ids = BTreeSet::new();
        for (shard_id, slot) in &self.shards {
            if !self.shard_is_local_authoritative_primary(shard_id) {
                continue;
            }
            if let Some(config) = self.shard_configs.get(&shard_id.0) {
                file_system_ids.insert(config.file_system_id.clone());
            }
            let backend = slot.backend()?;
            let tables = backend.current_tables_snapshot()?;
            let shard_summary = metadata_summary_from_tables(&tables, |object| {
                self.object_is_listable_from_shard(object, shard_id)
            })?;
            accumulate_metadata_summary_detail_counts(&mut summary, &shard_summary);
        }
        summary.file_system_count = file_system_ids.len();
        Ok(summary)
    }

    fn metadata_status_summaries(&self) -> Result<(DfsMetadataSummary, DfsMetadataSummary)> {
        let mut summary = DfsMetadataSummary::default();
        let mut primary_summary = DfsMetadataSummary::default();
        let mut file_system_ids = BTreeSet::new();
        let mut primary_file_system_ids = BTreeSet::new();
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            let shard_summary = backend.resident_metadata_status_summary();
            if shard_summary.file_system_count > 0
                && let Some(config) = self.shard_configs.get(&shard_id.0)
            {
                file_system_ids.insert(config.file_system_id.clone());
            }
            accumulate_metadata_summary_detail_counts(&mut summary, &shard_summary);
            if self.shard_is_local_authoritative_primary(shard_id) {
                if let Some(config) = self.shard_configs.get(&shard_id.0) {
                    // Primary-owned operational status counts the configured
                    // filesystem even before its first metadata object is
                    // materialized. Keep this identical to
                    // `metadata_summary_for_local_primary_shards` without
                    // reopening or scanning the shard tables.
                    primary_file_system_ids.insert(config.file_system_id.clone());
                }
                accumulate_metadata_summary_detail_counts(&mut primary_summary, &shard_summary);
            }
        }
        summary.file_system_count = file_system_ids.len();
        primary_summary.file_system_count = primary_file_system_ids.len();
        if !self.fallback_routing_indexes.is_empty()
            && (summary.object_count > 0
                || summary.pending_repair_count > 0
                || summary.cross_shard_namespace_lock_count > 0)
        {
            let generation_count = self.fallback_routing_indexes.len();
            let omission = if summary.cross_shard_namespace_lock_count == 0 {
                DfsOperatorStatusOmission::historical_routing_topology_with_cross_shard_lock_zero_proof(
                    generation_count,
                )
            } else {
                DfsOperatorStatusOmission::historical_routing_topology(generation_count)
            };
            return Err(MetadataBackendError::StatusOmitted { omission });
        }
        Ok((summary, primary_summary))
    }

    fn metadata_summary_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsMetadataSummary>> {
        let Some(_) = self.get_file_system(file_system_id)? else {
            return Ok(None);
        };
        let mut summary = DfsMetadataSummary {
            file_system_count: 1,
            ..DfsMetadataSummary::default()
        };
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            let tables = backend.current_tables_snapshot()?;
            let Some(shard_summary) =
                metadata_summary_for_file_system_from_tables(&tables, file_system_id, |object| {
                    self.object_is_listable_from_shard(object, shard_id)
                })?
            else {
                continue;
            };
            accumulate_metadata_summary_detail_counts(&mut summary, &shard_summary);
        }
        Ok(Some(summary))
    }

    fn put_conflict(&mut self, conflict: &DfsConflictRecord) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.require_shard_id_for_object(&conflict.object_id)?;
        self.shard_backend_mut(&shard_id)?.put_conflict(conflict)
    }

    fn get_conflict(
        &self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend(&shard_id)?
            .get_conflict(object_id, conflict_id)
    }

    fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsConflictRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(Vec::new());
        };
        let backend = self.shard_backend(&shard_id)?;
        collect_metadata_pages(|cursor| {
            let page = backend.conflicts_for_object_page(
                object_id,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.conflicts, page.next_cursor))
        })
    }

    fn conflicts_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsConflictPageCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictPage> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(DfsConflictPage {
                conflicts: Vec::new(),
                next_cursor: None,
            });
        };
        self.shard_backend(&shard_id)?
            .conflicts_for_object_page(object_id, after, max_entries)
    }

    fn remove_conflict(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend_mut(&shard_id)?
            .remove_conflict(object_id, conflict_id)
    }

    fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend_mut(&shard_id)?
            .update_conflict_status(object_id, conflict_id, status)
    }

    fn journal_since(&self, after_sequence: u64) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut entries = Vec::new();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            entries.extend(backend.journal_since(after_sequence)?);
        }
        entries.sort_by(|left, right| {
            left.sequence
                .cmp(&right.sequence)
                .then_with(|| left.shard_id.cmp(&right.shard_id))
        });
        Ok(entries)
    }

    fn journal_batch_for_shard_since_limited(
        &self,
        shard_id: &DfsMetadataShardId,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        self.shard_backend(shard_id)?.journal_batch_since_limited(
            after_sequence,
            max_entries,
            max_encoded_bytes,
        )
    }

    fn journal_frames_for_batch(
        &self,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<Vec<DfsMetadataJournalFrameRecord>> {
        DfsMetadataBackend::journal_frames_for_batch(self.shard_backend(&batch.shard_id)?, batch)
    }

    fn export_shard_table_snapshot(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataTableSnapshot>> {
        Ok(Some(LocalSobjMetadataBackend::export_table_snapshot(
            self.shard_backend(shard_id)?,
        )))
    }

    fn metadata_prefix_proof(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataPrefixProof>> {
        self.shard_backend(shard_id)?
            .metadata_prefix_proof()
            .map(Some)
    }

    fn refresh_metadata_shard_durable_view(&mut self, shard_id: &DfsMetadataShardId) -> Result<()> {
        self.refresh_metadata_shard_durable_view_and_reconcile(shard_id, true)?;
        Ok(())
    }

    fn refresh_metadata_shard_durable_view_if_changed(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        self.refresh_metadata_shard_durable_view_and_reconcile(shard_id, false)
    }

    fn metadata_shard_write_freeze(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        self.shard_backend(shard_id)?
            .metadata_shard_write_freeze(shard_id)
    }

    fn metadata_shard_authoritative_writes_frozen(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        self.shard_backend(shard_id)?
            .metadata_shard_authoritative_writes_frozen(shard_id)
    }

    fn freeze_metadata_shard_for_membership_handoff(
        &mut self,
        shard_id: &DfsMetadataShardId,
        handoff_id: &str,
        retiring_epoch: u64,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        self.shard_backend_mut(shard_id)?
            .freeze_metadata_shard_for_membership_handoff(shard_id, handoff_id, retiring_epoch)
    }

    fn mark_metadata_shard_membership_handoff_activated(
        &mut self,
        shard_id: &DfsMetadataShardId,
        handoff_id: &str,
        activated_epoch: u64,
    ) -> Result<bool> {
        self.shard_backend_mut(shard_id)?
            .mark_metadata_shard_membership_handoff_activated(shard_id, handoff_id, activated_epoch)
    }

    fn clear_metadata_shard_membership_handoff_freeze(
        &mut self,
        shard_id: &DfsMetadataShardId,
        handoff_id: &str,
        active_shard: &DfsMetadataShardConfig,
    ) -> Result<bool> {
        if active_shard.shard_id != shard_id.0 {
            return Err(MetadataBackendError::Serde(format!(
                "active metadata shard config {} does not match rebound shard {}",
                active_shard.shard_id, shard_id.0
            )));
        }
        let previous = self
            .shard_configs
            .get(&shard_id.0)
            .cloned()
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "cannot rebind unconfigured metadata shard {}",
                    shard_id.0
                ))
            })?;
        if previous.file_system_id != active_shard.file_system_id
            || previous.assignment != active_shard.assignment
            || previous.default_policy != active_shard.default_policy
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} rebind changes immutable routing or policy configuration",
                shard_id.0
            )));
        }

        self.shard_configs
            .insert(shard_id.0.clone(), active_shard.clone());
        let rebound = match self.shard_backend_mut(shard_id) {
            Ok(backend) => backend.clear_metadata_shard_membership_handoff_freeze(
                shard_id,
                handoff_id,
                active_shard,
            ),
            Err(error) => Err(error),
        };
        match rebound {
            Ok(true) => Ok(true),
            Ok(false) => {
                self.shard_configs.insert(shard_id.0.clone(), previous);
                Ok(false)
            }
            Err(error) => {
                self.shard_configs.insert(shard_id.0.clone(), previous);
                Err(error)
            }
        }
    }

    fn export_table_snapshot(&self) -> Result<DfsMetadataTableSnapshot> {
        let mut tables = DfsMetadataTableSnapshotTables::default();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(snapshot_tables) =
                LocalSobjMetadataBackend::export_table_snapshot(backend).tables
            {
                tables.file_systems.extend(snapshot_tables.file_systems);
                tables.objects.extend(snapshot_tables.objects);
                tables
                    .file_system_objects
                    .extend(snapshot_tables.file_system_objects);
                tables.paths.extend(snapshot_tables.paths);
                tables.inodes.extend(snapshot_tables.inodes);
                tables.dirfrags.extend(snapshot_tables.dirfrags);
                tables.dentries.extend(snapshot_tables.dentries);
                tables.hardlinks.extend(snapshot_tables.hardlinks);
                tables
                    .compacted_tombstones
                    .extend(snapshot_tables.compacted_tombstones);
                tables.versions.extend(snapshot_tables.versions);
                tables.version_pins.extend(snapshot_tables.version_pins);
                tables.receipts.extend(snapshot_tables.receipts);
                tables.intents.extend(snapshot_tables.intents);
                tables
                    .intent_receipts
                    .extend(snapshot_tables.intent_receipts);
                tables
                    .intent_receipt_retention
                    .merge_conservative(snapshot_tables.intent_receipt_retention);
                tables.conflicts.extend(snapshot_tables.conflicts);
                tables
                    .advisory_lock_states
                    .extend(snapshot_tables.advisory_lock_states);
                tables
                    .visible_commits
                    .extend(snapshot_tables.visible_commits);
                tables
                    .namespace_transactions
                    .extend(snapshot_tables.namespace_transactions);
                tables
                    .namespace_transaction_receipts
                    .extend(snapshot_tables.namespace_transaction_receipts);
                tables
                    .namespace_prepares
                    .extend(snapshot_tables.namespace_prepares);
                tables
                    .namespace_prepare_receipts
                    .extend(snapshot_tables.namespace_prepare_receipts);
                tables.namespace_jobs.extend(snapshot_tables.namespace_jobs);
                tables
                    .namespace_job_receipts
                    .extend(snapshot_tables.namespace_job_receipts);
                tables
                    .namespace_request_receipts
                    .extend(snapshot_tables.namespace_request_receipts);
                tables
                    .namespace_requests
                    .extend(snapshot_tables.namespace_requests);
                tables
                    .namespace_receipt_retention
                    .merge_conservative(snapshot_tables.namespace_receipt_retention);
            }
        }
        Ok(DfsMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            materialized_through_sequence: None,
            tables: Some(Box::new(tables)),
            ops: Vec::new(),
            semantic_state_fingerprint: None,
            prefix_proof: None,
        })
    }

    fn metadata_replication_cursor(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<u64> {
        self.shard_backend(shard_id)?
            .metadata_replication_cursor(target_node_id, shard_id)
    }

    fn put_metadata_replication_cursor(
        &mut self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        next_after_sequence: u64,
    ) -> Result<()> {
        self.shard_backend_mut(shard_id)?
            .put_metadata_replication_cursor(target_node_id, shard_id, next_after_sequence)
    }

    fn metadata_prefix_ack_floor(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataPrefixAckFloor>> {
        self.shard_backend(shard_id)?
            .metadata_prefix_ack_floor(shard_id)
    }

    fn put_metadata_prefix_ack_floor(&mut self, floor: &DfsMetadataPrefixAckFloor) -> Result<()> {
        self.shard_backend_mut(&floor.shard_id)?
            .put_metadata_prefix_ack_floor(floor)
    }

    fn metadata_authority_ack_floor(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataAuthorityAckFloor>> {
        self.shard_backend(shard_id)?
            .metadata_authority_ack_floor(shard_id)
    }

    fn put_metadata_authority_ack_floor(
        &mut self,
        floor: &DfsMetadataAuthorityAckFloor,
    ) -> Result<()> {
        self.shard_backend_mut(&floor.shard_id)?
            .put_metadata_authority_ack_floor(floor)
    }

    fn metadata_replay_cursor(&self, source_shard_id: &DfsMetadataShardId) -> Result<u64> {
        self.shard_backend(source_shard_id)?
            .metadata_replay_cursor(source_shard_id)
    }

    fn metadata_shard_statuses(&self) -> Result<Vec<DfsMetadataShardStatus>> {
        let mut statuses = Vec::new();
        for shard in self.shard_configs.values() {
            let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
            let backend = self.shard_backend(&shard_id)?;
            let journal_last_sequence = backend.journal_last_sequence();
            let journal_next_sequence = journal_last_sequence.saturating_add(1);
            let mut replica_lag = Vec::new();
            for target_node_id in &shard.replica_node_ids {
                let replicated_through_sequence =
                    backend.metadata_replication_cursor(target_node_id, &shard_id)?;
                replica_lag.push(DfsMetadataReplicaLagStatus {
                    target_node_id: target_node_id.clone(),
                    replicated_through_sequence,
                    lag_entries: journal_last_sequence.saturating_sub(replicated_through_sequence),
                });
            }
            statuses.push(DfsMetadataShardStatus {
                shard_id,
                file_system_id: Some(shard.file_system_id.clone()),
                primary_node_id: Some(shard.primary_node_id.clone()),
                replica_node_ids: shard.replica_node_ids.clone(),
                epoch: Some(shard.epoch),
                journal_next_sequence,
                journal_last_sequence,
                replica_lag,
                replay_cursors: backend.replay_cursor_statuses()?,
            });
        }
        Ok(statuses)
    }

    fn apply_journal_batch(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize> {
        let affected_object_ids = journal_batch_visible_object_placement_ids(batch);
        let applied = self
            .shard_backend_mut(&batch.shard_id)?
            .apply_journal_batch(batch)?;
        self.reconcile_object_shard_indexes(affected_object_ids.iter())?;
        Ok(applied)
    }

    fn apply_journal_batch_with_replica_permit(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<usize> {
        self.require_replica_ingest_permit_for_shard(permit, &batch.shard_id)?;
        let affected_object_ids = journal_batch_visible_object_placement_ids(batch);
        let applied = self
            .shard_backend_mut(&batch.shard_id)?
            .apply_journal_batch_with_replica_permit(permit, batch)?;
        self.reconcile_object_shard_indexes(affected_object_ids.iter())?;
        Ok(applied)
    }

    fn apply_journal_batch_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.require_replica_ingest_permit_for_shard(permit, &batch.shard_id)?;
        let affected_object_ids = journal_batch_visible_object_placement_ids(batch);
        let report = self
            .shard_backend_mut(&batch.shard_id)?
            .apply_journal_batch_with_replica_permit_report(permit, batch)?;
        self.reconcile_object_shard_indexes(affected_object_ids.iter())?;
        Ok(report)
    }

    fn persist_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.require_replica_ingest_permit_for_shard(permit, &batch.shard_id)?;
        self.shard_backend_mut(&batch.shard_id)?
            .persist_journal_frames_with_replica_permit_report(permit, batch, frames)
    }

    fn persist_authority_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
        expected_target_prefix: &DfsMetadataPrefixProof,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.require_replica_ingest_permit_for_shard(permit, &batch.shard_id)?;
        self.shard_backend_mut(&batch.shard_id)?
            .persist_authority_journal_frames_with_replica_permit_report(
                permit,
                batch,
                frames,
                expected_target_prefix,
            )
    }

    fn replace_with_replicated_snapshot(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        let mut affected_object_ids = self
            .shard_backend(&source_shard_id)?
            .export_table_snapshot()
            .tables
            .into_iter()
            .flat_map(|tables| tables.objects)
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        if let Some(tables) = snapshot.tables.as_ref() {
            affected_object_ids
                .extend(tables.objects.iter().map(|object| object.object_id.clone()));
        }
        let archived = self
            .shard_backend_mut(&source_shard_id)?
            .replace_with_replicated_snapshot(
                source_shard_id,
                replayed_through_sequence,
                snapshot,
            )?;
        self.reconcile_object_shard_indexes(affected_object_ids.iter())?;
        Ok(archived)
    }

    fn replace_with_replicated_snapshot_with_replica_permit(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        self.require_replica_ingest_permit_for_shard(permit, &source_shard_id)?;
        let mut affected_object_ids = self
            .shard_backend(&source_shard_id)?
            .export_table_snapshot()
            .tables
            .into_iter()
            .flat_map(|tables| tables.objects)
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        if let Some(tables) = snapshot.tables.as_ref() {
            affected_object_ids
                .extend(tables.objects.iter().map(|object| object.object_id.clone()));
        }
        let archived = self
            .shard_backend_mut(&source_shard_id)?
            .replace_with_replicated_snapshot_with_replica_permit(
                permit,
                source_shard_id,
                replayed_through_sequence,
                snapshot,
            )?;
        self.reconcile_object_shard_indexes(affected_object_ids.iter())?;
        Ok(archived)
    }

    fn subscribe(&self, after_sequence: u64) -> DfsMetadataSubscription {
        let shard_id = self
            .shards
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| DfsMetadataShardId::new("default"));
        DfsMetadataSubscription::new(shard_id, after_sequence)
    }

    fn poll_subscription(
        &self,
        subscription: &mut DfsMetadataSubscription,
    ) -> Result<DfsMetadataJournalBatch> {
        self.shard_backend(&subscription.shard_id)?
            .poll_subscription(subscription)
    }
}
