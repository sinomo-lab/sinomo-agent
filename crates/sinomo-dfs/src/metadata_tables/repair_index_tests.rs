//! The derived repair index follows table operations, snapshot restore, journal
//! replay and rollback exactly as a recomputation from rows would.

use super::*;
use crate::metadata_keys::path_record_for_object;
use crate::metadata_records::{DfsMetadataTableSnapshot, DfsReplicaReceiptWithdrawalRecord};
use crate::metadata_summary::RepairRoot;
use crate::metadata_table_store::{
    METADATA_TABLE_SNAPSHOT_FORMAT_VERSION, metadata_tables_from_table_snapshot,
    metadata_tables_snapshot,
};
use crate::{DfsObjectKind, DfsWriteIntentStatus};
use sinomo_core::events::EventId;
use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt};

fn node(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).unwrap()
}

fn owner() -> NodeId {
    node("node_repair_index_owner")
}

fn replica() -> NodeId {
    node("node_repair_index_replica")
}

fn third() -> NodeId {
    node("node_repair_index_third")
}

fn file_system_id() -> DfsFileSystemId {
    DfsFileSystemId::from_legacy_named("fs_repair_index").unwrap()
}

fn object_id() -> DfsObjectId {
    DfsObjectId::from_legacy_named("obj_repair_index").unwrap()
}

fn version(name: &str) -> DfsVersion {
    DfsVersion::from_legacy_named(name).unwrap()
}

fn file_system() -> DfsFileSystem {
    DfsFileSystem {
        catalog_generation: 0,
        retention_policies: Default::default(),
        module_metadata: Default::default(),
        file_system_id: file_system_id(),
        display_name: "repair-index".into(),
        owner_node_id: owner(),
        subtree_policies: Vec::new(),
    }
}

fn object(version_name: &str, hash: &str) -> DfsObjectMetadata {
    DfsObjectMetadata {
        object_id: object_id(),
        file_system_id: file_system_id(),
        path: "/repair/index.bin".into(),
        namespace_link_count: (DfsObjectKind::File).initial_namespace_link_count(),
        kind: DfsObjectKind::File,
        tombstone_provenance: None,
        owner_node_id: owner(),
        current_locations: vec![owner()],
        content_hash: Some(hash.into()),
        content_size_bytes: Some(7),
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: version(version_name),
        policy: DfsObjectPolicy::ReplicatedEventual,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    }
}

fn version_row(object: &DfsObjectMetadata) -> DfsObjectVersionRecord {
    DfsObjectVersionRecord {
        content_durability: None,
        retention: Default::default(),
        module_metadata: Default::default(),
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone(),
        size_bytes: object.content_size_bytes,
        locations: object.current_locations.clone(),
        created_by_event_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
    }
}

/// A committed write at W1 of N for `desired`.
fn committed_intent(
    object: &DfsObjectMetadata,
    intent_id: &str,
    desired: Vec<NodeId>,
) -> DfsWriteIntentRecord {
    let replicas = desired.len();
    DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: intent_id.into(),
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        base_version: None,
        target_version: object.version.clone(),
        coordinator_node_id: owner(),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 0,
        desired_replica_node_ids: desired,
        policy: crate::DfsReplicationPolicy::new(1, replicas).unwrap(),
        status: DfsWriteIntentStatus::Committed,
        content_hash: object.content_hash.clone(),
        content_size_bytes: None,
        object_metadata: Some(object.clone()),
        touch_parent_on_create: false,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: Some(OffsetDateTime::UNIX_EPOCH),
    }
}

fn receipt(object: &DfsObjectMetadata, replica: NodeId, generation: u64) -> DfsMetadataTableOp {
    DfsMetadataTableOp::PutReceipt {
        receipt: DfsReplicaReceiptRecord {
            receipt: DfsWriteReceipt {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                committed_version: object.version.clone(),
                owner_node_id: owner(),
                event_id: EventId::from_legacy_named("evt_repair_index_receipt").unwrap(),
            },
            replica_node_id: replica,
            content_hash: object.content_hash.clone(),
            received_at: OffsetDateTime::UNIX_EPOCH,
            generation,
        },
    }
}

fn withdraw(object: &DfsObjectMetadata, replica: NodeId, generation: u64) -> DfsMetadataTableOp {
    DfsMetadataTableOp::WithdrawReceipt {
        withdrawal: DfsReplicaReceiptWithdrawalRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: object.version.clone(),
            replica_node_id: replica,
            generation,
        },
    }
}

fn key(object: &DfsObjectMetadata, replica: NodeId) -> DfsRepairNeedKey {
    (object.object_id.clone(), object.version.clone(), replica)
}

fn owned_need(object: &DfsObjectMetadata, intent_id: &str, generation: u64) -> DfsRepairNeed {
    DfsRepairNeed {
        file_system_id: object.file_system_id.clone(),
        content_hash: object.content_hash.clone().unwrap(),
        owner_intent_id: Some(intent_id.into()),
        root: RepairRoot::Permanent,
        owner_root: Some(RepairRoot::Permanent),
        receipt_generation: generation,
        enrollment_blocked_by: None,
    }
}

fn apply_checked(tables: &mut DfsMetadataTables, op: DfsMetadataTableOp, context: &str) {
    tables.apply_checked(op).unwrap();
    assert_repair_index_matches_rows(tables, context);
}

/// Object v1 written at W1/N2: the owner holds a receipt, the replica does not.
fn written_below_n_ops() -> Vec<DfsMetadataTableOp> {
    let v1 = object("version_repair_index_1", "blake3:repair-index-1");
    vec![
        DfsMetadataTableOp::PutFileSystem {
            file_system: file_system(),
        },
        DfsMetadataTableOp::PutObject { object: v1.clone() },
        DfsMetadataTableOp::PutFileSystemObject {
            path_record: path_record_for_object(&v1),
        },
        DfsMetadataTableOp::PutVersion {
            version: version_row(&v1),
        },
        DfsMetadataTableOp::PutIntent {
            intent: committed_intent(&v1, "intent_repair_index_1", vec![owner(), replica()]),
        },
        receipt(&v1, owner(), 0),
    ]
}

fn snapshot_of(tables: &DfsMetadataTables) -> DfsMetadataTableSnapshot {
    let mut snapshot = DfsMetadataTableSnapshot {
        format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
        materialized_through_sequence: 0,
        tables: Box::new(metadata_tables_snapshot(tables)),
        semantic_state_fingerprint: String::new(),
        prefix_proof: crate::metadata_records::dfs_metadata_prefix_proof_genesis(
            &crate::DfsMetadataShardId::new("repair-index-test"),
        ),
    };
    snapshot.semantic_state_fingerprint =
        crate::metadata_records::dfs_metadata_table_snapshot_state_fingerprint(&snapshot).unwrap();
    snapshot
}

#[test]
fn index_follows_writes_receipts_withdrawals_placement_and_replacement() {
    let v1 = object("version_repair_index_1", "blake3:repair-index-1");
    let mut tables = DfsMetadataTables::default();
    for op in written_below_n_ops() {
        tables.apply(op);
        assert_repair_index_matches_rows(&tables, "write below N");
    }
    assert_eq!(
        tables.repair_index().needs(),
        &BTreeMap::from([(
            key(&v1, replica()),
            owned_need(&v1, "intent_repair_index_1", 0)
        )]),
        "a W<N write leaves one need for the missing replica"
    );
    assert_eq!(
        tables.take_repair_index_changed(),
        BTreeSet::from([object_id()])
    );
    tables.apply(DfsMetadataTableOp::PutFileSystem {
        file_system: file_system(),
    });
    assert!(
        tables.take_repair_index_changed().is_empty(),
        "an operation that leaves every need unchanged raises no wake"
    );

    apply_checked(&mut tables, receipt(&v1, replica(), 0), "receipt arrival");
    assert_eq!(tables.repair_index().len(), 0);
    assert_eq!(
        tables.take_repair_index_changed(),
        BTreeSet::from([object_id()])
    );

    apply_checked(&mut tables, withdraw(&v1, replica(), 1), "withdrawal");
    assert_eq!(
        tables.repair_index().get(&key(&v1, replica())),
        Some(&owned_need(&v1, "intent_repair_index_1", 1)),
        "a withdrawn replica is needed again at the advanced slot generation"
    );

    // A second placement of the same version adds its own missing replica.
    tables.apply(DfsMetadataTableOp::PutIntent {
        intent: committed_intent(&v1, "intent_repair_index_placement", vec![owner(), third()]),
    });
    assert_repair_index_matches_rows(&tables, "placement change");
    assert_eq!(
        tables.repair_index().get(&key(&v1, third())),
        Some(&owned_need(&v1, "intent_repair_index_placement", 0))
    );
    assert_eq!(tables.repair_index().len(), 2);

    // Replacing the head supersedes v1's current demand.
    let v2 = object("version_repair_index_2", "blake3:repair-index-2");
    for op in [
        DfsMetadataTableOp::PutObject { object: v2.clone() },
        DfsMetadataTableOp::PutVersion {
            version: version_row(&v2),
        },
        DfsMetadataTableOp::PutIntent {
            intent: committed_intent(&v2, "intent_repair_index_2", vec![owner(), replica()]),
        },
        receipt(&v2, owner(), 0),
    ] {
        tables.apply(op);
        assert_repair_index_matches_rows(&tables, "version replacement");
    }
    assert_eq!(
        tables.repair_index().needs(),
        &BTreeMap::from([(
            key(&v2, replica()),
            owned_need(&v2, "intent_repair_index_2", 0)
        )])
    );

    // Collecting the superseded version changes no need.
    tables.take_repair_index_changed();
    for op in [
        DfsMetadataTableOp::RemoveReceipt {
            object_id: object_id(),
            version: v1.version.clone(),
            replica_node_id: owner(),
        },
        DfsMetadataTableOp::RemoveVersion {
            object_id: object_id(),
            version: v1.version.clone(),
        },
    ] {
        tables.apply(op);
        assert_repair_index_matches_rows(&tables, "version collection");
    }
    assert!(tables.take_repair_index_changed().is_empty());
    assert_eq!(tables.repair_index().len(), 1);

    // Compacting the owner intent removes the demand it carried.
    let owner_intent = tables
        .intents
        .get(&(object_id(), "intent_repair_index_2".to_string()))
        .cloned()
        .unwrap();
    tables.apply(DfsMetadataTableOp::PutIntentReceipt {
        receipt: crate::DfsWriteIntentReceipt::from_intent(
            &owner_intent,
            OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap(),
    });
    assert_repair_index_matches_rows(&tables, "intent compaction");
    assert_eq!(tables.repair_index().len(), 0);
}

#[test]
fn index_is_rebuilt_by_snapshot_restore_and_replay_and_restored_by_rollback() {
    let v1 = object("version_repair_index_1", "blake3:repair-index-1");
    let mut ops = written_below_n_ops();
    ops.extend([receipt(&v1, replica(), 0), withdraw(&v1, replica(), 1)]);
    let mut tables = DfsMetadataTables::default();
    for op in ops.iter().cloned() {
        tables.apply_checked(op).unwrap();
    }
    assert_repair_index_matches_rows(&tables, "withdrawn replica");
    assert_eq!(
        tables
            .repair_index()
            .get(&key(&v1, replica()))
            .map(|need| need.receipt_generation),
        Some(1),
        "the need carries the withdrawal tombstone's generation"
    );

    let restored =
        metadata_tables_from_table_snapshot(snapshot_of(&tables), "repair index snapshot").unwrap();
    assert_eq!(restored.repair_index(), tables.repair_index());
    assert_repair_index_matches_rows(&restored, "snapshot restore");

    let mut replayed = DfsMetadataTables::default();
    for op in ops {
        replayed.apply(op);
    }
    assert_eq!(replayed.repair_index(), tables.repair_index());

    let before = tables.repair_index().clone();
    let mut rollback = DfsMetadataTableRollback::default();
    tables
        .apply_checked_with_rollback(receipt(&v1, replica(), 1), &mut rollback)
        .unwrap();
    tables.apply_with_rollback(
        DfsMetadataTableOp::PutIntent {
            intent: committed_intent(&v1, "intent_repair_index_placement", vec![owner(), third()]),
        },
        &mut rollback,
    );
    assert_ne!(tables.repair_index(), &before);
    rollback.rollback(&mut tables);
    assert_eq!(tables.repair_index(), &before);
    assert_repair_index_matches_rows(&tables, "rollback");
}

#[test]
fn index_keeps_time_bounded_demand_and_leaves_liveness_to_dispatch() {
    // Table DATA only: a retained version whose keep-for window has passed.
    let historical = object("version_repair_index_1", "blake3:repair-index-1");
    let current = object("version_repair_index_2", "blake3:repair-index-2");
    let granted_at = 1_000_000_000;
    let mut captured = version_row(&historical);
    captured.content_durability = Some(crate::content_durability::from_intent(
        &crate::intent::distributed_intent_from_record(&committed_intent(
            &historical,
            "intent_repair_index_retained",
            vec![owner(), replica()],
        )),
    ));
    captured.retention.insert(
        "vendor.repair-index".into(),
        crate::DfsRevisionRetentionState {
            order: 1,
            grant: Some(Box::new(crate::DfsRevisionRetentionGrant {
                object: Box::new(historical.clone()),
                granted_at_unix_nanos: granted_at,
                origin: crate::DfsRetentionGrantOrigin::Rule,
                minimum: Some(crate::DfsOrderedRetentionGrant {
                    order: 1,
                    granted_at_unix_nanos: granted_at,
                    minimum: crate::DfsRetentionMinimum {
                        keep_last: 0,
                        keep_for_seconds: 60,
                    },
                }),
            })),
        },
    );
    let mut tables = DfsMetadataTables::default();
    for op in [
        DfsMetadataTableOp::PutFileSystem {
            file_system: file_system(),
        },
        DfsMetadataTableOp::PutObject {
            object: current.clone(),
        },
        DfsMetadataTableOp::PutFileSystemObject {
            path_record: path_record_for_object(&current),
        },
        DfsMetadataTableOp::PutVersion { version: captured },
        receipt(&historical, owner(), 0),
    ] {
        tables.apply(op);
        assert_repair_index_matches_rows(&tables, "retained version setup");
    }
    let deadline = granted_at + 60_000_000_000;
    let need = tables
        .repair_index()
        .get(&key(&historical, replica()))
        .cloned()
        .expect("the index keeps the demand regardless of the clock");
    assert_eq!(need.root, RepairRoot::Until(deadline));
    assert_eq!(need.owner_intent_id, None, "no intent owns version demand");
    assert!(!need.is_dispatchable(deadline - 1));
    assert_eq!(
        tables.metadata_status_summary().pending_repair_count,
        0,
        "status reads liveness at observation time"
    );
}

#[test]
fn the_end_of_an_unfinished_write_changes_the_ownerless_need_it_blocked() {
    // Retained version demand without an owner, as above, kept permanently.
    let historical = object("version_repair_index_1", "blake3:repair-index-1");
    let current = object("version_repair_index_2", "blake3:repair-index-2");
    let mut captured = version_row(&historical);
    captured.content_durability = Some(crate::content_durability::from_intent(
        &crate::intent::distributed_intent_from_record(&committed_intent(
            &historical,
            "intent_repair_index_retained",
            vec![owner(), replica()],
        )),
    ));
    captured.retention.insert(
        "vendor.repair-index".into(),
        crate::DfsRevisionRetentionState {
            order: 1,
            grant: Some(Box::new(crate::DfsRevisionRetentionGrant {
                object: Box::new(historical.clone()),
                granted_at_unix_nanos: 1,
                origin: crate::DfsRetentionGrantOrigin::Rule,
                minimum: Some(crate::DfsOrderedRetentionGrant {
                    order: 1,
                    granted_at_unix_nanos: 1,
                    minimum: crate::DfsRetentionMinimum {
                        keep_last: 1,
                        keep_for_seconds: 0,
                    },
                }),
            })),
        },
    );
    let mut tables = DfsMetadataTables::default();
    for op in [
        DfsMetadataTableOp::PutFileSystem {
            file_system: file_system(),
        },
        DfsMetadataTableOp::PutObject {
            object: current.clone(),
        },
        DfsMetadataTableOp::PutFileSystemObject {
            path_record: path_record_for_object(&current),
        },
        DfsMetadataTableOp::PutVersion { version: captured },
        receipt(&historical, owner(), 0),
    ] {
        apply_checked(&mut tables, op, "retained version setup");
    }
    let demand = key(&historical, replica());
    let need = tables.repair_index().get(&demand).cloned().unwrap();
    assert_eq!(need.owner_intent_id, None);
    assert_eq!(need.enrollment_blocked_by, None);
    tables.clear_repair_index_changes();
    // An unfinished write of that version owns the operation: enrollment
    // waits for it, and the need names it.
    let mut unfinished = committed_intent(
        &historical,
        "intent_repair_index_unfinished",
        vec![owner(), replica()],
    );
    unfinished.status = DfsWriteIntentStatus::Staging;
    apply_checked(
        &mut tables,
        DfsMetadataTableOp::PutIntent {
            intent: unfinished.clone(),
        },
        "unfinished write",
    );
    assert_eq!(
        tables
            .repair_index()
            .get(&demand)
            .and_then(|need| need.enrollment_blocked_by.clone()),
        Some("intent_repair_index_unfinished".to_string())
    );
    assert_eq!(
        tables.take_repair_index_changed(),
        BTreeSet::from([object_id()])
    );
    // It aborts. The need still has no owner, but its change reaches the
    // executor, which asks for enrollment again.
    unfinished.status = DfsWriteIntentStatus::Aborted;
    apply_checked(
        &mut tables,
        DfsMetadataTableOp::PutIntent { intent: unfinished },
        "aborted write",
    );
    let need = tables.repair_index().get(&demand).cloned().unwrap();
    assert_eq!(need.owner_intent_id, None);
    assert_eq!(need.enrollment_blocked_by, None);
    assert_eq!(
        tables.take_repair_index_changed(),
        BTreeSet::from([object_id()]),
        "the write's end is a net change of the need it blocked"
    );
}

#[test]
fn change_flag_reports_the_net_change_of_one_drained_batch() {
    let v1 = object("version_repair_index_1", "blake3:repair-index-1");
    let mut tables = DfsMetadataTables::default();
    for op in written_below_n_ops() {
        tables.apply(op);
    }
    assert_eq!(
        tables.take_repair_index_changed(),
        BTreeSet::from([object_id()])
    );

    // A placement whose missing replica is published in the same batch leaves
    // the index as the previous drain saw it.
    tables.apply(DfsMetadataTableOp::PutIntent {
        intent: committed_intent(&v1, "intent_repair_index_placement", vec![owner(), third()]),
    });
    tables.apply(receipt(&v1, third(), 0));
    assert_repair_index_matches_rows(&tables, "settled within one batch");
    assert!(tables.take_repair_index_changed().is_empty());

    // A reload rebuilds an index an earlier drain already announced.
    tables.apply(receipt(&v1, replica(), 0));
    tables.clear_repair_index_changes();
    assert!(tables.take_repair_index_changed().is_empty());
}
