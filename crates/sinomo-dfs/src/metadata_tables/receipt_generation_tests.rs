//! Receipt-slot generations in the materialized tables: every replica applies
//! the same journal through these checked operations.

use super::*;
use crate::metadata_records::DfsReplicaReceiptWithdrawalRecord;
use sinomo_core::DfsWriteReceipt;
use sinomo_core::events::EventId;

const HASH: &str = "blake3:receipt-generation";

fn file_system_id() -> DfsFileSystemId {
    DfsFileSystemId::from_legacy_named("fs_receipt_generation").unwrap()
}

fn object_id() -> DfsObjectId {
    DfsObjectId::from_legacy_named("obj_receipt_generation").unwrap()
}

fn version() -> DfsVersion {
    DfsVersion::from_legacy_named("version_receipt_generation").unwrap()
}

fn node(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).unwrap()
}

fn version_row() -> DfsObjectVersionRecord {
    DfsObjectVersionRecord {
        file_system_id: file_system_id(),
        object_id: object_id(),
        version: version(),
        content_hash: Some(HASH.into()),
        size_bytes: Some(7),
        locations: Vec::new(),
        created_by_event_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
        retention: Default::default(),
        module_metadata: Default::default(),
        content_durability: None,
    }
}

/// A shard holding one live version and no receipt rows.
fn live_version_tables() -> DfsMetadataTables {
    let mut tables = DfsMetadataTables::default();
    tables.apply(DfsMetadataTableOp::PutVersion {
        version: version_row(),
    });
    tables
}

fn receipt(replica: &NodeId, generation: u64) -> DfsReplicaReceiptRecord {
    DfsReplicaReceiptRecord {
        receipt: DfsWriteReceipt {
            file_system_id: file_system_id(),
            object_id: object_id(),
            committed_version: version(),
            owner_node_id: node("node_receipt_generation_owner"),
            event_id: EventId::from_legacy_named("event_receipt_generation").unwrap(),
        },
        replica_node_id: replica.clone(),
        content_hash: Some(HASH.into()),
        received_at: OffsetDateTime::UNIX_EPOCH,
        generation,
    }
}

fn withdrawal(replica: &NodeId, generation: u64) -> DfsReplicaReceiptWithdrawalRecord {
    DfsReplicaReceiptWithdrawalRecord {
        file_system_id: file_system_id(),
        object_id: object_id(),
        version: version(),
        replica_node_id: replica.clone(),
        generation,
    }
}

fn put(replica: &NodeId, generation: u64) -> DfsMetadataTableOp {
    DfsMetadataTableOp::PutReceipt {
        receipt: receipt(replica, generation),
    }
}

fn withdraw(replica: &NodeId, generation: u64) -> DfsMetadataTableOp {
    DfsMetadataTableOp::WithdrawReceipt {
        withdrawal: withdrawal(replica, generation),
    }
}

fn generation(tables: &DfsMetadataTables, replica: &NodeId) -> u64 {
    tables.receipt_slot_generation(&object_id(), &version(), replica)
}

fn slot_key(replica: &NodeId) -> (DfsObjectId, DfsVersion, NodeId) {
    (object_id(), version(), replica.clone())
}

fn assert_superseded(result: Result<()>, planned: u64, actual: u64) {
    match result {
        Err(MetadataBackendError::ReceiptGenerationSuperseded {
            expected, current, ..
        }) => assert_eq!((expected, current), (planned, actual)),
        other => panic!("expected a superseded receipt generation, got {other:?}"),
    }
}

#[test]
fn slot_generation_reads_absent_present_and_withdrawn_slots() {
    let replica = node("node_receipt_generation_replica");
    let mut tables = live_version_tables();
    assert_eq!(generation(&tables, &replica), 0, "a never-used slot");
    tables.apply_checked(put(&replica, 0)).unwrap();
    assert_eq!(generation(&tables, &replica), 0, "publication keeps it");
    assert!(tables.receipts.contains_key(&slot_key(&replica)));

    tables.apply_checked(withdraw(&replica, 1)).unwrap();
    assert_eq!(generation(&tables, &replica), 1, "withdrawal advances it");
    assert!(!tables.receipts.contains_key(&slot_key(&replica)));
    assert_eq!(
        tables.receipt_withdrawals.get(&slot_key(&replica)),
        Some(&withdrawal(&replica, 1)),
        "a withdrawal is a tombstone row, not a removal"
    );

    tables.apply_checked(put(&replica, 1)).unwrap();
    assert_eq!(generation(&tables, &replica), 1);
    assert!(tables.receipt_withdrawals.is_empty());
    assert_eq!(
        tables
            .receipts
            .get(&slot_key(&replica))
            .map(|row| row.generation),
        Some(1)
    );

    // An absent slot is withdrawn as well: a reply planned before the
    // failed read cannot be ordered against it.
    let other = node("node_receipt_generation_other");
    tables.apply_checked(withdraw(&other, 1)).unwrap();
    assert_eq!(generation(&tables, &other), 1);
    assert_eq!(generation(&tables, &replica), 1, "slots are independent");
}

#[test]
fn publication_and_withdrawal_compare_the_planned_generation() {
    let replica = node("node_receipt_generation_cas");
    let mut tables = live_version_tables();
    tables.apply_checked(put(&replica, 0)).unwrap();
    tables.apply_checked(withdraw(&replica, 1)).unwrap();
    let before = tables.clone();

    // A reply planned before the withdrawal is superseded, deterministically.
    assert_superseded(tables.validate_checked(&put(&replica, 0)), 0, 1);
    assert_superseded(tables.apply_checked(put(&replica, 0)), 0, 1);
    assert_superseded(tables.apply_checked(put(&replica, 2)), 2, 1);
    // A late withdrawal of the older observation is superseded too.
    assert_superseded(tables.apply_checked(withdraw(&replica, 1)), 0, 1);
    assert!(
        tables.apply_checked(withdraw(&replica, 0)).is_err(),
        "a withdrawal must advance its slot"
    );
    assert_eq!(tables, before, "a rejected compare changes nothing");

    let mut rollback = DfsMetadataTableRollback::default();
    assert_superseded(
        tables.apply_checked_with_rollback(put(&replica, 0), &mut rollback),
        0,
        1,
    );
    rollback.rollback(&mut tables);
    assert_eq!(tables, before);
}

#[test]
fn same_generation_redelivery_is_an_idempotent_publication() {
    let replica = node("node_receipt_generation_redelivery");
    let mut tables = live_version_tables();
    tables.apply_checked(withdraw(&replica, 1)).unwrap();
    tables.apply_checked(put(&replica, 1)).unwrap();
    let published = tables.clone();
    tables.apply_checked(put(&replica, 1)).unwrap();
    assert_eq!(
        tables, published,
        "the same receipt at the same generation leaves the slot unchanged"
    );
    assert_eq!(generation(&tables, &replica), 1);
}

#[test]
fn collection_keeps_a_live_generation_and_forgets_a_collected_version() {
    let replica = node("node_receipt_generation_gc");
    let fresh = node("node_receipt_generation_gc_fresh");
    let mut tables = live_version_tables();
    tables.apply_checked(withdraw(&replica, 1)).unwrap();
    tables.apply_checked(put(&replica, 1)).unwrap();
    tables.apply_checked(put(&fresh, 0)).unwrap();
    let remove = |replica: &NodeId| DfsMetadataTableOp::RemoveReceipt {
        object_id: object_id(),
        version: version(),
        replica_node_id: replica.clone(),
    };

    // Collecting one row of a live version cannot rewind its slot.
    tables.apply_checked(remove(&replica)).unwrap();
    assert!(!tables.receipts.contains_key(&slot_key(&replica)));
    assert_eq!(generation(&tables, &replica), 1);
    assert_superseded(tables.apply_checked(put(&replica, 0)), 0, 1);
    tables.apply_checked(remove(&fresh)).unwrap();
    assert_eq!(generation(&tables, &fresh), 0);
    assert!(!tables.receipt_withdrawals.contains_key(&slot_key(&fresh)));

    // Whole-version collection forgets every slot of that version.
    tables
        .apply_checked(DfsMetadataTableOp::RemoveVersion {
            object_id: object_id(),
            version: version(),
        })
        .unwrap();
    assert!(tables.receipt_withdrawals.is_empty());
    assert_eq!(generation(&tables, &replica), 0);
    assert!(
        tables.apply_checked(withdraw(&replica, 1)).is_err(),
        "a collected version has no slot to withdraw"
    );
}

#[test]
fn rollback_restores_receipt_slots_and_withdrawals() {
    let replica = node("node_receipt_generation_rollback");
    let other = node("node_receipt_generation_rollback_other");
    let mut tables = live_version_tables();
    tables.apply_checked(put(&replica, 0)).unwrap();
    tables.apply_checked(withdraw(&other, 1)).unwrap();
    let before = tables.clone();
    let mut rollback = DfsMetadataTableRollback::default();
    for op in [
        withdraw(&replica, 1),
        put(&replica, 1),
        withdraw(&replica, 2),
        put(&other, 1),
        DfsMetadataTableOp::RemoveReceipt {
            object_id: object_id(),
            version: version(),
            replica_node_id: other.clone(),
        },
        DfsMetadataTableOp::RemoveVersion {
            object_id: object_id(),
            version: version(),
        },
    ] {
        tables
            .apply_checked_with_rollback(op, &mut rollback)
            .unwrap();
    }
    assert!(tables.receipt_withdrawals.is_empty());
    assert!(tables.receipts.is_empty());
    rollback.rollback(&mut tables);
    assert_eq!(tables, before);
}

#[test]
fn ordered_replay_reaches_the_same_slots_and_rejections() {
    let replica = node("node_receipt_generation_replay");
    let journal = [
        put(&replica, 0),
        withdraw(&replica, 1),
        put(&replica, 0),
        put(&replica, 1),
        withdraw(&replica, 2),
        withdraw(&replica, 2),
    ];
    let replay = || {
        let mut tables = live_version_tables();
        let outcomes = journal
            .iter()
            .map(|op| tables.apply_checked(op.clone()).is_ok())
            .collect::<Vec<_>>();
        (tables, outcomes)
    };
    let (primary, primary_outcomes) = replay();
    let (replica_tables, replica_outcomes) = replay();
    assert_eq!(primary_outcomes, [true, true, false, true, true, false]);
    assert_eq!(replica_outcomes, primary_outcomes);
    assert_eq!(replica_tables, primary);
    assert_eq!(generation(&primary, &replica), 2);
}

#[test]
fn snapshots_round_trip_receipt_generations_and_withdrawals() {
    let present = node("node_receipt_generation_snapshot_present");
    let withdrawn = node("node_receipt_generation_snapshot_withdrawn");
    let mut tables = live_version_tables();
    tables.apply_checked(withdraw(&present, 1)).unwrap();
    tables.apply_checked(put(&present, 1)).unwrap();
    tables.apply_checked(withdraw(&withdrawn, 1)).unwrap();
    tables.apply_checked(withdraw(&withdrawn, 2)).unwrap();

    let restored = crate::metadata_table_store::metadata_tables_from_snapshot(
        crate::metadata_table_store::metadata_tables_snapshot(&tables),
    )
    .unwrap();
    assert_eq!(restored.receipts, tables.receipts);
    assert_eq!(restored.receipt_withdrawals, tables.receipt_withdrawals);
    assert_eq!(generation(&restored, &present), 1);
    assert_eq!(generation(&restored, &withdrawn), 2);

    let directory = sinomo_testdir::TestDir::new("receipt-generation-snapshot-image");
    let proof = DfsMetadataPrefixProof::from_digest(
        9,
        format!("dfs-metadata-prefix-v2:blake3:{}", "0".repeat(64)),
    )
    .unwrap();
    let staged =
        crate::metadata_snapshot::stage_snapshot(directory.path(), &tables, 9, &proof).unwrap();
    let decoded = crate::metadata_snapshot::decode_snapshot_image(
        std::fs::File::open(staged.path()).unwrap(),
        None,
    )
    .unwrap();
    assert_eq!(decoded.tables.receipts, tables.receipts);
    assert_eq!(
        decoded.tables.receipt_withdrawals,
        tables.receipt_withdrawals
    );
    assert_eq!(generation(&decoded.tables, &withdrawn), 2);
    assert_superseded(
        decoded.tables.clone().apply_checked(put(&withdrawn, 1)),
        1,
        2,
    );
}

#[test]
fn snapshot_rejects_a_withdrawal_beside_a_present_receipt() {
    let replica = node("node_receipt_generation_snapshot_conflict");
    let mut tables = live_version_tables();
    tables.apply_checked(put(&replica, 0)).unwrap();
    assert!(
        tables
            .load_receipt_withdrawal(withdrawal(&replica, 1))
            .is_err()
    );
    let other = node("node_receipt_generation_snapshot_zero");
    assert!(
        tables
            .load_receipt_withdrawal(withdrawal(&other, 0))
            .is_err(),
        "a tombstone always carries an advanced generation"
    );
}

#[test]
fn never_withdrawn_receipts_keep_their_existing_encoding() {
    let replica = node("node_receipt_generation_encoding");
    let legacy = receipt(&replica, 0);
    let encoded = serde_json::to_value(&legacy).unwrap();
    assert!(
        encoded.get("generation").is_none(),
        "a generation-zero receipt encodes exactly as before generations"
    );
    assert_eq!(
        serde_json::from_value::<DfsReplicaReceiptRecord>(encoded).unwrap(),
        legacy,
        "an existing row without a generation decodes at zero"
    );
    let advanced = receipt(&replica, 3);
    let encoded = serde_json::to_value(&advanced).unwrap();
    assert_eq!(encoded.get("generation"), Some(&serde_json::json!(3)));
    let codec = sinomo_core::network::SinomoObjectCodec::network_frame();
    let bytes = codec.encode(&advanced).unwrap();
    assert_eq!(
        codec.decode::<DfsReplicaReceiptRecord>(&bytes).unwrap(),
        advanced
    );
    let table_snapshot = crate::metadata_table_store::metadata_tables_snapshot(&{
        let mut tables = live_version_tables();
        tables.apply_checked(put(&replica, 0)).unwrap();
        tables
    });
    assert!(
        serde_json::to_value(&table_snapshot)
            .unwrap()
            .get("receipt_withdrawals")
            .is_none(),
        "a shard without withdrawals keeps its existing snapshot fields"
    );
}
