//! Checked terminal DATA transitions of a genuine genesis record. The altered
//! bodies below are never published.
use super::*;

fn original(
    name: &str,
) -> (
    DfsTestRuntime,
    NativeService,
    crate::DfsPreparedOperationRecord,
) {
    let (runtime, service, config) = native_case(name, 1);
    let id = format!("genesis:{}:{}:1", config.file_system_id, config.shard_id);
    let record = service
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&id)
        .unwrap()
        .unwrap();
    assert_eq!(record.status, crate::DfsPreparedOperationStatus::Committed);
    record.validate().unwrap();
    (runtime, service, record)
}
fn rebind_data(
    record: &mut crate::DfsPreparedOperationRecord,
    payload: crate::DfsPreparedOperationPayload,
) {
    record.digest =
        crate::DfsPreparedOperationDigest::at_sequence(&payload, record.digest.prepare_sequence)
            .unwrap();
    record.payload = std::sync::Arc::new(payload);
    record.committed_entry.as_mut().unwrap().digest = record.digest.clone();
    record.validate().unwrap();
}
fn tables(record: &crate::DfsPreparedOperationRecord) -> DfsMetadataTables {
    let mut tables = DfsMetadataTables::default();
    tables
        .prepared_operations_v7
        .insert(record.operation_id.clone(), record.clone());
    tables.rebuild_prepared_operation_reservations().unwrap();
    tables
}

#[test]
fn genesis_eviction_preserves_original_terminal_encoding() {
    let (runtime, service, record) = original("genesis-eviction-none");
    let terminal = crate::DfsPreparedOperationEviction::from_terminal_record(&record).unwrap();
    #[derive(serde::Serialize)]
    struct OriginalTerminal<'a> {
        digest: &'a crate::DfsPreparedOperationDigest,
        status: crate::DfsPreparedOperationStatus,
        committed_entry: &'a Option<Box<crate::DfsPreparedOperationCommitReference>>,
    }
    let old = OriginalTerminal {
        digest: &terminal.digest,
        status: terminal.status,
        committed_entry: &terminal.committed_entry,
    };
    let bytes =
        sinomo_core::object::serde_ext::to_bytes(&old, 1, sinomo_core::object::LengthMode::U32)
            .unwrap();
    assert_eq!(
        bytes,
        sinomo_core::object::serde_ext::to_bytes(
            &terminal,
            1,
            sinomo_core::object::LengthMode::U32
        )
        .unwrap()
    );
    let decoded: crate::DfsPreparedOperationEviction =
        sinomo_core::object::serde_ext::from_bytes(&bytes).unwrap();
    assert_eq!(decoded, terminal);
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn genesis_eviction_uses_original_body_and_preserves_exact_replay() {
    let (runtime, service, record) = original("genesis-eviction-replay-data");
    let expected = crate::DfsPreparedOperationEviction::from_terminal_record(&record).unwrap();
    let mut table = tables(&record);
    // A newer mutable filesystem row cannot restamp the original terminal.
    for effect in &record.metadata_effects {
        if let crate::DfsPreparedMetadataEffect::PutFileSystem { file_system } = effect {
            let mut newer = file_system.clone();
            newer.display_name.push_str("-later");
            table
                .file_systems
                .insert(newer.file_system_id.clone(), newer);
        }
    }
    let remove = || DfsMetadataTableOp::RemovePreparedOperation {
        digest: record.digest.clone(),
    };
    table.apply_checked(remove()).unwrap();
    assert!(
        !table
            .prepared_operations_v7
            .contains_key(&record.operation_id)
    );
    assert_eq!(
        table.prepared_operation_evictions[&record.operation_id],
        expected
    );
    table.apply_checked(remove()).unwrap();
    assert_eq!(
        table.prepared_operation_evictions[&record.operation_id],
        expected
    );
    let mut wrong = record.digest.clone();
    wrong.payload_digest = format!("dfs-prepared-operation-v7:blake3:{}", "b".repeat(64));
    assert!(
        table
            .apply_checked(DfsMetadataTableOp::RemovePreparedOperation { digest: wrong })
            .is_err()
    );
    assert_eq!(
        table.prepared_operation_evictions[&record.operation_id],
        expected
    );
    drop(service);
    runtime.close().unwrap();
}

#[test]
fn genesis_eviction_refuses_body_mismatch_and_a_retained_terminal_change() {
    let (runtime, service, record) = original("genesis-eviction-mismatch-data");
    let expected = crate::DfsPreparedOperationEviction::from_terminal_record(&record).unwrap();
    {
        let mut table = tables(&record);
        let mut inconsistent = expected.clone();
        inconsistent.committed_entry.as_mut().unwrap().sequence += 1;
        table
            .prepared_operation_evictions
            .insert(record.operation_id.clone(), inconsistent.clone());
        assert!(
            table
                .apply_checked(DfsMetadataTableOp::RemovePreparedOperation {
                    digest: record.digest.clone()
                })
                .is_err()
        );
        assert_eq!(table.prepared_operations_v7[&record.operation_id], record);
        assert_eq!(
            table.prepared_operation_evictions[&record.operation_id],
            inconsistent
        );
    }
    let mut changed = record.clone();
    let mut payload = changed.payload.as_ref().clone();
    for effect in &mut payload.metadata_effects {
        if let crate::DfsPreparedMetadataEffect::PutFileSystem { file_system } = effect {
            file_system.display_name.push_str("-changed");
        }
    }
    changed.payload = std::sync::Arc::new(payload);
    assert!(
        crate::DfsPreparedOperationEviction::from_terminal_record(&changed).is_err(),
        "original digest cannot describe changed body bytes"
    );
    let mut duplicate = record.clone();
    let mut payload = duplicate.payload.as_ref().clone();
    let fs = payload
        .metadata_effects
        .iter()
        .find(|effect| {
            matches!(
                effect,
                crate::DfsPreparedMetadataEffect::PutFileSystem { .. }
            )
        })
        .unwrap()
        .clone();
    payload.metadata_effects.push(fs);
    rebind_data(&mut duplicate, payload);
    assert!(crate::DfsPreparedOperationEviction::from_terminal_record(&duplicate).is_err());
    drop(service);
    runtime.close().unwrap();
}
