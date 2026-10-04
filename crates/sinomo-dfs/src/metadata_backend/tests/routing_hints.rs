//! Derived routing-hint recovery across hidden writes and canonical ownership.

use super::*;
use crate::metadata_shard_index::{
    DfsObjectShardIndexRecord, object_shard_index_write_count, reset_object_shard_index_write_count,
};

#[derive(Clone, Copy, Debug)]
enum HintDamage {
    Missing,
    Malformed,
    ForeignObject,
    WrongShard,
}

fn hint_shards() -> Vec<DfsMetadataShardConfig> {
    vec![
        shard_config("root", DfsMetadataShardAssignment::Root),
        shard_config(
            "services",
            DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".into(),
            },
        ),
    ]
}

fn pending_hint_fixture() -> (
    DfsObjectMetadata,
    DfsWriteIntentRecord,
    DfsReplicaReceiptRecord,
) {
    let mut child = object(version("routing_hint_base"));
    child.object_id = DfsObjectId::from_legacy_named("routing_hint_child").expect("child id");
    child.path = "/services/hint-child.bin".into();
    child.content_hash = None;
    child.content_size_bytes = None;
    let intent = DfsWriteIntentRecord {
        publication: Default::default(),
        intent_id: "routing-hint-pending-create".into(),
        file_system_id: child.file_system_id.clone(),
        object_id: child.object_id.clone(),
        base_version: None,
        target_version: version("routing_hint_target"),
        coordinator_node_id: node("node_owner"),
        operation_id: None,
        operation_order_token: None,
        owner_epoch: 1,
        desired_replica_node_ids: vec![node("node_owner")],
        policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
        status: DfsWriteIntentStatus::ReceiptsPending,
        content_hash: Some("blake3:routing-hint-target".into()),
        object_metadata: Some(child.clone()),
        touch_parent_on_create: true,
        create_predecessor_object_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: None,
    };
    let mut receipt = replica_receipt_record(
        &child,
        intent.target_version.clone(),
        OffsetDateTime::UNIX_EPOCH,
    );
    receipt.content_hash = intent.content_hash.clone();
    (child, intent, receipt)
}

fn damage_hint(
    backend: &LocalShardedMetadataBackend,
    child: &DfsObjectMetadata,
    damage: HintDamage,
) {
    let path = backend.object_shard_index_path(&child.object_id);
    match damage {
        HintDamage::Missing => fs::remove_file(path).expect("remove derived hint"),
        HintDamage::Malformed => fs::write(path, b"torn derived hint").expect("tear hint"),
        HintDamage::ForeignObject | HintDamage::WrongShard => {
            let record = DfsObjectShardIndexRecord {
                object_id: if matches!(damage, HintDamage::ForeignObject) {
                    DfsObjectId::from_legacy_named("foreign_hint_object").expect("foreign id")
                } else {
                    child.object_id.clone()
                },
                file_system_id: child.file_system_id.clone(),
                // This wrong-shard record is internally route-consistent. It
                // must still not choose the owner of a hidden create receipt.
                path: "/wrong-shard-hint.bin".into(),
                shard_id: DfsMetadataShardId::new("root"),
            };
            crate::metadata_io::write_sobj_atomic(&path, &record).expect("install wrong hint");
        }
    }
}

fn hint_parent() -> DfsObjectMetadata {
    let mut parent = object(version("routing_hint_parent_base"));
    parent.object_id = DfsObjectId::from_legacy_named("routing_hint_parent").expect("parent id");
    parent.path = "/services".into();
    parent.kind = DfsObjectKind::Directory;
    parent.content_hash = None;
    parent.content_size_bytes = None;
    parent.mode = 0o755;
    parent
}

#[test]
fn routing_hint_pending_create_reopens_and_completes_from_untrusted_hints() {
    for damage in [
        HintDamage::Missing,
        HintDamage::Malformed,
        HintDamage::ForeignObject,
        HintDamage::WrongShard,
    ] {
        let root = temp_root(&format!("routing_hint_pending_{damage:?}"));
        let mut backend = LocalShardedMetadataBackend::new(&root, hint_shards()).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        let parent = hint_parent();
        backend.put_object(&parent).expect("parent");
        let (child, pending, receipt) = pending_hint_fixture();
        reset_object_shard_index_write_count();
        let entries = backend
            .put_intent_and_receipts_for_object(
                Some(&child),
                &pending,
                std::slice::from_ref(&receipt),
            )
            .expect("durable hidden intent and exact receipt");
        assert_eq!(entries.len(), 2);
        assert_eq!(
            object_shard_index_write_count(),
            0,
            "a new derived hint must not add atomic durable replacements"
        );
        damage_hint(&backend, &child, damage);
        drop(backend);

        let mut reopened = LocalShardedMetadataBackend::new(&root, hint_shards()).expect("reopen");
        assert_eq!(
            reopened.get_object(&child.object_id).expect("hidden child"),
            None
        );
        assert_eq!(
            reopened
                .get_intent(&child.object_id, &pending.intent_id)
                .expect("exact intent"),
            Some(pending.clone())
        );
        assert_eq!(
            reopened.all_intents().expect("global active intents"),
            vec![pending.clone()]
        );
        assert_eq!(
            reopened
                .receipt_records_page(None, 8)
                .expect("exact receipt recovery")
                .receipts,
            vec![receipt.clone()]
        );

        let mut committed = child.clone();
        committed.version = pending.target_version.clone();
        committed.content_hash = pending.content_hash.clone();
        committed.content_size_bytes = Some(4096);
        let mut touched_parent = parent.clone();
        touched_parent.version = version("routing_hint_parent_touched");
        touched_parent.changed_at_unix_nanos = 1;
        touched_parent.modified_at_unix_nanos = 1;
        let transaction = DfsNamespaceTransaction {
            content_reference_identity: None,
            contribution_digest: None,
            extension_expectations: Vec::new(),
            file_version_pins: None,
            transaction_id: "routing-hint-canonical-create".into(),
            file_system_id: child.file_system_id.clone(),
            objects: vec![committed.clone(), touched_parent.clone()],
            versions: vec![
                object_version_record(
                    &committed,
                    committed.version.clone(),
                    OffsetDateTime::UNIX_EPOCH,
                ),
                object_version_record(
                    &touched_parent,
                    touched_parent.version.clone(),
                    OffsetDateTime::UNIX_EPOCH,
                ),
            ],
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        let shard = DfsMetadataShardId::new("services");
        let (expected_object_versions, expected_path_occupants, _) =
            crate::service_metadata::namespace_prepare_expectations(
                &reopened,
                &shard,
                &transaction,
            )
            .expect("canonical parent/path expectations");
        let mut prepare = DfsNamespacePrepareRecord {
            operation_id: "namespace:create:routing-hint-pending-create".into(),
            file_system_id: child.file_system_id.clone(),
            shard_id: shard.clone(),
            owner_epoch: 1,
            operation_digest: String::new(),
            expected_object_versions,
            expected_path_occupants,
            content_receipts: Vec::new(),
            transaction,
            completed_request: None,
            completed_request_completion: None,
            status: DfsNamespacePrepareStatus::Prepared,
        };
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
                .expect("digest");
        reopened
            .prepare_namespace_transaction(&prepare)
            .expect("durable hidden namespace prepare");
        assert_eq!(
            reopened.get_object(&child.object_id).expect("still hidden"),
            None
        );
        reopened
            .commit_prepared_namespace_transaction_on_shard(
                &shard,
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .expect("canonical namespace commit");
        let mut terminal = pending.clone();
        terminal.status = DfsWriteIntentStatus::Committed;
        terminal.object_metadata = Some(committed.clone());
        reopened
            .put_intent_for_object(Some(&committed), &terminal)
            .expect("terminal intent");
        drop(reopened);

        let final_backend =
            LocalShardedMetadataBackend::new(&root, hint_shards()).expect("final reopen");
        assert_eq!(
            final_backend
                .get_object(&child.object_id)
                .expect("committed child"),
            Some(committed)
        );
        assert_eq!(
            final_backend
                .get_object(&parent.object_id)
                .expect("atomic parent touch"),
            Some(touched_parent)
        );
        assert_eq!(
            final_backend
                .get_intent(&child.object_id, &pending.intent_id)
                .expect("terminal intent"),
            Some(terminal)
        );
        assert_eq!(
            final_backend
                .receipt_records_page(None, 8)
                .expect("durable receipt")
                .receipts,
            vec![receipt]
        );
        cleanup(&root);
    }
}

#[test]
fn routing_hint_hidden_aborted_history_survives_without_receipt_authority() {
    for damage in [
        HintDamage::Missing,
        HintDamage::Malformed,
        HintDamage::ForeignObject,
        HintDamage::WrongShard,
    ] {
        let root = temp_root(&format!("routing_hint_aborted_{damage:?}"));
        let mut backend = LocalShardedMetadataBackend::new(&root, hint_shards()).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        let (child, mut intent, receipt) = pending_hint_fixture();
        backend
            .put_intent_and_receipts_for_object(Some(&child), &intent, &[receipt])
            .expect("pending");
        intent.status = DfsWriteIntentStatus::Aborted;
        backend.put_intent(&intent).expect("durable abort");
        damage_hint(&backend, &child, damage);
        drop(backend);
        let reopened = LocalShardedMetadataBackend::new(&root, hint_shards()).expect("reopen");
        assert_eq!(
            reopened.all_intents().expect("aborted history"),
            vec![intent]
        );
        assert!(
            reopened
                .receipt_records_page(None, 8)
                .expect("receipt scan")
                .receipts
                .is_empty(),
            "a terminal hidden intent cannot authorize a receipt through its hint"
        );
        cleanup(&root);
    }
}

#[test]
fn routing_hint_real_io_errors_are_not_missing_state() {
    for directory_hint in [false, true] {
        let root = temp_root(&format!("routing_hint_io_{directory_hint}"));
        let mut backend = LocalShardedMetadataBackend::new(&root, hint_shards()).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        let (child, intent, receipt) = pending_hint_fixture();
        backend
            .put_intent_and_receipts_for_object(Some(&child), &intent, &[receipt])
            .expect("pending");
        let path = backend.object_shard_index_path(&child.object_id);
        if directory_hint {
            fs::remove_file(&path).expect("remove file");
            fs::create_dir(&path).expect("directory hint");
        } else {
            let parent = path.parent().expect("hint parent");
            fs::remove_dir_all(parent).expect("remove hint directory");
            fs::write(parent, b"blocking regular file").expect("regular-file parent");
        }
        assert!(
            matches!(
                backend.read_object_shard_index(&child.object_id),
                Err(MetadataBackendError::Io { .. })
            ),
            "real I/O error must not become missing or malformed hint"
        );
        assert!(
            matches!(backend.all_intents(), Err(MetadataBackendError::Io { .. })),
            "global recovery must expose the same I/O failure"
        );
        cleanup(&root);
    }
}

#[test]
fn routing_hint_moved_active_intent_ownership_uses_physical_versions() {
    for compatible in [false, true] {
        for damage in [
            HintDamage::Missing,
            HintDamage::Malformed,
            HintDamage::ForeignObject,
            HintDamage::WrongShard,
        ] {
            let root = temp_root(&format!("routing_hint_active_{compatible}_{damage:?}"));
            let mut backend =
                LocalShardedMetadataBackend::new(&root, hint_shards()).expect("backend");
            backend.put_file_system(&file_system()).expect("filesystem");
            let (mut source, mut intent, _) = pending_hint_fixture();
            source.path = "/source-owned-active.bin".into();
            source.changed_at_unix_nanos = 1;
            source.content_hash = Some("blake3:active-base".into());
            intent.base_version = Some(source.version.clone());
            intent.object_metadata = Some(source.clone());
            intent.touch_parent_on_create = false;
            backend.put_object(&source).expect("source object");
            backend.put_intent(&intent).expect("source active intent");
            let mut moved = source.clone();
            moved.path = "/services/moved-active.bin".into();
            moved.changed_at_unix_nanos = 2;
            moved.version = if compatible {
                intent.target_version.clone()
            } else {
                version("routing_hint_superseded")
            };
            backend
                .shard_backend_mut(&DfsMetadataShardId::new("services"))
                .expect("destination")
                .put_object(&moved)
                .expect("physical destination with stale source still present");
            damage_hint(&backend, &source, damage);
            drop(backend);
            let reopened = LocalShardedMetadataBackend::new(&root, hint_shards()).expect("reopen");
            let recovered = reopened
                .all_intents()
                .expect("recover current active ownership");
            assert_eq!(
                recovered,
                if compatible { vec![intent] } else { Vec::new() },
                "a hint must neither lose a base/target-compatible active intent nor resurrect a superseded source"
            );
            cleanup(&root);
        }
    }
}

#[test]
fn routing_hint_moved_history_uses_canonical_physical_owner() {
    for damage in [
        HintDamage::Missing,
        HintDamage::Malformed,
        HintDamage::ForeignObject,
        HintDamage::WrongShard,
    ] {
        let root = temp_root(&format!("routing_hint_history_{damage:?}"));
        let mut backend = LocalShardedMetadataBackend::new(&root, hint_shards()).expect("backend");
        backend.put_file_system(&file_system()).expect("filesystem");
        let (mut source, mut source_intent, _) = pending_hint_fixture();
        source.path = "/source-history.bin".into();
        source.changed_at_unix_nanos = 1;
        source.version = source_intent.target_version.clone();
        source.content_hash = source_intent.content_hash.clone();
        source_intent.status = DfsWriteIntentStatus::Committed;
        source_intent.object_metadata = Some(source.clone());
        source_intent.touch_parent_on_create = false;
        backend.put_object(&source).expect("source object");
        backend.put_intent(&source_intent).expect("source history");
        let mut moved = source.clone();
        moved.path = "/services/moved-history.bin".into();
        moved.changed_at_unix_nanos = 2;
        moved.version = version("routing_hint_new_current_after_history");
        let mut destination_intent = source_intent.clone();
        let mut moved_history_object = source.clone();
        moved_history_object.path = moved.path.clone();
        destination_intent.object_metadata = Some(moved_history_object);
        destination_intent.updated_at =
            Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1));
        let destination = backend
            .shard_backend_mut(&DfsMetadataShardId::new("services"))
            .expect("destination");
        destination
            .put_object(&moved)
            .expect("newer physical destination");
        destination
            .put_intent(&destination_intent)
            .expect("canonical moved history");
        damage_hint(&backend, &source, damage);
        drop(backend);
        let reopened = LocalShardedMetadataBackend::new(&root, hint_shards()).expect("reopen");
        assert_eq!(
            reopened.all_intents().expect("canonical history"),
            vec![destination_intent],
            "global history deduplication must not let stale source order or a hint choose the historical payload"
        );
        cleanup(&root);
    }
}

#[test]
fn routing_hint_cannot_choose_ambiguous_hidden_receipt_owner() {
    let root = temp_root("routing_hint_ambiguous_hidden_receipt");
    let mut backend = LocalShardedMetadataBackend::new(&root, hint_shards()).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");
    let (child, intent, receipt) = pending_hint_fixture();
    backend
        .put_intent_and_receipts_for_object(Some(&child), &intent, std::slice::from_ref(&receipt))
        .expect("services pending");
    let mut other_intent = intent.clone();
    let mut other_object = child.clone();
    other_object.path = "/conflicting-hidden-owner.bin".into();
    other_intent.object_metadata = Some(other_object);
    let other = backend
        .shard_backend_mut(&DfsMetadataShardId::new("root"))
        .expect("root shard");
    other
        .put_intent(&other_intent)
        .expect("conflicting canonical pending identity");
    other.put_receipt(&receipt).expect("other physical receipt");
    // The otherwise well-formed services hint cannot decide this ambiguity.
    assert!(
        backend
            .receipt_records_page(None, 8)
            .expect("fail-closed receipt scan")
            .receipts
            .is_empty()
    );
    damage_hint(&backend, &child, HintDamage::WrongShard);
    assert!(
        backend
            .receipt_records_page(None, 8)
            .expect("wrong hint cannot select root")
            .receipts
            .is_empty()
    );
    cleanup(&root);
}
