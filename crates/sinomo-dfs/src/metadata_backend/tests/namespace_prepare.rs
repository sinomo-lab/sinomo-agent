//! Namespace prepare regression tests.

use super::*;

#[test]
fn prepared_capture_defers_replica_location_until_exact_commit_and_retry() {
    let root = temp_root("prepared-capture-location-reservation");
    let shard = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), &root).unwrap();
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV6);
    let mut current = object(version("capture-location-reservation"));
    current.path = "/docs/readme.md".into();
    current.current_locations = vec![current.owner_node_id.clone()];
    current.content_size_bytes = Some(4);
    let mut original = object_version_record(
        &current,
        current.version.clone(),
        OffsetDateTime::UNIX_EPOCH,
    );
    original.metadata_format = crate::DfsRevisionMetadataFormat::NamespacedV6;
    original.module_metadata.insert(
        "vendor.foreign".into(),
        crate::DfsModuleMetadataValue {
            schema_version: 1,
            payload: serde_json::json!({"preserve": true}),
        },
    );
    backend.put_file_system(&file_system()).unwrap();
    backend.put_object_and_version(&current, &original).unwrap();
    let original = backend
        .get_version(&current.object_id, &current.version)
        .unwrap()
        .unwrap();
    let namespace = "org.example.capture-location";
    let operation_id = "capture-location-reservation";
    let request = crate::DfsContentReferenceRequest {
        wire_format: crate::DfsContentReferenceFormat::NamespacedV6,
        namespace: namespace.into(),
        identity: crate::DfsRevisionReference {
            file_system_id: current.file_system_id.clone(),
            object_id: current.object_id.clone(),
            version: current.version.clone(),
        },
        path: current.path.clone(),
        operation_id: operation_id.into(),
        operation: crate::DfsContentReferenceOperation::CaptureCurrent {
            reference_id: "capture-root".into(),
            metadata: None,
        },
    };
    let reference = crate::DfsMetadataVersionPinRecord {
        namespace: Some(namespace.into()),
        file_system_id: current.file_system_id.clone(),
        object_id: current.object_id.clone(),
        version: current.version.clone(),
        pin_id: crate::content_reference_storage_id(namespace, "capture-root"),
        created_at: OffsetDateTime::now_utc(),
    };
    let mut captured = original.clone();
    captured.retention.insert(
        namespace.into(),
        crate::DfsRevisionRetentionState {
            order: 1,
            grant: Some(Box::new(crate::DfsRevisionRetentionGrant {
                object: Box::new(current.clone()),
                granted_at_unix_nanos: crate::metadata_records::dfs_now_unix_nanos(),
                origin: crate::DfsRetentionGrantOrigin::ExplicitOperation,
                minimum: None,
            })),
        },
    );
    let mut prepare = namespace_prepare(operation_id, &shard.0, &current, current.clone());
    prepare.transaction.objects.clear();
    prepare.transaction.transaction_id = format!("file-history:{operation_id}");
    prepare.transaction.versions = vec![captured.clone()];
    prepare.transaction.extension_expectations = vec![crate::DfsRevisionMetadataExpectation {
        object_id: current.object_id.clone(),
        version: current.version.clone(),
        expected: Some(Box::new(original.clone())),
    }];
    prepare.transaction.content_reference_identity = Some(request.identity.clone());
    prepare.transaction.contribution_digest = Some(
        blake3::hash(&serde_json::to_vec(&request).unwrap())
            .to_hex()
            .to_string(),
    );
    prepare.transaction.file_version_pins = Some(crate::DfsFileVersionPinMutation {
        add: vec![reference.clone()],
        remove: Vec::new(),
    });
    prepare.transaction.created_at = OffsetDateTime::now_utc();
    crate::metadata_records::normalize_namespace_prepare_in_place(&mut prepare);
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare).unwrap();
    backend.prepare_namespace_transaction(&prepare).unwrap();
    assert!(prepare.transaction.objects.is_empty());
    assert_eq!(
        backend
            .current_tables_snapshot()
            .unwrap()
            .namespace_prepare_object_owner_other_than(
                &current.file_system_id,
                &current.object_id,
                "",
            ),
        Some(operation_id),
        "the actual revision/reference footprint reserves the object without an object mutation",
    );
    let duplicate = backend
        .add_object_location_if_current(
            &current.object_id,
            &current.version,
            current.content_hash.as_deref().unwrap(),
            &current.owner_node_id,
        )
        .unwrap()
        .unwrap();
    assert!(
        !duplicate.journal_appended,
        "an exact no-op remains valid during capture"
    );
    assert!(
        backend
            .add_object_location_if_current(
                &current.object_id,
                &version("stale-location-head"),
                current.content_hash.as_deref().unwrap(),
                &node("late-location-peer"),
            )
            .unwrap()
            .is_none(),
        "a stale head remains a no-update result"
    );
    let peer = node("late-location-peer");
    let mut receipt =
        replica_receipt_record(&current, current.version.clone(), OffsetDateTime::now_utc());
    receipt.replica_node_id = peer.clone();
    backend.put_receipt(&receipt).unwrap();
    let before_location = backend.journal_last_sequence();
    let error = backend
        .add_object_location_if_current(
            &current.object_id,
            &current.version,
            current.content_hash.as_deref().unwrap(),
            &peer,
        )
        .unwrap_err();
    assert!(matches!(error, MetadataBackendError::Serde(message)
        if message.contains("deferred by active namespace prepare") && message.contains(operation_id)));
    assert_eq!(backend.journal_last_sequence(), before_location);
    assert_eq!(
        backend.get_object(&current.object_id).unwrap(),
        Some(current.clone())
    );
    assert_eq!(
        backend
            .get_version(&current.object_id, &current.version)
            .unwrap(),
        Some(original)
    );
    assert!(
        backend
            .version_pins_for_object(&current.object_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        backend
            .get_namespace_transaction(&prepare.transaction.transaction_id)
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .get_namespace_transaction_receipt(&prepare.transaction.transaction_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        backend.get_namespace_prepare(operation_id).unwrap(),
        Some(prepare.clone())
    );
    assert_eq!(
        backend
            .receipt_for_replica(&current.object_id, &current.version, &peer)
            .unwrap(),
        Some(receipt.clone())
    );
    // Recovery reuses the unchanged admitted body; no CAS rebase is needed.
    backend.prepare_namespace_transaction(&prepare).unwrap();
    backend
        .commit_prepared_namespace_transaction(
            operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .unwrap();
    assert_eq!(
        backend
            .get_version(&current.object_id, &current.version)
            .unwrap(),
        Some(captured.clone())
    );
    assert_eq!(
        backend.version_pins_for_object(&current.object_id).unwrap(),
        vec![reference.clone()]
    );
    let update = backend
        .add_object_location_if_current(
            &current.object_id,
            &current.version,
            current.content_hash.as_deref().unwrap(),
            &peer,
        )
        .unwrap()
        .unwrap();
    assert!(update.journal_appended);
    let published = backend
        .get_version(&current.object_id, &current.version)
        .unwrap()
        .unwrap();
    let mut expected = captured;
    expected.locations = update.object.current_locations.clone();
    assert_eq!(
        published, expected,
        "location publication preserves grant and foreign metadata"
    );
    drop(backend);
    let mut reopened = LocalSobjMetadataBackend::new(shard, &root).unwrap();
    reopened.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV6);
    reopened
        .commit_prepared_namespace_transaction(
            operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .unwrap();
    let duplicate = reopened
        .add_object_location_if_current(
            &current.object_id,
            &current.version,
            current.content_hash.as_deref().unwrap(),
            &peer,
        )
        .unwrap()
        .unwrap();
    assert!(!duplicate.journal_appended);
    assert_eq!(
        reopened
            .get_version(&current.object_id, &current.version)
            .unwrap(),
        Some(published)
    );
    assert_eq!(
        reopened
            .version_pins_for_object(&current.object_id)
            .unwrap(),
        vec![reference]
    );
    assert_eq!(
        reopened
            .receipt_for_replica(&current.object_id, &current.version, &peer)
            .unwrap(),
        Some(receipt)
    );
}

fn full_revision_expectation_fixture(
    backend: &mut LocalSobjMetadataBackend,
) -> (DfsObjectMetadata, DfsObjectVersionRecord) {
    let mut current = object(version("full-expectation-source"));
    current.path = "/docs/full-expectation.txt".into();
    current.content_size_bytes = Some(4);
    current.created_by_event_id = Some(EventId::generate());
    let mut locations = vec![node("expectation-peer-a"), node("expectation-peer-b")];
    locations.sort();
    locations.reverse();
    locations.push(current.owner_node_id.clone());
    current.current_locations = locations;
    let timestamp = OffsetDateTime::from_unix_timestamp(1_700_000_001)
        .unwrap()
        .replace_nanosecond(691_861_654)
        .unwrap()
        .to_offset(time::UtcOffset::from_whole_seconds(3_600).unwrap());
    let mut row = object_version_record(&current, current.version.clone(), timestamp);
    row.metadata_format = crate::DfsRevisionMetadataFormat::NamespacedV6;
    row.content_durability = Some(Box::new(crate::DfsContentDurability {
        current_policy_granted_at_unix_nanos: Some(1_700_000_001_691_861_654),
        source_version: current.version.clone(),
        coordinator_node_id: node("expectation-coordinator"),
        owner_epoch: 7,
        desired_replica_node_ids: current.current_locations.clone(),
        policy: crate::DfsReplicationPolicy::new(1, 3).unwrap(),
    }));
    for (namespace, order) in [("vendor.alpha", 3), ("vendor.foreign", 9)] {
        row.retention.insert(
            namespace.into(),
            crate::DfsRevisionRetentionState {
                order,
                grant: Some(Box::new(crate::DfsRevisionRetentionGrant {
                    object: Box::new(current.clone()),
                    granted_at_unix_nanos: 1_700_000_001_691_861_654,
                    origin: crate::DfsRetentionGrantOrigin::ExplicitOperation,
                    minimum: Some(crate::DfsOrderedRetentionGrant {
                        order,
                        granted_at_unix_nanos: 1_700_000_001_691_861_654,
                        minimum: crate::DfsRetentionMinimum {
                            keep_last: 2,
                            keep_for_seconds: 60,
                        },
                    }),
                })),
            },
        );
        row.module_metadata.insert(
            namespace.into(),
            crate::DfsModuleMetadataValue {
                schema_version: order as u32,
                payload: serde_json::json!({
                    "namespace": namespace,
                    "nested": [true, "opaque", {"value": order}],
                }),
            },
        );
    }
    row.validate_extension_metadata().unwrap();
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV6);
    backend.put_file_system(&file_system()).unwrap();
    backend.put_object(&current).unwrap();
    backend.put_version(&row).unwrap();
    let stored = backend
        .get_version(&row.object_id, &row.version)
        .unwrap()
        .unwrap();
    assert_eq!(
        stored, row,
        "seed writer must preserve the complete logical row"
    );
    assert_eq!(stored.created_at.nanosecond(), 691_861_654);
    assert_eq!(stored.locations, current.current_locations);
    assert!(stored.locations[0] > stored.locations[1]);
    (current, stored)
}

fn full_revision_expectation_prepare(
    operation_id: &str,
    shard: &DfsMetadataShardId,
    current: &DfsObjectMetadata,
    expected: &DfsObjectVersionRecord,
    updated: DfsObjectVersionRecord,
) -> DfsNamespacePrepareRecord {
    let mut prepare = namespace_prepare(operation_id, &shard.0, current, current.clone());
    prepare.transaction.objects.clear();
    prepare.transaction.versions = vec![updated];
    prepare.transaction.extension_expectations = vec![crate::DfsRevisionMetadataExpectation {
        object_id: expected.object_id.clone(),
        version: expected.version.clone(),
        expected: Some(Box::new(expected.clone())),
    }];
    crate::metadata_records::normalize_namespace_prepare_in_place(&mut prepare);
    crate::metadata_prepare_codec::namespace_prepare_for_codec_authoring(
        &mut prepare,
        DfsMetadataNamespacePrepareCodec::PackedV6,
    );
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare).unwrap();
    prepare
}

fn full_revision_expectation_packed_roundtrip(
    prepare: &DfsNamespacePrepareRecord,
) -> DfsNamespacePrepareRecord {
    let expected = &prepare.transaction.extension_expectations;
    let json = serde_json::to_vec(prepare).unwrap();
    let json_prepare: DfsNamespacePrepareRecord = serde_json::from_slice(&json).unwrap();
    assert_eq!(
        json_prepare.transaction.extension_expectations, *expected,
        "full expected-row JSON roundtrip"
    );
    let canonical_bytes =
        sinomo_object_serde::to_bytes(prepare, 1, sinomo_core::object::LengthMode::U32).unwrap();
    let canonical: DfsNamespacePrepareRecord =
        sinomo_object_serde::from_bytes(&canonical_bytes).unwrap();
    assert_eq!(
        canonical.transaction.extension_expectations, *expected,
        "full expected-row canonical SOBJ roundtrip"
    );
    let mutation = DfsMetadataMutation::put_namespace_prepare_packed_v6(prepare).unwrap();
    let bytes =
        sinomo_object_serde::to_bytes(&mutation, 1, sinomo_core::object::LengthMode::U32).unwrap();
    let decoded: DfsMetadataMutation = sinomo_object_serde::from_bytes(&bytes).unwrap();
    let unpacked = decoded
        .namespace_prepare_payload()
        .unwrap()
        .unwrap()
        .into_owned();
    assert_eq!(
        unpacked.transaction.extension_expectations, *expected,
        "full expected-row PackedV6 SOBJ roundtrip"
    );
    assert_eq!(
        unpacked, *prepare,
        "complete PackedV6 logical prepare equality"
    );
    for decoded in [&json_prepare, &canonical, &unpacked] {
        assert_eq!(
            crate::service_replica::metadata_namespace_prepare_payload_digest(decoded).unwrap(),
            prepare.operation_digest,
            "expected-row codec must preserve the complete canonical payload digest",
        );
    }
    unpacked
}

#[test]
fn packed_v6_full_revision_expectation_round_trips_before_writer_and_after_reopen() {
    let root = temp_root("packed-v6-full-expectation-writer-reopen");
    let shard = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), &root).unwrap();
    let (current, expected) = full_revision_expectation_fixture(&mut backend);
    let mut updated = expected.clone();
    updated
        .module_metadata
        .get_mut("vendor.alpha")
        .unwrap()
        .payload = serde_json::json!({"accepted": [1, "complete-row"]});
    let prepare = full_revision_expectation_prepare(
        "full-expectation-codec-writer",
        &shard,
        &current,
        &expected,
        updated.clone(),
    );
    let unpacked = full_revision_expectation_packed_roundtrip(&prepare);
    let before = backend.journal_last_sequence();
    let entry = backend.prepare_namespace_transaction(&unpacked).unwrap();
    assert!(entry.sequence > before);
    assert_eq!(
        backend
            .get_version(&expected.object_id, &expected.version)
            .unwrap(),
        Some(expected.clone()),
        "hidden prepare must not publish its new revision"
    );
    drop(backend);
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), &root).unwrap();
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV6);
    let reopened_prepare = backend
        .get_namespace_prepare(&prepare.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        reopened_prepare, prepare,
        "reopen preserves full expected-row/digest readset"
    );
    backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .unwrap();
    assert_eq!(
        backend
            .get_version(&expected.object_id, &expected.version)
            .unwrap(),
        Some(updated.clone()),
        "actual commit writer installs exactly the conditionally authored row"
    );
    drop(backend);
    let mut backend = LocalSobjMetadataBackend::new(shard, &root).unwrap();
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV6);
    assert_eq!(
        backend
            .get_version(&expected.object_id, &expected.version)
            .unwrap(),
        Some(updated.clone()),
        "committed PackedV6 row survives durable replay/reopen without field changes"
    );
    let receipt = backend
        .get_namespace_transaction_receipt(&prepare.transaction.transaction_id)
        .unwrap()
        .unwrap();
    assert!(receipt.matches_transaction(&prepare.transaction).unwrap());
    backend.prepare_namespace_transaction(&unpacked).unwrap();
    assert_eq!(
        backend
            .get_version(&expected.object_id, &expected.version)
            .unwrap(),
        Some(updated),
        "exact admitted retry must retain the original outcome rather than check old expected row as fresh"
    );
}

#[test]
fn packed_v6_full_revision_expectation_rejects_foreign_metadata_race_without_publication() {
    let root = temp_root("packed-v6-full-expectation-foreign-race");
    let shard = DfsMetadataShardId::new("root");
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), &root).unwrap();
    let (current, expected) = full_revision_expectation_fixture(&mut backend);
    let mut proposed = expected.clone();
    proposed
        .module_metadata
        .get_mut("vendor.alpha")
        .unwrap()
        .payload = serde_json::json!({"stale": true});
    let mut stale = full_revision_expectation_prepare(
        "full-expectation-stale-foreign",
        &shard,
        &current,
        &expected,
        proposed,
    );
    stale.transaction.file_version_pins = Some(crate::DfsFileVersionPinMutation {
        add: vec![crate::DfsMetadataVersionPinRecord {
            namespace: Some("vendor.alpha".into()),
            file_system_id: expected.file_system_id.clone(),
            object_id: expected.object_id.clone(),
            version: expected.version.clone(),
            pin_id: "stale-operation-reference".into(),
            created_at: expected.created_at,
        }],
        remove: Vec::new(),
    });
    stale.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&stale).unwrap();
    let stale = full_revision_expectation_packed_roundtrip(&stale);
    let mut foreign = expected.clone();
    foreign
        .module_metadata
        .get_mut("vendor.foreign")
        .unwrap()
        .payload = serde_json::json!({"foreign": "newer independent namespace"});
    let fresh = full_revision_expectation_prepare(
        "full-expectation-real-foreign-update",
        &shard,
        &current,
        &expected,
        foreign.clone(),
    );
    backend.prepare_namespace_transaction(&fresh).unwrap();
    backend
        .commit_prepared_namespace_transaction(
            &fresh.operation_id,
            &fresh.operation_digest,
            fresh.owner_epoch,
        )
        .unwrap();
    let journal_before = backend.journal_last_sequence();
    let object_before = backend.get_object(&current.object_id).unwrap();
    let references_before = backend.version_pins_for_object(&current.object_id).unwrap();
    let error = backend.prepare_namespace_transaction(&stale).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("revision metadata changed before publication"),
        "{error}"
    );
    assert_eq!(
        backend.journal_last_sequence(),
        journal_before,
        "stale expectation rejects before any durable journal publication"
    );
    assert_eq!(
        backend
            .get_version(&expected.object_id, &expected.version)
            .unwrap(),
        Some(foreign.clone())
    );
    assert_eq!(
        backend.get_object(&current.object_id).unwrap(),
        object_before
    );
    assert_eq!(
        backend.version_pins_for_object(&current.object_id).unwrap(),
        references_before,
        "no reference effect from the rejected stale operation"
    );
    assert!(
        backend
            .get_namespace_prepare(&stale.operation_id)
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .get_namespace_transaction(&stale.transaction.transaction_id)
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .get_namespace_transaction_receipt(&stale.transaction.transaction_id)
            .unwrap()
            .is_none()
    );
    drop(backend);
    let backend = LocalSobjMetadataBackend::new(shard, &root).unwrap();
    assert_eq!(
        backend
            .get_version(&expected.object_id, &expected.version)
            .unwrap(),
        Some(foreign)
    );
    assert_eq!(
        backend.version_pins_for_object(&current.object_id).unwrap(),
        references_before
    );
}

#[test]
fn unconfigured_canonical_v6_prepare_and_configured_v3_author_modes_are_distinct() {
    let root = temp_root("canonical-versus-configured-prepare-mode");
    let shard = DfsMetadataShardId::new("root");
    let current = object(version("canonical-mode-source"));
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), &root).unwrap();
    backend.put_file_system(&file_system()).unwrap();
    backend.put_object(&current).unwrap();
    let mut prepare = namespace_prepare("canonical-v6-mode", &shard.0, &current, current.clone());
    let mut row = object_version_record(
        &current,
        current.version.clone(),
        OffsetDateTime::UNIX_EPOCH,
    );
    row.metadata_format = crate::DfsRevisionMetadataFormat::NamespacedV6;
    prepare.transaction.versions = vec![row];
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare).unwrap();
    let entry = backend.prepare_namespace_transaction(&prepare).unwrap();
    assert!(matches!(
        &entry.mutation,
        DfsMetadataMutation::PutNamespacePrepare {
            prepare: Some(_),
            packed_prepare_v1: None,
            packed_prepare_v2: None,
            ..
        }
    ));
    let encoded =
        sinomo_object_serde::to_bytes(&entry, 1, sinomo_core::object::LengthMode::U32).unwrap();
    let decoded: DfsMetadataJournalEntry = sinomo_object_serde::from_bytes(&encoded).unwrap();
    let canonical = decoded
        .mutation
        .namespace_prepare_payload()
        .unwrap()
        .unwrap();
    assert_eq!(
        canonical.transaction.versions[0].metadata_format,
        crate::DfsRevisionMetadataFormat::NamespacedV6
    );
    assert_eq!(
        canonical.operation_digest,
        crate::service_replica::metadata_namespace_prepare_payload_digest(&canonical).unwrap()
    );
    backend
        .abort_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .unwrap();
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV3);
    let mut packed = prepare;
    packed.operation_id = "configured-v3-mode".into();
    packed.transaction.transaction_id = "configured-v3-mode".into();
    packed.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&packed).unwrap();
    assert!(
        backend
            .prepare_namespace_transaction(&packed)
            .unwrap_err()
            .to_string()
            .contains("select its codec serialization mode")
    );
    crate::metadata_prepare_codec::namespace_prepare_for_codec_authoring(
        &mut packed,
        DfsMetadataNamespacePrepareCodec::PackedV3,
    );
    packed.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&packed).unwrap();
    let entry = backend.prepare_namespace_transaction(&packed).unwrap();
    let encoded =
        sinomo_object_serde::to_bytes(&entry, 1, sinomo_core::object::LengthMode::U32).unwrap();
    let decoded: DfsMetadataJournalEntry = sinomo_object_serde::from_bytes(&encoded).unwrap();
    let canonical = decoded
        .mutation
        .namespace_prepare_payload()
        .unwrap()
        .unwrap();
    assert_eq!(
        canonical.transaction.versions[0].metadata_format,
        crate::DfsRevisionMetadataFormat::Legacy
    );
    assert_eq!(
        canonical.operation_digest,
        crate::service_replica::metadata_namespace_prepare_payload_digest(&canonical).unwrap()
    );
}

#[test]
fn default_layout_fence_reserves_deleted_source_with_a_reused_path() {
    let root = temp_root("default-layout-deleted-source-reused-path");
    let shard = DfsMetadataShardId::new("root");
    let mut deleted = object(version("layout-deleted-source"));
    deleted.kind = DfsObjectKind::Tombstone;
    deleted.content_hash = None;
    deleted.content_size_bytes = None;
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), &root).unwrap();
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV6);
    backend.put_file_system(&file_system()).unwrap();
    backend.put_object(&deleted).unwrap();
    let mut occupant = object(version("layout-new-path-occupant"));
    occupant.object_id = DfsObjectId::from_legacy_named("layout-new-path-object").unwrap();
    occupant.path = deleted.path.clone();
    backend.put_object(&occupant).unwrap();
    let mut fence = namespace_prepare(
        "default-layout-fence-v1:reused-path",
        &shard.0,
        &deleted,
        deleted.clone(),
    );
    fence.transaction.transaction_id = "default-layout-fence-tx-v1:reused-path".into();
    fence.expected_path_occupants[0].expected_object_id = Some(occupant.object_id.clone());
    fence.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&fence).unwrap();
    backend.prepare_namespace_transaction(&fence).unwrap();
    assert_eq!(
        backend.get_object(&deleted.object_id).unwrap(),
        Some(deleted.clone())
    );
    assert_eq!(
        backend
            .get_object_by_path(&deleted.file_system_id, &deleted.path)
            .unwrap(),
        Some(occupant.clone())
    );
    assert!(
        backend
            .commit_prepared_namespace_transaction(
                &fence.operation_id,
                &fence.operation_digest,
                fence.owner_epoch
            )
            .is_err()
    );
    backend
        .abort_prepared_namespace_transaction(
            &fence.operation_id,
            &fence.operation_digest,
            fence.owner_epoch,
        )
        .unwrap();
    let mut stale = fence;
    stale.operation_id = "default-layout-fence-v1:wrong-reused-path-witness".into();
    stale.transaction.transaction_id =
        "default-layout-fence-tx-v1:wrong-reused-path-witness".into();
    stale.expected_path_occupants[0].expected_object_id = None;
    stale.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&stale).unwrap();
    assert!(
        backend.prepare_namespace_transaction(&stale).is_err(),
        "an absent-path assertion cannot replace the actual new occupant"
    );
    assert_eq!(
        backend
            .get_object_by_path(&deleted.file_system_id, &deleted.path)
            .unwrap(),
        Some(occupant)
    );
}

#[test]
fn default_layout_fence_reserves_directory_with_no_current_revision_row() {
    let root = temp_root("default-layout-missing-directory-revision");
    let shard = DfsMetadataShardId::new("root");
    let mut current = object(version("layout-directory-source"));
    current.kind = DfsObjectKind::Directory;
    current.content_hash = None;
    current.content_size_bytes = None;
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), &root).unwrap();
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV6);
    backend.put_file_system(&file_system()).unwrap();
    backend.put_object(&current).unwrap();
    assert!(
        backend
            .get_version(&current.object_id, &current.version)
            .unwrap()
            .is_none()
    );
    let mut fence = namespace_prepare(
        "default-layout-fence-v1:missing-directory",
        &shard.0,
        &current,
        current.clone(),
    );
    fence.transaction.transaction_id = "default-layout-fence-tx-v1:missing-directory".into();
    fence.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&fence).unwrap();
    backend.prepare_namespace_transaction(&fence).unwrap();
    assert_eq!(
        crate::inspect_metadata_snapshot_reader_floor(&root)
            .unwrap()
            .unwrap()
            .format_version,
        6
    );
    assert_eq!(
        backend.get_object(&current.object_id).unwrap(),
        Some(current.clone())
    );
    assert!(
        backend
            .get_version(&current.object_id, &current.version)
            .unwrap()
            .is_none()
    );
    let mut altered = fence.clone();
    altered.operation_id = "default-layout-fence-v1:altered-directory".into();
    altered.transaction.objects[0].mode ^= 0o100;
    altered.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&altered).unwrap();
    assert!(
        backend
            .prepare_namespace_transaction(&altered)
            .unwrap_err()
            .to_string()
            .contains("unchanged source revision")
    );
    assert!(
        backend
            .commit_prepared_namespace_transaction(
                &fence.operation_id,
                &fence.operation_digest,
                fence.owner_epoch
            )
            .is_err()
    );
    backend
        .abort_prepared_namespace_transaction(
            &fence.operation_id,
            &fence.operation_digest,
            fence.owner_epoch,
        )
        .unwrap();
    assert_eq!(
        backend.get_object(&current.object_id).unwrap(),
        Some(current.clone())
    );
    assert!(
        backend
            .get_version(&current.object_id, &current.version)
            .unwrap()
            .is_none()
    );
}

#[test]
fn default_layout_fence_requires_v6_never_commits_and_only_its_owner_can_evict() {
    let root = temp_root("default-layout-reserved-fence");
    let shard = DfsMetadataShardId::new("root");
    let current = object(version("layout-fence-source"));
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), &root).unwrap();
    backend.put_file_system(&file_system()).unwrap();
    let original = object_version_record(
        &current,
        current.version.clone(),
        OffsetDateTime::UNIX_EPOCH,
    );
    backend.put_object_and_version(&current, &original).unwrap();
    let mut fence = namespace_prepare(
        "default-layout-fence-v1:test",
        &shard.0,
        &current,
        current.clone(),
    );
    fence.transaction.objects.clear();
    let mut unchanged = original.clone();
    unchanged.metadata_format = crate::DfsRevisionMetadataFormat::NamespacedV6;
    fence.transaction.versions = vec![unchanged];
    fence.transaction.transaction_id = "default-layout-fence-tx-v1:test".into();
    fence.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&fence).unwrap();
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV3);
    assert!(
        backend
            .prepare_namespace_transaction(&fence)
            .unwrap_err()
            .to_string()
            .contains("packed-v6")
    );
    assert!(crate::metadata_prepare_codec::DfsPackedNamespacePrepareV2::pack_v3(&fence).is_err());
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV6);
    backend.prepare_namespace_transaction(&fence).unwrap();
    assert_eq!(
        crate::inspect_metadata_snapshot_reader_floor(&root)
            .unwrap()
            .unwrap()
            .format_version,
        6
    );
    assert_eq!(
        backend.get_object(&current.object_id).unwrap(),
        Some(current.clone())
    );
    assert_eq!(
        backend
            .get_version(&current.object_id, &current.version)
            .unwrap(),
        Some(original)
    );
    let mut moved = current.clone();
    moved.path = "/services/layout-fence-target".into();
    moved.version = version("layout-fence-target");
    moved.changed_at_unix_nanos += 1;
    let mut overlapping =
        namespace_prepare("layout-other-writer", &shard.0, &current, moved.clone());
    assert!(
        backend
            .get_object_by_path(&current.file_system_id, &moved.path)
            .unwrap()
            .is_none()
    );
    overlapping
        .expected_path_occupants
        .push(DfsNamespacePathOccupantExpectation {
            path: moved.path.clone(),
            expected_object_id: None,
        });
    crate::metadata_prepare_codec::namespace_prepare_for_codec_authoring(
        &mut overlapping,
        DfsMetadataNamespacePrepareCodec::PackedV6,
    );
    overlapping.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&overlapping).unwrap();
    let overlap_error = backend
        .prepare_namespace_transaction(&overlapping)
        .unwrap_err();
    assert!(
        overlap_error.to_string().contains("overlaps"),
        "{overlap_error}"
    );
    assert!(
        backend
            .commit_prepared_namespace_transaction(
                &fence.operation_id,
                &fence.operation_digest,
                fence.owner_epoch
            )
            .unwrap_err()
            .to_string()
            .contains("never committed")
    );
    assert_eq!(
        backend
            .get_namespace_prepare(&fence.operation_id)
            .unwrap()
            .unwrap()
            .status,
        DfsNamespacePrepareStatus::Prepared
    );
    let sequence = backend.journal_last_sequence();
    let target = DfsMetadataShardId::new("services");
    assert!(
        backend
            .put_cross_shard_tombstone_if_unchanged(
                &current, &moved, &shard, &target, sequence, None
            )
            .unwrap_err()
            .to_string()
            .contains("active namespace prepare")
    );
    let mut wrong = fence.clone();
    wrong.operation_digest = "blake3:wrong-fence".into();
    assert!(
        backend
            .put_cross_shard_tombstone_if_unchanged_with_fence(
                &current, &moved, &shard, &target, sequence, &wrong
            )
            .is_err()
    );
    assert!(matches!(
        backend
            .put_cross_shard_tombstone_if_unchanged_with_fence(
                &current, &moved, &shard, &target, sequence, &fence
            )
            .unwrap(),
        DfsConditionalSourceEviction::Applied { .. }
    ));
    assert_eq!(
        backend
            .default_layout_migration_fence_for_object(&current.file_system_id, &current.object_id)
            .unwrap(),
        Some(fence.clone()),
        "indexed recovery retains the original fence after source eviction changes its head"
    );
    backend
        .abort_prepared_namespace_transaction(
            &fence.operation_id,
            &fence.operation_digest,
            fence.owner_epoch,
        )
        .unwrap();
    assert!(
        backend
            .default_layout_migration_fence_for_object(&current.file_system_id, &current.object_id)
            .unwrap()
            .is_none()
    );
    drop(backend);
    let mut reopened = LocalSobjMetadataBackend::new(shard, &root).unwrap();
    assert_eq!(
        reopened
            .get_namespace_prepare(&fence.operation_id)
            .unwrap()
            .unwrap()
            .status,
        DfsNamespacePrepareStatus::Aborted
    );
    assert!(
        reopened
            .commit_prepared_namespace_transaction(
                &fence.operation_id,
                &fence.operation_digest,
                fence.owner_epoch
            )
            .is_err()
    );
}

#[test]
fn empty_prepare_requires_a_v6_reference_outcome_and_source_head_witness() {
    let root = temp_root("reference-noop-prepare-shape");
    let shard = DfsMetadataShardId::new("root");
    let current = object(version("reference-noop-current"));
    let mut backend = LocalSobjMetadataBackend::new(shard.clone(), &root).unwrap();
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV6);
    backend.put_file_system(&file_system()).unwrap();
    backend.put_object(&current).unwrap();
    let mut prepare = namespace_prepare("reference-noop", &shard.0, &current, current.clone());
    prepare.transaction.objects.clear();
    prepare.transaction.versions.clear();
    prepare.transaction.advisory_lock_states.clear();
    prepare.transaction.file_version_pins = Some(DfsFileVersionPinMutation::default());
    prepare.transaction.transaction_id = format!("file-history:{}", sinomo_core::Id::generate());
    prepare.transaction.content_reference_identity = Some(crate::DfsRevisionReference {
        file_system_id: current.file_system_id.clone(),
        object_id: current.object_id.clone(),
        version: current.version.clone(),
    });
    prepare.transaction.contribution_digest =
        Some(blake3::hash(b"definitive release").to_hex().to_string());
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare).unwrap();

    let mut arbitrary = prepare.clone();
    arbitrary.transaction.content_reference_identity = None;
    assert!(
        super::super::namespace_prepare::validate_namespace_prepare_shape(
            &backend.tables,
            &shard,
            &arbitrary
        )
        .err()
        .expect("an unannotated empty prepare must be rejected")
        .to_string()
        .contains("empty transaction")
    );
    let mut no_digest = prepare.clone();
    no_digest.transaction.contribution_digest = None;
    assert!(
        super::super::namespace_prepare::validate_namespace_prepare_shape(
            &backend.tables,
            &shard,
            &no_digest
        )
        .is_err()
    );
    let mut no_source_witness = prepare.clone();
    no_source_witness.expected_object_versions.clear();
    assert!(
        super::super::namespace_prepare::validate_namespace_prepare_shape(
            &backend.tables,
            &shard,
            &no_source_witness
        )
        .err()
        .expect("an empty prepare without its source witness must be rejected")
        .to_string()
        .contains("empty transaction")
    );
    assert!(
        crate::DfsPackedNamespacePrepareV2::pack_v3(&prepare).is_err(),
        "old codecs must not author minimal reference outcomes"
    );
    backend.prepare_namespace_transaction(&prepare).unwrap();
    assert_eq!(
        backend.get_object(&current.object_id).unwrap(),
        Some(current)
    );
    assert_eq!(
        backend
            .get_namespace_prepare(&prepare.operation_id)
            .unwrap(),
        Some(prepare)
    );
}

#[test]
fn namespace_prepare_table_log_references_authoritative_journal_and_reopens() {
    let root = temp_root("local_backend_prepare_table_journal_reference");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let current = object(version("version_prepare_table_reference_1"));
    let mut committed = current.clone();
    committed.version = version("version_prepare_table_reference_2");
    committed.mode = 0o600;
    let prepare = namespace_prepare(
        "operation-prepare-table-reference",
        &shard_id.0,
        &current,
        committed,
    );

    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV1);
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    let entry = backend
        .prepare_namespace_transaction(&prepare)
        .expect("durable hidden prepare");
    assert!(matches!(
        &entry.mutation,
        DfsMetadataMutation::PutNamespacePrepare {
            prepare: None,
            packed_prepare_v1: Some(_),
            ..
        }
    ));
    let authoritative_sequence = backend.journal_last_sequence();
    let authoritative_snapshot = backend.export_table_snapshot();

    let prepare_batch = metadata_table_batches(&root)
        .into_iter()
        .find(|batch| batch.sequence == entry.sequence)
        .expect("prepare table batch");
    assert!(
        prepare_batch.ops.iter().any(|op| matches!(
            op,
            DfsMetadataTableOp::PutNamespacePrepareJournalReference {
                shard_id: referenced_shard_id,
                journal_sequence,
                operation_id,
                operation_digest,
                owner_epoch,
                status: DfsNamespacePrepareStatus::Prepared,
                ..
            } if referenced_shard_id == &shard_id
                && *journal_sequence == entry.sequence
                && operation_id == &prepare.operation_id
                && operation_digest == &prepare.operation_digest
                && *owner_epoch == prepare.owner_epoch
        )),
        "the rebuildable table batch must retain a fenced journal reference"
    );
    assert!(
        !prepare_batch
            .ops
            .iter()
            .any(|op| matches!(op, DfsMetadataTableOp::PutNamespacePrepare { .. })),
        "the table log must not encode the full prepared transaction a second time"
    );
    let authoritative_record_bytes =
        sinomo_object_serde::to_bytes(&entry, 1, sinomo_core::object::LengthMode::U32)
            .expect("encode authoritative prepare for test accounting")
            .len()
            .saturating_add(std::mem::size_of::<u32>());
    assert!(
        prepare_batch.journal_reference_replay_cost_bytes
            >= u64::try_from(authoritative_record_bytes).unwrap_or(u64::MAX),
        "the compact table batch must retain at least the authoritative encoded replay cost"
    );

    drop(backend);
    let reopened =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("reopened backend");
    assert_eq!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("reopened prepare"),
        Some(prepare.clone()),
        "table replay must resolve the complete prepare from the journal"
    );
    drop(reopened);

    let segment_id = crate::metadata_journal::journal_segment_id(entry.sequence);
    fs::remove_file(
        root.join("journal")
            .join("segments")
            .join(format!("{segment_id:020}.slog")),
    )
    .expect("remove referenced journal segment");
    let error = match LocalSobjMetadataBackend::new(shard_id.clone(), &root) {
        Ok(_) => panic!("missing referenced journal entry must fail closed"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("cannot resolve namespace prepare"),
        "unexpected missing journal reference error: {error}"
    );

    let repair_only = LocalSobjMetadataBackend::open_for_replica_repair(shard_id.clone(), &root)
        .expect("non-primary repair-only open must ignore the unresolved table-log suffix");
    assert_eq!(
        repair_only
            .get_namespace_prepare(&prepare.operation_id)
            .expect("repair-only snapshot read"),
        None,
        "repair-only reads must expose only the last self-contained snapshot"
    );
    drop(repair_only);

    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        prepare.file_system_id.clone(),
        shard_id.clone(),
        node("node_prepare_table_repair_source"),
        1,
    );
    let (repaired, archived) =
        LocalSobjMetadataBackend::replace_unreadable_with_replicated_snapshot(
            &permit,
            shard_id.clone(),
            &root,
            shard_id.clone(),
            authoritative_sequence,
            authoritative_snapshot,
        )
        .expect("install authoritative snapshot over unresolved table reference");
    assert!(archived);
    assert_eq!(
        repaired
            .get_namespace_prepare(&prepare.operation_id)
            .expect("repaired prepare"),
        Some(prepare.clone())
    );
    drop(repaired);

    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen repaired backend");
    assert_eq!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("reopened repaired prepare"),
        Some(prepare)
    );
    cleanup(&root);
}

#[test]
fn packed_namespace_prepare_replays_with_exact_prefix_and_canonical_snapshot() {
    let root = temp_root("packed_namespace_prepare_replay");
    let source_root = root.join("source");
    let replica_root = root.join("replica");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let current = object(version("version_packed_prepare_base"));
    let mut committed = current.clone();
    committed.version = version("version_packed_prepare_committed");
    committed.mode = 0o600;
    let prepare = namespace_prepare("operation-packed-prepare", &shard_id.0, &current, committed);
    let mut config = shard_config(&shard_id.0, DfsMetadataShardAssignment::Root);
    config.epoch = prepare.owner_epoch;
    config.namespace_prepare_codec = DfsMetadataNamespacePrepareCodec::PackedV2;

    let mut source =
        LocalShardedMetadataBackend::new(&source_root, vec![config.clone()]).expect("source");
    source.put_file_system(&file_system()).expect("filesystem");
    source.put_object(&current).expect("current object");
    crate::metadata_records::reset_namespace_prepare_payload_validation_passes();
    source
        .prepare_namespace_transaction(&prepare)
        .expect("packed namespace prepare");
    assert_eq!(
        crate::metadata_records::namespace_prepare_payload_validation_passes(),
        0,
        "trusted packed-v2 construction and its journal-reference table op must not rescan the freshly built body"
    );

    let entries = source.journal_since(0).expect("source journal");
    let prepare_entry = entries.last().expect("prepare journal entry");
    let DfsMetadataMutation::PutNamespacePrepare {
        prepare: canonical,
        packed_prepare_v2,
        ..
    } = &prepare_entry.mutation
    else {
        panic!("last journal entry must be a namespace prepare");
    };
    assert!(
        canonical.is_none(),
        "packed activation must not retain a second canonical body"
    );
    assert!(
        packed_prepare_v2.is_some(),
        "packed activation must emit the packed v2 body"
    );
    let catch_up_batch = source
        .shard_backend(&shard_id)
        .expect("source shard backend")
        .journal_batch_since_limited(prepare_entry.sequence.saturating_sub(1), 1, 8 * 1024 * 1024)
        .expect("hydrate packed-v2 catch-up batch")
        .expect("packed-v2 catch-up batch");
    assert!(matches!(
        catch_up_batch.entries.as_slice(),
        [DfsMetadataJournalEntry {
            mutation: DfsMetadataMutation::PutNamespacePrepare {
                prepare: None,
                packed_prepare_v1: None,
                packed_prepare_v2: Some(_),
                ..
            },
            ..
        }]
    ));
    require_journal_entry_replay_payloads(&catch_up_batch.entries[0])
        .expect("hydration must preserve the sole packed-v2 representation");
    assert_eq!(
        prepare_entry
            .mutation
            .namespace_prepare_payload()
            .expect("decode packed prepare")
            .as_deref(),
        Some(&prepare)
    );
    let source_prefix = DfsMetadataBackend::metadata_prefix_proof(&source, &shard_id)
        .expect("source prefix proof")
        .expect("source shard prefix proof");
    let operation = crate::service_replica::metadata_authority_operation_for_namespace_prepare(
        &prepare,
        prepare_entry.sequence,
    )
    .expect("namespace authority operation");
    let probe = DfsMetadataAuthorityProbe {
        probe_id: "probe-packed-prepare".to_string(),
        expected_shard: config.clone(),
        operation,
        durable_journal_ack: false,
        journal_frames: Vec::new(),
        prepared_prefix: Some(source_prefix.clone()),
    };
    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        config.file_system_id.clone(),
        shard_id.clone(),
        config.primary_node_id.clone(),
        config.epoch,
    );
    let replay_batch = DfsMetadataJournalBatch {
        shard_id: shard_id.clone(),
        next_after_sequence: prepare_entry.sequence,
        entries: entries.clone(),
    };

    let mut replica =
        LocalShardedMetadataBackend::new(&replica_root, vec![config.clone()]).expect("replica");
    crate::metadata_records::reset_namespace_prepare_payload_validation_passes();
    crate::service_replica::reset_namespace_prepare_canonical_digest_passes();
    let apply_report = replica
        .apply_journal_batch_with_replica_permit_report(&permit, &replay_batch)
        .expect("incremental packed replay");
    assert_eq!(apply_report.applied_entries(), replay_batch.entries.len());
    assert_eq!(
        crate::metadata_records::namespace_prepare_payload_validation_passes(),
        1,
        "replica persistence, materialization, and journal-reference planning must share one packed-v2 validation proof"
    );
    assert_eq!(
        crate::service_replica::namespace_prepare_canonical_digest_passes(),
        1,
        "a newly materialized packed prepare needs one independent canonical digest"
    );
    let confirmation_id =
        crate::service_replica::verify_persisted_metadata_authority_probe_after_apply(
            &replica,
            &config,
            &probe,
            &apply_report,
        )
        .expect("fresh replica authority proof");
    assert_eq!(
        Some(confirmation_id),
        crate::service_replica::metadata_authority_probe_confirmation_id(&probe)
    );
    assert_eq!(
        crate::service_replica::namespace_prepare_canonical_digest_passes(),
        1,
        "the immediate authority ACK must reuse the apply digest instead of rehashing the table record"
    );
    let mut mismatched_probe = probe.clone();
    let DfsMetadataAuthorityOperation::NamespaceMutation { payload_digest, .. } =
        &mut mismatched_probe.operation
    else {
        unreachable!("fixture creates a namespace operation")
    };
    *payload_digest = "blake3:mismatched-packed-prepare".to_string();
    crate::service_replica::reset_namespace_prepare_canonical_digest_passes();
    let mismatch_error =
        crate::service_replica::verify_persisted_metadata_authority_probe_after_apply(
            &replica,
            &config,
            &mismatched_probe,
            &apply_report,
        )
        .expect_err("mismatched compact proof must use and fail durable verification");
    assert!(
        mismatch_error
            .to_string()
            .contains("mismatched operation digest")
    );
    assert_eq!(
        crate::service_replica::namespace_prepare_canonical_digest_passes(),
        1,
        "compact evidence must never bypass durable verification for a different operation"
    );
    assert_eq!(
        DfsMetadataBackend::metadata_prefix_proof(&replica, &shard_id)
            .expect("replica prefix proof")
            .expect("replica shard prefix proof"),
        source_prefix,
        "replica prefix hashing must preserve the exact packed journal bytes"
    );
    assert_eq!(
        replica
            .get_namespace_prepare(&prepare.operation_id)
            .expect("replica prepare"),
        Some(prepare.clone())
    );

    crate::metadata_records::reset_namespace_prepare_payload_validation_passes();
    crate::service_replica::reset_namespace_prepare_canonical_digest_passes();
    let duplicate_report = replica
        .apply_journal_batch_with_replica_permit_report(&permit, &replay_batch)
        .expect("duplicate packed replay");
    assert_eq!(duplicate_report.applied_entries(), 0);
    crate::service_replica::verify_persisted_metadata_authority_probe_after_apply(
        &replica,
        &config,
        &probe,
        &duplicate_report,
    )
    .expect("duplicate replay durable fallback");
    assert_eq!(
        crate::service_replica::namespace_prepare_canonical_digest_passes(),
        1,
        "a duplicate apply has no fresh proof and must independently verify durable state"
    );

    let snapshot = source
        .export_shard_table_snapshot(&shard_id)
        .expect("export shard snapshot")
        .expect("source shard snapshot");
    assert!(
        snapshot
            .tables
            .as_ref()
            .expect("structured snapshot tables")
            .namespace_prepares
            .contains(&prepare),
        "snapshots must remain canonical across journal codec changes"
    );

    drop(replica);
    crate::metadata_records::reset_namespace_prepare_payload_validation_passes();
    crate::service_replica::reset_namespace_prepare_canonical_digest_passes();
    let replica = LocalShardedMetadataBackend::new(&replica_root, vec![config.clone()])
        .expect("reopen replica");
    assert!(
        crate::metadata_records::namespace_prepare_payload_validation_passes() <= 1,
        "reopen must never validate a journal-referenced packed-v2 prepare more than once"
    );
    assert_eq!(
        replica
            .get_namespace_prepare(&prepare.operation_id)
            .expect("reopened replica prepare"),
        Some(prepare.clone()),
        "journal-referenced table replay must resolve packed prepare bodies"
    );
    crate::service_replica::verify_persisted_metadata_authority_probe_after_apply(
        &replica,
        &config,
        &probe,
        &DfsMetadataReplicaApplyReport::default(),
    )
    .expect("reopened replica durable authority proof");
    assert_eq!(
        crate::service_replica::namespace_prepare_canonical_digest_passes(),
        1,
        "recovery must retain the independent durable-table verification path"
    );

    let mut ambiguous = prepare_entry.clone();
    let DfsMetadataMutation::PutNamespacePrepare {
        prepare: canonical,
        packed_prepare_v2,
        ..
    } = &mut ambiguous.mutation
    else {
        panic!("prepare entry changed mutation kind");
    };
    assert!(packed_prepare_v2.is_some());
    *canonical = Some(Box::new(prepare));
    let error = require_journal_entry_replay_payloads(&ambiguous)
        .expect_err("dual prepare representations must fail replay");
    assert!(
        error.to_string().contains("both canonical and packed"),
        "unexpected ambiguous prepare error: {error}"
    );

    cleanup(&root);
}

#[test]
fn local_backend_namespace_prepare_is_hidden_atomic_and_snapshot_durable() {
    let root = temp_root("local_backend_namespace_prepare_hidden_atomic_snapshot");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let file_system = file_system();
    let current = object(version("version_prepare_1"));
    let mut committed = current.clone();
    committed.version = version("version_prepare_2");
    committed.changed_at_unix_nanos = 2;
    let prepare = namespace_prepare(
        "operation-hidden-commit",
        &shard_id.0,
        &current,
        committed.clone(),
    );
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&current).expect("current object");

    crate::metadata_tables::reset_namespace_prepare_footprint_constructions();
    let prepare_entry = backend
        .prepare_namespace_transaction(&prepare)
        .expect("prepare namespace transaction");
    assert_eq!(
        crate::metadata_tables::namespace_prepare_footprint_constructions(),
        1,
        "prepare admission must construct its validated footprint once"
    );
    assert!(matches!(
        prepare_entry.mutation,
        DfsMetadataMutation::PutNamespacePrepare {
            status: DfsNamespacePrepareStatus::Prepared,
            ..
        }
    ));
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("visible object"),
        Some(current.clone()),
        "installing a hidden prepare must not apply its namespace transaction"
    );
    assert_eq!(
        backend
            .get_namespace_prepare(&prepare.operation_id)
            .expect("durable prepare"),
        Some(prepare.clone())
    );

    backend
        .compact_metadata_table_log_if_needed(0)
        .expect("force prepare snapshot");
    drop(backend);
    let mut reopened =
        LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("reopen prepared");
    assert_eq!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("snapshotted prepare"),
        Some(prepare.clone()),
        "structured snapshots must preserve hidden prepares"
    );
    assert!(
        reopened
            .tables
            .namespace_prepare_footprint_indexes_match_history(),
        "structured snapshot hydration must reconstruct active prepare ownership"
    );
    assert_eq!(
        reopened
            .get_object(&current.object_id)
            .expect("reopened visible object"),
        Some(current.clone())
    );

    let fence_error = reopened
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            "blake3:different-operation",
            prepare.owner_epoch,
        )
        .expect_err("digest reuse must be fenced");
    assert!(fence_error.to_string().contains("digest/epoch fence"));
    assert_eq!(
        reopened
            .get_object(&current.object_id)
            .expect("visible object after bad fence"),
        Some(current.clone())
    );

    reset_namespace_prepared_commit_reference_planner_calls();
    crate::metadata_tables::reset_namespace_prepare_footprint_constructions();
    let commit_entry = reopened
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("commit prepared namespace transaction");
    assert_eq!(
        crate::metadata_tables::namespace_prepare_footprint_constructions(),
        0,
        "normal Prepared -> Committed publication must not rebuild a validated footprint"
    );
    assert_eq!(
        namespace_prepared_commit_reference_planner_calls(),
        0,
        "the normal prepared commit must preapply directly instead of using the reference clone planner"
    );
    assert!(matches!(
        commit_entry.mutation,
        DfsMetadataMutation::CommitNamespacePrepare { .. }
    ));
    assert_eq!(
        reopened
            .get_object(&current.object_id)
            .expect("committed object"),
        Some(committed.clone())
    );
    assert!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("committed prepare")
            .is_none(),
        "committing must release the full prepared body"
    );
    let prepare_receipt = reopened
        .tables
        .namespace_prepare_receipts
        .get(&prepare.operation_id)
        .cloned()
        .expect("compact committed prepare receipt");
    assert_eq!(prepare_receipt.status, DfsNamespacePrepareStatus::Committed);
    assert_eq!(prepare_receipt.retained_at, commit_entry.created_at);
    assert!(
        reopened
            .tables
            .namespace_prepare_footprint_indexes_match_history(),
        "Prepared -> Committed table apply must release active ownership"
    );
    assert!(
        reopened
            .get_namespace_transaction(&prepare.transaction.transaction_id)
            .expect("committed transaction")
            .is_none(),
        "the canonical table state and compact receipt replace the full transaction"
    );
    let transaction_receipt = reopened
        .tables
        .namespace_transaction_receipts
        .get(&prepare.transaction.transaction_id)
        .cloned()
        .expect("compact committed transaction receipt");
    assert!(
        transaction_receipt
            .matches_transaction(&prepare.transaction)
            .expect("transaction receipt identity")
    );
    assert_eq!(transaction_receipt.retained_at, commit_entry.created_at);
    let repeated_commit = reopened
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("repeated committed transaction uses the idempotent recovery path");
    assert!(matches!(
        repeated_commit.mutation,
        DfsMetadataMutation::PutNamespacePrepareReceipt { .. }
    ));
    assert_eq!(
        reopened
            .get_object(&current.object_id)
            .expect("object after repeated commit"),
        Some(committed.clone())
    );

    drop(reopened);
    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen committed");
    assert_eq!(
        reopened
            .get_object(&current.object_id)
            .expect("durable committed object"),
        Some(committed)
    );
    assert!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("durable committed prepare")
            .is_none()
    );
    assert_eq!(
        reopened
            .tables
            .namespace_prepare_receipts
            .get(&prepare.operation_id),
        Some(&prepare_receipt)
    );
    assert_eq!(
        reopened
            .tables
            .namespace_transaction_receipts
            .get(&prepare.transaction.transaction_id),
        Some(&transaction_receipt)
    );
    assert!(
        reopened
            .tables
            .namespace_prepare_footprint_indexes_match_history(),
        "snapshot plus table-log replay must retain terminal release"
    );
    let mut reuse_after_reopen = prepare;
    reuse_after_reopen.operation_id = "operation-reuse-after-reopen".to_string();
    reuse_after_reopen.transaction.transaction_id = "transaction:reuse-after-reopen".to_string();
    reset_namespace_prepare_history_scan_calls();
    let footprint = namespace_prepare_footprint(&reuse_after_reopen);
    validate_namespace_prepare_does_not_overlap(&reopened.tables, &reuse_after_reopen, &footprint)
        .expect("terminal history must not block its released footprint after reopen");
    assert_eq!(namespace_prepare_history_scan_calls(), 0);

    cleanup(&root);
}

#[test]
fn local_backend_namespace_prepare_abort_is_fenced_durable_and_releases_footprint() {
    let root = temp_root("local_backend_namespace_prepare_abort");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let file_system = file_system();
    let current = object(version("version_prepare_abort_1"));
    let mut hidden = current.clone();
    hidden.version = version("version_prepare_abort_2");
    hidden.changed_at_unix_nanos = 2;
    let prepare = namespace_prepare("operation-abort", &shard_id.0, &current, hidden.clone());
    backend.put_file_system(&file_system).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .prepare_namespace_transaction(&prepare)
        .expect("prepare namespace transaction");

    let fence_error = backend
        .abort_prepared_namespace_transaction(
            &prepare.operation_id,
            "blake3:different-operation",
            prepare.owner_epoch,
        )
        .expect_err("digest reuse must be fenced");
    assert!(fence_error.to_string().contains("digest/epoch fence"));

    let abort_entry = backend
        .abort_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("abort namespace transaction");
    assert!(matches!(
        abort_entry.mutation,
        DfsMetadataMutation::PutNamespacePrepare {
            status: DfsNamespacePrepareStatus::Aborted,
            ..
        }
    ));
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("visible object"),
        Some(current.clone()),
        "aborting a hidden prepare must not publish its transaction"
    );
    assert!(
        backend
            .tables
            .namespace_prepare_footprint_indexes_match_history()
    );

    let commit_error = backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect_err("an aborted prepare cannot commit");
    assert!(commit_error.to_string().contains("is aborted"));
    backend
        .abort_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("repeated abort is idempotent");

    let mut replacement = prepare.clone();
    replacement.operation_id = "operation-after-abort".to_string();
    replacement.transaction.transaction_id = "transaction:after-abort".to_string();
    replacement.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&replacement)
            .expect("replacement digest");
    backend
        .prepare_namespace_transaction(&replacement)
        .expect("aborted footprint is reusable");

    drop(backend);
    let reopened = LocalSobjMetadataBackend::new(shard_id, &root).expect("reopen backend");
    assert_eq!(
        reopened
            .get_namespace_prepare(&prepare.operation_id)
            .expect("aborted prepare")
            .map(|record| record.status),
        Some(DfsNamespacePrepareStatus::Aborted)
    );
    assert!(
        reopened
            .tables
            .namespace_prepare_footprint_indexes_match_history()
    );
    cleanup(&root);
}

#[test]
fn local_backend_rebinds_exact_prepared_namespace_with_epoch_fences() {
    let root = temp_root("local_backend_rebinds_prepared_namespace");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let current = object(version("version_rebind_prepare_1"));
    let mut committed = current.clone();
    committed.version = version("version_rebind_prepare_2");
    committed.changed_at_unix_nanos = 2;
    let prepare = namespace_prepare("operation-rebind", &shard_id.0, &current, committed.clone());
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .prepare_namespace_transaction(&prepare)
        .expect("initial prepare");

    let mut rebound = prepare.clone();
    rebound.owner_epoch = prepare.owner_epoch + 1;
    rebound.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&rebound)
            .expect("rebound digest");

    let stale_fence = backend
        .rebind_prepared_namespace_transaction(
            &prepare.operation_id,
            "blake3:stale",
            prepare.owner_epoch,
            &rebound,
        )
        .expect_err("stale digest fence must reject rebind");
    assert!(stale_fence.to_string().contains("fence mismatch"));
    assert_eq!(
        backend
            .get_namespace_prepare(&prepare.operation_id)
            .expect("prepare"),
        Some(prepare.clone())
    );

    let mut changed_payload = rebound.clone();
    changed_payload.transaction.objects[0].mode ^= 0o100;
    changed_payload.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&changed_payload)
            .expect("changed digest");
    let payload_error = backend
        .rebind_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
            &changed_payload,
        )
        .expect_err("rebind must preserve the exact namespace payload");
    assert!(payload_error.to_string().contains("operation payload"));

    backend
        .rebind_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
            &rebound,
        )
        .expect("rebind prepared namespace");
    backend
        .rebind_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
            &rebound,
        )
        .expect("exact lost-response rebind retry is idempotent");

    let mut concurrent = rebound.clone();
    concurrent.owner_epoch += 1;
    concurrent.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&concurrent)
            .expect("concurrent digest");
    let concurrent_error = backend
        .rebind_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
            &concurrent,
        )
        .expect_err("the consumed old fence must reject a concurrent rebind");
    assert!(concurrent_error.to_string().contains("fence mismatch"));
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("hidden object"),
        Some(current.clone()),
        "rebinding must not make the transaction visible"
    );
    backend
        .commit_prepared_namespace_transaction(
            &rebound.operation_id,
            &rebound.operation_digest,
            rebound.owner_epoch,
        )
        .expect("commit rebound prepare");
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("visible object"),
        Some(committed)
    );
    cleanup(&root);
}

#[test]
fn local_backend_namespace_prepare_rejects_stale_overlap_and_commit_races() {
    let root = temp_root("local_backend_namespace_prepare_stale_overlap_commit_race");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let current = object(version("version_prepare_race_1"));
    let mut committed = current.clone();
    committed.version = version("version_prepare_race_2");
    committed.changed_at_unix_nanos = 2;
    let prepare = namespace_prepare("operation-race", &shard_id.0, &current, committed);
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .prepare_namespace_transaction(&prepare)
        .expect("first prepare");

    let mut stale = prepare.clone();
    stale.operation_id = "operation-stale".to_string();
    stale.operation_digest = "blake3:operation-stale".to_string();
    stale.transaction.transaction_id = "transaction:operation-stale".to_string();
    stale.expected_object_versions[0].expected_version = Some(version("version_prepare_stale"));
    let stale_error = backend
        .prepare_namespace_transaction(&stale)
        .expect_err("stale object version must reject prepare");
    assert!(matches!(
        stale_error,
        MetadataBackendError::Conflict {
            ref object_id,
            ref expected,
            current: ref current_version,
        } if object_id == &current.object_id
            && expected == &version("version_prepare_stale")
            && current_version == &version("version_prepare_race_1")
    ));

    let mut overlap = prepare.clone();
    overlap.operation_id = "operation-overlap".to_string();
    overlap.operation_digest = "blake3:operation-overlap".to_string();
    overlap.transaction.transaction_id = "transaction:operation-overlap".to_string();
    reset_namespace_prepare_history_scan_calls();
    let overlap_error = backend
        .prepare_namespace_transaction(&overlap)
        .expect_err("active footprint overlap must reject prepare");
    assert_eq!(
        namespace_prepare_history_scan_calls(),
        0,
        "ordinary prepare admission must not scan retained prepare history"
    );
    assert!(
        overlap_error
            .to_string()
            .contains("overlaps active prepare")
    );

    let mut raced = current.clone();
    raced.version = version("version_prepare_race_3");
    raced.changed_at_unix_nanos = 3;
    backend
        .put_object(&raced)
        .expect("concurrent visible mutation");
    let commit_error = backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect_err("commit must revalidate the prepared expectations");
    assert!(matches!(
        commit_error,
        MetadataBackendError::Conflict {
            ref object_id,
            ref expected,
            current: ref current_version,
        } if object_id == &raced.object_id
            && expected == &version("version_prepare_race_1")
            && current_version == &version("version_prepare_race_3")
    ));
    assert_eq!(
        backend.get_object(&raced.object_id).expect("raced object"),
        Some(raced)
    );
    assert_eq!(
        backend
            .get_namespace_prepare(&prepare.operation_id)
            .expect("still prepared")
            .map(|record| record.status),
        Some(DfsNamespacePrepareStatus::Prepared),
        "a failed commit must leave the hidden prepare and visible namespace unchanged"
    );

    cleanup(&root);
}

#[test]
fn local_backend_namespace_prepare_commit_revalidates_transaction_identity() {
    let root = temp_root("local_backend_namespace_prepare_transaction_identity_race");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let current_a = object(version("version_prepare_transaction_a_1"));
    let mut committed_a = current_a.clone();
    committed_a.version = version("version_prepare_transaction_a_2");
    let prepare_a = namespace_prepare(
        "operation-transaction-identity-a",
        &shard_id.0,
        &current_a,
        committed_a,
    );

    let mut current_b = object(version("version_prepare_transaction_b_1"));
    current_b.object_id =
        DfsObjectId::from_legacy_named("obj_prepare_transaction_b").expect("object id");
    current_b.path = "/docs/prepare-transaction-b".to_string();
    let mut committed_b = current_b.clone();
    committed_b.version = version("version_prepare_transaction_b_2");
    let mut prepare_b = namespace_prepare(
        "operation-transaction-identity-b",
        &shard_id.0,
        &current_b,
        committed_b,
    );
    prepare_b.transaction.transaction_id = prepare_a.transaction.transaction_id.clone();
    prepare_b.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare_b)
            .expect("second prepare digest");

    backend.put_object(&current_a).expect("current object a");
    backend.put_object(&current_b).expect("current object b");
    backend
        .prepare_namespace_transaction(&prepare_a)
        .expect("first disjoint prepare");
    backend
        .prepare_namespace_transaction(&prepare_b)
        .expect("second disjoint prepare with unresolved transaction id");
    backend
        .commit_prepared_namespace_transaction(
            &prepare_a.operation_id,
            &prepare_a.operation_digest,
            prepare_a.owner_epoch,
        )
        .expect("first transaction commit");

    let conflict = backend
        .commit_prepared_namespace_transaction(
            &prepare_b.operation_id,
            &prepare_b.operation_digest,
            prepare_b.owner_epoch,
        )
        .expect_err("second commit must revalidate transaction identity");
    assert!(
        conflict.to_string().contains("reuses transaction id"),
        "unexpected transaction identity error: {conflict}"
    );
    assert_eq!(
        backend
            .get_namespace_prepare(&prepare_b.operation_id)
            .expect("second prepare")
            .map(|prepare| prepare.status),
        Some(DfsNamespacePrepareStatus::Prepared)
    );
    assert_eq!(
        backend.get_object(&current_b.object_id).expect("object b"),
        Some(current_b)
    );
    cleanup(&root);
}

#[test]
fn local_backend_namespace_prepare_commit_revalidates_advisory_lock_state() {
    let root = temp_root("local_backend_namespace_prepare_advisory_lock_race");
    let shard_id = DfsMetadataShardId::new("shard-root");
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), &root).expect("backend");
    let current = object(version("version_prepare_lock_1"));
    let mut committed = current.clone();
    committed.version = version("version_prepare_lock_2");
    let mut prepare = namespace_prepare(
        "operation-advisory-lock-race",
        &shard_id.0,
        &current,
        committed,
    );
    let prepared_lock = DfsAdvisoryLockStateRecord::empty(
        current.file_system_id.clone(),
        current.object_id.clone(),
        7,
    );
    prepare
        .transaction
        .advisory_lock_states
        .push(prepared_lock.clone());
    prepare.operation_digest =
        crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare)
            .expect("prepare digest");
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    backend
        .prepare_namespace_transaction(&prepare)
        .expect("prepare with advisory-lock state");

    let mut raced_lock = prepared_lock;
    raced_lock.authority_epoch += 1;
    backend
        .tables
        .apply(DfsMetadataTableOp::PutAdvisoryLockState { state: raced_lock });
    let conflict = backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect_err("commit must revalidate advisory-lock state");
    assert!(
        matches!(&conflict, MetadataBackendError::AdvisoryLock(_)),
        "unexpected advisory-lock error: {conflict}"
    );
    assert_eq!(
        backend
            .get_namespace_prepare(&prepare.operation_id)
            .expect("prepare after lock race")
            .map(|prepare| prepare.status),
        Some(DfsNamespacePrepareStatus::Prepared)
    );
    cleanup(&root);
}

#[test]
fn namespace_prepare_footprint_index_matches_reference_lifecycle_and_scope() {
    fn assert_overlap_equivalent(
        tables: &DfsMetadataTables,
        candidate: &DfsNamespacePrepareRecord,
        expected_accept: bool,
    ) {
        reset_namespace_prepare_history_scan_calls();
        let footprint = namespace_prepare_footprint(candidate);
        let indexed =
            validate_namespace_prepare_does_not_overlap(tables, candidate, &footprint).is_ok();
        assert_eq!(
            namespace_prepare_history_scan_calls(),
            0,
            "the indexed validator must never call the history-scan oracle"
        );
        let reference =
            validate_namespace_prepare_does_not_overlap_reference(tables, candidate).is_ok();
        assert_eq!(indexed, reference, "indexed/reference decision mismatch");
        assert_eq!(indexed, expected_accept, "unexpected overlap decision");
    }

    let shard_id = DfsMetadataShardId::new("shard-root");
    let current = object(version("version_footprint_active_1"));
    let mut committed = current.clone();
    committed.version = version("version_footprint_active_2");
    let active = namespace_prepare(
        "operation-footprint-active",
        &shard_id.0,
        &current,
        committed,
    );
    let mut tables = DfsMetadataTables::default();
    tables.apply(DfsMetadataTableOp::put_namespace_prepare(active.clone()));
    assert!(tables.namespace_prepare_footprint_indexes_match_history());

    assert_overlap_equivalent(&tables, &active, true);

    let mut same_object = current.clone();
    same_object.path = "/docs/object-conflict-only.md".to_string();
    let mut same_object_target = same_object.clone();
    same_object_target.version = version("version_footprint_object_conflict");
    let object_conflict = namespace_prepare(
        "operation-footprint-object-conflict",
        &shard_id.0,
        &same_object,
        same_object_target,
    );
    assert_overlap_equivalent(&tables, &object_conflict, false);

    let mut same_path = current.clone();
    same_path.object_id =
        DfsObjectId::from_legacy_named("obj_footprint_path_conflict").expect("object id");
    let mut same_path_target = same_path.clone();
    same_path_target.version = version("version_footprint_path_conflict");
    let path_conflict = namespace_prepare(
        "operation-footprint-path-conflict",
        &shard_id.0,
        &same_path,
        same_path_target,
    );
    assert_overlap_equivalent(&tables, &path_conflict, false);

    let mut disjoint = same_path.clone();
    disjoint.path = "/docs/disjoint.md".to_string();
    let mut disjoint_target = disjoint.clone();
    disjoint_target.version = version("version_footprint_disjoint");
    let disjoint_prepare = namespace_prepare(
        "operation-footprint-disjoint",
        &shard_id.0,
        &disjoint,
        disjoint_target,
    );
    assert_overlap_equivalent(&tables, &disjoint_prepare, true);

    let mut other_file_system = current.clone();
    other_file_system.file_system_id =
        DfsFileSystemId::from_legacy_named("sifs-footprint-other").expect("filesystem id");
    let mut other_file_system_target = other_file_system.clone();
    other_file_system_target.version = version("version_footprint_other_fs");
    let cross_file_system = namespace_prepare(
        "operation-footprint-other-fs",
        &shard_id.0,
        &other_file_system,
        other_file_system_target,
    );
    assert_overlap_equivalent(&tables, &cross_file_system, true);
    tables.apply(DfsMetadataTableOp::put_namespace_prepare(cross_file_system));
    assert!(tables.namespace_prepare_footprint_indexes_match_history());

    let mut committed_active = active.clone();
    committed_active.status = DfsNamespacePrepareStatus::Committed;
    tables.apply(DfsMetadataTableOp::put_namespace_prepare(committed_active));
    let mut replacement = active.clone();
    replacement.operation_id = "operation-footprint-replacement".to_string();
    replacement.transaction.transaction_id = "transaction:footprint-replacement".to_string();
    assert_overlap_equivalent(&tables, &replacement, true);
    tables.apply(DfsMetadataTableOp::put_namespace_prepare(
        replacement.clone(),
    ));

    let mut aborted_replacement = replacement.clone();
    aborted_replacement.status = DfsNamespacePrepareStatus::Aborted;
    tables.apply(DfsMetadataTableOp::put_namespace_prepare(
        aborted_replacement,
    ));
    let mut after_abort = replacement;
    after_abort.operation_id = "operation-footprint-after-abort".to_string();
    after_abort.transaction.transaction_id = "transaction:footprint-after-abort".to_string();
    assert_overlap_equivalent(&tables, &after_abort, true);
    assert!(tables.namespace_prepare_footprint_indexes_match_history());

    let before_preapply = tables.clone();
    let mut rollback = DfsMetadataTableRollback::default();
    tables.apply_with_rollback(
        DfsMetadataTableOp::put_namespace_prepare(after_abort.clone()),
        &mut rollback,
    );
    let mut blocked_during_preapply = after_abort;
    blocked_during_preapply.operation_id = "operation-footprint-preapplied-conflict".to_string();
    blocked_during_preapply.transaction.transaction_id =
        "transaction:footprint-preapplied-conflict".to_string();
    assert_overlap_equivalent(&tables, &blocked_during_preapply, false);
    rollback.rollback(&mut tables);
    assert_eq!(tables, before_preapply);
    assert!(tables.namespace_prepare_footprint_indexes_match_history());
}

#[test]
fn sharded_backend_namespace_prepare_routes_one_shard_and_commits_indexes() {
    let root = temp_root("sharded_backend_namespace_prepare_route_commit");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    let mut current = object(version("version_sharded_prepare_1"));
    current.path = "/services/prepared.txt".to_string();
    let mut committed = current.clone();
    committed.version = version("version_sharded_prepare_2");
    committed.changed_at_unix_nanos = 2;
    let mut prepare =
        namespace_prepare("operation-sharded", "services", &current, committed.clone());
    backend.put_file_system(&file_system()).expect("filesystem");
    backend.put_object(&current).expect("current object");
    crate::metadata_api::reset_namespace_prepare_normalization_passes();
    backend
        .prepare_namespace_transaction_in_place(&mut prepare)
        .expect("sharded prepare");
    assert_eq!(
        crate::metadata_api::namespace_prepare_normalization_passes(),
        1,
        "the in-place router and local store must consume the caller's one canonicalization"
    );
    assert_eq!(
        backend
            .get_object(&current.object_id)
            .expect("hidden object"),
        Some(current)
    );
    backend
        .commit_prepared_namespace_transaction(
            &prepare.operation_id,
            &prepare.operation_digest,
            prepare.owner_epoch,
        )
        .expect("sharded commit");
    assert_eq!(
        backend
            .get_object_by_path(&committed.file_system_id, &committed.path)
            .expect("committed path"),
        Some(committed.clone())
    );
    assert_eq!(
        backend
            .read_object_shard_index(&committed.object_id)
            .expect("object shard index")
            .map(|index| index.shard_id),
        Some(DfsMetadataShardId::new("services"))
    );

    cleanup(&root);
}

#[test]
fn sharded_namespace_prepare_expectations_use_one_exact_table_snapshot() {
    let root = temp_root("sharded_namespace_prepare_expectations_one_snapshot");
    let mut backend = LocalShardedMetadataBackend::new(
        &root,
        vec![
            shard_config("root", DfsMetadataShardAssignment::Root),
            shard_config(
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ],
    )
    .expect("sharded backend");
    backend.put_file_system(&file_system()).expect("filesystem");

    let mut moving = object(version("version_snapshot_moving_1"));
    moving.path = "/services/from.txt".to_string();
    let mut occupant = object(version("version_snapshot_occupant_1"));
    occupant.object_id =
        DfsObjectId::from_legacy_named("obj_namespace_snapshot_occupant").expect("object id");
    occupant.path = "/services/to.txt".to_string();
    backend.put_object(&moving).expect("moving object");
    backend.put_object(&occupant).expect("target occupant");

    let mut moved = moving.clone();
    moved.path = occupant.path.clone();
    moved.version = version("version_snapshot_moving_2");
    let transaction = DfsNamespaceTransaction {
        content_reference_identity: None,
        contribution_digest: None,
        extension_expectations: Vec::new(),
        file_version_pins: None,
        transaction_id: "transaction:one-snapshot".to_string(),
        file_system_id: moving.file_system_id.clone(),
        objects: vec![moved],
        versions: Vec::new(),
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };

    reset_metadata_current_tables_snapshot_calls();
    let (object_expectations, path_expectations, paths) = backend
        .namespace_prepare_expectations(&DfsMetadataShardId::new("services"), &transaction)
        .expect("namespace expectations");
    assert_eq!(
        metadata_current_tables_snapshot_calls(),
        1,
        "the sharded/local fast path must project every expectation from one table snapshot"
    );
    assert_eq!(
        paths,
        std::collections::BTreeSet::from([moving.path.clone(), occupant.path.clone()]),
        "the immutable snapshot must contribute the moving object's current path"
    );
    assert_eq!(object_expectations.len(), 2);
    assert_eq!(path_expectations.len(), 2);
    assert_eq!(
        object_expectations
            .iter()
            .find(|expected| expected.object_id == moving.object_id)
            .and_then(|expected| expected.expected_version.as_ref()),
        Some(&moving.version)
    );
    assert_eq!(
        object_expectations
            .iter()
            .find(|expected| expected.object_id == occupant.object_id)
            .and_then(|expected| expected.expected_version.as_ref()),
        Some(&occupant.version),
        "the target-path occupant version must come from the same snapshot"
    );
    assert_eq!(
        path_expectations
            .iter()
            .find(|expected| expected.path == moving.path)
            .and_then(|expected| expected.expected_object_id.as_ref()),
        Some(&moving.object_id)
    );
    assert_eq!(
        path_expectations
            .iter()
            .find(|expected| expected.path == occupant.path)
            .and_then(|expected| expected.expected_object_id.as_ref()),
        Some(&occupant.object_id)
    );

    cleanup(&root);
}
