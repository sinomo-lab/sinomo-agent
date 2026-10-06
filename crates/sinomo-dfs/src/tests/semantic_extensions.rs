//! Core extension authority, exact retry and legacy-encoding regressions.
use super::*;

fn metadata_value(generation: u32) -> DfsModuleMetadataValue {
    DfsModuleMetadataValue {
        schema_version: 7,
        payload: serde_json::json!({"generation": generation, "opaque": ["vendor-data"]}),
    }
}

fn identity(version: &DfsObjectVersionRecord) -> DfsRevisionReference {
    DfsRevisionReference {
        file_system_id: version.file_system_id.clone(),
        object_id: version.object_id.clone(),
        version: version.version.clone(),
    }
}

fn semantic_fixture(
    root: &Path,
) -> (
    NetworkedDfsService<ReceiveOnlyTransport>,
    DfsObjectMetadata,
    DfsObjectVersionRecord,
) {
    let owner = NodeId::from_legacy_named("semantic-extension-owner").unwrap();
    let mut object = file_metadata(owner.clone());
    object.path = "/services/semantic-source.txt".into();
    let content = LocalContentStore::new(root.join("content")).unwrap();
    let manifest = content
        .put_object_version(
            object.object_id.as_str(),
            object.version.as_str(),
            b"immutable source bytes",
        )
        .unwrap();
    object.content_hash = Some(manifest.content_hash);
    object.content_size_bytes = Some(manifest.size_bytes);
    let version = DfsObjectVersionRecord {
        metadata_format: DfsRevisionMetadataFormat::NamespacedV6,
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone(),
        size_bytes: object.content_size_bytes,
        locations: object.current_locations.clone(),
        created_by_event_id: object.last_modified_by_event_id.clone(),
        created_at: OffsetDateTime::UNIX_EPOCH,
        visible: true,
        retention: BTreeMap::new(),
        module_metadata: BTreeMap::from([("vendor.other".into(), metadata_value(0))]),
        content_durability: None,
    };
    let mut backend = LocalSobjMetadataBackend::new(
        DfsMetadataShardId::new("semantic-root"),
        root.join("metadata"),
    )
    .unwrap();
    backend.set_namespace_prepare_codec(DfsMetadataNamespacePrepareCodec::PackedV6);
    backend
        .put_file_system(&DfsFileSystem {
            metadata_format: DfsRevisionMetadataFormat::NamespacedV6,
            file_system_id: object.file_system_id.clone(),
            display_name: "Semantic extension fixture".into(),
            owner_node_id: owner.clone(),
            subtree_policies: Vec::new(),
            retention_policies: BTreeMap::new(),
            module_metadata: BTreeMap::new(),
        })
        .unwrap();
    backend.put_object_and_version(&object, &version).unwrap();
    let mut dfs = NetworkedDfsService::new(ReceiveOnlyTransport::new(owner), content)
        .with_metadata_backend(backend)
        .unwrap();
    dfs.bootstrap_distributed_sifs().unwrap();
    (dfs, object, version)
}

fn read_version<T: DfsMessageTransport>(
    dfs: &NetworkedDfsService<T>,
    reference: &DfsRevisionReference,
) -> DfsObjectVersionRecord {
    dfs.metadata_backend
        .as_ref()
        .unwrap()
        .get_version(&reference.object_id, &reference.version)
        .unwrap()
        .unwrap()
}

fn contribution(
    reference: DfsRevisionReference,
    _label: &str,
    expected_metadata: Option<DfsModuleMetadataValue>,
    metadata: Option<DfsModuleMetadataValue>,
) -> DfsAtomicContribution {
    let operation_id = sinomo_core::Id::generate();
    let created_at = OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(operation_id.timestamp_millis()) * 1_000_000,
    )
    .unwrap();
    DfsAtomicContribution {
        namespace: "vendor.semantic".into(),
        file_system_id: reference.file_system_id.clone(),
        operation_id: operation_id.to_string(),
        created_at,
        expected_heads: vec![DfsContributionHeadExpectation {
            object_id: reference.object_id.clone(),
            version: reference.version.clone(),
            path: "/services/semantic-source.txt".into(),
        }],
        authority: None,
        updates: vec![DfsRevisionMetadataContribution {
            identity: reference,
            expected_metadata,
            metadata,
        }],
    }
}

#[test]
fn legacy_pin_requests_preserve_canonical_bytes_and_field_order() {
    let reference = DfsRevisionReference {
        file_system_id: sifs_file_system_id(),
        object_id: DfsObjectId::from_legacy_named("semantic-legacy-object").unwrap(),
        version: DfsVersion::from_legacy_named("semantic-legacy-version").unwrap(),
    };
    for operation in [
        DfsFileHistoryPinOperation::Capture {
            pin_id: "checkpoint".into(),
        },
        DfsFileHistoryPinOperation::Pin {
            pin_id: "independent-pin".into(),
        },
        DfsFileHistoryPinOperation::Unpin {
            pin_id: "independent-pin".into(),
        },
    ] {
        let legacy = DfsFileHistoryPinRequest {
            path: "/services/legacy.txt".into(),
            identity: reference.clone(),
            operation,
            operation_id: "stable-legacy-pin".into(),
        };
        let original = serde_json::to_vec(&legacy).unwrap();
        let decoded: DfsContentReferenceRequest = serde_json::from_slice(&original).unwrap();
        assert_eq!(decoded.wire_format, DfsContentReferenceFormat::Legacy);
        assert_eq!(decoded.namespace, "module.dfs.history");
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), original);
        // This comparison protects the identity used by old durable receipts,
        // rather than merely proving that both encodings decode successfully.
        assert_eq!(
            blake3::hash(&original),
            blake3::hash(&serde_json::to_vec(&decoded).unwrap())
        );
    }
}

#[test]
fn new_reference_requests_preserve_explicit_namespace_and_opaque_metadata() {
    let request = DfsContentReferenceRequest {
        wire_format: DfsContentReferenceFormat::NamespacedV6,
        namespace: "vendor.semantic".into(),
        path: "/services/new.txt".into(),
        identity: DfsRevisionReference {
            file_system_id: sifs_file_system_id(),
            object_id: DfsObjectId::from_legacy_named("semantic-new-object").unwrap(),
            version: DfsVersion::from_legacy_named("semantic-new-version").unwrap(),
        },
        operation: DfsContentReferenceOperation::CaptureCurrent {
            reference_id: "derived-index".into(),
            metadata: Some(metadata_value(5)),
        },
        operation_id: "stable-new-pin".into(),
    };
    let bytes = serde_json::to_vec(&request).unwrap();
    let wire: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(wire["namespace"], "vendor.semantic");
    assert_eq!(wire["operation"]["pin_id"], "derived-index");
    assert!(wire["operation"].get("metadata").is_some());
    let decoded: DfsContentReferenceRequest = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded.wire_format, DfsContentReferenceFormat::NamespacedV6);
    assert_eq!(decoded, request);
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
}

#[test]
fn maximum_length_reference_can_be_reconciled_and_released() {
    let root = temp_root("semantic-reference-qualified-key-bound");
    let (mut dfs, object, version) = semantic_fixture(&root);
    let reference_id = "r".repeat(256);
    let namespace = "vendor.semantic";
    let request = DfsContentReferenceRequest {
        wire_format: DfsContentReferenceFormat::NamespacedV6,
        namespace: namespace.into(),
        path: object.path,
        identity: identity(&version),
        operation: DfsContentReferenceOperation::CaptureCurrent {
            reference_id: reference_id.clone(),
            metadata: None,
        },
        operation_id: "semantic-long-reference-capture".into(),
    };
    assert!(content_reference_storage_id(namespace, &reference_id).len() > 256);
    dfs.retain_content_revision(request.clone())
        .expect("a maximum-length logical reference must be publishable");
    assert!(dfs.content_reference_present(&request).unwrap());

    let release = DfsContentReferenceRequest {
        operation: DfsContentReferenceOperation::Release { reference_id },
        operation_id: "semantic-long-reference-release".into(),
        ..request
    };
    dfs.retain_content_revision(release.clone()).unwrap();
    assert!(
        !dfs.content_reference_present(&release).unwrap(),
        "the qualified key must remain reconcilable after release"
    );
}

#[test]
fn atomic_module_metadata_preserves_foreign_namespace_and_immutable_content() {
    let root = temp_root("semantic-contribution-preserves-content");
    let (mut dfs, object, before) = semantic_fixture(&root);
    let reference = identity(&before);
    let committed = DfsCommittedWorkTracker::new();
    dfs.set_committed_work_tracker(committed.clone());
    dfs.apply_atomic_contribution(contribution(
        reference.clone(),
        "metadata-create",
        None,
        Some(metadata_value(1)),
    ))
    .unwrap();
    assert!(
        committed.take().semantic_changes,
        "metadata-only publication must retain its owner wake before a response is built"
    );
    let after = read_version(&dfs, &reference);
    assert_eq!(
        after.module_metadata.get("vendor.other"),
        before.module_metadata.get("vendor.other")
    );
    assert_eq!(
        after.module_metadata.get("vendor.semantic"),
        Some(&metadata_value(1))
    );
    let mut unchanged_revision = after.clone();
    unchanged_revision.module_metadata = before.module_metadata.clone();
    assert_eq!(
        unchanged_revision, before,
        "an extension cannot change immutable revision evidence"
    );
    assert_eq!(
        dfs.metadata_backend
            .as_ref()
            .unwrap()
            .get_object(&object.object_id)
            .unwrap(),
        Some(object.clone())
    );
    assert_eq!(
        dfs.posix_read_file(&object.path).unwrap(),
        b"immutable source bytes"
    );
}

#[test]
fn stale_revision_expectation_rejects_the_complete_transaction_before_mutation() {
    let root = temp_root("semantic-expectation-is-atomic");
    let (mut dfs, object, first) = semantic_fixture(&root);
    let mut second = first.clone();
    second.object_id = DfsObjectId::from_legacy_named("semantic-second-object").unwrap();
    second.version = DfsVersion::from_legacy_named("semantic-second-version").unwrap();
    let backend = dfs.metadata_backend.as_mut().unwrap();
    backend.put_version(&second).unwrap();
    let expected_second = second.clone();
    second
        .module_metadata
        .insert("vendor.other".into(), metadata_value(9));
    backend.put_version(&second).unwrap();
    let mut first_update = first.clone();
    first_update
        .module_metadata
        .insert("vendor.semantic".into(), metadata_value(1));
    let mut second_update = expected_second.clone();
    second_update
        .module_metadata
        .insert("vendor.semantic".into(), metadata_value(1));
    let mut changed_object = object.clone();
    changed_object.mode = 0o600;
    let transaction = DfsNamespaceTransaction {
        content_reference_identity: None,
        contribution_digest: None,
        extension_expectations: vec![
            DfsRevisionMetadataExpectation {
                object_id: first.object_id.clone(),
                version: first.version.clone(),
                expected: Some(Box::new(first.clone())),
            },
            DfsRevisionMetadataExpectation {
                object_id: second.object_id.clone(),
                version: second.version.clone(),
                expected: Some(Box::new(expected_second)),
            },
        ],
        transaction_id: "semantic-stale-atomic-batch".into(),
        file_system_id: first.file_system_id.clone(),
        objects: vec![changed_object],
        versions: vec![first_update, second_update],
        file_version_pins: None,
        advisory_lock_states: Vec::new(),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    let error = backend
        .apply_namespace_transaction(&transaction)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("revision metadata changed before publication"),
        "{error}"
    );
    assert_eq!(
        backend
            .get_version(&first.object_id, &first.version)
            .unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        backend
            .get_version(&second.object_id, &second.version)
            .unwrap(),
        Some(second.clone())
    );
    assert_eq!(
        backend.get_object(&object.object_id).unwrap(),
        Some(object.clone())
    );
    assert!(
        backend
            .get_namespace_transaction(&transaction.transaction_id)
            .unwrap()
            .is_none()
    ); // Fresh observations admit the same kind of batch, proving the earlier
    // rejection came from its stale row rather than an unsupported operation.
    let mut fresh = transaction;
    fresh.transaction_id = "semantic-fresh-atomic-batch".into();
    fresh.extension_expectations[1].expected = Some(Box::new(second.clone()));
    fresh.versions[1]
        .module_metadata
        .insert("vendor.other".into(), metadata_value(9));
    backend.apply_namespace_transaction(&fresh).unwrap();
    for version in [&first, &second] {
        let current = backend
            .get_version(&version.object_id, &version.version)
            .unwrap()
            .unwrap();
        assert_eq!(
            current.module_metadata.get("vendor.semantic"),
            Some(&metadata_value(1))
        );
    }
    assert_eq!(
        backend.get_object(&object.object_id).unwrap().unwrap().mode,
        0o600
    );
}

#[test]
fn admitted_contribution_retry_does_not_revert_intervening_metadata() {
    let root = temp_root("semantic-exact-contribution-retry");
    let (mut dfs, _, before) = semantic_fixture(&root);
    let reference = identity(&before);
    let admitted = contribution(
        reference.clone(),
        "exact-retry",
        None,
        Some(metadata_value(1)),
    );
    dfs.apply_atomic_contribution(admitted.clone()).unwrap();
    dfs.apply_atomic_contribution(contribution(
        reference.clone(),
        "later-edit",
        Some(metadata_value(1)),
        Some(metadata_value(2)),
    ))
    .unwrap();
    let mut intervening = read_version(&dfs, &reference);
    intervening
        .module_metadata
        .insert("vendor.other".into(), metadata_value(8));
    dfs.metadata_backend
        .as_mut()
        .unwrap()
        .put_version(&intervening)
        .unwrap();
    dfs.apply_atomic_contribution(admitted.clone())
        .expect("same admitted input remains successful after later edits");
    assert_eq!(
        read_version(&dfs, &reference),
        intervening,
        "exact replay cannot restore old module metadata"
    );
    let mut different = admitted;
    different.updates[0].metadata = Some(metadata_value(99));
    assert!(
        dfs.apply_atomic_contribution(different).is_err(),
        "same operation id cannot name different inputs"
    );
    assert_eq!(read_version(&dfs, &reference), intervening);
}

#[test]
fn contribution_rechecks_the_original_head_under_the_actual_writer() {
    let root = temp_root("semantic-contribution-head-cas");
    let (mut dfs, object, before) = semantic_fixture(&root);
    let reference = identity(&before);
    let admitted = contribution(reference.clone(), "head-cas", None, Some(metadata_value(1)));
    let mut moved = object.clone();
    moved.version = DfsVersion::from_legacy_named("semantic-head-after-move").unwrap();
    let expected_moved = moved.clone();
    let _hook = crate::service_metadata::install_namespace_prepare_after_expectations_test_hook(
        move |backend| {
            backend.put_object(&moved).unwrap();
        },
    );
    assert!(dfs.apply_atomic_contribution(admitted).is_err());
    assert_eq!(read_version(&dfs, &reference), before);
    assert_eq!(
        dfs.metadata_backend
            .as_ref()
            .unwrap()
            .get_object(&object.object_id)
            .unwrap(),
        Some(expected_moved),
        "an intervening head update must survive failed contribution admission"
    );
}

#[test]
fn contribution_requires_complete_distinct_original_head_expectations() {
    let root = temp_root("semantic-contribution-head-shape");
    let (mut dfs, _, before) = semantic_fixture(&root);
    let reference = identity(&before);
    let valid = contribution(
        reference.clone(),
        "head-shape",
        None,
        Some(metadata_value(1)),
    );
    let mut missing = valid.clone();
    missing.expected_heads.clear();
    assert!(dfs.apply_atomic_contribution(missing).is_err());
    let mut duplicate = valid.clone();
    duplicate
        .expected_heads
        .push(duplicate.expected_heads[0].clone());
    assert!(dfs.apply_atomic_contribution(duplicate).is_err());
    let mut stale = valid;
    stale.expected_heads[0].version = DfsVersion::from_legacy_named("semantic-stale-head").unwrap();
    assert!(dfs.apply_atomic_contribution(stale).is_err());
    assert_eq!(read_version(&dfs, &reference), before);
}

#[test]
fn contribution_retry_context_roundtrips_through_json_and_sobj() {
    let root = temp_root("semantic-contribution-context-wire");
    let (_, object, before) = semantic_fixture(&root);
    let mut request = contribution(
        identity(&before),
        "context-wire",
        Some(metadata_value(1)),
        Some(metadata_value(2)),
    );
    request.authority = Some(DfsContributionAuthorityExpectation {
        shard_id: DfsMetadataShardId::new("services"),
        primary_node_id: object.current_locations[0].clone(),
        owner_epoch: 3,
    });
    let json = serde_json::to_vec(&request).unwrap();
    assert_eq!(
        serde_json::from_slice::<DfsAtomicContribution>(&json).unwrap(),
        request
    );
    let value = sinomo_core::object::serde_ext::to_value(&request).unwrap();
    assert_eq!(
        sinomo_core::object::serde_ext::from_value::<DfsAtomicContribution>(&value).unwrap(),
        request
    );
    let bytes =
        sinomo_core::object::serde_ext::to_bytes(&request, 1, sinomo_core::object::LengthMode::U32)
            .unwrap();
    assert_eq!(
        sinomo_core::object::serde_ext::from_bytes::<DfsAtomicContribution>(&bytes).unwrap(),
        request
    );
}

#[test]
fn contribution_to_an_old_object_does_not_require_ownership_of_its_reused_path() {
    let root = temp_root("semantic-contribution-reused-locator");
    let (mut dfs, object, before) = semantic_fixture(&root);
    let mut deleted = object.clone();
    deleted.kind = DfsObjectKind::Tombstone;
    deleted.version = DfsVersion::from_legacy_named("semantic-deleted-head").unwrap();
    deleted.content_hash = None;
    deleted.content_size_bytes = None;
    let mut replacement = object.clone();
    replacement.object_id = DfsObjectId::from_legacy_named("semantic-reused-new-object").unwrap();
    replacement.version = DfsVersion::from_legacy_named("semantic-reused-new-head").unwrap();
    let mut replacement_revision = before.clone();
    replacement_revision.object_id = replacement.object_id.clone();
    replacement_revision.version = replacement.version.clone();
    {
        let backend = dfs.metadata_backend.as_mut().unwrap();
        backend.put_object(&deleted).unwrap();
        backend
            .put_object_and_version(&replacement, &replacement_revision)
            .unwrap();
    }
    let reference = identity(&before);
    let mut request = contribution(
        reference.clone(),
        "reused-locator",
        None,
        Some(metadata_value(1)),
    );
    request.expected_heads[0].version = deleted.version;
    dfs.apply_atomic_contribution(request).unwrap();
    assert_eq!(
        read_version(&dfs, &reference)
            .module_metadata
            .get("vendor.semantic"),
        Some(&metadata_value(1))
    );
    assert_eq!(
        dfs.metadata_backend
            .as_ref()
            .unwrap()
            .get_object_by_path(&object.file_system_id, &object.path)
            .unwrap(),
        Some(replacement),
        "an old object's metadata contribution cannot mutate the new path occupant"
    );
}

#[test]
fn expired_contribution_cannot_restamp_its_original_timestamp() {
    let root = temp_root("semantic-contribution-expired-retry");
    let (mut dfs, _, before) = semantic_fixture(&root);
    let reference = identity(&before);
    let mut admitted = contribution(
        reference.clone(),
        "expired-original",
        None,
        Some(metadata_value(1)),
    );
    admitted.created_at = OffsetDateTime::UNIX_EPOCH;
    admitted.operation_id = "00000000000000000000000000".into();
    dfs.apply_atomic_contribution(admitted.clone()).unwrap();
    // Return this namespace to its original value, so only durable replay
    // evidence or its expiry floor can prevent the old operation reapplying.
    dfs.apply_atomic_contribution(contribution(
        reference.clone(),
        "clear-after-original",
        Some(metadata_value(1)),
        None,
    ))
    .unwrap();
    assert_eq!(read_version(&dfs, &reference), before);
    let transaction_id = format!(
        "metadata-contribution:vendor.semantic:{}",
        admitted.operation_id
    );
    let first_now = u64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos()).unwrap();
    let first = dfs
        .metadata_backend
        .as_mut()
        .unwrap()
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 4096)
                .with_now_unix_nanos(first_now)
                .with_max_removals(4096),
        )
        .unwrap();
    assert!(first.compacted_namespace_transaction_count >= 1);
    assert!(
        dfs.metadata_backend
            .as_ref()
            .unwrap()
            .get_namespace_transaction(&transaction_id)
            .unwrap()
            .is_none()
    );
    assert!(
        dfs.metadata_backend
            .as_ref()
            .unwrap()
            .get_namespace_transaction_receipt(&transaction_id)
            .unwrap()
            .is_some()
    );
    dfs.apply_atomic_contribution(admitted.clone())
        .expect("compact exact proof remains successful");
    assert_eq!(read_version(&dfs, &reference), before);
    dfs.metadata_backend
        .as_mut()
        .unwrap()
        .gc_metadata_history(
            DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 4096)
                .with_now_unix_nanos(first_now.saturating_add(1))
                .with_max_removals(4096),
        )
        .unwrap();
    assert!(
        dfs.metadata_backend
            .as_ref()
            .unwrap()
            .get_namespace_transaction_receipt(&transaction_id)
            .unwrap()
            .is_none()
    );
    let mut restamped = admitted.clone();
    restamped.created_at = OffsetDateTime::now_utc();
    let error = dfs.apply_atomic_contribution(restamped).unwrap_err();
    assert!(error.to_string().contains("timestamp"), "{error}");
    let error = dfs.apply_atomic_contribution(admitted.clone()).unwrap_err();
    assert!(error.to_string().contains("history expired"), "{error}");
    assert_eq!(read_version(&dfs, &reference), before);
    drop(dfs);
    let mut reopened = NetworkedDfsService::new(
        ReceiveOnlyTransport::new(NodeId::from_legacy_named("semantic-extension-owner").unwrap()),
        LocalContentStore::new(root.join("content")).unwrap(),
    )
    .with_metadata_backend(
        LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("semantic-root"),
            root.join("metadata"),
        )
        .unwrap(),
    )
    .unwrap();
    let error = reopened.apply_atomic_contribution(admitted).unwrap_err();
    assert!(error.to_string().contains("history expired"), "{error}");
    assert_eq!(read_version(&reopened, &reference), before);
}

#[test]
fn missing_object_or_deleted_object_with_a_live_reference_is_not_gc_authority() {
    let root = temp_root("semantic-lifecycle-deletion-proof");
    let (mut dfs, object, mut retained) = semantic_fixture(&root);
    let absent = DfsObjectId::from_legacy_named("semantic-missing-object").unwrap();
    let missing = dfs
        .object_lifecycle(&object.file_system_id, &absent)
        .unwrap();
    assert!(missing.object.is_none());
    assert!(
        !missing.reclaimable,
        "absence cannot authorize source reclamation"
    );
    retained.retention.insert(
        "vendor.semantic".into(),
        DfsRevisionRetentionState {
            order: 1,
            grant: Some(Box::new(DfsRevisionRetentionGrant {
                object: Box::new(object.clone()),
                granted_at_unix_nanos: 1,
                origin: DfsRetentionGrantOrigin::ExplicitOperation,
                minimum: None,
            })),
        },
    );
    let pin = DfsMetadataVersionPinRecord {
        namespace: Some("vendor.semantic".into()),
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: retained.version.clone(),
        pin_id: content_reference_storage_id("vendor.semantic", "independent-consumer"),
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    let mut deleted = object.clone();
    deleted.kind = DfsObjectKind::Tombstone;
    deleted.version = DfsVersion::from_legacy_named("semantic-source-deleted").unwrap();
    deleted.content_hash = None;
    deleted.content_size_bytes = None;
    {
        let backend = dfs.metadata_backend.as_mut().unwrap();
        backend.put_version(&retained).unwrap();
        backend.put_version_pin(&pin).unwrap();
        backend.put_object(&deleted).unwrap();
    }
    let protected = dfs
        .object_lifecycle(&object.file_system_id, &object.object_id)
        .unwrap();
    assert_eq!(
        protected.object.as_ref().map(|object| &object.kind),
        Some(&DfsObjectKind::Tombstone)
    );
    assert!(
        !protected.reclaimable,
        "a deleted source's independent reference still protects associated data"
    );
    let protected_bytes = dfs
        .read_content_revision_range(&object, 0, 64 * 1024)
        .unwrap();
    let NetworkedDfsRangeReadOutcome::Found { data, .. } = protected_bytes else {
        panic!("the independent generic reference must authorize retained bytes");
    };
    assert_eq!(data.bytes, b"immutable source bytes");
    dfs.metadata_backend
        .as_mut()
        .unwrap()
        .remove_version_pin(
            &pin.file_system_id,
            &pin.object_id,
            &pin.version,
            &pin.pin_id,
        )
        .unwrap();
    assert!(
        dfs.object_lifecycle(&object.file_system_id, &object.object_id)
            .unwrap()
            .reclaimable,
        "canonical deletion with every grant released is the positive control"
    );
    assert!(
        dfs.read_content_revision_range(&object, 0, 64 * 1024)
            .is_err(),
        "a leftover content manifest cannot authorize an unprotected revision"
    );
}
