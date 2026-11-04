//! Core extension authority and exact retry through the current kernel.
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
    crate::test_runtime::DfsTestRuntime,
    NetworkedDfsService<crate::DfsNodeTransport>,
    DfsObjectMetadata,
    DfsObjectVersionRecord,
) {
    let (runtime, owner) = crate::test_runtime::DfsTestRuntime::start_single_node_in_data_dir(
        "native-semantic-extension",
        SinomoDataDir::new(root),
    )
    .unwrap();
    let mut client = runtime.client(&owner).unwrap();
    let stat = client
        .posix_write_file(
            "/services/semantic-source.txt",
            b"immutable source bytes",
            DfsWriteOptions::replicated_eventual(vec![owner]),
        )
        .unwrap();
    drop(client);
    // Contributions are Main-side store work: the client delegating to its
    // Main keeps no store, so they run on the shard's store-backed opener.
    let mut dfs = runtime
        .metadata_primary_service("/services/semantic-source.txt")
        .unwrap();
    let object = dfs
        .metadata_backend
        .as_ref()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    let reference = DfsRevisionReference {
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
    };
    let mut initialize = contribution(
        &mut dfs,
        reference.clone(),
        "initialize-unrelated-metadata",
        None,
        Some(metadata_value(0)),
    );
    initialize.namespace = "vendor.other".into();
    dfs.apply_atomic_contribution(initialize).unwrap();
    let version = read_version(&dfs, &reference);
    assert!(
        version.content_durability.is_some(),
        "fixture must retain the genuine local write's durability grant"
    );
    (runtime, dfs, object, version)
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

fn publish_semantic_effects<T: DfsMessageTransport>(
    dfs: &mut NetworkedDfsService<T>,
    fs: &DfsFileSystemId,
    effects: Vec<DfsPreparedMetadataEffect>,
) {
    let config = dfs
        .namespace_shards_for_exact_paths(fs, ["/services/semantic-source.txt"])
        .unwrap()
        .remove(0);
    let backend = dfs.metadata_backend.as_ref().unwrap();
    let mut objects = BTreeMap::new();
    let mut paths = BTreeMap::new();
    let mut versions = Vec::new();
    for effect in &effects {
        for object in effect.affected_object_ids() {
            let current = backend.get_object(&object).unwrap();
            objects.insert(
                object.clone(),
                DfsNamespaceObjectVersionExpectation {
                    object_id: object,
                    expected_version: current.map(|object| object.version),
                },
            );
        }
        if let DfsPreparedMetadataEffect::PutObject { object } = effect {
            let current = backend.get_object_by_path(fs, &object.path).unwrap();
            paths.insert(
                object.path.clone(),
                DfsNamespacePathOccupantExpectation {
                    path: object.path.clone(),
                    expected_object_id: current.map(|object| object.object_id),
                },
            );
        }
        if let DfsPreparedMetadataEffect::PutVersion { version } = effect {
            versions.push(DfsRevisionMetadataExpectation {
                object_id: version.object_id.clone(),
                version: version.version.clone(),
                expected: backend
                    .get_version(&version.object_id, &version.version)
                    .unwrap()
                    .map(Box::new),
            });
        }
    }
    let mut payload = DfsPreparedOperationPayload::for_metadata_effects(
        DfsPreparedOperationScope {
            file_system_id: fs.clone(),
            shard_id: DfsMetadataShardId::new(config.shard_id.clone()),
            owner_epoch: config.epoch,
            operation_id: format!("semantic-fixture:{}", sinomo_core::Id::generate()),
        },
        objects.into_values().collect(),
        paths.into_values().collect(),
        effects,
        OffsetDateTime::now_utc(),
    )
    .unwrap();
    payload.transaction.extension_expectations = versions;
    crate::test_support::publish_current_payload(dfs, payload).unwrap();
}

fn publish_semantic_transaction<T: DfsMessageTransport>(
    dfs: &mut NetworkedDfsService<T>,
    transaction: DfsNamespaceTransaction,
) -> Result<(), DfsError> {
    let config = dfs
        .namespace_shards_for_exact_paths(
            &transaction.file_system_id,
            ["/services/semantic-source.txt"],
        )?
        .remove(0);
    let backend = dfs.metadata_backend.as_ref().unwrap();
    let expected_objects = transaction
        .objects
        .iter()
        .map(|object| object.object_id.clone())
        .chain(
            transaction
                .versions
                .iter()
                .map(|version| version.object_id.clone()),
        )
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|object_id| {
            Ok(DfsNamespaceObjectVersionExpectation {
                expected_version: backend
                    .get_object(&object_id)?
                    .map(|current| current.version),
                object_id,
            })
        })
        .collect::<Result<Vec<_>, MetadataBackendError>>()?;
    let expected_paths = transaction
        .objects
        .iter()
        .map(|object| {
            Ok(DfsNamespacePathOccupantExpectation {
                path: object.path.clone(),
                expected_object_id: backend
                    .get_object_by_path(&object.file_system_id, &object.path)?
                    .map(|current| current.object_id),
            })
        })
        .collect::<Result<Vec<_>, MetadataBackendError>>()?;
    let payload = DfsPreparedOperationPayload::for_namespace_transaction(
        DfsPreparedOperationScope {
            file_system_id: transaction.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(config.shard_id.clone()),
            owner_epoch: config.epoch,
            operation_id: format!("semantic-transaction:{}", transaction.transaction_id),
        },
        expected_objects,
        expected_paths,
        transaction,
        Vec::new(),
        None,
        None,
        Vec::new(),
        None,
        None,
    )?;
    crate::test_support::publish_current_payload(dfs, payload)
}

fn contribution<T: DfsMessageTransport>(
    dfs: &mut NetworkedDfsService<T>,
    reference: DfsRevisionReference,
    _label: &str,
    expected_metadata: Option<DfsModuleMetadataValue>,
    metadata: Option<DfsModuleMetadataValue>,
) -> DfsAtomicContribution {
    let head = dfs
        .metadata_backend
        .as_ref()
        .unwrap()
        .get_object(&reference.object_id)
        .unwrap()
        .expect("actual original current head");
    let config = dfs
        .namespace_shards_for_exact_paths(&reference.file_system_id, [head.path.as_str()])
        .unwrap()
        .remove(0);
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
            object_id: head.object_id,
            version: head.version,
            path: head.path,
        }],
        authority: Some(DfsContributionAuthorityExpectation {
            shard_id: DfsMetadataShardId::new(config.shard_id),
            primary_node_id: config.primary_node_id,
            owner_epoch: config.epoch,
        }),
        updates: vec![DfsRevisionMetadataContribution {
            identity: reference,
            expected_metadata,
            metadata,
        }],
    }
}

#[test]
fn new_reference_requests_preserve_explicit_namespace_and_opaque_metadata() {
    let request = DfsContentReferenceRequest {
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
    assert_eq!(wire["operation"]["reference_id"], "derived-index");
    assert!(wire["operation"].get("metadata").is_some());
    let decoded: DfsContentReferenceRequest = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(decoded, request);
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
}

#[test]
fn maximum_length_reference_can_be_reconciled_and_released() {
    let root = temp_root("semantic-reference-qualified-key-bound");
    let (_runtime, mut dfs, object, version) = semantic_fixture(&root);
    let reference_id = "r".repeat(256);
    let namespace = "vendor.semantic";
    let request = DfsContentReferenceRequest {
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

    drop(dfs);
    _runtime.close().unwrap();
}

#[test]
fn atomic_module_metadata_preserves_foreign_namespace_and_immutable_content() {
    let root = temp_root("semantic-contribution-preserves-content");
    let (_runtime, mut dfs, object, before) = semantic_fixture(&root);
    let reference = identity(&before);
    let committed = DfsCommittedWorkTracker::new();
    dfs.set_committed_work_tracker(committed.clone());
    let fresh = contribution(
        &mut dfs,
        reference.clone(),
        "metadata-create",
        None,
        Some(metadata_value(1)),
    );
    dfs.apply_atomic_contribution(fresh).unwrap();
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

    drop(dfs);
    _runtime.close().unwrap();
}

#[test]
fn stale_revision_expectation_rejects_the_complete_transaction_before_mutation() {
    let root = temp_root("semantic-expectation-is-atomic");
    let (_runtime, mut dfs, object, first) = semantic_fixture(&root);
    let second_stat = dfs
        .posix_write_file(
            "/services/semantic-second.txt",
            b"second immutable source bytes",
            DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()]),
        )
        .unwrap();
    let second_reference = DfsRevisionReference {
        file_system_id: second_stat.file_system_id,
        object_id: second_stat.object_id,
        version: second_stat.version,
    };
    let mut initialize = contribution(
        &mut dfs,
        second_reference.clone(),
        "second-original-metadata",
        None,
        Some(metadata_value(0)),
    );
    initialize.namespace = "vendor.other".into();
    dfs.apply_atomic_contribution(initialize).unwrap();
    let mut second = read_version(&dfs, &second_reference);
    let expected_second = second.clone();
    let mut changed = contribution(
        &mut dfs,
        second_reference.clone(),
        "second-intervening-metadata",
        Some(metadata_value(0)),
        Some(metadata_value(9)),
    );
    changed.namespace = "vendor.other".into();
    dfs.apply_atomic_contribution(changed).unwrap();
    second = read_version(&dfs, &second_reference);
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
    let error = publish_semantic_transaction(&mut dfs, transaction.clone()).unwrap_err();
    let backend = dfs.metadata_backend.as_ref().unwrap();
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
            .get_namespace_transaction_receipt(&transaction.transaction_id)
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
    publish_semantic_transaction(&mut dfs, fresh).unwrap();
    let backend = dfs.metadata_backend.as_ref().unwrap();
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

    drop(dfs);
    _runtime.close().unwrap();
}

#[test]
fn admitted_contribution_retry_does_not_revert_intervening_metadata() {
    let root = temp_root("semantic-exact-contribution-retry");
    let (_runtime, mut dfs, object, before) = semantic_fixture(&root);
    let reference = identity(&before);
    let admitted = contribution(
        &mut dfs,
        reference.clone(),
        "exact-retry",
        None,
        Some(metadata_value(1)),
    );
    dfs.apply_atomic_contribution(admitted.clone()).unwrap();
    let fresh = contribution(
        &mut dfs,
        reference.clone(),
        "later-edit",
        Some(metadata_value(1)),
        Some(metadata_value(2)),
    );
    dfs.apply_atomic_contribution(fresh).unwrap();
    let mut foreign = contribution(
        &mut dfs,
        reference.clone(),
        "intervening-foreign-metadata",
        Some(metadata_value(0)),
        Some(metadata_value(8)),
    );
    foreign.namespace = "vendor.other".into();
    dfs.apply_atomic_contribution(foreign).unwrap();
    let intervening = read_version(&dfs, &reference);
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

    drop(dfs);
    _runtime.close().unwrap();
}

#[test]
fn contribution_rechecks_the_original_head_under_the_actual_writer() {
    let root = temp_root("semantic-contribution-head-cas");
    let (_runtime, mut dfs, object, before) = semantic_fixture(&root);
    let reference = identity(&before);
    let admitted = contribution(
        &mut dfs,
        reference.clone(),
        "head-cas",
        None,
        Some(metadata_value(1)),
    );
    let expected_moved = Arc::new(Mutex::new(None));
    let observed = Arc::clone(&expected_moved);
    let path = object.path.clone();
    let owner = object.owner_node_id.clone();
    let mut concurrent = _runtime.client(&owner).unwrap();
    // The delegating writer keeps no store; observe the owner's durable store.
    let store_root = default_dfs_metadata_shards_root(_runtime.data_dir(&owner));
    let shards = _runtime.metadata_shards().unwrap();
    let _hook = crate::service_metadata::install_namespace_prepare_after_expectations_test_hook(
        move |_backend| {
            let stat = concurrent
                .posix_write_file(
                    &path,
                    b"intervening native write",
                    DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
                )
                .unwrap();
            *observed.lock().unwrap() =
                crate::local_state::open_local_sharded_metadata_backend_for_inspection_at_node(
                    store_root.clone(),
                    shards.clone(),
                    &owner,
                )
                .unwrap()
                .get_object(&stat.object_id)
                .unwrap();
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
        expected_moved.lock().unwrap().clone(),
        "an intervening head update must survive failed contribution admission"
    );

    drop(_hook);
    drop(dfs);
    _runtime.close().unwrap();
}

#[test]
fn native_publication_takes_the_current_configuration_snapshot() {
    native_publication_configuration_cut(false);
}

#[test]
fn native_publication_keeps_semantic_revalidation_after_admission() {
    native_publication_configuration_cut(true);
}

fn native_publication_configuration_cut(after_initial_cut: bool) {
    let root = temp_root(if after_initial_cut {
        "native-publication-configuration-after-cut"
    } else {
        "native-publication-configuration-before-cut"
    });
    let (runtime, mut dfs, _, before) = semantic_fixture(&root);
    let reference = identity(&before);
    let request = contribution(
        &mut dfs,
        reference.clone(),
        "configuration-cut",
        None,
        Some(metadata_value(2)),
    );
    let owner = dfs.node_id().clone();
    let authority = crate::runtime_host::daemon_dfs_metadata_authority_generation(
        &sinomo_core::network::ClusterPaths::new(runtime.data_dir(&owner)),
        &runtime.runtime_handle(),
    )
    .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let changed = Arc::new(Mutex::new(None));
    let observed = Arc::clone(&changed);
    let update = Arc::clone(&authority);
    let update_control = control.clone();
    let fs = reference.file_system_id.clone();
    let configuration_update = move || {
        let generation = update.projection_generation();
        let storage = update.storage_generation();
        // Real configuration publication through this fixture's mounted Core.
        // The hook supplies neither a snapshot value nor author proof.
        update_control.with_client(|kv| {
            let mut store = DfsControlPlaneStore::new(kv.as_mut());
            let mut current = store.get_file_system_config(&fs).unwrap().unwrap();
            current.display_name.push_str("-native-current-cut");
            store.put_file_system_config(&current).unwrap();
        });
        crate::test_runtime::executor().block_on(async {
            tokio::time::timeout(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                update.ensure_write_configuration_ready_async(),
            )
            .await
            .unwrap()
            .unwrap();
        });
        let refreshed = update.projection_generation();
        assert!(
            refreshed > generation,
            "actual committed configuration must advance its projection"
        );
        assert_eq!(
            update.storage_generation(),
            storage,
            "display name preserves physical storage custody"
        );
        *observed.lock().unwrap() = Some((generation, refreshed, storage));
    };
    if after_initial_cut {
        crate::publication_guard::EFFECT_ADMISSION_AFTER_CUT.with(|slot| {
            assert!(
                slot.borrow_mut()
                    .replace(Box::new(configuration_update))
                    .is_none()
            )
        });
    } else {
        configuration_update();
    }
    let result = dfs.apply_atomic_contribution(request);
    let current = read_version(&dfs, &reference);
    let observed = changed.lock().unwrap().clone();
    std::fs::write(
        runtime.data_dir(&owner).root().join("boundary-diagnostic-publication-current-cut.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "after_initial_cut": after_initial_cut, "actual_generations": observed,
            "result": format!("{:?}", result.as_ref().map(|_| ())),
            "before": before, "after": current,
            "qualification": "Real mounted Core configuration update before or after the effect operation's publication snapshot; no synthetic projection, epoch, acknowledgement or authority."
        })).unwrap(),
    ).unwrap();
    crate::publication_guard::EFFECT_ADMISSION_AFTER_CUT.with(|slot| slot.borrow_mut().take());
    drop(authority);
    drop(control);
    drop(dfs);
    runtime.close().unwrap();
    assert!(
        observed.is_some(),
        "the actual snapshot boundary was reached"
    );
    if after_initial_cut {
        assert!(matches!(result, Err(DfsError::Metadata { ref message })
            if message == "publication configuration advanced while effect admission queued"));
        assert_eq!(
            current, before,
            "late configuration change cannot publish the contribution"
        );
    } else {
        result
            .expect("a fresh initial cut must not reject a completed native configuration refresh");
        assert_eq!(
            current.module_metadata.get("vendor.semantic"),
            Some(&metadata_value(2))
        );
        assert_eq!(current.object_id, before.object_id);
        assert_eq!(current.version, before.version);
        assert_eq!(current.content_hash, before.content_hash);
    }
}

#[test]
fn contribution_requires_complete_distinct_original_head_expectations() {
    let root = temp_root("semantic-contribution-head-shape");
    let (_runtime, mut dfs, _, before) = semantic_fixture(&root);
    let reference = identity(&before);
    let valid = contribution(
        &mut dfs,
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

    drop(dfs);
    _runtime.close().unwrap();
}

#[test]
fn contribution_retry_context_roundtrips_through_json_and_sobj() {
    let root = temp_root("semantic-contribution-context-wire");
    let (_runtime, mut dfs, _object, before) = semantic_fixture(&root);
    let request = contribution(
        &mut dfs,
        identity(&before),
        "context-wire",
        Some(metadata_value(1)),
        Some(metadata_value(2)),
    );
    assert!(request.authority.is_some());
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

    drop(dfs);
    _runtime.close().unwrap();
}

#[test]
fn contribution_to_an_old_object_does_not_require_ownership_of_its_reused_path() {
    let root = temp_root("semantic-contribution-reused-locator");
    let (_runtime, mut dfs, object, before) = semantic_fixture(&root);
    dfs.posix_unlink(&object.path).unwrap();
    let deleted = dfs
        .metadata_backend
        .as_ref()
        .unwrap()
        .get_object(&object.object_id)
        .unwrap()
        .expect("actual canonical tombstone");
    assert_eq!(deleted.kind, DfsObjectKind::Tombstone);
    let recreated = dfs
        .posix_write_file(
            &object.path,
            b"immutable source bytes",
            DfsWriteOptions::replicated_eventual(vec![object.owner_node_id.clone()]),
        )
        .unwrap();
    let replacement = dfs
        .metadata_backend
        .as_ref()
        .unwrap()
        .get_object(&recreated.object_id)
        .unwrap()
        .unwrap();
    assert_ne!(
        replacement.object_id, object.object_id,
        "the replacement is a real new path occupant"
    );
    let reference = identity(&before);
    let request = contribution(
        &mut dfs,
        reference.clone(),
        "reused-locator",
        None,
        Some(metadata_value(1)),
    );
    assert_eq!(request.expected_heads[0].version, deleted.version);
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

    drop(dfs);
    _runtime.close().unwrap();
}

#[test]
fn expired_contribution_cannot_restamp_its_original_timestamp() {
    let root = temp_root("semantic-contribution-expired-retry");
    let (mut _runtime, mut dfs, _, before) = semantic_fixture(&root);
    let reference = identity(&before);
    let mut admitted = contribution(
        &mut dfs,
        reference.clone(),
        "expired-original",
        None,
        Some(metadata_value(1)),
    );
    admitted.created_at = OffsetDateTime::UNIX_EPOCH;
    admitted.operation_id = "00000000000000000000000000".into();
    dfs.apply_atomic_contribution(admitted.clone()).unwrap();
    let transaction_id = format!(
        "metadata-contribution:vendor.semantic:{}",
        admitted.operation_id
    );
    let record = dfs
        .metadata_backend
        .as_ref()
        .unwrap()
        .prepared_operation_record(&format!("namespace:{transaction_id}"))
        .unwrap()
        .expect("actual committed contribution body");
    assert_eq!(record.status, DfsPreparedOperationStatus::Committed);
    let digest = record.digest.clone();
    let original_commit = record.committed_entry.clone();
    // Return this namespace to its original value, so only durable replay
    // evidence or its expiry floor can prevent the old operation reapplying.
    let fresh = contribution(
        &mut dfs,
        reference.clone(),
        "clear-after-original",
        Some(metadata_value(1)),
        None,
    );
    dfs.apply_atomic_contribution(fresh).unwrap();
    assert_eq!(read_version(&dfs, &reference), before);
    publish_semantic_effects(
        &mut dfs,
        &before.file_system_id,
        vec![DfsPreparedMetadataEffect::RemovePreparedOperation {
            digest: digest.clone(),
        }],
    );
    assert!(
        dfs.metadata_backend
            .as_ref()
            .unwrap()
            .prepared_operation_record(&digest.operation_id)
            .unwrap()
            .is_none()
    );
    let snapshot = dfs
        .metadata_backend
        .as_ref()
        .unwrap()
        .export_shard_table_snapshot(&digest.shard_id)
        .unwrap()
        .unwrap();
    let witness = snapshot
        .tables
        .prepared_operation_evictions
        .iter()
        .find(|witness| witness.digest.operation_id == digest.operation_id)
        .expect("exact current terminal witness");
    assert_eq!(witness.digest, digest);
    assert_eq!(witness.committed_entry, original_commit);
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
    let now = u64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos()).unwrap();
    dfs.gc_metadata_history(
        DfsMetadataHistoryGcOptions::delete_eligible(Duration::ZERO, 4096)
            .with_now_unix_nanos(now)
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
    assert!(error.to_string().contains("terminally evicted"), "{error}");
    assert_eq!(read_version(&dfs, &reference), before);
    let node = dfs.node_id().clone();
    drop(dfs);
    _runtime.stop_node(&node).unwrap();
    _runtime.reopen_node(node.clone()).unwrap();
    let mut reopened = _runtime
        .metadata_primary_service("/services/semantic-source.txt")
        .unwrap();
    let error = reopened.apply_atomic_contribution(admitted).unwrap_err();
    assert!(error.to_string().contains("terminally evicted"), "{error}");
    assert_eq!(read_version(&reopened, &reference), before);

    drop(reopened);
    _runtime.close().unwrap();
}

#[test]
fn missing_object_or_deleted_object_with_a_live_reference_is_not_gc_authority() {
    let root = temp_root("semantic-lifecycle-deletion-proof");
    let (_runtime, mut dfs, object, retained) = semantic_fixture(&root);
    let absent = DfsObjectId::from_legacy_named("semantic-missing-object").unwrap();
    let missing = dfs
        .object_lifecycle(&object.file_system_id, &absent)
        .unwrap();
    assert!(missing.object.is_none());
    assert!(
        !missing.reclaimable,
        "absence cannot authorize source reclamation"
    );
    let reference = DfsContentReferenceRequest {
        namespace: "vendor.semantic".into(),
        path: object.path.clone(),
        identity: identity(&retained),
        operation: DfsContentReferenceOperation::CaptureCurrent {
            reference_id: "independent-consumer".into(),
            metadata: None,
        },
        operation_id: sinomo_core::Id::generate().to_string(),
    };
    dfs.retain_content_revision(reference.clone()).unwrap();
    dfs.posix_unlink(&object.path).unwrap();
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
    dfs.retain_content_revision(DfsContentReferenceRequest {
        operation: DfsContentReferenceOperation::Release {
            reference_id: "independent-consumer".into(),
        },
        operation_id: sinomo_core::Id::generate().to_string(),
        ..reference
    })
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

    drop(dfs);
    _runtime.close().unwrap();
}
