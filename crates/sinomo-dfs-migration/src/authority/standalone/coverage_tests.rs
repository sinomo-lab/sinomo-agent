use super::*;
use sinomo_dfs::test_support::{DfsTestRuntime, StandaloneDataOwnerFixture};
use sinomo_dfs::{DfsObjectKind, DfsObjectMetadata};

fn source_rows(
    node: &sinomo_core::node::NodeId,
) -> (sinomo_dfs::DfsFileSystem, Vec<DfsObjectMetadata>) {
    let fs = sinomo_dfs::DfsFileSystem::sifs(node.clone());
    let mut rows = Vec::new();
    for (name, path, kind) in [
        ("root", "/", DfsObjectKind::Directory),
        ("child", "/child", DfsObjectKind::Directory),
        ("file", "/file", DfsObjectKind::File),
        ("alias", "/alias", DfsObjectKind::Hardlink),
    ] {
        rows.push(DfsObjectMetadata {
            object_id: sinomo_dfs::DfsObjectId::from_legacy_named(name).unwrap(),
            file_system_id: fs.file_system_id.clone(),
            path: path.into(),
            kind,
            namespace_link_count: None,
            tombstone_provenance: None,
            owner_node_id: node.clone(),
            current_locations: Vec::new(),
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: if name == "alias" {
                Some(sinomo_dfs::DfsObjectId::from_legacy_named("file").unwrap())
            } else {
                None
            },
            mode: 0o755,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: sinomo_dfs::DfsVersion::from_legacy_named(&format!("version-{name}")).unwrap(),
            policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        });
    }
    (fs, rows)
}
fn write_source(root: &Path, node: &sinomo_core::node::NodeId) -> Vec<u8> {
    let (fs, objects) = source_rows(node);
    let original = sinomo_core::object::serde_ext::to_bytes(
        &serde_json::json!({
            "format_version":2, "tables":{"file_systems":[fs],"objects":objects}
        }),
        1,
        sinomo_core::object::LengthMode::U32,
    )
    .unwrap();
    std::fs::create_dir_all(root.join("records/tables")).unwrap();
    std::fs::write(root.join("records/tables/snapshot.sobj"), &original).unwrap();
    original
}
fn local_context(
    name: &str,
) -> (
    DfsTestRuntime,
    sinomo_module::ComponentContext,
    PathBuf,
    Vec<u8>,
) {
    let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
    // This initializes only the actual persisted local principal and executor.
    // No native node/component/cluster authority is started or registered.
    let runtime = DfsTestRuntime::new(name, node.clone()).unwrap();
    let context = sinomo_module::ComponentContext::new(Arc::new(runtime.data_dir(&node).clone()))
        .with_host_role(sinomo_module::ComponentHostRole::Embedded)
        .with_runtime(runtime.runtime_handle());
    assert!(context.cluster_access().is_none());
    assert!(
        sinomo_dfs::runtime_host::registered_component_cluster(&context)
            .unwrap()
            .is_none()
    );
    let root = context.data_dir().dfs_dir().join("metadata/root");
    let original = write_source(&root, &node);
    (runtime, context, root, original)
}

#[test]
fn standalone_coverage_refuses_omitted_changed_unknown_and_ambiguous_roots() {
    let (runtime, context, root, original) = local_context("standalone-coverage-refusal");
    let owner = StandaloneDataOwnerFixture::acquire(&context).unwrap();
    let factory = ExplicitStandaloneSourceFactory::from_context(&context).unwrap();
    let authority = Arc::new(StandaloneImportAuthority { factory });
    let converter = crate::metadata::MetadataConverter::with_authority(authority.clone());
    let shard = sinomo_dfs::DfsMetadataShardId::new("root");
    let capture = (0..16)
        .find_map(|_| {
            converter
                .capture_metadata_unit(&root, &root, &shard, 256)
                .unwrap()
        })
        .expect("bounded original source capture");
    authority
        .factory
        .require_complete_coverage(&capture.source)
        .unwrap();
    let metadata = root.parent().unwrap();
    let extra = metadata.join("omitted");
    std::fs::create_dir(&extra).unwrap();
    assert!(
        authority
            .factory
            .require_complete_coverage(&capture.source)
            .is_err()
    );
    std::fs::remove_dir(&extra).unwrap();
    write_source(&extra, &authority.factory.node_id);
    assert!(
        authority
            .factory
            .require_complete_coverage(&capture.source)
            .is_err(),
        "identical rows in another physical root do not prove unique ownership"
    );
    std::fs::write(
        extra.join("records/tables/snapshot.sobj"),
        b"unknown or unreadable source",
    )
    .unwrap();
    assert!(
        authority
            .factory
            .require_complete_coverage(&capture.source)
            .is_err()
    );
    std::fs::remove_dir_all(&extra).unwrap();
    let routing = metadata.join("shard-index/unknown-source");
    std::fs::create_dir_all(&routing).unwrap();
    assert!(
        authority
            .factory
            .require_complete_coverage(&capture.source)
            .is_err()
    );
    std::fs::remove_dir_all(metadata.join("shard-index")).unwrap();
    let foreign_job = capture
        .source
        .evidence_root
        .parent()
        .unwrap()
        .join("unrecorded-source");
    std::fs::create_dir(&foreign_job).unwrap();
    assert!(
        authority
            .factory
            .require_complete_coverage(&capture.source)
            .is_err()
    );
    std::fs::remove_dir(&foreign_job).unwrap();
    let nested = capture
        .source
        .evidence_root
        .join("source-evidence/unknown-subtree");
    std::fs::create_dir_all(&nested).unwrap();
    assert!(
        authority
            .factory
            .require_complete_coverage(&capture.source)
            .is_err()
    );
    std::fs::remove_dir(&nested).unwrap();
    std::fs::write(
        root.join("records/tables/snapshot.sobj"),
        b"changed original bytes",
    )
    .unwrap();
    assert!(
        authority
            .factory
            .require_complete_coverage(&capture.source)
            .is_err()
    );
    std::fs::write(root.join("records/tables/snapshot.sobj"), &original).unwrap();
    // Restoring bytes does not authorize replacing an immutable source inode;
    // this test writes through the same retained file and revalidates it.
    assert_eq!(
        std::fs::read(root.join("records/tables/snapshot.sobj")).unwrap(),
        original
    );
    drop(converter);
    drop(authority);
    drop(owner);
    runtime.close().unwrap();
}

#[cfg(unix)]
#[test]
fn standalone_physical_aliases_deduplicate_but_changed_alias_set_refuses() {
    let (runtime, context, root, _) = local_context("standalone-coverage-alias");
    std::os::unix::fs::symlink(&root, root.parent().unwrap().join("same-physical-root")).unwrap();
    let owner = StandaloneDataOwnerFixture::acquire(&context).unwrap();
    let authority = Arc::new(StandaloneImportAuthority {
        factory: ExplicitStandaloneSourceFactory::from_context(&context).unwrap(),
    });
    let converter = crate::metadata::MetadataConverter::with_authority(authority.clone());
    let shard = sinomo_dfs::DfsMetadataShardId::new("root");
    let capture = (0..16)
        .find_map(|_| {
            converter
                .capture_metadata_unit(&root, &root, &shard, 256)
                .unwrap()
        })
        .unwrap();
    authority
        .factory
        .require_complete_coverage(&capture.source)
        .unwrap();
    let proof: Coverage = read_checkpoint(
        &capture
            .source
            .evidence_root
            .join("standalone-coverage.json"),
    )
    .unwrap();
    assert_eq!(proof.aliases.len(), 2);
    std::fs::remove_file(root.parent().unwrap().join("same-physical-root")).unwrap();
    assert!(
        authority
            .factory
            .require_complete_coverage(&capture.source)
            .is_err()
    );
    drop(converter);
    drop(authority);
    drop(owner);
    runtime.close().unwrap();
}

#[test]
fn standalone_managed_owner_counts_survive_current_codec_and_restart() {
    let (runtime, context, root, original) = local_context("standalone-managed-current-generation");
    let binding = Arc::new(Mutex::new(None));
    let captured = Arc::clone(&binding);
    let factory = Arc::new(
        sinomo_module::StaticComponentFactory::new(
            sinomo_module::ComponentDescriptor::new("explicit-standalone-source-test", "1"),
            move |context, scope| {
                let owner = StandaloneDataOwnerFixture::acquire(context).map_err(|e| {
                    sinomo_module::ComponentError::component("explicit-standalone-source-test", e)
                })?;
                scope.defer(move || drop(owner));
                let lifecycle = scope.shutdown_receiver();
                *captured.lock().unwrap() =
                    Some(crate::authority::standalone_storage_preparation_provider(
                        context, scope, lifecycle,
                    )?);
                Ok(())
            },
        )
        .unwrap(),
    );
    let mut components = sinomo_module::ComponentReconciler::new(
        context.clone(),
        sinomo_core::service::ServiceRegistry::default(),
    );
    components.reconcile(&[factory]).unwrap();
    let actual = binding.lock().unwrap().take().unwrap();
    let paths = sinomo_dfs::runtime_host::DfsRuntimePaths::for_host_role(
        context.data_dir(), context.host_role(),
    );
    let owner = crate::authority::OWNERS.get().unwrap().lock().unwrap()
        .get(&paths.root).and_then(std::sync::Weak::upgrade).unwrap();
    let mut changed = actual.readiness;
    let scope = DfsStoragePreparationScope::Metadata(sinomo_dfs::DfsMetadataShardId::new("root"));
    block_native(&runtime.runtime_handle(), async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            match (actual.provider)(&root, &scope, 256)? {
                sinomo_dfs::DfsStoragePreparation::Ready
                    if owner.retained_metadata_artifacts_observed(&root)? => break,
                sinomo_dfs::DfsStoragePreparation::Ready => {}
                sinomo_dfs::DfsStoragePreparation::Pending { .. } => {}
            }
            tokio::time::timeout_at(deadline, changed.changed())
                .await
                .map_err(|_| error("actual standalone owner did not complete"))?
                .map_err(error)?;
        }
        Ok::<_, MetadataBackendError>(())
    })
    .unwrap();
    let header = sinomo_dfs::inspect_metadata_storage_generation(&root).unwrap();
    assert!(matches!(
        header.identity.origin,
        sinomo_dfs::DfsMetadataGenerationOrigin::StandaloneOwned { .. }
    ));
    assert!(matches!(
        header.state,
        sinomo_dfs::DfsMetadataGenerationState::Active { .. }
    ));
    let read_counts = || {
        let snapshot = sinomo_dfs::read_metadata_candidate(&root).unwrap();
        assert!(snapshot.tables.prepared_operations_v7.is_empty());
        let counts = snapshot
            .tables
            .objects
            .iter()
            .map(|row| (row.path.as_str(), row.namespace_link_count))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(counts["/"], Some(3));
        assert_eq!(counts["/child"], Some(2));
        assert_eq!(counts["/file"], Some(2));
        assert_eq!(counts["/alias"], None);
    };
    read_counts();
    drop(owner);
    block_native(
        &runtime.runtime_handle(),
        components.shutdown_and_wait(std::time::Duration::from_secs(5)),
    )
    .unwrap();
    drop(actual.provider);
    drop(changed);
    drop(components);
    drop(binding);
    assert_eq!(
        sinomo_dfs::inspect_metadata_storage_generation(&root).unwrap(),
        header
    );
    let backend = sinomo_dfs::LocalSobjMetadataBackend::new(
        sinomo_dfs::DfsMetadataShardId::new("root"),
        &root,
    )
    .unwrap();
    drop(backend);
    read_counts();
    let lineage = crate::metadata::associated_completed_metadata_lineage(&root)
        .unwrap()
        .unwrap();
    assert_eq!(
        crate::metadata::associate_completed_metadata_artifact(&root, &lineage.retained_root)
            .unwrap(),
        Some(lineage.source_cut.clone())
    );
    assert!(
        crate::metadata::associate_completed_metadata_artifact(
            &root,
            &root.parent().unwrap().join("unknown-artifact")
        )
        .unwrap()
        .is_none()
    );
    let marker_bytes = std::fs::read(root.join("metadata-import-v7.json")).unwrap();
    let mut incomplete: serde_json::Value = serde_json::from_slice(&marker_bytes).unwrap();
    incomplete["installation_complete"] = serde_json::json!(false);
    std::fs::write(
        root.join("metadata-import-v7.json"),
        serde_json::to_vec(&incomplete).unwrap(),
    )
    .unwrap();
    assert!(
        crate::metadata::associated_completed_metadata_lineage(&root)
            .unwrap()
            .is_none()
    );
    std::fs::write(root.join("metadata-import-v7.json"), marker_bytes).unwrap();
    let marker: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("metadata-import-v7.json")).unwrap())
            .unwrap();
    assert_eq!(
        std::fs::read(
            PathBuf::from(marker["retained_source_root"].as_str().unwrap())
                .join("records/tables/snapshot.sobj")
        )
        .unwrap(),
        original
    );
    runtime.close().unwrap();
}

#[test]
fn standalone_owned_candidate_resumes_before_cut_and_new_root_refuses_install() {
    let (runtime, context, root, original) = local_context("standalone-precut-owned-candidate");
    let owner = StandaloneDataOwnerFixture::acquire(&context).unwrap();
    let authority = Arc::new(StandaloneImportAuthority {
        factory: ExplicitStandaloneSourceFactory::from_context(&context).unwrap(),
    });
    let converter = crate::metadata::MetadataConverter::with_authority(authority.clone());
    let shard = sinomo_dfs::DfsMetadataShardId::new("root");
    let capture = (0..16)
        .find_map(|_| {
            converter
                .capture_metadata_unit(&root, &root, &shard, 256)
                .unwrap()
        })
        .unwrap();
    authority
        .factory
        .require_complete_coverage(&capture.source)
        .unwrap();
    let generation = authority
        .standalone_generation_for_source(&capture.source)
        .unwrap();
    // Actual generic writer initialization can stop before its header write.
    drop(
        sinomo_dfs::DfsMetadataCandidateWriter::open(&capture.source.planned_target_root).unwrap(),
    );
    authority
        .factory
        .require_complete_coverage(&capture.source)
        .unwrap();
    std::fs::write(
        capture.source.planned_target_root.join("unknown-original"),
        b"retained data",
    )
    .unwrap();
    assert!(
        authority
            .factory
            .require_complete_coverage(&capture.source)
            .is_err()
    );
    std::fs::remove_file(capture.source.planned_target_root.join("unknown-original")).unwrap();
    // An actual staged writer can stop before materializing its first image.
    drop(
        sinomo_dfs::DfsMetadataCandidateWriter::open_generation(
            &capture.source.planned_target_root,
            generation.clone(),
        )
        .unwrap(),
    );
    authority
        .factory
        .require_complete_coverage(&capture.source)
        .unwrap();
    let source = crate::namespace_counts::NamespaceCountMaintenanceSource {
        captured: capture.clone(),
    };
    let limits = crate::namespace_counts::NamespaceCountMaintenanceLimits {
        max_objects: 64,
        max_encoded_bytes: 1024 * 1024,
        deadline: std::time::Instant::now() + std::time::Duration::from_secs(5),
    };
    let plan = crate::namespace_counts::plan_standalone_namespace_count_maintenance(
        std::slice::from_ref(&source),
        limits,
    )
    .unwrap();
    let objects = plan.counted_objects(&source, limits).unwrap();
    let built = crate::metadata::canonical_generation::build_canonical_generation(
        &capture,
        &generation,
        &objects,
        &capture.source.planned_target_root,
    )
    .unwrap();
    assert!(
        !capture
            .source
            .evidence_root
            .join("source-cut.json")
            .exists()
    );
    authority
        .factory
        .require_complete_coverage(&capture.source)
        .unwrap();
    let cut = converter
        .enroll_standalone_source(&root, &shard, &objects)
        .unwrap();
    assert_eq!(cut.target_exact_prefix, built.identity.prefix_proof);
    let extra = root.parent().unwrap().join("new-owner-before-certificate");
    std::fs::create_dir(&extra).unwrap();
    let refused = (0..16).find_map(|_| {
        converter
            .prepare_standalone_current_generation(&root, &shard, 256)
            .err()
    });
    assert!(
        refused.is_some(),
        "changed complete root set must refuse before install effects"
    );
    assert_eq!(
        std::fs::read(root.join("records/tables/snapshot.sobj")).unwrap(),
        original
    );
    assert!(!root.join("metadata-import-v7.json").exists());
    assert!(!root.join("metadata-generation.sobj").exists());
    drop(converter);
    drop(authority);
    drop(owner);
    runtime.close().unwrap();
}
