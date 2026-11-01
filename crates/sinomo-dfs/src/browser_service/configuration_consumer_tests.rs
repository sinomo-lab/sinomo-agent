//! Native controls for content planning and independent namespace admission.
use super::write_options_tests::{native_browser_policy, native_browser_runtime};
use super::*;

#[test]
fn write_configuration_no_links_pending_preserves_original_create_and_cas() {
    use crate::DaemonDfsBrowserNoLinksOperation as O;
    use crate::DaemonDfsBrowserNoLinksResponse as R;
    let (runtime, nodes) = native_browser_runtime("native-scoped-configuration-gap", 1);
    let data_dir = Arc::new(runtime.data_dir(&nodes[0]).clone());
    let handle = runtime.runtime_handle();
    let host = dfs_browser_component_host(&data_dir).unwrap();
    let backend =
        DfsBrowserBackend::for_component_data_dir(Arc::clone(&data_dir), &handle).unwrap();
    let root = "/documents/scoped-configuration-gap";
    let request = |path: &str, operation| crate::DaemonDfsBrowserNoLinksRequest {
        expected_cluster_id: None,
        root: root.into(),
        path: path.into(),
        operation,
    };
    backend.no_links(request(root, O::CreateDirAll)).unwrap();
    let existing_path = format!("{root}/existing.md");
    let R::Metadata { metadata: original } = backend
        .no_links(request(
            &existing_path,
            O::CreateNew {
                bytes: b"original".to_vec(),
            },
        ))
        .unwrap()
    else {
        panic!("actual native create receipt");
    };
    let control = crate::SharedDfsControlPlaneKvClient::new(Box::new(
        crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(
            &ClusterPaths::new(&data_dir),
            &handle,
        ),
    )
        as Box<dyn crate::DfsControlPlaneKvClient>);
    let gap = runtime
        .lose_configuration_delta_and_pause_rebuild(
            &nodes[0],
            crate::file_system_config_key(&sifs_file_system_id()),
        )
        .unwrap();
    let first =
        DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    native_browser_policy(&control, root, first.clone());
    let mut second = first;
    second.required_node_ids = vec![nodes[0].clone()];
    native_browser_policy(&control, root, second);
    gap.wait_rebuild(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert!(
        gap.dropped_delta().is_some(),
        "withhold a real committed delta"
    );
    let fresh_path = format!("{root}/fresh.md");
    let mut requests = vec![
        request(
            &fresh_path,
            O::CreateNew {
                bytes: b"fresh bytes".to_vec(),
            },
        ),
        request(
            &existing_path,
            O::ReplaceIfVersion {
                expected_object_id: original.object_id.clone(),
                expected_version: original.version.clone(),
                bytes: b"replaced bytes".to_vec(),
            },
        ),
    ];
    let before = backend
        .lock_service()
        .unwrap()
        .metadata_backend()
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    for pending in &mut requests {
        let original_request = pending.clone();
        let allocation = match &pending.operation {
            O::CreateNew { bytes } | O::ReplaceIfVersion { bytes, .. } => bytes.as_ptr(),
            _ => unreachable!(),
        };
        let owned = std::mem::replace(pending, request(root, O::Stat));
        *pending = match backend.try_no_links(owned).unwrap() {
            DfsBrowserMutationAttempt::ConfigurationPending(original) => original,
            DfsBrowserMutationAttempt::Complete(_) => {
                panic!("configuration gap admitted a content write")
            }
        };
        assert_eq!(*pending, original_request);
        let bytes = match &pending.operation {
            O::CreateNew { bytes } | O::ReplaceIfVersion { bytes, .. } => bytes,
            _ => unreachable!(),
        };
        assert_eq!(bytes.as_ptr(), allocation);
        let service = backend.lock_service().unwrap();
        let metadata = service.metadata_backend().unwrap();
        assert_eq!(metadata.journal_last_sequence().unwrap(), before);
        assert!(
            metadata
                .get_object_by_path(&sifs_file_system_id(), &fresh_path)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            metadata
                .get_object_by_path(&sifs_file_system_id(), &existing_path)
                .unwrap()
                .unwrap()
                .version
                .as_str(),
            original.version
        );
    }
    gap.release();
    handle.block_on(async {
        tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            host.ensure_write_configuration_ready(&data_dir, &handle),
        )
        .await
        .unwrap()
        .unwrap();
    });
    for request in requests {
        let DfsBrowserMutationAttempt::Complete(R::Metadata { metadata }) =
            backend.try_no_links(request).unwrap()
        else {
            panic!("one actual native content operation after readiness");
        };
        let service = backend.lock_service().unwrap();
        let object = crate::DfsObjectId::new(&metadata.object_id).unwrap();
        let version = crate::DfsVersion::new(&metadata.version).unwrap();
        let intents = service
            .metadata_backend()
            .unwrap()
            .intents_for_object_version_page(&object, &version, None, 16)
            .unwrap();
        assert_eq!(
            intents
                .intents
                .iter()
                .filter(|intent| intent.status == DfsWriteIntentStatus::Committed)
                .count(),
            1
        );
    }
    assert_eq!(
        backend
            .lock_service()
            .unwrap()
            .posix_read_file(&fresh_path)
            .unwrap(),
        b"fresh bytes"
    );
    assert_eq!(
        backend
            .lock_service()
            .unwrap()
            .posix_read_file(&existing_path)
            .unwrap(),
        b"replaced bytes"
    );
    drop(gap);
    drop(control);
    drop(backend);
    drop(host);
    runtime.close().unwrap();
}

#[test]
fn write_configuration_scoped_mkdir_keeps_metadata_only_namespace_admission() {
    let (runtime, nodes) = native_browser_runtime("native-scoped-mkdir-no-content-preflight", 1);
    let warm = DfsBrowserBackend::for_component_data_dir(
        Arc::new(runtime.data_dir(&nodes[0]).clone()),
        &runtime.runtime_handle(),
    )
    .unwrap();
    let scoped = |path: &str, operation| {
        runtime.rpc_request(
            &nodes[0],
            &nodes[0],
            ObjectRpcRequest::encode_body(
                DAEMON_DFS_BROWSER_NO_LINKS_ROUTE,
                sinomo_api::DAEMON_RPC_CATEGORY,
                &crate::DaemonDfsBrowserNoLinksRequest {
                    expected_cluster_id: None,
                    root: "/services".into(),
                    path: path.into(),
                    operation,
                },
            )
            .unwrap(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
    };
    // Genuine native authority loss remains fail-closed. This deliberately
    // broad fault is a negative control, not the metadata-only positive setup.
    let authority_denial = runtime.deny_configuration_floor(&nodes[0]).unwrap();
    let denied_path = "/services/scoped-metadata-authority-denied";
    let refusal = match scoped(
        denied_path,
        crate::DaemonDfsBrowserNoLinksOperation::CreateDirAll,
    ) {
        Err(error) => error,
        Ok(response) => match response
            .decode_body::<crate::DaemonDfsBrowserNoLinksResponse>()
            .unwrap()
        {
            crate::DaemonDfsBrowserNoLinksResponse::Failed {
                failure: crate::DaemonDfsBrowserNoLinksFailure::Unavailable { detail, .. },
            } => detail,
            response => panic!("native authority loss admitted namespace mutation: {response:?}"),
        },
    };
    assert!(
        refusal.contains("native configuration floor is unavailable at the injected read boundary"),
        "{refusal}"
    );
    drop(authority_denial);
    // Deny only the browser's content-planning preflight. The real native
    // metadata owner and namespace authority stay available and unchanged.
    let content_denial = runtime.deny_browser_content_preflight(&nodes[0]).unwrap();
    runtime
        .rpc_request(
            &nodes[0],
            &nodes[0],
            ObjectRpcRequest::encode_body(
                DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
                sinomo_api::DAEMON_RPC_CATEGORY,
                &crate::DaemonDfsBrowserCreateDirAllRequest {
                    path: "/services/normal-metadata-mkdir".into(),
                },
            )
            .unwrap(),
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
        .unwrap();
    let response: crate::DaemonDfsBrowserNoLinksResponse = scoped(
        "/services/scoped-metadata-mkdir/nested",
        crate::DaemonDfsBrowserNoLinksOperation::CreateDirAll,
    )
    .unwrap()
    .decode_body()
    .unwrap();
    assert_eq!(
        response,
        crate::DaemonDfsBrowserNoLinksResponse::DirectoryCreated
    );
    assert_eq!(
        content_denial.denied_browser_content_preflights(),
        0,
        "ordinary and scoped mkdir must not borrow content readiness"
    );
    let content_path = "/services/scoped-content-preflight-denied.md";
    let error = scoped(
        content_path,
        crate::DaemonDfsBrowserNoLinksOperation::CreateNew {
            bytes: b"must not be created".to_vec(),
        },
    )
    .unwrap_err();
    assert!(
        error.contains("browser content readiness denied at injected preflight"),
        "{error}"
    );
    assert_eq!(
        content_denial.denied_browser_content_preflights(),
        1,
        "the actual content-writing route must witness the same negative seam"
    );
    drop(content_denial);
    let mut inspection = runtime.client(&nodes[0]).unwrap();
    assert_eq!(
        inspection
            .posix_stat_distributed("/services/scoped-metadata-mkdir/nested")
            .unwrap()
            .kind,
        crate::DfsObjectKind::Directory
    );
    for path in [denied_path, content_path] {
        assert!(
            matches!(
                inspection.posix_stat_distributed(path),
                Err(crate::DfsPosixError::NotFound { .. })
            ),
            "rejected native operation must not create {path}"
        );
    }
    drop(inspection);
    drop(warm);
    runtime.close().unwrap();
}

#[test]
fn write_configuration_feature_options_keep_the_captured_cut_across_a_real_delta() {
    let (runtime, nodes) = native_browser_runtime("native-feature-captured-configuration", 1);
    let data_dir = Arc::new(runtime.data_dir(&nodes[0]).clone());
    let handle = runtime.runtime_handle();
    let backend =
        DfsBrowserBackend::for_component_data_dir(Arc::clone(&data_dir), &handle).unwrap();
    let control = crate::SharedDfsControlPlaneKvClient::new(Box::new(
        crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(
            &ClusterPaths::new(&data_dir),
            &handle,
        ),
    )
        as Box<dyn crate::DfsControlPlaneKvClient>);
    let path = "/documents/captured-feature-options";
    let first =
        DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    native_browser_policy(&control, path, first.clone());
    let mut second = first;
    second.required_node_ids = vec![nodes[0].clone()];
    let attempt = backend
        .try_with_configured_mutation_service(&[path], (), |service, configuration, ()| {
            let configuration = configuration.expect("actual native captured configuration");
            let initial = backend.write_options_for_path_with_configuration(
                service,
                path,
                Some(configuration),
            )?;
            assert!(initial.policy.required_node_ids.is_empty());
            native_browser_policy(&control, path, second);
            let retained = backend.write_options_for_path_with_configuration(
                service,
                path,
                Some(configuration),
            )?;
            assert_eq!(
                retained, initial,
                "candidate and policy planning use the captured cut together"
            );
            Ok(())
        })
        .unwrap();
    assert!(matches!(attempt, DfsBrowserMutationAttempt::Complete(())));
    let host = dfs_browser_component_host(&data_dir).unwrap();
    handle.block_on(async {
        tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            host.ensure_write_configuration_ready(&data_dir, &handle),
        )
        .await
        .unwrap()
        .unwrap();
    });
    let current = backend
        .try_with_configured_mutation_service(&[path], (), |service, configuration, ()| {
            backend.write_options_for_path_with_configuration(service, path, configuration)
        })
        .unwrap();
    let DfsBrowserMutationAttempt::Complete(current) = current else {
        panic!("new native cut must be available");
    };
    assert_eq!(current.policy.required_node_ids, vec![nodes[0].clone()]);
    drop(control);
    drop(backend);
    drop(host);
    runtime.close().unwrap();
}
