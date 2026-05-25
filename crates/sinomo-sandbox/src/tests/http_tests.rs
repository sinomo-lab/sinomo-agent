use super::*;

#[test]
fn sandbox_http_fetch_is_disabled_by_default_and_allows_allowlisted_hosts() {
    struct FakeTransport;
    impl SandboxHttpTransport for FakeTransport {
        fn fetch(
            &self,
            _request: &SandboxHttpRequest,
        ) -> Result<SandboxHttpResponse, SandboxNetworkError> {
            Ok(SandboxHttpResponse {
                status: 200,
                headers: BTreeMap::new(),
                body: b"ok".to_vec(),
            })
        }
    }

    let request = SandboxHttpRequest::get("https://packages.example/archive.tar.gz");
    let error = sandbox_http_fetch(&SandboxPolicy::default(), &FakeTransport, &request)
        .expect_err("network disabled");
    assert!(matches!(
        error,
        SandboxNetworkError::PolicyDenied(SandboxPolicyDenial { ref code, .. })
            if code == "network-capability-denied"
    ));

    let mut policy = SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    policy
        .allowed_http_hosts
        .insert("packages.example".to_string());
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    let response = sandbox_http_fetch(&policy, &FakeTransport, &request).expect("fetch");
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"ok");
}

#[test]
fn sandbox_http_fetch_denies_unlisted_hosts_and_large_responses() {
    struct LargeTransport;
    impl SandboxHttpTransport for LargeTransport {
        fn fetch(
            &self,
            _request: &SandboxHttpRequest,
        ) -> Result<SandboxHttpResponse, SandboxNetworkError> {
            Ok(SandboxHttpResponse {
                status: 200,
                headers: BTreeMap::new(),
                body: vec![b'x'; 8],
            })
        }
    }

    let mut policy = SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    policy
        .allowed_http_hosts
        .insert("allowed.example".to_string());
    let denied = SandboxHttpRequest::get("https://other.example/file");
    let error = sandbox_http_fetch(&policy, &LargeTransport, &denied).expect_err("host denied");
    assert!(matches!(
        error,
        SandboxNetworkError::PolicyDenied(SandboxPolicyDenial { ref code, .. })
            if code == "network-host-denied"
    ));

    let mut request = SandboxHttpRequest::get("https://allowed.example/file");
    request.max_response_bytes = 4;
    let error =
        sandbox_http_fetch(&policy, &LargeTransport, &request).expect_err("response too large");
    assert!(matches!(
        error,
        SandboxNetworkError::ResponseTooLarge {
            actual: 8,
            limit: 4
        }
    ));
}

#[test]
fn sandbox_http_fetch_denies_non_http_schemes() {
    struct FakeTransport;
    impl SandboxHttpTransport for FakeTransport {
        fn fetch(
            &self,
            _request: &SandboxHttpRequest,
        ) -> Result<SandboxHttpResponse, SandboxNetworkError> {
            Ok(SandboxHttpResponse {
                status: 200,
                headers: BTreeMap::new(),
                body: b"ok".to_vec(),
            })
        }
    }

    let mut policy = SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    policy
        .allowed_http_hosts
        .insert("packages.example".to_string());
    let request = SandboxHttpRequest::get("ftp://packages.example/archive.tar.gz");

    let error = sandbox_http_fetch(&policy, &FakeTransport, &request).expect_err("scheme denied");

    assert!(matches!(
        error,
        SandboxNetworkError::PolicyDenied(SandboxPolicyDenial { ref code, .. })
            if code == "network-scheme-denied"
    ));
}

#[test]
fn sandbox_http_fetch_enforces_request_and_response_policy_budgets() {
    struct FakeTransport;
    impl SandboxHttpTransport for FakeTransport {
        fn fetch(
            &self,
            _request: &SandboxHttpRequest,
        ) -> Result<SandboxHttpResponse, SandboxNetworkError> {
            Ok(SandboxHttpResponse {
                status: 200,
                headers: BTreeMap::new(),
                body: b"ok".to_vec(),
            })
        }
    }

    let mut policy = SandboxPolicy::default();
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    policy
        .allowed_http_hosts
        .insert("packages.example".to_string());
    policy.max_http_request_bytes = 4;
    policy.max_http_response_bytes = 8;

    let mut request = SandboxHttpRequest::get("https://packages.example/archive.tar.gz");
    request.body = b"too large".to_vec();
    let error = sandbox_http_fetch(&policy, &FakeTransport, &request)
        .expect_err("request body budget denied");
    assert!(matches!(
        error,
        SandboxNetworkError::PolicyDenied(SandboxPolicyDenial { ref code, .. })
            if code == "network-request-bytes-denied"
    ));

    let mut request = SandboxHttpRequest::get("https://packages.example/archive.tar.gz");
    request.max_response_bytes = 16;
    let error =
        sandbox_http_fetch(&policy, &FakeTransport, &request).expect_err("response budget denied");
    assert!(matches!(
        error,
        SandboxNetworkError::PolicyDenied(SandboxPolicyDenial { ref code, .. })
            if code == "network-response-budget-denied"
    ));
}

#[test]
fn sandbox_http_exchange_record_redacts_sensitive_header_names() {
    let sandbox_id = SandboxId::from_legacy_named("sandbox_http_exchange").expect("sandbox");
    let command_id = SandboxCommandId::from_legacy_named("command_http_exchange").expect("command");
    let request_id =
        SandboxNetworkRequestId::from_legacy_named("request_http_exchange").expect("request");
    let mut request =
        SandboxHttpRequest::get("https://packages.example/index.json?token=secret#fragment");
    request
        .headers
        .insert("Authorization".to_string(), "Bearer secret".to_string());
    request
        .headers
        .insert("Accept".to_string(), "*/*".to_string());
    let response = SandboxHttpResponse {
        status: 200,
        headers: BTreeMap::new(),
        body: b"{}".to_vec(),
    };
    let record = sandbox_http_exchange_record_with_id(
        request_id.clone(),
        sandbox_id.clone(),
        command_id.clone(),
        &request,
        Some(&response),
    )
    .expect("record");
    assert_eq!(record.request_id, request_id);
    assert_eq!(record.sandbox_id, sandbox_id);
    assert_eq!(record.command_id, command_id);
    assert_eq!(record.host, "packages.example");
    assert_eq!(record.url, "https://packages.example/index.json");
    assert!(!record.url.contains("secret"));
    assert!(!record.url.contains("fragment"));
    assert_eq!(record.status, Some(200));
    assert_eq!(record.response_bytes, 2);
    assert_eq!(record.redacted_headers, vec!["Authorization"]);
}

#[test]
fn sandbox_http_exchange_record_redacts_userinfo_and_preserves_authority() {
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_http_exchange_authority").expect("sandbox");
    let command_id =
        SandboxCommandId::from_legacy_named("command_http_exchange_authority").expect("command");
    let request_id = SandboxNetworkRequestId::from_legacy_named("request_http_exchange_authority")
        .expect("request");
    let request =
        SandboxHttpRequest::get("http://user:secret@Packages.Example:8443/pkg?token=secret");

    let record =
        sandbox_http_exchange_record_with_id(request_id, sandbox_id, command_id, &request, None)
            .expect("record");

    assert_eq!(record.host, "packages.example");
    assert_eq!(record.url, "http://packages.example:8443/pkg");
    assert!(!record.url.contains("user"));
    assert!(!record.url.contains("secret"));
}

#[test]
fn sandbox_http_exchange_record_redacts_ipv6_urls_without_losing_port() {
    let sandbox_id = SandboxId::from_legacy_named("sandbox_http_exchange_ipv6").expect("sandbox");
    let command_id =
        SandboxCommandId::from_legacy_named("command_http_exchange_ipv6").expect("command");
    let request_id =
        SandboxNetworkRequestId::from_legacy_named("request_http_exchange_ipv6").expect("request");
    let request = SandboxHttpRequest::get("http://[::1]:8443/pkg?token=secret#fragment");

    let record =
        sandbox_http_exchange_record_with_id(request_id, sandbox_id, command_id, &request, None)
            .expect("record");

    assert_eq!(record.host, "::1");
    assert_eq!(record.url, "http://[::1]:8443/pkg");
    assert!(!record.url.contains("secret"));
    assert!(!record.url.contains("fragment"));
}

#[test]
fn sandbox_http_exchange_record_rejects_malformed_ipv6_authority() {
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_http_exchange_bad_ipv6").expect("sandbox");
    let command_id =
        SandboxCommandId::from_legacy_named("command_http_exchange_bad_ipv6").expect("command");
    let request_id = SandboxNetworkRequestId::from_legacy_named("request_http_exchange_bad_ipv6")
        .expect("request");

    for url in [
        "http://[::1/pkg",
        "http://[::1]extra/pkg",
        "http://[::1]:not-a-port/pkg",
    ] {
        let request = SandboxHttpRequest::get(url);
        let denial = sandbox_http_exchange_record_with_id(
            request_id.clone(),
            sandbox_id.clone(),
            command_id.clone(),
            &request,
            None,
        )
        .expect_err("malformed IPv6 authority should be rejected");
        assert_eq!(denial.code, "network-url-invalid");
    }
}

#[test]
fn sandbox_http_exchange_record_rejects_malformed_unbracketed_authority() {
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_http_exchange_bad_authority").expect("sandbox");
    let command_id = SandboxCommandId::from_legacy_named("command_http_exchange_bad_authority")
        .expect("command");
    let request_id =
        SandboxNetworkRequestId::from_legacy_named("request_http_exchange_bad_authority")
            .expect("request");

    for url in [
        "http://packages.example:not-a-port/pkg",
        "http://packages.example:/pkg",
        "http://::1/pkg",
    ] {
        let request = SandboxHttpRequest::get(url);
        let denial = sandbox_http_exchange_record_with_id(
            request_id.clone(),
            sandbox_id.clone(),
            command_id.clone(),
            &request,
            None,
        )
        .expect_err("malformed authority should be rejected");
        assert_eq!(denial.code, "network-url-invalid");
    }
}

#[test]
fn sandbox_http_exchange_record_round_trips_through_sobj() {
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_http_exchange_roundtrip").expect("sandbox");
    let command_id =
        SandboxCommandId::from_legacy_named("command_http_exchange_roundtrip").expect("command");
    let request_id = SandboxNetworkRequestId::from_legacy_named("request_http_exchange_roundtrip")
        .expect("request");
    let request = SandboxHttpRequest::get("https://packages.example/index.json?token=secret");
    let response = SandboxHttpResponse {
        status: 204,
        headers: BTreeMap::new(),
        body: Vec::new(),
    };
    let record = sandbox_http_exchange_record_with_id(
        request_id.clone(),
        sandbox_id.clone(),
        command_id.clone(),
        &request,
        Some(&response),
    )
    .expect("record");

    assert_eq!(
        sandbox_http_exchange_record_path(&sandbox_id, &command_id, &request_id),
        format!(
            "{}/logs/{command_id}.{request_id}.http.sobj",
            sandbox_instance_root(&sandbox_id)
        )
    );
    let bytes = encode_http_exchange_record(&record).expect("encode");
    assert_eq!(decode_http_exchange_record(&bytes).expect("decode"), record);
}

#[test]
fn sandbox_http_exchange_record_persists_to_local_logs() {
    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_http_exchange_local");
    let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
    let sandbox_id = SandboxId::from_legacy_named("sandbox_http_exchange_local").expect("sandbox");
    let command_id =
        SandboxCommandId::from_legacy_named("command_http_exchange_local").expect("command");
    let request_id =
        SandboxNetworkRequestId::from_legacy_named("request_http_exchange_local").expect("request");
    let request = SandboxHttpRequest::get("https://packages.example/index.json");
    let response = SandboxHttpResponse {
        status: 200,
        headers: BTreeMap::new(),
        body: b"{}".to_vec(),
    };
    let record = sandbox_http_exchange_record_with_id(
        request_id.clone(),
        sandbox_id,
        command_id.clone(),
        &request,
        Some(&response),
    )
    .expect("record");

    persist_http_exchange_record(&mut sandbox_fs, &record).expect("persist");

    assert_eq!(
        load_http_exchange_record(&mut sandbox_fs, &command_id, &request_id).expect("load"),
        record
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sandbox_http_fetch_recording_persists_successful_exchange() {
    struct FakeTransport;

    impl SandboxHttpTransport for FakeTransport {
        fn fetch(
            &self,
            _request: &SandboxHttpRequest,
        ) -> Result<SandboxHttpResponse, SandboxNetworkError> {
            Ok(SandboxHttpResponse {
                status: 200,
                headers: BTreeMap::new(),
                body: b"ok".to_vec(),
            })
        }
    }

    let (_manifest, layout, runtime_root) =
        create_test_instance("sandbox_http_fetch_recording_success");
    let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_http_hosts
        .insert("packages.example".to_string());
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_http_fetch_recording_success").expect("sandbox");
    let command_id = SandboxCommandId::from_legacy_named("command_http_fetch_recording_success")
        .expect("command");
    let request = SandboxHttpRequest::get("https://packages.example/index.json");

    let recorded = sandbox_http_fetch_recording(
        &policy,
        &FakeTransport,
        &mut sandbox_fs,
        sandbox_id,
        command_id.clone(),
        &request,
    )
    .expect("fetch");

    assert_eq!(recorded.response.status, 200);
    assert_eq!(recorded.record.status, Some(200));
    assert_eq!(
        load_http_exchange_record(&mut sandbox_fs, &command_id, &recorded.request_id)
            .expect("load"),
        recorded.record
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sandbox_http_fetch_to_file_stores_response_body_and_audit_record() {
    struct FakeTransport;

    impl SandboxHttpTransport for FakeTransport {
        fn fetch(
            &self,
            _request: &SandboxHttpRequest,
        ) -> Result<SandboxHttpResponse, SandboxNetworkError> {
            Ok(SandboxHttpResponse {
                status: 200,
                headers: BTreeMap::new(),
                body: b"package-bytes".to_vec(),
            })
        }
    }

    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_http_fetch_to_file");
    let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_http_hosts
        .insert("packages.example".to_string());
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    let request = SandboxHttpRequest::get("https://packages.example/archive.tar.gz");
    let output_path = SandboxPath::new("/work/archive.tar.gz").expect("output");

    let result = sandbox_http_fetch_to_file(
        &policy,
        &FakeTransport,
        &mut sandbox_fs,
        SandboxId::from_legacy_named("sandbox_http_fetch_to_file").expect("sandbox"),
        SandboxCommandId::from_legacy_named("command_http_fetch_to_file").expect("command"),
        &request,
        &output_path,
        &SandboxResourceLimits::default(),
    )
    .expect("fetch");

    assert_eq!(result.status, 200);
    assert_eq!(result.response_bytes, 13);
    assert_eq!(
        sandbox_fs.read(&output_path, None).expect("read output"),
        b"package-bytes"
    );
    assert_eq!(
        load_http_exchange_record(
            &mut sandbox_fs,
            &result.record.command_id,
            &result.request_id
        )
        .expect("load audit"),
        result.record
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sandbox_http_fetch_to_file_obeys_workspace_write_limits() {
    struct FakeTransport;

    impl SandboxHttpTransport for FakeTransport {
        fn fetch(
            &self,
            _request: &SandboxHttpRequest,
        ) -> Result<SandboxHttpResponse, SandboxNetworkError> {
            Ok(SandboxHttpResponse {
                status: 200,
                headers: BTreeMap::new(),
                body: b"package-bytes".to_vec(),
            })
        }
    }

    let (_manifest, layout, runtime_root) =
        create_test_instance("sandbox_http_fetch_to_file_write_limit");
    let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_http_hosts
        .insert("packages.example".to_string());
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    let request = SandboxHttpRequest::get("https://packages.example/archive.tar.gz");
    let output_path = SandboxPath::new("/work/archive.tar.gz").expect("output");
    let command_id =
        SandboxCommandId::from_legacy_named("command_http_fetch_write_limit").expect("command");
    let mut limits = SandboxResourceLimits::default();
    limits.max_written_bytes_per_command = 4;

    let error = sandbox_http_fetch_to_file(
        &policy,
        &FakeTransport,
        &mut sandbox_fs,
        SandboxId::from_legacy_named("sandbox_http_fetch_to_file_write_limit").expect("sandbox"),
        command_id.clone(),
        &request,
        &output_path,
        &limits,
    )
    .expect_err("fetch body write should obey sandbox write quota");

    assert!(matches!(
        error,
        SandboxRecordedHttpFetchError::Fs(SandboxFsError::QuotaExceeded {
            ref kind,
            actual: 13,
            limit: 4,
        }) if kind == "written-bytes-per-command"
    ));
    assert!(matches!(
        sandbox_fs
            .read(&output_path, None)
            .expect_err("quota-denied body must not be written"),
        SandboxFsError::NotFound { .. }
    ));
    let log_entries = sandbox_fs
        .list(&SandboxPath::new("/logs").expect("logs path"))
        .expect("audit log entries");
    assert_eq!(
        log_entries
            .iter()
            .filter(|entry| entry.path.to_string().contains(&format!("{command_id}.")))
            .count(),
        1,
        "failed fetch writes should still keep the HTTP audit record"
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sandbox_http_fetch_to_file_stores_response_body_and_audit_record_in_sifs() {
    struct FakeTransport;

    impl SandboxHttpTransport for FakeTransport {
        fn fetch(
            &self,
            _request: &SandboxHttpRequest,
        ) -> Result<SandboxHttpResponse, SandboxNetworkError> {
            Ok(SandboxHttpResponse {
                status: 200,
                headers: BTreeMap::new(),
                body: b"sifs-package-bytes".to_vec(),
            })
        }
    }

    let runtime_root = unique_test_dir("sinomo-sandbox-http-fetch-to-file-sifs");
    let network = InProcessNetwork::new();
    let owner = NodeId::from_legacy_named("node_sifs_http_fetch_owner").expect("owner");
    let replica = NodeId::from_legacy_named("node_sifs_http_fetch_replica").expect("replica");
    let metadata_root = runtime_root.join("metadata");
    let owner_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
            .expect("owner metadata");
    let mut owner_dfs = NetworkedDfsService::new(
        network.add_node(owner.clone()),
        LocalContentStore::new(runtime_root.join("owner-content")).expect("owner content"),
    )
    .with_metadata_backend(owner_backend)
    .expect("owner metadata backend");
    owner_dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let replica_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root.clone())
            .expect("replica metadata");
    let mut replica_dfs = NetworkedDfsService::new(
        network.add_node(replica.clone()),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend");
    let mut store = SifsSandboxInstanceStore::new(owner_dfs)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let manifest = store
        .create(
            SandboxId::from_legacy_named("sandbox_sifs_http_fetch_to_file").expect("sandbox"),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest)
        .with_replica_candidates(vec![owner.clone(), replica.clone()]);
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_http_hosts
        .insert("packages.example".to_string());
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    let request = SandboxHttpRequest::get("https://packages.example/archive.tar.gz");
    let output_path = SandboxPath::new("/work/archive.tar.gz").expect("output");

    let result = sandbox_http_fetch_to_file(
        &policy,
        &FakeTransport,
        &mut sandbox_fs,
        manifest.sandbox_id.clone(),
        SandboxCommandId::from_legacy_named("command_sifs_http_fetch_to_file").expect("command"),
        &request,
        &output_path,
        &manifest.limits,
    )
    .expect("fetch");

    assert_eq!(result.status, 200);
    assert_eq!(result.response_bytes, 18);
    assert_eq!(
        sandbox_fs.read(&output_path, None).expect("read output"),
        b"sifs-package-bytes"
    );
    assert_eq!(
        load_http_exchange_record(
            &mut sandbox_fs,
            &result.record.command_id,
            &result.request_id
        )
        .expect("load audit"),
        result.record
    );
    replica_dfs
        .process_incoming()
        .expect("replica receives fetched body and audit record");
    let replica_backend =
        LocalJsonMetadataBackend::new(DfsMetadataShardId::new("sifs"), metadata_root)
            .expect("replica metadata reload");
    let replica_dfs = NetworkedDfsService::new(
        network.add_node(replica),
        LocalContentStore::new(runtime_root.join("replica-content")).expect("replica content"),
    )
    .with_metadata_backend(replica_backend)
    .expect("replica metadata backend reload");
    let mut replica_store = SifsSandboxInstanceStore::new(replica_dfs);
    let replica_manifest = replica_store
        .load(&manifest.sandbox_id)
        .expect("replica load manifest");
    let mut replica_fs = SifsSandboxFileSystem::new(replica_store.into_inner(), &replica_manifest);
    assert_eq!(
        replica_fs.read(&output_path, None).expect("replica output"),
        b"sifs-package-bytes"
    );
    assert_eq!(
        load_http_exchange_record(
            &mut replica_fs,
            &result.record.command_id,
            &result.request_id
        )
        .expect("replica audit"),
        result.record
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sandbox_http_fetch_recording_persists_transport_failure() {
    struct FailingTransport;

    impl SandboxHttpTransport for FailingTransport {
        fn fetch(
            &self,
            _request: &SandboxHttpRequest,
        ) -> Result<SandboxHttpResponse, SandboxNetworkError> {
            Err(SandboxNetworkError::Transport {
                detail: "dial failed".to_string(),
            })
        }
    }

    let (_manifest, layout, runtime_root) =
        create_test_instance("sandbox_http_fetch_recording_failure");
    let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
    let mut policy = SandboxPolicy::default();
    policy
        .allowed_http_hosts
        .insert("packages.example".to_string());
    policy
        .allowed_command_capabilities
        .insert(SANDBOX_NETWORK_HTTP_CAPABILITY.to_string());
    let sandbox_id =
        SandboxId::from_legacy_named("sandbox_http_fetch_recording_failure").expect("sandbox");
    let command_id = SandboxCommandId::from_legacy_named("command_http_fetch_recording_failure")
        .expect("command");
    let request = SandboxHttpRequest::get("https://packages.example/index.json");

    let error = sandbox_http_fetch_recording(
        &policy,
        &FailingTransport,
        &mut sandbox_fs,
        sandbox_id,
        command_id.clone(),
        &request,
    )
    .expect_err("fetch should fail");

    assert!(matches!(
        error,
        SandboxRecordedHttpFetchError::Network(SandboxNetworkError::Transport { .. })
    ));
    let logs_path = SandboxPath::new("/logs").expect("logs");
    let http_logs = sandbox_fs
        .list(&logs_path)
        .expect("list logs")
        .into_iter()
        .filter(|entry| entry.path.as_str().ends_with(".http.sobj"))
        .collect::<Vec<_>>();
    assert_eq!(http_logs.len(), 1);
    let bytes = sandbox_fs
        .read(&http_logs[0].path, None)
        .expect("read record");
    let record = decode_http_exchange_record(&bytes).expect("decode");
    assert_eq!(record.command_id, command_id);
    assert_eq!(record.status, None);
    assert_eq!(record.response_bytes, 0);
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sandbox_http_exchange_record_persists_to_sifs_logs() {
    let runtime_root = unique_test_dir("sinomo-sandbox-http-exchange-sifs");
    let network = InProcessNetwork::new();
    let node_id = NodeId::from_legacy_named("node_sifs_http_exchange").expect("node");
    let mut dfs = NetworkedDfsService::new(
        network.add_node(node_id),
        LocalContentStore::new(runtime_root.join("content")).expect("content"),
    );
    dfs.bootstrap_distributed_sifs().expect("bootstrap");
    let mut store = SifsSandboxInstanceStore::new(dfs);
    let manifest = store
        .create(
            SandboxId::from_legacy_named("sandbox_sifs_http_exchange").expect("sandbox"),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").expect("toolchain"),
            OffsetDateTime::now_utc(),
        )
        .expect("create");
    let mut sandbox_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
    let command_id =
        SandboxCommandId::from_legacy_named("command_sifs_http_exchange").expect("command");
    let request_id =
        SandboxNetworkRequestId::from_legacy_named("request_sifs_http_exchange").expect("request");
    let request = SandboxHttpRequest::get("https://packages.example/index.json");
    let record = sandbox_http_exchange_record_with_id(
        request_id.clone(),
        manifest.sandbox_id.clone(),
        command_id.clone(),
        &request,
        None,
    )
    .expect("record");

    persist_http_exchange_record(&mut sandbox_fs, &record).expect("persist");

    assert_eq!(
        load_http_exchange_record(&mut sandbox_fs, &command_id, &request_id).expect("load"),
        record
    );
    fs::remove_dir_all(runtime_root).ok();
}

#[test]
fn sandbox_http_exchange_event_reports_redacted_request_metadata() {
    let sandbox_id = SandboxId::from_legacy_named("sandbox_http_exchange_event").expect("sandbox");
    let command_id =
        SandboxCommandId::from_legacy_named("command_http_exchange_event").expect("command");
    let request_id =
        SandboxNetworkRequestId::from_legacy_named("request_http_exchange_event").expect("request");
    let request = SandboxHttpRequest::get("https://packages.example/index.json");
    let record = sandbox_http_exchange_record_with_id(
        request_id.clone(),
        sandbox_id.clone(),
        command_id.clone(),
        &request,
        None,
    )
    .expect("record");
    let node_id = NodeId::from_legacy_named("node_http_exchange_event").expect("node");

    let event = sandbox_http_exchange_event(node_id.clone(), &record);

    assert_eq!(event.origin_node_id, node_id);
    assert_eq!(event.source.as_str(), "sandbox.network");
    assert_eq!(event.kind.as_str(), "sandbox.network.http");
    assert_eq!(event.payload["sandbox_id"], sandbox_id.to_string());
    assert_eq!(
        event.payload["workspace_root"],
        sandbox_instance_root(&sandbox_id)
    );
    assert_eq!(event.payload["command_id"], command_id.to_string());
    assert_eq!(event.payload["request_id"], request_id.to_string());
    assert_eq!(event.payload["method"], "GET");
    assert_eq!(event.payload["url"], "https://packages.example/index.json");
    assert_eq!(event.payload["host"], "packages.example");
    assert!(
        !event.payload["url"]
            .as_str()
            .unwrap_or_default()
            .contains("secret")
    );
    assert_eq!(event.payload["response_bytes"], 0);
}

#[test]
fn http_exchange_events_append_from_sandbox_logs() {
    let (_manifest, layout, runtime_root) = create_test_instance("sandbox_http_exchange_events");
    let mut sandbox_fs = LocalSandboxFileSystem::new(layout);
    let sandbox_id = SandboxId::from_legacy_named("sandbox_http_exchange_events").expect("sandbox");
    let command_id =
        SandboxCommandId::from_legacy_named("command_http_exchange_events").expect("command");
    let request_id = SandboxNetworkRequestId::from_legacy_named("request_http_exchange_events")
        .expect("request");
    let request = SandboxHttpRequest::get("https://packages.example/index.json");
    let record = sandbox_http_exchange_record_with_id(
        request_id.clone(),
        sandbox_id,
        command_id.clone(),
        &request,
        None,
    )
    .expect("record");
    persist_http_exchange_record(&mut sandbox_fs, &record).expect("persist");
    let mut event_store = sinomo_events::InMemoryEventStore::new();
    let node_id = NodeId::from_legacy_named("node_http_exchange_events").expect("node");

    let receipts =
        append_sandbox_http_exchange_events(&mut sandbox_fs, node_id.clone(), &mut event_store)
            .expect("append events");

    assert_eq!(receipts.len(), 1);
    let events = event_store
        .replay(sinomo_events::ReplayQuery::by_kind_prefix(
            "sandbox.network",
        ))
        .expect("replay");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.origin_node_id, node_id);
    assert_eq!(events[0].event.kind.as_str(), "sandbox.network.http");
    assert_eq!(
        events[0].event.payload["request_id"],
        request_id.to_string()
    );
    fs::remove_dir_all(runtime_root).ok();
}
