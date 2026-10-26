use super::*;
use crate::keyspace::{cluster_connectivity_key, cluster_endpoint_key, cluster_member_key};
use crate::kv_local_store::ClusterKvPathsExt;
use sinomo_base::ClusterBootstrapState;
use sinomo_kv::InProcessKvStore;
use sinomo_network::{
    ClusterDirectory as _, ClusterObjectRpcAsyncHost, ClusterObjectRpcClient,
    ClusterObjectRpcOptions, ObjectRpcCall,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(serde::Serialize, serde::Deserialize)]
struct OriginalOperation {
    operation_id: String,
    bytes: Vec<u8>,
}

struct ReadOnlyHost;
impl ClusterObjectRpcAsyncHost for ReadOnlyHost {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColdReadCase {
    Success,
    ObservedCutChanged,
    Deadline,
}

async fn cold_source_scenario(case: ColdReadCase) {
    let _registry = TEST_NETWORK_MODULE_REGISTRY_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let root = sinomo_testdir::TestDir::new(&format!("cold-authenticated-source-read-{case:?}"));
    let data = sinomo_local::SinomoDataDir::new(root.join("caller"));
    let remote_data = sinomo_local::SinomoDataDir::new(root.join("remote"));
    data.ensure_root_layout().unwrap();
    remote_data.ensure_root_layout().unwrap();
    let local_key = sinomo_node::load_or_create_main_node_identity(&data.root()).unwrap();
    let remote_key = sinomo_node::load_or_create_main_node_identity(&remote_data.root()).unwrap();
    let local = NodeId::from_legacy_named("cold_source_caller").unwrap();
    let remote = NodeId::from_legacy_named("cold_source_remote").unwrap();
    let cluster =
        sinomo_node::ClusterId::from_legacy_named(format!("cold_source_cluster_{case:?}")).unwrap();
    let listener = sinomo_network::AsyncDaemonControlListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let endpoint =
        sinomo_network::daemon_lan_endpoint(&remote, listener.local_addr_string().unwrap(), 0);
    let now = OffsetDateTime::now_utc();
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster.clone(),
        "cold source",
        local.clone(),
        "caller",
        local_key.public_key,
        now,
    );
    let mut member = ClusterMemberRecord::local_non_voting(
        cluster.clone(),
        remote.clone(),
        "remote",
        remote_key.public_key.clone(),
        now,
    );
    member.endpoints.push(endpoint.clone());
    state.members.push(member);
    let paths = ClusterPaths::new(&data);
    paths.ensure().unwrap();
    crate::write_cluster_state(&paths, &state).unwrap();
    let expired = sinomo_network::cluster_connectivity_record(
        &state,
        &endpoint,
        sinomo_network::ClusterConnectivityState::Healthy,
        None,
        0,
        None,
        None,
        now - time::Duration::hours(1),
        sinomo_network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
    );
    let mut kv = InProcessKvStore::new();
    for member in &state.members {
        kv.put_serde(cluster_member_key(&member.node_id), member, None)
            .unwrap();
    }
    kv.put_serde(cluster_endpoint_key(&remote, &endpoint), &endpoint, None)
        .unwrap();
    kv.put_serde(
        cluster_connectivity_key(&local, &remote, &endpoint.endpoint_id),
        &expired,
        None,
    )
    .unwrap();
    crate::write_cluster_kv_snapshot(&paths, &kv.snapshot().unwrap()).unwrap();
    let before = std::fs::read(&paths.kv_paths().snapshot).unwrap();
    sinomo_kv::deny_process_local_raft_access_for_root(paths.kv_paths().raft.clone()).unwrap();
    let observed = Arc::new(KvClusterDirectory::new(paths.clone()));
    assert!(observed.connectivity_records().unwrap().is_empty());
    assert_eq!(
        observed.connectivity_freshness_baseline().unwrap().records,
        vec![expired.clone()]
    );

    let mut changed = expired;
    changed.state = sinomo_network::ClusterConnectivityState::Unreachable;
    changed.consecutive_failures = 3;
    kv.put_serde(
        cluster_connectivity_key(&local, &remote, &endpoint.endpoint_id),
        &changed,
        None,
    )
    .unwrap();
    let replacement = kv.snapshot().unwrap();
    let server_paths = paths.clone();
    let reads = Arc::new(AtomicUsize::new(0));
    let server_reads = reads.clone();
    let server_directory = sinomo_network::DirectorySnapshot::try_from_records(
        sinomo_network::DirectoryVersion(1),
        state.members.clone(),
        vec![endpoint.clone()],
    )
    .unwrap();
    let identity = Arc::new(
        sinomo_network::DaemonRoutedSessionIdentity::new(remote.clone(), remote_data).unwrap(),
    );
    let provider = sinomo_network::ClusterLinkProvider::new(
        remote.clone(),
        identity.clone(),
        sinomo_network::NetworkHealthPolicy::default(),
    )
    .unwrap();
    let effects = Arc::new(AtomicUsize::new(0));
    let server_effects = effects.clone();
    let expected_local = local.clone();
    let target = remote.clone();
    let server = tokio::spawn(async move {
        for phase in 0..if case == ColdReadCase::Success { 2 } else { 1 } {
            let mut stream = listener.accept().await.unwrap().into_inner();
            let metadata = sinomo_network::read_authenticated_transport_prelude(&mut stream)
                .await
                .unwrap();
            let source = sinomo_network::verify_builtin_authenticated_transport_metadata(
                &metadata,
                &target,
                &server_directory,
                identity.as_ref(),
            )
            .unwrap();
            assert_eq!(source, expected_local);
            let direct = sinomo_network::AuthenticatedDirectStream::accepted(
                Box::new(stream),
                target.clone(),
                source,
                sinomo_base::Id::generate(),
                metadata.channel_binding,
                metadata.capabilities,
            )
            .unwrap();
            let carrier = match sinomo_network::accept_or_relay_routed_session(
                direct,
                &server_directory,
                identity.as_ref(),
                &provider,
                None,
            )
            .await
            .unwrap()
            {
                sinomo_network::RoutedSessionDisposition::Destination(stream) => stream,
                _ => panic!("target cannot relay this call"),
            };
            let mut acceptor = sinomo_network::accept_routed_stream_carrier(carrier);
            let stream = acceptor.accept().await.unwrap().unwrap();
            assert_eq!(stream.peer_node_id, expected_local);
            let peer = sinomo_network::RpcPeerContext::authenticated(
                stream.local_node_id.clone(),
                stream.peer_node_id.clone(),
                stream.session_id.clone(),
                stream.channel_binding,
            )
            .unwrap();
            let effects = server_effects.clone();
            let reads = server_reads.clone();
            let paths = server_paths.clone();
            let replacement = replacement.clone();
            let served = sinomo_network::serve_service_rpc_peer_stream(
                stream,
                &peer,
                sinomo_network::ObjectFrameIoOptions::default(),
                move |mut request, _| async move {
                    let route = request.request.route.clone();
                    if phase == 0 {
                        assert_eq!(route, sinomo_network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE);
                        let _: () = request.decode_body().await?;
                        assert_eq!(effects.load(Ordering::SeqCst), 0);
                        reads.fetch_add(1, Ordering::SeqCst);
                        if case == ColdReadCase::ObservedCutChanged {
                            // Inject a newly observed committed cut during the
                            // authenticated read. The original effect must wait.
                            crate::write_cluster_kv_snapshot(&paths, &replacement).unwrap();
                        }
                        if case == ColdReadCase::Deadline {
                            std::future::pending::<()>().await;
                        }
                        sinomo_network::ObjectRpcOutgoingResponse::encode_value(
                            route,
                            Vec::<ClusterConnectivityRecord>::new(),
                        )
                    } else {
                        assert_eq!(route, "/test/original-cold-operation");
                        assert_eq!(
                            request
                                .request
                                .metadata
                                .get("operation-id")
                                .map(String::as_str),
                            Some("original-cold-id")
                        );
                        let operation: OriginalOperation = request.decode_body().await?;
                        assert_eq!(operation.operation_id, "original-cold-id");
                        assert_eq!(operation.bytes, vec![0x6b; 65579]);
                        assert_eq!(effects.fetch_add(1, Ordering::SeqCst), 0);
                        sinomo_network::ObjectRpcOutgoingResponse::encode_value(
                            route,
                            "original response".to_string(),
                        )
                    }
                },
            )
            .await;
            if case != ColdReadCase::Deadline {
                served.unwrap();
            }
        }
    });
    let mut host = ReadOnlyHost;
    let mut client = ClusterObjectRpcClient::routed_from_fenced_bootstrap_state_with_connectivity(
        &mut host, &paths, &state, observed,
    )
    .unwrap();
    let mut call = ObjectRpcCall::encode_value(
        "/test/original-cold-operation",
        "test",
        OriginalOperation {
            operation_id: "original-cold-id".into(),
            bytes: vec![0x6b_u8; 65579],
        },
    )
    .unwrap();
    call.request
        .metadata
        .insert("operation-id".into(), "original-cold-id".into());
    let result = client
        .call_target(
            &state,
            &remote,
            call,
            ClusterObjectRpcOptions::default()
                .with_overall_timeout(std::time::Duration::from_secs(3)),
        )
        .await;
    match case {
        ColdReadCase::Success => {
            assert_eq!(
                result.unwrap().decode_body::<String>().await.unwrap(),
                "original response"
            );
            assert_eq!(effects.load(Ordering::SeqCst), 1);
        }
        ColdReadCase::ObservedCutChanged => {
            assert!(
                matches!(result, Err(NetworkError::Directory { .. })),
                "changed cut result: {result:?}"
            );
            assert_eq!(effects.load(Ordering::SeqCst), 0);
        }
        ColdReadCase::Deadline => {
            assert!(
                matches!(
                    result,
                    Err(NetworkError::Timeout {
                        delivery_state: sinomo_network::NetworkDeliveryState::DefinitelyNotSent,
                        ..
                    })
                ),
                "pre-dispatch timeout: {result:?}"
            );
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            server.abort();
        }
    }
    let joined = tokio::time::timeout(std::time::Duration::from_secs(3), server)
        .await
        .unwrap();
    if case != ColdReadCase::Deadline {
        joined.unwrap();
    }
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    if case != ColdReadCase::ObservedCutChanged {
        assert_eq!(std::fs::read(&paths.kv_paths().snapshot).unwrap(), before);
    }
    assert!(!paths.connectivity_local.exists());
    assert!(!sinomo_kv::evict_resident_raft_node_for_test(
        &paths.kv_paths().raft_wal_paths(&local)
    ));
    assert!(sinomo_kv::process_local_raft_access_is_denied(
        &paths.kv_paths().raft
    ));
    assert!(sinomo_kv::clear_process_local_raft_access_denial_for_test(
        &paths.kv_paths().raft
    ));
    for scope in
        sinomo_network::cluster_routed_stream_pool_scopes_from_fenced_bootstrap_state(&state)
            .unwrap()
    {
        sinomo_network::retire_pooled_routed_streams(&scope).await;
    }
    let mut digest = blake3::Hasher::new();
    for value in [
        endpoint.endpoint_id.as_str(),
        endpoint.address.as_str(),
        remote_key.public_key.as_str(),
    ] {
        digest.update(&(value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    digest.update(format!("{:?}", endpoint.transport).as_bytes());
    sinomo_network::retire_pooled_routed_streams(&format!(
        "{}:direct-endpoint:{}",
        cluster,
        digest.finalize().to_hex()
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_shared_expiry_routes_without_main_or_raft_and_preserves_original_body() {
    cold_source_scenario(ColdReadCase::Success).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_source_read_rejects_an_observed_semantic_change_before_original_dispatch() {
    cold_source_scenario(ColdReadCase::ObservedCutChanged).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_source_read_deadline_is_definitely_not_sent_and_cancels_owned_reads() {
    cold_source_scenario(ColdReadCase::Deadline).await;
}
