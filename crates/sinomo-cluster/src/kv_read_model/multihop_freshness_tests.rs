//! Real authenticated TCP carriers with an enforced A -> B -> C -> D adjacency.
//! The served connectivity payload is controlled; no Main or local Raft exists.
use super::*;
use crate::keyspace::{cluster_connectivity_key, cluster_endpoint_key, cluster_member_key};
use crate::kv_local_store::ClusterKvPathsExt;
use sinomo_kv::InProcessKvStore;
use sinomo_network::{
    ClusterDirectory as _, ClusterObjectRpcAsyncHost, ClusterObjectRpcClient,
    ClusterObjectRpcOptions, ObjectRpcCall,
};
use std::sync::atomic::{AtomicUsize, Ordering};

struct ReadOnlyHost;
impl ClusterObjectRpcAsyncHost for ReadOnlyHost {}
#[derive(serde::Serialize, serde::Deserialize)]
struct OriginalOperation {
    id: String,
    bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceCase {
    Success,
    SemanticChange,
    Deadline,
}

async fn multihop_source_scenario(case: SourceCase) {
    let _registry = TEST_NETWORK_MODULE_REGISTRY_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let root = sinomo_testdir::TestDir::new("cold-live-two-bridges");
    let cluster = sinomo_node::ClusterId::from_legacy_named("cold-live-two-bridges").unwrap();
    let nodes = (0..4)
        .map(|i| NodeId::from_legacy_named(format!("cold-live-node-{i}")).unwrap())
        .collect::<Vec<_>>();
    let mut data = Vec::new();
    let mut keys = Vec::new();
    let mut listeners = Vec::new();
    let mut endpoints = Vec::new();
    for i in 0..4 {
        let dir = sinomo_local::SinomoDataDir::new(root.join(format!("node-{i}")));
        dir.ensure_root_layout().unwrap();
        keys.push(sinomo_node::load_or_create_main_node_identity(&dir.root()).unwrap());
        if i > 0 {
            let listener = sinomo_network::AsyncDaemonControlListener::bind("127.0.0.1:0")
                .await
                .unwrap();
            endpoints.push(sinomo_network::daemon_lan_endpoint(
                &nodes[i],
                listener.local_addr_string().unwrap(),
                0,
            ));
            listeners.push(listener);
        }
        data.push(dir);
    }
    let now = OffsetDateTime::now_utc();
    let mut state = sinomo_base::ClusterBootstrapState::initialized_locally(
        cluster.clone(),
        "cold bridges",
        nodes[0].clone(),
        "caller",
        keys[0].public_key.clone(),
        now,
    );
    for i in 1..4 {
        let mut member = ClusterMemberRecord::local_non_voting(
            cluster.clone(),
            nodes[i].clone(),
            format!("node-{i}"),
            keys[i].public_key.clone(),
            now,
        );
        member.endpoints.push(endpoints[i - 1].clone());
        state.members.push(member);
    }
    // Relays sign their own adjacent transport. Each real signer needs its
    // matching bootstrap identity, just as the caller does; a key alone is
    // insufficient for a legacy-named fixture node.
    for (i, dir) in data.iter().enumerate().skip(1) {
        let paths = ClusterPaths::new(dir);
        paths.ensure().unwrap();
        let mut own = state.clone();
        own.local_node_id = nodes[i].clone();
        crate::write_cluster_state(&paths, &own).unwrap();
    }
    let paths = ClusterPaths::new(&data[0]);
    paths.ensure().unwrap();
    crate::write_cluster_state(&paths, &state).unwrap();
    let mut live = Vec::new();
    let mut kv = InProcessKvStore::new();
    for member in &state.members {
        kv.put_serde(cluster_member_key(&member.node_id), member, None)
            .unwrap();
    }
    for (i, endpoint) in endpoints.iter().enumerate() {
        kv.put_serde(
            cluster_endpoint_key(&endpoint.node_id, endpoint),
            endpoint,
            None,
        )
        .unwrap();
        let mut source = state.clone();
        source.local_node_id = nodes[i].clone();
        let observation = sinomo_network::cluster_connectivity_record(
            &source,
            endpoint,
            sinomo_network::ClusterConnectivityState::Healthy,
            None,
            0,
            None,
            None,
            now,
            900,
        );
        let mut expired = observation.clone();
        expired.observed_at -= time::Duration::hours(1);
        expired.expires_at -= time::Duration::hours(1);
        kv.put_serde(
            cluster_connectivity_key(
                &expired.source_node_id,
                &expired.target_node_id,
                &expired.endpoint_id,
            ),
            &expired,
            None,
        )
        .unwrap();
        live.push(observation);
    }
    crate::write_cluster_kv_snapshot(&paths, &kv.snapshot().unwrap()).unwrap();
    let original_snapshot = std::fs::read(&paths.kv_paths().snapshot).unwrap();
    let mut changed = live[2].clone();
    changed.state = sinomo_network::ClusterConnectivityState::Unreachable;
    changed.consecutive_failures = 3;
    kv.put_serde(
        cluster_connectivity_key(
            &changed.source_node_id,
            &changed.target_node_id,
            &changed.endpoint_id,
        ),
        &changed,
        None,
    )
    .unwrap();
    let replacement = kv.snapshot().unwrap();

    sinomo_kv::deny_process_local_raft_access_for_root(paths.kv_paths().raft.clone()).unwrap();
    let observed = Arc::new(KvClusterDirectory::new(paths.clone()));
    assert!(observed.connectivity_records().unwrap().is_empty());
    let baseline = observed.connectivity_freshness_baseline().unwrap();
    assert_eq!(baseline.records.len(), 3);
    for record in &live {
        assert!(
            baseline
                .records
                .iter()
                .any(|old| sinomo_network::cluster_connectivity_stable_eq(old, record)),
            "missing baseline: {record:?}"
        );
    }
    let directory = sinomo_network::DirectorySnapshot::try_from_records(
        sinomo_network::DirectoryVersion(1),
        state.members.clone(),
        endpoints.clone(),
    )
    .unwrap();
    let live_snapshot =
        sinomo_network::connectivity_snapshot_from_directory_records(&directory, live.clone(), now)
            .unwrap();
    assert!(
        !sinomo_network::plan_routes(
            &directory,
            &live_snapshot,
            &nodes[0],
            &nodes[3],
            Default::default()
        )
        .is_empty(),
        "controlled live graph has an executable two-bridge route"
    );
    let reads = Arc::new((0..4).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let effects = Arc::new(AtomicUsize::new(0));
    let rejected_direct = Arc::new(AtomicUsize::new(0));
    let mut servers = tokio::task::JoinSet::new();
    for (offset, listener) in listeners.into_iter().enumerate() {
        let i = offset + 1;
        let node = nodes[i].clone();
        let adjacent = nodes[i - 1].clone();
        let caller = nodes[0].clone();
        let directory = directory.clone();
        let replies = live.clone();
        let server_paths = paths.clone();
        let replacement = replacement.clone();

        let reads = reads.clone();
        let effects = effects.clone();
        let rejected_direct = rejected_direct.clone();
        let identity = Arc::new(
            sinomo_network::DaemonRoutedSessionIdentity::new(node.clone(), data[i].clone())
                .unwrap(),
        );
        let provider = Arc::new(
            sinomo_network::ClusterLinkProvider::new(
                node.clone(),
                identity.clone(),
                sinomo_network::NetworkHealthPolicy::default(),
            )
            .unwrap(),
        );
        servers.spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let mut stream = listener.accept().await.unwrap().into_inner();
                let node = node.clone();
                let adjacent = adjacent.clone();
                let caller = caller.clone();
                let directory = directory.clone();
                let identity = identity.clone();
                let provider = provider.clone();
                let replies = replies.clone();
                let server_paths = server_paths.clone();
                let replacement = replacement.clone();

                let reads = reads.clone();
                let effects = effects.clone();
                let rejected_direct = rejected_direct.clone();
                connections.spawn(async move {
                    let metadata =
                        sinomo_network::read_authenticated_transport_prelude(&mut stream).await?;
                    let source = sinomo_network::verify_builtin_authenticated_transport_metadata(
                        &metadata,
                        &node,
                        &directory,
                        identity.as_ref(),
                    )?;
                    // Real authentication precedes adjacency rejection. C and D cannot
                    // accept the caller's direct link, only their previous bridge.
                    if source != adjacent {
                        rejected_direct.fetch_add(1, Ordering::SeqCst);
                        return Ok::<(), sinomo_network::NetworkError>(());
                    }
                    let direct = sinomo_network::AuthenticatedDirectStream::accepted(
                        Box::new(stream),
                        node.clone(),
                        source,
                        sinomo_base::Id::generate(),
                        metadata.channel_binding,
                        metadata.capabilities,
                    )?;
                    let disposition = sinomo_network::accept_or_relay_routed_session(
                        direct,
                        &directory,
                        identity.as_ref(),
                        provider.as_ref(),
                        None,
                    )
                    .await
                    .map_err(|error| {
                        eprintln!("fixture relay/accept node={node} error={error}");
                        error
                    })?;
                    if let sinomo_network::RoutedSessionDisposition::Destination(carrier) =
                        disposition
                    {
                        let mut acceptor = sinomo_network::accept_routed_stream_carrier(carrier);
                        while let Some(stream) = acceptor.accept().await? {
                            assert_eq!(stream.peer_node_id, caller);
                            let peer = sinomo_network::RpcPeerContext::authenticated(
                                stream.local_node_id.clone(),
                                stream.peer_node_id.clone(),
                                stream.session_id.clone(),
                                stream.channel_binding,
                            )?;
                            let replies = replies.clone();
                            let server_paths = server_paths.clone();
                            let replacement = replacement.clone();

                            let reads = reads.clone();
                            let effects = effects.clone();
                            sinomo_network::serve_service_rpc_peer_stream(
                                stream,
                                &peer,
                                sinomo_network::ObjectFrameIoOptions::default(),
                                move |mut request, _| {
                                    let replies = replies.clone();
                                    let server_paths = server_paths.clone();
                                    let replacement = replacement.clone();

                                    let reads = reads.clone();
                                    let effects = effects.clone();
                                    async move {
                                        let route = request.request.route.clone();
                                        if route
                                            == sinomo_network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE
                                        {
                                            let _: () = request.decode_body().await?;
                                            reads[i].fetch_add(1, Ordering::SeqCst);
                                            if i == 2 && case == SourceCase::SemanticChange {
                                                crate::write_cluster_kv_snapshot(
                                                    &server_paths,
                                                    &replacement,
                                                )
                                                .unwrap();
                                            }
                                            if i == 2 && case == SourceCase::Deadline {
                                                std::future::pending::<()>().await;
                                            }

                                            // Deliberately include third-party rows. Only source i is trusted.
                                            sinomo_network::ObjectRpcOutgoingResponse::encode_value(
                                                route, replies,
                                            )
                                        } else {
                                            assert_eq!(i, 3);
                                            assert_eq!(route, "/test/cold-multihop-original");
                                            assert_eq!(
                                                request
                                                    .request
                                                    .metadata
                                                    .get("operation-id")
                                                    .map(String::as_str),
                                                Some("original-multihop")
                                            );
                                            let body: OriginalOperation =
                                                request.decode_body().await?;
                                            assert_eq!(body.id, "original-multihop");
                                            assert_eq!(body.bytes, vec![0x71; 65579]);
                                            assert_eq!(effects.fetch_add(1, Ordering::SeqCst), 0);
                                            sinomo_network::ObjectRpcOutgoingResponse::encode_value(
                                                route,
                                                "original response".to_string(),
                                            )
                                        }
                                    }
                                },
                            )
                            .await?;
                        }
                    }
                    Ok::<(), sinomo_network::NetworkError>(())
                });
                while connections.try_join_next().is_some() {}
            }
        });
    }
    let mut host = ReadOnlyHost;
    let mut client = ClusterObjectRpcClient::routed_from_fenced_bootstrap_state_with_connectivity(
        &mut host, &paths, &state, observed,
    )
    .unwrap();
    let mut call = ObjectRpcCall::encode_value(
        "/test/cold-multihop-original",
        "test",
        OriginalOperation {
            id: "original-multihop".into(),
            bytes: vec![0x71; 65579],
        },
    )
    .unwrap();
    call.request
        .metadata
        .insert("operation-id".into(), "original-multihop".into());
    let result = client
        .call_target(
            &state,
            &nodes[3],
            call,
            ClusterObjectRpcOptions::default()
                .with_overall_timeout(std::time::Duration::from_secs(5)),
        )
        .await;
    let response = match result {
        Ok(response) => response.decode_body::<String>().await,
        Err(error) => Err(error),
    };
    // Cleanup also runs on the expected failed-before result.
    for scope in
        sinomo_network::cluster_routed_stream_pool_scopes_from_fenced_bootstrap_state(&state)
            .unwrap()
    {
        sinomo_network::retire_pooled_routed_streams(&scope).await;
    }
    for (i, endpoint) in endpoints.iter().enumerate() {
        let mut hash = blake3::Hasher::new();
        for value in [
            endpoint.endpoint_id.as_str(),
            endpoint.address.as_str(),
            keys[i + 1].public_key.as_str(),
        ] {
            hash.update(&(value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        hash.update(format!("{:?}", endpoint.transport).as_bytes());
        sinomo_network::retire_pooled_routed_streams(&format!(
            "{}:direct-endpoint:{}",
            cluster,
            hash.finalize().to_hex()
        ))
        .await;
    }
    servers.abort_all();
    while servers.join_next().await.is_some() {}
    if case != SourceCase::SemanticChange {
        assert_eq!(
            std::fs::read(&paths.kv_paths().snapshot).unwrap(),
            original_snapshot
        );
    }
    assert!(!paths.connectivity_local.exists());
    assert!(!sinomo_kv::evict_resident_raft_node_for_test(
        &paths.kv_paths().raft_wal_paths(&nodes[0])
    ));
    assert!(sinomo_kv::clear_process_local_raft_access_denial_for_test(
        &paths.kv_paths().raft
    ));
    eprintln!(
        "source_reads={:?} original_effects={} blocked_direct={}",
        reads
            .iter()
            .map(|n| n.load(Ordering::SeqCst))
            .collect::<Vec<_>>(),
        effects.load(Ordering::SeqCst),
        rejected_direct.load(Ordering::SeqCst)
    );
    match case {
        SourceCase::Success => {
            assert_eq!(response.unwrap(), "original response");
            assert_eq!(effects.load(Ordering::SeqCst), 1);
        }
        SourceCase::SemanticChange => {
            assert!(
                matches!(
                    response,
                    Err(sinomo_network::NetworkError::Directory { .. })
                ),
                "{response:?}"
            );
            assert_eq!(effects.load(Ordering::SeqCst), 0);
        }
        SourceCase::Deadline => {
            assert!(
                matches!(
                    response,
                    Err(sinomo_network::NetworkError::Timeout {
                        delivery_state: sinomo_network::NetworkDeliveryState::DefinitelyNotSent,
                        ..
                    })
                ),
                "{response:?}"
            );
            assert_eq!(effects.load(Ordering::SeqCst), 0);
        }
    }
    assert_eq!(
        reads[2].load(Ordering::SeqCst),
        1,
        "C's own authenticated reply is required; B cannot renew C's edge"
    );
    assert!(rejected_direct.load(Ordering::SeqCst) > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_source_reads_cross_two_bridges_without_trusting_third_party_rows() {
    multihop_source_scenario(SourceCase::Success).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multihop_source_semantic_change_prevents_original_dispatch() {
    multihop_source_scenario(SourceCase::SemanticChange).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multihop_source_deadline_preserves_original_delivery_and_drains() {
    multihop_source_scenario(SourceCase::Deadline).await;
}
