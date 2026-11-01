//! Routing from committed KV connectivity over real authenticated TCP carriers
//! with an enforced A -> B -> C -> D adjacency. The caller has no Main daemon
//! or local Raft and reads only the committed directory cut. No node serves a
//! connectivity read, so every route comes from committed KV rows.
use super::*;
use crate::keyspace::{cluster_connectivity_key, cluster_endpoint_key, cluster_member_key};
use crate::kv_local_store::ClusterKvPathsExt;
use sinomo_base::ClusterBootstrapState;
use sinomo_kv::InProcessKvStore;
use sinomo_network::{
    ClusterConnectivityState, ClusterDirectory as _, ClusterObjectRpcAsyncHost,
    ClusterObjectRpcClient, ClusterObjectRpcOptions, ObjectRpcCall,
};
use std::sync::atomic::{AtomicUsize, Ordering};

const ORIGINAL_ROUTE: &str = "/test/committed-route-original";

struct ReadOnlyHost;
impl ClusterObjectRpcAsyncHost for ReadOnlyHost {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BridgeCommit {
    /// B -> C was committed healthy long ago and never renewed in KV.
    StableHealthy,
    /// B -> C reaches KV only after the caller's first attempt.
    CommittedLater,
    /// B committed that its edge to C is unreachable.
    Unreachable,
}

fn committed_edge(
    state: &ClusterBootstrapState,
    source: &NodeId,
    endpoint: &ClusterEndpointRecord,
    connectivity: ClusterConnectivityState,
    observed_at: OffsetDateTime,
) -> ClusterConnectivityRecord {
    let mut source_state = state.clone();
    source_state.local_node_id = source.clone();
    let (failures, ttl) = match connectivity {
        ClusterConnectivityState::Unreachable => (
            3,
            sinomo_network::CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
        ),
        _ => (0, sinomo_network::CLUSTER_CONNECTIVITY_TTL_SECONDS),
    };
    sinomo_network::cluster_connectivity_record(
        &source_state,
        endpoint,
        connectivity,
        Some(1),
        failures,
        None,
        None,
        observed_at,
        ttl,
    )
}

fn put_edge(kv: &mut InProcessKvStore, record: &ClusterConnectivityRecord) {
    kv.put_serde(
        cluster_connectivity_key(
            &record.source_node_id,
            &record.target_node_id,
            &record.endpoint_id,
        ),
        record,
        None,
    )
    .unwrap();
}

// The guard serializes the process-global module registry for the whole
// routed call, as in the other routed fixtures of this module.
#[allow(clippy::await_holding_lock)]
async fn committed_chain_scenario(bridge: BridgeCommit) {
    let _registry = TEST_NETWORK_MODULE_REGISTRY_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let root = sinomo_testdir::TestDir::new(&format!("committed-route-chain-{bridge:?}"));
    let cluster =
        sinomo_node::ClusterId::from_legacy_named(format!("committed-route-{bridge:?}")).unwrap();
    let nodes = (0..4)
        .map(|i| NodeId::from_legacy_named(format!("committed-route-node-{i}")).unwrap())
        .collect::<Vec<_>>();
    let mut data = Vec::new();
    let mut keys = Vec::new();
    let mut listeners = Vec::new();
    let mut endpoints = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        let dir = sinomo_local::SinomoDataDir::new(root.join(format!("node-{i}")));
        dir.ensure_root_layout().unwrap();
        keys.push(sinomo_node::load_or_create_main_node_identity(&dir.root()).unwrap());
        if i > 0 {
            let listener = sinomo_network::AsyncDaemonControlListener::bind("127.0.0.1:0")
                .await
                .unwrap();
            endpoints.push(sinomo_network::daemon_lan_endpoint(
                node,
                listener.local_addr_string().unwrap(),
                0,
            ));
            listeners.push((i, listener));
        }
        data.push(dir);
    }
    let now = OffsetDateTime::now_utc();
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster.clone(),
        "committed route",
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
    // Relays sign their own adjacent links with their own bootstrap identity.
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

    // Every row was committed one hour ago by its source and never renewed:
    // quorum publication is semantic, so a stable source writes nothing.
    let committed_at = now - time::Duration::hours(1);
    let mut kv = InProcessKvStore::new();
    for member in &state.members {
        kv.put_serde(cluster_member_key(&member.node_id), member, None)
            .unwrap();
    }
    for endpoint in &endpoints {
        kv.put_serde(
            cluster_endpoint_key(&endpoint.node_id, endpoint),
            endpoint,
            None,
        )
        .unwrap();
    }
    put_edge(
        &mut kv,
        &committed_edge(
            &state,
            &nodes[0],
            &endpoints[0],
            ClusterConnectivityState::Healthy,
            committed_at,
        ),
    );
    match bridge {
        BridgeCommit::StableHealthy => put_edge(
            &mut kv,
            &committed_edge(
                &state,
                &nodes[1],
                &endpoints[1],
                ClusterConnectivityState::Healthy,
                committed_at,
            ),
        ),
        BridgeCommit::CommittedLater => {}
        BridgeCommit::Unreachable => put_edge(
            &mut kv,
            &committed_edge(
                &state,
                &nodes[1],
                &endpoints[1],
                ClusterConnectivityState::Unreachable,
                now - time::Duration::hours(3),
            ),
        ),
    }
    put_edge(
        &mut kv,
        &committed_edge(
            &state,
            &nodes[2],
            &endpoints[2],
            ClusterConnectivityState::Healthy,
            committed_at,
        ),
    );
    crate::write_cluster_kv_snapshot(&paths, &kv.snapshot().unwrap()).unwrap();
    sinomo_kv::deny_process_local_raft_access_for_root(paths.kv_paths().raft.clone()).unwrap();

    let observed = Arc::new(KvClusterDirectory::new(paths.clone()));
    let committed_rows = if bridge == BridgeCommit::CommittedLater {
        2
    } else {
        3
    };
    assert!(
        observed.connectivity_records().unwrap().is_empty(),
        "every committed row is past its recorded lifetime"
    );
    let (_, _, _, routing_rows) = observed.routing_records_snapshot().unwrap().into_parts();
    assert_eq!(routing_rows.len(), committed_rows);
    assert!(
        routing_rows
            .iter()
            .all(|row| sinomo_network::cluster_network_record_live(row.expires_at, now)),
        "routing reads committed rows as the sources' current statements"
    );

    let directory = sinomo_network::DirectorySnapshot::try_from_records(
        sinomo_network::DirectoryVersion(1),
        state.members.clone(),
        endpoints.clone(),
    )
    .unwrap();
    let effects = Arc::new(AtomicUsize::new(0));
    // Any connection from a non-adjacent node, or any call terminating at a
    // bridge, would be a discovery path outside the committed graph.
    let misdirected = Arc::new(AtomicUsize::new(0));
    let mut servers = tokio::task::JoinSet::new();
    for (i, listener) in listeners {
        let node = nodes[i].clone();
        let adjacent = nodes[i - 1].clone();
        let caller = nodes[0].clone();
        let directory = directory.clone();
        let effects = effects.clone();
        let misdirected = misdirected.clone();
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
                let effects = effects.clone();
                let misdirected = misdirected.clone();
                connections.spawn(async move {
                    let metadata =
                        sinomo_network::read_authenticated_transport_prelude(&mut stream).await?;
                    let source = sinomo_network::verify_builtin_authenticated_transport_metadata(
                        &metadata,
                        &node,
                        &directory,
                        identity.as_ref(),
                    )?;
                    if source != adjacent {
                        misdirected.fetch_add(1, Ordering::SeqCst);
                        return Ok::<(), NetworkError>(());
                    }
                    let direct = sinomo_network::AuthenticatedDirectStream::accepted(
                        Box::new(stream),
                        node.clone(),
                        source,
                        sinomo_base::Id::generate(),
                        metadata.channel_binding,
                        metadata.capabilities,
                    )?;
                    let sinomo_network::RoutedSessionDisposition::Destination(carrier) =
                        sinomo_network::accept_or_relay_routed_session(
                            direct,
                            &directory,
                            identity.as_ref(),
                            provider.as_ref(),
                            None,
                        )
                        .await?
                    else {
                        return Ok(());
                    };
                    if i != 3 {
                        misdirected.fetch_add(1, Ordering::SeqCst);
                        return Ok(());
                    }
                    let mut acceptor = sinomo_network::accept_routed_stream_carrier(carrier);
                    while let Some(stream) = acceptor.accept().await? {
                        assert_eq!(stream.peer_node_id, caller);
                        let peer = sinomo_network::RpcPeerContext::authenticated(
                            stream.local_node_id.clone(),
                            stream.peer_node_id.clone(),
                            stream.session_id.clone(),
                            stream.channel_binding,
                        )?;
                        let effects = effects.clone();
                        let misdirected = misdirected.clone();
                        sinomo_network::serve_service_rpc_peer_stream(
                            stream,
                            &peer,
                            sinomo_network::ObjectFrameIoOptions::default(),
                            move |request, _| {
                                let effects = effects.clone();
                                let misdirected = misdirected.clone();
                                async move {
                                    let route = request.request.route.clone();
                                    if route != ORIGINAL_ROUTE {
                                        misdirected.fetch_add(1, Ordering::SeqCst);
                                        return Err(NetworkError::Io {
                                            operation: "committed route fixture".into(),
                                            detail: format!("unexpected route {route}"),
                                        });
                                    }
                                    let body: Vec<u8> = request.decode_body().await?;
                                    assert_eq!(body, vec![0x5c; 4096]);
                                    effects.fetch_add(1, Ordering::SeqCst);
                                    sinomo_network::ObjectRpcOutgoingResponse::encode_value(
                                        route,
                                        "original response".to_string(),
                                    )
                                }
                            },
                        )
                        .await?;
                    }
                    Ok(())
                });
            }
        });
    }

    let call_original = |observed: Arc<KvClusterDirectory>| {
        let paths = paths.clone();
        let state = state.clone();
        let target = nodes[3].clone();
        async move {
            let mut host = ReadOnlyHost;
            let mut client =
                ClusterObjectRpcClient::routed_from_fenced_bootstrap_state_with_connectivity(
                    &mut host, &paths, &state, observed,
                )
                .unwrap();
            let call =
                ObjectRpcCall::encode_value(ORIGINAL_ROUTE, "test", vec![0x5c_u8; 4096]).unwrap();
            match client
                .call_target(
                    &state,
                    &target,
                    call,
                    ClusterObjectRpcOptions::default()
                        .with_overall_timeout(std::time::Duration::from_secs(5)),
                )
                .await
            {
                Ok(response) => response.decode_body::<String>().await,
                Err(error) => Err(error),
            }
        }
    };
    let no_executable_path = |result: &Result<String, NetworkError>| {
        matches!(
            result,
            Err(NetworkError::RetryExhausted {
                attempts: 0,
                delivery_state: sinomo_network::NetworkDeliveryState::DefinitelyNotSent,
                detail,
                ..
            }) if detail == "no executable path in the current snapshots"
        )
    };

    let first = call_original(observed.clone()).await;
    match bridge {
        BridgeCommit::StableHealthy => {
            assert_eq!(first.unwrap(), "original response");
            assert_eq!(effects.load(Ordering::SeqCst), 1);
        }
        BridgeCommit::Unreachable => {
            assert!(
                no_executable_path(&first),
                "committed failure result: {first:?}"
            );
            assert_eq!(effects.load(Ordering::SeqCst), 0);
        }
        BridgeCommit::CommittedLater => {
            assert!(
                no_executable_path(&first),
                "missing bridge result: {first:?}"
            );
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            // B's commit arrives at the caller's committed cut. Its observation
            // time already lies a full TTL in the past.
            put_edge(
                &mut kv,
                &committed_edge(
                    &state,
                    &nodes[1],
                    &endpoints[1],
                    ClusterConnectivityState::Healthy,
                    committed_at,
                ),
            );
            crate::write_cluster_kv_snapshot(&paths, &kv.snapshot().unwrap()).unwrap();
            // A prepared fenced connector is reused for at most one second.
            tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
            let second = call_original(observed.clone()).await;
            assert_eq!(second.unwrap(), "original response");
            assert_eq!(effects.load(Ordering::SeqCst), 1);
        }
    }
    assert_eq!(misdirected.load(Ordering::SeqCst), 0);
    assert!(!paths.connectivity_local.exists());

    servers.abort_all();
    for scope in
        sinomo_network::cluster_routed_stream_pool_scopes_from_fenced_bootstrap_state(&state)
            .unwrap()
    {
        sinomo_network::retire_pooled_routed_streams(&scope).await;
    }
    assert!(sinomo_kv::clear_process_local_raft_access_denial_for_test(
        &paths.kv_paths().raft
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stable_committed_chain_routes_through_two_bridges_after_its_ttl() {
    committed_chain_scenario(BridgeCommit::StableHealthy).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn newly_committed_bridge_is_used_without_source_reads() {
    committed_chain_scenario(BridgeCommit::CommittedLater).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_unreachable_bridge_prevents_original_dispatch() {
    committed_chain_scenario(BridgeCommit::Unreachable).await;
}
