//! Real carrier/encoded-RPC control; provider responses come from native owners.
use super::*;
use sinomo_api::{
    DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE, DAEMON_STATUS_ROUTE, DaemonClusterStatus,
    DaemonObjectRpcForwardRequest, DaemonObjectRpcForwardResponse, DaemonPaths, DaemonRunOptions,
    DaemonStatus,
};
use sinomo_core::network::{
    AsyncDaemonControlListener, ObjectFrameIoOptions, ObjectRpcOutgoingResponse, RpcPeerContext,
    accept_local_object_rpc_stream_carrier, read_local_object_rpc_carrier_prelude,
    serve_service_rpc_peer_stream,
};
use sinomo_dfs::test_support::DfsTestRuntime;

#[derive(Clone, Copy, PartialEq)]
enum Scenario {
    ResolvedRemoteFirst,
    LostMutationReply,
}

#[test]
fn discovered_provider_follows_daemon_resolution_without_probes() {
    native_provider_control(Scenario::ResolvedRemoteFirst);
}

#[test]
fn discovered_provider_lost_mutation_reply_never_replays_to_another_provider() {
    native_provider_control(Scenario::LostMutationReply);
}

// The fixture's catalog comes from the same initialized native owner used by
// its components. A direct on-disk local read can contend with that owner's
// resident Raft store and is not a second catalog source for this proxy.
fn native_catalog_snapshot(
    native: &DfsTestRuntime,
    node: &NodeId,
    prefix: &str,
) -> sinomo_core::kv::KvLocalRead<Vec<sinomo_core::kv::KvEntry>> {
    let cluster = native
        .component_context(node)
        .unwrap()
        .cluster_access()
        .unwrap();
    let read = native
        .runtime_handle()
        .block_on(cluster.kv_range_prefix_authoritative_async(prefix.to_string()))
        .unwrap();
    // Preserve the actual owner's entries and proof coordinates in the existing
    // observational local-range wire shape. This proxy models that read view;
    // it does not establish production daemon local-catalog freshness.
    sinomo_core::kv::KvLocalRead {
        value: read.value,
        revision: read.authority.revision,
        observed_term: read.authority.term,
        applied_index: read.authority.commit_index,
        known_leader: None,
    }
}

fn native_provider_control(scenario: Scenario) {
    let name = if scenario == Scenario::ResolvedRemoteFirst {
        "vfs-provider-resolution-native"
    } else {
        "vfs-provider-reply-loss-native"
    };
    let (native, nodes) = DfsTestRuntime::start_cluster(name, 3).unwrap();
    let source = nodes[2].clone(); // The connected daemon's node.
    let data = native.data_dir(&source).clone();
    let paths = ClusterPaths::new(&data);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths).unwrap();
    let catalog_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let remote = loop {
        let read = native_catalog_snapshot(
            &native,
            &source,
            sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX,
        );
        let instances = sinomo_core::service::service_instances_from_entries(&read.value).unwrap();
        let providers = sinomo_core::service::service_instances_providing_capability(
            instances,
            sinomo_dfs::SIFS_PATH_ACCESS_CAPABILITY,
        );
        if providers.len() >= 2
            && providers.iter().any(|provider| provider.node_id == source)
            && let Some(remote) = providers.iter().find(|provider| provider.node_id != source)
        {
            break remote.node_id.clone();
        }
        assert!(
            Instant::now() < catalog_deadline,
            "normal native catalog must advertise both actual providers before fault injection"
        );
        std::thread::yield_now();
    };
    // The daemon owns provider selection; this proxy returns its resolution.
    let resolved = match scenario {
        Scenario::ResolvedRemoteFirst => vec![remote.clone(), source.clone()],
        Scenario::LostMutationReply => vec![source.clone(), remote.clone()],
    };

    let member = state
        .members
        .iter()
        .find(|member| member.node_id == source)
        .unwrap();
    let status = DaemonStatus::new(
        &DaemonRunOptions::default(),
        &DaemonPaths::new(&data),
        Some(DaemonClusterStatus {
            cluster_id: state.cluster_id.clone(),
            node_id: source.clone(),
            process: "main".into(),
            member_role: member.role.clone(),
            member_state: member.state.clone(),
        }),
    );
    let native = Arc::new(Mutex::new(native));
    let seen_operations = Arc::new(Mutex::new(Vec::new()));
    let native_receipt = Arc::new(Mutex::new(None::<sinomo_api::DaemonDfsBrowserStatResponse>));
    let file_path = "/services/provider-mutation-reply-loss.md";
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (result, operation_nodes) = runtime.block_on(async {
        let listener = AsyncDaemonControlListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr_string().unwrap();
        let first_gate = Arc::new(tokio::sync::Semaphore::new(0));
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let entered = Arc::new(Mutex::new(Some(entered_tx)));
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let server_native = native.clone();
        let server_source = source.clone();
        let server_resolved = resolved.clone();
        let server_operations = seen_operations.clone();
        let server_gate = first_gate.clone();
        let server_receipt = native_receipt.clone();
        let server = tokio::spawn(async move {
            let mut carriers = tokio::task::JoinSet::new();
            loop {
                let accepted = tokio::select! {
                    _ = &mut stop_rx => break,
                    accepted = listener.accept() => accepted.unwrap(),
                };
                let mut carrier = accepted.into_inner();
                let native = server_native.clone();
                let source = server_source.clone();
                let resolved = server_resolved.clone();
                let operations = server_operations.clone();
                let gate = server_gate.clone();
                let status = status.clone();
                let entered = entered.clone();
                let receipt = server_receipt.clone();
                let mut shutdown = shutdown_rx.clone();
                carriers.spawn(async move {
                    let context = tokio::select! {
                        _ = shutdown.changed() => return,
                        context = read_local_object_rpc_carrier_prelude(&mut carrier) => context.unwrap(),
                    };
                    let mut acceptor = accept_local_object_rpc_stream_carrier(Box::new(carrier), &context).unwrap();
                    let mut streams = tokio::task::JoinSet::new();
                    while let Some(stream) = tokio::select! {
                        _ = shutdown.changed() => None,
                        stream = acceptor.accept() => stream.unwrap(),
                    } {
                        let context = RpcPeerContext::unauthenticated_local(stream.session_id.clone(), stream.channel_binding);
                        let native = native.clone();
                        let source = source.clone();
                        let resolved = resolved.clone();
                        let operations = operations.clone();
                        let gate = gate.clone();
                        let status = status.clone();
                        let entered = entered.clone();
                        let receipt = receipt.clone();
                        streams.spawn(async move {
                            serve_service_rpc_peer_stream(stream, &context, ObjectFrameIoOptions::default(), move |inbound, _| async move {
                                if inbound.request.route == DAEMON_STATUS_ROUTE {
                                    let _: () = inbound.decode_body().await?;
                                    return ObjectRpcOutgoingResponse::encode_borrowed_value(DAEMON_STATUS_ROUTE, &status);
                                }
                                if inbound.request.route == sinomo_core::service::CLUSTER_SERVICE_PROVIDERS_ROUTE {
                                    let request: sinomo_core::service::ServiceProvidersRequest = inbound.decode_body().await?;
                                    assert_eq!(request.capability, sinomo_dfs::SIFS_PATH_ACCESS_CAPABILITY);
                                    return ObjectRpcOutgoingResponse::encode_borrowed_value(
                                        sinomo_core::service::CLUSTER_SERVICE_PROVIDERS_ROUTE,
                                        &sinomo_core::service::ServiceProvidersResponse { providers: resolved },
                                    );
                                }
                                assert_eq!(inbound.request.route, DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE);
                                let forward: DaemonObjectRpcForwardRequest = inbound.decode_body().await?;
                                assert_eq!(forward.destination_process_affinity, sinomo_api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon);
                                let target = forward.target_node_id.clone();
                                assert_eq!(
                                    forward.request.route,
                                    sinomo_api::DAEMON_DFS_BROWSER_NO_LINKS_ROUTE,
                                    "selection must not probe providers"
                                );
                                operations.lock().unwrap().push(target.clone());
                                let held = scenario == Scenario::LostMutationReply;
                                let timeout = Duration::from_millis(forward.overall_timeout_millis.unwrap_or(5000));
                                let native_source = source.clone();
                                let response = tokio::task::spawn_blocking(move || native.lock().unwrap()
                                    .rpc_request(&native_source, &target, forward.request, timeout)).await.unwrap().unwrap();
                                if held {
                                    let actual: sinomo_api::DaemonDfsBrowserNoLinksResponse = response.decode_body().unwrap();
                                    let sinomo_api::DaemonDfsBrowserNoLinksResponse::Metadata { metadata } = actual else {
                                        panic!("native owner must produce the actual exact operation receipt");
                                    };
                                    *receipt.lock().unwrap() = Some(metadata);
                                }
                                if held {
                                    // Only hold the actual first provider reply AFTER native ingress/return.
                                    if let Some(entered) = entered.lock().unwrap().take() { let _ = entered.send(()); }
                                    let permit = gate.acquire().await.unwrap(); permit.forget();
                                }
                                ObjectRpcOutgoingResponse::encode_borrowed_value(DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE, &DaemonObjectRpcForwardResponse::Success {
                                    response, streamed_value_body: false, forward_elapsed_micros: 0,
                                })
                            }).await
                        });
                    }
                    streams.abort_all();
                    while streams.join_next().await.is_some() {}
                });
            }
            let _ = shutdown_tx.send(true);
            while carriers.join_next().await.is_some() {}
        });
        let mut backend = DaemonSifsVfsBackend::new(address).with_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
        backend.discovery_data_dir = Some(data);
        let mut call = tokio::task::spawn_blocking(move || match scenario {
            Scenario::ResolvedRemoteFirst => backend.stat_beneath_no_links("/services", "/services"),
            Scenario::LostMutationReply => backend.create_new_versioned_beneath_no_links(
                "/services", file_path, b"native committed bytes; caller outcome unknown",
                &VfsWriteOptions::default().create_new(),
            ),
        });
        let observed_ingress = scenario != Scenario::LostMutationReply
            || tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, entered_rx).await.is_ok_and(|result| result.is_ok());
        let completed = tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT + Duration::from_secs(1), &mut call).await;
        let before_release = completed.is_ok();
        first_gate.add_permits(1);
        let result = match completed { Ok(result) => result.unwrap(), Err(_) => call.await.unwrap() };
        let _ = stop_tx.send(());
        server.await.unwrap();
        assert!(observed_ingress, "control must observe the actual selected native response before holding delivery");
        assert!(before_release, "operation must not require the failed provider reply");
        (result, seen_operations.lock().unwrap().clone())
    });
    if scenario == Scenario::LostMutationReply {
        let receipt = native_receipt
            .lock()
            .unwrap()
            .clone()
            .expect("actual native commit receipt was held");
        let mut inspection = native.lock().unwrap().client(&source).unwrap();
        let stat = inspection.posix_stat_distributed(file_path).unwrap();
        assert_eq!(stat.object_id.to_string(), receipt.object_id);
        assert_eq!(stat.version.to_string(), receipt.version);
        assert_eq!(
            inspection.posix_read_file_distributed(file_path).unwrap(),
            b"native committed bytes; caller outcome unknown"
        );
        drop(inspection);
    }
    Arc::try_unwrap(native)
        .ok()
        .expect("all proxy owners drained")
        .into_inner()
        .unwrap()
        .close()
        .unwrap();
    match scenario {
        Scenario::ResolvedRemoteFirst => {
            let metadata = result.expect("the resolved provider completes the operation");
            assert_eq!(metadata.path, "/services");
            assert_eq!(
                operation_nodes,
                vec![remote],
                "one operation send to the daemon's first resolved provider"
            );
        }
        Scenario::LostMutationReply => {
            let error = result
                .expect_err("native data/receipt presence must not be presented as caller success");
            assert!(
                matches!(error, VfsError::Backend { ref detail, .. } if detail.contains("PossiblyApplied")),
                "{error}"
            );
            assert_eq!(
                operation_nodes,
                vec![source],
                "uncertain mutation must not be replayed to any provider"
            );
        }
    }
}
