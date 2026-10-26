//! One real daemon process. The parent supplies fresh fixture configuration,
//! never a transport signer, authenticated link or private DFS capability.
use super::*;
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterObjectRpcOptions, NetworkDeliveryState, NetworkError, NetworkTimeoutPhase, ObjectRpcCall,
};
use sinomo_core::service::{ServiceDisposition, ServiceError, ServiceRegistry, ServiceRoute};
use sinomo_module::{
    ComponentCatalog, ComponentError, ComponentFactory, ComponentHostRole,
    ComponentObjectRpcTransport, ModuleId, ModuleObjectRpcRoute, ModuleRegistration,
    ModuleRouteAuth, ModuleSurfaces, StaticComponentFactory,
};
use std::collections::BTreeSet;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Instant;

const COMPONENT: &str = "module.fixture.dfs_native_peer";
const MAX_DRIVER_INPUT: usize = 4096;
const MAX_NONCE: usize = 192;
type OwnedContacts = std::collections::BTreeMap<NodeId, (String, String)>;

fn validate_configuration(
    input: &NodeInput,
    hello: &NodeHello,
    configuration: &NodeConfiguration,
) -> TestResult<BTreeSet<NodeId>> {
    let state = &configuration.state;
    let ids = state
        .members
        .iter()
        .map(|m| m.node_id.clone())
        .collect::<BTreeSet<_>>();
    let public_keys = state
        .members
        .iter()
        .map(|m| m.node_public_key.as_str())
        .collect::<BTreeSet<_>>();
    let voters = state
        .members
        .iter()
        .filter(|m| m.role == sinomo_core::ClusterMemberRole::Voter)
        .collect::<Vec<_>>();
    if state.local_node_id != input.node_id
        || state.term != 1
        || state.kv_revision != 1
        || state.members.len() != 3
        || ids.len() != 3
        || public_keys.len() != 3
        || voters.len() != 1
        || configuration.bootstrap_initial_voter != (voters[0].node_id == input.node_id)
    {
        return Err("fresh native fixture requires exact local ID and one initial voter in three unique members".into());
    }
    let mut endpoints = BTreeSet::new();
    for member in &state.members {
        if member.cluster_id != state.cluster_id
            || member.node_public_key.is_empty()
            || !member.state.is_authorized()
            || member.endpoints.len() != 1
            || (member.role != sinomo_core::ClusterMemberRole::Voter
                && member.role != sinomo_core::ClusterMemberRole::NonVoting)
        {
            return Err(
                "fresh native roster contains a foreign, pending or unsupported member".into(),
            );
        }
        let endpoint = &member.endpoints[0];
        let address: SocketAddr = endpoint.address.parse()?;
        if endpoint.node_id != member.node_id
            || address.ip() != Ipv4Addr::LOCALHOST
            || address.port() == 0
            || !endpoints.insert(address)
            || !matches!(
                endpoint.transport,
                sinomo_core::ClusterTransportKind::Loopback
                    | sinomo_core::ClusterTransportKind::LanTcp
            )
        {
            return Err(
                "native fixture endpoints must be distinct owned loopback listeners".into(),
            );
        }
        if member.node_id == input.node_id
            && (member.node_public_key != hello.public_key || endpoint.address != hello.address)
        {
            return Err(
                "native fixture configuration changed this child's real key or held listener"
                    .into(),
            );
        }
    }
    if !ids.contains(&input.node_id) {
        return Err("native fixture omits this actual child".into());
    }
    Ok(ids)
}

fn failure(error: NetworkError) -> ProbeFailure {
    ProbeFailure {
        delivery_state: error.delivery_state().map(|state| format!("{state:?}")),
        error: error.to_string(),
    }
}

fn invalid(detail: impl Into<String>) -> NetworkError {
    NetworkError::Serialization {
        detail: detail.into(),
    }
}

async fn probe(
    transport: &dyn ComponentObjectRpcTransport,
    request: &DriverRequest,
    deadline: Instant,
    root: &Path,
    runtime: &tokio::runtime::Handle,
    source: &NodeId,
    cluster: &sinomo_core::Id,
    peers: &BTreeSet<NodeId>,
    contacts: &OwnedContacts,
    selected: &std::sync::Mutex<Option<sinomo_core::ClusterEndpointRecord>>,
) -> Result<ProbeReply, NetworkError> {
    let wire = match request.kind {
        ProbeKind::Ping | ProbeKind::EndpointPing | ProbeKind::EndpointWrongPeer => {
            sinomo_core::cluster::cluster_network_ping_rpc_request(&request.nonce, 0)?
        }
        ProbeKind::DfsStatus | ProbeKind::EndpointDfsStatus => {
            ObjectRpcRequest::encode_body(crate::DFS_STATUS_ROUTE, crate::DFS_RPC_CATEGORY, &())?
        }
    };
    let route = wire.route.clone();
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| {
            NetworkError::timeout(
                "native process driver before call",
                NetworkTimeoutPhase::Overall,
                OP_CAP,
                NetworkDeliveryState::DefinitelyNotSent,
            )
        })?;
    let result = if request.kind.path() == ProbePath::AuthenticatedEndpoint {
        let data = SinomoDataDir::new(root);
        let paths = ClusterPaths::new(&data);
        // Endpoint contacts use the actual native-written member view, never
        // a supplied authenticated stream or a synthetic connectivity edge.
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)?;
        if &state.local_node_id != source || &state.cluster_id != cluster {
            return Err(invalid(
                "native diagnostic source identity changed under its process",
            ));
        }
        let contact = |node: &NodeId| -> Result<sinomo_core::ClusterEndpointRecord, NetworkError> {
            if !peers.contains(node) {
                return Err(invalid("diagnostic contact is outside owned fixture"));
            }
            let member = state
                .members
                .iter()
                .find(|member| &member.node_id == node && member.state.is_authorized())
                .ok_or_else(|| invalid("diagnostic target lacks current native member binding"))?;
            let (address, public_key) = contacts.get(node).ok_or_else(|| {
                invalid("diagnostic target lacks its actual held fixture listener")
            })?;
            if &member.node_public_key != public_key {
                return Err(invalid(
                    "native member key differs from the owned fixture identity",
                ));
            }
            // Preserve the native descriptor ID/type/privacy checked by the
            // routed handshake. Address scope alone cannot recreate that ID.
            // A missing owned advertisement stays unavailable; do not choose
            // a default/shadow endpoint or synthesize a replacement descriptor.
            member
                .endpoints
                .iter()
                .find(|endpoint| {
                    &endpoint.node_id == node
                        && &endpoint.address == address
                        && matches!(
                            endpoint.transport,
                            sinomo_core::ClusterTransportKind::Loopback
                                | sinomo_core::ClusterTransportKind::LanTcp
                        )
                })
                .cloned()
                .ok_or_else(|| {
                    invalid("owned listener lacks its current native endpoint descriptor")
                })
        };
        let mut endpoint = contact(&request.target)?;
        if matches!(request.kind, ProbeKind::EndpointWrongPeer) {
            let other = peers
                .iter()
                .find(|node| *node != source && **node != request.target)
                .ok_or_else(|| invalid("wrong-contact control lacks another actual peer"))?;
            // Deliberately invalid address hint only. The intended node/key and
            // all native handshake checks remain unchanged, and must refuse it.
            endpoint.address = contact(other)?.address;
        }
        *selected.lock().unwrap() = Some(endpoint.clone());
        eprintln!(
            "NATIVE_ENDPOINT_PROBE source={source} target={} nonce={} endpoint={endpoint:?}",
            request.target, request.nonce
        );
        let mut host = sinomo_daemon::DaemonObjectRpcAsyncHost::direct_only(runtime)
            .with_paths(&sinomo_daemon::DaemonPaths::new(&data));
        sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &paths)
            .call_endpoint(
                &state,
                &endpoint,
                ObjectRpcCall::value(wire),
                ClusterObjectRpcOptions::default().with_overall_timeout(remaining),
            )
            .await?
    } else {
        transport
            .call_async(
                &request.target,
                ObjectRpcCall::value(wire),
                ClusterObjectRpcOptions::default().with_overall_timeout(remaining),
                true,
            )
            .await?
    };
    // Semantic attachments are refused by the maintained value decoder. It
    // consumes the actual result exactly once, within the same absolute cap.
    let response = result.into_materialized_value_response(MAX_REPLY).await?;
    if response.route != route || response.body.len() > MAX_REPLY {
        return Err(invalid(
            "native driver reply route or bounded value differs",
        ));
    }
    let header_responder = response.responder_node_id().map(str::to_owned);
    if header_responder
        .as_deref()
        .is_some_and(|node| node != request.target.as_str())
    {
        return Err(invalid("native driver reply names another responder"));
    }
    let responder_node_id = match request.kind {
        ProbeKind::Ping | ProbeKind::EndpointPing | ProbeKind::EndpointWrongPeer => {
            let ping: sinomo_core::cluster::ClusterNetworkPingResponse = response.decode_body()?;
            if ping.responder_node_id != request.target
                || ping.nonce != request.nonce
                || ping.payload_bytes != 0
                || !ping.response_payload.is_empty()
                || ping.request_attachment_bytes != 0
                || ping.response_attachment_bytes != 0
                || ping.payload_mode
                    != sinomo_core::cluster::ClusterNetworkPingPayloadMode::EmbeddedSobj
            {
                return Err(invalid(
                    "actual native ping responder/nonce/payload differs",
                ));
            }
            Some(ping.responder_node_id.to_string())
        }
        ProbeKind::DfsStatus | ProbeKind::EndpointDfsStatus => {
            let _: crate::DfsOperatorStatus = response.decode_body()?;
            // DFS status has no echoed nonce or responder in its typed body.
            // Preserve only an actual native header, never copy the target.
            header_responder
        }
    };
    if Instant::now() >= deadline {
        return Err(NetworkError::timeout(
            "native process driver after decode",
            NetworkTimeoutPhase::Overall,
            OP_CAP,
            NetworkDeliveryState::PossiblyApplied,
        ));
    }
    Ok(ProbeReply {
        responder_node_id,
        route: response.route,
        body: response.body,
    })
}

fn driver_catalog(
    input: &NodeInput,
    peers: BTreeSet<NodeId>,
    contacts: OwnedContacts,
) -> TestResult<ComponentCatalog> {
    let registration = ModuleRegistration::native_rust(
        ModuleId::from_legacy_named(COMPONENT)?,
        "1.0.0",
        "fixture",
        "fixture",
        "1.0.0",
        sinomo_core::TrustRing::R1TrustedRuntime,
        COMPONENT,
    )
    .with_surfaces(
        ModuleSurfaces::default()
            .with_capability_export(sinomo_core::CapabilityName::new(DRIVER_OPERATION)?)
            .with_object_rpc_route(ModuleObjectRpcRoute::new(
                DRIVER_ROUTE,
                DRIVER_CATEGORY,
                DRIVER_OPERATION,
                ModuleRouteAuth::LocalCommand,
            )),
    );
    let descriptor = sinomo_module::component_descriptor_from_registration(&registration)?;
    let expected_root = input.data_root.clone();
    let source = input.node_id.clone();
    let factory: Arc<dyn ComponentFactory> = Arc::new(StaticComponentFactory::new(
        descriptor,
        move |component, scope| {
            let bad = |detail: &str| ComponentError::Component {
                component_id: COMPONENT.into(),
                detail: detail.into(),
            };
            if component.data_dir().root() != expected_root
                || component.host_role() != ComponentHostRole::Main
            {
                return Err(bad("native driver mounted under another root or host role"));
            }
            let transport = component
                .object_rpc_transport()
                .ok_or_else(|| bad("actual native object-RPC transport missing"))?;
            let local = sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(
                component.data_dir(),
            ))
            .map_err(|error| ComponentError::Component {
                component_id: COMPONENT.into(),
                detail: error.to_string(),
            })?;
            if local.local_node_id != source {
                return Err(bad("native driver component has another startup node"));
            }
            let cluster = local.cluster_id.clone();
            let native_runtime = component
                .runtime()
                .ok_or_else(|| bad("native driver has no actual runtime"))?
                .clone();
            let source = source.clone();
            let root = expected_root.clone();
            let peers = peers.clone();
            let contacts = contacts.clone();
            scope.set_services(ServiceRegistry::builder().register_dynamic(
                "dfs-native-peer-fixture",
                ServiceRoute::from_route_metadata(DRIVER_ROUTE, DRIVER_CATEGORY, "local-command", [DRIVER_OPERATION]),
                move |mut context, request| {
                    let transport = Arc::clone(&transport);
                    let source = source.clone();
                    let root = root.clone();
                    let peers = peers.clone();
                    let contacts = contacts.clone();
                    let cluster = cluster.clone();
                    let native_runtime = native_runtime.clone();
                    async move {
                        let deadline = Instant::now() + OP_CAP;
                        if context.data_dir.root() != root || request.route != DRIVER_ROUTE {
                            return Err(ServiceError::Service("native driver request root/route differs".into()));
                        }
                        let command: DriverRequest = tokio::time::timeout_at(
                            tokio::time::Instant::from_std(deadline),
                            context.decode_request_body_bounded(&request, MAX_DRIVER_INPUT),
                        ).await.map_err(|_| ServiceError::Service("native driver decode reached original five-second deadline".into()))??;
                        if !peers.contains(&command.target) || command.nonce.is_empty() || command.nonce.len() > MAX_NONCE
                            || !command.nonce.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"-_:./".contains(&byte))
                        {
                            return Err(ServiceError::Service("native driver target/nonce is outside the fixture".into()));
                        }
                        let selected = std::sync::Mutex::new(None);
                        let result = match tokio::time::timeout_at(
                            tokio::time::Instant::from_std(deadline), probe(transport.as_ref(), &command, deadline,
                                &root, &native_runtime, &source, &cluster, &peers, &contacts, &selected),
                        ).await {
                            Ok(result) => result.map_err(failure),
                            Err(_) => Err(failure(NetworkError::timeout("native process driver call and materialization",
                                NetworkTimeoutPhase::Overall, OP_CAP, NetworkDeliveryState::PossiblyApplied))),
                        };
                        context.respond_value(DRIVER_ROUTE, DriverResponse {
                            source_node_id: source, source_pid: std::process::id(), target: command.target,
                            nonce: command.nonce, path: command.kind.path(), contact: selected.into_inner().unwrap(), result,
                        })?;
                        Ok(ServiceDisposition::Continue)
                    }
                },
            ).build());
            Ok(())
        },
    )?);
    Ok(ComponentCatalog::builder()
        .register_module_factory(
            crate::dfs_module_registration(),
            Arc::new(crate::component_factory()?),
        )?
        .register_module_factory(registration, factory)?
        .build()?)
}

pub(super) fn run_node(input: NodeInput) -> TestResult<()> {
    if !input.owned_root.is_absolute()
        || input.owned_root.canonicalize()? != input.owned_root
        || input.data_root != input.owned_root.join("data")
    {
        return Err("native child roots are not its exact parent-owned paths".into());
    }
    match fs::symlink_metadata(&input.data_root) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        _ => return Err("native process constructor requires an absent fresh DATA root".into()),
    }
    let data = SinomoDataDir::new(&input.data_root);
    data.ensure_root_layout()?;
    DirectoryId::read(&input.data_root)?;
    let identity = sinomo_core::node::load_or_create_main_node_identity(&data.root())?;
    // Keep an owned ephemeral conflict alive while the maintained helper
    // chooses and retains a real fallback listener with a reported port.
    let occupied = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let occupied_port = occupied.local_addr()?.port();
    let listener =
        sinomo_daemon::prepare_daemon_control_listener(sinomo_daemon::DaemonRunOptions {
            host: Ipv4Addr::LOCALHOST.to_string(),
            port: occupied_port,
            port_fallback: true,
        })?;
    if listener.options().port == 0 || listener.fallback_from_port() != Some(occupied_port) {
        return Err("native prepared listener did not retain its actual fallback port".into());
    }
    drop(occupied);
    let hello = NodeHello {
        pid: std::process::id(),
        invocation: input.invocation.clone(),
        node_id: input.node_id.clone(),
        address: format!("127.0.0.1:{}", listener.options().port),
        public_key: identity.public_key,
    };
    write_data(&input.owned_root.join("hello.sobj"), &hello)?;
    let mut release = [0u8; 1];
    std::io::stdin().read_exact(&mut release)?;
    if release != [1] {
        return Err("native child configuration release byte differs".into());
    }
    let configuration: NodeConfiguration = read_data(&input.owned_root.join("configuration.sobj"))?;
    let peers = validate_configuration(&input, &hello, &configuration)?;
    let contacts = configuration
        .state
        .members
        .iter()
        .map(|member| {
            (
                member.node_id.clone(),
                (
                    member.endpoints[0].address.clone(),
                    member.node_public_key.clone(),
                ),
            )
        })
        .collect();
    let paths = ClusterPaths::new(&data);
    let kv = paths.kv_paths();
    if paths.state.exists() || kv.raft_wal_paths(&input.node_id).root.exists() {
        return Err("native process bootstrap found pre-existing configuration or WAL".into());
    }
    sinomo_core::cluster::write_cluster_state(&paths, &configuration.state)?;
    if configuration.bootstrap_initial_voter {
        // This is fresh initial creation, never a leader reset or retained
        // store adoption. The owner bootstrap also checks all empty KV fields.
        let node = kv.open_raft_node(&input.node_id)?;
        let snapshot = node.store().try_snapshot()?;
        eprintln!(
            "NATIVE_FIXTURE_INITIAL_RAFT hard_state={:?} revision={} entries={}",
            node.hard_state(),
            snapshot.revision.0,
            snapshot.entries.len()
        );
        // The actual native fresh hard state starts at term1, not term0.
        // Compare every field with that owner's exact default; never accept
        // a retained vote/leader/commit or nonempty revision-zero snapshot.
        if node.hard_state() != &sinomo_core::kv::RaftHardState::default()
            || snapshot.revision.0 != 0
            || !snapshot.entries.is_empty()
            || !snapshot.leases.is_empty()
            || !snapshot.events.is_empty()
            || !snapshot.committed_requests.is_empty()
            || !snapshot.expired_requests.is_empty()
        {
            return Err("initial native voter owner is not an empty revision-zero store".into());
        }
        drop(snapshot);
        drop(node);
        kv.locked_set_raft_leader_compact(&input.node_id, 1)?;
        sinomo_core::cluster::bootstrap_single_voter_cluster_state_to_local_kv(
            &paths,
            &configuration.state,
        )?;
    }
    // Nonvoters receive no seeded WAL or copied snapshot. Only the actual
    // daemon's normal native replication/authority paths can advance them.
    let cluster = sinomo_daemon::daemon_cluster_status(
        &configuration.state,
        sinomo_daemon::DaemonClusterProcess::Main,
    )
    .ok_or("actual local startup cluster status unavailable")?;
    sinomo_daemon::run_daemon_control_loop_with_prepared_listener(
        listener,
        sinomo_daemon::DaemonPaths::new(&data),
        "daemon",
        Some(cluster),
        driver_catalog(&input, peers, contacts)?,
        sinomo_daemon::DaemonRuntimeConfig::default(),
    )?;
    Ok(())
}
