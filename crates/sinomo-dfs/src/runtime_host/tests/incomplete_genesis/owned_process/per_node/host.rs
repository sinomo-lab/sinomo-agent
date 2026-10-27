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

mod prefix;
mod membership;

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
        // The fresh PREFIX fixture may use real DFS-eligible Raft learners.
        // This flag only validates initial DATA; it never changes live roles.
        let expected_replica_role = if configuration.dfs_learners {
            member.role == sinomo_core::ClusterMemberRole::Learner
                && member.state == sinomo_core::ClusterMemberState::Learner
        } else {
            member.role == sinomo_core::ClusterMemberRole::NonVoting
        };
        if member.cluster_id != state.cluster_id
            || member.node_public_key.is_empty()
            || !member.state.is_authorized()
            || member.endpoints.len() != 1
            || (member.role != sinomo_core::ClusterMemberRole::Voter && !expected_replica_role)
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
    observed_source: &std::sync::Mutex<Option<sinomo_core::ClusterEndpointRecord>>,
) -> Result<ProbeReply, NetworkError> {
    if request.prefix_contact_hints.is_some() && !matches!(request.kind, ProbeKind::PrefixRound | ProbeKind::PrefixRoundWrongKey) {
        return Err(invalid("PREFIX route hints are not admitted for this probe kind"));
    }
    if request.owned_contact_hint.is_some()
        && !matches!(request.kind, ProbeKind::RegisteredEndpointPing | ProbeKind::FormationEndpointPing
            | ProbeKind::RegisteredEndpointWrongId)
    {
        return Err(invalid("owned route hint is not admitted for this probe kind"));
    }
    if matches!(request.kind, ProbeKind::PrefixRound | ProbeKind::PrefixRoundWrongKey) {
        return prefix::probe(
            request, deadline, root, runtime, source, cluster, peers, contacts,
        );
    }
    if matches!(request.kind, ProbeKind::RequestMetadataMembers) {
        return membership::request(request, deadline, root, runtime, source, cluster, peers).await;
    }
    if matches!(request.kind, ProbeKind::PreparePrefixBaseline) {
        return membership::prepare_prefix_baseline(request, deadline, root, runtime, source, cluster, peers).await;
    }
    if matches!(request.kind, ProbeKind::ObserveOwnedEndpoint) {
        if request.target != *source {
            return Err(invalid("owned endpoint observation must execute at its actual target"));
        }
        let data = SinomoDataDir::new(root);
        let paths = ClusterPaths::new(&data);
        let owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
            &crate::runtime_host::DfsRuntimePaths::new(&data),
        ).map_err(|e| invalid(e.to_string()))?;
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)?;
        if owner.physical_data_root() != root || state.local_node_id != *source || state.cluster_id != *cluster {
            return Err(invalid("owned endpoint observation changed actual source/root/cluster"));
        }
        // This is exactly the receiver's resolve_authenticated_transport_peer
        // composition, read only. It supplies no authentication or grant.
        let directory = sinomo_core::network::cluster_directory_with_bootstrap_state(
            sinomo_core::network::cluster_directory_for(&paths), &state,
        ).snapshot()?;
        let (address, key) = contacts.get(source).ok_or_else(|| invalid("actual held listener missing"))?;
        let member = directory.node(source).filter(|m| &m.public_key == key)
            .ok_or_else(|| invalid("receiver directory lacks its current owner/key"))?;
        let endpoint = member.endpoints.iter().find(|e| &e.node_id == source
            && &e.address == address && matches!(e.transport,
                sinomo_core::ClusterTransportKind::Loopback | sinomo_core::ClusterTransportKind::LanTcp))
            .ok_or_else(|| invalid(format!("receiver directory has no current descriptor for its held listener; observed endpoints: {:?}", member.endpoints)))?;
        *observed_source.lock().unwrap() = Some(endpoint.clone());
        return Ok(ProbeReply { responder_node_id: None, route: DRIVER_ROUTE.into(),
            body: crate::metadata_io::encode_sobj(endpoint).map_err(|e| invalid(e.to_string()))? });
    }
    let wire = match request.kind {
        ProbeKind::Ping
        | ProbeKind::EndpointPing
        | ProbeKind::EndpointWrongPeer
        | ProbeKind::RegisteredEndpointPing
        | ProbeKind::FormationEndpointPing
        | ProbeKind::RegisteredEndpointWrongPeer
        | ProbeKind::RegisteredEndpointWrongId => {
            sinomo_core::cluster::cluster_network_ping_rpc_request(&request.nonce, 0)?
        }
        ProbeKind::DfsStatus | ProbeKind::EndpointDfsStatus => {
            ObjectRpcRequest::encode_body(crate::DFS_STATUS_ROUTE, crate::DFS_RPC_CATEGORY, &())?
        }
        ProbeKind::PrefixRound | ProbeKind::PrefixRoundWrongKey | ProbeKind::RequestMetadataMembers | ProbeKind::PreparePrefixBaseline | ProbeKind::ObserveOwnedEndpoint => unreachable!("native owner call dispatched above"),
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
    let registered_endpoint = matches!(
        request.kind,
        ProbeKind::RegisteredEndpointPing
            | ProbeKind::FormationEndpointPing
            | ProbeKind::RegisteredEndpointWrongPeer
            | ProbeKind::RegisteredEndpointWrongId
    );
    let result = if request.kind.path() == ProbePath::AuthenticatedEndpoint || registered_endpoint {
        let data = SinomoDataDir::new(root);
        let paths = ClusterPaths::new(&data);
        // Endpoint contacts use the actual native-written member view, never
        // a supplied authenticated stream or a synthetic connectivity edge.
        // Local native discovery supplies the exact owned contact; the real
        // registered carrier separately authorizes the peer and authenticates it.
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)?;
        if &state.local_node_id != source || &state.cluster_id != cluster {
            return Err(invalid(
                "native diagnostic source identity changed under its process",
            ));
        }
        // Piggyback only this already-read native owner's held-listener
        // descriptor. It is route DATA, never a reconstructed capability.
        let (own_address, own_key) = contacts.get(source)
            .ok_or_else(|| invalid("source has no actual owned listener"))?;
        let own_member = state.members.iter().find(|m| &m.node_id == source
            && m.state.is_authorized() && &m.node_public_key == own_key)
            .ok_or_else(|| invalid("source native member/held key differs"))?;
        *observed_source.lock().unwrap() = own_member.endpoints.iter().find(|e|
            &e.node_id == source && &e.address == own_address
            && matches!(e.transport, sinomo_core::ClusterTransportKind::Loopback
                | sinomo_core::ClusterTransportKind::LanTcp)).cloned();
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
            if node == &request.target
                && let Some(hint) = &request.owned_contact_hint
            {
                if &hint.node_id != node || &hint.address != address
                    || !matches!(hint.transport, sinomo_core::ClusterTransportKind::Loopback
                        | sinomo_core::ClusterTransportKind::LanTcp)
                {
                    return Err(invalid("owned hint differs from intended held listener"));
                }
                // Explicit input selection, never a failed-call fallback.
                // Native transport must authenticate this target and reject a
                // stale/foreign endpoint ID even if these route bytes match.
                return Ok(hint.clone());
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
        if matches!(
            request.kind,
            ProbeKind::EndpointWrongPeer | ProbeKind::RegisteredEndpointWrongPeer
        ) {
            let other = peers
                .iter()
                .find(|node| *node != source && **node != request.target)
                .ok_or_else(|| invalid("wrong-contact control lacks another actual peer"))?;
            // Deliberately invalid address hint only. The intended node/key and
            // all native handshake checks remain unchanged, and must refuse it.
            endpoint.address = contact(other)?.address;
        }
        if matches!(request.kind, ProbeKind::RegisteredEndpointWrongId) {
            let invalid_id =
                sinomo_core::Id::derived("dfs.test.unknown-native-endpoint", &request.nonce);
            if state
                .members
                .iter()
                .flat_map(|m| &m.endpoints)
                .any(|e| e.endpoint_id == invalid_id)
            {
                return Err(invalid("negative endpoint ID unexpectedly already exists"));
            }
            endpoint.endpoint_id = invalid_id;
        }
        *selected.lock().unwrap() = Some(endpoint.clone());
        eprintln!(
            "NATIVE_ENDPOINT_PROBE source={source} target={} nonce={} endpoint={endpoint:?}",
            request.target, request.nonce
        );
        if registered_endpoint {
            transport.call_endpoint(
                &endpoint,
                ObjectRpcCall::value(wire),
                ClusterObjectRpcOptions::default().with_overall_timeout(remaining),
                matches!(request.kind, ProbeKind::FormationEndpointPing),
            )?
        } else {
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
        }
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
        ProbeKind::Ping
        | ProbeKind::EndpointPing
        | ProbeKind::EndpointWrongPeer
        | ProbeKind::RegisteredEndpointPing
        | ProbeKind::FormationEndpointPing
        | ProbeKind::RegisteredEndpointWrongPeer
        | ProbeKind::RegisteredEndpointWrongId => {
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
        ProbeKind::PrefixRound | ProbeKind::PrefixRoundWrongKey | ProbeKind::RequestMetadataMembers | ProbeKind::PreparePrefixBaseline | ProbeKind::ObserveOwnedEndpoint => unreachable!("native owner call dispatched above"),
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
                        let observed_source = std::sync::Mutex::new(None);
                        let result = match tokio::time::timeout_at(
                            tokio::time::Instant::from_std(deadline), probe(transport.as_ref(), &command, deadline,
                                &root, &native_runtime, &source, &cluster, &peers, &contacts, &selected, &observed_source),
                        ).await {
                            Ok(result) => result.map_err(failure),
                            Err(_) => Err(failure(NetworkError::timeout("native process driver call and materialization",
                                NetworkTimeoutPhase::Overall, OP_CAP, NetworkDeliveryState::PossiblyApplied))),
                        };
                        context.respond_value(DRIVER_ROUTE, DriverResponse {
                            source_node_id: source, source_pid: std::process::id(), target: command.target,
                            nonce: command.nonce, path: command.kind.path(), contact: selected.into_inner().unwrap(),
                            source_contact: observed_source.into_inner().unwrap(), result,
                        })?;
                        Ok(ServiceDisposition::Continue)
                    }
                },
            ).build());
            Ok(())
        },
    )?);
    Ok(ComponentCatalog::builder()
        // Match the initialized Main profile: only the actual live registry's
        // lifecycle owner may advertise DFS providers. Without this component
        // the receiver routes exist locally but peer discovery stays empty.
        .register_factory(Arc::new(
            sinomo_module::cluster_service_catalog_publisher_component_factory()?,
        ))?
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
    if let Some(expected) = input.retained.as_ref() {
        return run_retained_node(&input, expected);
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

const RETAINED_IDENTITY_CAP: usize = 64 * 1024;
const RETAINED_WAL_CAP: usize = 16 * 1024 * 1024;

fn previous_owned_process_absent(expected: &RetainedNodeStart) -> TestResult<()> {
    if expected.old_pid == 0 || expected.old_start == 0 || expected.old_pid == std::process::id() {
        return Err("retained start has no distinct actual previous process identity".into());
    }
    match ProcessIdentity::read(expected.old_pid) {
        Ok(actual) if actual.start == expected.old_start => {
            Err("retained native process is still present with its captured start identity".into())
        }
        Ok(_) => Ok(()), // PID was reused; never signal an unrelated process.
        Err(error)
            if error
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::NotFound) =>
        {
            Ok(())
        }
        Err(error) => Err(error), // Unreadable/malformed is not proof of absence.
    }
}

fn open_retained_path(
    path: &Path,
    directory: bool,
    expected: Option<[u64; 2]>,
) -> TestResult<File> {
    let before = fs::symlink_metadata(path)?;
    if !path.is_absolute()
        || path.canonicalize()? != path
        || before.file_type().is_symlink()
        || before.is_dir() != directory
        || (!directory && !before.is_file())
    {
        return Err("retained native source is missing, aliased or the wrong physical type".into());
    }
    let file = File::open(path)?;
    let opened = file.metadata()?;
    let named = fs::symlink_metadata(path)?;
    let identity = [opened.dev(), opened.ino()];
    if [before.dev(), before.ino()] != identity
        || [named.dev(), named.ino()] != identity
        || expected.is_some_and(|expected| expected != identity)
    {
        return Err("retained native physical source was replaced".into());
    }
    Ok(file)
}

fn read_retained_file(path: &Path, file: &File, cap: usize) -> TestResult<Vec<u8>> {
    let before = FileId::of(&file.metadata()?);
    let bytes = read_pinned(file, before.size, cap)?;
    if FileId::of(&file.metadata()?) != before || FileId::of(&fs::symlink_metadata(path)?) != before
    {
        return Err("retained native file changed during bounded observation".into());
    }
    Ok(bytes)
}

fn run_retained_node(input: &NodeInput, expected: &RetainedNodeStart) -> TestResult<()> {
    previous_owned_process_absent(expected)?;
    let data = SinomoDataDir::new(&input.data_root);
    // Do not call ensure_root_layout or load_or_create identity here. All
    // physical sources must already exist before any normal daemon opener.
    let mut pins = vec![(
        input.data_root.clone(),
        open_retained_path(&input.data_root, true, Some(expected.data_identity))?,
    )];
    let owner_path = input.data_root.join("daemon/data-owner.lock");
    let owner = open_retained_path(&owner_path, false, Some(expected.data_owner_identity))?;
    fs2::FileExt::try_lock_exclusive(&owner)?;
    pins.push((owner_path, owner));
    let identity_path = sinomo_core::node::NodeIdentityPaths::new(&input.data_root).main;
    let identity_file =
        open_retained_path(&identity_path, false, Some(expected.identity_identity))?;
    let identity_bytes = read_retained_file(&identity_path, &identity_file, RETAINED_IDENTITY_CAP)?;
    if blake3::hash(&identity_bytes).to_hex().as_str() != expected.identity_blake3 {
        return Err("retained native identity bytes differ from the preserved original".into());
    }
    // Same native stored DATA codec, read-only. Never print/serialize this
    // private material, construct a signer or generate a replacement key.
    let identity: sinomo_core::node::NodeIdentity =
        sinomo_core::object::serde_ext::from_bytes(&identity_bytes)?;
    if identity.public_key != expected.public_key || expected.public_key.is_empty() {
        return Err("retained native public identity differs".into());
    }
    drop(identity);
    drop(identity_bytes);
    pins.push((identity_path, identity_file));
    let initial: NodeConfiguration = read_data(&input.owned_root.join("configuration.sobj"))?;
    let original: NodeHello = read_data(&input.owned_root.join("hello.sobj"))?;
    if original.pid != expected.old_pid
        || original.node_id != input.node_id
        || original.public_key != expected.public_key
        || original.address != expected.address
        || input.invocation != format!("{}:restart", original.invocation)
        || !initial.bootstrap_initial_voter
        || initial.state.cluster_id != expected.cluster_id
    {
        return Err(
            "retained A start differs from the original owned fixture hello/configuration".into(),
        );
    }
    let peers = validate_configuration(input, &original, &initial)?;
    let contacts: OwnedContacts = initial
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
    let state_file = open_retained_path(&paths.state, false, None)?;
    // Pin the original DATA before the native read. Never write the initial
    // configuration back, nor treat its old role/term as current permission.
    let state_bytes = read_retained_file(&paths.state, &state_file, DATA_CAP)?;
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)?;
    if state.local_node_id != input.node_id
        || state.cluster_id != expected.cluster_id
        || state.initialized_at != initial.state.initialized_at
        || state.term < initial.state.term
        || state.kv_revision < initial.state.kv_revision
        || state.members.len() != 3
        || state
            .members
            .iter()
            .map(|member| member.node_id.clone())
            .collect::<BTreeSet<_>>()
            != peers
    {
        return Err(
            "retained native node/cluster/member state differs from the original fixture".into(),
        );
    }
    for member in &state.members {
        let (address, key) = contacts
            .get(&member.node_id)
            .ok_or("retained member outside original contact allowlist")?;
        if member.cluster_id != state.cluster_id
            || member.node_public_key != *key
            || !member.state.is_authorized()
            || !member.endpoints.iter().any(|endpoint| {
                endpoint.node_id == member.node_id
                    && endpoint.address == *address
                    && matches!(
                        endpoint.transport,
                        sinomo_core::ClusterTransportKind::Loopback
                            | sinomo_core::ClusterTransportKind::LanTcp
                    )
            })
        {
            return Err("retained native membership lost an original owned contact/key".into());
        }
    }
    if read_retained_file(&paths.state, &state_file, DATA_CAP)? != state_bytes {
        return Err("retained native cluster state changed during validation".into());
    }
    pins.push((paths.state.clone(), state_file));
    let wal = paths.kv_paths().raft_wal_paths(&input.node_id);
    pins.push((
        wal.root.clone(),
        open_retained_path(&wal.root, true, Some(expected.wal_root_identity))?,
    ));
    let wal_lock = open_retained_path(&wal.lock, false, Some(expected.wal_lock_identity))?;
    fs2::FileExt::try_lock_exclusive(&wal_lock)?;
    pins.push((wal.lock.clone(), wal_lock));
    let wal_file = open_retained_path(&wal.wal, false, Some(expected.wal_identity))?;
    let wal_bytes = read_retained_file(&wal.wal, &wal_file, RETAINED_WAL_CAP)?;
    if blake3::hash(&wal_bytes).to_hex().as_str() != expected.wal_blake3 {
        return Err("retained native WAL bytes differ from the post-kill preservation".into());
    }
    drop(wal_bytes);
    pins.push((wal.wal.clone(), wal_file));
    let hard_file = open_retained_path(&wal.hard_state, false, Some(expected.hard_state_identity))?;
    let hard_bytes = read_retained_file(&wal.hard_state, &hard_file, DATA_CAP)?;
    if blake3::hash(&hard_bytes).to_hex().as_str() != expected.hard_state_blake3 {
        return Err("retained native Raft hard-state bytes changed after preservation".into());
    }
    let hard: sinomo_core::kv::RaftHardState =
        sinomo_core::object::serde_ext::from_bytes(&hard_bytes)?;
    if expected.raft_commit_index == 0
        || hard.current_term < expected.raft_term
        || hard.commit_index < expected.raft_commit_index
    {
        return Err("retained native Raft term/commit is missing or regressed".into());
    }
    pins.push((wal.hard_state.clone(), hard_file));
    match (
        &expected.wal_snapshot_identity,
        &expected.wal_snapshot_blake3,
    ) {
        (Some(identity), Some(hash)) => {
            let snapshot = open_retained_path(&wal.snapshot, false, Some(*identity))?;
            let bytes = read_retained_file(&wal.snapshot, &snapshot, RETAINED_WAL_CAP)?;
            if blake3::hash(&bytes).to_hex().as_str() != hash.as_str() {
                return Err(
                    "retained native Raft snapshot bytes changed after preservation".into(),
                );
            }
            pins.push((wal.snapshot.clone(), snapshot));
        }
        (None, None) => match fs::symlink_metadata(&wal.snapshot) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(_) => return Err("retained native Raft snapshot appeared after preservation".into()),
        },
        _ => return Err("retained native Raft snapshot expectation is incomplete".into()),
    }
    previous_owned_process_absent(expected)?;
    let address: SocketAddr = expected.address.parse()?;
    if address.ip() != Ipv4Addr::LOCALHOST || address.port() == 0 {
        return Err("retained native listener is not its exact original loopback contact".into());
    }
    let hello_path = input.owned_root.join("restart-hello.sobj");
    match fs::symlink_metadata(&hello_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
        Ok(_) => return Err("retained native restart hello already exists; preserve it".into()),
    }
    let listener =
        sinomo_daemon::prepare_daemon_control_listener(sinomo_daemon::DaemonRunOptions {
            host: Ipv4Addr::LOCALHOST.to_string(),
            port: address.port(),
            port_fallback: false,
        })?;
    if listener.options().port != address.port() || listener.fallback_from_port().is_some() {
        return Err("retained native listener changed its original port".into());
    }
    // Recheck names against the retained descriptors, with both native lock
    // files still held. No directory mtime is mistaken for physical identity.
    for (path, file) in &pins {
        let descriptor = file.metadata()?;
        let named = fs::symlink_metadata(path)?;
        if named.file_type().is_symlink()
            || [named.dev(), named.ino()] != [descriptor.dev(), descriptor.ino()]
            || named.is_dir() != descriptor.is_dir()
            || path.canonicalize()? != *path
        {
            return Err("retained native path changed before normal owner acquisition".into());
        }
    }
    let cluster =
        sinomo_daemon::daemon_cluster_status(&state, sinomo_daemon::DaemonClusterProcess::Main)
            .ok_or("retained actual startup cluster status unavailable")?;
    let catalog = driver_catalog(input, peers, contacts)?;
    write_data(
        &hello_path,
        &NodeHello {
            pid: std::process::id(),
            invocation: input.invocation.clone(),
            node_id: input.node_id.clone(),
            address: expected.address.clone(),
            public_key: expected.public_key.clone(),
        },
    )?;
    // The normal daemon acquires its OWN actual storage/lifecycle locks. The
    // manifest and these read-only pins are not runtime authority or a signer.
    drop(pins);
    sinomo_daemon::run_daemon_control_loop_with_prepared_listener(
        listener,
        sinomo_daemon::DaemonPaths::new(&data),
        "daemon",
        Some(cluster),
        catalog,
        sinomo_daemon::DaemonRuntimeConfig::default(),
    )?;
    Ok(())
}
