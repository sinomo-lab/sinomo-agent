//! Shared unit-test host for the actual embedded DFS component.
//! Clients always use DfsNodeTransport; healthy replies come from native ingress.
use crate::*;
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterObjectRpcOptions, ClusterPaths, NetworkDeliveryState, NetworkError, NetworkTimeoutPhase,
    ObjectRpcCall, ObjectRpcOutgoingResponse, ObjectRpcRequest, ObjectRpcResponse, ObjectRpcResult,
};
use sinomo_core::node::{ClusterId, NodeId};
use sinomo_core::service::{
    ClusterAccess, ResponseSink, ServiceContext, ServiceError, ServiceRegistry, ServiceSigner,
};
use sinomo_module::{
    ComponentContext, ComponentEventBus, ComponentHostRole, ComponentObjectRpcTransport,
    ComponentReconciler,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::{Notify, Semaphore};

const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_OBSERVATIONS: usize = 4096;
type RpcFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ObjectRpcResult, NetworkError>> + Send + 'a>>;
type UnitFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

fn initialize_opt_in_trace() {
    if std::env::var("SINOMO_DFS_TEST_TRACE").as_deref() != Ok("1") {
        return;
    }
    static TRACE: std::sync::Once = std::sync::Once::new();
    TRACE.call_once(|| {
        let installed = tracing_subscriber::fmt()
            .with_env_filter("warn,sinomo.dfs.perf=trace,sinomo.dfs.maintenance=debug,sinomo.dfs.reconciliation=trace")
            .with_test_writer()
            .try_init();
        assert!(installed.is_ok() || tracing::enabled!(target: "sinomo.dfs.perf", tracing::Level::TRACE),
            "opt-in DFS fixture trace could not enable the existing performance target: {installed:?}");
    });
    assert!(
        tracing::enabled!(target: "sinomo.dfs.perf", tracing::Level::TRACE),
        "opt-in DFS fixture performance trace is disabled"
    );
}

fn executor() -> &'static tokio::runtime::Runtime {
    initialize_opt_in_trace();
    static EXECUTOR: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    EXECUTOR.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("dfs-shared-test-runtime")
            .enable_all()
            .build()
            .expect("shared DFS test executor")
    })
}
fn run_rpc(future: RpcFuture<'_>) -> Result<ObjectRpcResult, NetworkError> {
    if let Ok(current) = tokio::runtime::Handle::try_current() {
        assert!(
            matches!(
                current.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ),
            "synchronous DFS test RPC cannot block a current-thread executor"
        );
        tokio::task::block_in_place(|| executor().block_on(future))
    } else {
        executor().block_on(future)
    }
}
fn run_unit(future: UnitFuture<'_>) -> Result<(), String> {
    if let Ok(current) = tokio::runtime::Handle::try_current() {
        if !matches!(
            current.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return Err(
                "synchronous DFS test lifecycle cannot block a current-thread executor".into(),
            );
        }
        tokio::task::block_in_place(|| executor().block_on(future))
    } else {
        executor().block_on(future)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RpcOrigin {
    RegisteredHost,
    ExplicitFixtureRpc,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RpcPhase {
    Constructed,
    BeforeDispatch,
    AfterDispatch,
    Completed,
    EndedWithoutResponse,
}
#[derive(Clone, Debug)]
pub(crate) struct RpcObservation {
    pub source: NodeId,
    pub target: NodeId,
    pub call_id: u64,
    pub route: String,
    pub phase: RpcPhase,
    pub origin: RpcOrigin,
    pub options: ClusterObjectRpcOptions,
    pub observe_connectivity: bool,
    pub streamed_value: bool,
    pub body_bytes: Option<u64>,
}
#[derive(Clone)]
pub(crate) enum FaultAction {
    PauseBeforeDispatch(Arc<Semaphore>),
    BudgetExhausted,
}
#[derive(Clone)]
pub(crate) struct FaultRule {
    pub source: Option<NodeId>,
    pub target: Option<NodeId>,
    pub route: Option<String>,
    pub call_id: Option<u64>,
    pub origin: Option<RpcOrigin>,
    pub remaining_hits: usize,
    pub action: FaultAction,
}
struct Endpoint {
    data_dir: Arc<SinomoDataDir>,
    cluster: Arc<dyn ClusterAccess>,
    registry: ServiceRegistry,
    accepting: AtomicBool,
    active: AtomicUsize,
    idle: Notify,
}
struct Transfer {
    source: NodeId,
    target: NodeId,
    task: tokio::task::JoinHandle<Result<(), NetworkError>>,
}
struct Fabric {
    dropped_observations: AtomicUsize,
    exhausted_budgets: AtomicUsize,
    next_call: AtomicU64,
    nodes: Mutex<BTreeMap<NodeId, Arc<Endpoint>>>,
    faults: Mutex<Vec<FaultRule>>,
    observations: Mutex<Vec<RpcObservation>>,
    transfers: Mutex<Vec<Transfer>>,
}
impl Fabric {
    fn record(&self, observation: RpcObservation) {
        if let Ok(mut records) = self.observations.lock() {
            if records.len() < MAX_OBSERVATIONS {
                records.push(observation);
                return;
            }
        }
        self.dropped_observations.fetch_add(1, Ordering::Relaxed);
    }
    fn fault(&self, observation: &RpcObservation) -> Option<FaultAction> {
        let mut rules = self.faults.lock().expect("DFS test fault rules");
        let rule = rules.iter_mut().find(|rule| {
            rule.source
                .as_ref()
                .is_none_or(|id| id == &observation.source)
                && rule
                    .target
                    .as_ref()
                    .is_none_or(|id| id == &observation.target)
                && rule
                    .route
                    .as_ref()
                    .is_none_or(|route| route == &observation.route)
                && rule
                    .origin
                    .is_none_or(|origin| origin == observation.origin)
                && rule.call_id.is_none_or(|id| id == observation.call_id)
        })?;
        if rule.remaining_hits == 0 {
            self.exhausted_budgets.fetch_add(1, Ordering::Relaxed);
            return Some(FaultAction::BudgetExhausted);
        }
        rule.remaining_hits -= 1;
        Some(rule.action.clone())
    }
    fn transfer(
        &self,
        source: NodeId,
        target: NodeId,
        task: tokio::task::JoinHandle<Result<(), NetworkError>>,
    ) {
        self.transfers
            .lock()
            .expect("DFS test body transfers")
            .push(Transfer {
                source,
                target,
                task,
            });
    }
}
struct Invocation {
    endpoint: Arc<Endpoint>,
    fabric: Arc<Fabric>,
    observation: RpcObservation,
    completed: bool,
}
impl Drop for Invocation {
    fn drop(&mut self) {
        self.endpoint.active.fetch_sub(1, Ordering::AcqRel);
        self.endpoint.idle.notify_waiters();
        if !self.completed {
            self.observation.phase = RpcPhase::EndedWithoutResponse;
            self.fabric.record(self.observation.clone());
        }
    }
}
#[derive(Clone)]
struct Carrier {
    source: NodeId,
    fabric: Arc<Fabric>,
    origin: RpcOrigin,
}
impl Carrier {
    fn observation(
        &self,
        target: &NodeId,
        call: &ObjectRpcCall,
        options: ClusterObjectRpcOptions,
        observe_connectivity: bool,
    ) -> RpcObservation {
        RpcObservation {
            source: self.source.clone(),
            target: target.clone(),
            call_id: self.fabric.next_call.fetch_add(1, Ordering::Relaxed),
            route: call.request.route.clone(),
            phase: RpcPhase::Constructed,
            origin: self.origin,
            options,
            observe_connectivity,
            streamed_value: call.has_streamed_value_body(),
            body_bytes: call.body.total_bytes(),
        }
    }
    async fn deliver(
        &self,
        target: &NodeId,
        call: ObjectRpcCall,
        mut observation: RpcObservation,
        dispatch_started: Arc<AtomicBool>,
    ) -> Result<ObjectRpcResult, NetworkError> {
        let endpoint = {
            let nodes = self.fabric.nodes.lock().expect("DFS test nodes");
            let endpoint = nodes
                .get(target)
                .cloned()
                .ok_or_else(|| NetworkError::UnknownNode {
                    node_id: target.clone(),
                })?;
            if !endpoint.accepting.load(Ordering::Acquire) {
                return Err(NetworkError::UnknownNode {
                    node_id: target.clone(),
                });
            }
            endpoint.active.fetch_add(1, Ordering::AcqRel);
            endpoint
        };
        let mut invocation = Invocation {
            endpoint: endpoint.clone(),
            fabric: self.fabric.clone(),
            observation: observation.clone(),
            completed: false,
        };
        if !endpoint.accepting.load(Ordering::Acquire) {
            return Err(NetworkError::UnknownNode {
                node_id: target.clone(),
            });
        }
        observation.phase = RpcPhase::BeforeDispatch;
        self.fabric.record(observation.clone());
        let fault = self.fabric.fault(&observation);
        match &fault {
            Some(FaultAction::BudgetExhausted) => {
                return Err(NetworkError::delivery_failed(
                    "shared DFS test before-dispatch fault",
                    NetworkDeliveryState::DefinitelyNotSent,
                    "bounded injected carrier failure",
                ));
            }
            Some(FaultAction::PauseBeforeDispatch(gate)) => {
                let permit = gate.acquire().await.map_err(|_| {
                    NetworkError::delivery_failed(
                        "shared DFS test dispatch gate",
                        NetworkDeliveryState::DefinitelyNotSent,
                        "case or node closed",
                    )
                })?;
                permit.forget();
            }
            _ => {}
        }
        if !endpoint.accepting.load(Ordering::Acquire) {
            return Err(NetworkError::delivery_failed(
                "shared DFS test dispatch admission",
                NetworkDeliveryState::DefinitelyNotSent,
                "node closed before native dispatch",
            ));
        }
        let (incoming, request_transfer) = call.into_test_incoming(executor().handle())?;
        self.fabric
            .transfer(self.source.clone(), target.clone(), request_transfer);
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let source_state = self
            .fabric
            .nodes
            .lock()
            .expect("DFS test source identity")
            .get(&self.source)
            .map(|source| source.data_dir.clone());
        let source_data = source_state.ok_or_else(|| NetworkError::UnknownNode {
            node_id: self.source.clone(),
        })?;
        let source_state =
            sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(&source_data))?;
        let signer = ServiceSigner::Remote {
            node_id: self.source.to_string(),
            public_key: source_state
                .members
                .iter()
                .find(|member| member.node_id == self.source)
                .ok_or_else(|| NetworkError::UnknownNode {
                    node_id: self.source.clone(),
                })?
                .node_public_key
                .clone(),
        };
        let context = ServiceContext::new(
            endpoint.data_dir.clone(),
            Some(endpoint.cluster.clone()),
            signer,
            incoming.body,
            Box::new(ReplySink(response_tx)),
        );
        dispatch_started.store(true, Ordering::Release);
        endpoint
            .registry
            .dispatch(context, incoming.request)
            .await
            .map_err(|error| NetworkError::Io {
                operation: "shared DFS native service dispatch".into(),
                detail: error.to_string(),
            })?;
        let outgoing = response_rx.await.map_err(|error| NetworkError::Io {
            operation: "receive shared DFS native service response".into(),
            detail: error.to_string(),
        })?;
        observation.phase = RpcPhase::AfterDispatch;
        self.fabric.record(observation.clone());
        let (result, response_transfer) = outgoing.into_test_result(executor().handle())?;
        self.fabric
            .transfer(target.clone(), self.source.clone(), response_transfer);
        observation.phase = RpcPhase::Completed;
        self.fabric.record(observation);
        invocation.completed = true;
        Ok(result)
    }
    async fn invoke(
        &self,
        target: &NodeId,
        call: ObjectRpcCall,
        observation: RpcObservation,
    ) -> Result<ObjectRpcResult, NetworkError> {
        let dispatch_started = Arc::new(AtomicBool::new(false));
        let timeout = observation.options.overall_timeout();
        let operation = self.deliver(target, call, observation, dispatch_started.clone());
        if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, operation)
                .await
                .map_err(|_| {
                    NetworkError::timeout(
                        "shared DFS native RPC overall deadline",
                        NetworkTimeoutPhase::Overall,
                        timeout,
                        if dispatch_started.load(Ordering::Acquire) {
                            NetworkDeliveryState::PossiblyApplied
                        } else {
                            NetworkDeliveryState::DefinitelyNotSent
                        },
                    )
                })?
        } else {
            operation.await
        }
    }
}
struct ReplySink(tokio::sync::oneshot::Sender<ObjectRpcOutgoingResponse>);
impl ResponseSink for ReplySink {
    fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
        self.0
            .send(response)
            .map_err(|_| ServiceError::Service("shared DFS caller cancelled".into()))
    }
}
impl ComponentObjectRpcTransport for Carrier {
    fn call(
        &self,
        target: &NodeId,
        call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResult, NetworkError> {
        let observation = self.observation(target, &call, options, observe_connectivity);
        self.fabric.record(observation.clone());
        run_rpc(Box::pin(self.invoke(target, call, observation)))
    }
    fn call_async<'a>(
        &'a self,
        target: &'a NodeId,
        call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
        observe_connectivity: bool,
    ) -> RpcFuture<'a> {
        let observation = self.observation(target, &call, options, observe_connectivity);
        self.fabric.record(observation.clone());
        Box::pin(async move { self.invoke(target, call, observation).await })
    }
}
struct Node {
    data_dir: Arc<SinomoDataDir>,
    kv_available: Arc<AtomicBool>,
    reconciler: Option<ComponentReconciler>,
}
pub(crate) struct DfsTestRuntime {
    root: Option<sinomo_testdir::TestDir>,
    state: sinomo_core::ClusterBootstrapState,
    primary: NodeId,
    authority: Arc<dyn ClusterAccess>,
    authority_admission: Arc<tokio::sync::Mutex<()>>,
    kv_peers: Arc<crate::runtime_host::DfsTestClusterPeerBridge>,
    kv_bootstrapped: bool,
    resident_pins: BTreeMap<NodeId, sinomo_core::kv::ResidentRaftNodeRegistryPin>,
    fabric: Arc<Fabric>,
    nodes: BTreeMap<NodeId, Node>,
    closed: bool,
}
impl DfsTestRuntime {
    /// Total node count; the normal planner's root owner is the first sorted ID.
    pub(crate) fn ordered_node_ids(prefix: &str, total_number_nodes: usize) -> Vec<NodeId> {
        assert!(total_number_nodes > 0);
        let mut nodes = (0..total_number_nodes)
            .map(|index| {
                NodeId::from_legacy_named(&format!("{prefix}-{index}"))
                    .expect("normal fixture node ID")
            })
            .collect::<Vec<_>>();
        nodes.sort();
        nodes
    }
    /// Supply a deliberate protocol frame without adding a service transport type.
    /// Healthy journal/snapshot ACKs must come from the real hosted receiver.
    pub(crate) fn queue_protocol_message(
        transport: &mut DfsNodeTransport,
        source: NodeId,
        target: NodeId,
        message: DfsNetworkMessage,
    ) {
        assert!(
            matches!(
                &message,
                DfsNetworkMessage::ReadResponse(_) | DfsNetworkMessage::CommitVersion(_)
            ),
            "this shared queue hook is only for the two explicit protocol regressions"
        );
        transport.test_enqueue_protocol_message(build_dfs_routed_message(
            &source, &target, 1, 1, message,
        ));
    }

    pub(crate) fn local_transport_with_drain_counter(
        local: NodeId,
        counter: Arc<std::sync::Mutex<usize>>,
    ) -> DfsNodeTransport {
        Self::local_transport(local).test_with_drain_counter(counter)
    }

    pub(crate) fn local_transport(local: NodeId) -> DfsNodeTransport {
        crate::runtime_host::daemon_dfs_node_transport(local, Vec::<NodeId>::new())
    }
    pub(crate) fn new(name: &str, primary: NodeId) -> Result<Self, String> {
        initialize_opt_in_trace();
        let root = sinomo_testdir::TestDir::new(name);
        let data_dir = SinomoDataDir::new(root.join(primary.as_str()));
        data_dir.ensure_root_layout().map_err(|e| e.to_string())?;
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named(name).map_err(|e| e.to_string())?,
            name,
            primary.clone(),
            name,
            format!("pk-{primary}"),
            OffsetDateTime::now_utc(),
        );
        for member in &mut state.members {
            member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
            member.allowed_capabilities = member.capabilities.clone();
        }
        let paths = ClusterPaths::new(&data_dir);
        sinomo_core::cluster::write_cluster_state(&paths, &state).map_err(|e| e.to_string())?;
        let authority_admission = Arc::new(tokio::sync::Mutex::new(()));
        let kv_peers = Arc::new(crate::runtime_host::DfsTestClusterPeerBridge::default());
        let authority = crate::runtime_host::test_cluster_access_with_peers_for_runtime(
            &data_dir,
            Arc::clone(&authority_admission),
            Some(Arc::clone(&kv_peers)),
        );
        let fabric = Arc::new(Fabric {
            dropped_observations: AtomicUsize::new(0),
            exhausted_budgets: AtomicUsize::new(0),
            next_call: AtomicU64::new(1),
            nodes: Mutex::new(BTreeMap::new()),
            faults: Mutex::new(Vec::new()),
            observations: Mutex::new(Vec::new()),
            transfers: Mutex::new(Vec::new()),
        });
        let mut nodes = BTreeMap::new();
        nodes.insert(
            primary.clone(),
            Node {
                data_dir: Arc::new(data_dir),
                kv_available: Arc::new(AtomicBool::new(false)),
                reconciler: None,
            },
        );
        Ok(Self {
            root: Some(root),
            state,
            primary,
            authority,
            authority_admission,
            kv_peers,
            kv_bootstrapped: false,
            resident_pins: BTreeMap::new(),
            fabric,
            nodes,
            closed: false,
        })
    }
    pub(crate) fn define_node(&mut self, node: NodeId) -> Result<(), String> {
        if self.kv_bootstrapped {
            return Err("define the entire node roster before native KV bootstrap".into());
        }
        if self.nodes.values().any(|node| node.reconciler.is_some()) {
            return Err("define the case topology before mounting nodes".into());
        }
        if self.nodes.contains_key(&node) {
            return Err("duplicate DFS case identity".into());
        }
        let mut member = sinomo_core::ClusterMemberRecord::pending_learner(
            self.state.cluster_id.clone(),
            node.clone(),
            node.to_string(),
            format!("pk-{node}"),
            OffsetDateTime::now_utc(),
        );
        sinomo_core::node::apply_cluster_raft_role(
            &mut member,
            sinomo_core::ClusterMemberRole::Learner,
            OffsetDateTime::now_utc(),
        );
        self.state.members.push(member.clone());
        let primary_paths = ClusterPaths::new(self.data_dir(&self.primary));
        sinomo_core::cluster::write_cluster_state(&primary_paths, &self.state)
            .map_err(|e| e.to_string())?;
        let data_dir = Arc::new(SinomoDataDir::new(
            self.root.as_ref().unwrap().join(node.as_str()),
        ));
        data_dir.ensure_root_layout().map_err(|e| e.to_string())?;
        self.nodes.insert(
            node,
            Node {
                data_dir,
                kv_available: Arc::new(AtomicBool::new(false)),
                reconciler: None,
            },
        );
        Ok(())
    }
    fn prepare_projection(&mut self, node: &NodeId) -> Result<(), String> {
        let primary_paths = ClusterPaths::new(self.data_dir(&self.primary));
        let primary_state = sinomo_core::cluster::read_cluster_state_without_kv(&primary_paths)
            .map_err(|error| error.to_string())?;
        let paths = ClusterPaths::new(self.data_dir(node));
        let initialize_resident = !self.resident_pins.contains_key(node);
        if node != &self.primary {
            let mut local_state = primary_state.clone();
            local_state.local_node_id = node.clone();
            sinomo_core::cluster::write_cluster_state(&paths, &local_state)
                .map_err(|error| error.to_string())?;
            let primary = primary_state
                .members
                .iter()
                .find(|member| member.node_id == self.primary)
                .ok_or("primary membership is missing")?;
            let authenticated_primary = sinomo_core::cluster::ClusterAuthenticatedPeer {
                node_id: self.primary.to_string(),
                public_key: primary.node_public_key.clone(),
            };
            let admission = Arc::clone(&self.authority_admission);
            run_unit(Box::pin(async {
                tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async {
                    let _admission = admission.lock().await;
                    let request = primary_paths
                        .kv_paths()
                        .locked_build_raft_append_request_from_next_index_async(&primary_state, 1)
                        .await
                        .map_err(|error| error.to_string())?;
                    let response = sinomo_core::cluster::handle_cluster_raft_append_request_async(
                        executor().handle(),
                        &paths,
                        request,
                        &authenticated_primary,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                    if !response.accepted {
                        return Err(response.detail);
                    }
                    if initialize_resident {
                        paths
                            .kv_paths()
                            .locked_initialize_raft_resident(node)
                            .map_err(|error| error.to_string())?;
                    }
                    Ok(())
                })
                .await
                .map_err(|_| {
                    "real replica KV bootstrap reached its existing network deadline".to_string()
                })?
            }))?;
        } else if initialize_resident {
            paths
                .kv_paths()
                .locked_initialize_raft_resident(node)
                .map_err(|error| error.to_string())?;
        }
        if !self.resident_pins.contains_key(node) {
            let pin = sinomo_core::kv::pin_resident_raft_node_registry_for_test(
                &paths.kv_paths().raft_wal_paths(node),
            )
            .map_err(|error| error.to_string())?;
            self.resident_pins.insert(node.clone(), pin);
        }
        let node_state = &self.nodes[node];
        node_state.kv_available.store(true, Ordering::Release);
        self.kv_peers.register(
            node.clone(),
            (*node_state.data_dir).clone(),
            Arc::clone(&node_state.kv_available),
        );
        Ok(())
    }
    fn wait_native_authority(&self, node: &NodeId) -> Result<(), String> {
        let paths = ClusterPaths::new(self.data_dir(node));
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)
            .map_err(|error| error.to_string())?;
        let authority = crate::runtime_host::daemon_dfs_metadata_authority_generation_for_identity(
            &paths,
            &state.cluster_id,
            &state.local_node_id,
            Some(executor().handle()),
        )
        .map_err(|error| error.to_string())?;
        run_unit(Box::pin(async {
            tokio::time::timeout(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                authority.ensure_write_configuration_ready_async(),
            )
            .await
            .map_err(|_| {
                "native DFS configuration readiness reached its existing network deadline"
                    .to_string()
            })?
            .map_err(|error| error.to_string())
        }))
    }
    fn wait_startup_completion(&self) -> Result<(), String> {
        let mut owners = self
            .nodes
            .iter()
            .filter(|(_, node)| node.reconciler.is_some())
            .map(|(_, node)| {
                crate::default_reconciliation::subscribe_default_dfs_reconciliation_completion(
                    &ClusterPaths::new(&node.data_dir),
                )
                .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let completion = run_unit(Box::pin(async {
            tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async {
                loop {
                    for owner in &mut owners {
                        owner.wait_current_complete().await.map_err(|error| error.to_string())?;
                    }
                    if owners.iter().map(|owner| owner.is_current_complete())
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|error| error.to_string())?
                        .into_iter().all(|complete| complete) {
                        return Ok(());
                    }
                }
            }).await.map_err(|_| {
                "mounted native DFS owners did not complete their current unparked startup generation before the existing network deadline".to_string()
            })?
        }));
        if let Err(error) = completion {
            eprintln!("native DFS startup completion failed: {error}; owners={owners:?}");
            return Err(error);
        }
        for (node_id, node) in &self.nodes {
            if node.reconciler.is_some() {
                self.wait_native_authority(node_id)?;
            }
        }
        Ok(())
    }
    fn mount_node(&mut self, node: &NodeId) -> Result<(), String> {
        if self.nodes[node].reconciler.is_some() {
            return Err("duplicate live DFS test node".into());
        }
        let data_dir = self.nodes[node].data_dir.clone();
        let carrier = Arc::new(Carrier {
            source: node.clone(),
            fabric: self.fabric.clone(),
            origin: RpcOrigin::RegisteredHost,
        });
        let cluster = crate::runtime_host::test_cluster_access_for_node_on_voter(
            &data_dir,
            self.data_dir(&self.primary),
            Arc::clone(&self.authority_admission),
            Some(Arc::clone(&self.kv_peers)),
        );
        let context = ComponentContext::new(data_dir.clone())
            .with_host_role(ComponentHostRole::Embedded)
            .with_runtime(executor().handle().clone())
            .with_cluster_access(Arc::clone(&cluster))
            .with_object_rpc_transport(carrier)
            .with_event_bus(ComponentEventBus::default());
        let mut reconciler = ComponentReconciler::new(context, ServiceRegistry::default());
        let factory: Arc<dyn sinomo_module::ComponentFactory> =
            Arc::new(crate::component_factory().map_err(|e| e.to_string())?);
        reconciler
            .reconcile(&[factory])
            .map_err(|e| e.to_string())?;
        if reconciler.active_component_ids().is_empty() {
            return Err("embedded DFS component did not activate".into());
        }
        let endpoint = Arc::new(Endpoint {
            data_dir,
            cluster,
            registry: reconciler
                .service_registry()
                .active_snapshot()
                .registry()
                .clone(),
            accepting: AtomicBool::new(false),
            active: AtomicUsize::new(0),
            idle: Notify::new(),
        });
        self.fabric
            .nodes
            .lock()
            .unwrap()
            .insert(node.clone(), endpoint);
        self.nodes.get_mut(node).unwrap().reconciler = Some(reconciler);
        Ok(())
    }
    fn ready_node(&mut self, node: &NodeId) -> Result<(), String> {
        let reconciler = self
            .nodes
            .get_mut(node)
            .unwrap()
            .reconciler
            .as_mut()
            .unwrap();
        reconciler.notify_host_start();
        reconciler
            .prepare_host_on_runtime(Some(executor().handle()))
            .map_err(|e| e.to_string())?;
        reconciler.notify_host_ready();
        self.fabric.nodes.lock().unwrap()[node]
            .accepting
            .store(true, Ordering::Release);
        Ok(())
    }
    fn bootstrap_planned_cluster(&mut self) -> Result<(), String> {
        if self.kv_bootstrapped {
            return Ok(());
        }
        let paths = ClusterPaths::new(self.data_dir(&self.primary));
        // Match product initialization: declare the full initial partition,
        // establish native initial leadership, then commit its checked seed.
        sinomo_core::cluster::write_cluster_state(&paths, &self.state)
            .map_err(|error| error.to_string())?;
        paths
            .kv_paths()
            .locked_set_raft_leader_compact(&self.primary, self.state.term)
            .map_err(|error| error.to_string())?;
        sinomo_core::cluster::bootstrap_single_voter_cluster_state_to_local_kv(&paths, &self.state)
            .map_err(|error| error.to_string())?;
        self.kv_bootstrapped = true;
        Ok(())
    }

    fn seed_default_configuration(&self) -> Result<(), String> {
        let data_dir = self.data_dir(&self.primary).clone();
        let paths = ClusterPaths::new(&data_dir);
        let control = crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(
            &paths,
            executor().handle(),
        );
        let file_system = self.file_system();
        let state = self.state.clone();
        let primary = self.primary.clone();
        run_unit(Box::pin(async move {
            tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async move {
                let mut host = DfsControlPlaneDefaultClusterAsyncBootstrapHost::new_with_resident_metadata_backend(
                    data_dir, paths, primary, Box::new(control), None,
                );
                let shards = default_cluster_metadata_shards_with_async_host(
                    &mut host, &state, &file_system,
                ).await.map_err(|error| error.to_string())?;
                host.put_file_system_config_async(DfsFileSystemConfig::from(&file_system))
                    .await.map_err(|error| error.to_string())?;
                for shard in shards {
                    host.put_metadata_shard_async(shard).await.map_err(|error| error.to_string())?;
                }
                Ok(())
            }).await.map_err(|_| "normal DFS initial planning reached its existing network deadline".to_string())?
        }))
    }

    pub(crate) fn start_nodes(&mut self) -> Result<(), String> {
        self.bootstrap_planned_cluster()?;
        let ids: Vec<_> = self.nodes.keys().cloned().collect();
        self.prepare_projection(&self.primary.clone())?;
        // Establish the primary's actual current-term ReadIndex before copying
        // its accepted entries to non-voters; this is not a synthetic certificate.
        run_unit(Box::pin(async {
            tokio::time::timeout(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                self.authority
                    .kv_range_prefix_authoritative_async("/dfs/filesystems/".into()),
            )
            .await
            .map_err(|_| {
                "initial real ReadIndex reached its existing network deadline".to_string()
            })?
            .map(|_| ())
            .map_err(|error| error.to_string())
        }))?;
        for id in &ids {
            if id != &self.primary {
                self.prepare_projection(id)?;
            }
        }
        for id in &ids {
            self.mount_node(id)?;
        }
        self.seed_default_configuration()?;
        // Initial replicas already own native resident Raft nodes. Let the
        // actual committed fanout deliver the planned DFS entries, without
        // racing a second full-log bootstrap/resident initialization against it.
        run_unit(Box::pin(
            self.kv_peers
                .join(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
        ))?;
        // Join native cold authority while component background work is still
        // behind the normal host-prepare edge, then publish normal readiness.
        for id in &ids {
            self.wait_native_authority(id)?;
        }
        for id in &ids {
            self.ready_node(id)?;
        }
        self.wait_startup_completion()
    }
    pub(crate) fn data_dir(&self, node: &NodeId) -> &SinomoDataDir {
        &self.nodes[node].data_dir
    }
    pub(crate) fn file_system(&self) -> DfsFileSystem {
        default_cluster_sifs(&self.state)
    }
    pub(crate) fn root_shard(&self) -> Result<DfsMetadataShardConfig, String> {
        let paths = ClusterPaths::new(self.data_dir(&self.primary));
        let mut control = crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(
            &paths,
            executor().handle(),
        );
        DfsControlPlaneStore::new(&mut control)
            .get_metadata_shard(&sifs_file_system_id(), "root")
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "normal initial planner did not commit a root shard".into())
    }
    pub(crate) fn client(
        &self,
        node: &NodeId,
    ) -> Result<NetworkedDfsService<DfsNodeTransport>, String> {
        if self.nodes[node].reconciler.is_none() {
            return Err("DFS case node is stopped".into());
        }
        self.wait_startup_completion()?;
        crate::runtime_host::open_dfs_runtime_service(
            self.data_dir(node),
            Some(executor().handle()),
        )
        .map_err(|e| e.to_string())
    }
    pub(crate) fn rpc_request(
        &self,
        source: &NodeId,
        target: &NodeId,
        request: ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, String> {
        let deadline = std::time::Instant::now() + timeout;
        let carrier = Carrier {
            source: source.clone(),
            fabric: self.fabric.clone(),
            origin: RpcOrigin::ExplicitFixtureRpc,
        };
        let result = carrier
            .call(
                target,
                ObjectRpcCall::value(request),
                DfsObjectRpcSendOptions::default()
                    .with_overall_timeout(timeout)
                    .cluster_rpc_options(),
                true,
            )
            .map_err(|e| e.to_string())?;
        run_rpc(Box::pin(async {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let response = tokio::time::timeout(
                remaining,
                result.into_materialized_value_response(16 * 1024 * 1024),
            )
            .await
            .map_err(|_| {
                NetworkError::timeout(
                    "shared DFS fixture response body deadline",
                    NetworkTimeoutPhase::Overall,
                    timeout,
                    NetworkDeliveryState::PossiblyApplied,
                )
            })??;
            Ok(ObjectRpcResult::value(response))
        }))
        .map(|result| result.response)
        .map_err(|e| e.to_string())
    }
    pub(crate) fn install_fault(&self, rule: FaultRule) {
        assert!(
            (1..=32).contains(&rule.remaining_hits),
            "bounded fault rule required"
        );
        let mut rules = self.fabric.faults.lock().unwrap();
        assert!(rules.len() < 64);
        rules.push(rule)
    }
    pub(crate) fn assert_fault_budgets(&self) -> Result<(), String> {
        if self.fabric.exhausted_budgets.load(Ordering::Relaxed) == 0 {
            Ok(())
        } else {
            Err(
                "bounded DFS test fault budget exhausted; matching traffic stayed failed closed"
                    .into(),
            )
        }
    }
    pub(crate) fn observation_dropped_count(&self) -> usize {
        self.fabric.dropped_observations.load(Ordering::Relaxed)
    }
    pub(crate) fn stop_node(&mut self, node: &NodeId) -> Result<(), String> {
        self.wait_startup_completion()?;
        run_unit(Box::pin(self.kv_peers.join(CLOSE_TIMEOUT)))?;
        self.stop_node_inner(node)
    }
    fn stop_node_inner(&mut self, node: &NodeId) -> Result<(), String> {
        self.nodes[node]
            .kv_available
            .store(false, Ordering::Release);
        let endpoint = {
            let nodes = self.fabric.nodes.lock().unwrap();
            let endpoint = nodes
                .get(node)
                .cloned()
                .ok_or_else(|| "unknown live DFS test node".to_string())?;
            endpoint.accepting.store(false, Ordering::Release);
            endpoint
        };
        for rule in self.fabric.faults.lock().unwrap().iter() {
            if rule.target.as_ref().is_none_or(|id| id == node)
                || rule.source.as_ref().is_none_or(|id| id == node)
            {
                if let FaultAction::PauseBeforeDispatch(gate) = &rule.action {
                    gate.close()
                }
            }
        }
        let node_state = self
            .nodes
            .get_mut(node)
            .ok_or("missing owned DFS test node")?;
        run_unit(Box::pin(async {
            tokio::time::timeout(CLOSE_TIMEOUT, async {
                while endpoint.active.load(Ordering::Acquire) != 0 {
                    let notified = endpoint.idle.notified();
                    if endpoint.active.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    notified.await;
                }
            })
            .await
            .map_err(|_| "DFS native ingress did not drain".to_string())?;
            node_state
                .reconciler
                .as_mut()
                .unwrap()
                .stop_tasks_and_wait(CLOSE_TIMEOUT)
                .await
                .map_err(|e| e.to_string())?;
            crate::runtime_host::join_registered_runtime_for_test(
                &node_state.data_dir,
                CLOSE_TIMEOUT,
            )
            .await
        }))?;
        self.join_transfers(Some(node))?;
        run_unit(Box::pin(async {
            self.nodes
                .get_mut(node)
                .unwrap()
                .reconciler
                .as_mut()
                .unwrap()
                .shutdown_and_wait(CLOSE_TIMEOUT)
                .await
                .map_err(|e| e.to_string())
        }))?;
        self.fabric.nodes.lock().unwrap().remove(node);
        self.nodes.get_mut(node).unwrap().reconciler.take();
        // Every source component/host producer for this node is stopped. Join
        // any wave queued after the pre-stop drain before exposing its storage.
        run_unit(Box::pin(self.kv_peers.join(CLOSE_TIMEOUT)))?;
        Ok(())
    }
    pub(crate) fn reopen_node(&mut self, node: NodeId) -> Result<(), String> {
        self.prepare_projection(&node)?;
        self.mount_node(&node)?;
        self.wait_native_authority(&node)?;
        self.ready_node(&node)?;
        self.wait_startup_completion()
    }
    pub(crate) fn close(mut self) -> Result<(), String> {
        self.close_inner()
    }
    fn join_transfers(&self, node: Option<&NodeId>) -> Result<(), String> {
        let all = std::mem::take(&mut *self.fabric.transfers.lock().unwrap());
        let (mut transfers, other): (Vec<_>, Vec<_>) = all.into_iter().partition(|transfer| {
            node.is_none_or(|id| &transfer.source == id || &transfer.target == id)
        });
        let result = run_unit(Box::pin(async {
            while let Some(task) = transfers.last_mut() {
                let result = tokio::time::timeout(CLOSE_TIMEOUT, &mut task.task).await;
                match result {
                    Ok(Ok(Ok(()))) => {
                        transfers.pop();
                    }
                    Ok(Ok(Err(error))) => {
                        transfers.pop();
                        return Err(error.to_string());
                    }
                    Ok(Err(error)) if error.is_cancelled() => {
                        transfers.pop();
                    }
                    Ok(Err(error)) => {
                        transfers.pop();
                        return Err(error.to_string());
                    }
                    Err(_) => {
                        for transfer in &transfers {
                            transfer.task.abort()
                        }
                        return Err("DFS body transfer did not finish".into());
                    }
                }
            }
            Ok(())
        }));
        let mut owned = self.fabric.transfers.lock().unwrap();
        owned.extend(other);
        owned.extend(transfers);
        drop(owned);
        result
    }
    fn close_inner(&mut self) -> Result<(), String> {
        run_unit(Box::pin(self.kv_peers.join(CLOSE_TIMEOUT)))?;
        let ids: Vec<_> = self
            .nodes
            .iter()
            .filter(|(_, node)| node.reconciler.is_some())
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            self.stop_node_inner(&id)?
        }
        // No live component/host producer remains to enqueue a later wave.
        run_unit(Box::pin(self.kv_peers.join(CLOSE_TIMEOUT)))?;
        self.join_transfers(None)?;
        self.assert_fault_budgets()?;
        if self.fabric.dropped_observations.load(Ordering::Relaxed) != 0 {
            return Err("DFS test observations were censored".into());
        }
        self.closed = true;
        Ok(())
    }
}
impl Drop for DfsTestRuntime {
    fn drop(&mut self) {
        if !self.closed {
            if let Err(error) = self.close_inner() {
                eprintln!("shared DFS test cleanup failed: {error}");
                self.kv_peers.retain_after_failed_shutdown();
                for transfer in self.fabric.transfers.lock().unwrap().drain(..) {
                    transfer.task.abort()
                }
                if let Some(root) = self.root.take() {
                    eprintln!("retained DFS test root: {}", root.keep().display())
                }
            }
        }
    }
}

// Counts this client's synchronous txn API calls, not other runtime clients.
#[derive(Clone, Debug, Default)]
pub(crate) struct DfsTestControlPlaneProbe {
    calls: Arc<AtomicUsize>,
}
impl DfsTestControlPlaneProbe {
    pub(crate) fn txn_calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct ProbedNormalControlPlane {
    inner: Box<dyn DfsControlPlaneKvClient>,
    probe: DfsTestControlPlaneProbe,
}
impl DfsControlPlaneKvClient for ProbedNormalControlPlane {
    fn async_client(&mut self) -> Option<&mut dyn AsyncDfsControlPlaneKvClient> {
        // Preserve the actual native async view/capability and its futures.
        // Async transactions are deliberately outside this sync probe's count.
        self.inner.async_client()
    }
    fn write_configuration_projection(
        &self,
    ) -> Result<Option<Arc<crate::DfsWriteConfigurationProjection>>, sinomo_core::kv::KvError> {
        self.inner.write_configuration_projection()
    }
    fn write_configuration_readiness(
        &self,
    ) -> Result<crate::DfsWriteConfigurationReadiness, sinomo_core::kv::KvError> {
        self.inner.write_configuration_readiness()
    }
    fn refresh_latest(&mut self) -> Result<(), sinomo_core::kv::KvError> {
        self.inner.refresh_latest()
    }
    fn placement_authority_process_identity(&self) -> Option<DfsPlacementAuthorityProcessIdentity> {
        self.inner.placement_authority_process_identity()
    }
    fn authorizes_prior_process_placement_authority_recovery(
        &self,
        local_node_id: &NodeId,
        current_process: &DfsPlacementAuthorityProcessIdentity,
    ) -> bool {
        self.inner
            .authorizes_prior_process_placement_authority_recovery(local_node_id, current_process)
    }
    fn get(&self, key: &str) -> Result<Option<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
        self.inner.get(key)
    }
    fn get_consistent(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<sinomo_core::kv::KvEntry>>, sinomo_core::kv::KvError> {
        self.inner.get_consistent(keys)
    }
    fn get_authoritative_consistent(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<Option<sinomo_core::kv::KvEntry>>, sinomo_core::kv::KvError> {
        self.inner.get_authoritative_consistent(keys)
    }
    fn get_authoritative_consistent_before(
        &mut self,
        keys: &[String],
        deadline: std::time::Instant,
    ) -> Result<Vec<Option<sinomo_core::kv::KvEntry>>, sinomo_core::kv::KvError> {
        self.inner
            .get_authoritative_consistent_before(keys, deadline)
    }
    fn range_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
        self.inner.range_prefix(prefix)
    }
    fn cached_metadata_shards(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<Vec<DfsMetadataShardConfig>>, sinomo_core::kv::KvError> {
        self.inner.cached_metadata_shards(file_system_id)
    }
    fn range_prefix_authoritative(
        &mut self,
        prefix: &str,
    ) -> Result<crate::control_plane::DfsAuthoritativePrefixProjection, sinomo_core::kv::KvError>
    {
        self.inner.range_prefix_authoritative(prefix)
    }
    fn put(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> Result<sinomo_core::kv::KvRevision, sinomo_core::kv::KvError> {
        self.inner.put(key, value)
    }
    fn txn(
        &mut self,
        txn: sinomo_core::kv::KvTxn,
    ) -> Result<sinomo_core::kv::KvTxnResponse, sinomo_core::kv::KvError> {
        self.probe.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.txn(txn)
    }
    // The actual normal DaemonDfsControlPlaneKvClient (including its gated
    // normal view) uses the trait default linearizable_compare_value. Retain
    // that identical default so its one fresh barrier goes through counted txn.
}

impl DfsTestRuntime {
    pub(crate) fn client_with_control_plane_probe(
        &self,
        node: &NodeId,
    ) -> Result<
        (
            NetworkedDfsService<DfsNodeTransport>,
            DfsTestControlPlaneProbe,
        ),
        String,
    > {
        let mut client = self.client(node)?;
        let inner = client
            .control_plane
            .take()
            .ok_or("normal client has no control plane")?;
        let probe = DfsTestControlPlaneProbe::default();
        client.control_plane = Some(Box::new(ProbedNormalControlPlane {
            inner,
            probe: probe.clone(),
        }));
        Ok((client, probe))
    }
}

#[cfg(test)]
mod pilot;
