//! DFS clients that hand work to the authoritative local daemon.
use crate::*;
use sinomo_api::transport::*;
use sinomo_api::*;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{ObjectRpcCall, ObjectRpcRequest, ObjectRpcResponse, ObjectRpcResult};
use sinomo_core::node::NodeId;
use std::time::Duration;

pub use crate::{
    DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES as DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES,
    DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE as DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE,
};
/// Normal whole-call budget for the local mounted-mutation durability handoff.
pub const DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Largest caller-selected whole-call budget. Every request retains an
/// absolute deadline even while authenticated keepalives show transport
/// progress.
pub const DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_TIMEOUT: Duration = Duration::from_secs(30);

/// Explicit local-command client used by separately supervised filesystem
/// mount processes to transfer mutation ownership to the main daemon.
///
/// This client never discovers a data directory from the process environment:
/// the mount supervisor must provide both the directory and the daemon address
/// it launched against. Only a loopback address is accepted, so the durable
/// marker protocol cannot be exposed as an ordinary routed cluster RPC.
#[derive(Clone, Debug)]
pub struct DaemonDfsMountedMutationHandoffClient {
    data_dir: SinomoDataDir,
    address: String,
    timeout: Duration,
}

impl DaemonDfsMountedMutationHandoffClient {
    pub fn new(
        data_dir: SinomoDataDir,
        address: impl Into<String>,
    ) -> Result<Self, DaemonApiError> {
        data_dir.ensure_root_layout()?;
        let address = address.into();
        if !sinomo_core::network::is_loopback_connect_address(&address) {
            return Err(DaemonApiError::Network {
                address,
                detail:
                    "mounted-mutation handoff requires an explicit main-daemon loopback address"
                        .to_string(),
            });
        }
        Ok(Self {
            data_dir,
            address,
            timeout: DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_DEFAULT_TIMEOUT,
        })
    }

    /// Override the one whole-call budget while retaining a finite upper
    /// bound. The same duration also bounds connection establishment and every
    /// interval without stream progress.
    pub fn with_timeout(mut self, timeout: Duration) -> Result<Self, DaemonApiError> {
        if timeout.is_zero() || timeout > DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_TIMEOUT {
            return Err(DaemonApiError::Parse {
                detail: format!(
                    "mounted-mutation handoff timeout must be greater than zero and no more than {:?}",
                    DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_TIMEOUT
                ),
            });
        }
        self.timeout = timeout;
        Ok(self)
    }

    pub fn data_dir(&self) -> &SinomoDataDir {
        &self.data_dir
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn probe(&self) -> Result<DaemonDfsMountedMutationHandoffResponse, DaemonApiError> {
        let response = self.call(DaemonDfsMountedMutationHandoffRequest::Probe)?;
        validate_daemon_dfs_mounted_mutation_handoff_response(
            response,
            crate::DaemonDfsMountedMutationHandoffPhase::Ready,
            None,
            None,
        )
    }

    /// Older daemons only implement the more restrictive installation-wide probe.
    pub fn probe_fuse(&self) -> Result<DaemonDfsMountedMutationHandoffResponse, DaemonApiError> {
        let response = match self.call(DaemonDfsMountedMutationHandoffRequest::ProbeFuse) {
            Ok(response) => response,
            // A legacy Ready reply requires an entirely clean outbox. It
            // cannot admit anything that the FUSE-specific probe would fence.
            Err(DaemonApiError::RemoteService { .. }) => return self.probe(),
            Err(error) => return Err(error),
        };
        validate_daemon_dfs_mounted_mutation_handoff_response(
            response,
            crate::DaemonDfsMountedMutationHandoffPhase::Ready,
            None,
            None,
        )
    }

    /// Durably arm a prepared marker. A successful return is the authoritative
    /// fsynced precondition which permits the caller to enter the mutation.
    pub fn arm(
        &self,
        marker_id: crate::MountedSifsMutationMarkerId,
        marker_nonce: crate::MountedSifsMutationNonce,
        arm_request_id: crate::MountedSifsMutationArmRequestId,
    ) -> Result<DaemonDfsMountedMutationHandoffResponse, DaemonApiError> {
        let expected_marker_id = marker_id.clone();
        let expected_arm_request_id = arm_request_id.clone();
        let response = self.call(DaemonDfsMountedMutationHandoffRequest::Arm {
            marker_id,
            marker_nonce,
            arm_request_id,
        })?;
        validate_daemon_dfs_mounted_mutation_handoff_response(
            response,
            crate::DaemonDfsMountedMutationHandoffPhase::Armed,
            Some(&expected_marker_id),
            Some(&expected_arm_request_id),
        )
    }

    pub fn commit_applied(
        &self,
        marker_id: crate::MountedSifsMutationMarkerId,
        marker_nonce: crate::MountedSifsMutationNonce,
        arm_request_id: crate::MountedSifsMutationArmRequestId,
        captured_work: crate::MountedSifsMutationCapturedWork,
    ) -> Result<DaemonDfsMountedMutationHandoffResponse, DaemonApiError> {
        let expected_marker_id = marker_id.clone();
        let expected_arm_request_id = arm_request_id.clone();
        let response = self.call(DaemonDfsMountedMutationHandoffRequest::CommitApplied {
            marker_id,
            marker_nonce,
            arm_request_id,
            captured_work,
        })?;
        validate_daemon_dfs_mounted_mutation_handoff_response(
            response,
            crate::DaemonDfsMountedMutationHandoffPhase::DaemonAccepted,
            Some(&expected_marker_id),
            Some(&expected_arm_request_id),
        )
    }

    /// Preserve an ambiguous mutation error together with every exact work bit
    /// captured before the error. This transition deliberately
    /// has no convenience path which substitutes an empty capture.
    pub fn commit_unknown(
        &self,
        marker_id: crate::MountedSifsMutationMarkerId,
        marker_nonce: crate::MountedSifsMutationNonce,
        arm_request_id: crate::MountedSifsMutationArmRequestId,
        observation: crate::MountedSifsMutationErrorObservation,
    ) -> Result<DaemonDfsMountedMutationHandoffResponse, DaemonApiError> {
        let expected_marker_id = marker_id.clone();
        let expected_arm_request_id = arm_request_id.clone();
        let response = self.call(DaemonDfsMountedMutationHandoffRequest::CommitUnknown {
            marker_id,
            marker_nonce,
            arm_request_id,
            observation,
        })?;
        validate_daemon_dfs_mounted_mutation_handoff_response(
            response,
            crate::DaemonDfsMountedMutationHandoffPhase::DaemonAccepted,
            Some(&expected_marker_id),
            Some(&expected_arm_request_id),
        )
    }

    pub fn prove_no_write(
        &self,
        marker_id: crate::MountedSifsMutationMarkerId,
        marker_nonce: crate::MountedSifsMutationNonce,
        arm_request_id: crate::MountedSifsMutationArmRequestId,
        proof_code: impl Into<String>,
    ) -> Result<DaemonDfsMountedMutationHandoffResponse, DaemonApiError> {
        let expected_marker_id = marker_id.clone();
        let expected_arm_request_id = arm_request_id.clone();
        let response = self.call(DaemonDfsMountedMutationHandoffRequest::ProveNoWrite {
            marker_id,
            marker_nonce,
            arm_request_id,
            proof_code: proof_code.into(),
        })?;
        validate_daemon_dfs_mounted_mutation_handoff_response(
            response,
            crate::DaemonDfsMountedMutationHandoffPhase::ProvenNoWrite,
            Some(&expected_marker_id),
            Some(&expected_arm_request_id),
        )
    }

    /// Abort only a marker whose mutation was positively never started. The
    /// daemon rejects an armed or later marker rather than accepting an empty
    /// work receipt.
    pub fn abort_unstarted(
        &self,
        marker_id: crate::MountedSifsMutationMarkerId,
        marker_nonce: crate::MountedSifsMutationNonce,
    ) -> Result<DaemonDfsMountedMutationHandoffResponse, DaemonApiError> {
        let expected_marker_id = marker_id.clone();
        let response = self.call(DaemonDfsMountedMutationHandoffRequest::AbortUnstarted {
            marker_id,
            marker_nonce,
        })?;
        validate_daemon_dfs_mounted_mutation_handoff_response(
            response,
            crate::DaemonDfsMountedMutationHandoffPhase::AbortedUnstarted,
            Some(&expected_marker_id),
            None,
        )
    }

    fn call(
        &self,
        request: DaemonDfsMountedMutationHandoffRequest,
    ) -> Result<DaemonDfsMountedMutationHandoffResponse, DaemonApiError> {
        let call = daemon_dfs_mounted_mutation_handoff_call(&request)?;
        let data_dir = self.data_dir.clone();
        let address = self.address.clone();
        let timeout = crate::MountedSifsCall::deadline().map_or(self.timeout, |deadline| {
            self.timeout
                .min(deadline.saturating_duration_since(std::time::Instant::now()))
        });
        if timeout.is_zero() {
            return Err(DaemonApiError::Parse {
                detail: "mounted filesystem operation deadline expired".into(),
            });
        }
        block_on_daemon_client(Box::pin(async move {
            let runtime = tokio::runtime::Handle::current();
            let deadline = daemon_api_overall_deadline(Some(timeout));
            let options = daemon_dfs_mounted_mutation_handoff_options(timeout);
            run_daemon_api_with_overall_deadline(
                "mounted-mutation main-daemon handoff",
                deadline,
                Box::pin(async move {
                    call_daemon_address_rpc_async(data_dir, &address, call, options, &runtime)
                        .await?
                        .decode_body()
                        .await
                        .map_err(Into::into)
                }),
            )
            .await
        }))
    }
}

fn daemon_dfs_mounted_mutation_handoff_options(
    timeout: Duration,
) -> sinomo_core::network::ClusterObjectRpcOptions {
    sinomo_core::network::ClusterObjectRpcOptions::new(
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
    )
    .with_overall_timeout(timeout)
    .with_destination_process_affinity(sinomo_core::network::DestinationProcessAffinity::MainDaemon)
}

fn daemon_dfs_mounted_mutation_handoff_call(
    request: &DaemonDfsMountedMutationHandoffRequest,
) -> Result<ObjectRpcCall, DaemonApiError> {
    request
        .validate()
        .map_err(|detail| DaemonApiError::Parse { detail })?;
    let replayable = ObjectRpcCall::encode_replayable_value(
        DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE,
        DAEMON_RPC_CATEGORY,
        request,
    )?;
    let encoded_bytes = replayable.encoded_value_bytes();
    if encoded_bytes > DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES {
        return Err(DaemonApiError::Parse {
            detail: format!(
                "mounted-mutation handoff request is {encoded_bytes} encoded bytes; max is {DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES}"
            ),
        });
    }
    let mut call = replayable.call();
    call.request = call.request.with_response_retry_safe();
    if let Some(operation_id) = daemon_dfs_mounted_mutation_handoff_operation_id(request) {
        call.request = call.request.with_operation_id(operation_id);
    }
    Ok(call)
}

fn daemon_dfs_mounted_mutation_handoff_operation_id(
    request: &DaemonDfsMountedMutationHandoffRequest,
) -> Option<&str> {
    match request {
        DaemonDfsMountedMutationHandoffRequest::Probe
        | DaemonDfsMountedMutationHandoffRequest::ProbeFuse => None,
        DaemonDfsMountedMutationHandoffRequest::Arm { arm_request_id, .. }
        | DaemonDfsMountedMutationHandoffRequest::CommitApplied { arm_request_id, .. }
        | DaemonDfsMountedMutationHandoffRequest::CommitUnknown { arm_request_id, .. }
        | DaemonDfsMountedMutationHandoffRequest::ProveNoWrite { arm_request_id, .. } => {
            Some(arm_request_id.as_str())
        }
        DaemonDfsMountedMutationHandoffRequest::AbortUnstarted { marker_id, .. } => {
            Some(marker_id.as_str())
        }
    }
}

fn validate_daemon_dfs_mounted_mutation_handoff_response(
    response: DaemonDfsMountedMutationHandoffResponse,
    expected_phase: crate::DaemonDfsMountedMutationHandoffPhase,
    expected_marker_id: Option<&sinomo_core::Id>,
    expected_arm_request_id: Option<&sinomo_core::Id>,
) -> Result<DaemonDfsMountedMutationHandoffResponse, DaemonApiError> {
    if response.phase != expected_phase
        || response.marker_id.as_ref() != expected_marker_id
        || response.arm_request_id.as_ref() != expected_arm_request_id
    {
        return Err(DaemonApiError::Parse {
            detail: format!(
                "mounted-mutation handoff returned mismatched acknowledgement: phase={:?}, marker_id={:?}, arm_request_id={:?}",
                response.phase, response.marker_id, response.arm_request_id
            ),
        });
    }
    Ok(response)
}

/// DFS peer transport that deliberately terminates at the running local
/// daemon. The daemon owns endpoint selection, retries, authentication, and
/// cluster streams; mounted clients never open a competing cluster
/// router or fall back to direct advertised endpoints.
#[derive(Clone, Debug)]
pub struct DaemonDfsPeerTransport {
    data_dir: SinomoDataDir,
    address: Option<String>,
    deferred_cluster_id: Option<sinomo_core::node::ClusterId>,
    local_node_id: NodeId,
    connected_process_affinity: sinomo_api::DaemonObjectRpcDestinationProcessAffinity,
    runtime: std::sync::Arc<tokio::runtime::Runtime>,
}

impl DaemonDfsPeerTransport {
    /// Namespace captured by a deferred requester. This is identity data,
    /// not proof that Main is currently available or that an effect is admitted.
    pub fn captured_cluster_id(&self) -> Option<&sinomo_core::node::ClusterId> {
        self.deferred_cluster_id.as_ref()
    }

    /// Discovers and probes the main daemon for `data_dir` once, when a mounted
    /// service is opened. Clustered mounts fail closed when no local daemon is
    /// available instead of silently changing transport semantics.
    pub fn connect(data_dir: SinomoDataDir) -> Result<Self, DaemonApiError> {
        data_dir.ensure_root_layout()?;
        let client = DaemonApiClient::connect_for_paths(&DaemonPaths::new(&data_dir))?;
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let local_node_id =
            sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?.local_node_id;
        let runtime = daemon_dfs_peer_runtime()?;
        Ok(Self {
            data_dir,
            address: Some(client.address().to_owned()),
            deferred_cluster_id: None,
            local_node_id,
            connected_process_affinity:
                sinomo_api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
            runtime,
        })
    }

    /// Observational caller construction. The saved node/cluster identify the
    /// caller, not an available or authorized Main. Resolve and verify Main at
    /// each invocation, including after an initially unavailable daemon starts.
    pub fn deferred(data_dir: SinomoDataDir) -> Result<Self, DaemonApiError> {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(
            &sinomo_core::network::ClusterPaths::new(&data_dir),
        )?;
        Ok(Self {
            data_dir,
            address: None,
            deferred_cluster_id: Some(state.cluster_id),
            local_node_id: state.local_node_id,
            connected_process_affinity:
                sinomo_api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
            runtime: daemon_dfs_peer_runtime()?,
        })
    }

    async fn resolve_main(
        &self,
        timeout: Duration,
        runtime: &tokio::runtime::Handle,
    ) -> Result<String, DaemonApiError> {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(
            &sinomo_core::network::ClusterPaths::new(&self.data_dir),
        )?;
        if Some(&state.cluster_id) != self.deferred_cluster_id.as_ref()
            || state.local_node_id != self.local_node_id
        {
            return Err(DaemonApiError::NetworkDelivery {
                detail: "deferred DFS caller node/cluster identity changed before dispatch".into(),
                delivery_state: sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
            });
        }
        match probe_daemon_status_async(&DaemonPaths::new(&self.data_dir), timeout.min(DAEMON_API_PROBE_TIMEOUT), runtime).await {
            DaemonProbeStatus::Running(status) if status.cluster.as_ref().is_some_and(|cluster|
                cluster.cluster_id == state.cluster_id && cluster.node_id == self.local_node_id
                    && cluster.process == sinomo_core::node::DAEMON_MAIN_CLUSTER_ROLE) => Ok(status.address),
            DaemonProbeStatus::Running(_) => Err(DaemonApiError::NetworkDelivery {
                detail: "deferred DFS transport discovered another node, cluster or process instead of its Main".into(),
                delivery_state: sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
            }),
            _ => Err(DaemonApiError::NotRunning {
                detail: format!("authenticated Main is unavailable for {}", self.data_dir.root().display()),
            }),
        }
    }

    #[cfg(test)]
    pub fn at_address(
        data_dir: SinomoDataDir,
        address: impl Into<String>,
        local_node_id: NodeId,
    ) -> Self {
        Self::at_address_with_process_affinity(
            data_dir,
            address,
            local_node_id,
            sinomo_api::DaemonObjectRpcDestinationProcessAffinity::Any,
        )
    }

    #[cfg(test)]
    fn at_address_with_process_affinity(
        data_dir: SinomoDataDir,
        address: impl Into<String>,
        local_node_id: NodeId,
        connected_process_affinity: sinomo_api::DaemonObjectRpcDestinationProcessAffinity,
    ) -> Self {
        Self {
            data_dir,
            address: Some(address.into()),
            deferred_cluster_id: None,
            local_node_id,
            connected_process_affinity,
            runtime: daemon_dfs_peer_runtime().expect("daemon DFS peer runtime"),
        }
    }

    async fn call_rpc_inner(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        mut options: crate::DfsObjectRpcSendOptions,
        runtime: &tokio::runtime::Handle,
    ) -> Result<ObjectRpcResult, DaemonApiError> {
        let deadline = daemon_api_overall_deadline(options.overall_timeout);
        if deadline.is_some_and(|(at, _)| tokio::time::Instant::now() >= at) {
            return Err(DaemonApiError::NetworkDelivery {
                detail: "DFS operation deadline expired before Main discovery".into(),
                delivery_state: sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
            });
        }
        let address = match &self.address {
            Some(address) => address.clone(),
            None => {
                let resolve = async {
                    self.resolve_main(options.connect_timeout, runtime)
                        .await
                        .map_err(|error| DaemonApiError::NetworkDelivery {
                            detail: error.to_string(),
                            delivery_state:
                                sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                        })
                };
                if let Some((deadline, _)) = deadline {
                    tokio::time::timeout_at(deadline, resolve)
                        .await.map_err(|_| DaemonApiError::NetworkDelivery {
                            detail: "DFS operation deadline expired during Main discovery before dispatch".into(),
                            delivery_state: sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                        })??
                } else {
                    resolve.await?
                }
            }
        };
        if let Some((deadline, _)) = deadline {
            options.overall_timeout = Some(
                deadline
                    .checked_duration_since(tokio::time::Instant::now())
                    .filter(|remaining| !remaining.is_zero())
                    .ok_or_else(|| DaemonApiError::NetworkDelivery {
                        detail: "DFS operation deadline expired before Main dispatch".into(),
                        delivery_state:
                            sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                    })?,
            );
        }
        let send = send_daemon_object_rpc_call_with_affinity_async(
            self.data_dir.clone(),
            &address,
            DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
            target_node_id,
            call,
            options.connect_timeout,
            None,
            options.overall_timeout,
            options.observe_connectivity,
            sinomo_api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
            sinomo_core::network::DaemonRpcTimeouts::default(),
            Some(&self.local_node_id),
            Some(self.connected_process_affinity),
            runtime,
        );
        run_daemon_api_with_overall_deadline("DFS Main call", deadline, Box::pin(send)).await
    }
}

fn daemon_dfs_peer_runtime() -> Result<std::sync::Arc<tokio::runtime::Runtime>, DaemonApiError> {
    static SHARED: std::sync::OnceLock<Result<std::sync::Arc<tokio::runtime::Runtime>, String>> =
        std::sync::OnceLock::new();
    SHARED
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("sinomo-daemon-dfs-peer")
                .enable_all()
                .build()
                .map(std::sync::Arc::new)
                .map_err(|source| source.to_string())
        })
        .clone()
        .map_err(|detail| DaemonApiError::Network {
            address: "daemon DFS peer runtime".to_string(),
            detail,
        })
}

impl crate::DfsPeerTransport for DaemonDfsPeerTransport {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: crate::DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, crate::DfsPeerTransportError> {
        let runtime = self.runtime.handle().clone();
        block_on_daemon_client_on_owner_runtime(
            &runtime,
            Box::pin(async {
                self.call_rpc_inner(target_node_id, call, options, &runtime)
                    .await
            }),
        )
        .and_then(|result| {
            result.response.error_detail()?;
            Ok(result)
        })
        .map_err(daemon_api_dfs_peer_transport_error)
    }
}

impl crate::AsyncDfsPeerTransport for DaemonDfsPeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcCall,
        options: crate::DfsObjectRpcSendOptions,
    ) -> crate::DfsPeerTransportFuture<'a, Result<ObjectRpcResult, crate::DfsPeerTransportError>>
    {
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::try_current()
                .map_err(|source| crate::DfsPeerTransportError::new(source.to_string()))?;
            let result = self
                .call_rpc_inner(target_node_id, call, options, &runtime)
                .await
                .map_err(daemon_api_dfs_peer_transport_error)?;
            result
                .response
                .error_detail()
                .map_err(crate::DfsPeerTransportError::from_network)?;
            Ok(result)
        })
    }
}

fn daemon_api_dfs_peer_transport_error(source: DaemonApiError) -> crate::DfsPeerTransportError {
    let delivery_state = source.delivery_state();
    match source {
        DaemonApiError::UnknownRoute { route } => crate::DfsPeerTransportError::from_network(
            sinomo_core::network::NetworkError::UnknownRoute { route },
        ),
        source => {
            crate::DfsPeerTransportError::with_delivery_state(source.to_string(), delivery_state)
        }
    }
}

/// DFS forwarding over a generic daemon API client.
pub trait DaemonDfsApiClientExt {
    fn dfs_object_rpc_to_target(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        connect_timeout: Duration,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResponse, DaemonApiError>;
    fn dfs_object_rpc_to_target_with_options(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        options: crate::DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResponse, DaemonApiError>;
}
impl DaemonDfsApiClientExt for DaemonApiClient {
    /// Sends a DFS object-RPC from inside the running local daemon.
    fn dfs_object_rpc_to_target(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        connect_timeout: Duration,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        self.dfs_object_rpc_to_target_with_options(
            target_node_id,
            request,
            crate::DfsObjectRpcSendOptions::new(connect_timeout)
                .with_observe_connectivity(observe_connectivity),
        )
    }
    /// Sends a DFS object-RPC while keeping transport phase limits separate
    /// from an optional DFS operation-wide deadline.
    fn dfs_object_rpc_to_target_with_options(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        options: crate::DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResponse, DaemonApiError> {
        let data_dir = SinomoDataDir::from_env_or_home()?;
        let address = self.address().to_owned();
        let outer_timeouts =
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(self.timeout());
        let target_node_id = target_node_id.clone();
        let request = request.clone();
        block_on_daemon_client(Box::pin(async move {
            let runtime = tokio::runtime::Handle::current();
            let deadline = daemon_api_overall_deadline(options.overall_timeout);
            run_daemon_api_with_overall_deadline(
                "daemon-mediated DFS object RPC value call",
                deadline,
                Box::pin(async move {
                    let call = ObjectRpcCall::value(request);
                    let result = send_daemon_object_rpc_call_with_affinity_async(
                        data_dir,
                        &address,
                        DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
                        &target_node_id,
                        call,
                        options.connect_timeout,
                        None,
                        options.overall_timeout,
                        options.observe_connectivity,
                        sinomo_api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
                        outer_timeouts,
                        None,
                        None,
                        &runtime,
                    )
                    .await?;
                    decode_object_rpc_value_response(result).await
                }),
            )
            .await
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deferred_main_transport_constructs_offline_and_never_dispatches_expired_or_rebound_calls() {
        let root = sinomo_testdir::TestDir::new("deferred-main-transport");
        let data = SinomoDataDir::new(&root);
        let (mut native, node) =
            crate::test_support::DfsTestRuntime::start_single_node_in_data_dir(
                "deferred-main-transport",
                data.clone(),
            )
            .unwrap();
        native.stop_node(&node).unwrap();
        let peer = DaemonDfsPeerTransport::deferred(data.clone()).unwrap();
        assert!(
            peer.address.is_none(),
            "no endpoint was proved at construction"
        );
        let request = || {
            ObjectRpcCall::value(
                ObjectRpcRequest::encode_body(
                    DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
                    "dfs",
                    &DaemonDfsBrowserCreateDirAllRequest {
                        path: "/services/never-dispatched".into(),
                    },
                )
                .unwrap(),
            )
        };
        let options =
            crate::DfsObjectRpcSendOptions::default().with_overall_timeout(Duration::from_secs(1));
        let error = peer.call_object_rpc(&node, request(), options).unwrap_err();
        assert_eq!(
            error.delivery_state(),
            Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)
        );
        assert!(error.to_string().contains("not running"), "{error}");
        let expired = peer
            .call_object_rpc(
                &node,
                request(),
                crate::DfsObjectRpcSendOptions::default().with_overall_timeout(Duration::ZERO),
            )
            .unwrap_err();
        assert!(
            expired
                .to_string()
                .contains("deadline expired before Main discovery")
        );
        assert_eq!(
            expired.delivery_state(),
            Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)
        );
        let paths = sinomo_core::network::ClusterPaths::new(&data);
        let original = std::fs::read(&paths.state).unwrap();
        let mut state = sinomo_core::cluster::read_cluster_state_without_kv(&paths).unwrap();
        state.cluster_id = sinomo_core::Id::generate(); // negative DATA, never authority
        sinomo_core::cluster::write_cluster_state(&paths, &state).unwrap();
        let changed = peer.call_object_rpc(&node, request(), options).unwrap_err();
        assert!(changed.to_string().contains("identity changed"));
        assert_eq!(
            changed.delivery_state(),
            Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)
        );
        std::fs::write(&paths.state, original).unwrap();
        native.reopen_node(node.clone()).unwrap();
        let mut service = native.client(&node).unwrap();
        assert!(matches!(
            service.posix_stat_distributed("/services/never-dispatched"),
            Err(crate::DfsPosixError::NotFound { .. })
        ));
        drop(service);
        native.close().unwrap();
    }

    fn mounted_mutation_test_ids() -> (
        crate::MountedSifsMutationMarkerId,
        crate::MountedSifsMutationNonce,
        crate::MountedSifsMutationArmRequestId,
    ) {
        (
            sinomo_core::Id::derived("test.mounted-mutation.client.marker", b"marker"),
            sinomo_core::Id::derived("test.mounted-mutation.client.nonce", b"nonce"),
            sinomo_core::Id::derived("test.mounted-mutation.client.arm", b"arm"),
        )
    }

    fn mounted_mutation_test_capture() -> crate::MountedSifsMutationCapturedWork {
        crate::MountedSifsMutationCapturedWork::new(crate::DfsCommittedWork {
            publish_metadata: true,
            ..crate::DfsCommittedWork::default()
        })
    }

    #[test]
    fn mounted_mutation_client_is_explicit_loopback_and_deadline_bounded() {
        let (_root, data_dir) = test_channel_data_dir("api-mounted-mutation-client");
        let client =
            DaemonDfsMountedMutationHandoffClient::new(data_dir.clone(), "127.0.0.1:39393")
                .expect("explicit loopback client");
        assert_eq!(client.data_dir().root(), data_dir.root());
        assert_eq!(client.address(), "127.0.0.1:39393");
        assert_eq!(
            client.timeout(),
            DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_DEFAULT_TIMEOUT
        );

        let timeout = Duration::from_millis(750);
        let options = daemon_dfs_mounted_mutation_handoff_options(timeout);
        assert_eq!(options.overall_timeout(), Some(timeout));
        assert_eq!(
            options.destination_process_affinity(),
            sinomo_core::network::DestinationProcessAffinity::MainDaemon
        );
        assert!(client.clone().with_timeout(Duration::ZERO).is_err());
        assert!(
            client
                .with_timeout(
                    DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_TIMEOUT + Duration::from_nanos(1),
                )
                .is_err()
        );

        let error = DaemonDfsMountedMutationHandoffClient::new(data_dir, "10.0.0.20:39393")
            .expect_err("remote handoff address must fail closed");
        assert!(error.to_string().contains("loopback"));
    }

    #[test]
    fn mounted_mutation_transitions_are_same_id_response_retry_safe() {
        let (marker_id, marker_nonce, arm_request_id) = mounted_mutation_test_ids();
        let arm = DaemonDfsMountedMutationHandoffRequest::Arm {
            marker_id: marker_id.clone(),
            marker_nonce: marker_nonce.clone(),
            arm_request_id: arm_request_id.clone(),
        };
        let arm_call = daemon_dfs_mounted_mutation_handoff_call(&arm).expect("arm call");
        assert_eq!(
            arm_call.request.route,
            DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE
        );
        assert_eq!(arm_call.request.category, DAEMON_RPC_CATEGORY);
        assert!(arm_call.request.is_response_retry_safe());
        assert_eq!(
            arm_call.request.operation_id(),
            Some(arm_request_id.as_str())
        );

        let commit = DaemonDfsMountedMutationHandoffRequest::CommitApplied {
            marker_id: marker_id.clone(),
            marker_nonce: marker_nonce.clone(),
            arm_request_id: arm_request_id.clone(),
            captured_work: mounted_mutation_test_capture(),
        };
        let commit_call = daemon_dfs_mounted_mutation_handoff_call(&commit).expect("commit call");
        assert!(commit_call.request.is_response_retry_safe());
        assert_eq!(
            commit_call.request.operation_id(),
            arm_call.request.operation_id(),
            "arm and commit must retain the caller-stable arm request id"
        );

        let abort = DaemonDfsMountedMutationHandoffRequest::AbortUnstarted {
            marker_id: marker_id.clone(),
            marker_nonce,
        };
        let abort_call = daemon_dfs_mounted_mutation_handoff_call(&abort).expect("abort call");
        assert!(abort_call.request.is_response_retry_safe());
        assert_eq!(abort_call.request.operation_id(), Some(marker_id.as_str()));

        let probe = daemon_dfs_mounted_mutation_handoff_call(
            &DaemonDfsMountedMutationHandoffRequest::Probe,
        )
        .expect("probe call");
        assert!(probe.request.is_response_retry_safe());
        assert_eq!(probe.request.operation_id(), None);
    }

    #[test]
    fn mounted_mutation_client_rejects_oversized_semantic_payloads() {
        let (marker_id, marker_nonce, arm_request_id) = mounted_mutation_test_ids();
        let oversized = DaemonDfsMountedMutationHandoffRequest::CommitUnknown {
            marker_id,
            marker_nonce,
            arm_request_id,
            observation: crate::MountedSifsMutationErrorObservation {
                captured_work: mounted_mutation_test_capture(),
                error_code: "e".repeat(crate::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_LABEL_BYTES + 1),
            },
        };
        let error =
            daemon_dfs_mounted_mutation_handoff_call(&oversized).expect_err("label length limit");
        assert!(error.to_string().contains("error_code"));
        assert!(error.to_string().contains("max is"));
    }

    #[test]
    fn mounted_mutation_response_requires_exact_acknowledgement_identity() {
        let (marker_id, _marker_nonce, arm_request_id) = mounted_mutation_test_ids();
        let response = DaemonDfsMountedMutationHandoffResponse {
            marker_id: Some(marker_id.clone()),
            arm_request_id: Some(arm_request_id.clone()),
            phase: crate::DaemonDfsMountedMutationHandoffPhase::Armed,
        };
        validate_daemon_dfs_mounted_mutation_handoff_response(
            response.clone(),
            crate::DaemonDfsMountedMutationHandoffPhase::Armed,
            Some(&marker_id),
            Some(&arm_request_id),
        )
        .expect("matching arm acknowledgement");

        let wrong_arm = sinomo_core::Id::derived("test.mounted-mutation.client.arm", b"wrong");
        let error = validate_daemon_dfs_mounted_mutation_handoff_response(
            response,
            crate::DaemonDfsMountedMutationHandoffPhase::Armed,
            Some(&marker_id),
            Some(&wrong_arm),
        )
        .expect_err("mismatched arm acknowledgement");
        assert!(error.to_string().contains("mismatched acknowledgement"));

        let aborted = DaemonDfsMountedMutationHandoffResponse {
            marker_id: Some(marker_id.clone()),
            arm_request_id: None,
            phase: crate::DaemonDfsMountedMutationHandoffPhase::AbortedUnstarted,
        };
        validate_daemon_dfs_mounted_mutation_handoff_response(
            aborted.clone(),
            crate::DaemonDfsMountedMutationHandoffPhase::AbortedUnstarted,
            Some(&marker_id),
            None,
        )
        .expect("abort acknowledgement echoes marker identity");

        let missing_marker = DaemonDfsMountedMutationHandoffResponse {
            marker_id: None,
            ..aborted
        };
        assert!(
            validate_daemon_dfs_mounted_mutation_handoff_response(
                missing_marker,
                crate::DaemonDfsMountedMutationHandoffPhase::AbortedUnstarted,
                Some(&marker_id),
                None,
            )
            .is_err(),
            "every non-probe transition must echo its marker identity"
        );
    }

    fn test_channel_data_dir(label: &str) -> (sinomo_testdir::TestDir, SinomoDataDir) {
        let root = sinomo_testdir::TestDir::new(label);
        let data_dir = SinomoDataDir::new(root.path());
        data_dir.ensure_root_layout().expect("test data dir");
        (root, data_dir)
    }

    async fn accept_test_stream_native_carrier(
        stream: sinomo_core::network::AsyncDaemonControlStream,
    ) -> Result<sinomo_core::network::RoutedStreamAcceptor, sinomo_core::network::NetworkError>
    {
        let mut stream = stream.into_inner();
        let context =
            sinomo_core::network::read_local_object_rpc_carrier_prelude(&mut stream).await?;
        sinomo_core::network::accept_local_object_rpc_stream_carrier(Box::new(stream), &context)
    }

    async fn serve_test_peer_stream_native_service<F, Fut>(
        stream: sinomo_core::network::PeerStream,
        handler: F,
    ) -> Result<(), sinomo_core::network::NetworkError>
    where
        F: FnOnce(
            sinomo_core::network::ObjectRpcIncomingRequest,
            sinomo_core::network::RpcPeerContext,
        ) -> Fut,
        Fut: std::future::Future<
                Output = Result<
                    sinomo_core::network::ObjectRpcOutgoingResponse,
                    sinomo_core::network::NetworkError,
                >,
            >,
    {
        let context = sinomo_core::network::RpcPeerContext::unauthenticated_local(
            stream.session_id.clone(),
            stream.channel_binding,
        );
        sinomo_core::network::serve_service_rpc_peer_stream(
            stream,
            &context,
            sinomo_core::network::ObjectFrameIoOptions::default(),
            handler,
        )
        .await
    }

    async fn serve_test_logical_stream_native_service<F, Fut>(
        acceptor: &mut sinomo_core::network::RoutedStreamAcceptor,
        handler: F,
    ) -> Result<(), sinomo_core::network::NetworkError>
    where
        F: FnOnce(
            sinomo_core::network::ObjectRpcIncomingRequest,
            sinomo_core::network::RpcPeerContext,
        ) -> Fut,
        Fut: std::future::Future<
                Output = Result<
                    sinomo_core::network::ObjectRpcOutgoingResponse,
                    sinomo_core::network::NetworkError,
                >,
            >,
    {
        let stream =
            acceptor
                .accept()
                .await?
                .ok_or_else(|| sinomo_core::network::NetworkError::Io {
                    operation: "accept test local RPC logical stream".to_string(),
                    detail: "local RPC carrier closed before the logical stream arrived"
                        .to_string(),
                })?;
        serve_test_peer_stream_native_service(stream, handler).await
    }

    async fn serve_test_stream_native_service<F, Fut>(
        stream: sinomo_core::network::AsyncDaemonControlStream,
        handler: F,
    ) -> Result<(), sinomo_core::network::NetworkError>
    where
        F: FnOnce(
            sinomo_core::network::ObjectRpcIncomingRequest,
            sinomo_core::network::RpcPeerContext,
        ) -> Fut,
        Fut: std::future::Future<
                Output = Result<
                    sinomo_core::network::ObjectRpcOutgoingResponse,
                    sinomo_core::network::NetworkError,
                >,
            >,
    {
        let mut acceptor = accept_test_stream_native_carrier(stream).await?;
        serve_test_logical_stream_native_service(&mut acceptor, handler).await
    }

    fn write_test_cluster_state(
        data_dir: &SinomoDataDir,
        label: &str,
        local_node_id: NodeId,
        public_key: String,
    ) {
        let cluster_paths = sinomo_core::network::ClusterPaths::new(data_dir);
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(format!("cluster_{label}"))
                .expect("cluster id"),
            format!("API test cluster {label}"),
            local_node_id,
            "local",
            public_key,
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn daemon_dfs_peer_transport_sends_the_fail_closed_main_dfs_forward_route() {
        let (_root, data_dir) = test_channel_data_dir("sinomo-api-dfs-forward-route");
        let local_node =
            NodeId::from_legacy_named("node_api_dfs_forward_local").expect("local node");
        let target_node =
            NodeId::from_legacy_named("node_api_dfs_forward_target").expect("target node");
        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let expected_target = target_node.clone();
        let server = tokio::spawn(async move {
            let carrier = listener.accept().await.expect("accept local RPC carrier");
            let mut acceptor = accept_test_stream_native_carrier(carrier)
                .await
                .expect("accept stream-native local RPC carrier");
            serve_test_logical_stream_native_service(
                &mut acceptor,
                move |inbound, _peer| async move {
                    assert_eq!(
                        inbound.request.route,
                        DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE,
                        "the real DFS peer transport must use a route an old daemon cannot silently downgrade"
                    );
                    let forward = inbound
                        .decode_body::<DaemonObjectRpcForwardRequest>()
                        .await
                        .expect("forward request");
                    assert_eq!(forward.target_node_id, expected_target);
                    assert_eq!(
                        forward.destination_process_affinity,
                        sinomo_api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
                        "DFS forwarding must terminate at the main daemon; shadow DFS is fail-closed"
                    );
                    let inner = ObjectRpcResponse::encode_body(forward.request.route, &"dfs-pong")
                        .expect("inner response");
                    sinomo_core::network::ObjectRpcOutgoingResponse::encode_borrowed_value(
                        DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE,
                        &DaemonObjectRpcForwardResponse::Success {
                            response: inner,
                            streamed_value_body: false,
                            forward_elapsed_micros: 0,
                        },
                    )
                },
            )
            .await
            .expect("serve stream-native DFS forward session");
        });

        let transport =
            DaemonDfsPeerTransport::at_address(data_dir.clone(), address.clone(), local_node);
        let request = ObjectRpcRequest::encode_body("/dfs/test/actual-forward", "dfs", &())
            .expect("DFS request");
        let result = transport
            .call_rpc_inner(
                &target_node,
                ObjectRpcCall::value(request),
                crate::DfsObjectRpcSendOptions::new(Duration::from_secs(2))
                    .with_overall_timeout(Duration::from_secs(2)),
                &tokio::runtime::Handle::current(),
            )
            .await
            .expect("DFS forwarded call");
        assert_eq!(
            result
                .response
                .decode_body::<String>()
                .expect("inner response"),
            "dfs-pong"
        );
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn daemon_dfs_peer_transport_uses_proven_main_for_a_local_target() {
        let root = sinomo_testdir::TestDir::new("sinomo-api-dfs-local-target-session");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let identity = sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main identity");
        let local_node =
            NodeId::from_legacy_named("node_api_dfs_local_target_session").expect("local node");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_api_dfs_local_target_session")
                .expect("cluster id"),
            "API DFS local target session",
            local_node.clone(),
            "local",
            identity.public_key,
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let request_body = vec![0x41_u8; 512 * 1024 + 31];
        let response_body = vec![0x52_u8; 768 * 1024 + 47];
        let expected_request = request_body.clone();
        let server_response = response_body.clone();
        let server = tokio::spawn(async move {
            let stream = listener
                .accept()
                .await
                .expect("accept local service stream");
            serve_test_stream_native_service(stream, move |incoming, _peer| async move {
                assert_eq!(
                    incoming.request.route,
                    sinomo_core::kv::CLUSTER_KV_TXN_ROUTE,
                    "a transport connected through the main-daemon discovery path must keep the local target on the one-hop direct path"
                );
                let request_body = incoming
                    .body
                    .collect_to_vec(1024 * 1024)
                    .await
                    .expect("local request body");
                assert_eq!(request_body, expected_request);
                let inner = ObjectRpcResponse::encode_body(
                    sinomo_core::kv::CLUSTER_KV_TXN_ROUTE,
                    &"local-ok",
                )
                .expect("local response head");
                Ok(sinomo_core::network::ObjectRpcOutgoingResponse {
                    response: inner,
                    body: sinomo_core::network::ObjectRpcBodySource::bytes(server_response),
                })
            })
            .await
            .expect("serve local stream-native request");
        });

        let transport = DaemonDfsPeerTransport::at_address_with_process_affinity(
            data_dir.clone(),
            address.clone(),
            local_node.clone(),
            sinomo_api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
        );
        let request = ObjectRpcRequest::encode_body(
            sinomo_core::kv::CLUSTER_KV_TXN_ROUTE,
            sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
            &"local-request",
        )
        .expect("local request head");
        let result = crate::AsyncDfsPeerTransport::call_object_rpc_async(
            &transport,
            &local_node,
            ObjectRpcCall::new(
                request,
                sinomo_core::network::ObjectRpcBodySource::bytes(request_body),
            ),
            crate::DfsObjectRpcSendOptions::default().with_overall_timeout(Duration::from_secs(30)),
        )
        .await
        .expect("local target RPC");

        assert_eq!(
            result
                .response
                .decode_body::<String>()
                .expect("local response"),
            "local-ok"
        );
        assert_eq!(
            result
                .body
                .collect_to_vec(1024 * 1024)
                .await
                .expect("local response body"),
            response_body
        );
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn unproven_local_main_target_uses_the_fail_closed_forward_route() {
        let root = sinomo_testdir::TestDir::new("sinomo-api-unproven-local-main-forward");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let identity = sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main identity");
        let local_node =
            NodeId::from_legacy_named("node_api_unproven_local_main_forward").expect("local node");
        write_test_cluster_state(
            &data_dir,
            "unproven_local_main_forward",
            local_node.clone(),
            identity.public_key,
        );
        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let expected_node = local_node.clone();
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept local carrier");
            serve_test_stream_native_service(stream, move |incoming, _peer| async move {
                assert_eq!(
                    incoming.request.route, DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE,
                    "an address-only client must not direct-dispatch or use a legacy route"
                );
                let payload: DaemonObjectRpcForwardRequest =
                    incoming.decode_body().await.expect("forward payload");
                assert_eq!(payload.target_node_id, expected_node);
                assert_eq!(
                    payload.destination_process_affinity,
                    sinomo_api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
                );
                let inner = ObjectRpcResponse::encode_body(payload.request.route, &"forwarded")
                    .expect("inner response");
                sinomo_core::network::ObjectRpcOutgoingResponse::encode_borrowed_value(
                    DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE,
                    &DaemonObjectRpcForwardResponse::Success {
                        response: inner,
                        streamed_value_body: false,
                        forward_elapsed_micros: 0,
                    },
                )
            })
            .await
            .expect("serve fail-closed forward");
        });

        let transport = DaemonDfsPeerTransport::at_address(data_dir, address, local_node.clone());
        let request = ObjectRpcRequest::encode_body("/test/unproven-local-main", "test", &())
            .expect("inner request");
        let result = transport
            .call_rpc_inner(
                &local_node,
                ObjectRpcCall::value(request),
                crate::DfsObjectRpcSendOptions::new(Duration::from_secs(2))
                    .with_overall_timeout(Duration::from_secs(5)),
                &tokio::runtime::Handle::current(),
            )
            .await
            .expect("unproven local forward");
        assert_eq!(
            result
                .response
                .decode_body::<String>()
                .expect("inner response"),
            "forwarded",
        );
        server.await.expect("server task");
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;

    fn mounted_mutation_test_ids() -> (
        crate::MountedSifsMutationMarkerId,
        crate::MountedSifsMutationNonce,
        crate::MountedSifsMutationArmRequestId,
    ) {
        (
            sinomo_core::Id::derived("test.mounted-mutation.marker", b"marker"),
            sinomo_core::Id::derived("test.mounted-mutation.nonce", b"nonce"),
            sinomo_core::Id::derived("test.mounted-mutation.arm", b"arm"),
        )
    }

    #[test]
    fn mounted_mutation_handoff_wire_is_versioned_and_typed() {
        assert_eq!(
            DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_V1_ROUTE,
            "/daemon/dfs/mounted-mutation/handoff-v1"
        );
        assert_eq!(
            DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES,
            crate::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_STATE_BYTES
        );

        let probe = serde_json::to_value(DaemonDfsMountedMutationHandoffRequest::Probe)
            .expect("encode probe");
        assert_eq!(probe, serde_json::json!({ "operation": "probe" }));

        let (marker_id, marker_nonce, arm_request_id) = mounted_mutation_test_ids();
        let arm = serde_json::to_value(DaemonDfsMountedMutationHandoffRequest::Arm {
            marker_id: marker_id.clone(),
            marker_nonce,
            arm_request_id: arm_request_id.clone(),
        })
        .expect("encode arm");
        assert_eq!(arm["operation"], "arm");
        assert_eq!(arm["marker_id"], marker_id.as_str());
        assert_eq!(arm["arm_request_id"], arm_request_id.as_str());

        let response = DaemonDfsMountedMutationHandoffResponse {
            marker_id: Some(marker_id),
            arm_request_id: Some(arm_request_id),
            phase: DaemonDfsMountedMutationHandoffPhase::DaemonAccepted,
        };
        let encoded = serde_json::to_value(&response).expect("encode response");
        assert_eq!(encoded["phase"], "daemon-accepted");
        assert!(encoded.get("appended_repair_record_count").is_none());
        assert_eq!(
            serde_json::from_value::<DaemonDfsMountedMutationHandoffResponse>(encoded)
                .expect("decode response"),
            response
        );
    }

    #[test]
    fn mounted_mutation_error_never_decodes_as_an_empty_applied_capture() {
        let (marker_id, marker_nonce, arm_request_id) = mounted_mutation_test_ids();
        let captured_work =
            crate::MountedSifsMutationCapturedWork::new(crate::DfsCommittedWork::default());
        let unknown = DaemonDfsMountedMutationHandoffRequest::CommitUnknown {
            marker_id: marker_id.clone(),
            marker_nonce: marker_nonce.clone(),
            arm_request_id: arm_request_id.clone(),
            observation: crate::MountedSifsMutationErrorObservation {
                captured_work,
                error_code: "backend-returned-error-after-entry".to_string(),
            },
        };
        let encoded = serde_json::to_value(&unknown).expect("encode unknown result");
        assert_eq!(encoded["operation"], "commit-unknown");
        assert!(encoded.get("observation").is_some());
        assert!(encoded.get("captured_work").is_none());
        assert_eq!(
            serde_json::from_value::<DaemonDfsMountedMutationHandoffRequest>(encoded)
                .expect("decode unknown result"),
            unknown
        );
        let wire = sinomo_core::network::SinomoObjectCodec::network_frame()
            .encode(&unknown)
            .expect("encode SOBJ request value");
        assert!(wire.len() <= DAEMON_DFS_MOUNTED_MUTATION_HANDOFF_MAX_PAYLOAD_BYTES);
        assert_eq!(
            sinomo_core::network::SinomoObjectCodec::network_frame()
                .decode::<DaemonDfsMountedMutationHandoffRequest>(&wire)
                .expect("decode SOBJ request value"),
            unknown
        );

        let ambiguous_legacy = serde_json::json!({
            "operation": "commit",
            "marker_id": marker_id,
            "marker_nonce": marker_nonce,
            "arm_request_id": arm_request_id
        });
        assert!(
            serde_json::from_value::<DaemonDfsMountedMutationHandoffRequest>(ambiguous_legacy)
                .is_err(),
            "an unclassified error must not decode as an empty applied capture"
        );
    }

    #[test]
    fn mounted_mutation_handoff_validates_bounded_labels() {
        let (marker_id, marker_nonce, arm_request_id) = mounted_mutation_test_ids();
        let empty_proof = DaemonDfsMountedMutationHandoffRequest::ProveNoWrite {
            marker_id: marker_id.clone(),
            marker_nonce: marker_nonce.clone(),
            arm_request_id: arm_request_id.clone(),
            proof_code: String::new(),
        };
        let error = empty_proof.validate().expect_err("empty proof code");
        assert!(error.contains("proof_code cannot be empty"));

        let empty_error = DaemonDfsMountedMutationHandoffRequest::CommitUnknown {
            marker_id: marker_id.clone(),
            marker_nonce: marker_nonce.clone(),
            arm_request_id: arm_request_id.clone(),
            observation: crate::MountedSifsMutationErrorObservation {
                captured_work: crate::MountedSifsMutationCapturedWork::default(),
                error_code: String::new(),
            },
        };
        let error = empty_error.validate().expect_err("empty error code");
        assert!(error.contains("error_code cannot be empty"));

        let request = DaemonDfsMountedMutationHandoffRequest::ProveNoWrite {
            marker_id,
            marker_nonce,
            arm_request_id,
            proof_code: "x"
                .repeat(crate::MOUNTED_SIFS_MUTATION_OUTBOX_MAX_LABEL_BYTES.saturating_add(1)),
        };
        let error = request.validate().expect_err("oversized proof code");
        assert!(error.contains("proof_code"));
        assert!(error.contains("max is"));
    }

    #[test]
    fn dfs_subscription_covers_namespace_and_placement_events() {
        let request = DaemonEventSubscribeRequest::for_dfs_path_prefix("/users/project");
        assert_eq!(request.event_kind_prefixes, vec!["dfs."]);
        assert_eq!(request.path_prefixes, vec!["/users/project"]);
    }

    #[test]
    fn conditional_replace_has_a_dedicated_fail_closed_route() {
        assert_ne!(
            DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE,
            DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE
        );
        assert!(
            DAEMON_DFS_BROWSER_ROUTES.contains(&DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE)
        );

        let write = serde_json::to_value(DaemonDfsBrowserWriteFileRequest {
            path: "/notes.txt".to_string(),
            bytes: b"plain write".to_vec(),
        })
        .expect("serialize plain write");
        assert_eq!(write.as_object().map(|fields| fields.len()), Some(2));
        assert!(write.get("expected_object_id").is_none());
        assert!(write.get("expected_version").is_none());
    }

    #[test]
    fn placement_authority_admin_route_is_not_a_browser_local_command() {
        assert!(!DAEMON_DFS_BROWSER_ROUTES.contains(&DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE));
        assert_eq!(
            DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE,
            "/daemon/dfs/placement-authority"
        );
    }

    #[test]
    fn placement_update_v2_requires_a_preview_token_on_a_dedicated_route() {
        assert!(DAEMON_DFS_BROWSER_ROUTES.contains(&DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE));

        let missing_token = serde_json::json!({
            "path": "/users/team",
            "scope": "self-only"
        });
        assert!(
            serde_json::from_value::<DaemonDfsBrowserPlacementUpdateV2Request>(missing_token)
                .is_err(),
            "the fail-closed route must not default a missing preview token"
        );
    }

    #[test]
    fn placement_activity_request_defaults_to_a_bounded_unfiltered_page() {
        let request: DaemonDfsBrowserPlacementActivityRequest =
            serde_json::from_value(serde_json::json!({})).expect("default activity request");
        assert!(request.statuses.is_empty());
        assert!(request.path_prefix.is_none());
        assert!(request.cursor.is_none());
        assert_eq!(
            request.limit,
            crate::DEFAULT_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE
        );

        let encoded = serde_json::to_value(DaemonDfsBrowserPlacementActivityRequest {
            statuses: vec![crate::DfsPlacementReconciliationActivityStatus::Retrying],
            path_prefix: Some("/team".to_string()),
            cursor: Some(crate::DfsPlacementReconciliationActivityCursor {
                created_at_unix_nanos: 42,
                job_id: "activity-job".to_string(),
            }),
            limit: 25,
        })
        .expect("encode activity request");
        let decoded: DaemonDfsBrowserPlacementActivityRequest =
            serde_json::from_value(encoded).expect("decode activity request");
        assert_eq!(decoded.limit, 25);
        assert_eq!(
            decoded.statuses,
            vec![crate::DfsPlacementReconciliationActivityStatus::Retrying]
        );

        assert!(
            serde_json::from_value::<DaemonDfsBrowserPlacementActivityResponse>(
                serde_json::json!({"page": {"activities": []}})
            )
            .is_err(),
            "current activity responses require explicit completeness"
        );
    }

    #[test]
    fn conditional_replace_outcomes_preserve_receipt_and_error_kinds() {
        let committed = DaemonDfsBrowserReplaceFileIfVersionResponse {
            outcome: DaemonDfsBrowserReplaceFileIfVersionOutcome::Committed {
                receipt: DaemonDfsBrowserWriteReceipt {
                    object_id: "object-1".to_string(),
                    version: "version-2".to_string(),
                    content_hash: Some("hash-2".to_string()),
                    size_bytes: Some(7),
                },
                maintenance_warning: Some("maintenance queued".to_string()),
            },
        };
        let encoded = serde_json::to_vec(&committed).expect("encode committed outcome");
        let decoded: DaemonDfsBrowserReplaceFileIfVersionResponse =
            serde_json::from_slice(&encoded).expect("decode committed outcome");
        assert_eq!(decoded, committed);
        assert!(decoded.outcome.is_committed());

        for outcome in [
            DaemonDfsBrowserReplaceFileIfVersionOutcome::Conflict {
                detail: "stale base".to_string(),
            },
            DaemonDfsBrowserReplaceFileIfVersionOutcome::NotFound {
                detail: "removed".to_string(),
            },
            DaemonDfsBrowserReplaceFileIfVersionOutcome::Rejected {
                detail: "not writable".to_string(),
            },
            DaemonDfsBrowserReplaceFileIfVersionOutcome::Unknown {
                detail: "commit unconfirmed".to_string(),
            },
        ] {
            let encoded = serde_json::to_vec(&outcome).expect("encode typed outcome");
            let decoded: DaemonDfsBrowserReplaceFileIfVersionOutcome =
                serde_json::from_slice(&encoded).expect("decode typed outcome");
            assert_eq!(decoded, outcome);
            assert!(!decoded.is_committed());
        }
    }

    #[test]
    fn dfs_browser_read_file_wire_requires_current_outcomes_and_types_unavailability() {
        assert!(
            serde_json::from_value::<DaemonDfsBrowserReadFileResponse>(
                serde_json::json!({"bytes":[1,2,3]})
            )
            .is_err()
        );
        let found = DaemonDfsBrowserReadFileResponse::found(vec![4, 5]);
        let rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
            &found,
        )
        .unwrap();
        let decoded: DaemonDfsBrowserReadFileResponse = rpc.decode_body().unwrap();
        assert_eq!(decoded.into_result().unwrap(), vec![4, 5]);
        let unavailable = DaemonDfsBrowserReadFileResponse::content_unavailable(
            "/file",
            "current bytes unavailable",
        );
        let rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
            &unavailable,
        )
        .unwrap();
        let decoded: DaemonDfsBrowserReadFileResponse = rpc.decode_body().unwrap();
        assert!(
            matches!(decoded.into_result(), Err(DaemonDfsBrowserReadFileFailure::ContentUnavailable { path, .. }) if path == "/file")
        );
        let missing = DaemonDfsBrowserReadFileResponse::not_found("/missing");
        let rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
            &missing,
        )
        .unwrap();
        let decoded: DaemonDfsBrowserReadFileResponse = rpc.decode_body().unwrap();
        assert!(
            matches!(decoded.into_result(), Err(DaemonDfsBrowserReadFileFailure::NotFound { path }) if path == "/missing")
        );
    }

    #[test]
    fn dfs_browser_stat_wire_preserves_success_and_types_confirmed_absence() {
        let stat = DaemonDfsBrowserStatResponse {
            file_system_id: "sifs".to_string(),
            object_id: "object-stat-wire".to_string(),
            path: "/present.txt".to_string(),
            kind: crate::DfsObjectKind::File,
            version: "version-stat-wire".to_string(),
            content_hash: Some("blake3:stat-wire".to_string()),
            size_bytes: Some(3),
            link_count: 1,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 2,
            created_at_unix_nanos: 3,
            changed_at_unix_nanos: 4,
            policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            current_locations: vec!["node-stat-wire".to_string()],
            local_pins: Vec::new(),
            health: None,
            metadata_shard: None,
            placement: None,
        };

        let current = DaemonDfsBrowserStatOutcomeResponse::found(stat.clone());
        let rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_STAT_ROUTE,
            &current,
        )
        .unwrap();
        let decoded: DaemonDfsBrowserStatOutcomeResponse = rpc.decode_body().unwrap();
        assert_eq!(decoded.into_result().unwrap(), stat);
        let untagged = serde_json::to_value(&stat).unwrap();
        assert!(serde_json::from_value::<DaemonDfsBrowserStatOutcomeResponse>(untagged).is_err());

        let missing = DaemonDfsBrowserStatOutcomeResponse::not_found("/missing.txt");
        let missing_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_STAT_ROUTE,
            &missing,
        )
        .expect("encode typed missing stat");
        assert!(
            missing_rpc
                .decode_body::<DaemonDfsBrowserStatResponse>()
                .is_err(),
            "a typed stat failure cannot decode as a success"
        );
        let missing: DaemonDfsBrowserStatOutcomeResponse = missing_rpc
            .decode_body()
            .expect("current client decodes typed missing stat");
        assert!(matches!(
            missing.into_result(),
            Err(DaemonDfsBrowserReadFileFailure::NotFound { ref path })
                if path == "/missing.txt"
        ));

        let mut mixed = serde_json::to_value(&current).expect("encode current stat success");
        mixed
            .as_object_mut()
            .expect("stat success is an object")
            .insert(
                "failure".to_string(),
                serde_json::to_value(DaemonDfsBrowserReadFileFailure::NotFound {
                    path: "/present.txt".to_string(),
                })
                .expect("encode typed stat failure"),
            );
        assert!(
            serde_json::from_value::<DaemonDfsBrowserStatOutcomeResponse>(mixed.clone()).is_err(),
            "a mixed success and failure stat body must fail closed"
        );
        let mixed_rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_STAT_ROUTE,
            &mixed,
        )
        .expect("encode adversarial mixed stat body");
        assert!(
            mixed_rpc
                .decode_body::<DaemonDfsBrowserStatOutcomeResponse>()
                .is_err(),
            "the SOBJ wire decoder must reject a mixed stat body"
        );
    }

    #[test]
    fn dfs_browser_prefix_wire_requires_current_outcomes_and_roundtrips_truncation() {
        assert!(
            serde_json::from_value::<DaemonDfsBrowserReadFilePrefixResponse>(
                serde_json::json!({"bytes":[1,2],"truncated":true})
            )
            .is_err()
        );
        let found = DaemonDfsBrowserReadFilePrefixResponse::found(vec![1, 2, 3], true);
        let rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
            &found,
        )
        .unwrap();
        let decoded: DaemonDfsBrowserReadFilePrefixResponse = rpc.decode_body().unwrap();
        assert_eq!(decoded.into_result().unwrap(), (vec![1, 2, 3], true));
        let unavailable = DaemonDfsBrowserReadFilePrefixResponse::content_unavailable(
            "/prefix",
            "current bytes unavailable",
        );
        let rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
            &unavailable,
        )
        .unwrap();
        let decoded: DaemonDfsBrowserReadFilePrefixResponse = rpc.decode_body().unwrap();
        assert!(
            matches!(decoded.into_result(), Err(DaemonDfsBrowserReadFileFailure::ContentUnavailable { path, .. }) if path == "/prefix")
        );
    }

    #[test]
    fn dfs_browser_bulk_read_wire_requires_current_typed_outcomes() {
        for item in [
            DaemonDfsBrowserFileRead::found("/record.json", vec![1, 2, 3]),
            DaemonDfsBrowserFileRead::not_found("/missing.json"),
            DaemonDfsBrowserFileRead::content_unavailable(
                "/unavailable.json",
                "no readable replica",
            ),
        ] {
            let response = sinomo_core::network::ObjectRpcResponse::encode_body(
                DAEMON_DFS_BROWSER_READ_FILES_ROUTE,
                &item,
            )
            .unwrap();
            let decoded: DaemonDfsBrowserFileRead = response.decode_body().unwrap();
            assert_eq!(decoded, item);
        }
        assert!(
            serde_json::from_value::<DaemonDfsBrowserFileRead>(
                serde_json::json!({"path":"/missing-outcome.json"})
            )
            .is_err()
        );
        assert!(matches!(
            DaemonDfsBrowserFileRead::content_unavailable("/unavailable", "no readable replica")
                .into_result(),
            Err(DaemonDfsBrowserReadFileFailure::ContentUnavailable { .. })
        ));
    }

    #[test]
    fn daemon_api_error_preserves_possibly_applied_delivery_state() {
        let error = DaemonApiError::from(sinomo_core::network::NetworkError::timeout(
            "conditional replace response",
            sinomo_core::network::NetworkTimeoutPhase::FirstByte,
            std::time::Duration::from_secs(1),
            sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
        ));
        assert_eq!(
            error.delivery_state(),
            Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied)
        );
    }
}
