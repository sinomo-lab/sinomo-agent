use crate::{DaemonError, DaemonPaths, DaemonStatus, daemon_rpc_body_sha256};
use sinomo_build::{
    BUILD_ARTIFACT_ROUTE, BUILD_CAPABILITIES_ROUTE, BUILD_RPC_CATEGORY, BUILD_STATUS_ROUTE,
    BUILD_SUBMIT_ROUTE, BuildArtifactRequest, BuildArtifactResponse, BuildCapabilitiesRequest,
    BuildCapabilitiesResponse, BuildStatusRequest, BuildStatusResponse, BuildSubmitRequest,
    BuildSubmitResponse,
};
use sinomo_config::{
    CONFIG_CATALOG_ROUTE, CONFIG_GET_ROUTE, CONFIG_RPC_CATEGORY, CONFIG_SET_ROUTE,
    ConfigCatalogRequest, ConfigCatalogResponse, ConfigClient, ConfigDefinition, ConfigError,
    ConfigGetRequest, ConfigGetResponse, ConfigResolution, ConfigSetRequest, ConfigSetResponse,
    ConfigWriteResult, ConfigWriteTarget,
};
use sinomo_core::ClusterEndpointRecord;
use sinomo_core::cluster::{ClusterMemberPublishRequest, ClusterMemberPublishResponse};
use sinomo_core::kv::{
    ClusterRaftAppendRequest, ClusterRaftAppendResponse, ClusterRaftVoteRequest,
    ClusterRaftVoteResponse, KvSnapshot,
};
use sinomo_core::network::{
    NetworkError, ObjectRpcRequest, ObjectRpcSessionCall, ObjectRpcSessionResult,
};
use sinomo_core::node::NodeId;
use sinomo_dfs::{
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse,
    DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse,
};
use sinomo_rollout::{
    ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse, ClusterRolloutBuildRequest,
    ClusterRolloutNativeDylibModuleRequest, ROLLOUT_BINARY_ROUTE, ROLLOUT_BUILD_ROUTE,
    ROLLOUT_CLEAR_STATUS_ROUTE, ROLLOUT_DIAGNOSTICS_ROUTE, ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
    ROLLOUT_RPC_CATEGORY,
};
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

fn require_daemon_client_value_response(
    result: ObjectRpcSessionResult,
) -> Result<ObjectRpcSessionResult, DaemonError> {
    if !result.body.is_empty() {
        return Err(DaemonError::Parse {
            detail: "object RPC value response unexpectedly contained an attachment".to_string(),
        });
    }
    Ok(result)
}

fn object_rpc_request_from_kv_payload(
    payload: sinomo_core::kv::ClusterKvRpcPayload,
) -> ObjectRpcRequest {
    ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body)
}

pub fn send_daemon_rpc(
    address: &str,
    request: &ObjectRpcRequest,
    timeout: Duration,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    send_daemon_rpc_with_timeouts(
        address,
        request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
    )
}

pub(crate) fn send_daemon_rpc_on_runtime(
    address: &str,
    request: &ObjectRpcRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(
        runtime,
        send_daemon_rpc_async(
            address,
            request,
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
            &runtime_for_future,
        ),
    )
}

pub async fn send_daemon_rpc_async(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let call = ObjectRpcSessionCall::value(request.clone());
    let options = sinomo_core::network::ClusterObjectRpcOptions::new(timeouts);
    let result = send_daemon_rpc_session_async(address, call, options, runtime).await?;
    Ok(require_daemon_client_value_response(result)?.response)
}

/// Send one signed object-RPC session through a daemon address. Loopback
/// addresses use the local daemon directly; remote addresses retain endpoint
/// authentication and routing.
pub async fn send_daemon_rpc_session_async(
    address: &str,
    mut call: ObjectRpcSessionCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcSessionResult, DaemonError> {
    let data_dir = local_session_data_dir()?;
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
    let mut host =
        DaemonSignedObjectRpcAsyncHost::new(runtime, false).with_cluster_paths(&cluster_paths);
    call.request.auth = None;
    if sinomo_core::network::is_loopback_connect_address(address) {
        sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host)
            .call_loopback(&data_dir, address, call, options)
            .await
            .map_err(Into::into)
    } else {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
        let endpoint = local_session_endpoint_for_address(&state, address)?;
        sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, runtime, &cluster_paths)
            .call_endpoint(&state, &endpoint, call, options)
            .await
            .map_err(Into::into)
    }
}

/// Like [`send_daemon_rpc`], but lets callers provide separate connect and idle
/// deadlines. Use an idle deadline for RPCs that must never hang on a silent
/// peer, and omit it for long-running operations that report progress elsewhere.
pub fn send_daemon_rpc_with_timeouts(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    block_on_daemon_client(async {
        let runtime = tokio::runtime::Handle::current();
        send_daemon_rpc_async(address, request, timeouts, &runtime).await
    })
}

/// A [`ConfigClient`] backed by RPC to a running daemon's `/config/*` service over the daemon
/// control protocol. The daemon resolves each request with its own cluster access, so a remote
/// caller (e.g. the desktop app) sees exactly the same configuration as the daemon and CLI —
/// keeping every component on one source of truth.
#[derive(Clone)]
pub struct RemoteConfigClient {
    address: String,
    timeout: Duration,
    runtime: Option<tokio::runtime::Handle>,
}

impl RemoteConfigClient {
    pub fn new(address: impl Into<String>, timeout: Duration) -> Self {
        Self {
            address: address.into(),
            timeout,
            runtime: None,
        }
    }

    pub fn new_on_runtime(
        address: impl Into<String>,
        timeout: Duration,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        Self {
            address: address.into(),
            timeout,
            runtime: Some(runtime.clone()),
        }
    }

    fn rpc<Req, Resp>(&self, route: &str, body: &Req) -> Result<Resp, ConfigError>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        let client_error = |detail: String| ConfigError::Client { detail };
        let request = ObjectRpcRequest::encode_body(route, CONFIG_RPC_CATEGORY, body)
            .map_err(|error| client_error(error.to_string()))?;
        let response = match &self.runtime {
            Some(runtime) => {
                send_daemon_rpc_on_runtime(&self.address, &request, self.timeout, runtime)
                    .map_err(|error| client_error(error.to_string()))?
            }
            None => send_daemon_rpc(&self.address, &request, self.timeout)
                .map_err(|error| client_error(error.to_string()))?,
        };
        response
            .decode_body()
            .map_err(|error| client_error(error.to_string()))
    }
}

impl ConfigClient for RemoteConfigClient {
    fn catalog(&self, owner: Option<&str>) -> Result<Vec<ConfigDefinition>, ConfigError> {
        let response: ConfigCatalogResponse = self.rpc(
            CONFIG_CATALOG_ROUTE,
            &ConfigCatalogRequest {
                owner: owner.map(str::to_string),
            },
        )?;
        Ok(response.definitions)
    }

    fn get(&self, key: &str) -> Result<ConfigResolution, ConfigError> {
        let response: ConfigGetResponse = self.rpc(
            CONFIG_GET_ROUTE,
            &ConfigGetRequest {
                key: key.to_string(),
            },
        )?;
        Ok(response.resolution)
    }

    fn set(
        &self,
        key: &str,
        value: serde_json::Value,
        target: ConfigWriteTarget,
    ) -> Result<ConfigWriteResult, ConfigError> {
        let response: ConfigSetResponse = self.rpc(
            CONFIG_SET_ROUTE,
            &ConfigSetRequest {
                key: key.to_string(),
                value,
                target,
            },
        )?;
        Ok(response.result)
    }
}

pub fn send_daemon_rpc_stream(
    address: &str,
    request: &ObjectRpcRequest,
    timeout: Duration,
    mut on_response: impl FnMut(sinomo_core::network::ObjectRpcResponse) -> Result<bool, DaemonError>,
) -> Result<(), DaemonError> {
    block_on_daemon_client_stream(send_daemon_rpc_stream_async(
        address,
        request,
        sinomo_core::network::DaemonRpcTimeouts::default().with_connect_timeout(Some(timeout)),
        &mut on_response,
    ))
}

pub async fn send_daemon_rpc_stream_async(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    mut on_response: impl FnMut(sinomo_core::network::ObjectRpcResponse) -> Result<bool, DaemonError>,
) -> Result<(), DaemonError> {
    let data_dir = local_session_data_dir()?;
    let runtime = tokio::runtime::Handle::current();
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
    let mut host =
        DaemonSignedObjectRpcAsyncHost::new(&runtime, false).with_cluster_paths(&cluster_paths);
    let mut request = request.clone();
    request.auth = None;
    let call = ObjectRpcSessionCall::value(request);
    let options = sinomo_core::network::ClusterObjectRpcOptions::new(timeouts);
    let result = if sinomo_core::network::is_loopback_connect_address(address) {
        let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
        client
            .call_loopback(&data_dir, address, call, options)
            .await?
    } else {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
        let endpoint = local_session_endpoint_for_address(&state, address)?;
        let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
            &mut host,
            &runtime,
            &cluster_paths,
        );
        client
            .call_endpoint(&state, &endpoint, call, options)
            .await?
    };
    result.response.error_detail()?;
    let mut body = result.body;
    loop {
        let rpc = sinomo_core::network::read_object_frame_async(
            &mut body,
            sinomo_core::network::ObjectFrameIoOptions::none(),
        )
        .await?;
        if !on_response(rpc)? {
            return Ok(());
        }
    }
}

pub fn send_daemon_rpc_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeout: Duration,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let timeouts = sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout);
    send_daemon_rpc_to_endpoint_with_timeouts(endpoint, request, timeouts)
}

pub fn send_daemon_rpc_to_endpoint_with_timeouts(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            let runtime = handle.clone();
            tokio::task::block_in_place(|| {
                runtime.block_on(send_daemon_rpc_to_endpoint_async(
                    endpoint, request, timeouts, &runtime,
                ))
            })
        } else {
            Err(DaemonError::Network {
                address: "daemon client runtime".to_string(),
                detail: "daemon RPC sync client cannot run inside a current-thread Tokio runtime; use the async or owner-runtime API".to_string(),
            })
        };
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| DaemonError::Network {
            address: "daemon client runtime".to_string(),
            detail: source.to_string(),
        })?;
    let output = runtime.block_on(send_daemon_rpc_to_endpoint_async(
        endpoint,
        request,
        timeouts,
        runtime.handle(),
    ));
    runtime.shutdown_timeout(Duration::from_millis(250));
    output
}

pub(crate) fn send_daemon_rpc_to_endpoint_with_options_on_runtime(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(
        runtime,
        send_daemon_rpc_to_endpoint_with_options_async(
            endpoint,
            request,
            options,
            &runtime_for_future,
        ),
    )
}

/// Send the two invitation handshake calls over the deliberately narrow raw
/// bootstrap frame accepted before the introducer can authorize the candidate
/// as a member. Challenge proof authenticates the issuer; registration is
/// signed by the candidate's durable node key. This also supports an invite
/// whose issuer names a bridge introducer that had not joined when the invite
/// was minted, so no bridge session key could have been embedded yet.
pub(crate) fn send_daemon_join_bootstrap_to_endpoint_on_runtime(
    daemon_paths: &DaemonPaths,
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(
        runtime,
        send_daemon_join_bootstrap_to_endpoint_async(
            daemon_paths,
            endpoint,
            request,
            timeout,
            &runtime_for_future,
        ),
    )
}

async fn send_daemon_join_bootstrap_to_endpoint_async(
    daemon_paths: &DaemonPaths,
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    if request.category != sinomo_core::network::CLUSTER_MEMBERSHIP_RPC_CATEGORY
        || !matches!(
            request.route.as_str(),
            sinomo_core::network::CLUSTER_JOIN_CHALLENGE_ROUTE
                | sinomo_core::network::CLUSTER_JOIN_REGISTER_ROUTE
        )
    {
        return Err(DaemonError::Network {
            address: endpoint.address.clone(),
            detail: "raw join bootstrap transport only accepts cluster join handshakes".to_string(),
        });
    }
    let mut request = request.clone();
    if request.route == sinomo_core::network::CLUSTER_JOIN_REGISTER_ROUTE {
        let data_dir = daemon_data_dir_from_paths(daemon_paths)?;
        let signed_request = request.clone();
        request.auth = Some(
            runtime
                .spawn_blocking(move || {
                    sinomo_core::network::sign_daemon_rpc_request_from_data_dir(
                        &data_dir,
                        &signed_request,
                    )
                })
                .await
                .map_err(|source| DaemonError::Network {
                    address: endpoint.address.clone(),
                    detail: format!("join registration signing task failed: {source}"),
                })??,
        );
    } else {
        request.auth = None;
    }
    let mut host = DaemonSignedObjectRpcAsyncHost::new(runtime, false).with_paths(daemon_paths);
    let mut stream = sinomo_core::network::ClusterSignedObjectRpcAsyncHost::open_object_rpc_session_stream_async(
        &mut host,
        endpoint,
        sinomo_core::network::NetworkHealthPolicy::new(timeout, timeout),
    )
    .await?;
    let io = sinomo_core::network::ObjectFrameIoOptions::idle_timeout(timeout);
    sinomo_core::network::write_object_frame_async(&mut stream, &request, io).await?;
    let response: sinomo_core::network::ObjectRpcResponse =
        sinomo_core::network::read_object_frame_async(&mut stream, io).await?;
    response.error_detail()?;
    Ok(response)
}

async fn send_daemon_rpc_to_endpoint_with_options_async(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    send_daemon_rpc_to_endpoint_with_options_and_connectivity_async(
        endpoint, request, options, runtime, true,
    )
    .await
}

pub async fn send_daemon_rpc_to_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    send_daemon_rpc_to_endpoint_async_with_connectivity_observation(
        endpoint, request, timeouts, runtime, true,
    )
    .await
}

async fn send_daemon_rpc_to_endpoint_async_with_connectivity_observation(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let options = sinomo_core::network::ClusterObjectRpcOptions::new(timeouts);
    send_daemon_rpc_to_endpoint_with_options_and_connectivity_async(
        endpoint,
        request,
        options,
        runtime,
        observe_connectivity,
    )
    .await
}

async fn send_daemon_rpc_to_endpoint_with_options_and_connectivity_async(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let paths = cluster_paths_from_env().ok_or_else(|| DaemonError::Network {
        address: endpoint.address.clone(),
        detail: "authenticated endpoint RPC requires a local cluster state".to_string(),
    })?;
    let state = sinomo_core::cluster::read_cluster_state(&paths)?;
    let mut host = DaemonSignedObjectRpcAsyncHost::new(runtime, observe_connectivity)
        .with_cluster_paths(&paths);
    let mut client =
        sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, runtime, &paths);
    let result = client
        .call_endpoint(
            &state,
            endpoint,
            ObjectRpcSessionCall::value(request.clone()),
            options,
        )
        .await?;
    Ok(require_daemon_client_value_response(result)?.response)
}

pub async fn send_object_rpc_session_to_target_async_with_options(
    target_node_id: &NodeId,
    call: ObjectRpcSessionCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<ObjectRpcSessionResult, DaemonError> {
    let paths = cluster_paths_from_env().ok_or_else(|| DaemonError::Network {
        address: target_node_id.to_string(),
        detail: "cluster target RPC requires a local Sinomo data directory".to_string(),
    })?;
    send_object_rpc_session_to_target_async_with_cluster_paths_and_options(
        &paths,
        target_node_id,
        call,
        options,
        runtime,
        observe_connectivity,
    )
    .await
}

/// Routes one object-RPC session to a cluster node using an explicit local
/// cluster-state root for route discovery and request signing.
pub async fn send_object_rpc_session_to_target_async_with_cluster_paths_and_options(
    paths: &sinomo_core::network::ClusterPaths,
    target_node_id: &NodeId,
    call: ObjectRpcSessionCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<ObjectRpcSessionResult, DaemonError> {
    let state = sinomo_core::cluster::read_cluster_state(paths).map_err(DaemonError::from)?;
    let mut host = DaemonSignedObjectRpcAsyncHost::new(runtime, observe_connectivity)
        .with_cluster_paths(paths);
    let mut client =
        sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, runtime, paths);
    client
        .call_target(&state, target_node_id, call, options)
        .await
        .map_err(Into::into)
}

pub(crate) async fn send_object_rpc_session_to_target_async_with_daemon_paths_and_options(
    daemon_paths: &DaemonPaths,
    target_node_id: &NodeId,
    call: ObjectRpcSessionCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<ObjectRpcSessionResult, DaemonError> {
    send_object_rpc_session_to_target_async_with_daemon_paths_and_network_error(
        daemon_paths,
        target_node_id,
        call,
        options,
        runtime,
        observe_connectivity,
    )
    .await
    .map_err(DaemonError::from)
}

pub(crate) async fn send_object_rpc_session_to_target_async_with_daemon_paths_and_network_error(
    daemon_paths: &DaemonPaths,
    target_node_id: &NodeId,
    call: ObjectRpcSessionCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<ObjectRpcSessionResult, sinomo_core::network::NetworkError> {
    let data_dir = daemon_data_dir_from_paths(daemon_paths)?;
    let paths = sinomo_core::network::ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state(&paths)?;
    let mut host =
        DaemonSignedObjectRpcAsyncHost::new(runtime, observe_connectivity).with_paths(daemon_paths);
    let mut client =
        sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, runtime, &paths);
    client
        .call_target(&state, target_node_id, call, options)
        .await
}

fn block_on_daemon_client<F, T>(future: F) -> Result<T, DaemonError>
where
    F: std::future::Future<Output = Result<T, DaemonError>> + Send,
    T: Send,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            tokio::task::block_in_place(|| handle.block_on(future))
        } else {
            Err(DaemonError::Network {
                address: "daemon client runtime".to_string(),
                detail: "daemon RPC sync client cannot run inside a current-thread Tokio runtime; use the async or owner-runtime API".to_string(),
            })
        };
    }
    block_on_daemon_client_on_ephemeral_sync_runtime(future)
}

fn block_on_daemon_client_stream<F, T>(future: F) -> Result<T, DaemonError>
where
    F: std::future::Future<Output = Result<T, DaemonError>>,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(DaemonError::Network {
            address: "daemon client runtime".to_string(),
            detail: "streaming daemon RPC sync client cannot run inside a Tokio runtime; use send_daemon_rpc_stream_async".to_string(),
        });
    }
    block_on_daemon_client_on_ephemeral_sync_runtime(future)
}

fn block_on_daemon_client_on_owner_runtime<F, T>(
    runtime: &tokio::runtime::Handle,
    future: F,
) -> Result<T, DaemonError>
where
    F: std::future::Future<Output = Result<T, DaemonError>> + Send,
    T: Send,
{
    if let Ok(current) = tokio::runtime::Handle::try_current() {
        return if matches!(
            current.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            tokio::task::block_in_place(|| runtime.block_on(future))
        } else {
            Err(DaemonError::Network {
                address: "daemon client runtime".to_string(),
                detail: "daemon client owner-runtime bridge cannot run inside a current-thread Tokio runtime; use the async daemon client API".to_string(),
            })
        };
    }
    runtime.block_on(future)
}

fn block_on_daemon_client_on_ephemeral_sync_runtime<F, T>(future: F) -> Result<T, DaemonError>
where
    F: std::future::Future<Output = Result<T, DaemonError>>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| DaemonError::Network {
            address: "daemon client runtime".to_string(),
            detail: source.to_string(),
        })?;
    let output = runtime.block_on(future);
    runtime.shutdown_timeout(Duration::from_millis(250));
    output
}

pub(crate) struct DaemonSignedObjectRpcAsyncHost {
    runtime: tokio::runtime::Handle,
    observe_connectivity: bool,
    paths: Option<DaemonPaths>,
    use_tcp_tunnel: bool,
}

impl DaemonSignedObjectRpcAsyncHost {
    pub(crate) fn new(runtime: &tokio::runtime::Handle, observe_connectivity: bool) -> Self {
        Self {
            runtime: runtime.clone(),
            observe_connectivity,
            paths: None,
            use_tcp_tunnel: true,
        }
    }

    pub(crate) fn direct_only(runtime: &tokio::runtime::Handle) -> Self {
        Self {
            runtime: runtime.clone(),
            observe_connectivity: false,
            paths: None,
            use_tcp_tunnel: false,
        }
    }

    pub(crate) fn with_paths(mut self, paths: &DaemonPaths) -> Self {
        self.paths = Some(paths.clone());
        self
    }

    pub(crate) fn with_cluster_paths(mut self, paths: &sinomo_core::network::ClusterPaths) -> Self {
        self.paths = daemon_paths_from_cluster_paths(paths);
        self
    }

    fn signing_data_dir(&self) -> Result<sinomo_core::local::SinomoDataDir, NetworkError> {
        if let Some(paths) = self.paths.as_ref() {
            return daemon_data_dir_from_paths(paths);
        }
        sinomo_core::local::SinomoDataDir::from_env_or_home().map_err(|source| NetworkError::Io {
            operation: "cluster object RPC signing data directory".to_string(),
            detail: source.to_string(),
        })
    }

    fn tcp_tunnel_available(&self) -> bool {
        self.use_tcp_tunnel
            && self
                .paths
                .as_ref()
                .is_some_and(daemon_paths_have_settled_cluster_state)
    }
}

fn daemon_data_dir_from_paths(
    paths: &DaemonPaths,
) -> Result<sinomo_core::local::SinomoDataDir, NetworkError> {
    let Some(data_dir) = paths.root.parent() else {
        return Err(NetworkError::Io {
            operation: "daemon data directory".to_string(),
            detail: format!(
                "daemon path {} has no parent data directory",
                paths.root.display()
            ),
        });
    };
    Ok(sinomo_core::local::SinomoDataDir::new(data_dir))
}

fn daemon_paths_have_settled_cluster_state(paths: &DaemonPaths) -> bool {
    let Ok(data_dir) = daemon_data_dir_from_paths(paths) else {
        return false;
    };
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
    let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths) else {
        return false;
    };
    state.cluster_id.as_str() != sinomo_core::cluster::PENDING_INVITE_CLUSTER_ID
}

fn send_daemon_rpc_value<Req, Resp>(
    address: &str,
    route: &str,
    category: &str,
    value: &Req,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    let request = ObjectRpcRequest::encode_body(route, category, value)?;
    send_daemon_rpc_request_with_decoder(address, &request, timeout, |response| {
        response.decode_body()
    })
}

fn send_daemon_rpc_value_to_endpoint<Req, Resp>(
    endpoint: &ClusterEndpointRecord,
    route: &str,
    category: &str,
    value: &Req,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    let request = ObjectRpcRequest::encode_body(route, category, value)?;
    send_daemon_rpc_request_to_endpoint_with_decoder(endpoint, &request, timeout, |response| {
        response.decode_body()
    })
}

fn send_daemon_rpc_value_to_endpoint_on_runtime<Req, Resp>(
    endpoint: &ClusterEndpointRecord,
    route: &str,
    category: &str,
    value: &Req,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned + Send,
{
    let request = ObjectRpcRequest::encode_body(route, category, value)?;
    send_daemon_rpc_request_to_endpoint_with_decoder_on_runtime(
        endpoint,
        &request,
        timeout,
        runtime,
        |response| response.decode_body(),
    )
}

fn send_daemon_rollout_value<Req, Resp>(
    address: &str,
    route: &str,
    category: &str,
    value: &Req,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    let request = ObjectRpcRequest::encode_body(route, category, value)?;
    let response = send_daemon_rpc_with_timeouts(
        address,
        &request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
    )?;
    response.decode_body().map_err(Into::into)
}

fn send_daemon_rollout_value_to_endpoint<Req, Resp>(
    endpoint: &ClusterEndpointRecord,
    route: &str,
    category: &str,
    value: &Req,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    let request = ObjectRpcRequest::encode_body(route, category, value)?;
    let response = send_daemon_rpc_to_endpoint_with_timeouts(
        endpoint,
        &request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
    )?;
    response.decode_body().map_err(Into::into)
}

fn send_daemon_rpc_request_with_decoder<Resp, DecodeError>(
    address: &str,
    request: &ObjectRpcRequest,
    timeout: Duration,
    decode: impl FnOnce(&sinomo_core::network::ObjectRpcResponse) -> Result<Resp, DecodeError>,
) -> Result<Resp, DaemonError>
where
    DecodeError: Into<DaemonError>,
{
    let response = send_daemon_rpc(address, request, timeout)?;
    decode(&response).map_err(Into::into)
}

fn send_daemon_rpc_request_to_endpoint_with_decoder<Resp, DecodeError>(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeout: Duration,
    decode: impl FnOnce(&sinomo_core::network::ObjectRpcResponse) -> Result<Resp, DecodeError>,
) -> Result<Resp, DaemonError>
where
    DecodeError: Into<DaemonError>,
{
    let response = send_daemon_rpc_to_endpoint(endpoint, request, timeout)?;
    decode(&response).map_err(Into::into)
}

fn send_daemon_rpc_request_to_endpoint_with_decoder_on_runtime<Resp, Decode, DecodeError>(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
    decode: Decode,
) -> Result<Resp, DaemonError>
where
    Resp: Send,
    Decode: FnOnce(&sinomo_core::network::ObjectRpcResponse) -> Result<Resp, DecodeError> + Send,
    DecodeError: Into<DaemonError>,
{
    send_daemon_rpc_request_to_endpoint_with_decoder_on_runtime_with_connectivity_observation(
        endpoint, request, timeout, runtime, true, decode,
    )
}

fn send_daemon_rpc_request_to_endpoint_with_decoder_on_runtime_with_connectivity_observation<
    Resp,
    Decode,
    DecodeError,
>(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
    decode: Decode,
) -> Result<Resp, DaemonError>
where
    Resp: Send,
    Decode: FnOnce(&sinomo_core::network::ObjectRpcResponse) -> Result<Resp, DecodeError> + Send,
    DecodeError: Into<DaemonError>,
{
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(
        runtime,
        send_daemon_rpc_request_to_endpoint_with_decoder_async_with_connectivity_observation(
            endpoint,
            request,
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
            &runtime_for_future,
            observe_connectivity,
            decode,
        ),
    )
}

async fn send_daemon_rpc_request_to_endpoint_with_decoder_async<Resp, Decode, DecodeError>(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
    decode: Decode,
) -> Result<Resp, DaemonError>
where
    Decode: FnOnce(&sinomo_core::network::ObjectRpcResponse) -> Result<Resp, DecodeError>,
    DecodeError: Into<DaemonError>,
{
    send_daemon_rpc_request_to_endpoint_with_decoder_async_with_connectivity_observation(
        endpoint, request, timeouts, runtime, true, decode,
    )
    .await
}

async fn send_daemon_rpc_request_to_endpoint_with_decoder_async_with_connectivity_observation<
    Resp,
    Decode,
    DecodeError,
>(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
    decode: Decode,
) -> Result<Resp, DaemonError>
where
    Decode: FnOnce(&sinomo_core::network::ObjectRpcResponse) -> Result<Resp, DecodeError>,
    DecodeError: Into<DaemonError>,
{
    let response = send_daemon_rpc_to_endpoint_async_with_connectivity_observation(
        endpoint,
        request,
        timeouts,
        runtime,
        observe_connectivity,
    )
    .await?;
    decode(&response).map_err(Into::into)
}

impl sinomo_core::network::ClusterSignedObjectRpcAsyncHost for DaemonSignedObjectRpcAsyncHost {
    fn sign_object_rpc_request_async<'a>(
        &'a mut self,
        request: &'a ObjectRpcRequest,
    ) -> sinomo_core::network::ClusterSignedObjectRpcFuture<'a, Vec<u8>> {
        let route = request.route.clone();
        let category = request.category.clone();
        let body_sha256 = daemon_rpc_body_sha256(request);
        let runtime = self.runtime.clone();
        let data_dir = self.signing_data_dir();
        Box::pin(async move {
            let data_dir = data_dir?;
            runtime
                .spawn_blocking(move || {
                    sinomo_core::network::sign_daemon_rpc_request_digest_from_data_dir(
                        &data_dir,
                        &route,
                        &category,
                        body_sha256,
                    )
                })
                .await
                .map_err(|source| sinomo_core::network::NetworkError::Io {
                    operation: "cluster object RPC signing".to_string(),
                    detail: format!("object RPC signing task failed: {source}"),
                })?
        })
    }

    fn observe_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        observation: sinomo_core::network::ClusterSignedObjectRpcObservation,
    ) -> sinomo_core::network::ClusterSignedObjectRpcFuture<'a, ()> {
        if !self.observe_connectivity {
            return Box::pin(async { Ok(()) });
        }
        let runtime = self.runtime.clone();
        let paths = self
            .paths
            .as_ref()
            .and_then(|paths| crate::cluster::cluster_paths_from_daemon_paths(paths).ok());
        Box::pin(async move {
            let (connectivity_state, latency_ms, bypass_failure_grace, command_auth_denied) =
                daemon_rpc_connectivity_observation(observation);
            remember_endpoint_command_connectivity_async(
                runtime,
                paths,
                endpoint.clone(),
                connectivity_state,
                latency_ms,
                bypass_failure_grace,
                command_auth_denied,
            )
            .await;
            Ok(())
        })
    }

    fn open_object_rpc_session_stream_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        health: sinomo_core::network::NetworkHealthPolicy,
    ) -> sinomo_core::network::ClusterSignedObjectRpcFuture<
        'a,
        Box<dyn sinomo_core::network::NetworkDuplexStream>,
    > {
        let runtime = self.runtime.clone();
        let daemon_paths = self.paths.clone();
        let use_tcp_tunnel = self.tcp_tunnel_available() && daemon_endpoint_is_tcp_tunnel(endpoint);
        Box::pin(async move {
            if use_tcp_tunnel {
                return crate::tcp_tunnel::open_object_rpc_session_stream_via_tcp_tunnel(
                    endpoint,
                    health,
                    &runtime,
                    daemon_paths.as_ref(),
                )
                .await;
            }
            sinomo_core::network::open_daemon_rpc_stream_to_cluster_endpoint_async(endpoint, health)
                .await
        })
    }
}

fn daemon_paths_from_cluster_paths(
    paths: &sinomo_core::network::ClusterPaths,
) -> Option<DaemonPaths> {
    let data_dir = paths.root.parent()?;
    Some(DaemonPaths::new(&sinomo_core::local::SinomoDataDir::new(
        data_dir,
    )))
}

fn daemon_endpoint_is_tcp_tunnel(endpoint: &ClusterEndpointRecord) -> bool {
    endpoint.transport == sinomo_core::ClusterTransportKind::LanTcp
        && endpoint
            .address
            .starts_with(sinomo_core::network::CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX)
}

fn daemon_rpc_connectivity_observation(
    observation: sinomo_core::network::ClusterSignedObjectRpcObservation,
) -> (
    sinomo_core::network::ClusterConnectivityState,
    Option<u64>,
    bool,
    bool,
) {
    if observation.is_success() {
        (
            sinomo_core::network::ClusterConnectivityState::Healthy,
            observation.latency_ms(),
            false,
            false,
        )
    } else if observation.is_io_failure() {
        (
            sinomo_core::network::ClusterConnectivityState::Degraded,
            None,
            observation.is_hard_io_failure(),
            false,
        )
    } else if observation.is_auth_denied() {
        (
            sinomo_core::network::ClusterConnectivityState::Unreachable,
            None,
            false,
            true,
        )
    } else {
        (
            sinomo_core::network::ClusterConnectivityState::Degraded,
            None,
            false,
            false,
        )
    }
}

async fn remember_endpoint_command_connectivity_async(
    runtime: tokio::runtime::Handle,
    paths: Option<sinomo_core::network::ClusterPaths>,
    endpoint: ClusterEndpointRecord,
    connectivity_state: sinomo_core::network::ClusterConnectivityState,
    latency_ms: Option<u64>,
    bypass_failure_grace: bool,
    command_auth_denied: bool,
) {
    let Some(paths) = paths.or_else(cluster_paths_from_env) else {
        return;
    };
    let _ = runtime
        .spawn_blocking(move || {
            let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&paths) else {
                return;
            };
            if command_auth_denied {
                crate::cluster::remember_cluster_endpoint_command_auth_denied(
                    &paths, &state, &endpoint, None,
                );
            } else if bypass_failure_grace {
                crate::cluster::remember_cluster_endpoint_command_connectivity_without_failure_grace(
                    &paths,
                    &state,
                    &endpoint,
                    connectivity_state,
                    latency_ms,
                    None,
                );
            } else {
                crate::cluster::remember_cluster_endpoint_command_connectivity(
                    &paths,
                    &state,
                    &endpoint,
                    connectivity_state,
                    latency_ms,
                    None,
                );
            }
        })
        .await;
}

pub(crate) fn cluster_paths_from_env() -> Option<sinomo_core::network::ClusterPaths> {
    let data_dir = sinomo_core::local::SinomoDataDir::from_env_or_home().ok()?;
    Some(sinomo_core::network::ClusterPaths::new(&data_dir))
}

#[cfg(test)]
fn daemon_client_network_error(
    operation: &str,
    error: DaemonError,
) -> sinomo_core::network::NetworkError {
    match error {
        DaemonError::AuthDenied => sinomo_core::network::NetworkError::AuthDenied,
        DaemonError::AuthExpired => sinomo_core::network::NetworkError::AuthExpired,
        DaemonError::ClusterReadOnly { detail } => {
            sinomo_core::network::NetworkError::ClusterReadOnly { detail }
        }
        DaemonError::Network { address, detail } => sinomo_core::network::NetworkError::Io {
            operation: format!("{operation} {address}"),
            detail,
        },
        other => sinomo_core::network::NetworkError::Io {
            operation: operation.to_string(),
            detail: other.to_string(),
        },
    }
}

pub fn fetch_daemon_status_from_endpoint(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<DaemonStatus, DaemonError> {
    send_daemon_rpc_value_to_endpoint(
        endpoint,
        crate::DAEMON_STATUS_ROUTE,
        crate::DAEMON_RPC_CATEGORY,
        &(),
        timeout,
    )
}

pub(crate) fn fetch_daemon_status_from_endpoint_on_runtime(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonStatus, DaemonError> {
    send_daemon_rpc_value_to_endpoint_on_runtime(
        endpoint,
        crate::DAEMON_STATUS_ROUTE,
        crate::DAEMON_RPC_CATEGORY,
        &(),
        timeout,
        runtime,
    )
}

pub async fn fetch_daemon_status_from_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonStatus, DaemonError> {
    let request =
        ObjectRpcRequest::encode_body(crate::DAEMON_STATUS_ROUTE, crate::DAEMON_RPC_CATEGORY, &())?;
    send_daemon_rpc_request_to_endpoint_with_decoder_async(
        endpoint,
        &request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
        runtime,
        |response| response.decode_body(),
    )
    .await
}

pub async fn send_dfs_metadata_journal_batch_to_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    request: &DfsMetadataJournalBatchCommand,
    connect_timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<DfsMetadataJournalBatchResponse, DaemonError> {
    let rpc_request = sinomo_dfs::dfs_metadata_journal_batch_rpc_request(request)?;
    send_daemon_rpc_request_to_endpoint_with_decoder_async(
        endpoint,
        &rpc_request,
        sinomo_core::network::DaemonRpcTimeouts::default()
            .with_connect_timeout(Some(connect_timeout)),
        runtime,
        |response| sinomo_dfs::decode_dfs_metadata_journal_batch_rpc_response(response),
    )
    .await
}

pub async fn send_dfs_metadata_snapshot_install_to_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    request: &DfsMetadataSnapshotInstallCommand,
    connect_timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<DfsMetadataSnapshotInstallResponse, DaemonError> {
    let rpc_request = sinomo_dfs::dfs_metadata_snapshot_install_rpc_request(request)?;
    send_daemon_rpc_request_to_endpoint_with_decoder_async(
        endpoint,
        &rpc_request,
        sinomo_core::network::DaemonRpcTimeouts::default()
            .with_connect_timeout(Some(connect_timeout)),
        runtime,
        |response| sinomo_dfs::decode_dfs_metadata_snapshot_install_rpc_response(response),
    )
    .await
}

pub fn send_object_rpc_to_target(
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    connect_timeout: Duration,
    observe_connectivity: bool,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    send_object_rpc_to_target_with_timeouts(
        target_node_id,
        request,
        object_rpc_transfer_timeouts(connect_timeout),
        observe_connectivity,
    )
}

pub fn send_object_rpc_to_target_with_timeouts(
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    observe_connectivity: bool,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    send_object_rpc_to_target_with_options(
        target_node_id,
        request,
        sinomo_core::network::ClusterObjectRpcOptions::new(timeouts),
        observe_connectivity,
    )
}

pub fn send_object_rpc_to_target_with_options(
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    observe_connectivity: bool,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return send_object_rpc_to_target_with_options_on_runtime(
            target_node_id,
            request,
            options,
            &handle,
            observe_connectivity,
        );
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| DaemonError::Network {
            address: "daemon client runtime".to_string(),
            detail: source.to_string(),
        })?;
    let result = send_object_rpc_to_target_with_options_on_runtime(
        target_node_id,
        request,
        options,
        runtime.handle(),
        observe_connectivity,
    );
    runtime.shutdown_timeout(Duration::from_millis(250));
    result
}

pub fn send_object_rpc_session_to_target_with_options(
    target_node_id: &NodeId,
    call: ObjectRpcSessionCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    observe_connectivity: bool,
) -> Result<ObjectRpcSessionResult, DaemonError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return send_object_rpc_session_to_target_with_options_on_runtime(
            target_node_id,
            call,
            options,
            &handle,
            observe_connectivity,
        );
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| DaemonError::Network {
            address: "daemon client runtime".to_string(),
            detail: source.to_string(),
        })?;
    let result = send_object_rpc_session_to_target_with_options_on_runtime(
        target_node_id,
        call,
        options,
        runtime.handle(),
        observe_connectivity,
    );
    runtime.shutdown_timeout(Duration::from_millis(250));
    result
}

pub(crate) fn send_object_rpc_session_to_target_on_runtime_with_daemon_paths_and_options(
    daemon_paths: &DaemonPaths,
    target_node_id: &NodeId,
    call: ObjectRpcSessionCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<ObjectRpcSessionResult, DaemonError> {
    let daemon_paths = daemon_paths.clone();
    let target_node_id = target_node_id.clone();
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(runtime, async move {
        send_object_rpc_session_to_target_async_with_daemon_paths_and_options(
            &daemon_paths,
            &target_node_id,
            call,
            options,
            &runtime_for_future,
            observe_connectivity,
        )
        .await
    })
}

pub(crate) fn send_object_rpc_to_target_with_options_on_runtime(
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let result = send_object_rpc_session_to_target_with_options_on_runtime(
        target_node_id,
        ObjectRpcSessionCall::value(request.clone()),
        options,
        runtime,
        observe_connectivity,
    )?;
    Ok(require_daemon_client_value_response(result)?.response)
}

fn send_object_rpc_session_to_target_with_options_on_runtime(
    target_node_id: &NodeId,
    call: ObjectRpcSessionCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<ObjectRpcSessionResult, DaemonError> {
    let target_node_id = target_node_id.clone();
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(runtime, async move {
        send_object_rpc_session_to_target_async_with_options(
            &target_node_id,
            call,
            options,
            &runtime_for_future,
            observe_connectivity,
        )
        .await
    })
}

pub async fn send_object_rpc_to_target_async(
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    connect_timeout: Duration,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    send_object_rpc_to_target_async_with_options(
        target_node_id,
        request,
        sinomo_core::network::ClusterObjectRpcOptions::new(object_rpc_transfer_timeouts(
            connect_timeout,
        )),
        runtime,
        observe_connectivity,
    )
    .await
}

pub async fn send_object_rpc_to_target_async_with_options(
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let result = send_object_rpc_session_to_target_async_with_options(
        target_node_id,
        ObjectRpcSessionCall::value(request.clone()),
        options,
        runtime,
        observe_connectivity,
    )
    .await?;
    Ok(require_daemon_client_value_response(result)?.response)
}

#[cfg(test)]
pub(crate) async fn send_object_rpc_to_target_async_with_daemon_paths(
    daemon_paths: &DaemonPaths,
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    connect_timeout: Duration,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let result = send_object_rpc_session_to_target_async_with_daemon_paths_and_options(
        daemon_paths,
        target_node_id,
        ObjectRpcSessionCall::value(request.clone()),
        sinomo_core::network::ClusterObjectRpcOptions::new(object_rpc_transfer_timeouts(
            connect_timeout,
        )),
        runtime,
        observe_connectivity,
    )
    .await?;
    Ok(require_daemon_client_value_response(result)?.response)
}

pub(crate) fn object_rpc_transfer_timeouts(
    connect_timeout: Duration,
) -> sinomo_core::network::DaemonRpcTimeouts {
    // Transfer duration is unbounded while bytes keep moving. Only connection
    // setup is caller-configurable; every no-progress period uses the network
    // layer's common idle limit.
    sinomo_core::network::DaemonRpcTimeouts::default().with_connect_timeout(Some(connect_timeout))
}

#[cfg(test)]
pub(crate) fn object_rpc_forward_control_timeouts(
    connect_timeout: Duration,
) -> sinomo_core::network::DaemonRpcTimeouts {
    object_rpc_transfer_timeouts(connect_timeout)
}

pub fn fetch_daemon_status_from_address(
    address: &str,
    timeout: Duration,
) -> Result<DaemonStatus, DaemonError> {
    send_daemon_rpc_value(
        address,
        crate::DAEMON_STATUS_ROUTE,
        crate::DAEMON_RPC_CATEGORY,
        &(),
        timeout,
    )
}

pub fn send_daemon_cluster_maintenance_refresh(
    address: &str,
    timeout: Duration,
) -> Result<crate::DaemonClusterMaintenanceRefreshResponse, DaemonError> {
    send_daemon_rpc_value(
        address,
        crate::DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE,
        crate::DAEMON_RPC_CATEGORY,
        &crate::DaemonClusterMaintenanceRefreshRequest,
        timeout,
    )
}

pub fn send_daemon_cluster_maintenance_refresh_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<crate::DaemonClusterMaintenanceRefreshResponse, DaemonError> {
    send_daemon_rpc_value_to_endpoint(
        endpoint,
        crate::DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE,
        crate::DAEMON_RPC_CATEGORY,
        &crate::DaemonClusterMaintenanceRefreshRequest,
        timeout,
    )
}

pub fn send_daemon_shutdown_request(
    address: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    send_daemon_rpc_value(
        address,
        crate::DAEMON_SHUTDOWN_ROUTE,
        crate::DAEMON_RPC_CATEGORY,
        &(),
        timeout,
    )
}

fn decode_cluster_kv_snapshot_rpc_response(
    response: &sinomo_core::network::ObjectRpcResponse,
) -> Result<KvSnapshot, DaemonError> {
    response.error_detail()?;
    sinomo_core::kv::decode_cluster_kv_snapshot_rpc_body(&response.body).map_err(Into::into)
}

fn decode_cluster_raft_append_rpc_response(
    response: &sinomo_core::network::ObjectRpcResponse,
) -> Result<ClusterRaftAppendResponse, DaemonError> {
    response.error_detail()?;
    sinomo_core::kv::decode_cluster_raft_append_rpc_response_body(&response.body)
        .map_err(Into::into)
}

fn decode_cluster_raft_vote_rpc_response(
    response: &sinomo_core::network::ObjectRpcResponse,
) -> Result<ClusterRaftVoteResponse, DaemonError> {
    response.error_detail()?;
    sinomo_core::kv::decode_cluster_raft_vote_rpc_response_body(&response.body).map_err(Into::into)
}

pub(crate) fn fetch_cluster_kv_snapshot_from_endpoint_on_runtime(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<KvSnapshot, DaemonError> {
    let rpc_request =
        object_rpc_request_from_kv_payload(sinomo_core::kv::cluster_kv_snapshot_rpc_payload()?);
    send_daemon_rpc_request_to_endpoint_with_decoder_on_runtime(
        endpoint,
        &rpc_request,
        timeout,
        runtime,
        decode_cluster_kv_snapshot_rpc_response,
    )
}

pub(crate) fn send_cluster_raft_append_on_runtime(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRaftAppendRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterRaftAppendResponse, DaemonError> {
    let rpc_request = object_rpc_request_from_kv_payload(
        sinomo_core::kv::cluster_raft_append_rpc_payload(request)?,
    );
    send_daemon_rpc_request_to_endpoint_with_decoder_on_runtime(
        endpoint,
        &rpc_request,
        timeout,
        runtime,
        decode_cluster_raft_append_rpc_response,
    )
}

pub(crate) fn send_cluster_raft_vote_on_runtime(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRaftVoteRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterRaftVoteResponse, DaemonError> {
    let rpc_request = object_rpc_request_from_kv_payload(
        sinomo_core::kv::cluster_raft_vote_rpc_payload(request)?,
    );
    send_daemon_rpc_request_to_endpoint_with_decoder_on_runtime(
        endpoint,
        &rpc_request,
        timeout,
        runtime,
        decode_cluster_raft_vote_rpc_response,
    )
}

pub(crate) async fn send_cluster_member_publish_async(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterMemberPublishRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterMemberPublishResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_core::network::CLUSTER_MEMBER_PUBLISH_ROUTE,
        sinomo_core::network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        request,
    )?;
    send_daemon_rpc_request_to_endpoint_with_decoder_async(
        endpoint,
        &rpc_request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
        runtime,
        |response| response.decode_body(),
    )
    .await
}

pub fn send_daemon_build_capabilities(
    address: &str,
    request: &BuildCapabilitiesRequest,
) -> Result<BuildCapabilitiesResponse, DaemonError> {
    send_daemon_rpc_value(
        address,
        BUILD_CAPABILITIES_ROUTE,
        BUILD_RPC_CATEGORY,
        request,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    )
}

pub fn send_daemon_build_capabilities_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &BuildCapabilitiesRequest,
) -> Result<BuildCapabilitiesResponse, DaemonError> {
    send_daemon_rpc_value_to_endpoint(
        endpoint,
        BUILD_CAPABILITIES_ROUTE,
        BUILD_RPC_CATEGORY,
        request,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    )
}

pub fn send_daemon_build_capabilities_to_target(
    target_node_id: &NodeId,
    request: &BuildCapabilitiesRequest,
) -> Result<BuildCapabilitiesResponse, DaemonError> {
    send_daemon_build_head_to_target(BUILD_CAPABILITIES_ROUTE, request, target_node_id)
}

pub fn send_daemon_build_status(
    address: &str,
    request: &BuildStatusRequest,
) -> Result<BuildStatusResponse, DaemonError> {
    send_daemon_rpc_value(
        address,
        BUILD_STATUS_ROUTE,
        BUILD_RPC_CATEGORY,
        request,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    )
}

pub fn send_daemon_build_status_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &BuildStatusRequest,
) -> Result<BuildStatusResponse, DaemonError> {
    send_daemon_rpc_value_to_endpoint(
        endpoint,
        BUILD_STATUS_ROUTE,
        BUILD_RPC_CATEGORY,
        request,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    )
}

pub fn send_daemon_build_status_to_target(
    target_node_id: &NodeId,
    request: &BuildStatusRequest,
) -> Result<BuildStatusResponse, DaemonError> {
    send_daemon_build_head_to_target(BUILD_STATUS_ROUTE, request, target_node_id)
}

fn send_daemon_build_head_to_target<Req, Resp>(
    route: &str,
    request: &Req,
    target_node_id: &NodeId,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    let request = ObjectRpcRequest::encode_body(route, BUILD_RPC_CATEGORY, request)?;
    let response = send_object_rpc_to_target(
        target_node_id,
        &request,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        true,
    )?;
    response.decode_body().map_err(Into::into)
}

pub fn send_daemon_build_submit(
    address: &str,
    request: &BuildSubmitRequest,
    source_bundle: Option<&Path>,
) -> Result<BuildSubmitResponse, DaemonError> {
    let data_dir = local_session_data_dir()?;
    let address = address.to_string();
    let request = request.clone();
    let source_bundle = source_bundle.map(Path::to_path_buf);
    block_on_rollout_session(async move {
        let runtime = tokio::runtime::Handle::current();
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let mut host =
            DaemonSignedObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let call = build_submit_session_call(&request, source_bundle.as_deref())?;
        let result = if sinomo_core::network::is_loopback_connect_address(&address) {
            let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
            client
                .call_loopback(
                    &data_dir,
                    &address,
                    call,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        } else {
            let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
            let endpoint = local_session_endpoint_for_address(&state, &address)?;
            let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
                &mut host,
                &runtime,
                &cluster_paths,
            );
            client
                .call_endpoint(
                    &state,
                    &endpoint,
                    call,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        };
        decode_build_submit_session_result(result).await
    })
}

pub fn send_daemon_build_submit_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &BuildSubmitRequest,
    source_bundle: Option<&Path>,
) -> Result<BuildSubmitResponse, DaemonError> {
    let data_dir = local_session_data_dir()?;
    let endpoint = endpoint.clone();
    let request = request.clone();
    let source_bundle = source_bundle.map(Path::to_path_buf);
    block_on_rollout_session(async move {
        let runtime = tokio::runtime::Handle::current();
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let mut host =
            DaemonSignedObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let call = build_submit_session_call(&request, source_bundle.as_deref())?;
        let result = if sinomo_core::network::is_loopback_connect_address(&endpoint.address) {
            let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
            client
                .call_loopback(
                    &data_dir,
                    &endpoint.address,
                    call,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        } else {
            let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
            let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
                &mut host,
                &runtime,
                &cluster_paths,
            );
            client
                .call_endpoint(
                    &state,
                    &endpoint,
                    call,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        };
        decode_build_submit_session_result(result).await
    })
}

pub fn send_daemon_build_submit_to_target(
    target_node_id: &NodeId,
    request: &BuildSubmitRequest,
    source_bundle: Option<&Path>,
) -> Result<BuildSubmitResponse, DaemonError> {
    let (cluster_paths, state) = local_session_cluster_state()?;
    let target_node_id = target_node_id.clone();
    let request = request.clone();
    let source_bundle = source_bundle.map(Path::to_path_buf);
    block_on_rollout_session(async move {
        let runtime = tokio::runtime::Handle::current();
        let mut host =
            DaemonSignedObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
            &mut host,
            &runtime,
            &cluster_paths,
        );
        let result = client
            .call_target(
                &state,
                &target_node_id,
                build_submit_session_call(&request, source_bundle.as_deref())?,
                sinomo_core::network::ClusterObjectRpcOptions::default(),
            )
            .await?;
        decode_build_submit_session_result(result).await
    })
}

fn build_submit_session_call(
    request: &BuildSubmitRequest,
    source_bundle: Option<&Path>,
) -> Result<ObjectRpcSessionCall, DaemonError> {
    let body = match source_bundle {
        Some(path) => {
            let body = sinomo_core::network::ObjectRpcBodySource::file(path.to_path_buf())?;
            if body.total_bytes() != Some(request.plan.source.bundle_bytes) {
                return Err(DaemonError::Io {
                    path: path.display().to_string(),
                    detail: format!(
                        "build source bundle size changed after planning: expected {}, found {}",
                        request.plan.source.bundle_bytes,
                        body.total_bytes().unwrap_or_default()
                    ),
                });
            }
            body
        }
        None => sinomo_core::network::ObjectRpcBodySource::default(),
    };
    let head = ObjectRpcRequest::encode_body(BUILD_SUBMIT_ROUTE, BUILD_RPC_CATEGORY, request)?
        .with_operation_id(format!("build-submit:{}", request.plan.key.path_key()))
        .with_response_retry_safe();
    Ok(ObjectRpcSessionCall::new(head, body))
}

async fn decode_build_submit_session_result(
    result: ObjectRpcSessionResult,
) -> Result<BuildSubmitResponse, DaemonError> {
    let result = require_daemon_client_value_response(result)?;
    result.response.decode_body().map_err(Into::into)
}

pub fn send_daemon_build_artifact(
    address: &str,
    request: &BuildArtifactRequest,
    destination_path: &Path,
) -> Result<BuildArtifactResponse, DaemonError> {
    let data_dir = local_session_data_dir()?;
    let address = address.to_string();
    let request = request.clone();
    let destination_path = destination_path.to_path_buf();
    block_on_rollout_session(async move {
        let runtime = tokio::runtime::Handle::current();
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let mut host =
            DaemonSignedObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let call = build_artifact_session_call(&request)?;
        let result = if sinomo_core::network::is_loopback_connect_address(&address) {
            let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
            client
                .call_loopback(
                    &data_dir,
                    &address,
                    call,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        } else {
            let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
            let endpoint = local_session_endpoint_for_address(&state, &address)?;
            let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
                &mut host,
                &runtime,
                &cluster_paths,
            );
            client
                .call_endpoint(
                    &state,
                    &endpoint,
                    call,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        };
        receive_build_artifact_session_result(result, &request, &destination_path).await
    })
}

pub fn send_daemon_build_artifact_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &BuildArtifactRequest,
    destination_path: &Path,
) -> Result<BuildArtifactResponse, DaemonError> {
    let data_dir = local_session_data_dir()?;
    let endpoint = endpoint.clone();
    let request = request.clone();
    let destination_path = destination_path.to_path_buf();
    block_on_rollout_session(async move {
        let runtime = tokio::runtime::Handle::current();
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let mut host =
            DaemonSignedObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let call = build_artifact_session_call(&request)?;
        let result = if sinomo_core::network::is_loopback_connect_address(&endpoint.address) {
            let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
            client
                .call_loopback(
                    &data_dir,
                    &endpoint.address,
                    call,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        } else {
            let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
            let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
                &mut host,
                &runtime,
                &cluster_paths,
            );
            client
                .call_endpoint(
                    &state,
                    &endpoint,
                    call,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        };
        receive_build_artifact_session_result(result, &request, &destination_path).await
    })
}

pub fn send_daemon_build_artifact_to_target(
    target_node_id: &NodeId,
    request: &BuildArtifactRequest,
    destination_path: &Path,
) -> Result<BuildArtifactResponse, DaemonError> {
    let (cluster_paths, state) = local_session_cluster_state()?;
    let target_node_id = target_node_id.clone();
    let request = request.clone();
    let destination_path = destination_path.to_path_buf();
    block_on_rollout_session(async move {
        let runtime = tokio::runtime::Handle::current();
        let mut host =
            DaemonSignedObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
            &mut host,
            &runtime,
            &cluster_paths,
        );
        let result = client
            .call_target(
                &state,
                &target_node_id,
                build_artifact_session_call(&request)?,
                sinomo_core::network::ClusterObjectRpcOptions::default(),
            )
            .await?;
        receive_build_artifact_session_result(result, &request, &destination_path).await
    })
}

fn build_artifact_session_call(
    request: &BuildArtifactRequest,
) -> Result<ObjectRpcSessionCall, DaemonError> {
    let head = ObjectRpcRequest::encode_body(BUILD_ARTIFACT_ROUTE, BUILD_RPC_CATEGORY, request)?
        .with_operation_id(format!(
            "build-artifact:{}:{}",
            request.build_key.path_key(),
            request.artifact_id
        ))
        .with_response_retry_safe();
    Ok(ObjectRpcSessionCall::new(
        head,
        sinomo_core::network::ObjectRpcBodySource::default(),
    ))
}

async fn receive_build_artifact_session_result(
    result: ObjectRpcSessionResult,
    request: &BuildArtifactRequest,
    destination_path: &Path,
) -> Result<BuildArtifactResponse, DaemonError> {
    let mut result = result;
    let response: BuildArtifactResponse = result.response.decode_body()?;
    if response.artifact.artifact_id != request.artifact_id {
        return Err(DaemonError::Parse {
            detail: format!(
                "build artifact response id {} does not match requested {}",
                response.artifact.artifact_id, request.artifact_id
            ),
        });
    }
    if response.artifact.provenance.build_key != request.build_key {
        return Err(DaemonError::Parse {
            detail: format!(
                "build artifact response key {} does not match requested {}",
                response.artifact.provenance.build_key, request.build_key
            ),
        });
    }
    let parent = destination_path.parent().ok_or_else(|| DaemonError::Io {
        path: destination_path.display().to_string(),
        detail: "artifact destination has no parent directory".to_string(),
    })?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|source| DaemonError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    let partial = destination_path.with_extension(format!(
        "partial-{}-{}",
        std::process::id(),
        time::OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    let mut output = tokio::fs::File::create(&partial)
        .await
        .map_err(|source| DaemonError::Io {
            path: partial.display().to_string(),
            detail: source.to_string(),
        })?;
    let mut received = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = result
            .body
            .read(&mut buffer)
            .await
            .map_err(|source| DaemonError::Io {
                path: partial.display().to_string(),
                detail: format!("receive build artifact body: {source}"),
            })?;
        if count == 0 {
            break;
        }
        received = received.saturating_add(count as u64);
        if received > response.artifact.size_bytes {
            let _ = tokio::fs::remove_file(&partial).await;
            return Err(DaemonError::Parse {
                detail: format!(
                    "build artifact {} exceeded declared size {}",
                    response.artifact.artifact_id, response.artifact.size_bytes
                ),
            });
        }
        if let Err(source) = output.write_all(&buffer[..count]).await {
            let _ = tokio::fs::remove_file(&partial).await;
            return Err(DaemonError::Io {
                path: partial.display().to_string(),
                detail: source.to_string(),
            });
        }
    }
    if let Err(source) = output.flush().await {
        let _ = tokio::fs::remove_file(&partial).await;
        return Err(DaemonError::Io {
            path: partial.display().to_string(),
            detail: source.to_string(),
        });
    }
    if let Err(source) = output.sync_all().await {
        let _ = tokio::fs::remove_file(&partial).await;
        return Err(DaemonError::Io {
            path: partial.display().to_string(),
            detail: source.to_string(),
        });
    }
    drop(output);
    if let Err(error) = sinomo_build::verify_artifact_file(&response.artifact, &partial) {
        let _ = tokio::fs::remove_file(&partial).await;
        return Err(DaemonError::Parse {
            detail: error.to_string(),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if let Err(source) = tokio::fs::set_permissions(
            &partial,
            std::fs::Permissions::from_mode(response.artifact.mode),
        )
        .await
        {
            let _ = tokio::fs::remove_file(&partial).await;
            return Err(DaemonError::Io {
                path: partial.display().to_string(),
                detail: source.to_string(),
            });
        }
    }
    if destination_path.exists() {
        tokio::fs::remove_file(destination_path)
            .await
            .map_err(|source| DaemonError::Io {
                path: destination_path.display().to_string(),
                detail: source.to_string(),
            })?;
    }
    tokio::fs::rename(&partial, destination_path)
        .await
        .map_err(|source| DaemonError::Io {
            path: format!("{} -> {}", partial.display(), destination_path.display()),
            detail: source.to_string(),
        })?;
    Ok(response)
}

pub fn send_daemon_rollout_binary(
    address: &str,
    request: &ClusterRolloutBinaryRequest,
    attachment_path: &Path,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    block_on_rollout_session(send_daemon_rollout_binary_to_address_async(
        local_session_data_dir()?,
        address.to_string(),
        request.clone(),
        attachment_path.to_path_buf(),
    ))
}

pub fn send_daemon_rollout_binary_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBinaryRequest,
    attachment_path: &Path,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    // Loopback endpoint records are host-relative and can be advertised by
    // every cluster member. Resolving one through the shared member list can
    // pin a remote member key while the socket reaches this host's daemon.
    if sinomo_core::network::is_loopback_connect_address(&endpoint.address) {
        return block_on_rollout_session(send_daemon_rollout_binary_to_loopback_async(
            local_session_data_dir()?,
            endpoint.address.clone(),
            request.clone(),
            attachment_path.to_path_buf(),
        ));
    }
    let (cluster_paths, state) = local_session_cluster_state()?;
    send_daemon_rollout_binary_to_endpoint_with_state(
        &cluster_paths,
        &state,
        endpoint,
        request,
        attachment_path,
    )
}

pub fn send_daemon_rollout_binary_to_target(
    target_node_id: &NodeId,
    request: &ClusterRolloutBinaryRequest,
    attachment_path: &Path,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let (cluster_paths, state) = local_session_cluster_state()?;
    block_on_rollout_session(send_daemon_rollout_binary_to_target_async(
        cluster_paths,
        state,
        target_node_id.clone(),
        request.clone(),
        attachment_path.to_path_buf(),
    ))
}

fn local_session_cluster_state() -> Result<
    (
        sinomo_core::network::ClusterPaths,
        sinomo_core::ClusterBootstrapState,
    ),
    DaemonError,
> {
    let data_dir = local_session_data_dir()?;
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    Ok((cluster_paths, state))
}

fn local_session_data_dir() -> Result<sinomo_core::local::SinomoDataDir, DaemonError> {
    sinomo_core::local::SinomoDataDir::from_env_or_home().map_err(|source| DaemonError::Io {
        path: "Sinomo data directory".to_string(),
        detail: source.to_string(),
    })
}

fn local_session_endpoint_for_address(
    state: &sinomo_core::ClusterBootstrapState,
    address: &str,
) -> Result<ClusterEndpointRecord, DaemonError> {
    let mut matches = state
        .members
        .iter()
        .flat_map(|member| member.endpoints.iter())
        .filter(|endpoint| endpoint.address == address);
    if let Some(endpoint) = matches.next() {
        if let Some(conflict) = matches.find(|candidate| candidate.node_id != endpoint.node_id) {
            return Err(DaemonError::Network {
                address: address.to_string(),
                detail: format!(
                    "daemon session address is ambiguous between cluster nodes {} and {}; target a node or endpoint explicitly",
                    endpoint.node_id, conflict.node_id
                ),
            });
        }
        return Ok(endpoint.clone());
    }
    Err(DaemonError::Network {
        address: address.to_string(),
        detail: "daemon session endpoint is not authorized in local cluster state".to_string(),
    })
}

fn send_daemon_rollout_binary_to_endpoint_with_state(
    cluster_paths: &sinomo_core::network::ClusterPaths,
    state: &sinomo_core::ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBinaryRequest,
    attachment_path: &Path,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    block_on_rollout_session(send_daemon_rollout_binary_to_endpoint_async(
        cluster_paths.clone(),
        state.clone(),
        endpoint.clone(),
        request.clone(),
        attachment_path.to_path_buf(),
    ))
}

fn block_on_rollout_session<F, T>(future: F) -> Result<T, DaemonError>
where
    F: std::future::Future<Output = Result<T, DaemonError>> + Send,
    T: Send,
{
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        return block_on_daemon_client_on_owner_runtime(&runtime, future);
    }
    block_on_daemon_client_on_ephemeral_sync_runtime(future)
}

fn rollout_binary_session_call(
    request: &ClusterRolloutBinaryRequest,
    attachment_path: &Path,
) -> Result<sinomo_core::network::ObjectRpcSessionCall, DaemonError> {
    let artifact = request
        .manifest
        .artifact(&request.artifact_id)
        .ok_or_else(|| DaemonError::Parse {
            detail: format!(
                "rollout manifest does not contain artifact {}",
                request.artifact_id
            ),
        })?;
    let attachment =
        sinomo_core::network::ObjectRpcBodySource::file(attachment_path.to_path_buf())?;
    if attachment.total_bytes() != Some(artifact.total_bytes) {
        return Err(DaemonError::Io {
            path: attachment_path.display().to_string(),
            detail: format!(
                "rollout artifact size changed after preparation: expected {}, found {}",
                artifact.total_bytes,
                attachment.total_bytes().unwrap_or_default()
            ),
        });
    }
    let head = ObjectRpcRequest::encode_body(ROLLOUT_BINARY_ROUTE, ROLLOUT_RPC_CATEGORY, request)?
        .with_operation_id(format!(
            "{}:{}",
            request.manifest.upload_id, request.artifact_id
        ))
        .with_response_retry_safe();
    Ok(sinomo_core::network::ObjectRpcSessionCall::new(
        head, attachment,
    ))
}

async fn send_daemon_rollout_binary_to_address_async(
    data_dir: sinomo_core::local::SinomoDataDir,
    address: String,
    request: ClusterRolloutBinaryRequest,
    attachment_path: std::path::PathBuf,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    // Never resolve a loopback address through cluster endpoint records: the
    // same address can legitimately belong to every node in the snapshot.
    if sinomo_core::network::is_loopback_connect_address(&address) {
        return send_daemon_rollout_binary_to_loopback_async(
            data_dir,
            address,
            request,
            attachment_path,
        )
        .await;
    }
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let endpoint = local_session_endpoint_for_address(&state, &address)?;
    send_daemon_rollout_binary_to_endpoint_async(
        cluster_paths,
        state,
        endpoint,
        request,
        attachment_path,
    )
    .await
}

async fn send_daemon_rollout_binary_to_loopback_async(
    data_dir: sinomo_core::local::SinomoDataDir,
    address: String,
    request: ClusterRolloutBinaryRequest,
    attachment_path: std::path::PathBuf,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let runtime = tokio::runtime::Handle::current();
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
    let mut host =
        DaemonSignedObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
    let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
    let result = client
        .call_loopback(
            &data_dir,
            &address,
            rollout_binary_session_call(&request, &attachment_path)?,
            sinomo_core::network::ClusterObjectRpcOptions::default(),
        )
        .await?;
    decode_rollout_binary_session_result(result).await
}

async fn decode_rollout_binary_session_result(
    result: ObjectRpcSessionResult,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let result = require_daemon_client_value_response(result)?;
    result.response.decode_body().map_err(Into::into)
}

async fn send_daemon_rollout_binary_to_endpoint_async(
    cluster_paths: sinomo_core::network::ClusterPaths,
    state: sinomo_core::ClusterBootstrapState,
    endpoint: ClusterEndpointRecord,
    request: ClusterRolloutBinaryRequest,
    attachment_path: std::path::PathBuf,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let runtime = tokio::runtime::Handle::current();
    let mut host =
        DaemonSignedObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
    let mut client =
        sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &runtime, &cluster_paths);
    let result = client
        .call_endpoint(
            &state,
            &endpoint,
            rollout_binary_session_call(&request, &attachment_path)?,
            sinomo_core::network::ClusterObjectRpcOptions::default(),
        )
        .await?;
    decode_rollout_binary_session_result(result).await
}

async fn send_daemon_rollout_binary_to_target_async(
    cluster_paths: sinomo_core::network::ClusterPaths,
    state: sinomo_core::ClusterBootstrapState,
    target_node_id: NodeId,
    request: ClusterRolloutBinaryRequest,
    attachment_path: std::path::PathBuf,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let runtime = tokio::runtime::Handle::current();
    let mut host =
        DaemonSignedObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
    let mut client =
        sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &runtime, &cluster_paths);
    let result = client
        .call_target(
            &state,
            &target_node_id,
            rollout_binary_session_call(&request, &attachment_path)?,
            sinomo_core::network::ClusterObjectRpcOptions::default(),
        )
        .await?;
    decode_rollout_binary_session_result(result).await
}

pub fn send_daemon_rollout_native_dylib_module(
    address: &str,
    request: &ClusterRolloutNativeDylibModuleRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rollout_value(
        address,
        ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_native_dylib_module_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutNativeDylibModuleRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rollout_value_to_endpoint(
        endpoint,
        ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_build(
    address: &str,
    request: &ClusterRolloutBuildRequest,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let data_dir = local_session_data_dir()?;
    let address = address.to_string();
    let request = request.clone();
    block_on_rollout_session(async move {
        let runtime = tokio::runtime::Handle::current();
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let mut host =
            DaemonSignedObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let result = if sinomo_core::network::is_loopback_connect_address(&address) {
            let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
            client
                .call_loopback(
                    &data_dir,
                    &address,
                    rollout_build_session_call(request)?,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        } else {
            let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
            let endpoint = local_session_endpoint_for_address(&state, &address)?;
            let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
                &mut host,
                &runtime,
                &cluster_paths,
            );
            client
                .call_endpoint(
                    &state,
                    &endpoint,
                    rollout_build_session_call(request)?,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        };
        decode_rollout_build_session_result(result).await
    })
}

pub fn send_daemon_rollout_build_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBuildRequest,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let data_dir = local_session_data_dir()?;
    let endpoint = endpoint.clone();
    let request = request.clone();
    block_on_rollout_session(async move {
        let runtime = tokio::runtime::Handle::current();
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let mut host =
            DaemonSignedObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let result = if sinomo_core::network::is_loopback_connect_address(&endpoint.address) {
            let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
            client
                .call_loopback(
                    &data_dir,
                    &endpoint.address,
                    rollout_build_session_call(request)?,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        } else {
            let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
            let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
                &mut host,
                &runtime,
                &cluster_paths,
            );
            client
                .call_endpoint(
                    &state,
                    &endpoint,
                    rollout_build_session_call(request)?,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        };
        decode_rollout_build_session_result(result).await
    })
}

pub fn send_daemon_rollout_build_to_target(
    target_node_id: &NodeId,
    request: &ClusterRolloutBuildRequest,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let (cluster_paths, state) = local_session_cluster_state()?;
    let target_node_id = target_node_id.clone();
    let request = request.clone();
    block_on_rollout_session(async move {
        let runtime = tokio::runtime::Handle::current();
        let mut host =
            DaemonSignedObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
            &mut host,
            &runtime,
            &cluster_paths,
        );
        let result = client
            .call_target(
                &state,
                &target_node_id,
                rollout_build_session_call(request)?,
                sinomo_core::network::ClusterObjectRpcOptions::default(),
            )
            .await?;
        decode_rollout_build_session_result(result).await
    })
}

fn rollout_build_session_call(
    mut head_payload: ClusterRolloutBuildRequest,
) -> Result<ObjectRpcSessionCall, DaemonError> {
    let operation_id = format!("rollout-build:{}", head_payload.requested_at);
    let source_bundle = match head_payload.source_bundle.take() {
        Some(source_bundle) if !source_bundle.is_empty() => source_bundle,
        Some(source_bundle) => {
            head_payload.source_bundle = Some(source_bundle);
            Vec::new()
        }
        None => Vec::new(),
    };
    let head =
        ObjectRpcRequest::encode_body(ROLLOUT_BUILD_ROUTE, ROLLOUT_RPC_CATEGORY, &head_payload)?
            .with_operation_id(operation_id);
    Ok(ObjectRpcSessionCall::new(
        head,
        sinomo_core::network::ObjectRpcBodySource::bytes(source_bundle),
    ))
}

async fn decode_rollout_build_session_result(
    result: ObjectRpcSessionResult,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let result = require_daemon_client_value_response(result)?;
    result.response.decode_body().map_err(Into::into)
}

pub fn send_daemon_rollout_clear_status(
    address: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    send_daemon_rollout_value(
        address,
        ROLLOUT_CLEAR_STATUS_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        &(),
        timeout,
    )
}

pub fn send_daemon_rollout_clear_status_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<String, DaemonError> {
    send_daemon_rollout_value_to_endpoint(
        endpoint,
        ROLLOUT_CLEAR_STATUS_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        &(),
        timeout,
    )
}

pub fn send_daemon_rollout_diagnostics(
    address: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    let request =
        ObjectRpcRequest::encode_body(ROLLOUT_DIAGNOSTICS_ROUTE, ROLLOUT_RPC_CATEGORY, &())?
            .with_response_retry_safe();
    send_daemon_rpc_request_with_decoder(address, &request, timeout, |response| {
        response.decode_body()
    })
}

pub fn send_daemon_rollout_diagnostics_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<String, DaemonError> {
    let request =
        ObjectRpcRequest::encode_body(ROLLOUT_DIAGNOSTICS_ROUTE, ROLLOUT_RPC_CATEGORY, &())?
            .with_response_retry_safe();
    send_daemon_rpc_request_to_endpoint_with_decoder(endpoint, &request, timeout, |response| {
        response.decode_body()
    })
}

#[cfg(test)]
mod tests {
    use sinomo_core::ClusterEndpointPrivacy;
    use sinomo_core::base::{ClusterEndpointRecord, ClusterEndpointSource, ClusterTransportKind};
    use sinomo_core::network::ClusterEndpointId;
    use sinomo_core::network::ObjectRpcRequest;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    #[test]
    fn object_rpc_transfer_timeouts_use_default_no_progress_limit() {
        let timeouts = super::object_rpc_transfer_timeouts(Duration::from_secs(60));

        assert_eq!(timeouts.connect_timeout, Some(Duration::from_secs(60)));
        assert_eq!(
            timeouts.idle_timeout,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );
    }

    #[test]
    fn rollout_build_session_moves_source_bundle_out_of_request_head() {
        let source_bundle = vec![7_u8; 32 * 1024];
        let request = sinomo_rollout::ClusterRolloutBuildRequest {
            source: "test-source".to_string(),
            remote_repo: None,
            source_bundle: Some(source_bundle.clone()),
            source_head: Some("abc123".to_string()),
            force_source_overwrite: false,
            host: "127.0.0.1".to_string(),
            port: 21_353,
            shadow_port: 21_354,
            requested_at: "2026-07-12T00:00:00Z".to_string(),
        };

        let call = super::rollout_build_session_call(request).expect("session call");
        let head: sinomo_rollout::ClusterRolloutBuildRequest =
            call.request.decode_body().expect("request head");

        assert_eq!(head.source_bundle, None);
        assert_eq!(call.body.total_bytes(), Some(source_bundle.len() as u64));
        assert_eq!(
            call.request.operation_id(),
            Some("rollout-build:2026-07-12T00:00:00Z")
        );
        assert!(
            !call.request.is_response_retry_safe(),
            "remote builds are not idempotent and must not retry after possible delivery"
        );
    }

    #[tokio::test]
    async fn rollout_binary_loopback_session_pins_the_local_daemon_identity() {
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-binary-loopback-session");
        let data_dir = sinomo_core::local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main node identity");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");

        let bytes = b"loopback rollout body".to_vec();
        let attachment_path = root.join("candidate");
        std::fs::write(&attachment_path, &bytes).expect("candidate body");
        let artifact_id = "loopback-binary".to_string();
        let request = sinomo_rollout::ClusterRolloutBinaryRequest {
            manifest: sinomo_rollout::ClusterRolloutBinaryManifest {
                upload_id: "loopback-upload".to_string(),
                source: "test".to_string(),
                target_os: std::env::consts::OS.to_string(),
                target_arch: std::env::consts::ARCH.to_string(),
                host: "127.0.0.1".to_string(),
                port: 21_353,
                shadow_port: 21_354,
                requested_at: "2026-07-12T00:00:00Z".to_string(),
                binary: sinomo_rollout::ClusterRolloutUploadArtifactSpec {
                    artifact_id: artifact_id.clone(),
                    total_bytes: bytes.len() as u64,
                    blake3: blake3::hash(&bytes).as_bytes().to_vec(),
                },
                native_dylib_modules: Vec::new(),
            },
            artifact_id,
            activate: false,
        };
        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let server_data_dir = data_dir.clone();
        let expected = bytes.clone();
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept loopback session");
            let (mut incoming, responder) = sinomo_core::network::accept_object_rpc_server_session(
                Box::new(stream.into_inner()),
                server_data_dir,
                sinomo_core::network::NetworkCallPolicy::default(),
                |_request, _node_id, _public_key| Ok(()),
            )
            .await
            .expect("accept object RPC session");
            let mut received = Vec::new();
            incoming
                .body
                .read_to_end(&mut received)
                .await
                .expect("read rollout body");
            assert_eq!(received, expected);
            let response = sinomo_core::network::ObjectRpcResponse::encode_body(
                sinomo_rollout::ROLLOUT_BINARY_ROUTE,
                &sinomo_rollout::ClusterRolloutBinaryResponse {
                    status: "staged".to_string(),
                    detail: "received loopback body".to_string(),
                    main_binary: "main".into(),
                    shadow_binary: "shadow".into(),
                    source_head: None,
                },
            )
            .expect("response head");
            responder
                .respond(sinomo_core::network::ObjectRpcSessionResponse::value(
                    response,
                ))
                .await
                .expect("respond");
        });

        let response = super::send_daemon_rollout_binary_to_address_async(
            data_dir,
            address,
            request,
            attachment_path,
        )
        .await
        .expect("loopback binary rollout");
        server.await.expect("server task");

        assert_eq!(response.status, "staged");
        assert_eq!(response.detail, "received loopback body");
    }

    #[test]
    fn daemon_session_address_rejects_cross_node_ambiguity() {
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_ambiguous_session_address")
                .expect("cluster id");
        let first_node =
            sinomo_core::node::NodeId::from_legacy_named("node_ambiguous_session_first")
                .expect("first node");
        let second_node =
            sinomo_core::node::NodeId::from_legacy_named("node_ambiguous_session_second")
                .expect("second node");
        let now = time::OffsetDateTime::now_utc();
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "ambiguous session address",
            first_node.clone(),
            "first",
            "pk-first",
            now,
        );
        state.members[0].endpoints.push(ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_ambiguous_first")
                .expect("first endpoint"),
            node_id: first_node,
            transport: ClusterTransportKind::LanTcp,
            address: "198.51.100.7:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 0,
            last_observed_latency_ms: None,
        });
        let mut second = sinomo_core::ClusterMemberRecord::local_non_voting(
            cluster_id,
            second_node.clone(),
            "second",
            "pk-second",
            now,
        );
        second.endpoints.push(ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_ambiguous_second")
                .expect("second endpoint"),
            node_id: second_node,
            transport: ClusterTransportKind::LanTcp,
            address: "198.51.100.7:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 0,
            last_observed_latency_ms: None,
        });
        state.members.push(second);

        let error = super::local_session_endpoint_for_address(&state, "198.51.100.7:21353")
            .expect_err("cross-node address must be ambiguous");

        assert!(error.to_string().contains("address is ambiguous"));
        assert!(error.to_string().contains("target a node or endpoint"));
    }

    #[test]
    fn daemon_endpoint_tcp_tunnel_detection_requires_synthetic_address() {
        let node_id = sinomo_core::node::NodeId::from_legacy_named("node_tcp_tunnel_detection")
            .expect("node id");
        let endpoint_id = ClusterEndpointId::from_legacy_named("endpoint_tcp_tunnel_detection")
            .expect("endpoint id");
        let mut endpoint = ClusterEndpointRecord {
            endpoint_id,
            node_id,
            transport: ClusterTransportKind::LanTcp,
            address: "10.0.0.2:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 0,
            last_observed_latency_ms: None,
        };

        assert!(!super::daemon_endpoint_is_tcp_tunnel(&endpoint));

        endpoint.address = format!(
            "{}{}",
            sinomo_core::network::CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX,
            endpoint.node_id.as_str()
        );
        assert!(super::daemon_endpoint_is_tcp_tunnel(&endpoint));

        endpoint.transport = ClusterTransportKind::Loopback;
        assert!(!super::daemon_endpoint_is_tcp_tunnel(&endpoint));
    }

    #[test]
    fn daemon_endpoint_host_does_not_tunnel_real_tcp_endpoint_with_stale_tunnel() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-client-real-endpoint-no-tunnel");
        let data_dir = sinomo_core::local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let daemon_paths = crate::DaemonPaths::new(&data_dir);
        daemon_paths.ensure().expect("daemon paths");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");

        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_client_real_endpoint")
                .expect("cluster id");
        let local_node =
            sinomo_core::node::NodeId::from_legacy_named("node_client_real_endpoint_local")
                .expect("local node");
        let target_node =
            sinomo_core::node::NodeId::from_legacy_named("node_client_real_endpoint_target")
                .expect("target node");
        let local_identity = sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("local identity");
        let target_root = sinomo_testdir::TestDir::new("sinomo-daemon-client-real-endpoint-target");
        let target_data_dir = sinomo_core::local::SinomoDataDir::new(&target_root);
        target_data_dir.ensure_root_layout().expect("target layout");
        let target_identity =
            sinomo_core::node::load_or_create_main_node_identity(&target_data_dir.root())
                .expect("target identity");
        let now = time::OffsetDateTime::now_utc();
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "client real endpoint",
            local_node,
            "local",
            local_identity.public_key,
            now,
        );
        state
            .members
            .push(sinomo_core::ClusterMemberRecord::local_non_voting(
                cluster_id,
                target_node.clone(),
                "target",
                target_identity.public_key,
                now,
            ));
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let (tunnel_tx, mut tunnel_rx) = tokio::sync::mpsc::channel(1);
        crate::tcp_tunnel::insert_test_tcp_tunnel(target_node.clone(), tunnel_tx);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let result = runtime.block_on(async {
            let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
                .await
                .expect("listener");
            let address = listener.local_addr_string().expect("listener address");
            let server = tokio::spawn(async move {
                let stream = listener.accept().await.expect("accept direct endpoint");
                let (request, responder) = sinomo_core::network::accept_object_rpc_server_session(
                    Box::new(stream.into_inner()),
                    target_data_dir,
                    sinomo_core::network::NetworkCallPolicy::default(),
                    |_request, _node_id, _public_key| Ok(()),
                )
                .await
                .expect("direct session request");
                assert!(request.body.is_empty());
                assert_eq!(request.request.route, "/test/direct-endpoint");
                let response = sinomo_core::network::ObjectRpcResponse::encode_body(
                    request.request.route,
                    &"direct-ok",
                )
                .expect("direct response");
                responder
                    .respond(sinomo_core::network::ObjectRpcSessionResponse::value(
                        response,
                    ))
                    .await
                    .expect("write direct response");
            });

            let endpoint = ClusterEndpointRecord {
                endpoint_id: ClusterEndpointId::from_legacy_named(
                    "endpoint_client_real_endpoint_direct",
                )
                .expect("endpoint"),
                node_id: target_node.clone(),
                transport: ClusterTransportKind::LanTcp,
                address,
                source: ClusterEndpointSource::Static,
                privacy: ClusterEndpointPrivacy::Lan,
                priority: 0,
                last_observed_latency_ms: None,
            };
            let request = ObjectRpcRequest::encode_body("/test/direct-endpoint", "test", &())
                .expect("request");
            let mut host = super::DaemonSignedObjectRpcAsyncHost::new(runtime.handle(), false)
                .with_paths(&daemon_paths);
            let response = sinomo_core::network::ClusterObjectRpcClient::routed(
                &mut host,
                runtime.handle(),
                &cluster_paths,
            )
            .call_endpoint(
                &state,
                &endpoint,
                sinomo_core::network::ObjectRpcSessionCall::value(request),
                sinomo_core::network::ClusterObjectRpcOptions::new(
                    sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(
                        1,
                    )),
                ),
            )
            .await
            .expect("direct endpoint response");
            assert!(response.body.is_empty());
            server.await.expect("direct endpoint server");
            assert!(
                tunnel_rx.try_recv().is_err(),
                "real endpoint traffic must not be sent through the stale tunnel"
            );
            response
                .response
                .decode_body::<String>()
                .expect("response body")
        });
        crate::tcp_tunnel::remove_test_tcp_tunnel(&target_node);

        assert_eq!(result, "direct-ok");
    }

    #[test]
    #[ignore = "requires SINOMO_LIVE_DFS_STATUS_ENDPOINT and SINOMO_LIVE_DFS_STATUS_NODE_ID"]
    fn live_signed_dfs_status_endpoint_from_env() {
        let endpoint_address = std::env::var("SINOMO_LIVE_DFS_STATUS_ENDPOINT")
            .expect("SINOMO_LIVE_DFS_STATUS_ENDPOINT");
        let node_id = sinomo_core::node::NodeId::new(
            std::env::var("SINOMO_LIVE_DFS_STATUS_NODE_ID")
                .expect("SINOMO_LIVE_DFS_STATUS_NODE_ID"),
        )
        .expect("valid SINOMO_LIVE_DFS_STATUS_NODE_ID");
        let endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::derived(
                "sinomo.test.live-dfs-status",
                endpoint_address.as_str(),
            ),
            node_id,
            transport: ClusterTransportKind::LanTcp,
            address: endpoint_address,
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 0,
            last_observed_latency_ms: None,
        };
        let request = sinomo_dfs::dfs_status_rpc_request().expect("dfs status request");

        let status = super::send_daemon_rpc_request_to_endpoint_with_decoder(
            &endpoint,
            &request,
            Duration::from_secs(5),
            sinomo_dfs::decode_dfs_status_rpc_response,
        )
        .expect("signed DFS status response");

        assert!(
            status.file_system_count > 0,
            "live DFS status should report at least one file system"
        );
    }

    #[test]
    #[ignore = "requires SINOMO_LIVE_DFS_STATUS_NODE_ID"]
    fn live_routed_dfs_status_with_daemon_paths_from_env() {
        let node_id = sinomo_core::node::NodeId::new(
            std::env::var("SINOMO_LIVE_DFS_STATUS_NODE_ID")
                .expect("SINOMO_LIVE_DFS_STATUS_NODE_ID"),
        )
        .expect("valid SINOMO_LIVE_DFS_STATUS_NODE_ID");
        let data_dir = sinomo_core::local::SinomoDataDir::from_env_or_home().expect("data dir");
        let paths = crate::DaemonPaths::new(&data_dir);
        let request = sinomo_dfs::dfs_status_rpc_request().expect("dfs status request");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .thread_name("live-routed-dfs-status")
            .build()
            .expect("runtime");

        let response = runtime
            .block_on(super::send_object_rpc_to_target_async_with_daemon_paths(
                &paths,
                &node_id,
                &request,
                Duration::from_secs(20),
                runtime.handle(),
                true,
            ))
            .expect("routed DFS status response");
        let status = sinomo_dfs::decode_dfs_status_rpc_response(&response)
            .expect("decode DFS status response");

        assert!(
            status.file_system_count > 0,
            "live routed DFS status should report at least one file system"
        );
    }

    #[test]
    #[ignore = "requires SINOMO_LIVE_DFS_STATUS_ENDPOINT and SINOMO_LIVE_DFS_STATUS_NODE_ID"]
    fn live_endpoint_dfs_status_with_daemon_paths_from_env() {
        let endpoint_address = std::env::var("SINOMO_LIVE_DFS_STATUS_ENDPOINT")
            .expect("SINOMO_LIVE_DFS_STATUS_ENDPOINT");
        let node_id = sinomo_core::node::NodeId::new(
            std::env::var("SINOMO_LIVE_DFS_STATUS_NODE_ID")
                .expect("SINOMO_LIVE_DFS_STATUS_NODE_ID"),
        )
        .expect("valid SINOMO_LIVE_DFS_STATUS_NODE_ID");
        let endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::derived(
                "sinomo.test.live-dfs-status",
                endpoint_address.as_str(),
            ),
            node_id,
            transport: ClusterTransportKind::LanTcp,
            address: endpoint_address,
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::InternetDirect,
            priority: 0,
            last_observed_latency_ms: None,
        };
        let data_dir = sinomo_core::local::SinomoDataDir::from_env_or_home().expect("data dir");
        let paths = crate::DaemonPaths::new(&data_dir);
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("cluster state");
        let request = sinomo_dfs::dfs_status_rpc_request().expect("dfs status request");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .thread_name("live-endpoint-dfs-status")
            .build()
            .expect("runtime");
        let mut host =
            super::DaemonSignedObjectRpcAsyncHost::new(runtime.handle(), true).with_paths(&paths);
        let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
            &mut host,
            runtime.handle(),
            &cluster_paths,
        );

        let result = runtime
            .block_on(client.call_endpoint(
                &state,
                &endpoint,
                sinomo_core::network::ObjectRpcSessionCall::value(request),
                sinomo_core::network::ClusterObjectRpcOptions::new(
                    sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(
                        5,
                    )),
                ),
            ))
            .expect("endpoint DFS status response");
        assert!(
            result.body.is_empty(),
            "DFS status value response must not contain an attachment"
        );
        let response = result.response;
        let status = sinomo_dfs::decode_dfs_status_rpc_response(&response)
            .expect("decode DFS status response");

        assert!(
            status.file_system_count > 0,
            "live endpoint DFS status should report at least one file system"
        );
    }

    #[test]
    fn object_rpc_forward_control_timeouts_bound_silent_local_forward() {
        let timeouts = super::object_rpc_forward_control_timeouts(Duration::from_secs(60));

        assert_eq!(timeouts.connect_timeout, Some(Duration::from_secs(60)));
        assert_eq!(
            timeouts.idle_timeout,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );
    }

    #[test]
    fn daemon_rpc_sync_client_rejects_current_thread_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let request = ObjectRpcRequest::encode_body(
            crate::DAEMON_STATUS_ROUTE,
            crate::DAEMON_RPC_CATEGORY,
            &(),
        )
        .expect("request");

        let error = runtime.block_on(async {
            super::send_daemon_rpc_with_timeouts(
                "127.0.0.1:21353",
                &request,
                sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(Duration::from_millis(1)),
            )
            .expect_err("sync client must not create an ephemeral runtime from async code")
        });

        match error {
            crate::DaemonError::Network { address, detail } => {
                assert_eq!(address, "daemon client runtime");
                assert!(detail.contains("current-thread Tokio runtime"));
            }
            error => panic!("unexpected error: {error}"),
        }
    }

    #[test]
    fn daemon_rpc_owner_runtime_bridge_rejects_current_thread_runtime() {
        let owner_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("owner runtime");
        let caller_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("caller runtime");

        let error = caller_runtime.block_on(async {
            super::block_on_daemon_client_on_owner_runtime(owner_runtime.handle(), async {
                Ok::<_, crate::DaemonError>(())
            })
            .expect_err("owner-runtime bridge must not spawn a helper thread")
        });

        match error {
            crate::DaemonError::Network { address, detail } => {
                assert_eq!(address, "daemon client runtime");
                assert!(detail.contains("owner-runtime bridge"));
                assert!(detail.contains("current-thread Tokio runtime"));
            }
            error => panic!("unexpected error: {error}"),
        }
    }

    #[test]
    fn daemon_endpoint_host_direct_only_disables_tcp_tunnel() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let direct = super::DaemonSignedObjectRpcAsyncHost::direct_only(runtime.handle());
        assert!(!direct.observe_connectivity);
        assert!(!direct.use_tcp_tunnel);
        assert!(!direct.tcp_tunnel_available());

        let routed = super::DaemonSignedObjectRpcAsyncHost::new(runtime.handle(), true);
        assert!(routed.observe_connectivity);
        assert!(routed.use_tcp_tunnel);
        assert!(!routed.tcp_tunnel_available());

        let root = sinomo_testdir::TestDir::new("sinomo-daemon-client-tcp-tunnel-availability");
        let data_dir = sinomo_core::local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let daemon_paths = crate::DaemonPaths::new(&data_dir);
        daemon_paths.ensure().expect("daemon paths");

        let pre_cluster = super::DaemonSignedObjectRpcAsyncHost::new(runtime.handle(), true)
            .with_paths(&daemon_paths);
        assert!(!pre_cluster.tcp_tunnel_available());

        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_client_tunnel_available")
                .expect("cluster id");
        let local_node =
            sinomo_core::node::NodeId::from_legacy_named("node_client_tunnel_available")
                .expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "client tunnel available",
            local_node,
            "local",
            "pk-local",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("state");

        let clustered = super::DaemonSignedObjectRpcAsyncHost::new(runtime.handle(), true)
            .with_paths(&daemon_paths);
        assert!(clustered.tcp_tunnel_available());
    }

    #[test]
    fn daemon_signed_endpoint_application_auth_denial_does_not_retry_or_poison_connectivity() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-client-auth-denied-observation");
        let data_dir = sinomo_core::local::SinomoDataDir::new(&root);
        let daemon_paths = crate::DaemonPaths::with_root(root.join("daemon"));
        let local_identity =
            sinomo_core::node::load_or_create_main_node_identity(&root).expect("identity");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_client_auth_denied")
                .expect("cluster id");
        let local_node =
            sinomo_core::node::NodeId::from_legacy_named("node_client_auth_denied_local")
                .expect("local node");
        let target_node =
            sinomo_core::node::NodeId::from_legacy_named("node_client_auth_denied_target")
                .expect("target node");
        let target_root = sinomo_testdir::TestDir::new("sinomo-daemon-client-auth-denied-target");
        let target_data_dir = sinomo_core::local::SinomoDataDir::new(&target_root);
        target_data_dir.ensure_root_layout().expect("target layout");
        let target_identity =
            sinomo_core::node::load_or_create_main_node_identity(&target_data_dir.root())
                .expect("target identity");
        let now = time::OffsetDateTime::now_utc();
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "client-auth-denied",
            local_node.clone(),
            "local",
            local_identity.public_key,
            now,
        );
        state
            .members
            .push(sinomo_core::ClusterMemberRecord::local_non_voting(
                cluster_id,
                target_node.clone(),
                "target",
                target_identity.public_key,
                now,
            ));
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let endpoint_id =
            sinomo_core::network::ClusterEndpointId::from_legacy_named("endpoint_auth_denied")
                .expect("endpoint");
        let result = runtime.block_on(async {
            let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
                .await
                .expect("listener");
            let address = listener.local_addr_string().expect("listener address");
            let server = tokio::spawn(async move {
                let stream = listener.accept().await.expect("accept");
                let (request, responder) = sinomo_core::network::accept_object_rpc_server_session(
                    Box::new(stream.into_inner()),
                    target_data_dir,
                    sinomo_core::network::NetworkCallPolicy::default(),
                    |_request, _node_id, _public_key| Ok(()),
                )
                .await
                .expect("request session");
                let auth: sinomo_core::network::SignedDaemonCommand =
                    sinomo_core::network::SinomoObjectCodec::default()
                        .decode(request.request.auth.as_deref().expect("signed request"))
                        .expect("auth envelope");
                let expected_digest =
                    sinomo_core::network::daemon_rpc_body_sha256(&request.request);
                assert_eq!(
                    auth.rpc_body_sha256.as_deref(),
                    Some(expected_digest.as_slice())
                );
                let response = sinomo_core::network::ObjectRpcResponse::encode_error(
                    request.request.route,
                    "network authorization was denied",
                )
                .expect("auth denied response");
                responder
                    .respond(sinomo_core::network::ObjectRpcSessionResponse::value(
                        response,
                    ))
                    .await
                    .expect("response");
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), listener.accept())
                        .await
                        .is_err(),
                    "ordinary endpoint RPC must not retry with legacy auth"
                );
            });

            let endpoint = sinomo_core::ClusterEndpointRecord {
                endpoint_id: endpoint_id.clone(),
                node_id: target_node.clone(),
                transport: sinomo_core::ClusterTransportKind::Loopback,
                address,
                source: sinomo_core::ClusterEndpointSource::Static,
                privacy: sinomo_core::ClusterEndpointPrivacy::LocalOnly,
                priority: 0,
                last_observed_latency_ms: None,
            };
            let mut host = super::DaemonSignedObjectRpcAsyncHost::new(runtime.handle(), true)
                .with_paths(&daemon_paths);
            // Bind request authentication and canonical session proofs to the same
            // test identity; an endpoint-only client would resolve proofs from the
            // ambient data directory instead.
            let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
                &mut host,
                runtime.handle(),
                &cluster_paths,
            );
            let request = ObjectRpcRequest::encode_body("/test/auth-denied", "test", &())
                .expect("request")
                .with_metadata("sender-controlled", "must-stay-bound");
            let result = client
                .call_endpoint(
                    &state,
                    &endpoint,
                    sinomo_core::network::ObjectRpcSessionCall::value(request),
                    sinomo_core::network::ClusterObjectRpcOptions::new(
                        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(
                            Duration::from_secs(1),
                        ),
                    ),
                )
                .await;
            server.await.expect("server");
            result
        });
        assert!(matches!(
            result,
            Err(sinomo_core::network::NetworkError::AuthDenied)
        ));

        let records = sinomo_core::network::read_local_cluster_connectivity_records_for_source(
            &cluster_paths,
            &local_node,
        )
        .expect("connectivity records");
        let record = records
            .iter()
            .find(|record| {
                record.target_node_id == target_node && record.endpoint_id == endpoint_id
            })
            .expect("auth-denied endpoint record");
        assert_eq!(
            record.state,
            sinomo_core::network::ClusterConnectivityState::Healthy
        );
        assert_eq!(record.consecutive_failures, 0);
        assert!(record.last_success_at.is_some());
        assert!(record.failure_reason.is_none());
    }

    #[test]
    fn daemon_rpc_connectivity_observation_classifies_endpoint_failures() {
        let healthy = super::daemon_rpc_connectivity_observation(
            sinomo_core::network::ClusterSignedObjectRpcObservation::success(Some(5)),
        );
        assert_eq!(
            healthy.0,
            sinomo_core::network::ClusterConnectivityState::Healthy
        );
        assert_eq!(healthy.1, Some(5));

        let degraded = super::daemon_rpc_connectivity_observation(
            sinomo_core::network::ClusterSignedObjectRpcObservation::io_failure(),
        );

        let hard_failure = super::daemon_rpc_connectivity_observation(
            sinomo_core::network::ClusterSignedObjectRpcObservation::hard_io_failure(),
        );
        assert_eq!(
            hard_failure,
            (
                sinomo_core::network::ClusterConnectivityState::Degraded,
                None,
                true,
                false
            )
        );
        assert_eq!(
            degraded,
            (
                sinomo_core::network::ClusterConnectivityState::Degraded,
                None,
                false,
                false
            )
        );

        let auth_denied = super::daemon_rpc_connectivity_observation(
            sinomo_core::network::ClusterSignedObjectRpcObservation::auth_denied(),
        );
        assert_eq!(
            auth_denied,
            (
                sinomo_core::network::ClusterConnectivityState::Unreachable,
                None,
                false,
                true
            )
        );
    }

    #[test]
    fn daemon_client_network_error_preserves_auth_classification() {
        assert!(matches!(
            super::daemon_client_network_error("cluster endpoint", crate::DaemonError::AuthDenied),
            sinomo_core::network::NetworkError::AuthDenied
        ));
        assert!(matches!(
            super::daemon_client_network_error(
                "cluster endpoint",
                crate::DaemonError::ClusterReadOnly {
                    detail: "quorum unavailable".to_string(),
                },
            ),
            sinomo_core::network::NetworkError::ClusterReadOnly { .. }
        ));
    }
}
