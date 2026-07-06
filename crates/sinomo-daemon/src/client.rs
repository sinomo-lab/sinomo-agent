use crate::{
    DaemonError, DaemonPaths, DaemonStatus, daemon_rpc_body_sha256,
    sign_daemon_rpc_request_digest_for_local_node, sign_daemon_rpc_request_for_local_node,
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
    ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse, ClusterRaftAppendRequest,
    ClusterRaftAppendResponse, ClusterRaftVoteRequest, ClusterRaftVoteResponse, KvSnapshot,
};
use sinomo_core::network::{AsyncDaemonControlStream, ObjectRpcRequest};
use sinomo_core::node::NodeId;
use sinomo_dfs::{
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse,
    DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse,
};
use sinomo_rollout::{
    ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse,
    ClusterRolloutBinaryUploadChunkRequest, ClusterRolloutBinaryUploadChunkResponse,
    ClusterRolloutBinaryUploadFinalizeRequest, ClusterRolloutBuildRequest,
    ClusterRolloutNativeDylibModuleRequest, ROLLOUT_BINARY_ROUTE,
    ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE, ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE, ROLLOUT_BUILD_ROUTE,
    ROLLOUT_CLEAR_STATUS_ROUTE, ROLLOUT_DIAGNOSTICS_ROUTE, ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
    ROLLOUT_RPC_CATEGORY,
};
use std::time::Duration;

const OBJECT_RPC_TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

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
    let signed = sign_daemon_rpc_request_for_local_node(request)?;
    send_daemon_rpc_raw(address, Some(&signed), request, timeout)
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
    let route = request.route.clone();
    let category = request.category.clone();
    let body_sha256 = daemon_rpc_body_sha256(request);
    let signed = runtime
        .spawn_blocking(move || {
            sign_daemon_rpc_request_digest_for_local_node(&route, &category, body_sha256)
        })
        .await
        .map_err(|source| DaemonError::Network {
            address: address.to_string(),
            detail: format!("daemon RPC signing task failed: {source}"),
        })??;
    let control_request = request.clone().with_auth(signed);
    let response = sinomo_core::network::send_daemon_control_rpc_request_async(
        address,
        &control_request,
        timeouts,
        sinomo_core::network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
    )
    .await?;
    Ok(response)
}

pub(crate) fn send_daemon_rpc_raw(
    address: &str,
    auth: Option<&[u8]>,
    request: &ObjectRpcRequest,
    timeout: Duration,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    send_daemon_rpc_raw_with_timeouts(
        address,
        auth,
        request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
    )
}

/// Like [`send_daemon_rpc`], but lets callers provide separate connect and idle
/// deadlines. Use an idle deadline for RPCs that must never hang on a silent
/// peer, and omit it for long-running operations that report progress elsewhere.
pub fn send_daemon_rpc_with_timeouts(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let signed = sign_daemon_rpc_request_for_local_node(request)?;
    send_daemon_rpc_raw_with_timeouts(address, Some(&signed), request, timeouts)
}

pub(crate) fn send_daemon_rpc_raw_with_timeouts(
    address: &str,
    auth: Option<&[u8]>,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    block_on_daemon_client(async {
        let control_request = match auth {
            Some(auth) => request.clone().with_auth(auth.to_vec()),
            None => request.clone(),
        };
        sinomo_core::network::send_daemon_control_rpc_request_async(
            address,
            &control_request,
            timeouts,
            sinomo_core::network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
        )
        .await
        .map_err(DaemonError::from)
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
        sinomo_core::network::DaemonRpcTimeouts::connect_only(timeout),
        &mut on_response,
    ))
}

pub async fn send_daemon_rpc_stream_async(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    mut on_response: impl FnMut(sinomo_core::network::ObjectRpcResponse) -> Result<bool, DaemonError>,
) -> Result<(), DaemonError> {
    let route = request.route.clone();
    let category = request.category.clone();
    let body_sha256 = daemon_rpc_body_sha256(request);
    let signed = tokio::task::spawn_blocking(move || {
        sign_daemon_rpc_request_digest_for_local_node(&route, &category, body_sha256)
    })
    .await
    .map_err(|source| DaemonError::Network {
        address: address.to_string(),
        detail: format!("daemon streaming RPC signing task failed: {source}"),
    })??;
    let control_request = request.clone().with_auth(signed);
    let mut stream = AsyncDaemonControlStream::connect_with_options(address, timeouts).await?;
    let frame_options =
        sinomo_core::network::ObjectFrameIoOptions::none().with_idle_timeout(timeouts.idle_timeout);
    stream
        .write_request(address, &control_request, frame_options)
        .await?;
    stream.shutdown_write(address).await?;
    loop {
        let rpc = stream
            .read_response(
                address,
                sinomo_core::network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
                frame_options,
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

pub(crate) fn send_daemon_rpc_to_endpoint_with_timeouts_on_runtime(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(
        runtime,
        send_daemon_rpc_to_endpoint_async(endpoint, request, timeouts, &runtime_for_future),
    )
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
    let mut host = DaemonSignedEndpointRpcAsyncHost::new(runtime, observe_connectivity);
    let options = sinomo_core::network::ClusterObjectRpcOptions::new(timeouts);
    let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
    client
        .call_endpoint(endpoint, request, options)
        .await
        .map_err(DaemonError::from)
}

async fn send_daemon_rpc_to_target_async_with_connectivity_observation(
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let paths = cluster_paths_from_env().ok_or_else(|| DaemonError::Network {
        address: target_node_id.to_string(),
        detail: "cluster target RPC requires a local Sinomo data directory".to_string(),
    })?;
    let state = sinomo_core::cluster::read_cluster_state(&paths).map_err(DaemonError::from)?;
    let mut host = DaemonSignedEndpointRpcAsyncHost::new(runtime, observe_connectivity)
        .with_cluster_paths(&paths);
    let mut client =
        sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, runtime, &paths);
    client
        .call_target(
            &state,
            target_node_id,
            request,
            sinomo_core::network::ClusterObjectRpcOptions::new(timeouts),
        )
        .await
        .map_err(DaemonError::from)
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

pub(crate) struct DaemonSignedEndpointRpcAsyncHost {
    runtime: tokio::runtime::Handle,
    observe_connectivity: bool,
    paths: Option<DaemonPaths>,
    use_tcp_tunnel: bool,
}

impl DaemonSignedEndpointRpcAsyncHost {
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
}

fn send_daemon_rpc_body<Req, Resp>(
    address: &str,
    route: &str,
    category: &str,
    body: &Req,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    let request = ObjectRpcRequest::encode_body(route, category, body)?;
    send_daemon_rpc_request_with_decoder(address, &request, timeout, |response| {
        response.decode_body()
    })
}

fn send_daemon_rpc_body_to_endpoint<Req, Resp>(
    endpoint: &ClusterEndpointRecord,
    route: &str,
    category: &str,
    body: &Req,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    let request = ObjectRpcRequest::encode_body(route, category, body)?;
    send_daemon_rpc_request_to_endpoint_with_decoder(endpoint, &request, timeout, |response| {
        response.decode_body()
    })
}

fn send_daemon_rpc_body_to_endpoint_on_runtime<Req, Resp>(
    endpoint: &ClusterEndpointRecord,
    route: &str,
    category: &str,
    body: &Req,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned + Send,
{
    let request = ObjectRpcRequest::encode_body(route, category, body)?;
    send_daemon_rpc_request_to_endpoint_with_decoder_on_runtime(
        endpoint,
        &request,
        timeout,
        runtime,
        |response| response.decode_body(),
    )
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

fn send_daemon_rpc_request_with_decoder_and_timeouts<Resp, DecodeError>(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    decode: impl FnOnce(&sinomo_core::network::ObjectRpcResponse) -> Result<Resp, DecodeError>,
) -> Result<Resp, DaemonError>
where
    DecodeError: Into<DaemonError>,
{
    let response = send_daemon_rpc_with_timeouts(address, request, timeouts)?;
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

fn send_daemon_rpc_request_to_endpoint_with_decoder_and_timeouts<Resp, DecodeError>(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    decode: impl FnOnce(&sinomo_core::network::ObjectRpcResponse) -> Result<Resp, DecodeError>,
) -> Result<Resp, DaemonError>
where
    DecodeError: Into<DaemonError>,
{
    let response = send_daemon_rpc_to_endpoint_with_timeouts(endpoint, request, timeouts)?;
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

impl sinomo_core::network::ClusterSignedEndpointRpcAsyncHost for DaemonSignedEndpointRpcAsyncHost {
    fn sign_endpoint_rpc_request_async<'a>(
        &'a mut self,
        request: &'a ObjectRpcRequest,
    ) -> sinomo_core::network::ClusterSignedEndpointRpcFuture<'a, Vec<u8>> {
        let route = request.route.clone();
        let category = request.category.clone();
        let body_sha256 = daemon_rpc_body_sha256(request);
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    sign_daemon_rpc_request_digest_for_local_node(&route, &category, body_sha256)
                })
                .await
                .map_err(|source| sinomo_core::network::NetworkError::Io {
                    operation: "cluster endpoint signing".to_string(),
                    detail: format!("endpoint signing task failed: {source}"),
                })?
                .map_err(|error| daemon_client_network_error("cluster endpoint signing", error))
        })
    }

    fn observe_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        observation: sinomo_core::network::ClusterSignedEndpointRpcObservation,
    ) -> sinomo_core::network::ClusterSignedEndpointRpcFuture<'a, ()> {
        if !self.observe_connectivity {
            return Box::pin(async { Ok(()) });
        }
        let runtime = self.runtime.clone();
        let paths = self
            .paths
            .as_ref()
            .and_then(|paths| crate::cluster::cluster_paths_from_daemon_paths(paths).ok());
        Box::pin(async move {
            let (connectivity_state, latency_ms, bypass_failure_grace) =
                daemon_rpc_connectivity_observation(observation);
            remember_endpoint_command_connectivity_async(
                runtime,
                paths,
                endpoint.clone(),
                connectivity_state,
                latency_ms,
                bypass_failure_grace,
            )
            .await;
            Ok(())
        })
    }

    fn send_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        auth: &'a [u8],
        request: &'a ObjectRpcRequest,
        timeouts: sinomo_core::network::DaemonRpcTimeouts,
    ) -> sinomo_core::network::ClusterSignedEndpointRpcFuture<
        'a,
        sinomo_core::network::ObjectRpcResponse,
    > {
        let runtime = self.runtime.clone();
        Box::pin(async move {
            if self.use_tcp_tunnel
                && let Some(result) = crate::tcp_tunnel::send_signed_endpoint_rpc_via_tcp_tunnel(
                    endpoint,
                    auth,
                    request,
                    timeouts,
                    &runtime,
                    self.paths.as_ref(),
                )
                .await
            {
                return result;
            }
            sinomo_core::network::send_daemon_rpc_to_cluster_endpoint_async(
                endpoint,
                Some(auth),
                request,
                timeouts,
            )
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

fn daemon_rpc_connectivity_observation(
    observation: sinomo_core::network::ClusterSignedEndpointRpcObservation,
) -> (
    sinomo_core::network::ClusterConnectivityState,
    Option<u64>,
    bool,
) {
    if observation.is_success() {
        (
            sinomo_core::network::ClusterConnectivityState::Healthy,
            observation.latency_ms(),
            false,
        )
    } else if observation.is_io_failure() {
        (
            sinomo_core::network::ClusterConnectivityState::Degraded,
            None,
            false,
        )
    } else if observation.is_auth_denied() {
        (
            sinomo_core::network::ClusterConnectivityState::Degraded,
            None,
            true,
        )
    } else {
        (
            sinomo_core::network::ClusterConnectivityState::Degraded,
            None,
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
) {
    let Some(paths) = paths.or_else(cluster_paths_from_env) else {
        return;
    };
    let _ = runtime
        .spawn_blocking(move || {
            let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&paths) else {
                return;
            };
            if bypass_failure_grace {
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
    send_daemon_rpc_body_to_endpoint(
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
    send_daemon_rpc_body_to_endpoint_on_runtime(
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
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(connect_timeout),
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
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(connect_timeout),
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
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return send_object_rpc_to_target_on_runtime(
            target_node_id,
            request,
            connect_timeout,
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
    let result = send_object_rpc_to_target_on_runtime(
        target_node_id,
        request,
        connect_timeout,
        runtime.handle(),
        observe_connectivity,
    );
    runtime.shutdown_timeout(Duration::from_millis(250));
    result
}

pub(crate) fn send_object_rpc_to_target_on_runtime(
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    connect_timeout: Duration,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let target_node_id = target_node_id.clone();
    let request = request.clone();
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(runtime, async move {
        send_object_rpc_to_target_async(
            &target_node_id,
            &request,
            connect_timeout,
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
    send_daemon_rpc_to_target_async_with_connectivity_observation(
        target_node_id,
        request,
        object_rpc_transfer_timeouts(connect_timeout),
        runtime,
        observe_connectivity,
    )
    .await
}

pub(crate) fn object_rpc_transfer_timeouts(
    connect_timeout: Duration,
) -> sinomo_core::network::DaemonRpcTimeouts {
    sinomo_core::network::DaemonRpcTimeouts::connect_only(connect_timeout)
        .with_idle_timeout(Some(OBJECT_RPC_TRANSFER_IDLE_TIMEOUT))
}

pub fn fetch_daemon_status_from_address(
    address: &str,
    timeout: Duration,
) -> Result<DaemonStatus, DaemonError> {
    send_daemon_rpc_body(
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
    send_daemon_rpc_body(
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
    send_daemon_rpc_body_to_endpoint(
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
    send_daemon_rpc_body(
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

fn decode_cluster_kv_watch_events_rpc_response(
    response: &sinomo_core::network::ObjectRpcResponse,
) -> Result<ClusterKvWatchEventsResponse, DaemonError> {
    response.error_detail()?;
    sinomo_core::kv::decode_cluster_kv_watch_events_rpc_response_body(&response.body)
        .map_err(Into::into)
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

pub(crate) fn fetch_cluster_kv_snapshot_from_endpoint(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
) -> Result<KvSnapshot, DaemonError> {
    let rpc_request =
        object_rpc_request_from_kv_payload(sinomo_core::kv::cluster_kv_snapshot_rpc_payload()?);
    send_daemon_rpc_request_to_endpoint_with_decoder(
        endpoint,
        &rpc_request,
        timeout,
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

pub(crate) async fn send_cluster_raft_append_async(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRaftAppendRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterRaftAppendResponse, DaemonError> {
    let rpc_request = object_rpc_request_from_kv_payload(
        sinomo_core::kv::cluster_raft_append_rpc_payload(request)?,
    );
    send_daemon_rpc_request_to_endpoint_with_decoder_async(
        endpoint,
        &rpc_request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
        runtime,
        decode_cluster_raft_append_rpc_response,
    )
    .await
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

pub(crate) async fn send_cluster_raft_vote_async(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRaftVoteRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterRaftVoteResponse, DaemonError> {
    let rpc_request = object_rpc_request_from_kv_payload(
        sinomo_core::kv::cluster_raft_vote_rpc_payload(request)?,
    );
    send_daemon_rpc_request_to_endpoint_with_decoder_async(
        endpoint,
        &rpc_request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
        runtime,
        decode_cluster_raft_vote_rpc_response,
    )
    .await
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

pub(crate) async fn send_cluster_kv_watch_events_async(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterKvWatchEventsRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterKvWatchEventsResponse, DaemonError> {
    if let Some(response) = sinomo_core::kv::cluster_kv_watch_events_noop_response(request) {
        return Ok(response);
    }
    let rpc_request = object_rpc_request_from_kv_payload(
        sinomo_core::kv::cluster_kv_watch_events_rpc_payload(request)?,
    );
    send_daemon_rpc_request_to_endpoint_with_decoder_async(
        endpoint,
        &rpc_request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
        runtime,
        decode_cluster_kv_watch_events_rpc_response,
    )
    .await
}

pub fn send_daemon_rollout_binary(
    address: &str,
    request: &ClusterRolloutBinaryRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rpc_body(
        address,
        ROLLOUT_BINARY_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_binary_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBinaryRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rpc_body_to_endpoint(
        endpoint,
        ROLLOUT_BINARY_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_binary_upload_chunk(
    address: &str,
    request: &ClusterRolloutBinaryUploadChunkRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryUploadChunkResponse, DaemonError> {
    let request = ObjectRpcRequest::encode_body(
        ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
    )?
    .with_response_retry_safe();
    send_daemon_rpc_request_with_decoder(address, &request, timeout, |response| {
        response.decode_body()
    })
}

pub fn send_daemon_rollout_binary_upload_chunk_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBinaryUploadChunkRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryUploadChunkResponse, DaemonError> {
    let request = ObjectRpcRequest::encode_body(
        ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
    )?
    .with_response_retry_safe();
    send_daemon_rpc_request_to_endpoint_with_decoder(endpoint, &request, timeout, |response| {
        response.decode_body()
    })
}

pub fn send_daemon_rollout_binary_upload_finalize(
    address: &str,
    request: &ClusterRolloutBinaryUploadFinalizeRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rpc_body(
        address,
        ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_binary_upload_finalize_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBinaryUploadFinalizeRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rpc_body_to_endpoint(
        endpoint,
        ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_native_dylib_module(
    address: &str,
    request: &ClusterRolloutNativeDylibModuleRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rpc_body(
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
    send_daemon_rpc_body_to_endpoint(
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
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rollout_build_with_timeouts(
        address,
        request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
    )
}

pub fn send_daemon_rollout_build_with_timeouts(
    address: &str,
    request: &ClusterRolloutBuildRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let request =
        ObjectRpcRequest::encode_body(ROLLOUT_BUILD_ROUTE, ROLLOUT_RPC_CATEGORY, request)?;
    send_daemon_rpc_request_with_decoder_and_timeouts(address, &request, timeouts, |response| {
        response.decode_body()
    })
}

pub fn send_daemon_rollout_build_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBuildRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rollout_build_to_endpoint_with_timeouts(
        endpoint,
        request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
    )
}

pub fn send_daemon_rollout_build_to_endpoint_with_timeouts(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBuildRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let request =
        ObjectRpcRequest::encode_body(ROLLOUT_BUILD_ROUTE, ROLLOUT_RPC_CATEGORY, request)?;
    send_daemon_rpc_request_to_endpoint_with_decoder_and_timeouts(
        endpoint,
        &request,
        timeouts,
        |response| response.decode_body(),
    )
}

pub fn send_daemon_rollout_clear_status(
    address: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    send_daemon_rpc_body(
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
    send_daemon_rpc_body_to_endpoint(
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
    use sinomo_core::network::ObjectRpcRequest;
    use std::time::Duration;

    #[test]
    fn object_rpc_transfer_timeouts_keep_transfer_idle_separate_from_connect_budget() {
        let timeouts = super::object_rpc_transfer_timeouts(Duration::from_secs(60));

        assert_eq!(timeouts.connect_timeout, Some(Duration::from_secs(60)));
        assert_eq!(timeouts.idle_timeout, Some(Duration::from_secs(300)));
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
            super::send_daemon_rpc_raw_with_timeouts(
                "127.0.0.1:21353",
                None,
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

        let direct = super::DaemonSignedEndpointRpcAsyncHost::direct_only(runtime.handle());
        assert!(!direct.observe_connectivity);
        assert!(!direct.use_tcp_tunnel);

        let routed = super::DaemonSignedEndpointRpcAsyncHost::new(runtime.handle(), true);
        assert!(routed.observe_connectivity);
        assert!(routed.use_tcp_tunnel);
    }

    #[test]
    fn daemon_rpc_connectivity_observation_classifies_endpoint_failures() {
        let healthy = super::daemon_rpc_connectivity_observation(
            sinomo_core::network::ClusterSignedEndpointRpcObservation::success(Some(5)),
        );
        assert_eq!(
            healthy.0,
            sinomo_core::network::ClusterConnectivityState::Healthy
        );
        assert_eq!(healthy.1, Some(5));

        let degraded = super::daemon_rpc_connectivity_observation(
            sinomo_core::network::ClusterSignedEndpointRpcObservation::io_failure(),
        );
        assert_eq!(
            degraded,
            (
                sinomo_core::network::ClusterConnectivityState::Degraded,
                None,
                false
            )
        );

        let auth_denied = super::daemon_rpc_connectivity_observation(
            sinomo_core::network::ClusterSignedEndpointRpcObservation::auth_denied(),
        );
        assert_eq!(
            auth_denied,
            (
                sinomo_core::network::ClusterConnectivityState::Degraded,
                None,
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
