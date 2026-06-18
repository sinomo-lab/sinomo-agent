use crate::{DaemonError, DaemonStatus, sign_daemon_rpc_request_for_local_node};
use sinomo_config::{
    CONFIG_CATALOG_ROUTE, CONFIG_GET_ROUTE, CONFIG_RPC_CATEGORY, CONFIG_SET_ROUTE,
    ConfigCatalogRequest, ConfigCatalogResponse, ConfigClient, ConfigDefinition, ConfigError,
    ConfigGetRequest, ConfigGetResponse, ConfigResolution, ConfigSetRequest, ConfigSetResponse,
    ConfigWriteResult, ConfigWriteTarget,
};
use sinomo_core::ClusterEndpointRecord;
use sinomo_dfs::{
    DfsDirectReadRequest, DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse,
    DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse, DfsReadResponse,
};
use sinomo_kv::{
    ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse, ClusterRaftAppendRequest,
    ClusterRaftAppendResponse, ClusterRaftVoteRequest, ClusterRaftVoteResponse, KvSnapshot, KvTxn,
    KvTxnResponse,
};
use sinomo_network::{
    AsyncDaemonControlStream, ClusterMemberPublishRequest, ClusterMemberPublishResponse,
    ObjectRpcRequest,
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
use std::time::{Duration, Instant};

fn object_rpc_request_from_kv_payload(payload: sinomo_kv::ClusterKvRpcPayload) -> ObjectRpcRequest {
    ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body)
}

pub fn send_daemon_rpc(
    address: &str,
    request: &ObjectRpcRequest,
    timeout: Duration,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let signed = sign_daemon_rpc_request_for_local_node(request)?;
    send_daemon_rpc_raw(address, Some(&signed), request, timeout)
}

pub(crate) fn send_daemon_rpc_on_runtime(
    address: &str,
    request: &ObjectRpcRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(
        runtime,
        send_daemon_rpc_async(
            address,
            request,
            sinomo_network::DaemonRpcTimeouts::connect_and_idle(timeout),
            &runtime_for_future,
        ),
    )
}

pub async fn send_daemon_rpc_async(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let request_for_signing = request.clone();
    let signed = runtime
        .spawn_blocking(move || sign_daemon_rpc_request_for_local_node(&request_for_signing))
        .await
        .map_err(|source| DaemonError::Network {
            address: address.to_string(),
            detail: format!("daemon RPC signing task failed: {source}"),
        })??;
    let response =
        sinomo_network::send_daemon_rpc_to_address_async(address, Some(&signed), request, timeouts)
            .await?;
    Ok(response)
}

pub(crate) fn send_daemon_rpc_raw(
    address: &str,
    auth: Option<&[u8]>,
    request: &ObjectRpcRequest,
    timeout: Duration,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    send_daemon_rpc_raw_with_timeouts(
        address,
        auth,
        request,
        sinomo_network::DaemonRpcTimeouts::connect_and_idle(timeout),
    )
}

/// Like [`send_daemon_rpc`], but with an idle deadline on the stream as well
/// as the connect timeout. Use for RPCs that must never hang on a silent
/// peer (e.g. cluster join, where an unresponsive introducer otherwise wedges
/// the caller's retry loop forever).
pub fn send_daemon_rpc_with_timeouts(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_network::DaemonRpcTimeouts,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let signed = sign_daemon_rpc_request_for_local_node(request)?;
    send_daemon_rpc_raw_with_timeouts(address, Some(&signed), request, timeouts)
}

pub(crate) fn send_daemon_rpc_raw_with_timeouts(
    address: &str,
    auth: Option<&[u8]>,
    request: &ObjectRpcRequest,
    timeouts: sinomo_network::DaemonRpcTimeouts,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    block_on_daemon_client(async {
        sinomo_network::send_daemon_rpc_to_address_async(address, auth, request, timeouts)
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
}

impl RemoteConfigClient {
    pub fn new(address: impl Into<String>, timeout: Duration) -> Self {
        Self {
            address: address.into(),
            timeout,
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
        let response = send_daemon_rpc(&self.address, &request, self.timeout)
            .map_err(|error| client_error(error.to_string()))?;
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
    mut on_response: impl FnMut(sinomo_network::ObjectRpcResponse) -> Result<bool, DaemonError>,
) -> Result<(), DaemonError> {
    block_on_daemon_client_stream(send_daemon_rpc_stream_async(
        address,
        request,
        sinomo_network::DaemonRpcTimeouts::connect_only(timeout),
        &mut on_response,
    ))
}

pub async fn send_daemon_rpc_stream_async(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_network::DaemonRpcTimeouts,
    mut on_response: impl FnMut(sinomo_network::ObjectRpcResponse) -> Result<bool, DaemonError>,
) -> Result<(), DaemonError> {
    let request_for_signing = request.clone();
    let signed = tokio::task::spawn_blocking(move || {
        sign_daemon_rpc_request_for_local_node(&request_for_signing)
    })
    .await
    .map_err(|source| DaemonError::Network {
        address: address.to_string(),
        detail: format!("daemon streaming RPC signing task failed: {source}"),
    })??;
    let control_request = request.clone().with_auth(signed);
    let mut stream = AsyncDaemonControlStream::connect_with_options(address, timeouts).await?;
    let frame_options =
        sinomo_network::ObjectFrameIoOptions::none().with_idle_timeout(timeouts.idle_timeout);
    stream
        .write_request(address, &control_request, frame_options)
        .await?;
    stream.shutdown_write(address).await?;
    loop {
        let rpc = stream
            .read_response(
                address,
                sinomo_network::IROH_DAEMON_COMMAND_MAX_BYTES,
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
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let timeouts = sinomo_network::DaemonRpcTimeouts::connect_and_idle(timeout);
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

pub(crate) fn send_daemon_rpc_to_endpoint_on_runtime(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(
        runtime,
        send_daemon_rpc_to_endpoint_async(
            endpoint,
            request,
            sinomo_network::DaemonRpcTimeouts::connect_and_idle(timeout),
            &runtime_for_future,
        ),
    )
}

pub async fn send_daemon_rpc_to_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeouts: sinomo_network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    send_daemon_rpc_to_endpoint_async_with_connectivity_observation(
        endpoint, request, timeouts, runtime, true,
    )
    .await
}

async fn send_daemon_rpc_to_endpoint_async_with_connectivity_observation(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeouts: sinomo_network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let mut host = DaemonSignedEndpointRpcAsyncHost::new(runtime, observe_connectivity);
    let options = sinomo_network::ClusterObjectRpcOptions::new(timeouts);
    let paths = cluster_paths_from_env();
    let route_context = paths
        .as_ref()
        .map(|paths| sinomo_network::ClusterObjectRpcRouteContext::new(runtime, paths));
    sinomo_network::call_cluster_endpoint_rpc_async(
        &mut host,
        route_context,
        endpoint,
        request,
        options,
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
    allow_local_route_policy: bool,
}

impl DaemonSignedEndpointRpcAsyncHost {
    pub(crate) fn new(runtime: &tokio::runtime::Handle, observe_connectivity: bool) -> Self {
        Self {
            runtime: runtime.clone(),
            observe_connectivity,
            allow_local_route_policy: true,
        }
    }

    pub(crate) fn direct_only(runtime: &tokio::runtime::Handle) -> Self {
        Self {
            runtime: runtime.clone(),
            observe_connectivity: false,
            allow_local_route_policy: false,
        }
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
    decode: impl FnOnce(&sinomo_network::ObjectRpcResponse) -> Result<Resp, DecodeError>,
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
    decode: impl FnOnce(&sinomo_network::ObjectRpcResponse) -> Result<Resp, DecodeError>,
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
    Decode: FnOnce(&sinomo_network::ObjectRpcResponse) -> Result<Resp, DecodeError> + Send,
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
    Decode: FnOnce(&sinomo_network::ObjectRpcResponse) -> Result<Resp, DecodeError> + Send,
    DecodeError: Into<DaemonError>,
{
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(
        runtime,
        send_daemon_rpc_request_to_endpoint_with_decoder_async_with_connectivity_observation(
            endpoint,
            request,
            sinomo_network::DaemonRpcTimeouts::connect_and_idle(timeout),
            &runtime_for_future,
            observe_connectivity,
            decode,
        ),
    )
}

async fn send_daemon_rpc_request_to_endpoint_with_decoder_async<Resp, Decode, DecodeError>(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    timeouts: sinomo_network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
    decode: Decode,
) -> Result<Resp, DaemonError>
where
    Decode: FnOnce(&sinomo_network::ObjectRpcResponse) -> Result<Resp, DecodeError>,
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
    timeouts: sinomo_network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
    decode: Decode,
) -> Result<Resp, DaemonError>
where
    Decode: FnOnce(&sinomo_network::ObjectRpcResponse) -> Result<Resp, DecodeError>,
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

impl sinomo_network::ClusterSignedEndpointRpcAsyncHost for DaemonSignedEndpointRpcAsyncHost {
    fn sign_endpoint_rpc_request_async<'a>(
        &'a mut self,
        request: &'a ObjectRpcRequest,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, Vec<u8>> {
        let request = request.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || sign_daemon_rpc_request_for_local_node(&request))
                .await
                .map_err(|source| sinomo_network::NetworkError::Io {
                    operation: "cluster endpoint signing".to_string(),
                    detail: format!("endpoint signing task failed: {source}"),
                })?
                .map_err(|error| daemon_client_network_error("cluster endpoint signing", error))
        })
    }

    fn try_gateway_signed_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ObjectRpcRequest,
        request_auth: &'a [u8],
        timeouts: sinomo_network::DaemonRpcTimeouts,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, Option<sinomo_network::ObjectRpcResponse>>
    {
        if !self.allow_local_route_policy {
            return Box::pin(async { Ok(None) });
        }
        let endpoint = endpoint.clone();
        let request = request.clone();
        let request_auth = request_auth.to_vec();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            match route_endpoint_request_via_local_daemon_async(
                &runtime,
                &endpoint,
                &request,
                &request_auth,
                timeouts,
                &DaemonRpcAttemptDeadline::from_policy(timeouts),
            )
            .await
            {
                Some(Ok(response)) => Ok(Some(response)),
                Some(Err(error)) => Err(daemon_client_network_error(
                    "cluster endpoint gateway",
                    error,
                )),
                None => Ok(None),
            }
        })
    }

    fn endpoint_rpc_cluster_id_async<'a>(
        &'a mut self,
        _endpoint: &'a ClusterEndpointRecord,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, Option<sinomo_network::ClusterId>> {
        if !self.allow_local_route_policy {
            return Box::pin(async { Ok(None) });
        }
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let Some(paths) = cluster_paths_from_env() else {
                return Ok(None);
            };
            runtime
                .spawn_blocking(move || {
                    Ok(sinomo_network::read_cluster_state(&paths)
                        .ok()
                        .map(|state| state.cluster_id))
                })
                .await
                .map_err(|source| sinomo_network::NetworkError::Io {
                    operation: "cluster endpoint relay context".to_string(),
                    detail: format!("endpoint relay context task failed: {source}"),
                })?
        })
    }

    fn send_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        auth: &'a [u8],
        request: &'a ObjectRpcRequest,
        timeouts: sinomo_network::DaemonRpcTimeouts,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, sinomo_network::ObjectRpcResponse> {
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = sinomo_network::send_daemon_rpc_to_cluster_endpoint_async(
                endpoint,
                Some(auth),
                request,
                timeouts,
            )
            .await;
            if self.observe_connectivity
                && let Some((connectivity_state, latency_ms, bypass_failure_grace)) =
                    daemon_rpc_connectivity_observation(result.as_ref(), started)
            {
                remember_endpoint_command_connectivity_from_env_async(
                    runtime,
                    endpoint.clone(),
                    connectivity_state,
                    latency_ms,
                    bypass_failure_grace,
                )
                .await;
            }
            result
        })
    }
}

fn daemon_rpc_connectivity_observation(
    result: Result<&sinomo_network::ObjectRpcResponse, &sinomo_network::NetworkError>,
    started: Instant,
) -> Option<(sinomo_network::ClusterConnectivityState, Option<u64>, bool)> {
    match result {
        Ok(_) => Some((
            sinomo_network::ClusterConnectivityState::Healthy,
            Some(daemon_rpc_elapsed_latency_ms(started)),
            false,
        )),
        Err(sinomo_network::NetworkError::Io { .. }) => Some((
            sinomo_network::ClusterConnectivityState::Degraded,
            None,
            false,
        )),
        Err(sinomo_network::NetworkError::AuthDenied) => Some((
            sinomo_network::ClusterConnectivityState::Degraded,
            None,
            true,
        )),
        Err(_) => None,
    }
}

fn daemon_rpc_timeout_from_policy(timeouts: sinomo_network::DaemonRpcTimeouts) -> Option<Duration> {
    timeouts.connect_timeout.or(timeouts.idle_timeout)
}

fn daemon_rpc_elapsed_latency_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

async fn remember_endpoint_command_connectivity_from_env_async(
    runtime: tokio::runtime::Handle,
    endpoint: ClusterEndpointRecord,
    connectivity_state: sinomo_network::ClusterConnectivityState,
    latency_ms: Option<u64>,
    bypass_failure_grace: bool,
) {
    let Some(paths) = cluster_paths_from_env() else {
        return;
    };
    let _ = runtime
        .spawn_blocking(move || {
            let Ok(state) = sinomo_network::read_cluster_state_without_kv(&paths) else {
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

pub(crate) fn cluster_paths_from_env() -> Option<sinomo_network::ClusterPaths> {
    let data_dir = sinomo_local::SinomoDataDir::from_env_or_home().ok()?;
    Some(sinomo_network::ClusterPaths::new(&data_dir))
}

fn daemon_client_network_error(
    operation: &str,
    error: DaemonError,
) -> sinomo_network::NetworkError {
    match error {
        DaemonError::AuthDenied => sinomo_network::NetworkError::AuthDenied,
        DaemonError::AuthExpired => sinomo_network::NetworkError::AuthExpired,
        DaemonError::ClusterReadOnly { detail } => {
            sinomo_network::NetworkError::ClusterReadOnly { detail }
        }
        DaemonError::Network { address, detail } => sinomo_network::NetworkError::Io {
            operation: format!("{operation} {address}"),
            detail,
        },
        other => sinomo_network::NetworkError::Io {
            operation: operation.to_string(),
            detail: other.to_string(),
        },
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DaemonRpcAttemptDeadline {
    started: Instant,
    timeout: Option<Duration>,
}

impl DaemonRpcAttemptDeadline {
    pub(crate) fn from_policy(timeouts: sinomo_network::DaemonRpcTimeouts) -> Self {
        Self {
            started: Instant::now(),
            timeout: daemon_rpc_timeout_from_policy(timeouts),
        }
    }

    #[cfg(test)]
    pub(crate) fn remaining(&self) -> Duration {
        self.timeout
            .expect("remaining is only defined for bounded daemon RPC attempts")
            .checked_sub(self.started.elapsed())
            .unwrap_or_else(|| Duration::from_millis(1))
    }

    pub(crate) fn checked_remaining(&self) -> Option<Option<Duration>> {
        match self.timeout {
            Some(timeout) => timeout
                .checked_sub(self.started.elapsed())
                .filter(|remaining| !remaining.is_zero())
                .map(Some),
            None => Some(None),
        }
    }

    pub(crate) fn checked_remaining_policy(
        &self,
        timeouts: sinomo_network::DaemonRpcTimeouts,
    ) -> Option<sinomo_network::DaemonRpcTimeouts> {
        match self.checked_remaining()? {
            Some(remaining) => Some(
                timeouts.with_connect_timeout(Some(
                    timeouts
                        .connect_timeout
                        .map(|timeout| timeout.min(remaining))
                        .unwrap_or(remaining),
                )),
            ),
            None => Some(timeouts),
        }
    }

    #[cfg(test)]
    pub(crate) fn timeout_error(&self, address: &str) -> DaemonError {
        let timeout = self
            .timeout
            .expect("timeout_error is only defined for bounded daemon RPC attempts");
        DaemonError::Network {
            address: address.to_string(),
            detail: format!("timed out after {}ms", timeout.as_millis()),
        }
    }
}

async fn route_endpoint_request_via_local_daemon_async(
    runtime: &tokio::runtime::Handle,
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    request_auth: &[u8],
    timeouts: sinomo_network::DaemonRpcTimeouts,
    deadline: &DaemonRpcAttemptDeadline,
) -> Option<Result<sinomo_network::ObjectRpcResponse, DaemonError>> {
    if request.route == sinomo_network::CLUSTER_RELAY_ROUTE {
        return None;
    }
    let status = match crate::status::default_main_daemon_status_with_timeouts_async(
        deadline.checked_remaining_policy(timeouts)?,
        runtime,
    )
    .await
    {
        Ok(Some(status)) => status,
        Ok(None) | Err(_) => return None,
    };
    if !local_daemon_can_gateway_endpoint(
        status.pid,
        status.cluster.as_ref(),
        endpoint,
        std::process::id(),
    ) {
        return None;
    }

    let cluster = status.cluster.as_ref()?;
    let outer = sinomo_network::RelayEnvelope::new(
        cluster.cluster_id.clone(),
        endpoint.node_id.clone(),
        request.clone().with_auth(request_auth.to_vec()),
    )
    .into_request()
    .map_err(DaemonError::from);
    let outer = match outer {
        Ok(outer) => outer,
        Err(error) => return Some(Err(error)),
    };
    match send_daemon_rpc_async(
        &status.address,
        &outer,
        deadline.checked_remaining_policy(timeouts)?,
        runtime,
    )
    .await
    {
        Ok(response) if sinomo_network::relay_response_is_routing_error(&response) => None,
        Ok(response) => Some(Ok(response)),
        Err(_) => None,
    }
}

fn local_daemon_can_gateway_endpoint(
    daemon_pid: u32,
    daemon_cluster: Option<&crate::DaemonClusterStatus>,
    endpoint: &ClusterEndpointRecord,
    current_pid: u32,
) -> bool {
    daemon_pid != current_pid
        && daemon_cluster.is_some_and(|cluster| {
            endpoint.node_id != cluster.node_id
                && endpoint.node_id != sinomo_node::shadow_daemon_node_id(&cluster.node_id)
        })
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
        sinomo_network::DaemonRpcTimeouts::connect_and_idle(timeout),
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
        sinomo_network::DaemonRpcTimeouts::connect_and_idle(connect_timeout),
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
        sinomo_network::DaemonRpcTimeouts::connect_and_idle(connect_timeout),
        runtime,
        |response| sinomo_dfs::decode_dfs_metadata_snapshot_install_rpc_response(response),
    )
    .await
}

pub(crate) fn send_dfs_object_rpc_to_endpoint_on_runtime(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    connect_timeout: Duration,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    let endpoint = endpoint.clone();
    let request = request.clone();
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(runtime, async move {
        send_dfs_object_rpc_to_endpoint_async(
            &endpoint,
            &request,
            connect_timeout,
            &runtime_for_future,
            observe_connectivity,
        )
        .await
    })
}

pub async fn send_dfs_object_rpc_to_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    connect_timeout: Duration,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<sinomo_network::ObjectRpcResponse, DaemonError> {
    send_daemon_rpc_to_endpoint_async_with_connectivity_observation(
        endpoint,
        request,
        sinomo_network::DaemonRpcTimeouts::connect_and_idle(connect_timeout),
        runtime,
        observe_connectivity,
    )
    .await
}

pub(crate) fn send_dfs_direct_read_to_endpoint_on_runtime(
    endpoint: &ClusterEndpointRecord,
    request: &DfsDirectReadRequest,
    connect_timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<DfsReadResponse, DaemonError> {
    let endpoint = endpoint.clone();
    let request = request.clone();
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(runtime, async move {
        send_dfs_direct_read_to_endpoint_async(
            &endpoint,
            &request,
            connect_timeout,
            &runtime_for_future,
        )
        .await
    })
}

pub fn send_dfs_direct_read_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &DfsDirectReadRequest,
    connect_timeout: Duration,
) -> Result<DfsReadResponse, DaemonError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| DaemonError::Io {
            path: "tokio runtime".to_string(),
            detail: source.to_string(),
        })?;
    send_dfs_direct_read_to_endpoint_on_runtime(
        endpoint,
        request,
        connect_timeout,
        runtime.handle(),
    )
}

pub async fn send_dfs_direct_read_to_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    request: &DfsDirectReadRequest,
    connect_timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<DfsReadResponse, DaemonError> {
    let rpc_request = sinomo_dfs::dfs_direct_read_rpc_request(request)?;
    let response = send_daemon_rpc_to_endpoint_async_with_connectivity_observation(
        endpoint,
        &rpc_request,
        sinomo_network::DaemonRpcTimeouts::connect_and_idle(connect_timeout),
        runtime,
        false,
    )
    .await?;
    sinomo_dfs::dfs_direct_read_response_from_rpc_response(response).map_err(DaemonError::from)
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
    response: &sinomo_network::ObjectRpcResponse,
) -> Result<KvSnapshot, DaemonError> {
    response.error_detail()?;
    sinomo_kv::decode_cluster_kv_snapshot_rpc_body(&response.body).map_err(Into::into)
}

fn decode_cluster_kv_txn_rpc_response(
    response: &sinomo_network::ObjectRpcResponse,
) -> Result<KvTxnResponse, DaemonError> {
    response.error_detail()?;
    sinomo_kv::decode_cluster_kv_txn_rpc_body(&response.body).map_err(Into::into)
}

fn decode_cluster_kv_watch_events_rpc_response(
    response: &sinomo_network::ObjectRpcResponse,
) -> Result<ClusterKvWatchEventsResponse, DaemonError> {
    response.error_detail()?;
    sinomo_kv::decode_cluster_kv_watch_events_rpc_response_body(&response.body).map_err(Into::into)
}

fn decode_cluster_raft_append_rpc_response(
    response: &sinomo_network::ObjectRpcResponse,
) -> Result<ClusterRaftAppendResponse, DaemonError> {
    response.error_detail()?;
    sinomo_kv::decode_cluster_raft_append_rpc_response_body(&response.body).map_err(Into::into)
}

fn decode_cluster_raft_vote_rpc_response(
    response: &sinomo_network::ObjectRpcResponse,
) -> Result<ClusterRaftVoteResponse, DaemonError> {
    response.error_detail()?;
    sinomo_kv::decode_cluster_raft_vote_rpc_response_body(&response.body).map_err(Into::into)
}

pub(crate) fn fetch_cluster_kv_snapshot_from_endpoint_on_runtime(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<KvSnapshot, DaemonError> {
    let rpc_request =
        object_rpc_request_from_kv_payload(sinomo_kv::cluster_kv_snapshot_rpc_payload()?);
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
        object_rpc_request_from_kv_payload(sinomo_kv::cluster_kv_snapshot_rpc_payload()?);
    send_daemon_rpc_request_to_endpoint_with_decoder(
        endpoint,
        &rpc_request,
        timeout,
        decode_cluster_kv_snapshot_rpc_response,
    )
}

pub(crate) async fn fetch_cluster_kv_snapshot_from_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<KvSnapshot, DaemonError> {
    let rpc_request =
        object_rpc_request_from_kv_payload(sinomo_kv::cluster_kv_snapshot_rpc_payload()?);
    send_daemon_rpc_request_to_endpoint_with_decoder_async(
        endpoint,
        &rpc_request,
        sinomo_network::DaemonRpcTimeouts::connect_and_idle(timeout),
        runtime,
        decode_cluster_kv_snapshot_rpc_response,
    )
    .await
}

pub(crate) fn send_cluster_kv_txn_on_runtime(
    endpoint: &ClusterEndpointRecord,
    txn: &KvTxn,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<KvTxnResponse, DaemonError> {
    let rpc_request =
        object_rpc_request_from_kv_payload(sinomo_kv::cluster_kv_txn_rpc_payload(txn)?);
    send_daemon_rpc_request_to_endpoint_with_decoder_on_runtime(
        endpoint,
        &rpc_request,
        timeout,
        runtime,
        decode_cluster_kv_txn_rpc_response,
    )
}

pub(crate) async fn send_cluster_kv_txn_async(
    endpoint: &ClusterEndpointRecord,
    txn: &KvTxn,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<KvTxnResponse, DaemonError> {
    let rpc_request =
        object_rpc_request_from_kv_payload(sinomo_kv::cluster_kv_txn_rpc_payload(txn)?);
    send_daemon_rpc_request_to_endpoint_with_decoder_async(
        endpoint,
        &rpc_request,
        sinomo_network::DaemonRpcTimeouts::connect_and_idle(timeout),
        runtime,
        decode_cluster_kv_txn_rpc_response,
    )
    .await
}

pub(crate) fn send_cluster_raft_append_on_runtime(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRaftAppendRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterRaftAppendResponse, DaemonError> {
    let rpc_request =
        object_rpc_request_from_kv_payload(sinomo_kv::cluster_raft_append_rpc_payload(request)?);
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
    let rpc_request =
        object_rpc_request_from_kv_payload(sinomo_kv::cluster_raft_append_rpc_payload(request)?);
    send_daemon_rpc_request_to_endpoint_with_decoder_async(
        endpoint,
        &rpc_request,
        sinomo_network::DaemonRpcTimeouts::connect_and_idle(timeout),
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
    let rpc_request =
        object_rpc_request_from_kv_payload(sinomo_kv::cluster_raft_vote_rpc_payload(request)?);
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
    let rpc_request =
        object_rpc_request_from_kv_payload(sinomo_kv::cluster_raft_vote_rpc_payload(request)?);
    send_daemon_rpc_request_to_endpoint_with_decoder_async(
        endpoint,
        &rpc_request,
        sinomo_network::DaemonRpcTimeouts::connect_and_idle(timeout),
        runtime,
        decode_cluster_raft_vote_rpc_response,
    )
    .await
}

pub(crate) fn send_cluster_member_publish_on_runtime(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterMemberPublishRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterMemberPublishResponse, DaemonError> {
    send_daemon_rpc_body_to_endpoint_on_runtime(
        endpoint,
        sinomo_network::CLUSTER_MEMBER_PUBLISH_ROUTE,
        sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        request,
        timeout,
        runtime,
    )
}

pub(crate) async fn send_cluster_member_publish_async(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterMemberPublishRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterMemberPublishResponse, DaemonError> {
    let rpc_request = ObjectRpcRequest::encode_body(
        sinomo_network::CLUSTER_MEMBER_PUBLISH_ROUTE,
        sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        request,
    )?;
    send_daemon_rpc_request_to_endpoint_with_decoder_async(
        endpoint,
        &rpc_request,
        sinomo_network::DaemonRpcTimeouts::connect_and_idle(timeout),
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
    if let Some(response) = sinomo_kv::cluster_kv_watch_events_noop_response(request) {
        return Ok(response);
    }
    let rpc_request = object_rpc_request_from_kv_payload(
        sinomo_kv::cluster_kv_watch_events_rpc_payload(request)?,
    );
    send_daemon_rpc_request_to_endpoint_with_decoder_async(
        endpoint,
        &rpc_request,
        sinomo_network::DaemonRpcTimeouts::connect_and_idle(timeout),
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
    send_daemon_rpc_body(
        address,
        ROLLOUT_BUILD_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_build_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRolloutBuildRequest,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rpc_body_to_endpoint(
        endpoint,
        ROLLOUT_BUILD_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
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

#[cfg(test)]
mod tests {
    use super::{DaemonRpcAttemptDeadline, local_daemon_can_gateway_endpoint};
    use crate::DaemonClusterStatus;
    use sinomo_core::{
        ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource, ClusterMemberRole,
        ClusterMemberState, ClusterTransportKind,
    };
    use sinomo_network::{ClusterId, DaemonRpcTimeouts, ObjectRpcRequest, ObjectRpcResponse};
    use sinomo_node::NodeId;
    use sinomo_rollout::ROLLOUT_BUILD_ROUTE;
    use std::time::{Duration, Instant};

    fn node_id(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn endpoint(node_id: NodeId) -> ClusterEndpointRecord {
        ClusterEndpointRecord {
            endpoint_id: sinomo_core::Id::generate(),
            node_id,
            transport: ClusterTransportKind::LanTcp,
            address: "10.0.0.2:21353".to_string(),
            source: ClusterEndpointSource::KvMembership,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 0,
            last_observed_latency_ms: None,
        }
    }

    fn cluster_status(node_id: NodeId) -> DaemonClusterStatus {
        DaemonClusterStatus {
            cluster_id: ClusterId::from_legacy_named("cluster_gateway_test").expect("cluster id"),
            node_id,
            process: "daemon-main".to_string(),
            member_role: ClusterMemberRole::Voter,
            member_state: ClusterMemberState::Voter,
        }
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
                sinomo_network::DaemonRpcTimeouts::connect_and_idle(Duration::from_millis(1)),
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
    fn local_daemon_gateway_only_handles_remote_endpoints_from_client_processes() {
        let local = node_id("node_gateway_local");
        let local_shadow = sinomo_node::shadow_daemon_node_id(&local);
        let remote = node_id("node_gateway_remote");
        let cluster = cluster_status(local.clone());
        assert!(local_daemon_can_gateway_endpoint(
            42,
            Some(&cluster),
            &endpoint(remote),
            7
        ));
        assert!(!local_daemon_can_gateway_endpoint(
            42,
            Some(&cluster),
            &endpoint(local),
            7
        ));
        assert!(!local_daemon_can_gateway_endpoint(
            42,
            Some(&cluster),
            &endpoint(local_shadow),
            7
        ));
        assert!(!local_daemon_can_gateway_endpoint(
            42,
            Some(&cluster),
            &endpoint(node_id("node_gateway_other")),
            42
        ));
        assert!(!local_daemon_can_gateway_endpoint(
            42,
            None,
            &endpoint(node_id("node_gateway_other")),
            7
        ));
    }

    #[test]
    fn relay_routing_errors_are_gateway_fallbacks() {
        let relay_error =
            ObjectRpcResponse::encode_error(sinomo_network::CLUSTER_RELAY_ROUTE, "no route")
                .expect("relay error");
        assert!(sinomo_network::relay_response_is_routing_error(
            &relay_error
        ));

        let target_error = ObjectRpcResponse::encode_error(ROLLOUT_BUILD_ROUTE, "build failed")
            .expect("target error");
        assert!(!sinomo_network::relay_response_is_routing_error(
            &target_error
        ));
    }

    #[test]
    fn daemon_rpc_attempt_deadline_shrinks_fallback_budget() {
        let deadline = DaemonRpcAttemptDeadline {
            started: Instant::now() - Duration::from_millis(40),
            timeout: Some(Duration::from_millis(100)),
        };

        let remaining = deadline.remaining();

        assert!(
            remaining < Duration::from_millis(100),
            "fallback attempts must not receive the original timeout"
        );
        assert!(
            remaining <= Duration::from_millis(80),
            "expected elapsed time to reduce the remaining budget, got {remaining:?}"
        );
    }

    #[test]
    fn daemon_rpc_attempt_deadline_bounds_elapsed_budget_to_short_tail() {
        let deadline = DaemonRpcAttemptDeadline {
            started: Instant::now() - Duration::from_millis(100),
            timeout: Some(Duration::from_millis(10)),
        };

        assert_eq!(deadline.remaining(), Duration::from_millis(1));
        assert_eq!(deadline.checked_remaining(), None);
        let error = deadline.timeout_error("iroh://example");
        match error {
            crate::DaemonError::Network { address, detail } => {
                assert_eq!(address, "iroh://example");
                assert_eq!(detail, "timed out after 10ms");
            }
            error => panic!("unexpected error: {error}"),
        }
    }

    #[test]
    fn daemon_rpc_attempt_deadline_preserves_unbounded_policy() {
        let deadline = DaemonRpcAttemptDeadline::from_policy(DaemonRpcTimeouts::none());

        assert_eq!(deadline.checked_remaining(), Some(None));
        assert_eq!(
            deadline.checked_remaining_policy(DaemonRpcTimeouts::none()),
            Some(DaemonRpcTimeouts::none())
        );
    }

    #[test]
    fn daemon_rpc_connectivity_observation_classifies_endpoint_failures() {
        let response =
            ObjectRpcResponse::encode_body(crate::DAEMON_STATUS_ROUTE, &()).expect("response");
        let started = Instant::now() - Duration::from_millis(5);

        let healthy = super::daemon_rpc_connectivity_observation(Ok(&response), started)
            .expect("healthy observation");
        assert_eq!(healthy.0, sinomo_network::ClusterConnectivityState::Healthy);
        assert!(healthy.1.is_some());

        let io_error = sinomo_network::NetworkError::Io {
            operation: "read".to_string(),
            detail: "timed out".to_string(),
        };
        let degraded = super::daemon_rpc_connectivity_observation(Err(&io_error), Instant::now())
            .expect("transport failure observation");
        assert_eq!(
            degraded,
            (
                sinomo_network::ClusterConnectivityState::Degraded,
                None,
                false
            )
        );

        let auth_denied = super::daemon_rpc_connectivity_observation(
            Err(&sinomo_network::NetworkError::AuthDenied),
            Instant::now(),
        )
        .expect("auth denied connectivity observation");
        assert_eq!(
            auth_denied,
            (
                sinomo_network::ClusterConnectivityState::Degraded,
                None,
                true
            )
        );
        assert_eq!(
            super::daemon_rpc_connectivity_observation(
                Err(&sinomo_network::NetworkError::ClusterReadOnly {
                    detail: "quorum unavailable".to_string(),
                }),
                Instant::now()
            ),
            None
        );
    }

    #[test]
    fn daemon_client_network_error_preserves_auth_classification() {
        assert!(matches!(
            super::daemon_client_network_error("cluster endpoint", crate::DaemonError::AuthDenied),
            sinomo_network::NetworkError::AuthDenied
        ));
        assert!(matches!(
            super::daemon_client_network_error(
                "cluster endpoint",
                crate::DaemonError::ClusterReadOnly {
                    detail: "quorum unavailable".to_string(),
                },
            ),
            sinomo_network::NetworkError::ClusterReadOnly { .. }
        ));
    }
}
