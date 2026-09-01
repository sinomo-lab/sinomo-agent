use crate::{DaemonError, DaemonPaths, DaemonStatus};
use sinomo_build::{
    BUILD_CAPABILITIES_ROUTE, BUILD_RPC_CATEGORY, BUILD_STATUS_ROUTE, BuildArtifactRequest,
    BuildArtifactResponse, BuildCapabilitiesRequest, BuildCapabilitiesResponse, BuildStatusRequest,
    BuildStatusResponse, BuildSubmitRequest, BuildSubmitResponse,
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
    ClusterKvAuthoritativeRangePrefixRequest, ClusterKvAuthoritativeWatchStartRequest,
    ClusterKvWatchCancelRequest, ClusterKvWatchCancelResponse, ClusterKvWatchDrainRequest,
    ClusterKvWatchDrainResponse, ClusterKvWatchRegistration, ClusterRaftAppendRequest,
    ClusterRaftAppendResponse, ClusterRaftVoteRequest, ClusterRaftVoteResponse,
    KvAuthoritativeRead, KvEntry, KvLocalRead, KvRequestId, KvSnapshot,
};
use sinomo_core::network::{NetworkError, ObjectRpcCall, ObjectRpcRequest, ObjectRpcResult};
use sinomo_core::node::NodeId;
use sinomo_rollout::{
    ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse, ClusterRolloutBuildRequest,
    ClusterRolloutNativeDylibModuleRequest, ROLLOUT_CLEAR_STATUS_ROUTE, ROLLOUT_DIAGNOSTICS_ROUTE,
    ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE, ROLLOUT_RPC_CATEGORY,
};
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

const DAEMON_BUILD_TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const DAEMON_BUILD_TRANSFER_OVERALL_TIMEOUT: Duration = Duration::from_secs(30 * 60);
static DAEMON_RPC_OBSERVATION_COORDINATOR: OnceLock<
    Arc<sinomo_core::cluster::ClusterCommandObservationCoordinator>,
> = OnceLock::new();

async fn materialize_daemon_client_value_response(
    result: ObjectRpcResult,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    result
        .into_materialized_value_response(usize::MAX)
        .await
        .map_err(Into::into)
}

/// Runs a compatibility value call and folds its stream-native response body
/// back into one response under the same whole-call budget. The network client
/// deliberately returns `ObjectRpcResult` as soon as ownership of a streaming
/// body is transferred; without this outer boundary, materialization could
/// outlive the caller's requested overall timeout indefinitely.
pub(crate) async fn materialize_daemon_client_value_call_with_options<F>(
    operation: &'static str,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    call: F,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError>
where
    F: std::future::Future<Output = Result<ObjectRpcResult, DaemonError>>,
{
    let call_and_materialize = async move {
        let result = call.await?;
        materialize_daemon_client_value_response(result).await
    };
    let Some(timeout) = options.overall_timeout() else {
        return call_and_materialize.await;
    };
    match tokio::time::timeout(timeout, call_and_materialize).await {
        Ok(result) => result,
        Err(_) => Err(NetworkError::timeout(
            operation,
            sinomo_core::network::NetworkTimeoutPhase::Overall,
            timeout,
            sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
        )
        .into()),
    }
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

pub async fn send_daemon_rpc_async(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let result = send_daemon_rpc_result_async(address, request, timeouts, runtime).await?;
    materialize_daemon_client_value_response(result).await
}

async fn send_daemon_rpc_result_async(
    address: &str,
    request: &ObjectRpcRequest,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcResult, DaemonError> {
    let call = ObjectRpcCall::value(request.clone());
    let options = sinomo_core::network::ClusterObjectRpcOptions::new(timeouts);
    send_daemon_rpc_call_async(address, call, options, runtime).await
}

/// Send one object-RPC call through a daemon address. Loopback
/// addresses use the local daemon directly; remote addresses retain endpoint
/// authentication and routing.
pub async fn send_daemon_rpc_call_async(
    address: &str,
    mut call: ObjectRpcCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcResult, DaemonError> {
    let data_dir = local_session_data_dir()?;
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
    let mut host = DaemonObjectRpcAsyncHost::new(runtime, false).with_cluster_paths(&cluster_paths);
    call.request.auth = None;
    if sinomo_core::network::is_loopback_connect_address(address) {
        sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host)
            .call_loopback(&data_dir, address, call, options)
            .await
            .map_err(Into::into)
    } else {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
        let endpoint = local_session_endpoint_for_address(&state, address)?;
        sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths)
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
    send_daemon_rpc_with_options(
        address,
        request,
        sinomo_core::network::ClusterObjectRpcOptions::new(timeouts),
    )
}

/// Sends a compatibility value RPC while preserving the caller's whole-call
/// deadline across routing, transport progress, and response materialization.
pub fn send_daemon_rpc_with_options(
    address: &str,
    request: &ObjectRpcRequest,
    options: sinomo_core::network::ClusterObjectRpcOptions,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    block_on_daemon_client(async {
        let runtime = tokio::runtime::Handle::current();
        let call = ObjectRpcCall::value(request.clone());
        materialize_daemon_client_value_call_with_options(
            "daemon address object RPC value call",
            options,
            send_daemon_rpc_call_async(address, call, options, &runtime),
        )
        .await
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

    fn rpc<Req, Resp>(&self, route: &str, body: Req) -> Result<Resp, ConfigError>
    where
        Req: serde::Serialize + Send + 'static,
        Resp: serde::de::DeserializeOwned + Send + 'static,
    {
        let client_error = |detail: String| ConfigError::Client { detail };
        match &self.runtime {
            Some(runtime) => send_daemon_rpc_value_on_runtime(
                &self.address,
                route,
                CONFIG_RPC_CATEGORY,
                body,
                self.timeout,
                runtime,
            ),
            None => send_daemon_rpc_value(
                &self.address,
                route,
                CONFIG_RPC_CATEGORY,
                body,
                self.timeout,
            ),
        }
        .map_err(|error| client_error(error.to_string()))
    }

    pub fn set_with_request_id(
        &self,
        key: &str,
        value: serde_json::Value,
        target: ConfigWriteTarget,
        request_id: KvRequestId,
    ) -> Result<ConfigWriteResult, ConfigError> {
        let response: ConfigSetResponse = self.rpc(
            CONFIG_SET_ROUTE,
            ConfigSetRequest {
                request_id: Some(request_id),
                key: key.to_string(),
                value,
                target,
            },
        )?;
        Ok(response.result)
    }
}

impl ConfigClient for RemoteConfigClient {
    fn catalog(&self, owner: Option<&str>) -> Result<Vec<ConfigDefinition>, ConfigError> {
        let response: ConfigCatalogResponse = self.rpc(
            CONFIG_CATALOG_ROUTE,
            ConfigCatalogRequest {
                owner: owner.map(str::to_string),
            },
        )?;
        Ok(response.definitions)
    }

    fn get(&self, key: &str) -> Result<ConfigResolution, ConfigError> {
        let response: ConfigGetResponse = self.rpc(
            CONFIG_GET_ROUTE,
            ConfigGetRequest {
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
        self.set_with_request_id(key, value, target, KvRequestId::generate())
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
        DaemonObjectRpcAsyncHost::new(&runtime, false).with_cluster_paths(&cluster_paths);
    let mut request = request.clone();
    request.auth = None;
    let call = ObjectRpcCall::value(request);
    let options = sinomo_core::network::ClusterObjectRpcOptions::new(timeouts);
    let result = if sinomo_core::network::is_loopback_connect_address(address) {
        let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
        client
            .call_loopback(&data_dir, address, call, options)
            .await?
    } else {
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
        let endpoint = local_session_endpoint_for_address(&state, address)?;
        let mut client =
            sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
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
    send_daemon_rpc_to_endpoint_with_options(
        endpoint,
        request,
        sinomo_core::network::ClusterObjectRpcOptions::new(timeouts),
    )
}

/// Sends to an authenticated endpoint while preserving the caller's
/// whole-call deadline even when transport progress keeps the idle timer live.
pub fn send_daemon_rpc_to_endpoint_with_options(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    options: sinomo_core::network::ClusterObjectRpcOptions,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            let runtime = handle.clone();
            tokio::task::block_in_place(|| {
                send_daemon_rpc_to_endpoint_with_options_on_runtime(
                    endpoint, request, options, &runtime,
                )
            })
        } else {
            Err(DaemonError::Network {
                address: "daemon client runtime".to_string(),
                detail: "daemon RPC sync client cannot run inside a current-thread Tokio runtime; use the async or owner-runtime API".to_string(),
            })
        };
    }
    let runtime = daemon_client_sync_runtime()?;
    send_daemon_rpc_to_endpoint_with_options_on_runtime(
        endpoint,
        request,
        options,
        runtime.handle(),
    )
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
    let call_started_ns = sinomo_core::network::network_timeline_capture_is_active()
        .then(sinomo_core::network::network_timeline_monotonic_ns)
        .unwrap_or(0);
    let mut request = request.clone();
    if call_started_ns != 0 && request.auth.is_none() {
        request.ensure_trace_context();
        request
            .metadata
            .entry(sinomo_core::network::OBJECT_RPC_OBSERVE_SAMPLED_METADATA_KEY.to_string())
            .or_insert_with(|| "true".to_string());
    }
    let timeline = sinomo_core::network::network_timeline_trace_context_for_request(&request);
    let endpoint_correlation_id = timeline
        .and_then(|_| sinomo_core::network::register_network_timeline_endpoint(endpoint))
        .unwrap_or(0);
    record_raw_join_bootstrap_timeline_stage(
        timeline,
        endpoint_correlation_id,
        sinomo_core::network::network_timeline_stage::CALL_PREPARE,
        sinomo_core::network::network_timeline_flag::REQUEST,
        0,
        call_started_ns,
        true,
        u32::try_from(request.body.len()).unwrap_or(u32::MAX),
    );
    let result = async {
        if request.route == sinomo_core::network::CLUSTER_JOIN_REGISTER_ROUTE {
            let data_dir = daemon_data_dir_from_paths(daemon_paths)?;
            let signed_request = request.clone();
            let sign_started_ns = timeline
                .map(|_| sinomo_core::network::network_timeline_monotonic_ns())
                .unwrap_or(0);
            let sign_result = runtime
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
                });
            record_raw_join_bootstrap_timeline_stage(
                timeline,
                endpoint_correlation_id,
                sinomo_core::network::network_timeline_stage::REQUEST_SIGN,
                sinomo_core::network::network_timeline_flag::REQUEST,
                0,
                sign_started_ns,
                sign_result.as_ref().is_ok_and(|result| result.is_ok()),
                0,
            );
            request.auth = Some(sign_result??);
        } else {
            request.auth = None;
        }
        let mut host = DaemonObjectRpcAsyncHost::new(runtime, false).with_paths(daemon_paths);
        let open_started_ns = timeline
            .map(|_| sinomo_core::network::network_timeline_monotonic_ns())
            .unwrap_or(0);
        let open_result =
            sinomo_core::network::ClusterObjectRpcAsyncHost::open_object_rpc_stream_async(
                &mut host,
                endpoint,
                sinomo_core::network::NetworkHealthPolicy::new(timeout, timeout),
            )
            .await;
        record_raw_join_bootstrap_timeline_stage(
            timeline,
            endpoint_correlation_id,
            sinomo_core::network::network_timeline_stage::ENDPOINT_OPEN,
            sinomo_core::network::network_timeline_flag::REQUEST,
            0,
            open_started_ns,
            open_result.is_ok(),
            0,
        );
        let mut stream = open_result?;
        let io = sinomo_core::network::ObjectFrameIoOptions::idle_timeout(timeout);
        let write_started_ns = timeline
            .map(|_| sinomo_core::network::network_timeline_monotonic_ns())
            .unwrap_or(0);
        let write_result =
            sinomo_core::network::write_object_frame_async(&mut stream, &request, io).await;
        record_raw_join_bootstrap_timeline_stage(
            timeline,
            endpoint_correlation_id,
            sinomo_core::network::network_timeline_stage::FRAME_SOCKET_WRITE,
            sinomo_core::network::network_timeline_flag::REQUEST,
            1,
            write_started_ns,
            write_result.is_ok(),
            u32::try_from(request.body.len()).unwrap_or(u32::MAX),
        );
        write_result?;
        let read_started_ns = timeline
            .map(|_| sinomo_core::network::network_timeline_monotonic_ns())
            .unwrap_or(0);
        let response_result: Result<sinomo_core::network::ObjectRpcResponse, _> =
            sinomo_core::network::read_object_frame_async(&mut stream, io).await;
        record_raw_join_bootstrap_timeline_stage(
            timeline,
            endpoint_correlation_id,
            sinomo_core::network::network_timeline_stage::FRAME_READ,
            sinomo_core::network::network_timeline_flag::RESPONSE,
            2,
            read_started_ns,
            response_result.is_ok(),
            response_result
                .as_ref()
                .map(|response| u32::try_from(response.body.len()).unwrap_or(u32::MAX))
                .unwrap_or(0),
        );
        let response = response_result?;
        response.error_detail()?;
        Ok(response)
    }
    .await;
    record_raw_join_bootstrap_timeline_stage(
        timeline,
        endpoint_correlation_id,
        sinomo_core::network::network_timeline_stage::CALL_COMPLETE,
        sinomo_core::network::network_timeline_flag::REQUEST
            | sinomo_core::network::network_timeline_flag::RESPONSE,
        0,
        call_started_ns,
        result.is_ok(),
        0,
    );
    result
}

#[allow(clippy::too_many_arguments)]
fn record_raw_join_bootstrap_timeline_stage(
    timeline: Option<sinomo_core::network::NetworkTimelineTraceContext>,
    endpoint_correlation_id: u64,
    stage: u16,
    flags: u16,
    frame_id: u64,
    started_ns: u64,
    success: bool,
    bytes: u32,
) {
    let Some(timeline) = timeline else {
        return;
    };
    let duration_ns =
        sinomo_core::network::network_timeline_monotonic_ns().saturating_sub(started_ns);
    sinomo_core::network::record_network_timeline_event_now(
        sinomo_core::network::NetworkTimelineEvent::new(
            timeline,
            stage,
            flags
                | if success {
                    sinomo_core::network::network_timeline_flag::SUCCESS
                } else {
                    0
                },
            0,
            0,
            frame_id,
            bytes,
            0,
            duration_ns,
        )
        .with_endpoint_correlation_id(endpoint_correlation_id),
    );
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
    materialize_daemon_client_value_call_with_options(
        "cluster endpoint object RPC value call",
        options,
        send_daemon_rpc_result_to_endpoint_with_options_and_connectivity_async(
            endpoint,
            request,
            options,
            runtime,
            observe_connectivity,
        ),
    )
    .await
}

async fn send_daemon_rpc_result_to_endpoint_with_options_and_connectivity_async(
    endpoint: &ClusterEndpointRecord,
    request: &ObjectRpcRequest,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<ObjectRpcResult, DaemonError> {
    send_daemon_rpc_call_to_endpoint_with_options_and_connectivity_async(
        endpoint,
        ObjectRpcCall::value(request.clone()),
        options,
        runtime,
        observe_connectivity,
    )
    .await
}

async fn send_daemon_rpc_call_to_endpoint_with_options_and_connectivity_async(
    endpoint: &ClusterEndpointRecord,
    call: ObjectRpcCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<ObjectRpcResult, DaemonError> {
    let paths = cluster_paths_from_env().ok_or_else(|| DaemonError::Network {
        address: endpoint.address.clone(),
        detail: "routed endpoint RPC requires a local cluster state".to_string(),
    })?;
    let state = sinomo_core::cluster::read_cluster_state_cached(&paths)?;
    let mut host =
        DaemonObjectRpcAsyncHost::new(runtime, observe_connectivity).with_cluster_paths(&paths);
    let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &paths);
    client
        .call_endpoint(&state, endpoint, call, options)
        .await
        .map_err(Into::into)
}

pub async fn send_object_rpc_call_to_target_async_with_options(
    target_node_id: &NodeId,
    call: ObjectRpcCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<ObjectRpcResult, DaemonError> {
    let paths = cluster_paths_from_env().ok_or_else(|| DaemonError::Network {
        address: target_node_id.to_string(),
        detail: "cluster target RPC requires a local Sinomo data directory".to_string(),
    })?;
    send_object_rpc_call_to_target_async_with_cluster_paths_and_options(
        &paths,
        target_node_id,
        call,
        options,
        runtime,
        observe_connectivity,
    )
    .await
}

/// Routes one object-RPC call to a cluster node using an explicit local
/// cluster-state root for route discovery and request signing.
pub async fn send_object_rpc_call_to_target_async_with_cluster_paths_and_options(
    paths: &sinomo_core::network::ClusterPaths,
    target_node_id: &NodeId,
    call: ObjectRpcCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<ObjectRpcResult, DaemonError> {
    let state =
        sinomo_core::cluster::read_cluster_state_cached(paths).map_err(DaemonError::from)?;
    let mut host =
        DaemonObjectRpcAsyncHost::new(runtime, observe_connectivity).with_cluster_paths(paths);
    let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, paths);
    client
        .call_target(&state, target_node_id, call, options)
        .await
        .map_err(Into::into)
}

pub(crate) async fn send_object_rpc_call_to_target_async_with_daemon_paths_and_network_error(
    daemon_paths: &DaemonPaths,
    target_node_id: &NodeId,
    call: ObjectRpcCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<ObjectRpcResult, sinomo_core::network::NetworkError> {
    let data_dir = daemon_data_dir_from_paths(daemon_paths)?;
    let paths = sinomo_core::network::ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_cached_shared(&paths)?;
    // Same-node process-affine component calls still cross the main-daemon
    // service boundary, but the routed client intentionally rejects local
    // targets. Resolve that process explicitly; remote targets continue
    // through the routed carrier.
    if target_node_id == &state.local_node_id {
        let status = crate::read_daemon_status(daemon_paths).map_err(|source| {
            sinomo_core::network::NetworkError::delivery_failed(
                "resolve explicit local main-daemon service path",
                sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                source.to_string(),
            )
        })?;
        let cluster = status.cluster.as_ref().ok_or_else(|| {
            sinomo_core::network::NetworkError::delivery_failed(
                "resolve explicit local main-daemon service path",
                sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                "local main-daemon status has no cluster authority",
            )
        })?;
        let exact_local_main = daemon_paths.lock.exists()
            && status.data_dir == data_dir.root()
            && status.lock_file == daemon_paths.lock
            && cluster.cluster_id == state.cluster_id
            && cluster.node_id == state.local_node_id
            && cluster.process == "daemon-main";
        if !exact_local_main {
            return Err(sinomo_core::network::NetworkError::delivery_failed(
                "resolve explicit local main-daemon service path",
                sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                format!(
                    "local main-daemon status does not own cluster {} node {} at {}",
                    state.cluster_id,
                    state.local_node_id,
                    daemon_paths.root.display(),
                ),
            ));
        }
        return send_object_rpc_call_to_local_daemon_address_with_daemon_paths_and_network_error(
            daemon_paths,
            &status.address,
            call,
            options,
            runtime,
        )
        .await;
    }
    let mut host =
        DaemonObjectRpcAsyncHost::new(runtime, observe_connectivity).with_paths(daemon_paths);
    let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &paths);
    client
        .call_target(&state, target_node_id, call, options)
        .await
}

/// Sends an object-RPC directly to a daemon listener on this machine while
/// retaining the caller's streamed body and network delivery-state errors.
///
/// The caller must first prove which local process owns `address`; this helper
/// deliberately does not infer a process role from a node identity shared by
/// the main and shadow daemons.
pub(crate) async fn send_object_rpc_call_to_local_daemon_address_with_daemon_paths_and_network_error(
    daemon_paths: &DaemonPaths,
    address: &str,
    mut call: ObjectRpcCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
) -> Result<ObjectRpcResult, sinomo_core::network::NetworkError> {
    let data_dir = daemon_data_dir_from_paths(daemon_paths)?;
    let mut host = DaemonObjectRpcAsyncHost::new(runtime, false).with_paths(daemon_paths);
    call.request.auth = None;
    if sinomo_core::network::is_loopback_connect_address(address) {
        return sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host)
            .call_loopback(&data_dir, address, call, options)
            .await;
    }
    let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
    let state = sinomo_core::cluster::read_cluster_state_cached_shared(&cluster_paths)?;
    let endpoint = local_session_endpoint_for_address(&state, address).map_err(|error| {
        sinomo_core::network::NetworkError::delivery_failed(
            "resolve exact local main daemon endpoint",
            sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
            error.to_string(),
        )
    })?;
    sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths)
        .call_endpoint(&state, &endpoint, call, options)
        .await
}

fn block_on_daemon_client<F, T>(future: F) -> Result<T, DaemonError>
where
    F: std::future::Future<Output = Result<T, DaemonError>> + Send,
    T: Send,
{
    // Heap-pin the composed transport future before Tokio's synchronous
    // bridge carries it through generic `block_on` frames. The response body
    // is already streamed and heap-backed; retaining the concrete future by
    // value here only multiplies its static stack footprint on callers such as
    // bounded CLI fan-out workers.
    let future = Box::pin(future);
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
    block_on_daemon_client_on_process_runtime_pinned(future)
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
    block_on_daemon_client_on_process_runtime_pinned(Box::pin(future))
}

fn block_on_daemon_client_on_owner_runtime<F, T>(
    runtime: &tokio::runtime::Handle,
    future: F,
) -> Result<T, DaemonError>
where
    F: std::future::Future<Output = Result<T, DaemonError>> + Send,
    T: Send,
{
    let future = Box::pin(future);
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

fn daemon_client_sync_runtime() -> Result<&'static tokio::runtime::Runtime, DaemonError> {
    static RUNTIME: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();
    match RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("sinomo-daemon-client-sync")
            .enable_all()
            .build()
            .map_err(|source| source.to_string())
    }) {
        Ok(runtime) => Ok(runtime),
        Err(detail) => Err(DaemonError::Network {
            address: "daemon client runtime".to_string(),
            detail: detail.clone(),
        }),
    }
}

fn block_on_daemon_client_on_process_runtime<F, T>(future: F) -> Result<T, DaemonError>
where
    F: std::future::Future<Output = Result<T, DaemonError>>,
{
    block_on_daemon_client_on_process_runtime_pinned(Box::pin(future))
}

fn block_on_daemon_client_on_process_runtime_pinned<F, T>(
    future: std::pin::Pin<Box<F>>,
) -> Result<T, DaemonError>
where
    F: std::future::Future<Output = Result<T, DaemonError>>,
{
    daemon_client_sync_runtime()?.block_on(future)
}

pub(crate) struct DaemonObjectRpcAsyncHost {
    runtime: tokio::runtime::Handle,
    observe_connectivity: bool,
    paths: Option<DaemonPaths>,
}

impl DaemonObjectRpcAsyncHost {
    pub(crate) fn new(runtime: &tokio::runtime::Handle, observe_connectivity: bool) -> Self {
        Self {
            runtime: runtime.clone(),
            observe_connectivity,
            paths: None,
        }
    }

    pub(crate) fn direct_only(runtime: &tokio::runtime::Handle) -> Self {
        Self {
            runtime: runtime.clone(),
            observe_connectivity: false,
            paths: None,
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

    fn identity_data_dir(&self) -> Result<sinomo_core::local::SinomoDataDir, NetworkError> {
        if let Some(paths) = self.paths.as_ref() {
            return daemon_data_dir_from_paths(paths);
        }
        sinomo_core::local::SinomoDataDir::from_env_or_home().map_err(|source| NetworkError::Io {
            operation: "cluster transport identity data directory".to_string(),
            detail: source.to_string(),
        })
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

fn send_daemon_rpc_value<Req, Resp>(
    address: &str,
    route: &str,
    category: &str,
    value: Req,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + Send + 'static,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    let call = ObjectRpcCall::encode_value(route, category, value)?;
    block_on_daemon_client(async {
        let runtime = tokio::runtime::Handle::current();
        send_daemon_rpc_call_async(
            address,
            call,
            sinomo_core::network::ClusterObjectRpcOptions::new(
                sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
            ),
            &runtime,
        )
        .await?
        .decode_body()
        .await
        .map_err(Into::into)
    })
}

fn send_daemon_rpc_value_on_runtime<Req, Resp>(
    address: &str,
    route: &str,
    category: &str,
    value: Req,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + Send + 'static,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    let call = ObjectRpcCall::encode_value(route, category, value)?;
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(runtime, async {
        send_daemon_rpc_call_async(
            address,
            call,
            sinomo_core::network::ClusterObjectRpcOptions::new(
                sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
            ),
            &runtime_for_future,
        )
        .await?
        .decode_body()
        .await
        .map_err(Into::into)
    })
}

fn send_daemon_rpc_value_to_endpoint<Req, Resp>(
    endpoint: &ClusterEndpointRecord,
    route: &str,
    category: &str,
    value: Req,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + Send + 'static,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    let call = ObjectRpcCall::encode_value(route, category, value)?;
    block_on_daemon_client(async {
        let runtime = tokio::runtime::Handle::current();
        send_daemon_rpc_call_to_endpoint_with_options_and_connectivity_async(
            endpoint,
            call,
            sinomo_core::network::ClusterObjectRpcOptions::new(
                sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
            ),
            &runtime,
            true,
        )
        .await?
        .decode_body()
        .await
        .map_err(Into::into)
    })
}

fn send_daemon_rpc_value_to_endpoint_on_runtime<Req, Resp>(
    endpoint: &ClusterEndpointRecord,
    route: &str,
    category: &str,
    value: Req,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + Send + 'static,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    let call = ObjectRpcCall::encode_value(route, category, value)?;
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(runtime, async {
        send_daemon_rpc_call_to_endpoint_with_options_and_connectivity_async(
            endpoint,
            call,
            sinomo_core::network::ClusterObjectRpcOptions::new(
                sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
            ),
            &runtime_for_future,
            true,
        )
        .await?
        .decode_body()
        .await
        .map_err(Into::into)
    })
}

fn send_daemon_rpc_borrowed_value_to_endpoint_on_runtime<Req, Resp>(
    endpoint: &ClusterEndpointRecord,
    route: &str,
    category: &str,
    value: &Req,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    let call = ObjectRpcCall::encode_borrowed_value(route, category, value)?;
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(runtime, async {
        send_daemon_rpc_call_to_endpoint_with_options_and_connectivity_async(
            endpoint,
            call,
            sinomo_core::network::ClusterObjectRpcOptions::new(
                sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
            ),
            &runtime_for_future,
            true,
        )
        .await?
        .decode_body()
        .await
        .map_err(Into::into)
    })
}

async fn send_daemon_rpc_borrowed_value_to_endpoint_async<Req, Resp>(
    endpoint: &ClusterEndpointRecord,
    route: &str,
    category: &str,
    value: &Req,
    timeouts: sinomo_core::network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    let call = ObjectRpcCall::encode_borrowed_value(route, category, value)?;
    send_daemon_rpc_call_to_endpoint_with_options_and_connectivity_async(
        endpoint,
        call,
        sinomo_core::network::ClusterObjectRpcOptions::new(timeouts),
        runtime,
        true,
    )
    .await?
    .decode_body()
    .await
    .map_err(Into::into)
}

fn send_daemon_rpc_shared_value<Req, Resp>(
    address: &str,
    route: &str,
    category: &str,
    value: Arc<Req>,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + Send + Sync + 'static,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    let call = ObjectRpcCall::encode_shared_value(route, category, value)?;
    block_on_daemon_client(async {
        let runtime = tokio::runtime::Handle::current();
        send_daemon_rpc_call_async(
            address,
            call,
            sinomo_core::network::ClusterObjectRpcOptions::new(
                sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
            ),
            &runtime,
        )
        .await?
        .decode_body()
        .await
        .map_err(Into::into)
    })
}

fn send_daemon_rpc_shared_value_to_endpoint<Req, Resp>(
    endpoint: &ClusterEndpointRecord,
    route: &str,
    category: &str,
    value: Arc<Req>,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + Send + Sync + 'static,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    let call = ObjectRpcCall::encode_shared_value(route, category, value)?;
    block_on_daemon_client(async {
        let runtime = tokio::runtime::Handle::current();
        send_daemon_rpc_call_to_endpoint_with_options_and_connectivity_async(
            endpoint,
            call,
            sinomo_core::network::ClusterObjectRpcOptions::new(
                sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
            ),
            &runtime,
            true,
        )
        .await?
        .decode_body()
        .await
        .map_err(Into::into)
    })
}

fn send_daemon_rpc_shared_value_to_target<Req, Resp>(
    target_node_id: &NodeId,
    route: &str,
    category: &str,
    value: Arc<Req>,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + Send + Sync + 'static,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    let call = ObjectRpcCall::encode_shared_value(route, category, value)?;
    block_on_daemon_client(async {
        let runtime = tokio::runtime::Handle::current();
        send_object_rpc_call_to_target_async_with_options(
            target_node_id,
            call,
            sinomo_core::network::ClusterObjectRpcOptions::new(object_rpc_transfer_timeouts(
                timeout,
            )),
            &runtime,
            true,
        )
        .await?
        .decode_body()
        .await
        .map_err(Into::into)
    })
}

fn send_daemon_rollout_value<Req, Resp>(
    address: &str,
    route: &str,
    category: &str,
    value: Req,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + Send + 'static,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    send_daemon_rpc_value(address, route, category, value, timeout)
}

fn send_daemon_rollout_value_to_endpoint<Req, Resp>(
    endpoint: &ClusterEndpointRecord,
    route: &str,
    category: &str,
    value: Req,
    timeout: Duration,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + Send + 'static,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    send_daemon_rpc_value_to_endpoint(endpoint, route, category, value, timeout)
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

impl sinomo_core::network::ClusterObjectRpcAsyncHost for DaemonObjectRpcAsyncHost {
    fn observe_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        observation: sinomo_core::network::ClusterObjectRpcObservation,
    ) -> sinomo_core::network::ClusterObjectRpcFuture<'a, ()> {
        if !self.observe_connectivity {
            return Box::pin(async { Ok(()) });
        }
        let runtime = self.runtime.clone();
        let paths = self
            .paths
            .as_ref()
            .and_then(|paths| crate::cluster::cluster_paths_from_daemon_paths(paths).ok());
        let success = observation.is_success();
        let connectivity_observation =
            sinomo_core::cluster::ClusterCommandConnectivityObservation::from_rpc(observation);
        let coordinator = Arc::clone(daemon_rpc_observation_coordinator());
        let sequence = coordinator.next_sequence();
        let key = sinomo_core::cluster::cluster_command_observation_key(paths.as_ref(), endpoint);
        if success {
            if let Some(reservation) = coordinator.try_reserve_success(key, sequence) {
                let endpoint = endpoint.clone();
                let _observation_task = runtime.spawn_blocking(move || {
                    reservation.publish(move || {
                        remember_endpoint_command_connectivity(
                            paths,
                            endpoint,
                            connectivity_observation,
                        );
                    });
                });
            }
            return Box::pin(async { Ok(()) });
        }
        let endpoint = endpoint.clone();
        let Some(reservation) = coordinator.try_reserve_failure(key, sequence) else {
            return Box::pin(async { Ok(()) });
        };
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    reservation.publish(move || {
                        remember_endpoint_command_connectivity(
                            paths,
                            endpoint,
                            connectivity_observation,
                        );
                    });
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "record cluster endpoint RPC failure".to_string(),
                    detail: format!("connectivity observation task failed: {source}"),
                })?;
            Ok(())
        })
    }

    fn open_object_rpc_stream_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        health: sinomo_core::network::NetworkHealthPolicy,
    ) -> sinomo_core::network::ClusterObjectRpcFuture<
        'a,
        Box<dyn sinomo_core::network::NetworkDuplexStream>,
    > {
        let module_backed = sinomo_core::network::network_module_registry()
            .and_then(|registry| registry.module_for_endpoint(endpoint))
            .is_some();
        let routed_data_dir = module_backed.then(|| self.identity_data_dir());
        let endpoint = endpoint.clone();
        Box::pin(async move {
            if let Some(data_dir) = routed_data_dir {
                let data_dir = data_dir?;
                let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
                let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
                let directory =
                    sinomo_core::network::cluster_directory_for(&cluster_paths).snapshot()?;
                let identity = sinomo_core::network::DaemonRoutedSessionIdentity::new(
                    state.local_node_id,
                    data_dir,
                )?;
                let provider =
                    sinomo_core::network::RegisteredNetworkModuleLinkProvider::new(health);
                let stream = sinomo_core::network::connect_direct_routed_endpoint(
                    &directory,
                    &endpoint,
                    &provider,
                    &identity,
                    sinomo_core::network::ConnectOptions {
                        timeout: health.connect_timeout,
                        ..sinomo_core::network::ConnectOptions::default()
                    },
                )
                .await?;
                return Ok(Box::new(stream) as Box<dyn sinomo_core::network::NetworkDuplexStream>);
            }
            sinomo_core::network::open_daemon_rpc_stream_to_cluster_endpoint_async(
                &endpoint, health,
            )
            .await
        })
    }
}

fn daemon_rpc_observation_coordinator()
-> &'static Arc<sinomo_core::cluster::ClusterCommandObservationCoordinator> {
    DAEMON_RPC_OBSERVATION_COORDINATOR.get_or_init(|| {
        Arc::new(sinomo_core::cluster::ClusterCommandObservationCoordinator::production())
    })
}

fn daemon_paths_from_cluster_paths(
    paths: &sinomo_core::network::ClusterPaths,
) -> Option<DaemonPaths> {
    let data_dir = paths.root.parent()?;
    Some(DaemonPaths::new(&sinomo_core::local::SinomoDataDir::new(
        data_dir,
    )))
}

fn remember_endpoint_command_connectivity(
    paths: Option<sinomo_core::network::ClusterPaths>,
    endpoint: ClusterEndpointRecord,
    observation: sinomo_core::cluster::ClusterCommandConnectivityObservation,
) {
    let Some(paths) = paths.or_else(cluster_paths_from_env) else {
        return;
    };
    let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&paths) else {
        return;
    };
    let _ = observation.record_daemon_owned(&paths, &state, &endpoint);
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
        (),
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
        (),
        timeout,
        runtime,
    )
}

pub async fn fetch_daemon_status_from_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonStatus, DaemonError> {
    send_daemon_rpc_borrowed_value_to_endpoint_async(
        endpoint,
        crate::DAEMON_STATUS_ROUTE,
        crate::DAEMON_RPC_CATEGORY,
        &(),
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
        runtime,
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
    let runtime = daemon_client_sync_runtime()?;
    send_object_rpc_to_target_with_options_on_runtime(
        target_node_id,
        request,
        options,
        runtime.handle(),
        observe_connectivity,
    )
}

pub fn send_object_rpc_call_to_target_with_options(
    target_node_id: &NodeId,
    call: ObjectRpcCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    observe_connectivity: bool,
) -> Result<ObjectRpcResult, DaemonError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return send_object_rpc_call_to_target_with_options_on_runtime(
            target_node_id,
            call,
            options,
            &handle,
            observe_connectivity,
        );
    }
    let runtime = daemon_client_sync_runtime()?;
    send_object_rpc_call_to_target_with_options_on_runtime(
        target_node_id,
        call,
        options,
        runtime.handle(),
        observe_connectivity,
    )
}

pub(crate) fn send_object_rpc_call_to_target_on_runtime_with_daemon_paths_and_network_error(
    daemon_paths: &DaemonPaths,
    target_node_id: &NodeId,
    call: ObjectRpcCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<ObjectRpcResult, sinomo_core::network::NetworkError> {
    let daemon_paths = daemon_paths.clone();
    let target_node_id = target_node_id.clone();
    let runtime_for_future = runtime.clone();
    let future = Box::pin(async move {
        send_object_rpc_call_to_target_async_with_daemon_paths_and_network_error(
            &daemon_paths,
            &target_node_id,
            call,
            options,
            &runtime_for_future,
            observe_connectivity,
        )
        .await
    });
    if let Ok(current) = tokio::runtime::Handle::try_current() {
        if matches!(
            current.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return tokio::task::block_in_place(|| runtime.block_on(future));
        }
        return Err(sinomo_core::network::NetworkError::delivery_failed(
            "component object-RPC owner-runtime bridge",
            sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
            "synchronous component object RPC cannot run inside a current-thread Tokio runtime",
        ));
    }
    runtime.block_on(future)
}

pub(crate) fn send_object_rpc_to_target_with_options_on_runtime(
    target_node_id: &NodeId,
    request: &ObjectRpcRequest,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<sinomo_core::network::ObjectRpcResponse, DaemonError> {
    let target_node_id = target_node_id.clone();
    let request = request.clone();
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(runtime, async move {
        send_object_rpc_to_target_async_with_options(
            &target_node_id,
            &request,
            options,
            &runtime_for_future,
            observe_connectivity,
        )
        .await
    })
}

fn send_object_rpc_call_to_target_with_options_on_runtime(
    target_node_id: &NodeId,
    call: ObjectRpcCall,
    options: sinomo_core::network::ClusterObjectRpcOptions,
    runtime: &tokio::runtime::Handle,
    observe_connectivity: bool,
) -> Result<ObjectRpcResult, DaemonError> {
    let target_node_id = target_node_id.clone();
    let runtime_for_future = runtime.clone();
    block_on_daemon_client_on_owner_runtime(runtime, async move {
        send_object_rpc_call_to_target_async_with_options(
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
    materialize_daemon_client_value_call_with_options(
        "cluster target object RPC value call",
        options,
        send_object_rpc_call_to_target_async_with_options(
            target_node_id,
            ObjectRpcCall::value(request.clone()),
            options,
            runtime,
            observe_connectivity,
        ),
    )
    .await
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
        (),
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
        crate::DaemonClusterMaintenanceRefreshRequest,
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
        crate::DaemonClusterMaintenanceRefreshRequest,
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
        (),
        timeout,
    )
}

pub fn send_daemon_remote_shadow_shutdown_request(
    address: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    send_daemon_rpc_value(
        address,
        crate::DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE,
        crate::DAEMON_RPC_CATEGORY,
        (),
        timeout,
    )
}

fn validate_cluster_kv_local_snapshot(
    read: KvLocalRead<KvSnapshot>,
) -> Result<KvLocalRead<KvSnapshot>, DaemonError> {
    read.value.validate_resource_bounds()?;
    if read.revision != read.value.revision {
        return Err(DaemonError::Parse {
            detail: format!(
                "local cluster KV observation revision {:?} does not match snapshot revision {:?}",
                read.revision, read.value.revision
            ),
        });
    }
    Ok(read)
}

pub(crate) fn fetch_cluster_kv_local_snapshot_from_endpoint_on_runtime(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<KvLocalRead<KvSnapshot>, DaemonError> {
    let read: KvLocalRead<KvSnapshot> = send_daemon_rpc_borrowed_value_to_endpoint_on_runtime(
        endpoint,
        sinomo_core::kv::CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE,
        sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
        &(),
        timeout,
        runtime,
    )?;
    validate_cluster_kv_local_snapshot(read)
}

pub(crate) async fn fetch_cluster_kv_local_snapshot_from_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<KvLocalRead<KvSnapshot>, DaemonError> {
    let read: KvLocalRead<KvSnapshot> = send_daemon_rpc_borrowed_value_to_endpoint_async(
        endpoint,
        sinomo_core::kv::CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE,
        sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
        &(),
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
        runtime,
    )
    .await?;
    validate_cluster_kv_local_snapshot(read)
}

pub fn fetch_cluster_kv_authoritative_range_prefix_from_endpoint_on_runtime(
    endpoint: &ClusterEndpointRecord,
    prefix: &str,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<KvAuthoritativeRead<Vec<KvEntry>>, DaemonError> {
    let read: KvAuthoritativeRead<Vec<KvEntry>> =
        send_daemon_rpc_borrowed_value_to_endpoint_on_runtime(
            endpoint,
            sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE,
            sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
            &ClusterKvAuthoritativeRangePrefixRequest {
                prefix: prefix.to_string(),
            },
            timeout,
            runtime,
        )?;
    sinomo_core::kv::validate_cluster_kv_current_term_authority_proof(&read.authority)?;
    Ok(read)
}

/// Starts a barrier-backed watch on the addressed authority. The current
/// server contract deliberately fails on a non-leader rather than forwarding
/// only the barrier and registering against a potentially stale local store.
pub fn start_cluster_kv_authoritative_watch_from_endpoint_on_runtime(
    endpoint: &ClusterEndpointRecord,
    prefix: &str,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<KvAuthoritativeRead<ClusterKvWatchRegistration>, DaemonError> {
    let read: KvAuthoritativeRead<ClusterKvWatchRegistration> =
        send_daemon_rpc_borrowed_value_to_endpoint_on_runtime(
            endpoint,
            sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE,
            sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
            &ClusterKvAuthoritativeWatchStartRequest {
                prefix: prefix.to_string(),
            },
            timeout,
            runtime,
        )?;
    sinomo_core::kv::validate_cluster_kv_authoritative_watch_start_response(&read)?;
    Ok(read)
}

pub(crate) async fn start_cluster_kv_authoritative_watch_from_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    prefix: &str,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<KvAuthoritativeRead<ClusterKvWatchRegistration>, DaemonError> {
    let read: KvAuthoritativeRead<ClusterKvWatchRegistration> =
        send_daemon_rpc_borrowed_value_to_endpoint_async(
            endpoint,
            sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE,
            sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
            &ClusterKvAuthoritativeWatchStartRequest {
                prefix: prefix.to_string(),
            },
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
            runtime,
        )
        .await?;
    sinomo_core::kv::validate_cluster_kv_authoritative_watch_start_response(&read)?;
    Ok(read)
}

pub fn drain_cluster_kv_watch_from_endpoint_on_runtime(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterKvWatchDrainRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterKvWatchDrainResponse, DaemonError> {
    send_daemon_rpc_borrowed_value_to_endpoint_on_runtime(
        endpoint,
        sinomo_core::kv::CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE,
        sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
        request,
        timeout,
        runtime,
    )
}

pub(crate) async fn drain_cluster_kv_watch_from_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterKvWatchDrainRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterKvWatchDrainResponse, DaemonError> {
    send_daemon_rpc_borrowed_value_to_endpoint_async(
        endpoint,
        sinomo_core::kv::CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE,
        sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
        request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
        runtime,
    )
    .await
}

pub fn cancel_cluster_kv_watch_from_endpoint_on_runtime(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterKvWatchCancelRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterKvWatchCancelResponse, DaemonError> {
    send_daemon_rpc_borrowed_value_to_endpoint_on_runtime(
        endpoint,
        sinomo_core::kv::CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE,
        sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
        request,
        timeout,
        runtime,
    )
}

pub(crate) async fn cancel_cluster_kv_watch_from_endpoint_async(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterKvWatchCancelRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterKvWatchCancelResponse, DaemonError> {
    send_daemon_rpc_borrowed_value_to_endpoint_async(
        endpoint,
        sinomo_core::kv::CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE,
        sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
        request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
        runtime,
    )
    .await
}

pub(crate) fn send_cluster_raft_append_on_runtime(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRaftAppendRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterRaftAppendResponse, DaemonError> {
    send_daemon_rpc_borrowed_value_to_endpoint_on_runtime(
        endpoint,
        sinomo_core::kv::CLUSTER_RAFT_APPEND_ROUTE,
        sinomo_core::kv::CLUSTER_RAFT_RPC_CATEGORY,
        request,
        timeout,
        runtime,
    )
}

pub(crate) fn send_cluster_raft_vote_on_runtime(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRaftVoteRequest,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Result<ClusterRaftVoteResponse, DaemonError> {
    send_daemon_rpc_borrowed_value_to_endpoint_on_runtime(
        endpoint,
        sinomo_core::kv::CLUSTER_RAFT_VOTE_ROUTE,
        sinomo_core::kv::CLUSTER_RAFT_RPC_CATEGORY,
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
    send_daemon_rpc_borrowed_value_to_endpoint_async(
        endpoint,
        sinomo_core::network::CLUSTER_MEMBER_PUBLISH_ROUTE,
        sinomo_core::network::CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        request,
        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
        runtime,
    )
    .await
}

pub fn send_daemon_build_capabilities(
    address: &str,
    request: BuildCapabilitiesRequest,
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
    request: BuildCapabilitiesRequest,
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
    request: BuildCapabilitiesRequest,
) -> Result<BuildCapabilitiesResponse, DaemonError> {
    send_daemon_build_head_to_target(BUILD_CAPABILITIES_ROUTE, request, target_node_id)
}

pub fn send_daemon_build_status(
    address: &str,
    request: BuildStatusRequest,
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
    request: BuildStatusRequest,
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
    request: BuildStatusRequest,
) -> Result<BuildStatusResponse, DaemonError> {
    send_daemon_build_head_to_target(BUILD_STATUS_ROUTE, request, target_node_id)
}

fn send_daemon_build_head_to_target<Req, Resp>(
    route: &str,
    request: Req,
    target_node_id: &NodeId,
) -> Result<Resp, DaemonError>
where
    Req: serde::Serialize + Send + 'static,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    let call = ObjectRpcCall::encode_value(route, BUILD_RPC_CATEGORY, request)?;
    block_on_daemon_client(async {
        let runtime = tokio::runtime::Handle::current();
        send_object_rpc_call_to_target_async_with_options(
            target_node_id,
            call,
            main_daemon_build_target_rpc_options(
                sinomo_core::network::ClusterObjectRpcOptions::new(object_rpc_transfer_timeouts(
                    sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                )),
            ),
            &runtime,
            true,
        )
        .await?
        .decode_body()
        .await
        .map_err(Into::into)
    })
}

fn main_daemon_build_target_rpc_options(
    options: sinomo_core::network::ClusterObjectRpcOptions,
) -> sinomo_core::network::ClusterObjectRpcOptions {
    options.with_destination_process_affinity(
        sinomo_core::network::DestinationProcessAffinity::MainDaemon,
    )
}

fn daemon_build_transfer_timeouts() -> sinomo_core::network::DaemonRpcTimeouts {
    // Build submission and artifact delivery are bounded bulk transfers. A
    // routed WAN carrier may legitimately pause longer than the five-second
    // interactive default while the remote service opens its cached artifact
    // or applies backpressure. Retain the ordinary connect bound, allow a
    // longer no-progress interval, and keep a finite whole-operation budget.
    sinomo_core::network::DaemonRpcTimeouts::default()
        .with_idle_timeout(Some(DAEMON_BUILD_TRANSFER_IDLE_TIMEOUT))
}

fn daemon_build_transfer_rpc_options() -> sinomo_core::network::ClusterObjectRpcOptions {
    sinomo_core::network::ClusterObjectRpcOptions::new(daemon_build_transfer_timeouts())
        .with_overall_timeout(DAEMON_BUILD_TRANSFER_OVERALL_TIMEOUT)
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
            DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let call = sinomo_build::build_submit_rpc_call(&request, source_bundle.as_deref())?;
        let result = if sinomo_core::network::is_loopback_connect_address(&address) {
            let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
            client
                .call_loopback(
                    &data_dir,
                    &address,
                    call,
                    daemon_build_transfer_rpc_options(),
                )
                .await?
        } else {
            let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
            let endpoint = local_session_endpoint_for_address(&state, &address)?;
            let mut client =
                sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
            client
                .call_endpoint(&state, &endpoint, call, daemon_build_transfer_rpc_options())
                .await?
        };
        decode_build_submit_rpc_result(result).await
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
            DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let call = sinomo_build::build_submit_rpc_call(&request, source_bundle.as_deref())?;
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
            let mut client =
                sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
            client
                .call_endpoint(
                    &state,
                    &endpoint,
                    call,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        };
        decode_build_submit_rpc_result(result).await
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
            DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let mut client =
            sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
        let result = client
            .call_target(
                &state,
                &target_node_id,
                sinomo_build::build_submit_rpc_call(&request, source_bundle.as_deref())?,
                main_daemon_build_target_rpc_options(daemon_build_transfer_rpc_options()),
            )
            .await?;
        decode_build_submit_rpc_result(result).await
    })
}

async fn decode_build_submit_rpc_result(
    result: ObjectRpcResult,
) -> Result<BuildSubmitResponse, DaemonError> {
    result.decode_body().await.map_err(Into::into)
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
            DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let call = sinomo_build::build_artifact_rpc_call(&request)?;
        let result = if sinomo_core::network::is_loopback_connect_address(&address) {
            let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
            client
                .call_loopback(
                    &data_dir,
                    &address,
                    call,
                    daemon_build_transfer_rpc_options(),
                )
                .await?
        } else {
            let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
            let endpoint = local_session_endpoint_for_address(&state, &address)?;
            let mut client =
                sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
            client
                .call_endpoint(&state, &endpoint, call, daemon_build_transfer_rpc_options())
                .await?
        };
        sinomo_build::receive_build_artifact_rpc_result(result, &request, &destination_path)
            .await
            .map_err(Into::into)
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
            DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let call = sinomo_build::build_artifact_rpc_call(&request)?;
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
            let mut client =
                sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
            client
                .call_endpoint(
                    &state,
                    &endpoint,
                    call,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        };
        sinomo_build::receive_build_artifact_rpc_result(result, &request, &destination_path)
            .await
            .map_err(Into::into)
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
            DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let mut client =
            sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
        let result = client
            .call_target(
                &state,
                &target_node_id,
                sinomo_build::build_artifact_rpc_call(&request)?,
                main_daemon_build_target_rpc_options(daemon_build_transfer_rpc_options()),
            )
            .await?;
        sinomo_build::receive_build_artifact_rpc_result(result, &request, &destination_path)
            .await
            .map_err(Into::into)
    })
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
                    "daemon RPC address is ambiguous between cluster nodes {} and {}; target a node or endpoint explicitly",
                    endpoint.node_id, conflict.node_id
                ),
            });
        }
        return Ok(endpoint.clone());
    }
    Err(DaemonError::Network {
        address: address.to_string(),
        detail: "daemon RPC endpoint is not authorized in local cluster state".to_string(),
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
    block_on_daemon_client_on_process_runtime(future)
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
    let mut host = DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
    let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
    let result = client
        .call_loopback(
            &data_dir,
            &address,
            sinomo_rollout::rollout_binary_rpc_call(&request, &attachment_path)?,
            sinomo_core::network::ClusterObjectRpcOptions::default(),
        )
        .await?;
    decode_rollout_binary_rpc_result(result).await
}

async fn decode_rollout_binary_rpc_result(
    result: ObjectRpcResult,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    result.decode_body().await.map_err(Into::into)
}

async fn send_daemon_rollout_binary_to_endpoint_async(
    cluster_paths: sinomo_core::network::ClusterPaths,
    state: sinomo_core::ClusterBootstrapState,
    endpoint: ClusterEndpointRecord,
    request: ClusterRolloutBinaryRequest,
    attachment_path: std::path::PathBuf,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let runtime = tokio::runtime::Handle::current();
    let mut host = DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
    let mut client =
        sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
    let result = client
        .call_endpoint(
            &state,
            &endpoint,
            sinomo_rollout::rollout_binary_rpc_call(&request, &attachment_path)?,
            sinomo_core::network::ClusterObjectRpcOptions::default(),
        )
        .await?;
    decode_rollout_binary_rpc_result(result).await
}

async fn send_daemon_rollout_binary_to_target_async(
    cluster_paths: sinomo_core::network::ClusterPaths,
    state: sinomo_core::ClusterBootstrapState,
    target_node_id: NodeId,
    request: ClusterRolloutBinaryRequest,
    attachment_path: std::path::PathBuf,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    let runtime = tokio::runtime::Handle::current();
    let mut host = DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
    let mut client =
        sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
    let result = client
        .call_target(
            &state,
            &target_node_id,
            sinomo_rollout::rollout_binary_rpc_call(&request, &attachment_path)?,
            sinomo_core::network::ClusterObjectRpcOptions::default(),
        )
        .await?;
    decode_rollout_binary_rpc_result(result).await
}

pub fn send_daemon_rollout_native_dylib_module(
    address: &str,
    request: Arc<ClusterRolloutNativeDylibModuleRequest>,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rpc_shared_value(
        address,
        ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_native_dylib_module_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: Arc<ClusterRolloutNativeDylibModuleRequest>,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rpc_shared_value_to_endpoint(
        endpoint,
        ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        request,
        timeout,
    )
}

pub fn send_daemon_rollout_native_dylib_module_to_target(
    target_node_id: &NodeId,
    request: Arc<ClusterRolloutNativeDylibModuleRequest>,
    timeout: Duration,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    send_daemon_rpc_shared_value_to_target(
        target_node_id,
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
            DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let result = if sinomo_core::network::is_loopback_connect_address(&address) {
            let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
            client
                .call_loopback(
                    &data_dir,
                    &address,
                    sinomo_rollout::rollout_build_rpc_call(request)?,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        } else {
            let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
            let endpoint = local_session_endpoint_for_address(&state, &address)?;
            let mut client =
                sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
            client
                .call_endpoint(
                    &state,
                    &endpoint,
                    sinomo_rollout::rollout_build_rpc_call(request)?,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        };
        decode_rollout_build_rpc_result(result).await
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
            DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let result = if sinomo_core::network::is_loopback_connect_address(&endpoint.address) {
            let mut client = sinomo_core::network::ClusterObjectRpcClient::endpoint_only(&mut host);
            client
                .call_loopback(
                    &data_dir,
                    &endpoint.address,
                    sinomo_rollout::rollout_build_rpc_call(request)?,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        } else {
            let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
            let mut client =
                sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
            client
                .call_endpoint(
                    &state,
                    &endpoint,
                    sinomo_rollout::rollout_build_rpc_call(request)?,
                    sinomo_core::network::ClusterObjectRpcOptions::default(),
                )
                .await?
        };
        decode_rollout_build_rpc_result(result).await
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
            DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let mut client =
            sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
        let result = client
            .call_target(
                &state,
                &target_node_id,
                sinomo_rollout::rollout_build_rpc_call(request)?,
                sinomo_core::network::ClusterObjectRpcOptions::default(),
            )
            .await?;
        decode_rollout_build_rpc_result(result).await
    })
}

async fn decode_rollout_build_rpc_result(
    result: ObjectRpcResult,
) -> Result<ClusterRolloutBinaryResponse, DaemonError> {
    result.decode_body().await.map_err(Into::into)
}

pub fn send_daemon_rollout_clear_status(
    address: &str,
    timeout: Duration,
) -> Result<String, DaemonError> {
    send_daemon_rollout_value(
        address,
        ROLLOUT_CLEAR_STATUS_ROUTE,
        ROLLOUT_RPC_CATEGORY,
        (),
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
        (),
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cluster_node_target_uses_explicit_local_main_service_path() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-local-target-service-path");
        let data_dir = sinomo_core::local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_local_target")
                .expect("cluster id");
        let local_node = sinomo_core::node::NodeId::from_legacy_named("node_daemon_local_target")
            .expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "daemon local target",
            local_node.clone(),
            "local",
            "pk-local",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let daemon_paths = crate::DaemonPaths::new(&data_dir);
        daemon_paths.ensure().expect("daemon paths");
        std::fs::write(&daemon_paths.lock, []).expect("daemon authority lock evidence");
        let listener = sinomo_core::network::AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("local daemon listener");
        let address = listener.local_addr_string().expect("listener address");
        let port = address
            .parse::<std::net::SocketAddr>()
            .expect("listener socket address")
            .port();
        let local_member = state
            .members
            .iter()
            .find(|member| member.node_id == local_node)
            .expect("local member");
        let mut status = crate::DaemonStatus::new(
            &crate::DaemonRunOptions {
                host: "127.0.0.1".to_string(),
                port,
                port_fallback: false,
            },
            &daemon_paths,
            Some(crate::DaemonClusterStatus {
                cluster_id,
                node_id: local_node.clone(),
                process: "daemon-main".to_string(),
                member_role: local_member.role.clone(),
                member_state: local_member.state.clone(),
            }),
        );
        status.address = address;
        crate::write_daemon_status(&daemon_paths, &status).expect("daemon status");

        let server = tokio::spawn(async move {
            let mut carrier = listener
                .accept()
                .await
                .expect("accept local daemon carrier")
                .into_inner();
            let carrier_context =
                sinomo_core::network::read_local_object_rpc_carrier_prelude(&mut carrier)
                    .await
                    .expect("local carrier prelude");
            let mut acceptor = sinomo_core::network::accept_local_object_rpc_stream_carrier(
                Box::new(carrier),
                &carrier_context,
            )
            .expect("accept local carrier");
            let stream = acceptor
                .accept()
                .await
                .expect("accept local logical stream")
                .expect("local carrier closed before request");
            let context = sinomo_core::network::RpcPeerContext::unauthenticated_local(
                stream.session_id.clone(),
                stream.channel_binding,
            );
            sinomo_core::network::serve_service_rpc_peer_stream(
                stream,
                &context,
                sinomo_core::network::ObjectFrameIoOptions::default(),
                |incoming, peer| async move {
                    assert!(matches!(
                        peer.identity,
                        sinomo_core::network::RpcPeerIdentity::UnauthenticatedLocal
                    ));
                    assert_eq!(incoming.request.route, "/test/daemon-local-target");
                    assert!(incoming.request.auth.is_none());
                    Ok::<_, sinomo_core::network::NetworkError>(
                        sinomo_core::network::ObjectRpcOutgoingResponse::value(
                            sinomo_core::network::ObjectRpcResponse::encode_body(
                                "/test/daemon-local-target",
                                &"local-main",
                            )?,
                        ),
                    )
                },
            )
            .await
            .expect("serve local target request");
        });

        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            "/test/daemon-local-target",
            "test",
            &"request",
        )
        .expect("request");
        let result = crate::cluster::call_node_object_rpc_with_cluster_paths_async_on_runtime(
            &cluster_paths,
            &state,
            &local_node,
            sinomo_core::network::ObjectRpcCall::value(request.clone()),
            sinomo_core::network::ClusterObjectRpcOptions::default()
                .with_overall_timeout(Duration::from_secs(2)),
            &tokio::runtime::Handle::current(),
        )
        .await
        .expect("local target uses explicit daemon service path")
        .into_materialized_value_response(usize::MAX)
        .await
        .expect("local target uses explicit daemon service path");
        assert_eq!(
            result.decode_body::<String>().expect("response body"),
            "local-main"
        );
        server.await.expect("local daemon server");

        status.cluster.as_mut().expect("cluster status").process = "daemon-shadow".to_string();
        crate::write_daemon_status(&daemon_paths, &status).expect("shadow daemon status");
        let error = crate::cluster::call_node_object_rpc_with_cluster_paths_async_on_runtime(
            &cluster_paths,
            &state,
            &local_node,
            sinomo_core::network::ObjectRpcCall::value(request),
            sinomo_core::network::ClusterObjectRpcOptions::default()
                .with_overall_timeout(Duration::from_secs(2)),
            &tokio::runtime::Handle::current(),
        )
        .await
        .expect_err("shadow daemon status must not authorize the local main service path");
        assert!(
            error
                .to_string()
                .contains("local main-daemon status does not own cluster")
        );
        assert!(error.to_string().contains("DefinitelyNotSent"));
    }

    #[test]
    fn synchronous_client_runtime_keeps_background_tasks_alive_between_calls() {
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let task = super::block_on_daemon_client_on_process_runtime(async move {
            Ok(tokio::spawn(async move {
                release_rx.await.expect("release background task");
                37_u8
            }))
        })
        .expect("spawn background task");

        let value = super::block_on_daemon_client_on_process_runtime(async move {
            release_tx
                .send(())
                .map_err(|_| crate::DaemonError::Network {
                    address: "daemon client runtime test".to_string(),
                    detail: "background task stopped with its originating call".to_string(),
                })?;
            task.await.map_err(|source| crate::DaemonError::Network {
                address: "daemon client runtime test".to_string(),
                detail: source.to_string(),
            })
        })
        .expect("background task survives the first synchronous call");

        assert_eq!(value, 37);
    }

    #[tokio::test]
    async fn materialized_value_call_deadline_covers_a_stalled_streamed_response_body() {
        let timeout = Duration::from_millis(20);
        let (_body_sender, body) =
            sinomo_core::network::ObjectRpcIncomingBody::test_channel(None, 1);
        let response = sinomo_core::network::ObjectRpcResponse::from_encoded_body(
            "/test/stalled-streamed-value",
            Vec::new(),
        );
        let result =
            sinomo_core::network::ObjectRpcResult::from_forwarded_body(response, body, true)
                .expect("construct streamed value response");
        let options =
            sinomo_core::network::ClusterObjectRpcOptions::default().with_overall_timeout(timeout);

        let error = match super::materialize_daemon_client_value_call_with_options(
            "test object RPC value call",
            options,
            async move { Ok(result) },
        )
        .await
        {
            Ok(_) => panic!("a response body that never completes must time out"),
            Err(error) => error,
        };
        assert!(
            matches!(
                error,
                crate::DaemonError::Parse { ref detail }
                    if detail == &sinomo_core::network::NetworkError::timeout(
                        "test object RPC value call",
                        sinomo_core::network::NetworkTimeoutPhase::Overall,
                        timeout,
                        sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                    )
                    .to_string()
            ),
            "unexpected timeout error: {error}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn materialized_value_call_deadline_covers_a_progress_active_streamed_response() {
        let timeout = Duration::from_millis(20);
        let (body_sender, body) =
            sinomo_core::network::ObjectRpcIncomingBody::test_channel(None, 1);
        let response = sinomo_core::network::ObjectRpcResponse::from_encoded_body(
            "/test/progress-active-streamed-value",
            Vec::new(),
        );
        let result =
            sinomo_core::network::ObjectRpcResult::from_forwarded_body(response, body, true)
                .expect("construct streamed value response");
        let progress = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let producer_progress = std::sync::Arc::clone(&progress);
        let producer = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
                if body_sender.send(vec![1]).await.is_err() {
                    return;
                }
                producer_progress.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        });
        let options =
            sinomo_core::network::ClusterObjectRpcOptions::default().with_overall_timeout(timeout);
        let started = tokio::time::Instant::now();

        let error = match super::materialize_daemon_client_value_call_with_options(
            "test progress-active object RPC value call",
            options,
            async move { Ok(result) },
        )
        .await
        {
            Ok(_) => panic!("continuous response progress must not extend the overall deadline"),
            Err(error) => error,
        };
        let elapsed = tokio::time::Instant::now().duration_since(started);
        producer.abort();

        assert_eq!(elapsed, timeout);
        assert!(
            progress.load(std::sync::atomic::Ordering::SeqCst) >= 2,
            "the response must make progress before the overall deadline wins"
        );
        assert!(
            matches!(
                error,
                crate::DaemonError::Parse { ref detail }
                    if detail == &sinomo_core::network::NetworkError::timeout(
                        "test progress-active object RPC value call",
                        sinomo_core::network::NetworkTimeoutPhase::Overall,
                        timeout,
                        sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                    )
                    .to_string()
            ),
            "unexpected timeout error: {error}"
        );
    }

    #[test]
    fn local_cluster_kv_snapshot_decoder_preserves_observation_metadata() {
        let snapshot = sinomo_core::kv::InProcessKvStore::new()
            .try_snapshot()
            .expect("empty snapshot");
        let read = sinomo_core::kv::KvLocalRead {
            revision: snapshot.revision,
            observed_term: 7,
            applied_index: 11,
            known_leader: Some(
                sinomo_core::node::NodeId::from_legacy_named("node_local_snapshot_leader")
                    .expect("leader node id"),
            ),
            value: snapshot,
        };
        let decoded =
            super::validate_cluster_kv_local_snapshot(read.clone()).expect("validate snapshot");

        assert_eq!(decoded, read);

        let inconsistent = sinomo_core::kv::KvLocalRead {
            revision: sinomo_core::kv::KvRevision(read.revision.0 + 1),
            ..read
        };
        assert!(matches!(
            super::validate_cluster_kv_local_snapshot(inconsistent),
            Err(crate::DaemonError::Parse { .. })
        ));
    }

    #[test]
    fn authoritative_range_and_watch_decoders_require_barrier_proof() {
        let authority = sinomo_core::kv::KvAuthorityProof {
            term: 5,
            commit_index: 13,
            revision: sinomo_core::kv::KvRevision(8),
            basis: sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 2,
                required_quorum: 2,
            },
        };
        let range = sinomo_core::kv::KvAuthoritativeRead {
            value: vec![sinomo_core::kv::KvEntry {
                key: "/client/range".to_string(),
                value: b"value".to_vec(),
                create_revision: authority.revision,
                modify_revision: authority.revision,
                version: sinomo_core::kv::KvVersion(1),
                lease_id: None,
            }],
            authority,
        };
        sinomo_core::kv::validate_cluster_kv_current_term_authority_proof(&range.authority)
            .expect("validate range");

        let watch = sinomo_core::kv::KvAuthoritativeRead {
            value: sinomo_core::kv::ClusterKvWatchRegistration {
                watch_id: sinomo_core::kv::WatchId(21),
                prefix: "/client/".to_string(),
                next_revision: authority.revision.next(),
            },
            authority,
        };
        sinomo_core::kv::validate_cluster_kv_authoritative_watch_start_response(&watch)
            .expect("validate watch");

        let stale_proof = sinomo_core::kv::KvAuthoritativeRead {
            value: range.value,
            authority: sinomo_core::kv::KvAuthorityProof {
                basis: sinomo_core::kv::KvAuthorityBasis::ExactCommittedRequest,
                ..authority
            },
        };
        assert!(matches!(
            sinomo_core::kv::validate_cluster_kv_current_term_authority_proof(
                &stale_proof.authority
            ),
            Err(sinomo_core::kv::KvError::AuthorityUnavailable { .. })
        ));
    }

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
    fn build_transfers_allow_routed_wan_backpressure_with_a_finite_deadline() {
        let timeouts = super::daemon_build_transfer_timeouts();
        assert_eq!(
            timeouts.connect_timeout,
            Some(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        );
        assert_eq!(
            timeouts.idle_timeout,
            Some(super::DAEMON_BUILD_TRANSFER_IDLE_TIMEOUT)
        );

        let options = super::daemon_build_transfer_rpc_options();
        assert_eq!(
            options.overall_timeout(),
            Some(super::DAEMON_BUILD_TRANSFER_OVERALL_TIMEOUT)
        );
        assert_eq!(
            super::main_daemon_build_target_rpc_options(options).destination_process_affinity(),
            sinomo_core::network::DestinationProcessAffinity::MainDaemon
        );
    }

    #[tokio::test]
    async fn rollout_owned_stream_survives_backpressure() {
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-binary-loopback-session");
        let data_dir = sinomo_core::local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        sinomo_core::node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main node identity");
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");

        let bytes = (0..16 * 1024 * 1024)
            .map(|offset| (offset % 251) as u8)
            .collect::<Vec<_>>();
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
                host_override: None,
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
        let expected = bytes.clone();
        let server = tokio::spawn(async move {
            let mut carrier = listener
                .accept()
                .await
                .expect("accept loopback stream")
                .into_inner();
            let carrier_context =
                sinomo_core::network::read_local_object_rpc_carrier_prelude(&mut carrier)
                    .await
                    .expect("local carrier prelude");
            let mut acceptor = sinomo_core::network::accept_local_object_rpc_stream_carrier(
                Box::new(carrier),
                &carrier_context,
            )
            .expect("accept local carrier");
            let stream = acceptor
                .accept()
                .await
                .expect("accept local logical stream")
                .expect("local carrier closed before rollout stream");
            let context = sinomo_core::network::RpcPeerContext::unauthenticated_local(
                stream.session_id.clone(),
                stream.channel_binding,
            );
            sinomo_core::network::serve_service_rpc_peer_stream(
                stream,
                &context,
                sinomo_core::network::ObjectFrameIoOptions::default(),
                move |mut incoming, peer| async move {
                    assert!(matches!(
                        peer.identity,
                        sinomo_core::network::RpcPeerIdentity::UnauthenticatedLocal
                    ));
                    assert!(
                        incoming.request.auth.is_none(),
                        "the local carrier identity is the authority; request-head auth must not be duplicated"
                    );
                    let mut received = Vec::with_capacity(expected.len());
                    let mut chunk = [0_u8; 8 * 1024];
                    loop {
                        let read = incoming
                            .body
                            .read(&mut chunk)
                            .await
                            .expect("read rollout body");
                        if read == 0 {
                            break;
                        }
                        received.extend_from_slice(&chunk[..read]);
                        // Make the receiver visibly slower than the sender so
                        // the owned logical stream must propagate carrier
                        // backpressure rather than buffering the whole body.
                        tokio::task::yield_now().await;
                    }
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
                    Ok(sinomo_core::network::ObjectRpcOutgoingResponse::value(
                        response,
                    ))
                },
            )
            .await
            .expect("serve stream-native rollout RPC");
        });

        let response = tokio::time::timeout(
            Duration::from_secs(60),
            super::send_daemon_rollout_binary_to_address_async(
                data_dir,
                address,
                request,
                attachment_path,
            ),
        )
        .await
        .expect("owned rollout stream must not hang under backpressure")
        .expect("loopback binary rollout");
        tokio::time::timeout(Duration::from_secs(60), server)
            .await
            .expect("server must finish consuming the rollout stream")
            .expect("server task");

        assert_eq!(response.status, "staged");
        assert_eq!(response.detail, "received loopback body");
    }

    #[test]
    fn daemon_rpc_address_rejects_cross_node_ambiguity() {
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
        let target_cluster_paths = sinomo_core::network::ClusterPaths::new(&target_data_dir);
        let mut target_state = state.clone();
        target_state.local_node_id = target_node.clone();
        sinomo_core::cluster::write_cluster_state(&target_cluster_paths, &target_state)
            .expect("target cluster state");
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
            let directory = sinomo_core::network::DirectorySnapshot::try_from_records(
                sinomo_core::network::DirectoryVersion(1),
                state.members.clone(),
                std::iter::once(endpoint.clone()),
            )
            .expect("directory");
            let target_routed_identity = std::sync::Arc::new(
                sinomo_core::network::DaemonRoutedSessionIdentity::new(
                    target_node.clone(),
                    target_data_dir,
                )
                .expect("target routed identity"),
            );
            let target_link_provider = sinomo_core::network::ClusterLinkProvider::new(
                target_node.clone(),
                target_routed_identity.clone(),
                sinomo_core::network::NetworkHealthPolicy::default(),
            )
            .expect("target link provider");
            let server_target_node = target_node.clone();
            let server_local_node = local_node.clone();
            let server = tokio::spawn(async move {
                let mut stream = tokio::time::timeout(Duration::from_secs(3), listener.accept())
                    .await
                    .expect("client dialed authenticated endpoint")
                    .expect("accept")
                    .into_inner();
                let metadata =
                    sinomo_core::network::read_authenticated_transport_prelude(&mut stream)
                        .await
                        .expect("authenticated transport prelude");
                let source_node =
                    sinomo_core::network::verify_builtin_authenticated_transport_metadata(
                        &metadata,
                        &server_target_node,
                        &directory,
                        target_routed_identity.as_ref(),
                    )
                    .expect("source transport identity");
                let direct = sinomo_core::network::AuthenticatedDirectStream::accepted(
                    Box::new(stream),
                    server_target_node.clone(),
                    source_node,
                    sinomo_core::Id::generate(),
                    metadata.channel_binding,
                    metadata.capabilities,
                )
                .expect("accepted direct stream");
                let routed = match sinomo_core::network::accept_or_relay_routed_session(
                    direct,
                    &directory,
                    target_routed_identity.as_ref(),
                    &target_link_provider,
                    None,
                )
                .await
                .expect("routed session")
                {
                    sinomo_core::network::RoutedSessionDisposition::Destination(stream) => stream,
                    sinomo_core::network::RoutedSessionDisposition::Relayed(_) => {
                        panic!("two-node route terminated as relay")
                    }
                };
                let mut acceptor = sinomo_core::network::accept_routed_stream_carrier(routed);
                let routed = acceptor
                    .accept()
                    .await
                    .expect("accept routed logical stream")
                    .expect("routed logical stream");
                let context = sinomo_core::network::RpcPeerContext::authenticated(
                    routed.local_node_id.clone(),
                    routed.peer_node_id.clone(),
                    routed.session_id.clone(),
                    routed.channel_binding,
                )
                .expect("service peer context");
                sinomo_core::network::serve_service_rpc_peer_stream(
                    routed,
                    &context,
                    sinomo_core::network::ObjectFrameIoOptions::default(),
                    move |request, peer| async move {
                        assert_eq!(
                            peer.identity,
                            sinomo_core::network::RpcPeerIdentity::Authenticated {
                                local_node_id: server_target_node,
                                peer_node_id: server_local_node,
                            }
                        );
                        assert!(
                            request.request.auth.is_none(),
                            "ordinary service RPC must not add a per-call signature"
                        );
                        Err::<sinomo_core::network::ObjectRpcOutgoingResponse, _>(
                            sinomo_core::network::NetworkError::AuthDenied,
                        )
                    },
                )
                .await
                .expect("send final service authorization denial");
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), listener.accept())
                        .await
                        .is_err(),
                    "ordinary endpoint RPC must not retry after authorization denial"
                );
            });

            let mut host = super::DaemonObjectRpcAsyncHost::new(runtime.handle(), true)
                .with_paths(&daemon_paths);
            // Bind request authentication and routed-stream proofs to the same
            // test identity; an endpoint-only client would resolve proofs from
            // the ambient data directory instead.
            let mut client =
                sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
            let request = ObjectRpcRequest::encode_body("/test/auth-denied", "test", &())
                .expect("request")
                .with_metadata("sender-controlled", "must-stay-bound");
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                client.call_endpoint(
                    &state,
                    &endpoint,
                    sinomo_core::network::ObjectRpcCall::value(request),
                    sinomo_core::network::ClusterObjectRpcOptions::new(
                        sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(
                            Duration::from_secs(1),
                        ),
                    ),
                ),
            )
            .await
            .expect("stream-native endpoint RPC completed before timeout");
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
