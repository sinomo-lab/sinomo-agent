//! Typed clients for the independently owned rollout component.

use serde::de::DeserializeOwned;
use sinomo_core::ClusterEndpointRecord;
use sinomo_core::network::{ObjectRpcRequest, ObjectRpcResult};
use sinomo_core::node::NodeId;
use sinomo_daemon::{
    DaemonError, DaemonObjectRpcAsyncHost, block_on_daemon_client_on_owner_runtime,
    block_on_daemon_client_on_process_runtime, send_daemon_rpc, send_daemon_rpc_shared_value,
    send_daemon_rpc_shared_value_to_endpoint, send_daemon_rpc_shared_value_to_target,
    send_daemon_rpc_to_endpoint, send_daemon_rpc_value, send_daemon_rpc_value_to_endpoint,
};
use sinomo_rollout::{
    ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse, ClusterRolloutBuildRequest,
    ClusterRolloutNativeDylibModuleRequest, ROLLOUT_CLEAR_STATUS_ROUTE, ROLLOUT_DIAGNOSTICS_ROUTE,
    ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE, ROLLOUT_RPC_CATEGORY,
};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

fn rollout_error(error: sinomo_rollout::RolloutError) -> DaemonError {
    DaemonError::Io {
        path: "sinomo rollout".to_string(),
        detail: error.to_string(),
    }
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
    Resp: DeserializeOwned + Send + 'static,
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
    Resp: DeserializeOwned + Send + 'static,
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
            sinomo_rollout::rollout_binary_rpc_call(&request, &attachment_path)
                .map_err(rollout_error)?,
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
            sinomo_rollout::rollout_binary_rpc_call(&request, &attachment_path)
                .map_err(rollout_error)?,
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
            sinomo_rollout::rollout_binary_rpc_call(&request, &attachment_path)
                .map_err(rollout_error)?,
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
                    sinomo_rollout::rollout_build_rpc_call(request).map_err(rollout_error)?,
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
                    sinomo_rollout::rollout_build_rpc_call(request).map_err(rollout_error)?,
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
                    sinomo_rollout::rollout_build_rpc_call(request).map_err(rollout_error)?,
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
                    sinomo_rollout::rollout_build_rpc_call(request).map_err(rollout_error)?,
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
                sinomo_rollout::rollout_build_rpc_call(request).map_err(rollout_error)?,
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
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

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
}
