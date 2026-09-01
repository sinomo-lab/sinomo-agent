//! Typed clients for the independently owned build component.

use sinomo_build::{
    BUILD_CAPABILITIES_ROUTE, BUILD_RPC_CATEGORY, BUILD_STATUS_ROUTE, BuildArtifactRequest,
    BuildArtifactResponse, BuildCapabilitiesRequest, BuildCapabilitiesResponse, BuildStatusRequest,
    BuildStatusResponse, BuildSubmitRequest, BuildSubmitResponse,
};
use sinomo_core::ClusterEndpointRecord;
use sinomo_core::network::{ObjectRpcCall, ObjectRpcResult};
use sinomo_core::node::NodeId;
use sinomo_daemon::{
    DaemonError, DaemonObjectRpcAsyncHost, block_on_daemon_client_session,
    local_session_cluster_state, local_session_data_dir, local_session_endpoint_for_address,
    object_rpc_transfer_timeouts, send_daemon_rpc_value, send_daemon_rpc_value_to_endpoint,
    send_object_rpc_call_to_target_async_with_options,
};
use std::path::Path;
use std::time::Duration;

const DAEMON_BUILD_TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const DAEMON_BUILD_TRANSFER_OVERALL_TIMEOUT: Duration = Duration::from_secs(30 * 60);

fn build_error(error: sinomo_build::BuildError) -> DaemonError {
    match error {
        sinomo_build::BuildError::Io { path, detail } => DaemonError::Io { path, detail },
        other => DaemonError::Parse {
            detail: other.to_string(),
        },
    }
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
    block_on_daemon_client_session(async {
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
    block_on_daemon_client_session(async move {
        let runtime = tokio::runtime::Handle::current();
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let mut host =
            DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let call = sinomo_build::build_submit_rpc_call(&request, source_bundle.as_deref())
            .map_err(build_error)?;
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
    block_on_daemon_client_session(async move {
        let runtime = tokio::runtime::Handle::current();
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let mut host =
            DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let call = sinomo_build::build_submit_rpc_call(&request, source_bundle.as_deref())
            .map_err(build_error)?;
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
    block_on_daemon_client_session(async move {
        let runtime = tokio::runtime::Handle::current();
        let mut host =
            DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let mut client =
            sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
        let result = client
            .call_target(
                &state,
                &target_node_id,
                sinomo_build::build_submit_rpc_call(&request, source_bundle.as_deref())
                    .map_err(build_error)?,
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

#[cfg(test)]
mod tests {
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
    block_on_daemon_client_session(async move {
        let runtime = tokio::runtime::Handle::current();
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let mut host =
            DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let call = sinomo_build::build_artifact_rpc_call(&request).map_err(build_error)?;
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
            .map_err(build_error)
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
    block_on_daemon_client_session(async move {
        let runtime = tokio::runtime::Handle::current();
        let cluster_paths = sinomo_core::network::ClusterPaths::new(&data_dir);
        let mut host =
            DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let call = sinomo_build::build_artifact_rpc_call(&request).map_err(build_error)?;
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
            .map_err(build_error)
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
    block_on_daemon_client_session(async move {
        let runtime = tokio::runtime::Handle::current();
        let mut host =
            DaemonObjectRpcAsyncHost::new(&runtime, true).with_cluster_paths(&cluster_paths);
        let mut client =
            sinomo_core::network::ClusterObjectRpcClient::routed(&mut host, &cluster_paths);
        let result = client
            .call_target(
                &state,
                &target_node_id,
                sinomo_build::build_artifact_rpc_call(&request).map_err(build_error)?,
                main_daemon_build_target_rpc_options(daemon_build_transfer_rpc_options()),
            )
            .await?;
        sinomo_build::receive_build_artifact_rpc_result(result, &request, &destination_path)
            .await
            .map_err(build_error)
    })
}
