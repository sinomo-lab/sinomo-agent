use crate::control_plane_options::DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT;
#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
#[cfg(any(test, feature = "test-support"))]
use crate::local_state_store::read_cluster_state;
use sinomo_base::ClusterBootstrapState;
use sinomo_network::ClusterPaths;
use sinomo_network::DaemonRpcTimeouts;
use sinomo_network::NetworkError;
use sinomo_network::{ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector};
use sinomo_network::{
    ClusterObjectRpcClient, ClusterObjectRpcOptions, ClusterSignedEndpointRpcAsyncHost,
};
use sinomo_network::{ObjectRpcRequest, ObjectRpcResponse};
use sinomo_node::NodeId;
use std::time::Instant;

fn membership_forward_trace(args: std::fmt::Arguments<'_>) {
    let enabled = std::env::var("SINOMO_CLUSTER_JOIN_TRACE")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"));
    if enabled {
        eprintln!("[sinomo cluster join] {args}");
    }
}

pub trait ClusterMembershipRpcForwardHost {
    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError>;
}

pub fn forward_cluster_membership_rpc_to_state_authority_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request: &ObjectRpcRequest,
    failure_detail: &str,
    host: &mut impl ClusterMembershipRpcForwardHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let mut failures = Vec::new();
    for target_node_id in ClusterEndpointSelector::new(paths, state)
        .select_internal_unique_node_targets(ClusterEndpointInternalSelectionPurpose::VoterCommand)
        .into_iter()
        .map(|endpoint| endpoint.node_id)
    {
        match host.call_node_object_rpc(state, &target_node_id, request) {
            Ok(response) => return Ok(response),
            Err(error) => {
                failures.push(format!("node {target_node_id}: {error}"));
            }
        }
    }
    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "{failure_detail}{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

pub async fn forward_cluster_membership_rpc_to_state_authority_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request: &ObjectRpcRequest,
    failure_detail: &str,
    host: &mut (impl ClusterSignedEndpointRpcAsyncHost + Send),
) -> Result<ObjectRpcResponse, NetworkError> {
    let state_authority_targets =
        ClusterEndpointSelector::select_internal_unique_node_targets_async(
            runtime,
            paths,
            state,
            ClusterEndpointInternalSelectionPurpose::VoterCommand,
        )
        .await?;
    let mut failures = Vec::new();
    let mut client = ClusterObjectRpcClient::routed(host, runtime, paths);
    for target in &state_authority_targets {
        let started = Instant::now();
        membership_forward_trace(format_args!(
            "membership forward routed node {} via {} ({:?}) started",
            target.node_id, target.address, target.transport
        ));
        match client
            .call_target(
                state,
                &target.node_id,
                request,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                    DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT,
                )),
            )
            .await
        {
            Ok(response) => {
                membership_forward_trace(format_args!(
                    "membership forward routed node {} via {} ({:?}) ok in {:?}",
                    target.node_id,
                    target.address,
                    target.transport,
                    started.elapsed()
                ));
                return Ok(response);
            }
            Err(error) => {
                membership_forward_trace(format_args!(
                    "membership forward routed node {} via {} ({:?}) failed in {:?}: {error}",
                    target.node_id,
                    target.address,
                    target.transport,
                    started.elapsed()
                ));
                failures.push(format!(
                    "routed node {} via {} ({:?}): {error}",
                    target.node_id, target.address, target.transport
                ));
            }
        }
    }
    {
        let mut direct_client = ClusterObjectRpcClient::endpoint_only(host);
        for target in &state_authority_targets {
            let started = Instant::now();
            membership_forward_trace(format_args!(
                "membership forward direct node {} via {} ({:?}) started",
                target.node_id, target.address, target.transport
            ));
            match direct_client
                .call_endpoint(
                    target,
                    request,
                    ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(
                        DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT,
                    )),
                )
                .await
            {
                Ok(response) => {
                    membership_forward_trace(format_args!(
                        "membership forward direct node {} via {} ({:?}) ok in {:?}",
                        target.node_id,
                        target.address,
                        target.transport,
                        started.elapsed()
                    ));
                    return Ok(response);
                }
                Err(error) => {
                    membership_forward_trace(format_args!(
                        "membership forward direct node {} via {} ({:?}) failed in {:?}: {error}",
                        target.node_id,
                        target.address,
                        target.transport,
                        started.elapsed()
                    ));
                    failures.push(format!(
                        "direct node {} via {} ({:?}): {error}",
                        target.node_id, target.address, target.transport
                    ));
                }
            }
        }
    }
    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "{failure_detail}{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

/// A bridge introducer named in an invite minted before it joined is not itself a
/// proof-capable channel/issuer, so it cannot answer a join challenge. Relay the challenge to
/// a reachable cluster state authority (the invite issuer can produce an issuer-signed proof).
/// The routing-aware send routes the challenge across segments when no authority is directly
/// reachable.
#[cfg(any(test, feature = "test-support"))]
pub fn forward_cluster_join_challenge_rpc_to_state_authority_with_host(
    paths: &ClusterPaths,
    request: &ObjectRpcRequest,
    host: &mut impl ClusterMembershipRpcForwardHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let state = read_cluster_state(paths)?;
    forward_cluster_membership_rpc_to_state_authority_with_host(
        paths,
        &state,
        request,
        "join challenge introducer is not a channel/issuer and could not relay the challenge to any state authority",
        host,
    )
}

#[cfg(any(test, feature = "test-support"))]
pub async fn forward_cluster_join_challenge_rpc_to_state_authority_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: &ObjectRpcRequest,
    host: &mut (impl ClusterSignedEndpointRpcAsyncHost + Send),
) -> Result<ObjectRpcResponse, NetworkError> {
    let state_paths = paths.clone();
    let state = runtime
        .spawn_blocking(move || read_cluster_state(&state_paths))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "read cluster state".to_string(),
            detail: format!("cluster state read task failed: {source}"),
        })??;
    forward_cluster_membership_rpc_to_state_authority_async_with_host(
        runtime,
        paths,
        &state,
        request,
        "join challenge introducer is not a channel/issuer and could not relay the challenge to any state authority",
        host,
    )
    .await
}
