use crate::ClusterPaths;
use crate::control_plane_options::DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT;
use crate::endpoint_selector::{ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector};
use crate::error::NetworkError;
#[cfg(test)]
use crate::local_state_store::read_cluster_state;
use crate::object::DaemonRpcTimeouts;
use crate::route::{
    ClusterObjectRpcClient, ClusterObjectRpcNodeOptions, ClusterSignedEndpointRpcAsyncHost,
};
use crate::rpc::{ObjectRpcRequest, ObjectRpcResponse};
use sinomo_core::ClusterBootstrapState;
use sinomo_node::NodeId;

pub(crate) trait ClusterMembershipRpcForwardHost {
    fn send_membership_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError>;
}

pub(crate) fn forward_cluster_membership_rpc_to_voter_with_host(
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
        match host.send_membership_rpc_to_node(state, &target_node_id, request) {
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

pub async fn forward_cluster_membership_rpc_to_voter_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request: &ObjectRpcRequest,
    failure_detail: &str,
    host: &mut (impl ClusterSignedEndpointRpcAsyncHost + Send),
) -> Result<ObjectRpcResponse, NetworkError> {
    let voter_targets = ClusterEndpointSelector::select_internal_unique_node_targets_async(
        runtime,
        paths,
        state,
        ClusterEndpointInternalSelectionPurpose::VoterCommand,
    )
    .await?;
    let mut failures = Vec::new();
    let mut client = ClusterObjectRpcClient::routed(host, runtime, paths);
    for target in &voter_targets {
        match client
            .call_node(
                state,
                &target.node_id,
                request,
                ClusterObjectRpcNodeOptions::internal(
                    ClusterEndpointInternalSelectionPurpose::VoterCommand,
                    DaemonRpcTimeouts::connect_and_idle(
                        DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT,
                    ),
                ),
            )
            .await
        {
            Ok(response) => return Ok(response),
            Err(error) => {
                failures.push(format!("node {}: {error}", target.node_id));
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
/// a reachable voter (the invite issuer can produce an issuer-signed proof). The routing-aware
/// send routes the challenge across segments when no voter is directly reachable.
#[cfg(test)]
pub(crate) fn forward_cluster_join_challenge_rpc_to_voter_with_host(
    paths: &ClusterPaths,
    request: &ObjectRpcRequest,
    host: &mut impl ClusterMembershipRpcForwardHost,
) -> Result<ObjectRpcResponse, NetworkError> {
    let state = read_cluster_state(paths)?;
    forward_cluster_membership_rpc_to_voter_with_host(
        paths,
        &state,
        request,
        "join challenge introducer is not a channel/issuer and could not relay the challenge to any voter",
        host,
    )
}

#[cfg(test)]
pub(crate) async fn forward_cluster_join_challenge_rpc_to_voter_async_with_host(
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
    forward_cluster_membership_rpc_to_voter_async_with_host(
        runtime,
        paths,
        &state,
        request,
        "join challenge introducer is not a channel/issuer and could not relay the challenge to any voter",
        host,
    )
    .await
}
