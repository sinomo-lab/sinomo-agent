use crate::ClusterPaths;
use crate::endpoint_selector::{
    ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector,
    cluster_node_blocks_leader_preference, cluster_node_blocks_leader_preference_async,
};
use crate::error::NetworkError;
use crate::kv_local_store::locked_cluster_raft_known_leader_id_async;
use crate::object::DaemonRpcTimeouts;
use crate::route::{
    ClusterObjectRpcClient, ClusterObjectRpcNodeOptions, ClusterSignedEndpointRpcAsyncHost,
};
use crate::rpc::{ObjectRpcRequest, ObjectRpcResponse};
use sinomo_core::{ClusterBootstrapState, ClusterEndpointRecord, ClusterTransportKind};
use sinomo_kv::{ClusterKvTxnCommitResult, KvTxn};
use sinomo_node::NodeId;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClusterKvTxnForwardOptions {
    pub(crate) timeout: Duration,
    pub(crate) tcp_command_timeout: Duration,
    pub(crate) iroh_command_timeout: Duration,
}

impl ClusterKvTxnForwardOptions {
    pub(crate) fn command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        match endpoint.transport {
            ClusterTransportKind::Iroh => remaining.min(self.iroh_command_timeout),
            _ => remaining.min(self.tcp_command_timeout),
        }
    }
}

pub(crate) trait ClusterKvTxnForwardHost {
    fn known_kv_state_authority_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError>;

    fn send_object_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>;
}

pub(crate) fn forward_cluster_kv_txn_to_state_authority_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    txn: &KvTxn,
    options: ClusterKvTxnForwardOptions,
    host: &mut impl ClusterKvTxnForwardHost,
) -> Result<Option<ClusterKvTxnCommitResult>, NetworkError> {
    let mut targets = ClusterEndpointSelector::new(paths, state)
        .select_internal_unique_node_targets(
            ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
        );
    if targets.is_empty() {
        return Ok(None);
    }
    if let Ok(Some(authority_id)) = host.known_kv_state_authority_id(paths, state)
        && !cluster_node_blocks_leader_preference(paths, state, &authority_id)
        && let Some(position) = targets
            .iter()
            .position(|endpoint| endpoint.node_id == authority_id)
    {
        let authority = targets.remove(position);
        targets.insert(0, authority);
    }

    let mut failures = Vec::new();
    let deadline = Instant::now() + options.timeout;
    for target in &targets {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            failures.push(format!(
                "node forwarding did not complete before {}ms command budget",
                options.timeout.as_millis()
            ));
            break;
        }
        match forward_cluster_kv_txn_to_state_authority_node_with_host(
            state,
            &target.node_id,
            txn,
            options.command_timeout(target, remaining),
            host,
        ) {
            Ok(response) => return Ok(Some(response)),
            Err(error) => failures.push(format!("node {} failed: {error}", target.node_id)),
        }
    }

    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "could not forward cluster KV transaction to any known state authority{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

fn forward_cluster_kv_txn_to_state_authority_node_with_host(
    state: &ClusterBootstrapState,
    authority_node_id: &NodeId,
    txn: &KvTxn,
    timeout: Duration,
    host: &mut impl ClusterKvTxnForwardHost,
) -> Result<ClusterKvTxnCommitResult, NetworkError> {
    let started = Instant::now();
    let txn_payload = sinomo_kv::cluster_kv_txn_rpc_payload(txn)?;
    let txn_request = ObjectRpcRequest::from_encoded_body(
        txn_payload.route,
        txn_payload.category,
        txn_payload.body,
    );
    let txn_response =
        host.send_object_rpc_to_node(state, authority_node_id, &txn_request, timeout)?;
    txn_response.error_detail()?;
    let txn_response = sinomo_kv::decode_cluster_kv_txn_rpc_body(&txn_response.body)?;

    let snapshot_timeout = timeout
        .saturating_sub(started.elapsed())
        .max(Duration::from_millis(1));
    let snapshot_payload = sinomo_kv::cluster_kv_snapshot_rpc_payload()?;
    let snapshot_request = ObjectRpcRequest::from_encoded_body(
        snapshot_payload.route,
        snapshot_payload.category,
        snapshot_payload.body,
    );
    let snapshot_response = host.send_object_rpc_to_node(
        state,
        authority_node_id,
        &snapshot_request,
        snapshot_timeout,
    )?;
    snapshot_response.error_detail()?;
    let snapshot = sinomo_kv::decode_cluster_kv_snapshot_rpc_body(&snapshot_response.body)?;
    Ok(ClusterKvTxnCommitResult {
        snapshot,
        txn_response: Some(txn_response),
    })
}

pub(crate) async fn forward_cluster_kv_txn_to_state_authority_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    txn: &KvTxn,
    options: ClusterKvTxnForwardOptions,
    host: &mut H,
) -> Result<Option<ClusterKvTxnCommitResult>, NetworkError>
where
    H: ClusterSignedEndpointRpcAsyncHost + Send,
{
    let mut targets = ClusterEndpointSelector::select_internal_unique_node_targets_async(
        runtime,
        paths,
        state,
        ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
    )
    .await?;
    if targets.is_empty() {
        return Ok(None);
    }
    if let Ok(Some(authority_id)) =
        locked_cluster_raft_known_leader_id_async(runtime, paths, state).await
        && !cluster_node_blocks_leader_preference_async(runtime, paths, state, &authority_id)
            .await?
        && let Some(position) = targets
            .iter()
            .position(|endpoint| endpoint.node_id == authority_id)
    {
        let authority = targets.remove(position);
        targets.insert(0, authority);
    }

    let mut failures = Vec::new();
    let deadline = Instant::now() + options.timeout;
    for target in &targets {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            failures.push(format!(
                "node forwarding did not complete before {}ms command budget",
                options.timeout.as_millis()
            ));
            break;
        }
        match forward_cluster_kv_txn_to_state_authority_node_async_with_host(
            state,
            &target.node_id,
            txn,
            options.command_timeout(target, remaining),
            runtime,
            paths,
            host,
        )
        .await
        {
            Ok(response) => return Ok(Some(response)),
            Err(error) => failures.push(format!("node {} failed: {error}", target.node_id)),
        }
    }

    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "could not forward cluster KV transaction to any known state authority{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

async fn forward_cluster_kv_txn_to_state_authority_node_async_with_host<H>(
    state: &ClusterBootstrapState,
    authority_node_id: &NodeId,
    txn: &KvTxn,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    host: &mut H,
) -> Result<ClusterKvTxnCommitResult, NetworkError>
where
    H: ClusterSignedEndpointRpcAsyncHost + Send,
{
    let started = Instant::now();
    let txn_payload = sinomo_kv::cluster_kv_txn_rpc_payload(txn)?;
    let txn_request = ObjectRpcRequest::from_encoded_body(
        txn_payload.route,
        txn_payload.category,
        txn_payload.body,
    );
    let mut client = ClusterObjectRpcClient::routed(host, runtime, paths);
    let txn_response = client
        .call_node(
            state,
            authority_node_id,
            &txn_request,
            ClusterObjectRpcNodeOptions::internal(
                ClusterEndpointInternalSelectionPurpose::VoterCommand,
                DaemonRpcTimeouts::connect_and_idle(timeout),
            ),
        )
        .await?;
    txn_response.error_detail()?;
    let txn_response = sinomo_kv::decode_cluster_kv_txn_rpc_body(&txn_response.body)?;

    let snapshot_timeout = timeout
        .saturating_sub(started.elapsed())
        .max(Duration::from_millis(1));
    let snapshot_payload = sinomo_kv::cluster_kv_snapshot_rpc_payload()?;
    let snapshot_request = ObjectRpcRequest::from_encoded_body(
        snapshot_payload.route,
        snapshot_payload.category,
        snapshot_payload.body,
    );
    let snapshot_response = client
        .call_node(
            state,
            authority_node_id,
            &snapshot_request,
            ClusterObjectRpcNodeOptions::internal(
                ClusterEndpointInternalSelectionPurpose::VoterCommand,
                DaemonRpcTimeouts::connect_and_idle(snapshot_timeout),
            ),
        )
        .await?;
    snapshot_response.error_detail()?;
    let snapshot = sinomo_kv::decode_cluster_kv_snapshot_rpc_body(&snapshot_response.body)?;
    Ok(ClusterKvTxnCommitResult {
        snapshot,
        txn_response: Some(txn_response),
    })
}
