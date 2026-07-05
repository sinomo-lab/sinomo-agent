#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
use crate::kv_local_store::locked_kv_state_authority_id_async;
use sinomo_base::{ClusterBootstrapState, ClusterEndpointRecord, ClusterTransportKind};
use sinomo_kv::{ClusterKvTxnCommitResult, KvTxn};
use sinomo_network::ClusterPaths;
use sinomo_network::DaemonRpcTimeouts;
use sinomo_network::NetworkError;
use sinomo_network::{
    ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector,
    cluster_node_blocks_leader_preference, cluster_node_blocks_leader_preference_async,
};
use sinomo_network::{
    ClusterObjectRpcClient, ClusterObjectRpcOptions, ClusterSignedEndpointRpcAsyncHost,
};
use sinomo_network::{ObjectRpcRequest, ObjectRpcResponse};
use sinomo_node::NodeId;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvTxnForwardOptions {
    pub timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
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

    pub(crate) fn attempt_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
        remaining_targets: usize,
    ) -> Duration {
        let target_count = remaining_targets.max(1).min(u32::MAX as usize) as u32;
        let fair_share = remaining
            .checked_div(target_count)
            .unwrap_or(remaining)
            .max(Duration::from_millis(1));
        self.command_timeout(endpoint, remaining).min(fair_share)
    }
}

pub trait ClusterKvTxnForwardHost {
    fn known_kv_state_authority_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError>;

    fn call_endpoint_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.call_node_object_rpc(state, &endpoint.node_id, request, timeout)
    }

    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>;
}

pub fn forward_cluster_kv_txn_to_state_authority_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    txn: &KvTxn,
    options: ClusterKvTxnForwardOptions,
    host: &mut impl ClusterKvTxnForwardHost,
) -> Result<Option<ClusterKvTxnCommitResult>, NetworkError> {
    let mut targets = ClusterEndpointSelector::new(paths, state)
        .select_internal(ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand);
    if targets.is_empty() {
        return Ok(None);
    }
    let known_authority_id = host
        .known_kv_state_authority_id(paths, state)
        .ok()
        .flatten();
    match known_authority_id {
        Some(authority_id)
            if !cluster_node_blocks_leader_preference(paths, state, &authority_id) =>
        {
            if authority_id == state.local_node_id {
                return Ok(None);
            }
            prefer_known_state_authority_endpoints(&mut targets, &authority_id);
        }
        _ if sinomo_kv::cluster_kv_raft_local_is_voter(state) => {
            return Ok(None);
        }
        _ => {}
    }

    let mut failures = Vec::new();
    let deadline = Instant::now() + options.timeout;
    for (index, target) in targets.iter().enumerate() {
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
        match forward_cluster_kv_txn_to_state_authority_endpoint_with_host(
            state,
            target,
            txn,
            options.attempt_timeout(target, remaining, targets.len() - index),
            host,
        ) {
            Ok(response) => return Ok(Some(response)),
            Err(error) => failures.push(format!(
                "node {} endpoint {} failed: {error}",
                target.node_id, target.address
            )),
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

fn forward_cluster_kv_txn_to_state_authority_endpoint_with_host(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    txn: &KvTxn,
    timeout: Duration,
    host: &mut impl ClusterKvTxnForwardHost,
) -> Result<ClusterKvTxnCommitResult, NetworkError> {
    let started = Instant::now();
    let txn_payload = sinomo_kv::cluster_kv_txn_rpc_payload(txn).map_err(kv_err)?;
    let txn_request = ObjectRpcRequest::from_encoded_body(
        txn_payload.route,
        txn_payload.category,
        txn_payload.body,
    );
    let txn_response = host.call_endpoint_object_rpc(state, endpoint, &txn_request, timeout)?;
    txn_response.error_detail()?;
    let txn_response =
        sinomo_kv::decode_cluster_kv_txn_rpc_body(&txn_response.body).map_err(kv_err)?;

    let snapshot_timeout = timeout
        .saturating_sub(started.elapsed())
        .max(Duration::from_millis(1));
    let snapshot_payload = sinomo_kv::cluster_kv_snapshot_rpc_payload().map_err(kv_err)?;
    let snapshot_request = ObjectRpcRequest::from_encoded_body(
        snapshot_payload.route,
        snapshot_payload.category,
        snapshot_payload.body,
    );
    let snapshot_response =
        host.call_endpoint_object_rpc(state, endpoint, &snapshot_request, snapshot_timeout)?;
    snapshot_response.error_detail()?;
    let snapshot =
        sinomo_kv::decode_cluster_kv_snapshot_rpc_body(&snapshot_response.body).map_err(kv_err)?;
    Ok(ClusterKvTxnCommitResult {
        snapshot,
        txn_response: Some(txn_response),
    })
}

pub async fn forward_cluster_kv_txn_to_state_authority_async_with_host<H>(
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
    let mut targets = ClusterEndpointSelector::select_internal_async(
        runtime,
        paths,
        state,
        ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
    )
    .await?;
    if targets.is_empty() {
        return Ok(None);
    }
    let known_authority_id = locked_kv_state_authority_id_async(runtime, paths, state)
        .await
        .ok()
        .flatten();
    match known_authority_id {
        Some(authority_id)
            if !cluster_node_blocks_leader_preference_async(
                runtime,
                paths,
                state,
                &authority_id,
            )
            .await? =>
        {
            if authority_id == state.local_node_id {
                return Ok(None);
            }
            prefer_known_state_authority_endpoints(&mut targets, &authority_id);
        }
        _ if sinomo_kv::cluster_kv_raft_local_is_voter(state) => {
            return Ok(None);
        }
        _ => {}
    }

    let mut failures = Vec::new();
    let deadline = Instant::now() + options.timeout;
    for (index, target) in targets.iter().enumerate() {
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
        match forward_cluster_kv_txn_to_state_authority_endpoint_async_with_host(
            target,
            txn,
            options.attempt_timeout(target, remaining, targets.len() - index),
            host,
        )
        .await
        {
            Ok(response) => return Ok(Some(response)),
            Err(error) => failures.push(format!(
                "node {} endpoint {} failed: {error}",
                target.node_id, target.address
            )),
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

fn prefer_known_state_authority_endpoints(
    targets: &mut Vec<ClusterEndpointRecord>,
    authority_id: &NodeId,
) {
    if !targets
        .iter()
        .any(|endpoint| endpoint.node_id == *authority_id)
    {
        return;
    }
    let mut preferred = Vec::new();
    let mut other = Vec::new();
    for target in targets.drain(..) {
        if target.node_id == *authority_id {
            preferred.push(target);
        } else {
            other.push(target);
        }
    }
    preferred.append(&mut other);
    *targets = preferred;
}

async fn forward_cluster_kv_txn_to_state_authority_endpoint_async_with_host<H>(
    endpoint: &ClusterEndpointRecord,
    txn: &KvTxn,
    timeout: Duration,
    host: &mut H,
) -> Result<ClusterKvTxnCommitResult, NetworkError>
where
    H: ClusterSignedEndpointRpcAsyncHost + Send,
{
    let started = Instant::now();
    let txn_payload = sinomo_kv::cluster_kv_txn_rpc_payload(txn).map_err(kv_err)?;
    let txn_request = ObjectRpcRequest::from_encoded_body(
        txn_payload.route,
        txn_payload.category,
        txn_payload.body,
    );
    let mut client = ClusterObjectRpcClient::endpoint_only(host);
    let txn_response = client
        .call_endpoint(
            endpoint,
            &txn_request,
            ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(timeout)),
        )
        .await?;
    txn_response.error_detail()?;
    let txn_response =
        sinomo_kv::decode_cluster_kv_txn_rpc_body(&txn_response.body).map_err(kv_err)?;

    let snapshot_timeout = timeout
        .saturating_sub(started.elapsed())
        .max(Duration::from_millis(1));
    let snapshot_payload = sinomo_kv::cluster_kv_snapshot_rpc_payload().map_err(kv_err)?;
    let snapshot_request = ObjectRpcRequest::from_encoded_body(
        snapshot_payload.route,
        snapshot_payload.category,
        snapshot_payload.body,
    );
    let snapshot_response = client
        .call_endpoint(
            endpoint,
            &snapshot_request,
            ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(snapshot_timeout)),
        )
        .await?;
    snapshot_response.error_detail()?;
    let snapshot =
        sinomo_kv::decode_cluster_kv_snapshot_rpc_body(&snapshot_response.body).map_err(kv_err)?;
    Ok(ClusterKvTxnCommitResult {
        snapshot,
        txn_response: Some(txn_response),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::{CapabilityName, ClusterMemberRecord, ClusterMemberState};
    use sinomo_node::{ClusterId, DAEMON_MAIN_RUN_CAPABILITY};
    use time::OffsetDateTime;

    struct RecordingForwardHost {
        known_authority: Option<NodeId>,
        calls: usize,
    }

    impl ClusterKvTxnForwardHost for RecordingForwardHost {
        fn known_kv_state_authority_id(
            &mut self,
            _paths: &ClusterPaths,
            _state: &ClusterBootstrapState,
        ) -> Result<Option<NodeId>, NetworkError> {
            Ok(self.known_authority.clone())
        }

        fn call_node_object_rpc(
            &mut self,
            _state: &ClusterBootstrapState,
            _target_node_id: &NodeId,
            _request: &ObjectRpcRequest,
            _timeout: Duration,
        ) -> Result<ObjectRpcResponse, NetworkError> {
            self.calls = self.calls.saturating_add(1);
            Err(NetworkError::Io {
                operation: "test forward".to_string(),
                detail: "forward attempted".to_string(),
            })
        }
    }

    fn forward_test_state(
        local_is_voter: bool,
    ) -> (sinomo_testdir::TestDir, ClusterBootstrapState) {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-kv-forward");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_kv_forward").expect("cluster id");
        let local = NodeId::from_legacy_named("node_kv_forward_local").expect("local node");
        let remote = NodeId::from_legacy_named("node_kv_forward_remote").expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "kv forward",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        if !local_is_voter {
            state.members[0] = ClusterMemberRecord::local_non_voting(
                cluster_id.clone(),
                local,
                "local",
                "pk-local",
                now,
            );
        }
        state.members[0].capabilities =
            vec![CapabilityName::new(DAEMON_MAIN_RUN_CAPABILITY).expect("main daemon capability")];
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        let mut remote_member = ClusterMemberRecord::local_voter(
            cluster_id,
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        );
        remote_member.capabilities =
            vec![CapabilityName::new(DAEMON_MAIN_RUN_CAPABILITY).expect("main daemon capability")];
        remote_member.allowed_capabilities = remote_member.capabilities.clone();
        remote_member.endpoints = vec![sinomo_network::daemon_lan_endpoint(
            &remote,
            "10.0.0.2:21353".to_string(),
            0,
        )];
        state.members.push(remote_member);
        (root, state)
    }

    fn test_txn() -> KvTxn {
        KvTxn {
            compares: Vec::new(),
            success: Vec::new(),
            failure: Vec::new(),
        }
    }

    fn forward_options() -> ClusterKvTxnForwardOptions {
        ClusterKvTxnForwardOptions {
            timeout: Duration::from_secs(1),
            tcp_command_timeout: Duration::from_secs(1),
            iroh_command_timeout: Duration::from_secs(1),
        }
    }

    #[test]
    fn voter_without_known_authority_does_not_forward_to_arbitrary_voter() {
        let (root, state) = forward_test_state(true);
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let mut host = RecordingForwardHost {
            known_authority: None,
            calls: 0,
        };

        let forwarded = forward_cluster_kv_txn_to_state_authority_with_host(
            &paths,
            &state,
            &test_txn(),
            forward_options(),
            &mut host,
        )
        .expect("forward decision");

        assert!(forwarded.is_none());
        assert_eq!(host.calls, 0);
    }

    #[test]
    fn non_voter_without_known_authority_can_forward_to_voter() {
        let (root, mut state) = forward_test_state(false);
        state.members[0].state = ClusterMemberState::NonVoting;
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let mut host = RecordingForwardHost {
            known_authority: None,
            calls: 0,
        };

        let forwarded = forward_cluster_kv_txn_to_state_authority_with_host(
            &paths,
            &state,
            &test_txn(),
            forward_options(),
            &mut host,
        );

        assert!(matches!(
            forwarded,
            Err(NetworkError::ClusterReadOnly { .. })
        ));
        assert_eq!(host.calls, 1);
    }
}
