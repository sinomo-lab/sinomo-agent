#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
use crate::kv_local_store::locked_kv_state_authority_id_async;
use sinomo_base::ClusterBootstrapState;
use sinomo_kv::{ClusterKvTxnCommitResult, KvAuthoritativeRead, KvSnapshot, KvTxnRequest};
use sinomo_network::ClusterPaths;
use sinomo_network::{
    ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector,
    cluster_node_blocks_leader_preference, cluster_node_blocks_leader_preference_async,
};
use sinomo_network::{
    ClusterObjectRpcClient, ClusterObjectRpcOptions, ClusterSignedObjectRpcAsyncHost,
};
use sinomo_network::{NetworkDeliveryState, NetworkError};
use sinomo_network::{ObjectRpcRequest, ObjectRpcResponse, ObjectRpcSessionCall};
use sinomo_node::NodeId;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvTxnForwardOptions {
    pub timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
}

impl ClusterKvTxnForwardOptions {
    pub(crate) fn routed_node_command_timeout(&self, remaining: Duration) -> Duration {
        remaining.min(self.tcp_command_timeout.max(self.iroh_command_timeout))
    }

    pub(crate) fn routed_node_attempt_timeout(
        &self,
        remaining: Duration,
        remaining_targets: usize,
    ) -> Duration {
        let target_count = remaining_targets.max(1).min(u32::MAX as usize) as u32;
        let fair_share = remaining
            .checked_div(target_count)
            .unwrap_or(remaining)
            .max(Duration::from_millis(1));
        self.routed_node_command_timeout(remaining).min(fair_share)
    }
}

pub trait ClusterKvTxnForwardHost {
    fn known_kv_state_authority_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError>;

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
    request: &KvTxnRequest,
    options: ClusterKvTxnForwardOptions,
    host: &mut impl ClusterKvTxnForwardHost,
) -> Result<Option<ClusterKvTxnCommitResult>, NetworkError> {
    let mut targets = ClusterEndpointSelector::new(paths, state).select_internal_unique_node_ids(
        ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
    );
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
            prefer_known_state_authority_node(&mut targets, &authority_id);
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
        match forward_cluster_kv_txn_to_state_authority_node_with_host(
            state,
            target,
            request,
            options.routed_node_attempt_timeout(remaining, targets.len() - index),
            host,
        ) {
            Ok(response) => return Ok(Some(response)),
            Err(error) => {
                failures.push(format!("node {target} failed: {error}"));
                if !cluster_kv_forward_error_allows_next_authority(&error) {
                    return Err(error);
                }
            }
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

pub fn forward_cluster_kv_authoritative_snapshot_to_state_authority_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvTxnForwardOptions,
    host: &mut impl ClusterKvTxnForwardHost,
) -> Result<Option<KvAuthoritativeRead<KvSnapshot>>, NetworkError> {
    let mut targets = ClusterEndpointSelector::new(paths, state).select_internal_unique_node_ids(
        ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
    );
    if targets.is_empty() {
        return Ok(None);
    }
    match host
        .known_kv_state_authority_id(paths, state)
        .ok()
        .flatten()
    {
        Some(authority_id)
            if !cluster_node_blocks_leader_preference(paths, state, &authority_id) =>
        {
            if authority_id == state.local_node_id {
                return Ok(None);
            }
            prefer_known_state_authority_node(&mut targets, &authority_id);
        }
        _ if sinomo_kv::cluster_kv_raft_local_is_voter(state) => return Ok(None),
        _ => {}
    }

    let deadline = Instant::now() + options.timeout;
    let mut failures = Vec::new();
    for (index, target) in targets.iter().enumerate() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        let payload = sinomo_kv::cluster_kv_authoritative_snapshot_rpc_payload().map_err(kv_err)?;
        let request =
            ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body)
                .with_response_retry_safe();
        let timeout = options.routed_node_attempt_timeout(remaining, targets.len() - index);
        match host.call_node_object_rpc(state, target, &request, timeout) {
            Ok(response) => {
                response.error_detail()?;
                let read =
                    sinomo_kv::decode_cluster_kv_authoritative_snapshot_rpc_body(&response.body)
                        .map_err(kv_err)?;
                return Ok(Some(read));
            }
            Err(error) => failures.push(format!("node {target} failed: {error}")),
        }
    }
    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "could not establish an authoritative cluster KV read{}",
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
    target_node_id: &NodeId,
    request: &KvTxnRequest,
    timeout: Duration,
    host: &mut impl ClusterKvTxnForwardHost,
) -> Result<ClusterKvTxnCommitResult, NetworkError> {
    let txn_payload = sinomo_kv::cluster_kv_txn_rpc_payload(request).map_err(kv_err)?;
    let txn_request = ObjectRpcRequest::from_encoded_body(
        txn_payload.route,
        txn_payload.category,
        txn_payload.body,
    );
    let txn_response = host.call_node_object_rpc(state, target_node_id, &txn_request, timeout)?;
    txn_response.error_detail()?;
    let response = sinomo_kv::decode_cluster_kv_txn_rpc_body(&txn_response.body).map_err(kv_err)?;
    validate_forwarded_cluster_kv_txn_response(request, response)
}

pub async fn forward_cluster_kv_txn_to_state_authority_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request: &KvTxnRequest,
    options: ClusterKvTxnForwardOptions,
    host: &mut H,
) -> Result<Option<ClusterKvTxnCommitResult>, NetworkError>
where
    H: ClusterSignedObjectRpcAsyncHost + Send,
{
    let mut targets = ClusterEndpointSelector::select_internal_unique_node_ids_async(
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
            prefer_known_state_authority_node(&mut targets, &authority_id);
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
        match forward_cluster_kv_txn_to_state_authority_node_async_with_host(
            runtime,
            paths,
            state,
            target,
            request,
            options.routed_node_attempt_timeout(remaining, targets.len() - index),
            host,
        )
        .await
        {
            Ok(response) => return Ok(Some(response)),
            Err(error) => {
                failures.push(format!("node {target} failed: {error}"));
                if !cluster_kv_forward_error_allows_next_authority(&error) {
                    return Err(error);
                }
            }
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

pub async fn forward_cluster_kv_authoritative_snapshot_to_state_authority_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvTxnForwardOptions,
    host: &mut H,
) -> Result<Option<KvAuthoritativeRead<KvSnapshot>>, NetworkError>
where
    H: ClusterSignedObjectRpcAsyncHost + Send,
{
    let mut targets = ClusterEndpointSelector::select_internal_unique_node_ids_async(
        runtime,
        paths,
        state,
        ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
    )
    .await?;
    if targets.is_empty() {
        return Ok(None);
    }
    match locked_kv_state_authority_id_async(runtime, paths, state)
        .await
        .ok()
        .flatten()
    {
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
            prefer_known_state_authority_node(&mut targets, &authority_id);
        }
        _ if sinomo_kv::cluster_kv_raft_local_is_voter(state) => return Ok(None),
        _ => {}
    }

    let deadline = Instant::now() + options.timeout;
    let mut failures = Vec::new();
    for (index, target) in targets.iter().enumerate() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        let payload = sinomo_kv::cluster_kv_authoritative_snapshot_rpc_payload().map_err(kv_err)?;
        let request =
            ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body)
                .with_response_retry_safe();
        let timeout = options.routed_node_attempt_timeout(remaining, targets.len() - index);
        let mut client = ClusterObjectRpcClient::routed(host, runtime, paths);
        match client
            .call_target(
                state,
                target,
                ObjectRpcSessionCall::value(request),
                ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
            )
            .await
        {
            Ok(response) if response.body.is_empty() => {
                response.response.error_detail()?;
                let read = sinomo_kv::decode_cluster_kv_authoritative_snapshot_rpc_body(
                    &response.response.body,
                )
                .map_err(kv_err)?;
                return Ok(Some(read));
            }
            Ok(_) => failures.push(format!(
                "node {target} returned an unexpected authoritative-read attachment"
            )),
            Err(error) => failures.push(format!("node {target} failed: {error}")),
        }
    }
    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "could not establish an authoritative cluster KV read{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

fn prefer_known_state_authority_node(targets: &mut Vec<NodeId>, authority_id: &NodeId) {
    if !targets.iter().any(|node_id| node_id == authority_id) {
        return;
    }
    let mut preferred = Vec::new();
    let mut other = Vec::new();
    for target in targets.drain(..) {
        if target == *authority_id {
            preferred.push(target);
        } else {
            other.push(target);
        }
    }
    preferred.append(&mut other);
    *targets = preferred;
}

fn cluster_kv_forward_error_allows_next_authority(error: &NetworkError) -> bool {
    error.delivery_state() == Some(NetworkDeliveryState::DefinitelyNotSent)
}

async fn forward_cluster_kv_txn_to_state_authority_node_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    request: &KvTxnRequest,
    timeout: Duration,
    host: &mut H,
) -> Result<ClusterKvTxnCommitResult, NetworkError>
where
    H: ClusterSignedObjectRpcAsyncHost + Send,
{
    let txn_payload = sinomo_kv::cluster_kv_txn_rpc_payload(request).map_err(kv_err)?;
    let txn_request = ObjectRpcRequest::from_encoded_body(
        txn_payload.route,
        txn_payload.category,
        txn_payload.body,
    );
    let mut client = ClusterObjectRpcClient::routed(host, runtime, paths);
    let txn_response = client
        .call_target(
            state,
            target_node_id,
            ObjectRpcSessionCall::value(txn_request),
            ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
        )
        .await?;
    if !txn_response.body.is_empty() {
        return Err(NetworkError::Serialization {
            detail: "cluster KV transaction returned an unexpected attachment".to_string(),
        });
    }
    let txn_response = txn_response.response;
    txn_response.error_detail()?;
    let response = sinomo_kv::decode_cluster_kv_txn_rpc_body(&txn_response.body).map_err(kv_err)?;
    validate_forwarded_cluster_kv_txn_response(request, response)
}

fn validate_forwarded_cluster_kv_txn_response(
    request: &KvTxnRequest,
    response: ClusterKvTxnCommitResult,
) -> Result<ClusterKvTxnCommitResult, NetworkError> {
    if response.request_id != request.request_id {
        return Err(NetworkError::Serialization {
            detail: format!(
                "forwarded cluster KV response request ID {} does not match submitted request ID {}",
                response.request_id, request.request_id
            ),
        });
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::{CapabilityName, ClusterMemberRecord, ClusterMemberState};
    use sinomo_kv::{KvAuthorityBasis, KvAuthorityProof, KvRevision, KvTxn, KvTxnResponse};
    use sinomo_node::{ClusterId, DAEMON_MAIN_RUN_CAPABILITY};
    use time::OffsetDateTime;

    #[test]
    fn kv_forward_does_not_try_another_authority_after_ambiguous_delivery() {
        let ambiguous = NetworkError::outcome_unknown(
            "cluster KV transaction",
            None,
            "response was not observed",
        );
        let not_sent = NetworkError::delivery_failed(
            "cluster KV transaction",
            NetworkDeliveryState::DefinitelyNotSent,
            "connect failed",
        );

        assert!(!cluster_kv_forward_error_allows_next_authority(&ambiguous));
        assert!(cluster_kv_forward_error_allows_next_authority(&not_sent));
        assert!(!cluster_kv_forward_error_allows_next_authority(
            &NetworkError::Io {
                operation: "legacy KV forward host".to_string(),
                detail: "delivery state unavailable".to_string(),
            }
        ));
        assert!(!cluster_kv_forward_error_allows_next_authority(
            &NetworkError::ObjectRpc {
                route: "/cluster/kv/txn".to_string(),
                detail: "authority returned a definitive application error".to_string(),
            }
        ));
    }

    struct CommittedResponseHost {
        calls: usize,
        mismatch_request_id: bool,
    }

    impl ClusterKvTxnForwardHost for CommittedResponseHost {
        fn known_kv_state_authority_id(
            &mut self,
            _paths: &ClusterPaths,
            _state: &ClusterBootstrapState,
        ) -> Result<Option<NodeId>, NetworkError> {
            Ok(None)
        }

        fn call_node_object_rpc(
            &mut self,
            _state: &ClusterBootstrapState,
            _target_node_id: &NodeId,
            request: &ObjectRpcRequest,
            _timeout: Duration,
        ) -> Result<ObjectRpcResponse, NetworkError> {
            self.calls = self.calls.saturating_add(1);
            if request.route == sinomo_kv::CLUSTER_KV_TXN_ROUTE {
                let submitted = sinomo_kv::decode_cluster_kv_txn_rpc_request_body(&request.body)
                    .expect("transaction request");
                let snapshot = KvSnapshot {
                    revision: KvRevision(1),
                    compacted_revision: KvRevision(0),
                    entries: Vec::new(),
                    leases: Vec::new(),
                    events: Vec::new(),
                    committed_requests: Vec::new(),
                    expired_requests: Vec::new(),
                };
                let payload =
                    sinomo_kv::cluster_kv_txn_response_rpc_payload(&ClusterKvTxnCommitResult {
                        request_id: if self.mismatch_request_id {
                            sinomo_kv::KvRequestId::generate()
                        } else {
                            submitted.request_id
                        },
                        snapshot,
                        txn_response: Some(KvTxnResponse {
                            succeeded: true,
                            revision: KvRevision(1),
                            responses: Vec::new(),
                        }),
                        authority: KvAuthorityProof {
                            term: 1,
                            commit_index: 1,
                            revision: KvRevision(1),
                            basis: KvAuthorityBasis::CurrentTermQuorum {
                                voter_acks: 1,
                                required_quorum: 1,
                            },
                        },
                        replayed_committed_request: false,
                    })
                    .expect("transaction response payload");
                return Ok(ObjectRpcResponse::from_encoded_body(
                    payload.route,
                    payload.body,
                ));
            }
            Err(NetworkError::delivery_failed(
                "fetch snapshot",
                NetworkDeliveryState::DefinitelyNotSent,
                "snapshot endpoint unavailable",
            ))
        }
    }

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
            Err(NetworkError::delivery_failed(
                "test forward",
                NetworkDeliveryState::DefinitelyNotSent,
                "forward attempted",
            ))
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
    fn kv_forward_returns_complete_committed_response_without_second_snapshot_rpc() {
        let (_root, state) = forward_test_state(false);
        let target = state.members[1].node_id.clone();
        let mut host = CommittedResponseHost {
            calls: 0,
            mismatch_request_id: false,
        };
        let request = KvTxnRequest::generated(test_txn());

        let result = forward_cluster_kv_txn_to_state_authority_node_with_host(
            &state,
            &target,
            &request,
            Duration::from_secs(1),
            &mut host,
        )
        .expect("complete committed response");

        assert_eq!(host.calls, 1);
        assert_eq!(result.request_id, request.request_id);
        assert_eq!(result.snapshot.revision, KvRevision(1));
        assert_eq!(result.authority.term, 1);
        assert!(matches!(
            result.authority.basis,
            KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 1,
                required_quorum: 1
            }
        ));
    }

    #[test]
    fn kv_forward_rejects_response_for_a_different_request_id() {
        let (_root, state) = forward_test_state(false);
        let target = state.members[1].node_id.clone();
        let mut host = CommittedResponseHost {
            calls: 0,
            mismatch_request_id: true,
        };
        let request = KvTxnRequest::generated(test_txn());

        let error = forward_cluster_kv_txn_to_state_authority_node_with_host(
            &state,
            &target,
            &request,
            Duration::from_secs(1),
            &mut host,
        )
        .expect_err("mismatched request ID must fail closed");

        assert!(matches!(
            error,
            NetworkError::Serialization { detail }
                if detail.contains("does not match submitted request ID")
        ));
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
            &KvTxnRequest::generated(test_txn()),
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
            &KvTxnRequest::generated(test_txn()),
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
