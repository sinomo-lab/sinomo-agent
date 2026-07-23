#[allow(unused_imports)]
use crate::kv_local_store::{
    kv_err, locked_kv_state_authority_id_async,
    read_cluster_kv_latest_observed_snapshot_arc_if_present,
};
use sinomo_base::ClusterBootstrapState;
use sinomo_kv::{
    ClusterKvTxnCommitResult, ClusterKvTxnRpcResponse, KvAuthoritativeRead, KvAuthorityBasis,
    KvAuthorityProof, KvSnapshot, KvTxnRequest,
};
use sinomo_network::ClusterPaths;
use sinomo_network::{
    ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector,
    cluster_node_blocks_leader_preference, cluster_node_blocks_leader_preference_async,
};
use sinomo_network::{
    ClusterObjectRpcClient, ClusterObjectRpcOptions, ClusterSignedObjectRpcAsyncHost,
};
use sinomo_network::{NetworkDeliveryState, NetworkError};
use sinomo_network::{ObjectRpcCall, ObjectRpcRequest, ObjectRpcResponse};
use sinomo_node::NodeId;
use std::time::{Duration, Instant};

const FORWARDED_CLUSTER_KV_NON_LEADER_DETAIL: &str =
    "forwarded cluster KV transaction reached a non-leader voter";
const FORWARDED_CLUSTER_KV_READ_NON_LEADER_DETAIL: &str =
    "forwarded authoritative cluster KV read reached a non-leader voter";

fn verified_cluster_non_leader_error(target_node_id: &NodeId, error: NetworkError) -> NetworkError {
    let NetworkError::ClusterReadOnly { detail } = error else {
        return error;
    };
    if detail.contains(FORWARDED_CLUSTER_KV_NON_LEADER_DETAIL)
        || detail.contains(FORWARDED_CLUSTER_KV_READ_NON_LEADER_DETAIL)
    {
        NetworkError::VerifiedClusterNonLeader {
            node_id: target_node_id.clone(),
            detail,
        }
    } else {
        NetworkError::ClusterReadOnly { detail }
    }
}

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
    let preferred_authority_id = match known_authority_id {
        Some(authority_id)
            if !cluster_node_blocks_leader_preference(paths, state, &authority_id) =>
        {
            if authority_id == state.local_node_id {
                return Ok(None);
            }
            prefer_known_state_authority_node(&mut targets, &authority_id);
            Some(authority_id)
        }
        _ if sinomo_kv::cluster_kv_raft_local_is_voter(state) => {
            return Ok(None);
        }
        _ => None,
    };

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
            paths,
            state,
            target,
            request,
            options.routed_node_attempt_timeout(remaining, targets.len() - index),
            host,
        ) {
            Ok(response) => return Ok(Some(response)),
            Err(error) => {
                failures.push(format!("node {target} failed: {error}"));
                if verified_non_leader_rejects_preferred_authority(
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                ) || !cluster_kv_forward_error_allows_next_authority(&error, target)
                {
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
    let preferred_authority_id = match host
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
            Some(authority_id)
        }
        _ if sinomo_kv::cluster_kv_raft_local_is_voter(state) => return Ok(None),
        _ => None,
    };

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
                if let Err(error) = response
                    .error_detail()
                    .map_err(|error| verified_cluster_non_leader_error(target, error))
                {
                    failures.push(format!("node {target} failed: {error}"));
                    if verified_non_leader_rejects_preferred_authority(
                        &error,
                        preferred_authority_id.as_ref(),
                        target,
                    ) || !cluster_kv_forward_error_allows_next_authority(&error, target)
                    {
                        return Err(error);
                    }
                    continue;
                }
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
    paths: &ClusterPaths,
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
    txn_response
        .error_detail()
        .map_err(|error| verified_cluster_non_leader_error(target_node_id, error))?;
    let response = sinomo_kv::decode_cluster_kv_txn_rpc_body(&txn_response.body).map_err(kv_err)?;
    materialize_forwarded_cluster_kv_txn_response(
        request,
        response,
        read_cluster_kv_latest_observed_snapshot_arc_if_present(paths)
            .ok()
            .flatten()
            .map(|snapshot| (*snapshot).clone()),
        || fetch_cluster_kv_snapshot_from_node_with_host(state, target_node_id, timeout, host),
    )
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
    let preferred_authority_id = match known_authority_id {
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
            Some(authority_id)
        }
        _ if sinomo_kv::cluster_kv_raft_local_is_voter(state) => {
            return Ok(None);
        }
        _ => None,
    };

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
                if verified_non_leader_rejects_preferred_authority(
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                ) || !cluster_kv_forward_error_allows_next_authority(&error, target)
                {
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
    let preferred_authority_id = match locked_kv_state_authority_id_async(runtime, paths, state)
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
            Some(authority_id)
        }
        _ if sinomo_kv::cluster_kv_raft_local_is_voter(state) => return Ok(None),
        _ => None,
    };

    let deadline = Instant::now() + options.timeout;
    let mut failures = Vec::new();
    for (index, target) in targets.iter().enumerate() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        let mut call = ObjectRpcCall::encode_value(
            sinomo_kv::CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE,
            sinomo_kv::CLUSTER_KV_RPC_CATEGORY,
            sinomo_kv::ClusterKvAuthoritativeSnapshotRequest,
        )?;
        call.request = call.request.with_response_retry_safe();
        let timeout = options.routed_node_attempt_timeout(remaining, targets.len() - index);
        let mut client = ClusterObjectRpcClient::routed(host, paths);
        match client
            .call_target(
                state,
                target,
                call,
                ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
            )
            .await
        {
            Ok(result) => {
                let read = match result
                    .decode_body::<KvAuthoritativeRead<KvSnapshot>>()
                    .await
                {
                    Ok(read) => read,
                    Err(error) => {
                        let error = verified_cluster_non_leader_error(target, error);
                        failures.push(format!("node {target} failed: {error}"));
                        if verified_non_leader_rejects_preferred_authority(
                            &error,
                            preferred_authority_id.as_ref(),
                            target,
                        ) || !cluster_kv_forward_error_allows_next_authority(&error, target)
                        {
                            return Err(error);
                        }
                        continue;
                    }
                };
                sinomo_kv::validate_cluster_kv_authoritative_snapshot_read(&read)
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

fn cluster_kv_forward_error_allows_next_authority(
    error: &NetworkError,
    target_node_id: &NodeId,
) -> bool {
    matches!(
        error,
        NetworkError::VerifiedClusterNonLeader { node_id, .. } if node_id == target_node_id
    ) || error.delivery_state() == Some(NetworkDeliveryState::DefinitelyNotSent)
}

fn verified_non_leader_rejects_preferred_authority(
    error: &NetworkError,
    preferred_authority_id: Option<&NodeId>,
    target_node_id: &NodeId,
) -> bool {
    matches!(
        error,
        NetworkError::VerifiedClusterNonLeader { node_id, .. }
            if node_id == target_node_id && preferred_authority_id == Some(target_node_id)
    )
}

async fn forward_cluster_kv_txn_to_state_authority_node_async_with_host<H>(
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
    let mut client = ClusterObjectRpcClient::routed(host, paths);
    let txn_response = client
        .call_target(
            state,
            target_node_id,
            ObjectRpcCall::encode_borrowed_value(
                sinomo_kv::CLUSTER_KV_TXN_ROUTE,
                sinomo_kv::CLUSTER_KV_RPC_CATEGORY,
                request,
            )?,
            ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
        )
        .await?;
    let response = txn_response
        .decode_body::<ClusterKvTxnRpcResponse>()
        .await
        .map_err(|error| verified_cluster_non_leader_error(target_node_id, error))?;
    if response.request_id != request.request_id {
        return Err(forwarded_cluster_kv_request_id_mismatch(
            request,
            &response.request_id,
        ));
    }
    let local_paths = paths.clone();
    let local_snapshot = tokio::task::spawn_blocking(move || {
        read_cluster_kv_latest_observed_snapshot_arc_if_present(&local_paths)
    })
    .await
    .ok()
    .and_then(Result::ok)
    .flatten()
    .map(|snapshot| (*snapshot).clone());
    if let Some(result) =
        try_materialize_forwarded_cluster_kv_txn_response(request, &response, local_snapshot)?
    {
        return Ok(result);
    }
    let snapshot = fetch_cluster_kv_snapshot_from_node_async_with_host(
        paths,
        state,
        target_node_id,
        timeout,
        host,
    )
    .await?;
    materialize_forwarded_cluster_kv_txn_response_from_snapshot(request, response, snapshot)
}

fn forwarded_cluster_kv_request_id_mismatch(
    request: &KvTxnRequest,
    response_request_id: &sinomo_kv::KvRequestId,
) -> NetworkError {
    NetworkError::Serialization {
        detail: format!(
            "forwarded cluster KV response request ID {response_request_id} does not match submitted request ID {}",
            request.request_id
        ),
    }
}

fn try_materialize_forwarded_cluster_kv_txn_response(
    request: &KvTxnRequest,
    response: &ClusterKvTxnRpcResponse,
    snapshot: Option<KvSnapshot>,
) -> Result<Option<ClusterKvTxnCommitResult>, NetworkError> {
    if response.request_id != request.request_id {
        return Err(forwarded_cluster_kv_request_id_mismatch(
            request,
            &response.request_id,
        ));
    }
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    if matches!(
        response.authority.basis,
        KvAuthorityBasis::CurrentTermQuorum { .. }
    ) && snapshot.revision == response.authority.revision
    {
        return Ok(Some(ClusterKvTxnCommitResult {
            request_id: response.request_id.clone(),
            command_digest: response.command_digest,
            snapshot,
            txn_response: response.txn_response.clone(),
            authority: response.authority.clone(),
            replayed_committed_request: response.replayed_committed_request,
        }));
    }
    let Some(record) = snapshot
        .committed_requests
        .iter()
        .find(|record| record.request_id == request.request_id)
    else {
        return Ok(None);
    };
    let record_term = record.term;
    let record_log_index = record.log_index;
    let record_revision = record.revision;
    let record_txn_response = record.txn_response.clone();
    if record.command_digest != response.command_digest {
        return Err(NetworkError::Serialization {
            detail: format!(
                "forwarded cluster KV response digest does not match replicated request {}",
                request.request_id
            ),
        });
    }
    Ok(Some(ClusterKvTxnCommitResult {
        request_id: response.request_id.clone(),
        command_digest: response.command_digest,
        snapshot,
        txn_response: record_txn_response,
        authority: KvAuthorityProof {
            term: record_term,
            commit_index: record_log_index,
            revision: record_revision,
            basis: KvAuthorityBasis::ExactCommittedRequest,
        },
        replayed_committed_request: true,
    }))
}

fn materialize_forwarded_cluster_kv_txn_response_from_snapshot(
    request: &KvTxnRequest,
    response: ClusterKvTxnRpcResponse,
    snapshot: KvSnapshot,
) -> Result<ClusterKvTxnCommitResult, NetworkError> {
    try_materialize_forwarded_cluster_kv_txn_response(request, &response, Some(snapshot))?
        .ok_or_else(|| NetworkError::Serialization {
            detail: format!(
                "authoritative cluster KV snapshot omitted committed request {}",
                request.request_id
            ),
        })
}

fn materialize_forwarded_cluster_kv_txn_response(
    request: &KvTxnRequest,
    response: ClusterKvTxnRpcResponse,
    local_snapshot: Option<KvSnapshot>,
    fetch_authoritative_snapshot: impl FnOnce() -> Result<KvSnapshot, NetworkError>,
) -> Result<ClusterKvTxnCommitResult, NetworkError> {
    if let Some(result) =
        try_materialize_forwarded_cluster_kv_txn_response(request, &response, local_snapshot)?
    {
        return Ok(result);
    }
    materialize_forwarded_cluster_kv_txn_response_from_snapshot(
        request,
        response,
        fetch_authoritative_snapshot()?,
    )
}

fn fetch_cluster_kv_snapshot_from_node_with_host(
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    timeout: Duration,
    host: &mut impl ClusterKvTxnForwardHost,
) -> Result<KvSnapshot, NetworkError> {
    let payload = sinomo_kv::cluster_kv_authoritative_snapshot_rpc_payload().map_err(kv_err)?;
    let request =
        ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body)
            .with_response_retry_safe();
    let response = host.call_node_object_rpc(state, target_node_id, &request, timeout)?;
    response
        .error_detail()
        .map_err(|error| verified_cluster_non_leader_error(target_node_id, error))?;
    sinomo_kv::decode_cluster_kv_authoritative_snapshot_rpc_body(&response.body)
        .map(|read| read.value)
        .map_err(kv_err)
}

async fn fetch_cluster_kv_snapshot_from_node_async_with_host<H>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    timeout: Duration,
    host: &mut H,
) -> Result<KvSnapshot, NetworkError>
where
    H: ClusterSignedObjectRpcAsyncHost + Send,
{
    let mut call = ObjectRpcCall::encode_value(
        sinomo_kv::CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE,
        sinomo_kv::CLUSTER_KV_RPC_CATEGORY,
        sinomo_kv::ClusterKvAuthoritativeSnapshotRequest,
    )?;
    call.request = call.request.with_response_retry_safe();
    let mut client = ClusterObjectRpcClient::routed(host, paths);
    let read = client
        .call_target(
            state,
            target_node_id,
            call,
            ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
        )
        .await?
        .decode_body::<KvAuthoritativeRead<KvSnapshot>>()
        .await
        .map_err(|error| verified_cluster_non_leader_error(target_node_id, error))?;
    sinomo_kv::validate_cluster_kv_authoritative_snapshot_read(&read).map_err(kv_err)?;
    Ok(read.value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::{CapabilityName, ClusterMemberRecord, ClusterMemberState};
    use sinomo_kv::{
        KvAuthorityBasis, KvAuthorityProof, KvCommittedRequestRecord, KvRevision, KvTxn,
        KvTxnResponse,
    };
    use sinomo_node::{ClusterId, DAEMON_MAIN_RUN_CAPABILITY};
    use time::OffsetDateTime;

    #[test]
    fn authenticated_forwarded_non_leader_response_retains_responder_identity() {
        let target = NodeId::from_legacy_named("verified_non_leader_target").expect("node");
        let error = verified_cluster_non_leader_error(
            &target,
            NetworkError::ClusterReadOnly {
                detail: format!(
                    "{FORWARDED_CLUSTER_KV_NON_LEADER_DETAIL}; forwarded requests do not start elections"
                ),
            },
        );

        assert!(matches!(
            error,
            NetworkError::VerifiedClusterNonLeader { node_id, .. } if node_id == target
        ));
        assert!(matches!(
            verified_cluster_non_leader_error(
                &target,
                NetworkError::ClusterReadOnly {
                    detail: "quorum is temporarily unavailable".to_string(),
                },
            ),
            NetworkError::ClusterReadOnly { .. }
        ));
    }

    #[test]
    fn kv_forward_does_not_try_another_authority_after_ambiguous_delivery() {
        let rejected_node =
            NodeId::from_legacy_named("node_kv_forward_rejected").expect("rejected node");
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

        assert!(!cluster_kv_forward_error_allows_next_authority(
            &ambiguous,
            &rejected_node,
        ));
        assert!(cluster_kv_forward_error_allows_next_authority(
            &not_sent,
            &rejected_node,
        ));
        let verified_non_leader = NetworkError::VerifiedClusterNonLeader {
            node_id: rejected_node.clone(),
            detail: "target rejected forwarded request as a non-leader".to_string(),
        };
        assert!(cluster_kv_forward_error_allows_next_authority(
            &verified_non_leader,
            &rejected_node,
        ));
        assert!(verified_non_leader_rejects_preferred_authority(
            &verified_non_leader,
            Some(&rejected_node),
            &rejected_node,
        ));
        assert!(!verified_non_leader_rejects_preferred_authority(
            &verified_non_leader,
            None,
            &rejected_node,
        ));
        let different_node =
            NodeId::from_legacy_named("node_kv_forward_different").expect("different node");
        assert!(!cluster_kv_forward_error_allows_next_authority(
            &verified_non_leader,
            &different_node,
        ));
        assert!(!cluster_kv_forward_error_allows_next_authority(
            &NetworkError::Io {
                operation: "legacy KV forward host".to_string(),
                detail: "delivery state unavailable".to_string(),
            },
            &rejected_node,
        ));
        assert!(!cluster_kv_forward_error_allows_next_authority(
            &NetworkError::ObjectRpc {
                route: "/cluster/kv/txn".to_string(),
                detail: "authority returned a definitive application error".to_string(),
            },
            &rejected_node,
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
            if request.route == sinomo_kv::CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE {
                let snapshot = KvSnapshot {
                    revision: KvRevision(1),
                    compacted_revision: KvRevision(0),
                    entries: Vec::new(),
                    leases: Vec::new(),
                    events: Vec::new(),
                    committed_requests: Vec::new(),
                    expired_requests: Vec::new(),
                };
                let response = KvAuthoritativeRead {
                    authority: KvAuthorityProof {
                        term: 1,
                        commit_index: 1,
                        revision: snapshot.revision,
                        basis: KvAuthorityBasis::CurrentTermQuorum {
                            voter_acks: 1,
                            required_quorum: 1,
                        },
                    },
                    value: snapshot,
                };
                let payload =
                    sinomo_kv::cluster_kv_authoritative_snapshot_response_rpc_payload(&response)
                        .expect("authoritative response payload");
                return Ok(ObjectRpcResponse::from_encoded_body(
                    payload.route,
                    payload.body,
                ));
            }
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
                            submitted.request_id.clone()
                        },
                        command_digest: submitted
                            .into_command_request()
                            .digest()
                            .expect("transaction digest"),
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

    struct NonLeaderThenCommittedHost {
        known_authority: Option<NodeId>,
        calls: usize,
    }

    impl ClusterKvTxnForwardHost for NonLeaderThenCommittedHost {
        fn known_kv_state_authority_id(
            &mut self,
            _paths: &ClusterPaths,
            _state: &ClusterBootstrapState,
        ) -> Result<Option<NodeId>, NetworkError> {
            Ok(self.known_authority.clone())
        }

        fn call_node_object_rpc(
            &mut self,
            state: &ClusterBootstrapState,
            target_node_id: &NodeId,
            request: &ObjectRpcRequest,
            timeout: Duration,
        ) -> Result<ObjectRpcResponse, NetworkError> {
            self.calls = self.calls.saturating_add(1);
            if self.calls == 1 {
                let detail = if request.route == sinomo_kv::CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE
                {
                    FORWARDED_CLUSTER_KV_READ_NON_LEADER_DETAIL
                } else {
                    FORWARDED_CLUSTER_KV_NON_LEADER_DETAIL
                };
                return ObjectRpcResponse::encode_error(
                    request.route.clone(),
                    format!("cluster is read-only: {detail}"),
                );
            }
            CommittedResponseHost {
                calls: 0,
                mismatch_request_id: false,
            }
            .call_node_object_rpc(state, target_node_id, request, timeout)
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

    fn add_second_remote_voter(state: &mut ClusterBootstrapState) -> NodeId {
        let now = OffsetDateTime::now_utc();
        let remote =
            NodeId::from_legacy_named("node_kv_forward_remote_second").expect("remote node");
        let mut member = ClusterMemberRecord::local_voter(
            state.cluster_id.clone(),
            remote.clone(),
            "remote-second",
            "pk-remote-second",
            now,
        );
        member.capabilities =
            vec![CapabilityName::new(DAEMON_MAIN_RUN_CAPABILITY).expect("main daemon capability")];
        member.allowed_capabilities = member.capabilities.clone();
        member.endpoints = vec![sinomo_network::daemon_lan_endpoint(
            &remote,
            "10.0.0.3:21353".to_string(),
            0,
        )];
        state.members.push(member);
        remote
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
        let (root, state) = forward_test_state(false);
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        crate::kv_local_store::write_cluster_kv_snapshot(
            &paths,
            &KvSnapshot {
                revision: KvRevision(1),
                compacted_revision: KvRevision::ZERO,
                entries: Vec::new(),
                leases: Vec::new(),
                events: Vec::new(),
                committed_requests: Vec::new(),
                expired_requests: Vec::new(),
            },
        )
        .expect("local replicated snapshot");
        let target = state.members[1].node_id.clone();
        let mut host = CommittedResponseHost {
            calls: 0,
            mismatch_request_id: false,
        };
        let request = KvTxnRequest::generated(test_txn());

        let result = forward_cluster_kv_txn_to_state_authority_node_with_host(
            &paths,
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
        let (root, state) = forward_test_state(false);
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let target = state.members[1].node_id.clone();
        let mut host = CommittedResponseHost {
            calls: 0,
            mismatch_request_id: true,
        };
        let request = KvTxnRequest::generated(test_txn());

        let error = forward_cluster_kv_txn_to_state_authority_node_with_host(
            &paths,
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
    fn kv_forward_materializes_an_ahead_replicated_snapshot_as_exact_commit_proof() {
        let request = KvTxnRequest::generated(test_txn());
        let txn_response = KvTxnResponse {
            succeeded: true,
            revision: KvRevision(1),
            responses: Vec::new(),
        };
        let response = ClusterKvTxnRpcResponse {
            request_id: request.request_id.clone(),
            command_digest: request
                .clone()
                .into_command_request()
                .digest()
                .expect("request digest"),
            txn_response: Some(txn_response.clone()),
            authority: KvAuthorityProof {
                term: 7,
                commit_index: 11,
                revision: KvRevision(1),
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
            replayed_committed_request: false,
        };
        let snapshot = KvSnapshot {
            revision: KvRevision(2),
            compacted_revision: KvRevision::ZERO,
            entries: Vec::new(),
            leases: Vec::new(),
            events: Vec::new(),
            committed_requests: vec![KvCommittedRequestRecord {
                request_id: request.request_id.clone(),
                command_digest: request
                    .clone()
                    .into_command_request()
                    .digest()
                    .expect("request digest"),
                term: 7,
                log_index: 11,
                revision: KvRevision(1),
                txn_response: Some(txn_response),
            }],
            expired_requests: Vec::new(),
        };

        let result =
            try_materialize_forwarded_cluster_kv_txn_response(&request, &response, Some(snapshot))
                .expect("materialization")
                .expect("replicated exact commit");

        assert_eq!(result.snapshot.revision, KvRevision(2));
        assert!(result.replayed_committed_request);
        assert_eq!(result.authority.term, 7);
        assert_eq!(result.authority.commit_index, 11);
        assert_eq!(result.authority.revision, KvRevision(1));
        assert_eq!(
            result.authority.basis,
            KvAuthorityBasis::ExactCommittedRequest
        );
    }

    #[test]
    fn kv_forward_exact_commit_uses_the_durable_record_transaction_result() {
        let request = KvTxnRequest::generated(test_txn());
        let committed_command_digest = sinomo_kv::KvCommandDigest([42; 32]);
        assert_ne!(
            committed_command_digest,
            request
                .clone()
                .into_command_request()
                .digest()
                .expect("logical request digest")
        );
        let durable_txn_response = KvTxnResponse {
            succeeded: true,
            revision: KvRevision(1),
            responses: Vec::new(),
        };
        let response = ClusterKvTxnRpcResponse {
            request_id: request.request_id.clone(),
            command_digest: committed_command_digest,
            txn_response: Some(KvTxnResponse {
                succeeded: false,
                revision: KvRevision(99),
                responses: Vec::new(),
            }),
            authority: KvAuthorityProof {
                term: 7,
                commit_index: 11,
                revision: KvRevision(1),
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
            replayed_committed_request: false,
        };
        let snapshot = KvSnapshot {
            revision: KvRevision(2),
            compacted_revision: KvRevision::ZERO,
            entries: Vec::new(),
            leases: Vec::new(),
            events: Vec::new(),
            committed_requests: vec![KvCommittedRequestRecord {
                request_id: request.request_id.clone(),
                command_digest: committed_command_digest,
                term: 7,
                log_index: 11,
                revision: KvRevision(1),
                txn_response: Some(durable_txn_response.clone()),
            }],
            expired_requests: Vec::new(),
        };

        let result =
            try_materialize_forwarded_cluster_kv_txn_response(&request, &response, Some(snapshot))
                .expect("materialization")
                .expect("replicated exact commit");

        assert_eq!(result.txn_response, Some(durable_txn_response));
        assert!(result.replayed_committed_request);
        assert_eq!(
            result.authority.basis,
            KvAuthorityBasis::ExactCommittedRequest
        );
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

    #[test]
    fn non_voter_skips_verified_non_leader_when_no_authority_is_preferred() {
        let (root, mut state) = forward_test_state(false);
        add_second_remote_voter(&mut state);
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let request = KvTxnRequest::generated(test_txn());
        let mut host = NonLeaderThenCommittedHost {
            known_authority: None,
            calls: 0,
        };

        let response = forward_cluster_kv_txn_to_state_authority_with_host(
            &paths,
            &state,
            &request,
            forward_options(),
            &mut host,
        )
        .expect("forward after verified rejection")
        .expect("committed response");

        // One rejected voter, one compact commit response, then one snapshot
        // fetch because this non-voter has no replicated local commit.
        assert_eq!(host.calls, 3);
        assert_eq!(response.request_id, request.request_id);
    }

    #[test]
    fn preferred_authority_rejection_returns_before_trying_another_voter() {
        let (root, mut state) = forward_test_state(false);
        add_second_remote_voter(&mut state);
        let preferred = state.members[1].node_id.clone();
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let mut host = NonLeaderThenCommittedHost {
            known_authority: Some(preferred.clone()),
            calls: 0,
        };

        let error = forward_cluster_kv_txn_to_state_authority_with_host(
            &paths,
            &state,
            &KvTxnRequest::generated(test_txn()),
            forward_options(),
            &mut host,
        )
        .expect_err("preferred authority rejection must invalidate durable authority first");

        assert_eq!(host.calls, 1);
        assert!(matches!(
            error,
            NetworkError::VerifiedClusterNonLeader { node_id, .. } if node_id == preferred
        ));
    }

    #[test]
    fn authoritative_read_skips_verified_non_leader_when_no_authority_is_preferred() {
        let (root, mut state) = forward_test_state(false);
        add_second_remote_voter(&mut state);
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let mut host = NonLeaderThenCommittedHost {
            known_authority: None,
            calls: 0,
        };

        let read = forward_cluster_kv_authoritative_snapshot_to_state_authority_with_host(
            &paths,
            &state,
            forward_options(),
            &mut host,
        )
        .expect("forward read after verified rejection")
        .expect("authoritative read");

        assert_eq!(host.calls, 2);
        assert_eq!(read.value.revision, KvRevision(1));
        assert_eq!(read.authority.revision, KvRevision(1));
    }
}
