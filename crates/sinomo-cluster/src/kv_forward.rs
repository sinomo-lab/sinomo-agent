#[allow(unused_imports)]
use crate::kv_local_store::{
    ClusterKvPathsExt, kv_err, locked_kv_state_authority_id_async,
    read_cluster_kv_latest_observed_snapshot_arc_if_present,
};
use sinomo_base::ClusterBootstrapState;
use sinomo_kv::{
    ClusterKvRequestOutcomeRead, ClusterKvRequestOutcomeRequest, ClusterKvTxnCommitResult,
    ClusterKvTxnCompactCommitResult, ClusterKvTxnRpcResponse, KvAuthoritativeRead,
    KvAuthorityBasis, KvAuthorityProof, KvEntry, KvRequestId, KvRequestOutcome, KvSnapshot,
    KvTxnRequest,
};
use sinomo_network::ClusterPaths;
use sinomo_network::{
    ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector,
    cluster_node_blocks_leader_preference, cluster_node_blocks_leader_preference_async,
};
use sinomo_network::{ClusterObjectRpcAsyncHost, ClusterObjectRpcClient, ClusterObjectRpcOptions};
use sinomo_network::{NetworkDeliveryState, NetworkError};
use sinomo_network::{ObjectRpcCall, ObjectRpcRequest, ObjectRpcResponse};
use sinomo_node::NodeId;
use std::time::{Duration, Instant};

const FORWARDED_CLUSTER_KV_NON_LEADER_DETAIL: &str =
    "forwarded cluster KV transaction reached a non-leader voter";
const FORWARDED_CLUSTER_KV_READ_NON_LEADER_DETAIL: &str =
    "forwarded authoritative cluster KV read reached a non-leader voter";
// The leader dispatches committed fanout before returning the compact
// transaction response. Give that operation-owned replication a short,
// notification-driven opportunity to materialize the exact commit locally
// before asking the leader for a full authoritative snapshot. This mirrors
// etcd's arm-before-request notifier pattern and avoids both polling and lost
// wakeups.
const FORWARDED_CLUSTER_KV_FANOUT_GRACE: Duration = Duration::from_millis(250);

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
    let deadline = Instant::now() + timeout;
    let kv_paths = paths.kv_paths();
    let mut observed_generation = sinomo_kv::cluster_kv_watch_generation(&kv_paths);
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
    let local_snapshot = read_cluster_kv_latest_observed_snapshot_arc_if_present(paths)
        .ok()
        .flatten()
        .map(|snapshot| (*snapshot).clone());
    if let Some(result) =
        try_materialize_forwarded_cluster_kv_txn_response(request, &response, local_snapshot)?
    {
        return Ok(result);
    }

    let remaining = deadline
        .checked_duration_since(Instant::now())
        .unwrap_or_default();
    let fanout_wait = remaining.min(FORWARDED_CLUSTER_KV_FANOUT_GRACE);
    if !fanout_wait.is_zero() {
        let fanout_deadline = Instant::now() + fanout_wait;
        loop {
            let wait = fanout_deadline.saturating_duration_since(Instant::now());
            if wait.is_zero()
                || !sinomo_kv::wait_for_cluster_kv_watch_change(
                    &kv_paths,
                    observed_generation,
                    wait,
                )
            {
                break;
            }
            observed_generation = sinomo_kv::cluster_kv_watch_generation(&kv_paths);
            let local_snapshot = read_cluster_kv_latest_observed_snapshot_arc_if_present(paths)
                .ok()
                .flatten()
                .map(|snapshot| (*snapshot).clone());
            if let Some(result) = try_materialize_forwarded_cluster_kv_txn_response(
                request,
                &response,
                local_snapshot,
            )? {
                metrics::counter!(
                    "sinomo.kv.forward.materialization",
                    "source" => "committed_fanout"
                )
                .increment(1);
                return Ok(result);
            }
        }
    }

    metrics::counter!(
        "sinomo.kv.forward.materialization",
        "source" => "authoritative_snapshot"
    )
    .increment(1);
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .unwrap_or_default();
    let snapshot =
        fetch_cluster_kv_snapshot_from_node_with_host(state, target_node_id, remaining, host)?;
    materialize_forwarded_cluster_kv_txn_response_from_snapshot(request, response, snapshot)
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
    H: ClusterObjectRpcAsyncHost + Send,
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

async fn forward_cluster_kv_txn_compact_to_node_async_with_host<H>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    request: &KvTxnRequest,
    timeout: Duration,
    host: &mut H,
) -> Result<ClusterKvTxnCompactCommitResult, NetworkError>
where
    H: ClusterObjectRpcAsyncHost + Send,
{
    let mut client = ClusterObjectRpcClient::routed(host, paths);
    let response = client
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
        .await?
        .decode_body::<ClusterKvTxnRpcResponse>()
        .await
        .map_err(|error| verified_cluster_non_leader_error(target_node_id, error))?;
    if response.request_id != request.request_id {
        return Err(forwarded_cluster_kv_request_id_mismatch(
            request,
            &response.request_id,
        ));
    }
    Ok(ClusterKvTxnCompactCommitResult {
        request_id: response.request_id,
        submitted_command_digest: response.submitted_command_digest,
        command_digest: response.command_digest,
        txn_response: response.txn_response,
        authority: response.authority,
        replayed_committed_request: response.replayed_committed_request,
    })
}

/// Compact forwarding path. The authenticated leader's bounded transaction
/// response is sufficient to prove completion; it does not force a second
/// authoritative barrier or transfer the complete keyspace.
pub async fn forward_cluster_kv_txn_compact_to_state_authority_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request: &KvTxnRequest,
    options: ClusterKvTxnForwardOptions,
    host: &mut H,
) -> Result<Option<ClusterKvTxnCompactCommitResult>, NetworkError>
where
    H: ClusterObjectRpcAsyncHost + Send,
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
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match forward_cluster_kv_txn_compact_to_node_async_with_host(
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
            "could not forward compact cluster KV transaction to any known state authority{}",
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
    H: ClusterObjectRpcAsyncHost + Send,
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

/// Forwards a request-outcome lookup to the selected state authority without
/// fetching the authoritative snapshot. The response is bounded by retained
/// request history and carries the same current-term quorum proof as other
/// authoritative reads.
pub async fn forward_cluster_kv_request_outcome_to_state_authority_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request_id: &KvRequestId,
    options: ClusterKvTxnForwardOptions,
    host: &mut H,
) -> Result<Option<ClusterKvRequestOutcomeRead>, NetworkError>
where
    H: ClusterObjectRpcAsyncHost + Send,
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
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let call = cluster_kv_request_outcome_forward_call(request_id)?;
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
                let read = match result.decode_body::<ClusterKvRequestOutcomeRead>().await {
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
                return validate_forwarded_request_outcome_read(read, state, request_id).map(Some);
            }
            Err(error) => failures.push(format!("node {target} failed: {error}")),
        }
    }
    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "could not establish an authoritative cluster KV request outcome{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

fn cluster_kv_request_outcome_forward_call(
    request_id: &KvRequestId,
) -> Result<ObjectRpcCall, NetworkError> {
    let mut call = ObjectRpcCall::encode_value(
        sinomo_kv::CLUSTER_KV_REQUEST_OUTCOME_ROUTE,
        sinomo_kv::CLUSTER_KV_RPC_CATEGORY,
        ClusterKvRequestOutcomeRequest {
            request_id: request_id.clone(),
        },
    )?;
    call.request = call.request.with_response_retry_safe();
    Ok(call)
}

fn validate_forwarded_request_outcome_read(
    read: ClusterKvRequestOutcomeRead,
    state: &ClusterBootstrapState,
    request_id: &KvRequestId,
) -> Result<ClusterKvRequestOutcomeRead, NetworkError> {
    sinomo_kv::validate_cluster_kv_request_outcome_read(&read).map_err(kv_err)?;
    let voter_count = sinomo_kv::cluster_kv_raft_voter_ids(state)
        .map_err(kv_err)?
        .len();
    let expected_quorum = sinomo_kv::cluster_kv_raft_quorum_size(voter_count);
    let KvAuthorityBasis::CurrentTermQuorum {
        voter_acks,
        required_quorum,
    } = read.authority.basis
    else {
        return Err(NetworkError::ClusterReadOnly {
            detail: "authoritative request outcome was not current-term quorum confirmed"
                .to_string(),
        });
    };
    if required_quorum != expected_quorum
        || voter_acks < required_quorum
        || voter_acks > voter_count
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "authoritative request-outcome proof reported {voter_acks} voter acknowledgements and quorum {required_quorum}, but committed membership has {voter_count} voters and requires {expected_quorum}"
            ),
        });
    }
    if read.authority.term < state.term {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "authoritative request-outcome proof term {} is older than locally observed term {}",
                read.authority.term, state.term
            ),
        });
    }
    if let KvRequestOutcome::Committed(record) = &read.outcome
        && &record.request_id != request_id
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "authoritative request outcome returned committed request {} for requested id {request_id}",
                record.request_id
            ),
        });
    }
    Ok(read)
}

/// Forwards a bounded authoritative prefix read directly to the selected
/// state authority. This is the read analogue of the compact transaction RPC:
/// followers do not fetch or import the complete keyspace merely to answer a
/// point/prefix request.
pub async fn forward_cluster_kv_authoritative_range_prefix_to_state_authority_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    prefix: &str,
    options: ClusterKvTxnForwardOptions,
    host: &mut H,
) -> Result<Option<KvAuthoritativeRead<Vec<KvEntry>>>, NetworkError>
where
    H: ClusterObjectRpcAsyncHost + Send,
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
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let mut call = ObjectRpcCall::encode_value(
            sinomo_kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE,
            sinomo_kv::CLUSTER_KV_RPC_CATEGORY,
            sinomo_kv::ClusterKvAuthoritativeRangePrefixRequest {
                prefix: prefix.to_string(),
            },
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
                    .decode_body::<KvAuthoritativeRead<Vec<KvEntry>>>()
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
                return validate_forwarded_authoritative_range_read(read, state, prefix).map(Some);
            }
            Err(error) => failures.push(format!("node {target} failed: {error}")),
        }
    }
    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "could not establish an authoritative cluster KV prefix read{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

fn validate_forwarded_authoritative_range_read(
    read: KvAuthoritativeRead<Vec<KvEntry>>,
    state: &ClusterBootstrapState,
    prefix: &str,
) -> Result<KvAuthoritativeRead<Vec<KvEntry>>, NetworkError> {
    sinomo_kv::validate_cluster_kv_current_term_authority_proof(&read.authority).map_err(kv_err)?;
    let voter_count = sinomo_kv::cluster_kv_raft_voter_ids(state)
        .map_err(kv_err)?
        .len();
    let expected_quorum = sinomo_kv::cluster_kv_raft_quorum_size(voter_count);
    let KvAuthorityBasis::CurrentTermQuorum {
        voter_acks,
        required_quorum,
    } = read.authority.basis
    else {
        return Err(NetworkError::ClusterReadOnly {
            detail: "authoritative prefix read was not current-term quorum confirmed".to_string(),
        });
    };
    if required_quorum != expected_quorum
        || voter_acks < required_quorum
        || voter_acks > voter_count
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "authoritative prefix proof reported {voter_acks} voter acknowledgements and quorum {required_quorum}, but committed membership has {voter_count} voters and requires {expected_quorum}"
            ),
        });
    }
    if read.authority.term < state.term {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "authoritative prefix proof term {} is older than locally observed term {}",
                read.authority.term, state.term
            ),
        });
    }
    if read.value.len() > sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "authoritative prefix response returned {} entries, exceeding the {} entry limit",
                read.value.len(),
                sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES
            ),
        });
    }

    let mut payload_bytes = 0_usize;
    let mut previous_key: Option<&str> = None;
    for entry in &read.value {
        payload_bytes = payload_bytes
            .saturating_add(entry.key.len())
            .saturating_add(entry.value.len());
        if payload_bytes > sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "authoritative prefix response exceeded the {} byte payload limit",
                    sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES
                ),
            });
        }
        if !entry.key.starts_with(prefix) {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "authoritative prefix response returned key {:?} outside requested prefix {:?}",
                    entry.key, prefix
                ),
            });
        }
        if previous_key.is_some_and(|previous| previous >= entry.key.as_str()) {
            return Err(NetworkError::ClusterReadOnly {
                detail: "authoritative prefix response keys are not strictly increasing"
                    .to_string(),
            });
        }
        if entry.create_revision > entry.modify_revision
            || entry.modify_revision > read.authority.revision
        {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "authoritative prefix entry {:?} has revisions {}..{} outside proof revision {}",
                    entry.key,
                    entry.create_revision.0,
                    entry.modify_revision.0,
                    read.authority.revision.0
                ),
            });
        }
        previous_key = Some(entry.key.as_str());
    }
    Ok(read)
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
    H: ClusterObjectRpcAsyncHost + Send,
{
    let deadline = Instant::now() + timeout;
    let kv_paths = paths.kv_paths();
    let mut observed_generation = sinomo_kv::cluster_kv_watch_generation(&kv_paths);
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

    let remaining = deadline
        .checked_duration_since(Instant::now())
        .unwrap_or_default();
    let fanout_wait = remaining.min(FORWARDED_CLUSTER_KV_FANOUT_GRACE);
    if !fanout_wait.is_zero() {
        let fanout_deadline = Instant::now() + fanout_wait;
        loop {
            let wait = fanout_deadline.saturating_duration_since(Instant::now());
            if wait.is_zero() {
                break;
            }
            let wait_paths = kv_paths.clone();
            let local_paths = paths.clone();
            let wait_result = tokio::task::spawn_blocking(move || {
                let changed = sinomo_kv::wait_for_cluster_kv_watch_change(
                    &wait_paths,
                    observed_generation,
                    wait,
                );
                let generation = sinomo_kv::cluster_kv_watch_generation(&wait_paths);
                let snapshot = changed
                    .then(|| read_cluster_kv_latest_observed_snapshot_arc_if_present(&local_paths));
                (changed, generation, snapshot)
            })
            .await;
            let Ok((changed, generation, local_snapshot)) = wait_result else {
                break;
            };
            if !changed {
                break;
            }
            observed_generation = generation;
            let local_snapshot = local_snapshot
                .and_then(Result::ok)
                .flatten()
                .map(|snapshot| (*snapshot).clone());
            if let Some(result) = try_materialize_forwarded_cluster_kv_txn_response(
                request,
                &response,
                local_snapshot,
            )? {
                metrics::counter!(
                    "sinomo.kv.forward.materialization",
                    "source" => "committed_fanout"
                )
                .increment(1);
                return Ok(result);
            }
        }
    }

    metrics::counter!(
        "sinomo.kv.forward.materialization",
        "source" => "authoritative_snapshot"
    )
    .increment(1);
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .unwrap_or_default();
    let snapshot = fetch_cluster_kv_snapshot_from_node_async_with_host(
        paths,
        state,
        target_node_id,
        remaining,
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
    H: ClusterObjectRpcAsyncHost + Send,
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
        KvTxnResponse, KvVersion,
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
    fn forwarded_request_outcome_uses_bounded_rpc_instead_of_snapshot_route() {
        let request_id = KvRequestId::generate();
        let call = cluster_kv_request_outcome_forward_call(&request_id)
            .expect("build request-outcome forward call");

        assert_eq!(
            call.request.route,
            sinomo_kv::CLUSTER_KV_REQUEST_OUTCOME_ROUTE
        );
        assert_ne!(
            call.request.route,
            sinomo_kv::CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE
        );
        assert!(call.request.is_response_retry_safe());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let body = runtime
            .block_on(call.body.collect_to_vec(64 * 1024))
            .expect("collect forwarded request outcome body");
        assert_eq!(
            sinomo_kv::decode_cluster_kv_request_outcome_rpc_request_body(&body)
                .expect("decode forwarded request outcome")
                .request_id,
            request_id
        );
    }

    #[test]
    fn forwarded_request_outcome_is_fenced_by_membership_term_and_request_id() {
        let (_root, mut state) = forward_test_state(true);
        add_second_remote_voter(&mut state);
        state.term = 4;
        let request = sinomo_kv::KvCommandRequest::generated(sinomo_kv::KvCommand::Barrier);
        let committed = KvCommittedRequestRecord {
            request_id: request.request_id.clone(),
            command_digest: request.digest().expect("request digest"),
            term: 4,
            log_index: 7,
            revision: KvRevision(6),
            txn_response: None,
        };
        let valid = ClusterKvRequestOutcomeRead {
            authority: KvAuthorityProof {
                term: 4,
                commit_index: 7,
                revision: KvRevision(6),
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
            outcome: KvRequestOutcome::Committed(committed),
        };

        assert!(
            validate_forwarded_request_outcome_read(valid.clone(), &state, &request.request_id)
                .is_ok()
        );

        let mut wrong_quorum = valid.clone();
        wrong_quorum.authority.basis = KvAuthorityBasis::CurrentTermQuorum {
            voter_acks: 1,
            required_quorum: 1,
        };
        assert!(
            validate_forwarded_request_outcome_read(wrong_quorum, &state, &request.request_id)
                .is_err()
        );

        let mut stale = valid.clone();
        stale.authority.term = 3;
        assert!(
            validate_forwarded_request_outcome_read(stale, &state, &request.request_id).is_err()
        );

        assert!(
            validate_forwarded_request_outcome_read(valid, &state, &KvRequestId::generate())
                .is_err()
        );
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

    struct FanoutAfterResponseHost {
        paths: ClusterPaths,
        calls: usize,
        fanout: Option<std::thread::JoinHandle<()>>,
    }

    impl ClusterKvTxnForwardHost for FanoutAfterResponseHost {
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
            if request.route != sinomo_kv::CLUSTER_KV_TXN_ROUTE {
                return Err(NetworkError::delivery_failed(
                    "unexpected authoritative snapshot fallback",
                    NetworkDeliveryState::DefinitelyNotSent,
                    "committed fanout should satisfy the forwarded transaction",
                ));
            }
            let submitted = sinomo_kv::decode_cluster_kv_txn_rpc_request_body(&request.body)
                .expect("transaction request");
            let command_digest = submitted
                .clone()
                .into_command_request()
                .digest()
                .expect("transaction digest");
            let txn_response = KvTxnResponse {
                succeeded: true,
                revision: KvRevision(1),
                responses: Vec::new(),
            };
            let paths = self.paths.clone();
            let request_id = submitted.request_id.clone();
            let durable_txn_response = txn_response.clone();
            self.fanout = Some(std::thread::spawn(move || {
                // An unrelated watch update must not end the operation-owned
                // grace window before this request's committed fanout lands.
                std::thread::sleep(Duration::from_millis(5));
                sinomo_kv::notify_cluster_kv_watchers(&paths.kv_paths());
                std::thread::sleep(Duration::from_millis(10));
                crate::kv_local_store::write_cluster_kv_snapshot(
                    &paths,
                    &KvSnapshot {
                        revision: KvRevision(1),
                        compacted_revision: KvRevision::ZERO,
                        entries: Vec::new(),
                        leases: Vec::new(),
                        events: Vec::new(),
                        committed_requests: vec![KvCommittedRequestRecord {
                            request_id,
                            command_digest,
                            term: 1,
                            log_index: 1,
                            revision: KvRevision(1),
                            txn_response: Some(durable_txn_response),
                        }],
                        expired_requests: Vec::new(),
                    },
                )
                .expect("materialize committed fanout snapshot");
            }));
            let payload =
                sinomo_kv::cluster_kv_txn_response_rpc_payload(&ClusterKvTxnCommitResult {
                    request_id: submitted.request_id,
                    command_digest,
                    snapshot: KvSnapshot {
                        revision: KvRevision::ZERO,
                        compacted_revision: KvRevision::ZERO,
                        entries: Vec::new(),
                        leases: Vec::new(),
                        events: Vec::new(),
                        committed_requests: Vec::new(),
                        expired_requests: Vec::new(),
                    },
                    txn_response: Some(txn_response),
                    authority: KvAuthorityProof {
                        term: 1,
                        commit_index: 1,
                        revision: KvRevision(1),
                        basis: KvAuthorityBasis::CurrentTermQuorum {
                            voter_acks: 2,
                            required_quorum: 2,
                        },
                    },
                    replayed_committed_request: false,
                })
                .expect("transaction response payload");
            Ok(ObjectRpcResponse::from_encoded_body(
                payload.route,
                payload.body,
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

    fn forwarded_range_entry(key: &str, create_revision: u64, modify_revision: u64) -> KvEntry {
        KvEntry {
            key: key.to_string(),
            value: vec![1],
            create_revision: KvRevision(create_revision),
            modify_revision: KvRevision(modify_revision),
            version: KvVersion(1),
            lease_id: None,
        }
    }

    fn forwarded_range_read(
        term: u64,
        revision: u64,
        voter_acks: usize,
        required_quorum: usize,
        entries: Vec<KvEntry>,
    ) -> KvAuthoritativeRead<Vec<KvEntry>> {
        KvAuthoritativeRead {
            authority: KvAuthorityProof {
                term,
                commit_index: 7,
                revision: KvRevision(revision),
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks,
                    required_quorum,
                },
            },
            value: entries,
        }
    }

    #[test]
    fn forwarded_prefix_read_requires_committed_membership_quorum_and_current_term() {
        let (_root, mut state) = forward_test_state(true);
        add_second_remote_voter(&mut state);
        state.term = 4;

        assert!(
            validate_forwarded_authoritative_range_read(
                forwarded_range_read(4, 6, 2, 2, vec![forwarded_range_entry("/agents/a", 2, 6)],),
                &state,
                "/agents/",
            )
            .is_ok()
        );
        assert!(
            validate_forwarded_authoritative_range_read(
                forwarded_range_read(4, 6, 1, 1, vec![forwarded_range_entry("/agents/a", 2, 6)],),
                &state,
                "/agents/",
            )
            .is_err(),
            "a responder cannot shrink a three-voter quorum to one"
        );
        assert!(
            validate_forwarded_authoritative_range_read(
                forwarded_range_read(3, 6, 2, 2, vec![forwarded_range_entry("/agents/a", 2, 6)],),
                &state,
                "/agents/",
            )
            .is_err(),
            "a proof older than the caller's observed term must be rejected"
        );
    }

    #[test]
    fn forwarded_prefix_read_rejects_noncanonical_or_unbounded_results() {
        let (_root, mut state) = forward_test_state(true);
        add_second_remote_voter(&mut state);

        for entries in [
            vec![forwarded_range_entry("/other/a", 1, 1)],
            vec![
                forwarded_range_entry("/agents/b", 1, 1),
                forwarded_range_entry("/agents/a", 1, 1),
            ],
            vec![forwarded_range_entry("/agents/a", 4, 3)],
            vec![forwarded_range_entry("/agents/a", 1, 8)],
        ] {
            assert!(
                validate_forwarded_authoritative_range_read(
                    forwarded_range_read(1, 7, 2, 2, entries),
                    &state,
                    "/agents/",
                )
                .is_err()
            );
        }

        let entries = (0..=sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
            .map(|index| forwarded_range_entry(&format!("/agents/{index:05}"), 1, 1))
            .collect();
        assert!(
            validate_forwarded_authoritative_range_read(
                forwarded_range_read(1, 1, 2, 2, entries),
                &state,
                "/agents/",
            )
            .is_err(),
            "the forwarding boundary must enforce the server's range cardinality limit"
        );
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
    fn kv_forward_waits_for_operation_owned_fanout_before_snapshot_fallback() {
        let (root, state) = forward_test_state(false);
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let target = state.members[1].node_id.clone();
        let mut host = FanoutAfterResponseHost {
            paths: paths.clone(),
            calls: 0,
            fanout: None,
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
        .expect("committed fanout materializes the forwarded transaction");

        if let Some(fanout) = host.fanout.take() {
            fanout.join().expect("fanout thread");
        }
        assert_eq!(host.calls, 1, "no full-snapshot RPC should be issued");
        assert_eq!(result.request_id, request.request_id);
        assert_eq!(result.snapshot.revision, KvRevision(1));
        assert_eq!(result.authority.commit_index, 1);
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
            submitted_command_digest: Some(
                request
                    .clone()
                    .into_command_request()
                    .digest()
                    .expect("submitted request digest"),
            ),
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
            submitted_command_digest: Some(
                request
                    .clone()
                    .into_command_request()
                    .digest()
                    .expect("submitted request digest"),
            ),
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
