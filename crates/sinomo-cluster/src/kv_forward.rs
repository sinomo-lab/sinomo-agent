#[allow(unused_imports)]
use crate::kv_local_store::{ClusterKvPathsExt, kv_err, read_cluster_kv_cached_snapshot_if_hot};
use sinomo_base::ClusterBootstrapState;
use sinomo_kv::{
    ClusterKvRequestOutcomeRead, ClusterKvRequestOutcomeRequest, ClusterKvTxnCommitResult,
    ClusterKvTxnCompactCommitResult, ClusterKvTxnRpcResponse, KvAuthoritativeRead,
    KvAuthorityBasis, KvAuthorityProof, KvEntry, KvRequestId, KvRequestOutcome, KvSnapshot,
    KvTxnRequest,
};
use sinomo_network::ClusterPaths;
use sinomo_network::{ClusterObjectRpcAsyncHost, ClusterObjectRpcOptions};
use sinomo_network::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
use sinomo_network::{ObjectRpcCall, ObjectRpcRequest, ObjectRpcResponse};
use sinomo_node::NodeId;
use std::future::Future;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use crate::control_plane_options::main_daemon_cluster_rpc_options;

pub(crate) const FORWARDED_CLUSTER_KV_NON_LEADER_DETAIL: &str =
    "forwarded cluster KV transaction reached a non-leader voter";
pub(crate) const FORWARDED_CLUSTER_KV_READ_NON_LEADER_DETAIL: &str =
    "forwarded authoritative cluster KV read reached a non-leader voter";
pub(crate) const FORWARDED_CLUSTER_KV_EXACT_READ_NON_LEADER_DETAIL: &str =
    "forwarded authoritative exact-key-set read reached a non-leader voter";
pub(crate) const FORWARDED_CLUSTER_KV_POINT_READ_NON_LEADER_DETAIL: &str =
    "forwarded authoritative point read reached a non-leader voter";
pub(crate) const FORWARDED_CLUSTER_KV_PREFIX_READ_NON_LEADER_DETAIL: &str =
    "forwarded authoritative prefix read reached a non-leader voter";
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
    if [
        FORWARDED_CLUSTER_KV_NON_LEADER_DETAIL,
        FORWARDED_CLUSTER_KV_READ_NON_LEADER_DETAIL,
        FORWARDED_CLUSTER_KV_EXACT_READ_NON_LEADER_DETAIL,
        FORWARDED_CLUSTER_KV_POINT_READ_NON_LEADER_DETAIL,
        FORWARDED_CLUSTER_KV_PREFIX_READ_NON_LEADER_DETAIL,
    ]
    .into_iter()
    .any(|marker| detail.contains(marker))
    {
        NetworkError::VerifiedClusterNonLeader {
            node_id: target_node_id.clone(),
            detail,
        }
    } else {
        NetworkError::ClusterReadOnly { detail }
    }
}

/// Preserve the authenticated target identity when the routed client rejects
/// an RPC error response before the caller can decode its typed body.
///
/// `ClusterObjectRpcClient::call_target` validates `response.error_detail()`
/// itself, so an authenticated non-leader rejection normally exits from this
/// await rather than from the later `decode_body` await. Both boundaries must
/// feed the same classifier or a stale durable leader cannot be invalidated.
pub(crate) async fn await_authenticated_cluster_kv_target<T>(
    target_node_id: &NodeId,
    call: impl Future<Output = Result<T, NetworkError>>,
) -> Result<T, NetworkError> {
    call.await
        .map_err(|error| verified_cluster_non_leader_error(target_node_id, error))
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

fn cluster_kv_forward_deadline(timeout: Duration) -> Instant {
    let started = Instant::now();
    started.checked_add(timeout).unwrap_or(started)
}

fn cluster_kv_forward_attempt_deadline(overall_deadline: Instant, timeout: Duration) -> Instant {
    Instant::now()
        .checked_add(timeout)
        .map(|deadline| deadline.min(overall_deadline))
        .unwrap_or(overall_deadline)
}

fn forwarded_cluster_kv_txn_timeout(timeout: Duration) -> NetworkError {
    NetworkError::timeout(
        "materialize forwarded cluster KV transaction",
        NetworkTimeoutPhase::Overall,
        timeout,
        NetworkDeliveryState::PossiblyApplied,
    )
}

fn forwarded_cluster_kv_txn_remaining(
    deadline: Instant,
    timeout: Duration,
) -> Result<Duration, NetworkError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(forwarded_cluster_kv_txn_timeout(timeout))
    } else {
        Ok(remaining)
    }
}

struct AbortForwardBlockingTaskOnDrop<T> {
    task: tokio::task::JoinHandle<T>,
}

const FORWARD_BLOCKING_QUEUED: u8 = 0;
const FORWARD_BLOCKING_ADMITTED: u8 = 1;
const FORWARD_BLOCKING_CANCELLED: u8 = 2;
const FORWARD_BLOCKING_COMPLETE: u8 = 3;

impl<T> Drop for AbortForwardBlockingTaskOnDrop<T> {
    fn drop(&mut self) {
        // `spawn_blocking` can cancel queued work even though an already-running
        // blocking closure cannot be interrupted. The closures admitted here do
        // only a hot-cache lookup or one deadline-bounded notifier wait; they
        // never start durable hydration. Full snapshot cloning uses the
        // explicitly admitted-and-awaited helper below.
        self.task.abort();
    }
}

async fn run_forward_blocking_before_deadline<T, F>(
    deadline: Instant,
    timeout: Duration,
    work: F,
) -> Result<T, NetworkError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, NetworkError> + Send + 'static,
{
    forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
    let admission = std::sync::Arc::new(AtomicU8::new(FORWARD_BLOCKING_QUEUED));
    let task_admission = std::sync::Arc::clone(&admission);
    let mut task = AbortForwardBlockingTaskOnDrop {
        task: tokio::task::spawn_blocking(move || {
            if task_admission
                .compare_exchange(
                    FORWARD_BLOCKING_QUEUED,
                    FORWARD_BLOCKING_ADMITTED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                return Err(forwarded_cluster_kv_txn_timeout(timeout));
            }
            let result = (|| {
                forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
                let value = work()?;
                forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
                Ok(value)
            })();
            task_admission.store(FORWARD_BLOCKING_COMPLETE, Ordering::Release);
            result
        }),
    };
    let remaining = forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
    match tokio::time::timeout(remaining, &mut task.task).await {
        Ok(result) => result.map_err(|source| NetworkError::Io {
            operation: "materialize forwarded cluster KV transaction".to_string(),
            detail: format!("forwarded materialization task failed: {source}"),
        })?,
        Err(_) => {
            if admission
                .compare_exchange(
                    FORWARD_BLOCKING_QUEUED,
                    FORWARD_BLOCKING_CANCELLED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                task.task.abort();
                return Err(forwarded_cluster_kv_txn_timeout(timeout));
            }
            // The blocking closure won admission at the timeout edge. It is
            // deadline-cooperative, so await its terminal deadline result
            // instead of detaching it behind the completed caller.
            (&mut task.task).await.map_err(|source| NetworkError::Io {
                operation: "materialize forwarded cluster KV transaction".to_string(),
                detail: format!("forwarded materialization task failed: {source}"),
            })?
        }
    }
}

/// Runs an inherently complete, O(total snapshot) materialization without
/// letting an outer timeout detach it. A queued closure is cancelled exactly
/// at the deadline; once the closure wins admission, the caller awaits the
/// coherent owned snapshot just like the authoritative full-snapshot path.
async fn run_forward_admitted_snapshot_materialization<T, F>(
    deadline: Instant,
    timeout: Duration,
    work: F,
) -> Result<T, NetworkError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, NetworkError> + Send + 'static,
{
    forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
    let admission = std::sync::Arc::new(AtomicU8::new(FORWARD_BLOCKING_QUEUED));
    let task_admission = std::sync::Arc::clone(&admission);
    let mut task = AbortForwardBlockingTaskOnDrop {
        task: tokio::task::spawn_blocking(move || {
            if task_admission
                .compare_exchange(
                    FORWARD_BLOCKING_QUEUED,
                    FORWARD_BLOCKING_ADMITTED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                return Err(forwarded_cluster_kv_txn_timeout(timeout));
            }
            forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
            let result = work();
            task_admission.store(FORWARD_BLOCKING_COMPLETE, Ordering::Release);
            result
        }),
    };
    let remaining = forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
    match tokio::time::timeout(remaining, &mut task.task).await {
        Ok(result) => result.map_err(|source| NetworkError::Io {
            operation: "materialize forwarded cluster KV snapshot".to_string(),
            detail: format!("forwarded snapshot materialization task failed: {source}"),
        })?,
        Err(_) => {
            if admission
                .compare_exchange(
                    FORWARD_BLOCKING_QUEUED,
                    FORWARD_BLOCKING_CANCELLED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                task.task.abort();
                return Err(forwarded_cluster_kv_txn_timeout(timeout));
            }
            // `abort` cannot cancel an admitted `spawn_blocking` closure. Wait
            // for its coherent full-snapshot result rather than returning a
            // timeout while the O(N) clone continues in the background.
            (&mut task.task).await.map_err(|source| NetworkError::Io {
                operation: "materialize forwarded cluster KV snapshot".to_string(),
                detail: format!("forwarded snapshot materialization task failed: {source}"),
            })?
        }
    }
}

fn cluster_kv_forward_remaining(
    deadline: Instant,
    timeout: Duration,
    operation: &'static str,
) -> Result<Duration, NetworkError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(NetworkError::timeout(
            operation,
            NetworkTimeoutPhase::Overall,
            timeout,
            NetworkDeliveryState::DefinitelyNotSent,
        ))
    } else {
        Ok(remaining)
    }
}

async fn decode_forwarded_cluster_kv_authority_before<T, F>(
    target_node_id: &NodeId,
    operation: &'static str,
    deadline: Instant,
    timeout: Duration,
    call: F,
) -> Result<T, NetworkError>
where
    T: serde::de::DeserializeOwned + Send + 'static,
    F: Future<Output = Result<sinomo_network::ObjectRpcResult, NetworkError>>,
{
    let remaining = cluster_kv_forward_remaining(deadline, timeout, operation)?;
    let rpc = async {
        await_authenticated_cluster_kv_target(target_node_id, call)
            .await?
            .decode_body::<T>()
            .await
            .map_err(|error| verified_cluster_non_leader_error(target_node_id, error))
    };
    tokio::time::timeout(remaining, rpc).await.map_err(|_| {
        NetworkError::timeout(
            operation,
            NetworkTimeoutPhase::Overall,
            timeout,
            NetworkDeliveryState::PossiblyApplied,
        )
    })?
}

fn projected_remote_cluster_kv_voters(
    state: &ClusterBootstrapState,
) -> Result<Vec<NodeId>, NetworkError> {
    let mut targets = sinomo_kv::cluster_kv_raft_voter_ids(state).map_err(kv_err)?;
    targets.retain(|target| target != &state.local_node_id);
    Ok(targets)
}

/// Select forwarding candidates and inspect the durable leader hint under one
/// caller-owned deadline. In particular, the blocking leader lookup must not
/// acquire the Raft operation lock after this future has timed out.
async fn cluster_kv_forward_authority_targets_before_deadline(
    _runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    deadline: Instant,
    timeout: Duration,
    operation: &'static str,
) -> Result<Option<(Vec<NodeId>, Option<NodeId>)>, NetworkError> {
    cluster_kv_forward_remaining(deadline, timeout, operation)?;
    // `state` is the caller's already-projected committed membership fence.
    // Re-entering the process-wide endpoint selector here would read the
    // current cluster directory again, mixing generations and potentially
    // hydrating a cold full KV snapshot before a point read can even be sent.
    // The fenced routed client below owns endpoint selection from this exact
    // membership view, so this layer needs only the exact remote voter IDs.
    let mut targets = projected_remote_cluster_kv_voters(state)?;
    cluster_kv_forward_remaining(deadline, timeout, operation)?;
    if targets.is_empty() {
        return Ok(None);
    }

    // A leader hint is advisory. Never enqueue behind the process-wide Raft
    // operation gate merely to reorder exact voter targets: on contention the
    // zero-budget lookup returns `Busy` and forwarding immediately tries the
    // committed voter set in its canonical order.
    let known_authority_lookup = paths
        .kv_paths()
        .raft_store()
        .try_locked_known_voter_leader_id(state);
    let known_authority_id = known_authority_lookup.ok().flatten();
    cluster_kv_forward_remaining(deadline, timeout, operation)?;

    let preferred_authority_id = match known_authority_id {
        Some(authority_id) if authority_id == state.local_node_id => return Ok(None),
        Some(authority_id) if targets.contains(&authority_id) => {
            prefer_known_state_authority_node(&mut targets, &authority_id);
            Some(authority_id)
        }
        // A missing hint says nothing about which exact voter currently leads.
        // This is expected after restart and while catching up. Probe the
        // committed remote voters before starting a disruptive election; every
        // authority route validates current-term leadership before serving.
        _ => None,
    };

    Ok(Some((targets, preferred_authority_id)))
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
    let deadline = cluster_kv_forward_deadline(options.timeout);
    let mut targets = projected_remote_cluster_kv_voters(state)?;
    if targets.is_empty() {
        return Ok(None);
    }
    let known_authority_id = host
        .known_kv_state_authority_id(paths, state)
        .ok()
        .flatten();
    let preferred_authority_id = match known_authority_id {
        Some(authority_id) if authority_id == state.local_node_id => return Ok(None),
        Some(authority_id) if targets.contains(&authority_id) => {
            prefer_known_state_authority_node(&mut targets, &authority_id);
            Some(authority_id)
        }
        // A missing hint is not evidence that this voter should campaign.
        // Exact remote voters independently validate current-term leadership.
        _ => None,
    };

    let mut failures = Vec::new();
    let mut rejected_preferred_authority = None;
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
        let attempt_timeout = options.routed_node_attempt_timeout(remaining, targets.len() - index);
        let attempt_deadline = cluster_kv_forward_attempt_deadline(deadline, attempt_timeout);
        match forward_cluster_kv_txn_to_state_authority_node_with_host(
            paths,
            state,
            target,
            request,
            attempt_timeout,
            attempt_deadline,
            host,
        ) {
            Ok(response) => return Ok(Some(response)),
            Err(error) => {
                failures.push(format!("node {target} failed: {error}"));
                record_verified_non_leader_rejected_preferred_authority(
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                retain_verified_non_leader_rejected_preferred_authority(
                    &mut rejected_preferred_authority,
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                if !cluster_kv_forward_error_allows_next_authority(&error, target) {
                    return Err(error);
                }
            }
        }
    }

    Err(cluster_kv_forward_terminal_error(
        rejected_preferred_authority,
        format!(
            "could not forward cluster KV transaction to any known state authority{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    ))
}

pub fn forward_cluster_kv_authoritative_snapshot_to_state_authority_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvTxnForwardOptions,
    host: &mut impl ClusterKvTxnForwardHost,
) -> Result<Option<KvAuthoritativeRead<KvSnapshot>>, NetworkError> {
    let mut targets = projected_remote_cluster_kv_voters(state)?;
    if targets.is_empty() {
        return Ok(None);
    }
    let preferred_authority_id = match host
        .known_kv_state_authority_id(paths, state)
        .ok()
        .flatten()
    {
        Some(authority_id) if authority_id == state.local_node_id => return Ok(None),
        Some(authority_id) if targets.contains(&authority_id) => {
            prefer_known_state_authority_node(&mut targets, &authority_id);
            Some(authority_id)
        }
        // A missing hint is not evidence that this voter should campaign.
        // Exact remote voters independently validate current-term leadership.
        _ => None,
    };

    let deadline = Instant::now() + options.timeout;
    let mut failures = Vec::new();
    let mut rejected_preferred_authority = None;
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
        match host
            .call_node_object_rpc(state, target, &request, timeout)
            .map_err(|error| verified_cluster_non_leader_error(target, error))
        {
            Ok(response) => {
                if let Err(error) = response
                    .error_detail()
                    .map_err(|error| verified_cluster_non_leader_error(target, error))
                {
                    failures.push(format!("node {target} failed: {error}"));
                    record_verified_non_leader_rejected_preferred_authority(
                        &error,
                        preferred_authority_id.as_ref(),
                        target,
                    );
                    retain_verified_non_leader_rejected_preferred_authority(
                        &mut rejected_preferred_authority,
                        &error,
                        preferred_authority_id.as_ref(),
                        target,
                    );
                    if !cluster_kv_forward_error_allows_next_authority(&error, target) {
                        return Err(error);
                    }
                    continue;
                }
                let read =
                    sinomo_kv::decode_cluster_kv_authoritative_snapshot_rpc_body(&response.body)
                        .map_err(kv_err)?;
                return validate_forwarded_authoritative_snapshot_read(read, state).map(Some);
            }
            Err(error) => {
                failures.push(format!("node {target} failed: {error}"));
                record_verified_non_leader_rejected_preferred_authority(
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                retain_verified_non_leader_rejected_preferred_authority(
                    &mut rejected_preferred_authority,
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                if !cluster_kv_forward_error_allows_next_authority(&error, target) {
                    return Err(error);
                }
            }
        }
    }
    Err(cluster_kv_forward_terminal_error(
        rejected_preferred_authority,
        format!(
            "could not establish an authoritative cluster KV read{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    ))
}

fn forward_cluster_kv_txn_to_state_authority_node_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    request: &KvTxnRequest,
    timeout: Duration,
    deadline: Instant,
    host: &mut impl ClusterKvTxnForwardHost,
) -> Result<ClusterKvTxnCommitResult, NetworkError> {
    let kv_paths = paths.kv_paths();
    let mut observed_generation = sinomo_kv::cluster_kv_watch_generation(&kv_paths);
    let txn_payload = sinomo_kv::cluster_kv_txn_rpc_payload(request).map_err(kv_err)?;
    let txn_request = ObjectRpcRequest::from_encoded_body(
        txn_payload.route,
        txn_payload.category,
        txn_payload.body,
    );
    let rpc_timeout = forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
    let txn_response = host
        .call_node_object_rpc(state, target_node_id, &txn_request, rpc_timeout)
        .map_err(|error| verified_cluster_non_leader_error(target_node_id, error))?;
    txn_response
        .error_detail()
        .map_err(|error| verified_cluster_non_leader_error(target_node_id, error))?;
    let response = sinomo_kv::decode_cluster_kv_txn_rpc_body(&txn_response.body).map_err(kv_err)?;
    if let Some(result) = try_materialize_forwarded_cluster_kv_txn_response_from_hot_local_cache(
        paths, request, &response, deadline, timeout,
    )? {
        return Ok(result);
    }

    let remaining = forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
    let fanout_wait = remaining.min(FORWARDED_CLUSTER_KV_FANOUT_GRACE);
    if !fanout_wait.is_zero() {
        let fanout_deadline = cluster_kv_forward_attempt_deadline(deadline, fanout_wait);
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
            if let Some(result) =
                try_materialize_forwarded_cluster_kv_txn_response_from_hot_local_cache(
                    paths, request, &response, deadline, timeout,
                )?
            {
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
    let remaining = forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
    let snapshot =
        fetch_cluster_kv_snapshot_from_node_with_host(state, target_node_id, remaining, host)?;
    let result =
        materialize_forwarded_cluster_kv_txn_response_from_snapshot(request, response, snapshot)?;
    forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
    Ok(result)
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
    let deadline = cluster_kv_forward_deadline(options.timeout);
    let Some((targets, preferred_authority_id)) =
        cluster_kv_forward_authority_targets_before_deadline(
            runtime,
            paths,
            state,
            deadline,
            options.timeout,
            "forward cluster KV transaction",
        )
        .await?
    else {
        return Ok(None);
    };

    let mut failures = Vec::new();
    let mut rejected_preferred_authority = None;
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
        let attempt_timeout = options.routed_node_attempt_timeout(remaining, targets.len() - index);
        let attempt_deadline = cluster_kv_forward_attempt_deadline(deadline, attempt_timeout);
        match forward_cluster_kv_txn_to_state_authority_node_async_with_host(
            paths,
            state,
            target,
            request,
            attempt_timeout,
            attempt_deadline,
            host,
        )
        .await
        {
            Ok(response) => return Ok(Some(response)),
            Err(error) => {
                failures.push(format!("node {target} failed: {error}"));
                record_verified_non_leader_rejected_preferred_authority(
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                retain_verified_non_leader_rejected_preferred_authority(
                    &mut rejected_preferred_authority,
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                if !cluster_kv_forward_error_allows_next_authority(&error, target) {
                    return Err(error);
                }
            }
        }
    }

    Err(cluster_kv_forward_terminal_error(
        rejected_preferred_authority,
        format!(
            "could not forward cluster KV transaction to any known state authority{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    ))
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
    let response = await_authenticated_cluster_kv_target(
        target_node_id,
        host.call_node_object_rpc_async(
            paths,
            state,
            target_node_id,
            ObjectRpcCall::encode_borrowed_value(
                sinomo_kv::CLUSTER_KV_TXN_ROUTE,
                sinomo_kv::CLUSTER_KV_RPC_CATEGORY,
                request,
            )?,
            main_daemon_cluster_rpc_options(
                ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
            ),
        ),
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
    let deadline = cluster_kv_forward_deadline(options.timeout);
    let Some((targets, preferred_authority_id)) =
        cluster_kv_forward_authority_targets_before_deadline(
            runtime,
            paths,
            state,
            deadline,
            options.timeout,
            "forward compact cluster KV transaction",
        )
        .await?
    else {
        return Ok(None);
    };
    let mut failures = Vec::new();
    let mut rejected_preferred_authority = None;
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
                record_verified_non_leader_rejected_preferred_authority(
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                retain_verified_non_leader_rejected_preferred_authority(
                    &mut rejected_preferred_authority,
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                if !cluster_kv_forward_error_allows_next_authority(&error, target) {
                    return Err(error);
                }
            }
        }
    }
    Err(cluster_kv_forward_terminal_error(
        rejected_preferred_authority,
        format!(
            "could not forward compact cluster KV transaction to any known state authority{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    ))
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
    let deadline = cluster_kv_forward_deadline(options.timeout);
    let Some((targets, preferred_authority_id)) =
        cluster_kv_forward_authority_targets_before_deadline(
            runtime,
            paths,
            state,
            deadline,
            options.timeout,
            "forward authoritative cluster KV snapshot",
        )
        .await?
    else {
        return Ok(None);
    };
    let mut failures = Vec::new();
    let mut rejected_preferred_authority = None;
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
        let attempt_deadline = cluster_kv_forward_attempt_deadline(deadline, timeout);
        match decode_forwarded_cluster_kv_authority_before::<KvAuthoritativeRead<KvSnapshot>, _>(
            target,
            "forward authoritative cluster KV snapshot",
            attempt_deadline,
            timeout,
            host.call_node_object_rpc_async(
                paths,
                state,
                target,
                call,
                main_daemon_cluster_rpc_options(
                    ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
                ),
            ),
        )
        .await
        {
            Ok(read) => {
                return validate_forwarded_authoritative_snapshot_read(read, state).map(Some);
            }
            Err(error) => {
                failures.push(format!("node {target} failed: {error}"));
                record_verified_non_leader_rejected_preferred_authority(
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                retain_verified_non_leader_rejected_preferred_authority(
                    &mut rejected_preferred_authority,
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                if !cluster_kv_forward_error_allows_next_authority(&error, target) {
                    return Err(error);
                }
            }
        }
    }
    Err(cluster_kv_forward_terminal_error(
        rejected_preferred_authority,
        format!(
            "could not establish an authoritative cluster KV read{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    ))
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
    let deadline = cluster_kv_forward_deadline(options.timeout);
    let Some((targets, preferred_authority_id)) =
        cluster_kv_forward_authority_targets_before_deadline(
            runtime,
            paths,
            state,
            deadline,
            options.timeout,
            "forward authoritative cluster KV request outcome",
        )
        .await?
    else {
        return Ok(None);
    };
    let mut failures = Vec::new();
    let mut rejected_preferred_authority = None;
    for (index, target) in targets.iter().enumerate() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let call = cluster_kv_request_outcome_forward_call(request_id)?;
        let timeout = options.routed_node_attempt_timeout(remaining, targets.len() - index);
        let attempt_deadline = cluster_kv_forward_attempt_deadline(deadline, timeout);
        match decode_forwarded_cluster_kv_authority_before::<ClusterKvRequestOutcomeRead, _>(
            target,
            "forward authoritative cluster KV request outcome",
            attempt_deadline,
            timeout,
            host.call_node_object_rpc_async(
                paths,
                state,
                target,
                call,
                main_daemon_cluster_rpc_options(
                    ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
                ),
            ),
        )
        .await
        {
            Ok(read) => {
                return validate_forwarded_request_outcome_read(read, state, request_id).map(Some);
            }
            Err(error) => {
                failures.push(format!("node {target} failed: {error}"));
                record_verified_non_leader_rejected_preferred_authority(
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                retain_verified_non_leader_rejected_preferred_authority(
                    &mut rejected_preferred_authority,
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                if !cluster_kv_forward_error_allows_next_authority(&error, target) {
                    return Err(error);
                }
            }
        }
    }
    Err(cluster_kv_forward_terminal_error(
        rejected_preferred_authority,
        format!(
            "could not establish an authoritative cluster KV request outcome{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    ))
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

/// Reject a forwarded proof that predates either scalar authority floor the
/// caller already observed. A higher term cannot compensate for an older
/// committed KV generation, and a higher revision cannot compensate for an
/// older term.
pub(crate) fn validate_forwarded_cluster_kv_authority_floor(
    authority: &KvAuthorityProof,
    state: &ClusterBootstrapState,
    proof_kind: &'static str,
) -> Result<(), NetworkError> {
    if authority.term < state.term {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "{proof_kind} term {} is older than locally observed term {}",
                authority.term, state.term
            ),
        });
    }
    if authority.revision.0 < state.kv_revision {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "{proof_kind} revision {} is older than locally observed revision {}",
                authority.revision.0, state.kv_revision
            ),
        });
    }
    Ok(())
}

pub(crate) fn validate_forwarded_authoritative_snapshot_read(
    read: KvAuthoritativeRead<KvSnapshot>,
    state: &ClusterBootstrapState,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
    sinomo_kv::validate_cluster_kv_authoritative_snapshot_read(&read).map_err(kv_err)?;
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
            detail: "authoritative snapshot read was not current-term quorum confirmed".to_string(),
        });
    };
    if required_quorum != expected_quorum
        || voter_acks < required_quorum
        || voter_acks > voter_count
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "authoritative snapshot proof reported {voter_acks} voter acknowledgements and quorum {required_quorum}, but committed membership has {voter_count} voters and requires {expected_quorum}"
            ),
        });
    }
    validate_forwarded_cluster_kv_authority_floor(
        &read.authority,
        state,
        "authoritative snapshot proof",
    )?;
    if read.authority.revision != read.value.revision {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "authoritative proof revision {} does not match returned snapshot revision {}",
                read.authority.revision.0, read.value.revision.0
            ),
        });
    }
    Ok(read)
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
    validate_forwarded_cluster_kv_authority_floor(
        &read.authority,
        state,
        "authoritative request-outcome proof",
    )?;
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

/// Forwards one authoritative point read directly to the selected state
/// authority. Point reads must use the exact-entry route: implementing them as
/// prefix reads makes unrelated keys that share the requested key string count
/// against the bounded prefix cardinality and payload limits.
pub async fn forward_cluster_kv_authoritative_entry_to_state_authority_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    key: &str,
    options: ClusterKvTxnForwardOptions,
    host: &mut H,
) -> Result<Option<KvAuthoritativeRead<Option<KvEntry>>>, NetworkError>
where
    H: ClusterObjectRpcAsyncHost + Send,
{
    let deadline = cluster_kv_forward_deadline(options.timeout);
    let Some((targets, preferred_authority_id)) =
        cluster_kv_forward_authority_targets_before_deadline(
            runtime,
            paths,
            state,
            deadline,
            options.timeout,
            "forward authoritative cluster KV point read",
        )
        .await?
    else {
        return Ok(None);
    };
    let mut failures = Vec::new();
    let mut rejected_preferred_authority = None;
    for (index, target) in targets.iter().enumerate() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let call = cluster_kv_authoritative_entry_forward_call(key)?;
        let timeout = options.routed_node_attempt_timeout(remaining, targets.len() - index);
        let attempt_deadline = cluster_kv_forward_attempt_deadline(deadline, timeout);
        match decode_forwarded_cluster_kv_authority_before::<
            KvAuthoritativeRead<Option<KvEntry>>,
            _,
        >(
            target,
            "forward authoritative cluster KV point read",
            attempt_deadline,
            timeout,
            host.call_node_object_rpc_async(
                paths,
                state,
                target,
                call,
                main_daemon_cluster_rpc_options(
                    ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
                ),
            ),
        )
        .await
        {
            Ok(read) => {
                return validate_forwarded_authoritative_entry_read(read, state, key).map(Some);
            }
            Err(error) => {
                failures.push(format!("node {target} failed: {error}"));
                record_verified_non_leader_rejected_preferred_authority(
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                retain_verified_non_leader_rejected_preferred_authority(
                    &mut rejected_preferred_authority,
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                if !cluster_kv_forward_error_allows_next_authority(&error, target) {
                    return Err(error);
                }
            }
        }
    }
    Err(cluster_kv_forward_terminal_error(
        rejected_preferred_authority,
        format!(
            "could not establish an authoritative cluster KV point read{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    ))
}

fn cluster_kv_authoritative_entry_forward_call(key: &str) -> Result<ObjectRpcCall, NetworkError> {
    let mut call = ObjectRpcCall::encode_value(
        sinomo_kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE,
        sinomo_kv::CLUSTER_KV_RPC_CATEGORY,
        sinomo_kv::ClusterKvEntryRequest {
            key: key.to_string(),
        },
    )?;
    call.request = call.request.with_response_retry_safe();
    Ok(call)
}

fn validate_forwarded_authoritative_entry_read(
    read: KvAuthoritativeRead<Option<KvEntry>>,
    state: &ClusterBootstrapState,
    key: &str,
) -> Result<KvAuthoritativeRead<Option<KvEntry>>, NetworkError> {
    sinomo_kv::validate_cluster_kv_authoritative_entry_read(&read).map_err(kv_err)?;
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
            detail: "authoritative point read was not current-term quorum confirmed".to_string(),
        });
    };
    if required_quorum != expected_quorum
        || voter_acks < required_quorum
        || voter_acks > voter_count
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "authoritative point proof reported {voter_acks} voter acknowledgements and quorum {required_quorum}, but committed membership has {voter_count} voters and requires {expected_quorum}"
            ),
        });
    }
    validate_forwarded_cluster_kv_authority_floor(
        &read.authority,
        state,
        "authoritative point proof",
    )?;
    if let Some(entry) = read.value.as_ref()
        && entry.key != key
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "authoritative point response returned key {:?} for requested key {:?}",
                entry.key, key
            ),
        });
    }
    Ok(read)
}

/// Forwards an ordered exact-key set as one authoritative operation. The
/// authority performs one ReadIndex and returns exactly one slot per requested
/// key, so unrelated writes cannot make a multi-key read straddle revisions.
pub async fn forward_cluster_kv_authoritative_entries_to_state_authority_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    keys: &[String],
    options: ClusterKvTxnForwardOptions,
    host: &mut H,
) -> Result<Option<KvAuthoritativeRead<Vec<Option<KvEntry>>>>, NetworkError>
where
    H: ClusterObjectRpcAsyncHost + Send,
{
    let deadline = cluster_kv_forward_deadline(options.timeout);
    sinomo_kv::validate_cluster_kv_authoritative_entries_request_keys(keys).map_err(kv_err)?;
    let Some((targets, preferred_authority_id)) =
        cluster_kv_forward_authority_targets_before_deadline(
            runtime,
            paths,
            state,
            deadline,
            options.timeout,
            "forward authoritative cluster KV exact-key-set read",
        )
        .await?
    else {
        return Ok(None);
    };
    let mut failures = Vec::new();
    let mut rejected_preferred_authority = None;
    for (index, target) in targets.iter().enumerate() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let call = cluster_kv_authoritative_entries_forward_call(keys)?;
        let timeout = options.routed_node_attempt_timeout(remaining, targets.len() - index);
        let attempt_deadline = cluster_kv_forward_attempt_deadline(deadline, timeout);
        match decode_forwarded_cluster_kv_authority_before::<
            sinomo_kv::ClusterKvAuthoritativeEntriesResponse,
            _,
        >(
            target,
            "forward authoritative cluster KV exact-key-set read",
            attempt_deadline,
            timeout,
            host.call_node_object_rpc_async(
                paths,
                state,
                target,
                call,
                main_daemon_cluster_rpc_options(
                    ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
                ),
            ),
        )
        .await
        {
            Ok(response) => {
                let read =
                    sinomo_kv::unpack_cluster_kv_authoritative_entries_response(keys, response)
                        .map_err(kv_err)?;
                return validate_forwarded_authoritative_entries_read(read, state, keys).map(Some);
            }
            Err(error) => {
                failures.push(format!("node {target} failed: {error}"));
                record_verified_non_leader_rejected_preferred_authority(
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                retain_verified_non_leader_rejected_preferred_authority(
                    &mut rejected_preferred_authority,
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                if !cluster_kv_forward_error_allows_next_authority(&error, target) {
                    return Err(error);
                }
            }
        }
    }
    Err(cluster_kv_forward_terminal_error(
        rejected_preferred_authority,
        format!(
            "could not establish an authoritative cluster KV exact-key-set read{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    ))
}

fn cluster_kv_authoritative_entries_forward_call(
    keys: &[String],
) -> Result<ObjectRpcCall, NetworkError> {
    sinomo_kv::validate_cluster_kv_authoritative_entries_request_keys(keys).map_err(kv_err)?;
    let mut call = ObjectRpcCall::encode_value(
        sinomo_kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE,
        sinomo_kv::CLUSTER_KV_RPC_CATEGORY,
        sinomo_kv::ClusterKvAuthoritativeEntriesRequest {
            keys: keys.to_vec(),
        },
    )?;
    call.request = call.request.with_response_retry_safe();
    Ok(call)
}

pub(crate) fn validate_forwarded_authoritative_entries_read(
    read: KvAuthoritativeRead<Vec<Option<KvEntry>>>,
    state: &ClusterBootstrapState,
    keys: &[String],
) -> Result<KvAuthoritativeRead<Vec<Option<KvEntry>>>, NetworkError> {
    sinomo_kv::validate_cluster_kv_authoritative_entries_read(keys, &read).map_err(kv_err)?;
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
            detail: "authoritative exact-key-set read was not current-term quorum confirmed"
                .to_string(),
        });
    };
    if required_quorum != expected_quorum
        || voter_acks < required_quorum
        || voter_acks > voter_count
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "authoritative exact-key-set proof reported {voter_acks} voter acknowledgements and quorum {required_quorum}, but committed membership has {voter_count} voters and requires {expected_quorum}"
            ),
        });
    }
    validate_forwarded_cluster_kv_authority_floor(
        &read.authority,
        state,
        "authoritative exact-key-set proof",
    )?;
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
    forward_cluster_kv_authoritative_prefix_kind_to_state_authority_async_with_host(
        runtime, paths, state, prefix, options, false, host,
    )
    .await
}

pub(crate) async fn forward_cluster_kv_authoritative_complete_range_prefix_to_state_authority_async_with_host<
    H,
>(
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
    forward_cluster_kv_authoritative_prefix_kind_to_state_authority_async_with_host(
        runtime, paths, state, prefix, options, true, host,
    )
    .await
}

async fn forward_cluster_kv_authoritative_prefix_kind_to_state_authority_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    prefix: &str,
    options: ClusterKvTxnForwardOptions,
    complete: bool,
    host: &mut H,
) -> Result<Option<KvAuthoritativeRead<Vec<KvEntry>>>, NetworkError>
where
    H: ClusterObjectRpcAsyncHost + Send,
{
    let deadline = cluster_kv_forward_deadline(options.timeout);
    let Some((targets, preferred_authority_id)) =
        cluster_kv_forward_authority_targets_before_deadline(
            runtime,
            paths,
            state,
            deadline,
            options.timeout,
            "forward authoritative cluster KV prefix read",
        )
        .await?
    else {
        return Ok(None);
    };
    let mut failures = Vec::new();
    let mut rejected_preferred_authority = None;
    for (index, target) in targets.iter().enumerate() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let mut call = ObjectRpcCall::encode_value(
            if complete {
                sinomo_kv::CLUSTER_KV_AUTHORITATIVE_COMPLETE_RANGE_PREFIX_ROUTE
            } else {
                sinomo_kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE
            },
            sinomo_kv::CLUSTER_KV_RPC_CATEGORY,
            sinomo_kv::ClusterKvAuthoritativeRangePrefixRequest {
                prefix: prefix.to_string(),
            },
        )?;
        call.request = call.request.with_response_retry_safe();
        let timeout = options.routed_node_attempt_timeout(remaining, targets.len() - index);
        let attempt_deadline = cluster_kv_forward_attempt_deadline(deadline, timeout);
        match decode_forwarded_cluster_kv_authority_before::<KvAuthoritativeRead<Vec<KvEntry>>, _>(
            target,
            "forward authoritative cluster KV prefix read",
            attempt_deadline,
            timeout,
            host.call_node_object_rpc_async(
                paths,
                state,
                target,
                call,
                main_daemon_cluster_rpc_options(
                    ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
                ),
            ),
        )
        .await
        {
            Ok(read) => {
                return if complete {
                    validate_forwarded_authoritative_complete_range_read(read, state, prefix)
                } else {
                    validate_forwarded_authoritative_range_read(read, state, prefix)
                }
                .map(Some);
            }
            Err(error) => {
                failures.push(format!("node {target} failed: {error}"));
                record_verified_non_leader_rejected_preferred_authority(
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                retain_verified_non_leader_rejected_preferred_authority(
                    &mut rejected_preferred_authority,
                    &error,
                    preferred_authority_id.as_ref(),
                    target,
                );
                if !cluster_kv_forward_error_allows_next_authority(&error, target) {
                    return Err(error);
                }
            }
        }
    }
    Err(cluster_kv_forward_terminal_error(
        rejected_preferred_authority,
        format!(
            "could not establish an authoritative cluster KV prefix read{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    ))
}

fn validate_forwarded_authoritative_range_read(
    read: KvAuthoritativeRead<Vec<KvEntry>>,
    state: &ClusterBootstrapState,
    prefix: &str,
) -> Result<KvAuthoritativeRead<Vec<KvEntry>>, NetworkError> {
    validate_forwarded_authoritative_range_read_with_limits(
        read,
        state,
        prefix,
        sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
        sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
    )
}

pub(crate) fn validate_forwarded_authoritative_complete_range_read(
    read: KvAuthoritativeRead<Vec<KvEntry>>,
    state: &ClusterBootstrapState,
    prefix: &str,
) -> Result<KvAuthoritativeRead<Vec<KvEntry>>, NetworkError> {
    let read = validate_forwarded_authoritative_range_read_with_limits(
        read,
        state,
        prefix,
        sinomo_kv::CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_ENTRIES,
        sinomo_kv::CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
    )?;
    sinomo_kv::validate_cluster_kv_authoritative_complete_range_prefix_read(prefix, &read)
        .map_err(kv_err)?;
    Ok(read)
}

fn validate_forwarded_authoritative_range_read_with_limits(
    read: KvAuthoritativeRead<Vec<KvEntry>>,
    state: &ClusterBootstrapState,
    prefix: &str,
    max_entries: usize,
    max_payload_bytes: usize,
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
    validate_forwarded_cluster_kv_authority_floor(
        &read.authority,
        state,
        "authoritative prefix proof",
    )?;
    if read.value.len() > max_entries {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "authoritative prefix response returned {} entries, exceeding the {} entry limit",
                read.value.len(),
                max_entries
            ),
        });
    }

    let mut payload_bytes = 0_usize;
    let mut previous_key: Option<&str> = None;
    for entry in &read.value {
        payload_bytes = payload_bytes
            .saturating_add(entry.key.len())
            .saturating_add(entry.value.len());
        if payload_bytes > max_payload_bytes {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "authoritative prefix response exceeded the {} byte payload limit",
                    max_payload_bytes
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

fn record_verified_non_leader_rejected_preferred_authority(
    error: &NetworkError,
    preferred_authority_id: Option<&NodeId>,
    target_node_id: &NodeId,
) {
    if verified_non_leader_rejects_preferred_authority(
        error,
        preferred_authority_id,
        target_node_id,
    ) {
        // The hint is advisory and can become stale between requests. An
        // authenticated exact voter rejected it as a non-leader, so continue
        // within this operation's existing deadline and let the next exact
        // voter prove current-term leadership.
        metrics::counter!("sinomo.kv.forward.preferred_authority", "outcome" => "stale")
            .increment(1);
    }
}

fn retain_verified_non_leader_rejected_preferred_authority(
    retained: &mut Option<NetworkError>,
    error: &NetworkError,
    preferred_authority_id: Option<&NodeId>,
    target_node_id: &NodeId,
) {
    if retained.is_none()
        && verified_non_leader_rejects_preferred_authority(
            error,
            preferred_authority_id,
            target_node_id,
        )
    {
        *retained = Some(error.clone());
    }
}

fn cluster_kv_forward_terminal_error(
    rejected_preferred_authority: Option<NetworkError>,
    aggregate_detail: String,
) -> NetworkError {
    // An authenticated rejection from the exact voter named by the durable
    // leader hint is stronger than the lossy aggregate assembled after all
    // safe candidates are exhausted. Preserve that identity so the caller can
    // compare-and-clear only the matching stale hint, then retry the canonical
    // voter set. Unsafe or ambiguous failures return from their loop
    // immediately and can never be replaced here.
    rejected_preferred_authority.unwrap_or(NetworkError::ClusterReadOnly {
        detail: aggregate_detail,
    })
}

async fn forward_cluster_kv_txn_to_state_authority_node_async_with_host<H>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    request: &KvTxnRequest,
    timeout: Duration,
    deadline: Instant,
    host: &mut H,
) -> Result<ClusterKvTxnCommitResult, NetworkError>
where
    H: ClusterObjectRpcAsyncHost + Send,
{
    let kv_paths = paths.kv_paths();
    let mut observed_generation = sinomo_kv::cluster_kv_watch_generation(&kv_paths);
    let rpc_timeout = forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
    let txn_response = await_authenticated_cluster_kv_target(
        target_node_id,
        host.call_node_object_rpc_async(
            paths,
            state,
            target_node_id,
            ObjectRpcCall::encode_borrowed_value(
                sinomo_kv::CLUSTER_KV_TXN_ROUTE,
                sinomo_kv::CLUSTER_KV_RPC_CATEGORY,
                request,
            )?,
            main_daemon_cluster_rpc_options(
                ClusterObjectRpcOptions::default().with_overall_timeout(rpc_timeout),
            ),
        ),
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
    if let Some(result) =
        try_materialize_forwarded_cluster_kv_txn_response_from_hot_local_cache_async(
            paths, request, &response, deadline, timeout,
        )
        .await?
    {
        return Ok(result);
    }

    let remaining = forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
    let fanout_wait = remaining.min(FORWARDED_CLUSTER_KV_FANOUT_GRACE);
    if !fanout_wait.is_zero() {
        let fanout_deadline = cluster_kv_forward_attempt_deadline(deadline, fanout_wait);
        loop {
            let wait = fanout_deadline.saturating_duration_since(Instant::now());
            if wait.is_zero() {
                break;
            }
            let wait_paths = kv_paths.clone();
            let wait_result = run_forward_blocking_before_deadline(deadline, timeout, move || {
                let wait = fanout_deadline.saturating_duration_since(Instant::now());
                let changed = sinomo_kv::wait_for_cluster_kv_watch_change(
                    &wait_paths,
                    observed_generation,
                    wait,
                );
                let generation = sinomo_kv::cluster_kv_watch_generation(&wait_paths);
                Ok((changed, generation))
            })
            .await?;
            let (changed, generation) = wait_result;
            if !changed {
                break;
            }
            observed_generation = generation;
            if let Some(result) =
                try_materialize_forwarded_cluster_kv_txn_response_from_hot_local_cache_async(
                    paths, request, &response, deadline, timeout,
                )
                .await?
            {
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
    let remaining = forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
    let snapshot = tokio::time::timeout(
        remaining,
        fetch_cluster_kv_snapshot_from_node_async_with_host(
            paths,
            state,
            target_node_id,
            remaining,
            host,
        ),
    )
    .await
    .map_err(|_| forwarded_cluster_kv_txn_timeout(timeout))??;
    let result =
        materialize_forwarded_cluster_kv_txn_response_from_snapshot(request, response, snapshot)?;
    forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
    Ok(result)
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

fn try_materialize_forwarded_cluster_kv_txn_response_from_hot_snapshot(
    request: &KvTxnRequest,
    response: &ClusterKvTxnRpcResponse,
    snapshot: Option<std::sync::Arc<KvSnapshot>>,
    deadline: Instant,
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
    let matches_authority_revision = matches!(
        response.authority.basis,
        KvAuthorityBasis::CurrentTermQuorum { .. }
    ) && snapshot.revision == response.authority.revision;
    let contains_exact_commit = snapshot
        .committed_requests
        .iter()
        .any(|record| record.request_id == request.request_id);
    if !matches_authority_revision && !contains_exact_commit {
        return Ok(None);
    }
    // Returning the legacy full commit result requires an owned snapshot. Once
    // this O(total retained state) clone is admitted, complete it coherently;
    // callers that only need the bounded transaction proof use the compact
    // path and never enter this boundary.
    let snapshot = snapshot.as_ref().clone();
    let overrun = Instant::now().saturating_duration_since(deadline);
    if !overrun.is_zero() {
        metrics::counter!(
            "sinomo.kv.forward.materialization_deadline_overrun",
            "source" => "hot_snapshot_clone"
        )
        .increment(1);
        metrics::histogram!(
            "sinomo.kv.forward.materialization_deadline_overrun.us",
            "source" => "hot_snapshot_clone"
        )
        .record(overrun.as_micros().min(u128::from(u64::MAX)) as f64);
    }
    try_materialize_forwarded_cluster_kv_txn_response(request, response, Some(snapshot))
}

fn try_materialize_forwarded_cluster_kv_txn_response_from_hot_local_cache(
    paths: &ClusterPaths,
    request: &KvTxnRequest,
    response: &ClusterKvTxnRpcResponse,
    deadline: Instant,
    timeout: Duration,
) -> Result<Option<ClusterKvTxnCommitResult>, NetworkError> {
    forwarded_cluster_kv_txn_remaining(deadline, timeout)?;
    // This is deliberately a cache-only peek. Any miss falls through to the
    // notifier/fetch path instead of opening Raft or decoding a snapshot file.
    let snapshot = read_cluster_kv_cached_snapshot_if_hot(paths).ok().flatten();
    let result = try_materialize_forwarded_cluster_kv_txn_response_from_hot_snapshot(
        request, response, snapshot, deadline,
    )?;
    Ok(result)
}

async fn try_materialize_forwarded_cluster_kv_txn_response_from_hot_local_cache_async(
    paths: &ClusterPaths,
    request: &KvTxnRequest,
    response: &ClusterKvTxnRpcResponse,
    deadline: Instant,
    timeout: Duration,
) -> Result<Option<ClusterKvTxnCommitResult>, NetworkError> {
    let paths = paths.clone();
    let request = request.clone();
    let response = response.clone();
    run_forward_admitted_snapshot_materialization(deadline, timeout, move || {
        try_materialize_forwarded_cluster_kv_txn_response_from_hot_local_cache(
            &paths, &request, &response, deadline, timeout,
        )
    })
    .await
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
            authority: response.authority,
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
    let response = host
        .call_node_object_rpc(state, target_node_id, &request, timeout)
        .map_err(|error| verified_cluster_non_leader_error(target_node_id, error))?;
    response
        .error_detail()
        .map_err(|error| verified_cluster_non_leader_error(target_node_id, error))?;
    let read = sinomo_kv::decode_cluster_kv_authoritative_snapshot_rpc_body(&response.body)
        .map_err(kv_err)?;
    validate_forwarded_authoritative_snapshot_read(read, state).map(|read| read.value)
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
    let read = decode_forwarded_cluster_kv_authority_before::<KvAuthoritativeRead<KvSnapshot>, _>(
        target_node_id,
        "fetch authoritative cluster KV snapshot",
        cluster_kv_forward_deadline(timeout),
        timeout,
        host.call_node_object_rpc_async(
            paths,
            state,
            target_node_id,
            call,
            main_daemon_cluster_rpc_options(
                ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
            ),
        ),
    )
    .await?;
    validate_forwarded_authoritative_snapshot_read(read, state).map(|read| read.value)
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
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use time::OffsetDateTime;

    struct CountingDynamicRouteDirectory {
        reads: Arc<AtomicUsize>,
    }

    impl CountingDynamicRouteDirectory {
        fn record(&self) {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl sinomo_network::ClusterDirectory for CountingDynamicRouteDirectory {
        fn members(&self) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
            self.record();
            Ok(Vec::new())
        }

        fn endpoints(&self) -> Result<Vec<sinomo_base::ClusterEndpointRecord>, NetworkError> {
            self.record();
            Ok(Vec::new())
        }

        fn node_statuses(
            &self,
        ) -> Result<BTreeMap<NodeId, sinomo_base::ClusterNodeStatusRecord>, NetworkError> {
            self.record();
            Ok(BTreeMap::new())
        }

        fn connectivity_records(
            &self,
        ) -> Result<Vec<sinomo_network::ClusterConnectivityRecord>, NetworkError> {
            self.record();
            Ok(Vec::new())
        }

        fn version(&self) -> Result<sinomo_network::DirectoryVersion, NetworkError> {
            self.record();
            Ok(sinomo_network::DirectoryVersion(1))
        }
    }

    struct DynamicRouteProviderActivation(Arc<AtomicBool>);

    impl Drop for DynamicRouteProviderActivation {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Release);
        }
    }

    #[derive(Default)]
    struct FencedForwardDispatchHost;

    impl ClusterObjectRpcAsyncHost for FencedForwardDispatchHost {}

    #[derive(Default)]
    struct RecordingNodeDispatchHost {
        calls: usize,
    }

    impl ClusterObjectRpcAsyncHost for RecordingNodeDispatchHost {
        fn call_node_object_rpc_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            _call: ObjectRpcCall,
            _options: ClusterObjectRpcOptions,
        ) -> sinomo_network::ClusterObjectRpcFuture<'a, sinomo_network::ObjectRpcResult> {
            self.calls = self.calls.saturating_add(1);
            Box::pin(async {
                Err(NetworkError::delivery_failed(
                    "test host node dispatch",
                    NetworkDeliveryState::DefinitelyNotSent,
                    "host dispatch seam reached",
                ))
            })
        }
    }

    #[tokio::test]
    async fn async_txn_and_snapshot_forward_use_host_node_dispatch_seam() {
        let (root, state) = forward_test_state(false);
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(root.path()));
        paths.ensure().expect("cluster paths");
        let target = state.members[1].node_id.clone();
        let request = KvTxnRequest::generated(test_txn());
        let timeout = Duration::from_secs(1);
        let mut host = RecordingNodeDispatchHost::default();

        let error = forward_cluster_kv_txn_to_state_authority_node_async_with_host(
            &paths,
            &state,
            &target,
            &request,
            timeout,
            Instant::now() + timeout,
            &mut host,
        )
        .await
        .expect_err("recording dispatch rejects the forwarded transaction");

        assert_eq!(host.calls, 1);
        assert!(error.to_string().contains("host dispatch seam reached"));

        let error = fetch_cluster_kv_snapshot_from_node_async_with_host(
            &paths, &state, &target, timeout, &mut host,
        )
        .await
        .expect_err("recording dispatch rejects the snapshot fallback");

        assert_eq!(host.calls, 2);
        assert!(error.to_string().contains("host dispatch seam reached"));
    }

    #[test]
    fn authenticated_forwarded_non_leader_response_retains_responder_identity() {
        let target = NodeId::from_legacy_named("verified_non_leader_target").expect("node");
        for marker in [
            FORWARDED_CLUSTER_KV_NON_LEADER_DETAIL,
            FORWARDED_CLUSTER_KV_READ_NON_LEADER_DETAIL,
            FORWARDED_CLUSTER_KV_EXACT_READ_NON_LEADER_DETAIL,
            FORWARDED_CLUSTER_KV_POINT_READ_NON_LEADER_DETAIL,
            FORWARDED_CLUSTER_KV_PREFIX_READ_NON_LEADER_DETAIL,
        ] {
            let error = verified_cluster_non_leader_error(
                &target,
                NetworkError::ClusterReadOnly {
                    detail: format!("{marker}; forwarded requests do not start elections"),
                },
            );

            assert!(matches!(
                error,
                NetworkError::VerifiedClusterNonLeader { node_id, .. } if node_id == target
            ));
        }
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

    #[tokio::test]
    async fn authenticated_non_leader_from_async_call_boundary_retains_target_identity() {
        let target = NodeId::from_legacy_named("verified_async_non_leader_target").expect("node");

        for detail in [
            FORWARDED_CLUSTER_KV_NON_LEADER_DETAIL,
            FORWARDED_CLUSTER_KV_READ_NON_LEADER_DETAIL,
            FORWARDED_CLUSTER_KV_EXACT_READ_NON_LEADER_DETAIL,
            FORWARDED_CLUSTER_KV_POINT_READ_NON_LEADER_DETAIL,
            FORWARDED_CLUSTER_KV_PREFIX_READ_NON_LEADER_DETAIL,
        ] {
            let error = await_authenticated_cluster_kv_target(
                &target,
                std::future::ready(Err::<(), _>(NetworkError::ClusterReadOnly {
                    detail: detail.to_string(),
                })),
            )
            .await
            .expect_err("authenticated non-leader rejection");

            assert!(matches!(
                error,
                NetworkError::VerifiedClusterNonLeader { node_id, .. } if node_id == target
            ));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn forwarded_exact_read_deadline_covers_stalled_streamed_response_body() {
        let timeout = Duration::from_millis(80);
        let target = NodeId::from_legacy_named("stalled_exact_read_target").expect("node");
        let (body_sender, body) = sinomo_network::ObjectRpcIncomingBody::test_channel(Some(16), 1);
        let response = ObjectRpcResponse::from_encoded_body(
            sinomo_kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE,
            Vec::new(),
        );
        let result = sinomo_network::ObjectRpcResult::from_forwarded_body(response, body, true)
            .expect("construct streamed exact-read response");
        let started = tokio::time::Instant::now();
        let deadline = Instant::now()
            .checked_add(timeout)
            .expect("exact-read deadline");

        let error = decode_forwarded_cluster_kv_authority_before::<
            sinomo_kv::ClusterKvAuthoritativeEntriesResponse,
            _,
        >(
            &target,
            "forward authoritative cluster KV exact-key-set read",
            deadline,
            timeout,
            async move {
                tokio::time::sleep(Duration::from_millis(55)).await;
                Ok(result)
            },
        )
        .await
        .expect_err("a stalled streamed exact-read body must use the original deadline");
        // Keep the sending half live through decode so the receiver genuinely
        // stalls rather than observing an immediately closed channel.
        drop(body_sender);

        assert_eq!(tokio::time::Instant::now() - started, timeout);
        assert!(matches!(
            error,
            NetworkError::Timeout {
                ref operation,
                phase: NetworkTimeoutPhase::Overall,
                timeout: actual_timeout,
                delivery_state: NetworkDeliveryState::PossiblyApplied,
            } if operation == "forward authoritative cluster KV exact-key-set read"
                && actual_timeout == timeout
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
    fn every_forwarded_read_family_requires_both_scalar_authority_floors() {
        let (_root, mut state) = forward_test_state(true);
        add_second_remote_voter(&mut state);
        state.term = 4;
        state.kv_revision = 6;
        let request_id = KvRequestId::generate();
        let keys = vec!["/authority/floor".to_string()];

        for (term, revision, expected) in [
            (4, 5, "revision 5 is older than locally observed revision 6"),
            (3, 6, "term 3 is older than locally observed term 4"),
        ] {
            let authority = KvAuthorityProof {
                term,
                commit_index: 7,
                revision: KvRevision(revision),
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            };
            let request_error = validate_forwarded_request_outcome_read(
                ClusterKvRequestOutcomeRead {
                    authority,
                    outcome: KvRequestOutcome::Unknown {
                        detail: "not retained".to_string(),
                    },
                },
                &state,
                &request_id,
            )
            .expect_err("request-outcome proof below either scalar floor must fail")
            .to_string();
            let point_error = validate_forwarded_authoritative_entry_read(
                KvAuthoritativeRead {
                    authority,
                    value: None,
                },
                &state,
                &keys[0],
            )
            .expect_err("point proof below either scalar floor must fail")
            .to_string();
            let exact_error = validate_forwarded_authoritative_entries_read(
                KvAuthoritativeRead {
                    authority,
                    value: vec![None],
                },
                &state,
                &keys,
            )
            .expect_err("exact-set proof below either scalar floor must fail")
            .to_string();
            let prefix_error = validate_forwarded_authoritative_range_read(
                KvAuthoritativeRead {
                    authority,
                    value: Vec::new(),
                },
                &state,
                "/authority/",
            )
            .expect_err("prefix proof below either scalar floor must fail")
            .to_string();

            for error in [request_error, point_error, exact_error, prefix_error] {
                assert!(
                    error.contains(expected),
                    "forwarded read lost the composed scalar floor: {error}"
                );
            }
        }
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
        assert!(!cluster_kv_forward_error_allows_next_authority(
            &NetworkError::delivery_failed(
                "cluster KV transaction",
                NetworkDeliveryState::PossiblyApplied,
                "response was lost after delivery",
            ),
            &rejected_node,
        ));
        assert!(!cluster_kv_forward_error_allows_next_authority(
            &NetworkError::UnknownRoute {
                route: sinomo_kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE.to_string(),
            },
            &rejected_node,
        ));
    }

    #[test]
    fn safe_terminal_exhaustion_retains_preferred_non_leader_proof_for_every_route_family() {
        let preferred =
            NodeId::from_legacy_named("node_kv_forward_preferred").expect("preferred node");
        let definitely_not_sent = NetworkError::delivery_failed(
            "cluster KV authority probe",
            NetworkDeliveryState::DefinitelyNotSent,
            "connect failed",
        );
        assert!(cluster_kv_forward_error_allows_next_authority(
            &definitely_not_sent,
            &preferred,
        ));

        for marker in [
            FORWARDED_CLUSTER_KV_NON_LEADER_DETAIL,
            FORWARDED_CLUSTER_KV_READ_NON_LEADER_DETAIL,
            FORWARDED_CLUSTER_KV_EXACT_READ_NON_LEADER_DETAIL,
            FORWARDED_CLUSTER_KV_POINT_READ_NON_LEADER_DETAIL,
            FORWARDED_CLUSTER_KV_PREFIX_READ_NON_LEADER_DETAIL,
        ] {
            let rejection = verified_cluster_non_leader_error(
                &preferred,
                NetworkError::ClusterReadOnly {
                    detail: marker.to_string(),
                },
            );
            let mut retained = None;
            retain_verified_non_leader_rejected_preferred_authority(
                &mut retained,
                &rejection,
                Some(&preferred),
                &preferred,
            );
            let terminal = cluster_kv_forward_terminal_error(
                retained,
                format!("safe exhaustion after {definitely_not_sent}"),
            );
            assert!(matches!(
                terminal,
                NetworkError::VerifiedClusterNonLeader { node_id, .. }
                    if node_id == preferred
            ));
        }
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
            state: &ClusterBootstrapState,
            _target_node_id: &NodeId,
            request: &ObjectRpcRequest,
            _timeout: Duration,
        ) -> Result<ObjectRpcResponse, NetworkError> {
            self.calls = self.calls.saturating_add(1);
            let required_quorum = sinomo_kv::cluster_kv_raft_quorum_size_for_state(state);
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
                        term: state.term,
                        commit_index: 1,
                        revision: snapshot.revision,
                        basis: KvAuthorityBasis::CurrentTermQuorum {
                            voter_acks: required_quorum,
                            required_quorum,
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
                            term: state.term,
                            commit_index: 1,
                            revision: KvRevision(1),
                            basis: KvAuthorityBasis::CurrentTermQuorum {
                                voter_acks: required_quorum,
                                required_quorum,
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

    struct EarlyNonLeaderHost {
        known_authority: Option<NodeId>,
        calls: usize,
    }

    impl ClusterKvTxnForwardHost for EarlyNonLeaderHost {
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
            request: &ObjectRpcRequest,
            _timeout: Duration,
        ) -> Result<ObjectRpcResponse, NetworkError> {
            self.calls = self.calls.saturating_add(1);
            let detail = if request.route == sinomo_kv::CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE {
                FORWARDED_CLUSTER_KV_READ_NON_LEADER_DETAIL
            } else {
                FORWARDED_CLUSTER_KV_NON_LEADER_DETAIL
            };
            Err(NetworkError::ClusterReadOnly {
                detail: detail.to_string(),
            })
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

    #[tokio::test]
    async fn busy_operation_gate_falls_through_to_exact_voter_targets_promptly() {
        let (root, state) = forward_test_state(true);
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let store = paths.kv_paths().raft_store();
        let held = store.lock_operation().expect("hold Raft operation gate");
        let timeout = Duration::from_secs(5);
        let started = Instant::now();

        let selected = cluster_kv_forward_authority_targets_before_deadline(
            &tokio::runtime::Handle::current(),
            &paths,
            &state,
            cluster_kv_forward_deadline(timeout),
            timeout,
            "test exact voter dispatch",
        )
        .await
        .expect("busy advisory hint falls through")
        .expect("busy local voter falls through to an exact remote voter target");

        assert!(
            started.elapsed() < Duration::from_millis(100),
            "best-effort authority lookup must not consume the five-second request budget"
        );
        assert_eq!(selected.0, vec![state.members[1].node_id.clone()]);
        assert_eq!(selected.1, None);
        drop(held);
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

    #[test]
    fn nonleader_point_and_exact_set_forward_dispatch_bypasses_dynamic_kv_projection() {
        let _registry_guard = crate::kv_read_model::TEST_NETWORK_MODULE_REGISTRY_LOCK
            .lock()
            .expect("network module registry lock");
        let (root, mut state) = forward_test_state(false);
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        data_dir.ensure_root_layout().expect("data root");
        let identity = sinomo_node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main node identity");
        // Routed-session setup validates encoded public keys before asking the
        // test host to open a stream. One valid fixture key is sufficient: the
        // host deliberately stops before a transport handshake.
        for member in &mut state.members {
            member.node_public_key = identity.public_key.clone();
        }
        let remote_node_id = state.members[1].node_id.clone();
        state.members[1].endpoints[0] =
            sinomo_network::daemon_lan_endpoint(&remote_node_id, "127.0.0.1:1".to_string(), 0);
        let shadow_node_id = sinomo_node::shadow_daemon_node_id(&remote_node_id);
        let mut folded_shadow_endpoint =
            sinomo_network::daemon_lan_endpoint(&shadow_node_id, "127.0.0.1:2".to_string(), 0);
        folded_shadow_endpoint.node_id = remote_node_id.clone();
        state.members[1]
            .endpoints
            .push(folded_shadow_endpoint.clone());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        crate::local_state_store::write_cluster_state(&paths, &state).expect("cluster state");
        let remote_endpoint = state.members[1]
            .endpoints
            .first()
            .expect("remote route endpoint")
            .clone();
        sinomo_network::record_local_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &remote_endpoint,
            sinomo_network::ClusterConnectivityState::Healthy,
            Some(1),
            false,
        )
        .expect("healthy remote route");
        sinomo_network::record_local_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &folded_shadow_endpoint,
            sinomo_network::ClusterConnectivityState::Healthy,
            Some(0),
            false,
        )
        .expect("faster folded shadow route");

        sinomo_kv::evict_cluster_kv_cached_snapshot_arc_for_test(&paths.kv_paths());
        crate::kv_local_store::reset_cluster_kv_cached_snapshot_reads_for_test(&paths);

        let dynamic_reads = Arc::new(AtomicUsize::new(0));
        let provider_active = Arc::new(AtomicBool::new(true));
        let _provider_activation = DynamicRouteProviderActivation(Arc::clone(&provider_active));
        let owner_thread = std::thread::current().id();
        let provider_reads = Arc::clone(&dynamic_reads);
        sinomo_network::set_cluster_directory_provider(Arc::new(move |_| {
            let directory: Arc<dyn sinomo_network::ClusterDirectory> = if provider_active
                .load(Ordering::Acquire)
                && std::thread::current().id() == owner_thread
            {
                Arc::new(CountingDynamicRouteDirectory {
                    reads: Arc::clone(&provider_reads),
                })
            } else {
                Arc::new(sinomo_network::EmptyClusterDirectory)
            };
            directory
        }));

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut point_host = FencedForwardDispatchHost;
        let point = runtime.block_on(
            forward_cluster_kv_authoritative_entry_to_state_authority_async_with_host(
                runtime.handle(),
                &paths,
                &state,
                "/sifs/authority",
                forward_options(),
                &mut point_host,
            ),
        );
        let point_error = point.expect_err("closed test endpoint must reject the point forward");
        assert!(
            point_error.to_string().contains("after 1 attempts"),
            "point forward must exclude the faster folded shadow endpoint that does not host KV authority: {point_error}"
        );

        state.members[1].endpoints.truncate(1);
        state.members[1].endpoints[0] =
            sinomo_network::daemon_lan_endpoint(&remote_node_id, "127.0.0.1:2".to_string(), 0);
        crate::local_state_store::write_cluster_state(&paths, &state)
            .expect("updated exact-set route state");
        let exact_endpoint = state.members[1].endpoints[0].clone();
        sinomo_network::record_local_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &exact_endpoint,
            sinomo_network::ClusterConnectivityState::Healthy,
            Some(1),
            false,
        )
        .expect("restore healthy route after deliberate point failure");
        let mut exact_host = FencedForwardDispatchHost;
        let exact = runtime.block_on(
            forward_cluster_kv_authoritative_entries_to_state_authority_async_with_host(
                runtime.handle(),
                &paths,
                &state,
                &["/sifs/authority".to_string(), "/sifs/config".to_string()],
                forward_options(),
                &mut exact_host,
            ),
        );
        let exact_error =
            exact.expect_err("closed test endpoint must reject the exact-set forward");
        assert!(
            exact_error.to_string().contains("after 1 attempts"),
            "exact-set forward must reach one concrete routed transport attempt: {exact_error}"
        );

        assert_eq!(
            dynamic_reads.load(Ordering::SeqCst),
            0,
            "forward target selection and routing must not read the dynamic KV directory"
        );
        assert_eq!(
            crate::kv_local_store::cluster_kv_cached_snapshot_reads_for_test(&paths),
            0,
            "point/exact-set forwarding must not materialize the cached full snapshot"
        );
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
    fn follower_point_forward_uses_exact_route_despite_prefix_collision_limit() {
        let (_root, mut state) = forward_test_state(false);
        add_second_remote_voter(&mut state);
        state.term = 4;
        let key = "/agents";
        let prefix_colliders = (0..=sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
            .map(|index| forwarded_range_entry(&format!("{key}/{index:05}"), 1, 6))
            .collect();
        assert!(
            validate_forwarded_authoritative_range_read(
                forwarded_range_read(4, 6, 2, 2, prefix_colliders),
                &state,
                key,
            )
            .is_err(),
            "the old prefix-based point-forwarding path must exceed its range limit"
        );

        let exact = KvAuthoritativeRead {
            authority: KvAuthorityProof {
                term: 4,
                commit_index: 7,
                revision: KvRevision(6),
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
            value: Some(forwarded_range_entry(key, 1, 6)),
        };
        let mut wrong_key = exact.clone();
        wrong_key.value = Some(forwarded_range_entry("/agents/00000", 1, 6));
        assert!(
            validate_forwarded_authoritative_entry_read(wrong_key, &state, key).is_err(),
            "an exact-entry response cannot substitute a prefix-colliding key"
        );
        assert_eq!(
            validate_forwarded_authoritative_entry_read(exact, &state, key)
                .expect("exact forwarded point read")
                .value
                .expect("point entry")
                .key,
            key
        );

        let call = cluster_kv_authoritative_entry_forward_call(key)
            .expect("build exact-entry forward call");
        assert_eq!(
            call.request.route,
            sinomo_kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE
        );
        assert_ne!(
            call.request.route,
            sinomo_kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE
        );
        assert!(call.request.is_response_retry_safe());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let body = runtime
            .block_on(call.body.collect_to_vec(64 * 1024))
            .expect("collect exact-entry forward body");
        assert_eq!(
            sinomo_kv::decode_cluster_kv_authoritative_entry_rpc_request_body(&body)
                .expect("decode exact-entry request")
                .key,
            key
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
            cluster_kv_forward_deadline(Duration::from_secs(1)),
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
    fn kv_forward_cold_local_snapshot_skips_hydration_and_fetches_authority() {
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
        sinomo_kv::evict_cluster_kv_cached_snapshot_arc_for_test(&paths.kv_paths());
        assert!(
            crate::kv_local_store::read_cluster_kv_cached_snapshot_if_hot(&paths)
                .expect("hot-only snapshot probe")
                .is_none(),
            "the fixture must exercise a durable-but-cold local snapshot"
        );

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
            cluster_kv_forward_deadline(Duration::from_secs(1)),
            &mut host,
        )
        .expect("cold local cache falls back to the authority snapshot");

        assert_eq!(
            host.calls, 2,
            "a cold local snapshot must remain cold and use the bounded authority fetch"
        );
        assert_eq!(result.request_id, request.request_id);
        assert_eq!(result.snapshot.revision, KvRevision(1));
    }

    #[test]
    fn async_forward_deadline_aborts_queued_blocking_materialization() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("runtime");

        runtime.block_on(async {
            let blocker_started = std::sync::Arc::new(std::sync::Barrier::new(2));
            let release_blocker = std::sync::Arc::new(std::sync::Barrier::new(2));
            let blocking_started = std::sync::Arc::clone(&blocker_started);
            let blocking_release = std::sync::Arc::clone(&release_blocker);
            let blocker = tokio::task::spawn_blocking(move || {
                blocking_started.wait();
                blocking_release.wait();
            });
            blocker_started.wait();

            let ran_after_deadline = std::sync::Arc::new(AtomicBool::new(false));
            let task_ran = std::sync::Arc::clone(&ran_after_deadline);
            let timeout = Duration::from_millis(20);
            let error = run_forward_blocking_before_deadline(
                cluster_kv_forward_deadline(timeout),
                timeout,
                move || {
                    task_ran.store(true, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await
            .expect_err("queued materialization must obey the forwarding deadline");

            assert_eq!(
                error.delivery_state(),
                Some(NetworkDeliveryState::PossiblyApplied)
            );
            release_blocker.wait();
            blocker.await.expect("blocking-pool occupant");
            tokio::task::yield_now().await;
            assert!(
                !ran_after_deadline.load(Ordering::SeqCst),
                "timed-out queued blocking work must not run later"
            );
        });
    }

    #[test]
    fn async_forward_deadline_awaits_admitted_snapshot_materialization() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");

        runtime.block_on(async {
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let timeout = Duration::from_millis(20);
            let materialization = tokio::spawn(async move {
                run_forward_admitted_snapshot_materialization(
                    cluster_kv_forward_deadline(timeout),
                    timeout,
                    move || {
                        entered_tx.send(()).map_err(|_| NetworkError::Io {
                            operation: "test forwarded snapshot admission".to_string(),
                            detail: "test observer dropped".to_string(),
                        })?;
                        release_rx.recv().map_err(|source| NetworkError::Io {
                            operation: "test forwarded snapshot admission".to_string(),
                            detail: source.to_string(),
                        })?;
                        Ok(7_u8)
                    },
                )
                .await
            });

            entered_rx.await.expect("snapshot clone admitted");
            tokio::time::sleep(timeout.saturating_mul(2)).await;
            assert!(
                !materialization.is_finished(),
                "an admitted full-snapshot materialization must not detach at the deadline"
            );
            release_tx.send(()).expect("release admitted clone");
            assert_eq!(
                materialization
                    .await
                    .expect("join materialization")
                    .expect("admitted materialization result"),
                7
            );
        });
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
            cluster_kv_forward_deadline(Duration::from_secs(1)),
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
            cluster_kv_forward_deadline(Duration::from_secs(1)),
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
    fn voter_without_known_authority_probes_exact_voters_before_campaigning() {
        let (root, mut state) = forward_test_state(true);
        add_second_remote_voter(&mut state);
        state.term = 41;
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let mut host = NonLeaderThenCommittedHost {
            known_authority: None,
            calls: 0,
        };
        let term_before_forward = state.term;

        let read = forward_cluster_kv_authoritative_snapshot_to_state_authority_with_host(
            &paths,
            &state,
            forward_options(),
            &mut host,
        )
        .expect("probe exact voters after a verified non-leader rejection")
        .expect("remote leader authoritative read");

        assert_eq!(host.calls, 2);
        assert_eq!(read.value.revision, KvRevision(1));
        assert_eq!(state.term, term_before_forward);
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
    fn stale_preferred_authority_falls_through_to_new_leader_without_campaigning() {
        let (root, mut state) = forward_test_state(true);
        add_second_remote_voter(&mut state);
        let preferred = state.members[1].node_id.clone();
        state.term = 57;
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let request = KvTxnRequest::generated(test_txn());
        let mut host = NonLeaderThenCommittedHost {
            known_authority: Some(preferred),
            calls: 0,
        };
        let term_before_forward = state.term;

        let response = forward_cluster_kv_txn_to_state_authority_with_host(
            &paths,
            &state,
            &request,
            forward_options(),
            &mut host,
        )
        .expect("probe another exact voter after stale preferred hint")
        .expect("new remote leader committed response");

        assert_eq!(host.calls, 3);
        assert_eq!(response.request_id, request.request_id);
        assert_eq!(state.term, term_before_forward);
    }

    #[test]
    fn host_level_txn_non_leader_rejections_exhaust_exact_voters() {
        let (root, mut state) = forward_test_state(false);
        add_second_remote_voter(&mut state);
        let preferred = state.members[1].node_id.clone();
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let mut host = EarlyNonLeaderHost {
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
        .expect_err("all exact voters reject the forwarded transaction");

        assert_eq!(host.calls, 2);
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

    #[test]
    fn stale_preferred_authority_snapshot_read_falls_through_to_new_leader() {
        let (root, mut state) = forward_test_state(true);
        add_second_remote_voter(&mut state);
        let preferred = state.members[1].node_id.clone();
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let mut host = NonLeaderThenCommittedHost {
            known_authority: Some(preferred),
            calls: 0,
        };

        let read = forward_cluster_kv_authoritative_snapshot_to_state_authority_with_host(
            &paths,
            &state,
            forward_options(),
            &mut host,
        )
        .expect("probe another exact voter after stale preferred read hint")
        .expect("new remote leader authoritative read");

        assert_eq!(host.calls, 2);
        assert_eq!(read.value.revision, KvRevision(1));
    }

    #[test]
    fn host_level_snapshot_fetch_rejection_retains_target_identity() {
        let (_root, state) = forward_test_state(false);
        let target = state.members[1].node_id.clone();
        let mut host = EarlyNonLeaderHost {
            known_authority: None,
            calls: 0,
        };

        let error = fetch_cluster_kv_snapshot_from_node_with_host(
            &state,
            &target,
            Duration::from_secs(1),
            &mut host,
        )
        .expect_err("host-level snapshot fetch rejection must preserve its identity");

        assert_eq!(host.calls, 1);
        assert!(matches!(
            error,
            NetworkError::VerifiedClusterNonLeader { node_id, .. } if node_id == target
        ));
    }
}
