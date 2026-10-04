use crate::control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
use crate::control_plane_options::ClusterKvTxnQuorumOptions;
use crate::kv_forward::{
    ClusterKvTxnForwardOptions, FORWARDED_CLUSTER_KV_EXACT_READ_NON_LEADER_DETAIL,
    FORWARDED_CLUSTER_KV_POINT_READ_NON_LEADER_DETAIL,
    FORWARDED_CLUSTER_KV_PREFIX_READ_NON_LEADER_DETAIL,
    FORWARDED_CLUSTER_KV_READ_NON_LEADER_DETAIL,
    forward_cluster_kv_authoritative_entries_to_state_authority_async_with_host,
    forward_cluster_kv_authoritative_entry_to_state_authority_async_with_host,
    forward_cluster_kv_authoritative_range_prefix_to_state_authority_async_with_host,
    forward_cluster_kv_authoritative_snapshot_to_state_authority_async_with_host,
    forward_cluster_kv_authoritative_snapshot_to_state_authority_with_host,
    forward_cluster_kv_request_outcome_to_state_authority_async_with_host,
    forward_cluster_kv_txn_compact_to_state_authority_async_with_host,
    forward_cluster_kv_txn_to_state_authority_async_with_host,
    forward_cluster_kv_txn_to_state_authority_with_host,
    validate_forwarded_authoritative_entries_read, validate_forwarded_authoritative_snapshot_read,
};
use crate::kv_local_store::ClusterKvPathsExt;
use crate::kv_local_store::kv_err;
use crate::kv_local_store::{
    LocalClusterRaftCommandStorage, LocalClusterRaftReadProjection,
    imported_snapshot_advances_local_kv, locked_kv_state_authority_id,
    write_imported_cluster_kv_snapshot_if_changed,
};
use crate::kv_snapshot::refresh_current_cluster_kv_store_with_host;
#[cfg(any(test, feature = "test-support"))]
use crate::local_state_store::read_cluster_state_without_kv;
use crate::local_state_store::{
    project_cluster_raft_config_record, read_cluster_state_without_kv_async,
};
use crate::prelude::*;
use crate::raft_election::{
    ClusterRaftVoteTransportAsyncHost, elect_cluster_raft_leader_async_with_host,
    elect_cluster_raft_leader_with_host,
};
use crate::raft_quorum::{
    ClusterKvQuorumAsyncHost, ClusterRaftBlockingCancellationPolicy,
    ClusterRaftBlockingDeadlinePolicy, ClusterRaftReadIndexAdmissionMode,
    ClusterRaftReadIndexAttempt, ClusterRaftReadIndexAuthorityFence,
    ClusterRaftReadProjectionPolicy,
    commit_cluster_raft_command_compact_with_quorum_async_with_host,
    commit_cluster_raft_command_with_control_plane_driver,
    commit_cluster_raft_command_with_quorum_async_with_host,
    read_cluster_raft_with_read_index_async_with_projection_policy,
    read_cluster_raft_with_read_index_attempt_async_with_host_and_admission,
    read_cluster_raft_with_read_index_attempt_async_with_projection_policy_and_admission,
    read_cluster_raft_with_read_index_attempt_with_transport, run_cluster_raft_supervised_blocking,
};
use crate::state_authority_reachability::{
    ClusterStateAuthorityReachabilityAsyncHost, ClusterStateAuthorityReachabilityOptions,
    cluster_reachable_state_authority_count_async_with_host,
    cluster_reachable_state_authority_count_with_host,
};
#[cfg(test)]
use sinomo_kv::KvAuthorityBasis;
#[allow(unused_imports)]
use sinomo_kv::KvError;
use sinomo_kv::{
    ClusterKvRaftQuorumCommitOptions, ClusterKvRaftQuorumCommitResult,
    ClusterKvStateAuthorityVoteRequest, ClusterKvStateAuthorityVoteResponse,
    ClusterKvTxnCommitResult, ClusterKvTxnRpcResponse,
};
use sinomo_kv::{KvAuthoritativeRead, KvCommand};
use sinomo_kv::{KvCommandRequest, KvSnapshot, KvTxnRequest};
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterObjectRpcAsyncHost;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{ObjectRpcRequest, ObjectRpcResponse};
use std::collections::BTreeMap;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
#[cfg(not(test))]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

pub trait ClusterKvTxnQuorumHost {
    fn refresh_current_cluster_kv_store(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        Ok(())
    }

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

    /// Send the exact empty AppendEntries heartbeat used by synchronous
    /// ReadIndex. This stays at the cluster bridge: KV owns the Raft payload,
    /// networking owns delivery, and neither crate depends on the other.
    fn send_cluster_kv_read_index_probe(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &sinomo_kv::ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<sinomo_kv::ClusterRaftAppendResponse, NetworkError> {
        let payload = sinomo_kv::cluster_raft_append_rpc_payload(request).map_err(kv_err)?;
        let rpc_request =
            ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body)
                .with_response_retry_safe();
        let response = self.call_node_object_rpc(state, target_node_id, &rpc_request, timeout)?;
        response.error_detail()?;
        sinomo_kv::decode_cluster_raft_append_rpc_response_body(&response.body).map_err(kv_err)
    }

    /// Dispatch synchronous ReadIndex heartbeats concurrently. Production
    /// transports must override this batch seam; the fail-closed default
    /// prevents a hidden sequential implementation from letting one dead voter
    /// consume the whole authority deadline.
    fn send_cluster_kv_read_index_probes(
        &mut self,
        _state: &ClusterBootstrapState,
        targets: Vec<NodeId>,
        _request: &sinomo_kv::ClusterRaftAppendRequest,
        _timeout: Duration,
        _required_acks: usize,
    ) -> Result<
        Vec<(
            NodeId,
            Result<sinomo_kv::ClusterRaftAppendResponse, NetworkError>,
        )>,
        NetworkError,
    > {
        Ok(targets
            .into_iter()
            .map(|target| {
                (
                    target,
                    Err(NetworkError::ClusterReadOnly {
                        detail: "synchronous ReadIndex batch transport is unavailable".to_string(),
                    }),
                )
            })
            .collect())
    }

    fn local_is_kv_state_authority(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError>;

    fn request_kv_state_authority_vote(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ClusterKvStateAuthorityVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterKvStateAuthorityVoteResponse, NetworkError>;

    /// Dispatch one election's vote requests as a batch. Production
    /// transports must override this seam with concurrent delivery. The
    /// default is deliberately fail-closed so a missing override cannot
    /// silently serialize the authority deadline across voters.
    fn request_kv_state_authority_votes(
        &mut self,
        _state: &ClusterBootstrapState,
        targets: Vec<NodeId>,
        _request: &ClusterKvStateAuthorityVoteRequest,
        _timeout: Duration,
        _required_grants: usize,
    ) -> Result<
        Vec<(
            NodeId,
            Result<ClusterKvStateAuthorityVoteResponse, NetworkError>,
        )>,
        NetworkError,
    > {
        Ok(targets
            .into_iter()
            .map(|target| {
                (
                    target,
                    Err(NetworkError::ClusterReadOnly {
                        detail: "synchronous Raft vote batch transport is unavailable".to_string(),
                    }),
                )
            })
            .collect())
    }

    fn forward_cluster_kv_txn_to_state_authority(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        request: &KvTxnRequest,
        options: ClusterKvTxnQuorumOptions,
    ) -> Result<Option<ClusterKvTxnCommitResult>, NetworkError>
    where
        Self: Sized,
    {
        forward_cluster_kv_txn_to_state_authority_with_host(
            paths,
            state,
            request,
            options.forward_options,
            self,
        )
    }

    fn elect_kv_state_authority(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        options: ClusterKvTxnQuorumOptions,
    ) -> Result<bool, NetworkError>
    where
        Self: Sized,
    {
        elect_cluster_raft_leader_with_host(paths, state, options.leader_election_options, self)
    }

    fn reachable_cluster_state_authority_count(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        options: ClusterStateAuthorityReachabilityOptions,
    ) -> usize;

    fn commit_cluster_kv_command_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvCommandRequest,
        options: ClusterKvRaftQuorumCommitOptions,
    ) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError>;
}

impl<T> crate::kv_forward::ClusterKvTxnForwardHost for T
where
    T: ClusterKvTxnQuorumHost,
{
    fn known_kv_state_authority_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError> {
        ClusterKvTxnQuorumHost::known_kv_state_authority_id(self, paths, state)
    }

    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        ClusterKvTxnQuorumHost::call_node_object_rpc(self, state, target_node_id, request, timeout)
    }
}

impl<T> crate::raft_election::ClusterRaftVoteTransportHost for T
where
    T: ClusterKvTxnQuorumHost,
{
    #[cfg(any(test, feature = "test-support"))]
    fn send_raft_vote(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &sinomo_kv::ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> Result<sinomo_kv::ClusterRaftVoteResponse, NetworkError> {
        ClusterKvTxnQuorumHost::request_kv_state_authority_vote(
            self,
            state,
            target_node_id,
            request,
            timeout,
        )
    }

    fn send_raft_votes(
        &mut self,
        state: &ClusterBootstrapState,
        targets: Vec<NodeId>,
        request: &sinomo_kv::ClusterRaftVoteRequest,
        timeout: Duration,
        required_grants: usize,
    ) -> Result<
        Vec<(
            NodeId,
            Result<sinomo_kv::ClusterRaftVoteResponse, NetworkError>,
        )>,
        NetworkError,
    > {
        ClusterKvTxnQuorumHost::request_kv_state_authority_votes(
            self,
            state,
            targets,
            request,
            timeout,
            required_grants,
        )
    }
}

pub trait ClusterKvTxnQuorumAsyncHost:
    ClusterObjectRpcAsyncHost
    + ClusterKvQuorumAsyncHost
    + ClusterStateAuthorityReachabilityAsyncHost
    + Send
{
    fn forward_cluster_kv_authoritative_entry_async<'a>(
        &'a mut self,
        runtime: &'a tokio::runtime::Handle,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        key: &'a str,
        options: ClusterKvTxnForwardOptions,
    ) -> ClusterMembershipRpcFuture<'a, Option<KvAuthoritativeRead<Option<sinomo_kv::KvEntry>>>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            forward_cluster_kv_authoritative_entry_to_state_authority_async_with_host(
                runtime, paths, state, key, options, self,
            )
            .await
        })
    }

    fn forward_cluster_kv_authoritative_entries_async<'a>(
        &'a mut self,
        runtime: &'a tokio::runtime::Handle,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        keys: &'a [String],
        options: ClusterKvTxnForwardOptions,
    ) -> ClusterMembershipRpcFuture<'a, Option<KvAuthoritativeRead<Vec<Option<sinomo_kv::KvEntry>>>>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            forward_cluster_kv_authoritative_entries_to_state_authority_async_with_host(
                runtime, paths, state, keys, options, self,
            )
            .await
        })
    }

    fn forward_cluster_kv_authoritative_range_prefix_async<'a>(
        &'a mut self,
        runtime: &'a tokio::runtime::Handle,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        prefix: &'a str,
        options: ClusterKvTxnForwardOptions,
    ) -> ClusterMembershipRpcFuture<'a, Option<KvAuthoritativeRead<Vec<sinomo_kv::KvEntry>>>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            forward_cluster_kv_authoritative_range_prefix_to_state_authority_async_with_host(
                runtime, paths, state, prefix, options, self,
            )
            .await
        })
    }

    fn forward_cluster_kv_authoritative_snapshot_async<'a>(
        &'a mut self,
        runtime: &'a tokio::runtime::Handle,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        options: ClusterKvTxnForwardOptions,
    ) -> ClusterMembershipRpcFuture<'a, Option<KvAuthoritativeRead<KvSnapshot>>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            forward_cluster_kv_authoritative_snapshot_to_state_authority_async_with_host(
                runtime, paths, state, options, self,
            )
            .await
        })
    }

    fn request_kv_state_authority_vote_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterKvStateAuthorityVoteRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvStateAuthorityVoteResponse>;

    /// Async batch counterpart of `request_kv_state_authority_votes`.
    /// Production networking must override it with concurrent fanout.
    fn request_kv_state_authority_votes_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        targets: Vec<NodeId>,
        _request: &'a ClusterKvStateAuthorityVoteRequest,
        _timeout: Duration,
        _required_grants: usize,
    ) -> ClusterMembershipRpcFuture<
        'a,
        Vec<(
            NodeId,
            Result<ClusterKvStateAuthorityVoteResponse, NetworkError>,
        )>,
    > {
        Box::pin(async move {
            Ok(targets
                .into_iter()
                .map(|target| {
                    (
                        target,
                        Err(NetworkError::ClusterReadOnly {
                            detail: "asynchronous Raft vote batch transport is unavailable"
                                .to_string(),
                        }),
                    )
                })
                .collect())
        })
    }
}

impl<T> ClusterRaftVoteTransportAsyncHost for T
where
    T: ClusterKvTxnQuorumAsyncHost,
{
    fn send_raft_votes_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        targets: Vec<NodeId>,
        request: &'a sinomo_kv::ClusterRaftVoteRequest,
        timeout: Duration,
        required_grants: usize,
    ) -> ClusterMembershipRpcFuture<
        'a,
        Vec<(
            NodeId,
            Result<sinomo_kv::ClusterRaftVoteResponse, NetworkError>,
        )>,
    > {
        ClusterKvTxnQuorumAsyncHost::request_kv_state_authority_votes_async(
            self,
            state,
            targets,
            request,
            timeout,
            required_grants,
        )
    }
}

impl<T> ClusterKvTxnQuorumHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn refresh_current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        refresh_current_cluster_kv_store_with_host(
            paths,
            state,
            self.timeouts.kv_snapshot_refresh_options(),
            self,
        )
        .map(|_| ())
    }

    fn known_kv_state_authority_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError> {
        locked_kv_state_authority_id(paths, state)
    }

    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.transport
            .call_node_object_rpc(state, target_node_id, request, timeout)
    }

    fn send_cluster_kv_read_index_probes(
        &mut self,
        state: &ClusterBootstrapState,
        targets: Vec<NodeId>,
        request: &sinomo_kv::ClusterRaftAppendRequest,
        timeout: Duration,
        required_acks: usize,
    ) -> Result<
        Vec<(
            NodeId,
            Result<sinomo_kv::ClusterRaftAppendResponse, NetworkError>,
        )>,
        NetworkError,
    > {
        self.transport
            .send_kv_read_index_probes(state, targets, request, timeout, required_acks)
    }

    fn request_kv_state_authority_vote(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ClusterKvStateAuthorityVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterKvStateAuthorityVoteResponse, NetworkError> {
        self.transport.request_kv_state_authority_vote_from_node(
            state,
            target_node_id,
            request,
            timeout,
        )
    }

    fn request_kv_state_authority_votes(
        &mut self,
        state: &ClusterBootstrapState,
        targets: Vec<NodeId>,
        request: &ClusterKvStateAuthorityVoteRequest,
        timeout: Duration,
        required_grants: usize,
    ) -> Result<
        Vec<(
            NodeId,
            Result<ClusterKvStateAuthorityVoteResponse, NetworkError>,
        )>,
        NetworkError,
    > {
        self.transport.request_kv_state_authority_votes_from_nodes(
            state,
            targets,
            request,
            timeout,
            required_grants,
        )
    }

    fn local_is_kv_state_authority(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError> {
        locked_cluster_raft_local_is_leader(paths, state)
    }

    fn reachable_cluster_state_authority_count(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        options: ClusterStateAuthorityReachabilityOptions,
    ) -> usize {
        cluster_reachable_state_authority_count_with_host(paths, state, options, self)
    }

    fn commit_cluster_kv_command_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvCommandRequest,
        options: ClusterKvRaftQuorumCommitOptions,
    ) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError> {
        commit_cluster_raft_command_with_control_plane_driver(paths, state, request, options, self)
    }
}

struct NetworkClusterKvTxnQuorumHost<'a, H> {
    paths: &'a ClusterPaths,
    options: ClusterKvTxnQuorumOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvTxnQuorumHost for NetworkClusterKvTxnQuorumHost<'_, H>
where
    H: ClusterKvTxnQuorumHost,
{
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn refresh_current_cluster_kv_store(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<(), Self::Error> {
        self.host
            .refresh_current_cluster_kv_store(self.paths, state)
    }

    fn local_is_raft_leader(&mut self, state: &ClusterBootstrapState) -> Result<bool, Self::Error> {
        self.host.local_is_kv_state_authority(self.paths, state)
    }

    fn forward_cluster_kv_txn_to_voter(
        &mut self,
        state: &ClusterBootstrapState,
        request: &KvTxnRequest,
    ) -> Result<Option<sinomo_kv::ClusterKvTxnCommitResult>, Self::Error> {
        let deadline = Instant::now() + self.options.forward_options.timeout;
        let leader_fence = try_capture_cluster_raft_voter_leader_fence(self.paths, state);
        let result = self.host.forward_cluster_kv_txn_to_state_authority(
            self.paths,
            state,
            request,
            self.options,
        );
        if let Err(error) = &result
            && clear_verified_stale_cluster_raft_leader(
                self.paths,
                state,
                leader_fence.as_ref(),
                error,
            )?
            && let Some(forward_options) =
                cluster_kv_forward_options_before_deadline(self.options.forward_options, deadline)
        {
            let mut retry_options = self.options;
            retry_options.forward_options = forward_options;
            return self.host.forward_cluster_kv_txn_to_state_authority(
                self.paths,
                state,
                request,
                retry_options,
            );
        }
        result
    }

    fn elect_cluster_raft_leader(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, Self::Error> {
        self.host
            .elect_kv_state_authority(self.paths, state, self.options)
    }

    fn reachable_voter_count(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<usize, Self::Error> {
        Ok(self.host.reachable_cluster_state_authority_count(
            self.paths,
            state,
            self.options.state_authority_reachability_options,
        ))
    }

    fn commit_cluster_raft_txn_with_quorum(
        &mut self,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<sinomo_kv::ClusterKvRaftQuorumCommitResult, Self::Error> {
        self.host.commit_cluster_kv_command_with_quorum(
            self.paths,
            state,
            request.into_command_request(),
            ClusterKvRaftQuorumCommitOptions {
                timeout: self.options.raft_append_timeout,
            },
        )
    }

    fn cluster_quorum_commit_error_allows_retry(&mut self, error: &Self::Error) -> bool {
        matches!(error, NetworkError::ClusterReadOnly { .. })
    }

    fn cluster_authority_error(&mut self, failure: sinomo_kv::KvAuthorityFailure) -> Self::Error {
        NetworkError::ClusterReadOnly {
            detail: serde_json::to_string(&failure).unwrap_or_else(|_| format!("{failure:?}")),
        }
    }
}

pub fn commit_cluster_kv_txn_with_quorum_response_with_host(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvTxnRequest,
    options: ClusterKvTxnQuorumOptions,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<ClusterKvTxnCommitResult, NetworkError> {
    let started = Instant::now();
    let deadline = started
        .checked_add(options.raft_append_timeout)
        .unwrap_or(started);
    let mut consensus_state = state.clone();
    project_committed_cluster_raft_authority_before_sync_command(
        paths,
        &mut consensus_state,
        deadline,
    )?;
    // The authority floor is a durable observation and remains valid even if
    // forwarding/election/quorum work below fails. Preserve it so retries do
    // not repeatedly validate a stale bootstrap term.
    *state = consensus_state.clone();
    let allow_forwarding = options.allow_forwarding;
    let mut kv_host = NetworkClusterKvTxnQuorumHost {
        paths,
        options,
        host,
    };
    let result = sinomo_kv::commit_cluster_kv_txn_with_quorum_with_host(
        &mut consensus_state,
        request,
        sinomo_kv::ClusterKvTxnQuorumDriverOptions { allow_forwarding },
        &mut kv_host,
    )?;
    state.term = state
        .term
        .max(consensus_state.term)
        .max(result.authority.term);
    state.kv_revision = state
        .kv_revision
        .max(consensus_state.kv_revision)
        .max(result.authority.revision.0);
    Ok(result)
}

/// Captures the synchronous command's durable authority floor from one
/// resident Raft view.
///
/// Bootstrap state is a topology carrier and can legitimately lag a leader
/// election. Projecting only `/cluster/raft/config` leaves that stale term in
/// place: the store-level leader hint can then say "local" while the command
/// session rejects `state.term != hard_state.current_term`. Read hard state,
/// applied revision, and config together before the generic quorum driver so
/// a healthy leader does not enter an election/retry loop with term zero.
fn project_committed_cluster_raft_authority_before_sync_command(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    deadline: Instant,
) -> Result<(), NetworkError> {
    remaining_authoritative_read_budget(deadline, "synchronous command authority projection")?;
    let mut projected = state.clone();
    let mut storage = LocalClusterRaftCommandStorage::new_with_operation_deadline(
        paths,
        projected.local_node_id.clone(),
        deadline,
    );
    let read_state = storage.read_index_state()?;
    projected.term = projected.term.max(read_state.current_term);
    projected.kv_revision = projected.kv_revision.max(read_state.revision.0);
    if let Some(entry) = storage.read_index_entry(&sinomo_kv::cluster_kv_raft_config_key())? {
        let config: sinomo_kv::ClusterKvRaftConfigRecord = entry.decode().map_err(kv_err)?;
        project_cluster_raft_config_record(&mut projected, &config)?;
    }
    remaining_authoritative_read_budget(deadline, "synchronous command authority projection")?;
    *state = projected;
    Ok(())
}

fn authoritative_read_from_commit(
    result: ClusterKvRaftQuorumCommitResult,
    state: &ClusterBootstrapState,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
    validate_forwarded_authoritative_read(
        KvAuthoritativeRead {
            value: result.snapshot,
            authority: result.authority,
        },
        state,
    )
}

fn remaining_authoritative_read_budget(
    deadline: Instant,
    operation: &'static str,
) -> Result<Duration, NetworkError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!("{operation} exhausted its Raft confirmation budget"),
        });
    }
    Ok(remaining)
}

const LOCAL_READ_INDEX_BUSY_RETRY_INTERVAL: Duration = Duration::from_millis(5);
const LOCAL_READ_INDEX_BUSY_RETRY_BUDGET: Duration = Duration::from_millis(5);
const BOUNDED_AUTHORITY_BUSY_RETRY_BUDGET: Duration = Duration::from_millis(250);
const RANGE_AUTHORITY_BUSY_RETRY_BUDGET: Duration = Duration::from_millis(250);

/// Rechecks local ReadIndex admission after a forwarding attempt had time to
/// finish the operation that made the first local probe busy.
///
/// A locally-originated read yields between short, bounded, non-queued probes.
/// That gives the in-flight owner a chance to clear at the first available
/// slot without letting background reads join the Raft operation FIFO ahead of
/// formation or mutation writes. Formation-sensitive request-outcome reads
/// use one interval; originating value reads (including snapshots) can use a
/// longer adaptive grace budget within their original deadline.
/// Already-forwarded requests never reach this helper, so they retain prompt
/// rejection and never campaign. Callers preserve the original forwarding
/// failure when this best-effort retry errors, exhausts its grace budget, or
/// reports NotLeader; only a newly established local authority result supersedes
/// it.
async fn retry_local_read_index_after_busy_forward<H, T, F>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    deadline: Instant,
    operation: &'static str,
    projection_policy: ClusterRaftReadProjectionPolicy,
    retry_budget: Duration,
    read_value: F,
    host: &mut H,
) -> Result<Option<ClusterRaftReadIndexAttempt<T>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
    T: Send + 'static,
    F: Clone
        + FnOnce(&mut dyn LocalClusterRaftReadProjection) -> Result<T, NetworkError>
        + Send
        + 'static,
{
    if !options.allow_forwarding || !sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        return Ok(None);
    }
    let remaining = remaining_authoritative_read_budget(deadline, operation)?;
    let retry_deadline = Instant::now() + remaining.min(retry_budget);
    loop {
        let retry_remaining = retry_deadline.saturating_duration_since(Instant::now());
        if retry_remaining.is_zero() {
            return Ok(Some(ClusterRaftReadIndexAttempt::LocalAdmissionBusy));
        }
        tokio::time::sleep(retry_remaining.min(LOCAL_READ_INDEX_BUSY_RETRY_INTERVAL)).await;
        let remaining = remaining_authoritative_read_budget(deadline, operation)?;
        let attempt =
            read_cluster_raft_with_read_index_attempt_async_with_projection_policy_and_admission(
                runtime,
                paths,
                state,
                remaining,
                options.raft_append_replication_options,
                projection_policy,
                ClusterRaftReadIndexAdmissionMode::ForwardOnBusy,
                read_value.clone(),
                host,
            )
            .await?;
        if !matches!(attempt, ClusterRaftReadIndexAttempt::LocalAdmissionBusy) {
            return Ok(Some(attempt));
        }
    }
}

fn authoritative_read_phase_timeout(operation: &str) -> NetworkError {
    NetworkError::ClusterReadOnly {
        detail: format!("{operation} exhausted its Raft confirmation budget"),
    }
}

async fn run_authoritative_read_blocking_before_deadline<T, F>(
    runtime: &tokio::runtime::Handle,
    deadline: Instant,
    phase: &'static str,
    operation: &'static str,
    work: F,
) -> Result<T, NetworkError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, NetworkError> + Send + 'static,
{
    remaining_authoritative_read_budget(deadline, phase)?;
    run_cluster_raft_supervised_blocking(
        runtime,
        Some((
            deadline,
            authoritative_read_phase_timeout(phase),
            ClusterRaftBlockingDeadlinePolicy::DeadlineBounded,
        )),
        ClusterRaftBlockingCancellationPolicy::CancelQueued,
        "barrier",
        phase,
        operation,
        work,
    )
    .await
}

fn finish_authoritative_projection_deadline(
    deadline: Instant,
    projection_policy: ClusterRaftReadProjectionPolicy,
    path: &'static str,
    operation: &'static str,
) -> Result<(), NetworkError> {
    match projection_policy {
        ClusterRaftReadProjectionPolicy::DeadlineBounded => {
            remaining_authoritative_read_budget(deadline, operation)?;
        }
        ClusterRaftReadProjectionPolicy::CompleteAdmittedSnapshot => {
            let overrun = Instant::now().saturating_duration_since(deadline);
            if !overrun.is_zero() {
                metrics::counter!(
                    "sinomo.kv.authoritative_read.projection_deadline_overrun",
                    "path" => path,
                    "projection" => "full_snapshot"
                )
                .increment(1);
                metrics::histogram!(
                    "sinomo.kv.authoritative_read.projection_deadline_overrun.us",
                    "path" => path,
                    "projection" => "full_snapshot"
                )
                .record(overrun.as_micros().min(u128::from(u64::MAX)) as f64);
            }
        }
    }
    Ok(())
}

type AuthoritativeBarrierConfigFence = Option<(String, Vec<u8>, u64, u64, u64, Option<u64>)>;

/// Identifies the one first-current-term no-op that can establish ReadIndex
/// for a particular leader and committed membership view.
///
/// A root is included because tests and embedded runtimes can host multiple
/// independent clusters in one process. The exact config entry and voter set
/// prevent a barrier confirmed by one membership from releasing readers that
/// prepared against another membership in the same term.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AuthoritativeBarrierSingleflightKey {
    cluster_root: PathBuf,
    leader_id: NodeId,
    leader_term: u64,
    config_entry: AuthoritativeBarrierConfigFence,
    voters: Vec<NodeId>,
}

impl AuthoritativeBarrierSingleflightKey {
    fn new(
        paths: &ClusterPaths,
        leader_id: NodeId,
        leader_term: u64,
        config_entry: Option<&sinomo_kv::KvEntry>,
        mut voters: Vec<NodeId>,
    ) -> Self {
        voters.sort();
        voters.dedup();
        Self {
            cluster_root: paths.root.clone(),
            leader_id,
            leader_term,
            config_entry: config_entry.map(|entry| {
                (
                    entry.key.clone(),
                    entry.value.clone(),
                    entry.create_revision.0,
                    entry.modify_revision.0,
                    entry.version.0,
                    entry.lease_id.map(|lease_id| lease_id.0),
                )
            }),
            voters,
        }
    }

    fn same_cluster_node(&self, other: &Self) -> bool {
        self.cluster_root == other.cluster_root && self.leader_id == other.leader_id
    }
}

#[derive(Clone, Debug)]
enum AuthoritativeBarrierSingleflightOutcome {
    Established,
    Refence,
    RetryFlight,
    Failed(NetworkError),
}

struct AuthoritativeBarrierSingleflight {
    /// Reusing the exact request identity is what makes owner cancellation and
    /// ambiguous delivery reclaim safe: the replacement owner reconciles or
    /// replays the same durable proposal instead of appending another no-op.
    request: KvCommandRequest,
    generation: u64,
    result: tokio::sync::watch::Sender<Option<AuthoritativeBarrierSingleflightOutcome>>,
    blocking_result: Mutex<Option<AuthoritativeBarrierSingleflightOutcome>>,
    blocking_changed: Condvar,
    #[cfg(test)]
    waiters: AtomicUsize,
}

impl AuthoritativeBarrierSingleflight {
    fn publish(&self, outcome: AuthoritativeBarrierSingleflightOutcome) {
        {
            let mut blocking_result = self
                .blocking_result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *blocking_result = Some(outcome.clone());
            self.blocking_changed.notify_all();
        }
        self.result.send_replace(Some(outcome));
    }

    fn clear_for_reclaim(&self) {
        {
            let mut blocking_result = self
                .blocking_result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *blocking_result = None;
        }
        self.result.send_replace(None);
    }
}

type AuthoritativeBarrierSingleflightRegistry =
    BTreeMap<AuthoritativeBarrierSingleflightKey, Arc<AuthoritativeBarrierSingleflight>>;

fn authoritative_barrier_singleflight_registry()
-> &'static Mutex<AuthoritativeBarrierSingleflightRegistry> {
    static REGISTRY: OnceLock<Mutex<AuthoritativeBarrierSingleflightRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn next_authoritative_barrier_singleflight_generation() -> u64 {
    static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
    NEXT_GENERATION.fetch_add(1, Ordering::Relaxed)
}

struct AuthoritativeBarrierSingleflightOwner {
    key: AuthoritativeBarrierSingleflightKey,
    flight: Arc<AuthoritativeBarrierSingleflight>,
    completed: bool,
}

impl AuthoritativeBarrierSingleflightOwner {
    fn request(&self) -> KvCommandRequest {
        self.flight.request.clone()
    }

    fn finish_established(mut self) {
        self.completed = true;
        self.flight
            .publish(AuthoritativeBarrierSingleflightOutcome::Established);

        // Retain the established result so a caller that prepared the same
        // cold fence just before publication cannot race in and append a
        // second barrier. Superseding terms/configurations evict older
        // completed entries; active owners remain reachable by their waiters.
        let Ok(mut registry) = authoritative_barrier_singleflight_registry().lock() else {
            return;
        };
        let generation = self.flight.generation;
        let newer_completed_exists = registry.iter().any(|(key, flight)| {
            key != &self.key
                && key.same_cluster_node(&self.key)
                && flight.generation > generation
                && flight.result.borrow().is_some()
        });
        if newer_completed_exists {
            if registry
                .get(&self.key)
                .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
            {
                registry.remove(&self.key);
            }
            return;
        }
        registry.retain(|key, flight| {
            key == &self.key
                || !key.same_cluster_node(&self.key)
                || flight.generation > generation
                || flight.result.borrow().is_none()
        });
    }

    fn finish_failed(mut self, error: NetworkError) -> NetworkError {
        self.completed = true;
        self.remove_from_registry();
        self.flight
            .publish(AuthoritativeBarrierSingleflightOutcome::Failed(
                error.clone(),
            ));
        error
    }

    fn finish_refence(mut self) {
        self.completed = true;
        // Remove the stale fence before waking its waiters. Every participant
        // must rebuild the durable term/configuration view instead of
        // reclaiming the obsolete request identity.
        self.remove_from_registry();
        self.flight
            .publish(AuthoritativeBarrierSingleflightOutcome::Refence);
    }

    fn publish_retry(mut self) {
        self.completed = true;
        // Keep this flight registered: the next waiter reclaims the same
        // request id and therefore preserves ambiguous-delivery semantics.
        self.flight
            .publish(AuthoritativeBarrierSingleflightOutcome::RetryFlight);
    }

    fn remove_from_registry(&self) {
        let Ok(mut registry) = authoritative_barrier_singleflight_registry().lock() else {
            return;
        };
        if registry
            .get(&self.key)
            .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
        {
            registry.remove(&self.key);
        }
    }
}

impl Drop for AuthoritativeBarrierSingleflightOwner {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        // Dropping the future says nothing about whether the request crossed
        // the network boundary. Wake a replacement owner, but retain the exact
        // request id so any PossiblyApplied attempt is reconciled rather than
        // duplicated.
        self.flight
            .publish(AuthoritativeBarrierSingleflightOutcome::RetryFlight);
    }
}

struct AuthoritativeBarrierSingleflightWaiter {
    result: tokio::sync::watch::Receiver<Option<AuthoritativeBarrierSingleflightOutcome>>,
    flight: Arc<AuthoritativeBarrierSingleflight>,
}

#[cfg(test)]
impl Drop for AuthoritativeBarrierSingleflightWaiter {
    fn drop(&mut self) {
        self.flight.waiters.fetch_sub(1, Ordering::Relaxed);
    }
}

enum AuthoritativeBarrierSingleflightClaim {
    Owner(AuthoritativeBarrierSingleflightOwner),
    Waiter(AuthoritativeBarrierSingleflightWaiter),
    Refence,
}

fn authoritative_barrier_fence_is_newer(
    candidate: &AuthoritativeBarrierSingleflightKey,
    reference: &AuthoritativeBarrierSingleflightKey,
) -> bool {
    if candidate.leader_term != reference.leader_term {
        return candidate.leader_term > reference.leader_term;
    }
    let candidate_config_revision = candidate
        .config_entry
        .as_ref()
        .map(|(_, _, _, modify_revision, _, _)| *modify_revision);
    let reference_config_revision = reference
        .config_entry
        .as_ref()
        .map(|(_, _, _, modify_revision, _, _)| *modify_revision);
    candidate_config_revision > reference_config_revision
}

fn claim_authoritative_barrier_singleflight(
    key: AuthoritativeBarrierSingleflightKey,
    request: KvCommandRequest,
) -> Result<AuthoritativeBarrierSingleflightClaim, NetworkError> {
    let mut registry = authoritative_barrier_singleflight_registry()
        .lock()
        .map_err(|error| NetworkError::Io {
            operation: "claim authoritative Raft barrier singleflight".to_string(),
            detail: format!("authoritative barrier singleflight registry lock poisoned: {error}"),
        })?;

    // Completed entries for unrelated embedded/test roots have no stale
    // preparers participating in this claim. Pruning them bounds the process
    // registry while retaining the current cluster/node fence needed to close
    // the late-claimer race.
    registry.retain(|existing_key, flight| {
        existing_key.cluster_root == key.cluster_root
            || flight.result.borrow().is_none()
            || Arc::strong_count(flight) > 1
    });
    if registry.iter().any(|(existing_key, flight)| {
        existing_key.same_cluster_node(&key)
            && authoritative_barrier_fence_is_newer(existing_key, &key)
            && matches!(
                &*flight.result.borrow(),
                Some(AuthoritativeBarrierSingleflightOutcome::Established)
            )
    }) {
        // A preparer from an older term/configuration must never evict the
        // completed fence which superseded it. Re-read durable authority and
        // either enter ReadIndex or join the current cold flight.
        return Ok(AuthoritativeBarrierSingleflightClaim::Refence);
    }
    // A new term or configuration supersedes completed fences for the same
    // cluster/node. In-flight older owners remain alive for existing waiters.
    registry.retain(|existing_key, flight| {
        existing_key == &key
            || !existing_key.same_cluster_node(&key)
            || flight.result.borrow().is_none()
            || authoritative_barrier_fence_is_newer(existing_key, &key)
    });

    if let Some(flight) = registry.get(&key).cloned() {
        if matches!(
            &*flight.result.borrow(),
            Some(AuthoritativeBarrierSingleflightOutcome::RetryFlight)
        ) {
            flight.clear_for_reclaim();
            metrics::counter!(
                "sinomo.kv.authoritative_read.barrier_singleflight",
                "role" => "reclaim"
            )
            .increment(1);
            return Ok(AuthoritativeBarrierSingleflightClaim::Owner(
                AuthoritativeBarrierSingleflightOwner {
                    key,
                    flight,
                    completed: false,
                },
            ));
        }
        #[cfg(test)]
        flight.waiters.fetch_add(1, Ordering::Relaxed);
        let result = flight.result.subscribe();
        metrics::counter!(
            "sinomo.kv.authoritative_read.barrier_singleflight",
            "role" => "waiter"
        )
        .increment(1);
        return Ok(AuthoritativeBarrierSingleflightClaim::Waiter(
            AuthoritativeBarrierSingleflightWaiter { result, flight },
        ));
    }

    let (result, _initial_receiver) = tokio::sync::watch::channel(None);
    let flight = Arc::new(AuthoritativeBarrierSingleflight {
        request,
        generation: next_authoritative_barrier_singleflight_generation(),
        result,
        blocking_result: Mutex::new(None),
        blocking_changed: Condvar::new(),
        #[cfg(test)]
        waiters: AtomicUsize::new(0),
    });
    registry.insert(key.clone(), Arc::clone(&flight));
    metrics::counter!(
        "sinomo.kv.authoritative_read.barrier_singleflight",
        "role" => "owner"
    )
    .increment(1);
    Ok(AuthoritativeBarrierSingleflightClaim::Owner(
        AuthoritativeBarrierSingleflightOwner {
            key,
            flight,
            completed: false,
        },
    ))
}

async fn wait_for_authoritative_barrier_singleflight(
    mut waiter: AuthoritativeBarrierSingleflightWaiter,
    deadline: Instant,
) -> Result<AuthoritativeBarrierSingleflightOutcome, NetworkError> {
    loop {
        if let Some(result) = waiter.result.borrow().clone() {
            return Ok(result);
        }
        let remaining = remaining_authoritative_read_budget(
            deadline,
            "shared first-current-term authoritative barrier",
        )?;
        match tokio::time::timeout(remaining, waiter.result.changed()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return Err(NetworkError::ClusterReadOnly {
                    detail: "authoritative barrier owner ended without publishing an outcome"
                        .to_string(),
                });
            }
            Err(_) => {
                return Err(authoritative_read_phase_timeout(
                    "shared first-current-term authoritative barrier",
                ));
            }
        }
    }
}

fn wait_for_authoritative_barrier_singleflight_blocking(
    waiter: AuthoritativeBarrierSingleflightWaiter,
    deadline: Instant,
) -> Result<AuthoritativeBarrierSingleflightOutcome, NetworkError> {
    let flight = Arc::clone(&waiter.flight);
    let mut result = flight
        .blocking_result
        .lock()
        .map_err(|error| NetworkError::Io {
            operation: "wait for authoritative Raft barrier singleflight".to_string(),
            detail: format!("authoritative barrier blocking result lock poisoned: {error}"),
        })?;
    loop {
        if let Some(result) = result.clone() {
            return Ok(result);
        }
        let remaining = remaining_authoritative_read_budget(
            deadline,
            "shared first-current-term authoritative barrier",
        )?;
        let (next_result, wait) = flight
            .blocking_changed
            .wait_timeout(result, remaining)
            .map_err(|error| NetworkError::Io {
                operation: "wait for authoritative Raft barrier singleflight".to_string(),
                detail: format!("authoritative barrier blocking wait lock poisoned: {error}"),
            })?;
        result = next_result;
        if wait.timed_out() && result.is_none() {
            return Err(authoritative_read_phase_timeout(
                "shared first-current-term authoritative barrier",
            ));
        }
    }
}

fn authoritative_barrier_failure_allows_reclaim(error: &NetworkError) -> bool {
    match error {
        NetworkError::Timeout { delivery_state, .. }
        | NetworkError::RetryExhausted { delivery_state, .. }
        | NetworkError::DeliveryFailed { delivery_state, .. } => {
            *delivery_state == sinomo_network::NetworkDeliveryState::DefinitelyNotSent
        }
        // An unknown outcome is deliberately shared unchanged. The Raft
        // driver may have cleared leadership while reconciling it, and a
        // replacement attempt must not turn the original PossiblyApplied
        // result into a misleading non-leader error.
        NetworkError::OutcomeUnknown { .. } => false,
        NetworkError::ClusterReadOnly { detail } => {
            detail.contains("exhausted its Raft confirmation budget")
                || detail.starts_with("Raft quorum commit timed out during ")
        }
        _ => false,
    }
}

#[cfg(test)]
fn authoritative_barrier_singleflight_waiters_for_root(root: &std::path::Path) -> usize {
    authoritative_barrier_singleflight_registry()
        .lock()
        .ok()
        .map(|registry| {
            registry
                .iter()
                .filter(|(key, _)| key.cluster_root == root)
                .map(|(_, flight)| flight.waiters.load(Ordering::Relaxed))
                .sum()
        })
        .unwrap_or_default()
}

#[cfg(test)]
fn clear_authoritative_barrier_singleflight_for_root(root: &std::path::Path) {
    if let Ok(mut registry) = authoritative_barrier_singleflight_registry().lock() {
        registry.retain(|key, _| key.cluster_root != root);
    }
}

#[cfg(test)]
struct AuthoritativeBarrierFencePause {
    armed: AtomicBool,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

#[cfg(test)]
fn authoritative_barrier_fence_pause_registry()
-> &'static Mutex<BTreeMap<PathBuf, Arc<AuthoritativeBarrierFencePause>>> {
    static REGISTRY: OnceLock<Mutex<BTreeMap<PathBuf, Arc<AuthoritativeBarrierFencePause>>>> =
        OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn install_authoritative_barrier_fence_pause(
    root: &std::path::Path,
) -> Arc<AuthoritativeBarrierFencePause> {
    let pause = Arc::new(AuthoritativeBarrierFencePause {
        armed: AtomicBool::new(true),
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    authoritative_barrier_fence_pause_registry()
        .lock()
        .expect("authoritative barrier fence-pause registry")
        .insert(root.to_path_buf(), Arc::clone(&pause));
    pause
}

#[cfg(test)]
async fn pause_authoritative_barrier_after_initial_fence(root: &std::path::Path) {
    let pause = authoritative_barrier_fence_pause_registry()
        .lock()
        .ok()
        .and_then(|registry| registry.get(root).cloned());
    let Some(pause) = pause else {
        return;
    };
    if !pause.armed.swap(false, Ordering::SeqCst) {
        return;
    }
    pause.entered.add_permits(1);
    pause
        .release
        .acquire()
        .await
        .expect("release paused authoritative barrier fence")
        .forget();
}

#[cfg(test)]
fn clear_authoritative_barrier_fence_pause(root: &std::path::Path) {
    if let Ok(mut registry) = authoritative_barrier_fence_pause_registry().lock() {
        registry.remove(root);
    }
}

struct AuthoritativeBarrierFence {
    read_state: sinomo_kv::ClusterKvRaftReadIndexState,
    config_entry: Option<sinomo_kv::KvEntry>,
    pending_current_term_barrier: Option<sinomo_kv::KvLogEntry>,
    voters: Vec<NodeId>,
    projected_state: ClusterBootstrapState,
}

impl From<ClusterRaftReadIndexAuthorityFence> for AuthoritativeBarrierFence {
    fn from(fence: ClusterRaftReadIndexAuthorityFence) -> Self {
        Self {
            read_state: fence.read_state,
            config_entry: fence.config_fence,
            pending_current_term_barrier: fence.pending_current_term_barrier,
            voters: fence.confirmed_voters.into_iter().collect(),
            projected_state: fence.confirmed_state,
        }
    }
}

impl AuthoritativeBarrierFence {
    fn singleflight_key(
        &self,
        paths: &ClusterPaths,
        local_node_id: NodeId,
    ) -> AuthoritativeBarrierSingleflightKey {
        AuthoritativeBarrierSingleflightKey::new(
            paths,
            local_node_id,
            self.read_state.current_term,
            self.config_entry.as_ref(),
            self.voters.clone(),
        )
    }

    fn barrier_request(&self) -> KvCommandRequest {
        self.pending_current_term_barrier.as_ref().map_or_else(
            || KvCommandRequest::generated(KvCommand::Barrier),
            |entry| KvCommandRequest::new(entry.request_id.clone(), KvCommand::Barrier),
        )
    }
}

fn prepare_authoritative_barrier_fence_sync(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    deadline: Instant,
) -> Result<AuthoritativeBarrierFence, NetworkError> {
    remaining_authoritative_read_budget(deadline, "authoritative KV barrier configuration fence")?;
    let local_node_id = state.local_node_id.clone();
    let mut projected_state = state.clone();
    let mut storage = LocalClusterRaftCommandStorage::new_read_index_with_operation_deadline(
        paths,
        local_node_id.clone(),
        deadline,
    );
    let read_state = storage.read_index_state()?;
    let config_entry = storage.read_index_entry(&sinomo_kv::cluster_kv_raft_config_key())?;
    let pending_current_term_barrier = storage.pending_current_term_barrier()?;
    if let Some(entry) = config_entry.as_ref() {
        let config: sinomo_kv::ClusterKvRaftConfigRecord = entry.decode().map_err(kv_err)?;
        project_cluster_raft_config_record(&mut projected_state, &config)?;
    }
    if read_state.leader_id.as_ref() != Some(&local_node_id)
        || !sinomo_kv::cluster_kv_raft_local_is_voter(&projected_state)
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: "local node lost verified Raft leadership before authoritative barrier"
                .to_string(),
        });
    }
    if read_state.current_term < projected_state.term {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "local Raft term {} trails projected cluster term {} before authoritative barrier",
                read_state.current_term, projected_state.term,
            ),
        });
    }
    projected_state.term = projected_state.term.max(read_state.current_term);
    projected_state.kv_revision = projected_state.kv_revision.max(read_state.revision.0);
    let voters = sinomo_kv::cluster_kv_raft_voter_ids(&projected_state).map_err(kv_err)?;
    remaining_authoritative_read_budget(deadline, "authoritative KV barrier configuration fence")?;
    Ok(AuthoritativeBarrierFence {
        read_state,
        config_entry,
        pending_current_term_barrier,
        voters,
        projected_state,
    })
}

async fn prepare_authoritative_barrier_fence(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    deadline: Instant,
) -> Result<AuthoritativeBarrierFence, NetworkError> {
    let local_node_id = state.local_node_id.clone();
    let fence_paths = paths.clone();
    let fence_node_id = local_node_id.clone();
    let mut fence_state = state.clone();
    run_authoritative_read_blocking_before_deadline(
        runtime,
        deadline,
        "authoritative KV barrier configuration fence",
        "prepare authoritative cluster KV barrier read",
        move || {
            let mut storage =
                LocalClusterRaftCommandStorage::new_read_index_with_operation_deadline(
                &fence_paths,
                fence_node_id.clone(),
                deadline,
            );
            let read_state = storage.read_index_state()?;
            let config_entry =
                storage.read_index_entry(&sinomo_kv::cluster_kv_raft_config_key())?;
            let pending_current_term_barrier = storage.pending_current_term_barrier()?;
            if let Some(entry) = config_entry.as_ref() {
                let config: sinomo_kv::ClusterKvRaftConfigRecord =
                    entry.decode().map_err(kv_err)?;
                project_cluster_raft_config_record(&mut fence_state, &config)?;
            }
            if read_state.leader_id.as_ref() != Some(&fence_node_id)
                || !sinomo_kv::cluster_kv_raft_local_is_voter(&fence_state)
            {
                return Err(NetworkError::ClusterReadOnly {
                    detail: "local node lost verified Raft leadership before authoritative barrier"
                        .to_string(),
                });
            }
            if read_state.current_term < fence_state.term {
                return Err(NetworkError::ClusterReadOnly {
                    detail: format!(
                        "local Raft term {} trails projected cluster term {} before authoritative barrier",
                        read_state.current_term, fence_state.term,
                    ),
                });
            }
            // Bind the proposal state to the durable hard-state term. The
            // quorum driver rejects a caller whose projected term differs,
            // closing the stale-preparer gap between this fence and append.
            fence_state.term = fence_state.term.max(read_state.current_term);
            fence_state.kv_revision = fence_state.kv_revision.max(read_state.revision.0);
            let voters = sinomo_kv::cluster_kv_raft_voter_ids(&fence_state).map_err(kv_err)?;
            Ok(AuthoritativeBarrierFence {
                read_state,
                config_entry,
                pending_current_term_barrier,
                voters,
                projected_state: fence_state,
            })
        },
    )
    .await
}

fn project_committed_cluster_raft_authority_from_read_projection(
    state: &mut ClusterBootstrapState,
    storage: &mut impl LocalClusterRaftReadProjection,
) -> Result<(), NetworkError> {
    let read_state = storage.read_index_state()?;
    state.term = state.term.max(read_state.current_term);
    state.kv_revision = state.kv_revision.max(read_state.revision.0);
    if let Some(entry) = storage.read_index_entry(&sinomo_kv::cluster_kv_raft_config_key())? {
        let config: sinomo_kv::ClusterKvRaftConfigRecord = entry.decode().map_err(kv_err)?;
        project_cluster_raft_config_record(state, &config)?;
    }
    Ok(())
}

async fn project_committed_cluster_raft_authority_before_deadline(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    deadline: Instant,
    operation: &'static str,
) -> Result<ClusterBootstrapState, NetworkError> {
    remaining_authoritative_read_budget(deadline, operation)?;
    let project_paths = paths.clone();
    let mut consensus_state = state.clone();
    run_authoritative_read_blocking_before_deadline(
        runtime,
        deadline,
        operation,
        "project committed cluster Raft authority",
        move || {
            let local_node_id = consensus_state.local_node_id.clone();
            let wal_paths = project_paths.kv_paths().raft_wal_paths(&local_node_id);
            if let Some(projected) = sinomo_kv::with_cluster_kv_resident_read_projection(
                &wal_paths,
                &local_node_id,
                |storage| {
                    project_committed_cluster_raft_authority_from_read_projection(
                        &mut consensus_state,
                        storage,
                    )
                },
            ) {
                projected.map_err(kv_err)?.value?;
            } else {
                // A concurrent owner may make the shared projection
                // temporarily unavailable. Wait only within the caller's
                // existing deadline for that same resident node; this
                // ReadIndex session is configured to reject a cold restore.
                let mut storage =
                    LocalClusterRaftCommandStorage::new_read_index_with_operation_deadline(
                        &project_paths,
                        local_node_id,
                        deadline,
                    );
                project_committed_cluster_raft_authority_from_read_projection(
                    &mut consensus_state,
                    &mut storage,
                )?;
            }
            Ok::<_, NetworkError>(consensus_state)
        },
    )
    .await
}

async fn commit_cluster_kv_authoritative_barrier_with_consensus_state<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    deadline: Instant,
    host: &mut H,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence_and_projection_policy(
        runtime,
        paths,
        state,
        options,
        deadline,
        None,
        ClusterRaftReadProjectionPolicy::CompleteAdmittedSnapshot,
        |storage| storage.read_index_snapshot(),
        host,
    )
    .await
}

/// Establishes the first current-term commit and projects the requested value
/// from the resident store while the same leader/configuration is still in
/// force. Unlike the compatibility snapshot barrier, the command result is
/// bounded and the final projection is indexed.
async fn commit_cluster_kv_authoritative_barrier_then_read<H, T, F>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    deadline: Instant,
    read_value: F,
    host: &mut H,
) -> Result<KvAuthoritativeRead<T>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
    T: Send + 'static,
    F: FnOnce(&mut dyn LocalClusterRaftReadProjection) -> Result<T, NetworkError> + Send + 'static,
{
    commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence(
        runtime, paths, state, options, deadline, None, read_value, host,
    )
    .await
}

async fn commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence<H, T, F>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    deadline: Instant,
    initial_fence: Option<AuthoritativeBarrierFence>,
    read_value: F,
    host: &mut H,
) -> Result<KvAuthoritativeRead<T>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
    T: Send + 'static,
    F: FnOnce(&mut dyn LocalClusterRaftReadProjection) -> Result<T, NetworkError> + Send + 'static,
{
    commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence_and_projection_policy(
        runtime,
        paths,
        state,
        options,
        deadline,
        initial_fence,
        ClusterRaftReadProjectionPolicy::DeadlineBounded,
        read_value,
        host,
    )
    .await
}

async fn commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence_and_projection_policy<
    H,
    T,
    F,
>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    deadline: Instant,
    initial_fence: Option<AuthoritativeBarrierFence>,
    projection_policy: ClusterRaftReadProjectionPolicy,
    read_value: F,
    host: &mut H,
) -> Result<KvAuthoritativeRead<T>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
    T: Send + 'static,
    F: FnOnce(&mut dyn LocalClusterRaftReadProjection) -> Result<T, NetworkError> + Send + 'static,
{
    let local_node_id = state.local_node_id.clone();
    let read_value = Arc::new(Mutex::new(Some(read_value)));
    let mut fence = match initial_fence {
        Some(fence) => fence,
        None => prepare_authoritative_barrier_fence(runtime, paths, state, deadline).await?,
    };
    #[cfg(test)]
    pause_authoritative_barrier_after_initial_fence(&paths.root).await;

    'refence: loop {
        *state = fence.projected_state.clone();
        if fence.read_state.has_current_term_commit {
            let read_value = Arc::clone(&read_value);
            let remaining = remaining_authoritative_read_budget(
                deadline,
                "authoritative KV refreshed post-miss ReadIndex",
            )?;
            if let Some(read) = read_cluster_raft_with_read_index_async_with_projection_policy(
                runtime,
                paths,
                state,
                remaining,
                options.raft_append_replication_options,
                projection_policy,
                move |storage| {
                    let read = read_value
                        .lock()
                        .map_err(|_| NetworkError::Io {
                            operation: "project authoritative cluster KV read".to_string(),
                            detail: "authoritative read projection lock is poisoned".to_string(),
                        })?
                        .take()
                        .ok_or_else(|| NetworkError::Io {
                            operation: "project authoritative cluster KV read".to_string(),
                            detail: "authoritative read projection was already consumed"
                                .to_string(),
                        })?;
                    read(storage)
                },
                host,
            )
            .await?
            {
                return Ok(read);
            }
            fence = prepare_authoritative_barrier_fence(runtime, paths, state, deadline).await?;
            continue;
        }

        // Claim the exact durable fence first. Only the owner reopens resident
        // storage and re-fences immediately before append; waiters do not each
        // enqueue the same lock acquisition. A changed term/config/pending
        // request publishes `Refence` to the whole cohort before any proposal.
        let barrier_key = fence.singleflight_key(paths, local_node_id.clone());
        let barrier_request = fence.barrier_request();

        enum BarrierReadRole {
            Owner(sinomo_kv::ClusterKvRaftQuorumCompactCommitResult),
            Waiter,
        }
        let role = loop {
            remaining_authoritative_read_budget(
                deadline,
                "authoritative KV barrier singleflight claim",
            )?;
            match claim_authoritative_barrier_singleflight(
                barrier_key.clone(),
                barrier_request.clone(),
            )? {
                AuthoritativeBarrierSingleflightClaim::Refence => {
                    fence = prepare_authoritative_barrier_fence(runtime, paths, state, deadline)
                        .await?;
                    continue 'refence;
                }
                AuthoritativeBarrierSingleflightClaim::Owner(owner) => {
                    let owner_request = owner.request();
                    let refreshed =
                        prepare_authoritative_barrier_fence(runtime, paths, state, deadline)
                            .await?;
                    let refreshed_key = refreshed.singleflight_key(paths, local_node_id.clone());
                    let pending_request_changed = refreshed
                        .pending_current_term_barrier
                        .as_ref()
                        .is_some_and(|entry| entry.request_id != owner_request.request_id);
                    *state = refreshed.projected_state.clone();
                    if refreshed.read_state.has_current_term_commit
                        || refreshed_key != barrier_key
                        || pending_request_changed
                    {
                        owner.finish_refence();
                        fence = refreshed;
                        continue 'refence;
                    }

                    let remaining = remaining_authoritative_read_budget(
                        deadline,
                        "authoritative KV compact barrier",
                    )?;
                    metrics::counter!(
                        "sinomo.kv.authoritative_read",
                        "path" => "compact_barrier"
                    )
                    .increment(1);
                    let result = commit_cluster_raft_command_compact_with_quorum_async_with_host(
                        runtime,
                        paths,
                        state,
                        owner_request.clone(),
                        ClusterKvRaftQuorumCommitOptions { timeout: remaining },
                        options.raft_append_replication_options,
                        host,
                    )
                    .await;
                    match result {
                        Ok(compact) => {
                            owner.finish_established();
                            metrics::counter!(
                                "sinomo.kv.authoritative_read.barrier_singleflight",
                                "role" => "established"
                            )
                            .increment(1);
                            if compact.replayed_committed_request {
                                break BarrierReadRole::Waiter;
                            }
                            break BarrierReadRole::Owner(compact);
                        }
                        Err(error) => {
                            // The durable term/configuration can advance in
                            // the narrow gap after the owner fence and before
                            // the quorum driver's leadership check. A stale
                            // projected term is rejected before append; do not
                            // pin that obsolete flight as Failed. Re-fence and
                            // let the caller enter the new term's ReadIndex or
                            // cold flight. Preserve the original error only
                            // when durable authority is unchanged.
                            if let Ok(refreshed) =
                                prepare_authoritative_barrier_fence(runtime, paths, state, deadline)
                                    .await
                            {
                                let refreshed_key =
                                    refreshed.singleflight_key(paths, local_node_id.clone());
                                let pending_request_changed =
                                    refreshed.pending_current_term_barrier.as_ref().is_some_and(
                                        |entry| entry.request_id != owner_request.request_id,
                                    );
                                if refreshed.read_state.has_current_term_commit
                                    || refreshed_key != barrier_key
                                    || pending_request_changed
                                {
                                    owner.finish_refence();
                                    fence = refreshed;
                                    continue 'refence;
                                }
                            }
                            if authoritative_barrier_failure_allows_reclaim(&error) {
                                owner.publish_retry();
                                return Err(error);
                            }
                            return Err(owner.finish_failed(error));
                        }
                    }
                }
                AuthoritativeBarrierSingleflightClaim::Waiter(waiter) => {
                    match wait_for_authoritative_barrier_singleflight(waiter, deadline).await? {
                        AuthoritativeBarrierSingleflightOutcome::Established => {
                            break BarrierReadRole::Waiter;
                        }
                        AuthoritativeBarrierSingleflightOutcome::Refence => {
                            fence = prepare_authoritative_barrier_fence(
                                runtime, paths, state, deadline,
                            )
                            .await?;
                            continue 'refence;
                        }
                        AuthoritativeBarrierSingleflightOutcome::RetryFlight => continue,
                        AuthoritativeBarrierSingleflightOutcome::Failed(error) => {
                            return Err(error);
                        }
                    }
                }
            }
        };

        if matches!(&role, BarrierReadRole::Waiter) {
            let read_value = Arc::clone(&read_value);
            let remaining = remaining_authoritative_read_budget(
                deadline,
                "authoritative KV post-barrier ReadIndex",
            )?;
            if let Some(read) = read_cluster_raft_with_read_index_async_with_projection_policy(
                runtime,
                paths,
                state,
                remaining,
                options.raft_append_replication_options,
                projection_policy,
                move |storage| {
                    let read = read_value
                        .lock()
                        .map_err(|_| NetworkError::Io {
                            operation: "project authoritative cluster KV read".to_string(),
                            detail: "authoritative read projection lock is poisoned".to_string(),
                        })?
                        .take()
                        .ok_or_else(|| NetworkError::Io {
                            operation: "project authoritative cluster KV read".to_string(),
                            detail: "authoritative read projection was already consumed"
                                .to_string(),
                        })?;
                    read(storage)
                },
                host,
            )
            .await?
            {
                return Ok(read);
            }
            fence = prepare_authoritative_barrier_fence(runtime, paths, state, deadline).await?;
            continue;
        }

        let BarrierReadRole::Owner(compact) = role else {
            unreachable!("waiting authoritative barrier reads return through ReadIndex above")
        };
        let confirmed_term = compact.authority.term;
        let confirmed_commit_index = compact.authority.commit_index;
        let config_fence = fence.config_entry.clone();
        let final_paths = paths.clone();
        let final_node_id = local_node_id.clone();
        let projected_node_id = final_node_id.clone();
        let read_value = Arc::clone(&read_value);
        remaining_authoritative_read_budget(
            deadline,
            "authoritative KV compact barrier final read",
        )?;
        let finish_read = move |storage: &mut dyn LocalClusterRaftReadProjection| {
            let applied_state = storage.read_index_state()?;
            if applied_state.current_term != confirmed_term
                || applied_state.leader_id.as_ref() != Some(&projected_node_id)
            {
                return Err(NetworkError::ClusterReadOnly {
                    detail: format!(
                        "Raft leadership changed while projecting compact barrier term {confirmed_term}"
                    ),
                });
            }
            if applied_state.applied_index > applied_state.commit_index
                || applied_state.applied_index < confirmed_commit_index
            {
                return Err(NetworkError::ClusterReadOnly {
                    detail: format!(
                        "local Raft apply index {} does not cover compact barrier {} at committed index {}",
                        applied_state.applied_index,
                        confirmed_commit_index,
                        applied_state.commit_index
                    ),
                });
            }
            let applied_config =
                storage.read_index_entry(&sinomo_kv::cluster_kv_raft_config_key())?;
            if applied_config != config_fence {
                return Err(NetworkError::ClusterReadOnly {
                        detail: "committed Raft configuration changed while projecting compact barrier; retry the read"
                            .to_string(),
                    });
            }
            let read = read_value
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "project authoritative cluster KV read".to_string(),
                    detail: "authoritative read projection lock is poisoned".to_string(),
                })?
                .take()
                .ok_or_else(|| NetworkError::Io {
                    operation: "project authoritative cluster KV read".to_string(),
                    detail: "authoritative read projection was already consumed".to_string(),
                })?;
            remaining_authoritative_read_budget(
                deadline,
                "authoritative KV compact barrier final projection admission",
            )?;
            let value = read(storage);
            finish_authoritative_projection_deadline(
                deadline,
                projection_policy,
                "compact_barrier",
                "authoritative KV compact barrier final projection",
            )?;
            let value = value?;
            Ok((applied_state, value))
        };
        let (applied_state, value) = match projection_policy {
            // A bounded point/prefix projection borrows the canonical idle
            // resident view. Concurrent readers serialize only on its tiny
            // in-memory read section and never enter the Raft operation queue.
            ClusterRaftReadProjectionPolicy::DeadlineBounded => {
                let wal_paths = final_paths.kv_paths().raft_wal_paths(&final_node_id);
                let projected = sinomo_kv::with_cluster_kv_resident_read_projection(
                    &wal_paths,
                    &final_node_id,
                    |storage| finish_read(storage),
                )
                .ok_or_else(|| NetworkError::ClusterReadOnly {
                    detail:
                        "resident Raft view changed after compact barrier; retry the authoritative read"
                            .to_string(),
                })?
                .map_err(kv_err)?;
                projected.value?
            }
            // Full snapshots remain proportional to the complete keyspace and
            // retain supervised completion semantics once admitted.
            ClusterRaftReadProjectionPolicy::CompleteAdmittedSnapshot => {
                let wal_paths = final_paths.kv_paths().raft_wal_paths(&final_node_id);
                run_cluster_raft_supervised_blocking(
                    runtime,
                    Some((
                        deadline,
                        authoritative_read_phase_timeout(
                            "authoritative KV compact barrier final projection",
                        ),
                        ClusterRaftBlockingDeadlinePolicy::CompleteAdmitted,
                    )),
                    ClusterRaftBlockingCancellationPolicy::CancelQueued,
                    "compact_barrier",
                    "authoritative KV compact barrier final projection",
                    "project authoritative cluster KV compact barrier",
                    move || {
                        let projected = sinomo_kv::with_cluster_kv_resident_read_projection(
                            &wal_paths,
                            &final_node_id,
                            |storage| finish_read(storage),
                        )
                        .ok_or_else(|| NetworkError::ClusterReadOnly {
                            detail: "resident Raft view changed after compact barrier; retry the authoritative snapshot"
                                .to_string(),
                        })?
                        .map_err(kv_err)?;
                        projected.value
                    },
                )
                .await?
            }
        };

        state.term = state.term.max(applied_state.current_term);
        state.kv_revision = state.kv_revision.max(applied_state.revision.0);
        let mut authority = compact.authority;
        authority.commit_index = applied_state.applied_index;
        authority.revision = applied_state.revision;
        return Ok(KvAuthoritativeRead { value, authority });
    }
}

fn read_cluster_kv_authoritative_snapshot_with_read_index_attempt_sync(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    deadline: Instant,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<ClusterRaftReadIndexAttempt<KvSnapshot>, NetworkError> {
    read_cluster_raft_with_read_index_attempt_with_transport(
        paths,
        state,
        deadline,
        options.raft_append_replication_options,
        ClusterRaftReadProjectionPolicy::CompleteAdmittedSnapshot,
        |storage| storage.read_index_snapshot(),
        |probe_state, targets, request, timeout, required_acks| {
            host.send_cluster_kv_read_index_probes(
                probe_state,
                targets,
                request,
                timeout,
                required_acks,
            )
        },
    )
}

fn read_cluster_kv_authoritative_snapshot_with_read_index_sync(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    deadline: Instant,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<Option<KvAuthoritativeRead<KvSnapshot>>, NetworkError> {
    match read_cluster_kv_authoritative_snapshot_with_read_index_attempt_sync(
        paths, state, options, deadline, host,
    )? {
        ClusterRaftReadIndexAttempt::Confirmed(read) => Ok(Some(read)),
        ClusterRaftReadIndexAttempt::NeedsBarrier(_) => Ok(None),
        ClusterRaftReadIndexAttempt::NotLeader => Err(NetworkError::ClusterReadOnly {
            detail: "local node lost verified Raft leadership before ReadIndex preparation"
                .to_string(),
        }),
        ClusterRaftReadIndexAttempt::ForwardOnly(reason) => Err(reason.into_error()),
        ClusterRaftReadIndexAttempt::LocalAdmissionBusy => Err(NetworkError::ClusterReadOnly {
            detail: "local Raft admission was busy before ReadIndex preparation".to_string(),
        }),
    }
}

fn commit_cluster_kv_authoritative_barrier_sync(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    deadline: Instant,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
    commit_cluster_kv_authoritative_barrier_sync_with_initial_fence(
        paths, state, options, deadline, None, host,
    )
}

fn commit_cluster_kv_authoritative_barrier_sync_with_initial_fence(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    deadline: Instant,
    initial_fence: Option<AuthoritativeBarrierFence>,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
    let local_node_id = state.local_node_id.clone();
    let mut fence = match initial_fence {
        Some(fence) => fence,
        None => prepare_authoritative_barrier_fence_sync(paths, state, deadline)?,
    };

    'refence: loop {
        *state = fence.projected_state.clone();
        if fence.read_state.has_current_term_commit {
            if let Some(read) = read_cluster_kv_authoritative_snapshot_with_read_index_sync(
                paths, state, options, deadline, host,
            )? {
                return Ok(read);
            }
            fence = prepare_authoritative_barrier_fence_sync(paths, state, deadline)?;
            continue;
        }

        // Close the late-preparer race before publishing or joining a flight.
        // A term/configuration change here must make this caller re-fence; it
        // must never create a barrier under the newer durable term.
        let prepared_key = fence.singleflight_key(paths, local_node_id.clone());
        let refreshed = prepare_authoritative_barrier_fence_sync(paths, state, deadline)?;
        let refreshed_key = refreshed.singleflight_key(paths, local_node_id.clone());
        *state = refreshed.projected_state.clone();
        if refreshed.read_state.has_current_term_commit || refreshed_key != prepared_key {
            fence = refreshed;
            continue;
        }
        fence = refreshed;
        let barrier_key = refreshed_key;
        let barrier_request = fence.barrier_request();

        enum SyncBarrierReadRole {
            Owner(ClusterKvRaftQuorumCommitResult),
            Waiter,
        }

        let role = loop {
            remaining_authoritative_read_budget(
                deadline,
                "authoritative KV barrier singleflight claim",
            )?;
            match claim_authoritative_barrier_singleflight(
                barrier_key.clone(),
                barrier_request.clone(),
            )? {
                AuthoritativeBarrierSingleflightClaim::Refence => {
                    fence = prepare_authoritative_barrier_fence_sync(paths, state, deadline)?;
                    continue 'refence;
                }
                AuthoritativeBarrierSingleflightClaim::Owner(owner) => {
                    let owner_request = owner.request();
                    let refreshed =
                        prepare_authoritative_barrier_fence_sync(paths, state, deadline)?;
                    let refreshed_key = refreshed.singleflight_key(paths, local_node_id.clone());
                    let pending_request_changed = refreshed
                        .pending_current_term_barrier
                        .as_ref()
                        .is_some_and(|entry| entry.request_id != owner_request.request_id);
                    *state = refreshed.projected_state.clone();
                    if refreshed.read_state.has_current_term_commit
                        || refreshed_key != barrier_key
                        || pending_request_changed
                    {
                        owner.finish_refence();
                        fence = refreshed;
                        continue 'refence;
                    }

                    let remaining = remaining_authoritative_read_budget(
                        deadline,
                        "authoritative KV synchronous barrier",
                    )?;
                    metrics::counter!(
                        "sinomo.kv.authoritative_read",
                        "path" => "compact_barrier"
                    )
                    .increment(1);
                    let result = host.commit_cluster_kv_command_with_quorum(
                        paths,
                        state,
                        owner_request.clone(),
                        ClusterKvRaftQuorumCommitOptions { timeout: remaining },
                    );
                    match result {
                        Ok(result) => {
                            owner.finish_established();
                            metrics::counter!(
                                "sinomo.kv.authoritative_read.barrier_singleflight",
                                "role" => "established"
                            )
                            .increment(1);
                            if result.replayed_committed_request {
                                break SyncBarrierReadRole::Waiter;
                            }
                            break SyncBarrierReadRole::Owner(result);
                        }
                        Err(error) => {
                            if let Ok(refreshed) =
                                prepare_authoritative_barrier_fence_sync(paths, state, deadline)
                            {
                                let refreshed_key =
                                    refreshed.singleflight_key(paths, local_node_id.clone());
                                let pending_request_changed =
                                    refreshed.pending_current_term_barrier.as_ref().is_some_and(
                                        |entry| entry.request_id != owner_request.request_id,
                                    );
                                if refreshed.read_state.has_current_term_commit
                                    || refreshed_key != barrier_key
                                    || pending_request_changed
                                {
                                    owner.finish_refence();
                                    fence = refreshed;
                                    continue 'refence;
                                }
                            }
                            if authoritative_barrier_failure_allows_reclaim(&error) {
                                owner.publish_retry();
                                return Err(error);
                            }
                            return Err(owner.finish_failed(error));
                        }
                    }
                }
                AuthoritativeBarrierSingleflightClaim::Waiter(waiter) => {
                    match wait_for_authoritative_barrier_singleflight_blocking(waiter, deadline)? {
                        AuthoritativeBarrierSingleflightOutcome::Established => {
                            break SyncBarrierReadRole::Waiter;
                        }
                        AuthoritativeBarrierSingleflightOutcome::Refence => {
                            fence =
                                prepare_authoritative_barrier_fence_sync(paths, state, deadline)?;
                            continue 'refence;
                        }
                        AuthoritativeBarrierSingleflightOutcome::RetryFlight => continue,
                        AuthoritativeBarrierSingleflightOutcome::Failed(error) => {
                            return Err(error);
                        }
                    }
                }
            }
        };

        match role {
            SyncBarrierReadRole::Owner(result) => {
                return authoritative_read_from_commit(result, state);
            }
            SyncBarrierReadRole::Waiter => {
                if let Some(read) = read_cluster_kv_authoritative_snapshot_with_read_index_sync(
                    paths, state, options, deadline, host,
                )? {
                    return Ok(read);
                }
                fence = prepare_authoritative_barrier_fence_sync(paths, state, deadline)?;
            }
        }
    }
}

fn validate_forwarded_authoritative_read(
    read: KvAuthoritativeRead<KvSnapshot>,
    state: &ClusterBootstrapState,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
    validate_forwarded_authoritative_snapshot_read(read, state)
}

fn record_sync_authoritative_read_latency(started: Instant, path: &'static str, succeeded: bool) {
    metrics::histogram!(
        "sinomo.kv.authoritative_read.us",
        "mode" => "sync",
        "path" => path,
        "outcome" => if succeeded { "success" } else { "error" }
    )
    .record(started.elapsed().as_micros().min(u128::from(u64::MAX)) as f64);
}

fn record_async_authoritative_read_latency(
    started: Instant,
    operation: &'static str,
    succeeded: bool,
) {
    metrics::histogram!(
        "sinomo.kv.authoritative_read.us",
        "mode" => "async",
        "operation" => operation,
        "outcome" => if succeeded { "success" } else { "error" }
    )
    .record(started.elapsed().as_micros().min(u128::from(u64::MAX)) as f64);
}

pub fn read_cluster_kv_authoritative_snapshot_with_host(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
    let started = Instant::now();
    let deadline = started
        .checked_add(options.raft_append_timeout)
        .unwrap_or(started);
    let mut metric_path = "preflight";
    let result = (|| {
        // The outer ReadIndex preparation projects durable config and leader
        // state in the same resident checkout used to claim the proof wave.
        let mut consensus_state = state.clone();
        let read = read_cluster_kv_authoritative_snapshot_with_consensus_state(
            paths,
            &mut consensus_state,
            options,
            deadline,
            &mut metric_path,
            host,
        )?;
        state.term = state
            .term
            .max(consensus_state.term)
            .max(read.authority.term);
        state.kv_revision = state
            .kv_revision
            .max(consensus_state.kv_revision)
            .max(read.authority.revision.0);
        Ok(read)
    })();
    record_sync_authoritative_read_latency(started, metric_path, result.is_ok());
    result
}

fn read_cluster_kv_authoritative_snapshot_with_consensus_state(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    deadline: Instant,
    metric_path: &mut &'static str,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
    *metric_path = "read_index";
    let forward_only = match read_cluster_kv_authoritative_snapshot_with_read_index_attempt_sync(
        paths, state, options, deadline, host,
    )? {
        ClusterRaftReadIndexAttempt::Confirmed(read) => return Ok(read),
        ClusterRaftReadIndexAttempt::NeedsBarrier(fence) => {
            *metric_path = "barrier";
            return commit_cluster_kv_authoritative_barrier_sync_with_initial_fence(
                paths,
                state,
                options,
                deadline,
                Some(fence.into()),
                host,
            );
        }
        ClusterRaftReadIndexAttempt::ForwardOnly(reason) => {
            reason.apply_forwarding_floor(state)?;
            Some(reason)
        }
        ClusterRaftReadIndexAttempt::NotLeader
        | ClusterRaftReadIndexAttempt::LocalAdmissionBusy => None,
    };

    let forwarding_failure = if options.allow_forwarding {
        *metric_path = "forward";
        let forward_options =
            cluster_kv_forward_options_before_deadline(options.forward_options, deadline);
        match forward_options.map(|forward_options| {
            forward_cluster_kv_authoritative_snapshot_recovering_stale_leader(
                paths,
                state,
                forward_options,
                host,
            )
        }) {
            Some(Ok(Some(read))) => return validate_forwarded_authoritative_read(read, state),
            Some(Ok(None)) | None => None,
            Some(Err(error)) => Some(error),
        }
    } else {
        None
    };
    if let Some(reason) = forward_only {
        // A lagging resident projection is not eligible to campaign or append
        // a barrier. Preserve its typed local-behind diagnosis even when the
        // best-effort forwarding attempt had no route or failed.
        return Err(reason.into_error());
    }
    if !sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        return Err(
            forwarding_failure.unwrap_or_else(|| NetworkError::ClusterReadOnly {
                detail:
                    "local node is not a Raft voter and could not forward an authoritative read"
                        .to_string(),
            }),
        );
    }
    if !options.allow_forwarding {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "{FORWARDED_CLUSTER_KV_READ_NON_LEADER_DETAIL}; forwarded requests do not start elections"
            ),
        });
    }
    *metric_path = "preflight";
    let remaining = remaining_authoritative_read_budget(
        deadline,
        "synchronous authoritative KV leader election",
    )?;
    let mut election_options = options;
    election_options.raft_append_timeout = remaining;
    election_options.forward_options.timeout =
        election_options.forward_options.timeout.min(remaining);
    election_options.leader_election_options.timeout = election_options
        .leader_election_options
        .timeout
        .min(remaining);
    election_options.leader_election_options.campaign_jitter_max = election_options
        .leader_election_options
        .campaign_jitter_max
        .min(remaining);
    if !host.elect_kv_state_authority(paths, state, election_options)? {
        return Err(NetworkError::ClusterReadOnly {
            detail: forwarding_failure.map_or_else(
                || "local node could not establish current-term Raft leadership".to_string(),
                |error| {
                    format!(
                        "local node could not establish current-term Raft leadership; forwarding failed: {error}"
                    )
                },
            ),
        });
    }
    *metric_path = "barrier";
    commit_cluster_kv_authoritative_barrier_sync(paths, state, options, deadline, host)
}

#[cfg(any(test, feature = "test-support"))]
pub fn handle_cluster_kv_txn_request_with_host(
    paths: &ClusterPaths,
    request: KvTxnRequest,
    options: ClusterKvTxnQuorumOptions,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<ClusterKvTxnCommitResult, NetworkError> {
    let mut state = read_cluster_state_without_kv(paths).map_err(|error| match error {
        NetworkError::ClusterOrphan => NetworkError::ClusterReadOnly {
            detail: "local node has no cluster state".to_string(),
        },
        other => other,
    })?;
    let result = commit_cluster_kv_txn_with_quorum_response_with_host(
        paths, &mut state, request, options, host,
    )?;
    write_imported_cluster_kv_snapshot_if_changed(paths, &result.snapshot)?;
    Ok(result)
}

pub(crate) fn locked_cluster_raft_local_is_leader(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<bool, NetworkError> {
    paths
        .kv_paths()
        .locked_raft_local_is_leader(state)
        .map_err(kv_err)
}

fn clear_verified_stale_cluster_raft_leader(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    expected: Option<&sinomo_kv::ClusterKvRaftLeaderFence>,
    error: &NetworkError,
) -> Result<bool, NetworkError> {
    let NetworkError::VerifiedClusterNonLeader { node_id, .. } = error else {
        return Ok(false);
    };
    let Some(expected) = expected else {
        return Ok(false);
    };
    if node_id == &state.local_node_id || node_id != &expected.leader_id {
        return Ok(false);
    }
    paths
        .kv_paths()
        .raft_store()
        .clear_verified_voter_leader_compact(state, expected)
        .map_err(kv_err)
}

fn try_capture_cluster_raft_voter_leader_fence(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Option<sinomo_kv::ClusterKvRaftLeaderFence> {
    // Forwarding remains nonblocking on the local Raft operation gate. A busy
    // gate simply withholds clear authority; the authenticated rejection is
    // still safe to report, but cannot erase an unfenced leader generation.
    paths
        .kv_paths()
        .raft_store()
        .try_locked_known_voter_leader_fence(state)
        .ok()
        .flatten()
}

fn forward_cluster_kv_authoritative_snapshot_recovering_stale_leader(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvTxnForwardOptions,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<Option<KvAuthoritativeRead<KvSnapshot>>, NetworkError> {
    let deadline = Instant::now() + options.timeout;
    let leader_fence = try_capture_cluster_raft_voter_leader_fence(paths, state);
    let result = forward_cluster_kv_authoritative_snapshot_to_state_authority_with_host(
        paths, state, options, host,
    );
    if let Err(error) = &result
        && clear_verified_stale_cluster_raft_leader(paths, state, leader_fence.as_ref(), error)?
        && let Some(options) = cluster_kv_forward_options_before_deadline(options, deadline)
    {
        return forward_cluster_kv_authoritative_snapshot_to_state_authority_with_host(
            paths, state, options, host,
        );
    }
    result
}

fn cluster_kv_forward_options_before_deadline(
    mut options: ClusterKvTxnForwardOptions,
    deadline: Instant,
) -> Option<ClusterKvTxnForwardOptions> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return None;
    }
    options.timeout = remaining;
    Some(options)
}

fn cluster_raft_leader_election_options_before_deadline(
    mut options: crate::control_plane_options::ClusterRaftLeaderElectionOptions,
    deadline: Instant,
    operation: &'static str,
) -> Result<crate::control_plane_options::ClusterRaftLeaderElectionOptions, NetworkError> {
    let remaining = remaining_authoritative_read_budget(deadline, operation)?;
    options.timeout = options.timeout.min(remaining);
    options.campaign_jitter_max = options.campaign_jitter_max.min(remaining);
    Ok(options)
}

async fn clear_verified_stale_cluster_raft_leader_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    expected: Option<&sinomo_kv::ClusterKvRaftLeaderFence>,
    error: &NetworkError,
    deadline: Instant,
) -> Result<bool, NetworkError> {
    let NetworkError::VerifiedClusterNonLeader { node_id, .. } = error else {
        return Ok(false);
    };
    let Some(expected) = expected else {
        return Ok(false);
    };
    if node_id == &state.local_node_id || node_id != &expected.leader_id {
        return Ok(false);
    }
    remaining_authoritative_read_budget(deadline, "verified stale Raft leader clear admission")?;
    let paths = paths.clone();
    let state = state.clone();
    let expected = expected.clone();
    // The store operation performs one atomic expected-leader check and clear
    // under deadline-capped operation/resident admission. Await it directly:
    // dropping an outer timeout would otherwise leave a late blocking task
    // free to erase a newer leader after the caller had already returned.
    run_authoritative_read_blocking_before_deadline(
        runtime,
        deadline,
        "verified stale Raft leader clear admission",
        "clear rejected cluster Raft leader",
        move || {
            paths
                .kv_paths()
                .raft_store()
                .clear_verified_voter_leader_compact_before(&state, &expected, deadline)
                .map_err(kv_err)
        },
    )
    .await
}

fn forwarded_snapshot_read_admission_is_busy(error: &NetworkError) -> bool {
    matches!(error, NetworkError::ClusterReadOnly { detail }
        if detail.contains("local Raft admission is busy")
            && detail.contains("forwarded snapshot read"))
}

async fn forward_cluster_kv_authoritative_snapshot_recovering_stale_leader_async<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvTxnForwardOptions,
    deadline: Instant,
    host: &mut H,
) -> Result<Option<KvAuthoritativeRead<KvSnapshot>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let remaining =
        remaining_authoritative_read_budget(deadline, "authoritative KV snapshot forwarding")?;
    let retry_deadline = Instant::now() + remaining.min(BOUNDED_AUTHORITY_BUSY_RETRY_BUDGET);
    loop {
        let Some(options) = cluster_kv_forward_options_before_deadline(options, deadline) else {
            return Err(authoritative_read_phase_timeout(
                "authoritative KV snapshot forwarding",
            ));
        };
        let leader_fence = try_capture_cluster_raft_voter_leader_fence(paths, state);
        let mut result = host
            .forward_cluster_kv_authoritative_snapshot_async(runtime, paths, state, options)
            .await;
        if let Err(error) = &result
            && !forwarded_snapshot_read_admission_is_busy(error)
            && clear_verified_stale_cluster_raft_leader_async(
                runtime,
                paths,
                state,
                leader_fence.as_ref(),
                error,
                deadline,
            )
            .await?
            && let Some(options) = cluster_kv_forward_options_before_deadline(options, deadline)
        {
            result = host
                .forward_cluster_kv_authoritative_snapshot_async(runtime, paths, state, options)
                .await;
        }
        remaining_authoritative_read_budget(deadline, "authoritative KV snapshot forwarding")?;
        // The receiver remains fail-fast when already forwarded. The origin
        // owns this bounded, read-only re-probe; busy is not stale leadership.
        let retry_remaining = retry_deadline.saturating_duration_since(Instant::now());
        if !result
            .as_ref()
            .err()
            .is_some_and(forwarded_snapshot_read_admission_is_busy)
            || retry_remaining.is_zero()
        {
            return result;
        }
        tokio::time::sleep(retry_remaining.min(LOCAL_READ_INDEX_BUSY_RETRY_INTERVAL)).await;
    }
}

async fn forward_cluster_kv_authoritative_range_prefix_recovering_stale_leader_async<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    prefix: &str,
    options: ClusterKvTxnForwardOptions,
    deadline: Instant,
    host: &mut H,
) -> Result<Option<KvAuthoritativeRead<Vec<sinomo_kv::KvEntry>>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let Some(options) = cluster_kv_forward_options_before_deadline(options, deadline) else {
        return Err(authoritative_read_phase_timeout(
            "authoritative KV prefix forwarding",
        ));
    };
    let leader_fence = try_capture_cluster_raft_voter_leader_fence(paths, state);
    let result = host
        .forward_cluster_kv_authoritative_range_prefix_async(runtime, paths, state, prefix, options)
        .await;
    if let Err(error) = &result
        && clear_verified_stale_cluster_raft_leader_async(
            runtime,
            paths,
            state,
            leader_fence.as_ref(),
            error,
            deadline,
        )
        .await?
        && let Some(options) = cluster_kv_forward_options_before_deadline(options, deadline)
    {
        let retried = host
            .forward_cluster_kv_authoritative_range_prefix_async(
                runtime, paths, state, prefix, options,
            )
            .await;
        remaining_authoritative_read_budget(deadline, "authoritative KV prefix forwarding")?;
        return retried;
    }
    remaining_authoritative_read_budget(deadline, "authoritative KV prefix forwarding")?;
    result
}

fn forwarded_prefix_read_admission_is_busy(error: &NetworkError) -> bool {
    matches!(
        error,
        NetworkError::ClusterReadOnly { detail }
            if detail.contains("local Raft admission is busy")
                && detail.contains("forwarded prefix read")
    )
}

async fn retry_forward_cluster_kv_authoritative_range_prefix_after_busy<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    prefix: &str,
    options: ClusterKvTxnForwardOptions,
    deadline: Instant,
    host: &mut H,
) -> Result<Option<KvAuthoritativeRead<Vec<sinomo_kv::KvEntry>>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let remaining =
        remaining_authoritative_read_budget(deadline, "authoritative KV prefix forwarding retry")?;
    let retry_deadline = Instant::now() + remaining.min(RANGE_AUTHORITY_BUSY_RETRY_BUDGET);
    let mut last_busy_error = None;
    loop {
        let retry_remaining = retry_deadline.saturating_duration_since(Instant::now());
        if retry_remaining.is_zero() {
            return last_busy_error.map_or(Ok(None), Err);
        }
        tokio::time::sleep(retry_remaining.min(LOCAL_READ_INDEX_BUSY_RETRY_INTERVAL)).await;
        match forward_cluster_kv_authoritative_range_prefix_recovering_stale_leader_async(
            runtime, paths, state, prefix, options, deadline, host,
        )
        .await
        {
            Ok(Some(read)) => return Ok(Some(read)),
            Ok(None) => {}
            Err(error) if forwarded_prefix_read_admission_is_busy(&error) => {
                last_busy_error = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
}

async fn forward_cluster_kv_authoritative_entry_recovering_stale_leader_async<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    key: &str,
    options: ClusterKvTxnForwardOptions,
    deadline: Instant,
    host: &mut H,
) -> Result<Option<KvAuthoritativeRead<Option<sinomo_kv::KvEntry>>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let Some(options) = cluster_kv_forward_options_before_deadline(options, deadline) else {
        return Err(authoritative_read_phase_timeout(
            "authoritative KV point forwarding",
        ));
    };
    let leader_fence = try_capture_cluster_raft_voter_leader_fence(paths, state);
    let result = host
        .forward_cluster_kv_authoritative_entry_async(runtime, paths, state, key, options)
        .await;
    if let Err(error) = &result
        && clear_verified_stale_cluster_raft_leader_async(
            runtime,
            paths,
            state,
            leader_fence.as_ref(),
            error,
            deadline,
        )
        .await?
        && let Some(options) = cluster_kv_forward_options_before_deadline(options, deadline)
    {
        let retried = host
            .forward_cluster_kv_authoritative_entry_async(runtime, paths, state, key, options)
            .await;
        remaining_authoritative_read_budget(deadline, "authoritative KV point forwarding")?;
        return retried;
    }
    remaining_authoritative_read_budget(deadline, "authoritative KV point forwarding")?;
    result
}

fn forwarded_point_read_admission_is_busy(error: &NetworkError) -> bool {
    matches!(
        error,
        NetworkError::ClusterReadOnly { detail }
            if detail.contains("local Raft admission is busy")
                && detail.contains("forwarded point read")
    )
}

async fn retry_forward_cluster_kv_authoritative_entry_after_busy<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    key: &str,
    options: ClusterKvTxnForwardOptions,
    deadline: Instant,
    host: &mut H,
) -> Result<Option<KvAuthoritativeRead<Option<sinomo_kv::KvEntry>>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let remaining =
        remaining_authoritative_read_budget(deadline, "authoritative KV point forwarding retry")?;
    let retry_deadline = Instant::now() + remaining.min(BOUNDED_AUTHORITY_BUSY_RETRY_BUDGET);
    let mut last_busy_error = None;
    loop {
        let retry_remaining = retry_deadline.saturating_duration_since(Instant::now());
        if retry_remaining.is_zero() {
            return last_busy_error.map_or(Ok(None), Err);
        }
        tokio::time::sleep(retry_remaining.min(LOCAL_READ_INDEX_BUSY_RETRY_INTERVAL)).await;
        match forward_cluster_kv_authoritative_entry_recovering_stale_leader_async(
            runtime, paths, state, key, options, deadline, host,
        )
        .await
        {
            Ok(Some(read)) => return Ok(Some(read)),
            Ok(None) => {}
            Err(error) if forwarded_point_read_admission_is_busy(&error) => {
                last_busy_error = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
}

async fn forward_cluster_kv_authoritative_entries_recovering_stale_leader_async<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    keys: &[String],
    options: ClusterKvTxnForwardOptions,
    deadline: Instant,
    host: &mut H,
) -> Result<Option<KvAuthoritativeRead<Vec<Option<sinomo_kv::KvEntry>>>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let Some(options) = cluster_kv_forward_options_before_deadline(options, deadline) else {
        return Err(authoritative_read_phase_timeout(
            "authoritative KV exact-key-set forwarding",
        ));
    };
    let leader_fence = try_capture_cluster_raft_voter_leader_fence(paths, state);
    let result = host
        .forward_cluster_kv_authoritative_entries_async(runtime, paths, state, keys, options)
        .await;
    if let Err(error) = &result
        && clear_verified_stale_cluster_raft_leader_async(
            runtime,
            paths,
            state,
            leader_fence.as_ref(),
            error,
            deadline,
        )
        .await?
        && let Some(options) = cluster_kv_forward_options_before_deadline(options, deadline)
    {
        let retried = host
            .forward_cluster_kv_authoritative_entries_async(runtime, paths, state, keys, options)
            .await;
        remaining_authoritative_read_budget(deadline, "authoritative KV exact-key-set forwarding")?;
        return retried;
    }
    remaining_authoritative_read_budget(deadline, "authoritative KV exact-key-set forwarding")?;
    result
}

fn forwarded_exact_key_set_read_admission_is_busy(error: &NetworkError) -> bool {
    matches!(
        error,
        NetworkError::ClusterReadOnly { detail }
            if detail.contains("local Raft admission is busy")
                && detail.contains("forwarded exact-key-set read")
    )
}

async fn retry_forward_cluster_kv_authoritative_entries_after_busy<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    keys: &[String],
    options: ClusterKvTxnForwardOptions,
    deadline: Instant,
    host: &mut H,
) -> Result<Option<KvAuthoritativeRead<Vec<Option<sinomo_kv::KvEntry>>>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let remaining = remaining_authoritative_read_budget(
        deadline,
        "authoritative KV exact-key-set forwarding retry",
    )?;
    let retry_deadline = Instant::now() + remaining.min(BOUNDED_AUTHORITY_BUSY_RETRY_BUDGET);
    let mut last_busy_error = None;
    loop {
        let retry_remaining = retry_deadline.saturating_duration_since(Instant::now());
        if retry_remaining.is_zero() {
            return last_busy_error.map_or(Ok(None), Err);
        }
        tokio::time::sleep(retry_remaining.min(LOCAL_READ_INDEX_BUSY_RETRY_INTERVAL)).await;
        match forward_cluster_kv_authoritative_entries_recovering_stale_leader_async(
            runtime, paths, state, keys, options, deadline, host,
        )
        .await
        {
            Ok(Some(read)) => return Ok(Some(read)),
            Ok(None) => {}
            Err(error) if forwarded_exact_key_set_read_admission_is_busy(&error) => {
                last_busy_error = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
}

fn exact_key_set_route_is_definitely_unsupported(error: &NetworkError) -> bool {
    matches!(
        error,
        NetworkError::UnknownRoute { route }
            if route == sinomo_kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE
    )
}

fn project_mixed_version_authoritative_entries_snapshot(
    snapshot: KvAuthoritativeRead<KvSnapshot>,
    state: &ClusterBootstrapState,
    keys: &[String],
) -> Result<KvAuthoritativeRead<Vec<Option<sinomo_kv::KvEntry>>>, NetworkError> {
    let snapshot = validate_forwarded_authoritative_read(snapshot, state)?;
    sinomo_kv::project_cluster_kv_authoritative_entries(&snapshot, keys).map_err(kv_err)
}

async fn forward_cluster_kv_request_outcome_recovering_stale_leader_async<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request_id: &sinomo_kv::KvRequestId,
    options: ClusterKvTxnForwardOptions,
    deadline: Instant,
    host: &mut H,
) -> Result<Option<sinomo_kv::ClusterKvRequestOutcomeRead>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let Some(options) = cluster_kv_forward_options_before_deadline(options, deadline) else {
        return Err(authoritative_read_phase_timeout(
            "authoritative KV request-outcome forwarding",
        ));
    };
    let leader_fence = try_capture_cluster_raft_voter_leader_fence(paths, state);
    let result = forward_cluster_kv_request_outcome_to_state_authority_async_with_host(
        runtime, paths, state, request_id, options, host,
    )
    .await;
    if let Err(error) = &result
        && clear_verified_stale_cluster_raft_leader_async(
            runtime,
            paths,
            state,
            leader_fence.as_ref(),
            error,
            deadline,
        )
        .await?
        && let Some(options) = cluster_kv_forward_options_before_deadline(options, deadline)
    {
        let retried = forward_cluster_kv_request_outcome_to_state_authority_async_with_host(
            runtime, paths, state, request_id, options, host,
        )
        .await;
        remaining_authoritative_read_budget(
            deadline,
            "authoritative KV request-outcome forwarding",
        )?;
        return retried;
    }
    remaining_authoritative_read_budget(deadline, "authoritative KV request-outcome forwarding")?;
    result
}

async fn locked_cluster_raft_local_is_leader_before_deadline(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    deadline: Instant,
) -> Result<bool, NetworkError> {
    remaining_authoritative_read_budget(deadline, "cluster Raft local leader check")?;
    let paths = paths.clone();
    let state = state.clone();
    run_authoritative_read_blocking_before_deadline(
        runtime,
        deadline,
        "cluster Raft local leader check",
        "cluster Raft local leader before deadline",
        move || {
            let mut storage =
                LocalClusterRaftCommandStorage::new_read_index_with_operation_deadline(
                    &paths,
                    state.local_node_id.clone(),
                    deadline,
                );
            storage.local_is_leader(&state)
        },
    )
    .await
}

async fn write_imported_cluster_kv_snapshot_if_changed_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<(), NetworkError> {
    let paths = paths.clone();
    let snapshot = snapshot.clone();
    runtime
        .spawn_blocking(move || {
            write_imported_cluster_kv_snapshot_if_it_advances(&paths, &snapshot)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster kv snapshot import".to_string(),
            detail: format!("cluster kv snapshot import task failed: {source}"),
        })?
        .map(|_| ())
}

fn write_imported_cluster_kv_snapshot_if_it_advances(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, NetworkError> {
    if !imported_snapshot_advances_local_kv(paths, snapshot) {
        return Ok(false);
    }
    write_imported_cluster_kv_snapshot_if_changed(paths, snapshot)
}

struct NetworkClusterKvTxnQuorumAsyncHost<'a, H> {
    runtime: &'a tokio::runtime::Handle,
    paths: &'a ClusterPaths,
    options: ClusterKvTxnQuorumOptions,
    deadline: Instant,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvTxnQuorumAsyncHost for NetworkClusterKvTxnQuorumAsyncHost<'_, H>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn local_is_raft_leader_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, bool, Self::Error> {
        let runtime = self.runtime;
        let paths = self.paths;
        let deadline = self.deadline;
        Box::pin(async move {
            locked_cluster_raft_local_is_leader_before_deadline(runtime, paths, state, deadline)
                .await
        })
    }

    fn forward_cluster_kv_txn_to_voter_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        request: &'a KvTxnRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        Option<sinomo_kv::ClusterKvTxnCommitResult>,
        Self::Error,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        let options = self.options.forward_options;
        let operation_deadline = self.deadline;
        let host = &mut *self.host;
        Box::pin(async move {
            let Some(options) =
                cluster_kv_forward_options_before_deadline(options, operation_deadline)
            else {
                return Err(authoritative_read_phase_timeout(
                    "cluster KV transaction forwarding",
                ));
            };
            let leader_fence = try_capture_cluster_raft_voter_leader_fence(paths, state);
            let result = forward_cluster_kv_txn_to_state_authority_async_with_host(
                runtime, paths, state, request, options, host,
            )
            .await;
            if let Err(error) = &result
                && clear_verified_stale_cluster_raft_leader_async(
                    runtime,
                    paths,
                    state,
                    leader_fence.as_ref(),
                    error,
                    operation_deadline,
                )
                .await?
                && let Some(options) =
                    cluster_kv_forward_options_before_deadline(options, operation_deadline)
            {
                return forward_cluster_kv_txn_to_state_authority_async_with_host(
                    runtime, paths, state, request, options, host,
                )
                .await;
            }
            result
        })
    }

    fn forward_cluster_kv_txn_compact_to_voter_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        request: &'a KvTxnRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        Option<sinomo_kv::ClusterKvTxnCompactCommitResult>,
        Self::Error,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        let options = self.options.forward_options;
        let operation_deadline = self.deadline;
        let host = &mut *self.host;
        Box::pin(async move {
            let Some(options) =
                cluster_kv_forward_options_before_deadline(options, operation_deadline)
            else {
                return Err(authoritative_read_phase_timeout(
                    "compact cluster KV transaction forwarding",
                ));
            };
            let leader_fence = try_capture_cluster_raft_voter_leader_fence(paths, state);
            let result = forward_cluster_kv_txn_compact_to_state_authority_async_with_host(
                runtime, paths, state, request, options, host,
            )
            .await;
            if let Err(error) = &result
                && clear_verified_stale_cluster_raft_leader_async(
                    runtime,
                    paths,
                    state,
                    leader_fence.as_ref(),
                    error,
                    operation_deadline,
                )
                .await?
                && let Some(options) =
                    cluster_kv_forward_options_before_deadline(options, operation_deadline)
            {
                return forward_cluster_kv_txn_compact_to_state_authority_async_with_host(
                    runtime, paths, state, request, options, host,
                )
                .await;
            }
            result
        })
    }

    fn elect_cluster_raft_leader_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, bool, Self::Error> {
        let runtime = self.runtime;
        let paths = self.paths;
        let options = self.options.leader_election_options;
        let deadline = self.deadline;
        let host = &mut *self.host;
        Box::pin(async move {
            let options = cluster_raft_leader_election_options_before_deadline(
                options,
                deadline,
                "cluster KV transaction leader election",
            )?;
            // Election owns an absolute deadline and must finish any admitted
            // higher-term persistence before returning. An outer timeout here
            // would cancel that mandatory safety write at the boundary.
            elect_cluster_raft_leader_async_with_host(runtime, paths, state, options, host).await
        })
    }

    fn reachable_voter_count_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, usize, Self::Error> {
        let paths = self.paths;
        let options = self.options.state_authority_reachability_options;
        let deadline = self.deadline;
        let host = &mut *self.host;
        Box::pin(async move {
            let remaining =
                remaining_authoritative_read_budget(deadline, "cluster KV reachable voter check")?;
            tokio::time::timeout(
                remaining,
                cluster_reachable_state_authority_count_async_with_host(
                    paths, state, options, host,
                ),
            )
            .await
            .map_err(|_| authoritative_read_phase_timeout("cluster KV reachable voter check"))?
        })
    }

    fn commit_cluster_raft_txn_with_quorum_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        sinomo_kv::ClusterKvRaftQuorumCommitResult,
        Self::Error,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        let options = self.options;
        let deadline = self.deadline;
        let host = &mut *self.host;
        Box::pin(async move {
            let remaining = remaining_authoritative_read_budget(
                deadline,
                "cluster KV transaction quorum commit",
            )?;
            commit_cluster_raft_command_with_quorum_async_with_host(
                runtime,
                paths,
                state,
                request.into_command_request(),
                ClusterKvRaftQuorumCommitOptions { timeout: remaining },
                options.raft_append_replication_options,
                host,
            )
            .await
        })
    }

    fn commit_cluster_raft_txn_compact_with_quorum_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        sinomo_kv::ClusterKvRaftQuorumCompactCommitResult,
        Self::Error,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        let options = self.options;
        let deadline = self.deadline;
        let host = &mut *self.host;
        Box::pin(async move {
            let remaining = remaining_authoritative_read_budget(
                deadline,
                "compact cluster KV transaction quorum commit",
            )?;
            commit_cluster_raft_command_compact_with_quorum_async_with_host(
                runtime,
                paths,
                state,
                request.into_command_request(),
                ClusterKvRaftQuorumCommitOptions { timeout: remaining },
                options.raft_append_replication_options,
                host,
            )
            .await
        })
    }

    fn cluster_quorum_commit_error_allows_retry(&mut self, error: &Self::Error) -> bool {
        matches!(error, NetworkError::ClusterReadOnly { .. })
    }

    fn cluster_authority_error(&mut self, failure: sinomo_kv::KvAuthorityFailure) -> Self::Error {
        NetworkError::ClusterReadOnly {
            detail: serde_json::to_string(&failure).unwrap_or_else(|_| format!("{failure:?}")),
        }
    }
}

pub async fn commit_cluster_kv_txn_with_quorum_response_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvTxnRequest,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<ClusterKvTxnCommitResult, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let started = Instant::now();
    let deadline = started
        .checked_add(options.raft_append_timeout)
        .unwrap_or(started);
    let mut consensus_state = project_committed_cluster_raft_authority_before_deadline(
        runtime,
        paths,
        state,
        deadline,
        "project committed cluster Raft config for transaction",
    )
    .await?;
    let allow_forwarding = options.allow_forwarding;
    let mut kv_host = NetworkClusterKvTxnQuorumAsyncHost {
        runtime,
        paths,
        options,
        deadline,
        host,
    };
    let result = sinomo_kv::commit_cluster_kv_txn_with_quorum_async_with_host(
        &mut consensus_state,
        request,
        sinomo_kv::ClusterKvTxnQuorumDriverOptions { allow_forwarding },
        &mut kv_host,
    )
    .await?;
    state.term = state.term.max(consensus_state.term);
    state.kv_revision = state.kv_revision.max(consensus_state.kv_revision);
    // A forwarded commit observes the voter's post-commit snapshot; import it
    // when it advances the local view so the forwarding node reads its own
    // write immediately. The committed-append fan-out runs on the voter and
    // can miss this node (cold formation: no relay route back yet), and
    // re-committing the same payload is a semantic no-effect that triggers no
    // new fan-out. Importing the response here makes convergence owned by the
    // forwarded operation instead of relying on a background snapshot sweep.
    let import_paths = paths.clone();
    let import_snapshot = result.snapshot.clone();
    runtime
        .spawn_blocking(move || {
            if imported_snapshot_advances_local_kv(&import_paths, &import_snapshot) {
                write_imported_cluster_kv_snapshot_if_changed(&import_paths, &import_snapshot)?;
            }
            Ok::<_, NetworkError>(())
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster kv snapshot import".to_string(),
            detail: format!("cluster kv snapshot import task failed: {source}"),
        })??;
    Ok(result)
}

pub async fn commit_cluster_kv_txn_compact_with_quorum_response_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvTxnRequest,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<sinomo_kv::ClusterKvTxnCompactCommitResult, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let started = Instant::now();
    let deadline = started
        .checked_add(options.raft_append_timeout)
        .unwrap_or(started);
    let mut consensus_state = project_committed_cluster_raft_authority_before_deadline(
        runtime,
        paths,
        state,
        deadline,
        "project committed cluster Raft config for compact transaction",
    )
    .await?;
    let allow_forwarding = options.allow_forwarding;
    let mut kv_host = NetworkClusterKvTxnQuorumAsyncHost {
        runtime,
        paths,
        options,
        deadline,
        host,
    };
    let result = sinomo_kv::commit_cluster_kv_txn_compact_with_quorum_async_with_host(
        &mut consensus_state,
        request,
        sinomo_kv::ClusterKvTxnQuorumDriverOptions { allow_forwarding },
        &mut kv_host,
    )
    .await?;
    state.term = state.term.max(consensus_state.term);
    state.kv_revision = state.kv_revision.max(consensus_state.kv_revision);
    Ok(result)
}

pub async fn read_cluster_kv_authoritative_snapshot_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let started = Instant::now();
    let result = read_cluster_kv_authoritative_snapshot_async_with_host_inner(
        runtime, paths, state, options, host,
    )
    .await;
    record_async_authoritative_read_latency(started, "snapshot", result.is_ok());
    result
}

async fn read_cluster_kv_authoritative_snapshot_async_with_host_inner<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let deadline = Instant::now() + options.raft_append_timeout;
    let mut consensus_state = state.clone();
    let read = read_cluster_kv_authoritative_snapshot_async_with_consensus_state(
        runtime,
        paths,
        &mut consensus_state,
        options,
        deadline,
        host,
    )
    .await?;
    adopt_newer_authoritative_consensus_members(state, &consensus_state);
    state.term = state
        .term
        .max(consensus_state.term)
        .max(read.authority.term);
    state.kv_revision = state
        .kv_revision
        .max(consensus_state.kv_revision)
        .max(read.authority.revision.0);
    Ok(read)
}

fn retained_request_outcome_read(
    read: KvAuthoritativeRead<Option<sinomo_kv::KvRequestOutcome>>,
) -> sinomo_kv::ClusterKvRequestOutcomeRead {
    sinomo_kv::ClusterKvRequestOutcomeRead {
        authority: read.authority,
        outcome: read
            .value
            .unwrap_or_else(|| sinomo_kv::KvRequestOutcome::Unknown {
                detail: "request id has no retained committed record".to_string(),
            }),
    }
}

/// Publish a membership projection only when the consensus attempt advanced
/// beyond the caller's KV generation. The scalar revision and its topology
/// must move together; otherwise a successful resident-newer read would leave
/// the caller advertising the new generation with stale voter/member fields.
fn adopt_newer_authoritative_consensus_members(
    state: &mut ClusterBootstrapState,
    consensus_state: &ClusterBootstrapState,
) {
    if consensus_state.kv_revision > state.kv_revision {
        state.members.clone_from(&consensus_state.members);
        state.updated_at = state.updated_at.max(consensus_state.updated_at);
    }
}

async fn overlay_node_local_request_attempt_after_authoritative_history(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request_id: &sinomo_kv::KvRequestId,
    mut read: sinomo_kv::ClusterKvRequestOutcomeRead,
    consult_node_local_attempt: bool,
    deadline: Instant,
) -> Result<sinomo_kv::ClusterKvRequestOutcomeRead, NetworkError> {
    if !consult_node_local_attempt
        || matches!(
            read.outcome,
            sinomo_kv::KvRequestOutcome::Committed(_) | sinomo_kv::KvRequestOutcome::Expired
        )
    {
        return Ok(read);
    }

    // Local attempt records are not consensus history. Consult them only after
    // the attached ReadIndex proof has ruled out a retained commit/expiry, and
    // never let them override a final authoritative outcome.
    struct AbortRequestAttemptTaskOnDrop<T> {
        task: tokio::task::JoinHandle<T>,
    }

    impl<T> Drop for AbortRequestAttemptTaskOnDrop<T> {
        fn drop(&mut self) {
            // `spawn_blocking` work can still be cancelled while it is queued.
            // Once admitted, the absolute operation deadline passed below
            // bounds every ledger/cache lock and the cold replay itself.
            self.task.abort();
        }
    }

    let remaining = remaining_authoritative_read_budget(
        deadline,
        "authoritative KV request-outcome attempt lookup",
    )?;
    let kv_paths = paths.kv_paths();
    let request_id = request_id.clone();
    let mut task = AbortRequestAttemptTaskOnDrop {
        task: runtime.spawn_blocking(move || {
            sinomo_kv::read_cluster_kv_request_attempt_before(&kv_paths, &request_id, deadline)
                .map_err(kv_err)
        }),
    };
    let local_outcome = match tokio::time::timeout(remaining, &mut task.task).await {
        Ok(result) => result.map_err(|source| NetworkError::Io {
            operation: "read cluster KV request attempt".to_string(),
            detail: format!("cluster KV request-attempt task failed: {source}"),
        })??,
        Err(_) => {
            task.task.abort();
            return Err(authoritative_read_phase_timeout(
                "authoritative KV request-outcome attempt lookup",
            ));
        }
    };
    remaining_authoritative_read_budget(
        deadline,
        "authoritative KV request-outcome attempt lookup",
    )?;
    if let Some(local_outcome) = local_outcome {
        read.outcome = local_outcome;
    }
    Ok(read)
}

/// Resolves one request id through a quorum-confirmed, bounded resident lookup.
///
/// The leader uses etcd-style ReadIndex and reads only the committed-request
/// and expiry indexes. Followers forward this same request-outcome RPC instead
/// of fetching the complete keyspace. Node-local attempt state is overlaid only
/// after authoritative retained history has ruled out a committed/expired
/// outcome. RPC authorities pass `consult_node_local_attempt = false`; the
/// original local caller performs that overlay so an authority's unrelated
/// attempt ledger cannot mask the originating node's state.
pub async fn read_cluster_kv_request_outcome_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request_id: sinomo_kv::KvRequestId,
    consult_node_local_attempt: bool,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<sinomo_kv::ClusterKvRequestOutcomeRead, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let deadline = Instant::now() + options.raft_append_timeout;
    let mut consensus_state = state.clone();
    let request_id_for_read = request_id.clone();
    let remaining = remaining_authoritative_read_budget(
        deadline,
        "authoritative KV request-outcome ReadIndex",
    )?;
    let (local_admission_busy, forward_only) =
        match read_cluster_raft_with_read_index_attempt_async_with_host_and_admission(
            runtime,
            paths,
            &mut consensus_state,
            remaining,
            options.raft_append_replication_options,
            ClusterRaftReadIndexAdmissionMode::ForwardOnBusy,
            move |storage| storage.read_index_request_outcome(&request_id_for_read),
            host,
        )
        .await?
        {
            ClusterRaftReadIndexAttempt::Confirmed(read) => {
                let read = overlay_node_local_request_attempt_after_authoritative_history(
                    runtime,
                    paths,
                    &request_id,
                    retained_request_outcome_read(read),
                    consult_node_local_attempt,
                    deadline,
                )
                .await?;
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
            ClusterRaftReadIndexAttempt::NeedsBarrier(fence) => {
                let request_id_for_read = request_id.clone();
                let read = commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence(
                    runtime,
                    paths,
                    &mut consensus_state,
                    options,
                    deadline,
                    Some(fence.into()),
                    move |storage| storage.read_index_request_outcome(&request_id_for_read),
                    host,
                )
                .await?;
                let read = overlay_node_local_request_attempt_after_authoritative_history(
                    runtime,
                    paths,
                    &request_id,
                    retained_request_outcome_read(read),
                    consult_node_local_attempt,
                    deadline,
                )
                .await?;
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
            ClusterRaftReadIndexAttempt::NotLeader => (false, None),
            ClusterRaftReadIndexAttempt::ForwardOnly(reason) => {
                reason.apply_forwarding_floor(&mut consensus_state)?;
                (false, Some(reason))
            }
            ClusterRaftReadIndexAttempt::LocalAdmissionBusy => (true, None),
        };

    let mut forwarding_failure = None;
    if options.allow_forwarding {
        remaining_authoritative_read_budget(
            deadline,
            "authoritative KV request-outcome forwarding",
        )?;
        match forward_cluster_kv_request_outcome_recovering_stale_leader_async(
            runtime,
            paths,
            &consensus_state,
            &request_id,
            options.forward_options,
            deadline,
            host,
        )
        .await
        {
            Ok(Some(read)) => {
                let read = overlay_node_local_request_attempt_after_authoritative_history(
                    runtime,
                    paths,
                    &request_id,
                    read,
                    consult_node_local_attempt,
                    deadline,
                )
                .await?;
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
            Ok(None) => {}
            Err(error) => forwarding_failure = Some(error),
        }
    }
    if let Some(reason) = forward_only {
        return Err(reason.into_error());
    }
    if local_admission_busy {
        let request_id_for_read = request_id.clone();
        if let Ok(Some(retry)) = retry_local_read_index_after_busy_forward(
            runtime,
            paths,
            &mut consensus_state,
            options,
            deadline,
            "authoritative KV request-outcome local admission retry",
            ClusterRaftReadProjectionPolicy::DeadlineBounded,
            LOCAL_READ_INDEX_BUSY_RETRY_BUDGET,
            move |storage| storage.read_index_request_outcome(&request_id_for_read),
            host,
        )
        .await
        {
            let read = match retry {
                ClusterRaftReadIndexAttempt::Confirmed(read) => Some(read),
                ClusterRaftReadIndexAttempt::NeedsBarrier(fence) => {
                    let request_id_for_read = request_id.clone();
                    commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence(
                        runtime,
                        paths,
                        &mut consensus_state,
                        options,
                        deadline,
                        Some(fence.into()),
                        move |storage| storage.read_index_request_outcome(&request_id_for_read),
                        host,
                    )
                    .await
                    .ok()
                }
                ClusterRaftReadIndexAttempt::ForwardOnly(reason) => {
                    return Err(reason.into_error());
                }
                ClusterRaftReadIndexAttempt::NotLeader
                | ClusterRaftReadIndexAttempt::LocalAdmissionBusy => None,
            };
            if let Some(read) = read {
                let read = overlay_node_local_request_attempt_after_authoritative_history(
                    runtime,
                    paths,
                    &request_id,
                    retained_request_outcome_read(read),
                    consult_node_local_attempt,
                    deadline,
                )
                .await?;
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
        }
        return Err(forwarding_failure.unwrap_or_else(|| NetworkError::ClusterReadOnly {
            detail: "local Raft admission is busy and no authority accepted the forwarded request-outcome read; local election was skipped"
                .to_string(),
        }));
    }
    if !sinomo_kv::cluster_kv_raft_local_is_voter(&consensus_state) {
        return Err(forwarding_failure.unwrap_or_else(|| NetworkError::ClusterReadOnly {
            detail: "local node is not a Raft voter and could not forward an authoritative request outcome"
                .to_string(),
        }));
    }
    if !options.allow_forwarding {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "{FORWARDED_CLUSTER_KV_READ_NON_LEADER_DETAIL}; forwarded requests do not start elections"
            ),
        });
    }
    let election_options = cluster_raft_leader_election_options_before_deadline(
        options.leader_election_options,
        deadline,
        "authoritative KV request-outcome leader election",
    )?;
    if !elect_cluster_raft_leader_async_with_host(
        runtime,
        paths,
        &consensus_state,
        election_options,
        host,
    )
    .await?
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: forwarding_failure.map_or_else(
                || "local node could not establish current-term Raft leadership".to_string(),
                |error| {
                    format!(
                        "local node could not establish current-term Raft leadership; forwarding failed: {error}"
                    )
                },
            ),
        });
    }
    let request_id_for_read = request_id.clone();
    let read = commit_cluster_kv_authoritative_barrier_then_read(
        runtime,
        paths,
        &mut consensus_state,
        options,
        deadline,
        move |storage| storage.read_index_request_outcome(&request_id_for_read),
        host,
    )
    .await?;
    let read = overlay_node_local_request_attempt_after_authoritative_history(
        runtime,
        paths,
        &request_id,
        retained_request_outcome_read(read),
        consult_node_local_attempt,
        deadline,
    )
    .await?;
    adopt_newer_authoritative_consensus_members(state, &consensus_state);
    state.term = state
        .term
        .max(consensus_state.term)
        .max(read.authority.term);
    state.kv_revision = state
        .kv_revision
        .max(consensus_state.kv_revision)
        .max(read.authority.revision.0);
    Ok(read)
}

/// Reads an ordered exact-key set from one quorum-confirmed resident Raft
/// view. The steady-state path establishes ReadIndex once and projects every
/// key from the same indexed store view; it never materializes a full snapshot
/// and never retries individual keys until revisions happen to match.
pub async fn read_cluster_kv_authoritative_entries_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    keys: Vec<String>,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<KvAuthoritativeRead<Vec<Option<sinomo_kv::KvEntry>>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let started = Instant::now();
    let result = read_cluster_kv_authoritative_entries_async_with_host_inner(
        runtime, paths, state, keys, options, host,
    )
    .await;
    record_async_authoritative_read_latency(started, "entries", result.is_ok());
    result
}

async fn read_cluster_kv_authoritative_entries_async_with_host_inner<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    keys: Vec<String>,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<KvAuthoritativeRead<Vec<Option<sinomo_kv::KvEntry>>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    sinomo_kv::validate_cluster_kv_authoritative_entries_request_keys(&keys).map_err(kv_err)?;
    let deadline = Instant::now() + options.raft_append_timeout;
    let mut consensus_state = state.clone();
    let keys_for_read = keys.clone();
    let remaining = remaining_authoritative_read_budget(
        deadline,
        "authoritative KV exact-key-set authority proof",
    )?;
    let (local_admission_busy, forward_only) =
        match read_cluster_raft_with_read_index_attempt_async_with_host_and_admission(
            runtime,
            paths,
            &mut consensus_state,
            remaining,
            options.raft_append_replication_options,
            // Authority requests must not spend their network-sized budget
            // queued behind a process-local Raft checkout. A local caller can
            // forward on `Busy`; an already-forwarded request fails promptly
            // so its caller can retry after the in-flight mutation completes.
            ClusterRaftReadIndexAdmissionMode::ForwardOnBusy,
            move |storage| storage.read_index_entries(&keys_for_read),
            host,
        )
        .await?
        {
            ClusterRaftReadIndexAttempt::Confirmed(read) => {
                sinomo_kv::validate_cluster_kv_authoritative_entries_read(&keys, &read)
                    .map_err(kv_err)?;
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
            ClusterRaftReadIndexAttempt::NeedsBarrier(fence) => {
                let keys_for_read = keys.clone();
                let read = commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence(
                    runtime,
                    paths,
                    &mut consensus_state,
                    options,
                    deadline,
                    Some(fence.into()),
                    move |storage| storage.read_index_entries(&keys_for_read),
                    host,
                )
                .await?;
                sinomo_kv::validate_cluster_kv_authoritative_entries_read(&keys, &read)
                    .map_err(kv_err)?;
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
            // The ReadIndex attempt already adopted the durable fence with
            // monotonic term/revision rules. Replacing the whole state from the
            // returned payload here could regress a newer caller floor.
            ClusterRaftReadIndexAttempt::NotLeader => (false, None),
            ClusterRaftReadIndexAttempt::ForwardOnly(reason) => {
                reason.apply_forwarding_floor(&mut consensus_state)?;
                (false, Some(reason))
            }
            ClusterRaftReadIndexAttempt::LocalAdmissionBusy => (true, None),
        };

    let mut forwarding_failure = None;
    if options.allow_forwarding {
        remaining_authoritative_read_budget(deadline, "authoritative KV exact-key-set forwarding")?;
        match forward_cluster_kv_authoritative_entries_recovering_stale_leader_async(
            runtime,
            paths,
            &consensus_state,
            &keys,
            options.forward_options,
            deadline,
            host,
        )
        .await
        {
            Ok(Some(read)) => {
                let read =
                    validate_forwarded_authoritative_entries_read(read, &consensus_state, &keys)?;
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
            Ok(None) => {}
            Err(error) if exact_key_set_route_is_definitely_unsupported(&error) => {
                // Mixed-version only: an old authority can prove it does not
                // know the new bounded route. Preserve one-view semantics by
                // fetching one authoritative snapshot and projecting the set.
                // Timeouts and ambiguous delivery errors never enter here.
                metrics::counter!(
                    "sinomo.kv.authoritative_read",
                    "path" => "compat_snapshot_exact_set"
                )
                .increment(1);
                remaining_authoritative_read_budget(
                    deadline,
                    "authoritative KV exact-key-set mixed-version snapshot fallback",
                )?;
                match forward_cluster_kv_authoritative_snapshot_recovering_stale_leader_async(
                    runtime,
                    paths,
                    &consensus_state,
                    options.forward_options,
                    deadline,
                    host,
                )
                .await
                {
                    Ok(Some(snapshot)) => {
                        let read = project_mixed_version_authoritative_entries_snapshot(
                            snapshot,
                            &consensus_state,
                            &keys,
                        )?;
                        adopt_newer_authoritative_consensus_members(state, &consensus_state);
                        state.term = state
                            .term
                            .max(consensus_state.term)
                            .max(read.authority.term);
                        state.kv_revision = state
                            .kv_revision
                            .max(consensus_state.kv_revision)
                            .max(read.authority.revision.0);
                        return Ok(read);
                    }
                    Ok(None) => forwarding_failure = Some(error),
                    Err(fallback_error) => forwarding_failure = Some(fallback_error),
                }
            }
            Err(error) => forwarding_failure = Some(error),
        }
    }
    if let Some(reason) = forward_only {
        return Err(reason.into_error());
    }
    if !local_admission_busy
        && forwarding_failure
            .as_ref()
            .is_some_and(forwarded_exact_key_set_read_admission_is_busy)
    {
        match retry_forward_cluster_kv_authoritative_entries_after_busy(
            runtime,
            paths,
            &consensus_state,
            &keys,
            options.forward_options,
            deadline,
            host,
        )
        .await
        {
            Ok(Some(read)) => {
                let read =
                    validate_forwarded_authoritative_entries_read(read, &consensus_state, &keys)?;
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
            Ok(None) => {}
            Err(error) => forwarding_failure = Some(error),
        }
    }
    if local_admission_busy {
        let keys_for_read = keys.clone();
        if let Ok(Some(retry)) = retry_local_read_index_after_busy_forward(
            runtime,
            paths,
            &mut consensus_state,
            options,
            deadline,
            "authoritative KV exact-key-set local admission retry",
            ClusterRaftReadProjectionPolicy::DeadlineBounded,
            BOUNDED_AUTHORITY_BUSY_RETRY_BUDGET,
            move |storage| storage.read_index_entries(&keys_for_read),
            host,
        )
        .await
        {
            let read = match retry {
                ClusterRaftReadIndexAttempt::Confirmed(read) => Some(read),
                ClusterRaftReadIndexAttempt::NeedsBarrier(fence) => {
                    let keys_for_read = keys.clone();
                    commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence(
                        runtime,
                        paths,
                        &mut consensus_state,
                        options,
                        deadline,
                        Some(fence.into()),
                        move |storage| storage.read_index_entries(&keys_for_read),
                        host,
                    )
                    .await
                    .ok()
                }
                ClusterRaftReadIndexAttempt::ForwardOnly(reason) => {
                    reason.apply_forwarding_floor(&mut consensus_state)?;
                    None
                }
                ClusterRaftReadIndexAttempt::NotLeader
                | ClusterRaftReadIndexAttempt::LocalAdmissionBusy => None,
            };
            if let Some(read) = read {
                sinomo_kv::validate_cluster_kv_authoritative_entries_read(&keys, &read)
                    .map_err(kv_err)?;
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
        }
        if options.allow_forwarding
            && forwarding_failure
                .as_ref()
                .is_none_or(forwarded_exact_key_set_read_admission_is_busy)
        {
            match retry_forward_cluster_kv_authoritative_entries_after_busy(
                runtime,
                paths,
                &consensus_state,
                &keys,
                options.forward_options,
                deadline,
                host,
            )
            .await
            {
                Ok(Some(read)) => {
                    let read = validate_forwarded_authoritative_entries_read(
                        read,
                        &consensus_state,
                        &keys,
                    )?;
                    adopt_newer_authoritative_consensus_members(state, &consensus_state);
                    state.term = state
                        .term
                        .max(consensus_state.term)
                        .max(read.authority.term);
                    state.kv_revision = state
                        .kv_revision
                        .max(consensus_state.kv_revision)
                        .max(read.authority.revision.0);
                    return Ok(read);
                }
                Ok(None) => {}
                Err(error) => forwarding_failure = Some(error),
            }
        }
        return Err(forwarding_failure.unwrap_or_else(|| NetworkError::ClusterReadOnly {
            detail: "local Raft admission is busy and no authority accepted the forwarded exact-key-set read; local election was skipped"
                .to_string(),
        }));
    }
    if !sinomo_kv::cluster_kv_raft_local_is_voter(&consensus_state) {
        return Err(forwarding_failure.unwrap_or_else(|| NetworkError::ClusterReadOnly {
            detail: "local node is not a Raft voter and could not forward an authoritative exact-key-set read"
                .to_string(),
        }));
    }
    if !options.allow_forwarding {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "{FORWARDED_CLUSTER_KV_EXACT_READ_NON_LEADER_DETAIL}; forwarded requests do not start elections"
            ),
        });
    }
    let election_options = cluster_raft_leader_election_options_before_deadline(
        options.leader_election_options,
        deadline,
        "authoritative KV leader election",
    )?;
    if !elect_cluster_raft_leader_async_with_host(
        runtime,
        paths,
        &consensus_state,
        election_options,
        host,
    )
    .await?
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: forwarding_failure.map_or_else(
                || "local node could not establish current-term Raft leadership".to_string(),
                |error| {
                    format!(
                        "local node could not establish current-term Raft leadership; forwarding failed: {error}"
                    )
                },
            ),
        });
    }
    let keys_for_read = keys.clone();
    let read = commit_cluster_kv_authoritative_barrier_then_read(
        runtime,
        paths,
        &mut consensus_state,
        options,
        deadline,
        move |storage| storage.read_index_entries(&keys_for_read),
        host,
    )
    .await?;
    sinomo_kv::validate_cluster_kv_authoritative_entries_read(&keys, &read).map_err(kv_err)?;
    adopt_newer_authoritative_consensus_members(state, &consensus_state);
    state.term = state
        .term
        .max(consensus_state.term)
        .max(read.authority.term);
    state.kv_revision = state
        .kv_revision
        .max(consensus_state.kv_revision)
        .max(read.authority.revision.0);
    Ok(read)
}

/// Reads one exact key from a quorum-confirmed resident Raft view without
/// materializing the complete KV snapshot on the steady-state leader path.
pub async fn read_cluster_kv_authoritative_entry_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    key: String,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<KvAuthoritativeRead<Option<sinomo_kv::KvEntry>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let started = Instant::now();
    let result = read_cluster_kv_authoritative_entry_async_with_host_inner(
        runtime, paths, state, key, options, host,
    )
    .await;
    record_async_authoritative_read_latency(started, "entry", result.is_ok());
    result
}

async fn read_cluster_kv_authoritative_entry_async_with_host_inner<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    key: String,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<KvAuthoritativeRead<Option<sinomo_kv::KvEntry>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let deadline = Instant::now() + options.raft_append_timeout;
    let mut consensus_state = state.clone();
    let key_for_read = key.clone();
    let remaining =
        remaining_authoritative_read_budget(deadline, "authoritative KV point authority proof")?;
    let (local_admission_busy, forward_only) =
        match read_cluster_raft_with_read_index_attempt_async_with_host_and_admission(
            runtime,
            paths,
            &mut consensus_state,
            remaining,
            options.raft_append_replication_options,
            ClusterRaftReadIndexAdmissionMode::ForwardOnBusy,
            move |storage| storage.read_index_entry(&key_for_read),
            host,
        )
        .await?
        {
            ClusterRaftReadIndexAttempt::Confirmed(read) => {
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
            ClusterRaftReadIndexAttempt::NeedsBarrier(fence) => {
                // Carry the exact first-current-term fence into the compact
                // barrier. Re-projecting config and leadership here would reopen
                // the resident node a second time before useful work starts.
                let key_for_read = key.clone();
                let read = commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence(
                    runtime,
                    paths,
                    &mut consensus_state,
                    options,
                    deadline,
                    Some(fence.into()),
                    move |storage| storage.read_index_entry(&key_for_read),
                    host,
                )
                .await?;
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
            ClusterRaftReadIndexAttempt::NotLeader => (false, None),
            ClusterRaftReadIndexAttempt::ForwardOnly(reason) => {
                reason.apply_forwarding_floor(&mut consensus_state)?;
                (false, Some(reason))
            }
            ClusterRaftReadIndexAttempt::LocalAdmissionBusy => (true, None),
        };

    let mut forwarding_failure = None;
    if options.allow_forwarding {
        remaining_authoritative_read_budget(deadline, "authoritative KV point forwarding")?;
        match forward_cluster_kv_authoritative_entry_recovering_stale_leader_async(
            runtime,
            paths,
            &consensus_state,
            &key,
            options.forward_options,
            deadline,
            host,
        )
        .await
        {
            Ok(Some(read)) => {
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
            Ok(None) => {}
            Err(error) => forwarding_failure = Some(error),
        }
    }
    if let Some(reason) = forward_only {
        return Err(reason.into_error());
    }
    if !local_admission_busy
        && forwarding_failure
            .as_ref()
            .is_some_and(forwarded_point_read_admission_is_busy)
    {
        match retry_forward_cluster_kv_authoritative_entry_after_busy(
            runtime,
            paths,
            &consensus_state,
            &key,
            options.forward_options,
            deadline,
            host,
        )
        .await
        {
            Ok(Some(read)) => {
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
            Ok(None) => {}
            Err(error) => forwarding_failure = Some(error),
        }
    }
    if local_admission_busy {
        let key_for_read = key.clone();
        if let Ok(Some(retry)) = retry_local_read_index_after_busy_forward(
            runtime,
            paths,
            &mut consensus_state,
            options,
            deadline,
            "authoritative KV point local admission retry",
            ClusterRaftReadProjectionPolicy::DeadlineBounded,
            BOUNDED_AUTHORITY_BUSY_RETRY_BUDGET,
            move |storage| storage.read_index_entry(&key_for_read),
            host,
        )
        .await
        {
            let read = match retry {
                ClusterRaftReadIndexAttempt::Confirmed(read) => Some(read),
                ClusterRaftReadIndexAttempt::NeedsBarrier(fence) => {
                    let key_for_read = key.clone();
                    commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence(
                        runtime,
                        paths,
                        &mut consensus_state,
                        options,
                        deadline,
                        Some(fence.into()),
                        move |storage| storage.read_index_entry(&key_for_read),
                        host,
                    )
                    .await
                    .ok()
                }
                ClusterRaftReadIndexAttempt::ForwardOnly(reason) => {
                    reason.apply_forwarding_floor(&mut consensus_state)?;
                    None
                }
                ClusterRaftReadIndexAttempt::NotLeader
                | ClusterRaftReadIndexAttempt::LocalAdmissionBusy => None,
            };
            if let Some(read) = read {
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
        }
        if options.allow_forwarding
            && forwarding_failure
                .as_ref()
                .is_none_or(forwarded_point_read_admission_is_busy)
        {
            match retry_forward_cluster_kv_authoritative_entry_after_busy(
                runtime,
                paths,
                &consensus_state,
                &key,
                options.forward_options,
                deadline,
                host,
            )
            .await
            {
                Ok(Some(read)) => {
                    adopt_newer_authoritative_consensus_members(state, &consensus_state);
                    state.term = state
                        .term
                        .max(consensus_state.term)
                        .max(read.authority.term);
                    state.kv_revision = state
                        .kv_revision
                        .max(consensus_state.kv_revision)
                        .max(read.authority.revision.0);
                    return Ok(read);
                }
                Ok(None) => {}
                Err(error) => forwarding_failure = Some(error),
            }
        }
        return Err(forwarding_failure.unwrap_or_else(|| NetworkError::ClusterReadOnly {
            detail: "local Raft admission is busy and no authority accepted the forwarded point read; local election was skipped"
                .to_string(),
        }));
    }
    if !sinomo_kv::cluster_kv_raft_local_is_voter(&consensus_state) {
        return Err(
            forwarding_failure.unwrap_or_else(|| {
                NetworkError::ClusterReadOnly {
            detail:
                "local node is not a Raft voter and could not forward an authoritative point read"
                    .to_string(),
        }
            }),
        );
    }
    if !options.allow_forwarding {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "{FORWARDED_CLUSTER_KV_POINT_READ_NON_LEADER_DETAIL}; forwarded requests do not start elections"
            ),
        });
    }
    let election_options = cluster_raft_leader_election_options_before_deadline(
        options.leader_election_options,
        deadline,
        "authoritative KV leader election",
    )?;
    if !elect_cluster_raft_leader_async_with_host(
        runtime,
        paths,
        &consensus_state,
        election_options,
        host,
    )
    .await?
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: forwarding_failure.map_or_else(
                || "local node could not establish current-term Raft leadership".to_string(),
                |error| {
                    format!(
                        "local node could not establish current-term Raft leadership; forwarding failed: {error}"
                    )
                },
            ),
        });
    }
    let key_for_read = key.clone();
    let read = commit_cluster_kv_authoritative_barrier_then_read(
        runtime,
        paths,
        &mut consensus_state,
        options,
        deadline,
        move |storage| storage.read_index_entry(&key_for_read),
        host,
    )
    .await?;
    adopt_newer_authoritative_consensus_members(state, &consensus_state);
    state.term = state
        .term
        .max(consensus_state.term)
        .max(read.authority.term);
    state.kv_revision = state
        .kv_revision
        .max(consensus_state.kv_revision)
        .max(read.authority.revision.0);
    Ok(read)
}

/// Reads one bounded prefix from a quorum-confirmed resident Raft view.
///
/// The steady-state leader path uses ReadIndex and projects directly from the
/// indexed store. Leadership changes and the first read in a term retain the
/// existing snapshot/barrier path, preserving forwarding and election
/// semantics while avoiding an `O(total keyspace)` snapshot on normal prefix
/// reads.
pub async fn read_cluster_kv_authoritative_range_prefix_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    prefix: String,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<KvAuthoritativeRead<Vec<sinomo_kv::KvEntry>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let started = Instant::now();
    let result = read_cluster_kv_authoritative_range_prefix_async_with_host_inner(
        runtime, paths, state, prefix, options, host,
    )
    .await;
    record_async_authoritative_read_latency(started, "prefix", result.is_ok());
    result
}

async fn read_cluster_kv_authoritative_range_prefix_async_with_host_inner<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    prefix: String,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<KvAuthoritativeRead<Vec<sinomo_kv::KvEntry>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let deadline = Instant::now() + options.raft_append_timeout;
    let mut consensus_state = state.clone();
    let prefix_for_read = prefix.clone();
    let remaining =
        remaining_authoritative_read_budget(deadline, "authoritative KV prefix authority proof")?;
    let (local_admission_busy, forward_only) =
        match read_cluster_raft_with_read_index_attempt_async_with_host_and_admission(
            runtime,
            paths,
            &mut consensus_state,
            remaining,
            options.raft_append_replication_options,
            ClusterRaftReadIndexAdmissionMode::ForwardOnBusy,
            move |storage| {
                storage.read_index_prefix_limited_by_payload_bytes(
                    &prefix_for_read,
                    sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                    sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
                )
            },
            host,
        )
        .await?
        {
            ClusterRaftReadIndexAttempt::Confirmed(read) => {
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
            ClusterRaftReadIndexAttempt::NeedsBarrier(fence) => {
                let prefix_for_read = prefix.clone();
                let read = commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence(
                    runtime,
                    paths,
                    &mut consensus_state,
                    options,
                    deadline,
                    Some(fence.into()),
                    move |storage| {
                        storage.read_index_prefix_limited_by_payload_bytes(
                            &prefix_for_read,
                            sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                            sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
                        )
                    },
                    host,
                )
                .await?;
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
            ClusterRaftReadIndexAttempt::NotLeader => (false, None),
            ClusterRaftReadIndexAttempt::ForwardOnly(reason) => {
                reason.apply_forwarding_floor(&mut consensus_state)?;
                (false, Some(reason))
            }
            ClusterRaftReadIndexAttempt::LocalAdmissionBusy => (true, None),
        };

    let mut forwarding_failure = None;
    if options.allow_forwarding {
        remaining_authoritative_read_budget(deadline, "authoritative KV prefix forwarding")?;
        match forward_cluster_kv_authoritative_range_prefix_recovering_stale_leader_async(
            runtime,
            paths,
            &consensus_state,
            &prefix,
            options.forward_options,
            deadline,
            host,
        )
        .await
        {
            Ok(Some(read)) => {
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
            Ok(None) => {}
            Err(error) => forwarding_failure = Some(error),
        }
    }
    if let Some(reason) = forward_only {
        return Err(reason.into_error());
    }
    if !local_admission_busy
        && forwarding_failure
            .as_ref()
            .is_some_and(forwarded_prefix_read_admission_is_busy)
    {
        match retry_forward_cluster_kv_authoritative_range_prefix_after_busy(
            runtime,
            paths,
            &consensus_state,
            &prefix,
            options.forward_options,
            deadline,
            host,
        )
        .await
        {
            Ok(Some(read)) => {
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
            Ok(None) => {}
            Err(error) => forwarding_failure = Some(error),
        }
    }
    if local_admission_busy {
        let prefix_for_read = prefix.clone();
        if let Ok(Some(retry)) = retry_local_read_index_after_busy_forward(
            runtime,
            paths,
            &mut consensus_state,
            options,
            deadline,
            "authoritative KV prefix local admission retry",
            ClusterRaftReadProjectionPolicy::DeadlineBounded,
            RANGE_AUTHORITY_BUSY_RETRY_BUDGET,
            move |storage| {
                storage.read_index_prefix_limited_by_payload_bytes(
                    &prefix_for_read,
                    sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                    sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
                )
            },
            host,
        )
        .await
        {
            let read = match retry {
                ClusterRaftReadIndexAttempt::Confirmed(read) => Some(read),
                ClusterRaftReadIndexAttempt::NeedsBarrier(fence) => {
                    let prefix_for_read = prefix.clone();
                    commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence(
                        runtime,
                        paths,
                        &mut consensus_state,
                        options,
                        deadline,
                        Some(fence.into()),
                        move |storage| {
                            storage.read_index_prefix_limited_by_payload_bytes(
                                &prefix_for_read,
                                sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                                sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
                            )
                        },
                        host,
                    )
                    .await
                    .ok()
                }
                ClusterRaftReadIndexAttempt::ForwardOnly(reason) => {
                    return Err(reason.into_error());
                }
                ClusterRaftReadIndexAttempt::NotLeader
                | ClusterRaftReadIndexAttempt::LocalAdmissionBusy => None,
            };
            if let Some(read) = read {
                adopt_newer_authoritative_consensus_members(state, &consensus_state);
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(read);
            }
        }
        // A follower can discover that it is not the leader only after the
        // contending local operation releases admission. The first forwarding
        // wave raced that same operation on every voter, so briefly re-probe the
        // now-settling authority topology before rejecting the read. This remains
        // forwarding-only recovery: it neither queues behind the local Raft FIFO
        // nor starts an election.
        if options.allow_forwarding
            && forwarding_failure
                .as_ref()
                .is_none_or(forwarded_prefix_read_admission_is_busy)
        {
            match retry_forward_cluster_kv_authoritative_range_prefix_after_busy(
                runtime,
                paths,
                &consensus_state,
                &prefix,
                options.forward_options,
                deadline,
                host,
            )
            .await
            {
                Ok(Some(read)) => {
                    adopt_newer_authoritative_consensus_members(state, &consensus_state);
                    state.term = state
                        .term
                        .max(consensus_state.term)
                        .max(read.authority.term);
                    state.kv_revision = state
                        .kv_revision
                        .max(consensus_state.kv_revision)
                        .max(read.authority.revision.0);
                    return Ok(read);
                }
                Ok(None) => {}
                Err(error) => forwarding_failure = Some(error),
            }
        }
        return Err(forwarding_failure.unwrap_or_else(|| NetworkError::ClusterReadOnly {
            detail: "local Raft admission is busy and no authority accepted the forwarded prefix read; local election was skipped"
                .to_string(),
        }));
    }
    if !sinomo_kv::cluster_kv_raft_local_is_voter(&consensus_state) {
        return Err(
            forwarding_failure.unwrap_or_else(|| {
                NetworkError::ClusterReadOnly {
            detail:
                "local node is not a Raft voter and could not forward an authoritative prefix read"
                    .to_string(),
        }
            }),
        );
    }
    if !options.allow_forwarding {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "{FORWARDED_CLUSTER_KV_PREFIX_READ_NON_LEADER_DETAIL}; forwarded requests do not start elections"
            ),
        });
    }
    let election_options = cluster_raft_leader_election_options_before_deadline(
        options.leader_election_options,
        deadline,
        "authoritative KV leader election",
    )?;
    if !elect_cluster_raft_leader_async_with_host(
        runtime,
        paths,
        &consensus_state,
        election_options,
        host,
    )
    .await?
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: forwarding_failure.map_or_else(
                || "local node could not establish current-term Raft leadership".to_string(),
                |error| {
                    format!(
                        "local node could not establish current-term Raft leadership; forwarding failed: {error}"
                    )
                },
            ),
        });
    }
    let prefix_for_read = prefix.clone();
    let read = commit_cluster_kv_authoritative_barrier_then_read(
        runtime,
        paths,
        &mut consensus_state,
        options,
        deadline,
        move |storage| {
            storage.read_index_prefix_limited_by_payload_bytes(
                &prefix_for_read,
                sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
            )
        },
        host,
    )
    .await?;
    adopt_newer_authoritative_consensus_members(state, &consensus_state);
    state.term = state
        .term
        .max(consensus_state.term)
        .max(read.authority.term);
    state.kv_revision = state
        .kv_revision
        .max(consensus_state.kv_revision)
        .max(read.authority.revision.0);
    Ok(read)
}

async fn read_cluster_kv_authoritative_snapshot_async_with_consensus_state<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    deadline: Instant,
    host: &mut H,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let remaining =
        remaining_authoritative_read_budget(deadline, "authoritative KV snapshot authority proof")?;
    let (local_admission_busy, forward_only) =
        match read_cluster_raft_with_read_index_attempt_async_with_projection_policy_and_admission(
            runtime,
            paths,
            state,
            remaining,
            options.raft_append_replication_options,
            ClusterRaftReadProjectionPolicy::CompleteAdmittedSnapshot,
            ClusterRaftReadIndexAdmissionMode::ForwardOnBusy,
            |storage| storage.read_index_snapshot(),
            host,
        )
        .await?
        {
            ClusterRaftReadIndexAttempt::Confirmed(read) => return Ok(read),
            ClusterRaftReadIndexAttempt::NeedsBarrier(fence) => {
                return commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence_and_projection_policy(
                runtime,
                paths,
                state,
                options,
                deadline,
                Some(fence.into()),
                ClusterRaftReadProjectionPolicy::CompleteAdmittedSnapshot,
                |storage| storage.read_index_snapshot(),
                host,
            )
            .await;
            }
            ClusterRaftReadIndexAttempt::NotLeader => (false, None),
            ClusterRaftReadIndexAttempt::ForwardOnly(reason) => {
                reason.apply_forwarding_floor(state)?;
                (false, Some(reason))
            }
            ClusterRaftReadIndexAttempt::LocalAdmissionBusy => (true, None),
        };

    let forwarding_failure = if options.allow_forwarding {
        remaining_authoritative_read_budget(deadline, "authoritative KV read forwarding")?;
        match forward_cluster_kv_authoritative_snapshot_recovering_stale_leader_async(
            runtime,
            paths,
            state,
            options.forward_options,
            deadline,
            host,
        )
        .await
        {
            Ok(Some(read)) => return validate_forwarded_authoritative_read(read, state),
            Ok(None) => None,
            Err(error) => Some(error),
        }
    } else {
        None
    };
    if let Some(reason) = forward_only {
        return Err(reason.into_error());
    }
    if !local_admission_busy
        && forwarding_failure
            .as_ref()
            .is_some_and(forwarded_snapshot_read_admission_is_busy)
    {
        // A responsive but busy authority is not evidence for a new election.
        return Err(forwarding_failure.expect("busy forwarding failure"));
    }
    if local_admission_busy {
        if let Ok(Some(retry)) = retry_local_read_index_after_busy_forward(
            runtime,
            paths,
            state,
            options,
            deadline,
            "authoritative KV snapshot local admission retry",
            ClusterRaftReadProjectionPolicy::CompleteAdmittedSnapshot,
            BOUNDED_AUTHORITY_BUSY_RETRY_BUDGET,
            |storage| storage.read_index_snapshot(),
            host,
        )
        .await
        {
            match retry {
                ClusterRaftReadIndexAttempt::Confirmed(read) => return Ok(read),
                ClusterRaftReadIndexAttempt::NeedsBarrier(fence) => {
                    if let Ok(read) = commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence_and_projection_policy(
                        runtime,
                        paths,
                        state,
                        options,
                        deadline,
                        Some(fence.into()),
                        ClusterRaftReadProjectionPolicy::CompleteAdmittedSnapshot,
                        |storage| storage.read_index_snapshot(),
                        host,
                    )
                    .await
                    {
                        return Ok(read);
                    }
                }
                ClusterRaftReadIndexAttempt::ForwardOnly(reason) => {
                    return Err(reason.into_error());
                }
                ClusterRaftReadIndexAttempt::NotLeader
                | ClusterRaftReadIndexAttempt::LocalAdmissionBusy => {}
            }
        }
        return Err(forwarding_failure.unwrap_or_else(|| NetworkError::ClusterReadOnly {
            detail: "local Raft admission is busy and no authority accepted the forwarded snapshot read; local election was skipped"
                .to_string(),
        }));
    }
    if !sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        return Err(
            forwarding_failure.unwrap_or_else(|| NetworkError::ClusterReadOnly {
                detail:
                    "local node is not a Raft voter and could not forward an authoritative read"
                        .to_string(),
            }),
        );
    }
    if !options.allow_forwarding {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "{FORWARDED_CLUSTER_KV_READ_NON_LEADER_DETAIL}; forwarded requests do not start elections"
            ),
        });
    }
    let election_options = cluster_raft_leader_election_options_before_deadline(
        options.leader_election_options,
        deadline,
        "authoritative KV leader election",
    )?;
    if !elect_cluster_raft_leader_async_with_host(runtime, paths, state, election_options, host)
        .await?
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: forwarding_failure.map_or_else(
                || "local node could not establish current-term Raft leadership".to_string(),
                |error| {
                    format!(
                        "local node could not establish current-term Raft leadership; forwarding failed: {error}"
                    )
                },
            ),
        });
    }
    commit_cluster_kv_authoritative_barrier_with_consensus_state(
        runtime, paths, state, options, deadline, host,
    )
    .await
}

pub async fn handle_cluster_kv_txn_request_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: KvTxnRequest,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<ClusterKvTxnCommitResult, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let mut state = read_cluster_state_without_kv_async(runtime, paths)
        .await
        .map_err(|error| match error {
            NetworkError::ClusterOrphan => NetworkError::ClusterReadOnly {
                detail: "local node has no cluster state".to_string(),
            },
            other => other,
        })?;
    let result = commit_cluster_kv_txn_with_quorum_response_async_with_host(
        runtime, paths, &mut state, request, options, host,
    )
    .await?;
    write_imported_cluster_kv_snapshot_if_changed_async(runtime, paths, &result.snapshot).await?;
    Ok(result)
}

/// Executes a daemon/service transaction without constructing an owned view of
/// the complete post-commit keyspace. Callers that only need the transaction
/// response and authority proof should use this path.
pub async fn handle_cluster_kv_txn_request_compact_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: KvTxnRequest,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<sinomo_kv::ClusterKvTxnCompactCommitResult, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let mut state = read_cluster_state_without_kv_async(runtime, paths)
        .await
        .map_err(|error| match error {
            NetworkError::ClusterOrphan => NetworkError::ClusterReadOnly {
                detail: "local node has no cluster state".to_string(),
            },
            other => other,
        })?;
    commit_cluster_kv_txn_compact_with_quorum_response_async_with_host(
        runtime, paths, &mut state, request, options, host,
    )
    .await
}

/// Handles an already-forwarded transaction on the verified local leader and
/// returns the compact wire result directly. This avoids constructing and then
/// discarding the complete KV snapshot in the daemon service path.
pub async fn handle_forwarded_cluster_kv_txn_rpc_request_compact_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: KvTxnRequest,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<ClusterKvTxnRpcResponse, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let compact = handle_cluster_kv_txn_request_compact_async_with_host(
        runtime, paths, request, options, host,
    )
    .await?;
    let txn_response = compact
        .txn_response
        .ok_or_else(|| NetworkError::Serialization {
            detail: "cluster KV compact commit omitted its transaction response".to_string(),
        })?;
    if txn_response.revision != compact.authority.revision {
        return Err(NetworkError::Serialization {
            detail: format!(
                "cluster KV compact transaction revision {} does not match authority revision {}",
                txn_response.revision.0, compact.authority.revision.0
            ),
        });
    }
    Ok(ClusterKvTxnRpcResponse {
        request_id: compact.request_id,
        submitted_command_digest: compact.submitted_command_digest,
        command_digest: compact.command_digest,
        txn_response: Some(txn_response),
        authority: compact.authority,
        replayed_committed_request: compact.replayed_committed_request,
    })
}

pub async fn handle_cluster_kv_authoritative_snapshot_request_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    body: &[u8],
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    sinomo_kv::decode_cluster_kv_authoritative_snapshot_rpc_request_body(body).map_err(kv_err)?;
    let mut state = read_cluster_state_without_kv_async(runtime, paths)
        .await
        .map_err(|error| match error {
            NetworkError::ClusterOrphan => NetworkError::ClusterReadOnly {
                detail: "local node has no cluster state".to_string(),
            },
            other => other,
        })?;
    let read = read_cluster_kv_authoritative_snapshot_async_with_host(
        runtime, paths, &mut state, options, host,
    )
    .await?;
    // A follower's local Raft materialization can intentionally trail the
    // leader even after it has accepted the committed log. Preserve the exact
    // quorum-confirmed view returned by an authoritative read so local readers
    // (including DFS replica authority validation) can converge without a
    // separate mutation or operator-triggered snapshot refresh.
    write_imported_cluster_kv_snapshot_if_changed_async(runtime, paths, &read.value).await?;
    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::{ClusterMemberRecord, ClusterMemberState};
    use sinomo_kv::{InProcessKvStore, KvAuthorityProof, KvRequestOutcome, KvRevision};
    use sinomo_node::ClusterId;
    use time::OffsetDateTime;

    fn assert_bounded_authority_busy_preparations(
        preparations_before: usize,
        preparations_after: usize,
        context: &str,
    ) {
        let preparations = preparations_after.saturating_sub(preparations_before);
        let max_preparations = 1_usize.saturating_add(
            (BOUNDED_AUTHORITY_BUSY_RETRY_BUDGET.as_millis()
                / LOCAL_READ_INDEX_BUSY_RETRY_INTERVAL.as_millis()) as usize,
        );
        assert!(
            (2..=max_preparations).contains(&preparations),
            "{context} must perform an initial busy probe and only bounded non-queued retries; before={preparations_before}, after={preparations_after}, max={max_preparations}"
        );
    }

    struct NoNetworkRequestOutcomeHost;

    impl ClusterObjectRpcAsyncHost for NoNetworkRequestOutcomeHost {}

    impl ClusterKvQuorumAsyncHost for NoNetworkRequestOutcomeHost {
        fn last_local_kv_replication_index_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _state: &'a ClusterBootstrapState,
        ) -> ClusterMembershipRpcFuture<'a, u64> {
            Box::pin(async {
                Err(NetworkError::ClusterReadOnly {
                    detail: "single-voter request-outcome test unexpectedly requested replication"
                        .to_string(),
                })
            })
        }

        fn build_kv_replication_request_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _state: &'a ClusterBootstrapState,
            _next_index: u64,
        ) -> ClusterMembershipRpcFuture<'a, sinomo_kv::ClusterRaftAppendRequest> {
            Box::pin(async {
                Err(NetworkError::ClusterReadOnly {
                    detail: "single-voter request-outcome test unexpectedly built replication"
                        .to_string(),
                })
            })
        }

        fn send_cluster_kv_replication_to_node_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            _request: &'a sinomo_kv::ClusterRaftAppendRequest,
            _timeout: Duration,
        ) -> ClusterMembershipRpcFuture<'a, sinomo_kv::ClusterRaftAppendResponse> {
            Box::pin(async {
                Err(NetworkError::ClusterReadOnly {
                    detail: "single-voter request-outcome test unexpectedly sent replication"
                        .to_string(),
                })
            })
        }
    }

    impl ClusterStateAuthorityReachabilityAsyncHost for NoNetworkRequestOutcomeHost {
        fn probe_cluster_state_authority_node_async<'a>(
            &'a mut self,
            _state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            _timeout: Duration,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<bool, NetworkError>> + Send + 'a>,
        > {
            Box::pin(async { Ok(false) })
        }
    }

    impl ClusterKvTxnQuorumAsyncHost for NoNetworkRequestOutcomeHost {
        fn request_kv_state_authority_vote_async<'a>(
            &'a mut self,
            _state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            _request: &'a ClusterKvStateAuthorityVoteRequest,
            _timeout: Duration,
        ) -> ClusterMembershipRpcFuture<'a, ClusterKvStateAuthorityVoteResponse> {
            Box::pin(async {
                Err(NetworkError::ClusterReadOnly {
                    detail: "single-voter request-outcome test unexpectedly requested a vote"
                        .to_string(),
                })
            })
        }
    }

    #[derive(Default)]
    struct ExactSetReadIndexHost {
        quorum_waves: usize,
        quorum_delay: Duration,
        object_rpc_stream_opens: usize,
        contended: Option<Arc<ContendedExactSetReadIndexHostState>>,
        release_busy_admission_on_forward: Option<sinomo_kv::ClusterKvRaftOperationGuard>,
        busy_admission_release_delay: Duration,
        release_busy_admission_on_snapshot_forward: Option<sinomo_kv::ClusterKvRaftOperationGuard>,
        forwarded_point_reads: usize,
        forwarded_point_read: Option<KvAuthoritativeRead<Option<sinomo_kv::KvEntry>>>,
        forwarded_point_read_returns_busy_count: usize,
        forwarded_prefix_reads: usize,
        forwarded_prefix_read: Option<KvAuthoritativeRead<Vec<sinomo_kv::KvEntry>>>,
        forwarded_prefix_read_returns_none_count: usize,
        forwarded_prefix_read_returns_busy_count: usize,
        forwarded_snapshot_reads: usize,
        forwarded_snapshot_read: Option<KvAuthoritativeRead<KvSnapshot>>,
        forwarded_snapshot_read_returns_busy_count: usize,
        forwarded_exact_reads: usize,
        forwarded_exact_states: Vec<ClusterBootstrapState>,
        forwarded_exact_read: Option<KvAuthoritativeRead<Vec<Option<sinomo_kv::KvEntry>>>>,
        forwarded_exact_read_returns_busy_count: usize,
        forwarded_exact_read_returns_none: bool,
        forwarded_exact_read_unknown_route: bool,
        vote_requests: usize,
    }

    struct ContendedExactSetReadIndexHostState {
        pre_dispatch_callbacks: AtomicUsize,
        pre_dispatch_entered: tokio::sync::Semaphore,
        pre_dispatch_release: tokio::sync::Semaphore,
        quorum_waves: AtomicUsize,
        wave_entered: tokio::sync::Semaphore,
        wave_release: tokio::sync::Semaphore,
    }

    impl ContendedExactSetReadIndexHostState {
        fn new() -> Self {
            Self {
                pre_dispatch_callbacks: AtomicUsize::new(0),
                pre_dispatch_entered: tokio::sync::Semaphore::new(0),
                pre_dispatch_release: tokio::sync::Semaphore::new(0),
                quorum_waves: AtomicUsize::new(0),
                wave_entered: tokio::sync::Semaphore::new(0),
                wave_release: tokio::sync::Semaphore::new(0),
            }
        }
    }

    impl ClusterObjectRpcAsyncHost for ExactSetReadIndexHost {
        fn open_object_rpc_stream_async<'a>(
            &'a mut self,
            _endpoint: &'a sinomo_base::ClusterEndpointRecord,
            _health: sinomo_network::NetworkHealthPolicy,
        ) -> sinomo_network::ClusterObjectRpcFuture<'a, Box<dyn sinomo_network::NetworkDuplexStream>>
        {
            self.object_rpc_stream_opens = self.object_rpc_stream_opens.saturating_add(1);
            Box::pin(async {
                Err(NetworkError::ClusterReadOnly {
                    detail: "current-term exact-set read unexpectedly opened an object RPC stream"
                        .to_string(),
                })
            })
        }
    }

    impl ClusterKvQuorumAsyncHost for ExactSetReadIndexHost {
        fn on_kv_replication_sent_async<'a>(&'a mut self) -> ClusterMembershipRpcFuture<'a, ()> {
            let contended = self.contended.clone();
            Box::pin(async move {
                if let Some(contended) = contended
                    && contended
                        .pre_dispatch_callbacks
                        .fetch_add(1, Ordering::SeqCst)
                        == 0
                {
                    contended.pre_dispatch_entered.add_permits(1);
                    contended
                        .pre_dispatch_release
                        .acquire()
                        .await
                        .expect("release contended ReadIndex dispatch")
                        .forget();
                }
                Ok(())
            })
        }

        fn last_local_kv_replication_index_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _state: &'a ClusterBootstrapState,
        ) -> ClusterMembershipRpcFuture<'a, u64> {
            Box::pin(async {
                Err(NetworkError::ClusterReadOnly {
                    detail: "exact-set ReadIndex unexpectedly requested a replication index"
                        .to_string(),
                })
            })
        }

        fn build_kv_replication_request_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _state: &'a ClusterBootstrapState,
            _next_index: u64,
        ) -> ClusterMembershipRpcFuture<'a, sinomo_kv::ClusterRaftAppendRequest> {
            Box::pin(async {
                Err(NetworkError::ClusterReadOnly {
                    detail: "exact-set ReadIndex unexpectedly built a replication request"
                        .to_string(),
                })
            })
        }

        fn send_cluster_kv_replication_to_node_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            _request: &'a sinomo_kv::ClusterRaftAppendRequest,
            _timeout: Duration,
        ) -> ClusterMembershipRpcFuture<'a, sinomo_kv::ClusterRaftAppendResponse> {
            Box::pin(async {
                Err(NetworkError::ClusterReadOnly {
                    detail: "exact-set ReadIndex must use one batched quorum wave".to_string(),
                })
            })
        }

        fn send_cluster_kv_replications_to_quorum_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            state: &'a ClusterBootstrapState,
            attempts: crate::raft_quorum::ClusterRaftQuorumBatch,
            required_acks: usize,
        ) -> ClusterMembershipRpcFuture<'a, crate::raft_quorum::ClusterRaftQuorumBatchOutcomes>
        {
            self.quorum_waves = self.quorum_waves.saturating_add(1);
            let contended = self.contended.clone();
            let quorum_delay = self.quorum_delay;
            Box::pin(async move {
                if !quorum_delay.is_zero() {
                    tokio::time::sleep(quorum_delay).await;
                }
                if let Some(contended) = contended {
                    contended.quorum_waves.fetch_add(1, Ordering::SeqCst);
                    contended.wave_entered.add_permits(1);
                    contended
                        .wave_release
                        .acquire()
                        .await
                        .expect("release contended exact-set ReadIndex wave")
                        .forget();
                }
                Ok(attempts
                    .into_iter()
                    .take(required_acks)
                    .map(|(target, request, _timeout)| {
                        assert!(
                            request.snapshot.is_none(),
                            "ReadIndex quorum probe must not transfer a snapshot"
                        );
                        assert!(
                            request.append.entries.is_empty(),
                            "ReadIndex quorum probe must not append a barrier"
                        );
                        assert!(
                            request.read_index_probe,
                            "ReadIndex quorum probe must carry the receiver fast-path marker"
                        );
                        let response = sinomo_kv::ClusterRaftAppendResponse {
                            accepted: true,
                            detail: "accepted exact-set ReadIndex heartbeat".to_string(),
                            state: state.clone(),
                            append: sinomo_kv::RaftAppendEntriesResponse {
                                term: request.append.term,
                                success: true,
                                match_index: request.append.prev_log_index,
                            },
                        };
                        (target, request, Ok(response))
                    })
                    .collect())
            })
        }
    }

    impl ClusterStateAuthorityReachabilityAsyncHost for ExactSetReadIndexHost {
        fn probe_cluster_state_authority_node_async<'a>(
            &'a mut self,
            _state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            _timeout: Duration,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<bool, NetworkError>> + Send + 'a>,
        > {
            Box::pin(async { Ok(false) })
        }
    }

    impl ClusterKvTxnQuorumAsyncHost for ExactSetReadIndexHost {
        fn forward_cluster_kv_authoritative_entry_async<'a>(
            &'a mut self,
            _runtime: &'a tokio::runtime::Handle,
            _paths: &'a ClusterPaths,
            state: &'a ClusterBootstrapState,
            _key: &'a str,
            _options: ClusterKvTxnForwardOptions,
        ) -> ClusterMembershipRpcFuture<'a, Option<KvAuthoritativeRead<Option<sinomo_kv::KvEntry>>>>
        {
            self.forwarded_point_reads = self.forwarded_point_reads.saturating_add(1);
            let call = self.forwarded_point_reads;
            let release = self.release_busy_admission_on_forward.take();
            let release_delay = self.busy_admission_release_delay;
            let response = self.forwarded_point_read.clone();
            let returns_busy = call <= self.forwarded_point_read_returns_busy_count;
            let remote_voter_count = sinomo_kv::cluster_kv_raft_voter_ids(state)
                .expect("point-forward test voter topology")
                .into_iter()
                .filter(|node_id| node_id != &state.local_node_id)
                .count();
            Box::pin(async move {
                assert_eq!(remote_voter_count, 2);
                if let Some(release) = release {
                    if release_delay.is_zero() {
                        drop(release);
                    } else {
                        std::thread::spawn(move || {
                            std::thread::sleep(release_delay);
                            drop(release);
                        });
                    }
                }
                if returns_busy {
                    return Err(NetworkError::ClusterReadOnly {
                        detail: "local Raft admission is busy and no authority accepted the forwarded point read; local election was skipped"
                            .to_string(),
                    });
                }
                if let Some(response) = response {
                    return Ok(Some(response));
                }
                Err(NetworkError::ClusterReadOnly {
                    detail: "both authenticated remote voters confirmed they are non-leaders"
                        .to_string(),
                })
            })
        }

        fn forward_cluster_kv_authoritative_entries_async<'a>(
            &'a mut self,
            _runtime: &'a tokio::runtime::Handle,
            _paths: &'a ClusterPaths,
            state: &'a ClusterBootstrapState,
            keys: &'a [String],
            _options: ClusterKvTxnForwardOptions,
        ) -> ClusterMembershipRpcFuture<
            'a,
            Option<KvAuthoritativeRead<Vec<Option<sinomo_kv::KvEntry>>>>,
        > {
            self.forwarded_exact_reads = self.forwarded_exact_reads.saturating_add(1);
            let call = self.forwarded_exact_reads;
            self.forwarded_exact_states.push(state.clone());
            let response = self.forwarded_exact_read.clone();
            let returns_busy = call <= self.forwarded_exact_read_returns_busy_count;
            let returns_none = self.forwarded_exact_read_returns_none;
            let unknown_route = self.forwarded_exact_read_unknown_route;
            Box::pin(async move {
                if unknown_route {
                    return Err(NetworkError::UnknownRoute {
                        route: sinomo_kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE.to_string(),
                    });
                }
                if returns_busy {
                    return Err(NetworkError::ClusterReadOnly {
                        detail: "local Raft admission is busy and no authority accepted the forwarded exact-key-set read; local election was skipped"
                            .to_string(),
                    });
                }
                if returns_none {
                    return Ok(None);
                }
                let response = response.ok_or_else(|| NetworkError::ClusterReadOnly {
                    detail: "current-term exact-set read unexpectedly forwarded".to_string(),
                })?;
                assert_eq!(response.value.len(), keys.len());
                Ok(Some(response))
            })
        }

        fn forward_cluster_kv_authoritative_range_prefix_async<'a>(
            &'a mut self,
            _runtime: &'a tokio::runtime::Handle,
            _paths: &'a ClusterPaths,
            state: &'a ClusterBootstrapState,
            _prefix: &'a str,
            _options: ClusterKvTxnForwardOptions,
        ) -> ClusterMembershipRpcFuture<'a, Option<KvAuthoritativeRead<Vec<sinomo_kv::KvEntry>>>>
        {
            self.forwarded_prefix_reads = self.forwarded_prefix_reads.saturating_add(1);
            let call = self.forwarded_prefix_reads;
            let release = self.release_busy_admission_on_forward.take();
            let release_delay = self.busy_admission_release_delay;
            let response = self.forwarded_prefix_read.clone();
            let returns_none = call <= self.forwarded_prefix_read_returns_none_count;
            let returns_busy = call <= self.forwarded_prefix_read_returns_busy_count;
            let remote_voter_count = sinomo_kv::cluster_kv_raft_voter_ids(state)
                .expect("prefix-forward test voter topology")
                .into_iter()
                .filter(|node_id| node_id != &state.local_node_id)
                .count();
            Box::pin(async move {
                assert_eq!(remote_voter_count, 2);
                if let Some(release) = release {
                    if release_delay.is_zero() {
                        drop(release);
                    } else {
                        std::thread::spawn(move || {
                            std::thread::sleep(release_delay);
                            drop(release);
                        });
                    }
                }
                if returns_none {
                    return Ok(None);
                }
                if returns_busy {
                    return Err(NetworkError::ClusterReadOnly {
                        detail: "local Raft admission is busy and no authority accepted the forwarded prefix read; local election was skipped"
                            .to_string(),
                    });
                }
                if let Some(response) = response {
                    return Ok(Some(response));
                }
                Err(NetworkError::ClusterReadOnly {
                    detail: "both authenticated remote voters confirmed they are non-leaders"
                        .to_string(),
                })
            })
        }

        fn forward_cluster_kv_authoritative_snapshot_async<'a>(
            &'a mut self,
            _runtime: &'a tokio::runtime::Handle,
            _paths: &'a ClusterPaths,
            _state: &'a ClusterBootstrapState,
            _options: ClusterKvTxnForwardOptions,
        ) -> ClusterMembershipRpcFuture<'a, Option<KvAuthoritativeRead<KvSnapshot>>> {
            self.forwarded_snapshot_reads = self.forwarded_snapshot_reads.saturating_add(1);
            if self.forwarded_snapshot_read_returns_busy_count > 0 {
                self.forwarded_snapshot_read_returns_busy_count -= 1;
                return Box::pin(async {
                    Err(NetworkError::ClusterReadOnly {
                        detail: "local Raft admission is busy and no authority accepted the forwarded snapshot read; local election was skipped".into(),
                    })
                });
            }
            if let Some(read) = self.forwarded_snapshot_read.clone() {
                return Box::pin(async move { Ok(Some(read)) });
            }
            let release = self.release_busy_admission_on_snapshot_forward.take();
            let release_delay = self.busy_admission_release_delay;
            Box::pin(async move {
                let Some(release) = release else {
                    return Err(NetworkError::ClusterReadOnly {
                        detail: "current-term snapshot read unexpectedly forwarded".to_string(),
                    });
                };
                if release_delay.is_zero() {
                    drop(release);
                } else {
                    std::thread::spawn(move || {
                        std::thread::sleep(release_delay);
                        drop(release);
                    });
                }
                Err(NetworkError::ClusterReadOnly {
                    detail: "mixed-version snapshot fallback sentinel".to_string(),
                })
            })
        }

        fn request_kv_state_authority_vote_async<'a>(
            &'a mut self,
            _state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            _request: &'a ClusterKvStateAuthorityVoteRequest,
            _timeout: Duration,
        ) -> ClusterMembershipRpcFuture<'a, ClusterKvStateAuthorityVoteResponse> {
            self.vote_requests = self.vote_requests.saturating_add(1);
            Box::pin(async {
                Err(NetworkError::ClusterReadOnly {
                    detail: "current-term exact-set read unexpectedly requested a vote".to_string(),
                })
            })
        }

        fn request_kv_state_authority_votes_async<'a>(
            &'a mut self,
            _state: &'a ClusterBootstrapState,
            targets: Vec<NodeId>,
            _request: &'a ClusterKvStateAuthorityVoteRequest,
            _timeout: Duration,
            _required_grants: usize,
        ) -> ClusterMembershipRpcFuture<
            'a,
            Vec<(
                NodeId,
                Result<ClusterKvStateAuthorityVoteResponse, NetworkError>,
            )>,
        > {
            self.vote_requests = self.vote_requests.saturating_add(targets.len());
            Box::pin(async move {
                Ok(targets
                    .into_iter()
                    .map(|target| {
                        (
                            target,
                            Err(NetworkError::ClusterReadOnly {
                                detail: "current-term exact-set read unexpectedly requested a vote"
                                    .to_string(),
                            }),
                        )
                    })
                    .collect())
            })
        }
    }

    struct ColdBarrierFixture {
        _root: sinomo_testdir::TestDir,
        _resident_registry_pin: sinomo_kv::ResidentRaftNodeRegistryPin,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        local: NodeId,
        initial_last_index: u64,
    }

    struct CrossedFloorTopologyFixture {
        cold: ColdBarrierFixture,
        caller_state: ClusterBootstrapState,
        removed_caller_voter: NodeId,
        promoted_resident_voter: NodeId,
        caller_endpoint_id: sinomo_base::Id,
        resident_revision: u64,
        resident_last_index: u64,
    }

    fn cold_barrier_fixture(name: &str) -> ColdBarrierFixture {
        let root = sinomo_testdir::TestDir::new(name);
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named(format!("cluster_{name}"))
            .expect("cold barrier cluster id");
        let local =
            NodeId::from_legacy_named(format!("node_{name}_a")).expect("cold barrier local node");
        let peer_b =
            NodeId::from_legacy_named(format!("node_{name}_b")).expect("cold barrier peer b");
        let peer_c =
            NodeId::from_legacy_named(format!("node_{name}_c")).expect("cold barrier peer c");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            name,
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state.term = 7;
        let capabilities = sinomo_node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities.clone();
        for (position, node_id) in [peer_b.clone(), peer_c.clone()].into_iter().enumerate() {
            let mut member = ClusterMemberRecord::local_voter(
                cluster_id.clone(),
                node_id.clone(),
                format!("peer-{}", position + 1),
                format!("pk-peer-{}", position + 1),
                now,
            );
            member.capabilities = capabilities.clone();
            member.allowed_capabilities = capabilities.clone();
            member.endpoints = vec![sinomo_network::daemon_lan_endpoint(
                &node_id,
                format!("10.99.8.{}:21353", position + 2),
                0,
            )];
            state.members.push(member);
        }
        crate::write_cluster_state(&paths, &state).expect("write cold barrier cluster state");

        let config = sinomo_kv::ClusterKvRaftConfigRecord {
            cluster_id,
            term: 6,
            kv_revision: 1,
            voters: vec![local.clone(), peer_b, peer_c],
            learners: Vec::new(),
            non_voting: Vec::new(),
            updated_at: now,
        };
        // Parallel fixtures churn the bounded resident registry. Pin only its
        // coordination slot so a resident-only ownership probe cannot become
        // a cold-cache probe; mutable checkout remains fully exercised.
        let resident_registry_pin = sinomo_kv::pin_resident_raft_node_registry_for_test(
            &paths.kv_paths().raft_wal_paths(&local),
        )
        .expect("pin cold barrier fixture resident slot");
        let mut node = paths
            .kv_paths()
            .raft_store()
            .open_node(&local)
            .expect("open cold barrier Raft node");
        node.become_leader(6).expect("become previous-term leader");
        for member in &state.members {
            node.append_local_command(
                6,
                KvCommand::Op(KvOp::Put {
                    key: crate::keyspace::cluster_member_key(&member.node_id),
                    value: sinomo_kv::encode_kv_value(member).expect("encode cold barrier member"),
                    lease_id: None,
                }),
            )
            .expect("append cold barrier member");
        }
        node.append_local_command(
            6,
            KvCommand::Op(KvOp::Put {
                key: sinomo_kv::cluster_kv_raft_config_key(),
                value: sinomo_kv::encode_kv_value(&config).expect("encode cold barrier config"),
                lease_id: None,
            }),
        )
        .expect("append cold barrier config");
        node.append_local_command(
            6,
            KvCommand::Op(KvOp::Put {
                key: "/cold/a".to_string(),
                value: b"alpha".to_vec(),
                lease_id: None,
            }),
        )
        .expect("append cold value a");
        let final_entry = node
            .append_local_command(
                6,
                KvCommand::Op(KvOp::Put {
                    key: "/cold/b".to_string(),
                    value: b"beta".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append cold value b");
        node.commit_to(final_entry.index)
            .expect("commit previous-term fixture");
        node.become_leader(7)
            .expect("become cold current-term leader");
        state.kv_revision = node.store().revision().0;
        let initial_last_index = node.last_log_index();
        assert_ne!(
            node.log_entry_at(node.hard_state().commit_index)
                .map(|entry| entry.term),
            Some(node.hard_state().current_term),
            "fixture must begin without a current-term commit"
        );
        drop(node);

        ColdBarrierFixture {
            _root: root,
            _resident_registry_pin: resident_registry_pin,
            paths,
            state,
            local,
            initial_last_index,
        }
    }

    fn commit_current_term_barrier(fixture: &ColdBarrierFixture) -> u64 {
        let mut storage =
            LocalClusterRaftCommandStorage::new(&fixture.paths, fixture.local.clone());
        let barrier = storage
            .append_uncommitted(KvCommandRequest::generated(KvCommand::Barrier))
            .expect("append current-term authority anchor");
        storage
            .commit_index_compact(barrier.index)
            .expect("commit current-term authority anchor");
        let last_index = storage
            .last_log_index()
            .expect("current-term authority anchor last index");
        drop(storage);
        last_index
    }

    fn crossed_floor_changed_voter_fixture(name: &str) -> CrossedFloorTopologyFixture {
        let cold = cold_barrier_fixture(name);
        let now = OffsetDateTime::now_utc();
        let mut caller_state = cold.state.clone();
        let cluster_id = caller_state.cluster_id.clone();
        let removed_caller_voter = caller_state.members[2].node_id.clone();
        let capabilities = caller_state.members[0].capabilities.clone();
        let allowed_capabilities = caller_state.members[0].allowed_capabilities.clone();
        let mut added_members = Vec::new();
        for (suffix, octet) in [("d", 4_u8), ("e", 5_u8), ("f", 6_u8)] {
            let node_id = NodeId::from_legacy_named(format!("node_{name}_{suffix}"))
                .expect("crossed-floor added node");
            let mut member = ClusterMemberRecord::local_non_voting(
                cluster_id.clone(),
                node_id.clone(),
                format!("peer-{suffix}"),
                format!("pk-peer-{suffix}"),
                now,
            );
            member.capabilities = capabilities.clone();
            member.allowed_capabilities = allowed_capabilities.clone();
            member.endpoints = vec![sinomo_network::daemon_lan_endpoint(
                &node_id,
                format!("10.99.9.{octet}:21353"),
                0,
            )];
            added_members.push(member);
        }
        caller_state.members.extend(added_members.iter().cloned());

        let mut node = cold
            .paths
            .kv_paths()
            .raft_store()
            .open_node(&cold.local)
            .expect("open crossed-floor Raft node");
        let mut last_member_index = 0;
        for member in &added_members {
            last_member_index = node
                .append_local_command(
                    7,
                    KvCommand::Op(KvOp::Put {
                        key: crate::keyspace::cluster_member_key(&member.node_id),
                        value: sinomo_kv::encode_kv_value(member)
                            .expect("encode crossed-floor member"),
                        lease_id: None,
                    }),
                )
                .expect("append crossed-floor member")
                .index;
        }
        node.commit_to(last_member_index)
            .expect("commit crossed-floor members");

        let mut config = sinomo_kv::ClusterKvRaftConfigRecord {
            cluster_id,
            term: 7,
            kv_revision: 0,
            voters: caller_state.members[..3]
                .iter()
                .map(|member| member.node_id.clone())
                .collect(),
            learners: Vec::new(),
            non_voting: added_members
                .iter()
                .map(|member| member.node_id.clone())
                .collect(),
            updated_at: now,
        };
        let mut commit_config = |config: &mut sinomo_kv::ClusterKvRaftConfigRecord| {
            config.kv_revision = node.store().revision().0.saturating_add(1);
            let entry = node
                .append_local_command(
                    7,
                    KvCommand::Op(KvOp::Put {
                        key: sinomo_kv::cluster_kv_raft_config_key(),
                        value: sinomo_kv::encode_kv_value(config)
                            .expect("encode crossed-floor config"),
                        lease_id: None,
                    }),
                )
                .expect("append crossed-floor config");
            node.commit_to(entry.index)
                .expect("commit crossed-floor config");
            node.store().revision().0
        };
        let caller_revision = commit_config(&mut config);
        caller_state.kv_revision = caller_revision;
        caller_state.term = 8;

        let promoted_resident_voter = added_members[0].node_id.clone();
        let caller_endpoint = sinomo_network::daemon_lan_endpoint(
            &promoted_resident_voter,
            "10.199.9.44:21353".to_string(),
            999,
        );
        let caller_endpoint_id = caller_endpoint.endpoint_id.clone();
        let promoted_caller_member = caller_state
            .members
            .iter_mut()
            .find(|member| member.node_id == promoted_resident_voter)
            .expect("promoted caller member");
        sinomo_network::upsert_member_endpoint(promoted_caller_member, caller_endpoint);

        for promoted in &added_members {
            config.voters.push(promoted.node_id.clone());
            config
                .non_voting
                .retain(|node_id| node_id != &promoted.node_id);
            commit_config(&mut config);
        }
        config
            .voters
            .retain(|node_id| node_id != &removed_caller_voter);
        let resident_revision = commit_config(&mut config);
        let resident_last_index = node.last_log_index();
        drop(node);

        CrossedFloorTopologyFixture {
            cold,
            caller_state,
            removed_caller_voter,
            promoted_resident_voter,
            caller_endpoint_id,
            resident_revision,
            resident_last_index,
        }
    }

    struct ColdBarrierHostState {
        barrier_waves: AtomicUsize,
        read_index_waves: AtomicUsize,
        block_first_barrier: bool,
        block_first_post_commit: bool,
        post_commit_blocked: AtomicBool,
        fail_first_barrier_unknown: AtomicBool,
        first_barrier_entered: tokio::sync::Semaphore,
        first_barrier_release: tokio::sync::Semaphore,
        first_post_commit_entered: tokio::sync::Semaphore,
        first_post_commit_release: tokio::sync::Semaphore,
    }

    impl ColdBarrierHostState {
        fn new(
            block_first_barrier: bool,
            fail_first_barrier_unknown: bool,
            block_first_post_commit: bool,
        ) -> Self {
            Self {
                barrier_waves: AtomicUsize::new(0),
                read_index_waves: AtomicUsize::new(0),
                block_first_barrier,
                block_first_post_commit,
                post_commit_blocked: AtomicBool::new(false),
                fail_first_barrier_unknown: AtomicBool::new(fail_first_barrier_unknown),
                first_barrier_entered: tokio::sync::Semaphore::new(0),
                first_barrier_release: tokio::sync::Semaphore::new(0),
                first_post_commit_entered: tokio::sync::Semaphore::new(0),
                first_post_commit_release: tokio::sync::Semaphore::new(0),
            }
        }
    }

    struct ColdBarrierHost {
        shared: Arc<ColdBarrierHostState>,
    }

    impl ClusterObjectRpcAsyncHost for ColdBarrierHost {}

    impl ClusterKvQuorumAsyncHost for ColdBarrierHost {
        fn last_local_kv_replication_index_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _state: &'a ClusterBootstrapState,
        ) -> ClusterMembershipRpcFuture<'a, u64> {
            Box::pin(async {
                Err(NetworkError::ClusterReadOnly {
                    detail: "cold barrier test unexpectedly used legacy replication index"
                        .to_string(),
                })
            })
        }

        fn build_kv_replication_request_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _state: &'a ClusterBootstrapState,
            _next_index: u64,
        ) -> ClusterMembershipRpcFuture<'a, sinomo_kv::ClusterRaftAppendRequest> {
            Box::pin(async {
                Err(NetworkError::ClusterReadOnly {
                    detail: "cold barrier test unexpectedly used legacy request construction"
                        .to_string(),
                })
            })
        }

        fn send_cluster_kv_replication_to_node_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            _request: &'a sinomo_kv::ClusterRaftAppendRequest,
            _timeout: Duration,
        ) -> ClusterMembershipRpcFuture<'a, sinomo_kv::ClusterRaftAppendResponse> {
            Box::pin(async {
                Err(NetworkError::ClusterReadOnly {
                    detail: "cold barrier test requires one batched quorum wave".to_string(),
                })
            })
        }

        fn send_cluster_kv_replications_to_quorum_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            state: &'a ClusterBootstrapState,
            attempts: crate::raft_quorum::ClusterRaftQuorumBatch,
            required_acks: usize,
        ) -> ClusterMembershipRpcFuture<'a, crate::raft_quorum::ClusterRaftQuorumBatchOutcomes>
        {
            Box::pin(async move {
                let barrier_wave = attempts.iter().any(|(_, request, _)| {
                    request.append.prev_log_index > request.append.leader_commit
                        || request
                            .append
                            .entries
                            .iter()
                            .any(|entry| matches!(&entry.command, KvCommand::Barrier))
                });
                if barrier_wave {
                    let wave = self.shared.barrier_waves.fetch_add(1, Ordering::SeqCst);
                    if wave == 0 && self.shared.block_first_barrier {
                        self.shared.first_barrier_entered.add_permits(1);
                        self.shared
                            .first_barrier_release
                            .acquire()
                            .await
                            .expect("release first cold barrier wave")
                            .forget();
                    }
                    if self
                        .shared
                        .fail_first_barrier_unknown
                        .swap(false, Ordering::SeqCst)
                    {
                        return Err(NetworkError::OutcomeUnknown {
                            operation: "test first-current-term barrier".to_string(),
                            operation_id: None,
                            detail: "test dropped the barrier acknowledgement".to_string(),
                        });
                    }
                } else {
                    self.shared.read_index_waves.fetch_add(1, Ordering::SeqCst);
                }
                Ok(attempts
                    .into_iter()
                    .take(required_acks)
                    .map(|(target, request, _timeout)| {
                        let response = sinomo_kv::ClusterRaftAppendResponse {
                            accepted: true,
                            detail: "accepted cold barrier test append".to_string(),
                            state: state.clone(),
                            append: sinomo_kv::RaftAppendEntriesResponse {
                                term: request.append.term,
                                success: true,
                                match_index: request.append.prev_log_index,
                            },
                        };
                        (target, request, Ok(response))
                    })
                    .collect())
            })
        }

        fn fan_out_committed_cluster_kv_command_async<'a>(
            &'a mut self,
            _paths: ClusterPaths,
            _state: ClusterBootstrapState,
            _events: std::sync::Arc<Vec<sinomo_kv::KvEvent>>,
            _targets: Vec<NodeId>,
        ) -> ClusterMembershipRpcFuture<'a, ()> {
            Box::pin(async move {
                if self.shared.block_first_post_commit
                    && !self.shared.post_commit_blocked.swap(true, Ordering::SeqCst)
                {
                    self.shared.first_post_commit_entered.add_permits(1);
                    self.shared
                        .first_post_commit_release
                        .acquire()
                        .await
                        .expect("release first cold barrier post-commit publication")
                        .forget();
                }
                Ok(())
            })
        }
    }

    impl ClusterStateAuthorityReachabilityAsyncHost for ColdBarrierHost {
        fn probe_cluster_state_authority_node_async<'a>(
            &'a mut self,
            _state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            _timeout: Duration,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<bool, NetworkError>> + Send + 'a>,
        > {
            Box::pin(async { Ok(false) })
        }
    }

    impl ClusterKvTxnQuorumAsyncHost for ColdBarrierHost {
        fn request_kv_state_authority_vote_async<'a>(
            &'a mut self,
            _state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            _request: &'a ClusterKvStateAuthorityVoteRequest,
            _timeout: Duration,
        ) -> ClusterMembershipRpcFuture<'a, ClusterKvStateAuthorityVoteResponse> {
            Box::pin(async {
                Err(NetworkError::ClusterReadOnly {
                    detail: "cold current-term leader unexpectedly requested a vote".to_string(),
                })
            })
        }
    }

    struct SyncAuthoritativeHostState {
        barrier_commits: AtomicUsize,
        read_index_batches: AtomicUsize,
        submitted_barrier_ids: Mutex<Vec<sinomo_kv::KvRequestId>>,
        block_first_barrier: bool,
        barrier_entered: (Mutex<bool>, Condvar),
        barrier_release: (Mutex<bool>, Condvar),
        block_first_probe: bool,
        advance_term_before_commit: AtomicBool,
    }

    impl SyncAuthoritativeHostState {
        fn new(block_first_barrier: bool, block_first_probe: bool) -> Self {
            Self {
                barrier_commits: AtomicUsize::new(0),
                read_index_batches: AtomicUsize::new(0),
                submitted_barrier_ids: Mutex::new(Vec::new()),
                block_first_barrier,
                barrier_entered: (Mutex::new(false), Condvar::new()),
                barrier_release: (Mutex::new(false), Condvar::new()),
                block_first_probe,
                advance_term_before_commit: AtomicBool::new(false),
            }
        }

        fn wait_for_barrier_entry(&self) {
            let (lock, changed) = &self.barrier_entered;
            let entered = lock.lock().expect("barrier entry lock");
            let (entered, wait) = changed
                .wait_timeout_while(entered, Duration::from_secs(2), |entered| !*entered)
                .expect("barrier entry wait");
            assert!(*entered && !wait.timed_out(), "barrier owner did not enter");
        }

        fn release_barrier(&self) {
            let (lock, changed) = &self.barrier_release;
            *lock.lock().expect("barrier release lock") = true;
            changed.notify_all();
        }
    }

    struct SyncAuthoritativeHost {
        shared: Arc<SyncAuthoritativeHostState>,
    }

    impl ClusterKvTxnQuorumHost for SyncAuthoritativeHost {
        fn known_kv_state_authority_id(
            &mut self,
            _paths: &ClusterPaths,
            state: &ClusterBootstrapState,
        ) -> Result<Option<NodeId>, NetworkError> {
            Ok(Some(state.local_node_id.clone()))
        }

        fn call_node_object_rpc(
            &mut self,
            _state: &ClusterBootstrapState,
            _target_node_id: &NodeId,
            _request: &ObjectRpcRequest,
            _timeout: Duration,
        ) -> Result<ObjectRpcResponse, NetworkError> {
            Err(NetworkError::ClusterReadOnly {
                detail: "sync authoritative test requires batched ReadIndex transport".to_string(),
            })
        }

        fn send_cluster_kv_read_index_probes(
            &mut self,
            state: &ClusterBootstrapState,
            targets: Vec<NodeId>,
            request: &sinomo_kv::ClusterRaftAppendRequest,
            timeout: Duration,
            required_acks: usize,
        ) -> Result<
            Vec<(
                NodeId,
                Result<sinomo_kv::ClusterRaftAppendResponse, NetworkError>,
            )>,
            NetworkError,
        > {
            self.shared
                .read_index_batches
                .fetch_add(1, Ordering::SeqCst);
            assert!(
                request.snapshot.is_none(),
                "ReadIndex must not send snapshot"
            );
            assert!(
                request.append.entries.is_empty(),
                "ReadIndex must not append entries"
            );
            if required_acks == 0 {
                return Ok(Vec::new());
            }
            let response = |target: NodeId| {
                (
                    target,
                    Ok(sinomo_kv::ClusterRaftAppendResponse {
                        accepted: true,
                        detail: "accepted synchronous ReadIndex heartbeat".to_string(),
                        state: state.clone(),
                        append: sinomo_kv::RaftAppendEntriesResponse {
                            term: request.append.term,
                            success: true,
                            match_index: request.append.prev_log_index,
                        },
                    }),
                )
            };
            if !self.shared.block_first_probe || targets.len() < 2 {
                return Ok(targets
                    .into_iter()
                    .take(required_acks)
                    .map(response)
                    .collect());
            }

            // Model the production batch contract: the first target can be
            // stalled while a later target establishes quorum, after which the
            // stalled operation is cancelled and joined before return.
            let cancellation = Arc::new((Mutex::new(false), Condvar::new()));
            let (sender, receiver) = std::sync::mpsc::channel();
            let mut outcomes = Vec::new();
            std::thread::scope(|scope| {
                for (position, target) in targets.into_iter().enumerate() {
                    let cancellation = Arc::clone(&cancellation);
                    let sender = sender.clone();
                    let outcome = response(target);
                    scope.spawn(move || {
                        if position == 0 {
                            let (lock, changed) = &*cancellation;
                            let cancelled = lock.lock().expect("probe cancellation lock");
                            let _cancelled = changed
                                .wait_while(cancelled, |cancelled| !*cancelled)
                                .expect("probe cancellation wait");
                            return;
                        }
                        let _ = sender.send(outcome);
                    });
                }
                drop(sender);
                while outcomes.len() < required_acks {
                    let outcome = receiver
                        .recv_timeout(timeout)
                        .expect("healthy concurrent ReadIndex probe");
                    outcomes.push(outcome);
                }
                let (lock, changed) = &*cancellation;
                *lock.lock().expect("probe cancellation lock") = true;
                changed.notify_all();
            });
            Ok(outcomes)
        }

        fn local_is_kv_state_authority(
            &mut self,
            paths: &ClusterPaths,
            state: &ClusterBootstrapState,
        ) -> Result<bool, NetworkError> {
            locked_cluster_raft_local_is_leader(paths, state)
        }

        fn request_kv_state_authority_vote(
            &mut self,
            _state: &ClusterBootstrapState,
            _target_node_id: &NodeId,
            _request: &ClusterKvStateAuthorityVoteRequest,
            _timeout: Duration,
        ) -> Result<ClusterKvStateAuthorityVoteResponse, NetworkError> {
            Err(NetworkError::ClusterReadOnly {
                detail: "sync authoritative leader test unexpectedly requested a vote".to_string(),
            })
        }

        fn reachable_cluster_state_authority_count(
            &mut self,
            _paths: &ClusterPaths,
            state: &ClusterBootstrapState,
            _options: ClusterStateAuthorityReachabilityOptions,
        ) -> usize {
            sinomo_kv::cluster_kv_raft_voter_ids(state)
                .map(|voters| voters.len())
                .unwrap_or_default()
        }

        fn commit_cluster_kv_command_with_quorum(
            &mut self,
            paths: &ClusterPaths,
            state: &mut ClusterBootstrapState,
            request: KvCommandRequest,
            options: ClusterKvRaftQuorumCommitOptions,
        ) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError> {
            let commit_number = self.shared.barrier_commits.fetch_add(1, Ordering::SeqCst);
            self.shared
                .submitted_barrier_ids
                .lock()
                .expect("submitted barrier ids")
                .push(request.request_id.clone());
            if commit_number == 0 && self.shared.block_first_barrier {
                let (entered_lock, entered_changed) = &self.shared.barrier_entered;
                *entered_lock.lock().expect("barrier entry lock") = true;
                entered_changed.notify_all();
                let (release_lock, release_changed) = &self.shared.barrier_release;
                let release = release_lock.lock().expect("barrier release lock");
                let _release = release_changed
                    .wait_while(release, |release| !*release)
                    .expect("barrier release wait");
            }
            if self
                .shared
                .advance_term_before_commit
                .swap(false, Ordering::SeqCst)
            {
                LocalClusterRaftCommandStorage::new(paths, state.local_node_id.clone())
                    .observe_term(state.term.saturating_add(1))?;
            }
            let response_state = state.clone();
            crate::raft_quorum::commit_cluster_raft_command_with_local_storage_and_transport(
                paths,
                state,
                request,
                options,
                crate::control_plane_options::ClusterControlPlaneTimeouts::default_daemon_transport(
                ),
                move |_endpoint, append, _timeout| {
                    Ok(sinomo_kv::ClusterRaftAppendResponse {
                        accepted: true,
                        detail: "accepted synchronous barrier append".to_string(),
                        state: response_state.clone(),
                        append: sinomo_kv::RaftAppendEntriesResponse {
                            term: append.append.term,
                            success: true,
                            match_index: append
                                .append
                                .entries
                                .last()
                                .map_or(append.append.prev_log_index, |entry| entry.index),
                        },
                    })
                },
                |_paths, _state, _target, _connectivity, _latency| {},
                |_paths, _state, _events, _targets| {},
                || {},
            )
        }
    }

    fn sync_authoritative_options(timeout: Duration) -> ClusterKvTxnQuorumOptions {
        let timeouts =
            crate::control_plane_options::ClusterControlPlaneTimeouts::default_daemon_transport();
        let mut options = timeouts.kv_txn_quorum_options(false);
        options.raft_append_timeout = timeout;
        options.raft_append_replication_options.timeout = timeout;
        options
    }

    fn initialize_single_voter_read_index_leader(
        paths: &ClusterPaths,
        cluster_name: &str,
        node_name: &str,
    ) -> ClusterBootstrapState {
        paths.ensure().expect("cluster paths");
        let cluster_id = ClusterId::from_legacy_named(cluster_name).expect("cluster id");
        let local = NodeId::from_legacy_named(node_name).expect("local node id");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            cluster_name,
            local.clone(),
            "local",
            "pk-local",
            OffsetDateTime::UNIX_EPOCH,
        );
        let capabilities = sinomo_node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        crate::write_cluster_state(paths, &state).expect("write cluster state");
        crate::sync_cluster_state_to_local_raft_unchecked(paths, &state)
            .expect("seed cluster raft config");
        paths
            .kv_paths()
            .raft_store()
            .set_leader_compact(&local, 1)
            .expect("establish local leader");
        state
    }

    fn commit_single_voter_test_value(
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        key: &str,
        value: &[u8],
    ) -> KvSnapshot {
        let mut storage = LocalClusterRaftCommandStorage::new(paths, state.local_node_id.clone());
        let entry = storage
            .append_uncommitted(KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
                key: key.to_string(),
                value: value.to_vec(),
                lease_id: None,
            })))
            .expect("append test value");
        storage
            .commit_index_compact(entry.index)
            .expect("commit test value");
        drop(storage);
        sinomo_kv::read_cluster_kv_current_snapshot_for_node(
            &paths.kv_paths(),
            state.local_node_id.clone(),
        )
        .expect("read local Raft snapshot")
    }

    #[test]
    fn committed_authority_projection_does_not_outlive_authoritative_read_deadline() {
        let root =
            sinomo_testdir::TestDir::new("sinomo-cluster-config-projection-deadline-admission");
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        let state = initialize_single_voter_read_index_leader(
            &paths,
            "cluster_config_projection_deadline",
            "node_config_projection_deadline",
        );
        let mut held_storage =
            LocalClusterRaftCommandStorage::new(&paths, state.local_node_id.clone());
        held_storage
            .read_index_state()
            .expect("hold the resident Raft node and operation lock");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let started = Instant::now();
        let result = runtime.block_on(project_committed_cluster_raft_authority_before_deadline(
            runtime.handle(),
            &paths,
            &state,
            started + Duration::from_millis(50),
            "test committed config projection",
        ));
        assert!(
            result.is_err(),
            "held operation lock must reject projection"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "projection must honor the caller's admission deadline"
        );

        // Keep the resident owner held beyond the deadline so a canceled
        // blocking task cannot remain queued and execute after its async
        // caller has left.
        std::thread::sleep(Duration::from_millis(100));
        drop(held_storage);
        let follow_up_deadline = Instant::now() + Duration::from_millis(500);
        assert!(
            runtime
                .block_on(locked_cluster_raft_local_is_leader_before_deadline(
                    runtime.handle(),
                    &paths,
                    &state,
                    follow_up_deadline,
                ))
                .expect("fresh admission after expired projection"),
            "expired projection must not retain an operation-lock ticket"
        );
    }

    #[test]
    fn committed_authority_projection_uses_resident_read_view_without_cold_restore() {
        let root =
            sinomo_testdir::TestDir::new("sinomo-cluster-authority-projection-resident-only");
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        let state = initialize_single_voter_read_index_leader(
            &paths,
            "cluster_authority_projection_resident_only",
            "node_authority_projection_resident_only",
        );
        let snapshot = commit_single_voter_test_value(
            &paths,
            &state,
            "/config/authority-projection-resident-only",
            b"resident",
        );
        let mut stale_state = state.clone();
        stale_state.term = 0;
        stale_state.kv_revision = 0;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let projected = runtime
            .block_on(project_committed_cluster_raft_authority_before_deadline(
                runtime.handle(),
                &paths,
                &stale_state,
                Instant::now() + Duration::from_millis(500),
                "test resident authority projection",
            ))
            .expect("project authority from the hot resident view");
        assert_eq!(projected.term, 1);
        assert_eq!(projected.kv_revision, snapshot.revision.0);

        let wal_paths = paths.kv_paths().raft_wal_paths(&state.local_node_id);
        assert!(
            sinomo_kv::evict_resident_raft_node_for_test(&wal_paths),
            "fixture must have a resident Raft node before eviction"
        );
        let cold = runtime.block_on(project_committed_cluster_raft_authority_before_deadline(
            runtime.handle(),
            &paths,
            &stale_state,
            Instant::now() + Duration::from_millis(100),
            "test resident-only authority projection",
        ));
        assert!(
            cold.is_err(),
            "authority projection must not reconstruct a cold Raft node or materialize a snapshot"
        );
    }

    #[test]
    fn authoritative_snapshot_rpc_skips_equal_generation_materialized_conflict() {
        let root = sinomo_testdir::TestDir::new(
            "sinomo-cluster-authoritative-snapshot-equal-generation-conflict",
        );
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        let state = initialize_single_voter_read_index_leader(
            &paths,
            "cluster_authoritative_snapshot_equal_generation",
            "node_authoritative_snapshot_equal_generation",
        );
        let authoritative = commit_single_voter_test_value(
            &paths,
            &state,
            "/config/authoritative-snapshot-test",
            b"raft-authority",
        );
        let mut materialized_conflict = authoritative.clone();
        materialized_conflict
            .entries
            .iter_mut()
            .find(|entry| entry.key == "/config/authoritative-snapshot-test")
            .expect("test entry")
            .value = b"conflicting-materialized-view".to_vec();
        sinomo_kv::write_cluster_kv_snapshot(&paths.kv_paths(), &materialized_conflict)
            .expect("install equal-generation materialized conflict");

        let payload = sinomo_kv::cluster_kv_authoritative_snapshot_rpc_payload()
            .expect("authoritative snapshot payload");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = NoNetworkRequestOutcomeHost;
        let read = runtime
            .block_on(
                handle_cluster_kv_authoritative_snapshot_request_async_with_host(
                    runtime.handle(),
                    &paths,
                    &payload.body,
                    crate::ClusterControlPlaneTimeouts::default_daemon_transport()
                        .kv_txn_quorum_options(true),
                    &mut host,
                ),
            )
            .expect("quorum-proven authoritative response");

        assert_eq!(read.value, authoritative);
        let cached = sinomo_kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
            .expect("read unchanged materialized conflict");
        assert_eq!(cached, materialized_conflict);
    }

    #[test]
    fn authoritative_snapshot_imports_when_it_advances_local_raft() {
        let root = sinomo_testdir::TestDir::new(
            "sinomo-cluster-authoritative-snapshot-advances-local-raft",
        );
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        let state = initialize_single_voter_read_index_leader(
            &paths,
            "cluster_authoritative_snapshot_advances",
            "node_authoritative_snapshot_advances",
        );
        let local = commit_single_voter_test_value(
            &paths,
            &state,
            "/config/authoritative-snapshot-local",
            b"local",
        );
        let mut imported_store = InProcessKvStore::restore(local).expect("restore local snapshot");
        imported_store
            .put(
                "/config/authoritative-snapshot-newer",
                b"newer".to_vec(),
                None,
            )
            .expect("advance imported snapshot");
        let imported = imported_store
            .try_snapshot()
            .expect("newer imported snapshot");

        assert!(
            write_imported_cluster_kv_snapshot_if_it_advances(&paths, &imported)
                .expect("import newer authoritative snapshot")
        );
        assert_eq!(
            sinomo_kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
                .expect("read imported materialized snapshot"),
            imported
        );
    }

    #[test]
    fn leader_request_outcome_read_index_does_not_append_barrier_or_snapshot_route() {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-request-outcome-read-index");
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::UNIX_EPOCH;
        let cluster_id =
            ClusterId::from_legacy_named("cluster_request_outcome_read_index").expect("cluster");
        let local =
            NodeId::from_legacy_named("node_request_outcome_read_index").expect("local node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "request outcome read index",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let capabilities = sinomo_node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        crate::write_cluster_state(&paths, &state).expect("write cluster state");
        crate::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("seed cluster raft config");
        paths
            .kv_paths()
            .raft_store()
            .set_leader_compact(&local, 1)
            .expect("establish leader");

        let mut storage = LocalClusterRaftCommandStorage::new(&paths, local.clone());
        let request = KvCommandRequest::generated(KvCommand::Barrier);
        let request_id = request.request_id.clone();
        let entry = storage
            .append_uncommitted(request)
            .expect("append current-term commit");
        storage
            .commit_index_compact(entry.index)
            .expect("commit current-term entry");
        drop(storage);

        let wal_path = paths.kv_paths().raft_wal_paths(&local).wal;
        let wal_len_before = std::fs::metadata(&wal_path)
            .expect("request-outcome WAL before")
            .len();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = NoNetworkRequestOutcomeHost;
        let read = runtime
            .block_on(read_cluster_kv_request_outcome_async_with_host(
                runtime.handle(),
                &paths,
                &mut state,
                request_id.clone(),
                true,
                crate::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true),
                &mut host,
            ))
            .expect("ReadIndex request outcome");

        assert!(matches!(
            read.outcome,
            KvRequestOutcome::Committed(record) if record.request_id == request_id
        ));
        assert_eq!(
            std::fs::metadata(&wal_path)
                .expect("request-outcome WAL after")
                .len(),
            wal_len_before,
            "a steady-state request-outcome read must not append a barrier"
        );
    }

    #[test]
    fn leader_exact_set_read_uses_one_read_index_wave_without_barrier_or_snapshot_route() {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-exact-set-read-index");
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::UNIX_EPOCH;
        let cluster_id =
            ClusterId::from_legacy_named("cluster_exact_set_read_index").expect("cluster");
        let local = NodeId::from_legacy_named("node_exact_set_read_index_a").expect("local node");
        let peer_b = NodeId::from_legacy_named("node_exact_set_read_index_b").expect("peer b node");
        let peer_c = NodeId::from_legacy_named("node_exact_set_read_index_c").expect("peer c node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "exact-set read index",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state.term = 7;
        let capabilities = sinomo_node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities.clone();
        for (position, node_id) in [peer_b.clone(), peer_c.clone()].into_iter().enumerate() {
            let mut member = ClusterMemberRecord::local_voter(
                cluster_id.clone(),
                node_id.clone(),
                format!("peer-{}", position + 1),
                format!("pk-peer-{}", position + 1),
                now,
            );
            member.capabilities = capabilities.clone();
            member.allowed_capabilities = capabilities.clone();
            member.endpoints = vec![sinomo_network::daemon_lan_endpoint(
                &node_id,
                format!("10.99.1.{}:21353", position + 2),
                0,
            )];
            state.members.push(member);
        }

        let config = sinomo_kv::ClusterKvRaftConfigRecord {
            cluster_id,
            term: 7,
            kv_revision: 1,
            voters: vec![local.clone(), peer_b, peer_c],
            learners: Vec::new(),
            non_voting: Vec::new(),
            updated_at: now,
        };
        let mut node = paths
            .kv_paths()
            .raft_store()
            .open_node(&local)
            .expect("open local Raft node");
        node.become_leader(7)
            .expect("establish current-term leader");
        for member in &state.members {
            node.append_local_command(
                7,
                KvCommand::Op(KvOp::Put {
                    key: crate::keyspace::cluster_member_key(&member.node_id),
                    value: sinomo_kv::encode_kv_value(member).expect("encode exact-set member"),
                    lease_id: None,
                }),
            )
            .expect("append exact-set member");
        }
        node.append_local_command(
            7,
            KvCommand::Op(KvOp::Put {
                key: sinomo_kv::cluster_kv_raft_config_key(),
                value: sinomo_kv::encode_kv_value(&config).expect("encode Raft config"),
                lease_id: None,
            }),
        )
        .expect("append Raft config");
        node.append_local_command(
            7,
            KvCommand::Op(KvOp::Put {
                key: "/exact/a".to_string(),
                value: b"alpha".to_vec(),
                lease_id: None,
            }),
        )
        .expect("append first exact-set value");
        let final_entry = node
            .append_local_command(
                7,
                KvCommand::Op(KvOp::Put {
                    key: "/exact/b".to_string(),
                    value: b"beta".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append second exact-set value");
        node.commit_to(final_entry.index)
            .expect("commit exact-set fixture");
        state.kv_revision = node.store().revision().0;
        let last_index_before = node.last_log_index();
        drop(node);

        let keys = vec![
            "/exact/b".to_string(),
            "/exact/missing".to_string(),
            "/exact/a".to_string(),
            "/exact/b".to_string(),
        ];
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut host = ExactSetReadIndexHost::default();
        let read = runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &paths,
                &mut state,
                keys.clone(),
                crate::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true),
                &mut host,
            ))
            .expect("quorum-confirmed exact-set read");

        let values = read
            .value
            .iter()
            .map(|entry| entry.as_ref().map(|entry| entry.value.as_slice()))
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![
                Some(b"beta".as_slice()),
                None,
                Some(b"alpha".as_slice()),
                Some(b"beta".as_slice()),
            ]
        );
        assert_eq!(host.quorum_waves, 1, "the exact set needs one ReadIndex");
        assert_eq!(
            host.object_rpc_stream_opens, 0,
            "the current-term leader must not fetch an authoritative snapshot"
        );
        assert_eq!(
            paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&local)
                .expect("last Raft index after exact-set read"),
            last_index_before,
            "the steady-state exact-set read must not append a barrier"
        );

        let preparations_before =
            crate::raft_quorum::read_index_resident_preparations_for_root(&paths.root);
        let contended = Arc::new(ContendedExactSetReadIndexHostState::new());
        let mut first_state = state.clone();
        let mut second_state = state.clone();
        let mut first_host = ExactSetReadIndexHost {
            contended: Some(Arc::clone(&contended)),
            ..ExactSetReadIndexHost::default()
        };
        let mut second_host = ExactSetReadIndexHost {
            contended: Some(Arc::clone(&contended)),
            ..ExactSetReadIndexHost::default()
        };
        let options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        let (first, second, ()) = runtime.block_on(async {
            let first = read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &paths,
                &mut first_state,
                keys.clone(),
                options,
                &mut first_host,
            );
            let second = read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &paths,
                &mut second_state,
                keys.clone(),
                options,
                &mut second_host,
            );
            let release = async {
                tokio::time::timeout(
                    Duration::from_secs(1),
                    contended.pre_dispatch_entered.acquire(),
                )
                .await
                .expect("contended exact-set owner did not reach pre-dispatch")
                .expect("contended exact-set pre-dispatch semaphore")
                .forget();
                tokio::time::timeout(Duration::from_secs(1), async {
                    while crate::raft_quorum::read_index_singleflight_waiters_for_root(&paths.root)
                        != 1
                    {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("second exact-set reader did not join the unsealed proof flight");
                contended.pre_dispatch_release.add_permits(1);
                tokio::time::timeout(Duration::from_secs(1), contended.wave_entered.acquire())
                    .await
                    .expect("contended exact-set proof did not dispatch")
                    .expect("contended exact-set proof dispatched")
                    .forget();
                contended.wave_release.add_permits(1);
            };
            tokio::join!(first, second, release)
        });
        assert!(first.is_ok(), "first contended exact-set read: {first:?}");
        assert!(
            second.is_ok(),
            "second contended exact-set read: {second:?}"
        );
        assert_eq!(
            contended.quorum_waves.load(Ordering::SeqCst),
            1,
            "concurrent exact-set reads must share one quorum proof"
        );
        assert_eq!(
            crate::raft_quorum::read_index_resident_preparations_for_root(&paths.root),
            preparations_before.saturating_add(1),
            "the proof flight must be claimed before config, leader, and ReadIndex resident preparation"
        );
    }

    #[test]
    fn forwarding_exact_read_does_not_wait_on_busy_local_raft_admission() {
        let fixture = cold_barrier_fixture("busy_local_admission_forwards_exact_read");
        let mut state = fixture.state.clone();
        let response = KvAuthoritativeRead {
            value: vec![None],
            authority: KvAuthorityProof {
                term: state.term,
                commit_index: fixture.initial_last_index,
                revision: KvRevision(state.kv_revision),
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
        };
        let mut host = ExactSetReadIndexHost {
            forwarded_exact_read: Some(response),
            ..ExactSetReadIndexHost::default()
        };
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(5);
        options.forward_options.timeout = Duration::from_secs(5);
        let held = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&fixture.paths.kv_paths())
            .expect("hold local Raft operation admission");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let started = Instant::now();

        let read = runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                vec!["/cold/missing".to_string()],
                options,
                &mut host,
            ))
            .expect("busy local admission must immediately forward the exact read");

        assert_eq!(read.value, vec![None]);
        assert_eq!(host.forwarded_exact_reads, 1);
        assert_eq!(host.quorum_waves, 0);
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "held local admission consumed {:?} of the five-second authority budget",
            started.elapsed()
        );
        drop(held);
    }

    #[test]
    fn stale_local_read_index_forwards_without_regressing_caller_topology_or_campaigning() {
        let fixture = cold_barrier_fixture("stale_local_read_index_forwards_exact_read");
        let mut state = fixture.state.clone();
        let caller_non_voter = state.members[2].node_id.clone();
        state.members[2].role = sinomo_base::ClusterMemberRole::NonVoting;
        state.members[2].state = ClusterMemberState::NonVoting;
        state.kv_revision = state.kv_revision.saturating_add(100);
        let caller_revision = state.kv_revision;
        let response = KvAuthoritativeRead {
            value: vec![None],
            authority: KvAuthorityProof {
                term: state.term,
                commit_index: fixture.initial_last_index,
                revision: KvRevision(caller_revision),
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
        };
        let mut host = ExactSetReadIndexHost {
            forwarded_exact_read: Some(response),
            ..ExactSetReadIndexHost::default()
        };
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(5);
        options.forward_options.timeout = Duration::from_secs(5);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let started = Instant::now();

        let read = runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                vec!["/cold/missing".to_string()],
                options,
                &mut host,
            ))
            .expect("a caller-ahead projection must forward to the retained authority");

        assert_eq!(read.value, vec![None]);
        assert_eq!(host.forwarded_exact_reads, 1);
        assert_eq!(host.quorum_waves, 0, "local ReadIndex/barrier is forbidden");
        assert_eq!(host.vote_requests, 0, "local campaigning is forbidden");
        assert_eq!(host.forwarded_exact_states.len(), 1);
        let forwarded_member = host.forwarded_exact_states[0]
            .members
            .iter()
            .find(|member| member.node_id == caller_non_voter)
            .expect("forwarded caller member");
        assert_eq!(
            forwarded_member.role,
            sinomo_base::ClusterMemberRole::NonVoting,
            "the stale resident config must not replace the caller's topology before forwarding"
        );
        assert_eq!(
            state
                .members
                .iter()
                .find(|member| member.node_id == caller_non_voter)
                .expect("caller member after forwarding")
                .role,
            sinomo_base::ClusterMemberRole::NonVoting,
            "successful forwarding must preserve the caller's fresher topology"
        );
        assert_eq!(state.kv_revision, caller_revision);
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("last Raft index after caller-ahead read"),
            fixture.initial_last_index,
            "a caller-ahead read must not append a current-term barrier"
        );
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "caller-ahead forwarding consumed {:?} of the five-second authority budget",
            started.elapsed()
        );
    }

    #[test]
    fn stale_local_read_index_forwarding_keeps_newer_resident_term_and_caller_revision() {
        let fixture = cold_barrier_fixture("crossed_forwarding_resident_term_caller_revision");
        let mut state = fixture.state.clone();
        let resident_term = state.term;
        let resident_revision = state.kv_revision;
        state.term = resident_term.saturating_sub(1);
        state.kv_revision = resident_revision.saturating_add(100);
        let caller_revision = state.kv_revision;
        let response = KvAuthoritativeRead {
            value: vec![None],
            authority: KvAuthorityProof {
                term: resident_term,
                commit_index: fixture.initial_last_index,
                revision: KvRevision(caller_revision),
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
        };
        let mut host = ExactSetReadIndexHost {
            forwarded_exact_read: Some(response),
            ..ExactSetReadIndexHost::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                vec!["/cold/missing".to_string()],
                crate::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true),
                &mut host,
            ))
            .expect("crossed authority floors must forward");

        assert_eq!(host.forwarded_exact_reads, 1);
        assert_eq!(host.forwarded_exact_states[0].term, resident_term);
        assert_eq!(host.forwarded_exact_states[0].kv_revision, caller_revision);
        assert_eq!(host.quorum_waves, 0, "local barrier is forbidden");
        assert_eq!(host.vote_requests, 0, "local campaigning is forbidden");
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("last Raft index after crossed-floor forward"),
            fixture.initial_last_index
        );
    }

    #[test]
    fn stale_local_read_index_forwarding_keeps_newer_caller_term_and_resident_revision() {
        let fixture = cold_barrier_fixture("crossed_forwarding_caller_term_resident_revision");
        let mut state = fixture.state.clone();
        let resident_term = state.term;
        let resident_revision = state.kv_revision;
        state.term = resident_term.saturating_add(1);
        state.kv_revision = resident_revision.saturating_sub(1);
        let caller_term = state.term;
        let response = KvAuthoritativeRead {
            value: vec![None],
            authority: KvAuthorityProof {
                term: caller_term,
                commit_index: fixture.initial_last_index,
                revision: KvRevision(resident_revision),
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
        };
        let mut host = ExactSetReadIndexHost {
            forwarded_exact_read: Some(response),
            ..ExactSetReadIndexHost::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                vec!["/cold/missing".to_string()],
                crate::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true),
                &mut host,
            ))
            .expect("crossed authority floors must forward");

        assert_eq!(host.forwarded_exact_reads, 1);
        assert_eq!(host.forwarded_exact_states[0].term, caller_term);
        assert_eq!(
            host.forwarded_exact_states[0].kv_revision,
            resident_revision
        );
        assert_eq!(host.quorum_waves, 0, "local barrier is forbidden");
        assert_eq!(host.vote_requests, 0, "local campaigning is forbidden");
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("last Raft index after crossed-floor forward"),
            fixture.initial_last_index
        );
    }

    #[test]
    fn crossed_floor_forwarding_uses_newer_five_voter_topology_and_three_vote_quorum() {
        let fixture = crossed_floor_changed_voter_fixture("crossed_floor_changed_five_voters");
        let mut state = fixture.caller_state.clone();
        let response = KvAuthoritativeRead {
            value: vec![None],
            authority: KvAuthorityProof {
                term: state.term,
                commit_index: fixture.resident_last_index,
                revision: KvRevision(fixture.resident_revision),
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 3,
                    required_quorum: 3,
                },
            },
        };
        let mut host = ExactSetReadIndexHost {
            forwarded_exact_read: Some(response),
            ..ExactSetReadIndexHost::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let read = runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.cold.paths,
                &mut state,
                vec!["/cold/missing".to_string()],
                crate::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true),
                &mut host,
            ))
            .expect("three of five votes must satisfy the rebuilt forwarding topology");

        assert!(matches!(
            read.authority.basis,
            KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 3,
                required_quorum: 3
            }
        ));
        let state_voters =
            sinomo_kv::cluster_kv_raft_voter_ids(&state).expect("published caller voter topology");
        assert_eq!(state_voters.len(), 5);
        assert!(!state_voters.contains(&fixture.removed_caller_voter));

        runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.cold.paths,
                &mut state,
                vec!["/cold/missing".to_string()],
                crate::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true),
                &mut host,
            ))
            .expect("the published topology must remain coherent on the next equal-revision read");

        assert_eq!(host.forwarded_exact_reads, 2);
        assert_eq!(host.forwarded_exact_states.len(), 2);
        let forwarded_state = &host.forwarded_exact_states[0];
        let forwarded_voters = sinomo_kv::cluster_kv_raft_voter_ids(forwarded_state)
            .expect("forwarded voter topology");
        assert_eq!(forwarded_voters.len(), 5);
        assert!(
            !forwarded_voters.contains(&fixture.removed_caller_voter),
            "a voter removed by the newer committed config must not be targeted or counted"
        );
        assert!(
            forwarded_state
                .members
                .iter()
                .all(|member| member.node_id != fixture.removed_caller_voter),
            "a caller-only removed member must not survive the forwarding fence"
        );
        let promoted = forwarded_state
            .members
            .iter()
            .find(|member| member.node_id == fixture.promoted_resident_voter)
            .expect("promoted resident voter");
        assert_eq!(promoted.role, sinomo_base::ClusterMemberRole::Voter);
        assert_eq!(promoted.state, ClusterMemberState::Voter);
        assert!(
            promoted
                .endpoints
                .iter()
                .any(|endpoint| endpoint.endpoint_id == fixture.caller_endpoint_id),
            "the current caller's endpoint observation must be retained for an allowed member"
        );
        assert_eq!(host.quorum_waves, 0, "local barrier is forbidden");
        assert_eq!(host.vote_requests, 0, "local campaigning is forbidden");
        assert_eq!(
            fixture
                .cold
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.cold.local)
                .expect("last Raft index after changed-topology forward"),
            fixture.resident_last_index
        );
    }

    #[test]
    fn crossed_floor_forwarding_rejects_old_two_of_three_quorum_after_five_voter_change() {
        let fixture = crossed_floor_changed_voter_fixture("crossed_floor_rejects_old_quorum");
        let mut state = fixture.caller_state.clone();
        let weak_response = KvAuthoritativeRead {
            value: vec![None],
            authority: KvAuthorityProof {
                term: state.term,
                commit_index: fixture.resident_last_index,
                revision: KvRevision(fixture.resident_revision),
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
        };
        let mut host = ExactSetReadIndexHost {
            forwarded_exact_read: Some(weak_response.clone()),
            ..ExactSetReadIndexHost::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.cold.paths,
                &mut state,
                vec!["/cold/missing".to_string()],
                crate::ClusterControlPlaneTimeouts::default_daemon_transport()
                    .kv_txn_quorum_options(true),
                &mut host,
            ))
            .expect_err("the old two-of-three proof must not satisfy five-voter membership");

        assert_eq!(host.forwarded_exact_reads, 1);
        let forwarded_state = &host.forwarded_exact_states[0];
        let validation_error = validate_forwarded_authoritative_entries_read(
            weak_response,
            forwarded_state,
            &["/cold/missing".to_string()],
        )
        .expect_err("two acknowledgements cannot prove a five-voter read");
        assert!(
            validation_error
                .to_string()
                .contains("committed membership has 5 voters and requires 3"),
            "unexpected weak-quorum rejection: {validation_error}"
        );
        let forwarded_voters = sinomo_kv::cluster_kv_raft_voter_ids(forwarded_state)
            .expect("forwarded voter topology");
        assert!(!forwarded_voters.contains(&fixture.removed_caller_voter));
        assert_eq!(host.quorum_waves, 0, "local barrier is forbidden");
        assert_eq!(host.vote_requests, 0, "local campaigning is forbidden");
        assert_eq!(
            fixture
                .cold
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.cold.local)
                .expect("last Raft index after rejected changed-topology forward"),
            fixture.resident_last_index
        );
    }

    #[test]
    fn stale_local_read_index_forward_miss_returns_local_behind_without_campaigning() {
        let fixture = cold_barrier_fixture("stale_local_read_index_forward_miss");
        let mut state = fixture.state.clone();
        let resident_revision = state.kv_revision;
        state.kv_revision = resident_revision.saturating_add(100);
        let caller_revision = state.kv_revision;
        let mut host = ExactSetReadIndexHost {
            forwarded_exact_read_returns_none: true,
            ..ExactSetReadIndexHost::default()
        };
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(5);
        options.forward_options.timeout = Duration::from_secs(5);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let started = Instant::now();

        let error = runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                vec!["/cold/missing".to_string()],
                options,
                &mut host,
            ))
            .expect_err("a failed forward must preserve the typed local-behind failure");

        assert!(
            error.to_string().contains(&format!(
                "local Raft revision {resident_revision} trails caller-observed cluster revision {caller_revision}"
            )),
            "unexpected caller-ahead error: {error}"
        );
        assert!(
            error.to_string().contains(&format!(
                "observed term {}, revision {resident_revision}; required term {}, revision {caller_revision}",
                state.term, state.term
            )),
            "local-behind diagnostics lost a scalar authority floor: {error}"
        );
        assert_eq!(host.forwarded_exact_reads, 1);
        assert_eq!(host.quorum_waves, 0, "local barrier is forbidden");
        assert_eq!(host.vote_requests, 0, "local campaigning is forbidden");
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("last Raft index after failed caller-ahead forward"),
            fixture.initial_last_index,
            "failed forwarding must not append a current-term barrier"
        );
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "failed caller-ahead forwarding consumed {:?} of the five-second authority budget",
            started.elapsed()
        );
    }

    #[test]
    fn sub_hundred_millisecond_quorum_latency_does_not_expand_into_authority_timeout() {
        let fixture = cold_barrier_fixture("bounded_network_latency_read_index");
        let mut storage =
            LocalClusterRaftCommandStorage::new(&fixture.paths, fixture.local.clone());
        let barrier = storage
            .append_uncommitted(KvCommandRequest::generated(KvCommand::Barrier))
            .expect("append current-term authority anchor");
        storage
            .commit_index_compact(barrier.index)
            .expect("commit current-term authority anchor");
        drop(storage);

        let mut exact_state = fixture.state.clone();
        let mut prefix_state = fixture.state.clone();
        let simulated_quorum_latency = Duration::from_millis(75);
        let mut exact_host = ExactSetReadIndexHost {
            quorum_delay: simulated_quorum_latency,
            ..ExactSetReadIndexHost::default()
        };
        let mut prefix_host = ExactSetReadIndexHost {
            quorum_delay: simulated_quorum_latency,
            ..ExactSetReadIndexHost::default()
        };
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(5);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let exact_started = Instant::now();
        let exact = runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut exact_state,
                vec!["/cold/a".to_string(), "/cold/missing".to_string()],
                options,
                &mut exact_host,
            ))
            .expect("exact authority read with bounded quorum latency");
        let exact_elapsed = exact_started.elapsed();
        assert_eq!(
            exact.value[0].as_ref().map(|entry| entry.value.as_slice()),
            Some(b"alpha".as_slice())
        );
        assert_eq!(exact.value[1], None);
        assert_eq!(exact_host.quorum_waves, 1);
        assert!(
            exact_elapsed >= simulated_quorum_latency && exact_elapsed < Duration::from_millis(500),
            "75ms quorum latency expanded into {exact_elapsed:?} of a five-second authority budget"
        );

        let prefix_started = Instant::now();
        let prefix = runtime
            .block_on(read_cluster_kv_authoritative_range_prefix_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut prefix_state,
                "/cold/".to_string(),
                options,
                &mut prefix_host,
            ))
            .expect("prefix authority read with bounded quorum latency");
        let prefix_elapsed = prefix_started.elapsed();
        assert_eq!(prefix.value.len(), 2);
        assert_eq!(prefix_host.quorum_waves, 1);
        assert!(
            prefix_elapsed >= simulated_quorum_latency
                && prefix_elapsed < Duration::from_millis(500),
            "75ms quorum latency expanded into {prefix_elapsed:?} of a five-second authority budget"
        );
    }

    #[test]
    fn busy_local_point_read_waits_briefly_for_forwarding_contender() {
        let fixture = cold_barrier_fixture("busy_local_point_read_rejoins_resident_leader");
        let last_index_before = commit_current_term_barrier(&fixture);
        let preparations_before =
            crate::raft_quorum::read_index_resident_preparations_for_root(&fixture.paths.root);
        let held = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&fixture.paths.kv_paths())
            .expect("hold local Raft operation admission");
        let mut state = fixture.state.clone();
        let mut host = ExactSetReadIndexHost {
            release_busy_admission_on_forward: Some(held),
            busy_admission_release_delay: Duration::from_millis(12),
            ..ExactSetReadIndexHost::default()
        };
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(5);
        options.forward_options.timeout = Duration::from_secs(5);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let started = Instant::now();

        let read = runtime
            .block_on(read_cluster_kv_authoritative_entry_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                "/cold/a".to_string(),
                options,
                &mut host,
            ))
            .expect(
                "point read must rejoin the resident leader after forwarding releases admission",
            );

        assert_eq!(
            read.value.as_ref().map(|entry| entry.value.as_slice()),
            Some(b"alpha".as_slice())
        );
        assert_eq!(host.forwarded_point_reads, 1);
        assert_eq!(host.quorum_waves, 1);
        assert_eq!(host.vote_requests, 0, "local retry must never campaign");
        let preparations_after =
            crate::raft_quorum::read_index_resident_preparations_for_root(&fixture.paths.root);
        assert!(
            preparations_after >= preparations_before.saturating_add(3),
            "point read must perform the initial busy probe and multiple non-queued retries; before={preparations_before}, after={preparations_after}"
        );
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("point retry last Raft index"),
            last_index_before,
            "local retry must not append a barrier or campaign"
        );
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "point read local rejoin took {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn follower_point_read_retries_remote_busy_authority_without_campaigning() {
        let fixture = cold_barrier_fixture("follower_point_read_retries_remote_busy_authority");
        let last_index_before = commit_current_term_barrier(&fixture);
        fixture
            .paths
            .kv_paths()
            .raft_store()
            .open_node(&fixture.local)
            .expect("open follower fixture node")
            .clear_known_leader()
            .expect("make fixture a follower without a known leader");
        let mut state = fixture.state.clone();
        let forwarded = KvAuthoritativeRead {
            value: None,
            authority: KvAuthorityProof {
                term: state.term,
                commit_index: last_index_before,
                revision: KvRevision(state.kv_revision),
                basis: sinomo_kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
        };
        let mut host = ExactSetReadIndexHost {
            forwarded_point_read: Some(forwarded),
            forwarded_point_read_returns_busy_count: 3,
            ..ExactSetReadIndexHost::default()
        };
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(5);
        options.forward_options.timeout = Duration::from_secs(5);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let read = runtime
            .block_on(read_cluster_kv_authoritative_entry_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                "/cold/missing".to_string(),
                options,
                &mut host,
            ))
            .expect("follower must re-probe a transiently busy remote authority");

        assert!(read.value.is_none());
        assert_eq!(host.forwarded_point_reads, 4);
        assert_eq!(host.quorum_waves, 0, "follower must not run ReadIndex");
        assert_eq!(
            host.vote_requests, 0,
            "remote busy retry must never campaign"
        );
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("remote busy retry last Raft index"),
            last_index_before,
            "remote busy retry must not append a barrier or campaign"
        );
    }

    #[test]
    fn follower_exact_read_retries_remote_busy_authority_without_campaigning() {
        let fixture = cold_barrier_fixture("follower_exact_read_retries_remote_busy_authority");
        let last_index_before = commit_current_term_barrier(&fixture);
        fixture
            .paths
            .kv_paths()
            .raft_store()
            .open_node(&fixture.local)
            .expect("open follower fixture node")
            .clear_known_leader()
            .expect("make fixture a follower without a known leader");
        let mut state = fixture.state.clone();
        let keys = vec!["/cold/missing".to_string()];
        let forwarded = KvAuthoritativeRead {
            value: vec![None],
            authority: KvAuthorityProof {
                term: state.term,
                commit_index: last_index_before,
                revision: KvRevision(state.kv_revision),
                basis: sinomo_kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
        };
        let mut host = ExactSetReadIndexHost {
            forwarded_exact_read: Some(forwarded),
            forwarded_exact_read_returns_busy_count: 3,
            ..ExactSetReadIndexHost::default()
        };
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(5);
        options.forward_options.timeout = Duration::from_secs(5);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let read = runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                keys,
                options,
                &mut host,
            ))
            .expect("follower must re-probe a transiently busy remote authority");

        assert_eq!(read.value, vec![None]);
        assert_eq!(host.forwarded_exact_reads, 4);
        assert_eq!(host.quorum_waves, 0, "follower must not run ReadIndex");
        assert_eq!(
            host.vote_requests, 0,
            "remote busy retry must never campaign"
        );
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("remote busy retry last Raft index"),
            last_index_before,
            "remote busy retry must not append a barrier or campaign"
        );
    }

    #[test]
    fn busy_local_prefix_read_waits_briefly_for_forwarding_contender() {
        let fixture = cold_barrier_fixture("busy_local_prefix_read_rejoins_resident_leader");
        let last_index_before = commit_current_term_barrier(&fixture);
        let preparations_before =
            crate::raft_quorum::read_index_resident_preparations_for_root(&fixture.paths.root);
        let held = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&fixture.paths.kv_paths())
            .expect("hold local Raft operation admission");
        let mut state = fixture.state.clone();
        let mut host = ExactSetReadIndexHost {
            release_busy_admission_on_forward: Some(held),
            busy_admission_release_delay: Duration::from_millis(12),
            ..ExactSetReadIndexHost::default()
        };
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(5);
        options.forward_options.timeout = Duration::from_secs(5);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let started = Instant::now();

        let read = runtime
            .block_on(read_cluster_kv_authoritative_range_prefix_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                "/cold/".to_string(),
                options,
                &mut host,
            ))
            .expect("prefix read must wait briefly and rejoin the resident leader");

        assert_eq!(
            read.value
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["/cold/a", "/cold/b"]
        );
        assert_eq!(host.forwarded_prefix_reads, 1);
        assert_eq!(host.quorum_waves, 1);
        assert_eq!(host.vote_requests, 0, "local retry must never campaign");
        let preparations_after =
            crate::raft_quorum::read_index_resident_preparations_for_root(&fixture.paths.root);
        assert!(
            preparations_after >= preparations_before.saturating_add(3),
            "prefix read must perform the initial busy probe and multiple non-queued retries; before={preparations_before}, after={preparations_after}"
        );
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("prefix retry last Raft index"),
            last_index_before,
            "local retry must not append a barrier or campaign"
        );
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "prefix read bounded local rejoin took {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn busy_follower_prefix_read_reforwards_after_local_admission_clears() {
        let fixture = cold_barrier_fixture("busy_follower_prefix_read_reforwards");
        let last_index_before = commit_current_term_barrier(&fixture);
        fixture
            .paths
            .kv_paths()
            .raft_store()
            .open_node(&fixture.local)
            .expect("open follower fixture node")
            .clear_known_leader()
            .expect("make fixture a follower without a known leader");
        let held = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&fixture.paths.kv_paths())
            .expect("hold local Raft operation admission");
        let mut state = fixture.state.clone();
        let forwarded = KvAuthoritativeRead {
            value: Vec::new(),
            authority: KvAuthorityProof {
                term: state.term,
                commit_index: last_index_before,
                revision: KvRevision(state.kv_revision),
                basis: sinomo_kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
        };
        let mut host = ExactSetReadIndexHost {
            release_busy_admission_on_forward: Some(held),
            forwarded_prefix_read: Some(forwarded),
            forwarded_prefix_read_returns_none_count: 3,
            ..ExactSetReadIndexHost::default()
        };
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(5);
        options.forward_options.timeout = Duration::from_secs(5);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let read = runtime
            .block_on(read_cluster_kv_authoritative_range_prefix_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                "/cold/".to_string(),
                options,
                &mut host,
            ))
            .expect("busy follower must retry forwarding after admission settles");

        assert!(read.value.is_empty());
        assert_eq!(host.forwarded_prefix_reads, 4);
        assert_eq!(host.quorum_waves, 0, "follower must not run ReadIndex");
        assert_eq!(host.vote_requests, 0, "forward retry must never campaign");
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("follower retry last Raft index"),
            last_index_before,
            "forward retry must not append a barrier or campaign"
        );
    }

    #[test]
    fn follower_prefix_read_retries_remote_busy_authority_without_campaigning() {
        let fixture = cold_barrier_fixture("follower_prefix_read_retries_remote_busy_authority");
        let last_index_before = commit_current_term_barrier(&fixture);
        fixture
            .paths
            .kv_paths()
            .raft_store()
            .open_node(&fixture.local)
            .expect("open follower fixture node")
            .clear_known_leader()
            .expect("make fixture a follower without a known leader");
        let mut state = fixture.state.clone();
        let forwarded = KvAuthoritativeRead {
            value: Vec::new(),
            authority: KvAuthorityProof {
                term: state.term,
                commit_index: last_index_before,
                revision: KvRevision(state.kv_revision),
                basis: sinomo_kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
        };
        let mut host = ExactSetReadIndexHost {
            forwarded_prefix_read: Some(forwarded),
            forwarded_prefix_read_returns_busy_count: 3,
            ..ExactSetReadIndexHost::default()
        };
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(5);
        options.forward_options.timeout = Duration::from_secs(5);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let read = runtime
            .block_on(read_cluster_kv_authoritative_range_prefix_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                "/cold/".to_string(),
                options,
                &mut host,
            ))
            .expect("follower must re-probe a transiently busy remote authority");

        assert!(read.value.is_empty());
        assert_eq!(host.forwarded_prefix_reads, 4);
        assert_eq!(host.quorum_waves, 0, "follower must not run ReadIndex");
        assert_eq!(
            host.vote_requests, 0,
            "remote busy retry must never campaign"
        );
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("remote busy retry last Raft index"),
            last_index_before,
            "remote busy retry must not append a barrier or campaign"
        );
    }

    #[test]
    fn follower_snapshot_read_retries_remote_busy_authority_without_campaigning() {
        let fixture = cold_barrier_fixture("follower_snapshot_read_busy_authority");
        let last_index = commit_current_term_barrier(&fixture);
        let snapshot = sinomo_kv::read_cluster_kv_current_snapshot_for_node(
            &fixture.paths.kv_paths(),
            fixture.local.clone(),
        )
        .unwrap();
        fixture
            .paths
            .kv_paths()
            .raft_store()
            .open_node(&fixture.local)
            .unwrap()
            .clear_known_leader()
            .unwrap();
        let mut state = fixture.state.clone();
        let forwarded = KvAuthoritativeRead {
            authority: KvAuthorityProof {
                term: state.term,
                commit_index: last_index,
                revision: snapshot.revision,
                basis: sinomo_kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
            value: snapshot,
        };
        let mut host = ExactSetReadIndexHost {
            forwarded_snapshot_read: Some(forwarded.clone()),
            forwarded_snapshot_read_returns_busy_count: 3,
            ..ExactSetReadIndexHost::default()
        };
        let options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let read = runtime
            .block_on(read_cluster_kv_authoritative_snapshot_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                options,
                &mut host,
            ))
            .expect("originating follower must retry transient remote snapshot backpressure");
        assert_eq!(read, forwarded);
        assert_eq!(host.forwarded_snapshot_reads, 4);
        assert_eq!(host.vote_requests, 0);
        assert_eq!(host.quorum_waves, 0);
    }

    #[test]
    fn remote_busy_snapshot_read_exhausts_grace_without_campaigning() {
        let fixture = cold_barrier_fixture("snapshot_remote_busy_exhausted");
        commit_current_term_barrier(&fixture);
        fixture
            .paths
            .kv_paths()
            .raft_store()
            .open_node(&fixture.local)
            .unwrap()
            .clear_known_leader()
            .unwrap();
        let mut state = fixture.state.clone();
        let mut host = ExactSetReadIndexHost {
            forwarded_snapshot_read_returns_busy_count: usize::MAX,
            ..ExactSetReadIndexHost::default()
        };
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(2);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let started = Instant::now();
        let error = runtime
            .block_on(read_cluster_kv_authoritative_snapshot_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                options,
                &mut host,
            ))
            .expect_err("sustained remote backpressure must remain explicit");
        assert!(forwarded_snapshot_read_admission_is_busy(&error));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!((2..=60).contains(&host.forwarded_snapshot_reads));
        assert_eq!(host.vote_requests, 0);
        assert_eq!(host.quorum_waves, 0);
    }

    #[test]
    fn busy_local_snapshot_read_waits_for_bounded_admission_without_campaigning() {
        let fixture = cold_barrier_fixture("busy_local_snapshot_bounded_rejoin");
        let last_index = commit_current_term_barrier(&fixture);
        let held =
            sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&fixture.paths.kv_paths()).unwrap();
        let mut state = fixture.state.clone();
        let mut host = ExactSetReadIndexHost {
            release_busy_admission_on_snapshot_forward: Some(held),
            busy_admission_release_delay: Duration::from_millis(30),
            ..ExactSetReadIndexHost::default()
        };
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(2);
        options.forward_options.timeout = Duration::from_secs(2);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let read = runtime
            .block_on(read_cluster_kv_authoritative_snapshot_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                options,
                &mut host,
            ))
            .expect("a short busy period must not fail the originating snapshot read");
        assert!(matches!(
            read.authority.basis,
            sinomo_kv::KvAuthorityBasis::CurrentTermQuorum { .. }
        ));
        assert_eq!(host.forwarded_snapshot_reads, 1);
        assert_eq!(host.quorum_waves, 1);
        assert_eq!(host.vote_requests, 0);
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .unwrap(),
            last_index
        );
    }

    #[test]
    fn busy_snapshot_read_preserves_deadline_and_forwarded_fail_fast_behavior() {
        for allow_forwarding in [false, true] {
            let fixture = cold_barrier_fixture("busy_snapshot_deadline");
            commit_current_term_barrier(&fixture);
            let _held =
                sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&fixture.paths.kv_paths())
                    .unwrap();
            let mut state = fixture.state.clone();
            let mut host = ExactSetReadIndexHost::default();
            let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
                .kv_txn_quorum_options(allow_forwarding);
            options.raft_append_timeout = Duration::from_millis(20);
            options.forward_options.timeout = Duration::from_millis(20);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let started = Instant::now();
            runtime
                .block_on(read_cluster_kv_authoritative_snapshot_async_with_host(
                    runtime.handle(),
                    &fixture.paths,
                    &mut state,
                    options,
                    &mut host,
                ))
                .expect_err("a busy admission is not authority");
            assert!(
                started.elapsed() < Duration::from_millis(150),
                "retry must honor the short caller deadline"
            );
            assert_eq!(host.vote_requests, 0);
            assert_eq!(host.quorum_waves, 0);
            if !allow_forwarding {
                assert_eq!(host.forwarded_snapshot_reads, 0);
            }
        }
    }

    #[test]
    fn busy_local_exact_read_retries_after_failed_mixed_version_snapshot_fallback() {
        let fixture = cold_barrier_fixture("busy_local_exact_read_mixed_version_snapshot_rejoin");
        let last_index_before = commit_current_term_barrier(&fixture);
        let preparations_before =
            crate::raft_quorum::read_index_resident_preparations_for_root(&fixture.paths.root);
        let held = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&fixture.paths.kv_paths())
            .expect("hold local Raft operation admission");
        let mut state = fixture.state.clone();
        let mut host = ExactSetReadIndexHost {
            release_busy_admission_on_snapshot_forward: Some(held),
            busy_admission_release_delay: Duration::from_millis(12),
            forwarded_exact_read_unknown_route: true,
            ..ExactSetReadIndexHost::default()
        };
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(5);
        options.forward_options.timeout = Duration::from_secs(5);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let started = Instant::now();

        let read = runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                vec!["/cold/a".to_string()],
                options,
                &mut host,
            ))
            .expect(
                "failed mixed-version snapshot fallback must rejoin the resident exact-read leader",
            );

        assert_eq!(
            read.value[0].as_ref().map(|entry| entry.value.as_slice()),
            Some(b"alpha".as_slice())
        );
        assert_eq!(host.forwarded_exact_reads, 1);
        assert_eq!(host.forwarded_snapshot_reads, 1);
        assert_eq!(host.quorum_waves, 1);
        assert_eq!(host.vote_requests, 0, "local retry must never campaign");
        let preparations_after =
            crate::raft_quorum::read_index_resident_preparations_for_root(&fixture.paths.root);
        assert!(
            preparations_after >= preparations_before.saturating_add(3),
            "mixed-version fallback must perform the initial busy probe and multiple non-queued retries; before={preparations_before}, after={preparations_after}"
        );
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("mixed-version retry last Raft index"),
            last_index_before,
            "mixed-version local retry must not append a barrier or campaign"
        );
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "mixed-version exact read local rejoin took {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn busy_local_exact_read_preserves_failed_mixed_version_fallback_when_retry_stays_busy() {
        let fixture =
            cold_barrier_fixture("busy_local_exact_read_mixed_version_snapshot_still_busy");
        let last_index_before = commit_current_term_barrier(&fixture);
        let preparations_before =
            crate::raft_quorum::read_index_resident_preparations_for_root(&fixture.paths.root);
        let mut held = LocalClusterRaftCommandStorage::new(&fixture.paths, fixture.local.clone());
        held.read_index_state()
            .expect("hold local Raft resident and operation admission");
        let mut state = fixture.state.clone();
        let mut host = ExactSetReadIndexHost {
            forwarded_exact_read_unknown_route: true,
            ..ExactSetReadIndexHost::default()
        };
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(5);
        options.forward_options.timeout = Duration::from_secs(5);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let started = Instant::now();

        let error = runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                vec!["/cold/a".to_string()],
                options,
                &mut host,
            ))
            .expect_err("a still-busy retry must preserve the snapshot fallback failure");

        assert!(
            matches!(&error, NetworkError::ClusterReadOnly { detail } if detail == "current-term snapshot read unexpectedly forwarded"),
            "unexpected mixed-version fallback failure: {error}"
        );
        assert_eq!(host.forwarded_exact_reads, 1);
        assert_eq!(host.forwarded_snapshot_reads, 1);
        assert_eq!(host.quorum_waves, 0);
        assert_eq!(host.vote_requests, 0);
        assert_bounded_authority_busy_preparations(
            preparations_before,
            crate::raft_quorum::read_index_resident_preparations_for_root(&fixture.paths.root),
            "mixed-version failure",
        );
        drop(held);
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("mixed-version still-busy last Raft index"),
            last_index_before
        );
        assert!(
            started.elapsed() < BOUNDED_AUTHORITY_BUSY_RETRY_BUDGET + Duration::from_millis(250),
            "mixed-version still-busy retry took {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn already_forwarded_exact_read_does_not_wait_on_busy_local_raft_admission() {
        let fixture = cold_barrier_fixture("busy_local_admission_rejects_forwarded_exact_read");
        let mut state = fixture.state.clone();
        let mut host = ExactSetReadIndexHost::default();
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(false);
        options.raft_append_timeout = Duration::from_secs(5);
        options.forward_options.timeout = Duration::from_secs(5);
        let held = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&fixture.paths.kv_paths())
            .expect("hold local Raft operation admission");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let started = Instant::now();

        let error = runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                vec!["/cold/missing".to_string()],
                options,
                &mut host,
            ))
            .expect_err("an already-forwarded read must reject busy local admission promptly");

        assert!(matches!(error, NetworkError::ClusterReadOnly { .. }));
        assert_eq!(host.forwarded_exact_reads, 0);
        assert_eq!(host.quorum_waves, 0);
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "already-forwarded authority read waited {:?} on a five-second local admission budget",
            started.elapsed()
        );
        drop(held);
    }

    #[test]
    fn already_forwarded_prefix_read_does_not_wait_on_busy_local_raft_admission() {
        let fixture = cold_barrier_fixture("busy_local_admission_rejects_forwarded_prefix_read");
        let mut state = fixture.state.clone();
        let mut host = ExactSetReadIndexHost::default();
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(false);
        options.raft_append_timeout = Duration::from_secs(5);
        options.forward_options.timeout = Duration::from_secs(5);
        let held = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&fixture.paths.kv_paths())
            .expect("hold local Raft operation admission");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let started = Instant::now();

        let error = runtime
            .block_on(read_cluster_kv_authoritative_range_prefix_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                "/dfs/filesystems/".to_string(),
                options,
                &mut host,
            ))
            .expect_err("an already-forwarded prefix read must reject busy admission promptly");

        assert!(matches!(error, NetworkError::ClusterReadOnly { .. }));
        assert_eq!(host.quorum_waves, 0);
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "already-forwarded prefix authority waited {:?} on a five-second local admission budget",
            started.elapsed()
        );
        drop(held);
    }

    #[test]
    fn forwarding_exact_read_bypasses_saturated_blocking_pool_on_busy_local_admission() {
        let fixture = cold_barrier_fixture("busy_local_admission_saturated_blocking_pool");
        let mut state = fixture.state.clone();
        let response = KvAuthoritativeRead {
            value: vec![None],
            authority: KvAuthorityProof {
                term: state.term,
                commit_index: fixture.initial_last_index,
                revision: KvRevision(state.kv_revision),
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
        };
        let mut host = ExactSetReadIndexHost {
            forwarded_exact_read: Some(response),
            ..ExactSetReadIndexHost::default()
        };
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(5);
        options.forward_options.timeout = Duration::from_secs(5);
        let held = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&fixture.paths.kv_paths())
            .expect("hold local Raft operation admission");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let (blocker_started_tx, blocker_started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_blocker_tx, release_blocker_rx) = std::sync::mpsc::sync_channel(0);
        let blocker = runtime.spawn_blocking(move || {
            blocker_started_tx.send(()).expect("report blocker start");
            release_blocker_rx.recv().expect("release blocking worker");
        });
        blocker_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking worker saturated");
        let started = Instant::now();

        let read = runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                vec!["/cold/missing".to_string()],
                options,
                &mut host,
            ))
            .expect("busy local admission must forward without the blocking pool");

        assert_eq!(read.value, vec![None]);
        assert_eq!(host.forwarded_exact_reads, 1);
        assert_eq!(host.quorum_waves, 0);
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "blocking-pool starvation consumed {:?} of the five-second authority budget",
            started.elapsed()
        );
        release_blocker_tx.send(()).expect("release blocker");
        runtime.block_on(blocker).expect("join blocker");
        drop(held);
    }

    #[test]
    fn failed_forward_does_not_retry_busy_local_raft_admission_through_election() {
        let fixture = cold_barrier_fixture("busy_local_admission_failed_forward");
        let mut state = fixture.state.clone();
        let mut host = ExactSetReadIndexHost::default();
        let mut options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        options.raft_append_timeout = Duration::from_secs(5);
        options.forward_options.timeout = Duration::from_secs(5);
        let mut held = LocalClusterRaftCommandStorage::new(&fixture.paths, fixture.local.clone());
        held.read_index_state()
            .expect("hold local Raft resident and operation admission");
        let preparations_before =
            crate::raft_quorum::read_index_resident_preparations_for_root(&fixture.paths.root);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let started = Instant::now();

        let error = runtime
            .block_on(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                vec!["/cold/missing".to_string()],
                options,
                &mut host,
            ))
            .expect_err("failed forwarding must not retry a proven-busy local admission");

        assert!(
            matches!(&error, NetworkError::ClusterReadOnly { detail } if detail == "current-term exact-set read unexpectedly forwarded"),
            "the still-busy local retry must preserve the original forwarding failure: {error}"
        );
        assert_eq!(host.forwarded_exact_reads, 1);
        assert_eq!(host.quorum_waves, 0);
        assert_eq!(host.vote_requests, 0);
        assert_bounded_authority_busy_preparations(
            preparations_before,
            crate::raft_quorum::read_index_resident_preparations_for_root(&fixture.paths.root),
            "failed forwarding",
        );
        assert!(
            started.elapsed() < BOUNDED_AUTHORITY_BUSY_RETRY_BUDGET + Duration::from_millis(250),
            "failed forwarding retried the held local admission for {:?}",
            started.elapsed()
        );
        drop(held);
    }

    #[test]
    fn concurrent_mixed_warm_reads_share_one_outer_read_index_preparation_and_wave() {
        let fixture = cold_barrier_fixture("warm_mixed_read_index_herd");
        let mut storage =
            LocalClusterRaftCommandStorage::new(&fixture.paths, fixture.local.clone());
        let barrier = storage
            .append_uncommitted(KvCommandRequest::generated(KvCommand::Barrier))
            .expect("append warm-fixture current-term barrier");
        storage
            .commit_index_compact(barrier.index)
            .expect("commit warm-fixture current-term barrier");
        drop(storage);

        let preparations_before =
            crate::raft_quorum::read_index_resident_preparations_for_root(&fixture.paths.root);
        let last_index_before = fixture
            .paths
            .kv_paths()
            .raft_store()
            .locked_last_log_index(&fixture.local)
            .expect("warm fixture last index");
        let contended = Arc::new(ContendedExactSetReadIndexHostState::new());
        let mut point_state = fixture.state.clone();
        let mut exact_state = fixture.state.clone();
        let mut prefix_state = fixture.state.clone();
        let mut snapshot_state = fixture.state.clone();
        let mut point_host = ExactSetReadIndexHost {
            contended: Some(Arc::clone(&contended)),
            ..ExactSetReadIndexHost::default()
        };
        let mut exact_host = ExactSetReadIndexHost {
            contended: Some(Arc::clone(&contended)),
            ..ExactSetReadIndexHost::default()
        };
        let mut prefix_host = ExactSetReadIndexHost {
            contended: Some(Arc::clone(&contended)),
            ..ExactSetReadIndexHost::default()
        };
        let mut snapshot_host = ExactSetReadIndexHost {
            contended: Some(Arc::clone(&contended)),
            ..ExactSetReadIndexHost::default()
        };
        let options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let (point, exact, prefix, snapshot, ()) = runtime.block_on(async {
            // These are four independent production tasks. Heap-pin them here
            // instead of embedding all four state machines in one `join!`
            // future, which sums their debug-build frames on the 2 MiB Rust
            // test thread stack and does not model how the daemon schedules
            // concurrent requests.
            let point = Box::pin(read_cluster_kv_authoritative_entry_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut point_state,
                "/cold/a".to_string(),
                options,
                &mut point_host,
            ));
            let exact = Box::pin(read_cluster_kv_authoritative_entries_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut exact_state,
                vec!["/cold/b".to_string(), "/cold/missing".to_string()],
                options,
                &mut exact_host,
            ));
            let prefix = Box::pin(read_cluster_kv_authoritative_range_prefix_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut prefix_state,
                "/cold/".to_string(),
                options,
                &mut prefix_host,
            ));
            let snapshot = Box::pin(read_cluster_kv_authoritative_snapshot_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut snapshot_state,
                options,
                &mut snapshot_host,
            ));
            let release = async {
                tokio::time::timeout(
                    Duration::from_secs(1),
                    contended.pre_dispatch_entered.acquire(),
                )
                .await
                .expect("mixed warm owner did not reach pre-dispatch")
                .expect("mixed warm pre-dispatch semaphore")
                .forget();
                tokio::time::timeout(Duration::from_secs(1), async {
                    while crate::raft_quorum::read_index_singleflight_waiters_for_root(
                        &fixture.paths.root,
                    ) != 3
                    {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("all mixed read variants must join before dispatch");
                contended.pre_dispatch_release.add_permits(1);
                tokio::time::timeout(Duration::from_secs(1), contended.wave_entered.acquire())
                    .await
                    .expect("mixed warm ReadIndex wave did not dispatch")
                    .expect("mixed warm ReadIndex wave semaphore")
                    .forget();
                contended.wave_release.add_permits(1);
            };
            tokio::join!(point, exact, prefix, snapshot, release)
        });

        assert_eq!(
            point
                .expect("mixed point read")
                .value
                .expect("mixed point entry")
                .value,
            b"alpha"
        );
        assert_eq!(
            exact.expect("mixed exact-set read").value[0]
                .as_ref()
                .expect("mixed exact-set entry")
                .value,
            b"beta"
        );
        assert_eq!(prefix.expect("mixed prefix read").value.len(), 2);
        assert!(
            snapshot
                .expect("mixed snapshot read")
                .value
                .entries
                .iter()
                .any(|entry| entry.key == "/cold/a")
        );
        assert_eq!(
            contended.quorum_waves.load(Ordering::SeqCst),
            1,
            "point, exact-set, prefix, and snapshot must share one pre-dispatch quorum wave"
        );
        assert_eq!(
            crate::raft_quorum::read_index_resident_preparations_for_root(&fixture.paths.root),
            preparations_before.saturating_add(1),
            "the mixed herd must project config, leader, and ReadIndex in one outer preparation"
        );
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("last index after mixed warm herd"),
            last_index_before,
            "a warm mixed read herd must not append a barrier"
        );
        assert_eq!(
            point_host.object_rpc_stream_opens
                + exact_host.object_rpc_stream_opens
                + prefix_host.object_rpc_stream_opens
                + snapshot_host.object_rpc_stream_opens,
            0,
            "no warm read variant should enter the forwarding/snapshot-transfer route"
        );
    }

    #[test]
    fn forwarded_request_outcome_overlays_original_attempt_but_not_final_history() {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-request-outcome-overlay");
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let request = KvCommandRequest::generated(KvCommand::Barrier);
        sinomo_kv::mark_cluster_kv_request_rejected_before_commit(
            &paths.kv_paths(),
            &request,
            "original node rejected before forwarding",
        )
        .expect("record original-node attempt");
        let proof = KvAuthorityProof {
            term: 3,
            commit_index: 7,
            revision: KvRevision(5),
            basis: KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 2,
                required_quorum: 2,
            },
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let authority_only = runtime
            .block_on(
                overlay_node_local_request_attempt_after_authoritative_history(
                    runtime.handle(),
                    &paths,
                    &request.request_id,
                    sinomo_kv::ClusterKvRequestOutcomeRead {
                        authority: proof,
                        outcome: KvRequestOutcome::Unknown {
                            detail: "authority has no retained final record".to_string(),
                        },
                    },
                    false,
                    Instant::now() + Duration::from_secs(1),
                ),
            )
            .expect("authority-only request outcome");
        assert!(matches!(
            authority_only.outcome,
            KvRequestOutcome::Unknown { .. }
        ));

        let overlaid = runtime
            .block_on(
                overlay_node_local_request_attempt_after_authoritative_history(
                    runtime.handle(),
                    &paths,
                    &request.request_id,
                    sinomo_kv::ClusterKvRequestOutcomeRead {
                        authority: proof,
                        outcome: KvRequestOutcome::Unknown {
                            detail: "authority has no retained final record".to_string(),
                        },
                    },
                    true,
                    Instant::now() + Duration::from_secs(1),
                ),
            )
            .expect("overlay original attempt");
        assert!(matches!(
            overlaid.outcome,
            KvRequestOutcome::RejectedBeforeCommit { .. }
        ));

        let committed = sinomo_kv::KvCommittedRequestRecord {
            request_id: request.request_id.clone(),
            command_digest: request.digest().expect("request digest"),
            term: 3,
            log_index: 7,
            revision: KvRevision(5),
            txn_response: None,
        };
        let final_read = runtime
            .block_on(
                overlay_node_local_request_attempt_after_authoritative_history(
                    runtime.handle(),
                    &paths,
                    &request.request_id,
                    sinomo_kv::ClusterKvRequestOutcomeRead {
                        authority: proof,
                        outcome: KvRequestOutcome::Committed(committed.clone()),
                    },
                    true,
                    Instant::now() + Duration::from_secs(1),
                ),
            )
            .expect("preserve final history");
        assert_eq!(final_read.outcome, KvRequestOutcome::Committed(committed));
    }

    #[test]
    fn verified_non_leader_response_clears_only_the_matching_durable_leader() {
        assert_verified_non_leader_clears_matching_durable_leader(
            "sinomo-cluster-verified-non-leader-voter",
            three_voter_state(),
        );
    }

    #[test]
    fn non_voter_clears_matching_durable_leader_after_verified_rejection() {
        let mut state = three_voter_state();
        state.members[0].state = ClusterMemberState::NonVoting;
        assert!(!sinomo_kv::cluster_kv_raft_local_is_voter(&state));
        assert_verified_non_leader_clears_matching_durable_leader(
            "sinomo-cluster-verified-non-leader-non-voter",
            state,
        );
    }

    #[test]
    fn delayed_non_leader_rejection_does_not_clear_same_voter_in_newer_term() {
        let root =
            sinomo_testdir::TestDir::new("sinomo-cluster-verified-non-leader-same-node-aba-fence");
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let state = three_voter_state();
        crate::write_cluster_state(&paths, &state).expect("cluster state");
        crate::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("seed cluster raft");
        let remote = state.members[1].node_id.clone();
        let store = paths.kv_paths().raft_store();
        let metadata = store
            .local_vote_metadata(&state.local_node_id)
            .expect("local vote metadata");
        let first_term = metadata.current_term.saturating_add(1);
        assert!(
            crate::apply_cluster_raft_append(
                &paths,
                &state.local_node_id,
                None,
                sinomo_kv::RaftAppendEntriesRequest {
                    term: first_term,
                    leader_id: remote.clone(),
                    prev_log_index: metadata.last_log_index,
                    prev_log_term: metadata.last_log_term,
                    entries: Vec::new(),
                    leader_commit: metadata.last_log_index,
                },
            )
            .expect("apply remote leader heartbeat")
            .success
        );
        let stale_fence = try_capture_cluster_raft_voter_leader_fence(&paths, &state)
            .expect("capture production stale-leader fence");
        assert_eq!(stale_fence.leader_id, remote);
        assert_eq!(stale_fence.current_term, first_term);
        assert!(
            crate::apply_cluster_raft_append(
                &paths,
                &state.local_node_id,
                None,
                sinomo_kv::RaftAppendEntriesRequest {
                    term: first_term.saturating_add(1),
                    leader_id: remote.clone(),
                    prev_log_index: metadata.last_log_index,
                    prev_log_term: metadata.last_log_term,
                    entries: Vec::new(),
                    leader_commit: metadata.last_log_index,
                },
            )
            .expect("apply newer same-node leader heartbeat")
            .success
        );
        let rejection = NetworkError::VerifiedClusterNonLeader {
            node_id: remote.clone(),
            detail: "delayed authenticated non-leader response".to_string(),
        };

        assert!(
            !clear_verified_stale_cluster_raft_leader(
                &paths,
                &state,
                Some(&stale_fence),
                &rejection,
            )
            .expect("fenced stale-leader clear")
        );
        assert_eq!(
            store
                .known_voter_leader_fence(&state)
                .expect("newer leader fence"),
            Some(sinomo_kv::ClusterKvRaftLeaderFence {
                leader_id: remote,
                current_term: first_term.saturating_add(1),
            })
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn expired_async_stale_leader_clear_leaves_no_late_work_after_held_lock() {
        let root = sinomo_testdir::TestDir::new(
            "sinomo-cluster-async-stale-leader-clear-operation-deadline",
        );
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let state = three_voter_state();
        crate::write_cluster_state(&paths, &state).expect("cluster state");
        crate::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("seed cluster raft");
        let remote = state.members[1].node_id.clone();
        let store = paths.kv_paths().raft_store();
        let metadata = store
            .local_vote_metadata(&state.local_node_id)
            .expect("local vote metadata");
        assert!(
            crate::apply_cluster_raft_append(
                &paths,
                &state.local_node_id,
                None,
                sinomo_kv::RaftAppendEntriesRequest {
                    term: metadata.current_term.saturating_add(1),
                    leader_id: remote.clone(),
                    prev_log_index: metadata.last_log_index,
                    prev_log_term: metadata.last_log_term,
                    entries: Vec::new(),
                    leader_commit: metadata.last_log_index,
                },
            )
            .expect("remote leader heartbeat")
            .success
        );
        let error = NetworkError::VerifiedClusterNonLeader {
            node_id: remote.clone(),
            detail: "known voter rejected forwarding".to_string(),
        };
        let leader_fence = store
            .known_voter_leader_fence(&state)
            .expect("capture stale leader fence")
            .expect("stale leader fence");
        let operation_guard = store.lock_operation().expect("hold Raft operation lock");
        let started = Instant::now();
        clear_verified_stale_cluster_raft_leader_async(
            &tokio::runtime::Handle::current(),
            &paths,
            &state,
            Some(&leader_fence),
            &error,
            started + Duration::from_millis(50),
        )
        .await
        .expect_err("held operation lock must exhaust stale-leader clear admission");
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "deadline-capped clear must not wait on the unbounded operation lock"
        );

        // If the async wrapper had dropped a timed-out blocking task, it
        // would acquire this lock and clear the leader after release.
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(operation_guard);
        tokio::task::yield_now().await;
        assert_eq!(
            store
                .known_voter_leader_id(&state)
                .expect("known leader after expired clear"),
            Some(remote),
            "expired clear must leave no queued mutation that runs later"
        );
    }

    fn assert_verified_non_leader_clears_matching_durable_leader(
        test_name: &str,
        state: ClusterBootstrapState,
    ) {
        let root = sinomo_testdir::TestDir::new(test_name);
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        crate::write_cluster_state(&paths, &state).expect("cluster state");
        crate::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("seed cluster raft");
        let remote = state.members[1].node_id.clone();
        let other = state.members[2].node_id.clone();
        let store = paths.kv_paths().raft_store();
        let metadata = store
            .local_vote_metadata(&state.local_node_id)
            .expect("local vote metadata");
        let heartbeat = sinomo_kv::RaftAppendEntriesRequest {
            term: metadata.current_term.saturating_add(1),
            leader_id: remote.clone(),
            prev_log_index: metadata.last_log_index,
            prev_log_term: metadata.last_log_term,
            entries: Vec::new(),
            leader_commit: metadata.last_log_index,
        };
        assert!(
            crate::apply_cluster_raft_append(&paths, &state.local_node_id, None, heartbeat.clone())
                .expect("remote leader heartbeat")
                .success
        );
        let leader_fence = store
            .known_voter_leader_fence(&state)
            .expect("capture stale leader fence")
            .expect("stale leader fence");
        let matching_rejection = NetworkError::VerifiedClusterNonLeader {
            node_id: remote.clone(),
            detail: "known voter rejected forwarding".to_string(),
        };

        assert!(
            !clear_verified_stale_cluster_raft_leader(&paths, &state, None, &matching_rejection,)
                .expect("unfenced rejection cannot clear leader")
        );
        assert_eq!(
            store.known_voter_leader_id(&state).expect("known leader"),
            Some(remote.clone())
        );

        assert!(
            !clear_verified_stale_cluster_raft_leader(
                &paths,
                &state,
                Some(&leader_fence),
                &NetworkError::VerifiedClusterNonLeader {
                    node_id: other,
                    detail: "different voter rejected forwarding".to_string(),
                },
            )
            .expect("preserve nonmatching leader")
        );
        assert_eq!(
            store.known_voter_leader_id(&state).expect("known leader"),
            Some(remote.clone())
        );

        assert!(
            clear_verified_stale_cluster_raft_leader(
                &paths,
                &state,
                Some(&leader_fence),
                &matching_rejection,
            )
            .expect("clear matching leader")
        );
        assert_eq!(
            store
                .known_voter_leader_id(&state)
                .expect("known leader after clear"),
            None
        );
    }

    #[derive(Default)]
    struct GrantingLeaderRecoveryHost {
        vote_calls: usize,
    }

    #[derive(Default)]
    struct ExhaustedNonLeaderRecoveryHost {
        forwarded_targets: Vec<NodeId>,
        vote_batches: usize,
        barrier_attempts: usize,
    }

    impl ClusterKvTxnQuorumHost for ExhaustedNonLeaderRecoveryHost {
        fn known_kv_state_authority_id(
            &mut self,
            paths: &ClusterPaths,
            state: &ClusterBootstrapState,
        ) -> Result<Option<NodeId>, NetworkError> {
            locked_kv_state_authority_id(paths, state)
        }

        fn call_node_object_rpc(
            &mut self,
            _state: &ClusterBootstrapState,
            target_node_id: &NodeId,
            _request: &ObjectRpcRequest,
            _timeout: Duration,
        ) -> Result<ObjectRpcResponse, NetworkError> {
            self.forwarded_targets.push(target_node_id.clone());
            Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "{FORWARDED_CLUSTER_KV_READ_NON_LEADER_DETAIL}; forwarded requests do not start elections"
                ),
            })
        }

        fn local_is_kv_state_authority(
            &mut self,
            paths: &ClusterPaths,
            state: &ClusterBootstrapState,
        ) -> Result<bool, NetworkError> {
            locked_cluster_raft_local_is_leader(paths, state)
        }

        fn request_kv_state_authority_vote(
            &mut self,
            state: &ClusterBootstrapState,
            _target_node_id: &NodeId,
            request: &ClusterKvStateAuthorityVoteRequest,
            _timeout: Duration,
        ) -> Result<ClusterKvStateAuthorityVoteResponse, NetworkError> {
            Ok(ClusterKvStateAuthorityVoteResponse {
                accepted: true,
                detail: "granted after exhausting non-leader voters".to_string(),
                state: state.clone(),
                vote: sinomo_kv::RaftRequestVoteResponse {
                    term: request.vote.term,
                    vote_granted: true,
                },
            })
        }

        fn request_kv_state_authority_votes(
            &mut self,
            state: &ClusterBootstrapState,
            targets: Vec<NodeId>,
            request: &ClusterKvStateAuthorityVoteRequest,
            _timeout: Duration,
            required_grants: usize,
        ) -> Result<
            Vec<(
                NodeId,
                Result<ClusterKvStateAuthorityVoteResponse, NetworkError>,
            )>,
            NetworkError,
        > {
            self.vote_batches = self.vote_batches.saturating_add(1);
            Ok(targets
                .into_iter()
                .take(required_grants)
                .map(|target| {
                    (
                        target,
                        Ok(ClusterKvStateAuthorityVoteResponse {
                            accepted: true,
                            detail: "granted after exhausting non-leader voters".to_string(),
                            state: state.clone(),
                            vote: sinomo_kv::RaftRequestVoteResponse {
                                term: request.vote.term,
                                vote_granted: true,
                            },
                        }),
                    )
                })
                .collect())
        }

        fn reachable_cluster_state_authority_count(
            &mut self,
            _paths: &ClusterPaths,
            state: &ClusterBootstrapState,
            _options: ClusterStateAuthorityReachabilityOptions,
        ) -> usize {
            sinomo_kv::cluster_kv_raft_voter_ids(state)
                .map(|voters| voters.len())
                .unwrap_or_default()
        }

        fn commit_cluster_kv_command_with_quorum(
            &mut self,
            _paths: &ClusterPaths,
            _state: &mut ClusterBootstrapState,
            _request: KvCommandRequest,
            _options: ClusterKvRaftQuorumCommitOptions,
        ) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError> {
            self.barrier_attempts = self.barrier_attempts.saturating_add(1);
            Err(NetworkError::ClusterReadOnly {
                detail: "test stopped after recovered leader entered barrier commit".to_string(),
            })
        }
    }

    #[test]
    fn exhausted_authenticated_non_leaders_clear_stale_hint_and_reach_local_campaign() {
        let root = sinomo_testdir::TestDir::new(
            "sinomo-cluster-exhausted-non-leaders-recover-local-campaign",
        );
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let mut state = three_voter_state();
        crate::write_cluster_state(&paths, &state).expect("cluster state");
        crate::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("seed cluster raft");
        let stale_leader = state.members[1].node_id.clone();
        let store = paths.kv_paths().raft_store();
        let metadata = store
            .local_vote_metadata(&state.local_node_id)
            .expect("local vote metadata");
        assert!(
            crate::apply_cluster_raft_append(
                &paths,
                &state.local_node_id,
                None,
                sinomo_kv::RaftAppendEntriesRequest {
                    term: metadata.current_term.saturating_add(1),
                    leader_id: stale_leader.clone(),
                    prev_log_index: metadata.last_log_index,
                    prev_log_term: metadata.last_log_term,
                    entries: Vec::new(),
                    leader_commit: metadata.last_log_index,
                },
            )
            .expect("apply stale remote leader heartbeat")
            .success
        );
        let stale_fence = try_capture_cluster_raft_voter_leader_fence(&paths, &state)
            .expect("capture stale leader generation");
        assert_eq!(stale_fence.leader_id, stale_leader);
        let term_before = stale_fence.current_term;

        let mut options = sync_authoritative_options(Duration::from_secs(3));
        options.allow_forwarding = true;
        options.leader_election_options.campaign_jitter_max = Duration::ZERO;
        options.leader_election_options.timeout = Duration::from_secs(3);
        options.forward_options.timeout = Duration::from_secs(3);
        let mut host = ExhaustedNonLeaderRecoveryHost::default();
        let error = read_cluster_kv_authoritative_snapshot_with_host(
            &paths, &mut state, options, &mut host,
        )
        .expect_err("fixture stops after recovered leader enters barrier commit");

        assert!(matches!(
            error,
            NetworkError::ClusterReadOnly { detail }
                if detail.contains("stopped after recovered leader entered barrier commit")
        ));
        assert_eq!(
            host.forwarded_targets.len(),
            4,
            "the first pass and the post-clear retry must each exhaust both exact remote voters"
        );
        assert_eq!(
            host.vote_batches, 1,
            "local campaign must request quorum votes"
        );
        assert_eq!(
            host.barrier_attempts, 1,
            "successful election must enter the authoritative barrier path"
        );
        let recovered = store
            .known_voter_leader_fence(&state)
            .expect("read recovered leader")
            .expect("recovered leader fence");
        assert_eq!(recovered.leader_id, state.local_node_id);
        assert!(
            recovered.current_term > term_before,
            "self-vote must advance the durable term"
        );
    }

    impl crate::raft_election::ClusterRaftVoteTransportHost for GrantingLeaderRecoveryHost {
        fn send_raft_vote(
            &mut self,
            _state: &ClusterBootstrapState,
            _target_node_id: &NodeId,
            request: &sinomo_kv::ClusterRaftVoteRequest,
            _timeout: Duration,
        ) -> Result<sinomo_kv::ClusterRaftVoteResponse, NetworkError> {
            self.vote_calls = self.vote_calls.saturating_add(1);
            Ok(sinomo_kv::ClusterRaftVoteResponse {
                accepted: true,
                detail: "granted after authenticated non-leader rejection".to_string(),
                state: request.state.clone(),
                vote: sinomo_kv::RaftRequestVoteResponse {
                    term: request.vote.term,
                    vote_granted: true,
                },
            })
        }

        fn send_raft_votes(
            &mut self,
            _state: &ClusterBootstrapState,
            targets: Vec<NodeId>,
            request: &sinomo_kv::ClusterRaftVoteRequest,
            _timeout: Duration,
            required_grants: usize,
        ) -> Result<
            Vec<(
                NodeId,
                Result<sinomo_kv::ClusterRaftVoteResponse, NetworkError>,
            )>,
            NetworkError,
        > {
            Ok(targets
                .into_iter()
                .take(required_grants)
                .map(|target| {
                    self.vote_calls = self.vote_calls.saturating_add(1);
                    (
                        target,
                        Ok(sinomo_kv::ClusterRaftVoteResponse {
                            accepted: true,
                            detail: "granted after authenticated non-leader rejection".to_string(),
                            state: request.state.clone(),
                            vote: sinomo_kv::RaftRequestVoteResponse {
                                term: request.vote.term,
                                vote_granted: true,
                            },
                        }),
                    )
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn async_authenticated_non_leader_clears_stale_leader_and_allows_recovery() {
        let root =
            sinomo_testdir::TestDir::new("sinomo-cluster-async-authenticated-non-leader-recovery");
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let mut state = three_voter_state();
        state.members[2].state = ClusterMemberState::NonVoting;
        let remote = state.members[1].node_id.clone();
        state.members[1]
            .endpoints
            .push(sinomo_network::daemon_lan_endpoint(
                &remote,
                "10.0.0.42:21353".to_string(),
                0,
            ));
        crate::write_cluster_state(&paths, &state).expect("cluster state");
        crate::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("seed cluster raft");

        let store = paths.kv_paths().raft_store();
        let metadata = store
            .local_vote_metadata(&state.local_node_id)
            .expect("local vote metadata");
        assert!(
            crate::apply_cluster_raft_append(
                &paths,
                &state.local_node_id,
                None,
                sinomo_kv::RaftAppendEntriesRequest {
                    term: metadata.current_term.saturating_add(1),
                    leader_id: remote.clone(),
                    prev_log_index: metadata.last_log_index,
                    prev_log_term: metadata.last_log_term,
                    entries: Vec::new(),
                    leader_commit: metadata.last_log_index,
                },
            )
            .expect("remote leader heartbeat")
            .success
        );

        let error = crate::kv_forward::await_authenticated_cluster_kv_target(
            &remote,
            std::future::ready(Err::<(), _>(NetworkError::ClusterReadOnly {
                detail: "forwarded cluster KV transaction reached a non-leader voter; forwarded requests do not start elections".to_string(),
            })),
        )
        .await
        .expect_err("remote rejects stale leader identity");
        assert!(matches!(
            &error,
            NetworkError::VerifiedClusterNonLeader { node_id, .. } if node_id == &remote
        ));
        let leader_fence = store
            .known_voter_leader_fence(&state)
            .expect("capture stale leader fence")
            .expect("stale leader fence");
        assert!(
            clear_verified_stale_cluster_raft_leader(&paths, &state, Some(&leader_fence), &error,)
                .expect("clear authenticated stale leader")
        );
        assert_eq!(
            store.known_voter_leader_id(&state).expect("cleared leader"),
            None
        );

        let mut host = GrantingLeaderRecoveryHost::default();
        assert!(
            elect_cluster_raft_leader_with_host(
                &paths,
                &state,
                crate::control_plane_options::ClusterRaftLeaderElectionOptions {
                    kv: sinomo_kv::ClusterKvRaftLeaderElectionOptions { max_attempts: 1 },
                    timeout: Duration::from_secs(1),
                    campaign_jitter_max: Duration::ZERO,
                    tcp_command_timeout: Duration::from_secs(1),
                    iroh_command_timeout: Duration::from_secs(1),
                    tcp_foreground_attempt_timeout: Duration::from_secs(1),
                    public_foreground_attempt_timeout: Duration::from_secs(1),
                    iroh_foreground_attempt_timeout: Duration::from_secs(1),
                },
                &mut host,
            )
            .expect("recover local raft leadership")
        );
        assert_eq!(host.vote_calls, 1);
        assert_eq!(
            store
                .known_voter_leader_id(&state)
                .expect("recovered leader"),
            Some(state.local_node_id.clone())
        );
    }

    fn three_voter_state() -> ClusterBootstrapState {
        let now = OffsetDateTime::UNIX_EPOCH;
        let cluster_id = ClusterId::from_legacy_named("cluster_authority_proof").expect("cluster");
        let local = NodeId::from_legacy_named("node_authority_proof_local").expect("local");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "authority proof",
            local,
            "local",
            "pk-local",
            now,
        );
        for (node_id, name) in [
            (state.local_node_id.clone(), "local"),
            (
                NodeId::from_legacy_named("node_authority_proof_second").expect("second"),
                "second",
            ),
            (
                NodeId::from_legacy_named("node_authority_proof_third").expect("third"),
                "third",
            ),
        ] {
            let mut member = ClusterMemberRecord::local_voter(
                cluster_id.clone(),
                node_id,
                name,
                format!("pk-{name}"),
                now,
            );
            member.capabilities = sinomo_node::main_daemon_capabilities(&member.role);
            member.allowed_capabilities = member.capabilities.clone();
            if name == "local" {
                state.members[0] = member;
            } else {
                state.members.push(member);
            }
        }
        state
    }

    fn authoritative_read(
        snapshot_revision: KvRevision,
        proof_revision: KvRevision,
        voter_acks: usize,
        required_quorum: usize,
    ) -> KvAuthoritativeRead<KvSnapshot> {
        let mut snapshot = InProcessKvStore::new().try_snapshot().expect("snapshot");
        snapshot.revision = snapshot_revision;
        KvAuthoritativeRead {
            value: snapshot,
            authority: KvAuthorityProof {
                term: 3,
                commit_index: 7,
                revision: proof_revision,
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks,
                    required_quorum,
                },
            },
        }
    }

    #[test]
    fn authoritative_proof_requires_real_membership_quorum_and_matching_revision() {
        let mut state = three_voter_state();
        state.term = 3;
        state.kv_revision = 9;
        let revision = KvRevision(9);
        assert!(
            validate_forwarded_authoritative_read(
                authoritative_read(revision, revision, 2, 2),
                &state,
            )
            .is_ok()
        );
        assert!(
            validate_forwarded_authoritative_read(
                authoritative_read(revision, revision, 1, 2),
                &state,
            )
            .is_err(),
            "fewer acknowledgements than quorum must be rejected"
        );
        assert!(
            validate_forwarded_authoritative_read(
                authoritative_read(revision, revision, 2, 1),
                &state,
            )
            .is_err(),
            "a proof cannot claim a smaller quorum than committed membership"
        );
        assert!(
            validate_forwarded_authoritative_read(
                authoritative_read(revision, KvRevision(10), 2, 2),
                &state,
            )
            .is_err(),
            "proof and snapshot revisions must identify the same committed view"
        );

        let stale_revision = validate_forwarded_authoritative_read(
            authoritative_read(KvRevision(8), KvRevision(8), 2, 2),
            &state,
        )
        .expect_err("a sufficient-term snapshot below the revision floor must fail");
        assert!(
            stale_revision
                .to_string()
                .contains("revision 8 is older than locally observed revision 9"),
            "unexpected stale-revision snapshot error: {stale_revision}"
        );

        let mut stale_term = authoritative_read(revision, revision, 2, 2);
        stale_term.authority.term = 2;
        let stale_term = validate_forwarded_authoritative_read(stale_term, &state)
            .expect_err("a sufficient-revision snapshot below the term floor must fail");
        assert!(
            stale_term
                .to_string()
                .contains("term 2 is older than locally observed term 3"),
            "unexpected stale-term snapshot error: {stale_term}"
        );
    }

    #[test]
    fn mixed_version_exact_key_set_snapshot_fallback_gate_is_strict() {
        assert!(exact_key_set_route_is_definitely_unsupported(
            &NetworkError::UnknownRoute {
                route: sinomo_kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE.to_string(),
            }
        ));
        assert!(!exact_key_set_route_is_definitely_unsupported(
            &NetworkError::UnknownRoute {
                route: sinomo_kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE.to_string(),
            }
        ));
        assert!(!exact_key_set_route_is_definitely_unsupported(
            &NetworkError::ObjectRpc {
                route: sinomo_kv::CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE.to_string(),
                detail: "unknown service route".to_string(),
            }
        ));
        assert!(!exact_key_set_route_is_definitely_unsupported(
            &NetworkError::timeout(
                "mixed-version exact-key set",
                sinomo_network::NetworkTimeoutPhase::Overall,
                Duration::from_secs(1),
                sinomo_network::NetworkDeliveryState::DefinitelyNotSent,
            )
        ));
    }

    #[test]
    fn mixed_version_exact_key_set_snapshot_fallback_enforces_composed_scalar_floors() {
        let mut state = three_voter_state();
        state.term = 3;
        state.kv_revision = 9;
        let keys = vec!["/mixed-version/missing".to_string()];

        let read = project_mixed_version_authoritative_entries_snapshot(
            authoritative_read(KvRevision(9), KvRevision(9), 2, 2),
            &state,
            &keys,
        )
        .expect("fallback snapshot at both scalar floors");
        assert_eq!(read.value, vec![None]);

        let stale_revision = project_mixed_version_authoritative_entries_snapshot(
            authoritative_read(KvRevision(8), KvRevision(8), 2, 2),
            &state,
            &keys,
        )
        .expect_err("mixed-version fallback must reject a stale revision");
        assert!(
            stale_revision
                .to_string()
                .contains("revision 8 is older than locally observed revision 9"),
            "unexpected mixed-version stale-revision error: {stale_revision}"
        );

        let mut stale_term = authoritative_read(KvRevision(9), KvRevision(9), 2, 2);
        stale_term.authority.term = 2;
        let stale_term =
            project_mixed_version_authoritative_entries_snapshot(stale_term, &state, &keys)
                .expect_err("mixed-version fallback must reject a stale term");
        assert!(
            stale_term
                .to_string()
                .contains("term 2 is older than locally observed term 3"),
            "unexpected mixed-version stale-term error: {stale_term}"
        );
    }

    #[test]
    fn synchronous_authoritative_snapshot_uses_one_cold_barrier_then_warm_read_index() {
        let mut fixture = cold_barrier_fixture("sync_authoritative_warm_read_index");
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
        let shared = Arc::new(SyncAuthoritativeHostState::new(false, false));
        let mut host = SyncAuthoritativeHost {
            shared: Arc::clone(&shared),
        };
        let options = sync_authoritative_options(Duration::from_secs(3));

        let first = read_cluster_kv_authoritative_snapshot_with_host(
            &fixture.paths,
            &mut fixture.state,
            options,
            &mut host,
        )
        .expect("first synchronous authoritative read");
        assert_eq!(
            first
                .value
                .get("/cold/a")
                .map(|entry| entry.value.as_slice()),
            Some(b"alpha".as_slice())
        );
        for _ in 0..8 {
            let read = read_cluster_kv_authoritative_snapshot_with_host(
                &fixture.paths,
                &mut fixture.state,
                options,
                &mut host,
            )
            .expect("warm synchronous authoritative read");
            assert_eq!(read.value.revision, first.value.revision);
        }

        assert_eq!(shared.barrier_commits.load(Ordering::SeqCst), 1);
        assert!(
            shared.read_index_batches.load(Ordering::SeqCst) >= 8,
            "every warm read must use ReadIndex"
        );
        let last_index = fixture
            .paths
            .kv_paths()
            .raft_store()
            .open_node(&fixture.local)
            .expect("open warm sync node")
            .last_log_index();
        assert_eq!(last_index, fixture.initial_last_index + 1);
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
    }

    #[test]
    fn synchronous_commit_projects_durable_term_before_leader_validation() {
        let fixture = cold_barrier_fixture("sync_commit_durable_term_projection");
        let shared = Arc::new(SyncAuthoritativeHostState::new(false, false));
        let mut host = SyncAuthoritativeHost {
            shared: Arc::clone(&shared),
        };
        let mut stale_state = fixture.state.clone();
        stale_state.term = 0;
        stale_state.kv_revision = 0;

        let result = commit_cluster_kv_txn_with_quorum_response_with_host(
            &fixture.paths,
            &mut stale_state,
            KvTxnRequest::generated(sinomo_kv::KvTxn {
                compares: Vec::new(),
                success: vec![sinomo_kv::KvOp::Put {
                    key: "/sync/durable-term".to_string(),
                    value: b"committed".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            }),
            sync_authoritative_options(Duration::from_secs(3)),
            &mut host,
        )
        .expect("stale bootstrap term must adopt the resident authority floor");

        assert_eq!(stale_state.term, 7);
        assert!(stale_state.kv_revision >= result.snapshot.revision.0);
        assert_eq!(
            result
                .snapshot
                .get("/sync/durable-term")
                .map(|entry| entry.value.as_slice()),
            Some(b"committed".as_slice())
        );
        assert_eq!(shared.barrier_commits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn concurrent_synchronous_cold_reads_share_exactly_one_barrier() {
        let fixture = cold_barrier_fixture("sync_authoritative_concurrent_cold");
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
        let shared = Arc::new(SyncAuthoritativeHostState::new(true, false));
        let paths = fixture.paths.clone();
        let state = fixture.state.clone();
        let options = sync_authoritative_options(Duration::from_secs(3));
        let owner_shared = Arc::clone(&shared);
        let owner = std::thread::spawn(move || {
            let mut state = state;
            let mut host = SyncAuthoritativeHost {
                shared: owner_shared,
            };
            read_cluster_kv_authoritative_snapshot_with_host(&paths, &mut state, options, &mut host)
        });
        shared.wait_for_barrier_entry();

        let mut waiters = Vec::new();
        for _ in 0..3 {
            let paths = fixture.paths.clone();
            let mut state = fixture.state.clone();
            let shared = Arc::clone(&shared);
            waiters.push(std::thread::spawn(move || {
                let mut host = SyncAuthoritativeHost { shared };
                read_cluster_kv_authoritative_snapshot_with_host(
                    &paths, &mut state, options, &mut host,
                )
            }));
        }
        let wait_deadline = Instant::now() + Duration::from_secs(2);
        while authoritative_barrier_singleflight_waiters_for_root(&fixture.paths.root) < 3 {
            assert!(
                Instant::now() < wait_deadline,
                "cold sync waiters did not join"
            );
            std::thread::yield_now();
        }
        shared.release_barrier();

        owner
            .join()
            .expect("join cold sync owner")
            .expect("cold sync owner read");
        for waiter in waiters {
            waiter
                .join()
                .expect("join cold sync waiter")
                .expect("cold sync waiter read");
        }
        assert_eq!(shared.barrier_commits.load(Ordering::SeqCst), 1);
        let last_index = fixture
            .paths
            .kv_paths()
            .raft_store()
            .open_node(&fixture.local)
            .expect("open concurrent sync node")
            .last_log_index();
        assert_eq!(last_index, fixture.initial_last_index + 1);
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
    }

    #[test]
    fn synchronous_read_index_does_not_wait_for_blocked_first_voter() {
        let mut fixture = cold_barrier_fixture("sync_read_index_concurrent_probe");
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
        let initial_shared = Arc::new(SyncAuthoritativeHostState::new(false, false));
        let mut initial_host = SyncAuthoritativeHost {
            shared: initial_shared,
        };
        let options = sync_authoritative_options(Duration::from_secs(3));
        read_cluster_kv_authoritative_snapshot_with_host(
            &fixture.paths,
            &mut fixture.state,
            options,
            &mut initial_host,
        )
        .expect("establish current-term commit");

        let shared = Arc::new(SyncAuthoritativeHostState::new(false, true));
        let mut host = SyncAuthoritativeHost {
            shared: Arc::clone(&shared),
        };
        let started = Instant::now();
        read_cluster_kv_authoritative_snapshot_with_host(
            &fixture.paths,
            &mut fixture.state,
            options,
            &mut host,
        )
        .expect("healthy later voter confirms synchronous ReadIndex");
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "blocked first voter must be cancelled after another voter establishes quorum"
        );
        assert_eq!(shared.barrier_commits.load(Ordering::SeqCst), 0);
        assert_eq!(shared.read_index_batches.load(Ordering::SeqCst), 1);
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
    }

    #[test]
    fn synchronous_cold_read_adopts_durable_pending_barrier_after_registry_restart() {
        let mut fixture = cold_barrier_fixture("sync_authoritative_restart_adoption");
        let pending = KvCommandRequest::generated(KvCommand::Barrier);
        LocalClusterRaftCommandStorage::new(&fixture.paths, fixture.local.clone())
            .append_uncommitted(pending.clone())
            .expect("append durable pending barrier");
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
        let shared = Arc::new(SyncAuthoritativeHostState::new(false, false));
        let mut host = SyncAuthoritativeHost {
            shared: Arc::clone(&shared),
        };
        read_cluster_kv_authoritative_snapshot_with_host(
            &fixture.paths,
            &mut fixture.state,
            sync_authoritative_options(Duration::from_secs(3)),
            &mut host,
        )
        .expect("adopt durable synchronous barrier");
        assert_eq!(shared.barrier_commits.load(Ordering::SeqCst), 1);
        assert_eq!(
            shared
                .submitted_barrier_ids
                .lock()
                .expect("submitted ids")
                .as_slice(),
            &[pending.request_id]
        );
        let last_index = fixture
            .paths
            .kv_paths()
            .raft_store()
            .open_node(&fixture.local)
            .expect("open restart adoption node")
            .last_log_index();
        assert_eq!(last_index, fixture.initial_last_index + 1);
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
    }

    #[test]
    fn stale_synchronous_cold_reader_cannot_append_in_newer_term() {
        let mut fixture = cold_barrier_fixture("sync_authoritative_stale_term");
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
        let shared = Arc::new(SyncAuthoritativeHostState::new(false, false));
        shared
            .advance_term_before_commit
            .store(true, Ordering::SeqCst);
        let mut host = SyncAuthoritativeHost {
            shared: Arc::clone(&shared),
        };
        read_cluster_kv_authoritative_snapshot_with_host(
            &fixture.paths,
            &mut fixture.state,
            sync_authoritative_options(Duration::from_secs(3)),
            &mut host,
        )
        .expect_err("stale synchronous reader must lose authority");
        let node = fixture
            .paths
            .kv_paths()
            .raft_store()
            .open_node(&fixture.local)
            .expect("open stale-term node");
        assert_eq!(node.hard_state().current_term, 8);
        assert_eq!(node.last_log_index(), fixture.initial_last_index);
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
    }

    #[test]
    fn synchronous_authoritative_read_lock_timeout_leaves_no_late_work() {
        let mut fixture = cold_barrier_fixture("sync_authoritative_lock_deadline");
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
        let store = fixture.paths.kv_paths().raft_store();
        let operation_guard = store.lock_operation().expect("hold Raft operation lock");
        let shared = Arc::new(SyncAuthoritativeHostState::new(false, false));
        let mut host = SyncAuthoritativeHost {
            shared: Arc::clone(&shared),
        };
        let started = Instant::now();
        read_cluster_kv_authoritative_snapshot_with_host(
            &fixture.paths,
            &mut fixture.state,
            sync_authoritative_options(Duration::from_millis(50)),
            &mut host,
        )
        .expect_err("held operation lock must exhaust sync authority deadline");
        assert!(started.elapsed() < Duration::from_secs(1));
        std::thread::sleep(Duration::from_millis(100));
        drop(operation_guard);
        assert_eq!(shared.barrier_commits.load(Ordering::SeqCst), 0);
        let node = store
            .open_node(&fixture.local)
            .expect("open node after expired sync authority waiter");
        assert_eq!(node.last_log_index(), fixture.initial_last_index);
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn concurrent_mixed_cold_authoritative_reads_share_one_barrier_then_read_index() {
        let fixture = cold_barrier_fixture("cold_barrier_mixed_reads");
        let shared = Arc::new(ColdBarrierHostState::new(true, false, false));
        let options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);

        let mut tasks = Vec::new();
        {
            let paths = fixture.paths.clone();
            let mut state = fixture.state.clone();
            let shared = Arc::clone(&shared);
            tasks.push(tokio::spawn(async move {
                let mut host = ColdBarrierHost { shared };
                let read = read_cluster_kv_authoritative_entry_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &paths,
                    &mut state,
                    "/cold/a".to_string(),
                    options,
                    &mut host,
                )
                .await?;
                Ok::<_, NetworkError>((read.value.expect("cold point entry").value, read.authority))
            }));
        }
        {
            let paths = fixture.paths.clone();
            let mut state = fixture.state.clone();
            let shared = Arc::clone(&shared);
            tasks.push(tokio::spawn(async move {
                let mut host = ColdBarrierHost { shared };
                let read = read_cluster_kv_authoritative_entries_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &paths,
                    &mut state,
                    vec!["/cold/b".to_string(), "/cold/missing".to_string()],
                    options,
                    &mut host,
                )
                .await?;
                Ok::<_, NetworkError>((
                    read.value[0]
                        .as_ref()
                        .expect("cold exact entry")
                        .value
                        .clone(),
                    read.authority,
                ))
            }));
        }
        {
            let paths = fixture.paths.clone();
            let mut state = fixture.state.clone();
            let shared = Arc::clone(&shared);
            tasks.push(tokio::spawn(async move {
                let mut host = ColdBarrierHost { shared };
                let read = read_cluster_kv_authoritative_range_prefix_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &paths,
                    &mut state,
                    "/cold/".to_string(),
                    options,
                    &mut host,
                )
                .await?;
                Ok::<_, NetworkError>((
                    read.value
                        .iter()
                        .find(|entry| entry.key == "/cold/a")
                        .expect("cold prefix entry")
                        .value
                        .clone(),
                    read.authority,
                ))
            }));
        }
        {
            let paths = fixture.paths.clone();
            let mut state = fixture.state.clone();
            let shared = Arc::clone(&shared);
            tasks.push(tokio::spawn(async move {
                let mut host = ColdBarrierHost { shared };
                let read = read_cluster_kv_authoritative_snapshot_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &paths,
                    &mut state,
                    options,
                    &mut host,
                )
                .await?;
                Ok::<_, NetworkError>((
                    read.value
                        .entries
                        .iter()
                        .find(|entry| entry.key == "/cold/b")
                        .expect("cold snapshot entry")
                        .value
                        .clone(),
                    read.authority,
                ))
            }));
        }

        tokio::time::timeout(
            Duration::from_secs(2),
            shared.first_barrier_entered.acquire(),
        )
        .await
        .expect("one cold barrier entered")
        .expect("cold barrier entry semaphore")
        .forget();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if authoritative_barrier_singleflight_waiters_for_root(&fixture.paths.root) >= 3 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all other cold readers joined the barrier flight");
        shared.first_barrier_release.add_permits(1);

        let mut values = Vec::new();
        for task in tasks {
            let (value, proof) = task
                .await
                .expect("join cold authoritative read")
                .expect("cold authoritative read");
            assert!(matches!(
                proof.basis,
                KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                }
            ));
            values.push(value);
        }
        values.sort();
        assert_eq!(
            values,
            vec![
                b"alpha".to_vec(),
                b"alpha".to_vec(),
                b"beta".to_vec(),
                b"beta".to_vec(),
            ]
        );
        assert_eq!(shared.barrier_waves.load(Ordering::SeqCst), 1);
        let batch_read_index_waves = shared.read_index_waves.load(Ordering::SeqCst);
        assert!(
            (1..=3).contains(&batch_read_index_waves),
            "every waiter must use ReadIndex; overlapping quorum waves may coalesce"
        );
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("last index after cold read batch"),
            fixture.initial_last_index + 1,
            "the cold batch must append exactly one barrier"
        );

        let mut later_state = fixture.state.clone();
        let mut later_host = ColdBarrierHost {
            shared: Arc::clone(&shared),
        };
        let later = read_cluster_kv_authoritative_entry_async_with_host(
            &tokio::runtime::Handle::current(),
            &fixture.paths,
            &mut later_state,
            "/cold/a".to_string(),
            options,
            &mut later_host,
        )
        .await
        .expect("later warm authoritative read");
        assert_eq!(later.value.expect("later point entry").value, b"alpha");
        assert_eq!(shared.barrier_waves.load(Ordering::SeqCst), 1);
        assert_eq!(
            shared.read_index_waves.load(Ordering::SeqCst),
            batch_read_index_waves + 1,
            "a later warm read must use ReadIndex without another barrier"
        );
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn admitted_compact_barrier_snapshot_releases_resident_node_before_caller_returns() {
        let fixture = cold_barrier_fixture("compact_barrier_snapshot_ownership");
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
        let shared = Arc::new(ColdBarrierHostState::new(false, false, false));
        let options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        let paths = fixture.paths.clone();
        let local = fixture.local.clone();
        let root = fixture.paths.root.clone();
        let mut state = fixture.state.clone();
        let mut host = ColdBarrierHost {
            shared: Arc::clone(&shared),
        };
        let (projection_entered_tx, projection_entered_rx) = std::sync::mpsc::sync_channel(1);
        let (projection_release_tx, projection_release_rx) = std::sync::mpsc::sync_channel(0);

        let read_paths = paths.clone();
        let read = tokio::spawn(async move {
            commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence_and_projection_policy(
                &tokio::runtime::Handle::current(),
                &read_paths,
                &mut state,
                options,
                Instant::now() + Duration::from_millis(100),
                None,
                ClusterRaftReadProjectionPolicy::CompleteAdmittedSnapshot,
                move |storage| {
                    projection_entered_tx
                        .send(())
                        .expect("signal admitted compact-barrier snapshot");
                    projection_release_rx
                        .recv()
                        .expect("release admitted compact-barrier snapshot");
                    storage.read_index_snapshot()
                },
                &mut host,
            )
            .await
        });

        tokio::task::spawn_blocking(move || projection_entered_rx.recv())
            .await
            .expect("join compact-barrier projection-entry waiter")
            .expect("compact-barrier snapshot acquired resident ownership");
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !read.is_finished(),
            "compact-barrier caller must await admitted snapshot work"
        );
        projection_release_tx
            .send(())
            .expect("release compact-barrier snapshot");
        let read = read
            .await
            .expect("join compact-barrier snapshot caller")
            .expect("admitted compact-barrier snapshot result");
        assert_eq!(read.value.revision, read.authority.revision);

        let diagnostic_paths = paths.clone();
        tokio::time::timeout(
            Duration::from_millis(250),
            tokio::task::spawn_blocking(move || {
                LocalClusterRaftCommandStorage::new_read_index_with_operation_deadline(
                    &diagnostic_paths,
                    local,
                    Instant::now() + Duration::from_millis(200),
                )
                .read_index_state()
            }),
        )
        .await
        .expect("compact-barrier projection must release resident ownership before return")
        .expect("join compact-barrier resident probe")
        .expect("compact-barrier resident probe");
        assert_eq!(shared.barrier_waves.load(Ordering::SeqCst), 1);
        clear_authoritative_barrier_singleflight_for_root(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_admitted_compact_barrier_projection_is_supervised_to_release() {
        let fixture = cold_barrier_fixture("compact_barrier_cancelled_projection");
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
        let shared = Arc::new(ColdBarrierHostState::new(false, false, false));
        let options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);
        let paths = fixture.paths.clone();
        let local = fixture.local.clone();
        let root = fixture.paths.root.clone();
        let mut state = fixture.state.clone();
        let mut host = ColdBarrierHost {
            shared: Arc::clone(&shared),
        };
        let (projection_entered_tx, projection_entered_rx) = std::sync::mpsc::sync_channel(1);
        let (projection_release_tx, projection_release_rx) = std::sync::mpsc::sync_channel(0);
        let (projection_completed_tx, projection_completed_rx) = std::sync::mpsc::sync_channel(1);

        let read_paths = paths.clone();
        let read = tokio::spawn(async move {
            commit_cluster_kv_authoritative_barrier_then_read_with_initial_fence_and_projection_policy(
                &tokio::runtime::Handle::current(),
                &read_paths,
                &mut state,
                options,
                Instant::now() + Duration::from_secs(2),
                None,
                ClusterRaftReadProjectionPolicy::CompleteAdmittedSnapshot,
                move |storage| {
                    projection_entered_tx
                        .send(())
                        .expect("signal admitted compact-barrier projection");
                    projection_release_rx
                        .recv()
                        .expect("release admitted compact-barrier projection");
                    let result = storage.read_index_snapshot();
                    projection_completed_tx
                        .send(())
                        .expect("signal compact-barrier projection completion");
                    result
                },
                &mut host,
            )
            .await
        });

        tokio::task::spawn_blocking(move || projection_entered_rx.recv())
            .await
            .expect("join compact-barrier projection-entry waiter")
            .expect("compact-barrier projection acquired resident ownership");
        read.abort();
        let _ = read.await;
        projection_release_tx
            .send(())
            .expect("release cancelled compact-barrier projection");
        tokio::task::spawn_blocking(move || projection_completed_rx.recv())
            .await
            .expect("join compact-barrier projection completion waiter")
            .expect("supervisor completed compact-barrier projection");

        let diagnostic_paths = paths.clone();
        tokio::time::timeout(
            Duration::from_millis(250),
            tokio::task::spawn_blocking(move || {
                LocalClusterRaftCommandStorage::new_read_index_with_operation_deadline(
                    &diagnostic_paths,
                    local,
                    Instant::now() + Duration::from_millis(200),
                )
                .read_index_state()
            }),
        )
        .await
        .expect("cancelled compact-barrier projection must release resident ownership")
        .expect("join compact-barrier post-cancellation probe")
        .expect("compact-barrier post-cancellation probe");
        assert_eq!(shared.barrier_waves.load(Ordering::SeqCst), 1);
        clear_authoritative_barrier_singleflight_for_root(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refreshed_current_term_commit_skips_redundant_barrier_after_initial_miss() {
        let fixture = cold_barrier_fixture("cold_barrier_refresh_race");
        let mut storage = LocalClusterRaftCommandStorage::new(
            &fixture.paths,
            fixture.state.local_node_id.clone(),
        );
        let established = storage
            .append_uncommitted(KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
                key: "/cold/race".to_string(),
                value: b"established".to_vec(),
                lease_id: None,
            })))
            .expect("append intervening current-term write");
        storage
            .commit_index_compact(established.index)
            .expect("commit intervening current-term write");
        drop(storage);
        let last_index = fixture
            .paths
            .kv_paths()
            .raft_store()
            .locked_last_log_index(&fixture.local)
            .expect("last index before refreshed fence");

        let shared = Arc::new(ColdBarrierHostState::new(false, false, false));
        let mut host = ColdBarrierHost {
            shared: Arc::clone(&shared),
        };
        let mut state = fixture.state.clone();
        let deadline = Instant::now() + Duration::from_secs(2);
        let read = commit_cluster_kv_authoritative_barrier_then_read(
            &tokio::runtime::Handle::current(),
            &fixture.paths,
            &mut state,
            crate::ClusterControlPlaneTimeouts::default_daemon_transport()
                .kv_txn_quorum_options(true),
            deadline,
            |storage| storage.read_index_entry("/cold/race"),
            &mut host,
        )
        .await
        .expect("refreshed ReadIndex after initial miss");
        assert_eq!(read.value.expect("intervening entry").value, b"established");
        assert!(matches!(
            read.authority.basis,
            KvAuthorityBasis::CurrentTermQuorum { .. }
        ));
        assert_eq!(shared.barrier_waves.load(Ordering::SeqCst), 0);
        assert_eq!(shared.read_index_waves.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("last index after refreshed fence"),
            last_index
        );
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn paused_old_term_cold_fence_cannot_evict_or_append_after_new_term_barrier() {
        let fixture = cold_barrier_fixture("cold_barrier_paused_old_term_fence");
        let pause = install_authoritative_barrier_fence_pause(&fixture.paths.root);
        let shared = Arc::new(ColdBarrierHostState::new(false, false, false));
        let options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);

        let old_term_read = {
            let paths = fixture.paths.clone();
            let mut state = fixture.state.clone();
            let shared = Arc::clone(&shared);
            tokio::spawn(async move {
                let mut host = ColdBarrierHost { shared };
                read_cluster_kv_authoritative_entry_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &paths,
                    &mut state,
                    "/cold/a".to_string(),
                    options,
                    &mut host,
                )
                .await
            })
        };
        tokio::time::timeout(Duration::from_secs(2), pause.entered.acquire())
            .await
            .expect("old-term caller reached its initial cold fence")
            .expect("old-term fence pause semaphore")
            .forget();

        fixture
            .paths
            .kv_paths()
            .raft_store()
            .set_leader_compact(&fixture.local, 8)
            .expect("advance durable leadership before old preparer claims");
        let mut new_term_state = fixture.state.clone();
        new_term_state.term = 8;
        let mut new_term_host = ColdBarrierHost {
            shared: Arc::clone(&shared),
        };
        let new_term_read = read_cluster_kv_authoritative_entry_async_with_host(
            &tokio::runtime::Handle::current(),
            &fixture.paths,
            &mut new_term_state,
            "/cold/a".to_string(),
            options,
            &mut new_term_host,
        )
        .await
        .expect("new-term cold barrier read");
        assert_eq!(
            new_term_read.value.expect("new-term cold value").value,
            b"alpha"
        );
        assert_eq!(shared.barrier_waves.load(Ordering::SeqCst), 1);

        pause.release.add_permits(1);
        let old_term_read = old_term_read
            .await
            .expect("join paused old-term caller")
            .expect("old-term caller re-fences into ReadIndex");
        assert_eq!(
            old_term_read.value.expect("re-fenced old-term value").value,
            b"alpha"
        );
        assert_eq!(
            shared.barrier_waves.load(Ordering::SeqCst),
            1,
            "the stale preparer must not append a second new-term barrier"
        );
        assert_eq!(
            shared.read_index_waves.load(Ordering::SeqCst),
            1,
            "the stale preparer must re-enter ReadIndex after the new-term barrier"
        );

        let node = fixture
            .paths
            .kv_paths()
            .raft_store()
            .open_node(&fixture.local)
            .expect("inspect durable new-term log");
        assert_eq!(
            node.log()
                .iter()
                .filter(|entry| { entry.term == 8 && matches!(&entry.command, KvCommand::Barrier) })
                .count(),
            1,
            "one leader term may contain only its one cold recovery barrier"
        );
        assert_eq!(node.last_log_index(), fixture.initial_last_index + 1);
        drop(node);

        clear_authoritative_barrier_fence_pause(&fixture.paths.root);
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restart_adopts_durable_same_term_pending_barrier() {
        let fixture = cold_barrier_fixture("cold_barrier_restart_adoption");
        let pending_request = KvCommandRequest::generated(KvCommand::Barrier);
        let pending_request_id = pending_request.request_id.clone();
        let mut storage = LocalClusterRaftCommandStorage::new(
            &fixture.paths,
            fixture.state.local_node_id.clone(),
        );
        let pending_entry = storage
            .append_uncommitted(pending_request)
            .expect("persist pending current-term barrier before restart");
        assert_eq!(pending_entry.index, fixture.initial_last_index + 1);
        drop(storage);

        // The registry is process-local. Clearing it models a daemon restart;
        // the durable WAL entry and request identity must still be adopted.
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
        let shared = Arc::new(ColdBarrierHostState::new(false, false, false));
        let mut host = ColdBarrierHost {
            shared: Arc::clone(&shared),
        };
        let mut state = fixture.state.clone();
        let read = read_cluster_kv_authoritative_entry_async_with_host(
            &tokio::runtime::Handle::current(),
            &fixture.paths,
            &mut state,
            "/cold/a".to_string(),
            crate::ClusterControlPlaneTimeouts::default_daemon_transport()
                .kv_txn_quorum_options(true),
            &mut host,
        )
        .await
        .expect("authoritative read adopts pending restart barrier");
        assert_eq!(
            read.value.expect("cold value after restart").value,
            b"alpha"
        );
        assert!(matches!(
            read.authority.basis,
            KvAuthorityBasis::CurrentTermQuorum { .. }
        ));
        assert_eq!(shared.barrier_waves.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("last index after restart barrier adoption"),
            pending_entry.index,
            "restart must not append a second same-term barrier"
        );
        let mut storage = LocalClusterRaftCommandStorage::new(
            &fixture.paths,
            fixture.state.local_node_id.clone(),
        );
        assert!(
            storage
                .pending_current_term_barrier()
                .expect("pending barrier after restart read")
                .is_none(),
            "adopted barrier must be committed and no longer pending"
        );
        let outcome = storage
            .read_index_request_outcome(&pending_request_id)
            .expect("adopted barrier outcome")
            .expect("retained adopted barrier outcome");
        assert!(matches!(outcome, KvRequestOutcome::Committed(_)));
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_cold_barrier_owner_is_reclaimed_with_same_durable_request() {
        let fixture = cold_barrier_fixture("cold_barrier_cancel_reclaim");
        let shared = Arc::new(ColdBarrierHostState::new(true, false, true));
        let options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);

        let owner = {
            let paths = fixture.paths.clone();
            let mut state = fixture.state.clone();
            let shared = Arc::clone(&shared);
            tokio::spawn(async move {
                let mut host = ColdBarrierHost { shared };
                read_cluster_kv_authoritative_entry_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &paths,
                    &mut state,
                    "/cold/a".to_string(),
                    options,
                    &mut host,
                )
                .await
            })
        };
        tokio::time::timeout(
            Duration::from_secs(2),
            shared.first_barrier_entered.acquire(),
        )
        .await
        .expect("cancelled owner entered barrier transport")
        .expect("cancelled owner barrier semaphore")
        .forget();

        let waiter = {
            let paths = fixture.paths.clone();
            let mut state = fixture.state.clone();
            let shared = Arc::clone(&shared);
            tokio::spawn(async move {
                let mut host = ColdBarrierHost { shared };
                read_cluster_kv_authoritative_range_prefix_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &paths,
                    &mut state,
                    "/cold/".to_string(),
                    options,
                    &mut host,
                )
                .await
            })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if authoritative_barrier_singleflight_waiters_for_root(&fixture.paths.root) >= 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second cold reader joined cancelled owner");

        shared.first_barrier_release.add_permits(1);
        tokio::time::timeout(
            Duration::from_secs(2),
            shared.first_post_commit_entered.acquire(),
        )
        .await
        .expect("cancelled owner reached post-commit publication")
        .expect("cancelled owner post-commit semaphore")
        .forget();

        owner.abort();
        assert!(
            owner
                .await
                .expect_err("owner task must be cancelled")
                .is_cancelled()
        );
        let read = waiter
            .await
            .expect("join reclaimed cold barrier waiter")
            .expect("reclaimed cold barrier read");
        assert_eq!(read.value.len(), 2);
        assert!(matches!(
            read.authority.basis,
            KvAuthorityBasis::CurrentTermQuorum { .. }
        ));
        assert_eq!(
            shared.barrier_waves.load(Ordering::SeqCst),
            1,
            "exact committed replay must not send a second barrier wave"
        );
        assert_eq!(
            shared.read_index_waves.load(Ordering::SeqCst),
            1,
            "a replayed compact proof must be followed by ReadIndex"
        );
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("last index after cancelled owner reclaim"),
            fixture.initial_last_index + 1,
            "reclaim must reuse the original barrier request instead of appending another"
        );
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn possibly_applied_cold_barrier_error_is_shared_unchanged_before_new_term_recovery() {
        let fixture = cold_barrier_fixture("cold_barrier_unknown_reclaim");
        let shared = Arc::new(ColdBarrierHostState::new(true, true, false));
        let options = crate::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true);

        let owner = {
            let paths = fixture.paths.clone();
            let mut state = fixture.state.clone();
            let shared = Arc::clone(&shared);
            tokio::spawn(async move {
                let mut host = ColdBarrierHost { shared };
                read_cluster_kv_authoritative_entry_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &paths,
                    &mut state,
                    "/cold/a".to_string(),
                    options,
                    &mut host,
                )
                .await
            })
        };
        tokio::time::timeout(
            Duration::from_secs(2),
            shared.first_barrier_entered.acquire(),
        )
        .await
        .expect("unknown owner entered barrier transport")
        .expect("unknown owner entry semaphore")
        .forget();

        let waiter = {
            let paths = fixture.paths.clone();
            let mut state = fixture.state.clone();
            let shared = Arc::clone(&shared);
            tokio::spawn(async move {
                let mut host = ColdBarrierHost { shared };
                read_cluster_kv_authoritative_snapshot_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &paths,
                    &mut state,
                    options,
                    &mut host,
                )
                .await
            })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if authoritative_barrier_singleflight_waiters_for_root(&fixture.paths.root) >= 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second cold reader joined ambiguous owner");
        shared.first_barrier_release.add_permits(1);

        let owner_error = owner
            .await
            .expect("join ambiguous cold barrier owner")
            .expect_err("ambiguous owner must retain its exact error");
        assert!(matches!(owner_error, NetworkError::OutcomeUnknown { .. }));
        let waiter_error = waiter
            .await
            .expect("join ambiguous cold barrier waiter")
            .expect_err("ambiguous cold barrier waiter must preserve owner outcome");
        assert_eq!(waiter_error, owner_error);
        assert_eq!(shared.barrier_waves.load(Ordering::SeqCst), 1);
        assert_eq!(shared.read_index_waves.load(Ordering::SeqCst), 0);

        // The failed flight was removed. After normal Raft recovery elects a
        // new term, an independent call can safely commit the retained suffix
        // with one new-term barrier instead of inheriting a pinned error.
        fixture
            .paths
            .kv_paths()
            .raft_store()
            .set_leader_compact(&fixture.local, 8)
            .expect("restore leadership in a new term after ambiguous outcome");
        let mut recovered_state = fixture.state.clone();
        recovered_state.term = 8;
        let mut recovered_host = ColdBarrierHost {
            shared: Arc::clone(&shared),
        };
        let recovered = read_cluster_kv_authoritative_snapshot_async_with_host(
            &tokio::runtime::Handle::current(),
            &fixture.paths,
            &mut recovered_state,
            options,
            &mut recovered_host,
        )
        .await
        .expect("new-term recovery after ambiguous barrier");
        assert!(matches!(
            recovered.authority.basis,
            KvAuthorityBasis::CurrentTermQuorum { .. }
        ));
        assert_eq!(shared.barrier_waves.load(Ordering::SeqCst), 2);
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .locked_last_log_index(&fixture.local)
                .expect("last index after ambiguous owner reclaim"),
            fixture.initial_last_index + 2,
            "new-term recovery retains the old suffix and appends one new-term barrier"
        );
        clear_authoritative_barrier_singleflight_for_root(&fixture.paths.root);
    }

    #[test]
    fn first_current_term_compact_barrier_does_not_materialize_complete_snapshot() {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-compact-barrier");
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let local = NodeId::from_legacy_named("node_compact_barrier").expect("local node");
        let store = paths.kv_paths().raft_store();
        let mut node = store.open_node(&local).expect("open Raft node");
        node.become_leader(1).expect("become leader");
        drop(node);

        let mut storage = LocalClusterRaftCommandStorage::new(&paths, local.clone());
        let entry = storage
            .append_uncommitted(KvCommandRequest::generated(KvCommand::Barrier))
            .expect("append first-current-term barrier");
        let result = storage
            .commit_index_compact(entry.index)
            .expect("commit compact barrier");

        assert!(result.events.is_empty());
        assert!(
            result.checkpoint_snapshot.is_none(),
            "an ordinary authority barrier must stay delta-sized unless periodic compaction is due"
        );
        assert_eq!(result.read_state.current_term, 1);
        assert_eq!(result.read_state.leader_id, Some(local));
    }
}
