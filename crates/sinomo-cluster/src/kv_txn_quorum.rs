use crate::control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
use crate::control_plane_options::ClusterKvTxnQuorumOptions;
use crate::kv_forward::{
    ClusterKvTxnForwardOptions,
    forward_cluster_kv_authoritative_snapshot_to_state_authority_async_with_host,
    forward_cluster_kv_authoritative_snapshot_to_state_authority_with_host,
    forward_cluster_kv_txn_to_state_authority_async_with_host,
    forward_cluster_kv_txn_to_state_authority_with_host,
};
use crate::kv_local_store::ClusterKvPathsExt;
use crate::kv_local_store::kv_err;
use crate::kv_local_store::{
    imported_snapshot_advances_local_kv, locked_kv_state_authority_id,
    write_imported_cluster_kv_snapshot_if_changed,
};
use crate::kv_snapshot::refresh_current_cluster_kv_store_with_host;
#[cfg(any(test, feature = "test-support"))]
use crate::local_state_store::read_cluster_state_without_kv;
use crate::local_state_store::{
    project_committed_cluster_raft_config, read_cluster_state_without_kv_async,
};
use crate::prelude::*;
use crate::raft_election::{
    ClusterRaftVoteTransportAsyncHost, elect_cluster_raft_leader_async_with_host,
    elect_cluster_raft_leader_with_host,
};
use crate::raft_quorum::{
    ClusterKvQuorumAsyncHost, commit_cluster_raft_command_with_control_plane_driver,
    commit_cluster_raft_command_with_quorum_async_with_host,
};
use crate::state_authority_reachability::{
    ClusterStateAuthorityReachabilityAsyncHost, ClusterStateAuthorityReachabilityOptions,
    cluster_reachable_state_authority_count_async_with_host,
    cluster_reachable_state_authority_count_with_host,
};
#[allow(unused_imports)]
use sinomo_kv::KvError;
use sinomo_kv::{
    ClusterKvRaftQuorumCommitOptions, ClusterKvRaftQuorumCommitResult,
    ClusterKvStateAuthorityVoteRequest, ClusterKvStateAuthorityVoteResponse,
    ClusterKvTxnCommitResult,
};
use sinomo_kv::{KvAuthoritativeRead, KvAuthorityBasis, KvCommand};
use sinomo_kv::{KvCommandRequest, KvSnapshot, KvTxnRequest};
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterObjectRpcAsyncHost;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{ObjectRpcRequest, ObjectRpcResponse};
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
}

pub trait ClusterKvTxnQuorumAsyncHost:
    ClusterObjectRpcAsyncHost
    + ClusterKvQuorumAsyncHost
    + ClusterStateAuthorityReachabilityAsyncHost
    + Send
{
    fn request_kv_state_authority_vote_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterKvStateAuthorityVoteRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvStateAuthorityVoteResponse>;
}

impl<T> ClusterRaftVoteTransportAsyncHost for T
where
    T: ClusterKvTxnQuorumAsyncHost,
{
    fn send_raft_vote_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a sinomo_kv::ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, sinomo_kv::ClusterRaftVoteResponse> {
        ClusterKvTxnQuorumAsyncHost::request_kv_state_authority_vote_async(
            self,
            state,
            target_node_id,
            request,
            timeout,
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
        let result = self.host.forward_cluster_kv_txn_to_state_authority(
            self.paths,
            state,
            request,
            self.options,
        );
        if let Err(error) = &result {
            if clear_verified_stale_cluster_raft_leader(self.paths, state, error)?
                && let Some(forward_options) = cluster_kv_forward_options_before_deadline(
                    self.options.forward_options,
                    deadline,
                )
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
    let mut consensus_state = state.clone();
    project_committed_cluster_raft_config(paths, &mut consensus_state)?;
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
    state.term = state.term.max(consensus_state.term);
    state.kv_revision = state.kv_revision.max(consensus_state.kv_revision);
    Ok(result)
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

fn validate_forwarded_authoritative_read(
    read: KvAuthoritativeRead<KvSnapshot>,
    state: &ClusterBootstrapState,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
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
            detail: "authoritative read was not current-term quorum confirmed".to_string(),
        });
    };
    if required_quorum != expected_quorum
        || voter_acks < required_quorum
        || voter_acks > voter_count
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "authoritative proof reported {voter_acks} voter acknowledgements and quorum {required_quorum}, but committed membership has {voter_count} voters and requires {expected_quorum}"
            ),
        });
    }
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

pub fn read_cluster_kv_authoritative_snapshot_with_host(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
    let _ = host.refresh_current_cluster_kv_store(paths, state);
    let mut consensus_state = state.clone();
    project_committed_cluster_raft_config(paths, &mut consensus_state)?;
    let read = read_cluster_kv_authoritative_snapshot_with_consensus_state(
        paths,
        &mut consensus_state,
        options,
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
}

fn read_cluster_kv_authoritative_snapshot_with_consensus_state(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    options: ClusterKvTxnQuorumOptions,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
    if !sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        if options.allow_forwarding
            && let Some(read) = forward_cluster_kv_authoritative_snapshot_recovering_stale_leader(
                paths,
                state,
                options.forward_options,
                host,
            )?
        {
            return validate_forwarded_authoritative_read(read, state);
        }
        return Err(NetworkError::ClusterReadOnly {
            detail: "local node is not a Raft voter and could not forward an authoritative read"
                .to_string(),
        });
    }
    if !host.local_is_kv_state_authority(paths, state)? {
        let forwarding_failure = if options.allow_forwarding {
            match forward_cluster_kv_authoritative_snapshot_recovering_stale_leader(
                paths,
                state,
                options.forward_options,
                host,
            ) {
                Ok(Some(read)) => return validate_forwarded_authoritative_read(read, state),
                Ok(None) => None,
                Err(error) => Some(error),
            }
        } else {
            None
        };
        if !options.allow_forwarding {
            return Err(NetworkError::ClusterReadOnly {
                detail: "forwarded authoritative cluster KV read reached a non-leader voter; forwarded requests do not start elections".to_string(),
            });
        }
        if !host.elect_kv_state_authority(paths, state, options)? {
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
    }
    if !host.local_is_kv_state_authority(paths, state)? {
        return Err(NetworkError::ClusterReadOnly {
            detail: "local node lacks verified current-term Raft leadership".to_string(),
        });
    }
    let result = host.commit_cluster_kv_command_with_quorum(
        paths,
        state,
        KvCommandRequest::generated(KvCommand::Barrier),
        ClusterKvRaftQuorumCommitOptions {
            timeout: options.raft_append_timeout,
        },
    )?;
    authoritative_read_from_commit(result, state)
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
    error: &NetworkError,
) -> Result<bool, NetworkError> {
    let NetworkError::VerifiedClusterNonLeader { node_id, .. } = error else {
        return Ok(false);
    };
    if node_id == &state.local_node_id {
        return Ok(false);
    }
    let store = paths.kv_paths().raft_store();
    let _raft_guard = store.lock_operation().map_err(kv_err)?;
    if store.known_voter_leader_id(state).map_err(kv_err)?.as_ref() != Some(node_id) {
        return Ok(false);
    }
    store
        .clear_leader_and_write_snapshot_if_changed(&state.local_node_id)
        .map_err(kv_err)?;
    Ok(true)
}

fn forward_cluster_kv_authoritative_snapshot_recovering_stale_leader(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvTxnForwardOptions,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<Option<KvAuthoritativeRead<KvSnapshot>>, NetworkError> {
    let deadline = Instant::now() + options.timeout;
    let result = forward_cluster_kv_authoritative_snapshot_to_state_authority_with_host(
        paths, state, options, host,
    );
    if let Err(error) = &result
        && clear_verified_stale_cluster_raft_leader(paths, state, error)?
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

async fn clear_verified_stale_cluster_raft_leader_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    error: &NetworkError,
) -> Result<bool, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    let error = error.clone();
    runtime
        .spawn_blocking(move || clear_verified_stale_cluster_raft_leader(&paths, &state, &error))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "clear rejected cluster Raft leader".to_string(),
            detail: format!("stale leader clear task failed: {source}"),
        })?
}

async fn forward_cluster_kv_authoritative_snapshot_recovering_stale_leader_async<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvTxnForwardOptions,
    host: &mut H,
) -> Result<Option<KvAuthoritativeRead<KvSnapshot>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let deadline = Instant::now() + options.timeout;
    let result = forward_cluster_kv_authoritative_snapshot_to_state_authority_async_with_host(
        runtime, paths, state, options, host,
    )
    .await;
    if let Err(error) = &result
        && clear_verified_stale_cluster_raft_leader_async(runtime, paths, state, error).await?
        && let Some(options) = cluster_kv_forward_options_before_deadline(options, deadline)
    {
        return forward_cluster_kv_authoritative_snapshot_to_state_authority_async_with_host(
            runtime, paths, state, options, host,
        )
        .await;
    }
    result
}

async fn locked_cluster_raft_local_is_leader_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<bool, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || locked_cluster_raft_local_is_leader(&paths, &state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft local leader".to_string(),
            detail: format!("local leader task failed: {source}"),
        })?
}

async fn write_imported_cluster_kv_snapshot_if_changed_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<(), NetworkError> {
    let paths = paths.clone();
    let snapshot = snapshot.clone();
    runtime
        .spawn_blocking(move || write_imported_cluster_kv_snapshot_if_changed(&paths, &snapshot))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster kv snapshot import".to_string(),
            detail: format!("cluster kv snapshot import task failed: {source}"),
        })?
        .map(|_| ())
}

struct NetworkClusterKvTxnQuorumAsyncHost<'a, H> {
    runtime: &'a tokio::runtime::Handle,
    paths: &'a ClusterPaths,
    options: ClusterKvTxnQuorumOptions,
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
        Box::pin(
            async move { locked_cluster_raft_local_is_leader_async(runtime, paths, state).await },
        )
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
        let host = &mut *self.host;
        Box::pin(async move {
            let deadline = Instant::now() + options.timeout;
            let result = forward_cluster_kv_txn_to_state_authority_async_with_host(
                runtime, paths, state, request, options, host,
            )
            .await;
            if let Err(error) = &result {
                if clear_verified_stale_cluster_raft_leader_async(runtime, paths, state, error)
                    .await?
                    && let Some(options) =
                        cluster_kv_forward_options_before_deadline(options, deadline)
                {
                    return forward_cluster_kv_txn_to_state_authority_async_with_host(
                        runtime, paths, state, request, options, host,
                    )
                    .await;
                }
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
        let host = &mut *self.host;
        Box::pin(async move {
            elect_cluster_raft_leader_async_with_host(runtime, paths, state, options, host).await
        })
    }

    fn reachable_voter_count_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, usize, Self::Error> {
        let paths = self.paths;
        let options = self.options.state_authority_reachability_options;
        let host = &mut *self.host;
        Box::pin(async move {
            cluster_reachable_state_authority_count_async_with_host(paths, state, options, host)
                .await
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
        let host = &mut *self.host;
        Box::pin(async move {
            commit_cluster_raft_command_with_quorum_async_with_host(
                runtime,
                paths,
                state,
                request.into_command_request(),
                ClusterKvRaftQuorumCommitOptions {
                    timeout: options.raft_append_timeout,
                },
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
    let project_paths = paths.clone();
    let mut consensus_state = state.clone();
    consensus_state = runtime
        .spawn_blocking(move || {
            project_committed_cluster_raft_config(&project_paths, &mut consensus_state)?;
            Ok::<_, NetworkError>(consensus_state)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "project committed cluster Raft config".to_string(),
            detail: format!("Raft config projection task failed: {source}"),
        })??;
    let allow_forwarding = options.allow_forwarding;
    let mut kv_host = NetworkClusterKvTxnQuorumAsyncHost {
        runtime,
        paths,
        options,
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
    let project_paths = paths.clone();
    let mut consensus_state = state.clone();
    consensus_state = runtime
        .spawn_blocking(move || {
            project_committed_cluster_raft_config(&project_paths, &mut consensus_state)?;
            Ok::<_, NetworkError>(consensus_state)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "project committed cluster Raft config".to_string(),
            detail: format!("Raft config projection task failed: {source}"),
        })??;
    let read = read_cluster_kv_authoritative_snapshot_async_with_consensus_state(
        runtime,
        paths,
        &mut consensus_state,
        options,
        host,
    )
    .await?;
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
    host: &mut H,
) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    if !sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        if options.allow_forwarding
            && let Some(read) =
                forward_cluster_kv_authoritative_snapshot_recovering_stale_leader_async(
                    runtime,
                    paths,
                    state,
                    options.forward_options,
                    host,
                )
                .await?
        {
            return validate_forwarded_authoritative_read(read, state);
        }
        return Err(NetworkError::ClusterReadOnly {
            detail: "local node is not a Raft voter and could not forward an authoritative read"
                .to_string(),
        });
    }
    if !locked_cluster_raft_local_is_leader_async(runtime, paths, state).await? {
        let forwarding_failure = if options.allow_forwarding {
            match forward_cluster_kv_authoritative_snapshot_recovering_stale_leader_async(
                runtime,
                paths,
                state,
                options.forward_options,
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
        if !options.allow_forwarding {
            return Err(NetworkError::ClusterReadOnly {
                detail: "forwarded authoritative cluster KV read reached a non-leader voter; forwarded requests do not start elections".to_string(),
            });
        }
        if !elect_cluster_raft_leader_async_with_host(
            runtime,
            paths,
            state,
            options.leader_election_options,
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
    }
    if !locked_cluster_raft_local_is_leader_async(runtime, paths, state).await? {
        return Err(NetworkError::ClusterReadOnly {
            detail: "local node lacks verified current-term Raft leadership".to_string(),
        });
    }
    let result = commit_cluster_raft_command_with_quorum_async_with_host(
        runtime,
        paths,
        state,
        KvCommandRequest::generated(KvCommand::Barrier),
        ClusterKvRaftQuorumCommitOptions {
            timeout: options.raft_append_timeout,
        },
        options.raft_append_replication_options,
        host,
    )
    .await?;
    authoritative_read_from_commit(result, state)
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
    use sinomo_kv::{InProcessKvStore, KvAuthorityProof, KvRevision};
    use sinomo_node::ClusterId;
    use time::OffsetDateTime;

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

        assert!(
            !clear_verified_stale_cluster_raft_leader(
                &paths,
                &state,
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
                &NetworkError::VerifiedClusterNonLeader {
                    node_id: remote,
                    detail: "known voter rejected forwarding".to_string(),
                },
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
        let state = three_voter_state();
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
                authoritative_read(revision, KvRevision(8), 2, 2),
                &state,
            )
            .is_err(),
            "proof and snapshot revisions must identify the same committed view"
        );
    }
}
