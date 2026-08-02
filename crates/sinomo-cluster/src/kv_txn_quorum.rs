use crate::control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
use crate::control_plane_options::ClusterKvTxnQuorumOptions;
use crate::kv_forward::{
    ClusterKvTxnForwardOptions,
    forward_cluster_kv_authoritative_range_prefix_to_state_authority_async_with_host,
    forward_cluster_kv_authoritative_snapshot_to_state_authority_async_with_host,
    forward_cluster_kv_authoritative_snapshot_to_state_authority_with_host,
    forward_cluster_kv_request_outcome_to_state_authority_async_with_host,
    forward_cluster_kv_txn_compact_to_state_authority_async_with_host,
    forward_cluster_kv_txn_to_state_authority_async_with_host,
    forward_cluster_kv_txn_to_state_authority_with_host,
};
use crate::kv_local_store::ClusterKvPathsExt;
use crate::kv_local_store::kv_err;
use crate::kv_local_store::{
    LocalClusterRaftCommandStorage, imported_snapshot_advances_local_kv,
    locked_kv_state_authority_id, write_imported_cluster_kv_snapshot_if_changed,
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
    ClusterKvQuorumAsyncHost, commit_cluster_raft_command_compact_with_quorum_async_with_host,
    commit_cluster_raft_command_with_control_plane_driver,
    commit_cluster_raft_command_with_quorum_async_with_host,
    read_cluster_raft_with_read_index_async_with_host,
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
    ClusterKvTxnCommitResult, ClusterKvTxnRpcResponse,
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

fn remaining_authoritative_read_budget(
    deadline: Instant,
    operation: &str,
) -> Result<Duration, NetworkError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!("{operation} exhausted its Raft confirmation budget"),
        });
    }
    Ok(remaining)
}

fn authoritative_read_phase_timeout(operation: &str) -> NetworkError {
    NetworkError::ClusterReadOnly {
        detail: format!("{operation} exhausted its Raft confirmation budget"),
    }
}

async fn project_committed_cluster_raft_config_before_deadline(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    deadline: Instant,
    operation: &str,
) -> Result<ClusterBootstrapState, NetworkError> {
    let remaining = remaining_authoritative_read_budget(deadline, operation)?;
    let project_paths = paths.clone();
    let mut consensus_state = state.clone();
    tokio::time::timeout(
        remaining,
        runtime.spawn_blocking(move || {
            project_committed_cluster_raft_config(&project_paths, &mut consensus_state)?;
            Ok::<_, NetworkError>(consensus_state)
        }),
    )
    .await
    .map_err(|_| authoritative_read_phase_timeout(operation))?
    .map_err(|source| NetworkError::Io {
        operation: operation.to_string(),
        detail: format!("Raft config projection task failed: {source}"),
    })?
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
    let remaining = remaining_authoritative_read_budget(deadline, "authoritative KV barrier")?;
    metrics::counter!("sinomo.kv.authoritative_read", "path" => "barrier").increment(1);
    let result = commit_cluster_raft_command_with_quorum_async_with_host(
        runtime,
        paths,
        state,
        KvCommandRequest::generated(KvCommand::Barrier),
        ClusterKvRaftQuorumCommitOptions { timeout: remaining },
        options.raft_append_replication_options,
        host,
    )
    .await?;
    authoritative_read_from_commit(result, state)
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
    F: FnOnce(&mut LocalClusterRaftCommandStorage) -> Result<T, NetworkError> + Send + 'static,
{
    let local_node_id = state.local_node_id.clone();
    let fence_paths = paths.clone();
    let fence_node_id = local_node_id.clone();
    let fence_budget = remaining_authoritative_read_budget(
        deadline,
        "authoritative KV barrier configuration fence",
    )?;
    let config_fence = tokio::time::timeout(
        fence_budget,
        runtime.spawn_blocking(move || {
            let mut storage = LocalClusterRaftCommandStorage::new(&fence_paths, fence_node_id);
            storage.read_index_entry(&sinomo_kv::cluster_kv_raft_config_key())
        }),
    )
    .await
    .map_err(|_| authoritative_read_phase_timeout("authoritative KV barrier configuration fence"))?
    .map_err(|source| NetworkError::Io {
        operation: "prepare authoritative cluster KV barrier read".to_string(),
        detail: format!("cluster KV barrier configuration task failed: {source}"),
    })??;

    let remaining = remaining_authoritative_read_budget(deadline, "authoritative KV barrier")?;
    metrics::counter!("sinomo.kv.authoritative_read", "path" => "compact_barrier").increment(1);
    let compact = tokio::time::timeout(
        remaining,
        commit_cluster_raft_command_compact_with_quorum_async_with_host(
            runtime,
            paths,
            state,
            KvCommandRequest::generated(KvCommand::Barrier),
            ClusterKvRaftQuorumCommitOptions { timeout: remaining },
            options.raft_append_replication_options,
            host,
        ),
    )
    .await
    .map_err(|_| authoritative_read_phase_timeout("authoritative KV compact barrier"))??;

    let confirmed_term = compact.authority.term;
    let confirmed_commit_index = compact.authority.commit_index;
    let final_paths = paths.clone();
    let final_node_id = local_node_id.clone();
    let final_budget = remaining_authoritative_read_budget(
        deadline,
        "authoritative KV compact barrier final read",
    )?;
    let (applied_state, value) = tokio::time::timeout(
        final_budget,
        runtime.spawn_blocking(move || {
            let mut storage = LocalClusterRaftCommandStorage::new(&final_paths, final_node_id.clone());
            let applied_state = storage.read_index_state()?;
            if applied_state.current_term != confirmed_term
                || applied_state.leader_id.as_ref() != Some(&final_node_id)
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
            let value = read_value(&mut storage)?;
            Ok((applied_state, value))
        }),
    )
    .await
    .map_err(|_| {
        authoritative_read_phase_timeout("authoritative KV compact barrier final read")
    })?
    .map_err(|source| NetworkError::Io {
        operation: "project authoritative cluster KV compact barrier".to_string(),
        detail: format!("cluster KV compact barrier projection task failed: {source}"),
    })??;

    state.term = state.term.max(applied_state.current_term);
    state.kv_revision = state.kv_revision.max(applied_state.revision.0);
    let mut authority = compact.authority;
    authority.commit_index = applied_state.applied_index;
    authority.revision = applied_state.revision;
    Ok(KvAuthoritativeRead { value, authority })
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
        .clear_leader_compact(&state.local_node_id)
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

async fn forward_cluster_kv_authoritative_range_prefix_recovering_stale_leader_async<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    prefix: &str,
    options: ClusterKvTxnForwardOptions,
    host: &mut H,
) -> Result<Option<KvAuthoritativeRead<Vec<sinomo_kv::KvEntry>>>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let deadline = Instant::now() + options.timeout;
    let result = forward_cluster_kv_authoritative_range_prefix_to_state_authority_async_with_host(
        runtime, paths, state, prefix, options, host,
    )
    .await;
    if let Err(error) = &result
        && clear_verified_stale_cluster_raft_leader_async(runtime, paths, state, error).await?
        && let Some(options) = cluster_kv_forward_options_before_deadline(options, deadline)
    {
        return forward_cluster_kv_authoritative_range_prefix_to_state_authority_async_with_host(
            runtime, paths, state, prefix, options, host,
        )
        .await;
    }
    result
}

async fn forward_cluster_kv_request_outcome_recovering_stale_leader_async<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request_id: &sinomo_kv::KvRequestId,
    options: ClusterKvTxnForwardOptions,
    host: &mut H,
) -> Result<Option<sinomo_kv::ClusterKvRequestOutcomeRead>, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let deadline = Instant::now() + options.timeout;
    let result = forward_cluster_kv_request_outcome_to_state_authority_async_with_host(
        runtime, paths, state, request_id, options, host,
    )
    .await;
    if let Err(error) = &result
        && clear_verified_stale_cluster_raft_leader_async(runtime, paths, state, error).await?
        && let Some(options) = cluster_kv_forward_options_before_deadline(options, deadline)
    {
        return forward_cluster_kv_request_outcome_to_state_authority_async_with_host(
            runtime, paths, state, request_id, options, host,
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
        let host = &mut *self.host;
        Box::pin(async move {
            let deadline = Instant::now() + options.timeout;
            let result = forward_cluster_kv_txn_compact_to_state_authority_async_with_host(
                runtime, paths, state, request, options, host,
            )
            .await;
            if let Err(error) = &result {
                if clear_verified_stale_cluster_raft_leader_async(runtime, paths, state, error)
                    .await?
                    && let Some(options) =
                        cluster_kv_forward_options_before_deadline(options, deadline)
                {
                    return forward_cluster_kv_txn_compact_to_state_authority_async_with_host(
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
        let host = &mut *self.host;
        Box::pin(async move {
            commit_cluster_raft_command_compact_with_quorum_async_with_host(
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
    let deadline = Instant::now() + options.raft_append_timeout;
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
        deadline,
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
    let remaining = remaining_authoritative_read_budget(
        deadline,
        "authoritative KV request-outcome attempt lookup",
    )?;
    let kv_paths = paths.kv_paths();
    let request_id = request_id.clone();
    let local_outcome = tokio::time::timeout(
        remaining,
        runtime.spawn_blocking(move || {
            sinomo_kv::read_cluster_kv_request_attempt(&kv_paths, &request_id).map_err(kv_err)
        }),
    )
    .await
    .map_err(|_| {
        authoritative_read_phase_timeout("authoritative KV request-outcome attempt lookup")
    })?
    .map_err(|source| NetworkError::Io {
        operation: "read cluster KV request attempt".to_string(),
        detail: format!("cluster KV request-attempt task failed: {source}"),
    })??;
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
    let mut consensus_state = project_committed_cluster_raft_config_before_deadline(
        runtime,
        paths,
        state,
        deadline,
        "project committed cluster Raft config for request outcome",
    )
    .await?;

    let local_is_leader = if sinomo_kv::cluster_kv_raft_local_is_voter(&consensus_state) {
        let remaining = remaining_authoritative_read_budget(
            deadline,
            "authoritative KV request-outcome leader check",
        )?;
        tokio::time::timeout(
            remaining,
            locked_cluster_raft_local_is_leader_async(runtime, paths, &consensus_state),
        )
        .await
        .map_err(|_| {
            authoritative_read_phase_timeout("authoritative KV request-outcome leader check")
        })??
    } else {
        false
    };
    if local_is_leader {
        let request_id_for_read = request_id.clone();
        let remaining = remaining_authoritative_read_budget(
            deadline,
            "authoritative KV request-outcome ReadIndex",
        )?;
        let read = if let Some(read) = read_cluster_raft_with_read_index_async_with_host(
            runtime,
            paths,
            &mut consensus_state,
            remaining,
            options.raft_append_replication_options,
            move |storage| storage.read_index_request_outcome(&request_id_for_read),
            host,
        )
        .await?
        {
            read
        } else {
            let request_id_for_read = request_id.clone();
            commit_cluster_kv_authoritative_barrier_then_read(
                runtime,
                paths,
                &mut consensus_state,
                options,
                deadline,
                move |storage| storage.read_index_request_outcome(&request_id_for_read),
                host,
            )
            .await?
        };
        let read = overlay_node_local_request_attempt_after_authoritative_history(
            runtime,
            paths,
            &request_id,
            retained_request_outcome_read(read),
            consult_node_local_attempt,
            deadline,
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
        return Ok(read);
    }

    let mut forwarding_failure = None;
    if options.allow_forwarding {
        let remaining = remaining_authoritative_read_budget(
            deadline,
            "authoritative KV request-outcome forwarding",
        )?;
        match tokio::time::timeout(
            remaining,
            forward_cluster_kv_request_outcome_recovering_stale_leader_async(
                runtime,
                paths,
                &consensus_state,
                &request_id,
                options.forward_options,
                host,
            ),
        )
        .await
        .map_err(|_| {
            authoritative_read_phase_timeout("authoritative KV request-outcome forwarding")
        })? {
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
    if !sinomo_kv::cluster_kv_raft_local_is_voter(&consensus_state) {
        return Err(forwarding_failure.unwrap_or_else(|| NetworkError::ClusterReadOnly {
            detail: "local node is not a Raft voter and could not forward an authoritative request outcome"
                .to_string(),
        }));
    }
    if !options.allow_forwarding {
        return Err(NetworkError::ClusterReadOnly {
            detail: "forwarded authoritative cluster KV read reached a non-leader voter; forwarded requests do not start elections"
                .to_string(),
        });
    }
    let remaining = remaining_authoritative_read_budget(
        deadline,
        "authoritative KV request-outcome leader election",
    )?;
    if !tokio::time::timeout(
        remaining,
        elect_cluster_raft_leader_async_with_host(
            runtime,
            paths,
            &mut consensus_state,
            options.leader_election_options,
            host,
        ),
    )
    .await
    .map_err(|_| {
        authoritative_read_phase_timeout("authoritative KV request-outcome leader election")
    })?? {
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
    let deadline = Instant::now() + options.raft_append_timeout;
    let mut consensus_state = project_committed_cluster_raft_config_before_deadline(
        runtime,
        paths,
        state,
        deadline,
        "project committed cluster Raft config for point read",
    )
    .await?;

    let local_is_leader = if sinomo_kv::cluster_kv_raft_local_is_voter(&consensus_state) {
        let remaining =
            remaining_authoritative_read_budget(deadline, "authoritative KV leader check")?;
        tokio::time::timeout(
            remaining,
            locked_cluster_raft_local_is_leader_async(runtime, paths, &consensus_state),
        )
        .await
        .map_err(|_| authoritative_read_phase_timeout("authoritative KV leader check"))??
    } else {
        false
    };
    if local_is_leader {
        let key_for_read = key.clone();
        let remaining =
            remaining_authoritative_read_budget(deadline, "authoritative KV point ReadIndex")?;
        if let Some(read) = read_cluster_raft_with_read_index_async_with_host(
            runtime,
            paths,
            &mut consensus_state,
            remaining,
            options.raft_append_replication_options,
            move |storage| storage.read_index_entry(&key_for_read),
            host,
        )
        .await?
        {
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

        // This leader already spent its ReadIndex attempt. Go directly to the
        // first-current-term barrier under the same deadline; routing through
        // the snapshot helper would perform a second heartbeat round.
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

    let mut forwarding_failure = None;
    if options.allow_forwarding {
        let remaining =
            remaining_authoritative_read_budget(deadline, "authoritative KV point forwarding")?;
        match tokio::time::timeout(
            remaining,
            forward_cluster_kv_authoritative_range_prefix_recovering_stale_leader_async(
                runtime,
                paths,
                &consensus_state,
                &key,
                options.forward_options,
                host,
            ),
        )
        .await
        .map_err(|_| authoritative_read_phase_timeout("authoritative KV point forwarding"))?
        {
            Ok(Some(read)) => {
                state.term = state
                    .term
                    .max(consensus_state.term)
                    .max(read.authority.term);
                state.kv_revision = state
                    .kv_revision
                    .max(consensus_state.kv_revision)
                    .max(read.authority.revision.0);
                return Ok(KvAuthoritativeRead {
                    value: read.value.into_iter().find(|entry| entry.key == key),
                    authority: read.authority,
                });
            }
            Ok(None) => {}
            Err(error) => forwarding_failure = Some(error),
        }
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
            detail: "forwarded authoritative point read reached a non-leader voter; forwarded requests do not start elections"
                .to_string(),
        });
    }
    let remaining =
        remaining_authoritative_read_budget(deadline, "authoritative KV leader election")?;
    if !tokio::time::timeout(
        remaining,
        elect_cluster_raft_leader_async_with_host(
            runtime,
            paths,
            &mut consensus_state,
            options.leader_election_options,
            host,
        ),
    )
    .await
    .map_err(|_| authoritative_read_phase_timeout("authoritative KV leader election"))??
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
    let deadline = Instant::now() + options.raft_append_timeout;
    let mut consensus_state = project_committed_cluster_raft_config_before_deadline(
        runtime,
        paths,
        state,
        deadline,
        "project committed cluster Raft config for prefix read",
    )
    .await?;

    let local_is_leader = if sinomo_kv::cluster_kv_raft_local_is_voter(&consensus_state) {
        let remaining =
            remaining_authoritative_read_budget(deadline, "authoritative KV leader check")?;
        tokio::time::timeout(
            remaining,
            locked_cluster_raft_local_is_leader_async(runtime, paths, &consensus_state),
        )
        .await
        .map_err(|_| authoritative_read_phase_timeout("authoritative KV leader check"))??
    } else {
        false
    };
    if local_is_leader {
        let prefix_for_read = prefix.clone();
        let remaining =
            remaining_authoritative_read_budget(deadline, "authoritative KV prefix ReadIndex")?;
        if let Some(read) = read_cluster_raft_with_read_index_async_with_host(
            runtime,
            paths,
            &mut consensus_state,
            remaining,
            options.raft_append_replication_options,
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

        // As for point reads, a failed steady-state probe falls straight back
        // to one barrier and shares the original end-to-end timeout.
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

    let mut forwarding_failure = None;
    if options.allow_forwarding {
        let remaining =
            remaining_authoritative_read_budget(deadline, "authoritative KV prefix forwarding")?;
        match tokio::time::timeout(
            remaining,
            forward_cluster_kv_authoritative_range_prefix_recovering_stale_leader_async(
                runtime,
                paths,
                &consensus_state,
                &prefix,
                options.forward_options,
                host,
            ),
        )
        .await
        .map_err(|_| authoritative_read_phase_timeout("authoritative KV prefix forwarding"))?
        {
            Ok(Some(read)) => {
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
            detail: "forwarded authoritative prefix read reached a non-leader voter; forwarded requests do not start elections"
                .to_string(),
        });
    }
    let remaining =
        remaining_authoritative_read_budget(deadline, "authoritative KV leader election")?;
    if !tokio::time::timeout(
        remaining,
        elect_cluster_raft_leader_async_with_host(
            runtime,
            paths,
            &mut consensus_state,
            options.leader_election_options,
            host,
        ),
    )
    .await
    .map_err(|_| authoritative_read_phase_timeout("authoritative KV leader election"))??
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
    if !sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        if options.allow_forwarding {
            let remaining =
                remaining_authoritative_read_budget(deadline, "authoritative KV read forwarding")?;
            let forwarded = tokio::time::timeout(
                remaining,
                forward_cluster_kv_authoritative_snapshot_recovering_stale_leader_async(
                    runtime,
                    paths,
                    state,
                    options.forward_options,
                    host,
                ),
            )
            .await
            .map_err(|_| authoritative_read_phase_timeout("authoritative KV read forwarding"))??;
            if let Some(read) = forwarded {
                return validate_forwarded_authoritative_read(read, state);
            }
        }
        return Err(NetworkError::ClusterReadOnly {
            detail: "local node is not a Raft voter and could not forward an authoritative read"
                .to_string(),
        });
    }
    let remaining = remaining_authoritative_read_budget(deadline, "authoritative KV leader check")?;
    let local_is_leader = tokio::time::timeout(
        remaining,
        locked_cluster_raft_local_is_leader_async(runtime, paths, state),
    )
    .await
    .map_err(|_| authoritative_read_phase_timeout("authoritative KV leader check"))??;
    if !local_is_leader {
        let forwarding_failure = if options.allow_forwarding {
            let remaining =
                remaining_authoritative_read_budget(deadline, "authoritative KV read forwarding")?;
            match tokio::time::timeout(
                remaining,
                forward_cluster_kv_authoritative_snapshot_recovering_stale_leader_async(
                    runtime,
                    paths,
                    state,
                    options.forward_options,
                    host,
                ),
            )
            .await
            .map_err(|_| authoritative_read_phase_timeout("authoritative KV read forwarding"))?
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
        let remaining =
            remaining_authoritative_read_budget(deadline, "authoritative KV leader election")?;
        if !tokio::time::timeout(
            remaining,
            elect_cluster_raft_leader_async_with_host(
                runtime,
                paths,
                state,
                options.leader_election_options,
                host,
            ),
        )
        .await
        .map_err(|_| authoritative_read_phase_timeout("authoritative KV leader election"))??
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
    let remaining =
        remaining_authoritative_read_budget(deadline, "authoritative KV final leader check")?;
    if !tokio::time::timeout(
        remaining,
        locked_cluster_raft_local_is_leader_async(runtime, paths, state),
    )
    .await
    .map_err(|_| authoritative_read_phase_timeout("authoritative KV final leader check"))??
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: "local node lacks verified current-term Raft leadership".to_string(),
        });
    }
    if let Some(read) = read_cluster_raft_with_read_index_async_with_host(
        runtime,
        paths,
        state,
        remaining_authoritative_read_budget(deadline, "authoritative KV snapshot ReadIndex")?,
        options.raft_append_replication_options,
        |storage| storage.read_index_snapshot(),
        host,
    )
    .await?
    {
        return Ok(read);
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
