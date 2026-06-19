use crate::ClusterPaths;
use crate::control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
use crate::control_plane_options::{ClusterKvTxnQuorumOptions, ClusterRaftLeaderElectionOptions};
use crate::error::NetworkError;
use crate::kv_forward::{
    ClusterKvTxnForwardOptions, forward_cluster_kv_txn_to_voter_async_with_host,
    forward_cluster_kv_txn_to_voter_with_host,
};
use crate::kv_local_store::{
    cluster_kv_raft_store, imported_snapshot_advances_local_kv,
    locked_cluster_raft_known_leader_id, write_imported_cluster_kv_snapshot_if_changed,
};
use crate::kv_semantics::cluster_semantic_txn_no_effect_response;
use crate::kv_snapshot::refresh_current_cluster_kv_store_with_host;
#[cfg(test)]
use crate::local_state_store::read_cluster_state_without_kv;
use crate::local_state_store::read_cluster_state_without_kv_async;
use crate::prelude::*;
use crate::raft_election::{
    ClusterRaftVoteTransportAsyncHost, elect_cluster_raft_leader_async_with_host,
    elect_cluster_raft_leader_with_host,
};
use crate::raft_quorum::{
    ClusterRaftQuorumAsyncHost, commit_cluster_raft_command_with_control_plane_driver,
    commit_cluster_raft_command_with_quorum_async_with_host,
};
use crate::route::ClusterSignedEndpointRpcAsyncHost;
use crate::rpc::{ObjectRpcRequest, ObjectRpcResponse};
use crate::rpc_future::ClusterMembershipRpcFuture;
use crate::state_authority_reachability::{
    ClusterStateAuthorityReachabilityAsyncHost, ClusterStateAuthorityReachabilityOptions,
    cluster_reachable_state_authority_count_async_with_host,
    cluster_reachable_state_authority_count_with_host,
};
use sinomo_kv::{
    ClusterKvRaftQuorumCommitOptions, ClusterKvRaftQuorumCommitResult, ClusterKvTxnCommitResult,
    ClusterRaftVoteRequest, ClusterRaftVoteResponse,
};
use std::time::Duration;

pub trait ClusterKvTxnQuorumHost {
    fn refresh_current_cluster_kv_store(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        Ok(())
    }

    fn known_raft_leader_id(
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

    fn local_is_raft_leader(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError>;

    fn send_raft_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftVoteResponse, NetworkError>;

    fn forward_cluster_kv_txn_to_voter(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        txn: &KvTxn,
        options: ClusterKvTxnForwardOptions,
    ) -> Result<Option<ClusterKvTxnCommitResult>, NetworkError>
    where
        Self: Sized,
    {
        forward_cluster_kv_txn_to_voter_with_host(paths, state, txn, options, self)
    }

    fn elect_cluster_raft_leader(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        options: ClusterRaftLeaderElectionOptions,
    ) -> Result<bool, NetworkError>
    where
        Self: Sized,
    {
        elect_cluster_raft_leader_with_host(paths, state, options, self)
    }

    fn reachable_cluster_state_authority_count(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        options: ClusterStateAuthorityReachabilityOptions,
    ) -> usize;

    fn commit_cluster_raft_command_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        command: KvCommand,
        options: ClusterKvRaftQuorumCommitOptions,
    ) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError>;
}

impl<T> crate::kv_forward::ClusterKvTxnForwardHost for T
where
    T: ClusterKvTxnQuorumHost,
{
    fn known_raft_leader_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError> {
        ClusterKvTxnQuorumHost::known_raft_leader_id(self, paths, state)
    }

    fn send_object_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        ClusterKvTxnQuorumHost::send_object_rpc_to_node(
            self,
            state,
            target_node_id,
            request,
            timeout,
        )
    }
}

impl<T> crate::raft_election::ClusterRaftVoteTransportHost for T
where
    T: ClusterKvTxnQuorumHost,
{
    fn send_raft_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftVoteResponse, NetworkError> {
        ClusterKvTxnQuorumHost::send_raft_vote(self, endpoint, request, timeout)
    }
}

pub trait ClusterKvTxnQuorumAsyncHost:
    ClusterSignedEndpointRpcAsyncHost
    + ClusterRaftQuorumAsyncHost
    + ClusterStateAuthorityReachabilityAsyncHost
    + Send
{
    fn send_raft_vote_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftVoteResponse>;
}

impl<T> ClusterRaftVoteTransportAsyncHost for T
where
    T: ClusterKvTxnQuorumAsyncHost,
{
    fn send_raft_vote_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftVoteResponse> {
        ClusterKvTxnQuorumAsyncHost::send_raft_vote_async(self, endpoint, request, timeout)
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

    fn known_raft_leader_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError> {
        locked_cluster_raft_known_leader_id(paths, state)
    }

    fn send_object_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.transport
            .send_object_rpc_to_node(state, target_node_id, request, timeout)
    }

    fn send_raft_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftVoteResponse, NetworkError> {
        self.transport.send_raft_vote(endpoint, request, timeout)
    }

    fn local_is_raft_leader(
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

    fn commit_cluster_raft_command_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        command: KvCommand,
        options: ClusterKvRaftQuorumCommitOptions,
    ) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError> {
        commit_cluster_raft_command_with_control_plane_driver(paths, state, command, options, self)
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

    fn refresh_current_cluster_kv_store(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<(), Self::Error> {
        self.host
            .refresh_current_cluster_kv_store(self.paths, state)
    }

    fn local_is_raft_leader(&mut self, state: &ClusterBootstrapState) -> Result<bool, Self::Error> {
        self.host.local_is_raft_leader(self.paths, state)
    }

    fn forward_cluster_kv_txn_to_voter(
        &mut self,
        state: &ClusterBootstrapState,
        txn: &KvTxn,
    ) -> Result<Option<sinomo_kv::ClusterKvTxnCommitResult>, Self::Error> {
        self.host.forward_cluster_kv_txn_to_voter(
            self.paths,
            state,
            txn,
            self.options.forward_options,
        )
    }

    fn elect_cluster_raft_leader(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, Self::Error> {
        self.host
            .elect_cluster_raft_leader(self.paths, state, self.options.leader_election_options)
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

    fn semantic_txn_no_effect_response(
        &mut self,
        txn: &KvTxn,
    ) -> Result<Option<sinomo_kv::ClusterKvTxnCommitResult>, Self::Error> {
        Ok(
            cluster_semantic_txn_no_effect_response(self.paths, txn, OffsetDateTime::now_utc())?
                .map(
                    |(snapshot, txn_response)| sinomo_kv::ClusterKvTxnCommitResult {
                        snapshot,
                        txn_response: Some(txn_response),
                    },
                ),
        )
    }

    fn commit_cluster_raft_txn_with_quorum(
        &mut self,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<sinomo_kv::ClusterKvRaftQuorumCommitResult, Self::Error> {
        self.host.commit_cluster_raft_command_with_quorum(
            self.paths,
            state,
            KvCommand::Txn(txn),
            ClusterKvRaftQuorumCommitOptions {
                timeout: self.options.raft_append_timeout,
            },
        )
    }

    fn cluster_read_only_error(&mut self, detail: String) -> Self::Error {
        NetworkError::ClusterReadOnly { detail }
    }
}

pub fn commit_cluster_kv_txn_with_quorum_response_with_host(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
    options: ClusterKvTxnQuorumOptions,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<ClusterKvTxnCommitResult, NetworkError> {
    let allow_forwarding = options.allow_forwarding;
    let mut kv_host = NetworkClusterKvTxnQuorumHost {
        paths,
        options,
        host,
    };
    sinomo_kv::commit_cluster_kv_txn_with_quorum_with_host(
        state,
        txn,
        sinomo_kv::ClusterKvTxnQuorumDriverOptions { allow_forwarding },
        &mut kv_host,
    )
}

#[cfg(test)]
pub fn handle_cluster_kv_txn_request_with_host(
    paths: &ClusterPaths,
    txn: KvTxn,
    options: ClusterKvTxnQuorumOptions,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<KvTxnResponse, NetworkError> {
    let mut state = read_cluster_state_without_kv(paths).map_err(|error| match error {
        NetworkError::ClusterOrphan => NetworkError::ClusterReadOnly {
            detail: "local node has no cluster state".to_string(),
        },
        other => other,
    })?;
    let result = commit_cluster_kv_txn_with_quorum_response_with_host(
        paths, &mut state, txn, options, host,
    )?;
    write_imported_cluster_kv_snapshot_if_changed(paths, &result.snapshot)?;
    Ok(result.txn_response.unwrap_or(KvTxnResponse {
        succeeded: true,
        revision: result.snapshot.revision,
        responses: Vec::new(),
    }))
}

pub(crate) fn locked_cluster_raft_local_is_leader(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<bool, NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_local_is_leader(state)
        .map_err(Into::into)
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

async fn cluster_semantic_txn_no_effect_response_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    txn: &KvTxn,
    now: OffsetDateTime,
) -> Result<Option<(KvSnapshot, KvTxnResponse)>, NetworkError> {
    let paths = paths.clone();
    let txn = txn.clone();
    runtime
        .spawn_blocking(move || cluster_semantic_txn_no_effect_response(&paths, &txn, now))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster semantic txn no-effect check".to_string(),
            detail: format!("semantic txn no-effect task failed: {source}"),
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
        txn: &'a KvTxn,
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
            forward_cluster_kv_txn_to_voter_async_with_host(
                runtime, paths, state, txn, options, host,
            )
            .await
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

    fn semantic_txn_no_effect_response_async<'a>(
        &'a mut self,
        txn: &'a KvTxn,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        Option<sinomo_kv::ClusterKvTxnCommitResult>,
        Self::Error,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        Box::pin(async move {
            Ok(cluster_semantic_txn_no_effect_response_async(
                runtime,
                paths,
                txn,
                OffsetDateTime::now_utc(),
            )
            .await?
            .map(
                |(snapshot, txn_response)| sinomo_kv::ClusterKvTxnCommitResult {
                    snapshot,
                    txn_response: Some(txn_response),
                },
            ))
        })
    }

    fn commit_cluster_raft_txn_with_quorum_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
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
                KvCommand::Txn(txn),
                ClusterKvRaftQuorumCommitOptions {
                    timeout: options.raft_append_timeout,
                },
                options.raft_append_replication_options,
                host,
            )
            .await
        })
    }

    fn cluster_read_only_error(&mut self, detail: String) -> Self::Error {
        NetworkError::ClusterReadOnly { detail }
    }
}

pub async fn commit_cluster_kv_txn_with_quorum_response_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<ClusterKvTxnCommitResult, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let allow_forwarding = options.allow_forwarding;
    let mut kv_host = NetworkClusterKvTxnQuorumAsyncHost {
        runtime,
        paths,
        options,
        host,
    };
    let result = sinomo_kv::commit_cluster_kv_txn_with_quorum_async_with_host(
        state,
        txn,
        sinomo_kv::ClusterKvTxnQuorumDriverOptions { allow_forwarding },
        &mut kv_host,
    )
    .await?;
    // A forwarded commit observes the voter's post-commit snapshot; import it
    // when it advances the local view so the forwarding node reads its own
    // write immediately. The committed-append fan-out runs on the voter and
    // can miss this node (cold formation: no relay route back yet), and
    // re-committing the same payload is a semantic no-effect that triggers no
    // new fan-out — without this import a forwarded write can stay invisible
    // locally until the periodic snapshot pull.
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

pub async fn handle_cluster_kv_txn_request_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    txn: KvTxn,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<KvTxnResponse, NetworkError>
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
        runtime, paths, &mut state, txn, options, host,
    )
    .await?;
    write_imported_cluster_kv_snapshot_if_changed_async(runtime, paths, &result.snapshot).await?;
    Ok(result.txn_response.unwrap_or(KvTxnResponse {
        succeeded: true,
        revision: result.snapshot.revision,
        responses: Vec::new(),
    }))
}
