use crate::ClusterPaths;
use crate::error::NetworkError;
use crate::kv_local_store::{apply_cluster_raft_append, cluster_kv_raft_store};
use crate::local_membership::cluster_state_needs_join_registration;
use crate::local_state_store::{read_cluster_state, write_cluster_state};
use crate::member_store::merge_cluster_state_from_peer;
use crate::membership_rpc::ClusterAuthenticatedPeer;
use crate::prelude::*;
use crate::rpc::{ObjectRpcRequest, ObjectRpcResponse};
use sinomo_kv::{
    ClusterRaftAppendRequest, ClusterRaftAppendResponse, ClusterRaftVoteRequest,
    ClusterRaftVoteResponse,
};

pub(crate) fn handle_cluster_raft_rpc_request(
    paths: &ClusterPaths,
    request: ObjectRpcRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ObjectRpcResponse, NetworkError> {
    let route = request.route.clone();
    let Some(payload) = sinomo_kv::handle_cluster_raft_rpc_payload(
        request.route.as_str(),
        &request.body,
        |append| handle_locked_cluster_raft_append_request(paths, append, authenticated_peer),
        |vote| handle_locked_cluster_raft_vote_request(paths, vote, authenticated_peer),
    )?
    else {
        return Err(NetworkError::UnknownRoute { route });
    };
    Ok(ObjectRpcResponse::from_encoded_body(
        payload.route,
        payload.body,
    ))
}

pub async fn handle_cluster_kv_consensus_rpc_request_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: ObjectRpcRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ObjectRpcResponse, NetworkError> {
    let paths = paths.clone();
    let authenticated_peer = authenticated_peer.clone();
    runtime
        .spawn_blocking(move || {
            handle_cluster_raft_rpc_request(&paths, request, &authenticated_peer)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster kv consensus rpc".to_string(),
            detail: format!("cluster kv consensus rpc task failed: {source}"),
        })?
}

pub(crate) fn handle_cluster_raft_append_request(
    paths: &ClusterPaths,
    request: ClusterRaftAppendRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftAppendResponse, NetworkError> {
    let mut state = read_cluster_state(paths)?;
    if request.state.cluster_id != state.cluster_id {
        return cluster_raft_append_rejection(
            paths,
            &state,
            format!(
                "peer cluster `{}` does not match local cluster `{}`",
                request.state.cluster_id, state.cluster_id
            ),
        );
    }
    let leader_authorized_by_local_state =
        sinomo_kv::cluster_kv_raft_node_is_voter(&state, &request.append.leader_id)
            && cluster_authenticated_peer_matches_node(
                &state,
                authenticated_peer,
                &request.append.leader_id,
            );
    let leader_authorized_for_pending_join = cluster_state_needs_join_registration(&state)
        && request.append.leader_id == request.state.local_node_id
        && sinomo_kv::cluster_kv_raft_node_is_voter(&request.state, &request.append.leader_id)
        && cluster_authenticated_peer_matches_node(
            &request.state,
            authenticated_peer,
            &request.append.leader_id,
        );
    if !leader_authorized_by_local_state && !leader_authorized_for_pending_join {
        return cluster_raft_append_rejection(
            paths,
            &state,
            format!(
                "raft append rejected from unauthorized or mismatched leader {}",
                request.append.leader_id
            ),
        );
    }
    merge_cluster_state_from_peer(&mut state, request.state);
    write_cluster_state(paths, &state)?;
    let append = apply_cluster_raft_append(
        paths,
        &state.local_node_id,
        request.snapshot,
        request.append,
    )?;
    Ok(ClusterRaftAppendResponse {
        accepted: append.success,
        detail: if append.success {
            "raft append applied".to_string()
        } else {
            "raft append rejected by local log".to_string()
        },
        state,
        append,
    })
}

pub(crate) fn handle_locked_cluster_raft_append_request(
    paths: &ClusterPaths,
    request: ClusterRaftAppendRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftAppendResponse, NetworkError> {
    let _raft_guard = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&paths.kv_paths())?;
    handle_cluster_raft_append_request(paths, request, authenticated_peer)
}

fn cluster_raft_append_rejection(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    detail: String,
) -> Result<ClusterRaftAppendResponse, NetworkError> {
    Ok(ClusterRaftAppendResponse {
        accepted: false,
        detail,
        state: state.clone(),
        append: cluster_kv_raft_store(paths)
            .append_entries_rejection_response(&state.local_node_id)?,
    })
}

pub(crate) fn handle_cluster_raft_vote_request(
    paths: &ClusterPaths,
    request: ClusterRaftVoteRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftVoteResponse, NetworkError> {
    let mut state = read_cluster_state(paths)?;
    if request.state.cluster_id != state.cluster_id {
        return cluster_kv_raft_store(paths)
            .cluster_raft_vote_rejection_response(
                &state,
                format!(
                    "peer cluster `{}` does not match local cluster `{}`",
                    request.state.cluster_id, state.cluster_id
                ),
            )
            .map_err(Into::into);
    }
    let candidate_is_authorized_voter = sinomo_kv::cluster_kv_raft_candidate_is_authorized_voter(
        &state,
        &request.vote.candidate_id,
    );
    if !candidate_is_authorized_voter
        || !cluster_authenticated_peer_matches_node(
            &state,
            authenticated_peer,
            &request.vote.candidate_id,
        )
    {
        return cluster_kv_raft_store(paths)
            .cluster_raft_vote_rejection_response(
                &state,
                format!(
                    "raft vote rejected for unauthorized or mismatched candidate {}",
                    request.vote.candidate_id
                ),
            )
            .map_err(Into::into);
    }
    merge_cluster_state_from_peer(&mut state, request.state);
    write_cluster_state(paths, &state)?;
    cluster_kv_raft_store(paths)
        .apply_vote_response(&state, request.vote)
        .map_err(Into::into)
}

pub(crate) fn handle_locked_cluster_raft_vote_request(
    paths: &ClusterPaths,
    request: ClusterRaftVoteRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftVoteResponse, NetworkError> {
    let _raft_guard = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&paths.kv_paths())?;
    handle_cluster_raft_vote_request(paths, request, authenticated_peer)
}

pub(crate) fn cluster_authenticated_peer_matches_node(
    state: &ClusterBootstrapState,
    authenticated_peer: &ClusterAuthenticatedPeer,
    node_id: &NodeId,
) -> bool {
    state.members.iter().any(|member| {
        member.node_id == *node_id
            && member.cluster_id == state.cluster_id
            && member.state.is_authorized()
            && member.node_id.as_str() == authenticated_peer.node_id
            && member.node_public_key == authenticated_peer.public_key
    })
}
