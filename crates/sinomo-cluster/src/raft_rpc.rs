use crate::kv_local_store::ClusterKvPathsExt;
use crate::kv_local_store::apply_cluster_raft_append;
#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
use crate::local_state_store::{
    acquire_local_cluster_state_mutation_lock, project_cluster_raft_config_record,
    project_committed_cluster_raft_config, read_cluster_consensus_state,
    read_cluster_state_without_kv, read_cluster_state_without_kv_cached_shared,
    write_cluster_state_unlocked,
};
use crate::member_store::merge_cluster_state_from_peer;
use crate::membership_rpc::ClusterAuthenticatedPeer;
use crate::prelude::*;
use crate::raft_quorum::{
    ClusterRaftBlockingCancellationPolicy, run_cluster_raft_supervised_blocking,
};
use sinomo_kv::{
    ClusterKvRaftReadIndexProbeMetadata, ClusterKvRaftReadIndexProbeOutcome,
    ClusterRaftAppendRequest, ClusterRaftAppendResponse, ClusterRaftVoteRequest,
    ClusterRaftVoteResponse, RaftAppendEntriesResponse,
};
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{ObjectRpcRequest, ObjectRpcResponse};

fn publish_authenticated_cluster_raft_append_term(
    paths: &ClusterPaths,
    request: &ClusterRaftAppendRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<(), NetworkError> {
    let local_state = read_cluster_state_without_kv_cached_shared(paths)?;
    if request.state.cluster_id != local_state.cluster_id
        || request.append.term <= local_state.term
        || !sinomo_kv::cluster_kv_raft_node_is_voter(&local_state, &request.append.leader_id)
        || !cluster_authenticated_peer_matches_node(
            &local_state,
            authenticated_peer,
            &request.append.leader_id,
        )
    {
        return Ok(());
    }
    crate::raft_quorum::publish_cluster_raft_higher_term_reactively(
        paths,
        &local_state.local_node_id,
        request.append.term,
    )
}

fn publish_authenticated_cluster_raft_vote_term(
    paths: &ClusterPaths,
    request: &ClusterRaftVoteRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<(), NetworkError> {
    let local_state = read_cluster_state_without_kv_cached_shared(paths)?;
    if request.state.cluster_id != local_state.cluster_id
        || request.vote.term <= local_state.term
        || !sinomo_kv::cluster_kv_raft_candidate_is_authorized_voter(
            &local_state,
            &request.vote.candidate_id,
        )
        || !cluster_authenticated_peer_matches_node(
            &local_state,
            authenticated_peer,
            &request.vote.candidate_id,
        )
    {
        return Ok(());
    }
    crate::raft_quorum::publish_cluster_raft_higher_term_reactively(
        paths,
        &local_state.local_node_id,
        request.vote.term,
    )
}

fn publish_authenticated_cluster_raft_rpc_term(
    paths: &ClusterPaths,
    request: &ObjectRpcRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<(), NetworkError> {
    match request.route.as_str() {
        sinomo_kv::CLUSTER_RAFT_APPEND_ROUTE => {
            let request: ClusterRaftAppendRequest = request.decode_body()?;
            publish_authenticated_cluster_raft_append_term(paths, &request, authenticated_peer)
        }
        sinomo_kv::CLUSTER_RAFT_VOTE_ROUTE => {
            let request: ClusterRaftVoteRequest = request.decode_body()?;
            publish_authenticated_cluster_raft_vote_term(paths, &request, authenticated_peer)
        }
        _ => Ok(()),
    }
}

pub fn handle_cluster_raft_rpc_request(
    paths: &ClusterPaths,
    request: ObjectRpcRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ObjectRpcResponse, NetworkError> {
    publish_authenticated_cluster_raft_rpc_term(paths, &request, authenticated_peer)?;
    let route = request.route.clone();
    let Some(payload) = sinomo_kv::handle_cluster_raft_rpc_payload(
        request.route.as_str(),
        &request.body,
        |append| handle_locked_cluster_raft_append_request(paths, append, authenticated_peer),
        |vote| handle_locked_cluster_raft_vote_request(paths, vote, authenticated_peer),
        kv_err,
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
    // Decode/authenticate and raise a higher-term floor before this request can
    // wait in Tokio's blocking queue. Caller cancellation may cancel queued
    // request application, but cannot discard the independently persisted
    // network observation.
    publish_authenticated_cluster_raft_rpc_term(paths, &request, authenticated_peer)?;
    let paths = paths.clone();
    let authenticated_peer = authenticated_peer.clone();
    run_cluster_raft_supervised_blocking(
        runtime,
        None,
        ClusterRaftBlockingCancellationPolicy::CancelQueued,
        "consensus_rpc",
        "generic consensus request",
        "cluster kv consensus rpc",
        move || handle_cluster_raft_rpc_request(&paths, request, &authenticated_peer),
    )
    .await
}

pub async fn handle_cluster_raft_append_request_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: ClusterRaftAppendRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftAppendResponse, NetworkError> {
    publish_authenticated_cluster_raft_append_term(paths, &request, authenticated_peer)?;
    // Marked ReadIndex heartbeats touch only the cached bootstrap projection
    // and one resident-node mutex. Evaluate them on the async worker so a
    // saturated blocking pool cannot consume the authority budget before the
    // receiver even observes the probe.
    if let Some(response) =
        handle_cluster_raft_read_index_probe_request(paths, &request, authenticated_peer)?
    {
        return Ok(response);
    }
    let paths = paths.clone();
    let authenticated_peer = authenticated_peer.clone();
    run_cluster_raft_supervised_blocking(
        runtime,
        None,
        ClusterRaftBlockingCancellationPolicy::CancelQueued,
        "append_rpc",
        "durable append request",
        "cluster raft append rpc",
        move || {
            let _raft_guard =
                sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&paths.kv_paths())
                    .map_err(kv_err)?;
            handle_cluster_raft_append_request_durable(&paths, request, &authenticated_peer)
        },
    )
    .await
}

pub async fn handle_cluster_raft_vote_request_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: ClusterRaftVoteRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftVoteResponse, NetworkError> {
    publish_authenticated_cluster_raft_vote_term(paths, &request, authenticated_peer)?;
    let paths = paths.clone();
    let authenticated_peer = authenticated_peer.clone();
    run_cluster_raft_supervised_blocking(
        runtime,
        None,
        ClusterRaftBlockingCancellationPolicy::CancelQueued,
        "vote_rpc",
        "durable vote request",
        "cluster raft vote rpc",
        move || handle_locked_cluster_raft_vote_request(&paths, request, &authenticated_peer),
    )
    .await
}

#[cfg(any(test, feature = "test-support"))]
pub fn handle_cluster_raft_append_request(
    paths: &ClusterPaths,
    request: ClusterRaftAppendRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftAppendResponse, NetworkError> {
    publish_authenticated_cluster_raft_append_term(paths, &request, authenticated_peer)?;
    // Keep the test-support/direct entrypoint on the same ordering boundary as
    // the production RPC handlers. In particular, a higher-term publication
    // may start its durable closer immediately; taking the ordinary operation
    // gate here prevents state projection and AppendEntries application from
    // racing that resident-node owner and spuriously returning Busy.
    handle_locked_cluster_raft_append_request(paths, request, authenticated_peer)
}

fn handle_cluster_raft_append_request_durable(
    paths: &ClusterPaths,
    request: ClusterRaftAppendRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftAppendResponse, NetworkError> {
    let _mutation_lock = acquire_local_cluster_state_mutation_lock(paths)?;
    let mut state = read_cluster_consensus_state(paths)?;
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
    if !leader_authorized_by_local_state {
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
    project_committed_cluster_raft_config(paths, &mut state)?;
    write_cluster_state_unlocked(paths, &state)?;
    let append = apply_cluster_raft_append(
        paths,
        &state.local_node_id,
        request.snapshot,
        request.append,
    )?;
    // The append may itself have committed a new config. Persist its effective
    // role projection so restart/local demotion cannot resurrect stale voters.
    project_committed_cluster_raft_config(paths, &mut state)?;
    write_cluster_state_unlocked(paths, &state)?;
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
    if let Some(response) =
        handle_cluster_raft_read_index_probe_request(paths, &request, authenticated_peer)?
    {
        return Ok(response);
    }
    let _raft_guard =
        sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&paths.kv_paths()).map_err(kv_err)?;
    handle_cluster_raft_append_request_durable(paths, request, authenticated_peer)
}

fn marked_read_index_probe_response(
    state: ClusterBootstrapState,
    append: RaftAppendEntriesResponse,
    detail: impl Into<String>,
) -> ClusterRaftAppendResponse {
    ClusterRaftAppendResponse {
        accepted: append.success,
        detail: detail.into(),
        state,
        append,
    }
}

fn marked_read_index_probe_rejection(
    state: ClusterBootstrapState,
    term: u64,
    detail: impl Into<String>,
) -> ClusterRaftAppendResponse {
    marked_read_index_probe_response(
        state,
        RaftAppendEntriesResponse {
            term,
            success: false,
            // A marked probe never updates replication progress. In
            // particular, retained busy metadata is not a log-tail proof.
            match_index: 0,
        },
        detail,
    )
}

fn project_marked_read_index_probe_state(
    mut state: ClusterBootstrapState,
    metadata: &ClusterKvRaftReadIndexProbeMetadata,
) -> Result<ClusterBootstrapState, NetworkError> {
    if let Some(entry) = metadata.raft_config_entry.as_ref() {
        let config: sinomo_kv::ClusterKvRaftConfigRecord = entry.decode().map_err(kv_err)?;
        project_cluster_raft_config_record(&mut state, &config)?;
    }
    state.term = state.term.max(metadata.observed_term);
    state.kv_revision = state.kv_revision.max(metadata.revision.0);
    Ok(state)
}

/// Handle the empty heartbeat used only for ReadIndex without joining either
/// local durable mutation queue.
///
/// Only an exact idle resident node may acknowledge the term/log anchor. A
/// checked-out resident returns a healthy, non-progress RPC rejection when its
/// retained term already covers the request. Unknown or higher terms fall
/// through so ordinary AppendEntries can durably observe them.
fn handle_cluster_raft_read_index_probe_request(
    paths: &ClusterPaths,
    request: &ClusterRaftAppendRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<Option<ClusterRaftAppendResponse>, NetworkError> {
    if !request.read_index_probe {
        return Ok(None);
    }
    if request.snapshot.is_some()
        || !request.append.entries.is_empty()
        || request.append.leader_commit != request.append.prev_log_index
    {
        return Err(NetworkError::InvalidClusterCommand {
            detail: "marked Raft ReadIndex probe must be an empty heartbeat anchored exactly at its committed index"
                .to_string(),
        });
    }

    let local_state = read_cluster_state_without_kv_cached_shared(paths)?;
    if request.state.cluster_id != local_state.cluster_id {
        return Ok(Some(marked_read_index_probe_rejection(
            local_state.as_ref().clone(),
            local_state.term,
            format!(
                "peer cluster `{}` does not match local cluster `{}`",
                request.state.cluster_id, local_state.cluster_id
            ),
        )));
    }

    let outcome = paths
        .kv_paths()
        .raft_store()
        .read_index_probe(&local_state.local_node_id, &request.append)
        .map_err(kv_err)?;
    let (metadata, append, detail) = match outcome {
        ClusterKvRaftReadIndexProbeOutcome::NeedsDurable
            if request.append.term <= local_state.term =>
        {
            let leader_authorized_by_local_config =
                sinomo_kv::cluster_kv_raft_node_is_voter(&local_state, &request.append.leader_id)
                    && cluster_authenticated_peer_matches_node(
                        &local_state,
                        authenticated_peer,
                        &request.append.leader_id,
                    );
            return Ok(Some(marked_read_index_probe_rejection(
                local_state.as_ref().clone(),
                local_state.term,
                if leader_authorized_by_local_config {
                    "Raft ReadIndex probe rejected because resident state is unavailable"
                        .to_string()
                } else {
                    format!(
                        "Raft ReadIndex probe rejected from unauthorized or mismatched leader {}",
                        request.append.leader_id
                    )
                },
            )));
        }
        // A request above the locally projected term is a mandatory durable
        // safety observation. The preflight publisher has already raised the
        // process-local authority fence; retain the ordinary AppendEntries path
        // so hard state is advanced before any later acknowledgement.
        ClusterKvRaftReadIndexProbeOutcome::NeedsDurable => return Ok(None),
        ClusterKvRaftReadIndexProbeOutcome::Busy { metadata } => {
            let append = RaftAppendEntriesResponse {
                term: metadata.observed_term,
                success: false,
                match_index: 0,
            };
            (
                metadata,
                append,
                "Raft ReadIndex probe deferred while resident state is busy",
            )
        }
        ClusterKvRaftReadIndexProbeOutcome::Responded { response, metadata } => {
            let detail = if response.success {
                "Raft ReadIndex probe acknowledged from resident state"
            } else {
                "Raft ReadIndex probe rejected by resident state"
            };
            (metadata, response, detail)
        }
    };
    let projected_state =
        project_marked_read_index_probe_state(local_state.as_ref().clone(), &metadata)?;
    let leader_authorized_by_exact_config =
        sinomo_kv::cluster_kv_raft_node_is_voter(&projected_state, &request.append.leader_id)
            && cluster_authenticated_peer_matches_node(
                &projected_state,
                authenticated_peer,
                &request.append.leader_id,
            );
    if !leader_authorized_by_exact_config {
        return Ok(Some(marked_read_index_probe_rejection(
            projected_state,
            metadata.observed_term,
            format!(
                "Raft ReadIndex probe rejected from unauthorized or mismatched leader {}",
                request.append.leader_id
            ),
        )));
    }
    Ok(Some(marked_read_index_probe_response(
        projected_state,
        append,
        detail,
    )))
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
        append: paths
            .kv_paths()
            .raft_append_rejection_response(&state.local_node_id)
            .map_err(kv_err)?,
    })
}

fn handle_cluster_raft_vote_request_durable(
    paths: &ClusterPaths,
    request: ClusterRaftVoteRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
    operation_guard: &sinomo_kv::ClusterKvRaftOperationGuard,
) -> Result<ClusterRaftVoteResponse, NetworkError> {
    let _mutation_lock = acquire_local_cluster_state_mutation_lock(paths)?;
    let mut state = read_cluster_state_without_kv(paths)?;
    let local_node_id = state.local_node_id.clone();
    let peer_state = request.state;
    let candidate_id = request.vote.candidate_id.clone();
    let vote = paths
        .kv_paths()
        .raft_store()
        .apply_vote_after_config_validation(
            operation_guard,
            &local_node_id,
            request.vote,
            |config_entry| -> Result<
                sinomo_kv::ClusterKvRaftVoteDisposition<(ClusterBootstrapState, Option<String>)>,
                NetworkError,
            > {
                let committed_config: Option<sinomo_kv::ClusterKvRaftConfigRecord> = config_entry
                    .map(|entry| entry.decode().map_err(kv_err))
                    .transpose()?;
                if let Some(config) = committed_config.as_ref() {
                    project_cluster_raft_config_record(&mut state, config)?;
                }
                if peer_state.cluster_id != state.cluster_id {
                    let detail = format!(
                        "peer cluster `{}` does not match local cluster `{}`",
                        peer_state.cluster_id, state.cluster_id
                    );
                    return Ok(sinomo_kv::ClusterKvRaftVoteDisposition::Reject((
                        state,
                        Some(detail),
                    )));
                }
                let candidate_is_authorized_voter =
                    sinomo_kv::cluster_kv_raft_candidate_is_authorized_voter(&state, &candidate_id);
                if !candidate_is_authorized_voter
                    || !cluster_authenticated_peer_matches_node(
                        &state,
                        authenticated_peer,
                        &candidate_id,
                    )
                {
                    let detail = format!(
                        "raft vote rejected for unauthorized or mismatched candidate {candidate_id}"
                    );
                    return Ok(sinomo_kv::ClusterKvRaftVoteDisposition::Reject((
                        state,
                        Some(detail),
                    )));
                }
                merge_cluster_state_from_peer(&mut state, peer_state);
                // Peer topology is an identity/routing observation, not a
                // membership authority. Reapply the exact committed config so
                // the request cannot smuggle a voter-role transition.
                if let Some(config) = committed_config.as_ref() {
                    project_cluster_raft_config_record(&mut state, config)?;
                }
                write_cluster_state_unlocked(paths, &state)?;
                Ok(sinomo_kv::ClusterKvRaftVoteDisposition::Apply((
                    state, None,
                )))
            },
        )
        .map_err(kv_err)??;
    let (vote, (state, rejection_detail)) = vote;
    Ok(ClusterRaftVoteResponse {
        accepted: vote.vote_granted,
        detail: rejection_detail.unwrap_or_else(|| {
            if vote.vote_granted {
                "raft vote granted".to_string()
            } else {
                "raft vote rejected by local hard state".to_string()
            }
        }),
        state,
        vote,
    })
}

pub(crate) fn handle_locked_cluster_raft_vote_request(
    paths: &ClusterPaths,
    request: ClusterRaftVoteRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftVoteResponse, NetworkError> {
    let raft_guard =
        sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&paths.kv_paths()).map_err(kv_err)?;
    handle_cluster_raft_vote_request_durable(paths, request, authenticated_peer, &raft_guard)
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
