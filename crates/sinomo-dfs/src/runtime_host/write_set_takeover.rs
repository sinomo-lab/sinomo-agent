//! Takeover of a write-set shard's lost primary on Main: the member side of
//! corroboration.
//!
//! A write-set member that judges the primary lost asks every other
//! write-set member whether it still reaches that primary. This node answers
//! from its own retained record and its own network verdict, after asking
//! the network to verify the primary unless its verdict already settled that
//! there is no path. The answer takes no shard lock, lane or admission, so it
//! is given while the shard's mutation lanes are busy, and it never waits on a
//! DFS timer: the network bounds a verification episode.

use super::peer_verdicts::DfsPeerVerdicts;
use super::write_set_keeping::cluster_member_facts;
use super::*;
use crate::takeover_corroboration::{
    DfsMetadataTakeoverCorroborationReply, DfsMetadataTakeoverCorroborationRequest,
    DfsTakeoverAbstainReason, DfsTakeoverCorroborationKind, DfsTakeoverHandOffOutcome,
    DfsTakeoverPrimaryReach, DfsTakeoverRank, DfsTakeoverVerdictEvidence,
    takeover_corroboration_judgement, takeover_corroboration_record_check,
};
use crate::{DfsWriteSetMemberReach, DfsMetadataShardId};

/// Answer one corroboration request from the authenticated `caller`.
pub(crate) async fn answer_dfs_takeover_corroboration(
    data_dir: SinomoDataDir,
    runtime: tokio::runtime::Handle,
    caller: NodeId,
    request: DfsMetadataTakeoverCorroborationRequest,
) -> DfsMetadataTakeoverCorroborationReply {
    let cluster_paths = ClusterPaths::new(&data_dir);
    let Ok(state) =
        sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(&cluster_paths)
    else {
        return unavailable(&request, &caller);
    };
    let local = state.local_node_id.clone();
    let Ok(authority) = daemon_dfs_metadata_authority_generation_for_identity(
        &cluster_paths,
        &state.cluster_id,
        &local,
        Some(&runtime),
    ) else {
        return unavailable(&request, &local);
    };
    let snapshot = authority.configuration_snapshot();
    let context = snapshot
        .entries(crate::metadata_authority_runtime::DFS_METADATA_AUTHORITY_PREFIX)
        .and_then(|entries| {
            crate::metadata_replica_context_from_authority_entries(
                entries,
                &local,
                &request.file_system_id,
                &request.shard_id,
            )
            .ok()
            .flatten()
        });
    let view = match takeover_corroboration_record_check(
        &request,
        &caller,
        &local,
        context.as_ref().map(|context| &context.shard),
        context.as_ref().and_then(|context| context.write_set.as_ref()),
    ) {
        Ok(view) => view,
        Err((view, reason)) => {
            return DfsMetadataTakeoverCorroborationReply::abstain(&request, &local, view, reason);
        }
    };
    // A primary the published membership no longer authorizes has no verdict
    // at all; without the membership scopes nobody is judged departed.
    let members = cluster_member_facts(&snapshot);
    let departed = members.as_ref().is_some_and(|members| {
        members
            .get(&request.primary_node_id)
            .is_none_or(|member| member.departed)
    });
    let mobile = members
        .as_ref()
        .and_then(|members| members.get(&local))
        .is_some_and(|member| member.mobile);
    let mut verdicts = DfsPeerVerdicts::new(&data_dir, &cluster_paths, &state);
    verdicts.retain(true);
    let (answer, evidence) =
        judge_takeover_primary(&mut verdicts, &request.primary_node_id, departed).await;
    let durable_sequence = local_durable_sequence(&data_dir, &request.file_system_id, &request.shard_id).await;
    let hand_off = match &request.kind {
        DfsTakeoverCorroborationKind::Query => None,
        DfsTakeoverCorroborationKind::HandOff { ranking } => Some(decline_or_run_hand_off(
            &local,
            &answer,
            durable_sequence,
            mobile,
            &request.candidate_node_id,
            ranking,
        )),
    };
    DfsMetadataTakeoverCorroborationReply {
        attempt_id: request.attempt_id.clone(),
        member_node_id: local,
        record_view: Some(view),
        answer,
        durable_sequence,
        hand_off,
        verdict_evidence: Some(evidence),
    }
}

fn unavailable(
    request: &DfsMetadataTakeoverCorroborationRequest,
    member: &NodeId,
) -> DfsMetadataTakeoverCorroborationReply {
    DfsMetadataTakeoverCorroborationReply::abstain(
        request,
        member,
        None,
        DfsTakeoverAbstainReason::Unavailable,
    )
}

/// This node's judgement of `primary`. Unless its verdict already settled
/// `NoPath` it asks the network to verify the primary, then waits for an
/// episode that was running or requested when the question arrived to end, so
/// a verdict from edge records that have not aged out cannot veto a primary
/// that died. Without a probe owner the current settled verdict answers.
async fn judge_takeover_primary(
    verdicts: &mut DfsPeerVerdicts,
    primary: &NodeId,
    departed: bool,
) -> (DfsTakeoverPrimaryReach, DfsTakeoverVerdictEvidence) {
    let settled_lost = verdicts.current().reach(primary) == DfsWriteSetMemberReach::NoPath;
    let requested = !departed && !settled_lost && verdicts.request_verification(primary);
    let mut episode_seen = false;
    loop {
        let snapshot = verdicts.current();
        let (verdict, verifying) = snapshot.entry(primary);
        episode_seen |= verifying;
        let evidence = DfsTakeoverVerdictEvidence {
            snapshot_generation: snapshot.generation,
            verification_generation: snapshot.verification_generation,
        };
        if (departed || !requested || episode_seen)
            && let Some(answer) = takeover_corroboration_judgement(departed, verdict, verifying)
        {
            return (answer, evidence);
        }
        verdicts.changed().await;
    }
}

/// This node's durable journal head for the shard, from the last completed
/// resident status projection: the shard is not checked out for it.
async fn local_durable_sequence(
    data_dir: &SinomoDataDir,
    file_system_id: &crate::DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
) -> Option<u64> {
    let data_dir = data_dir.clone();
    let file_system_id = file_system_id.clone();
    let shard_id = shard_id.clone();
    tokio::task::spawn_blocking(move || {
        let cluster_paths = ClusterPaths::new(&data_dir);
        let peer_snapshot = read_daemon_dfs_message_peer_snapshot(&cluster_paths).ok()??;
        let resident = daemon_dfs_bounded_status_resident(&data_dir, &peer_snapshot).ok()?;
        let status = resident.bounded_status_snapshot().ok()?;
        status
            .metadata_shards
            .iter()
            .find(|shard| {
                shard.shard_id == shard_id
                    && shard.file_system_id.as_ref() == Some(&file_system_id)
            })
            .map(|shard| shard.journal_last_sequence)
    })
    .await
    .ok()
    .flatten()
}

/// A hand-off's outcome on this member. Running the takeover from the
/// asker's round belongs to the candidate driver; until it accepts the run,
/// a member that would run it declines without preference, and the asker
/// ranks again without it.
fn decline_or_run_hand_off(
    local: &NodeId,
    answer: &DfsTakeoverPrimaryReach,
    durable_sequence: Option<u64>,
    mobile: bool,
    asker: &NodeId,
    ranking: &[DfsTakeoverRank],
) -> DfsTakeoverHandOffOutcome {
    let Some(journal_sequence) = durable_sequence else {
        return DfsTakeoverHandOffOutcome::Declined {
            reason: "no-durable-head".into(),
            prefer: None,
        };
    };
    let own = DfsTakeoverRank {
        node_id: local.clone(),
        journal_sequence,
        mobile,
    };
    match crate::decide_metadata_takeover_hand_off(&own, answer, asker, ranking) {
        crate::DfsTakeoverHandOffDecision::Run => DfsTakeoverHandOffOutcome::Declined {
            reason: "no-driver".into(),
            prefer: None,
        },
        crate::DfsTakeoverHandOffDecision::Decline { reason, prefer } => {
            DfsTakeoverHandOffOutcome::Declined {
                reason: reason.into(),
                prefer,
            }
        }
    }
}

/// Ask `member` one corroboration question over direct object RPC. No
/// overall deadline applies: the network's connect and no-progress limits
/// end a call to a lost member, and a slow member's answer is waited for.
pub(super) async fn ask_dfs_takeover_corroboration(
    peer: Arc<dyn crate::AsyncDfsPeerTransport>,
    member: &NodeId,
    request: &DfsMetadataTakeoverCorroborationRequest,
) -> Result<DfsMetadataTakeoverCorroborationReply, crate::DfsPeerTransportError> {
    let call = sinomo_core::network::ObjectRpcCall::value(
        sinomo_core::network::ObjectRpcRequest::encode_body(
            crate::DFS_METADATA_TAKEOVER_CORROBORATION_ROUTE,
            crate::DFS_RPC_CATEGORY,
            request,
        )
        .map_err(crate::DfsPeerTransportError::from_network)?,
    );
    peer.call_object_rpc_async(member, call, crate::DfsObjectRpcSendOptions::default())
        .await?
        .decode_body::<DfsMetadataTakeoverCorroborationReply>()
        .await
        .map_err(crate::DfsPeerTransportError::from_network)
}

/// Test support: ask `member` from `asker`'s Main, as its candidate role does.
#[cfg(test)]
pub(crate) fn ask_dfs_takeover_corroboration_for_test(
    data_dir: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
    member: &NodeId,
    request: &DfsMetadataTakeoverCorroborationRequest,
) -> Result<DfsMetadataTakeoverCorroborationReply, crate::DfsPeerTransportError> {
    let (_, peer) = daemon_dfs_peer_transports_on_runtime(data_dir, runtime);
    runtime.block_on(ask_dfs_takeover_corroboration(peer, member, request))
}
