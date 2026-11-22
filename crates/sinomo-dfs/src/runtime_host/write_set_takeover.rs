//! Takeover of a write-set shard's lost primary on Main.
//!
//! The candidate role runs inside the keeper on every write-set member other
//! than the primary. When this node judges the primary lost (its network
//! settled that there is no path, or the primary left the cluster), it asks
//! the other write-set members whether they still reach the primary. Any
//! member that does vetoes the attempt. Otherwise the member with the longest
//! durable journal confirms the record with one authoritative read, promises
//! to accept nothing more at the read epoch, and submits the guarded swap
//! that makes it primary at the next epoch. An unknown swap is re-sent as the
//! identical request until its own outcome settles it. Once this node's
//! publication names it primary at a takeover epoch, activation rebinds its
//! store, aligns the write set with the epoch start and requests the
//! reconciliation that resumes the shard's work.
//!
//! The member side answers corroboration from this node's own retained record
//! and its own network verdict, after asking the network to verify the
//! primary unless its verdict already settled that there is no path. The
//! answer takes no shard lock, lane or admission, so it is given while the
//! shard's mutation lanes are busy, and it never waits on a DFS timer: the
//! network bounds a verification episode.
//!
//! Every wait is an event: a verdict change, a publication, a reply, or a
//! completion. The only timers re-send: an unknown swap, a failed confirm
//! read, and a request that the network examine the primary whose episode
//! concluded nothing. In each case no primary can commit and no publication
//! or verdict change may arrive to wake the candidate; a timer never decides
//! an outcome.

use super::peer_verdicts::DfsPeerVerdicts;
use super::write_set_keeping::cluster_member_facts;
use super::*;
use crate::takeover_corroboration::{
    DfsMetadataTakeoverCorroborationReply, DfsMetadataTakeoverCorroborationRequest,
    DfsTakeoverAbstainReason, DfsTakeoverCorroborationKind, DfsTakeoverHandOffOutcome,
    DfsTakeoverPrimaryReach, DfsTakeoverRank, DfsTakeoverVerdictEvidence,
    takeover_corroboration_judgement, takeover_corroboration_record_check,
};
use crate::{DfsMetadataShardId, DfsMetadataTakeoverEvent, DfsWriteSetMemberReach};
use std::collections::BTreeSet;

mod candidate;
mod steps;
pub(super) use candidate::{
    HAND_OFF_UNAVAILABLE, TAKEOVER_RESEND_FIRST, TakeoverCompletion, TakeoverRole, TakeoverShard,
};
#[cfg(any(test, feature = "test-support"))]
pub(crate) use steps::test_hooks::{
    hold_takeover_activation_for_test, stop_takeover_after_promise_for_test,
};

pub(super) type ShardKey = (crate::DfsFileSystemId, DfsMetadataShardId);

/// Event schema of the takeover events this component publishes.
pub(crate) const DFS_TAKEOVER_EVENT_SCHEMA_ID: &str = "sinomo.dfs.metadata-takeover";
pub(crate) const DFS_TAKEOVER_EVENT_SCHEMA_VERSION: &str = "1.0.0";

/// Every takeover event kind this component publishes.
pub(crate) const DFS_TAKEOVER_EVENT_KINDS: [&str; 10] = [
    DfsMetadataTakeoverEvent::CORROBORATION_STARTED,
    DfsMetadataTakeoverEvent::UNCORROBORATED_STARTED,
    DfsMetadataTakeoverEvent::CORROBORATION_VETOED,
    DfsMetadataTakeoverEvent::CORROBORATION_HANDED_OFF,
    DfsMetadataTakeoverEvent::PROMISED,
    DfsMetadataTakeoverEvent::SWAPPED,
    DfsMetadataTakeoverEvent::SWAP_SUPERSEDED,
    DfsMetadataTakeoverEvent::PROMISE_RELEASED,
    DfsMetadataTakeoverEvent::ACTIVATED,
    DfsMetadataTakeoverEvent::ALIGNED,
];

pub(crate) fn dfs_takeover_event_publication(
    kind: &str,
) -> sinomo_module::ComponentEventPublication {
    sinomo_module::ComponentEventPublication::new(
        kind,
        DFS_TAKEOVER_EVENT_SCHEMA_ID,
        DFS_TAKEOVER_EVENT_SCHEMA_VERSION,
    )
}

#[cfg(any(test, feature = "test-support"))]
static TAKEOVER_EVENTS_FOR_TEST: std::sync::LazyLock<
    std::sync::Mutex<BTreeMap<std::path::PathBuf, Vec<(String, DfsMetadataTakeoverEvent)>>>,
> = std::sync::LazyLock::new(Default::default);

#[cfg(any(test, feature = "test-support"))]
fn record_takeover_event_for_test(
    data_dir: &SinomoDataDir,
    name: &str,
    event: &DfsMetadataTakeoverEvent,
) {
    TAKEOVER_EVENTS_FOR_TEST
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .entry(data_dir.root())
        .or_default()
        .push((name.to_string(), event.clone()));
}

#[cfg(any(test, feature = "test-support"))]
static TAKEOVER_TRACE_FOR_TEST: std::sync::LazyLock<
    std::sync::Mutex<BTreeMap<std::path::PathBuf, Vec<String>>>,
> = std::sync::LazyLock::new(Default::default);

/// Test support: one line of what `data_dir`'s candidate role did.
#[cfg(any(test, feature = "test-support"))]
fn record_takeover_trace_for_test(data_dir: &SinomoDataDir, line: String) {
    TAKEOVER_TRACE_FOR_TEST
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .entry(data_dir.root())
        .or_default()
        .push(line);
}

/// Test support: what `root`'s candidate role did, for diagnostics.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn takeover_trace_for_test(root: &std::path::Path) -> Vec<String> {
    TAKEOVER_TRACE_FOR_TEST
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(root)
        .cloned()
        .unwrap_or_default()
}

/// Test support: the takeover events `root`'s node published, with names.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn takeover_events_for_test(
    root: &std::path::Path,
) -> Vec<(String, DfsMetadataTakeoverEvent)> {
    TAKEOVER_EVENTS_FOR_TEST
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(root)
        .cloned()
        .unwrap_or_default()
}

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
        context
            .as_ref()
            .and_then(|context| context.write_set.as_ref()),
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
    let durable_sequence =
        local_durable_sequence(&data_dir, &request.file_system_id, &request.shard_id).await;
    // This node has the asker's evidence: its own keeper evaluates the shard
    // and may be the member the ranking picks.
    if answer == DfsTakeoverPrimaryReach::Lost {
        crate::write_set_keeping_signals::wake_write_set_keeper(&local);
    }
    let hand_off = match &request.kind {
        DfsTakeoverCorroborationKind::Query => None,
        DfsTakeoverCorroborationKind::HandOff { ranking } => Some(
            run_or_decline_hand_off(&local, &request, &answer, durable_sequence, mobile, ranking)
                .await,
        ),
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
/// that died. Any end of that episode ends the wait, with or without a
/// conclusion: the answer is this node's verdict, not a certificate. Without
/// a probe owner the current settled verdict answers.
async fn judge_takeover_primary(
    verdicts: &mut DfsPeerVerdicts,
    primary: &NodeId,
    departed: bool,
) -> (DfsTakeoverPrimaryReach, DfsTakeoverVerdictEvidence) {
    let asked = verdicts.current();
    let settled_lost = asked.reach(primary) == DfsWriteSetMemberReach::NoPath;
    let settled_before = asked.settled_generation(primary);
    let requested = !departed && !settled_lost && verdicts.request_verification(primary);
    let mut episode_seen = false;
    loop {
        let snapshot = verdicts.current();
        let (verdict, verifying) = snapshot.entry(primary);
        // An episode this wait saw running, or one that settled since the
        // question arrived. The second also covers an episode that began and
        // settled between two wakes, which the first never sees.
        episode_seen |= snapshot.settled_generation(primary) > settled_before;
        let evidence = DfsTakeoverVerdictEvidence {
            snapshot_generation: snapshot.generation,
            verification_generation: snapshot.verification_generation,
        };
        if (departed
            || !requested
            || episode_seen
            || (!verifying && !verdicts.request_verification(primary)))
            && let Some(answer) = takeover_corroboration_judgement(departed, verdict, verifying)
        {
            return (answer, evidence);
        }
        verdicts.changed().await;
    }
}

/// This node's durable journal head sequence for the shard, from its journal
/// head without a shard lock or checkout. It only ranks candidates: the
/// promise captures the exact head under the writer lock.
async fn local_durable_sequence(
    data_dir: &SinomoDataDir,
    _file_system_id: &crate::DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
) -> Option<u64> {
    let root = crate::default_dfs_metadata_shards_root(data_dir)
        .join(crate::metadata_keys::key_segment(&shard_id.0));
    tokio::task::spawn_blocking(move || {
        root.exists()
            .then(|| crate::metadata_journal::read_next_journal_sequence_lenient(&root).ok())
            .flatten()
            .map(|next| next.saturating_sub(1))
    })
    .await
    .ok()
    .flatten()
}

/// A hand-off's outcome on this member: it runs the takeover from the
/// asker's round when it judges the primary lost and still ranks itself first
/// with its own head, and declines otherwise. The run belongs to this node's
/// candidate role, so one node never runs two attempts for a shard, and that
/// role holds the swap to the rule of an attempt of its own: this node's
/// no-path verdict must count under the record's epoch.
async fn run_or_decline_hand_off(
    local: &NodeId,
    request: &DfsMetadataTakeoverCorroborationRequest,
    answer: &DfsTakeoverPrimaryReach,
    durable_sequence: Option<u64>,
    mobile: bool,
    ranking: &[DfsTakeoverRank],
) -> DfsTakeoverHandOffOutcome {
    let decline = |reason: &str, prefer| DfsTakeoverHandOffOutcome::Declined {
        reason: reason.into(),
        prefer,
    };
    let Some(journal_sequence) = durable_sequence else {
        return decline("no-durable-head", None);
    };
    let own = DfsTakeoverRank {
        node_id: local.clone(),
        journal_sequence,
        mobile,
    };
    match crate::decide_metadata_takeover_hand_off(
        &own,
        answer,
        &request.candidate_node_id,
        ranking,
    ) {
        crate::DfsTakeoverHandOffDecision::Decline { reason, prefer } => decline(reason, prefer),
        crate::DfsTakeoverHandOffDecision::Run => {
            let Some(outcome) = post_takeover_hand_off(
                local,
                TakeoverHandOff {
                    key: (request.file_system_id.clone(), request.shard_id.clone()),
                    epoch: request.epoch,
                    primary: request.primary_node_id.clone(),
                },
            ) else {
                return decline(HAND_OFF_UNAVAILABLE, None);
            };
            outcome
                .await
                .unwrap_or_else(|_| decline(HAND_OFF_UNAVAILABLE, None))
        }
    }
}

/// A hand-off this node's candidate role runs from another member's round.
pub(super) struct TakeoverHandOff {
    pub(super) key: ShardKey,
    pub(super) epoch: u64,
    pub(super) primary: NodeId,
}

pub(super) type TakeoverHandOffReply = tokio::sync::oneshot::Sender<DfsTakeoverHandOffOutcome>;

static TAKEOVER_HAND_OFFS: std::sync::LazyLock<
    std::sync::Mutex<BTreeMap<NodeId, Vec<(TakeoverHandOff, TakeoverHandOffReply)>>>,
> = std::sync::LazyLock::new(Default::default);
static TAKEOVER_KEEPERS: std::sync::LazyLock<std::sync::Mutex<BTreeSet<NodeId>>> =
    std::sync::LazyLock::new(Default::default);

/// Hand `hand_off` to `local`'s keeper; `None` when no keeper runs here.
fn post_takeover_hand_off(
    local: &NodeId,
    hand_off: TakeoverHandOff,
) -> Option<tokio::sync::oneshot::Receiver<DfsTakeoverHandOffOutcome>> {
    if !TAKEOVER_KEEPERS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .contains(local)
    {
        return None;
    }
    let (reply, outcome) = tokio::sync::oneshot::channel();
    TAKEOVER_HAND_OFFS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .entry(local.clone())
        .or_default()
        .push((hand_off, reply));
    crate::write_set_keeping_signals::wake_write_set_keeper(local);
    Some(outcome)
}

/// The keeper of `local` runs from now until the guard drops.
pub(super) struct TakeoverKeeperRegistration(NodeId);

impl TakeoverKeeperRegistration {
    pub(super) fn new(local: &NodeId) -> Self {
        TAKEOVER_KEEPERS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(local.clone());
        Self(local.clone())
    }
}

impl Drop for TakeoverKeeperRegistration {
    fn drop(&mut self) {
        TAKEOVER_KEEPERS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.0);
        decline_takeover_hand_offs(&self.0);
    }
}

/// Answer every hand-off posted for `local`'s keeper that it cannot run now.
pub(super) fn decline_takeover_hand_offs(local: &NodeId) {
    for (_, reply) in take_takeover_hand_offs(local) {
        let _ = reply.send(DfsTakeoverHandOffOutcome::Declined {
            reason: HAND_OFF_UNAVAILABLE.into(),
            prefer: None,
        });
    }
}

/// Every hand-off posted for `local`'s keeper since it last took them.
pub(super) fn take_takeover_hand_offs(
    local: &NodeId,
) -> Vec<(TakeoverHandOff, TakeoverHandOffReply)> {
    TAKEOVER_HAND_OFFS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(local)
        .unwrap_or_default()
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
