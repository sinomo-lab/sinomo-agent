//! The candidate role: per-shard takeover attempts, driven from the keeper's
//! evaluation and from the completions of the steps they start.

use super::super::peer_verdicts::{DfsPeerVerdictSnapshot, DfsPeerVerdicts, StandingVerdict};
use super::super::write_set_keeping::MemberRecord;
use super::super::*;
use super::ShardKey;
use super::steps::{
    TakeOutcome, activate_metadata_takeover, align_metadata_takeover_members,
    read_takeover_promise, retire_takeover_fence, settle_takeover_promise, take_metadata_shard,
};
use crate::takeover_corroboration::{
    DFS_METADATA_TAKEOVER_CORROBORATION_FORMAT, DFS_TAKEOVER_HAND_OFF_PRIMARY_REACHED,
    DfsMetadataTakeoverCorroborationReply, DfsMetadataTakeoverCorroborationRequest,
    DfsTakeoverCorroborationKind, DfsTakeoverHandOffOutcome, DfsTakeoverPrimaryReach,
    DfsTakeoverRank,
};
use crate::{
    DfsMetadataShardConfig, DfsMetadataShardWriteSet, DfsMetadataShardWriteSetState,
    DfsMetadataTakeoverEvent, DfsMetadataTakeoverPromise, DfsMetadataTakeoverSettlement,
    DfsTakeoverAnswerSummary, DfsTakeoverRoundEntry, DfsTakeoverRoundOutcome, DfsTakeoverStanding,
    DfsWriteSetMemberFacts, DfsWriteSetMemberReach,
};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// The first re-send of an unknown swap; each later one doubles, up to the
/// network's own timeout. The timer only re-sends: it never decides.
pub(in crate::runtime_host) const TAKEOVER_RESEND_FIRST: Duration = Duration::from_millis(250);

/// One switched or switching shard this node is configured for.
pub(in crate::runtime_host) struct TakeoverShard {
    pub(in crate::runtime_host) key: ShardKey,
    pub(in crate::runtime_host) shard: DfsMetadataShardConfig,
    pub(in crate::runtime_host) state: DfsMetadataShardWriteSetState,
}

/// What one step the candidate role started ended with.
pub(in crate::runtime_host) enum TakeoverCompletion {
    LocalHead {
        key: ShardKey,
        attempt: String,
        journal_sequence: Option<u64>,
    },
    Answer {
        key: ShardKey,
        attempt: String,
        member: NodeId,
        reply: Option<DfsMetadataTakeoverCorroborationReply>,
    },
    HandOff {
        key: ShardKey,
        attempt: String,
        member: NodeId,
        reply: Option<DfsMetadataTakeoverCorroborationReply>,
    },
    Taken {
        key: ShardKey,
        attempt: String,
        outcome: TakeOutcome,
    },
    Resend {
        key: ShardKey,
        attempt: String,
    },
    Activated {
        key: ShardKey,
        epoch: u64,
        result: Result<Vec<(NodeId, crate::DfsWriteSetAlignment)>, String>,
    },
    Aligned {
        key: ShardKey,
        members: Vec<NodeId>,
        result: Result<Vec<(NodeId, crate::DfsWriteSetAlignment)>, String>,
    },
    /// This node's own confirm read after the member it handed off to could
    /// not reach cluster KV.
    Rechecked {
        key: ShardKey,
        attempt: String,
        outcome: Recheck,
    },
    /// The re-send timer of a confirm read that failed.
    Reconfirm {
        key: ShardKey,
        attempt: String,
    },
}

/// How this node's own confirm read ended.
pub(in crate::runtime_host) enum Recheck {
    Confirmed,
    /// The read itself failed.
    Unconfirmed,
    /// The record no longer names the primary at the attempt's epoch.
    Refused,
}

/// A hand-off whose target declined for availability, kept while this node
/// re-runs its own confirm read.
struct PendingHandOff {
    local: DfsTakeoverRank,
    to: NodeId,
    entries: BTreeMap<NodeId, DfsTakeoverRoundEntry>,
    declined: BTreeSet<NodeId>,
    request: DfsMetadataTakeoverCorroborationRequest,
}

/// A hand-off target's decline that says it could not reach cluster KV, or
/// could not run the attempt at all, rather than that it should not take
/// over. It may be momentary: the KV voters may still be electing.
fn declined_for_availability(outcome: &DfsTakeoverHandOffOutcome) -> bool {
    matches!(outcome, DfsTakeoverHandOffOutcome::Declined { reason, .. }
        if reason == HAND_OFF_NO_QUORUM || reason == HAND_OFF_UNAVAILABLE)
}

/// The decline of a hand-off target whose confirm read failed.
pub(in crate::runtime_host) const HAND_OFF_NO_QUORUM: &str = "no-quorum";
/// The decline of a hand-off target whose keeper could not evaluate it.
pub(in crate::runtime_host) const HAND_OFF_UNAVAILABLE: &str = "unavailable";
/// The decline of a hand-off target whose verdict for the primary is no
/// longer no-path but no path either: it has no evidence. It does not veto.
const HAND_OFF_NOT_LOST: &str = "primary-not-lost";
/// The decline of a hand-off target whose no-path verdict for the primary
/// already stood when it first read the epoch, with nothing examining the
/// primary: its swap would depose the primary on a verdict the network has
/// not examined under this record.
const HAND_OFF_UNEXAMINED: &str = "primary-unexamined";

struct Attempt {
    id: String,
    epoch: u64,
    generation: u64,
    primary: NodeId,
    cause: &'static str,
    promised: bool,
    /// Which members are mobile, for the ranking.
    mobile: BTreeSet<NodeId>,
    /// The asker of a hand-off this attempt runs, answered once it settles.
    reply: Option<tokio::sync::oneshot::Sender<DfsTakeoverHandOffOutcome>>,
    /// The next re-send interval of a confirm read that failed.
    reconfirm: Duration,
    step: Step,
}

enum Step {
    Corroborating {
        local: Option<DfsTakeoverRank>,
        pending: BTreeSet<NodeId>,
        entries: BTreeMap<NodeId, DfsTakeoverRoundEntry>,
        declined: BTreeSet<NodeId>,
    },
    HandingOff {
        local: DfsTakeoverRank,
        to: NodeId,
        entries: BTreeMap<NodeId, DfsTakeoverRoundEntry>,
        declined: BTreeSet<NodeId>,
        /// The target already declined once for availability, and this node
        /// then read the record itself: another such decline is final.
        rechecked: bool,
        request: DfsMetadataTakeoverCorroborationRequest,
    },
    /// The target declined for availability; this node reads the record
    /// itself before asking it once more.
    Rechecking {
        local: DfsTakeoverRank,
        to: NodeId,
        entries: BTreeMap<NodeId, DfsTakeoverRoundEntry>,
        declined: BTreeSet<NodeId>,
        request: DfsMetadataTakeoverCorroborationRequest,
    },
    Taking,
    /// The confirm read failed. It is re-run on the re-send timer while the
    /// attempt's trigger holds and its record is unchanged; the re-run's own
    /// outcome decides. `hand_off` is the declined hand-off the read rechecks.
    Unconfirmed {
        hand_off: Option<PendingHandOff>,
        due: bool,
    },
    Unknown {
        promise: DfsMetadataTakeoverPromise,
        interval: Duration,
        resending: bool,
    },
    /// The swap applied here, or the member this node handed off to runs the
    /// takeover: the next publication decides what comes next.
    Following {
        publication_generation: u64,
    },
}

/// The candidate role's attempts, and the activations it ran as a new
/// primary.
pub(in crate::runtime_host) struct TakeoverRole {
    local: NodeId,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    runtime: tokio::runtime::Handle,
    events: ComponentEventClient,
    done: tokio::sync::mpsc::UnboundedSender<TakeoverCompletion>,
    attempts: BTreeMap<ShardKey, Attempt>,
    standing: BTreeMap<ShardKey, DfsTakeoverStanding>,
    resumed: BTreeSet<ShardKey>,
    activated: BTreeMap<ShardKey, u64>,
    activating: BTreeSet<ShardKey>,
    aligning: BTreeSet<(ShardKey, NodeId)>,
    last_events: BTreeMap<ShardKey, sinomo_core::Id>,
    /// Activations that failed, with the inputs they saw: retried only once
    /// the publication or the verdicts move.
    activation_blocked: BTreeMap<ShardKey, (u64, u64, u64)>,
    /// Hand-offs waiting for the network to finish examining the primary.
    waiting_hand_offs: Vec<(super::TakeoverHandOff, super::TakeoverHandOffReply)>,
    publication_generation: u64,
    verdict_generation: u64,
}

impl TakeoverRole {
    pub(in crate::runtime_host) fn new(
        local: NodeId,
        data_dir: SinomoDataDir,
        runtime: tokio::runtime::Handle,
        events: ComponentEventClient,
        done: tokio::sync::mpsc::UnboundedSender<TakeoverCompletion>,
    ) -> Self {
        Self {
            local,
            cluster_paths: ClusterPaths::new(&data_dir),
            data_dir,
            runtime,
            events,
            done,
            attempts: BTreeMap::new(),
            standing: BTreeMap::new(),
            resumed: BTreeSet::new(),
            activated: BTreeMap::new(),
            activating: BTreeSet::new(),
            aligning: BTreeSet::new(),
            last_events: BTreeMap::new(),
            activation_blocked: BTreeMap::new(),
            waiting_hand_offs: Vec::new(),
            publication_generation: 0,
            verdict_generation: 0,
        }
    }

    /// Whether the candidate role needs this node's verdicts: it is a
    /// write-set member other than the primary of some switched shard.
    pub(in crate::runtime_host) fn needs_verdicts(&self, shards: &[TakeoverShard]) -> bool {
        shards.iter().any(|shard| {
            shard.shard.primary_node_id != self.local
                && shard
                    .state
                    .switched()
                    .is_some_and(|write_set| write_set.contains(&self.local))
        })
    }

    /// Plan and drive every shard. Returns the primaries a delivery failure
    /// asked to verify whose verdicts are neither settled lost nor verifying.
    pub(in crate::runtime_host) fn evaluate(
        &mut self,
        shards: &[TakeoverShard],
        members: &BTreeMap<NodeId, MemberRecord>,
        verdicts: &DfsPeerVerdictSnapshot,
        source: &mut DfsPeerVerdicts,
        unreachable_primaries: &BTreeSet<ShardKey>,
        publication_generation: u64,
    ) -> Vec<NodeId> {
        self.verdict_generation = verdicts.generation;
        self.publication_generation = publication_generation;
        let present = shards
            .iter()
            .map(|shard| shard.key.clone())
            .collect::<BTreeSet<_>>();
        self.attempts.retain(|key, attempt| {
            present.contains(key) || matches!(attempt.step, Step::Taking | Step::Unknown { .. })
        });
        self.standing.retain(|key, _| present.contains(key));
        let mut verify = Vec::new();
        for shard in shards {
            let Some(write_set) = shard.state.switched() else {
                self.drop_unless_settling(&shard.key);
                continue;
            };
            if shard.shard.primary_node_id == self.local {
                self.drop_unless_settling(&shard.key);
                // Activation follows this node's own settlement, so its swap
                // and promise end are recorded first.
                if !self.attempts.contains_key(&shard.key) {
                    self.activate_if_new(shard, write_set, verdicts);
                }
                continue;
            }
            if !write_set.contains(&self.local) {
                self.drop_unless_settling(&shard.key);
                // A promise on disk from before this process started is
                // settled by its own swap's outcome even once the record has
                // moved this node out of the write set: until then it accepts
                // nothing at the promised epoch, so it could not be re-added.
                if self.resumed.insert(shard.key.clone()) && !self.attempts.contains_key(&shard.key)
                {
                    self.resume_promise(shard);
                }
                continue;
            }
            let primary = &shard.shard.primary_node_id;
            if unreachable_primaries.contains(&shard.key) {
                let (_, verifying) = verdicts.entry(primary);
                if !verifying && verdicts.reach(primary) != DfsWriteSetMemberReach::NoPath {
                    verify.push(primary.clone());
                }
            }
            // A promise on disk from before this process started is settled
            // before anything else.
            if self.resumed.insert(shard.key.clone())
                && !self.attempts.contains_key(&shard.key)
                && self.resume_promise(shard)
            {
                continue;
            }
            let facts = takeover_member_facts(shard, members, verdicts);
            // Judged on every evaluation, so the epoch is noted when this
            // node first reads it as a write-set member, whatever else runs.
            let primary_examined =
                source.examined_under(verdicts, &shard.key, shard.shard.epoch, primary);
            if self.attempts.contains_key(&shard.key) {
                self.drive(shard, write_set, &facts, verdicts);
                continue;
            }
            let view = crate::DfsTakeoverView {
                local: &self.local,
                shard: &shard.shard,
                state: &shard.state,
                members: &facts,
                primary_examined,
                // A store that cannot promise from its exact materialized head
                // refuses inside the promise itself.
                store_ready: true,
                busy: false,
                standing: self.standing.get(&shard.key).copied(),
                publication_generation,
                verdict_generation: verdicts.generation,
            };
            let crate::DfsTakeoverPlan::Start { cause, asked } =
                crate::plan_metadata_takeover(&view)
            else {
                continue;
            };
            // A promise left by an earlier attempt is settled, never replaced.
            if self.resume_promise(shard) {
                continue;
            }
            self.start(shard, write_set, &facts, cause, asked, verdicts);
        }
        verify
    }

    /// Judge the hand-offs that arrived and the ones still waiting for the
    /// network to examine their primary.
    pub(in crate::runtime_host) fn hand_offs(
        &mut self,
        shards: &[TakeoverShard],
        members: Option<&BTreeMap<NodeId, MemberRecord>>,
        verdicts: &DfsPeerVerdictSnapshot,
        source: &mut DfsPeerVerdicts,
        arrived: Vec<(super::TakeoverHandOff, super::TakeoverHandOffReply)>,
    ) {
        let waiting = std::mem::take(&mut self.waiting_hand_offs);
        for (hand_off, reply) in waiting.into_iter().chain(arrived) {
            if let Some(waiting) = self.hand_off(shards, members, verdicts, source, hand_off, reply)
            {
                self.waiting_hand_offs.push(waiting);
            }
        }
    }

    /// Answer every waiting hand-off that this role cannot judge now. Its
    /// asker waits without a deadline.
    pub(in crate::runtime_host) fn decline_waiting_hand_offs(&mut self) {
        for (_, reply) in std::mem::take(&mut self.waiting_hand_offs) {
            let _ = reply.send(DfsTakeoverHandOffOutcome::Declined {
                reason: HAND_OFF_UNAVAILABLE.into(),
                prefer: None,
            });
        }
    }

    /// Run a hand-off from another member's round: that member ranked this
    /// node first. A round of this node's own gives way to it; an attempt
    /// already promising or settling answers that the asker should follow.
    ///
    /// This node's swap deposes the primary on this node's own verdict, so
    /// that verdict must count under the record's epoch exactly as for an
    /// attempt of this node's own; the asker's evidence is the asker's.
    /// While the network examines the primary the hand-off waits, and is
    /// returned to be judged again on the next evaluation: the episode's
    /// settlement is that event. A verdict nothing is examining declines
    /// the hand-off, and so does one that is no longer no-path; the asker
    /// then ranks again without this node, unless this node now reaches the
    /// primary, which vetoes the asker's attempt.
    fn hand_off(
        &mut self,
        shards: &[TakeoverShard],
        members: Option<&BTreeMap<NodeId, MemberRecord>>,
        verdicts: &DfsPeerVerdictSnapshot,
        source: &mut DfsPeerVerdicts,
        hand_off: super::TakeoverHandOff,
        reply: super::TakeoverHandOffReply,
    ) -> Option<(super::TakeoverHandOff, super::TakeoverHandOffReply)> {
        let decline = |reason: &str| DfsTakeoverHandOffOutcome::Declined {
            reason: reason.into(),
            prefer: None,
        };
        // The asker stopped waiting: its attempt ended. Whatever this node
        // does next is its own evaluation's.
        if reply.is_closed() {
            return None;
        }
        let Some(shard) = shards.iter().find(|shard| shard.key == hand_off.key) else {
            let _ = reply.send(decline("no-record"));
            return None;
        };
        if shard.shard.epoch != hand_off.epoch || shard.shard.primary_node_id != hand_off.primary {
            let _ = reply.send(DfsTakeoverHandOffOutcome::Followed);
            return None;
        }
        if !shard
            .state
            .switched()
            .is_some_and(|write_set| write_set.contains(&self.local))
            || shard.shard.primary_node_id == self.local
        {
            let _ = reply.send(decline("not-eligible"));
            return None;
        }
        // An attempt of this node's own already passed the same rule.
        if let Some(attempt) = self.attempts.get_mut(&shard.key) {
            match attempt.step {
                Step::Corroborating { .. }
                | Step::HandingOff { .. }
                | Step::Rechecking { .. }
                | Step::Unconfirmed { .. } => {
                    attempt.step = Step::Taking;
                    attempt.reply = Some(reply);
                    self.spawn_take(shard);
                }
                Step::Taking if attempt.reply.is_none() => attempt.reply = Some(reply),
                _ => {
                    let _ = reply.send(DfsTakeoverHandOffOutcome::Followed);
                }
            }
            return None;
        }
        if self.resume_promise(shard) {
            let _ = reply.send(DfsTakeoverHandOffOutcome::Followed);
            return None;
        }
        let write_set = shard.state.switched()?;
        // Departure is a membership fact cluster KV ordered; it needs no
        // examination. Without the membership scopes nobody is judged
        // departed.
        let primary = &shard.shard.primary_node_id;
        let departed = members
            .is_some_and(|members| members.get(primary).is_none_or(|member| member.departed));
        if !departed {
            let (verdict, verifying) = verdicts.entry(primary);
            let standing =
                source.standing_verdict(verdicts, &shard.key, shard.shard.epoch, primary);
            match (verdict, standing) {
                // An episode is running, or the one this node asked for is
                // pending: its settlement decides.
                _ if verifying => return Some((hand_off, reply)),
                (sinomo_core::network::PeerPathVerdict::NoPath, StandingVerdict::Counts) => {}
                (sinomo_core::network::PeerPathVerdict::NoPath, StandingVerdict::BeingExamined) => {
                    return Some((hand_off, reply));
                }
                (sinomo_core::network::PeerPathVerdict::NoPath, StandingVerdict::Unexamined) => {
                    let _ = reply.send(decline(HAND_OFF_UNEXAMINED));
                    return None;
                }
                (sinomo_core::network::PeerPathVerdict::Path, _) => {
                    let _ = reply.send(decline(DFS_TAKEOVER_HAND_OFF_PRIMARY_REACHED));
                    return None;
                }
                _ => {
                    let _ = reply.send(decline(HAND_OFF_NOT_LOST));
                    return None;
                }
            }
        }
        self.attempts.insert(
            shard.key.clone(),
            Attempt {
                id: sinomo_core::Id::generate().to_string(),
                epoch: shard.shard.epoch,
                generation: write_set.generation,
                primary: shard.shard.primary_node_id.clone(),
                cause: "hand-off",
                promised: false,
                mobile: BTreeSet::new(),
                reply: Some(reply),
                reconfirm: TAKEOVER_RESEND_FIRST,
                step: Step::Taking,
            },
        );
        self.spawn_take(shard);
        None
    }

    /// Forget an attempt the record moved past, unless its swap is still
    /// being settled: a promise is always ended by its own swap's outcome.
    fn drop_unless_settling(&mut self, key: &ShardKey) {
        if self
            .attempts
            .get(key)
            .is_some_and(|attempt| !matches!(attempt.step, Step::Taking | Step::Unknown { .. }))
        {
            self.attempts.remove(key);
        }
    }

    /// Resume an unsettled promise found on disk: re-send its identical swap.
    fn resume_promise(&mut self, shard: &TakeoverShard) -> bool {
        let Some(promise) = read_takeover_promise(&self.data_dir, &shard.key.1) else {
            return false;
        };
        #[cfg(any(test, feature = "test-support"))]
        super::record_takeover_trace_for_test(
            &self.data_dir,
            format!("resume promise {}", promise.swap_request_id),
        );
        if promise.cut.shard.file_system_id != shard.key.0 {
            return false;
        }
        let id = sinomo_core::Id::generate().to_string();
        self.attempts.insert(
            shard.key.clone(),
            Attempt {
                id: id.clone(),
                epoch: promise.cut.shard.epoch,
                generation: promise
                    .cut
                    .write_set_state()
                    .switched()
                    .map_or(0, |write_set| write_set.generation),
                primary: promise.cut.shard.primary_node_id.clone(),
                cause: "restart-resume",
                promised: true,
                mobile: BTreeSet::new(),
                reply: None,
                reconfirm: TAKEOVER_RESEND_FIRST,
                step: Step::Unknown {
                    promise: promise.clone(),
                    interval: TAKEOVER_RESEND_FIRST,
                    resending: true,
                },
            },
        );
        self.spawn_settle(shard.key.clone(), id, promise);
        true
    }

    fn start(
        &mut self,
        shard: &TakeoverShard,
        write_set: &DfsMetadataShardWriteSet,
        facts: &BTreeMap<NodeId, DfsWriteSetMemberFacts>,
        cause: crate::DfsTakeoverCause,
        asked: BTreeSet<NodeId>,
        verdicts: &DfsPeerVerdictSnapshot,
    ) {
        let id = sinomo_core::Id::generate().to_string();
        self.attempts.insert(
            shard.key.clone(),
            Attempt {
                id: id.clone(),
                epoch: shard.shard.epoch,
                generation: write_set.generation,
                primary: shard.shard.primary_node_id.clone(),
                cause: cause.as_str(),
                promised: false,
                reply: None,
                reconfirm: TAKEOVER_RESEND_FIRST,
                mobile: facts
                    .iter()
                    .filter(|(_, facts)| facts.mobile)
                    .map(|(node, _)| node.clone())
                    .collect(),
                step: if asked.is_empty() {
                    Step::Taking
                } else {
                    Step::Corroborating {
                        local: None,
                        pending: asked.clone(),
                        entries: BTreeMap::new(),
                        declined: BTreeSet::new(),
                    }
                },
            },
        );
        let mut started = self.event(&shard.key, None, Vec::new());
        started.node_ids = asked.iter().cloned().collect();
        let primary_evidence = if cause == crate::DfsTakeoverCause::PrimaryDeparted {
            format!(
                "sinomo-cluster:member/{}/departed",
                shard.shard.primary_node_id
            )
        } else {
            format!(
                "sinomo-network:peer-reachability/{}/{}/{}",
                self.local, shard.shard.primary_node_id, verdicts.generation
            )
        };
        self.publish(
            &shard.key,
            if asked.is_empty() {
                DfsMetadataTakeoverEvent::UNCORROBORATED_STARTED
            } else {
                DfsMetadataTakeoverEvent::CORROBORATION_STARTED
            },
            started,
            vec![
                primary_evidence,
                format!(
                    "dfs:write-set/{}/{}/{}/{}",
                    shard.key.0, shard.key.1.0, shard.shard.epoch, write_set.generation
                ),
            ],
        );
        if asked.is_empty() {
            self.spawn_take(shard);
            return;
        }
        self.spawn_local_head(shard.key.clone(), id.clone());
        for member in asked {
            let request = self.request(&shard.key, DfsTakeoverCorroborationKind::Query);
            if let Some(request) = request {
                self.spawn_ask(shard.key.clone(), member, request, false);
            }
        }
    }

    fn request(
        &self,
        key: &ShardKey,
        kind: DfsTakeoverCorroborationKind,
    ) -> Option<DfsMetadataTakeoverCorroborationRequest> {
        let attempt = self.attempts.get(key)?;
        Some(DfsMetadataTakeoverCorroborationRequest {
            format: DFS_METADATA_TAKEOVER_CORROBORATION_FORMAT,
            attempt_id: attempt.id.clone(),
            kind,
            file_system_id: key.0.clone(),
            shard_id: key.1.clone(),
            epoch: attempt.epoch,
            write_set_generation: attempt.generation,
            primary_node_id: attempt.primary.clone(),
            candidate_node_id: self.local.clone(),
        })
    }

    /// Advance an attempt on a wake that is not its own completion.
    fn drive(
        &mut self,
        shard: &TakeoverShard,
        write_set: &DfsMetadataShardWriteSet,
        facts: &BTreeMap<NodeId, DfsWriteSetMemberFacts>,
        verdicts: &DfsPeerVerdictSnapshot,
    ) {
        enum Next {
            Nothing,
            Drop,
            /// Drop a waiting confirm read and its standing.
            End,
            Decide,
            Reconfirm,
        }
        let key = &shard.key;
        let Some(attempt) = self.attempts.get_mut(key) else {
            return;
        };
        let moved = shard.shard.epoch != attempt.epoch;
        let primary_reached = facts
            .get(&attempt.primary)
            .is_some_and(|facts| !facts.departed && facts.reach == DfsWriteSetMemberReach::Path);
        // The trigger that started the attempt: a settled no-path verdict for
        // the primary, or its departure.
        let triggered = facts
            .get(&attempt.primary)
            .is_some_and(|facts| facts.departed)
            || verdicts.reach(&attempt.primary) == DfsWriteSetMemberReach::NoPath;
        let next = match &mut attempt.step {
            Step::Taking | Step::Unknown { .. } => Next::Nothing,
            // A failed confirm read is re-run only while nothing it depends on
            // moved: any record or verdict change ends it.
            Step::Unconfirmed { due, .. } => {
                if moved || write_set.generation != attempt.generation || !triggered {
                    Next::End
                } else if *due {
                    Next::Reconfirm
                } else {
                    Next::Nothing
                }
            }
            Step::Following {
                publication_generation,
            } => {
                if moved
                    || write_set.generation != attempt.generation
                    || *publication_generation != self.publication_generation
                {
                    Next::Drop
                } else {
                    Next::Nothing
                }
            }
            _ if moved || primary_reached => Next::Drop,
            Step::Corroborating {
                pending, entries, ..
            } => {
                // A member this node no longer reaches is left out.
                for member in pending.clone() {
                    if verdicts.reach(&member) == DfsWriteSetMemberReach::NoPath {
                        pending.remove(&member);
                        entries.insert(member, DfsTakeoverRoundEntry::LeftOut);
                    }
                }
                Next::Decide
            }
            Step::HandingOff { .. } | Step::Rechecking { .. } => Next::Nothing,
        };
        match next {
            Next::Nothing => {}
            Next::Drop => {
                self.attempts.remove(key);
            }
            Next::End => {
                self.end_standing(key);
                self.attempts.remove(key);
            }
            Next::Decide => self.decide_if_complete(shard),
            Next::Reconfirm => {
                let Step::Unconfirmed { hand_off, .. } =
                    std::mem::replace(&mut attempt.step, Step::Taking)
                else {
                    return;
                };
                match hand_off {
                    Some(PendingHandOff {
                        local,
                        to,
                        entries,
                        declined,
                        request,
                    }) => {
                        attempt.step = Step::Rechecking {
                            local,
                            to,
                            entries,
                            declined,
                            request,
                        };
                        self.spawn_recheck(shard);
                    }
                    None => self.spawn_take(shard),
                }
            }
        }
    }

    /// Wait to re-run a confirm read that failed: re-sent after the attempt's
    /// interval, which doubles to the network timeout. The timer only wakes
    /// the attempt; `drive` re-runs the read while the trigger still holds.
    fn wait_to_reconfirm(&mut self, key: &ShardKey, hand_off: Option<PendingHandOff>) {
        let Some(current) = self.attempts.get_mut(key) else {
            return;
        };
        let interval = current.reconfirm;
        current.reconfirm = interval
            .saturating_mul(2)
            .min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
        current.step = Step::Unconfirmed {
            hand_off,
            due: false,
        };
        let done = self.done.clone();
        let (key, attempt) = (key.clone(), current.id.clone());
        self.runtime.spawn(async move {
            tokio::time::sleep(interval).await;
            let _ = done.send(TakeoverCompletion::Reconfirm { key, attempt });
        });
    }

    /// Decide the round once every asked member settled and the local head
    /// is known.
    fn decide_if_complete(&mut self, shard: &TakeoverShard) {
        let Some(Attempt {
            step:
                Step::Corroborating {
                    local: Some(local),
                    pending,
                    entries,
                    declined,
                },
            ..
        }) = self.attempts.get(&shard.key)
        else {
            return;
        };
        if !pending.is_empty() {
            return;
        }
        let (local, entries, declined) = (local.clone(), entries.clone(), declined.clone());
        self.decide(shard, local, entries, declined);
    }

    fn decide(
        &mut self,
        shard: &TakeoverShard,
        local: DfsTakeoverRank,
        entries: BTreeMap<NodeId, DfsTakeoverRoundEntry>,
        declined: BTreeSet<NodeId>,
    ) {
        let key = shard.key.clone();
        let answers = answer_summaries(&entries);
        match crate::decide_metadata_takeover_round(&local, &entries, &declined) {
            DfsTakeoverRoundOutcome::Vetoed { by } => self.veto(&key, by, answers),
            DfsTakeoverRoundOutcome::Follow => {
                self.end_standing(&key);
                self.attempts.remove(&key);
            }
            DfsTakeoverRoundOutcome::Proceed => {
                if let Some(attempt) = self.attempts.get_mut(&key) {
                    attempt.step = Step::Taking;
                }
                self.spawn_take(shard);
            }
            DfsTakeoverRoundOutcome::HandOff { to, ranking } => {
                let Some(request) =
                    self.request(&key, DfsTakeoverCorroborationKind::HandOff { ranking })
                else {
                    return;
                };
                if let Some(attempt) = self.attempts.get_mut(&key) {
                    attempt.step = Step::HandingOff {
                        local,
                        to: to.clone(),
                        entries,
                        declined,
                        rechecked: false,
                        request: request.clone(),
                    };
                }
                let mut event = self.event(&key, None, answers);
                event.node_ids = vec![to.clone()];
                self.publish(
                    &key,
                    DfsMetadataTakeoverEvent::CORROBORATION_HANDED_OFF,
                    event,
                    Vec::new(),
                );
                self.spawn_ask(key, to, request, true);
            }
        }
    }

    /// End the attempt because `by` reaches the primary. The veto ends this
    /// attempt only: a new one starts once the record, the publication or
    /// this node's verdicts move.
    fn veto(&mut self, key: &ShardKey, by: NodeId, answers: Vec<DfsTakeoverAnswerSummary>) {
        self.end_standing(key);
        let mut event = self.event(key, None, answers);
        event.node_ids = vec![by];
        self.publish(
            key,
            DfsMetadataTakeoverEvent::CORROBORATION_VETOED,
            event,
            Vec::new(),
        );
        self.attempts.remove(key);
    }

    /// Record that this attempt ended on its own evidence: no new attempt
    /// starts until the record or this node's verdicts move.
    fn end_standing(&mut self, key: &ShardKey) {
        if let Some(attempt) = self.attempts.get(key) {
            self.standing.insert(
                key.clone(),
                DfsTakeoverStanding {
                    epoch: attempt.epoch,
                    generation: attempt.generation,
                    publication_generation: self.publication_generation,
                    verdict_generation: self.verdict_generation,
                },
            );
        }
    }

    /// Handle one completion. Returns the event the keeper's primary role
    /// names as the cause of its next event for the shard.
    pub(in crate::runtime_host) fn complete(
        &mut self,
        completion: TakeoverCompletion,
        shards: &[TakeoverShard],
    ) -> Option<(ShardKey, sinomo_core::Id)> {
        let shard_for = |key: &ShardKey| shards.iter().find(|shard| &shard.key == key);
        match completion {
            TakeoverCompletion::LocalHead {
                key,
                attempt,
                journal_sequence,
            } => {
                if let Some(current) = self.attempts.get_mut(&key)
                    && current.id == attempt
                {
                    let mobile = current.mobile.contains(&self.local);
                    if let Step::Corroborating { local, .. } = &mut current.step {
                        *local = Some(DfsTakeoverRank {
                            node_id: self.local.clone(),
                            journal_sequence: journal_sequence.unwrap_or(0),
                            mobile,
                        });
                    }
                }
                if let Some(shard) = shard_for(&key) {
                    self.decide_if_complete(shard);
                }
            }
            TakeoverCompletion::Answer {
                key,
                attempt,
                member,
                reply,
            } => {
                if let Some(current) = self.attempts.get_mut(&key)
                    && current.id == attempt
                {
                    let mobile = current.mobile.contains(&member);
                    if let Step::Corroborating {
                        pending, entries, ..
                    } = &mut current.step
                        && pending.remove(&member)
                    {
                        entries.insert(
                            member,
                            reply.map_or(DfsTakeoverRoundEntry::LeftOut, |reply| {
                                DfsTakeoverRoundEntry::Replied {
                                    answer: reply.answer,
                                    durable_sequence: reply.durable_sequence,
                                    mobile,
                                }
                            }),
                        );
                    }
                }
                if let Some(shard) = shard_for(&key) {
                    self.decide_if_complete(shard);
                }
            }
            TakeoverCompletion::HandOff {
                key,
                attempt,
                member,
                reply,
            } => {
                #[cfg(any(test, feature = "test-support"))]
                super::record_takeover_trace_for_test(
                    &self.data_dir,
                    format!(
                        "hand-off {attempt} to {member}: {:?}",
                        reply.as_ref().map(|reply| &reply.hand_off)
                    ),
                );
                let Some(Attempt {
                    id,
                    step:
                        Step::HandingOff {
                            local,
                            to,
                            entries,
                            declined,
                            rechecked,
                            request,
                        },
                    ..
                }) = self.attempts.get(&key)
                else {
                    return None;
                };
                if id != &attempt || to != &member {
                    return None;
                }
                // A member that reaches the primary vetoes the attempt, as in
                // the round: one that answered lost from a verdict that stood
                // may find a path while it holds the hand-off.
                if let Some(reply) = reply.as_ref().filter(|reply| reply.reaches_primary()) {
                    let mut entries = entries.clone();
                    let mobile = self
                        .attempts
                        .get(&key)
                        .is_some_and(|current| current.mobile.contains(&member));
                    entries.insert(
                        member.clone(),
                        DfsTakeoverRoundEntry::Replied {
                            answer: DfsTakeoverPrimaryReach::Reaches,
                            durable_sequence: reply.durable_sequence,
                            mobile,
                        },
                    );
                    self.veto(&key, member, answer_summaries(&entries));
                    return None;
                }
                let outcome = reply.and_then(|reply| reply.hand_off);
                match outcome {
                    Some(
                        DfsTakeoverHandOffOutcome::Swapped | DfsTakeoverHandOffOutcome::Followed,
                    ) => {
                        let publication_generation = self.publication_generation;
                        if let Some(current) = self.attempts.get_mut(&key) {
                            current.step = Step::Following {
                                publication_generation,
                            };
                        }
                    }
                    // The higher-ranked member could not reach cluster KV,
                    // which may be momentary. This node reads the record
                    // itself first: if it cannot either, neither proceeds;
                    // if it can, a quorum is serving and the target is asked
                    // once more before this node takes over in its place.
                    Some(outcome) if declined_for_availability(&outcome) && !rechecked => {
                        let step = Step::Rechecking {
                            local: local.clone(),
                            to: to.clone(),
                            entries: entries.clone(),
                            declined: declined.clone(),
                            request: request.clone(),
                        };
                        if let Some(current) = self.attempts.get_mut(&key) {
                            current.step = step;
                        }
                        if let Some(shard) = shard_for(&key) {
                            self.spawn_recheck(shard);
                        }
                    }
                    // Any other decline, a second availability decline or a
                    // failed call: rank again without it. None of them
                    // reaches the primary.
                    _ => {
                        let (local, entries) = (local.clone(), entries.clone());
                        let mut declined = declined.clone();
                        declined.insert(member);
                        if let Some(shard) = shard_for(&key) {
                            self.decide(shard, local, entries, declined);
                        }
                    }
                }
            }
            TakeoverCompletion::Taken {
                key,
                attempt,
                outcome,
            } => {
                let epoch = shard_for(&key).map(|shard| shard.shard.epoch);
                self.taken(key, attempt, outcome, epoch);
            }
            TakeoverCompletion::Rechecked {
                key,
                attempt,
                outcome,
            } => {
                #[cfg(any(test, feature = "test-support"))]
                super::record_takeover_trace_for_test(
                    &self.data_dir,
                    format!(
                        "recheck {attempt}: confirmed={}",
                        match outcome {
                            Recheck::Confirmed => "true",
                            Recheck::Unconfirmed => "unconfirmed",
                            Recheck::Refused => "false",
                        }
                    ),
                );
                let current = self.attempts.get_mut(&key)?;
                if current.id != attempt {
                    return None;
                }
                let Step::Rechecking {
                    local,
                    to,
                    entries,
                    declined,
                    request,
                } = std::mem::replace(&mut current.step, Step::Taking)
                else {
                    return None;
                };
                match outcome {
                    Recheck::Confirmed => {}
                    // Neither can reach cluster KV now: this node re-runs its
                    // read on the re-send timer, and asks the target again
                    // once it can.
                    Recheck::Unconfirmed => {
                        let hand_off = PendingHandOff {
                            local,
                            to,
                            entries,
                            declined,
                            request,
                        };
                        self.wait_to_reconfirm(&key, Some(hand_off));
                        return None;
                    }
                    Recheck::Refused => {
                        self.end_standing(&key);
                        self.attempts.remove(&key);
                        return None;
                    }
                }
                current.step = Step::HandingOff {
                    local,
                    to: to.clone(),
                    entries,
                    declined,
                    rechecked: true,
                    request: request.clone(),
                };
                self.spawn_ask(key, to, request, true);
            }
            TakeoverCompletion::Reconfirm { key, attempt } => {
                if let Some(current) = self.attempts.get_mut(&key)
                    && current.id == attempt
                    && let Step::Unconfirmed { due, .. } = &mut current.step
                {
                    // `drive` re-runs it on the evaluation this wake causes.
                    *due = true;
                }
            }
            TakeoverCompletion::Resend { key, attempt } => {
                if let Some(current) = self.attempts.get_mut(&key)
                    && current.id == attempt
                    && let Step::Unknown {
                        promise, resending, ..
                    } = &mut current.step
                    && !*resending
                {
                    *resending = true;
                    let promise = promise.clone();
                    self.spawn_settle(key, attempt, promise);
                }
            }
            TakeoverCompletion::Activated { key, epoch, result } => {
                self.activating.remove(&key);
                match result {
                    Ok(aligned) => {
                        self.activated.insert(key.clone(), epoch);
                        return self.activated_events(&key, shard_for(&key), &aligned);
                    }
                    Err(error) => {
                        tracing::debug!(target: "sinomo.dfs.write_set", %error, shard = %key.1.0, "takeover activation not completed");
                        self.activation_blocked.insert(
                            key,
                            (epoch, self.publication_generation, self.verdict_generation),
                        );
                    }
                }
            }
            TakeoverCompletion::Aligned {
                key,
                members,
                result,
            } => {
                for member in members {
                    self.aligning.remove(&(key.clone(), member));
                }
                match result {
                    Ok(aligned) => self.aligned_events(&key, shard_for(&key), &aligned),
                    Err(error) => {
                        tracing::debug!(target: "sinomo.dfs.write_set", %error, shard = %key.1.0, "members not aligned");
                    }
                }
            }
        }
        None
    }

    fn taken(
        &mut self,
        key: ShardKey,
        attempt: String,
        outcome: TakeOutcome,
        record_epoch: Option<u64>,
    ) {
        #[cfg(any(test, feature = "test-support"))]
        super::record_takeover_trace_for_test(
            &self.data_dir,
            format!(
                "taken {attempt} current={:?} outcome={}",
                self.attempts.get(&key).map(|current| current.id.clone()),
                match &outcome {
                    TakeOutcome::NotTaken { follow, reason } =>
                        format!("not-taken follow={follow} {reason}"),
                    TakeOutcome::Unconfirmed { reason } =>
                        format!("not-taken unconfirmed {reason}"),
                    TakeOutcome::Failed { reason } => format!("failed {reason}"),
                    TakeOutcome::Settled { settlement, .. } => format!("settled {settlement:?}"),
                }
            ),
        );
        if self
            .attempts
            .get(&key)
            .is_none_or(|current| current.id != attempt)
        {
            return;
        }
        let reply = self
            .attempts
            .get_mut(&key)
            .and_then(|current| current.reply.take());
        let served = reply.is_some();
        if let Some(reply) = reply {
            let _ = reply.send(match &outcome {
                TakeOutcome::Settled { settlement, .. } => match settlement {
                    DfsMetadataTakeoverSettlement::Applied => DfsTakeoverHandOffOutcome::Swapped,
                    DfsMetadataTakeoverSettlement::RejectedEpochUnchanged => {
                        DfsTakeoverHandOffOutcome::Declined {
                            reason: "released".into(),
                            prefer: None,
                        }
                    }
                    // A superseded swap or one still re-sent: the asker waits
                    // for the publication.
                    DfsMetadataTakeoverSettlement::RejectedEpochAdvanced
                    | DfsMetadataTakeoverSettlement::Unknown { .. } => {
                        DfsTakeoverHandOffOutcome::Followed
                    }
                },
                TakeOutcome::NotTaken { follow: true, .. } => DfsTakeoverHandOffOutcome::Followed,
                TakeOutcome::NotTaken { .. } | TakeOutcome::Unconfirmed { .. } => {
                    DfsTakeoverHandOffOutcome::Declined {
                        reason: HAND_OFF_NO_QUORUM.into(),
                        prefer: None,
                    }
                }
                TakeOutcome::Failed { reason } => DfsTakeoverHandOffOutcome::Declined {
                    reason: reason.clone(),
                    prefer: None,
                },
            });
        }
        let (promise, settlement) = match outcome {
            // This node's own attempt re-runs a failed confirm read. A
            // hand-off it ran for an asker ends: the asker rechecks.
            TakeOutcome::Unconfirmed { reason } if !served => {
                tracing::debug!(target: "sinomo.dfs.write_set", %reason, shard = %key.1.0, "takeover confirm read failed; re-sending");
                self.wait_to_reconfirm(&key, None);
                return;
            }
            TakeOutcome::Unconfirmed { reason } => {
                tracing::debug!(target: "sinomo.dfs.write_set", %reason, shard = %key.1.0, "takeover not confirmed");
                self.end_standing(&key);
                self.attempts.remove(&key);
                return;
            }
            TakeOutcome::NotTaken { follow, reason } => {
                tracing::debug!(target: "sinomo.dfs.write_set", %reason, follow, shard = %key.1.0, "takeover not confirmed");
                self.end_standing(&key);
                self.attempts.remove(&key);
                return;
            }
            TakeOutcome::Failed { reason } => {
                tracing::debug!(target: "sinomo.dfs.write_set", %reason, shard = %key.1.0, "takeover attempt failed");
                self.end_standing(&key);
                self.attempts.remove(&key);
                return;
            }
            TakeOutcome::Settled {
                promise,
                settlement,
            } => (promise, settlement),
        };
        let first = self
            .attempts
            .get_mut(&key)
            .is_some_and(|current| !std::mem::replace(&mut current.promised, true));
        let request = format!("sinomo-kv:request/{}", promise.swap_request_id);
        if first {
            let event = self.event(&key, Some(&promise), Vec::new());
            self.publish(
                &key,
                DfsMetadataTakeoverEvent::PROMISED,
                event,
                vec![
                    format!(
                        "dfs:prefix/{}/{}/{}",
                        key.1.0, promise.epoch_start.journal_sequence, promise.epoch_start.digest
                    ),
                    request.clone(),
                    format!("dfs:takeover-attempt/{attempt}"),
                ],
            );
        }
        let ended = match settlement {
            DfsMetadataTakeoverSettlement::Applied => {
                let mut event = self.event(&key, Some(&promise), Vec::new());
                event.next_epoch = Some(promise.cut.shard.epoch.saturating_add(1));
                self.publish(
                    &key,
                    DfsMetadataTakeoverEvent::SWAPPED,
                    event,
                    vec![request],
                );
                let publication_generation = self.publication_generation;
                if let Some(current) = self.attempts.get_mut(&key) {
                    current.step = Step::Following {
                        publication_generation,
                    };
                }
                false
            }
            DfsMetadataTakeoverSettlement::RejectedEpochUnchanged => {
                let event = self.event(&key, Some(&promise), Vec::new());
                self.publish(
                    &key,
                    DfsMetadataTakeoverEvent::PROMISE_RELEASED,
                    event,
                    vec![request],
                );
                true
            }
            DfsMetadataTakeoverSettlement::RejectedEpochAdvanced => {
                let event = self.event(&key, Some(&promise), Vec::new());
                self.publish(
                    &key,
                    DfsMetadataTakeoverEvent::SWAP_SUPERSEDED,
                    event,
                    vec![request],
                );
                // The kept fence is retired once this node's record reached
                // it; if the record is there already, now.
                if let Some(epoch) = record_epoch.filter(|epoch| *epoch > promise.cut.shard.epoch) {
                    let (data_dir, runtime, shard_id) =
                        (self.data_dir.clone(), self.runtime.clone(), key.1.clone());
                    self.runtime.spawn_blocking(move || {
                        if let Err(error) = retire_takeover_fence(&data_dir, &runtime, &shard_id, epoch) {
                            tracing::debug!(target: "sinomo.dfs.write_set", %error, shard = %shard_id.0, "member fence not retired");
                        }
                    });
                }
                true
            }
            DfsMetadataTakeoverSettlement::Unknown { detail } => {
                tracing::debug!(target: "sinomo.dfs.write_set", %detail, shard = %key.1.0, "takeover swap outcome unknown; re-sending");
                let Some(current) = self.attempts.get_mut(&key) else {
                    return;
                };
                let interval = match &current.step {
                    Step::Unknown { interval, .. } => interval
                        .saturating_mul(2)
                        .min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
                    _ => TAKEOVER_RESEND_FIRST,
                };
                current.step = Step::Unknown {
                    promise,
                    interval,
                    resending: false,
                };
                let done = self.done.clone();
                let resend = (key.clone(), attempt.clone());
                self.runtime.spawn(async move {
                    tokio::time::sleep(interval).await;
                    let (key, attempt) = resend;
                    let _ = done.send(TakeoverCompletion::Resend { key, attempt });
                });
                false
            }
        };
        if ended {
            self.attempts.remove(&key);
        }
    }

    /// Run activation once per takeover epoch at which this node became
    /// primary: after its own swap, after a hand-off, or after a restart.
    fn activate_if_new(
        &mut self,
        shard: &TakeoverShard,
        write_set: &DfsMetadataShardWriteSet,
        verdicts: &DfsPeerVerdictSnapshot,
    ) {
        if !matches!(
            write_set.last_change.kind,
            crate::DfsMetadataShardWriteSetChangeKind::Takeover { .. }
        ) || write_set.epoch_start.is_none()
            || self.activated.get(&shard.key) == Some(&shard.shard.epoch)
            || self.activation_blocked.get(&shard.key)
                == Some(&(
                    shard.shard.epoch,
                    self.publication_generation,
                    verdicts.generation,
                ))
            || !self.activating.insert(shard.key.clone())
        {
            return;
        }
        let unreachable = write_set
            .write_set
            .iter()
            .filter(|member| verdicts.reach(member) == DfsWriteSetMemberReach::NoPath)
            .cloned()
            .collect::<BTreeSet<_>>();
        let members = crate::metadata_shard_configured_node_ids(&shard.shard)
            .into_iter()
            .collect::<Vec<_>>();
        let (data_dir, cluster_paths, runtime, local, done) = (
            self.data_dir.clone(),
            self.cluster_paths.clone(),
            self.runtime.clone(),
            self.local.clone(),
            self.done.clone(),
        );
        let key = shard.key.clone();
        let epoch = shard.shard.epoch;
        self.runtime.spawn_blocking(move || {
            let result = activate_metadata_takeover(
                &data_dir,
                &cluster_paths,
                &runtime,
                &local,
                &key,
                &members,
                &unreachable,
            );
            let _ = done.send(TakeoverCompletion::Activated { key, epoch, result });
        });
    }

    /// Align members a commit or the publisher found unaligned with the
    /// current epoch start, once per member at a time.
    pub(in crate::runtime_host) fn align_members(
        &mut self,
        shard: &TakeoverShard,
        members: BTreeSet<NodeId>,
    ) {
        if shard.shard.primary_node_id != self.local
            || shard
                .state
                .switched()
                .is_none_or(|write_set| write_set.epoch_start.is_none())
        {
            return;
        }
        let targets = members
            .into_iter()
            .filter(|member| self.aligning.insert((shard.key.clone(), member.clone())))
            .collect::<BTreeSet<_>>();
        if targets.is_empty() {
            return;
        }
        let cut_members = crate::metadata_shard_configured_node_ids(&shard.shard)
            .into_iter()
            .collect::<Vec<_>>();
        let (data_dir, cluster_paths, runtime, done) = (
            self.data_dir.clone(),
            self.cluster_paths.clone(),
            self.runtime.clone(),
            self.done.clone(),
        );
        let key = shard.key.clone();
        self.runtime.spawn_blocking(move || {
            let result = align_metadata_takeover_members(
                &data_dir,
                &cluster_paths,
                &runtime,
                &key,
                &cut_members,
                &targets,
            );
            let _ = done.send(TakeoverCompletion::Aligned {
                key,
                members: targets.into_iter().collect(),
                result,
            });
        });
    }

    fn spawn_local_head(&self, key: ShardKey, attempt: String) {
        let data_dir = self.data_dir.clone();
        let done = self.done.clone();
        self.runtime.spawn(async move {
            let journal_sequence = super::local_durable_sequence(&data_dir, &key.0, &key.1).await;
            let _ = done.send(TakeoverCompletion::LocalHead {
                key,
                attempt,
                journal_sequence,
            });
        });
    }

    fn spawn_ask(
        &self,
        key: ShardKey,
        member: NodeId,
        request: DfsMetadataTakeoverCorroborationRequest,
        hand_off: bool,
    ) {
        let (_, peer) = daemon_dfs_peer_transports_on_runtime(&self.data_dir, &self.runtime);
        let done = self.done.clone();
        self.runtime.spawn(async move {
            let reply = super::ask_dfs_takeover_corroboration(peer, &member, &request)
                .await
                .ok();
            let attempt = request.attempt_id;
            let _ = done.send(if hand_off {
                TakeoverCompletion::HandOff {
                    key,
                    attempt,
                    member,
                    reply,
                }
            } else {
                TakeoverCompletion::Answer {
                    key,
                    attempt,
                    member,
                    reply,
                }
            });
        });
    }

    fn spawn_take(&self, shard: &TakeoverShard) {
        let Some(attempt) = self.attempts.get(&shard.key) else {
            return;
        };
        let (data_dir, cluster_paths, runtime, local, done) = (
            self.data_dir.clone(),
            self.cluster_paths.clone(),
            self.runtime.clone(),
            self.local.clone(),
            self.done.clone(),
        );
        let key = shard.key.clone();
        let (id, epoch, primary) = (attempt.id.clone(), attempt.epoch, attempt.primary.clone());
        let members = crate::metadata_shard_configured_node_ids(&shard.shard)
            .into_iter()
            .collect::<Vec<_>>();
        self.runtime.spawn_blocking(move || {
            let outcome = take_metadata_shard(
                &data_dir,
                &cluster_paths,
                &runtime,
                &local,
                &key,
                epoch,
                &primary,
                &members,
            );
            let _ = done.send(TakeoverCompletion::Taken {
                key,
                attempt: id,
                outcome,
            });
        });
    }

    /// Read the record as the confirm step does, without promising.
    fn spawn_recheck(&self, shard: &TakeoverShard) {
        let Some(attempt) = self.attempts.get(&shard.key) else {
            return;
        };
        let (cluster_paths, runtime, local, done) = (
            self.cluster_paths.clone(),
            self.runtime.clone(),
            self.local.clone(),
            self.done.clone(),
        );
        let key = shard.key.clone();
        let (id, epoch, primary) = (attempt.id.clone(), attempt.epoch, attempt.primary.clone());
        let members = crate::metadata_shard_configured_node_ids(&shard.shard)
            .into_iter()
            .collect::<Vec<_>>();
        self.runtime.spawn_blocking(move || {
            let outcome = match super::steps::confirm_metadata_takeover_record(
                &cluster_paths,
                &runtime,
                &local,
                &key,
                epoch,
                &primary,
                &members,
            ) {
                Ok(_) => Recheck::Confirmed,
                Err(TakeOutcome::Unconfirmed { .. }) => Recheck::Unconfirmed,
                Err(_) => Recheck::Refused,
            };
            let _ = done.send(TakeoverCompletion::Rechecked {
                key,
                attempt: id,
                outcome,
            });
        });
    }

    fn spawn_settle(&self, key: ShardKey, attempt: String, promise: DfsMetadataTakeoverPromise) {
        let (data_dir, cluster_paths, runtime, local, done) = (
            self.data_dir.clone(),
            self.cluster_paths.clone(),
            self.runtime.clone(),
            self.local.clone(),
            self.done.clone(),
        );
        self.runtime.spawn_blocking(move || {
            let outcome = settle_takeover_promise(
                &data_dir,
                &cluster_paths,
                &runtime,
                &local,
                &key.1,
                promise,
            );
            let _ = done.send(TakeoverCompletion::Taken {
                key,
                attempt,
                outcome,
            });
        });
    }

    fn event(
        &self,
        key: &ShardKey,
        promise: Option<&DfsMetadataTakeoverPromise>,
        answers: Vec<DfsTakeoverAnswerSummary>,
    ) -> DfsMetadataTakeoverEvent {
        let attempt = self.attempts.get(key);
        DfsMetadataTakeoverEvent {
            file_system_id: key.0.clone(),
            shard_id: key.1.0.clone(),
            epoch: attempt.map_or(0, |attempt| attempt.epoch),
            next_epoch: None,
            generation: attempt.map_or(0, |attempt| attempt.generation),
            previous_primary_node_id: attempt
                .map_or_else(|| self.local.clone(), |attempt| attempt.primary.clone()),
            candidate_node_id: self.local.clone(),
            attempt_id: attempt.map_or_else(String::new, |attempt| attempt.id.clone()),
            swap_request_id: promise.map(|promise| promise.swap_request_id.to_string()),
            epoch_start: promise.map(|promise| promise.epoch_start.clone()),
            cause: attempt.map_or("", |attempt| attempt.cause).to_string(),
            answers,
            node_ids: Vec::new(),
            dropped_records: None,
        }
    }

    fn activated_events(
        &mut self,
        key: &ShardKey,
        shard: Option<&TakeoverShard>,
        aligned: &[(NodeId, crate::DfsWriteSetAlignment)],
    ) -> Option<(ShardKey, sinomo_core::Id)> {
        let shard = shard?;
        let write_set = shard.state.switched()?;
        let previous = match &write_set.last_change.kind {
            crate::DfsMetadataShardWriteSetChangeKind::Takeover {
                previous_primary_node_id,
            } => previous_primary_node_id.clone(),
            _ => return None,
        };
        let base = DfsMetadataTakeoverEvent {
            file_system_id: key.0.clone(),
            shard_id: key.1.0.clone(),
            epoch: shard.shard.epoch.saturating_sub(1),
            next_epoch: Some(shard.shard.epoch),
            generation: write_set.generation,
            previous_primary_node_id: previous,
            candidate_node_id: self.local.clone(),
            attempt_id: String::new(),
            swap_request_id: Some(write_set.last_change.change_id.clone()),
            epoch_start: write_set.epoch_start.clone(),
            cause: "activation".into(),
            answers: Vec::new(),
            node_ids: Vec::new(),
            dropped_records: None,
        };
        let request = format!("dfs:takeover-request/{}", write_set.last_change.change_id);
        self.publish_aligned(key, &base, aligned, &request);
        let mut activated = base;
        activated.node_ids = aligned.iter().map(|(member, _)| member.clone()).collect();
        let id = self.publish(
            key,
            DfsMetadataTakeoverEvent::ACTIVATED,
            activated,
            vec![request],
        );
        Some((key.clone(), id))
    }

    fn aligned_events(
        &mut self,
        key: &ShardKey,
        shard: Option<&TakeoverShard>,
        aligned: &[(NodeId, crate::DfsWriteSetAlignment)],
    ) {
        let Some(shard) = shard else {
            return;
        };
        let Some(write_set) = shard.state.switched() else {
            return;
        };
        let base = DfsMetadataTakeoverEvent {
            file_system_id: key.0.clone(),
            shard_id: key.1.0.clone(),
            epoch: shard.shard.epoch.saturating_sub(1),
            next_epoch: Some(shard.shard.epoch),
            generation: write_set.generation,
            previous_primary_node_id: self.local.clone(),
            candidate_node_id: self.local.clone(),
            attempt_id: String::new(),
            swap_request_id: Some(write_set.last_change.change_id.clone()),
            epoch_start: write_set.epoch_start.clone(),
            cause: "alignment-required".into(),
            answers: Vec::new(),
            node_ids: Vec::new(),
            dropped_records: None,
        };
        let request = format!("dfs:takeover-request/{}", write_set.last_change.change_id);
        self.publish_aligned(key, &base, aligned, &request);
    }

    fn publish_aligned(
        &mut self,
        key: &ShardKey,
        base: &DfsMetadataTakeoverEvent,
        aligned: &[(NodeId, crate::DfsWriteSetAlignment)],
        request: &str,
    ) {
        for (member, alignment) in aligned {
            let crate::DfsWriteSetAlignment::Aligned { head } = alignment else {
                continue;
            };
            let mut event = base.clone();
            event.node_ids = vec![member.clone()];
            self.publish(
                key,
                DfsMetadataTakeoverEvent::ALIGNED,
                event,
                vec![
                    format!(
                        "dfs:prefix/{}/{}/{}",
                        key.1.0, head.journal_sequence, head.digest
                    ),
                    request.to_string(),
                ],
            );
        }
    }

    /// Publish one typed event on the component bus with its evidence, linked
    /// to the shard's previous takeover event.
    fn publish(
        &mut self,
        key: &ShardKey,
        name: &'static str,
        payload: DfsMetadataTakeoverEvent,
        refs: Vec<String>,
    ) -> sinomo_core::Id {
        let event_id = sinomo_core::Id::generate();
        let previous = self.last_events.insert(key.clone(), event_id.clone());
        #[cfg(any(test, feature = "test-support"))]
        super::record_takeover_event_for_test(&self.data_dir, name, &payload);
        let salience = if matches!(
            name,
            DfsMetadataTakeoverEvent::SWAPPED
                | DfsMetadataTakeoverEvent::ACTIVATED
                | DfsMetadataTakeoverEvent::CORROBORATION_VETOED
        ) {
            0.5
        } else {
            0.3
        };
        let Ok(value) = serde_json::to_value(&payload) else {
            return event_id;
        };
        let (Ok(source), Ok(kind), Ok(salience), Ok(confidence)) = (
            sinomo_core::Source::new("component/sinomo-dfs"),
            sinomo_core::EventKind::new(name.to_string()),
            sinomo_core::Salience::new(salience),
            sinomo_core::Confidence::new(1.0),
        ) else {
            return event_id;
        };
        let mut envelope = sinomo_core::EventEnvelope::new(
            event_id.clone(),
            time::OffsetDateTime::now_utc(),
            self.local.clone(),
            source,
            kind,
            salience,
            confidence,
            sinomo_core::RoutingMetadata::new(
                sinomo_core::RoutingScope::Local,
                sinomo_core::LatencyClass::Reactive,
                sinomo_core::PrivacyClass::Derived,
            ),
        )
        .with_payload(value);
        for reference in refs {
            envelope.evidence.refs.push(sinomo_core::EvidenceRef::new(
                reference,
                sinomo_core::EvidenceKind::Other,
            ));
        }
        if let Some(previous) = previous {
            envelope.links.caused_by.push(previous);
        }
        let publication = super::dfs_takeover_event_publication(name);
        let events = self.events.clone();
        self.runtime.spawn(async move {
            if let Err(error) = events.publish(&publication, envelope).await {
                tracing::debug!(target: "sinomo.dfs.write_set", %error, "takeover event not published");
            }
        });
        event_id
    }
}

fn answer_summaries(
    entries: &BTreeMap<NodeId, DfsTakeoverRoundEntry>,
) -> Vec<DfsTakeoverAnswerSummary> {
    entries
        .iter()
        .map(|(member, entry)| {
            let (answer, journal_sequence) = match entry {
                DfsTakeoverRoundEntry::Replied {
                    answer,
                    durable_sequence,
                    ..
                } => (
                    match answer {
                        DfsTakeoverPrimaryReach::Reaches => "reaches".to_string(),
                        DfsTakeoverPrimaryReach::Lost => "lost".to_string(),
                        DfsTakeoverPrimaryReach::Abstain { reason } => {
                            format!(
                                "abstain-{}",
                                serde_json::to_value(reason)
                                    .ok()
                                    .and_then(|value| value.as_str().map(str::to_string))
                                    .unwrap_or_default()
                            )
                        }
                    },
                    *durable_sequence,
                ),
                DfsTakeoverRoundEntry::LeftOut => ("left-out".to_string(), None),
            };
            DfsTakeoverAnswerSummary {
                member_node_id: member.clone(),
                answer,
                journal_sequence,
            }
        })
        .collect()
}

/// The facts the takeover planner reads for a shard's members and primary.
fn takeover_member_facts(
    shard: &TakeoverShard,
    members: &BTreeMap<NodeId, MemberRecord>,
    verdicts: &DfsPeerVerdictSnapshot,
) -> BTreeMap<NodeId, DfsWriteSetMemberFacts> {
    crate::metadata_shard_configured_node_ids(&shard.shard)
        .into_iter()
        .map(|node| {
            let record = members.get(&node);
            let facts = DfsWriteSetMemberFacts {
                eligible: record.is_some_and(|record| record.eligible),
                departed: record.is_none_or(|record| record.departed),
                mobile: record.is_some_and(|record| record.mobile),
                reach: verdicts.reach(&node),
                view: None,
                fenced: false,
            };
            (node, facts)
        })
        .collect()
}
