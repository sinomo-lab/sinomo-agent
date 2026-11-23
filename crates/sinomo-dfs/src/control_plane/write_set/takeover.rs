//! Whether a write-set member takes over its shard's lost primary, and which
//! member does: a pure function of the published record, member facts, this
//! node's network verdicts and the replies a corroboration round gathered.
//!
//! The keeper's candidate role gathers the inputs and executes the plan; this
//! module decides. Verdicts and replies choose whether and who to try; the
//! promise and the guarded swap keep every choice safe, so two members that
//! decide differently still produce at most one new primary.

use super::*;
use crate::takeover_corroboration::{
    DFS_TAKEOVER_HAND_OFF_PRIMARY_REACHED, DfsTakeoverAbstainReason, DfsTakeoverPrimaryReach,
    DfsTakeoverRank,
};
use std::collections::BTreeMap;

/// Why the primary is judged lost.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DfsTakeoverCause {
    /// This node's network settled that it has no path to the primary.
    PrimaryNoPath,
    /// The primary left the cluster: it has no verdict at all.
    PrimaryDeparted,
}

impl DfsTakeoverCause {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::PrimaryNoPath => "primary-no-path",
            Self::PrimaryDeparted => "member-departed",
        }
    }
}

/// How an earlier attempt for a shard ended on its own evidence: a veto, a
/// newer record, or a confirm read or promise that failed. No new attempt
/// starts until the record, this node's publication or its verdicts move.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DfsTakeoverStanding {
    pub(crate) epoch: u64,
    pub(crate) generation: u64,
    pub(crate) publication_generation: u64,
    pub(crate) verdict_generation: u64,
}

/// Inputs for one switched shard this node may take over.
#[derive(Clone, Debug)]
pub(crate) struct DfsTakeoverView<'a> {
    pub(crate) local: &'a NodeId,
    pub(crate) shard: &'a DfsMetadataShardConfig,
    pub(crate) state: &'a DfsMetadataShardWriteSetState,
    /// Every member the publication knows, with this node's verdicts.
    pub(crate) members: &'a BTreeMap<NodeId, DfsWriteSetMemberFacts>,
    /// The primary's no-path verdict is evidence gathered under this record:
    /// it became no-path while this node already read the primary at this
    /// epoch, or the network examined the primary since. A verdict that only
    /// stood while the record changed starts nothing.
    pub(crate) primary_examined: bool,
    /// The local store is open normally, not for repair only: a repair-only
    /// store may lack acknowledged changes.
    pub(crate) store_ready: bool,
    /// An attempt or an unsettled promise is in progress for the shard.
    pub(crate) busy: bool,
    pub(crate) standing: Option<DfsTakeoverStanding>,
    pub(crate) publication_generation: u64,
    pub(crate) verdict_generation: u64,
}

/// What the candidate role does next for one shard.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DfsTakeoverPlan {
    /// Not eligible, or the primary is not judged lost.
    Idle,
    /// Start an attempt: ask these write-set members first; with none to ask,
    /// go straight to the confirm read.
    Start {
        cause: DfsTakeoverCause,
        asked: BTreeSet<NodeId>,
    },
}

/// Whether this node starts a takeover attempt for the shard, and whom it
/// asks to corroborate. Only a write-set member other than the primary is
/// eligible: every acknowledged change is on its store.
pub(crate) fn plan_metadata_takeover(view: &DfsTakeoverView<'_>) -> DfsTakeoverPlan {
    let DfsMetadataShardWriteSetState::Switched(write_set) = view.state else {
        return DfsTakeoverPlan::Idle;
    };
    let primary = &view.shard.primary_node_id;
    if primary == view.local
        || !write_set.contains(view.local)
        || !view.store_ready
        || view.busy
        || view.standing.is_some_and(|standing| {
            standing
                == DfsTakeoverStanding {
                    epoch: view.shard.epoch,
                    generation: write_set.generation,
                    publication_generation: view.publication_generation,
                    verdict_generation: view.verdict_generation,
                }
        })
    {
        return DfsTakeoverPlan::Idle;
    }
    let facts = |node: &NodeId| view.members.get(node);
    let departed = |node: &NodeId| facts(node).is_none_or(|facts| facts.departed);
    let no_path = |node: &NodeId| {
        facts(node).is_some_and(|facts| facts.reach == DfsWriteSetMemberReach::NoPath)
    };
    // Departure is a membership fact cluster KV ordered; it needs no
    // examination.
    let cause = if departed(primary) {
        DfsTakeoverCause::PrimaryDeparted
    } else if no_path(primary) && view.primary_examined {
        DfsTakeoverCause::PrimaryNoPath
    } else {
        return DfsTakeoverPlan::Idle;
    };
    // Members this node cannot reach, or that left, could not answer.
    let asked = write_set
        .write_set
        .iter()
        .filter(|node| *node != primary && *node != view.local)
        .filter(|node| !departed(node) && !no_path(node))
        .cloned()
        .collect();
    DfsTakeoverPlan::Start { cause, asked }
}

/// One asked member's settled entry in a corroboration round.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DfsTakeoverRoundEntry {
    Replied {
        answer: DfsTakeoverPrimaryReach,
        durable_sequence: Option<u64>,
        mobile: bool,
    },
    /// The call ended with a delivery failure, or this node's verdict for the
    /// member settled `NoPath` meanwhile.
    LeftOut,
}

/// What a complete corroboration round decides.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DfsTakeoverRoundOutcome {
    /// A member still reaches the primary.
    Vetoed { by: NodeId },
    /// A member's record is later than this node's: wait for the publication.
    Follow,
    /// This node ranks first: confirm, promise and swap.
    Proceed,
    /// Another member ranks first: ask it to run the takeover.
    HandOff {
        to: NodeId,
        ranking: Vec<DfsTakeoverRank>,
    },
}

/// Order candidates: the longest durable journal first, so the fewest
/// unacknowledged records are dropped; then non-mobile members; then node id,
/// so concurrent rounds converge on one member.
pub(crate) fn rank_metadata_takeover_candidates(
    mut ranks: Vec<DfsTakeoverRank>,
) -> Vec<DfsTakeoverRank> {
    ranks.sort_by(|left, right| {
        right
            .journal_sequence
            .cmp(&left.journal_sequence)
            .then(left.mobile.cmp(&right.mobile))
            .then(left.node_id.cmp(&right.node_id))
    });
    ranks
}

/// Decide a complete round from this node's own rank and every asked
/// member's entry. `declined` names members already asked to run it that
/// declined, which are not ranked again.
pub(crate) fn decide_metadata_takeover_round(
    local: &DfsTakeoverRank,
    entries: &BTreeMap<NodeId, DfsTakeoverRoundEntry>,
    declined: &BTreeSet<NodeId>,
) -> DfsTakeoverRoundOutcome {
    let replied = entries.iter().filter_map(|(node, entry)| match entry {
        DfsTakeoverRoundEntry::Replied {
            answer,
            durable_sequence,
            mobile,
        } => Some((node, answer, durable_sequence, mobile)),
        DfsTakeoverRoundEntry::LeftOut => None,
    });
    let mut ranks = vec![local.clone()];
    for (node, answer, durable_sequence, mobile) in replied {
        match answer {
            DfsTakeoverPrimaryReach::Reaches => {
                return DfsTakeoverRoundOutcome::Vetoed { by: node.clone() };
            }
            DfsTakeoverPrimaryReach::Abstain {
                reason: DfsTakeoverAbstainReason::NewerRecord,
            } => return DfsTakeoverRoundOutcome::Follow,
            DfsTakeoverPrimaryReach::Lost => {
                if let Some(journal_sequence) = durable_sequence
                    && !declined.contains(node)
                {
                    ranks.push(DfsTakeoverRank {
                        node_id: node.clone(),
                        journal_sequence: *journal_sequence,
                        mobile: *mobile,
                    });
                }
            }
            DfsTakeoverPrimaryReach::Abstain { .. } => {}
        }
    }
    let ranking = rank_metadata_takeover_candidates(ranks);
    match ranking.first() {
        Some(first) if first.node_id != local.node_id => DfsTakeoverRoundOutcome::HandOff {
            to: first.node_id.clone(),
            ranking,
        },
        _ => DfsTakeoverRoundOutcome::Proceed,
    }
}

/// A hand-off request's answer on the member asked to run the takeover.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DfsTakeoverHandOffDecision {
    /// This member judges the primary lost and ranks itself first with its
    /// own head: it confirms, promises and swaps without a second round.
    Run,
    Decline {
        reason: &'static str,
        prefer: Option<NodeId>,
    },
}

/// Whether the member asked to run the takeover does so. It ranks the
/// asker's ranking again with its own current head; when that puts the asker
/// first it declines in the asker's favour, so a disagreement bounces once.
/// A member that reaches the primary declines with
/// [`DFS_TAKEOVER_HAND_OFF_PRIMARY_REACHED`], which vetoes the attempt.
pub(crate) fn decide_metadata_takeover_hand_off(
    local: &DfsTakeoverRank,
    own_answer: &DfsTakeoverPrimaryReach,
    asker: &NodeId,
    ranking: &[DfsTakeoverRank],
) -> DfsTakeoverHandOffDecision {
    if own_answer != &DfsTakeoverPrimaryReach::Lost {
        return DfsTakeoverHandOffDecision::Decline {
            reason: if own_answer == &DfsTakeoverPrimaryReach::Reaches {
                DFS_TAKEOVER_HAND_OFF_PRIMARY_REACHED
            } else {
                "primary-not-lost"
            },
            prefer: None,
        };
    }
    let mut ranks = ranking
        .iter()
        .filter(|rank| rank.node_id != local.node_id)
        .cloned()
        .collect::<Vec<_>>();
    ranks.push(local.clone());
    let ranking = rank_metadata_takeover_candidates(ranks);
    match ranking.first() {
        Some(first) if first.node_id == local.node_id => DfsTakeoverHandOffDecision::Run,
        Some(first) if &first.node_id == asker => DfsTakeoverHandOffDecision::Decline {
            reason: "prefer-asker",
            prefer: Some(asker.clone()),
        },
        Some(first) => DfsTakeoverHandOffDecision::Decline {
            reason: "prefer-other",
            prefer: Some(first.node_id.clone()),
        },
        None => DfsTakeoverHandOffDecision::Decline {
            reason: "empty-ranking",
            prefer: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).unwrap()
    }

    fn facts(reach: DfsWriteSetMemberReach) -> DfsWriteSetMemberFacts {
        DfsWriteSetMemberFacts {
            eligible: true,
            departed: false,
            mobile: false,
            reach,
            view: None,
            fenced: false,
        }
    }

    fn rank(name: &str, journal_sequence: u64, mobile: bool) -> DfsTakeoverRank {
        DfsTakeoverRank {
            node_id: node(name),
            journal_sequence,
            mobile,
        }
    }

    struct Case {
        shard: DfsMetadataShardConfig,
        state: DfsMetadataShardWriteSetState,
        members: BTreeMap<NodeId, DfsWriteSetMemberFacts>,
        store_ready: bool,
        busy: bool,
        standing: Option<DfsTakeoverStanding>,
        primary_examined: bool,
    }

    impl Case {
        /// Primary `a`, write set `write_set` (with `a`), seen from `b`.
        fn new(write_set: &[&str]) -> Self {
            let shard = DfsMetadataShardConfig {
                file_system_id: crate::sifs_file_system_id(),
                shard_id: "root".into(),
                primary_node_id: node("a"),
                replica_node_ids: write_set
                    .iter()
                    .filter(|name| **name != "a")
                    .map(|name| node(name))
                    .collect(),
                epoch: 3,
                assignment: crate::DfsMetadataShardAssignment::Root,
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            };
            let mut members_in = write_set.iter().map(|name| node(name)).collect::<Vec<_>>();
            members_in.sort();
            let record = DfsMetadataShardWriteSet {
                format: DFS_METADATA_SHARD_WRITE_SET_FORMAT,
                file_system_id: shard.file_system_id.clone(),
                shard_id: shard.shard_id.clone(),
                epoch: 3,
                primary_node_id: node("a"),
                generation: 5,
                write_set: members_in,
                epoch_start: None,
                switched_at_epoch: 2,
                substitutes: Vec::new(),
                raised_n: None,
                last_change: DfsMetadataShardWriteSetChange {
                    kind: DfsMetadataShardWriteSetChangeKind::Switch,
                    change_id: "switch".into(),
                },
            };
            let members = write_set
                .iter()
                .map(|name| (node(name), facts(DfsWriteSetMemberReach::Path)))
                .collect();
            Self {
                shard,
                state: DfsMetadataShardWriteSetState::Switched(record),
                members,
                store_ready: true,
                busy: false,
                standing: None,
                primary_examined: true,
            }
        }

        fn reach(mut self, name: &str, reach: DfsWriteSetMemberReach) -> Self {
            self.members.get_mut(&node(name)).unwrap().reach = reach;
            self
        }

        fn plan_from(&self, local: &str) -> DfsTakeoverPlan {
            plan_metadata_takeover(&DfsTakeoverView {
                local: &node(local),
                shard: &self.shard,
                state: &self.state,
                members: &self.members,
                primary_examined: self.primary_examined,
                store_ready: self.store_ready,
                busy: self.busy,
                standing: self.standing,
                publication_generation: 4,
                verdict_generation: 9,
            })
        }
    }

    #[test]
    fn only_a_write_set_member_that_judges_the_primary_lost_starts() {
        let case = Case::new(&["a", "b", "c"]);
        assert_eq!(
            case.plan_from("b"),
            DfsTakeoverPlan::Idle,
            "the primary has a path"
        );
        let case = Case::new(&["a", "b", "c"]).reach("a", DfsWriteSetMemberReach::Unsettled);
        assert_eq!(
            case.plan_from("b"),
            DfsTakeoverPlan::Idle,
            "a verifying verdict waits"
        );
        let case = Case::new(&["a", "b", "c"]).reach("a", DfsWriteSetMemberReach::NoPath);
        assert_eq!(
            case.plan_from("b"),
            DfsTakeoverPlan::Start {
                cause: DfsTakeoverCause::PrimaryNoPath,
                asked: [node("c")].into_iter().collect(),
            }
        );
        assert_eq!(
            case.plan_from("a"),
            DfsTakeoverPlan::Idle,
            "the primary itself"
        );
        assert_eq!(
            case.plan_from("d"),
            DfsTakeoverPlan::Idle,
            "not in the write set"
        );
        let mut unswitched = Case::new(&["a", "b", "c"]).reach("a", DfsWriteSetMemberReach::NoPath);
        unswitched.state = DfsMetadataShardWriteSetState::Unswitched;
        assert_eq!(unswitched.plan_from("b"), DfsTakeoverPlan::Idle);
        let mut repairing = Case::new(&["a", "b", "c"]).reach("a", DfsWriteSetMemberReach::NoPath);
        repairing.store_ready = false;
        assert_eq!(repairing.plan_from("b"), DfsTakeoverPlan::Idle);
        let mut busy = Case::new(&["a", "b", "c"]).reach("a", DfsWriteSetMemberReach::NoPath);
        busy.busy = true;
        assert_eq!(busy.plan_from("b"), DfsTakeoverPlan::Idle);
    }

    #[test]
    fn a_departed_primary_is_taken_over_without_a_verdict_and_unreachable_members_are_not_asked() {
        let mut case = Case::new(&["a", "b", "c", "d"])
            .reach("a", DfsWriteSetMemberReach::Unsettled)
            .reach("d", DfsWriteSetMemberReach::NoPath);
        case.members.get_mut(&node("a")).unwrap().departed = true;
        assert_eq!(
            case.plan_from("b"),
            DfsTakeoverPlan::Start {
                cause: DfsTakeoverCause::PrimaryDeparted,
                asked: [node("c")].into_iter().collect(),
            }
        );
        // A two-member write set asks nobody.
        let case = Case::new(&["a", "b"]).reach("a", DfsWriteSetMemberReach::NoPath);
        assert_eq!(
            case.plan_from("b"),
            DfsTakeoverPlan::Start {
                cause: DfsTakeoverCause::PrimaryNoPath,
                asked: BTreeSet::new(),
            }
        );
    }

    #[test]
    fn a_no_path_verdict_that_only_stood_while_the_record_changed_starts_nothing() {
        let mut case = Case::new(&["a", "b", "c"]).reach("a", DfsWriteSetMemberReach::NoPath);
        case.primary_examined = false;
        assert_eq!(
            case.plan_from("b"),
            DfsTakeoverPlan::Idle,
            "the network has not examined the primary under this record"
        );
        case.primary_examined = true;
        assert!(matches!(
            case.plan_from("b"),
            DfsTakeoverPlan::Start {
                cause: DfsTakeoverCause::PrimaryNoPath,
                ..
            }
        ));
        // A departed primary is taken over without an examination.
        case.primary_examined = false;
        case.members.get_mut(&node("a")).unwrap().departed = true;
        assert!(matches!(
            case.plan_from("b"),
            DfsTakeoverPlan::Start {
                cause: DfsTakeoverCause::PrimaryDeparted,
                ..
            }
        ));
    }

    #[test]
    fn a_standing_veto_holds_until_the_record_or_the_verdicts_move() {
        let mut case = Case::new(&["a", "b", "c"]).reach("a", DfsWriteSetMemberReach::NoPath);
        let standing = DfsTakeoverStanding {
            epoch: 3,
            generation: 5,
            publication_generation: 4,
            verdict_generation: 9,
        };
        case.standing = Some(standing);
        assert_eq!(case.plan_from("b"), DfsTakeoverPlan::Idle);
        case.standing = Some(DfsTakeoverStanding {
            verdict_generation: 8,
            ..standing
        });
        assert!(matches!(case.plan_from("b"), DfsTakeoverPlan::Start { .. }));
        case.standing = Some(DfsTakeoverStanding {
            publication_generation: 3,
            ..standing
        });
        assert!(
            matches!(case.plan_from("b"), DfsTakeoverPlan::Start { .. }),
            "a new publication, such as one after KV returns, plans again"
        );
    }

    #[test]
    fn ranking_prefers_the_longest_journal_then_non_mobile_then_node_id() {
        let ranked = rank_metadata_takeover_candidates(vec![
            rank("c", 7, false),
            rank("b", 9, true),
            rank("d", 9, false),
            rank("e", 9, false),
        ]);
        let order = ranked
            .iter()
            .map(|rank| rank.node_id.clone())
            .collect::<Vec<_>>();
        assert_eq!(order, vec![node("d"), node("e"), node("b"), node("c")]);
    }

    #[test]
    fn a_round_is_vetoed_followed_or_ranked() {
        let lost = |sequence| DfsTakeoverRoundEntry::Replied {
            answer: DfsTakeoverPrimaryReach::Lost,
            durable_sequence: Some(sequence),
            mobile: false,
        };
        let local = rank("b", 8, false);
        let none = BTreeSet::new();
        let entries = BTreeMap::from([
            (node("c"), lost(9)),
            (
                node("d"),
                DfsTakeoverRoundEntry::Replied {
                    answer: DfsTakeoverPrimaryReach::Reaches,
                    durable_sequence: Some(8),
                    mobile: false,
                },
            ),
        ]);
        assert_eq!(
            decide_metadata_takeover_round(&local, &entries, &none),
            DfsTakeoverRoundOutcome::Vetoed { by: node("d") }
        );
        let entries = BTreeMap::from([(
            node("c"),
            DfsTakeoverRoundEntry::Replied {
                answer: DfsTakeoverPrimaryReach::Abstain {
                    reason: DfsTakeoverAbstainReason::NewerRecord,
                },
                durable_sequence: None,
                mobile: false,
            },
        )]);
        assert_eq!(
            decide_metadata_takeover_round(&local, &entries, &none),
            DfsTakeoverRoundOutcome::Follow
        );
        // A longer member is asked to run it; left-out and abstaining
        // members are not ranked.
        let entries = BTreeMap::from([
            (node("c"), lost(9)),
            (node("d"), DfsTakeoverRoundEntry::LeftOut),
            (
                node("e"),
                DfsTakeoverRoundEntry::Replied {
                    answer: DfsTakeoverPrimaryReach::Abstain {
                        reason: DfsTakeoverAbstainReason::NoEvidence,
                    },
                    durable_sequence: Some(20),
                    mobile: false,
                },
            ),
        ]);
        assert_eq!(
            decide_metadata_takeover_round(&local, &entries, &none),
            DfsTakeoverRoundOutcome::HandOff {
                to: node("c"),
                ranking: vec![rank("c", 9, false), rank("b", 8, false)],
            }
        );
        // After it declines, this node ranks first.
        let declined = [node("c")].into_iter().collect();
        assert_eq!(
            decide_metadata_takeover_round(&local, &entries, &declined),
            DfsTakeoverRoundOutcome::Proceed
        );
    }

    #[test]
    fn a_hand_off_runs_only_when_its_target_still_ranks_itself_first() {
        let asker = node("b");
        let ranking = vec![rank("c", 9, false), rank("b", 8, false)];
        assert_eq!(
            decide_metadata_takeover_hand_off(
                &rank("c", 9, false),
                &DfsTakeoverPrimaryReach::Lost,
                &asker,
                &ranking
            ),
            DfsTakeoverHandOffDecision::Run
        );
        // Its own head is shorter than the asker saw: it bounces back once.
        assert_eq!(
            decide_metadata_takeover_hand_off(
                &rank("c", 7, false),
                &DfsTakeoverPrimaryReach::Lost,
                &asker,
                &ranking
            ),
            DfsTakeoverHandOffDecision::Decline {
                reason: "prefer-asker",
                prefer: Some(asker.clone()),
            }
        );
        // A target that reaches the primary says so: that vetoes.
        assert_eq!(
            decide_metadata_takeover_hand_off(
                &rank("c", 9, false),
                &DfsTakeoverPrimaryReach::Reaches,
                &asker,
                &ranking
            ),
            DfsTakeoverHandOffDecision::Decline {
                reason: DFS_TAKEOVER_HAND_OFF_PRIMARY_REACHED,
                prefer: None,
            }
        );
        // One without evidence does not reach it, and does not veto.
        assert_eq!(
            decide_metadata_takeover_hand_off(
                &rank("c", 9, false),
                &DfsTakeoverPrimaryReach::Abstain {
                    reason: DfsTakeoverAbstainReason::NoEvidence,
                },
                &asker,
                &ranking
            ),
            DfsTakeoverHandOffDecision::Decline {
                reason: "primary-not-lost",
                prefer: None,
            }
        );
    }
}
