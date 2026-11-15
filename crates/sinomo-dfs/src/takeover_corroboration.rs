//! Takeover corroboration: a write-set member that judges its shard's primary
//! lost asks the other write-set members whether they still reach it, before
//! it promises anything.
//!
//! The route is direct object RPC between write-set members. A member answers
//! from its own retained record and its own network verdict for the named
//! primary, and asks the network to verify the primary first unless its
//! verdict is already a settled `NoPath`. A member that still reaches the
//! primary vetoes the attempt. A hand-off request asks the member the asker
//! ranked first to run the takeover itself.
//!
//! Replies are inputs to a choice, never authority: the promise and the
//! guarded swap keep every outcome safe.

use crate::{
    DfsFileSystemId, DfsMetadataMemberRecordView, DfsMetadataShardConfig, DfsMetadataShardId,
    DfsMetadataShardWriteSet,
};
use serde::{Deserialize, Serialize};
use sinomo_core::network::PeerPathVerdict;
use sinomo_core::node::NodeId;

/// Wire format of the corroboration request and reply.
pub const DFS_METADATA_TAKEOVER_CORROBORATION_FORMAT: u16 = 1;

/// One question from a takeover candidate to another write-set member.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataTakeoverCorroborationRequest {
    pub format: u16,
    pub attempt_id: String,
    pub kind: DfsTakeoverCorroborationKind,
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    /// The epoch and write-set generation of the asker's record view.
    pub epoch: u64,
    pub write_set_generation: u64,
    /// The primary the asker judges lost.
    pub primary_node_id: NodeId,
    /// The asker.
    pub candidate_node_id: NodeId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case", tag = "kind", deny_unknown_fields)]
pub enum DfsTakeoverCorroborationKind {
    /// Does this member still reach the primary?
    Query,
    /// The asker ranked this member first: run the takeover if this member
    /// agrees, from the asker's round.
    HandOff { ranking: Vec<DfsTakeoverRank> },
}

/// One ranked member: its durable journal head and whether it is mobile.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsTakeoverRank {
    pub node_id: NodeId,
    pub journal_sequence: u64,
    pub mobile: bool,
}

/// One member's answer.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataTakeoverCorroborationReply {
    pub attempt_id: String,
    pub member_node_id: NodeId,
    /// This member's own record view; absent when it holds no record of the
    /// shard on the write-set protocol.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_view: Option<DfsMetadataMemberRecordView>,
    pub answer: DfsTakeoverPrimaryReach,
    /// This member's durable journal head sequence, for the ranking.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durable_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hand_off: Option<DfsTakeoverHandOffOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict_evidence: Option<DfsTakeoverVerdictEvidence>,
}

/// Whether this member reaches the named primary.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case", tag = "reach", deny_unknown_fields)]
pub enum DfsTakeoverPrimaryReach {
    Reaches,
    Lost,
    Abstain { reason: DfsTakeoverAbstainReason },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsTakeoverAbstainReason {
    /// The asker or this node is not a configured member of the shard in
    /// this node's record, or the shard is not on the write-set protocol.
    NotAMember,
    /// This node's record names a later epoch than the asker's.
    NewerRecord,
    /// This node's network has no verdict for the primary.
    NoEvidence,
    /// The request is not a well-formed request from the asker.
    Malformed,
    /// This node could not read its own record or verdicts.
    Unavailable,
}

/// What a hand-off request led to on the member asked to run it.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case", tag = "outcome", deny_unknown_fields)]
pub enum DfsTakeoverHandOffOutcome {
    /// This member's swap applied: it is the new primary.
    Swapped,
    /// The record moved on: the asker follows its publication.
    Followed,
    /// This member does not run the takeover; the asker recomputes its
    /// ranking without it. `prefer` names the member it ranks first.
    Declined {
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefer: Option<NodeId>,
    },
}

/// The network evidence an answer came from.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsTakeoverVerdictEvidence {
    pub snapshot_generation: u64,
    pub verification_generation: u64,
}

impl DfsMetadataTakeoverCorroborationReply {
    pub(crate) fn abstain(
        request: &DfsMetadataTakeoverCorroborationRequest,
        member: &NodeId,
        record_view: Option<DfsMetadataMemberRecordView>,
        reason: DfsTakeoverAbstainReason,
    ) -> Self {
        Self {
            attempt_id: request.attempt_id.clone(),
            member_node_id: member.clone(),
            record_view,
            answer: DfsTakeoverPrimaryReach::Abstain { reason },
            durable_sequence: None,
            hand_off: None,
            verdict_evidence: None,
        }
    }
}

/// What a member answers from its own record before judging the primary: the
/// record view to report with its judgement, or an abstention.
pub(crate) fn takeover_corroboration_record_check(
    request: &DfsMetadataTakeoverCorroborationRequest,
    caller: &NodeId,
    local: &NodeId,
    shard: Option<&DfsMetadataShardConfig>,
    write_set: Option<&DfsMetadataShardWriteSet>,
) -> Result<DfsMetadataMemberRecordView, (Option<DfsMetadataMemberRecordView>, DfsTakeoverAbstainReason)>
{
    if request.format != DFS_METADATA_TAKEOVER_CORROBORATION_FORMAT
        || &request.candidate_node_id != caller
        || request.attempt_id.is_empty()
    {
        return Err((None, DfsTakeoverAbstainReason::Malformed));
    }
    let (Some(shard), Some(write_set)) = (shard, write_set) else {
        return Err((None, DfsTakeoverAbstainReason::NotAMember));
    };
    let view = DfsMetadataMemberRecordView {
        epoch: shard.epoch,
        generation: write_set.generation,
        primary_node_id: shard.primary_node_id.clone(),
    };
    let configured = crate::metadata_shard_configured_node_ids(shard);
    if !configured.contains(caller) || !configured.contains(local) {
        return Err((Some(view), DfsTakeoverAbstainReason::NotAMember));
    }
    if shard.epoch > request.epoch {
        return Err((Some(view), DfsTakeoverAbstainReason::NewerRecord));
    }
    Ok(view)
}

/// This member's judgement of the named primary, or `None` while its network
/// is still verifying it. A departed primary is lost without a verdict: a
/// removed node has none.
pub(crate) fn takeover_corroboration_judgement(
    departed: bool,
    verdict: PeerPathVerdict,
    verifying: bool,
) -> Option<DfsTakeoverPrimaryReach> {
    if departed {
        return Some(DfsTakeoverPrimaryReach::Lost);
    }
    if verifying {
        return None;
    }
    Some(match verdict {
        PeerPathVerdict::Path => DfsTakeoverPrimaryReach::Reaches,
        PeerPathVerdict::NoPath => DfsTakeoverPrimaryReach::Lost,
        PeerPathVerdict::Unknown => DfsTakeoverPrimaryReach::Abstain {
            reason: DfsTakeoverAbstainReason::NoEvidence,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).unwrap()
    }

    fn shard(epoch: u64) -> (DfsMetadataShardConfig, DfsMetadataShardWriteSet) {
        let shard = DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: "root".into(),
            primary_node_id: node("corroborate-a"),
            replica_node_ids: vec![node("corroborate-b"), node("corroborate-c")],
            epoch,
            assignment: crate::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let mut members = vec![node("corroborate-a"), node("corroborate-b"), node("corroborate-c")];
        members.sort();
        let write_set = DfsMetadataShardWriteSet {
            format: crate::DFS_METADATA_SHARD_WRITE_SET_FORMAT,
            file_system_id: shard.file_system_id.clone(),
            shard_id: "root".into(),
            epoch,
            primary_node_id: node("corroborate-a"),
            generation: 7,
            write_set: members,
            epoch_start: None,
            switched_at_epoch: epoch,
            substitutes: Vec::new(),
            raised_n: None,
            last_change: crate::DfsMetadataShardWriteSetChange {
                kind: crate::DfsMetadataShardWriteSetChangeKind::Switch,
                change_id: "switch".into(),
            },
        };
        (shard, write_set)
    }

    fn request(kind: DfsTakeoverCorroborationKind) -> DfsMetadataTakeoverCorroborationRequest {
        DfsMetadataTakeoverCorroborationRequest {
            format: DFS_METADATA_TAKEOVER_CORROBORATION_FORMAT,
            attempt_id: "attempt".into(),
            kind,
            file_system_id: crate::sifs_file_system_id(),
            shard_id: DfsMetadataShardId::new("root"),
            epoch: 3,
            write_set_generation: 7,
            primary_node_id: node("corroborate-a"),
            candidate_node_id: node("corroborate-b"),
        }
    }

    #[test]
    fn requests_and_replies_round_trip_and_refuse_unknown_fields() {
        let hand_off = request(DfsTakeoverCorroborationKind::HandOff {
            ranking: vec![DfsTakeoverRank {
                node_id: node("corroborate-c"),
                journal_sequence: 9,
                mobile: false,
            }],
        });
        for request in [request(DfsTakeoverCorroborationKind::Query), hand_off] {
            let encoded = serde_json::to_value(&request).unwrap();
            assert_eq!(
                serde_json::from_value::<DfsMetadataTakeoverCorroborationRequest>(encoded.clone())
                    .unwrap(),
                request
            );
            let mut extended = encoded;
            extended["later"] = serde_json::json!(1);
            assert!(
                serde_json::from_value::<DfsMetadataTakeoverCorroborationRequest>(extended)
                    .is_err()
            );
        }
        for answer in [
            DfsTakeoverPrimaryReach::Reaches,
            DfsTakeoverPrimaryReach::Lost,
            DfsTakeoverPrimaryReach::Abstain {
                reason: DfsTakeoverAbstainReason::NewerRecord,
            },
        ] {
            let reply = DfsMetadataTakeoverCorroborationReply {
                attempt_id: "attempt".into(),
                member_node_id: node("corroborate-c"),
                record_view: Some(DfsMetadataMemberRecordView {
                    epoch: 3,
                    generation: 7,
                    primary_node_id: node("corroborate-a"),
                }),
                answer,
                durable_sequence: Some(9),
                hand_off: Some(DfsTakeoverHandOffOutcome::Declined {
                    reason: "prefer-asker".into(),
                    prefer: Some(node("corroborate-b")),
                }),
                verdict_evidence: Some(DfsTakeoverVerdictEvidence {
                    snapshot_generation: 4,
                    verification_generation: 2,
                }),
            };
            let encoded = serde_json::to_vec(&reply).unwrap();
            assert_eq!(
                serde_json::from_slice::<DfsMetadataTakeoverCorroborationReply>(&encoded).unwrap(),
                reply
            );
        }
    }

    #[test]
    fn a_member_answers_only_from_its_own_record_of_the_shard() {
        let (config, write_set) = shard(3);
        let b = node("corroborate-b");
        let c = node("corroborate-c");
        let query = request(DfsTakeoverCorroborationKind::Query);
        let view = takeover_corroboration_record_check(&query, &b, &c, Some(&config), Some(&write_set))
            .unwrap();
        assert_eq!((view.epoch, view.generation), (3, 7));
        // The caller must be the candidate it names.
        assert_eq!(
            takeover_corroboration_record_check(&query, &c, &c, Some(&config), Some(&write_set))
                .unwrap_err()
                .1,
            DfsTakeoverAbstainReason::Malformed
        );
        // An unswitched shard, or one the caller is not a member of.
        assert_eq!(
            takeover_corroboration_record_check(&query, &b, &c, Some(&config), None)
                .unwrap_err()
                .1,
            DfsTakeoverAbstainReason::NotAMember
        );
        let mut outside = config.clone();
        outside.replica_node_ids.retain(|node| node != &b);
        assert_eq!(
            takeover_corroboration_record_check(&query, &b, &c, Some(&outside), Some(&write_set))
                .unwrap_err()
                .1,
            DfsTakeoverAbstainReason::NotAMember
        );
        // A later record: the asker follows.
        let (later, later_set) = shard(4);
        let (view, reason) =
            takeover_corroboration_record_check(&query, &b, &c, Some(&later), Some(&later_set))
                .unwrap_err();
        assert_eq!(reason, DfsTakeoverAbstainReason::NewerRecord);
        assert_eq!(view.unwrap().epoch, 4);
    }

    #[test]
    fn the_primary_is_judged_from_a_settled_verdict_or_its_departure() {
        use DfsTakeoverPrimaryReach::*;
        assert_eq!(
            takeover_corroboration_judgement(false, PeerPathVerdict::Path, false),
            Some(Reaches)
        );
        assert_eq!(
            takeover_corroboration_judgement(false, PeerPathVerdict::NoPath, false),
            Some(Lost)
        );
        assert_eq!(
            takeover_corroboration_judgement(false, PeerPathVerdict::Path, true),
            None,
            "a verdict the network is verifying is waited for"
        );
        assert_eq!(
            takeover_corroboration_judgement(false, PeerPathVerdict::Unknown, false),
            Some(Abstain {
                reason: DfsTakeoverAbstainReason::NoEvidence
            })
        );
        assert_eq!(
            takeover_corroboration_judgement(true, PeerPathVerdict::Unknown, true),
            Some(Lost)
        );
    }
}
