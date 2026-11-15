//! What a shard primary does next to keep its write set: a pure function of
//! the published record, member facts, network verdicts and member views.
//!
//! The keeper (`runtime_host::write_set_keeping`) gathers the inputs on each
//! event and executes the plan; this module decides. Network verdicts are
//! inputs to choices, never authority: every change is still one guarded
//! transaction on the exact record.

use super::*;
use std::collections::BTreeMap;

/// A member's network verdict as the keeper acts on it. Only a settled
/// verdict (the network is not verifying the peer) starts anything.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DfsWriteSetMemberReach {
    Path,
    NoPath,
    Unsettled,
}

/// What the keeper knows about one cluster member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DfsWriteSetMemberFacts {
    /// May serve as a metadata member: authorized, not non-voting, offers
    /// metadata replication and advertises the write-set capability.
    pub(crate) eligible: bool,
    /// No longer a cluster member, or no longer able to be one.
    pub(crate) departed: bool,
    pub(crate) mobile: bool,
    pub(crate) reach: DfsWriteSetMemberReach,
    /// The latest acknowledgement view this primary accepted from it.
    pub(crate) view: Option<crate::DfsMetadataMemberAckView>,
    /// It refused this primary's epoch at the current record generation: it
    /// promised a takeover candidate to accept nothing more at it.
    pub(crate) fenced: bool,
}

/// Inputs for one shard the local node is primary of.
#[derive(Clone, Debug)]
pub(crate) struct DfsWriteSetKeepingView<'a> {
    pub(crate) shard: &'a DfsMetadataShardConfig,
    pub(crate) state: &'a DfsMetadataShardWriteSetState,
    pub(crate) handoff_pending: bool,
    pub(crate) operation_floor: bool,
    pub(crate) bounds: DfsMetadataShardBounds,
    /// Every member the publication knows, configured or not.
    pub(crate) members: &'a BTreeMap<NodeId, DfsWriteSetMemberFacts>,
    pub(crate) pending_adds: &'a BTreeSet<NodeId>,
    /// The largest content W a pending commit needs the record to accept.
    pub(crate) demanded_w: usize,
}

/// Why a member leaves the write set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DfsWriteSetShrinkCause {
    NetworkNoPath,
    MemberDeparted,
    /// The member stated that it will not acknowledge this epoch.
    MemberFenced,
}

/// Why members leave the configured set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DfsWriteSetRemoveCause {
    SwitchReduction,
    SubstituteSurplus,
    MobileSurplus,
    /// Members beyond the desired set that are neither substitutes nor all
    /// mobile, after the shard's N came down.
    AboveN,
    MemberDeparted,
}

/// One guarded transaction the keeper submits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DfsWriteSetKeepingTxn {
    Switch,
    Shrink {
        member: NodeId,
        cause: DfsWriteSetShrinkCause,
    },
    RaiseN {
        w: usize,
    },
    Recruit {
        member: NodeId,
        admission: DfsMetadataShardRecruit,
    },
    Remove {
        members: BTreeSet<NodeId>,
        retain_at_least: usize,
        retire_only: bool,
        cause: DfsWriteSetRemoveCause,
    },
}

/// The keeper's next steps for one shard.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct DfsWriteSetKeepingPlan {
    /// At most one transaction; the next waits for its outcome.
    pub(crate) txn: Option<DfsWriteSetKeepingTxn>,
    /// Out-of-sync configured members whose view names the current epoch and
    /// generation: each may join (catch-up and pending add under admission).
    pub(crate) joins: Vec<NodeId>,
    /// Out-of-sync configured members to bring to the current generation and
    /// head: a generation-qualified probe, and replication behind it.
    pub(crate) catch_ups: Vec<NodeId>,
}

/// The members a shard should keep, the primary first.
pub(crate) fn metadata_write_set_desired_members(
    shard: &DfsMetadataShardConfig,
    write_set: Option<&DfsMetadataShardWriteSet>,
    members: &BTreeMap<NodeId, DfsWriteSetMemberFacts>,
    bounds: &DfsMetadataShardBounds,
) -> Vec<NodeId> {
    let configured = metadata_shard_configured_node_ids(shard);
    let substitutes = write_set
        .map(|write_set| {
            write_set
                .substitutes
                .iter()
                .map(|substitute| {
                    (
                        substitute.node_id.clone(),
                        substitute.recruited_at_generation,
                    )
                })
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let candidates = members
        .iter()
        .filter(|(node_id, facts)| {
            !facts.departed
                && facts.eligible
                && (configured.contains(*node_id) || facts.reach == DfsWriteSetMemberReach::Path)
        })
        .map(|(node_id, facts)| DfsMetadataWriteSetCandidate {
            node_id: node_id.clone(),
            mobile: facts.mobile,
            in_write_set: write_set.is_some_and(|write_set| write_set.contains(node_id)),
            reachable: facts.reach == DfsWriteSetMemberReach::Path,
            kind: if !configured.contains(node_id) {
                DfsMetadataWriteSetCandidateKind::Candidate
            } else if let Some(generation) = substitutes.get(node_id) {
                DfsMetadataWriteSetCandidateKind::Substitute {
                    recruited_at_generation: *generation,
                }
            } else {
                DfsMetadataWriteSetCandidateKind::Original
            },
        })
        .collect::<Vec<_>>();
    let primary_mobile = members
        .get(&shard.primary_node_id)
        .is_some_and(|facts| facts.mobile);
    let sized = DfsMetadataShardBounds {
        w_floor: bounds.w_floor,
        n: metadata_shard_effective_n(bounds, write_set),
    };
    select_metadata_shard_members(shard, &candidates, &sized, primary_mobile)
}

/// Plan the next steps for one shard the local node is primary of.
pub(crate) fn plan_metadata_write_set_keeping(
    view: &DfsWriteSetKeepingView<'_>,
) -> DfsWriteSetKeepingPlan {
    let configured = metadata_shard_configured_node_ids(view.shard);
    let facts = |node: &NodeId| view.members.get(node);
    let write_set = match view.state {
        DfsMetadataShardWriteSetState::Switched(write_set) => write_set,
        DfsMetadataShardWriteSetState::Unswitched | DfsMetadataShardWriteSetState::Stale { .. } => {
            // The gate: every configured member runs the write-set protocol.
            let gate = !view.handoff_pending
                && !view.operation_floor
                && configured
                    .iter()
                    .all(|node| facts(node).is_some_and(|facts| facts.eligible));
            return DfsWriteSetKeepingPlan {
                txn: gate.then_some(DfsWriteSetKeepingTxn::Switch),
                ..Default::default()
            };
        }
        DfsMetadataShardWriteSetState::Unreadable { .. } => return Default::default(),
    };
    if view.handoff_pending {
        return Default::default();
    }
    let primary = &view.shard.primary_node_id;
    let departed = |node: &NodeId| facts(node).is_none_or(|facts| facts.departed);
    let reach = |node: &NodeId| {
        facts(node).map_or(DfsWriteSetMemberReach::Unsettled, |facts| facts.reach)
    };
    let mut plan = DfsWriteSetKeepingPlan::default();

    // A lost member stops blocking commits first, never below the floor. A
    // member that departed, or refused the epoch for a takeover candidate,
    // said so itself; a network verdict only suggests it.
    if write_set.write_set.len() > view.bounds.w_floor {
        let lost = write_set
            .write_set
            .iter()
            .filter(|node| *node != primary)
            .filter_map(|node| {
                if departed(node) {
                    Some((0, node, DfsWriteSetShrinkCause::MemberDeparted))
                } else if facts(node).is_some_and(|facts| facts.fenced) {
                    Some((0, node, DfsWriteSetShrinkCause::MemberFenced))
                } else if reach(node) == DfsWriteSetMemberReach::NoPath {
                    let rank = if facts(node).is_some_and(|facts| facts.mobile) {
                        1
                    } else if write_set.substitutes.iter().any(|s| &s.node_id == node) {
                        2
                    } else {
                        3
                    };
                    Some((rank, node, DfsWriteSetShrinkCause::NetworkNoPath))
                } else {
                    None
                }
            })
            .min_by(|left, right| {
                left.0.cmp(&right.0).then_with(|| {
                    let generation = |node: &NodeId| {
                        write_set
                            .substitutes
                            .iter()
                            .find(|s| &s.node_id == node)
                            .map_or(0, |s| s.recruited_at_generation)
                    };
                    generation(right.1)
                        .cmp(&generation(left.1))
                        .then_with(|| left.1.cmp(right.1))
                })
            });
        if let Some((_, member, cause)) = lost {
            plan.txn = Some(DfsWriteSetKeepingTxn::Shrink {
                member: member.clone(),
                cause,
            });
        }
    }

    // A pending commit whose content W the record does not yet accept.
    if plan.txn.is_none() && view.demanded_w > write_set.accepted_w() {
        plan.txn = Some(DfsWriteSetKeepingTxn::RaiseN {
            w: view.demanded_w,
        });
    }

    // A member that left the cluster can never rejoin.
    let departed_outside = configured
        .iter()
        .filter(|node| *node != primary && !write_set.contains(node) && departed(node))
        .cloned()
        .collect::<BTreeSet<_>>();
    if plan.txn.is_none() && !departed_outside.is_empty() {
        plan.txn = Some(DfsWriteSetKeepingTxn::Remove {
            members: departed_outside.clone(),
            retain_at_least: 1,
            retire_only: true,
            cause: DfsWriteSetRemoveCause::MemberDeparted,
        });
    }

    let desired = metadata_write_set_desired_members(
        view.shard,
        Some(write_set),
        view.members,
        &view.bounds,
    );
    let effective_n = metadata_shard_effective_n(&view.bounds, Some(write_set));

    // Configured members the desired set names rejoin once caught up.
    for node in desired.iter().filter(|node| {
        configured.contains(*node)
            && !write_set.contains(node)
            && !view.pending_adds.contains(*node)
            && !departed(node)
            && reach(node) == DfsWriteSetMemberReach::Path
    }) {
        let current = facts(node).and_then(|facts| facts.view.as_ref()).is_some_and(|member| {
            member.epoch == view.shard.epoch && member.write_set_generation == write_set.generation
        });
        if current {
            plan.joins.push(node.clone());
        } else {
            plan.catch_ups.push(node.clone());
        }
    }

    // Recruit the first desired member that is not configured, one at a time.
    let recruiting = write_set.substitutes.iter().any(|substitute| {
        !write_set.contains(&substitute.node_id)
            && reach(&substitute.node_id) == DfsWriteSetMemberReach::Path
    });
    if plan.txn.is_none()
        && !recruiting
        && let Some(candidate) = desired.iter().find(|node| !configured.contains(*node))
    {
        let admission = if write_set.write_set.len() < effective_n {
            Some(DfsMetadataShardRecruit::BelowN)
        } else if configured.len() <= effective_n
            && configured.iter().all(|node| write_set.contains(node))
        {
            Some(DfsMetadataShardRecruit::Replacement)
        } else {
            None
        };
        if let Some(admission) = admission {
            plan.txn = Some(DfsWriteSetKeepingTxn::Recruit {
                member: candidate.clone(),
                admission,
            });
        }
    }

    // Once every desired member is in the write set, the rest is surplus:
    // the most recent substitutes first, never an offline original.
    if plan.txn.is_none() && desired.iter().all(|node| write_set.contains(node)) {
        let surplus = configured
            .iter()
            .filter(|node| !desired.contains(*node) && *node != primary && !departed(node))
            .filter(|node| {
                let substitute = write_set.substitutes.iter().any(|s| &s.node_id == *node);
                substitute || reach(node) == DfsWriteSetMemberReach::Path
            })
            .cloned()
            .collect::<BTreeSet<_>>();
        if !surplus.is_empty() {
            let mobile = surplus
                .iter()
                .all(|node| facts(node).is_some_and(|facts| facts.mobile));
            let substitutes = surplus
                .iter()
                .all(|node| write_set.substitutes.iter().any(|s| &s.node_id == node));
            plan.txn = Some(DfsWriteSetKeepingTxn::Remove {
                retire_only: surplus.iter().all(|node| !write_set.contains(node)),
                members: surplus,
                retain_at_least: desired.len(),
                cause: if mobile {
                    DfsWriteSetRemoveCause::MobileSurplus
                } else if substitutes {
                    DfsWriteSetRemoveCause::SubstituteSurplus
                } else {
                    DfsWriteSetRemoveCause::AboveN
                },
            });
        }
    }
    plan
}

/// The members a switch-time reduction removes: every configured member the
/// desired set does not name, offline ones included.
pub(crate) fn metadata_write_set_reduction(
    shard: &DfsMetadataShardConfig,
    write_set: &DfsMetadataShardWriteSet,
    members: &BTreeMap<NodeId, DfsWriteSetMemberFacts>,
    bounds: &DfsMetadataShardBounds,
) -> Option<DfsWriteSetKeepingTxn> {
    let desired =
        metadata_write_set_desired_members(shard, Some(write_set), members, bounds);
    let remove = metadata_shard_configured_node_ids(shard)
        .into_iter()
        .filter(|node| !desired.contains(node))
        .collect::<BTreeSet<_>>();
    (!remove.is_empty()).then_some(DfsWriteSetKeepingTxn::Remove {
        retire_only: false,
        members: remove,
        retain_at_least: desired.len(),
        cause: DfsWriteSetRemoveCause::SwitchReduction,
    })
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

    struct Case {
        shard: DfsMetadataShardConfig,
        state: DfsMetadataShardWriteSetState,
        members: BTreeMap<NodeId, DfsWriteSetMemberFacts>,
        pending: BTreeSet<NodeId>,
        bounds: DfsMetadataShardBounds,
        demanded_w: usize,
    }

    impl Case {
        /// Primary `a` with replicas `replicas`, write set `write_set`.
        fn new(replicas: &[&str], write_set: &[&str], w_floor: usize, n: usize) -> Self {
            let shard = DfsMetadataShardConfig {
                file_system_id: crate::sifs_file_system_id(),
                shard_id: "root".into(),
                primary_node_id: node("a"),
                replica_node_ids: replicas.iter().map(|name| node(name)).collect(),
                epoch: 2,
                assignment: crate::DfsMetadataShardAssignment::Root,
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            };
            let mut members_in = write_set.iter().map(|name| node(name)).collect::<Vec<_>>();
            members_in.push(node("a"));
            members_in.sort();
            members_in.dedup();
            let record = DfsMetadataShardWriteSet {
                format: DFS_METADATA_SHARD_WRITE_SET_FORMAT,
                file_system_id: shard.file_system_id.clone(),
                shard_id: shard.shard_id.clone(),
                epoch: 2,
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
            let mut members = BTreeMap::new();
            members.insert(node("a"), facts(DfsWriteSetMemberReach::Path));
            for name in replicas {
                members.insert(node(name), facts(DfsWriteSetMemberReach::Path));
            }
            Self {
                shard,
                state: DfsMetadataShardWriteSetState::Switched(record),
                members,
                pending: BTreeSet::new(),
                bounds: DfsMetadataShardBounds { w_floor, n },
                demanded_w: 0,
            }
        }

        fn record(&mut self) -> &mut DfsMetadataShardWriteSet {
            match &mut self.state {
                DfsMetadataShardWriteSetState::Switched(record) => record,
                _ => unreachable!(),
            }
        }

        fn reach(mut self, name: &str, reach: DfsWriteSetMemberReach) -> Self {
            self.members
                .entry(node(name))
                .or_insert_with(|| facts(reach))
                .reach = reach;
            self
        }

        fn plan(&self) -> DfsWriteSetKeepingPlan {
            plan_metadata_write_set_keeping(&DfsWriteSetKeepingView {
                shard: &self.shard,
                state: &self.state,
                handoff_pending: false,
                operation_floor: false,
                bounds: self.bounds,
                members: &self.members,
                pending_adds: &self.pending,
                demanded_w: self.demanded_w,
            })
        }
    }

    fn shrink(name: &str) -> Option<DfsWriteSetKeepingTxn> {
        Some(DfsWriteSetKeepingTxn::Shrink {
            member: node(name),
            cause: DfsWriteSetShrinkCause::NetworkNoPath,
        })
    }

    #[test]
    fn a_stable_shard_needs_nothing() {
        let case = Case::new(&["b", "c"], &["b", "c"], 2, 3);
        assert_eq!(case.plan(), DfsWriteSetKeepingPlan::default());
    }

    #[test]
    fn shrink_needs_a_settled_no_path_and_never_goes_below_the_floor() {
        let case = Case::new(&["b", "c"], &["b", "c"], 2, 3).reach("c", DfsWriteSetMemberReach::NoPath);
        assert_eq!(case.plan().txn, shrink("c"));
        let case = Case::new(&["b", "c"], &["b", "c"], 2, 3).reach("c", DfsWriteSetMemberReach::Unsettled);
        assert_eq!(case.plan().txn, None, "an unsettled verdict starts nothing");
        let case = Case::new(&["b", "c"], &["b"], 2, 3).reach("b", DfsWriteSetMemberReach::NoPath);
        assert!(
            !matches!(case.plan().txn, Some(DfsWriteSetKeepingTxn::Shrink { .. })),
            "a dead member at the floor stays"
        );
    }

    #[test]
    fn a_returning_member_catches_up_then_joins_at_the_current_generation() {
        let mut case = Case::new(&["b", "c"], &["b"], 1, 3);
        assert_eq!(case.plan().catch_ups, vec![node("c")]);
        let head = crate::DfsMetadataPrefixProof::from_digest(
            4,
            format!("dfs-metadata-prefix-v2:blake3:{:064x}", 4),
        )
        .unwrap();
        case.members.get_mut(&node("c")).unwrap().view = Some(crate::DfsMetadataMemberAckView {
            epoch: 2,
            write_set_generation: 4,
            durable_prefix: head.clone(),
        });
        assert_eq!(case.plan().catch_ups, vec![node("c")], "an older generation is not evidence");
        case.members.get_mut(&node("c")).unwrap().view = Some(crate::DfsMetadataMemberAckView {
            epoch: 2,
            write_set_generation: 5,
            durable_prefix: head,
        });
        assert_eq!(case.plan().joins, vec![node("c")]);
        case.pending.insert(node("c"));
        assert!(case.plan().joins.is_empty(), "a pending add is not joined twice");
    }

    #[test]
    fn a_short_write_set_recruits_only_without_a_reachable_original() {
        let case = Case::new(&["b", "c"], &["b"], 1, 3)
            .reach("c", DfsWriteSetMemberReach::Path)
            .reach("d", DfsWriteSetMemberReach::Path);
        assert_eq!(case.plan().txn, None, "the reachable original c rejoins instead");
        let case = Case::new(&["b", "c"], &["b"], 1, 3)
            .reach("c", DfsWriteSetMemberReach::NoPath)
            .reach("d", DfsWriteSetMemberReach::Path);
        assert_eq!(
            case.plan().txn,
            Some(DfsWriteSetKeepingTxn::Recruit {
                member: node("d"),
                admission: DfsMetadataShardRecruit::BelowN,
            })
        );
    }

    #[test]
    fn a_dead_member_at_a_floor_equal_to_n_is_replaced_beyond_n() {
        let case = Case::new(&["b", "c"], &["b", "c"], 3, 3)
            .reach("c", DfsWriteSetMemberReach::NoPath)
            .reach("d", DfsWriteSetMemberReach::Path);
        assert_eq!(
            case.plan().txn,
            Some(DfsWriteSetKeepingTxn::Recruit {
                member: node("d"),
                admission: DfsMetadataShardRecruit::Replacement,
            })
        );
        // Once the replacement is in the write set, the dead member leaves.
        let mut case = Case::new(&["b", "c", "d"], &["b", "c", "d"], 3, 3)
            .reach("c", DfsWriteSetMemberReach::NoPath);
        case.record().substitutes.push(DfsMetadataShardSubstitute {
            node_id: node("d"),
            recruited_at_generation: 4,
        });
        assert_eq!(case.plan().txn, shrink("c"));
    }

    #[test]
    fn the_most_recent_substitute_is_retired_when_the_original_is_back() {
        let mut case = Case::new(&["b", "c", "d"], &["b", "c", "d"], 1, 3);
        case.record().substitutes.push(DfsMetadataShardSubstitute {
            node_id: node("d"),
            recruited_at_generation: 4,
        });
        assert_eq!(
            case.plan().txn,
            Some(DfsWriteSetKeepingTxn::Remove {
                members: [node("d")].into_iter().collect(),
                retain_at_least: 3,
                retire_only: false,
                cause: DfsWriteSetRemoveCause::SubstituteSurplus,
            })
        );
        // An offline original outside the write set keeps its membership
        // while its substitute holds the copy.
        let mut case = Case::new(&["b", "c", "d"], &["b", "d"], 1, 3)
            .reach("c", DfsWriteSetMemberReach::NoPath);
        case.record().substitutes.push(DfsMetadataShardSubstitute {
            node_id: node("d"),
            recruited_at_generation: 4,
        });
        assert_eq!(case.plan(), DfsWriteSetKeepingPlan::default());
    }

    #[test]
    fn copies_stay_on_non_mobile_members_and_a_mobile_one_is_replaced() {
        let mut case = Case::new(&["m"], &["m"], 1, 2).reach("d", DfsWriteSetMemberReach::Path);
        case.members.get_mut(&node("m")).unwrap().mobile = true;
        assert_eq!(
            case.plan().txn,
            Some(DfsWriteSetKeepingTxn::Recruit {
                member: node("d"),
                admission: DfsMetadataShardRecruit::Replacement,
            })
        );
        let mut case = Case::new(&["m", "d"], &["m", "d"], 1, 2);
        case.members.get_mut(&node("m")).unwrap().mobile = true;
        case.record().substitutes.push(DfsMetadataShardSubstitute {
            node_id: node("d"),
            recruited_at_generation: 4,
        });
        assert_eq!(
            case.plan().txn,
            Some(DfsWriteSetKeepingTxn::Remove {
                members: [node("m")].into_iter().collect(),
                retain_at_least: 2,
                retire_only: false,
                cause: DfsWriteSetRemoveCause::MobileSurplus,
            })
        );
        // With no non-mobile member to take its place, the mobile one stays.
        let mut case = Case::new(&["m"], &["m"], 2, 2);
        case.members.get_mut(&node("m")).unwrap().mobile = true;
        assert_eq!(case.plan(), DfsWriteSetKeepingPlan::default());
    }

    #[test]
    fn a_commit_above_the_accepted_w_raises_n_and_recruits_up_to_it() {
        let mut case = Case::new(&["b"], &["b"], 1, 2).reach("d", DfsWriteSetMemberReach::Path);
        case.demanded_w = 3;
        assert_eq!(case.plan().txn, Some(DfsWriteSetKeepingTxn::RaiseN { w: 3 }));
        case.demanded_w = 0;
        case.record().raised_n = Some(3);
        assert_eq!(
            case.plan().txn,
            Some(DfsWriteSetKeepingTxn::Recruit {
                member: node("d"),
                admission: DfsMetadataShardRecruit::BelowN,
            })
        );
        // A recruit made for the raise is not surplus below it.
        let mut case = Case::new(&["b", "d"], &["b", "d"], 1, 2);
        case.record().raised_n = Some(3);
        case.record().substitutes.push(DfsMetadataShardSubstitute {
            node_id: node("d"),
            recruited_at_generation: 4,
        });
        assert_eq!(case.plan().txn, None);
    }

    #[test]
    fn a_departed_member_leaves_without_a_network_verdict() {
        let mut case = Case::new(&["b", "c"], &["b", "c"], 1, 3);
        case.members.get_mut(&node("c")).unwrap().departed = true;
        assert_eq!(
            case.plan().txn,
            Some(DfsWriteSetKeepingTxn::Shrink {
                member: node("c"),
                cause: DfsWriteSetShrinkCause::MemberDeparted,
            })
        );
        let mut case = Case::new(&["b", "c"], &["b"], 1, 3);
        case.members.get_mut(&node("c")).unwrap().departed = true;
        assert!(matches!(
            case.plan().txn,
            Some(DfsWriteSetKeepingTxn::Remove {
                cause: DfsWriteSetRemoveCause::MemberDeparted,
                retire_only: true,
                ..
            })
        ));
    }

    #[test]
    fn a_member_that_refuses_the_epoch_is_shrunk_above_the_floor_only() {
        let mut case = Case::new(&["b", "c"], &["b", "c"], 1, 3);
        case.members.get_mut(&node("b")).unwrap().fenced = true;
        assert_eq!(
            case.plan().txn,
            Some(DfsWriteSetKeepingTxn::Shrink {
                member: node("b"),
                cause: DfsWriteSetShrinkCause::MemberFenced,
            })
        );
        let mut case = Case::new(&["b", "c"], &["b"], 2, 3);
        case.members.get_mut(&node("b")).unwrap().fenced = true;
        assert!(
            !matches!(case.plan().txn, Some(DfsWriteSetKeepingTxn::Shrink { .. })),
            "at the floor, writes wait for the takeover to settle"
        );
    }

    #[test]
    fn the_switch_waits_for_every_configured_member() {
        let mut case = Case::new(&["b", "c"], &["b", "c"], 1, 2);
        case.state = DfsMetadataShardWriteSetState::Unswitched;
        assert_eq!(case.plan().txn, Some(DfsWriteSetKeepingTxn::Switch));
        case.members.get_mut(&node("c")).unwrap().eligible = false;
        assert_eq!(case.plan().txn, None);
    }
}
