//! Ranking, quorum, round and promise decisions of a converting takeover,
//! and the stage admission of a takeover attempt.
use super::promise::*;
use super::*;
use crate::authority::staged_service::conversion_install::{StagedAttempt, admit_conversion_stage};

fn node(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).unwrap()
}

fn rank(name: &str, sequence: u64) -> TakeoverRank {
    TakeoverRank {
        node_id: node(name),
        source_journal_sequence: sequence,
    }
}

fn shard(primary: &str, replicas: &[&str], epoch: u64) -> DfsMetadataShardConfig {
    DfsMetadataShardConfig {
        file_system_id: sinomo_dfs::sifs_file_system_id(),
        shard_id: "users".into(),
        primary_node_id: node(primary),
        replica_node_ids: replicas.iter().map(|name| node(name)).collect(),
        epoch,
        assignment: sinomo_dfs::DfsMetadataShardAssignment::PathPrefix {
            prefix: "/users".into(),
        },
        default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
    }
}

fn entry(key: String, value: &impl Serialize) -> KvEntry {
    KvEntry {
        key,
        value: sinomo_core::kv::encode_kv_value(value).unwrap(),
        create_revision: sinomo_core::kv::KvRevision(1),
        modify_revision: sinomo_core::kv::KvRevision(1),
        version: sinomo_core::kv::KvVersion(1),
        lease_id: None,
    }
}

fn write_set(shard: &DfsMetadataShardConfig, members: &[&str]) -> KvEntry {
    let mut write_set = members.iter().map(|name| node(name)).collect::<Vec<_>>();
    write_set.sort();
    entry(
        sinomo_dfs::metadata_shard_write_set_key(&shard.file_system_id, &shard.shard_id),
        &sinomo_dfs::DfsMetadataShardWriteSet {
            format: sinomo_dfs::DFS_METADATA_SHARD_WRITE_SET_FORMAT,
            file_system_id: shard.file_system_id.clone(),
            shard_id: shard.shard_id.clone(),
            epoch: shard.epoch,
            primary_node_id: shard.primary_node_id.clone(),
            generation: 1,
            write_set,
            epoch_start: None,
            switched_at_epoch: shard.epoch,
            substitutes: Vec::new(),
            raised_n: None,
            last_change: sinomo_dfs::DfsMetadataShardWriteSetChange {
                kind: sinomo_dfs::DfsMetadataShardWriteSetChangeKind::Switch,
                change_id: "switch".into(),
            },
        },
    )
}

fn set(names: &[&str]) -> BTreeSet<NodeId> {
    names.iter().map(|name| node(name)).collect()
}

/// A member's state with its own copy ranked at `own`.
fn ranked(own: u64) -> PromiseState {
    let mut state = PromiseState::default().for_key(3, &node("primary-p"), 1);
    state.own = Some(own);
    state
}

#[test]
fn ranking_prefers_the_most_advanced_copy_then_the_smaller_node_id() {
    // Node ids order by their own encoding, not by the names they are made from.
    let (smaller, larger) = if node("b") < node("d") {
        ("b", "d")
    } else {
        ("d", "b")
    };
    let ranked =
        rank_takeover_candidates(vec![rank("c", 7), rank("d", 9), rank("b", 9), rank("e", 8)]);
    assert_eq!(
        ranked
            .iter()
            .map(|rank| rank.node_id.clone())
            .collect::<Vec<_>>(),
        vec![node(smaller), node(larger), node("e"), node("c")]
    );
    assert_eq!(
        rank(smaller, 9).cmp_rank(&rank(larger, 9)),
        std::cmp::Ordering::Greater
    );
    assert_eq!(
        rank(larger, 10).cmp_rank(&rank(smaller, 9)),
        std::cmp::Ordering::Greater
    );
}

#[test]
fn a_switched_shard_ranks_within_its_write_set_and_needs_every_member_of_it() {
    let shard = shard("primary-p", &["a", "b", "c"], 3);
    let switched = write_set(&shard, &["primary-p", "a", "b"]);
    let quorum = TakeoverQuorum::for_shard(&shard, Some(&switched), &[]).unwrap();
    assert_eq!(
        quorum.members(),
        set(&["a", "b"]),
        "a configured member outside the write set is no candidate"
    );
    assert!(quorum.satisfied(&set(&["a", "b"])));
    assert!(
        !quorum.satisfied(&set(&["a", "c"])),
        "a member outside the write set does not count"
    );
    assert!(
        !quorum.satisfied(&set(&["a", "primary-p"])),
        "the lost primary never counts"
    );
    // A write set of the primary alone has no candidate.
    let alone =
        TakeoverQuorum::for_shard(&shard, Some(&write_set(&shard, &["primary-p"])), &[]).unwrap();
    assert!(alone.members().is_empty());
    assert!(!alone.satisfied(&BTreeSet::new()));
}

#[test]
fn an_unswitched_shard_needs_a_majority_of_its_configured_members() {
    let five = shard("primary-p", &["a", "b", "c", "d"], 3);
    let quorum = TakeoverQuorum::for_shard(&five, None, &[]).unwrap();
    assert_eq!(quorum.members(), set(&["a", "b", "c", "d"]));
    assert!(
        quorum.satisfied(&set(&["a", "b", "c"])),
        "three of five configured members"
    );
    assert!(!quorum.satisfied(&set(&["a", "b"])));
    assert!(
        !quorum.satisfied(&set(&["a", "b", "primary-p"])),
        "the lost primary never counts"
    );
    let three = shard("primary-p", &["a", "b"], 3);
    let quorum = TakeoverQuorum::for_shard(&three, None, &[]).unwrap();
    assert!(quorum.satisfied(&set(&["a", "b"])));
    assert!(!quorum.satisfied(&set(&["a"])));
    // A write set bound to an older serving record leaves the all-member rule.
    let stale = write_set(&shard("primary-p", &["a", "b"], 2), &["primary-p", "a"]);
    assert!(matches!(
        TakeoverQuorum::for_shard(&three, Some(&stale), &[]).unwrap(),
        TakeoverQuorum::Majority { .. }
    ));
}

/// A member promises only the best-ranked candidate it knows of that is not
/// lost, itself included, and refuses every other.
#[test]
fn a_member_promises_one_candidate_the_best_ranked_one_it_knows() {
    let local = node("m");
    let never_lost = |_: &NodeId| false;
    // Until it ranks its own copy it promises nobody, but learns the asker.
    let mut state = PromiseState::default().for_key(3, &node("primary-p"), 1);
    assert_eq!(
        decide_promise(&mut state, &local, &rank("a", 9), never_lost),
        Err(Refusal::Unranked)
    );
    assert_eq!(state.known.get(&node("a")), Some(&9));
    assert_eq!(state.promised, None);
    // A less advanced candidate than the member itself is refused.
    let mut state = ranked(8);
    assert_eq!(
        decide_promise(&mut state, &local, &rank("a", 7), never_lost),
        Err(Refusal::Outranked { by: rank("m", 8) })
    );
    // A more advanced one is promised; then a less advanced one is refused
    // in its favour, and the promised one again is idempotent.
    assert_eq!(
        decide_promise(&mut state, &local, &rank("b", 9), never_lost),
        Ok(())
    );
    assert_eq!(state.promised, Some(node("b")));
    assert_eq!(
        decide_promise(&mut state, &local, &rank("c", 8), never_lost),
        Err(Refusal::Outranked { by: rank("b", 9) })
    );
    assert_eq!(state.promised, Some(node("b")));
    assert_eq!(
        decide_promise(&mut state, &local, &rank("b", 9), never_lost),
        Ok(())
    );
    // The member as its own candidate is refused while it knows better.
    assert_eq!(
        decide_promise(&mut state, &local, &rank("m", 8), never_lost),
        Err(Refusal::Outranked { by: rank("b", 9) })
    );
    let mut alone = ranked(8);
    assert_eq!(
        decide_promise(&mut alone, &local, &rank("m", 8), never_lost),
        Ok(())
    );
    assert_eq!(alone.promised, Some(local.clone()));
    // Equal copies rank by node id, the smaller first.
    for name in ["a", "z"] {
        let mut tie = ranked(9);
        let decision = decide_promise(&mut tie, &local, &rank(name, 9), never_lost);
        assert_eq!(decision.is_ok(), node(name) < local, "{name}: {decision:?}");
    }
}

/// A promise moves away from a candidate only once the member judges it
/// lost, and a lost candidate does not outrank anyone.
#[test]
fn a_promise_is_released_once_its_candidate_is_lost() {
    let local = node("m");
    let mut state = ranked(5);
    assert_eq!(
        decide_promise(&mut state, &local, &rank("b", 9), |_| false),
        Ok(())
    );
    let lost_b = |node: &NodeId| *node == NodeId::from_legacy_named("b").unwrap();
    assert_eq!(
        decide_promise(&mut state, &local, &rank("c", 7), |_| false),
        Err(Refusal::Outranked { by: rank("b", 9) }),
        "the promised candidate is live"
    );
    assert_eq!(
        decide_promise(&mut state, &local, &rank("c", 7), lost_b),
        Ok(()),
        "released from a lost candidate"
    );
    assert_eq!(state.promised, Some(node("c")));
    // A candidate is never judged lost by its own request.
    assert_eq!(
        decide_promise(&mut state, &local, &rank("b", 9), lost_b),
        Ok(())
    );
}

/// Without a loss a member's promise only moves up the rank order, so no
/// sequence of requests makes it cycle.
#[test]
fn without_a_loss_a_promise_only_moves_up_and_settles_on_the_best() {
    let local = node("m");
    let candidates = [rank("a", 4), rank("b", 9), rank("c", 6), rank("d", 9)];
    let best = rank_takeover_candidates(candidates.to_vec())[0].clone();
    // Every order of arrival, each candidate asking twice.
    let orders = [[0, 1, 2, 3], [3, 2, 1, 0], [2, 0, 3, 1], [1, 3, 0, 2]];
    for order in orders {
        let mut state = ranked(3);
        let mut promised = Vec::new();
        for index in order.iter().chain(order.iter()) {
            if decide_promise(&mut state, &local, &candidates[*index], |_| false).is_ok()
                && promised.last() != Some(&candidates[*index])
            {
                promised.push(candidates[*index].clone());
            }
        }
        assert!(
            promised
                .windows(2)
                .all(|pair| pair[1].cmp_rank(&pair[0]) == std::cmp::Ordering::Greater),
            "{order:?}: {promised:?}"
        );
        assert_eq!(state.promised.as_ref(), Some(&best.node_id), "{order:?}");
    }
}

#[test]
fn a_new_epoch_lost_primary_or_round_starts_a_new_promise_record() {
    let mut state = ranked(5);
    decide_promise(&mut state, &node("m"), &rank("b", 9), |_| false).unwrap();
    assert_eq!(state.clone().for_key(3, &node("primary-p"), 1), state);
    for (epoch, primary, round) in [(4, "primary-p", 1), (3, "other", 1), (3, "primary-p", 2)] {
        let fresh = state.clone().for_key(epoch, &node(primary), round);
        assert_eq!(
            (fresh.own, fresh.promised.clone(), fresh.known.len()),
            (None, None, 0),
            "{epoch} {primary} {round}"
        );
    }
}

/// A member promises one candidate per round: a promise of one round never
/// carries into another, so the next round starts with nobody promised and
/// nobody ranked, and a member's promise in it moves by the same rule.
#[test]
fn promises_are_kept_per_round() {
    let local = node("m");
    let mut first = ranked(5);
    decide_promise(&mut first, &local, &rank("b", 9), |_| false).unwrap();
    assert_eq!(first.promised, Some(node("b")));
    // Round 2, once b is lost: a fresh record, which ranks again.
    let mut second = first.clone().for_key(3, &node("primary-p"), 2);
    assert_eq!(
        (second.round, &second.promised, second.own),
        (2, &None, None)
    );
    assert_eq!(
        decide_promise(&mut second, &local, &rank("c", 7), |_| false),
        Err(Refusal::Unranked),
        "the member ranks its own copy again in the new round"
    );
    second.own = Some(5);
    assert_eq!(
        decide_promise(&mut second, &local, &rank("c", 7), |_| false),
        Ok(())
    );
    assert_eq!(
        decide_promise(&mut second, &local, &rank("d", 6), |_| false),
        Err(Refusal::Outranked { by: rank("c", 7) }),
        "one promise per round"
    );
    assert_eq!(
        first.promised,
        Some(node("b")),
        "round 1's record is untouched"
    );
    // A request of another round never reuses this round's record.
    let back = second.clone().for_key(3, &node("primary-p"), 1);
    assert_eq!((back.round, back.promised), (1, None));
}

fn storage_record(
    shard: &DfsMetadataShardConfig,
    converter: Option<&str>,
    takeover: Option<DfsMetadataConversionTakeover>,
) -> DfsMetadataShardStorage {
    let mut attempt = takeover_attempt(shard, converter.unwrap_or("unused"));
    attempt.seal = None;
    attempt.contributions_digest = None;
    attempt.takeover = takeover;
    attempt.primary_node_id = converter.map_or_else(|| shard.primary_node_id.clone(), node);
    attempt.attempt_id = sinomo_dfs::metadata_shard_conversion_attempt_id(
        &shard.file_system_id,
        &shard.shard_id,
        &shard.assignment,
        shard.epoch,
        &attempt.primary_node_id,
        &attempt.source,
        attempt.takeover.as_ref(),
    )
    .unwrap();
    let record = DfsMetadataShardStorage::converting(shard, attempt);
    record.validate_against(shard).unwrap();
    record
}

/// A member moves to the next takeover round only once it judges the
/// recorded converter lost, or the serving record no longer admits it; a
/// live converter continues its round and is never replaced. The next round
/// is the recorded one plus one, counting every earlier converter but its
/// own lost.
#[test]
fn a_member_moves_to_the_next_round_only_once_the_recorded_converter_is_lost() {
    let current = shard("primary-p", &["a", "b", "c"], 3);
    let first = DfsMetadataConversionTakeover::first_round(node("primary-p"));
    let lost = |names: &'static [&'static str]| {
        move |candidate: &NodeId| names.iter().any(|name| node(name) == *candidate)
    };
    let round =
        |record: Option<&DfsMetadataShardStorage>, at: &str, judged: &'static [&'static str]| {
            match takeover_round(&current, None, record, &node(at), lost(judged)) {
                TakeoverRound::First(takeover) => ("first", Some(takeover)),
                TakeoverRound::Continue(attempt) => ("continue", attempt.takeover.clone()),
                TakeoverRound::Next { takeover, replaced } => {
                    assert_eq!(
                        Some(&replaced.primary_node_id),
                        record
                            .and_then(|r| r.converting_attempt())
                            .map(|a| &a.primary_node_id)
                    );
                    ("next", Some(takeover))
                }
                TakeoverRound::Recorded(attempt) => ("recorded", attempt.takeover.clone()),
            }
        };
    // Nothing recorded at the epoch, or only the primary's own attempt: the
    // first round, resting on the primary's loss.
    assert_eq!(round(None, "a", &[]), ("first", Some(first.clone())));
    let own = storage_record(&current, None, None);
    assert_eq!(
        round(Some(&own), "a", &["primary-p"]),
        ("first", Some(first.clone()))
    );
    // Round 1 by a: a continues it; b parks while a is live to it.
    let by_a = storage_record(&current, Some("a"), Some(first.clone()));
    assert_eq!(
        round(Some(&by_a), "a", &["a"]),
        ("continue", Some(first.clone()))
    );
    assert_eq!(
        round(Some(&by_a), "b", &["primary-p"]),
        ("recorded", Some(first.clone()))
    );
    // Once b judges a lost, b runs round 2, counting a lost.
    let second = first.next_round(&node("a"), &node("b"));
    assert_eq!(
        round(Some(&by_a), "b", &["a"]),
        ("next", Some(second.clone()))
    );
    assert_eq!(
        (second.round, second.lost_converters.clone()),
        (2, vec![node("a")])
    );
    // A converter the serving record no longer admits is lost to everyone.
    let without_a = shard("primary-p", &["b", "c"], 3);
    match takeover_round(&without_a, None, Some(&by_a), &node("b"), |_| false) {
        TakeoverRound::Next { takeover, .. } => assert_eq!(takeover, second),
        other => panic!("{other:?}"),
    }
    match takeover_round(&without_a, None, Some(&by_a), &node("a"), |_| false) {
        TakeoverRound::Recorded(_) => {}
        other => panic!("a converter no longer admitted runs nothing: {other:?}"),
    }
    let switched = write_set(&current, &["primary-p", "b", "c"]);
    match takeover_round(&current, Some(&switched), Some(&by_a), &node("c"), |_| {
        false
    }) {
        TakeoverRound::Next { takeover, .. } => {
            assert_eq!(takeover, first.next_round(&node("a"), &node("c")))
        }
        other => panic!("a converter outside the write set is lost: {other:?}"),
    }
    // Round 2 by b: a returned converter of round 1 runs round 3 without
    // counting itself lost; c counts both lost.
    let by_b = storage_record(&current, Some("b"), Some(second.clone()));
    assert_eq!(
        round(Some(&by_b), "a", &[]),
        ("recorded", Some(second.clone()))
    );
    let (_, from_a) = round(Some(&by_b), "a", &["b"]);
    assert_eq!(
        from_a.map(|t| (t.round, t.lost_converters)),
        Some((3, vec![node("b")]))
    );
    let (_, from_c) = round(Some(&by_b), "c", &["b"]);
    let mut both = vec![node("a"), node("b")];
    both.sort();
    assert_eq!(
        from_c.map(|t| (t.round, t.lost_converters)),
        Some((3, both))
    );
    // A serving change ends the epoch's takeover: the next epoch starts over.
    let moved = shard("b", &["primary-p", "a", "c"], 4);
    match takeover_round(&moved, None, Some(&by_b), &node("a"), |_| true) {
        TakeoverRound::First(takeover) => assert_eq!(
            takeover,
            DfsMetadataConversionTakeover::first_round(node("b"))
        ),
        other => panic!("{other:?}"),
    }
}

/// On a switched shard, where every candidate must promise, a round's
/// quorum leaves out the earlier converters it counts lost; an unswitched
/// shard's majority never needs any one member.
#[test]
fn a_round_quorum_does_not_wait_for_the_converters_it_counts_lost() {
    let current = shard("primary-p", &["a", "b", "c"], 3);
    let switched = write_set(&current, &["primary-p", "a", "b"]);
    let lost_a = [node("a")];
    let quorum = TakeoverQuorum::for_shard(&current, Some(&switched), &lost_a).unwrap();
    assert_eq!(quorum.members(), set(&["b"]));
    assert!(
        quorum.satisfied(&set(&["b"])),
        "the lost converter is not needed"
    );
    let majority = TakeoverQuorum::for_shard(&current, None, &lost_a).unwrap();
    assert_eq!(
        majority,
        TakeoverQuorum::for_shard(&current, None, &[]).unwrap()
    );
    assert!(majority.satisfied(&set(&["b", "c", "a"])));
    assert!(!majority.satisfied(&set(&["b"])));
}

/// A sealed takeover attempt of `converter` under `shard`.
fn takeover_attempt(shard: &DfsMetadataShardConfig, converter: &str) -> DfsMetadataShardConversion {
    let source = sinomo_dfs::DfsMetadataConversionSource {
        source_identity: "takeover-source".into(),
        source_format: Some(10),
        source_journal_sequence: 9,
        source_state_fingerprint: "takeover-state".into(),
        source_cut_digest: "a".repeat(64),
        pending_mapping_digest: "b".repeat(64),
    };
    let takeover = DfsMetadataConversionTakeover::first_round(shard.primary_node_id.clone());
    DfsMetadataShardConversion {
        attempt_id: sinomo_dfs::metadata_shard_conversion_attempt_id(
            &shard.file_system_id,
            &shard.shard_id,
            &shard.assignment,
            shard.epoch,
            &node(converter),
            &source,
            Some(&takeover),
        )
        .unwrap(),
        epoch: shard.epoch,
        primary_node_id: node(converter),
        takeover: Some(takeover),
        source,
        seal: Some(DfsMetadataGenerationSeal {
            prefix_proof: sinomo_dfs::DfsMetadataPrefixProof {
                version: 2,
                journal_sequence: 9,
                digest: format!("dfs-metadata-prefix-v2:blake3:{}", "1".repeat(64)),
            },
            state_fingerprint: "image-state".into(),
            file_fingerprint: "image-file".into(),
        }),
        contributions_digest: Some("c".repeat(64)),
    }
}

/// Only a takeover recorded under the current serving record, by a
/// converter it still admits, is the epoch's takeover.
#[test]
fn a_recorded_takeover_must_run_under_the_serving_record() {
    let current = shard("primary-p", &["a", "b"], 3);
    let attempt = takeover_attempt(&current, "a");
    let record = DfsMetadataShardStorage::converting(&current, attempt.clone());
    assert_eq!(
        recorded_takeover(&current, None, Some(&record)),
        Some(&attempt)
    );
    let switched = write_set(&current, &["primary-p", "b"]);
    assert_eq!(
        recorded_takeover(&current, Some(&switched), Some(&record)),
        None,
        "a converter outside the write set"
    );
    let moved = DfsMetadataShardConfig {
        epoch: 4,
        ..current.clone()
    };
    assert_eq!(recorded_takeover(&moved, None, Some(&record)), None);
    let own = DfsMetadataShardStorage::converting(
        &current,
        DfsMetadataShardConversion {
            takeover: None,
            primary_node_id: current.primary_node_id.clone(),
            attempt_id: sinomo_dfs::metadata_shard_conversion_attempt_id(
                &current.file_system_id,
                &current.shard_id,
                &current.assignment,
                current.epoch,
                &current.primary_node_id,
                &attempt.source,
                None,
            )
            .unwrap(),
            ..attempt.clone()
        },
    );
    assert_eq!(
        recorded_takeover(&current, None, Some(&own)),
        None,
        "the primary's own attempt"
    );
}

/// A member admits a takeover attempt's stage only from its converter, at
/// the shard's current epoch, when it is in the takeover's install set.
#[test]
fn a_member_admits_a_takeover_stage_only_from_its_converter() {
    let current = shard("primary-p", &["a", "b", "c"], 3);
    let attempt = takeover_attempt(&current, "a");
    let storage = entry(
        sinomo_dfs::metadata_shard_storage_key(&current.file_system_id, &current.shard_id),
        &DfsMetadataShardStorage::converting(&current, attempt.clone()),
    );
    let shard_entry = entry(
        sinomo_dfs::metadata_shard_key(&current.file_system_id, &current.shard_id),
        &current,
    );
    let request = StagedAttempt {
        file_system_id: current.file_system_id.clone(),
        shard_id: current.shard_id.clone(),
        attempt_id: attempt.attempt_id.clone(),
        epoch: attempt.epoch,
        seal: attempt.seal.clone().unwrap(),
    };
    let admit = |local: &str, source: &str, shard: &KvEntry, write_set: Option<&KvEntry>| {
        admit_conversion_stage(
            &node(local),
            &node(source),
            &request,
            Some(shard),
            write_set,
            Some(&storage),
        )
    };
    assert_eq!(
        admit("b", "a", &shard_entry, None).unwrap().attempt,
        attempt
    );
    for (local, source) in [("b", "primary-p"), ("b", "c")] {
        let refused = admit(local, source, &shard_entry, None)
            .unwrap_err()
            .to_string();
        assert!(
            refused.contains("not the converter of a takeover"),
            "{refused}"
        );
    }
    // On a switched shard the takeover installs on its write set without the
    // lost primary.
    let switched = write_set(&current, &["primary-p", "a", "b"]);
    admit("b", "a", &shard_entry, Some(&switched)).unwrap();
    assert!(
        admit("c", "a", &shard_entry, Some(&switched))
            .unwrap_err()
            .to_string()
            .contains("not a member")
    );
    // Once the epoch moved, the attempt is no longer admitted.
    let moved = entry(
        shard_entry.key.clone(),
        &DfsMetadataShardConfig {
            epoch: 4,
            ..current.clone()
        },
    );
    assert!(
        admit("b", "a", &moved, None)
            .unwrap_err()
            .to_string()
            .contains("current epoch")
    );
}

/// A takeover parked on how this node judges its primary is requeued on any
/// change of that verdict, never by a timer; a change during its step reruns
/// it once it parks.
#[test]
fn a_unit_parked_on_a_verdict_is_requeued_when_the_verdict_changes() {
    use crate::authority::owner::inventory::{ObservedState, UnitKey, UnitRole};
    use crate::authority::owner::scheduler::Scheduler;
    let unit = |shard: &str| UnitKey {
        relative_root: Path::new("dfs/metadata").join(shard),
        role: UnitRole::Metadata {
            shard: sinomo_dfs::DfsMetadataShardId::new(shard),
        },
    };
    let waiting = || ObservedState::Waiting {
        evidence: "the primary is not judged lost".into(),
    };
    let mut scheduler = Scheduler::new();
    let (users, services) = (unit("users"), unit("services"));
    for (key, primary) in [(&users, "primary-p"), (&services, "primary-q")] {
        scheduler.enqueue(key.clone(), false).unwrap();
        let (id, mut reference) = scheduler.next().unwrap();
        scheduler.watch_node_verdicts(key, set(&[primary])).unwrap();
        reference.state = waiting();
        scheduler.finish(&id, reference);
    }
    assert!(scheduler.next().is_none());
    assert!(
        scheduler.next_retry().is_none(),
        "a parked takeover owns no timer"
    );
    assert_eq!(
        scheduler.watched_nodes(),
        set(&["primary-p", "primary-q"]),
        "the watcher subscribes for them"
    );
    assert!(!scheduler.node_verdicts_changed(&set(&["other"])));
    assert!(
        !scheduler.nodes_became_reachable(&set(&["primary-p"])),
        "becoming reachable alone is no verdict wake"
    );
    assert!(scheduler.node_verdicts_changed(&set(&["primary-p"])));
    let (id, mut woken) = scheduler.next().expect("the unit watching P is requeued");
    assert_eq!(woken.unit, users);
    assert!(
        scheduler.next().is_none(),
        "the unit watching Q keeps waiting"
    );
    woken.state = ObservedState::VerifiedCurrent;
    scheduler.finish(&id, woken);
    assert_eq!(
        scheduler.watched_nodes(),
        set(&["primary-q"]),
        "a settled unit drops its watch"
    );
    // A change while a step runs, before it registers its watch, reruns it.
    let fresh = unit("root");
    scheduler.enqueue(fresh.clone(), false).unwrap();
    let (id, mut running) = scheduler.next().unwrap();
    assert!(!scheduler.node_verdicts_changed(&set(&["primary-r"])));
    scheduler
        .watch_node_verdicts(&fresh, set(&["primary-r"]))
        .unwrap();
    running.state = waiting();
    scheduler.finish(&id, running);
    let (_, rerun) = scheduler
        .next()
        .expect("the earlier change reruns the parked step");
    assert_eq!(rerun.unit, fresh);
    assert!(
        scheduler
            .watch_node_verdicts(&unit("unowned"), set(&["primary-p"]))
            .is_err()
    );
}
