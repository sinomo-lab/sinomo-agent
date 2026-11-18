//! Converting takeover: a member converts a closed shard whose primary is
//! lost, from its own copy, and its activation also makes it primary at the
//! next epoch.
//!
//! A closed shard has no open current-format store, so the write-set takeover
//! of an open shard cannot run on it. Instead, a member of the shard's
//! `Metadata{shard}` unit converts in the primary's place when this node
//! judges the primary lost: it left the cluster, or this node's network
//! settled that there is no path to it (the verdicts the write-set takeover
//! planner uses). The unit is woken by changes of the shard's keys and of its
//! verdicts, never by a timer.
//!
//! **Candidates and quorum.** On a switched shard the candidates are the
//! write set without the lost primary, and every one of them must promise;
//! on an unswitched shard they are the configured members without it, and a
//! majority of the configured members must promise. Every configured member,
//! the lost primary included, must advertise the storage capability unless
//! it departed the cluster, so the lost primary cannot still serve the old
//! format.
//!
//! **Rank.** A candidate's rank is the journal sequence of the source it
//! captured of its own copy; ties go to the smaller node id. A member promises
//! only the best-ranked candidate it knows of that is not lost ([`promise`]),
//! so the converter is the most advanced member of its quorum.
//!
//! **Steps.** A candidate captures its own copy, fixes its takeover attempt
//! from that capture, collects its promise quorum and records the attempt
//! Converting; from then on it converts exactly as a primary does: it reads
//! the contributions, builds and seals the image, installs it under the rule
//! of the configuration its activation makes current, and activates with
//! [`sinomo_dfs::build_metadata_shard_storage_takeover_activate`]. Receivers
//! activate their staged image under the new serving record.
//!
//! **Rounds.** Once a takeover attempt is recorded at the epoch, the epoch is
//! the takeover's: the primary no longer converts there, and every other
//! member parks while the recorded converter is live. A member that judges
//! the recorded converter lost by the same verdicts, or whose serving record
//! no longer admits it, may run the next round
//! ([`DfsMetadataConversionTakeover::next_round`]): its promises are keyed by
//! the round, its members promise it only once they judge that converter lost
//! too, and its Converting transaction replaces exactly the recorded round.
//! A round counts every earlier converter but its own as lost, so on a
//! switched shard neither its promises nor its install wait for one; they
//! repair by snapshot once the shard is active. So the shard never waits
//! for a lost converter, as it never waits for the lost primary.
use super::*;
use sinomo_core::node::NodeId;

pub(in crate::authority) mod promise;
#[cfg(test)]
mod tests;

use promise::{PeerReach, PromiseReply, PromiseRequest, TakeoverQuorum, TakeoverRank};

/// What a converting takeover needs from its node beyond the conversion
/// environment.
pub(in crate::authority) trait TakeoverEnv {
    /// This node's settled verdict of its path to `node`.
    fn reach(&self, node: &NodeId) -> PeerReach;
    /// Wake the unit on any change of this node's verdict of `nodes`; the
    /// set replaces the previous one.
    fn watch_verdicts(&self, nodes: BTreeSet<NodeId>) -> Result<()>;
    /// Record this node's own rank under `request`'s key.
    fn record_rank(&self, request: &PromiseRequest) -> Result<()>;
    /// Ask `member`, this node included, to promise `request`'s candidate.
    fn promise(&self, member: &NodeId, request: &PromiseRequest) -> Result<PromiseReply>;
}

/// Whether the shard's member records say `node` left the cluster, by the
/// write-set keeper's departed verdict
/// ([`DfsMetadataShardStorageMember::departed`]). A node that is not a
/// configured member has no records here and counts as departed.
fn departed(keys: &ShardKeys, node: &NodeId) -> bool {
    keys.members
        .iter()
        .find(|(member, _, _)| member == node)
        .is_none_or(|(node_id, member, policy)| {
            DfsMetadataShardStorageMember {
                node_id: node_id.clone(),
                member: member.as_ref(),
                policy: policy.as_ref(),
            }
            .departed()
        })
}

/// The planner's loss verdict: `node` departed, or this node's network
/// settled that it has no path to it.
fn judged_lost(facilities: &dyn TakeoverEnv, keys: &ShardKeys, node: &NodeId) -> bool {
    departed(keys, node) || facilities.reach(node) == PeerReach::NoPath
}

/// Whether the serving record still admits `attempt`'s converter for its
/// takeover round.
fn converter_admitted(
    shard: &DfsMetadataShardConfig,
    write_set: Option<&KvEntry>,
    attempt: &DfsMetadataShardConversion,
) -> bool {
    attempt.takeover.as_ref().is_some_and(|takeover| {
        DfsMetadataShardConversionInstallRule::for_takeover(
            shard,
            write_set,
            &attempt.primary_node_id,
            takeover,
        )
        .is_ok()
    })
}

/// The takeover attempt recorded at the shard's current epoch whose
/// converter the serving record still admits, if any.
#[cfg(test)]
pub(in crate::authority) fn recorded_takeover<'a>(
    shard: &DfsMetadataShardConfig,
    write_set: Option<&KvEntry>,
    record: Option<&'a DfsMetadataShardStorage>,
) -> Option<&'a DfsMetadataShardConversion> {
    record
        .and_then(|record| record.epoch_takeover(shard))
        .filter(|attempt| converter_admitted(shard, write_set, attempt))
}

/// What the shard's takeover at its current epoch asks of one node.
#[derive(Debug, Eq, PartialEq)]
pub(in crate::authority) enum TakeoverRound<'a> {
    /// No takeover is recorded at the epoch: the first round, which rests on
    /// the primary's loss.
    First(DfsMetadataConversionTakeover),
    /// The node is the converter of the round recorded at the epoch, which
    /// the serving record still admits: it continues that attempt.
    Continue(&'a DfsMetadataShardConversion),
    /// The recorded round's converter is lost: the node may run this next
    /// round in its place.
    Next {
        takeover: DfsMetadataConversionTakeover,
        replaced: &'a DfsMetadataShardConversion,
    },
    /// The recorded round's converter is not lost, or it is the node itself
    /// and no longer admitted: the node runs no round.
    Recorded(&'a DfsMetadataShardConversion),
}

/// The round `node` may take part in under the shard's records, where `lost`
/// is a loss verdict of the recorded converter by the node that asks. A
/// converter the serving record no longer admits is lost to every node. The
/// next round is derived from the recorded one, so rounds only increase, and
/// a node moves to it only once the recorded converter is lost, so a live
/// converter is never replaced.
pub(in crate::authority) fn takeover_round<'a>(
    shard: &DfsMetadataShardConfig,
    write_set: Option<&KvEntry>,
    record: Option<&'a DfsMetadataShardStorage>,
    node: &NodeId,
    lost: impl Fn(&NodeId) -> bool,
) -> TakeoverRound<'a> {
    let Some(recorded) = record.and_then(|record| record.epoch_takeover(shard)) else {
        return TakeoverRound::First(DfsMetadataConversionTakeover::first_round(
            shard.primary_node_id.clone(),
        ));
    };
    let converter = &recorded.primary_node_id;
    let admitted = converter_admitted(shard, write_set, recorded);
    if converter == node {
        return if admitted {
            TakeoverRound::Continue(recorded)
        } else {
            TakeoverRound::Recorded(recorded)
        };
    }
    if admitted && !lost(converter) {
        return TakeoverRound::Recorded(recorded);
    }
    TakeoverRound::Next {
        takeover: DfsMetadataConversionTakeover::for_converter(shard, record, node),
        replaced: recorded,
    }
}

/// Delete every attempt of this node that no longer runs: on a node other
/// than the primary only this node's attempt of `takeover` at the current
/// epoch can.
fn remove_superseded(
    shard_files: &ShardFiles,
    shard: &DfsMetadataShardConfig,
    local: &NodeId,
    takeover: &DfsMetadataConversionTakeover,
) -> Result<()> {
    for (_, files) in shard_files.attempts()? {
        let current = files
            .read::<AttemptRecord>("attempt.json")?
            .is_some_and(|record| {
                record.attempt.epoch == shard.epoch
                    && record.attempt.primary_node_id == *local
                    && record.attempt.takeover.as_ref() == Some(takeover)
            });
        if !current {
            remove_attempt(&files)?;
        }
    }
    Ok(())
}

/// The takeover's part of one conversion step. `None` leaves the step to the
/// primary's own conversion: this node is the primary and no takeover is
/// recorded at the epoch.
pub(super) fn step<E: ShardConversionEnv>(
    env: &E,
    shard_files: &ShardFiles,
    keys: &ShardKeys,
    record: Option<&DfsMetadataShardStorage>,
    local: &NodeId,
) -> Result<Option<Step>> {
    let shard = &keys.shard;
    let facilities = env.takeover();
    let lost =
        |node: &NodeId| facilities.is_some_and(|facilities| judged_lost(facilities, keys, node));
    // The round to run, the node whose loss it rests on, and whether this
    // node has yet to judge that loss.
    let (takeover, rests_on, judge) =
        match takeover_round(shard, keys.write_set.as_ref(), record, local, lost) {
            TakeoverRound::Continue(attempt) => {
                if let Some(facilities) = facilities {
                    facilities.watch_verdicts(BTreeSet::new())?;
                }
                let takeover = attempt.takeover.clone().expect("a recorded takeover");
                remove_superseded(shard_files, shard, local, &takeover)?;
                return convert(env, shard_files, keys, Some(attempt), local, takeover, None)
                    .map(Some);
            }
            TakeoverRound::Recorded(attempt) => {
                // The epoch's takeover runs: nothing this node holds of its own
                // attempts can still run, and its copy is installed or repaired
                // once the takeover activates. A loss of its converter wakes
                // the unit for the next round.
                if let Some(facilities) = facilities {
                    facilities.watch_verdicts(
                        Some(&attempt.primary_node_id)
                            .filter(|converter| *converter != local)
                            .cloned()
                            .into_iter()
                            .collect(),
                    )?;
                }
                shard_files.remove_except(None)?;
                let round = attempt
                    .takeover
                    .as_ref()
                    .map_or(1, |takeover| takeover.round);
                return parked(format!(
                    "a converting takeover by {} runs round {round} at this epoch",
                    attempt.primary_node_id
                ))
                .map(Some);
            }
            TakeoverRound::Next { takeover, replaced } => {
                (takeover, replaced.primary_node_id.clone(), false)
            }
            TakeoverRound::First(takeover) => {
                if shard.primary_node_id == *local {
                    if let Some(facilities) = facilities {
                        facilities.watch_verdicts(BTreeSet::new())?;
                    }
                    return Ok(None);
                }
                (takeover, shard.primary_node_id.clone(), true)
            }
        };
    // A serving change, or a later round, superseded this node's other
    // attempts.
    remove_superseded(shard_files, shard, local, &takeover)?;
    let Some(facilities) = facilities else {
        return parked("only the shard's primary converts it").map(Some);
    };
    let quorum =
        TakeoverQuorum::for_shard(shard, keys.write_set.as_ref(), &takeover.lost_converters)?;
    if !quorum.members().contains(local) {
        facilities.watch_verdicts(BTreeSet::new())?;
        return parked("only the shard's primary converts it").map(Some);
    }
    if judge && !judged_lost(facilities, keys, &rests_on) {
        facilities.watch_verdicts([rests_on].into())?;
        return parked("only the shard's primary converts it; this member does not judge it lost")
            .map(Some);
    }
    convert(
        env,
        shard_files,
        keys,
        None,
        local,
        takeover,
        Some(&rests_on),
    )
    .map(Some)
}

/// Convert in the place of the lost primary, or of the lost converter of
/// the previous round: fix the attempt of `takeover` from this node's own
/// copy and, until it is recorded, collect the round's promise quorum, which
/// rests on the loss of `rests_on`; then the primary's steps under the
/// takeover's install rule and activation.
fn convert<E: ShardConversionEnv>(
    env: &E,
    shard_files: &ShardFiles,
    keys: &ShardKeys,
    recorded: Option<&DfsMetadataShardConversion>,
    local: &NodeId,
    takeover: DfsMetadataConversionTakeover,
    rests_on: Option<&NodeId>,
) -> Result<Step> {
    let shard = &keys.shard;
    if keys.handoff.is_some() {
        return parked("a membership handoff is pending on the shard");
    }
    let rule = DfsMetadataShardConversionInstallRule::for_takeover(
        shard,
        keys.write_set.as_ref(),
        local,
        &takeover,
    )
    .map_err(error)?;
    let members = keys.storage_members();
    if let Err(reason) = keys.cut(&members, Vec::new()).takeover_member_compares() {
        return parked(format!("a member lacks the storage capability: {reason}"));
    }
    // A member that departed the cluster receives nothing; the capability
    // gate compares its record as it stands.
    let reachable_install_set = rule
        .install_set()
        .into_iter()
        .filter(|node| !departed(keys, node))
        .collect();
    let member_compares = keys
        .cut(&members, Vec::new())
        .storage_capable_compares_for(&reachable_install_set)
        .map_err(error)?;
    let cut = keys.cut(&members, member_compares);

    let (attempt, files, captured) =
        match fix_attempt(env, shard_files, keys, recorded, local, Some(&takeover))? {
            Fixed::Attempt {
                attempt,
                files,
                captured,
            } => (attempt, files, captured),
            Fixed::Capturing { processed_records } => {
                return Ok(Step::Done(ConversionProgress::Pending {
                    processed_records,
                }));
            }
        };
    let Some(recorded) = recorded else {
        let rests_on = rests_on.ok_or_else(|| error("an unrecorded takeover rests on no loss"))?;
        if let Some(evidence) = gather_promises(env, keys, &attempt, &takeover, rests_on, local)? {
            return parked(evidence);
        }
        let record = DfsMetadataShardStorage::converting(shard, attempt.clone());
        return submit_saved(env, &files, "converting-request.json", || {
            sinomo_dfs::build_metadata_shard_storage_converting(&cut, &record).map_err(error)
        });
    };

    let built = match files.read::<BuiltImage>("built.json")? {
        Some(built) => built,
        None => {
            let Some(captured) = captured else {
                return Err(error(
                    "conversion capture was not bound before the image build",
                ));
            };
            match build_image(env, &files, shard, &attempt, &captured)? {
                Some(built) => built,
                None => return Ok(Step::Done(round_progress(&files)?)),
            }
        }
    };
    let accepted: AcceptedContributions = files
        .read("contributions-accepted.json")?
        .ok_or_else(|| error("built attempt lost its accepted contributions"))?;
    validate_built_image(&files, &accepted, &built, &attempt, shard)?;

    match recorded.sealed() {
        Some((seal, digest)) if *seal == built.seal && digest == built.contributions_digest => {}
        Some(_) => return Err(error("the shard's attempt is sealed with another image")),
        None => {
            return submit_saved(env, &files, "seal-request.json", || {
                sinomo_dfs::build_metadata_shard_storage_seal(
                    &cut,
                    &built.seal,
                    &built.contributions_digest,
                )
                .map(|(request, _)| request)
                .map_err(error)
            });
        }
    }

    let mut sealed_attempt = attempt.clone();
    sealed_attempt.seal = Some(built.seal.clone());
    sealed_attempt.contributions_digest = Some(built.contributions_digest.clone());
    let acknowledged = match install(env, &files, shard, &rule, &sealed_attempt, &built, local)? {
        Ok(acknowledged) => acknowledged,
        Err(evidence) => return parked(evidence),
    };
    let ack_compares = match keys
        .cut(&members, Vec::new())
        .storage_capable_compares_for(&acknowledged)
    {
        Ok(compares) => compares,
        Err(reason) => {
            return parked(format!(
                "an acknowledging member lacks the storage capability: {reason}"
            ));
        }
    };
    // The activation supersedes the shard's head watermark as a takeover
    // swap does, comparing its exact bytes; a change after this read rejects
    // it and the step re-plans.
    let watermark_key =
        sinomo_dfs::metadata_shard_head_watermark_key(&shard.file_system_id, &shard.shard_id);
    let watermark = env
        .read_exact(std::slice::from_ref(&watermark_key))?
        .into_iter()
        .next()
        .flatten();
    let activation_cut = DfsMetadataShardStorageCut {
        watermark: watermark.as_ref(),
        ..keys.cut(&members, ack_compares)
    };
    submit_saved(env, &files, "activation-request.json", || {
        sinomo_dfs::build_metadata_shard_storage_takeover_activate(&activation_cut)
            .map(|(request, _)| request)
            .map_err(error)
    })
}

/// Ask every member of the round's quorum, this node first, to promise
/// this node's attempt. `None` once the quorum promises; otherwise what the
/// takeover waits for, with the unit parked on the verdicts and nodes that
/// can change an answer, `rests_on` among them. Every member is asked even
/// when this node is outranked, so a better-ranked candidate learns of it
/// and is woken.
fn gather_promises<E: ShardConversionEnv>(
    env: &E,
    keys: &ShardKeys,
    attempt: &DfsMetadataShardConversion,
    takeover: &DfsMetadataConversionTakeover,
    rests_on: &NodeId,
    local: &NodeId,
) -> Result<Option<String>> {
    let facilities = env
        .takeover()
        .ok_or_else(|| error("a converting takeover needs this node's takeover facilities"))?;
    let shard = &keys.shard;
    let quorum =
        TakeoverQuorum::for_shard(shard, keys.write_set.as_ref(), &takeover.lost_converters)?;
    let request = PromiseRequest {
        file_system_id: shard.file_system_id.clone(),
        shard_id: shard.shard_id.clone(),
        epoch: shard.epoch,
        lost_primary: shard.primary_node_id.clone(),
        round: takeover.round,
        lost_converters: takeover.lost_converters.clone(),
        candidate: TakeoverRank {
            node_id: local.clone(),
            source_journal_sequence: attempt.source.source_journal_sequence,
        },
    };
    facilities.record_rank(&request)?;
    let asked = std::iter::once(local.clone())
        .chain(
            quorum
                .members()
                .into_iter()
                .filter(|member| member != local),
        )
        .collect::<Vec<_>>();
    let mut promised = BTreeSet::new();
    let mut verdicts = BTreeSet::from([rests_on.clone()]);
    let mut unreachable = BTreeSet::new();
    let mut waiting = Vec::new();
    for member in asked {
        match facilities.promise(&member, &request)? {
            PromiseReply::Promised => {
                promised.insert(member);
            }
            PromiseReply::Refused(refusal) => {
                verdicts.extend(refusal.names());
                waiting.push(format!("{member}: {refusal}"));
            }
            PromiseReply::Unavailable { reason } => {
                waiting.push(format!("{member} is unavailable ({reason})"));
                unreachable.insert(member);
            }
        }
    }
    if quorum.satisfied(&promised) {
        return Ok(None);
    }
    facilities.watch_verdicts(verdicts)?;
    env.watch_nodes(unreachable)?;
    Ok(Some(format!(
        "a converting takeover waits for its promise quorum: {}",
        waiting.join("; ")
    )))
}

impl<C: ContributionSource> TakeoverEnv for OwnerConversionEnv<'_, C> {
    fn reach(&self, node: &NodeId) -> PeerReach {
        self.owner.authority.takeover_reach(node)
    }
    fn watch_verdicts(&self, nodes: BTreeSet<NodeId>) -> Result<()> {
        self.owner.work.watch_node_verdicts(self.unit, nodes)
    }
    fn record_rank(&self, request: &PromiseRequest) -> Result<()> {
        self.owner.authority.record_takeover_rank(request)
    }
    fn promise(&self, member: &NodeId, request: &PromiseRequest) -> Result<PromiseReply> {
        let local = self.local_node()?;
        if *member == local {
            return self
                .owner
                .authority
                .answer_takeover_promise(&local, &local, request)
                .map(Into::into);
        }
        Ok(self.owner.authority.ask_takeover_promise(member, request))
    }
}
