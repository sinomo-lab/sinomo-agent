//! The write-set keeper: the shard primary's reactive loop that keeps each
//! switched shard's write set at or above its W floor and its copies at N.
//!
//! It runs on Main only, as one task of the DFS component. It wakes on the
//! published configuration (record, membership, roles, rules), on this node's
//! per-peer network verdicts, on member views the primary accepts, on demands
//! a commit leaves, and on its own completions. Every wait is one of these
//! channels: there is no timer, scan or poll, and with nothing changing it
//! does no work. The decisions are `plan_metadata_write_set_keeping`; this
//! module gathers its inputs and executes it through the guarded write-set
//! transactions, the resident service for steps that need the shard store,
//! and the metadata journal route for member probes.

use super::*;
use crate::{
    DfsControlPlaneStore, DfsFileSystemId, DfsMetadataShardConfig, DfsMetadataShardId,
    DfsMetadataWriteSetChangeRequest,
    DfsMetadataWriteSetEvent, DfsMetadataWriteSetTxnOutcome, DfsWriteSetKeepingTxn,
    DfsWriteSetMemberFacts, DfsWriteSetMemberReach,
};
#[cfg(any(test, feature = "test-support"))]
use sinomo_core::network::PeerPathVerdict;
use std::collections::{BTreeMap, BTreeSet};

type ShardKey = (DfsFileSystemId, DfsMetadataShardId);

/// Event schema of the write-set events this component publishes.
pub(crate) const DFS_WRITE_SET_EVENT_SCHEMA_ID: &str = "sinomo.dfs.write-set";
pub(crate) const DFS_WRITE_SET_EVENT_SCHEMA_VERSION: &str = "1.0.0";

/// Every write-set event kind this component publishes.
pub(crate) const DFS_WRITE_SET_EVENT_KINDS: [&str; 7] = [
    DfsMetadataWriteSetEvent::SWITCHED,
    DfsMetadataWriteSetEvent::SHRUNK,
    DfsMetadataWriteSetEvent::ADD_SUBMITTED,
    DfsMetadataWriteSetEvent::ADDED,
    DfsMetadataWriteSetEvent::RECRUITED,
    DfsMetadataWriteSetEvent::RETIRED,
    DfsMetadataWriteSetEvent::N_RAISED,
];

pub(crate) fn dfs_write_set_event_publication(
    kind: &str,
) -> sinomo_module::ComponentEventPublication {
    sinomo_module::ComponentEventPublication::new(
        kind,
        DFS_WRITE_SET_EVENT_SCHEMA_ID,
        DFS_WRITE_SET_EVENT_SCHEMA_VERSION,
    )
}

/// Run the keeper until `shutdown`.
pub(crate) async fn run_dfs_write_set_keeping(
    data_dir: SinomoDataDir,
    authority: Arc<sinomo_dfs::DfsMetadataAuthorityRuntime>,
    events: ComponentEventClient,
    state: sinomo_core::ClusterBootstrapState,
    runtime: tokio::runtime::Handle,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut keeper = Keeper::new(data_dir, authority, events, state, runtime);
    let _hand_offs = super::write_set_takeover::TakeoverKeeperRegistration::new(&keeper.local);
    let mut published = keeper.authority.subscribe_configuration_snapshot();
    let evidence = crate::write_set_member_evidence::metadata_member_view_wake(&keeper.local);
    let demands = crate::write_set_keeping_signals::commit_demand_wake(&keeper.local);
    #[cfg(any(test, feature = "test-support"))]
    let test_control = keeping_test_control_wake();
    loop {
        if *shutdown.borrow() {
            return;
        }
        if !keeping_suspended(&keeper.data_dir) {
            keeper.evaluate();
        } else {
            super::write_set_takeover::decline_takeover_hand_offs(&keeper.local);
        }
        #[cfg(any(test, feature = "test-support"))]
        let test_wake = test_control.notified();
        #[cfg(not(any(test, feature = "test-support")))]
        let test_wake = std::future::pending::<()>();
        tokio::select! {
            changed = published.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            () = keeper.verdicts.changed() => {}
            () = evidence.notified() => {}
            () = demands.notified() => {}
            () = test_wake => {}
            Some(completion) = keeper.completions.recv() => keeper.complete(completion),
            Some(completion) = keeper.takeover_completions.recv() => {
                keeper.complete_takeover(completion);
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

/// One step the keeper started and whose outcome it waits for.
enum Completion {
    Txn {
        key: ShardKey,
        txn: DfsWriteSetKeepingTxn,
        change: Option<DfsMetadataWriteSetChangeRequest>,
        outcome: Result<DfsMetadataWriteSetTxnOutcome, String>,
        /// The event cause when the step's own evidence decides it rather
        /// than the planned step: a switch that keeps every member.
        cause: Option<&'static str>,
    },
    Probe {
        key: ShardKey,
        member: NodeId,
        generation: u64,
        answered: bool,
    },
    Join {
        key: ShardKey,
        member: NodeId,
        change: Option<DfsMetadataWriteSetChangeRequest>,
        outcome: Result<DfsMetadataWriteSetTxnOutcome, String>,
    },
    /// The re-send timer of a member's failed rejoin step.
    Retry {
        key: ShardKey,
        member: NodeId,
        attempt: Attempt,
    },
}

/// Inputs a failed step saw; it is retried once one of them changed, and on
/// the re-send timer while none did.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Attempt {
    /// The publication's generation: any configuration or record change,
    /// including a store reopen after a membership change.
    publication_generation: u64,
    reach: DfsWriteSetMemberReach,
    view: Option<crate::DfsMetadataMemberAckView>,
}

#[derive(Default)]
struct ShardKeeping {
    txn_running: bool,
    /// A submitted change whose outcome is unknown, resubmitted on the next
    /// wake until the record decides it.
    unknown: Option<(DfsWriteSetKeepingTxn, DfsMetadataWriteSetChangeRequest)>,
    /// The evidence-decided event cause of the unknown change, if any.
    unknown_cause: Option<&'static str>,
    /// Record, reachability, and the content W the rejected step tried to
    /// raise. A higher content demand is a new input at an otherwise equal cut.
    blocked: Option<(u64, u64, usize)>,
    probing: BTreeSet<NodeId>,
    joining: Option<NodeId>,
    failed: BTreeMap<NodeId, Attempt>,
    /// The next re-send interval of each member's failed rejoin step.
    retry: BTreeMap<NodeId, std::time::Duration>,
    /// The record generation a member's own view has reached, learned from a
    /// probe that waited for it.
    observed: BTreeMap<NodeId, u64>,
    demanded_w: usize,
    awaited: BTreeSet<NodeId>,
    verified: BTreeSet<NodeId>,
    /// Members a commit found fenced for a takeover candidate, with the
    /// record generation they were first planned at: a fence counts only
    /// until the record moves.
    fenced: BTreeMap<NodeId, Option<u64>>,
    reduce_after_switch: bool,
    interest: BTreeSet<NodeId>,
    last_event: Option<sinomo_core::Id>,
}

struct Keeper {
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    local: NodeId,
    runtime: tokio::runtime::Handle,
    authority: Arc<sinomo_dfs::DfsMetadataAuthorityRuntime>,
    events: ComponentEventClient,
    verdicts: super::peer_verdicts::DfsPeerVerdicts,
    shards: BTreeMap<ShardKey, ShardKeeping>,
    /// The record epoch last seen for each switched shard this node holds a
    /// copy of: a member's acceptance fence is checked only when it moves.
    member_epochs: BTreeMap<ShardKey, u64>,
    completion_tx: tokio::sync::mpsc::UnboundedSender<Completion>,
    completions: tokio::sync::mpsc::UnboundedReceiver<Completion>,
    /// The candidate role: takeover of a lost primary, and activation once
    /// this node is the new one.
    takeover: super::write_set_takeover::TakeoverRole,
    takeover_completions:
        tokio::sync::mpsc::UnboundedReceiver<super::write_set_takeover::TakeoverCompletion>,
}

impl Keeper {
    fn new(
        data_dir: SinomoDataDir,
        authority: Arc<sinomo_dfs::DfsMetadataAuthorityRuntime>,
        events: ComponentEventClient,
        state: sinomo_core::ClusterBootstrapState,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let cluster_paths = ClusterPaths::new(&data_dir);
        let (completion_tx, completions) = tokio::sync::mpsc::unbounded_channel();
        let (takeover_tx, takeover_completions) = tokio::sync::mpsc::unbounded_channel();
        Self {
            takeover: super::write_set_takeover::TakeoverRole::new(
                state.local_node_id.clone(),
                data_dir.clone(),
                runtime.clone(),
                events.clone(),
                takeover_tx,
            ),
            takeover_completions,
            verdicts: super::peer_verdicts::DfsPeerVerdicts::new(&data_dir, &cluster_paths, &state),
            local: state.local_node_id.clone(),
            data_dir,
            cluster_paths,
            runtime,
            authority,
            events,
            shards: BTreeMap::new(),
            member_epochs: BTreeMap::new(),
            completion_tx,
            completions,
        }
    }

    /// Plan every shard this node is primary of and start what the plans
    /// allow. Pure in-memory work unless a step starts.
    fn evaluate(&mut self) {
        let snapshot = self.authority.configuration_snapshot();
        let Some(entries) = snapshot
            .entries(crate::metadata_authority_runtime::DFS_METADATA_AUTHORITY_PREFIX)
            .map(<[_]>::to_vec)
        else {
            // A hand-off asker waits without a deadline: answer it now.
            super::write_set_takeover::decline_takeover_hand_offs(&self.local);
            return;
        };
        let mut unreachable_primaries = BTreeSet::new();
        let mut alignment = BTreeMap::<ShardKey, BTreeSet<NodeId>>::new();
        for (key, demands) in crate::write_set_keeping_signals::take_commit_demands(&self.local) {
            if demands.primary_unreachable {
                unreachable_primaries.insert(key.clone());
            }
            if !demands.alignment.is_empty() {
                alignment.entry(key.clone()).or_default().extend(demands.alignment);
            }
            if demands.raise_n == 0
                && demands.unreachable.is_empty()
                && demands.fenced.is_empty()
            {
                continue;
            }
            let shard = self.shards.entry(key).or_default();
            shard.demanded_w = shard.demanded_w.max(demands.raise_n);
            shard.awaited.extend(demands.unreachable);
            for member in demands.fenced {
                shard.fenced.entry(member).or_insert(None);
            }
        }
        let mut primary_shards = BTreeMap::new();
        let mut member_shards = BTreeMap::new();
        for (file_system_id, shards) in
            crate::metadata_authority_projection::metadata_shard_index_from_authority_entries(
                &entries,
            )
        {
            let Ok(shards) = shards else {
                continue;
            };
            let Ok(file_system_id) = file_system_id.parse::<DfsFileSystemId>() else {
                continue;
            };
            for shard in &shards {
                if crate::metadata_shard_configured_node_ids(shard).contains(&self.local) {
                    member_shards.insert(
                        (
                            file_system_id.clone(),
                            DfsMetadataShardId::new(shard.shard_id.clone()),
                        ),
                        shard.clone(),
                    );
                }
                if shard.primary_node_id == self.local {
                    primary_shards.insert(
                        (
                            file_system_id.clone(),
                            DfsMetadataShardId::new(shard.shard_id.clone()),
                        ),
                        (shard.clone(), shards.clone()),
                    );
                }
            }
        }
        // Deposed: the publication names another primary. Pending adds the
        // epoch advance superseded are released; nothing else is left.
        let deposed = self
            .shards
            .keys()
            .filter(|key| !primary_shards.contains_key(*key))
            .cloned()
            .collect::<Vec<_>>();
        for key in deposed {
            if let Some(shard) = self.shards.remove(&key) {
                for member in shard.interest {
                    crate::write_set_member_evidence::clear_metadata_member_view_interest(
                        &self.local,
                        &key.1,
                        &member,
                    );
                }
            }
            crate::write_set_member_evidence::forget_metadata_member_views(&self.local, &key.1);
        }
        self.retire_reached_member_fences(&entries, &member_shards);
        let takeover_shards = takeover_shards(&entries, &member_shards);
        // A hand-off needs only the record, never the member facts below, so
        // it is answered on every evaluation.
        for (hand_off, reply) in super::write_set_takeover::take_takeover_hand_offs(&self.local) {
            self.takeover.hand_off(&takeover_shards, hand_off, reply);
        }
        // Network verdicts are needed only while this node is primary of a
        // switched shard, or a write-set member that may take one over: an
        // unswitched shard is never shrunk, re-added, recruited for or taken
        // over, so it holds no reachability subscription.
        let keeps_switched_shard = self.takeover.needs_verdicts(&takeover_shards)
            || primary_shards.iter().any(|(key, (shard, _))| {
            let value = entries
                .binary_search_by(|entry| {
                    entry
                        .key
                        .as_str()
                        .cmp(crate::metadata_shard_write_set_key(&key.0, &key.1.0).as_str())
                })
                .ok()
                .map(|index| entries[index].value.as_slice());
            crate::metadata_shard_write_set_state(shard, value)
                .switched()
                .is_some()
        });
        #[cfg(any(test, feature = "test-support"))]
        record_test_reachability_interest(&self.data_dir.root(), keeps_switched_shard);
        self.verdicts.retain(keeps_switched_shard);
        let verdicts = self.verdicts.current();
        // Without the cluster scopes no member can be judged; the publication
        // that carries them wakes the keeper again.
        let Some(members) = cluster_member_facts(&snapshot) else {
            return;
        };
        for peer in self.takeover.evaluate(
            &takeover_shards,
            &members,
            &verdicts,
            &unreachable_primaries,
            snapshot.generation(),
        ) {
            self.verdicts.request_verification(&peer);
        }
        for (key, members) in alignment {
            if let Some(shard) = takeover_shards.iter().find(|shard| shard.key == key) {
                self.takeover.align_members(shard, members);
            }
        }
        for (key, (shard, shards)) in primary_shards {
            self.evaluate_shard(&key, &shard, &shards, &snapshot, &entries, &members, &verdicts);
        }
    }

    fn complete_takeover(&mut self, completion: super::write_set_takeover::TakeoverCompletion) {
        let snapshot = self.authority.configuration_snapshot();
        let entries = snapshot
            .entries(crate::metadata_authority_runtime::DFS_METADATA_AUTHORITY_PREFIX)
            .map(<[_]>::to_vec)
            .unwrap_or_default();
        let member_shards = configured_switched_shards(&entries, &self.local);
        let shards = takeover_shards(&entries, &member_shards);
        // The new primary's first keeping event names its activation.
        if let Some((key, event)) = self.takeover.complete(completion, &shards) {
            self.shards.entry(key).or_default().last_event = Some(event);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_shard(
        &mut self,
        key: &ShardKey,
        shard: &DfsMetadataShardConfig,
        shards: &[DfsMetadataShardConfig],
        snapshot: &crate::DfsConfigurationSnapshot,
        entries: &[sinomo_core::kv::KvEntry],
        members: &BTreeMap<NodeId, MemberRecord>,
        verdicts: &super::peer_verdicts::DfsPeerVerdictSnapshot,
    ) {
        let entry = |key: &str| {
            entries
                .binary_search_by(|entry| entry.key.as_str().cmp(key))
                .ok()
                .map(|index| entries[index].value.clone())
        };
        let state = crate::metadata_shard_write_set_state(
            shard,
            entry(&crate::metadata_shard_write_set_key(&key.0, &key.1.0)).as_deref(),
        );
        let handoff_pending =
            entry(&crate::metadata_shard_membership_handoff_key(&key.0, &key.1.0)).is_some();
        let operation_floor =
            entry(&crate::metadata_shard_operation_floor_key(&key.0, &key.1.0)).is_some();
        let Ok(Some(Some(file_system))) = snapshot.file_system_config(&key.0) else {
            return;
        };
        let configured = crate::metadata_shard_configured_node_ids(shard);
        let fenced = {
            let generation = state.switched().map_or(0, |write_set| write_set.generation);
            let keeping = self.shards.entry(key.clone()).or_default();
            keeping
                .fenced
                .retain(|_, seen| *seen.get_or_insert(generation) == generation);
            keeping.fenced.keys().cloned().collect::<BTreeSet<_>>()
        };
        let mut facts = BTreeMap::new();
        for node in members.keys().chain(configured.iter()) {
            let record = members.get(node);
            let view = crate::write_set_member_evidence::metadata_member_view(
                &self.local,
                &key.1,
                node,
            );
            facts.insert(
                node.clone(),
                DfsWriteSetMemberFacts {
                    eligible: record.is_some_and(|record| record.eligible),
                    departed: record.is_none_or(|record| record.departed),
                    mobile: record.is_some_and(|record| record.mobile),
                    reach: if node == &self.local {
                        DfsWriteSetMemberReach::Path
                    } else {
                        verdicts.reach(node)
                    },
                    view,
                    fenced: fenced.contains(node),
                },
            );
        }
        let eligible = facts.values().filter(|facts| facts.eligible).count();
        let Ok(bounds) =
            crate::metadata_shard_replication_bounds(&file_system, shard, shards, eligible)
        else {
            return;
        };
        let record_generation = state.switched().map_or(0, |write_set| write_set.generation);
        let switched = state.switched().cloned();
        let keeping = self.shards.entry(key.clone()).or_default();
        // Settle an unknown change from the record before anything else.
        if let Some((txn, change)) = keeping.unknown.clone()
            && !keeping.txn_running
        {
            let applied = switched.as_ref().is_some_and(|current| {
                current.last_change.change_id == change.write_set.last_change.change_id
            });
            let moved = switched
                .as_ref()
                .is_some_and(|current| current.generation >= change.write_set.generation);
            if applied {
                keeping.unknown = None;
                let event = change.write_set.clone();
                let cause = keeping.unknown_cause.take().unwrap_or(txn_cause(&txn));
                self.publish_event(key, &event, cause, None);
            } else if moved || switched.is_none() {
                keeping.unknown = None;
                keeping.unknown_cause = None;
            } else {
                keeping.txn_running = true;
                self.spawn_resubmit(key.clone(), txn, change);
                return;
            }
        }
        let keeping = self.shards.entry(key.clone()).or_default();
        // Members a commit could not reach: ask the network, and once the
        // verdict settles either the plan shrinks them or the change commits.
        let mut recover = false;
        for member in keeping.awaited.clone() {
            match verdicts.reach(&member) {
                DfsWriteSetMemberReach::NoPath => {
                    keeping.awaited.remove(&member);
                    keeping.verified.remove(&member);
                }
                DfsWriteSetMemberReach::Path if keeping.verified.contains(&member) => {
                    keeping.awaited.remove(&member);
                    keeping.verified.remove(&member);
                    recover = true;
                }
                _ => {
                    if keeping.verified.insert(member.clone()) {
                        self.verdicts.request_verification(&member);
                    }
                }
            }
        }
        if recover {
            request_prepared_change_recovery(&self.data_dir);
        }
        let keeping = self.shards.entry(key.clone()).or_default();
        let pending_adds = if switched.is_some() {
            read_pending_write_set_adds(&self.data_dir, &key.1)
        } else {
            BTreeSet::new()
        };
        let view = crate::DfsWriteSetKeepingView {
            shard,
            state: &state,
            handoff_pending,
            operation_floor,
            bounds,
            members: &facts,
            pending_adds: &pending_adds,
            demanded_w: keeping.demanded_w,
        };
        let mut plan = crate::plan_metadata_write_set_keeping(&view);
        // The reduction that follows a switch this keeper made takes the place
        // of any other removal: it names every member the switch left above N.
        if let (true, Some(write_set)) = (keeping.reduce_after_switch, switched.as_ref()) {
            match crate::metadata_write_set_reduction(shard, write_set, &facts, &bounds) {
                Some(reduction)
                    if plan.txn.is_none()
                        || matches!(plan.txn, Some(crate::DfsWriteSetKeepingTxn::Remove { .. })) =>
                {
                    plan.txn = Some(reduction)
                }
                None => keeping.reduce_after_switch = false,
                _ => {}
            }
        }
        if let Some(write_set) = switched.as_ref()
            && keeping.demanded_w <= write_set.accepted_w()
        {
            keeping.demanded_w = 0;
        }
        let verdict_generation = verdicts.generation;
        // Members being brought back: views wake the keeper for them.
        let wanted = plan
            .catch_ups
            .iter()
            .chain(&plan.joins)
            .cloned()
            .collect::<BTreeSet<_>>();
        for member in wanted.difference(&keeping.interest) {
            crate::write_set_member_evidence::register_metadata_member_view_interest(
                &self.local,
                &key.1,
                member,
            );
        }
        for member in keeping.interest.difference(&wanted) {
            crate::write_set_member_evidence::clear_metadata_member_view_interest(
                &self.local,
                &key.1,
                member,
            );
        }
        keeping.interest = wanted;
        let publication_generation = snapshot.generation();
        let attempt = |member: &NodeId| Attempt {
            publication_generation,
            reach: facts
                .get(member)
                .map_or(DfsWriteSetMemberReach::Unsettled, |facts| facts.reach),
            view: facts.get(member).and_then(|facts| facts.view.clone()),
        };
        // A probe that waited for the current generation makes the member
        // ready to join even when its answer carried no view.
        let mut joins = plan.joins.clone();
        for member in &plan.catch_ups {
            if keeping.observed.get(member) == Some(&record_generation) {
                joins.push(member.clone());
            }
        }
        if keeping.joining.is_none()
            && let Some(member) = joins
                .iter()
                .find(|member| keeping.failed.get(*member) != Some(&attempt(member)))
                .cloned()
            && let Some(write_set) = switched.as_ref()
        {
            keeping.joining = Some(member.clone());
            let cut_members = configured.iter().cloned().collect::<Vec<_>>();
            self.spawn_join(key.clone(), member, cut_members, write_set.generation);
        }
        let keeping = self.shards.entry(key.clone()).or_default();
        let mut probes = Vec::new();
        for member in plan.catch_ups.clone() {
            if keeping.observed.get(&member) == Some(&record_generation)
                || keeping.probing.contains(&member)
                || keeping.failed.get(&member) == Some(&attempt(&member))
            {
                continue;
            }
            keeping.probing.insert(member.clone());
            let prefix = facts
                .get(&member)
                .and_then(|facts| facts.view.as_ref())
                .map(|view| view.durable_prefix.clone());
            probes.push((member, prefix));
        }
        for (member, prefix) in probes {
            self.spawn_probe(key.clone(), shard.clone(), member, record_generation, prefix);
        }
        let keeping = self.shards.entry(key.clone()).or_default();
        if let Some(txn) = plan.txn
            && !keeping.txn_running
            && keeping.unknown.is_none()
            && keeping
                .blocked
                .is_none_or(|(record, verdict, attempted_w)| {
                    record != record_generation
                        || verdict != verdict_generation
                        || matches!(&txn, DfsWriteSetKeepingTxn::RaiseN { w } if *w > attempted_w)
                })
        {
            keeping.txn_running = true;
            let mut cut_members = configured.iter().cloned().collect::<Vec<_>>();
            if let DfsWriteSetKeepingTxn::Recruit { member, .. } = &txn {
                cut_members.push(member.clone());
            }
            self.spawn_txn(key.clone(), txn, cut_members, bounds);
        }
    }

    fn complete(&mut self, completion: Completion) {
        match completion {
            Completion::Txn {
                key,
                txn,
                change,
                outcome,
                cause,
            } => {
                let verdict_generation = self.verdicts.current().generation;
                let record_generation = self.record_generation(&key);
                let keeping = self.shards.entry(key.clone()).or_default();
                keeping.txn_running = false;
                match (outcome, change) {
                    (Ok(DfsMetadataWriteSetTxnOutcome::Applied), Some(change)) => {
                        keeping.blocked = None;
                        if matches!(txn, DfsWriteSetKeepingTxn::Switch) {
                            keeping.reduce_after_switch = true;
                        }
                        if matches!(
                            txn,
                            DfsWriteSetKeepingTxn::Remove {
                                cause: crate::DfsWriteSetRemoveCause::SwitchReduction,
                                ..
                            }
                        ) {
                            keeping.reduce_after_switch = false;
                        }
                        let recovers = matches!(
                            txn,
                            DfsWriteSetKeepingTxn::Shrink { .. } | DfsWriteSetKeepingTxn::RaiseN { .. }
                        );
                        let cause = cause
                            .or(keeping.unknown_cause.take())
                            .unwrap_or(txn_cause(&txn));
                        self.publish_event(&key, &change.write_set, cause, None);
                        if recovers {
                            request_prepared_change_recovery(&self.data_dir);
                        }
                    }
                    (Ok(DfsMetadataWriteSetTxnOutcome::Unknown { .. }), Some(change)) => {
                        keeping.unknown = Some((txn, change));
                        if cause.is_some() {
                            keeping.unknown_cause = cause;
                        }
                    }
                    (outcome, _) => {
                        tracing::debug!(target: "sinomo.dfs.write_set", shard = %key.1.0,
                            ?txn, ?outcome, record_generation, verdict_generation,
                            "write-set change refused at this cut");
                        // Refused before submission or rejected by the record:
                        // nothing is submitted again until an input moves.
                        let attempted_w = match txn {
                            DfsWriteSetKeepingTxn::RaiseN { w } => w,
                            _ => 0,
                        };
                        keeping.blocked =
                            Some((record_generation, verdict_generation, attempted_w));
                        keeping.unknown_cause = None;
                    }
                }
            }
            Completion::Probe {
                key,
                member,
                generation,
                answered,
            } => {
                let attempt = self.attempt(&key, &member);
                let keeping = self.shards.entry(key.clone()).or_default();
                keeping.probing.remove(&member);
                if answered {
                    keeping.observed.insert(member, generation);
                } else {
                    keeping.failed.insert(member.clone(), attempt.clone());
                    self.schedule_retry(key, member, attempt);
                }
            }
            Completion::Join {
                key,
                member,
                change,
                outcome,
            } => {
                let attempt = self.attempt(&key, &member);
                let keeping = self.shards.entry(key.clone()).or_default();
                keeping.joining = None;
                keeping.observed.remove(&member);
                match (outcome, change) {
                    (Ok(DfsMetadataWriteSetTxnOutcome::Applied), Some(change)) => {
                        keeping.failed.remove(&member);
                        keeping.retry.remove(&member);
                        let submitted = self.publish_event_named(
                            &key,
                            DfsMetadataWriteSetEvent::ADD_SUBMITTED,
                            &change.write_set,
                            "rejoin-caught-up",
                            None,
                        );
                        self.publish_event(&key, &change.write_set, "rejoin-caught-up", submitted);
                        self.spawn_release(key, vec![member]);
                        request_prepared_change_recovery(&self.data_dir);
                    }
                    (Ok(DfsMetadataWriteSetTxnOutcome::Unknown { .. }), Some(change)) => {
                        keeping.unknown = Some((
                            DfsWriteSetKeepingTxn::Recruit {
                                member: member.clone(),
                                admission: crate::DfsMetadataShardRecruit::BelowN,
                            },
                            change,
                        ));
                        let _ = attempt;
                    }
                    _ => {
                        keeping.failed.insert(member.clone(), attempt.clone());
                        self.schedule_retry(key, member, attempt);
                    }
                }
            }
            Completion::Retry {
                key,
                member,
                attempt,
            } => {
                // The inputs the failed step saw are unchanged, so nothing
                // else would retry it: let the planner run it again. A
                // changed input already did, and the planner decides whether
                // the step is still wanted at all.
                let keeping = self.shards.entry(key).or_default();
                if keeping.failed.get(&member) == Some(&attempt) {
                    keeping.failed.remove(&member);
                }
            }
        }
    }

    /// Re-send a member's failed rejoin step after its interval, which starts
    /// at the takeover re-send interval and doubles to the network timeout.
    /// The timer only wakes the keeper; the step's own outcome decides.
    fn schedule_retry(&mut self, key: ShardKey, member: NodeId, attempt: Attempt) {
        let keeping = self.shards.entry(key.clone()).or_default();
        let interval = keeping
            .retry
            .get(&member)
            .copied()
            .unwrap_or(super::write_set_takeover::TAKEOVER_RESEND_FIRST);
        keeping.retry.insert(
            member.clone(),
            interval
                .saturating_mul(2)
                .min(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
        );
        let done = self.completion_tx.clone();
        self.runtime.spawn(async move {
            tokio::time::sleep(interval).await;
            let _ = done.send(Completion::Retry {
                key,
                member,
                attempt,
            });
        });
    }

    fn record_generation(&self, key: &ShardKey) -> u64 {
        let snapshot = self.authority.configuration_snapshot();
        snapshot
            .entry(&crate::metadata_shard_write_set_key(&key.0, &key.1.0))
            .flatten()
            .and_then(|entry| entry.decode::<crate::DfsMetadataShardWriteSet>().ok())
            .map_or(0, |write_set| write_set.generation)
    }

    fn attempt(&self, key: &ShardKey, member: &NodeId) -> Attempt {
        let _ = key;
        Attempt {
            publication_generation: self.authority.configuration_snapshot().generation(),
            reach: self.verdicts.current().reach(member),
            view: crate::write_set_member_evidence::metadata_member_view(
                &self.local,
                &key.1,
                member,
            ),
        }
    }

    fn spawn_txn(
        &self,
        key: ShardKey,
        txn: DfsWriteSetKeepingTxn,
        members: Vec<NodeId>,
        bounds: crate::DfsMetadataShardBounds,
    ) {
        let cluster_paths = self.cluster_paths.clone();
        let data_dir = self.data_dir.clone();
        let runtime = self.runtime.clone();
        let done = self.completion_tx.clone();
        self.runtime.spawn_blocking(move || {
            let result = (|| -> Result<
                (
                    Option<DfsMetadataWriteSetChangeRequest>,
                    DfsMetadataWriteSetTxnOutcome,
                    Option<&'static str>,
                ),
                String,
            > {
                let mut client = daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(&runtime))
                    .ok_or_else(|| "no cluster control-plane client".to_string())?;
                let mut store = DfsControlPlaneStore::new(client.as_mut());
                let cut = store
                    .read_metadata_shard_record_cut(&key.0, &key.1.0, &members)
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "the shard has no configuration".to_string())?;
                if matches!(txn, DfsWriteSetKeepingTxn::Switch) {
                    // The evidence is read and the switch submitted under the
                    // shard's admission, so no version is committed in between
                    // whose W the initial raised N would miss.
                    let (peer, _) = daemon_dfs_peer_transports_on_runtime(&data_dir, &runtime);
                    let sender = crate::metadata_replication::dfs_metadata_journal_batch_sender_from_peer_transport(&data_dir, peer);
                    return with_daemon_dfs_exact_shard_service(&data_dir, &runtime, &cut.shard, "write-set-switch", |service| -> Result<_, String> {
                        let evidence = service
                            .write_set_switch_evidence(&cut, &sender)
                            .map_err(|error| error.to_string())?;
                        let configured = crate::metadata_shard_configured_node_ids(&cut.shard).len();
                        let change = crate::build_metadata_write_set_switch_with_raised_n(
                            &cut,
                            &evidence.members,
                            evidence.initial_raised_n(configured),
                        )
                        .map_err(|error| error.to_string())?;
                        let outcome = store.submit_metadata_write_set_change(&change);
                        let cause = evidence
                            .keeps_every_member()
                            .then_some(SWITCH_KEEPS_EVERY_MEMBER);
                        Ok((Some(change), outcome, cause))
                    })
                    .map_err(|error| error.to_string())
                    .and_then(|result| result);
                }
                let change = match &txn {
                    DfsWriteSetKeepingTxn::Switch => unreachable!("handled above"),
                    DfsWriteSetKeepingTxn::Shrink { member, .. } => {
                        crate::build_metadata_write_set_shrink(&cut, member, &bounds)
                    }
                    DfsWriteSetKeepingTxn::RaiseN { w } => {
                        crate::build_metadata_write_set_raise_n(&cut, *w)
                    }
                    DfsWriteSetKeepingTxn::Recruit { member, admission } => {
                        crate::build_metadata_write_set_recruit(&cut, member, &bounds, *admission)
                    }
                    DfsWriteSetKeepingTxn::Remove {
                        members,
                        retain_at_least,
                        retire_only,
                        ..
                    } => crate::build_metadata_write_set_remove(
                        &cut,
                        members,
                        &bounds,
                        *retain_at_least,
                        *retire_only,
                    ),
                }
                .map_err(|error| error.to_string())?;
                let outcome = store.submit_metadata_write_set_change(&change);
                Ok((Some(change), outcome, None))
            })();
            let (change, outcome, cause) = match result {
                Ok((change, outcome, cause)) => (change, Ok(outcome), cause),
                Err(error) => {
                    tracing::debug!(target: "sinomo.dfs.write_set", %error, ?txn, "write-set change not submitted");
                    #[cfg(any(test, feature = "test-support"))]
                    record_keeping_failure_for_test(&data_dir, format!("{txn:?}: {error}"));
                    (None, Err(error), None)
                }
            };
            let _ = done.send(Completion::Txn {
                key,
                txn,
                change,
                outcome,
                cause,
            });
        });
    }

    fn spawn_resubmit(
        &self,
        key: ShardKey,
        txn: DfsWriteSetKeepingTxn,
        change: DfsMetadataWriteSetChangeRequest,
    ) {
        let cluster_paths = self.cluster_paths.clone();
        let runtime = self.runtime.clone();
        let done = self.completion_tx.clone();
        self.runtime.spawn_blocking(move || {
            let outcome = daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(&runtime))
                .ok_or_else(|| "no cluster control-plane client".to_string())
                .map(|mut client| {
                    DfsControlPlaneStore::new(client.as_mut()).submit_metadata_write_set_change(&change)
                });
            let _ = done.send(Completion::Txn {
                key,
                txn,
                change: Some(change),
                outcome,
                cause: None,
            });
        });
    }

    fn spawn_probe(
        &self,
        key: ShardKey,
        shard: DfsMetadataShardConfig,
        member: NodeId,
        generation: u64,
        prefix: Option<crate::DfsMetadataPrefixProof>,
    ) {
        let (_, peer) = daemon_dfs_peer_transports_on_runtime(&self.data_dir, &self.runtime);
        let sender = crate::metadata_replication::async_dfs_metadata_journal_batch_sender_from_peer_transport(
            &self.data_dir,
            peer,
        );
        let local = self.local.clone();
        let done = self.completion_tx.clone();
        self.runtime.spawn(async move {
            // The member answers once its own view reaches `generation`. With
            // no earlier view the probe names no real prefix; its answer, a
            // refusal, still says the member's view has reached it.
            let prefix = prefix.unwrap_or_else(write_set_probe_sentinel_prefix);
            let command = crate::metadata_replication::dfs_metadata_write_set_member_probe_command(
                &local,
                &member,
                &shard.file_system_id,
                &key.1,
                shard.epoch,
                &prefix,
                generation,
            );
            let answered = match sender(&member, &command, None).await {
                Ok(response) => {
                    crate::write_set_member_evidence::record_metadata_member_view(
                        &local,
                        &key.1,
                        &member,
                        response.member_view.as_ref(),
                    );
                    // A member that refuses this epoch, for a takeover
                    // candidate's promise, cannot join it until that settles.
                    !response
                        .result
                        .rejection_code()
                        .is_some_and(crate::DfsMetadataRejectionCode::refuses_source_epoch)
                }
                Err(_) => false,
            };
            let _ = done.send(Completion::Probe {
                key,
                member,
                generation,
                answered,
            });
        });
    }

    fn spawn_join(&self, key: ShardKey, member: NodeId, members: Vec<NodeId>, _generation: u64) {
        let cluster_paths = self.cluster_paths.clone();
        let data_dir = self.data_dir.clone();
        let runtime = self.runtime.clone();
        let done = self.completion_tx.clone();
        self.runtime.spawn_blocking(move || {
            let result = (|| -> Result<
                (Option<DfsMetadataWriteSetChangeRequest>, DfsMetadataWriteSetTxnOutcome),
                String,
            > {
                let mut client = daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(&runtime))
                    .ok_or_else(|| "no cluster control-plane client".to_string())?;
                let mut store = DfsControlPlaneStore::new(client.as_mut());
                let mut cut_members = members;
                if !cut_members.contains(&member) {
                    cut_members.push(member.clone());
                }
                let cut = store
                    .read_metadata_shard_record_cut(&key.0, &key.1.0, &cut_members)
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "the shard has no configuration".to_string())?;
                let change = crate::build_metadata_write_set_add(&cut, &member)
                    .map_err(|error| error.to_string())?;
                let (peer, _) = daemon_dfs_peer_transports_on_runtime(&data_dir, &runtime);
                let sender = crate::metadata_replication::dfs_metadata_journal_batch_sender_from_peer_transport(&data_dir, peer);
                // The join holds only this shard's admission, as a commit
                // does: no prepare lands between the member's evidence and
                // its pending add, and the member's answers to other
                // primaries' probes are admitted beside it.
                let joined = with_daemon_dfs_exact_shard_service(
                    &data_dir,
                    &runtime,
                    &cut.shard,
                    &format!("write-set-join:{member}"),
                    |service| {
                        #[cfg(test)]
                        join_admitted_for_test(&data_dir);
                        service.join_write_set_member(&cut, &member, &sender)
                    },
                )
                .and_then(|joined| joined)
                .map_err(|error| error.to_string())?;
                match joined {
                    crate::DfsWriteSetJoin::Pending { .. } => {
                        let outcome = store.submit_metadata_write_set_change(&change);
                        Ok((Some(change), outcome))
                    }
                    crate::DfsWriteSetJoin::NotReady { reason } => Err(reason),
                }
            })();
            let (change, outcome) = match result {
                Ok((change, outcome)) => (change, Ok(outcome)),
                Err(error) => {
                    tracing::debug!(target: "sinomo.dfs.write_set", %error, %member, "write-set join not completed");
                    #[cfg(any(test, feature = "test-support"))]
                    record_keeping_failure_for_test(&data_dir, format!("join {member}: {error}"));
                    (None, Err(error))
                }
            };
            let _ = done.send(Completion::Join {
                key,
                member,
                change,
                outcome,
            });
        });
    }

    /// Drop pending adds the record has moved past (an add that applied, or
    /// every add once the epoch advanced), so a steady shard keeps no
    /// member-state file.
    fn spawn_release(&self, key: ShardKey, members: Vec<NodeId>) {
        let cluster_paths = self.cluster_paths.clone();
        let data_dir = self.data_dir.clone();
        let runtime = self.runtime.clone();
        self.runtime.spawn_blocking(move || {
            let Some(mut client) = daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(&runtime)) else {
                return;
            };
            let Ok(Some(cut)) = DfsControlPlaneStore::new(client.as_mut())
                .read_metadata_shard_record_cut(&key.0, &key.1.0, &members)
            else {
                return;
            };
            let _ = with_daemon_dfs_main_maintenance_service(&data_dir, &runtime, |service| {
                service
                    .metadata_backend
                    .as_deref_mut()
                    .map(|backend| backend.release_pending_write_set_adds(&key.1, &cut))
            });
        });
    }

    /// A member's acceptance fence, left by a takeover promise, is retired
    /// once this node's own view of the record reaches it: from then on the
    /// record's epoch refuses older sources. A node that is no longer primary
    /// releases the pending adds the epoch advance superseded. Both are
    /// checked only when a switched shard's epoch moves, or at the first
    /// publication after start, so the member-state file outlives neither.
    fn retire_reached_member_fences(
        &mut self,
        entries: &[sinomo_core::kv::KvEntry],
        member_shards: &BTreeMap<ShardKey, DfsMetadataShardConfig>,
    ) {
        self.member_epochs
            .retain(|key, _| member_shards.contains_key(key));
        for (key, shard) in member_shards {
            let value = entries
                .binary_search_by(|entry| {
                    entry
                        .key
                        .as_str()
                        .cmp(crate::metadata_shard_write_set_key(&key.0, &key.1.0).as_str())
                })
                .ok()
                .map(|index| entries[index].value.as_slice());
            if crate::metadata_shard_write_set_state(shard, value)
                .switched()
                .is_none()
            {
                continue;
            }
            if self.member_epochs.insert(key.clone(), shard.epoch) == Some(shard.epoch) {
                continue;
            }
            let Some(state) = read_write_set_member_state_file(&self.data_dir, &key.1) else {
                continue;
            };
            if shard.primary_node_id != self.local && !state.pending_adds.is_empty() {
                self.spawn_release(key.clone(), Vec::new());
            }
            if state.promise.is_some()
                || state.accept_from_epoch == 0
                || shard.epoch < state.accept_from_epoch
            {
                continue;
            }
            let data_dir = self.data_dir.clone();
            let runtime = self.runtime.clone();
            let shard_id = key.1.clone();
            let epoch = shard.epoch;
            self.runtime.spawn_blocking(move || {
                let result = with_daemon_dfs_main_maintenance_service(&data_dir, &runtime, |service| {
                    service
                        .metadata_backend
                        .as_deref_mut()
                        .map(|backend| backend.retire_write_set_member_fence(&shard_id, epoch))
                });
                if let Err(error) = result {
                    tracing::debug!(target: "sinomo.dfs.write_set", %error, shard = %shard_id.0, "member fence not retired");
                }
            });
        }
    }

    fn publish_event(
        &mut self,
        key: &ShardKey,
        write_set: &crate::DfsMetadataShardWriteSet,
        cause: &str,
        caused_by: Option<sinomo_core::Id>,
    ) {
        let (name, _) = DfsMetadataWriteSetEvent::applied(write_set, cause);
        self.publish_event_named(key, name, write_set, cause, caused_by);
    }

    /// Publish one typed event on the component bus: the change, its record
    /// and request identity as evidence, and the shard's previous keeping
    /// event as its cause.
    fn publish_event_named(
        &mut self,
        key: &ShardKey,
        name: &'static str,
        write_set: &crate::DfsMetadataShardWriteSet,
        cause: &str,
        caused_by: Option<sinomo_core::Id>,
    ) -> Option<sinomo_core::Id> {
        let (_, mut payload) = DfsMetadataWriteSetEvent::applied(write_set, cause);
        if name == DfsMetadataWriteSetEvent::ADD_SUBMITTED
            && let crate::DfsMetadataShardWriteSetChangeKind::Add { node_id } =
                &write_set.last_change.kind
        {
            payload.node_ids = vec![node_id.clone()];
        }
        let keeping = self.shards.entry(key.clone()).or_default();
        let previous = keeping.last_event.clone();
        let event_id = sinomo_core::Id::generate();
        keeping.last_event = Some(event_id.clone());
        #[cfg(any(test, feature = "test-support"))]
        record_keeping_event_for_test(&self.data_dir, name, &payload);
        let Ok(value) = serde_json::to_value(&payload) else {
            return Some(event_id);
        };
        let (Ok(source), Ok(kind)) = (
            sinomo_core::Source::new("component/sinomo-dfs"),
            sinomo_core::EventKind::new(name.to_string()),
        ) else {
            return Some(event_id);
        };
        let (Ok(salience), Ok(confidence)) = (
            sinomo_core::Salience::new(0.3),
            sinomo_core::Confidence::new(1.0),
        ) else {
            return Some(event_id);
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
        envelope.evidence.refs.push(sinomo_core::EvidenceRef::new(
            format!("sinomo-kv:request/{}", write_set.last_change.change_id),
            sinomo_core::EvidenceKind::Other,
        ));
        envelope.evidence.refs.push(sinomo_core::EvidenceRef::new(
            format!(
                "dfs:write-set/{}/{}/{}/{}",
                key.0, key.1.0, write_set.epoch, write_set.generation
            ),
            sinomo_core::EvidenceKind::Other,
        ));
        if let Some(caused_by) = caused_by.or(previous.clone()) {
            envelope.links.caused_by.push(caused_by);
        }
        if let Some(previous) = previous {
            envelope.links.supersedes.push(previous);
        }
        let publication = dfs_write_set_event_publication(name);
        let events = self.events.clone();
        self.runtime.spawn(async move {
            if let Err(error) = events.publish(&publication, envelope).await {
                tracing::debug!(target: "sinomo.dfs.write_set", %error, "write-set event not published");
            }
        });
        Some(event_id)
    }
}

/// The switch's cause when content of unrecorded W keeps every configured
/// member: the record's initial raised N is at least their number, so no
/// reduction follows.
const SWITCH_KEEPS_EVERY_MEMBER: &str = "capability-gate-unrecorded-content-w";

fn txn_cause(txn: &DfsWriteSetKeepingTxn) -> &'static str {
    match txn {
        DfsWriteSetKeepingTxn::Switch => "capability-gate",
        DfsWriteSetKeepingTxn::Shrink {
            cause: crate::DfsWriteSetShrinkCause::NetworkNoPath,
            ..
        } => "network-no-path",
        DfsWriteSetKeepingTxn::Shrink {
            cause: crate::DfsWriteSetShrinkCause::MemberDeparted,
            ..
        } => "member-departed",
        DfsWriteSetKeepingTxn::Shrink {
            cause: crate::DfsWriteSetShrinkCause::MemberFenced,
            ..
        } => "member-fenced",
        DfsWriteSetKeepingTxn::RaiseN { .. } => "commit-content-w",
        DfsWriteSetKeepingTxn::Recruit {
            admission: crate::DfsMetadataShardRecruit::BelowN,
            ..
        } => "write-set-below-n",
        DfsWriteSetKeepingTxn::Recruit {
            admission: crate::DfsMetadataShardRecruit::Replacement,
            ..
        } => "replacement",
        DfsWriteSetKeepingTxn::Remove { cause, .. } => match cause {
            crate::DfsWriteSetRemoveCause::SwitchReduction => "switch-reduction",
            crate::DfsWriteSetRemoveCause::SubstituteSurplus => "substitute-surplus",
            crate::DfsWriteSetRemoveCause::AboveN => "above-n",
            crate::DfsWriteSetRemoveCause::MobileSurplus => "mobile-surplus",
            crate::DfsWriteSetRemoveCause::MemberDeparted => "member-departed",
        },
    }
}

/// A prefix no member holds: a probe at it only learns that the member's view
/// reached the named generation.
fn write_set_probe_sentinel_prefix() -> crate::DfsMetadataPrefixProof {
    crate::DfsMetadataPrefixProof::from_digest(0, format!("dfs-metadata-prefix-v2:blake3:{:064x}", 0))
        .expect("well-formed sentinel prefix")
}

fn request_prepared_change_recovery(data_dir: &SinomoDataDir) {
    if let Err(error) =
        request_daemon_dfs_maintenance_reconciliation(&DfsRuntimePaths::new(data_dir))
    {
        tracing::debug!(target: "sinomo.dfs.write_set", %error, "prepared-change recovery not requested");
    }
}

/// Every shard the local node is configured for, by key.
fn configured_switched_shards(
    entries: &[sinomo_core::kv::KvEntry],
    local: &NodeId,
) -> BTreeMap<ShardKey, DfsMetadataShardConfig> {
    let mut shards = BTreeMap::new();
    for (file_system_id, index) in
        crate::metadata_authority_projection::metadata_shard_index_from_authority_entries(entries)
    {
        let (Ok(index), Ok(file_system_id)) = (index, file_system_id.parse::<DfsFileSystemId>())
        else {
            continue;
        };
        for shard in index {
            if crate::metadata_shard_configured_node_ids(&shard).contains(local) {
                shards.insert(
                    (
                        file_system_id.clone(),
                        DfsMetadataShardId::new(shard.shard_id.clone()),
                    ),
                    shard,
                );
            }
        }
    }
    shards
}

/// The shards the candidate role reads, with their write-set state.
fn takeover_shards(
    entries: &[sinomo_core::kv::KvEntry],
    member_shards: &BTreeMap<ShardKey, DfsMetadataShardConfig>,
) -> Vec<super::write_set_takeover::TakeoverShard> {
    member_shards
        .iter()
        .map(|(key, shard)| {
            let value = entries
                .binary_search_by(|entry| {
                    entry
                        .key
                        .as_str()
                        .cmp(crate::metadata_shard_write_set_key(&key.0, &key.1.0).as_str())
                })
                .ok()
                .map(|index| entries[index].value.as_slice());
            super::write_set_takeover::TakeoverShard {
                key: key.clone(),
                shard: shard.clone(),
                state: crate::metadata_shard_write_set_state(shard, value),
            }
        })
        .collect()
}

/// The shard's durable member state, read without the writer lock: callers
/// only decide whether to take that lock.
fn read_write_set_member_state_file(
    data_dir: &SinomoDataDir,
    shard_id: &DfsMetadataShardId,
) -> Option<crate::DfsMetadataWriteSetMemberState> {
    let path = crate::default_dfs_metadata_shards_root(data_dir)
        .join(crate::metadata_keys::key_segment(&shard_id.0))
        .join("coordination")
        .join("write-set-member-state.sobj");
    crate::metadata_io::read_sobj_optional::<crate::DfsMetadataWriteSetMemberState>(&path)
        .ok()
        .flatten()
}

fn read_pending_write_set_adds(
    data_dir: &SinomoDataDir,
    shard_id: &DfsMetadataShardId,
) -> BTreeSet<NodeId> {
    read_write_set_member_state_file(data_dir, shard_id)
        .map(|state| {
            state
                .pending_adds
                .into_iter()
                .map(|pending| pending.node_id)
                .collect()
        })
        .unwrap_or_default()
}

/// One cluster member as the keeper reads it from the publication.
#[derive(Clone, Debug)]
pub(super) struct MemberRecord {
    pub(super) eligible: bool,
    pub(super) departed: bool,
    pub(super) mobile: bool,
}

/// The cluster's member facts, or `None` while this publication does not
/// carry the cluster scopes (only the metadata publication has arrived): no
/// member may then be judged departed or ineligible. A record that does not
/// decode is not eligible and not departed.
pub(super) fn cluster_member_facts(
    snapshot: &crate::DfsConfigurationSnapshot,
) -> Option<BTreeMap<NodeId, MemberRecord>> {
    const MEMBERS: &str = "/cluster/members/";
    let providers = sinomo_core::service::service_instances_from_entries(
        snapshot.entries("/cluster/services/")?,
    )
    .map(|instances| {
        sinomo_core::service::service_instances_providing_capability(
            instances,
            "dfs.metadata.replicate",
        )
        .into_iter()
        .map(|instance| instance.node_id)
        .collect::<BTreeSet<_>>()
    })
    .unwrap_or_default();
    let has = |record: &sinomo_core::ClusterMemberRecord, capability: &str| {
        record
            .capabilities
            .iter()
            .any(|advertised| advertised.as_str() == capability)
    };
    let mut facts = BTreeMap::new();
    for entry in snapshot.entries(MEMBERS)? {
        match entry.decode::<sinomo_core::ClusterMemberRecord>() {
            Ok(record) => {
                // A member is departed only when the cluster no longer
                // authorizes it. A non-voting member (seeded before learner
                // admission, or a demoted voter) still holds copies: metadata
                // members need not be KV voters.
                let departed = !record.state.is_authorized();
                let eligible = !departed
                    && providers.contains(&record.node_id)
                    && has(&record, sinomo_core::node::DFS_METADATA_PREPARED_OPERATION_V7_CAPABILITY)
                    && has(&record, sinomo_core::node::DFS_METADATA_WRITE_SET_V1_CAPABILITY)
                    && has(&record, sinomo_core::node::DFS_METADATA_STORAGE_GENERATION_V1_CAPABILITY);
                facts.insert(
                    record.node_id.clone(),
                    MemberRecord {
                        eligible,
                        departed,
                        mobile: record.node_role == sinomo_core::ClusterNodeRole::Mobile,
                    },
                );
            }
            Err(_) => {
                if let Some(Ok(node_id)) = entry
                    .key
                    .strip_prefix(MEMBERS)
                    .filter(|rest| !rest.contains('/'))
                    .map(str::parse::<NodeId>)
                {
                    facts.entry(node_id).or_insert(MemberRecord {
                        eligible: false,
                        departed: false,
                        mobile: false,
                    });
                }
            }
        }
    }
    Some(facts)
}


#[cfg(not(any(test, feature = "test-support")))]
fn keeping_suspended(_data_dir: &SinomoDataDir) -> bool {
    false
}

#[cfg(any(test, feature = "test-support"))]
mod test_control {
    //! Test support: suspend the keeper, supply verdicts and observe what it
    //! did. Nothing here is consulted unless a test installs it.

    use super::*;
    use std::path::PathBuf;
    use std::sync::{LazyLock, Mutex};

    #[derive(Default)]
    struct Control {
        suspended: BTreeSet<PathBuf>,
        enabled: BTreeSet<PathBuf>,
        events: BTreeMap<PathBuf, Vec<(String, DfsMetadataWriteSetEvent)>>,
        failures: BTreeMap<PathBuf, Vec<String>>,
        /// Whether each keeper's last evaluation wanted a reachability
        /// subscription.
        reachability: BTreeMap<PathBuf, bool>,
    }

    static CONTROL: LazyLock<Mutex<Control>> = LazyLock::new(Default::default);
    static WAKE: LazyLock<Arc<tokio::sync::Notify>> = LazyLock::new(Default::default);

    fn control() -> std::sync::MutexGuard<'static, Control> {
        CONTROL.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(super) fn keeping_test_control_wake() -> Arc<tokio::sync::Notify> {
        Arc::clone(&WAKE)
    }

    pub(super) fn keeping_suspended(data_dir: &SinomoDataDir) -> bool {
        control().suspended.contains(&data_dir.root())
    }

    /// Suspend or resume the keeper of the node owning `root`. A resumed
    /// keeper stays resumed across the node's restarts.
    pub(crate) fn set_write_set_keeping_suspended(root: PathBuf, suspended: bool) {
        {
            let mut control = control();
            if suspended {
                control.enabled.remove(&root);
                control.suspended.insert(root);
            } else {
                control.suspended.remove(&root);
                control.enabled.insert(root);
            }
        }
        WAKE.notify_waiters();
    }

    /// The test runtime mounts every node with its keeper suspended unless a
    /// test resumed it: tests that drive write-set transactions by hand keep
    /// that control.
    pub(crate) fn suspend_write_set_keeping_unless_enabled(root: PathBuf) {
        let mut control = control();
        if !control.enabled.contains(&root) {
            control.suspended.insert(root);
        }
    }

    pub(super) fn record_test_reachability_interest(root: &std::path::Path, needed: bool) {
        control().reachability.insert(root.to_path_buf(), needed);
    }

    /// Whether the keeper of `root` held, at its last evaluation, a network
    /// reachability subscription; `None` before its first evaluation.
    pub(crate) fn keeping_reachability_interest_for_test(root: &PathBuf) -> Option<bool> {
        control().reachability.get(root).copied()
    }

    /// Set the verdict every write-set role of `root` sees for `peer`, and
    /// wake its keeper, which may not have been waiting on test verdicts yet.
    pub(crate) fn set_test_peer_verdict(
        root: PathBuf,
        peer: NodeId,
        verdict: PeerPathVerdict,
        verifying: bool,
    ) {
        super::super::peer_verdicts::test_seam::set_peer_verdict(root, peer, verdict, verifying);
        WAKE.notify_waiters();
    }

    pub(crate) fn test_verification_requests(root: &PathBuf) -> Vec<NodeId> {
        super::super::peer_verdicts::test_seam::verification_requests(root)
    }

    pub(super) fn record_keeping_event_for_test(
        data_dir: &SinomoDataDir,
        name: &str,
        event: &DfsMetadataWriteSetEvent,
    ) {
        control()
            .events
            .entry(data_dir.root())
            .or_default()
            .push((name.to_string(), event.clone()));
    }

    pub(crate) fn keeping_events_for_test(root: &PathBuf) -> Vec<(String, DfsMetadataWriteSetEvent)> {
        control().events.get(root).cloned().unwrap_or_default()
    }

    pub(super) fn record_keeping_failure_for_test(data_dir: &SinomoDataDir, failure: String) {
        control()
            .failures
            .entry(data_dir.root())
            .or_default()
            .push(failure);
    }

    pub(crate) fn keeping_failures_for_test(root: &PathBuf) -> Vec<String> {
        control().failures.get(root).cloned().unwrap_or_default()
    }

    /// Joins held at their admission until every node of a rendezvous has
    /// admitted one, so the joins overlap.
    #[cfg(test)]
    struct JoinRendezvous {
        roots: BTreeSet<PathBuf>,
        admitted: Mutex<BTreeSet<PathBuf>>,
        all_admitted: std::sync::Condvar,
    }

    #[cfg(test)]
    static JOIN_RENDEZVOUS: LazyLock<Mutex<BTreeMap<PathBuf, Arc<JoinRendezvous>>>> =
        LazyLock::new(Default::default);

    /// Ends the rendezvous when dropped; a join still held is let go.
    #[cfg(test)]
    pub(crate) struct JoinRendezvousGuard(Arc<JoinRendezvous>);

    #[cfg(test)]
    impl JoinRendezvousGuard {
        /// Whether every node of the rendezvous admitted a join while it
        /// was held.
        pub(crate) fn all_admitted(&self) -> bool {
            self.0.roots.is_subset(
                &self
                    .0
                    .admitted
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()),
            )
        }
    }

    #[cfg(test)]
    impl Drop for JoinRendezvousGuard {
        fn drop(&mut self) {
            let mut registered = JOIN_RENDEZVOUS
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            for root in &self.0.roots {
                registered.remove(root);
            }
            drop(registered);
            self.0
                .admitted
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .extend(self.0.roots.iter().cloned());
            self.0.all_admitted.notify_all();
        }
    }

    /// Hold the first join each node of `roots` admits until all of them
    /// have admitted one.
    #[cfg(test)]
    pub(crate) fn hold_joins_until_all_admitted_for_test(roots: Vec<PathBuf>) -> JoinRendezvousGuard {
        let rendezvous = Arc::new(JoinRendezvous {
            roots: roots.iter().cloned().collect(),
            admitted: Mutex::default(),
            all_admitted: std::sync::Condvar::new(),
        });
        let mut registered = JOIN_RENDEZVOUS
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for root in roots {
            registered.insert(root, Arc::clone(&rendezvous));
        }
        JoinRendezvousGuard(rendezvous)
    }

    /// A join on `data_dir` holds its admission: wait for the rest of its
    /// rendezvous, if a test set one.
    #[cfg(test)]
    pub(super) fn join_admitted_for_test(data_dir: &SinomoDataDir) {
        let root = data_dir.root();
        let Some(rendezvous) = JOIN_RENDEZVOUS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&root)
            .cloned()
        else {
            return;
        };
        let mut admitted = rendezvous
            .admitted
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        admitted.insert(root);
        rendezvous.all_admitted.notify_all();
        while !rendezvous.roots.is_subset(&admitted) {
            admitted = rendezvous
                .all_admitted
                .wait(admitted)
                .unwrap_or_else(|error| error.into_inner());
        }
        drop(admitted);
        // Later joins on these nodes are not held.
        let mut registered = JOIN_RENDEZVOUS
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for root in &rendezvous.roots {
            if registered
                .get(root)
                .is_some_and(|current| Arc::ptr_eq(current, &rendezvous))
            {
                registered.remove(root);
            }
        }
    }
}
#[cfg(any(test, feature = "test-support"))]
pub(crate) use test_control::*;
