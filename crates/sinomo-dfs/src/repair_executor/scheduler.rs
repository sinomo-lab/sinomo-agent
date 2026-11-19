//! Event-proportional scheduling state of one node's repair executor.
//!
//! A pass touches only what its events name: the needs of objects whose index
//! entries changed, the attempts whose outcome arrived, the needs whose time
//! came due, the parked needs of targets that became reachable and the needs
//! waiting for a newer authority view. The whole index of a shard is read only
//! when this node gains that shard's primary assignment, including the first
//! observation after process start; that traversal is bounded per pass.

use std::collections::{BTreeMap, BTreeSet};

use sinomo_core::node::NodeId;

use crate::maintenance::DfsMaintenanceExecutionExclusions;
use crate::metadata_tables::{DfsRepairNeed, DfsRepairNeedKey};
use crate::{DfsError, DfsObjectId};

/// Index entries a gained assignment's traversal reads per pass.
pub(crate) const DFS_REPAIR_TRAVERSAL_PER_PASS: usize = 256;
const DFS_REPAIR_TRAVERSAL_PAGE: usize = 64;
/// Attempts awaiting their outcome at once; an outcome admits the next need.
pub(crate) const DFS_REPAIR_EXECUTOR_MAX_IN_FLIGHT: usize = 32;

/// Read access to the derived repair index of the node's primary shards.
pub(crate) trait DfsRepairNeedSource {
    fn needs_for_objects(
        &self,
        shard: &str,
        objects: &BTreeSet<DfsObjectId>,
    ) -> Result<Vec<(DfsRepairNeedKey, DfsRepairNeed)>, DfsError>;
    fn needs_for_keys(
        &self,
        shard: &str,
        keys: &[DfsRepairNeedKey],
    ) -> Result<Vec<(DfsRepairNeedKey, DfsRepairNeed)>, DfsError>;
    fn page(
        &self,
        shard: &str,
        after: Option<&DfsRepairNeedKey>,
        limit: usize,
    ) -> Result<Vec<(DfsRepairNeedKey, DfsRepairNeed)>, DfsError>;
    fn need_count(&self, shard: &str) -> Result<usize, DfsError>;
}

/// One ended attempt delivered to a pass.
pub(crate) type DfsRepairOutcomeEvent = (
    DfsRepairNeedKey,
    Option<String>,
    DfsRepairAttemptOutcome,
    Option<String>,
);

/// How an attempt ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DfsRepairAttemptOutcome {
    /// The reply published the receipt; the index change settles the need.
    Settled,
    /// The slot moved past the attempt's plan: re-plan at once, no backoff,
    /// once per slot generation.
    Superseded,
    /// A rejected reply or a failed send: back off.
    Failed,
    /// Dispatch refused because the local authority view has not caught up
    /// with the published assignment: wait for the authority edge.
    AwaitingAuthority,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Phase {
    Ready,
    /// Selected and handed to dispatch; the send's outcome decides next.
    Dispatching,
    /// Left without a reply yet; `recheck_at` is a re-send time, never a
    /// network deadline.
    InFlight {
        request_id: String,
        recheck_at: u64,
    },
    Backoff {
        due: u64,
    },
    /// The target is unreachable; only a reachability change releases it.
    Parked,
    AwaitingAuthority,
    /// Live demand without an owner intent: the pass asked for an owner, and
    /// the object's next index change, which brings one, releases it.
    AwaitingOwner,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NeedState {
    shard: String,
    /// The owner intent and content the need was last read with, for status.
    intent_id: Option<String>,
    content_hash: String,
    /// When the last attempt was made, for status.
    last_attempt_unix_nanos: u64,
    generation: u64,
    failures: u32,
    /// The slot generation at which an attempt last ended superseded. A
    /// second supersession at the same generation means the re-plan changed
    /// nothing, so it backs off like a failure instead of re-sending at once.
    superseded_at: Option<u64>,
    /// The error the need's last failed attempt reported, for status. It
    /// lives with the need: settling or a primary change drops both.
    last_error: Option<String>,
    phase: Phase,
}

/// One need selected for dispatch.
#[derive(Clone, Debug)]
pub(crate) struct DfsRepairExecutorCandidate {
    pub(crate) key: DfsRepairNeedKey,
    pub(crate) need: DfsRepairNeed,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DfsRepairExecutorSelection {
    pub(crate) candidates: Vec<DfsRepairExecutorCandidate>,
    /// Live demand without an owner intent, once per object version: the pass
    /// enrolls an owner through the existing retained-owner path.
    pub(crate) ownerless: BTreeSet<(DfsObjectId, crate::DfsVersion)>,
    /// Index entries this pass read.
    pub(crate) examined: usize,
}

/// What a pass consumes besides the scheduler's own state.
pub(crate) struct DfsRepairPassInput<'a> {
    pub(crate) now: u64,
    pub(crate) primaries: BTreeMap<String, u64>,
    pub(crate) exclusions: &'a DfsMaintenanceExecutionExclusions,
    pub(crate) changed: BTreeMap<String, BTreeSet<DfsObjectId>>,
    /// Shards whose index an installed snapshot replaced: read again.
    pub(crate) replaced: BTreeSet<String>,
    /// Ended attempts: the need, the attempt's request id, its outcome and
    /// the error a failed attempt reported.
    pub(crate) outcomes: Vec<DfsRepairOutcomeEvent>,
    pub(crate) authority_changed: bool,
    /// Objects a foreground durability wait is blocked on: their backed-off
    /// or parked needs are attempted again at once.
    pub(crate) demanded: BTreeSet<DfsObjectId>,
    pub(crate) limit: usize,
}

#[derive(Debug, Default)]
pub(crate) struct DfsRepairScheduler {
    primaries: BTreeMap<String, u64>,
    /// Gained assignments whose index is still being read, with the cursor.
    traversals: BTreeMap<String, Option<DfsRepairNeedKey>>,
    needs: BTreeMap<DfsRepairNeedKey, NeedState>,
    /// The tracked needs of each object, so an object's index change touches
    /// only that object's entries.
    by_object: BTreeMap<DfsObjectId, BTreeSet<DfsRepairNeedKey>>,
    /// Ready needs in the order they became ready, so every need is reached
    /// under sustained arrivals; a demanded need goes ahead of them.
    ready: BTreeSet<(u64, DfsRepairNeedKey)>,
    ready_order: BTreeMap<DfsRepairNeedKey, u64>,
    ready_arrivals: u64,
    ready_demands: u64,
    due: BTreeSet<(u64, DfsRepairNeedKey)>,
    parked: BTreeMap<NodeId, BTreeSet<DfsRepairNeedKey>>,
    /// When the evidence that excludes each target can expire by time alone,
    /// with no reachability event, as of the last pass. Only a parked
    /// target's expiry is a timer: the pass re-reads reachability then.
    exclusion_expiries: BTreeMap<NodeId, u64>,
    awaiting_authority: BTreeSet<DfsRepairNeedKey>,
    awaiting_owner: BTreeSet<DfsRepairNeedKey>,
    in_flight: usize,
    backoff: usize,
    need_counts: BTreeMap<String, usize>,
    /// Index entries read and tracked needs touched over the scheduler's
    /// life; tests bound per-event work with it.
    pub(crate) examined_total: u64,
}

fn backoff_delay_nanos(failures: u32) -> u64 {
    crate::transfer::repair_retry_delay_nanos_for_ticks(crate::transfer::repair_retry_delay_ticks(
        failures,
    ))
}

pub(super) fn in_flight_resend_nanos() -> u64 {
    u64::try_from(crate::DFS_PENDING_TRANSFER_STALE_AFTER.as_nanos()).unwrap_or(u64::MAX)
}

impl DfsRepairScheduler {
    /// Dispatchable needs in this node's primary shards, from the index's
    /// maintained per-shard counts as of the last pass.
    pub(crate) fn need_count(&self) -> usize {
        self.need_counts.values().sum()
    }

    pub(crate) fn tracked_count(&self) -> usize {
        self.needs.len()
    }

    /// Tracked needs other than demand waiting for an owner.
    pub(crate) fn attempted_count(&self) -> usize {
        self.needs.len().saturating_sub(self.awaiting_owner.len())
    }

    /// Attempts awaiting their outcome.
    pub(crate) fn in_flight_count(&self) -> usize {
        self.in_flight
    }

    /// Needs backing off after a failed attempt.
    pub(crate) fn backoff_count(&self) -> usize {
        self.backoff
    }

    pub(crate) fn has_parked_needs(&self) -> bool {
        !self.parked.is_empty()
    }

    pub(crate) fn has_awaiting_authority(&self) -> bool {
        !self.awaiting_authority.is_empty()
    }

    pub(crate) fn has_ready_work(&self) -> bool {
        !self.traversals.is_empty() || (!self.ready.is_empty() && self.capacity() > 0)
    }

    /// When the evidence that excludes a parked need's target can expire.
    fn parked_reevaluate_at(&self) -> Option<u64> {
        self.parked
            .keys()
            .filter_map(|target| self.exclusion_expiries.get(target).copied())
            .min()
    }

    /// The single timer: the earliest backoff end, in-flight re-check or
    /// expiry of the evidence that parks a need.
    pub(crate) fn next_due(&self) -> Option<u64> {
        let reevaluate = self.parked_reevaluate_at();
        self.due
            .first()
            .map(|(due, _)| *due)
            .into_iter()
            .chain(reevaluate)
            .min()
    }

    /// Queue `key` as ready: after every need already ready, or ahead of
    /// them for a demanded need.
    fn file_ready(&mut self, key: &DfsRepairNeedKey, demanded: bool) {
        const ARRIVALS: u64 = 1 << 62;
        let order = if demanded {
            self.ready_demands = self.ready_demands.saturating_add(1);
            ARRIVALS.saturating_sub(self.ready_demands)
        } else {
            self.ready_arrivals = self.ready_arrivals.saturating_add(1);
            ARRIVALS.saturating_add(self.ready_arrivals)
        };
        if let Some(previous) = self.ready_order.insert(key.clone(), order) {
            self.ready.remove(&(previous, key.clone()));
        }
        self.ready.insert((order, key.clone()));
    }

    pub(crate) fn primaries(&self) -> &BTreeMap<String, u64> {
        &self.primaries
    }

    /// Indexes this pass may consult. A demand for a previously untracked
    /// object has no shard hint and needs the current primary set.
    pub(super) fn work_shards(
        &self,
        primaries: &BTreeMap<String, u64>,
        events: &super::DfsRepairExecutorEvents,
        now: u64,
    ) -> BTreeSet<String> {
        if events
            .demanded
            .iter()
            .any(|object| !self.by_object.contains_key(object))
        {
            return primaries.keys().cloned().collect();
        }
        let mut shards: BTreeSet<_> = primaries
            .iter()
            .filter(|(id, epoch)| {
                self.primaries.get(*id) != Some(*epoch) || !self.need_counts.contains_key(*id)
            })
            .map(|(id, _)| id.clone())
            .chain(self.traversals.keys().cloned())
            .chain(events.changed.keys().cloned())
            .chain(events.replaced.iter().cloned())
            .collect();
        let mut add = |key: &DfsRepairNeedKey| {
            if let Some(need) = self.needs.get(key) {
                shards.insert(need.shard.clone());
            }
        };
        for (_, key) in &self.ready {
            add(key);
        }
        for (_, key) in self.due.iter().take_while(|(due, _)| *due <= now) {
            add(key);
        }
        for keys in self.parked.values() {
            for key in keys {
                add(key);
            }
        }
        if events.authority_changed {
            for key in &self.awaiting_authority {
                add(key);
            }
        }
        for (key, ..) in &events.outcomes {
            add(key);
        }
        shards.retain(|shard| primaries.contains_key(shard));
        shards
    }

    fn capacity(&self) -> usize {
        DFS_REPAIR_EXECUTOR_MAX_IN_FLIGHT.saturating_sub(self.in_flight)
    }

    /// Remove a need's scheduling state wherever it is filed.
    fn forget(&mut self, key: &DfsRepairNeedKey) {
        if let Some(state) = self.needs.remove(key) {
            self.unfile(key, &state);
            if let Some(keys) = self.by_object.get_mut(&key.0) {
                keys.remove(key);
                if keys.is_empty() {
                    self.by_object.remove(&key.0);
                }
            }
        }
    }

    fn unfile(&mut self, key: &DfsRepairNeedKey, state: &NeedState) {
        match &state.phase {
            Phase::Ready => {
                if let Some(order) = self.ready_order.remove(key) {
                    self.ready.remove(&(order, key.clone()));
                }
            }
            Phase::Dispatching => {
                self.in_flight = self.in_flight.saturating_sub(1);
            }
            Phase::InFlight { recheck_at, .. } => {
                self.in_flight = self.in_flight.saturating_sub(1);
                self.due.remove(&(*recheck_at, key.clone()));
            }
            Phase::Backoff { due } => {
                self.backoff = self.backoff.saturating_sub(1);
                self.due.remove(&(*due, key.clone()));
            }
            Phase::Parked => {
                let target = &key.2;
                if let Some(keys) = self.parked.get_mut(target) {
                    keys.remove(key);
                    if keys.is_empty() {
                        self.parked.remove(target);
                    }
                }
            }
            Phase::AwaitingAuthority => {
                self.awaiting_authority.remove(key);
            }
            Phase::AwaitingOwner => {
                self.awaiting_owner.remove(key);
            }
        }
    }

    /// Move a need to `phase`, filing it in the matching structure.
    fn set_phase(&mut self, key: &DfsRepairNeedKey, phase: Phase) {
        let Some(mut state) = self.needs.remove(key) else {
            return;
        };
        self.unfile(key, &state);
        match &phase {
            Phase::Ready => {
                self.file_ready(key, false);
            }
            Phase::Dispatching => {
                self.in_flight += 1;
            }
            Phase::InFlight { recheck_at, .. } => {
                self.in_flight += 1;
                self.due.insert((*recheck_at, key.clone()));
            }
            Phase::Backoff { due } => {
                self.backoff += 1;
                self.due.insert((*due, key.clone()));
            }
            Phase::Parked => {
                self.parked
                    .entry(key.2.clone())
                    .or_default()
                    .insert(key.clone());
            }
            Phase::AwaitingAuthority => {
                self.awaiting_authority.insert(key.clone());
            }
            Phase::AwaitingOwner => {
                self.awaiting_owner.insert(key.clone());
            }
        }
        state.phase = phase;
        self.needs.insert(key.clone(), state);
    }

    fn track_ready(&mut self, shard: &str, key: DfsRepairNeedKey, need: &DfsRepairNeed) {
        self.file_ready(&key, false);
        self.by_object
            .entry(key.0.clone())
            .or_default()
            .insert(key.clone());
        self.needs.insert(
            key,
            NeedState {
                shard: shard.to_owned(),
                intent_id: need.owner_intent_id.clone(),
                content_hash: need.content_hash.clone(),
                last_attempt_unix_nanos: 0,
                generation: need.receipt_generation,
                failures: 0,
                superseded_at: None,
                last_error: None,
                phase: Phase::Ready,
            },
        );
    }

    /// A successor restarts timing: drop every need of a shard whose primary
    /// assignment changed or was lost, and read the whole index of a gained one.
    fn observe_primaries(&mut self, primaries: BTreeMap<String, u64>) {
        if primaries == self.primaries {
            return;
        }
        let changed = self
            .primaries
            .iter()
            .filter(|(shard, epoch)| primaries.get(*shard) != Some(*epoch))
            .map(|(shard, _)| shard.clone())
            .chain(
                primaries
                    .iter()
                    .filter(|(shard, epoch)| self.primaries.get(*shard) != Some(*epoch))
                    .map(|(shard, _)| shard.clone()),
            )
            .collect::<BTreeSet<_>>();
        let dropped = self
            .needs
            .iter()
            .filter(|(_, state)| changed.contains(&state.shard))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in dropped {
            self.forget(&key);
        }
        self.traversals.retain(|shard, _| !changed.contains(shard));
        self.need_counts
            .retain(|shard, _| primaries.contains_key(shard) && !changed.contains(shard));
        for shard in primaries.keys() {
            if changed.contains(shard) {
                self.traversals.insert(shard.clone(), None);
            }
        }
        self.primaries = primaries;
    }

    fn apply_outcome(
        &mut self,
        key: &DfsRepairNeedKey,
        request_id: Option<&str>,
        outcome: DfsRepairAttemptOutcome,
        error: Option<String>,
        now: u64,
    ) {
        let Some(state) = self.needs.get(key) else {
            return;
        };
        let current = match &state.phase {
            Phase::Dispatching => true,
            Phase::InFlight {
                request_id: current,
                ..
            } => request_id.is_none_or(|request_id| request_id == current),
            _ => false,
        };
        if !current {
            // A reply to an attempt this executor already replaced.
            return;
        }
        if let Some(error) = error {
            self.needs.get_mut(key).expect("present above").last_error = Some(error);
        }
        let outcome = match outcome {
            DfsRepairAttemptOutcome::Superseded => {
                let state = self.needs.get_mut(key).expect("present above");
                if state.superseded_at == Some(state.generation) {
                    DfsRepairAttemptOutcome::Failed
                } else {
                    state.superseded_at = Some(state.generation);
                    DfsRepairAttemptOutcome::Superseded
                }
            }
            outcome => outcome,
        };
        match outcome {
            DfsRepairAttemptOutcome::Settled => self.forget(key),
            DfsRepairAttemptOutcome::Superseded => self.set_phase(key, Phase::Ready),
            DfsRepairAttemptOutcome::AwaitingAuthority => {
                self.set_phase(key, Phase::AwaitingAuthority)
            }
            DfsRepairAttemptOutcome::Failed => {
                let failures = {
                    let state = self.needs.get_mut(key).expect("present above");
                    state.failures = state.failures.saturating_add(1);
                    state.failures
                };
                self.set_phase(
                    key,
                    Phase::Backoff {
                        due: now.saturating_add(backoff_delay_nanos(failures)),
                    },
                );
            }
        }
    }

    /// The current needs of changed objects, compared with what is tracked.
    /// Returns the tracked entries it touched.
    fn apply_object_needs(
        &mut self,
        shard: &str,
        objects: &BTreeSet<DfsObjectId>,
        current: Vec<(DfsRepairNeedKey, DfsRepairNeed)>,
    ) -> usize {
        let current = current.into_iter().collect::<BTreeMap<_, _>>();
        let tracked = objects
            .iter()
            .filter_map(|object| self.by_object.get(object))
            .flatten()
            .filter(|key| {
                self.needs
                    .get(*key)
                    .is_some_and(|state| state.shard == shard)
            })
            .cloned()
            .collect::<Vec<_>>();
        let touched = tracked.len();
        for key in tracked {
            match current.get(&key) {
                None => self.forget(&key),
                Some(need) => {
                    let state = self.needs.get_mut(&key).expect("tracked");
                    state.intent_id.clone_from(&need.owner_intent_id);
                    if matches!(state.phase, Phase::AwaitingOwner) {
                        // Only a net change of the object's needs reaches
                        // here, a change of this need or of another need of
                        // the object: an enrolled owner releases it, and any
                        // other change, such as the end of the write that
                        // blocked enrollment, has the next selection ask
                        // again.
                        self.set_phase(&key, Phase::Ready);
                        continue;
                    }
                    if state.generation != need.receipt_generation {
                        state.generation = need.receipt_generation;
                        // An outstanding attempt planned at the older
                        // generation is superseded; a failed one keeps its
                        // backoff, so an attempt whose own failure moved the
                        // slot cannot re-send in a loop.
                        if matches!(state.phase, Phase::InFlight { .. }) {
                            self.set_phase(&key, Phase::Ready);
                        }
                    }
                }
            }
        }
        for (key, need) in current {
            if !self.needs.contains_key(&key) {
                self.track_ready(shard, key, &need);
            }
        }
        touched
    }

    /// Serve every event delivered since the last pass and select the needs
    /// to dispatch now, up to the concurrency bound. A pass that fails leaves
    /// no need selected: needs it moved to dispatch, or to wait for the owner
    /// enrollment it would have asked for, are ready again, so a failed read
    /// never holds one of the in-flight slots or loses an enrollment request.
    pub(crate) fn pass(
        &mut self,
        source: &dyn DfsRepairNeedSource,
        input: DfsRepairPassInput<'_>,
    ) -> Result<DfsRepairExecutorSelection, DfsError> {
        let mut selected = Vec::new();
        let result = self.pass_selecting(source, input, &mut selected);
        if result.is_err() {
            for key in selected {
                if matches!(
                    self.needs.get(&key).map(|state| &state.phase),
                    Some(Phase::Dispatching | Phase::AwaitingOwner)
                ) {
                    self.set_phase(&key, Phase::Ready);
                }
            }
        }
        result
    }

    fn pass_selecting(
        &mut self,
        source: &dyn DfsRepairNeedSource,
        input: DfsRepairPassInput<'_>,
        selected: &mut Vec<DfsRepairNeedKey>,
    ) -> Result<DfsRepairExecutorSelection, DfsError> {
        let now = input.now;
        let mut examined = 0usize;
        self.observe_primaries(input.primaries);
        for (key, request_id, outcome, error) in input.outcomes {
            self.apply_outcome(&key, request_id.as_deref(), outcome, error, now);
        }
        let recount: BTreeSet<_> = self
            .primaries
            .keys()
            .filter(|shard| !self.need_counts.contains_key(*shard))
            .chain(input.changed.keys())
            .chain(input.replaced.iter())
            .filter(|shard| self.primaries.contains_key(*shard))
            .cloned()
            .collect();
        for shard in input.replaced {
            if self.primaries.contains_key(&shard) {
                self.traversals.insert(shard, None);
            }
        }
        for (shard, objects) in input.changed {
            if !self.primaries.contains_key(&shard) || objects.is_empty() {
                continue;
            }
            let current = source.needs_for_objects(&shard, &objects)?;
            examined += current.len();
            examined += self.apply_object_needs(&shard, &objects, current);
        }
        // A demand goes ahead of other ready needs and admits a need never
        // attempted yet. It never cuts short a backoff or a park a failed
        // attempt earned: those end at their time or reachability edge.
        for object in &input.demanded {
            if !self.by_object.contains_key(object) {
                let objects = BTreeSet::from([object.clone()]);
                for shard in self.primaries.keys().cloned().collect::<Vec<_>>() {
                    let current = source.needs_for_objects(&shard, &objects)?;
                    examined += current.len();
                    examined += self.apply_object_needs(&shard, &objects, current);
                }
            }
            let keys = self.by_object.get(object).cloned().unwrap_or_default();
            for key in keys {
                examined += 1;
                if matches!(
                    self.needs.get(&key).map(|state| &state.phase),
                    Some(Phase::Ready)
                ) {
                    self.file_ready(&key, true);
                }
            }
        }
        while let Some((due, key)) = self.due.first().cloned() {
            if due > now {
                break;
            }
            examined += 1;
            self.set_phase(&key, Phase::Ready);
        }
        // Every pass releases the needs parked on targets its own fresh
        // exclusions no longer contain. An exclusion can end on a
        // reachability edge or evidence expiry, which wake a pass, but also
        // with another wake's pass, such as after a stored status leaves
        // Offline or a membership change: one lookup per parked target, never
        // a read of the parked needs themselves.
        let reachable = self
            .parked
            .keys()
            .filter(|target| !input.exclusions.contains(target))
            .cloned()
            .collect::<Vec<_>>();
        for target in reachable {
            let keys = self.parked.get(&target).cloned().unwrap_or_default();
            examined += keys.len();
            for key in keys {
                self.set_phase(&key, Phase::Ready);
            }
        }
        if input.authority_changed {
            examined += self.awaiting_authority.len();
            for key in self.awaiting_authority.clone() {
                self.set_phase(&key, Phase::Ready);
            }
        }
        let mut traversal_budget = DFS_REPAIR_TRAVERSAL_PER_PASS;
        for shard in self.traversals.keys().cloned().collect::<Vec<_>>() {
            while traversal_budget > 0 {
                let after = self.traversals.get(&shard).cloned().flatten();
                let page = source.page(
                    &shard,
                    after.as_ref(),
                    DFS_REPAIR_TRAVERSAL_PAGE.min(traversal_budget),
                )?;
                let reached_end = page.len() < DFS_REPAIR_TRAVERSAL_PAGE.min(traversal_budget);
                traversal_budget = traversal_budget.saturating_sub(page.len());
                examined += page.len();
                let last = page.last().map(|(key, _)| key.clone());
                for (key, need) in page {
                    if !self.needs.contains_key(&key) {
                        self.track_ready(&shard, key, &need);
                    }
                }
                if reached_end || last.is_none() {
                    self.traversals.remove(&shard);
                    break;
                }
                self.traversals.insert(shard.clone(), last);
            }
        }
        for shard in recount {
            let count = source.need_count(&shard)?;
            self.need_counts.insert(shard, count);
        }

        let mut candidates = Vec::new();
        let mut ownerless = BTreeSet::new();
        let limit = input.limit.min(self.capacity());
        while candidates.len() < limit {
            let batch = self
                .ready
                .iter()
                .take(limit - candidates.len())
                .map(|(_, key)| key.clone())
                .collect::<Vec<_>>();
            if batch.is_empty() {
                break;
            }
            let mut by_shard = BTreeMap::<String, Vec<DfsRepairNeedKey>>::new();
            for key in &batch {
                let shard = self.needs.get(key).map(|state| state.shard.clone());
                if let Some(shard) = shard {
                    by_shard.entry(shard).or_default().push(key.clone());
                }
            }
            let mut found = BTreeMap::new();
            for (shard, keys) in by_shard {
                let needs = source.needs_for_keys(&shard, &keys)?;
                examined += needs.len();
                found.extend(
                    needs
                        .into_iter()
                        .map(|(key, need)| (key, (shard.clone(), need))),
                );
            }
            for key in batch {
                let Some((_, need)) = found.remove(&key) else {
                    self.forget(&key);
                    continue;
                };
                if !need.is_dispatchable(now) {
                    // Live demand without any owner asks for one and stays
                    // tracked until a change to its need: the owner's
                    // enrollment, or the end of the unfinished write that
                    // blocks enrollment, which is part of the need. While
                    // that write runs, enrollment would decline: nothing is
                    // asked. Anything else the index no longer makes
                    // dispatchable is dropped: the change that makes it so
                    // again brings it back.
                    if need.owner_intent_id.is_none() && need.root.is_live(now) {
                        if need.enrollment_blocked_by.is_none() {
                            ownerless.insert((key.0.clone(), key.1.clone()));
                        }
                        self.set_phase(&key, Phase::AwaitingOwner);
                        selected.push(key.clone());
                    } else {
                        self.forget(&key);
                    }
                    continue;
                }
                let failures = self.needs.get(&key).map_or(0, |state| state.failures);
                if input.exclusions.contains(&key.2) && failures > 0 {
                    // The first attempt is never deferred by a cached
                    // projection; an attempted target waits for its
                    // reachability change.
                    self.set_phase(&key, Phase::Parked);
                    continue;
                }
                if let Some(state) = self.needs.get_mut(&key) {
                    state.generation = need.receipt_generation;
                }
                self.set_phase(&key, Phase::Dispatching);
                selected.push(key.clone());
                candidates.push(DfsRepairExecutorCandidate { key, need });
            }
        }
        self.exclusion_expiries = input.exclusions.reevaluate_at_by_target().clone();
        self.examined_total = self.examined_total.saturating_add(examined as u64);
        Ok(DfsRepairExecutorSelection {
            candidates,
            ownerless,
            examined,
        })
    }

    /// Up to `limit` tracked needs as operator status rows.
    pub(crate) fn status_sample(&self, limit: usize) -> Vec<crate::DfsRepairAttemptRecord> {
        self.needs
            .iter()
            .take(limit)
            .map(|((object_id, version, target), state)| {
                let (in_flight, request_id, next) = match &state.phase {
                    Phase::InFlight {
                        request_id,
                        recheck_at,
                    } => (true, Some(request_id.clone()), *recheck_at),
                    Phase::Dispatching => (true, None, 0),
                    Phase::Backoff { due } => (false, None, *due),
                    Phase::Ready => (false, None, 0),
                    // Only an edge releases these; no time does.
                    Phase::Parked | Phase::AwaitingAuthority | Phase::AwaitingOwner => {
                        (false, None, u64::MAX)
                    }
                };
                crate::DfsRepairAttemptRecord {
                    intent_id: state.intent_id.clone().unwrap_or_default(),
                    object_id: object_id.clone(),
                    version: version.clone(),
                    content_hash: state.content_hash.clone(),
                    missing_replica_node_id: target.clone(),
                    attempts: state.failures.saturating_add(u32::from(in_flight)),
                    in_flight,
                    last_attempt_unix_nanos: state.last_attempt_unix_nanos,
                    next_retry_unix_nanos: next,
                    transfer_request_id: request_id,
                    last_error: state.last_error.clone(),
                    transfer_delivery_state: None,
                    completed_transfer_request_id: None,
                    transfer_kind: Some(
                        sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair,
                    ),
                }
            })
            .collect()
    }

    /// Test support: every backoff ends now, as the executor's timer would
    /// at its end.
    #[cfg(test)]
    pub(crate) fn elapse_backoffs_for_test(&mut self) {
        let backing_off = self
            .needs
            .iter()
            .filter(|(_, state)| matches!(state.phase, Phase::Backoff { .. }))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in backing_off {
            self.set_phase(&key, Phase::Backoff { due: 0 });
        }
    }

    /// How the owner enrollment the pass asked for ended, for the needs of
    /// `object_id`'s `version` that wait for an owner.
    pub(crate) fn record_owner_enrollment(
        &mut self,
        object_id: &DfsObjectId,
        version: &crate::DfsVersion,
        enrollment: DfsRepairOwnerEnrollment,
        now: u64,
    ) {
        let keys = self
            .by_object
            .get(object_id)
            .into_iter()
            .flatten()
            .filter(|key| &key.1 == version && self.awaiting_owner.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            match &enrollment {
                // The owner's publication changes the index, which releases
                // the need; a decline waits for the change that alters it.
                DfsRepairOwnerEnrollment::Enrolled | DfsRepairOwnerEnrollment::Declined => {}
                DfsRepairOwnerEnrollment::AuthorityLag => {
                    self.set_phase(&key, Phase::AwaitingAuthority)
                }
                DfsRepairOwnerEnrollment::Failed { error } => {
                    let failures = {
                        let state = self.needs.get_mut(&key).expect("filed above");
                        state.failures = state.failures.saturating_add(1);
                        state.last_error = Some(error.clone());
                        state.failures
                    };
                    self.set_phase(
                        &key,
                        Phase::Backoff {
                            due: now.saturating_add(backoff_delay_nanos(failures)),
                        },
                    );
                }
            }
        }
    }

    pub(crate) fn record_dispatch(
        &mut self,
        key: &DfsRepairNeedKey,
        result: DfsRepairDispatchResult,
        now: u64,
    ) {
        let Some(state) = self.needs.get_mut(key) else {
            return;
        };
        if !matches!(state.phase, Phase::Dispatching) {
            return;
        }
        if !matches!(result, DfsRepairDispatchResult::Outcome(_)) {
            state.last_attempt_unix_nanos = now;
        }
        match result {
            DfsRepairDispatchResult::Sent { request_id } => self.set_phase(
                key,
                Phase::InFlight {
                    request_id,
                    recheck_at: now.saturating_add(in_flight_resend_nanos()),
                },
            ),
            DfsRepairDispatchResult::SendFailed { error } => {
                self.apply_outcome(key, None, DfsRepairAttemptOutcome::Failed, Some(error), now)
            }
            DfsRepairDispatchResult::Failed { error } => {
                self.apply_outcome(key, None, DfsRepairAttemptOutcome::Failed, Some(error), now)
            }
            DfsRepairDispatchResult::Outcome(outcome) => {
                self.apply_outcome(key, None, outcome, None, now)
            }
        }
    }
}

/// How an owner enrollment for live ownerless demand ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DfsRepairOwnerEnrollment {
    /// The enrollment's publication changed the object's needs; that index
    /// change releases the need.
    Enrolled,
    /// The local authority view lags the published assignment: the
    /// authority edge releases the need.
    AuthorityLag,
    /// Enrollment published nothing for the need: the version has no
    /// retention grant, the object is gone, or an existing owner is reported
    /// active without covering this need. Nothing in the executor repairs it
    /// meanwhile; the next change of the object's needs, which reaches the
    /// executor as an index change, asks again.
    Declined,
    /// Enrollment failed: back off like a failed attempt.
    Failed { error: String },
}

/// What dispatch did with one selected need.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DfsRepairDispatchResult {
    /// The request left; its reply or send failure is the outcome.
    Sent { request_id: String },
    /// The attempt was made and its send failed at once: a failed outcome.
    SendFailed { error: String },
    /// Dispatch failed before anything left: a failed outcome.
    Failed { error: String },
    /// Dispatch already decided the attempt without sending.
    Outcome(DfsRepairAttemptOutcome),
}

#[cfg(test)]
#[path = "scheduler_tests.rs"]
mod tests;
