//! One demand/background queue. Conversion progress remains in the job checkpoint.
use super::inventory::{Inventory, ObservedState, UnitKey, UnitReference};
use super::*;
use sinomo_core::node::NodeId;
use std::collections::VecDeque;

#[derive(Debug)]
pub(super) struct Scheduler {
    units: BTreeMap<String, UnitReference>,
    demand: VecDeque<String>,
    // Priority belongs to unfinished work, including its parked/running turns.
    // This advisory set is cleared at settlement and is not recovery authority.
    demanded: BTreeSet<String>,
    background: VecDeque<String>,
    queued: BTreeSet<String>,
    // A dependency arriving during a step must survive its eventual park.
    running: BTreeMap<String, bool>,
    prefer_demand: bool,
    retry_at: BTreeMap<String, std::time::Instant>,
    retry_delay: BTreeMap<String, std::time::Duration>,
    // Advisory exact-key subscriptions for the existing owned units. These
    // only wake revalidation; configuration and permission are read natively.
    native_watch_keys: BTreeMap<String, BTreeSet<String>>,
    // Advisory node subscriptions: a unit parked on an unreachable node is
    // requeued when that node becomes reachable, never by a timer.
    reachability_watch_nodes: BTreeMap<String, BTreeSet<NodeId>>,
    // Nodes that became reachable during a running step, so a watch that
    // step registers afterwards still observes them.
    reachable_while_running: BTreeMap<String, BTreeSet<NodeId>>,
    // Advisory verdict subscriptions: a unit waiting on how this node judges
    // a peer (a converting takeover waiting for its primary, or a candidate
    // that outranks it, to be judged lost) is requeued on any change of that
    // peer's verdict, never by a timer.
    verdict_watch_nodes: BTreeMap<String, BTreeSet<NodeId>>,
    // Nodes whose verdict changed during a running step.
    verdicts_changed_while_running: BTreeMap<String, BTreeSet<NodeId>>,
    // The re-send fallback of units parked on other shards' jobs, whose
    // events are best effort; it arms their re-checks in `retry_at`.
    source_job_rechecks: super::source_job_recheck::SourceJobRechecks,
    content_verified: BTreeMap<String, (String, String)>,
}
impl Scheduler {
    pub fn new() -> Self {
        Self {
            units: BTreeMap::new(),
            demand: VecDeque::new(),
            demanded: BTreeSet::new(),
            background: VecDeque::new(),
            queued: BTreeSet::new(),
            running: BTreeMap::new(),
            prefer_demand: true,
            retry_at: BTreeMap::new(),
            retry_delay: BTreeMap::new(),
            native_watch_keys: BTreeMap::new(),
            reachability_watch_nodes: BTreeMap::new(),
            reachable_while_running: BTreeMap::new(),
            verdict_watch_nodes: BTreeMap::new(),
            verdicts_changed_while_running: BTreeMap::new(),
            source_job_rechecks: Default::default(),
            content_verified: BTreeMap::new(),
        }
    }
    /// Caller demand changes priority only; it cannot replace a durable job.
    pub fn enqueue(&mut self, key: UnitKey, demanded: bool) -> Result<()> {
        let id = key.id()?;
        self.units
            .entry(id.clone())
            .or_insert_with(|| UnitReference::discovered(key));
        if demanded && !matches!(self.units[&id].state, ObservedState::Blocked { .. }) {
            self.demanded.insert(id.clone());
        }
        if let Some(changed) = self.running.get_mut(&id) {
            if !demanded {
                *changed = true;
            }
            return Ok(());
        }
        if matches!(self.units[&id].state, ObservedState::Blocked { .. })
            || (demanded && matches!(self.units[&id].state, ObservedState::Waiting { .. }))
        {
            // Reading readiness is not evidence of a dependency change.
            return Ok(());
        }
        if demanded && matches!(self.units[&id].state, ObservedState::VerifiedCurrent | ObservedState::RetainedOriginal) {
            // Demand reaches the queue only after current DATA inspection
            // failed. Invalidate the previous readiness observation so the
            // eventual repair publishes a fresh edge even if its class matches.
            self.units.get_mut(&id).expect("registered unit").state = ObservedState::Discovered;
        }
        self.retry_at.remove(&id);
        self.queue(id);
        Ok(())
    }
    fn queue(&mut self, id: String) {
        if self.demanded.contains(&id) {
            self.background.retain(|queued| queued != &id);
            if !self.demand.contains(&id) { self.demand.push_back(id.clone()); }
            self.queued.insert(id);
        } else if self.queued.insert(id.clone()) {
            self.background.push_back(id);
        }
    }
    pub fn state(&self, key: &UnitKey) -> Result<Option<&ObservedState>> {
        Ok(self.units.get(&key.id()?).map(|unit| &unit.state))
    }
    pub fn next(&mut self) -> Option<(String, UnitReference)> {
        let now = std::time::Instant::now();
        let due = self.retry_at.iter().filter(|(_, at)| **at <= now).map(|(id, _)| id.clone()).collect::<Vec<_>>();
        for id in due {
            self.retry_at.remove(&id);
            if !self.running.contains_key(&id) {
                self.queue(id);
            }
        }
        // Alternate available classes so demanded work cannot starve cold DATA.
        let id = if self.prefer_demand { self.demand.pop_front().or_else(||self.background.pop_front()) }
            else { self.background.pop_front().or_else(||self.demand.pop_front()) }?;
        self.prefer_demand = !self.prefer_demand;
        self.queued.remove(&id);
        self.running.insert(id.clone(), false);
        self.reachable_while_running.insert(id.clone(), BTreeSet::new());
        self.verdicts_changed_while_running.insert(id.clone(), BTreeSet::new());
        self.source_job_rechecks.started(&id);
        Some((id.clone(), self.units[&id].clone()))
    }
    pub fn finish(&mut self, id: &str, reference: UnitReference) -> bool {
        let changed_while_running = self.running.remove(id).unwrap_or(false);
        self.reachable_while_running.remove(id);
        self.verdicts_changed_while_running.remove(id);
        self.retry_at.remove(id);
        if !matches!(reference.state, ObservedState::Waiting { .. }) {
            self.retry_delay.remove(id);
        }
        let readiness_changed = self.units.get(id).is_none_or(|previous| {
            std::mem::discriminant(&previous.state) != std::mem::discriminant(&reference.state)
        }) && matches!(
            reference.state,
            ObservedState::VerifiedCurrent
                | ObservedState::RetainedOriginal
                | ObservedState::Blocked { .. }
        );
        let rerun =
            changed_while_running && matches!(reference.state, ObservedState::Waiting { .. });
        let waiting = matches!(reference.state, ObservedState::Waiting { .. }) && !rerun;
        if let Some(delay) = self.source_job_rechecks.finished(id, waiting) {
            self.retry_at.insert(id.to_owned(), std::time::Instant::now() + delay);
        }
        if matches!(reference.state, ObservedState::VerifiedCurrent | ObservedState::RetainedOriginal | ObservedState::Blocked { .. }) {
            self.native_watch_keys.remove(id);
            self.reachability_watch_nodes.remove(id);
            self.verdict_watch_nodes.remove(id);
            self.demanded.remove(id);
        }
        self.units.insert(id.to_owned(), reference);
        if rerun {
            self.queue(id.to_owned());
        }
        readiness_changed
    }
    /// Timers belong only to an attempted operation that returned a retryable
    /// error and to the re-check of a wait on other shards' jobs
    /// ([`Self::recheck_source_jobs`]), never to another parked dependency
    /// or a retained archive.
    pub fn retry_failed(&mut self, id: &str) {
        if self.queued.contains(id) {
            return;
        }
        let delay = self
            .retry_delay
            .entry(id.to_owned())
            .or_insert(std::time::Duration::from_millis(250));
        self.retry_at
            .insert(id.to_owned(), std::time::Instant::now() + *delay);
        *delay = (*delay * 2).min(std::time::Duration::from_secs(30));
    }
    pub fn content_verified(
        &self,
        key: &UnitKey,
        inventory: &str,
        incarnation: &str,
    ) -> Result<bool> {
        Ok(self
            .content_verified
            .get(&key.id()?)
            .is_some_and(|(actual, root)| actual == inventory && root == incarnation))
    }
    pub fn mark_content_verified(
        &mut self,
        key: &UnitKey,
        inventory: String,
        incarnation: String,
    ) -> Result<()> {
        self.content_verified
            .insert(key.id()?, (inventory, incarnation));
        while self.content_verified.len() > 4096 {
            let oldest = self.content_verified.keys().next().cloned().expect("nonempty cache");
            self.content_verified.remove(&oldest);
        }
        Ok(())
    }
    pub fn next_retry(&self) -> Option<std::time::Instant> {
        self.retry_at.values().copied().min()
    }
    pub fn dependencies_changed(&mut self, affected: impl Fn(&UnitKey) -> bool) -> bool {
        let ids = self
            .units
            .iter()
            .filter(|(id, reference)| {
                affected(&reference.unit)
                    && (self.running.contains_key(*id)
                        || matches!(reference.state, ObservedState::Waiting { .. }))
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let changed = !ids.is_empty();
        for id in ids {
            self.retry_at.remove(&id);
            self.source_job_rechecks.woken(&id);
            if let Some(changed) = self.running.get_mut(&id) {
                *changed = true;
            } else {
                self.queue(id);
            }
        }
        changed
    }
    pub fn native_dependencies_changed(
        &mut self,
        observation: &sinomo_core::kv::KvCommittedObservation,
    ) -> bool {
        use sinomo_core::kv::KvCommittedObservationValue;
        let exact = match &observation.value {
            KvCommittedObservationValue::Delta(delta) => self.native_watch_keys.iter()
                .filter(|(_, keys)| delta.events.iter().any(|event| keys.contains(&event.key)))
                .map(|(id, _)| id.clone()).collect::<BTreeSet<_>>(),
            _ => BTreeSet::new(),
        };
        self.dependencies_changed(|unit| match &observation.value {
            KvCommittedObservationValue::Delta(delta) => unit.id().is_ok_and(|id| exact.contains(&id)) || delta
                .events
                .iter()
                .any(|event| native_dependency(&unit.role, &event.key)),
            KvCommittedObservationValue::Snapshot(_)
            | KvCommittedObservationValue::Discontinuity(_) => matches!(
                unit.role,
                inventory::UnitRole::FileSystem { .. }
                    | inventory::UnitRole::Metadata { .. }
                    | inventory::UnitRole::MetadataGeneration { .. }
                    | inventory::UnitRole::MetadataRouting
                    | inventory::UnitRole::PlacementGoals
            ),
        })
    }
    pub fn watch_native_keys(&mut self, unit: &UnitKey, keys: BTreeSet<String>) -> Result<()> {
        let id = unit.id()?;
        if !self.units.contains_key(&id) { return Err(error("native dependency has no existing work owner")); }
        self.native_watch_keys.insert(id, keys);
        Ok(())
    }
    /// Park `unit` on `nodes` until one of them becomes reachable. A node
    /// that became reachable earlier in the unit's running step reruns it.
    pub fn watch_reachable_nodes(&mut self, unit: &UnitKey, nodes: BTreeSet<NodeId>) -> Result<()> {
        let id = unit.id()?;
        if !self.units.contains_key(&id) { return Err(error("reachability dependency has no existing work owner")); }
        if let (Some(changed), Some(seen)) = (self.running.get_mut(&id), self.reachable_while_running.get(&id))
            && !seen.is_disjoint(&nodes) {
            *changed = true;
        }
        self.reachability_watch_nodes.insert(id, nodes);
        Ok(())
    }
    /// The running step of `unit` parks on other shards' jobs, whose events
    /// are best effort: re-check it on the re-send fallback
    /// ([`super::source_job_recheck`]) until a step no longer parks so.
    pub fn recheck_source_jobs(&mut self, unit: &UnitKey) -> Result<()> {
        let id = unit.id()?;
        if !self.units.contains_key(&id) { return Err(error("source-job wait has no existing work owner")); }
        self.source_job_rechecks.parked(id);
        Ok(())
    }
    /// Requeue `unit` on any change of how this node judges one of `nodes`.
    /// A change earlier in the unit's running step reruns it.
    pub fn watch_node_verdicts(&mut self, unit: &UnitKey, nodes: BTreeSet<NodeId>) -> Result<()> {
        let id = unit.id()?;
        if !self.units.contains_key(&id) { return Err(error("verdict dependency has no existing work owner")); }
        if let (Some(changed), Some(seen)) = (self.running.get_mut(&id), self.verdicts_changed_while_running.get(&id))
            && !seen.is_disjoint(&nodes) {
            *changed = true;
        }
        self.verdict_watch_nodes.insert(id, nodes);
        Ok(())
    }
    /// Every node some unit waits to reach or waits on the verdict of.
    pub fn watched_nodes(&self) -> BTreeSet<NodeId> {
        self.reachability_watch_nodes.values().chain(self.verdict_watch_nodes.values()).flatten().cloned().collect()
    }
    /// Requeue the Waiting or running units watching the verdict of any of
    /// `nodes`.
    pub fn node_verdicts_changed(&mut self, nodes: &BTreeSet<NodeId>) -> bool {
        for seen in self.verdicts_changed_while_running.values_mut() {
            seen.extend(nodes.iter().cloned());
        }
        let watching = self.verdict_watch_nodes.iter()
            .filter(|(_, watched)| !watched.is_disjoint(nodes))
            .map(|(id, _)| id.clone()).collect::<BTreeSet<_>>();
        !watching.is_empty() && self.dependencies_changed(|unit| unit.id().is_ok_and(|id| watching.contains(&id)))
    }
    /// Requeue the Waiting or running units watching any of `nodes`.
    pub fn nodes_became_reachable(&mut self, nodes: &BTreeSet<NodeId>) -> bool {
        for seen in self.reachable_while_running.values_mut() {
            seen.extend(nodes.iter().cloned());
        }
        let watching = self.reachability_watch_nodes.iter()
            .filter(|(_, watched)| !watched.is_disjoint(nodes))
            .map(|(id, _)| id.clone()).collect::<BTreeSet<_>>();
        !watching.is_empty() && self.dependencies_changed(|unit| unit.id().is_ok_and(|id| watching.contains(&id)))
    }
    /// Reloaded observations are hints; the owner revalidates current DATA.
    pub fn recovered(&mut self, reference: UnitReference) -> Result<()> {
        let key = reference.unit.clone();
        let id = key.id()?;
        if let Some(existing) = self.units.get_mut(&id) {
            if existing.job.is_some() && reference.job != existing.job {
                return Err(error("rediscovery changed an already-owned original job"));
            }
            if existing.job.is_none() { existing.job = reference.job; }
            // A directory may be replayed, but that is not a new execution or
            // a reason to revalidate the same live owner's settled unit forever.
            return Ok(());
        }
        self.content_verified.remove(&id);
        self.units.insert(id.clone(), reference);
        // First recovery revalidates every retained observation, including a
        // prior hard failure. Ordinary background requests cannot repeat it.
        if self.queued.insert(id.clone()) {
            self.background.push_back(id);
        }
        Ok(())
    }
}

fn native_dependency(role: &inventory::UnitRole, key: &str) -> bool {
    use inventory::UnitRole;
    // A converted generation staged on a member waits only on its own
    // shard's serving, write-set and storage records.
    if let UnitRole::MetadataGeneration { identity } = role
        && super::conversion_receiver::conversion_attempt_of(identity).is_some()
    {
        let (fs, shard) = (&identity.file_system_id, &identity.shard_id.0);
        return key == sinomo_dfs::metadata_shard_key(fs, shard)
            || key == sinomo_dfs::metadata_shard_write_set_key(fs, shard)
            || key == sinomo_dfs::metadata_shard_storage_key(fs, shard);
    }
    let Some(tail) = key.strip_prefix("/dfs/filesystems/") else {
        return false;
    };
    let Some((filesystem, record)) = tail.split_once('/') else {
        return false;
    };
    let relevant = record == "config"
        || record.starts_with("metadata-")
        || record.starts_with("placement-reconciliation");
    relevant
        && match role {
            UnitRole::FileSystem { file_system_id, .. } => filesystem == file_system_id.to_string(),
            UnitRole::MetadataGeneration { identity } => {
                filesystem == identity.file_system_id.to_string()
            }
            UnitRole::Metadata { shard } => {
                record == "config" || record.rsplit('/').next() == Some(&shard.0)
            }
            UnitRole::MetadataRouting | UnitRole::PlacementGoals => true,
            _ => false,
        }
}

pub(in crate::authority) struct OwnerWork {
    pub scheduler: Mutex<Scheduler>,
    pub inventory: Inventory,
    pub discovery: Mutex<super::discovery::Discovery>,
    pub wake: tokio::sync::Notify,
    /// Wakes the reachability watcher when a unit registers nodes to wait on.
    pub reachability: tokio::sync::Notify,
    pub changed: tokio::sync::watch::Sender<u64>,
    pub attached: std::sync::atomic::AtomicBool,
    pub active: std::sync::atomic::AtomicUsize,
    pub cancelled: std::sync::atomic::AtomicBool,
    pub drained: tokio::sync::Notify,
}
impl OwnerWork {
    pub fn new(data_root: &Path) -> Result<Self> {
        let (changed, _) = tokio::sync::watch::channel(0);
        let inventory = Inventory::open(data_root)?;
        let discovery = super::discovery::Discovery::open(data_root, inventory.root())?;
        Ok(Self {
            scheduler: Mutex::new(Scheduler::new()),
            inventory,
            discovery: Mutex::new(discovery),
            wake: Default::default(),
            reachability: Default::default(),
            changed,
            attached: Default::default(),
            active: Default::default(),
            cancelled: Default::default(),
            drained: Default::default(),
        })
    }
    /// Park `unit` on `nodes` (decision Q2): it waits for one of them to
    /// become reachable instead of retrying on a timer.
    pub(super) fn watch_reachable_nodes(&self, unit: &UnitKey, nodes: BTreeSet<NodeId>) -> Result<()> {
        self.scheduler.lock().map_err(error)?.watch_reachable_nodes(unit, nodes)?;
        self.reachability.notify_one();
        Ok(())
    }
    /// Requeue `unit` when this node's verdict of any of `nodes` changes.
    pub(super) fn watch_node_verdicts(&self, unit: &UnitKey, nodes: BTreeSet<NodeId>) -> Result<()> {
        let watching = !nodes.is_empty();
        self.scheduler.lock().map_err(error)?.watch_node_verdicts(unit, nodes)?;
        if watching {
            self.reachability.notify_one();
        }
        Ok(())
    }
    pub fn notify(&self) {
        if let Ok(mut scheduler) = self.scheduler.lock() {
            scheduler.dependencies_changed(|_| true);
        }
        self.changed
            .send_modify(|generation| *generation = generation.wrapping_add(1));
        self.wake.notify_one();
    }
}

#[cfg(test)]
mod wake_tests {
    use super::*;
    use inventory::UnitRole;

    /// A member's staged converted generation wakes only on its own shard's
    /// serving, write-set and storage records; a complete-filesystem
    /// cohort's generation keeps waking on every metadata record of its
    /// filesystem.
    #[test]
    fn a_staged_conversion_wakes_only_on_its_own_shards_records() {
        let fs = sinomo_dfs::sifs_file_system_id();
        let converted = sinomo_dfs::metadata_shard_converted_generation(&fs, "users", &"e".repeat(64));
        let staged = UnitRole::MetadataGeneration { identity: converted.clone() };
        for key in [
            sinomo_dfs::metadata_shard_key(&fs, "users"),
            sinomo_dfs::metadata_shard_write_set_key(&fs, "users"),
            sinomo_dfs::metadata_shard_storage_key(&fs, "users"),
        ] {
            assert!(native_dependency(&staged, &key), "{key}");
        }
        for key in [
            sinomo_dfs::file_system_config_key(&fs),
            sinomo_dfs::metadata_shard_storage_key(&fs, "services"),
            sinomo_dfs::metadata_shard_key(&fs, "root"),
            sinomo_dfs::metadata_shard_membership_handoff_key(&fs, "users"),
        ] {
            assert!(!native_dependency(&staged, &key), "{key}");
        }
        let mut cohort = converted;
        cohort.generation = "filesystem-generation".into();
        let cohort = UnitRole::MetadataGeneration { identity: cohort };
        assert!(native_dependency(&cohort, &sinomo_dfs::file_system_config_key(&fs)));
        assert!(native_dependency(&cohort, &sinomo_dfs::metadata_shard_storage_key(&fs, "services")));
    }
}
