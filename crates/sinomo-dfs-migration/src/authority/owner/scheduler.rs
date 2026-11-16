//! One demand/background queue. Conversion progress remains in the job checkpoint.
use super::inventory::{Inventory, ObservedState, UnitKey, UnitReference};
use super::*;
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
        Some((id.clone(), self.units[&id].clone()))
    }
    pub fn finish(&mut self, id: &str, reference: UnitReference) -> bool {
        let changed_while_running = self.running.remove(id).unwrap_or(false);
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
        if matches!(reference.state, ObservedState::VerifiedCurrent | ObservedState::RetainedOriginal | ObservedState::Blocked { .. }) {
            self.native_watch_keys.remove(id);
            self.demanded.remove(id);
        }
        self.units.insert(id.to_owned(), reference);
        if rerun {
            self.queue(id.to_owned());
        }
        readiness_changed
    }
    /// Timers belong only to an attempted operation that returned a retryable
    /// error, never to a successfully parked dependency or retained archive.
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
            changed,
            attached: Default::default(),
            active: Default::default(),
            cancelled: Default::default(),
            drained: Default::default(),
        })
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
