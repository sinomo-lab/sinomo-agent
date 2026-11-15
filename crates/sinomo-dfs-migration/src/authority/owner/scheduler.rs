//! One demand/background queue. Conversion progress remains in the job checkpoint.
use super::inventory::{Inventory, ObservedState, UnitKey, UnitReference};
use super::*;
use std::collections::VecDeque;

#[derive(Debug)]
pub(super) struct Scheduler {
    units: BTreeMap<String, UnitReference>,
    demand: VecDeque<String>,
    background: VecDeque<String>,
    queued: BTreeSet<String>,
    running: BTreeSet<String>,
    prefer_demand: bool,
    retry_at: BTreeMap<String, std::time::Instant>,
    content_verified: BTreeMap<String, (String, String)>,
}
impl Scheduler {
    pub fn new() -> Self {
        Self { units: BTreeMap::new(), demand: VecDeque::new(), background: VecDeque::new(), queued: BTreeSet::new(), running: BTreeSet::new(), prefer_demand: true, retry_at: BTreeMap::new(), content_verified: BTreeMap::new() }
    }
    /// Caller demand changes priority only; it cannot replace a durable job.
    pub fn enqueue(&mut self, key: UnitKey, demanded: bool) -> Result<()> {
        let id = key.id()?;
        self.units.entry(id.clone()).or_insert_with(|| UnitReference::discovered(key));
        if self.running.contains(&id) { return Ok(()); }
        if demanded && self.retry_at.get(&id).is_some_and(|at| *at > std::time::Instant::now()) {
            // A readiness wake is advisory, not a new source dependency. Repeated
            // checks must not turn Waiting into an immediate retry/notify loop.
            // Native observations still advance this same deadline below.
            return Ok(());
        }
        if demanded { self.retry_at.remove(&id); }
        if demanded {
            self.background.retain(|queued| queued != &id);
            if !self.demand.contains(&id) { self.demand.push_back(id.clone()); }
            self.queued.insert(id);
        } else if self.queued.insert(id.clone()) {
            self.background.push_back(id);
        }
        Ok(())
    }
    pub fn state(&self, key: &UnitKey) -> Result<Option<&ObservedState>> {
        Ok(self.units.get(&key.id()?).map(|unit| &unit.state))
    }
    pub fn next(&mut self) -> Option<(String, UnitReference)> {
        let now = std::time::Instant::now();
        let due = self.retry_at.iter().filter(|(_, at)| **at <= now).map(|(id, _)| id.clone()).collect::<Vec<_>>();
        for id in due {
            self.retry_at.remove(&id);
            if !self.running.contains(&id) && self.queued.insert(id.clone()) { self.background.push_back(id); }
        }
        // Alternate available classes so demanded work cannot starve cold DATA.
        let id = if self.prefer_demand { self.demand.pop_front().or_else(||self.background.pop_front()) }
            else { self.background.pop_front().or_else(||self.demand.pop_front()) }?;
        self.prefer_demand = !self.prefer_demand;
        self.queued.remove(&id);
        self.running.insert(id.clone());
        Some((id.clone(), self.units[&id].clone()))
    }
    pub fn finish(&mut self, id: &str, reference: UnitReference) {
        self.running.remove(id);
        if matches!(reference.state, ObservedState::Waiting { .. }) {
            self.retry_at.insert(id.to_owned(), std::time::Instant::now() + std::time::Duration::from_millis(250));
        } else { self.retry_at.remove(id); }
        self.units.insert(id.to_owned(), reference);
    }
    pub fn content_verified(&self, key: &UnitKey, inventory: &str, incarnation: &str) -> Result<bool> {
        Ok(self.content_verified.get(&key.id()?).is_some_and(|(actual, root)| actual == inventory && root == incarnation))
    }
    pub fn mark_content_verified(&mut self, key: &UnitKey, inventory: String, incarnation: String) -> Result<()> {
        self.content_verified.insert(key.id()?, (inventory, incarnation));
        while self.content_verified.len() > 4096 {
            let oldest = self.content_verified.keys().next().cloned().expect("nonempty cache");
            self.content_verified.remove(&oldest);
        }
        Ok(())
    }
    pub fn next_retry(&self) -> Option<std::time::Instant> { self.retry_at.values().copied().min() }
    pub fn dependencies_changed(&mut self) {
        for at in self.retry_at.values_mut() { *at = std::time::Instant::now(); }
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
        self.units.insert(id, reference);
        self.enqueue(key, false)
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
        Ok(Self { scheduler: Mutex::new(Scheduler::new()), inventory, discovery: Mutex::new(discovery),
            wake: Default::default(), changed, attached: Default::default(), active: Default::default(), cancelled: Default::default(), drained: Default::default() })
    }
    pub fn notify(&self) {
        self.changed.send_modify(|generation| *generation = generation.wrapping_add(1));
        self.wake.notify_one();
    }
}
