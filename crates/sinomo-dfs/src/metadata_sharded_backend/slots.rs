//! The shard slots of one local sharded metadata router, and the one rule for
//! reading them node-wide.
//!
//! A node's copy of a shard is closed while opening it is refused because its
//! storage is closed: the root awaits storage preparation, or the shard's
//! storage record names a conversion in progress
//! ([`metadata_shard_storage_is_closed`]). A closed copy is still held. Three
//! kinds of access follow from that:
//!
//! - A read addressed to the shard ([`LocalMetadataShardSlots::get`] and the
//!   router's `shard_backend`) opens its slot and meets the refusal, which the
//!   caller can retry.
//! - A node-wide scan ([`LocalMetadataShardSlots::scan`] and its variants)
//!   reads every open copy and sees a closed one as a node without that copy
//!   sees it. The scan is the default way to visit slots; a new node-wide path
//!   gets this rule without naming it.
//! - A path that must see every copy for its answer to hold opts in with
//!   [`LocalMetadataShardSlots::every_copy`], which yields a closed copy's
//!   refusal instead of leaving it out.
//!
//! Nothing here treats a closed copy as absent data: its slot stays in the
//! map, unopened, and its files are not touched.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};

use super::{LocalMetadataShardSlot, metadata_shard_storage_is_closed};
use crate::metadata_backend::LocalSobjMetadataBackend;
use crate::metadata_error::Result;
use crate::metadata_records::DfsMetadataShardId;

/// Whether a slot's open was refused because its storage is closed.
///
/// It is set by the first open of the slot that meets that refusal: an eager
/// open while the router is built, the reservation a global checkout makes,
/// or the open a scan makes of a deferred slot. It is never cleared for the
/// router's lifetime, so a scan visits the same slots from its first page to
/// its last. A read addressed to the shard still tries to open it.
#[derive(Debug, Default)]
pub(super) struct ShardStorageClosure(AtomicBool);

impl ShardStorageClosure {
    /// A slot reserved closed by a global checkout, which never opened it.
    pub(super) fn reserved_closed() -> Self {
        Self(AtomicBool::new(true))
    }

    pub(super) fn is_closed(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Record `opened` for the slot: a refusal because its storage is closed
    /// marks it closed and becomes `Ok(None)`; any other result is returned.
    pub(super) fn observe_open<T>(&self, opened: Result<T>) -> Result<Option<T>> {
        match opened {
            Ok(opened) => Ok(Some(opened)),
            Err(error) if metadata_shard_storage_is_closed(&error) => {
                self.0.store(true, Ordering::Release);
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}

/// Every slot of one router, keyed by shard id in canonical order.
///
/// The map is private to this module: the router reaches a slot only by
/// addressing its shard, by scanning, or by opting in to every held slot.
#[derive(Debug, Default)]
pub(super) struct LocalMetadataShardSlots {
    slots: BTreeMap<DfsMetadataShardId, LocalMetadataShardSlot>,
}

impl FromIterator<(DfsMetadataShardId, LocalMetadataShardSlot)> for LocalMetadataShardSlots {
    fn from_iter<I: IntoIterator<Item = (DfsMetadataShardId, LocalMetadataShardSlot)>>(
        slots: I,
    ) -> Self {
        Self {
            slots: slots.into_iter().collect(),
        }
    }
}

impl LocalMetadataShardSlots {
    /// The number of shard copies the router holds, closed or not.
    pub(super) fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether the router holds a copy of `shard_id`, closed or not.
    pub(super) fn holds(&self, shard_id: &DfsMetadataShardId) -> bool {
        self.slots.contains_key(shard_id)
    }

    /// Whether the router holds a closed copy of `shard_id`, opening a deferred
    /// slot to find out. Any other open failure is returned.
    pub(super) fn probe_closed(&self, shard_id: &DfsMetadataShardId) -> Result<bool> {
        let Some(slot) = self.slots.get(shard_id) else {
            return Ok(false);
        };
        if slot.storage_closure.is_closed() {
            return Ok(true);
        }
        Ok(slot.storage_closure.observe_open(slot.backend())?.is_none())
    }

    /// The held copies known to be closed. After a scan this is every closed
    /// copy the router holds.
    pub(super) fn closed_ids(&self) -> impl Iterator<Item = &DfsMetadataShardId> {
        self.slots
            .iter()
            .filter(|(_, slot)| slot.storage_closure.is_closed())
            .map(|(shard_id, _)| shard_id)
    }

    /// The slot of a read addressed to `shard_id`. Opening a closed slot meets
    /// its refusal.
    pub(super) fn get(&self, shard_id: &DfsMetadataShardId) -> Option<&LocalMetadataShardSlot> {
        self.slots.get(shard_id)
    }

    /// [`Self::get`] for exclusive access.
    pub(super) fn get_mut(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Option<&mut LocalMetadataShardSlot> {
        self.slots.get_mut(shard_id)
    }

    /// Each open copy's backend in shard order, for a node-wide scan. A
    /// deferred slot opens here; a closed one is left out, as on a node
    /// without that copy. Any other open failure is yielded.
    pub(super) fn scan(
        &self,
    ) -> impl Iterator<Item = Result<(&DfsMetadataShardId, &LocalSobjMetadataBackend)>> {
        self.scan_slots()
            .map(|scanned| scanned.and_then(|(shard_id, slot)| Ok((shard_id, slot.backend()?))))
    }

    /// [`Self::scan`], yielding each open copy's slot, whose backend is open.
    pub(super) fn scan_slots(
        &self,
    ) -> impl Iterator<Item = Result<(&DfsMetadataShardId, &LocalMetadataShardSlot)>> {
        self.scan_slots_in((Bound::Unbounded, Bound::Unbounded))
    }

    /// [`Self::scan_slots`] over the shard ids in `range`, for a scan that
    /// resumes at a shard cursor.
    pub(super) fn scan_slots_in<'a>(
        &'a self,
        range: (Bound<&'a DfsMetadataShardId>, Bound<&'a DfsMetadataShardId>),
    ) -> impl Iterator<Item = Result<(&'a DfsMetadataShardId, &'a LocalMetadataShardSlot)>> + 'a
    {
        self.slots
            .range::<DfsMetadataShardId, _>(range)
            .filter_map(|(shard_id, slot)| {
                if slot.storage_closure.is_closed() {
                    return None;
                }
                match slot.storage_closure.observe_open(slot.backend()) {
                    Ok(Some(_)) => Some(Ok((shard_id, slot))),
                    Ok(None) => None,
                    Err(error) => Some(Err(error)),
                }
            })
    }

    /// [`Self::scan`] over the shards `keep` selects, decided before any slot
    /// opens, for a scan that reads only some of the node's shards.
    pub(super) fn scan_where<'a>(
        &'a self,
        keep: impl Fn(&DfsMetadataShardId) -> bool + 'a,
    ) -> impl Iterator<Item = Result<(&'a DfsMetadataShardId, &'a LocalSobjMetadataBackend)>> + 'a
    {
        self.slots.iter().filter_map(move |(shard_id, slot)| {
            if !keep(shard_id) || slot.storage_closure.is_closed() {
                return None;
            }
            match slot.storage_closure.observe_open(slot.backend()) {
                Ok(Some(backend)) => Some(Ok((shard_id, backend))),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            }
        })
    }

    /// [`Self::scan`] that also reads the copy a lookup names: `addressed`
    /// is read even when closed, so its refusal is yielded instead of the
    /// copy being left out. Shards come in shard order.
    pub(super) fn scan_addressing<'a>(
        &'a self,
        addressed: Option<&'a DfsMetadataShardId>,
    ) -> impl Iterator<Item = Result<(&'a DfsMetadataShardId, &'a LocalSobjMetadataBackend)>> + 'a
    {
        self.slots.iter().filter_map(move |(shard_id, slot)| {
            if addressed == Some(shard_id) {
                return Some(slot.backend().map(|backend| (shard_id, backend)));
            }
            if slot.storage_closure.is_closed() {
                return None;
            }
            match slot.storage_closure.observe_open(slot.backend()) {
                Ok(Some(backend)) => Some(Ok((shard_id, backend))),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            }
        })
    }

    /// The first shard id in `range` that a scan would visit, as far as it is
    /// known without opening a slot: a deferred slot not yet found closed is
    /// named, and the scan that resumes there leaves it out if it is.
    pub(super) fn next_scanned_id_in<'a>(
        &'a self,
        range: (Bound<&'a DfsMetadataShardId>, Bound<&'a DfsMetadataShardId>),
    ) -> Option<&'a DfsMetadataShardId> {
        self.slots
            .range::<DfsMetadataShardId, _>(range)
            .find(|(_, slot)| !slot.storage_closure.is_closed())
            .map(|(shard_id, _)| shard_id)
    }

    /// [`Self::scan`] with exclusive access to each open copy's backend.
    pub(super) fn scan_mut(
        &mut self,
    ) -> impl Iterator<Item = Result<(&DfsMetadataShardId, &mut LocalSobjMetadataBackend)>> {
        self.slots.iter_mut().filter_map(|(shard_id, slot)| {
            if slot.storage_closure.is_closed() {
                return None;
            }
            match slot
                .storage_closure
                .observe_open(slot.backend().map(|_| ()))
            {
                Ok(Some(())) => Some(slot.backend_mut().map(|backend| (shard_id, backend))),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            }
        })
    }

    /// The shard ids [`Self::scan`] visits, opening deferred slots as it does.
    pub(super) fn scanned_ids(&self) -> Result<Vec<DfsMetadataShardId>> {
        self.scan_slots()
            .map(|scanned| scanned.map(|(shard_id, _)| shard_id.clone()))
            .collect()
    }

    /// Each held copy's backend in shard order, for a path whose answer needs
    /// every copy the node holds. A closed copy yields its refusal rather than
    /// being left out.
    pub(super) fn every_copy(
        &self,
    ) -> impl Iterator<Item = Result<(&DfsMetadataShardId, &LocalSobjMetadataBackend)>> {
        self.slots
            .iter()
            .map(|(shard_id, slot)| Ok((shard_id, slot.backend()?)))
    }

    /// Every held slot, closed or not, without opening any. For the router's
    /// own bookkeeping: already-open backends, teardown, and topology checks.
    pub(super) fn including_closed(
        &self,
    ) -> impl Iterator<Item = (&DfsMetadataShardId, &LocalMetadataShardSlot)> {
        self.slots.iter()
    }

    /// [`Self::including_closed`] with exclusive access.
    pub(super) fn including_closed_mut(
        &mut self,
    ) -> impl Iterator<Item = (&DfsMetadataShardId, &mut LocalMetadataShardSlot)> {
        self.slots.iter_mut()
    }

    /// Every held slot, closed or not, by value: a lease returning them.
    pub(super) fn into_including_closed(
        self,
    ) -> impl Iterator<Item = (DfsMetadataShardId, LocalMetadataShardSlot)> {
        self.slots.into_iter()
    }
}
