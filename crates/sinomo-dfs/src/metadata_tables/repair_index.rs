//! Derived index of current repair needs, maintained from table rows.
//!
//! Every replica derives the same index from the same journal, so a successor
//! primary can schedule repair the moment it is assigned. The index is never
//! serialized: snapshot loads and journal replay rebuild it through the same
//! table operations, and the rollback recorder restores it per object.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Included, Unbounded};

use sinomo_core::node::NodeId;

use super::DfsMetadataTables;
use crate::metadata_keys::id_lower_bound;
use crate::metadata_summary::RepairRoot;
use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};

/// One missing replica of one object version.
pub(crate) type DfsRepairNeedKey = (DfsObjectId, DfsVersion, NodeId);

/// What dispatch needs for one missing replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DfsRepairNeed {
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) content_hash: String,
    /// The full write intent that owns this placement, or `None` for real
    /// version demand without an eligible owner.
    pub(crate) owner_intent_id: Option<String>,
    /// How long the demand holds; `Until` roots expire without a write.
    pub(crate) root: RepairRoot,
    pub(crate) owner_root: Option<RepairRoot>,
    /// Generation of the missing replica's receipt slot, which a repair
    /// reply must echo to publish.
    pub(crate) receipt_generation: u64,
    /// For demand without an owner: an unfinished write intent of the same
    /// version, which owns the operation until it ends and so blocks owner
    /// enrollment. Its end changes the need.
    pub(crate) enrollment_blocked_by: Option<String>,
}

impl DfsRepairNeed {
    /// Whether the executor may dispatch this need now: its root and its
    /// owner's root are still live.
    pub(crate) fn is_dispatchable(&self, now_unix_nanos: u64) -> bool {
        self.owner_intent_id.is_some()
            && self.root.is_live(now_unix_nanos)
            && self
                .owner_root
                .is_some_and(|root| root.is_live(now_unix_nanos))
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DfsRepairIndex {
    needs: BTreeMap<DfsRepairNeedKey, DfsRepairNeed>,
    /// Needs that have an owner, maintained with `needs` so status and
    /// placement read the count without traversing the index.
    owned_count: usize,
    /// Each object's needs before its first change since the last drain. The
    /// writer or replica applier drains it once per applied batch and hands
    /// the executor the objects whose needs changed on net, so a need that
    /// appears and settles inside one batch reaches nobody. Not table state.
    changed: BTreeMap<DfsObjectId, Vec<(DfsRepairNeedKey, DfsRepairNeed)>>,
}

impl PartialEq for DfsRepairIndex {
    fn eq(&self, other: &Self) -> bool {
        self.needs == other.needs
    }
}

fn owned(entries: &[(DfsRepairNeedKey, DfsRepairNeed)]) -> usize {
    entries
        .iter()
        .filter(|(_, need)| need.owner_intent_id.is_some())
        .count()
}

impl Eq for DfsRepairIndex {}

fn object_range(
    object_id: &DfsObjectId,
) -> (
    std::ops::Bound<DfsRepairNeedKey>,
    std::ops::Bound<DfsRepairNeedKey>,
) {
    (
        Included((object_id.clone(), id_lower_bound(), id_lower_bound())),
        Unbounded,
    )
}

impl DfsRepairIndex {
    pub(crate) fn object_entries(
        &self,
        object_id: &DfsObjectId,
    ) -> Vec<(DfsRepairNeedKey, DfsRepairNeed)> {
        self.needs
            .range(object_range(object_id))
            .take_while(|((id, _, _), _)| id == object_id)
            .map(|(key, need)| (key.clone(), need.clone()))
            .collect()
    }

    /// Replace one object's needs; returns whether they changed.
    pub(crate) fn replace_object(
        &mut self,
        object_id: &DfsObjectId,
        needs: BTreeMap<DfsRepairNeedKey, DfsRepairNeed>,
    ) -> bool {
        let current = self.object_entries(object_id);
        if current.len() == needs.len()
            && current
                .iter()
                .zip(&needs)
                .all(|((key, need), (new_key, new_need))| key == new_key && need == new_need)
        {
            return false;
        }
        for (key, _) in &current {
            self.needs.remove(key);
        }
        self.owned_count = self.owned_count.saturating_sub(owned(&current))
            + needs
                .values()
                .filter(|need| need.owner_intent_id.is_some())
                .count();
        self.needs.extend(needs);
        self.changed.entry(object_id.clone()).or_insert(current);
        true
    }

    /// The objects whose needs differ from their state at the previous drain.
    pub(crate) fn take_changed(&mut self) -> BTreeSet<DfsObjectId> {
        let changed = std::mem::take(&mut self.changed);
        changed
            .into_iter()
            .filter(|(object_id, before)| &self.object_entries(object_id) != before)
            .map(|(object_id, _)| object_id)
            .collect()
    }

    /// Needs that have an owner.
    pub(crate) fn owned_count(&self) -> usize {
        self.owned_count
    }

    /// Forget pending changes without a wake: a reload rebuilds the same
    /// index that an earlier drain already announced.
    pub(crate) fn clear_changed(&mut self) {
        self.changed.clear();
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.needs.len()
    }

    pub(crate) fn get(&self, key: &DfsRepairNeedKey) -> Option<&DfsRepairNeed> {
        self.needs.get(key)
    }

    /// Bounded page after `after`, in key order.
    pub(crate) fn page(
        &self,
        after: Option<&DfsRepairNeedKey>,
        limit: usize,
    ) -> Vec<(DfsRepairNeedKey, DfsRepairNeed)> {
        let lower = after.map_or(Unbounded, |key| std::ops::Bound::Excluded(key.clone()));
        self.needs
            .range((lower, Unbounded))
            .take(limit)
            .map(|(key, need)| (key.clone(), need.clone()))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn needs(&self) -> &BTreeMap<DfsRepairNeedKey, DfsRepairNeed> {
        &self.needs
    }
}

impl DfsMetadataTables {
    pub(crate) fn repair_index(&self) -> &DfsRepairIndex {
        &self.repair_index
    }

    /// Drain the objects whose repair needs applied table operations changed.
    pub(crate) fn take_repair_index_changed(&mut self) -> BTreeSet<DfsObjectId> {
        self.repair_index.take_changed()
    }

    pub(crate) fn clear_repair_index_changes(&mut self) {
        self.repair_index.clear_changed();
    }

    /// Recompute one object's needs from its rows.
    pub(crate) fn refresh_repair_index_for_object(&mut self, object_id: &DfsObjectId) {
        let (_, needs) = self.object_status_and_repair_needs(object_id);
        self.repair_index.replace_object(object_id, needs);
    }

    /// Recompute every need from rows, without consulting the incremental
    /// index. Parity checks compare this with the maintained index.
    #[cfg(test)]
    pub(crate) fn repair_needs_from_rows(&self) -> BTreeMap<DfsRepairNeedKey, DfsRepairNeed> {
        let mut needs = BTreeMap::new();
        let object_ids = self
            .objects
            .keys()
            .chain(self.compacted_tombstones.keys())
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        for object_id in object_ids {
            needs.extend(self.object_status_and_repair_needs(&object_id).1);
        }
        needs
    }
}

/// Parity check: the maintained index equals a recomputation from rows.
#[cfg(test)]
pub(crate) fn assert_repair_index_matches_rows(tables: &DfsMetadataTables, context: &str) {
    let from_rows = tables.repair_needs_from_rows();
    assert_eq!(
        tables.repair_index.needs(),
        &from_rows,
        "derived repair index diverged after {context}"
    );
    assert_eq!(
        tables.repair_index.owned_count(),
        from_rows
            .values()
            .filter(|need| need.owner_intent_id.is_some())
            .count(),
        "maintained repair need count diverged after {context}"
    );
}
