use super::inventory::{ObservedState, UnitKey, UnitReference, UnitRole};
use super::scheduler::Scheduler;

#[test]
fn repeated_pending_demand_keeps_backoff_but_native_progress_wakes_it() {
    let mut scheduler = Scheduler::new();
    let key = UnitKey { relative_root: "dfs/metadata/root".into(),
        role: UnitRole::Metadata { shard: sinomo_dfs::DfsMetadataShardId::new("root") } };
    scheduler.enqueue(key.clone(), true).unwrap();
    let (id, mut reference) = scheduler.next().unwrap();
    reference.state = ObservedState::Waiting { evidence: "held original source".into() };
    scheduler.finish(&id, reference);
    let retry = scheduler.next_retry().unwrap();
    for _ in 0..8 {
        scheduler.enqueue(key.clone(), true).unwrap();
        assert_eq!(scheduler.next_retry(), Some(retry));
        assert!(scheduler.next().is_none());
    }
    scheduler.dependencies_changed();
    scheduler.enqueue(key.clone(), true).unwrap();
    assert_eq!(scheduler.next().unwrap().1.unit, key);
}

#[test]
fn replayed_inventory_does_not_requeue_settled_units_ahead_of_cold_work() {
    let mut scheduler = Scheduler::new();
    let mut settled = Vec::new();
    for n in 0..32 {
        let key = UnitKey { relative_root: "dfs/content".into(), role: UnitRole::ContentVersion { object_id: "warm".into(), version: n.to_string() } };
        scheduler.enqueue(key.clone(), false).unwrap();
        let (id, mut actual) = scheduler.next().unwrap();
        actual.state = ObservedState::VerifiedCurrent;
        scheduler.finish(&id, actual.clone());
        settled.push(actual);
    }
    let cold = UnitKey { relative_root: "dfs/content".into(), role: UnitRole::ContentInventory };
    scheduler.enqueue(cold.clone(), false).unwrap();
    for _ in 0..4 {
        for reference in &settled { scheduler.recovered(reference.clone()).unwrap(); }
    }
    let (id, selected) = scheduler.next().unwrap();
    assert_eq!(selected.unit, cold);
    scheduler.finish(&id, UnitReference { state: ObservedState::VerifiedCurrent, ..selected });
    assert!(scheduler.next().is_none(), "rediscovery created repeat work for this live owner");
    // A genuinely new owner still revalidates persisted observations.
    let mut reopened = Scheduler::new();
    reopened.recovered(settled.pop().unwrap()).unwrap();
    assert!(reopened.next().is_some());
}
