use super::inventory::{ObservedState, UnitKey, UnitReference, UnitRole};
use super::scheduler::Scheduler;

#[test]
fn caller_priority_survives_owned_steps_and_dependency_waits() {
    for arrival in ["queued", "running", "waiting"] {
        let mut scheduler = Scheduler::new();
        let demanded = UnitKey {
            relative_root: "dfs/content".into(),
            role: UnitRole::ContentVersion {
                object_id: "new-object".into(),
                version: "new-version".into(),
            },
        };
        scheduler
            .enqueue(demanded.clone(), arrival == "queued")
            .unwrap();
        let (id, mut reference) = scheduler.next().unwrap();
        if arrival == "running" {
            scheduler.enqueue(demanded.clone(), true).unwrap();
        }
        reference.state = if arrival == "waiting" {
            ObservedState::Waiting {
                evidence: "original index is not complete".into(),
            }
        } else {
            ObservedState::Runnable
        };
        scheduler.finish(&id, reference);
        if arrival == "waiting" {
            scheduler.enqueue(demanded.clone(), true).unwrap();
            assert!(
                scheduler.next().is_none(),
                "priority alone cannot release a dependency wait"
            );
            assert!(scheduler.next_retry().is_none());
        }
        for n in 0..32 {
            scheduler
                .enqueue(
                    UnitKey {
                        relative_root: "dfs/content".into(),
                        role: UnitRole::ContentVersion {
                            object_id: "old-object".into(),
                            version: n.to_string(),
                        },
                    },
                    false,
                )
                .unwrap();
        }
        if arrival == "waiting" {
            scheduler.dependencies_changed(|unit| unit == &demanded);
        } else {
            // The real owner resumes Runnable work without a second caller.
            scheduler.enqueue(demanded.clone(), false).unwrap();
        }
        let (cold_id, mut cold) = scheduler
            .next()
            .expect("background receives its alternating turn");
        assert_ne!(cold.unit, demanded);
        cold.state = ObservedState::VerifiedCurrent;
        scheduler.finish(&cold_id, cold);
        let (id, mut resumed) = scheduler.next().unwrap();
        assert_eq!(
            resumed.unit, demanded,
            "{arrival}: continuation must retain caller priority"
        );
        resumed.state = ObservedState::VerifiedCurrent;
        scheduler.finish(&id, resumed);
        assert!(
            scheduler.next().is_some(),
            "cold inventory still advances after demand settles"
        );
    }
}
#[test]
fn waiting_for_a_dependency_has_no_periodic_retry() {
    let mut scheduler = Scheduler::new();
    let key = UnitKey {
        relative_root: "dfs/.content-retained/original".into(),
        role: UnitRole::RetainedArtifact,
    };
    scheduler.enqueue(key.clone(), false).unwrap();
    let (id, mut reference) = scheduler.next().unwrap();
    reference.state = ObservedState::Waiting {
        evidence: "original source awaits its owner".into(),
    };
    scheduler.finish(&id, reference);
    assert!(
        scheduler.next_retry().is_none(),
        "waiting is not a periodic maintenance job"
    );
    scheduler.enqueue(key, true).unwrap();
    assert!(
        scheduler.next().is_none(),
        "ordinary demand cannot manufacture a changed dependency"
    );
}

#[test]
fn repeated_pending_demand_keeps_waiting_but_native_progress_wakes_it() {
    let mut scheduler = Scheduler::new();
    let key = UnitKey {
        relative_root: "dfs/metadata/root".into(),
        role: UnitRole::Metadata {
            shard: sinomo_dfs::DfsMetadataShardId::new("root"),
        },
    };
    scheduler.enqueue(key.clone(), true).unwrap();
    let (id, mut reference) = scheduler.next().unwrap();
    reference.state = ObservedState::Waiting {
        evidence: "held original source".into(),
    };
    scheduler.finish(&id, reference);
    for _ in 0..8 {
        scheduler.enqueue(key.clone(), true).unwrap();
        assert!(scheduler.next_retry().is_none());
        assert!(scheduler.next().is_none());
    }
    scheduler.dependencies_changed(|unit| unit == &key);
    scheduler.enqueue(key.clone(), true).unwrap();
    assert_eq!(scheduler.next().unwrap().1.unit, key);
}

#[test]
fn failed_operation_backoff_and_inflight_dependency_are_not_lost() {
    let mut scheduler = Scheduler::new();
    let key = UnitKey {
        relative_root: "dfs/metadata/root".into(),
        role: UnitRole::Metadata {
            shard: sinomo_dfs::DfsMetadataShardId::new("root"),
        },
    };
    scheduler.enqueue(key.clone(), true).unwrap();
    let (id, mut reference) = scheduler.next().unwrap();
    reference.state = ObservedState::Waiting {
        evidence: "native read timed out".into(),
    };
    assert!(!scheduler.finish(&id, reference));
    scheduler.retry_failed(&id);
    let retry = scheduler.next_retry().unwrap();
    for _ in 0..8 {
        scheduler.enqueue(key.clone(), true).unwrap();
        scheduler.dependencies_changed(|_| false);
        assert_eq!(scheduler.next_retry(), Some(retry));
        assert!(scheduler.next().is_none());
    }
    scheduler.dependencies_changed(|unit| unit == &key);
    let (id, mut reference) = scheduler.next().unwrap();
    scheduler.dependencies_changed(|unit| unit == &key);
    reference.state = ObservedState::Waiting {
        evidence: "observation preceded the dependency edge".into(),
    };
    assert!(!scheduler.finish(&id, reference));
    let (id, mut reference) = scheduler
        .next()
        .expect("in-flight dependency must survive parking");
    reference.state = ObservedState::VerifiedCurrent;
    assert!(scheduler.finish(&id, reference.clone()));
    assert!(
        !scheduler.finish(&id, reference),
        "unchanged readiness must not wake reconciliation"
    );
    assert!(scheduler.next().is_none());
    assert!(scheduler.next_retry().is_none());
}

#[test]
fn replayed_inventory_does_not_requeue_settled_units_ahead_of_cold_work() {
    let mut scheduler = Scheduler::new();
    let mut settled = Vec::new();
    for n in 0..32 {
        let key = UnitKey {
            relative_root: "dfs/content".into(),
            role: UnitRole::ContentVersion {
                object_id: "warm".into(),
                version: n.to_string(),
            },
        };
        scheduler.enqueue(key.clone(), false).unwrap();
        let (id, mut actual) = scheduler.next().unwrap();
        actual.state = ObservedState::VerifiedCurrent;
        scheduler.finish(&id, actual.clone());
        settled.push(actual);
    }
    let cold = UnitKey {
        relative_root: "dfs/content".into(),
        role: UnitRole::ContentInventory,
    };
    scheduler.enqueue(cold.clone(), false).unwrap();
    for _ in 0..4 {
        for reference in &settled { scheduler.recovered(reference.clone()).unwrap(); }
    }
    let (id, selected) = scheduler.next().unwrap();
    assert_eq!(selected.unit, cold);
    scheduler.finish(
        &id,
        UnitReference {
            state: ObservedState::VerifiedCurrent,
            ..selected
        },
    );
    assert!(
        scheduler.next().is_none(),
        "rediscovery created repeat work for this live owner"
    );
    // A genuinely new owner still revalidates persisted observations.
    let mut reopened = Scheduler::new();
    reopened.recovered(settled.pop().unwrap()).unwrap();
    assert!(reopened.next().is_some());
}

#[test]
fn hard_failure_is_not_retried_by_background_dependencies_but_restart_revalidates() {
    let mut scheduler = Scheduler::new();
    let key = UnitKey {
        relative_root: "dfs/metadata".into(),
        role: UnitRole::FileSystem {
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            source_catalog_generation: 0,
            target_format: 12,
        },
    };
    scheduler.enqueue(key.clone(), false).unwrap();
    let (id, mut reference) = scheduler.next().unwrap();
    reference.state = ObservedState::Blocked { evidence: "invalid original proof".into() };
    scheduler.finish(&id, reference.clone());
    for _ in 0..3 {
        scheduler.recovered(reference.clone()).unwrap();
        scheduler.enqueue(key.clone(), false).unwrap();
        scheduler.enqueue(key.clone(), true).unwrap();
        scheduler.dependencies_changed(|_| true);
        assert!(scheduler.next().is_none(), "ordinary dependent work cannot retry an unchanged hard failure");
        assert!(scheduler.next_retry().is_none());
    }
    let mut restarted = Scheduler::new();
    restarted.recovered(reference.clone()).unwrap();
    assert_eq!(restarted.next().unwrap().1, reference, "restart revalidates the original retained unit once");
}
