use super::inventory::{ObservedState, UnitKey, UnitReference, UnitRole};
use super::scheduler::{OwnerWork, Scheduler};

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
        role: UnitRole::MetadataRouting,
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

fn reachability_unit(shard: &str) -> UnitKey {
    UnitKey {
        relative_root: std::path::Path::new("dfs/metadata").join(shard),
        role: UnitRole::Metadata {
            shard: sinomo_dfs::DfsMetadataShardId::new(shard),
        },
    }
}
fn reachability_nodes(names: &[&str]) -> std::collections::BTreeSet<sinomo_core::node::NodeId> {
    names
        .iter()
        .map(|name| sinomo_core::node::NodeId::from_legacy_named(*name).unwrap())
        .collect()
}
fn park_on(scheduler: &mut Scheduler, key: &UnitKey, node: &str) {
    scheduler.enqueue(key.clone(), false).unwrap();
    let (id, mut reference) = scheduler.next().unwrap();
    assert_eq!(&reference.unit, key);
    scheduler
        .watch_reachable_nodes(key, reachability_nodes(&[node]))
        .unwrap();
    reference.state = ObservedState::Waiting {
        evidence: "contributing shard has no reachable member".into(),
    };
    scheduler.finish(&id, reference);
}

#[test]
fn a_waiting_unit_wakes_only_when_a_node_it_watches_becomes_reachable() {
    let mut scheduler = Scheduler::new();
    let (on_x, on_y) = (reachability_unit("alpha"), reachability_unit("beta"));
    park_on(&mut scheduler, &on_x, "node-x");
    park_on(&mut scheduler, &on_y, "node-y");
    assert!(scheduler.next().is_none());
    assert!(scheduler.next_retry().is_none(), "a parked wait owns no timer");
    assert_eq!(scheduler.watched_nodes(), reachability_nodes(&["node-x", "node-y"]));
    assert!(!scheduler.nodes_became_reachable(&reachability_nodes(&["node-z"])));
    assert!(scheduler.next().is_none());
    assert!(scheduler.nodes_became_reachable(&reachability_nodes(&["node-x"])));
    let (id, mut woken) = scheduler.next().expect("the unit watching X is requeued");
    assert_eq!(woken.unit, on_x);
    assert!(scheduler.next().is_none(), "the unit watching Y keeps waiting");
    assert!(scheduler.next_retry().is_none());
    woken.state = ObservedState::VerifiedCurrent;
    scheduler.finish(&id, woken);
    assert_eq!(
        scheduler.watched_nodes(),
        reachability_nodes(&["node-y"]),
        "a settled unit drops its watch"
    );
}

#[test]
fn reachability_during_a_step_survives_its_park() {
    let mut scheduler = Scheduler::new();
    let key = reachability_unit("alpha");
    scheduler.enqueue(key.clone(), false).unwrap();
    let (id, mut reference) = scheduler.next().unwrap();
    // The step observed X unreachable; X became reachable before the step
    // registered its watch.
    assert!(!scheduler.nodes_became_reachable(&reachability_nodes(&["node-x"])));
    scheduler
        .watch_reachable_nodes(&key, reachability_nodes(&["node-x"]))
        .unwrap();
    reference.state = ObservedState::Waiting {
        evidence: "contributing shard has no reachable member".into(),
    };
    scheduler.finish(&id, reference);
    let (id, mut rerun) = scheduler.next().expect("the earlier change reruns the parked step");
    assert_eq!(rerun.unit, key);
    rerun.state = ObservedState::Waiting {
        evidence: "contributing shard has no reachable member".into(),
    };
    scheduler.finish(&id, rerun);
    assert!(scheduler.next().is_none(), "a later step starts with no stale change");
    assert!(scheduler
        .watch_reachable_nodes(&reachability_unit("unowned"), reachability_nodes(&["node-x"]))
        .is_err());
}

#[test]
fn registering_a_reachability_watch_wakes_the_watcher_without_a_timer() {
    let root = sinomo_testdir::TestDir::new("owner-reachability-watch");
    let work = OwnerWork::new(root.path()).unwrap();
    let key = reachability_unit("alpha");
    assert!(work
        .watch_reachable_nodes(&key, reachability_nodes(&["node-x"]))
        .is_err(), "a watch needs an existing work owner");
    work.scheduler.lock().unwrap().enqueue(key.clone(), false).unwrap();
    work.watch_reachable_nodes(&key, reachability_nodes(&["node-x"])).unwrap();
    assert_eq!(work.scheduler.lock().unwrap().watched_nodes(), reachability_nodes(&["node-x"]));
    // The watcher may not be waiting yet: the registration is a stored permit.
    let registered = std::pin::pin!(work.reachability.notified());
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(registered.poll(&mut context).is_ready());
    assert!(work.scheduler.lock().unwrap().next_retry().is_none());
}
