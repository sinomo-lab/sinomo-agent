//! Wakes a shard's parked conversion when a job of another shard that
//! touches it parks or leaves Running.
//!
//! A conversion's contribution rounds wait while a source answers with a job
//! touching the converting shard T that is not parked on T. Neither the job
//! parking nor its end changes a KV key the unit watches, so the job's
//! coordinator announces each as `dfs.namespace.job.parked` or
//! `dfs.namespace.job.completed`, naming the shards it touched
//! (`sinomo_dfs::DfsNamespaceJobEvent`). The owner listens on its node and
//! requeues the waiting `Metadata{T}` unit of each touched shard T, which
//! reads its sources again. Nothing here polls or keeps a timer; delivery is
//! best effort, and the owner's re-check (`source_job_recheck`) covers a
//! lost event.
use super::inventory::UnitRole;
use super::scheduler::Scheduler;
use super::*;

/// Requeue the waiting or running `Metadata` units of the shards `event`
/// touched. Returns whether any unit was requeued.
pub(super) fn source_job_changed(
    scheduler: &mut Scheduler,
    event: &sinomo_dfs::DfsNamespaceJobEvent,
) -> bool {
    scheduler.dependencies_changed(|unit| {
        matches!(&unit.role, UnitRole::Metadata { shard } if event.touched_shard_ids.contains(shard))
    })
}

impl MigrationOwner {
    /// Listen for the namespace job events `node` announces or receives,
    /// for the life of the owner's component scope.
    pub(super) fn listen_for_source_jobs(
        self: &Arc<Self>,
        node: &sinomo_core::node::NodeId,
        scope: &mut sinomo_module::ComponentScope,
    ) {
        let weak = Arc::downgrade(self);
        let listener = sinomo_dfs::listen_for_namespace_job_events(node, move |event| {
            if let Some(owner) = weak.upgrade() {
                let changed = owner
                    .work
                    .scheduler
                    .lock()
                    .is_ok_and(|mut scheduler| source_job_changed(&mut scheduler, event));
                if changed {
                    owner.work.wake.notify_one();
                }
            }
        });
        scope.defer(move || drop(listener));
    }
}

#[cfg(test)]
mod tests {
    use super::super::inventory::{ObservedState, UnitKey};
    use super::*;

    fn unit(shard: &str) -> UnitKey {
        UnitKey {
            relative_root: Path::new("dfs/metadata").join(shard),
            role: UnitRole::Metadata {
                shard: sinomo_dfs::DfsMetadataShardId::new(shard),
            },
        }
    }

    fn event(
        kind: sinomo_dfs::DfsNamespaceJobEventKind,
        touched: &str,
    ) -> sinomo_dfs::DfsNamespaceJobEvent {
        sinomo_dfs::DfsNamespaceJobEvent {
            kind,
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            coordinator_shard_id: sinomo_dfs::DfsMetadataShardId::new("services"),
            job_id: "services-directory-move".into(),
            status: match kind {
                sinomo_dfs::DfsNamespaceJobEventKind::Parked => {
                    sinomo_dfs::DfsNamespaceJobStatus::Running
                }
                sinomo_dfs::DfsNamespaceJobEventKind::Completed => {
                    sinomo_dfs::DfsNamespaceJobStatus::Completed
                }
            },
            touched_shard_ids: vec![sinomo_dfs::DfsMetadataShardId::new(touched)],
        }
    }

    /// Run the next queued unit once and leave it parked as a conversion
    /// whose rounds wait on a source's job; returns it.
    fn run_and_park(scheduler: &mut Scheduler) -> UnitKey {
        let (id, mut reference) = scheduler.next().expect("a queued unit");
        reference.state = ObservedState::Waiting {
            evidence: "jobs of shards services touch this shard".into(),
        };
        let unit = reference.unit.clone();
        scheduler.finish(&id, reference);
        unit
    }

    fn park(scheduler: &mut Scheduler, key: &UnitKey) {
        scheduler.enqueue(key.clone(), false).unwrap();
        assert_eq!(run_and_park(scheduler), *key);
    }

    /// The job's park, and then its end, each requeue the parked unit of the
    /// shard it touched, and only that unit; without the event the unit
    /// stays parked with no timer.
    #[test]
    fn a_source_jobs_park_or_end_wakes_only_the_touched_shards_parked_unit() {
        use sinomo_dfs::DfsNamespaceJobEventKind::{Completed, Parked};
        for kind in [Parked, Completed] {
            let mut scheduler = Scheduler::new();
            let (users, root) = (unit("users"), unit("root"));
            park(&mut scheduler, &users);
            park(&mut scheduler, &root);
            assert!(scheduler.next().is_none(), "both units are parked");
            assert!(
                scheduler.next_retry().is_none(),
                "a unit that asked no re-check owns no timer"
            );
            assert!(
                !source_job_changed(&mut scheduler, &event(kind, "services")),
                "no parked unit of services"
            );
            assert!(scheduler.next().is_none());
            assert!(source_job_changed(&mut scheduler, &event(kind, "users")));
            let (_, woken) = scheduler
                .next()
                .expect("the touched shard's unit is requeued");
            assert_eq!(woken.unit, users);
            assert!(scheduler.next().is_none(), "the root unit stays parked");
        }
    }

    /// A listener on the node hears the node's own events and requeues the
    /// touched shard's parked unit; once it is dropped, as the owner's scope
    /// drops it, the unit stays parked.
    #[test]
    fn a_listener_on_the_node_wakes_the_touched_shards_unit_while_registered() {
        use std::sync::atomic::Ordering::SeqCst;
        let node = sinomo_core::node::NodeId::from_legacy_named("source-job-wakes-owner").unwrap();
        let scheduler = Arc::new(Mutex::new(Scheduler::new()));
        let users = unit("users");
        park(&mut scheduler.lock().unwrap(), &users);
        let woken = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let listener = {
            let (scheduler, woken) = (Arc::clone(&scheduler), Arc::clone(&woken));
            sinomo_dfs::listen_for_namespace_job_events(&node, move |event| {
                if source_job_changed(&mut scheduler.lock().unwrap(), event) {
                    woken.fetch_add(1, SeqCst);
                }
            })
        };
        sinomo_dfs::test_support::announce_namespace_job_event(
            &node,
            event(sinomo_dfs::DfsNamespaceJobEventKind::Parked, "users"),
        );
        assert_eq!(woken.load(SeqCst), 1);
        assert_eq!(run_and_park(&mut scheduler.lock().unwrap()), users);
        drop(listener);
        sinomo_dfs::test_support::announce_namespace_job_event(
            &node,
            event(sinomo_dfs::DfsNamespaceJobEventKind::Completed, "users"),
        );
        assert_eq!(woken.load(SeqCst), 1, "no listener, no wake");
        let mut scheduler = scheduler.lock().unwrap();
        assert!(scheduler.next().is_none(), "the unit stays parked");
        assert!(
            scheduler.next_retry().is_none(),
            "a unit that asked no re-check owns no timer"
        );
    }
}
