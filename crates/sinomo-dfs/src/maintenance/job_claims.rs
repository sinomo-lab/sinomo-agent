//! One driver steps a namespace job at a time, and a refused step's wake.
//!
//! A node's job has several drivers: the served request that started it
//! steps it inline, a request maintenance recovers runs it again, and the
//! jobs pass resumes it. Each takes the job's step claim on the node around a
//! step and releases it when the step ends: on success, on an error, on
//! cancellation, and when the step hands a phase to its host. A driver that
//! finds the job claimed does not step it and is not refused as busy. It waits
//! for the claim's release, an event, holding no lanes or admission, and then
//! reads the job again: a job that ended answers it, and one that still runs
//! is stepped if it still needs to be.
//!
//! - A served request hands the wait to its host and runs again once the
//!   claim is released.
//! - Maintenance leaves the job, or the request it recovers, for now and
//!   records the wait here; the release marks that owner due and wakes the
//!   node's maintenance.
//! - A caller without a host waits in place; it holds no admission.
//!
//! A job step that another move's guard or a fence on a row it writes refuses
//! as busy stays Running. Its owner waits here until a job on the node ends or
//! a fence that refused a publication is dropped; either marks it due and
//! wakes the node's maintenance.
//!
//! The records live in memory. After a restart no step holds a claim, and the
//! startup maintenance pass runs every Running job again.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

use sinomo_core::local::SinomoDataDir;
use sinomo_core::node::NodeId;

use crate::read_handoff::DfsReadOwner;

/// A maintenance owner to run again, with its node's data root.
type Waiter = (PathBuf, DfsReadOwner);

/// The claimed jobs, by node and job id, with the maintenance owners that
/// wait for each claim's release.
static CLAIMS: Mutex<BTreeMap<(NodeId, String), BTreeSet<Waiter>>> = Mutex::new(BTreeMap::new());
static RELEASED: Condvar = Condvar::new();

/// The owners whose job step a guard or fence refused as busy, by node.
static BUSY: Mutex<BTreeMap<NodeId, BTreeSet<Waiter>>> = Mutex::new(BTreeMap::new());

/// One driver's claim on a job's step. Dropping it releases the claim.
#[derive(Debug)]
pub(crate) struct DfsJobStepClaim {
    node: NodeId,
    job_id: String,
}

impl Drop for DfsJobStepClaim {
    fn drop(&mut self) {
        let waiters = CLAIMS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&(self.node.clone(), std::mem::take(&mut self.job_id)))
            .unwrap_or_default();
        RELEASED.notify_all();
        wake(waiters);
    }
}

/// Claim the step of the job `job_id` on `node`; `None` while another driver
/// holds it.
pub(crate) fn try_claim(node: &NodeId, job_id: &str) -> Option<DfsJobStepClaim> {
    let mut claims = CLAIMS.lock().unwrap_or_else(|error| error.into_inner());
    let key = (node.clone(), job_id.to_owned());
    if claims.contains_key(&key) {
        return None;
    }
    claims.insert(key, BTreeSet::new());
    Some(DfsJobStepClaim {
        node: node.clone(),
        job_id: job_id.to_owned(),
    })
}

/// Run `owner` again once the claim on the job `job_id` on `node` is
/// released. Returns false when the job is not claimed any more: the caller
/// tries its claim again.
pub(crate) fn rerun_on_release(
    node: &NodeId,
    job_id: &str,
    root: PathBuf,
    owner: DfsReadOwner,
) -> bool {
    let mut claims = CLAIMS.lock().unwrap_or_else(|error| error.into_inner());
    let Some(waiters) = claims.get_mut(&(node.clone(), job_id.to_owned())) else {
        return false;
    };
    waiters.insert((root, owner));
    true
}

/// Wait until the job `job_id` on `node` is not claimed, or `deadline`
/// passes. Returns whether it was released.
pub(crate) fn wait_for_release(node: &NodeId, job_id: &str, deadline: Option<Instant>) -> bool {
    let key = (node.clone(), job_id.to_owned());
    let mut claims = CLAIMS.lock().unwrap_or_else(|error| error.into_inner());
    while claims.contains_key(&key) {
        match deadline {
            Some(deadline) => {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    return false;
                };
                claims = RELEASED
                    .wait_timeout(claims, remaining)
                    .unwrap_or_else(|error| error.into_inner())
                    .0;
            }
            None => {
                claims = RELEASED
                    .wait(claims)
                    .unwrap_or_else(|error| error.into_inner());
            }
        }
    }
    true
}

/// `owner`'s job step on `node` was refused as busy by another move's guard
/// or a fence: it runs again when a job on the node ends or such a fence is
/// dropped.
pub(crate) fn rerun_on_busy_release(node: &NodeId, root: PathBuf, owner: DfsReadOwner) {
    BUSY.lock()
        .unwrap_or_else(|error| error.into_inner())
        .entry(node.clone())
        .or_default()
        .insert((root, owner));
}

/// A job on `node` ended: the steps it refused as busy run again.
pub(crate) fn note_busy_wait_event(node: &NodeId) {
    let waiters = BUSY
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(node)
        .unwrap_or_default();
    wake(waiters);
}

/// A fence that refused a publication was dropped. Fences are the process's,
/// so every node's steps refused as busy run again.
pub(crate) fn note_refusing_fence_dropped() {
    let waiters = std::mem::take(&mut *BUSY.lock().unwrap_or_else(|error| error.into_inner()));
    wake(waiters.into_values().flatten().collect());
}

fn wake(waiters: BTreeSet<Waiter>) {
    let mut roots = BTreeSet::new();
    for (root, owner) in waiters {
        crate::read_handoff::resume(&root, &owner);
        roots.insert(root);
    }
    for root in roots {
        crate::runtime_host::notify_daemon_dfs_maintenance_work(&SinomoDataDir::new(root));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn one_driver_holds_a_step_and_its_release_wakes_the_waiters() {
        let node = NodeId::from_legacy_named("job-claims-node").unwrap();
        let claim = try_claim(&node, "job").expect("an unclaimed job is claimed");
        assert!(try_claim(&node, "job").is_none(), "one driver at a time");
        assert!(
            try_claim(&node, "other").is_some(),
            "another job is its own"
        );
        let soon = Some(Instant::now() + Duration::from_millis(50));
        assert!(!wait_for_release(&node, "job", soon));
        let waiter = std::thread::spawn({
            let node = node.clone();
            move || wait_for_release(&node, "job", None)
        });
        drop(claim);
        assert!(waiter.join().unwrap());
        assert!(!rerun_on_release(
            &node,
            "job",
            PathBuf::from("/nonexistent"),
            DfsReadOwner::Job("job".into())
        ));
        assert!(
            try_claim(&node, "job").is_some(),
            "a released claim is free"
        );
    }

    #[test]
    fn a_maintenance_waiter_is_due_on_the_release() {
        let node = NodeId::from_legacy_named("job-claims-waiter").unwrap();
        let root = PathBuf::from("/nonexistent/job-claims-waiter");
        let owner = DfsReadOwner::Job("held".into());
        let claim = try_claim(&node, "held").unwrap();
        assert!(rerun_on_release(&node, "held", root.clone(), owner.clone()));
        assert!(!crate::read_handoff::due_owners(&root).contains(&owner));
        drop(claim);
        assert!(crate::read_handoff::due_owners(&root).contains(&owner));
        crate::read_handoff::forget(&root, &owner);
    }

    #[test]
    fn a_step_refused_as_busy_is_due_on_a_job_end_or_a_refusing_fence_drop() {
        let node = NodeId::from_legacy_named("job-claims-busy").unwrap();
        let other = NodeId::from_legacy_named("job-claims-busy-other").unwrap();
        let root = PathBuf::from("/nonexistent/job-claims-busy");
        let owner = DfsReadOwner::Job("refused".into());
        rerun_on_busy_release(&node, root.clone(), owner.clone());
        note_busy_wait_event(&other);
        assert!(
            !crate::read_handoff::due_owners(&root).contains(&owner),
            "another node's job end"
        );
        note_busy_wait_event(&node);
        assert!(crate::read_handoff::due_owners(&root).contains(&owner));
        crate::read_handoff::forget(&root, &owner);
        rerun_on_busy_release(&node, root.clone(), owner.clone());
        note_refusing_fence_dropped();
        assert!(crate::read_handoff::due_owners(&root).contains(&owner));
        crate::read_handoff::forget(&root, &owner);
    }
}
