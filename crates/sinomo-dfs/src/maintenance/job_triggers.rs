//! What requests a namespace jobs turn on a node besides its jobs' own
//! events, and the turn's request.
//!
//! A node's maintenance resumes the Running jobs of the shards its
//! configuration names it primary of. Two configuration events request a
//! jobs turn for named shards, so a job that can run here does not wait for
//! unrelated work:
//!
//! - A publication that names this node primary of a shard at an epoch it
//!   did not hold before requests the turn for that shard: a successor
//!   resumes the jobs its copy holds (see `watch_dfs_primary_assignments` in
//!   the runtime host).
//! - The next publication after a pass that could not read the
//!   configuration, and so resumed nothing, requests the turn for every shard
//!   that publication names this node primary of. The pass records that
//!   here; the publication takes the record.
//!
//! A jobs turn runs only the jobs pass, over the requested shards, under the
//! node's maintenance admission; it runs no other maintenance phase. The
//! request stays recorded until a pass over its shards runs to its end:
//!
//! - A pass that ends at its job bound after completing a job requests the
//!   turn again at once.
//! - A pass cut short by foreground work leaves the request, and the turn
//!   runs again once that work's admission is released.
//! - A pass that stops at a job's remote phase requests the turn again once
//!   that phase completes; a phase that fails ends the job's continuation by
//!   its own rule, and the turn's remaining shards wait for their next
//!   request.
//!
//! The records live in memory. After a restart the startup maintenance pass
//! reads the configuration again.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use sinomo_core::node::NodeId;

use crate::DfsMetadataShardId;

static UNREADABLE: Mutex<BTreeSet<NodeId>> = Mutex::new(BTreeSet::new());

type Wake = Arc<dyn Fn() + Send + Sync>;

#[derive(Default)]
struct Turns {
    /// The shards each node's next jobs turn runs over, by data root.
    requested: BTreeMap<PathBuf, BTreeSet<DfsMetadataShardId>>,
    /// The shards of a turn that stopped at a job's remote phase.
    continuing: BTreeMap<PathBuf, BTreeSet<DfsMetadataShardId>>,
    /// The maintenance loop's edge for each data root's jobs turn.
    wakes: BTreeMap<PathBuf, Wake>,
}

static TURNS: Mutex<Option<Turns>> = Mutex::new(None);

fn with_turns<R>(f: impl FnOnce(&mut Turns) -> R) -> R {
    let mut turns = TURNS.lock().unwrap_or_else(|error| error.into_inner());
    f(turns.get_or_insert_with(Turns::default))
}

/// A jobs pass on `node` could not read its configuration.
pub(crate) fn note_configuration_unreadable(node: &NodeId) {
    UNREADABLE
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(node.clone());
}

/// Take `node`'s record of a pass that could not read its configuration.
/// Returns whether there was one.
pub(crate) fn take_configuration_unreadable(node: &NodeId) -> bool {
    UNREADABLE
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(node)
}

/// Request a jobs turn over `shards` on the node at `root`, and wake its
/// maintenance loop's jobs turn.
pub(crate) fn request_jobs_turn(root: &Path, shards: impl IntoIterator<Item = DfsMetadataShardId>) {
    let shards = shards.into_iter().collect::<BTreeSet<_>>();
    if shards.is_empty() {
        return;
    }
    let wake = with_turns(|turns| {
        turns
            .requested
            .entry(root.to_path_buf())
            .or_default()
            .extend(shards);
        turns.wakes.get(root).cloned()
    });
    if let Some(wake) = wake {
        wake();
    }
}

/// Whether the node at `root` has a jobs turn requested.
pub(crate) fn jobs_turn_requested(root: &Path) -> bool {
    with_turns(|turns| turns.requested.contains_key(root))
}

/// Wake the jobs turn of the node at `root` if one is requested.
pub(crate) fn wake_requested_jobs_turn(root: &Path) {
    let wake = with_turns(|turns| {
        turns
            .requested
            .contains_key(root)
            .then(|| turns.wakes.get(root).cloned())
            .flatten()
    });
    if let Some(wake) = wake {
        wake();
    }
}

/// The requested shards of the node at `root`, taken for a pass. Dropping
/// the claim records them again unless the pass settles it.
pub(crate) fn take_requested_jobs_turn(root: &Path) -> DfsJobsTurnClaim {
    let shards = with_turns(|turns| turns.requested.remove(root).unwrap_or_default());
    DfsJobsTurnClaim {
        root: root.to_path_buf(),
        shards: Some(shards),
    }
}

/// The shards one jobs pass runs over, taken from their request.
#[derive(Debug)]
pub(crate) struct DfsJobsTurnClaim {
    root: PathBuf,
    shards: Option<BTreeSet<DfsMetadataShardId>>,
}

impl DfsJobsTurnClaim {
    pub(crate) fn shards(&self) -> &BTreeSet<DfsMetadataShardId> {
        self.shards.as_ref().expect("an unsettled claim")
    }

    /// The pass ran to its end.
    pub(crate) fn complete(mut self) {
        self.shards = None;
    }

    /// The pass ended at its job bound after completing a job: request the
    /// turn again at once.
    pub(crate) fn continue_now(mut self) {
        let shards = self.shards.take().unwrap_or_default();
        request_jobs_turn(&self.root, shards);
    }

    /// The pass stopped at a job's remote phase: request the turn again once
    /// that phase completes.
    pub(crate) fn continue_after_remote_phase(mut self) {
        let shards = self.shards.take().unwrap_or_default();
        with_turns(|turns| {
            turns
                .continuing
                .entry(self.root.clone())
                .or_default()
                .extend(shards)
        });
    }
}

impl Drop for DfsJobsTurnClaim {
    fn drop(&mut self) {
        // A pass cut short leaves its request; the admission release that
        // ends the foreground work wakes the turn again.
        if let Some(shards) = self.shards.take()
            && !shards.is_empty()
        {
            with_turns(|turns| {
                turns
                    .requested
                    .entry(self.root.clone())
                    .or_default()
                    .extend(shards)
            });
        }
    }
}

/// The remote phase a jobs pass on the node at `root` stopped at completed
/// (`completed`) or failed: the pass's shards are requested again, or left
/// for their next request.
pub(crate) fn remote_phase_ended(root: &Path, completed: bool) {
    let shards = with_turns(|turns| turns.continuing.remove(root));
    if completed && let Some(shards) = shards {
        request_jobs_turn(root, shards);
    }
}

/// The maintenance loop's edge for the jobs turn of the node at `root`.
/// Registration wakes it for a turn requested before it.
pub(crate) fn register_jobs_turn_wake(
    root: &Path,
    wake: impl Fn() + Send + Sync + 'static,
) -> DfsJobsTurnWakeRegistration {
    let wake: Wake = Arc::new(wake);
    let pending = with_turns(|turns| {
        turns.wakes.insert(root.to_path_buf(), Arc::clone(&wake));
        turns.requested.contains_key(root)
    });
    if pending {
        wake();
    }
    DfsJobsTurnWakeRegistration {
        root: root.to_path_buf(),
        wake,
    }
}

/// Unregisters its jobs-turn wake when dropped.
pub(crate) struct DfsJobsTurnWakeRegistration {
    root: PathBuf,
    wake: Wake,
}

impl Drop for DfsJobsTurnWakeRegistration {
    fn drop(&mut self) {
        with_turns(|turns| {
            if turns
                .wakes
                .get(&self.root)
                .is_some_and(|current| Arc::ptr_eq(current, &self.wake))
            {
                turns.wakes.remove(&self.root);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn a_publication_takes_an_unreadable_record_once() {
        let node = NodeId::from_legacy_named("job-triggers-unreadable").unwrap();
        let other = NodeId::from_legacy_named("job-triggers-other").unwrap();
        assert!(!take_configuration_unreadable(&node));
        note_configuration_unreadable(&node);
        assert!(
            !take_configuration_unreadable(&other),
            "the record is the node's"
        );
        assert!(take_configuration_unreadable(&node));
        assert!(!take_configuration_unreadable(&node));
    }

    fn shard(id: &str) -> DfsMetadataShardId {
        DfsMetadataShardId::new(id)
    }

    #[test]
    fn a_requested_turn_wakes_and_stays_until_its_pass_settles_it() {
        let root = PathBuf::from("/nonexistent/job-triggers-turn");
        request_jobs_turn(&root, [shard("users")]);
        let wakes = Arc::new(AtomicUsize::new(0));
        let _registration = register_jobs_turn_wake(&root, {
            let wakes = Arc::clone(&wakes);
            move || {
                wakes.fetch_add(1, Ordering::SeqCst);
            }
        });
        assert_eq!(wakes.load(Ordering::SeqCst), 1, "an earlier request");
        request_jobs_turn(&root, [shard("services")]);
        assert_eq!(wakes.load(Ordering::SeqCst), 2);

        // A pass cut short leaves the request.
        let claim = take_requested_jobs_turn(&root);
        assert_eq!(claim.shards().len(), 2);
        assert!(!jobs_turn_requested(&root));
        drop(claim);
        assert!(jobs_turn_requested(&root));

        // One that stops at a remote phase runs again when it completes.
        take_requested_jobs_turn(&root).continue_after_remote_phase();
        assert!(!jobs_turn_requested(&root));
        remote_phase_ended(&root, true);
        assert!(jobs_turn_requested(&root));
        assert_eq!(wakes.load(Ordering::SeqCst), 3);
        take_requested_jobs_turn(&root).continue_after_remote_phase();
        remote_phase_ended(&root, false);
        assert!(!jobs_turn_requested(&root), "a failed phase's own rule");

        // One at its bound runs again at once; one that ends settles it.
        request_jobs_turn(&root, [shard("users")]);
        take_requested_jobs_turn(&root).continue_now();
        assert!(jobs_turn_requested(&root));
        take_requested_jobs_turn(&root).complete();
        assert!(!jobs_turn_requested(&root));
        wake_requested_jobs_turn(&root);
        assert_eq!(wakes.load(Ordering::SeqCst), 5);
    }
}
