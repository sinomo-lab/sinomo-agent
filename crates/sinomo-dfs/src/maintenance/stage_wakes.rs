//! What sends a refused stage of a shard lane's routed write again.
//!
//! A routed write stays on the shard lane that served it until it finishes,
//! and the lane's unfinished-write phase sends its due stages again (see
//! `run_daemon_dfs_unfinished_write_phase_on_shard_lanes` in the runtime
//! host). A stage that a member refused, or that ended without an answer,
//! leaves the write short of W until a later attempt lands. Two events make
//! that attempt due:
//!
//! - this node's verdict for the member changes back to a path after the
//!   member could not be reached; and
//! - the stage's re-send timer, a fallback with the attempt's backoff and no
//!   cap on attempts.
//!
//! Either requests a writes turn: the node's maintenance runs only the lane
//! write phase, which sends the due stages again. A stage refused again is
//! recorded again with the next attempt's backoff.
//!
//! The records live in memory. After a restart a write's hydration and the
//! startup reconciliation send its stages again.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sinomo_core::local::SinomoDataDir;
use sinomo_core::node::NodeId;

/// A refused stage: the write's intent and the member it was sent to.
pub(crate) type DfsRefusedStage = (String, NodeId);

type Wake = Arc<dyn Fn() + Send + Sync>;

/// One member's path-back watch, kept while a refused stage waits on it.
struct MemberWatch {
    _on_path_back: Arc<dyn Fn() + Send + Sync>,
    _watch: Option<crate::runtime_host::DfsPeerPathBackWatch>,
}

#[derive(Default)]
struct RootWakes {
    /// When each refused stage is due again.
    due: BTreeMap<DfsRefusedStage, Instant>,
    watches: BTreeMap<NodeId, MemberWatch>,
    /// The maintenance loop's edge for this node's writes turn.
    wake: Option<Wake>,
}

static ROOTS: Mutex<BTreeMap<PathBuf, RootWakes>> = Mutex::new(BTreeMap::new());

/// Test support: how many refused stages each node recorded.
#[cfg(test)]
static NOTED: Mutex<BTreeMap<PathBuf, usize>> = Mutex::new(BTreeMap::new());

#[cfg(test)]
pub(crate) fn refused_stages_noted(root: &Path) -> usize {
    NOTED
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(root)
        .copied()
        .unwrap_or(0)
}

fn with_root<R>(root: &Path, f: impl FnOnce(&mut RootWakes) -> R) -> R {
    let mut roots = ROOTS.lock().unwrap_or_else(|error| error.into_inner());
    f(roots.entry(root.to_path_buf()).or_default())
}

/// The stage of the write `intent_id` to `member` on the node at `data_dir`
/// was refused: send it again after `delay`, or once this node's path to
/// `member` comes back.
pub(crate) fn note_stage_refused(
    data_dir: &SinomoDataDir,
    intent_id: &str,
    member: &NodeId,
    delay: Duration,
) {
    let root = data_dir.root();
    #[cfg(test)]
    {
        *NOTED
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(root.clone())
            .or_default() += 1;
    }
    let at = Instant::now()
        .checked_add(delay)
        .unwrap_or_else(Instant::now);
    let watched = with_root(&root, |wakes| {
        wakes.due.insert((intent_id.to_owned(), member.clone()), at);
        wakes.watches.contains_key(member)
    });
    if !watched {
        let on_path_back: Arc<dyn Fn() + Send + Sync> = Arc::new({
            let root = root.clone();
            let member = member.clone();
            move || stages_to_member_due(&root, &member)
        });
        let watch = crate::runtime_host::watch_peer_path_back(
            data_dir,
            member.clone(),
            Arc::downgrade(&on_path_back),
        );
        with_root(&root, |wakes| {
            if wakes.due.keys().any(|(_, waiting)| waiting == member) {
                wakes.watches.entry(member.clone()).or_insert(MemberWatch {
                    _on_path_back: on_path_back,
                    _watch: watch,
                });
            }
        });
    }
    wake(&root);
}

/// This node's path to `member` came back: its refused stages are due now.
fn stages_to_member_due(root: &Path, member: &NodeId) {
    let now = Instant::now();
    with_root(root, |wakes| {
        for ((_, waiting), at) in wakes.due.iter_mut() {
            if waiting == member {
                *at = now;
            }
        }
    });
    wake(root);
}

fn wake(root: &Path) {
    if let Some(wake) = with_root(root, |wakes| wakes.wake.clone()) {
        wake();
    }
}

/// Wake the writes turn of the node at `root` if a refused stage is due:
/// the admission a turn gave way to was released.
pub(crate) fn wake_due_writes_turn(root: &Path) {
    if has_due(root, Instant::now()) {
        wake(root);
    }
}

/// When the first refused stage of the node at `root` is due.
pub(crate) fn next_due(root: &Path) -> Option<Instant> {
    with_root(root, |wakes| wakes.due.values().min().copied())
}

/// Whether a refused stage of the node at `root` is due at `now`.
pub(crate) fn has_due(root: &Path, now: Instant) -> bool {
    next_due(root).is_some_and(|at| at <= now)
}

/// Take the refused stages of the node at `root` that are due at `now`. A
/// member none of the remaining stages waits on is no longer watched.
pub(crate) fn take_due(root: &Path, now: Instant) -> BTreeSet<DfsRefusedStage> {
    with_root(root, |wakes| {
        let due = wakes
            .due
            .iter()
            .filter(|(_, at)| **at <= now)
            .map(|(stage, _)| stage.clone())
            .collect::<BTreeSet<_>>();
        for stage in &due {
            wakes.due.remove(stage);
        }
        let waited = wakes
            .due
            .keys()
            .map(|(_, member)| member.clone())
            .collect::<BTreeSet<_>>();
        wakes.watches.retain(|member, _| waited.contains(member));
        due
    })
}

/// The maintenance loop's edge for the writes turn of the node at `root`.
/// Registration wakes it for a stage due before it.
pub(crate) fn register_writes_turn_wake(
    root: &Path,
    wake: impl Fn() + Send + Sync + 'static,
) -> DfsWritesTurnWakeRegistration {
    let wake: Wake = Arc::new(wake);
    let pending = with_root(root, |wakes| {
        wakes.wake = Some(Arc::clone(&wake));
        !wakes.due.is_empty()
    });
    if pending {
        wake();
    }
    DfsWritesTurnWakeRegistration {
        root: root.to_path_buf(),
        wake,
    }
}

/// Unregisters its writes-turn wake when dropped.
pub(crate) struct DfsWritesTurnWakeRegistration {
    root: PathBuf,
    wake: Wake,
}

impl Drop for DfsWritesTurnWakeRegistration {
    fn drop(&mut self) {
        with_root(&self.root, |wakes| {
            if wakes
                .wake
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &self.wake))
            {
                wakes.wake = None;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn a_refused_stage_is_due_after_its_backoff_and_taken_once() {
        let data_dir = SinomoDataDir::new(PathBuf::from("/nonexistent/stage-wakes"));
        let root = data_dir.root();
        let member = NodeId::from_legacy_named("stage-wakes-member").unwrap();
        let wakes = Arc::new(AtomicUsize::new(0));
        let _registration = register_writes_turn_wake(&root, {
            let wakes = Arc::clone(&wakes);
            move || {
                wakes.fetch_add(1, Ordering::SeqCst);
            }
        });
        assert_eq!(wakes.load(Ordering::SeqCst), 0, "nothing is due yet");
        note_stage_refused(&data_dir, "intent", &member, Duration::from_secs(60));
        assert_eq!(wakes.load(Ordering::SeqCst), 1, "the loop sees the timer");
        let now = Instant::now();
        assert!(!has_due(&root, now));
        assert!(take_due(&root, now).is_empty());
        let later = now + Duration::from_secs(61);
        assert!(has_due(&root, later));
        assert_eq!(
            take_due(&root, later),
            BTreeSet::from([("intent".to_owned(), member.clone())])
        );
        assert!(next_due(&root).is_none(), "a taken stage is not due again");

        // The member's path coming back makes its stages due at once.
        note_stage_refused(&data_dir, "intent", &member, Duration::from_secs(60));
        stages_to_member_due(&root, &member);
        assert!(has_due(&root, Instant::now()));
        assert_eq!(take_due(&root, Instant::now()).len(), 1);
    }
}
