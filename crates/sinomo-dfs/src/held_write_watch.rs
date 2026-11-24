//! The events that send a held routed write again under its id.
//!
//! A routed write its primary answered `Pending` waits for the primary's
//! answer, which the primary sends when the write commits; nothing sends the
//! write again on a timer. Two events say that answer may never arrive:
//!
//! - this node's verdict for the primary changes back to a path after the
//!   primary could not be reached, so an answer the primary queued for this
//!   node may be lost, or the primary may have restarted; and
//! - Main announces an answer to the request that no attached owner took
//!   (`dfs.write.answer_unclaimed`): a requester outside Main registers no
//!   response owner there, and a registered owner may not be attached.
//!
//! On either, the requester runs the call again. An answer Main kept for its
//! owner comes with the owner's registration and is read first; otherwise
//! the requester sends the same write again under the same id, and the
//! primary, or the member that took over from it, answers from the write's
//! row inline: a committed write from its receipt. A requester watches only
//! the writes it holds, so an event for any other request is ignored, and an
//! answer settles the write, so the event sends it once.

use crate::service_state::DfsCancellationWaker;
use crate::{DfsMessageTransport, NetworkedDfsService};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Main's announcements of answers no attached response owner took, as a
/// mounted client hears them. Holding the returned value keeps `wake` called
/// for announcements of `request_id`.
pub(crate) trait DfsUnclaimedAnswerSource: Send + Sync + std::fmt::Debug {
    fn watch(
        &self,
        request_id: &str,
        object_id: &crate::DfsObjectId,
        wake: std::sync::Weak<dyn Fn() + Send + Sync>,
    ) -> Box<dyn std::any::Any + Send>;
}

/// Asks one held write to be sent again, and ends the wait it is in.
#[derive(Default)]
pub(crate) struct DfsHeldWriteWake {
    due: AtomicBool,
    waker: Mutex<Option<DfsCancellationWaker>>,
}

impl std::fmt::Debug for DfsHeldWriteWake {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsHeldWriteWake")
            .field("due", &self.is_due())
            .finish_non_exhaustive()
    }
}

impl DfsHeldWriteWake {
    /// Ask for the write to be sent again, and end its wait.
    pub(crate) fn fire(&self) {
        self.due.store(true, Ordering::Release);
        let waker = self
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(waker) = waker {
            waker();
        }
    }

    pub(crate) fn is_due(&self) -> bool {
        self.due.load(Ordering::Acquire)
    }

    /// Whether the write is due to be sent again; the next fire asks again.
    pub(crate) fn take_due(&self) -> bool {
        self.due.swap(false, Ordering::AcqRel)
    }

    /// End `waker`'s wait on a fire, and at once if one is already due.
    pub(crate) fn set_waker(&self, waker: Option<DfsCancellationWaker>) {
        *self
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = waker.clone();
        if let Some(waker) = waker
            && self.is_due()
        {
            waker();
        }
    }
}

/// What watches one held write, kept while its call holds the write.
pub(crate) struct DfsHeldWriteWatch {
    pub(crate) wake: Arc<DfsHeldWriteWake>,
    /// What the event sources hold weakly: dropping the watch ends them.
    _fire: Arc<dyn Fn() + Send + Sync>,
    _path_back: Option<crate::runtime_host::DfsPeerPathBackWatch>,
    _answers: Option<Box<dyn std::any::Any + Send>>,
}

impl std::fmt::Debug for DfsHeldWriteWatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsHeldWriteWatch")
            .field("wake", &self.wake)
            .field("path_back", &self._path_back.is_some())
            .field("answers", &self._answers.is_some())
            .finish()
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Watch the events that ask the held routed write `request_id` to be
    /// sent again (see this module). `None` when the write is not held.
    pub(crate) fn watch_held_routed_write(&self, request_id: &str) -> Option<DfsHeldWriteWatch> {
        let pending = self.writes.pending_routed_writes.get(request_id)?;
        let primary = pending.authority_shard.primary_node_id.clone();
        let object_id = pending.object_id.clone();
        let wake = Arc::new(DfsHeldWriteWake::default());
        let fired = Arc::downgrade(&wake);
        let fire: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            if let Some(wake) = fired.upgrade() {
                wake.fire();
            }
        });
        let path_back = self
            .node_data_dir
            .as_ref()
            .filter(|_| primary != self.node_id)
            .and_then(|data_dir| {
                crate::runtime_host::watch_peer_path_back(data_dir, primary, Arc::downgrade(&fire))
            });
        let answers = self
            .unclaimed_answers
            .as_ref()
            .map(|source| source.watch(request_id, &object_id, Arc::downgrade(&fire)));
        Some(DfsHeldWriteWatch {
            wake,
            _fire: fire,
            _path_back: path_back,
            _answers: answers,
        })
    }
}
