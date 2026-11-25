//! Reads that maintenance work hands to its host instead of waiting for them
//! while it holds admission.
//!
//! A job step, or a pending request that maintenance recovers, that reads
//! another node's shard from that shard's primary yields the read. The host
//! sends it once the turn has released admission, and the read becomes its
//! owner's pending item: the owner does not run again while the read is on
//! its way. The answer's arrival wakes a turn, which runs the owner first with
//! the answer in place of the send.
//!
//! A read that ends without an answer, on the network's verdict that its
//! primary has no path or on a broken call, parks its owner. The owner stays
//! quiet until one of the events that make the read worth sending again: the
//! path to that primary comes back, an examination of the primary concluded
//! after the read ended finds a path, or the shard's record moves off the
//! epoch and primary the read was sent under. The event wakes a turn, which
//! runs the owner first; its run sends the read again.
//!
//! This state is in memory only. After a restart, the owner's next run sends
//! its read again.

use sinomo_core::node::NodeId;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub(crate) type DfsHandedOffReadAnswer = std::result::Result<
    crate::source_adoption::CompletedRemoteTargetNamespacePhase,
    crate::DfsError,
>;

/// The work a handed-off read belongs to, which runs again with its answer.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum DfsReadOwner {
    Job(String),
    Request {
        requester_node_id: NodeId,
        request_id: String,
    },
}

impl DfsReadOwner {
    pub(crate) fn request(requester_node_id: &NodeId, request_id: &str) -> Self {
        Self::Request {
            requester_node_id: requester_node_id.clone(),
            request_id: request_id.to_owned(),
        }
    }

    /// The owner part of a read key: the same on every run of its owner.
    pub(crate) fn key_owner(&self) -> String {
        match self {
            Self::Job(job_id) => format!("job:{job_id}"),
            Self::Request {
                requester_node_id,
                request_id,
            } => format!("{requester_node_id}/{request_id}"),
        }
    }
}

#[derive(Default)]
struct OwnerReads {
    /// The key of the read on its way to its primary.
    sent: Option<String>,
    /// The read ended without an answer: the owner waits for an event that
    /// makes it worth sending again. Notifying this ends that wait.
    parked: Option<Arc<tokio::sync::Notify>>,
    /// The owner is to run: an answer arrived that it has not yet run with,
    /// or the event its parked read waited for came.
    due: bool,
    /// The answers the owner's next runs take in place of their sends.
    answers: BTreeMap<String, DfsHandedOffReadAnswer>,
}

type RootReads = BTreeMap<DfsReadOwner, OwnerReads>;

static HANDED_OFF_READS: Mutex<BTreeMap<PathBuf, RootReads>> = Mutex::new(BTreeMap::new());

fn with_root<R>(root: &Path, operation: impl FnOnce(&mut RootReads) -> R) -> R {
    let mut roots = HANDED_OFF_READS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let owners = roots.entry(root.to_path_buf()).or_default();
    let result = operation(owners);
    owners.retain(|_, reads| {
        reads.sent.is_some() || reads.parked.is_some() || reads.due || !reads.answers.is_empty()
    });
    if owners.is_empty() {
        roots.remove(root);
    }
    result
}

/// Whether `owner` waits for its handed-off read: the read is on its way, or
/// it ended unanswered and the event it waits for has not come.
pub(crate) fn waiting(root: &Path, owner: &DfsReadOwner) -> bool {
    with_root(root, |owners| {
        owners
            .get(owner)
            .is_some_and(|reads| reads.sent.is_some() || reads.parked.is_some())
    })
}

/// Whether `owner`'s read ended unanswered and the event it waits for has not
/// come.
#[cfg(test)]
pub(crate) fn parked(root: &Path, owner: &DfsReadOwner) -> bool {
    with_root(root, |owners| {
        owners
            .get(owner)
            .is_some_and(|reads| reads.parked.is_some())
    })
}

/// The owners to run: an answer arrived, or a parked read's event came,
/// since they last ran.
pub(crate) fn due_owners(root: &Path) -> Vec<DfsReadOwner> {
    with_root(root, |owners| {
        owners
            .iter()
            .filter(|(_, reads)| reads.due)
            .map(|(owner, _)| owner.clone())
            .collect()
    })
}

/// How many owners are to run.
pub(crate) fn due_count(root: &Path) -> usize {
    with_root(root, |owners| {
        owners.values().filter(|reads| reads.due).count()
    })
}

/// The answers `owner`'s run takes in place of its sends. The owner is no
/// longer due: it runs now.
pub(crate) fn answers_for_run(
    root: &Path,
    owner: &DfsReadOwner,
) -> Vec<(String, DfsHandedOffReadAnswer)> {
    with_root(root, |owners| {
        owners.get_mut(owner).map_or_else(Vec::new, |reads| {
            reads.due = false;
            reads
                .answers
                .iter()
                .map(|(key, answer)| (key.clone(), answer.clone()))
                .collect()
        })
    })
}

/// Record that the host sends `key` for `owner`. Returns false when the owner
/// already waits for a read.
pub(crate) fn begin_send(root: &Path, owner: &DfsReadOwner, key: &str) -> bool {
    with_root(root, |owners| {
        let reads = owners.entry(owner.clone()).or_default();
        if reads.sent.is_some() || reads.parked.is_some() {
            return false;
        }
        reads.sent = Some(key.to_owned());
        true
    })
}

/// The send of `key` for `owner` ended. An answer is kept for the owner's
/// next run and makes the owner due. Without one, nothing is kept and the
/// owner's next run sends the read again; this is how a send its host
/// abandons ends. Returns whether the owner became due.
pub(crate) fn end_send(
    root: &Path,
    owner: &DfsReadOwner,
    key: &str,
    answer: Option<DfsHandedOffReadAnswer>,
) -> bool {
    with_root(root, |owners| {
        let Some(reads) = owners.get_mut(owner) else {
            return false;
        };
        if reads.sent.as_deref() != Some(key) {
            return false;
        }
        reads.sent = None;
        let Some(answer) = answer else {
            return false;
        };
        reads.answers.insert(key.to_owned(), answer);
        reads.due = true;
        true
    })
}

/// The send of `key` for `owner` ended without an answer: park the owner
/// until an event makes the read worth sending again. Returns what ends the
/// wait for that event, `None` when the send is no longer the owner's.
pub(crate) fn park(
    root: &Path,
    owner: &DfsReadOwner,
    key: &str,
) -> Option<Arc<tokio::sync::Notify>> {
    with_root(root, |owners| {
        let reads = owners.get_mut(owner)?;
        if reads.sent.as_deref() != Some(key) {
            return None;
        }
        reads.sent = None;
        let stop = Arc::new(tokio::sync::Notify::new());
        reads.parked = Some(Arc::clone(&stop));
        Some(stop)
    })
}

/// End `owner`'s park on `stop`. When the event it waited for came, `due`
/// makes the owner run, and its run sends the read again; a wait its host
/// abandons leaves the owner to send it again when it next runs. Returns
/// false when the owner was no longer parked on `stop`.
pub(crate) fn unpark(
    root: &Path,
    owner: &DfsReadOwner,
    stop: &Arc<tokio::sync::Notify>,
    due: bool,
) -> bool {
    with_root(root, |owners| {
        let Some(reads) = owners.get_mut(owner) else {
            return false;
        };
        if !reads
            .parked
            .as_ref()
            .is_some_and(|parked| Arc::ptr_eq(parked, stop))
        {
            return false;
        }
        reads.parked = None;
        reads.due |= due;
        true
    })
}

/// `owner`'s run was cancelled before it finished: it is to run again, first,
/// with the answers it has. Its host wakes a turn for it once the admission
/// the cancelling work took is released.
pub(crate) fn resume(root: &Path, owner: &DfsReadOwner) {
    with_root(root, |owners| {
        owners.entry(owner.clone()).or_default().due = true;
    });
}

/// Forget `owner`'s answers and any parked read: its run ended without
/// handing off a read, or it no longer runs here.
pub(crate) fn forget(root: &Path, owner: &DfsReadOwner) {
    with_root(root, |owners| {
        if let Some(reads) = owners.get_mut(owner) {
            reads.due = false;
            reads.answers.clear();
            if let Some(stop) = reads.parked.take() {
                stop.notify_one();
            }
        }
    });
}

impl<T> crate::NetworkedDfsService<T> {
    /// Run `run` as `owner`'s run under a maintenance host that sends the
    /// reads it hands off. The run takes the answers its earlier reads
    /// brought back. Returns whether it handed a read off: that read is then
    /// the owner's pending item and its run changed nothing after it.
    pub(crate) fn run_with_handed_off_reads<R>(
        &mut self,
        root: &Path,
        owner: DfsReadOwner,
        run: impl FnOnce(&mut Self) -> R,
    ) -> (R, bool) {
        let answers = answers_for_run(root, &owner);
        let keys = answers
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        self.answered_remote_reads.extend(answers);
        let result = run(self);
        for key in keys {
            self.answered_remote_reads.remove(&key);
        }
        if matches!(
            self.remote_namespace_phase.as_deref(),
            Some(crate::source_adoption::RemoteNamespacePhase::ReadAtPrimary(
                _
            ))
        ) {
            let phase = self
                .remote_namespace_phase
                .take()
                .expect("handed-off read checked above");
            self.read_handoff.reads.push((owner, phase));
            return (result, true);
        }
        forget(root, &owner);
        (result, false)
    }

    /// The data root whose maintenance host sends this service's handed-off
    /// reads, when it has one.
    pub(crate) fn handed_off_reads_root(&self) -> Option<PathBuf> {
        if !self.read_handoff.maintenance_sends {
            return None;
        }
        self.publication_execution
            .current()
            .map(|owners| owners.data_root().to_path_buf())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answer() -> DfsHandedOffReadAnswer {
        Err(crate::DfsError::Metadata {
            message: "answer".into(),
        })
    }

    #[test]
    fn a_handed_off_read_is_sent_once_and_its_answer_makes_its_owner_due_once() {
        let root = Path::new("/read-handoff-test/one");
        let owner = DfsReadOwner::Job("job".into());
        assert!(begin_send(root, &owner, "key"));
        assert!(waiting(root, &owner));
        assert!(!begin_send(root, &owner, "other"));
        assert!(due_owners(root).is_empty());
        assert!(!end_send(root, &owner, "other", Some(answer())));
        assert!(end_send(root, &owner, "key", Some(answer())));
        assert!(!waiting(root, &owner));
        assert_eq!(due_owners(root), vec![owner.clone()]);
        assert_eq!(due_count(root), 1);
        let answers = answers_for_run(root, &owner);
        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0].0, "key");
        assert_eq!(due_count(root), 0);
        // The answer stays for the owner's later runs until one ends without
        // handing off a read.
        assert_eq!(answers_for_run(root, &owner).len(), 1);
        forget(root, &owner);
        assert!(answers_for_run(root, &owner).is_empty());
    }

    #[test]
    fn an_abandoned_send_leaves_nothing() {
        let root = Path::new("/read-handoff-test/two");
        let owner = DfsReadOwner::request(
            &NodeId::from_legacy_named("requester").expect("requester id"),
            "request",
        );
        assert!(begin_send(root, &owner, "key"));
        assert!(!end_send(root, &owner, "key", None));
        assert!(!waiting(root, &owner));
        assert_eq!(due_count(root), 0);
        assert!(answers_for_run(root, &owner).is_empty());
    }

    #[test]
    fn an_unanswered_read_parks_its_owner_until_its_event() {
        let root = Path::new("/read-handoff-test/three");
        let owner = DfsReadOwner::Job("job".into());
        assert!(begin_send(root, &owner, "key"));
        assert!(park(root, &owner, "other").is_none());
        let stop = park(root, &owner, "key").expect("the send was the owner's");
        assert!(waiting(root, &owner), "a parked owner stays quiet");
        assert!(!begin_send(root, &owner, "key"));
        assert_eq!(due_count(root), 0);
        let stale = Arc::new(tokio::sync::Notify::new());
        assert!(!unpark(root, &owner, &stale, true));
        assert!(unpark(root, &owner, &stop, true));
        assert!(!waiting(root, &owner));
        assert_eq!(due_owners(root), vec![owner.clone()]);
        assert!(answers_for_run(root, &owner).is_empty());
        assert_eq!(due_count(root), 0);

        // A wait its host abandons leaves the owner to run when it next does.
        assert!(begin_send(root, &owner, "key"));
        let stop = park(root, &owner, "key").expect("the send was the owner's");
        assert!(unpark(root, &owner, &stop, false));
        assert!(!waiting(root, &owner));
        assert_eq!(due_count(root), 0);

        // Forgetting a parked owner ends its wait.
        assert!(begin_send(root, &owner, "key"));
        let stop = park(root, &owner, "key").expect("the send was the owner's");
        forget(root, &owner);
        assert!(!waiting(root, &owner));
        assert!(!unpark(root, &owner, &stop, true));
        let ended = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(std::time::Duration::from_secs(1), stop.notified()).await
            });
        assert!(ended.is_ok(), "forgetting the owner ended the wait");
    }
}
