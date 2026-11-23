//! A mounted call waits for a newer shard record without its mount's lock.
//!
//! A mount serves every syscall through one service behind one lock. A call
//! that follows its shard's record would wait for the newer record while it
//! holds that lock, and every other syscall of the mount would wait behind it.
//! Instead the call hands the wait back. The run of the adapter's operation
//! ends with a cancellation, the adapter releases its service lock and its
//! mutation admission, waits on the configuration watch until the record
//! changes, the call's re-send timer is due or its budget ends, and runs the
//! same operation again.
//!
//! The run that follows is the same call. Each namespace call a run settled
//! is answered again from its answer, in the order the runs made them: a
//! call takes the first settled answer of the same kind of call on the same
//! path after the answer the run took last, so a run that skips a call it no
//! longer needs is not answered with another call's answer. The call that
//! handed its wait back resumes where it stopped: with its operation id, its
//! body, its re-send timer, what its earlier attempts left and its own
//! budget. A write whose primary answered that it holds the write is not
//! sent again: the next run waits for the primary's answer again. Once a run
//! has sent a change this replay cannot answer again, such as a content write
//! made without its caller's id, it keeps waiting where it is.

use crate::service_record_follow::{
    DfsFollowedNamespaceRoute, DfsNamespaceFollow, DfsRecordFollowResend,
};
use crate::service_state::DfsCancellationWaker;
use crate::{
    DfsCancellationFlag, DfsConfigurationChangeWatch, DfsError, DfsFileSystemId,
    DfsMessageTransport, DfsNamespaceOperation, DfsNetworkMessage, NetworkedDfsService,
};
use std::sync::Arc;
use std::time::Instant;

/// The cancellation that ends a run whose record wait was handed back.
const DFS_MOUNTED_RECORD_WAIT_HANDED_BACK: &str =
    "mounted call: its wait for a newer shard record was handed back to the mount";

fn record_wait_handed_back() -> DfsError {
    DfsError::Cancelled {
        operation: DFS_MOUNTED_RECORD_WAIT_HANDED_BACK,
    }
}

/// Which namespace call of a run a call is. A run that takes the same path
/// makes the same kind of call on the same path, even when a value its body
/// takes at the call, such as the current time, differs.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DfsMountedCallKey {
    file_system_id: DfsFileSystemId,
    kind: std::mem::Discriminant<DfsNamespaceOperation>,
    path: Option<String>,
}

impl DfsMountedCallKey {
    fn of(file_system_id: &DfsFileSystemId, operation: &DfsNamespaceOperation) -> Self {
        Self {
            file_system_id: file_system_id.clone(),
            kind: std::mem::discriminant(operation),
            path: operation.routing_path().map(str::to_owned),
        }
    }
}

/// A namespace call whose wait for a newer record its run handed back.
#[derive(Debug)]
pub(crate) struct DfsParkedNamespaceCall {
    /// The call's operation id; a read's attempts each take their own.
    pub(crate) request_id: Option<String>,
    /// The body every attempt sends.
    pub(crate) operation: DfsNamespaceOperation,
    /// The call's own budget, from its first run.
    pub(crate) own_deadline: Option<Instant>,
    pub(crate) follow: DfsNamespaceFollow,
}

/// How a namespace call of a mounted call proceeds.
pub(crate) enum DfsMountedNamespaceCall {
    /// No mounted call is running.
    Unscoped,
    /// An earlier run settled this call.
    Answered(Result<DfsFollowedNamespaceRoute, DfsError>),
    /// This run already handed a wait back and sends nothing more.
    Stopped(DfsError),
    /// The call runs, resuming the wait an earlier run handed back.
    Live {
        resumed: Option<DfsParkedNamespaceCall>,
    },
}

/// What one mounted call keeps across its runs.
#[derive(Debug, Default)]
pub(crate) struct DfsMountedCallScope {
    /// The namespace calls the runs settled, in call order.
    settled: Vec<(
        DfsMountedCallKey,
        Result<DfsFollowedNamespaceRoute, DfsError>,
    )>,
    /// Where this run's next namespace call looks for its settled answer.
    cursor: usize,
    /// The call whose wait an earlier run handed back.
    parked: Option<(DfsMountedCallKey, DfsParkedNamespaceCall)>,
    /// The re-send timer of the probe whose wait an earlier run handed back.
    probe_resend: Option<DfsRecordFollowResend>,
    /// The wait this run handed back. The run sends nothing after it.
    handed_back: Option<MountedSifsRecordWait>,
    /// This run sent a change a later run could not answer again.
    unreplayable: bool,
    /// Calls in progress whose every change goes under its caller's id, so
    /// that a later run answers it again.
    replayable_depth: usize,
    /// The routed writes whose primary holds them and whose wait for the
    /// primary's answer a run handed back, by request id. Their bodies stay
    /// with the requester until the call ends.
    held_writes: Vec<String>,
}

/// One mounted adapter call, run until it no longer hands a record wait
/// back.
///
/// ```ignore
/// let mut replay = MountedSifsCallReplay::new();
/// loop {
///     let result = replay.run(&mut *lock_service()?, |service| operation(service));
///     match replay.take_record_wait() {
///         Some(wait) => wait.wait(),
///         None => return result,
///     }
/// }
/// ```
#[derive(Debug, Default)]
pub struct MountedSifsCallReplay {
    scope: Option<Box<DfsMountedCallScope>>,
    record_wait: Option<MountedSifsRecordWait>,
}

impl MountedSifsCallReplay {
    pub fn new() -> Self {
        Self::default()
    }

    /// Run `operation` on `service` as one run of this call. A run inside
    /// another mounted call is part of that call.
    pub fn run<T: DfsMessageTransport, R>(
        &mut self,
        service: &mut NetworkedDfsService<T>,
        operation: impl FnOnce(&mut NetworkedDfsService<T>) -> R,
    ) -> R {
        if service.mounted_call.is_some() {
            return operation(service);
        }
        // A write's handed-back wait may have left its arrival waker; the
        // run that follows reads what arrived itself.
        service.transport.set_incoming_arrival_waker(None);
        let mut scope = self.scope.take().unwrap_or_default();
        scope.cursor = 0;
        scope.unreplayable = false;
        scope.replayable_depth = 0;
        service.mounted_call = Some(scope);
        let result = operation(service);
        let mut scope = service
            .mounted_call
            .take()
            .expect("a mounted call's run keeps its scope");
        self.record_wait = scope.handed_back.take();
        if self.record_wait.is_none() {
            // The call ended: a held write it no longer waits for keeps only
            // what a late answer needs.
            for request_id in std::mem::take(&mut scope.held_writes) {
                service.compact_pending_routed_write(&request_id);
            }
        }
        self.scope = Some(scope);
        result
    }

    /// The record wait the last run handed back. The caller makes it with
    /// the mount's locks released, then runs the call again.
    pub fn take_record_wait(&mut self) -> Option<MountedSifsRecordWait> {
        self.record_wait.take()
    }
}

/// How long one wait on the configuration watch lasts while a held write
/// waits for its primary's answer with nothing due. Its end only starts the
/// same wait again: the call does not run and sends nothing.
const MOUNTED_HELD_WRITE_WATCH_SLICE: std::time::Duration = std::time::Duration::from_secs(3600);

/// A wait for a newer shard record, handed back by a mounted call's run.
pub struct MountedSifsRecordWait {
    watch: Box<dyn DfsConfigurationChangeWatch>,
    /// When the call is due to run again; `None` when nothing is due on a
    /// timer.
    until: Option<Instant>,
    /// Set when a message arrives for the mount, for a wait that has no
    /// timer.
    arrived: Option<Arc<std::sync::atomic::AtomicBool>>,
    cancellation: Option<DfsCancellationFlag>,
}

impl std::fmt::Debug for MountedSifsRecordWait {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MountedSifsRecordWait")
            .field("until", &self.until)
            .finish_non_exhaustive()
    }
}

impl MountedSifsRecordWait {
    /// Wait until the configuration changes, the call's re-send timer or
    /// budget is due, a message the wait is for arrives, or the mount is
    /// cancelled. The caller holds none of the mount's locks.
    pub fn wait(mut self) {
        let wake_watch = self.watch.waker();
        let waker: DfsCancellationWaker = Arc::new(move || wake_watch());
        let _registration = self
            .cancellation
            .as_ref()
            .map(|cancellation| cancellation.register_waker(&waker));
        if self
            .cancellation
            .as_ref()
            .is_some_and(DfsCancellationFlag::is_cancelled)
        {
            return;
        }
        let Some(until) = self.until else {
            // Nothing is due on a timer: only an arrival, a configuration
            // change or the mount's cancellation ends the wait.
            loop {
                let arrived = self
                    .arrived
                    .as_ref()
                    .is_some_and(|arrived| arrived.load(std::sync::atomic::Ordering::Acquire));
                if arrived
                    || self
                        .cancellation
                        .as_ref()
                        .is_some_and(DfsCancellationFlag::is_cancelled)
                {
                    return;
                }
                // A change, or a failed watch, ends the wait; the next run
                // reads the record.
                match self.watch.wait_for_change(MOUNTED_HELD_WRITE_WATCH_SLICE) {
                    Ok(false) => {}
                    Ok(true) | Err(_) => return,
                }
            }
        };
        let wait = until.saturating_duration_since(Instant::now());
        if !wait.is_zero() {
            // A failed watch ends the wait; the next run reads the record.
            let _ = self.watch.wait_for_change(wait);
        }
    }
}

/// Whether sending `message` makes a change a later run of a mounted call
/// could not answer again. Namespace requests are answered from their
/// settled answers or resumed under their id; reads and probes change
/// nothing.
fn message_changes_beyond_replay(message: &DfsNetworkMessage) -> bool {
    matches!(
        message,
        DfsNetworkMessage::WriteRequest(_)
            | DfsNetworkMessage::DistributedWriteRequest(_)
            | DfsNetworkMessage::ExclusiveCreateRequest(_)
            | DfsNetworkMessage::RetainedWriteRequest(_)
            | DfsNetworkMessage::StageBlob(_)
            | DfsNetworkMessage::StageRetainedBlob(_)
            | DfsNetworkMessage::RepairRetainedBlob(_)
            | DfsNetworkMessage::StagePatch(_)
            | DfsNetworkMessage::CommitVersion(_)
            | DfsNetworkMessage::RepairReplica(_)
            | DfsNetworkMessage::MetadataJournalBatch(_)
            | DfsNetworkMessage::MetadataSnapshotInstall(_)
    )
}

impl<T> NetworkedDfsService<T> {
    /// Whether the running mounted call handed a wait back: the run sends
    /// nothing more.
    pub(crate) fn mounted_call_handed_back(&self) -> bool {
        self.mounted_call
            .as_ref()
            .is_some_and(|scope| scope.handed_back.is_some())
    }

    /// The running mounted call's refusal to send once it handed a wait back.
    pub(crate) fn check_mounted_call_running(&self) -> Result<(), DfsError> {
        if self.mounted_call_handed_back() {
            return Err(record_wait_handed_back());
        }
        Ok(())
    }

    /// Note a message the running mounted call sends.
    pub(crate) fn note_mounted_call_send(&mut self, message: &DfsNetworkMessage) {
        if let Some(scope) = self.mounted_call.as_mut()
            && scope.replayable_depth == 0
            && message_changes_beyond_replay(message)
        {
            scope.unreplayable = true;
        }
    }

    /// Run `call`, whose every change goes under its caller's operation id:
    /// a later run of the mounted call makes the same call under the same
    /// id, and its primary answers that repeat with what the first attempt
    /// did. Its changes therefore do not keep the run from handing a record
    /// wait back.
    pub(crate) fn with_replayable_mounted_effects<R>(
        &mut self,
        call: impl FnOnce(&mut Self) -> R,
    ) -> R {
        if let Some(scope) = self.mounted_call.as_mut() {
            scope.replayable_depth += 1;
        }
        let result = call(self);
        if let Some(scope) = self.mounted_call.as_mut() {
            scope.replayable_depth = scope.replayable_depth.saturating_sub(1);
        }
        result
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Whether a wait for a newer record is handed back to a running mounted
    /// call instead of made here.
    pub(crate) fn mounted_call_hands_back_waits(&self) -> bool {
        self.mounted_call
            .as_ref()
            .is_some_and(|scope| !scope.unreplayable && scope.handed_back.is_none())
            && self.follows_record_changes()
    }

    /// How the namespace call the running mounted call makes now proceeds.
    pub(crate) fn mounted_namespace_call(
        &mut self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
    ) -> DfsMountedNamespaceCall {
        let Some(scope) = self.mounted_call.as_mut() else {
            return DfsMountedNamespaceCall::Unscoped;
        };
        if scope.handed_back.is_some() {
            return DfsMountedNamespaceCall::Stopped(record_wait_handed_back());
        }
        let key = DfsMountedCallKey::of(file_system_id, operation);
        if let Some(offset) = scope.settled[scope.cursor..]
            .iter()
            .position(|(settled, _)| *settled == key)
        {
            let index = scope.cursor + offset;
            scope.cursor = index + 1;
            return DfsMountedNamespaceCall::Answered(scope.settled[index].1.clone());
        }
        let resumed = match scope.parked.take() {
            Some((parked_key, parked)) if parked_key == key => Some(parked),
            other => {
                scope.parked = other;
                None
            }
        };
        DfsMountedNamespaceCall::Live { resumed }
    }

    /// Keep the answer of a namespace call that ran in this run, in its
    /// place among the answers, unless the run handed the call's wait back.
    pub(crate) fn settle_mounted_namespace_call(
        &mut self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
        answer: &Result<DfsFollowedNamespaceRoute, DfsError>,
    ) {
        let Some(scope) = self.mounted_call.as_mut() else {
            return;
        };
        if scope.handed_back.is_none() {
            let at = scope.cursor.min(scope.settled.len());
            scope.settled.insert(
                at,
                (
                    DfsMountedCallKey::of(file_system_id, operation),
                    answer.clone(),
                ),
            );
            scope.cursor = at + 1;
        }
    }

    /// Hand the wait of a namespace call back to the mount. Returns the
    /// cancellation that ends this run.
    pub(crate) fn park_mounted_namespace_call(
        &mut self,
        file_system_id: &DfsFileSystemId,
        parked: DfsParkedNamespaceCall,
        watch: Box<dyn DfsConfigurationChangeWatch>,
        until: Instant,
    ) -> DfsError {
        let cancellation = self.cancellation.clone();
        if let Some(scope) = self.mounted_call.as_mut() {
            let key = DfsMountedCallKey::of(file_system_id, &parked.operation);
            scope.parked = Some((key, parked));
            scope.handed_back = Some(MountedSifsRecordWait {
                watch,
                until: Some(until),
                arrived: None,
                cancellation,
            });
        }
        record_wait_handed_back()
    }

    /// Hand the wait of an undelivered probe back to the mount, keeping its
    /// re-send timer for the next run. Returns the cancellation that ends
    /// this run.
    pub(crate) fn hand_back_mounted_probe_wait(
        &mut self,
        resend: DfsRecordFollowResend,
        watch: Box<dyn DfsConfigurationChangeWatch>,
        until: Instant,
    ) -> DfsError {
        let cancellation = self.cancellation.clone();
        if let Some(scope) = self.mounted_call.as_mut() {
            scope.probe_resend = Some(resend);
            scope.handed_back = Some(MountedSifsRecordWait {
                watch,
                until: Some(until),
                arrived: None,
                cancellation,
            });
        }
        record_wait_handed_back()
    }

    /// Hand the record wait of a routed write back to the mount, keeping its
    /// re-send timer for the next run. A message that arrives for the mount
    /// while it waits, such as an answer to the write, ends the wait too.
    /// Returns the cancellation that ends this run.
    pub(crate) fn hand_back_mounted_write_wait(
        &mut self,
        resend: DfsRecordFollowResend,
        watch: Box<dyn DfsConfigurationChangeWatch>,
        until: Instant,
    ) -> DfsError {
        let wake_watch = watch.waker();
        self.transport
            .set_incoming_arrival_waker(Some(Arc::new(move || wake_watch())));
        self.hand_back_mounted_probe_wait(resend, watch, until)
    }

    /// Hand the wait for the answer to a routed write its primary holds
    /// back to the mount. The write keeps its body, and the call's next run
    /// sends nothing for it (see [`Self::mounted_held_routed_write`]).
    /// Nothing is due on a timer: the primary answers when the write commits.
    /// The wait ends when a message arrives for the mount, such as that
    /// answer, when the configuration changes, or at the caller's deadline.
    /// Returns the cancellation that ends this run.
    pub(crate) fn hand_back_mounted_held_write_wait(
        &mut self,
        request_id: &str,
        watch: Box<dyn DfsConfigurationChangeWatch>,
    ) -> DfsError {
        let arrived = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let wake_watch = watch.waker();
        let on_arrival = Arc::clone(&arrived);
        self.transport
            .set_incoming_arrival_waker(Some(Arc::new(move || {
                on_arrival.store(true, std::sync::atomic::Ordering::Release);
                wake_watch();
            })));
        let until = self.transport.operation_deadline();
        let cancellation = self.cancellation.clone();
        if let Some(scope) = self.mounted_call.as_mut() {
            if !scope.held_writes.iter().any(|held| held == request_id) {
                scope.held_writes.push(request_id.to_owned());
            }
            scope.handed_back = Some(MountedSifsRecordWait {
                watch,
                until,
                arrived: Some(arrived),
                cancellation,
            });
        }
        record_wait_handed_back()
    }

    /// The outcome a run of a mounted call takes for the routed write
    /// `request_id` it is about to send under `shard`'s record, when an
    /// earlier run sent that write and its primary holds it. The run sends
    /// nothing: it takes the primary's answer if one arrived, and otherwise
    /// waits for that answer again. `None` when the call holds no such write,
    /// or when the write is unanswered and the record the run plans with is
    /// another one, so that the write goes to the primary that record names.
    pub(crate) fn mounted_held_routed_write(
        &self,
        request_id: &str,
        shard: &crate::DfsMetadataShardConfig,
    ) -> Option<Result<crate::NetworkedDfsWriteOutcome, DfsError>> {
        let scope = self.mounted_call.as_ref()?;
        if !scope.held_writes.iter().any(|held| held == request_id) {
            return None;
        }
        let pending = self.writes.pending_routed_writes.get(request_id)?;
        if pending.canonical_request_id != request_id {
            return None;
        }
        if let Some(answered) = &pending.result {
            return Some(answered.clone());
        }
        let sent_under = &pending.authority_shard;
        (pending.follow.is_none()
            && pending.command.is_some()
            && sent_under.file_system_id == shard.file_system_id
            && sent_under.shard_id == shard.shard_id
            && sent_under.epoch == shard.epoch
            && sent_under.primary_node_id == shard.primary_node_id)
            .then(|| {
                Ok(crate::NetworkedDfsWriteOutcome::Pending {
                    intent_id: pending
                        .remote_intent_id
                        .clone()
                        .unwrap_or_else(|| pending.canonical_request_id.clone()),
                    durable_receipts: pending.durable_receipts,
                    required_receipts: pending.required_receipts,
                })
            })
    }

    /// The re-send timer of the probe an earlier run handed back.
    pub(crate) fn take_mounted_probe_resend(&mut self) -> Option<DfsRecordFollowResend> {
        self.mounted_call
            .as_mut()
            .and_then(|scope| scope.probe_resend.take())
    }
}
