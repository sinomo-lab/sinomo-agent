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
    /// The routed writes the call keeps across its runs: one whose primary
    /// holds it and whose wait for the answer a run handed back, and one a
    /// run handed back to send again. Their response owners stay registered
    /// until the call ends, and a write the primary holds keeps its body.
    held_writes: Vec<DfsMountedHeldWrite>,
}

/// A routed write a mounted call keeps across its runs.
#[derive(Debug)]
struct DfsMountedHeldWrite {
    request_id: String,
    /// What watches a write its primary holds for the events that ask it to
    /// be sent again; `None` for a write the call sends again itself.
    watch: Option<crate::held_write_watch::DfsHeldWriteWatch>,
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
            for held in std::mem::take(&mut scope.held_writes) {
                service.compact_pending_routed_write(&held.request_id);
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
    /// Asks the held write this wait is for to be sent again.
    held: Option<Arc<crate::held_write_watch::DfsHeldWriteWake>>,
    /// The record the held write this wait is for was sent under.
    sent_under: Option<crate::service_record_follow::DfsRecordChangeEvidence>,
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
    /// cancelled. A held write's wait ends on a change of the record it was
    /// sent under, not on every replaced publication. The caller holds none
    /// of the mount's locks.
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
        if self.sent_under.is_some() || self.until.is_none() {
            // Nothing is due on a timer: only an arrival, a change of the
            // record, a re-send the write's watch asks for, the caller's
            // budget or the mount's cancellation ends the wait.
            loop {
                let arrived = self
                    .arrived
                    .as_ref()
                    .is_some_and(|arrived| arrived.load(std::sync::atomic::Ordering::Acquire));
                let resend = self.held.as_ref().is_some_and(|held| held.is_due());
                if arrived
                    || resend
                    || self
                        .cancellation
                        .as_ref()
                        .is_some_and(DfsCancellationFlag::is_cancelled)
                {
                    return;
                }
                let slice = match self.until {
                    Some(until) => {
                        let left = until.saturating_duration_since(Instant::now());
                        if left.is_zero() {
                            return;
                        }
                        left
                    }
                    None => MOUNTED_HELD_WRITE_WATCH_SLICE,
                };
                // A change of the record, or a failed watch, ends the wait;
                // the next run reads the record.
                match self.watch.wait_for_change(slice) {
                    Ok(false) => {}
                    Ok(true) if !self.record_may_have_moved() => {}
                    Ok(true) | Err(_) => return,
                }
            }
        }
        let Some(until) = self.until else {
            return;
        };
        let wait = until.saturating_duration_since(Instant::now());
        if !wait.is_zero() {
            // A failed watch ends the wait; the next run reads the record.
            let _ = self.watch.wait_for_change(wait);
        }
    }

    /// Whether the publication the watch holds may name a record other than
    /// the one the held write was sent under: a newer record, or one the
    /// watch cannot read.
    fn record_may_have_moved(&self) -> bool {
        let Some(sent_under) = &self.sent_under else {
            return true;
        };
        self.watch
            .held_shard_record(
                &sent_under.dispatched.file_system_id,
                &sent_under.dispatched.shard_id,
            )
            .is_none_or(|current| sent_under.satisfied_by(&current))
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
                held: None,
                sent_under: None,
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
                held: None,
                sent_under: None,
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
    /// answer, when a publication changes the record `sent_under` names,
    /// when an event asks for the write to be sent again (see
    /// [`crate::held_write_watch`]), or at the caller's deadline; a
    /// publication that leaves that record in place does not run the call.
    /// Returns the cancellation that ends this run.
    pub(crate) fn hand_back_mounted_held_write_wait(
        &mut self,
        request_id: &str,
        sent_under: crate::service_record_follow::DfsRecordChangeEvidence,
        watch: Box<dyn DfsConfigurationChangeWatch>,
    ) -> DfsError {
        let arrived = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let wake_watch = watch.waker();
        let on_arrival = Arc::clone(&arrived);
        let wake_on_arrival = Arc::clone(&wake_watch);
        self.transport
            .set_incoming_arrival_waker(Some(Arc::new(move || {
                on_arrival.store(true, std::sync::atomic::Ordering::Release);
                wake_on_arrival();
            })));
        let holds_watch = self.mounted_call.as_ref().is_some_and(|scope| {
            scope
                .held_writes
                .iter()
                .any(|held| held.request_id == request_id && held.watch.is_some())
        });
        let armed = (!holds_watch)
            .then(|| self.watch_held_routed_write(request_id))
            .flatten();
        let until = self.transport.operation_deadline();
        let cancellation = self.cancellation.clone();
        if let Some(scope) = self.mounted_call.as_mut() {
            let position = scope
                .held_writes
                .iter()
                .position(|held| held.request_id == request_id)
                .unwrap_or_else(|| {
                    scope.held_writes.push(DfsMountedHeldWrite {
                        request_id: request_id.to_owned(),
                        watch: None,
                    });
                    scope.held_writes.len() - 1
                });
            let held = &mut scope.held_writes[position];
            if let Some(armed) = armed {
                held.watch = Some(armed);
            }
            let wake = held.watch.as_ref().map(|watch| Arc::clone(&watch.wake));
            if let Some(wake) = &wake {
                wake.set_waker(Some(Arc::new(move || wake_watch())));
            }
            scope.handed_back = Some(MountedSifsRecordWait {
                watch,
                until,
                arrived: Some(arrived),
                held: wake,
                sent_under: Some(sent_under),
                cancellation,
            });
        }
        record_wait_handed_back()
    }

    /// Whether an event asked the held write `request_id` of the running
    /// mounted call to be sent again; the next event asks again.
    pub(crate) fn take_mounted_held_write_due(&self, request_id: &str) -> bool {
        self.mounted_call.as_ref().is_some_and(|scope| {
            scope.held_writes.iter().any(|held| {
                held.request_id == request_id
                    && held
                        .watch
                        .as_ref()
                        .is_some_and(|watch| watch.wake.take_due())
            })
        })
    }

    /// Compact a routed write whose run hands its wait back to send it again,
    /// and keep it with the mounted call: the call is still waiting, so its
    /// response owner stays registered, never released by the late-answer
    /// ledger's age or count, until the call ends.
    pub(crate) fn compact_pending_routed_write_for_mounted_call(&mut self, request_id: &str) {
        self.compact_pending_routed_write(request_id);
        let Some(canonical) = self.canonical_pending_routed_write_request_id(request_id) else {
            return;
        };
        for pending in self
            .writes
            .pending_routed_writes
            .values_mut()
            .filter(|pending| pending.canonical_request_id == canonical)
        {
            pending.compact_retained_at = None;
        }
        if let Some(scope) = self.mounted_call.as_mut()
            && !scope
                .held_writes
                .iter()
                .any(|held| held.request_id == canonical)
        {
            scope.held_writes.push(DfsMountedHeldWrite {
                request_id: canonical,
                watch: None,
            });
        }
    }

    /// The outcome a run of a mounted call takes for the routed write
    /// `request_id` it is about to send under `shard`'s record, when an
    /// earlier run sent that write and its primary holds it. The run sends
    /// nothing: it takes the primary's answer if one arrived, and otherwise
    /// waits for that answer again, with its response owner registered
    /// again, so an answer Main kept while no owner was attached comes to
    /// it. `None` when the call holds no such write, when an event asked for
    /// it to be sent again and no answer has come, or when the write is
    /// unanswered and the record the run plans with is another one: the
    /// write then goes again under its id.
    pub(crate) fn mounted_held_routed_write(
        &mut self,
        request_id: &str,
        shard: &crate::DfsMetadataShardConfig,
    ) -> Option<Result<crate::NetworkedDfsWriteOutcome, DfsError>> {
        let scope = self.mounted_call.as_ref()?;
        let held = scope
            .held_writes
            .iter()
            .find(|held| held.request_id == request_id)?;
        let resend = held
            .watch
            .as_ref()
            .is_some_and(|watch| watch.wake.take_due());
        let pending = self.writes.pending_routed_writes.get(request_id)?;
        if pending.canonical_request_id != request_id {
            return None;
        }
        if let Some(answered) = &pending.result {
            return Some(answered.clone());
        }
        let sent_under = &pending.authority_shard;
        let unchanged = pending.follow.is_none()
            && pending.command.is_some()
            && sent_under.file_system_id == shard.file_system_id
            && sent_under.shard_id == shard.shard_id
            && sent_under.epoch == shard.epoch
            && sent_under.primary_node_id == shard.primary_node_id;
        if !unchanged {
            return None;
        }
        let outcome = crate::NetworkedDfsWriteOutcome::Pending {
            intent_id: pending
                .remote_intent_id
                .clone()
                .unwrap_or_else(|| pending.canonical_request_id.clone()),
            durable_receipts: pending.durable_receipts,
            required_receipts: pending.required_receipts,
        };
        let object_id = pending.object_id.clone();
        let aliases = self
            .writes
            .pending_routed_writes
            .iter()
            .filter(|(_, pending)| pending.canonical_request_id == request_id)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        // Registering again hands over an answer Main kept while this call's
        // owner was not attached.
        if let Err(error) = self
            .transport
            .register_routed_write_response_owner(request_id, &object_id, &aliases)
        {
            tracing::debug!(%error, request_id, "held write's response owner was not registered again");
        }
        if resend {
            // An answer that already arrived is read before the write is
            // sent again.
            if self
                .transport
                .pending_incoming_count()
                .ok()
                .flatten()
                .is_some_and(|pending| pending > 0)
                && let Err(error) = self.process_incoming()
            {
                return Some(Err(error));
            }
            return self
                .writes
                .pending_routed_writes
                .get(request_id)
                .and_then(|pending| pending.result.clone());
        }
        Some(Ok(outcome))
    }

    /// The re-send timer of the probe an earlier run handed back.
    pub(crate) fn take_mounted_probe_resend(&mut self) -> Option<DfsRecordFollowResend> {
        self.mounted_call
            .as_mut()
            .and_then(|scope| scope.probe_resend.take())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;

    /// A publication a watch observes: the record it holds, how many times
    /// it was replaced, and one pending wake.
    #[derive(Default)]
    struct Publication {
        state: Mutex<(Option<crate::DfsMetadataShardConfig>, u64, bool)>,
        changed: std::sync::Condvar,
    }

    impl Publication {
        fn replace(&self, record: crate::DfsMetadataShardConfig) {
            let mut state = self.state.lock().unwrap();
            state.0 = Some(record);
            state.1 += 1;
            self.changed.notify_all();
        }
    }

    struct PublicationWatch {
        publication: Arc<Publication>,
        seen: u64,
    }

    impl DfsConfigurationChangeWatch for PublicationWatch {
        fn wait_for_change(&mut self, wait: Duration) -> Result<bool, sinomo_core::kv::KvError> {
            let state = self.publication.state.lock().unwrap();
            let seen = self.seen;
            let (mut state, _) = self
                .publication
                .changed
                .wait_timeout_while(state, wait, |state| state.1 == seen && !state.2)
                .unwrap();
            state.2 = false;
            let changed = state.1 != self.seen;
            self.seen = state.1;
            Ok(changed)
        }

        fn waker(&self) -> Arc<dyn Fn() + Send + Sync> {
            let publication = Arc::clone(&self.publication);
            Arc::new(move || {
                publication.state.lock().unwrap().2 = true;
                publication.changed.notify_all();
            })
        }

        fn held_shard_record(
            &self,
            file_system_id: &DfsFileSystemId,
            shard_id: &str,
        ) -> Option<crate::DfsMetadataShardConfig> {
            self.publication
                .state
                .lock()
                .unwrap()
                .0
                .clone()
                .filter(|record| {
                    record.file_system_id == *file_system_id && record.shard_id == shard_id
                })
        }
    }

    /// A held write's wait runs the call again on a change of the record
    /// the write was sent under, not on a publication that leaves it in
    /// place.
    #[test]
    fn held_write_wait_ends_on_its_record_change_not_on_every_publication() {
        let sent_under = crate::DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: "held".into(),
            primary_node_id: sinomo_core::node::NodeId::from_legacy_named("held-primary").unwrap(),
            replica_node_ids: Vec::new(),
            epoch: 2,
            assignment: crate::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let publication = Arc::new(Publication::default());
        publication.replace(sent_under.clone());
        let wait = MountedSifsRecordWait {
            watch: Box::new(PublicationWatch {
                publication: Arc::clone(&publication),
                seen: 1,
            }),
            until: None,
            arrived: Some(Arc::new(std::sync::atomic::AtomicBool::new(false))),
            held: None,
            sent_under: Some(crate::service_record_follow::DfsRecordChangeEvidence {
                dispatched: sent_under.clone(),
                floor_epoch: None,
            }),
            cancellation: None,
        };
        let (ended, waited) = std::sync::mpsc::channel();
        let waiting = std::thread::spawn(move || {
            wait.wait();
            let _ = ended.send(());
        });
        // Publications that keep the record: the wait goes on.
        for _ in 0..3 {
            publication.replace(sent_under.clone());
            assert!(
                waited.recv_timeout(Duration::from_millis(200)).is_err(),
                "a publication that leaves the record in place does not end the wait"
            );
        }
        // The record moves to a newer epoch: the wait ends.
        let mut newer = sent_under;
        newer.epoch = 3;
        publication.replace(newer);
        waited
            .recv_timeout(Duration::from_secs(5))
            .expect("a change of the record ends the wait");
        waiting.join().unwrap();
    }
}
