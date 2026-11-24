//! Runtime-local custody for responses addressed to a private DFS transport.
//!
//! This owner stores delivery keys and bounded messages, never write authority
//! or outcomes. The receiving service retains every response validation rule.
//!
//! A registration lasts as long as the requester's call. An answer that
//! arrives while the call's owner is not attached, its inbox gone, is kept
//! with the registration and handed to the owner that registers the same
//! request again; it goes with the registration when the call ends. Nothing
//! is kept by time.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use sinomo_core::network::{NetworkDeliveryState, NetworkError};

use crate::{DfsNetworkMessage, DfsObjectId, DfsRoutedMessage};

const MAX_DELIVERY_KEYS: usize = 4096;
const MAX_INBOX_MESSAGES: usize = 256;
const MAX_INBOX_BYTES: usize = 64 * 1024 * 1024;
/// Answers kept for one registered request while its owner is not attached.
const MAX_KEPT_MESSAGES_PER_REQUEST: usize = 16;
/// Answers kept for every request of this runtime while their owners are
/// not attached.
const MAX_KEPT_BYTES: usize = 64 * 1024 * 1024;

fn custody_error(detail: impl Into<String>) -> NetworkError {
    NetworkError::Io {
        operation: "DFS routed response custody".into(),
        detail: detail.into(),
    }
}

#[derive(Default)]
struct DeliveryState {
    next_generation: u64,
    closed: bool,
    entries: BTreeMap<(String, DfsObjectId), DeliveryEntry>,
    /// Bytes of every entry's kept answers.
    kept_bytes: usize,
}

struct DeliveryEntry {
    canonical: String,
    generation: u64,
    inbox: Weak<ResponseInbox>,
    /// Answers that arrived while the owner was not attached.
    kept: VecDeque<(DfsRoutedMessage, usize)>,
}

impl DeliveryState {
    /// Remove `key`'s entry and release its kept answers' charge.
    fn remove_entry(&mut self, key: &(String, DfsObjectId)) -> Option<DeliveryEntry> {
        let entry = self.entries.remove(key)?;
        let kept = entry.kept.iter().map(|(_, bytes)| *bytes).sum::<usize>();
        self.kept_bytes = self.kept_bytes.saturating_sub(kept);
        Some(entry)
    }
}

/// Who took an answer delivered to this node's Main.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DfsResponseCustody {
    /// The request's registered owner has it.
    Delivered,
    /// The request is registered but its owner is not attached: the answer
    /// is kept for the owner that registers the request again.
    Kept,
    /// No owner registered the request.
    Unowned,
}

/// Bounded response delivery scoped to one existing DFS runtime host.
#[derive(Default)]
pub struct DfsRoutedResponseDelivery {
    state: Mutex<DeliveryState>,
}

impl std::fmt::Debug for DfsRoutedResponseDelivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DfsRoutedResponseDelivery")
            .finish_non_exhaustive()
    }
}

impl DfsRoutedResponseDelivery {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register(
        self: &Arc<Self>,
        inbox: &DfsRoutedResponseInboxOwner,
        canonical: &str,
        object_id: &DfsObjectId,
        ids: &[String],
    ) -> Result<Option<DfsRoutedResponseRegistration>, NetworkError> {
        let failure = |detail| {
            NetworkError::delivery_failed(
                "register DFS response owner",
                NetworkDeliveryState::DefinitelyNotSent,
                detail,
            )
        };
        let mut keys = BTreeSet::from([canonical.to_string()]);
        keys.extend(ids.iter().cloned());
        if keys.iter().any(|key| key.is_empty() || key.len() > 1024) {
            return Err(failure("invalid response delivery key"));
        }
        let keys = keys
            .into_iter()
            .map(|key| (key, object_id.clone()))
            .collect::<BTreeSet<_>>();
        let mut state = self
            .state
            .lock()
            .map_err(|_| failure("delivery owner lock poisoned"))?;
        if state.closed {
            return Err(failure("DFS runtime response owner has shut down"));
        }
        if inbox
            .inbox
            .state
            .lock()
            .map_err(|_| failure("response inbox lock poisoned"))?
            .closed
        {
            return Err(failure("response inbox is closed"));
        }
        let mut additions = Vec::new();
        for key in &keys {
            if let Some(existing) = state.entries.get(key)
                && let Some(owner) = existing.inbox.upgrade()
            {
                if !Arc::ptr_eq(&owner, &inbox.inbox) || existing.canonical != canonical {
                    return Err(failure("response delivery key already has an active owner"));
                }
                continue;
            }
            additions.push(key.clone());
        }
        // Answers kept for this same request while it had no owner go to the
        // owner registering it now, in the order they arrived.
        let mut handed_over = Vec::new();
        for key in &additions {
            if state.entries.get(key).is_some_and(|existing| {
                existing.canonical == canonical && !existing.kept.is_empty()
            }) && let Some(mut existing) = state.remove_entry(key)
            {
                handed_over.extend(existing.kept.drain(..));
            }
        }
        if additions.is_empty() {
            return Ok(None);
        }
        let newly_allocated = additions
            .iter()
            .filter(|key| !state.entries.contains_key(*key))
            .count();
        if state.entries.len().saturating_add(newly_allocated) > MAX_DELIVERY_KEYS {
            return Err(failure("runtime response delivery key limit reached"));
        }
        let generation = state
            .next_generation
            .checked_add(1)
            .ok_or_else(|| failure("response delivery generation exhausted"))?;
        state.next_generation = generation;
        let mut tokens = Vec::with_capacity(additions.len());
        for key in additions {
            if let Some(replaced) = state.remove_entry(&key) {
                // Another request's answers do not pass to this owner.
                drop(replaced);
            }
            state.entries.insert(
                key.clone(),
                DeliveryEntry {
                    canonical: canonical.to_owned(),
                    generation,
                    inbox: Arc::downgrade(&inbox.inbox),
                    kept: VecDeque::new(),
                },
            );
            tokens.push((key, generation));
        }
        drop(state);
        if !handed_over.is_empty() {
            inbox.inbox.accept_kept(handed_over);
        }
        Ok(Some(DfsRoutedResponseRegistration {
            owner: Arc::downgrade(self),
            tokens,
        }))
    }

    /// How many answers are kept for requests whose owner is not attached.
    #[cfg(test)]
    pub(crate) fn kept_answer_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .values()
            .map(|entry| entry.kept.len())
            .sum()
    }

    /// The ingress supplies the already validated wire/declared value length.
    /// Acceptance means inbox custody only; the owning service validates effects.
    pub(crate) fn try_deliver_with_size(
        &self,
        message: &DfsRoutedMessage,
        encoded_bytes: usize,
    ) -> Result<bool, NetworkError> {
        Ok(self.deliver_with_size(message, encoded_bytes)? != DfsResponseCustody::Unowned)
    }

    /// Like [`Self::try_deliver_with_size`], and says who took the answer.
    pub(crate) fn deliver_with_size(
        &self,
        message: &DfsRoutedMessage,
        encoded_bytes: usize,
    ) -> Result<DfsResponseCustody, NetworkError> {
        let DfsNetworkMessage::DistributedWriteResponse(response) = &message.message else {
            return Ok(DfsResponseCustody::Unowned);
        };
        let mut state = self
            .state
            .lock()
            .map_err(|_| custody_error("delivery owner lock poisoned"))?;
        if state.closed {
            return Err(custody_error("DFS runtime response owner has shut down"));
        }
        let key = (response.request_id.clone(), response.object_id.clone());
        let Some(entry) = state.entries.get(&key) else {
            return Ok(DfsResponseCustody::Unowned);
        };
        let Some(inbox) = entry.inbox.upgrade() else {
            // The call is live but its owner is not attached: keep the answer
            // for the owner that registers this request again.
            let charge = encoded_bytes.max(1);
            let kept_bytes = state.kept_bytes;
            let entry = state
                .entries
                .get_mut(&key)
                .expect("the entry was read under this lock");
            if entry.kept.len() >= MAX_KEPT_MESSAGES_PER_REQUEST
                || kept_bytes
                    .checked_add(charge)
                    .is_none_or(|bytes| bytes > MAX_KEPT_BYTES)
            {
                return Err(custody_error(
                    "answers kept for an unattached owner are full; sender retains terminal response custody",
                ));
            }
            entry.kept.push_back((message.clone(), charge));
            state.kept_bytes += charge;
            return Ok(DfsResponseCustody::Kept);
        };
        // Keep registration and mailbox admission ordered with lease removal.
        // No callback or service lock is taken while this owner is locked.
        let mut incoming = inbox
            .state
            .lock()
            .map_err(|_| custody_error("response inbox lock poisoned"))?;
        if incoming.closed {
            return Err(custody_error("response inbox is closed"));
        }
        let charge = encoded_bytes.max(1);
        if incoming.messages.len() >= MAX_INBOX_MESSAGES
            || incoming
                .bytes
                .checked_add(charge)
                .is_none_or(|bytes| bytes > MAX_INBOX_BYTES)
        {
            return Err(custody_error(
                "response inbox is full; sender retains terminal response custody",
            ));
        }
        incoming.messages.push_back((message.clone(), charge));
        incoming.bytes += charge;
        let arrival = incoming.arrival.clone();
        drop(incoming);
        drop(state);
        inbox.ready.notify_all();
        inbox.async_ready.notify_waiters();
        // The owner's other wait, if it is in one, ends too. Neither lock is
        // held while its waker runs.
        if let Some(arrival) = arrival {
            arrival();
        }
        Ok(DfsResponseCustody::Delivered)
    }

    #[cfg(test)]
    pub(crate) fn try_deliver(&self, message: &DfsRoutedMessage) -> Result<bool, NetworkError> {
        let bytes = crate::metadata_io::encode_sobj(message)
            .map_err(|error| custody_error(error.to_string()))?;
        self.try_deliver_with_size(message, bytes.len())
    }

    pub(crate) fn shutdown(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        for entry in state.entries.values() {
            if let Some(inbox) = entry.inbox.upgrade() {
                inbox.close();
            }
        }
        state.entries.clear();
        state.kept_bytes = 0;
    }
}

/// A generation-fenced routing lease. It retains neither inbox nor service.
pub(crate) struct DfsRoutedResponseRegistration {
    owner: Weak<DfsRoutedResponseDelivery>,
    tokens: Vec<((String, DfsObjectId), u64)>,
}

impl Drop for DfsRoutedResponseRegistration {
    fn drop(&mut self) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let mut state = owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (key, generation) in &self.tokens {
            if state
                .entries
                .get(key)
                .is_some_and(|entry| entry.generation == *generation)
            {
                // The call ended: what was kept for it goes with it.
                state.remove_entry(key);
            }
        }
    }
}

#[derive(Default)]
struct InboxState {
    closed: bool,
    /// One pending interruption, retained until a wait consumes it so a
    /// wake raised just before the wait starts is not lost.
    interrupted: bool,
    bytes: usize,
    messages: VecDeque<(DfsRoutedMessage, usize)>,
    /// Run after each delivery, for an owner that waits on something other
    /// than this inbox.
    arrival: Option<crate::service_state::DfsCancellationWaker>,
}

#[derive(Default)]
struct ResponseInbox {
    state: Mutex<InboxState>,
    ready: Condvar,
    async_ready: tokio::sync::Notify,
}

impl ResponseInbox {
    /// Take answers kept while this inbox's request had no owner. They are
    /// already in custody, so they are taken beyond the inbox's own bounds.
    fn accept_kept(&self, kept: Vec<(DfsRoutedMessage, usize)>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return;
        }
        for (message, charge) in kept {
            state.bytes = state.bytes.saturating_add(charge);
            state.messages.push_back((message, charge));
        }
        let arrival = state.arrival.clone();
        drop(state);
        self.ready.notify_all();
        self.async_ready.notify_waiters();
        if let Some(arrival) = arrival {
            arrival();
        }
    }

    fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        drop(state);
        self.ready.notify_all();
        self.async_ready.notify_waiters();
    }
}

/// Exactly one transport owns this endpoint. Transport clones create a fresh
/// endpoint instead of copying response leases or sharing a consumer queue.
pub(crate) struct DfsRoutedResponseInboxOwner {
    inbox: Arc<ResponseInbox>,
}

impl DfsRoutedResponseInboxOwner {
    #[cfg(test)]
    pub(crate) fn inbox_identity_for_test(&self) -> usize {
        Arc::as_ptr(&self.inbox) as usize
    }

    pub(crate) fn new() -> Self {
        Self {
            inbox: Arc::new(ResponseInbox::default()),
        }
    }

    pub(crate) fn drain(&self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        let mut state = self
            .inbox
            .state
            .lock()
            .map_err(|_| custody_error("response inbox lock poisoned"))?;
        let messages = state
            .messages
            .drain(..)
            .map(|(message, _)| message)
            .collect();
        state.bytes = 0;
        Ok(messages)
    }

    pub(crate) fn len(&self) -> Result<usize, NetworkError> {
        let state = self
            .inbox
            .state
            .lock()
            .map_err(|_| custody_error("response inbox lock poisoned"))?;
        Ok(state.messages.len())
    }

    /// Run `waker` after each message delivered to this inbox, until it is
    /// replaced or cleared.
    pub(crate) fn set_arrival_waker(
        &self,
        waker: Option<crate::service_state::DfsCancellationWaker>,
    ) {
        self.inbox
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .arrival = waker;
    }

    /// Ends the current or next [`Self::wait`] on this inbox early.
    pub(crate) fn interrupt_waker(&self) -> crate::service_state::DfsCancellationWaker {
        let inbox = Arc::downgrade(&self.inbox);
        Arc::new(move || {
            let Some(inbox) = inbox.upgrade() else {
                return;
            };
            inbox
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .interrupted = true;
            inbox.ready.notify_all();
            inbox.async_ready.notify_waiters();
        })
    }

    pub(crate) fn wait(&self, timeout: Duration) -> Result<bool, NetworkError> {
        let state = self
            .inbox
            .state
            .lock()
            .map_err(|_| custody_error("response inbox lock poisoned"))?;
        let (mut state, _) = self
            .inbox
            .ready
            .wait_timeout_while(state, timeout, |state| {
                state.messages.is_empty() && !state.closed && !state.interrupted
            })
            .map_err(|_| custody_error("response inbox wait poisoned"))?;
        state.interrupted = false;
        if state.closed {
            return Err(custody_error("response inbox is closed"));
        }
        Ok(!state.messages.is_empty())
    }

    pub(crate) async fn wait_async(&self, timeout: Duration) -> Result<bool, NetworkError> {
        let notified = self.inbox.async_ready.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        {
            let state = self
                .inbox
                .state
                .lock()
                .map_err(|_| custody_error("response inbox lock poisoned"))?;
            if state.closed {
                return Err(custody_error("response inbox is closed"));
            }
            if !state.messages.is_empty() {
                return Ok(true);
            }
        }
        let _ = tokio::time::timeout(timeout, notified).await;
        let state = self
            .inbox
            .state
            .lock()
            .map_err(|_| custody_error("response inbox lock poisoned"))?;
        if state.closed {
            return Err(custody_error("response inbox is closed"));
        }
        Ok(!state.messages.is_empty())
    }
}

impl Drop for DfsRoutedResponseInboxOwner {
    fn drop(&mut self) {
        self.inbox.close();
    }
}

#[cfg(test)]
mod tests;
