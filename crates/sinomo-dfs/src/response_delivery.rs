//! Runtime-local custody for responses addressed to a private DFS transport.
//!
//! This owner stores delivery keys and bounded messages, never write authority
//! or outcomes. The receiving service retains every response validation rule.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use sinomo_core::network::{NetworkDeliveryState, NetworkError};

use crate::{DfsNetworkMessage, DfsObjectId, DfsRoutedMessage};

const MAX_DELIVERY_KEYS: usize = 4096;
const MAX_INBOX_MESSAGES: usize = 256;
const MAX_INBOX_BYTES: usize = 64 * 1024 * 1024;

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
}

struct DeliveryEntry {
    canonical: String,
    generation: u64,
    inbox: Weak<ResponseInbox>,
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
            state.entries.insert(
                key.clone(),
                DeliveryEntry {
                    canonical: canonical.to_owned(),
                    generation,
                    inbox: Arc::downgrade(&inbox.inbox),
                },
            );
            tokens.push((key, generation));
        }
        Ok(Some(DfsRoutedResponseRegistration {
            owner: Arc::downgrade(self),
            tokens,
        }))
    }

    /// The ingress supplies the already validated wire/declared value length.
    /// Acceptance means inbox custody only; the owning service validates effects.
    pub(crate) fn try_deliver_with_size(
        &self,
        message: &DfsRoutedMessage,
        encoded_bytes: usize,
    ) -> Result<bool, NetworkError> {
        let DfsNetworkMessage::DistributedWriteResponse(response) = &message.message else {
            return Ok(false);
        };
        let state = self
            .state
            .lock()
            .map_err(|_| custody_error("delivery owner lock poisoned"))?;
        if state.closed {
            return Err(custody_error("DFS runtime response owner has shut down"));
        }
        let key = (response.request_id.clone(), response.object_id.clone());
        let Some(entry) = state.entries.get(&key) else {
            return Ok(false);
        };
        let Some(inbox) = entry.inbox.upgrade() else {
            return Ok(false);
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
        drop(incoming);
        inbox.ready.notify_all();
        inbox.async_ready.notify_waiters();
        Ok(true)
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
                state.entries.remove(key);
            }
        }
    }
}

#[derive(Default)]
struct InboxState {
    closed: bool,
    bytes: usize,
    messages: VecDeque<(DfsRoutedMessage, usize)>,
}

#[derive(Default)]
struct ResponseInbox {
    state: Mutex<InboxState>,
    ready: Condvar,
    async_ready: tokio::sync::Notify,
}

impl ResponseInbox {
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

    pub(crate) fn wait(&self, timeout: Duration) -> Result<bool, NetworkError> {
        let state = self
            .inbox
            .state
            .lock()
            .map_err(|_| custody_error("response inbox lock poisoned"))?;
        let (state, _) = self
            .inbox
            .ready
            .wait_timeout_while(state, timeout, |state| {
                state.messages.is_empty() && !state.closed
            })
            .map_err(|_| custody_error("response inbox wait poisoned"))?;
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
