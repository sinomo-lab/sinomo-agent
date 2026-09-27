//! Bounded, fair admission for attachment producers. A permit lives with the
//! executing blocking operation, not its cancellable RPC waiter.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

#[derive(Clone, Debug)]
pub(crate) struct AttachmentWorkLimits {
    pub max_queued: usize,
    pub max_queued_per_owner: usize,
    pub max_buffered_bytes: u64,
    pub bytes_per_minute: u64,
    pub operations_per_minute: u64,
}

impl Default for AttachmentWorkLimits {
    fn default() -> Self {
        Self {
            max_queued: 64,
            max_queued_per_owner: 4,
            max_buffered_bytes: 128 * 1024 * 1024,
            bytes_per_minute: 256 * 1024 * 1024,
            operations_per_minute: 1024,
        }
    }
}

impl AttachmentWorkLimits {
    pub(crate) fn for_actor(policy: &crate::DfsAttachmentPolicy, actor: &str) -> Self {
        let producer = policy.producer(actor);
        Self {
            max_queued: policy.max_queued,
            max_queued_per_owner: policy.max_queued_per_producer,
            max_buffered_bytes: policy.max_inflight_bytes,
            bytes_per_minute: producer.bytes_per_minute,
            operations_per_minute: producer.operations_per_minute,
        }
    }
}

#[derive(Default)]
pub(crate) struct AttachmentAdmission {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    queues: BTreeMap<String, VecDeque<Waiter>>,
    order: VecDeque<String>,
    active: bool,
    bytes: u64,
    pending: usize,
    sequence: u64,
    operations: BTreeSet<(String, String)>,
    rates: BTreeMap<String, Rate>,
}

struct Rate {
    since: Instant,
    bytes: u64,
    operations: u64,
}
struct Waiter {
    id: u64,
    operation: String,
    bytes: u64,
    ready: oneshot::Sender<()>,
}

pub(crate) struct AttachmentPermit {
    admission: Arc<AttachmentAdmission>,
    owner: String,
    operation: String,
    bytes: u64,
}

struct Waiting {
    admission: Arc<AttachmentAdmission>,
    owner: String,
    operation: String,
    bytes: u64,
    id: u64,
    armed: bool,
}

impl AttachmentAdmission {
    pub(crate) async fn acquire(
        self: &Arc<Self>,
        owner: String,
        operation: String,
        bytes: u64,
        limits: &AttachmentWorkLimits,
    ) -> Result<AttachmentPermit, String> {
        let (tx, rx) = oneshot::channel();
        let id;
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "attachment admission lock poisoned")?;
            if state
                .operations
                .contains(&(owner.clone(), operation.clone()))
            {
                return Err("attachment operation is already queued or running; reconcile using the same operation id".into());
            }
            if state.pending >= limits.max_queued
                || state
                    .queues
                    .get(&owner)
                    .is_some_and(|q| q.len() >= limits.max_queued_per_owner)
                || bytes > limits.max_buffered_bytes.saturating_sub(state.bytes)
            {
                return Err(
                    "attachment producer queue or byte budget exhausted; retry with backoff".into(),
                );
            }
            let now = Instant::now();
            state
                .rates
                .retain(|_, r| now.duration_since(r.since) < Duration::from_secs(60));
            if !state.rates.contains_key(&owner) && state.rates.len() >= 1024 {
                return Err("attachment producer admission capacity exhausted".into());
            }
            let rate = state.rates.entry(owner.clone()).or_insert(Rate {
                since: now,
                bytes: 0,
                operations: 0,
            });
            if bytes > limits.bytes_per_minute.saturating_sub(rate.bytes)
                || rate.operations >= limits.operations_per_minute
            {
                return Err(
                    "attachment producer rate budget exhausted; retry after the admission window"
                        .into(),
                );
            }
            rate.bytes += bytes;
            rate.operations += 1;
            state.sequence = state.sequence.wrapping_add(1);
            id = state.sequence;
            state.bytes += bytes;
            state.pending += 1;
            state.operations.insert((owner.clone(), operation.clone()));
            if !state.queues.contains_key(&owner) {
                state.order.push_back(owner.clone());
            }
            state
                .queues
                .entry(owner.clone())
                .or_default()
                .push_back(Waiter {
                    id,
                    operation: operation.clone(),
                    bytes,
                    ready: tx,
                });
            state.dispatch();
        }
        let mut waiting = Waiting {
            admission: Arc::clone(self),
            owner: owner.clone(),
            operation: operation.clone(),
            bytes,
            id,
            armed: true,
        };
        rx.await.map_err(|_| "attachment admission cancelled")?;
        waiting.armed = false;
        Ok(AttachmentPermit {
            admission: Arc::clone(self),
            owner,
            operation,
            bytes,
        })
    }
}

impl State {
    fn dispatch(&mut self) {
        if self.active {
            return;
        }
        while let Some(owner) = self.order.pop_front() {
            let queue = self.queues.get_mut(&owner).expect("queued owner");
            let waiter = queue.pop_front().expect("nonempty owner queue");
            if queue.is_empty() {
                self.queues.remove(&owner);
            } else {
                self.order.push_back(owner.clone());
            }
            if waiter.ready.send(()).is_ok() {
                self.active = true;
                return;
            }
            self.bytes -= waiter.bytes;
            self.pending -= 1;
            self.operations.remove(&(owner, waiter.operation));
        }
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut state = self
            .admission
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let queued = state
            .queues
            .get_mut(&self.owner)
            .and_then(|queue| {
                let index = queue.iter().position(|waiter| waiter.id == self.id)?;
                queue.remove(index)
            })
            .is_some();
        if state
            .queues
            .get(&self.owner)
            .is_some_and(VecDeque::is_empty)
        {
            state.queues.remove(&self.owner);
            state.order.retain(|owner| owner != &self.owner);
        }
        // A sent wake owns the active slot even if cancellation wins before the
        // await resumes. Releasing that slot here prevents a stranded producer.
        if state
            .operations
            .remove(&(self.owner.clone(), self.operation.clone()))
        {
            state.bytes -= self.bytes;
            state.pending -= 1;
            if !queued {
                state.active = false;
            }
        }
        state.dispatch();
    }
}

impl Drop for AttachmentPermit {
    fn drop(&mut self) {
        let mut state = self
            .admission
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        state
            .operations
            .remove(&(self.owner.clone(), self.operation.clone()));
        state.bytes -= self.bytes;
        state.pending -= 1;
        state.active = false;
        state.dispatch();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn attachment_admission_deduplicates_and_releases_cancelled_waiters() {
        let admission = Arc::new(AttachmentAdmission::default());
        let limits = AttachmentWorkLimits {
            max_buffered_bytes: 16,
            ..Default::default()
        };
        let active = admission
            .acquire("a".into(), "one".into(), 8, &limits)
            .await
            .unwrap();
        assert!(
            admission
                .acquire("a".into(), "one".into(), 8, &limits)
                .await
                .is_err()
        );
        let a = Arc::clone(&admission);
        let l = limits.clone();
        let queued = tokio::spawn(async move { a.acquire("b".into(), "two".into(), 8, &l).await });
        tokio::task::yield_now().await;
        assert!(
            admission
                .acquire("c".into(), "three".into(), 1, &limits)
                .await
                .is_err()
        );
        queued.abort();
        let _ = queued.await;
        drop(active);
        let permit = admission
            .acquire("c".into(), "three".into(), 16, &limits)
            .await
            .unwrap();
        drop(permit);
        let state = admission.state.lock().unwrap();
        assert_eq!((state.pending, state.bytes, state.active), (0, 0, false));
    }
}

#[cfg(test)]
mod fairness_tests {
    use super::*;
    #[tokio::test]
    async fn attachment_queue_round_robins_owners_and_recovers_cancelled_grants() {
        let admission = Arc::new(AttachmentAdmission::default());
        let limits = AttachmentWorkLimits::default();
        let active = admission
            .acquire("active".into(), "0".into(), 0, &limits)
            .await
            .unwrap();
        let mut pending = Vec::new();
        for (owner, operation) in [("a", "1"), ("a", "2"), ("b", "3")] {
            let mut future =
                Box::pin(admission.acquire(owner.into(), operation.into(), 1, &limits));
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(
                    future.as_mut().poll(cx).is_pending()
                ))
                .await
            );
            pending.push(future);
        }
        drop(active); // a/1 granted, but its future has not resumed.
        drop(pending.remove(0)); // cancellation must transfer the active slot.
        let b = pending.remove(1).await.unwrap(); // b before a/2.
        assert_eq!(b.owner, "b");
        drop(b);
        drop(pending.remove(0).await.unwrap());
        let state = admission.state.lock().unwrap();
        assert_eq!((state.pending, state.bytes, state.active), (0, 0, false));
    }
}
