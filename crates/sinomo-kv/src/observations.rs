//! Ordered process-local observations of already committed KV effects.
//!
//! Admission reserves one fixed slot before the canonical effect. Commit seals
//! its exact storage/proposal owner fences and assigns an ordinal without heap
//! allocation. Captured payloads retain the same owner release latches, and
//! callbacks run only after those owners release. These observations never
//! grant authority or substitute for replay.

use crate::{ClusterKvCommittedDelta, ClusterKvPaths, KvError, KvRevision, KvSnapshot};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};

/// Production bounds outstanding effects, including the callback currently
/// running. Captured payloads share their immutable Arc allocations once.
/// Their size is variable: neither the command bound nor the Raft compaction
/// trigger establishes a fixed bound on all valid deltas or checkpoints.
pub const KV_COMMITTED_OBSERVATION_MAX_SLOTS: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KvObservationProposalOwnership {
    Required,
    Absent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KvCommittedObservationKind {
    Contiguous,
    Checkpoint,
    Replacement,
}

/// One actual newly applied request/result under its enclosing native quorum.
/// This observation grants no authority and is never synthesized for replay.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct KvOriginalCurrentTermCommit {
    pub root: PathBuf,
    pub node_id: sinomo_node::NodeId,
    pub request: crate::KvCommandRequest,
    pub committed: crate::KvCommittedRequestRecord,
    /// The actual certified enclosing prefix; the stored request keeps its own coordinates.
    pub authority: crate::KvAuthorityProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvCommittedObservationPayload {
    pub delta: Option<Arc<ClusterKvCommittedDelta>>,
    pub snapshot: Option<Arc<KvSnapshot>>,
    /// Actual captured snapshot prefix; an installed snapshot may precede
    /// later entries applied in the same uninterrupted storage-owner scope.
    pub snapshot_applied_index: Option<u64>,
    /// Optional honest captured encoded size from an existing producer
    /// builder. This is not a heap-memory estimate. Production has no byte
    /// quota for unknown-size snapshots and does not scan a store for sizing.
    pub encoded_bytes: Option<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KvCommittedObservationDiscontinuity {
    /// Producer returned an error, unwound, or lost its last publication owner.
    PublicationAbandoned,
    PayloadOverflow {
        declared_bytes: usize,
        allowance_bytes: usize,
    },
    InconsistentPayload,
    /// This subscriber failed its own delivery; other subscribers continue.
    SubscriberFailed {
        subscriber_id: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvCommittedObservationValue {
    Delta(Arc<ClusterKvCommittedDelta>),
    Snapshot(Arc<KvSnapshot>),
    Discontinuity(KvCommittedObservationDiscontinuity),
}

#[derive(Clone, Debug)]
pub struct KvCommittedObservation {
    /// None only for a repair whose corrupt prior projection was undecodable.
    pub previous_revision: Option<KvRevision>,
    /// None on an uncertain repair before any actual revision was captured.
    pub revision: Option<KvRevision>,
    pub applied_index: Option<u64>,
    pub kind: KvCommittedObservationKind,
    pub value: KvCommittedObservationValue,
    pub original_current_term_commits: Option<Arc<Vec<Arc<KvOriginalCurrentTermCommit>>>>,
    preparation: OnceLock<()>,
    delivery: Arc<DeliveryStatus>,
}

impl KvCommittedObservation {
    /// Run the bridge's pure cache invalidation once, before consumer callbacks.
    /// Each emitted phase has its own preparation fence.
    pub fn prepare_once(&self, prepare: impl FnOnce()) {
        self.preparation.get_or_init(|| {
            prepare();
            // A failed/unwound preparer is not evidence that invalidation ran.
            self.delivery.prepared.store(true, Ordering::Release);
        });
    }
}

impl PartialEq for KvCommittedObservation {
    fn eq(&self, other: &Self) -> bool {
        self.previous_revision == other.previous_revision
            && self.revision == other.revision
            && self.applied_index == other.applied_index
            && self.kind == other.kind
            && self.value == other.value
            && self.original_current_term_commits == other.original_current_term_commits
    }
}
impl Eq for KvCommittedObservation {}

#[derive(Debug, Default)]
struct DeliveryStatus {
    failed_subscribers: AtomicUsize,
    prepared: AtomicBool,
}

#[derive(Clone, Copy, Debug)]
struct Metadata {
    ordinal: u64,
    previous_revision: Option<KvRevision>,
    revision: Option<KvRevision>,
    applied_index: Option<u64>,
    kind: KvCommittedObservationKind,
}

enum Resolution {
    Payload(Arc<KvCommittedObservationPayload>),
    Gap(KvCommittedObservationDiscontinuity),
    NoEffect,
}

enum Slot {
    Free,
    Admitted {
        lease: u64,
    },
    Committed {
        lease: u64,
        metadata: Metadata,
        delivery: Arc<DeliveryStatus>,
        storage_released: bool,
        proposal_released: bool,
        resolution: Option<Resolution>,
        original_current_term_commits: Option<Arc<Vec<Arc<KvOriginalCurrentTermCommit>>>>,
        uncertified_original_requests:
            Vec<(crate::KvCommandRequest, crate::KvCommittedRequestRecord)>,
    },
    Delivering {
        lease: u64,
    },
}

type Observer = Arc<dyn Fn(&KvCommittedObservation) + Send + Sync>;

struct QueueState {
    slots: Vec<Slot>,
    next_lease: u64,
    next_ordinal: u64,
    next_delivery: u64,
    reserved_bytes: usize,
    draining: bool,
    drainer_thread: Option<std::thread::ThreadId>,
    next_subscription: u64,
    subscribers: BTreeMap<u64, Observer>,
    original_commit_interests: BTreeSet<u64>,
    failure_events: u64,
    suppressed_failures: u64,
}

struct Queue {
    root: PathBuf,
    allowance: Option<usize>,
    state: Mutex<QueueState>,
    delivery_wake: Condvar,
}

static QUEUES: OnceLock<Mutex<BTreeMap<PathBuf, Weak<Queue>>>> = OnceLock::new();

impl Queue {
    fn new(root: PathBuf, slots: usize, allowance: Option<usize>) -> Self {
        Self {
            root,
            allowance,
            state: Mutex::new(QueueState {
                slots: (0..slots).map(|_| Slot::Free).collect(),
                next_lease: 1,
                next_ordinal: 1,
                next_delivery: 1,
                reserved_bytes: 0,
                draining: false,
                drainer_thread: None,
                next_subscription: 1,
                subscribers: BTreeMap::new(),
                original_commit_interests: BTreeSet::new(),
                failure_events: 0,
                suppressed_failures: 0,
            }),
            delivery_wake: Condvar::new(),
        }
    }

    fn drain(self: &Arc<Self>, triggering_index: usize, triggering_lease: u64) {
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.draining {
                return;
            }
            // A cancellation/release of n+1 may find n ready. It must not
            // initiate callbacks while n+1's own proposal or storage owner is
            // still held. Only an exact fully released source can activate.
            if !matches!(state.slots[triggering_index], Slot::Committed {
                lease, storage_released: true, proposal_released: true, ..
            } if lease == triggering_lease)
            {
                return;
            }
            state.draining = true;
            state.drainer_thread = Some(std::thread::current().id());
        }
        let mut guard = DrainGuard {
            queue: Arc::clone(self),
            in_flight: None,
            armed: true,
        };
        loop {
            let next = {
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                let ordinal = state.next_delivery;
                let index = state.slots.iter().position(|slot| matches!(slot,
                    Slot::Committed { metadata, storage_released: true, proposal_released: true, resolution: Some(_), .. }
                    if metadata.ordinal == ordinal));
                let Some(index) = index else {
                    // Clear the flag under the same mutex that tested the head:
                    // concurrent ready/release cannot lose its drain wake.
                    state.draining = false;
                    state.drainer_thread = None;
                    guard.armed = false;
                    return;
                };
                let Slot::Committed {
                    lease,
                    metadata,
                    delivery,
                    resolution,
                    original_current_term_commits,
                    ..
                } = std::mem::replace(&mut state.slots[index], Slot::Free)
                else {
                    unreachable!("eligible committed slot")
                };
                state.slots[index] = Slot::Delivering { lease };
                guard.in_flight = Some((
                    index,
                    lease,
                    metadata,
                    Arc::clone(&delivery),
                    original_current_term_commits.clone(),
                ));
                let resolution = resolution.expect("eligible resolution");
                let observers = if matches!(resolution, Resolution::NoEffect) {
                    Vec::new()
                } else {
                    state
                        .subscribers
                        .iter()
                        .map(|(id, observer)| (*id, Arc::clone(observer)))
                        .collect::<Vec<_>>()
                };
                (
                    index,
                    lease,
                    metadata,
                    delivery,
                    resolution,
                    original_current_term_commits,
                    observers,
                )
            };
            let (
                index,
                lease,
                metadata,
                delivery,
                resolution,
                original_current_term_commits,
                observers,
            ) = next;
            let failed = std::cell::RefCell::new(BTreeSet::new());
            let mark_failed = |subscriber_id| {
                if failed.borrow_mut().insert(subscriber_id) {
                    delivery.failed_subscribers.fetch_add(1, Ordering::Release);
                }
            };
            let emit = |value, previous_revision, revision, applied_index| {
                let kind = if metadata.kind == KvCommittedObservationKind::Contiguous
                    && matches!(value, KvCommittedObservationValue::Snapshot(_))
                {
                    KvCommittedObservationKind::Checkpoint
                } else {
                    metadata.kind
                };
                let observation = KvCommittedObservation {
                    previous_revision,
                    revision,
                    applied_index,
                    kind,
                    value,
                    original_current_term_commits: original_current_term_commits.clone(),
                    preparation: OnceLock::new(),
                    delivery: Arc::clone(&delivery),
                };
                for (subscriber_id, observer) in &observers {
                    // Isolate the failed consumer, withdraw its projection with
                    // a typed gap, and preserve FIFO for all other consumers.
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        observer(&observation)
                    }))
                    .is_err()
                    {
                        mark_failed(*subscriber_id);
                        self.warn_failure(metadata, *subscriber_id, "callback");
                        let discontinuity = KvCommittedObservation {
                            previous_revision,
                            revision,
                            applied_index,
                            kind,
                            value: KvCommittedObservationValue::Discontinuity(
                                KvCommittedObservationDiscontinuity::SubscriberFailed {
                                    subscriber_id: *subscriber_id,
                                },
                            ),
                            original_current_term_commits: None,
                            preparation: OnceLock::new(),
                            delivery: Arc::clone(&delivery),
                        };
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            observer(&discontinuity)
                        }))
                        .is_err()
                        {
                            self.warn_failure(metadata, *subscriber_id, "discontinuity callback");
                        }
                    }
                }
            };
            match resolution {
                Resolution::NoEffect => {}
                Resolution::Gap(reason) => emit(
                    KvCommittedObservationValue::Discontinuity(reason),
                    metadata.previous_revision,
                    metadata.revision,
                    metadata.applied_index,
                ),
                Resolution::Payload(payload) => {
                    if metadata.kind == KvCommittedObservationKind::Replacement {
                        let mut previous_revision = metadata.previous_revision;
                        if let Some(snapshot) = payload.snapshot.as_ref() {
                            emit(
                                KvCommittedObservationValue::Snapshot(Arc::clone(snapshot)),
                                previous_revision,
                                Some(snapshot.revision),
                                payload.snapshot_applied_index,
                            );
                            previous_revision = Some(snapshot.revision);
                        }
                        if let Some(delta) = payload.delta.as_ref() {
                            emit(
                                KvCommittedObservationValue::Delta(Arc::clone(delta)),
                                previous_revision,
                                metadata.revision,
                                metadata.applied_index,
                            );
                        }
                    } else {
                        emit(
                            KvCommittedObservationValue::Delta(Arc::clone(
                                payload.delta.as_ref().expect("validated ordinary delta"),
                            )),
                            metadata.previous_revision,
                            metadata.revision,
                            metadata.applied_index,
                        );
                        if let Some(snapshot) = payload.snapshot.as_ref() {
                            emit(
                                KvCommittedObservationValue::Snapshot(Arc::clone(snapshot)),
                                metadata.revision,
                                Some(snapshot.revision),
                                payload.snapshot_applied_index,
                            );
                        }
                    }
                }
            }
            // Release payload/callback captures outside the mutex and retain
            // capacity until callbacks have finished using this dispatch.
            // Captured values may also unwind during subscriber teardown.
            // That teardown is outside locks and cannot park following items.
            for (subscriber_id, observer) in observers {
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(observer)))
                    .is_err()
                {
                    mark_failed(subscriber_id);
                    self.warn_failure(metadata, subscriber_id, "callback teardown");
                }
            }
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if matches!(state.slots[index], Slot::Delivering { lease: current } if current == lease)
            {
                state.slots[index] = Slot::Free;
                state.reserved_bytes = state
                    .reserved_bytes
                    .saturating_sub(self.allowance.unwrap_or(0));
                state.next_delivery += 1;
                self.delivery_wake.notify_all();
            }
            guard.in_flight = None;
        }
    }
    fn warn_failure(&self, metadata: Metadata, subscriber_id: u64, phase: &'static str) {
        let suppressed = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.failure_events = state.failure_events.saturating_add(1);
            if state.failure_events.is_power_of_two() {
                let suppressed = state.suppressed_failures;
                state.suppressed_failures = 0;
                Some(suppressed)
            } else {
                state.suppressed_failures = state.suppressed_failures.saturating_add(1);
                None
            }
        };
        if let Some(suppressed) = suppressed {
            tracing::warn!(
                target: "sinomo.kv.observations",
                root = %self.root.display(), ordinal = metadata.ordinal,
                revision = ?metadata.revision, subscriber = subscriber_id,
                phase, suppressed,
                "committed KV observer delivery failed"
            );
        }
    }
}

struct DrainGuard {
    queue: Arc<Queue>,
    in_flight: Option<(
        usize,
        u64,
        Metadata,
        Arc<DeliveryStatus>,
        Option<Arc<Vec<Arc<KvOriginalCurrentTermCommit>>>>,
    )>,
    armed: bool,
}

impl Drop for DrainGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut state = self
            .queue
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some((index, lease, metadata, delivery, original_current_term_commits)) =
            self.in_flight.take()
        {
            if matches!(state.slots[index], Slot::Delivering { lease: current } if current == lease)
            {
                state.slots[index] = Slot::Committed {
                    lease,
                    metadata,
                    delivery,
                    storage_released: true,
                    proposal_released: true,
                    resolution: Some(Resolution::Gap(
                        KvCommittedObservationDiscontinuity::PublicationAbandoned,
                    )),
                    original_current_term_commits,
                    uncertified_original_requests: Vec::new(),
                };
            }
        }
        state.draining = false;
        state.drainer_thread = None;
        self.queue.delivery_wake.notify_all();
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        if let Some(registry) = QUEUES.get() {
            let mut registry = registry.lock().unwrap_or_else(|error| error.into_inner());
            if registry
                .get(&self.root)
                .is_some_and(|registered| std::ptr::eq(registered.as_ptr(), self))
            {
                registry.remove(&self.root);
            }
        }
    }
}

fn queue(paths: &ClusterKvPaths) -> Arc<Queue> {
    // Match the existing proposal gate's configured root identity. Callers
    // reuse ClusterKvPaths owned by their canonical data-directory owner; this
    // boundary does no filesystem traversal or payload scan.
    let mut registry = QUEUES
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if let Some(queue) = registry.get(&paths.root).and_then(Weak::upgrade) {
        return queue;
    }
    let queue = Arc::new(Queue::new(
        paths.root.clone(),
        KV_COMMITTED_OBSERVATION_MAX_SLOTS,
        None,
    ));
    registry.insert(paths.root.clone(), Arc::downgrade(&queue));
    queue
}

#[derive(Clone)]
pub struct KvObservationCapacityPermit {
    source: Arc<Source>,
}

/// Weak locator for the original proposal owner and ordered delivery slot.
/// It can live outside an embedding facade mutex without retaining the source,
/// queue, subscriber, or facade through a reference cycle.
#[derive(Clone)]
pub struct KvObservationProposalCustodyHandle {
    source: Weak<Source>,
    queue: Weak<Queue>,
    index: usize,
    lease: u64,
}

impl KvObservationProposalCustodyHandle {
    pub fn retain(&self) -> Result<Option<crate::raft::ClusterKvRaftCommitSequenceGuard>, KvError> {
        if let Some(source) = self.source.upgrade() {
            let state = source
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(ownership) = state.retain_proposal_ownership(&source.queue.root)? {
                return Ok(Some(
                    crate::raft::ClusterKvRaftCommitSequenceGuard::from_retained_ownership(
                        ownership,
                    ),
                ));
            }
        }
        // A released physical owner may still have FIFO callbacks pending or
        // running. Refuse a new facade critical section until that exact slot
        // retires, rather than race its callback or manufacture another fence.
        if let Some(queue) = self.queue.upgrade() {
            let state = queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if matches!(&state.slots[self.index],
                Slot::Committed { lease, .. } | Slot::Delivering { lease } if *lease == self.lease)
            {
                return Err(KvError::Busy {
                    path: queue.root.display().to_string(),
                });
            }
        }
        Ok(None)
    }
}

struct Source {
    queue: Arc<Queue>,
    index: usize,
    lease: u64,
    ownership: KvObservationProposalOwnership,
    state: Mutex<SourceState>,
}

struct SourceState {
    next_fence: u64,
    storage: Option<u64>,
    proposal: Option<u64>,
    // Weak registration avoids an owner/source cycle. Only an admitted storage
    // fence upgrades and retains the physical proposal owner for its own phase.
    proposal_ownership: Option<Weak<crate::raft::ClusterKvRaftCommitSequenceOwnership>>,
    committed: Option<(Metadata, u64, Option<u64>)>,
    // The preallocated receipt moves to the producer at commit. A committed
    // source/fence must never be a strong owner of the receipt's cancellation.
    receipt: Option<Arc<Receipt>>,
}

impl SourceState {
    fn retain_proposal_ownership(
        &self,
        root: &std::path::Path,
    ) -> Result<Option<Arc<crate::raft::ClusterKvRaftCommitSequenceOwnership>>, KvError> {
        self.proposal_ownership
            .as_ref()
            .map(|ownership| {
                ownership.upgrade().ok_or_else(|| KvError::Busy {
                    path: root.display().to_string(),
                })
            })
            .transpose()
    }
}

#[derive(Clone, Copy, Debug)]
enum OwnerKind {
    Storage,
    Proposal,
}

pub struct KvObservationOwnerFence {
    source: Arc<Source>,
    id: u64,
    kind: OwnerKind,
    retained_proposal: Option<Arc<crate::raft::ClusterKvRaftCommitSequenceOwnership>>,
}

impl KvObservationOwnerFence {
    pub(crate) fn bind_proposal_ownership(
        &self,
        ownership: &Arc<crate::raft::ClusterKvRaftCommitSequenceOwnership>,
    ) {
        let mut source = self
            .source
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert!(matches!(self.kind, OwnerKind::Proposal));
        assert_eq!(source.proposal, Some(self.id));
        source.proposal_ownership = Some(Arc::downgrade(ownership));
    }
}

pub fn try_admit_committed_observation(
    paths: &ClusterKvPaths,
    ownership: KvObservationProposalOwnership,
) -> Result<KvObservationCapacityPermit, KvError> {
    admit(queue(paths), ownership)
}

fn admit(
    queue: Arc<Queue>,
    ownership: KvObservationProposalOwnership,
) -> Result<KvObservationCapacityPermit, KvError> {
    let (index, lease) = {
        let mut state = queue
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.drainer_thread == Some(std::thread::current().id()) {
            // Strict FIFO cannot wait for a nested mutation's observers from
            // inside the observer currently blocking that FIFO. Reject before
            // any canonical effect; callbacks project and wake existing owners.
            return Err(KvError::Busy {
                path: queue
                    .root
                    .join("committed-observation-callback")
                    .display()
                    .to_string(),
            });
        }
        let index = state
            .slots
            .iter()
            .position(|slot| matches!(slot, Slot::Free))
            .ok_or_else(|| KvError::ResourceExhausted {
                operation: "committed KV observation capacity admission".to_string(),
                requested_bytes: queue.allowance.unwrap_or(0),
            })?;
        // Reserve ordinal/lease headroom before effects, including every
        // concurrently admitted source that may still commit afterward.
        if state
            .next_ordinal
            .checked_add(state.slots.len() as u64)
            .is_none()
            || state.next_lease == u64::MAX
        {
            return Err(KvError::ResourceExhausted {
                operation: "committed KV observation ordinal admission".to_string(),
                requested_bytes: queue.allowance.unwrap_or(0),
            });
        }
        let lease = state.next_lease;
        state.next_lease += 1;
        state.slots[index] = Slot::Admitted { lease };
        state.reserved_bytes += queue.allowance.unwrap_or(0);
        (index, lease)
    };
    let receipt = Arc::new(Receipt {
        queue: Arc::clone(&queue),
        index,
        lease,
        metadata: OnceLock::new(),
        source: OnceLock::new(),
        delivery: Arc::new(DeliveryStatus::default()),
    });
    let source = Arc::new(Source {
        queue,
        index,
        lease,
        ownership,
        state: Mutex::new(SourceState {
            next_fence: 1,
            storage: None,
            proposal: None,
            proposal_ownership: None,
            committed: None,
            receipt: Some(receipt),
        }),
    });
    let source_state = source
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let _ = source_state
        .receipt
        .as_ref()
        .expect("preallocated receipt")
        .source
        .set(Arc::downgrade(&source));
    drop(source_state);
    Ok(KvObservationCapacityPermit { source })
}

impl KvObservationCapacityPermit {
    pub fn proposal_custody_handle(&self) -> KvObservationProposalCustodyHandle {
        KvObservationProposalCustodyHandle {
            source: Arc::downgrade(&self.source),
            queue: Arc::downgrade(&self.source.queue),
            index: self.source.index,
            lease: self.source.lease,
        }
    }

    pub fn storage_owner(&self) -> Result<KvObservationOwnerFence, KvError> {
        self.owner(OwnerKind::Storage)
    }
    pub fn proposal_owner(&self) -> Result<KvObservationOwnerFence, KvError> {
        self.owner(OwnerKind::Proposal)
    }
    fn owner(&self, kind: OwnerKind) -> Result<KvObservationOwnerFence, KvError> {
        let mut state = self
            .source
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.committed.is_some()
            || state.next_fence == u64::MAX
            || matches!(kind, OwnerKind::Proposal)
                && self.source.ownership == KvObservationProposalOwnership::Absent
        {
            return Err(invalid(
                "observation source cannot acquire this owner fence",
            ));
        }
        if match kind {
            OwnerKind::Storage => state.storage.is_some(),
            OwnerKind::Proposal => state.proposal.is_some(),
        } {
            return Err(invalid("observation source already owns this fence"));
        }
        let retained_proposal = if matches!(kind, OwnerKind::Storage) {
            state.retain_proposal_ownership(&self.source.queue.root)?
        } else {
            None
        };
        let id = state.next_fence;
        state.next_fence += 1;
        match kind {
            OwnerKind::Storage => state.storage = Some(id),
            OwnerKind::Proposal => state.proposal = Some(id),
        }
        Ok(KvObservationOwnerFence {
            source: Arc::clone(&self.source),
            id,
            kind,
            retained_proposal,
        })
    }

    pub fn commit(
        &self,
        previous_revision: KvRevision,
        revision: KvRevision,
        applied_index: Option<u64>,
        kind: KvCommittedObservationKind,
    ) -> Result<KvCommittedObservationReceipt, KvError> {
        self.commit_metadata(Some(previous_revision), Some(revision), applied_index, kind)
    }

    /// Reserve a raw projection repair without decoding or inventing the
    /// corrupt previous revision. Actual successful IO fills terminal metadata.
    pub fn commit_unknown_projection_prestate(
        &self,
    ) -> Result<KvCommittedObservationReceipt, KvError> {
        if self.source.ownership != KvObservationProposalOwnership::Absent {
            return Err(invalid(
                "unknown projection prestate cannot replace native proposal evidence",
            ));
        }
        self.commit_metadata(None, None, None, KvCommittedObservationKind::Replacement)
    }

    fn commit_metadata(
        &self,
        previous_revision: Option<KvRevision>,
        revision: Option<KvRevision>,
        applied_index: Option<u64>,
        kind: KvCommittedObservationKind,
    ) -> Result<KvCommittedObservationReceipt, KvError> {
        let mut source = self
            .source
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if source.committed.is_some() {
            return Err(invalid("observation source already committed"));
        }
        let storage = source
            .storage
            .ok_or_else(|| invalid("committed observation requires an active storage owner"))?;
        let proposal = match self.source.ownership {
            KvObservationProposalOwnership::Required => Some(source.proposal.ok_or_else(|| {
                invalid("committed observation requires an active proposal owner")
            })?),
            KvObservationProposalOwnership::Absent => None,
        };
        let mut state = self
            .source
            .queue
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !matches!(state.slots[self.source.index], Slot::Admitted { lease } if lease == self.source.lease)
        {
            return Err(invalid(
                "committed observation lost its pre-effect reservation",
            ));
        }
        let metadata = Metadata {
            ordinal: state.next_ordinal,
            previous_revision,
            revision,
            applied_index,
            kind,
        };
        state.next_ordinal += 1;
        let receipt = source
            .receipt
            .take()
            .expect("admission preallocates receipt");
        let _ = receipt.metadata.set(Mutex::new(metadata));
        state.slots[self.source.index] = Slot::Committed {
            lease: self.source.lease,
            metadata,
            delivery: Arc::clone(&receipt.delivery),
            storage_released: false,
            proposal_released: proposal.is_none(),
            resolution: None,
            original_current_term_commits: None,
            uncertified_original_requests: Vec::new(),
        };
        source.committed = Some((metadata, storage, proposal));
        Ok(KvCommittedObservationReceipt { receipt })
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        let source = self
            .state
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        if source.committed.is_none() {
            let mut state = self
                .queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if matches!(state.slots[self.index], Slot::Admitted { lease } if lease == self.lease) {
                state.slots[self.index] = Slot::Free;
                state.reserved_bytes = state
                    .reserved_bytes
                    .saturating_sub(self.queue.allowance.unwrap_or(0));
            }
        }
    }
}

impl Drop for KvObservationOwnerFence {
    fn drop(&mut self) {
        let committed = {
            let mut source = self
                .source
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let active = match self.kind {
                OwnerKind::Storage => &mut source.storage,
                OwnerKind::Proposal => &mut source.proposal,
            };
            if *active == Some(self.id) {
                *active = None;
                if matches!(self.kind, OwnerKind::Proposal) {
                    source.proposal_ownership = None;
                }
            }
            source.committed
        };
        if let Some((metadata, storage, proposal)) = committed {
            {
                let mut state = self
                    .source
                    .queue
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if let Slot::Committed {
                    metadata: current,
                    storage_released,
                    proposal_released,
                    ..
                } = &mut state.slots[self.source.index]
                {
                    if current.ordinal == metadata.ordinal {
                        match self.kind {
                            OwnerKind::Storage if self.id == storage => *storage_released = true,
                            OwnerKind::Proposal if Some(self.id) == proposal => {
                                *proposal_released = true
                            }
                            _ => {}
                        }
                    }
                }
            }
            // The actual owner wrapper must drop its real guards BEFORE this
            // fence. Neither source nor queue mutex is held while draining.
            self.source
                .queue
                .drain(self.source.index, self.source.lease);
        }
        // The actual storage owner has already released before this fence.
        // Releasing its last custody reference now drops the actual proposal
        // file before the proposal fence, outside every observer queue lock.
        drop(self.retained_proposal.take());
    }
}

#[derive(Clone)]
pub struct KvCommittedObservationReceipt {
    receipt: Arc<Receipt>,
}

struct Receipt {
    queue: Arc<Queue>,
    index: usize,
    lease: u64,
    metadata: OnceLock<Mutex<Metadata>>,
    source: OnceLock<Weak<Source>>,
    delivery: Arc<DeliveryStatus>,
}

impl KvCommittedObservationReceipt {
    /// Copy actual newly applied DATA while its same resident node is held,
    /// before checkpoint compaction. No proof, callback or authority is created.
    pub(crate) fn capture_original_current_term_requests(
        &self,
        node: &crate::raft::DurableRaftKvNode,
        before_index: u64,
        committed_index: u64,
    ) {
        let mut state = self
            .receipt
            .queue
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if state.original_commit_interests.is_empty() {
            return;
        }
        let entries = node
            .log()
            .iter()
            .filter(|entry| before_index < entry.index && entry.index <= committed_index);
        // A changed native voter configuration cannot inherit this wave's voter
        // fence. Its own existing configuration operation retains its own proof.
        if entries
            .clone()
            .any(|entry| crate::raft::command_may_mutate_cluster_raft_config(&entry.command))
        {
            return;
        }
        let Slot::Committed {
            lease,
            uncertified_original_requests,
            ..
        } = &mut state.slots[self.receipt.index]
        else {
            return;
        };
        if *lease != self.receipt.lease {
            return;
        }
        for entry in entries.filter(|entry| entry.term == node.hard_state().current_term) {
            let Some(committed) = node.store().committed_request(&entry.request_id) else {
                continue;
            };
            if committed.term != entry.term || committed.log_index != entry.index {
                continue;
            }
            let request =
                crate::KvCommandRequest::new(entry.request_id.clone(), entry.command.clone());
            if uncertified_original_requests
                .iter()
                .any(|(prior, _)| prior.request_id == request.request_id)
            {
                continue;
            }
            uncertified_original_requests.push((request, committed.clone()));
        }
    }

    /// Attach only the existing actual wave's certified prefix to its copied
    /// stored requests. Same receipt and exact release latches own every fact.
    pub(crate) fn observe_original_current_term_commits(
        &self,
        node_id: &sinomo_node::NodeId,
        request: &crate::KvCommandRequest,
        committed: &crate::KvCommittedRequestRecord,
        entry: &crate::raft::KvLogEntry,
        authority: crate::KvAuthorityProof,
    ) {
        let mut state = self
            .receipt
            .queue
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if state.original_commit_interests.is_empty() {
            return;
        }
        if !matches!(authority.basis, crate::KvAuthorityBasis::CurrentTermQuorum { voter_acks, required_quorum }
            if required_quorum > 0 && voter_acks >= required_quorum)
            || request.request_id != committed.request_id
            || entry.request_id != request.request_id
            || entry.command != request.command
            || entry.term != committed.term
            || entry.index != committed.log_index
            || authority.term != committed.term
            || authority.commit_index < committed.log_index
            || authority.revision < committed.revision
        {
            return;
        }
        if let Slot::Committed {
            lease,
            metadata,
            original_current_term_commits,
            uncertified_original_requests,
            ..
        } = &mut state.slots[self.receipt.index]
            && *lease == self.receipt.lease
            && metadata.applied_index == Some(authority.commit_index)
            && metadata.revision == Some(authority.revision)
        {
            let mut copied = std::mem::take(uncertified_original_requests);
            if !copied
                .iter()
                .any(|(prior, _)| prior.request_id == request.request_id)
            {
                copied.push((request.clone(), committed.clone()));
            }
            let originals = Arc::make_mut(
                original_current_term_commits.get_or_insert_with(|| Arc::new(Vec::new())),
            );
            for (request, committed) in copied {
                if committed.term != authority.term
                    || committed.log_index > authority.commit_index
                    || committed.revision > authority.revision
                    || originals
                        .iter()
                        .any(|prior| prior.request.request_id == request.request_id)
                {
                    continue;
                }
                originals.push(Arc::new(KvOriginalCurrentTermCommit {
                    root: self.receipt.queue.root.clone(),
                    node_id: node_id.clone(),
                    request,
                    committed,
                    authority,
                }));
            }
        }
    }
    /// Causal preparation evidence for this receipt's own delivered event.
    /// Inspect after completion; neither subscriber presence nor another
    /// publication's preparation can set this flag.
    pub fn was_delivery_prepared(&self) -> bool {
        self.receipt.delivery.prepared.load(Ordering::Acquire)
    }

    /// Await this ordinal's completed callback delivery after staging it and
    /// releasing both exact owners. This is an event wait, never a polling or
    /// proposal/storage critical section. Normal producers call it before
    /// returning their committed reply.
    pub fn wait_delivered(&self) -> Result<(), KvError> {
        self.receipt
            .queue
            .drain(self.receipt.index, self.receipt.lease);
        let mut state = self
            .receipt
            .queue
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        loop {
            match &state.slots[self.receipt.index] {
                Slot::Committed {
                    lease,
                    storage_released,
                    proposal_released,
                    ..
                } if *lease == self.receipt.lease => {
                    if !storage_released || !proposal_released {
                        return Err(invalid(
                            "committed observation delivery wait still owns an exact source fence",
                        ));
                    }
                }
                Slot::Delivering { lease } if *lease == self.receipt.lease => {}
                _ => {
                    let failed_subscribers = self
                        .receipt
                        .delivery
                        .failed_subscribers
                        .load(Ordering::Acquire);
                    if failed_subscribers == 0 {
                        return Ok(());
                    }
                    drop(state);
                    let metadata = self
                        .receipt
                        .metadata
                        .get()
                        .expect("committed metadata")
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    return Err(KvError::CommittedObservationDeliveryFailed {
                        revision: metadata.revision,
                        applied_index: metadata.applied_index,
                        failed_subscribers,
                    });
                }
            }
            if state.drainer_thread == Some(std::thread::current().id()) {
                return Err(invalid(
                    "a committed observer cannot wait on its own serial drainer",
                ));
            }
            state = self
                .receipt
                .queue
                .delivery_wake
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
    }

    /// Extend this same uninterrupted committed owner scope after another
    /// durable effect, before payload collection can fail. No callback runs.
    pub fn advance_committed(
        &self,
        revision: KvRevision,
        applied_index: u64,
    ) -> Result<(), KvError> {
        self.advance_terminal(revision, Some(applied_index))
    }

    /// Record a successful derived projection write without manufacturing a
    /// Raft applied prefix. The provisional receipt keeps the known pre-state
    /// revision if a potentially published write instead fails ambiguously.
    /// A pure replacement preserves the materialized writer's existing ability
    /// to install an older snapshot; this never grants a native applied prefix.
    pub fn advance_projection(&self, revision: KvRevision) -> Result<(), KvError> {
        self.advance_terminal(revision, None)
    }

    fn advance_terminal(
        &self,
        revision: KvRevision,
        applied_index: Option<u64>,
    ) -> Result<(), KvError> {
        let source = self
            .receipt
            .source
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| invalid("committed observation lost its owner scope"))?;
        let owners = source
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let (_, storage, proposal) = owners
            .committed
            .ok_or_else(|| invalid("observation scope has not committed"))?;
        if owners.storage != Some(storage) || proposal.is_some() && owners.proposal != proposal {
            return Err(invalid(
                "committed observation cannot advance after its exact owners released",
            ));
        }
        let mut metadata = self
            .receipt
            .metadata
            .get()
            .expect("committed metadata")
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let projection_replacement = metadata.kind == KvCommittedObservationKind::Replacement
            && metadata.applied_index.is_none()
            && applied_index.is_none();
        if metadata.revision.is_some_and(|current| revision < current) && !projection_replacement
            || metadata
                .applied_index
                .is_some_and(|current| applied_index.is_none_or(|index| index < current))
        {
            return Err(invalid(
                "committed observation terminal metadata cannot regress",
            ));
        }
        let mut state = self
            .receipt
            .queue
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Slot::Committed {
            lease,
            metadata: current,
            resolution: None,
            ..
        } = &mut state.slots[self.receipt.index]
        else {
            return Err(invalid("committed observation payload already resolved"));
        };
        if *lease != self.receipt.lease {
            return Err(invalid("committed observation lease is obsolete"));
        }
        metadata.revision = Some(revision);
        metadata.applied_index = applied_index;
        *current = *metadata;
        Ok(())
    }

    /// Scalar native capture evidence used by publication builders. A restore
    /// already covers this exact prefix; no later applied step occurred.
    pub(crate) fn replacement_covers_native_snapshot(
        &self,
        revision: KvRevision,
        applied_index: u64,
    ) -> bool {
        let metadata = self
            .receipt
            .metadata
            .get()
            .expect("committed receipt metadata")
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        metadata.kind == KvCommittedObservationKind::Replacement
            && metadata.revision == Some(revision)
            && metadata.applied_index == Some(applied_index)
    }

    pub fn ready(&self, payload: Arc<KvCommittedObservationPayload>) -> Result<(), KvError> {
        let metadata = *self
            .receipt
            .metadata
            .get()
            .expect("committed receipt metadata")
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut payload = Some(payload);
        {
            let mut state = self
                .receipt
                .queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let Slot::Committed {
                lease, resolution, ..
            } = &mut state.slots[self.receipt.index]
            else {
                return Ok(());
            };
            if *lease != self.receipt.lease || resolution.is_some() {
                return Ok(());
            }
            let captured = payload.as_ref().expect("pending payload");
            let reason = if let (Some(declared_bytes), Some(allowance_bytes)) =
                (captured.encoded_bytes, self.receipt.queue.allowance)
                && declared_bytes > allowance_bytes
            {
                Some(KvCommittedObservationDiscontinuity::PayloadOverflow {
                    declared_bytes,
                    allowance_bytes,
                })
            } else if self.receipt.queue.allowance.is_some() && captured.encoded_bytes.is_none() {
                Some(KvCommittedObservationDiscontinuity::InconsistentPayload)
            } else if captured.delta.as_ref().is_some_and(|delta| {
                Some(delta.revision) != metadata.revision
                    || metadata
                        .revision
                        .is_none_or(|revision| delta.compacted_revision > revision)
            }) || metadata.kind != KvCommittedObservationKind::Replacement
                && captured.delta.is_none()
                || captured.snapshot.as_ref().is_some_and(|snapshot| {
                    if metadata.kind == KvCommittedObservationKind::Replacement {
                        metadata
                            .revision
                            .is_none_or(|revision| snapshot.revision > revision)
                            || captured.delta.is_none()
                                && Some(snapshot.revision) != metadata.revision
                    } else {
                        Some(snapshot.revision) != metadata.revision
                    }
                })
                || match (
                    captured.snapshot.is_some(),
                    captured.snapshot_applied_index,
                    metadata.applied_index,
                ) {
                    (false, None, _) | (true, None, None) => false,
                    (true, Some(index), Some(terminal)) => {
                        if metadata.kind == KvCommittedObservationKind::Replacement {
                            index > terminal || captured.delta.is_none() && index != terminal
                        } else {
                            index != terminal
                        }
                    }
                    _ => true,
                }
                || metadata.kind != KvCommittedObservationKind::Contiguous
                    && captured.snapshot.is_none()
            {
                Some(KvCommittedObservationDiscontinuity::InconsistentPayload)
            } else {
                None
            };
            *resolution = Some(match reason {
                Some(reason) => Resolution::Gap(reason),
                None => Resolution::Payload(payload.take().expect("pending payload")),
            });
        }
        // An oversized/inconsistent capture is never retained while its typed
        // gap callback runs, and is never converted into an empty delta.
        drop(payload);
        self.receipt
            .queue
            .drain(self.receipt.index, self.receipt.lease);
        Ok(())
    }

    /// Resolve an uncertain effect while retaining the ordered acknowledgement
    /// handle. The known metadata is a hint only; a gap grants no authority.
    /// Exact owner release latches still prevent callback delivery under locks.
    pub fn abandon_publication(&self) -> Result<(), KvError> {
        {
            let mut state = self
                .receipt
                .queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Slot::Committed {
                lease, resolution, ..
            } = &mut state.slots[self.receipt.index]
                && *lease == self.receipt.lease
                && resolution.is_none()
            {
                *resolution = Some(Resolution::Gap(
                    KvCommittedObservationDiscontinuity::PublicationAbandoned,
                ));
            }
        }
        self.receipt
            .queue
            .drain(self.receipt.index, self.receipt.lease);
        Ok(())
    }

    /// Resolve a definitely unchanged pre-write attempt without publishing a
    /// gap or manufactured empty payload. Ambiguous I/O errors must abandon
    /// publication instead. Release latches and ordered completion still apply.
    pub fn no_effect(&self) -> Result<(), KvError> {
        {
            let mut state = self
                .receipt
                .queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let Slot::Committed {
                lease, resolution, ..
            } = &mut state.slots[self.receipt.index]
            else {
                return Err(invalid("cannot retract an already delivered observation"));
            };
            if *lease != self.receipt.lease {
                return Err(invalid("observation lease already retired"));
            }
            if resolution.is_some() {
                return if matches!(resolution, Some(Resolution::NoEffect)) {
                    Ok(())
                } else {
                    Err(invalid("observation publication already resolved"))
                };
            }
            *resolution = Some(Resolution::NoEffect);
        }
        self.receipt
            .queue
            .drain(self.receipt.index, self.receipt.lease);
        Ok(())
    }
}

impl Drop for Receipt {
    fn drop(&mut self) {
        if self.metadata.get().is_none() {
            return;
        }
        {
            let mut state = self
                .queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Slot::Committed {
                lease, resolution, ..
            } = &mut state.slots[self.index]
            {
                if *lease == self.lease && resolution.is_none() {
                    *resolution = Some(Resolution::Gap(
                        KvCommittedObservationDiscontinuity::PublicationAbandoned,
                    ));
                }
            }
        }
        // Active exact release latches keep this harmless under durable locks.
        // If both owners already released, cancellation itself is the final
        // reactive wake, including a dropped last receipt on an idle queue.
        self.queue.drain(self.index, self.lease);
    }
}

pub struct KvCommittedObservationSubscription {
    queue: Arc<Queue>,
    id: u64,
}

/// Observe committed items in source order. Callbacks project captured state
/// and wake existing owners; a synchronous mutation of this same root is
/// rejected at pre-effect admission because it cannot wait on its own FIFO.
/// Native local-floor and established ReadIndex reads need no mutation receipt.
pub fn subscribe_committed_observations(
    paths: &ClusterKvPaths,
    callback: impl Fn(&KvCommittedObservation) + Send + Sync + 'static,
) -> KvCommittedObservationSubscription {
    let queue = queue(paths);
    let id = {
        let mut state = queue
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let id = state.next_subscription;
        state.next_subscription += 1;
        state.subscribers.insert(id, Arc::new(callback));
        id
    };
    KvCommittedObservationSubscription { queue, id }
}

impl KvCommittedObservationSubscription {
    /// Opt this SAME lifecycle-owned subscription into copying original producer
    /// facts. Ordinary subscriptions copy none. This grants no KV authority.
    pub fn include_original_current_term_commits(&self, enabled: bool) {
        let mut state = self
            .queue
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if enabled && state.subscribers.contains_key(&self.id) {
            state.original_commit_interests.insert(self.id);
        } else {
            state.original_commit_interests.remove(&self.id);
        }
    }
}

impl Drop for KvCommittedObservationSubscription {
    fn drop(&mut self) {
        let removed = {
            let mut state = self
                .queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.original_commit_interests.remove(&self.id);
            state.subscribers.remove(&self.id)
        };
        // Callback captures may have arbitrary reentrant destructors.
        drop(removed);
    }
}

fn invalid(detail: &str) -> KvError {
    KvError::Serialization {
        detail: detail.to_string(),
    }
}

impl std::fmt::Debug for KvObservationCapacityPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KvObservationCapacityPermit")
            .field("root", &self.source.queue.root)
            .field("slot", &self.source.index)
            .field("lease", &self.source.lease)
            .finish()
    }
}

impl std::fmt::Debug for KvObservationOwnerFence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KvObservationOwnerFence")
            .field("owner", &self.kind)
            .field("fence", &self.id)
            .field("lease", &self.source.lease)
            .finish()
    }
}

impl std::fmt::Debug for KvCommittedObservationReceipt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KvCommittedObservationReceipt")
            .field("root", &self.receipt.queue.root)
            .field("slot", &self.receipt.index)
            .field("lease", &self.receipt.lease)
            .finish()
    }
}

impl PartialEq for KvCommittedObservationReceipt {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.receipt, &other.receipt)
    }
}
impl Eq for KvCommittedObservationReceipt {}

impl std::fmt::Debug for KvCommittedObservationSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KvCommittedObservationSubscription")
            .field("root", &self.queue.root)
            .field("subscription", &self.id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    fn test_queue(slots: usize, allowance: Option<usize>) -> Arc<Queue> {
        Arc::new(Queue::new(
            PathBuf::from("isolated-observation-tests"),
            slots,
            allowance,
        ))
    }

    fn subscribe(
        queue: &Arc<Queue>,
        observer: impl Fn(&KvCommittedObservation) + Send + Sync + 'static,
    ) -> KvCommittedObservationSubscription {
        let mut state = queue.state.lock().unwrap();
        let id = state.next_subscription;
        state.next_subscription += 1;
        state.subscribers.insert(id, Arc::new(observer));
        KvCommittedObservationSubscription {
            queue: Arc::clone(queue),
            id,
        }
    }

    fn payload(
        revision: u64,
        snapshot_revision: Option<u64>,
        encoded_bytes: Option<usize>,
    ) -> Arc<KvCommittedObservationPayload> {
        Arc::new(KvCommittedObservationPayload {
            delta: Some(Arc::new(ClusterKvCommittedDelta {
                revision: KvRevision(revision),
                compacted_revision: KvRevision(0),
                events: Vec::new().into(),
                final_key_states: Vec::new().into(),
            })),
            snapshot: snapshot_revision.map(|revision| {
                let mut snapshot = crate::InProcessKvStore::new().try_snapshot().unwrap();
                snapshot.revision = KvRevision(revision);
                Arc::new(snapshot)
            }),
            snapshot_applied_index: snapshot_revision.map(|revision| revision * 10),
            encoded_bytes,
        })
    }

    fn committed(
        queue: &Arc<Queue>,
        revision: u64,
        kind: KvCommittedObservationKind,
    ) -> (
        KvObservationCapacityPermit,
        KvObservationOwnerFence,
        KvCommittedObservationReceipt,
    ) {
        let permit = admit(Arc::clone(queue), KvObservationProposalOwnership::Absent).unwrap();
        let storage = permit.storage_owner().unwrap();
        let receipt = permit
            .commit(
                KvRevision(revision - 1),
                KvRevision(revision),
                Some(revision * 10),
                kind,
            )
            .unwrap();
        (permit, storage, receipt)
    }

    #[test]
    fn later_ready_waits_for_earlier_publication_and_cloned_ready_is_once() {
        let queue = test_queue(3, None);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&seen);
        let _subscription = subscribe(&queue, move |event| {
            output.lock().unwrap().push(event.revision.unwrap().0)
        });
        let (_first_permit, first_storage, first) =
            committed(&queue, 1, KvCommittedObservationKind::Contiguous);
        let (_second_permit, second_storage, second) =
            committed(&queue, 2, KvCommittedObservationKind::Contiguous);
        drop(first_storage);
        drop(second_storage);
        second.ready(payload(2, None, None)).unwrap();
        assert!(seen.lock().unwrap().is_empty());
        let retry = first.clone();
        first.ready(payload(1, None, None)).unwrap();
        retry.ready(payload(1, None, None)).unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![1, 2]);
    }

    #[test]
    fn callbacks_wait_for_both_actual_owners_and_abandoned_receipt_unblocks_next() {
        let queue = test_queue(3, None);
        let storage_mutex = Arc::new(Mutex::new(()));
        let proposal_mutex = Arc::new(Mutex::new(()));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&seen);
        let check_storage = Arc::clone(&storage_mutex);
        let check_proposal = Arc::clone(&proposal_mutex);
        let _subscription = subscribe(&queue, move |event| {
            assert!(check_storage.try_lock().is_ok());
            assert!(check_proposal.try_lock().is_ok());
            output.lock().unwrap().push(event.clone());
        });
        let first_permit =
            admit(Arc::clone(&queue), KvObservationProposalOwnership::Required).unwrap();
        let actual_storage = storage_mutex.lock().unwrap();
        let first_storage = first_permit.storage_owner().unwrap();
        let actual_proposal = proposal_mutex.lock().unwrap();
        let first_proposal = first_permit.proposal_owner().unwrap();
        let first = first_permit
            .commit(
                KvRevision(0),
                KvRevision(1),
                Some(10),
                KvCommittedObservationKind::Contiguous,
            )
            .unwrap();
        assert!(
            first_permit.storage_owner().is_err(),
            "a committed scope is sealed"
        );
        let clone = first.clone();
        drop(first);
        assert!(seen.lock().unwrap().is_empty());
        drop(clone);
        assert!(seen.lock().unwrap().is_empty());
        let (_second_permit, second_storage, second) =
            committed(&queue, 2, KvCommittedObservationKind::Contiguous);
        drop(second_storage);
        second.ready(payload(2, None, None)).unwrap();
        assert!(seen.lock().unwrap().is_empty());
        drop(actual_storage);
        drop(first_storage);
        assert!(seen.lock().unwrap().is_empty());
        drop(actual_proposal);
        drop(first_proposal);
        let events = seen.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[0].value,
            KvCommittedObservationValue::Discontinuity(
                KvCommittedObservationDiscontinuity::PublicationAbandoned
            )
        ));
        assert!(matches!(
            events[1].value,
            KvCommittedObservationValue::Delta(_)
        ));
    }

    #[test]
    fn unrelated_eligible_head_is_not_drained_by_a_still_locked_or_obsolete_source() {
        let queue = test_queue(3, None);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&seen);
        let _subscription = subscribe(&queue, move |event| {
            output.lock().unwrap().push(event.revision.unwrap())
        });
        let (_first_permit, first_storage, first) =
            committed(&queue, 1, KvCommittedObservationKind::Contiguous);
        drop(first_storage);
        // Stage a ready older head without activating, reproducing the narrow
        // preemption between resolving a payload and its after-release wake.
        if let Slot::Committed { resolution, .. } =
            &mut queue.state.lock().unwrap().slots[first.receipt.index]
        {
            *resolution = Some(Resolution::Payload(payload(1, None, None)));
        }
        let permit = admit(Arc::clone(&queue), KvObservationProposalOwnership::Required).unwrap();
        let storage = permit.storage_owner().unwrap();
        let proposal = permit.proposal_owner().unwrap();
        let receipt = permit
            .commit(
                KvRevision(1),
                KvRevision(2),
                Some(20),
                KvCommittedObservationKind::Contiguous,
            )
            .unwrap();
        drop(receipt);
        assert!(seen.lock().unwrap().is_empty());
        drop(storage);
        assert!(
            seen.lock().unwrap().is_empty(),
            "remaining proposal fence excludes drain initiation"
        );
        drop(proposal);
        assert_eq!(*seen.lock().unwrap(), vec![KvRevision(1), KvRevision(2)]);
        let old = first.clone();
        let (_next_permit, next_storage, next) =
            committed(&queue, 3, KvCommittedObservationKind::Contiguous);
        drop(old);
        assert_eq!(
            seen.lock().unwrap().len(),
            2,
            "obsolete lease cannot activate unrelated work"
        );
        drop(next);
        drop(next_storage);
        assert_eq!(seen.lock().unwrap().len(), 3);
    }

    #[test]
    fn precommit_drop_releases_capacity_without_a_gap_and_commit_requires_exact_owners() {
        let queue = test_queue(1, None);
        let permit = admit(Arc::clone(&queue), KvObservationProposalOwnership::Required).unwrap();
        assert!(
            permit
                .commit(
                    KvRevision(0),
                    KvRevision(1),
                    Some(10),
                    KvCommittedObservationKind::Contiguous
                )
                .is_err()
        );
        let storage = permit.storage_owner().unwrap();
        assert!(
            permit
                .commit(
                    KvRevision(0),
                    KvRevision(1),
                    Some(10),
                    KvCommittedObservationKind::Contiguous
                )
                .is_err()
        );
        assert!(admit(Arc::clone(&queue), KvObservationProposalOwnership::Absent).is_err());
        drop(storage);
        drop(permit);
        let next = admit(Arc::clone(&queue), KvObservationProposalOwnership::Absent).unwrap();
        drop(next);
        let state = queue.state.lock().unwrap();
        assert_eq!(state.next_ordinal, 1);
        assert!(matches!(state.slots[0], Slot::Free));
    }

    #[test]
    fn blocked_callback_retains_capacity_but_admission_never_waits_for_it() {
        let queue = test_queue(2, None);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let _subscription = subscribe(&queue, move |_| {
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        });
        let (_permit, storage, receipt) =
            committed(&queue, 1, KvCommittedObservationKind::Contiguous);
        drop(storage);
        let worker = std::thread::spawn(move || receipt.ready(payload(1, None, None)).unwrap());
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let pending = admit(Arc::clone(&queue), KvObservationProposalOwnership::Absent).unwrap();
        assert!(admit(Arc::clone(&queue), KvObservationProposalOwnership::Absent).is_err());
        drop(pending);
        assert!(admit(Arc::clone(&queue), KvObservationProposalOwnership::Absent).is_ok());
        release_tx.send(()).unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn replacement_carries_snapshot_then_appended_delta_with_exact_phase_metadata() {
        let queue = test_queue(2, None);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&seen);
        let _subscription = subscribe(&queue, move |event| {
            output.lock().unwrap().push(event.clone())
        });
        let (_permit, storage, receipt) =
            committed(&queue, 1, KvCommittedObservationKind::Replacement);
        receipt.advance_committed(KvRevision(2), 20).unwrap();
        drop(storage);
        assert!(receipt.advance_committed(KvRevision(3), 30).is_err());
        receipt.ready(payload(2, Some(1), None)).unwrap();
        let events = seen.lock().unwrap();
        assert!(matches!(
            events[0].value,
            KvCommittedObservationValue::Snapshot(_)
        ));
        assert_eq!(
            (events[0].revision.unwrap(), events[0].applied_index),
            (KvRevision(1), Some(10))
        );
        assert!(matches!(
            events[1].value,
            KvCommittedObservationValue::Delta(_)
        ));
        assert_eq!(
            (
                events[1].previous_revision.unwrap(),
                events[1].revision.unwrap(),
                events[1].applied_index
            ),
            (KvRevision(1), KvRevision(2), Some(20))
        );
        drop(events);
        let (_permit, storage, receipt) =
            committed(&queue, 3, KvCommittedObservationKind::Checkpoint);
        drop(storage);
        receipt.ready(payload(3, Some(3), None)).unwrap();
        let events = seen.lock().unwrap();
        assert!(matches!(
            events[2].value,
            KvCommittedObservationValue::Delta(_)
        ));
        assert!(matches!(
            events[3].value,
            KvCommittedObservationValue::Snapshot(_)
        ));
    }

    #[test]
    fn pure_snapshot_replacement_emits_no_synthetic_delta() {
        let queue = test_queue(1, None);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&seen);
        let _subscription = subscribe(&queue, move |event| {
            output.lock().unwrap().push(event.clone())
        });
        let (_permit, storage, receipt) =
            committed(&queue, 1, KvCommittedObservationKind::Replacement);
        let mut capture = (*payload(1, Some(1), None)).clone();
        capture.delta = None;
        drop(storage);
        receipt.ready(Arc::new(capture)).unwrap();
        receipt.wait_delivered().unwrap();
        let events = seen.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0].value,
            KvCommittedObservationValue::Snapshot(_)
        ));
        assert_eq!(
            (events[0].revision.unwrap(), events[0].applied_index),
            (KvRevision(1), Some(10))
        );
    }

    #[test]
    fn projection_replacement_preserves_unknown_applied_index() {
        let queue = test_queue(1, None);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&seen);
        let _subscription = subscribe(&queue, move |event| {
            output.lock().unwrap().push(event.clone())
        });
        let permit = admit(Arc::clone(&queue), KvObservationProposalOwnership::Absent).unwrap();
        let storage = permit.storage_owner().unwrap();
        let receipt = permit
            .commit(
                KvRevision(0),
                KvRevision(1),
                None,
                KvCommittedObservationKind::Replacement,
            )
            .unwrap();
        let mut capture = (*payload(1, Some(1), None)).clone();
        capture.delta = None;
        capture.snapshot_applied_index = None;
        drop(storage);
        receipt.ready(Arc::new(capture)).unwrap();
        receipt.wait_delivered().unwrap();
        let events = seen.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].applied_index, None,
            "a derived projection does not fabricate a Raft prefix"
        );
        assert!(matches!(
            events[0].value,
            KvCommittedObservationValue::Snapshot(_)
        ));
    }

    #[test]
    fn projection_success_advances_unknown_index_but_failed_publish_keeps_prestate_hint() {
        let queue = test_queue(3, None);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&seen);
        let _subscription = subscribe(&queue, move |event| {
            output.lock().unwrap().push(event.clone())
        });
        let permit = admit(Arc::clone(&queue), KvObservationProposalOwnership::Absent).unwrap();
        let storage = permit.storage_owner().unwrap();
        let receipt = permit
            .commit(
                KvRevision(1),
                KvRevision(1),
                None,
                KvCommittedObservationKind::Replacement,
            )
            .unwrap();
        receipt.advance_projection(KvRevision(2)).unwrap();
        let mut capture = (*payload(2, Some(2), None)).clone();
        capture.delta = None;
        capture.snapshot_applied_index = None;
        drop(storage);
        receipt.ready(Arc::new(capture)).unwrap();
        receipt.wait_delivered().unwrap();
        let failed = admit(Arc::clone(&queue), KvObservationProposalOwnership::Absent).unwrap();
        let storage = failed.storage_owner().unwrap();
        let receipt = failed
            .commit(
                KvRevision(2),
                KvRevision(2),
                None,
                KvCommittedObservationKind::Replacement,
            )
            .unwrap();
        drop(receipt);
        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "unknown publication cannot escape an active storage owner"
        );
        drop(storage);
        let events = seen.lock().unwrap();
        assert_eq!(events[0].revision, Some(KvRevision(2)));
        assert_eq!(events[0].applied_index, None);
        assert_eq!(
            events[1].revision,
            Some(KvRevision(2)),
            "failure does not invent the attempted future revision"
        );
        assert_eq!(events[1].applied_index, None);
        assert!(matches!(
            events[1].value,
            KvCommittedObservationValue::Discontinuity(
                KvCommittedObservationDiscontinuity::PublicationAbandoned
            )
        ));
        drop(events);
        let (_permit, native_storage, native) =
            committed(&queue, 3, KvCommittedObservationKind::Contiguous);
        assert!(
            native.advance_projection(KvRevision(4)).is_err(),
            "known native prefix cannot become unknown"
        );
        drop(native);
        drop(native_storage);
    }

    #[test]
    fn pure_projection_replacement_can_install_an_older_snapshot_without_native_prefix() {
        let queue = test_queue(1, None);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&seen);
        let _subscription = subscribe(&queue, move |event| {
            output.lock().unwrap().push(event.clone())
        });
        let permit = admit(Arc::clone(&queue), KvObservationProposalOwnership::Absent).unwrap();
        let storage = permit.storage_owner().unwrap();
        let receipt = permit
            .commit(
                KvRevision(10),
                KvRevision(10),
                None,
                KvCommittedObservationKind::Replacement,
            )
            .unwrap();
        receipt.advance_projection(KvRevision(5)).unwrap();
        let mut capture = (*payload(5, Some(5), None)).clone();
        capture.delta = None;
        capture.snapshot_applied_index = None;
        drop(storage);
        receipt.ready(Arc::new(capture)).unwrap();
        receipt.wait_delivered().unwrap();
        let events = seen.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            (
                events[0].previous_revision.unwrap(),
                events[0].revision.unwrap(),
                events[0].applied_index
            ),
            (KvRevision(10), KvRevision(5), None)
        );
        assert!(matches!(
            events[0].value,
            KvCommittedObservationValue::Snapshot(_)
        ));
    }

    #[test]
    fn explicit_uncertain_publication_can_wait_for_gap_without_dropping_its_ack_handle() {
        let queue = test_queue(1, None);
        let gaps = Arc::new(AtomicUsize::new(0));
        let output = Arc::clone(&gaps);
        let _subscription = subscribe(&queue, move |event| {
            assert!(matches!(
                event.value,
                KvCommittedObservationValue::Discontinuity(
                    KvCommittedObservationDiscontinuity::PublicationAbandoned
                )
            ));
            output.fetch_add(1, Ordering::Relaxed);
        });
        let (_permit, storage, receipt) =
            committed(&queue, 1, KvCommittedObservationKind::Replacement);
        receipt.abandon_publication().unwrap();
        assert_eq!(gaps.load(Ordering::Relaxed), 0);
        drop(storage);
        receipt.wait_delivered().unwrap();
        receipt.abandon_publication().unwrap();
        assert_eq!(gaps.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn corrupt_projection_repair_reports_unknown_prestate_and_actual_success_revision() {
        let queue = test_queue(2, None);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&seen);
        let _subscription = subscribe(&queue, move |event| {
            output.lock().unwrap().push(event.clone())
        });
        let permit = admit(Arc::clone(&queue), KvObservationProposalOwnership::Absent).unwrap();
        let storage = permit.storage_owner().unwrap();
        let failed = permit.commit_unknown_projection_prestate().unwrap();
        failed.abandon_publication().unwrap();
        drop(storage);
        failed.wait_delivered().unwrap();
        let permit = admit(Arc::clone(&queue), KvObservationProposalOwnership::Absent).unwrap();
        let storage = permit.storage_owner().unwrap();
        let repaired = permit.commit_unknown_projection_prestate().unwrap();
        repaired.advance_projection(KvRevision(5)).unwrap();
        let mut capture = (*payload(5, Some(5), None)).clone();
        capture.delta = None;
        capture.snapshot_applied_index = None;
        drop(storage);
        repaired.ready(Arc::new(capture)).unwrap();
        repaired.wait_delivered().unwrap();
        let events = seen.lock().unwrap();
        assert_eq!(
            (
                events[0].previous_revision,
                events[0].revision,
                events[0].applied_index
            ),
            (None, None, None)
        );
        assert!(matches!(
            events[0].value,
            KvCommittedObservationValue::Discontinuity(_)
        ));
        assert_eq!(
            (
                events[1].previous_revision,
                events[1].revision,
                events[1].applied_index
            ),
            (None, Some(KvRevision(5)), None)
        );
        assert!(matches!(
            events[1].value,
            KvCommittedObservationValue::Snapshot(_)
        ));
    }

    #[test]
    fn definitely_unchanged_attempt_skips_without_a_gap_or_synthetic_payload() {
        let queue = test_queue(3, None);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&seen);
        let _subscription = subscribe(&queue, move |event| {
            output.lock().unwrap().push(event.revision.unwrap())
        });
        let (_first_permit, first_storage, first) =
            committed(&queue, 1, KvCommittedObservationKind::Contiguous);
        let noop_permit =
            admit(Arc::clone(&queue), KvObservationProposalOwnership::Absent).unwrap();
        let noop_storage = noop_permit.storage_owner().unwrap();
        let noop = noop_permit
            .commit(
                KvRevision(1),
                KvRevision(1),
                None,
                KvCommittedObservationKind::Replacement,
            )
            .unwrap();
        let (_third_permit, third_storage, third) =
            committed(&queue, 2, KvCommittedObservationKind::Contiguous);
        noop.no_effect().unwrap();
        drop(first_storage);
        drop(noop_storage);
        drop(third_storage);
        third.ready(payload(2, None, None)).unwrap();
        assert!(seen.lock().unwrap().is_empty());
        first.ready(payload(1, None, None)).unwrap();
        noop.wait_delivered().unwrap();
        third.wait_delivered().unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![KvRevision(1), KvRevision(2)]);
    }

    #[test]
    fn optional_known_byte_policy_emits_gap_without_retaining_oversize_payload_or_failing_reply() {
        let queue = test_queue(1, Some(4));
        let capture = payload(1, None, Some(5));
        let weak = Arc::downgrade(&capture);
        let seen = Arc::new(AtomicBool::new(false));
        let output = Arc::clone(&seen);
        let _subscription = subscribe(&queue, move |event| {
            assert!(weak.upgrade().is_none());
            assert!(matches!(
                event.value,
                KvCommittedObservationValue::Discontinuity(
                    KvCommittedObservationDiscontinuity::PayloadOverflow {
                        declared_bytes: 5,
                        allowance_bytes: 4
                    }
                )
            ));
            output.store(true, Ordering::SeqCst);
        });
        let (_permit, storage, receipt) =
            committed(&queue, 1, KvCommittedObservationKind::Contiguous);
        drop(storage);
        assert!(receipt.ready(capture).is_ok());
        assert!(seen.load(Ordering::SeqCst));
        assert_eq!(queue.state.lock().unwrap().reserved_bytes, 0);
    }

    #[test]
    fn callback_nested_admission_rejects_before_effect_and_preadmitted_ready_is_serial() {
        let queue = test_queue(3, None);
        let _panicking = subscribe(&queue, |_| panic!("injected observer panic"));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&seen);
        let weak = Arc::downgrade(&queue);
        let once = AtomicBool::new(false);
        let (_first_permit, first_storage, first) =
            committed(&queue, 1, KvCommittedObservationKind::Contiguous);
        let (_second_permit, second_storage, second) =
            committed(&queue, 2, KvCommittedObservationKind::Contiguous);
        drop(first_storage);
        drop(second_storage);
        let staged = second.clone();
        let _subscription = subscribe(&queue, move |event| {
            output.lock().unwrap().push(event.revision.unwrap());
            if !once.swap(true, Ordering::SeqCst) {
                let queue = weak.upgrade().unwrap();
                assert!(matches!(
                    admit(Arc::clone(&queue), KvObservationProposalOwnership::Absent),
                    Err(KvError::Busy { .. })
                ));
                assert_eq!(
                    queue.state.lock().unwrap().next_ordinal,
                    3,
                    "nested mutation was rejected before an effect or ordinal"
                );
                staged.ready(payload(2, None, None)).unwrap();
            }
        });
        first.ready(payload(1, None, None)).unwrap();
        assert!(matches!(
            second.wait_delivered(),
            Err(KvError::CommittedObservationDeliveryFailed {
                failed_subscribers: 1,
                ..
            })
        ));
        assert_eq!(*seen.lock().unwrap(), vec![KvRevision(1), KvRevision(2)]);
        assert!(!queue.state.lock().unwrap().draining);
    }

    #[test]
    fn failed_subscriber_receives_gap_and_own_ack_fails_while_other_fifo_continues() {
        let queue = test_queue(2, None);
        let ready = Arc::new(AtomicBool::new(true));
        let withdrawn = Arc::clone(&ready);
        let gaps = Arc::new(AtomicUsize::new(0));
        let observed_gaps = Arc::clone(&gaps);
        let _failed = subscribe(&queue, move |event| match event.value {
            KvCommittedObservationValue::Delta(_) => panic!("projection application failed"),
            KvCommittedObservationValue::Discontinuity(
                KvCommittedObservationDiscontinuity::SubscriberFailed { .. },
            ) => {
                withdrawn.store(false, Ordering::Release);
                observed_gaps.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        });
        let seen = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&seen);
        let _good = subscribe(&queue, move |event| {
            assert!(matches!(event.value, KvCommittedObservationValue::Delta(_)));
            output.lock().unwrap().push(event.revision.unwrap());
        });
        for revision in 1..=2 {
            let (_permit, storage, receipt) =
                committed(&queue, revision, KvCommittedObservationKind::Contiguous);
            drop(storage);
            receipt.ready(payload(revision, None, None)).unwrap();
            assert!(
                matches!(receipt.wait_delivered(), Err(KvError::CommittedObservationDeliveryFailed {
                revision: Some(KvRevision(actual)), applied_index: Some(index), failed_subscribers: 1,
            }) if actual == revision && index == revision * 10)
            );
        }
        assert!(!ready.load(Ordering::Acquire));
        assert_eq!(gaps.load(Ordering::Relaxed), 2);
        assert_eq!(*seen.lock().unwrap(), vec![KvRevision(1), KvRevision(2)]);
        assert_eq!(queue.state.lock().unwrap().next_delivery, 3);
    }

    #[test]
    fn event_preparation_runs_once_before_all_consumers() {
        let queue = test_queue(1, None);
        let prepared = Arc::new(AtomicUsize::new(0));
        let mut subscriptions = Vec::new();
        for _ in 0..2 {
            let prepared = Arc::clone(&prepared);
            subscriptions.push(subscribe(&queue, move |event| {
                event.prepare_once(|| {
                    prepared.fetch_add(1, Ordering::SeqCst);
                });
                assert_eq!(prepared.load(Ordering::SeqCst), 1);
            }));
        }
        let (_permit, storage, receipt) =
            committed(&queue, 1, KvCommittedObservationKind::Contiguous);
        drop(storage);
        receipt.ready(payload(1, None, None)).unwrap();
        receipt.wait_delivered().unwrap();
        assert_eq!(prepared.load(Ordering::SeqCst), 1);
        assert!(receipt.was_delivery_prepared());
    }

    #[test]
    fn delivery_preparation_evidence_belongs_to_own_completed_receipt() {
        let queue = test_queue(2, None);
        let subscription = subscribe(&queue, |event| event.prepare_once(|| {}));
        let (_permit, storage, prepared) =
            committed(&queue, 1, KvCommittedObservationKind::Contiguous);
        assert!(!prepared.was_delivery_prepared());
        drop(storage);
        prepared.ready(payload(1, None, None)).unwrap();
        prepared.wait_delivered().unwrap();
        assert!(prepared.was_delivery_prepared());
        drop(subscription);
        let _observer_without_preparation = subscribe(&queue, |_| {});
        let (_permit, storage, unprepared) =
            committed(&queue, 2, KvCommittedObservationKind::Contiguous);
        drop(storage);
        unprepared.ready(payload(2, None, None)).unwrap();
        unprepared.wait_delivered().unwrap();
        assert!(
            !unprepared.was_delivery_prepared(),
            "another receipt's successful preparer is no evidence for this receipt"
        );
    }

    #[test]
    fn unwound_preparer_is_not_delivery_preparation_evidence() {
        let queue = test_queue(1, None);
        let _observer = subscribe(&queue, |event| {
            if matches!(event.value, KvCommittedObservationValue::Delta(_)) {
                event.prepare_once(|| panic!("injected preparation failure"));
            }
        });
        let (_permit, storage, receipt) =
            committed(&queue, 1, KvCommittedObservationKind::Contiguous);
        drop(storage);
        receipt.ready(payload(1, None, None)).unwrap();
        assert!(receipt.wait_delivered().is_err());
        assert!(!receipt.was_delivery_prepared());
    }

    #[test]
    fn external_later_producer_waits_until_its_own_callbacks_finish() {
        let queue = test_queue(3, None);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let second_delivered = Arc::new(AtomicBool::new(false));
        let delivered = Arc::clone(&second_delivered);
        let _subscription = subscribe(&queue, move |event| {
            if event.revision == Some(KvRevision(1)) {
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            } else if event.revision == Some(KvRevision(2)) {
                delivered.store(true, Ordering::Release);
            }
        });
        let (_first_permit, first_storage, first) =
            committed(&queue, 1, KvCommittedObservationKind::Contiguous);
        let (_second_permit, second_storage, second) =
            committed(&queue, 2, KvCommittedObservationKind::Contiguous);
        drop(first_storage);
        drop(second_storage);
        let (staged_tx, staged_rx) = mpsc::channel();
        let (returned_tx, returned_rx) = mpsc::channel();
        let waited = Arc::clone(&second_delivered);
        let second_worker = std::thread::spawn(move || {
            second.ready(payload(2, None, None)).unwrap();
            staged_tx.send(()).unwrap();
            second.wait_delivered().unwrap();
            assert!(waited.load(Ordering::Acquire));
            returned_tx.send(()).unwrap();
        });
        staged_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(returned_rx.try_recv().is_err());
        let first_worker = std::thread::spawn(move || {
            first.ready(payload(1, None, None)).unwrap();
            first.wait_delivered().unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            returned_rx.try_recv().is_err(),
            "later KV reply cannot precede its own callback"
        );
        assert!(!second_delivered.load(Ordering::Acquire));
        release_tx.send(()).unwrap();
        returned_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        first_worker.join().unwrap();
        second_worker.join().unwrap();
    }

    #[test]
    fn callback_capture_teardown_panic_does_not_park_already_ready_following_items() {
        struct PanicOnDrop;
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("injected callback capture teardown");
            }
        }
        let queue = test_queue(3, None);
        let holder = Arc::new(Mutex::new(None::<KvCommittedObservationSubscription>));
        let captured_holder = Arc::clone(&holder);
        let captured = PanicOnDrop;
        let subscription = subscribe(&queue, move |_| {
            let _keep_captured = &captured;
            drop(captured_holder.lock().unwrap().take());
        });
        *holder.lock().unwrap() = Some(subscription);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&seen);
        let _subscription = subscribe(&queue, move |event| {
            output.lock().unwrap().push(event.revision.unwrap())
        });
        let (_permit, first_storage, first) =
            committed(&queue, 1, KvCommittedObservationKind::Contiguous);
        let (_permit, second_storage, second) =
            committed(&queue, 2, KvCommittedObservationKind::Contiguous);
        drop(first_storage);
        drop(second_storage);
        second.ready(payload(2, None, None)).unwrap();
        first.ready(payload(1, None, None)).unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![KvRevision(1), KvRevision(2)]);
        assert!(!queue.state.lock().unwrap().draining);
        assert!(holder.lock().unwrap().is_none());
    }
}
