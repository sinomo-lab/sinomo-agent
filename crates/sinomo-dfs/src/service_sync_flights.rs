//! Share only a live metadata barrier for the same authority, participants and
//! authenticated prefix. Completed results never become a proof cache.
use crate::service_replica::DfsMetadataCommitRequirement;
use crate::{
    DfsCancellationFlag, DfsError, DfsMetadataQuorumSyncCertificate, DfsMetadataQuorumSyncTarget,
};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

type Outcome = Result<DfsMetadataQuorumSyncCertificate, DfsError>;

#[derive(Debug, Default)]
pub(crate) struct DfsMetadataSyncFlights(Mutex<Vec<Arc<Flight>>>);

#[derive(Debug)]
pub(crate) struct Flight {
    target: DfsMetadataQuorumSyncTarget,
    requirement: DfsMetadataCommitRequirement,
    result: Mutex<Option<Outcome>>,
    ready: Condvar,
    #[cfg(test)]
    joined: std::sync::atomic::AtomicUsize,
}

pub(crate) enum Admission {
    Lead(Leader),
    Join(Arc<Flight>),
}

pub(crate) struct Leader {
    owner: Arc<DfsMetadataSyncFlights>,
    flight: Arc<Flight>,
    published: bool,
}

impl DfsMetadataSyncFlights {
    #[cfg(test)]
    pub(crate) fn live_waiters(&self) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|f| f.joined.load(std::sync::atomic::Ordering::Relaxed))
            .sum()
    }

    pub(crate) fn enter(
        self: &Arc<Self>,
        target: &DfsMetadataQuorumSyncTarget,
        requirement: &DfsMetadataCommitRequirement,
    ) -> Admission {
        let mut key = target.clone();
        key.barrier_id.clear();
        let mut active = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(flight) = active
            .iter()
            .find(|f| f.target == key && f.requirement == *requirement)
        {
            #[cfg(test)]
            flight
                .joined
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Admission::Join(flight.clone());
        }
        let flight = Arc::new(Flight {
            target: key,
            requirement: requirement.clone(),
            result: Mutex::default(),
            ready: Condvar::new(),
            #[cfg(test)]
            joined: Default::default(),
        });
        active.push(flight.clone());
        Admission::Lead(Leader {
            owner: self.clone(),
            flight,
            published: false,
        })
    }
}
impl Flight {
    pub(crate) fn wait(
        self: &Arc<Self>,
        timeout: Duration,
        cancellation: Option<&DfsCancellationFlag>,
    ) -> Outcome {
        let flight = self.clone();
        let waker: crate::service_state::DfsCancellationWaker = Arc::new(move || {
            let _lock = flight.result.lock().unwrap_or_else(|e| e.into_inner());
            flight.ready.notify_all();
        });
        let _registered = cancellation.map(|flag| flag.register_waker(&waker));
        let result = self.result.lock().unwrap_or_else(|e| e.into_inner());
        let (result, _) = self
            .ready
            .wait_timeout_while(result, timeout, |value| {
                value.is_none() && !cancellation.is_some_and(DfsCancellationFlag::is_cancelled)
            })
            .unwrap_or_else(|e| e.into_inner());
        if cancellation.is_some_and(DfsCancellationFlag::is_cancelled) {
            return Err(DfsError::Cancelled {
                operation: "joining write-set sync",
            });
        }
        result.clone().unwrap_or_else(|| {
            Err(DfsError::Network {
                message: "in-flight write-set barrier exceeded the caller deadline".into(),
            })
        })
    }
}
impl Leader {
    pub(crate) fn finish(mut self, result: &Outcome) {
        self.complete(result.clone());
        self.published = true;
    }
    fn complete(&self, outcome: Outcome) {
        let mut active = self.owner.0.lock().unwrap_or_else(|e| e.into_inner());
        let mut result = self.flight.result.lock().unwrap_or_else(|e| e.into_inner());
        if result.is_none() {
            active.retain(|f| !Arc::ptr_eq(f, &self.flight));
            *result = Some(outcome);
            self.flight.ready.notify_all();
        }
    }
}
impl Drop for Leader {
    fn drop(&mut self) {
        if !self.published {
            self.complete(Err(DfsError::Cancelled {
                operation: "write-set barrier leader",
            }));
        }
    }
}
