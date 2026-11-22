//! The requester's budget and departure, as one served request observes them.
//!
//! A requester's remaining budget arrives as a relative duration in the
//! request's metadata ([`crate::routes::DFS_REQUEST_BUDGET_METADATA_KEY`]) and
//! is measured from arrival. Its departure arrives as the network layer
//! dropping the served handler future. Both end the served wait: the change
//! the request prepared keeps whatever outcome it already has, and a retry with
//! the same operation id, or the shard's recovery, resolves it.

use std::time::{Duration, Instant};

use crate::DfsCancellationFlag;

/// What bounds one served request: its requester's budget, if it set one, and
/// its requester's departure.
#[derive(Clone, Debug, Default)]
pub struct DfsServedRequestScope {
    deadline: Option<Instant>,
    cancellation: DfsCancellationFlag,
    executing: std::sync::Arc<tokio::sync::watch::Sender<bool>>,
}

impl DfsServedRequestScope {
    /// Capture a request at arrival. The returned guard belongs to the served
    /// handler future: dropping it, on completion or because the requester
    /// left, ends every wait the request still has.
    pub fn at_arrival(budget: Option<Duration>) -> (Self, DfsServedRequestDeparture) {
        let cancellation = DfsCancellationFlag::new();
        let scope = Self {
            // A budget too large to represent is no budget.
            deadline: budget.and_then(|budget| Instant::now().checked_add(budget)),
            cancellation: cancellation.clone(),
            executing: Default::default(),
        };
        (scope, DfsServedRequestDeparture(Some(cancellation)))
    }

    /// When the requester's budget ends, measured on this node.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn tokio_deadline(&self) -> Option<tokio::time::Instant> {
        self.deadline.map(tokio::time::Instant::from_std)
    }

    /// Fires when the requester has left.
    pub fn cancellation(&self) -> &DfsCancellationFlag {
        &self.cancellation
    }

    /// Mark the request's blocking execution as running until the returned
    /// guard drops. While it runs, the execution itself observes the budget
    /// and answers with the outcome it reached.
    pub fn enter_execution(&self) -> DfsServedRequestExecution {
        self.executing.send_replace(true);
        DfsServedRequestExecution(std::sync::Arc::clone(&self.executing))
    }

    /// Drive `request`, the served handler's work, to its answer or to the end
    /// of the requester's budget.
    ///
    /// A request that is waiting, for admission or for a publication, ends at
    /// the budget with [`DfsRequestBudgetEnded`]. A request whose blocking execution is
    /// running is left to answer: that execution observes the same deadline
    /// and reports the outcome it reached, which a wrapper could not know.
    pub async fn until_budget<F: std::future::Future>(
        &self,
        request: F,
    ) -> Result<F::Output, DfsRequestBudgetEnded> {
        let Some(deadline) = self.tokio_deadline() else {
            return Ok(request.await);
        };
        let mut request = std::pin::pin!(request);
        tokio::select! {
            biased;
            output = request.as_mut() => return Ok(output),
            () = tokio::time::sleep_until(deadline) => {}
        }
        let mut executing = self.executing.subscribe();
        loop {
            if !*executing.borrow_and_update() {
                self.cancellation.cancel();
                return Err(DfsRequestBudgetEnded);
            }
            tokio::select! {
                biased;
                output = request.as_mut() => return Ok(output),
                changed = executing.changed() => {
                    if changed.is_err() {
                        return Ok(request.await);
                    }
                }
            }
        }
    }

    /// The earlier of this request's budget and `existing`.
    pub fn deadline_within(&self, existing: Option<Instant>) -> Option<Instant> {
        match (self.deadline, existing) {
            (Some(budget), Some(existing)) => Some(budget.min(existing)),
            (deadline, None) | (None, deadline) => deadline,
        }
    }
}

/// A served request reached its requester's budget before it ran.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsRequestBudgetEnded;

/// Held while a served request's blocking execution runs.
#[derive(Debug)]
pub struct DfsServedRequestExecution(std::sync::Arc<tokio::sync::watch::Sender<bool>>);

impl Drop for DfsServedRequestExecution {
    fn drop(&mut self) {
        self.0.send_replace(false);
    }
}

/// Owned by the served handler future; its drop is the requester's departure.
#[derive(Debug)]
pub struct DfsServedRequestDeparture(Option<DfsCancellationFlag>);

impl DfsServedRequestDeparture {
    /// The handler answered: its requester did not leave, so nothing the
    /// request left running is cancelled.
    pub fn answered(mut self) {
        self.0.take();
    }
}

impl Drop for DfsServedRequestDeparture {
    fn drop(&mut self) {
        if let Some(cancellation) = self.0.take() {
            cancellation.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_answered_request_does_not_cancel_what_it_left_running() {
        let (scope, departure) = DfsServedRequestScope::at_arrival(None);
        departure.answered();
        assert!(!scope.cancellation().is_cancelled());
    }

    #[test]
    fn a_dropped_handler_guard_cancels_the_served_request() {
        let (scope, departure) = DfsServedRequestScope::at_arrival(None);
        assert_eq!(scope.deadline(), None);
        assert!(!scope.cancellation().is_cancelled());
        drop(departure);
        assert!(scope.cancellation().is_cancelled());
    }

    #[tokio::test]
    async fn a_waiting_request_ends_at_its_budget_and_an_executing_one_answers() {
        let (scope, _departure) =
            DfsServedRequestScope::at_arrival(Some(Duration::from_millis(50)));
        let waiting = scope.until_budget(std::future::pending::<()>()).await;
        assert_eq!(waiting, Err(DfsRequestBudgetEnded));
        assert!(scope.cancellation().is_cancelled());

        let (scope, _departure) =
            DfsServedRequestScope::at_arrival(Some(Duration::from_millis(50)));
        let execution = scope.enter_execution();
        let answered = scope
            .until_budget(async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                drop(execution);
                7
            })
            .await;
        assert_eq!(answered, Ok(7));
        assert!(!scope.cancellation().is_cancelled());
    }

    #[test]
    fn the_budget_is_measured_from_arrival_and_only_shortens() {
        let before = Instant::now();
        let (scope, _departure) = DfsServedRequestScope::at_arrival(Some(Duration::from_secs(30)));
        let deadline = scope.deadline().expect("budget deadline");
        assert!(deadline >= before + Duration::from_secs(30));
        let sooner = before + Duration::from_secs(1);
        assert_eq!(scope.deadline_within(Some(sooner)), Some(sooner));
        assert_eq!(scope.deadline_within(None), Some(deadline));
    }
}
