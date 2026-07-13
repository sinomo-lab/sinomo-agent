use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

const CANCELLATION_ACTIVE: u8 = 0;
const CANCELLATION_CANCELLED: u8 = 1;
const CANCELLATION_COMPLETING: u8 = 2;
const CANCELLATION_COMPLETED: u8 = 3;

#[derive(Clone, Debug, Default)]
pub struct SandboxCancellationToken {
    state: Arc<AtomicU8>,
    lifecycle_mutation_fenced: Arc<AtomicBool>,
    durability_failure: Arc<Mutex<Option<String>>>,
}

impl SandboxCancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Attempt cancellation while guest execution is still active. Returns false
    /// once the runner has atomically entered finalization, so a late cancel cannot
    /// report success after the primary command already completed.
    pub fn cancel(&self) -> bool {
        self.state
            .compare_exchange(
                CANCELLATION_ACTIVE,
                CANCELLATION_CANCELLED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::SeqCst) == CANCELLATION_CANCELLED
    }

    /// Win the guest-completion/cancellation race before any finalization work.
    pub fn begin_completing(&self) -> bool {
        self.state
            .compare_exchange(
                CANCELLATION_ACTIVE,
                CANCELLATION_COMPLETING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }

    pub fn complete(&self) {
        let _ = self.state.compare_exchange(
            CANCELLATION_COMPLETING,
            CANCELLATION_COMPLETED,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub fn is_completing_or_completed(&self) -> bool {
        matches!(
            self.state.load(Ordering::SeqCst),
            CANCELLATION_COMPLETING | CANCELLATION_COMPLETED
        )
    }

    /// Cancel guest work and fence any explicit lifecycle mutation after the
    /// caller loses the exclusive DFS lifecycle lease.
    pub fn fence_lifecycle_mutation(&self) {
        self.lifecycle_mutation_fenced.store(true, Ordering::SeqCst);
        let _ = self.cancel();
    }

    pub fn lifecycle_mutation_is_fenced(&self) -> bool {
        self.lifecycle_mutation_fenced.load(Ordering::SeqCst)
    }

    pub fn record_durability_failure(&self, detail: impl Into<String>) {
        if let Ok(mut failure) = self.durability_failure.lock()
            && failure.is_none()
        {
            *failure = Some(detail.into());
        }
    }

    pub fn durability_failure(&self) -> Option<String> {
        self.durability_failure
            .lock()
            .ok()
            .and_then(|failure| failure.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_fence_cancels_guest_and_fences_lifecycle_mutation() {
        let token = SandboxCancellationToken::new();
        let observer = token.clone();
        token.fence_lifecycle_mutation();
        assert!(observer.is_cancelled());
        assert!(observer.lifecycle_mutation_is_fenced());
    }

    #[test]
    fn ordinary_cancellation_does_not_claim_lifecycle_lease_loss() {
        let token = SandboxCancellationToken::new();
        token.cancel();
        assert!(token.is_cancelled());
        assert!(!token.lifecycle_mutation_is_fenced());
    }

    #[test]
    fn finalization_wins_late_cancellation() {
        let token = SandboxCancellationToken::new();
        assert!(token.begin_completing());
        assert!(!token.cancel());
        assert!(!token.is_cancelled());
        assert!(token.is_completing_or_completed());
        token.complete();
        assert!(token.is_completing_or_completed());
    }

    #[test]
    fn cancellation_wins_before_finalization() {
        let token = SandboxCancellationToken::new();
        assert!(token.cancel());
        assert!(!token.begin_completing());
        assert!(token.is_cancelled());
    }
}
