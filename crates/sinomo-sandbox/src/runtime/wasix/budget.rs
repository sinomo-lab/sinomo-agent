use crate::{SANDBOX_UNLIMITED_WALL_TIME_MS, SandboxCancellation, SandboxWasiError};
use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use wasmer_wasix::{os::task::process::WasiProcess, wasmer_wasix_types::wasi::Signal};

const WASIX_TERMINATION_RUNNING: u8 = 0;
const WASIX_TERMINATION_TIMEOUT: u8 = 1;
const WASIX_TERMINATION_CANCELLED: u8 = 2;
const WASIX_TERMINATION_POLL_INTERVAL: Duration = Duration::from_millis(25);
const WASIX_TERMINATION_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

pub(super) struct WasixProcessBudgetGuard {
    done: Arc<AtomicBool>,
    done_signal: Arc<(Mutex<bool>, Condvar)>,
    termination_reason: Arc<AtomicU8>,
}

impl WasixProcessBudgetGuard {
    pub(super) fn start(
        timeout_ms: u64,
        cancellation: SandboxCancellation,
        process: WasiProcess,
    ) -> Self {
        let done = Arc::new(AtomicBool::new(false));
        let done_signal = Arc::new((Mutex::new(false), Condvar::new()));
        let termination_reason = Arc::new(AtomicU8::new(WASIX_TERMINATION_RUNNING));

        if timeout_ms != SANDBOX_UNLIMITED_WALL_TIME_MS {
            spawn_timeout_guard(
                timeout_ms,
                Arc::clone(&done),
                Arc::clone(&done_signal),
                Arc::clone(&termination_reason),
                process.clone(),
            );
        }
        spawn_cancellation_guard(
            cancellation,
            Arc::clone(&done),
            Arc::clone(&done_signal),
            Arc::clone(&termination_reason),
            process,
        );

        Self {
            done,
            done_signal,
            termination_reason,
        }
    }

    pub(super) fn finish(&self) {
        self.done.store(true, Ordering::SeqCst);
        let (lock, condvar) = &*self.done_signal;
        if let Ok(mut done) = lock.lock() {
            *done = true;
            condvar.notify_all();
        }
    }

    pub(super) fn done_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.done)
    }

    pub(super) fn termination_reason_handle(&self) -> Arc<AtomicU8> {
        Arc::clone(&self.termination_reason)
    }

    pub(super) fn termination_error(
        &self,
        timeout_ms: u64,
        metering_exhausted: bool,
    ) -> Option<SandboxWasiError> {
        match self.termination_reason.load(Ordering::SeqCst) {
            WASIX_TERMINATION_TIMEOUT => Some(SandboxWasiError::TimedOut { timeout_ms }),
            WASIX_TERMINATION_CANCELLED => Some(SandboxWasiError::Cancelled),
            _ if metering_exhausted => Some(SandboxWasiError::TimedOut { timeout_ms }),
            _ => None,
        }
    }
}

fn spawn_timeout_guard(
    timeout_ms: u64,
    done: Arc<AtomicBool>,
    done_signal: Arc<(Mutex<bool>, Condvar)>,
    termination_reason: Arc<AtomicU8>,
    process: WasiProcess,
) {
    thread::spawn(move || {
        let (lock, condvar) = &*done_signal;
        let Ok(done_lock) = lock.lock() else {
            return;
        };
        let Ok((done_lock, timeout)) =
            condvar.wait_timeout_while(done_lock, Duration::from_millis(timeout_ms), |done| !*done)
        else {
            return;
        };
        drop(done_lock);
        if !timeout.timed_out() {
            return;
        }
        if !done.load(Ordering::SeqCst)
            && termination_reason
                .compare_exchange(
                    WASIX_TERMINATION_RUNNING,
                    WASIX_TERMINATION_TIMEOUT,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
        {
            process.signal_process(Signal::Sigkill);
        }
    });
}

fn spawn_cancellation_guard(
    cancellation: SandboxCancellation,
    done: Arc<AtomicBool>,
    done_signal: Arc<(Mutex<bool>, Condvar)>,
    termination_reason: Arc<AtomicU8>,
    process: WasiProcess,
) {
    thread::spawn(move || {
        let (lock, condvar) = &*done_signal;
        while !done.load(Ordering::SeqCst) {
            if cancellation.is_cancelled() {
                if termination_reason
                    .compare_exchange(
                        WASIX_TERMINATION_RUNNING,
                        WASIX_TERMINATION_CANCELLED,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    process.signal_process(Signal::Sigkill);
                }
                break;
            }
            let Ok(done_lock) = lock.lock() else {
                return;
            };
            let Ok((done_lock, _timeout)) =
                condvar.wait_timeout_while(done_lock, Duration::from_millis(25), |done| !*done)
            else {
                return;
            };
            drop(done_lock);
        }
    });
}

pub(super) fn run_with_host_wall_timeout<T, F>(
    timeout_ms: u64,
    done: Arc<AtomicBool>,
    termination_reason: Arc<AtomicU8>,
    process: WasiProcess,
    run: F,
) -> Result<T, SandboxWasiError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    thread::spawn(move || {
        let _ = sender.send(run());
    });

    let deadline = host_wall_deadline(timeout_ms);
    loop {
        match receiver.recv_timeout(WASIX_TERMINATION_POLL_INTERVAL) {
            Ok(result) => return Ok(result),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                done.store(true, Ordering::SeqCst);
                return Err(SandboxWasiError::Runtime {
                    detail: "WASIX process runner thread terminated without a result".to_string(),
                });
            }
        }

        match termination_reason.load(Ordering::SeqCst) {
            WASIX_TERMINATION_TIMEOUT => {
                return wait_for_wasix_process_shutdown(
                    receiver,
                    done,
                    SandboxWasiError::TimedOut { timeout_ms },
                );
            }
            WASIX_TERMINATION_CANCELLED => {
                return wait_for_wasix_process_shutdown(
                    receiver,
                    done,
                    SandboxWasiError::Cancelled,
                );
            }
            _ if deadline.is_some_and(|deadline| Instant::now() >= deadline) => {
                if termination_reason
                    .compare_exchange(
                        WASIX_TERMINATION_RUNNING,
                        WASIX_TERMINATION_TIMEOUT,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    process.signal_process(Signal::Sigkill);
                }
            }
            _ => {}
        }
    }
}

fn host_wall_deadline(timeout_ms: u64) -> Option<Instant> {
    if timeout_ms == SANDBOX_UNLIMITED_WALL_TIME_MS {
        return None;
    }
    Instant::now().checked_add(Duration::from_millis(timeout_ms))
}

fn wait_for_wasix_process_shutdown<T>(
    receiver: std::sync::mpsc::Receiver<T>,
    done: Arc<AtomicBool>,
    error: SandboxWasiError,
) -> Result<T, SandboxWasiError> {
    let shutdown_deadline = Instant::now() + WASIX_TERMINATION_SHUTDOWN_GRACE;
    let mut grace_elapsed = false;
    loop {
        match receiver.recv_timeout(WASIX_TERMINATION_POLL_INTERVAL) {
            Ok(result) => return Ok(result),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                if !grace_elapsed && Instant::now() >= shutdown_deadline =>
            {
                // The process did not stop promptly after SIGKILL. Keep waiting
                // instead of returning while a non-yielding WASM task can still
                // consume CPU in the background; instruction metering remains
                // the final hard stop for such guests.
                grace_elapsed = true;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                done.store(true, Ordering::SeqCst);
                return Err(error);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::host_wall_deadline;

    #[test]
    fn host_wall_deadline_handles_normal_timeout() {
        assert!(host_wall_deadline(1_000).is_some());
    }

    #[test]
    fn host_wall_deadline_handles_unrepresentable_timeout_without_panicking() {
        let result = std::panic::catch_unwind(|| host_wall_deadline(u64::MAX));
        assert!(result.is_ok());
    }
}
