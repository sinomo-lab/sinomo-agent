use super::*;
use crate::syscalls::*;
use wasmer::FromToNativeWasmType;

/// ### `thread_signal()`
/// Send a signal to a particular thread in the current process.
/// Note: This is similar to `signal` in POSIX.
/// Inputs:
/// - `Signal`
///   Signal to be raised for this process
#[instrument(level = "trace", skip_all, fields(%tid, ?sig), ret)]
pub fn thread_signal(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    tid: Tid,
    sig: i32,
) -> Result<Errno, WasiError> {
    let sig = signal_from_raw(sig);
    let ret = {
        let tid: WasiThreadId = tid.into();
        if ctx.data().process.signal_thread(&tid, sig) {
            if sig == Signal::Sigwakeup {
                wake_all_futex_waiters(ctx.data().state.deref());
            } else if sig != Signal::Signone {
                wake_futex_waiters_for_thread(ctx.data().state.deref(), tid.raw());
                wake_shared_cond_waiters_for_thread(ctx.data().state.deref(), tid.raw());
            }
            Errno::Success
        } else {
            Errno::Badf
        }
    };

    WasiEnv::do_pending_operations(&mut ctx)?;

    Ok(ret)
}

fn signal_from_raw(sig: i32) -> Signal {
    if sig == Signal::Sigwakeup.to_native() {
        Signal::Sigwakeup
    } else {
        Signal::from_native(sig)
    }
}

fn wake_all_futex_waiters(state: &crate::state::WasiState) {
    let mut guard = state.futexs.lock().unwrap();
    let futex_ids = guard.futexes.keys().copied().collect::<Vec<_>>();
    for futex_idx in futex_ids {
        let woken = {
            let Some(futex) = guard.futexes.get_mut(&futex_idx) else {
                continue;
            };
            let waiters = futex.wakers.keys().copied().collect::<Vec<_>>();
            let mut woken = Vec::with_capacity(waiters.len());
            for poller_idx in waiters {
                futex.masks.remove(&poller_idx);
                if let Some(waker) = futex.wakers.remove(&poller_idx).flatten() {
                    waker.wake();
                }
                woken.push(poller_idx);
            }
            woken
        };
        for poller_idx in woken {
            guard.poller_futexes.remove(&poller_idx);
            guard.poller_threads.remove(&poller_idx);
            guard.interrupted_pollers.remove(&poller_idx);
            guard.woken_pollers.insert(poller_idx);
        }
    }
    guard.futexes.retain(|_, futex| !futex.wakers.is_empty());
}

/// ### `tkill()`
/// POSIX/Linux-shaped thread signal delivery within the current WASIX process.
/// Returns 0 or a negative errno.
#[instrument(level = "trace", skip_all, fields(%tid, ?sig), ret)]
pub fn tkill(ctx: FunctionEnvMut<'_, WasiEnv>, tid: Tid, sig: Signal) -> Result<i32, WasiError> {
    let errno = thread_signal(ctx, tid, sig.to_native())?;
    if errno == Errno::Success {
        Ok(0)
    } else {
        Ok(-errno.to_native())
    }
}

/// ### `tgkill()`
/// POSIX/Linux-shaped thread-group signal delivery. The runtime currently
/// supports targeting threads in the caller's process.
#[instrument(level = "trace", skip_all, fields(%tgid, %tid, ?sig), ret)]
pub fn tgkill(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    tgid: Pid,
    tid: Tid,
    sig: Signal,
) -> Result<i32, WasiError> {
    if tgid != ctx.data().process.pid().raw() as Pid {
        return Ok(-Errno::Badf.to_native());
    }

    tkill(ctx, tid, sig)
}
