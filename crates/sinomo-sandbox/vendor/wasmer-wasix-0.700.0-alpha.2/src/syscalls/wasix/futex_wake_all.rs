use super::*;
use crate::syscalls::*;

/// Wake up all threads that are waiting on futex_wait on this futex.
///
/// ## Parameters
///
/// * `futex` - Memory location that holds a futex that others may be waiting on
#[instrument(level = "trace", skip_all, fields(futex_idx = field::Empty, woken = field::Empty), ret)]
pub fn futex_wake_all<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    futex_ptr: WasmPtr<u32, M>,
    ret_woken: WasmPtr<Bool, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let state = env.state.deref();

    let pointer: u64 = futex_ptr.offset().into();
    //Span::current().record("futex_idx", pointer);

    let woken = {
        let mut guard = state.futexs.lock().unwrap();
        if let Some(futex) = guard.futexes.remove(&pointer) {
            let mut woke_waiter = false;
            for waker in futex.wakers {
                if let Some(waker) = waker.1 {
                    waker.wake();
                    woke_waiter = true;
                }
            }
            tracing::trace!("wake_all (hit) on {pointer}");
            woke_waiter
        } else {
            tracing::trace!("wake_all (miss) on {pointer}");
            false
        }
    };
    //Span::current().record("woken", woken);

    let woken = match woken {
        false => Bool::False,
        true => Bool::True,
    };
    wasi_try_mem_ok!(ret_woken.write(&memory, woken));
    Ok(Errno::Success)
}
