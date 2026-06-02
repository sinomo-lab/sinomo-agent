use super::*;
use crate::syscalls::*;

/// ### `sched_yield()`
/// Yields execution of the thread
#[instrument(level = "trace", skip_all, ret)]
pub fn sched_yield<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    ctx = wasi_try_ok!(maybe_backoff::<M>(ctx)?);
    if ctx.data().state.has_shared_memory_mappings() {
        let memory = unsafe { ctx.data().memory_view(&ctx) };
        if let Err(err) = sync_shared_memory_to_guest::<M>(ctx.data(), &memory) {
            return Ok(err);
        }
    }

    //trace!("wasi[{}:{}]::sched_yield", ctx.data().pid(), ctx.data().tid());
    thread_sleep_internal::<M>(ctx, 1_000_000)
}
