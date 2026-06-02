use super::*;
use crate::syscalls::*;

/// ### `thread_id()`
/// Returns the index of the current thread
/// (threads indices are sequencial from zero)
#[instrument(level = "trace", skip_all, fields(tid = field::Empty), ret)]
pub fn thread_id<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    ret_tid: WasmPtr<Tid, M>,
) -> Errno {
    let env = ctx.data();
    let tid: Tid = env.thread.tid().into();
    Span::current().record("tid", tid);
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem!(ret_tid.write(&memory, tid));
    Errno::Success
}

/// ### `gettid()`
/// Returns the current sandbox thread id using the POSIX/Linux import shape.
#[instrument(level = "trace", skip_all, fields(tid = field::Empty), ret)]
pub fn gettid(mut ctx: FunctionEnvMut<'_, WasiEnv>) -> Result<Tid, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let tid: Tid = ctx.data().thread.tid().into();
    Span::current().record("tid", tid);
    Ok(tid)
}
