use std::task::Waker;

use super::*;
use crate::syscalls::*;

macro_rules! sinomo_debug {
    ($($arg:tt)*) => {
        if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
            eprintln!($($arg)*);
        }
    };
}

/// ### `thread_join()`
/// Joins this thread with another thread, blocking this
/// one until the other finishes
///
/// ## Parameters
///
/// * `tid` - Handle of the thread to wait on
//#[instrument(level = "trace", skip_all, fields(%join_tid), ret)]
pub fn thread_join<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    join_tid: Tid,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    thread_join_internal::<M>(ctx, join_tid)
}

pub(super) fn thread_join_internal<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    join_tid: Tid,
) -> Result<Errno, WasiError> {
    let tid: WasiThreadId = join_tid.into();
    if let Some(_child_exit_code) = unsafe { handle_rewind::<M, i32>(&mut ctx) } {
        if tid.raw() != ctx.data().pid().raw() {
            ctx.data().process.remove_thread(&tid);
        }
        return Ok(Errno::Success);
    }

    ctx = wasi_try_ok!(maybe_snapshot::<M>(ctx)?);

    let env = ctx.data();
    if tid == env.tid() {
        return Ok(Errno::Inval);
    }
    sinomo_debug!(
        "sinomo-debug thread_join start pid={} current_tid={} join_tid={}",
        env.pid().raw(),
        env.tid().raw(),
        tid.raw()
    );
    let other_thread = env.process.get_thread(&tid);
    if let Some(other_thread) = other_thread {
        let reaped_thread_id = if other_thread.is_main() {
            None
        } else {
            Some(tid)
        };
        let res = __asyncify_with_deep_sleep::<M, _, _>(ctx, async move {
            other_thread
                .join()
                .await
                .map_err(|err| err.as_exit_code().unwrap_or(ExitCode::from(Errno::Unknown)))
                .unwrap_or_else(|a| a)
                .raw()
        })?;
        if let AsyncifyAction::Finish(ctx, _) = res
            && let Some(tid) = reaped_thread_id
        {
            sinomo_debug!(
                "sinomo-debug thread_join finish pid={} current_tid={} join_tid={}",
                ctx.data().pid().raw(),
                ctx.data().tid().raw(),
                tid.raw()
            );
            ctx.data().process.remove_thread(&tid);
        }
        Ok(Errno::Success)
    } else {
        Ok(Errno::Badf)
    }
}
