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
    sig: Signal,
) -> Result<Errno, WasiError> {
    let ret = {
        let tid: WasiThreadId = tid.into();
        if ctx.data().process.signal_thread(&tid, sig) {
            Errno::Success
        } else {
            Errno::Badf
        }
    };

    WasiEnv::do_pending_operations(&mut ctx)?;

    Ok(ret)
}

/// ### `tkill()`
/// POSIX/Linux-shaped thread signal delivery within the current WASIX process.
/// Returns 0 or a negative errno.
#[instrument(level = "trace", skip_all, fields(%tid, ?sig), ret)]
pub fn tkill(ctx: FunctionEnvMut<'_, WasiEnv>, tid: Tid, sig: Signal) -> Result<i32, WasiError> {
    let errno = thread_signal(ctx, tid, sig)?;
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
