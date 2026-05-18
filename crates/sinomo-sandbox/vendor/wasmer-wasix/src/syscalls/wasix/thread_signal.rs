use super::*;
use crate::syscalls::*;

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
