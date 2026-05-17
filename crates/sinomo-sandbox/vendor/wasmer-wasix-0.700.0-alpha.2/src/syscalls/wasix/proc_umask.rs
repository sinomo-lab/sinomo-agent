use super::*;
use crate::syscalls::*;

/// ### `proc_umask()`
/// Sets the process file creation mask and returns the previous mask.
#[instrument(level = "trace", skip_all, fields(mask = %mask, previous = field::Empty), ret)]
pub fn proc_umask<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    mask: u32,
    previous_ptr: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let previous = env.process.set_umask(mask);
    Span::current().record("previous", previous);
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(previous_ptr.write(&memory, previous));
    Ok(Errno::Success)
}
