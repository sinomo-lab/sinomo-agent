use super::*;
use crate::syscalls::*;

/// ### `thread_local_create()`
/// Creates a process-global thread-local storage key.
///
/// The destructor address is recorded with the key and invoked during normal
/// WASIX thread cleanup for non-zero per-thread values. Values are isolated per
/// WASIX thread and removed when the key is destroyed.
#[instrument(level = "trace", skip_all, fields(destructor, key = field::Empty), ret)]
pub fn thread_local_create<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    destructor: u64,
    ret_key: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let key = wasi_try_ok!(env.process.create_thread_local_key(destructor));
    Span::current().record("key", key);

    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_key.write(&memory, key));
    Ok(Errno::Success)
}

/// ### `thread_local_destroy()`
/// Destroys a process-global thread-local storage key and clears values for all
/// live threads in the process.
#[instrument(level = "trace", skip_all, fields(key), ret)]
pub fn thread_local_destroy(mut ctx: FunctionEnvMut<'_, WasiEnv>, key: u32) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if ctx.data().process.destroy_thread_local_key(key) {
        Ok(Errno::Success)
    } else {
        Ok(Errno::Badf)
    }
}

/// ### `thread_local_set()`
/// Sets the calling thread's value for a process-global thread-local key.
#[instrument(level = "trace", skip_all, fields(key, value), ret)]
pub fn thread_local_set(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    key: u32,
    value: u64,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    if !env.process.has_thread_local_key(key) {
        return Ok(Errno::Badf);
    }
    env.thread.set_thread_local_value(key, value);
    Ok(Errno::Success)
}

/// ### `thread_local_get()`
/// Reads the calling thread's value for a process-global thread-local key.
///
/// Unset keys return zero, matching pthread TLS behavior.
#[instrument(level = "trace", skip_all, fields(key, value = field::Empty), ret)]
pub fn thread_local_get<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    key: u32,
    ret_value: WasmPtr<M::Offset, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    if !env.process.has_thread_local_key(key) {
        return Ok(Errno::Badf);
    }
    let value = env.thread.thread_local_value(key);
    Span::current().record("value", value);
    let value = wasi_try_ok!(value.try_into().map_err(|_| Errno::Overflow));

    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_value.write(&memory, value));
    Ok(Errno::Success)
}
