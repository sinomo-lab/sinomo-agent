use super::*;
use crate::state::WasiSignalStack;
use crate::syscalls::*;
use wasmer::FromToNativeWasmType;

/// ### `proc_raise()`
/// Send a signal to the process of the calling thread.
/// Note: This is similar to `raise` in POSIX.
/// Inputs:
/// - `Signal`
///   Signal to be raised for this process
#[instrument(level = "trace", skip_all, fields(sig), ret)]
pub fn proc_raise(mut ctx: FunctionEnvMut<'_, WasiEnv>, sig: Signal) -> Result<Errno, WasiError> {
    let env = ctx.data();
    env.process.signal_process(sig);

    WasiEnv::do_pending_operations(&mut ctx)?;

    Ok(Errno::Success)
}

/// ### `raise()`
/// POSIX-shaped signal raise. Returns 0 or a negative errno.
#[instrument(level = "trace", skip_all, fields(sig), ret)]
pub fn raise(ctx: FunctionEnvMut<'_, WasiEnv>, sig: Signal) -> Result<i32, WasiError> {
    let errno = proc_raise(ctx, sig)?;
    if errno == Errno::Success {
        Ok(0)
    } else {
        Ok(-errno.to_native())
    }
}

/// ### `proc_raise()`
/// Send a signal to the process of the calling thread.
/// Note: This is similar to `raise` in POSIX.
/// Inputs:
/// - `Signal`
///   Signal to be raised for this process
pub fn proc_raise_interval(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    sig: Signal,
    interval: Timestamp,
    repeat: Bool,
) -> Result<Errno, WasiError> {
    let env = ctx.data();
    let interval = match interval {
        0 => None,
        a => Some(Duration::from_millis(a)),
    };
    let repeat = matches!(repeat, Bool::True);
    env.process.signal_interval(sig, interval, repeat);

    WasiEnv::do_pending_operations(&mut ctx)?;

    Ok(Errno::Success)
}

/// ### `sigaltstack()`
/// Stores POSIX alternate signal stack state in the runtime. The libc wrapper
/// owns Linux validation and struct packing; this import owns lifecycle state.
#[instrument(level = "trace", skip_all, fields(%flags, %size), ret)]
pub fn sigaltstack_set<M: MemorySize + 'static>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    sp: u64,
    flags: i32,
    size: u64,
    old_sp: WasmPtr<i64, M>,
    old_flags: WasmPtr<i32, M>,
    old_size: WasmPtr<i64, M>,
) -> Result<i32, WasiError> {
    let old = {
        let mut signal_stack = ctx.data().state.signal_stack.lock().unwrap();
        let old = signal_stack.clone();
        *signal_stack = WasiSignalStack { sp, flags, size };
        old
    };
    if let Err(err) = write_signal_stack(&ctx, old_sp, old_flags, old_size, old) {
        return Ok(-err.to_native());
    }
    Ok(0)
}

/// ### `sigaltstack_get()`
/// Reads POSIX alternate signal stack state from the runtime.
#[instrument(level = "trace", skip_all, ret)]
pub fn sigaltstack_get<M: MemorySize + 'static>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    old_sp: WasmPtr<i64, M>,
    old_flags: WasmPtr<i32, M>,
    old_size: WasmPtr<i64, M>,
) -> Result<i32, WasiError> {
    let stack = ctx.data().state.signal_stack.lock().unwrap().clone();
    if let Err(err) = write_signal_stack(&ctx, old_sp, old_flags, old_size, stack) {
        return Ok(-err.to_native());
    }
    Ok(0)
}

/// ### `signal_mask_set()`
/// Sets the current WASIX thread's POSIX signal mask words.
#[instrument(level = "trace", skip_all, ret)]
pub fn signal_mask_set(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    low: u64,
    high: u64,
) -> Result<i32, WasiError> {
    ctx.data().thread.set_signal_mask(low, high);
    Ok(0)
}

/// ### `signal_mask_get()`
/// Reads the current WASIX thread's POSIX signal mask words.
#[instrument(level = "trace", skip_all, ret)]
pub fn signal_mask_get<M: MemorySize + 'static>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    low: WasmPtr<i64, M>,
    high: WasmPtr<i64, M>,
) -> Result<i32, WasiError> {
    if low.is_null() || high.is_null() {
        return Ok(-Errno::Fault.to_native());
    }
    let (low_value, high_value) = ctx.data().thread.signal_mask();
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    if let Err(err) = low.write(&memory, low_value as i64) {
        return Ok(-mem_error_to_wasi(err).to_native());
    }
    if let Err(err) = high.write(&memory, high_value as i64) {
        return Ok(-mem_error_to_wasi(err).to_native());
    }
    Ok(0)
}

fn write_signal_stack<M: MemorySize>(
    ctx: &FunctionEnvMut<'_, WasiEnv>,
    old_sp: WasmPtr<i64, M>,
    old_flags: WasmPtr<i32, M>,
    old_size: WasmPtr<i64, M>,
    stack: WasiSignalStack,
) -> Result<(), Errno> {
    if old_sp.is_null() && old_flags.is_null() && old_size.is_null() {
        return Ok(());
    }
    if old_sp.is_null() || old_flags.is_null() || old_size.is_null() {
        return Err(Errno::Fault);
    }
    let memory = unsafe { ctx.data().memory_view(ctx) };
    old_sp
        .write(&memory, stack.sp.min(i64::MAX as u64) as i64)
        .map_err(mem_error_to_wasi)?;
    old_flags
        .write(&memory, stack.flags)
        .map_err(mem_error_to_wasi)?;
    old_size
        .write(&memory, stack.size.min(i64::MAX as u64) as i64)
        .map_err(mem_error_to_wasi)?;
    Ok(())
}
