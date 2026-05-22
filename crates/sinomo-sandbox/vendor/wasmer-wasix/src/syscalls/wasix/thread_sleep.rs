use super::*;
use crate::syscalls::*;
use wasmer::FromToNativeWasmType;
use wasmer_wasix_types::wasi::Snapshot0Clockid;

const NANOS_PER_SECOND: u64 = 1_000_000_000;
const NANOS_PER_MICROSECOND: u64 = 1_000;
const TIMER_ABSTIME: i32 = 1;

/// ### `thread_sleep()`
/// Sends the current thread to sleep for a period of time
///
/// ## Parameters
///
/// * `duration` - Amount of time that the thread should sleep
#[instrument(level = "trace", skip_all, fields(%duration), ret)]
pub fn thread_sleep<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    duration: Timestamp,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_link_operations(&mut ctx, true)?;
    if let Some(forced_exit) = ctx.data().should_exit() {
        return Err(WasiError::Exit(forced_exit));
    }
    let has_pending_signal = {
        let signals = ctx.data().thread.signals().lock().unwrap();
        !signals.0.is_empty()
    };
    if has_pending_signal {
        let signal_handler_installed = ctx
            .data()
            .try_inner()
            .map(|inner| inner.main_module_instance_handles().signal.is_some())
            .unwrap_or(false);
        match WasiEnv::process_signals_and_exit(&mut ctx)? {
            Ok(processed) if signal_handler_installed && processed => return Ok(Errno::Intr),
            Ok(_) => {}
            Err(err) => return Ok(err),
        }
    }

    thread_sleep_internal::<M>(ctx, duration)
}

pub(crate) fn thread_sleep_internal<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    duration: Timestamp,
) -> Result<Errno, WasiError> {
    if let Some(interrupted) = unsafe { handle_rewind::<M, bool>(&mut ctx) } {
        let env = ctx.data();
        let memory = unsafe { env.memory_view(&ctx) };
        sync_shared_memory_to_guest::<M>(env, &memory)
            .map_err(|err| WasiError::Exit(err.into()))?;
        return if interrupted {
            Ok(Errno::Intr)
        } else {
            Ok(Errno::Success)
        };
    }

    ctx = wasi_try_ok!(maybe_backoff::<M>(ctx)?);
    ctx = wasi_try_ok!(maybe_snapshot::<M>(ctx)?);

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };

    #[cfg(feature = "sys-thread")]
    if duration == 0 {
        std::thread::yield_now();
        sync_shared_memory_to_guest::<M>(env, &memory)
            .map_err(|err| WasiError::Exit(err.into()))?;
    }

    if duration > 0 {
        if env.process.signal_due_intervals() {
            WasiEnv::do_pending_operations(&mut ctx)?;
            return Ok(Errno::Intr);
        }

        let mut duration = Duration::from_nanos(duration);
        let interval_due_before_requested_end = if let Some(next_signal) =
            env.process.next_signal_interval_duration()
            && next_signal < duration
        {
            duration = next_signal;
            true
        } else {
            false
        };
        let tasks = env.tasks().clone();
        let thread = env.thread.clone();
        let interrupted = block_on(async move {
            tokio::select! {
                _ = tasks.sleep_now(duration) => false,
                _ = thread.wait_for_signal() => true,
            }
        });
        let env = ctx.data();
        if has_live_cross_process_shared_memory(env) {
            let memory = unsafe { env.memory_view(&ctx) };
            sync_shared_memory_to_guest::<M>(env, &memory)
                .map_err(|err| WasiError::Exit(err.into()))?;
        }
        if ctx.data().process.signal_due_intervals() {
            WasiEnv::do_pending_operations(&mut ctx)?;
            return Ok(Errno::Intr);
        }
        if interrupted {
            let signal_handler_installed = ctx
                .data()
                .try_inner()
                .map(|inner| inner.main_module_instance_handles().signal.is_some())
                .unwrap_or(false);
            match WasiEnv::process_signals_and_exit(&mut ctx)? {
                Ok(processed) if signal_handler_installed && processed => return Ok(Errno::Intr),
                Ok(_) => {}
                Err(err) => return Ok(err),
            }
        }
        if interval_due_before_requested_end {
            return Ok(Errno::Intr);
        }
    }
    Ok(Errno::Success)
}

/// ### `sleep()`
/// POSIX-shaped seconds sleep. Returns unslept seconds on interruption; this
/// runtime currently completes sleeps unless the guest traps, so success is 0.
#[instrument(level = "trace", skip_all, fields(%seconds), ret)]
pub fn sleep<M: MemorySize + 'static>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    seconds: u32,
) -> Result<u32, WasiError> {
    let duration = u64::from(seconds).saturating_mul(NANOS_PER_SECOND);
    let errno = thread_sleep::<M>(ctx, duration)?;
    if errno == Errno::Success {
        Ok(0)
    } else {
        Ok(seconds)
    }
}

/// ### `usleep()`
/// POSIX-shaped microsecond sleep. Returns 0 or a negative errno.
#[instrument(level = "trace", skip_all, fields(%microseconds), ret)]
pub fn usleep<M: MemorySize + 'static>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    microseconds: u64,
) -> Result<i32, WasiError> {
    let duration = microseconds.saturating_mul(NANOS_PER_MICROSECOND);
    sleep_nanos_errno::<M>(ctx, duration)
}

/// ### `nanosleep()`
/// POSIX-shaped relative sleep. Reads a guest `struct timespec` and returns 0
/// or a negative errno.
#[instrument(level = "trace", skip_all, ret)]
pub fn nanosleep<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    request: WasmPtr<i64, M>,
    remain: WasmPtr<i64, M>,
) -> Result<i32, WasiError> {
    let duration = match read_timespec_nanos(&ctx, request) {
        Ok(duration) => duration,
        Err(errno) => return Ok(-errno.to_native()),
    };
    let rc = sleep_nanos_errno::<M>(ctx.as_mut(), duration)?;
    if rc == 0 && !remain.is_null() {
        return write_timespec(&ctx, remain, 0, 0);
    }
    Ok(rc)
}

/// ### `clock_nanosleep()`
/// Linux-shaped clock-relative or clock-absolute sleep. Absolute sleeps are
/// evaluated against the guest-visible clock and then lowered to `thread_sleep`.
#[instrument(level = "trace", skip_all, fields(%clock_id, %flags), ret)]
pub fn clock_nanosleep<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    clock_id: i32,
    flags: i32,
    request: WasmPtr<i64, M>,
    remain: WasmPtr<i64, M>,
) -> Result<i32, WasiError> {
    if flags & !TIMER_ABSTIME != 0 {
        return Ok(-Errno::Inval.to_native());
    }
    let clock = match linux_clock_id(clock_id) {
        Some(clock) => clock,
        None => return Ok(-Errno::Inval.to_native()),
    };
    let requested = match read_timespec_nanos(&ctx, request) {
        Ok(duration) => duration,
        Err(errno) => return Ok(-errno.to_native()),
    };
    let duration = if flags & TIMER_ABSTIME != 0 {
        let now = match platform_clock_time_get(clock, 1_000_000) {
            Ok(now) => now,
            Err(errno) => return Ok(-errno.to_native()),
        };
        requested.saturating_sub(u64::try_from(now).unwrap_or(0))
    } else {
        requested
    };
    let rc = sleep_nanos_errno::<M>(ctx.as_mut(), duration)?;
    if rc == 0 && !remain.is_null() {
        return write_timespec(&ctx, remain, 0, 0);
    }
    Ok(rc)
}

fn sleep_nanos_errno<M: MemorySize + 'static>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    duration: Timestamp,
) -> Result<i32, WasiError> {
    let errno = thread_sleep::<M>(ctx, duration)?;
    if errno == Errno::Success {
        Ok(0)
    } else {
        Ok(-errno.to_native())
    }
}

fn read_timespec_nanos<M: MemorySize>(
    ctx: &FunctionEnvMut<'_, WasiEnv>,
    ptr: WasmPtr<i64, M>,
) -> Result<Timestamp, Errno> {
    if ptr.is_null() {
        return Err(Errno::Fault);
    }
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let seconds = ptr.read(&memory).map_err(crate::mem_error_to_wasi)?;
    let nanos = if M::is_64bit() {
        let nanos_ptr = ptr_at_byte_offset::<i64, M>(ptr, 8)?;
        nanos_ptr.read(&memory).map_err(crate::mem_error_to_wasi)?
    } else {
        let nanos_ptr = ptr_at_byte_offset::<i32, M>(ptr, 8)?;
        i64::from(nanos_ptr.read(&memory).map_err(crate::mem_error_to_wasi)?)
    };
    if seconds < 0 || !(0..NANOS_PER_SECOND as i64).contains(&nanos) {
        return Err(Errno::Inval);
    }
    let seconds = seconds as u64;
    let nanos = nanos as u64;
    Ok(seconds
        .saturating_mul(NANOS_PER_SECOND)
        .saturating_add(nanos))
}

fn write_timespec<M: MemorySize>(
    ctx: &FunctionEnvMut<'_, WasiEnv>,
    ptr: WasmPtr<i64, M>,
    seconds: i64,
    nanos: i64,
) -> Result<i32, WasiError> {
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    if let Err(err) = ptr.write(&memory, seconds) {
        return Ok(-crate::mem_error_to_wasi(err).to_native());
    }
    if M::is_64bit() {
        let nanos_ptr = match ptr_at_byte_offset::<i64, M>(ptr, 8) {
            Ok(ptr) => ptr,
            Err(errno) => return Ok(-errno.to_native()),
        };
        if let Err(err) = nanos_ptr.write(&memory, nanos) {
            return Ok(-crate::mem_error_to_wasi(err).to_native());
        }
    } else {
        let nanos_ptr = match ptr_at_byte_offset::<i32, M>(ptr, 8) {
            Ok(ptr) => ptr,
            Err(errno) => return Ok(-errno.to_native()),
        };
        let padding_ptr = match ptr_at_byte_offset::<i32, M>(ptr, 12) {
            Ok(ptr) => ptr,
            Err(errno) => return Ok(-errno.to_native()),
        };
        if let Err(err) = nanos_ptr.write(&memory, nanos as i32) {
            return Ok(-crate::mem_error_to_wasi(err).to_native());
        }
        if let Err(err) = padding_ptr.write(&memory, 0) {
            return Ok(-crate::mem_error_to_wasi(err).to_native());
        }
    }
    Ok(0)
}

fn ptr_at_byte_offset<T, M: MemorySize>(
    ptr: WasmPtr<i64, M>,
    byte_offset: u64,
) -> Result<WasmPtr<T, M>, Errno> {
    let base: u64 = ptr.offset().into();
    let offset = base.checked_add(byte_offset).ok_or(Errno::Overflow)?;
    let offset = offset.try_into().map_err(|_| Errno::Overflow)?;
    Ok(WasmPtr::new(offset))
}

fn linux_clock_id(clock_id: i32) -> Option<Snapshot0Clockid> {
    match clock_id {
        0 => Some(Snapshot0Clockid::Realtime),
        1 => Some(Snapshot0Clockid::Monotonic),
        2 => Some(Snapshot0Clockid::ProcessCputimeId),
        3 => Some(Snapshot0Clockid::ThreadCputimeId),
        _ => None,
    }
}
