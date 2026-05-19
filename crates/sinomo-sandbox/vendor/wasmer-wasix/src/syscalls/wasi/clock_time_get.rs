use super::*;
use crate::syscalls::*;
use wasmer::FromToNativeWasmType;

// NOTE: This syscall is not instrumented since it will be logged too much,
// hence introducing too much noise to the logs.

/// ### `clock_time_get()`
/// Get the time of the specified clock
///
/// Inputs:
///
/// - `Clockid clock_id`
///   The ID of the clock to query
/// - `Timestamp precision`
///   The maximum amount of error the reading may have
///
/// Output:
///
/// - `Timestamp *time`
///   The value of the clock in nanoseconds
#[cfg_attr(
    feature = "extra-logging",
    tracing::instrument(level = "trace", skip_all, ret)
)]
pub fn clock_time_get<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    clock_id: Snapshot0Clockid,
    precision: Timestamp,
    time: WasmPtr<Timestamp, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    ctx = wasi_try_ok!(maybe_backoff::<M>(ctx)?);

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };

    let mut t_out = wasi_try_ok!(platform_clock_time_get(clock_id, precision));
    {
        let guard = env.state.clock_offset.lock().unwrap();
        if let Some(offset) = guard.get(&clock_id) {
            t_out += *offset;
        }
    };
    wasi_try_mem_ok!(time.write(&memory, t_out as Timestamp));
    Ok(Errno::Success)
}

/// ### `clock_gettime()`
/// POSIX-shaped clock query. Writes a guest `struct timespec` and returns 0 or
/// a negative errno.
#[cfg_attr(
    feature = "extra-logging",
    tracing::instrument(level = "trace", skip_all, ret)
)]
pub fn clock_gettime<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    clock_id: i32,
    timespec: WasmPtr<i64, M>,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    ctx = match maybe_backoff::<M>(ctx)? {
        Ok(ctx) => ctx,
        Err(errno) => {
            return if errno == Errno::Success {
                Ok(0)
            } else {
                Ok(-errno.to_native())
            };
        }
    };

    let clock = match linux_clock_id(clock_id) {
        Some(clock) => clock,
        None => return Ok(-Errno::Inval.to_native()),
    };
    let mut time = match platform_clock_time_get(clock, 1) {
        Ok(time) => time,
        Err(errno) => return Ok(-errno.to_native()),
    };
    {
        let guard = ctx.data().state.clock_offset.lock().unwrap();
        if let Some(offset) = guard.get(&clock) {
            time += *offset;
        }
    }
    write_timespec(&ctx, timespec, time)
}

/// ### `clock_getres()`
/// POSIX-shaped clock resolution query. Writes a guest `struct timespec` and
/// returns 0 or a negative errno.
#[instrument(level = "trace", skip_all, fields(%clock_id), ret)]
pub fn clock_getres<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    clock_id: i32,
    timespec: WasmPtr<i64, M>,
) -> Result<i32, WasiError> {
    let clock = match linux_clock_id(clock_id) {
        Some(clock) => clock,
        None => return Ok(-Errno::Inval.to_native()),
    };
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let out_addr = WasmPtr::<Timestamp, M>::new(timespec.offset()).deref(&memory);
    let resolution = match platform_clock_res_get(clock, out_addr) {
        Ok(resolution) => resolution,
        Err(errno) => return Ok(-errno.to_native()),
    };
    write_timespec(&ctx, timespec, resolution as i64)
}

fn write_timespec<M: MemorySize>(
    ctx: &FunctionEnvMut<'_, WasiEnv>,
    ptr: WasmPtr<i64, M>,
    nanos: i64,
) -> Result<i32, WasiError> {
    if ptr.is_null() {
        return Ok(-Errno::Fault.to_native());
    }
    let seconds = nanos / 1_000_000_000;
    let subsecond_nanos = nanos % 1_000_000_000;
    let env = ctx.data();
    let memory = unsafe { env.memory_view(ctx) };
    if let Err(err) = ptr.write(&memory, seconds) {
        return Ok(-crate::mem_error_to_wasi(err).to_native());
    }
    if M::is_64bit() {
        let nanos_ptr = match ptr_at_byte_offset::<i64, M>(ptr, 8) {
            Ok(ptr) => ptr,
            Err(errno) => return Ok(-errno.to_native()),
        };
        if let Err(err) = nanos_ptr.write(&memory, subsecond_nanos) {
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
        if let Err(err) = nanos_ptr.write(&memory, subsecond_nanos as i32) {
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
