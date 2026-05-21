use super::*;
use crate::{WasiProcess, os::task::process::WasiProcessId, syscalls::*};
use wasmer::FromToNativeWasmType;

const SCHED_OTHER: i32 = 0;
const SCHED_FIFO: i32 = 1;
const SCHED_RR: i32 = 2;
const SCHED_BATCH: i32 = 3;
const SCHED_IDLE: i32 = 5;
const SCHED_RESET_ON_FORK: i32 = 0x40000000;
const ROUND_ROBIN_INTERVAL_NANOS: i64 = 10_000_000;
const REALTIME_PRIORITY_MIN: i32 = 0;
const REALTIME_PRIORITY_MAX: i32 = 99;

/// ### `proc_getscheduler()`
/// Returns guest-visible POSIX scheduler metadata for a process.
#[instrument(level = "trace", skip_all, fields(%pid), ret)]
pub fn proc_getscheduler<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    policy_ptr: WasmPtr<i32, M>,
    priority_ptr: WasmPtr<i32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let Some(process) = selected_process(env, pid) else {
        return Ok(Errno::Srch);
    };
    let (policy, priority) = process.scheduler();
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(policy_ptr.write(&memory, policy));
    wasi_try_mem_ok!(priority_ptr.write(&memory, priority));
    Ok(Errno::Success)
}

/// ### `sched_getscheduler()`
/// POSIX-shaped scheduler policy query. Returns the policy or a negative errno.
#[instrument(level = "trace", skip_all, fields(%pid), ret)]
pub fn sched_getscheduler<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let Some(process) = selected_process(env, pid) else {
        return Ok(-Errno::Srch.to_native());
    };
    let (policy, _) = process.scheduler();
    Ok(policy)
}

/// ### `sched_getparam()`
/// POSIX-shaped scheduler priority query. Writes the priority value and returns
/// 0 or a negative errno.
#[instrument(level = "trace", skip_all, fields(%pid), ret)]
pub fn sched_getparam<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    priority_ptr: WasmPtr<i32, M>,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let Some(process) = selected_process(env, pid) else {
        return Ok(-Errno::Srch.to_native());
    };
    let (_, priority) = process.scheduler();
    let memory = unsafe { env.memory_view(&ctx) };
    if let Err(err) = priority_ptr.write(&memory, priority) {
        return Ok(-crate::mem_error_to_wasi(err).to_native());
    }
    Ok(0)
}

/// ### `proc_setscheduler()`
/// Updates guest-visible POSIX scheduler metadata for a process.
#[instrument(level = "trace", skip_all, fields(%pid, %policy, %priority), ret)]
pub fn proc_setscheduler(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    policy: i32,
    priority: i32,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let Some(process) = selected_process(env, pid) else {
        return Ok(Errno::Srch);
    };
    let base_policy = policy & !SCHED_RESET_ON_FORK;
    if !valid_policy(base_policy) || !valid_priority(base_policy, priority) {
        return Ok(Errno::Inval);
    }
    process.set_scheduler(base_policy, priority);
    Ok(Errno::Success)
}

/// ### `sched_setscheduler()`
/// POSIX-shaped scheduler policy/priority update. Returns 0 or a negative
/// errno.
#[instrument(level = "trace", skip_all, fields(%pid, %policy, %priority), ret)]
pub fn sched_setscheduler(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    policy: i32,
    priority: i32,
) -> Result<i32, WasiError> {
    let errno = proc_setscheduler(ctx, pid, policy, priority)?;
    if errno == Errno::Success {
        Ok(0)
    } else {
        Ok(-errno.to_native())
    }
}

/// ### `sched_setparam()`
/// POSIX-shaped scheduler priority update that preserves the current policy.
/// Returns 0 or a negative errno.
#[instrument(level = "trace", skip_all, fields(%pid, %priority), ret)]
pub fn sched_setparam<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    priority: i32,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let Some(process) = selected_process(env, pid) else {
        return Ok(-Errno::Srch.to_native());
    };
    let (policy, _) = process.scheduler();
    let errno = proc_setscheduler(ctx, pid, policy, priority)?;
    if errno == Errno::Success {
        Ok(0)
    } else {
        Ok(-errno.to_native())
    }
}

/// ### `proc_getaffinity()`
/// Returns guest-visible POSIX CPU affinity metadata for a process.
#[instrument(level = "trace", skip_all, fields(%pid), ret)]
pub fn proc_getaffinity<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    mask_low_ptr: WasmPtr<u64, M>,
    mask_high_ptr: WasmPtr<u64, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let Some(process) = selected_process(env, pid) else {
        return Ok(Errno::Srch);
    };
    let affinity = process.cpu_affinity();
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(mask_low_ptr.write(&memory, affinity as u64));
    wasi_try_mem_ok!(mask_high_ptr.write(&memory, (affinity >> 64) as u64));
    Ok(Errno::Success)
}

/// ### `sched_getaffinity()`
/// POSIX-shaped CPU affinity query. Writes a 128-bit affinity mask as low and
/// high u64 values and returns 0 or a negative errno.
#[instrument(level = "trace", skip_all, fields(%pid), ret)]
pub fn sched_getaffinity<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    mask_low_ptr: WasmPtr<u64, M>,
    mask_high_ptr: WasmPtr<u64, M>,
) -> Result<i32, WasiError> {
    let errno = proc_getaffinity(ctx, pid, mask_low_ptr, mask_high_ptr)?;
    if errno == Errno::Success {
        Ok(0)
    } else {
        Ok(-errno.to_native())
    }
}

/// ### `proc_setaffinity()`
/// Updates guest-visible POSIX CPU affinity metadata for a process.
#[instrument(level = "trace", skip_all, fields(%pid, %mask_low, %mask_high), ret)]
pub fn proc_setaffinity(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    mask_low: u64,
    mask_high: u64,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let Some(process) = selected_process(env, pid) else {
        return Ok(Errno::Srch);
    };
    let affinity = mask_low as u128 | ((mask_high as u128) << 64);
    if affinity == 0 {
        return Ok(Errno::Inval);
    }
    process.set_cpu_affinity(affinity);
    Ok(Errno::Success)
}

/// ### `sched_setaffinity()`
/// POSIX-shaped CPU affinity update. Accepts a 128-bit affinity mask split into
/// low and high u64 values and returns 0 or a negative errno.
#[instrument(level = "trace", skip_all, fields(%pid, %mask_low, %mask_high), ret)]
pub fn sched_setaffinity(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    mask_low: u64,
    mask_high: u64,
) -> Result<i32, WasiError> {
    let errno = proc_setaffinity(ctx, pid, mask_low, mask_high)?;
    if errno == Errno::Success {
        Ok(0)
    } else {
        Ok(-errno.to_native())
    }
}

/// ### `proc_getcpu()`
/// Returns the guest-visible current CPU number.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_getcpu<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    cpu_ptr: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(cpu_ptr.write(&memory, env.process.current_cpu()));
    Ok(Errno::Success)
}

/// ### `sched_getcpu()`
/// Linux-shaped current CPU query. Returns the current CPU number or a negative
/// errno.
#[instrument(level = "trace", skip_all, ret)]
pub fn sched_getcpu(mut ctx: FunctionEnvMut<'_, WasiEnv>) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let cpu = ctx.data().process.current_cpu();
    match i32::try_from(cpu) {
        Ok(cpu) => Ok(cpu),
        Err(_) => Ok(-Errno::Overflow.to_native()),
    }
}

/// ### `sched_rr_get_interval()`
/// POSIX-shaped round-robin interval query. The runtime currently models
/// scheduler metadata, not real host RR slices, so it exposes a stable
/// sandbox-local 10ms interval after validating the target process.
#[instrument(level = "trace", skip_all, fields(%pid), ret)]
pub fn sched_rr_get_interval<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    interval_ptr: WasmPtr<i64, M>,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let Some(_) = selected_process(env, pid) else {
        return Ok(-Errno::Srch.to_native());
    };
    write_timespec(&ctx, interval_ptr, ROUND_ROBIN_INTERVAL_NANOS)
}

fn selected_process(env: &WasiEnv, pid: Pid) -> Option<WasiProcess> {
    if pid == 0 || pid == env.process.pid().raw() as Pid {
        return Some(env.process.clone());
    }
    env.control_plane
        .get_process(WasiProcessId::from(pid as u32))
}

fn valid_policy(policy: i32) -> bool {
    matches!(
        policy,
        SCHED_OTHER | SCHED_FIFO | SCHED_RR | SCHED_BATCH | SCHED_IDLE
    )
}

fn priority_bounds(policy: i32) -> Option<(i32, i32)> {
    match policy {
        SCHED_FIFO | SCHED_RR => Some((REALTIME_PRIORITY_MIN, REALTIME_PRIORITY_MAX)),
        SCHED_OTHER | SCHED_BATCH | SCHED_IDLE => Some((0, 0)),
        _ => None,
    }
}

fn valid_priority(policy: i32, priority: i32) -> bool {
    let Some((min, max)) = priority_bounds(policy) else {
        return false;
    };
    priority >= min && priority <= max
}

/// ### `sched_get_priority_max()`
/// POSIX-shaped scheduler priority bound query.
#[instrument(level = "trace", skip_all, fields(%policy), ret)]
pub fn sched_get_priority_max(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    policy: i32,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    if let Some((_, max)) = priority_bounds(policy) {
        Ok(max)
    } else {
        Ok(-Errno::Inval.to_native())
    }
}

/// ### `sched_get_priority_min()`
/// POSIX-shaped scheduler priority bound query.
#[instrument(level = "trace", skip_all, fields(%policy), ret)]
pub fn sched_get_priority_min(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    policy: i32,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    if let Some((min, _)) = priority_bounds(policy) {
        Ok(min)
    } else {
        Ok(-Errno::Inval.to_native())
    }
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
    let memory = unsafe { ctx.data().memory_view(ctx) };
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
