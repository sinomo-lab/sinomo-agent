use super::*;
use crate::{os::task::process::WasiProcessId, syscalls::*, WasiProcess};

const SCHED_OTHER: i32 = 0;
const SCHED_FIFO: i32 = 1;
const SCHED_RR: i32 = 2;
const SCHED_BATCH: i32 = 3;
const SCHED_IDLE: i32 = 5;
const SCHED_RESET_ON_FORK: i32 = 0x40000000;

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
    if !valid_policy(base_policy) || priority != 0 {
        return Ok(Errno::Inval);
    }
    process.set_scheduler(base_policy, priority);
    Ok(Errno::Success)
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

fn selected_process(env: &WasiEnv, pid: Pid) -> Option<WasiProcess> {
    if pid == 0 || pid == env.process.pid().raw() as Pid {
        return Some(env.process.clone());
    }
    env.control_plane.get_process(WasiProcessId::from(pid as u32))
}

fn valid_policy(policy: i32) -> bool {
    matches!(policy, SCHED_OTHER | SCHED_FIFO | SCHED_RR | SCHED_BATCH | SCHED_IDLE)
}
