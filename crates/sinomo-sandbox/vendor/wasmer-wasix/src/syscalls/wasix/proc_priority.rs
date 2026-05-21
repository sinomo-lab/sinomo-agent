use super::*;
use crate::WasiProcess;
use crate::syscalls::*;
use wasmer::FromToNativeWasmType;

const PRIO_PROCESS: i32 = 0;
const PRIO_PGRP: i32 = 1;
const PRIO_USER: i32 = 2;
const PRIO_MIN: i32 = -20;
const PRIO_MAX: i32 = 20;

/// ### `proc_getpriority()`
/// Returns the guest-visible POSIX nice value for a process, process group, or user.
#[instrument(level = "trace", skip_all, fields(%which, %who), ret)]
pub fn proc_getpriority<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    which: i32,
    who: i32,
    priority_ptr: WasmPtr<i32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let priorities = match matching_priorities(env, which, who) {
        Ok(priorities) => priorities,
        Err(err) => return Ok(err),
    };
    let Some(priority) = priorities.into_iter().min() else {
        return Ok(Errno::Srch);
    };
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(priority_ptr.write(&memory, priority));
    Ok(Errno::Success)
}

/// ### `getpriority()`
/// POSIX-shaped nice-value query. Returns the selected nice value or a
/// negative errno.
#[instrument(level = "trace", skip_all, fields(%which, %who), ret)]
pub fn getpriority(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    which: i32,
    who: i32,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let priorities = match matching_priorities(env, which, who) {
        Ok(priorities) => priorities,
        Err(err) => return Ok(-err.to_native()),
    };
    let Some(priority) = priorities.into_iter().min() else {
        return Ok(-Errno::Srch.to_native());
    };
    Ok(priority)
}

/// ### `proc_setpriority()`
/// Sets the guest-visible POSIX nice value for a process, process group, or user.
#[instrument(level = "trace", skip_all, fields(%which, %who, %priority), ret)]
pub fn proc_setpriority(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    which: i32,
    who: i32,
    priority: i32,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if !(PRIO_MIN..=PRIO_MAX).contains(&priority) {
        return Ok(Errno::Inval);
    }

    let env = ctx.data();
    let processes = match matching_processes(env, which, who) {
        Ok(processes) => processes,
        Err(err) => return Ok(err),
    };
    if processes.is_empty() {
        return Ok(Errno::Srch);
    }
    for process in processes {
        process.set_nice(priority);
    }
    Ok(Errno::Success)
}

/// ### `setpriority()`
/// POSIX-shaped nice-value update. Returns 0 or a negative errno.
#[instrument(level = "trace", skip_all, fields(%which, %who, %priority), ret)]
pub fn setpriority(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    which: i32,
    who: i32,
    priority: i32,
) -> Result<i32, WasiError> {
    let errno = proc_setpriority(ctx, which, who, priority)?;
    if errno == Errno::Success {
        Ok(0)
    } else {
        Ok(-errno.to_native())
    }
}

/// ### `nice()`
/// Adjusts the calling process nice value and returns the new value or a
/// negative errno.
#[instrument(level = "trace", skip_all, fields(%increment), ret)]
pub fn nice(mut ctx: FunctionEnvMut<'_, WasiEnv>, increment: i32) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let current = ctx.data().process.nice();
    let next = current.saturating_add(increment).clamp(PRIO_MIN, PRIO_MAX);
    let errno = proc_setpriority(ctx, PRIO_PROCESS, 0, next)?;
    if errno == Errno::Success {
        Ok(next)
    } else {
        Ok(-errno.to_native())
    }
}

fn matching_priorities(env: &WasiEnv, which: i32, who: i32) -> Result<Vec<i32>, Errno> {
    matching_processes(env, which, who).map(|processes| {
        processes
            .into_iter()
            .map(|process| process.nice())
            .collect()
    })
}

fn matching_processes(env: &WasiEnv, which: i32, who: i32) -> Result<Vec<WasiProcess>, Errno> {
    let target = normalized_target(env, which, who)?;
    let processes = env.control_plane.processes();
    let matched = processes
        .into_iter()
        .filter(|process| match target {
            PriorityTarget::Process(pid) => process.pid() == pid,
            PriorityTarget::ProcessGroup(pgid) => process.pgid() == pgid,
            PriorityTarget::User(uid) => process.identity().real_uid == uid,
        })
        .collect();
    Ok(matched)
}

fn normalized_target(env: &WasiEnv, which: i32, who: i32) -> Result<PriorityTarget, Errno> {
    match which {
        PRIO_PROCESS => {
            let pid = if who == 0 {
                env.process.pid()
            } else {
                WasiProcessId::from(u32::try_from(who).map_err(|_| Errno::Inval)?)
            };
            Ok(PriorityTarget::Process(pid))
        }
        PRIO_PGRP => {
            let pgid = if who == 0 {
                env.process.pgid()
            } else {
                WasiProcessId::from(u32::try_from(who).map_err(|_| Errno::Inval)?)
            };
            Ok(PriorityTarget::ProcessGroup(pgid))
        }
        PRIO_USER => {
            let uid = if who == 0 {
                env.process.identity().real_uid
            } else {
                u32::try_from(who).map_err(|_| Errno::Inval)?
            };
            Ok(PriorityTarget::User(uid))
        }
        _ => Err(Errno::Inval),
    }
}

enum PriorityTarget {
    Process(WasiProcessId),
    ProcessGroup(WasiProcessId),
    User(u32),
}
