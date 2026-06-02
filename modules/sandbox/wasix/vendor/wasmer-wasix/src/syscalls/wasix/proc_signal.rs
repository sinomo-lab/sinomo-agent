use super::*;
use crate::{WasiProcess, os::task::TaskStatus, syscalls::*};
use wasmer::FromToNativeWasmType;

const LINUX_O_CLOEXEC: i32 = 0o2000000;
const LINUX_O_NONBLOCK: i32 = 0o4000;

/// ### `proc_signal()`
/// Sends a signal to a child process
///
/// ## Parameters
///
/// * `pid` - Handle of the child process to wait on
/// * `sig` - Signal to send the child process
#[instrument(level = "trace", skip_all, fields(%pid, ?sig), ret)]
pub fn proc_signal(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    sig: Signal,
) -> Result<Errno, WasiError> {
    let env = ctx.data();
    let pid_signed = pid as i32;
    let ret = match pid_signed {
        0 => signal_process_group(env, env.process.pgid(), sig),
        -1 => signal_all_processes(env, sig),
        pid if pid < -1 => match pid.checked_neg() {
            Some(pgid) => signal_process_group(env, WasiProcessId::from(pgid as u32), sig),
            None => Errno::Inval,
        },
        _ => {
            let pid: WasiProcessId = pid.into();
            if let Some(process) = env.control_plane.get_process(pid) {
                if sig != Signal::Signone {
                    for tid in process.signal_process_direct(sig) {
                        wake_futex_waiters_for_thread(env.state.deref(), tid.raw());
                        wake_shared_cond_waiters_for_thread(env.state.deref(), tid.raw());
                    }
                }
                Errno::Success
            } else {
                Errno::Srch
            }
        }
    };

    WasiEnv::do_pending_operations(&mut ctx)?;

    Ok(ret)
}

/// ### `kill()`
/// POSIX-shaped signal delivery. Returns 0 or a negative errno.
#[instrument(level = "trace", skip_all, fields(%pid, ?sig), ret)]
pub fn kill(ctx: FunctionEnvMut<'_, WasiEnv>, pid: Pid, sig: Signal) -> Result<i32, WasiError> {
    let errno = proc_signal(ctx, pid, sig)?;
    if errno == Errno::Success {
        Ok(0)
    } else {
        Ok(-errno.to_native())
    }
}

/// ### `pidfd_open()`
/// Linux-shaped process file descriptor creation. The descriptor is runtime
/// metadata over an eventfd handle so it can participate in the same fd table
/// and close lifecycle as other WASIX descriptors.
#[instrument(level = "trace", skip_all, fields(%pid, %flags, ret_fd = field::Empty), ret)]
pub fn pidfd_open(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    flags: i32,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if pid <= 0 || flags & !(LINUX_O_CLOEXEC | LINUX_O_NONBLOCK) != 0 {
        return Ok(-Errno::Inval.to_native());
    }

    let target_pid = WasiProcessId::from(pid as u32);
    if target_pid != ctx.data().process.pid()
        && ctx.data().control_plane.get_process(target_pid).is_none()
    {
        return Ok(-Errno::Badf.to_native());
    }

    let fd = match fd_event_internal(&mut ctx, 0, 0, None)? {
        Ok(fd) => fd,
        Err(err) => return Ok(-err.to_native()),
    };
    if let Err(err) = apply_pidfd_flags(&mut ctx, fd, flags) {
        let _ = ctx.data().state.fs.close_fd(fd);
        return Ok(-err.to_native());
    }

    ctx.data()
        .state
        .pidfds
        .lock()
        .unwrap()
        .insert(fd, target_pid.raw());
    sync_process_metadata(ctx.data());
    Span::current().record("ret_fd", fd);
    Ok(fd as i32)
}

/// ### `pidfd_send_signal()`
/// Sends a signal through a runtime-owned pidfd.
#[instrument(level = "trace", skip_all, fields(%pidfd, ?sig, %flags), ret)]
pub fn pidfd_send_signal(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    pidfd: WasiFd,
    sig: Signal,
    flags: u32,
) -> Result<i32, WasiError> {
    if flags != 0 {
        return Ok(-Errno::Inval.to_native());
    }
    let Some(pid) = lookup_pidfd(&ctx, pidfd) else {
        return Ok(-Errno::Badf.to_native());
    };
    kill(ctx, pid, sig)
}

/// ### `pidfd_getfd()`
/// Duplicates a target fd from the current process through a pidfd. Cross
/// process descriptor extraction is intentionally denied for now.
#[instrument(level = "trace", skip_all, fields(%pidfd, %targetfd, %flags), ret)]
pub fn pidfd_getfd(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pidfd: WasiFd,
    targetfd: WasiFd,
    flags: u32,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if flags != 0 {
        return Ok(-Errno::Inval.to_native());
    }
    let Some(pid) = lookup_pidfd(&ctx, pidfd) else {
        return Ok(-Errno::Badf.to_native());
    };
    if pid != ctx.data().process.pid().raw() as Pid {
        return Ok(-Errno::Perm.to_native());
    }
    match fd_dup_internal(&mut ctx, targetfd, 0, false) {
        Ok(fd) => {
            sync_process_metadata(ctx.data());
            Ok(fd as i32)
        }
        Err(err) => Ok(-err.to_native()),
    }
}

/// ### `pidfd_status()`
/// Returns 1 when a pidfd target has reached a terminal state, 0 when it is
/// still alive, or a negative errno on an invalid descriptor.
#[instrument(level = "trace", skip_all, fields(%pidfd), ret)]
pub fn pidfd_status(ctx: FunctionEnvMut<'_, WasiEnv>, pidfd: WasiFd) -> Result<i32, WasiError> {
    let Some(pid) = lookup_pidfd(&ctx, pidfd) else {
        return Ok(-Errno::Badf.to_native());
    };
    let target_pid = WasiProcessId::from(pid as u32);
    if target_pid == ctx.data().process.pid() {
        return Ok(0);
    }
    let Some(process) = ctx.data().control_plane.get_process(target_pid) else {
        return Ok(1);
    };
    Ok(matches!(
        process.task_status(),
        TaskStatus::Finished(_) | TaskStatus::Signaled { .. }
    ) as i32)
}

/// ### `pidfd_pid()`
/// Returns the process id associated with a runtime-owned pidfd.
#[instrument(level = "trace", skip_all, fields(%pidfd), ret)]
pub fn pidfd_pid(ctx: FunctionEnvMut<'_, WasiEnv>, pidfd: WasiFd) -> Result<i32, WasiError> {
    let Some(pid) = lookup_pidfd(&ctx, pidfd) else {
        return Ok(-Errno::Badf.to_native());
    };
    Ok(pid as i32)
}

fn lookup_pidfd(ctx: &FunctionEnvMut<'_, WasiEnv>, pidfd: WasiFd) -> Option<Pid> {
    ctx.data()
        .state
        .pidfds
        .lock()
        .unwrap()
        .get(&pidfd)
        .copied()
        .map(|pid| pid as Pid)
}

fn apply_pidfd_flags(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    flags: i32,
) -> Result<(), Errno> {
    if flags & LINUX_O_NONBLOCK != 0 {
        let ret =
            fd_fdstat_set_flags_internal(ctx, fd, Fdflags::NONBLOCK).map_err(|_| Errno::Fault)?;
        if ret != Errno::Success {
            return Err(ret);
        }
    }
    if flags & LINUX_O_CLOEXEC != 0 {
        let ret =
            fd_fdflags_set_internal(ctx, fd, Fdflagsext::CLOEXEC).map_err(|_| Errno::Fault)?;
        if ret != Errno::Success {
            return Err(ret);
        }
    }
    Ok(())
}

fn signal_process_group(env: &WasiEnv, pgid: WasiProcessId, sig: Signal) -> Errno {
    let targets = env
        .control_plane
        .processes()
        .into_iter()
        .filter(|process| process.pgid() == pgid)
        .collect::<Vec<_>>();
    signal_process_targets(env, targets, sig)
}

fn signal_all_processes(env: &WasiEnv, sig: Signal) -> Errno {
    signal_process_targets(env, env.control_plane.processes(), sig)
}

fn signal_process_targets(env: &WasiEnv, targets: Vec<WasiProcess>, sig: Signal) -> Errno {
    if targets.is_empty() {
        return Errno::Srch;
    }
    if sig != Signal::Signone {
        for process in targets {
            for tid in process.signal_process_direct(sig) {
                wake_futex_waiters_for_thread(env.state.deref(), tid.raw());
                wake_shared_cond_waiters_for_thread(env.state.deref(), tid.raw());
            }
        }
    }
    Errno::Success
}
