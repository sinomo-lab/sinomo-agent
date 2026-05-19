use super::*;
use crate::{WasiProcess, WasiProcessId, syscalls::*};
use wasmer::FromToNativeWasmType;

/// ### `proc_getpgid()`
/// Returns the process group ID for the selected process.
#[instrument(level = "trace", skip_all, fields(%pid, pgid = field::Empty), ret)]
pub fn proc_getpgid<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    ret_pgid: WasmPtr<Pid, M>,
) -> Errno {
    let env = ctx.data();
    let Some(process) = selected_process(env, pid) else {
        return Errno::Badf;
    };
    let pgid = process.pgid().raw();
    Span::current().record("pgid", pgid);
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem!(ret_pgid.write(&memory, pgid as Pid));
    Errno::Success
}

/// ### `proc_getpgrp()`
/// Returns the current process group ID.
#[instrument(level = "trace", skip_all, fields(pgid = field::Empty), ret)]
pub fn proc_getpgrp<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    ret_pgid: WasmPtr<Pid, M>,
) -> Errno {
    let env = ctx.data();
    let pgid = env.process.pgid().raw();
    Span::current().record("pgid", pgid);
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem!(ret_pgid.write(&memory, pgid as Pid));
    Errno::Success
}

/// ### `getpgrp()`
/// POSIX-shaped process group id wrapper.
#[instrument(level = "trace", skip_all, fields(pgid = field::Empty), ret)]
pub fn getpgrp(ctx: FunctionEnvMut<'_, WasiEnv>) -> i32 {
    let pgid = ctx.data().process.pgid().raw();
    Span::current().record("pgid", pgid);
    pgid as i32
}

/// ### `getpgid()`
/// POSIX-shaped process group lookup.
#[instrument(level = "trace", skip_all, fields(%pid, pgid = field::Empty), ret)]
pub fn getpgid(ctx: FunctionEnvMut<'_, WasiEnv>, pid: Pid) -> i32 {
    let env = ctx.data();
    let Some(process) = selected_process(env, pid) else {
        return -Errno::Badf.to_native();
    };
    let pgid = process.pgid().raw();
    Span::current().record("pgid", pgid);
    pgid as i32
}

/// ### `proc_getsid()`
/// Returns the session ID for the selected process.
#[instrument(level = "trace", skip_all, fields(%pid, sid = field::Empty), ret)]
pub fn proc_getsid<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    ret_sid: WasmPtr<Pid, M>,
) -> Errno {
    let env = ctx.data();
    let Some(process) = selected_process(env, pid) else {
        return Errno::Badf;
    };
    let sid = process.session_id().raw();
    Span::current().record("sid", sid);
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem!(ret_sid.write(&memory, sid as Pid));
    Errno::Success
}

/// ### `getsid()`
/// POSIX-shaped session id lookup.
#[instrument(level = "trace", skip_all, fields(%pid, sid = field::Empty), ret)]
pub fn getsid(ctx: FunctionEnvMut<'_, WasiEnv>, pid: Pid) -> i32 {
    let env = ctx.data();
    let Some(process) = selected_process(env, pid) else {
        return -Errno::Badf.to_native();
    };
    let sid = process.session_id().raw();
    Span::current().record("sid", sid);
    sid as i32
}

/// ### `proc_setpgid()`
/// Sets the process group ID for the current process or one of its children.
#[instrument(level = "trace", skip_all, fields(%pid, %pgid), ret)]
pub fn proc_setpgid(mut ctx: FunctionEnvMut<'_, WasiEnv>, pid: Pid, pgid: Pid) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let target_pid = if pid == 0 {
        env.process.pid()
    } else {
        WasiProcessId::from(pid)
    };
    let target_pgid = if pgid == 0 {
        target_pid
    } else {
        WasiProcessId::from(pgid)
    };

    let Some(mut process) = selected_process(env, pid) else {
        return Ok(Errno::Badf);
    };
    if process.pid() != env.process.pid() && process.ppid() != env.process.pid() {
        return Ok(Errno::Perm);
    }
    if process.session_id() == process.pid() {
        return Ok(Errno::Perm);
    }
    if process.session_id() != env.process.session_id() {
        return Ok(Errno::Perm);
    }
    if target_pgid != target_pid && !process_group_exists_in_session(env, target_pgid, process.session_id()) {
        return Ok(Errno::Perm);
    }

    process.set_process_group_id(target_pgid);
    Ok(Errno::Success)
}

/// ### `setpgid()`
/// POSIX-shaped process group setter. Returns 0 on success or a negative errno.
#[instrument(level = "trace", skip_all, fields(%pid, %pgid), ret)]
pub fn setpgid(ctx: FunctionEnvMut<'_, WasiEnv>, pid: Pid, pgid: Pid) -> Result<i32, WasiError> {
    Ok(errno_to_posix_unit(proc_setpgid(ctx, pid, pgid)?))
}

/// ### `proc_setsid()`
/// Creates a new session with the current process as session and group leader.
#[instrument(level = "trace", skip_all, fields(sid = field::Empty), ret)]
pub fn proc_setsid<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ret_sid: WasmPtr<Pid, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    if env.process.pgid() == env.process.pid() {
        return Ok(Errno::Perm);
    }

    let mut process = env.process.clone();
    process.create_session();
    let sid = process.session_id().raw();
    Span::current().record("sid", sid);
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_sid.write(&memory, sid as Pid));
    Ok(Errno::Success)
}

/// ### `setsid()`
/// POSIX-shaped session creator. Returns the new session id or a negative errno.
#[instrument(level = "trace", skip_all, fields(sid = field::Empty), ret)]
pub fn setsid(mut ctx: FunctionEnvMut<'_, WasiEnv>) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    if env.process.pgid() == env.process.pid() {
        return Ok(-Errno::Perm.to_native());
    }

    let mut process = env.process.clone();
    process.create_session();
    let sid = process.session_id().raw();
    Span::current().record("sid", sid);
    Ok(sid as i32)
}

/// ### `proc_tcgetpgrp()`
/// Returns the foreground process group for the controlling terminal.
#[instrument(level = "trace", skip_all, fields(%fd, pgid = field::Empty), ret)]
pub fn proc_tcgetpgrp<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    ret_pgid: WasmPtr<Pid, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    if let Err(err) = validate_terminal_fd(env, fd) {
        return Ok(err);
    }
    let pgid = env
        .runtime
        .tty()
        .and_then(|bridge| bridge.foreground_process_group())
        .unwrap_or_else(|| env.process.pgid().raw());
    Span::current().record("pgid", pgid);
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_pgid.write(&memory, pgid as Pid));
    Ok(Errno::Success)
}

/// ### `proc_tcgetsid()`
/// Returns the session ID associated with the controlling terminal.
#[instrument(level = "trace", skip_all, fields(%fd, sid = field::Empty), ret)]
pub fn proc_tcgetsid<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    ret_sid: WasmPtr<Pid, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    if let Err(err) = validate_terminal_fd(env, fd) {
        return Ok(err);
    }
    let sid = terminal_session_id(env).unwrap_or_else(|| env.process.session_id());
    Span::current().record("sid", sid.raw());
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_sid.write(&memory, sid.raw() as Pid));
    Ok(Errno::Success)
}

/// ### `tcgetpgrp()`
/// POSIX-shaped foreground process-group lookup.
#[instrument(level = "trace", skip_all, fields(%fd, pgid = field::Empty), ret)]
pub fn tcgetpgrp(mut ctx: FunctionEnvMut<'_, WasiEnv>, fd: WasiFd) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    if let Err(err) = validate_terminal_fd(env, fd) {
        return Ok(-err.to_native());
    }
    let pgid = env
        .runtime
        .tty()
        .and_then(|bridge| bridge.foreground_process_group())
        .unwrap_or_else(|| env.process.pgid().raw());
    Span::current().record("pgid", pgid);
    Ok(pgid as i32)
}

/// ### `tcgetsid()`
/// POSIX-shaped terminal session lookup.
#[instrument(level = "trace", skip_all, fields(%fd, sid = field::Empty), ret)]
pub fn tcgetsid(mut ctx: FunctionEnvMut<'_, WasiEnv>, fd: WasiFd) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    if let Err(err) = validate_terminal_fd(env, fd) {
        return Ok(-err.to_native());
    }
    let sid = terminal_session_id(env).unwrap_or_else(|| env.process.session_id());
    Span::current().record("sid", sid.raw());
    Ok(sid.raw() as i32)
}

/// ### `proc_tcsetpgrp()`
/// Sets the foreground process group for the controlling terminal.
#[instrument(level = "trace", skip_all, fields(%fd, %pgid), ret)]
pub fn proc_tcsetpgrp(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    pgid: Pid,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    if pgid == 0 {
        return Ok(Errno::Inval);
    }
    if let Err(err) = validate_terminal_fd(env, fd) {
        return Ok(err);
    }
    if env.runtime.tty().is_none() {
        return Ok(Errno::Notty);
    }
    if is_background_terminal_caller(env) {
        env.process.signal_process_direct(Signal::Sigttou);
        match WasiEnv::process_signals_and_exit(&mut ctx)? {
            Ok(_) => {}
            Err(err) => return Ok(err),
        }
    }

    let env = ctx.data();
    let Some(bridge) = env.runtime.tty() else {
        return Ok(Errno::Notty);
    };
    let pgid = WasiProcessId::from(pgid);
    if !process_group_exists_in_session(env, pgid, env.process.session_id()) {
        return Ok(Errno::Perm);
    }
    bridge.set_foreground_process_group(pgid.raw());
    Ok(Errno::Success)
}

/// ### `tcsetpgrp()`
/// POSIX-shaped foreground process-group setter.
#[instrument(level = "trace", skip_all, fields(%fd, %pgid), ret)]
pub fn tcsetpgrp(ctx: FunctionEnvMut<'_, WasiEnv>, fd: WasiFd, pgid: Pid) -> Result<i32, WasiError> {
    Ok(errno_to_posix_unit(proc_tcsetpgrp(ctx, fd, pgid)?))
}

/// ### `proc_tty_get_tostop()`
/// Returns whether background writes to the controlling terminal should stop.
#[instrument(level = "trace", skip_all, fields(tostop = field::Empty), ret)]
pub fn proc_tty_get_tostop<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ret_tostop: WasmPtr<Bool, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let tostop = env
        .runtime
        .tty()
        .map(|bridge| bridge.tty_get().tostop)
        .unwrap_or_default();
    Span::current().record("tostop", tostop);
    let memory = unsafe { env.memory_view(&ctx) };
    let tostop = if tostop { Bool::True } else { Bool::False };
    wasi_try_mem_ok!(ret_tostop.write(&memory, tostop));
    Ok(Errno::Success)
}

/// ### `proc_tty_set_tostop()`
/// Updates whether background writes to the controlling terminal should stop.
#[instrument(level = "trace", skip_all, fields(%tostop), ret)]
pub fn proc_tty_set_tostop(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    tostop: Bool,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let Some(bridge) = env.runtime.tty() else {
        return Ok(Errno::Notty);
    };
    let mut state = bridge.tty_get();
    state.tostop = tostop == Bool::True;
    bridge.tty_set(state);
    Ok(Errno::Success)
}

fn selected_process(env: &WasiEnv, pid: Pid) -> Option<WasiProcess> {
    if pid == 0 || WasiProcessId::from(pid) == env.process.pid() {
        Some(env.process.clone())
    } else {
        env.control_plane.get_process(WasiProcessId::from(pid))
    }
}

fn errno_to_posix_unit(errno: Errno) -> i32 {
    if errno == Errno::Success {
        0
    } else {
        -errno.to_native()
    }
}

fn process_group_exists_in_session(
    env: &WasiEnv,
    pgid: WasiProcessId,
    session_id: WasiProcessId,
) -> bool {
    env.control_plane
        .processes()
        .into_iter()
        .any(|process| process.pgid() == pgid && process.session_id() == session_id)
}

fn validate_terminal_fd(env: &WasiEnv, fd: WasiFd) -> Result<(), Errno> {
    env.state.fs.get_fd(fd)?;
    if env.runtime.tty().is_some() {
        Ok(())
    } else {
        Err(Errno::Notty)
    }
}

fn is_background_terminal_caller(env: &WasiEnv) -> bool {
    let Some(bridge) = env.runtime.tty() else {
        return false;
    };
    bridge
        .foreground_process_group()
        .is_some_and(|fg_pgid| fg_pgid != env.process.pgid().raw())
}

fn terminal_session_id(env: &WasiEnv) -> Option<WasiProcessId> {
    let foreground_group = env.runtime.tty()?.foreground_process_group()?;
    env.control_plane
        .processes()
        .into_iter()
        .find(|process| process.pgid().raw() == foreground_group)
        .map(|process| process.session_id())
}
