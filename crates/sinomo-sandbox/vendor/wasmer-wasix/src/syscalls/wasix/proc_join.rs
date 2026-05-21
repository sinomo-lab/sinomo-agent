use std::task::{Poll, Waker};

use serde::{Deserialize, Serialize};
use wasmer::FromToNativeWasmType;
use wasmer_wasix_types::wasi::{
    ErrnoSignal, JoinFlags, JoinStatus, JoinStatusType, JoinStatusUnion, OptionPid,
};

use super::*;
use crate::{WasiProcess, os::task::TaskStatus, syscalls::*};

macro_rules! sinomo_debug {
    ($($arg:tt)*) => {
        if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
            eprintln!($($arg)*);
        }
    };
}

const WAIT_STATUS_EXITED: u32 = 1 << 0;
const WAIT_STATUS_STOPPED: u32 = 1 << 1;
const WAIT_STATUS_CONTINUED: u32 = 1 << 2;
const POSIX_WNOHANG: u32 = 1;
const POSIX_WUNTRACED: u32 = 2;
const POSIX_WSTOPPED: u32 = 2;
const POSIX_WEXITED: u32 = 4;
const POSIX_WCONTINUED: u32 = 8;
const POSIX_WNOWAIT: u32 = 0x0100_0000;
const POSIX_SIGCHLD: i32 = 17;
const POSIX_SIGCONT: i32 = 18;
const POSIX_CLD_EXITED: i32 = 1;
const POSIX_CLD_KILLED: i32 = 2;
const POSIX_CLD_STOPPED: i32 = 5;
const POSIX_CLD_CONTINUED: i32 = 6;

#[derive(Serialize, Deserialize)]
enum JoinStatusResult {
    Nothing,
    NoStatus,
    ExitNormal(WasiProcessId, ExitCode),
    ExitSignal(WasiProcessId, ExitCode, Signal),
    Stopped(WasiProcessId, Signal),
    Continued(WasiProcessId),
    Err(Errno),
}

trait WaitReturn {
    fn wait_unwind() -> Self;
}

impl WaitReturn for Errno {
    fn wait_unwind() -> Self {
        Errno::Success
    }
}

impl WaitReturn for i32 {
    fn wait_unwind() -> Self {
        0
    }
}

/// ### `proc_join()`
/// Joins the child process, blocking this one until the other finishes
///
/// ## Parameters
///
/// * `pid` - Handle of the child process to wait on
//#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn proc_join<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid_ptr: WasmPtr<OptionPid, M>,
    flags: JoinFlags,
    status_ptr: WasmPtr<JoinStatus, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    proc_join_internal(ctx, pid_ptr, flags, status_ptr)
}

/// ### `proc_wait_status()`
/// Observes child process status without reaping it.
pub fn proc_wait_status<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid_ptr: WasmPtr<OptionPid, M>,
    flags: JoinFlags,
    status_ptr: WasmPtr<JoinStatus, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    proc_wait_status_internal(ctx, pid_ptr, flags, status_ptr)
}

/// ### `proc_wait_status_select()`
/// Observes selected child process status classes without reaping them.
pub fn proc_wait_status_select<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid_ptr: WasmPtr<OptionPid, M>,
    flags: JoinFlags,
    status_kinds: u32,
    status_ptr: WasmPtr<JoinStatus, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let observe_exited = status_kinds & WAIT_STATUS_EXITED != 0;
    let observe_stopped = status_kinds & WAIT_STATUS_STOPPED != 0;
    let observe_continued = status_kinds & WAIT_STATUS_CONTINUED != 0;
    if !observe_exited && !observe_stopped && !observe_continued {
        return Ok(Errno::Inval);
    }
    proc_wait_status_select_internal(
        ctx,
        pid_ptr,
        flags,
        status_ptr,
        observe_exited,
        observe_stopped,
        observe_continued,
    )
}

/// ### `waitpid()`
/// POSIX-shaped child wait wrapper. Returns a child pid, zero for `WNOHANG`
/// with no status, or a negative errno value on failure.
#[instrument(level = "trace", skip_all, fields(%pid, %options), ret)]
pub fn waitpid<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: i32,
    status_ptr: WasmPtr<i32, M>,
    options: u32,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    ctx = match maybe_snapshot::<M>(ctx)? {
        Ok(ctx) => ctx,
        Err(err) => return Ok(-err.to_native()),
    };

    let supported = POSIX_WNOHANG | POSIX_WUNTRACED | POSIX_WCONTINUED;
    if options & !supported != 0 {
        return Ok(-Errno::Inval.to_native());
    }

    let mut flags = JoinFlags::empty();
    if options & POSIX_WNOHANG != 0 {
        flags |= JoinFlags::NON_BLOCKING;
    }
    if options & POSIX_WUNTRACED != 0 {
        flags |= JoinFlags::WAKE_STOPPED;
    }
    let observe_stopped = options & POSIX_WUNTRACED != 0;
    let observe_continued = options & POSIX_WCONTINUED != 0;

    let ret_result = {
        move |ctx: FunctionEnvMut<'_, WasiEnv>, status: JoinStatusResult| {
            let (ret_pid, wait_status) = match status {
                JoinStatusResult::Nothing | JoinStatusResult::NoStatus => (0, None),
                JoinStatusResult::ExitNormal(pid, exit_code) => {
                    sinomo_debug!(
                        "sinomo-debug waitpid_result parent={} child={} exit_code={}",
                        ctx.data().pid().raw(),
                        pid.raw(),
                        exit_code.raw()
                    );
                    (
                        pid.raw() as i32,
                        Some(((exit_code.raw() as i32) & 0xff) << 8),
                    )
                }
                JoinStatusResult::ExitSignal(pid, _exit_code, signal) => {
                    sinomo_debug!(
                        "sinomo-debug waitpid_result parent={} child={} signal={}",
                        ctx.data().pid().raw(),
                        pid.raw(),
                        signal.to_native()
                    );
                    (pid.raw() as i32, Some(signal.to_native() & 0x7f))
                }
                JoinStatusResult::Stopped(pid, signal) => (
                    pid.raw() as i32,
                    Some(((signal.to_native() & 0xff) << 8) | 0x7f),
                ),
                JoinStatusResult::Continued(pid) => (pid.raw() as i32, Some(0xffff)),
                JoinStatusResult::Err(err) => return Ok(-err.to_native()),
            };

            if let Some(wait_status) = wait_status
                && !status_ptr.is_null()
            {
                let memory = unsafe { ctx.data().memory_view(&ctx) };
                if let Err(err) = status_ptr.write(&memory, wait_status) {
                    return Ok(-crate::mem_error_to_wasi(err).to_native());
                }
            }
            Ok(ret_pid)
        }
    };

    match pid {
        -1 => {
            let process = ctx.data().process.clone();
            wait_status_for_matching_child::<M, _, _, _>(
                ctx,
                process,
                flags,
                true,
                observe_stopped,
                observe_continued,
                true,
                |_| true,
                ret_result,
            )
        }
        0 => {
            let pgid = ctx.data().process.pgid();
            let process = ctx.data().process.clone();
            wait_status_for_matching_child::<M, _, _, _>(
                ctx,
                process,
                flags,
                true,
                observe_stopped,
                observe_continued,
                true,
                move |child| child.pgid() == pgid,
                ret_result,
            )
        }
        pid if pid < -1 => {
            let Some(group_pid) = pid.checked_neg() else {
                return Ok(-Errno::Inval.to_native());
            };
            let pgid = WasiProcessId::from(group_pid as u32);
            let process = ctx.data().process.clone();
            wait_status_for_matching_child::<M, _, _, _>(
                ctx,
                process,
                flags,
                true,
                observe_stopped,
                observe_continued,
                true,
                move |child| child.pgid() == pgid,
                ret_result,
            )
        }
        pid => {
            let child_pid = WasiProcessId::from(pid as u32);
            let process = {
                let inner = ctx.data().process.lock();
                inner
                    .children
                    .iter()
                    .find(|child| child.pid() == child_pid)
                    .cloned()
            };
            let Some(process) = process else {
                return Ok(-Errno::Child.to_native());
            };
            wait_status_for_process::<M, _, _>(
                ctx,
                process,
                flags,
                true,
                observe_stopped,
                observe_continued,
                true,
                ret_result,
            )
        }
    }
}

/// ### `waitid()`
/// POSIX-shaped wait-status query. This writes the scalar fields needed for
/// `siginfo_t` construction and returns 0 or a negative errno.
#[instrument(level = "trace", skip_all, fields(%pid, %options), ret)]
pub fn waitid<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: i32,
    options: u32,
    ret_pid: WasmPtr<i32, M>,
    ret_signo: WasmPtr<i32, M>,
    ret_code: WasmPtr<i32, M>,
    ret_status: WasmPtr<i32, M>,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    ctx = match maybe_snapshot::<M>(ctx)? {
        Ok(ctx) => ctx,
        Err(err) => return Ok(-err.to_native()),
    };

    let observed = POSIX_WEXITED | POSIX_WSTOPPED | POSIX_WCONTINUED;
    let supported = observed | POSIX_WNOHANG | POSIX_WNOWAIT;
    if options & observed == 0 || options & !supported != 0 {
        return Ok(-Errno::Inval.to_native());
    }

    let mut flags = JoinFlags::empty();
    if options & POSIX_WNOHANG != 0 {
        flags |= JoinFlags::NON_BLOCKING;
    }
    if options & POSIX_WSTOPPED != 0 {
        flags |= JoinFlags::WAKE_STOPPED;
    }

    let observe_exited = options & POSIX_WEXITED != 0;
    let observe_stopped = options & POSIX_WSTOPPED != 0;
    let observe_continued = options & POSIX_WCONTINUED != 0;
    let consume_continued = options & POSIX_WNOWAIT == 0;

    let ret_result = {
        move |ctx: FunctionEnvMut<'_, WasiEnv>, status: JoinStatusResult| {
            let (pid_value, signo, code, status_value) = match status {
                JoinStatusResult::Nothing | JoinStatusResult::NoStatus => (0, 0, 0, 0),
                JoinStatusResult::ExitNormal(pid, exit_code) => (
                    pid.raw() as i32,
                    POSIX_SIGCHLD,
                    POSIX_CLD_EXITED,
                    exit_code.raw() as i32,
                ),
                JoinStatusResult::ExitSignal(pid, _exit_code, signal) => (
                    pid.raw() as i32,
                    POSIX_SIGCHLD,
                    POSIX_CLD_KILLED,
                    signal.to_native(),
                ),
                JoinStatusResult::Stopped(pid, signal) => (
                    pid.raw() as i32,
                    POSIX_SIGCHLD,
                    POSIX_CLD_STOPPED,
                    signal.to_native(),
                ),
                JoinStatusResult::Continued(pid) => {
                    if consume_continued {
                        let _ =
                            continued_matching_child(&ctx.data().process, pid.raw() as Pid, true);
                    }
                    (
                        pid.raw() as i32,
                        POSIX_SIGCHLD,
                        POSIX_CLD_CONTINUED,
                        POSIX_SIGCONT,
                    )
                }
                JoinStatusResult::Err(err) => return Ok(-err.to_native()),
            };

            let memory = unsafe { ctx.data().memory_view(&ctx) };
            for (ptr, value) in [
                (ret_pid, pid_value),
                (ret_signo, signo),
                (ret_code, code),
                (ret_status, status_value),
            ] {
                if ptr.is_null() {
                    return Ok(-Errno::Fault.to_native());
                }
                if let Err(err) = ptr.write(&memory, value) {
                    return Ok(-crate::mem_error_to_wasi(err).to_native());
                }
            }
            Ok(0)
        }
    };

    match pid {
        -1 => {
            let process = ctx.data().process.clone();
            wait_status_for_matching_child::<M, _, _, _>(
                ctx,
                process,
                flags,
                observe_exited,
                observe_stopped,
                observe_continued,
                options & POSIX_WNOWAIT == 0,
                |_| true,
                ret_result,
            )
        }
        0 => {
            let pgid = ctx.data().process.pgid();
            let process = ctx.data().process.clone();
            wait_status_for_matching_child::<M, _, _, _>(
                ctx,
                process,
                flags,
                observe_exited,
                observe_stopped,
                observe_continued,
                options & POSIX_WNOWAIT == 0,
                move |child| child.pgid() == pgid,
                ret_result,
            )
        }
        pid if pid < -1 => {
            let Some(group_pid) = pid.checked_neg() else {
                return Ok(-Errno::Inval.to_native());
            };
            let pgid = WasiProcessId::from(group_pid as u32);
            let process = ctx.data().process.clone();
            wait_status_for_matching_child::<M, _, _, _>(
                ctx,
                process,
                flags,
                observe_exited,
                observe_stopped,
                observe_continued,
                options & POSIX_WNOWAIT == 0,
                move |child| child.pgid() == pgid,
                ret_result,
            )
        }
        pid => {
            let child_pid = WasiProcessId::from(pid as u32);
            let process = {
                let inner = ctx.data().process.lock();
                inner
                    .children
                    .iter()
                    .find(|child| child.pid() == child_pid)
                    .cloned()
            };
            let Some(process) = process else {
                return Ok(-Errno::Child.to_native());
            };
            wait_status_for_process::<M, _, _>(
                ctx,
                process,
                flags,
                observe_exited,
                observe_stopped,
                observe_continued,
                options & POSIX_WNOWAIT == 0,
                ret_result,
            )
        }
    }
}

/// ### `proc_child_continued_status()`
/// Observes or consumes a pending child continued status for POSIX WCONTINUED.
#[instrument(level = "trace", skip_all, fields(%pid, %consume), ret)]
pub fn proc_child_continued_status<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    consume: Bool,
    ret_pid: WasmPtr<Pid, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let selected = continued_matching_child(&env.process, pid, consume == Bool::True);
    let memory = unsafe { env.memory_view(&ctx) };
    let ret = selected.map(|pid| pid.raw()).unwrap_or_default() as Pid;
    wasi_try_mem_ok!(ret_pid.write(&memory, ret));
    Ok(Errno::Success)
}

/// ### `proc_child_continued_status_wait()`
/// Observes or consumes a pending child continued status for POSIX WCONTINUED,
/// blocking until one arrives when requested by the caller.
#[instrument(
    level = "trace",
    skip_all,
    fields(%pid, %consume, %nonblocking),
    ret
)]
pub fn proc_child_continued_status_wait<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    consume: Bool,
    nonblocking: Bool,
    ret_pid: WasmPtr<Pid, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    ctx = wasi_try_ok!(maybe_snapshot::<M>(ctx)?);

    let ret_result = {
        move |ctx: FunctionEnvMut<'_, WasiEnv>, status: Result<Option<WasiProcessId>, Errno>| {
            let mut ret = Errno::Success;
            let pid = match status {
                Ok(Some(pid)) => pid.raw() as Pid,
                Ok(None) => 0,
                Err(err) => {
                    ret = err;
                    0
                }
            };
            let memory = unsafe { ctx.data().memory_view(&ctx) };
            wasi_try_mem_ok!(ret_pid.write(&memory, pid));
            Ok(ret)
        }
    };

    if let Some(status) = unsafe { handle_rewind::<M, _>(&mut ctx) } {
        return ret_result(ctx, status);
    }

    let process = ctx.data().process.clone();
    let consume = consume == Bool::True;
    if let Some(selected) = continued_matching_child(&process, pid, consume) {
        return ret_result(ctx, Ok(Some(selected)));
    }
    if nonblocking == Bool::True {
        return ret_result(ctx, Ok(None));
    }
    if !has_live_matching_child(&process, pid) {
        return ret_result(ctx, Err(Errno::Child));
    }

    let res = __asyncify_with_deep_sleep::<M, _, _>(ctx, async move {
        futures::future::poll_fn(move |cx| {
            if let Some(selected) = continued_matching_child(&process, pid, consume) {
                return Poll::Ready(Ok(Some(selected)));
            }
            if !has_live_matching_child(&process, pid) {
                return Poll::Ready(Err(Errno::Child));
            }
            process.subscribe_status_change(cx.waker());
            if let Some(selected) = continued_matching_child(&process, pid, consume) {
                return Poll::Ready(Ok(Some(selected)));
            }
            Poll::Pending
        })
        .await
    })?;
    match res {
        AsyncifyAction::Finish(ctx, result) => ret_result(ctx, result),
        AsyncifyAction::Unwind => Ok(Errno::Success),
    }
}

pub(super) fn proc_join_internal<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid_ptr: WasmPtr<OptionPid, M>,
    flags: JoinFlags,
    status_ptr: WasmPtr<JoinStatus, M>,
) -> Result<Errno, WasiError> {
    ctx = wasi_try_ok!(maybe_snapshot::<M>(ctx)?);

    // This lambda will look at what we wrote in the status variable
    // and use this to determine the return code sent back to the caller
    let ret_result = {
        move |ctx: FunctionEnvMut<'_, WasiEnv>, status: JoinStatusResult| {
            let mut ret = Errno::Success;

            let view = unsafe { ctx.data().memory_view(&ctx) };
            if let Err(err) = sync_shared_memory_to_guest::<M>(ctx.data(), &view) {
                return Ok(err);
            }
            let status = match status {
                JoinStatusResult::Nothing => JoinStatus {
                    tag: JoinStatusType::Nothing,
                    u: JoinStatusUnion { nothing: 0 },
                },
                JoinStatusResult::NoStatus => {
                    let option_pid = OptionPid {
                        tag: OptionTag::Some,
                        pid: 0,
                    };
                    pid_ptr.write(&view, option_pid).ok();

                    JoinStatus {
                        tag: JoinStatusType::Nothing,
                        u: JoinStatusUnion { nothing: 0 },
                    }
                }
                JoinStatusResult::ExitNormal(pid, exit_code) => {
                    sinomo_debug!(
                        "sinomo-debug proc_join_result parent={} child={} exit_code={}",
                        ctx.data().pid().raw(),
                        pid.raw(),
                        exit_code.raw()
                    );
                    let option_pid = OptionPid {
                        tag: OptionTag::Some,
                        pid: pid.raw() as Pid,
                    };
                    pid_ptr.write(&view, option_pid).ok();

                    JoinStatus {
                        tag: JoinStatusType::ExitNormal,
                        u: JoinStatusUnion {
                            exit_normal: exit_code.into(),
                        },
                    }
                }
                JoinStatusResult::ExitSignal(pid, exit_code, signal) => {
                    sinomo_debug!(
                        "sinomo-debug proc_join_result parent={} child={} signal={} exit_code={}",
                        ctx.data().pid().raw(),
                        pid.raw(),
                        signal.to_native(),
                        exit_code.raw()
                    );
                    let option_pid = OptionPid {
                        tag: OptionTag::Some,
                        pid: pid.raw() as Pid,
                    };
                    pid_ptr.write(&view, option_pid).ok();

                    JoinStatus {
                        tag: JoinStatusType::ExitSignal,
                        u: JoinStatusUnion {
                            exit_signal: ErrnoSignal {
                                exit_code: exit_code.into(),
                                signal,
                            },
                        },
                    }
                }
                JoinStatusResult::Stopped(pid, signal) => {
                    let option_pid = OptionPid {
                        tag: OptionTag::Some,
                        pid: pid.raw() as Pid,
                    };
                    pid_ptr.write(&view, option_pid).ok();

                    JoinStatus {
                        tag: JoinStatusType::Stopped,
                        u: JoinStatusUnion { stopped: signal },
                    }
                }
                JoinStatusResult::Continued(pid) => {
                    let option_pid = OptionPid {
                        tag: OptionTag::Some,
                        pid: pid.raw() as Pid,
                    };
                    pid_ptr.write(&view, option_pid).ok();

                    JoinStatus {
                        tag: JoinStatusType::Nothing,
                        u: JoinStatusUnion { nothing: 0 },
                    }
                }
                JoinStatusResult::Err(err) => {
                    ret = err;
                    JoinStatus {
                        tag: JoinStatusType::Nothing,
                        u: JoinStatusUnion { nothing: 0 },
                    }
                }
            };
            wasi_try_mem_ok!(status_ptr.write(&view, status));
            Ok(ret)
        }
    };

    // If we were just restored the stack then we were woken after a deep sleep
    // and the return calues are already set
    if let Some(status) = unsafe { handle_rewind::<M, _>(&mut ctx) } {
        let ret = ret_result(ctx, status);
        tracing::trace!("rewound join ret={:?}", ret);
        return ret;
    }

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    if let Err(err) = sync_shared_memory_to_guest::<M>(env, &memory) {
        return Ok(err);
    }
    let option_pid = wasi_try_mem_ok!(pid_ptr.read(&memory));
    let option_pid = match option_pid.tag {
        OptionTag::None => None,
        OptionTag::Some => Some(option_pid.pid),
        _ => return Ok(Errno::Inval),
    };
    tracing::trace!("filter_pid = {:?}", option_pid);

    // Clear the existing values (in case something goes wrong)
    wasi_try_mem_ok!(pid_ptr.write(
        &memory,
        OptionPid {
            tag: OptionTag::None,
            pid: 0,
        }
    ));
    wasi_try_mem_ok!(status_ptr.write(
        &memory,
        JoinStatus {
            tag: JoinStatusType::Nothing,
            u: JoinStatusUnion { nothing: 0 },
        }
    ));

    // If the ID is maximum then it means wait for any of the children
    let pid = match option_pid {
        None => {
            let mut process = ctx.data_mut().process.clone();

            if flags.contains(JoinFlags::WAKE_STOPPED)
                && let Some((pid, signal)) = unreported_stopped_matching_child(&process, |_| true)
            {
                return ret_result(ctx, JoinStatusResult::Stopped(pid, signal));
            }

            if flags.contains(JoinFlags::NON_BLOCKING) {
                return match process.try_join_any_child_status() {
                    Ok(Some((child, status))) => {
                        ret_result(ctx, child_status_join_result(Ok(Some((child, status)))))
                    }
                    Ok(None) => ret_result(ctx, JoinStatusResult::NoStatus),
                    Err(err) => ret_result(ctx, JoinStatusResult::Err(err)),
                };
            }

            // We wait for any process to exit (if it takes too long
            // then we go into a deep sleep)
            let res = __asyncify_with_deep_sleep::<M, _, _>(ctx, async move {
                if flags.contains(JoinFlags::WAKE_STOPPED) {
                    child_status_join_result(
                        process
                            .join_any_child_until_unreported_stopped_or_finished()
                            .await,
                    )
                } else {
                    child_status_join_result(process.join_any_child_matching_status(|_| true).await)
                }
            })?;
            return match res {
                AsyncifyAction::Finish(ctx, result) => ret_result(ctx, result),
                AsyncifyAction::Unwind => Ok(Errno::Success),
            };
        }
        Some(pid) => pid,
    };

    // Otherwise we wait for the specific PID
    let pid_signed = pid as i32;
    if pid_signed <= 0 {
        let group_filter = match pid_signed {
            0 => Some(ctx.data().process.pgid()),
            -1 => None,
            pid => match pid.checked_neg() {
                Some(pgid) => Some(WasiProcessId::from(pgid as u32)),
                None => return ret_result(ctx, JoinStatusResult::Err(Errno::Inval)),
            },
        };

        let mut process = ctx.data_mut().process.clone();

        if flags.contains(JoinFlags::WAKE_STOPPED)
            && let Some((pid, signal)) = unreported_stopped_matching_child(&process, |child| {
                group_filter.is_none_or(|pgid| child.pgid() == pgid)
            })
        {
            return ret_result(ctx, JoinStatusResult::Stopped(pid, signal));
        }

        if flags.contains(JoinFlags::NON_BLOCKING) {
            return match process.try_join_any_child_matching_status(|child| {
                group_filter.is_none_or(|pgid| child.pgid() == pgid)
            }) {
                Ok(Some((child, status))) => {
                    ret_result(ctx, child_status_join_result(Ok(Some((child, status)))))
                }
                Ok(None) => ret_result(ctx, JoinStatusResult::NoStatus),
                Err(err) => ret_result(ctx, JoinStatusResult::Err(err)),
            };
        }

        let res = __asyncify_with_deep_sleep::<M, _, _>(ctx, async move {
            if flags.contains(JoinFlags::WAKE_STOPPED) {
                child_status_join_result(
                    process
                        .join_any_child_matching_until_unreported_stopped_or_finished(|child| {
                            group_filter.is_none_or(|pgid| child.pgid() == pgid)
                        })
                        .await,
                )
            } else {
                child_status_join_result(
                    process
                        .join_any_child_matching_status(|child| {
                            group_filter.is_none_or(|pgid| child.pgid() == pgid)
                        })
                        .await,
                )
            }
        })?;
        return match res {
            AsyncifyAction::Finish(ctx, result) => ret_result(ctx, result),
            AsyncifyAction::Unwind => Ok(Errno::Success),
        };
    }

    let pid: WasiProcessId = pid.into();

    // Waiting for a process that is an explicit child will join it
    // meaning it will no longer be a sub-process of the main process
    let (mut process, is_child) = {
        let inner = ctx.data().process.lock();
        let process = inner
            .children
            .iter()
            .filter(|c| c.pid == pid)
            .map(Clone::clone)
            .next();
        let is_child = process.is_some();
        (process, is_child)
    };

    // Otherwise it could be the case that we are waiting for a process
    // that is not a child of this process but may still be running
    if process.is_none() {
        process = ctx.data().control_plane.get_process(pid);
    }

    if let Some(process) = process {
        if flags.contains(JoinFlags::WAKE_STOPPED)
            && let Some(signal) = process.take_unreported_stopped_signal()
        {
            return ret_result(ctx, JoinStatusResult::Stopped(pid, signal));
        }

        if flags.contains(JoinFlags::NON_BLOCKING) {
            if let Some(status) = process.try_join_status() {
                if is_child {
                    ctx.data().process.record_reaped_child_usage(&process);
                    let mut inner = ctx.data().process.lock();
                    inner.children.retain(|c| c.pid != pid);
                    drop(inner);
                    ctx.data().control_plane.remove_process(pid);
                }
                ret_result(ctx, child_status_join_result(Ok(Some((process, status)))))
            } else {
                ret_result(ctx, JoinStatusResult::NoStatus)
            }
        } else {
            // Wait for the process to finish
            let parent_process = ctx.data().process.clone();
            let res = __asyncify_with_deep_sleep::<M, _, _>(ctx, async move {
                let status = if flags.contains(JoinFlags::WAKE_STOPPED) {
                    process.join_until_unreported_stopped_or_finished().await
                } else {
                    process.join_status().await
                };
                match status {
                    TaskStatus::Stopped(signal) => {
                        tracing::trace!(%pid, ?signal, "triggered child stop join");
                        JoinStatusResult::Stopped(pid, signal)
                    }
                    TaskStatus::Signaled { exit_code, signal } => {
                        if is_child {
                            parent_process.record_reaped_child_usage(&process);
                            let mut inner = parent_process.lock();
                            inner.children.retain(|c| c.pid != pid);
                            drop(inner);
                            process.compute.must_upgrade().remove_process(pid);
                        }
                        tracing::trace!(%exit_code, ?signal, "triggered child signal join");
                        JoinStatusResult::ExitSignal(pid, exit_code, signal)
                    }
                    TaskStatus::Finished(join) => {
                        let exit_code = join.unwrap_or_else(|_| Errno::Child.into());
                        if is_child {
                            parent_process.record_reaped_child_usage(&process);
                            let mut inner = parent_process.lock();
                            inner.children.retain(|c| c.pid != pid);
                            drop(inner);
                            process.compute.must_upgrade().remove_process(pid);
                        }
                        tracing::trace!(%exit_code, "triggered child join");
                        JoinStatusResult::ExitNormal(pid, exit_code)
                    }
                    TaskStatus::Pending | TaskStatus::Running => {
                        JoinStatusResult::Err(Errno::Child)
                    }
                }
            })?;
            match res {
                AsyncifyAction::Finish(ctx, result) => ret_result(ctx, result),
                AsyncifyAction::Unwind => Ok(Errno::Success),
            }
        }
    } else {
        trace!(ret_id = pid.raw(), "status=nothing");
        ret_result(ctx, JoinStatusResult::Err(Errno::Child))
    }
}

fn proc_wait_status_internal<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid_ptr: WasmPtr<OptionPid, M>,
    flags: JoinFlags,
    status_ptr: WasmPtr<JoinStatus, M>,
) -> Result<Errno, WasiError> {
    proc_wait_status_select_internal(ctx, pid_ptr, flags, status_ptr, true, true, false)
}

fn proc_wait_status_select_internal<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid_ptr: WasmPtr<OptionPid, M>,
    flags: JoinFlags,
    status_ptr: WasmPtr<JoinStatus, M>,
    observe_exited: bool,
    observe_stopped: bool,
    observe_continued: bool,
) -> Result<Errno, WasiError> {
    ctx = wasi_try_ok!(maybe_snapshot::<M>(ctx)?);

    let ret_result = {
        move |ctx: FunctionEnvMut<'_, WasiEnv>, status: JoinStatusResult| {
            let mut ret = Errno::Success;
            let view = unsafe { ctx.data().memory_view(&ctx) };
            let status = match status {
                JoinStatusResult::Nothing => JoinStatus {
                    tag: JoinStatusType::Nothing,
                    u: JoinStatusUnion { nothing: 0 },
                },
                JoinStatusResult::NoStatus => {
                    let option_pid = OptionPid {
                        tag: OptionTag::Some,
                        pid: 0,
                    };
                    pid_ptr.write(&view, option_pid).ok();
                    JoinStatus {
                        tag: JoinStatusType::Nothing,
                        u: JoinStatusUnion { nothing: 0 },
                    }
                }
                JoinStatusResult::ExitNormal(pid, exit_code) => {
                    let option_pid = OptionPid {
                        tag: OptionTag::Some,
                        pid: pid.raw() as Pid,
                    };
                    pid_ptr.write(&view, option_pid).ok();
                    JoinStatus {
                        tag: JoinStatusType::ExitNormal,
                        u: JoinStatusUnion {
                            exit_normal: exit_code.into(),
                        },
                    }
                }
                JoinStatusResult::ExitSignal(pid, exit_code, signal) => {
                    let option_pid = OptionPid {
                        tag: OptionTag::Some,
                        pid: pid.raw() as Pid,
                    };
                    pid_ptr.write(&view, option_pid).ok();
                    JoinStatus {
                        tag: JoinStatusType::ExitSignal,
                        u: JoinStatusUnion {
                            exit_signal: ErrnoSignal {
                                exit_code: exit_code.into(),
                                signal,
                            },
                        },
                    }
                }
                JoinStatusResult::Stopped(pid, signal) => {
                    let option_pid = OptionPid {
                        tag: OptionTag::Some,
                        pid: pid.raw() as Pid,
                    };
                    pid_ptr.write(&view, option_pid).ok();
                    JoinStatus {
                        tag: JoinStatusType::Stopped,
                        u: JoinStatusUnion { stopped: signal },
                    }
                }
                JoinStatusResult::Continued(pid) => {
                    let option_pid = OptionPid {
                        tag: OptionTag::Some,
                        pid: pid.raw() as Pid,
                    };
                    pid_ptr.write(&view, option_pid).ok();
                    JoinStatus {
                        tag: JoinStatusType::Nothing,
                        u: JoinStatusUnion { nothing: 0 },
                    }
                }
                JoinStatusResult::Err(err) => {
                    ret = err;
                    JoinStatus {
                        tag: JoinStatusType::Nothing,
                        u: JoinStatusUnion { nothing: 0 },
                    }
                }
            };
            wasi_try_mem_ok!(status_ptr.write(&view, status));
            Ok(ret)
        }
    };

    if let Some(status) = unsafe { handle_rewind::<M, _>(&mut ctx) } {
        return ret_result(ctx, status);
    }

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let option_pid = wasi_try_mem_ok!(pid_ptr.read(&memory));
    let option_pid = match option_pid.tag {
        OptionTag::None => None,
        OptionTag::Some => Some(option_pid.pid),
        _ => return Ok(Errno::Inval),
    };

    wasi_try_mem_ok!(pid_ptr.write(
        &memory,
        OptionPid {
            tag: OptionTag::None,
            pid: 0,
        }
    ));
    wasi_try_mem_ok!(status_ptr.write(
        &memory,
        JoinStatus {
            tag: JoinStatusType::Nothing,
            u: JoinStatusUnion { nothing: 0 },
        }
    ));

    match option_pid {
        None => {
            let process = ctx.data().process.clone();
            wait_status_for_matching_child::<M, _, _, _>(
                ctx,
                process,
                flags,
                observe_exited,
                observe_stopped,
                observe_continued,
                true,
                |_| true,
                ret_result,
            )
        }
        Some(pid) => {
            let pid_signed = pid as i32;
            if pid_signed <= 0 {
                let group_filter = match pid_signed {
                    0 => Some(ctx.data().process.pgid()),
                    -1 => None,
                    pid => match pid.checked_neg() {
                        Some(pgid) => Some(WasiProcessId::from(pgid as u32)),
                        None => return ret_result(ctx, JoinStatusResult::Err(Errno::Inval)),
                    },
                };
                let process = ctx.data().process.clone();
                wait_status_for_matching_child::<M, _, _, _>(
                    ctx,
                    process,
                    flags,
                    observe_exited,
                    observe_stopped,
                    observe_continued,
                    true,
                    move |child| group_filter.is_none_or(|pgid| child.pgid() == pgid),
                    ret_result,
                )
            } else {
                let pid: WasiProcessId = pid.into();
                let process = {
                    let inner = ctx.data().process.lock();
                    inner
                        .children
                        .iter()
                        .find(|child| child.pid() == pid)
                        .cloned()
                };
                let Some(process) = process else {
                    return ret_result(ctx, JoinStatusResult::Err(Errno::Child));
                };
                wait_status_for_process::<M, _, _>(
                    ctx,
                    process,
                    flags,
                    observe_exited,
                    observe_stopped,
                    observe_continued,
                    true,
                    ret_result,
                )
            }
        }
    }
}

fn wait_status_for_matching_child<M, F, R, T>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    process: WasiProcess,
    flags: JoinFlags,
    observe_exited: bool,
    observe_stopped: bool,
    observe_continued: bool,
    consume_exited: bool,
    mut matches: F,
    ret_result: R,
) -> Result<T, WasiError>
where
    M: MemorySize + 'static,
    F: FnMut(&WasiProcess) -> bool + Send + 'static,
    R: FnOnce(FunctionEnvMut<'_, WasiEnv>, JoinStatusResult) -> Result<T, WasiError>
        + Send
        + 'static,
    T: WaitReturn,
{
    let children = {
        let inner = process.lock();
        inner.children.clone()
    };
    if children.is_empty() {
        return ret_result(ctx, JoinStatusResult::Err(Errno::Child));
    }

    let mut live_children = Vec::new();
    let mut saw_matching_child = false;
    for child in children {
        if !matches(&child) {
            continue;
        }
        saw_matching_child = true;
        if observe_continued && child.peek_unreported_continued() {
            return ret_result(ctx, JoinStatusResult::Continued(child.pid()));
        }
        if let Some(signal) = child.peek_unreported_stopped_signal()
            && observe_stopped
            && flags.contains(JoinFlags::WAKE_STOPPED)
        {
            return ret_result(ctx, JoinStatusResult::Stopped(child.pid(), signal));
        }
        let Some(child) = process.compute.must_upgrade().get_process(child.pid()) else {
            continue;
        };
        if observe_exited && let Some(status) = child.try_join_status() {
            if consume_exited {
                reap_child_process(&process, &child);
            }
            return ret_result(ctx, child_status_join_result(Ok(Some((child, status)))));
        }
        live_children.push(child);
    }

    if !saw_matching_child {
        return ret_result(ctx, JoinStatusResult::Err(Errno::Child));
    }
    if flags.contains(JoinFlags::NON_BLOCKING) {
        return ret_result(ctx, JoinStatusResult::NoStatus);
    }
    if live_children.is_empty() {
        return ret_result(ctx, JoinStatusResult::Err(Errno::Child));
    }

    if observe_continued {
        let res = __asyncify_with_deep_sleep::<M, _, _>(ctx, async move {
            futures::future::poll_fn(move |cx| {
                let mut saw_pending_child = false;
                for child in live_children.iter() {
                    if child.peek_unreported_continued() {
                        return Poll::Ready(JoinStatusResult::Continued(child.pid()));
                    }
                    if let Some(signal) = child.peek_unreported_stopped_signal()
                        && observe_stopped
                        && flags.contains(JoinFlags::WAKE_STOPPED)
                    {
                        return Poll::Ready(JoinStatusResult::Stopped(child.pid(), signal));
                    }
                    if observe_exited && let Some(status) = child.try_join_status() {
                        if consume_exited {
                            reap_child_process(&process, child);
                        }
                        return Poll::Ready(child_status_join_result(Ok(Some((
                            child.clone(),
                            status,
                        )))));
                    }
                    if !child.task_status().is_finished() {
                        saw_pending_child = true;
                    }
                }

                if !saw_pending_child {
                    return Poll::Ready(JoinStatusResult::Err(Errno::Child));
                }
                process.subscribe_status_change(cx.waker());
                for child in live_children.iter() {
                    if child.peek_unreported_continued() {
                        return Poll::Ready(JoinStatusResult::Continued(child.pid()));
                    }
                    if let Some(signal) = child.peek_unreported_stopped_signal()
                        && observe_stopped
                        && flags.contains(JoinFlags::WAKE_STOPPED)
                    {
                        return Poll::Ready(JoinStatusResult::Stopped(child.pid(), signal));
                    }
                    if observe_exited && let Some(status) = child.try_join_status() {
                        if consume_exited {
                            reap_child_process(&process, child);
                        }
                        return Poll::Ready(child_status_join_result(Ok(Some((
                            child.clone(),
                            status,
                        )))));
                    }
                }
                Poll::Pending
            })
            .await
        })?;
        return match res {
            AsyncifyAction::Finish(ctx, result) => ret_result(ctx, result),
            AsyncifyAction::Unwind => Ok(T::wait_unwind()),
        };
    }

    let res = __asyncify_with_deep_sleep::<M, _, _>(ctx, async move {
        let mut waits = live_children
            .into_iter()
            .map(|child| {
                Box::pin(async move {
                    let status = if observe_stopped && flags.contains(JoinFlags::WAKE_STOPPED) {
                        child.join_until_stopped_or_finished().await
                    } else {
                        TaskStatus::Finished(child.join().await)
                    };
                    (child, status)
                })
            })
            .collect::<Vec<_>>();

        while !waits.is_empty() {
            let ((child, status), _index, remaining) = futures::future::select_all(waits).await;
            match status {
                TaskStatus::Stopped(_) if observe_stopped => {
                    return child_status_join_result(Ok(Some((child, status))));
                }
                TaskStatus::Finished(_) | TaskStatus::Signaled { .. } if observe_exited => {
                    if consume_exited {
                        reap_child_process(&process, &child);
                    }
                    return child_status_join_result(Ok(Some((child, status))));
                }
                _ => {
                    waits = remaining;
                }
            }
        }

        JoinStatusResult::NoStatus
    })?;
    match res {
        AsyncifyAction::Finish(ctx, result) => ret_result(ctx, result),
        AsyncifyAction::Unwind => Ok(T::wait_unwind()),
    }
}

fn wait_status_for_process<M, R, T>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    process: WasiProcess,
    flags: JoinFlags,
    observe_exited: bool,
    observe_stopped: bool,
    observe_continued: bool,
    consume_exited: bool,
    ret_result: R,
) -> Result<T, WasiError>
where
    M: MemorySize + 'static,
    R: FnOnce(FunctionEnvMut<'_, WasiEnv>, JoinStatusResult) -> Result<T, WasiError>
        + Send
        + 'static,
    T: WaitReturn,
{
    if observe_continued && process.peek_unreported_continued() {
        return ret_result(ctx, JoinStatusResult::Continued(process.pid()));
    }
    if let Some(signal) = process.peek_unreported_stopped_signal()
        && observe_stopped
        && flags.contains(JoinFlags::WAKE_STOPPED)
    {
        return ret_result(ctx, JoinStatusResult::Stopped(process.pid(), signal));
    }
    let parent_process = ctx.data().process.clone();
    if observe_exited && let Some(status) = process.try_join_status() {
        if consume_exited {
            reap_child_process(&parent_process, &process);
        }
        return ret_result(ctx, child_status_join_result(Ok(Some((process, status)))));
    }
    if flags.contains(JoinFlags::NON_BLOCKING) {
        return ret_result(ctx, JoinStatusResult::NoStatus);
    }

    if observe_continued {
        let res = __asyncify_with_deep_sleep::<M, _, _>(ctx, async move {
            futures::future::poll_fn(move |cx| {
                if process.peek_unreported_continued() {
                    return Poll::Ready(JoinStatusResult::Continued(process.pid()));
                }
                if let Some(signal) = process.peek_unreported_stopped_signal()
                    && observe_stopped
                    && flags.contains(JoinFlags::WAKE_STOPPED)
                {
                    return Poll::Ready(JoinStatusResult::Stopped(process.pid(), signal));
                }
                if observe_exited && let Some(status) = process.try_join_status() {
                    if consume_exited {
                        reap_child_process(&parent_process, &process);
                    }
                    return Poll::Ready(child_status_join_result(Ok(Some((
                        process.clone(),
                        status,
                    )))));
                }
                process.subscribe_status_change(cx.waker());
                if process.peek_unreported_continued() {
                    return Poll::Ready(JoinStatusResult::Continued(process.pid()));
                }
                if let Some(signal) = process.peek_unreported_stopped_signal()
                    && observe_stopped
                    && flags.contains(JoinFlags::WAKE_STOPPED)
                {
                    return Poll::Ready(JoinStatusResult::Stopped(process.pid(), signal));
                }
                if observe_exited && let Some(status) = process.try_join_status() {
                    if consume_exited {
                        reap_child_process(&parent_process, &process);
                    }
                    return Poll::Ready(child_status_join_result(Ok(Some((
                        process.clone(),
                        status,
                    )))));
                }
                Poll::Pending
            })
            .await
        })?;
        return match res {
            AsyncifyAction::Finish(ctx, result) => ret_result(ctx, result),
            AsyncifyAction::Unwind => Ok(T::wait_unwind()),
        };
    }

    let res = __asyncify_with_deep_sleep::<M, _, _>(ctx, async move {
        let status = if observe_stopped && flags.contains(JoinFlags::WAKE_STOPPED) {
            process.join_until_stopped_or_finished().await
        } else {
            process.join_status().await
        };
        match status {
            TaskStatus::Stopped(_) if observe_stopped => {
                child_status_join_result(Ok(Some((process, status))))
            }
            TaskStatus::Finished(_) | TaskStatus::Signaled { .. } if observe_exited => {
                if consume_exited {
                    reap_child_process(&parent_process, &process);
                }
                child_status_join_result(Ok(Some((process, status))))
            }
            _ => JoinStatusResult::NoStatus,
        }
    })?;
    match res {
        AsyncifyAction::Finish(ctx, result) => ret_result(ctx, result),
        AsyncifyAction::Unwind => Ok(T::wait_unwind()),
    }
}

fn unreported_stopped_matching_child<F>(
    process: &WasiProcess,
    mut matches: F,
) -> Option<(WasiProcessId, Signal)>
where
    F: FnMut(&WasiProcess) -> bool,
{
    let children = {
        let inner = process.lock();
        inner.children.clone()
    };

    for child in children {
        if !matches(&child) {
            continue;
        }
        if let Some(signal) = child.take_unreported_stopped_signal() {
            return Some((child.pid(), signal));
        }
    }
    None
}

fn continued_matching_child(
    process: &WasiProcess,
    pid: Pid,
    consume: bool,
) -> Option<WasiProcessId> {
    let pid_signed = pid as i32;
    let group_filter = match pid_signed {
        0 => Some(process.pgid()),
        -1 => None,
        pid if pid < -1 => Some(WasiProcessId::from(pid.checked_neg()? as u32)),
        _ => None,
    };

    let children = {
        let inner = process.lock();
        inner.children.clone()
    };

    for child in children {
        if pid_signed > 0 && child.pid().raw() != pid {
            continue;
        }
        if group_filter.is_some_and(|pgid| child.pgid() != pgid) {
            continue;
        }
        let continued = if consume {
            child.take_unreported_continued()
        } else {
            child.peek_unreported_continued()
        };
        if continued {
            return Some(child.pid());
        }
    }
    None
}

fn has_live_matching_child(process: &WasiProcess, pid: Pid) -> bool {
    let pid_signed = pid as i32;
    let group_filter = match pid_signed {
        0 => Some(process.pgid()),
        -1 => None,
        pid if pid < -1 => match pid.checked_neg() {
            Some(pgid) => Some(WasiProcessId::from(pgid as u32)),
            None => return false,
        },
        _ => None,
    };

    let children = {
        let inner = process.lock();
        inner.children.clone()
    };
    let Some(control_plane) = process.compute.upgrade() else {
        return false;
    };

    for child in children {
        if pid_signed > 0 && child.pid().raw() != pid {
            continue;
        }
        if group_filter.is_some_and(|pgid| child.pgid() != pgid) {
            continue;
        }
        let Some(child) = control_plane.get_process(child.pid()) else {
            continue;
        };
        if !child.task_status().is_finished() {
            return true;
        }
    }
    false
}

fn child_status_join_result(
    child_status: Result<Option<(WasiProcess, TaskStatus)>, Errno>,
) -> JoinStatusResult {
    match child_status {
        Ok(Some((child, status))) => {
            let pid = child.pid();
            match status {
                TaskStatus::Stopped(signal) => {
                    tracing::trace!(%pid, ?signal, "triggered child stop join");
                    JoinStatusResult::Stopped(pid, signal)
                }
                TaskStatus::Finished(join) => {
                    let exit_code = join.unwrap_or_else(|_| Errno::Child.into());
                    tracing::trace!(%pid, %exit_code, "triggered child join");
                    trace!(ret_id = pid.raw(), exit_code = exit_code.raw());
                    JoinStatusResult::ExitNormal(pid, exit_code)
                }
                TaskStatus::Signaled { exit_code, signal } => {
                    tracing::trace!(%pid, %exit_code, ?signal, "triggered child signal join");
                    trace!(ret_id = pid.raw(), exit_code = exit_code.raw(), ?signal);
                    JoinStatusResult::ExitSignal(pid, exit_code, signal)
                }
                TaskStatus::Pending | TaskStatus::Running => {
                    tracing::trace!(%pid, "child status join woke without stop or exit");
                    JoinStatusResult::Err(Errno::Child)
                }
            }
        }
        Ok(None) => {
            tracing::trace!("triggered child join (no child)");
            JoinStatusResult::Err(Errno::Child)
        }
        Err(err) => {
            tracing::trace!(%err, "error triggered on child status join");
            JoinStatusResult::Err(err)
        }
    }
}

fn reap_child_process(parent: &WasiProcess, child: &WasiProcess) {
    let pid = child.pid();
    let is_child = {
        let inner = parent.lock();
        inner.children.iter().any(|existing| existing.pid() == pid)
    };
    if !is_child {
        return;
    }
    parent.record_reaped_child_usage(child);
    let mut inner = parent.lock();
    inner.children.retain(|existing| existing.pid() != pid);
    drop(inner);
    child.compute.must_upgrade().remove_process(pid);
}
