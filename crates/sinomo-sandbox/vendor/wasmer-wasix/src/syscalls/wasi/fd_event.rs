use super::*;
use crate::{
    fs::NotificationInner,
    state::{WasiIntervalTimer, WasiPosixTimer, WasiSignalFd, WasiTimerFd},
    syscalls::*,
};
use wasmer::FromToNativeWasmType;
use wasmer_wasix_types::wasi::Snapshot0Clockid;

const LINUX_EFD_SEMAPHORE: i32 = 0x0000_0001;
const LINUX_EFD_CLOEXEC: i32 = 0o2000000;
const LINUX_EFD_NONBLOCK: i32 = 0o4000;
const LINUX_TFD_CLOEXEC: i32 = 0o2000000;
const LINUX_TFD_NONBLOCK: i32 = 0o4000;
const LINUX_TFD_TIMER_ABSTIME: i32 = 1;
const LINUX_TFD_TIMER_CANCEL_ON_SET: i32 = 2;
const LINUX_SFD_CLOEXEC: i32 = 0o2000000;
const LINUX_SFD_NONBLOCK: i32 = 0o4000;
const LINUX_TIMER_ABSTIME: i32 = 1;
const LINUX_ITIMER_REAL: i32 = 0;
const LINUX_ITIMER_VIRTUAL: i32 = 1;
const LINUX_ITIMER_PROF: i32 = 2;
const NANOS_PER_SEC: u64 = 1_000_000_000;
const NANOS_PER_USEC: u64 = 1_000;
const POSIX_TIMER_LIMIT: u32 = 64;

/// ### `fd_event()`
/// Creates a file handle for event notifications
#[instrument(level = "trace", skip_all, fields(%initial_val, ret_fd = field::Empty), ret)]
pub fn fd_event<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    initial_val: u64,
    flags: EventFdFlags,
    ret_fd: WasmPtr<WasiFd, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let fd = wasi_try_ok!(fd_event_internal(&mut ctx, initial_val, flags, None)?);

    let env = ctx.data();
    let (memory, state, _) = unsafe { env.get_memory_and_wasi_state_and_inodes(&ctx, 0) };
    Span::current().record("ret_fd", fd);
    wasi_try_mem_ok!(ret_fd.write(&memory, fd));

    #[cfg(feature = "journal")]
    if env.enable_journal {
        JournalEffector::save_fd_event(&mut ctx, initial_val, flags, fd).map_err(|err| {
            tracing::error!("failed to save fd_event event - {}", err);
            WasiError::Exit(ExitCode::from(Errno::Fault))
        })?;
    }

    Ok(Errno::Success)
}

/// ### `eventfd()`
/// Linux-shaped eventfd creation. Returns the new fd or a negative errno.
#[instrument(level = "trace", skip_all, fields(%initial_val, %flags, ret_fd = field::Empty), ret)]
pub fn eventfd(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    initial_val: u32,
    flags: i32,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if flags & !(LINUX_EFD_SEMAPHORE | LINUX_EFD_CLOEXEC | LINUX_EFD_NONBLOCK) != 0 {
        return Ok(-Errno::Inval.to_native());
    }

    let mut event_flags = 0;
    if flags & LINUX_EFD_SEMAPHORE != 0 {
        event_flags |= EVENT_FD_FLAGS_SEMAPHORE;
    }
    let fd = match fd_event_internal(&mut ctx, initial_val as u64, event_flags, None)? {
        Ok(fd) => fd,
        Err(err) => return Ok(-err.to_native()),
    };

    #[cfg(feature = "journal")]
    {
        let env = ctx.data();
        if env.enable_journal {
            JournalEffector::save_fd_event(&mut ctx, initial_val as u64, event_flags, fd).map_err(
                |err| {
                    tracing::error!("failed to save fd_event event - {}", err);
                    WasiError::Exit(ExitCode::from(Errno::Fault))
                },
            )?;
        }
    }

    if let Err(err) = apply_linux_eventfd_flags(&mut ctx, fd, flags) {
        let _ = ctx.data().state.fs.close_fd(fd);
        return Ok(-err.to_native());
    }

    Span::current().record("ret_fd", fd);
    Ok(fd as i32)
}

/// ### `timerfd_create()`
/// Linux-shaped timerfd creation. Returns the new fd or a negative errno.
#[instrument(level = "trace", skip_all, fields(%clock_id, %flags, ret_fd = field::Empty), ret)]
pub fn timerfd_create(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    clock_id: i32,
    flags: i32,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if flags & !(LINUX_TFD_CLOEXEC | LINUX_TFD_NONBLOCK) != 0 {
        return Ok(-Errno::Inval.to_native());
    }
    let Some(clock_id) = linux_clock_id(clock_id) else {
        return Ok(-Errno::Inval.to_native());
    };
    let now = match timerfd_now(&ctx, clock_id) {
        Ok(now) => now,
        Err(err) => return Ok(-err.to_native()),
    };

    let fd = match fd_event_internal(&mut ctx, 0, 0, None)? {
        Ok(fd) => fd,
        Err(err) => return Ok(-err.to_native()),
    };
    if let Err(err) = apply_linux_timerfd_flags(&mut ctx, fd, flags) {
        let _ = ctx.data().state.fs.close_fd(fd);
        return Ok(-err.to_native());
    }

    ctx.data().state.timerfds.lock().unwrap().insert(
        fd,
        WasiTimerFd {
            clock_id,
            value_ns: 0,
            interval_ns: 0,
            armed_at_ns: now,
            consumed: 0,
        },
    );
    sync_process_metadata(ctx.data());
    Span::current().record("ret_fd", fd);
    Ok(fd as i32)
}

/// ### `timerfd_settime()`
/// Updates runtime-owned timerfd state and optionally writes the previous
/// remaining timer value as scalar `timespec` fields.
#[instrument(level = "trace", skip_all, fields(%fd, %flags), ret)]
pub fn timerfd_settime<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    flags: i32,
    value_sec: i64,
    value_nsec: i64,
    interval_sec: i64,
    interval_nsec: i64,
    old_value_sec: WasmPtr<i64, M>,
    old_value_nsec: WasmPtr<i64, M>,
    old_interval_sec: WasmPtr<i64, M>,
    old_interval_nsec: WasmPtr<i64, M>,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if flags & !(LINUX_TFD_TIMER_ABSTIME | LINUX_TFD_TIMER_CANCEL_ON_SET) != 0 {
        return Ok(-Errno::Inval.to_native());
    }
    let value_ns = match timespec_to_ns(value_sec, value_nsec) {
        Ok(value) => value,
        Err(err) => return Ok(-err.to_native()),
    };
    let interval_ns = match timespec_to_ns(interval_sec, interval_nsec) {
        Ok(value) => value,
        Err(err) => return Ok(-err.to_native()),
    };

    let (old_value_ns, old_interval_ns, new_value_ns) = {
        let mut timers = ctx.data().state.timerfds.lock().unwrap();
        let Some(timer) = timers.get_mut(&fd) else {
            return Ok(-Errno::Badf.to_native());
        };
        let now = match timerfd_now(&ctx, timer.clock_id) {
            Ok(now) => now,
            Err(err) => return Ok(-err.to_native()),
        };
        let old_value_ns = timer_remaining_ns(timer, now);
        let old_interval_ns = timer.interval_ns;
        let new_value_ns = if flags & LINUX_TFD_TIMER_ABSTIME != 0 {
            value_ns.saturating_sub(now)
        } else {
            value_ns
        };
        timer.value_ns = new_value_ns;
        timer.interval_ns = interval_ns;
        timer.armed_at_ns = now;
        timer.consumed = 0;
        (old_value_ns, old_interval_ns, new_value_ns)
    };

    if let Err(err) = write_timer_values(
        &ctx,
        old_value_sec,
        old_value_nsec,
        old_interval_sec,
        old_interval_nsec,
        old_value_ns,
        old_interval_ns,
    ) {
        return Ok(-err.to_native());
    }
    Span::current().record("value_ns", new_value_ns);
    Ok(0)
}

/// ### `timerfd_gettime()`
/// Writes the current remaining timerfd value as scalar `timespec` fields.
#[instrument(level = "trace", skip_all, fields(%fd), ret)]
pub fn timerfd_gettime<M: MemorySize + 'static>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    value_sec: WasmPtr<i64, M>,
    value_nsec: WasmPtr<i64, M>,
    interval_sec: WasmPtr<i64, M>,
    interval_nsec: WasmPtr<i64, M>,
) -> Result<i32, WasiError> {
    let (remaining_ns, interval_ns) = {
        let timers = ctx.data().state.timerfds.lock().unwrap();
        let Some(timer) = timers.get(&fd) else {
            return Ok(-Errno::Badf.to_native());
        };
        let now = match timerfd_now(&ctx, timer.clock_id) {
            Ok(now) => now,
            Err(err) => return Ok(-err.to_native()),
        };
        (timer_remaining_ns(timer, now), timer.interval_ns)
    };
    if let Err(err) = write_timer_values(
        &ctx,
        value_sec,
        value_nsec,
        interval_sec,
        interval_nsec,
        remaining_ns,
        interval_ns,
    ) {
        return Ok(-err.to_native());
    }
    Ok(0)
}

/// ### `timerfd_read()`
/// Consumes and returns ready timer expirations, or zero if none are ready.
#[instrument(level = "trace", skip_all, fields(%fd), ret)]
pub fn timerfd_read(ctx: FunctionEnvMut<'_, WasiEnv>, fd: WasiFd) -> Result<i64, WasiError> {
    let mut timers = ctx.data().state.timerfds.lock().unwrap();
    let Some(timer) = timers.get_mut(&fd) else {
        return Ok(-Errno::Badf.to_native() as i64);
    };
    let now = match timerfd_now(&ctx, timer.clock_id) {
        Ok(now) => now,
        Err(err) => return Ok(-err.to_native() as i64),
    };
    let total = timer_expirations(timer, now);
    if total <= timer.consumed {
        return Ok(0);
    }
    let ready = total.saturating_sub(timer.consumed);
    timer.consumed = total;
    Ok(ready.min(i64::MAX as u64) as i64)
}

/// ### `timerfd_ready()`
/// Returns 1 when the runtime-owned timerfd is readable, 0 when not readable,
/// or a negative errno for an invalid descriptor.
#[instrument(level = "trace", skip_all, fields(%fd), ret)]
pub fn timerfd_ready(ctx: FunctionEnvMut<'_, WasiEnv>, fd: WasiFd) -> Result<i32, WasiError> {
    let timers = ctx.data().state.timerfds.lock().unwrap();
    let Some(timer) = timers.get(&fd) else {
        return Ok(-Errno::Badf.to_native());
    };
    let now = match timerfd_now(&ctx, timer.clock_id) {
        Ok(now) => now,
        Err(err) => return Ok(-err.to_native()),
    };
    Ok((timer_expirations(timer, now) > timer.consumed) as i32)
}

/// ### `timerfd_remaining_ms()`
/// Returns the milliseconds until the next timerfd expiration, or -1 when no
/// timer is armed or the fd is not a runtime-owned timerfd.
#[instrument(level = "trace", skip_all, fields(%fd), ret)]
pub fn timerfd_remaining_ms(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
) -> Result<i32, WasiError> {
    let timers = ctx.data().state.timerfds.lock().unwrap();
    let Some(timer) = timers.get(&fd) else {
        return Ok(-1);
    };
    let now = match timerfd_now(&ctx, timer.clock_id) {
        Ok(now) => now,
        Err(_) => return Ok(-1),
    };
    let remaining_ns = timer_remaining_ns(timer, now);
    if remaining_ns == 0 {
        return Ok(if timer.value_ns == 0 { -1 } else { 0 });
    }
    let ms = remaining_ns.saturating_add(999_999) / 1_000_000;
    Ok(ms.min(i32::MAX as u64) as i32)
}

#[instrument(level = "trace", skip_all, fields(%fd), ret)]
pub fn timerfd_known(ctx: FunctionEnvMut<'_, WasiEnv>, fd: WasiFd) -> Result<i32, WasiError> {
    Ok(ctx.data().state.timerfds.lock().unwrap().contains_key(&fd) as i32)
}

/// ### `signalfd()`
/// Linux-shaped signal fd creation/update. The descriptor and mask are runtime
/// owned; guests can still build libc-specific `signalfd_siginfo` records in
/// their compatibility layer.
#[instrument(level = "trace", skip_all, fields(%fd, %flags), ret)]
pub fn signalfd(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: i32,
    mask_low: u64,
    mask_high: u64,
    flags: i32,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if flags & !(LINUX_SFD_CLOEXEC | LINUX_SFD_NONBLOCK) != 0 {
        return Ok(-Errno::Inval.to_native());
    }

    if fd >= 0 {
        let fd = fd as WasiFd;
        let mut signalfds = ctx.data().state.signalfds.lock().unwrap();
        let Some(descriptor) = signalfds.get_mut(&fd) else {
            return Ok(-Errno::Inval.to_native());
        };
        descriptor.mask_low = mask_low;
        descriptor.mask_high = mask_high;
        return Ok(fd as i32);
    }

    let fd = match fd_event_internal(&mut ctx, 0, 0, None)? {
        Ok(fd) => fd,
        Err(err) => return Ok(-err.to_native()),
    };
    if let Err(err) = apply_linux_signalfd_flags(&mut ctx, fd, flags) {
        let _ = ctx.data().state.fs.close_fd(fd);
        return Ok(-err.to_native());
    }

    ctx.data().state.signalfds.lock().unwrap().insert(
        fd,
        WasiSignalFd {
            mask_low,
            mask_high,
        },
    );
    sync_process_metadata(ctx.data());
    Span::current().record("ret_fd", fd);
    Ok(fd as i32)
}

#[instrument(level = "trace", skip_all, fields(%fd), ret)]
pub fn signalfd_known(ctx: FunctionEnvMut<'_, WasiEnv>, fd: WasiFd) -> Result<i32, WasiError> {
    Ok(ctx.data().state.signalfds.lock().unwrap().contains_key(&fd) as i32)
}

#[instrument(level = "trace", skip_all, fields(%fd), ret)]
pub fn signalfd_mask_low(ctx: FunctionEnvMut<'_, WasiEnv>, fd: WasiFd) -> Result<i64, WasiError> {
    let Some(descriptor) = ctx.data().state.signalfds.lock().unwrap().get(&fd).cloned() else {
        return Ok(-Errno::Badf.to_native() as i64);
    };
    Ok(descriptor.mask_low as i64)
}

#[instrument(level = "trace", skip_all, fields(%fd), ret)]
pub fn signalfd_mask_high(ctx: FunctionEnvMut<'_, WasiEnv>, fd: WasiFd) -> Result<i64, WasiError> {
    let Some(descriptor) = ctx.data().state.signalfds.lock().unwrap().get(&fd).cloned() else {
        return Ok(-Errno::Badf.to_native() as i64);
    };
    Ok(descriptor.mask_high as i64)
}

/// ### `posix_timer_create()`
/// Creates a runtime-owned POSIX interval timer and returns a guest-stable
/// timer id or a negative errno.
#[instrument(level = "trace", skip_all, fields(%clock_id, ret_timer = field::Empty), ret)]
pub fn posix_timer_create(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    clock_id: i32,
) -> Result<i32, WasiError> {
    let Some(clock_id) = linux_clock_id(clock_id) else {
        return Ok(-Errno::Inval.to_native());
    };
    let now = match timerfd_now(&ctx, clock_id) {
        Ok(now) => now,
        Err(err) => return Ok(-err.to_native()),
    };
    let mut timers = ctx.data().state.posix_timers.lock().unwrap();
    let Some(timer_id) = (1..=POSIX_TIMER_LIMIT).find(|id| !timers.contains_key(id)) else {
        return Ok(-Errno::Again.to_native());
    };
    timers.insert(
        timer_id,
        WasiPosixTimer {
            clock_id,
            value_ns: 0,
            interval_ns: 0,
            armed_at_ns: now,
        },
    );
    Span::current().record("ret_timer", timer_id);
    Ok(timer_id as i32)
}

/// ### `posix_timer_settime()`
/// Updates runtime-owned POSIX timer state and optionally writes the previous
/// remaining value as scalar `timespec` fields.
#[instrument(level = "trace", skip_all, fields(%timer_id, %flags), ret)]
pub fn posix_timer_settime<M: MemorySize + 'static>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    timer_id: u32,
    flags: i32,
    value_sec: i64,
    value_nsec: i64,
    interval_sec: i64,
    interval_nsec: i64,
    old_value_sec: WasmPtr<i64, M>,
    old_value_nsec: WasmPtr<i64, M>,
    old_interval_sec: WasmPtr<i64, M>,
    old_interval_nsec: WasmPtr<i64, M>,
) -> Result<i32, WasiError> {
    if flags & !LINUX_TIMER_ABSTIME != 0 {
        return Ok(-Errno::Inval.to_native());
    }
    let value_ns = match timespec_to_ns(value_sec, value_nsec) {
        Ok(value) => value,
        Err(err) => return Ok(-err.to_native()),
    };
    let interval_ns = match timespec_to_ns(interval_sec, interval_nsec) {
        Ok(value) => value,
        Err(err) => return Ok(-err.to_native()),
    };

    let (old_value_ns, old_interval_ns, new_value_ns) = {
        let mut timers = ctx.data().state.posix_timers.lock().unwrap();
        let Some(timer) = timers.get_mut(&timer_id) else {
            return Ok(-Errno::Inval.to_native());
        };
        let now = match timerfd_now(&ctx, timer.clock_id) {
            Ok(now) => now,
            Err(err) => return Ok(-err.to_native()),
        };
        let old_value_ns = posix_timer_remaining_ns(timer, now);
        let old_interval_ns = timer.interval_ns;
        let new_value_ns = if flags & LINUX_TIMER_ABSTIME != 0 {
            value_ns.saturating_sub(now)
        } else {
            value_ns
        };
        timer.value_ns = new_value_ns;
        timer.interval_ns = interval_ns;
        timer.armed_at_ns = now;
        (old_value_ns, old_interval_ns, new_value_ns)
    };

    if let Err(err) = write_timer_values(
        &ctx,
        old_value_sec,
        old_value_nsec,
        old_interval_sec,
        old_interval_nsec,
        old_value_ns,
        old_interval_ns,
    ) {
        return Ok(-err.to_native());
    }
    Span::current().record("value_ns", new_value_ns);
    Ok(0)
}

/// ### `posix_timer_gettime()`
/// Writes the current remaining POSIX timer value as scalar `timespec` fields.
#[instrument(level = "trace", skip_all, fields(%timer_id), ret)]
pub fn posix_timer_gettime<M: MemorySize + 'static>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    timer_id: u32,
    value_sec: WasmPtr<i64, M>,
    value_nsec: WasmPtr<i64, M>,
    interval_sec: WasmPtr<i64, M>,
    interval_nsec: WasmPtr<i64, M>,
) -> Result<i32, WasiError> {
    let (remaining_ns, interval_ns) = {
        let timers = ctx.data().state.posix_timers.lock().unwrap();
        let Some(timer) = timers.get(&timer_id) else {
            return Ok(-Errno::Inval.to_native());
        };
        let now = match timerfd_now(&ctx, timer.clock_id) {
            Ok(now) => now,
            Err(err) => return Ok(-err.to_native()),
        };
        (posix_timer_remaining_ns(timer, now), timer.interval_ns)
    };
    if let Err(err) = write_timer_values(
        &ctx,
        value_sec,
        value_nsec,
        interval_sec,
        interval_nsec,
        remaining_ns,
        interval_ns,
    ) {
        return Ok(-err.to_native());
    }
    Ok(0)
}

/// ### `posix_timer_getoverrun()`
/// Returns completed interval periods beyond the first timer expiration.
#[instrument(level = "trace", skip_all, fields(%timer_id), ret)]
pub fn posix_timer_getoverrun(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    timer_id: u32,
) -> Result<i32, WasiError> {
    let timers = ctx.data().state.posix_timers.lock().unwrap();
    let Some(timer) = timers.get(&timer_id) else {
        return Ok(-Errno::Inval.to_native());
    };
    if timer.value_ns == 0 || timer.interval_ns == 0 {
        return Ok(0);
    }
    let now = match timerfd_now(&ctx, timer.clock_id) {
        Ok(now) => now,
        Err(err) => return Ok(-err.to_native()),
    };
    let elapsed = now.saturating_sub(timer.armed_at_ns);
    if elapsed <= timer.value_ns {
        return Ok(0);
    }
    Ok(((elapsed - timer.value_ns) / timer.interval_ns).min(i32::MAX as u64) as i32)
}

#[instrument(level = "trace", skip_all, fields(%timer_id), ret)]
pub fn posix_timer_delete(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    timer_id: u32,
) -> Result<i32, WasiError> {
    if ctx
        .data()
        .state
        .posix_timers
        .lock()
        .unwrap()
        .remove(&timer_id)
        .is_some()
    {
        Ok(0)
    } else {
        Ok(-Errno::Inval.to_native())
    }
}

/// ### `itimer_settime()`
/// Runtime-owned `setitimer` state and signal scheduling for POSIX interval
/// timers. Time values use scalar `timeval` fields.
#[instrument(level = "trace", skip_all, fields(%which), ret)]
pub fn itimer_settime<M: MemorySize + 'static>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    which: i32,
    value_sec: i64,
    value_usec: i64,
    interval_sec: i64,
    interval_usec: i64,
    old_value_sec: WasmPtr<i64, M>,
    old_value_usec: WasmPtr<i64, M>,
    old_interval_sec: WasmPtr<i64, M>,
    old_interval_usec: WasmPtr<i64, M>,
) -> Result<i32, WasiError> {
    let Some(signal) = itimer_signal(which) else {
        return Ok(-Errno::Inval.to_native());
    };
    let value_ns = match timeval_to_ns(value_sec, value_usec) {
        Ok(value) => value,
        Err(err) => return Ok(-err.to_native()),
    };
    let interval_ns = match timeval_to_ns(interval_sec, interval_usec) {
        Ok(value) => value,
        Err(err) => return Ok(-err.to_native()),
    };
    let now = match timerfd_now(&ctx, Snapshot0Clockid::Monotonic) {
        Ok(now) => now,
        Err(err) => return Ok(-err.to_native()),
    };

    let (old_value_ns, old_interval_ns) = {
        let mut timers = ctx.data().state.interval_timers.lock().unwrap();
        let old = timers.get(&which).cloned();
        if value_ns == 0 && interval_ns == 0 {
            timers.remove(&which);
        } else {
            timers.insert(
                which,
                WasiIntervalTimer {
                    value_ns,
                    interval_ns,
                    armed_at_ns: now,
                },
            );
        }
        let old_value_ns = old
            .as_ref()
            .map(|timer| interval_timer_remaining_ns(timer, now))
            .unwrap_or(0);
        let old_interval_ns = old.as_ref().map(|timer| timer.interval_ns).unwrap_or(0);
        (old_value_ns, old_interval_ns)
    };

    if let Err(err) = write_timeval_values(
        &ctx,
        old_value_sec,
        old_value_usec,
        old_interval_sec,
        old_interval_usec,
        old_value_ns,
        old_interval_ns,
    ) {
        return Ok(-err.to_native());
    }

    let scheduled = if value_ns != 0 { value_ns } else { interval_ns };
    let interval = if scheduled == 0 {
        None
    } else {
        Some(std::time::Duration::from_nanos(scheduled))
    };
    ctx.data()
        .process
        .signal_interval(signal, interval, interval_ns != 0);
    Ok(0)
}

/// ### `itimer_gettime()`
/// Writes the current runtime-owned `getitimer` value as scalar `timeval`
/// fields.
#[instrument(level = "trace", skip_all, fields(%which), ret)]
pub fn itimer_gettime<M: MemorySize + 'static>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    which: i32,
    value_sec: WasmPtr<i64, M>,
    value_usec: WasmPtr<i64, M>,
    interval_sec: WasmPtr<i64, M>,
    interval_usec: WasmPtr<i64, M>,
) -> Result<i32, WasiError> {
    if itimer_signal(which).is_none() {
        return Ok(-Errno::Inval.to_native());
    }
    let now = match timerfd_now(&ctx, Snapshot0Clockid::Monotonic) {
        Ok(now) => now,
        Err(err) => return Ok(-err.to_native()),
    };
    let (remaining_ns, interval_ns) = {
        let timers = ctx.data().state.interval_timers.lock().unwrap();
        let Some(timer) = timers.get(&which) else {
            if let Err(err) = write_timeval_values(
                &ctx,
                value_sec,
                value_usec,
                interval_sec,
                interval_usec,
                0,
                0,
            ) {
                return Ok(-err.to_native());
            }
            return Ok(0);
        };
        (interval_timer_remaining_ns(timer, now), timer.interval_ns)
    };
    if let Err(err) = write_timeval_values(
        &ctx,
        value_sec,
        value_usec,
        interval_sec,
        interval_usec,
        remaining_ns,
        interval_ns,
    ) {
        return Ok(-err.to_native());
    }
    Ok(0)
}

fn apply_linux_eventfd_flags(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    flags: i32,
) -> Result<(), Errno> {
    if flags & LINUX_EFD_NONBLOCK != 0 {
        let ret =
            fd_fdstat_set_flags_internal(ctx, fd, Fdflags::NONBLOCK).map_err(|_| Errno::Fault)?;
        if ret != Errno::Success {
            return Err(ret);
        }
    }
    if flags & LINUX_EFD_CLOEXEC != 0 {
        let ret =
            fd_fdflags_set_internal(ctx, fd, Fdflagsext::CLOEXEC).map_err(|_| Errno::Fault)?;
        if ret != Errno::Success {
            return Err(ret);
        }
    }
    Ok(())
}

fn apply_linux_timerfd_flags(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    flags: i32,
) -> Result<(), Errno> {
    let event_flags = ((flags & LINUX_TFD_CLOEXEC) != 0) as i32 * LINUX_EFD_CLOEXEC
        | ((flags & LINUX_TFD_NONBLOCK) != 0) as i32 * LINUX_EFD_NONBLOCK;
    apply_linux_eventfd_flags(ctx, fd, event_flags)
}

fn apply_linux_signalfd_flags(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    flags: i32,
) -> Result<(), Errno> {
    let event_flags = ((flags & LINUX_SFD_CLOEXEC) != 0) as i32 * LINUX_EFD_CLOEXEC
        | ((flags & LINUX_SFD_NONBLOCK) != 0) as i32 * LINUX_EFD_NONBLOCK;
    apply_linux_eventfd_flags(ctx, fd, event_flags)
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

fn timerfd_now(
    ctx: &FunctionEnvMut<'_, WasiEnv>,
    clock_id: Snapshot0Clockid,
) -> Result<u64, Errno> {
    let mut now = platform_clock_time_get(clock_id, 1)?;
    {
        let guard = ctx.data().state.clock_offset.lock().unwrap();
        if let Some(offset) = guard.get(&clock_id) {
            now += *offset;
        }
    }
    Ok(now.max(0) as u64)
}

fn timespec_to_ns(sec: i64, nsec: i64) -> Result<u64, Errno> {
    if sec < 0 || !(0..1_000_000_000).contains(&nsec) {
        return Err(Errno::Inval);
    }
    let sec = sec as u64;
    let nsec = nsec as u64;
    sec.checked_mul(NANOS_PER_SEC)
        .and_then(|value| value.checked_add(nsec))
        .ok_or(Errno::Overflow)
}

fn timer_expirations(timer: &WasiTimerFd, now: u64) -> u64 {
    if timer.value_ns == 0 {
        return 0;
    }
    let elapsed = now.saturating_sub(timer.armed_at_ns);
    if elapsed < timer.value_ns {
        return 0;
    }
    if timer.interval_ns == 0 {
        return 1;
    }
    1u64.saturating_add(elapsed.saturating_sub(timer.value_ns) / timer.interval_ns)
}

fn timer_remaining_ns(timer: &WasiTimerFd, now: u64) -> u64 {
    if timer.value_ns == 0 {
        return 0;
    }
    let elapsed = now.saturating_sub(timer.armed_at_ns);
    if elapsed < timer.value_ns {
        return timer.value_ns - elapsed;
    }
    if timer.interval_ns == 0 {
        return 0;
    }
    let into_period = elapsed.saturating_sub(timer.value_ns) % timer.interval_ns;
    if into_period == 0 {
        0
    } else {
        timer.interval_ns - into_period
    }
}

fn posix_timer_remaining_ns(timer: &WasiPosixTimer, now: u64) -> u64 {
    if timer.value_ns == 0 {
        return 0;
    }
    let elapsed = now.saturating_sub(timer.armed_at_ns);
    if elapsed < timer.value_ns {
        return timer.value_ns - elapsed;
    }
    if timer.interval_ns == 0 {
        return 0;
    }
    let into_period = elapsed.saturating_sub(timer.value_ns) % timer.interval_ns;
    if into_period == 0 {
        timer.interval_ns
    } else {
        timer.interval_ns - into_period
    }
}

fn interval_timer_remaining_ns(timer: &WasiIntervalTimer, now: u64) -> u64 {
    if timer.value_ns == 0 {
        return 0;
    }
    let elapsed = now.saturating_sub(timer.armed_at_ns);
    if elapsed < timer.value_ns {
        return timer.value_ns - elapsed;
    }
    if timer.interval_ns == 0 {
        return 0;
    }
    let into_period = elapsed.saturating_sub(timer.value_ns) % timer.interval_ns;
    if into_period == 0 {
        timer.interval_ns
    } else {
        timer.interval_ns - into_period
    }
}

fn itimer_signal(which: i32) -> Option<Signal> {
    match which {
        LINUX_ITIMER_REAL => Some(Signal::Sigalrm),
        LINUX_ITIMER_VIRTUAL => Some(Signal::Sigvtalrm),
        LINUX_ITIMER_PROF => Some(Signal::Sigprof),
        _ => None,
    }
}

fn timeval_to_ns(sec: i64, usec: i64) -> Result<u64, Errno> {
    if sec < 0 || !(0..1_000_000).contains(&usec) {
        return Err(Errno::Inval);
    }
    let sec = sec as u64;
    let usec = usec as u64;
    sec.checked_mul(NANOS_PER_SEC)
        .and_then(|value| value.checked_add(usec.saturating_mul(NANOS_PER_USEC)))
        .ok_or(Errno::Overflow)
}

fn write_timer_values<M: MemorySize>(
    ctx: &FunctionEnvMut<'_, WasiEnv>,
    value_sec: WasmPtr<i64, M>,
    value_nsec: WasmPtr<i64, M>,
    interval_sec: WasmPtr<i64, M>,
    interval_nsec: WasmPtr<i64, M>,
    value_ns: u64,
    interval_ns: u64,
) -> Result<(), Errno> {
    if value_sec.is_null()
        && value_nsec.is_null()
        && interval_sec.is_null()
        && interval_nsec.is_null()
    {
        return Ok(());
    }
    if value_sec.is_null()
        || value_nsec.is_null()
        || interval_sec.is_null()
        || interval_nsec.is_null()
    {
        return Err(Errno::Fault);
    }
    let memory = unsafe { ctx.data().memory_view(ctx) };
    value_sec
        .write(
            &memory,
            (value_ns / NANOS_PER_SEC).min(i64::MAX as u64) as i64,
        )
        .map_err(mem_error_to_wasi)?;
    value_nsec
        .write(&memory, (value_ns % NANOS_PER_SEC) as i64)
        .map_err(mem_error_to_wasi)?;
    interval_sec
        .write(
            &memory,
            (interval_ns / NANOS_PER_SEC).min(i64::MAX as u64) as i64,
        )
        .map_err(mem_error_to_wasi)?;
    interval_nsec
        .write(&memory, (interval_ns % NANOS_PER_SEC) as i64)
        .map_err(mem_error_to_wasi)?;
    Ok(())
}

fn write_timeval_values<M: MemorySize>(
    ctx: &FunctionEnvMut<'_, WasiEnv>,
    value_sec: WasmPtr<i64, M>,
    value_usec: WasmPtr<i64, M>,
    interval_sec: WasmPtr<i64, M>,
    interval_usec: WasmPtr<i64, M>,
    value_ns: u64,
    interval_ns: u64,
) -> Result<(), Errno> {
    if value_sec.is_null()
        && value_usec.is_null()
        && interval_sec.is_null()
        && interval_usec.is_null()
    {
        return Ok(());
    }
    if value_sec.is_null()
        || value_usec.is_null()
        || interval_sec.is_null()
        || interval_usec.is_null()
    {
        return Err(Errno::Fault);
    }
    let memory = unsafe { ctx.data().memory_view(ctx) };
    value_sec
        .write(
            &memory,
            (value_ns / NANOS_PER_SEC).min(i64::MAX as u64) as i64,
        )
        .map_err(mem_error_to_wasi)?;
    value_usec
        .write(
            &memory,
            ((value_ns % NANOS_PER_SEC) / NANOS_PER_USEC) as i64,
        )
        .map_err(mem_error_to_wasi)?;
    interval_sec
        .write(
            &memory,
            (interval_ns / NANOS_PER_SEC).min(i64::MAX as u64) as i64,
        )
        .map_err(mem_error_to_wasi)?;
    interval_usec
        .write(
            &memory,
            ((interval_ns % NANOS_PER_SEC) / NANOS_PER_USEC) as i64,
        )
        .map_err(mem_error_to_wasi)?;
    Ok(())
}

pub fn fd_event_internal(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    initial_val: u64,
    flags: EventFdFlags,
    with_fd: Option<WasiFd>,
) -> Result<Result<WasiFd, Errno>, WasiError> {
    let env = ctx.data();
    let (memory, state, mut inodes) = unsafe { env.get_memory_and_wasi_state_and_inodes(&ctx, 0) };

    let is_semaphore = flags & EVENT_FD_FLAGS_SEMAPHORE != 0;
    let kind = Kind::EventNotifications {
        inner: Arc::new(NotificationInner::new(initial_val, is_semaphore)),
    };

    let inode =
        state
            .fs
            .create_inode_with_default_stat(inodes, kind, false, "event".to_string().into());
    let rights = Rights::FD_READ
        | Rights::FD_WRITE
        | Rights::POLL_FD_READWRITE
        | Rights::FD_FDSTAT_SET_FLAGS;
    let fd = wasi_try_ok_ok!(if let Some(fd) = with_fd {
        state
            .fs
            .with_fd(
                rights,
                rights,
                Fdflags::empty(),
                Fdflagsext::empty(),
                0,
                inode,
                fd,
            )
            .map(|_| fd)
    } else {
        state.fs.create_fd(
            rights,
            rights,
            Fdflags::empty(),
            Fdflagsext::empty(),
            0,
            inode,
        )
    });

    sync_process_metadata(env);
    Ok(Ok(fd))
}
