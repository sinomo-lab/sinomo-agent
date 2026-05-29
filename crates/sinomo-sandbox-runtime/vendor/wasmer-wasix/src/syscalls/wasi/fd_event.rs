use super::*;
use crate::{
    fs::NotificationInner,
    state::{
        WasiIntervalTimer, WasiMessageQueue, WasiMessageQueueDescriptor, WasiMessageQueueHandle,
        WasiMessageQueueMessage, WasiMessageQueueNotification, WasiPosixTimer, WasiSignalFd,
        WasiTimerFd,
    },
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
const POSIX_MQUEUE_DESCRIPTOR_LIMIT: i32 = 16_384;
const POSIX_MQUEUE_NAME_MAX: usize = 255;
const POSIX_MQUEUE_PATH_MAX: usize = 4096;
const POSIX_MQUEUE_PRIO_MAX: u32 = 32_768;
const POSIX_O_ACCMODE: i32 = 0o3;
const POSIX_O_RDONLY: i32 = 0;
const POSIX_O_WRONLY: i32 = 1;
const POSIX_O_RDWR: i32 = 2;
const WASIX_O_NONBLOCK: i32 = 1 << 2;
const WASIX_O_RDONLY: i32 = 0x0400_0000;
const WASIX_O_WRONLY: i32 = 0x1000_0000;
const POSIX_MQUEUE_WAIT_SLICE: std::time::Duration = std::time::Duration::from_millis(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MqueueAccessMode {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

fn mqueue_access_mode(flags: i32) -> Option<MqueueAccessMode> {
    let wasix_read = flags & WASIX_O_RDONLY != 0;
    let wasix_write = flags & WASIX_O_WRONLY != 0;
    match (wasix_read, wasix_write) {
        (true, true) => return Some(MqueueAccessMode::ReadWrite),
        (true, false) => return Some(MqueueAccessMode::ReadOnly),
        (false, true) => return Some(MqueueAccessMode::WriteOnly),
        (false, false) => {}
    }

    match flags & POSIX_O_ACCMODE {
        POSIX_O_RDONLY => Some(MqueueAccessMode::ReadOnly),
        POSIX_O_WRONLY => Some(MqueueAccessMode::WriteOnly),
        POSIX_O_RDWR => Some(MqueueAccessMode::ReadWrite),
        _ => None,
    }
}

fn mqueue_nonblocking(flags: i32) -> bool {
    flags & (WASIX_O_NONBLOCK | LINUX_EFD_NONBLOCK | LINUX_TFD_NONBLOCK) != 0
}

fn mqueue_update_nonblocking_flags(current_flags: i32, new_flags: i64) -> i32 {
    let mut flags = current_flags & !(WASIX_O_NONBLOCK | LINUX_EFD_NONBLOCK | LINUX_TFD_NONBLOCK);
    if new_flags & i64::from(WASIX_O_NONBLOCK | LINUX_EFD_NONBLOCK | LINUX_TFD_NONBLOCK) != 0 {
        flags |= WASIX_O_NONBLOCK;
    }
    flags
}

fn mqueue_insert_message(queue: &mut WasiMessageQueue, priority: u32, bytes: Vec<u8>) {
    let insert_at = queue
        .messages
        .iter()
        .position(|message| priority > message.priority)
        .unwrap_or(queue.messages.len());
    queue
        .messages
        .insert(insert_at, WasiMessageQueueMessage { priority, bytes });
}

fn mqueue_take_notification(queue: &mut WasiMessageQueue) -> Option<WasiMessageQueueNotification> {
    if queue.messages.len() != 1 || queue.blocked_receivers != 0 {
        return None;
    }
    queue.notification.take()
}

fn mqueue_deliver_notification(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    notification: WasiMessageQueueNotification,
) -> Result<(), WasiError> {
    if notification.signal == Signal::Signone {
        return Ok(());
    }
    if let Some(process) = ctx
        .data()
        .control_plane
        .get_process(notification.pid.into())
    {
        process.signal_process_direct(notification.signal);
        if notification.pid == ctx.data().pid().raw() {
            let _ = process_pending_operations_interruptibly(ctx)?;
        }
    }
    Ok(())
}

fn mqueue_abs_timeout_ns(timeout_sec: i64, timeout_nsec: i64) -> Result<u64, Errno> {
    if !(0..NANOS_PER_SEC as i64).contains(&timeout_nsec) {
        return Err(Errno::Inval);
    }
    if timeout_sec < 0 {
        return Ok(0);
    }
    let seconds = u64::try_from(timeout_sec).map_err(|_| Errno::Overflow)?;
    seconds
        .checked_mul(NANOS_PER_SEC)
        .and_then(|base| base.checked_add(timeout_nsec as u64))
        .ok_or(Errno::Overflow)
}

fn mqueue_wait_duration(
    ctx: &FunctionEnvMut<'_, WasiEnv>,
    deadline: Option<(i64, i64)>,
) -> Result<Option<std::time::Duration>, Errno> {
    let Some((timeout_sec, timeout_nsec)) = deadline else {
        return Ok(Some(POSIX_MQUEUE_WAIT_SLICE));
    };
    let deadline_ns = mqueue_abs_timeout_ns(timeout_sec, timeout_nsec)?;
    let now = timerfd_now(ctx, Snapshot0Clockid::Realtime).map_err(|_| Errno::Io)?;
    if now >= deadline_ns {
        return Ok(None);
    }
    let remaining = std::time::Duration::from_nanos(deadline_ns - now);
    Ok(Some(remaining.min(POSIX_MQUEUE_WAIT_SLICE)))
}

fn process_pending_operations_interruptibly(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
) -> Result<bool, WasiError> {
    WasiEnv::do_pending_link_operations(ctx, true)?;
    if let Some(forced_exit) = ctx.data().should_exit() {
        return Err(WasiError::Exit(forced_exit));
    }
    let signals = ctx.data().thread.pop_signals();
    if signals.is_empty() {
        return Ok(false);
    }
    WasiEnv::process_signals_internal(ctx, signals)
}

fn sinomo_mqueue_debug(args: std::fmt::Arguments<'_>) {
    if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
        eprintln!("sinomo-debug mqueue {args}");
    }
}

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

#[instrument(level = "trace", skip_all, ret)]
pub fn mqueue_open<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    name_ptr: WasmPtr<u8, M>,
    name_len: M::Offset,
    flags: i32,
    create: i32,
    exclusive: i32,
    has_attr: i32,
    max_messages: u64,
    message_size: u64,
) -> Result<i32, WasiError> {
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let name = match name_ptr.read_utf8_string(&memory, name_len) {
        Ok(name) => name,
        Err(err) => return Ok(-mem_error_to_wasi(err).to_native()),
    };
    if !name.starts_with('/') || name.len() < 2 {
        return Ok(-Errno::Inval.to_native());
    }
    if name.len() >= POSIX_MQUEUE_PATH_MAX || name[1..].len() > POSIX_MQUEUE_NAME_MAX {
        return Ok(-Errno::Nametoolong.to_native());
    }
    if name[1..].contains('/') {
        return Ok(-Errno::Access.to_native());
    }
    if mqueue_access_mode(flags).is_none() {
        return Ok(-Errno::Inval.to_native());
    }

    let mut state = ctx.data().state.message_queues.lock().unwrap();
    let existing = state.queues.get(&name).cloned();
    let exists = existing.is_some();
    if exists && create != 0 && exclusive != 0 {
        return Ok(-Errno::Exist.to_native());
    }
    let queue = if let Some(queue) = existing {
        queue
    } else {
        if create == 0 {
            return Ok(-Errno::Noent.to_native());
        }
        let max_messages = if has_attr == 0 { 10 } else { max_messages };
        let message_size = if has_attr == 0 { 8192 } else { message_size };
        if max_messages == 0
            || message_size == 0
            || max_messages > i64::MAX as u64
            || message_size > i64::MAX as u64
        {
            return Ok(-Errno::Inval.to_native());
        }
        sinomo_mqueue_debug(format_args!(
            "open name={} flags={:#x} has_attr={} max_messages={} message_size={}",
            name, flags, has_attr, max_messages, message_size
        ));
        let queue = Arc::new(WasiMessageQueueHandle {
            state: Mutex::new(WasiMessageQueue {
                max_messages,
                message_size,
                messages: Default::default(),
                blocked_receivers: 0,
                notification: None,
            }),
            send_waiters: std::sync::Condvar::new(),
            receive_waiters: std::sync::Condvar::new(),
        });
        state.queues.insert(name.clone(), queue.clone());
        queue
    };

    let descriptor = (1..=POSIX_MQUEUE_DESCRIPTOR_LIMIT)
        .map(|offset| {
            let next = state.next_descriptor.saturating_add(offset);
            if next <= 0 { offset } else { next }
        })
        .find(|candidate| !state.descriptors.contains_key(candidate));
    let Some(descriptor) = descriptor else {
        return Ok(-Errno::Mfile.to_native());
    };
    state.next_descriptor = descriptor;
    state.descriptors.insert(
        descriptor,
        WasiMessageQueueDescriptor {
            flags,
            queue,
        },
    );
    Ok(descriptor)
}

#[instrument(level = "trace", skip_all, ret)]
pub fn mqueue_send<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    descriptor: i32,
    message_ptr: WasmPtr<u8, M>,
    message_len: M::Offset,
    priority: u32,
) -> Result<i32, WasiError> {
    mqueue_send_internal(
        ctx,
        descriptor,
        message_ptr,
        message_len,
        priority,
        None,
    )
}

#[instrument(level = "trace", skip_all, ret)]
pub fn mqueue_timedsend<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    descriptor: i32,
    message_ptr: WasmPtr<u8, M>,
    message_len: M::Offset,
    priority: u32,
    timeout_sec: i64,
    timeout_nsec: i64,
) -> Result<i32, WasiError> {
    mqueue_send_internal(
        ctx,
        descriptor,
        message_ptr,
        message_len,
        priority,
        Some((timeout_sec, timeout_nsec)),
    )
}

fn mqueue_send_internal<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    descriptor: i32,
    message_ptr: WasmPtr<u8, M>,
    message_len: M::Offset,
    priority: u32,
    deadline: Option<(i64, i64)>,
) -> Result<i32, WasiError> {
    let message_len_usize = match usize::try_from(message_len.into()) {
        Ok(len) => len,
        Err(_) => return Ok(-Errno::Overflow.to_native()),
    };
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let message = match message_ptr
        .slice(&memory, message_len)
        .and_then(|slice| slice.read_to_vec())
    {
        Ok(message) => message,
        Err(err) => return Ok(-mem_error_to_wasi(err).to_native()),
    };
    if message.len() != message_len_usize {
        return Ok(-Errno::Inval.to_native());
    }

    let (queue, nonblock) = {
        let state = ctx.data().state.message_queues.lock().unwrap();
        let Some(descriptor) = state.descriptors.get(&descriptor) else {
            return Ok(-Errno::Badf.to_native());
        };
        if mqueue_access_mode(descriptor.flags) == Some(MqueueAccessMode::ReadOnly) {
            return Ok(-Errno::Badf.to_native());
        }
        (
            descriptor.queue.clone(),
            mqueue_nonblocking(descriptor.flags),
        )
    };

    let mut waited_for_space = false;
    loop {
        let mut state = queue.state.lock().unwrap();
        if priority >= POSIX_MQUEUE_PRIO_MAX {
            return Ok(-Errno::Inval.to_native());
        }
        if message.len() > state.message_size as usize {
            return Ok(-Errno::Msgsize.to_native());
        }
        if state.messages.len() < state.max_messages as usize {
            sinomo_mqueue_debug(format_args!(
                "send descriptor={} len={} priority={} before={} max={}",
                descriptor,
                message.len(),
                priority,
                state.messages.len(),
                state.max_messages
            ));
            mqueue_insert_message(&mut state, priority, message.clone());
            let notification = mqueue_take_notification(&mut state);
            queue.receive_waiters.notify_one();
            drop(state);
            if let Some(notification) = notification {
                mqueue_deliver_notification(&mut ctx, notification)?;
            }
            return Ok(0);
        }
        sinomo_mqueue_debug(format_args!(
            "send-block descriptor={} len={} priority={} current={} max={} nonblock={}",
            descriptor,
            message.len(),
            priority,
            state.messages.len(),
            state.max_messages,
            nonblock
        ));
        if nonblock {
            return Ok(-Errno::Again.to_native());
        }
        waited_for_space = true;
        let wait_duration = match mqueue_wait_duration(&ctx, deadline) {
            Ok(Some(wait_duration)) => wait_duration,
            Ok(None) => return Ok(-Errno::Timedout.to_native()),
            Err(err) => return Ok(-err.to_native()),
        };
        drop(
            queue
                .send_waiters
                .wait_timeout(state, wait_duration)
                .unwrap(),
        );
        if process_pending_operations_interruptibly(&mut ctx)? {
            sinomo_mqueue_debug(format_args!("send-interrupted descriptor={}", descriptor));
            return Ok(-Errno::Intr.to_native());
        }
    }
}

#[instrument(level = "trace", skip_all, ret)]
pub fn mqueue_receive<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    descriptor: i32,
    buffer_ptr: WasmPtr<u8, M>,
    buffer_len: M::Offset,
    priority_ptr: WasmPtr<u32, M>,
) -> Result<i32, WasiError> {
    mqueue_receive_internal(ctx, descriptor, buffer_ptr, buffer_len, priority_ptr, None)
}

#[instrument(level = "trace", skip_all, ret)]
pub fn mqueue_timedreceive<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    descriptor: i32,
    buffer_ptr: WasmPtr<u8, M>,
    buffer_len: M::Offset,
    priority_ptr: WasmPtr<u32, M>,
    timeout_sec: i64,
    timeout_nsec: i64,
) -> Result<i32, WasiError> {
    mqueue_receive_internal(
        ctx,
        descriptor,
        buffer_ptr,
        buffer_len,
        priority_ptr,
        Some((timeout_sec, timeout_nsec)),
    )
}

fn mqueue_receive_internal<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    descriptor: i32,
    buffer_ptr: WasmPtr<u8, M>,
    buffer_len: M::Offset,
    priority_ptr: WasmPtr<u32, M>,
    deadline: Option<(i64, i64)>,
) -> Result<i32, WasiError> {
    let buffer_len_usize = match usize::try_from(buffer_len.into()) {
        Ok(len) => len,
        Err(_) => return Ok(-Errno::Overflow.to_native()),
    };
    let (queue, nonblock) = {
        let state = ctx.data().state.message_queues.lock().unwrap();
        let Some(descriptor) = state.descriptors.get(&descriptor) else {
            return Ok(-Errno::Badf.to_native());
        };
        if mqueue_access_mode(descriptor.flags) == Some(MqueueAccessMode::WriteOnly) {
            return Ok(-Errno::Badf.to_native());
        }
        (
            descriptor.queue.clone(),
            mqueue_nonblocking(descriptor.flags),
        )
    };
    let message = loop {
        let mut state = queue.state.lock().unwrap();
        if buffer_len_usize < state.message_size as usize {
            return Ok(-Errno::Msgsize.to_native());
        }
        if let Some(message) = state.messages.pop_front() {
            sinomo_mqueue_debug(format_args!(
                "receive descriptor={} len={} priority={} remaining={} max={}",
                descriptor,
                message.bytes.len(),
                message.priority,
                state.messages.len(),
                state.max_messages
            ));
            queue.send_waiters.notify_one();
            break message;
        }
        if nonblock {
            return Ok(-Errno::Again.to_native());
        }
        sinomo_mqueue_debug(format_args!(
            "receive-block descriptor={} buffer_len={} max={}",
            descriptor, buffer_len_usize, state.max_messages
        ));
        let wait_duration = match mqueue_wait_duration(&ctx, deadline) {
            Ok(Some(wait_duration)) => wait_duration,
            Ok(None) => return Ok(-Errno::Timedout.to_native()),
            Err(err) => return Ok(-err.to_native()),
        };
        state.blocked_receivers = state.blocked_receivers.saturating_add(1);
        let (mut state, _) = queue
            .receive_waiters
            .wait_timeout(state, wait_duration)
            .unwrap();
        state.blocked_receivers = state.blocked_receivers.saturating_sub(1);
        drop(state);
        if process_pending_operations_interruptibly(&mut ctx)? {
            return Ok(-Errno::Intr.to_native());
        }
    };
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let message_len = match to_offset::<M>(message.bytes.len()) {
        Ok(len) => len,
        Err(err) => return Ok(-err.to_native()),
    };
    if let Err(err) = buffer_ptr
        .slice(&memory, message_len)
        .map_err(mem_error_to_wasi)
        .and_then(|slice| slice.write_slice(&message.bytes).map_err(mem_error_to_wasi))
    {
        return Ok(-err.to_native());
    }
    if !priority_ptr.is_null()
        && let Err(err) = priority_ptr.write(&memory, message.priority)
    {
        return Ok(-mem_error_to_wasi(err).to_native());
    }
    Ok(message.bytes.len().min(i32::MAX as usize) as i32)
}

#[instrument(level = "trace", skip_all, ret)]
pub fn mqueue_getattr<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    descriptor: i32,
    flags_ptr: WasmPtr<i64, M>,
    maxmsg_ptr: WasmPtr<i64, M>,
    msgsize_ptr: WasmPtr<i64, M>,
    curmsgs_ptr: WasmPtr<i64, M>,
) -> Result<i32, WasiError> {
    let (flags, queue) = {
        let state = ctx.data().state.message_queues.lock().unwrap();
        let Some(descriptor) = state.descriptors.get(&descriptor) else {
            return Ok(-Errno::Badf.to_native());
        };
        (descriptor.flags, descriptor.queue.clone())
    };
    let queue = queue.state.lock().unwrap();
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    for (ptr, value) in [
        (flags_ptr, flags as i64),
        (maxmsg_ptr, queue.max_messages.min(i64::MAX as u64) as i64),
        (msgsize_ptr, queue.message_size.min(i64::MAX as u64) as i64),
        (curmsgs_ptr, queue.messages.len().min(i64::MAX as usize) as i64),
    ] {
        if let Err(err) = ptr.write(&memory, value) {
            return Ok(-mem_error_to_wasi(err).to_native());
        }
    }
    Ok(0)
}

#[instrument(level = "trace", skip_all, ret)]
pub fn mqueue_setattr(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    descriptor: i32,
    new_flags: i64,
) -> Result<i32, WasiError> {
    let mut state = ctx.data().state.message_queues.lock().unwrap();
    let Some(descriptor) = state.descriptors.get_mut(&descriptor) else {
        return Ok(-Errno::Badf.to_native());
    };
    descriptor.flags = mqueue_update_nonblocking_flags(descriptor.flags, new_flags);
    Ok(0)
}

#[instrument(level = "trace", skip_all, ret)]
pub fn mqueue_close(ctx: FunctionEnvMut<'_, WasiEnv>, descriptor: i32) -> Result<i32, WasiError> {
    let removed = ctx
        .data()
        .state
        .message_queues
        .lock()
        .unwrap()
        .descriptors
        .remove(&descriptor);
    let Some(removed) = removed else {
        return Ok(-Errno::Badf.to_native());
    };
    let mut queue = removed.queue.state.lock().unwrap();
    if matches!(&queue.notification, Some(notification) if notification.descriptor == descriptor) {
        queue.notification = None;
    }
    Ok(0)
}

#[instrument(level = "trace", skip_all, ret)]
pub fn mqueue_notify(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    descriptor: i32,
    has_notification: i32,
    signal: u32,
) -> Result<i32, WasiError> {
    let queue = {
        let state = ctx.data().state.message_queues.lock().unwrap();
        let Some(descriptor) = state.descriptors.get(&descriptor) else {
            return Ok(-Errno::Badf.to_native());
        };
        descriptor.queue.clone()
    };
    let mut queue = queue.state.lock().unwrap();
    if has_notification == 0 {
        let current_pid = ctx.data().pid().raw();
        if matches!(&queue.notification, Some(notification) if notification.pid == current_pid) {
            queue.notification = None;
        }
        return Ok(0);
    }
    if queue.notification.is_some() {
        return Ok(-Errno::Busy.to_native());
    }
    let Ok(signal) = Signal::try_from(signal as u8) else {
        return Ok(-Errno::Inval.to_native());
    };
    if signal == Signal::Sigwakeup {
        return Ok(-Errno::Inval.to_native());
    }
    queue.notification = Some(WasiMessageQueueNotification {
        pid: ctx.data().pid().raw(),
        descriptor,
        signal,
    });
    Ok(0)
}

#[instrument(level = "trace", skip_all, ret)]
pub fn mqueue_unlink<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    name_ptr: WasmPtr<u8, M>,
    name_len: M::Offset,
) -> Result<i32, WasiError> {
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let name = match name_ptr.read_utf8_string(&memory, name_len) {
        Ok(name) => name,
        Err(err) => return Ok(-mem_error_to_wasi(err).to_native()),
    };
    if ctx
        .data()
        .state
        .message_queues
        .lock()
        .unwrap()
        .queues
        .remove(&name)
        .is_some()
    {
        Ok(0)
    } else {
        Ok(-Errno::Noent.to_native())
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
