use std::{
    sync::atomic::{AtomicU64, Ordering},
    task::Waker,
};

use super::*;
use crate::syscalls::*;

macro_rules! sinomo_debug {
    ($($arg:tt)*) => {
        if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
            eprintln!($($arg)*);
        }
    };
}

static FUTEX_TIMEOUT_DEBUG_COUNT: AtomicU64 = AtomicU64::new(0);

/// Poller returns true if its triggered and false if it times out
struct FutexPoller {
    state: Arc<WasiState>,
    poller_idx: u64,
    futex_idx: u64,
    timeout: Option<Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>>,
}

struct FutexWaitAnyRegistration {
    waiter_index: u32,
    poller_idx: u64,
    futex_idx: u64,
}

struct FutexWaitAnyPoller {
    state: Arc<WasiState>,
    registrations: Vec<FutexWaitAnyRegistration>,
    timeout: Option<Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>>,
}
impl Future for FutexPoller {
    type Output = bool;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<bool> {
        let mut guard = self.state.futexs.lock().unwrap();
        if guard.woken_pollers.remove(&self.poller_idx) {
            guard.poller_futexes.remove(&self.poller_idx);
            sinomo_debug!(
                "sinomo-debug futex_wait wake poller={} futex={:#x}",
                self.poller_idx,
                self.futex_idx
            );
            return Poll::Ready(true);
        }

        let futex_idx = guard
            .poller_futexes
            .get(&self.poller_idx)
            .copied()
            .unwrap_or(self.futex_idx);

        // If the futex itself is no longer registered then it was likely
        // woken by a wake call
        let futex = match guard.futexes.get_mut(&futex_idx) {
            Some(f) => f,
            None => {
                sinomo_debug!(
                    "sinomo-debug futex_wait missing-futex poller={} futex={:#x}",
                    self.poller_idx,
                    futex_idx
                );
                return Poll::Ready(true);
            }
        };
        let waker = match futex.wakers.get_mut(&self.poller_idx) {
            Some(w) => w,
            None => {
                sinomo_debug!(
                    "sinomo-debug futex_wait missing-waker poller={} futex={:#x}",
                    self.poller_idx,
                    futex_idx
                );
                return Poll::Ready(true);
            }
        };

        // Register the waker
        waker.replace(cx.waker().clone());

        // Check for timeout
        drop(guard);
        if let Some(timeout) = self.timeout.as_mut() {
            let timeout = timeout.as_mut();
            if timeout.poll(cx).is_ready() {
                self.timeout.take();
                let timeout_count = FUTEX_TIMEOUT_DEBUG_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
                if timeout_count <= 32 || timeout_count % 1000 == 0 {
                    sinomo_debug!(
                        "sinomo-debug futex_wait timeout poller={} futex={:#x} count={}",
                        self.poller_idx,
                        futex_idx,
                        timeout_count
                    );
                }
                return Poll::Ready(false);
            }
        }

        // We will now wait to be woken
        Poll::Pending
    }
}
impl Drop for FutexPoller {
    fn drop(&mut self) {
        let mut guard = self.state.futexs.lock().unwrap();

        let mut should_remove = false;
        let futex_idx = guard
            .poller_futexes
            .remove(&self.poller_idx)
            .unwrap_or(self.futex_idx);
        guard.woken_pollers.remove(&self.poller_idx);
        if let Some(futex) = guard.futexes.get_mut(&futex_idx) {
            futex.masks.remove(&self.poller_idx);
            if let Some(Some(waker)) = futex.wakers.remove(&self.poller_idx) {
                waker.wake();
            }
            should_remove = futex.wakers.is_empty();
        }
        if should_remove {
            guard.futexes.remove(&futex_idx);
        }
    }
}

impl Future for FutexWaitAnyPoller {
    type Output = Option<u32>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<u32>> {
        let mut guard = self.state.futexs.lock().unwrap();
        let mut woken = None;
        for registration in &self.registrations {
            if guard.woken_pollers.remove(&registration.poller_idx) {
                guard.poller_futexes.remove(&registration.poller_idx);
                woken = Some(registration.waiter_index);
                break;
            }

            let futex_idx = guard
                .poller_futexes
                .get(&registration.poller_idx)
                .copied()
                .unwrap_or(registration.futex_idx);
            let Some(futex) = guard.futexes.get_mut(&futex_idx) else {
                woken = Some(registration.waiter_index);
                break;
            };
            let Some(waker) = futex.wakers.get_mut(&registration.poller_idx) else {
                woken = Some(registration.waiter_index);
                break;
            };
            waker.replace(cx.waker().clone());
        }
        drop(guard);
        if woken.is_some() {
            return Poll::Ready(woken);
        }

        if let Some(timeout) = self.timeout.as_mut() {
            let timeout = timeout.as_mut();
            if timeout.poll(cx).is_ready() {
                self.timeout.take();
                return Poll::Ready(None);
            }
        }

        Poll::Pending
    }
}

impl Drop for FutexWaitAnyPoller {
    fn drop(&mut self) {
        let mut guard = self.state.futexs.lock().unwrap();
        let mut empty_futexes = Vec::new();
        for registration in &self.registrations {
            let futex_idx = guard
                .poller_futexes
                .remove(&registration.poller_idx)
                .unwrap_or(registration.futex_idx);
            guard.woken_pollers.remove(&registration.poller_idx);
            if let Some(futex) = guard.futexes.get_mut(&futex_idx) {
                futex.masks.remove(&registration.poller_idx);
                if let Some(Some(waker)) = futex.wakers.remove(&registration.poller_idx) {
                    waker.wake();
                }
                if futex.wakers.is_empty() {
                    empty_futexes.push(futex_idx);
                }
            }
        }
        for futex_idx in empty_futexes {
            guard.futexes.remove(&futex_idx);
        }
    }
}

/// Wait for a futex_wake operation to wake us.
/// Returns with EINVAL if the futex doesn't hold the expected value.
/// Returns false on timeout, and true in all other cases.
///
/// ## Parameters
///
/// * `futex` - Memory location that holds the value that will be checked
/// * `expected` - Expected value that should be currently held at the memory location
/// * `timeout` - Timeout should the futex not be triggered in the allocated time
#[instrument(level = "trace", skip_all, fields(futex_idx = field::Empty, poller_idx = field::Empty, %expected, timeout = field::Empty, woken = field::Empty))]
pub fn futex_wait<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    futex_ptr: WasmPtr<u32, M>,
    expected: u32,
    timeout: WasmPtr<OptionTimestamp, M>,
    ret_woken: WasmPtr<Bool, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    futex_wait_internal(ctx, futex_ptr, expected, timeout, ret_woken)
}

pub fn futex_wait_mask<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    futex_ptr: WasmPtr<u32, M>,
    expected: u32,
    mask: u32,
    timeout: WasmPtr<OptionTimestamp, M>,
    ret_woken: WasmPtr<Bool, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    futex_wait_mask_internal(ctx, futex_ptr, expected, mask, timeout, ret_woken)
}

pub fn futex_wait_any<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    waiters: WasmPtr<u8, M>,
    waiters_len: M::Offset,
    timeout: WasmPtr<OptionTimestamp, M>,
    ret_index: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    ctx = wasi_try_ok!(maybe_backoff::<M>(ctx)?);
    ctx = wasi_try_ok!(maybe_snapshot::<M>(ctx)?);

    if let Some(result) = unsafe { handle_rewind::<M, Option<u32>>(&mut ctx) } {
        let env = ctx.data();
        let memory = unsafe { env.memory_view(&ctx) };
        let index = result.unwrap_or(u32::MAX);
        wasi_try_mem_ok!(ret_index.write(&memory, index));
        return Ok(Errno::Success);
    }

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let waiter_count_u64: u64 = waiters_len.into();
    let waiter_count =
        usize::try_from(waiter_count_u64).map_err(|_| WasiError::Exit(Errno::Overflow.into()))?;
    if waiter_count == 0 {
        return Ok(Errno::Inval);
    }

    let timeout = read_optional_timeout::<M>(&memory, timeout)?;

    let waiters = read_futex_wait_any_entries::<M>(&memory, waiters, waiter_count)?;
    let sync_shared = waiters
        .iter()
        .any(|waiter| is_registered_shared_futex_offset(env, waiter.futex_idx));
    let state = env.state.clone();
    let poller = {
        let mut guard = env.state.futexs.lock().unwrap();
        let timeout = timeout.map(|timeout| env.tasks().sleep_now(timeout));
        let mut registrations = Vec::with_capacity(waiters.len());
        for waiter in &waiters {
            guard.poller_seed += 1;
            let poller_idx = guard.poller_seed;
            let futex = guard.futexes.entry(waiter.futex_idx).or_default();
            futex.wakers.insert(poller_idx, Default::default());
            futex.masks.insert(poller_idx, waiter.mask);
            guard.poller_futexes.insert(poller_idx, waiter.futex_idx);
            registrations.push(FutexWaitAnyRegistration {
                waiter_index: waiter.waiter_index,
                poller_idx,
                futex_idx: waiter.futex_idx,
            });
        }
        FutexWaitAnyPoller {
            state,
            registrations,
            timeout,
        }
    };

    if sync_shared {
        sync_shared_memory_to_guest::<M>(env, &memory)
            .map_err(|err| WasiError::Exit(err.into()))?;
    }
    for waiter in &waiters {
        let futex_ptr = WasmPtr::<u32, M>::new(waiter.futex_offset);
        let val = wasi_try_mem_ok!(futex_ptr.read(&memory));
        if val != waiter.expected {
            return Ok(Errno::Again);
        }
    }
    wasi_try_mem_ok!(ret_index.write(&memory, u32::MAX));

    let res = __asyncify_with_deep_sleep::<M, _, _>(ctx, Box::pin(poller))?;
    if let AsyncifyAction::Finish(ctx, result) = res {
        let env = ctx.data();
        let memory = unsafe { env.memory_view(&ctx) };
        if sync_shared {
            sync_shared_memory_to_guest::<M>(env, &memory)
                .map_err(|err| WasiError::Exit(err.into()))?;
        }
        let index = result.unwrap_or(u32::MAX);
        wasi_try_mem_ok!(ret_index.write(&memory, index));
    }
    Ok(Errno::Success)
}

pub(super) fn futex_wait_internal<M: MemorySize + 'static>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    futex_ptr: WasmPtr<u32, M>,
    expected: u32,
    timeout: WasmPtr<OptionTimestamp, M>,
    ret_woken: WasmPtr<Bool, M>,
) -> Result<Errno, WasiError> {
    futex_wait_mask_internal(ctx, futex_ptr, expected, u32::MAX, timeout, ret_woken)
}

pub(super) fn futex_wait_mask_internal<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    futex_ptr: WasmPtr<u32, M>,
    expected: u32,
    mask: u32,
    timeout: WasmPtr<OptionTimestamp, M>,
    ret_woken: WasmPtr<Bool, M>,
) -> Result<Errno, WasiError> {
    if mask == 0 {
        return Ok(Errno::Inval);
    }
    ctx = wasi_try_ok!(maybe_backoff::<M>(ctx)?);
    ctx = wasi_try_ok!(maybe_snapshot::<M>(ctx)?);

    // If we were just restored then we were woken after a deep sleep
    // and thus we repeat all the checks again, we do not immediately
    // exit here as it could be the case that we were woken but the
    // expected value does not match
    if let Some(_woken) = unsafe { handle_rewind::<M, bool>(&mut ctx) } {
        let env = ctx.data();
        let memory = unsafe { env.memory_view(&ctx) };
        if is_registered_shared_futex_offset(env, futex_ptr.offset().into()) {
            sync_shared_memory_to_guest::<M>(env, &memory)
                .map_err(|err| WasiError::Exit(err.into()))?;
        }
        // fall through so the normal checks kick in, this will
        // ensure that the expected value has changed before
        // this syscall returns even if it was woken
    }

    // Determine the timeout
    let mut env = ctx.data();
    let timeout = {
        let memory = unsafe { env.memory_view(&ctx) };
        read_optional_timeout::<M>(&memory, timeout)?
    };
    Span::current().record("timeout", format!("{timeout:?}"));

    let state = env.state.clone();
    let futex_idx: u64 = futex_ptr.offset().into();
    let sync_shared = is_registered_shared_futex_offset(env, futex_idx);
    Span::current().record("futex_idx", futex_idx);

    // We generate a new poller which also registers in the
    // shared state futex lookup. When this object is dropped
    // it will remove itself from the lookup. It can also be
    // removed whenever the wake call is invoked (which could
    // be before the poller is polled).
    let poller = {
        let mut guard = env.state.futexs.lock().unwrap();
        guard.poller_seed += 1;
        let poller_idx = guard.poller_seed;

        // Create the timeout if one exists
        let timeout = timeout.map(|timeout| env.tasks().sleep_now(timeout));

        // We insert the futex before we check the condition variable to avoid
        // certain race conditions
        let futex = guard.futexes.entry(futex_idx).or_default();
        futex.wakers.insert(poller_idx, Default::default());
        futex.masks.insert(poller_idx, mask);
        guard.poller_futexes.insert(poller_idx, futex_idx);

        Span::current().record("poller_idx", poller_idx);
        FutexPoller {
            state: env.state.clone(),
            poller_idx,
            futex_idx,
            timeout,
        }
    };

    // We check if the expected value has changed. POSIX/Linux-shaped futex
    // waiters use EAGAIN for this race: the caller registered interest, but
    // the futex became available before the wait actually slept.
    let memory = unsafe { env.memory_view(&ctx) };
    if sync_shared {
        sync_shared_memory_to_guest::<M>(env, &memory)
            .map_err(|err| WasiError::Exit(err.into()))?;
    }
    let val = wasi_try_mem_ok!(futex_ptr.read(&memory));
    if val != expected {
        return Ok(Errno::Again);
    }

    // We clear the woken flag (so if the poller fails to trigger
    // then the value is not set) - the poller will set it to true
    wasi_try_mem_ok!(ret_woken.write(&memory, Bool::False));

    tracing::trace!("wait on {futex_idx}");
    let res = wasi_try_ok!(__asyncify(&mut ctx, None, async move { Ok(poller.await) })?);
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    if sync_shared {
        sync_shared_memory_to_guest::<M>(env, &memory)
            .map_err(|err| WasiError::Exit(err.into()))?;
    }
    if res {
        wasi_try_mem_ok!(ret_woken.write(&memory, Bool::True));
    } else {
        wasi_try_mem_ok!(ret_woken.write(&memory, Bool::False));
    }
    Ok(Errno::Success)
}

struct FutexWaitAnyEntry<M: MemorySize> {
    waiter_index: u32,
    futex_idx: u64,
    futex_offset: M::Offset,
    expected: u32,
    mask: u32,
}

fn read_futex_wait_any_entries<M: MemorySize>(
    memory: &MemoryView,
    waiters: WasmPtr<u8, M>,
    waiter_count: usize,
) -> Result<Vec<FutexWaitAnyEntry<M>>, WasiError> {
    const FUTEX_WAITV_ENTRY_SIZE: usize = 24;

    let start: usize = waiters
        .offset()
        .try_into()
        .map_err(|_| WasiError::Exit(Errno::Inval.into()))?;
    let byte_len = waiter_count
        .checked_mul(FUTEX_WAITV_ENTRY_SIZE)
        .ok_or_else(|| WasiError::Exit(Errno::Overflow.into()))?;
    let end = start
        .checked_add(byte_len)
        .ok_or_else(|| WasiError::Exit(Errno::Overflow.into()))?;
    let data = unsafe { memory.data_unchecked() };
    let bytes = data
        .get(start..end)
        .ok_or_else(|| WasiError::Exit(Errno::Fault.into()))?;

    let mut entries = Vec::with_capacity(waiter_count);
    for (index, chunk) in bytes.chunks_exact(FUTEX_WAITV_ENTRY_SIZE).enumerate() {
        let expected = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
        let futex_idx = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
        let mask = u32::MAX;
        let futex_offset: M::Offset = futex_idx
            .try_into()
            .map_err(|_| WasiError::Exit(Errno::Inval.into()))?;
        entries.push(FutexWaitAnyEntry {
            waiter_index: index as u32,
            futex_idx,
            futex_offset,
            expected: expected as u32,
            mask,
        });
    }
    Ok(entries)
}

fn read_optional_timeout<M: MemorySize>(
    memory: &MemoryView,
    timeout: WasmPtr<OptionTimestamp, M>,
) -> Result<Option<Duration>, WasiError> {
    if timeout.offset().into() == 0 {
        return Ok(None);
    }
    let timeout = timeout
        .read(memory)
        .map_err(|err| WasiError::Exit(mem_error_to_wasi(err).into()))?;
    Ok(match timeout.tag {
        OptionTag::Some => Some(Duration::from_nanos(timeout.u)),
        _ => None,
    })
}
