use super::*;
use crate::syscalls::*;

/// Wake up one thread that's blocked on futex_wait on this futex.
/// Returns true if this actually woke up such a thread,
/// or false if no thread was waiting on this futex.
///
/// ## Parameters
///
/// * `futex` - Memory location that holds a futex that others may be waiting on
#[instrument(level = "trace", skip_all, fields(futex_idx = field::Empty, woken = field::Empty), ret)]
pub fn futex_wake<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    futex_ptr: WasmPtr<u32, M>,
    ret_woken: WasmPtr<Bool, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    futex_wake_mask_internal(ctx, futex_ptr, u32::MAX, ret_woken)
}

pub fn futex_wake_mask<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    futex_ptr: WasmPtr<u32, M>,
    mask: u32,
    ret_woken: WasmPtr<Bool, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    futex_wake_mask_internal(ctx, futex_ptr, mask, ret_woken)
}

pub fn futex_requeue<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    from_futex: WasmPtr<u32, M>,
    to_futex: WasmPtr<u32, M>,
    wake_count: u32,
    requeue_count: u32,
    mask: u32,
    ret_affected: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if mask == 0 || from_futex.offset() == to_futex.offset() {
        return Ok(Errno::Inval);
    }

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let state = env.state.deref();
    let from = from_futex.offset().into();
    let to = to_futex.offset().into();
    let mut affected = 0u32;

    let mut guard = state.futexs.lock().unwrap();
    let mut wake_ids = collect_matching_waiters(&guard, from, mask, wake_count as usize);
    let wake_id_count = wake_ids.len();
    let requeue_ids = collect_matching_waiters(
        &guard,
        from,
        mask,
        wake_id_count.saturating_add(requeue_count as usize),
    )
    .drain(wake_id_count..)
    .collect::<Vec<_>>();

    let mut requeued = Vec::new();
    let mut remove_source = false;
    if let Some(futex) = guard.futexes.get_mut(&from) {
        for id in wake_ids {
            futex.masks.remove(&id);
            if let Some(Some(waker)) = futex.wakers.remove(&id) {
                waker.wake();
                affected = affected.saturating_add(1);
            }
        }
        for id in requeue_ids {
            let waiter_mask = futex.masks.remove(&id).unwrap_or(u32::MAX);
            if let Some(waker) = futex.wakers.remove(&id) {
                requeued.push((id, waker, waiter_mask));
                affected = affected.saturating_add(1);
            }
        }
        remove_source = futex.wakers.is_empty();
    }
    if remove_source {
        guard.futexes.remove(&from);
    }
    if !requeued.is_empty() {
        let target = guard.futexes.entry(to).or_default();
        for (id, waker, waiter_mask) in requeued {
            target.wakers.insert(id, waker);
            target.masks.insert(id, waiter_mask);
        }
    }
    drop(guard);

    wasi_try_mem_ok!(ret_affected.write(&memory, affected));
    Ok(Errno::Success)
}

fn futex_wake_mask_internal<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    futex_ptr: WasmPtr<u32, M>,
    mask: u32,
    ret_woken: WasmPtr<Bool, M>,
) -> Result<Errno, WasiError> {
    if mask == 0 {
        return Ok(Errno::Inval);
    }
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let state = env.state.deref();

    let pointer: u64 = futex_ptr.offset().into();
    Span::current().record("futex_idx", pointer);

    let woken = wake_futex_address(state, pointer, mask, 1) > 0;
    Span::current().record("woken", woken);

    let woken = match woken {
        false => Bool::False,
        true => Bool::True,
    };
    wasi_try_mem_ok!(ret_woken.write(&memory, woken));

    Ok(Errno::Success)
}

pub(crate) fn wake_futex_address(state: &WasiState, pointer: u64, mask: u32, max_wake: u32) -> u32 {
    if mask == 0 || max_wake == 0 {
        return 0;
    }

    let mut guard = state.futexs.lock().unwrap();
    let wake_ids = collect_matching_waiters(&guard, pointer, mask, max_wake as usize);
    let mut woken = 0u32;
    let mut remove_futex = false;
    if let Some(futex) = guard.futexes.get_mut(&pointer) {
        for id in wake_ids {
            futex.masks.remove(&id);
            if let Some(Some(waker)) = futex.wakers.remove(&id) {
                waker.wake();
                woken = woken.saturating_add(1);
            }
        }
        remove_futex = futex.wakers.is_empty();
    }
    if remove_futex {
        guard.futexes.remove(&pointer);
    }
    if woken > 0 {
        tracing::trace!("wake(hit) on {pointer}");
    } else {
        tracing::trace!("wake(miss) on {pointer}");
    }
    woken
}

fn collect_matching_waiters(
    guard: &crate::state::WasiFutexState,
    pointer: u64,
    mask: u32,
    max_count: usize,
) -> Vec<u64> {
    if max_count == 0 {
        return Vec::new();
    }
    guard
        .futexes
        .get(&pointer)
        .map(|futex| {
            futex
                .wakers
                .keys()
                .copied()
                .filter(|id| futex.masks.get(id).copied().unwrap_or(u32::MAX) & mask != 0)
                .take(max_count)
                .collect()
        })
        .unwrap_or_default()
}
