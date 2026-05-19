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

    let mut guard = state.futexs.lock().unwrap();
    let wake_ids = collect_matching_waiters(&guard, from, mask, wake_count as usize);
    let wake_id_count = wake_ids.len();
    let requeue_ids = collect_matching_waiters(
        &guard,
        from,
        mask,
        wake_id_count.saturating_add(requeue_count as usize),
    )
    .drain(wake_id_count..)
    .collect::<Vec<_>>();

    let affected = requeue_futex_address_locked(&mut guard, from, to, wake_ids, requeue_ids);
    drop(guard);

    wasi_try_mem_ok!(ret_affected.write(&memory, affected));
    Ok(Errno::Success)
}

fn requeue_futex_address_locked(
    guard: &mut crate::state::WasiFutexState,
    from: u64,
    to: u64,
    wake_ids: Vec<u64>,
    requeue_ids: Vec<u64>,
) -> u32 {
    let mut affected = 0u32;
    let mut requeued = Vec::new();
    let mut woke_pollers = Vec::new();
    let mut moved_pollers = Vec::new();
    let mut remove_source = false;
    if let Some(futex) = guard.futexes.get_mut(&from) {
        for id in wake_ids {
            futex.masks.remove(&id);
            if let Some(waker) = futex.wakers.remove(&id) {
                woke_pollers.push(id);
                if let Some(waker) = waker {
                    waker.wake();
                }
                affected = affected.saturating_add(1);
            }
        }
        for id in requeue_ids {
            let waiter_mask = futex.masks.remove(&id).unwrap_or(u32::MAX);
            if let Some(waker) = futex.wakers.remove(&id) {
                moved_pollers.push(id);
                requeued.push((id, waker, waiter_mask));
                affected = affected.saturating_add(1);
            }
        }
        remove_source = futex.wakers.is_empty();
    }
    for id in woke_pollers {
        guard.poller_futexes.remove(&id);
        guard.woken_pollers.insert(id);
    }
    for id in moved_pollers {
        guard.poller_futexes.insert(id, to);
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
    affected
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
    let woken = wake_futex_address_locked(&mut guard, pointer, mask, max_wake);
    if woken > 0 {
        tracing::trace!("wake(hit) on {pointer}");
    } else {
        tracing::trace!("wake(miss) on {pointer}");
    }
    woken
}

fn wake_futex_address_locked(
    guard: &mut crate::state::WasiFutexState,
    pointer: u64,
    mask: u32,
    max_wake: u32,
) -> u32 {
    let wake_ids = collect_matching_waiters(guard, pointer, mask, max_wake as usize);
    let mut woken = 0u32;
    let mut woke_pollers = Vec::new();
    let mut remove_futex = false;
    if let Some(futex) = guard.futexes.get_mut(&pointer) {
        for id in wake_ids {
            futex.masks.remove(&id);
            if let Some(waker) = futex.wakers.remove(&id) {
                woke_pollers.push(id);
                if let Some(waker) = waker {
                    waker.wake();
                }
                woken = woken.saturating_add(1);
            }
        }
        remove_futex = futex.wakers.is_empty();
    }
    for id in woke_pollers {
        guard.poller_futexes.remove(&id);
        guard.woken_pollers.insert(id);
    }
    if remove_futex {
        guard.futexes.remove(&pointer);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{WasiFutex, WasiFutexState};

    #[test]
    fn wake_counts_waiter_before_rust_waker_is_installed() {
        let mut state = WasiFutexState::default();
        let futex = state.futexes.entry(64).or_insert_with(WasiFutex::default);
        futex.wakers.insert(1, None);
        futex.masks.insert(1, u32::MAX);

        assert_eq!(wake_futex_address_locked(&mut state, 64, u32::MAX, 1), 1);
        assert!(!state.futexes.contains_key(&64));
    }

    #[test]
    fn wake_respects_waiter_masks() {
        let mut state = WasiFutexState::default();
        let futex = state.futexes.entry(64).or_insert_with(WasiFutex::default);
        futex.wakers.insert(1, None);
        futex.masks.insert(1, 0b0010);
        futex.wakers.insert(2, None);
        futex.masks.insert(2, 0b0100);

        assert_eq!(wake_futex_address_locked(&mut state, 64, 0b0010, 1), 1);
        let futex = state.futexes.get(&64).expect("second waiter remains");
        assert!(!futex.wakers.contains_key(&1));
        assert!(futex.wakers.contains_key(&2));
    }

    #[test]
    fn requeue_moves_waiter_without_waking_it() {
        let mut state = WasiFutexState::default();
        let futex = state.futexes.entry(64).or_insert_with(WasiFutex::default);
        futex.wakers.insert(1, None);
        futex.masks.insert(1, u32::MAX);
        state.poller_futexes.insert(1, 64);

        assert_eq!(requeue_futex_address_locked(&mut state, 64, 96, Vec::new(), vec![1]), 1);
        assert!(!state.woken_pollers.contains(&1));
        assert_eq!(state.poller_futexes.get(&1), Some(&96));
        assert!(!state.futexes.contains_key(&64));
        assert!(state.futexes.get(&96).expect("target futex").wakers.contains_key(&1));

        assert_eq!(wake_futex_address_locked(&mut state, 96, u32::MAX, 1), 1);
        assert!(state.woken_pollers.contains(&1));
        assert!(!state.poller_futexes.contains_key(&1));
    }
}
