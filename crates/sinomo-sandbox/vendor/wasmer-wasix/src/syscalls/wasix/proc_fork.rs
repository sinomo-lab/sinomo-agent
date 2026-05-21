use super::*;
use crate::{
    WasiThreadHandle, WasiVForkAsyncify, capture_store_snapshot,
    os::task::{OwnedTaskStatus, TaskStatus, thread::WasiMemoryLayout},
    runtime::task_manager::{TaskWasm, TaskWasmRunProperties},
    state::context_switching::ContextSwitchingEnvironment,
    syscalls::*,
};
use serde::{Deserialize, Serialize};
use wasmer::{FromToNativeWasmType, Memory};

#[derive(Serialize, Deserialize)]
pub(crate) struct ForkResult {
    pub pid: Pid,
    pub ret: Errno,
}

struct ForkOutcome {
    pid: Pid,
    ret: Errno,
}

macro_rules! sinomo_debug {
    ($($arg:tt)*) => {
        if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
            eprintln!($($arg)*);
        }
    };
}

#[derive(Serialize, Deserialize)]
struct CloneThreadResult {
    tid: Tid,
    ret: Errno,
}

/// ### `proc_fork()`
/// Forks the current process into a new subprocess. If the function
/// returns a zero then its the new subprocess. If it returns a positive
/// number then its the current process and the $pid represents the child.
#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn proc_fork<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    copy_memory: Bool,
    pid_ptr: WasmPtr<Pid, M>,
) -> Result<Errno, WasiError> {
    let outcome = proc_fork_common(ctx, copy_memory, Some(pid_ptr))?;
    Ok(outcome.ret)
}

/// ### `fork()`
/// POSIX-shaped fork wrapper. Returns 0 in the child, the child pid in the
/// parent, or a negative errno value on failure.
#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn fork<M: MemorySize>(ctx: FunctionEnvMut<'_, WasiEnv>) -> Result<i32, WasiError> {
    let outcome = proc_fork_common::<M>(ctx, Bool::True, None)?;
    Ok(fork_outcome_to_posix(outcome))
}

/// ### `vfork()`
/// POSIX-shaped vfork wrapper over the existing WASIX lightweight fork path.
#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn vfork<M: MemorySize>(ctx: FunctionEnvMut<'_, WasiEnv>) -> Result<i32, WasiError> {
    let outcome = proc_fork_common::<M>(ctx, Bool::False, None)?;
    Ok(fork_outcome_to_posix(outcome))
}

/// Runtime hint used by the POSIX sysroot `mmap()` implementation when a
/// process creates a shared mapping. The runtime keeps normal fork memory
/// private while synchronizing these registered guest ranges across related
/// processes.
#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn mmap_shared_hint<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
) -> Result<Errno, WasiError> {
    mmap_shared_hint_inner::<M>(ctx, offset, len, None, 0)
}

pub fn mmap_shared_file_hint<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: i32,
    file_offset: u64,
    offset: M::Offset,
    len: M::Offset,
) -> Result<Errno, WasiError> {
    mmap_shared_hint_inner::<M>(ctx, offset, len, Some(fd), file_offset)
}

fn mmap_shared_hint_inner<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
    fd: Option<i32>,
    file_offset: u64,
) -> Result<Errno, WasiError> {
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let offset_u64 = offset.into();
    let len_u64 = len.into();
    let existing_mapping = env
        .state
        .shared_memory_mappings()
        .into_iter()
        .any(|mapping| {
            (offset_u64 >= mapping.offset
                && offset_u64.saturating_add(len_u64) <= mapping.offset.saturating_add(mapping.len))
                || (fd.is_some()
                    && mapping.fd == fd
                    && mapping.file_offset == file_offset
                    && mapping.len == len_u64)
        });
    if existing_mapping && fd.is_none() {
        return match sync_shared_memory_range_to_guest::<M>(env, &memory, offset_u64, len_u64) {
            Ok(()) => Ok(Errno::Success),
            Err(err) => Ok(err),
        };
    }
    let bytes = wasi_try_mem_ok!(WasmPtr::<u8, M>::new(offset).slice(&memory, len))
        .read_to_vec()
        .map_err(mem_error_to_wasi)
        .map_err(|err| WasiError::Exit(err.into()))?;
    if let Some(fd) = fd {
        env.state.register_shared_memory_mapping_with_file(
            offset_u64,
            len_u64,
            bytes,
            fd,
            file_offset,
        );
    } else {
        env.state
            .register_shared_memory_mapping(offset_u64, len_u64, bytes);
    }
    if existing_mapping {
        return match sync_shared_memory_range_to_guest::<M>(env, &memory, offset_u64, len_u64) {
            Ok(()) => Ok(Errno::Success),
            Err(err) => Ok(err),
        };
    }
    Ok(Errno::Success)
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_futex_hint<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
) -> Result<Errno, WasiError> {
    let offset = offset.into();
    let len = len.into();
    ctx.data().state.register_shared_futex_range(offset, len);
    ctx.data()
        .state
        .shared_mutex_owners
        .lock()
        .unwrap()
        .remove(&offset);
    ctx.data()
        .state
        .shared_mutex_owner_pids
        .lock()
        .unwrap()
        .remove(&offset);
    ctx.data()
        .state
        .shared_mutex_depths
        .lock()
        .unwrap()
        .remove(&offset);
    ctx.data()
        .state
        .shared_cond_sequences
        .lock()
        .unwrap()
        .remove(&offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    match sync_shared_memory_range_from_guest::<M>(ctx.data(), &memory, offset, len) {
        Ok(()) => Ok(Errno::Success),
        Err(err) => Ok(err),
    }
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_futex_unhint<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
) -> Result<Errno, WasiError> {
    ctx.data()
        .state
        .unregister_shared_futex_range(offset.into(), len.into());
    Ok(Errno::Success)
}

pub fn shared_memory_is_cross_process(ctx: FunctionEnvMut<'_, WasiEnv>) -> u32 {
    u32::from(has_live_cross_process_shared_memory(ctx.data()))
}

pub fn shared_memory_was_cross_process(ctx: FunctionEnvMut<'_, WasiEnv>) -> u32 {
    u32::from(ctx.data().state.has_cross_process_shared_memory())
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_memory_sync_from<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
) -> Result<Errno, WasiError> {
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    match sync_shared_memory_from_guest::<M>(env, &memory) {
        Ok(()) => Ok(Errno::Success),
        Err(err) => Ok(err),
    }
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_memory_sync_from_preserving_futexes<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
) -> Result<Errno, WasiError> {
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let preserve_ranges = env
        .state
        .shared_futex_ranges()
        .into_iter()
        .map(|range| (range.start, range.end.saturating_sub(range.start) as usize))
        .collect::<Vec<_>>();
    match sync_shared_memory_from_guest_preserving_ranges::<M>(env, &memory, &preserve_ranges) {
        Ok(()) => Ok(Errno::Success),
        Err(err) => Ok(err),
    }
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_memory_sync_to<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
) -> Result<Errno, WasiError> {
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    match sync_shared_memory_to_guest::<M>(env, &memory) {
        Ok(()) => Ok(Errno::Success),
        Err(err) => Ok(err),
    }
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_memory_range_sync_from<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
) -> Result<Errno, WasiError> {
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    match sync_shared_memory_range_from_guest::<M>(env, &memory, offset.into(), len.into()) {
        Ok(()) => Ok(Errno::Success),
        Err(err) => Ok(err),
    }
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_memory_range_sync_to<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
) -> Result<Errno, WasiError> {
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    match sync_shared_memory_range_to_guest::<M>(env, &memory, offset.into(), len.into()) {
        Ok(()) => Ok(Errno::Success),
        Err(err) => Ok(err),
    }
}

pub fn shared_cond_sequence<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
) -> Result<u32, WasiError> {
    let offset = offset.into();
    if let Some(seq) = ctx
        .data()
        .state
        .shared_cond_sequences
        .lock()
        .unwrap()
        .get(&offset)
        .copied()
    {
        let memory = unsafe { ctx.data().memory_view(&ctx) };
        let _ = with_shared_cond_sequence_word::<M>(ctx.data(), &memory, offset, seq);
        return Ok(seq);
    }

    if let Some(seq) =
        shared_cond_sequence_word(ctx.data(), offset).map_err(|err| WasiError::Exit(err.into()))?
    {
        ctx.data()
            .state
            .shared_cond_sequences
            .lock()
            .unwrap()
            .insert(offset, seq);
        return Ok(seq);
    }

    let seq = ctx
        .data()
        .state
        .shared_cond_sequences
        .lock()
        .unwrap()
        .get(&offset)
        .copied()
        .unwrap_or(0);
    if seq == 0 {
        sinomo_debug!(
            "sinomo-debug shared_cond_sequence zero pid={} offset={:#x}",
            ctx.data().pid().raw(),
            offset
        );
    }
    Ok(seq)
}

pub fn shared_cond_broadcast<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
) -> Result<u32, WasiError> {
    let offset = offset.into();
    let seq = {
        let mut sequences = ctx.data().state.shared_cond_sequences.lock().unwrap();
        let seq = sequences.get(&offset).copied().unwrap_or(0).wrapping_add(1);
        sequences.insert(offset, seq);
        seq
    };
    sinomo_debug!(
        "sinomo-debug shared_cond_broadcast pid={} offset={:#x} seq={}",
        ctx.data().pid().raw(),
        offset,
        seq
    );
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let _ = with_shared_cond_sequence_word::<M>(ctx.data(), &memory, offset, seq);
    Ok(seq)
}

pub fn shared_barrier_generation<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
) -> Result<u32, WasiError> {
    let offset = offset.into();
    let pid = ctx.data().pid().raw();
    let result = with_shared_u32_words(ctx.data(), offset, 5, |words| {
        sinomo_debug!(
            "sinomo-debug shared_barrier_generation pid={} offset={:#x} words={:?}",
            pid,
            offset,
            words
        );
        words[2]
    });
    Ok(result.unwrap_or(0))
}

pub fn shared_barrier_arrive<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    limit: u32,
) -> Result<i32, WasiError> {
    let offset = offset.into();
    let pid = ctx.data().pid().raw();
    if limit == 0 {
        return Ok(Errno::Inval.to_native() as i32);
    }
    let result = with_shared_u32_words_mut(ctx.data(), offset, 5, |words| {
        let arrived = words[1].saturating_add(1);
        let result = if arrived == limit {
            words[1] = 0;
            words[3] = limit.saturating_sub(1);
            words[2] = words[2].wrapping_add(1);
            -1
        } else if arrived > limit {
            Errno::Inval.to_native() as i32
        } else {
            words[1] = arrived;
            0
        };
        sinomo_debug!(
            "sinomo-debug shared_barrier_arrive pid={} offset={:#x} limit={} result={} words={:?}",
            pid,
            offset,
            limit,
            result,
            words
        );
        result
    });
    Ok(result.unwrap_or_else(|err| err.to_native() as i32))
}

pub fn shared_barrier_release<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
) -> Result<i32, WasiError> {
    let offset = offset.into();
    let pid = ctx.data().pid().raw();
    let result = with_shared_u32_words_mut(ctx.data(), offset, 5, |words| {
        let result = if words[3] == 0 {
            0
        } else {
            words[3] -= 1;
            words[3] as i32
        };
        sinomo_debug!(
            "sinomo-debug shared_barrier_release pid={} offset={:#x} result={} words={:?}",
            pid,
            offset,
            result,
            words
        );
        result
    });
    Ok(result.unwrap_or_else(|err| err.to_native() as i32))
}

const SINOMO_RWLOCK_WORDS: usize = 6;
const SINOMO_RWLOCK_STATE_WORD: usize = 0;
const SINOMO_RWLOCK_WRITER_WAITERS_WORD: usize = 1;
const SINOMO_RWLOCK_WRITER_PRIORITY_WORD: usize = 4;
const SINOMO_RWLOCK_WRITER_POLICY_WORD: usize = 5;
const SINOMO_SCHED_FIFO: i32 = 1;
const SINOMO_SCHED_RR: i32 = 2;

pub fn shared_rwlock_try_rdlock<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    policy: i32,
    priority: i32,
    registered_waiter: u32,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    publish_shared_memory_around_rwlock::<M>(ctx.data(), &memory, offset)?;
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let mut state = lock.state.lock().unwrap();
    sinomo_debug!(
        "sinomo-debug shared_rwlock_try_rdlock pid={} offset={:#x} policy={} priority={} state readers={} writer={} waiters={} max_policy={} max_priority={}",
        ctx.data().pid().raw(),
        offset,
        policy,
        priority,
        state.readers,
        state.writer,
        state.writer_waiters,
        state.max_writer_policy,
        state.max_writer_priority
    );
    let blocks_reader = state.writer
        || (state.writer_waiters > 0
            && rwlock_policy_orders_readers(state.max_writer_policy)
            && rwlock_policy_orders_readers(policy)
            && priority <= state.max_writer_priority);
    if blocks_reader {
        write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
        return Ok(Errno::Busy.to_native() as i32);
    }
    if registered_waiter != 0 {
        rwlock_remove_reader_waiter(&mut state, policy, priority);
    }
    state.readers = state.readers.saturating_add(1);
    write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
    Ok(0)
}

pub fn shared_rwlock_try_wrlock<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    registered_waiter: u32,
    policy: i32,
    priority: i32,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    publish_shared_memory_around_rwlock::<M>(ctx.data(), &memory, offset)?;
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let owner = shared_mutex_owner_token(ctx.data());
    let mut state = lock.state.lock().unwrap();
    sinomo_debug!(
        "sinomo-debug shared_rwlock_try_wrlock pid={} offset={:#x} registered={} state readers={} writer={} waiters={}",
        ctx.data().pid().raw(),
        offset,
        registered_waiter,
        state.readers,
        state.writer,
        state.writer_waiters
    );
    if state.writer && state.writer_owner == owner {
        write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
        return Ok(Errno::Deadlk.to_native() as i32);
    }
    if registered_waiter != 0
        && state.writer_waiters > 0
        && rwlock_policy_orders_readers(state.max_writer_policy)
        && rwlock_policy_orders_readers(policy)
        && priority < state.max_writer_priority
    {
        write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
        return Ok(Errno::Busy.to_native() as i32);
    }
    if registered_waiter != 0
        && rwlock_policy_orders_readers(policy)
        && state.max_reader_priority > priority
    {
        write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
        return Ok(Errno::Busy.to_native() as i32);
    }
    if state.writer || state.readers > 0 {
        write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
        return Ok(Errno::Busy.to_native() as i32);
    }
    state.writer = true;
    state.writer_owner = owner;
    if registered_waiter != 0 && state.writer_waiters > 0 {
        rwlock_remove_writer_waiter(&mut state, policy, priority);
    }
    write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
    Ok(0)
}

pub fn shared_rwlock_register_writer<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    policy: i32,
    priority: i32,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    publish_shared_memory_around_rwlock::<M>(ctx.data(), &memory, offset)?;
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let mut state = lock.state.lock().unwrap();
    sinomo_debug!(
        "sinomo-debug shared_rwlock_register_writer pid={} offset={:#x} policy={} priority={} before waiters={} max_policy={} max_priority={}",
        ctx.data().pid().raw(),
        offset,
        policy,
        priority,
        state.writer_waiters,
        state.max_writer_policy,
        state.max_writer_priority
    );
    state.writer_waiters = state.writer_waiters.saturating_add(1);
    state.writer_waiter_priorities.push((policy, priority));
    if rwlock_policy_orders_readers(policy) && priority >= state.max_writer_priority {
        state.max_writer_priority = priority;
        state.max_writer_policy = policy;
    }
    write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
    Ok(0)
}

pub fn shared_rwlock_unregister_writer<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    policy: i32,
    priority: i32,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    publish_shared_memory_around_rwlock::<M>(ctx.data(), &memory, offset)?;
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let mut state = lock.state.lock().unwrap();
    sinomo_debug!(
        "sinomo-debug shared_rwlock_unlock pid={} offset={:#x} state readers={} writer={} waiters={}",
        ctx.data().pid().raw(),
        offset,
        state.readers,
        state.writer,
        state.writer_waiters
    );
    rwlock_remove_writer_waiter(&mut state, policy, priority);
    if state.writer_waiters == 0 {
        state.max_writer_priority = 0;
        state.max_writer_policy = 0;
    } else {
        rwlock_recompute_writer_priority(&mut state);
    }
    write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
    Ok(0)
}

pub fn shared_rwlock_register_reader<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    policy: i32,
    priority: i32,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    publish_shared_memory_around_rwlock::<M>(ctx.data(), &memory, offset)?;
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let mut state = lock.state.lock().unwrap();
    state.reader_waiter_priorities.push((policy, priority));
    if rwlock_policy_orders_readers(policy) && priority > state.max_reader_priority {
        state.max_reader_priority = priority;
    }
    write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
    Ok(0)
}

pub fn shared_rwlock_unregister_reader<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    policy: i32,
    priority: i32,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let mut state = lock.state.lock().unwrap();
    rwlock_remove_reader_waiter(&mut state, policy, priority);
    write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
    Ok(0)
}

pub fn shared_rwlock_unlock<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    publish_shared_memory_around_rwlock::<M>(ctx.data(), &memory, offset)?;
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let mut state = lock.state.lock().unwrap();
    if state.writer {
        state.writer = false;
        state.writer_owner = 0;
    } else if state.readers > 0 {
        state.readers -= 1;
    } else {
        return Ok(Errno::Perm.to_native() as i32);
    }
    write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
    lock.waiters.notify_all();
    Ok(0)
}

pub fn shared_rwlock_wait<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    timeout_ns: i64,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let state = lock.state.lock().unwrap();
    let wait_duration = if timeout_ns < 0 {
        std::time::Duration::from_millis(10)
    } else {
        std::time::Duration::from_nanos(timeout_ns as u64).min(std::time::Duration::from_millis(10))
    };
    drop(lock.waiters.wait_timeout(state, wait_duration).unwrap());
    Ok(0)
}

pub fn shared_rwlock_destroy<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let key = shared_rwlock_key(ctx.data(), offset);
    let mut locks = ctx.data().state.shared_rwlocks.lock().unwrap();
    if let Some(lock) = locks.get(&key) {
        let state = lock.state.lock().unwrap();
        if state.writer || state.readers > 0 {
            return Ok(Errno::Busy.to_native() as i32);
        }
    }
    locks.remove(&key);
    Ok(0)
}

fn rwlock_policy_orders_readers(policy: i32) -> bool {
    policy == SINOMO_SCHED_FIFO || policy == SINOMO_SCHED_RR
}

fn rwlock_remove_writer_waiter(
    state: &mut crate::state::WasiSharedRwLockState,
    policy: i32,
    priority: i32,
) {
    if state.writer_waiters > 0 {
        state.writer_waiters -= 1;
    }
    if let Some(index) =
        state
            .writer_waiter_priorities
            .iter()
            .position(|(waiter_policy, waiter_priority)| {
                *waiter_policy == policy && *waiter_priority == priority
            })
    {
        state.writer_waiter_priorities.remove(index);
    } else {
        state.writer_waiter_priorities.pop();
    }
    rwlock_recompute_writer_priority(state);
}

fn rwlock_recompute_writer_priority(state: &mut crate::state::WasiSharedRwLockState) {
    state.max_writer_priority = 0;
    state.max_writer_policy = 0;
    for (policy, priority) in &state.writer_waiter_priorities {
        if rwlock_policy_orders_readers(*policy) && *priority >= state.max_writer_priority {
            state.max_writer_priority = *priority;
            state.max_writer_policy = *policy;
        }
    }
    state.writer_waiters = state.writer_waiter_priorities.len() as u32;
}

fn rwlock_remove_reader_waiter(
    state: &mut crate::state::WasiSharedRwLockState,
    policy: i32,
    priority: i32,
) {
    if let Some(index) =
        state
            .reader_waiter_priorities
            .iter()
            .position(|(waiter_policy, waiter_priority)| {
                *waiter_policy == policy && *waiter_priority == priority
            })
    {
        state.reader_waiter_priorities.remove(index);
    } else {
        state.reader_waiter_priorities.pop();
    }
    state.max_reader_priority = 0;
    for (policy, priority) in &state.reader_waiter_priorities {
        if rwlock_policy_orders_readers(*policy) && *priority > state.max_reader_priority {
            state.max_reader_priority = *priority;
        }
    }
}

fn publish_shared_memory_around_rwlock<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
) -> Result<(), WasiError> {
    if env.state.has_shared_memory_mappings() {
        sync_shared_memory_from_guest_preserving_range::<M>(
            env,
            memory,
            offset,
            SINOMO_RWLOCK_WORDS * 4,
        )
        .map_err(|err| WasiError::Exit(err.into()))?;
    }
    Ok(())
}

fn shared_rwlock<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
) -> Result<std::sync::Arc<crate::state::WasiSharedRwLock>, WasiError> {
    let key = shared_rwlock_key(env, offset);
    let mut locks = env.state.shared_rwlocks.lock().unwrap();
    sinomo_debug!(
        "sinomo-debug shared_rwlock lookup pid={} offset={:#x} key={:#x} existing={}",
        env.pid().raw(),
        offset,
        key,
        locks.contains_key(&key)
    );
    Ok(locks
        .entry(key)
        .or_insert_with(|| {
            let mut initial = crate::state::WasiSharedRwLockState::default();
            if let Ok(words) = read_rwlock_words::<M>(memory, offset) {
                let raw_state = words[SINOMO_RWLOCK_STATE_WORD] as i32;
                initial.readers = raw_state.max(0) as u32;
                initial.writer = raw_state == -1;
                initial.writer_owner = 0;
                initial.writer_waiters = words[SINOMO_RWLOCK_WRITER_WAITERS_WORD];
                initial.max_writer_priority = words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD] as i32;
                initial.max_writer_policy = words[SINOMO_RWLOCK_WRITER_POLICY_WORD] as i32;
            }
            std::sync::Arc::new(crate::state::WasiSharedRwLock {
                state: std::sync::Mutex::new(initial),
                waiters: std::sync::Condvar::new(),
            })
        })
        .clone())
}

fn shared_rwlock_key(env: &WasiEnv, offset: u64) -> u64 {
    env.state
        .shared_memory_mappings()
        .into_iter()
        .find_map(|mapping| {
            let len = (SINOMO_RWLOCK_WORDS * 4) as u64;
            if offset >= mapping.offset
                && offset.saturating_add(len) <= mapping.offset.saturating_add(mapping.len)
            {
                let base = std::sync::Arc::as_ptr(&mapping.bytes) as usize as u64;
                Some(base ^ ((offset - mapping.offset) << 32))
            } else {
                None
            }
        })
        .unwrap_or(offset)
}

fn read_rwlock_words<M: MemorySize>(
    memory: &MemoryView,
    offset: u64,
) -> Result<[u32; SINOMO_RWLOCK_WORDS], Errno> {
    let ptr = WasmPtr::<u32, M>::new(to_offset::<M>(
        offset.try_into().map_err(|_| Errno::Overflow)?,
    )?);
    let slice = ptr
        .slice(memory, to_offset::<M>(SINOMO_RWLOCK_WORDS)?)
        .map_err(mem_error_to_wasi)?;
    let mut words = [0u32; SINOMO_RWLOCK_WORDS];
    for (index, cell) in slice.iter().enumerate() {
        words[index] = cell.read().map_err(mem_error_to_wasi)?;
    }
    Ok(words)
}

fn write_shared_rwlock_words<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
    state: &crate::state::WasiSharedRwLockState,
) -> Result<(), WasiError> {
    let mut words = [0u32; SINOMO_RWLOCK_WORDS];
    words[SINOMO_RWLOCK_STATE_WORD] = if state.writer {
        (-1i32) as u32
    } else {
        state.readers
    };
    words[SINOMO_RWLOCK_WRITER_WAITERS_WORD] = state.writer_waiters;
    words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD] = state.max_writer_priority as u32;
    words[SINOMO_RWLOCK_WRITER_POLICY_WORD] = state.max_writer_policy as u32;

    let ptr = WasmPtr::<u32, M>::new(
        to_offset::<M>(
            offset
                .try_into()
                .map_err(|_| WasiError::Exit(Errno::Overflow.into()))?,
        )
        .map_err(|err| WasiError::Exit(err.into()))?,
    );
    let slice = ptr
        .slice(
            memory,
            to_offset::<M>(SINOMO_RWLOCK_WORDS).map_err(|err| WasiError::Exit(err.into()))?,
        )
        .map_err(|err| WasiError::Exit(mem_error_to_wasi(err).into()))?;
    for (index, cell) in slice.iter().enumerate() {
        if index == 2 || index == 3 {
            continue;
        }
        cell.write(words[index])
            .map_err(|err| WasiError::Exit(mem_error_to_wasi(err).into()))?;
    }

    if let Some(mapping) = env
        .state
        .shared_memory_mappings()
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add((SINOMO_RWLOCK_WORDS * 4) as u64)
                    <= mapping.offset.saturating_add(mapping.len)
        })
    {
        let index = usize::try_from(offset - mapping.offset)
            .map_err(|_| WasiError::Exit(Errno::Overflow.into()))?;
        let mut bytes = mapping.bytes.lock().unwrap();
        for (word_index, word) in words.iter().enumerate() {
            if word_index == 2 || word_index == 3 {
                continue;
            }
            let start = index + word_index * 4;
            bytes[start..start + 4].copy_from_slice(&word.to_le_bytes());
        }
    }
    Ok(())
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_mutex_try_lock<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    mutex_type: u32,
    owner_tid: u32,
) -> Result<Errno, WasiError> {
    let tid = if owner_tid == 0 {
        shared_mutex_owner_token(ctx.data())
    } else {
        owner_tid
    };
    let guest_offset = offset;
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let offset_u64 = Into::<u64>::into(guest_offset);
    let offset_key = shared_mutex_key(ctx.data(), offset_u64);
    let result = with_mutex_words::<M>(ctx.data(), &memory, offset_u64, |words| {
        let kind = mutex_type & 3;
        let mut owners = ctx.data().state.shared_mutex_owners.lock().unwrap();
        let mut owner_pids = ctx.data().state.shared_mutex_owner_pids.lock().unwrap();
        let mut depths = ctx.data().state.shared_mutex_depths.lock().unwrap();
        let mut owner = words[1] & 0x7fff_ffff;
        if let Some(tracked_owner) = owners.get(&offset_key).copied() {
            owner = tracked_owner;
            words[1] = tracked_owner;
            words[3] = depths.get(&offset_key).copied().unwrap_or(0);
        }
        if owner == 0 {
            owners.remove(&offset_key);
            owner_pids.remove(&offset_key);
            depths.remove(&offset_key);
        }
        if owner == tid {
            if kind == 1 {
                let depth = words[3].saturating_add(1);
                depths.insert(offset_key, depth);
                words[1] = tid;
                words[3] = depth;
                owner_pids.insert(offset_key, ctx.data().pid().raw());
                return Errno::Success;
            }
            if kind == 2 {
                return Errno::Deadlk;
            }
            return Errno::Busy;
        }
        if owner == 0 {
            depths.insert(offset_key, 0);
            owners.insert(offset_key, tid);
            words[1] = tid;
            words[3] = 0;
            owner_pids.insert(offset_key, ctx.data().pid().raw());
            sinomo_debug!(
                "sinomo-debug shared_mutex_try_lock success pid={} offset={:#x} kind={} tid={} words={:?}",
                ctx.data().pid().raw(),
                offset_key,
                kind,
                tid,
                words
            );
            return Errno::Success;
        }
        sinomo_debug!(
            "sinomo-debug shared_mutex_try_lock busy pid={} offset={:#x} kind={} tid={} owner={} words={:?}",
            ctx.data().pid().raw(),
            offset_key,
            kind,
            tid,
            owner,
            words
        );
        Errno::Busy
    });
    // A successful lock acquire must observe ordinary MAP_SHARED writes
    // published by the previous owner. Preserve registered futex objects while
    // pulling the rest of the mapping into the guest so mutex/cond words are
    // still controlled by their dedicated runtime paths.
    if result == Errno::Success && shared_mutex_has_mapping(ctx.data(), offset_u64) {
        let preserve_ranges = ctx
            .data()
            .state
            .shared_futex_ranges()
            .into_iter()
            .map(|range| (range.start, range.end.saturating_sub(range.start) as usize))
            .collect::<Vec<_>>();
        let _ = sync_shared_memory_to_guest_preserving_ranges::<M>(
            ctx.data(),
            &memory,
            &preserve_ranges,
        );
    }
    let _ = copy_shared_mutex_to_guest::<M>(ctx.data(), &memory, guest_offset.into());
    Ok(result)
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_mutex_unlock<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    mutex_type: u32,
    owner_tid: u32,
) -> Result<Errno, WasiError> {
    let tid = if owner_tid == 0 {
        shared_mutex_owner_token(ctx.data())
    } else {
        owner_tid
    };
    let guest_offset = offset;
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let mutex_start = Into::<u64>::into(guest_offset);
    if shared_mutex_has_mapping(ctx.data(), mutex_start) {
        let preserve_ranges = ctx
            .data()
            .state
            .shared_futex_ranges()
            .into_iter()
            .map(|range| (range.start, range.end.saturating_sub(range.start) as usize))
            .collect::<Vec<_>>();
        let mut preserve_ranges = preserve_ranges;
        preserve_ranges.push((mutex_start, 16));
        if let Err(err) = sync_shared_memory_from_guest_preserving_ranges::<M>(
            ctx.data(),
            &memory,
            &preserve_ranges,
        ) {
            return Ok(err);
        }
    }
    let offset_key = shared_mutex_key(ctx.data(), mutex_start);
    let result = with_mutex_words::<M>(ctx.data(), &memory, mutex_start, |words| {
        let kind = mutex_type & 3;
        let mut owners = ctx.data().state.shared_mutex_owners.lock().unwrap();
        let mut owner_pids = ctx.data().state.shared_mutex_owner_pids.lock().unwrap();
        let mut depths = ctx.data().state.shared_mutex_depths.lock().unwrap();
        let mut owner = words[1] & 0x7fff_ffff;
        if let Some(tracked_owner) = owners.get(&offset_key).copied() {
            owner = tracked_owner;
            words[1] = tracked_owner;
            words[3] = depths.get(&offset_key).copied().unwrap_or(0);
        }
        if owner == 0 {
            owners.remove(&offset_key);
            owner_pids.remove(&offset_key);
            depths.remove(&offset_key);
        }
        if owner != tid {
            sinomo_debug!(
                "sinomo-debug shared_mutex_unlock EPERM pid={} offset={:#x} kind={} owner={} tid={} words={:?}",
                ctx.data().pid().raw(),
                offset_key,
                kind,
                owner,
                tid,
                words
            );
            return Errno::Perm;
        }
        let depth = words[3];
        if kind == 1 && depth > 0 {
            let new_depth = depth - 1;
            words[1] = tid;
            words[3] = new_depth;
            owners.insert(offset_key, tid);
            owner_pids.insert(offset_key, ctx.data().pid().raw());
            depths.insert(offset_key, new_depth);
        } else {
            if kind == 1 || kind == 2 {
                sinomo_debug!(
                    "sinomo-debug shared_mutex_unlock success pid={} offset={:#x} kind={} tid={} words_before={:?}",
                    ctx.data().pid().raw(),
                    offset_key,
                    kind,
                    tid,
                    words
                );
            }
            words[1] = 0;
            words[3] = 0;
            owners.remove(&offset_key);
            owner_pids.remove(&offset_key);
            depths.remove(&offset_key);
        }
        Errno::Success
    });
    if mutex_type & 3 == 0 {
        let _ = with_mutex_words::<M>(ctx.data(), &memory, mutex_start, |words| {
            sinomo_debug!(
                "sinomo-debug shared_mutex_unlock normal-after-central pid={} offset={:#x} tid={} words={:?}",
                ctx.data().pid().raw(),
                Into::<u64>::into(guest_offset),
                tid,
                words
            );
            Errno::Success
        });
    }
    // Unlock is a release operation. The guest-to-shared sync above publishes
    // ordinary shared-memory writes before releasing the central mutex owner.
    // Pulling the whole shared mapping back into the unlocking process here can
    // overwrite unrelated guest state during high-contention wakeups; only the
    // mutex word itself needs to be reflected locally before returning.
    let _ = copy_shared_mutex_to_guest::<M>(ctx.data(), &memory, guest_offset.into());
    sinomo_debug!(
        "sinomo-debug shared_mutex_unlock return pid={} offset={:#x} tid={} result={:?}",
        ctx.data().pid().raw(),
        Into::<u64>::into(guest_offset),
        tid,
        result
    );
    Ok(result)
}

fn with_guest_mutex_words<M: MemorySize>(
    memory: &MemoryView,
    offset: M::Offset,
    f: impl FnOnce(&mut [u32; 4]) -> Errno,
) -> Errno {
    let ptr = WasmPtr::<u32, M>::new(offset);
    let slice = match ptr.slice(memory, M::Offset::from(4u32)) {
        Ok(slice) => slice,
        Err(err) => return mem_error_to_wasi(err),
    };
    let mut words = [0u32; 4];
    for (index, cell) in slice.iter().enumerate() {
        words[index] = match cell.read() {
            Ok(value) => value,
            Err(err) => return mem_error_to_wasi(err),
        };
    }
    let result = f(&mut words);
    if mutex_result_preserves_words(result) {
        for (index, cell) in slice.iter().enumerate() {
            if let Err(err) = cell.write(words[index]) {
                return mem_error_to_wasi(err);
            }
        }
    }
    result
}

fn mutex_result_preserves_words(result: Errno) -> bool {
    matches!(
        result,
        Errno::Success | Errno::Busy | Errno::Perm | Errno::Deadlk
    )
}

fn with_mutex_words<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
    f: impl FnOnce(&mut [u32; 4]) -> Errno,
) -> Errno {
    if shared_mutex_has_mapping(env, offset) {
        with_shared_mutex_words(env, offset, f)
    } else {
        let Ok(offset) = usize::try_from(offset) else {
            return Errno::Overflow;
        };
        let Ok(offset) = to_offset::<M>(offset) else {
            return Errno::Overflow;
        };
        with_guest_mutex_words::<M>(memory, offset, f)
    }
}

fn shared_mutex_has_mapping(env: &WasiEnv, offset: u64) -> bool {
    env.state
        .shared_memory_mappings()
        .into_iter()
        .any(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add(16) <= mapping.offset.saturating_add(mapping.len)
        })
}

fn shared_mutex_key(env: &WasiEnv, offset: u64) -> u64 {
    if shared_mutex_has_mapping(env, offset) {
        offset
    } else {
        (1u64 << 63) | ((env.pid().raw() as u64) << 32) | (offset & 0xffff_ffff)
    }
}

fn shared_mutex_owner_token(env: &WasiEnv) -> u32 {
    let pid = env.pid().raw();
    let tid = env.tid().raw();
    let token = pid.wrapping_mul(65_537) ^ tid;
    let token = token & 0x7fff_ffff;
    if token == 0 { 1 } else { token }
}

fn copy_shared_mutex_to_guest<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
) -> Result<(), Errno> {
    let Some(mapping) = env
        .state
        .shared_memory_mappings()
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add(16) <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Ok(());
    };
    let index = usize::try_from(offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let bytes = mapping.bytes.lock().unwrap();
    if index.saturating_add(16) > bytes.len() {
        return Err(Errno::Inval);
    }
    let ptr = WasmPtr::<u8, M>::new(to_offset::<M>(
        offset.try_into().map_err(|_| Errno::Overflow)?,
    )?);
    let len = to_offset::<M>(16)?;
    ptr.slice(memory, len)
        .map_err(mem_error_to_wasi)?
        .write_slice(&bytes[index..index + 16])
        .map_err(mem_error_to_wasi)?;
    Ok(())
}

fn with_shared_cond_sequence_word<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    cond_offset: u64,
    seq: u32,
) -> Result<(), Errno> {
    let seq_offset = cond_offset.saturating_add(8);
    if let Some(mapping) = env
        .state
        .shared_memory_mappings()
        .into_iter()
        .find(|mapping| {
            seq_offset >= mapping.offset
                && seq_offset.saturating_add(4) <= mapping.offset.saturating_add(mapping.len)
        })
    {
        let index = usize::try_from(seq_offset - mapping.offset).map_err(|_| Errno::Overflow)?;
        let mut bytes = mapping.bytes.lock().unwrap();
        if index.saturating_add(4) > bytes.len() {
            return Err(Errno::Inval);
        }
        bytes[index..index + 4].copy_from_slice(&seq.to_le_bytes());
    }

    let ptr = WasmPtr::<u32, M>::new(to_offset::<M>(
        seq_offset.try_into().map_err(|_| Errno::Overflow)?,
    )?);
    ptr.write(memory, seq).map_err(mem_error_to_wasi)?;
    Ok(())
}

fn shared_cond_sequence_word(env: &WasiEnv, cond_offset: u64) -> Result<Option<u32>, Errno> {
    let seq_offset = cond_offset.saturating_add(8);
    let Some(mapping) = env
        .state
        .shared_memory_mappings()
        .into_iter()
        .find(|mapping| {
            seq_offset >= mapping.offset
                && seq_offset.saturating_add(4) <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Ok(None);
    };
    let index = usize::try_from(seq_offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let bytes = mapping.bytes.lock().unwrap();
    if index.saturating_add(4) > bytes.len() {
        return Err(Errno::Inval);
    }
    Ok(Some(u32::from_le_bytes([
        bytes[index],
        bytes[index + 1],
        bytes[index + 2],
        bytes[index + 3],
    ])))
}

fn with_shared_u32_words<T>(
    env: &WasiEnv,
    offset: u64,
    count: usize,
    f: impl FnOnce(&[u32]) -> T,
) -> Result<T, Errno> {
    let Some(mapping) = env
        .state
        .shared_memory_mappings()
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add((count * 4) as u64)
                    <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Err(Errno::Inval);
    };
    let index = usize::try_from(offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let bytes = mapping.bytes.lock().unwrap();
    if index.saturating_add(count * 4) > bytes.len() {
        return Err(Errno::Inval);
    }
    let mut words = vec![0u32; count];
    for (word_index, word) in words.iter_mut().enumerate() {
        let start = index + word_index * 4;
        *word = u32::from_le_bytes([
            bytes[start],
            bytes[start + 1],
            bytes[start + 2],
            bytes[start + 3],
        ]);
    }
    Ok(f(&words))
}

fn with_shared_u32_words_mut<T>(
    env: &WasiEnv,
    offset: u64,
    count: usize,
    f: impl FnOnce(&mut [u32]) -> T,
) -> Result<T, Errno> {
    let Some(mapping) = env
        .state
        .shared_memory_mappings()
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add((count * 4) as u64)
                    <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Err(Errno::Inval);
    };
    let index = usize::try_from(offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let mut bytes = mapping.bytes.lock().unwrap();
    if index.saturating_add(count * 4) > bytes.len() {
        return Err(Errno::Inval);
    }
    let mut words = vec![0u32; count];
    for (word_index, word) in words.iter_mut().enumerate() {
        let start = index + word_index * 4;
        *word = u32::from_le_bytes([
            bytes[start],
            bytes[start + 1],
            bytes[start + 2],
            bytes[start + 3],
        ]);
    }
    let result = f(&mut words);
    for (word_index, word) in words.iter().enumerate() {
        let start = index + word_index * 4;
        bytes[start..start + 4].copy_from_slice(&word.to_le_bytes());
    }
    Ok(result)
}

fn sync_shared_memory_range_to_guest<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
    len: u64,
) -> Result<(), Errno> {
    if !env.state.has_shared_memory_mappings() || len == 0 {
        return Ok(());
    }
    let Some(mapping) = env
        .state
        .shared_memory_mappings()
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add(len) <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Ok(());
    };
    let index = usize::try_from(offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let len = usize::try_from(len).map_err(|_| Errno::Overflow)?;
    let bytes = mapping.bytes.lock().unwrap();
    if index.saturating_add(len) > bytes.len() {
        return Err(Errno::Inval);
    }
    let ptr = WasmPtr::<u8, M>::new(to_offset::<M>(
        offset.try_into().map_err(|_| Errno::Overflow)?,
    )?);
    ptr.slice(memory, to_offset::<M>(len)?)
        .map_err(mem_error_to_wasi)?
        .write_slice(&bytes[index..index + len])
        .map_err(mem_error_to_wasi)?;
    Ok(())
}

fn sync_shared_memory_from_guest_preserving_range<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    preserve_offset: u64,
    preserve_len: usize,
) -> Result<(), Errno> {
    sync_shared_memory_from_guest_preserving_ranges::<M>(
        env,
        memory,
        &[(preserve_offset, preserve_len)],
    )
}

fn sync_shared_memory_from_guest_preserving_ranges<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    preserve_ranges: &[(u64, usize)],
) -> Result<(), Errno> {
    crate::syscalls::sync_shared_memory_from_guest_preserving_ranges::<M>(
        env,
        memory,
        preserve_ranges,
    )
}

fn sync_shared_memory_to_guest_preserving_ranges<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    preserve_ranges: &[(u64, usize)],
) -> Result<(), Errno> {
    if !env.state.has_shared_memory_mappings() {
        return Ok(());
    }
    for mapping in env.state.shared_memory_mappings() {
        let len = usize::try_from(mapping.len).map_err(|_| Errno::Overflow)?;
        let ptr = WasmPtr::<u8, M>::new(to_offset::<M>(
            mapping.offset.try_into().map_err(|_| Errno::Overflow)?,
        )?);
        let slice = ptr
            .slice(memory, to_offset::<M>(len)?)
            .map_err(mem_error_to_wasi)?;
        let bytes = mapping.bytes.lock().unwrap().clone();
        if bytes.len() != len {
            return Err(Errno::Inval);
        }

        let mapping_end = mapping.offset.saturating_add(mapping.len);
        let mut excluded = Vec::new();
        for (preserve_offset, preserve_len) in preserve_ranges {
            if *preserve_len == 0 {
                continue;
            }
            let preserve_end = preserve_offset.saturating_add(*preserve_len as u64);
            let start = (*preserve_offset).max(mapping.offset);
            let end = preserve_end.min(mapping_end);
            if start < end {
                excluded.push((
                    usize::try_from(start - mapping.offset).map_err(|_| Errno::Overflow)?,
                    usize::try_from(end - mapping.offset).map_err(|_| Errno::Overflow)?,
                ));
            }
        }
        excluded.sort_unstable_by_key(|(start, _)| *start);

        let mut merged: Vec<(usize, usize)> = Vec::new();
        for (start, end) in excluded {
            if let Some((_, last_end)) = merged.last_mut()
                && start <= *last_end
            {
                *last_end = (*last_end).max(end);
                continue;
            }
            merged.push((start, end));
        }

        let mut cursor = 0usize;
        for (start, end) in merged {
            if cursor < start {
                slice
                    .subslice(cursor as u64..start as u64)
                    .write_slice(&bytes[cursor..start])
                    .map_err(mem_error_to_wasi)?;
            }
            cursor = cursor.max(end);
        }
        if cursor < len {
            slice
                .subslice(cursor as u64..len as u64)
                .write_slice(&bytes[cursor..])
                .map_err(mem_error_to_wasi)?;
        }
    }
    Ok(())
}

fn import_guest_mutex_if_shared_unowned<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
    _current_tid: u32,
) -> Result<(), Errno> {
    if env
        .state
        .shared_mutex_owners
        .lock()
        .unwrap()
        .contains_key(&offset)
    {
        return Ok(());
    }

    let Some(mapping) = env
        .state
        .shared_memory_mappings()
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add(16) <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Ok(());
    };
    let mut guest_words = [0u32; 4];
    let ptr = WasmPtr::<u32, M>::new(to_offset::<M>(
        offset.try_into().map_err(|_| Errno::Overflow)?,
    )?);
    let slice = ptr
        .slice(memory, M::Offset::from(4u32))
        .map_err(mem_error_to_wasi)?;
    for (index, cell) in slice.iter().enumerate() {
        guest_words[index] = cell.read().map_err(mem_error_to_wasi)?;
    }
    let index = usize::try_from(offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let mut bytes = mapping.bytes.lock().unwrap();
    if index.saturating_add(16) > bytes.len() {
        return Err(Errno::Inval);
    }
    let shared_owner = u32::from_le_bytes([
        bytes[index + 4],
        bytes[index + 5],
        bytes[index + 6],
        bytes[index + 7],
    ]) & 0x7fffffff;
    if shared_owner != 0 {
        return Ok(());
    }
    env.state
        .shared_mutex_owners
        .lock()
        .unwrap()
        .remove(&offset);
    env.state
        .shared_mutex_owner_pids
        .lock()
        .unwrap()
        .remove(&offset);
    env.state
        .shared_mutex_depths
        .lock()
        .unwrap()
        .remove(&offset);
    let guest_owner = guest_words[1] & 0x7fffffff;
    if guest_owner != 0 {
        guest_words[1] = 0;
        guest_words[3] = 0;
    }
    for (word_index, word) in guest_words.iter().enumerate() {
        let start = index + word_index * 4;
        bytes[start..start + 4].copy_from_slice(&word.to_le_bytes());
    }
    Ok(())
}

fn with_shared_mutex_words(
    env: &WasiEnv,
    offset: u64,
    f: impl FnOnce(&mut [u32; 4]) -> Errno,
) -> Errno {
    let Some(mapping) = env
        .state
        .shared_memory_mappings()
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add(16) <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Errno::Inval;
    };
    let index = match usize::try_from(offset - mapping.offset) {
        Ok(index) => index,
        Err(_) => return Errno::Overflow,
    };
    let mut bytes = mapping.bytes.lock().unwrap();
    if index.saturating_add(16) > bytes.len() {
        return Errno::Inval;
    }
    let mut words = [0u32; 4];
    for (word_index, word) in words.iter_mut().enumerate() {
        let start = index + word_index * 4;
        *word = u32::from_le_bytes([
            bytes[start],
            bytes[start + 1],
            bytes[start + 2],
            bytes[start + 3],
        ]);
    }
    let result = f(&mut words);
    if mutex_result_preserves_words(result) {
        for (word_index, word) in words.iter().enumerate() {
            let start = index + word_index * 4;
            bytes[start..start + 4].copy_from_slice(&word.to_le_bytes());
        }
    }
    result
}

fn shared_mutex_owner(env: &WasiEnv, offset: u64) -> Result<Option<u32>, Errno> {
    let Some(mapping) = env
        .state
        .shared_memory_mappings()
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add(16) <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Ok(None);
    };
    let index = usize::try_from(offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let bytes = mapping.bytes.lock().unwrap();
    if index.saturating_add(8) > bytes.len() {
        return Err(Errno::Inval);
    }
    Ok(Some(
        u32::from_le_bytes([
            bytes[index + 4],
            bytes[index + 5],
            bytes[index + 6],
            bytes[index + 7],
        ]) & 0x7fffffff,
    ))
}

/// ### `clone_thread()`
/// POSIX/Linux-shaped same-process clone for `CLONE_THREAD | CLONE_VM`
/// callers. This uses the runtime's asyncify rewind path: the parent receives
/// the new TID and the cloned thread resumes from the syscall with return
/// value zero.
#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn clone_thread<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    child_stack: u64,
    parent_tid_ptr: WasmPtr<i32, M>,
    child_tid_ptr: WasmPtr<i32, M>,
    tls: u64,
    set_parent_tid: Bool,
    set_child_tid: Bool,
    clear_child_tid: Bool,
) -> Result<i32, WasiError> {
    clone_thread_common::<M>(
        ctx,
        child_stack,
        parent_tid_ptr,
        child_tid_ptr,
        tls,
        set_parent_tid == Bool::True,
        set_child_tid == Bool::True,
        clear_child_tid == Bool::True,
    )
}

fn fork_outcome_to_posix(outcome: ForkOutcome) -> i32 {
    if outcome.ret == Errno::Success {
        outcome.pid as i32
    } else {
        -outcome.ret.to_native()
    }
}

fn clone_thread_result_to_posix(result: CloneThreadResult) -> i32 {
    if result.ret == Errno::Success {
        result.tid as i32
    } else {
        -result.ret.to_native()
    }
}

fn clone_thread_common<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    child_stack: u64,
    parent_tid_ptr: WasmPtr<i32, M>,
    child_tid_ptr: WasmPtr<i32, M>,
    tls: u64,
    set_parent_tid: bool,
    set_child_tid: bool,
    clear_child_tid: bool,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if let Some(context_switching_environment) = ctx.data().context_switching_environment.as_ref()
        && context_switching_environment.active_context_id()
            != context_switching_environment.main_context_id()
    {
        warn!(
            "thread cloning is only supported from the main context when using WASIX context-switching features"
        );
        return Ok(-Errno::Notsup.to_native());
    }

    if let Some(result) = unsafe { handle_rewind::<M, CloneThreadResult>(&mut ctx) } {
        return Ok(clone_thread_result_to_posix(result));
    }

    if !supports_asyncified_fork(ctx.data()) {
        warn!("thread cloning requires asyncify unwind and rewind exports");
        return Ok(-Errno::Noexec.to_native());
    }

    let current_layout = ctx.data().thread.memory_layout().clone();
    let stack_upper = if child_stack == 0 {
        current_layout.stack_upper
    } else {
        child_stack
    };
    let stack_size = current_layout.stack_size;
    let stack_lower = match stack_upper.checked_sub(stack_size) {
        Some(value) => value,
        None => return Ok(-Errno::Inval.to_native()),
    };
    let layout = WasiMemoryLayout {
        stack_upper,
        stack_lower,
        guard_size: current_layout.guard_size,
        stack_size,
        tls_base: if tls == 0 {
            current_layout.tls_base
        } else {
            Some(tls)
        },
    };

    let thread_start = ThreadStartType::ThreadSpawn { start_ptr: 0 };
    let thread_handle = match ctx.data().process.new_thread(layout.clone(), thread_start) {
        Ok(handle) => handle,
        Err(err) => {
            warn!(?err, "failed to create cloned thread");
            return Ok(-Errno::Again.to_native());
        }
    };
    let tid: Tid = thread_handle.id().into();
    if clear_child_tid {
        thread_handle.set_clear_child_tid(child_tid_ptr.offset().into());
    }
    let thread_id = thread_handle.id();

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    if set_parent_tid
        && !parent_tid_ptr.is_null()
        && let Err(err) = parent_tid_ptr.write(&memory, tid as i32)
    {
        env.process.remove_thread(&thread_id);
        return Ok(-crate::mem_error_to_wasi(err).to_native());
    }
    if set_child_tid
        && !child_tid_ptr.is_null()
        && let Err(err) = child_tid_ptr.write(&memory, tid as i32)
    {
        env.process.remove_thread(&thread_id);
        return Ok(-crate::mem_error_to_wasi(err).to_native());
    }

    let state = env.state.clone();
    let snapshot = capture_store_snapshot(&mut ctx.as_store_mut());
    unwind::<M, _>(ctx, move |mut ctx, memory_stack, rewind_stack| {
        let env_inner = ctx.data().inner();
        let instance_handles = env_inner.main_module_instance_handles();
        let module = instance_handles.module_clone();
        let dynamic_linker = env_inner.linker().cloned();
        let mut thread_env = ctx.data().clone();
        thread_env.state = state;
        thread_env.thread = thread_handle.as_thread();
        thread_env.layout = layout;
        thread_env.enable_deep_sleep = false;
        let child_handle = thread_handle;
        let tasks = thread_env.tasks().clone();
        let spawn_type = if let Some(linker) = dynamic_linker {
            let func_env = ctx.as_ref();
            let store = ctx.as_store_mut();
            SpawnType::NewLinkerInstanceGroup(linker, func_env, store, false)
        } else {
            let memory = instance_handles.memory_clone();
            SpawnType::ShareMemory(memory, ctx.as_store_ref())
        };

        let memory_stack = memory_stack.freeze();
        let rewind_stack = rewind_stack.freeze();
        let store_data = match snapshot.serialize() {
            Ok(store_data) => Bytes::from(store_data),
            Err(err) => {
                warn!("failed to serialize clone-thread store snapshot - {}", err);
                ctx.data().process.remove_thread(&thread_id);
                return OnCalledAction::Trap(Box::new(WasiError::Exit(Errno::Perm.into())));
            }
        };

        let child_memory_stack = memory_stack.clone();
        let child_rewind_stack = rewind_stack.clone();
        let child_store_data = store_data.clone();
        let spawn_result = tasks.task_wasm(
            TaskWasm::new(
                Box::new(move |props: TaskWasmRunProperties| {
                    run_clone_thread::<M>(
                        props.ctx,
                        props.store,
                        child_handle,
                        child_memory_stack,
                        child_rewind_stack,
                        child_store_data.clone(),
                    );
                }),
                thread_env,
                module,
                false,
                false,
            )
            .with_globals(snapshot)
            .with_memory(spawn_type),
        );

        if let Err(err) = spawn_result {
            warn!("failed to spawn cloned thread - {}", err);
            ctx.data().process.remove_thread(&thread_id);
            match rewind::<M, _>(
                ctx,
                Some(memory_stack),
                rewind_stack,
                store_data,
                CloneThreadResult {
                    tid: 0,
                    ret: Errno::Again,
                },
            ) {
                Errno::Success => OnCalledAction::InvokeAgain,
                err => OnCalledAction::Trap(Box::new(WasiError::Exit(err.into()))),
            }
        } else {
            match rewind::<M, _>(
                ctx,
                Some(memory_stack),
                rewind_stack,
                store_data,
                CloneThreadResult {
                    tid,
                    ret: Errno::Success,
                },
            ) {
                Errno::Success => OnCalledAction::InvokeAgain,
                err => OnCalledAction::Trap(Box::new(WasiError::Exit(err.into()))),
            }
        }
    })
    .map(|err| -err.to_native())
}

fn clone_thread_rewind_result<M: MemorySize>(result: CloneThreadResult) -> RewindResultType {
    RewindResultType::RewindWithResult(
        bincode::serde::encode_to_vec(&result, config::legacy())
            .unwrap()
            .into(),
    )
}

fn run_clone_thread<M: MemorySize>(
    ctx: WasiFunctionEnv,
    store: Store,
    child_handle: WasiThreadHandle,
    memory_stack: Bytes,
    rewind_stack: Bytes,
    store_data: Bytes,
) {
    let layout = ctx.data(&store).layout.clone();
    let rewind_state = RewindState {
        memory_stack,
        rewind_stack,
        store_data,
        start: ThreadStartType::ThreadSpawn { start_ptr: 0 },
        layout,
        is_64bit: M::is_64bit(),
    };
    run_clone_thread_from_rewind::<M>(
        ctx,
        store,
        child_handle,
        Some((
            rewind_state,
            clone_thread_rewind_result::<M>(CloneThreadResult {
                tid: 0,
                ret: Errno::Success,
            }),
        )),
    );
}

fn run_clone_thread_from_rewind<M: MemorySize>(
    ctx: WasiFunctionEnv,
    mut store: Store,
    child_handle: WasiThreadHandle,
    rewind_state: Option<(RewindState, RewindResultType)>,
) {
    let env = ctx.data(&store);
    let tasks = env.tasks().clone();
    let pid = env.pid();
    let tid = env.tid();

    if let Some((rewind_state, rewind_result)) = rewind_state {
        let mut ctx_mut = ctx.env.clone().into_mut(&mut store);
        let res = rewind_ext::<M>(
            &mut ctx_mut,
            Some(rewind_state.memory_stack),
            rewind_state.rewind_stack,
            rewind_state.store_data,
            rewind_result,
        );
        if res != Errno::Success {
            child_handle.set_status_finished(Ok(res.into()));
            return;
        }
    }

    trace!(%pid, %tid, "re-invoking clone thread continuation");
    let env_inner = ctx.data(&store).inner();
    let module_handles = env_inner.main_module_instance_handles();
    let Some(start) = module_handles.start.clone() else {
        warn!(%pid, %tid, "cloned thread is missing _start export");
        child_handle.set_status_finished(Ok(Errno::Noexec.into()));
        return;
    };

    let mut ret: ExitCode = Errno::Success.into();
    let (mut store, err) =
        ContextSwitchingEnvironment::run_main_context(&ctx, store, start.into(), vec![]);
    if let Err(err) = err {
        match err.downcast::<WasiError>() {
            Ok(WasiError::Exit(exit_code)) => {
                ret = exit_code;
            }
            Ok(WasiError::ThreadExit) => {
                ret = Errno::Success.into();
            }
            Ok(WasiError::DeepSleep(deep)) => {
                trace!(%pid, %tid, "cloned thread entered a deep sleep");
                let respawn = {
                    let tasks = tasks.clone();
                    let rewind_state = deep.rewind;
                    move |ctx, store, rewind_result| {
                        run_clone_thread_from_rewind::<M>(
                            ctx,
                            store,
                            child_handle,
                            Some((
                                rewind_state,
                                RewindResultType::RewindWithResult(rewind_result),
                            )),
                        );
                    }
                };
                unsafe {
                    tasks.resume_wasm_after_poller(Box::new(respawn), ctx, store, deep.trigger)
                };
                return;
            }
            _ => {
                ret = Errno::Unknown.into();
            }
        }
    }

    trace!(%pid, %tid, "cloned thread exited (code = {})", ret);
    ctx.on_exit(&mut store, Some(ret));
    child_handle.set_status_finished(Ok(ret));
    drop(child_handle);
}

fn supports_asyncified_fork(env: &WasiEnv) -> bool {
    let handles = env.inner().main_module_instance_handles();
    handles.asyncify_start_unwind.is_some()
        && handles.asyncify_stop_unwind.is_some()
        && handles.asyncify_start_rewind.is_some()
        && handles.asyncify_stop_rewind.is_some()
}

fn proc_fork_common<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    copy_memory: Bool,
    pid_ptr: Option<WasmPtr<Pid, M>>,
) -> Result<ForkOutcome, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if let Some(context_switching_environment) = ctx.data().context_switching_environment.as_ref()
        && context_switching_environment.active_context_id()
            != context_switching_environment.main_context_id()
    {
        warn!(
            "process forking is only supported from the main context when using WASIX context-switching features"
        );
        return Ok(ForkOutcome {
            pid: 0,
            ret: Errno::Notsup,
        });
    }

    // If we were just restored then we need to return the value instead
    if let Some(result) = unsafe { handle_rewind::<M, ForkResult>(&mut ctx) } {
        if result.pid == 0 {
            trace!("handle_rewind - i am child (ret={})", result.ret);
        } else {
            trace!(
                "handle_rewind - i am parent (child={}, ret={})",
                result.pid, result.ret
            );
            if ctx.data().state.has_shared_memory_mappings() {
                sinomo_debug!(
                    "sinomo-debug proc_fork parent-rewind pid={} child={} ret={:?}",
                    ctx.data().pid().raw(),
                    result.pid,
                    result.ret
                );
            }
        }
        if let Some(pid_ptr) = pid_ptr {
            let memory = unsafe { ctx.data().memory_view(&ctx) };
            if let Err(err) = pid_ptr.write(&memory, result.pid) {
                return Ok(ForkOutcome {
                    pid: 0,
                    ret: crate::mem_error_to_wasi(err),
                });
            }
        }
        return Ok(ForkOutcome {
            pid: result.pid,
            ret: result.ret,
        });
    }
    trace!(%copy_memory, "capturing");

    if let Some(vfork) = ctx.data().vfork.as_ref() {
        warn!("process forking not supported in an active vfork");
        return Ok(ForkOutcome {
            pid: 0,
            ret: Errno::Notsup,
        });
    }
    if ctx.data().process.child_process_limit_reached() {
        sinomo_debug!(
            "sinomo-debug proc_fork child-process-limit pid={}",
            ctx.data().pid().raw()
        );
        return Ok(ForkOutcome {
            pid: 0,
            ret: Errno::Again,
        });
    }
    if ctx.data().process.task_limit_reached() {
        sinomo_debug!(
            "sinomo-debug proc_fork task-limit pid={}",
            ctx.data().pid().raw()
        );
        return Ok(ForkOutcome {
            pid: 0,
            ret: Errno::Again,
        });
    }

    if !supports_asyncified_fork(ctx.data()) {
        warn!("process forking requires asyncify unwind and rewind exports");
        return Ok(ForkOutcome {
            pid: 0,
            ret: Errno::Noexec,
        });
    }
    let shared_memory_was_cross_process = ctx.data().state.has_cross_process_shared_memory();
    if ctx.data().state.has_shared_memory_mappings() {
        ctx.data().state.mark_cross_process_shared_memory();
    }
    let parent_had_unwaited_children = {
        let inner = ctx.data().process.lock();
        !inner.children.is_empty()
    };

    // Fork the environment which will copy all the open file handlers
    // and associate a new context but otherwise shares things like the
    // file system interface. The handle to the forked process is stored
    // in the parent process context
    let (mut child_env, mut child_handle) = match ctx.data().fork() {
        Ok(p) => p,
        Err(err) => {
            debug!("could not fork process: {err}");
            // TODO: evaluate the appropriate error code, document it in the spec.
            return Ok(ForkOutcome {
                pid: 0,
                ret: Errno::Perm,
            });
        }
    };
    let child_pid = child_env.process.pid();
    let child_finished = child_env.process.finished.clone();

    // We write a zero to the PID before we capture the stack
    // so that this is what will be returned to the child
    {
        let mut inner = ctx.data().process.lock();
        inner.children.push(child_env.process.clone());
    }
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    if env.state.has_shared_memory_mappings() {
        let mappings = env.state.shared_memory_mappings();
        let total_len = mappings.iter().map(|mapping| mapping.len).sum::<u64>();
        let direction = if shared_memory_was_cross_process && !parent_had_unwaited_children {
            "from-guest-preserving-futexes"
        } else if shared_memory_was_cross_process {
            "unchanged-unwaited-children"
        } else {
            "from-guest"
        };
        sinomo_debug!(
            "sinomo-debug proc_fork shared-sync-{} pid={} mappings={} total_len={}",
            direction,
            env.pid().raw(),
            mappings.len(),
            total_len
        );
    }
    if shared_memory_was_cross_process && !parent_had_unwaited_children {
        let preserve_ranges = env
            .state
            .shared_futex_ranges()
            .into_iter()
            .map(|range| (range.start, range.end.saturating_sub(range.start) as usize))
            .collect::<Vec<_>>();
        if let Err(err) =
            sync_shared_memory_from_guest_preserving_ranges::<M>(env, &memory, &preserve_ranges)
        {
            return Ok(ForkOutcome { pid: 0, ret: err });
        }
    } else if shared_memory_was_cross_process {
        // Existing children may already have published MAP_SHARED writes, even
        // if they exited before the parent reaps them. Do not copy the
        // parent's potentially stale shared bytes back before every subsequent
        // fork; synchronization objects pull the central mapping when the child
        // acquires them.
    } else if let Err(err) = sync_shared_memory_from_guest::<M>(env, &memory) {
        return Ok(ForkOutcome { pid: 0, ret: err });
    }
    if env.state.has_shared_memory_mappings() {
        let direction = if shared_memory_was_cross_process && !parent_had_unwaited_children {
            "from-guest-preserving-futexes"
        } else if shared_memory_was_cross_process {
            "unchanged-unwaited-children"
        } else {
            "from-guest"
        };
        sinomo_debug!(
            "sinomo-debug proc_fork shared-sync-{}-done pid={}",
            direction,
            env.pid().raw()
        );
    }

    // Setup some properties in the child environment
    if let Some(pid_ptr) = pid_ptr {
        if let Err(err) = pid_ptr.write(&memory, 0) {
            return Ok(ForkOutcome {
                pid: 0,
                ret: crate::mem_error_to_wasi(err),
            });
        }
    }
    let pid = child_env.pid();
    let tid = child_env.tid();

    // If we are not copying the memory then we act like a `vfork`
    // instead which will pretend to be the new process for a period
    // of time until `proc_exec` is called at which point the fork
    // actually occurs
    if copy_memory == Bool::False {
        // Perform the unwind action
        return unwind::<M, _>(ctx, move |mut ctx, mut memory_stack, rewind_stack| {
            // Grab all the globals and serialize them
            let store_data =
                match crate::utils::store::capture_store_snapshot(&mut ctx.as_store_mut())
                    .serialize()
                {
                    Ok(store_data) => store_data,
                    Err(err) => {
                        warn!("failed to serialize vfork store snapshot - {}", err);
                        return OnCalledAction::Trap(Box::new(WasiError::Exit(Errno::Perm.into())));
                    }
                };
            let store_data = Bytes::from(store_data);

            // We first fork the environment and replace the current environment
            // so that the process can continue to prepare for the real fork as
            // if it had actually forked
            child_env.swap_inner(ctx.data_mut());
            std::mem::swap(ctx.data_mut(), &mut child_env);
            let previous_vfork = ctx.data_mut().vfork.replace(WasiVFork {
                asyncify: Some(WasiVForkAsyncify {
                    rewind_stack: rewind_stack.clone(),
                    store_data: store_data.clone(),
                    is_64bit: M::is_64bit(),
                }),
                env: Box::new(child_env),
                handle: child_handle,
            });
            assert!(previous_vfork.is_none()); // Already checked above

            // Carry on as if the fork had taken place (which basically means
            // it prevents to be the new process with the old one suspended)
            // Rewind the stack and carry on
            match rewind::<M, _>(
                ctx,
                Some(memory_stack.freeze()),
                rewind_stack.freeze(),
                store_data,
                ForkResult {
                    pid: 0,
                    ret: Errno::Success,
                },
            ) {
                Errno::Success => OnCalledAction::InvokeAgain,
                err => {
                    warn!("failed - could not rewind the stack - errno={}", err);
                    OnCalledAction::Trap(Box::new(WasiError::Exit(err.into())))
                }
            }
        })
        .map(|ret| ForkOutcome { pid: 0, ret });
    }

    // Create the thread that will back this forked process
    let state = env.state.clone();
    let bin_factory = env.bin_factory.clone();

    // Perform the unwind action
    let snapshot = capture_store_snapshot(&mut ctx.as_store_mut());
    unwind::<M, _>(ctx, move |mut ctx, mut memory_stack, rewind_stack| {
        let tasks = ctx.data().tasks().clone();
        let span = debug_span!(
            "unwind",
            memory_stack_len = memory_stack.len(),
            rewind_stack_len = rewind_stack.len()
        );
        let _span_guard = span.enter();
        let memory_stack = memory_stack.freeze();
        let rewind_stack = rewind_stack.freeze();

        // Grab all the globals and serialize them
        let store_data = match snapshot.serialize() {
            Ok(store_data) => store_data,
            Err(err) => {
                warn!("failed to serialize fork store snapshot - {}", err);
                match rewind::<M, _>(
                    ctx,
                    Some(memory_stack),
                    rewind_stack,
                    Bytes::new(),
                    ForkResult {
                        pid: 0,
                        ret: Errno::Perm,
                    },
                ) {
                    Errno::Success => return OnCalledAction::InvokeAgain,
                    err => {
                        warn!("failed - could not rewind the stack - errno={}", err);
                        return OnCalledAction::Trap(Box::new(WasiError::Exit(err.into())));
                    }
                }
            }
        };
        let store_data = Bytes::from(store_data);

        // Now we use the environment and memory references
        let runtime = child_env.runtime.clone();
        let tasks = child_env.tasks().clone();
        let child_memory_stack = memory_stack.clone();
        let child_rewind_stack = rewind_stack.clone();

        let env_inner = ctx.data().inner();
        let instance_handles = env_inner.main_module_instance_handles();
        let module = instance_handles.module_clone();
        let dynamic_linker = env_inner.linker().cloned();
        let spawn_type = if let Some(linker) = dynamic_linker {
            let func_env = ctx.as_ref();
            let store = ctx.as_store_mut();
            SpawnType::NewLinkerInstanceGroup(linker, func_env, store, true)
        } else {
            let memory = instance_handles.memory_clone();
            SpawnType::CopyMemory(memory, ctx.as_store_ref())
        };

        // Spawn a new process with this current execution environment
        let signaler = Box::new(child_env.process.clone());
        {
            let runtime = runtime.clone();
            let tasks = tasks.clone();
            let tasks_outer = tasks.clone();
            let child_store_data = store_data.clone();
            let child_state = state.clone();

            let run = move |mut props: TaskWasmRunProperties| {
                let ctx = props.ctx;
                let mut store = props.store;

                // Rewind the stack and carry on
                {
                    if child_state.has_shared_memory_mappings() {
                        sinomo_debug!(
                            "sinomo-debug proc_fork child-task-start pid={}",
                            child_pid.raw()
                        );
                    }
                    trace!("rewinding child");
                    let mut ctx = ctx.env.clone().into_mut(&mut store);
                    let (_data, mut store) = ctx.data_and_store_mut();
                    match rewind::<M, _>(
                        ctx,
                        Some(child_memory_stack),
                        child_rewind_stack,
                        child_store_data.clone(),
                        ForkResult {
                            pid: 0,
                            ret: Errno::Success,
                        },
                    ) {
                        Errno::Success => OnCalledAction::InvokeAgain,
                        err => {
                            warn!(
                                "wasm rewind failed - could not rewind the stack - errno={}",
                                err
                            );
                            return;
                        }
                    };
                    if child_state.has_shared_memory_mappings() {
                        sinomo_debug!(
                            "sinomo-debug proc_fork child-rewind-done pid={}",
                            child_pid.raw()
                        );
                    }
                }

                // Invoke the start function
                run::<M>(ctx, store, child_handle, None);
            };

            let spawn_result = tasks_outer
                .task_wasm(
                    TaskWasm::new(Box::new(run), child_env, module, false, false)
                        .with_globals(snapshot)
                        .with_memory(spawn_type),
                )
                .map_err(|err| {
                    warn!(
                        "failed to fork as the process could not be spawned - {}",
                        err
                    );
                    err
                });
            if spawn_result.is_err() {
                {
                    let mut inner = ctx.data().process.lock();
                    inner.children.retain(|child| child.pid != child_pid);
                }
                ctx.data().control_plane.remove_process(child_pid);
                match rewind::<M, _>(
                    ctx,
                    Some(memory_stack),
                    rewind_stack,
                    store_data,
                    ForkResult {
                        pid: 0,
                        ret: Errno::Perm,
                    },
                ) {
                    Errno::Success => OnCalledAction::InvokeAgain,
                    err => {
                        warn!("failed - could not rewind the stack - errno={}", err);
                        OnCalledAction::Trap(Box::new(WasiError::Exit(err.into())))
                    }
                }
            } else {
                if state.has_shared_memory_mappings() {
                    sinomo_debug!(
                        "sinomo-debug proc_fork child-spawned parent_pid={} child_pid={}",
                        ctx.data().pid().raw(),
                        child_pid.raw()
                    );
                }
                match rewind::<M, _>(
                    ctx,
                    Some(memory_stack),
                    rewind_stack,
                    store_data,
                    ForkResult {
                        pid: child_pid.raw() as Pid,
                        ret: Errno::Success,
                    },
                ) {
                    Errno::Success => OnCalledAction::InvokeAgain,
                    err => {
                        warn!("failed - could not rewind the stack - errno={}", err);
                        OnCalledAction::Trap(Box::new(WasiError::Exit(err.into())))
                    }
                }
            }
        }
    })
    .map(|ret| ForkOutcome { pid: 0, ret })
}

fn run<M: MemorySize>(
    ctx: WasiFunctionEnv,
    mut store: Store,
    child_handle: WasiThreadHandle,
    rewind_state: Option<(RewindState, RewindResultType)>,
) -> ExitCode {
    let env = ctx.data(&store);
    let tasks = env.tasks().clone();
    let pid = env.pid();
    let tid = env.tid();

    // If we need to rewind then do so
    if let Some((rewind_state, rewind_result)) = rewind_state {
        let mut ctx = ctx.env.clone().into_mut(&mut store);
        let res = rewind_ext::<M>(
            &mut ctx,
            Some(rewind_state.memory_stack),
            rewind_state.rewind_stack,
            rewind_state.store_data,
            rewind_result,
        );
        if res != Errno::Success {
            return res.into();
        }
    }

    let mut ret: ExitCode = Errno::Success.into();
    let (mut store, err) = if ctx.data(&store).thread.is_main() {
        trace!(%pid, %tid, "re-invoking main");
        let env_inner = ctx.data(&store).inner();
        let module_handles = env_inner.main_module_instance_handles();
        let Some(start) = module_handles.start.clone() else {
            warn!(%pid, %tid, "forked process is missing _start export");
            return Errno::Noexec.into();
        };
        ContextSwitchingEnvironment::run_main_context(&ctx, store, start.into(), vec![])
    } else {
        trace!(%pid, %tid, "re-invoking thread_spawn");
        let env_inner = ctx.data(&store).inner();
        let module_handles = env_inner.main_module_instance_handles();
        let Some(start) = module_handles.thread_spawn.clone() else {
            warn!(%pid, %tid, "forked thread is missing wasi_thread_start export");
            return Errno::Noexec.into();
        };
        let params = vec![0i32.into(), 0i32.into()];
        ContextSwitchingEnvironment::run_main_context(&ctx, store, start.into(), params)
    };
    if let Err(err) = err {
        match err.downcast::<WasiError>() {
            Ok(WasiError::Exit(exit_code)) => {
                ret = exit_code;
            }
            Ok(WasiError::DeepSleep(deep)) => {
                trace!(%pid, %tid, "entered a deep sleep");

                // Create the respawn function
                let respawn = {
                    let tasks = tasks.clone();
                    let rewind_state = deep.rewind;
                    move |ctx, store, rewind_result| {
                        run::<M>(
                            ctx,
                            store,
                            child_handle,
                            Some((
                                rewind_state,
                                RewindResultType::RewindWithResult(rewind_result),
                            )),
                        );
                    }
                };

                /// Spawns the WASM process after a trigger
                unsafe {
                    tasks.resume_wasm_after_poller(Box::new(respawn), ctx, store, deep.trigger)
                };
                return Errno::Success.into();
            }
            _ => {}
        }
    }
    trace!(%pid, %tid, "child exited (code = {})", ret);

    // Clean up the environment and return the result
    ctx.on_exit((&mut store), Some(ret));

    // We drop the handle at the last moment which will close the thread
    drop(child_handle);
    ret
}
