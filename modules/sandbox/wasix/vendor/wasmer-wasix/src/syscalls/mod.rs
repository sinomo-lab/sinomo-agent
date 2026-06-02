#![allow(
    unused,
    clippy::too_many_arguments,
    clippy::cognitive_complexity,
    clippy::result_large_err
)]

pub mod types {
    pub use wasmer_wasix_types::{types::*, wasi};
}

#[cfg(any(
    target_os = "freebsd",
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple"
))]
pub mod unix;
#[cfg(target_family = "wasm")]
pub mod wasm;
#[cfg(target_os = "windows")]
pub mod windows;

pub mod journal;
pub mod wasi;
pub mod wasix;

use bincode::config;
use bytes::{Buf, BufMut};
use futures::{
    Future,
    future::{BoxFuture, LocalBoxFuture},
};
use tracing::instrument;
use virtual_mio::block_on;
pub use wasi::*;
pub use wasix::*;
use wasmer_journal::SnapshotTrigger;
use wasmer_wasix_types::wasix::ThreadStartType;

pub mod legacy;

pub(crate) use std::{
    borrow::{Borrow, Cow},
    cell::RefCell,
    collections::{HashMap, HashSet, hash_map::Entry},
    convert::{Infallible, TryInto},
    io::{self, Read, Seek, Write},
    mem::transmute,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    num::NonZeroU64,
    ops::{Deref, DerefMut},
    path::Path,
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering, fence},
        mpsc,
    },
    task::{Context, Poll},
    thread::LocalKey,
    time::Duration,
};
use std::{io::IoSlice, marker::PhantomData, mem::MaybeUninit, task::Waker, time::Instant};

pub(crate) use bytes::{Bytes, BytesMut};
pub(crate) use cooked_waker::IntoWaker;
pub use journal::*;
pub(crate) use sha2::Sha256;
pub(crate) use tracing::{debug, error, trace, warn};
#[cfg(any(
    target_os = "freebsd",
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple"
))]
pub use unix::*;
#[cfg(target_family = "wasm")]
pub use wasm::*;

pub(crate) use virtual_fs::{
    AsyncReadExt, AsyncSeekExt, AsyncWriteExt, DuplexPipe, FileSystem, FsError, VirtualFile,
};
pub(crate) use virtual_net::StreamSecurity;
pub(crate) use wasmer::{
    AsStoreMut, AsStoreRef, Extern, Function, FunctionEnv, FunctionEnvMut, Global, Instance,
    Memory, Memory32, Memory64, MemoryAccessError, MemoryError, MemorySize, MemoryView, Module,
    OnCalledAction, Pages, RuntimeError, Store, TypedFunction, Value, WasmPtr, WasmSlice,
};
pub(crate) use wasmer_wasix_types::{asyncify::__wasi_asyncify_t, wasi::EventUnion};
#[cfg(target_os = "windows")]
pub use windows::*;

pub(crate) use self::types::{
    wasi::{
        Addressfamily, Advice, Clockid, Dircookie, Dirent, Disposition, DlFlags, DlHandle, Errno,
        Event, EventFdReadwrite, Eventrwflags, Eventtype, ExitCode, Fd as WasiFd, Fdflags,
        Fdflagsext, Fdstat, Filesize, Filestat, Filetype, Fstflags, Linkcount, Longsize, OptionFd,
        Pid, Prestat, ProcSpawnFdOp, Rights, SignalDisposition, Snapshot0Clockid, Sockoption,
        Sockstatus, Socktype, StackSnapshot, StdioMode as WasiStdioMode, Streamsecurity,
        Subscription, SubscriptionFsReadwrite, Tid, Timestamp, TlKey, TlUser, TlVal, Tty, Whence,
    },
    *,
};
use self::{
    state::{WasiInstanceGuardMemory, conv_env_vars},
    utils::WasiDummyWaker,
};
pub(crate) use crate::os::task::{
    TaskStatus,
    process::{WasiProcessId, WasiProcessWait},
    thread::{WasiThread, WasiThreadId},
};
use crate::{
    DeepSleepWork, RewindPostProcess, RewindState, RewindStateOption, SpawnError, WasiInodes,
    WasiResult, WasiRuntimeError,
    fs::{
        Fd, FdInner, InodeVal, Kind, MAX_SYMLINKS, fs_error_into_wasi_err,
        virtual_file_type_to_wasi_file_type,
    },
    journal::{DynJournal, DynReadableJournal, DynWritableJournal, JournalEffector},
    os::task::{
        process::{MaybeCheckpointResult, WasiProcessCheckpoint},
        thread::{RewindResult, RewindResultType},
    },
    utils::store::StoreSnapshot,
};
pub(crate) use crate::{
    Runtime, VirtualTaskManager, WasiEnv, WasiError, WasiFunctionEnv, WasiModuleTreeHandles,
    WasiVFork,
    bin_factory::spawn_exec_module,
    import_object_for_all_wasi_versions, mem_error_to_wasi,
    net::{
        read_ip_port,
        socket::{InodeHttpSocketType, InodeSocket, InodeSocketKind},
        write_ip_port,
    },
    runtime::SpawnType,
    state::{
        self, InodeGuard, InodeWeakGuard, PollEvent, PollEventBuilder, WasiFutex, WasiState,
        iterate_poll_events,
    },
    utils::{self, map_io_err},
};
pub(crate) use crate::{net::net_error_into_wasi_err, utils::WasiParkingLot};

pub(crate) fn to_offset<M: MemorySize>(offset: usize) -> Result<M::Offset, Errno> {
    let ret: M::Offset = offset.try_into().map_err(|_| Errno::Inval)?;
    Ok(ret)
}

pub(crate) fn from_offset<M: MemorySize>(offset: M::Offset) -> Result<usize, Errno> {
    let ret: usize = offset.try_into().map_err(|_| Errno::Inval)?;
    Ok(ret)
}

pub(crate) fn sync_shared_memory_from_guest<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
) -> Result<(), Errno> {
    let preserve_ranges = if env.state.has_cross_process_shared_memory() {
        env.state
            .shared_futex_ranges()
            .into_iter()
            .map(|range| (range.start, range.end.saturating_sub(range.start) as usize))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    sync_shared_memory_from_guest_with_preserve_ranges::<M>(env, memory, &preserve_ranges)
}

pub(crate) fn sync_shared_memory_from_guest_including_futexes<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
) -> Result<(), Errno> {
    sync_shared_memory_from_guest_with_preserve_ranges::<M>(env, memory, &[])
}

pub(crate) fn sync_shared_memory_from_exiting_guest(
    env: &WasiEnv,
    memory: &MemoryView,
) -> Result<(), Errno> {
    if !env.state.has_shared_memory_mappings() {
        return Ok(());
    }
    fence(Ordering::SeqCst);
    let data = unsafe { memory.data_unchecked() };
    for mapping in env.state.shared_memory_mappings_for_pid(env.pid().raw()) {
        let len = usize::try_from(mapping.len).map_err(|_| Errno::Overflow)?;
        let start = usize::try_from(mapping.offset).map_err(|_| Errno::Overflow)?;
        let end = start.checked_add(len).ok_or(Errno::Overflow)?;
        let bytes = data.get(start..end).ok_or(Errno::Fault)?;
        mapping.write_bytes(bytes)?;
        mapping
            .process_snapshots
            .lock()
            .unwrap()
            .insert(env.pid().raw(), bytes.to_vec());
    }
    fence(Ordering::SeqCst);
    Ok(())
}

fn sync_shared_memory_from_guest_with_preserve_ranges<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    preserve_ranges: &[(u64, usize)],
) -> Result<(), Errno> {
    if !env.state.has_shared_memory_mappings() {
        return Ok(());
    }
    fence(Ordering::SeqCst);
    for mapping in env.state.shared_memory_mappings_for_pid(env.pid().raw()) {
        if is_direct_shared_mapping_active(env, memory, &mapping)? {
            continue;
        }
        let len = usize::try_from(mapping.len).map_err(|_| Errno::Overflow)?;
        let start = usize::try_from(mapping.offset).map_err(|_| Errno::Overflow)?;
        let end = start.checked_add(len).ok_or(Errno::Overflow)?;
        let data = unsafe { memory.data_unchecked() };
        let bytes = data.get(start..end).ok_or(Errno::Fault)?.to_vec();
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
        if std::env::var_os("SINOMO_WASIX_SHM_DEBUG").is_some() && !merged.is_empty() {
            eprintln!(
                "sinomo-debug shared_memory_publish_preserve pid={} mapping={:#x} ranges={:?}",
                env.pid().raw(),
                mapping.offset,
                merged
            );
        }
        mapping.publish_delta_from_guest(env.pid().raw(), &bytes, &merged)?;
    }
    Ok(())
}

fn is_direct_shared_mapping_active(
    env: &WasiEnv,
    memory: &MemoryView,
    mapping: &state::WasiSharedMemoryMapping,
) -> Result<bool, Errno> {
    let offset = usize::try_from(mapping.offset).map_err(|_| Errno::Overflow)?;
    if mapping.offset.saturating_add(mapping.len) > memory.data_size() {
        return Err(Errno::Fault);
    }
    let base = unsafe { memory.data_ptr().add(offset) as usize };
    Ok(mapping
        .mapped_bases
        .lock()
        .unwrap()
        .contains(&(env.pid().raw(), env.tid().raw(), base)))
}

pub(crate) fn sync_shared_memory_range_from_guest<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
    len: u64,
) -> Result<(), Errno> {
    if !env.state.has_shared_memory_mappings() {
        return Ok(());
    }
    if len == 0 {
        return Ok(());
    }
    let Some(mapping) = env
        .state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add(len) <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Ok(());
    };
    if is_direct_shared_mapping_active(env, memory, &mapping)? {
        return Ok(());
    }
    let index = usize::try_from(offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let len = usize::try_from(len).map_err(|_| Errno::Overflow)?;
    let ptr = WasmPtr::<u8, M>::new(to_offset::<M>(
        offset.try_into().map_err(|_| Errno::Overflow)?,
    )?);
    let bytes = ptr
        .slice(memory, to_offset::<M>(len)?)
        .map_err(mem_error_to_wasi)?
        .read_to_vec()
        .map_err(mem_error_to_wasi)?;
    if bytes.len() != len {
        return Err(Errno::Inval);
    }

    let mapping_len = usize::try_from(mapping.len).map_err(|_| Errno::Overflow)?;
    let mut shared_range = mapping.read_range(index, len)?;
    let mut snapshots = mapping.process_snapshots.lock().unwrap();
    let snapshot = snapshots.entry(env.pid().raw()).or_insert_with(|| {
        mapping
            .read_bytes()
            .unwrap_or_else(|_| vec![0; mapping_len])
    });
    if snapshot.len() != mapping_len {
        *snapshot = mapping
            .read_bytes()
            .unwrap_or_else(|_| vec![0; mapping_len]);
    }
    for (byte_index, guest_byte) in bytes.iter().enumerate() {
        let shared_index = index + byte_index;
        if *guest_byte != snapshot[shared_index] {
            shared_range[byte_index] = *guest_byte;
        }
        snapshot[shared_index] = *guest_byte;
    }
    mapping.write_range(index, &shared_range)?;
    Ok(())
}

pub(crate) fn sync_shared_memory_range_to_guest<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
    len: u64,
) -> Result<(), Errno> {
    if !env.state.has_shared_memory_mappings() {
        return Ok(());
    }
    if len == 0 {
        return Ok(());
    }
    let Some(mapping) = env
        .state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add(len) <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Ok(());
    };
    if is_direct_shared_mapping_active(env, memory, &mapping)? {
        return Ok(());
    }
    let index = usize::try_from(offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let len = usize::try_from(len).map_err(|_| Errno::Overflow)?;
    let bytes = mapping.read_range(index, len)?;
    if bytes.len() != len {
        return Err(Errno::Inval);
    }

    let ptr = WasmPtr::<u8, M>::new(to_offset::<M>(
        offset.try_into().map_err(|_| Errno::Overflow)?,
    )?);
    ptr.slice(memory, to_offset::<M>(len)?)
        .map_err(mem_error_to_wasi)?
        .write_slice(&bytes)
        .map_err(mem_error_to_wasi)?;

    let mapping_len = usize::try_from(mapping.len).map_err(|_| Errno::Overflow)?;
    let mut snapshots = mapping.process_snapshots.lock().unwrap();
    let snapshot = snapshots.entry(env.pid().raw()).or_insert_with(|| {
        mapping
            .read_bytes()
            .unwrap_or_else(|_| vec![0; mapping_len])
    });
    if snapshot.len() != mapping_len {
        *snapshot = mapping
            .read_bytes()
            .unwrap_or_else(|_| vec![0; mapping_len]);
    }
    snapshot[index..index + len].copy_from_slice(&bytes);
    fence(Ordering::SeqCst);
    Ok(())
}

pub(crate) fn sync_shared_memory_from_guest_preserving_ranges<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    preserve_ranges: &[(u64, usize)],
) -> Result<(), Errno> {
    if !env.state.has_shared_memory_mappings() {
        return Ok(());
    }
    for mapping in env.state.shared_memory_mappings_for_pid(env.pid().raw()) {
        if is_direct_shared_mapping_active(env, memory, &mapping)? {
            continue;
        }
        let len = usize::try_from(mapping.len).map_err(|_| Errno::Overflow)?;
        let start = usize::try_from(mapping.offset).map_err(|_| Errno::Overflow)?;
        let end = start.checked_add(len).ok_or(Errno::Overflow)?;
        let data = unsafe { memory.data_unchecked() };
        let guest_bytes = data.get(start..end).ok_or(Errno::Fault)?.to_vec();
        if guest_bytes.len() != len {
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
        mapping.publish_delta_from_guest(env.pid().raw(), &guest_bytes, &merged)?;
    }
    Ok(())
}

pub(crate) fn is_registered_shared_memory_offset(env: &WasiEnv, offset: u64) -> bool {
    env.state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .any(|mapping| {
            offset >= mapping.offset && offset < mapping.offset.saturating_add(mapping.len)
        })
}

pub(crate) fn is_registered_shared_futex_offset(env: &WasiEnv, offset: u64) -> bool {
    has_live_cross_process_shared_memory(env)
        && (is_registered_shared_memory_offset(env, offset)
            || env.state.is_shared_futex_offset(offset))
}

pub(crate) fn has_live_cross_process_shared_memory(env: &WasiEnv) -> bool {
    if !env.state.has_cross_process_shared_memory() {
        return false;
    }
    if env.process.pid().raw() != 1 && env.process.ppid().raw() != 0 {
        return true;
    }
    let pid = env.process.pid().raw();
    env.process.lock().children.iter().any(|child| {
        child.pid().raw() != pid
            && matches!(
                child.task_status(),
                TaskStatus::Pending | TaskStatus::Running | TaskStatus::Stopped(_)
            )
    })
}

pub(crate) fn sync_shared_memory_to_guest<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
) -> Result<(), Errno> {
    if !env.state.has_shared_memory_mappings() {
        return Ok(());
    }
    for mapping in env.state.shared_memory_mappings_for_pid(env.pid().raw()) {
        if is_direct_shared_mapping_active(env, memory, &mapping)? {
            continue;
        }
        let ptr = WasmPtr::<u8, M>::new(to_offset::<M>(
            mapping.offset.try_into().map_err(|_| Errno::Overflow)?,
        )?);
        let len = to_offset::<M>(mapping.len.try_into().map_err(|_| Errno::Overflow)?)?;
        let bytes = mapping.read_bytes()?;
        if bytes.len() != mapping.len as usize {
            return Err(Errno::Inval);
        }
        ptr.slice(memory, len)
            .map_err(mem_error_to_wasi)?
            .write_slice(&bytes)
            .map_err(mem_error_to_wasi)?;
        mapping
            .process_snapshots
            .lock()
            .unwrap()
            .insert(env.pid().raw(), bytes);
    }
    fence(Ordering::SeqCst);
    Ok(())
}

pub(crate) fn write_bytes_inner<T: Write, M: MemorySize>(
    mut write_loc: T,
    memory: &MemoryView,
    iovs_arr_cell: WasmSlice<__wasi_ciovec_t<M>>,
) -> Result<usize, Errno> {
    let mut bytes_written = 0usize;
    for iov in iovs_arr_cell.iter() {
        let iov_inner = iov.read().map_err(mem_error_to_wasi)?;
        let bytes = WasmPtr::<u8, M>::new(iov_inner.buf)
            .slice(memory, iov_inner.buf_len)
            .map_err(mem_error_to_wasi)?;
        let bytes = bytes.read_to_vec().map_err(mem_error_to_wasi)?;
        write_loc.write_all(&bytes).map_err(map_io_err)?;

        bytes_written += from_offset::<M>(iov_inner.buf_len)?;
    }
    Ok(bytes_written)
}

pub(crate) fn write_bytes<T: Write, M: MemorySize>(
    mut write_loc: T,
    memory: &MemoryView,
    iovs_arr: WasmSlice<__wasi_ciovec_t<M>>,
) -> Result<usize, Errno> {
    let result = write_bytes_inner::<_, M>(&mut write_loc, memory, iovs_arr);
    write_loc.flush();
    result
}

pub(crate) fn copy_from_slice<M: MemorySize>(
    mut read_loc: &[u8],
    memory: &MemoryView,
    iovs_arr: WasmSlice<__wasi_iovec_t<M>>,
) -> Result<usize, Errno> {
    let mut bytes_read = 0usize;

    let iovs_arr = iovs_arr.access().map_err(mem_error_to_wasi)?;
    for iovs in iovs_arr.iter() {
        let mut buf = WasmPtr::<u8, M>::new(iovs.buf)
            .slice(memory, iovs.buf_len)
            .map_err(mem_error_to_wasi)?
            .access()
            .map_err(mem_error_to_wasi)?;

        let to_read = from_offset::<M>(iovs.buf_len)?;
        let to_read = to_read.min(read_loc.len());
        if to_read == 0 {
            break;
        }
        let (left, right) = read_loc.split_at(to_read);
        let amt = buf.copy_from_slice_min(left);
        if amt != to_read {
            return Ok(bytes_read + amt);
        }

        read_loc = right;
        bytes_read += to_read;
    }
    Ok(bytes_read)
}

pub(crate) fn read_bytes<T: Read, M: MemorySize>(
    mut reader: T,
    memory: &MemoryView,
    iovs_arr: WasmSlice<__wasi_iovec_t<M>>,
) -> Result<usize, Errno> {
    let mut bytes_read = 0usize;

    let iovs_arr = iovs_arr.access().map_err(mem_error_to_wasi)?;
    for iovs in iovs_arr.iter() {
        let mut buf = WasmPtr::<u8, M>::new(iovs.buf)
            .slice(memory, iovs.buf_len)
            .map_err(mem_error_to_wasi)?
            .access()
            .map_err(mem_error_to_wasi)?;

        let to_read = buf.len();
        let has_read = reader.read(buf.as_mut()).map_err(map_io_err)?;

        bytes_read += has_read;
        if has_read != to_read {
            return Ok(bytes_read);
        }
    }
    Ok(bytes_read)
}

// TODO: remove allow once inodes are refactored (see comments on [`WasiState`])
/// Writes data to the stderr
#[allow(clippy::await_holding_lock)]
pub unsafe fn stderr_write<'a>(
    ctx: &FunctionEnvMut<'_, WasiEnv>,
    buf: &[u8],
) -> LocalBoxFuture<'a, Result<(), Errno>> {
    let env = ctx.data();
    let (memory, state, inodes) = unsafe { env.get_memory_and_wasi_state_and_inodes(ctx, 0) };

    let buf = buf.to_vec();
    let mut stderr = WasiInodes::stderr_mut(&state.fs.fd_map).map_err(fs_error_into_wasi_err);
    Box::pin(async move { stderr?.write_all(&buf).await.map_err(map_io_err) })
}

fn block_on_with_timeout<T, Fut>(
    tasks: &Arc<dyn VirtualTaskManager>,
    timeout: Option<Duration>,
    work: Fut,
) -> WasiResult<T>
where
    Fut: Future<Output = WasiResult<T>>,
{
    let mut nonblocking = false;
    if timeout == Some(Duration::ZERO) {
        nonblocking = true;
    }
    let timeout = async {
        if let Some(timeout) = timeout {
            if !nonblocking {
                tasks.sleep_now(timeout).await
            } else {
                InfiniteSleep::default().await
            }
        } else {
            InfiniteSleep::default().await
        }
    };

    let work = async move {
        tokio::select! {
            // The main work we are doing
            res = work => res,
            // Optional timeout
            _ = timeout => Ok(Err(Errno::Timedout)),
        }
    };

    // Fast path
    if nonblocking {
        let waker = WasiDummyWaker.into_waker();
        let mut cx = Context::from_waker(&waker);
        let mut pinned_work = Box::pin(work);
        if let Poll::Ready(res) = pinned_work.as_mut().poll(&mut cx) {
            return res;
        }
        return Ok(Err(Errno::Again));
    }

    // Slow path, block on the work and process process
    block_on(work)
}

/// Asyncify takes the current thread and blocks on the async runtime associated with it
/// thus allowed for asynchronous operations to execute. It has built in functionality
/// to (optionally) timeout the IO, force exit the process, callback signals and pump
/// synchronous IO engine
pub(crate) fn __asyncify<T, Fut>(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    timeout: Option<Duration>,
    work: Fut,
) -> WasiResult<T>
where
    T: 'static,
    Fut: std::future::Future<Output = Result<T, Errno>>,
{
    let mut env = ctx.data();

    // Check if we need to exit the asynchronous loop
    if let Some(exit_code) = env.should_exit() {
        return Err(WasiError::Exit(exit_code));
    }

    // This poller will process any signals when the main working function is idle
    struct SignalPoller<'a, 'b, Fut, T>
    where
        Fut: Future<Output = Result<T, Errno>>,
    {
        ctx: &'a mut FunctionEnvMut<'b, WasiEnv>,
        pinned_work: Pin<Box<Fut>>,
    }
    impl<Fut, T> Future for SignalPoller<'_, '_, Fut, T>
    where
        Fut: Future<Output = Result<T, Errno>>,
    {
        type Output = Result<Fut::Output, WasiError>;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if let Some(signals) = self.ctx.data().thread.pop_signals_or_subscribe(cx.waker()) {
                if let Err(err) = WasiEnv::process_signals_internal(self.ctx, signals) {
                    return Poll::Ready(Err(err));
                }
                return Poll::Ready(Ok(Err(Errno::Intr)));
            }
            if let Poll::Ready(res) = Pin::new(&mut self.pinned_work).poll(cx) {
                return Poll::Ready(Ok(res));
            }
            Poll::Pending
        }
    }

    // Block on the work
    let mut pinned_work = Box::pin(work);
    let tasks = env.tasks().clone();
    let poller = SignalPoller { ctx, pinned_work };
    block_on_with_timeout(&tasks, timeout, poller)
}

/// Future that will be polled by asyncify methods
/// (the return value is what will be returned in rewind
///  or in the instant response)
pub type AsyncifyFuture = dyn Future<Output = Bytes> + Send + Sync + 'static;

// This poller will process any signals when the main working function is idle
struct AsyncifyPoller<'a, 'b, 'c, T, Fut>
where
    Fut: Future<Output = T> + Send + Sync + 'static,
{
    ctx: &'b mut FunctionEnvMut<'c, WasiEnv>,
    work: &'a mut Pin<Box<Fut>>,
}
impl<T, Fut> Future for AsyncifyPoller<'_, '_, '_, T, Fut>
where
    Fut: Future<Output = T> + Send + Sync + 'static,
{
    type Output = Result<T, WasiError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        WasiEnv::do_pending_link_operations(self.ctx, false);

        let env = self.ctx.data();
        if let Some(forced_exit) = env.thread.try_join() {
            return Poll::Ready(Err(WasiError::Exit(forced_exit.unwrap_or_else(|err| {
                tracing::debug!("exit runtime error - {}", err);
                Errno::Child.into()
            }))));
        }
        if env.thread.has_signals_or_subscribe(cx.waker()) {
            let has_exit = {
                let signals = env.thread.signals().lock().unwrap();
                let dispositions = env.state.signals.lock().unwrap();
                signals
                    .0
                    .iter()
                    .filter_map(|sig| {
                        if matches!(dispositions.get(sig), Some(Disposition::Ignore)) {
                            return None;
                        }
                        if *sig == Signal::Sigkill {
                            Some(env.thread.set_or_get_exit_code_for_signal(*sig))
                        } else {
                            None
                        }
                    })
                    .next()
            };

            match WasiEnv::process_signals_and_exit(self.ctx) {
                Ok(Ok(_)) => {
                    if let Some(exit_code) = has_exit {
                        return Poll::Ready(Err(WasiError::Exit(exit_code)));
                    } else {
                        // Re-subscribe so we get woken up for further signals as well
                        self.ctx.data().thread.signals_subscribe(cx.waker());
                    }
                }
                Ok(Err(err)) => return Poll::Ready(Err(WasiError::Exit(ExitCode::from(err)))),
                Err(err) => return Poll::Ready(Err(err)),
            }
        }
        if let Poll::Ready(res) = self.work.as_mut().poll(cx) {
            return Poll::Ready(Ok(res));
        }
        Poll::Pending
    }
}

pub enum AsyncifyAction<'a, R> {
    /// Indicates that asyncify callback finished and the
    /// caller now has ownership of the ctx again
    Finish(FunctionEnvMut<'a, WasiEnv>, R),
    /// Indicates that asyncify should unwind by immediately exiting
    /// the current function
    Unwind,
}

/// Exponentially increasing backoff of CPU usage
///
/// Under certain conditions the process will exponentially backoff
/// using waits that either put the thread into a low usage state
/// or even underload the thread completely when deep sleep is enabled
///
/// The use-case for this is to handle rogue WASM processes that
/// generate excessively high CPU usage and need to be artificially
/// throttled
///
pub(crate) fn maybe_backoff<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
) -> Result<Result<FunctionEnvMut<'_, WasiEnv>, Errno>, WasiError> {
    let env = ctx.data();

    // Fast path that exits this high volume call if we do not have
    // exponential backoff enabled
    if env.enable_exponential_cpu_backoff.is_none() {
        return Ok(Ok(ctx));
    }

    // Determine if we need to do a backoff, if so lets do one
    if let Some(backoff) = env.process.acquire_cpu_backoff_token(env.tasks()) {
        tracing::trace!("exponential CPU backoff {:?}", backoff.backoff_time());
        if let AsyncifyAction::Finish(mut ctx, _) =
            __asyncify_with_deep_sleep::<M, _, _>(ctx, backoff)?
        {
            Ok(Ok(ctx))
        } else {
            Ok(Err(Errno::Success))
        }
    } else {
        Ok(Ok(ctx))
    }
}

/// Asyncify takes the current thread and blocks on the async runtime associated with it
/// thus allowed for asynchronous operations to execute. It has built in functionality
/// to (optionally) timeout the IO, force exit the process, callback signals and pump
/// synchronous IO engine
///
/// This will either return the `ctx` as the asyncify has completed successfully
/// or it will return an WasiError which will exit the WASM call using asyncify
/// and instead process it on a shared task
///
pub(crate) fn __asyncify_with_deep_sleep<M: MemorySize, T, Fut>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    work: Fut,
) -> Result<AsyncifyAction<'_, T>, WasiError>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
    Fut: Future<Output = T> + Send + Sync + 'static,
{
    // Determine the deep sleep time
    let deep_sleep_time = match ctx.data().enable_journal {
        true => Duration::from_micros(100),
        false => Duration::from_millis(50),
    };

    // Box up the trigger
    let mut trigger = Box::pin(work);

    // Define the work
    let tasks = ctx.data().tasks().clone();
    let work = async move {
        let env = ctx.data();

        // Create the deep sleeper
        // Deep sleep breaks the linker completely, as it expects other modules to catch the
        // signal for DL ops
        let is_main_thread = env.tid().raw() == env.pid().raw();
        let is_only_thread = env.process.all_threads().len() == 1;
        let tasks_for_deep_sleep = if is_main_thread
            && is_only_thread
            && env.enable_deep_sleep
            && env.inner().linker().is_none()
        {
            Some(env.tasks().clone())
        } else {
            None
        };

        let deep_sleep_wait = async {
            if let Some(tasks) = tasks_for_deep_sleep {
                tasks.sleep_now(deep_sleep_time).await
            } else {
                InfiniteSleep::default().await
            }
        };

        Ok(tokio::select! {
            // Inner wait with finializer
            res = AsyncifyPoller {
                ctx: &mut ctx,
                work: &mut trigger,
            } => {
                let result = res?;
                AsyncifyAction::Finish(ctx, result)
            },
            // Determines when and if we should go into a deep sleep
            _ = deep_sleep_wait => {
                let pid = ctx.data().pid();
                let tid = ctx.data().tid();

                // We put thread into a deep sleeping state and
                // notify anyone who is waiting for that
                let thread = ctx.data().thread.clone();
                thread.set_deep_sleeping(true);
                ctx.data().process.inner.1.notify_one();

                tracing::trace!(%pid, %tid, "thread entering deep sleep");
                deep_sleep::<M>(ctx, Box::pin(async move {
                    // After this wakes the background work or waking
                    // event has triggered and its time to result
                    let result = trigger.await;
                    tracing::trace!(%pid, %tid, "thread leaving deep sleep");
                    thread.set_deep_sleeping(false);
                    bincode::serde::encode_to_vec(&result, config::legacy()).unwrap().into()
                }))?;
                AsyncifyAction::Unwind
            },
        })
    };

    // Block until the work is finished or until we
    // unload the thread using asyncify
    block_on(work)
}

/// Asyncify takes the current thread and blocks on the async runtime associated with it
/// thus allowed for asynchronous operations to execute. It has built in functionality
/// to (optionally) timeout the IO, force exit the process, callback signals and pump
/// synchronous IO engine
pub(crate) fn __asyncify_light<T, Fut>(
    env: &WasiEnv,
    _timeout: Option<Duration>,
    work: Fut,
) -> WasiResult<T>
where
    T: 'static,
    Fut: Future<Output = Result<T, Errno>>,
{
    let snapshot_wait = wait_for_snapshot(env);

    // Block until the work is finished or until we
    // unload the thread using asyncify
    Ok(block_on(work))
}

// This should be compiled away, it will simply wait forever however its never
// used by itself, normally this is passed into asyncify which will still abort
// the operating on timeouts, signals or other work due to a select! around the await
#[derive(Default)]
pub struct InfiniteSleep {}
impl std::future::Future for InfiniteSleep {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

/// Performs an immutable operation on the socket while running in an asynchronous runtime
/// This has built in signal support
pub(crate) fn __sock_asyncify<T, F, Fut>(
    env: &WasiEnv,
    sock: WasiFd,
    rights: Rights,
    actor: F,
) -> Result<T, Errno>
where
    F: FnOnce(crate::net::socket::InodeSocket, Fd) -> Fut,
    Fut: std::future::Future<Output = Result<T, Errno>>,
{
    let fd_entry = env.state.fs.get_fd(sock)?;
    if !rights.is_empty() && !fd_entry.inner.rights.contains(rights) {
        return Err(Errno::Access);
    }

    let mut work = {
        let inode = fd_entry.inode.clone();
        let tasks = env.tasks().clone();
        let mut guard = inode.write();
        match guard.deref_mut() {
            Kind::Socket { socket } => {
                // Clone the socket and release the lock
                let socket = socket.clone();
                drop(guard);

                // Start the work using the socket
                actor(socket, fd_entry)
            }
            _ => {
                return Err(Errno::Notsock);
            }
        }
    };

    // Block until the work is finished or until we
    // unload the thread using asyncify
    block_on(work)
}

/// Performs mutable work on a socket under an asynchronous runtime with
/// built in signal processing
pub(crate) fn __sock_asyncify_mut<T, F, Fut>(
    ctx: &'_ mut FunctionEnvMut<'_, WasiEnv>,
    sock: WasiFd,
    rights: Rights,
    actor: F,
) -> Result<T, Errno>
where
    F: FnOnce(crate::net::socket::InodeSocket, Fd) -> Fut,
    Fut: std::future::Future<Output = Result<T, Errno>>,
{
    let env = ctx.data();
    let tasks = env.tasks().clone();

    let fd_entry = env.state.fs.get_fd(sock)?;
    if !rights.is_empty() && !fd_entry.inner.rights.contains(rights) {
        return Err(Errno::Access);
    }

    let inode = fd_entry.inode.clone();
    let mut guard = inode.write();
    match guard.deref_mut() {
        Kind::Socket { socket } => {
            // Clone the socket and release the lock
            let socket = socket.clone();
            drop(guard);

            // Start the work using the socket
            let mut work = actor(socket, fd_entry);

            // Otherwise we block on the work and process it
            // using an asynchronou context
            block_on(work)
        }
        _ => Err(Errno::Notsock),
    }
}

/// Performs an immutable operation on the socket while running in an asynchronous runtime
/// This has built in signal support
pub(crate) fn __sock_actor<T, F>(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    sock: WasiFd,
    rights: Rights,
    actor: F,
) -> Result<T, Errno>
where
    T: 'static,
    F: FnOnce(crate::net::socket::InodeSocket, Fd) -> Result<T, Errno>,
{
    let env = ctx.data();
    let tasks = env.tasks().clone();

    let fd_entry = env.state.fs.get_fd(sock)?;
    if !rights.is_empty() && !fd_entry.inner.rights.contains(rights) {
        return Err(Errno::Access);
    }

    let inode = fd_entry.inode.clone();

    let tasks = env.tasks().clone();
    let mut guard = inode.write();
    match guard.deref_mut() {
        Kind::Socket { socket } => {
            // Clone the socket and release the lock
            let socket = socket.clone();
            drop(guard);

            // Start the work using the socket
            actor(socket, fd_entry)
        }
        _ => Err(Errno::Notsock),
    }
}

/// Performs mutable work on a socket under an asynchronous runtime with
/// built in signal processing
pub(crate) fn __sock_actor_mut<T, F>(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    sock: WasiFd,
    rights: Rights,
    actor: F,
) -> Result<T, Errno>
where
    T: 'static,
    F: FnOnce(crate::net::socket::InodeSocket, Fd) -> Result<T, Errno>,
{
    let env = ctx.data();
    let tasks = env.tasks().clone();

    let fd_entry = env.state.fs.get_fd(sock)?;
    if !rights.is_empty() && !fd_entry.inner.rights.contains(rights) {
        return Err(Errno::Access);
    }

    let inode = fd_entry.inode.clone();
    let mut guard = inode.write();
    match guard.deref_mut() {
        Kind::Socket { socket } => {
            // Clone the socket and release the lock
            let socket = socket.clone();
            drop(guard);

            // Start the work using the socket
            actor(socket, fd_entry)
        }
        _ => Err(Errno::Notsock),
    }
}

/// Replaces a socket with another socket in under an asynchronous runtime.
/// This is used for opening sockets or connecting sockets which changes
/// the fundamental state of the socket to another state machine
pub(crate) fn __sock_upgrade<'a, F, Fut>(
    ctx: &'a mut FunctionEnvMut<'_, WasiEnv>,
    sock: WasiFd,
    rights: Rights,
    actor: F,
) -> Result<(), Errno>
where
    F: FnOnce(crate::net::socket::InodeSocket, Fdflags) -> Fut,
    Fut: std::future::Future<Output = Result<Option<crate::net::socket::InodeSocket>, Errno>> + 'a,
{
    let env = ctx.data();
    let fd_entry = env.state.fs.get_fd(sock)?;
    if !rights.is_empty() && !fd_entry.inner.rights.contains(rights) {
        tracing::warn!(
            "wasi[{}:{}]::sock_upgrade(fd={}, rights={:?}) - failed - no access rights to upgrade",
            ctx.data().pid(),
            ctx.data().tid(),
            sock,
            rights
        );
        return Err(Errno::Access);
    }

    let tasks = env.tasks().clone();
    {
        let inode = fd_entry.inode;
        let mut guard = inode.write();
        match guard.deref_mut() {
            Kind::Socket { socket } => {
                let socket = socket.clone();
                drop(guard);

                // Start the work using the socket
                let work = actor(socket, fd_entry.inner.flags);

                // Block on the work and process it
                let res = block_on(work);
                let new_socket = res?;

                if let Some(mut new_socket) = new_socket {
                    let mut guard = inode.write();
                    match guard.deref_mut() {
                        Kind::Socket { socket, .. } => {
                            std::mem::swap(socket, &mut new_socket);
                        }
                        _ => {
                            tracing::warn!(
                                "wasi[{}:{}]::sock_upgrade(fd={}, rights={:?}) - failed - not a socket",
                                ctx.data().pid(),
                                ctx.data().tid(),
                                sock,
                                rights
                            );
                            return Err(Errno::Notsock);
                        }
                    }
                }
            }
            _ => {
                tracing::warn!(
                    "wasi[{}:{}]::sock_upgrade(fd={}, rights={:?}) - failed - not a socket",
                    ctx.data().pid(),
                    ctx.data().tid(),
                    sock,
                    rights
                );
                return Err(Errno::Notsock);
            }
        }
    }

    Ok(())
}

#[must_use]
pub(crate) fn write_buffer_array<M: MemorySize>(
    memory: &MemoryView,
    from: &[Vec<u8>],
    ptr_buffer: WasmPtr<WasmPtr<u8, M>, M>,
    buffer: WasmPtr<u8, M>,
) -> Errno {
    let ptrs = wasi_try_mem!(ptr_buffer.slice(memory, wasi_try!(to_offset::<M>(from.len()))));

    let mut current_buffer_offset = 0usize;
    for ((i, sub_buffer), ptr) in from.iter().enumerate().zip(ptrs.iter()) {
        let mut buf_offset = buffer.offset();
        buf_offset += wasi_try!(to_offset::<M>(current_buffer_offset));
        let new_ptr = WasmPtr::new(buf_offset);
        wasi_try_mem!(ptr.write(new_ptr));

        let data =
            wasi_try_mem!(new_ptr.slice(memory, wasi_try!(to_offset::<M>(sub_buffer.len()))));
        wasi_try_mem!(data.write_slice(sub_buffer));
        wasi_try_mem!(
            wasi_try_mem!(new_ptr.add_offset(wasi_try!(to_offset::<M>(sub_buffer.len()))))
                .write(memory, 0)
        );

        current_buffer_offset += sub_buffer.len() + 1;
    }

    Errno::Success
}

pub(crate) fn get_current_time_in_nanos() -> Result<Timestamp, Errno> {
    let now = platform_clock_time_get(Snapshot0Clockid::Monotonic, 1_000_000)? as u128;
    Ok(now as Timestamp)
}

pub(crate) fn get_stack_lower(env: &WasiEnv) -> u64 {
    env.layout.stack_lower
}

pub(crate) fn get_stack_upper(env: &WasiEnv) -> u64 {
    env.layout.stack_upper
}

pub(crate) unsafe fn get_memory_stack_pointer(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
) -> Result<u64, String> {
    // Get the current value of the stack pointer (which we will use
    // to save all of the stack)
    let stack_upper = get_stack_upper(ctx.data());
    let stack_pointer = if let Some(stack_pointer) = ctx
        .data()
        .inner()
        .main_module_instance_handles()
        .stack_pointer
        .clone()
    {
        match stack_pointer.get(ctx) {
            Value::I32(a) => a as u64,
            Value::I64(a) => a as u64,
            _ => stack_upper,
        }
    } else {
        return Err("failed to save stack: no __stack_pointer global".to_string());
    };
    Ok(stack_pointer)
}

fn get_memory_stack_pointer_from_handles(
    env: &WasiEnv,
    store: &mut impl AsStoreMut,
) -> Result<u64, String> {
    let stack_upper = get_stack_upper(env);
    let handles = env.inner().main_module_instance_handles();
    if let Some(stack_pointer) = handles.stack_pointer.clone() {
        return Ok(match stack_pointer.get(store) {
            Value::I32(a) => a as u64,
            Value::I64(a) => a as u64,
            _ => stack_upper,
        });
    }
    if let Ok(get_stack_pointer) = handles
        .instance
        .exports
        .get_typed_function::<(), i32>(store, "__wasilibc_get_stack_pointer")
    {
        return get_stack_pointer
            .call(store)
            .map(|stack_pointer| stack_pointer as u64)
            .map_err(|err| format!("failed to read stack pointer export: {err}"));
    }
    Err("failed to save stack: no __stack_pointer global".to_string())
}

pub(crate) unsafe fn get_memory_stack_offset(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
) -> Result<u64, String> {
    let stack_upper = get_stack_upper(ctx.data());
    let stack_pointer = unsafe { get_memory_stack_pointer(ctx) }?;
    Ok(stack_upper - stack_pointer)
}

pub(crate) fn set_memory_stack_offset(
    env: &WasiEnv,
    store: &mut impl AsStoreMut,
    offset: u64,
) -> Result<(), String> {
    // Sets the stack pointer
    let stack_upper = get_stack_upper(env);
    let stack_pointer = stack_upper - offset;
    let handles = env.inner().main_module_instance_handles();
    if let Some(stack_pointer_ptr) = handles.stack_pointer.clone() {
        match stack_pointer_ptr.get(store) {
            Value::I32(_) => {
                stack_pointer_ptr.set(store, Value::I32(stack_pointer as i32));
            }
            Value::I64(_) => {
                stack_pointer_ptr.set(store, Value::I64(stack_pointer as i64));
            }
            _ => {
                return Err(
                    "failed to save stack: __stack_pointer global is of an unknown type"
                        .to_string(),
                );
            }
        }
    } else if let Ok(set_stack_pointer) = handles
        .instance
        .exports
        .get_typed_function::<i32, ()>(store, "__wasilibc_set_stack_pointer")
    {
        set_stack_pointer
            .call(store, stack_pointer as i32)
            .map_err(|err| format!("failed to set stack pointer export: {err}"))?;
    } else {
        return Err("failed to save stack: no __stack_pointer global".to_string());
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn get_memory_stack<M: MemorySize>(
    env: &WasiEnv,
    store: &mut impl AsStoreMut,
) -> Result<BytesMut, String> {
    // Get the current value of the stack pointer (which we will use
    // to save all of the stack)
    let stack_pointer = get_memory_stack_pointer_from_handles(env, store)?;
    let memory = env
        .try_memory_view(store)
        .ok_or_else(|| "unable to access the memory of the instance".to_string())?;
    let stack_offset = env.layout.stack_upper - stack_pointer;

    // Read the memory stack into a vector
    let memory_stack_ptr = WasmPtr::<u8, M>::new(
        stack_pointer
            .try_into()
            .map_err(|err| format!("failed to save stack: stack pointer overflow (stack_pointer={}, stack_lower={}, stack_upper={})", stack_offset, env.layout.stack_lower, env.layout.stack_upper))?,
    );

    memory_stack_ptr
        .slice(
            &memory,
            stack_offset
                .try_into()
                .map_err(|err| format!("failed to save stack: stack pointer overflow (stack_pointer={}, stack_lower={}, stack_upper={})", stack_offset, env.layout.stack_lower, env.layout.stack_upper))?,
        )
        .and_then(|memory_stack| memory_stack.read_to_bytes())
        .map_err(|err| format!("failed to read stack: {err}"))
}

#[allow(dead_code)]
pub(crate) fn set_memory_stack<M: MemorySize>(
    env: &WasiEnv,
    store: &mut impl AsStoreMut,
    stack: Bytes,
) -> Result<(), String> {
    // First we restore the memory stack
    let stack_upper = get_stack_upper(env);
    let stack_offset = stack.len() as u64;
    let stack_pointer = stack_upper - stack_offset;
    let stack_ptr = WasmPtr::<u8, M>::new(
        stack_pointer
            .try_into()
            .map_err(|_| "failed to restore stack: stack pointer overflow".to_string())?,
    );

    let memory = env
        .try_memory_view(store)
        .ok_or_else(|| "unable to set the stack pointer of the instance".to_string())?;
    stack_ptr
        .slice(
            &memory,
            stack_offset
                .try_into()
                .map_err(|_| "failed to restore stack: stack pointer overflow".to_string())?,
        )
        .and_then(|memory_stack| memory_stack.write_slice(&stack[..]))
        .map_err(|err| format!("failed to write stack: {err}"))?;

    // Set the stack pointer itself and return
    set_memory_stack_offset(env, store, stack_offset)?;
    Ok(())
}

/// Puts the process to deep sleep and wakes it again when
/// the supplied future completes
#[must_use = "you must return the result immediately so the stack can unwind"]
pub(crate) fn deep_sleep<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    trigger: Pin<Box<AsyncifyFuture>>,
) -> Result<(), WasiError> {
    // Grab all the globals and serialize them
    let store_data = crate::utils::store::capture_store_snapshot(&mut ctx.as_store_mut())
        .serialize()
        .unwrap();
    let store_data = Bytes::from(store_data);
    let thread_start = ctx.data().thread.thread_start_type();

    // Perform the unwind action
    let tasks = ctx.data().tasks().clone();
    let res = unwind::<M, _>(ctx, move |mut ctx, memory_stack, rewind_stack| {
        let memory_stack = memory_stack.freeze();
        let rewind_stack = rewind_stack.freeze();
        let thread_layout = ctx.data().thread.memory_layout().clone();

        // If journal'ing is enabled then we dump the stack into the journal
        if ctx.data().enable_journal {
            // Grab all the globals and serialize them
            let store_data = crate::utils::store::capture_store_snapshot(&mut ctx.as_store_mut())
                .serialize()
                .unwrap();
            let store_data = Bytes::from(store_data);

            tracing::trace!(
                "stack snapshot unwind (memory_stack={}, rewind_stack={}, store_data={})",
                memory_stack.len(),
                rewind_stack.len(),
                store_data.len(),
            );

            #[cfg(feature = "journal")]
            {
                // Write our thread state to the snapshot
                let tid = ctx.data().thread.tid();
                let thread_start = ctx.data().thread.thread_start_type();
                if let Err(err) = JournalEffector::save_thread_state::<M>(
                    &mut ctx,
                    tid,
                    memory_stack.clone(),
                    rewind_stack.clone(),
                    store_data.clone(),
                    thread_start,
                    thread_layout.clone(),
                ) {
                    return wasmer_types::OnCalledAction::Trap(err.into());
                }
            }

            // If all the threads are now in a deep sleep state
            // then we can trigger the idle snapshot event
            let inner = ctx.data().process.inner.clone();
            let is_idle = {
                let mut guard = inner.0.lock().unwrap();
                guard.threads.values().all(WasiThread::is_deep_sleeping)
            };

            // When we idle the journal functionality may be set
            // will take a snapshot of the memory and threads so
            // that it can resumed.
            #[cfg(feature = "journal")]
            {
                if is_idle && ctx.data_mut().has_snapshot_trigger(SnapshotTrigger::Idle) {
                    let mut guard = inner.0.lock().unwrap();
                    if let Err(err) = JournalEffector::save_memory_and_snapshot(
                        &mut ctx,
                        &mut guard,
                        SnapshotTrigger::Idle,
                    ) {
                        return wasmer_types::OnCalledAction::Trap(err.into());
                    }
                }
            }
        }

        // Schedule the process on the stack so that it can be resumed
        OnCalledAction::Trap(Box::new(WasiError::DeepSleep(DeepSleepWork {
            trigger,
            rewind: RewindState {
                memory_stack,
                rewind_stack,
                store_data,
                start: thread_start,
                layout: thread_layout,
                is_64bit: M::is_64bit(),
            },
        })))
    })?;

    // If there is an error then exit the process, otherwise we are done
    match res {
        Errno::Success => Ok(()),
        err => Err(WasiError::Exit(ExitCode::from(err))),
    }
}

#[must_use = "you must return the result immediately so the stack can unwind"]
pub fn unwind<M: MemorySize, F>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    callback: F,
) -> Result<Errno, WasiError>
where
    F: FnOnce(FunctionEnvMut<'_, WasiEnv>, BytesMut, BytesMut) -> OnCalledAction
        + Send
        + Sync
        + 'static,
{
    if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
        eprintln!(
            "sinomo-debug unwind begin pid={} tid={} stack_lower={} stack_upper={}",
            ctx.data().pid().raw(),
            ctx.data().tid().raw(),
            ctx.data().layout.stack_lower,
            ctx.data().layout.stack_upper
        );
    }
    // Get the current stack pointer (this will be used to determine the
    // upper limit of stack space remaining to unwind into)
    let (env, mut store) = ctx.data_and_store_mut();
    let memory_stack = match get_memory_stack::<M>(env, &mut store) {
        Ok(a) => a,
        Err(err) => {
            warn!("unable to get the memory stack - {}", err);
            if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
                eprintln!("sinomo-debug unwind get-memory-stack-error {err}");
            }
            return Err(WasiError::Exit(Errno::Unknown.into()));
        }
    };
    if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
        eprintln!(
            "sinomo-debug unwind memory-stack pid={} tid={} len={}",
            ctx.data().pid().raw(),
            ctx.data().tid().raw(),
            memory_stack.len()
        );
    }

    // Perform a check to see if we have enough room
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };

    // Write the addresses to the start of the stack space
    let unwind_pointer = env.layout.stack_lower;
    let unwind_data_start =
        unwind_pointer + (std::mem::size_of::<__wasi_asyncify_t<M::Offset>>() as u64);
    let unwind_data = __wasi_asyncify_t::<M::Offset> {
        start: wasi_try_ok!(unwind_data_start.try_into().map_err(|_| Errno::Overflow)),
        end: wasi_try_ok!(
            (env.layout.stack_upper - memory_stack.len() as u64)
                .try_into()
                .map_err(|_| Errno::Overflow)
        ),
    };
    let unwind_data_ptr: WasmPtr<__wasi_asyncify_t<M::Offset>, M> = WasmPtr::new(wasi_try_ok!(
        unwind_pointer.try_into().map_err(|_| Errno::Overflow)
    ));
    wasi_try_mem_ok!(unwind_data_ptr.write(&memory, unwind_data));

    // Invoke the callback that will prepare to unwind
    // We need to start unwinding the stack
    let asyncify_data = wasi_try_ok!(unwind_pointer.try_into().map_err(|_| Errno::Overflow));
    if let Some(asyncify_start_unwind) = env
        .inner()
        .main_module_instance_handles()
        .asyncify_start_unwind
        .clone()
    {
        if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
            eprintln!(
                "sinomo-debug unwind start pid={} tid={} asyncify_data={} stack_begin={} stack_end={}",
                ctx.data().pid().raw(),
                ctx.data().tid().raw(),
                asyncify_data,
                unwind_data.start.into(),
                unwind_data.end.into()
            );
        }
        if let Err(err) = asyncify_start_unwind.call(&mut ctx, asyncify_data) {
            warn!("failed to start asyncify unwind - {}", err);
            if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
                eprintln!("sinomo-debug unwind start-error {err}");
            }
            return Err(WasiError::Exit(Errno::Unknown.into()));
        }
        if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
            eprintln!(
                "sinomo-debug unwind started pid={} tid={}",
                ctx.data().pid().raw(),
                ctx.data().tid().raw()
            );
        }
    } else {
        warn!("failed to unwind the stack because the asyncify_start_rewind export is missing");
        return Err(WasiError::Exit(Errno::Noexec.into()));
    }

    // Set callback that will be invoked when this process finishes
    let env = ctx.data();
    let unwind_stack_begin: u64 = unwind_data.start.into();
    let total_stack_space = env.layout.stack_size;
    let func = ctx.as_ref();
    trace!(
        stack_upper = env.layout.stack_upper,
        stack_lower = env.layout.stack_lower,
        "wasi[{}:{}]::unwinding (used_stack_space={} total_stack_space={})",
        ctx.data().pid(),
        ctx.data().tid(),
        memory_stack.len(),
        total_stack_space
    );
    ctx.as_store_mut().on_called(move |mut store| {
        let mut ctx = func.into_mut(&mut store);
        let env = ctx.data();
        let memory = env
            .try_memory_view(&ctx)
            .ok_or_else(|| "failed to save stack: stack pointer overflow - unable to access the memory of the instance".to_string())?;

        let unwind_data_ptr: WasmPtr<__wasi_asyncify_t<M::Offset>, M> = WasmPtr::new(
            unwind_pointer
                .try_into()
                .map_err(|_| Errno::Overflow)
                .unwrap(),
        );
        let unwind_data_result = unwind_data_ptr.read(&memory).unwrap();
        let unwind_stack_finish: u64 = unwind_data_result.start.into();
        let unwind_size = unwind_stack_finish - unwind_stack_begin;
        trace!(
            "wasi[{}:{}]::unwound (memory_stack_size={} unwind_size={})",
            ctx.data().pid(),
            ctx.data().tid(),
            memory_stack.len(),
            unwind_size
        );

        // Read the memory stack into a vector
        let unwind_stack_ptr = WasmPtr::<u8, M>::new(
            unwind_stack_begin
                .try_into()
                .map_err(|_| "failed to save stack: stack pointer overflow".to_string())?,
        );
        let unwind_stack = unwind_stack_ptr
            .slice(
                &memory,
                unwind_size
                    .try_into()
                    .map_err(|_| "failed to save stack: stack pointer overflow".to_string())?,
            )
            .and_then(|memory_stack| memory_stack.read_to_bytes())
            .map_err(|err| format!("failed to read stack: {err}"))?;

        // Notify asyncify that we are no longer unwinding
        if let Some(asyncify_stop_unwind) = env
            .inner()
            .main_module_instance_handles()
            .asyncify_stop_unwind
            .clone()
        {
            asyncify_stop_unwind.call(&mut ctx);
        } else {
            warn!("failed to unwind the stack because the asyncify_start_rewind export is missing");
            return Ok(OnCalledAction::Finish);
        }

        Ok(callback(ctx, memory_stack, unwind_stack))
    });

    // We need to exit the function so that it can unwind and then invoke the callback
    Ok(Errno::Success)
}

// NOTE: not tracing-instrumented because [`rewind_ext`] already is.
#[must_use = "the action must be passed to the call loop"]
pub fn rewind<M: MemorySize, T>(
    mut ctx: FunctionEnvMut<WasiEnv>,
    memory_stack: Option<Bytes>,
    rewind_stack: Bytes,
    store_data: Bytes,
    result: T,
) -> Errno
where
    T: serde::Serialize,
{
    let rewind_result = bincode::serde::encode_to_vec(&result, config::legacy())
        .unwrap()
        .into();
    rewind_ext::<M>(
        &mut ctx,
        memory_stack,
        rewind_stack,
        store_data,
        RewindResultType::RewindWithResult(rewind_result),
    )
}

#[instrument(level = "trace", skip_all, fields(rewind_stack_len = rewind_stack.len(), store_data_len = store_data.len()))]
#[must_use = "the action must be passed to the call loop"]
pub fn rewind_ext<M: MemorySize>(
    ctx: &mut FunctionEnvMut<WasiEnv>,
    memory_stack: Option<Bytes>,
    rewind_stack: Bytes,
    store_data: Bytes,
    rewind_result: RewindResultType,
) -> Errno {
    // Store the memory stack so that it can be restored later
    ctx.data_mut().thread.set_rewind(RewindResult {
        memory_stack,
        rewind_result,
    });

    // Deserialize the store data back into a snapshot
    let store_snapshot = match StoreSnapshot::deserialize(&store_data[..]) {
        Ok(a) => a,
        Err(err) => {
            warn!("snapshot restore failed - the store snapshot could not be deserialized");
            return Errno::Unknown;
        }
    };
    crate::utils::store::restore_store_snapshot(ctx, &store_snapshot);
    let env = ctx.data();
    let memory = match env.try_memory_view(&ctx) {
        Some(v) => v,
        None => {
            warn!("snapshot restore failed - unable to access the memory of the instance");
            return Errno::Unknown;
        }
    };

    // Write the addresses to the start of the stack space
    let rewind_pointer = env.layout.stack_lower;
    let rewind_data_start =
        rewind_pointer + (std::mem::size_of::<__wasi_asyncify_t<M::Offset>>() as u64);
    let rewind_data_end = rewind_data_start + (rewind_stack.len() as u64);
    if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
        eprintln!(
            "sinomo-debug rewind_ext pid={} tid={} stack_lower={} stack_upper={} rewind_len={} rewind_end={}",
            env.pid().raw(),
            env.tid().raw(),
            env.layout.stack_lower,
            env.layout.stack_upper,
            rewind_stack.len(),
            rewind_data_end
        );
    }
    if rewind_data_end > env.layout.stack_upper {
        warn!(
            "attempting to rewind a stack bigger than the allocated stack space ({} > {})",
            rewind_data_end, env.layout.stack_upper
        );
        return Errno::Overflow;
    }
    let rewind_data = __wasi_asyncify_t::<M::Offset> {
        start: wasi_try!(rewind_data_end.try_into().map_err(|_| Errno::Overflow)),
        end: wasi_try!(
            env.layout
                .stack_upper
                .try_into()
                .map_err(|_| Errno::Overflow)
        ),
    };
    let rewind_data_ptr: WasmPtr<__wasi_asyncify_t<M::Offset>, M> = WasmPtr::new(wasi_try!(
        rewind_pointer.try_into().map_err(|_| Errno::Overflow)
    ));
    wasi_try_mem!(rewind_data_ptr.write(&memory, rewind_data));

    // Copy the data to the address
    let rewind_stack_ptr = WasmPtr::<u8, M>::new(wasi_try!(
        rewind_data_start.try_into().map_err(|_| Errno::Overflow)
    ));
    wasi_try_mem!(
        rewind_stack_ptr
            .slice(
                &memory,
                wasi_try!(rewind_stack.len().try_into().map_err(|_| Errno::Overflow))
            )
            .and_then(|stack| { stack.write_slice(&rewind_stack[..]) })
    );

    // Invoke the callback that will prepare to rewind
    let asyncify_data = wasi_try!(rewind_pointer.try_into().map_err(|_| Errno::Overflow));
    if let Some(asyncify_start_rewind) = env
        .inner()
        .main_module_instance_handles()
        .asyncify_start_rewind
        .clone()
    {
        asyncify_start_rewind.call(ctx, asyncify_data);
    } else {
        warn!(
            "failed to rewind the stack because the asyncify_start_rewind export is missing or inaccessible"
        );
        return Errno::Noexec;
    }

    Errno::Success
}

pub fn rewind_ext2(
    ctx: &mut FunctionEnvMut<WasiEnv>,
    rewind_state: RewindStateOption,
) -> Result<(), ExitCode> {
    if let Some((rewind_state, rewind_result)) = rewind_state {
        tracing::trace!("Rewinding");
        let errno = if rewind_state.is_64bit {
            crate::rewind_ext::<wasmer_types::Memory64>(
                ctx,
                Some(rewind_state.memory_stack),
                rewind_state.rewind_stack,
                rewind_state.store_data,
                rewind_result,
            )
        } else {
            crate::rewind_ext::<wasmer_types::Memory32>(
                ctx,
                Some(rewind_state.memory_stack),
                rewind_state.rewind_stack,
                rewind_state.store_data,
                rewind_result,
            )
        };

        if errno != Errno::Success {
            let exit_code = ExitCode::from(errno);
            ctx.data().blocking_on_exit(Some(exit_code));
            return Err(exit_code);
        }
    }

    Ok(())
}

pub fn anyhow_err_to_runtime_err(err: anyhow::Error) -> WasiRuntimeError {
    WasiRuntimeError::Runtime(RuntimeError::user(err.into()))
}

pub(crate) unsafe fn handle_rewind<M: MemorySize, T>(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
) -> Option<T>
where
    T: serde::de::DeserializeOwned,
{
    unsafe { handle_rewind_ext::<M, T>(ctx, HandleRewindType::ResultDriven) }.flatten()
}

pub(crate) enum HandleRewindType {
    /// Handle rewind types that have a result to be processed
    ResultDriven,
    /// Handle rewind types that are result-less (generally these
    /// are caused by snapshot events)
    ResultLess,
}

pub(crate) unsafe fn handle_rewind_ext_with_default<M: MemorySize, T>(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    type_: HandleRewindType,
) -> Option<T>
where
    T: serde::de::DeserializeOwned + Default,
{
    let ret = unsafe { handle_rewind_ext::<M, T>(ctx, type_) };
    ret.unwrap_or_default()
}

pub(crate) unsafe fn handle_rewind_ext<M: MemorySize, T>(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    type_: HandleRewindType,
) -> Option<Option<T>>
where
    T: serde::de::DeserializeOwned,
{
    let env = ctx.data();
    if !env.thread.has_rewind_of_type(type_) {
        return None;
    };

    // If the stack has been restored
    let tid = env.tid();
    let pid = env.pid();
    if let Some(result) = ctx.data_mut().thread.take_rewind() {
        // Deserialize the result
        let memory_stack = result.memory_stack;

        // Notify asyncify that we are no longer rewinding
        let env = ctx.data();
        if let Some(asyncify_stop_rewind) = env
            .inner()
            .main_module_instance_handles()
            .asyncify_stop_rewind
            .clone()
        {
            asyncify_stop_rewind.call(ctx);
        } else {
            warn!(
                "failed to handle rewind because the asyncify_start_rewind export is missing or inaccessible"
            );
            return Some(None);
        }

        // Restore the memory stack
        let (env, mut store) = ctx.data_and_store_mut();
        if let Some(memory_stack) = memory_stack {
            set_memory_stack::<M>(env, &mut store, memory_stack);
        }

        match result.rewind_result {
            RewindResultType::RewindRestart => {
                tracing::trace!(%pid, %tid, "rewind for syscall restart");
                None
            }
            RewindResultType::RewindWithoutResult => {
                tracing::trace!(%pid, %tid, "rewind with no result");
                Some(None)
            }
            RewindResultType::RewindWithResult(rewind_result) => {
                tracing::trace!(%pid, %tid, "rewind with result (data={})", rewind_result.len());
                let (ret, _) = bincode::serde::decode_from_slice(&rewind_result, config::legacy())
                    .expect("failed to deserialize the rewind result");
                Some(Some(ret))
            }
        }
    } else {
        tracing::trace!(%pid, %tid, "rewind miss");
        Some(None)
    }
}

// Function to prepare the WASI environment
pub(crate) fn _prepare_wasi(
    wasi_env: &mut WasiEnv,
    args: Option<Vec<String>>,
    envs: Option<Vec<(String, String)>>,
    signals: Option<Vec<SignalDisposition>>,
) {
    // Swap out the arguments with the new ones
    if let Some(args) = args {
        *wasi_env.state.args.lock().unwrap() = args;
    }

    // Update the env vars
    if let Some(envs) = envs {
        let envs = conv_env_vars(
            envs.into_iter()
                .map(|(key, val)| (key, val.into_bytes()))
                .collect(),
        );
        *wasi_env.state.envs.lock().unwrap() = envs;
    }

    if let Some(signals) = signals {
        let mut guard = wasi_env.state.signals.lock().unwrap();
        for signal in signals {
            match signal.disp {
                Disposition::Default => {
                    guard.remove(&signal.sig);
                }
                Disposition::Ignore => {
                    guard.insert(signal.sig, signal.disp);
                }
                _ => {
                    guard.insert(signal.sig, signal.disp);
                }
            }
        }
        drop(guard);
    }

    sync_process_metadata(wasi_env);
}

pub(crate) fn sync_process_metadata(wasi_env: &WasiEnv) {
    let args = wasi_env.state.args.lock().unwrap().clone();
    let envs = wasi_env.state.envs.lock().unwrap().clone();
    let cwd = wasi_env.state.fs.current_dir.lock().unwrap().clone();
    let mut fd_infos = wasi_env
        .state
        .fs
        .fd_map
        .read()
        .unwrap()
        .iter()
        .map(|(fd, fd_entry)| {
            let target = process_fd_target(fd, fd_entry);
            crate::os::task::process::ProcessFdInfo {
                fd,
                pos: fd_entry
                    .inner
                    .offset
                    .load(std::sync::atomic::Ordering::SeqCst),
                flags: process_fdinfo_flags(fd_entry),
                target,
            }
        })
        .collect::<Vec<_>>();
    fd_infos.sort_unstable_by_key(|info| info.fd);
    wasi_env.process.set_metadata(args, envs, cwd, fd_infos);
}

fn process_fd_target(fd: WasiFd, fd_entry: &Fd) -> String {
    let inode_name = fd_entry.inode.name.read().unwrap().clone();
    let target_kind = match fd_entry.inode.read().deref() {
        Kind::File { .. } | Kind::Buffer { .. } => {
            return process_fd_path_target(&inode_name).unwrap_or_else(|| format!("file:[{fd}]"));
        }
        Kind::Dir { .. } | Kind::Root { .. } => {
            return process_fd_path_target(&inode_name).unwrap_or_else(|| format!("dir:[{fd}]"));
        }
        Kind::Symlink { .. } => {
            return process_fd_path_target(&inode_name)
                .unwrap_or_else(|| format!("symlink:[{fd}]"));
        }
        Kind::Socket { .. } => "socket",
        Kind::PipeTx { .. } | Kind::PipeRx { .. } | Kind::DuplexPipe { .. } => "pipe",
        Kind::Epoll { .. } => "eventpoll",
        Kind::EventNotifications { .. } => "eventfd",
    };
    format!("{target_kind}:[{fd}]")
}

fn process_fd_path_target(inode_name: &str) -> Option<String> {
    if inode_name.is_empty()
        || inode_name == "pipe"
        || inode_name == "event"
        || inode_name == "socket"
    {
        return None;
    }
    if inode_name.starts_with('/') {
        return Some(inode_name.to_string());
    }
    if inode_name == "."
        || inode_name == ".."
        || inode_name.starts_with("./")
        || inode_name.starts_with("../")
    {
        return None;
    }
    Some(format!("/{inode_name}"))
}

fn process_fdinfo_flags(fd_entry: &Fd) -> u32 {
    // Linux /proc/<pid>/fdinfo reports the access mode together with file status
    // flags in octal. Keep this sandbox-local but recognizable to POSIX tools.
    let mut flags = if fd_entry.open_flags & Fd::WRITE != 0 {
        if fd_entry.open_flags & Fd::READ != 0 {
            0o2
        } else {
            0o1
        }
    } else {
        0
    };
    if fd_entry.inner.flags.contains(Fdflags::APPEND) {
        flags |= 0o2000;
    }
    if fd_entry.inner.flags.contains(Fdflags::NONBLOCK) {
        flags |= 0o4000;
    }
    if fd_entry.inner.flags.contains(Fdflags::DSYNC) {
        flags |= 0o10000;
    }
    if fd_entry.inner.flags.contains(Fdflags::SYNC) {
        flags |= 0o4010000;
    }
    if fd_entry.inner.fd_flags.contains(Fdflagsext::CLOEXEC) {
        flags |= 0o2000000;
    }
    flags
}

pub(crate) fn conv_spawn_err_to_errno(err: &SpawnError) -> Errno {
    match err {
        SpawnError::AccessDenied => Errno::Access,
        SpawnError::Unsupported => Errno::Noexec,
        _ if err.is_not_found() => Errno::Noent,
        _ => Errno::Inval,
    }
}

pub(crate) fn conv_spawn_err_to_exit_code(err: &SpawnError) -> ExitCode {
    conv_spawn_err_to_errno(err).into()
}
