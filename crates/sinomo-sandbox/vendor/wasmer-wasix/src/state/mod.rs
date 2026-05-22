//! WARNING: the API exposed here is unstable and very experimental.  Certain things are not ready
//! yet and may be broken in patch releases.  If you're using this and have any specific needs,
//! please [let us know here](https://github.com/wasmerio/wasmer/issues/583) or by filing an issue.
//!
//! Wasmer always has a virtual root directory located at `/` at which all pre-opened directories can
//! be found.  It's possible to traverse between preopened directories this way as well (for example
//! `preopen-dir1/../preopen-dir2`).
//!
//! A preopened directory is a directory or directory + name combination passed into the
//! `generate_import_object` function.  These are directories that the caller has given
//! the WASI module permission to access.
//!
//! You can implement `VirtualFile` for your own types to get custom behavior and extend WASI, see the
//! [WASI plugin example](https://github.com/wasmerio/wasmer/blob/main/examples/plugin.rs).

#![allow(clippy::cognitive_complexity, clippy::too_many_arguments)]

mod builder;
pub mod context_switching;
mod env;
mod func_env;
mod handles;
mod linker;
mod shared_memory;
mod types;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, Condvar, Mutex},
    task::Waker,
    time::Duration,
};

#[cfg(feature = "enable-serde")]
use serde::{Deserialize, Serialize};
use virtual_fs::{FileOpener, FileSystem, FsError, OpenOptions, VirtualFile};
use wasmer_wasix_types::wasi::{
    Disposition, Errno, Fd as WasiFd, Rights, Signal, Snapshot0Clockid,
};

use crate::os::task::process::MemorySnapshotRegion;

pub use self::{
    builder::*,
    env::{WasiEnv, WasiEnvInit, WasiModuleInstanceHandles, WasiModuleTreeHandles},
    func_env::WasiFunctionEnv,
    types::*,
};
pub use crate::fs::{InodeGuard, InodeWeakGuard};
pub(crate) use shared_memory::{
    WasiSharedMemoryBacking, host_page_size, is_unsupported_error,
};
use crate::{
    fs::{WasiFs, WasiFsRoot, WasiInodes, WasiStateFileGuard, fs_error_into_wasi_err},
    syscalls::types::*,
    utils::WasiParkingLot,
};
pub(crate) use handles::*;
pub(crate) use linker::*;

/// all the rights enabled
pub const ALL_RIGHTS: Rights = Rights::all();

#[allow(dead_code)]
struct WasiStateOpener {
    root_fs: WasiFsRoot,
}

impl FileOpener for WasiStateOpener {
    fn open(
        &self,
        path: &Path,
        conf: &virtual_fs::OpenOptionsConfig,
    ) -> virtual_fs::Result<Box<dyn VirtualFile + Send + Sync + 'static>> {
        let mut new_options = self.root_fs.new_open_options();
        new_options.options(conf.clone());
        new_options.open(path)
    }
}

/// Represents a futex which will make threads wait for completion in a more
/// CPU efficient manner
#[derive(Debug, Default)]
pub struct WasiFutex {
    pub(crate) wakers: BTreeMap<u64, Option<Waker>>,
    pub(crate) masks: BTreeMap<u64, u32>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "enable-serde", derive(Serialize, Deserialize))]
pub(crate) struct WasiTimerFd {
    pub clock_id: Snapshot0Clockid,
    pub value_ns: u64,
    pub interval_ns: u64,
    pub armed_at_ns: u64,
    pub consumed: u64,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "enable-serde", derive(Serialize, Deserialize))]
pub(crate) struct WasiSignalFd {
    pub mask_low: u64,
    pub mask_high: u64,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "enable-serde", derive(Serialize, Deserialize))]
pub(crate) struct WasiPosixTimer {
    pub clock_id: Snapshot0Clockid,
    pub value_ns: u64,
    pub interval_ns: u64,
    pub armed_at_ns: u64,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "enable-serde", derive(Serialize, Deserialize))]
pub(crate) struct WasiIntervalTimer {
    pub value_ns: u64,
    pub interval_ns: u64,
    pub armed_at_ns: u64,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "enable-serde", derive(Serialize, Deserialize))]
pub(crate) struct WasiSignalStack {
    pub sp: u64,
    pub flags: i32,
    pub size: u64,
}

impl Default for WasiSignalStack {
    fn default() -> Self {
        Self {
            sp: 0,
            flags: 2,
            size: 0,
        }
    }
}

/// Structure that holds the state of BUS calls to this process and from
/// this process. BUS calls are the equivalent of RPC's with support
/// for all the major serializers
#[derive(Debug, Default)]
pub struct WasiBusState {
    poll_waker: WasiParkingLot,
}

impl WasiBusState {
    /// Gets a reference to the waker that can be used for
    /// asynchronous calls
    // TODO: review allow...
    #[allow(dead_code)]
    pub fn get_poll_waker(&self) -> Waker {
        self.poll_waker.get_waker()
    }

    /// Wakes one of the reactors thats currently waiting
    // TODO: review allow...
    #[allow(dead_code)]
    pub fn poll_wake(&self) {
        self.poll_waker.wake()
    }

    /// Will wait until either the reactor is triggered
    /// or the timeout occurs
    // TODO: review allow...
    #[allow(dead_code)]
    pub fn poll_wait(&self, timeout: Duration) -> bool {
        self.poll_waker.wait(timeout)
    }
}

/// Stores the state of the futexes
#[derive(Debug, Default)]
#[cfg_attr(feature = "enable-serde", derive(Serialize, Deserialize))]
pub(crate) struct WasiFutexState {
    pub poller_seed: u64,
    pub futexes: HashMap<u64, WasiFutex>,
    pub poller_futexes: BTreeMap<u64, u64>,
    pub poller_threads: BTreeMap<u64, u32>,
    pub woken_pollers: BTreeSet<u64>,
    pub interrupted_pollers: BTreeSet<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct WasiSharedMemoryMapping {
    pub offset: u64,
    pub len: u64,
    pub bytes: Arc<Mutex<Vec<u8>>>,
    pub process_snapshots: Arc<Mutex<HashMap<u32, Vec<u8>>>>,
    pub backing: Arc<WasiSharedMemoryBacking>,
    pub mapped_bases: Arc<Mutex<BTreeSet<(u32, u32, usize)>>>,
    pub fd: Option<i32>,
    pub file_key: Option<u64>,
    pub file_offset: u64,
    pub owner_pid: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WasiMemoryProtection {
    None,
    Read,
    ReadWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WasiMemoryProtectionRegion {
    pub pid: u32,
    pub offset: u64,
    pub len: u64,
    pub protection: WasiMemoryProtection,
}

impl WasiSharedMemoryMapping {
    pub(crate) fn read_bytes(&self) -> Result<Vec<u8>, Errno> {
        let len = usize::try_from(self.len).map_err(|_| Errno::Overflow)?;
        let mut cache = self.bytes.lock().unwrap();
        let bytes = self.backing.read(len).map_err(|err| {
            if is_unsupported_error(&err) {
                Errno::Notsup
            } else {
                Errno::Io
            }
        })?;
        if bytes.len() != len {
            return Err(Errno::Inval);
        }
        *cache = bytes.clone();
        Ok(bytes)
    }

    pub(crate) fn write_bytes(&self, bytes: &[u8]) -> Result<(), Errno> {
        let len = usize::try_from(self.len).map_err(|_| Errno::Overflow)?;
        if bytes.len() != len {
            return Err(Errno::Inval);
        }
        let mut cache = self.bytes.lock().unwrap();
        self.backing.write(bytes).map_err(|err| {
            if is_unsupported_error(&err) {
                Errno::Notsup
            } else {
                Errno::Io
            }
        })?;
        *cache = bytes.to_vec();
        Ok(())
    }

    pub(crate) fn read_range(&self, offset: usize, len: usize) -> Result<Vec<u8>, Errno> {
        let mapping_len = usize::try_from(self.len).map_err(|_| Errno::Overflow)?;
        if offset.saturating_add(len) > mapping_len {
            return Err(Errno::Inval);
        }
        let mut cache = self.bytes.lock().unwrap();
        let bytes = self.backing.read_at(offset, len).map_err(|err| {
            if is_unsupported_error(&err) {
                Errno::Notsup
            } else {
                Errno::Io
            }
        })?;
        if bytes.len() != len {
            return Err(Errno::Inval);
        }
        if cache.len() != mapping_len {
            cache.resize(mapping_len, 0);
        }
        cache[offset..offset + len].copy_from_slice(&bytes);
        Ok(bytes)
    }

    pub(crate) fn write_range(&self, offset: usize, bytes: &[u8]) -> Result<(), Errno> {
        let mapping_len = usize::try_from(self.len).map_err(|_| Errno::Overflow)?;
        if offset.saturating_add(bytes.len()) > mapping_len {
            return Err(Errno::Inval);
        }
        let mut cache = self.bytes.lock().unwrap();
        self.backing.write_at(offset, bytes).map_err(|err| {
            if is_unsupported_error(&err) {
                Errno::Notsup
            } else {
                Errno::Io
            }
        })?;
        if cache.len() != mapping_len {
            cache.resize(mapping_len, 0);
        }
        cache[offset..offset + bytes.len()].copy_from_slice(bytes);
        Ok(())
    }

    pub(crate) fn write_synchronized_range(
        &self,
        offset: usize,
        bytes: &[u8],
    ) -> Result<(), Errno> {
        self.write_range(offset, bytes)?;
        let mapping_len = usize::try_from(self.len).map_err(|_| Errno::Overflow)?;
        let mut snapshots = self.process_snapshots.lock().unwrap();
        for snapshot in snapshots.values_mut() {
            if snapshot.len() != mapping_len {
                snapshot.resize(mapping_len, 0);
            }
            snapshot[offset..offset + bytes.len()].copy_from_slice(bytes);
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub(crate) struct WasiSharedRwLockState {
    pub readers: u32,
    pub writer: bool,
    pub writer_owner: u32,
    pub writer_waiters: u32,
    pub max_writer_priority: i32,
    pub max_writer_policy: i32,
    pub writer_waiter_priorities: Vec<(i32, i32)>,
    pub reader_waiter_priorities: Vec<(i32, i32)>,
    pub max_reader_priority: i32,
}

#[derive(Debug, Default)]
pub(crate) struct WasiSharedRwLock {
    pub state: Mutex<WasiSharedRwLockState>,
    pub waiters: Condvar,
}

#[derive(Debug, Default)]
pub(crate) struct WasiSharedSemaphore {
    pub value: Mutex<u32>,
    pub waiters: Condvar,
}

#[derive(Debug, Default)]
pub(crate) struct WasiSharedCondWaitState {
    pub waiters_by_tid: BTreeSet<u32>,
    pub interrupted_tids: BTreeSet<u32>,
}

#[derive(Debug, Default)]
pub(crate) struct WasiSharedCondWait {
    pub state: Mutex<WasiSharedCondWaitState>,
    pub waiters: Condvar,
}

#[derive(Debug, Default)]
pub(crate) struct WasiSharedMutexWaitState {
    pub waiters_by_pid: HashMap<u32, u32>,
    pub handoff_pid: Option<u32>,
}

#[derive(Debug, Default)]
pub(crate) struct WasiSharedMutexWait {
    pub state: Mutex<WasiSharedMutexWaitState>,
    pub waiters: Condvar,
}

#[derive(Debug, Clone)]
pub(crate) struct WasiMessageQueueMessage {
    pub priority: u32,
    pub bytes: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct WasiMessageQueue {
    pub max_messages: u64,
    pub message_size: u64,
    pub messages: VecDeque<WasiMessageQueueMessage>,
    pub blocked_receivers: usize,
    pub notification: Option<WasiMessageQueueNotification>,
}

#[derive(Debug, Clone)]
pub(crate) struct WasiMessageQueueNotification {
    pub pid: u32,
    pub descriptor: i32,
    pub signal: Signal,
}

#[derive(Debug)]
pub(crate) struct WasiMessageQueueHandle {
    pub state: Mutex<WasiMessageQueue>,
    pub send_waiters: Condvar,
    pub receive_waiters: Condvar,
}

#[derive(Debug, Clone)]
pub(crate) struct WasiMessageQueueDescriptor {
    pub flags: i32,
    pub queue: Arc<WasiMessageQueueHandle>,
}

#[derive(Debug, Default)]
pub(crate) struct WasiMessageQueueState {
    pub next_descriptor: i32,
    pub queues: HashMap<String, Arc<WasiMessageQueueHandle>>,
    pub descriptors: HashMap<i32, WasiMessageQueueDescriptor>,
}

/// Top level data type containing all* the state with which WASI can
/// interact.
///
/// * The contents of files are not stored and may be modified by
///   other, concurrently running programs.  Data such as the contents
///   of directories are lazily loaded.
#[derive(Debug)]
#[cfg_attr(feature = "enable-serde", derive(Serialize, Deserialize))]
pub(crate) struct WasiState {
    pub secret: [u8; 32],

    pub fs: WasiFs,
    pub inodes: WasiInodes,
    pub futexs: Arc<Mutex<WasiFutexState>>,
    pub shared_futex_ranges: Arc<Mutex<Vec<MemorySnapshotRegion>>>,
    pub shared_mutex_owners: Arc<Mutex<HashMap<u64, u32>>>,
    pub shared_mutex_owner_pids: Arc<Mutex<HashMap<u64, u32>>>,
    pub shared_mutex_depths: Arc<Mutex<HashMap<u64, u32>>>,
    pub shared_mutex_waits: Arc<Mutex<HashMap<u64, Arc<WasiSharedMutexWait>>>>,
    pub shared_cond_sequences: Arc<Mutex<HashMap<u64, u32>>>,
    pub shared_cond_waits: Arc<Mutex<HashMap<u64, Arc<WasiSharedCondWait>>>>,
    pub shared_rwlocks: Arc<Mutex<HashMap<u64, Arc<WasiSharedRwLock>>>>,
    pub shared_semaphores: Arc<Mutex<HashMap<u64, Arc<WasiSharedSemaphore>>>>,
    pub shared_memory_cross_process: Arc<AtomicBool>,
    pub pidfds: Mutex<HashMap<WasiFd, u32>>,
    pub timerfds: Mutex<HashMap<WasiFd, WasiTimerFd>>,
    pub signalfds: Mutex<HashMap<WasiFd, WasiSignalFd>>,
    pub posix_timers: Mutex<HashMap<u32, WasiPosixTimer>>,
    pub interval_timers: Mutex<HashMap<i32, WasiIntervalTimer>>,
    pub message_queues: Arc<Mutex<WasiMessageQueueState>>,
    pub shared_memory_mappings: Arc<Mutex<Vec<WasiSharedMemoryMapping>>>,
    pub memory_protections: Arc<Mutex<Vec<WasiMemoryProtectionRegion>>>,
    pub signal_stack: Mutex<WasiSignalStack>,
    pub clock_offset: Mutex<HashMap<Snapshot0Clockid, i64>>,
    pub args: Mutex<Vec<String>>,
    pub envs: Mutex<Vec<Vec<u8>>>,
    pub signals: Mutex<HashMap<Signal, Disposition>>,

    // TODO: should not be here, since this requires active work to resolve.
    // State should only hold active runtime state that can be reproducibly re-created.
    pub preopen: Vec<String>,
}

impl WasiState {
    // fn new(fs: WasiFs, inodes: Arc<RwLock<WasiInodes>>) -> Self {
    //     WasiState {
    //         fs,
    //         secret: rand::thread_rng().gen::<[u8; 32]>(),
    //         inodes,
    //         args: Vec::new(),
    //         preopen: Vec::new(),
    //         threading: Default::default(),
    //         futexs: Default::default(),
    //         clock_offset: Default::default(),
    //         envs: Vec::new(),
    //     }
    // }
}

// Implementations of direct to FS calls so that we can easily change their implementation
impl WasiState {
    pub(crate) fn register_shared_memory_mapping(
        &self,
        owner_pid: u32,
        offset: u64,
        len: u64,
        bytes: Vec<u8>,
    ) -> Result<(), Errno> {
        self.register_shared_memory_mapping_inner(owner_pid, offset, len, bytes, None, 0)
    }

    pub(crate) fn register_shared_memory_mapping_with_file(
        &self,
        owner_pid: u32,
        offset: u64,
        len: u64,
        bytes: Vec<u8>,
        fd: i32,
        file_offset: u64,
    ) -> Result<(), Errno> {
        self.register_shared_memory_mapping_inner(owner_pid, offset, len, bytes, Some(fd), file_offset)
    }

    fn register_shared_memory_mapping_inner(
        &self,
        owner_pid: u32,
        offset: u64,
        len: u64,
        bytes: Vec<u8>,
        fd: Option<i32>,
        file_offset: u64,
    ) -> Result<(), Errno> {
        if len == 0 {
            return Ok(());
        }
        let file_key = fd.and_then(|fd| {
            let fd = WasiFd::try_from(fd).ok()?;
            let fd_map = self.fs.fd_map.read().ok()?;
            fd_map.get(fd).map(|entry| entry.inode.ino().as_u64())
        });
        let end = offset.saturating_add(len);
        let mut mappings = self.shared_memory_mappings.lock().unwrap();
        let shared_mapping = fd.and_then(|fd| {
            mappings
                .iter()
                .find(|mapping| {
                    (mapping.file_key.is_some() && mapping.file_key == file_key
                        || mapping.file_key.is_none() && mapping.fd == Some(fd))
                        && mapping.file_offset == file_offset
                        && mapping.len == len
                })
                .map(|mapping| {
                    (
                        mapping.bytes.clone(),
                        mapping.process_snapshots.clone(),
                        mapping.backing.clone(),
                        mapping.mapped_bases.clone(),
                    )
                })
        });
        if shared_mapping.is_none() {
            self.shared_mutex_owners
                .lock()
                .unwrap()
                .retain(|mutex_offset, _| *mutex_offset < offset || *mutex_offset >= end);
            self.shared_mutex_owner_pids
                .lock()
                .unwrap()
                .retain(|mutex_offset, _| *mutex_offset < offset || *mutex_offset >= end);
            self.shared_mutex_depths
                .lock()
                .unwrap()
                .retain(|mutex_offset, _| *mutex_offset < offset || *mutex_offset >= end);
            self.shared_mutex_waits
                .lock()
                .unwrap()
                .retain(|mutex_offset, _| *mutex_offset < offset || *mutex_offset >= end);
            self.shared_cond_sequences
                .lock()
                .unwrap()
                .retain(|cond_offset, _| *cond_offset < offset || *cond_offset >= end);
            self.shared_cond_waits
                .lock()
                .unwrap()
                .retain(|cond_offset, _| *cond_offset < offset || *cond_offset >= end);
            self.shared_rwlocks
                .lock()
                .unwrap()
                .retain(|rwlock_offset, _| *rwlock_offset < offset || *rwlock_offset >= end);
            self.shared_semaphores
                .lock()
                .unwrap()
                .retain(|semaphore_offset, _| {
                    *semaphore_offset < offset || *semaphore_offset >= end
                });
            self.shared_futex_ranges
                .lock()
                .unwrap()
                .retain(|range| range.end <= offset || range.start >= end);
        }
        let (shared_bytes, process_snapshots, backing, mapped_bases) = match shared_mapping {
            Some(shared_mapping) => shared_mapping,
            None => match WasiSharedMemoryBacking::new(&bytes) {
                Ok(backing) => (
                    Arc::new(Mutex::new(bytes.clone())),
                    Arc::new(Mutex::new(HashMap::new())),
                    Arc::new(backing),
                    Arc::new(Mutex::new(BTreeSet::new())),
                ),
                Err(err) => {
                    tracing::warn!(
                        error = &err as &dyn std::error::Error,
                        "host platform does not support in-memory WASIX shared mappings"
                    );
                    return Err(Errno::Notsup);
                }
            },
        };
        process_snapshots
            .lock()
            .unwrap()
            .insert(owner_pid, bytes.clone());
        mappings.retain(|mapping| {
            let mapping_end = mapping.offset.saturating_add(mapping.len);
            mapping.owner_pid != owner_pid || mapping_end <= offset || mapping.offset >= end
        });
        self.memory_protections.lock().unwrap().retain(|protection| {
            let protection_end = protection.offset.saturating_add(protection.len);
            protection.pid != owner_pid || protection_end <= offset || protection.offset >= end
        });
        mappings.push(WasiSharedMemoryMapping {
            offset,
            len,
            bytes: shared_bytes,
            process_snapshots,
            backing,
            mapped_bases,
            fd,
            file_key,
            file_offset,
            owner_pid,
        });
        Ok(())
    }

    pub(crate) fn inherit_shared_memory_mappings_for_child(&self, parent_pid: u32, child_pid: u32) {
        let mut mappings = self.shared_memory_mappings.lock().unwrap();
        let inherited_protections = {
            let protections = self.memory_protections.lock().unwrap();
            protections
                .iter()
                .filter(|protection| protection.pid == parent_pid)
                .map(|protection| {
                    let mut protection = *protection;
                    protection.pid = child_pid;
                    protection
                })
                .collect::<Vec<_>>()
        };
        let inherited = mappings
            .iter()
            .filter(|mapping| mapping.owner_pid == parent_pid)
            .cloned()
            .map(|mut mapping| {
                mapping.owner_pid = child_pid;
                let snapshot = mapping
                    .process_snapshots
                    .lock()
                    .unwrap()
                    .get(&parent_pid)
                    .cloned()
                    .unwrap_or_else(|| {
                        mapping
                            .read_bytes()
                            .unwrap_or_else(|_| mapping.bytes.lock().unwrap().clone())
                    });
                mapping
                    .process_snapshots
                    .lock()
                    .unwrap()
                    .insert(child_pid, snapshot);
                mapping
            })
            .collect::<Vec<_>>();
        mappings.extend(inherited);
        self.memory_protections
            .lock()
            .unwrap()
            .extend(inherited_protections);
    }

    pub(crate) fn record_memory_protection(
        &self,
        pid: u32,
        offset: u64,
        len: u64,
        protection: WasiMemoryProtection,
    ) {
        if len == 0 {
            return;
        }
        let Some(end) = offset.checked_add(len) else {
            return;
        };
        let mut protections = self.memory_protections.lock().unwrap();
        protections.retain(|existing| {
            let existing_end = existing.offset.saturating_add(existing.len);
            existing.pid != pid || existing_end <= offset || existing.offset >= end
        });
        if protection != WasiMemoryProtection::ReadWrite {
            protections.push(WasiMemoryProtectionRegion {
                pid,
                offset,
                len,
                protection,
            });
        }
    }

    pub(crate) fn memory_protections_for_pid_range(
        &self,
        pid: u32,
        offset: u64,
        len: u64,
    ) -> Vec<WasiMemoryProtectionRegion> {
        let Some(end) = offset.checked_add(len) else {
            return Vec::new();
        };
        self.memory_protections
            .lock()
            .unwrap()
            .iter()
            .filter(|protection| {
                let protection_end = protection.offset.saturating_add(protection.len);
                protection.pid == pid && protection.offset < end && protection_end > offset
            })
            .copied()
            .collect()
    }

    pub(crate) fn memory_access_allowed(
        &self,
        pid: u32,
        offset: u64,
        len: u64,
        write: bool,
    ) -> bool {
        if len == 0 {
            return true;
        }
        if self.memory_protections.lock().unwrap().is_empty() {
            return true;
        }
        self.memory_protections_for_pid_range(pid, offset, len)
            .into_iter()
            .all(|protection| match protection.protection {
                WasiMemoryProtection::None => false,
                WasiMemoryProtection::Read => !write,
                WasiMemoryProtection::ReadWrite => true,
            })
    }

    pub(crate) fn register_shared_futex_range(&self, offset: u64, len: u64) {
        if len == 0 {
            return;
        }
        self.shared_futex_ranges
            .lock()
            .unwrap()
            .push(MemorySnapshotRegion {
                start: offset,
                end: offset.saturating_add(len),
            });
    }

    pub(crate) fn unregister_shared_futex_range(&self, offset: u64, len: u64) {
        if len == 0 {
            return;
        }
        let end = offset.saturating_add(len);
        self.shared_futex_ranges
            .lock()
            .unwrap()
            .retain(|range| range.start != offset || range.end != end);
        self.shared_mutex_owners.lock().unwrap().remove(&offset);
        self.shared_mutex_owner_pids.lock().unwrap().remove(&offset);
        self.shared_mutex_depths.lock().unwrap().remove(&offset);
        self.shared_mutex_waits.lock().unwrap().remove(&offset);
        self.shared_cond_sequences.lock().unwrap().remove(&offset);
        self.shared_cond_waits.lock().unwrap().remove(&offset);
        self.shared_rwlocks.lock().unwrap().remove(&offset);
        self.shared_semaphores.lock().unwrap().remove(&offset);
    }

    pub(crate) fn is_shared_futex_offset(&self, offset: u64) -> bool {
        self.shared_futex_ranges
            .lock()
            .unwrap()
            .iter()
            .any(|range| offset >= range.start && offset < range.end)
    }

    pub(crate) fn shared_futex_ranges(&self) -> Vec<MemorySnapshotRegion> {
        self.shared_futex_ranges.lock().unwrap().clone()
    }

    pub(crate) fn has_shared_memory_mappings(&self) -> bool {
        !self.shared_memory_mappings.lock().unwrap().is_empty()
    }

    pub(crate) fn shared_memory_mappings(&self) -> Vec<WasiSharedMemoryMapping> {
        self.shared_memory_mappings.lock().unwrap().clone()
    }

    pub(crate) fn shared_memory_mappings_for_pid(&self, pid: u32) -> Vec<WasiSharedMemoryMapping> {
        self.shared_memory_mappings
            .lock()
            .unwrap()
            .iter()
            .filter(|mapping| mapping.owner_pid == pid)
            .cloned()
            .collect()
    }

    pub(crate) fn has_cross_process_shared_memory(&self) -> bool {
        self.shared_memory_cross_process.load(Ordering::SeqCst)
    }

    pub(crate) fn mark_cross_process_shared_memory(&self) {
        self.shared_memory_cross_process
            .store(true, Ordering::SeqCst);
    }

    pub(crate) fn fs_read_dir<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<virtual_fs::ReadDir, Errno> {
        self.fs
            .root_fs
            .read_dir(path.as_ref())
            .map_err(fs_error_into_wasi_err)
    }

    pub(crate) fn fs_create_dir<P: AsRef<Path>>(&self, path: P) -> Result<(), Errno> {
        self.fs
            .root_fs
            .create_dir(path.as_ref())
            .map_err(fs_error_into_wasi_err)
    }

    pub(crate) fn fs_remove_dir<P: AsRef<Path>>(&self, path: P) -> Result<(), Errno> {
        self.fs
            .root_fs
            .remove_dir(path.as_ref())
            .map_err(fs_error_into_wasi_err)
    }

    pub(crate) async fn fs_rename<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        from: P,
        to: Q,
    ) -> Result<(), Errno> {
        self.fs
            .root_fs
            .rename(from.as_ref(), to.as_ref())
            .await
            .map_err(fs_error_into_wasi_err)
    }

    pub(crate) fn fs_remove_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Errno> {
        self.fs
            .root_fs
            .remove_file(path.as_ref())
            .map_err(fs_error_into_wasi_err)
    }

    pub(crate) fn fs_new_open_options(&self) -> OpenOptions<'_> {
        self.fs.root_fs.new_open_options()
    }

    /// Turn the WasiState into bytes
    #[cfg(feature = "enable-serde")]
    pub fn freeze(&self) -> Option<Vec<u8>> {
        bincode::serialize(self).ok()
    }

    /// Get a WasiState from bytes
    #[cfg(feature = "enable-serde")]
    pub fn unfreeze(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }

    /// Get the `VirtualFile` object at stdout
    pub fn stdout(&self) -> Result<Option<Box<dyn VirtualFile + Send + Sync + 'static>>, FsError> {
        self.std_dev_get(__WASI_STDOUT_FILENO)
    }

    /// Get the `VirtualFile` object at stderr
    pub fn stderr(&self) -> Result<Option<Box<dyn VirtualFile + Send + Sync + 'static>>, FsError> {
        self.std_dev_get(__WASI_STDERR_FILENO)
    }

    /// Get the `VirtualFile` object at stdin
    pub fn stdin(&self) -> Result<Option<Box<dyn VirtualFile + Send + Sync + 'static>>, FsError> {
        self.std_dev_get(__WASI_STDIN_FILENO)
    }

    /// Internal helper function to get a standard device handle.
    /// Expects one of `__WASI_STDIN_FILENO`, `__WASI_STDOUT_FILENO`, `__WASI_STDERR_FILENO`.
    fn std_dev_get(
        &self,
        fd: WasiFd,
    ) -> Result<Option<Box<dyn VirtualFile + Send + Sync + 'static>>, FsError> {
        let ret = WasiStateFileGuard::new(self, fd)?.map(|a| {
            let ret = Box::new(a);
            let ret: Box<dyn VirtualFile + Send + Sync + 'static> = ret;
            ret
        });
        Ok(ret)
    }

    /// Forking the WasiState is used when either fork or vfork is called
    pub fn fork(&self) -> Self {
        let has_shared_memory_mappings = self.has_shared_memory_mappings();
        WasiState {
            fs: self.fs.fork(),
            secret: self.secret,
            inodes: self.inodes.clone(),
            futexs: if has_shared_memory_mappings {
                self.futexs.clone()
            } else {
                Default::default()
            },
            shared_futex_ranges: self.shared_futex_ranges.clone(),
            shared_mutex_owners: self.shared_mutex_owners.clone(),
            shared_mutex_owner_pids: self.shared_mutex_owner_pids.clone(),
            shared_mutex_depths: self.shared_mutex_depths.clone(),
            shared_mutex_waits: self.shared_mutex_waits.clone(),
            shared_cond_sequences: self.shared_cond_sequences.clone(),
            shared_cond_waits: self.shared_cond_waits.clone(),
            shared_rwlocks: self.shared_rwlocks.clone(),
            shared_semaphores: self.shared_semaphores.clone(),
            shared_memory_cross_process: self.shared_memory_cross_process.clone(),
            pidfds: Mutex::new(self.pidfds.lock().unwrap().clone()),
            timerfds: Mutex::new(self.timerfds.lock().unwrap().clone()),
            signalfds: Mutex::new(self.signalfds.lock().unwrap().clone()),
            posix_timers: Default::default(),
            interval_timers: Default::default(),
            message_queues: self.message_queues.clone(),
            shared_memory_mappings: self.shared_memory_mappings.clone(),
            memory_protections: self.memory_protections.clone(),
            signal_stack: Mutex::new(self.signal_stack.lock().unwrap().clone()),
            clock_offset: Mutex::new(self.clock_offset.lock().unwrap().clone()),
            args: Mutex::new(self.args.lock().unwrap().clone()),
            envs: Mutex::new(self.envs.lock().unwrap().clone()),
            signals: Mutex::new(self.signals.lock().unwrap().clone()),
            preopen: self.preopen.clone(),
        }
    }
}
