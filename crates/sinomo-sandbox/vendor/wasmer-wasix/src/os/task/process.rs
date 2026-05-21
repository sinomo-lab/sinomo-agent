use crate::{WasiEnv, WasiRuntimeError, journal::SnapshotTrigger};
#[cfg(feature = "journal")]
use crate::{WasiResult, journal::JournalEffector, syscalls::do_checkpoint_from_outside, unwind};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    convert::TryInto,
    ops::Range,
    sync::{
        Arc, Condvar, Mutex, MutexGuard,
        atomic::{AtomicU32, Ordering},
    },
    task::Waker,
    time::Duration,
};
use tracing::trace;
use wasmer::FunctionEnvMut;
use wasmer_types::ModuleHash;
use wasmer_wasix_types::{
    types::Signal,
    wasi::{Errno, ExitCode, Fd as WasiFd, Snapshot0Clockid},
    wasix::ThreadStartType,
};

use crate::{
    WasiThread, WasiThreadHandle, WasiThreadId, os::task::signal::WasiSignalInterval,
    syscalls::platform_clock_time_get,
};

use super::{
    TaskStatus,
    backoff::WasiProcessCpuBackoff,
    control_plane::{ControlPlaneError, WasiControlPlaneHandle},
    signal::{SignalDeliveryError, SignalHandlerAbi},
    task_join_handle::OwnedTaskStatus,
    thread::WasiMemoryLayout,
};

macro_rules! sinomo_debug {
    ($($arg:tt)*) => {
        if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
            eprintln!($($arg)*);
        }
    };
}

/// Represents the ID of a sub-process
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WasiProcessId(u32);

impl WasiProcessId {
    pub fn raw(&self) -> u32 {
        self.0
    }
}

impl From<i32> for WasiProcessId {
    fn from(id: i32) -> Self {
        Self(id as u32)
    }
}

impl From<WasiProcessId> for i32 {
    fn from(val: WasiProcessId) -> Self {
        val.0 as i32
    }
}

impl From<u32> for WasiProcessId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

impl From<WasiProcessId> for u32 {
    fn from(val: WasiProcessId) -> Self {
        val.0
    }
}

impl std::fmt::Display for WasiProcessId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Debug for WasiProcessId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub type LockableWasiProcessInner = Arc<(Mutex<WasiProcessInner>, Condvar)>;

fn default_namespace_ids() -> HashMap<String, u64> {
    [
        "cgroup",
        "ipc",
        "mnt",
        "net",
        "pid",
        "pid_for_children",
        "time",
        "uts",
    ]
    .into_iter()
    .map(|namespace| (namespace.to_string(), 1))
    .collect()
}

fn namespace_id_for(pid: u64, namespace: &str) -> u64 {
    let namespace_tag = match namespace {
        "cgroup" => 1,
        "ipc" => 2,
        "mnt" => 3,
        "net" => 4,
        "pid" | "pid_for_children" => 5,
        "time" => 6,
        "uts" => 7,
        _ => 0,
    };
    (pid << 8) | namespace_tag
}

/// Represents a process running within the compute state
/// TODO: fields should be private and only accessed via methods.
#[derive(Debug, Clone)]
pub struct WasiProcess {
    /// Unique ID of this process
    pub(crate) pid: WasiProcessId,
    /// Process ID of the parent process, if this process was spawned by another guest process.
    pub(crate) parent_pid: Option<WasiProcessId>,
    /// Process group ID used by procfs and future job-control behavior.
    pub(crate) process_group_id: WasiProcessId,
    /// Session ID used by procfs and future job-control behavior.
    pub(crate) session_id: WasiProcessId,
    /// Hash of the module that this process is using
    pub(crate) module_hash: ModuleHash,
    /// The inner protected region of the process with a conditional
    /// variable that is used for coordination such as snapshots.
    pub(crate) inner: LockableWasiProcessInner,
    /// Reference back to the compute engine
    // TODO: remove this reference, access should happen via separate state instead
    // (we don't want cyclical references)
    pub(crate) compute: WasiControlPlaneHandle,
    /// Reference to the exit code for the main thread
    pub(crate) finished: Arc<OwnedTaskStatus>,
    /// Number of threads waiting for children to exit
    pub(crate) waiting: Arc<AtomicU32>,
    /// Number of tokens that are currently active and thus
    /// the exponential backoff of CPU is halted (as in CPU
    /// is allowed to run freely)
    pub(crate) cpu_run_tokens: Arc<AtomicU32>,
}

/// Represents a freeze of all threads to perform some action
/// on the total state-machine. This is normally done for
/// things like snapshots which require the memory to remain
/// stable while it performs a diff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WasiProcessCheckpoint {
    /// No checkpoint will take place and the process
    /// should just execute as per normal
    Execute,
    /// The process needs to take a snapshot of the
    /// memory and state-machine
    Snapshot { trigger: SnapshotTrigger },
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemorySnapshotRegion {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone)]
pub struct ProcessFdInfo {
    pub fd: WasiFd,
    pub pos: u64,
    pub flags: u32,
    pub target: String,
}

#[derive(Debug, Clone)]
pub struct ProcessIdentity {
    pub real_uid: u32,
    pub effective_uid: u32,
    pub saved_uid: u32,
    pub real_gid: u32,
    pub effective_gid: u32,
    pub saved_gid: u32,
    pub groups: Vec<u32>,
}

impl Default for ProcessIdentity {
    fn default() -> Self {
        Self {
            real_uid: 0,
            effective_uid: 0,
            saved_uid: 0,
            real_gid: 0,
            effective_gid: 0,
            saved_gid: 0,
            groups: vec![0],
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ProcessResourceLimit {
    pub current: u64,
    pub maximum: u64,
}

const RLIM_INFINITY: u64 = u64::MAX;
const RLIMIT_CPU: i32 = 0;
const RLIMIT_FSIZE: i32 = 1;
const RLIMIT_CORE: i32 = 4;
const RLIMIT_DATA: i32 = 2;
const RLIMIT_RSS: i32 = 5;
const RLIMIT_AS: i32 = 9;
const RLIMIT_MEMLOCK: i32 = 8;
const RLIMIT_STACK: i32 = 3;
const RLIMIT_NOFILE: i32 = 7;
const RLIMIT_NPROC: i32 = 6;
const DEFAULT_SOFT_TASK_LIMIT: u64 = 64;

fn default_resource_limit(resource: i32, envs: &[Vec<u8>]) -> ProcessResourceLimit {
    let memory_limit =
        process_env_u64(envs, "SINOMO_SANDBOX_MEMORY_LIMIT_BYTES", 512 * 1024 * 1024);
    let wall_time_ms = process_env_u64(envs, "SINOMO_SANDBOX_WALL_TIME_LIMIT_MS", 30_000);
    let wall_time_seconds = if wall_time_ms == RLIM_INFINITY {
        RLIM_INFINITY
    } else {
        wall_time_ms.saturating_add(999) / 1_000
    };
    let value = match resource {
        RLIMIT_CPU => wall_time_seconds,
        RLIMIT_CORE => 0,
        RLIMIT_DATA | RLIMIT_RSS | RLIMIT_AS | RLIMIT_MEMLOCK => memory_limit,
        RLIMIT_STACK => 8 * 1024 * 1024,
        RLIMIT_NOFILE => 1024,
        RLIMIT_NPROC => DEFAULT_SOFT_TASK_LIMIT,
        _ => RLIM_INFINITY,
    };
    let maximum = match resource {
        RLIMIT_STACK => memory_limit,
        _ => value,
    };
    ProcessResourceLimit {
        current: value,
        maximum,
    }
}

fn sandbox_resource_limits(
    memory_limit_bytes: u64,
    wall_time_ms: u64,
    task_limit: u64,
) -> HashMap<i32, ProcessResourceLimit> {
    let wall_time_seconds = if wall_time_ms == RLIM_INFINITY {
        RLIM_INFINITY
    } else {
        wall_time_ms.saturating_add(999) / 1_000
    };
    let mut limits = HashMap::new();
    limits.insert(
        RLIMIT_CPU,
        ProcessResourceLimit {
            current: wall_time_seconds,
            maximum: wall_time_seconds,
        },
    );
    for resource in [RLIMIT_DATA, RLIMIT_RSS, RLIMIT_AS, RLIMIT_MEMLOCK] {
        limits.insert(
            resource,
            ProcessResourceLimit {
                current: memory_limit_bytes,
                maximum: memory_limit_bytes,
            },
        );
    }
    limits.insert(
        RLIMIT_STACK,
        ProcessResourceLimit {
            current: 8 * 1024 * 1024,
            maximum: memory_limit_bytes,
        },
    );
    limits.insert(
        RLIMIT_CORE,
        ProcessResourceLimit {
            current: 0,
            maximum: 0,
        },
    );
    limits.insert(
        RLIMIT_FSIZE,
        ProcessResourceLimit {
            current: RLIM_INFINITY,
            maximum: RLIM_INFINITY,
        },
    );
    limits.insert(
        RLIMIT_NOFILE,
        ProcessResourceLimit {
            current: 1024,
            maximum: 1024,
        },
    );
    limits.insert(
        RLIMIT_NPROC,
        ProcessResourceLimit {
            current: task_limit.min(DEFAULT_SOFT_TASK_LIMIT),
            maximum: task_limit,
        },
    );
    limits
}

fn process_env_u64(envs: &[Vec<u8>], name: &str, fallback: u64) -> u64 {
    let prefix = format!("{name}=");
    envs.iter()
        .find_map(|entry| {
            let entry = std::str::from_utf8(entry).ok()?;
            let value = entry.strip_prefix(&prefix)?;
            value.parse().ok()
        })
        .unwrap_or(fallback)
}

impl From<Range<u64>> for MemorySnapshotRegion {
    fn from(value: Range<u64>) -> Self {
        Self {
            start: value.start,
            end: value.end,
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<Range<u64>> for MemorySnapshotRegion {
    fn into(self) -> Range<u64> {
        self.start..self.end
    }
}

// TODO: fields should be private and only accessed via methods.
#[derive(Debug)]
pub struct WasiProcessInner {
    /// Unique ID of this process
    pub pid: WasiProcessId,
    /// Process ID of the parent process, if this process was spawned by another guest process.
    pub parent_pid: Option<WasiProcessId>,
    /// Process group ID used by procfs and job-control behavior.
    pub process_group_id: WasiProcessId,
    /// Session ID used by procfs and job-control behavior.
    pub session_id: WasiProcessId,
    /// Stop signal already reported through proc_join with WAKE_STOPPED.
    pub reported_stopped_signal: Option<Signal>,
    /// Continued status waiting to be reported through POSIX wait WCONTINUED.
    pub unreported_continued: bool,
    /// POSIX command name as set by `prctl(PR_SET_NAME)`.
    pub process_name: Option<String>,
    /// Guest-visible UTS node name returned by `uname` and `gethostname`.
    pub hostname: String,
    /// Guest-visible UTS domain name returned by `uname` and `getdomainname`.
    pub domainname: String,
    /// Guest-visible dumpable process flag.
    pub dumpable: bool,
    /// Guest-visible no-new-privileges process flag.
    pub no_new_privs: bool,
    /// Guest-visible seccomp mode metadata.
    pub seccomp_mode: u32,
    /// Guest-visible timer slack in nanoseconds.
    pub timer_slack_nanos: u64,
    /// Guest-visible Linux personality metadata.
    pub personality: u64,
    /// Guest address for clear-child-TID futex metadata.
    pub clear_child_tid: u64,
    /// Guest address and length for robust futex-list metadata.
    pub robust_list: Option<(u64, u64)>,
    /// Guest-visible Linux capability masks.
    pub cap_effective: u64,
    pub cap_permitted: u64,
    pub cap_inheritable: u64,
    pub cap_bounding: u64,
    /// Guest-visible Linux prctl process-control metadata.
    pub keepcaps: bool,
    pub securebits: u32,
    pub timing_mode: u32,
    pub thp_disable: u32,
    pub io_flusher: bool,
    pub ptracer: i32,
    /// Guest-visible sandbox namespace inode ids by namespace name.
    pub namespace_ids: HashMap<String, u64>,
    /// Guest-visible architecture TLS-base and restartable-sequence metadata.
    pub arch_fs_base: u64,
    pub arch_gs_base: u64,
    pub rseq: Option<(u64, u32, u32)>,
    /// Guest-visible NUMA and I/O-priority metadata.
    pub mempolicy_mode: i32,
    pub mempolicy_nodes: u64,
    pub mempolicy_home_node: u64,
    pub ioprio: i32,
    /// Signal delivered to this process when its parent exits.
    pub parent_death_signal: Signal,
    /// Whether orphaned descendant processes should reparent to this process.
    pub child_subreaper: bool,
    /// POSIX file creation mask for this process.
    pub umask: u32,
    /// Guest-visible POSIX nice value. This is scheduling metadata for POSIX
    /// compatibility and does not change host scheduler priority.
    pub nice: i32,
    /// Guest-visible POSIX scheduler policy.
    pub scheduler_policy: i32,
    /// Guest-visible POSIX scheduler priority.
    pub scheduler_priority: i32,
    /// Guest-visible CPU affinity mask. The runtime currently exposes a
    /// single virtual CPU by default, while keeping the metadata mutable for
    /// POSIX callers that save and restore affinity.
    pub cpu_affinity: u128,
    /// Guest-visible current CPU number.
    pub current_cpu: u32,
    /// Guest-visible POSIX resource limit overrides.
    pub resource_limits: HashMap<i32, ProcessResourceLimit>,
    /// Process-global WASIX thread-local storage keys. Values are stored per
    /// thread on `WasiThread`.
    pub thread_local_keys: HashMap<u32, u64>,
    pub next_thread_local_key: u32,
    /// Accumulated elapsed user-time proxy for reaped child processes.
    pub child_user_nanos: u128,
    /// Maximum resident-set proxy reported by reaped child processes.
    pub child_maxrss_kib: u64,
    /// Guest-visible POSIX identity. This is sandbox metadata only and is never
    /// mapped to host credentials.
    pub identity: ProcessIdentity,
    /// Number of threads waiting for children to exit
    pub(crate) waiting: Arc<AtomicU32>,
    /// The threads that make up this process
    pub threads: HashMap<WasiThreadId, WasiThread>,
    /// Number of threads running for this process
    pub thread_count: u32,
    /// Signals that will be triggered at specific intervals
    pub signal_intervals: HashMap<Signal, WasiSignalInterval>,
    /// Process-directed signals that could not be delivered because every
    /// thread had the signal masked when the signal arrived.
    pub pending_process_signals: Vec<Signal>,
    /// List of all the children spawned from this thread
    pub children: Vec<WasiProcess>,
    /// Guest argument vector for procfs and process introspection.
    pub args: Vec<String>,
    /// Guest environment entries in `KEY=VALUE` form for procfs.
    pub envs: Vec<Vec<u8>>,
    /// Guest current working directory.
    pub cwd: String,
    /// Number of guest file descriptors currently known for procfs introspection.
    pub fd_size: usize,
    /// Guest file descriptor metadata currently known for procfs introspection.
    pub fd_infos: Vec<ProcessFdInfo>,
    /// Guest executable path selected when the process was spawned or exec'd.
    pub executable_path: Option<String>,
    /// Monotonic process start time in nanoseconds.
    pub start_time_nanos: u128,
    /// Represents a checkpoint which blocks all the threads
    /// and then executes some maintenance action
    pub checkpoint: WasiProcessCheckpoint,
    /// If true then the journaling will be disabled after the
    /// next snapshot is taken
    pub disable_journaling_after_checkpoint: bool,
    /// If true then the process will stop running after the
    /// next snapshot is taken
    pub stop_running_after_checkpoint: bool,
    /// List of situations that the process will checkpoint on
    #[cfg(feature = "journal")]
    pub snapshot_on: HashSet<SnapshotTrigger>,
    /// Any wakers waiting on this process (for example for a checkpoint)
    pub wakers: Vec<Waker>,
    /// The snapshot memory significantly reduce the amount of
    /// duplicate entries in the journal for memory that has not changed
    #[cfg(feature = "journal")]
    pub snapshot_memory_hash: HashMap<MemorySnapshotRegion, u64>,
    /// Represents all the backoff properties for this process
    /// which will be used to determine if the CPU should be
    /// throttled or not
    pub(super) backoff: WasiProcessCpuBackoff,
}

pub enum MaybeCheckpointResult<'a> {
    NotThisTime(FunctionEnvMut<'a, WasiEnv>),
    Unwinding,
}

impl WasiProcessInner {
    /// Checkpoints the process which will cause all other threads to
    /// pause and for the thread and memory state to be saved
    #[cfg(feature = "journal")]
    pub fn checkpoint<M: wasmer_types::MemorySize>(
        inner: LockableWasiProcessInner,
        ctx: FunctionEnvMut<'_, WasiEnv>,
        for_what: WasiProcessCheckpoint,
    ) -> WasiResult<MaybeCheckpointResult<'_>> {
        // Set the checkpoint flag and then enter the normal processing loop
        {
            let mut guard = inner.0.lock().unwrap();
            guard.checkpoint = for_what;
            for waker in guard.wakers.drain(..) {
                waker.wake();
            }
            inner.1.notify_all();
        }

        Self::maybe_checkpoint::<M>(inner, ctx)
    }

    /// If a checkpoint has been started this will block the current process
    /// until the checkpoint operation has completed
    #[cfg(feature = "journal")]
    pub fn maybe_checkpoint<M: wasmer_types::MemorySize>(
        inner: LockableWasiProcessInner,
        ctx: FunctionEnvMut<'_, WasiEnv>,
    ) -> WasiResult<MaybeCheckpointResult<'_>> {
        // Enter the lock which will determine if we are in a checkpoint or not

        use bytes::Bytes;
        use wasmer::AsStoreMut;
        use wasmer_types::OnCalledAction;

        use crate::{WasiError, os::task::thread::RewindResultType, rewind_ext};
        let guard = inner.0.lock().unwrap();
        if guard.checkpoint == WasiProcessCheckpoint::Execute {
            // No checkpoint so just carry on
            return Ok(Ok(MaybeCheckpointResult::NotThisTime(ctx)));
        }
        trace!("checkpoint capture");
        drop(guard);

        // Perform the unwind action
        let thread_layout = ctx.data().thread.memory_layout().clone();
        unwind::<M, _>(ctx, move |mut ctx, memory_stack, rewind_stack| {
            // Grab all the globals and serialize them
            let store_data = crate::utils::store::capture_store_snapshot(&mut ctx.as_store_mut())
                .serialize()
                .unwrap();
            let memory_stack = memory_stack.freeze();
            let rewind_stack = rewind_stack.freeze();
            let store_data = Bytes::from(store_data);

            tracing::debug!(
                "stack snapshot unwind (memory_stack={}, rewind_stack={}, store_data={})",
                memory_stack.len(),
                rewind_stack.len(),
                store_data.len(),
            );

            // Write our thread state to the snapshot
            let thread_start = ctx.data().thread.thread_start_type();
            let tid = ctx.data().thread.tid();
            if let Err(err) = JournalEffector::save_thread_state::<M>(
                &mut ctx,
                tid,
                memory_stack.clone(),
                rewind_stack.clone(),
                store_data.clone(),
                thread_start,
                thread_layout,
            ) {
                return wasmer_types::OnCalledAction::Trap(err.into());
            }

            let mut guard = inner.0.lock().unwrap();

            // Wait for the checkpoint to finish (or if we are the last thread
            // to freeze then we have to execute the checksum operation)
            loop {
                if let WasiProcessCheckpoint::Snapshot { trigger } = guard.checkpoint {
                    ctx.data().thread.set_checkpointing(true);

                    // Now if we are the last thread we also write the memory
                    let is_last_thread = guard
                        .threads
                        .values()
                        .all(|t| t.is_check_pointing() || t.is_deep_sleeping());
                    if is_last_thread {
                        if let Err(err) =
                            JournalEffector::save_memory_and_snapshot(&mut ctx, &mut guard, trigger)
                        {
                            inner.1.notify_all();
                            return wasmer_types::OnCalledAction::Trap(err.into());
                        }

                        // Clear the checkpointing flag and notify everyone to wake up
                        ctx.data().thread.set_checkpointing(false);
                        trace!("checkpoint complete");
                        if guard.disable_journaling_after_checkpoint {
                            ctx.data_mut().enable_journal = false;
                        }
                        guard.checkpoint = WasiProcessCheckpoint::Execute;
                        for waker in guard.wakers.drain(..) {
                            waker.wake();
                        }
                        inner.1.notify_all();
                    } else {
                        guard = inner.1.wait(guard).unwrap();
                    }
                    continue;
                }

                ctx.data().thread.set_checkpointing(false);
                trace!("checkpoint finished");

                if guard.stop_running_after_checkpoint {
                    trace!("will stop running now");
                    // Need to stop recording journal events so we don't also record the
                    // thread and process exit events
                    ctx.data_mut().enable_journal = false;
                    return OnCalledAction::Finish;
                }

                // Rewind the stack and carry on
                return match rewind_ext::<M>(
                    &mut ctx,
                    Some(memory_stack),
                    rewind_stack,
                    store_data,
                    RewindResultType::RewindWithoutResult,
                ) {
                    Errno::Success => OnCalledAction::InvokeAgain,
                    err => {
                        tracing::warn!(
                            "snapshot resumption failed - could not rewind the stack - errno={}",
                            err
                        );
                        OnCalledAction::Trap(Box::new(WasiError::Exit(err.into())))
                    }
                };
            }
        })?;

        Ok(Ok(MaybeCheckpointResult::Unwinding))
    }

    // Execute any checkpoints that can be executed while outside of the WASM process
    #[cfg(not(feature = "journal"))]
    pub fn do_checkpoints_from_outside(_ctx: &mut FunctionEnvMut<'_, WasiEnv>) {}

    // Execute any checkpoints that can be executed while outside of the WASM process
    #[cfg(feature = "journal")]
    pub fn do_checkpoints_from_outside(ctx: &mut FunctionEnvMut<'_, WasiEnv>) {
        let inner = ctx.data().process.inner.clone();
        let mut guard = inner.0.lock().unwrap();

        // Wait for the checkpoint to finish (or if we are the last thread
        // to freeze then we have to execute the checksum operation)
        while let WasiProcessCheckpoint::Snapshot { trigger } = guard.checkpoint {
            ctx.data().thread.set_checkpointing(true);

            // Now if we are the last thread we also write the memory
            let is_last_thread = guard
                .threads
                .values()
                .all(|t| t.is_check_pointing() || t.is_deep_sleeping());
            if is_last_thread {
                if let Err(err) =
                    JournalEffector::save_memory_and_snapshot(ctx, &mut guard, trigger)
                {
                    inner.1.notify_all();
                    tracing::error!("failed to snapshot memory and threads - {}", err);
                    return;
                }

                // Clear the checkpointing flag and notify everyone to wake up
                ctx.data().thread.set_checkpointing(false);
                trace!("checkpoint complete");
                if guard.disable_journaling_after_checkpoint {
                    ctx.data_mut().enable_journal = false;
                }
                guard.checkpoint = WasiProcessCheckpoint::Execute;
                for waker in guard.wakers.drain(..) {
                    waker.wake();
                }
                inner.1.notify_all();
            } else {
                guard = inner.1.wait(guard).unwrap();
            }
            continue;
        }

        ctx.data().thread.set_checkpointing(false);
        trace!("checkpoint finished");
    }
}

// TODO: why do we need this, how is it used?
pub(crate) struct WasiProcessWait {
    waiting: Arc<AtomicU32>,
}

impl WasiProcessWait {
    pub fn new(process: &WasiProcess) -> Self {
        process.waiting.fetch_add(1, Ordering::AcqRel);
        Self {
            waiting: process.waiting.clone(),
        }
    }
}

impl Drop for WasiProcessWait {
    fn drop(&mut self) {
        self.waiting.fetch_sub(1, Ordering::AcqRel);
    }
}

impl WasiProcess {
    pub fn new(pid: WasiProcessId, module_hash: ModuleHash, plane: WasiControlPlaneHandle) -> Self {
        let max_cpu_backoff_time = plane
            .upgrade()
            .and_then(|p| p.config().enable_exponential_cpu_backoff)
            .unwrap_or(Duration::from_secs(30));
        let max_cpu_cool_off_time = Duration::from_millis(500);

        let waiting = Arc::new(AtomicU32::new(0));
        let start_time_nanos =
            platform_clock_time_get(Snapshot0Clockid::Monotonic, 1_000_000).unwrap_or(0) as u128;
        let inner = Arc::new((
            Mutex::new(WasiProcessInner {
                pid,
                parent_pid: None,
                process_group_id: pid,
                session_id: pid,
                reported_stopped_signal: None,
                unreported_continued: false,
                process_name: None,
                hostname: "sinomo-sandbox".to_string(),
                domainname: "localdomain".to_string(),
                dumpable: true,
                no_new_privs: false,
                seccomp_mode: 0,
                timer_slack_nanos: 50_000,
                personality: 0,
                clear_child_tid: 0,
                robust_list: None,
                cap_effective: 0,
                cap_permitted: 0,
                cap_inheritable: 0,
                cap_bounding: 0,
                keepcaps: false,
                securebits: 0,
                timing_mode: 0,
                thp_disable: 0,
                io_flusher: false,
                ptracer: 0,
                namespace_ids: default_namespace_ids(),
                arch_fs_base: 0,
                arch_gs_base: 0,
                rseq: None,
                mempolicy_mode: 0,
                mempolicy_nodes: 1,
                mempolicy_home_node: 0,
                ioprio: 0,
                parent_death_signal: Signal::Signone,
                child_subreaper: false,
                umask: 0o022,
                nice: 0,
                scheduler_policy: 0,
                scheduler_priority: 0,
                cpu_affinity: 1,
                current_cpu: 0,
                resource_limits: Default::default(),
                thread_local_keys: Default::default(),
                next_thread_local_key: 1,
                child_user_nanos: 0,
                child_maxrss_kib: 0,
                identity: ProcessIdentity::default(),
                threads: Default::default(),
                thread_count: Default::default(),
                signal_intervals: Default::default(),
                pending_process_signals: Default::default(),
                children: Default::default(),
                args: Default::default(),
                envs: Default::default(),
                cwd: "/".to_string(),
                fd_size: 0,
                fd_infos: Default::default(),
                executable_path: None,
                start_time_nanos,
                checkpoint: WasiProcessCheckpoint::Execute,
                wakers: Default::default(),
                waiting: waiting.clone(),
                #[cfg(feature = "journal")]
                snapshot_on: Default::default(),
                #[cfg(feature = "journal")]
                snapshot_memory_hash: Default::default(),
                disable_journaling_after_checkpoint: false,
                stop_running_after_checkpoint: false,
                backoff: WasiProcessCpuBackoff::new(max_cpu_backoff_time, max_cpu_cool_off_time),
            }),
            Condvar::new(),
        ));

        #[derive(Debug)]
        struct SignalHandler(LockableWasiProcessInner, WasiControlPlaneHandle);
        impl SignalHandlerAbi for SignalHandler {
            fn signal(&self, signal: u8) -> Result<(), SignalDeliveryError> {
                if let Ok(signal) = signal.try_into() {
                    signal_terminal_process_group(&self.0, &self.1, signal);
                    Ok(())
                } else {
                    Err(SignalDeliveryError)
                }
            }
        }

        WasiProcess {
            pid,
            parent_pid: None,
            process_group_id: pid,
            session_id: pid,
            module_hash,
            compute: plane.clone(),
            inner: inner.clone(),
            finished: Arc::new(
                OwnedTaskStatus::new(TaskStatus::Pending)
                    .with_signal_handler(Arc::new(SignalHandler(inner, plane.clone()))),
            ),
            waiting,
            cpu_run_tokens: Arc::new(AtomicU32::new(0)),
        }
    }

    pub(super) fn set_pid(&mut self, pid: WasiProcessId) {
        self.pid = pid;
        let mut inner = self.inner.0.lock().unwrap();
        inner.pid = pid;
        inner.process_group_id = pid;
        inner.session_id = pid;
    }

    pub(crate) fn set_parent_pid(&mut self, parent_pid: WasiProcessId) {
        self.parent_pid = Some(parent_pid);
        self.inner.0.lock().unwrap().parent_pid = Some(parent_pid);
    }

    pub(crate) fn reparent_to(&self, parent_pid: Option<WasiProcessId>) {
        self.inner.0.lock().unwrap().parent_pid = parent_pid;
    }

    pub(crate) fn add_child(&self, child: WasiProcess) {
        let mut inner = self.inner.0.lock().unwrap();
        if !inner
            .children
            .iter()
            .any(|existing| existing.pid == child.pid)
        {
            inner.children.push(child);
        }
    }

    pub(crate) fn remove_child(&self, child_pid: WasiProcessId) -> Option<WasiProcess> {
        let mut inner = self.inner.0.lock().unwrap();
        let index = inner
            .children
            .iter()
            .position(|existing| existing.pid == child_pid)?;
        Some(inner.children.remove(index))
    }

    pub(crate) fn inherit_job_control_from(&mut self, parent: &WasiProcess) {
        let process_group_id = parent.pgid();
        let session_id = parent.session_id();
        let umask = parent.umask();
        let nice = parent.nice();
        let (scheduler_policy, scheduler_priority) = parent.scheduler();
        let cpu_affinity = parent.cpu_affinity();
        let current_cpu = parent.current_cpu();
        let resource_limits = parent.resource_limits();
        let identity = parent.identity();
        let process_name = parent.process_name();
        let (hostname, domainname) = parent.uts_identity();
        let dumpable = parent.dumpable();
        let no_new_privs = parent.no_new_privs();
        let seccomp_mode = parent.seccomp_mode();
        let timer_slack_nanos = parent.timer_slack_nanos();
        let personality = parent.personality();
        let robust_list = parent.robust_list();
        let (cap_effective, cap_permitted, cap_inheritable, cap_bounding) = parent.capabilities();
        let (keepcaps, securebits, timing_mode, thp_disable, io_flusher, ptracer) =
            parent.prctl_metadata();
        let namespace_ids = parent.namespace_ids();
        let (arch_fs_base, arch_gs_base, rseq) = parent.arch_runtime_metadata();
        let (mempolicy_mode, mempolicy_nodes, mempolicy_home_node, ioprio) =
            parent.numa_io_metadata();
        self.process_group_id = process_group_id;
        self.session_id = session_id;
        let mut inner = self.inner.0.lock().unwrap();
        inner.process_group_id = process_group_id;
        inner.session_id = session_id;
        inner.umask = umask;
        inner.nice = nice;
        inner.scheduler_policy = scheduler_policy;
        inner.scheduler_priority = scheduler_priority;
        inner.cpu_affinity = cpu_affinity;
        inner.current_cpu = current_cpu;
        inner.resource_limits = resource_limits;
        inner.identity = identity;
        inner.process_name = process_name;
        inner.hostname = hostname;
        inner.domainname = domainname;
        inner.dumpable = dumpable;
        inner.no_new_privs = no_new_privs;
        inner.seccomp_mode = seccomp_mode;
        inner.timer_slack_nanos = timer_slack_nanos;
        inner.personality = personality;
        inner.clear_child_tid = 0;
        inner.robust_list = robust_list;
        inner.cap_effective = cap_effective;
        inner.cap_permitted = cap_permitted;
        inner.cap_inheritable = cap_inheritable;
        inner.cap_bounding = cap_bounding;
        inner.keepcaps = keepcaps;
        inner.securebits = securebits;
        inner.timing_mode = timing_mode;
        inner.thp_disable = thp_disable;
        inner.io_flusher = io_flusher;
        inner.ptracer = ptracer;
        inner.namespace_ids = namespace_ids;
        inner.arch_fs_base = arch_fs_base;
        inner.arch_gs_base = arch_gs_base;
        inner.rseq = rseq;
        inner.mempolicy_mode = mempolicy_mode;
        inner.mempolicy_nodes = mempolicy_nodes;
        inner.mempolicy_home_node = mempolicy_home_node;
        inner.ioprio = ioprio;
    }

    pub(crate) fn set_process_group_id(&mut self, process_group_id: WasiProcessId) {
        self.process_group_id = process_group_id;
        self.inner.0.lock().unwrap().process_group_id = process_group_id;
    }

    pub(crate) fn create_session(&mut self) {
        self.process_group_id = self.pid;
        self.session_id = self.pid;
        let mut inner = self.inner.0.lock().unwrap();
        inner.process_group_id = self.pid;
        inner.session_id = self.pid;
    }

    /// Gets the process ID of this process
    pub fn pid(&self) -> WasiProcessId {
        self.pid
    }

    /// Gets the process ID of the parent process
    pub fn ppid(&self) -> WasiProcessId {
        self.inner
            .0
            .lock()
            .unwrap()
            .parent_pid
            .unwrap_or(WasiProcessId(0))
    }

    /// Gets the process group ID.
    pub fn pgid(&self) -> WasiProcessId {
        self.inner.0.lock().unwrap().process_group_id
    }

    /// Gets the session ID.
    pub fn session_id(&self) -> WasiProcessId {
        self.inner.0.lock().unwrap().session_id
    }

    /// Gets the POSIX file creation mask.
    pub fn umask(&self) -> u32 {
        self.inner.0.lock().unwrap().umask
    }

    /// Sets the POSIX file creation mask and returns the previous mask.
    pub fn set_umask(&self, mask: u32) -> u32 {
        let mut inner = self.inner.0.lock().unwrap();
        let previous = inner.umask;
        inner.umask = mask & 0o777;
        previous
    }

    /// Gets the guest-visible POSIX nice value.
    pub fn nice(&self) -> i32 {
        self.inner.0.lock().unwrap().nice
    }

    /// Sets the guest-visible POSIX nice value.
    pub fn set_nice(&self, nice: i32) {
        self.inner.0.lock().unwrap().nice = nice.clamp(-20, 20);
    }

    /// Gets guest-visible POSIX scheduler metadata.
    pub fn scheduler(&self) -> (i32, i32) {
        let inner = self.inner.0.lock().unwrap();
        (inner.scheduler_policy, inner.scheduler_priority)
    }

    /// Sets guest-visible POSIX scheduler metadata.
    pub fn set_scheduler(&self, policy: i32, priority: i32) {
        let mut inner = self.inner.0.lock().unwrap();
        inner.scheduler_policy = policy;
        inner.scheduler_priority = priority;
    }

    /// Gets guest-visible POSIX CPU affinity metadata.
    pub fn cpu_affinity(&self) -> u128 {
        self.inner.0.lock().unwrap().cpu_affinity
    }

    /// Sets guest-visible POSIX CPU affinity metadata.
    pub fn set_cpu_affinity(&self, affinity: u128) {
        self.inner.0.lock().unwrap().cpu_affinity = affinity;
    }

    /// Gets the guest-visible current CPU number.
    pub fn current_cpu(&self) -> u32 {
        self.inner.0.lock().unwrap().current_cpu
    }

    /// Gets the guest-visible POSIX resource limit override table.
    pub fn resource_limits(&self) -> HashMap<i32, ProcessResourceLimit> {
        self.inner.0.lock().unwrap().resource_limits.clone()
    }

    /// Gets a guest-visible POSIX resource limit, computing a sandbox default
    /// from this process's environment when no override has been installed.
    pub fn resource_limit(&self, resource: i32) -> ProcessResourceLimit {
        let inner = self.inner.0.lock().unwrap();
        inner
            .resource_limits
            .get(&resource)
            .copied()
            .unwrap_or_else(|| default_resource_limit(resource, &inner.envs))
    }

    /// Sets a guest-visible POSIX resource limit override.
    pub fn set_resource_limit(&self, resource: i32, limit: ProcessResourceLimit) {
        self.inner
            .0
            .lock()
            .unwrap()
            .resource_limits
            .insert(resource, limit);
    }

    pub fn open_file_descriptor_limit(&self) -> u64 {
        self.resource_limit(RLIMIT_NOFILE).current
    }

    pub fn file_size_limit(&self) -> u64 {
        self.resource_limit(RLIMIT_FSIZE).current
    }

    pub fn child_process_limit_reached(&self) -> bool {
        let Some(control_plane) = self.compute.upgrade() else {
            return false;
        };
        (control_plane.process_count() as u64) >= self.resource_limit(RLIMIT_NPROC).current
    }

    pub fn task_limit_reached(&self) -> bool {
        let limit = self.resource_limit(RLIMIT_NPROC).current;
        let inner = self.inner.0.lock().unwrap();
        (inner.thread_count as u64).saturating_add(inner.children.len() as u64) >= limit
    }

    pub fn record_reaped_child_usage(&self, child: &WasiProcess) {
        let now =
            platform_clock_time_get(Snapshot0Clockid::Monotonic, 1_000_000).unwrap_or(0) as u128;
        let elapsed_nanos = now.saturating_sub(child.start_time_nanos());
        let child_address_space = child.resource_limit(RLIMIT_AS).current;
        let child_maxrss_kib = if child_address_space == u64::MAX {
            0
        } else {
            child_address_space / 1024
        };

        let mut inner = self.inner.0.lock().unwrap();
        inner.child_user_nanos = inner.child_user_nanos.saturating_add(elapsed_nanos);
        inner.child_maxrss_kib = inner.child_maxrss_kib.max(child_maxrss_kib);
    }

    pub fn child_resource_usage(&self) -> (u64, u64) {
        let inner = self.inner.0.lock().unwrap();
        let user_usec = (inner.child_user_nanos / 1_000).min(u128::from(u64::MAX)) as u64;
        (user_usec, inner.child_maxrss_kib)
    }

    pub fn child_user_ticks(&self) -> u64 {
        let inner = self.inner.0.lock().unwrap();
        (inner.child_user_nanos / 10_000_000).min(u128::from(u64::MAX)) as u64
    }

    /// Installs sandbox-owned POSIX resource limits that survive guest env replacement.
    pub fn set_sandbox_resource_limits(
        &self,
        memory_limit_bytes: u64,
        wall_time_ms: u64,
        task_limit: u64,
    ) {
        self.inner.0.lock().unwrap().resource_limits =
            sandbox_resource_limits(memory_limit_bytes, wall_time_ms, task_limit);
    }

    /// Gets the guest-visible POSIX identity.
    pub fn identity(&self) -> ProcessIdentity {
        self.inner.0.lock().unwrap().identity.clone()
    }

    /// Replaces the guest-visible POSIX identity.
    pub fn set_identity(&self, identity: ProcessIdentity) {
        self.inner.0.lock().unwrap().identity = identity;
    }

    /// Gains access to the process internals
    // TODO: Make this private, all inner access should be exposed with methods.
    pub fn lock(&self) -> MutexGuard<'_, WasiProcessInner> {
        self.inner.0.lock().unwrap()
    }

    /// Creates a a thread and returns it
    pub fn new_thread(
        &self,
        layout: WasiMemoryLayout,
        start: ThreadStartType,
    ) -> Result<WasiThreadHandle, ControlPlaneError> {
        let control_plane = self.compute.must_upgrade();

        // Determine if its the main thread or not
        let is_main = matches!(start, ThreadStartType::MainThread);

        // Generate a new process ID (this is because the process ID and thread ID
        // address space must not overlap in libc). For the main proecess the TID=PID
        let tid: WasiThreadId = if is_main {
            self.pid().raw().into()
        } else {
            let tid: u32 = control_plane.generate_id()?.into();
            tid.into()
        };

        self.new_thread_with_id(layout, start, tid)
    }

    /// Creates a a thread and returns it
    pub fn new_thread_with_id(
        &self,
        layout: WasiMemoryLayout,
        start: ThreadStartType,
        tid: WasiThreadId,
    ) -> Result<WasiThreadHandle, ControlPlaneError> {
        let control_plane = self.compute.must_upgrade();
        let task_count_guard = control_plane.register_task()?;

        let is_main = matches!(start, ThreadStartType::MainThread);

        // The wait finished should be the process version if its the main thread
        let mut inner = self.inner.0.lock().unwrap();
        let finished = if is_main {
            self.finished.clone()
        } else {
            Arc::new(OwnedTaskStatus::default())
        };

        // Insert the thread into the pool
        let ctrl = WasiThread::new(
            self.pid(),
            tid,
            is_main,
            finished,
            task_count_guard,
            layout,
            start,
        );
        inner.threads.insert(tid, ctrl.clone());
        inner.thread_count += 1;

        Ok(WasiThreadHandle::new(ctrl, &self.inner))
    }

    pub fn all_threads(&self) -> Vec<WasiThreadId> {
        let inner = self.inner.0.lock().unwrap();
        inner.threads.keys().cloned().collect()
    }

    pub fn set_metadata(
        &self,
        args: Vec<String>,
        envs: Vec<Vec<u8>>,
        cwd: String,
        fd_infos: Vec<ProcessFdInfo>,
    ) {
        let mut inner = self.inner.0.lock().unwrap();
        inner.args = args;
        inner.envs = envs;
        inner.cwd = cwd;
        inner.fd_size = fd_infos.len();
        inner.fd_infos = fd_infos;
    }

    pub fn args(&self) -> Vec<String> {
        self.inner.0.lock().unwrap().args.clone()
    }

    pub fn envs(&self) -> Vec<Vec<u8>> {
        self.inner.0.lock().unwrap().envs.clone()
    }

    pub fn cwd(&self) -> String {
        self.inner.0.lock().unwrap().cwd.clone()
    }

    pub fn fd_size(&self) -> usize {
        self.inner.0.lock().unwrap().fd_size
    }

    pub fn fd_numbers(&self) -> Vec<WasiFd> {
        self.inner
            .0
            .lock()
            .unwrap()
            .fd_infos
            .iter()
            .map(|info| info.fd)
            .collect()
    }

    pub fn fd_info(&self, fd: WasiFd) -> Option<ProcessFdInfo> {
        self.inner
            .0
            .lock()
            .unwrap()
            .fd_infos
            .iter()
            .find(|info| info.fd == fd)
            .cloned()
    }

    pub fn set_executable_path(&self, path: String) {
        self.inner.0.lock().unwrap().executable_path = Some(path);
    }

    pub fn set_executable_path_option(&self, path: Option<String>) {
        self.inner.0.lock().unwrap().executable_path = path;
    }

    pub fn executable_path(&self) -> Option<String> {
        self.inner.0.lock().unwrap().executable_path.clone()
    }

    pub fn create_thread_local_key(&self, destructor: u64) -> Result<u32, Errno> {
        let mut inner = self.inner.0.lock().unwrap();
        let start = inner.next_thread_local_key;
        loop {
            let key = inner.next_thread_local_key;
            inner.next_thread_local_key = inner.next_thread_local_key.wrapping_add(1);
            if inner.next_thread_local_key == 0 {
                inner.next_thread_local_key = 1;
            }
            if let std::collections::hash_map::Entry::Vacant(entry) =
                inner.thread_local_keys.entry(key)
            {
                entry.insert(destructor);
                return Ok(key);
            }
            if inner.next_thread_local_key == start {
                return Err(Errno::Nomem);
            }
        }
    }

    pub fn destroy_thread_local_key(&self, key: u32) -> bool {
        let mut inner = self.inner.0.lock().unwrap();
        if inner.thread_local_keys.remove(&key).is_none() {
            return false;
        }
        for thread in inner.threads.values() {
            thread.remove_thread_local_value(key);
        }
        true
    }

    pub fn has_thread_local_key(&self, key: u32) -> bool {
        self.inner
            .0
            .lock()
            .unwrap()
            .thread_local_keys
            .contains_key(&key)
    }

    pub fn thread_local_destructors_for_values(
        &self,
        values: std::collections::HashMap<u32, u64>,
    ) -> Vec<(u32, u64, u64)> {
        let inner = self.inner.0.lock().unwrap();
        values
            .into_iter()
            .filter_map(|(key, value)| {
                if value == 0 {
                    return None;
                }
                let destructor = *inner.thread_local_keys.get(&key)?;
                if destructor == 0 {
                    return None;
                }
                Some((key, destructor, value))
            })
            .collect()
    }

    pub fn start_time_nanos(&self) -> u128 {
        self.inner.0.lock().unwrap().start_time_nanos
    }

    pub fn task_status(&self) -> TaskStatus {
        self.finished.status()
    }

    pub fn take_unreported_stopped_signal(&self) -> Option<Signal> {
        let signal = self.task_status().stopped_signal()?;
        let mut inner = self.inner.0.lock().unwrap();
        if inner.reported_stopped_signal == Some(signal) {
            None
        } else {
            inner.reported_stopped_signal = Some(signal);
            Some(signal)
        }
    }

    pub fn peek_unreported_stopped_signal(&self) -> Option<Signal> {
        let signal = self.task_status().stopped_signal()?;
        let inner = self.inner.0.lock().unwrap();
        if inner.reported_stopped_signal == Some(signal) {
            None
        } else {
            Some(signal)
        }
    }

    pub fn take_unreported_continued(&self) -> bool {
        let mut inner = self.inner.0.lock().unwrap();
        if inner.unreported_continued {
            inner.unreported_continued = false;
            true
        } else {
            false
        }
    }

    pub fn peek_unreported_continued(&self) -> bool {
        self.inner.0.lock().unwrap().unreported_continued
    }

    pub fn process_name(&self) -> Option<String> {
        self.inner.0.lock().unwrap().process_name.clone()
    }

    pub fn set_process_name(&self, name: Option<String>) {
        self.inner.0.lock().unwrap().process_name = name;
    }

    pub fn uts_identity(&self) -> (String, String) {
        let inner = self.inner.0.lock().unwrap();
        (inner.hostname.clone(), inner.domainname.clone())
    }

    pub fn hostname(&self) -> String {
        self.inner.0.lock().unwrap().hostname.clone()
    }

    pub fn set_hostname(&self, hostname: String) {
        self.inner.0.lock().unwrap().hostname = hostname;
    }

    pub fn domainname(&self) -> String {
        self.inner.0.lock().unwrap().domainname.clone()
    }

    pub fn set_domainname(&self, domainname: String) {
        self.inner.0.lock().unwrap().domainname = domainname;
    }

    pub fn dumpable(&self) -> bool {
        self.inner.0.lock().unwrap().dumpable
    }

    pub fn set_dumpable(&self, dumpable: bool) {
        self.inner.0.lock().unwrap().dumpable = dumpable;
    }

    pub fn no_new_privs(&self) -> bool {
        self.inner.0.lock().unwrap().no_new_privs
    }

    pub fn set_no_new_privs(&self, no_new_privs: bool) {
        self.inner.0.lock().unwrap().no_new_privs = no_new_privs;
    }

    pub fn seccomp_mode(&self) -> u32 {
        self.inner.0.lock().unwrap().seccomp_mode
    }

    pub fn set_seccomp_mode(&self, seccomp_mode: u32) {
        self.inner.0.lock().unwrap().seccomp_mode = seccomp_mode;
    }

    pub fn timer_slack_nanos(&self) -> u64 {
        self.inner.0.lock().unwrap().timer_slack_nanos
    }

    pub fn set_timer_slack_nanos(&self, timer_slack_nanos: u64) {
        self.inner.0.lock().unwrap().timer_slack_nanos = timer_slack_nanos;
    }

    pub fn personality(&self) -> u64 {
        self.inner.0.lock().unwrap().personality
    }

    pub fn set_personality(&self, personality: u64) {
        self.inner.0.lock().unwrap().personality = personality;
    }

    pub fn clear_child_tid(&self) -> u64 {
        self.inner.0.lock().unwrap().clear_child_tid
    }

    pub fn set_clear_child_tid(&self, clear_child_tid: u64) {
        self.inner.0.lock().unwrap().clear_child_tid = clear_child_tid;
    }

    pub fn robust_list(&self) -> Option<(u64, u64)> {
        self.inner.0.lock().unwrap().robust_list
    }

    pub fn set_robust_list(&self, head: u64, len: u64) {
        self.inner.0.lock().unwrap().robust_list = Some((head, len));
    }

    pub fn capabilities(&self) -> (u64, u64, u64, u64) {
        let inner = self.inner.0.lock().unwrap();
        (
            inner.cap_effective,
            inner.cap_permitted,
            inner.cap_inheritable,
            inner.cap_bounding,
        )
    }

    pub fn set_capabilities(&self, effective: u64, permitted: u64, inheritable: u64) {
        let mut inner = self.inner.0.lock().unwrap();
        inner.cap_effective = effective;
        inner.cap_permitted = permitted;
        inner.cap_inheritable = inheritable;
    }

    pub fn capability_in_bounding_set(&self, capability: u32) -> bool {
        let inner = self.inner.0.lock().unwrap();
        (inner.cap_bounding & (1u64 << capability)) != 0
    }

    pub fn drop_capability_from_bounding_set(&self, capability: u32) {
        let mut inner = self.inner.0.lock().unwrap();
        inner.cap_bounding &= !(1u64 << capability);
    }

    pub fn prctl_metadata(&self) -> (bool, u32, u32, u32, bool, i32) {
        let inner = self.inner.0.lock().unwrap();
        (
            inner.keepcaps,
            inner.securebits,
            inner.timing_mode,
            inner.thp_disable,
            inner.io_flusher,
            inner.ptracer,
        )
    }

    pub fn set_keepcaps(&self, keepcaps: bool) {
        self.inner.0.lock().unwrap().keepcaps = keepcaps;
    }

    pub fn keepcaps(&self) -> bool {
        self.inner.0.lock().unwrap().keepcaps
    }

    pub fn set_securebits(&self, securebits: u32) {
        self.inner.0.lock().unwrap().securebits = securebits;
    }

    pub fn securebits(&self) -> u32 {
        self.inner.0.lock().unwrap().securebits
    }

    pub fn set_timing_mode(&self, timing_mode: u32) {
        self.inner.0.lock().unwrap().timing_mode = timing_mode;
    }

    pub fn timing_mode(&self) -> u32 {
        self.inner.0.lock().unwrap().timing_mode
    }

    pub fn set_thp_disable(&self, thp_disable: u32) {
        self.inner.0.lock().unwrap().thp_disable = thp_disable;
    }

    pub fn thp_disable(&self) -> u32 {
        self.inner.0.lock().unwrap().thp_disable
    }

    pub fn set_io_flusher(&self, io_flusher: bool) {
        self.inner.0.lock().unwrap().io_flusher = io_flusher;
    }

    pub fn io_flusher(&self) -> bool {
        self.inner.0.lock().unwrap().io_flusher
    }

    pub fn set_ptracer(&self, ptracer: i32) {
        self.inner.0.lock().unwrap().ptracer = ptracer;
    }

    pub fn ptracer(&self) -> i32 {
        self.inner.0.lock().unwrap().ptracer
    }

    pub fn namespace_ids(&self) -> HashMap<String, u64> {
        self.inner.0.lock().unwrap().namespace_ids.clone()
    }

    pub fn namespace_id(&self, namespace: &str) -> u64 {
        self.inner
            .0
            .lock()
            .unwrap()
            .namespace_ids
            .get(namespace)
            .copied()
            .unwrap_or(1)
    }

    pub fn unshare_namespaces(&self, namespaces: &[&str]) {
        let mut inner = self.inner.0.lock().unwrap();
        let pid = inner.pid.raw() as u64;
        for namespace in namespaces {
            if inner.namespace_ids.contains_key(*namespace) {
                inner
                    .namespace_ids
                    .insert((*namespace).to_string(), namespace_id_for(pid, namespace));
            }
        }
    }

    pub fn arch_runtime_metadata(&self) -> (u64, u64, Option<(u64, u32, u32)>) {
        let inner = self.inner.0.lock().unwrap();
        (inner.arch_fs_base, inner.arch_gs_base, inner.rseq)
    }

    pub fn set_arch_fs_base(&self, address: u64) {
        self.inner.0.lock().unwrap().arch_fs_base = address;
    }

    pub fn arch_fs_base(&self) -> u64 {
        self.inner.0.lock().unwrap().arch_fs_base
    }

    pub fn set_arch_gs_base(&self, address: u64) {
        self.inner.0.lock().unwrap().arch_gs_base = address;
    }

    pub fn arch_gs_base(&self) -> u64 {
        self.inner.0.lock().unwrap().arch_gs_base
    }

    pub fn set_rseq(&self, address: u64, len: u32, signature: u32) {
        self.inner.0.lock().unwrap().rseq = Some((address, len, signature));
    }

    pub fn clear_rseq_if_matches(&self, address: u64) -> bool {
        let mut inner = self.inner.0.lock().unwrap();
        if matches!(inner.rseq, Some((current, _, _)) if current == address) {
            inner.rseq = None;
            true
        } else {
            false
        }
    }

    pub fn numa_io_metadata(&self) -> (i32, u64, u64, i32) {
        let inner = self.inner.0.lock().unwrap();
        (
            inner.mempolicy_mode,
            inner.mempolicy_nodes,
            inner.mempolicy_home_node,
            inner.ioprio,
        )
    }

    pub fn set_mempolicy(&self, mode: i32, nodes: u64) {
        let mut inner = self.inner.0.lock().unwrap();
        inner.mempolicy_mode = mode;
        inner.mempolicy_nodes = nodes.max(1);
    }

    pub fn mempolicy(&self) -> (i32, u64) {
        let inner = self.inner.0.lock().unwrap();
        (inner.mempolicy_mode, inner.mempolicy_nodes)
    }

    pub fn set_mempolicy_home_node(&self, home_node: u64) {
        self.inner.0.lock().unwrap().mempolicy_home_node = home_node;
    }

    pub fn set_ioprio(&self, ioprio: i32) {
        self.inner.0.lock().unwrap().ioprio = ioprio;
    }

    pub fn ioprio(&self) -> i32 {
        self.inner.0.lock().unwrap().ioprio
    }

    pub fn parent_death_signal(&self) -> Signal {
        self.inner.0.lock().unwrap().parent_death_signal
    }

    pub fn set_parent_death_signal(&self, signal: Signal) {
        self.inner.0.lock().unwrap().parent_death_signal = signal;
    }

    pub fn child_subreaper(&self) -> bool {
        self.inner.0.lock().unwrap().child_subreaper
    }

    pub fn set_child_subreaper(&self, enabled: bool) {
        self.inner.0.lock().unwrap().child_subreaper = enabled;
    }

    pub async fn join_until_unreported_stopped_or_finished(&self) -> TaskStatus {
        loop {
            let current = self.task_status();
            match current {
                TaskStatus::Stopped(signal) => {
                    if self.take_unreported_stopped_signal().is_some() {
                        return TaskStatus::Stopped(signal);
                    }
                    self.finished
                        .await_status_change_from(TaskStatus::Stopped(signal))
                        .await;
                }
                TaskStatus::Finished(_) | TaskStatus::Signaled { .. } => return current,
                TaskStatus::Pending | TaskStatus::Running => {
                    let status = self.finished.await_stopped_or_terminated().await;
                    if !matches!(status, TaskStatus::Stopped(_)) {
                        return status;
                    }
                }
            }
        }
    }

    /// Removes a thread handle from this process without waiting for it.
    pub(crate) fn remove_thread(&self, tid: &WasiThreadId) -> bool {
        let mut inner = self.inner.0.lock().unwrap();
        if inner.threads.remove(tid).is_some() {
            inner.thread_count = inner.thread_count.saturating_sub(1);
            true
        } else {
            false
        }
    }

    /// Gets a reference to a particular thread
    pub fn get_thread(&self, tid: &WasiThreadId) -> Option<WasiThread> {
        let inner = self.inner.0.lock().unwrap();
        inner.threads.get(tid).cloned()
    }

    /// Signals a particular thread in the process
    pub fn signal_thread(&self, tid: &WasiThreadId, signal: Signal) -> bool {
        // Sometimes we will signal the process rather than the thread hence this libc hardcoded value
        let mut tid = tid.raw();
        if tid == 1073741823 {
            tid = self.pid().raw();
        }
        let tid: WasiThreadId = tid.into();

        let pid = self.pid();
        tracing::trace!(%pid, %tid, "signal-thread({:?})", signal);

        let inner = self.inner.0.lock().unwrap();
        if let Some(thread) = inner.threads.get(&tid) {
            if signal != Signal::Signone {
                thread.signal(signal);
            }
            true
        } else {
            trace!(
                "wasi[{}]::lost-signal(tid={}, sig={:?})",
                self.pid(),
                tid,
                signal
            );
            false
        }
    }

    /// Signals all the threads in this process
    pub fn signal_process(&self, signal: Signal) {
        signal_process_internal(self, signal);
    }

    /// Signals this process directly without forwarding to waited-on children.
    pub fn signal_process_direct(&self, signal: Signal) {
        signal_process_threads(self, signal);
    }

    pub(crate) fn deliver_process_pending_to_thread(&self, thread: &WasiThread) {
        let signals = {
            let mut guard = self.inner.0.lock().unwrap();
            let mut deliver = Vec::new();
            guard.pending_process_signals.retain(|signal| {
                if thread_blocks_signal(thread, *signal) {
                    true
                } else {
                    deliver.push(*signal);
                    false
                }
            });
            deliver
        };

        for signal in signals {
            thread.signal(signal);
        }
    }

    pub(crate) fn notify_parent_child_status_changed(&self) {
        let Some(parent_pid) = self.inner.0.lock().unwrap().parent_pid else {
            return;
        };
        let Some(control_plane) = self.compute.upgrade() else {
            return;
        };
        if let Some(parent) = control_plane.get_process(parent_pid) {
            parent.wake_status_waiters();
            parent.signal_process_direct(Signal::Sigchld);
        }
    }

    pub(crate) fn wake_status_waiters(&self) {
        let inner = &self.inner;
        let mut guard = inner.0.lock().unwrap();
        for waker in guard.wakers.drain(..) {
            waker.wake();
        }
        inner.1.notify_all();
    }

    pub(crate) fn subscribe_status_change(&self, waker: &Waker) {
        let mut guard = self.inner.0.lock().unwrap();
        if !guard
            .wakers
            .iter()
            .any(|existing| existing.will_wake(waker))
        {
            guard.wakers.push(waker.clone());
        }
    }

    /// Takes a snapshot of the process and disables journaling returning
    /// a future that can be waited on for the snapshot to complete
    ///
    /// Note: If you ignore the returned future the checkpoint will still
    /// occur but it will execute asynchronously
    pub fn snapshot_and_disable_journaling(
        &self,
        trigger: SnapshotTrigger,
    ) -> std::pin::Pin<Box<dyn futures::Future<Output = ()> + Send + Sync>> {
        let mut guard = self.inner.0.lock().unwrap();
        guard.disable_journaling_after_checkpoint = true;
        guard.checkpoint = WasiProcessCheckpoint::Snapshot { trigger };
        self.wait_for_checkpoint_finish()
    }

    /// Takes a snapshot of the process and shuts it down after the snapshot
    /// is taken.
    ///
    /// Note: If you ignore the returned future the checkpoint will still
    /// occur but it will execute asynchronously
    pub fn snapshot_and_stop(
        &self,
        trigger: SnapshotTrigger,
    ) -> std::pin::Pin<Box<dyn futures::Future<Output = ()> + Send + Sync>> {
        let mut guard = self.inner.0.lock().unwrap();
        guard.stop_running_after_checkpoint = true;
        guard.checkpoint = WasiProcessCheckpoint::Snapshot { trigger };
        self.wait_for_checkpoint_finish()
    }

    /// Takes a snapshot of the process
    ///
    /// Note: If you ignore the returned future the checkpoint will still
    /// occur but it will execute asynchronously
    pub fn snapshot(
        &self,
        trigger: SnapshotTrigger,
    ) -> std::pin::Pin<Box<dyn futures::Future<Output = ()> + Send + Sync>> {
        let mut guard = self.inner.0.lock().unwrap();
        guard.checkpoint = WasiProcessCheckpoint::Snapshot { trigger };
        self.wait_for_checkpoint_finish()
    }

    /// Disables the journaling functionality
    pub fn disable_journaling_after_checkpoint(&self) {
        let mut guard = self.inner.0.lock().unwrap();
        guard.disable_journaling_after_checkpoint = true;
    }

    /// Stop running once a checkpoint is taken
    pub fn stop_running_after_checkpoint(&self) {
        let mut guard = self.inner.0.lock().unwrap();
        guard.stop_running_after_checkpoint = true;
    }

    /// Wait for the checkout process to finish
    #[cfg(not(feature = "journal"))]
    pub fn wait_for_checkpoint(
        &self,
    ) -> std::pin::Pin<Box<dyn futures::Future<Output = ()> + Send + Sync>> {
        Box::pin(std::future::pending())
    }

    /// Wait for the checkout process to finish
    #[cfg(feature = "journal")]
    pub fn wait_for_checkpoint(
        &self,
    ) -> std::pin::Pin<Box<dyn futures::Future<Output = ()> + Send + Sync>> {
        use futures::Future;
        use std::{
            pin::Pin,
            task::{Context, Poll},
        };

        struct Poller {
            inner: LockableWasiProcessInner,
        }
        impl Future for Poller {
            type Output = ();
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let mut guard = self.inner.0.lock().unwrap();
                if !matches!(guard.checkpoint, WasiProcessCheckpoint::Execute) {
                    return Poll::Ready(());
                }
                if !guard.wakers.iter().any(|w| w.will_wake(cx.waker())) {
                    guard.wakers.push(cx.waker().clone());
                }
                Poll::Pending
            }
        }
        Box::pin(Poller {
            inner: self.inner.clone(),
        })
    }

    /// Wait for the checkout process to finish
    #[cfg(not(feature = "journal"))]
    pub fn wait_for_checkpoint_finish(
        &self,
    ) -> std::pin::Pin<Box<dyn futures::Future<Output = ()> + Send + Sync>> {
        Box::pin(std::future::pending())
    }

    /// Wait for the checkout process to finish
    #[cfg(feature = "journal")]
    pub fn wait_for_checkpoint_finish(
        &self,
    ) -> std::pin::Pin<Box<dyn futures::Future<Output = ()> + Send + Sync>> {
        use futures::Future;
        use std::{
            pin::Pin,
            task::{Context, Poll},
        };

        struct Poller {
            inner: LockableWasiProcessInner,
        }
        impl Future for Poller {
            type Output = ();
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let mut guard = self.inner.0.lock().unwrap();
                if matches!(guard.checkpoint, WasiProcessCheckpoint::Execute) {
                    return Poll::Ready(());
                }
                if !guard.wakers.iter().any(|w| w.will_wake(cx.waker())) {
                    guard.wakers.push(cx.waker().clone());
                }
                Poll::Pending
            }
        }
        Box::pin(Poller {
            inner: self.inner.clone(),
        })
    }

    /// Signals one of the threads every interval
    pub fn signal_interval(&self, signal: Signal, interval: Option<Duration>, repeat: bool) {
        let mut inner = self.inner.0.lock().unwrap();

        let interval = match interval {
            None => {
                inner.signal_intervals.remove(&signal);
                return;
            }
            Some(a) => a,
        };

        let now = match platform_clock_time_get(Snapshot0Clockid::Monotonic, 1_000_000) {
            Ok(now) => now as u128,
            Err(err) => {
                tracing::warn!(
                    ?signal,
                    %err,
                    "failed to read monotonic clock for signal interval"
                );
                return;
            }
        };
        inner.signal_intervals.insert(
            signal,
            WasiSignalInterval {
                signal,
                interval,
                last_signal: now,
                repeat,
            },
        );
    }

    /// Returns the number of active threads for this process
    pub fn active_threads(&self) -> u32 {
        let inner = self.inner.0.lock().unwrap();
        inner.thread_count
    }

    /// Waits until the process is finished.
    pub async fn join(&self) -> Result<ExitCode, Arc<WasiRuntimeError>> {
        let _guard = WasiProcessWait::new(self);
        self.finished.await_termination().await
    }

    pub async fn join_status(&self) -> TaskStatus {
        let _guard = WasiProcessWait::new(self);
        self.finished.await_termination_status().await
    }

    pub async fn join_until_stopped_or_finished(&self) -> TaskStatus {
        let _guard = WasiProcessWait::new(self);
        self.finished.await_stopped_or_terminated().await
    }

    /// Attempts to join on the process
    pub fn try_join(&self) -> Option<Result<ExitCode, Arc<WasiRuntimeError>>> {
        self.finished.status().into_finished()
    }

    pub fn try_join_status(&self) -> Option<TaskStatus> {
        let status = self.finished.status();
        if status.is_finished() {
            Some(status)
        } else {
            None
        }
    }

    /// Waits for all the children to be finished
    pub async fn join_children(&mut self) -> Option<Result<ExitCode, Arc<WasiRuntimeError>>> {
        let _guard = WasiProcessWait::new(self);
        let children: Vec<_> = {
            let inner = self.inner.0.lock().unwrap();
            inner.children.clone()
        };
        if children.is_empty() {
            return None;
        }
        let mut waits = Vec::new();
        for child in children {
            if let Some(process) = self.compute.must_upgrade().get_process(child.pid) {
                let parent = self.clone();
                let inner = self.inner.clone();
                let control_plane = self.compute.must_upgrade();
                waits.push(async move {
                    let join = process.join().await;
                    parent.record_reaped_child_usage(&child);
                    let mut inner = inner.0.lock().unwrap();
                    inner.children.retain(|a| a.pid != child.pid);
                    drop(inner);
                    control_plane.remove_process(child.pid);
                    join
                })
            } else {
                let mut inner = self.inner.0.lock().unwrap();
                inner.children.retain(|existing| existing.pid != child.pid);
            }
        }
        if waits.is_empty() {
            return None;
        }
        futures::future::join_all(waits.into_iter())
            .await
            .into_iter()
            .next()
    }

    /// Waits for any of the children to finished
    pub async fn join_any_child(&mut self) -> Result<Option<(WasiProcessId, ExitCode)>, Errno> {
        self.join_any_child_matching(|_| true).await
    }

    pub async fn join_any_child_until_stopped_or_finished(
        &mut self,
    ) -> Result<Option<(WasiProcess, TaskStatus)>, Errno> {
        self.join_any_child_matching_until_stopped_or_finished(|_| true)
            .await
    }

    pub async fn join_any_child_until_unreported_stopped_or_finished(
        &mut self,
    ) -> Result<Option<(WasiProcess, TaskStatus)>, Errno> {
        self.join_any_child_matching_until_unreported_stopped_or_finished(|_| true)
            .await
    }

    /// Waits for any matching child to finish.
    pub async fn join_any_child_matching<F>(
        &mut self,
        mut matches: F,
    ) -> Result<Option<(WasiProcessId, ExitCode)>, Errno>
    where
        F: FnMut(&WasiProcess) -> bool,
    {
        let _guard = WasiProcessWait::new(self);
        let children: Vec<_> = {
            let inner = self.inner.0.lock().unwrap();
            inner.children.clone()
        };
        if children.is_empty() {
            return Err(Errno::Child);
        }

        let mut waits = Vec::new();
        let mut saw_matching_child = false;
        for child in children {
            if !matches(&child) {
                continue;
            }
            saw_matching_child = true;
            if let Some(process) = self.compute.must_upgrade().get_process(child.pid) {
                let parent = self.clone();
                let inner = self.inner.clone();
                let control_plane = self.compute.must_upgrade();
                waits.push(async move {
                    let join = process.join().await;
                    parent.record_reaped_child_usage(&child);
                    let mut inner = inner.0.lock().unwrap();
                    inner.children.retain(|a| a.pid != child.pid);
                    drop(inner);
                    control_plane.remove_process(child.pid);
                    (child, join)
                })
            } else {
                let mut inner = self.inner.0.lock().unwrap();
                inner.children.retain(|existing| existing.pid != child.pid);
            }
        }
        if !saw_matching_child {
            return Err(Errno::Child);
        }
        if waits.is_empty() {
            return Err(Errno::Child);
        }
        let (child, res) = futures::future::select_all(waits.into_iter().map(Box::pin))
            .await
            .0;

        let code =
            res.unwrap_or_else(|e| e.as_exit_code().unwrap_or_else(|| Errno::Canceled.into()));

        Ok(Some((child.pid, code)))
    }

    pub async fn join_any_child_matching_status<F>(
        &mut self,
        mut matches: F,
    ) -> Result<Option<(WasiProcess, TaskStatus)>, Errno>
    where
        F: FnMut(&WasiProcess) -> bool,
    {
        let _guard = WasiProcessWait::new(self);
        let children: Vec<_> = {
            let inner = self.inner.0.lock().unwrap();
            inner.children.clone()
        };
        if children.is_empty() {
            return Err(Errno::Child);
        }

        let mut waits = Vec::new();
        let mut saw_matching_child = false;
        for child in children {
            if !matches(&child) {
                continue;
            }
            saw_matching_child = true;
            if let Some(process) = self.compute.must_upgrade().get_process(child.pid) {
                let parent = self.clone();
                let inner = self.inner.clone();
                let control_plane = self.compute.must_upgrade();
                waits.push(async move {
                    let status = process.join_status().await;
                    parent.record_reaped_child_usage(&child);
                    let mut inner = inner.0.lock().unwrap();
                    inner.children.retain(|a| a.pid != child.pid);
                    drop(inner);
                    control_plane.remove_process(child.pid);
                    (process, status)
                })
            } else {
                let mut inner = self.inner.0.lock().unwrap();
                inner.children.retain(|existing| existing.pid != child.pid);
            }
        }
        if !saw_matching_child {
            return Err(Errno::Child);
        }
        if waits.is_empty() {
            return Err(Errno::Child);
        }
        let (process, status) = futures::future::select_all(waits.into_iter().map(Box::pin))
            .await
            .0;

        Ok(Some((process, status)))
    }

    pub async fn join_any_child_matching_until_stopped_or_finished<F>(
        &mut self,
        mut matches: F,
    ) -> Result<Option<(WasiProcess, TaskStatus)>, Errno>
    where
        F: FnMut(&WasiProcess) -> bool,
    {
        let _guard = WasiProcessWait::new(self);
        let children: Vec<_> = {
            let inner = self.inner.0.lock().unwrap();
            inner.children.clone()
        };
        if children.is_empty() {
            return Err(Errno::Child);
        }

        let mut waits = Vec::new();
        let mut saw_matching_child = false;
        for child in children {
            if !matches(&child) {
                continue;
            }
            saw_matching_child = true;
            if let Some(process) = self.compute.must_upgrade().get_process(child.pid) {
                let parent = self.clone();
                let control_plane = self.compute.must_upgrade();
                let inner = self.inner.clone();
                waits.push(async move {
                    let status = process.join_until_stopped_or_finished().await;
                    if matches!(
                        status,
                        TaskStatus::Finished(_) | TaskStatus::Signaled { .. }
                    ) {
                        parent.record_reaped_child_usage(&child);
                        let mut inner = inner.0.lock().unwrap();
                        inner.children.retain(|a| a.pid != child.pid);
                        drop(inner);
                        control_plane.remove_process(child.pid);
                    }
                    (child, status)
                })
            } else {
                let mut inner = self.inner.0.lock().unwrap();
                inner.children.retain(|existing| existing.pid != child.pid);
            }
        }
        if !saw_matching_child {
            return Err(Errno::Child);
        }
        if waits.is_empty() {
            return Err(Errno::Child);
        }
        let (child, status) = futures::future::select_all(waits.into_iter().map(Box::pin))
            .await
            .0;

        Ok(Some((child, status)))
    }

    pub async fn join_any_child_matching_until_unreported_stopped_or_finished<F>(
        &mut self,
        mut matches: F,
    ) -> Result<Option<(WasiProcess, TaskStatus)>, Errno>
    where
        F: FnMut(&WasiProcess) -> bool,
    {
        let _guard = WasiProcessWait::new(self);
        let children: Vec<_> = {
            let inner = self.inner.0.lock().unwrap();
            inner.children.clone()
        };
        if children.is_empty() {
            return Err(Errno::Child);
        }

        let mut waits = Vec::new();
        let mut saw_matching_child = false;
        for child in children {
            if !matches(&child) {
                continue;
            }
            saw_matching_child = true;
            if let Some(process) = self.compute.must_upgrade().get_process(child.pid) {
                let parent = self.clone();
                let control_plane = self.compute.must_upgrade();
                let inner = self.inner.clone();
                waits.push(async move {
                    let status = process.join_until_unreported_stopped_or_finished().await;
                    if matches!(
                        status,
                        TaskStatus::Finished(_) | TaskStatus::Signaled { .. }
                    ) {
                        parent.record_reaped_child_usage(&child);
                        let mut inner = inner.0.lock().unwrap();
                        inner.children.retain(|a| a.pid != child.pid);
                        drop(inner);
                        control_plane.remove_process(child.pid);
                    }
                    (child, status)
                })
            } else {
                let mut inner = self.inner.0.lock().unwrap();
                inner.children.retain(|existing| existing.pid != child.pid);
            }
        }
        if !saw_matching_child {
            return Err(Errno::Child);
        }
        if waits.is_empty() {
            return Err(Errno::Child);
        }
        let (child, status) = futures::future::select_all(waits.into_iter().map(Box::pin))
            .await
            .0;

        Ok(Some((child, status)))
    }

    /// Attempts to join any finished child without blocking.
    pub fn try_join_any_child(&mut self) -> Result<Option<(WasiProcessId, ExitCode)>, Errno> {
        self.try_join_any_child_matching(|_| true)
    }

    pub fn try_join_any_child_status(
        &mut self,
    ) -> Result<Option<(WasiProcess, TaskStatus)>, Errno> {
        self.try_join_any_child_matching_status(|_| true)
    }

    /// Attempts to join any finished matching child without blocking.
    pub fn try_join_any_child_matching<F>(
        &mut self,
        mut matches: F,
    ) -> Result<Option<(WasiProcessId, ExitCode)>, Errno>
    where
        F: FnMut(&WasiProcess) -> bool,
    {
        let children: Vec<_> = {
            let inner = self.inner.0.lock().unwrap();
            inner.children.clone()
        };
        if children.is_empty() {
            return Err(Errno::Child);
        }

        let mut saw_live_child = false;
        let mut saw_matching_child = false;
        for child in children {
            if !matches(&child) {
                continue;
            }
            saw_matching_child = true;
            let control_plane = self.compute.must_upgrade();
            let Some(process) = control_plane.get_process(child.pid) else {
                let mut inner = self.inner.0.lock().unwrap();
                inner.children.retain(|existing| existing.pid != child.pid);
                continue;
            };
            saw_live_child = true;
            if let Some(join) = process.try_join() {
                self.record_reaped_child_usage(&child);
                let mut inner = self.inner.0.lock().unwrap();
                inner.children.retain(|existing| existing.pid != child.pid);
                drop(inner);
                control_plane.remove_process(child.pid);
                let code = join
                    .unwrap_or_else(|e| e.as_exit_code().unwrap_or_else(|| Errno::Canceled.into()));
                return Ok(Some((child.pid, code)));
            }
        }

        if !saw_matching_child {
            return Err(Errno::Child);
        }
        if saw_live_child {
            Ok(None)
        } else {
            Err(Errno::Child)
        }
    }

    pub fn try_join_any_child_matching_status<F>(
        &mut self,
        mut matches: F,
    ) -> Result<Option<(WasiProcess, TaskStatus)>, Errno>
    where
        F: FnMut(&WasiProcess) -> bool,
    {
        let children: Vec<_> = {
            let inner = self.inner.0.lock().unwrap();
            inner.children.clone()
        };
        if children.is_empty() {
            return Err(Errno::Child);
        }

        let mut saw_live_child = false;
        let mut saw_matching_child = false;
        for child in children {
            if !matches(&child) {
                continue;
            }
            saw_matching_child = true;
            let Some(process) = self.compute.must_upgrade().get_process(child.pid) else {
                let mut inner = self.inner.0.lock().unwrap();
                inner.children.retain(|existing| existing.pid != child.pid);
                continue;
            };
            match process.try_join_status() {
                Some(status) => {
                    self.record_reaped_child_usage(&child);
                    let mut inner = self.inner.0.lock().unwrap();
                    inner.children.retain(|existing| existing.pid != child.pid);
                    drop(inner);
                    self.compute.must_upgrade().remove_process(child.pid);
                    return Ok(Some((process, status)));
                }
                None => saw_live_child = true,
            }
        }

        if !saw_matching_child {
            return Err(Errno::Child);
        }
        if saw_live_child {
            Ok(None)
        } else {
            Err(Errno::Child)
        }
    }

    /// Terminate the process and all its threads
    pub fn terminate(&self, exit_code: ExitCode) {
        sinomo_debug!(
            "sinomo-debug process_terminate pid={} exit_code={}",
            self.pid().raw(),
            exit_code.raw()
        );
        let orphaned_stopped_groups = orphaned_stopped_process_groups_after_exit(self);
        let reparented_children = reparent_live_children_after_exit(self);
        self.finished.set_finished(Ok(exit_code));
        // FIXME: this is wrong, threads might still be running!
        // Need special logic for the main thread.
        let guard = self.inner.0.lock().unwrap();
        for thread in guard.threads.values() {
            thread.set_status_finished(Ok(exit_code))
        }
        drop(guard);

        self.notify_parent_child_status_changed();
        notify_new_parent_for_reparented_children(&reparented_children);
        signal_orphaned_stopped_process_groups(self, &orphaned_stopped_groups);
    }
}

fn reparent_live_children_after_exit(process: &WasiProcess) -> Vec<WasiProcess> {
    let exiting_pid = process.pid();
    let Some(control_plane) = process.compute.upgrade() else {
        return Vec::new();
    };

    let children = {
        let mut inner = process.inner.0.lock().unwrap();
        std::mem::take(&mut inner.children)
    };
    if children.is_empty() {
        return Vec::new();
    }

    let new_parent = if exiting_pid.raw() == 1 {
        None
    } else {
        nearest_live_subreaper_ancestor(process)
            .or_else(|| control_plane.get_process(WasiProcessId::from(1)))
    };
    let new_parent_pid = new_parent.as_ref().map(|parent| parent.pid());
    let mut reparented = Vec::new();

    for child in children {
        let Some(live_child) = control_plane.get_process(child.pid) else {
            continue;
        };
        let parent_death_signal = live_child.parent_death_signal();
        if parent_death_signal != Signal::Signone {
            live_child.signal_process_direct(parent_death_signal);
        }
        live_child.reparent_to(new_parent_pid);
        if let Some(parent) = new_parent.as_ref() {
            parent.add_child(live_child.clone());
        }
        reparented.push(live_child);
    }

    reparented
}

fn nearest_live_subreaper_ancestor(process: &WasiProcess) -> Option<WasiProcess> {
    let control_plane = process.compute.upgrade()?;
    let mut ancestor_pid = process.inner.0.lock().unwrap().parent_pid?;
    let mut visited = HashSet::new();

    while ancestor_pid.raw() != 0 && visited.insert(ancestor_pid) {
        let ancestor = control_plane.get_process(ancestor_pid)?;
        if ancestor.child_subreaper() {
            return Some(ancestor);
        }
        ancestor_pid = ancestor.inner.0.lock().unwrap().parent_pid?;
    }

    None
}

fn notify_new_parent_for_reparented_children(children: &[WasiProcess]) {
    for child in children {
        if child.task_status().is_finished() || child.task_status().stopped_signal().is_some() {
            child.notify_parent_child_status_changed();
        }
    }
}

fn orphaned_stopped_process_groups_after_exit(process: &WasiProcess) -> Vec<WasiProcessId> {
    let exiting_pid = process.pid();
    let exiting_session = process.session_id();
    let Some(control_plane) = process.compute.upgrade() else {
        return Vec::new();
    };
    let processes = control_plane.processes();
    let mut groups = Vec::new();

    for candidate in &processes {
        if candidate.pid() == exiting_pid || candidate.session_id() != exiting_session {
            continue;
        }
        if candidate.ppid() == exiting_pid {
            let pgid = candidate.pgid();
            if !groups.contains(&pgid) {
                groups.push(pgid);
            }
        }
    }

    groups
        .into_iter()
        .filter(|pgid| {
            process_group_has_stopped_member(&processes, exiting_session, *pgid, exiting_pid)
                && process_group_is_orphaned_after_exit(
                    &processes,
                    exiting_session,
                    *pgid,
                    exiting_pid,
                )
        })
        .collect()
}

fn process_group_has_stopped_member(
    processes: &[WasiProcess],
    session_id: WasiProcessId,
    pgid: WasiProcessId,
    exiting_pid: WasiProcessId,
) -> bool {
    processes.iter().any(|process| {
        process.pid() != exiting_pid
            && process.session_id() == session_id
            && process.pgid() == pgid
            && process.task_status().stopped_signal().is_some()
    })
}

fn process_group_is_orphaned_after_exit(
    processes: &[WasiProcess],
    session_id: WasiProcessId,
    pgid: WasiProcessId,
    exiting_pid: WasiProcessId,
) -> bool {
    for member in processes {
        if member.pid() == exiting_pid || member.session_id() != session_id || member.pgid() != pgid
        {
            continue;
        }

        let ppid = member.ppid();
        if ppid.raw() == 0 || ppid == exiting_pid {
            continue;
        }

        let Some(parent) = processes.iter().find(|process| process.pid() == ppid) else {
            continue;
        };

        if parent.session_id() == session_id
            && parent.pgid() != pgid
            && !parent.task_status().is_finished()
        {
            return false;
        }
    }
    true
}

fn signal_orphaned_stopped_process_groups(process: &WasiProcess, groups: &[WasiProcessId]) {
    if groups.is_empty() {
        return;
    }
    let exiting_pid = process.pid();
    let session_id = process.session_id();
    let Some(control_plane) = process.compute.upgrade() else {
        return;
    };
    let processes = control_plane.processes();
    for signal in [Signal::Sighup, Signal::Sigcont] {
        for target in &processes {
            if target.pid() != exiting_pid
                && target.session_id() == session_id
                && groups.contains(&target.pgid())
            {
                target.signal_process_direct(signal);
            }
        }
    }
}

/// Signals all the threads in this process
fn signal_process_internal(process: &WasiProcess, signal: Signal) {
    let process_inner = &process.inner;
    #[allow(unused_mut)]
    let mut guard = process_inner.0.lock().unwrap();
    let pid = guard.pid;
    tracing::trace!(%pid, "signal-process({:?})", signal);

    // If the snapshot on ctrl-c is currently registered then we need
    // to take a snapshot and exit
    #[cfg(feature = "journal")]
    {
        if signal == Signal::Sigint
            && (guard.snapshot_on.contains(&SnapshotTrigger::Sigint)
                || guard.snapshot_on.remove(&SnapshotTrigger::FirstSigint))
        {
            drop(guard);

            tracing::debug!(%pid, "snapshot-on-interrupt-signal");

            do_checkpoint_from_outside(
                process_inner,
                WasiProcessCheckpoint::Snapshot {
                    trigger: SnapshotTrigger::Sigint,
                },
            );
            return;
        };
    }

    // Check if there are subprocesses that will receive this signal
    // instead of this process
    if guard.waiting.load(Ordering::Acquire) > 0 {
        let mut triggered = false;
        for child in guard.children.iter() {
            child.signal_process(signal);
            triggered = true;
        }
        if triggered {
            return;
        }
    }

    if signal == Signal::Sigcont {
        if process.task_status().stopped_signal().is_some() {
            guard.unreported_continued = true;
        }
        guard.reported_stopped_signal = None;
    }
    signal_process_threads_with_mut_guard(&mut guard, signal);
    drop(guard);
    notify_parent_for_signal(process, signal);
}

fn signal_process_threads(process: &WasiProcess, signal: Signal) {
    let mut guard = process.inner.0.lock().unwrap();
    let pid = guard.pid;
    tracing::trace!(%pid, "signal-process-direct({:?})", signal);
    if signal == Signal::Sigcont {
        if process.task_status().stopped_signal().is_some() {
            guard.unreported_continued = true;
        }
        guard.reported_stopped_signal = None;
    }
    signal_process_threads_with_mut_guard(&mut guard, signal);
    drop(guard);
    notify_parent_for_signal(process, signal);
}

fn signal_terminal_process_group(
    process: &LockableWasiProcessInner,
    control_plane: &WasiControlPlaneHandle,
    signal: Signal,
) {
    let (pid, pgid) = {
        let guard = process.0.lock().unwrap();
        (guard.pid, guard.process_group_id)
    };
    tracing::trace!(%pid, %pgid, "signal-terminal-process-group({:?})", signal);

    let Some(control_plane) = control_plane.upgrade() else {
        signal_process_threads_for_inner(process, signal);
        return;
    };
    let mut delivered = false;
    for target in control_plane.processes() {
        if target.pgid() == pgid {
            target.signal_process_direct(signal);
            delivered = true;
        }
    }
    if !delivered {
        signal_process_threads_for_inner(process, signal);
    }
}

fn signal_process_threads_for_inner(process: &LockableWasiProcessInner, signal: Signal) {
    let mut guard = process.0.lock().unwrap();
    let pid = guard.pid;
    tracing::trace!(%pid, "signal-process-direct({:?})", signal);
    if signal == Signal::Sigcont {
        let was_stopped = guard
            .threads
            .values()
            .any(|thread| thread.status().stopped_signal().is_some());
        if was_stopped {
            guard.unreported_continued = true;
        }
        guard.reported_stopped_signal = None;
    }
    signal_process_threads_with_mut_guard(&mut guard, signal);
}

fn notify_parent_for_signal(process: &WasiProcess, signal: Signal) {
    if matches!(signal, Signal::Sigkill | Signal::Sigstop | Signal::Sigcont) {
        process.notify_parent_child_status_changed();
    }
}

fn signal_process_threads_with_mut_guard(
    guard: &mut MutexGuard<'_, WasiProcessInner>,
    signal: Signal,
) {
    if signal == Signal::Sigkill {
        for thread in guard.threads.values() {
            thread.set_or_get_exit_code_for_signal(signal);
        }
    } else if signal == Signal::Sigstop {
        for thread in guard.threads.values() {
            thread.set_status_stopped(signal);
        }
    } else if signal == Signal::Sigcont {
        for thread in guard.threads.values() {
            thread.set_status_continued();
        }
    }

    if matches!(signal, Signal::Sigkill | Signal::Sigstop | Signal::Sigcont) {
        for thread in guard.threads.values() {
            thread.signal(signal);
        }
        return;
    }

    if let Some(thread) = guard
        .threads
        .values()
        .find(|thread| !thread_blocks_signal(thread, signal))
    {
        thread.signal(signal);
    } else if !guard.pending_process_signals.contains(&signal) {
        guard.pending_process_signals.push(signal);
    }
}

fn signal_mask_bit(signal: Signal) -> Option<(u64, u64)> {
    let sig = signal as u8;
    if sig == 0 || sig > 128 {
        return None;
    }
    if sig <= 64 {
        Some((1u64 << (sig - 1), 0))
    } else {
        Some((0, 1u64 << (sig - 65)))
    }
}

fn thread_blocks_signal(thread: &WasiThread, signal: Signal) -> bool {
    let Some((low_bit, high_bit)) = signal_mask_bit(signal) else {
        return false;
    };
    let (low, high) = thread.signal_mask();
    (low & low_bit) != 0 || (high & high_bit) != 0
}

pub(crate) fn signal_default_terminates(signal: Signal) -> bool {
    matches!(
        signal,
        Signal::Sighup
            | Signal::Sigint
            | Signal::Sigquit
            | Signal::Sigill
            | Signal::Sigtrap
            | Signal::Sigabrt
            | Signal::Sigbus
            | Signal::Sigfpe
            | Signal::Sigkill
            | Signal::Sigusr1
            | Signal::Sigsegv
            | Signal::Sigusr2
            | Signal::Sigpipe
            | Signal::Sigalrm
            | Signal::Sigterm
            | Signal::Sigstkflt
            | Signal::Sigxcpu
            | Signal::Sigxfsz
            | Signal::Sigvtalrm
            | Signal::Sigprof
            | Signal::Sigpoll
            | Signal::Sigpwr
            | Signal::Sigsys
    )
}

pub(crate) fn signal_default_stops(signal: Signal) -> bool {
    matches!(
        signal,
        Signal::Sigstop | Signal::Sigtstp | Signal::Sigttin | Signal::Sigttou
    )
}

impl SignalHandlerAbi for WasiProcess {
    fn signal(&self, sig: u8) -> Result<(), SignalDeliveryError> {
        if let Ok(sig) = sig.try_into() {
            self.signal_process(sig);
            Ok(())
        } else {
            Err(SignalDeliveryError)
        }
    }
}
