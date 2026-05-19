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
mod types;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::Path,
    sync::Mutex,
    task::Waker,
    time::Duration,
};

#[cfg(feature = "enable-serde")]
use serde::{Deserialize, Serialize};
use virtual_fs::{FileOpener, FileSystem, FsError, OpenOptions, VirtualFile};
use wasmer_wasix_types::wasi::{
    Disposition, Errno, Fd as WasiFd, Rights, Signal, Snapshot0Clockid,
};

pub use self::{
    builder::*,
    env::{WasiEnv, WasiEnvInit, WasiModuleInstanceHandles, WasiModuleTreeHandles},
    func_env::WasiFunctionEnv,
    types::*,
};
pub use crate::fs::{InodeGuard, InodeWeakGuard};
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
    pub woken_pollers: BTreeSet<u64>,
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
    pub futexs: Mutex<WasiFutexState>,
    pub pidfds: Mutex<HashMap<WasiFd, u32>>,
    pub timerfds: Mutex<HashMap<WasiFd, WasiTimerFd>>,
    pub signalfds: Mutex<HashMap<WasiFd, WasiSignalFd>>,
    pub posix_timers: Mutex<HashMap<u32, WasiPosixTimer>>,
    pub interval_timers: Mutex<HashMap<i32, WasiIntervalTimer>>,
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
        WasiState {
            fs: self.fs.fork(),
            secret: self.secret,
            inodes: self.inodes.clone(),
            futexs: Default::default(),
            pidfds: Mutex::new(self.pidfds.lock().unwrap().clone()),
            timerfds: Mutex::new(self.timerfds.lock().unwrap().clone()),
            signalfds: Mutex::new(self.signalfds.lock().unwrap().clone()),
            posix_timers: Mutex::new(self.posix_timers.lock().unwrap().clone()),
            interval_timers: Mutex::new(self.interval_timers.lock().unwrap().clone()),
            signal_stack: Mutex::new(self.signal_stack.lock().unwrap().clone()),
            clock_offset: Mutex::new(self.clock_offset.lock().unwrap().clone()),
            args: Mutex::new(self.args.lock().unwrap().clone()),
            envs: Mutex::new(self.envs.lock().unwrap().clone()),
            signals: Mutex::new(self.signals.lock().unwrap().clone()),
            preopen: self.preopen.clone(),
        }
    }
}
