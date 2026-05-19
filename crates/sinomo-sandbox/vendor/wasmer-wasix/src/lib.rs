// FIXME: merge with ./lib.rs_upstream

#![allow(clippy::result_large_err)]
#![doc(html_favicon_url = "https://wasmer.io/images/icons/favicon-32x32.png")]
#![doc(html_logo_url = "https://github.com/wasmerio.png?size=200")]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! Wasmer's WASI implementation
//!
//! Use `generate_import_object` to create an [`Imports`].  This [`Imports`]
//! can be combined with a module to create an `Instance` which can execute WASI
//! Wasm functions.
//!
//! See `state` for the experimental WASI FS API.  Also see the
//! [WASI plugin example](https://github.com/wasmerio/wasmer/blob/main/examples/plugin.rs)
//! for an example of how to extend WASI using the WASI FS API.

#[cfg(all(
    not(feature = "sys"),
    not(feature = "js"),
    not(feature = "sys-minimal")
))]
compile_error!(
    "At least the `sys` or the `js` or `sys-minimal` feature must be enabled. Please, pick one."
);

#[cfg(any(
    all(feature = "js", feature = "sys"),
    all(feature = "js", feature = "sys-minimal")
))]
compile_error!(
    "Cannot have both `sys` and `js` or `sys-minimal` and `sys` features enabled at the same time. Please, pick one."
);

#[cfg(all(feature = "sys", target_arch = "wasm32"))]
compile_error!("The `sys` feature must be enabled only for non-`wasm32` target.");

#[cfg(all(feature = "js", not(target_arch = "wasm32")))]
compile_error!(
    "The `js` feature must be enabled only for the `wasm32` target (either `wasm32-unknown-unknown` or `wasm32-wasip1`)."
);

#[cfg(all(test, target_arch = "wasm32"))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

#[macro_use]
mod macros;
pub mod bin_factory;
pub mod os;
// TODO: should this be pub?
pub mod net;
// TODO: should this be pub?
pub mod capabilities;
pub mod fs;
pub mod http;
pub mod journal;
mod rewind;
pub mod runners;
pub mod runtime;
mod state;
mod syscalls;
mod utils;

use std::sync::Arc;

#[allow(unused_imports)]
use bytes::{Bytes, BytesMut};
use os::task::control_plane::ControlPlaneError;
use thiserror::Error;
// re-exports needed for OS
pub use wasmer;
pub use wasmer_wasix_types;

use wasmer::{
    AsStoreMut, Exports, FunctionEnv, Imports, Memory32, MemoryAccessError, MemorySize,
    RuntimeError, imports, namespace,
};

pub use virtual_fs;
pub use virtual_fs::{DuplexPipe, FsError, Pipe, VirtualFile, WasiBidirectionalSharedPipePair};
pub use virtual_net;
pub use virtual_net::{UnsupportedVirtualNetworking, VirtualNetworking};

#[cfg(feature = "host-vnet")]
pub use virtual_net::{
    host::{LocalNetworking, LocalTcpListener, LocalTcpStream, LocalUdpSocket},
    io_err_into_net_error,
};
use wasmer_wasix_types::wasi::{Errno, ExitCode};

pub use crate::{
    fs::{Fd, VIRTUAL_ROOT_FD, WasiFs, WasiInodes, default_fs_backing},
    os::{
        WasiTtyState,
        task::{
            control_plane::WasiControlPlane,
            process::{WasiProcess, WasiProcessId},
            thread::{WasiThread, WasiThreadError, WasiThreadHandle, WasiThreadId},
        },
    },
    rewind::*,
    runtime::{PluggableRuntime, Runtime, task_manager::VirtualTaskManager},
    state::{
        ALL_RIGHTS, WasiEnv, WasiEnvBuilder, WasiEnvInit, WasiFunctionEnv,
        WasiModuleInstanceHandles, WasiModuleTreeHandles, WasiStateCreationError,
    },
    syscalls::{journal::wait_for_snapshot, rewind, rewind_ext, types, unwind},
    utils::is_wasix_module,
    utils::{
        WasiVersion, get_wasi_version, get_wasi_versions, is_wasi_module,
        store::{StoreSnapshot, capture_store_snapshot, restore_store_snapshot},
    },
};

/// This is returned in `RuntimeError`.
/// Use `downcast` or `downcast_ref` to retrieve the `ExitCode`.
#[derive(Error, Debug)]
pub enum WasiError {
    #[error("WASI exited with code: {0}")]
    Exit(ExitCode),
    #[error("WASI thread exited")]
    ThreadExit,
    #[error("WASI deep sleep: {0:?}")]
    DeepSleep(DeepSleepWork),
    #[error("The WASI version could not be determined")]
    UnknownWasiVersion,
    #[error("Dynamically-linked symbol not found or has bad type: {0}")]
    DlSymbolResolutionFailed(String),
}

pub type WasiResult<T> = Result<Result<T, Errno>, WasiError>;

#[deny(unused, dead_code)]
#[derive(Error, Debug)]
pub enum SpawnError {
    /// Failed during serialization
    #[error("serialization failed")]
    Serialization,
    /// Failed during deserialization
    #[error("deserialization failed")]
    Deserialization,
    /// Invalid Wasmer process
    #[error("invalid wasmer")]
    InvalidWasmer,
    /// Failed to fetch the Wasmer process
    #[error("fetch failed")]
    FetchFailed,
    #[error(transparent)]
    CacheError(crate::runtime::module_cache::CacheError),
    /// Failed to compile the Wasmer process
    #[error("compile error: {error:?}")]
    CompileError {
        module_hash: wasmer_types::ModuleHash,
        error: wasmer::CompileError,
    },
    /// Invalid ABI
    #[error("Wasmer process has an invalid ABI")]
    InvalidABI,
    /// Bad handle
    #[error("bad handle")]
    BadHandle,
    /// Call is unsupported
    #[error("unsupported")]
    Unsupported,
    /// Not found
    #[error("not found: {message}")]
    NotFound { message: String },
    /// Tried to run the specified binary as a new WASI thread/process, but
    /// the binary name was not found.
    #[error("could not find binary '{binary}'")]
    BinaryNotFound { binary: String },
    #[error("could not find an entrypoint in the package '{package_id}'")]
    MissingEntrypoint {
        package_id: wasmer_config::package::PackageId,
    },
    #[error("could not load ")]
    ModuleLoad { message: String },
    /// Bad request
    #[error("bad request")]
    BadRequest,
    /// Access denied
    #[error("access denied")]
    AccessDenied,
    /// Internal error has occurred
    #[error("internal error")]
    InternalError,
    /// An error occurred while preparing the file system
    #[error(transparent)]
    FileSystemError(ExtendedFsError),
    /// Memory allocation failed
    #[error("memory allocation failed")]
    MemoryAllocationFailed,
    /// Memory access violation
    #[error("memory access violation")]
    MemoryAccessViolation,
    /// Some other unhandled error. If you see this, it's probably a bug.
    #[error("unknown error found")]
    UnknownError,
    #[error("runtime error")]
    Runtime(#[from] WasiRuntimeError),
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug)]
pub struct ExtendedFsError {
    pub error: virtual_fs::FsError,
    pub message: Option<String>,
}

impl ExtendedFsError {
    pub fn with_msg(error: virtual_fs::FsError, msg: impl Into<String>) -> Self {
        Self {
            error,
            message: Some(msg.into()),
        }
    }

    pub fn new(error: virtual_fs::FsError) -> Self {
        Self {
            error,
            message: None,
        }
    }
}

impl std::fmt::Display for ExtendedFsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "fs error: {}", self.error)?;

        if let Some(msg) = &self.message {
            write!(f, " | {msg}")?;
        }

        Ok(())
    }
}

impl std::error::Error for ExtendedFsError {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        Some(&self.error)
    }
}

impl SpawnError {
    /// Returns `true` if the spawn error is [`NotFound`].
    ///
    /// [`NotFound`]: SpawnError::NotFound
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            Self::NotFound { .. } | Self::MissingEntrypoint { .. } | Self::BinaryNotFound { .. }
        )
    }
}

#[derive(thiserror::Error, Debug)]
pub enum WasiRuntimeError {
    #[error("WASI state setup failed: {0}")]
    Init(#[from] WasiStateCreationError),
    #[error("Loading exports failed: {0}")]
    Export(#[from] wasmer::ExportError),
    #[error("Instantiation failed: {0}")]
    Instantiation(#[from] wasmer::InstantiationError),
    #[error("WASI error: {0}")]
    Wasi(#[from] WasiError),
    #[error("Process manager error: {0}")]
    ControlPlane(#[from] ControlPlaneError),
    #[error("{0}")]
    Runtime(#[from] RuntimeError),
    #[error("Memory access error: {0}")]
    Thread(#[from] WasiThreadError),
    #[error("{0}")]
    Anyhow(#[from] Arc<anyhow::Error>),
}

impl WasiRuntimeError {
    /// Retrieve the concrete exit code returned by an instance.
    ///
    /// Returns [`None`] if a general execution error occurred.
    pub fn as_exit_code(&self) -> Option<ExitCode> {
        if let WasiRuntimeError::Wasi(WasiError::Exit(code)) = self {
            Some(*code)
        } else if let WasiRuntimeError::Runtime(err) = self {
            if let Some(WasiError::Exit(code)) = err.downcast_ref() {
                Some(*code)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn display<'a>(&'a self, store: &'a mut impl AsStoreMut) -> WasiRuntimeErrorDisplay<'a> {
        if let WasiRuntimeError::Runtime(err) = self {
            WasiRuntimeErrorDisplay::Runtime(err.display(store))
        } else {
            WasiRuntimeErrorDisplay::Other(self)
        }
    }
}

pub enum WasiRuntimeErrorDisplay<'a> {
    Runtime(wasmer::RuntimeErrorDisplay<'a>),
    Other(&'a WasiRuntimeError),
}

impl std::fmt::Display for WasiRuntimeErrorDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WasiRuntimeErrorDisplay::Runtime(display) => write!(f, "{display}"),
            WasiRuntimeErrorDisplay::Other(err) => write!(f, "{err}"),
        }
    }
}

#[derive(Debug)]
pub struct WasiVFork {
    /// The information needed to rewind the stack with asyncify
    pub asyncify: Option<WasiVForkAsyncify>,

    /// The environment before the vfork occurred
    pub env: Box<WasiEnv>,

    /// Handle of the thread we have forked (dropping this handle
    /// will signal that the thread is dead)
    pub handle: WasiThreadHandle,
}

#[derive(Debug, Clone)]
pub struct WasiVForkAsyncify {
    /// The unwound stack before the vfork occurred
    pub rewind_stack: BytesMut,
    /// The mutable parts of the store
    pub store_data: Bytes,
    /// Whether the store is 64-bit
    pub is_64bit: bool,
}

impl Clone for WasiVFork {
    fn clone(&self) -> Self {
        Self {
            asyncify: self.asyncify.clone(),
            env: Box::new(self.env.as_ref().clone()),
            handle: self.handle.clone(),
        }
    }
}

/// Create an [`Imports`] with an existing [`WasiEnv`]. [`WasiEnv`] values are
/// typically constructed with [`WasiEnvBuilder`].
pub fn generate_import_object_from_env(
    store: &mut impl AsStoreMut,
    ctx: &FunctionEnv<WasiEnv>,
    version: WasiVersion,
) -> Imports {
    let mut imports = match version {
        WasiVersion::Snapshot0 => generate_import_object_snapshot0(store, ctx),
        WasiVersion::Snapshot1 | WasiVersion::Latest => {
            generate_import_object_snapshot1(store, ctx)
        }
        WasiVersion::Wasix32v1 => generate_import_object_wasix32_v1(store, ctx),
        WasiVersion::Wasix64v1 => generate_import_object_wasix64_v1(store, ctx),
    };

    let exports_wasi_generic = wasi_exports_generic(store, ctx);

    let imports_wasi_generic = imports! {
        "wasi" => exports_wasi_generic,
    };

    imports.extend(&imports_wasi_generic);

    imports
}

fn wasi_exports_generic(mut store: &mut impl AsStoreMut, env: &FunctionEnv<WasiEnv>) -> Exports {
    use syscalls::*;
    let namespace = namespace! {
        "thread-spawn" => Function::new_typed_with_env(&mut store, env, thread_spawn::<Memory32>),
    };
    namespace
}

fn wasi_unstable_exports(mut store: &mut impl AsStoreMut, env: &FunctionEnv<WasiEnv>) -> Exports {
    use syscalls::*;
    let namespace = namespace! {
        "args_get" => Function::new_typed_with_env(&mut store, env, args_get::<Memory32>),
        "args_sizes_get" => Function::new_typed_with_env(&mut store, env, args_sizes_get::<Memory32>),
        "clock_res_get" => Function::new_typed_with_env(&mut store, env, clock_res_get::<Memory32>),
        "clock_time_get" => Function::new_typed_with_env(&mut store, env, clock_time_get::<Memory32>),
        "environ_get" => Function::new_typed_with_env(&mut store, env, environ_get::<Memory32>),
        "environ_sizes_get" => Function::new_typed_with_env(&mut store, env, environ_sizes_get::<Memory32>),
        "fd_advise" => Function::new_typed_with_env(&mut store, env, fd_advise),
        "fd_allocate" => Function::new_typed_with_env(&mut store, env, fd_allocate),
        "fd_close" => Function::new_typed_with_env(&mut store, env, fd_close),
        "fd_datasync" => Function::new_typed_with_env(&mut store, env, fd_datasync),
        "fd_fdstat_get" => Function::new_typed_with_env(&mut store, env, fd_fdstat_get::<Memory32>),
        "fd_fdstat_set_flags" => Function::new_typed_with_env(&mut store, env, fd_fdstat_set_flags),
        "fd_fdstat_set_rights" => Function::new_typed_with_env(&mut store, env, fd_fdstat_set_rights),
        "fd_filestat_get" => Function::new_typed_with_env(&mut store, env, legacy::snapshot0::fd_filestat_get),
        "fd_filestat_set_size" => Function::new_typed_with_env(&mut store, env, fd_filestat_set_size),
        "fd_filestat_set_times" => Function::new_typed_with_env(&mut store, env, fd_filestat_set_times),
        "fd_pread" => Function::new_typed_with_env(&mut store, env, fd_pread::<Memory32>),
        "fd_prestat_get" => Function::new_typed_with_env(&mut store, env, fd_prestat_get::<Memory32>),
        "fd_prestat_dir_name" => Function::new_typed_with_env(&mut store, env, fd_prestat_dir_name::<Memory32>),
        "fd_pwrite" => Function::new_typed_with_env(&mut store, env, fd_pwrite::<Memory32>),
        "fd_read" => Function::new_typed_with_env(&mut store, env, fd_read::<Memory32>),
        "fd_readdir" => Function::new_typed_with_env(&mut store, env, fd_readdir::<Memory32>),
        "fd_renumber" => Function::new_typed_with_env(&mut store, env, fd_renumber),
        "fd_seek" => Function::new_typed_with_env(&mut store, env, legacy::snapshot0::fd_seek),
        "fd_sync" => Function::new_typed_with_env(&mut store, env, fd_sync),
        "fd_tell" => Function::new_typed_with_env(&mut store, env, fd_tell::<Memory32>),
        "fd_write" => Function::new_typed_with_env(&mut store, env, fd_write::<Memory32>),
        "path_create_directory" => Function::new_typed_with_env(&mut store, env, path_create_directory::<Memory32>),
        "path_filestat_get" => Function::new_typed_with_env(&mut store, env, legacy::snapshot0::path_filestat_get),
        "path_filestat_set_times" => Function::new_typed_with_env(&mut store, env, path_filestat_set_times::<Memory32>),
        "path_link" => Function::new_typed_with_env(&mut store, env, path_link::<Memory32>),
        "path_open" => Function::new_typed_with_env(&mut store, env, path_open::<Memory32>),
        "path_readlink" => Function::new_typed_with_env(&mut store, env, path_readlink::<Memory32>),
        "path_remove_directory" => Function::new_typed_with_env(&mut store, env, path_remove_directory::<Memory32>),
        "path_rename" => Function::new_typed_with_env(&mut store, env, path_rename::<Memory32>),
        "path_symlink" => Function::new_typed_with_env(&mut store, env, path_symlink::<Memory32>),
        "path_unlink_file" => Function::new_typed_with_env(&mut store, env, path_unlink_file::<Memory32>),
        "poll_oneoff" => Function::new_typed_with_env(&mut store, env, legacy::snapshot0::poll_oneoff::<Memory32>),
        "proc_exit" => Function::new_typed_with_env(&mut store, env, proc_exit::<Memory32>),
        "proc_raise" => Function::new_typed_with_env(&mut store, env, proc_raise),
        "random_get" => Function::new_typed_with_env(&mut store, env, random_get::<Memory32>),
        "sched_yield" => Function::new_typed_with_env(&mut store, env, sched_yield::<Memory32>),
        "sock_recv" => Function::new_typed_with_env(&mut store, env, sock_recv::<Memory32>),
        "sock_send" => Function::new_typed_with_env(&mut store, env, sock_send::<Memory32>),
        "sock_shutdown" => Function::new_typed_with_env(&mut store, env, sock_shutdown),
        "thread-spawn" => Function::new_typed_with_env(&mut store, env, thread_spawn::<Memory32>),
    };
    namespace
}

fn wasi_snapshot_preview1_exports(
    mut store: &mut impl AsStoreMut,
    env: &FunctionEnv<WasiEnv>,
) -> Exports {
    use syscalls::*;
    let namespace = namespace! {
        "args_get" => Function::new_typed_with_env(&mut store, env, args_get::<Memory32>),
        "args_sizes_get" => Function::new_typed_with_env(&mut store, env, args_sizes_get::<Memory32>),
        "clock_res_get" => Function::new_typed_with_env(&mut store, env, clock_res_get::<Memory32>),
        "clock_time_get" => Function::new_typed_with_env(&mut store, env, clock_time_get::<Memory32>),
        "environ_get" => Function::new_typed_with_env(&mut store, env, environ_get::<Memory32>),
        "environ_sizes_get" => Function::new_typed_with_env(&mut store, env, environ_sizes_get::<Memory32>),
        "fd_advise" => Function::new_typed_with_env(&mut store, env, fd_advise),
        "fd_allocate" => Function::new_typed_with_env(&mut store, env, fd_allocate),
        "fd_close" => Function::new_typed_with_env(&mut store, env, fd_close),
        "fd_datasync" => Function::new_typed_with_env(&mut store, env, fd_datasync),
        "fd_fdstat_get" => Function::new_typed_with_env(&mut store, env, fd_fdstat_get::<Memory32>),
        "fd_fdstat_set_flags" => Function::new_typed_with_env(&mut store, env, fd_fdstat_set_flags),
        "fd_fdstat_set_rights" => Function::new_typed_with_env(&mut store, env, fd_fdstat_set_rights),
        "fd_filestat_get" => Function::new_typed_with_env(&mut store, env, fd_filestat_get::<Memory32>),
        "fd_filestat_set_size" => Function::new_typed_with_env(&mut store, env, fd_filestat_set_size),
        "fd_filestat_set_times" => Function::new_typed_with_env(&mut store, env, fd_filestat_set_times),
        "fd_pread" => Function::new_typed_with_env(&mut store, env, fd_pread::<Memory32>),
        "fd_prestat_get" => Function::new_typed_with_env(&mut store, env, fd_prestat_get::<Memory32>),
        "fd_prestat_dir_name" => Function::new_typed_with_env(&mut store, env, fd_prestat_dir_name::<Memory32>),
        "fd_pwrite" => Function::new_typed_with_env(&mut store, env, fd_pwrite::<Memory32>),
        "fd_read" => Function::new_typed_with_env(&mut store, env, fd_read::<Memory32>),
        "fd_readdir" => Function::new_typed_with_env(&mut store, env, fd_readdir::<Memory32>),
        "fd_renumber" => Function::new_typed_with_env(&mut store, env, fd_renumber),
        "fd_seek" => Function::new_typed_with_env(&mut store, env, fd_seek::<Memory32>),
        "fd_sync" => Function::new_typed_with_env(&mut store, env, fd_sync),
        "fd_tell" => Function::new_typed_with_env(&mut store, env, fd_tell::<Memory32>),
        "fd_write" => Function::new_typed_with_env(&mut store, env, fd_write::<Memory32>),
        "path_create_directory" => Function::new_typed_with_env(&mut store, env, path_create_directory::<Memory32>),
        "path_filestat_get" => Function::new_typed_with_env(&mut store, env, path_filestat_get::<Memory32>),
        "path_filestat_set_times" => Function::new_typed_with_env(&mut store, env, path_filestat_set_times::<Memory32>),
        "path_link" => Function::new_typed_with_env(&mut store, env, path_link::<Memory32>),
        "path_open" => Function::new_typed_with_env(&mut store, env, path_open::<Memory32>),
        "path_readlink" => Function::new_typed_with_env(&mut store, env, path_readlink::<Memory32>),
        "path_remove_directory" => Function::new_typed_with_env(&mut store, env, path_remove_directory::<Memory32>),
        "path_rename" => Function::new_typed_with_env(&mut store, env, path_rename::<Memory32>),
        "path_symlink" => Function::new_typed_with_env(&mut store, env, path_symlink::<Memory32>),
        "path_unlink_file" => Function::new_typed_with_env(&mut store, env, path_unlink_file::<Memory32>),
        "poll_oneoff" => Function::new_typed_with_env(&mut store, env, poll_oneoff::<Memory32>),
        "proc_exit" => Function::new_typed_with_env(&mut store, env, proc_exit::<Memory32>),
        "proc_raise" => Function::new_typed_with_env(&mut store, env, proc_raise),
        "random_get" => Function::new_typed_with_env(&mut store, env, random_get::<Memory32>),
        "sched_yield" => Function::new_typed_with_env(&mut store, env, sched_yield::<Memory32>),
        "sock_accept" => Function::new_typed_with_env(&mut store, env, sock_accept::<Memory32>),
        "sock_recv" => Function::new_typed_with_env(&mut store, env, sock_recv::<Memory32>),
        "sock_send" => Function::new_typed_with_env(&mut store, env, sock_send::<Memory32>),
        "sock_shutdown" => Function::new_typed_with_env(&mut store, env, sock_shutdown),
        "thread-spawn" => Function::new_typed_with_env(&mut store, env, thread_spawn::<Memory32>),
    };
    namespace
}

fn wasix_exports_32(mut store: &mut impl AsStoreMut, env: &FunctionEnv<WasiEnv>) -> Exports {
    let engine_supports_async = store.as_store_ref().engine().supports_async();

    use syscalls::*;
    let namespace = namespace! {
        "args_get" => Function::new_typed_with_env(&mut store, env, args_get::<Memory32>),
        "args_sizes_get" => Function::new_typed_with_env(&mut store, env, args_sizes_get::<Memory32>),
        "call_dynamic" => Function::new_typed_with_env(&mut store, env, call_dynamic::<Memory32>),
        "reflect_signature" => Function::new_typed_with_env(&mut store, env, reflect_signature::<Memory32>),
        "clock_res_get" => Function::new_typed_with_env(&mut store, env, clock_res_get::<Memory32>),
        "clock_time_get" => Function::new_typed_with_env(&mut store, env, clock_time_get::<Memory32>),
        "clock_getres" => Function::new_typed_with_env(&mut store, env, clock_getres::<Memory32>),
        "clock_gettime" => Function::new_typed_with_env(&mut store, env, clock_gettime::<Memory32>),
        "clock_time_set" => Function::new_typed_with_env(&mut store, env, clock_time_set),
        "closure_prepare" => Function::new_typed_with_env(&mut store, env, closure_prepare::<Memory32>),
        "closure_allocate" => Function::new_typed_with_env(&mut store, env, closure_allocate::<Memory32>),
        "closure_free" => Function::new_typed_with_env(&mut store, env, closure_free),
        "environ_get" => Function::new_typed_with_env(&mut store, env, environ_get::<Memory32>),
        "environ_sizes_get" => Function::new_typed_with_env(&mut store, env, environ_sizes_get::<Memory32>),
        "epoll_create" => Function::new_typed_with_env(&mut store, env, epoll_create::<Memory32>),
        "epoll_ctl" => Function::new_typed_with_env(&mut store, env, epoll_ctl::<Memory32>),
        "epoll_wait" => Function::new_typed_with_env(&mut store, env, epoll_wait::<Memory32>),
        "fd_advise" => Function::new_typed_with_env(&mut store, env, fd_advise),
        "fd_allocate" => Function::new_typed_with_env(&mut store, env, fd_allocate),
        "fd_close" => Function::new_typed_with_env(&mut store, env, fd_close),
        "fd_datasync" => Function::new_typed_with_env(&mut store, env, fd_datasync),
        "fd_fdstat_get" => Function::new_typed_with_env(&mut store, env, fd_fdstat_get::<Memory32>),
        "fd_fdstat_set_flags" => Function::new_typed_with_env(&mut store, env, fd_fdstat_set_flags),
        "fd_fdstat_set_rights" => Function::new_typed_with_env(&mut store, env, fd_fdstat_set_rights),
        "fd_filestat_get" => Function::new_typed_with_env(&mut store, env, fd_filestat_get::<Memory32>),
        "fd_filestat_set_size" => Function::new_typed_with_env(&mut store, env, fd_filestat_set_size),
        "fd_filestat_set_times" => Function::new_typed_with_env(&mut store, env, fd_filestat_set_times),
        "fd_pread" => Function::new_typed_with_env(&mut store, env, fd_pread::<Memory32>),
        "fd_prestat_get" => Function::new_typed_with_env(&mut store, env, fd_prestat_get::<Memory32>),
        "fd_prestat_dir_name" => Function::new_typed_with_env(&mut store, env, fd_prestat_dir_name::<Memory32>),
        "fd_pwrite" => Function::new_typed_with_env(&mut store, env, fd_pwrite::<Memory32>),
        "fd_read" => Function::new_typed_with_env(&mut store, env, fd_read::<Memory32>),
        "fd_readdir" => Function::new_typed_with_env(&mut store, env, fd_readdir::<Memory32>),
        "fd_renumber" => Function::new_typed_with_env(&mut store, env, fd_renumber),
        "fd_dup" => Function::new_typed_with_env(&mut store, env, fd_dup::<Memory32>),
        "dup" => Function::new_typed_with_env(&mut store, env, dup),
        "dup2" => Function::new_typed_with_env(&mut store, env, dup2),
        "dup3" => Function::new_typed_with_env(&mut store, env, dup3),
        "fd_dup2" => Function::new_typed_with_env(&mut store, env, fd_dup2::<Memory32>),
        "fd_fdflags_get" => Function::new_typed_with_env(&mut store, env, fd_fdflags_get::<Memory32>),
        "fd_fdflags_set" => Function::new_typed_with_env(&mut store, env, fd_fdflags_set),
        "fd_event" => Function::new_typed_with_env(&mut store, env, fd_event::<Memory32>),
        "eventfd" => Function::new_typed_with_env(&mut store, env, eventfd),
        "timerfd_create" => Function::new_typed_with_env(&mut store, env, timerfd_create),
        "timerfd_settime" => Function::new_typed_with_env(&mut store, env, timerfd_settime::<Memory32>),
        "timerfd_gettime" => Function::new_typed_with_env(&mut store, env, timerfd_gettime::<Memory32>),
        "timerfd_read" => Function::new_typed_with_env(&mut store, env, timerfd_read),
        "timerfd_ready" => Function::new_typed_with_env(&mut store, env, timerfd_ready),
        "timerfd_remaining_ms" => Function::new_typed_with_env(&mut store, env, timerfd_remaining_ms),
        "timerfd_known" => Function::new_typed_with_env(&mut store, env, timerfd_known),
        "posix_timer_create" => Function::new_typed_with_env(&mut store, env, posix_timer_create),
        "posix_timer_settime" => Function::new_typed_with_env(&mut store, env, posix_timer_settime::<Memory32>),
        "posix_timer_gettime" => Function::new_typed_with_env(&mut store, env, posix_timer_gettime::<Memory32>),
        "posix_timer_getoverrun" => Function::new_typed_with_env(&mut store, env, posix_timer_getoverrun),
        "posix_timer_delete" => Function::new_typed_with_env(&mut store, env, posix_timer_delete),
        "itimer_settime" => Function::new_typed_with_env(&mut store, env, itimer_settime::<Memory32>),
        "itimer_gettime" => Function::new_typed_with_env(&mut store, env, itimer_gettime::<Memory32>),
        "signalfd" => Function::new_typed_with_env(&mut store, env, signalfd),
        "signalfd_known" => Function::new_typed_with_env(&mut store, env, signalfd_known),
        "signalfd_mask_low" => Function::new_typed_with_env(&mut store, env, signalfd_mask_low),
        "signalfd_mask_high" => Function::new_typed_with_env(&mut store, env, signalfd_mask_high),
        "fd_seek" => Function::new_typed_with_env(&mut store, env, fd_seek::<Memory32>),
        "fd_sync" => Function::new_typed_with_env(&mut store, env, fd_sync),
        "fd_tell" => Function::new_typed_with_env(&mut store, env, fd_tell::<Memory32>),
        "fd_write" => Function::new_typed_with_env(&mut store, env, fd_write::<Memory32>),
        "fd_pipe" => Function::new_typed_with_env(&mut store, env, fd_pipe::<Memory32>),
        "pipe" => Function::new_typed_with_env(&mut store, env, pipe::<Memory32>),
        "pipe2" => Function::new_typed_with_env(&mut store, env, pipe2::<Memory32>),
        "path_create_directory" => Function::new_typed_with_env(&mut store, env, path_create_directory::<Memory32>),
        "path_filestat_get" => Function::new_typed_with_env(&mut store, env, path_filestat_get::<Memory32>),
        "path_filestat_set_times" => Function::new_typed_with_env(&mut store, env, path_filestat_set_times::<Memory32>),
        "path_link" => Function::new_typed_with_env(&mut store, env, path_link::<Memory32>),
        "path_open" => Function::new_typed_with_env(&mut store, env, path_open::<Memory32>),
        "path_open2" => Function::new_typed_with_env(&mut store, env, path_open2::<Memory32>),
        "path_readlink" => Function::new_typed_with_env(&mut store, env, path_readlink::<Memory32>),
        "path_remove_directory" => Function::new_typed_with_env(&mut store, env, path_remove_directory::<Memory32>),
        "path_rename" => Function::new_typed_with_env(&mut store, env, path_rename::<Memory32>),
        "path_symlink" => Function::new_typed_with_env(&mut store, env, path_symlink::<Memory32>),
        "path_unlink_file" => Function::new_typed_with_env(&mut store, env, path_unlink_file::<Memory32>),
        "poll_oneoff" => Function::new_typed_with_env(&mut store, env, poll_oneoff::<Memory32>),
        "proc_exit" => Function::new_typed_with_env(&mut store, env, proc_exit::<Memory32>),
        "proc_fork" => Function::new_typed_with_env(&mut store, env, proc_fork::<Memory32>),
        "fork" => Function::new_typed_with_env(&mut store, env, fork::<Memory32>),
        "vfork" => Function::new_typed_with_env(&mut store, env, vfork::<Memory32>),
        "clone_thread" => Function::new_typed_with_env(&mut store, env, clone_thread::<Memory32>),
        "proc_fork_env" => Function::new_typed_with_env(&mut store, env, proc_fork_env::<Memory32>),
        "proc_join" => Function::new_typed_with_env(&mut store, env, proc_join::<Memory32>),
        "waitpid" => Function::new_typed_with_env(&mut store, env, waitpid::<Memory32>),
        "waitid" => Function::new_typed_with_env(&mut store, env, waitid::<Memory32>),
        "proc_wait_status" => Function::new_typed_with_env(&mut store, env, proc_wait_status::<Memory32>),
        "proc_wait_status_select" => Function::new_typed_with_env(&mut store, env, proc_wait_status_select::<Memory32>),
        "proc_signal" => Function::new_typed_with_env(&mut store, env, proc_signal),
        "kill" => Function::new_typed_with_env(&mut store, env, kill),
        "proc_signals_get" => Function::new_typed_with_env(&mut store, env, proc_signals_get::<Memory32>),
        "proc_signals_sizes_get" => Function::new_typed_with_env(&mut store, env, proc_signals_sizes_get::<Memory32>),
        "proc_exec" => Function::new_typed_with_env(&mut store, env, proc_exec::<Memory32>),
        "proc_exec2" => Function::new_typed_with_env(&mut store, env, proc_exec2::<Memory32>),
        "proc_exec3" => Function::new_typed_with_env(&mut store, env, proc_exec3::<Memory32>),
        "execv" => Function::new_typed_with_env(&mut store, env, execv::<Memory32>),
        "execve" => Function::new_typed_with_env(&mut store, env, execve::<Memory32>),
        "execvp" => Function::new_typed_with_env(&mut store, env, execvp::<Memory32>),
        "execvpe" => Function::new_typed_with_env(&mut store, env, execvpe::<Memory32>),
        "proc_exit2" => Function::new_typed_with_env(&mut store, env, proc_exit2::<Memory32>),
        "_exit" => Function::new_typed_with_env(&mut store, env, proc_exit2::<Memory32>),
        "proc_raise" => Function::new_typed_with_env(&mut store, env, proc_raise),
        "raise" => Function::new_typed_with_env(&mut store, env, raise),
        "proc_raise_interval" => Function::new_typed_with_env(&mut store, env, proc_raise_interval),
        "sigaltstack_set" => Function::new_typed_with_env(&mut store, env, sigaltstack_set::<Memory32>),
        "sigaltstack_get" => Function::new_typed_with_env(&mut store, env, sigaltstack_get::<Memory32>),
        "signal_mask_set" => Function::new_typed_with_env(&mut store, env, signal_mask_set),
        "signal_mask_get" => Function::new_typed_with_env(&mut store, env, signal_mask_get::<Memory32>),
        "unshare" => Function::new_typed_with_env(&mut store, env, unshare),
        "setns" => Function::new_typed_with_env(&mut store, env, setns),
        "proc_snapshot" => Function::new_typed_with_env(&mut store, env, proc_snapshot::<Memory32>),
        "proc_spawn" => Function::new_typed_with_env(&mut store, env, proc_spawn::<Memory32>),
        "process_spawn" => Function::new_typed_with_env(&mut store, env, proc_spawn::<Memory32>),
        "proc_spawn2" => Function::new_typed_with_env(&mut store, env, proc_spawn2::<Memory32>),
        "proc_umask" => Function::new_typed_with_env(&mut store, env, proc_umask::<Memory32>),
        "proc_getuid" => Function::new_typed_with_env(&mut store, env, proc_getuid::<Memory32>),
        "getuid" => Function::new_typed_with_env(&mut store, env, getuid),
        "geteuid" => Function::new_typed_with_env(&mut store, env, geteuid),
        "proc_getresuid" => Function::new_typed_with_env(&mut store, env, proc_getresuid::<Memory32>),
        "getresuid" => Function::new_typed_with_env(&mut store, env, proc_getresuid::<Memory32>),
        "proc_getgid" => Function::new_typed_with_env(&mut store, env, proc_getgid::<Memory32>),
        "getgid" => Function::new_typed_with_env(&mut store, env, getgid),
        "getegid" => Function::new_typed_with_env(&mut store, env, getegid),
        "proc_getresgid" => Function::new_typed_with_env(&mut store, env, proc_getresgid::<Memory32>),
        "getresgid" => Function::new_typed_with_env(&mut store, env, proc_getresgid::<Memory32>),
        "proc_getgroups" => Function::new_typed_with_env(&mut store, env, proc_getgroups::<Memory32>),
        "getgroups" => Function::new_typed_with_env(&mut store, env, proc_getgroups::<Memory32>),
        "proc_setresuid" => Function::new_typed_with_env(&mut store, env, proc_setresuid),
        "setuid" => Function::new_typed_with_env(&mut store, env, setuid),
        "seteuid" => Function::new_typed_with_env(&mut store, env, seteuid),
        "setresuid" => Function::new_typed_with_env(&mut store, env, proc_setresuid),
        "proc_setresgid" => Function::new_typed_with_env(&mut store, env, proc_setresgid),
        "setgid" => Function::new_typed_with_env(&mut store, env, setgid),
        "setegid" => Function::new_typed_with_env(&mut store, env, setegid),
        "setresgid" => Function::new_typed_with_env(&mut store, env, proc_setresgid),
        "proc_setgroups" => Function::new_typed_with_env(&mut store, env, proc_setgroups::<Memory32>),
        "setgroups" => Function::new_typed_with_env(&mut store, env, proc_setgroups::<Memory32>),
        "proc_id" => Function::new_typed_with_env(&mut store, env, proc_id::<Memory32>),
        "getpid" => Function::new_typed_with_env(&mut store, env, getpid),
        "proc_parent" => Function::new_typed_with_env(&mut store, env, proc_parent::<Memory32>),
        "getppid" => Function::new_typed_with_env(&mut store, env, getppid),
        "proc_inherit_parent" => Function::new_typed_with_env(&mut store, env, proc_inherit_parent),
        "proc_set_parent_death_signal" => Function::new_typed_with_env(&mut store, env, proc_set_parent_death_signal),
        "proc_get_parent_death_signal" => Function::new_typed_with_env(&mut store, env, proc_get_parent_death_signal::<Memory32>),
        "proc_set_child_subreaper" => Function::new_typed_with_env(&mut store, env, proc_set_child_subreaper),
        "proc_get_child_subreaper" => Function::new_typed_with_env(&mut store, env, proc_get_child_subreaper::<Memory32>),
        "proc_set_name" => Function::new_typed_with_env(&mut store, env, proc_set_name::<Memory32>),
        "proc_get_name" => Function::new_typed_with_env(&mut store, env, proc_get_name::<Memory32>),
        "proc_set_uts_name" => Function::new_typed_with_env(&mut store, env, proc_set_uts_name::<Memory32>),
        "proc_get_uts_name" => Function::new_typed_with_env(&mut store, env, proc_get_uts_name::<Memory32>),
        "proc_set_dumpable" => Function::new_typed_with_env(&mut store, env, proc_set_dumpable),
        "proc_get_dumpable" => Function::new_typed_with_env(&mut store, env, proc_get_dumpable::<Memory32>),
        "proc_set_no_new_privs" => Function::new_typed_with_env(&mut store, env, proc_set_no_new_privs),
        "proc_get_no_new_privs" => Function::new_typed_with_env(&mut store, env, proc_get_no_new_privs::<Memory32>),
        "proc_set_seccomp_mode" => Function::new_typed_with_env(&mut store, env, proc_set_seccomp_mode),
        "proc_get_seccomp_mode" => Function::new_typed_with_env(&mut store, env, proc_get_seccomp_mode::<Memory32>),
        "proc_set_timer_slack" => Function::new_typed_with_env(&mut store, env, proc_set_timer_slack),
        "proc_get_timer_slack" => Function::new_typed_with_env(&mut store, env, proc_get_timer_slack::<Memory32>),
        "proc_personality" => Function::new_typed_with_env(&mut store, env, proc_personality::<Memory32>),
        "proc_set_clear_child_tid" => Function::new_typed_with_env(&mut store, env, proc_set_clear_child_tid),
        "proc_get_clear_child_tid" => Function::new_typed_with_env(&mut store, env, proc_get_clear_child_tid::<Memory32>),
        "set_tid_address" => Function::new_typed_with_env(&mut store, env, set_tid_address),
        "get_clear_child_tid" => Function::new_typed_with_env(&mut store, env, get_clear_child_tid::<Memory32>),
        "proc_set_robust_list" => Function::new_typed_with_env(&mut store, env, proc_set_robust_list),
        "proc_get_robust_list" => Function::new_typed_with_env(&mut store, env, proc_get_robust_list::<Memory32>),
        "set_robust_list" => Function::new_typed_with_env(&mut store, env, set_robust_list),
        "get_robust_list" => Function::new_typed_with_env(&mut store, env, get_robust_list::<Memory32>),
        "proc_get_capabilities" => Function::new_typed_with_env(&mut store, env, proc_get_capabilities::<Memory32>),
        "proc_set_capabilities" => Function::new_typed_with_env(&mut store, env, proc_set_capabilities),
        "proc_get_capability_bounding" => Function::new_typed_with_env(&mut store, env, proc_get_capability_bounding::<Memory32>),
        "proc_drop_capability_bounding" => Function::new_typed_with_env(&mut store, env, proc_drop_capability_bounding),
        "proc_set_process_control" => Function::new_typed_with_env(&mut store, env, proc_set_process_control),
        "proc_get_process_control" => Function::new_typed_with_env(&mut store, env, proc_get_process_control::<Memory32>),
        "proc_unshare_namespaces" => Function::new_typed_with_env(&mut store, env, proc_unshare_namespaces),
        "proc_set_arch_base" => Function::new_typed_with_env(&mut store, env, proc_set_arch_base),
        "proc_get_arch_base" => Function::new_typed_with_env(&mut store, env, proc_get_arch_base::<Memory32>),
        "arch_prctl" => Function::new_typed_with_env(&mut store, env, arch_prctl::<Memory32>),
        "proc_set_rseq" => Function::new_typed_with_env(&mut store, env, proc_set_rseq),
        "proc_clear_rseq" => Function::new_typed_with_env(&mut store, env, proc_clear_rseq),
        "rseq" => Function::new_typed_with_env(&mut store, env, rseq),
        "membarrier" => Function::new_typed_with_env(&mut store, env, membarrier),
        "proc_set_mempolicy" => Function::new_typed_with_env(&mut store, env, proc_set_mempolicy),
        "proc_get_mempolicy" => Function::new_typed_with_env(&mut store, env, proc_get_mempolicy::<Memory32>),
        "proc_set_mempolicy_home_node" => Function::new_typed_with_env(&mut store, env, proc_set_mempolicy_home_node),
        "proc_set_ioprio" => Function::new_typed_with_env(&mut store, env, proc_set_ioprio),
        "proc_get_ioprio" => Function::new_typed_with_env(&mut store, env, proc_get_ioprio::<Memory32>),
        "proc_getpgid" => Function::new_typed_with_env(&mut store, env, proc_getpgid::<Memory32>),
        "getpgid" => Function::new_typed_with_env(&mut store, env, getpgid),
        "proc_getpgrp" => Function::new_typed_with_env(&mut store, env, proc_getpgrp::<Memory32>),
        "getpgrp" => Function::new_typed_with_env(&mut store, env, getpgrp),
        "proc_getsid" => Function::new_typed_with_env(&mut store, env, proc_getsid::<Memory32>),
        "getsid" => Function::new_typed_with_env(&mut store, env, getsid),
        "proc_setpgid" => Function::new_typed_with_env(&mut store, env, proc_setpgid),
        "setpgid" => Function::new_typed_with_env(&mut store, env, setpgid),
        "proc_setsid" => Function::new_typed_with_env(&mut store, env, proc_setsid::<Memory32>),
        "setsid" => Function::new_typed_with_env(&mut store, env, setsid),
        "proc_getpriority" => Function::new_typed_with_env(&mut store, env, proc_getpriority::<Memory32>),
        "getpriority" => Function::new_typed_with_env(&mut store, env, getpriority),
        "proc_setpriority" => Function::new_typed_with_env(&mut store, env, proc_setpriority),
        "setpriority" => Function::new_typed_with_env(&mut store, env, setpriority),
        "nice" => Function::new_typed_with_env(&mut store, env, nice),
        "proc_getrusage" => Function::new_typed_with_env(&mut store, env, proc_getrusage::<Memory32>),
        "getrusage" => Function::new_typed_with_env(&mut store, env, getrusage::<Memory32>),
        "proc_times" => Function::new_typed_with_env(&mut store, env, proc_times::<Memory32>),
        "proc_getrlimit" => Function::new_typed_with_env(&mut store, env, proc_getrlimit::<Memory32>),
        "getrlimit" => Function::new_typed_with_env(&mut store, env, getrlimit::<Memory32>),
        "proc_setrlimit" => Function::new_typed_with_env(&mut store, env, proc_setrlimit),
        "setrlimit" => Function::new_typed_with_env(&mut store, env, setrlimit),
        "prlimit" => Function::new_typed_with_env(&mut store, env, prlimit::<Memory32>),
        "proc_getscheduler" => Function::new_typed_with_env(&mut store, env, proc_getscheduler::<Memory32>),
        "sched_getscheduler" => Function::new_typed_with_env(&mut store, env, sched_getscheduler::<Memory32>),
        "sched_getparam" => Function::new_typed_with_env(&mut store, env, sched_getparam::<Memory32>),
        "proc_setscheduler" => Function::new_typed_with_env(&mut store, env, proc_setscheduler),
        "sched_setscheduler" => Function::new_typed_with_env(&mut store, env, sched_setscheduler),
        "sched_setparam" => Function::new_typed_with_env(&mut store, env, sched_setparam::<Memory32>),
        "sched_get_priority_max" => Function::new_typed_with_env(&mut store, env, sched_get_priority_max),
        "sched_get_priority_min" => Function::new_typed_with_env(&mut store, env, sched_get_priority_min),
        "sched_rr_get_interval" => Function::new_typed_with_env(&mut store, env, sched_rr_get_interval::<Memory32>),
        "proc_getaffinity" => Function::new_typed_with_env(&mut store, env, proc_getaffinity::<Memory32>),
        "sched_getaffinity" => Function::new_typed_with_env(&mut store, env, sched_getaffinity::<Memory32>),
        "proc_setaffinity" => Function::new_typed_with_env(&mut store, env, proc_setaffinity),
        "sched_setaffinity" => Function::new_typed_with_env(&mut store, env, sched_setaffinity),
        "proc_getcpu" => Function::new_typed_with_env(&mut store, env, proc_getcpu::<Memory32>),
        "sched_getcpu" => Function::new_typed_with_env(&mut store, env, sched_getcpu),
        "proc_child_continued_status" => Function::new_typed_with_env(&mut store, env, proc_child_continued_status::<Memory32>),
        "proc_child_continued_status_wait" => Function::new_typed_with_env(&mut store, env, proc_child_continued_status_wait::<Memory32>),
        "proc_tcgetpgrp" => Function::new_typed_with_env(&mut store, env, proc_tcgetpgrp::<Memory32>),
        "tcgetpgrp" => Function::new_typed_with_env(&mut store, env, tcgetpgrp),
        "proc_tcgetsid" => Function::new_typed_with_env(&mut store, env, proc_tcgetsid::<Memory32>),
        "tcgetsid" => Function::new_typed_with_env(&mut store, env, tcgetsid),
        "proc_tcsetpgrp" => Function::new_typed_with_env(&mut store, env, proc_tcsetpgrp),
        "tcsetpgrp" => Function::new_typed_with_env(&mut store, env, tcsetpgrp),
        "proc_tty_get_tostop" => Function::new_typed_with_env(&mut store, env, proc_tty_get_tostop::<Memory32>),
        "proc_tty_set_tostop" => Function::new_typed_with_env(&mut store, env, proc_tty_set_tostop),
        "random_get" => Function::new_typed_with_env(&mut store, env, random_get::<Memory32>),
        "tty_get" => Function::new_typed_with_env(&mut store, env, tty_get::<Memory32>),
        "tty_set" => Function::new_typed_with_env(&mut store, env, tty_set::<Memory32>),
        "getcwd" => Function::new_typed_with_env(&mut store, env, getcwd::<Memory32>),
        "proc_chdir" => Function::new_typed_with_env(&mut store, env, proc_chdir::<Memory32>),
        "proc_fchdir" => Function::new_typed_with_env(&mut store, env, proc_fchdir),
        "chdir" => Function::new_typed_with_env(&mut store, env, proc_chdir::<Memory32>),
        "fchdir" => Function::new_typed_with_env(&mut store, env, fchdir),
        "path_chmod" => Function::new_typed_with_env(&mut store, env, path_chmod::<Memory32>),
        "path_chmod_at" => Function::new_typed_with_env(&mut store, env, path_chmod_at::<Memory32>),
        "path_mode" => Function::new_typed_with_env(&mut store, env, path_mode::<Memory32>),
        "path_mode_at" => Function::new_typed_with_env(&mut store, env, path_mode_at::<Memory32>),
        "fd_chmod" => Function::new_typed_with_env(&mut store, env, fd_chmod),
        "fd_mode" => Function::new_typed_with_env(&mut store, env, fd_mode::<Memory32>),
        "path_chown" => Function::new_typed_with_env(&mut store, env, path_chown::<Memory32>),
        "path_chown_at" => Function::new_typed_with_env(&mut store, env, path_chown_at::<Memory32>),
        "path_owner_at" => Function::new_typed_with_env(&mut store, env, path_owner_at::<Memory32>),
        "path_touch_at" => Function::new_typed_with_env(&mut store, env, path_touch_at::<Memory32>),
        "path_times_at" => Function::new_typed_with_env(&mut store, env, path_times_at::<Memory32>),
        "path_symlink_at" => Function::new_typed_with_env(&mut store, env, path_symlink_at::<Memory32>),
        "path_read_symlink_at" => Function::new_typed_with_env(&mut store, env, path_read_symlink_at::<Memory32>),
        "path_is_symlink_at" => Function::new_typed_with_env(&mut store, env, path_is_symlink_at::<Memory32>),
        "path_unlink_symlink_at" => Function::new_typed_with_env(&mut store, env, path_unlink_symlink_at::<Memory32>),
        "fd_chown" => Function::new_typed_with_env(&mut store, env, fd_chown),
        "fd_owner" => Function::new_typed_with_env(&mut store, env, fd_owner::<Memory32>),
        "fd_touch" => Function::new_typed_with_env(&mut store, env, fd_touch),
        "dl_invalid_handle" => Function::new_typed_with_env(&mut store, env, dl_invalid_handle),
        "dlopen" => Function::new_typed_with_env(&mut store, env, dlopen::<Memory32>),
        "dlsym" => Function::new_typed_with_env(&mut store, env, dlsym::<Memory32>),
        "callback_signal" => Function::new_typed_with_env(&mut store, env, callback_signal::<Memory32>),
        "thread_spawn" => Function::new_typed_with_env(&mut store, env, thread_spawn_v2::<Memory32>),
        "thread_spawn_v2" => Function::new_typed_with_env(&mut store, env, thread_spawn_v2::<Memory32>),
        "thread_sleep" => Function::new_typed_with_env(&mut store, env, thread_sleep::<Memory32>),
        "sleep" => Function::new_typed_with_env(&mut store, env, sleep::<Memory32>),
        "usleep" => Function::new_typed_with_env(&mut store, env, usleep::<Memory32>),
        "nanosleep" => Function::new_typed_with_env(&mut store, env, nanosleep::<Memory32>),
        "clock_nanosleep" => Function::new_typed_with_env(&mut store, env, clock_nanosleep::<Memory32>),
        "thread_id" => Function::new_typed_with_env(&mut store, env, thread_id::<Memory32>),
        "gettid" => Function::new_typed_with_env(&mut store, env, gettid),
        "thread_local_create" => Function::new_typed_with_env(&mut store, env, thread_local_create::<Memory32>),
        "thread_local_destroy" => Function::new_typed_with_env(&mut store, env, thread_local_destroy),
        "thread_local_set" => Function::new_typed_with_env(&mut store, env, thread_local_set),
        "thread_local_get" => Function::new_typed_with_env(&mut store, env, thread_local_get::<Memory32>),
        "thread_signal" => Function::new_typed_with_env(&mut store, env, thread_signal),
        "tkill" => Function::new_typed_with_env(&mut store, env, tkill),
        "tgkill" => Function::new_typed_with_env(&mut store, env, tgkill),
        "pidfd_open" => Function::new_typed_with_env(&mut store, env, pidfd_open),
        "pidfd_send_signal" => Function::new_typed_with_env(&mut store, env, pidfd_send_signal),
        "pidfd_getfd" => Function::new_typed_with_env(&mut store, env, pidfd_getfd),
        "pidfd_status" => Function::new_typed_with_env(&mut store, env, pidfd_status),
        "pidfd_pid" => Function::new_typed_with_env(&mut store, env, pidfd_pid),
        "thread_join" => Function::new_typed_with_env(&mut store, env, thread_join::<Memory32>),
        "thread_parallelism" => Function::new_typed_with_env(&mut store, env, thread_parallelism::<Memory32>),
        "thread_exit" => Function::new_typed_with_env(&mut store, env, thread_exit),
        "sched_yield" => Function::new_typed_with_env(&mut store, env, sched_yield::<Memory32>),
        "stack_checkpoint" => Function::new_typed_with_env(&mut store, env, stack_checkpoint::<Memory32>),
        "stack_restore" => Function::new_typed_with_env(&mut store, env, stack_restore::<Memory32>),
        "context_create" => Function::new_typed_with_env(&mut store, env, context_create::<Memory32>),
        "context_switch" => if engine_supports_async { Function::new_typed_with_env_async(&mut store, env, context_switch) } else { Function::new_typed_with_env(&mut store, env, context_switch_not_supported) },
        "context_destroy" => Function::new_typed_with_env(&mut store, env, context_destroy),
        "futex_wait" => Function::new_typed_with_env(&mut store, env, futex_wait::<Memory32>),
        "futex_wait_mask" => Function::new_typed_with_env(&mut store, env, futex_wait_mask::<Memory32>),
        "futex_wait_any" => Function::new_typed_with_env(&mut store, env, futex_wait_any::<Memory32>),
        "futex_wake" => Function::new_typed_with_env(&mut store, env, futex_wake::<Memory32>),
        "futex_wake_mask" => Function::new_typed_with_env(&mut store, env, futex_wake_mask::<Memory32>),
        "futex_requeue" => Function::new_typed_with_env(&mut store, env, futex_requeue::<Memory32>),
        "futex_wake_all" => Function::new_typed_with_env(&mut store, env, futex_wake_all::<Memory32>),
        "port_bridge" => Function::new_typed_with_env(&mut store, env, port_bridge::<Memory32>),
        "port_unbridge" => Function::new_typed_with_env(&mut store, env, port_unbridge),
        "port_dhcp_acquire" => Function::new_typed_with_env(&mut store, env, port_dhcp_acquire),
        "port_addr_add" => Function::new_typed_with_env(&mut store, env, port_addr_add::<Memory32>),
        "port_addr_remove" => Function::new_typed_with_env(&mut store, env, port_addr_remove::<Memory32>),
        "port_addr_clear" => Function::new_typed_with_env(&mut store, env, port_addr_clear),
        "port_addr_list" => Function::new_typed_with_env(&mut store, env, port_addr_list::<Memory32>),
        "port_mac" => Function::new_typed_with_env(&mut store, env, port_mac::<Memory32>),
        "port_gateway_set" => Function::new_typed_with_env(&mut store, env, port_gateway_set::<Memory32>),
        "port_route_add" => Function::new_typed_with_env(&mut store, env, port_route_add::<Memory32>),
        "port_route_remove" => Function::new_typed_with_env(&mut store, env, port_route_remove::<Memory32>),
        "port_route_clear" => Function::new_typed_with_env(&mut store, env, port_route_clear),
        "port_route_list" => Function::new_typed_with_env(&mut store, env, port_route_list::<Memory32>),
        "sock_status" => Function::new_typed_with_env(&mut store, env, sock_status::<Memory32>),
        "sock_addr_local" => Function::new_typed_with_env(&mut store, env, sock_addr_local::<Memory32>),
        "sock_addr_peer" => Function::new_typed_with_env(&mut store, env, sock_addr_peer::<Memory32>),
        "sock_open" => Function::new_typed_with_env(&mut store, env, sock_open::<Memory32>),
        "sock_pair" => Function::new_typed_with_env(&mut store, env, sock_pair::<Memory32>),
        "sock_set_opt_flag" => Function::new_typed_with_env(&mut store, env, sock_set_opt_flag),
        "sock_get_opt_flag" => Function::new_typed_with_env(&mut store, env, sock_get_opt_flag::<Memory32>),
        "sock_set_opt_time" => Function::new_typed_with_env(&mut store, env, sock_set_opt_time::<Memory32>),
        "sock_get_opt_time" => Function::new_typed_with_env(&mut store, env, sock_get_opt_time::<Memory32>),
        "sock_set_opt_size" => Function::new_typed_with_env(&mut store, env, sock_set_opt_size),
        "sock_get_opt_size" => Function::new_typed_with_env(&mut store, env, sock_get_opt_size::<Memory32>),
        "sock_join_multicast_v4" => Function::new_typed_with_env(&mut store, env, sock_join_multicast_v4::<Memory32>),
        "sock_leave_multicast_v4" => Function::new_typed_with_env(&mut store, env, sock_leave_multicast_v4::<Memory32>),
        "sock_join_multicast_v6" => Function::new_typed_with_env(&mut store, env, sock_join_multicast_v6::<Memory32>),
        "sock_leave_multicast_v6" => Function::new_typed_with_env(&mut store, env, sock_leave_multicast_v6::<Memory32>),
        "sock_bind" => Function::new_typed_with_env(&mut store, env, sock_bind::<Memory32>),
        "sock_listen" => Function::new_typed_with_env(&mut store, env, sock_listen::<Memory32>),
        "sock_accept" => Function::new_typed_with_env(&mut store, env, sock_accept_v2::<Memory32>),
        "sock_accept_v2" => Function::new_typed_with_env(&mut store, env, sock_accept_v2::<Memory32>),
        "sock_connect" => Function::new_typed_with_env(&mut store, env, sock_connect::<Memory32>),
        "sock_recv" => Function::new_typed_with_env(&mut store, env, sock_recv::<Memory32>),
        "sock_recv_from" => Function::new_typed_with_env(&mut store, env, sock_recv_from::<Memory32>),
        "sock_send" => Function::new_typed_with_env(&mut store, env, sock_send::<Memory32>),
        "sock_send_to" => Function::new_typed_with_env(&mut store, env, sock_send_to::<Memory32>),
        "sock_send_file" => Function::new_typed_with_env(&mut store, env, sock_send_file::<Memory32>),
        "sock_shutdown" => Function::new_typed_with_env(&mut store, env, sock_shutdown),
        "resolve" => Function::new_typed_with_env(&mut store, env, resolve::<Memory32>),
    };
    namespace
}

fn wasix_exports_64(mut store: &mut impl AsStoreMut, env: &FunctionEnv<WasiEnv>) -> Exports {
    let engine_supports_async = store.as_store_ref().engine().supports_async();

    use syscalls::*;
    let namespace = namespace! {
        "args_get" => Function::new_typed_with_env(&mut store, env, args_get::<Memory64>),
        "args_sizes_get" => Function::new_typed_with_env(&mut store, env, args_sizes_get::<Memory64>),
        "call_dynamic" => Function::new_typed_with_env(&mut store, env, call_dynamic::<Memory64>),
        "reflect_signature" => Function::new_typed_with_env(&mut store, env, reflect_signature::<Memory64>),
        "clock_res_get" => Function::new_typed_with_env(&mut store, env, clock_res_get::<Memory64>),
        "clock_time_get" => Function::new_typed_with_env(&mut store, env, clock_time_get::<Memory64>),
        "clock_getres" => Function::new_typed_with_env(&mut store, env, clock_getres::<Memory64>),
        "clock_gettime" => Function::new_typed_with_env(&mut store, env, clock_gettime::<Memory64>),
        "clock_time_set" => Function::new_typed_with_env(&mut store, env, clock_time_set),
        "closure_prepare" => Function::new_typed_with_env(&mut store, env, closure_prepare::<Memory64>),
        "closure_allocate" => Function::new_typed_with_env(&mut store, env, closure_allocate::<Memory64>),
        "closure_free" => Function::new_typed_with_env(&mut store, env, closure_free),
        "environ_get" => Function::new_typed_with_env(&mut store, env, environ_get::<Memory64>),
        "environ_sizes_get" => Function::new_typed_with_env(&mut store, env, environ_sizes_get::<Memory64>),
        "epoll_create" => Function::new_typed_with_env(&mut store, env, epoll_create::<Memory64>),
        "epoll_ctl" => Function::new_typed_with_env(&mut store, env, epoll_ctl::<Memory64>),
        "epoll_wait" => Function::new_typed_with_env(&mut store, env, epoll_wait::<Memory64>),
        "fd_advise" => Function::new_typed_with_env(&mut store, env, fd_advise),
        "fd_allocate" => Function::new_typed_with_env(&mut store, env, fd_allocate),
        "fd_close" => Function::new_typed_with_env(&mut store, env, fd_close),
        "fd_datasync" => Function::new_typed_with_env(&mut store, env, fd_datasync),
        "fd_fdstat_get" => Function::new_typed_with_env(&mut store, env, fd_fdstat_get::<Memory64>),
        "fd_fdstat_set_flags" => Function::new_typed_with_env(&mut store, env, fd_fdstat_set_flags),
        "fd_fdstat_set_rights" => Function::new_typed_with_env(&mut store, env, fd_fdstat_set_rights),
        "fd_filestat_get" => Function::new_typed_with_env(&mut store, env, fd_filestat_get::<Memory64>),
        "fd_filestat_set_size" => Function::new_typed_with_env(&mut store, env, fd_filestat_set_size),
        "fd_filestat_set_times" => Function::new_typed_with_env(&mut store, env, fd_filestat_set_times),
        "fd_pread" => Function::new_typed_with_env(&mut store, env, fd_pread::<Memory64>),
        "fd_prestat_get" => Function::new_typed_with_env(&mut store, env, fd_prestat_get::<Memory64>),
        "fd_prestat_dir_name" => Function::new_typed_with_env(&mut store, env, fd_prestat_dir_name::<Memory64>),
        "fd_pwrite" => Function::new_typed_with_env(&mut store, env, fd_pwrite::<Memory64>),
        "fd_read" => Function::new_typed_with_env(&mut store, env, fd_read::<Memory64>),
        "fd_readdir" => Function::new_typed_with_env(&mut store, env, fd_readdir::<Memory64>),
        "fd_renumber" => Function::new_typed_with_env(&mut store, env, fd_renumber),
        "fd_dup" => Function::new_typed_with_env(&mut store, env, fd_dup::<Memory64>),
        "dup" => Function::new_typed_with_env(&mut store, env, dup),
        "dup2" => Function::new_typed_with_env(&mut store, env, dup2),
        "dup3" => Function::new_typed_with_env(&mut store, env, dup3),
        "fd_dup2" => Function::new_typed_with_env(&mut store, env, fd_dup2::<Memory64>),
        "fd_fdflags_get" => Function::new_typed_with_env(&mut store, env, fd_fdflags_get::<Memory64>),
        "fd_fdflags_set" => Function::new_typed_with_env(&mut store, env, fd_fdflags_set),
        "fd_event" => Function::new_typed_with_env(&mut store, env, fd_event::<Memory64>),
        "eventfd" => Function::new_typed_with_env(&mut store, env, eventfd),
        "timerfd_create" => Function::new_typed_with_env(&mut store, env, timerfd_create),
        "timerfd_settime" => Function::new_typed_with_env(&mut store, env, timerfd_settime::<Memory64>),
        "timerfd_gettime" => Function::new_typed_with_env(&mut store, env, timerfd_gettime::<Memory64>),
        "timerfd_read" => Function::new_typed_with_env(&mut store, env, timerfd_read),
        "timerfd_ready" => Function::new_typed_with_env(&mut store, env, timerfd_ready),
        "timerfd_remaining_ms" => Function::new_typed_with_env(&mut store, env, timerfd_remaining_ms),
        "timerfd_known" => Function::new_typed_with_env(&mut store, env, timerfd_known),
        "posix_timer_create" => Function::new_typed_with_env(&mut store, env, posix_timer_create),
        "posix_timer_settime" => Function::new_typed_with_env(&mut store, env, posix_timer_settime::<Memory64>),
        "posix_timer_gettime" => Function::new_typed_with_env(&mut store, env, posix_timer_gettime::<Memory64>),
        "posix_timer_getoverrun" => Function::new_typed_with_env(&mut store, env, posix_timer_getoverrun),
        "posix_timer_delete" => Function::new_typed_with_env(&mut store, env, posix_timer_delete),
        "itimer_settime" => Function::new_typed_with_env(&mut store, env, itimer_settime::<Memory64>),
        "itimer_gettime" => Function::new_typed_with_env(&mut store, env, itimer_gettime::<Memory64>),
        "signalfd" => Function::new_typed_with_env(&mut store, env, signalfd),
        "signalfd_known" => Function::new_typed_with_env(&mut store, env, signalfd_known),
        "signalfd_mask_low" => Function::new_typed_with_env(&mut store, env, signalfd_mask_low),
        "signalfd_mask_high" => Function::new_typed_with_env(&mut store, env, signalfd_mask_high),
        "fd_seek" => Function::new_typed_with_env(&mut store, env, fd_seek::<Memory64>),
        "fd_sync" => Function::new_typed_with_env(&mut store, env, fd_sync),
        "fd_tell" => Function::new_typed_with_env(&mut store, env, fd_tell::<Memory64>),
        "fd_write" => Function::new_typed_with_env(&mut store, env, fd_write::<Memory64>),
        "fd_pipe" => Function::new_typed_with_env(&mut store, env, fd_pipe::<Memory64>),
        "pipe" => Function::new_typed_with_env(&mut store, env, pipe::<Memory64>),
        "pipe2" => Function::new_typed_with_env(&mut store, env, pipe2::<Memory64>),
        "path_create_directory" => Function::new_typed_with_env(&mut store, env, path_create_directory::<Memory64>),
        "path_filestat_get" => Function::new_typed_with_env(&mut store, env, path_filestat_get::<Memory64>),
        "path_filestat_set_times" => Function::new_typed_with_env(&mut store, env, path_filestat_set_times::<Memory64>),
        "path_link" => Function::new_typed_with_env(&mut store, env, path_link::<Memory64>),
        "path_open" => Function::new_typed_with_env(&mut store, env, path_open::<Memory64>),
        "path_open2" => Function::new_typed_with_env(&mut store, env, path_open2::<Memory64>),
        "path_readlink" => Function::new_typed_with_env(&mut store, env, path_readlink::<Memory64>),
        "path_remove_directory" => Function::new_typed_with_env(&mut store, env, path_remove_directory::<Memory64>),
        "path_rename" => Function::new_typed_with_env(&mut store, env, path_rename::<Memory64>),
        "path_symlink" => Function::new_typed_with_env(&mut store, env, path_symlink::<Memory64>),
        "path_unlink_file" => Function::new_typed_with_env(&mut store, env, path_unlink_file::<Memory64>),
        "poll_oneoff" => Function::new_typed_with_env(&mut store, env, poll_oneoff::<Memory64>),
        "proc_exit" => Function::new_typed_with_env(&mut store, env, proc_exit::<Memory64>),
        "proc_fork" => Function::new_typed_with_env(&mut store, env, proc_fork::<Memory64>),
        "fork" => Function::new_typed_with_env(&mut store, env, fork::<Memory64>),
        "vfork" => Function::new_typed_with_env(&mut store, env, vfork::<Memory64>),
        "clone_thread" => Function::new_typed_with_env(&mut store, env, clone_thread::<Memory64>),
        "proc_fork_env" => Function::new_typed_with_env(&mut store, env, proc_fork_env::<Memory64>),
        "proc_join" => Function::new_typed_with_env(&mut store, env, proc_join::<Memory64>),
        "waitpid" => Function::new_typed_with_env(&mut store, env, waitpid::<Memory64>),
        "waitid" => Function::new_typed_with_env(&mut store, env, waitid::<Memory64>),
        "proc_wait_status" => Function::new_typed_with_env(&mut store, env, proc_wait_status::<Memory64>),
        "proc_wait_status_select" => Function::new_typed_with_env(&mut store, env, proc_wait_status_select::<Memory64>),
        "proc_signal" => Function::new_typed_with_env(&mut store, env, proc_signal),
        "kill" => Function::new_typed_with_env(&mut store, env, kill),
        "proc_signals_get" => Function::new_typed_with_env(&mut store, env, proc_signals_get::<Memory64>),
        "proc_signals_sizes_get" => Function::new_typed_with_env(&mut store, env, proc_signals_sizes_get::<Memory64>),
        "proc_exec" => Function::new_typed_with_env(&mut store, env, proc_exec::<Memory64>),
        "proc_exec2" => Function::new_typed_with_env(&mut store, env, proc_exec2::<Memory64>),
        "proc_exec3" => Function::new_typed_with_env(&mut store, env, proc_exec3::<Memory64>),
        "execv" => Function::new_typed_with_env(&mut store, env, execv::<Memory64>),
        "execve" => Function::new_typed_with_env(&mut store, env, execve::<Memory64>),
        "execvp" => Function::new_typed_with_env(&mut store, env, execvp::<Memory64>),
        "execvpe" => Function::new_typed_with_env(&mut store, env, execvpe::<Memory64>),
        "proc_exit2" => Function::new_typed_with_env(&mut store, env, proc_exit2::<Memory64>),
        "_exit" => Function::new_typed_with_env(&mut store, env, proc_exit2::<Memory64>),
        "proc_raise" => Function::new_typed_with_env(&mut store, env, proc_raise),
        "raise" => Function::new_typed_with_env(&mut store, env, raise),
        "proc_raise_interval" => Function::new_typed_with_env(&mut store, env, proc_raise_interval),
        "sigaltstack_set" => Function::new_typed_with_env(&mut store, env, sigaltstack_set::<Memory64>),
        "sigaltstack_get" => Function::new_typed_with_env(&mut store, env, sigaltstack_get::<Memory64>),
        "signal_mask_set" => Function::new_typed_with_env(&mut store, env, signal_mask_set),
        "signal_mask_get" => Function::new_typed_with_env(&mut store, env, signal_mask_get::<Memory64>),
        "unshare" => Function::new_typed_with_env(&mut store, env, unshare),
        "setns" => Function::new_typed_with_env(&mut store, env, setns),
        "proc_snapshot" => Function::new_typed_with_env(&mut store, env, proc_snapshot::<Memory64>),
        "proc_spawn" => Function::new_typed_with_env(&mut store, env, proc_spawn::<Memory64>),
        "process_spawn" => Function::new_typed_with_env(&mut store, env, proc_spawn::<Memory64>),
        "proc_spawn2" => Function::new_typed_with_env(&mut store, env, proc_spawn2::<Memory64>),
        "proc_umask" => Function::new_typed_with_env(&mut store, env, proc_umask::<Memory64>),
        "proc_getuid" => Function::new_typed_with_env(&mut store, env, proc_getuid::<Memory64>),
        "getuid" => Function::new_typed_with_env(&mut store, env, getuid),
        "geteuid" => Function::new_typed_with_env(&mut store, env, geteuid),
        "proc_getresuid" => Function::new_typed_with_env(&mut store, env, proc_getresuid::<Memory64>),
        "getresuid" => Function::new_typed_with_env(&mut store, env, proc_getresuid::<Memory64>),
        "proc_getgid" => Function::new_typed_with_env(&mut store, env, proc_getgid::<Memory64>),
        "getgid" => Function::new_typed_with_env(&mut store, env, getgid),
        "getegid" => Function::new_typed_with_env(&mut store, env, getegid),
        "proc_getresgid" => Function::new_typed_with_env(&mut store, env, proc_getresgid::<Memory64>),
        "getresgid" => Function::new_typed_with_env(&mut store, env, proc_getresgid::<Memory64>),
        "proc_getgroups" => Function::new_typed_with_env(&mut store, env, proc_getgroups::<Memory64>),
        "getgroups" => Function::new_typed_with_env(&mut store, env, proc_getgroups::<Memory64>),
        "proc_setresuid" => Function::new_typed_with_env(&mut store, env, proc_setresuid),
        "setuid" => Function::new_typed_with_env(&mut store, env, setuid),
        "seteuid" => Function::new_typed_with_env(&mut store, env, seteuid),
        "setresuid" => Function::new_typed_with_env(&mut store, env, proc_setresuid),
        "proc_setresgid" => Function::new_typed_with_env(&mut store, env, proc_setresgid),
        "setgid" => Function::new_typed_with_env(&mut store, env, setgid),
        "setegid" => Function::new_typed_with_env(&mut store, env, setegid),
        "setresgid" => Function::new_typed_with_env(&mut store, env, proc_setresgid),
        "proc_setgroups" => Function::new_typed_with_env(&mut store, env, proc_setgroups::<Memory64>),
        "setgroups" => Function::new_typed_with_env(&mut store, env, proc_setgroups::<Memory64>),
        "proc_id" => Function::new_typed_with_env(&mut store, env, proc_id::<Memory64>),
        "getpid" => Function::new_typed_with_env(&mut store, env, getpid),
        "proc_parent" => Function::new_typed_with_env(&mut store, env, proc_parent::<Memory64>),
        "getppid" => Function::new_typed_with_env(&mut store, env, getppid),
        "proc_inherit_parent" => Function::new_typed_with_env(&mut store, env, proc_inherit_parent),
        "proc_set_parent_death_signal" => Function::new_typed_with_env(&mut store, env, proc_set_parent_death_signal),
        "proc_get_parent_death_signal" => Function::new_typed_with_env(&mut store, env, proc_get_parent_death_signal::<Memory64>),
        "proc_set_child_subreaper" => Function::new_typed_with_env(&mut store, env, proc_set_child_subreaper),
        "proc_get_child_subreaper" => Function::new_typed_with_env(&mut store, env, proc_get_child_subreaper::<Memory64>),
        "proc_set_name" => Function::new_typed_with_env(&mut store, env, proc_set_name::<Memory64>),
        "proc_get_name" => Function::new_typed_with_env(&mut store, env, proc_get_name::<Memory64>),
        "proc_set_uts_name" => Function::new_typed_with_env(&mut store, env, proc_set_uts_name::<Memory64>),
        "proc_get_uts_name" => Function::new_typed_with_env(&mut store, env, proc_get_uts_name::<Memory64>),
        "proc_set_dumpable" => Function::new_typed_with_env(&mut store, env, proc_set_dumpable),
        "proc_get_dumpable" => Function::new_typed_with_env(&mut store, env, proc_get_dumpable::<Memory64>),
        "proc_set_no_new_privs" => Function::new_typed_with_env(&mut store, env, proc_set_no_new_privs),
        "proc_get_no_new_privs" => Function::new_typed_with_env(&mut store, env, proc_get_no_new_privs::<Memory64>),
        "proc_set_seccomp_mode" => Function::new_typed_with_env(&mut store, env, proc_set_seccomp_mode),
        "proc_get_seccomp_mode" => Function::new_typed_with_env(&mut store, env, proc_get_seccomp_mode::<Memory64>),
        "proc_set_timer_slack" => Function::new_typed_with_env(&mut store, env, proc_set_timer_slack),
        "proc_get_timer_slack" => Function::new_typed_with_env(&mut store, env, proc_get_timer_slack::<Memory64>),
        "proc_personality" => Function::new_typed_with_env(&mut store, env, proc_personality::<Memory64>),
        "proc_set_clear_child_tid" => Function::new_typed_with_env(&mut store, env, proc_set_clear_child_tid),
        "proc_get_clear_child_tid" => Function::new_typed_with_env(&mut store, env, proc_get_clear_child_tid::<Memory64>),
        "set_tid_address" => Function::new_typed_with_env(&mut store, env, set_tid_address),
        "get_clear_child_tid" => Function::new_typed_with_env(&mut store, env, get_clear_child_tid::<Memory64>),
        "proc_set_robust_list" => Function::new_typed_with_env(&mut store, env, proc_set_robust_list),
        "proc_get_robust_list" => Function::new_typed_with_env(&mut store, env, proc_get_robust_list::<Memory64>),
        "set_robust_list" => Function::new_typed_with_env(&mut store, env, set_robust_list),
        "get_robust_list" => Function::new_typed_with_env(&mut store, env, get_robust_list::<Memory64>),
        "proc_get_capabilities" => Function::new_typed_with_env(&mut store, env, proc_get_capabilities::<Memory64>),
        "proc_set_capabilities" => Function::new_typed_with_env(&mut store, env, proc_set_capabilities),
        "proc_get_capability_bounding" => Function::new_typed_with_env(&mut store, env, proc_get_capability_bounding::<Memory64>),
        "proc_drop_capability_bounding" => Function::new_typed_with_env(&mut store, env, proc_drop_capability_bounding),
        "proc_set_process_control" => Function::new_typed_with_env(&mut store, env, proc_set_process_control),
        "proc_get_process_control" => Function::new_typed_with_env(&mut store, env, proc_get_process_control::<Memory64>),
        "proc_unshare_namespaces" => Function::new_typed_with_env(&mut store, env, proc_unshare_namespaces),
        "proc_set_arch_base" => Function::new_typed_with_env(&mut store, env, proc_set_arch_base),
        "proc_get_arch_base" => Function::new_typed_with_env(&mut store, env, proc_get_arch_base::<Memory64>),
        "arch_prctl" => Function::new_typed_with_env(&mut store, env, arch_prctl::<Memory64>),
        "proc_set_rseq" => Function::new_typed_with_env(&mut store, env, proc_set_rseq),
        "proc_clear_rseq" => Function::new_typed_with_env(&mut store, env, proc_clear_rseq),
        "rseq" => Function::new_typed_with_env(&mut store, env, rseq),
        "membarrier" => Function::new_typed_with_env(&mut store, env, membarrier),
        "proc_set_mempolicy" => Function::new_typed_with_env(&mut store, env, proc_set_mempolicy),
        "proc_get_mempolicy" => Function::new_typed_with_env(&mut store, env, proc_get_mempolicy::<Memory64>),
        "proc_set_mempolicy_home_node" => Function::new_typed_with_env(&mut store, env, proc_set_mempolicy_home_node),
        "proc_set_ioprio" => Function::new_typed_with_env(&mut store, env, proc_set_ioprio),
        "proc_get_ioprio" => Function::new_typed_with_env(&mut store, env, proc_get_ioprio::<Memory64>),
        "proc_getpgid" => Function::new_typed_with_env(&mut store, env, proc_getpgid::<Memory64>),
        "getpgid" => Function::new_typed_with_env(&mut store, env, getpgid),
        "proc_getpgrp" => Function::new_typed_with_env(&mut store, env, proc_getpgrp::<Memory64>),
        "getpgrp" => Function::new_typed_with_env(&mut store, env, getpgrp),
        "proc_getsid" => Function::new_typed_with_env(&mut store, env, proc_getsid::<Memory64>),
        "getsid" => Function::new_typed_with_env(&mut store, env, getsid),
        "proc_setpgid" => Function::new_typed_with_env(&mut store, env, proc_setpgid),
        "setpgid" => Function::new_typed_with_env(&mut store, env, setpgid),
        "proc_setsid" => Function::new_typed_with_env(&mut store, env, proc_setsid::<Memory64>),
        "setsid" => Function::new_typed_with_env(&mut store, env, setsid),
        "proc_getpriority" => Function::new_typed_with_env(&mut store, env, proc_getpriority::<Memory64>),
        "getpriority" => Function::new_typed_with_env(&mut store, env, getpriority),
        "proc_setpriority" => Function::new_typed_with_env(&mut store, env, proc_setpriority),
        "setpriority" => Function::new_typed_with_env(&mut store, env, setpriority),
        "nice" => Function::new_typed_with_env(&mut store, env, nice),
        "proc_getrusage" => Function::new_typed_with_env(&mut store, env, proc_getrusage::<Memory64>),
        "getrusage" => Function::new_typed_with_env(&mut store, env, getrusage::<Memory64>),
        "proc_times" => Function::new_typed_with_env(&mut store, env, proc_times::<Memory64>),
        "proc_getrlimit" => Function::new_typed_with_env(&mut store, env, proc_getrlimit::<Memory64>),
        "getrlimit" => Function::new_typed_with_env(&mut store, env, getrlimit::<Memory64>),
        "proc_setrlimit" => Function::new_typed_with_env(&mut store, env, proc_setrlimit),
        "setrlimit" => Function::new_typed_with_env(&mut store, env, setrlimit),
        "prlimit" => Function::new_typed_with_env(&mut store, env, prlimit::<Memory64>),
        "proc_getscheduler" => Function::new_typed_with_env(&mut store, env, proc_getscheduler::<Memory64>),
        "sched_getscheduler" => Function::new_typed_with_env(&mut store, env, sched_getscheduler::<Memory64>),
        "sched_getparam" => Function::new_typed_with_env(&mut store, env, sched_getparam::<Memory64>),
        "proc_setscheduler" => Function::new_typed_with_env(&mut store, env, proc_setscheduler),
        "sched_setscheduler" => Function::new_typed_with_env(&mut store, env, sched_setscheduler),
        "sched_setparam" => Function::new_typed_with_env(&mut store, env, sched_setparam::<Memory64>),
        "sched_get_priority_max" => Function::new_typed_with_env(&mut store, env, sched_get_priority_max),
        "sched_get_priority_min" => Function::new_typed_with_env(&mut store, env, sched_get_priority_min),
        "sched_rr_get_interval" => Function::new_typed_with_env(&mut store, env, sched_rr_get_interval::<Memory64>),
        "proc_getaffinity" => Function::new_typed_with_env(&mut store, env, proc_getaffinity::<Memory64>),
        "sched_getaffinity" => Function::new_typed_with_env(&mut store, env, sched_getaffinity::<Memory64>),
        "proc_setaffinity" => Function::new_typed_with_env(&mut store, env, proc_setaffinity),
        "sched_setaffinity" => Function::new_typed_with_env(&mut store, env, sched_setaffinity),
        "proc_getcpu" => Function::new_typed_with_env(&mut store, env, proc_getcpu::<Memory64>),
        "sched_getcpu" => Function::new_typed_with_env(&mut store, env, sched_getcpu),
        "proc_child_continued_status" => Function::new_typed_with_env(&mut store, env, proc_child_continued_status::<Memory64>),
        "proc_child_continued_status_wait" => Function::new_typed_with_env(&mut store, env, proc_child_continued_status_wait::<Memory64>),
        "proc_tcgetpgrp" => Function::new_typed_with_env(&mut store, env, proc_tcgetpgrp::<Memory64>),
        "tcgetpgrp" => Function::new_typed_with_env(&mut store, env, tcgetpgrp),
        "proc_tcgetsid" => Function::new_typed_with_env(&mut store, env, proc_tcgetsid::<Memory64>),
        "tcgetsid" => Function::new_typed_with_env(&mut store, env, tcgetsid),
        "proc_tcsetpgrp" => Function::new_typed_with_env(&mut store, env, proc_tcsetpgrp),
        "tcsetpgrp" => Function::new_typed_with_env(&mut store, env, tcsetpgrp),
        "proc_tty_get_tostop" => Function::new_typed_with_env(&mut store, env, proc_tty_get_tostop::<Memory64>),
        "proc_tty_set_tostop" => Function::new_typed_with_env(&mut store, env, proc_tty_set_tostop),
        "random_get" => Function::new_typed_with_env(&mut store, env, random_get::<Memory64>),
        "tty_get" => Function::new_typed_with_env(&mut store, env, tty_get::<Memory64>),
        "tty_set" => Function::new_typed_with_env(&mut store, env, tty_set::<Memory64>),
        "getcwd" => Function::new_typed_with_env(&mut store, env, getcwd::<Memory64>),
        "proc_chdir" => Function::new_typed_with_env(&mut store, env, proc_chdir::<Memory64>),
        "proc_fchdir" => Function::new_typed_with_env(&mut store, env, proc_fchdir),
        "chdir" => Function::new_typed_with_env(&mut store, env, proc_chdir::<Memory64>),
        "fchdir" => Function::new_typed_with_env(&mut store, env, fchdir),
        "path_chmod" => Function::new_typed_with_env(&mut store, env, path_chmod::<Memory64>),
        "path_chmod_at" => Function::new_typed_with_env(&mut store, env, path_chmod_at::<Memory64>),
        "path_mode" => Function::new_typed_with_env(&mut store, env, path_mode::<Memory64>),
        "path_mode_at" => Function::new_typed_with_env(&mut store, env, path_mode_at::<Memory64>),
        "fd_chmod" => Function::new_typed_with_env(&mut store, env, fd_chmod),
        "fd_mode" => Function::new_typed_with_env(&mut store, env, fd_mode::<Memory64>),
        "path_chown" => Function::new_typed_with_env(&mut store, env, path_chown::<Memory64>),
        "path_chown_at" => Function::new_typed_with_env(&mut store, env, path_chown_at::<Memory64>),
        "path_owner_at" => Function::new_typed_with_env(&mut store, env, path_owner_at::<Memory64>),
        "path_touch_at" => Function::new_typed_with_env(&mut store, env, path_touch_at::<Memory64>),
        "path_times_at" => Function::new_typed_with_env(&mut store, env, path_times_at::<Memory64>),
        "path_symlink_at" => Function::new_typed_with_env(&mut store, env, path_symlink_at::<Memory64>),
        "path_read_symlink_at" => Function::new_typed_with_env(&mut store, env, path_read_symlink_at::<Memory64>),
        "path_is_symlink_at" => Function::new_typed_with_env(&mut store, env, path_is_symlink_at::<Memory64>),
        "path_unlink_symlink_at" => Function::new_typed_with_env(&mut store, env, path_unlink_symlink_at::<Memory64>),
        "fd_chown" => Function::new_typed_with_env(&mut store, env, fd_chown),
        "fd_owner" => Function::new_typed_with_env(&mut store, env, fd_owner::<Memory64>),
        "fd_touch" => Function::new_typed_with_env(&mut store, env, fd_touch),
        "dl_invalid_handle" => Function::new_typed_with_env(&mut store, env, dl_invalid_handle),
        "dlopen" => Function::new_typed_with_env(&mut store, env, dlopen::<Memory64>),
        "dlsym" => Function::new_typed_with_env(&mut store, env, dlsym::<Memory64>),
        "callback_signal" => Function::new_typed_with_env(&mut store, env, callback_signal::<Memory64>),
        "thread_spawn" => Function::new_typed_with_env(&mut store, env, thread_spawn_v2::<Memory64>),
        "thread_spawn_v2" => Function::new_typed_with_env(&mut store, env, thread_spawn_v2::<Memory64>),
        "thread_sleep" => Function::new_typed_with_env(&mut store, env, thread_sleep::<Memory64>),
        "sleep" => Function::new_typed_with_env(&mut store, env, sleep::<Memory64>),
        "usleep" => Function::new_typed_with_env(&mut store, env, usleep::<Memory64>),
        "nanosleep" => Function::new_typed_with_env(&mut store, env, nanosleep::<Memory64>),
        "clock_nanosleep" => Function::new_typed_with_env(&mut store, env, clock_nanosleep::<Memory64>),
        "thread_id" => Function::new_typed_with_env(&mut store, env, thread_id::<Memory64>),
        "gettid" => Function::new_typed_with_env(&mut store, env, gettid),
        "thread_local_create" => Function::new_typed_with_env(&mut store, env, thread_local_create::<Memory64>),
        "thread_local_destroy" => Function::new_typed_with_env(&mut store, env, thread_local_destroy),
        "thread_local_set" => Function::new_typed_with_env(&mut store, env, thread_local_set),
        "thread_local_get" => Function::new_typed_with_env(&mut store, env, thread_local_get::<Memory64>),
        "thread_signal" => Function::new_typed_with_env(&mut store, env, thread_signal),
        "tkill" => Function::new_typed_with_env(&mut store, env, tkill),
        "tgkill" => Function::new_typed_with_env(&mut store, env, tgkill),
        "pidfd_open" => Function::new_typed_with_env(&mut store, env, pidfd_open),
        "pidfd_send_signal" => Function::new_typed_with_env(&mut store, env, pidfd_send_signal),
        "pidfd_getfd" => Function::new_typed_with_env(&mut store, env, pidfd_getfd),
        "pidfd_status" => Function::new_typed_with_env(&mut store, env, pidfd_status),
        "pidfd_pid" => Function::new_typed_with_env(&mut store, env, pidfd_pid),
        "thread_join" => Function::new_typed_with_env(&mut store, env, thread_join::<Memory64>),
        "thread_parallelism" => Function::new_typed_with_env(&mut store, env, thread_parallelism::<Memory64>),
        "thread_exit" => Function::new_typed_with_env(&mut store, env, thread_exit),
        "sched_yield" => Function::new_typed_with_env(&mut store, env, sched_yield::<Memory64>),
        "stack_checkpoint" => Function::new_typed_with_env(&mut store, env, stack_checkpoint::<Memory64>),
        "stack_restore" => Function::new_typed_with_env(&mut store, env, stack_restore::<Memory64>),
        "context_create" => Function::new_typed_with_env(&mut store, env, context_create::<Memory64>),
        "context_switch" => if engine_supports_async { Function::new_typed_with_env_async(&mut store, env, context_switch) } else { Function::new_typed_with_env(&mut store, env, context_switch_not_supported) },
        "context_destroy" => Function::new_typed_with_env(&mut store, env, context_destroy),
        "futex_wait" => Function::new_typed_with_env(&mut store, env, futex_wait::<Memory64>),
        "futex_wait_mask" => Function::new_typed_with_env(&mut store, env, futex_wait_mask::<Memory64>),
        "futex_wait_any" => Function::new_typed_with_env(&mut store, env, futex_wait_any::<Memory64>),
        "futex_wake" => Function::new_typed_with_env(&mut store, env, futex_wake::<Memory64>),
        "futex_wake_mask" => Function::new_typed_with_env(&mut store, env, futex_wake_mask::<Memory64>),
        "futex_requeue" => Function::new_typed_with_env(&mut store, env, futex_requeue::<Memory64>),
        "futex_wake_all" => Function::new_typed_with_env(&mut store, env, futex_wake_all::<Memory64>),
        "port_bridge" => Function::new_typed_with_env(&mut store, env, port_bridge::<Memory64>),
        "port_unbridge" => Function::new_typed_with_env(&mut store, env, port_unbridge),
        "port_dhcp_acquire" => Function::new_typed_with_env(&mut store, env, port_dhcp_acquire),
        "port_addr_add" => Function::new_typed_with_env(&mut store, env, port_addr_add::<Memory64>),
        "port_addr_remove" => Function::new_typed_with_env(&mut store, env, port_addr_remove::<Memory64>),
        "port_addr_clear" => Function::new_typed_with_env(&mut store, env, port_addr_clear),
        "port_addr_list" => Function::new_typed_with_env(&mut store, env, port_addr_list::<Memory64>),
        "port_mac" => Function::new_typed_with_env(&mut store, env, port_mac::<Memory64>),
        "port_gateway_set" => Function::new_typed_with_env(&mut store, env, port_gateway_set::<Memory64>),
        "port_route_add" => Function::new_typed_with_env(&mut store, env, port_route_add::<Memory64>),
        "port_route_remove" => Function::new_typed_with_env(&mut store, env, port_route_remove::<Memory64>),
        "port_route_clear" => Function::new_typed_with_env(&mut store, env, port_route_clear),
        "port_route_list" => Function::new_typed_with_env(&mut store, env, port_route_list::<Memory64>),
        "sock_status" => Function::new_typed_with_env(&mut store, env, sock_status::<Memory64>),
        "sock_addr_local" => Function::new_typed_with_env(&mut store, env, sock_addr_local::<Memory64>),
        "sock_addr_peer" => Function::new_typed_with_env(&mut store, env, sock_addr_peer::<Memory64>),
        "sock_open" => Function::new_typed_with_env(&mut store, env, sock_open::<Memory64>),
        "sock_pair" => Function::new_typed_with_env(&mut store, env, sock_pair::<Memory64>),
        "sock_set_opt_flag" => Function::new_typed_with_env(&mut store, env, sock_set_opt_flag),
        "sock_get_opt_flag" => Function::new_typed_with_env(&mut store, env, sock_get_opt_flag::<Memory64>),
        "sock_set_opt_time" => Function::new_typed_with_env(&mut store, env, sock_set_opt_time::<Memory64>),
        "sock_get_opt_time" => Function::new_typed_with_env(&mut store, env, sock_get_opt_time::<Memory64>),
        "sock_set_opt_size" => Function::new_typed_with_env(&mut store, env, sock_set_opt_size),
        "sock_get_opt_size" => Function::new_typed_with_env(&mut store, env, sock_get_opt_size::<Memory64>),
        "sock_join_multicast_v4" => Function::new_typed_with_env(&mut store, env, sock_join_multicast_v4::<Memory64>),
        "sock_leave_multicast_v4" => Function::new_typed_with_env(&mut store, env, sock_leave_multicast_v4::<Memory64>),
        "sock_join_multicast_v6" => Function::new_typed_with_env(&mut store, env, sock_join_multicast_v6::<Memory64>),
        "sock_leave_multicast_v6" => Function::new_typed_with_env(&mut store, env, sock_leave_multicast_v6::<Memory64>),
        "sock_bind" => Function::new_typed_with_env(&mut store, env, sock_bind::<Memory64>),
        "sock_listen" => Function::new_typed_with_env(&mut store, env, sock_listen::<Memory64>),
        "sock_accept" => Function::new_typed_with_env(&mut store, env, sock_accept_v2::<Memory64>),
        "sock_accept_v2" => Function::new_typed_with_env(&mut store, env, sock_accept_v2::<Memory64>),
        "sock_connect" => Function::new_typed_with_env(&mut store, env, sock_connect::<Memory64>),
        "sock_recv" => Function::new_typed_with_env(&mut store, env, sock_recv::<Memory64>),
        "sock_recv_from" => Function::new_typed_with_env(&mut store, env, sock_recv_from::<Memory64>),
        "sock_send" => Function::new_typed_with_env(&mut store, env, sock_send::<Memory64>),
        "sock_send_to" => Function::new_typed_with_env(&mut store, env, sock_send_to::<Memory64>),
        "sock_send_file" => Function::new_typed_with_env(&mut store, env, sock_send_file::<Memory64>),
        "sock_shutdown" => Function::new_typed_with_env(&mut store, env, sock_shutdown),
        "resolve" => Function::new_typed_with_env(&mut store, env, resolve::<Memory64>),
    };
    namespace
}

// TODO: split function into two variants, one for JS and one for sys.
// (this will make code less messy)
fn import_object_for_all_wasi_versions(
    _module: &wasmer::Module,
    store: &mut impl AsStoreMut,
    env: &FunctionEnv<WasiEnv>,
) -> Imports {
    let exports_wasi_generic = wasi_exports_generic(store, env);
    let exports_wasi_unstable = wasi_unstable_exports(store, env);
    let exports_wasi_snapshot_preview1 = wasi_snapshot_preview1_exports(store, env);
    let exports_wasix_32v1 = wasix_exports_32(store, env);
    let exports_wasix_64v1 = wasix_exports_64(store, env);

    // Allowed due to JS feature flag complications.
    #[allow(unused_mut)]
    let mut imports = imports! {
        "wasi" => exports_wasi_generic,
        "wasi_unstable" => exports_wasi_unstable,
        "wasi_snapshot_preview1" => exports_wasi_snapshot_preview1,
        "wasix_32v1" => exports_wasix_32v1,
        "wasix_64v1" => exports_wasix_64v1,
    };
    imports
}

/// Combines a state generating function with the import list for legacy WASI
fn generate_import_object_snapshot0(
    store: &mut impl AsStoreMut,
    env: &FunctionEnv<WasiEnv>,
) -> Imports {
    let exports_unstable = wasi_unstable_exports(store, env);
    imports! {
        "wasi_unstable" => exports_unstable
    }
}

fn generate_import_object_snapshot1(
    store: &mut impl AsStoreMut,
    env: &FunctionEnv<WasiEnv>,
) -> Imports {
    let exports_wasi_snapshot_preview1 = wasi_snapshot_preview1_exports(store, env);
    imports! {
        "wasi_snapshot_preview1" => exports_wasi_snapshot_preview1
    }
}

/// Combines a state generating function with the import list for snapshot 1
fn generate_import_object_wasix32_v1(
    store: &mut impl AsStoreMut,
    env: &FunctionEnv<WasiEnv>,
) -> Imports {
    let exports_wasix_32v1 = wasix_exports_32(store, env);
    imports! {
        "wasix_32v1" => exports_wasix_32v1
    }
}

fn generate_import_object_wasix64_v1(
    store: &mut impl AsStoreMut,
    env: &FunctionEnv<WasiEnv>,
) -> Imports {
    let exports_wasix_64v1 = wasix_exports_64(store, env);
    imports! {
        "wasix_64v1" => exports_wasix_64v1
    }
}

fn mem_error_to_wasi(err: MemoryAccessError) -> Errno {
    match err {
        MemoryAccessError::HeapOutOfBounds => Errno::Memviolation,
        MemoryAccessError::Overflow => Errno::Overflow,
        MemoryAccessError::NonUtf8String => Errno::Inval,
        _ => Errno::Unknown,
    }
}

/// Run a synchronous function that would normally be blocking.
///
/// When the `sys-thread` feature is enabled, this will call
/// [`tokio::task::block_in_place()`]. Otherwise, it calls the function
/// immediately.
pub(crate) fn block_in_place<Ret>(thunk: impl FnOnce() -> Ret) -> Ret {
    cfg_if::cfg_if! {
        if #[cfg(feature = "sys-thread")] {
            tokio::task::block_in_place(thunk)
        } else {
            thunk()
        }
    }
}

/// Spawns a new blocking task that runs the provided closure.
///
/// The closure is executed on a separate thread, allowing it to perform blocking operations
/// without blocking the main thread. The closure is wrapped in a `Future` that resolves to the
/// result of the closure's execution.
pub(crate) async fn spawn_blocking<F, R>(f: F) -> Result<R, tokio::task::JoinError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    cfg_if::cfg_if! {
        if #[cfg(target_arch = "wasm32")] {
            Ok(block_in_place(f))
        } else {
            tokio::task::spawn_blocking(f).await
        }
    }
}

pub(crate) fn flatten_runtime_error(err: RuntimeError) -> RuntimeError {
    let e_ref = err.downcast_ref::<WasiRuntimeError>();
    match e_ref {
        Some(WasiRuntimeError::Wasi(_)) => {
            let Ok(WasiRuntimeError::Wasi(err)) = err.downcast::<WasiRuntimeError>() else {
                unreachable!()
            };
            RuntimeError::user(Box::new(err))
        }
        Some(WasiRuntimeError::Runtime(_)) => {
            let Ok(WasiRuntimeError::Runtime(err)) = err.downcast::<WasiRuntimeError>() else {
                unreachable!()
            };
            flatten_runtime_error(err)
        }
        _ => err,
    }
}
