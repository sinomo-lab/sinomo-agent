#[cfg(feature = "journal")]
use crate::journal::{DynJournal, JournalEffector, SnapshotTrigger};
use crate::{
    Runtime, VirtualTaskManager, WasiControlPlane, WasiEnvBuilder, WasiError, WasiFunctionEnv,
    WasiResult, WasiRuntimeError, WasiStateCreationError, WasiThreadError, WasiVFork,
    bin_factory::{BinFactory, BinaryPackage, BinaryPackageCommand},
    capabilities::Capabilities,
    fs::{WasiFsRoot, WasiInodes},
    import_object_for_all_wasi_versions,
    os::task::{
        TaskStatus,
        control_plane::ControlPlaneError,
        process::{WasiProcess, WasiProcessId, signal_default_stops, signal_default_terminates},
        thread::{WasiMemoryLayout, WasiThread, WasiThreadHandle, WasiThreadId},
    },
    syscalls::{platform_clock_time_get, sync_process_metadata},
};
use futures::future::BoxFuture;
use rand::Rng;
use std::{
    collections::HashMap,
    ops::Deref,
    path::{Path, PathBuf},
    str,
    sync::Arc,
    time::Duration,
};
use virtual_fs::{FileSystem, FsError, VirtualFile};
use virtual_mio::block_on;
use virtual_net::DynVirtualNetworking;
use wasmer::{
    AsStoreMut, AsStoreRef, ExportError, FunctionEnvMut, Instance, Memory, MemoryType, MemoryView,
    Module,
};
use wasmer_config::package::PackageSource;
use wasmer_types::ModuleHash;
use wasmer_wasix_types::{
    types::Signal,
    wasi::{Disposition, Errno, ExitCode, Snapshot0Clockid},
    wasix::ThreadStartType,
};
use webc::metadata::annotations::Wasi;

pub use super::handles::*;
use super::{Linker, WasiState, context_switching::ContextSwitchingEnvironment, conv_env_vars};

/// Data required to construct a [`WasiEnv`].
#[derive(Debug)]
pub struct WasiEnvInit {
    pub(crate) state: WasiState,
    pub runtime: Arc<dyn Runtime + Send + Sync>,
    pub webc_dependencies: Vec<BinaryPackage>,
    pub mapped_commands: HashMap<String, PathBuf>,
    pub bin_factory: BinFactory,
    pub capabilities: Capabilities,

    pub control_plane: WasiControlPlane,
    pub memory_ty: Option<MemoryType>,
    pub process: Option<WasiProcess>,
    pub thread: Option<WasiThreadHandle>,

    /// Whether to call the `_initialize` function in the WASI module.
    /// Will be true for regular new instances, but false for threads.
    pub call_initialize: bool,

    /// Indicates if the calling environment is capable of deep sleeping
    pub can_deep_sleep: bool,

    /// Indicates if extra tracing should be output
    pub extra_tracing: bool,

    /// Indicates triggers that will cause a snapshot to be taken
    #[cfg(feature = "journal")]
    pub snapshot_on: Vec<SnapshotTrigger>,

    /// Stop running after the first snapshot is taken
    #[cfg(feature = "journal")]
    pub stop_running_after_snapshot: bool,

    /// Skip writes to stdout and stderr when bootstrapping from a journal
    pub skip_stdio_during_bootstrap: bool,
    pub sinomo_metadata_root: Option<PathBuf>,
}

impl WasiEnvInit {
    pub fn duplicate(&self) -> Self {
        let inodes = WasiInodes::new();

        // TODO: preserve preopens?
        let fs =
            crate::fs::WasiFs::new_with_preopen(&inodes, &[], &[], self.state.fs.root_fs.clone())
                .unwrap();

        Self {
            state: WasiState {
                secret: rand::thread_rng().r#gen::<[u8; 32]>(),
                inodes,
                fs,
                futexs: Default::default(),
                shared_futex_ranges: Default::default(),
                shared_mutex_owners: Default::default(),
                shared_mutex_owner_pids: Default::default(),
                shared_mutex_depths: Default::default(),
                shared_mutex_waits: Default::default(),
                shared_cond_sequences: Default::default(),
                shared_cond_waits: Default::default(),
                shared_rwlocks: Default::default(),
                shared_semaphores: Default::default(),
                shared_memory_cross_process: Default::default(),
                pidfds: Default::default(),
                timerfds: Default::default(),
                signalfds: Default::default(),
                posix_timers: Default::default(),
                interval_timers: Default::default(),
                message_queues: Default::default(),
                shared_memory_mappings: Default::default(),
                memory_protections: Default::default(),
                signal_stack: Default::default(),
                clock_offset: std::sync::Mutex::new(
                    self.state.clock_offset.lock().unwrap().clone(),
                ),
                args: std::sync::Mutex::new(self.state.args.lock().unwrap().clone()),
                envs: std::sync::Mutex::new(self.state.envs.lock().unwrap().deref().clone()),
                signals: std::sync::Mutex::new(self.state.signals.lock().unwrap().deref().clone()),
                preopen: self.state.preopen.clone(),
            },
            runtime: self.runtime.clone(),
            webc_dependencies: self.webc_dependencies.clone(),
            mapped_commands: self.mapped_commands.clone(),
            bin_factory: self.bin_factory.clone(),
            capabilities: self.capabilities.clone(),
            control_plane: self.control_plane.clone(),
            memory_ty: None,
            process: None,
            thread: None,
            call_initialize: self.call_initialize,
            can_deep_sleep: self.can_deep_sleep,
            extra_tracing: false,
            #[cfg(feature = "journal")]
            snapshot_on: self.snapshot_on.clone(),
            #[cfg(feature = "journal")]
            stop_running_after_snapshot: self.stop_running_after_snapshot,
            skip_stdio_during_bootstrap: self.skip_stdio_during_bootstrap,
            sinomo_metadata_root: self.sinomo_metadata_root.clone(),
        }
    }
}

/// The environment provided to the WASI imports.
pub struct WasiEnv {
    pub control_plane: WasiControlPlane,
    /// Represents the process this environment is attached to
    pub process: WasiProcess,
    /// Represents the thread this environment is attached to
    pub thread: WasiThread,
    /// Represents the layout of the memory
    pub layout: WasiMemoryLayout,
    /// Represents a fork of the process that is currently in play
    pub vfork: Option<WasiVFork>,
    /// Seed used to rotate around the events returned by `poll_oneoff`
    pub poll_seed: u64,
    /// Shared state of the WASI system. Manages all the data that the
    /// executing WASI program can see.
    pub(crate) state: Arc<WasiState>,
    /// Binary factory attached to this environment
    pub bin_factory: BinFactory,
    /// List of the handles that are owned by this context
    /// (this can be used to ensure that threads own themselves or others)
    pub owned_handles: Vec<WasiThreadHandle>,
    /// Implementation of the WASI runtime.
    pub runtime: Arc<dyn Runtime + Send + Sync + 'static>,

    pub capabilities: Capabilities,

    /// Is this environment capable and setup for deep sleeping
    pub enable_deep_sleep: bool,

    /// Enables the snap shotting functionality
    pub enable_journal: bool,

    /// Enables an exponential backoff of the process CPU usage when there
    /// are no active run tokens (when set holds the maximum amount of
    /// time that it will pause the CPU)
    pub enable_exponential_cpu_backoff: Option<Duration>,

    /// Flag that indicates if the environment is currently replaying the journal
    /// (and hence it should not record new events)
    pub replaying_journal: bool,

    /// Should stdio be skipped when bootstrapping this module from an existing journal?
    pub skip_stdio_during_bootstrap: bool,
    pub sinomo_metadata_root: Option<PathBuf>,

    /// Flag that indicates the cleanup of the environment is to be disabled
    /// (this is normally used so that the instance can be reused later on)
    pub(crate) disable_fs_cleanup: bool,

    /// Inner functions and references that are loaded before the environment starts
    /// (inner is not safe to send between threads and so it is private and will
    ///  not be cloned when `WasiEnv` is cloned)
    /// TODO: We should move this outside of `WasiEnv` with some refactoring
    inner: WasiInstanceHandlesPointer,

    /// Tracks the active contexts of the WASIX context switching API
    ///
    /// This is `None` when the main function was not launched with context switching
    ///
    /// Should probably only be set by [`ContextSwitchingContext::run_main_context`]
    pub(crate) context_switching_environment: Option<ContextSwitchingEnvironment>,
}

impl std::fmt::Debug for WasiEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "env(pid={}, tid={})", self.pid().raw(), self.tid().raw())
    }
}

impl Clone for WasiEnv {
    fn clone(&self) -> Self {
        Self {
            control_plane: self.control_plane.clone(),
            process: self.process.clone(),
            poll_seed: self.poll_seed,
            thread: self.thread.clone(),
            layout: self.layout.clone(),
            vfork: self.vfork.clone(),
            state: self.state.clone(),
            bin_factory: self.bin_factory.clone(),
            inner: Default::default(),
            owned_handles: self.owned_handles.clone(),
            runtime: self.runtime.clone(),
            capabilities: self.capabilities.clone(),
            enable_deep_sleep: self.enable_deep_sleep,
            enable_journal: self.enable_journal,
            enable_exponential_cpu_backoff: self.enable_exponential_cpu_backoff,
            replaying_journal: self.replaying_journal,
            skip_stdio_during_bootstrap: self.skip_stdio_during_bootstrap,
            sinomo_metadata_root: self.sinomo_metadata_root.clone(),
            disable_fs_cleanup: self.disable_fs_cleanup,
            context_switching_environment: None,
        }
    }
}

impl WasiEnv {
    /// Construct a new [`WasiEnvBuilder`] that allows customizing an environment.
    pub fn builder(program_name: impl Into<String>) -> WasiEnvBuilder {
        WasiEnvBuilder::new(program_name)
    }

    pub fn sync_process_resource_limits(&self) {
        self.state
            .fs
            .set_fd_limit(self.process.open_file_descriptor_limit());
    }

    /// Forking the WasiState is used when either fork or vfork is called
    pub fn fork(&self) -> Result<(Self, WasiThreadHandle), ControlPlaneError> {
        let mut process = self.control_plane.new_process(self.process.module_hash)?;
        process.set_parent_pid(self.process.pid());
        process.inherit_job_control_from(&self.process);
        let handle = process.new_thread(self.layout.clone(), self.thread.thread_start_type())?;

        let thread = handle.as_thread();
        thread.copy_stack_from(&self.thread);
        let (signal_mask_low, signal_mask_high) = self.thread.signal_mask();
        thread.set_signal_mask(signal_mask_low, signal_mask_high);

        let state = Arc::new(self.state.fork());

        let bin_factory = self.bin_factory.clone();

        let new_env = Self {
            control_plane: self.control_plane.clone(),
            process,
            thread,
            layout: self.layout.clone(),
            vfork: None,
            poll_seed: 0,
            bin_factory,
            state,
            inner: Default::default(),
            owned_handles: Vec::new(),
            runtime: self.runtime.clone(),
            capabilities: self.capabilities.clone(),
            enable_deep_sleep: self.enable_deep_sleep,
            enable_journal: self.enable_journal,
            enable_exponential_cpu_backoff: self.enable_exponential_cpu_backoff,
            replaying_journal: false,
            skip_stdio_during_bootstrap: self.skip_stdio_during_bootstrap,
            sinomo_metadata_root: self.sinomo_metadata_root.clone(),
            disable_fs_cleanup: self.disable_fs_cleanup,
            context_switching_environment: None,
        };
        sync_process_metadata(&new_env);
        Ok((new_env, handle))
    }

    pub fn pid(&self) -> WasiProcessId {
        self.process.pid()
    }

    pub fn tid(&self) -> WasiThreadId {
        self.thread.tid()
    }

    /// Returns true if this WASM process will need and try to use
    /// asyncify while its running which normally means.
    pub fn will_use_asyncify(&self) -> bool {
        self.inner()
            .static_module_instance_handles()
            .map(|handles| self.enable_deep_sleep || handles.has_stack_checkpoint)
            .unwrap_or(false)
    }

    /// Re-initializes this environment so that it can be executed again
    pub fn reinit(&mut self) -> Result<(), WasiStateCreationError> {
        // If the cleanup logic is enabled then we need to rebuild the
        // file descriptors which would have been destroyed when the
        // main thread exited
        if !self.disable_fs_cleanup {
            // First we clear any open files as the descriptors would
            // otherwise clash
            if let Ok(mut map) = self.state.fs.fd_map.write() {
                map.clear();
            }
            self.state.fs.preopen_fds.write().unwrap().clear();
            *self.state.fs.current_dir.lock().unwrap() = "/".to_string();

            // We need to rebuild the basic file descriptors
            self.state.fs.create_stdin(&self.state.inodes);
            self.state.fs.create_stdout(&self.state.inodes);
            self.state.fs.create_stderr(&self.state.inodes);
            self.state
                .fs
                .create_rootfd()
                .map_err(WasiStateCreationError::WasiFsSetupError)?;
            self.state
                .fs
                .create_preopens(&self.state.inodes, true)
                .map_err(WasiStateCreationError::WasiFsSetupError)?;
        }

        // The process and thread state need to be reset
        self.process = WasiProcess::new(
            self.process.pid,
            self.process.module_hash,
            self.process.compute.clone(),
        );
        self.thread = WasiThread::new(
            self.thread.pid(),
            self.thread.tid(),
            self.thread.is_main(),
            self.process.finished.clone(),
            self.process.compute.must_upgrade().register_task()?,
            self.thread.memory_layout().clone(),
            self.thread.thread_start_type(),
        );
        sync_process_metadata(self);

        Ok(())
    }

    /// Returns true if this module is capable of deep sleep
    /// (needs asyncify to unwind and rewind)
    ///
    /// # Safety
    ///
    /// This function should only be called from within a syscall
    /// as it accessed objects that are a thread local (functions)
    pub unsafe fn capable_of_deep_sleep(&self) -> bool {
        if !self.control_plane.config().enable_asynchronous_threading {
            return false;
        }
        self.inner()
            .static_module_instance_handles()
            .map(|handles| {
                handles.asyncify_get_state.is_some()
                    && handles.asyncify_start_rewind.is_some()
                    && handles.asyncify_start_unwind.is_some()
            })
            .unwrap_or(false)
    }

    /// Returns true if this thread can go into a deep sleep
    pub fn layout(&self) -> &WasiMemoryLayout {
        &self.layout
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn from_init(
        init: WasiEnvInit,
        module_hash: ModuleHash,
    ) -> Result<Self, WasiRuntimeError> {
        let process = if let Some(p) = init.process {
            p
        } else {
            init.control_plane.new_process(module_hash)?
        };

        #[cfg(feature = "journal")]
        {
            let mut guard = process.inner.0.lock().unwrap();
            guard.snapshot_on = init.snapshot_on.into_iter().collect();
            guard.stop_running_after_checkpoint = init.stop_running_after_snapshot;
        }

        let layout = WasiMemoryLayout::default();
        let thread = if let Some(t) = init.thread {
            t
        } else {
            process.new_thread(layout.clone(), ThreadStartType::MainThread)?
        };

        let mut env = Self {
            control_plane: init.control_plane,
            process,
            thread: thread.as_thread(),
            layout,
            vfork: None,
            poll_seed: 0,
            state: Arc::new(init.state),
            inner: Default::default(),
            owned_handles: Vec::new(),
            #[cfg(feature = "journal")]
            enable_journal: init.runtime.active_journal().is_some(),
            #[cfg(not(feature = "journal"))]
            enable_journal: false,
            replaying_journal: false,
            skip_stdio_during_bootstrap: init.skip_stdio_during_bootstrap,
            sinomo_metadata_root: init.sinomo_metadata_root,
            enable_deep_sleep: init.capabilities.threading.enable_asynchronous_threading,
            enable_exponential_cpu_backoff: init
                .capabilities
                .threading
                .enable_exponential_cpu_backoff,
            runtime: init.runtime,
            bin_factory: init.bin_factory,
            capabilities: init.capabilities,
            disable_fs_cleanup: false,
            context_switching_environment: None,
        };
        env.owned_handles.push(thread);
        if let Some(bridge) = env.runtime.tty()
            && bridge.foreground_process_group().is_none()
        {
            bridge.set_foreground_process_group(env.process.pgid().raw());
        }
        sync_process_metadata(&env);

        // TODO: should not be here - should be callers responsibility!
        for pkg in &init.webc_dependencies {
            env.use_package(pkg)?;
        }

        #[cfg(feature = "sys")]
        env.map_commands(init.mapped_commands.clone())?;

        Ok(env)
    }

    // FIXME: use custom error type
    #[allow(clippy::result_large_err)]
    pub(crate) fn instantiate(
        self,
        module: Module,
        store: &mut impl AsStoreMut,
        memory: Option<Memory>,
        update_layout: bool,
        call_initialize: bool,
        parent_linker_and_ctx: Option<(Linker, &mut FunctionEnvMut<WasiEnv>, bool)>,
    ) -> Result<(Instance, WasiFunctionEnv), WasiThreadError> {
        let pid = self.process.pid();

        let mut store = store.as_store_mut();
        let engine = self.runtime().engine();
        let dynamic_linker_stack_size = self.runtime().dynamic_linker_stack_size();
        let mut func_env = WasiFunctionEnv::new(&mut store, self);

        let is_dl = super::linker::is_dynamically_linked(&module);
        if is_dl {
            let linker = match parent_linker_and_ctx {
                Some((linker, ctx, copy_memory)) => {
                    linker.create_instance_group(ctx, &mut store, &mut func_env, copy_memory)
                }
                None => {
                    // FIXME: should we be storing envs as raw byte arrays?
                    let ld_library_path_owned;
                    let ld_library_path = {
                        let envs = func_env.data(&store).state.envs.lock().unwrap();
                        ld_library_path_owned = match envs
                            .iter()
                            .find_map(|env| env.strip_prefix(b"LD_LIBRARY_PATH="))
                        {
                            Some(path) => path
                                .split(|b| *b == b':')
                                .filter_map(|p| str::from_utf8(p).ok())
                                .map(PathBuf::from)
                                .collect::<Vec<_>>(),
                            None => vec![],
                        };
                        ld_library_path_owned
                            .iter()
                            .map(AsRef::as_ref)
                            .collect::<Vec<_>>()
                    };

                    Linker::new(
                        engine,
                        &module,
                        &mut store,
                        memory,
                        &mut func_env,
                        dynamic_linker_stack_size,
                        &ld_library_path,
                    )
                }
            };

            match linker {
                Ok((_, linked_module)) => {
                    return Ok((linked_module.instance, func_env));
                }
                Err(e) => {
                    tracing::error!(
                        %pid,
                        error = &e as &dyn std::error::Error,
                        "Failed to link DL main module",
                    );
                    func_env
                        .data(&store)
                        .blocking_on_exit(Some(Errno::Noexec.into()));
                    return Err(WasiThreadError::LinkError(Arc::new(e)));
                }
            }
        }

        // Let's instantiate the module with the imports.
        let mut import_object =
            import_object_for_all_wasi_versions(&module, &mut store, &func_env.env);

        let imported_memory = if let Some(memory) = memory {
            import_object.define("env", "memory", memory.clone());
            Some(memory)
        } else {
            None
        };

        // Construct the instance.
        let instance = match Instance::new(&mut store, &module, &import_object) {
            Ok(a) => a,
            Err(err) => {
                tracing::error!(
                    %pid,
                    error = &err as &dyn std::error::Error,
                    "Instantiation failed",
                );
                func_env
                    .data(&store)
                    .blocking_on_exit(Some(Errno::Noexec.into()));
                return Err(WasiThreadError::InstanceCreateFailed(Box::new(err)));
            }
        };

        let handles = match imported_memory {
            Some(memory) => WasiModuleTreeHandles::Static(WasiModuleInstanceHandles::new(
                memory,
                &store,
                instance.clone(),
                None,
            )),
            None => {
                let exported_memory = instance
                    .exports
                    .iter()
                    .filter_map(|(_, export)| {
                        if let wasmer::Extern::Memory(memory) = export {
                            Some(memory.clone())
                        } else {
                            None
                        }
                    })
                    .next()
                    .ok_or(WasiThreadError::ExportError(ExportError::Missing(
                        "No imported or exported memory found".to_owned(),
                    )))?;
                WasiModuleTreeHandles::Static(WasiModuleInstanceHandles::new(
                    exported_memory,
                    &store,
                    instance.clone(),
                    None,
                ))
            }
        };

        // Initialize the WASI environment
        if let Err(err) = func_env.initialize_handles_and_layout(
            &mut store,
            instance.clone(),
            handles,
            None,
            update_layout,
        ) {
            tracing::error!(
                %pid,
                error = &err as &dyn std::error::Error,
                "Initialization failed",
            );
            func_env
                .data(&store)
                .blocking_on_exit(Some(Errno::Noexec.into()));
            return Err(WasiThreadError::ExportError(err));
        }

        if call_initialize {
            for init_name in [
                "__wasm_apply_data_relocs",
                "__wasm_apply_tls_relocs",
                "__wasm_call_ctors",
            ] {
                if let Ok(init) = instance.exports.get_function(init_name) {
                    if let Err(err) = init.call(&mut store, &[]) {
                        func_env
                            .data(&store)
                            .blocking_on_exit(Some(Errno::Noexec.into()));
                        return Err(WasiThreadError::InitFailed(Arc::new(anyhow::Error::from(
                            err,
                        ))));
                    }
                }
            }
        }

        // If this module exports an _initialize function, run that first.
        if call_initialize && let Ok(initialize) = instance.exports.get_function("_initialize") {
            let initialize_result = initialize.call(&mut store, &[]);
            if let Err(err) = initialize_result {
                func_env
                    .data(&store)
                    .blocking_on_exit(Some(Errno::Noexec.into()));
                return Err(WasiThreadError::InitFailed(Arc::new(anyhow::Error::from(
                    err,
                ))));
            }
        }

        Ok((instance, func_env))
    }

    /// Returns a copy of the current runtime implementation for this environment
    pub fn runtime(&self) -> &(dyn Runtime + Send + Sync) {
        self.runtime.deref()
    }

    /// Returns a copy of the current tasks implementation for this environment
    pub fn tasks(&self) -> &Arc<dyn VirtualTaskManager> {
        self.runtime.task_manager()
    }

    pub fn fs_root(&self) -> &WasiFsRoot {
        &self.state.fs.root_fs
    }

    /// Overrides the runtime implementation for this environment
    pub fn set_runtime<R>(&mut self, runtime: R)
    where
        R: Runtime + Send + Sync + 'static,
    {
        self.runtime = Arc::new(runtime);
    }

    /// Returns the number of active threads
    pub fn active_threads(&self) -> u32 {
        self.process.active_threads()
    }

    /// Called by most (if not all) syscalls to process pending operations that are
    /// cross-cutting, such as signals, thread/process exit, DL operations, etc.
    pub fn do_pending_operations(ctx: &mut FunctionEnvMut<'_, Self>) -> Result<(), WasiError> {
        Self::do_pending_link_operations(ctx, true)?;
        _ = Self::process_signals_and_exit(ctx)?;
        Ok(())
    }

    pub fn do_pending_link_operations(
        ctx: &mut FunctionEnvMut<'_, Self>,
        fast: bool,
    ) -> Result<(), WasiError> {
        if let Some(linker) = ctx.data().inner().linker().cloned()
            && let Err(e) = linker.do_pending_link_operations(ctx, fast)
        {
            tracing::warn!(err = ?e, "Failed to process pending link operations");
            return Err(WasiError::Exit(Errno::Noexec.into()));
        }
        Ok(())
    }

    /// Porcesses any signals that are batched up or any forced exit codes
    pub fn process_signals_and_exit(ctx: &mut FunctionEnvMut<'_, Self>) -> WasiResult<bool> {
        // If a signal handler has never been set then we need to handle signals
        // differently
        let env = ctx.data();
        let env_inner = env
            .try_inner()
            .ok_or_else(|| WasiError::Exit(Errno::Fault.into()))?;
        let inner = env_inner.main_module_instance_handles();
        if !inner.signal_set && inner.signal.is_none() {
            env.process.signal_due_intervals();
            let signals = env.thread.pop_signals();
            if !signals.is_empty() {
                let processed = Self::process_default_signal_batch(ctx, signals)?;
                Self::suspend_if_stopped(ctx, processed)
            } else {
                Self::suspend_if_stopped(ctx, false)
            }
        } else {
            // Check for forced exit
            if let Some(forced_exit) = env.should_exit() {
                return Err(WasiError::Exit(forced_exit));
            }

            let processed = match Self::process_signals(ctx)? {
                Ok(processed) => processed,
                Err(err) => return Ok(Err(err)),
            };
            Self::suspend_if_stopped(ctx, processed)
        }
    }

    fn process_default_signal_batch(
        ctx: &mut FunctionEnvMut<'_, Self>,
        signals: Vec<Signal>,
    ) -> Result<bool, WasiError> {
        let env = ctx.data();
        if signals.is_empty() {
            return Ok(false);
        }
        for sig in signals {
            if sig == Signal::Sigkill {
                let exit_code = env.thread.set_or_get_exit_code_for_signal(sig);
                return Err(WasiError::Exit(exit_code));
            }
            if sig == Signal::Sigstop {
                env.thread.set_status_stopped(sig);
                continue;
            }
            if matches!(
                ctx.data().state.signals.lock().unwrap().get(&sig),
                Some(Disposition::Ignore)
            ) {
                tracing::trace!(pid=%env.pid(), ?sig, "Signal ignored by disposition");
                continue;
            }
            if signal_default_stops(sig) {
                env.thread.set_status_stopped(sig);
                env.process.notify_parent_child_status_changed();
            } else if signal_default_terminates(sig) {
                let exit_code = env.thread.set_or_get_exit_code_for_signal(sig);
                return Err(WasiError::Exit(exit_code));
            } else {
                tracing::trace!(pid=%env.pid(), ?sig, "Signal ignored");
            }
        }
        Ok(true)
    }

    fn suspend_if_stopped(
        ctx: &mut FunctionEnvMut<'_, Self>,
        mut processed: bool,
    ) -> WasiResult<bool> {
        loop {
            let env = ctx.data();
            if let Some(forced_exit) = env.should_exit() {
                return Err(WasiError::Exit(forced_exit));
            }

            let TaskStatus::Stopped(signal) = env.thread.status() else {
                return Ok(Ok(processed));
            };
            tracing::trace!(pid=%env.pid(), ?signal, "suspending stopped WASIX thread");

            let thread = env.thread.clone();
            block_on(thread.wait_for_signal());
            let env = ctx.data();
            let signals = env.thread.pop_signals();
            if signals.is_empty() {
                continue;
            }
            processed = true;

            let env_inner = env
                .try_inner()
                .ok_or_else(|| WasiError::Exit(Errno::Fault.into()))?;
            let inner = env_inner.main_module_instance_handles();
            if !inner.signal_set && inner.signal.is_none() {
                Self::process_default_signal_batch(ctx, signals)?;
            } else {
                Self::process_signals_internal(ctx, signals)?;
            }
        }
    }

    /// Porcesses any signals that are batched up
    pub(crate) fn process_signals(ctx: &mut FunctionEnvMut<'_, Self>) -> WasiResult<bool> {
        // If a signal handler has never been set then we need to handle signals
        // differently
        let env = ctx.data();
        let env_inner = env
            .try_inner()
            .ok_or_else(|| WasiError::Exit(Errno::Fault.into()))?;
        let inner = env_inner.main_module_instance_handles();
        if !inner.signal_set && inner.signal.is_none() {
            return Ok(Ok(false));
        }

        // Check for any signals that we need to trigger
        // (but only if a signal handler is registered)
        let ret = if inner.signal.as_ref().is_some() {
            env.process.signal_due_intervals();
            let signals = env.thread.pop_unblocked_signals();
            Self::process_signals_internal(ctx, signals)?
        } else {
            false
        };

        Ok(Ok(ret))
    }

    pub(crate) fn process_signals_internal(
        ctx: &mut FunctionEnvMut<'_, Self>,
        mut signals: Vec<Signal>,
    ) -> Result<bool, WasiError> {
        let env = ctx.data();
        let env_inner = env
            .try_inner()
            .ok_or_else(|| WasiError::Exit(Errno::Fault.into()))?;
        let inner = env_inner.main_module_instance_handles();
        if let Some(handler) = inner.signal.clone() {
            // We might also have signals that trigger on timers
            let mut now = 0;
            {
                let mut has_signal_interval = false;
                let inner = env.process.inner.0.lock().unwrap();
                if !inner.signal_intervals.is_empty() {
                    match platform_clock_time_get(Snapshot0Clockid::Monotonic, 1_000_000) {
                        Ok(current_time) => {
                            now = current_time as u128;
                            for signal in inner.signal_intervals.values() {
                                let elapsed = now.saturating_sub(signal.last_signal);
                                if elapsed >= signal.interval.as_nanos() {
                                    has_signal_interval = true;
                                    break;
                                }
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                %err,
                                "failed to read monotonic clock for signal processing"
                            );
                        }
                    }
                }
                if has_signal_interval {
                    let mut inner = env.process.inner.0.lock().unwrap();
                    for signal in inner.signal_intervals.values_mut() {
                        let elapsed = now.saturating_sub(signal.last_signal);
                        if elapsed >= signal.interval.as_nanos() {
                            signal.last_signal = now;
                            signals.push(signal.signal);
                        }
                    }
                }
            }

            let mut processed = false;
            for signal in signals {
                // Skip over Sigwakeup, which is host-side-only
                if matches!(signal, Signal::Sigwakeup) {
                    continue;
                }
                if signal == Signal::Sigkill {
                    let exit_code = ctx.data().thread.set_or_get_exit_code_for_signal(signal);
                    return Err(WasiError::Exit(exit_code));
                }
                if signal == Signal::Sigstop {
                    ctx.data().thread.set_status_stopped(signal);
                    ctx.data().process.notify_parent_child_status_changed();
                    continue;
                }
                if signal == Signal::Sigcont {
                    ctx.data().thread.set_status_continued();
                    continue;
                }
                if !matches!(signal, Signal::Sigkill | Signal::Sigstop)
                    && matches!(
                        ctx.data().state.signals.lock().unwrap().get(&signal),
                        Some(Disposition::Ignore)
                    )
                {
                    tracing::trace!(
                        pid=%ctx.data().pid(),
                        ?signal,
                        "Signal ignored by disposition before handler",
                    );
                    continue;
                }

                tracing::trace!(
                    pid=%ctx.data().pid(),
                    ?signal,
                    "processing signal via handler",
                );
                if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
                    eprintln!(
                        "sinomo-debug process_signal_handler pid={} tid={} signal={:?} has_handler=true",
                        ctx.data().pid().raw(),
                        ctx.data().tid().raw(),
                        signal
                    );
                }
                if let Err(err) = handler.call(ctx, signal as i32) {
                    match err.downcast::<WasiError>() {
                        Ok(wasi_err) => {
                            if matches!(wasi_err, WasiError::Exit(code) if signal == Signal::Sigsegv && code.raw() == 29)
                            {
                                return Err(WasiError::Exit(Errno::Success.into()));
                            }
                            tracing::warn!(
                                pid=%ctx.data().pid(),
                                wasi_err=&wasi_err as &dyn std::error::Error,
                                "signal handler wasi error",
                            );
                            return Err(wasi_err);
                        }
                        Err(runtime_err) => {
                            if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
                                eprintln!(
                                    "sinomo-debug signal_handler_runtime_error pid={} tid={} signal={:?} error={} trap={:?}",
                                    ctx.data().pid().raw(),
                                    ctx.data().tid().raw(),
                                    signal,
                                    runtime_err,
                                    runtime_err.clone().to_trap()
                                );
                            }
                            if let Some(code) = ctx.data().should_exit() {
                                return Err(WasiError::Exit(code));
                            }
                            if let Some(WasiRuntimeError::Wasi(WasiError::Exit(code))) =
                                runtime_err.downcast_ref::<WasiRuntimeError>()
                            {
                                return Err(WasiError::Exit(*code));
                            }
                            if let Some(WasiError::Exit(code)) =
                                runtime_err.downcast_ref::<WasiError>()
                            {
                                return Err(WasiError::Exit(*code));
                            }
                            // anything other than a kill command should report
                            // the error, killed things may not gracefully close properly
                            if signal != Signal::Sigkill {
                                tracing::warn!(
                                    pid=%ctx.data().pid(),
                                    runtime_err=&runtime_err as &dyn std::error::Error,
                                    "signal handler runtime error",
                                );
                            }
                            return Err(WasiError::Exit(Errno::Intr.into()));
                        }
                    }
                }
                processed = true;
                tracing::trace!(
                    pid=%ctx.data().pid(),
                    "signal processed",
                );
            }
            Ok(processed)
        } else {
            if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() && !signals.is_empty() {
                eprintln!(
                    "sinomo-debug process_signal_handler pid={} tid={} signals={:?} has_handler=false signal_set={}",
                    ctx.data().pid().raw(),
                    ctx.data().tid().raw(),
                    signals,
                    inner.signal_set
                );
            }
            tracing::trace!("no signal handler");
            Ok(false)
        }
    }

    /// Returns an exit code if the thread or process has been forced to exit
    pub fn should_exit(&self) -> Option<ExitCode> {
        // Check for forced exit
        if let Some(forced_exit) = self.thread.try_join() {
            return Some(forced_exit.unwrap_or_else(|err| {
                tracing::debug!(
                    error = &*err as &dyn std::error::Error,
                    "exit runtime error",
                );
                Errno::Child.into()
            }));
        }
        if let Some(forced_exit) = self.process.try_join() {
            return Some(forced_exit.unwrap_or_else(|err| {
                tracing::debug!(
                    error = &*err as &dyn std::error::Error,
                    "exit runtime error",
                );
                Errno::Child.into()
            }));
        }
        None
    }

    /// Accesses the virtual networking implementation
    pub fn net(&self) -> &DynVirtualNetworking {
        self.runtime.networking()
    }

    /// Providers safe access to the initialized part of WasiEnv
    /// (it must be initialized before it can be used)
    pub(crate) fn inner(&self) -> WasiInstanceGuard<'_> {
        self.inner.get().expect(
            "You must initialize the WasiEnv before using it and can not pass it between threads",
        )
    }

    /// Provides safe access to the initialized part of WasiEnv
    /// (it must be initialized before it can be used)
    pub(crate) fn inner_mut(&mut self) -> WasiInstanceGuardMut<'_> {
        self.inner.get_mut().expect(
            "You must initialize the WasiEnv before using it and can not pass it between threads",
        )
    }

    /// Providers safe access to the initialized part of WasiEnv
    pub(crate) fn try_inner(&self) -> Option<WasiInstanceGuard<'_>> {
        self.inner.get()
    }

    /// Providers safe access to the initialized part of WasiEnv
    /// (it must be initialized before it can be used)
    #[allow(dead_code)]
    pub(crate) fn try_inner_mut(&mut self) -> Option<WasiInstanceGuardMut<'_>> {
        self.inner.get_mut()
    }

    /// Sets the inner object (this should only be called when
    /// creating the instance and eventually should be moved out
    /// of the WasiEnv)
    #[doc(hidden)]
    pub(crate) fn set_inner(&mut self, handles: WasiModuleTreeHandles) {
        self.inner.set(handles)
    }

    /// Swaps this inner with the WasiEnvironment of another, this
    /// is used by the vfork so that the inner handles can be restored
    /// after the vfork finishes.
    #[doc(hidden)]
    pub(crate) fn swap_inner(&mut self, other: &mut Self) {
        std::mem::swap(&mut self.inner, &mut other.inner);
    }

    /// Helper function to ensure the module isn't dynamically linked, needed since
    /// we only support a subset of WASIX functionality for dynamically linked modules.
    /// Specifically, anything that requires asyncify is not supported right now.
    #[allow(dead_code)]
    pub(crate) fn ensure_static_module(&self) -> Result<(), ()> {
        self.inner.get().unwrap().ensure_static_module()
    }

    /// Tries to clone the instance from this environment, but only if it's a static
    /// module, since dynamically linked modules are made up of multiple instances.
    pub fn try_clone_instance(&self) -> Option<Instance> {
        let guard = self.inner.get();
        match guard {
            Some(guard) => guard
                .static_module_instance_handles()
                .map(|instance| instance.instance.clone()),
            None => None,
        }
    }

    /// Providers safe access to the memory
    /// (it must be initialized before it can be used)
    pub fn try_memory(&self) -> Option<WasiInstanceGuardMemory<'_>> {
        self.try_inner().map(|i| i.memory())
    }

    /// Providers safe access to the memory
    /// (it must be initialized before it can be used)
    ///
    /// # Safety
    /// This has been marked as unsafe as it will panic if its executed
    /// on the wrong thread or before the inner is set
    pub unsafe fn memory(&self) -> WasiInstanceGuardMemory<'_> {
        self.try_memory().expect(
            "You must initialize the WasiEnv before using it and can not pass it between threads",
        )
    }

    /// Providers safe access to the memory
    /// (it must be initialized before it can be used)
    pub fn try_memory_view<'a>(
        &self,
        store: &'a (impl AsStoreRef + ?Sized),
    ) -> Option<MemoryView<'a>> {
        self.try_memory().map(|m| m.view(store))
    }

    /// Providers safe access to the memory
    /// (it must be initialized before it can be used)
    ///
    /// # Safety
    /// This has been marked as unsafe as it will panic if its executed
    /// on the wrong thread or before the inner is set
    pub unsafe fn memory_view<'a>(&self, store: &'a (impl AsStoreRef + ?Sized)) -> MemoryView<'a> {
        self.try_memory_view(store).expect(
            "You must initialize the WasiEnv before using it and can not pass it between threads",
        )
    }

    /// Copy the lazy reference so that when it's initialized during the
    /// export phase, all the other references get a copy of it
    #[allow(dead_code)]
    pub(crate) fn try_memory_clone(&self) -> Option<Memory> {
        self.try_inner()
            .map(|i| i.main_module_instance_handles().memory_clone())
    }

    /// Get the WASI state
    pub(crate) fn state(&self) -> &WasiState {
        &self.state
    }

    /// Get the `VirtualFile` object at stdout
    pub fn stdout(&self) -> Result<Option<Box<dyn VirtualFile + Send + Sync + 'static>>, FsError> {
        self.state.stdout()
    }

    /// Get the `VirtualFile` object at stderr
    pub fn stderr(&self) -> Result<Option<Box<dyn VirtualFile + Send + Sync + 'static>>, FsError> {
        self.state.stderr()
    }

    /// Get the `VirtualFile` object at stdin
    pub fn stdin(&self) -> Result<Option<Box<dyn VirtualFile + Send + Sync + 'static>>, FsError> {
        self.state.stdin()
    }

    /// Returns true if the process should perform snapshots or not
    pub fn should_journal(&self) -> bool {
        self.enable_journal && !self.replaying_journal
    }

    /// Returns true if the environment has an active journal
    #[cfg(feature = "journal")]
    pub fn has_active_journal(&self) -> bool {
        self.runtime().active_journal().is_some()
    }

    /// Returns the active journal or fails with an error
    #[cfg(feature = "journal")]
    pub fn active_journal(&self) -> Result<&DynJournal, Errno> {
        self.runtime().active_journal().ok_or_else(|| {
            tracing::debug!("failed to save thread exit as there is not active journal");
            Errno::Fault
        })
    }

    /// Returns true if a particular snapshot trigger is enabled
    #[cfg(feature = "journal")]
    pub fn has_snapshot_trigger(&self, trigger: SnapshotTrigger) -> bool {
        let guard = self.process.inner.0.lock().unwrap();
        guard.snapshot_on.contains(&trigger)
    }

    /// Returns true if a particular snapshot trigger is enabled
    #[cfg(feature = "journal")]
    pub fn pop_snapshot_trigger(&mut self, trigger: SnapshotTrigger) -> bool {
        let mut guard = self.process.inner.0.lock().unwrap();
        if trigger.only_once() {
            guard.snapshot_on.remove(&trigger)
        } else {
            guard.snapshot_on.contains(&trigger)
        }
    }

    /// Internal helper function to get a standard device handle.
    /// Expects one of `__WASI_STDIN_FILENO`, `__WASI_STDOUT_FILENO`, `__WASI_STDERR_FILENO`.
    pub fn std_dev_get(
        &self,
        fd: crate::syscalls::WasiFd,
    ) -> Result<Option<Box<dyn VirtualFile + Send + Sync + 'static>>, FsError> {
        self.state.std_dev_get(fd)
    }

    /// Unsafe:
    ///
    /// This will access the memory of the WASM process and create a view into it which is
    /// inherently unsafe as it could corrupt the memory. Also accessing the memory is not
    /// thread safe.
    pub(crate) unsafe fn get_memory_and_wasi_state<'a>(
        &'a self,
        store: &'a impl AsStoreRef,
        _mem_index: u32,
    ) -> (MemoryView<'a>, &'a WasiState) {
        let memory = unsafe { self.memory_view(store) };
        let state = self.state.deref();
        (memory, state)
    }

    /// Unsafe:
    ///
    /// This will access the memory of the WASM process and create a view into it which is
    /// inherently unsafe as it could corrupt the memory. Also accessing the memory is not
    /// thread safe.
    pub(crate) unsafe fn get_memory_and_wasi_state_and_inodes<'a>(
        &'a self,
        store: &'a impl AsStoreRef,
        _mem_index: u32,
    ) -> (MemoryView<'a>, &'a WasiState, &'a WasiInodes) {
        let memory = unsafe { self.memory_view(store) };
        let state = self.state.deref();
        let inodes = &state.inodes;
        (memory, state, inodes)
    }

    pub(crate) fn get_wasi_state_and_inodes(&self) -> (&WasiState, &WasiInodes) {
        let state = self.state.deref();
        let inodes = &state.inodes;
        (state, inodes)
    }

    pub fn use_package(&self, pkg: &BinaryPackage) -> Result<(), WasiStateCreationError> {
        block_on(self.use_package_async(pkg))
    }

    /// Make all the commands in a [`BinaryPackage`] available to the WASI
    /// instance.
    ///
    /// The [`BinaryPackageCommand::atom()`][cmd-atom] will be saved to
    /// `/bin/command`.
    ///
    /// This will also merge the command's filesystem
    /// ([`BinaryPackage::webc_fs`][pkg-fs]) into the current filesystem.
    ///
    /// [cmd-atom]: crate::bin_factory::BinaryPackageCommand::atom()
    /// [pkg-fs]: crate::bin_factory::BinaryPackage::webc_fs
    pub async fn use_package_async(
        &self,
        pkg: &BinaryPackage,
    ) -> Result<(), WasiStateCreationError> {
        tracing::trace!(package=%pkg.id, "merging package dependency into wasi environment");
        let root_fs = &self.state.fs.root_fs;

        // We first need to merge the filesystem in the package into the
        // main file system, if it has not been merged already.
        if let Err(e) = self.state.fs.conditional_union(pkg).await {
            tracing::warn!(
                error = &e as &dyn std::error::Error,
                "Unable to merge the package's filesystem into the main one",
            );
        }

        // Next, make sure all commands will be available

        if !pkg.commands.is_empty() {
            let _ = root_fs.create_dir(Path::new("/bin"));
            let _ = root_fs.create_dir(Path::new("/usr"));
            let _ = root_fs.create_dir(Path::new("/usr/bin"));

            for command in &pkg.commands {
                let path = format!("/bin/{}", command.name());
                let path2 = format!("/usr/bin/{}", command.name());
                let path = Path::new(path.as_str());
                let path2 = Path::new(path2.as_str());

                let atom = command.atom();

                match root_fs {
                    WasiFsRoot::Sandbox(root_fs) => {
                        if let Err(err) = root_fs
                            .new_open_options_ext()
                            .insert_ro_file(path, atom.clone())
                        {
                            tracing::debug!(
                                "failed to add package [{}] command [{}] - {}",
                                pkg.id,
                                command.name(),
                                err
                            );
                            continue;
                        }
                        if let Err(err) = root_fs.new_open_options_ext().insert_ro_file(path2, atom)
                        {
                            tracing::debug!(
                                "failed to add package [{}] command [{}] - {}",
                                pkg.id,
                                command.name(),
                                err
                            );
                            continue;
                        }
                    }
                    WasiFsRoot::Overlay(ofs) => {
                        let root_fs = ofs.primary();

                        if let Err(err) = root_fs
                            .new_open_options_ext()
                            .insert_ro_file(path, atom.clone())
                        {
                            tracing::debug!(
                                "failed to add package [{}] command [{}] - {}",
                                pkg.id,
                                command.name(),
                                err
                            );
                            continue;
                        }
                        if let Err(err) = root_fs.new_open_options_ext().insert_ro_file(path2, atom)
                        {
                            tracing::debug!(
                                "failed to add package [{}] command [{}] - {}",
                                pkg.id,
                                command.name(),
                                err
                            );
                            continue;
                        }
                    }
                    WasiFsRoot::Backing(fs) => {
                        // FIXME: we're counting on the fs being a mem_fs here. Otherwise, memory
                        // usage will be very high.
                        let mut f = fs.new_open_options().create(true).write(true).open(path)?;
                        if let Err(e) = f.copy_from_owned_buffer(&atom).await {
                            tracing::warn!(
                                error = &e as &dyn std::error::Error,
                                "Unable to copy file reference",
                            );
                        }
                        let mut f = fs.new_open_options().create(true).write(true).open(path2)?;
                        if let Err(e) = f.copy_from_owned_buffer(&atom).await {
                            tracing::warn!(
                                error = &e as &dyn std::error::Error,
                                "Unable to copy file reference",
                            );
                        }
                    }
                }

                let mut package = pkg.clone();
                package.entrypoint_cmd = Some(command.name().to_string());
                let package_arc = Arc::new(package);
                self.bin_factory
                    .set_binary(path.to_string_lossy().as_ref(), &package_arc);
                self.bin_factory
                    .set_binary(path2.to_string_lossy().as_ref(), &package_arc);

                tracing::debug!(
                    package=%pkg.id,
                    command_name=command.name(),
                    path=%path.display(),
                    "Injected a command into the filesystem",
                );
            }
        }

        Ok(())
    }

    /// Given a list of packages, load them from the registry and make them
    /// available.
    pub fn uses<I>(&self, uses: I) -> Result<(), WasiStateCreationError>
    where
        I: IntoIterator<Item = String>,
    {
        let rt = self.runtime();

        for package_name in uses {
            let specifier = package_name.parse::<PackageSource>().map_err(|e| {
                WasiStateCreationError::WasiIncludePackageError(format!(
                    "package_name={package_name}, {e}",
                ))
            })?;
            let pkg = block_on(BinaryPackage::from_registry(&specifier, rt)).map_err(|e| {
                WasiStateCreationError::WasiIncludePackageError(format!(
                    "package_name={package_name}, {e}",
                ))
            })?;
            self.use_package(&pkg)?;
        }

        Ok(())
    }

    #[cfg(feature = "sys")]
    pub fn map_commands(
        &self,
        map_commands: std::collections::HashMap<String, std::path::PathBuf>,
    ) -> Result<(), WasiStateCreationError> {
        // Load all the mapped atoms
        #[allow(unused_imports)]
        use std::path::Path;

        use shared_buffer::OwnedBuffer;
        #[allow(unused_imports)]
        use virtual_fs::FileSystem;

        #[cfg(feature = "sys")]
        for (command, target) in map_commands.iter() {
            // Read the file
            let file = std::fs::read(target).map_err(|err| {
                WasiStateCreationError::WasiInheritError(format!(
                    "failed to read local binary [{}] - {}",
                    target.as_os_str().to_string_lossy(),
                    err
                ))
            })?;
            let file = OwnedBuffer::from(file);

            if let WasiFsRoot::Sandbox(root_fs) = &self.state.fs.root_fs {
                let _ = root_fs.create_dir(Path::new("/bin"));
                let _ = root_fs.create_dir(Path::new("/usr"));
                let _ = root_fs.create_dir(Path::new("/usr/bin"));

                let path = format!("/bin/{command}");
                let path = Path::new(path.as_str());
                if let Err(err) = root_fs
                    .new_open_options_ext()
                    .insert_ro_file(path, file.clone())
                {
                    tracing::debug!("failed to add atom command [{}] - {}", command, err);
                    continue;
                }
                let path = format!("/usr/bin/{command}");
                let path = Path::new(path.as_str());
                if let Err(err) = root_fs.new_open_options_ext().insert_ro_file(path, file) {
                    tracing::debug!("failed to add atom command [{}] - {}", command, err);
                    continue;
                }
            } else {
                tracing::debug!(
                    "failed to add atom command [{}] to the root file system as it is not sandboxed",
                    command
                );
                continue;
            }
        }
        Ok(())
    }

    /// Cleans up all the open files (if this is the main thread)
    #[allow(clippy::await_holding_lock)]
    pub fn blocking_on_exit(&self, process_exit_code: Option<ExitCode>) {
        let cleanup = self.on_exit(process_exit_code);
        block_on(cleanup);
    }

    /// Cleans up all the open files (if this is the main thread)
    #[allow(clippy::await_holding_lock)]
    pub fn on_exit(&self, process_exit_code: Option<ExitCode>) -> BoxFuture<'static, ()> {
        const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

        // If snap-shooting is enabled then we should record an event that the thread has exited.
        #[cfg(feature = "journal")]
        if self.should_journal() && self.has_active_journal() {
            if let Err(err) = JournalEffector::save_thread_exit(self, self.tid(), process_exit_code)
            {
                tracing::warn!("failed to save snapshot event for thread exit - {}", err);
            }

            if self.thread.is_main()
                && let Err(err) = JournalEffector::save_process_exit(self, process_exit_code)
            {
                tracing::warn!("failed to save snapshot event for process exit - {}", err);
            }
        }

        // If the process wants to exit, also close all files and terminate it
        if let Some(process_exit_code) = process_exit_code {
            let process = self.process.clone();
            let disable_fs_cleanup = self.disable_fs_cleanup;
            let pid = self.pid();
            let tid = self.tid();
            let shared_mutex_owner_token = {
                let token = pid.raw().wrapping_mul(65_537) ^ tid.raw();
                let token = token & 0x7fff_ffff;
                if token == 0 { 1 } else { token }
            };

            let timeout = self.tasks().sleep_now(CLEANUP_TIMEOUT);
            let state = self.state.clone();
            Box::pin(async move {
                // Wake and mark the process terminated before closing shared
                // descriptors. Threaded modules can otherwise leave cleanup
                // waiting for process activity that only terminates after the
                // close path completes.
                process.signal_process(Signal::Sigquit);
                process.terminate(process_exit_code);
                process.notify_parent_child_status_changed();
                let _ = shared_mutex_owner_token;

                if !disable_fs_cleanup {
                    tracing::trace!(pid = %pid, "cleaning up open file handles");

                    // Perform the clean operation using the asynchronous runtime
                    tokio::select! {
                        _ = timeout => {
                            tracing::debug!(
                                "WasiEnv::cleanup has timed out after {CLEANUP_TIMEOUT:?}"
                            );
                        },
                        _ = state.fs.close_all() => { }
                    }
                }
            })
        } else {
            Box::pin(async {})
        }
    }

    pub fn prepare_spawn(&self, cmd: &BinaryPackageCommand) {
        if let Ok(Some(Wasi {
            main_args,
            env: env_vars,
            exec_name,
            ..
        })) = cmd.metadata().wasi()
        {
            if let Some(env_vars) = env_vars {
                let env_vars = env_vars
                    .into_iter()
                    .map(|env_var| {
                        let (k, v) = env_var.split_once('=').unwrap();

                        (k.to_string(), v.as_bytes().to_vec())
                    })
                    .collect::<Vec<_>>();

                let env_vars = conv_env_vars(env_vars);

                self.state
                    .envs
                    .lock()
                    .unwrap()
                    .extend_from_slice(env_vars.as_slice());
            }

            if let Some(main_args) = main_args {
                let mut args: std::sync::MutexGuard<'_, Vec<String>> =
                    self.state.args.lock().unwrap();
                // Insert main-args before user args
                args.splice(1..1, main_args);
            }

            if let Some(exec_name) = exec_name {
                self.state.args.lock().unwrap()[0] = exec_name;
            }
        }
    }
}
