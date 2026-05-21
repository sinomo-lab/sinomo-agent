use std::f32::consts::E;

use super::*;
#[cfg(feature = "journal")]
use crate::journal::JournalEffector;
use crate::{
    WasiThreadHandle,
    os::task::{control_plane::ControlPlaneError, thread::WasiMemoryLayout},
    runtime::{
        TaintReason,
        task_manager::{TaskWasm, TaskWasmRunProperties},
    },
    state::context_switching::ContextSwitchingEnvironment,
    syscalls::*,
};

use wasmer::Memory;
use wasmer_wasix_types::wasi::ThreadStart;

macro_rules! sinomo_debug {
    ($($arg:tt)*) => {
        if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
            eprintln!($($arg)*);
        }
    };
}

/// ### `thread_spawn()`
/// Creates a new thread by spawning that shares the same
/// memory address space, file handles and main event loops.
///
/// ## Parameters
///
/// * `start_ptr` - Pointer to the structure that describes the thread to be launched
/// * `ret_tid` - ID of the thread that was launched
///
/// ## Return
///
/// Returns the thread index of the newly created thread
/// (indices always start from the same value as `pid` and increments in steps)
#[instrument(level = "trace", skip_all, ret)]
pub fn thread_spawn_v2<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    start_ptr: WasmPtr<ThreadStart<M>, M>,
    ret_tid: WasmPtr<Tid, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    // Create the thread
    let tid = wasi_try_ok!(thread_spawn_internal_from_wasi(&mut ctx, start_ptr));

    // Success
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    wasi_try_mem_ok!(ret_tid.write(&memory, tid));

    tracing::debug!(
        tid,
        from_tid = ctx.data().thread.id().raw(),
        "spawned new thread"
    );

    Ok(Errno::Success)
}

pub fn thread_spawn_internal_from_wasi<M: MemorySize>(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    start_ptr: WasmPtr<ThreadStart<M>, M>,
) -> Result<Tid, Errno> {
    // Now we use the environment and memory references
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let start_ptr_offset = start_ptr.offset();

    // Read the properties about the stack which we will use for asyncify
    let layout = {
        let start: ThreadStart<M> = start_ptr.read(&memory).map_err(mem_error_to_wasi)?;
        let stack_upper: u64 = start.stack_upper.into();
        let stack_size: u64 = start.stack_size.into();
        let guard_size: u64 = start.guard_size.into();
        let tls_base: u64 = start.tls_base.into();
        let stack_lower = stack_upper.checked_sub(stack_size).ok_or(Errno::Inval)?;

        WasiMemoryLayout {
            stack_upper,
            stack_lower,
            guard_size,
            stack_size,
            tls_base: Some(tls_base),
        }
    };
    ensure_thread_memory_bounds::<M>(ctx, &layout, start_ptr_offset.into())?;
    let env = ctx.data();
    tracing::trace!(
        from_tid = env.thread.id().raw(),
        "thread_spawn with layout {:?}",
        layout
    );
    if env.process.task_limit_reached() {
        return Err(Errno::Again);
    }

    // Create the handle that represents this thread
    let thread_start = ThreadStartType::ThreadSpawn {
        start_ptr: start_ptr_offset.into(),
    };
    let mut thread_handle = match env.process.new_thread(layout.clone(), thread_start) {
        Ok(h) => Arc::new(h),
        Err(ControlPlaneError::TaskLimitReached { max }) => {
            error!(
                stack_base = layout.stack_lower,
                max_tasks = max,
                "failed to create thread handle: task limit reached",
            );
            return Err(Errno::Again);
        }
        Err(err) => {
            error!(
                stack_base = layout.stack_lower,
                "failed to create thread handle",
            );
            // TODO: evaluate the appropriate error code, document it in the spec.
            return Err(Errno::Access);
        }
    };
    let (signal_mask_low, signal_mask_high) = env.thread.signal_mask();
    thread_handle.set_signal_mask(signal_mask_low, signal_mask_high);
    let thread_id: Tid = thread_handle.id().into();
    Span::current().record("tid", thread_id);

    // Spawn the thread
    thread_spawn_internal_using_layout::<M>(
        ctx,
        thread_handle,
        layout,
        start_ptr_offset,
        None,
        true,
    )?;

    // Success
    Ok(thread_id)
}

pub(crate) fn ensure_thread_memory_bounds<M: MemorySize>(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    layout: &WasiMemoryLayout,
    start_ptr_offset: u64,
) -> Result<(), Errno> {
    const WASM_PAGE_SIZE: u64 = 65_536;

    let handles = ctx.data().inner().main_module_instance_handles();
    let memory = handles.memory_clone();
    let instance = handles.instance.clone();
    let mut store = ctx.as_store_mut();
    let tls_size = exported_u64_global(&instance, &mut store, "__tls_size").unwrap_or(0);
    let tls_align = exported_u64_global(&instance, &mut store, "__tls_align").unwrap_or(0);
    let required_end = layout
        .stack_upper
        .max(layout.stack_lower.saturating_add(layout.stack_size))
        .max(
            layout
                .tls_base
                .unwrap_or(0)
                .saturating_add(tls_size)
                .saturating_add(tls_align),
        )
        .max(start_ptr_offset.saturating_add(std::mem::size_of::<ThreadStart<M>>() as u64));
    let current_bytes = u64::from(memory.size(&store).0).saturating_mul(WASM_PAGE_SIZE);
    if required_end <= current_bytes {
        return Ok(());
    }

    let delta_pages = required_end
        .saturating_sub(current_bytes)
        .div_ceil(WASM_PAGE_SIZE)
        .try_into()
        .map_err(|_| Errno::Overflow)?;
    memory
        .grow(&mut store, Pages(delta_pages))
        .map_err(|_| Errno::Nomem)?;
    Ok(())
}

fn exported_u64_global(
    instance: &Instance,
    store: &mut impl AsStoreMut,
    name: &str,
) -> Option<u64> {
    match instance.exports.get_global(name).ok()?.get(store) {
        Value::I32(value) => Some(value as u64),
        Value::I64(value) => Some(value as u64),
        _ => None,
    }
}

pub fn thread_spawn_internal_using_layout<M: MemorySize>(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    thread_handle: Arc<WasiThreadHandle>,
    layout: WasiMemoryLayout,
    start_ptr_offset: M::Offset,
    rewind_state: Option<(RewindState, RewindResultType)>,
    initialize_layout_globals: bool,
) -> Result<(), Errno> {
    // We extract the memory which will be passed to the thread
    let func_env = ctx.as_ref();
    let mut store = ctx.as_store_mut();
    let env = func_env.as_ref(&store);
    let tasks = env.tasks().clone();
    let process = env.process.clone();

    let env_inner = env.inner();
    let module_handles = env_inner.main_module_instance_handles();

    let thread_memory = module_handles.memory_clone();
    let linker = env_inner.linker().cloned();

    // We capture some local variables
    let state = env.state.clone();
    let mut thread_env = env.clone();
    let thread_id = thread_handle.id();
    thread_env.thread = thread_handle.as_thread();
    thread_env.layout = layout;

    // Spawned threads share the process state with the main thread, but their
    // stack checkpoint/rewind path is not independent enough to safely unload a
    // blocking syscall through deep sleep. Keep child threads resident and let
    // their blocking syscalls complete through the host future path.
    thread_env.enable_deep_sleep = false;

    // This next function gets a context for the local thread and then
    // calls into the process
    let mut execute_module = {
        let thread_handle = thread_handle;
        move |ctx: WasiFunctionEnv, mut store: Store| {
            // Call the thread
            call_module::<M>(
                ctx,
                store,
                start_ptr_offset,
                thread_handle,
                rewind_state,
                initialize_layout_globals,
            )
        }
    };

    // If the process does not export a thread spawn function then obviously
    // we can't spawn a background thread
    if module_handles.thread_spawn.is_none() {
        warn!("thread failed - the program does not export a `wasi_thread_start` function");
        process.remove_thread(&thread_id);
        return Err(Errno::Notcapable);
    }
    let thread_module = module_handles.module_clone();
    let spawn_type = match linker {
        Some(linker) => {
            crate::runtime::SpawnType::NewLinkerInstanceGroup(linker, func_env, store, false)
        }
        None => {
            let mut child_store = env.runtime().new_store();
            let child_memory = thread_memory
                .share_in_store(&store.as_store_ref(), &mut child_store.as_store_mut())
                .map_err(|err| {
                    warn!(
                        error = &err as &dyn std::error::Error,
                        "failed to share process memory with new thread",
                    );
                    Errno::Nomem
                })?;
            crate::runtime::SpawnType::StoreAndMemory(child_store, Some(child_memory))
        }
    };

    // Now spawn a thread
    trace!("threading: spawning background thread");
    let run = move |props: TaskWasmRunProperties| {
        execute_module(props.ctx, props.store);
    };

    let mut task_wasm = TaskWasm::new(Box::new(run), thread_env, thread_module, false, false)
        .with_memory(spawn_type);

    if let Err(err) = tasks.task_wasm(task_wasm) {
        process.remove_thread(&thread_id);
        return Err(err.into());
    }

    // Success
    Ok(())
}

// This function calls into the module
fn call_module_internal<M: MemorySize>(
    ctx: &WasiFunctionEnv,
    mut store: Store,
    start_ptr_offset: M::Offset,
    initialize_layout_globals: bool,
) -> (Store, Result<Option<ExitCode>, DeepSleepWork>) {
    if initialize_layout_globals && let Err(err) = initialize_static_thread_globals(ctx, &mut store)
    {
        let tid = ctx.data(&store).tid();
        warn!(%tid, ?err, "failed to initialize static thread globals");
        let exit_code = Some(err.into());
        ctx.on_exit(&mut store, None);
        return (store, Ok(exit_code));
    }
    let Some(spawn) = ctx
        .data(&store)
        .inner()
        .main_module_instance_handles()
        .thread_spawn
        .clone()
    else {
        let tid = ctx.data(&store).tid();
        warn!(%tid, "spawned thread is missing wasi_thread_start export");
        let exit_code = Some(Errno::Noexec.into());
        ctx.on_exit(&mut store, None);
        return (store, Ok(exit_code));
    };
    let tid = ctx.data(&store).tid();
    if tid.raw() >= 240 {
        sinomo_debug!(
            "sinomo-debug thread_start pid={} tid={}",
            ctx.data(&store).pid().raw(),
            tid.raw()
        );
    }

    let spawn: Function = spawn.into();
    let tid_i32 = match tid.raw().try_into().map_err(|_| Errno::Overflow) {
        Ok(tid) => tid,
        Err(err) => {
            warn!(%tid, "spawned thread id does not fit callback parameter");
            let exit_code = Some(err.into());
            ctx.on_exit(&mut store, None);
            return (store, Ok(exit_code));
        }
    };
    let start_pointer_i32 = match start_ptr_offset.try_into().map_err(|_| Errno::Overflow) {
        Ok(start_ptr) => start_ptr,
        Err(err) => {
            warn!(%tid, "spawned thread start pointer does not fit callback parameter");
            let exit_code = Some(err.into());
            ctx.on_exit(&mut store, None);
            return (store, Ok(exit_code));
        }
    };
    let (mut store, thread_result) = ContextSwitchingEnvironment::run_main_context(
        ctx,
        store,
        spawn,
        vec![Value::I32(tid_i32), Value::I32(start_pointer_i32)],
    );
    let thread_result = thread_result.map(|_| ());

    trace!("callback finished (ret={:?})", thread_result);

    let exit_code = match handle_thread_result(ctx, &mut store, thread_result) {
        Ok(code) => code,
        Err(deep_sleep) => return (store, Err(deep_sleep)),
    };
    if tid.raw() >= 240 {
        sinomo_debug!(
            "sinomo-debug thread_exit_callback pid={} tid={} exit={:?}",
            ctx.data(&store).pid().raw(),
            tid.raw(),
            exit_code
        );
    }

    // Clean up the environment on exit
    ctx.on_exit(&mut store, None);
    (store, Ok(exit_code))
}

fn initialize_static_thread_globals(ctx: &WasiFunctionEnv, store: &mut Store) -> Result<(), Errno> {
    let (layout, stack_pointer, tls_base_global) = {
        let env = ctx.data(&store);
        let handles = env.inner().main_module_instance_handles();
        (
            env.layout.clone(),
            handles.stack_pointer.clone(),
            handles.tls_base.clone(),
        )
    };
    set_u64_global(store, stack_pointer.as_ref(), layout.stack_upper)?;
    if let Some(tls_base) = layout.tls_base {
        set_u64_global(store, tls_base_global.as_ref(), tls_base)?;
    }
    Ok(())
}

fn set_u64_global(store: &mut Store, global: Option<&Global>, value: u64) -> Result<(), Errno> {
    let Some(global) = global else {
        return Ok(());
    };
    let current = global.get(&mut *store);
    let value = match current {
        Value::I32(_) => Value::I32(value.try_into().map_err(|_| Errno::Overflow)?),
        Value::I64(_) => Value::I64(value.try_into().map_err(|_| Errno::Overflow)?),
        _ => return Err(Errno::Inval),
    };
    global.set(&mut *store, value).map_err(|_| Errno::Perm)
}

fn handle_thread_result(
    env: &WasiFunctionEnv,
    store: &mut Store,
    err: Result<(), RuntimeError>,
) -> Result<Option<ExitCode>, DeepSleepWork> {
    let tid = env.data(&store).tid();
    let pid = env.data(&store).pid();
    let Err(err) = err else {
        trace!("thread exited cleanly without calling thread_exit");
        return Ok(None);
    };
    match err.downcast::<WasiError>() {
        Ok(WasiError::ThreadExit) => {
            trace!("thread exited cleanly");
            Ok(None)
        }
        Ok(WasiError::Exit(code)) => {
            trace!(exit_code = ?code, "thread requested exit");
            if !code.is_success() {
                // TODO: Why do we need to taint the runtime on a non-zero exit code? Why not also for zero?
                env.data(&store)
                    .runtime
                    .on_taint(TaintReason::NonZeroExitCode(code));
            };
            env.data(&store).process.terminate(code);
            Ok(Some(code))
        }
        Ok(WasiError::DeepSleep(deep)) => {
            trace!("entered a deep sleep");
            Err(deep)
        }
        Ok(WasiError::UnknownWasiVersion) => {
            sinomo_debug!(
                "Thread {tid} of process {pid} failed because it has an unknown wasix version"
            );
            env.data(&store)
                .runtime
                .on_taint(TaintReason::UnknownWasiVersion);
            Ok(Some(ExitCode::from(129)))
        }
        Ok(WasiError::DlSymbolResolutionFailed(symbol)) => {
            sinomo_debug!("Thread {tid} of process {pid} failed to find required symbol: {symbol}");
            env.data(&store)
                .runtime
                .on_taint(TaintReason::DlSymbolResolutionFailed(symbol.clone()));
            Ok(Some(ExitCode::from(129)))
        }
        Err(err) => {
            sinomo_debug!("Thread {tid} of process {pid} failed with runtime error: {err}");
            env.data(&store)
                .runtime
                .on_taint(TaintReason::RuntimeError(err));
            Ok(Some(ExitCode::from(129)))
        }
    }
}

/// Calls the module
fn call_module<M: MemorySize>(
    mut ctx: WasiFunctionEnv,
    mut store: Store,
    start_ptr_offset: M::Offset,
    thread_handle: Arc<WasiThreadHandle>,
    rewind_state: Option<(RewindState, RewindResultType)>,
    initialize_layout_globals: bool,
) {
    let env = ctx.data(&store);
    let tasks = env.tasks().clone();

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
            return;
        }
    }

    // Now invoke the module
    let (store, ret) =
        call_module_internal::<M>(&ctx, store, start_ptr_offset, initialize_layout_globals);

    // If it went to deep sleep then we need to handle that
    let exit_code = match ret {
        Ok(exit_code) => exit_code.unwrap_or_else(|| Errno::Success.into()),
        Err(deep) => {
            // Create the callback that will be invoked when the thread respawns after a deep sleep
            let rewind = deep.rewind;
            let respawn = {
                let tasks = tasks.clone();
                move |ctx, store, trigger_res| {
                    // Call the thread
                    call_module::<M>(
                        ctx,
                        store,
                        start_ptr_offset,
                        thread_handle,
                        Some((rewind, RewindResultType::RewindWithResult(trigger_res))),
                        initialize_layout_globals,
                    );
                }
            };

            /// Spawns the WASM process after a trigger
            unsafe {
                tasks.resume_wasm_after_poller(Box::new(respawn), ctx, store, deep.trigger)
            };
            return;
        }
    };
    thread_handle.set_status_finished(Ok(exit_code));
    drop(thread_handle);
}
