use super::*;
use crate::syscalls::*;

use crate::{
    WasiThreadHandle, os::task::thread::WasiMemoryLayout, runtime::task_manager::TaskWasm,
    state::context_switching::ContextSwitchingEnvironment,
};
use std::sync::Arc;
use wasmer::{Function, Memory, Store, Value};
use wasmer_wasix_types::wasi::ThreadStart;
use wasmer_wasix_types::wasix::ThreadStartType;

/// ### `thread_spawn()`
/// Creates a new thread by spawning that shares the same
/// memory address space, file handles and main event loops.
///
/// ## Parameters
///
/// * `start_ptr` - Pointer to the structure that describes the thread to be launched
///
/// ## Return
///
/// Returns the thread index of the newly created thread
/// (indices always start from the same value as `pid` and increments in steps)
#[instrument(level = "trace", skip_all, ret)]
pub fn thread_spawn<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    start_ptr: WasmPtr<ThreadStart<M>, M>,
) -> i32 {
    thread_spawn_legacy_internal_from_wasi::<M>(&mut ctx, start_ptr.offset())
        .map(|tid| tid as i32)
        .map_err(|errno| errno as i32)
        .unwrap_or_else(|err| -err)
}

fn thread_spawn_legacy_internal_from_wasi<M: MemorySize>(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    start_arg: M::Offset,
) -> Result<Tid, Errno> {
    WasiEnv::do_pending_operations(ctx).map_err(|_| Errno::Intr)?;

    let layout = legacy_thread_layout_from_start_arg::<M>(ctx, start_arg)?;
    ensure_thread_memory_bounds::<M>(ctx, &layout, start_arg.into())?;
    let env = ctx.data();
    if env.process.task_limit_reached() {
        return Err(Errno::Again);
    }
    let thread_start = ThreadStartType::ThreadSpawn {
        start_ptr: start_arg.into(),
    };
    let thread_handle = Arc::new(
        env.process
            .new_thread(layout.clone(), thread_start)
            .map_err(|_| Errno::Access)?,
    );
    let (signal_mask_low, signal_mask_high) = env.thread.signal_mask();
    thread_handle.set_signal_mask(signal_mask_low, signal_mask_high);
    let thread_id: Tid = thread_handle.id().into();

    thread_spawn_internal_using_layout::<M>(ctx, thread_handle, layout, start_arg, None, false)?;

    Ok(thread_id)
}

fn legacy_thread_layout_from_start_arg<M: MemorySize>(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    start_arg: M::Offset,
) -> Result<WasiMemoryLayout, Errno> {
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let start_ptr = WasmPtr::<ThreadStart<M>, M>::new(start_arg);
    let start = start_ptr.read(&memory).map_err(mem_error_to_wasi)?;
    let stack_upper: u64 = start.stack_upper.into();
    let stack_size: u64 = start.stack_size.into();
    let guard_size: u64 = start.guard_size.into();
    let tls_base: u64 = start.tls_base.into();
    let stack_lower = stack_upper.checked_sub(stack_size).ok_or(Errno::Inval)?;
    Ok(WasiMemoryLayout {
        stack_upper,
        stack_lower,
        guard_size,
        stack_size,
        tls_base: Some(tls_base),
    })
}
