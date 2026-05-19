use super::*;
use crate::{
    os::task::process::{ProcessResourceLimit, WasiProcessId},
    syscalls::*,
    WasiProcess,
};
use wasmer::FromToNativeWasmType;
use wasmer_wasix_types::wasi::Snapshot0Clockid;

const RLIM_NLIMITS: i32 = 16;
const RUSAGE_CHILDREN: i32 = -1;
const RUSAGE_SELF: i32 = 0;
const RUSAGE_THREAD: i32 = 1;
const RLIMIT_AS: i32 = 9;
const RLIMIT_NOFILE: i32 = 7;

/// ### `proc_getrlimit()`
/// Returns a guest-visible POSIX resource limit for the current or selected process.
#[instrument(level = "trace", skip_all, fields(%pid, %resource), ret)]
pub fn proc_getrlimit<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    resource: i32,
    current_ptr: WasmPtr<u64, M>,
    maximum_ptr: WasmPtr<u64, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let Some(process) = selected_process(env, pid) else {
        return Ok(Errno::Srch);
    };
    if !valid_resource(resource) {
        return Ok(Errno::Inval);
    }
    let limit = process.resource_limit(resource);
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(current_ptr.write(&memory, limit.current));
    wasi_try_mem_ok!(maximum_ptr.write(&memory, limit.maximum));
    Ok(Errno::Success)
}

/// ### `getrlimit()`
/// POSIX-shaped current-process resource-limit query. Writes the current and
/// maximum limit values and returns 0 or a negative errno.
#[instrument(level = "trace", skip_all, fields(%resource), ret)]
pub fn getrlimit<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    resource: i32,
    current_ptr: WasmPtr<u64, M>,
    maximum_ptr: WasmPtr<u64, M>,
) -> Result<i32, WasiError> {
    let errno = proc_getrlimit(ctx, 0, resource, current_ptr, maximum_ptr)?;
    if errno == Errno::Success {
        Ok(0)
    } else {
        Ok(-errno.to_native())
    }
}

/// ### `proc_setrlimit()`
/// Sets a guest-visible POSIX resource limit for the current or selected process.
#[instrument(level = "trace", skip_all, fields(%pid, %resource, %current, %maximum), ret)]
pub fn proc_setrlimit(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    resource: i32,
    current: u64,
    maximum: u64,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let Some(process) = selected_process(env, pid) else {
        return Ok(Errno::Srch);
    };
    if !valid_resource(resource) || current > maximum {
        return Ok(Errno::Inval);
    }
    let old = process.resource_limit(resource);
    if maximum > old.maximum {
        return Ok(Errno::Perm);
    }
    process.set_resource_limit(resource, ProcessResourceLimit { current, maximum });
    if resource == RLIMIT_NOFILE && (pid == 0 || pid == env.process.pid().raw() as Pid) {
        env.sync_process_resource_limits();
    }
    Ok(Errno::Success)
}

/// ### `setrlimit()`
/// POSIX-shaped current-process resource-limit update. Returns 0 or a
/// negative errno.
#[instrument(level = "trace", skip_all, fields(%resource, %current, %maximum), ret)]
pub fn setrlimit(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    resource: i32,
    current: u64,
    maximum: u64,
) -> Result<i32, WasiError> {
    let errno = proc_setrlimit(ctx, 0, resource, current, maximum)?;
    if errno == Errno::Success {
        Ok(0)
    } else {
        Ok(-errno.to_native())
    }
}

/// ### `prlimit()`
/// Linux-shaped selected-process resource-limit operation. If `set_new` is
/// non-zero, updates the selected process before optionally returning the
/// resulting limits through non-null output pointers.
#[instrument(level = "trace", skip_all, fields(%pid, %resource, %set_new), ret)]
pub fn prlimit<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    resource: i32,
    set_new: i32,
    new_current: u64,
    new_maximum: u64,
    old_current_ptr: WasmPtr<u64, M>,
    old_maximum_ptr: WasmPtr<u64, M>,
) -> Result<i32, WasiError> {
    if set_new != 0 {
        let errno = proc_setrlimit(ctx.as_mut(), pid, resource, new_current, new_maximum)?;
        if errno != Errno::Success {
            return Ok(-errno.to_native());
        }
    }

    if old_current_ptr.is_null() && old_maximum_ptr.is_null() {
        return Ok(0);
    }
    if old_current_ptr.is_null() || old_maximum_ptr.is_null() {
        return Ok(-Errno::Fault.to_native());
    }
    let errno = proc_getrlimit(ctx, pid, resource, old_current_ptr, old_maximum_ptr)?;
    if errno == Errno::Success {
        Ok(0)
    } else {
        Ok(-errno.to_native())
    }
}

/// ### `proc_getrusage()`
/// Returns sandbox-scoped resource usage for the current process.
#[instrument(level = "trace", skip_all, fields(%who), ret)]
pub fn proc_getrusage<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    who: i32,
    user_usec_ptr: WasmPtr<u64, M>,
    system_usec_ptr: WasmPtr<u64, M>,
    maxrss_kib_ptr: WasmPtr<u64, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if !matches!(who, RUSAGE_CHILDREN | RUSAGE_SELF | RUSAGE_THREAD) {
        return Ok(Errno::Inval);
    }

    let env = ctx.data();
    let (user_usec, maxrss_kib) = if who == RUSAGE_CHILDREN {
        env.process.child_resource_usage()
    } else {
        let now = platform_clock_time_get(Snapshot0Clockid::Monotonic, 1_000_000).unwrap_or(0) as u128;
        let elapsed_nanos = now.saturating_sub(env.process.start_time_nanos());
        let user_usec = (elapsed_nanos / 1_000).min(u128::from(u64::MAX)) as u64;
        let address_space = env.process.resource_limit(RLIMIT_AS).current;
        let maxrss_kib = if address_space == u64::MAX {
            0
        } else {
            address_space / 1024
        };
        (user_usec, maxrss_kib)
    };

    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(user_usec_ptr.write(&memory, user_usec));
    wasi_try_mem_ok!(system_usec_ptr.write(&memory, 0));
    wasi_try_mem_ok!(maxrss_kib_ptr.write(&memory, maxrss_kib));
    Ok(Errno::Success)
}

/// ### `getrusage()`
/// POSIX-shaped resource-usage query. Writes user usec, system usec, and
/// maxrss KiB into guest-provided fields and returns 0 or a negative errno.
#[instrument(level = "trace", skip_all, fields(%who), ret)]
pub fn getrusage<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    who: i32,
    user_usec_ptr: WasmPtr<u64, M>,
    system_usec_ptr: WasmPtr<u64, M>,
    maxrss_kib_ptr: WasmPtr<u64, M>,
) -> Result<i32, WasiError> {
    let errno = proc_getrusage(ctx, who, user_usec_ptr, system_usec_ptr, maxrss_kib_ptr)?;
    if errno == Errno::Success {
        Ok(0)
    } else {
        Ok(-errno.to_native())
    }
}

/// ### `proc_times()`
/// Returns sandbox-scoped POSIX process times in 100 Hz clock ticks.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_times<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    elapsed_ticks_ptr: WasmPtr<u64, M>,
    user_ticks_ptr: WasmPtr<u64, M>,
    system_ticks_ptr: WasmPtr<u64, M>,
    child_user_ticks_ptr: WasmPtr<u64, M>,
    child_system_ticks_ptr: WasmPtr<u64, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let now = platform_clock_time_get(Snapshot0Clockid::Monotonic, 1_000_000).unwrap_or(0) as u128;
    let elapsed_nanos = now.saturating_sub(env.process.start_time_nanos());
    let ticks = (elapsed_nanos / 10_000_000).min(u128::from(u64::MAX)) as u64;
    let child_user_ticks = env.process.child_user_ticks();
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(elapsed_ticks_ptr.write(&memory, ticks));
    wasi_try_mem_ok!(user_ticks_ptr.write(&memory, ticks));
    wasi_try_mem_ok!(system_ticks_ptr.write(&memory, 0));
    wasi_try_mem_ok!(child_user_ticks_ptr.write(&memory, child_user_ticks));
    wasi_try_mem_ok!(child_system_ticks_ptr.write(&memory, 0));
    Ok(Errno::Success)
}

fn selected_process(env: &WasiEnv, pid: Pid) -> Option<WasiProcess> {
    if pid == 0 || pid == env.process.pid().raw() as Pid {
        return Some(env.process.clone());
    }
    env.control_plane.get_process(WasiProcessId::from(pid as u32))
}

fn valid_resource(resource: i32) -> bool {
    (0..RLIM_NLIMITS).contains(&resource)
}
