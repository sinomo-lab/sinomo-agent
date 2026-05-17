use super::*;
use crate::WasiProcess;
use crate::syscalls::*;

/// ### `proc_parent()`
/// Returns the parent handle of the supplied process
#[instrument(level = "trace", skip_all, fields(%pid, parent = field::Empty), ret)]
pub fn proc_parent<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    ret_parent: WasmPtr<Pid, M>,
) -> Errno {
    let env = ctx.data();
    let pid: WasiProcessId = pid.into();
    if pid == env.process.pid() {
        let memory = unsafe { env.memory_view(&ctx) };
        Span::current().record("parent", env.process.ppid().raw());
        wasi_try_mem!(ret_parent.write(&memory, env.process.ppid().raw() as Pid));
        Errno::Success
    } else if let Some(process) = env.control_plane.get_process(pid) {
        let memory = unsafe { env.memory_view(&ctx) };
        Span::current().record("parent", process.ppid().raw());
        wasi_try_mem!(ret_parent.write(&memory, process.ppid().raw() as Pid));
        Errno::Success
    } else {
        Errno::Badf
    }
}

/// ### `proc_inherit_parent()`
/// Reparents the current process to its current parent's parent.
///
/// This backs Linux `clone(CLONE_PARENT)` for process-style clone calls: after
/// the child is created through the normal fork path, the child adopts the same
/// parent as its creator while preserving process table ownership.
#[instrument(level = "trace", skip_all, fields(pid = field::Empty, old_parent = field::Empty, new_parent = field::Empty), ret)]
pub fn proc_inherit_parent(mut ctx: FunctionEnvMut<'_, WasiEnv>) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let child = env.process.clone();
    let child_pid = child.pid();
    let old_parent_pid = child.ppid();
    Span::current()
        .record("pid", child_pid.raw())
        .record("old_parent", old_parent_pid.raw());

    if old_parent_pid.raw() == 0 {
        return Ok(Errno::Success);
    }

    let Some(old_parent) = env.control_plane.get_process(old_parent_pid) else {
        child.reparent_to(None);
        Span::current().record("new_parent", 0);
        return Ok(Errno::Success);
    };

    let new_parent_pid = old_parent.ppid();
    let new_parent = if new_parent_pid.raw() == 0 {
        None
    } else {
        env.control_plane.get_process(new_parent_pid)
    };

    old_parent.remove_child(child_pid);
    if let Some(parent) = &new_parent {
        parent.add_child(child.clone());
        child.reparent_to(Some(parent.pid()));
        Span::current().record("new_parent", parent.pid().raw());
    } else {
        child.reparent_to(None);
        Span::current().record("new_parent", 0);
    }

    Ok(Errno::Success)
}

/// ### `proc_set_parent_death_signal()`
/// Stores the signal delivered to the current process when its parent exits.
#[instrument(level = "trace", skip_all, fields(signal = ?signal), ret)]
pub fn proc_set_parent_death_signal(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    signal: Signal,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    ctx.data().process.set_parent_death_signal(signal);
    Ok(Errno::Success)
}

/// ### `proc_get_parent_death_signal()`
/// Returns the signal delivered to the current process when its parent exits.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_get_parent_death_signal<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ret_signal: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let signal = env.process.parent_death_signal() as u32;
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_signal.write(&memory, signal));
    Ok(Errno::Success)
}

/// ### `proc_set_child_subreaper()`
/// Stores whether the current process should adopt orphaned descendant processes.
#[instrument(level = "trace", skip_all, fields(enabled), ret)]
pub fn proc_set_child_subreaper(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    enabled: Bool,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    ctx.data()
        .process
        .set_child_subreaper(enabled == Bool::True);
    Ok(Errno::Success)
}

/// ### `proc_get_child_subreaper()`
/// Returns whether the current process adopts orphaned descendant processes.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_get_child_subreaper<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ret_enabled: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let enabled = u32::from(env.process.child_subreaper());
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_enabled.write(&memory, enabled));
    Ok(Errno::Success)
}

/// ### `proc_set_name()`
/// Stores the POSIX process name used by prctl and procfs.
#[instrument(level = "trace", skip_all, fields(name = field::Empty), ret)]
pub fn proc_set_name<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    name: WasmPtr<u8, M>,
    name_len: M::Offset,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let name = name.read_utf8_string(&memory, name_len).map_err(|err| {
        debug!("failed to read process name: {err}");
        WasiError::Exit(Errno::Fault.into())
    })?;
    Span::current().record("name", name.as_str());
    env.process.set_process_name(Some(name));
    Ok(Errno::Success)
}

/// ### `proc_get_name()`
/// Returns the POSIX process name used by prctl and procfs.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_get_name<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ret_name: WasmPtr<u8, M>,
    ret_name_len: M::Offset,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let name = env
        .process
        .process_name()
        .unwrap_or_else(|| default_process_name(&env.process));
    let memory = unsafe { env.memory_view(&ctx) };
    let ret_name_len_u64: u64 = ret_name_len.into();
    let len = usize::try_from(ret_name_len_u64)
        .map_err(|_| WasiError::Exit(Errno::Overflow.into()))?;
    if len == 0 {
        return Ok(Errno::Success);
    }

    let view = ret_name.slice(&memory, ret_name_len).map_err(|err| {
        debug!("failed to get process name buffer: {err}");
        WasiError::Exit(Errno::Fault.into())
    })?;
    let mut bytes = vec![0u8; len];
    let copy_len = name.as_bytes().len().min(len.saturating_sub(1));
    bytes[..copy_len].copy_from_slice(&name.as_bytes()[..copy_len]);
    view.write_slice(&bytes).map_err(|err| {
        debug!("failed to write process name: {err}");
        WasiError::Exit(Errno::Fault.into())
    })?;
    Ok(Errno::Success)
}

const PROC_UTS_HOSTNAME: u32 = 1;
const PROC_UTS_DOMAINNAME: u32 = 2;

/// ### `proc_set_uts_name()`
/// Stores the guest-visible UTS hostname or domain name for the current process.
#[instrument(level = "trace", skip_all, fields(kind, name = field::Empty), ret)]
pub fn proc_set_uts_name<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    kind: u32,
    name: WasmPtr<u8, M>,
    name_len: M::Offset,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let name = name.read_utf8_string(&memory, name_len).map_err(|err| {
        debug!("failed to read UTS name: {err}");
        WasiError::Exit(Errno::Fault.into())
    })?;
    Span::current().record("name", name.as_str());
    match kind {
        PROC_UTS_HOSTNAME => env.process.set_hostname(name),
        PROC_UTS_DOMAINNAME => env.process.set_domainname(name),
        _ => return Ok(Errno::Inval),
    }
    Ok(Errno::Success)
}

/// ### `proc_get_uts_name()`
/// Returns the guest-visible UTS hostname or domain name for the current process.
#[instrument(level = "trace", skip_all, fields(kind), ret)]
pub fn proc_get_uts_name<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    kind: u32,
    ret_name: WasmPtr<u8, M>,
    ret_name_len: M::Offset,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let name = match kind {
        PROC_UTS_HOSTNAME => env.process.hostname(),
        PROC_UTS_DOMAINNAME => env.process.domainname(),
        _ => return Ok(Errno::Inval),
    };
    let memory = unsafe { env.memory_view(&ctx) };
    let ret_name_len_u64: u64 = ret_name_len.into();
    let len = usize::try_from(ret_name_len_u64)
        .map_err(|_| WasiError::Exit(Errno::Overflow.into()))?;
    if len == 0 {
        return Ok(Errno::Success);
    }

    let view = ret_name.slice(&memory, ret_name_len).map_err(|err| {
        debug!("failed to get UTS name buffer: {err}");
        WasiError::Exit(Errno::Fault.into())
    })?;
    let mut bytes = vec![0u8; len];
    let copy_len = name.as_bytes().len().min(len.saturating_sub(1));
    bytes[..copy_len].copy_from_slice(&name.as_bytes()[..copy_len]);
    view.write_slice(&bytes).map_err(|err| {
        debug!("failed to write UTS name: {err}");
        WasiError::Exit(Errno::Fault.into())
    })?;
    Ok(Errno::Success)
}

pub(crate) fn default_process_name(process: &WasiProcess) -> String {
    process
        .args()
        .first()
        .map(|arg| {
            std::path::Path::new(arg)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(arg)
                .to_string()
        })
        .unwrap_or_else(|| "wasix".to_string())
}

/// ### `proc_set_dumpable()`
/// Stores the guest-visible dumpable process flag.
#[instrument(level = "trace", skip_all, fields(dumpable), ret)]
pub fn proc_set_dumpable(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    dumpable: Bool,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    ctx.data().process.set_dumpable(dumpable == Bool::True);
    Ok(Errno::Success)
}

/// ### `proc_get_dumpable()`
/// Returns the guest-visible dumpable process flag.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_get_dumpable<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ret_dumpable: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let dumpable = u32::from(env.process.dumpable());
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_dumpable.write(&memory, dumpable));
    Ok(Errno::Success)
}

/// ### `proc_set_no_new_privs()`
/// Stores the guest-visible no-new-privileges flag.
#[instrument(level = "trace", skip_all, fields(enabled), ret)]
pub fn proc_set_no_new_privs(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    enabled: Bool,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    ctx.data().process.set_no_new_privs(enabled == Bool::True);
    Ok(Errno::Success)
}

/// ### `proc_get_no_new_privs()`
/// Returns the guest-visible no-new-privileges flag.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_get_no_new_privs<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ret_enabled: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let enabled = u32::from(env.process.no_new_privs());
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_enabled.write(&memory, enabled));
    Ok(Errno::Success)
}

/// ### `proc_set_seccomp_mode()`
/// Stores the guest-visible seccomp mode metadata.
#[instrument(level = "trace", skip_all, fields(mode), ret)]
pub fn proc_set_seccomp_mode(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    mode: u32,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    ctx.data().process.set_seccomp_mode(mode);
    Ok(Errno::Success)
}

/// ### `proc_get_seccomp_mode()`
/// Returns the guest-visible seccomp mode metadata.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_get_seccomp_mode<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ret_mode: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let mode = env.process.seccomp_mode();
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_mode.write(&memory, mode));
    Ok(Errno::Success)
}

/// ### `proc_set_timer_slack()`
/// Stores the guest-visible timer slack in nanoseconds.
#[instrument(level = "trace", skip_all, fields(nanos), ret)]
pub fn proc_set_timer_slack(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    nanos: u64,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    ctx.data().process.set_timer_slack_nanos(nanos);
    Ok(Errno::Success)
}

/// ### `proc_get_timer_slack()`
/// Returns the guest-visible timer slack in nanoseconds.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_get_timer_slack<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ret_nanos: WasmPtr<u64, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let nanos = env.process.timer_slack_nanos();
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_nanos.write(&memory, nanos));
    Ok(Errno::Success)
}

/// ### `proc_personality()`
/// Reads and optionally replaces the guest-visible Linux personality metadata.
#[instrument(level = "trace", skip_all, fields(personality), ret)]
pub fn proc_personality<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    personality: u64,
    ret_previous: WasmPtr<u64, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let previous = env.process.personality();
    if personality != u64::MAX {
        env.process.set_personality(personality);
    }
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_previous.write(&memory, previous));
    Ok(Errno::Success)
}

/// ### `proc_set_clear_child_tid()`
/// Stores the guest clear-child-TID address for the current thread.
#[instrument(level = "trace", skip_all, fields(address), ret)]
pub fn proc_set_clear_child_tid(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    address: u64,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    let env = ctx.data();
    env.thread.set_clear_child_tid(address);
    env.process.set_clear_child_tid(address);
    Ok(Errno::Success)
}

/// ### `proc_get_clear_child_tid()`
/// Returns the guest clear-child-TID address for the current thread.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_get_clear_child_tid<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ret_address: WasmPtr<u64, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let address = env.thread.clear_child_tid();
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_address.write(&memory, address));
    Ok(Errno::Success)
}

/// ### `proc_set_robust_list()`
/// Stores guest robust-futex-list metadata for the current thread.
#[instrument(level = "trace", skip_all, fields(head, len), ret)]
pub fn proc_set_robust_list(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    head: u64,
    len: u64,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    let env = ctx.data();
    env.thread.set_robust_list(head, len);
    env.process.set_robust_list(head, len);
    Ok(Errno::Success)
}

/// ### `proc_get_robust_list()`
/// Returns guest robust-futex-list metadata for the selected process.
#[instrument(level = "trace", skip_all, fields(pid), ret)]
pub fn proc_get_robust_list<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    ret_head: WasmPtr<u64, M>,
    ret_len: WasmPtr<u64, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let (head, len) = if pid == 0 || WasiProcessId::from(pid) == env.process.pid() {
        env.thread
            .robust_list()
            .or_else(|| env.process.robust_list())
            .unwrap_or((0, 0))
    } else {
        let requested = WasiProcessId::from(pid);
        let Some(process) = env.control_plane.get_process(requested) else {
            return Ok(Errno::Srch);
        };
        process.robust_list().unwrap_or((0, 0))
    };
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_head.write(&memory, head));
    wasi_try_mem_ok!(ret_len.write(&memory, len));
    Ok(Errno::Success)
}

/// ### `proc_get_capabilities()`
/// Returns guest-visible Linux capability masks for the current process.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_get_capabilities<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ret_effective: WasmPtr<u64, M>,
    ret_permitted: WasmPtr<u64, M>,
    ret_inheritable: WasmPtr<u64, M>,
    ret_bounding: WasmPtr<u64, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let (effective, permitted, inheritable, bounding) = env.process.capabilities();
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_effective.write(&memory, effective));
    wasi_try_mem_ok!(ret_permitted.write(&memory, permitted));
    wasi_try_mem_ok!(ret_inheritable.write(&memory, inheritable));
    wasi_try_mem_ok!(ret_bounding.write(&memory, bounding));
    Ok(Errno::Success)
}

/// ### `proc_set_capabilities()`
/// Stores guest-visible Linux capability masks for the current process.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_set_capabilities(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    effective: u64,
    permitted: u64,
    inheritable: u64,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    ctx.data()
        .process
        .set_capabilities(effective, permitted, inheritable);
    Ok(Errno::Success)
}

/// ### `proc_get_capability_bounding()`
/// Returns whether a capability is present in the guest-visible bounding set.
#[instrument(level = "trace", skip_all, fields(capability), ret)]
pub fn proc_get_capability_bounding<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    capability: u32,
    ret_enabled: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    if capability >= 64 {
        return Ok(Errno::Inval);
    }

    let env = ctx.data();
    let enabled = u32::from(env.process.capability_in_bounding_set(capability));
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_enabled.write(&memory, enabled));
    Ok(Errno::Success)
}

/// ### `proc_drop_capability_bounding()`
/// Drops a capability from the guest-visible bounding set.
#[instrument(level = "trace", skip_all, fields(capability), ret)]
pub fn proc_drop_capability_bounding(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    capability: u32,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    if capability >= 64 {
        return Ok(Errno::Inval);
    }

    ctx.data()
        .process
        .drop_capability_from_bounding_set(capability);
    Ok(Errno::Success)
}

const PROC_CONTROL_KEEPCAPS: u32 = 1;
const PROC_CONTROL_SECUREBITS: u32 = 2;
const PROC_CONTROL_TIMING_MODE: u32 = 3;
const PROC_CONTROL_THP_DISABLE: u32 = 4;
const PROC_CONTROL_IO_FLUSHER: u32 = 5;
const PROC_CONTROL_PTRACER: u32 = 6;

/// ### `proc_set_process_control()`
/// Stores guest-visible Linux prctl process-control metadata.
#[instrument(level = "trace", skip_all, fields(key, value), ret)]
pub fn proc_set_process_control(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    key: u32,
    value: u64,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    let process = &ctx.data().process;
    match key {
        PROC_CONTROL_KEEPCAPS => process.set_keepcaps(value != 0),
        PROC_CONTROL_SECUREBITS => process.set_securebits(value as u32),
        PROC_CONTROL_TIMING_MODE => process.set_timing_mode(value as u32),
        PROC_CONTROL_THP_DISABLE => process.set_thp_disable(value as u32),
        PROC_CONTROL_IO_FLUSHER => process.set_io_flusher(value != 0),
        PROC_CONTROL_PTRACER => process.set_ptracer(value as i32),
        _ => return Ok(Errno::Inval),
    }
    Ok(Errno::Success)
}

/// ### `proc_get_process_control()`
/// Returns guest-visible Linux prctl process-control metadata.
#[instrument(level = "trace", skip_all, fields(key), ret)]
pub fn proc_get_process_control<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    key: u32,
    ret_value: WasmPtr<u64, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    let process = &ctx.data().process;
    let value = match key {
        PROC_CONTROL_KEEPCAPS => u64::from(process.keepcaps()),
        PROC_CONTROL_SECUREBITS => u64::from(process.securebits()),
        PROC_CONTROL_TIMING_MODE => u64::from(process.timing_mode()),
        PROC_CONTROL_THP_DISABLE => u64::from(process.thp_disable()),
        PROC_CONTROL_IO_FLUSHER => u64::from(process.io_flusher()),
        PROC_CONTROL_PTRACER => process.ptracer() as u64,
        _ => return Ok(Errno::Inval),
    };
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_value.write(&memory, value));
    Ok(Errno::Success)
}

/// ### `proc_unshare_namespaces()`
/// Records sandbox-local namespace unshare/setns metadata for the current process.
#[instrument(level = "trace", skip_all, fields(flags), ret)]
pub fn proc_unshare_namespaces(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    flags: u64,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    let namespaces = namespaces_for_clone_flags(flags);
    ctx.data().process.unshare_namespaces(&namespaces);
    Ok(Errno::Success)
}

fn namespaces_for_clone_flags(flags: u64) -> Vec<&'static str> {
    let mut namespaces = Vec::new();
    if flags & 0x0200_0000 != 0 {
        namespaces.push("cgroup");
    }
    if flags & 0x0800_0000 != 0 {
        namespaces.push("ipc");
    }
    if flags & 0x0002_0000 != 0 {
        namespaces.push("mnt");
    }
    if flags & 0x4000_0000 != 0 {
        namespaces.push("net");
    }
    if flags & 0x2000_0000 != 0 {
        namespaces.push("pid");
        namespaces.push("pid_for_children");
    }
    if flags & 0x0400_0000 != 0 {
        namespaces.push("uts");
    }
    namespaces
}

const PROC_ARCH_FS_BASE: u32 = 1;
const PROC_ARCH_GS_BASE: u32 = 2;

/// ### `proc_set_arch_base()`
/// Stores guest-visible architecture TLS-base metadata.
#[instrument(level = "trace", skip_all, fields(kind, address), ret)]
pub fn proc_set_arch_base(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    kind: u32,
    address: u64,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    match kind {
        PROC_ARCH_FS_BASE => ctx.data().process.set_arch_fs_base(address),
        PROC_ARCH_GS_BASE => ctx.data().process.set_arch_gs_base(address),
        _ => return Ok(Errno::Inval),
    }
    Ok(Errno::Success)
}

/// ### `proc_get_arch_base()`
/// Returns guest-visible architecture TLS-base metadata.
#[instrument(level = "trace", skip_all, fields(kind), ret)]
pub fn proc_get_arch_base<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    kind: u32,
    ret_address: WasmPtr<u64, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    let address = match kind {
        PROC_ARCH_FS_BASE => ctx.data().process.arch_fs_base(),
        PROC_ARCH_GS_BASE => ctx.data().process.arch_gs_base(),
        _ => return Ok(Errno::Inval),
    };
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_address.write(&memory, address));
    Ok(Errno::Success)
}

/// ### `proc_set_rseq()`
/// Stores guest-visible restartable-sequence registration metadata.
#[instrument(level = "trace", skip_all, fields(address, len, signature), ret)]
pub fn proc_set_rseq(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    address: u64,
    len: u32,
    signature: u32,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    ctx.data().process.set_rseq(address, len, signature);
    Ok(Errno::Success)
}

/// ### `proc_clear_rseq()`
/// Clears restartable-sequence registration metadata if the address matches.
#[instrument(level = "trace", skip_all, fields(address), ret)]
pub fn proc_clear_rseq(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    address: u64,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    if ctx.data().process.clear_rseq_if_matches(address) {
        Ok(Errno::Success)
    } else {
        Ok(Errno::Inval)
    }
}

/// ### `proc_set_mempolicy()`
/// Stores guest-visible NUMA memory-policy metadata.
#[instrument(level = "trace", skip_all, fields(mode, nodes), ret)]
pub fn proc_set_mempolicy(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    mode: i32,
    nodes: u64,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    ctx.data().process.set_mempolicy(mode, nodes);
    Ok(Errno::Success)
}

/// ### `proc_get_mempolicy()`
/// Returns guest-visible NUMA memory-policy metadata.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_get_mempolicy<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ret_mode: WasmPtr<i32, M>,
    ret_nodes: WasmPtr<u64, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    let env = ctx.data();
    let (mode, nodes) = env.process.mempolicy();
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_mode.write(&memory, mode));
    wasi_try_mem_ok!(ret_nodes.write(&memory, nodes));
    Ok(Errno::Success)
}

/// ### `proc_set_mempolicy_home_node()`
/// Stores guest-visible NUMA home-node metadata.
#[instrument(level = "trace", skip_all, fields(home_node), ret)]
pub fn proc_set_mempolicy_home_node(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    home_node: u64,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    ctx.data().process.set_mempolicy_home_node(home_node);
    Ok(Errno::Success)
}

/// ### `proc_set_ioprio()`
/// Stores guest-visible Linux I/O priority metadata.
#[instrument(level = "trace", skip_all, fields(ioprio), ret)]
pub fn proc_set_ioprio(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ioprio: i32,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    ctx.data().process.set_ioprio(ioprio);
    Ok(Errno::Success)
}

/// ### `proc_get_ioprio()`
/// Returns guest-visible Linux I/O priority metadata.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_get_ioprio<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ret_ioprio: WasmPtr<i32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    let env = ctx.data();
    let ioprio = env.process.ioprio();
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(ret_ioprio.write(&memory, ioprio));
    Ok(Errno::Success)
}
