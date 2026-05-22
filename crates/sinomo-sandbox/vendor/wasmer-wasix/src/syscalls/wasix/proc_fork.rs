use super::*;
use crate::{
    WasiThreadHandle, WasiVForkAsyncify, capture_store_snapshot,
    os::task::{OwnedTaskStatus, TaskStatus, thread::WasiMemoryLayout},
    runtime::task_manager::{TaskWasm, TaskWasmRunProperties},
    state::{WasiMemoryProtection, WasiMemoryProtectionRegion, context_switching::ContextSwitchingEnvironment},
    syscalls::*,
};
use serde::{Deserialize, Serialize};
use wasmer::{FromToNativeWasmType, Memory};

#[derive(Serialize, Deserialize)]
pub(crate) struct ForkResult {
    pub pid: Pid,
    pub ret: Errno,
}

struct ForkOutcome {
    pid: Pid,
    ret: Errno,
}

macro_rules! sinomo_debug {
    ($($arg:tt)*) => {
        if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
            eprintln!($($arg)*);
        }
    };
}

macro_rules! sinomo_mutex_debug {
    ($($arg:tt)*) => {
        if std::env::var_os("SINOMO_WASIX_MUTEX_DEBUG").is_some() {
            eprintln!($($arg)*);
        }
    };
}

macro_rules! sinomo_cond_debug {
    ($($arg:tt)*) => {
        if std::env::var_os("SINOMO_WASIX_COND_DEBUG").is_some() {
            eprintln!($($arg)*);
        }
    };
}

macro_rules! sinomo_shm_debug {
    ($($arg:tt)*) => {
        if std::env::var_os("SINOMO_WASIX_SHM_DEBUG").is_some() {
            eprintln!($($arg)*);
        }
    };
}

fn guest_errno_i32(errno: Errno) -> i32 {
    match errno {
        Errno::Success => 0,
        Errno::Again => 6,
        Errno::Busy => 10,
        Errno::Canceled => 11,
        Errno::Deadlk => 16,
        Errno::Intr => 27,
        Errno::Inval => 28,
        Errno::Perm => 63,
        Errno::Timedout => 73,
        other => other.to_native() as i32,
    }
}

fn debug_shared_mapping_words(env: &WasiEnv, offset: u64, label: &str) {
    if std::env::var_os("SINOMO_WASIX_SHM_DEBUG").is_none() {
        return;
    }
    let Some(mapping) = env
        .state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .find(|mapping| offset >= mapping.offset && offset < mapping.offset.saturating_add(mapping.len))
    else {
        return;
    };
    let Ok(bytes) = mapping.read_bytes() else {
        return;
    };
    let read_i32 = |relative: usize| -> Option<i32> {
        let end = relative.checked_add(4)?;
        if end > bytes.len() {
            return None;
        }
        Some(i32::from_le_bytes([
            bytes[relative],
            bytes[relative + 1],
            bytes[relative + 2],
            bytes[relative + 3],
        ]))
    };
    let owner_index = usize::try_from(offset.saturating_sub(mapping.offset).saturating_add(4)).ok();
    sinomo_shm_debug!(
        "sinomo-debug shared_map_state label={} pid={} mutex_offset={:#x} count1={:?} count2={:?} predicate1={:?} predicate2={:?} mutex_owner={:?}",
        label,
        env.pid().raw(),
        offset,
        read_i32(0),
        read_i32(4),
        read_i32(104),
        read_i32(108),
        owner_index.and_then(read_i32)
    );
}

#[derive(Serialize, Deserialize)]
struct CloneThreadResult {
    tid: Tid,
    ret: Errno,
}

/// ### `proc_fork()`
/// Forks the current process into a new subprocess. If the function
/// returns a zero then its the new subprocess. If it returns a positive
/// number then its the current process and the $pid represents the child.
#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn proc_fork<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    copy_memory: Bool,
    pid_ptr: WasmPtr<Pid, M>,
) -> Result<Errno, WasiError> {
    let outcome = proc_fork_common(ctx, copy_memory, Some(pid_ptr))?;
    Ok(outcome.ret)
}

/// ### `fork()`
/// POSIX-shaped fork wrapper. Returns 0 in the child, the child pid in the
/// parent, or a negative errno value on failure.
#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn fork<M: MemorySize>(ctx: FunctionEnvMut<'_, WasiEnv>) -> Result<i32, WasiError> {
    let outcome = proc_fork_common::<M>(ctx, Bool::True, None)?;
    Ok(fork_outcome_to_posix(outcome))
}

/// ### `vfork()`
/// POSIX-shaped vfork wrapper over the existing WASIX lightweight fork path.
#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn vfork<M: MemorySize>(ctx: FunctionEnvMut<'_, WasiEnv>) -> Result<i32, WasiError> {
    let outcome = proc_fork_common::<M>(ctx, Bool::False, None)?;
    Ok(fork_outcome_to_posix(outcome))
}

/// Runtime hint used by the POSIX sysroot `mmap()` implementation when a
/// process creates a shared mapping. The runtime keeps normal fork memory
/// private while synchronizing these registered guest ranges across related
/// processes.
#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn mmap_shared_hint<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
) -> Result<Errno, WasiError> {
    mmap_shared_hint_inner::<M>(ctx, offset, len, None, 0)
}

pub fn mmap_shared_file_hint<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: i32,
    file_offset: u64,
    offset: M::Offset,
    len: M::Offset,
) -> Result<Errno, WasiError> {
    mmap_shared_hint_inner::<M>(ctx, offset, len, Some(fd), file_offset)
}

pub fn mmap_protect_none<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
) -> Result<Errno, WasiError> {
    mmap_protect::<M>(ctx, offset, len, region::Protection::NONE)
}

pub fn mmap_protect_read_write<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
) -> Result<Errno, WasiError> {
    mmap_protect::<M>(ctx, offset, len, region::Protection::READ_WRITE)
}

pub fn mmap_protect_read<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
) -> Result<Errno, WasiError> {
    mmap_protect::<M>(ctx, offset, len, region::Protection::READ)
}

pub fn memory_load_i32<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    addr: u32,
    offset: u32,
    bytes: u32,
    flags: u32,
) -> Result<i32, WasiError> {
    check_memory_access::<M>(&mut ctx, addr, offset, bytes, false)?;
    let access = memory_access_bounds(addr, offset, bytes).ok_or_else(memory_access_sigsegv(&mut ctx))?;
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    if !memory_access_in_memory(&memory, access) {
        memory_access_debug(ctx.data(), "load_i32_oob", addr, offset, bytes, false, Some(access));
        drop(memory);
        return Err(memory_access_sigsegv(&mut ctx)());
    }
    let data = unsafe { memory.data_unchecked() };
    let raw = read_guest_integer(data, access.0, access.1).expect("checked guest memory bounds");
    Ok(sign_extend(raw, bytes, flags != 0) as i32)
}

pub fn memory_load_i64<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    addr: u32,
    offset: u32,
    bytes: u32,
    flags: u32,
) -> Result<i64, WasiError> {
    check_memory_access::<M>(&mut ctx, addr, offset, bytes, false)?;
    let access = memory_access_bounds(addr, offset, bytes).ok_or_else(memory_access_sigsegv(&mut ctx))?;
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    if !memory_access_in_memory(&memory, access) {
        memory_access_debug(ctx.data(), "load_i64_oob", addr, offset, bytes, false, Some(access));
        drop(memory);
        return Err(memory_access_sigsegv(&mut ctx)());
    }
    let data = unsafe { memory.data_unchecked() };
    let raw = read_guest_integer(data, access.0, access.1).expect("checked guest memory bounds");
    Ok(sign_extend(raw, bytes, flags != 0) as i64)
}

pub fn memory_load_f32<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    addr: u32,
    offset: u32,
) -> Result<f32, WasiError> {
    check_memory_access::<M>(&mut ctx, addr, offset, 4, false)?;
    let access = memory_access_bounds(addr, offset, 4).ok_or_else(memory_access_sigsegv(&mut ctx))?;
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    if !memory_access_in_memory(&memory, access) {
        memory_access_debug(ctx.data(), "load_f32_oob", addr, offset, 4, false, Some(access));
        drop(memory);
        return Err(memory_access_sigsegv(&mut ctx)());
    }
    let data = unsafe { memory.data_unchecked() };
    let raw = read_guest_integer(data, access.0, access.1).expect("checked guest memory bounds");
    Ok(f32::from_bits(raw as u32))
}

pub fn memory_load_f64<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    addr: u32,
    offset: u32,
) -> Result<f64, WasiError> {
    check_memory_access::<M>(&mut ctx, addr, offset, 8, false)?;
    let access = memory_access_bounds(addr, offset, 8).ok_or_else(memory_access_sigsegv(&mut ctx))?;
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    if !memory_access_in_memory(&memory, access) {
        memory_access_debug(ctx.data(), "load_f64_oob", addr, offset, 8, false, Some(access));
        drop(memory);
        return Err(memory_access_sigsegv(&mut ctx)());
    }
    let data = unsafe { memory.data_unchecked() };
    let raw = read_guest_integer(data, access.0, access.1).expect("checked guest memory bounds");
    Ok(f64::from_bits(raw))
}

pub fn memory_store_i32<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    addr: u32,
    value: u32,
    offset: u32,
    bytes: u32,
) -> Result<(), WasiError> {
    memory_store_integer::<M>(&mut ctx, addr, value as u64, offset, bytes)
}

pub fn memory_store_i64<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    addr: u32,
    value: u64,
    offset: u32,
    bytes: u32,
) -> Result<(), WasiError> {
    memory_store_integer::<M>(&mut ctx, addr, value, offset, bytes)
}

pub fn memory_store_f32<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    addr: u32,
    value: f32,
    offset: u32,
) -> Result<(), WasiError> {
    memory_store_integer::<M>(&mut ctx, addr, value.to_bits() as u64, offset, 4)
}

pub fn memory_store_f64<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    addr: u32,
    value: f64,
    offset: u32,
) -> Result<(), WasiError> {
    memory_store_integer::<M>(&mut ctx, addr, value.to_bits(), offset, 8)
}

fn memory_store_integer<M: MemorySize>(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    addr: u32,
    value: u64,
    offset: u32,
    bytes: u32,
) -> Result<(), WasiError> {
    check_memory_access::<M>(ctx, addr, offset, bytes, true)?;
    let access = memory_access_bounds(addr, offset, bytes).ok_or_else(memory_access_sigsegv(ctx))?;
    let memory = unsafe { ctx.data().memory_view(ctx) };
    if !memory_access_in_memory(&memory, access) {
        memory_access_debug(ctx.data(), "store_oob", addr, offset, bytes, true, Some(access));
        drop(memory);
        return Err(memory_access_sigsegv(ctx)());
    }
    let data = unsafe { memory.data_unchecked_mut() };
    write_guest_integer(data, access.0, access.1, value).expect("checked guest memory bounds");
    Ok(())
}

fn check_memory_access<M: MemorySize>(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
    addr: u32,
    offset: u32,
    bytes: u32,
    write: bool,
) -> Result<(), WasiError> {
    let Some((start, len)) = memory_access_bounds(addr, offset, bytes) else {
        memory_access_debug(ctx.data(), "bounds_overflow", addr, offset, bytes, write, None);
        return Err(memory_access_sigsegv(ctx)());
    };
    if !ctx
        .data()
        .state
        .memory_access_allowed(ctx.data().pid().raw(), start as u64, len as u64, write)
    {
        memory_access_debug(ctx.data(), "protection_denied", addr, offset, bytes, write, Some((start, len)));
        return Err(memory_access_sigsegv(ctx)());
    }
    Ok(())
}

fn memory_access_bounds(addr: u32, offset: u32, bytes: u32) -> Option<(usize, usize)> {
    let start = (addr as u64).checked_add(offset as u64)?;
    let len = usize::try_from(bytes).ok()?;
    usize::try_from(start).ok()?.checked_add(len)?;
    Some((usize::try_from(start).ok()?, len))
}

fn memory_access_in_memory(memory: &MemoryView, access: (usize, usize)) -> bool {
    let Ok(memory_size) = usize::try_from(memory.data_size()) else {
        return false;
    };
    access
        .0
        .checked_add(access.1)
        .is_some_and(|end| end <= memory_size)
}

fn memory_access_debug(
    env: &WasiEnv,
    reason: &str,
    addr: u32,
    offset: u32,
    bytes: u32,
    write: bool,
    access: Option<(usize, usize)>,
) {
    if std::env::var_os("SINOMO_WASIX_DEBUG").is_none() {
        return;
    }
    eprintln!(
        "sinomo-debug memory_access_sigsegv pid={} tid={} reason={} addr={:#x} offset={} bytes={} write={} access={:?} protections={}",
        env.pid().raw(),
        env.tid().raw(),
        reason,
        addr,
        offset,
        bytes,
        write,
        access,
        env.state.memory_protections_for_pid_range(env.pid().raw(), 0, u64::MAX).len()
    );
}

fn memory_access_sigsegv(ctx: &mut FunctionEnvMut<'_, WasiEnv>) -> impl FnOnce() -> WasiError {
    let exit_code = ctx.data().thread.set_or_get_exit_code_for_signal(Signal::Sigsegv);
    move || WasiError::Exit(exit_code)
}

fn read_guest_integer(data: &[u8], start: usize, len: usize) -> Option<u64> {
    if len == 0 || len > 8 {
        return None;
    }
    let bytes = data.get(start..start.checked_add(len)?)?;
    let mut raw = 0u64;
    for (index, byte) in bytes.iter().enumerate() {
        raw |= (*byte as u64) << (index * 8);
    }
    Some(raw)
}

fn write_guest_integer(data: &mut [u8], start: usize, len: usize, value: u64) -> Option<()> {
    if len == 0 || len > 8 {
        return None;
    }
    let bytes = data.get_mut(start..start.checked_add(len)?)?;
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = ((value >> (index * 8)) & 0xff) as u8;
    }
    Some(())
}

fn sign_extend(value: u64, bytes: u32, signed: bool) -> u64 {
    if !signed || bytes == 0 || bytes >= 8 {
        return value;
    }
    let bits = bytes * 8;
    let shift = 64 - bits;
    (((value << shift) as i64) >> shift) as u64
}

fn mmap_protect<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
    protection: region::Protection,
) -> Result<Errno, WasiError> {
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let offset_u64 = offset.into();
    let len_u64 = len.into();
    if len_u64 == 0 {
        return Ok(Errno::Inval);
    }
    let memory_size = memory.data_size();
    let end = match offset_u64.checked_add(len_u64) {
        Some(end) => end,
        None => return Ok(Errno::Inval),
    };
    if end > memory_size {
        return Ok(Errno::Fault);
    }

    let host_page_size = region::page::size() as u64;
    if host_page_size == 0
        || offset_u64 % host_page_size != 0
        || len_u64 % host_page_size != 0
    {
        return Ok(Errno::Inval);
    }

    let offset_usize = match usize::try_from(offset_u64) {
        Ok(value) => value,
        Err(_) => return Ok(Errno::Overflow),
    };
    let len_usize = match usize::try_from(len_u64) {
        Ok(value) => value,
        Err(_) => return Ok(Errno::Overflow),
    };
    let ptr = unsafe { memory.data_ptr().add(offset_usize) };
    sinomo_debug!(
        "sinomo-debug mmap_protect pid={} offset={:#x} len={} protection={:?} ptr={:p}",
        env.pid().raw(),
        offset_u64,
        len_u64,
        protection,
        ptr
    );
    match unsafe { region::protect(ptr, len_usize, protection) } {
        Ok(()) => {
            if std::env::var_os("SINOMO_WASIX_DEBUG").is_some() {
                match region::query(ptr) {
                    Ok(region) => eprintln!(
                        "sinomo-debug mmap_protect_query pid={} ptr={:p} protection={:?} len={}",
                        env.pid().raw(),
                        ptr,
                        region.protection(),
                        region.len()
                    ),
                    Err(err) => eprintln!(
                        "sinomo-debug mmap_protect_query_failed pid={} ptr={:p} error={}",
                        env.pid().raw(),
                        ptr,
                        err
                    ),
                }
                probe_host_write_after_protect(env.pid().raw(), ptr, protection);
            }
            env.state.record_memory_protection(
                env.pid().raw(),
                offset_u64,
                len_u64,
                wasi_memory_protection(protection),
            );
            Ok(Errno::Success)
        }
        Err(err) => {
            tracing::warn!(
                pid=%env.pid(),
                offset=offset_u64,
                len=len_u64,
                ?protection,
                error=%err,
                "failed to update mmap guest page protection",
            );
            Ok(Errno::Fault)
        }
    }
}

fn mmap_shared_hint_inner<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
    fd: Option<i32>,
    file_offset: u64,
) -> Result<Errno, WasiError> {
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let offset_u64 = offset.into();
    let len_u64 = len.into();
    let existing_mapping = env
        .state
        .shared_memory_mappings()
        .into_iter()
        .any(|mapping| {
            (offset_u64 >= mapping.offset
                && offset_u64.saturating_add(len_u64) <= mapping.offset.saturating_add(mapping.len))
                || (fd.is_some()
                    && mapping.fd == fd
                    && mapping.file_offset == file_offset
                    && mapping.len == len_u64)
        });
    if existing_mapping && fd.is_none() {
        return map_registered_shared_memory::<M>(env, &memory, offset_u64, len_u64);
    }
    let bytes = wasi_try_mem_ok!(WasmPtr::<u8, M>::new(offset).slice(&memory, len))
        .read_to_vec()
        .map_err(mem_error_to_wasi)
        .map_err(|err| WasiError::Exit(err.into()))?;
    if let Some(fd) = fd {
        if let Err(err) = env.state.register_shared_memory_mapping_with_file(
            env.pid().raw(),
            offset_u64,
            len_u64,
            bytes,
            fd,
            file_offset,
        ) {
            return Ok(err);
        }
    } else {
        if let Err(err) =
            env.state
                .register_shared_memory_mapping(env.pid().raw(), offset_u64, len_u64, bytes)
        {
            return Ok(err);
        }
    }
    if existing_mapping {
        return map_registered_shared_memory::<M>(env, &memory, offset_u64, len_u64);
    }
    map_registered_shared_memory::<M>(env, &memory, offset_u64, len_u64)
}

fn map_registered_shared_memory<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
    len: u64,
) -> Result<Errno, WasiError> {
    let Some(mapping) = env
        .state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .find(|mapping| mapping.offset == offset && mapping.len == len)
    else {
        return Ok(Errno::Fault);
    };
    match map_shared_memory_backing(env, memory, &mapping) {
        Ok(()) => Ok(Errno::Success),
        Err(err) => Ok(err),
    }
}

pub(crate) fn map_shared_memory_backing(
    env: &WasiEnv,
    memory: &MemoryView,
    mapping: &crate::state::WasiSharedMemoryMapping,
) -> Result<(), Errno> {
    let page_size = u64::try_from(crate::state::host_page_size().map_err(|err| {
        if crate::state::is_unsupported_error(&err) {
            Errno::Notsup
        } else {
            Errno::Io
        }
    })?)
    .map_err(|_| Errno::Overflow)?;
    if mapping.offset % page_size != 0 || mapping.len == 0 || mapping.len % page_size != 0 {
        return Err(Errno::Inval);
    }
    let memory_size = memory.data_size();
    if mapping.offset.saturating_add(mapping.len) > memory_size {
        return Err(Errno::Fault);
    }
    if !env.state.has_cross_process_shared_memory() && mapping.fd.is_none() {
        return Ok(());
    }
    let len = usize::try_from(mapping.len).map_err(|_| Errno::Overflow)?;
    let base = unsafe { memory.data_ptr().add(mapping.offset as usize) };
    let mapped_key = (env.pid().raw(), env.tid().raw(), base as usize);
    {
        let mut mapped_bases = mapping.mapped_bases.lock().unwrap();
        if !mapped_bases.insert(mapped_key) {
            return Ok(());
        }
    }
    if let Err(err) = mapping.backing.map_at(base, len) {
        mapping.mapped_bases.lock().unwrap().remove(&mapped_key);
        return if crate::state::is_unsupported_error(&err) {
            Err(Errno::Notsup)
        } else {
            Err(Errno::Io)
        };
    }
    apply_recorded_memory_protections(env, base, mapping)?;
    sinomo_debug!(
        "sinomo-debug shared-mapping-register pid={} tid={} base={:p} offset=0x{:x} len={}",
        env.pid().raw(),
        env.tid().raw(),
        base,
        mapping.offset,
        mapping.len
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn probe_host_write_after_protect(pid: u32, ptr: *mut u8, protection: region::Protection) {
    if std::env::var_os("SINOMO_WASIX_PROTECT_PROBE").is_none() {
        return;
    }
    let child = unsafe { libc::fork() };
    if child < 0 {
        eprintln!("sinomo-debug mmap_protect_probe fork_failed pid={}", pid);
        return;
    }
    if child == 0 {
        unsafe {
            std::ptr::write_volatile(ptr, std::ptr::read_volatile(ptr));
            libc::_exit(0);
        }
    }
    let mut status = 0;
    let waited = unsafe { libc::waitpid(child, &mut status, 0) };
    if waited < 0 {
        eprintln!("sinomo-debug mmap_protect_probe wait_failed pid={}", pid);
        return;
    }
    eprintln!(
        "sinomo-debug mmap_protect_probe pid={} protection={:?} status={}",
        pid, protection, status
    );
}

#[cfg(not(target_os = "linux"))]
fn probe_host_write_after_protect(_pid: u32, _ptr: *mut u8, _protection: region::Protection) {}

fn wasi_memory_protection(protection: region::Protection) -> WasiMemoryProtection {
    match protection {
        region::Protection::NONE => WasiMemoryProtection::None,
        region::Protection::READ => WasiMemoryProtection::Read,
        _ => WasiMemoryProtection::ReadWrite,
    }
}

fn host_memory_protection(protection: WasiMemoryProtection) -> region::Protection {
    match protection {
        WasiMemoryProtection::None => region::Protection::NONE,
        WasiMemoryProtection::Read => region::Protection::READ,
        WasiMemoryProtection::ReadWrite => region::Protection::READ_WRITE,
    }
}

fn apply_recorded_memory_protections(
    env: &WasiEnv,
    base: *mut u8,
    mapping: &crate::state::WasiSharedMemoryMapping,
) -> Result<(), Errno> {
    for protection in env.state.memory_protections_for_pid_range(
        env.pid().raw(),
        mapping.offset,
        mapping.len,
    ) {
        apply_recorded_memory_protection(base, mapping, protection)?;
    }
    Ok(())
}

fn apply_recorded_memory_protection(
    base: *mut u8,
    mapping: &crate::state::WasiSharedMemoryMapping,
    protection: WasiMemoryProtectionRegion,
) -> Result<(), Errno> {
    let mapping_end = mapping.offset.saturating_add(mapping.len);
    let protection_end = protection.offset.saturating_add(protection.len);
    let start = protection.offset.max(mapping.offset);
    let end = protection_end.min(mapping_end);
    if start >= end {
        return Ok(());
    }
    let relative = usize::try_from(start - mapping.offset).map_err(|_| Errno::Overflow)?;
    let len = usize::try_from(end - start).map_err(|_| Errno::Overflow)?;
    let ptr = unsafe { base.add(relative) };
    unsafe { region::protect(ptr, len, host_memory_protection(protection.protection)) }
        .map_err(|_| Errno::Fault)
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_futex_hint<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
) -> Result<i32, WasiError> {
    let offset = offset.into();
    let len = len.into();
    ctx.data().state.register_shared_futex_range(offset, len);
    ctx.data()
        .state
        .shared_mutex_owners
        .lock()
        .unwrap()
        .remove(&offset);
    ctx.data()
        .state
        .shared_mutex_owner_pids
        .lock()
        .unwrap()
        .remove(&offset);
    ctx.data()
        .state
        .shared_mutex_depths
        .lock()
        .unwrap()
        .remove(&offset);
    ctx.data()
        .state
        .shared_cond_sequences
        .lock()
        .unwrap()
        .remove(&offset);
    ctx.data()
        .state
        .shared_semaphores
        .lock()
        .unwrap()
        .remove(&offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    match sync_shared_memory_range_from_guest::<M>(ctx.data(), &memory, offset, len) {
        Ok(()) => Ok(0),
        Err(err) => Ok(guest_errno_i32(err)),
    }
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_futex_unhint<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
) -> Result<i32, WasiError> {
    ctx.data()
        .state
        .unregister_shared_futex_range(offset.into(), len.into());
    Ok(0)
}

pub fn shared_memory_is_cross_process(ctx: FunctionEnvMut<'_, WasiEnv>) -> u32 {
    u32::from(has_live_cross_process_shared_memory(ctx.data()))
}

pub fn shared_memory_was_cross_process(ctx: FunctionEnvMut<'_, WasiEnv>) -> u32 {
    u32::from(ctx.data().state.has_cross_process_shared_memory())
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_memory_sync_from<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
) -> Result<i32, WasiError> {
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    match sync_shared_memory_from_guest::<M>(env, &memory) {
        Ok(()) => Ok(0),
        Err(err) => Ok(guest_errno_i32(err)),
    }
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_memory_sync_from_preserving_futexes<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
) -> Result<i32, WasiError> {
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let preserve_ranges = env
        .state
        .shared_futex_ranges()
        .into_iter()
        .map(|range| (range.start, range.end.saturating_sub(range.start) as usize))
        .collect::<Vec<_>>();
    match sync_shared_memory_from_guest_preserving_ranges::<M>(env, &memory, &preserve_ranges) {
        Ok(()) => Ok(0),
        Err(err) => Ok(guest_errno_i32(err)),
    }
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_memory_sync_to<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
) -> Result<i32, WasiError> {
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    match sync_shared_memory_to_guest::<M>(env, &memory) {
        Ok(()) => Ok(0),
        Err(err) => Ok(guest_errno_i32(err)),
    }
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_memory_range_sync_from<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
) -> Result<i32, WasiError> {
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    match sync_shared_memory_range_from_guest::<M>(env, &memory, offset.into(), len.into()) {
        Ok(()) => Ok(0),
        Err(err) => Ok(guest_errno_i32(err)),
    }
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_memory_range_sync_to<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    len: M::Offset,
) -> Result<i32, WasiError> {
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    match sync_shared_memory_range_to_guest::<M>(env, &memory, offset.into(), len.into()) {
        Ok(()) => Ok(0),
        Err(err) => Ok(guest_errno_i32(err)),
    }
}

pub fn shared_cond_sequence<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
) -> Result<u32, WasiError> {
    let offset = offset.into();
    if let Some(seq) = ctx
        .data()
        .state
        .shared_cond_sequences
        .lock()
        .unwrap()
        .get(&offset)
        .copied()
    {
        let memory = unsafe { ctx.data().memory_view(&ctx) };
        let _ = with_shared_cond_sequence_word::<M>(ctx.data(), &memory, offset, seq);
        return Ok(seq);
    }

    if let Some(seq) =
        shared_cond_sequence_word(ctx.data(), offset).map_err(|err| WasiError::Exit(err.into()))?
    {
        ctx.data()
            .state
            .shared_cond_sequences
            .lock()
            .unwrap()
            .insert(offset, seq);
        return Ok(seq);
    }

    let seq = ctx
        .data()
        .state
        .shared_cond_sequences
        .lock()
        .unwrap()
        .get(&offset)
        .copied()
        .unwrap_or(0);
    if seq == 0 {
        sinomo_debug!(
            "sinomo-debug shared_cond_sequence zero pid={} offset={:#x}",
            ctx.data().pid().raw(),
            offset
        );
    }
    Ok(seq)
}

pub fn shared_cond_broadcast<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
) -> Result<u32, WasiError> {
    let offset = offset.into();
    let seq = {
        let mut sequences = ctx.data().state.shared_cond_sequences.lock().unwrap();
        let seq = sequences.get(&offset).copied().unwrap_or(0).wrapping_add(1);
        sequences.insert(offset, seq);
        seq
    };
    sinomo_debug!(
        "sinomo-debug shared_cond_broadcast pid={} offset={:#x} seq={}",
        ctx.data().pid().raw(),
        offset,
        seq
    );
    sinomo_cond_debug!(
        "sinomo-debug shared_cond_broadcast pid={} offset={:#x} seq={}",
        ctx.data().pid().raw(),
        offset,
        seq
    );
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let _ = with_shared_cond_sequence_word::<M>(ctx.data(), &memory, offset, seq);
    if let Some(wait) = ctx
        .data()
        .state
        .shared_cond_waits
        .lock()
        .unwrap()
        .get(&offset)
        .cloned()
    {
        wait.waiters.notify_all();
    }
    Ok(seq)
}

pub fn shared_cond_wait<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    expected_seq: u32,
    timeout_ns: i64,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;
    let offset = offset.into();
    let tid = ctx.data().tid().raw();
    let wait = {
        let mut waits = ctx.data().state.shared_cond_waits.lock().unwrap();
        waits
            .entry(offset)
            .or_insert_with(|| {
                std::sync::Arc::new(crate::state::WasiSharedCondWait {
                    state: Default::default(),
                    waiters: std::sync::Condvar::new(),
                })
            })
            .clone()
    };
    let max_wait = if timeout_ns < 0 {
        None
    } else {
        Some(std::time::Duration::from_nanos(timeout_ns as u64))
    };
    let started = std::time::Instant::now();
    let mut guard = wait.state.lock().unwrap();
    guard.interrupted_tids.remove(&tid);
    guard.waiters_by_tid.insert(tid);
    sinomo_cond_debug!(
        "sinomo-debug shared_cond_wait_enter pid={} tid={} offset={:#x} expected={} timeout_ns={}",
        ctx.data().pid().raw(),
        tid,
        offset,
        expected_seq,
        timeout_ns
    );
    loop {
        if guard.interrupted_tids.remove(&tid) {
            guard.waiters_by_tid.remove(&tid);
            drop(guard);
            WasiEnv::do_pending_operations(&mut ctx)?;
            sinomo_debug!(
                "sinomo-debug shared_cond_wait_interrupted pid={} tid={} offset={:#x}",
                ctx.data().pid().raw(),
                tid,
                offset
            );
            return Ok(Errno::Intr.to_native() as i32);
        }
        let seq = ctx
            .data()
            .state
            .shared_cond_sequences
            .lock()
            .unwrap()
            .get(&offset)
            .copied()
            .unwrap_or(0);
        if seq != expected_seq {
            sinomo_cond_debug!(
                "sinomo-debug shared_cond_wait_leave pid={} offset={:#x} expected={} seq={} result=success",
                ctx.data().pid().raw(),
                offset,
                expected_seq,
                seq
            );
            guard.waiters_by_tid.remove(&tid);
            drop(guard);
            WasiEnv::do_pending_operations(&mut ctx)?;
            return Ok(0);
        }
        if let Some(max_wait) = max_wait {
            let elapsed = started.elapsed();
            if elapsed >= max_wait {
                sinomo_cond_debug!(
                    "sinomo-debug shared_cond_wait_leave pid={} offset={:#x} expected={} seq={} result=timeout-poll",
                    ctx.data().pid().raw(),
                    offset,
                    expected_seq,
                    seq
                );
                guard.waiters_by_tid.remove(&tid);
                drop(guard);
                WasiEnv::do_pending_operations(&mut ctx)?;
                return Ok(guest_errno_i32(Errno::Timedout));
            }
            let remaining = max_wait.saturating_sub(elapsed);
            let (next_guard, result) = wait.waiters.wait_timeout(guard, remaining).unwrap();
            guard = next_guard;
            if result.timed_out() {
                let seq = ctx
                    .data()
                    .state
                    .shared_cond_sequences
                    .lock()
                    .unwrap()
                    .get(&offset)
                    .copied()
                    .unwrap_or(0);
                sinomo_cond_debug!(
                    "sinomo-debug shared_cond_wait_leave pid={} offset={:#x} expected={} seq={} result=timeout-poll",
                    ctx.data().pid().raw(),
                    offset,
                    expected_seq,
                    seq
                );
                guard.waiters_by_tid.remove(&tid);
                drop(guard);
                WasiEnv::do_pending_operations(&mut ctx)?;
                return Ok(guest_errno_i32(Errno::Timedout));
            }
        } else {
            guard = wait.waiters.wait(guard).unwrap();
        }
    }
}

pub(crate) fn wake_shared_cond_waiters_for_thread(state: &crate::state::WasiState, tid: u32) -> u32 {
    let waits = state
        .shared_cond_waits
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let mut woken = 0u32;
    for wait in waits {
        let mut guard = wait.state.lock().unwrap();
        if guard.waiters_by_tid.contains(&tid) {
            guard.interrupted_tids.insert(tid);
            woken = woken.saturating_add(1);
            wait.waiters.notify_all();
        }
    }
    if woken > 0 {
        sinomo_debug!(
            "sinomo-debug shared_cond_signal_wake tid={} woken={}",
            tid,
            woken
        );
    }
    woken
}

pub fn shared_barrier_generation<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
) -> Result<u32, WasiError> {
    let offset = offset.into();
    let pid = ctx.data().pid().raw();
    let result = with_shared_u32_words(ctx.data(), offset, 5, |words| {
        sinomo_debug!(
            "sinomo-debug shared_barrier_generation pid={} offset={:#x} words={:?}",
            pid,
            offset,
            words
        );
        words[2]
    });
    Ok(result.unwrap_or(0))
}

pub fn shared_barrier_arrive<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    limit: u32,
) -> Result<i32, WasiError> {
    let offset = offset.into();
    let pid = ctx.data().pid().raw();
    if limit == 0 {
        return Ok(Errno::Inval.to_native() as i32);
    }
    let result = with_shared_u32_words_mut(ctx.data(), offset, 5, |words| {
        let arrived = words[1].saturating_add(1);
        let result = if arrived == limit {
            words[1] = 0;
            words[3] = limit.saturating_sub(1);
            words[2] = words[2].wrapping_add(1);
            -1
        } else if arrived > limit {
            Errno::Inval.to_native() as i32
        } else {
            words[1] = arrived;
            0
        };
        sinomo_debug!(
            "sinomo-debug shared_barrier_arrive pid={} offset={:#x} limit={} result={} words={:?}",
            pid,
            offset,
            limit,
            result,
            words
        );
        result
    });
    Ok(result.unwrap_or_else(|err| err.to_native() as i32))
}

pub fn shared_barrier_release<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
) -> Result<i32, WasiError> {
    let offset = offset.into();
    let pid = ctx.data().pid().raw();
    let result = with_shared_u32_words_mut(ctx.data(), offset, 5, |words| {
        let result = if words[3] == 0 {
            0
        } else {
            words[3] -= 1;
            words[3] as i32
        };
        sinomo_debug!(
            "sinomo-debug shared_barrier_release pid={} offset={:#x} result={} words={:?}",
            pid,
            offset,
            result,
            words
        );
        result
    });
    Ok(result.unwrap_or_else(|err| err.to_native() as i32))
}

pub fn shared_semaphore_getvalue<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let semaphore = shared_semaphore::<M>(ctx.data(), &memory, offset)?;
    let value = *semaphore.value.lock().unwrap();
    write_shared_semaphore_value::<M>(ctx.data(), &memory, offset, value)?;
    Ok(value.min(i32::MAX as u32) as i32)
}

pub fn shared_semaphore_post<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    max_value: u32,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let semaphore = shared_semaphore::<M>(ctx.data(), &memory, offset)?;
    let mut value = semaphore.value.lock().unwrap();
    if *value >= max_value {
        return Ok(Errno::Overflow.to_native() as i32);
    }
    *value = value.saturating_add(1);
    let new_value = *value;
    drop(value);
    write_shared_semaphore_value::<M>(ctx.data(), &memory, offset, new_value)?;
    semaphore.waiters.notify_one();
    Ok(0)
}

pub fn shared_semaphore_try_wait<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let semaphore = shared_semaphore::<M>(ctx.data(), &memory, offset)?;
    let mut value = semaphore.value.lock().unwrap();
    if *value == 0 {
        write_shared_semaphore_value::<M>(ctx.data(), &memory, offset, 0)?;
        return Ok(Errno::Again.to_native() as i32);
    }
    *value -= 1;
    let new_value = *value;
    drop(value);
    write_shared_semaphore_value::<M>(ctx.data(), &memory, offset, new_value)?;
    Ok(0)
}

pub fn shared_semaphore_wait<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    timeout_ns: i64,
) -> Result<i32, WasiError> {
    if process_pending_operations_interruptibly(&mut ctx)? {
        return Ok(guest_errno_i32(Errno::Intr));
    }
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let semaphore = shared_semaphore::<M>(ctx.data(), &memory, offset)?;
    let value = semaphore.value.lock().unwrap();
    if *value > 0 {
        return Ok(0);
    }
    let wait_duration = if timeout_ns < 0 {
        std::time::Duration::from_millis(10)
    } else {
        std::time::Duration::from_nanos(timeout_ns as u64).min(std::time::Duration::from_millis(10))
    };
    drop(semaphore.waiters.wait_timeout(value, wait_duration).unwrap());
    if process_pending_operations_interruptibly(&mut ctx)? {
        return Ok(guest_errno_i32(Errno::Intr));
    }
    Ok(0)
}

fn process_pending_operations_interruptibly(
    ctx: &mut FunctionEnvMut<'_, WasiEnv>,
) -> Result<bool, WasiError> {
    WasiEnv::do_pending_link_operations(ctx, true)?;
    match WasiEnv::process_signals_and_exit(ctx)? {
        Ok(processed) => Ok(processed),
        Err(err) => Err(WasiError::Exit(err.into())),
    }
}

pub fn shared_semaphore_destroy<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    ctx.data()
        .state
        .shared_semaphores
        .lock()
        .unwrap()
        .remove(&offset);
    Ok(0)
}

const SINOMO_RWLOCK_WORDS: usize = 6;
const SINOMO_RWLOCK_STATE_WORD: usize = 0;
const SINOMO_RWLOCK_WRITER_WAITERS_WORD: usize = 1;
const SINOMO_RWLOCK_WRITER_PRIORITY_WORD: usize = 4;
const SINOMO_RWLOCK_WRITER_POLICY_WORD: usize = 5;
const SINOMO_SCHED_FIFO: i32 = 1;
const SINOMO_SCHED_RR: i32 = 2;

pub fn shared_rwlock_try_rdlock<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    policy: i32,
    priority: i32,
    registered_waiter: u32,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    publish_shared_memory_around_rwlock::<M>(ctx.data(), &memory, offset)?;
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let mut state = lock.state.lock().unwrap();
    sinomo_debug!(
        "sinomo-debug shared_rwlock_try_rdlock pid={} offset={:#x} policy={} priority={} state readers={} writer={} waiters={} max_policy={} max_priority={}",
        ctx.data().pid().raw(),
        offset,
        policy,
        priority,
        state.readers,
        state.writer,
        state.writer_waiters,
        state.max_writer_policy,
        state.max_writer_priority
    );
    let blocks_reader = state.writer
        || (state.writer_waiters > 0
            && rwlock_policy_orders_readers(state.max_writer_policy)
            && rwlock_policy_orders_readers(policy)
            && priority <= state.max_writer_priority);
    if blocks_reader {
        write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
        return Ok(Errno::Busy.to_native() as i32);
    }
    if registered_waiter != 0 {
        rwlock_remove_reader_waiter(&mut state, policy, priority);
    }
    state.readers = state.readers.saturating_add(1);
    write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
    Ok(0)
}

pub fn shared_rwlock_try_wrlock<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    registered_waiter: u32,
    policy: i32,
    priority: i32,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    publish_shared_memory_around_rwlock::<M>(ctx.data(), &memory, offset)?;
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let owner = shared_mutex_owner_token(ctx.data());
    let mut state = lock.state.lock().unwrap();
    sinomo_debug!(
        "sinomo-debug shared_rwlock_try_wrlock pid={} offset={:#x} registered={} state readers={} writer={} waiters={}",
        ctx.data().pid().raw(),
        offset,
        registered_waiter,
        state.readers,
        state.writer,
        state.writer_waiters
    );
    if state.writer && state.writer_owner == owner {
        write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
        return Ok(Errno::Deadlk.to_native() as i32);
    }
    if registered_waiter != 0
        && state.writer_waiters > 0
        && rwlock_policy_orders_readers(state.max_writer_policy)
        && rwlock_policy_orders_readers(policy)
        && priority < state.max_writer_priority
    {
        write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
        return Ok(Errno::Busy.to_native() as i32);
    }
    if registered_waiter != 0
        && rwlock_policy_orders_readers(policy)
        && state.max_reader_priority > priority
    {
        write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
        return Ok(Errno::Busy.to_native() as i32);
    }
    if state.writer || state.readers > 0 {
        write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
        return Ok(Errno::Busy.to_native() as i32);
    }
    state.writer = true;
    state.writer_owner = owner;
    if registered_waiter != 0 && state.writer_waiters > 0 {
        rwlock_remove_writer_waiter(&mut state, policy, priority);
    }
    write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
    Ok(0)
}

pub fn shared_rwlock_register_writer<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    policy: i32,
    priority: i32,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    publish_shared_memory_around_rwlock::<M>(ctx.data(), &memory, offset)?;
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let mut state = lock.state.lock().unwrap();
    sinomo_debug!(
        "sinomo-debug shared_rwlock_register_writer pid={} offset={:#x} policy={} priority={} before waiters={} max_policy={} max_priority={}",
        ctx.data().pid().raw(),
        offset,
        policy,
        priority,
        state.writer_waiters,
        state.max_writer_policy,
        state.max_writer_priority
    );
    state.writer_waiters = state.writer_waiters.saturating_add(1);
    state.writer_waiter_priorities.push((policy, priority));
    if rwlock_policy_orders_readers(policy) && priority >= state.max_writer_priority {
        state.max_writer_priority = priority;
        state.max_writer_policy = policy;
    }
    write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
    Ok(0)
}

pub fn shared_rwlock_unregister_writer<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    policy: i32,
    priority: i32,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    publish_shared_memory_around_rwlock::<M>(ctx.data(), &memory, offset)?;
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let mut state = lock.state.lock().unwrap();
    sinomo_debug!(
        "sinomo-debug shared_rwlock_unlock pid={} offset={:#x} state readers={} writer={} waiters={}",
        ctx.data().pid().raw(),
        offset,
        state.readers,
        state.writer,
        state.writer_waiters
    );
    rwlock_remove_writer_waiter(&mut state, policy, priority);
    if state.writer_waiters == 0 {
        state.max_writer_priority = 0;
        state.max_writer_policy = 0;
    } else {
        rwlock_recompute_writer_priority(&mut state);
    }
    write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
    Ok(0)
}

pub fn shared_rwlock_register_reader<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    policy: i32,
    priority: i32,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    publish_shared_memory_around_rwlock::<M>(ctx.data(), &memory, offset)?;
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let mut state = lock.state.lock().unwrap();
    state.reader_waiter_priorities.push((policy, priority));
    if rwlock_policy_orders_readers(policy) && priority > state.max_reader_priority {
        state.max_reader_priority = priority;
    }
    write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
    Ok(0)
}

pub fn shared_rwlock_unregister_reader<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    policy: i32,
    priority: i32,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let mut state = lock.state.lock().unwrap();
    rwlock_remove_reader_waiter(&mut state, policy, priority);
    write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
    Ok(0)
}

pub fn shared_rwlock_unlock<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    publish_shared_memory_around_rwlock::<M>(ctx.data(), &memory, offset)?;
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let mut state = lock.state.lock().unwrap();
    if state.writer {
        state.writer = false;
        state.writer_owner = 0;
    } else if state.readers > 0 {
        state.readers -= 1;
    } else {
        return Ok(0);
    }
    write_shared_rwlock_words::<M>(ctx.data(), &memory, offset, &state)?;
    lock.waiters.notify_all();
    Ok(0)
}

pub fn shared_rwlock_wait<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    timeout_ns: i64,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let lock = shared_rwlock::<M>(ctx.data(), &memory, offset)?;
    let state = lock.state.lock().unwrap();
    let wait_duration = if timeout_ns < 0 {
        std::time::Duration::from_millis(10)
    } else {
        std::time::Duration::from_nanos(timeout_ns as u64).min(std::time::Duration::from_millis(10))
    };
    drop(lock.waiters.wait_timeout(state, wait_duration).unwrap());
    Ok(0)
}

pub fn shared_rwlock_destroy<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
) -> Result<i32, WasiError> {
    let offset = Into::<u64>::into(offset);
    let key = shared_rwlock_key(ctx.data(), offset);
    ctx.data().state.shared_rwlocks.lock().unwrap().remove(&key);
    Ok(0)
}

fn rwlock_policy_orders_readers(policy: i32) -> bool {
    policy == SINOMO_SCHED_FIFO || policy == SINOMO_SCHED_RR
}

fn rwlock_remove_writer_waiter(
    state: &mut crate::state::WasiSharedRwLockState,
    policy: i32,
    priority: i32,
) {
    if state.writer_waiters > 0 {
        state.writer_waiters -= 1;
    }
    if let Some(index) =
        state
            .writer_waiter_priorities
            .iter()
            .position(|(waiter_policy, waiter_priority)| {
                *waiter_policy == policy && *waiter_priority == priority
            })
    {
        state.writer_waiter_priorities.remove(index);
    } else {
        state.writer_waiter_priorities.pop();
    }
    rwlock_recompute_writer_priority(state);
}

fn rwlock_recompute_writer_priority(state: &mut crate::state::WasiSharedRwLockState) {
    state.max_writer_priority = 0;
    state.max_writer_policy = 0;
    for (policy, priority) in &state.writer_waiter_priorities {
        if rwlock_policy_orders_readers(*policy) && *priority >= state.max_writer_priority {
            state.max_writer_priority = *priority;
            state.max_writer_policy = *policy;
        }
    }
    state.writer_waiters = state.writer_waiter_priorities.len() as u32;
}

fn rwlock_remove_reader_waiter(
    state: &mut crate::state::WasiSharedRwLockState,
    policy: i32,
    priority: i32,
) {
    if let Some(index) =
        state
            .reader_waiter_priorities
            .iter()
            .position(|(waiter_policy, waiter_priority)| {
                *waiter_policy == policy && *waiter_priority == priority
            })
    {
        state.reader_waiter_priorities.remove(index);
    } else {
        state.reader_waiter_priorities.pop();
    }
    state.max_reader_priority = 0;
    for (policy, priority) in &state.reader_waiter_priorities {
        if rwlock_policy_orders_readers(*policy) && *priority > state.max_reader_priority {
            state.max_reader_priority = *priority;
        }
    }
}

fn publish_shared_memory_around_rwlock<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
) -> Result<(), WasiError> {
    if env.state.has_shared_memory_mappings() {
        sync_shared_memory_from_guest_preserving_range::<M>(
            env,
            memory,
            offset,
            SINOMO_RWLOCK_WORDS * 4,
        )
        .map_err(|err| WasiError::Exit(err.into()))?;
    }
    Ok(())
}

fn shared_rwlock<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
) -> Result<std::sync::Arc<crate::state::WasiSharedRwLock>, WasiError> {
    let key = shared_rwlock_key(env, offset);
    let mut locks = env.state.shared_rwlocks.lock().unwrap();
    sinomo_debug!(
        "sinomo-debug shared_rwlock lookup pid={} offset={:#x} key={:#x} existing={}",
        env.pid().raw(),
        offset,
        key,
        locks.contains_key(&key)
    );
    Ok(locks
        .entry(key)
        .or_insert_with(|| {
            let mut initial = crate::state::WasiSharedRwLockState::default();
            if let Ok(words) = read_rwlock_words::<M>(memory, offset) {
                let raw_state = words[SINOMO_RWLOCK_STATE_WORD] as i32;
                initial.readers = raw_state.max(0) as u32;
                initial.writer = raw_state == -1;
                initial.writer_owner = 0;
                initial.writer_waiters = words[SINOMO_RWLOCK_WRITER_WAITERS_WORD];
                initial.max_writer_priority = words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD] as i32;
                initial.max_writer_policy = words[SINOMO_RWLOCK_WRITER_POLICY_WORD] as i32;
            }
            std::sync::Arc::new(crate::state::WasiSharedRwLock {
                state: std::sync::Mutex::new(initial),
                waiters: std::sync::Condvar::new(),
            })
        })
        .clone())
}

fn shared_rwlock_key(env: &WasiEnv, offset: u64) -> u64 {
    env.state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .find_map(|mapping| {
            let len = (SINOMO_RWLOCK_WORDS * 4) as u64;
            if offset >= mapping.offset
                && offset.saturating_add(len) <= mapping.offset.saturating_add(mapping.len)
            {
                let base = std::sync::Arc::as_ptr(&mapping.bytes) as usize as u64;
                Some(base ^ ((offset - mapping.offset) << 32))
            } else {
                None
            }
        })
        .unwrap_or(offset)
}

fn read_rwlock_words<M: MemorySize>(
    memory: &MemoryView,
    offset: u64,
) -> Result<[u32; SINOMO_RWLOCK_WORDS], Errno> {
    let ptr = WasmPtr::<u32, M>::new(to_offset::<M>(
        offset.try_into().map_err(|_| Errno::Overflow)?,
    )?);
    let slice = ptr
        .slice(memory, to_offset::<M>(SINOMO_RWLOCK_WORDS)?)
        .map_err(mem_error_to_wasi)?;
    let mut words = [0u32; SINOMO_RWLOCK_WORDS];
    for (index, cell) in slice.iter().enumerate() {
        words[index] = cell.read().map_err(mem_error_to_wasi)?;
    }
    Ok(words)
}

fn write_shared_rwlock_words<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
    state: &crate::state::WasiSharedRwLockState,
) -> Result<(), WasiError> {
    let mut words = [0u32; SINOMO_RWLOCK_WORDS];
    words[SINOMO_RWLOCK_STATE_WORD] = if state.writer {
        (-1i32) as u32
    } else {
        state.readers
    };
    words[SINOMO_RWLOCK_WRITER_WAITERS_WORD] = state.writer_waiters;
    words[SINOMO_RWLOCK_WRITER_PRIORITY_WORD] = state.max_writer_priority as u32;
    words[SINOMO_RWLOCK_WRITER_POLICY_WORD] = state.max_writer_policy as u32;

    let ptr = WasmPtr::<u32, M>::new(
        to_offset::<M>(
            offset
                .try_into()
                .map_err(|_| WasiError::Exit(Errno::Overflow.into()))?,
        )
        .map_err(|err| WasiError::Exit(err.into()))?,
    );
    let slice = ptr
        .slice(
            memory,
            to_offset::<M>(SINOMO_RWLOCK_WORDS).map_err(|err| WasiError::Exit(err.into()))?,
        )
        .map_err(|err| WasiError::Exit(mem_error_to_wasi(err).into()))?;
    for (index, cell) in slice.iter().enumerate() {
        if index == 2 || index == 3 {
            continue;
        }
        cell.write(words[index])
            .map_err(|err| WasiError::Exit(mem_error_to_wasi(err).into()))?;
    }

    if let Some(mapping) = env
        .state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add((SINOMO_RWLOCK_WORDS * 4) as u64)
                    <= mapping.offset.saturating_add(mapping.len)
        })
    {
        let index = usize::try_from(offset - mapping.offset)
            .map_err(|_| WasiError::Exit(Errno::Overflow.into()))?;
        let byte_len = SINOMO_RWLOCK_WORDS * 4;
        let mut bytes = mapping
            .read_range(index, byte_len)
            .map_err(|err| WasiError::Exit(err.into()))?;
        for (word_index, word) in words.iter().enumerate() {
            if word_index == 2 || word_index == 3 {
                continue;
            }
            let start = word_index * 4;
            bytes[start..start + 4].copy_from_slice(&word.to_le_bytes());
        }
        mapping
            .write_synchronized_range(index, &bytes)
            .map_err(|err| WasiError::Exit(err.into()))?;
    }
    Ok(())
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_mutex_try_lock<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    mutex_type: u32,
    owner_tid: u32,
) -> Result<i32, WasiError> {
    let tid = if owner_tid == 0 {
        shared_mutex_owner_token(ctx.data())
    } else {
        owner_tid
    };
    let guest_offset = offset;
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let offset_u64 = Into::<u64>::into(guest_offset);
    let offset_key = shared_mutex_key(ctx.data(), offset_u64);
    let result = with_mutex_words::<M>(ctx.data(), &memory, offset_u64, |words| {
        let kind = mutex_type & 3;
        let mut owners = ctx.data().state.shared_mutex_owners.lock().unwrap();
        let mut owner_pids = ctx.data().state.shared_mutex_owner_pids.lock().unwrap();
        let mut depths = ctx.data().state.shared_mutex_depths.lock().unwrap();
        let owner = owners.get(&offset_key).copied().unwrap_or(0);
        words[1] = owner;
        words[3] = depths.get(&offset_key).copied().unwrap_or(0);
        if owner == 0 {
            owners.remove(&offset_key);
            owner_pids.remove(&offset_key);
            depths.remove(&offset_key);
        }
        if owner == tid {
            if kind == 1 {
                let depth = words[3].saturating_add(1);
                depths.insert(offset_key, depth);
                words[1] = tid;
                words[3] = depth;
                owner_pids.insert(offset_key, ctx.data().pid().raw());
                return Errno::Success;
            }
            if kind == 2 {
                return Errno::Deadlk;
            }
            return Errno::Busy;
        }
        if owner == 0 {
            depths.insert(offset_key, 0);
            owners.insert(offset_key, tid);
            words[1] = tid;
            words[3] = 0;
            owner_pids.insert(offset_key, ctx.data().pid().raw());
            sinomo_debug!(
                "sinomo-debug shared_mutex_try_lock success pid={} offset={:#x} kind={} tid={} words={:?}",
                ctx.data().pid().raw(),
                offset_key,
                kind,
                tid,
                words
            );
            return Errno::Success;
        }
        sinomo_debug!(
            "sinomo-debug shared_mutex_try_lock busy pid={} offset={:#x} kind={} tid={} owner={} words={:?}",
            ctx.data().pid().raw(),
            offset_key,
            kind,
            tid,
            owner,
            words
        );
        Errno::Busy
    });
    // A successful lock acquire must observe ordinary MAP_SHARED writes
    // published by the previous owner. Preserve registered futex objects while
    // pulling the rest of the mapping into the guest so mutex/cond words are
    // still controlled by their dedicated runtime paths.
    if result == Errno::Success
        && has_live_cross_process_shared_memory(ctx.data())
        && shared_mutex_has_mapping(ctx.data(), offset_u64)
    {
        let preserve_ranges = ctx
            .data()
            .state
            .shared_futex_ranges()
            .into_iter()
            .map(|range| (range.start, range.end.saturating_sub(range.start) as usize))
            .collect::<Vec<_>>();
        let _ = sync_shared_memory_to_guest_preserving_ranges::<M>(
            ctx.data(),
            &memory,
            &preserve_ranges,
        );
        debug_shared_mapping_words(ctx.data(), offset_u64, "mutex-acquire-after-pull");
    }
    let _ = copy_shared_mutex_to_guest::<M>(ctx.data(), &memory, guest_offset.into());
    Ok(guest_errno_i32(result))
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_mutex_unlock<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    mutex_type: u32,
    owner_tid: u32,
) -> Result<i32, WasiError> {
    let tid = if owner_tid == 0 {
        shared_mutex_owner_token(ctx.data())
    } else {
        owner_tid
    };
    let guest_offset = offset;
    let memory = unsafe { ctx.data().memory_view(&ctx) };
    let mutex_start = Into::<u64>::into(guest_offset);
    if has_live_cross_process_shared_memory(ctx.data())
        && shared_mutex_has_mapping(ctx.data(), mutex_start)
    {
        let preserve_ranges = ctx
            .data()
            .state
            .shared_futex_ranges()
            .into_iter()
            .map(|range| (range.start, range.end.saturating_sub(range.start) as usize))
            .collect::<Vec<_>>();
        let mut preserve_ranges = preserve_ranges;
        preserve_ranges.push((mutex_start, 16));
        if let Err(err) = sync_shared_memory_from_guest_preserving_ranges::<M>(
            ctx.data(),
            &memory,
            &preserve_ranges,
        ) {
            return Ok(guest_errno_i32(err));
        }
        debug_shared_mapping_words(ctx.data(), mutex_start, "mutex-unlock-after-publish");
    }
    let offset_key = shared_mutex_key(ctx.data(), mutex_start);
    let result = with_mutex_words::<M>(ctx.data(), &memory, mutex_start, |words| {
        let kind = mutex_type & 3;
        let mut owners = ctx.data().state.shared_mutex_owners.lock().unwrap();
        let mut owner_pids = ctx.data().state.shared_mutex_owner_pids.lock().unwrap();
        let mut depths = ctx.data().state.shared_mutex_depths.lock().unwrap();
        let owner = owners.get(&offset_key).copied().unwrap_or(0);
        words[1] = owner;
        words[3] = depths.get(&offset_key).copied().unwrap_or(0);
        if owner == 0 {
            owners.remove(&offset_key);
            owner_pids.remove(&offset_key);
            depths.remove(&offset_key);
        }
        if owner != tid {
            sinomo_debug!(
                "sinomo-debug shared_mutex_unlock EPERM pid={} offset={:#x} kind={} owner={} tid={} words={:?}",
                ctx.data().pid().raw(),
                offset_key,
                kind,
                owner,
                tid,
                words
            );
            return Errno::Perm;
        }
        let depth = words[3];
        if kind == 1 && depth > 0 {
            let new_depth = depth - 1;
            words[1] = tid;
            words[3] = new_depth;
            owners.insert(offset_key, tid);
            owner_pids.insert(offset_key, ctx.data().pid().raw());
            depths.insert(offset_key, new_depth);
        } else {
            if kind == 1 || kind == 2 {
                sinomo_debug!(
                    "sinomo-debug shared_mutex_unlock success pid={} offset={:#x} kind={} tid={} words_before={:?}",
                    ctx.data().pid().raw(),
                    offset_key,
                    kind,
                    tid,
                    words
                );
            }
            words[1] = 0;
            words[3] = 0;
            owners.remove(&offset_key);
            owner_pids.remove(&offset_key);
            depths.remove(&offset_key);
            if let Some(wait) = ctx
                .data()
                .state
                .shared_mutex_waits
                .lock()
                .unwrap()
                .get(&offset_key)
                .cloned()
            {
                sinomo_mutex_debug!(
                    "sinomo-debug shared_mutex_notify pid={} offset={:#x}",
                    ctx.data().pid().raw(),
                    offset_key
                );
                wait.waiters.notify_all();
            }
        }
        Errno::Success
    });
    if mutex_type & 3 == 0 {
        let _ = with_mutex_words::<M>(ctx.data(), &memory, mutex_start, |words| {
            sinomo_debug!(
                "sinomo-debug shared_mutex_unlock normal-after-central pid={} offset={:#x} tid={} words={:?}",
                ctx.data().pid().raw(),
                Into::<u64>::into(guest_offset),
                tid,
                words
            );
            Errno::Success
        });
    }
    // Unlock is a release operation. The guest-to-shared sync above publishes
    // ordinary shared-memory writes before releasing the central mutex owner.
    // Pulling the whole shared mapping back into the unlocking process here can
    // overwrite unrelated guest state during high-contention wakeups; only the
    // mutex word itself needs to be reflected locally before returning.
    let _ = copy_shared_mutex_to_guest::<M>(ctx.data(), &memory, guest_offset.into());
    sinomo_debug!(
        "sinomo-debug shared_mutex_unlock return pid={} offset={:#x} tid={} result={:?}",
        ctx.data().pid().raw(),
        Into::<u64>::into(guest_offset),
        tid,
        result
    );
    Ok(guest_errno_i32(result))
}

#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn shared_mutex_wait<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    offset: M::Offset,
    observed_owner: u32,
    timeout_ns: i64,
) -> Result<i32, WasiError> {
    let offset_u64 = Into::<u64>::into(offset);
    let offset_key = shared_mutex_key(ctx.data(), offset_u64);
    let current_pid = ctx.data().pid().raw();
    let wait = {
        let mut waits = ctx.data().state.shared_mutex_waits.lock().unwrap();
        waits
            .entry(offset_key)
            .or_insert_with(|| {
                std::sync::Arc::new(crate::state::WasiSharedMutexWait {
                    state: Default::default(),
                    waiters: std::sync::Condvar::new(),
                })
            })
            .clone()
    };

    {
        let mut state = wait.state.lock().unwrap();
        *state.waiters_by_pid.entry(current_pid).or_insert(0) += 1;
        sinomo_mutex_debug!(
            "sinomo-debug shared_mutex_wait_enter pid={} offset={:#x} observed={} waiters={:?} handoff_pid={:?}",
            current_pid,
            offset_key,
            observed_owner,
            state.waiters_by_pid,
            state.handoff_pid
        );
    }

    let max_wait = if timeout_ns < 0 {
        None
    } else {
        Some(std::time::Duration::from_nanos(timeout_ns as u64))
    };
    let started = std::time::Instant::now();
    let mut timed_out = false;
    let mut guard = wait.state.lock().unwrap();
    loop {
        drop(guard);
        let owner = current_shared_mutex_owner(ctx.data(), offset_key);
        guard = wait.state.lock().unwrap();
        let can_try = owner != observed_owner || owner == 0;
        if can_try {
            break;
        }
        if let Some(max_wait) = max_wait {
            let elapsed = started.elapsed();
            if elapsed >= max_wait {
                timed_out = true;
                break;
            }
            let remaining = max_wait.saturating_sub(elapsed);
            let (next_guard, result) = wait.waiters.wait_timeout(guard, remaining).unwrap();
            guard = next_guard;
            if result.timed_out() {
                timed_out = true;
                break;
            }
        } else {
            guard = wait.waiters.wait(guard).unwrap();
        }
    }

    if let Some(count) = guard.waiters_by_pid.get_mut(&current_pid) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            guard.waiters_by_pid.remove(&current_pid);
        }
    }
    if guard.waiters_by_pid.is_empty() {
        guard.handoff_pid = None;
    }
    sinomo_mutex_debug!(
        "sinomo-debug shared_mutex_wait_leave pid={} offset={:#x} timed_out={} waiters={:?} handoff_pid={:?}",
        current_pid,
        offset_key,
        timed_out,
        guard.waiters_by_pid,
        guard.handoff_pid
    );
    let _ = timed_out;
    Ok(0)
}

fn current_shared_mutex_owner(env: &WasiEnv, offset_key: u64) -> u32 {
    env.state
        .shared_mutex_owners
        .lock()
        .unwrap()
        .get(&offset_key)
        .copied()
        .unwrap_or(0)
}

fn with_guest_mutex_words<M: MemorySize>(
    memory: &MemoryView,
    offset: M::Offset,
    f: impl FnOnce(&mut [u32; 4]) -> Errno,
) -> Errno {
    let ptr = WasmPtr::<u32, M>::new(offset);
    let slice = match ptr.slice(memory, M::Offset::from(4u32)) {
        Ok(slice) => slice,
        Err(err) => return mem_error_to_wasi(err),
    };
    let mut words = [0u32; 4];
    for (index, cell) in slice.iter().enumerate() {
        words[index] = match cell.read() {
            Ok(value) => value,
            Err(err) => return mem_error_to_wasi(err),
        };
    }
    let result = f(&mut words);
    if mutex_result_preserves_words(result) {
        for (index, cell) in slice.iter().enumerate() {
            if let Err(err) = cell.write(words[index]) {
                return mem_error_to_wasi(err);
            }
        }
    }
    result
}

fn mutex_result_preserves_words(result: Errno) -> bool {
    matches!(
        result,
        Errno::Success | Errno::Busy | Errno::Perm | Errno::Deadlk
    )
}

fn with_mutex_words<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
    f: impl FnOnce(&mut [u32; 4]) -> Errno,
) -> Errno {
    if shared_mutex_has_mapping(env, offset) {
        with_shared_mutex_words(env, offset, f)
    } else {
        let Ok(offset) = usize::try_from(offset) else {
            return Errno::Overflow;
        };
        let Ok(offset) = to_offset::<M>(offset) else {
            return Errno::Overflow;
        };
        with_guest_mutex_words::<M>(memory, offset, f)
    }
}

fn shared_mutex_has_mapping(env: &WasiEnv, offset: u64) -> bool {
    env.state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .any(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add(16) <= mapping.offset.saturating_add(mapping.len)
        })
}

fn shared_mutex_key(env: &WasiEnv, offset: u64) -> u64 {
    if shared_mutex_has_mapping(env, offset) {
        offset
    } else {
        (1u64 << 63) | ((env.pid().raw() as u64) << 32) | (offset & 0xffff_ffff)
    }
}

fn shared_mutex_owner_token(env: &WasiEnv) -> u32 {
    let pid = env.pid().raw();
    let tid = env.tid().raw();
    let token = pid.wrapping_mul(65_537) ^ tid;
    let token = token & 0x7fff_ffff;
    if token == 0 { 1 } else { token }
}

fn copy_shared_mutex_to_guest<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
) -> Result<(), Errno> {
    let Some(mapping) = env
        .state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add(16) <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Ok(());
    };
    let index = usize::try_from(offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let bytes = mapping.read_bytes()?;
    if index.saturating_add(16) > bytes.len() {
        return Err(Errno::Inval);
    }
    let ptr = WasmPtr::<u8, M>::new(to_offset::<M>(
        offset.try_into().map_err(|_| Errno::Overflow)?,
    )?);
    let len = to_offset::<M>(16)?;
    ptr.slice(memory, len)
        .map_err(mem_error_to_wasi)?
        .write_slice(&bytes[index..index + 16])
        .map_err(mem_error_to_wasi)?;
    Ok(())
}

fn with_shared_cond_sequence_word<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    cond_offset: u64,
    seq: u32,
) -> Result<(), Errno> {
    let seq_offset = cond_offset.saturating_add(8);
    if let Some(mapping) = env
        .state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .find(|mapping| {
            seq_offset >= mapping.offset
                && seq_offset.saturating_add(4) <= mapping.offset.saturating_add(mapping.len)
        })
    {
        let index = usize::try_from(seq_offset - mapping.offset).map_err(|_| Errno::Overflow)?;
        mapping.write_synchronized_range(index, &seq.to_le_bytes())?;
    }

    let ptr = WasmPtr::<u32, M>::new(to_offset::<M>(
        seq_offset.try_into().map_err(|_| Errno::Overflow)?,
    )?);
    ptr.write(memory, seq).map_err(mem_error_to_wasi)?;
    Ok(())
}

fn shared_cond_sequence_word(env: &WasiEnv, cond_offset: u64) -> Result<Option<u32>, Errno> {
    let seq_offset = cond_offset.saturating_add(8);
    let Some(mapping) = env
        .state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .find(|mapping| {
            seq_offset >= mapping.offset
                && seq_offset.saturating_add(4) <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Ok(None);
    };
    let index = usize::try_from(seq_offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let bytes = mapping.read_range(index, 4)?;
    Ok(Some(u32::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3],
    ])))
}

fn shared_semaphore<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
) -> Result<std::sync::Arc<crate::state::WasiSharedSemaphore>, WasiError> {
    if let Some(semaphore) = env.state.shared_semaphores.lock().unwrap().get(&offset).cloned() {
        return Ok(semaphore);
    }

    let initial_value =
        shared_u32_word::<M>(env, memory, offset).map_err(|err| WasiError::Exit(err.into()))?;
    let semaphore = std::sync::Arc::new(crate::state::WasiSharedSemaphore {
        value: std::sync::Mutex::new(initial_value),
        waiters: std::sync::Condvar::new(),
    });
    env.state
        .shared_semaphores
        .lock()
        .unwrap()
        .insert(offset, semaphore.clone());
    write_shared_semaphore_value::<M>(env, memory, offset, initial_value)?;
    Ok(semaphore)
}

fn shared_u32_word<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
) -> Result<u32, Errno> {
    let Some(mapping) = env
        .state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add(4) <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        let ptr = WasmPtr::<u32, M>::new(to_offset::<M>(
            offset.try_into().map_err(|_| Errno::Overflow)?,
        )?);
        return ptr.read(memory).map_err(mem_error_to_wasi);
    };
    let index = usize::try_from(offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let bytes = mapping.read_range(index, 4)?;
    Ok(u32::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3],
    ]))
}

fn write_shared_semaphore_value<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
    value: u32,
) -> Result<(), WasiError> {
    if is_registered_shared_memory_offset(env, offset) {
        with_shared_u32_words_mut(env, offset, 4, |words| {
            words[0] = value;
        })
        .map_err(|err| WasiError::Exit(err.into()))?;
    }
    let ptr = WasmPtr::<u32, M>::new(to_offset::<M>(
        offset.try_into().map_err(|_| WasiError::Exit(Errno::Overflow.into()))?,
    )
    .map_err(|err| WasiError::Exit(err.into()))?);
    ptr.write(memory, value)
        .map_err(mem_error_to_wasi)
        .map_err(|err| WasiError::Exit(err.into()))?;
    Ok(())
}

fn with_shared_u32_words<T>(
    env: &WasiEnv,
    offset: u64,
    count: usize,
    f: impl FnOnce(&[u32]) -> T,
) -> Result<T, Errno> {
    let Some(mapping) = env
        .state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add((count * 4) as u64)
                    <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Err(Errno::Inval);
    };
    let index = usize::try_from(offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let bytes = mapping.read_range(index, count * 4)?;
    let mut words = vec![0u32; count];
    for (word_index, word) in words.iter_mut().enumerate() {
        let start = word_index * 4;
        *word = u32::from_le_bytes([
            bytes[start],
            bytes[start + 1],
            bytes[start + 2],
            bytes[start + 3],
        ]);
    }
    Ok(f(&words))
}

fn with_shared_u32_words_mut<T>(
    env: &WasiEnv,
    offset: u64,
    count: usize,
    f: impl FnOnce(&mut [u32]) -> T,
) -> Result<T, Errno> {
    let Some(mapping) = env
        .state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add((count * 4) as u64)
                    <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Err(Errno::Inval);
    };
    let index = usize::try_from(offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let mut bytes = mapping.read_range(index, count * 4)?;
    let mut words = vec![0u32; count];
    for (word_index, word) in words.iter_mut().enumerate() {
        let start = word_index * 4;
        *word = u32::from_le_bytes([
            bytes[start],
            bytes[start + 1],
            bytes[start + 2],
            bytes[start + 3],
        ]);
    }
    let result = f(&mut words);
    for (word_index, word) in words.iter().enumerate() {
        let start = word_index * 4;
        bytes[start..start + 4].copy_from_slice(&word.to_le_bytes());
    }
    mapping.write_synchronized_range(index, &bytes)?;
    Ok(result)
}

fn sync_shared_memory_range_to_guest<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
    len: u64,
) -> Result<(), Errno> {
    if !env.state.has_shared_memory_mappings() || len == 0 {
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
    let index = usize::try_from(offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let len = usize::try_from(len).map_err(|_| Errno::Overflow)?;
    let bytes = mapping.read_range(index, len)?;
    let ptr = WasmPtr::<u8, M>::new(to_offset::<M>(
        offset.try_into().map_err(|_| Errno::Overflow)?,
    )?);
    ptr.slice(memory, to_offset::<M>(len)?)
        .map_err(mem_error_to_wasi)?
        .write_slice(&bytes)
        .map_err(mem_error_to_wasi)?;
    Ok(())
}

fn sync_shared_memory_from_guest_preserving_range<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    preserve_offset: u64,
    preserve_len: usize,
) -> Result<(), Errno> {
    sync_shared_memory_from_guest_preserving_ranges::<M>(
        env,
        memory,
        &[(preserve_offset, preserve_len)],
    )
}

fn sync_shared_memory_from_guest_preserving_ranges<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    preserve_ranges: &[(u64, usize)],
) -> Result<(), Errno> {
    crate::syscalls::sync_shared_memory_from_guest_preserving_ranges::<M>(
        env,
        memory,
        preserve_ranges,
    )
}

fn sync_shared_memory_to_guest_preserving_ranges<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    preserve_ranges: &[(u64, usize)],
) -> Result<(), Errno> {
    if !env.state.has_shared_memory_mappings() {
        return Ok(());
    }
    for mapping in env.state.shared_memory_mappings_for_pid(env.pid().raw()) {
        let len = usize::try_from(mapping.len).map_err(|_| Errno::Overflow)?;
        let ptr = WasmPtr::<u8, M>::new(to_offset::<M>(
            mapping.offset.try_into().map_err(|_| Errno::Overflow)?,
        )?);
        let slice = ptr
            .slice(memory, to_offset::<M>(len)?)
            .map_err(mem_error_to_wasi)?;
        let bytes = mapping.read_bytes()?;
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

        let mut cursor = 0usize;
        let mut snapshot_updates = Vec::new();
        for (start, end) in merged {
            if cursor < start {
                slice
                    .subslice(cursor as u64..start as u64)
                    .write_slice(&bytes[cursor..start])
                    .map_err(mem_error_to_wasi)?;
                snapshot_updates.push((cursor, start));
            }
            cursor = cursor.max(end);
        }
        if cursor < len {
            slice
                .subslice(cursor as u64..len as u64)
                .write_slice(&bytes[cursor..])
                .map_err(mem_error_to_wasi)?;
            snapshot_updates.push((cursor, len));
        }
        let mut snapshots = mapping.process_snapshots.lock().unwrap();
        let snapshot = snapshots
            .entry(env.pid().raw())
            .or_insert_with(|| bytes.clone());
        if snapshot.len() != len {
            *snapshot = bytes.clone();
        }
        for (start, end) in snapshot_updates {
            snapshot[start..end].copy_from_slice(&bytes[start..end]);
        }
    }
    Ok(())
}

fn import_guest_mutex_if_shared_unowned<M: MemorySize>(
    env: &WasiEnv,
    memory: &MemoryView,
    offset: u64,
    _current_tid: u32,
) -> Result<(), Errno> {
    if env
        .state
        .shared_mutex_owners
        .lock()
        .unwrap()
        .contains_key(&offset)
    {
        return Ok(());
    }

    let Some(mapping) = env
        .state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add(16) <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Ok(());
    };
    let mut guest_words = [0u32; 4];
    let ptr = WasmPtr::<u32, M>::new(to_offset::<M>(
        offset.try_into().map_err(|_| Errno::Overflow)?,
    )?);
    let slice = ptr
        .slice(memory, M::Offset::from(4u32))
        .map_err(mem_error_to_wasi)?;
    for (index, cell) in slice.iter().enumerate() {
        guest_words[index] = cell.read().map_err(mem_error_to_wasi)?;
    }
    let index = usize::try_from(offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let mut bytes = mapping.read_range(index, 16)?;
    let shared_owner = u32::from_le_bytes([
        bytes[4], bytes[5], bytes[6], bytes[7],
    ]) & 0x7fffffff;
    if shared_owner != 0 {
        return Ok(());
    }
    env.state
        .shared_mutex_owners
        .lock()
        .unwrap()
        .remove(&offset);
    env.state
        .shared_mutex_owner_pids
        .lock()
        .unwrap()
        .remove(&offset);
    env.state
        .shared_mutex_depths
        .lock()
        .unwrap()
        .remove(&offset);
    let guest_owner = guest_words[1] & 0x7fffffff;
    if guest_owner != 0 {
        guest_words[1] = 0;
        guest_words[3] = 0;
    }
    for (word_index, word) in guest_words.iter().enumerate() {
        let start = word_index * 4;
        bytes[start..start + 4].copy_from_slice(&word.to_le_bytes());
    }
    mapping.write_synchronized_range(index, &bytes)?;
    Ok(())
}

fn import_runtime_mutex_owners_from_shared_memory(env: &WasiEnv) {
    const MUTEX_PSHARED: u32 = 128;
    for range in env.state.shared_futex_ranges() {
        if range.end.saturating_sub(range.start) < 16 {
            continue;
        }
        let Some(mapping) = env
            .state
            .shared_memory_mappings_for_pid(env.pid().raw())
            .into_iter()
            .find(|mapping| {
                range.start >= mapping.offset
                    && range.start.saturating_add(16)
                        <= mapping.offset.saturating_add(mapping.len)
            })
        else {
            continue;
        };
        let Ok(index) = usize::try_from(range.start - mapping.offset) else {
            continue;
        };
        let bytes = match mapping.read_range(index, 16) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let flags = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if flags == u32::MAX || (flags & MUTEX_PSHARED) == 0 {
            continue;
        }
        let owner = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) & 0x7fff_ffff;
        if owner == 0 {
            continue;
        }
        let depth = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
        drop(bytes);
        env.state
            .shared_mutex_owners
            .lock()
            .unwrap()
            .insert(range.start, owner);
        env.state
            .shared_mutex_depths
            .lock()
            .unwrap()
            .insert(range.start, depth);
    }
}

fn with_shared_mutex_words(
    env: &WasiEnv,
    offset: u64,
    f: impl FnOnce(&mut [u32; 4]) -> Errno,
) -> Errno {
    let Some(mapping) = env
        .state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add(16) <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Errno::Inval;
    };
    let index = match usize::try_from(offset - mapping.offset) {
        Ok(index) => index,
        Err(_) => return Errno::Overflow,
    };
    let mut bytes = match mapping.read_range(index, 16) {
        Ok(bytes) => bytes,
        Err(err) => return err,
    };
    let mut words = [0u32; 4];
    for (word_index, word) in words.iter_mut().enumerate() {
        let start = word_index * 4;
        *word = u32::from_le_bytes([
            bytes[start],
            bytes[start + 1],
            bytes[start + 2],
            bytes[start + 3],
        ]);
    }
    let result = f(&mut words);
    if mutex_result_preserves_words(result) {
        for (word_index, word) in words.iter().enumerate() {
            let start = word_index * 4;
            bytes[start..start + 4].copy_from_slice(&word.to_le_bytes());
        }
        if let Err(err) = mapping.write_synchronized_range(index, &bytes) {
            return err;
        }
    }
    result
}

fn shared_mutex_owner(env: &WasiEnv, offset: u64) -> Result<Option<u32>, Errno> {
    let Some(mapping) = env
        .state
        .shared_memory_mappings_for_pid(env.pid().raw())
        .into_iter()
        .find(|mapping| {
            offset >= mapping.offset
                && offset.saturating_add(16) <= mapping.offset.saturating_add(mapping.len)
        })
    else {
        return Ok(None);
    };
    let index = usize::try_from(offset - mapping.offset).map_err(|_| Errno::Overflow)?;
    let bytes = mapping.read_range(index, 8)?;
    Ok(Some(
        u32::from_le_bytes([
            bytes[4], bytes[5], bytes[6], bytes[7],
        ]) & 0x7fffffff,
    ))
}

/// ### `clone_thread()`
/// POSIX/Linux-shaped same-process clone for `CLONE_THREAD | CLONE_VM`
/// callers. This uses the runtime's asyncify rewind path: the parent receives
/// the new TID and the cloned thread resumes from the syscall with return
/// value zero.
#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn clone_thread<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    child_stack: u64,
    parent_tid_ptr: WasmPtr<i32, M>,
    child_tid_ptr: WasmPtr<i32, M>,
    tls: u64,
    set_parent_tid: Bool,
    set_child_tid: Bool,
    clear_child_tid: Bool,
) -> Result<i32, WasiError> {
    clone_thread_common::<M>(
        ctx,
        child_stack,
        parent_tid_ptr,
        child_tid_ptr,
        tls,
        set_parent_tid == Bool::True,
        set_child_tid == Bool::True,
        clear_child_tid == Bool::True,
    )
}

fn fork_outcome_to_posix(outcome: ForkOutcome) -> i32 {
    if outcome.ret == Errno::Success {
        outcome.pid as i32
    } else {
        -outcome.ret.to_native()
    }
}

fn clone_thread_result_to_posix(result: CloneThreadResult) -> i32 {
    if result.ret == Errno::Success {
        result.tid as i32
    } else {
        -result.ret.to_native()
    }
}

fn clone_thread_common<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    child_stack: u64,
    parent_tid_ptr: WasmPtr<i32, M>,
    child_tid_ptr: WasmPtr<i32, M>,
    tls: u64,
    set_parent_tid: bool,
    set_child_tid: bool,
    clear_child_tid: bool,
) -> Result<i32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if let Some(context_switching_environment) = ctx.data().context_switching_environment.as_ref()
        && context_switching_environment.active_context_id()
            != context_switching_environment.main_context_id()
    {
        warn!(
            "thread cloning is only supported from the main context when using WASIX context-switching features"
        );
        return Ok(-Errno::Notsup.to_native());
    }

    if let Some(result) = unsafe { handle_rewind::<M, CloneThreadResult>(&mut ctx) } {
        return Ok(clone_thread_result_to_posix(result));
    }

    if !supports_asyncified_fork(ctx.data()) {
        warn!("thread cloning requires asyncify unwind and rewind exports");
        return Ok(-Errno::Noexec.to_native());
    }

    let current_layout = ctx.data().thread.memory_layout().clone();
    let stack_upper = if child_stack == 0 {
        current_layout.stack_upper
    } else {
        child_stack
    };
    let stack_size = current_layout.stack_size;
    let stack_lower = match stack_upper.checked_sub(stack_size) {
        Some(value) => value,
        None => return Ok(-Errno::Inval.to_native()),
    };
    let layout = WasiMemoryLayout {
        stack_upper,
        stack_lower,
        guard_size: current_layout.guard_size,
        stack_size,
        tls_base: if tls == 0 {
            current_layout.tls_base
        } else {
            Some(tls)
        },
    };

    let thread_start = ThreadStartType::ThreadSpawn { start_ptr: 0 };
    let thread_handle = match ctx.data().process.new_thread(layout.clone(), thread_start) {
        Ok(handle) => handle,
        Err(err) => {
            warn!(?err, "failed to create cloned thread");
            return Ok(-Errno::Again.to_native());
        }
    };
    let tid: Tid = thread_handle.id().into();
    if clear_child_tid {
        thread_handle.set_clear_child_tid(child_tid_ptr.offset().into());
    }
    let thread_id = thread_handle.id();

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    if set_parent_tid
        && !parent_tid_ptr.is_null()
        && let Err(err) = parent_tid_ptr.write(&memory, tid as i32)
    {
        env.process.remove_thread(&thread_id);
        return Ok(-crate::mem_error_to_wasi(err).to_native());
    }
    if set_child_tid
        && !child_tid_ptr.is_null()
        && let Err(err) = child_tid_ptr.write(&memory, tid as i32)
    {
        env.process.remove_thread(&thread_id);
        return Ok(-crate::mem_error_to_wasi(err).to_native());
    }

    let state = env.state.clone();
    let snapshot = capture_store_snapshot(&mut ctx.as_store_mut());
    unwind::<M, _>(ctx, move |mut ctx, memory_stack, rewind_stack| {
        let env_inner = ctx.data().inner();
        let instance_handles = env_inner.main_module_instance_handles();
        let module = instance_handles.module_clone();
        let dynamic_linker = env_inner.linker().cloned();
        let mut thread_env = ctx.data().clone();
        thread_env.state = state;
        thread_env.thread = thread_handle.as_thread();
        thread_env.layout = layout;
        thread_env.enable_deep_sleep = false;
        let child_handle = thread_handle;
        let tasks = thread_env.tasks().clone();
        let spawn_type = if let Some(linker) = dynamic_linker {
            let func_env = ctx.as_ref();
            let store = ctx.as_store_mut();
            SpawnType::NewLinkerInstanceGroup(linker, func_env, store, false)
        } else {
            let memory = instance_handles.memory_clone();
            SpawnType::ShareMemory(memory, ctx.as_store_ref())
        };

        let memory_stack = memory_stack.freeze();
        let rewind_stack = rewind_stack.freeze();
        let store_data = match snapshot.serialize() {
            Ok(store_data) => Bytes::from(store_data),
            Err(err) => {
                warn!("failed to serialize clone-thread store snapshot - {}", err);
                ctx.data().process.remove_thread(&thread_id);
                return OnCalledAction::Trap(Box::new(WasiError::Exit(Errno::Perm.into())));
            }
        };

        let child_memory_stack = memory_stack.clone();
        let child_rewind_stack = rewind_stack.clone();
        let child_store_data = store_data.clone();
        let spawn_result = tasks.task_wasm(
            TaskWasm::new(
                Box::new(move |props: TaskWasmRunProperties| {
                    run_clone_thread::<M>(
                        props.ctx,
                        props.store,
                        child_handle,
                        child_memory_stack,
                        child_rewind_stack,
                        child_store_data.clone(),
                    );
                }),
                thread_env,
                module,
                false,
                false,
            )
            .with_globals(snapshot)
            .with_memory(spawn_type),
        );

        if let Err(err) = spawn_result {
            warn!("failed to spawn cloned thread - {}", err);
            ctx.data().process.remove_thread(&thread_id);
            match rewind::<M, _>(
                ctx,
                Some(memory_stack),
                rewind_stack,
                store_data,
                CloneThreadResult {
                    tid: 0,
                    ret: Errno::Again,
                },
            ) {
                Errno::Success => OnCalledAction::InvokeAgain,
                err => OnCalledAction::Trap(Box::new(WasiError::Exit(err.into()))),
            }
        } else {
            match rewind::<M, _>(
                ctx,
                Some(memory_stack),
                rewind_stack,
                store_data,
                CloneThreadResult {
                    tid,
                    ret: Errno::Success,
                },
            ) {
                Errno::Success => OnCalledAction::InvokeAgain,
                err => OnCalledAction::Trap(Box::new(WasiError::Exit(err.into()))),
            }
        }
    })
    .map(|err| -err.to_native())
}

fn clone_thread_rewind_result<M: MemorySize>(result: CloneThreadResult) -> RewindResultType {
    RewindResultType::RewindWithResult(
        bincode::serde::encode_to_vec(&result, config::legacy())
            .unwrap()
            .into(),
    )
}

fn run_clone_thread<M: MemorySize>(
    ctx: WasiFunctionEnv,
    store: Store,
    child_handle: WasiThreadHandle,
    memory_stack: Bytes,
    rewind_stack: Bytes,
    store_data: Bytes,
) {
    let layout = ctx.data(&store).layout.clone();
    let rewind_state = RewindState {
        memory_stack,
        rewind_stack,
        store_data,
        start: ThreadStartType::ThreadSpawn { start_ptr: 0 },
        layout,
        is_64bit: M::is_64bit(),
    };
    run_clone_thread_from_rewind::<M>(
        ctx,
        store,
        child_handle,
        Some((
            rewind_state,
            clone_thread_rewind_result::<M>(CloneThreadResult {
                tid: 0,
                ret: Errno::Success,
            }),
        )),
    );
}

fn run_clone_thread_from_rewind<M: MemorySize>(
    ctx: WasiFunctionEnv,
    mut store: Store,
    child_handle: WasiThreadHandle,
    rewind_state: Option<(RewindState, RewindResultType)>,
) {
    let env = ctx.data(&store);
    let tasks = env.tasks().clone();
    let pid = env.pid();
    let tid = env.tid();

    if let Some((rewind_state, rewind_result)) = rewind_state {
        let mut ctx_mut = ctx.env.clone().into_mut(&mut store);
        let res = rewind_ext::<M>(
            &mut ctx_mut,
            Some(rewind_state.memory_stack),
            rewind_state.rewind_stack,
            rewind_state.store_data,
            rewind_result,
        );
        if res != Errno::Success {
            child_handle.set_status_finished(Ok(res.into()));
            return;
        }
    }

    trace!(%pid, %tid, "re-invoking clone thread continuation");
    let env_inner = ctx.data(&store).inner();
    let module_handles = env_inner.main_module_instance_handles();
    let Some(start) = module_handles.start.clone() else {
        warn!(%pid, %tid, "cloned thread is missing _start export");
        child_handle.set_status_finished(Ok(Errno::Noexec.into()));
        return;
    };

    let mut ret: ExitCode = Errno::Success.into();
    let (mut store, err) =
        ContextSwitchingEnvironment::run_main_context(&ctx, store, start.into(), vec![]);
    if let Err(err) = err {
        match err.downcast::<WasiError>() {
            Ok(WasiError::Exit(exit_code)) => {
                ret = exit_code;
            }
            Ok(WasiError::ThreadExit) => {
                ret = Errno::Success.into();
            }
            Ok(WasiError::DeepSleep(deep)) => {
                trace!(%pid, %tid, "cloned thread entered a deep sleep");
                let respawn = {
                    let tasks = tasks.clone();
                    let rewind_state = deep.rewind;
                    move |ctx, store, rewind_result| {
                        run_clone_thread_from_rewind::<M>(
                            ctx,
                            store,
                            child_handle,
                            Some((
                                rewind_state,
                                RewindResultType::RewindWithResult(rewind_result),
                            )),
                        );
                    }
                };
                unsafe {
                    tasks.resume_wasm_after_poller(Box::new(respawn), ctx, store, deep.trigger)
                };
                return;
            }
            _ => {
                ret = Errno::Unknown.into();
            }
        }
    }

    trace!(%pid, %tid, "cloned thread exited (code = {})", ret);
    ctx.on_exit(&mut store, Some(ret));
    child_handle.set_status_finished(Ok(ret));
    drop(child_handle);
}

fn supports_asyncified_fork(env: &WasiEnv) -> bool {
    let handles = env.inner().main_module_instance_handles();
    handles.asyncify_start_unwind.is_some()
        && handles.asyncify_stop_unwind.is_some()
        && handles.asyncify_start_rewind.is_some()
        && handles.asyncify_stop_rewind.is_some()
}

fn proc_fork_common<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    copy_memory: Bool,
    pid_ptr: Option<WasmPtr<Pid, M>>,
) -> Result<ForkOutcome, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    if let Some(context_switching_environment) = ctx.data().context_switching_environment.as_ref()
        && context_switching_environment.active_context_id()
            != context_switching_environment.main_context_id()
    {
        warn!(
            "process forking is only supported from the main context when using WASIX context-switching features"
        );
        return Ok(ForkOutcome {
            pid: 0,
            ret: Errno::Notsup,
        });
    }

    // If we were just restored then we need to return the value instead
    if let Some(result) = unsafe { handle_rewind::<M, ForkResult>(&mut ctx) } {
        if result.pid == 0 {
            trace!("handle_rewind - i am child (ret={})", result.ret);
        } else {
            trace!(
                "handle_rewind - i am parent (child={}, ret={})",
                result.pid, result.ret
            );
            if ctx.data().state.has_shared_memory_mappings() {
                sinomo_debug!(
                    "sinomo-debug proc_fork parent-rewind pid={} child={} ret={:?}",
                    ctx.data().pid().raw(),
                    result.pid,
                    result.ret
                );
            }
        }
        if let Some(pid_ptr) = pid_ptr {
            let memory = unsafe { ctx.data().memory_view(&ctx) };
            if let Err(err) = pid_ptr.write(&memory, result.pid) {
                return Ok(ForkOutcome {
                    pid: 0,
                    ret: crate::mem_error_to_wasi(err),
                });
            }
        }
        return Ok(ForkOutcome {
            pid: result.pid,
            ret: result.ret,
        });
    }
    trace!(%copy_memory, "capturing");

    if let Some(vfork) = ctx.data().vfork.as_ref() {
        warn!("process forking not supported in an active vfork");
        return Ok(ForkOutcome {
            pid: 0,
            ret: Errno::Notsup,
        });
    }
    if ctx.data().process.child_process_limit_reached() {
        sinomo_debug!(
            "sinomo-debug proc_fork child-process-limit pid={}",
            ctx.data().pid().raw()
        );
        return Ok(ForkOutcome {
            pid: 0,
            ret: Errno::Again,
        });
    }
    if ctx.data().process.task_limit_reached() {
        sinomo_debug!(
            "sinomo-debug proc_fork task-limit pid={}",
            ctx.data().pid().raw()
        );
        return Ok(ForkOutcome {
            pid: 0,
            ret: Errno::Again,
        });
    }

    if !supports_asyncified_fork(ctx.data()) {
        warn!("process forking requires asyncify unwind and rewind exports");
        return Ok(ForkOutcome {
            pid: 0,
            ret: Errno::Noexec,
        });
    }
    let shared_memory_was_cross_process = ctx.data().state.has_cross_process_shared_memory();
    if ctx.data().state.has_shared_memory_mappings() {
        ctx.data().state.mark_cross_process_shared_memory();
    }
    let parent_had_unwaited_children = {
        let inner = ctx.data().process.lock();
        !inner.children.is_empty()
    };

    // Fork the environment which will copy all the open file handlers
    // and associate a new context but otherwise shares things like the
    // file system interface. The handle to the forked process is stored
    // in the parent process context
    let (mut child_env, mut child_handle) = match ctx.data().fork() {
        Ok(p) => p,
        Err(err) => {
            debug!("could not fork process: {err}");
            // TODO: evaluate the appropriate error code, document it in the spec.
            return Ok(ForkOutcome {
                pid: 0,
                ret: Errno::Perm,
            });
        }
    };
    let child_pid = child_env.process.pid();
    let child_finished = child_env.process.finished.clone();
    if let Ok(now) = platform_clock_time_get(Snapshot0Clockid::ProcessCputimeId, 1) {
        child_env
            .state
            .clock_offset
            .lock()
            .unwrap()
            .insert(Snapshot0Clockid::ProcessCputimeId, -(now as i64));
    }
    ctx.data()
        .state
        .inherit_shared_memory_mappings_for_child(ctx.data().pid().raw(), child_pid.raw());

    // We write a zero to the PID before we capture the stack
    // so that this is what will be returned to the child
    {
        let mut inner = ctx.data().process.lock();
        inner.children.push(child_env.process.clone());
    }
    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    if env.state.has_shared_memory_mappings() {
        let mappings = env.state.shared_memory_mappings_for_pid(env.pid().raw());
        let total_len = mappings.iter().map(|mapping| mapping.len).sum::<u64>();
        let direction = if shared_memory_was_cross_process && !parent_had_unwaited_children {
            "from-guest-fresh-generation"
        } else if shared_memory_was_cross_process {
            "unchanged-unwaited-children"
        } else {
            "from-guest"
        };
        sinomo_debug!(
            "sinomo-debug proc_fork shared-sync-{} pid={} mappings={} total_len={}",
            direction,
            env.pid().raw(),
            mappings.len(),
            total_len
        );
    }
    if shared_memory_was_cross_process && !parent_had_unwaited_children {
        if let Err(err) = sync_shared_memory_from_guest::<M>(env, &memory) {
            return Ok(ForkOutcome { pid: 0, ret: err });
        }
    } else if shared_memory_was_cross_process {
        // Existing children may already have published MAP_SHARED writes, even
        // if they exited before the parent reaps them. Do not copy the
        // parent's potentially stale shared bytes back before every subsequent
        // fork; synchronization objects pull the central mapping when the child
        // acquires them.
    } else {
        if let Err(err) = sync_shared_memory_from_guest_including_futexes::<M>(env, &memory) {
            return Ok(ForkOutcome { pid: 0, ret: err });
        }
        import_runtime_mutex_owners_from_shared_memory(env);
    }
    if env.state.has_shared_memory_mappings() {
        let direction = if shared_memory_was_cross_process && !parent_had_unwaited_children {
            "from-guest-fresh-generation"
        } else if shared_memory_was_cross_process {
            "unchanged-unwaited-children"
        } else {
            "from-guest"
        };
        sinomo_debug!(
            "sinomo-debug proc_fork shared-sync-{}-done pid={}",
            direction,
            env.pid().raw()
        );
        for mapping in env.state.shared_memory_mappings_for_pid(env.pid().raw()) {
            if let Err(err) = map_shared_memory_backing(env, &memory, &mapping) {
                return Ok(ForkOutcome { pid: 0, ret: err });
            }
        }
    }

    // Setup some properties in the child environment
    if let Some(pid_ptr) = pid_ptr {
        if let Err(err) = pid_ptr.write(&memory, 0) {
            return Ok(ForkOutcome {
                pid: 0,
                ret: crate::mem_error_to_wasi(err),
            });
        }
    }
    let pid = child_env.pid();
    let tid = child_env.tid();

    // If we are not copying the memory then we act like a `vfork`
    // instead which will pretend to be the new process for a period
    // of time until `proc_exec` is called at which point the fork
    // actually occurs
    if copy_memory == Bool::False {
        // Perform the unwind action
        return unwind::<M, _>(ctx, move |mut ctx, mut memory_stack, rewind_stack| {
            // Grab all the globals and serialize them
            let store_data =
                match crate::utils::store::capture_store_snapshot(&mut ctx.as_store_mut())
                    .serialize()
                {
                    Ok(store_data) => store_data,
                    Err(err) => {
                        warn!("failed to serialize vfork store snapshot - {}", err);
                        return OnCalledAction::Trap(Box::new(WasiError::Exit(Errno::Perm.into())));
                    }
                };
            let store_data = Bytes::from(store_data);

            // We first fork the environment and replace the current environment
            // so that the process can continue to prepare for the real fork as
            // if it had actually forked
            child_env.swap_inner(ctx.data_mut());
            std::mem::swap(ctx.data_mut(), &mut child_env);
            let previous_vfork = ctx.data_mut().vfork.replace(WasiVFork {
                asyncify: Some(WasiVForkAsyncify {
                    rewind_stack: rewind_stack.clone(),
                    store_data: store_data.clone(),
                    is_64bit: M::is_64bit(),
                }),
                env: Box::new(child_env),
                handle: child_handle,
            });
            assert!(previous_vfork.is_none()); // Already checked above

            // Carry on as if the fork had taken place (which basically means
            // it prevents to be the new process with the old one suspended)
            // Rewind the stack and carry on
            match rewind::<M, _>(
                ctx,
                Some(memory_stack.freeze()),
                rewind_stack.freeze(),
                store_data,
                ForkResult {
                    pid: 0,
                    ret: Errno::Success,
                },
            ) {
                Errno::Success => OnCalledAction::InvokeAgain,
                err => {
                    warn!("failed - could not rewind the stack - errno={}", err);
                    OnCalledAction::Trap(Box::new(WasiError::Exit(err.into())))
                }
            }
        })
        .map(|ret| ForkOutcome { pid: 0, ret });
    }

    // Create the thread that will back this forked process
    let state = env.state.clone();
    let bin_factory = env.bin_factory.clone();

    // Perform the unwind action
    let snapshot = capture_store_snapshot(&mut ctx.as_store_mut());
    unwind::<M, _>(ctx, move |mut ctx, mut memory_stack, rewind_stack| {
        let tasks = ctx.data().tasks().clone();
        let span = debug_span!(
            "unwind",
            memory_stack_len = memory_stack.len(),
            rewind_stack_len = rewind_stack.len()
        );
        let _span_guard = span.enter();
        let memory_stack = memory_stack.freeze();
        let rewind_stack = rewind_stack.freeze();

        // Grab all the globals and serialize them
        let store_data = match snapshot.serialize() {
            Ok(store_data) => store_data,
            Err(err) => {
                warn!("failed to serialize fork store snapshot - {}", err);
                match rewind::<M, _>(
                    ctx,
                    Some(memory_stack),
                    rewind_stack,
                    Bytes::new(),
                    ForkResult {
                        pid: 0,
                        ret: Errno::Perm,
                    },
                ) {
                    Errno::Success => return OnCalledAction::InvokeAgain,
                    err => {
                        warn!("failed - could not rewind the stack - errno={}", err);
                        return OnCalledAction::Trap(Box::new(WasiError::Exit(err.into())));
                    }
                }
            }
        };
        let store_data = Bytes::from(store_data);

        // Now we use the environment and memory references
        let runtime = child_env.runtime.clone();
        let tasks = child_env.tasks().clone();
        let child_memory_stack = memory_stack.clone();
        let child_rewind_stack = rewind_stack.clone();

        let env_inner = ctx.data().inner();
        let instance_handles = env_inner.main_module_instance_handles();
        let module = instance_handles.module_clone();
        let dynamic_linker = env_inner.linker().cloned();
        let spawn_type = if let Some(linker) = dynamic_linker {
            let func_env = ctx.as_ref();
            let store = ctx.as_store_mut();
            SpawnType::NewLinkerInstanceGroup(linker, func_env, store, true)
        } else {
            let memory = instance_handles.memory_clone();
            SpawnType::CopyMemory(memory, ctx.as_store_ref())
        };

        // Spawn a new process with this current execution environment
        let signaler = Box::new(child_env.process.clone());
        {
            let runtime = runtime.clone();
            let tasks = tasks.clone();
            let tasks_outer = tasks.clone();
            let child_store_data = store_data.clone();
            let child_state = state.clone();

            let run = move |mut props: TaskWasmRunProperties| {
                let ctx = props.ctx;
                let mut store = props.store;

                // Rewind the stack and carry on
                {
                    if child_state.has_shared_memory_mappings() {
                        sinomo_debug!(
                            "sinomo-debug proc_fork child-task-start pid={}",
                            child_pid.raw()
                        );
                    }
                    trace!("rewinding child");
                    let mut ctx = ctx.env.clone().into_mut(&mut store);
                    if child_state.has_shared_memory_mappings() {
                        let memory = unsafe { ctx.data().memory_view(&ctx) };
                        for mapping in child_state.shared_memory_mappings_for_pid(child_pid.raw()) {
                            if let Err(err) =
                                map_shared_memory_backing(ctx.data(), &memory, &mapping)
                            {
                                warn!(
                                    "failed to map child shared memory range - errno={}",
                                    err
                                );
                                return;
                            }
                        }
                    }
                    let (_data, mut store) = ctx.data_and_store_mut();
                    match rewind::<M, _>(
                        ctx,
                        Some(child_memory_stack),
                        child_rewind_stack,
                        child_store_data.clone(),
                        ForkResult {
                            pid: 0,
                            ret: Errno::Success,
                        },
                    ) {
                        Errno::Success => OnCalledAction::InvokeAgain,
                        err => {
                            warn!(
                                "wasm rewind failed - could not rewind the stack - errno={}",
                                err
                            );
                            return;
                        }
                    };
                    if child_state.has_shared_memory_mappings() {
                        sinomo_debug!(
                            "sinomo-debug proc_fork child-rewind-done pid={}",
                            child_pid.raw()
                        );
                    }
                }

                // Invoke the start function
                run::<M>(ctx, store, child_handle, None);
            };

            let spawn_result = tasks_outer
                .task_wasm(
                    TaskWasm::new(Box::new(run), child_env, module, false, false)
                        .with_globals(snapshot)
                        .with_memory(spawn_type),
                )
                .map_err(|err| {
                    warn!(
                        "failed to fork as the process could not be spawned - {}",
                        err
                    );
                    err
                });
            if spawn_result.is_err() {
                {
                    let mut inner = ctx.data().process.lock();
                    inner.children.retain(|child| child.pid != child_pid);
                }
                ctx.data().control_plane.remove_process(child_pid);
                match rewind::<M, _>(
                    ctx,
                    Some(memory_stack),
                    rewind_stack,
                    store_data,
                    ForkResult {
                        pid: 0,
                        ret: Errno::Perm,
                    },
                ) {
                    Errno::Success => OnCalledAction::InvokeAgain,
                    err => {
                        warn!("failed - could not rewind the stack - errno={}", err);
                        OnCalledAction::Trap(Box::new(WasiError::Exit(err.into())))
                    }
                }
            } else {
                if state.has_shared_memory_mappings() {
                    sinomo_debug!(
                        "sinomo-debug proc_fork child-spawned parent_pid={} child_pid={}",
                        ctx.data().pid().raw(),
                        child_pid.raw()
                    );
                }
                match rewind::<M, _>(
                    ctx,
                    Some(memory_stack),
                    rewind_stack,
                    store_data,
                    ForkResult {
                        pid: child_pid.raw() as Pid,
                        ret: Errno::Success,
                    },
                ) {
                    Errno::Success => OnCalledAction::InvokeAgain,
                    err => {
                        warn!("failed - could not rewind the stack - errno={}", err);
                        OnCalledAction::Trap(Box::new(WasiError::Exit(err.into())))
                    }
                }
            }
        }
    })
    .map(|ret| ForkOutcome { pid: 0, ret })
}

fn run<M: MemorySize>(
    ctx: WasiFunctionEnv,
    mut store: Store,
    child_handle: WasiThreadHandle,
    rewind_state: Option<(RewindState, RewindResultType)>,
) -> ExitCode {
    let env = ctx.data(&store);
    let tasks = env.tasks().clone();
    let pid = env.pid();
    let tid = env.tid();

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
            return res.into();
        }
    }

    {
        let mut ctx = ctx.env.clone().into_mut(&mut store);
        if ctx.data().state.has_shared_memory_mappings() {
            let memory = unsafe { ctx.data().memory_view(&ctx) };
            for mapping in ctx.data().state.shared_memory_mappings_for_pid(ctx.data().pid().raw()) {
                if let Ok(mapping_offset) = usize::try_from(mapping.offset) {
                    let base = unsafe { memory.data_ptr().add(mapping_offset) };
                    mapping
                        .mapped_bases
                        .lock()
                        .unwrap()
                        .remove(&(ctx.data().pid().raw(), ctx.data().tid().raw(), base as usize));
                }
                if let Err(err) = map_shared_memory_backing(ctx.data(), &memory, &mapping) {
                    warn!(
                        "failed to remap shared memory range before resuming process - errno={}",
                        err
                    );
                    return err.into();
                }
            }
        }
    }

    let mut ret: ExitCode = Errno::Success.into();
    let (mut store, err) = if ctx.data(&store).thread.is_main() {
        trace!(%pid, %tid, "re-invoking main");
        let env_inner = ctx.data(&store).inner();
        let module_handles = env_inner.main_module_instance_handles();
        let Some(start) = module_handles.start.clone() else {
            warn!(%pid, %tid, "forked process is missing _start export");
            return Errno::Noexec.into();
        };
        ContextSwitchingEnvironment::run_main_context(&ctx, store, start.into(), vec![])
    } else {
        trace!(%pid, %tid, "re-invoking thread_spawn");
        let env_inner = ctx.data(&store).inner();
        let module_handles = env_inner.main_module_instance_handles();
        let Some(start) = module_handles.thread_spawn.clone() else {
            warn!(%pid, %tid, "forked thread is missing wasi_thread_start export");
            return Errno::Noexec.into();
        };
        let params = vec![0i32.into(), 0i32.into()];
        ContextSwitchingEnvironment::run_main_context(&ctx, store, start.into(), params)
    };
    if let Err(err) = err {
        match err.downcast::<WasiError>() {
            Ok(WasiError::Exit(exit_code)) => {
                ret = exit_code;
            }
            Ok(WasiError::DeepSleep(deep)) => {
                trace!(%pid, %tid, "entered a deep sleep");

                // Create the respawn function
                let respawn = {
                    let tasks = tasks.clone();
                    let rewind_state = deep.rewind;
                    move |ctx, store, rewind_result| {
                        run::<M>(
                            ctx,
                            store,
                            child_handle,
                            Some((
                                rewind_state,
                                RewindResultType::RewindWithResult(rewind_result),
                            )),
                        );
                    }
                };

                /// Spawns the WASM process after a trigger
                unsafe {
                    tasks.resume_wasm_after_poller(Box::new(respawn), ctx, store, deep.trigger)
                };
                return Errno::Success.into();
            }
            _ => {}
        }
    }
    trace!(%pid, %tid, "child exited (code = {})", ret);

    // Clean up the environment and return the result
    ctx.on_exit((&mut store), Some(ret));

    // We drop the handle at the last moment which will close the thread
    drop(child_handle);
    ret
}
