use super::*;
use crate::syscalls::*;

const KEEP_ID: u32 = u32::MAX;

/// ### `proc_getuid()`
/// Returns the sandbox user id.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_getuid<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    uid_ptr: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(uid_ptr.write(&memory, env.process.identity().real_uid));
    Ok(Errno::Success)
}

/// ### `getuid()`
/// Returns the sandbox real user id using the POSIX import shape.
#[instrument(level = "trace", skip_all, ret)]
pub fn getuid(mut ctx: FunctionEnvMut<'_, WasiEnv>) -> Result<u32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    Ok(ctx.data().process.identity().real_uid)
}

/// ### `geteuid()`
/// Returns the sandbox effective user id using the POSIX import shape.
#[instrument(level = "trace", skip_all, ret)]
pub fn geteuid(mut ctx: FunctionEnvMut<'_, WasiEnv>) -> Result<u32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    Ok(ctx.data().process.identity().effective_uid)
}

/// ### `proc_getresuid()`
/// Returns the sandbox real, effective, and saved user ids.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_getresuid<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ruid_ptr: WasmPtr<u32, M>,
    euid_ptr: WasmPtr<u32, M>,
    suid_ptr: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let identity = env.process.identity();
    wasi_try_mem_ok!(ruid_ptr.write(&memory, identity.real_uid));
    wasi_try_mem_ok!(euid_ptr.write(&memory, identity.effective_uid));
    wasi_try_mem_ok!(suid_ptr.write(&memory, identity.saved_uid));
    Ok(Errno::Success)
}

/// ### `setuid()`
/// Updates all sandbox user ids using the POSIX import shape.
#[instrument(level = "trace", skip_all, fields(%uid), ret)]
pub fn setuid(mut ctx: FunctionEnvMut<'_, WasiEnv>, uid: u32) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let mut identity = env.process.identity();
    identity.real_uid = uid;
    identity.effective_uid = uid;
    identity.saved_uid = uid;
    env.process.set_identity(identity);
    Ok(Errno::Success)
}

/// ### `seteuid()`
/// Updates the sandbox effective user id using the POSIX import shape.
#[instrument(level = "trace", skip_all, fields(%uid), ret)]
pub fn seteuid(mut ctx: FunctionEnvMut<'_, WasiEnv>, uid: u32) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let mut identity = env.process.identity();
    identity.effective_uid = uid;
    env.process.set_identity(identity);
    Ok(Errno::Success)
}

/// ### `proc_getgid()`
/// Returns the sandbox group id.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_getgid<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    gid_ptr: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(gid_ptr.write(&memory, env.process.identity().real_gid));
    Ok(Errno::Success)
}

/// ### `getgid()`
/// Returns the sandbox real group id using the POSIX import shape.
#[instrument(level = "trace", skip_all, ret)]
pub fn getgid(mut ctx: FunctionEnvMut<'_, WasiEnv>) -> Result<u32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    Ok(ctx.data().process.identity().real_gid)
}

/// ### `getegid()`
/// Returns the sandbox effective group id using the POSIX import shape.
#[instrument(level = "trace", skip_all, ret)]
pub fn getegid(mut ctx: FunctionEnvMut<'_, WasiEnv>) -> Result<u32, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    Ok(ctx.data().process.identity().effective_gid)
}

/// ### `proc_getresgid()`
/// Returns the sandbox real, effective, and saved group ids.
#[instrument(level = "trace", skip_all, ret)]
pub fn proc_getresgid<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    rgid_ptr: WasmPtr<u32, M>,
    egid_ptr: WasmPtr<u32, M>,
    sgid_ptr: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let identity = env.process.identity();
    wasi_try_mem_ok!(rgid_ptr.write(&memory, identity.real_gid));
    wasi_try_mem_ok!(egid_ptr.write(&memory, identity.effective_gid));
    wasi_try_mem_ok!(sgid_ptr.write(&memory, identity.saved_gid));
    Ok(Errno::Success)
}

/// ### `setgid()`
/// Updates all sandbox group ids using the POSIX import shape.
#[instrument(level = "trace", skip_all, fields(%gid), ret)]
pub fn setgid(mut ctx: FunctionEnvMut<'_, WasiEnv>, gid: u32) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let mut identity = env.process.identity();
    identity.real_gid = gid;
    identity.effective_gid = gid;
    identity.saved_gid = gid;
    env.process.set_identity(identity);
    Ok(Errno::Success)
}

/// ### `setegid()`
/// Updates the sandbox effective group id using the POSIX import shape.
#[instrument(level = "trace", skip_all, fields(%gid), ret)]
pub fn setegid(mut ctx: FunctionEnvMut<'_, WasiEnv>, gid: u32) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let mut identity = env.process.identity();
    identity.effective_gid = gid;
    env.process.set_identity(identity);
    Ok(Errno::Success)
}

/// ### `proc_getgroups()`
/// Writes the sandbox supplementary groups and returns the group count.
#[instrument(level = "trace", skip_all, fields(%max_groups), ret)]
pub fn proc_getgroups<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    groups_ptr: WasmPtr<u32, M>,
    max_groups: M::Offset,
    count_ptr: WasmPtr<M::Offset, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let identity = env.process.identity();
    let count = identity.groups.len();
    let max_groups_usize = wasi_try_ok!(usize::try_from(max_groups.into()).map_err(|_| Errno::Overflow));
    if max_groups_usize > 0 {
        if max_groups_usize < count {
            return Ok(Errno::Inval);
        }
        for (index, group) in identity.groups.iter().enumerate() {
            let ptr = wasi_try_mem_ok!(groups_ptr.add_offset(wasi_try_ok!(to_offset::<M>(index))));
            wasi_try_mem_ok!(ptr.write(&memory, *group));
        }
    }
    wasi_try_mem_ok!(count_ptr.write(&memory, wasi_try_ok!(to_offset::<M>(count))));
    Ok(Errno::Success)
}

/// ### `proc_setresuid()`
/// Updates the sandbox real, effective, and saved user ids.
#[instrument(level = "trace", skip_all, fields(%ruid, %euid, %suid), ret)]
pub fn proc_setresuid(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    ruid: u32,
    euid: u32,
    suid: u32,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let mut identity = env.process.identity();
    if ruid != KEEP_ID {
        identity.real_uid = ruid;
    }
    if euid != KEEP_ID {
        identity.effective_uid = euid;
    }
    if suid != KEEP_ID {
        identity.saved_uid = suid;
    }
    env.process.set_identity(identity);
    Ok(Errno::Success)
}

/// ### `proc_setresgid()`
/// Updates the sandbox real, effective, and saved group ids.
#[instrument(level = "trace", skip_all, fields(%rgid, %egid, %sgid), ret)]
pub fn proc_setresgid(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    rgid: u32,
    egid: u32,
    sgid: u32,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let mut identity = env.process.identity();
    if rgid != KEEP_ID {
        identity.real_gid = rgid;
    }
    if egid != KEEP_ID {
        identity.effective_gid = egid;
    }
    if sgid != KEEP_ID {
        identity.saved_gid = sgid;
    }
    env.process.set_identity(identity);
    Ok(Errno::Success)
}

/// ### `proc_setgroups()`
/// Replaces the sandbox supplementary group list.
#[instrument(level = "trace", skip_all, fields(%group_count), ret)]
pub fn proc_setgroups<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    groups_ptr: WasmPtr<u32, M>,
    group_count: M::Offset,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let count = wasi_try_ok!(usize::try_from(group_count.into()).map_err(|_| Errno::Overflow));
    let mut groups = Vec::with_capacity(count);
    for index in 0..count {
        let ptr = wasi_try_mem_ok!(groups_ptr.add_offset(wasi_try_ok!(to_offset::<M>(index))));
        groups.push(wasi_try_mem_ok!(ptr.read(&memory)));
    }
    let mut identity = env.process.identity();
    identity.groups = groups;
    env.process.set_identity(identity);
    Ok(Errno::Success)
}
