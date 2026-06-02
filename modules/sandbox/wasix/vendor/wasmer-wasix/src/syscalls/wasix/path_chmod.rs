use super::*;
use crate::VIRTUAL_ROOT_FD;
use crate::syscalls::*;
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

const AT_FDCWD_AS_WASI_FD: WasiFd = (-100i32) as WasiFd;
const POSIX_ID_UNCHANGED: u32 = u32::MAX;
static POSIX_MODE_OVERRIDES: LazyLock<Mutex<HashMap<u64, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static POSIX_OWNER_OVERRIDES: LazyLock<Mutex<HashMap<u64, (u32, u32)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static POSIX_TIME_OVERRIDES: LazyLock<Mutex<HashMap<u64, (u64, u64, u64)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// ### `path_chmod()`
/// Sets POSIX permission bits on a sandbox path backed by the host filesystem.
#[instrument(level = "trace", skip_all, fields(path = field::Empty, mode = %mode), ret)]
pub fn path_chmod<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    mode: u32,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let path = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path.as_str());

    Ok(path_chmod_internal(env, &path, mode))
}

/// ### `path_chmod_at()`
/// Sets POSIX permission bits on a sandbox path relative to a directory fd.
#[instrument(level = "trace", skip_all, fields(%fd, path = field::Empty, mode = %mode), ret)]
pub fn path_chmod_at<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    mode: u32,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let path = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path.as_str());

    Ok(path_chmod_at_internal(env, fd, &path, mode))
}

/// ### `path_mode()`
/// Returns POSIX permission bits for a sandbox path backed by the host filesystem.
#[instrument(level = "trace", skip_all, fields(path = field::Empty, mode = field::Empty), ret)]
pub fn path_mode<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    mode_ptr: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let path = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path.as_str());

    let mode = match path_mode_internal(env, &path) {
        Ok(mode) => mode,
        Err(err) => return Ok(err),
    };
    Span::current().record("mode", mode);
    wasi_try_mem_ok!(mode_ptr.write(&memory, mode));
    Ok(Errno::Success)
}

/// ### `path_mode_at()`
/// Returns POSIX permission bits for a sandbox path relative to a directory fd.
#[instrument(level = "trace", skip_all, fields(%fd, path = field::Empty, mode = field::Empty), ret)]
pub fn path_mode_at<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    mode_ptr: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let path = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path.as_str());

    let mode = match path_mode_at_internal(env, fd, &path) {
        Ok(mode) => mode,
        Err(err) => return Ok(err),
    };
    Span::current().record("mode", mode);
    wasi_try_mem_ok!(mode_ptr.write(&memory, mode));
    Ok(Errno::Success)
}

/// ### `fd_chmod()`
/// Sets POSIX permission bits on a sandbox file descriptor backed by the host filesystem.
#[instrument(level = "trace", skip_all, fields(%fd, mode = %mode), ret)]
pub fn fd_chmod(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    mode: u32,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    Ok(fd_chmod_internal(env, fd, mode))
}

/// ### `fd_mode()`
/// Returns POSIX permission bits for a sandbox file descriptor backed by the host filesystem.
#[instrument(level = "trace", skip_all, fields(%fd, mode = field::Empty), ret)]
pub fn fd_mode<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    mode_ptr: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let mode = match fd_mode_internal(env, fd) {
        Ok(mode) => mode,
        Err(err) => return Ok(err),
    };
    Span::current().record("mode", mode);
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(mode_ptr.write(&memory, mode));
    Ok(Errno::Success)
}

/// ### `path_chown()`
/// Applies sandbox ownership to an existing host-backed sandbox path.
#[instrument(level = "trace", skip_all, fields(path = field::Empty, %uid, %gid), ret)]
pub fn path_chown<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    uid: u32,
    gid: u32,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let path = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path.as_str());
    Ok(path_chown_at_internal(
        env,
        VIRTUAL_ROOT_FD,
        &path,
        uid,
        gid,
    ))
}

/// ### `path_chown_at()`
/// Applies sandbox ownership to an existing path relative to a directory fd.
#[instrument(level = "trace", skip_all, fields(%fd, path = field::Empty, %uid, %gid), ret)]
pub fn path_chown_at<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    uid: u32,
    gid: u32,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let path = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path.as_str());
    Ok(path_chown_at_internal(env, fd, &path, uid, gid))
}

/// ### `fd_chown()`
/// Applies sandbox ownership to an existing host-backed sandbox descriptor.
#[instrument(level = "trace", skip_all, fields(%fd, %uid, %gid), ret)]
pub fn fd_chown(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    uid: u32,
    gid: u32,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    Ok(fd_chown_internal(env, fd, uid, gid))
}

/// ### `path_owner_at()`
/// Returns sandbox uid/gid ownership for a path relative to a directory fd.
#[instrument(level = "trace", skip_all, fields(%fd, path = field::Empty, uid = field::Empty, gid = field::Empty), ret)]
pub fn path_owner_at<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    uid_ptr: WasmPtr<u32, M>,
    gid_ptr: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let path = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path.as_str());
    let (uid, gid) = match path_owner_at_internal(env, fd, &path) {
        Ok(owner) => owner,
        Err(err) => return Ok(err),
    };
    Span::current().record("uid", uid);
    Span::current().record("gid", gid);
    wasi_try_mem_ok!(uid_ptr.write(&memory, uid));
    wasi_try_mem_ok!(gid_ptr.write(&memory, gid));
    Ok(Errno::Success)
}

/// ### `fd_owner()`
/// Returns sandbox uid/gid ownership for a file descriptor.
#[instrument(level = "trace", skip_all, fields(%fd, uid = field::Empty, gid = field::Empty), ret)]
pub fn fd_owner<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    uid_ptr: WasmPtr<u32, M>,
    gid_ptr: WasmPtr<u32, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let (uid, gid) = match fd_owner_internal(env, fd) {
        Ok(owner) => owner,
        Err(err) => return Ok(err),
    };
    Span::current().record("uid", uid);
    Span::current().record("gid", gid);
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem_ok!(uid_ptr.write(&memory, uid));
    wasi_try_mem_ok!(gid_ptr.write(&memory, gid));
    Ok(Errno::Success)
}

/// ### `path_touch_at()`
/// Updates sandbox POSIX atime/mtime/ctime metadata for a path and optionally its parent.
#[instrument(level = "trace", skip_all, fields(%fd, path = field::Empty, %touch_self, %touch_parent), ret)]
pub fn path_touch_at<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    touch_self: u32,
    touch_parent: u32,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let path = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path.as_str());
    Ok(path_touch_at_internal(
        env,
        fd,
        &path,
        touch_self != 0,
        touch_parent != 0,
    ))
}

/// ### `fd_touch()`
/// Updates sandbox POSIX atime/mtime/ctime metadata for a file descriptor.
#[instrument(level = "trace", skip_all, fields(%fd), ret)]
pub fn fd_touch(mut ctx: FunctionEnvMut<'_, WasiEnv>, fd: WasiFd) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    Ok(fd_touch_internal(env, fd))
}

/// ### `path_times_at()`
/// Returns sandbox POSIX atime/mtime/ctime metadata for a path relative to a directory fd.
#[instrument(level = "trace", skip_all, fields(%fd, path = field::Empty), ret)]
pub fn path_times_at<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    atime_ptr: WasmPtr<u64, M>,
    mtime_ptr: WasmPtr<u64, M>,
    ctime_ptr: WasmPtr<u64, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let path = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path.as_str());
    let (atime, mtime, ctime) = match path_times_at_internal(env, fd, &path) {
        Ok(times) => times,
        Err(err) => return Ok(err),
    };
    wasi_try_mem_ok!(atime_ptr.write(&memory, atime));
    wasi_try_mem_ok!(mtime_ptr.write(&memory, mtime));
    wasi_try_mem_ok!(ctime_ptr.write(&memory, ctime));
    Ok(Errno::Success)
}

/// ### `path_symlink_at()`
/// Persists a POSIX symlink mapping for libc calls that must survive process boundaries.
#[instrument(level = "trace", skip_all, fields(%fd, path = field::Empty, target = field::Empty), ret)]
pub fn path_symlink_at<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    target: WasmPtr<u8, M>,
    target_len: M::Offset,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let path = unsafe { get_input_str_ok!(&memory, path, path_len) };
    let target = unsafe { get_input_str_ok!(&memory, target, target_len) };
    Span::current().record("path", path.as_str());
    Span::current().record("target", target.as_str());

    Ok(path_symlink_at_internal(env, fd, &path, &target))
}

/// ### `path_read_symlink_at()`
/// Reads a POSIX symlink mapping persisted by `path_symlink_at()`.
#[instrument(level = "trace", skip_all, fields(%fd, path = field::Empty), ret)]
pub fn path_read_symlink_at<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
    buf: WasmPtr<u8, M>,
    buf_len: M::Offset,
    buf_used: WasmPtr<M::Offset, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let path = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path.as_str());

    let target = match path_symlink_target_at_internal(env, fd, &path) {
        Ok(target) => target,
        Err(err) => return Ok(err),
    };
    let target = target.as_bytes();
    let buf_len: u64 = buf_len.into();
    let used = target.len().min(buf_len as usize);
    let buf = wasi_try_mem_ok!(buf.slice(&memory, wasi_try_ok!(to_offset::<M>(used))));
    wasi_try_mem_ok!(buf.write_slice(&target[..used]));
    let used: M::Offset = wasi_try_ok!(used.try_into().map_err(|_| Errno::Overflow));
    wasi_try_mem_ok!(buf_used.deref(&memory).write(used));
    Ok(Errno::Success)
}

/// ### `path_is_symlink_at()`
/// Tests whether a path is a POSIX symlink.
#[instrument(level = "trace", skip_all, fields(%fd, path = field::Empty), ret)]
pub fn path_is_symlink_at<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let path = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path.as_str());
    match path_symlink_target_at_internal(env, fd, &path) {
        Ok(_) => Ok(Errno::Success),
        Err(err) => Ok(err),
    }
}

/// ### `path_unlink_symlink_at()`
/// Removes a POSIX symlink.
#[instrument(level = "trace", skip_all, fields(%fd, path = field::Empty), ret)]
pub fn path_unlink_symlink_at<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let path = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path.as_str());
    Ok(path_unlink_symlink_at_internal(env, fd, &path))
}

fn path_chmod_internal(env: &WasiEnv, path: &str, mode: u32) -> Errno {
    path_chmod_at_internal(env, VIRTUAL_ROOT_FD, path, mode)
}

fn path_chmod_at_internal(env: &WasiEnv, fd: WasiFd, path: &str, mode: u32) -> Errno {
    if let Err(err) = validate_trailing_directory_path(env, fd, path) {
        return err;
    }
    if let Some(target) = resolve_symlink_path_at(env, fd, path) {
        return path_chmod_at_internal(env, VIRTUAL_ROOT_FD, &target, mode);
    }
    let mode = match validate_chmod_request(env, fd, path, mode) {
        Ok(mode) => mode,
        Err(err) => return err,
    };
    let inode = match sandbox_inode_at(env, fd, path) {
        Ok(inode) => inode,
        Err(err) => return err,
    };
    let Some(key) = mode_key_for_inode(&inode) else {
        return Errno::Notsup;
    };
    match env.state.fs.root_fs.set_mode(Path::new(&key), mode) {
        Ok(()) => {
            set_inode_mode(inode.ino().as_u64(), mode);
            Errno::Success
        }
        Err(error) => fs_error_into_errno(error),
    }
}

fn path_mode_internal(env: &WasiEnv, path: &str) -> Result<u32, Errno> {
    path_mode_at_internal(env, VIRTUAL_ROOT_FD, path)
}

fn path_mode_at_internal(env: &WasiEnv, fd: WasiFd, path: &str) -> Result<u32, Errno> {
    validate_trailing_directory_path(env, fd, path)?;
    let inode = sandbox_inode_at(env, fd, path)?;
    if let Some(mode) = inode_mode(inode.ino().as_u64()) {
        return Ok(mode);
    }
    if let Some(key) = mode_key_for_inode(&inode)
        && let Ok(metadata) = env.state.fs.root_fs.metadata(Path::new(&key))
        && metadata.mode() != 0
    {
        return Ok(metadata.mode() & 0o7777);
    }
    Ok(default_inode_mode(&inode))
}

fn fd_chmod_internal(env: &WasiEnv, fd: WasiFd, mode: u32) -> Errno {
    let inode = match fd_inode(env, fd) {
        Ok(inode) => inode,
        Err(err) => return err,
    };
    let mode = match validate_fd_chmod_request(env, &inode, mode) {
        Ok(mode) => mode,
        Err(err) => return err,
    };

    let open_file_handle = {
        let guard = inode.read();
        match guard.deref() {
            Kind::File {
                handle: Some(handle),
                ..
            } => Some(handle.clone()),
            _ => None,
        }
    };
    if let Some(handle) = open_file_handle {
        match handle
            .write()
            .map_err(|_| Errno::Io)
            .and_then(|mut handle| handle.set_mode(mode).map_err(fs_error_into_errno))
        {
            Ok(()) => {
                set_inode_mode(inode.ino().as_u64(), mode);
                return Errno::Success;
            }
            Err(err) => return err,
        }
    }

    if let Some(key) = mode_key_for_inode(&inode) {
        match env.state.fs.root_fs.set_mode(Path::new(&key), mode) {
            Ok(()) => {
                set_inode_mode(inode.ino().as_u64(), mode);
                return Errno::Success;
            }
            Err(error) => return fs_error_into_errno(error),
        }
    }
    Errno::Notsup
}

fn fd_mode_internal(env: &WasiEnv, fd: WasiFd) -> Result<u32, Errno> {
    let inode = fd_inode(env, fd)?;
    if let Some(mode) = inode_mode(inode.ino().as_u64()) {
        return Ok(mode);
    }
    if let Some(key) = mode_key_for_inode(&inode)
        && let Ok(metadata) = env.state.fs.root_fs.metadata(Path::new(&key))
        && metadata.mode() != 0
    {
        return Ok(metadata.mode() & 0o7777);
    }
    Ok(default_inode_mode(&inode))
}

fn path_chown_at_internal(env: &WasiEnv, fd: WasiFd, path: &str, uid: u32, gid: u32) -> Errno {
    if let Err(err) = validate_trailing_directory_path(env, fd, path) {
        return err;
    }
    if let Some(target) = resolve_symlink_path_at(env, fd, path) {
        return path_chown_at_internal(env, VIRTUAL_ROOT_FD, &target, uid, gid);
    }
    let inode = match sandbox_inode_at(env, fd, path) {
        Ok(inode) => inode,
        Err(err) => return err,
    };
    let key = mode_key_for_inode(&inode);
    let current = key
        .as_deref()
        .and_then(|key| owner_from_metadata(env, key))
        .or_else(|| inode_owner(inode.ino().as_u64()))
        .unwrap_or((0, 0));
    let (uid, gid) = match validate_chown_request(env, current, uid, gid) {
        Ok(owner) => owner,
        Err(err) => return err,
    };
    let Some(key) = mode_key_for_inode(&inode) else {
        return Errno::Notsup;
    };
    match env.state.fs.root_fs.set_owner(Path::new(&key), uid, gid) {
        Ok(()) => {
            set_inode_owner(inode.ino().as_u64(), uid, gid);
            Errno::Success
        }
        Err(error) => fs_error_into_errno(error),
    }
}

fn validate_trailing_directory_path(env: &WasiEnv, fd: WasiFd, path: &str) -> Result<(), Errno> {
    if !path_requires_directory(path) {
        return Ok(());
    }
    let key = key_for_path_at(env, fd, path).ok_or(Errno::Noent)?;
    match env.state.fs.root_fs.metadata(Path::new(&key)) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(_) => Err(Errno::Notdir),
        Err(error) => Err(fs_error_into_errno(error)),
    }
}

fn path_requires_directory(path: &str) -> bool {
    path.ends_with('/') && path != "/"
}

fn fd_chown_internal(env: &WasiEnv, fd: WasiFd, uid: u32, gid: u32) -> Errno {
    let inode = match fd_inode(env, fd) {
        Ok(inode) => inode,
        Err(err) => return err,
    };
    let current = fd_owner_for_chown(env, &inode);
    let (uid, gid) = match validate_chown_request(env, current, uid, gid) {
        Ok(owner) => owner,
        Err(err) => return err,
    };
    let open_file_handle = {
        let guard = inode.read();
        match guard.deref() {
            Kind::File {
                handle: Some(handle),
                ..
            } => Some(handle.clone()),
            _ => None,
        }
    };
    if let Some(handle) = open_file_handle {
        match handle
            .write()
            .map_err(|_| Errno::Io)
            .and_then(|mut handle| handle.set_owner(uid, gid).map_err(fs_error_into_errno))
        {
            Ok(()) => {
                set_inode_owner(inode.ino().as_u64(), uid, gid);
                return Errno::Success;
            }
            Err(err) => return err,
        }
    }
    if let Some(key) = mode_key_for_inode(&inode) {
        match env.state.fs.root_fs.set_owner(Path::new(&key), uid, gid) {
            Ok(()) => {
                set_inode_owner(inode.ino().as_u64(), uid, gid);
                return Errno::Success;
            }
            Err(error) => return fs_error_into_errno(error),
        }
    }
    Errno::Notsup
}

fn path_owner_at_internal(env: &WasiEnv, fd: WasiFd, path: &str) -> Result<(u32, u32), Errno> {
    let inode = sandbox_inode_at(env, fd, path)?;
    if let Some(owner) = inode_owner(inode.ino().as_u64()) {
        return Ok(owner);
    }
    if let Some(key) = mode_key_for_inode(&inode) {
        if let Some(owner) = owner_from_metadata(env, &key) {
            return Ok(owner);
        }
    }
    Ok((0, 0))
}

fn fd_owner_internal(env: &WasiEnv, fd: WasiFd) -> Result<(u32, u32), Errno> {
    let inode = fd_inode(env, fd)?;
    if let Some(owner) = inode_owner(inode.ino().as_u64()) {
        return Ok(owner);
    }
    if let Some(key) = mode_key_for_inode(&inode) {
        if let Some(owner) = owner_from_metadata(env, &key) {
            return Ok(owner);
        }
    }
    Ok((0, 0))
}

fn path_touch_at_internal(
    env: &WasiEnv,
    fd: WasiFd,
    path: &str,
    touch_self: bool,
    touch_parent: bool,
) -> Errno {
    if let Err(err) = validate_trailing_directory_path(env, fd, path) {
        return err;
    }
    let now = current_time_ns();
    let mut touched = false;
    if touch_self {
        match path_set_times_at(env, fd, path, true, now, now) {
            Ok(()) => touched = true,
            Err(Errno::Noent) => {}
            Err(err) => return err,
        }
    }
    if touch_parent {
        if let Some(parent) = parent_key_for_path(env, fd, path)
            && let Ok(parent_inode) = sandbox_inode_at(env, VIRTUAL_ROOT_FD, &parent)
        {
            match env
                .state
                .fs
                .root_fs
                .set_times(Path::new(&parent), Some(now), Some(now))
            {
                Ok(()) => {}
                Err(error) => return fs_error_into_errno(error),
            }
            set_inode_times(parent_inode.ino().as_u64(), now, now, now);
            touched = true;
        }
    }
    if touched {
        Errno::Success
    } else {
        Errno::Noent
    }
}

fn fd_touch_internal(env: &WasiEnv, fd: WasiFd) -> Errno {
    let now = current_time_ns();
    match fd_set_times(env, fd, now, now) {
        Ok(()) => Errno::Success,
        Err(err) => err,
    }
}

pub(crate) fn fd_set_times(
    env: &WasiEnv,
    fd: WasiFd,
    atime: u64,
    mtime: u64,
) -> Result<(), Errno> {
    let inode = fd_inode(env, fd)?;
    let open_file_handle = {
        let guard = inode.read();
        match guard.deref() {
            Kind::File {
                handle: Some(handle),
                ..
            } => Some(handle.clone()),
            _ => None,
        }
    };
    if let Some(handle) = open_file_handle {
        handle
            .write()
            .map_err(|_| Errno::Io)?
            .set_times(Some(atime), Some(mtime))
            .map_err(fs_error_into_errno)?;
        set_inode_times(inode.ino().as_u64(), atime, mtime, current_time_ns());
        return Ok(());
    }
    if let Some(key) = mode_key_for_inode(&inode) {
        match env
            .state
            .fs
            .root_fs
            .set_times(Path::new(&key), Some(atime), Some(mtime))
        {
            Ok(()) => {}
            Err(error) => return Err(fs_error_into_errno(error)),
        }
    } else {
        return Err(Errno::Notsup);
    }
    set_inode_times(inode.ino().as_u64(), atime, mtime, current_time_ns());
    Ok(())
}

fn path_times_at_internal(env: &WasiEnv, fd: WasiFd, path: &str) -> Result<(u64, u64, u64), Errno> {
    let inode = sandbox_inode_at(env, fd, path)?;
    if let Some(times) = inode_times(inode.ino().as_u64()) {
        return Ok(times);
    }
    if let Some(key) = mode_key_for_inode(&inode)
        && let Ok(metadata) = env.state.fs.root_fs.metadata(Path::new(&key))
    {
        return Ok((metadata.accessed(), metadata.modified(), metadata.created()));
    }
    let stat = inode.stat.read().map_err(|_| Errno::Io)?;
    Ok((stat.st_atim, stat.st_mtim, stat.st_ctim))
}

pub(crate) fn path_write_allowed_at(env: &WasiEnv, fd: WasiFd, path: &str) -> Result<bool, Errno> {
    let identity = env.process.identity();
    let mode = path_mode_at_internal(env, fd, path)?;
    let (uid, gid) = path_owner_at_internal(env, fd, path)?;
    Ok(identity_allows_mode(
        &identity, mode, uid, gid, 0o200, 0o020, 0o002,
    ))
}

pub(crate) fn path_parent_mutation_allowed_at(
    env: &WasiEnv,
    fd: WasiFd,
    path: &str,
) -> Result<bool, Errno> {
    let parent_path = Path::new(path).parent().ok_or(Errno::Noent)?;
    let parent_key = if parent_path.as_os_str().is_empty() {
        "."
    } else {
        parent_path.to_str().ok_or(Errno::Inval)?
    };
    let mode = path_mode_at_internal(env, fd, parent_key).unwrap_or(0o777);
    let (uid, gid) = path_owner_at_internal(env, fd, parent_key).unwrap_or((0, 0));
    let identity = env.process.identity();
    Ok(identity_allows_mode(
        &identity, mode, uid, gid, 0o300, 0o030, 0o003,
    ))
}

pub(crate) fn path_touch_parent_at(env: &WasiEnv, fd: WasiFd, path: &str) {
    if let Some(parent) = parent_key_for_mutation(env, fd, path) {
        let now = current_time_ns();
        if let Ok(inode) = sandbox_inode_at(env, VIRTUAL_ROOT_FD, &parent) {
            set_inode_times(inode.ino().as_u64(), now, now, now);
        }
        let _ = env
            .state
            .fs
            .root_fs
            .set_times(Path::new(&parent), Some(now), Some(now));
    }
}

pub(crate) fn path_set_times_at(
    env: &WasiEnv,
    fd: WasiFd,
    path: &str,
    follow_symlink: bool,
    atime: u64,
    mtime: u64,
) -> Result<(), Errno> {
    let result = key_for_path_at(env, fd, path).map(|key| {
        if follow_symlink {
            env.state
                .fs
                .root_fs
                .set_times(Path::new(&key), Some(atime), Some(mtime))
        } else {
            env.state
                .fs
                .root_fs
                .set_symlink_times(Path::new(&key), Some(atime), Some(mtime))
        }
    });
    if let Some(result) = result {
        match result {
            Ok(()) => {}
            Err(error) => return Err(fs_error_into_errno(error)),
        };
    } else {
        return Err(Errno::Notsup);
    }

    if let Ok(inode) = sandbox_inode_at_with_follow(env, fd, path, follow_symlink) {
        set_inode_times(inode.ino().as_u64(), atime, mtime, current_time_ns());
    }
    Ok(())
}

pub(crate) fn path_remove_metadata_at(env: &WasiEnv, fd: WasiFd, path: &str) {
    let _ = (env, fd, path);
}

pub(crate) fn path_register_hardlink_at(
    env: &WasiEnv,
    source_fd: WasiFd,
    source_path: &str,
    target_fd: WasiFd,
    target_path: &str,
) {
    let now = current_time_ns();
    if let Ok(inode) = sandbox_inode_at(env, source_fd, source_path) {
        set_inode_times(inode.ino().as_u64(), now, now, now);
    }
    if let Ok(inode) = sandbox_inode_at(env, target_fd, target_path) {
        set_inode_times(inode.ino().as_u64(), now, now, now);
    }

    if let Ok(mode) = path_mode_at_internal(env, source_fd, source_path) {
        if let Ok(inode) = sandbox_inode_at(env, target_fd, target_path) {
            set_inode_mode(inode.ino().as_u64(), mode);
        }
    }
    if let Ok((uid, gid)) = path_owner_at_internal(env, source_fd, source_path) {
        if (uid, gid) != (0, 0) {
            if let Some(target_key) = key_for_path_at(env, target_fd, target_path)
                && env
                    .state
                    .fs
                    .root_fs
                    .set_owner(Path::new(&target_key), uid, gid)
                    .is_ok()
                && let Ok(inode) = sandbox_inode_at(env, target_fd, target_path)
            {
                set_inode_owner(inode.ino().as_u64(), uid, gid);
            }
        }
    }
}

pub(crate) fn path_hardlink_stat_at(
    env: &WasiEnv,
    fd: WasiFd,
    path: &str,
    follow_symlink: bool,
) -> Option<(u64, u64)> {
    let key = key_for_path_at(env, fd, path)?;
    let metadata = if follow_symlink {
        env.state.fs.root_fs.metadata(Path::new(&key)).ok()?
    } else {
        env.state.fs.root_fs.symlink_metadata(Path::new(&key)).ok()?
    };
    Some((metadata.ino(), metadata.nlink().max(1)))
}

pub(crate) fn path_rename_metadata_at(
    env: &WasiEnv,
    source_fd: WasiFd,
    source_path: &str,
    target_fd: WasiFd,
    target_path: &str,
) {
    path_touch_parent_at(env, source_fd, source_path);
    path_touch_parent_at(env, target_fd, target_path);
}

pub(crate) fn path_rename_symlink_at(
    env: &WasiEnv,
    source_fd: WasiFd,
    source_path: &str,
    target_fd: WasiFd,
    target_path: &str,
) -> Errno {
    let Some(source_key) = key_for_path_at(env, source_fd, source_path) else {
        return Errno::Noent;
    };
    let Some(target_key) = key_for_path_at(env, target_fd, target_path) else {
        return Errno::Noent;
    };
    let Ok(source_metadata) = env
        .state
        .fs
        .root_fs
        .symlink_metadata(Path::new(&source_key))
    else {
        return Errno::Noent;
    };
    if !source_metadata.file_type().is_symlink() {
        return Errno::Noent;
    }
    if env
        .state
        .fs
        .root_fs
        .symlink_metadata(Path::new(&target_key))
        .is_ok()
    {
        return Errno::Exist;
    }
    if env
        .state
        .fs
        .root_fs
        .readlink(Path::new(&source_key))
        .is_err()
    {
        return Errno::Noent;
    }
    if let Err(error) = block_on(
        env.state
            .fs
            .root_fs
            .rename(Path::new(&source_key), Path::new(&target_key)),
    ) {
        return fs_error_into_errno(error);
    }
    path_touch_parent_at(env, source_fd, source_path);
    path_touch_parent_at(env, target_fd, target_path);
    Errno::Success
}

fn identity_allows_mode(
    identity: &crate::os::task::process::ProcessIdentity,
    mode: u32,
    uid: u32,
    gid: u32,
    owner_mask: u32,
    group_mask: u32,
    other_mask: u32,
) -> bool {
    if identity.effective_uid == 0 {
        return true;
    }
    if identity.effective_uid == uid {
        return mode & owner_mask == owner_mask;
    }
    if identity.effective_gid == gid || identity.groups.iter().any(|group| *group == gid) {
        return mode & group_mask == group_mask;
    }
    mode & other_mask == other_mask
}

fn validate_chmod_request(
    env: &WasiEnv,
    fd: WasiFd,
    path: &str,
    requested_mode: u32,
) -> Result<u32, Errno> {
    let identity = env.process.identity();
    if identity.effective_uid == 0 {
        return Ok(requested_mode);
    }
    let (uid, gid) = path_owner_at_internal(env, fd, path)?;
    if identity.effective_uid != uid {
        return Err(Errno::Perm);
    }
    let mut mode = requested_mode;
    let group_matches =
        identity.effective_gid == gid || identity.groups.iter().any(|group| *group == gid);
    if !group_matches {
        mode &= !0o2000;
    }
    Ok(mode)
}

fn validate_fd_chmod_request(
    env: &WasiEnv,
    inode: &crate::fs::InodeGuard,
    requested_mode: u32,
) -> Result<u32, Errno> {
    let identity = env.process.identity();
    if identity.effective_uid == 0 {
        return Ok(requested_mode);
    }
    let (uid, gid) = fd_owner_for_chown(env, inode);
    if identity.effective_uid != uid {
        return Err(Errno::Perm);
    }
    let mut mode = requested_mode;
    let group_matches =
        identity.effective_gid == gid || identity.groups.iter().any(|group| *group == gid);
    if !group_matches {
        mode &= !0o2000;
    }
    Ok(mode)
}

fn fd_owner_for_chown(env: &WasiEnv, inode: &crate::fs::InodeGuard) -> (u32, u32) {
    inode_owner(inode.ino().as_u64())
        .or_else(|| {
            mode_key_for_inode(inode)
                .as_deref()
                .and_then(|key| owner_from_metadata(env, key))
        })
        .unwrap_or((0, 0))
}

fn validate_chown_request(
    env: &WasiEnv,
    current: (u32, u32),
    requested_uid: u32,
    requested_gid: u32,
) -> Result<(u32, u32), Errno> {
    if requested_uid == POSIX_ID_UNCHANGED && requested_gid == POSIX_ID_UNCHANGED {
        return Ok(current);
    }
    let identity = env.process.identity();
    let next_uid = if requested_uid == POSIX_ID_UNCHANGED {
        current.0
    } else {
        requested_uid
    };
    let next_gid = if requested_gid == POSIX_ID_UNCHANGED {
        current.1
    } else {
        requested_gid
    };
    if identity.effective_uid == 0 {
        return Ok((next_uid, next_gid));
    }
    if identity.effective_uid != current.0 {
        return Err(Errno::Perm);
    }
    if requested_uid != POSIX_ID_UNCHANGED && requested_uid != identity.effective_uid {
        return Err(Errno::Perm);
    }
    let group_allowed = requested_gid == POSIX_ID_UNCHANGED
        || requested_gid == identity.effective_gid
        || identity.groups.iter().any(|group| *group == requested_gid);
    if !group_allowed {
        return Err(Errno::Perm);
    }
    Ok((next_uid, next_gid))
}

fn resolve_symlink_path_at(env: &WasiEnv, fd: WasiFd, path: &str) -> Option<String> {
    let target = path_symlink_target_at_internal(env, fd, path).ok()?;
    if target.starts_with('/') {
        return Some(normalize_absolute_virtual_path(&target));
    }
    let key = key_for_path_at(env, fd, path)?;
    let parent = Path::new(&key).parent()?;
    Some(normalize_relative_virtual_path(
        &normalize_absolute_virtual_path(&parent.to_string_lossy()),
        &target,
    ))
}

fn path_symlink_at_internal(env: &WasiEnv, fd: WasiFd, path: &str, target: &str) -> Errno {
    if path.is_empty() {
        return Errno::Noent;
    }
    match path_parent_mutation_allowed_at(env, fd, path) {
        Ok(true) => {}
        Ok(false) => return Errno::Access,
        Err(err) => return err,
    }

    let link_path = match new_child_host_path_at(env, fd, path) {
        Ok(path) => path,
        Err(err) => return err,
    };
    if env.state.fs.root_fs.symlink_metadata(&link_path).is_ok() {
        return Errno::Exist;
    }
    if let Err(error) = env.state.fs.root_fs.symlink(Path::new(target), &link_path) {
        return fs_error_into_errno(error);
    }
    if let Some(parent) = link_path.parent() {
        let parent = normalize_absolute_virtual_path(&parent.to_string_lossy());
        let now = current_time_ns();
        let _ = env
            .state
            .fs
            .root_fs
            .set_times(Path::new(&parent), Some(now), Some(now));
    }
    Errno::Success
}

fn path_symlink_target_at_internal(env: &WasiEnv, fd: WasiFd, path: &str) -> Result<String, Errno> {
    let key =
        sandbox_host_path_at(env, fd, path).or_else(|_| new_child_host_path_at(env, fd, path))?;
    env.state
        .fs
        .root_fs
        .readlink(&key)
        .map(|target| target.to_string_lossy().into_owned())
        .map_err(fs_error_into_errno)
}

fn path_unlink_symlink_at_internal(env: &WasiEnv, fd: WasiFd, path: &str) -> Errno {
    match path_parent_mutation_allowed_at(env, fd, path) {
        Ok(true) => {}
        Ok(false) => return Errno::Access,
        Err(err) => return err,
    }

    let link_path = match sandbox_host_path_at(env, fd, path)
        .or_else(|_| new_child_host_path_at(env, fd, path))
    {
        Ok(path) => path,
        Err(err) => return err,
    };
    if let Err(error) = env.state.fs.root_fs.remove_file(&link_path) {
        return fs_error_into_errno(error);
    }
    if let Some(parent) = link_path.parent() {
        let now = current_time_ns();
        let parent = normalize_absolute_virtual_path(&parent.to_string_lossy());
        let _ = env
            .state
            .fs
            .root_fs
            .set_times(Path::new(&parent), Some(now), Some(now));
    }
    Errno::Success
}

fn sandbox_host_path(env: &WasiEnv, path: &str) -> Result<PathBuf, Errno> {
    sandbox_host_path_at(env, VIRTUAL_ROOT_FD, path)
}

pub(crate) fn sandbox_host_path_at(
    env: &WasiEnv,
    fd: WasiFd,
    path: &str,
) -> Result<PathBuf, Errno> {
    let inode = sandbox_inode_at(env, fd, path)?;
    inode_host_path(&inode)
}

fn new_child_host_path_at(env: &WasiEnv, fd: WasiFd, path: &str) -> Result<PathBuf, Errno> {
    let path = Path::new(path);
    let parent_arg = path.parent().unwrap_or_else(|| Path::new(""));
    let name = path.file_name().ok_or(Errno::Noent)?;
    let parent = if parent_arg.as_os_str().is_empty() {
        fd_host_path(env, fd)?
    } else {
        sandbox_host_path_at(env, fd, &parent_arg.to_string_lossy())?
    };
    Ok(parent.join(name))
}

fn fs_error_into_errno(error: virtual_fs::FsError) -> Errno {
    let error: std::io::Error = error.into();
    match error.kind() {
        std::io::ErrorKind::AlreadyExists => Errno::Exist,
        std::io::ErrorKind::NotFound => Errno::Noent,
        std::io::ErrorKind::PermissionDenied => Errno::Perm,
        std::io::ErrorKind::InvalidInput => Errno::Inval,
        std::io::ErrorKind::NotADirectory => Errno::Notdir,
        std::io::ErrorKind::IsADirectory => Errno::Isdir,
        std::io::ErrorKind::Unsupported => Errno::Notsup,
        _ => Errno::Io,
    }
}

fn inode_host_path(inode: &crate::fs::InodeGuard) -> Result<PathBuf, Errno> {
    let guard = inode.read();
    match guard.deref() {
        Kind::File { path, .. } | Kind::Dir { path, .. } => Ok(path.clone()),
        Kind::Symlink {
            path_to_symlink, ..
        } => Ok(path_to_symlink.clone()),
        Kind::Root { .. } => Err(Errno::Access),
        _ => Err(Errno::Notsup),
    }
}

fn sandbox_inode_at(env: &WasiEnv, fd: WasiFd, path: &str) -> Result<crate::fs::InodeGuard, Errno> {
    sandbox_inode_at_with_follow(env, fd, path, true)
}

fn sandbox_inode_at_with_follow(
    env: &WasiEnv,
    fd: WasiFd,
    path: &str,
    follow_symlink: bool,
) -> Result<crate::fs::InodeGuard, Errno> {
    if path.is_empty() {
        return Err(Errno::Noent);
    }

    let state = &env.state;
    let path = if path.starts_with('/') {
        normalize_absolute_virtual_path(path)
    } else if fd == VIRTUAL_ROOT_FD || fd == AT_FDCWD_AS_WASI_FD {
        normalize_absolute_virtual_path(&state.fs.relative_path_to_absolute(path.to_string()))
    } else {
        path.to_string()
    };
    let fd = if fd == AT_FDCWD_AS_WASI_FD {
        VIRTUAL_ROOT_FD
    } else {
        fd
    };
    let inode = match state
        .fs
        .get_inode_at_path(&state.inodes, fd, &path, follow_symlink)
    {
        Ok(inode) => inode,
        Err(err) if !path.starts_with('/') && fd != VIRTUAL_ROOT_FD => {
            let _ = sandbox_fd_relative_host_path(env, fd, &path).ok_or(err)?;
            return Err(err);
        }
        Err(err) => return Err(err),
    };
    Ok(inode)
}

fn fd_host_path(env: &WasiEnv, fd: WasiFd) -> Result<PathBuf, Errno> {
    let inode = env.state.fs.get_fd_inode(fd)?;
    let guard = inode.read();
    match guard.deref() {
        Kind::File { path, .. } | Kind::Dir { path, .. } => Ok(path.clone()),
        Kind::Symlink {
            path_to_symlink, ..
        } => Ok(path_to_symlink.clone()),
        Kind::Root { .. } => Err(Errno::Access),
        _ => Err(Errno::Notsup),
    }
}

fn fd_inode(env: &WasiEnv, fd: WasiFd) -> Result<crate::fs::InodeGuard, Errno> {
    env.state.fs.get_fd_inode(fd)
}

fn inode_mode(ino: u64) -> Option<u32> {
    POSIX_MODE_OVERRIDES.lock().ok()?.get(&ino).copied()
}

pub(crate) fn set_inode_mode(ino: u64, mode: u32) {
    if let Ok(mut modes) = POSIX_MODE_OVERRIDES.lock() {
        modes.insert(ino, mode & 0o7777);
    }
}

fn inode_owner(ino: u64) -> Option<(u32, u32)> {
    POSIX_OWNER_OVERRIDES.lock().ok()?.get(&ino).copied()
}

pub(crate) fn set_inode_owner(ino: u64, uid: u32, gid: u32) {
    if let Ok(mut owners) = POSIX_OWNER_OVERRIDES.lock() {
        owners.insert(ino, (uid, gid));
    }
}

fn inode_times(ino: u64) -> Option<(u64, u64, u64)> {
    POSIX_TIME_OVERRIDES.lock().ok()?.get(&ino).copied()
}

pub(crate) fn set_inode_times(ino: u64, atime: u64, mtime: u64, ctime: u64) {
    if let Ok(mut times) = POSIX_TIME_OVERRIDES.lock() {
        times.insert(ino, (atime, mtime, ctime));
    }
}

fn default_inode_mode(inode: &crate::fs::InodeGuard) -> u32 {
    let guard = inode.read();
    match guard.deref() {
        Kind::Dir { .. } | Kind::Root { .. } => 0o777,
        Kind::File { .. } => 0o666,
        _ => 0o666,
    }
}

fn mode_key_for_inode(inode: &crate::fs::InodeGuard) -> Option<String> {
    let guard = inode.read();
    match guard.deref() {
        Kind::File { path, .. } | Kind::Dir { path, .. } => Some(normalize_mode_key(path)),
        Kind::Root { .. } => Some("/".to_string()),
        _ => None,
    }
}

fn owner_from_metadata(env: &WasiEnv, key: &str) -> Option<(u32, u32)> {
    let metadata = env.state.fs.root_fs.metadata(Path::new(key)).ok()?;
    Some((metadata.uid(), metadata.gid()))
}

fn normalize_mode_key(path: &Path) -> String {
    normalize_absolute_virtual_path(&path.to_string_lossy())
}

fn parent_key_for_path(env: &WasiEnv, fd: WasiFd, path: &str) -> Option<String> {
    let inode = sandbox_inode_at(env, fd, path).ok()?;
    let key = mode_key_for_inode(&inode)?;
    let parent = Path::new(&key).parent()?;
    Some(normalize_absolute_virtual_path(&parent.to_string_lossy()))
}

fn parent_key_for_mutation(env: &WasiEnv, fd: WasiFd, path: &str) -> Option<String> {
    let key = key_for_path_at(env, fd, path)?;
    let parent = Path::new(&key).parent()?;
    Some(normalize_absolute_virtual_path(&parent.to_string_lossy()))
}

pub(crate) fn key_for_path_at(env: &WasiEnv, fd: WasiFd, path: &str) -> Option<String> {
    if path.is_empty() {
        return None;
    }
    if path.starts_with('/') {
        return Some(normalize_absolute_virtual_path(path));
    }
    if fd == VIRTUAL_ROOT_FD || fd == AT_FDCWD_AS_WASI_FD {
        return Some(normalize_absolute_virtual_path(
            &env.state.fs.relative_path_to_absolute(path.to_string()),
        ));
    }
    let inode = env.state.fs.get_fd_inode(fd).ok()?;
    let guard = inode.read();
    let base = match guard.deref() {
        Kind::Dir { path, .. } => normalize_mode_key(path),
        Kind::File { path, .. } => {
            normalize_absolute_virtual_path(&path.parent()?.to_string_lossy())
        }
        Kind::Root { .. } => "/".to_string(),
        _ => return None,
    };
    Some(normalize_relative_virtual_path(&base, path))
}

fn current_time_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn sandbox_fd_relative_host_path(env: &WasiEnv, fd: WasiFd, path: &str) -> Option<PathBuf> {
    let inode = env.state.fs.get_fd_inode(fd).ok()?;
    let guard = inode.read();
    let base = match guard.deref() {
        Kind::Dir { path, .. } => path.clone(),
        Kind::File { path, .. } => path.parent()?.to_path_buf(),
        _ => return None,
    };
    normalize_relative_host_path(&base, path)
}

fn normalize_relative_host_path(base: &Path, path: &str) -> Option<PathBuf> {
    let mut normalized = base.to_path_buf();
    for component in Path::new(path).components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
            Component::Normal(part) => normalized.push(part),
            Component::Prefix(_) => return None,
        }
    }
    Some(normalized)
}

fn normalize_absolute_virtual_path(path: &str) -> String {
    let mut normalized = PathBuf::from("/");
    for component in Path::new(path).components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
            Component::Prefix(_) => {}
        }
    }

    let normalized = normalized.to_string_lossy();
    if normalized.is_empty() {
        "/".to_string()
    } else {
        normalized.into_owned()
    }
}

fn normalize_relative_virtual_path(base: &str, path: &str) -> String {
    let mut normalized = PathBuf::from(normalize_absolute_virtual_path(base));
    for component in Path::new(path).components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
            Component::Prefix(_) => {}
        }
    }
    normalize_absolute_virtual_path(&normalized.to_string_lossy())
}
