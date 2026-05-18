use super::*;
use crate::syscalls::*;
use crate::VIRTUAL_ROOT_FD;
use std::path::{Component, Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

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

/// ### `fd_chmod()`
/// Sets POSIX permission bits on a sandbox file descriptor backed by the host filesystem.
#[instrument(level = "trace", skip_all, fields(%fd, mode = %mode), ret)]
pub fn fd_chmod(mut ctx: FunctionEnvMut<'_, WasiEnv>, fd: WasiFd, mode: u32) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    Ok(match fd_host_path(env, fd) {
        Ok(path) => chmod_host_path(&path, mode),
        Err(err) => err,
    })
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
    let mode = match fd_host_path(env, fd).and_then(|path| host_path_mode(&path)) {
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
    Ok(match sandbox_host_path(env, &path) {
        Ok(_) => Errno::Success,
        Err(err) => err,
    })
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
    Ok(match fd_host_path(env, fd) {
        Ok(_) => Errno::Success,
        Err(err) => err,
    })
}

fn path_chmod_internal(env: &WasiEnv, path: &str, mode: u32) -> Errno {
    let host_path = match sandbox_host_path(env, path) {
        Ok(host_path) => host_path,
        Err(err) => return err,
    };

    chmod_host_path(&host_path, mode)
}

fn path_mode_internal(env: &WasiEnv, path: &str) -> Result<u32, Errno> {
    let host_path = sandbox_host_path(env, path)?;
    host_path_mode(&host_path)
}

fn sandbox_host_path(env: &WasiEnv, path: &str) -> Result<PathBuf, Errno> {
    if path.is_empty() {
        return Err(Errno::Noent);
    }

    let state = &env.state;
    let absolute = normalize_absolute_virtual_path(&state.fs.relative_path_to_absolute(path.to_string()));
    let inode = state
        .fs
        .get_inode_at_path(&state.inodes, VIRTUAL_ROOT_FD, &absolute, true)?;
    let guard = inode.read();
    match guard.deref() {
        Kind::File { path, .. } | Kind::Dir { path, .. } => Ok(path.clone()),
        Kind::Root { .. } => Err(Errno::Access),
        _ => Err(Errno::Notsup),
    }
}

fn fd_host_path(env: &WasiEnv, fd: WasiFd) -> Result<PathBuf, Errno> {
    let inode = env.state.fs.get_fd_inode(fd)?;
    let guard = inode.read();
    match guard.deref() {
        Kind::File { path, .. } | Kind::Dir { path, .. } => Ok(path.clone()),
        Kind::Root { .. } => Err(Errno::Access),
        _ => Err(Errno::Notsup),
    }
}

#[cfg(unix)]
pub(crate) fn chmod_host_path(path: &Path, mode: u32) -> Errno {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Errno::Noent,
        Err(_) => return Errno::Io,
    };
    let mut permissions = metadata.permissions();
    permissions.set_mode((permissions.mode() & !0o7777) | (mode & 0o7777));
    match std::fs::set_permissions(path, permissions) {
        Ok(()) => Errno::Success,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Errno::Noent,
        Err(_) => Errno::Io,
    }
}

#[cfg(not(unix))]
pub(crate) fn chmod_host_path(_path: &Path, _mode: u32) -> Errno {
    Errno::Perm
}

#[cfg(unix)]
fn host_path_mode(path: &Path) -> Result<u32, Errno> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            Errno::Noent
        } else {
            Errno::Io
        }
    })?;
    Ok(metadata.permissions().mode() & 0o7777)
}

#[cfg(not(unix))]
fn host_path_mode(_path: &Path) -> Result<u32, Errno> {
    Err(Errno::Perm)
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
