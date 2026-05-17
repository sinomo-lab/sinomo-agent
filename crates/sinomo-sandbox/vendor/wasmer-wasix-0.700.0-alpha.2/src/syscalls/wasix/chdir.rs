use super::*;
use crate::fs::InodeGuard;
use crate::VIRTUAL_ROOT_FD;
use crate::syscalls::*;
use std::path::{Component, Path, PathBuf};

/// ### `chdir()`
/// Sets the current working directory
#[instrument(level = "trace", skip_all, fields(name = field::Empty), ret)]
pub fn chdir<M: MemorySize>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    path: WasmPtr<u8, M>,
    path_len: M::Offset,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let (memory, mut state) = unsafe { env.get_memory_and_wasi_state(&ctx, 0) };
    let path = unsafe { get_input_str_ok!(&memory, path, path_len) };
    Span::current().record("path", path.as_str());

    wasi_try_ok!(chdir_internal(ctx.data(), &path));
    let env = ctx.data();

    #[cfg(feature = "journal")]
    if env.enable_journal {
        JournalEffector::save_chdir(&mut ctx, path).map_err(|err| {
            tracing::error!("failed to chdir event - {}", err);
            WasiError::Exit(ExitCode::from(Errno::Fault))
        })?;
    }

    Ok(Errno::Success)
}

pub fn chdir_internal(env: &WasiEnv, path: &str) -> Result<(), Errno> {
    if path.is_empty() {
        return Err(Errno::Noent);
    }

    let state = &env.state;
    let path = normalize_absolute_virtual_path(&state.fs.relative_path_to_absolute(path.to_string()));

    let inode = state
        .fs
        .get_inode_at_path(&state.inodes, VIRTUAL_ROOT_FD, &path, true)?;
    let guard = inode.read();
    match guard.deref() {
        Kind::Root { .. } | Kind::Dir { .. } => {}
        _ => return Err(Errno::Notdir),
    }
    drop(guard);

    state.fs.set_current_dir(&path);
    sync_process_metadata(env);
    Ok(())
}

/// ### `fchdir()`
/// Sets the current working directory from an open directory descriptor.
#[instrument(level = "trace", skip_all, fields(%fd), ret)]
pub fn fchdir(mut ctx: FunctionEnvMut<'_, WasiEnv>, fd: WasiFd) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    let env = ctx.data();
    let path = wasi_try_ok!(directory_path_from_fd(env, fd));
    state_set_current_dir_and_sync(env, &path);

    #[cfg(feature = "journal")]
    if env.enable_journal {
        JournalEffector::save_chdir(&mut ctx, path).map_err(|err| {
            tracing::error!("failed to fchdir event - {}", err);
            WasiError::Exit(ExitCode::from(Errno::Fault))
        })?;
    }

    Ok(Errno::Success)
}

fn directory_path_from_fd(env: &WasiEnv, fd: WasiFd) -> Result<String, Errno> {
    let inode = env.state.fs.get_fd_inode(fd)?;
    directory_virtual_path(&inode)
}

fn directory_virtual_path(inode: &InodeGuard) -> Result<String, Errno> {
    let mut parts = Vec::new();
    let mut current = inode.clone();

    loop {
        let guard = current.read();
        match guard.deref() {
            Kind::Root { .. } => break,
            Kind::Dir { parent, .. } => {
                let name = current.name.read().unwrap().to_string();
                parts.push(name);
                let Some(parent) = parent.upgrade() else {
                    return Err(Errno::Noent);
                };
                drop(guard);
                current = parent;
            }
            _ => return Err(Errno::Notdir),
        }
    }

    let mut normalized = PathBuf::from("/");
    for part in parts.iter().rev() {
        for component in Path::new(part).components() {
            match component {
                Component::RootDir | Component::CurDir => {}
                Component::ParentDir => {
                    normalized.pop();
                }
                Component::Normal(part) => normalized.push(part),
                Component::Prefix(_) => {}
            }
        }
    }

    let normalized = normalized.to_string_lossy();
    if normalized.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(normalized.into_owned())
    }
}

fn state_set_current_dir_and_sync(env: &WasiEnv, path: &str) {
    env.state.fs.set_current_dir(path);
    sync_process_metadata(env);
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
