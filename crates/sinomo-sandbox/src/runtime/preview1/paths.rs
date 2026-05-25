use crate::{
    SandboxMountMode, SandboxPath, SandboxPathError, SandboxWasiError, SandboxWasiPreopen,
};
use std::{fs, path::Path, path::PathBuf};

// Preview1 extensions are legacy host-staged helpers. Cluster-backed sandbox
// execution must use the WASIX VFS/SIFS mount path instead of these host paths.
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

pub(super) const SINOMO_WASI_ERRNO_SUCCESS: i32 = 0;
pub(super) const SINOMO_WASI_ERRNO_PERM: i32 = 1;
pub(super) const SINOMO_WASI_ERRNO_NOENT: i32 = 2;
pub(super) const SINOMO_WASI_ERRNO_IO: i32 = 5;
pub(super) const SINOMO_WASI_ERRNO_INVAL: i32 = 22;
pub(crate) const SINOMO_WASI_ERRNO_ROFS: i32 = 30;

pub(super) fn sandbox_wasi_extension_guest_path(
    path: &str,
    cwd: &SandboxPath,
) -> Result<SandboxPath, SandboxPathError> {
    if path.starts_with('/') {
        SandboxPath::new(path)
    } else if cwd.as_str() == "/" {
        SandboxPath::new(format!("/{path}"))
    } else {
        SandboxPath::new(format!("{}/{path}", cwd.as_str()))
    }
}

pub(super) fn sandbox_wasi_extension_host_path<'a>(
    path: &SandboxPath,
    preopened_dirs: &'a [SandboxWasiPreopen],
) -> Option<(&'a SandboxWasiPreopen, PathBuf)> {
    let mut matches: Vec<_> = preopened_dirs
        .iter()
        .filter(|preopen| {
            if preopen.guest_path.as_str() == "/" {
                true
            } else {
                path == &preopen.guest_path
                    || path
                        .as_str()
                        .strip_prefix(preopen.guest_path.as_str())
                        .is_some_and(|suffix| suffix.starts_with('/'))
            }
        })
        .collect();
    matches.sort_by_key(|preopen| std::cmp::Reverse(preopen.guest_path.as_str().len()));
    let preopen = matches.into_iter().next()?;
    let relative = if preopen.guest_path.as_str() == "/" {
        path.as_str().trim_start_matches('/')
    } else if path == &preopen.guest_path {
        ""
    } else {
        path.as_str()
            .strip_prefix(preopen.guest_path.as_str())
            .and_then(|suffix| suffix.strip_prefix('/'))?
    };
    Some((preopen, preopen.host_path.join(relative)))
}

#[cfg(unix)]
pub(super) fn sandbox_wasi_extension_host_path_mode(
    preopen_root: &Path,
    host_path: &Path,
) -> Result<u32, i32> {
    let root = fs::canonicalize(preopen_root).map_err(|_| SINOMO_WASI_ERRNO_NOENT)?;
    let target = fs::canonicalize(host_path).map_err(|_| SINOMO_WASI_ERRNO_NOENT)?;
    if !target.starts_with(&root) {
        return Err(SINOMO_WASI_ERRNO_PERM);
    }
    let metadata = fs::metadata(&target).map_err(|_| SINOMO_WASI_ERRNO_NOENT)?;
    Ok(metadata.permissions().mode() & 0o7777)
}

#[cfg(not(unix))]
pub(super) fn sandbox_wasi_extension_host_path_mode(
    _preopen_root: &Path,
    _host_path: &Path,
) -> Result<u32, i32> {
    Err(SINOMO_WASI_ERRNO_PERM)
}

pub(super) fn sandbox_wasi_extension_write_guest_file(
    path: &SandboxPath,
    preopened_dirs: &[SandboxWasiPreopen],
    bytes: &[u8],
    append: bool,
) -> Result<(), SandboxWasiError> {
    let Some((preopen, host_path)) = sandbox_wasi_extension_host_path(path, preopened_dirs) else {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("child stdout path is not mounted: {path}"),
        });
    };
    if preopen.mode != SandboxMountMode::ReadWrite {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("child stdout path is read-only: {path}"),
        });
    }
    sandbox_wasi_extension_write_host_path(&preopen.host_path, &host_path, bytes, append).map_err(
        |detail| SandboxWasiError::Runtime {
            detail: format!("write child stdout path {path}: {detail}"),
        },
    )
}

pub(super) fn sandbox_wasi_extension_validate_guest_file_write_target(
    path: &SandboxPath,
    preopened_dirs: &[SandboxWasiPreopen],
) -> Result<(), SandboxWasiError> {
    let Some((preopen, host_path)) = sandbox_wasi_extension_host_path(path, preopened_dirs) else {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("child output path is not mounted: {path}"),
        });
    };
    if preopen.mode != SandboxMountMode::ReadWrite {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("child output path is read-only: {path}"),
        });
    }
    sandbox_wasi_extension_validate_host_write_target(&preopen.host_path, &host_path).map_err(
        |detail| SandboxWasiError::Runtime {
            detail: format!("validate child output path {path}: {detail}"),
        },
    )
}

#[cfg(test)]
pub(crate) fn sandbox_wasi_extension_read_host_path(
    preopen_root: &Path,
    host_path: &Path,
) -> Result<Vec<u8>, String> {
    let root = fs::canonicalize(preopen_root).map_err(|error| error.to_string())?;
    let target = fs::canonicalize(host_path).map_err(|error| error.to_string())?;
    if !target.starts_with(&root) {
        return Err("path escapes sandbox preopen".to_string());
    }
    fs::read(&target).map_err(|error| error.to_string())
}

pub(super) fn sandbox_wasi_extension_read_host_path_with_limit(
    preopen_root: &Path,
    host_path: &Path,
    limit: usize,
) -> Result<Vec<u8>, SandboxWasiError> {
    let root = fs::canonicalize(preopen_root).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("resolve sandbox preopen: {error}"),
    })?;
    let link_metadata =
        fs::symlink_metadata(host_path).map_err(|error| SandboxWasiError::InvalidRequest {
            detail: format!("sandbox path is invalid: {error}"),
        })?;
    if link_metadata.file_type().is_symlink() {
        return Err(SandboxWasiError::InvalidRequest {
            detail: "sandbox path is a symlink".to_string(),
        });
    }
    let target = fs::canonicalize(host_path).map_err(|error| SandboxWasiError::InvalidRequest {
        detail: format!("sandbox path is invalid: {error}"),
    })?;
    if !target.starts_with(&root) {
        return Err(SandboxWasiError::InvalidRequest {
            detail: "path escapes sandbox preopen".to_string(),
        });
    }
    let metadata = fs::metadata(&target).map_err(|error| SandboxWasiError::InvalidRequest {
        detail: format!("sandbox path is invalid: {error}"),
    })?;
    if !metadata.is_file() {
        return Err(SandboxWasiError::InvalidRequest {
            detail: "sandbox path is not a regular file".to_string(),
        });
    }
    if metadata.len() > limit as u64 {
        return Err(SandboxWasiError::MemoryLimitExceeded { limit });
    }
    fs::read(&target).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("read sandbox path: {error}"),
    })
}

pub(super) fn sandbox_wasi_extension_read_guest_file_with_limit(
    path: &SandboxPath,
    preopened_dirs: &[SandboxWasiPreopen],
    limit: usize,
) -> Result<Vec<u8>, SandboxWasiError> {
    let Some((preopen, host_path)) = sandbox_wasi_extension_host_path(path, preopened_dirs) else {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("child stdin path is not mounted: {path}"),
        });
    };
    let root = fs::canonicalize(&preopen.host_path).map_err(|error| SandboxWasiError::Runtime {
        detail: format!(
            "resolve child stdin preopen {}: {error}",
            preopen.guest_path
        ),
    })?;
    let link_metadata =
        fs::symlink_metadata(&host_path).map_err(|error| SandboxWasiError::InvalidRequest {
            detail: format!("child stdin path `{path}` is invalid: {error}"),
        })?;
    if link_metadata.file_type().is_symlink() {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("child stdin path `{path}` is a symlink"),
        });
    }
    let target =
        fs::canonicalize(&host_path).map_err(|error| SandboxWasiError::InvalidRequest {
            detail: format!("child stdin path `{path}` is invalid: {error}"),
        })?;
    if !target.starts_with(&root) {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("child stdin path `{path}` escapes its sandbox preopen"),
        });
    }
    let metadata = fs::metadata(&target).map_err(|error| SandboxWasiError::InvalidRequest {
        detail: format!("child stdin path `{path}` is invalid: {error}"),
    })?;
    if !metadata.is_file() {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("child stdin path `{path}` is not a regular file"),
        });
    }
    if metadata.len() > limit as u64 {
        return Err(SandboxWasiError::InputLimitExceeded { limit });
    }
    fs::read(&target).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("read child stdin path {path}: {error}"),
    })
}

fn sandbox_wasi_extension_write_host_path(
    preopen_root: &Path,
    host_path: &Path,
    bytes: &[u8],
    append: bool,
) -> Result<(), String> {
    sandbox_wasi_extension_validate_host_write_target(preopen_root, host_path)?;
    let mut options = fs::OpenOptions::new();
    options.create(true).write(true);
    if append {
        options.append(true);
    } else {
        options.truncate(true);
    }
    let mut file = options.open(host_path).map_err(|error| error.to_string())?;
    std::io::Write::write_all(&mut file, bytes).map_err(|error| error.to_string())
}

fn sandbox_wasi_extension_validate_host_write_target(
    preopen_root: &Path,
    host_path: &Path,
) -> Result<(), String> {
    let root = fs::canonicalize(preopen_root).map_err(|error| error.to_string())?;
    let parent = host_path
        .parent()
        .ok_or_else(|| "path has no parent".to_string())?;
    let parent = fs::canonicalize(parent).map_err(|error| error.to_string())?;
    if !parent.starts_with(&root) {
        return Err("path escapes sandbox preopen".to_string());
    }
    match fs::symlink_metadata(host_path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                return Err("path escapes sandbox preopen".to_string());
            }
            if !metadata.is_file() {
                return Err("path is not a regular file".to_string());
            }
            let target = fs::canonicalize(host_path).map_err(|error| error.to_string())?;
            if !target.starts_with(&root) {
                return Err("path escapes sandbox preopen".to_string());
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.to_string()),
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn sandbox_wasi_extension_chmod_host_path(
    preopen_root: &Path,
    host_path: &Path,
    mode: u32,
) -> i32 {
    let Ok(root) = fs::canonicalize(preopen_root) else {
        return SINOMO_WASI_ERRNO_NOENT;
    };
    let Ok(target) = fs::canonicalize(host_path) else {
        return SINOMO_WASI_ERRNO_NOENT;
    };
    if !target.starts_with(&root) {
        return SINOMO_WASI_ERRNO_PERM;
    }
    let Ok(metadata) = fs::metadata(&target) else {
        return SINOMO_WASI_ERRNO_NOENT;
    };
    let mut permissions = metadata.permissions();
    permissions.set_mode((permissions.mode() & !0o7777) | (mode & 0o7777));
    match fs::set_permissions(&target, permissions) {
        Ok(()) => SINOMO_WASI_ERRNO_SUCCESS,
        Err(_) => SINOMO_WASI_ERRNO_IO,
    }
}

#[cfg(not(unix))]
pub(super) fn sandbox_wasi_extension_chmod_host_path(
    _preopen_root: &Path,
    _host_path: &Path,
    _mode: u32,
) -> i32 {
    SINOMO_WASI_ERRNO_PERM
}
