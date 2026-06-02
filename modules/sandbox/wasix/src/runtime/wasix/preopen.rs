use crate::{
    SandboxMountMode, SandboxPath, SandboxWasiError, SandboxWasiPreopen, SandboxWasiVirtualPreopen,
};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub(super) fn validate_wasix_current_dir(
    cwd: &SandboxPath,
    preopened_dirs: &[SandboxWasiPreopen],
    virtual_preopened_dirs: &[SandboxWasiVirtualPreopen],
) -> Result<(), SandboxWasiError> {
    if preopened_dirs.is_empty() && virtual_preopened_dirs.is_empty() {
        return Ok(());
    }
    if let Some((preopen, virtual_path)) = wasix_virtual_path_for_guest(cwd, virtual_preopened_dirs)
    {
        let metadata = preopen.fs.metadata(&virtual_path).map_err(|error| {
            SandboxWasiError::InvalidRequest {
                detail: format!("WASIX cwd `{cwd}` is invalid: {error}"),
            }
        })?;
        if !metadata.is_dir() {
            return Err(SandboxWasiError::InvalidRequest {
                detail: format!("WASIX cwd `{cwd}` is not a directory"),
            });
        }
        return Ok(());
    }
    let Some((preopen, host_path)) = wasix_host_path_for_guest(cwd, preopened_dirs) else {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("WASIX cwd `{cwd}` is not mounted"),
        });
    };
    let root = fs::canonicalize(&preopen.host_path).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("resolve WASIX cwd preopen {}: {error}", preopen.guest_path),
    })?;
    let target =
        fs::canonicalize(&host_path).map_err(|error| SandboxWasiError::InvalidRequest {
            detail: format!("WASIX cwd `{cwd}` is invalid: {error}"),
        })?;
    if !target.starts_with(&root) {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("WASIX cwd `{cwd}` escapes its sandbox preopen"),
        });
    }
    let metadata = fs::metadata(&target).map_err(|error| SandboxWasiError::InvalidRequest {
        detail: format!("WASIX cwd `{cwd}` is invalid: {error}"),
    })?;
    if !metadata.is_dir() {
        return Err(SandboxWasiError::InvalidRequest {
            detail: format!("WASIX cwd `{cwd}` is not a directory"),
        });
    }
    Ok(())
}

fn sandbox_path_is_inside_preopen(path: &SandboxPath, preopen: &SandboxPath) -> bool {
    preopen.as_str() == "/"
        || path == preopen
        || path
            .as_str()
            .strip_prefix(preopen.as_str())
            .is_some_and(|suffix| suffix.starts_with('/'))
}

pub(super) fn validate_wasix_preopen_tree(
    preopen: &SandboxWasiPreopen,
) -> Result<(), SandboxWasiError> {
    collect_wasix_preopen_tree(&preopen.host_path).map_err(|error| SandboxWasiError::Runtime {
        detail: format!("validate WASIX preopen {}: {error}", preopen.guest_path),
    })?;
    Ok(())
}

pub(super) fn validate_wasix_pinned_child_exec_preopens(
    preopened_dirs: &[SandboxWasiPreopen],
) -> Result<(), SandboxWasiError> {
    let bin_preopen = preopened_dirs
        .iter()
        .find(|preopen| preopen.guest_path.as_str() == "/bin");
    if bin_preopen.is_some_and(|preopen| preopen.mode != SandboxMountMode::ReadOnly) {
        return Err(SandboxWasiError::InvalidRequest {
            detail: "WASIX pinned child exec requires /bin to be a read-only preopen".to_string(),
        });
    }

    if preopened_dirs.iter().any(|preopen| {
        preopen.guest_path.as_str() == "/" && preopen.mode != SandboxMountMode::ReadOnly
    }) && bin_preopen.is_none()
    {
        return Err(SandboxWasiError::InvalidRequest {
            detail:
                "WASIX pinned child exec requires a writable root preopen to be shadowed by a read-only /bin preopen"
                    .to_string(),
        });
    }
    Ok(())
}

fn collect_wasix_preopen_tree(path: &Path) -> Result<(), std::io::Error> {
    let metadata = collect_wasix_preopen_entry(path)?;
    if metadata.file_type().is_symlink() {
        return Err(std::io::Error::other(format!(
            "WASIX preopen contains symlink: {}",
            path.display()
        )));
    }
    if metadata.is_dir() {
        for entry in fs::read_dir(path)? {
            collect_wasix_preopen_tree(&entry?.path())?;
        }
    }
    Ok(())
}

fn collect_wasix_preopen_entry(path: &Path) -> Result<fs::Metadata, std::io::Error> {
    fs::symlink_metadata(path)
}

fn wasix_host_path_for_guest<'a>(
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

fn wasix_virtual_path_for_guest<'a>(
    path: &SandboxPath,
    preopened_dirs: &'a [SandboxWasiVirtualPreopen],
) -> Option<(&'a SandboxWasiVirtualPreopen, PathBuf)> {
    let mut matches: Vec<_> = preopened_dirs
        .iter()
        .filter(|preopen| sandbox_path_is_inside_preopen(path, &preopen.guest_path))
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
    if relative.is_empty() {
        Some((preopen, PathBuf::from("/")))
    } else {
        Some((preopen, PathBuf::from("/").join(relative)))
    }
}
