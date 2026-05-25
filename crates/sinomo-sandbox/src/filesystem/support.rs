use crate::internal::*;

pub(crate) fn normalize_sandbox_sifs_root(root: &str) -> String {
    let trimmed = root.trim();
    if trimmed == "/" {
        "/".to_string()
    } else {
        format!("/{}", trimmed.trim_matches('/'))
    }
}

pub(crate) fn sandbox_parent_path(
    path: &SandboxPath,
) -> Result<Option<SandboxPath>, SandboxFsError> {
    let value = path.as_str();
    if value == "/" {
        return Ok(None);
    }
    let parent = value.rsplit_once('/').map_or(
        "/",
        |(parent, _name)| {
            if parent.is_empty() { "/" } else { parent }
        },
    );
    Ok(Some(SandboxPath::new(parent)?))
}

pub(crate) fn sandbox_fs_error_from_dfs(
    path: &SandboxPath,
    error: DfsPosixError,
) -> SandboxFsError {
    match error {
        DfsPosixError::NotFound { .. } | DfsPosixError::MissingParent { .. } => {
            SandboxFsError::NotFound { path: path.clone() }
        }
        DfsPosixError::AlreadyExists { .. } => SandboxFsError::AlreadyExists { path: path.clone() },
        DfsPosixError::InvalidPath { path: invalid_path } => {
            SandboxFsError::Path(SandboxPathError::EscapesRoot { path: invalid_path })
        }
        DfsPosixError::NotDirectory { .. } => SandboxFsError::NotDirectory { path: path.clone() },
        DfsPosixError::IsDirectory { .. } => SandboxFsError::IsDirectory { path: path.clone() },
        DfsPosixError::NotSymlink { .. } => SandboxFsError::NotSymlink { path: path.clone() },
        DfsPosixError::DirectoryNotEmpty { .. } => {
            SandboxFsError::DirectoryNotEmpty { path: path.clone() }
        }
        other => SandboxFsError::Unavailable {
            reason: other.to_string(),
        },
    }
}

pub(crate) fn sandbox_toolchain_error_from_dfs(
    path: String,
    error: DfsPosixError,
) -> SandboxToolchainError {
    SandboxToolchainError::Io {
        path,
        detail: error.to_string(),
    }
}

pub(crate) fn sandbox_store_error_from_dfs(
    path: String,
    error: DfsPosixError,
) -> SandboxStoreError {
    SandboxStoreError::Io {
        path,
        detail: error.to_string(),
    }
}

pub(crate) fn ensure_host_path_is_within(
    base: &Path,
    host_path: &Path,
    sandbox_path: &SandboxPath,
) -> Result<(), SandboxFsError> {
    if !host_path.starts_with(base) {
        return Err(SandboxFsError::Path(SandboxPathError::EscapesRoot {
            path: sandbox_path.to_string(),
        }));
    }
    Ok(())
}

pub(crate) fn reject_symlink_chain(
    host_path: &Path,
    sandbox_path: &SandboxPath,
) -> Result<(), SandboxFsError> {
    let mut current = PathBuf::new();
    for component in host_path.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(SandboxFsError::Path(SandboxPathError::EscapesRoot {
                    path: sandbox_path.to_string(),
                }));
            }
            Ok(_) => {}
            Err(_) => {}
        }
    }
    Ok(())
}
