use crate::RolloutError;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

pub fn stage_rollout_bytes(bytes: &[u8], destination: &Path) -> Result<(), RolloutError> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    fs::write(destination, bytes).map_err(|source| RolloutError::Io {
        path: destination.display().to_string(),
        detail: source.to_string(),
    })?;
    set_executable(destination)
}

pub fn stage_rollout_file(source: &Path, destination: &Path) -> Result<(), RolloutError> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    fs::copy(source, destination).map_err(|error| RolloutError::Io {
        path: format!("{} -> {}", source.display(), destination.display()),
        detail: error.to_string(),
    })?;
    set_executable(destination)
}

pub fn activate_staged_rollout_binary(
    staged: &Path,
    destination: &Path,
) -> Result<Option<PathBuf>, RolloutError> {
    if !staged.exists() {
        return Ok(None);
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let previous = rollout_previous_binary_path(destination);
    let _ = fs::remove_file(&previous);
    if destination.exists() {
        fs::rename(destination, &previous).map_err(|source| RolloutError::Io {
            path: format!("{} -> {}", destination.display(), previous.display()),
            detail: source.to_string(),
        })?;
    }
    fs::rename(staged, destination).map_err(|source| RolloutError::Io {
        path: format!("{} -> {}", staged.display(), destination.display()),
        detail: source.to_string(),
    })?;
    Ok(Some(previous))
}

pub fn restore_rollout_binary_from_backup(
    backup: &Path,
    destination: &Path,
) -> Result<(), RolloutError> {
    if !backup.exists() {
        return Ok(());
    }
    let failed = destination.with_file_name(format!(
        "{}.failed",
        destination
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("sinomo")
    ));
    if destination.exists() {
        let _ = fs::remove_file(&failed);
        fs::rename(destination, &failed).map_err(|source| RolloutError::Io {
            path: format!("{} -> {}", destination.display(), failed.display()),
            detail: source.to_string(),
        })?;
    }
    fs::rename(backup, destination).map_err(|source| RolloutError::Io {
        path: format!("{} -> {}", backup.display(), destination.display()),
        detail: source.to_string(),
    })
}

pub fn rollout_previous_binary_path(destination: &Path) -> PathBuf {
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("sinomo");
    destination.with_file_name(format!("{file_name}.previous"))
}

fn set_executable(_destination: &Path) -> Result<(), RolloutError> {
    #[cfg(unix)]
    {
        fs::set_permissions(_destination, fs::Permissions::from_mode(0o755)).map_err(|source| {
            RolloutError::Io {
                path: _destination.display().to_string(),
                detail: source.to_string(),
            }
        })?;
    }
    Ok(())
}
