use crate::RolloutError;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

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

pub fn stage_rollout_file_with_retry(
    source: &Path,
    destination: &Path,
    timeout: Duration,
    retry_interval: Duration,
) -> Result<(), RolloutError> {
    let deadline = Instant::now() + timeout;
    loop {
        match stage_rollout_file(source, destination) {
            Ok(()) => return Ok(()),
            Err(_) if Instant::now() < deadline => {
                thread::sleep(retry_interval);
            }
            Err(error) => return Err(error),
        }
    }
}

pub fn install_binary_atomically(source: &Path, destination: &Path) -> Result<(), RolloutError> {
    if !source.exists() {
        return Err(RolloutError::MissingArtifact {
            artifact: source.display().to_string(),
        });
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let next = destination.with_extension("next");
    fs::copy(source, &next).map_err(|error| RolloutError::Io {
        path: format!("{} -> {}", source.display(), next.display()),
        detail: error.to_string(),
    })?;
    if destination.exists() {
        let previous = destination.with_extension("previous");
        let _ = fs::remove_file(&previous);
        fs::rename(destination, &previous).map_err(|source| RolloutError::Io {
            path: format!("{} -> {}", destination.display(), previous.display()),
            detail: source.to_string(),
        })?;
    }
    fs::rename(&next, destination).map_err(|source| RolloutError::Io {
        path: format!("{} -> {}", next.display(), destination.display()),
        detail: source.to_string(),
    })?;
    Ok(())
}

pub fn current_executable_path() -> Result<PathBuf, RolloutError> {
    std::env::current_exe().map_err(|source| RolloutError::Io {
        path: "current executable".to_string(),
        detail: source.to_string(),
    })
}

pub fn install_current_executable(destination: &Path) -> Result<(), RolloutError> {
    let current = current_executable_path()?;
    if same_path_if_exists(&current, destination) {
        return Ok(());
    }
    install_binary_atomically(&current, destination)
}

pub fn binary_should_update(source: &Path, destination: &Path) -> Result<bool, RolloutError> {
    if !destination.exists() {
        return Ok(false);
    }
    if same_path_if_exists(source, destination) {
        return Ok(false);
    }
    let source_bytes = fs::read(source).map_err(|error| RolloutError::Io {
        path: source.display().to_string(),
        detail: error.to_string(),
    })?;
    let destination_bytes = fs::read(destination).map_err(|error| RolloutError::Io {
        path: destination.display().to_string(),
        detail: error.to_string(),
    })?;
    Ok(source_bytes != destination_bytes)
}

pub fn current_executable_should_update(destination: &Path) -> Result<bool, RolloutError> {
    let current = current_executable_path()?;
    binary_should_update(&current, destination)
}

pub fn same_path_if_exists(left: &Path, right: &Path) -> bool {
    let Ok(left) = fs::canonicalize(left) else {
        return false;
    };
    let Ok(right) = fs::canonicalize(right) else {
        return false;
    };
    left == right
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
