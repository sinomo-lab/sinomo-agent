use crate::RolloutError;
use std::fs;
use std::io::ErrorKind;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

pub fn stage_rollout_bytes(bytes: &[u8], destination: &Path) -> Result<(), RolloutError> {
    if bytes.is_empty() {
        return Err(RolloutError::InvalidArtifact {
            detail: format!("candidate binary for {} is empty", destination.display()),
        });
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    if let Err(source) = fs::write(destination, bytes) {
        let _ = fs::remove_file(destination);
        return Err(RolloutError::Io {
            path: destination.display().to_string(),
            detail: source.to_string(),
        });
    }
    set_executable(destination)
}

pub fn stage_rollout_file(source: &Path, destination: &Path) -> Result<(), RolloutError> {
    ensure_non_empty_rollout_artifact(source)?;
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    if let Err(error) = fs::copy(source, destination) {
        let _ = fs::remove_file(destination);
        return Err(RolloutError::Io {
            path: format!("{} -> {}", source.display(), destination.display()),
            detail: error.to_string(),
        });
    }
    if let Err(error) = ensure_non_empty_rollout_artifact(destination) {
        let _ = fs::remove_file(destination);
        return Err(error);
    }
    set_executable(destination)
}

pub fn stage_rollout_bytes_pair(
    bytes: &[u8],
    primary_destination: &Path,
    linked_destination: &Path,
) -> Result<(), RolloutError> {
    stage_rollout_bytes(bytes, primary_destination)?;
    stage_rollout_link_or_copy(primary_destination, linked_destination)
}

pub fn stage_rollout_file_pair(
    source: &Path,
    primary_destination: &Path,
    linked_destination: &Path,
) -> Result<(), RolloutError> {
    stage_rollout_file(source, primary_destination)?;
    stage_rollout_link_or_copy(primary_destination, linked_destination)
}

fn stage_rollout_link_or_copy(source: &Path, destination: &Path) -> Result<(), RolloutError> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    remove_staged_file_if_present(destination)?;
    match fs::hard_link(source, destination) {
        Ok(()) => set_executable(destination),
        Err(_) => stage_rollout_file(source, destination),
    }
}

fn remove_staged_file_if_present(destination: &Path) -> Result<(), RolloutError> {
    match fs::remove_file(destination) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(RolloutError::Io {
            path: destination.display().to_string(),
            detail: error.to_string(),
        }),
    }
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
        let previous = rollout_previous_binary_path(destination);
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
    ensure_non_empty_rollout_artifact(staged)?;
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let previous = rollout_previous_binary_path(destination);
    let mut backup = if previous.exists() {
        ensure_non_empty_rollout_artifact(&previous)?;
        Some(previous.clone())
    } else {
        None
    };
    if destination.exists() {
        match ensure_non_empty_rollout_artifact(destination) {
            Ok(()) => {
                let _ = fs::remove_file(&previous);
                fs::rename(destination, &previous).map_err(|source| RolloutError::Io {
                    path: format!("{} -> {}", destination.display(), previous.display()),
                    detail: source.to_string(),
                })?;
                backup = Some(previous.clone());
            }
            Err(RolloutError::InvalidArtifact { .. }) => {
                fs::remove_file(destination).map_err(|source| RolloutError::Io {
                    path: destination.display().to_string(),
                    detail: source.to_string(),
                })?;
            }
            Err(error) => return Err(error),
        }
    }
    fs::rename(staged, destination).map_err(|source| RolloutError::Io {
        path: format!("{} -> {}", staged.display(), destination.display()),
        detail: source.to_string(),
    })?;
    Ok(backup)
}

pub fn restore_rollout_binary_from_backup(
    backup: &Path,
    destination: &Path,
) -> Result<(), RolloutError> {
    if !backup.exists() {
        return Ok(());
    }
    ensure_non_empty_rollout_artifact(backup)?;
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

fn ensure_non_empty_rollout_artifact(path: &Path) -> Result<(), RolloutError> {
    let metadata = fs::metadata(path).map_err(|source| RolloutError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    if !metadata.is_file() {
        return Err(RolloutError::InvalidArtifact {
            detail: format!("{} is not a regular file", path.display()),
        });
    }
    if metadata.len() == 0 {
        return Err(RolloutError::InvalidArtifact {
            detail: format!("{} is empty", path.display()),
        });
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_binary_atomically_preserves_executable_suffix_in_previous_backup() {
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-artifact");
        let source = root.join("candidate.exe");
        let destination = root.join("sinomo.exe");
        fs::write(&source, b"candidate").expect("source");
        fs::write(&destination, b"previous").expect("destination");

        install_binary_atomically(&source, &destination).expect("install");

        assert_eq!(
            fs::read(&destination).expect("destination bytes"),
            b"candidate"
        );
        assert_eq!(
            fs::read(rollout_previous_binary_path(&destination)).expect("previous bytes"),
            b"previous"
        );
        assert!(!root.join("sinomo.previous").exists());
    }

    #[test]
    fn stage_rollout_file_pair_replaces_both_staged_outputs() {
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-stage-pair");
        let source = root.join("candidate");
        let staged_main = root.join("staged").join("sinomo-main");
        let staged_shadow = root.join("staged").join("sinomo-shadow");
        fs::create_dir_all(staged_main.parent().expect("staged parent")).expect("staged parent");
        fs::write(&source, b"candidate").expect("source");
        fs::write(&staged_main, b"old-main").expect("old main");
        fs::write(&staged_shadow, b"partial-shadow").expect("old shadow");

        stage_rollout_file_pair(&source, &staged_main, &staged_shadow).expect("stage pair");

        assert_eq!(fs::read(&staged_main).expect("main bytes"), b"candidate");
        assert_eq!(
            fs::read(&staged_shadow).expect("shadow bytes"),
            b"candidate"
        );
    }

    #[test]
    fn stage_rollout_bytes_pair_stages_same_bytes_to_both_outputs() {
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-stage-bytes-pair");
        let staged_main = root.join("staged").join("sinomo-main");
        let staged_shadow = root.join("staged").join("sinomo-shadow");

        stage_rollout_bytes_pair(b"candidate", &staged_main, &staged_shadow).expect("stage pair");

        assert_eq!(fs::read(&staged_main).expect("main bytes"), b"candidate");
        assert_eq!(
            fs::read(&staged_shadow).expect("shadow bytes"),
            b"candidate"
        );
    }

    #[test]
    fn stage_rollout_rejects_empty_candidate_artifacts() {
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-empty-artifact");
        let empty_source = root.join("empty");
        let staged = root.join("staged").join("sinomo");
        fs::write(&empty_source, []).expect("empty source");

        let file_error =
            stage_rollout_file(&empty_source, &staged).expect_err("empty file rejected");
        assert!(matches!(file_error, RolloutError::InvalidArtifact { .. }));
        assert!(!staged.exists());

        let bytes_error = stage_rollout_bytes(&[], &staged).expect_err("empty bytes rejected");
        assert!(matches!(bytes_error, RolloutError::InvalidArtifact { .. }));
        assert!(!staged.exists());
    }

    #[test]
    fn activate_staged_rollout_binary_preserves_existing_backup_when_destination_is_empty() {
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-empty-destination");
        let staged = root.join("staged").join("sinomo");
        let destination = root.join("bin").join("sinomo");
        let previous = rollout_previous_binary_path(&destination);
        fs::create_dir_all(staged.parent().expect("staged parent")).expect("staged parent");
        fs::create_dir_all(destination.parent().expect("bin parent")).expect("bin parent");
        fs::write(&staged, b"candidate").expect("staged");
        fs::write(&destination, []).expect("empty installed destination");
        fs::write(&previous, b"previous-good").expect("previous backup");

        let backup =
            activate_staged_rollout_binary(&staged, &destination).expect("activate staged");

        assert_eq!(backup.as_deref(), Some(previous.as_path()));
        assert_eq!(
            fs::read(&destination).expect("destination bytes"),
            b"candidate"
        );
        assert_eq!(
            fs::read(&previous).expect("previous bytes"),
            b"previous-good"
        );
    }

    #[test]
    fn restore_rollout_binary_from_backup_rejects_empty_backup() {
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-empty-backup");
        let backup = root.join("sinomo.previous");
        let destination = root.join("sinomo");
        fs::write(&backup, []).expect("empty backup");
        fs::write(&destination, b"current").expect("current destination");

        let error = restore_rollout_binary_from_backup(&backup, &destination)
            .expect_err("empty backup rejected");

        assert!(matches!(error, RolloutError::InvalidArtifact { .. }));
        assert_eq!(
            fs::read(&destination).expect("destination bytes"),
            b"current"
        );
    }
}
