use crate::{RolloutError, SelfRolloutPaths};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

pub fn discover_current_git_repo() -> Result<Option<PathBuf>, RolloutError> {
    let current_dir = std::env::current_dir().map_err(|source| RolloutError::Io {
        path: ".".to_string(),
        detail: source.to_string(),
    })?;
    let output = Command::new("git")
        .args(["-C"])
        .arg(&current_dir)
        .args(["rev-parse", "--show-toplevel"])
        .output();
    match output {
        Ok(output) if output.status.success() => {
            let root = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if root.is_empty() {
                Ok(None)
            } else {
                Ok(Some(PathBuf::from(root)))
            }
        }
        Ok(_) if current_dir.join(".git").exists() && current_dir.join("Cargo.toml").exists() => {
            Ok(Some(current_dir))
        }
        Ok(_) => Ok(None),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(RolloutError::Io {
            path: "git rev-parse --show-toplevel".to_string(),
            detail: source.to_string(),
        }),
    }
}

pub fn deploy_managed_source_repo(
    paths: &SelfRolloutPaths,
    source_repo: &Path,
) -> Result<(), RolloutError> {
    if same_path_if_exists(source_repo, &paths.source_repo) {
        return Ok(());
    }
    let Some(parent) = paths.source_repo.parent() else {
        return Err(RolloutError::Io {
            path: paths.source_repo.display().to_string(),
            detail: "managed source path has no parent".to_string(),
        });
    };
    fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
        path: parent.display().to_string(),
        detail: source.to_string(),
    })?;
    let next = paths.source_repo.with_extension("next");
    let previous = paths.source_repo.with_extension("previous");
    remove_dir_if_exists(&next)?;
    let output = Command::new("git")
        .arg("clone")
        .arg("--no-hardlinks")
        .arg(source_repo)
        .arg(&next)
        .output()
        .map_err(|source| RolloutError::Io {
            path: "git clone".to_string(),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(RolloutError::Process {
            command: format!(
                "git clone --no-hardlinks {} {}",
                source_repo.display(),
                next.display()
            ),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    remove_dir_if_exists(&previous)?;
    if paths.source_repo.exists() {
        fs::rename(&paths.source_repo, &previous).map_err(|source| RolloutError::Io {
            path: format!("{} -> {}", paths.source_repo.display(), previous.display()),
            detail: source.to_string(),
        })?;
    }
    fs::rename(&next, &paths.source_repo).map_err(|source| RolloutError::Io {
        path: format!("{} -> {}", next.display(), paths.source_repo.display()),
        detail: source.to_string(),
    })?;
    remove_dir_if_exists(&previous)?;
    Ok(())
}

pub fn remove_dir_if_exists(path: &Path) -> Result<(), RolloutError> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(RolloutError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

fn same_path_if_exists(left: &Path, right: &Path) -> bool {
    let Ok(left) = fs::canonicalize(left) else {
        return false;
    };
    let Ok(right) = fs::canonicalize(right) else {
        return false;
    };
    left == right
}
