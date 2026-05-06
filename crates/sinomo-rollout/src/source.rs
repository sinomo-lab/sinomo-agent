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
    if paths.source_repo.join(".git").exists() {
        return merge_external_repo_into_managed(paths, source_repo);
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
    remove_all_git_remotes(&paths.source_repo)?;
    remove_dir_if_exists(&previous)?;
    Ok(())
}

pub fn sync_external_repo_from_managed(
    paths: &SelfRolloutPaths,
    external_repo: &Path,
) -> Result<(), RolloutError> {
    require_git_repo(&paths.source_repo, "managed source repo")?;
    require_git_repo(external_repo, "external source repo")?;
    merge_repo_head(external_repo, &paths.source_repo, "sinomo-internal")
}

pub fn create_source_repo_bundle(
    source_repo: &Path,
    bundle_path: &Path,
) -> Result<(), RolloutError> {
    require_git_repo(source_repo, "source repo")?;
    let dirty = git_one_line(source_repo, &["status", "--porcelain"])?;
    if !dirty.is_empty() {
        return Err(RolloutError::Process {
            command: format!("git -C {} status --porcelain", source_repo.display()),
            status: 1,
            stderr: "source repo has uncommitted changes; commit them before remote-build rollout or use --binary".to_string(),
        });
    }
    if let Some(parent) = bundle_path.parent() {
        fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let _ = fs::remove_file(bundle_path);
    let output = Command::new("git")
        .arg("-C")
        .arg(source_repo)
        .args(["bundle", "create"])
        .arg(bundle_path)
        .arg("HEAD")
        .output()
        .map_err(|source| RolloutError::Io {
            path: "git bundle create".to_string(),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(RolloutError::Process {
            command: format!(
                "git -C {} bundle create {} HEAD",
                source_repo.display(),
                bundle_path.display()
            ),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    run_git(
        source_repo,
        &["bundle", "verify", &bundle_path.display().to_string()],
    )?;
    Ok(())
}

pub fn deploy_managed_source_bundle(
    paths: &SelfRolloutPaths,
    bundle_path: &Path,
) -> Result<(), RolloutError> {
    if !bundle_path.exists() {
        return Err(RolloutError::MissingArtifact {
            artifact: bundle_path.display().to_string(),
        });
    }
    if paths.source_repo.join(".git").exists() {
        return merge_bundle_head_into_managed(paths, bundle_path);
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
        .arg(bundle_path)
        .arg(&next)
        .output()
        .map_err(|source| RolloutError::Io {
            path: "git clone".to_string(),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(RolloutError::Process {
            command: format!("git clone {} {}", bundle_path.display(), next.display()),
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
    remove_all_git_remotes(&paths.source_repo)?;
    remove_dir_if_exists(&previous)?;
    Ok(())
}

pub fn managed_source_status(
    paths: &SelfRolloutPaths,
) -> Result<ManagedSourceStatus, RolloutError> {
    let exists = paths.source_repo.exists();
    let is_git_repo = paths.source_repo.join(".git").exists();
    let remotes = if is_git_repo {
        git_lines(&paths.source_repo, &["remote"])?
    } else {
        Vec::new()
    };
    let head = if is_git_repo {
        Some(git_one_line(&paths.source_repo, &["rev-parse", "HEAD"])?)
    } else {
        None
    };
    Ok(ManagedSourceStatus {
        path: paths.source_repo.clone(),
        exists,
        is_git_repo,
        has_external_remotes: !remotes.is_empty(),
        remotes,
        head,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedSourceStatus {
    pub path: PathBuf,
    pub exists: bool,
    pub is_git_repo: bool,
    pub has_external_remotes: bool,
    pub remotes: Vec<String>,
    pub head: Option<String>,
}

fn merge_external_repo_into_managed(
    paths: &SelfRolloutPaths,
    source_repo: &Path,
) -> Result<(), RolloutError> {
    require_git_repo(&paths.source_repo, "managed source repo")?;
    require_git_repo(source_repo, "external source repo")?;
    merge_repo_head(&paths.source_repo, source_repo, "sinomo-external")?;
    remove_all_git_remotes(&paths.source_repo)
}

fn merge_bundle_head_into_managed(
    paths: &SelfRolloutPaths,
    bundle_path: &Path,
) -> Result<(), RolloutError> {
    require_git_repo(&paths.source_repo, "managed source repo")?;
    let fetch = Command::new("git")
        .arg("-C")
        .arg(&paths.source_repo)
        .arg("fetch")
        .arg(bundle_path)
        .arg("HEAD")
        .output()
        .map_err(|source| RolloutError::Io {
            path: "git fetch bundle".to_string(),
            detail: source.to_string(),
        })?;
    if !fetch.status.success() {
        return Err(RolloutError::Process {
            command: format!(
                "git -C {} fetch {} HEAD",
                paths.source_repo.display(),
                bundle_path.display()
            ),
            status: fetch.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&fetch.stderr).trim().to_string(),
        });
    }
    run_git(&paths.source_repo, &["merge", "--no-edit", "FETCH_HEAD"])?;
    remove_all_git_remotes(&paths.source_repo)
}

fn merge_repo_head(
    destination_repo: &Path,
    source_repo: &Path,
    remote_name: &str,
) -> Result<(), RolloutError> {
    let _ = run_git(destination_repo, &["remote", "remove", remote_name]);
    run_git_paths(
        destination_repo,
        &["remote", "add", remote_name],
        &[source_repo],
    )?;
    let fetch_result = run_git(destination_repo, &["fetch", remote_name, "HEAD"]);
    let remove_result = run_git(destination_repo, &["remote", "remove", remote_name]);
    fetch_result?;
    remove_result?;
    run_git(destination_repo, &["merge", "--no-edit", "FETCH_HEAD"]).map(|_| ())
}

fn remove_all_git_remotes(repo: &Path) -> Result<(), RolloutError> {
    for remote in git_lines(repo, &["remote"])? {
        run_git(repo, &["remote", "remove", &remote])?;
    }
    let _ = run_git(repo, &["branch", "--unset-upstream"]);
    Ok(())
}

fn require_git_repo(repo: &Path, label: &str) -> Result<(), RolloutError> {
    if repo.join(".git").exists() {
        Ok(())
    } else {
        Err(RolloutError::MissingArtifact {
            artifact: format!("{label} at {} is not a git repository", repo.display()),
        })
    }
}

fn git_one_line(repo: &Path, args: &[&str]) -> Result<String, RolloutError> {
    Ok(git_lines(repo, args)?
        .into_iter()
        .next()
        .unwrap_or_default())
}

fn git_lines(repo: &Path, args: &[&str]) -> Result<Vec<String>, RolloutError> {
    let output = run_git(repo, args)?;
    Ok(output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

fn run_git(repo: &Path, args: &[&str]) -> Result<String, RolloutError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .map_err(|source| RolloutError::Io {
            path: format!("git -C {} {}", repo.display(), args.join(" ")),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(RolloutError::Process {
            command: format!("git -C {} {}", repo.display(), args.join(" ")),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn run_git_paths(repo: &Path, args: &[&str], paths: &[&Path]) -> Result<String, RolloutError> {
    let mut command = Command::new("git");
    command.arg("-C").arg(repo).args(args);
    for path in paths {
        command.arg(path);
    }
    let output = command.output().map_err(|source| RolloutError::Io {
        path: format!("git -C {} {}", repo.display(), args.join(" ")),
        detail: source.to_string(),
    })?;
    if !output.status.success() {
        return Err(RolloutError::Process {
            command: format!("git -C {} {}", repo.display(), args.join(" ")),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SelfRolloutPaths;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn deploy_managed_source_bundle_initializes_missing_managed_repo() {
        let root = unique_temp("sinomo-rollout-source-bundle-init");
        let external = root.join("external");
        let data = root.join("data");
        let bundle = root.join("source.bundle");
        fs::create_dir_all(&external).expect("external dir");
        run_git_for_test(&external, &["init"]);
        run_git_for_test(&external, &["config", "user.email", "test@example.invalid"]);
        run_git_for_test(&external, &["config", "user.name", "Sinomo Test"]);
        fs::write(external.join("Cargo.toml"), "[workspace]\n").expect("cargo");
        run_git_for_test(&external, &["add", "-A"]);
        run_git_for_test(&external, &["commit", "-m", "initial"]);
        let expected_head = git_one_line(&external, &["rev-parse", "HEAD"]).expect("expected head");

        let paths = SelfRolloutPaths::new(&data);
        create_source_repo_bundle(&external, &bundle).expect("source bundle");
        deploy_managed_source_bundle(&paths, &bundle).expect("bundle deploy");
        let status = managed_source_status(&paths).expect("status");

        assert_eq!(status.head.as_deref(), Some(expected_head.as_str()));
        assert!(!status.has_external_remotes);
        assert!(paths.source_repo.join("Cargo.toml").exists());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn deploy_managed_source_bundle_updates_existing_managed_repo() {
        let root = unique_temp("sinomo-rollout-source-bundle");
        let external = root.join("external");
        let data = root.join("data");
        let bundle = root.join("source.bundle");
        fs::create_dir_all(&external).expect("external dir");
        run_git_for_test(&external, &["init"]);
        run_git_for_test(&external, &["config", "user.email", "test@example.invalid"]);
        run_git_for_test(&external, &["config", "user.name", "Sinomo Test"]);
        fs::write(external.join("Cargo.toml"), "[workspace]\n").expect("cargo");
        run_git_for_test(&external, &["add", "-A"]);
        run_git_for_test(&external, &["commit", "-m", "initial"]);

        let paths = SelfRolloutPaths::new(&data);
        deploy_managed_source_repo(&paths, &external).expect("initial deploy");
        fs::write(
            external.join("Cargo.toml"),
            "[workspace]\nresolver = \"3\"\n",
        )
        .expect("cargo update");
        run_git_for_test(&external, &["add", "-A"]);
        run_git_for_test(&external, &["commit", "-m", "second"]);
        let expected_head = git_one_line(&external, &["rev-parse", "HEAD"]).expect("expected head");

        create_source_repo_bundle(&external, &bundle).expect("source bundle");
        deploy_managed_source_bundle(&paths, &bundle).expect("bundle deploy");
        let status = managed_source_status(&paths).expect("status");

        assert_eq!(status.head.as_deref(), Some(expected_head.as_str()));
        assert!(!status.has_external_remotes);
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn deploy_managed_source_removes_external_remotes() {
        let root = unique_temp("sinomo-rollout-source");
        let external = root.join("external");
        let data = root.join("data");
        fs::create_dir_all(&external).expect("external dir");
        run_git_for_test(&external, &["init"]);
        run_git_for_test(&external, &["config", "user.email", "test@example.invalid"]);
        run_git_for_test(&external, &["config", "user.name", "Sinomo Test"]);
        fs::write(external.join("Cargo.toml"), "[workspace]\n").expect("cargo");
        run_git_for_test(&external, &["add", "-A"]);
        run_git_for_test(&external, &["commit", "-m", "initial"]);
        run_git_for_test(
            &external,
            &[
                "remote",
                "add",
                "origin",
                "https://example.invalid/repo.git",
            ],
        );

        let paths = SelfRolloutPaths::new(&data);
        deploy_managed_source_repo(&paths, &external).expect("deploy");
        let status = managed_source_status(&paths).expect("status");

        assert!(status.is_git_repo);
        assert!(!status.has_external_remotes);
        assert!(paths.source_repo.join("Cargo.toml").exists());
        fs::remove_dir_all(root).ok();
    }

    fn unique_temp(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{}-{}",
            prefix,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
    }

    fn run_git_for_test(repo: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .expect("git");
        assert!(
            output.status.success(),
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
