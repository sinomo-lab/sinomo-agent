use crate::{
    BuildError, BuildPaths, BuildSource, Result, build_content_hash, build_source_identity_hash,
};
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ManagedSourceDeployOptions {
    pub force_overwrite: bool,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedBuildSource {
    pub source: BuildSource,
    pub source_repo: PathBuf,
    pub source_bundle: PathBuf,
    pub lockfile_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedSourceBundle {
    pub path: PathBuf,
    pub hash: String,
    pub bytes: u64,
    pub revision: String,
}

pub fn discover_current_git_repo() -> Result<Option<PathBuf>> {
    let current_dir = env::current_dir().map_err(|source| BuildError::Io {
        path: ".".to_string(),
        detail: source.to_string(),
    })?;
    let output = crate::process::noninteractive_command("git")
        .arg("-C")
        .arg(&current_dir)
        .args(["rev-parse", "--show-toplevel"])
        .output();
    match output {
        Ok(output) if output.status.success() => {
            let root = String::from_utf8_lossy(&output.stdout).trim().to_string();
            Ok((!root.is_empty()).then(|| PathBuf::from(root)))
        }
        Ok(_) if current_dir.join(".git").exists() && current_dir.join("Cargo.toml").exists() => {
            Ok(Some(current_dir))
        }
        Ok(_) => Ok(None),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(BuildError::Io {
            path: "git rev-parse --show-toplevel".to_string(),
            detail: source.to_string(),
        }),
    }
}

pub fn build_source_head(repo: &Path) -> Result<String> {
    git_one_line(repo, &["rev-parse", "HEAD"])
}

pub fn resolve_build_source_commit(repo: &Path, revision: &str) -> Result<String> {
    let revision = revision.trim();
    if revision.len() < 7 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(BuildError::InvalidValue {
            detail: format!("build source revision {revision:?} is not a Git commit id"),
        });
    }
    let commit = format!("{revision}^{{commit}}");
    let resolved = git_one_line(
        repo,
        &["rev-parse", "--verify", "--end-of-options", &commit],
    )?;
    if resolved.len() < 40 || !resolved.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(BuildError::InvalidValue {
            detail: format!(
                "build source revision {revision:?} resolved to invalid commit {resolved:?}"
            ),
        });
    }
    Ok(resolved.to_ascii_lowercase())
}

pub fn build_source_heads_match(actual: Option<&str>, expected: &str) -> bool {
    let Some(actual) = actual else {
        return false;
    };
    actual == expected || actual.starts_with(expected) || expected.starts_with(actual)
}

pub fn prepare_build_source(paths: &BuildPaths, source_repo: &Path) -> Result<PreparedBuildSource> {
    crate::validate_deployment_source(source_repo)?;
    let bundle = prepare_source_bundle(paths, source_repo)?;
    let lockfile_path = source_repo.join("Cargo.lock");
    let lockfile = crate::process::noninteractive_command("git")
        .arg("-C")
        .arg(source_repo)
        .args(["show", &format!("{}:Cargo.lock", bundle.revision)])
        .output()
        .map_err(|source| BuildError::Io {
            path: lockfile_path.display().to_string(),
            detail: source.to_string(),
        })?;
    if !lockfile.status.success() {
        return Err(BuildError::InvalidValue {
            detail: "committed source has no readable Cargo.lock".into(),
        });
    }
    let identity_hash = build_source_identity_hash(&bundle.revision);
    Ok(PreparedBuildSource {
        source: BuildSource {
            label: source_repo.display().to_string(),
            revision: Some(bundle.revision),
            identity_hash: Some(identity_hash),
            bundle_hash: bundle.hash,
            bundle_bytes: bundle.bytes,
        },
        source_repo: source_repo.to_path_buf(),
        source_bundle: bundle.path,
        lockfile_hash: build_content_hash(&lockfile.stdout),
    })
}

pub fn prepare_source_bundle(
    paths: &BuildPaths,
    source_repo: &Path,
) -> Result<PreparedSourceBundle> {
    paths.ensure_layout()?;
    let scratch =
        sinomo_core::storage::scratch::ScratchDirectory::create(&paths.sources, "prepare")
            .map_err(|error| BuildError::Io {
                path: paths.sources.display().to_string(),
                detail: error.to_string(),
            })?;
    let temporary = scratch.path().join("source.bundle");
    let _cleanup = sinomo_core::storage::scratch::ScratchPath::new(temporary.clone());
    create_source_repo_bundle(source_repo, &temporary)?;
    let bytes = fs::read(&temporary).map_err(|source| BuildError::Io {
        path: temporary.display().to_string(),
        detail: source.to_string(),
    })?;
    let bundle_hash = build_content_hash(&bytes);
    let source_bundle = paths.source_bundle(&bundle_hash)?;
    let _source_lease =
        crate::manager::BuildPathLock::acquire(&source_bundle.with_extension("lock"))?;
    if !source_bundle.is_file() {
        match fs::rename(&temporary, &source_bundle) {
            Ok(()) => {}
            Err(_) if source_bundle.is_file() => {
                let _ = fs::remove_file(&temporary);
            }
            Err(source) => {
                let _ = fs::remove_file(&temporary);
                return Err(BuildError::Io {
                    path: source_bundle.display().to_string(),
                    detail: source.to_string(),
                });
            }
        }
    } else {
        let _ = fs::remove_file(&temporary);
    }
    fs::write(source_bundle.with_extension("last-used"), []).map_err(|error| BuildError::Io {
        path: source_bundle.display().to_string(),
        detail: error.to_string(),
    })?;
    Ok(PreparedSourceBundle {
        path: source_bundle,
        hash: bundle_hash,
        bytes: bytes.len() as u64,
        revision: build_source_head(source_repo)?,
    })
}

pub fn create_source_repo_bundle(source_repo: &Path, bundle_path: &Path) -> Result<()> {
    require_git_repo(source_repo, "source repo")?;
    let dirty = git_one_line(source_repo, &["status", "--porcelain"])?;
    if !dirty.is_empty() {
        return Err(BuildError::Process {
            command: format!("git -C {} status --porcelain", source_repo.display()),
            status: 1,
            stderr: "source repo has uncommitted changes; commit them before building or supply an already-built binary".to_string(),
        });
    }
    if let Some(parent) = bundle_path.parent() {
        fs::create_dir_all(parent).map_err(|source| BuildError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let _ = fs::remove_file(bundle_path);
    let output = source_bundle_command(source_repo, bundle_path)
        .output()
        .map_err(|source| BuildError::Io {
            path: "git bundle create".to_string(),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(BuildError::Process {
            command: format!(
                "git -C {} bundle create {} HEAD",
                source_repo.display(),
                bundle_path.display()
            ),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    run_git_paths(source_repo, &["bundle", "verify"], &[bundle_path])?;
    Ok(())
}

fn source_bundle_command(source_repo: &Path, bundle_path: &Path) -> std::process::Command {
    // The bundle remains independently content-addressed as a transport
    // payload. One pack thread avoids needless duplicate payloads when the
    // same checkout prepares a build repeatedly; the build key itself uses
    // the Git revision identity and therefore also survives clone-local pack
    // layout differences.
    let mut command = crate::process::noninteractive_command("git");
    command
        .args(["-c", "pack.threads=1"])
        .arg("-C")
        .arg(source_repo)
        .args(["bundle", "create"])
        .arg(bundle_path)
        .arg("HEAD");
    command
}

pub fn deploy_managed_source_repo(
    managed_repo: &Path,
    source_repo: &Path,
    options: ManagedSourceDeployOptions,
) -> Result<()> {
    if same_path_if_exists(source_repo, managed_repo) {
        return Ok(());
    }
    if managed_repo.join(".git").exists() {
        require_git_repo(source_repo, "external source repo")?;
        merge_repo_head(managed_repo, source_repo, options, true)?;
        return remove_all_git_remotes(managed_repo);
    }
    let parent = managed_repo.parent().ok_or_else(|| BuildError::Io {
        path: managed_repo.display().to_string(),
        detail: "managed source path has no parent".to_string(),
    })?;
    fs::create_dir_all(parent).map_err(|source| BuildError::Io {
        path: parent.display().to_string(),
        detail: source.to_string(),
    })?;
    let next = managed_repo.with_extension("next");
    let previous = managed_repo.with_extension("previous");
    remove_dir_if_exists(&next)?;
    let output = crate::process::noninteractive_command("git")
        .arg("clone")
        .arg("--no-hardlinks")
        .args(["--config", "core.autocrlf=false"])
        .arg(source_repo)
        .arg(&next)
        .output()
        .map_err(|source| BuildError::Io {
            path: "git clone".to_string(),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(BuildError::Process {
            command: format!(
                "git clone --no-hardlinks {} {}",
                source_repo.display(),
                next.display()
            ),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    replace_managed_checkout(managed_repo, &next, &previous)?;
    remove_all_git_remotes(managed_repo)
}

pub fn deploy_managed_source_bundle(
    managed_repo: &Path,
    bundle_path: &Path,
    options: ManagedSourceDeployOptions,
) -> Result<()> {
    if !bundle_path.is_file() {
        return Err(BuildError::Io {
            path: bundle_path.display().to_string(),
            detail: "source bundle does not exist".to_string(),
        });
    }
    if managed_repo.join(".git").exists() {
        require_git_repo(managed_repo, "managed source repo")?;
        let output = crate::process::noninteractive_command("git")
            .arg("-C")
            .arg(managed_repo)
            .arg("fetch")
            .arg(bundle_path)
            .arg("HEAD")
            .output()
            .map_err(|source| BuildError::Io {
                path: "git fetch bundle".to_string(),
                detail: source.to_string(),
            })?;
        if !output.status.success() {
            return Err(BuildError::Process {
                command: format!(
                    "git -C {} fetch {} HEAD",
                    managed_repo.display(),
                    bundle_path.display()
                ),
                status: output.status.code().unwrap_or(-1),
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }
        update_managed_repo_to_fetched_head(managed_repo, options, true)?;
        return remove_all_git_remotes(managed_repo);
    }
    let parent = managed_repo.parent().ok_or_else(|| BuildError::Io {
        path: managed_repo.display().to_string(),
        detail: "managed source path has no parent".to_string(),
    })?;
    fs::create_dir_all(parent).map_err(|source| BuildError::Io {
        path: parent.display().to_string(),
        detail: source.to_string(),
    })?;
    let next = managed_repo.with_extension("next");
    let previous = managed_repo.with_extension("previous");
    remove_dir_if_exists(&next)?;
    let output = crate::process::noninteractive_command("git")
        .arg("clone")
        .args(["--config", "core.autocrlf=false"])
        .arg(bundle_path)
        .arg(&next)
        .output()
        .map_err(|source| BuildError::Io {
            path: "git clone".to_string(),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(BuildError::Process {
            command: format!("git clone {} {}", bundle_path.display(), next.display()),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    replace_managed_checkout(managed_repo, &next, &previous)?;
    remove_all_git_remotes(managed_repo)
}

pub fn sync_external_repo_from_managed(managed_repo: &Path, external_repo: &Path) -> Result<()> {
    require_git_repo(managed_repo, "managed source repo")?;
    require_git_repo(external_repo, "external source repo")?;
    merge_repo_head(
        external_repo,
        managed_repo,
        ManagedSourceDeployOptions::default(),
        false,
    )
}

pub fn managed_source_status(managed_repo: &Path) -> Result<ManagedSourceStatus> {
    let exists = managed_repo.exists();
    let is_git_repo = managed_repo.join(".git").exists();
    let remotes = if is_git_repo {
        git_lines(managed_repo, &["remote"])?
    } else {
        Vec::new()
    };
    let head = if is_git_repo {
        Some(git_one_line(managed_repo, &["rev-parse", "HEAD"])?)
    } else {
        None
    };
    Ok(ManagedSourceStatus {
        path: managed_repo.to_path_buf(),
        exists,
        is_git_repo,
        has_external_remotes: !remotes.is_empty(),
        remotes,
        head,
    })
}

fn replace_managed_checkout(managed_repo: &Path, next: &Path, previous: &Path) -> Result<()> {
    remove_dir_if_exists(previous)?;
    if managed_repo.exists() {
        fs::rename(managed_repo, previous).map_err(|source| BuildError::Io {
            path: format!("{} -> {}", managed_repo.display(), previous.display()),
            detail: source.to_string(),
        })?;
    }
    fs::rename(next, managed_repo).map_err(|source| BuildError::Io {
        path: format!("{} -> {}", next.display(), managed_repo.display()),
        detail: source.to_string(),
    })?;
    remove_dir_if_exists(previous)
}

fn merge_repo_head(
    target_repo: &Path,
    source_repo: &Path,
    options: ManagedSourceDeployOptions,
    canonical_checkout: bool,
) -> Result<()> {
    require_git_repo(target_repo, "target repo")?;
    require_git_repo(source_repo, "source repo")?;
    run_git_paths(target_repo, &["fetch"], &[source_repo])?;
    update_managed_repo_to_fetched_head(target_repo, options, canonical_checkout)
}

fn update_managed_repo_to_fetched_head(
    repo: &Path,
    options: ManagedSourceDeployOptions,
    canonical_checkout: bool,
) -> Result<()> {
    match ensure_clean_git_worktree(repo) {
        Ok(()) => {}
        Err(_) if options.force_overwrite => preserve_dirty_managed_source(repo)?,
        Err(error) => return Err(error),
    }
    if canonical_checkout {
        run_git(repo, &["config", "core.autocrlf", "false"])?;
    }
    let head = git_one_line(repo, &["rev-parse", "HEAD"])?;
    let fetched = git_one_line(repo, &["rev-parse", "FETCH_HEAD"])?;
    if head != fetched {
        run_git(
            repo,
            &["branch", "-f", "sinomo-managed-source-backup", "HEAD"],
        )?;
    }
    run_git(repo, &["reset", "--hard", "FETCH_HEAD"])?;
    run_git(repo, &["clean", "-fd"])?;
    Ok(())
}

fn preserve_dirty_managed_source(repo: &Path) -> Result<()> {
    let message = format!(
        "sinomo-managed-source-force-overwrite-{}",
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    );
    run_git(
        repo,
        &["stash", "push", "--include-untracked", "-m", &message],
    )?;
    Ok(())
}

fn ensure_clean_git_worktree(repo: &Path) -> Result<()> {
    if git_one_line(repo, &["status", "--porcelain"])?.is_empty() {
        Ok(())
    } else {
        Err(BuildError::Process {
            command: format!("git -C {} status --porcelain", repo.display()),
            status: 1,
            stderr: "managed source worktree has local changes; refusing to overwrite".to_string(),
        })
    }
}

fn remove_all_git_remotes(repo: &Path) -> Result<()> {
    for remote in git_lines(repo, &["remote"])? {
        run_git(repo, &["remote", "remove", &remote])?;
    }
    Ok(())
}

fn require_git_repo(repo: &Path, label: &str) -> Result<()> {
    if repo.join(".git").exists() {
        Ok(())
    } else {
        Err(BuildError::InvalidValue {
            detail: format!("{label} is not initialized at {}", repo.display()),
        })
    }
}

fn git_one_line(repo: &Path, args: &[&str]) -> Result<String> {
    Ok(run_git(repo, args)?
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .to_string())
}

fn git_lines(repo: &Path, args: &[&str]) -> Result<Vec<String>> {
    Ok(run_git(repo, args)?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

fn run_git(repo: &Path, args: &[&str]) -> Result<String> {
    let output = crate::process::noninteractive_command("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .map_err(|source| BuildError::Io {
            path: format!("git -C {} {}", repo.display(), args.join(" ")),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(BuildError::Process {
            command: format!("git -C {} {}", repo.display(), args.join(" ")),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn run_git_paths(repo: &Path, args: &[&str], paths: &[&Path]) -> Result<String> {
    let mut command = crate::process::noninteractive_command("git");
    command.arg("-C").arg(repo).args(args);
    for path in paths {
        command.arg(path);
    }
    let output = command.output().map_err(|source| BuildError::Io {
        path: format!("git -C {} {}", repo.display(), args.join(" ")),
        detail: source.to_string(),
    })?;
    if !output.status.success() {
        return Err(BuildError::Process {
            command: format!("git -C {} {}", repo.display(), args.join(" ")),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn remove_dir_if_exists(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(BuildError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

fn same_path_if_exists(left: &Path, right: &Path) -> bool {
    fs::canonicalize(left)
        .ok()
        .zip(fs::canonicalize(right).ok())
        .is_some_and(|(left, right)| left == right)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepared_source_is_content_addressed_and_reused() {
        let root = sinomo_testdir::TestDir::new("sinomo-build-prepared-source");
        let repo = root.join("repo");
        fs::create_dir_all(&repo).expect("repo");
        run_git(&repo, &["init"]).expect("init");
        run_git(&repo, &["config", "user.name", "Sinomo Build Test"]).expect("name");
        run_git(&repo, &["config", "user.email", "build@sinomo.invalid"]).expect("email");
        fs::write(repo.join("Cargo.toml"), "[workspace]\nresolver = \"3\"\n").expect("manifest");
        fs::write(repo.join("Cargo.lock"), "# lock\n").expect("lock");
        run_git(&repo, &["add", "Cargo.toml", "Cargo.lock"]).expect("add");
        run_git(&repo, &["commit", "-m", "source"]).expect("commit");
        let paths = BuildPaths::new(root.join("data"));

        let first = prepare_build_source(&paths, &repo).expect("first source");
        let second = prepare_build_source(&paths, &repo).expect("second source");

        assert_eq!(first, second);
        assert!(first.source_bundle.is_file());
        assert!(first.source_bundle.starts_with(&paths.sources));
    }

    #[test]
    fn source_bundle_command_forces_deterministic_pack_generation() {
        let command = source_bundle_command(Path::new("source"), Path::new("source.bundle"));
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert_eq!(
            args,
            [
                "-c",
                "pack.threads=1",
                "-C",
                "source",
                "bundle",
                "create",
                "source.bundle",
                "HEAD",
            ]
        );
    }
}
