use crate::{
    RolloutError, SelfRolloutNativeDylibModuleBuild, SelfRolloutPaths,
    default_native_dylib_module_builds, ensure_rollout_layout, rollout_staged_main_binary,
    rollout_staged_shadow_binary, stage_rollout_file,
};
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

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

pub fn rollout_artifact_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{}-{nanos}", std::process::id())
}

pub fn rollout_source_head(repo: &Path) -> Result<String, RolloutError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["rev-parse", "--short=8", "HEAD"])
        .output()
        .map_err(|source| RolloutError::Io {
            path: format!("git -C {} rev-parse --short=8 HEAD", repo.display()),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(RolloutError::Process {
            command: format!("git -C {} rev-parse --short=8 HEAD", repo.display()),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn source_heads_match(actual: Option<&str>, expected: &str) -> bool {
    let Some(actual) = actual else {
        return false;
    };
    actual == expected || actual.starts_with(expected) || expected.starts_with(actual)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RolloutBuiltArtifacts {
    pub built_binary: PathBuf,
    pub native_dylib_modules: Vec<LocalRolloutNativeDylibModuleArtifact>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalRolloutNativeDylibModuleArtifact {
    pub module_id: String,
    pub interface_version: String,
    pub cache_key: String,
    pub library_name: String,
    pub built_library: PathBuf,
    pub source: String,
}

impl From<SelfRolloutNativeDylibModuleBuild> for LocalRolloutNativeDylibModuleArtifact {
    fn from(value: SelfRolloutNativeDylibModuleBuild) -> Self {
        Self {
            module_id: value.module_id,
            interface_version: value.interface_version,
            cache_key: value.cache_key,
            library_name: value.library_name,
            built_library: value.built_library,
            source: value.source,
        }
    }
}

pub fn rollout_build_dev_artifacts(
    repo: &Path,
    build_log: &Path,
) -> Result<RolloutBuiltArtifacts, RolloutError> {
    let modules = default_native_dylib_module_builds(repo);
    let build_args = rollout_build_args(&modules);
    let build = Command::new("cargo")
        .args(&build_args)
        .current_dir(repo)
        .output()
        .map_err(|source| RolloutError::Io {
            path: format!("cargo {}", build_args.join(" ")),
            detail: source.to_string(),
        })?;
    let command = format!("cargo {}", build_args.join(" "));
    let log = format!(
        "command: {command}\nrepo: {}\nstatus: {}\n\nstdout:\n{}\n\nstderr:\n{}\n",
        repo.display(),
        build.status,
        String::from_utf8_lossy(&build.stdout),
        String::from_utf8_lossy(&build.stderr)
    );
    fs::write(build_log, log).map_err(|source| RolloutError::Io {
        path: build_log.display().to_string(),
        detail: source.to_string(),
    })?;
    if !build.status.success() {
        return Err(RolloutError::Process {
            command,
            status: build.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&build.stderr).trim().to_string(),
        });
    }
    let built_binary = repo
        .join("target")
        .join("debug")
        .join(format!("sinomo{}", env::consts::EXE_SUFFIX));
    ensure_artifact_exists(&built_binary)?;
    let native_dylib_modules = modules
        .into_iter()
        .map(LocalRolloutNativeDylibModuleArtifact::from)
        .collect::<Vec<_>>();
    for module in &native_dylib_modules {
        ensure_artifact_exists(&module.built_library)?;
    }
    Ok(RolloutBuiltArtifacts {
        built_binary,
        native_dylib_modules,
    })
}

pub fn rollout_build_dev_binary(repo: &Path, build_log: &Path) -> Result<PathBuf, RolloutError> {
    rollout_build_dev_artifacts(repo, build_log).map(|artifacts| artifacts.built_binary)
}

fn rollout_build_dev_native_dylib_modules(
    repo: &Path,
    build_log: &Path,
) -> Result<Vec<LocalRolloutNativeDylibModuleArtifact>, RolloutError> {
    let modules = default_native_dylib_module_builds(repo);
    let build_args = rollout_module_build_args(&modules);
    let build = Command::new("cargo")
        .args(&build_args)
        .current_dir(repo)
        .output()
        .map_err(|source| RolloutError::Io {
            path: format!("cargo {}", build_args.join(" ")),
            detail: source.to_string(),
        })?;
    let command = format!("cargo {}", build_args.join(" "));
    let log = format!(
        "command: {command}\nrepo: {}\nstatus: {}\n\nstdout:\n{}\n\nstderr:\n{}\n",
        repo.display(),
        build.status,
        String::from_utf8_lossy(&build.stdout),
        String::from_utf8_lossy(&build.stderr)
    );
    fs::write(build_log, log).map_err(|source| RolloutError::Io {
        path: build_log.display().to_string(),
        detail: source.to_string(),
    })?;
    if !build.status.success() {
        return Err(RolloutError::Process {
            command,
            status: build.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&build.stderr).trim().to_string(),
        });
    }
    let artifacts = modules
        .into_iter()
        .map(LocalRolloutNativeDylibModuleArtifact::from)
        .collect::<Vec<_>>();
    for module in &artifacts {
        ensure_artifact_exists(&module.built_library)?;
    }
    Ok(artifacts)
}

fn rollout_build_args(modules: &[SelfRolloutNativeDylibModuleBuild]) -> Vec<String> {
    let mut args = vec!["build".to_string(), "-p".to_string(), "sinomo".to_string()];
    for module in modules {
        args.push("-p".to_string());
        args.push(module.package.clone());
    }
    args
}

fn rollout_module_build_args(modules: &[SelfRolloutNativeDylibModuleBuild]) -> Vec<String> {
    let mut args = vec!["build".to_string()];
    for module in modules {
        args.push("-p".to_string());
        args.push(module.package.clone());
    }
    args
}

fn ensure_artifact_exists(path: &Path) -> Result<(), RolloutError> {
    if path.exists() {
        Ok(())
    } else {
        Err(RolloutError::MissingArtifact {
            artifact: path.display().to_string(),
        })
    }
}

pub fn prepare_rollout_dev_binary(
    paths: &SelfRolloutPaths,
    repo: &Path,
    binary: Option<&Path>,
) -> Result<PathBuf, RolloutError> {
    ensure_rollout_layout(paths)?;
    if let Some(binary) = binary {
        if !binary.exists() {
            return Err(RolloutError::MissingArtifact {
                artifact: binary.display().to_string(),
            });
        }
        return Ok(binary.to_path_buf());
    }
    rollout_build_dev_binary(repo, &paths.plan().build_log)
}

pub fn prepare_rollout_dev_artifacts(
    paths: &SelfRolloutPaths,
    repo: &Path,
    binary: Option<&Path>,
) -> Result<RolloutBuiltArtifacts, RolloutError> {
    ensure_rollout_layout(paths)?;
    if let Some(binary) = binary {
        ensure_artifact_exists(binary)?;
        return Ok(RolloutBuiltArtifacts {
            built_binary: binary.to_path_buf(),
            native_dylib_modules: rollout_build_dev_native_dylib_modules(
                repo,
                &paths.plan().build_log,
            )?,
        });
    }
    rollout_build_dev_artifacts(repo, &paths.plan().build_log)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalRolloutArtifacts {
    pub built_binary: PathBuf,
    pub staged_main_binary: PathBuf,
    pub staged_shadow_binary: PathBuf,
    pub native_dylib_modules: Vec<LocalRolloutNativeDylibModuleArtifact>,
}

pub fn prepare_local_rollout_artifacts(
    paths: &SelfRolloutPaths,
    repo: Option<&Path>,
) -> Result<LocalRolloutArtifacts, RolloutError> {
    ensure_rollout_layout(paths)?;
    ensure_rollout_source_repo(paths, repo)?;
    let plan = paths.plan();
    let built_artifacts = rollout_build_dev_artifacts(&plan.build_dir, &plan.build_log)?;
    stage_local_rollout_artifacts(paths, built_artifacts)
}

pub fn stage_local_rollout_artifacts(
    paths: &SelfRolloutPaths,
    built_artifacts: RolloutBuiltArtifacts,
) -> Result<LocalRolloutArtifacts, RolloutError> {
    let staged_main_binary = rollout_staged_main_binary(paths);
    let staged_shadow_binary = rollout_staged_shadow_binary(paths);
    stage_rollout_file(&built_artifacts.built_binary, &staged_main_binary)?;
    stage_rollout_file(&built_artifacts.built_binary, &staged_shadow_binary)?;
    Ok(LocalRolloutArtifacts {
        built_binary: built_artifacts.built_binary,
        staged_main_binary,
        staged_shadow_binary,
        native_dylib_modules: built_artifacts.native_dylib_modules,
    })
}

pub fn rollout_remote_repo_path(paths: &SelfRolloutPaths, repo: Option<&Path>) -> PathBuf {
    match repo {
        Some(repo) if repo.is_absolute() => repo.to_path_buf(),
        Some(repo) => paths.root.parent().unwrap_or(&paths.root).join(repo),
        None => paths.source_repo.clone(),
    }
}

pub fn ensure_rollout_source_repo(
    paths: &SelfRolloutPaths,
    repo: Option<&Path>,
) -> Result<(), RolloutError> {
    deploy_managed_source_repo_or_current(paths, repo).map(|_| ())
}

pub fn create_source_repo_bundle_bytes(
    paths: &SelfRolloutPaths,
    repo: &Path,
) -> Result<(Vec<u8>, String), RolloutError> {
    let bundle = paths
        .root
        .join("staged")
        .join(format!("source-{}.bundle", rollout_artifact_suffix()));
    create_source_repo_bundle(repo, &bundle)?;
    let bytes = fs::read(&bundle).map_err(|source| RolloutError::Io {
        path: bundle.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok((bytes, rollout_source_head(repo)?))
}

pub fn prepare_rollout_source_bundle(
    paths: &SelfRolloutPaths,
    repo: &Path,
) -> Result<(Vec<u8>, String), RolloutError> {
    ensure_rollout_layout(paths)?;
    create_source_repo_bundle_bytes(paths, repo)
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

pub fn deploy_managed_source_repo_or_current(
    paths: &SelfRolloutPaths,
    repo: Option<&Path>,
) -> Result<PathBuf, RolloutError> {
    let source = match repo {
        Some(repo) => repo.to_path_buf(),
        None => env::current_dir().map_err(|source| RolloutError::Io {
            path: ".".to_string(),
            detail: source.to_string(),
        })?,
    };
    deploy_managed_source_repo(paths, &source)?;
    Ok(source)
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

pub fn rollout_status_view(paths: &SelfRolloutPaths) -> Result<RolloutStatusView, RolloutError> {
    Ok(RolloutStatusView {
        source_repo: paths.source_repo.clone(),
        main_binary: paths.main_binary.clone(),
        shadow_binary: paths.shadow_binary.clone(),
        state: paths.state.clone(),
        source_repo_exists: paths.source_repo.join(".git").exists(),
        main_binary_exists: paths.main_binary.exists(),
        shadow_binary_exists: paths.shadow_binary.exists(),
        source_status: managed_source_status(paths)?,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RolloutStatusView {
    pub source_repo: PathBuf,
    pub main_binary: PathBuf,
    pub shadow_binary: PathBuf,
    pub state: PathBuf,
    pub source_repo_exists: bool,
    pub main_binary_exists: bool,
    pub shadow_binary_exists: bool,
    pub source_status: ManagedSourceStatus,
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
    update_managed_repo_to_fetched_head(&paths.source_repo)?;
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
    update_managed_repo_to_fetched_head(destination_repo)
}

fn update_managed_repo_to_fetched_head(repo: &Path) -> Result<(), RolloutError> {
    if run_git(repo, &["merge", "--ff-only", "FETCH_HEAD"]).is_ok() {
        return Ok(());
    }

    ensure_clean_git_worktree(repo)?;
    let current = git_one_line(repo, &["rev-parse", "HEAD"])?;
    if !current.is_empty() {
        run_git(
            repo,
            &["branch", "-f", "sinomo-managed-source-backup", &current],
        )?;
    }
    run_git(repo, &["reset", "--hard", "FETCH_HEAD"]).map(|_| ())
}

fn ensure_clean_git_worktree(repo: &Path) -> Result<(), RolloutError> {
    let status = run_git(repo, &["status", "--porcelain"])?;
    if status.trim().is_empty() {
        Ok(())
    } else {
        Err(RolloutError::Process {
            command: format!("git -C {} status --porcelain", repo.display()),
            status: 1,
            stderr: "managed source worktree has local changes; refusing to overwrite".to_string(),
        })
    }
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
    fn source_heads_match_accepts_equal_or_prefix_heads() {
        assert!(source_heads_match(Some("abcdef12"), "abcdef12"));
        assert!(source_heads_match(Some("abcdef123456"), "abcdef12"));
        assert!(source_heads_match(Some("abcdef12"), "abcdef123456"));
        assert!(!source_heads_match(Some("abcdef12"), "12345678"));
        assert!(!source_heads_match(None, "abcdef12"));
    }

    #[test]
    fn rollout_remote_repo_path_resolves_relative_repos_under_data_dir() {
        let data_dir = PathBuf::from("/tmp/sinomo-rollout-data");
        let paths = SelfRolloutPaths::new(&data_dir);

        assert_eq!(
            rollout_remote_repo_path(&paths, None),
            paths.source_repo.clone()
        );
        assert_eq!(
            rollout_remote_repo_path(&paths, Some(Path::new("remote-source"))),
            data_dir.join("remote-source")
        );
        assert_eq!(
            rollout_remote_repo_path(&paths, Some(Path::new("/opt/sinomo-source"))),
            PathBuf::from("/opt/sinomo-source")
        );
    }

    #[test]
    fn stage_local_rollout_artifacts_stages_main_and_shadow_binaries() {
        let root = unique_temp("sinomo-rollout-local-artifacts");
        let data = root.join("data");
        let source = root.join(format!("source{}", env::consts::EXE_SUFFIX));
        fs::create_dir_all(&data).expect("data dir");
        fs::write(&source, b"binary").expect("source binary");
        let paths = SelfRolloutPaths::new(&data);
        ensure_rollout_layout(&paths).expect("layout");

        let module_artifacts = vec![LocalRolloutNativeDylibModuleArtifact {
            module_id: "module.test".to_string(),
            interface_version: "test.v1".to_string(),
            cache_key: "module/test".to_string(),
            library_name: "libtest.so".to_string(),
            built_library: root.join("target").join("debug").join("libtest.so"),
            source: "test".to_string(),
        }];
        let artifacts = stage_local_rollout_artifacts(
            &paths,
            RolloutBuiltArtifacts {
                built_binary: source.clone(),
                native_dylib_modules: module_artifacts.clone(),
            },
        )
        .expect("local artifacts");

        assert_eq!(artifacts.built_binary, source);
        assert_eq!(artifacts.native_dylib_modules, module_artifacts);
        assert_eq!(
            artifacts.staged_main_binary,
            rollout_staged_main_binary(&paths)
        );
        assert_eq!(
            artifacts.staged_shadow_binary,
            rollout_staged_shadow_binary(&paths)
        );
        assert_eq!(
            fs::read(&artifacts.staged_main_binary).expect("main staged"),
            b"binary"
        );
        assert_eq!(
            fs::read(&artifacts.staged_shadow_binary).expect("shadow staged"),
            b"binary"
        );
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn create_source_repo_bundle_bytes_returns_bundle_and_head() {
        let root = unique_temp("sinomo-rollout-source-bundle-bytes");
        let external = root.join("external");
        let data = root.join("data");
        fs::create_dir_all(&external).expect("external dir");
        run_git_for_test(&external, &["init"]);
        run_git_for_test(&external, &["config", "user.email", "test@example.invalid"]);
        run_git_for_test(&external, &["config", "user.name", "Sinomo Test"]);
        fs::write(external.join("Cargo.toml"), "[workspace]\n").expect("cargo");
        run_git_for_test(&external, &["add", "-A"]);
        run_git_for_test(&external, &["commit", "-m", "initial"]);
        let expected_head =
            git_one_line(&external, &["rev-parse", "--short=8", "HEAD"]).expect("head");

        let paths = SelfRolloutPaths::new(&data);
        let (bundle, head) = create_source_repo_bundle_bytes(&paths, &external).expect("bundle");

        assert!(!bundle.is_empty());
        assert_eq!(head, expected_head);
        assert!(paths.root.join("staged").exists());
        fs::remove_dir_all(root).ok();
    }

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
    fn deploy_managed_source_bundle_replaces_diverged_clean_managed_repo() {
        let root = unique_temp("sinomo-rollout-source-bundle-diverged");
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
        fs::write(paths.source_repo.join("local.txt"), "managed-only\n").expect("local file");
        run_git_for_test(&paths.source_repo, &["add", "-A"]);
        run_git_for_test(&paths.source_repo, &["commit", "-m", "managed-only"]);
        let diverged_head =
            git_one_line(&paths.source_repo, &["rev-parse", "HEAD"]).expect("diverged head");

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
        let backup = git_one_line(
            &paths.source_repo,
            &["rev-parse", "sinomo-managed-source-backup"],
        )
        .expect("backup");

        assert_eq!(status.head.as_deref(), Some(expected_head.as_str()));
        assert_eq!(backup, diverged_head);
        assert!(!paths.source_repo.join("local.txt").exists());
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
