use crate::{
    RolloutError, SelfRolloutPaths, ensure_rollout_layout, rollout_staged_main_binary,
    rollout_staged_shadow_binary, stage_rollout_file_pair,
};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(all(test, windows))]
fn rollout_child_command(program: impl AsRef<std::ffi::OsStr>) -> std::process::Command {
    let mut command = std::process::Command::new(program);
    use std::os::windows::process::CommandExt;

    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    command.creation_flags(CREATE_NO_WINDOW);
    command
}

#[cfg(all(test, not(windows)))]
fn rollout_child_command(program: impl AsRef<std::ffi::OsStr>) -> std::process::Command {
    std::process::Command::new(program)
}

pub fn discover_current_git_repo() -> Result<Option<PathBuf>, RolloutError> {
    sinomo_build::discover_current_git_repo().map_err(Into::into)
}

pub fn rollout_artifact_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{}-{nanos}", std::process::id())
}

pub fn rollout_source_head(repo: &Path) -> Result<String, RolloutError> {
    let head = sinomo_build::build_source_head(repo)?;
    Ok(head.chars().take(8).collect())
}

/// Resolve a full or abbreviated Git object name to its unique canonical
/// commit. Git performs the abbreviation ambiguity check; callers must not
/// treat a textual prefix match as artifact identity.
pub fn resolve_rollout_source_commit(repo: &Path, revision: &str) -> Result<String, RolloutError> {
    sinomo_build::resolve_build_source_commit(repo, revision).map_err(Into::into)
}

pub fn source_heads_match(actual: Option<&str>, expected: &str) -> bool {
    sinomo_build::build_source_heads_match(actual, expected)
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

pub fn rollout_build_artifacts(
    repo: &Path,
    build_log: &Path,
) -> Result<RolloutBuiltArtifacts, RolloutError> {
    execute_rollout_build(
        repo,
        build_log,
        sinomo_build::LocalBuildOutputSelection::Full,
    )
}

pub fn rollout_build_binary(repo: &Path, build_log: &Path) -> Result<PathBuf, RolloutError> {
    Ok(execute_rollout_build(
        repo,
        build_log,
        sinomo_build::LocalBuildOutputSelection::Binary,
    )?
    .built_binary)
}

fn rollout_build_native_dylib_modules(
    repo: &Path,
    build_log: &Path,
) -> Result<Vec<LocalRolloutNativeDylibModuleArtifact>, RolloutError> {
    Ok(execute_rollout_build(
        repo,
        build_log,
        sinomo_build::LocalBuildOutputSelection::DefaultNativeModules,
    )?
    .native_dylib_modules)
}

pub fn rollout_build_native_dylib_module(
    repo: &Path,
    build_log: &Path,
    selector: &str,
) -> Result<LocalRolloutNativeDylibModuleArtifact, RolloutError> {
    let mut modules = execute_rollout_build(
        repo,
        build_log,
        sinomo_build::LocalBuildOutputSelection::NativeModule(selector.to_string()),
    )?
    .native_dylib_modules;
    modules.pop().ok_or_else(|| RolloutError::InvalidArtifact {
        detail: format!("build service returned no native module for `{selector}`"),
    })
}

fn execute_rollout_build(
    repo: &Path,
    build_log: &Path,
    selection: sinomo_build::LocalBuildOutputSelection,
) -> Result<RolloutBuiltArtifacts, RolloutError> {
    let requires_binary = matches!(
        &selection,
        sinomo_build::LocalBuildOutputSelection::Full
            | sinomo_build::LocalBuildOutputSelection::Binary
    );
    let build_root = build_log.parent().ok_or_else(|| RolloutError::Io {
        path: build_log.display().to_string(),
        detail: "build log has no parent directory".to_string(),
    })?;
    let paths = sinomo_build::BuildPaths::from_build_root(build_root);
    let result = sinomo_build::execute_local_build(&paths, repo, selection)?;
    if result.build_log != build_log {
        fs::copy(&result.build_log, build_log).map_err(|source| RolloutError::Io {
            path: build_log.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let mut built_binary = None;
    let mut native_dylib_modules = Vec::new();
    for artifact in &result.manifest.artifacts {
        let path = result.artifact_path(&artifact.artifact_id).ok_or_else(|| {
            RolloutError::MissingArtifact {
                artifact: artifact.artifact_id.clone(),
            }
        })?;
        ensure_artifact_exists(&path)?;
        match artifact.kind {
            sinomo_build::BuildArtifactKind::Binary => {
                if built_binary.replace(path).is_some() {
                    return Err(RolloutError::InvalidArtifact {
                        detail: "build service returned multiple rollout binaries".to_string(),
                    });
                }
            }
            sinomo_build::BuildArtifactKind::NativeDylibModule => {
                let metadata = artifact.native_dylib.as_ref().ok_or_else(|| {
                    RolloutError::InvalidArtifact {
                        detail: format!(
                            "native module artifact {} has no loader metadata",
                            artifact.artifact_id
                        ),
                    }
                })?;
                native_dylib_modules.push(LocalRolloutNativeDylibModuleArtifact {
                    module_id: metadata.module_id.to_string(),
                    interface_version: metadata.interface_version.clone(),
                    cache_key: metadata.cache_key.clone(),
                    library_name: metadata.library_name.clone(),
                    built_library: path,
                    source: metadata.source.clone(),
                });
            }
        }
    }
    native_dylib_modules.sort_by(|left, right| left.module_id.cmp(&right.module_id));
    let built_binary = match (requires_binary, built_binary) {
        (true, None) => {
            return Err(RolloutError::InvalidArtifact {
                detail: "build service returned no rollout binary".to_string(),
            });
        }
        (_, built_binary) => built_binary.unwrap_or_default(),
    };
    Ok(RolloutBuiltArtifacts {
        built_binary,
        native_dylib_modules,
    })
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

pub fn prepare_rollout_binary(
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
    rollout_build_binary(repo, &paths.plan().build_log)
}

pub fn prepare_rollout_artifacts(
    paths: &SelfRolloutPaths,
    repo: &Path,
    binary: Option<&Path>,
) -> Result<RolloutBuiltArtifacts, RolloutError> {
    ensure_rollout_layout(paths)?;
    if let Some(binary) = binary {
        ensure_artifact_exists(binary)?;
        return Ok(RolloutBuiltArtifacts {
            built_binary: binary.to_path_buf(),
            native_dylib_modules: rollout_build_native_dylib_modules(
                repo,
                &paths.plan().build_log,
            )?,
        });
    }
    rollout_build_artifacts(repo, &paths.plan().build_log)
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
    prepare_local_rollout_artifacts_with_options(paths, repo, ManagedSourceDeployOptions::default())
}

pub fn prepare_local_rollout_artifacts_with_options(
    paths: &SelfRolloutPaths,
    repo: Option<&Path>,
    source_options: ManagedSourceDeployOptions,
) -> Result<LocalRolloutArtifacts, RolloutError> {
    ensure_rollout_layout(paths)?;
    ensure_rollout_source_repo_with_options(paths, repo, source_options)?;
    let plan = paths.plan();
    let built_artifacts = rollout_build_artifacts(&plan.source_repo, &plan.build_log)?;
    stage_local_rollout_artifacts(paths, built_artifacts)
}

pub fn stage_local_rollout_artifacts(
    paths: &SelfRolloutPaths,
    built_artifacts: RolloutBuiltArtifacts,
) -> Result<LocalRolloutArtifacts, RolloutError> {
    let staged_main_binary = rollout_staged_main_binary(paths);
    let staged_shadow_binary = rollout_staged_shadow_binary(paths);
    stage_rollout_file_pair(
        &built_artifacts.built_binary,
        &staged_main_binary,
        &staged_shadow_binary,
    )?;
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

fn build_paths(paths: &SelfRolloutPaths) -> sinomo_build::BuildPaths {
    sinomo_build::BuildPaths::new(paths.root.parent().unwrap_or(&paths.root))
}

pub type ManagedSourceDeployOptions = sinomo_build::ManagedSourceDeployOptions;

pub fn ensure_rollout_source_repo(
    paths: &SelfRolloutPaths,
    repo: Option<&Path>,
) -> Result<(), RolloutError> {
    ensure_rollout_source_repo_with_options(paths, repo, ManagedSourceDeployOptions::default())
}

pub fn ensure_rollout_source_repo_with_options(
    paths: &SelfRolloutPaths,
    repo: Option<&Path>,
    options: ManagedSourceDeployOptions,
) -> Result<(), RolloutError> {
    deploy_managed_source_repo_or_current_with_options(paths, repo, options).map(|_| ())
}

pub fn create_source_repo_bundle_bytes(
    paths: &SelfRolloutPaths,
    repo: &Path,
) -> Result<(Vec<u8>, String), RolloutError> {
    let bundle = sinomo_build::prepare_source_bundle(&build_paths(paths), repo)?;
    let bytes = fs::read(&bundle.path).map_err(|source| RolloutError::Io {
        path: bundle.path.display().to_string(),
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
    deploy_managed_source_repo_with_options(
        paths,
        source_repo,
        ManagedSourceDeployOptions::default(),
    )
}

pub fn deploy_managed_source_repo_with_options(
    paths: &SelfRolloutPaths,
    source_repo: &Path,
    options: ManagedSourceDeployOptions,
) -> Result<(), RolloutError> {
    sinomo_build::deploy_managed_source_repo(&paths.source_repo, source_repo, options)
        .map_err(Into::into)
}

pub fn sync_external_repo_from_managed(
    paths: &SelfRolloutPaths,
    external_repo: &Path,
) -> Result<(), RolloutError> {
    sinomo_build::sync_external_repo_from_managed(&paths.source_repo, external_repo)
        .map_err(Into::into)
}

pub fn deploy_managed_source_repo_or_current(
    paths: &SelfRolloutPaths,
    repo: Option<&Path>,
) -> Result<PathBuf, RolloutError> {
    deploy_managed_source_repo_or_current_with_options(
        paths,
        repo,
        ManagedSourceDeployOptions::default(),
    )
}

pub fn deploy_managed_source_repo_or_current_with_options(
    paths: &SelfRolloutPaths,
    repo: Option<&Path>,
    options: ManagedSourceDeployOptions,
) -> Result<PathBuf, RolloutError> {
    let source = match repo {
        Some(repo) => repo.to_path_buf(),
        None => env::current_dir().map_err(|source| RolloutError::Io {
            path: ".".to_string(),
            detail: source.to_string(),
        })?,
    };
    deploy_managed_source_repo_with_options(paths, &source, options)?;
    Ok(source)
}

pub fn create_source_repo_bundle(
    source_repo: &Path,
    bundle_path: &Path,
) -> Result<(), RolloutError> {
    sinomo_build::create_source_repo_bundle(source_repo, bundle_path).map_err(Into::into)
}

pub fn deploy_managed_source_bundle(
    paths: &SelfRolloutPaths,
    bundle_path: &Path,
) -> Result<(), RolloutError> {
    deploy_managed_source_bundle_with_options(
        paths,
        bundle_path,
        ManagedSourceDeployOptions::default(),
    )
}

pub fn deploy_managed_source_bundle_with_options(
    paths: &SelfRolloutPaths,
    bundle_path: &Path,
    options: ManagedSourceDeployOptions,
) -> Result<(), RolloutError> {
    sinomo_build::deploy_managed_source_bundle(&paths.source_repo, bundle_path, options)
        .map_err(Into::into)
}

pub fn managed_source_status(
    paths: &SelfRolloutPaths,
) -> Result<ManagedSourceStatus, RolloutError> {
    sinomo_build::managed_source_status(&paths.source_repo).map_err(Into::into)
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

pub type ManagedSourceStatus = sinomo_build::ManagedSourceStatus;

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

pub fn remove_dir_if_exists(path: &Path) -> Result<(), RolloutError> {
    sinomo_build::remove_dir_if_exists(path).map_err(Into::into)
}

#[cfg(test)]
fn git_one_line(repo: &Path, args: &[&str]) -> Result<String, RolloutError> {
    let output = rollout_child_command("git")
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
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SelfRolloutPaths;

    #[test]
    fn source_heads_match_accepts_equal_or_prefix_heads() {
        assert!(source_heads_match(Some("abcdef12"), "abcdef12"));
        assert!(source_heads_match(Some("abcdef123456"), "abcdef12"));
        assert!(source_heads_match(Some("abcdef12"), "abcdef123456"));
        assert!(!source_heads_match(Some("abcdef12"), "12345678"));
        assert!(!source_heads_match(None, "abcdef12"));
    }

    #[test]
    fn source_commit_resolution_is_unambiguous_across_lengths_and_synthetic_heads() {
        let root = unique_temp("sinomo-rollout-source-commit-resolution");
        let repo = root.join("repo");
        fs::create_dir_all(&repo).expect("repo dir");
        run_git_for_test(&repo, &["init"]);
        run_git_for_test(&repo, &["config", "user.email", "test@example.invalid"]);
        run_git_for_test(&repo, &["config", "user.name", "Sinomo Test"]);
        fs::write(repo.join("tracked.txt"), "first\n").expect("tracked file");
        run_git_for_test(&repo, &["add", "tracked.txt"]);
        run_git_for_test(&repo, &["commit", "-m", "initial"]);

        let full = git_one_line(&repo, &["rev-parse", "HEAD"]).expect("full head");
        assert_eq!(
            resolve_rollout_source_commit(&repo, &full[..8]).expect("unique 8-char head"),
            full
        );
        assert_eq!(
            resolve_rollout_source_commit(&repo, &full[..12]).expect("unique 12-char head"),
            full
        );
        assert_eq!(
            resolve_rollout_source_commit(&repo, &full).expect("full head"),
            full
        );

        fs::write(repo.join("tracked.txt"), "synthetic snapshot\n").expect("dirty tracked file");
        let synthetic = git_one_line(&repo, &["stash", "create", "rollout snapshot"])
            .expect("synthetic snapshot commit");
        assert!(!synthetic.is_empty());
        assert_eq!(
            resolve_rollout_source_commit(&repo, &synthetic[..12])
                .expect("synthetic commit remains resolvable"),
            synthetic
        );
        assert!(resolve_rollout_source_commit(&repo, "unknown").is_err());
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
        assert!(build_paths(&paths).sources.exists());
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
    }

    #[test]
    fn deploy_managed_source_repo_imports_detached_worktree_snapshot() {
        let root = unique_temp("sinomo-rollout-detached-source-import");
        let external = root.join("external");
        let detached = root.join("detached-snapshot");
        let data = root.join("data");
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
        run_git_for_test(&external, &["commit", "-m", "snapshot"]);
        let snapshot_head = git_one_line(&external, &["rev-parse", "HEAD"]).expect("head");
        let detached_path = detached.display().to_string();
        run_git_for_test(
            &external,
            &[
                "worktree",
                "add",
                "--detach",
                &detached_path,
                &snapshot_head,
            ],
        );

        deploy_managed_source_repo_with_options(
            &paths,
            &detached,
            ManagedSourceDeployOptions {
                force_overwrite: true,
            },
        )
        .expect("detached snapshot import");
        // The apply phase points back at this managed repository. Re-selecting
        // it must be a no-op rather than importing the caller's current repo.
        deploy_managed_source_repo_with_options(
            &paths,
            &paths.source_repo,
            ManagedSourceDeployOptions {
                force_overwrite: true,
            },
        )
        .expect("managed source re-selection");

        let status = managed_source_status(&paths).expect("status");
        assert_eq!(status.head.as_deref(), Some(snapshot_head.as_str()));
        assert_eq!(
            fs::read_to_string(paths.source_repo.join("Cargo.toml")).expect("manifest"),
            "[workspace]\nresolver = \"3\"\n"
        );
        assert!(!status.has_external_remotes);
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
        run_git_for_test(
            &paths.source_repo,
            &[
                "-c",
                "user.name=Sinomo Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "-m",
                "managed-only",
            ],
        );
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
    }

    #[test]
    fn deploy_managed_source_bundle_force_overwrite_stashes_dirty_managed_repo() {
        let root = unique_temp("sinomo-rollout-source-bundle-force");
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
        let local_manifest = "[workspace]\nlocal = true\n";
        fs::write(paths.source_repo.join("Cargo.toml"), local_manifest).expect("dirty cargo");
        fs::write(paths.source_repo.join("untracked.txt"), "local only\n").expect("untracked");

        let external_manifest = "[workspace]\nresolver = \"3\"\n";
        fs::write(external.join("Cargo.toml"), external_manifest).expect("cargo update");
        run_git_for_test(&external, &["add", "-A"]);
        run_git_for_test(&external, &["commit", "-m", "second"]);
        let expected_head = git_one_line(&external, &["rev-parse", "HEAD"]).expect("expected head");
        create_source_repo_bundle(&external, &bundle).expect("source bundle");

        let error = deploy_managed_source_bundle(&paths, &bundle)
            .expect_err("dirty managed source should require force");
        assert!(
            error
                .to_string()
                .contains("managed source worktree has local changes")
        );
        assert_eq!(
            fs::read_to_string(paths.source_repo.join("Cargo.toml")).expect("dirty manifest"),
            local_manifest
        );

        deploy_managed_source_bundle_with_options(
            &paths,
            &bundle,
            ManagedSourceDeployOptions {
                force_overwrite: true,
            },
        )
        .expect("force bundle deploy");
        let status = managed_source_status(&paths).expect("status");
        let stash = git_one_line(&paths.source_repo, &["stash", "list", "--max-count=1"])
            .expect("stash list");

        assert_eq!(status.head.as_deref(), Some(expected_head.as_str()));
        assert_eq!(
            fs::read_to_string(paths.source_repo.join("Cargo.toml")).expect("manifest"),
            external_manifest
        );
        assert!(!paths.source_repo.join("untracked.txt").exists());
        assert!(stash.contains("sinomo-managed-source-force-overwrite-"));
    }

    #[test]
    fn deploy_managed_source_bundle_force_overwrite_resets_dirty_same_head() {
        let root = unique_temp("sinomo-rollout-source-bundle-force-same-head");
        let external = root.join("external");
        let data = root.join("data");
        let bundle = root.join("source.bundle");
        fs::create_dir_all(&external).expect("external dir");
        run_git_for_test(&external, &["init"]);
        run_git_for_test(&external, &["config", "user.email", "test@example.invalid"]);
        run_git_for_test(&external, &["config", "user.name", "Sinomo Test"]);
        let external_manifest = "[workspace]\n";
        fs::write(external.join("Cargo.toml"), external_manifest).expect("cargo");
        run_git_for_test(&external, &["add", "-A"]);
        run_git_for_test(&external, &["commit", "-m", "initial"]);
        let expected_head = git_one_line(&external, &["rev-parse", "HEAD"]).expect("expected head");

        let paths = SelfRolloutPaths::new(&data);
        deploy_managed_source_repo(&paths, &external).expect("initial deploy");
        fs::write(
            paths.source_repo.join("Cargo.toml"),
            "[workspace]\nlocal = true\n",
        )
        .expect("dirty cargo");
        fs::write(paths.source_repo.join("untracked.txt"), "local only\n").expect("untracked");
        create_source_repo_bundle(&external, &bundle).expect("source bundle");

        deploy_managed_source_bundle_with_options(
            &paths,
            &bundle,
            ManagedSourceDeployOptions {
                force_overwrite: true,
            },
        )
        .expect("force bundle deploy");
        let status = managed_source_status(&paths).expect("status");
        let stash = git_one_line(&paths.source_repo, &["stash", "list", "--max-count=1"])
            .expect("stash list");

        assert_eq!(status.head.as_deref(), Some(expected_head.as_str()));
        assert_eq!(
            fs::read_to_string(paths.source_repo.join("Cargo.toml")).expect("manifest"),
            external_manifest
        );
        assert!(!paths.source_repo.join("untracked.txt").exists());
        assert!(stash.contains("sinomo-managed-source-force-overwrite-"));
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
    }

    #[test]
    fn deploy_managed_source_repo_ignores_sibling_repositories() {
        let root = unique_temp("sinomo-rollout-source-sibling");
        let external_root = root.join("external");
        let external = external_root.join("sinomo");
        let external_sui = external_root.join("sui");
        let data = root.join("data");

        fs::create_dir_all(&external).expect("external source dir");
        run_git_for_test(&external, &["init"]);
        run_git_for_test(&external, &["config", "user.email", "test@example.invalid"]);
        run_git_for_test(&external, &["config", "user.name", "Sinomo Test"]);
        fs::write(external.join("Cargo.toml"), "[workspace]\n").expect("cargo");
        run_git_for_test(&external, &["add", "-A"]);
        run_git_for_test(&external, &["commit", "-m", "initial"]);

        fs::create_dir_all(external_sui.join("crates").join("sui")).expect("sui crate");
        fs::write(
            external_sui.join("Cargo.toml"),
            "[workspace]\nmembers = [\"crates/*\"]\n",
        )
        .expect("sui workspace");
        fs::write(
            external_sui.join("crates").join("sui").join("Cargo.toml"),
            "[package]\nname = \"sui\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
        )
        .expect("sui manifest");
        run_git_for_test(&external_sui, &["init"]);
        run_git_for_test(
            &external_sui,
            &["config", "user.email", "test@example.invalid"],
        );
        run_git_for_test(&external_sui, &["config", "user.name", "Sinomo Test"]);
        run_git_for_test(&external_sui, &["add", "-A"]);
        run_git_for_test(&external_sui, &["commit", "-m", "initial"]);
        let paths = SelfRolloutPaths::new(&data);
        deploy_managed_source_repo(&paths, &external).expect("deploy");
        let managed_sui = paths
            .source_repo
            .parent()
            .expect("managed source parent")
            .join("sui");

        assert!(!managed_sui.exists());
    }

    fn unique_temp(prefix: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(prefix)
    }

    fn run_git_for_test(repo: &Path, args: &[&str]) {
        let output = rollout_child_command("git")
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
