use crate::{
    BuildArtifactKind, BuildArtifactSetManifest, BuildError, BuildExecutionPolicy, BuildJobId,
    BuildKey, BuildKeyInput, BuildOutputRequest, BuildPaths, BuildPlan, CargoBuildExecutor,
    NativeModuleSelection, PreparedBuildSource, Result, build_source_heads_match,
    discover_native_dylib_outputs, prepare_build_source, sample_build_resources,
};
use fs2::FileExt as _;
use sinomo_core::network::{read_object_file, read_optional_object_file, write_object_file};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

pub const DEFAULT_BUILD_PROFILE: &str = "release";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalBuildOutputSelection {
    Full,
    Binary,
    DefaultNativeModules,
    NativeModule(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedBuildResult {
    pub plan: BuildPlan,
    pub manifest: BuildArtifactSetManifest,
    pub artifact_root: PathBuf,
    pub build_log: PathBuf,
    pub cache_hit: bool,
}

impl ManagedBuildResult {
    pub fn artifact_path(&self, artifact_id: &str) -> Option<PathBuf> {
        self.manifest
            .artifact(artifact_id)
            .map(|artifact| self.artifact_root.join(&artifact.relative_path))
    }
}

pub fn execute_local_build(
    paths: &BuildPaths,
    source_repo: &Path,
    selection: LocalBuildOutputSelection,
) -> Result<ManagedBuildResult> {
    execute_local_build_with_profile(paths, source_repo, selection, DEFAULT_BUILD_PROFILE)
}

pub fn execute_local_build_with_profile(
    paths: &BuildPaths,
    source_repo: &Path,
    selection: LocalBuildOutputSelection,
    profile: &str,
) -> Result<ManagedBuildResult> {
    let prepared = prepare_build_source(paths, source_repo)?;
    let cargo_program = std::env::var_os("CARGO")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("cargo"));
    let executor = crate::service::configured_build_executor(&cargo_program, paths)
        .map_err(|detail| BuildError::InvalidValue { detail })?;
    let requested_host = crate::BuildTarget::host();
    let capability = executor
        .supported_targets()
        .iter()
        .find(|capability| requested_host.is_supported_by(&capability.target))
        .ok_or_else(|| BuildError::InvalidValue {
            detail: format!(
                "local build executor does not support host target {} / {}",
                requested_host.os, requested_host.arch
            ),
        })?;
    let outputs = local_build_outputs(&cargo_program, source_repo, &capability.target, selection)?;
    let plan = BuildPlan::new(
        prepared.source.clone(),
        BuildKeyInput {
            source_hash: prepared.source.build_key_source_hash().to_string(),
            source_revision: prepared.source.revision.clone(),
            lockfile_hash: prepared.lockfile_hash.clone(),
            target: capability.target.clone(),
            profile: profile.to_string(),
            features: Vec::new(),
            toolchain_fingerprint: capability.toolchain_fingerprint.clone(),
            outputs,
        },
    )?;
    execute_prepared_local_build(paths, &prepared, plan, &executor, &cargo_program)
}

fn local_build_outputs(
    cargo_program: &Path,
    source_repo: &Path,
    target: &crate::BuildTarget,
    selection: LocalBuildOutputSelection,
) -> Result<Vec<BuildOutputRequest>> {
    let (native_selection, include_binary) = match &selection {
        LocalBuildOutputSelection::Full => (Some(NativeModuleSelection::Default), true),
        LocalBuildOutputSelection::Binary => (None, true),
        LocalBuildOutputSelection::DefaultNativeModules => {
            (Some(NativeModuleSelection::Default), false)
        }
        LocalBuildOutputSelection::NativeModule(_) => (Some(NativeModuleSelection::All), false),
    };
    let mut outputs = match native_selection {
        Some(selection) => {
            discover_native_dylib_outputs(cargo_program, source_repo, target, selection)?
        }
        None => Vec::new(),
    };
    if let LocalBuildOutputSelection::NativeModule(selector) = selection {
        outputs.retain(|output| build_output_matches_selector(output, &selector));
        match outputs.len() {
            1 => {}
            0 => {
                return Err(BuildError::InvalidValue {
                    detail: format!(
                        "no native-dylib module matches `{selector}` for target {}",
                        target.path_key()
                    ),
                });
            }
            _ => {
                return Err(BuildError::InvalidValue {
                    detail: format!(
                        "native-dylib module selector `{selector}` matched multiple modules: {}",
                        outputs
                            .iter()
                            .map(|output| output.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                });
            }
        }
    }
    if include_binary {
        outputs.push(BuildOutputRequest::binary("sinomo", "sinomo"));
    }
    outputs.sort();
    outputs.dedup();
    if outputs.is_empty() {
        return Err(BuildError::InvalidValue {
            detail: "local build selected no outputs".to_string(),
        });
    }
    Ok(outputs)
}

fn build_output_matches_selector(output: &BuildOutputRequest, selector: &str) -> bool {
    let selector_module_id = sinomo_module::ModuleId::new(selector.to_string())
        .or_else(|_| sinomo_module::ModuleId::from_legacy_named(selector))
        .ok();
    output.name == selector
        || output.package == selector
        || output.native_dylib.as_ref().is_some_and(|metadata| {
            Some(&metadata.module_id) == selector_module_id.as_ref()
                || metadata.cache_key == selector
                || metadata.source == selector
        })
}

fn execute_prepared_local_build(
    paths: &BuildPaths,
    prepared: &PreparedBuildSource,
    plan: BuildPlan,
    executor: &CargoBuildExecutor,
    cargo_program: &Path,
) -> Result<ManagedBuildResult> {
    if let Some(manifest) = cached_build_manifest(paths, &plan.key, true)? {
        fs::write(
            &paths.local_log,
            format!("build key: {}\nexact artifact-set cache hit\n", plan.key),
        )
        .map_err(|source| BuildError::Io {
            path: paths.local_log.display().to_string(),
            detail: source.to_string(),
        })?;
        return Ok(ManagedBuildResult {
            artifact_root: paths.cache_artifacts_dir(&plan.key),
            build_log: paths.local_log.clone(),
            manifest,
            plan,
            cache_hit: true,
        });
    }

    let job_id = BuildJobId::generate();
    let job_dir = paths.job_dir(&job_id);
    fs::create_dir_all(&job_dir).map_err(|source| BuildError::Io {
        path: job_dir.display().to_string(),
        detail: source.to_string(),
    })?;
    copy_or_link(&prepared.source_bundle, &paths.job_source_bundle(&job_id))?;
    let manifest = execute_and_cache_build(paths, &job_id, &plan, executor, cargo_program)?;
    let job_log = paths.job_log(&job_id);
    fs::copy(&job_log, &paths.local_log).map_err(|source| BuildError::Io {
        path: paths.local_log.display().to_string(),
        detail: source.to_string(),
    })?;
    let _ = fs::remove_dir_all(paths.job_work_dir(&job_id));
    let _ = fs::remove_dir_all(job_dir);
    Ok(ManagedBuildResult {
        artifact_root: paths.cache_artifacts_dir(&plan.key),
        build_log: paths.local_log.clone(),
        manifest,
        plan,
        cache_hit: false,
    })
}

fn copy_or_link(source: &Path, destination: &Path) -> Result<()> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|source| BuildError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    match fs::hard_link(source, destination) {
        Ok(()) => Ok(()),
        Err(_) => fs::copy(source, destination)
            .map(|_| ())
            .map_err(|source| BuildError::Io {
                path: destination.display().to_string(),
                detail: source.to_string(),
            }),
    }
}

pub(crate) fn execute_and_cache_build(
    paths: &BuildPaths,
    job_id: &BuildJobId,
    plan: &BuildPlan,
    executor: &CargoBuildExecutor,
    cargo_program: &Path,
) -> Result<BuildArtifactSetManifest> {
    let work_dir = paths.job_work_dir(job_id);
    if work_dir.exists() {
        fs::remove_dir_all(&work_dir).map_err(|source| BuildError::Io {
            path: work_dir.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    fs::create_dir_all(&work_dir).map_err(|source| BuildError::Io {
        path: work_dir.display().to_string(),
        detail: source.to_string(),
    })?;
    let resources = sample_build_resources(paths, 0, 1, 0);
    let policy = BuildExecutionPolicy::for_source_bytes(&resources, plan.source.bundle_bytes)?;
    let checkout_root = paths.checkout_root(&plan.source.label);
    let _checkout_lock = BuildPathLock::acquire(&checkout_root.with_extension("lock"))?;
    let source_repo =
        materialize_build_source_bundle(&checkout_root, &paths.job_source_bundle(job_id))?;
    if let Some(expected_revision) = plan.source.revision.as_deref() {
        let actual_revision = crate::build_source_head(&source_repo)?;
        if !build_source_heads_match(Some(actual_revision.as_str()), expected_revision) {
            return Err(BuildError::InvalidValue {
                detail: format!(
                    "imported source revision {actual_revision} does not match requested {expected_revision}"
                ),
            });
        }
    }
    verify_build_lockfile(&source_repo, plan)?;
    validate_requested_outputs(cargo_program, &source_repo, plan)?;

    let target_dir = paths.incremental_target_dir(plan);
    let _incremental_lock = BuildPathLock::acquire(&target_dir.join(".sinomo-build.lock"))?;
    let artifact_root = work_dir.join("artifact-set");
    let result = executor.execute_with_target_dir(
        &source_repo,
        plan,
        &artifact_root,
        &target_dir,
        Some(policy.cargo_jobs),
    )?;
    let log = format!(
        "command: {}\nrepo: {}\nbuild key: {}\ncargo jobs: {}\nincremental target: {}\n\nstdout:\n{}\n\nstderr:\n{}\n",
        result.command.display(),
        source_repo.display(),
        plan.key,
        policy.cargo_jobs,
        target_dir.display(),
        result.stdout,
        result.stderr
    );
    fs::write(paths.job_log(job_id), log).map_err(|source| BuildError::Io {
        path: paths.job_log(job_id).display().to_string(),
        detail: source.to_string(),
    })?;
    publish_artifact_cache(
        paths,
        plan,
        &result.artifact_root,
        result.artifact_set,
        job_id,
    )
}

struct BuildPathLock {
    file: File,
}

impl BuildPathLock {
    fn acquire(path: &Path) -> Result<Self> {
        let parent = path.parent().ok_or_else(|| BuildError::Io {
            path: path.display().to_string(),
            detail: "build lock path has no parent directory".to_string(),
        })?;
        fs::create_dir_all(parent).map_err(|source| BuildError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map_err(|source| BuildError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        file.lock_exclusive().map_err(|source| BuildError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(Self { file })
    }
}

impl Drop for BuildPathLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

pub(crate) fn materialize_build_source_bundle(
    checkout_root: &Path,
    bundle_path: &Path,
) -> Result<PathBuf> {
    if !bundle_path.is_file() {
        return Err(BuildError::Io {
            path: bundle_path.display().to_string(),
            detail: "build source bundle does not exist".to_string(),
        });
    }
    let source_repo = checkout_root.join("sinomo");
    if source_repo.exists() {
        fs::remove_dir_all(&source_repo).map_err(|source| BuildError::Io {
            path: source_repo.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let parent = source_repo
        .parent()
        .expect("build source repository has a parent");
    fs::create_dir_all(parent).map_err(|source| BuildError::Io {
        path: parent.display().to_string(),
        detail: source.to_string(),
    })?;
    let output = crate::process::noninteractive_command("git")
        .arg("clone")
        .arg(bundle_path)
        .arg(&source_repo)
        .output()
        .map_err(|source| BuildError::Io {
            path: bundle_path.display().to_string(),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(BuildError::Process {
            command: format!(
                "git clone {} {}",
                bundle_path.display(),
                source_repo.display()
            ),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    remove_build_source_remotes(&source_repo)?;
    Ok(source_repo)
}

fn remove_build_source_remotes(repo: &Path) -> Result<()> {
    let output = crate::process::noninteractive_command("git")
        .arg("-C")
        .arg(repo)
        .arg("remote")
        .output()
        .map_err(|source| BuildError::Io {
            path: repo.display().to_string(),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(BuildError::Process {
            command: format!("git -C {} remote", repo.display()),
            status: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    for remote in String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|remote| !remote.is_empty())
    {
        let remove = crate::process::noninteractive_command("git")
            .arg("-C")
            .arg(repo)
            .args(["remote", "remove", remote])
            .output()
            .map_err(|source| BuildError::Io {
                path: repo.display().to_string(),
                detail: source.to_string(),
            })?;
        if !remove.status.success() {
            return Err(BuildError::Process {
                command: format!("git -C {} remote remove {remote}", repo.display()),
                status: remove.status.code().unwrap_or(-1),
                stderr: String::from_utf8_lossy(&remove.stderr).trim().to_string(),
            });
        }
    }
    Ok(())
}

fn verify_build_lockfile(source_repo: &Path, plan: &BuildPlan) -> Result<()> {
    let lockfile = source_repo.join("Cargo.lock");
    let bytes = fs::read(&lockfile).map_err(|source| BuildError::Io {
        path: lockfile.display().to_string(),
        detail: source.to_string(),
    })?;
    let actual = crate::build_content_hash(&bytes);
    if actual != plan.input.lockfile_hash {
        return Err(BuildError::InvalidValue {
            detail: format!(
                "build lockfile hash mismatch: expected {}, got {actual}",
                plan.input.lockfile_hash
            ),
        });
    }
    Ok(())
}

fn validate_requested_outputs(
    cargo_program: &Path,
    source_repo: &Path,
    plan: &BuildPlan,
) -> Result<()> {
    for output in plan
        .input
        .outputs
        .iter()
        .filter(|output| output.kind == BuildArtifactKind::Binary)
    {
        if output.name != "sinomo"
            || output.package != "sinomo"
            || output.cargo_target != "sinomo"
            || output.native_dylib.is_some()
        {
            return Err(BuildError::InvalidValue {
                detail: format!(
                    "unsupported binary output descriptor {}/{}/{}; expected sinomo/sinomo/sinomo",
                    output.name, output.package, output.cargo_target
                ),
            });
        }
    }
    let requested_module_ids = plan
        .input
        .outputs
        .iter()
        .filter(|output| output.kind == BuildArtifactKind::NativeDylibModule)
        .map(|output| {
            output
                .native_dylib
                .as_ref()
                .map(|metadata| metadata.module_id.clone())
                .ok_or_else(|| BuildError::InvalidValue {
                    detail: format!("native output {} has no loader metadata", output.name),
                })
        })
        .collect::<Result<Vec<_>>>()?;
    if requested_module_ids.is_empty() {
        return Ok(());
    }
    let mut discovered = discover_native_dylib_outputs(
        cargo_program,
        source_repo,
        &plan.input.target,
        NativeModuleSelection::ModuleIds(requested_module_ids),
    )?;
    let mut requested = plan
        .input
        .outputs
        .iter()
        .filter(|output| output.kind == BuildArtifactKind::NativeDylibModule)
        .cloned()
        .collect::<Vec<_>>();
    discovered.sort();
    requested.sort();
    if discovered != requested {
        return Err(BuildError::InvalidValue {
            detail: format!(
                "requested native module descriptors do not match source metadata for target {}",
                plan.input.target.path_key()
            ),
        });
    }
    Ok(())
}

pub(crate) fn cached_build_manifest(
    paths: &BuildPaths,
    key: &BuildKey,
    verify_content: bool,
) -> Result<Option<BuildArtifactSetManifest>> {
    let path = paths.cache_manifest(key);
    let manifest: Option<BuildArtifactSetManifest> = match read_optional_object_file(&path) {
        Ok(manifest) => manifest,
        Err(error) if paths.cache_entry_dir(key).exists() => {
            quarantine_cache_entry(paths, key, &error.to_string())?;
            return Ok(None);
        }
        Err(error) => {
            return Err(BuildError::Io {
                path: path.display().to_string(),
                detail: error.to_string(),
            });
        }
    };
    let Some(manifest) = manifest else {
        return Ok(None);
    };
    let validation = (|| -> Result<()> {
        manifest.validate()?;
        if &manifest.provenance.build_key != key {
            return Err(BuildError::InvalidBundle {
                detail: format!(
                    "cached build manifest key {} does not match path key {key}",
                    manifest.provenance.build_key
                ),
            });
        }
        let artifact_root = paths.cache_artifacts_dir(key);
        for artifact in &manifest.artifacts {
            let artifact_path = artifact_root.join(&artifact.relative_path);
            if verify_content {
                crate::verify_artifact_file(artifact, &artifact_path)?;
            } else {
                let size_bytes = fs::metadata(&artifact_path)
                    .map_err(|source| BuildError::Io {
                        path: artifact_path.display().to_string(),
                        detail: source.to_string(),
                    })?
                    .len();
                if size_bytes != artifact.size_bytes {
                    return Err(BuildError::InvalidBundle {
                        detail: format!(
                            "artifact {} size mismatch: expected {}, got {size_bytes}",
                            artifact.artifact_id, artifact.size_bytes
                        ),
                    });
                }
            }
        }
        Ok(())
    })();
    if let Err(error) = validation {
        quarantine_cache_entry(paths, key, &error.to_string())?;
        return Ok(None);
    }
    Ok(Some(manifest))
}

fn quarantine_cache_entry(paths: &BuildPaths, key: &BuildKey, detail: &str) -> Result<()> {
    let entry = paths.cache_entry_dir(key);
    if !entry.exists() {
        return Ok(());
    }
    let quarantine = entry.with_extension(format!(
        "corrupt-{}",
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    fs::rename(&entry, &quarantine).map_err(|source| BuildError::Io {
        path: format!("{} -> {}", entry.display(), quarantine.display()),
        detail: format!("cached artifact set is invalid ({detail}): {source}"),
    })?;
    tracing::warn!(
        build_key = %key,
        quarantine = %quarantine.display(),
        "quarantined invalid build artifact cache entry: {detail}"
    );
    Ok(())
}

fn publish_artifact_cache(
    paths: &BuildPaths,
    plan: &BuildPlan,
    artifact_root: &Path,
    manifest: BuildArtifactSetManifest,
    job_id: &BuildJobId,
) -> Result<BuildArtifactSetManifest> {
    let bundle = crate::create_artifact_bundle(manifest, artifact_root)?;
    let entry = paths.cache_entry_dir(&plan.key);
    let staging = entry.with_extension(format!("next-{job_id}"));
    if staging.exists() {
        fs::remove_dir_all(&staging).map_err(|source| BuildError::Io {
            path: staging.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let staging_artifacts = staging.join("artifacts");
    fs::create_dir_all(&staging_artifacts).map_err(|source| BuildError::Io {
        path: staging_artifacts.display().to_string(),
        detail: source.to_string(),
    })?;
    for artifact in &bundle.manifest.artifacts {
        copy_build_artifact(
            &artifact_root.join(&artifact.relative_path),
            &staging_artifacts.join(&artifact.relative_path),
        )?;
    }
    fs::write(staging.join("artifacts.tar"), &bundle.bytes).map_err(|source| BuildError::Io {
        path: staging.join("artifacts.tar").display().to_string(),
        detail: source.to_string(),
    })?;
    write_object_file(&staging.join("manifest.sobj"), &bundle.manifest).map_err(|error| {
        BuildError::Io {
            path: staging.join("manifest.sobj").display().to_string(),
            detail: error.to_string(),
        }
    })?;
    match fs::rename(&staging, &entry) {
        Ok(()) => Ok(bundle.manifest),
        Err(_) if entry.is_dir() => {
            let _ = fs::remove_dir_all(&staging);
            let existing: BuildArtifactSetManifest = read_object_file(&entry.join("manifest.sobj"))
                .map_err(|error| BuildError::Io {
                    path: entry.join("manifest.sobj").display().to_string(),
                    detail: error.to_string(),
                })?;
            existing.validate()?;
            if existing.provenance.build_key != plan.key {
                return Err(BuildError::InvalidBundle {
                    detail: "concurrent artifact cache winner has the wrong build key".to_string(),
                });
            }
            Ok(existing)
        }
        Err(source) => Err(BuildError::Io {
            path: entry.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

fn copy_build_artifact(source: &Path, destination: &Path) -> Result<()> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|source| BuildError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    fs::copy(source, destination)
        .map(|_| ())
        .map_err(|source| BuildError::Io {
            path: destination.display().to_string(),
            detail: source.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(repo: &Path, args: &[&str]) {
        let output = crate::process::noninteractive_command("git")
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

    #[test]
    fn selector_matches_module_identity_and_package() {
        let module_id = sinomo_module::ModuleId::from_legacy_named("module.test").expect("id");
        let output = BuildOutputRequest::native_dylib_module(
            module_id.to_string(),
            "test-package",
            "test_module",
            crate::NativeDylibArtifactMetadata::new(
                module_id,
                "v1",
                "module/test",
                "libtest_module.so",
                "profile:test",
            ),
        );

        assert!(build_output_matches_selector(&output, "module.test"));
        assert!(build_output_matches_selector(&output, "test-package"));
        assert!(build_output_matches_selector(&output, "module/test"));
    }

    #[test]
    fn local_build_uses_the_verified_cache_and_shared_incremental_target() {
        const MANIFEST: &str = r#"[package]
name = "sinomo"
version = "0.1.0"
edition = "2024"

[workspace]
"#;
        const LOCKFILE: &str = r#"# This file is automatically @generated by Cargo.
# It is not intended for manual editing.
version = 4

[[package]]
name = "sinomo"
version = "0.1.0"
"#;
        let root = sinomo_testdir::TestDir::new("sinomo-build-manager-local");
        let repo = root.join("repo");
        fs::create_dir_all(repo.join("src")).expect("source");
        fs::write(repo.join("Cargo.toml"), MANIFEST).expect("manifest");
        fs::write(repo.join("Cargo.lock"), LOCKFILE).expect("lockfile");
        fs::write(repo.join("src/main.rs"), "fn main() {}\n").expect("main");
        git(&repo, &["init"]);
        git(&repo, &["config", "user.name", "Sinomo Build Test"]);
        git(&repo, &["config", "user.email", "build@sinomo.invalid"]);
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "-m", "fixture"]);
        let paths = BuildPaths::new(root.join("data"));

        let first = execute_local_build(&paths, &repo, LocalBuildOutputSelection::Binary)
            .expect("first build");
        let second = execute_local_build(&paths, &repo, LocalBuildOutputSelection::Binary)
            .expect("cached build");

        assert!(!first.cache_hit);
        assert!(second.cache_hit);
        assert_eq!(first.plan.key, second.plan.key);
        assert!(first.artifact_root.is_dir());
        assert!(paths.incremental_target_dir(&first.plan).is_dir());
        assert!(
            paths
                .checkout_root(&first.plan.source.label)
                .join("sinomo")
                .is_dir()
        );
        assert!(!repo.join("target").exists());
    }
}
