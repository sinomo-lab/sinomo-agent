use crate::{
    BuildArtifactKind, BuildArtifactManifest, BuildArtifactSetManifest, BuildError,
    BuildOutputRequest, BuildPlan, BuildProvenance, BuildTarget, BuildTargetCapability,
    NativeDylibArtifactMetadata, Result, process::noninteractive_command,
};
use serde_json::Value;
use sinomo_module::ModuleId;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

pub const CARGO_BUILD_EXECUTOR_PROTOCOL: &str = "sinomo.cargo-build.v1";
pub const CARGO_BUILD_COMMIT_OVERRIDE_ENV: &str = "SINOMO_ROLLOUT_BUILD_COMMIT_OVERRIDE";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeModuleSelection {
    Default,
    All,
    ModuleIds(Vec<ModuleId>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RustToolchainInfo {
    pub host_target: String,
    pub fingerprint: String,
    pub verbose_version: String,
    pub cargo_version: String,
    pub executor_protocol: String,
}

/// Reads the authoritative host triple and fingerprints the complete compiler
/// version. This avoids relying on Cargo's build-script-only `TARGET` variable.
pub fn inspect_rust_toolchain(rustc_program: impl AsRef<Path>) -> Result<RustToolchainInfo> {
    inspect_cargo_toolchain(rustc_program, Path::new("cargo"))
}

/// Produces the complete capability fingerprint used in build keys. Both the
/// compiler and Cargo implementation, plus this executor's protocol version,
/// participate in cache identity.
pub fn inspect_cargo_toolchain(
    rustc_program: impl AsRef<Path>,
    cargo_program: impl AsRef<Path>,
) -> Result<RustToolchainInfo> {
    let rustc_program = rustc_program.as_ref();
    let cargo_program = cargo_program.as_ref();
    let verbose_version = command_stdout(rustc_program, &[OsString::from("-vV")])?;
    let cargo_version = command_stdout(cargo_program, &[OsString::from("-Vv")])?;
    let host_target = verbose_version
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| BuildError::InvalidValue {
            detail: "rustc -vV did not report a host target".to_string(),
        })?
        .to_string();
    let mut hasher = blake3::Hasher::new();
    hasher.update(CARGO_BUILD_EXECUTOR_PROTOCOL.as_bytes());
    hasher.update(env!("CARGO_PKG_VERSION").as_bytes());
    hasher.update(verbose_version.as_bytes());
    hasher.update(cargo_version.as_bytes());
    let fingerprint = format!("cargo-toolchain-blake3:{}", hasher.finalize().to_hex());
    Ok(RustToolchainInfo {
        host_target,
        fingerprint,
        verbose_version,
        cargo_version,
        executor_protocol: format!(
            "{}@{}",
            CARGO_BUILD_EXECUTOR_PROTOCOL,
            env!("CARGO_PKG_VERSION")
        ),
    })
}

/// Probes target OS/architecture/family through rustc itself and rejects target
/// triples whose standard-library target directory is not installed.
pub fn inspect_rust_target(
    rustc_program: impl AsRef<Path>,
    rust_target: impl AsRef<str>,
) -> Result<BuildTarget> {
    let rustc_program = rustc_program.as_ref();
    let rust_target = rust_target.as_ref().trim();
    if rust_target.is_empty() {
        return Err(BuildError::InvalidValue {
            detail: "Rust target triple must not be empty".to_string(),
        });
    }
    let cfg_args = [
        OsString::from("--print"),
        OsString::from("cfg"),
        OsString::from("--target"),
        OsString::from(rust_target),
    ];
    let cfg = command_stdout(rustc_program, &cfg_args)?;
    let arch = rustc_cfg_value(&cfg, "target_arch")?;
    let os = rustc_cfg_value(&cfg, "target_os")?;
    let family = rustc_cfg_value(&cfg, "target_family").ok();
    let libdir_args = [
        OsString::from("--print"),
        OsString::from("target-libdir"),
        OsString::from("--target"),
        OsString::from(rust_target),
    ];
    let target_libdir = PathBuf::from(command_stdout(rustc_program, &libdir_args)?.trim());
    if !target_libdir.is_dir() {
        return Err(BuildError::InvalidValue {
            detail: format!(
                "Rust target {rust_target} is not installed (target libdir {} is missing)",
                target_libdir.display()
            ),
        });
    }
    let mut target = BuildTarget::new(os, arch)?.with_rust_target(rust_target)?;
    if let Some(family) = family {
        target = target.with_family(family)?;
    }
    Ok(target)
}

pub fn parse_rust_target_list(value: &str) -> Result<Vec<String>> {
    let targets = value
        .split(|character: char| character == ',' || character == ';' || character.is_whitespace())
        .filter(|target| !target.is_empty())
        .map(str::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return Err(BuildError::InvalidValue {
            detail: "configured Rust target list must not be empty".to_string(),
        });
    }
    Ok(targets)
}

/// Uses Cargo's own target-filtered metadata instead of host `cfg!` values, so
/// a Linux planner can enumerate Windows-only native modules and vice versa.
pub fn discover_native_dylib_outputs(
    cargo_program: impl AsRef<Path>,
    source_repo: impl AsRef<Path>,
    target: &BuildTarget,
    selection: NativeModuleSelection,
) -> Result<Vec<BuildOutputRequest>> {
    target.validate()?;
    let rust_target = target
        .rust_target
        .as_deref()
        .ok_or_else(|| BuildError::InvalidValue {
            detail: "native module discovery requires a concrete Rust target triple".to_string(),
        })?;
    let cargo_program = cargo_program.as_ref();
    let source_repo = source_repo.as_ref();
    let args = vec![
        OsString::from("metadata"),
        OsString::from("--format-version"),
        OsString::from("1"),
        OsString::from("--no-deps"),
        // Discovery is part of build planning and output validation. It must
        // never rewrite Cargo.lock after that lockfile has participated in the
        // build key.
        OsString::from("--locked"),
        OsString::from("--filter-platform"),
        OsString::from(rust_target),
    ];
    let output = noninteractive_command(cargo_program)
        .args(&args)
        .current_dir(source_repo)
        .output()
        .map_err(|source| BuildError::Io {
            path: cargo_program.display().to_string(),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(command_error(cargo_program, &args, output));
    }
    let metadata: Value =
        serde_json::from_slice(&output.stdout).map_err(|source| BuildError::InvalidValue {
            detail: format!("Cargo metadata is invalid JSON: {source}"),
        })?;
    native_dylib_outputs_from_metadata(&metadata, source_repo, target, selection)
}

fn native_dylib_outputs_from_metadata(
    metadata: &Value,
    source_repo: &Path,
    target: &BuildTarget,
    selection: NativeModuleSelection,
) -> Result<Vec<BuildOutputRequest>> {
    let packages = metadata
        .get("packages")
        .and_then(Value::as_array)
        .ok_or_else(|| BuildError::InvalidValue {
            detail: "Cargo metadata has no packages array".to_string(),
        })?;
    let requested_ids = match &selection {
        NativeModuleSelection::ModuleIds(ids) => Some(ids.iter().cloned().collect::<BTreeSet<_>>()),
        NativeModuleSelection::Default | NativeModuleSelection::All => None,
    };
    let mut discovered_ids = BTreeSet::new();
    let mut outputs = Vec::new();
    for package in packages {
        let Some(native) = package
            .get("metadata")
            .and_then(|value| value.get("sinomo"))
            .and_then(|value| value.get("native-dylib"))
        else {
            continue;
        };
        if !native_platform_matches(native, target)? {
            continue;
        }
        if matches!(selection, NativeModuleSelection::Default)
            && !metadata_optional_bool(native, "default")?.unwrap_or(true)
        {
            continue;
        }
        let module_id_text = metadata_required_string(native, "module-id")?;
        let module_id = ModuleId::from_legacy_named(&module_id_text).map_err(|source| {
            BuildError::InvalidValue {
                detail: format!("native module id `{module_id_text}` is invalid: {source}"),
            }
        })?;
        if requested_ids
            .as_ref()
            .is_some_and(|ids| !ids.contains(&module_id))
        {
            continue;
        }
        let package_name = package
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| BuildError::InvalidValue {
                detail: "native module package has no name".to_string(),
            })?
            .to_string();
        let cargo_target = package
            .get("targets")
            .and_then(Value::as_array)
            .and_then(|targets| {
                targets.iter().find(|candidate| {
                    candidate
                        .get("crate_types")
                        .and_then(Value::as_array)
                        .is_some_and(|types| types.iter().any(|value| value == "cdylib"))
                })
            })
            .and_then(|value| value.get("name"))
            .and_then(Value::as_str)
            .ok_or_else(|| BuildError::InvalidValue {
                detail: format!("native module package {package_name} has no cdylib target"),
            })?
            .to_string();
        let interface_version = metadata_required_string(native, "interface-version")?;
        let cache_key = metadata_optional_string(native, "cache-key")?.unwrap_or_else(|| {
            package
                .get("manifest_path")
                .and_then(Value::as_str)
                .and_then(|path| Path::new(path).parent())
                .and_then(|path| path.strip_prefix(source_repo).ok())
                .map(path_to_slash_string)
                .unwrap_or_else(|| package_name.clone())
        });
        let source = metadata_optional_string(native, "source")?.unwrap_or_else(|| {
            format!(
                "build:{}",
                package
                    .get("manifest_path")
                    .and_then(Value::as_str)
                    .and_then(|path| Path::new(path).parent())
                    .and_then(|path| path.strip_prefix(source_repo).ok())
                    .map(path_to_slash_string)
                    .unwrap_or_else(|| package_name.clone())
            )
        });
        let library_name = match metadata_optional_string(native, "library-name")? {
            Some(value) => value,
            None => {
                let stem = metadata_optional_string(native, "library-stem")?
                    .unwrap_or_else(|| cargo_target.replace('-', "_"));
                native_dylib_library_name_for_target(target, &stem)
            }
        };
        let features = metadata_string_array(native, "features")?;
        let output = BuildOutputRequest::native_dylib_module(
            module_id.to_string(),
            package_name,
            cargo_target,
            NativeDylibArtifactMetadata::new(
                module_id.clone(),
                interface_version,
                cache_key,
                library_name,
                source,
            ),
        )
        .with_features(features);
        discovered_ids.insert(module_id);
        outputs.push(output);
    }
    if let Some(requested_ids) = requested_ids {
        let missing = requested_ids
            .difference(&discovered_ids)
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(BuildError::InvalidValue {
                detail: format!(
                    "requested native modules are not available for {}: {}",
                    target.path_key(),
                    missing.join(", ")
                ),
            });
        }
    }
    outputs.sort_by(|left, right| {
        left.native_dylib
            .as_ref()
            .map(|metadata| metadata.source.as_str())
            .cmp(
                &right
                    .native_dylib
                    .as_ref()
                    .map(|metadata| metadata.source.as_str()),
            )
            .then_with(|| left.package.cmp(&right.package))
    });
    Ok(outputs)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CargoBuildCommand {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub env: Vec<(OsString, OsString)>,
    pub current_dir: PathBuf,
    pub target_dir: PathBuf,
}

impl CargoBuildCommand {
    pub fn display(&self) -> String {
        self.env
            .iter()
            .map(|(key, value)| format!("{}={}", key.to_string_lossy(), value.to_string_lossy()))
            .chain(std::iter::once(
                self.program.as_os_str().to_string_lossy().into_owned(),
            ))
            .chain(
                self.args
                    .iter()
                    .map(|value| value.to_string_lossy().into_owned()),
            )
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CargoBuildResult {
    pub command: CargoBuildCommand,
    pub stdout: String,
    pub stderr: String,
    pub artifact_set: BuildArtifactSetManifest,
    pub artifact_root: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CargoBuildExecutor {
    cargo_program: PathBuf,
    supported_targets: Vec<BuildTargetCapability>,
    target_environments: BTreeMap<(BuildTarget, String), Vec<(OsString, OsString)>>,
}

impl CargoBuildExecutor {
    pub fn new(
        cargo_program: impl Into<PathBuf>,
        supported_targets: Vec<BuildTargetCapability>,
    ) -> Result<Self> {
        if supported_targets.is_empty() {
            return Err(BuildError::InvalidValue {
                detail: "Cargo build executor requires at least one supported target".to_string(),
            });
        }
        for capability in &supported_targets {
            capability.target.validate()?;
            if capability.target.rust_target.is_none() {
                return Err(BuildError::InvalidValue {
                    detail: "Cargo build executor targets must include concrete Rust triples"
                        .to_string(),
                });
            }
            if capability.artifact_kinds.is_empty() {
                return Err(BuildError::InvalidValue {
                    detail: format!(
                        "Cargo build executor target {} supports no artifact kinds",
                        capability.target.path_key()
                    ),
                });
            }
            if capability.toolchain_fingerprint.trim().is_empty() {
                return Err(BuildError::InvalidValue {
                    detail: format!(
                        "Cargo build executor target {} has no toolchain fingerprint",
                        capability.target.path_key()
                    ),
                });
            }
        }
        Ok(Self {
            cargo_program: cargo_program.into(),
            supported_targets,
            target_environments: BTreeMap::new(),
        })
    }

    pub fn with_target_environment(
        mut self,
        target: BuildTarget,
        toolchain_fingerprint: impl Into<String>,
        environment: Vec<(OsString, OsString)>,
    ) -> Result<Self> {
        let toolchain_fingerprint = toolchain_fingerprint.into();
        let supported = self.supported_targets.iter().any(|capability| {
            capability.target == target && capability.toolchain_fingerprint == toolchain_fingerprint
        });
        if !supported {
            return Err(BuildError::InvalidValue {
                detail: format!(
                    "Cargo target environment does not match advertised target {} with toolchain {}",
                    target.path_key(),
                    toolchain_fingerprint
                ),
            });
        }
        self.target_environments
            .insert((target, toolchain_fingerprint), environment);
        Ok(self)
    }

    pub fn supported_targets(&self) -> &[BuildTargetCapability] {
        &self.supported_targets
    }

    pub fn plan_command(
        &self,
        source_repo: impl AsRef<Path>,
        plan: &BuildPlan,
        target_dir: impl Into<PathBuf>,
    ) -> Result<CargoBuildCommand> {
        plan.validate()?;
        let kinds = plan.artifact_kinds();
        let supported = self.supported_targets.iter().any(|capability| {
            capability.target == plan.input.target
                && capability.toolchain_fingerprint == plan.input.toolchain_fingerprint
                && capability.supports(&plan.input.target, &kinds)
        });
        if !supported {
            return Err(BuildError::InvalidValue {
                detail: format!(
                    "Cargo executor does not advertise target {} with toolchain {}",
                    plan.input.target.path_key(),
                    plan.input.toolchain_fingerprint
                ),
            });
        }
        let source_repo = source_repo.as_ref().to_path_buf();
        if !source_repo.join("Cargo.toml").is_file() {
            return Err(BuildError::InvalidValue {
                detail: format!("{} is not a Cargo workspace", source_repo.display()),
            });
        }
        let target_dir = target_dir.into();
        let mut args = vec![
            OsString::from("build"),
            OsString::from("--locked"),
            OsString::from("--target-dir"),
            target_dir.as_os_str().to_os_string(),
            OsString::from("--target"),
            OsString::from(
                plan.input
                    .target
                    .rust_target
                    .as_deref()
                    .expect("validated concrete target"),
            ),
            OsString::from("--profile"),
            OsString::from(&plan.input.profile),
        ];
        let packages = plan
            .input
            .outputs
            .iter()
            .map(|output| output.package.as_str())
            .collect::<BTreeSet<_>>();
        for package in packages {
            args.push(OsString::from("-p"));
            args.push(OsString::from(package));
        }
        let features = plan
            .input
            .features
            .iter()
            .chain(
                plan.input
                    .outputs
                    .iter()
                    .flat_map(|output| output.features.iter()),
            )
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if !features.is_empty() {
            args.push(OsString::from("--features"));
            args.push(OsString::from(
                features.into_iter().collect::<Vec<_>>().join(","),
            ));
        }
        let mut env = self
            .target_environments
            .get(&(
                plan.input.target.clone(),
                plan.input.toolchain_fingerprint.clone(),
            ))
            .cloned()
            .unwrap_or_default();
        env.extend(plan.input.source_revision.iter().map(|revision| {
            (
                OsString::from(CARGO_BUILD_COMMIT_OVERRIDE_ENV),
                OsString::from(revision),
            )
        }));
        Ok(CargoBuildCommand {
            program: self.cargo_program.clone(),
            args,
            env,
            current_dir: source_repo,
            target_dir,
        })
    }

    pub fn execute(
        &self,
        source_repo: impl AsRef<Path>,
        plan: &BuildPlan,
        artifact_root: impl AsRef<Path>,
    ) -> Result<CargoBuildResult> {
        let artifact_root = artifact_root.as_ref();
        self.execute_with_target_dir(
            source_repo,
            plan,
            artifact_root,
            artifact_root.join(".cargo-target"),
            None,
        )
    }

    pub fn execute_with_target_dir(
        &self,
        source_repo: impl AsRef<Path>,
        plan: &BuildPlan,
        artifact_root: impl AsRef<Path>,
        target_dir: impl Into<PathBuf>,
        cargo_jobs: Option<u16>,
    ) -> Result<CargoBuildResult> {
        let artifact_root = artifact_root.as_ref();
        fs::create_dir_all(artifact_root).map_err(|source| BuildError::Io {
            path: artifact_root.display().to_string(),
            detail: source.to_string(),
        })?;
        let target_dir = target_dir.into();
        fs::create_dir_all(&target_dir).map_err(|source| BuildError::Io {
            path: target_dir.display().to_string(),
            detail: source.to_string(),
        })?;
        let mut command = self.plan_command(source_repo, plan, &target_dir)?;
        if let Some(cargo_jobs) = cargo_jobs {
            command.args.push(OsString::from("--jobs"));
            command.args.push(OsString::from(cargo_jobs.to_string()));
        }
        let mut process = noninteractive_command(&command.program);
        process
            .args(&command.args)
            .current_dir(&command.current_dir);
        for (key, value) in &command.env {
            process.env(key, value);
        }
        let output = process.output().map_err(|source| BuildError::Io {
            path: command.program.display().to_string(),
            detail: source.to_string(),
        })?;
        if !output.status.success() {
            return Err(command_error(&command.program, &command.args, output));
        }
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        let provenance = BuildProvenance::from_plan(plan)?;
        let mut artifacts = Vec::with_capacity(plan.input.outputs.len());
        for request in &plan.input.outputs {
            let built_path = cargo_output_path(
                &target_dir,
                &plan.input.target,
                &plan.input.profile,
                request,
            )?;
            if !built_path.is_file() {
                return Err(BuildError::InvalidValue {
                    detail: format!(
                        "Cargo did not produce exact requested output {} at {}",
                        request.name,
                        built_path.display()
                    ),
                });
            }
            let artifact_id = build_artifact_id(plan, request);
            let file_name = built_path
                .file_name()
                .ok_or_else(|| BuildError::InvalidValue {
                    detail: format!(
                        "Cargo output path {} has no file name",
                        built_path.display()
                    ),
                })?;
            let relative_path = PathBuf::from(&artifact_id).join(file_name);
            let installed_path = artifact_root.join(&relative_path);
            if let Some(parent) = installed_path.parent() {
                fs::create_dir_all(parent).map_err(|source| BuildError::Io {
                    path: parent.display().to_string(),
                    detail: source.to_string(),
                })?;
            }
            fs::copy(&built_path, &installed_path).map_err(|source| BuildError::Io {
                path: installed_path.display().to_string(),
                detail: source.to_string(),
            })?;
            let relative_path = path_to_slash_string(&relative_path);
            let artifact = match request.kind {
                BuildArtifactKind::Binary => BuildArtifactManifest::binary_from_file(
                    artifact_id,
                    request.name.clone(),
                    relative_path,
                    provenance.clone(),
                    artifact_root,
                )?,
                BuildArtifactKind::NativeDylibModule => {
                    BuildArtifactManifest::native_dylib_from_file(
                        artifact_id,
                        request.name.clone(),
                        relative_path,
                        provenance.clone(),
                        request
                            .native_dylib
                            .clone()
                            .ok_or_else(|| BuildError::InvalidValue {
                                detail: format!(
                                    "native output {} is missing loader metadata",
                                    request.name
                                ),
                            })?,
                        artifact_root,
                    )?
                }
            };
            artifacts.push(artifact);
        }
        let artifact_set = BuildArtifactSetManifest::new(provenance, artifacts)?;
        Ok(CargoBuildResult {
            command,
            stdout,
            stderr,
            artifact_set,
            artifact_root: artifact_root.to_path_buf(),
        })
    }
}

pub fn cargo_output_path(
    target_dir: impl AsRef<Path>,
    target: &BuildTarget,
    profile: &str,
    request: &BuildOutputRequest,
) -> Result<PathBuf> {
    let triple = target
        .rust_target
        .as_deref()
        .ok_or_else(|| BuildError::InvalidValue {
            detail: "Cargo output path requires a concrete Rust target triple".to_string(),
        })?;
    let profile_dir = if profile == "dev" { "debug" } else { profile };
    let file_name = match request.kind {
        BuildArtifactKind::Binary => executable_name_for_target(target, &request.cargo_target),
        BuildArtifactKind::NativeDylibModule => request
            .native_dylib
            .as_ref()
            .ok_or_else(|| BuildError::InvalidValue {
                detail: format!("native output {} is missing loader metadata", request.name),
            })?
            .library_name
            .clone(),
    };
    Ok(target_dir
        .as_ref()
        .join(triple)
        .join(profile_dir)
        .join(file_name))
}

pub fn executable_name_for_target(target: &BuildTarget, stem: &str) -> String {
    if target.is_windows() {
        format!("{stem}.exe")
    } else {
        stem.to_string()
    }
}

pub fn native_dylib_library_name_for_target(target: &BuildTarget, stem: &str) -> String {
    if target.is_windows() {
        format!("{stem}.dll")
    } else if target.os == "macos" || target.os == "ios" {
        format!("lib{stem}.dylib")
    } else {
        format!("lib{stem}.so")
    }
}

fn build_artifact_id(plan: &BuildPlan, request: &BuildOutputRequest) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(plan.key.as_str().as_bytes());
    hasher.update(request.name.as_bytes());
    hasher.update(request.package.as_bytes());
    hasher.update(request.cargo_target.as_bytes());
    let digest = hasher.finalize().to_hex().to_string();
    format!("artifact-{}", &digest[..24])
}

fn native_platform_matches(metadata: &Value, target: &BuildTarget) -> Result<bool> {
    let platforms = metadata_string_array(metadata, "platforms")?;
    Ok(platforms.is_empty()
        || platforms.iter().any(|platform| {
            platform == &target.os
                || (platform == "windows" && target.is_windows())
                || (platform == "unix" && target.is_unix())
        }))
}

fn metadata_required_string(metadata: &Value, key: &str) -> Result<String> {
    metadata_optional_string(metadata, key)?.ok_or_else(|| BuildError::InvalidValue {
        detail: format!("native-dylib metadata is missing `{key}`"),
    })
}

fn metadata_optional_string(metadata: &Value, key: &str) -> Result<Option<String>> {
    match metadata.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| Some(value.to_string()))
            .ok_or_else(|| BuildError::InvalidValue {
                detail: format!("native-dylib metadata `{key}` must be a non-empty string"),
            }),
    }
}

fn metadata_string_array(metadata: &Value, key: &str) -> Result<Vec<String>> {
    let Some(value) = metadata.get(key) else {
        return Ok(Vec::new());
    };
    let values = value.as_array().ok_or_else(|| BuildError::InvalidValue {
        detail: format!("native-dylib metadata `{key}` must be an array"),
    })?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| BuildError::InvalidValue {
                    detail: format!(
                        "native-dylib metadata `{key}` entries must be non-empty strings"
                    ),
                })
        })
        .collect()
}

fn metadata_optional_bool(metadata: &Value, key: &str) -> Result<Option<bool>> {
    match metadata.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_bool()
            .map(Some)
            .ok_or_else(|| BuildError::InvalidValue {
                detail: format!("native-dylib metadata `{key}` must be a boolean"),
            }),
    }
}

fn path_to_slash_string(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn command_stdout(program: &Path, args: &[OsString]) -> Result<String> {
    let output = noninteractive_command(program)
        .args(args)
        .output()
        .map_err(|source| BuildError::Io {
            path: program.display().to_string(),
            detail: source.to_string(),
        })?;
    if !output.status.success() {
        return Err(command_error(program, args, output));
    }
    String::from_utf8(output.stdout).map_err(|source| BuildError::InvalidValue {
        detail: format!("{} output is not UTF-8: {source}", program.display()),
    })
}

fn rustc_cfg_value(cfg: &str, key: &str) -> Result<String> {
    let prefix = format!("{key}=\"");
    cfg.lines()
        .find_map(|line| line.strip_prefix(&prefix)?.strip_suffix('"'))
        .map(str::to_string)
        .ok_or_else(|| BuildError::InvalidValue {
            detail: format!("rustc target cfg did not report {key}"),
        })
}

fn command_error(program: &Path, args: &[OsString], output: std::process::Output) -> BuildError {
    let command = std::iter::once(program.as_os_str())
        .chain(args.iter().map(OsString::as_os_str))
        .map(|value| value.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ");
    BuildError::Process {
        command,
        status: output.status.code().unwrap_or(-1),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BuildKeyInput, BuildSource, build_content_hash};
    use serde_json::json;
    use sinomo_testdir::TestDir;

    fn windows_target() -> BuildTarget {
        BuildTarget::new("windows", "x86_64")
            .expect("target")
            .with_family("windows")
            .expect("family")
            .with_rust_target("x86_64-pc-windows-msvc")
            .expect("triple")
    }

    #[test]
    fn target_filtered_discovery_finds_foreign_default_module() {
        let metadata = json!({
            "packages": [{
                "name": "sinomo-dfs-cloud-files",
                "manifest_path": "/source/modules/dfs/cloud-files/Cargo.toml",
                "metadata": {"sinomo": {"native-dylib": {
                    "module-id": "module.dfs.cloud-files",
                    "interface-version": "sinomo.dfs.cloud-files.v1",
                    "cache-key": "module/dfs/cloud-files",
                    "source": "rollout:modules/dfs/cloud-files",
                    "platforms": ["windows"]
                }}},
                "targets": [{"name": "sinomo_dfs_cloud_files", "crate_types": ["rlib", "cdylib"]}]
            }]
        });

        let outputs = native_dylib_outputs_from_metadata(
            &metadata,
            Path::new("/source"),
            &windows_target(),
            NativeModuleSelection::Default,
        )
        .expect("discover");

        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].package, "sinomo-dfs-cloud-files");
        assert_eq!(
            outputs[0]
                .native_dylib
                .as_ref()
                .map(|metadata| metadata.library_name.as_str()),
            Some("sinomo_dfs_cloud_files.dll")
        );
    }

    #[test]
    fn module_selector_uses_same_target_aware_catalog() {
        let module_id = ModuleId::from_legacy_named("module.dfs.cloud-files").expect("module");
        let metadata = json!({
            "packages": [{
                "name": "sinomo-dfs-cloud-files",
                "manifest_path": "/source/modules/dfs/cloud-files/Cargo.toml",
                "metadata": {"sinomo": {"native-dylib": {
                    "module-id": "module.dfs.cloud-files",
                    "interface-version": "v1",
                    "platforms": ["windows"]
                }}},
                "targets": [{"name": "cloud_files", "crate_types": ["cdylib"]}]
            }]
        });
        let outputs = native_dylib_outputs_from_metadata(
            &metadata,
            Path::new("/source"),
            &windows_target(),
            NativeModuleSelection::ModuleIds(vec![module_id]),
        )
        .expect("discover selected");
        assert_eq!(outputs.len(), 1);
    }

    #[test]
    fn cargo_command_and_output_path_use_concrete_target() {
        let target = windows_target();
        let capability = BuildTargetCapability {
            target: target.clone(),
            artifact_kinds: vec![BuildArtifactKind::Binary],
            toolchain_fingerprint: "rustc-windows".to_string(),
            supports_source_identity_hash: true,
        };
        let source_hash = build_content_hash(b"source");
        let plan = BuildPlan::new(
            BuildSource {
                label: "source".to_string(),
                revision: None,
                identity_hash: None,
                bundle_hash: source_hash.clone(),
                bundle_bytes: 6,
            },
            BuildKeyInput {
                source_hash,
                source_revision: None,
                lockfile_hash: build_content_hash(b"lock"),
                target: target.clone(),
                profile: "release".to_string(),
                features: Vec::new(),
                toolchain_fingerprint: "rustc-windows".to_string(),
                outputs: vec![BuildOutputRequest::binary("sinomo", "sinomo")],
            },
        )
        .expect("plan");
        let executor = CargoBuildExecutor::new("cargo", vec![capability])
            .expect("executor")
            .with_target_environment(
                target.clone(),
                "rustc-windows",
                vec![(
                    OsString::from("CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER"),
                    OsString::from("cross-link.exe"),
                )],
            )
            .expect("target environment");
        let source = std::env::current_dir().expect("cwd");
        let command = executor
            .plan_command(&source, &plan, "/tmp/build-target")
            .expect("command");

        assert!(
            command
                .args
                .iter()
                .any(|arg| arg == "x86_64-pc-windows-msvc")
        );
        assert!(command.env.contains(&(
            OsString::from("CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER"),
            OsString::from("cross-link.exe")
        )));
        assert_eq!(
            cargo_output_path(
                "/tmp/build-target",
                &target,
                "release",
                &plan.input.outputs[0]
            )
            .expect("path"),
            Path::new("/tmp/build-target/x86_64-pc-windows-msvc/release/sinomo.exe")
        );
    }

    #[test]
    fn configured_target_list_is_trimmed_deduplicated_and_stable() {
        assert_eq!(
            parse_rust_target_list(
                "x86_64-unknown-linux-gnu, aarch64-unknown-linux-gnu;\n\
                 x86_64-unknown-linux-gnu"
            )
            .expect("targets"),
            [
                "aarch64-unknown-linux-gnu".to_string(),
                "x86_64-unknown-linux-gnu".to_string(),
            ]
        );
    }

    #[test]
    fn cargo_executor_builds_and_verifies_exact_host_binary() {
        const PACKAGE: &str = "sinomo-build-executor-fixture";
        const MANIFEST: &str = r#"[package]
name = "sinomo-build-executor-fixture"
version = "0.1.0"
edition = "2024"
build = "build.rs"

[workspace]
"#;
        const LOCKFILE: &str = r#"# This file is automatically @generated by Cargo.
# It is not intended for manual editing.
version = 4

[[package]]
name = "sinomo-build-executor-fixture"
version = "0.1.0"
"#;
        const BUILD_RS: &str = r#"fn main() {
    let revision = std::env::var("SINOMO_ROLLOUT_BUILD_COMMIT_OVERRIDE")
        .expect("keyed build revision");
    println!("cargo:rustc-env=FIXTURE_BUILD_REVISION={revision}");
}
"#;
        const MAIN: &str = "fn main() { println!(\"{}\", env!(\"FIXTURE_BUILD_REVISION\")); }\n";

        let root = TestDir::new("sinomo-build-executor-host");
        let source_repo = root.join("source");
        fs::create_dir_all(source_repo.join("src")).expect("source directory");
        fs::write(source_repo.join("Cargo.toml"), MANIFEST).expect("manifest");
        fs::write(source_repo.join("Cargo.lock"), LOCKFILE).expect("lockfile");
        fs::write(source_repo.join("build.rs"), BUILD_RS).expect("build script");
        fs::write(source_repo.join("src/main.rs"), MAIN).expect("main source");

        let toolchain = inspect_cargo_toolchain("rustc", "cargo").expect("host toolchain");
        let target = inspect_rust_target("rustc", &toolchain.host_target).expect("host target");
        let capability = BuildTargetCapability {
            target: target.clone(),
            artifact_kinds: vec![BuildArtifactKind::Binary],
            toolchain_fingerprint: toolchain.fingerprint.clone(),
            supports_source_identity_hash: true,
        };
        let source_identity = [
            MANIFEST.as_bytes(),
            LOCKFILE.as_bytes(),
            BUILD_RS.as_bytes(),
            MAIN.as_bytes(),
        ]
        .concat();
        let source_hash = build_content_hash(&source_identity);
        let plan = BuildPlan::new(
            BuildSource {
                label: "tiny host executor fixture".to_string(),
                revision: Some("fixture-v1".to_string()),
                identity_hash: None,
                bundle_hash: source_hash.clone(),
                bundle_bytes: source_identity.len() as u64,
            },
            BuildKeyInput {
                source_hash,
                source_revision: Some("fixture-v1".to_string()),
                lockfile_hash: build_content_hash(LOCKFILE.as_bytes()),
                target,
                profile: "dev".to_string(),
                features: Vec::new(),
                toolchain_fingerprint: toolchain.fingerprint,
                outputs: vec![BuildOutputRequest::binary(PACKAGE, PACKAGE)],
            },
        )
        .expect("exact host build plan");
        let executor = CargoBuildExecutor::new("cargo", vec![capability]).expect("executor");
        let artifact_root = root.join("artifact-root");

        let result = executor
            .execute(&source_repo, &plan, &artifact_root)
            .expect("execute Cargo build");

        result.artifact_set.validate().expect("artifact set");
        assert_eq!(result.artifact_set.provenance.build_key, plan.key);
        assert_eq!(result.artifact_set.artifacts.len(), 1);
        let artifact = &result.artifact_set.artifacts[0];
        assert_eq!(artifact.kind, BuildArtifactKind::Binary);
        assert_eq!(artifact.name, PACKAGE);
        assert!(artifact.native_dylib.is_none());
        let artifact_path = result.artifact_root.join(&artifact.relative_path);
        assert!(artifact_path.is_file());
        crate::verify_artifact_file(artifact, &artifact_path).expect("verified artifact file");
        let output = noninteractive_command(&artifact_path)
            .output()
            .expect("run built fixture");
        assert!(output.status.success());
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "fixture-v1");
    }
}
