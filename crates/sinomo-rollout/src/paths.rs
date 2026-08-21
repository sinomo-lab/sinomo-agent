use crate::RolloutError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

pub const SANDBOX_WASIX_MODULE_PACKAGE: &str = "sinomo-sandbox-wasix-module";
pub const SANDBOX_WASIX_MODULE_RUNTIME_FEATURE: &str = "sinomo-sandbox-wasix-module/wasix-runtime";
pub const SANDBOX_WASIX_NATIVE_MODULE_CACHE_KEY: &str = "module/sandbox/wasix";
pub const SANDBOX_WASIX_NATIVE_MODULE_SOURCE: &str = "rollout:modules/sandbox/wasix";
pub const BENCHMARKS_MODULE_PACKAGE: &str = "sinomo-benchmarks";
pub const BENCHMARKS_MODULE_ID: &str = "module.benchmarks";
pub const BENCHMARKS_MODULE_INTERFACE_VERSION: &str = "sinomo.benchmarks.v1";
pub const BENCHMARKS_NATIVE_MODULE_CACHE_KEY: &str = "module/benchmark/sinomo-benchmarks";
pub const BENCHMARKS_NATIVE_MODULE_SOURCE: &str = "rollout:modules/benchmark/sinomo-benchmarks";
pub const DFS_CLOUD_FILES_MODULE_PACKAGE: &str = "sinomo-dfs-cloud-files";
pub const DFS_CLOUD_FILES_NATIVE_MODULE_CACHE_KEY: &str = "module/dfs/cloud-files";
pub const DFS_CLOUD_FILES_NATIVE_MODULE_SOURCE: &str = "rollout:modules/dfs/cloud-files";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SelfRolloutPaths {
    pub root: PathBuf,
    pub source_repo: PathBuf,
    pub bin: PathBuf,
    pub main_binary: PathBuf,
    pub shadow_binary: PathBuf,
    pub state: PathBuf,
    pub build_log: PathBuf,
}

impl SelfRolloutPaths {
    pub fn new(data_dir_root: &Path) -> Self {
        let root = data_dir_root.join("rollout");
        let bin = data_dir_root.join("bin");
        Self {
            source_repo: data_dir_root.join("source").join("sinomo"),
            main_binary: bin.join(executable_name("sinomo")),
            shadow_binary: bin.join(executable_name("sinomo-shadow")),
            state: root.join("state.sobj"),
            build_log: root.join("build.log"),
            root,
            bin,
        }
    }

    pub fn plan(&self) -> SelfRolloutPlan {
        let native_dylib_modules = default_native_dylib_module_builds(&self.source_repo);
        let mut build_args = vec![
            "build".to_string(),
            "--locked".to_string(),
            "-p".to_string(),
            "sinomo".to_string(),
        ];
        append_native_dylib_module_build_args(&mut build_args, &native_dylib_modules);
        SelfRolloutPlan {
            build_program: "cargo".to_string(),
            build_args,
            build_dir: self.source_repo.clone(),
            built_binary: self
                .source_repo
                .join("target")
                .join("debug")
                .join(executable_name("sinomo")),
            install_main_binary: self.main_binary.clone(),
            install_shadow_binary: self.shadow_binary.clone(),
            native_dylib_modules,
            build_log: self.build_log.clone(),
            state: self.state.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SelfRolloutPlan {
    pub build_program: String,
    pub build_args: Vec<String>,
    pub build_dir: PathBuf,
    pub built_binary: PathBuf,
    pub install_main_binary: PathBuf,
    pub install_shadow_binary: PathBuf,
    pub native_dylib_modules: Vec<SelfRolloutNativeDylibModuleBuild>,
    pub build_log: PathBuf,
    pub state: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SelfRolloutNativeDylibModuleBuild {
    pub package: String,
    pub features: Vec<String>,
    pub module_id: String,
    pub interface_version: String,
    pub cache_key: String,
    pub library_name: String,
    pub built_library: PathBuf,
    pub source: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeDylibModulePlatformPolicy {
    pub module_id: String,
    pub source: String,
    pub platforms: Vec<String>,
}

impl NativeDylibModulePlatformPolicy {
    fn applies_to(&self, current_os: &str, is_windows: bool, is_unix: bool) -> bool {
        self.platforms.is_empty()
            || self.platforms.iter().any(|platform| {
                platform == current_os
                    || (platform == "windows" && is_windows)
                    || (platform == "unix" && is_unix)
            })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NativeDylibPlatformReconciliationReport {
    pub removed_module_ids: Vec<String>,
}

pub fn executable_name(stem: &str) -> String {
    if cfg!(windows) {
        format!("{stem}.exe")
    } else {
        stem.to_string()
    }
}

pub fn native_dylib_library_name(stem: &str) -> String {
    if cfg!(windows) {
        format!("{stem}.dll")
    } else if cfg!(target_os = "macos") {
        format!("lib{stem}.dylib")
    } else {
        format!("lib{stem}.so")
    }
}

pub fn sandbox_wasix_native_module_library_name() -> String {
    native_dylib_library_name("sinomo_sandbox_wasix_module")
}

pub fn benchmarks_native_module_library_name() -> String {
    native_dylib_library_name("sinomo_benchmarks")
}

pub fn dfs_cloud_files_native_module_library_name() -> String {
    native_dylib_library_name("sinomo_dfs_cloud_files")
}

pub fn default_native_dylib_module_builds(
    build_dir: &Path,
) -> Vec<SelfRolloutNativeDylibModuleBuild> {
    discover_default_native_dylib_module_builds(build_dir)
        .unwrap_or_else(|_| legacy_default_native_dylib_module_builds(build_dir))
}

pub fn discover_default_native_dylib_module_builds(
    build_dir: &Path,
) -> Result<Vec<SelfRolloutNativeDylibModuleBuild>, RolloutError> {
    let discovered = discover_native_dylib_module_builds(build_dir)?;
    if discovered.is_empty() {
        Ok(legacy_default_native_dylib_module_builds(build_dir))
    } else {
        Ok(discovered)
    }
}

pub fn discover_native_dylib_module_builds(
    build_dir: &Path,
) -> Result<Vec<SelfRolloutNativeDylibModuleBuild>, RolloutError> {
    discover_native_dylib_module_builds_with_options(
        build_dir,
        NativeDylibModuleDiscoveryOptions::default(),
    )
}

pub fn discover_all_native_dylib_module_builds(
    build_dir: &Path,
) -> Result<Vec<SelfRolloutNativeDylibModuleBuild>, RolloutError> {
    discover_native_dylib_module_builds_with_options(
        build_dir,
        NativeDylibModuleDiscoveryOptions {
            include_non_default: true,
        },
    )
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct NativeDylibModuleDiscoveryOptions {
    include_non_default: bool,
}

fn discover_native_dylib_module_builds_with_options(
    build_dir: &Path,
    options: NativeDylibModuleDiscoveryOptions,
) -> Result<Vec<SelfRolloutNativeDylibModuleBuild>, RolloutError> {
    let manifests = discover_module_package_manifests(build_dir)?;
    let mut modules = Vec::new();
    for manifest in manifests {
        if let Some(module) =
            native_dylib_module_build_from_manifest(build_dir, &manifest, options)?
        {
            modules.push(module);
        }
    }
    modules.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then_with(|| left.package.cmp(&right.package))
    });
    Ok(modules)
}

pub fn discover_native_dylib_module_platform_policies(
    build_dir: &Path,
) -> Result<Vec<NativeDylibModulePlatformPolicy>, RolloutError> {
    let manifests = discover_module_package_manifests(build_dir)?;
    let mut policies = Vec::new();
    for manifest_path in manifests {
        let manifest = fs::read_to_string(&manifest_path).map_err(|source| RolloutError::Io {
            path: manifest_path.display().to_string(),
            detail: source.to_string(),
        })?;
        let parsed = toml::from_str::<toml::Value>(&manifest).map_err(|source| {
            RolloutError::InvalidArtifact {
                detail: format!("parse {}: {source}", manifest_path.display()),
            }
        })?;
        let Some(native_dylib) = parsed
            .get("package")
            .and_then(|package| package.get("metadata"))
            .and_then(|metadata| metadata.get("sinomo"))
            .and_then(|sinomo| sinomo.get("native-dylib"))
        else {
            continue;
        };
        let module_id = metadata_string(native_dylib, "module-id", &manifest_path)?;
        let relative_package_dir = manifest_path
            .parent()
            .and_then(|parent| parent.strip_prefix(build_dir).ok())
            .ok_or_else(|| RolloutError::InvalidArtifact {
                detail: format!(
                    "{} is not under rollout build dir {}",
                    manifest_path.display(),
                    build_dir.display()
                ),
            })?;
        let source = metadata_optional_string(native_dylib, "source", &manifest_path)?
            .unwrap_or_else(|| format!("rollout:{}", path_to_slash_string(relative_package_dir)));
        policies.push(NativeDylibModulePlatformPolicy {
            module_id,
            source,
            platforms: metadata_string_array(native_dylib, "platforms", &manifest_path)?,
        });
    }
    policies.sort_by(|left, right| left.module_id.cmp(&right.module_id));
    Ok(policies)
}

/// Returns whether a persisted native module was built for this host and the
/// generic native-module ABI it currently implements.
///
/// This deliberately does not infer desired module policy from the managed
/// source checkout. That checkout can be absent or stale during binary-push
/// rollouts, so it is not safe input for deciding whether an installed module
/// may load.
pub fn native_dylib_loader_state_is_host_compatible(
    state: &sinomo_module::NativeDylibModuleLoaderState,
) -> bool {
    sinomo_module::native_dylib_loader_state_is_host_compatible(state)
}

#[cfg(test)]
fn native_dylib_loader_state_is_host_compatible_for(
    state: &sinomo_module::NativeDylibModuleLoaderState,
    current_arch: &str,
    current_os: &str,
    current_family: &str,
) -> bool {
    sinomo_module::native_dylib_loader_state_is_host_compatible_for(
        state,
        current_arch,
        current_os,
        current_family,
    )
}

fn native_dylib_policy_matches_state(
    policy: &NativeDylibModulePlatformPolicy,
    state: &sinomo_module::NativeDylibModuleLoaderState,
) -> bool {
    policy.source == state.source
        && sinomo_module::ModuleId::new(policy.module_id.clone())
            .or_else(|_| sinomo_module::ModuleId::from_legacy_named(&policy.module_id))
            .is_ok_and(|module_id| module_id == state.module_id)
}

/// Removes rollout-managed loader states that an authoritative source checkout
/// explicitly excludes on this host.
///
/// Callers must only invoke this while operating on the source generation they
/// just built and must pass that exact checkout as `built_source_repo`; the
/// managed checkout is deliberately not inferred. Missing policies,
/// incompatible persisted host metadata, and user-managed states are preserved;
/// ordinary daemon startup is never a destructive desired-state reconciliation
/// point.
pub fn reconcile_rollout_native_dylib_platforms(
    data_dir: &Path,
    built_source_repo: &Path,
) -> Result<NativeDylibPlatformReconciliationReport, RolloutError> {
    let states = sinomo_module::read_native_dylib_module_loader_states(data_dir)?;
    let policies = discover_native_dylib_module_platform_policies(built_source_repo)?;
    let mut removed_module_ids = BTreeSet::new();
    for state in states {
        if !state.source.starts_with("rollout:") {
            continue;
        }
        let Some(policy) = policies
            .iter()
            .find(|policy| native_dylib_policy_matches_state(policy, &state))
        else {
            continue;
        };
        if policy.applies_to(env::consts::OS, cfg!(windows), cfg!(unix)) {
            continue;
        }
        if sinomo_module::remove_native_dylib_module_loader_state(data_dir, &state.module_id)?
            .is_some()
        {
            removed_module_ids.insert(policy.module_id.clone());
        }
    }
    Ok(NativeDylibPlatformReconciliationReport {
        removed_module_ids: removed_module_ids.into_iter().collect(),
    })
}

fn legacy_default_native_dylib_module_builds(
    build_dir: &Path,
) -> Vec<SelfRolloutNativeDylibModuleBuild> {
    let library_name = sandbox_wasix_native_module_library_name();
    let mut modules = vec![SelfRolloutNativeDylibModuleBuild {
        package: SANDBOX_WASIX_MODULE_PACKAGE.to_string(),
        features: vec![SANDBOX_WASIX_MODULE_RUNTIME_FEATURE.to_string()],
        module_id: sinomo_sandbox_wasix_api::SINOMO_SANDBOX_WASIX_MODULE_ID.to_string(),
        interface_version: sinomo_sandbox_wasix_api::SINOMO_SANDBOX_WASIX_INTERFACE_VERSION
            .to_string(),
        cache_key: SANDBOX_WASIX_NATIVE_MODULE_CACHE_KEY.to_string(),
        library_name: library_name.clone(),
        built_library: build_dir.join("target").join("debug").join(library_name),
        source: SANDBOX_WASIX_NATIVE_MODULE_SOURCE.to_string(),
    }];
    if cfg!(windows) {
        modules.push(dfs_cloud_files_native_dylib_module_build(build_dir));
    }
    modules
}

fn discover_module_package_manifests(build_dir: &Path) -> Result<Vec<PathBuf>, RolloutError> {
    let modules_dir = build_dir.join("modules");
    if !modules_dir.exists() {
        return Ok(Vec::new());
    }
    let mut manifests = Vec::new();
    let mut family_dirs = read_sorted_dirs(&modules_dir)?;
    family_dirs.sort();
    for family_dir in family_dirs {
        let mut module_dirs = read_sorted_dirs(&family_dir)?;
        module_dirs.sort();
        for module_dir in module_dirs {
            let manifest = module_dir.join("Cargo.toml");
            if manifest.exists() {
                manifests.push(manifest);
            }
        }
    }
    Ok(manifests)
}

fn read_sorted_dirs(path: &Path) -> Result<Vec<PathBuf>, RolloutError> {
    let entries = fs::read_dir(path).map_err(|source| RolloutError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut dirs = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| RolloutError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        let file_type = entry.file_type().map_err(|source| RolloutError::Io {
            path: entry.path().display().to_string(),
            detail: source.to_string(),
        })?;
        if file_type.is_dir() {
            dirs.push(entry.path());
        }
    }
    dirs.sort();
    Ok(dirs)
}

fn native_dylib_module_build_from_manifest(
    build_dir: &Path,
    manifest_path: &Path,
    options: NativeDylibModuleDiscoveryOptions,
) -> Result<Option<SelfRolloutNativeDylibModuleBuild>, RolloutError> {
    let manifest = fs::read_to_string(manifest_path).map_err(|source| RolloutError::Io {
        path: manifest_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let parsed = toml::from_str::<toml::Value>(&manifest).map_err(|source| {
        RolloutError::InvalidArtifact {
            detail: format!("parse {}: {source}", manifest_path.display()),
        }
    })?;
    let Some(native_dylib) = parsed
        .get("package")
        .and_then(|package| package.get("metadata"))
        .and_then(|metadata| metadata.get("sinomo"))
        .and_then(|sinomo| sinomo.get("native-dylib"))
    else {
        return Ok(None);
    };
    if !options.include_non_default && !metadata_bool(native_dylib, "default", true, manifest_path)?
    {
        return Ok(None);
    }
    if !metadata_platform_matches(native_dylib, manifest_path)? {
        return Ok(None);
    }

    let package = parsed
        .get("package")
        .and_then(|package| package.get("name"))
        .and_then(toml::Value::as_str)
        .ok_or_else(|| RolloutError::InvalidArtifact {
            detail: format!("{} is missing package.name", manifest_path.display()),
        })?;
    let module_id = metadata_string(native_dylib, "module-id", manifest_path)?;
    let interface_version = metadata_string(native_dylib, "interface-version", manifest_path)?;
    let relative_package_dir = manifest_path
        .parent()
        .and_then(|parent| parent.strip_prefix(build_dir).ok())
        .ok_or_else(|| RolloutError::InvalidArtifact {
            detail: format!(
                "{} is not under rollout build dir {}",
                manifest_path.display(),
                build_dir.display()
            ),
        })?;
    let source = metadata_optional_string(native_dylib, "source", manifest_path)?
        .unwrap_or_else(|| format!("rollout:{}", path_to_slash_string(relative_package_dir)));
    let cache_key = metadata_optional_string(native_dylib, "cache-key", manifest_path)?
        .unwrap_or_else(|| path_to_slash_string(relative_package_dir));
    let library_name = match metadata_optional_string(native_dylib, "library-name", manifest_path)?
    {
        Some(library_name) => library_name,
        None => {
            let stem = metadata_optional_string(native_dylib, "library-stem", manifest_path)?
                .unwrap_or_else(|| package.replace('-', "_"));
            native_dylib_library_name(&stem)
        }
    };
    let features = metadata_string_array(native_dylib, "features", manifest_path)?;

    Ok(Some(SelfRolloutNativeDylibModuleBuild {
        package: package.to_string(),
        features,
        module_id,
        interface_version,
        cache_key,
        library_name: library_name.clone(),
        built_library: build_dir.join("target").join("debug").join(library_name),
        source,
    }))
}

fn metadata_bool(
    table: &toml::Value,
    key: &str,
    default: bool,
    manifest_path: &Path,
) -> Result<bool, RolloutError> {
    match table.get(key) {
        None => Ok(default),
        Some(value) => value
            .as_bool()
            .ok_or_else(|| RolloutError::InvalidArtifact {
                detail: format!(
                    "{} metadata `{key}` must be a boolean",
                    manifest_path.display()
                ),
            }),
    }
}

fn metadata_string(
    table: &toml::Value,
    key: &str,
    manifest_path: &Path,
) -> Result<String, RolloutError> {
    metadata_optional_string(table, key, manifest_path)?.ok_or_else(|| {
        RolloutError::InvalidArtifact {
            detail: format!(
                "{} metadata is missing required `{key}`",
                manifest_path.display()
            ),
        }
    })
}

fn metadata_optional_string(
    table: &toml::Value,
    key: &str,
    manifest_path: &Path,
) -> Result<Option<String>, RolloutError> {
    match table.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(|value| Some(value.trim().to_string()))
            .ok_or_else(|| RolloutError::InvalidArtifact {
                detail: format!(
                    "{} metadata `{key}` must be a non-empty string",
                    manifest_path.display()
                ),
            }),
    }
}

fn metadata_string_array(
    table: &toml::Value,
    key: &str,
    manifest_path: &Path,
) -> Result<Vec<String>, RolloutError> {
    let Some(value) = table.get(key) else {
        return Ok(Vec::new());
    };
    let array = value
        .as_array()
        .ok_or_else(|| RolloutError::InvalidArtifact {
            detail: format!(
                "{} metadata `{key}` must be an array",
                manifest_path.display()
            ),
        })?;
    let mut values = Vec::new();
    for item in array {
        let value = item
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| RolloutError::InvalidArtifact {
                detail: format!(
                    "{} metadata `{key}` entries must be non-empty strings",
                    manifest_path.display()
                ),
            })?;
        values.push(value.trim().to_string());
    }
    Ok(values)
}

fn metadata_platform_matches(
    table: &toml::Value,
    manifest_path: &Path,
) -> Result<bool, RolloutError> {
    let platforms = metadata_string_array(table, "platforms", manifest_path)?;
    if platforms.is_empty() {
        return Ok(true);
    }
    let current_os = env::consts::OS;
    Ok(platforms.iter().any(|platform| {
        platform == current_os
            || (platform == "windows" && cfg!(windows))
            || (platform == "unix" && cfg!(unix))
    }))
}

fn path_to_slash_string(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

pub fn benchmark_native_dylib_module_build(build_dir: &Path) -> SelfRolloutNativeDylibModuleBuild {
    let library_name = benchmarks_native_module_library_name();
    SelfRolloutNativeDylibModuleBuild {
        package: BENCHMARKS_MODULE_PACKAGE.to_string(),
        features: Vec::new(),
        module_id: BENCHMARKS_MODULE_ID.to_string(),
        interface_version: BENCHMARKS_MODULE_INTERFACE_VERSION.to_string(),
        cache_key: BENCHMARKS_NATIVE_MODULE_CACHE_KEY.to_string(),
        library_name: library_name.clone(),
        built_library: build_dir.join("target").join("debug").join(library_name),
        source: BENCHMARKS_NATIVE_MODULE_SOURCE.to_string(),
    }
}

pub fn dfs_cloud_files_native_dylib_module_build(
    build_dir: &Path,
) -> SelfRolloutNativeDylibModuleBuild {
    let library_name = dfs_cloud_files_native_module_library_name();
    SelfRolloutNativeDylibModuleBuild {
        package: DFS_CLOUD_FILES_MODULE_PACKAGE.to_string(),
        features: Vec::new(),
        module_id: sinomo_dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_MODULE_ID.to_string(),
        interface_version: sinomo_dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
            .to_string(),
        cache_key: DFS_CLOUD_FILES_NATIVE_MODULE_CACHE_KEY.to_string(),
        library_name: library_name.clone(),
        built_library: build_dir.join("target").join("debug").join(library_name),
        source: DFS_CLOUD_FILES_NATIVE_MODULE_SOURCE.to_string(),
    }
}

pub fn append_native_dylib_module_build_args(
    args: &mut Vec<String>,
    modules: &[SelfRolloutNativeDylibModuleBuild],
) {
    for module in modules {
        args.push("-p".to_string());
        args.push(module.package.clone());
    }
    let features = modules
        .iter()
        .flat_map(|module| module.features.iter().cloned())
        .collect::<Vec<_>>();
    if !features.is_empty() {
        args.push("--features".to_string());
        args.push(features.join(","));
    }
}

pub fn ensure_rollout_layout(paths: &SelfRolloutPaths) -> Result<(), RolloutError> {
    for path in [&paths.root, &paths.bin] {
        fs::create_dir_all(path).map_err(|source| RolloutError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    if let Some(parent) = paths.source_repo.parent() {
        fs::create_dir_all(parent).map_err(|source| RolloutError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_rollout_paths_live_under_data_directory() {
        let root = PathBuf::from("/tmp/sinomo-data");
        let paths = SelfRolloutPaths::new(&root);

        assert_eq!(paths.root, root.join("rollout"));
        assert_eq!(paths.source_repo, root.join("source").join("sinomo"));
        assert_eq!(paths.bin, root.join("bin"));
        assert_eq!(
            paths.main_binary,
            root.join("bin").join(executable_name("sinomo"))
        );
        assert_eq!(
            paths.shadow_binary,
            root.join("bin").join(executable_name("sinomo-shadow"))
        );
        assert_eq!(paths.state, root.join("rollout").join("state.sobj"));
        assert_eq!(paths.build_log, root.join("rollout").join("build.log"));
    }

    #[test]
    fn self_rollout_plan_builds_sinomo_binary_from_source_repo() {
        let paths = SelfRolloutPaths::new(Path::new("/tmp/sinomo-data"));
        let plan = paths.plan();
        let expected_modules = default_native_dylib_module_builds(&paths.source_repo);
        let mut expected_build_args = vec![
            "build".to_string(),
            "--locked".to_string(),
            "-p".to_string(),
            "sinomo".to_string(),
        ];
        append_native_dylib_module_build_args(&mut expected_build_args, &expected_modules);

        assert_eq!(plan.build_program, "cargo");
        assert_eq!(plan.build_args, expected_build_args);
        assert_eq!(plan.build_dir, paths.source_repo);
        assert_eq!(
            plan.built_binary,
            plan.build_dir
                .join("target")
                .join("debug")
                .join(executable_name("sinomo"))
        );
        assert_eq!(plan.install_main_binary, paths.main_binary);
        assert_eq!(plan.install_shadow_binary, paths.shadow_binary);
        assert_eq!(
            plan.native_dylib_modules,
            default_native_dylib_module_builds(&plan.build_dir)
        );
    }

    #[test]
    fn native_dylib_module_builds_are_discovered_from_module_metadata() {
        let root = unique_temp("native-dylib-discovery");
        let module_dir = root.join("modules/remote/management");
        fs::create_dir_all(&module_dir).expect("module dir");
        fs::write(
            module_dir.join("Cargo.toml"),
            r#"
[package]
name = "sinomo-remote-management-module"
version = "0.1.0"

[package.metadata.sinomo.native-dylib]
module-id = "module.remote-management"
interface-version = "sinomo.remote-management.v2"
cache-key = "module/remote/management"
source = "rollout:modules/remote/management"
"#,
        )
        .expect("manifest");

        let modules = discover_default_native_dylib_module_builds(&root).expect("discover modules");

        assert_eq!(modules.len(), 1);
        assert_eq!(modules[0].package, "sinomo-remote-management-module");
        assert_eq!(modules[0].module_id, "module.remote-management");
        assert_eq!(modules[0].interface_version, "sinomo.remote-management.v2");
        assert_eq!(modules[0].cache_key, "module/remote/management");
        assert_eq!(modules[0].source, "rollout:modules/remote/management");
        assert_eq!(
            modules[0].built_library,
            root.join("target")
                .join("debug")
                .join(native_dylib_library_name("sinomo_remote_management_module"))
        );
    }

    #[test]
    fn native_dylib_module_discovery_filters_platforms_and_opt_in_modules() {
        let root = unique_temp("native-dylib-discovery-filter");
        let windows_dir = root.join("modules/dfs/cloud-files");
        let benchmark_dir = root.join("modules/benchmark/sinomo-benchmarks");
        fs::create_dir_all(&windows_dir).expect("windows module dir");
        fs::create_dir_all(&benchmark_dir).expect("benchmark module dir");
        fs::write(
            windows_dir.join("Cargo.toml"),
            r#"
[package]
name = "sinomo-dfs-cloud-files"
version = "0.1.0"

[package.metadata.sinomo.native-dylib]
module-id = "module.dfs.cloud-files"
interface-version = "sinomo.dfs.cloud-files.v1"
platforms = ["windows"]
"#,
        )
        .expect("windows manifest");
        fs::write(
            benchmark_dir.join("Cargo.toml"),
            r#"
[package]
name = "sinomo-benchmarks"
version = "0.1.0"

[package.metadata.sinomo.native-dylib]
module-id = "module.benchmarks"
interface-version = "sinomo.benchmarks.v1"
default = false
"#,
        )
        .expect("benchmark manifest");

        let modules = discover_default_native_dylib_module_builds(&root).expect("discover modules");
        let all_modules =
            discover_all_native_dylib_module_builds(&root).expect("discover all modules");

        assert_eq!(
            modules
                .iter()
                .any(|module| module.package == "sinomo-dfs-cloud-files"),
            cfg!(windows)
        );
        assert!(
            modules
                .iter()
                .all(|module| module.package != "sinomo-benchmarks")
        );
        assert!(
            all_modules
                .iter()
                .any(|module| module.package == "sinomo-benchmarks")
        );
    }

    #[test]
    fn loader_host_compatibility_uses_exact_platform_and_generic_abi() {
        let state = test_native_dylib_loader_state(
            "module.network-iroh",
            "x86_64-windows-windows",
            "rollout:modules/network/iroh",
        );
        assert!(native_dylib_loader_state_is_host_compatible_for(
            &state, "x86_64", "windows", "windows"
        ));

        let mut wrong_platform = state.clone();
        wrong_platform.platform_triple = "x86_64-linux-unix".to_string();
        assert!(!native_dylib_loader_state_is_host_compatible_for(
            &wrong_platform,
            "x86_64",
            "windows",
            "windows"
        ));

        let mut wrong_abi = state;
        wrong_abi.native_abi_version =
            sinomo_module::NATIVE_DYLIB_HOST_ABI_VERSION.saturating_add(1);
        assert!(!native_dylib_loader_state_is_host_compatible_for(
            &wrong_abi, "x86_64", "windows", "windows"
        ));
    }

    #[test]
    fn platform_reconciliation_removes_rollout_state_omitted_on_this_host() {
        let root = unique_temp("native-dylib-platform-reconcile");
        let module_dir = root
            .join("source/sinomo")
            .join("modules/test/opposite-platform");
        fs::create_dir_all(&module_dir).expect("module dir");
        let excluded_platform = if cfg!(windows) { "unix" } else { "windows" };
        fs::write(
            module_dir.join("Cargo.toml"),
            format!(
                r#"
[package]
name = "sinomo-opposite-platform-module"
version = "0.1.0"

[package.metadata.sinomo.native-dylib]
module-id = "module.opposite-platform"
interface-version = "sinomo.opposite-platform.v1"
source = "rollout:modules/test/opposite-platform"
platforms = ["{excluded_platform}"]
"#
            ),
        )
        .expect("manifest");
        let state = test_native_dylib_loader_state(
            "module.opposite-platform",
            &format!(
                "{}-{}-{}",
                env::consts::ARCH,
                env::consts::OS,
                env::consts::FAMILY
            ),
            "rollout:modules/test/opposite-platform",
        );
        sinomo_module::write_native_dylib_module_loader_state(&root, &state)
            .expect("write loader state");

        let report = reconcile_rollout_native_dylib_platforms(
            &root,
            &SelfRolloutPaths::new(&root).source_repo,
        )
        .expect("reconcile");

        assert_eq!(
            report.removed_module_ids,
            vec!["module.opposite-platform".to_string()]
        );
        assert!(
            sinomo_module::read_native_dylib_module_loader_state(&root, &state.module_id)
                .expect("read loader state")
                .is_none()
        );
    }

    #[test]
    fn platform_reconciliation_preserves_manual_same_id_override() {
        let root = unique_temp("native-dylib-platform-manual-override");
        write_opposite_platform_policy(&root, "module.network-iroh");
        let state = test_native_dylib_loader_state(
            "module.network-iroh",
            &format!(
                "{}-{}-{}",
                env::consts::ARCH,
                env::consts::OS,
                env::consts::FAMILY
            ),
            "manual:operator-installed-iroh",
        );
        sinomo_module::write_native_dylib_module_loader_state(&root, &state)
            .expect("write loader state");

        let report = reconcile_rollout_native_dylib_platforms(
            &root,
            &SelfRolloutPaths::new(&root).source_repo,
        )
        .expect("reconcile");

        assert!(report.removed_module_ids.is_empty());
        assert!(
            sinomo_module::read_native_dylib_module_loader_state(&root, &state.module_id)
                .expect("read loader state")
                .is_some()
        );
    }

    #[test]
    fn platform_reconciliation_preserves_state_without_matching_policy() {
        let root = unique_temp("native-dylib-platform-missing-policy");
        let state = test_native_dylib_loader_state(
            "module.rollout-without-source",
            &format!(
                "{}-{}-{}",
                env::consts::ARCH,
                env::consts::OS,
                env::consts::FAMILY
            ),
            "rollout:modules/removed-or-unavailable",
        );
        sinomo_module::write_native_dylib_module_loader_state(&root, &state)
            .expect("write loader state");

        let report = reconcile_rollout_native_dylib_platforms(
            &root,
            &SelfRolloutPaths::new(&root).source_repo,
        )
        .expect("reconcile");

        assert!(report.removed_module_ids.is_empty());
        assert!(
            sinomo_module::read_native_dylib_module_loader_state(&root, &state.module_id)
                .expect("read loader state")
                .is_some()
        );
    }

    #[test]
    fn platform_reconciliation_uses_exact_built_repo_not_stale_managed_checkout() {
        let root = unique_temp("native-dylib-platform-exact-source");
        let managed_repo = SelfRolloutPaths::new(&root).source_repo;
        let exact_build_repo = root.join("exact-build-source");
        let excluded_platform = if cfg!(windows) { "unix" } else { "windows" };
        write_platform_policy(&managed_repo, "module.opposite-platform", excluded_platform);
        write_platform_policy(
            &exact_build_repo,
            "module.opposite-platform",
            env::consts::OS,
        );
        let state = test_native_dylib_loader_state(
            "module.opposite-platform",
            &format!(
                "{}-{}-{}",
                env::consts::ARCH,
                env::consts::OS,
                env::consts::FAMILY
            ),
            "rollout:modules/test/opposite-platform",
        );
        sinomo_module::write_native_dylib_module_loader_state(&root, &state)
            .expect("write loader state");

        let report = reconcile_rollout_native_dylib_platforms(&root, &exact_build_repo)
            .expect("reconcile exact build source");

        assert!(report.removed_module_ids.is_empty());
        assert!(
            sinomo_module::read_native_dylib_module_loader_state(&root, &state.module_id)
                .expect("read loader state")
                .is_some()
        );
    }

    #[test]
    fn benchmark_native_dylib_module_build_is_opt_in() {
        let build_dir = Path::new("/tmp/sinomo-source");
        let default_modules = default_native_dylib_module_builds(build_dir);
        let benchmark_module = benchmark_native_dylib_module_build(build_dir);

        assert!(
            default_modules
                .iter()
                .all(|module| module.package != BENCHMARKS_MODULE_PACKAGE)
        );
        assert_eq!(benchmark_module.package, BENCHMARKS_MODULE_PACKAGE);
        assert_eq!(benchmark_module.module_id, BENCHMARKS_MODULE_ID);
        assert_eq!(
            benchmark_module.interface_version,
            BENCHMARKS_MODULE_INTERFACE_VERSION
        );
        assert_eq!(
            benchmark_module.cache_key,
            BENCHMARKS_NATIVE_MODULE_CACHE_KEY
        );
        assert_eq!(benchmark_module.source, BENCHMARKS_NATIVE_MODULE_SOURCE);
        assert_eq!(
            benchmark_module.built_library,
            build_dir
                .join("target")
                .join("debug")
                .join(benchmarks_native_module_library_name())
        );
    }

    #[test]
    fn dfs_cloud_files_native_dylib_module_is_default_only_on_windows() {
        let build_dir = Path::new("/tmp/sinomo-source");
        let default_modules = default_native_dylib_module_builds(build_dir);
        let cloud_files_module = dfs_cloud_files_native_dylib_module_build(build_dir);

        assert_eq!(cloud_files_module.package, DFS_CLOUD_FILES_MODULE_PACKAGE);
        assert_eq!(
            cloud_files_module.module_id,
            sinomo_dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_MODULE_ID
        );
        assert_eq!(
            cloud_files_module.interface_version,
            sinomo_dfs_cloud_files_api::SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
        );
        assert_eq!(
            cloud_files_module.cache_key,
            DFS_CLOUD_FILES_NATIVE_MODULE_CACHE_KEY
        );
        assert_eq!(
            cloud_files_module.source,
            DFS_CLOUD_FILES_NATIVE_MODULE_SOURCE
        );
        assert_eq!(
            cloud_files_module.built_library,
            build_dir
                .join("target")
                .join("debug")
                .join(dfs_cloud_files_native_module_library_name())
        );
        assert_eq!(
            default_modules
                .iter()
                .any(|module| module.package == DFS_CLOUD_FILES_MODULE_PACKAGE),
            cfg!(windows)
        );
    }

    fn unique_temp(label: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(&format!("sinomo-rollout-{label}"))
    }

    fn write_opposite_platform_policy(root: &Path, module_id: &str) {
        let source_repo = root.join("source/sinomo");
        let excluded_platform = if cfg!(windows) { "unix" } else { "windows" };
        write_platform_policy(&source_repo, module_id, excluded_platform);
    }

    fn write_platform_policy(source_repo: &Path, module_id: &str, platform: &str) {
        let module_dir = source_repo.join("modules/test/opposite-platform");
        fs::create_dir_all(&module_dir).expect("module dir");
        fs::write(
            module_dir.join("Cargo.toml"),
            format!(
                r#"
[package]
name = "sinomo-opposite-platform-module"
version = "0.1.0"

[package.metadata.sinomo.native-dylib]
module-id = "{module_id}"
interface-version = "sinomo.opposite-platform.v1"
source = "rollout:modules/test/opposite-platform"
platforms = ["{platform}"]
"#
            ),
        )
        .expect("manifest");
    }

    fn test_native_dylib_loader_state(
        module_id: &str,
        platform_triple: &str,
        source: &str,
    ) -> sinomo_module::NativeDylibModuleLoaderState {
        sinomo_module::NativeDylibModuleLoaderState {
            module_id: sinomo_module::ModuleId::from_legacy_named(module_id).expect("module id"),
            artifact_id: None,
            content_hash: "blake3:test".to_string(),
            replaces_content_hash: None,
            cache_key: module_id.replace('.', "/"),
            library_name: native_dylib_library_name(&module_id.replace('.', "_")),
            platform_triple: platform_triple.to_string(),
            native_abi_version: sinomo_module::NATIVE_DYLIB_HOST_ABI_VERSION,
            interface_version: "test.v1".to_string(),
            activated_at: "2026-07-12T00:00:00Z".to_string(),
            source: source.to_string(),
        }
    }
}
