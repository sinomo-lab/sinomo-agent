use crate::RolloutError;
use serde::{Deserialize, Serialize};
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
        let mut build_args = vec!["build".to_string(), "-p".to_string(), "sinomo".to_string()];
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
    let manifests = discover_module_package_manifests(build_dir)?;
    let mut modules = Vec::new();
    for manifest in manifests {
        if let Some(module) = native_dylib_module_build_from_manifest(build_dir, &manifest)? {
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
    if !metadata_bool(native_dylib, "default", true, manifest_path)? {
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
        let mut expected_build_args =
            vec!["build".to_string(), "-p".to_string(), "sinomo".to_string()];
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
interface-version = "sinomo.remote-management.v1"
cache-key = "module/remote/management"
source = "rollout:modules/remote/management"
"#,
        )
        .expect("manifest");

        let modules = discover_default_native_dylib_module_builds(&root).expect("discover modules");

        assert_eq!(modules.len(), 1);
        assert_eq!(modules[0].package, "sinomo-remote-management-module");
        assert_eq!(modules[0].module_id, "module.remote-management");
        assert_eq!(modules[0].interface_version, "sinomo.remote-management.v1");
        assert_eq!(modules[0].cache_key, "module/remote/management");
        assert_eq!(modules[0].source, "rollout:modules/remote/management");
        assert_eq!(
            modules[0].built_library,
            root.join("target")
                .join("debug")
                .join(native_dylib_library_name("sinomo_remote_management_module"))
        );

        let _ = fs::remove_dir_all(root);
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

        let _ = fs::remove_dir_all(root);
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

    fn unique_temp(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "sinomo-rollout-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&path);
        path
    }
}
