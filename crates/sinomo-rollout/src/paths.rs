use crate::RolloutError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

pub const SANDBOX_WASIX_MODULE_PACKAGE: &str = "sinomo-sandbox-wasix-module";
pub const SANDBOX_WASIX_NATIVE_MODULE_CACHE_KEY: &str = "module/sandbox/wasix";
pub const SANDBOX_WASIX_NATIVE_MODULE_SOURCE: &str = "rollout:modules/sandbox/wasix";

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
        for module in &native_dylib_modules {
            build_args.push("-p".to_string());
            build_args.push(module.package.clone());
        }
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

pub fn default_native_dylib_module_builds(
    build_dir: &Path,
) -> Vec<SelfRolloutNativeDylibModuleBuild> {
    let library_name = sandbox_wasix_native_module_library_name();
    vec![SelfRolloutNativeDylibModuleBuild {
        package: SANDBOX_WASIX_MODULE_PACKAGE.to_string(),
        module_id: sinomo_sandbox_wasix_api::SINOMO_SANDBOX_WASIX_MODULE_ID.to_string(),
        interface_version: sinomo_sandbox_wasix_api::SINOMO_SANDBOX_WASIX_INTERFACE_VERSION
            .to_string(),
        cache_key: SANDBOX_WASIX_NATIVE_MODULE_CACHE_KEY.to_string(),
        library_name: library_name.clone(),
        built_library: build_dir.join("target").join("debug").join(library_name),
        source: SANDBOX_WASIX_NATIVE_MODULE_SOURCE.to_string(),
    }]
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

        assert_eq!(plan.build_program, "cargo");
        assert_eq!(
            plan.build_args,
            vec![
                "build".to_string(),
                "-p".to_string(),
                "sinomo".to_string(),
                "-p".to_string(),
                SANDBOX_WASIX_MODULE_PACKAGE.to_string()
            ]
        );
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
}
