use crate::RolloutError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

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
        SelfRolloutPlan {
            build_program: "cargo".to_string(),
            build_args: vec!["build".to_string(), "-p".to_string(), "sinomo".to_string()],
            build_dir: self.source_repo.clone(),
            built_binary: self
                .source_repo
                .join("target")
                .join("debug")
                .join(executable_name("sinomo")),
            install_main_binary: self.main_binary.clone(),
            install_shadow_binary: self.shadow_binary.clone(),
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
    pub build_log: PathBuf,
    pub state: PathBuf,
}

pub fn executable_name(stem: &str) -> String {
    if cfg!(windows) {
        format!("{stem}.exe")
    } else {
        stem.to_string()
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

        assert_eq!(plan.build_program, "cargo");
        assert_eq!(
            plan.build_args,
            vec!["build".to_string(), "-p".to_string(), "sinomo".to_string()]
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
    }
}
