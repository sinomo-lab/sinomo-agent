use crate::RolloutError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::env;
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
        let build_paths = sinomo_build::BuildPaths::new(data_dir_root);
        Self {
            source_repo: build_paths.managed_source,
            main_binary: bin.join(executable_name("sinomo")),
            shadow_binary: bin.join(executable_name("sinomo-shadow")),
            state: root.join("state.sobj"),
            build_log: build_paths.local_log,
            root,
            bin,
        }
    }

    pub fn plan(&self) -> SelfRolloutPlan {
        SelfRolloutPlan {
            source_repo: self.source_repo.clone(),
            install_main_binary: self.main_binary.clone(),
            install_shadow_binary: self.shadow_binary.clone(),
            build_log: self.build_log.clone(),
            state: self.state.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SelfRolloutPlan {
    pub source_repo: PathBuf,
    pub install_main_binary: PathBuf,
    pub install_shadow_binary: PathBuf,
    pub build_log: PathBuf,
    pub state: PathBuf,
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

pub fn discover_native_dylib_module_platform_policies(
    source_repo: &Path,
) -> Result<Vec<NativeDylibModulePlatformPolicy>, RolloutError> {
    let Some(profile) = crate::read_module_distribution_profile(source_repo).map_err(|error| {
        RolloutError::InvalidArtifact {
            detail: error.to_string(),
        }
    })?
    else {
        return Ok(Vec::new());
    };
    let mut policies = profile
        .modules
        .into_iter()
        .map(|entry| NativeDylibModulePlatformPolicy {
            module_id: entry.module_id,
            source: entry.source,
            platforms: entry.platforms,
        })
        .collect::<Vec<_>>();
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

pub fn ensure_rollout_layout(paths: &SelfRolloutPaths) -> Result<(), RolloutError> {
    for path in [&paths.root, &paths.bin] {
        fs::create_dir_all(path).map_err(|source| RolloutError::Io {
            path: path.display().to_string(),
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
        assert_eq!(paths.build_log, root.join("build").join("local.log"));
    }

    #[test]
    fn self_rollout_plan_contains_only_activation_handoff_paths() {
        let paths = SelfRolloutPaths::new(Path::new("/tmp/sinomo-data"));
        let plan = paths.plan();

        assert_eq!(plan.source_repo, paths.source_repo);
        assert_eq!(plan.install_main_binary, paths.main_binary);
        assert_eq!(plan.install_shadow_binary, paths.shadow_binary);
        assert_eq!(plan.build_log, paths.build_log);
        assert_eq!(plan.state, paths.state);
    }

    #[test]
    fn loader_host_compatibility_uses_exact_platform_and_generic_abi() {
        let state = test_native_dylib_loader_state(
            "module.external-network",
            "x86_64-windows-windows",
            "profile:external-network",
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
        let excluded_platform = if cfg!(windows) { "unix" } else { "windows" };
        write_platform_policy(
            &SelfRolloutPaths::new(&root).source_repo,
            "module.opposite-platform",
            excluded_platform,
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
        write_opposite_platform_policy(&root, "module.external-network");
        let state = test_native_dylib_loader_state(
            "module.external-network",
            &format!(
                "{}-{}-{}",
                env::consts::ARCH,
                env::consts::OS,
                env::consts::FAMILY
            ),
            "manual:operator-installed-external-network",
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

    fn unique_temp(label: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(&format!("sinomo-rollout-{label}"))
    }

    fn write_opposite_platform_policy(root: &Path, module_id: &str) {
        let source_repo = root.join("source/sinomo");
        let excluded_platform = if cfg!(windows) { "unix" } else { "windows" };
        write_platform_policy(&source_repo, module_id, excluded_platform);
    }

    fn write_platform_policy(source_repo: &Path, module_id: &str, platform: &str) {
        write_profile(
            source_repo,
            vec![profile_entry(
                module_id,
                "sinomo.opposite-platform.v1",
                "sinomo-opposite-platform-module",
                "module/test/opposite-platform",
                "sinomo_opposite_platform_module",
                "rollout:modules/test/opposite-platform",
                vec![platform.to_string()],
                true,
            )],
        );
    }

    fn profile_entry(
        module_id: &str,
        interface_version: &str,
        package: &str,
        cache_key: &str,
        library_stem: &str,
        source: &str,
        platforms: Vec<String>,
        default_enabled: bool,
    ) -> crate::ModuleDistributionEntry {
        crate::ModuleDistributionEntry {
            module_id: module_id.to_string(),
            interface_version: interface_version.to_string(),
            package: package.to_string(),
            features: Vec::new(),
            cache_key: cache_key.to_string(),
            library_name: library_stem.to_string(),
            source: source.to_string(),
            platforms,
            default_enabled,
        }
    }

    fn write_profile(root: &Path, entries: Vec<crate::ModuleDistributionEntry>) {
        let signer_root = root.join(".module-profile-signer");
        let identity = sinomo_core::node::load_or_create_main_node_identity(&signer_root)
            .expect("profile signer");
        let mut profile = crate::ModuleDistributionProfile::new("test", "1");
        profile.modules = entries;
        crate::sign_module_distribution_profile(&mut profile, &identity).expect("sign profile");
        crate::write_module_distribution_profile(root, &profile).expect("write profile");
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
