use crate::{BuildError, BuildJobId, BuildKey, Result, SINOMO_BUILD_MODULE_OWNER};
use sinomo_core::storage::{
    ObjectSchema, STORAGE_CONTRACT_OBJECT_VERSION, StorageContractError, StorageLayout, StorageRing,
};
use std::fs;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildPaths {
    pub root: PathBuf,
    pub jobs: PathBuf,
    pub work: PathBuf,
    pub cache: PathBuf,
    pub sources: PathBuf,
    pub checkouts: PathBuf,
    pub incremental: PathBuf,
    pub toolchains: PathBuf,
    pub managed_source: PathBuf,
    pub local_log: PathBuf,
}

impl BuildPaths {
    /// Constructs the build-service layout beneath a Sinomo data-directory root.
    pub fn new(data_dir_root: impl Into<PathBuf>) -> Self {
        let data_dir_root = data_dir_root.into();
        Self::from_roots(
            data_dir_root.join("build"),
            data_dir_root.join("source").join("sinomo"),
        )
    }

    pub fn from_build_root(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let data_dir_root = root.parent().unwrap_or(&root);
        Self::from_roots(root.clone(), data_dir_root.join("source").join("sinomo"))
    }

    fn from_roots(root: PathBuf, managed_source: PathBuf) -> Self {
        Self {
            jobs: root.join("jobs"),
            work: root.join("work"),
            cache: root.join("cache"),
            sources: root.join("sources"),
            checkouts: root.join("checkouts"),
            incremental: root.join("incremental"),
            toolchains: root.join("toolchains"),
            local_log: root.join("local.log"),
            managed_source,
            root,
        }
    }

    pub fn ensure_layout(&self) -> Result<()> {
        for path in [
            &self.jobs,
            &self.work,
            &self.cache,
            &self.sources,
            &self.checkouts,
            &self.incremental,
            &self.toolchains,
        ] {
            fs::create_dir_all(path).map_err(|source| BuildError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        Ok(())
    }

    pub fn job_dir(&self, job_id: &BuildJobId) -> PathBuf {
        self.jobs.join(job_id.as_str())
    }

    pub fn job_status(&self, job_id: &BuildJobId) -> PathBuf {
        self.job_dir(job_id).join("status.sobj")
    }

    pub fn job_plan(&self, job_id: &BuildJobId) -> PathBuf {
        self.job_dir(job_id).join("plan.sobj")
    }

    pub fn job_log(&self, job_id: &BuildJobId) -> PathBuf {
        self.job_dir(job_id).join("build.log")
    }

    pub fn job_source_bundle(&self, job_id: &BuildJobId) -> PathBuf {
        self.job_dir(job_id).join("source.bundle")
    }

    pub fn job_work_dir(&self, job_id: &BuildJobId) -> PathBuf {
        self.work.join(job_id.as_str())
    }

    pub fn cache_entry_dir(&self, build_key: &BuildKey) -> PathBuf {
        self.cache.join(build_key.path_key())
    }

    pub fn cache_manifest(&self, build_key: &BuildKey) -> PathBuf {
        self.cache_entry_dir(build_key).join("manifest.sobj")
    }

    pub fn cache_bundle(&self, build_key: &BuildKey) -> PathBuf {
        self.cache_entry_dir(build_key).join("artifacts.tar")
    }

    pub fn cache_artifacts_dir(&self, build_key: &BuildKey) -> PathBuf {
        self.cache_entry_dir(build_key).join("artifacts")
    }

    pub fn source_bundle(&self, source_hash: &str) -> Result<PathBuf> {
        let digest =
            source_hash
                .strip_prefix("blake3:")
                .ok_or_else(|| BuildError::InvalidValue {
                    detail: "source bundle key must use the blake3:<hex> format".to_string(),
                })?;
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(BuildError::InvalidValue {
                detail: "source bundle key must contain a 64-character BLAKE3 digest".to_string(),
            });
        }
        Ok(self
            .sources
            .join(format!("{}.bundle", digest.to_ascii_lowercase())))
    }

    pub fn incremental_target_dir(&self, plan: &crate::BuildPlan) -> PathBuf {
        let toolchain = blake3::hash(plan.input.toolchain_fingerprint.as_bytes())
            .to_hex()
            .to_string();
        self.incremental
            .join(plan.input.target.path_key())
            .join(&toolchain[..16])
            .join(&plan.input.profile)
    }

    pub fn aarch64_gnu_cross_bin(&self) -> PathBuf {
        self.toolchains.join("aarch64-linux-gnu/usr/bin")
    }

    pub fn checkout_root(&self, source_label: &str) -> PathBuf {
        let source = blake3::hash(source_label.as_bytes()).to_hex().to_string();
        self.checkouts.join(&source[..16])
    }
}

pub fn register_storage_paths(
    layout: &mut StorageLayout,
) -> std::result::Result<(), StorageContractError> {
    layout.register_escape_path(
        SINOMO_BUILD_MODULE_OWNER,
        StorageRing::R1,
        "build/jobs",
        "node-local build queue records, source bundles, and build logs keyed by job id",
    )?;
    layout.register_escape_path(
        SINOMO_BUILD_MODULE_OWNER,
        StorageRing::R1,
        "build/work",
        "ephemeral compiler workspaces keyed by build job id",
    )?;
    layout.register_escape_path(
        SINOMO_BUILD_MODULE_OWNER,
        StorageRing::R1,
        "build/cache",
        "content-addressed verified artifact sets keyed by build key",
    )?;
    layout.register_escape_path(
        SINOMO_BUILD_MODULE_OWNER,
        StorageRing::R1,
        "build/sources",
        "content-addressed immutable source bundles prepared for local and cluster builds",
    )?;
    layout.register_escape_path(
        SINOMO_BUILD_MODULE_OWNER,
        StorageRing::R1,
        "build/checkouts",
        "stable locked compiler checkouts refreshed from verified source bundles",
    )?;
    layout.register_escape_path(
        SINOMO_BUILD_MODULE_OWNER,
        StorageRing::R1,
        "build/incremental",
        "reusable target and dependency outputs partitioned by target, toolchain, and profile",
    )?;
    layout.register_escape_path(
        SINOMO_BUILD_MODULE_OWNER,
        StorageRing::R1,
        "build/toolchains",
        "node-local compiler toolchains and target sysroots used by managed builds",
    )?;
    layout.register_escape_path(
        SINOMO_BUILD_MODULE_OWNER,
        StorageRing::R1,
        "source",
        "build-managed reviewed source checkouts",
    )?;
    layout.register_human_text(
        SINOMO_BUILD_MODULE_OWNER,
        StorageRing::R1,
        "build/local.log",
    )?;
    layout.register_sinomo_object(
        SINOMO_BUILD_MODULE_OWNER,
        StorageRing::R1,
        "build/jobs/example/status.sobj",
        ObjectSchema::new("BuildJobStatus", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("job_id", "BuildJobId")
            .field("key", "BuildKey")
            .field("state", "BuildJobState")
            .field("queue_position", "Option<u32>")
            .field("detail", "String")
            .field("artifact_set", "Option<BuildArtifactSetManifest>")
            .field("created_at", "OffsetDateTime")
            .field("started_at", "Option<OffsetDateTime>")
            .field("finished_at", "Option<OffsetDateTime>"),
    )?;
    layout.register_sinomo_object(
        SINOMO_BUILD_MODULE_OWNER,
        StorageRing::R1,
        "build/jobs/example/plan.sobj",
        ObjectSchema::new("BuildPlan", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("key", "BuildKey")
            .field("source", "BuildSource")
            .field("input", "BuildKeyInput"),
    )?;
    layout.register_sinomo_object(
        SINOMO_BUILD_MODULE_OWNER,
        StorageRing::R1,
        "build/cache/example/manifest.sobj",
        ObjectSchema::new("BuildArtifactSetManifest", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("schema_version", "u16")
            .field("provenance", "BuildProvenance")
            .field("artifacts", "Vec<BuildArtifactManifest>")
            .field("bundle_hash", "Option<String>")
            .field("bundle_bytes", "Option<u64>")
            .field("created_at", "OffsetDateTime"),
    )?;
    layout.register_binary(
        SINOMO_BUILD_MODULE_OWNER,
        StorageRing::R1,
        "build/cache/example/artifacts.tar",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::Id;
    use sinomo_testdir::TestDir;

    #[test]
    fn build_paths_keep_jobs_work_and_cache_separate() {
        let root = TestDir::new("sinomo-build-paths");
        let paths = BuildPaths::new(&root);
        paths.ensure_layout().expect("layout");
        let job_id = Id::from_legacy_named("buildjob_test").expect("job id");
        let build_key = BuildKey::new(format!("blake3:{}", "a".repeat(64))).expect("build key");

        assert_eq!(paths.root, root.join("build"));
        assert_eq!(
            paths.job_status(&job_id),
            root.join(format!("build/jobs/{job_id}/status.sobj"))
        );
        assert_eq!(
            paths.job_work_dir(&job_id),
            root.join(format!("build/work/{job_id}"))
        );
        assert_eq!(
            paths.cache_manifest(&build_key),
            root.join(format!("build/cache/{}/manifest.sobj", "a".repeat(64)))
        );
        assert_eq!(paths.managed_source, root.join("source/sinomo"));
        assert_eq!(paths.sources, root.join("build/sources"));
        assert_eq!(paths.checkouts, root.join("build/checkouts"));
        assert_eq!(paths.incremental, root.join("build/incremental"));
        assert_eq!(paths.toolchains, root.join("build/toolchains"));
        assert_eq!(
            paths.aarch64_gnu_cross_bin(),
            root.join("build/toolchains/aarch64-linux-gnu/usr/bin")
        );
        assert_eq!(paths.local_log, root.join("build/local.log"));
    }

    #[test]
    fn storage_registration_declares_build_owned_state() {
        let mut layout = StorageLayout::new();
        register_storage_paths(&mut layout).expect("register");

        assert_eq!(layout.escape_paths.len(), 8);
        assert!(
            layout
                .escape_paths
                .iter()
                .all(|path| path.module == SINOMO_BUILD_MODULE_OWNER)
        );
        assert!(
            layout
                .files
                .iter()
                .any(|file| file.relative_path == "build/cache/example/manifest.sobj")
        );
    }
}
