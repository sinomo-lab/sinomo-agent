use crate::{
    BuildArtifactKind, BuildArtifactManifest, BuildArtifactSetManifest, BuildError,
    BuildServiceCapabilities, BuildTarget, BuildTargetCapability, NativeDylibArtifactMetadata,
    Result,
};
use serde::{Deserialize, Serialize};
use sinomo_core::Id;
use std::collections::BTreeSet;
use time::OffsetDateTime;

pub const BUILD_PROTOCOL: &str = "sinomo.build.v1";
pub const BUILD_RPC_CATEGORY: &str = "build";
pub const BUILD_CAPABILITIES_ROUTE: &str = "/build/capabilities";
pub const BUILD_SUBMIT_ROUTE: &str = "/build/submit";
pub const BUILD_STATUS_ROUTE: &str = "/build/status";
pub const BUILD_ARTIFACT_ROUTE: &str = "/build/artifact";
pub const BUILD_OBJECT_RPC_ROUTES: &[&str] = &[
    BUILD_CAPABILITIES_ROUTE,
    BUILD_SUBMIT_ROUTE,
    BUILD_STATUS_ROUTE,
    BUILD_ARTIFACT_ROUTE,
];

pub type BuildJobId = Id;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct BuildKey(String);

impl BuildKey {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let digest = value
            .strip_prefix("blake3:")
            .ok_or_else(|| BuildError::InvalidValue {
                detail: "build key must use the blake3:<hex> format".to_string(),
            })?;
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(BuildError::InvalidValue {
                detail: "build key must contain a 64-character BLAKE3 digest".to_string(),
            });
        }
        Ok(Self(format!("blake3:{}", digest.to_ascii_lowercase())))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn path_key(&self) -> &str {
        self.0.strip_prefix("blake3:").unwrap_or(&self.0)
    }
}

impl std::fmt::Display for BuildKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct BuildOutputRequest {
    pub kind: BuildArtifactKind,
    /// Stable semantic output name, for example `sinomo` or a module ID.
    pub name: String,
    pub package: String,
    /// Exact Cargo target name (`[[bin]].name` or `[lib].name`).
    pub cargo_target: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_dylib: Option<NativeDylibArtifactMetadata>,
}

impl BuildOutputRequest {
    pub fn binary(name: impl Into<String>, package: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            kind: BuildArtifactKind::Binary,
            cargo_target: name.clone(),
            name,
            package: package.into(),
            features: Vec::new(),
            native_dylib: None,
        }
    }

    pub fn native_dylib_module(
        name: impl Into<String>,
        package: impl Into<String>,
        cargo_target: impl Into<String>,
        metadata: NativeDylibArtifactMetadata,
    ) -> Self {
        Self {
            kind: BuildArtifactKind::NativeDylibModule,
            name: name.into(),
            package: package.into(),
            cargo_target: cargo_target.into(),
            features: Vec::new(),
            native_dylib: Some(metadata),
        }
    }

    pub fn with_features(mut self, features: impl IntoIterator<Item = String>) -> Self {
        self.features.extend(features);
        self.features.sort();
        self.features.dedup();
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildKeyInput {
    /// Hash of the exact source bundle or otherwise immutable source tree.
    pub source_hash: String,
    /// Source revision embedded into produced binaries. Because it affects the
    /// output bytes, it participates in the build key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
    /// Hash of the dependency lockfile used by the build.
    pub lockfile_hash: String,
    pub target: BuildTarget,
    pub profile: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<String>,
    pub toolchain_fingerprint: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<BuildOutputRequest>,
}

impl BuildKeyInput {
    pub fn normalized(mut self) -> Self {
        self.features.sort();
        self.features.dedup();
        for output in &mut self.outputs {
            output.features.sort();
            output.features.dedup();
        }
        self.outputs.sort();
        self.outputs.dedup();
        self
    }

    /// Resolves a generic OS/architecture request to the exact capability that
    /// will execute it. The selected triple and toolchain become part of the
    /// build key, preventing GNU/musl/MSVC or toolchain cache collisions.
    pub fn resolved_for_capability(mut self, capability: &BuildTargetCapability) -> Result<Self> {
        if !self.target.is_supported_by(&capability.target) {
            return Err(BuildError::InvalidValue {
                detail: format!(
                    "selected target {} does not satisfy requested target {}",
                    capability.target.path_key(),
                    self.target.path_key()
                ),
            });
        }
        if capability.target.rust_target.is_none() {
            return Err(BuildError::InvalidValue {
                detail: "selected build capability has no concrete Rust target triple".to_string(),
            });
        }
        self.target = capability.target.clone();
        self.toolchain_fingerprint = capability.toolchain_fingerprint.clone();
        Ok(self.normalized())
    }
}

pub fn compute_build_key(input: &BuildKeyInput) -> Result<BuildKey> {
    input.target.validate()?;
    validate_blake3_hash(&input.source_hash, "source hash")?;
    if input
        .source_revision
        .as_deref()
        .is_some_and(|revision| revision.trim().is_empty())
    {
        return Err(BuildError::InvalidValue {
            detail: "source revision must not be empty when present".to_string(),
        });
    }
    validate_blake3_hash(&input.lockfile_hash, "lockfile hash")?;
    validate_path_token(&input.profile, "build profile")?;
    validate_non_empty(&input.toolchain_fingerprint, "toolchain fingerprint")?;
    let input = input.clone().normalized();
    let mut hasher = blake3::Hasher::new();
    hash_field(&mut hasher, "source", input.source_hash.as_bytes());
    hash_field(
        &mut hasher,
        "source-revision",
        input
            .source_revision
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
    );
    hash_field(&mut hasher, "lockfile", input.lockfile_hash.as_bytes());
    hash_field(&mut hasher, "target-os", input.target.os.as_bytes());
    hash_field(&mut hasher, "target-arch", input.target.arch.as_bytes());
    hash_field(
        &mut hasher,
        "target-family",
        input
            .target
            .family
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
    );
    hash_field(
        &mut hasher,
        "rust-target",
        input
            .target
            .rust_target
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
    );
    hash_field(&mut hasher, "profile", input.profile.as_bytes());
    hash_field(
        &mut hasher,
        "toolchain",
        input.toolchain_fingerprint.as_bytes(),
    );
    for feature in input.features {
        validate_non_empty(&feature, "feature")?;
        hash_field(&mut hasher, "feature", feature.as_bytes());
    }
    for output in input.outputs {
        validate_non_empty(&output.name, "output name")?;
        validate_path_token(&output.package, "output package")?;
        validate_path_token(&output.cargo_target, "Cargo target name")?;
        hash_field(
            &mut hasher,
            "output-kind",
            match output.kind {
                BuildArtifactKind::Binary => b"binary",
                BuildArtifactKind::NativeDylibModule => b"native-dylib-module",
            },
        );
        hash_field(&mut hasher, "output-name", output.name.as_bytes());
        hash_field(&mut hasher, "output-package", output.package.as_bytes());
        hash_field(&mut hasher, "cargo-target", output.cargo_target.as_bytes());
        for feature in output.features {
            validate_non_empty(&feature, "output feature")?;
            hash_field(&mut hasher, "output-feature", feature.as_bytes());
        }
        match (output.kind, output.native_dylib) {
            (BuildArtifactKind::Binary, None) => {}
            (BuildArtifactKind::NativeDylibModule, Some(metadata)) => {
                metadata.validate()?;
                hash_field(
                    &mut hasher,
                    "module-id",
                    metadata.module_id.as_str().as_bytes(),
                );
                hash_field(
                    &mut hasher,
                    "module-interface-version",
                    metadata.interface_version.as_bytes(),
                );
                hash_field(
                    &mut hasher,
                    "module-cache-key",
                    metadata.cache_key.as_bytes(),
                );
                hash_field(
                    &mut hasher,
                    "module-library-name",
                    metadata.library_name.as_bytes(),
                );
                hash_field(&mut hasher, "module-source", metadata.source.as_bytes());
            }
            (BuildArtifactKind::Binary, Some(_)) => {
                return Err(BuildError::InvalidValue {
                    detail: format!("binary output {} has native-module metadata", output.name),
                });
            }
            (BuildArtifactKind::NativeDylibModule, None) => {
                return Err(BuildError::InvalidValue {
                    detail: format!(
                        "native-dylib output {} is missing loader metadata",
                        output.name
                    ),
                });
            }
        }
    }
    BuildKey::new(format!("blake3:{}", hasher.finalize().to_hex()))
}

fn hash_field(hasher: &mut blake3::Hasher, label: &str, value: &[u8]) {
    hasher.update(&(label.len() as u64).to_le_bytes());
    hasher.update(label.as_bytes());
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

fn validate_non_empty(value: &str, label: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(BuildError::InvalidValue {
            detail: format!("{label} must not be empty"),
        });
    }
    Ok(())
}

fn validate_path_token(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(BuildError::InvalidValue {
            detail: format!("{label} `{value}` must be a non-empty path-safe token"),
        });
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildSource {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    pub bundle_hash: String,
    pub bundle_bytes: u64,
}

impl BuildSource {
    pub fn validate(&self) -> Result<()> {
        validate_non_empty(&self.label, "build source label")?;
        if self
            .revision
            .as_deref()
            .is_some_and(|revision| revision.trim().is_empty())
        {
            return Err(BuildError::InvalidValue {
                detail: "build source revision must not be empty when present".to_string(),
            });
        }
        validate_blake3_hash(&self.bundle_hash, "build source bundle hash")?;
        if self.bundle_bytes == 0 {
            return Err(BuildError::InvalidValue {
                detail: "build source bundle must not be empty".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildPlan {
    pub key: BuildKey,
    pub source: BuildSource,
    pub input: BuildKeyInput,
}

impl BuildPlan {
    pub fn new(source: BuildSource, input: BuildKeyInput) -> Result<Self> {
        let input = input.normalized();
        let key = compute_build_key(&input)?;
        let plan = Self { key, source, input };
        plan.validate()?;
        Ok(plan)
    }

    pub fn validate(&self) -> Result<()> {
        self.source.validate()?;
        if self.source.bundle_hash != self.input.source_hash {
            return Err(BuildError::InvalidValue {
                detail: "build source bundle hash must match build-key source hash".to_string(),
            });
        }
        if self.source.revision != self.input.source_revision {
            return Err(BuildError::InvalidValue {
                detail: "build source revision must match build-key source revision".to_string(),
            });
        }
        if self.input.target.rust_target.is_none() {
            return Err(BuildError::InvalidValue {
                detail: "build plan target must include the selected concrete Rust target triple"
                    .to_string(),
            });
        }
        if self.input.outputs.is_empty() {
            return Err(BuildError::InvalidValue {
                detail: "build plan must request at least one output".to_string(),
            });
        }
        let computed = compute_build_key(&self.input)?;
        if computed != self.key {
            return Err(BuildError::InvalidValue {
                detail: format!(
                    "build plan key {} does not match recomputed key {computed}",
                    self.key
                ),
            });
        }
        Ok(())
    }

    pub fn verify_key(&self) -> Result<()> {
        self.validate()
    }

    pub fn artifact_kinds(&self) -> Vec<BuildArtifactKind> {
        self.input
            .outputs
            .iter()
            .map(|output| output.kind)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

pub fn validate_blake3_hash(value: &str, label: &str) -> Result<()> {
    let digest = value
        .strip_prefix("blake3:")
        .ok_or_else(|| BuildError::InvalidValue {
            detail: format!("{label} must use the blake3:<hex> format"),
        })?;
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(BuildError::InvalidValue {
            detail: format!("{label} must contain a 64-character BLAKE3 digest"),
        });
    }
    Ok(())
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildCapabilitiesRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<BuildTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_key: Option<BuildKey>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_kinds: Vec<BuildArtifactKind>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildCapabilitiesResponse {
    pub capabilities: BuildServiceCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_requested_target: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_hit: Option<bool>,
}

/// The source bundle is carried as the `/build/submit` request body. It is not
/// embedded in this small queue request head or in cluster KV.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildSubmitRequest {
    pub plan: BuildPlan,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildSubmitResponse {
    pub job_id: BuildJobId,
    pub state: BuildJobState,
    pub cache_hit: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum BuildJobState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl BuildJobState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildStatusRequest {
    pub job_id: BuildJobId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildJobStatus {
    pub job_id: BuildJobId,
    pub key: BuildKey,
    pub state: BuildJobState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_position: Option<u32>,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_set: Option<BuildArtifactSetManifest>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub started_at: Option<OffsetDateTime>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub finished_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildStatusResponse {
    pub status: BuildJobStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildArtifactRequest {
    pub build_key: BuildKey,
    pub artifact_id: String,
}

/// Response head for `/build/artifact`; the verified bytes are carried in the
/// response body.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildArtifactResponse {
    pub artifact: BuildArtifactManifest,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_input() -> BuildKeyInput {
        BuildKeyInput {
            source_hash: format!("blake3:{}", "a".repeat(64)),
            source_revision: Some("deadbeef".to_string()),
            lockfile_hash: format!("blake3:{}", "b".repeat(64)),
            target: BuildTarget::new("linux", "x86_64").expect("target"),
            profile: "release".to_string(),
            features: vec!["zeta".to_string(), "alpha".to_string()],
            toolchain_fingerprint: "rustc-1.90-cargo-1.90".to_string(),
            outputs: vec![BuildOutputRequest::binary("sinomo", "sinomo")],
        }
    }

    #[test]
    fn build_key_is_canonical_over_feature_order() {
        let left = compute_build_key(&key_input()).expect("key");
        let mut right = key_input();
        right.features.reverse();
        assert_eq!(left, compute_build_key(&right).expect("key"));
    }

    #[test]
    fn build_key_is_canonical_over_output_feature_order() {
        let mut left = key_input();
        left.outputs[0].features = vec!["zeta".to_string(), "alpha".to_string()];
        let mut right = left.clone();
        right.outputs[0].features.reverse();

        assert_eq!(
            compute_build_key(&left).expect("left key"),
            compute_build_key(&right).expect("right key")
        );
    }

    #[test]
    fn build_key_changes_with_target_or_toolchain() {
        let baseline = compute_build_key(&key_input()).expect("baseline");
        let mut other_target = key_input();
        other_target.target = BuildTarget::new("linux", "aarch64").expect("target");
        assert_ne!(
            baseline,
            compute_build_key(&other_target).expect("target key")
        );

        let mut other_toolchain = key_input();
        other_toolchain.toolchain_fingerprint = "rustc-next".to_string();
        assert_ne!(
            baseline,
            compute_build_key(&other_toolchain).expect("toolchain key")
        );

        let mut other_revision = key_input();
        other_revision.source_revision = Some("cafebabe".to_string());
        assert_ne!(
            baseline,
            compute_build_key(&other_revision).expect("revision key")
        );
    }

    #[test]
    fn selected_capability_resolves_target_and_toolchain_before_keying() {
        let input = key_input();
        let capability = BuildTargetCapability {
            target: BuildTarget::new("linux", "x86_64")
                .expect("target")
                .with_family("unix")
                .expect("family")
                .with_rust_target("x86_64-unknown-linux-musl")
                .expect("triple"),
            artifact_kinds: vec![BuildArtifactKind::Binary],
            toolchain_fingerprint: "cargo-toolchain-musl".to_string(),
        };

        let resolved = input.resolved_for_capability(&capability).expect("resolve");

        assert_eq!(
            resolved.target.rust_target.as_deref(),
            Some("x86_64-unknown-linux-musl")
        );
        assert_eq!(resolved.toolchain_fingerprint, "cargo-toolchain-musl");
    }

    #[test]
    fn decoded_plan_revalidates_source_identity() {
        let mut input = key_input();
        input.target = input
            .target
            .with_family("unix")
            .expect("family")
            .with_rust_target("x86_64-unknown-linux-gnu")
            .expect("triple");
        let mut plan = BuildPlan::new(
            BuildSource {
                label: "test source".to_string(),
                revision: Some("deadbeef".to_string()),
                bundle_hash: input.source_hash.clone(),
                bundle_bytes: 1,
            },
            input,
        )
        .expect("plan");
        let mut mismatched_revision = plan.clone();
        mismatched_revision.source.revision = Some("cafebabe".to_string());
        assert!(matches!(
            mismatched_revision.verify_key(),
            Err(BuildError::InvalidValue { .. })
        ));

        plan.source.bundle_hash = format!("blake3:{}", "c".repeat(64));

        assert!(matches!(
            plan.verify_key(),
            Err(BuildError::InvalidValue { .. })
        ));
    }
}
