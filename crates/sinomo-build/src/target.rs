use crate::{BuildError, Result};
use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;
use std::cmp::Ordering;
use std::collections::BTreeSet;

/// A native compilation target. `rust_target` distinguishes ABI/toolchain
/// variants that share an OS and architecture (for example GNU and musl).
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct BuildTarget {
    pub os: String,
    pub arch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rust_target: Option<String>,
}

impl BuildTarget {
    pub fn new(os: impl Into<String>, arch: impl Into<String>) -> Result<Self> {
        let target = Self {
            os: os.into(),
            arch: arch.into(),
            family: None,
            rust_target: None,
        };
        target.validate()?;
        Ok(target)
    }

    pub fn host() -> Self {
        Self {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            family: Some(std::env::consts::FAMILY.to_string()),
            rust_target: None,
        }
    }

    /// Adds the concrete triple reported by `rustc -vV` to the host target.
    pub fn host_with_rust_target(rust_target: impl Into<String>) -> Result<Self> {
        Self::host().with_rust_target(rust_target)
    }

    pub fn with_family(mut self, family: impl Into<String>) -> Result<Self> {
        self.family = Some(family.into());
        self.validate()?;
        Ok(self)
    }

    pub fn with_rust_target(mut self, rust_target: impl Into<String>) -> Result<Self> {
        self.rust_target = Some(rust_target.into());
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        validate_target_token(&self.os, "target OS")?;
        validate_target_token(&self.arch, "target architecture")?;
        if let Some(family) = &self.family {
            validate_target_token(family, "target family")?;
        }
        if let Some(rust_target) = &self.rust_target {
            validate_target_token(rust_target, "Rust target triple")?;
        }
        Ok(())
    }

    /// True when `supported` is sufficiently specific and compatible with this
    /// requested target. An explicit Rust target must match exactly.
    pub fn is_supported_by(&self, supported: &Self) -> bool {
        self.os == supported.os
            && self.arch == supported.arch
            && self
                .family
                .as_ref()
                .is_none_or(|family| supported.family.as_ref() == Some(family))
            && self
                .rust_target
                .as_ref()
                .is_none_or(|target| supported.rust_target.as_ref() == Some(target))
    }

    pub fn path_key(&self) -> String {
        self.rust_target.clone().unwrap_or_else(|| {
            let family = self.family.as_deref().unwrap_or("unknown");
            format!("{}-{}-{family}", self.arch, self.os)
        })
    }

    pub fn is_windows(&self) -> bool {
        self.os == "windows" || self.family.as_deref() == Some("windows")
    }

    pub fn is_unix(&self) -> bool {
        self.family.as_deref() == Some("unix")
            || matches!(
                self.os.as_str(),
                "linux"
                    | "macos"
                    | "ios"
                    | "android"
                    | "freebsd"
                    | "openbsd"
                    | "netbsd"
                    | "dragonfly"
                    | "solaris"
            )
    }
}

fn validate_target_token(value: &str, label: &str) -> Result<()> {
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum BuildArtifactKind {
    Binary,
    NativeDylibModule,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildTargetCapability {
    pub target: BuildTarget,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_kinds: Vec<BuildArtifactKind>,
    pub toolchain_fingerprint: String,
    /// Newer builders key source-controlled builds by the immutable revision
    /// identity while verifying the Git bundle independently as transport.
    /// Missing means the legacy v1 bundle-hash key for rolling upgrades.
    #[serde(default)]
    pub supports_source_identity_hash: bool,
}

impl BuildTargetCapability {
    pub fn supports(&self, target: &BuildTarget, kinds: &[BuildArtifactKind]) -> bool {
        target.is_supported_by(&self.target)
            && kinds.iter().all(|kind| self.artifact_kinds.contains(kind))
    }
}

/// Live, non-authoritative resource facts used only for build placement.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildResourceSnapshot {
    pub cpu_threads: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_total_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_available_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_load_milli: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_total_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_available_mb: Option<u64>,
    pub active_jobs: u16,
    pub max_jobs: u16,
    pub queue_depth: u32,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildPlacementRequirements {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum_memory_available_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum_disk_available_mb: Option<u64>,
}

impl BuildPlacementRequirements {
    pub fn for_source_bytes(source_bytes: u64) -> Self {
        let source_mb = source_bytes.saturating_add(1_048_575) / 1_048_576;
        Self {
            minimum_memory_available_mb: Some(2_048),
            minimum_disk_available_mb: Some(2_048_u64.max(source_mb.saturating_mul(8))),
        }
    }

    pub fn is_satisfied_by(self, resources: &BuildResourceSnapshot) -> bool {
        self.minimum_memory_available_mb.is_none_or(|minimum| {
            resources
                .memory_available_mb
                .is_none_or(|available| available >= minimum)
        }) && self.minimum_disk_available_mb.is_none_or(|minimum| {
            resources
                .disk_available_mb
                .is_none_or(|available| available >= minimum)
        })
    }
}

impl BuildResourceSnapshot {
    pub fn available_slots(&self) -> u16 {
        self.max_jobs.saturating_sub(self.active_jobs)
    }

    pub fn is_saturated(&self) -> bool {
        self.available_slots() == 0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildServiceCapabilities {
    pub node_id: NodeId,
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<BuildTargetCapability>,
    pub resources: BuildResourceSnapshot,
}

impl BuildServiceCapabilities {
    pub fn supports(&self, target: &BuildTarget, kinds: &[BuildArtifactKind]) -> bool {
        self.targets
            .iter()
            .any(|capability| capability.supports(target, kinds))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BuildCandidate {
    pub capabilities: BuildServiceCapabilities,
    #[serde(default)]
    pub cache_hit: bool,
    #[serde(default)]
    pub transfer_cost: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RankedBuildCandidate {
    pub candidate: BuildCandidate,
    pub rank: usize,
    pub reason: String,
}

/// Returns only compatible builders, best first. Exact cache reuse dominates
/// build capacity; cache misses prefer idle, uncongested, powerful hosts.
pub fn rank_build_candidates(
    target: &BuildTarget,
    artifact_kinds: &[BuildArtifactKind],
    candidates: impl IntoIterator<Item = BuildCandidate>,
) -> Vec<RankedBuildCandidate> {
    rank_build_candidates_with_requirements(
        target,
        artifact_kinds,
        BuildPlacementRequirements::default(),
        candidates,
    )
}

pub fn rank_build_candidates_with_requirements(
    target: &BuildTarget,
    artifact_kinds: &[BuildArtifactKind],
    requirements: BuildPlacementRequirements,
    candidates: impl IntoIterator<Item = BuildCandidate>,
) -> Vec<RankedBuildCandidate> {
    let requested_kinds = artifact_kinds.iter().copied().collect::<BTreeSet<_>>();
    let requested_kinds = requested_kinds.into_iter().collect::<Vec<_>>();
    let mut candidates = candidates
        .into_iter()
        .filter(|candidate| {
            candidate.capabilities.protocol == crate::BUILD_PROTOCOL
                && candidate.capabilities.resources.cpu_threads > 0
                && candidate.capabilities.resources.max_jobs > 0
                && (candidate.cache_hit
                    || requirements.is_satisfied_by(&candidate.capabilities.resources))
                && candidate.capabilities.supports(target, &requested_kinds)
        })
        .collect::<Vec<_>>();
    candidates.sort_by(compare_build_candidates);
    candidates
        .into_iter()
        .enumerate()
        .map(|(rank, candidate)| RankedBuildCandidate {
            reason: candidate_rank_reason(&candidate),
            candidate,
            rank: rank + 1,
        })
        .collect()
}

fn compare_build_candidates(left: &BuildCandidate, right: &BuildCandidate) -> Ordering {
    right
        .cache_hit
        .cmp(&left.cache_hit)
        .then_with(|| compare_effective_throughput(left, right))
        .then_with(|| {
            right
                .capabilities
                .resources
                .memory_available_mb
                .unwrap_or_default()
                .cmp(
                    &left
                        .capabilities
                        .resources
                        .memory_available_mb
                        .unwrap_or_default(),
                )
        })
        .then_with(|| {
            left.capabilities
                .resources
                .cpu_load_milli
                .unwrap_or(u16::MAX)
                .cmp(
                    &right
                        .capabilities
                        .resources
                        .cpu_load_milli
                        .unwrap_or(u16::MAX),
                )
        })
        .then_with(|| {
            right
                .capabilities
                .resources
                .disk_available_mb
                .unwrap_or_default()
                .cmp(
                    &left
                        .capabilities
                        .resources
                        .disk_available_mb
                        .unwrap_or_default(),
                )
        })
        .then_with(|| left.transfer_cost.cmp(&right.transfer_cost))
        .then_with(|| {
            right
                .capabilities
                .resources
                .cpu_threads
                .cmp(&left.capabilities.resources.cpu_threads)
        })
        .then_with(|| left.capabilities.node_id.cmp(&right.capabilities.node_id))
}

fn compare_effective_throughput(left: &BuildCandidate, right: &BuildCandidate) -> Ordering {
    let left_resources = &left.capabilities.resources;
    let right_resources = &right.capabilities.resources;
    let left_denominator = 1_u64
        .saturating_add(u64::from(left_resources.active_jobs))
        .saturating_add(u64::from(left_resources.queue_depth));
    let right_denominator = 1_u64
        .saturating_add(u64::from(right_resources.active_jobs))
        .saturating_add(u64::from(right_resources.queue_depth));
    let left_weighted = u64::from(left_resources.cpu_threads).saturating_mul(right_denominator);
    let right_weighted = u64::from(right_resources.cpu_threads).saturating_mul(left_denominator);
    right_weighted.cmp(&left_weighted)
}

fn candidate_rank_reason(candidate: &BuildCandidate) -> String {
    let resources = &candidate.capabilities.resources;
    if candidate.cache_hit {
        return "exact build artifact cache hit".to_string();
    }
    format!(
        "{} available build slot(s), {} CPU thread(s), {} MiB disk available, {} queued job(s)",
        resources.available_slots(),
        resources.cpu_threads,
        resources.disk_available_mb.unwrap_or_default(),
        resources.queue_depth
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
    struct LegacyBuildTargetCapability {
        target: BuildTarget,
        artifact_kinds: Vec<BuildArtifactKind>,
        toolchain_fingerprint: String,
    }

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn candidate(name: &str, cpu_threads: u16, memory_mb: u64) -> BuildCandidate {
        let target = BuildTarget::new("linux", "aarch64")
            .expect("target")
            .with_family("unix")
            .expect("family");
        BuildCandidate {
            capabilities: BuildServiceCapabilities {
                node_id: node(name),
                protocol: crate::BUILD_PROTOCOL.to_string(),
                targets: vec![BuildTargetCapability {
                    target,
                    artifact_kinds: vec![
                        BuildArtifactKind::Binary,
                        BuildArtifactKind::NativeDylibModule,
                    ],
                    toolchain_fingerprint: "rustc-test".to_string(),
                    supports_source_identity_hash: true,
                }],
                resources: BuildResourceSnapshot {
                    cpu_threads,
                    memory_total_mb: Some(memory_mb),
                    memory_available_mb: Some(memory_mb),
                    cpu_load_milli: Some(0),
                    disk_total_mb: Some(256 * 1_024),
                    disk_available_mb: Some(128 * 1_024),
                    active_jobs: 0,
                    max_jobs: 2,
                    queue_depth: 0,
                },
            },
            cache_hit: false,
            transfer_cost: 1,
        }
    }

    #[test]
    fn source_identity_capability_is_wire_compatible_with_legacy_builders() {
        let legacy = LegacyBuildTargetCapability {
            target: BuildTarget::new("linux", "x86_64").expect("target"),
            artifact_kinds: vec![BuildArtifactKind::Binary],
            toolchain_fingerprint: "legacy-toolchain".to_string(),
        };
        let codec = sinomo_core::network::SinomoObjectCodec::network_frame();
        let legacy_bytes = codec.encode(&legacy).expect("encode legacy");
        let decoded: BuildTargetCapability = codec.decode(&legacy_bytes).expect("decode current");
        assert!(!decoded.supports_source_identity_hash);

        let current = BuildTargetCapability {
            target: legacy.target.clone(),
            artifact_kinds: legacy.artifact_kinds.clone(),
            toolchain_fingerprint: legacy.toolchain_fingerprint.clone(),
            supports_source_identity_hash: true,
        };
        let current_bytes = codec.encode(&current).expect("encode current");
        let decoded_legacy: LegacyBuildTargetCapability =
            codec.decode(&current_bytes).expect("decode legacy");
        assert_eq!(decoded_legacy, legacy);
    }

    #[test]
    fn powerful_idle_server_beats_raspberry_pi_for_cache_miss() {
        let target = BuildTarget::new("linux", "aarch64")
            .expect("target")
            .with_family("unix")
            .expect("family");
        let ranked = rank_build_candidates(
            &target,
            &[BuildArtifactKind::Binary],
            [
                candidate("raspberry-pi", 4, 4_096),
                candidate("server", 64, 131_072),
            ],
        );

        assert_eq!(ranked[0].candidate.capabilities.node_id, node("server"));
    }

    #[test]
    fn exact_cache_hit_beats_more_powerful_cache_miss() {
        let target = BuildTarget::new("linux", "aarch64")
            .expect("target")
            .with_family("unix")
            .expect("family");
        let mut cached = candidate("cached-builder", 4, 4_096);
        cached.cache_hit = true;
        let ranked = rank_build_candidates(
            &target,
            &[BuildArtifactKind::Binary],
            [candidate("server", 64, 131_072), cached],
        );

        assert_eq!(
            ranked[0].candidate.capabilities.node_id,
            node("cached-builder")
        );
        assert!(ranked[0].reason.contains("cache hit"));
    }

    #[test]
    fn unsupported_target_is_not_ranked() {
        let target = BuildTarget::new("windows", "x86_64").expect("target");
        let ranked = rank_build_candidates(
            &target,
            &[BuildArtifactKind::Binary],
            [candidate("linux", 64, 131_072)],
        );
        assert!(ranked.is_empty());
    }

    #[test]
    fn low_disk_cache_miss_is_rejected_but_exact_hit_remains_eligible() {
        let target = BuildTarget::new("linux", "aarch64")
            .expect("target")
            .with_family("unix")
            .expect("family");
        let mut low_disk = candidate("low-disk", 16, 32_768);
        low_disk.capabilities.resources.disk_available_mb = Some(512);
        let requirements = BuildPlacementRequirements::for_source_bytes(1024);

        assert!(
            rank_build_candidates_with_requirements(
                &target,
                &[BuildArtifactKind::Binary],
                requirements,
                [low_disk.clone()],
            )
            .is_empty()
        );

        low_disk.cache_hit = true;
        assert_eq!(
            rank_build_candidates_with_requirements(
                &target,
                &[BuildArtifactKind::Binary],
                requirements,
                [low_disk],
            )
            .len(),
            1
        );
    }

    #[test]
    fn busy_powerful_server_beats_idle_pi_on_effective_throughput() {
        let target = BuildTarget::new("linux", "aarch64")
            .expect("target")
            .with_family("unix")
            .expect("family");
        let pi = candidate("idle-pi", 4, 4_096);
        let mut server = candidate("busy-server", 64, 131_072);
        server.capabilities.resources.active_jobs = 1;
        server.capabilities.resources.max_jobs = 1;
        server.capabilities.resources.queue_depth = 1;
        let ranked = rank_build_candidates(&target, &[BuildArtifactKind::Binary], [pi, server]);

        assert_eq!(
            ranked[0].candidate.capabilities.node_id,
            node("busy-server")
        );
    }
}
