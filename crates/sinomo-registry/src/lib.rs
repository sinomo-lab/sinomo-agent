//! Artifact registry metadata skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    ArtifactManifest, CandidateBundleAttestation, CandidateBundleContentManifest,
    CandidateBundleFileDigest, CandidateBundleManifest, EvaluationScorecard, Id, PolicyAction,
    PolicyDecision, RolloutPlan, TrustRing,
};
use sinomo_kv::{KvEntry, KvOp, KvTxn};
use sinomo_module::{
    ArtifactId, ModuleId, artifact_signature_signer_public_key, verify_artifact_signature,
};
use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use thiserror::Error;
use time::OffsetDateTime;

pub const TRUSTED_ARTIFACT_SIGNERS_RELATIVE_PATH: &str = "registry/trusted-artifact-signers.json";
pub const CANDIDATE_BUNDLE_REGISTRY_RECORDS_RELATIVE_DIR: &str = "registry/candidate-bundles";
pub const VERIFIED_CANDIDATE_ROLLOUT_MATERIAL_RELATIVE_DIR: &str =
    "registry/verified-rollout-material";
pub const CANDIDATE_BUNDLE_VERIFIED_ROLLOUT_MATERIAL_RELATIVE_PATH: &str =
    "verified-rollout-material.json";
pub const CLUSTER_TRUSTED_ARTIFACT_SIGNER_PREFIX: &str =
    "/cluster/registry/trusted-artifact-signers/";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RegistryEntry {
    pub artifact: ArtifactManifest,
    pub policy_decision: PolicyDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollout_plan: Option<RolloutPlan>,
    pub status: RegistryStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub registered_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CandidateRecord {
    pub artifact: ArtifactManifest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_job_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scorecard: Option<EvaluationScorecard>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub registered_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CandidateBundleRegistryRecord {
    pub job_id: Id,
    pub bundle_path: String,
    pub bundle_content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_manifest_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attestation_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    pub verification: CandidateBundleIntegrityReport,
    #[serde(with = "time::serde::rfc3339")]
    pub registered_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VerifiedCandidateRolloutMaterialRecord {
    pub candidate_bundle_job_id: Id,
    pub artifact: ArtifactManifest,
    pub policy_decision: PolicyDecision,
    pub rollout_plan: RolloutPlan,
    pub active_artifact_id: ArtifactId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub registered_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VerifiedCandidateRolloutMaterialValidationReport {
    pub candidate_bundle_job_id: Id,
    pub artifact_id: ArtifactId,
    pub module_id: ModuleId,
    pub rollout_plan_id: Id,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<VerifiedCandidateRolloutMaterialValidationCheck>,
    #[serde(with = "time::serde::rfc3339")]
    pub verified_at: OffsetDateTime,
}

impl VerifiedCandidateRolloutMaterialValidationReport {
    pub fn is_valid(&self) -> bool {
        !self.checks.is_empty() && self.checks.iter().all(|check| check.passed)
    }

    pub fn failed_checks(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter(|check| !check.passed)
            .map(|check| check.name.clone())
            .collect()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct VerifiedCandidateRolloutMaterialValidationCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CandidateBundleIntegrityReport {
    pub job_id: Id,
    pub bundle_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recomputed_content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_manifest_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attestation_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<CandidateBundleIntegrityCheck>,
    #[serde(with = "time::serde::rfc3339")]
    pub verified_at: OffsetDateTime,
}

impl CandidateBundleIntegrityReport {
    pub fn is_valid(&self) -> bool {
        !self.checks.is_empty() && self.checks.iter().all(|check| check.passed)
    }

    pub fn failed_checks(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter(|check| !check.passed)
            .map(|check| check.name.clone())
            .collect()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CandidateBundleIntegrityCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TrustedArtifactSigner {
    pub signer_id: String,
    pub public_key: String,
    pub status: ArtifactSignerStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_rings: Vec<TrustRing>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_modules: Vec<ModuleId>,
    #[serde(with = "time::serde::rfc3339")]
    pub registered_at: OffsetDateTime,
    pub reason: String,
}

impl TrustedArtifactSigner {
    pub fn local_ed25519(
        signer_id: impl Into<String>,
        public_key: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            signer_id: signer_id.into(),
            public_key: public_key.into(),
            status: ArtifactSignerStatus::Trusted,
            allowed_rings: Vec::new(),
            allowed_modules: Vec::new(),
            registered_at: OffsetDateTime::now_utc(),
            reason: reason.into(),
        }
    }

    pub fn allows_artifact(&self, artifact: &ArtifactManifest) -> bool {
        (self.allowed_rings.is_empty() || self.allowed_rings.contains(&artifact.target_ring))
            && (self.allowed_modules.is_empty()
                || self.allowed_modules.contains(&artifact.module_id))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ArtifactSignerStatus {
    Trusted,
    Revoked,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ArtifactSignerAuthorizationReport {
    pub artifact_id: ArtifactId,
    pub module_id: ModuleId,
    pub target_ring: TrustRing,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signer_public_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signer_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<ArtifactSignerAuthorizationCheck>,
}

impl ArtifactSignerAuthorizationReport {
    pub fn is_authorized(&self) -> bool {
        !self.checks.is_empty() && self.checks.iter().all(|check| check.passed)
    }

    pub fn failed_checks(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter(|check| !check.passed)
            .map(|check| check.name.clone())
            .collect()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ArtifactSignerAuthorizationCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RegistryStatus {
    Candidate,
    Approved,
    Denied,
    Quarantined,
    Active,
    RolledBack,
}

#[derive(Clone, Debug, Default)]
pub struct ArtifactRegistry {
    candidate_records: BTreeMap<ArtifactId, CandidateRecord>,
    candidate_bundles: BTreeMap<Id, CandidateBundleRegistryRecord>,
    entries: BTreeMap<ArtifactId, RegistryEntry>,
    latest_by_module: BTreeMap<ModuleId, ArtifactId>,
    trusted_signers: BTreeMap<String, TrustedArtifactSigner>,
}

impl ArtifactRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, entry: RegistryEntry) -> Result<(), RegistryError> {
        let artifact_id = entry.artifact.artifact_id.clone();
        if self.entries.contains_key(&artifact_id) {
            return Err(RegistryError::DuplicateArtifact { artifact_id });
        }

        if matches!(
            entry.status,
            RegistryStatus::Approved | RegistryStatus::Active
        ) {
            self.latest_by_module.insert(
                entry.artifact.module_id.clone(),
                entry.artifact.artifact_id.clone(),
            );
        }

        self.entries.insert(artifact_id, entry);
        Ok(())
    }

    pub fn register_candidate(&mut self, record: CandidateRecord) -> Result<(), RegistryError> {
        let artifact_id = record.artifact.artifact_id.clone();
        if self.candidate_records.contains_key(&artifact_id) {
            return Err(RegistryError::DuplicateCandidate { artifact_id });
        }
        self.candidate_records.insert(artifact_id, record);
        Ok(())
    }

    pub fn register_verified_candidate_bundle(
        &mut self,
        manifest: &CandidateBundleManifest,
    ) -> Result<&CandidateBundleRegistryRecord, RegistryError> {
        if self.candidate_bundles.contains_key(&manifest.job_id) {
            return Err(RegistryError::DuplicateCandidateBundle {
                job_id: manifest.job_id.clone(),
            });
        }

        let verification = verify_candidate_bundle_integrity(manifest)?;
        if !verification.is_valid() {
            return Err(RegistryError::CandidateBundleIntegrityFailed {
                job_id: manifest.job_id.clone(),
                failed_checks: verification.failed_checks(),
            });
        }

        let record = CandidateBundleRegistryRecord {
            job_id: manifest.job_id.clone(),
            bundle_path: manifest.bundle_path.clone(),
            bundle_content_hash: verification
                .recomputed_content_hash
                .clone()
                .unwrap_or_else(|| manifest.bundle_content_hash.clone().unwrap_or_default()),
            content_manifest_path: manifest.content_manifest_path.clone(),
            attestation_path: manifest.attestation_path.clone(),
            evidence_refs: candidate_bundle_evidence_refs(manifest),
            verification,
            registered_at: OffsetDateTime::now_utc(),
        };
        let job_id = record.job_id.clone();
        self.candidate_bundles.insert(job_id.clone(), record);
        Ok(self
            .candidate_bundles
            .get(&job_id)
            .expect("candidate bundle inserted"))
    }

    pub fn register_trusted_artifact_signer(
        &mut self,
        signer: TrustedArtifactSigner,
    ) -> Result<(), RegistryError> {
        if self.trusted_signers.contains_key(&signer.public_key) {
            return Err(RegistryError::DuplicateTrustedSigner {
                public_key: signer.public_key,
            });
        }
        self.trusted_signers
            .insert(signer.public_key.clone(), signer);
        Ok(())
    }

    pub fn trusted_signer(&self, public_key: &str) -> Option<&TrustedArtifactSigner> {
        self.trusted_signers.get(public_key)
    }

    pub fn authorize_artifact_signer(
        &self,
        artifact: &ArtifactManifest,
    ) -> ArtifactSignerAuthorizationReport {
        authorize_artifact_signature(
            artifact,
            &self.trusted_signers.values().cloned().collect::<Vec<_>>(),
        )
    }

    pub fn load_trusted_artifact_signers_from_data_dir(
        &mut self,
        data_dir_root: impl AsRef<Path>,
    ) -> Result<usize, RegistryError> {
        let signers = read_trusted_artifact_signers(data_dir_root)?;
        let count = signers.len();
        self.trusted_signers.clear();
        for signer in signers {
            self.trusted_signers
                .insert(signer.public_key.clone(), signer);
        }
        Ok(count)
    }

    pub fn load_candidate_bundle_records_from_data_dir(
        &mut self,
        data_dir_root: impl AsRef<Path>,
    ) -> Result<usize, RegistryError> {
        let records = read_candidate_bundle_registry_records(data_dir_root)?;
        let count = records.len();
        self.candidate_bundles.clear();
        for record in records {
            self.candidate_bundles.insert(record.job_id.clone(), record);
        }
        Ok(count)
    }

    pub fn load_trusted_artifact_signers_from_kv_entries(
        &mut self,
        entries: &[KvEntry],
    ) -> Result<usize, RegistryError> {
        let signers = trusted_artifact_signers_from_kv_entries(entries)?;
        let count = signers.len();
        self.trusted_signers.clear();
        for signer in signers {
            self.trusted_signers
                .insert(signer.public_key.clone(), signer);
        }
        Ok(count)
    }

    pub fn candidate(&self, artifact_id: &ArtifactId) -> Option<&CandidateRecord> {
        self.candidate_records.get(artifact_id)
    }

    pub fn candidate_bundle(&self, job_id: &Id) -> Option<&CandidateBundleRegistryRecord> {
        self.candidate_bundles.get(job_id)
    }

    pub fn entry(&self, artifact_id: &ArtifactId) -> Option<&RegistryEntry> {
        self.entries.get(artifact_id)
    }

    pub fn latest_for_module(&self, module_id: &ModuleId) -> Option<&RegistryEntry> {
        self.latest_by_module
            .get(module_id)
            .and_then(|artifact_id| self.entries.get(artifact_id))
    }
}

pub fn verify_candidate_bundle_integrity(
    manifest: &CandidateBundleManifest,
) -> Result<CandidateBundleIntegrityReport, RegistryError> {
    let bundle_root = PathBuf::from(&manifest.bundle_path);
    let mut report = CandidateBundleIntegrityReport {
        job_id: manifest.job_id.clone(),
        bundle_path: manifest.bundle_path.clone(),
        declared_content_hash: manifest.bundle_content_hash.clone(),
        recomputed_content_hash: None,
        content_manifest_path: manifest.content_manifest_path.clone(),
        attestation_path: manifest.attestation_path.clone(),
        checks: Vec::new(),
        verified_at: OffsetDateTime::now_utc(),
    };

    let Some(content_manifest_path) = &manifest.content_manifest_path else {
        report.checks.push(integrity_check(
            "content-manifest-path-present",
            false,
            "candidate manifest does not reference bundle-content-manifest.json",
        ));
        return Ok(report);
    };
    report.checks.push(integrity_check(
        "content-manifest-path-present",
        true,
        "candidate manifest references bundle content manifest",
    ));

    let content_manifest_path = bundle_artifact_path(&bundle_root, content_manifest_path);
    let content_manifest: CandidateBundleContentManifest =
        read_json_bundle_artifact(&content_manifest_path)?;
    report.checks.push(integrity_check(
        "content-manifest-hash-algorithm",
        content_manifest.hash_algorithm == "blake3",
        format!(
            "declared hash algorithm is `{}`",
            content_manifest.hash_algorithm
        ),
    ));
    report.checks.push(integrity_check(
        "content-manifest-bundle-path",
        content_manifest.bundle_path == manifest.bundle_path,
        "content manifest bundle path matches candidate manifest",
    ));

    let recomputed_files = build_candidate_bundle_file_digests(&bundle_root)?;
    let recomputed_content_hash = bundle_content_hash(&recomputed_files);
    report.recomputed_content_hash = Some(recomputed_content_hash.clone());
    report.checks.push(integrity_check(
        "content-manifest-files-match",
        content_manifest.files == recomputed_files,
        "content manifest covers every non-control file with matching digest and byte count",
    ));
    report.checks.push(integrity_check(
        "content-manifest-hash-matches-recomputed",
        content_manifest.bundle_content_hash == recomputed_content_hash,
        "content manifest aggregate hash matches recomputed bundle hash",
    ));
    report.checks.push(integrity_check(
        "candidate-manifest-hash-matches-recomputed",
        manifest.bundle_content_hash.as_deref() == Some(recomputed_content_hash.as_str()),
        "candidate manifest aggregate hash matches recomputed bundle hash",
    ));

    let Some(attestation_path) = &manifest.attestation_path else {
        report.checks.push(integrity_check(
            "attestation-path-present",
            false,
            "candidate manifest does not reference bundle-attestation.json",
        ));
        return Ok(report);
    };
    report.checks.push(integrity_check(
        "attestation-path-present",
        true,
        "candidate manifest references bundle attestation",
    ));

    let attestation_path = bundle_artifact_path(&bundle_root, attestation_path);
    let attestation: CandidateBundleAttestation = read_json_bundle_artifact(&attestation_path)?;
    report.checks.push(integrity_check(
        "attestation-statement-type",
        attestation.statement_type == "https://in-toto.io/Statement/v1",
        format!("statement type is `{}`", attestation.statement_type),
    ));
    report.checks.push(integrity_check(
        "attestation-predicate-type",
        attestation.predicate_type
            == "https://sinomo.net/attestations/evolution-candidate-bundle/v1",
        format!("predicate type is `{}`", attestation.predicate_type),
    ));
    report.checks.push(integrity_check(
        "attestation-subject-hash",
        attestation.subject.digest == recomputed_content_hash,
        "attestation subject digest matches recomputed bundle hash",
    ));
    report.checks.push(integrity_check(
        "attestation-job-id",
        attestation.predicate.job_id == manifest.job_id,
        "attestation predicate job id matches candidate manifest",
    ));
    report.checks.push(integrity_check(
        "attestation-artifact-refs-exist",
        artifact_refs_exist(&bundle_root, &attestation.predicate.artifact_refs),
        "all filesystem artifact refs named by the attestation exist",
    ));

    Ok(report)
}

pub fn authorize_artifact_signature(
    artifact: &ArtifactManifest,
    trusted_signers: &[TrustedArtifactSigner],
) -> ArtifactSignerAuthorizationReport {
    let mut report = ArtifactSignerAuthorizationReport {
        artifact_id: artifact.artifact_id.clone(),
        module_id: artifact.module_id.clone(),
        target_ring: artifact.target_ring.clone(),
        signer_public_key: None,
        signer_id: None,
        checks: Vec::new(),
    };

    match verify_artifact_signature(artifact) {
        Ok(()) => report.checks.push(signer_authorization_check(
            "artifact-signature-valid",
            true,
            "artifact signature is cryptographically valid",
        )),
        Err(error) => {
            report.checks.push(signer_authorization_check(
                "artifact-signature-valid",
                false,
                error.to_string(),
            ));
            return report;
        }
    }

    let signer_public_key = match artifact_signature_signer_public_key(artifact) {
        Ok(public_key) => public_key,
        Err(error) => {
            report.checks.push(signer_authorization_check(
                "artifact-signature-signer-present",
                false,
                error.to_string(),
            ));
            return report;
        }
    };
    report.signer_public_key = Some(signer_public_key.clone());
    report.checks.push(signer_authorization_check(
        "artifact-signature-signer-present",
        true,
        "artifact signature names a signer public key",
    ));

    let Some(signer) = trusted_signers
        .iter()
        .find(|signer| signer.public_key == signer_public_key)
    else {
        report.checks.push(signer_authorization_check(
            "artifact-signer-known",
            false,
            "artifact signer public key is not registered as trusted",
        ));
        return report;
    };
    report.signer_id = Some(signer.signer_id.clone());
    report.checks.push(signer_authorization_check(
        "artifact-signer-known",
        true,
        format!("artifact signer `{}` is registered", signer.signer_id),
    ));
    report.checks.push(signer_authorization_check(
        "artifact-signer-trusted",
        signer.status == ArtifactSignerStatus::Trusted,
        format!("artifact signer status is {:?}", signer.status),
    ));
    report.checks.push(signer_authorization_check(
        "artifact-signer-ring-authorized",
        signer.allowed_rings.is_empty() || signer.allowed_rings.contains(&artifact.target_ring),
        "artifact signer is authorized for the target ring",
    ));
    report.checks.push(signer_authorization_check(
        "artifact-signer-module-authorized",
        signer.allowed_modules.is_empty() || signer.allowed_modules.contains(&artifact.module_id),
        "artifact signer is authorized for the target module",
    ));
    report
}

pub fn trusted_artifact_signers_path(data_dir_root: impl AsRef<Path>) -> PathBuf {
    data_dir_root
        .as_ref()
        .join(TRUSTED_ARTIFACT_SIGNERS_RELATIVE_PATH)
}

pub fn read_trusted_artifact_signers(
    data_dir_root: impl AsRef<Path>,
) -> Result<Vec<TrustedArtifactSigner>, RegistryError> {
    let path = trusted_artifact_signers_path(data_dir_root);
    match fs::read_to_string(&path) {
        Ok(content) => serde_json::from_str(&content).map_err(|source| {
            RegistryError::TrustedSignerStoreParse {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(source) => Err(RegistryError::TrustedSignerStoreRead {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

pub fn write_trusted_artifact_signers(
    data_dir_root: impl AsRef<Path>,
    signers: &[TrustedArtifactSigner],
) -> Result<PathBuf, RegistryError> {
    let path = trusted_artifact_signers_path(data_dir_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| RegistryError::TrustedSignerStoreWrite {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let content = serde_json::to_string_pretty(signers).map_err(|source| {
        RegistryError::TrustedSignerStoreParse {
            path: path.display().to_string(),
            detail: source.to_string(),
        }
    })?;
    fs::write(&path, content).map_err(|source| RegistryError::TrustedSignerStoreWrite {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(path)
}

pub fn candidate_bundle_registry_records_dir(data_dir_root: impl AsRef<Path>) -> PathBuf {
    data_dir_root
        .as_ref()
        .join(CANDIDATE_BUNDLE_REGISTRY_RECORDS_RELATIVE_DIR)
}

pub fn candidate_bundle_registry_record_path(
    data_dir_root: impl AsRef<Path>,
    job_id: &Id,
) -> PathBuf {
    candidate_bundle_registry_records_dir(data_dir_root).join(format!("{}.json", job_id.as_str()))
}

pub fn read_candidate_bundle_registry_record(
    data_dir_root: impl AsRef<Path>,
    job_id: &Id,
) -> Result<Option<CandidateBundleRegistryRecord>, RegistryError> {
    let path = candidate_bundle_registry_record_path(data_dir_root, job_id);
    match fs::read_to_string(&path) {
        Ok(content) => serde_json::from_str(&content).map(Some).map_err(|source| {
            RegistryError::CandidateBundleRecordStoreParse {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(RegistryError::CandidateBundleRecordStoreRead {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

pub fn read_candidate_bundle_registry_records(
    data_dir_root: impl AsRef<Path>,
) -> Result<Vec<CandidateBundleRegistryRecord>, RegistryError> {
    let dir = candidate_bundle_registry_records_dir(data_dir_root);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(RegistryError::CandidateBundleRecordStoreRead {
                path: dir.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| RegistryError::CandidateBundleRecordStoreRead {
            path: dir.display().to_string(),
            detail: source.to_string(),
        })?;
        let path = entry.path();
        if path.is_file()
            && path
                .extension()
                .is_some_and(|extension| extension == "json")
        {
            paths.push(path);
        }
    }
    paths.sort();
    let mut records = Vec::with_capacity(paths.len());
    for path in paths {
        let content = fs::read_to_string(&path).map_err(|source| {
            RegistryError::CandidateBundleRecordStoreRead {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        })?;
        records.push(serde_json::from_str(&content).map_err(|source| {
            RegistryError::CandidateBundleRecordStoreParse {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        })?);
    }
    Ok(records)
}

pub fn write_candidate_bundle_registry_record(
    data_dir_root: impl AsRef<Path>,
    record: &CandidateBundleRegistryRecord,
) -> Result<PathBuf, RegistryError> {
    let path = candidate_bundle_registry_record_path(data_dir_root, &record.job_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| {
            RegistryError::CandidateBundleRecordStoreWrite {
                path: parent.display().to_string(),
                detail: source.to_string(),
            }
        })?;
    }
    let content = serde_json::to_string_pretty(record).map_err(|source| {
        RegistryError::CandidateBundleRecordStoreParse {
            path: path.display().to_string(),
            detail: source.to_string(),
        }
    })?;
    fs::write(&path, content).map_err(|source| RegistryError::CandidateBundleRecordStoreWrite {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(path)
}

pub fn verified_candidate_rollout_material_dir(data_dir_root: impl AsRef<Path>) -> PathBuf {
    data_dir_root
        .as_ref()
        .join(VERIFIED_CANDIDATE_ROLLOUT_MATERIAL_RELATIVE_DIR)
}

pub fn verified_candidate_rollout_material_path(
    data_dir_root: impl AsRef<Path>,
    job_id: &Id,
) -> PathBuf {
    verified_candidate_rollout_material_dir(data_dir_root).join(format!("{}.json", job_id.as_str()))
}

pub fn candidate_bundle_verified_rollout_material_path(
    manifest: &CandidateBundleManifest,
) -> PathBuf {
    bundle_artifact_path(
        &PathBuf::from(&manifest.bundle_path),
        CANDIDATE_BUNDLE_VERIFIED_ROLLOUT_MATERIAL_RELATIVE_PATH,
    )
}

pub fn read_candidate_bundle_verified_rollout_material(
    manifest: &CandidateBundleManifest,
) -> Result<Option<VerifiedCandidateRolloutMaterialRecord>, RegistryError> {
    let path = candidate_bundle_verified_rollout_material_path(manifest);
    match fs::read_to_string(&path) {
        Ok(content) => serde_json::from_str(&content).map(Some).map_err(|source| {
            RegistryError::VerifiedRolloutMaterialStoreParse {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(RegistryError::VerifiedRolloutMaterialStoreRead {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

pub fn read_verified_candidate_rollout_material(
    data_dir_root: impl AsRef<Path>,
    job_id: &Id,
) -> Result<Option<VerifiedCandidateRolloutMaterialRecord>, RegistryError> {
    let path = verified_candidate_rollout_material_path(data_dir_root, job_id);
    match fs::read_to_string(&path) {
        Ok(content) => serde_json::from_str(&content).map(Some).map_err(|source| {
            RegistryError::VerifiedRolloutMaterialStoreParse {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(RegistryError::VerifiedRolloutMaterialStoreRead {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

pub fn write_verified_candidate_rollout_material(
    data_dir_root: impl AsRef<Path>,
    record: &VerifiedCandidateRolloutMaterialRecord,
) -> Result<PathBuf, RegistryError> {
    let path =
        verified_candidate_rollout_material_path(data_dir_root, &record.candidate_bundle_job_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| {
            RegistryError::VerifiedRolloutMaterialStoreWrite {
                path: parent.display().to_string(),
                detail: source.to_string(),
            }
        })?;
    }
    let content = serde_json::to_string_pretty(record).map_err(|source| {
        RegistryError::VerifiedRolloutMaterialStoreParse {
            path: path.display().to_string(),
            detail: source.to_string(),
        }
    })?;
    fs::write(&path, content).map_err(|source| {
        RegistryError::VerifiedRolloutMaterialStoreWrite {
            path: path.display().to_string(),
            detail: source.to_string(),
        }
    })?;
    Ok(path)
}

pub fn validate_verified_candidate_rollout_material(
    material: &VerifiedCandidateRolloutMaterialRecord,
    candidate_bundle: &CandidateBundleRegistryRecord,
    trusted_signers: &[TrustedArtifactSigner],
) -> VerifiedCandidateRolloutMaterialValidationReport {
    let mut checks = Vec::new();
    checks.push(verified_rollout_material_check(
        "candidate-bundle-job-id",
        material.candidate_bundle_job_id == candidate_bundle.job_id,
        format!(
            "material references {}, candidate bundle is {}",
            material.candidate_bundle_job_id, candidate_bundle.job_id
        ),
    ));
    checks.push(verified_rollout_material_check(
        "candidate-bundle-integrity",
        candidate_bundle.verification.is_valid(),
        if candidate_bundle.verification.is_valid() {
            format!(
                "candidate bundle integrity is valid with hash {}",
                candidate_bundle.bundle_content_hash
            )
        } else {
            format!(
                "candidate bundle integrity failed: {}",
                candidate_bundle.verification.failed_checks().join(", ")
            )
        },
    ));
    checks.push(verified_rollout_material_check(
        "artifact-rollout-artifact-id",
        material.artifact.artifact_id == material.rollout_plan.artifact_id,
        "artifact id matches rollout plan target artifact",
    ));
    checks.push(verified_rollout_material_check(
        "artifact-rollout-module-id",
        material.artifact.module_id == material.rollout_plan.module_id,
        "artifact module id matches rollout plan module id",
    ));
    checks.push(verified_rollout_material_check(
        "artifact-rollout-target-ring",
        material.artifact.target_ring == material.rollout_plan.target_ring,
        "artifact target ring matches rollout plan target ring",
    ));
    checks.push(verified_rollout_material_check(
        "policy-target-artifact",
        material.policy_decision.target_artifact_id == material.artifact.artifact_id,
        "policy target artifact matches candidate artifact",
    ));
    checks.push(verified_rollout_material_check(
        "policy-target-ring",
        material.policy_decision.target_ring == material.artifact.target_ring,
        "policy target ring matches candidate artifact ring",
    ));
    checks.push(verified_rollout_material_check(
        "policy-allows-artifact",
        material.policy_decision.action == PolicyAction::Allow,
        format!("policy action is {:?}", material.policy_decision.action),
    ));
    let signer_report = authorize_artifact_signature(&material.artifact, trusted_signers);
    checks.push(verified_rollout_material_check(
        "artifact-signature-authorized",
        signer_report.is_authorized(),
        if signer_report.is_authorized() {
            signer_report
                .signer_id
                .as_deref()
                .map(|signer| format!("artifact signature is authorized by signer `{signer}`"))
                .unwrap_or_else(|| "artifact signature is authorized".to_string())
        } else {
            format!(
                "artifact signature authorization failed: {}",
                signer_report.failed_checks().join(", ")
            )
        },
    ));
    checks.push(verified_rollout_material_check(
        "evidence-refs-present",
        !material.evidence_refs.is_empty(),
        format!(
            "material carries {} evidence refs",
            material.evidence_refs.len()
        ),
    ));

    VerifiedCandidateRolloutMaterialValidationReport {
        candidate_bundle_job_id: material.candidate_bundle_job_id.clone(),
        artifact_id: material.artifact.artifact_id.clone(),
        module_id: material.artifact.module_id.clone(),
        rollout_plan_id: material.rollout_plan.rollout_plan_id.clone(),
        checks,
        verified_at: OffsetDateTime::now_utc(),
    }
}

pub fn verify_and_write_verified_candidate_rollout_material(
    data_dir_root: impl AsRef<Path>,
    record: &VerifiedCandidateRolloutMaterialRecord,
    candidate_bundle: &CandidateBundleRegistryRecord,
    trusted_signers: &[TrustedArtifactSigner],
) -> Result<(PathBuf, VerifiedCandidateRolloutMaterialValidationReport), RegistryError> {
    let report =
        validate_verified_candidate_rollout_material(record, candidate_bundle, trusted_signers);
    if !report.is_valid() {
        return Err(RegistryError::VerifiedRolloutMaterialInvalid {
            job_id: record.candidate_bundle_job_id.clone(),
            failed_checks: report.failed_checks(),
        });
    }
    let path = write_verified_candidate_rollout_material(data_dir_root, record)?;
    Ok((path, report))
}

pub fn cluster_trusted_artifact_signer_key(public_key: &str) -> String {
    let digest = blake3::hash(public_key.as_bytes());
    format!(
        "{}blake3-{}",
        CLUSTER_TRUSTED_ARTIFACT_SIGNER_PREFIX,
        digest.to_hex()
    )
}

pub fn trusted_artifact_signer_kv_put_op(
    signer: &TrustedArtifactSigner,
) -> Result<KvOp, RegistryError> {
    KvOp::put_serde(
        cluster_trusted_artifact_signer_key(&signer.public_key),
        signer,
        None,
    )
    .map_err(|source| RegistryError::TrustedSignerKvEncode {
        public_key: signer.public_key.clone(),
        detail: source.to_string(),
    })
}

pub fn trusted_artifact_signers_kv_txn(
    signers: &[TrustedArtifactSigner],
) -> Result<KvTxn, RegistryError> {
    let mut success = Vec::with_capacity(signers.len());
    for signer in signers {
        success.push(trusted_artifact_signer_kv_put_op(signer)?);
    }
    Ok(KvTxn {
        compares: Vec::new(),
        success,
        failure: Vec::new(),
    })
}

pub fn trusted_artifact_signers_from_kv_entries(
    entries: &[KvEntry],
) -> Result<Vec<TrustedArtifactSigner>, RegistryError> {
    let mut signers = BTreeMap::new();
    for entry in entries {
        if !entry
            .key
            .starts_with(CLUSTER_TRUSTED_ARTIFACT_SIGNER_PREFIX)
        {
            continue;
        }
        let signer: TrustedArtifactSigner =
            entry
                .decode()
                .map_err(|source| RegistryError::TrustedSignerKvDecode {
                    key: entry.key.clone(),
                    detail: source.to_string(),
                })?;
        let expected_key = cluster_trusted_artifact_signer_key(&signer.public_key);
        if entry.key != expected_key {
            return Err(RegistryError::TrustedSignerKvKeyMismatch {
                key: entry.key.clone(),
                expected_key,
                public_key: signer.public_key,
            });
        }
        signers.insert(signer.public_key.clone(), signer);
    }
    Ok(signers.into_values().collect())
}

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    use sinomo_storage::StorageRing;

    layout.register_human_text(
        "sinomo-registry",
        StorageRing::R1,
        TRUSTED_ARTIFACT_SIGNERS_RELATIVE_PATH,
    )?;
    layout.register_escape_path(
        "sinomo-registry",
        StorageRing::R1,
        CANDIDATE_BUNDLE_REGISTRY_RECORDS_RELATIVE_DIR,
        "verified candidate bundle registry records use dynamic evolution job id files",
    )?;
    layout.register_escape_path(
        "sinomo-registry",
        StorageRing::R1,
        VERIFIED_CANDIDATE_ROLLOUT_MATERIAL_RELATIVE_DIR,
        "verified rollout material records use dynamic evolution job id files",
    )
}

fn build_candidate_bundle_file_digests(
    bundle_root: &Path,
) -> Result<Vec<CandidateBundleFileDigest>, RegistryError> {
    let mut files = Vec::new();
    collect_candidate_bundle_file_digests(bundle_root, bundle_root, &mut files)?;
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn collect_candidate_bundle_file_digests(
    bundle_root: &Path,
    path: &Path,
    files: &mut Vec<CandidateBundleFileDigest>,
) -> Result<(), RegistryError> {
    let entries = fs::read_dir(path).map_err(|source| RegistryError::BundleArtifactRead {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| RegistryError::BundleArtifactRead {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            collect_candidate_bundle_file_digests(bundle_root, &entry_path, files)?;
        } else if entry_path.is_file() {
            let relative = bundle_relative_path(bundle_root, &entry_path);
            if is_bundle_integrity_control_file(&relative) {
                continue;
            }
            files.push(file_digest(&entry_path, relative)?);
        }
    }
    Ok(())
}

fn file_digest(path: &Path, relative: String) -> Result<CandidateBundleFileDigest, RegistryError> {
    let mut file = fs::File::open(path).map_err(|source| RegistryError::BundleArtifactRead {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 8192];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| RegistryError::BundleArtifactRead {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        if read == 0 {
            break;
        }
        bytes = bytes.saturating_add(read as u64);
        hasher.update(&buffer[..read]);
    }
    Ok(CandidateBundleFileDigest {
        path: relative,
        bytes,
        digest: format!("blake3:{}", hasher.finalize().to_hex()),
    })
}

fn bundle_content_hash(files: &[CandidateBundleFileDigest]) -> String {
    let mut hasher = blake3::Hasher::new();
    for file in files {
        hasher.update(file.path.as_bytes());
        hasher.update(&[0]);
        hasher.update(file.digest.as_bytes());
        hasher.update(&[0]);
        hasher.update(file.bytes.to_string().as_bytes());
        hasher.update(&[0]);
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

fn read_json_bundle_artifact<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<T, RegistryError> {
    let content = fs::read_to_string(path).map_err(|source| RegistryError::BundleArtifactRead {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| RegistryError::BundleArtifactParse {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn bundle_artifact_path(bundle_root: &Path, path: &str) -> PathBuf {
    let artifact_path = PathBuf::from(path);
    if artifact_path.is_absolute() {
        artifact_path
    } else {
        bundle_root.join(artifact_path)
    }
}

fn bundle_relative_path(bundle_root: &Path, path: &Path) -> String {
    path.strip_prefix(bundle_root)
        .unwrap_or(path)
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn is_bundle_integrity_control_file(path: &str) -> bool {
    matches!(
        path,
        "manifest.json"
            | "job.json"
            | "record.json"
            | "bundle-content-manifest.json"
            | "bundle-attestation.json"
            | "candidate-bundle-registry-record.json"
            | "candidate-summary.md"
            | "approval-decision.json"
    )
}

fn artifact_refs_exist(bundle_root: &Path, refs: &[String]) -> bool {
    refs.iter().all(|artifact_ref| {
        let path = bundle_artifact_path(bundle_root, artifact_ref);
        path.exists()
    })
}

fn candidate_bundle_evidence_refs(manifest: &CandidateBundleManifest) -> Vec<String> {
    let mut refs = vec![manifest.mutation_packet_path.clone()];
    refs.extend(manifest.job_path.iter().cloned());
    refs.extend(manifest.model_output_path.iter().cloned());
    refs.extend(manifest.agent_run_path.iter().cloned());
    refs.extend(manifest.agent_summary_path.iter().cloned());
    refs.extend(manifest.benchmark_results_path.iter().cloned());
    refs.extend(manifest.resource_usage_path.iter().cloned());
    refs.extend(manifest.content_manifest_path.iter().cloned());
    refs.extend(manifest.attestation_path.iter().cloned());
    refs.extend(manifest.registry_record_path.iter().cloned());
    refs.extend(manifest.changed_files_path.iter().cloned());
    refs.extend(manifest.diff_path.iter().cloned());
    refs.extend(manifest.scorecard_path.iter().cloned());
    refs.extend(manifest.candidate_summary_path.iter().cloned());
    refs.extend(manifest.decision_path.iter().cloned());
    refs.extend(manifest.command_logs.iter().map(|log| log.log_path.clone()));
    let rollout_material_path = candidate_bundle_verified_rollout_material_path(manifest);
    if rollout_material_path.exists() {
        refs.push(rollout_material_path.display().to_string());
    }
    refs
}

fn integrity_check(
    name: impl Into<String>,
    passed: bool,
    detail: impl Into<String>,
) -> CandidateBundleIntegrityCheck {
    CandidateBundleIntegrityCheck {
        name: name.into(),
        passed,
        detail: detail.into(),
    }
}

fn signer_authorization_check(
    name: impl Into<String>,
    passed: bool,
    detail: impl Into<String>,
) -> ArtifactSignerAuthorizationCheck {
    ArtifactSignerAuthorizationCheck {
        name: name.into(),
        passed,
        detail: detail.into(),
    }
}

fn verified_rollout_material_check(
    name: impl Into<String>,
    passed: bool,
    detail: impl Into<String>,
) -> VerifiedCandidateRolloutMaterialValidationCheck {
    VerifiedCandidateRolloutMaterialValidationCheck {
        name: name.into(),
        passed,
        detail: detail.into(),
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum RegistryError {
    #[error("duplicate artifact: {artifact_id}")]
    DuplicateArtifact { artifact_id: ArtifactId },
    #[error("duplicate candidate artifact: {artifact_id}")]
    DuplicateCandidate { artifact_id: ArtifactId },
    #[error("duplicate candidate bundle: {job_id}")]
    DuplicateCandidateBundle { job_id: Id },
    #[error("duplicate trusted artifact signer: {public_key}")]
    DuplicateTrustedSigner { public_key: String },
    #[error("candidate bundle integrity failed for {job_id}: {failed_checks:?}")]
    CandidateBundleIntegrityFailed {
        job_id: Id,
        failed_checks: Vec<String>,
    },
    #[error("candidate bundle artifact `{path}` could not be read: {detail}")]
    BundleArtifactRead { path: String, detail: String },
    #[error("candidate bundle artifact `{path}` could not be parsed: {detail}")]
    BundleArtifactParse { path: String, detail: String },
    #[error("trusted signer store `{path}` could not be read: {detail}")]
    TrustedSignerStoreRead { path: String, detail: String },
    #[error("trusted signer store `{path}` could not be written: {detail}")]
    TrustedSignerStoreWrite { path: String, detail: String },
    #[error("trusted signer store `{path}` could not be parsed: {detail}")]
    TrustedSignerStoreParse { path: String, detail: String },
    #[error("candidate bundle registry store `{path}` could not be read: {detail}")]
    CandidateBundleRecordStoreRead { path: String, detail: String },
    #[error("candidate bundle registry store `{path}` could not be written: {detail}")]
    CandidateBundleRecordStoreWrite { path: String, detail: String },
    #[error("candidate bundle registry store `{path}` could not be parsed: {detail}")]
    CandidateBundleRecordStoreParse { path: String, detail: String },
    #[error("verified rollout material store `{path}` could not be read: {detail}")]
    VerifiedRolloutMaterialStoreRead { path: String, detail: String },
    #[error("verified rollout material store `{path}` could not be written: {detail}")]
    VerifiedRolloutMaterialStoreWrite { path: String, detail: String },
    #[error("verified rollout material store `{path}` could not be parsed: {detail}")]
    VerifiedRolloutMaterialStoreParse { path: String, detail: String },
    #[error("verified rollout material for {job_id} failed validation: {failed_checks:?}")]
    VerifiedRolloutMaterialInvalid {
        job_id: Id,
        failed_checks: Vec<String>,
    },
    #[error("trusted signer `{public_key}` could not be encoded for cluster KV: {detail}")]
    TrustedSignerKvEncode { public_key: String, detail: String },
    #[error("trusted signer cluster KV entry `{key}` could not be decoded: {detail}")]
    TrustedSignerKvDecode { key: String, detail: String },
    #[error(
        "trusted signer cluster KV entry `{key}` does not match signer `{public_key}`; expected `{expected_key}`"
    )]
    TrustedSignerKvKeyMismatch {
        key: String,
        expected_key: String,
        public_key: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        ActorKind, ActorRef, ArtifactManifest, ArtifactProvenance, CandidateBundleAttestation,
        CandidateBundleContentManifest, CandidateBundleManifest, CandidateBundlePredicate,
        CandidateBundleSubject, CapabilityDiff, EvaluationOutcome, Id, ModuleManifest,
        PolicyAction, PolicyDecision, TrustRing,
    };
    use std::path::{Path, PathBuf};

    type PolicyDecisionId = Id;
    type EvolutionJobId = Id;

    #[test]
    fn registry_tracks_latest_approved_artifact_for_module() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let artifact = ArtifactManifest {
            artifact_id: artifact_id.clone(),
            module_id: module_id.clone(),
            target_ring: TrustRing::R3SandboxedModule,
            content_hash: "sha256:abc".to_string(),
            signature: None,
            source_workspace: None,
            source_revision: None,
            module_manifest: ModuleManifest::r3_wasm(module_id.clone(), "0.1.0", "trigger"),
            provenance: ArtifactProvenance {
                produced_by: ActorRef {
                    kind: ActorKind::EvolutionOrchestrator,
                    id: "phase2-test".to_string(),
                },
                source_event_id: None,
                build_log_artifact_id: None,
                attestations: Vec::new(),
            },
        };

        let mut registry = ArtifactRegistry::new();
        registry
            .register_candidate(CandidateRecord {
                artifact: artifact.clone(),
                source_job_id: Some(
                    EvolutionJobId::from_legacy_named("evojob_candidate").expect("job"),
                ),
                scorecard: None,
                evidence_refs: vec!["sandbox://run/local".to_string()],
                registered_at: OffsetDateTime::now_utc(),
            })
            .expect("candidate");
        registry
            .register(RegistryEntry {
                artifact,
                policy_decision: PolicyDecision {
                    decision_id: PolicyDecisionId::from_legacy_named("policy_ok").expect("policy"),
                    target_ring: TrustRing::R3SandboxedModule,
                    target_artifact_id: artifact_id.clone(),
                    action: PolicyAction::Allow,
                    capability_diff: CapabilityDiff::default(),
                    required_additional_evals: Vec::new(),
                    max_rollout_weight_bps: Some(500),
                    reason: "ok".to_string(),
                },
                rollout_plan: None,
                status: RegistryStatus::Approved,
                registered_at: OffsetDateTime::now_utc(),
            })
            .expect("register");

        assert!(registry.latest_for_module(&module_id).is_some());
        assert!(registry.candidate(&artifact_id).is_some());
    }

    #[test]
    fn registry_verifies_and_registers_candidate_bundle_provenance() {
        let root = unique_test_root("verified-bundle");
        let mut manifest = write_candidate_bundle_fixture(&root);
        fs::write(root.join("candidate-summary.md"), "# Candidate Summary\n")
            .expect("write candidate summary");
        fs::write(
            root.join("approval-decision.json"),
            "{\"outcome\":\"approved\"}\n",
        )
        .expect("write approval decision");
        fs::write(
            root.join("candidate-bundle-registry-record.json"),
            "{\"placeholder\":true}\n",
        )
        .expect("write registry record");
        fs::write(root.join("job.json"), "{\"status\":\"approved\"}\n").expect("write job");
        fs::write(root.join("record.json"), "{\"decision\":\"approved\"}\n")
            .expect("write stored record");
        manifest.registry_record_path = Some(
            root.join("candidate-bundle-registry-record.json")
                .display()
                .to_string(),
        );

        let report = verify_candidate_bundle_integrity(&manifest).expect("verify bundle");
        assert!(report.is_valid(), "{:?}", report.failed_checks());
        assert_eq!(
            report.recomputed_content_hash.as_deref(),
            manifest.bundle_content_hash.as_deref()
        );

        let mut registry = ArtifactRegistry::new();
        let record = registry
            .register_verified_candidate_bundle(&manifest)
            .expect("register verified bundle");
        assert_eq!(record.job_id, manifest.job_id);
        assert_eq!(
            record.bundle_content_hash,
            manifest.bundle_content_hash.clone().expect("content hash")
        );
        assert!(
            record
                .evidence_refs
                .iter()
                .any(|artifact| artifact.ends_with("mutation-packet.json"))
        );
        assert!(registry.candidate_bundle(&manifest.job_id).is_some());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn candidate_bundle_registry_records_round_trip_through_data_dir_store() {
        let root = unique_test_root("candidate-bundle-record-store");
        let bundle_root = root.join("bundle");
        let manifest = write_candidate_bundle_fixture(&bundle_root);
        let mut registry = ArtifactRegistry::new();
        let record = registry
            .register_verified_candidate_bundle(&manifest)
            .expect("register verified bundle")
            .clone();

        assert!(
            read_candidate_bundle_registry_record(&root, &record.job_id)
                .expect("missing registry record")
                .is_none()
        );
        let path =
            write_candidate_bundle_registry_record(&root, &record).expect("write registry record");
        assert_eq!(
            path,
            candidate_bundle_registry_record_path(&root, &record.job_id)
        );
        assert!(path.starts_with(root.join(CANDIDATE_BUNDLE_REGISTRY_RECORDS_RELATIVE_DIR)));

        let loaded = read_candidate_bundle_registry_record(&root, &record.job_id)
            .expect("read registry record")
            .expect("registry record");
        assert_eq!(loaded, record);

        let records = read_candidate_bundle_registry_records(&root).expect("list records");
        assert_eq!(records, vec![record.clone()]);

        let mut loaded_registry = ArtifactRegistry::new();
        let loaded_count = loaded_registry
            .load_candidate_bundle_records_from_data_dir(&root)
            .expect("load registry records");
        assert_eq!(loaded_count, 1);
        assert!(
            loaded_registry
                .candidate_bundle(&record.job_id)
                .is_some_and(|loaded| loaded.bundle_content_hash == record.bundle_content_hash)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn verified_rollout_material_records_round_trip_through_data_dir_store() {
        let root = unique_test_root("registry-rollout-material-store");
        let (artifact, _signer) = signed_artifact_fixture("rollout-material-signer");
        let job_id = Id::from_legacy_named("evojob_rollout_material").expect("job");
        let active_artifact_id = Id::from_legacy_named("artifact_active").expect("active");
        let record = VerifiedCandidateRolloutMaterialRecord {
            candidate_bundle_job_id: job_id.clone(),
            artifact: artifact.clone(),
            policy_decision: PolicyDecision {
                decision_id: PolicyDecisionId::from_legacy_named("policy_material")
                    .expect("policy"),
                target_ring: artifact.target_ring.clone(),
                target_artifact_id: artifact.artifact_id.clone(),
                action: PolicyAction::Allow,
                capability_diff: CapabilityDiff::default(),
                required_additional_evals: Vec::new(),
                max_rollout_weight_bps: Some(500),
                reason: "material verified".to_string(),
            },
            rollout_plan: RolloutPlan {
                rollout_plan_id: Id::from_legacy_named("rollout_material").expect("rollout"),
                artifact_id: artifact.artifact_id.clone(),
                module_id: artifact.module_id.clone(),
                target_ring: artifact.target_ring.clone(),
                strategy: sinomo_core::RolloutStrategy::Shadow,
                target_nodes: Vec::new(),
                initial_weight_bps: 0,
                promotion_steps_bps: vec![500],
                rollback_policy: sinomo_core::RollbackPolicy::default(),
            },
            active_artifact_id,
            evidence_refs: vec!["registry://candidate-bundle/evojob_rollout_material".to_string()],
            registered_at: OffsetDateTime::now_utc(),
        };

        let path = write_verified_candidate_rollout_material(&root, &record)
            .expect("write rollout material");
        let loaded = read_verified_candidate_rollout_material(&root, &job_id)
            .expect("read rollout material")
            .expect("rollout material");

        assert_eq!(
            path,
            verified_candidate_rollout_material_path(&root, &job_id)
        );
        assert_eq!(loaded, record);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn verified_rollout_material_validation_requires_bundle_match_and_trusted_signature() {
        let root = unique_test_root("registry-rollout-material-validation");
        let bundle_root = root.join("bundle");
        let manifest = write_candidate_bundle_fixture(&bundle_root);
        let mut registry = ArtifactRegistry::new();
        let bundle_record = registry
            .register_verified_candidate_bundle(&manifest)
            .expect("register bundle")
            .clone();
        let (artifact, signer) = signed_artifact_fixture("rollout-material-validation-signer");
        let active_artifact_id = Id::from_legacy_named("artifact_active").expect("active");
        let material = VerifiedCandidateRolloutMaterialRecord {
            candidate_bundle_job_id: bundle_record.job_id.clone(),
            artifact: artifact.clone(),
            policy_decision: PolicyDecision {
                decision_id: PolicyDecisionId::from_legacy_named("policy_material_validation")
                    .expect("policy"),
                target_ring: artifact.target_ring.clone(),
                target_artifact_id: artifact.artifact_id.clone(),
                action: PolicyAction::Allow,
                capability_diff: CapabilityDiff::default(),
                required_additional_evals: Vec::new(),
                max_rollout_weight_bps: Some(500),
                reason: "material verified".to_string(),
            },
            rollout_plan: RolloutPlan {
                rollout_plan_id: Id::from_legacy_named("rollout_material_validation")
                    .expect("rollout"),
                artifact_id: artifact.artifact_id.clone(),
                module_id: artifact.module_id.clone(),
                target_ring: artifact.target_ring.clone(),
                strategy: sinomo_core::RolloutStrategy::Shadow,
                target_nodes: Vec::new(),
                initial_weight_bps: 0,
                promotion_steps_bps: vec![500],
                rollback_policy: sinomo_core::RollbackPolicy::default(),
            },
            active_artifact_id,
            evidence_refs: bundle_record.evidence_refs.clone(),
            registered_at: OffsetDateTime::now_utc(),
        };

        let report = validate_verified_candidate_rollout_material(
            &material,
            &bundle_record,
            std::slice::from_ref(&signer),
        );
        assert!(report.is_valid(), "{:?}", report.failed_checks());
        let (path, write_report) = verify_and_write_verified_candidate_rollout_material(
            &root,
            &material,
            &bundle_record,
            &[signer],
        )
        .expect("write validated material");
        assert!(path.exists());
        assert!(write_report.is_valid());

        let unsigned_report =
            validate_verified_candidate_rollout_material(&material, &bundle_record, &[]);
        assert!(!unsigned_report.is_valid());
        assert!(
            unsigned_report
                .failed_checks()
                .contains(&"artifact-signature-authorized".to_string())
        );
        let mut wrong_job = material.clone();
        wrong_job.candidate_bundle_job_id =
            Id::from_legacy_named("evojob_wrong_material").expect("wrong job");
        let wrong_job_report =
            validate_verified_candidate_rollout_material(&wrong_job, &bundle_record, &[]);
        assert!(!wrong_job_report.is_valid());
        assert!(
            wrong_job_report
                .failed_checks()
                .contains(&"candidate-bundle-job-id".to_string())
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn registry_rejects_tampered_candidate_bundle() {
        let root = unique_test_root("tampered-bundle");
        let manifest = write_candidate_bundle_fixture(&root);
        fs::write(
            root.join("mutation-packet.json"),
            "{\"objective\":\"tampered\"}\n",
        )
        .expect("tamper bundle");

        let report = verify_candidate_bundle_integrity(&manifest).expect("verify bundle");
        assert!(!report.is_valid());
        assert!(
            report
                .failed_checks()
                .contains(&"content-manifest-files-match".to_string())
        );

        let mut registry = ArtifactRegistry::new();
        let error = registry
            .register_verified_candidate_bundle(&manifest)
            .expect_err("tampered bundle is rejected");
        assert!(matches!(
            error,
            RegistryError::CandidateBundleIntegrityFailed { .. }
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn registry_authorizes_artifact_signature_only_from_trusted_signer() {
        let (artifact, signer) = signed_artifact_fixture("authorized-signer");
        let unknown_report = authorize_artifact_signature(&artifact, &[]);
        assert!(!unknown_report.is_authorized());
        assert!(
            unknown_report
                .failed_checks()
                .contains(&"artifact-signer-known".to_string())
        );

        let trusted_report = authorize_artifact_signature(&artifact, std::slice::from_ref(&signer));
        assert!(trusted_report.is_authorized(), "{trusted_report:?}");
        assert_eq!(
            trusted_report.signer_id.as_deref(),
            Some("authorized-signer")
        );

        let mut registry = ArtifactRegistry::new();
        registry
            .register_trusted_artifact_signer(signer)
            .expect("register signer");
        let registry_report = registry.authorize_artifact_signer(&artifact);
        assert!(registry_report.is_authorized(), "{registry_report:?}");
    }

    #[test]
    fn registry_rejects_signer_not_authorized_for_target_ring_or_module() {
        let (artifact, mut signer) = signed_artifact_fixture("scoped-signer");
        signer.allowed_rings = vec![TrustRing::R1TrustedRuntime];
        signer.allowed_modules =
            vec![ModuleId::from_legacy_named("module.other").expect("other module")];

        let report = authorize_artifact_signature(&artifact, &[signer]);

        assert!(!report.is_authorized());
        assert!(
            report
                .failed_checks()
                .contains(&"artifact-signer-ring-authorized".to_string())
        );
        assert!(
            report
                .failed_checks()
                .contains(&"artifact-signer-module-authorized".to_string())
        );
    }

    #[test]
    fn trusted_artifact_signers_round_trip_through_data_dir_store() {
        let root = unique_test_root("trusted-signers-store");
        let (_artifact, signer) = signed_artifact_fixture("stored-signer");

        assert!(
            read_trusted_artifact_signers(&root)
                .expect("missing store")
                .is_empty()
        );
        let path = write_trusted_artifact_signers(&root, std::slice::from_ref(&signer))
            .expect("write signers");

        assert_eq!(path, root.join(TRUSTED_ARTIFACT_SIGNERS_RELATIVE_PATH));
        let signers = read_trusted_artifact_signers(&root).expect("read signers");
        assert_eq!(signers, vec![signer.clone()]);

        let mut registry = ArtifactRegistry::new();
        let loaded = registry
            .load_trusted_artifact_signers_from_data_dir(&root)
            .expect("load signers");
        assert_eq!(loaded, 1);
        assert!(registry.trusted_signer(&signer.public_key).is_some());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn trusted_artifact_signers_round_trip_through_cluster_kv_entries() {
        let (artifact, signer) = signed_artifact_fixture("cluster-signer");
        let key = cluster_trusted_artifact_signer_key(&signer.public_key);
        assert!(key.starts_with(CLUSTER_TRUSTED_ARTIFACT_SIGNER_PREFIX));
        assert!(!key.ends_with(&signer.public_key));

        let txn = trusted_artifact_signers_kv_txn(std::slice::from_ref(&signer))
            .expect("build signer txn");
        assert_eq!(txn.success.len(), 1);

        let mut store = sinomo_kv::InProcessKvStore::new();
        let response = store.txn(txn).expect("apply signer txn");
        assert!(response.succeeded);
        assert!(store.get(&key).is_some());

        let entries = store.range_prefix(CLUSTER_TRUSTED_ARTIFACT_SIGNER_PREFIX);
        let signers = trusted_artifact_signers_from_kv_entries(&entries).expect("decode signers");
        assert_eq!(signers, vec![signer.clone()]);

        let mut registry = ArtifactRegistry::new();
        let loaded = registry
            .load_trusted_artifact_signers_from_kv_entries(&entries)
            .expect("load kv signers");
        assert_eq!(loaded, 1);
        assert!(
            registry
                .authorize_artifact_signer(&artifact)
                .is_authorized()
        );
    }

    #[test]
    fn trusted_artifact_signer_cluster_kv_decode_rejects_mismatched_keys() {
        let (_artifact, signer) = signed_artifact_fixture("mismatched-cluster-signer");
        let txn = trusted_artifact_signers_kv_txn(std::slice::from_ref(&signer))
            .expect("build signer txn");

        let mut store = sinomo_kv::InProcessKvStore::new();
        store.txn(txn).expect("apply signer txn");
        let mut entries = store.range_prefix(CLUSTER_TRUSTED_ARTIFACT_SIGNER_PREFIX);
        entries[0].key = format!("{CLUSTER_TRUSTED_ARTIFACT_SIGNER_PREFIX}wrong");

        let error = trusted_artifact_signers_from_kv_entries(&entries)
            .expect_err("mismatched signer key is rejected");
        assert!(matches!(
            error,
            RegistryError::TrustedSignerKvKeyMismatch { .. }
        ));
    }

    fn unique_test_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "sinomo-registry-{name}-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ))
    }

    fn signed_artifact_fixture(signer_id: &str) -> (ArtifactManifest, TrustedArtifactSigner) {
        let root = unique_test_root(signer_id);
        let identity_path = root.join("identity.sobj");
        let identity = sinomo_node::load_or_create_node_identity(&identity_path).expect("identity");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let mut artifact = ArtifactManifest {
            artifact_id,
            module_id: module_id.clone(),
            target_ring: TrustRing::R3SandboxedModule,
            content_hash: "blake3:candidate".to_string(),
            signature: None,
            source_workspace: None,
            source_revision: None,
            module_manifest: ModuleManifest::r3_wasm(module_id, "0.1.0", "trigger"),
            provenance: ArtifactProvenance {
                produced_by: ActorRef {
                    kind: ActorKind::Registry,
                    id: "local-registry".to_string(),
                },
                source_event_id: None,
                build_log_artifact_id: None,
                attestations: Vec::new(),
            },
        };
        artifact.signature = Some(
            sinomo_module::sign_artifact_manifest_with_node_identity(&artifact, &identity)
                .expect("sign"),
        );
        let signer =
            TrustedArtifactSigner::local_ed25519(signer_id, identity.public_key, "test signer");
        let _ = fs::remove_dir_all(root);
        (artifact, signer)
    }

    fn write_candidate_bundle_fixture(root: &Path) -> CandidateBundleManifest {
        fs::create_dir_all(root).expect("create bundle root");
        let job_id = EvolutionJobId::from_legacy_named("evojob_registry_bundle").expect("job");
        let mutation_packet_path = root.join("mutation-packet.json");
        let changed_files_path = root.join("changed-files.json");
        let diff_path = root.join("diff.patch");
        let scorecard_path = root.join("scorecard.json");
        fs::write(
            &mutation_packet_path,
            "{\"objective\":\"verify registry bundle\"}\n",
        )
        .expect("write mutation packet");
        fs::write(&changed_files_path, "[\"crates/example/src/lib.rs\"]\n")
            .expect("write changed files");
        fs::write(&diff_path, "diff --git a/example b/example\n").expect("write diff");
        fs::write(&scorecard_path, "{\"outcome\":\"passed\"}\n").expect("write scorecard");

        let files = build_candidate_bundle_file_digests(root).expect("digest bundle");
        let content_hash = bundle_content_hash(&files);
        let content_manifest_path = root.join("bundle-content-manifest.json");
        let content_manifest = CandidateBundleContentManifest {
            bundle_path: root.display().to_string(),
            generated_at: "2026-06-01T00:00:00Z".to_string(),
            bundle_content_hash: content_hash.clone(),
            hash_algorithm: "blake3".to_string(),
            files,
        };
        fs::write(
            &content_manifest_path,
            serde_json::to_string_pretty(&content_manifest).expect("content manifest json"),
        )
        .expect("write content manifest");

        let attestation_path = root.join("bundle-attestation.json");
        let attestation = CandidateBundleAttestation {
            statement_type: "https://in-toto.io/Statement/v1".to_string(),
            predicate_type: "https://sinomo.net/attestations/evolution-candidate-bundle/v1"
                .to_string(),
            subject: CandidateBundleSubject {
                name: format!("candidate-bundle:{job_id}"),
                digest: content_hash.clone(),
            },
            predicate: CandidateBundlePredicate {
                job_id: job_id.clone(),
                objective: "verify registry bundle".to_string(),
                target_ring: TrustRing::R2TrustedService,
                produced_by: "sinomo-evolution".to_string(),
                mutation_worker: "agent-loop".to_string(),
                provider: Some("test-provider".to_string()),
                model: Some("test-model".to_string()),
                source_workspace: "/source".to_string(),
                candidate_worktree: root.join("worktree").display().to_string(),
                candidate_branch: "sinomo/evolution/evojob_registry_bundle".to_string(),
                source_revision: Some("git:test".to_string()),
                verification_outcome: Some(EvaluationOutcome::Passed),
                changed_files: vec!["crates/example/src/lib.rs".to_string()],
                artifact_refs: vec![mutation_packet_path.display().to_string()],
                generated_at: "2026-06-01T00:00:00Z".to_string(),
            },
        };
        fs::write(
            &attestation_path,
            serde_json::to_string_pretty(&attestation).expect("attestation json"),
        )
        .expect("write attestation");

        CandidateBundleManifest {
            job_id,
            bundle_path: root.display().to_string(),
            created_at: OffsetDateTime::now_utc(),
            mutation_packet_path: mutation_packet_path.display().to_string(),
            job_path: None,
            model_output_path: None,
            agent_run_path: None,
            agent_summary_path: None,
            benchmark_results_path: None,
            resource_usage_path: None,
            content_manifest_path: Some(content_manifest_path.display().to_string()),
            attestation_path: Some(attestation_path.display().to_string()),
            registry_record_path: None,
            bundle_content_hash: Some(content_hash),
            changed_files_path: Some(changed_files_path.display().to_string()),
            diff_path: Some(diff_path.display().to_string()),
            scorecard_path: Some(scorecard_path.display().to_string()),
            candidate_summary_path: None,
            decision_path: None,
            command_logs: Vec::new(),
        }
    }
}
