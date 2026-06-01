//! Artifact registry metadata skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    ArtifactManifest, EvaluationScorecard, Id, PolicyAction, PolicyDecision, RolloutPlan, TrustRing,
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
pub const ARTIFACT_EVIDENCE_RECORDS_RELATIVE_DIR: &str = "registry/artifact-evidence";
pub const VERIFIED_RELEASE_MATERIAL_RELATIVE_DIR: &str = "registry/verified-release-material";
pub const RELEASE_MATERIAL_RELATIVE_PATH: &str = "release-material.json";
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
pub struct ArtifactEvidenceManifest {
    pub evidence_id: Id,
    pub evidence_kind: String,
    pub evidence_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_manifest_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attestation_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ArtifactEvidenceRecord {
    pub evidence_id: Id,
    pub evidence_kind: String,
    pub evidence_path: String,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_manifest_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attestation_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    pub verification: ArtifactEvidenceIntegrityReport,
    #[serde(with = "time::serde::rfc3339")]
    pub registered_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ArtifactEvidenceIntegrityReport {
    pub evidence_id: Id,
    pub evidence_kind: String,
    pub evidence_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recomputed_content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_manifest_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attestation_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<ArtifactEvidenceIntegrityCheck>,
    #[serde(with = "time::serde::rfc3339")]
    pub verified_at: OffsetDateTime,
}

impl ArtifactEvidenceIntegrityReport {
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
pub struct ArtifactEvidenceIntegrityCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ArtifactEvidenceContentManifest {
    #[serde(rename = "evidence_path", alias = "bundle_path")]
    pub evidence_path: String,
    pub generated_at: String,
    #[serde(rename = "content_hash", alias = "bundle_content_hash")]
    pub content_hash: String,
    pub hash_algorithm: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<ArtifactEvidenceFileDigest>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ArtifactEvidenceFileDigest {
    pub path: String,
    pub bytes: u64,
    pub digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ArtifactEvidenceAttestation {
    pub statement_type: String,
    pub predicate_type: String,
    pub subject: ArtifactEvidenceSubject,
    #[serde(default)]
    pub predicate: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ArtifactEvidenceSubject {
    pub name: String,
    pub digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VerifiedReleaseMaterialRecord {
    pub release_id: Id,
    pub release_material_hash: String,
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
pub struct VerifiedReleaseMaterialValidationReport {
    pub release_id: Id,
    pub artifact_id: ArtifactId,
    pub module_id: ModuleId,
    pub rollout_plan_id: Id,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<VerifiedReleaseMaterialValidationCheck>,
    #[serde(with = "time::serde::rfc3339")]
    pub verified_at: OffsetDateTime,
}

impl VerifiedReleaseMaterialValidationReport {
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
pub struct VerifiedReleaseMaterialValidationCheck {
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
    artifact_evidence_records: BTreeMap<Id, ArtifactEvidenceRecord>,
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

    pub fn register_verified_artifact_evidence(
        &mut self,
        manifest: &ArtifactEvidenceManifest,
    ) -> Result<&ArtifactEvidenceRecord, RegistryError> {
        if self
            .artifact_evidence_records
            .contains_key(&manifest.evidence_id)
        {
            return Err(RegistryError::DuplicateArtifactEvidence {
                evidence_id: manifest.evidence_id.clone(),
            });
        }

        let verification = verify_artifact_evidence_integrity(manifest)?;
        if !verification.is_valid() {
            return Err(RegistryError::ArtifactEvidenceIntegrityFailed {
                evidence_id: manifest.evidence_id.clone(),
                failed_checks: verification.failed_checks(),
            });
        }

        let record = ArtifactEvidenceRecord {
            evidence_id: manifest.evidence_id.clone(),
            evidence_kind: manifest.evidence_kind.clone(),
            evidence_path: manifest.evidence_path.clone(),
            content_hash: verification
                .recomputed_content_hash
                .clone()
                .unwrap_or_else(|| manifest.declared_content_hash.clone().unwrap_or_default()),
            content_manifest_path: manifest.content_manifest_path.clone(),
            attestation_path: manifest.attestation_path.clone(),
            evidence_refs: manifest.evidence_refs.clone(),
            verification,
            registered_at: OffsetDateTime::now_utc(),
        };
        let evidence_id = record.evidence_id.clone();
        self.artifact_evidence_records
            .insert(evidence_id.clone(), record);
        Ok(self
            .artifact_evidence_records
            .get(&evidence_id)
            .expect("artifact evidence record inserted"))
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

    pub fn load_artifact_evidence_records_from_data_dir(
        &mut self,
        data_dir_root: impl AsRef<Path>,
    ) -> Result<usize, RegistryError> {
        let records = read_artifact_evidence_records(data_dir_root)?;
        let count = records.len();
        self.artifact_evidence_records.clear();
        for record in records {
            self.artifact_evidence_records
                .insert(record.evidence_id.clone(), record);
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

    pub fn artifact_evidence(&self, evidence_id: &Id) -> Option<&ArtifactEvidenceRecord> {
        self.artifact_evidence_records.get(evidence_id)
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

pub fn verify_artifact_evidence_integrity(
    manifest: &ArtifactEvidenceManifest,
) -> Result<ArtifactEvidenceIntegrityReport, RegistryError> {
    let evidence_root = PathBuf::from(&manifest.evidence_path);
    let mut report = ArtifactEvidenceIntegrityReport {
        evidence_id: manifest.evidence_id.clone(),
        evidence_kind: manifest.evidence_kind.clone(),
        evidence_path: manifest.evidence_path.clone(),
        declared_content_hash: manifest.declared_content_hash.clone(),
        recomputed_content_hash: None,
        content_manifest_path: manifest.content_manifest_path.clone(),
        attestation_path: manifest.attestation_path.clone(),
        checks: Vec::new(),
        verified_at: OffsetDateTime::now_utc(),
    };

    let Some(content_manifest_path) = &manifest.content_manifest_path else {
        report.checks.push(artifact_evidence_integrity_check(
            "content-manifest-path-present",
            false,
            "artifact evidence manifest does not reference a content manifest",
        ));
        return Ok(report);
    };
    report.checks.push(artifact_evidence_integrity_check(
        "content-manifest-path-present",
        true,
        "artifact evidence manifest references a content manifest",
    ));

    let content_manifest_path = evidence_artifact_path(&evidence_root, content_manifest_path);
    let content_manifest: ArtifactEvidenceContentManifest =
        read_json_evidence_artifact(&content_manifest_path)?;
    report.checks.push(artifact_evidence_integrity_check(
        "content-manifest-hash-algorithm",
        content_manifest.hash_algorithm == "blake3",
        format!(
            "declared hash algorithm is `{}`",
            content_manifest.hash_algorithm
        ),
    ));
    report.checks.push(artifact_evidence_integrity_check(
        "content-manifest-evidence-path",
        content_manifest.evidence_path == manifest.evidence_path,
        "content manifest evidence path matches artifact evidence manifest",
    ));

    let recomputed_files = build_artifact_evidence_file_digests(&evidence_root)?;
    let recomputed_content_hash = artifact_evidence_content_hash(&recomputed_files);
    report.recomputed_content_hash = Some(recomputed_content_hash.clone());
    report.checks.push(artifact_evidence_integrity_check(
        "content-manifest-files-match",
        content_manifest.files == recomputed_files,
        "content manifest covers every non-control file with matching digest and byte count",
    ));
    report.checks.push(artifact_evidence_integrity_check(
        "content-manifest-hash-matches-recomputed",
        content_manifest.content_hash == recomputed_content_hash,
        "content manifest aggregate hash matches recomputed evidence hash",
    ));
    report.checks.push(artifact_evidence_integrity_check(
        "evidence-manifest-hash-matches-recomputed",
        manifest.declared_content_hash.as_deref() == Some(recomputed_content_hash.as_str()),
        "artifact evidence manifest aggregate hash matches recomputed evidence hash",
    ));

    let Some(attestation_path) = &manifest.attestation_path else {
        report.checks.push(artifact_evidence_integrity_check(
            "attestation-path-optional",
            true,
            "artifact evidence manifest does not require an attestation",
        ));
        return Ok(report);
    };
    report.checks.push(artifact_evidence_integrity_check(
        "attestation-path-present",
        true,
        "artifact evidence manifest references an attestation",
    ));

    let attestation_path = evidence_artifact_path(&evidence_root, attestation_path);
    let attestation: ArtifactEvidenceAttestation = read_json_evidence_artifact(&attestation_path)?;
    report.checks.push(artifact_evidence_integrity_check(
        "attestation-statement-type",
        attestation.statement_type == "https://in-toto.io/Statement/v1",
        format!("statement type is `{}`", attestation.statement_type),
    ));
    report.checks.push(artifact_evidence_integrity_check(
        "attestation-subject-hash",
        attestation.subject.digest == recomputed_content_hash,
        "attestation subject digest matches recomputed evidence hash",
    ));
    let artifact_refs = attestation
        .predicate
        .get("artifact_refs")
        .and_then(|value| value.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(ToString::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    report.checks.push(artifact_evidence_integrity_check(
        "attestation-artifact-refs-exist",
        artifact_evidence_refs_exist(&evidence_root, &artifact_refs),
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

pub fn artifact_evidence_records_dir(data_dir_root: impl AsRef<Path>) -> PathBuf {
    data_dir_root
        .as_ref()
        .join(ARTIFACT_EVIDENCE_RECORDS_RELATIVE_DIR)
}

pub fn artifact_evidence_record_path(data_dir_root: impl AsRef<Path>, evidence_id: &Id) -> PathBuf {
    artifact_evidence_records_dir(data_dir_root).join(format!("{}.json", evidence_id.as_str()))
}

pub fn read_artifact_evidence_record(
    data_dir_root: impl AsRef<Path>,
    evidence_id: &Id,
) -> Result<Option<ArtifactEvidenceRecord>, RegistryError> {
    let path = artifact_evidence_record_path(data_dir_root, evidence_id);
    match fs::read_to_string(&path) {
        Ok(content) => serde_json::from_str(&content).map(Some).map_err(|source| {
            RegistryError::ArtifactEvidenceRecordStoreParse {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(RegistryError::ArtifactEvidenceRecordStoreRead {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

pub fn read_artifact_evidence_records(
    data_dir_root: impl AsRef<Path>,
) -> Result<Vec<ArtifactEvidenceRecord>, RegistryError> {
    let dir = artifact_evidence_records_dir(data_dir_root);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(RegistryError::ArtifactEvidenceRecordStoreRead {
                path: dir.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| RegistryError::ArtifactEvidenceRecordStoreRead {
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
            RegistryError::ArtifactEvidenceRecordStoreRead {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        })?;
        records.push(serde_json::from_str(&content).map_err(|source| {
            RegistryError::ArtifactEvidenceRecordStoreParse {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        })?);
    }
    Ok(records)
}

pub fn write_artifact_evidence_record(
    data_dir_root: impl AsRef<Path>,
    record: &ArtifactEvidenceRecord,
) -> Result<PathBuf, RegistryError> {
    let path = artifact_evidence_record_path(data_dir_root, &record.evidence_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| {
            RegistryError::ArtifactEvidenceRecordStoreWrite {
                path: parent.display().to_string(),
                detail: source.to_string(),
            }
        })?;
    }
    let content = serde_json::to_string_pretty(record).map_err(|source| {
        RegistryError::ArtifactEvidenceRecordStoreParse {
            path: path.display().to_string(),
            detail: source.to_string(),
        }
    })?;
    fs::write(&path, content).map_err(|source| {
        RegistryError::ArtifactEvidenceRecordStoreWrite {
            path: path.display().to_string(),
            detail: source.to_string(),
        }
    })?;
    Ok(path)
}

pub fn verified_release_material_dir(data_dir_root: impl AsRef<Path>) -> PathBuf {
    data_dir_root
        .as_ref()
        .join(VERIFIED_RELEASE_MATERIAL_RELATIVE_DIR)
}

pub fn verified_release_material_path(data_dir_root: impl AsRef<Path>, release_id: &Id) -> PathBuf {
    verified_release_material_dir(data_dir_root).join(format!("{}.json", release_id.as_str()))
}

pub fn release_material_path(evidence_root: impl AsRef<Path>) -> PathBuf {
    evidence_artifact_path(evidence_root.as_ref(), RELEASE_MATERIAL_RELATIVE_PATH)
}

pub fn read_release_material_from_path(
    path: impl AsRef<Path>,
) -> Result<Option<VerifiedReleaseMaterialRecord>, RegistryError> {
    let path = path.as_ref();
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

pub fn read_verified_release_material(
    data_dir_root: impl AsRef<Path>,
    release_id: &Id,
) -> Result<Option<VerifiedReleaseMaterialRecord>, RegistryError> {
    let path = verified_release_material_path(data_dir_root, release_id);
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

pub fn write_verified_release_material(
    data_dir_root: impl AsRef<Path>,
    record: &VerifiedReleaseMaterialRecord,
) -> Result<PathBuf, RegistryError> {
    let path = verified_release_material_path(data_dir_root, &record.release_id);
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

pub fn validate_verified_release_material(
    material: &VerifiedReleaseMaterialRecord,
    evidence: &ArtifactEvidenceRecord,
    trusted_signers: &[TrustedArtifactSigner],
) -> VerifiedReleaseMaterialValidationReport {
    let mut checks = Vec::new();
    checks.push(verified_rollout_material_check(
        "release-id",
        material.release_id == evidence.evidence_id,
        format!(
            "material references {}, evidence record is {}",
            material.release_id, evidence.evidence_id
        ),
    ));
    checks.push(verified_rollout_material_check(
        "artifact-evidence-integrity",
        evidence.verification.is_valid(),
        if evidence.verification.is_valid() {
            format!(
                "artifact evidence integrity is valid with hash {}",
                evidence.content_hash
            )
        } else {
            format!(
                "artifact evidence integrity failed: {}",
                evidence.verification.failed_checks().join(", ")
            )
        },
    ));
    checks.push(verified_rollout_material_check(
        "release-material-hash",
        material.release_material_hash == evidence.content_hash,
        format!(
            "material hash {} evidence hash {}",
            material.release_material_hash, evidence.content_hash
        ),
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

    VerifiedReleaseMaterialValidationReport {
        release_id: material.release_id.clone(),
        artifact_id: material.artifact.artifact_id.clone(),
        module_id: material.artifact.module_id.clone(),
        rollout_plan_id: material.rollout_plan.rollout_plan_id.clone(),
        checks,
        verified_at: OffsetDateTime::now_utc(),
    }
}

pub fn verify_and_write_verified_release_material(
    data_dir_root: impl AsRef<Path>,
    record: &VerifiedReleaseMaterialRecord,
    evidence: &ArtifactEvidenceRecord,
    trusted_signers: &[TrustedArtifactSigner],
) -> Result<(PathBuf, VerifiedReleaseMaterialValidationReport), RegistryError> {
    let report = validate_verified_release_material(record, evidence, trusted_signers);
    if !report.is_valid() {
        return Err(RegistryError::VerifiedRolloutMaterialInvalid {
            release_id: record.release_id.clone(),
            failed_checks: report.failed_checks(),
        });
    }
    let path = write_verified_release_material(data_dir_root, record)?;
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
        ARTIFACT_EVIDENCE_RECORDS_RELATIVE_DIR,
        "verified artifact evidence records use dynamic evidence id files",
    )?;
    layout.register_escape_path(
        "sinomo-registry",
        StorageRing::R1,
        VERIFIED_RELEASE_MATERIAL_RELATIVE_DIR,
        "verified release material records use dynamic release id files",
    )
}

fn build_artifact_evidence_file_digests(
    evidence_root: &Path,
) -> Result<Vec<ArtifactEvidenceFileDigest>, RegistryError> {
    let mut files = Vec::new();
    collect_artifact_evidence_file_digests(evidence_root, evidence_root, &mut files)?;
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn collect_artifact_evidence_file_digests(
    evidence_root: &Path,
    path: &Path,
    files: &mut Vec<ArtifactEvidenceFileDigest>,
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
            collect_artifact_evidence_file_digests(evidence_root, &entry_path, files)?;
        } else if entry_path.is_file() {
            let relative = evidence_relative_path(evidence_root, &entry_path);
            if is_artifact_evidence_control_file(&relative) {
                continue;
            }
            files.push(artifact_evidence_file_digest(&entry_path, relative)?);
        }
    }
    Ok(())
}

fn artifact_evidence_file_digest(
    path: &Path,
    relative: String,
) -> Result<ArtifactEvidenceFileDigest, RegistryError> {
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
    Ok(ArtifactEvidenceFileDigest {
        path: relative,
        bytes,
        digest: format!("blake3:{}", hasher.finalize().to_hex()),
    })
}

fn artifact_evidence_content_hash(files: &[ArtifactEvidenceFileDigest]) -> String {
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

fn read_json_evidence_artifact<T: for<'de> Deserialize<'de>>(
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

fn evidence_artifact_path(evidence_root: &Path, path: &str) -> PathBuf {
    let artifact_path = PathBuf::from(path);
    if artifact_path.is_absolute() {
        artifact_path
    } else {
        evidence_root.join(artifact_path)
    }
}

fn evidence_relative_path(evidence_root: &Path, path: &Path) -> String {
    path.strip_prefix(evidence_root)
        .unwrap_or(path)
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn is_artifact_evidence_control_file(path: &str) -> bool {
    matches!(
        path,
        "manifest.json"
            | "job.json"
            | "record.json"
            | "content-manifest.json"
            | "attestation.json"
            | "bundle-content-manifest.json"
            | "bundle-attestation.json"
            | "artifact-evidence-record.json"
            | "candidate-summary.md"
            | "approval-decision.json"
            | RELEASE_MATERIAL_RELATIVE_PATH
    )
}

fn artifact_evidence_refs_exist(evidence_root: &Path, refs: &[String]) -> bool {
    refs.iter().all(|artifact_ref| {
        let path = evidence_artifact_path(evidence_root, artifact_ref);
        path.exists()
    })
}

fn artifact_evidence_integrity_check(
    name: impl Into<String>,
    passed: bool,
    detail: impl Into<String>,
) -> ArtifactEvidenceIntegrityCheck {
    ArtifactEvidenceIntegrityCheck {
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
) -> VerifiedReleaseMaterialValidationCheck {
    VerifiedReleaseMaterialValidationCheck {
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
    #[error("duplicate artifact evidence: {evidence_id}")]
    DuplicateArtifactEvidence { evidence_id: Id },
    #[error("duplicate trusted artifact signer: {public_key}")]
    DuplicateTrustedSigner { public_key: String },
    #[error("artifact evidence integrity failed for {evidence_id}: {failed_checks:?}")]
    ArtifactEvidenceIntegrityFailed {
        evidence_id: Id,
        failed_checks: Vec<String>,
    },
    #[error("artifact evidence `{path}` could not be read: {detail}")]
    BundleArtifactRead { path: String, detail: String },
    #[error("artifact evidence `{path}` could not be parsed: {detail}")]
    BundleArtifactParse { path: String, detail: String },
    #[error("trusted signer store `{path}` could not be read: {detail}")]
    TrustedSignerStoreRead { path: String, detail: String },
    #[error("trusted signer store `{path}` could not be written: {detail}")]
    TrustedSignerStoreWrite { path: String, detail: String },
    #[error("trusted signer store `{path}` could not be parsed: {detail}")]
    TrustedSignerStoreParse { path: String, detail: String },
    #[error("artifact evidence record store `{path}` could not be read: {detail}")]
    ArtifactEvidenceRecordStoreRead { path: String, detail: String },
    #[error("artifact evidence record store `{path}` could not be written: {detail}")]
    ArtifactEvidenceRecordStoreWrite { path: String, detail: String },
    #[error("artifact evidence record store `{path}` could not be parsed: {detail}")]
    ArtifactEvidenceRecordStoreParse { path: String, detail: String },
    #[error("verified rollout material store `{path}` could not be read: {detail}")]
    VerifiedRolloutMaterialStoreRead { path: String, detail: String },
    #[error("verified rollout material store `{path}` could not be written: {detail}")]
    VerifiedRolloutMaterialStoreWrite { path: String, detail: String },
    #[error("verified rollout material store `{path}` could not be parsed: {detail}")]
    VerifiedRolloutMaterialStoreParse { path: String, detail: String },
    #[error("verified release material for {release_id} failed validation: {failed_checks:?}")]
    VerifiedRolloutMaterialInvalid {
        release_id: Id,
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
        ActorKind, ActorRef, ArtifactManifest, ArtifactProvenance, CapabilityDiff, Id,
        ModuleManifest, PolicyAction, PolicyDecision, RollbackPolicy, RolloutStrategy, TrustRing,
    };
    use std::path::{Path, PathBuf};

    type PolicyDecisionId = Id;

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
                    kind: ActorKind::Operator,
                    id: "registry-test".to_string(),
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
                source_job_id: None,
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
    fn registry_verifies_and_registers_artifact_evidence() {
        let root = unique_test_root("verified-evidence");
        let manifest = write_artifact_evidence_fixture(&root);

        let report = verify_artifact_evidence_integrity(&manifest).expect("verify evidence");
        assert!(report.is_valid(), "{:?}", report.failed_checks());
        assert_eq!(
            report.recomputed_content_hash.as_deref(),
            manifest.declared_content_hash.as_deref()
        );

        let mut registry = ArtifactRegistry::new();
        let record = registry
            .register_verified_artifact_evidence(&manifest)
            .expect("register verified evidence");
        assert_eq!(record.evidence_id, manifest.evidence_id);
        assert_eq!(
            record.content_hash,
            manifest
                .declared_content_hash
                .clone()
                .expect("content hash")
        );
        assert!(registry.artifact_evidence(&manifest.evidence_id).is_some());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn artifact_evidence_records_round_trip_through_data_dir_store() {
        let root = unique_test_root("artifact-evidence-record-store");
        let evidence_root = root.join("evidence");
        let manifest = write_artifact_evidence_fixture(&evidence_root);
        let mut registry = ArtifactRegistry::new();
        let record = registry
            .register_verified_artifact_evidence(&manifest)
            .expect("register verified evidence")
            .clone();

        assert!(
            read_artifact_evidence_record(&root, &record.evidence_id)
                .expect("missing registry record")
                .is_none()
        );
        let path = write_artifact_evidence_record(&root, &record).expect("write registry record");
        assert_eq!(
            path,
            artifact_evidence_record_path(&root, &record.evidence_id)
        );
        assert!(path.starts_with(root.join(ARTIFACT_EVIDENCE_RECORDS_RELATIVE_DIR)));

        let loaded = read_artifact_evidence_record(&root, &record.evidence_id)
            .expect("read registry record")
            .expect("registry record");
        assert_eq!(loaded, record);

        let records = read_artifact_evidence_records(&root).expect("list records");
        assert_eq!(records, vec![record.clone()]);

        let mut loaded_registry = ArtifactRegistry::new();
        let loaded_count = loaded_registry
            .load_artifact_evidence_records_from_data_dir(&root)
            .expect("load registry records");
        assert_eq!(loaded_count, 1);
        assert!(
            loaded_registry
                .artifact_evidence(&record.evidence_id)
                .is_some_and(|loaded| loaded.content_hash == record.content_hash)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn verified_rollout_material_records_round_trip_through_data_dir_store() {
        let root = unique_test_root("registry-rollout-material-store");
        let (artifact, _signer) = signed_artifact_fixture("rollout-material-signer");
        let release_id = Id::from_legacy_named("release_material").expect("release");
        let active_artifact_id = Id::from_legacy_named("artifact_active").expect("active");
        let record = VerifiedReleaseMaterialRecord {
            release_id: release_id.clone(),
            release_material_hash: "blake3:release".to_string(),
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
                rollback_policy: RollbackPolicy::default(),
            },
            active_artifact_id,
            evidence_refs: vec!["registry://artifact-evidence/release_material".to_string()],
            registered_at: OffsetDateTime::now_utc(),
        };

        let path = write_verified_release_material(&root, &record).expect("write rollout material");
        let loaded = read_verified_release_material(&root, &release_id)
            .expect("read rollout material")
            .expect("rollout material");

        assert_eq!(path, verified_release_material_path(&root, &release_id));
        assert_eq!(loaded, record);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn verified_rollout_material_validation_requires_evidence_match_and_trusted_signature() {
        let root = unique_test_root("registry-rollout-material-validation");
        let evidence_root = root.join("evidence");
        let manifest = write_artifact_evidence_fixture(&evidence_root);
        let mut registry = ArtifactRegistry::new();
        let evidence_record = registry
            .register_verified_artifact_evidence(&manifest)
            .expect("register evidence")
            .clone();
        let (artifact, signer) = signed_artifact_fixture("rollout-material-validation-signer");
        let active_artifact_id = Id::from_legacy_named("artifact_active").expect("active");
        let material = VerifiedReleaseMaterialRecord {
            release_id: evidence_record.evidence_id.clone(),
            release_material_hash: evidence_record.content_hash.clone(),
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
                strategy: RolloutStrategy::Shadow,
                target_nodes: Vec::new(),
                initial_weight_bps: 0,
                promotion_steps_bps: vec![500],
                rollback_policy: RollbackPolicy::default(),
            },
            active_artifact_id,
            evidence_refs: evidence_record.evidence_refs.clone(),
            registered_at: OffsetDateTime::now_utc(),
        };

        let report = validate_verified_release_material(
            &material,
            &evidence_record,
            std::slice::from_ref(&signer),
        );
        assert!(report.is_valid(), "{:?}", report.failed_checks());
        let (path, write_report) = verify_and_write_verified_release_material(
            &root,
            &material,
            &evidence_record,
            &[signer],
        )
        .expect("write validated material");
        assert!(path.exists());
        assert!(write_report.is_valid());

        let unsigned_report = validate_verified_release_material(&material, &evidence_record, &[]);
        assert!(!unsigned_report.is_valid());
        assert!(
            unsigned_report
                .failed_checks()
                .contains(&"artifact-signature-authorized".to_string())
        );
        let mut wrong_release = material.clone();
        wrong_release.release_id = Id::from_legacy_named("release_wrong").expect("wrong release");
        let wrong_release_report =
            validate_verified_release_material(&wrong_release, &evidence_record, &[]);
        assert!(!wrong_release_report.is_valid());
        assert!(
            wrong_release_report
                .failed_checks()
                .contains(&"release-id".to_string())
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn registry_rejects_tampered_artifact_evidence() {
        let root = unique_test_root("tampered-evidence");
        let manifest = write_artifact_evidence_fixture(&root);
        fs::write(root.join("artifact.json"), "{\"objective\":\"tampered\"}\n")
            .expect("tamper evidence");

        let report = verify_artifact_evidence_integrity(&manifest).expect("verify evidence");
        assert!(!report.is_valid());
        assert!(
            report
                .failed_checks()
                .contains(&"content-manifest-files-match".to_string())
        );

        let mut registry = ArtifactRegistry::new();
        let error = registry
            .register_verified_artifact_evidence(&manifest)
            .expect_err("tampered evidence is rejected");
        assert!(matches!(
            error,
            RegistryError::ArtifactEvidenceIntegrityFailed { .. }
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

    fn write_artifact_evidence_fixture(root: &Path) -> ArtifactEvidenceManifest {
        fs::create_dir_all(root).expect("create evidence root");
        let evidence_id = Id::from_legacy_named("evidence_registry").expect("evidence");
        let artifact_path = root.join("artifact.json");
        fs::write(
            &artifact_path,
            "{\"objective\":\"verify registry evidence\"}\n",
        )
        .expect("write artifact evidence");

        let files = build_artifact_evidence_file_digests(root).expect("digest evidence");
        let content_hash = artifact_evidence_content_hash(&files);
        let content_manifest_path = root.join("content-manifest.json");
        let content_manifest = ArtifactEvidenceContentManifest {
            evidence_path: root.display().to_string(),
            generated_at: "2026-06-01T00:00:00Z".to_string(),
            content_hash: content_hash.clone(),
            hash_algorithm: "blake3".to_string(),
            files,
        };
        fs::write(
            &content_manifest_path,
            serde_json::to_string_pretty(&content_manifest).expect("content manifest json"),
        )
        .expect("write content manifest");

        let attestation_path = root.join("attestation.json");
        let attestation = ArtifactEvidenceAttestation {
            statement_type: "https://in-toto.io/Statement/v1".to_string(),
            predicate_type: "https://sinomo.net/attestations/artifact-evidence/v1".to_string(),
            subject: ArtifactEvidenceSubject {
                name: format!("artifact-evidence:{evidence_id}"),
                digest: content_hash.clone(),
            },
            predicate: serde_json::json!({
                "artifact_refs": [artifact_path.display().to_string()],
                "generated_at": "2026-06-01T00:00:00Z",
            }),
        };
        fs::write(
            &attestation_path,
            serde_json::to_string_pretty(&attestation).expect("attestation json"),
        )
        .expect("write attestation");

        ArtifactEvidenceManifest {
            evidence_id,
            evidence_kind: "test-evidence".to_string(),
            evidence_path: root.display().to_string(),
            declared_content_hash: Some(content_hash),
            content_manifest_path: Some(content_manifest_path.display().to_string()),
            attestation_path: Some(attestation_path.display().to_string()),
            evidence_refs: vec![artifact_path.display().to_string()],
        }
    }
}
