//! Artifact registry metadata skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    ArtifactId, ArtifactManifest, EvaluationScorecard, EvolutionJobId, ModuleId, PolicyDecision,
    RolloutPlan,
};
use std::collections::BTreeMap;
use thiserror::Error;
use time::OffsetDateTime;

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
    pub source_job_id: Option<EvolutionJobId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scorecard: Option<EvaluationScorecard>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub registered_at: OffsetDateTime,
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
    entries: BTreeMap<ArtifactId, RegistryEntry>,
    latest_by_module: BTreeMap<ModuleId, ArtifactId>,
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

    pub fn candidate(&self, artifact_id: &ArtifactId) -> Option<&CandidateRecord> {
        self.candidate_records.get(artifact_id)
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

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum RegistryError {
    #[error("duplicate artifact: {artifact_id}")]
    DuplicateArtifact { artifact_id: ArtifactId },
    #[error("duplicate candidate artifact: {artifact_id}")]
    DuplicateCandidate { artifact_id: ArtifactId },
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        ActorKind, ActorRef, ArtifactId, ArtifactManifest, ArtifactProvenance, CapabilityDiff,
        ModuleId, ModuleManifest, PolicyAction, PolicyDecision, PolicyDecisionId, TrustRing,
    };

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
}
