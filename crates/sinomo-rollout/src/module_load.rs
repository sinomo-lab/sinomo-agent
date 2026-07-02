use serde::{Deserialize, Serialize};
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
use sinomo_core::{
    ArtifactManifest, Confidence, EventEnvelope, EventKind, HotSwapState, LatencyClass,
    ModuleLoaderState, PolicyAction, PolicyDecision, PrivacyClass, RolloutPlan, RoutingMetadata,
    RoutingScope, Salience, Source, TrustRing,
};
use sinomo_module::{ArtifactId, ModuleId, verify_artifact_signature};
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LoadRequest {
    pub node_id: NodeId,
    pub artifact: ArtifactManifest,
    pub policy_decision: PolicyDecision,
    pub rollout_plan: RolloutPlan,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LoadDecision {
    pub node_id: NodeId,
    pub module_id: ModuleId,
    pub artifact_id: ArtifactId,
    pub action: LoaderAction,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum LoaderAction {
    AcceptShadow,
    AcceptCandidate,
    Reject,
    Quarantine,
    RollBack,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModulePolicyViolation {
    pub node_id: NodeId,
    pub module_id: ModuleId,
    pub artifact_id: ArtifactId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollout_plan_id: Option<crate::RolloutPlanId>,
    pub reason: String,
}

impl ModulePolicyViolation {
    pub fn event(&self) -> EventEnvelope {
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            self.node_id.clone(),
            Source::new("module.loader").expect("valid source"),
            EventKind::new("module.policy.violation").expect("valid event kind"),
            Salience::new(0.9).expect("valid salience"),
            Confidence::new(0.96).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(self.node_id.clone())
        .with_payload(serde_json::json!({
            "node_id": self.node_id,
            "module_id": self.module_id,
            "artifact_id": self.artifact_id,
            "rollout_plan_id": self.rollout_plan_id,
            "reason": self.reason,
        }))
    }
}

#[derive(Clone, Debug)]
pub struct ModuleLoader {
    node_id: NodeId,
}

impl ModuleLoader {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    pub fn evaluate_load(&self, request: LoadRequest) -> Result<LoadDecision, ModuleLoadError> {
        if request.node_id != self.node_id {
            return Err(ModuleLoadError::WrongNode {
                expected: self.node_id.clone(),
                actual: request.node_id,
            });
        }

        if request.artifact.target_ring != request.rollout_plan.target_ring {
            return Ok(LoadDecision {
                node_id: self.node_id.clone(),
                module_id: request.artifact.module_id,
                artifact_id: request.artifact.artifact_id,
                action: LoaderAction::Reject,
                reason: "artifact and rollout target rings differ".to_string(),
            });
        }

        if request.policy_decision.action == PolicyAction::Allow {
            if let Some(reason) = artifact_trust_material_failure(&request.artifact) {
                return Ok(LoadDecision {
                    node_id: self.node_id.clone(),
                    module_id: request.artifact.module_id,
                    artifact_id: request.artifact.artifact_id,
                    action: LoaderAction::Reject,
                    reason,
                });
            }
        }

        let action = match request.policy_decision.action {
            PolicyAction::Allow if request.artifact.target_ring == TrustRing::R3SandboxedModule => {
                LoaderAction::AcceptShadow
            }
            PolicyAction::Allow => LoaderAction::AcceptCandidate,
            PolicyAction::Quarantine => LoaderAction::Quarantine,
            PolicyAction::EmergencyStop => LoaderAction::RollBack,
            _ => LoaderAction::Reject,
        };

        Ok(LoadDecision {
            node_id: self.node_id.clone(),
            module_id: request.artifact.module_id,
            artifact_id: request.artifact.artifact_id,
            action,
            reason: "phase-2 loader performed manifest/policy/ring checks".to_string(),
        })
    }

    pub fn shadow_state(
        &self,
        request: &LoadRequest,
        active_artifact_id: ArtifactId,
    ) -> ModuleLoaderState {
        ModuleLoaderState {
            module_id: request.artifact.module_id.clone(),
            active_artifact_id,
            previous_artifact_id: None,
            shadow_artifact_id: Some(request.artifact.artifact_id.clone()),
            candidate_artifact_id: Some(request.artifact.artifact_id.clone()),
            hot_swap_state: HotSwapState::Shadowing,
        }
    }
}

fn artifact_trust_material_failure(artifact: &ArtifactManifest) -> Option<String> {
    if !has_supported_content_hash(&artifact.content_hash) {
        return Some(format!(
            "artifact content hash `{}` is not a supported sha256/blake3 digest",
            artifact.content_hash
        ));
    }
    if artifact.signature.is_some() {
        return verify_artifact_signature(artifact)
            .err()
            .map(|error| format!("artifact signature verification failed: {error}"));
    }
    if artifact
        .provenance
        .attestations
        .iter()
        .any(|attestation| is_supported_attestation_ref(attestation))
    {
        return None;
    }
    Some(
        "artifact is missing supported signature or verified artifact-evidence attestation"
            .to_string(),
    )
}

fn has_supported_content_hash(content_hash: &str) -> bool {
    let Some((algorithm, digest)) = content_hash.split_once(':') else {
        return false;
    };
    matches!(algorithm, "sha256" | "blake3") && !digest.trim().is_empty()
}

fn is_supported_attestation_ref(attestation: &str) -> bool {
    attestation.ends_with("artifact-evidence-record.json")
        || attestation.ends_with("attestation.json")
        || attestation.starts_with("registry://artifact-evidence/")
        || attestation.starts_with("in-toto:")
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ModuleLoadError {
    #[error("load request targets {actual}, but loader belongs to {expected}")]
    WrongNode { expected: NodeId, actual: NodeId },
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        ActorKind, ActorRef, ArtifactManifest, ArtifactProvenance, CapabilityDiff, ModuleManifest,
        RollbackPolicy, RolloutStrategy,
    };
    use sinomo_module::{
        artifact_signature_signer_public_key, sign_artifact_manifest_with_node_identity,
    };

    #[test]
    fn r3_allowed_artifact_loads_in_shadow() {
        let node_id = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let request = LoadRequest {
            node_id: node_id.clone(),
            artifact: artifact_manifest(
                &artifact_id,
                &module_id,
                "sha256:abc",
                vec!["bundle/artifact-evidence-record.json".to_string()],
            ),
            policy_decision: allow_policy(&artifact_id),
            rollout_plan: rollout_plan(&node_id, &artifact_id, &module_id),
        };

        let decision = ModuleLoader::new(node_id)
            .evaluate_load(request)
            .expect("load");

        assert_eq!(decision.action, LoaderAction::AcceptShadow);
    }

    #[test]
    fn policy_allowed_artifact_without_trust_material_is_rejected() {
        let node_id = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let request = LoadRequest {
            node_id: node_id.clone(),
            artifact: artifact_manifest(&artifact_id, &module_id, "blake3:abc", Vec::new()),
            policy_decision: allow_policy(&artifact_id),
            rollout_plan: rollout_plan(&node_id, &artifact_id, &module_id),
        };

        let decision = ModuleLoader::new(node_id)
            .evaluate_load(request)
            .expect("load");

        assert_eq!(decision.action, LoaderAction::Reject);
        assert!(decision.reason.contains("missing supported signature"));
    }

    #[test]
    fn policy_allowed_artifact_accepts_valid_local_ed25519_signature() {
        let node_id = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let (_identity_dir, identity_path) = unique_identity_path("valid-artifact-signature");
        let identity =
            sinomo_core::node::load_or_create_node_identity(&identity_path).expect("identity");
        let mut artifact = artifact_manifest(&artifact_id, &module_id, "blake3:abc", Vec::new());
        artifact.signature =
            Some(sign_artifact_manifest_with_node_identity(&artifact, &identity).expect("sign"));
        assert_eq!(
            artifact_signature_signer_public_key(&artifact).expect("signer"),
            identity.public_key
        );
        let request = LoadRequest {
            node_id: node_id.clone(),
            artifact,
            policy_decision: allow_policy(&artifact_id),
            rollout_plan: rollout_plan(&node_id, &artifact_id, &module_id),
        };

        let decision = ModuleLoader::new(node_id)
            .evaluate_load(request)
            .expect("load");

        assert_eq!(decision.action, LoaderAction::AcceptShadow);
    }

    #[test]
    fn policy_allowed_artifact_rejects_tampered_local_ed25519_signature() {
        let node_id = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let (_identity_dir, identity_path) = unique_identity_path("tampered-artifact-signature");
        let identity =
            sinomo_core::node::load_or_create_node_identity(&identity_path).expect("identity");
        let mut artifact = artifact_manifest(&artifact_id, &module_id, "blake3:abc", Vec::new());
        artifact.signature =
            Some(sign_artifact_manifest_with_node_identity(&artifact, &identity).expect("sign"));
        artifact.content_hash = "blake3:tampered".to_string();
        let request = LoadRequest {
            node_id: node_id.clone(),
            artifact,
            policy_decision: allow_policy(&artifact_id),
            rollout_plan: rollout_plan(&node_id, &artifact_id, &module_id),
        };

        let decision = ModuleLoader::new(node_id)
            .evaluate_load(request)
            .expect("load");

        assert_eq!(decision.action, LoaderAction::Reject);
        assert!(decision.reason.contains("signature verification failed"));
    }

    #[test]
    fn module_policy_violation_event_is_structured_for_audit() {
        let node_id = NodeId::from_legacy_named("node_desktop").expect("node");
        let event = ModulePolicyViolation {
            node_id: node_id.clone(),
            module_id: ModuleId::from_legacy_named("module.trigger.silence_policy")
                .expect("module"),
            artifact_id: ArtifactId::from_legacy_named("artifact_candidate").expect("artifact"),
            rollout_plan_id: Some(
                crate::RolloutPlanId::from_legacy_named("rollout_candidate").expect("rollout"),
            ),
            reason: "policy denied candidate load".to_string(),
        }
        .event();

        assert_eq!(event.kind.as_str(), "module.policy.violation");
        assert_eq!(event.target_node_id, Some(node_id));
        assert_eq!(
            event.payload["artifact_id"],
            ArtifactId::from_legacy_named("artifact_candidate")
                .expect("artifact")
                .as_str()
        );
    }

    fn artifact_manifest(
        artifact_id: &ArtifactId,
        module_id: &ModuleId,
        content_hash: &str,
        attestations: Vec<String>,
    ) -> ArtifactManifest {
        ArtifactManifest {
            artifact_id: artifact_id.clone(),
            module_id: module_id.clone(),
            target_ring: TrustRing::R3SandboxedModule,
            content_hash: content_hash.to_string(),
            signature: None,
            source_workspace: None,
            source_revision: None,
            module_manifest: ModuleManifest::r3_wasm(module_id.clone(), "0.1.0", "trigger"),
            provenance: ArtifactProvenance {
                produced_by: ActorRef {
                    kind: ActorKind::Operator,
                    id: "phase2-test".to_string(),
                },
                source_event_id: None,
                build_log_artifact_id: None,
                attestations,
            },
        }
    }

    fn allow_policy(artifact_id: &ArtifactId) -> PolicyDecision {
        PolicyDecision {
            decision_id: sinomo_core::Id::from_legacy_named("policy_ok").expect("policy"),
            target_ring: TrustRing::R3SandboxedModule,
            target_artifact_id: artifact_id.clone(),
            action: PolicyAction::Allow,
            capability_diff: CapabilityDiff::default(),
            required_additional_evals: Vec::new(),
            max_rollout_weight_bps: Some(500),
            reason: "ok".to_string(),
        }
    }

    fn rollout_plan(node: &NodeId, artifact_id: &ArtifactId, module_id: &ModuleId) -> RolloutPlan {
        RolloutPlan {
            rollout_plan_id: crate::RolloutPlanId::from_legacy_named("rollout_ok")
                .expect("rollout"),
            artifact_id: artifact_id.clone(),
            module_id: module_id.clone(),
            target_ring: TrustRing::R3SandboxedModule,
            strategy: RolloutStrategy::Shadow,
            target_nodes: vec![node.clone()],
            initial_weight_bps: 0,
            promotion_steps_bps: Vec::new(),
            rollback_policy: RollbackPolicy::default(),
        }
    }

    fn unique_identity_path(name: &str) -> (sinomo_testdir::TestDir, std::path::PathBuf) {
        let dir = sinomo_testdir::TestDir::new(&format!("sinomo-rollout-{name}"));
        let path = dir.join("identity.sobj");
        (dir, path)
    }
}
