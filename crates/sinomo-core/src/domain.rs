use crate::{CapabilityName, Id, TrustDomain};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NodeCapability {
    pub name: CapabilityName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl NodeCapability {
    pub fn new(name: CapabilityName) -> Self {
        Self {
            name,
            description: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DeviceCapability {
    pub name: CapabilityName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl DeviceCapability {
    pub fn new(name: CapabilityName) -> Self {
        Self {
            name,
            description: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModelCapability {
    pub name: CapabilityName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SessionState {
    pub session_id: Id,
    #[serde(default)]
    pub participant_nodes: Vec<Id>,
    #[serde(default)]
    pub active_devices: Vec<Id>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TaskState {
    pub task_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Id>,
    pub status: TaskStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum TaskStatus {
    Open,
    Waiting,
    Running,
    Blocked,
    Completed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ActionRequest {
    pub request_id: Id,
    pub origin_node_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_device_id: Option<Id>,
    pub kind: String,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ActionResult {
    pub request_id: Id,
    pub status: ActionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<Id>,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ActionStatus {
    Succeeded,
    Failed,
    Blocked,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryRecord {
    pub entity_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event_id: Option<Id>,
    pub summary: String,
    pub visibility: TrustDomain,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TriggerDecision {
    pub trigger_event_id: Id,
    pub action: TriggerAction,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum TriggerAction {
    StaySilent,
    Acknowledge,
    Interrupt,
    Alert,
    Escalate,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DeliberationJob {
    pub job_id: Id,
    pub trigger_event_id: Id,
    pub status: JobStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum JobStatus {
    Queued,
    Running,
    Superseded,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SyncPolicy {
    LocalOnly,
    Session,
    TrustedNetwork,
    DurableGlobal,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsObjectPolicy {
    LocalOwned,
    ReplicatedConfirmed,
    ReplicatedEventual,
    AppendOnly,
    EphemeralLocal,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteRequest {
    pub file_system_id: Id,
    pub object_id: Id,
    pub target_version: Id,
    pub requesting_node_id: Id,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteReceipt {
    pub file_system_id: Id,
    pub object_id: Id,
    pub committed_version: Id,
    pub owner_node_id: Id,
    pub event_id: Id,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct HotStateSnapshot {
    #[serde(default)]
    pub recent_events: Vec<Id>,
    #[serde(default)]
    pub active_tasks: Vec<Id>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct ContextPacket {
    #[serde(default)]
    pub salient_events: Vec<Id>,
    #[serde(default)]
    pub memory_records: Vec<Id>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct JobCancellation {
    pub job_id: Id,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by_event_id: Option<Id>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum TrustRing {
    #[serde(rename = "r0-continuity-root")]
    R0ContinuityRoot,
    #[serde(rename = "r1-trusted-runtime")]
    R1TrustedRuntime,
    #[serde(rename = "r2-trusted-service")]
    R2TrustedService,
    #[serde(rename = "r3-sandboxed-module")]
    R3SandboxedModule,
    #[serde(rename = "r4-disposable-workspace")]
    R4DisposableWorkspace,
    #[serde(rename = "r5-model-provider")]
    R5ModelProvider,
}

impl TrustRing {
    pub fn requires_continuity_gates(&self) -> bool {
        matches!(self, Self::R0ContinuityRoot | Self::R1TrustedRuntime)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleManifest {
    pub module_id: Id,
    pub kind: ModuleKind,
    pub version: String,
    pub api_version: String,
    pub owner: String,
    pub target_ring: TrustRing,
    pub capabilities: ModuleCapabilities,
    pub limits: ModuleResourceLimits,
    pub rollout: RolloutPolicy,
}

impl ModuleManifest {
    pub fn new(
        module_id: Id,
        kind: ModuleKind,
        version: impl Into<String>,
        api_version: impl Into<String>,
        owner: impl Into<String>,
        target_ring: TrustRing,
    ) -> Self {
        Self {
            module_id,
            kind,
            version: version.into(),
            api_version: api_version.into(),
            owner: owner.into(),
            target_ring,
            capabilities: ModuleCapabilities::default(),
            limits: ModuleResourceLimits::default(),
            rollout: RolloutPolicy::default(),
        }
    }

    pub fn r3_wasm(module_id: Id, version: impl Into<String>, owner: impl Into<String>) -> Self {
        Self::new(
            module_id,
            ModuleKind::WasmComponent,
            version,
            "sinomo-module-v1",
            owner,
            TrustRing::R3SandboxedModule,
        )
    }

    pub fn native_dylib(
        module_id: Id,
        version: impl Into<String>,
        owner: impl Into<String>,
        api_version: impl Into<String>,
        target_ring: TrustRing,
    ) -> Self {
        Self::new(
            module_id,
            ModuleKind::NativeDylib,
            version,
            api_version,
            owner,
            target_ring,
        )
    }

    pub fn native_rust(
        module_id: Id,
        version: impl Into<String>,
        owner: impl Into<String>,
        api_version: impl Into<String>,
        target_ring: TrustRing,
    ) -> Self {
        Self::new(
            module_id,
            ModuleKind::NativeRust,
            version,
            api_version,
            owner,
            target_ring,
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ModuleKind {
    NativeRust,
    NativeDylib,
    WasmComponent,
    ProcessSandbox,
    Declarative,
    Prompt,
    Config,
    Policy,
    Evaluator,
    NativeRuntime,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleCapabilities {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub imports: Vec<CapabilityName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exports: Vec<CapabilityName>,
}

impl ModuleCapabilities {
    pub fn is_empty(&self) -> bool {
        self.imports.is_empty() && self.exports.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleResourceLimits {
    pub cpu_ms: u64,
    pub memory_mb: u64,
    pub wall_time_ms: u64,
    pub network: NetworkAccess,
    pub filesystem: FileSystemAccess,
}

impl Default for ModuleResourceLimits {
    fn default() -> Self {
        Self {
            cpu_ms: 50,
            memory_mb: 64,
            wall_time_ms: 100,
            network: NetworkAccess::Deny,
            filesystem: FileSystemAccess::None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkAccess {
    Deny,
    Allowlisted,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum FileSystemAccess {
    None,
    ReadOnly,
    Scoped,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RolloutPolicy {
    pub strategy: RolloutStrategy,
    pub initial_weight_bps: u16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub promotion_steps_bps: Vec<u16>,
    pub rollback_policy: RollbackPolicy,
}

impl Default for RolloutPolicy {
    fn default() -> Self {
        Self {
            strategy: RolloutStrategy::Canary,
            initial_weight_bps: 500,
            promotion_steps_bps: vec![500, 2_500, 5_000, 10_000],
            rollback_policy: RollbackPolicy::default(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ArtifactManifest {
    pub artifact_id: Id,
    pub module_id: Id,
    pub target_ring: TrustRing,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_workspace: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
    pub module_manifest: ModuleManifest,
    pub provenance: ArtifactProvenance,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ArtifactProvenance {
    pub produced_by: ActorRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_log_artifact_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attestations: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ActorRef {
    pub kind: ActorKind,
    pub id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ActorKind {
    MutationWorker,
    Evaluator,
    PolicyEngine,
    Registry,
    ModelProvider,
    Operator,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct CapabilityDiff {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added_imports: Vec<CapabilityName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_imports: Vec<CapabilityName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added_exports: Vec<CapabilityName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_exports: Vec<CapabilityName>,
}

impl CapabilityDiff {
    pub fn requests_new_power(&self) -> bool {
        !self.added_imports.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PolicyDecision {
    pub decision_id: Id,
    pub target_ring: TrustRing,
    pub target_artifact_id: Id,
    pub action: PolicyAction,
    pub capability_diff: CapabilityDiff,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_additional_evals: Vec<EvaluationGate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rollout_weight_bps: Option<u16>,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum PolicyAction {
    Allow,
    Deny,
    Defer,
    Quarantine,
    RequireMoreEvidence,
    EmergencyStop,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvaluationScorecard {
    pub scorecard_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_artifact_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_artifact_id: Option<Id>,
    pub outcome: EvaluationOutcome,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gates: Vec<EvaluationGateResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics: Vec<EvaluationMetric>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EvaluationOutcome {
    Passed,
    Failed,
    Inconclusive,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvaluationGateResult {
    pub gate: EvaluationGate,
    pub outcome: EvaluationOutcome,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EvaluationGate {
    ManifestValidation,
    StaticChecks,
    UnitTests,
    IntegrationTests,
    ReplayTests,
    PrivateRegressionEvals,
    GeneratedAdversarialTests,
    ShadowMode,
    CanaryAnalysis,
    PostPromotionAudit,
    ContinuityChecks,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvaluationMetric {
    pub name: String,
    pub value: MetricValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_count: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MetricValue {
    Bool(bool),
    Integer(i64),
    DecimalBasisPoints(i64),
    Text(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RolloutPlan {
    pub rollout_plan_id: Id,
    pub artifact_id: Id,
    pub module_id: Id,
    pub target_ring: TrustRing,
    pub strategy: RolloutStrategy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_nodes: Vec<Id>,
    pub initial_weight_bps: u16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub promotion_steps_bps: Vec<u16>,
    pub rollback_policy: RollbackPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RolloutStrategy {
    Shadow,
    Canary,
    BlueGreen,
    Rolling,
    Immediate,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RollbackPolicy {
    pub scope: RollbackScope,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub triggers: Vec<RollbackTrigger>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_artifact_id: Option<Id>,
}

impl Default for RollbackPolicy {
    fn default() -> Self {
        Self {
            scope: RollbackScope::Local,
            triggers: vec![
                RollbackTrigger::Panic,
                RollbackTrigger::Timeout,
                RollbackTrigger::PolicyIncident,
                RollbackTrigger::ScoreRegression,
                RollbackTrigger::UserCorrectionSpike,
            ],
            restore_artifact_id: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RollbackScope {
    Local,
    SessionWide,
    TrustDomainWide,
    Global,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RollbackTrigger {
    BuildFailure,
    LoadFailure,
    Panic,
    Timeout,
    ResourceBudgetViolation,
    NewDeniedCapabilityRequest,
    PolicyIncident,
    ScoreRegression,
    EmergencyStop,
    UserCorrectionSpike,
    NodeHealthDegradation,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleLoaderState {
    pub module_id: Id,
    pub active_artifact_id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_artifact_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_artifact_id: Option<Id>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_artifact_id: Option<Id>,
    pub hot_swap_state: HotSwapState,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum HotSwapState {
    Stable,
    Loading,
    Shadowing,
    Draining,
    ActivationCheckpoint,
    ActiveCandidate,
    RollbackCheckpoint,
    RollingBack,
    Quarantined,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trust_rings_expose_continuity_safety_defaults() {
        assert!(TrustRing::R1TrustedRuntime.requires_continuity_gates());
        assert!(!TrustRing::R3SandboxedModule.requires_continuity_gates());
    }

    #[test]
    fn r3_module_manifest_serializes_capsule_shape() {
        let mut manifest = ModuleManifest::r3_wasm(
            Id::from_legacy_named("module.trigger.silence_policy").expect("module id"),
            "0.3.7",
            "trigger-policy",
        );
        manifest
            .capabilities
            .imports
            .push(CapabilityName::new("event.read.session").expect("capability"));
        manifest
            .capabilities
            .exports
            .push(CapabilityName::new("trigger.evaluate").expect("capability"));

        let json = serde_json::to_value(&manifest).expect("serialize manifest");
        assert_eq!(json["module_id"].as_str().expect("module id").len(), 26);
        assert!(!json["module_id"].as_str().expect("module id").contains('.'));
        assert_eq!(json["target_ring"], "r3-sandboxed-module");
        assert_eq!(json["kind"], "wasm-component");

        let round_tripped: ModuleManifest =
            serde_json::from_value(json).expect("deserialize manifest");
        assert_eq!(round_tripped.target_ring, TrustRing::R3SandboxedModule);
    }

    #[test]
    fn native_dylib_module_manifest_is_backend_neutral() {
        let manifest = ModuleManifest::native_dylib(
            Id::from_legacy_named("module.sandbox.wasix").expect("module id"),
            "0.1.0",
            "sinomo-sandbox",
            "sinomo.sandbox.wasix.v1",
            TrustRing::R1TrustedRuntime,
        );

        let json = serde_json::to_value(&manifest).expect("serialize manifest");
        assert_eq!(json["kind"], "native-dylib");
        assert_eq!(json["api_version"], "sinomo.sandbox.wasix.v1");
        assert_eq!(json["target_ring"], "r1-trusted-runtime");

        let round_tripped: ModuleManifest =
            serde_json::from_value(json).expect("deserialize manifest");
        assert_eq!(round_tripped.kind, ModuleKind::NativeDylib);
    }

    #[test]
    fn native_rust_module_manifest_is_backend_neutral() {
        let manifest = ModuleManifest::native_rust(
            Id::from_legacy_named("module.dfs").expect("module id"),
            "0.1.0",
            "sinomo-dfs",
            "sinomo.dfs.v1",
            TrustRing::R1TrustedRuntime,
        );

        let json = serde_json::to_value(&manifest).expect("serialize manifest");
        assert_eq!(json["kind"], "native-rust");
        assert_eq!(json["api_version"], "sinomo.dfs.v1");
        assert_eq!(json["target_ring"], "r1-trusted-runtime");

        let round_tripped: ModuleManifest =
            serde_json::from_value(json).expect("deserialize manifest");
        assert_eq!(round_tripped.kind, ModuleKind::NativeRust);
    }

    #[test]
    fn policy_decision_captures_capability_diff_as_data() {
        let decision = PolicyDecision {
            decision_id: Id::from_legacy_named("policy_allow_trigger").expect("decision id"),
            target_ring: TrustRing::R3SandboxedModule,
            target_artifact_id: Id::from_legacy_named("artifact_candidate").expect("artifact id"),
            action: PolicyAction::RequireMoreEvidence,
            capability_diff: CapabilityDiff {
                added_imports: vec![CapabilityName::new("event.read.session").expect("capability")],
                ..CapabilityDiff::default()
            },
            required_additional_evals: vec![EvaluationGate::ReplayTests],
            max_rollout_weight_bps: Some(500),
            reason: "new event read capability requires replay evidence".to_string(),
        };

        assert!(decision.capability_diff.requests_new_power());
        assert_eq!(decision.action, PolicyAction::RequireMoreEvidence);
    }
}
