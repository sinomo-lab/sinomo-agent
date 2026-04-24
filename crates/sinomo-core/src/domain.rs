use crate::{
    ArtifactId, CapabilityName, DeviceId, DfsObjectId, DfsVersion, EntityId, EventId,
    EvolutionJobId, ModuleId, NodeId, PolicyDecisionId, RolloutPlanId, ScorecardId, SessionId,
    TaskId, TrustDomain, WorkspaceId,
};
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
    pub session_id: SessionId,
    #[serde(default)]
    pub participant_nodes: Vec<NodeId>,
    #[serde(default)]
    pub active_devices: Vec<DeviceId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TaskState {
    pub task_id: TaskId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
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
    pub request_id: TaskId,
    pub origin_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node_id: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_device_id: Option<DeviceId>,
    pub kind: String,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ActionResult {
    pub request_id: TaskId,
    pub status: ActionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<EventId>,
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
    pub entity_id: EntityId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event_id: Option<EventId>,
    pub summary: String,
    pub visibility: TrustDomain,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TriggerDecision {
    pub trigger_event_id: EventId,
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
    pub job_id: TaskId,
    pub trigger_event_id: EventId,
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
    pub workspace_id: WorkspaceId,
    pub object_id: DfsObjectId,
    pub target_version: DfsVersion,
    pub requesting_node_id: NodeId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteReceipt {
    pub workspace_id: WorkspaceId,
    pub object_id: DfsObjectId,
    pub committed_version: DfsVersion,
    pub owner_node_id: NodeId,
    pub event_id: EventId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct HotStateSnapshot {
    #[serde(default)]
    pub recent_events: Vec<EventId>,
    #[serde(default)]
    pub active_tasks: Vec<TaskId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct ContextPacket {
    #[serde(default)]
    pub salient_events: Vec<EventId>,
    #[serde(default)]
    pub memory_records: Vec<EntityId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct JobCancellation {
    pub job_id: TaskId,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by_event_id: Option<EventId>,
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

    pub fn is_primary_auto_evolution_target(&self) -> bool {
        matches!(self, Self::R3SandboxedModule)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleManifest {
    pub module_id: ModuleId,
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
    pub fn r3_wasm(
        module_id: ModuleId,
        version: impl Into<String>,
        owner: impl Into<String>,
    ) -> Self {
        Self {
            module_id,
            kind: ModuleKind::WasmComponent,
            version: version.into(),
            api_version: "sinomo-module-v1".to_string(),
            owner: owner.into(),
            target_ring: TrustRing::R3SandboxedModule,
            capabilities: ModuleCapabilities::default(),
            limits: ModuleResourceLimits::default(),
            rollout: RolloutPolicy::default(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ModuleKind {
    WasmComponent,
    ProcessSandbox,
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
    pub artifact_id: ArtifactId,
    pub module_id: ModuleId,
    pub target_ring: TrustRing,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_workspace: Option<WorkspaceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
    pub module_manifest: ModuleManifest,
    pub provenance: ArtifactProvenance,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ArtifactProvenance {
    pub produced_by: ActorRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_log_artifact_id: Option<ArtifactId>,
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
    EvolutionOrchestrator,
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
    pub decision_id: PolicyDecisionId,
    pub target_ring: TrustRing,
    pub target_artifact_id: ArtifactId,
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
    pub scorecard_id: ScorecardId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_artifact_id: Option<ArtifactId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_artifact_id: Option<ArtifactId>,
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
pub struct EvolutionJob {
    pub job_id: EvolutionJobId,
    pub target_ring: TrustRing,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_module_id: Option<ModuleId>,
    pub objective: String,
    pub scope: EvolutionScope,
    pub status: EvolutionJobStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_gates: Vec<EvaluationGate>,
    pub rollback_policy: RollbackPolicy,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvolutionScope {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forbidden_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<WorkspaceId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EvolutionJobStatus {
    Observed,
    Scoped,
    Proposed,
    Implemented,
    Built,
    Evaluated,
    PolicyApproved,
    Staged,
    Shadowed,
    Canary,
    Promoted,
    Rejected,
    BuildFailed,
    EvalFailed,
    Denied,
    RolledBack,
    Reverted,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RolloutPlan {
    pub rollout_plan_id: RolloutPlanId,
    pub artifact_id: ArtifactId,
    pub module_id: ModuleId,
    pub target_ring: TrustRing,
    pub strategy: RolloutStrategy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_nodes: Vec<NodeId>,
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
    pub restore_artifact_id: Option<ArtifactId>,
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
    pub module_id: ModuleId,
    pub active_artifact_id: ArtifactId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_artifact_id: Option<ArtifactId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_artifact_id: Option<ArtifactId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_artifact_id: Option<ArtifactId>,
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
    fn trust_rings_expose_evolution_safety_defaults() {
        assert!(TrustRing::R1TrustedRuntime.requires_continuity_gates());
        assert!(TrustRing::R3SandboxedModule.is_primary_auto_evolution_target());
    }

    #[test]
    fn r3_module_manifest_serializes_capsule_shape() {
        let mut manifest = ModuleManifest::r3_wasm(
            ModuleId::new("module.trigger.silence_policy").expect("module id"),
            "0.3.7",
            "sinomo-trigger",
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
        assert_eq!(json["module_id"], "module.trigger.silence_policy");
        assert_eq!(json["target_ring"], "r3-sandboxed-module");
        assert_eq!(json["kind"], "wasm-component");

        let round_tripped: ModuleManifest =
            serde_json::from_value(json).expect("deserialize manifest");
        assert_eq!(round_tripped.target_ring, TrustRing::R3SandboxedModule);
    }

    #[test]
    fn policy_decision_captures_capability_diff_as_data() {
        let decision = PolicyDecision {
            decision_id: PolicyDecisionId::new("policy_allow_trigger").expect("decision id"),
            target_ring: TrustRing::R3SandboxedModule,
            target_artifact_id: ArtifactId::new("artifact_candidate").expect("artifact id"),
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
