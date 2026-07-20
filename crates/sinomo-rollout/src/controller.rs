use crate::{
    LoadDecision, LoadRequest, LoaderAction, ModuleLoader, RecordedRolloutTransition,
    RolloutTelemetryAdvanceState, RolloutTelemetryGateConfig, RolloutTransitionRecord,
    SelfRolloutPaths, read_rollout_runtime_state, read_rollout_telemetry_advance_state,
    record_rollout_transition, rollout_runtime_state_path, write_rollout_runtime_state,
    write_rollout_telemetry_advance_state,
};
use serde::{Deserialize, Serialize};
use sinomo_core::events::{
    EventId, EventSequence, EventStore, ReplayDirection, ReplayQuery, StoredEvent,
};
use sinomo_core::node::NodeId;
use sinomo_core::{
    ArtifactManifest, Confidence, EvaluationMetric, EvaluationOutcome, EvaluationScorecard,
    EventEnvelope, EventKind, HotSwapState, LatencyClass, MetricValue, ModuleLoaderState,
    PolicyDecision, PrivacyClass, RollbackTrigger, RolloutPlan, RolloutStrategy, RoutingMetadata,
    RoutingScope, Salience, Source, TrustRing,
};
use sinomo_module::{
    ArtifactId, MODULE_CRASH_EVENT_KIND, MODULE_INVOCATION_DECISION_EVENT_KIND,
    MODULE_POLICY_BLOCKED_EVENT_KIND, MODULE_SHADOW_DECISION_EVENT_KIND, MODULE_TIMEOUT_EVENT_KIND,
    MODULE_USER_CORRECTION_EVENT_KIND, ModuleCanaryTelemetryReport, ModuleLoaderRuntimeState,
    ModuleShadowTelemetryReport, module_loader_applied_at_now, module_loader_state_path,
    verify_artifact_signature, write_module_loader_runtime_state,
};
use sinomo_registry::{TrustedArtifactSigner, authorize_artifact_signature};
use std::path::PathBuf;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RolloutRuntimeState {
    pub plan: RolloutPlan,
    pub loader_state: ModuleLoaderState,
    pub stage: RolloutStage,
    pub current_weight_bps: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RolloutStage {
    Shadow,
    Canary,
    GuardedR1,
    Promoted,
    RolledBack,
    Quarantined,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RolloutObservation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scorecard: Option<EvaluationScorecard>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub incident_event_ids: Vec<EventId>,
    pub user_correction_count: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rollback_triggers: Vec<RollbackTrigger>,
}

pub const DEFAULT_SHADOW_TELEMETRY_LIMIT: usize = 1_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RolloutTelemetryAdvanceOptions {
    pub event_limit: usize,
    pub minimum_event_count: u64,
    pub automatic_promotion_enabled: bool,
    pub maximum_automatic_weight_bps: Option<u16>,
    pub maximum_candidate_latency_p95_ms: Option<u64>,
}

impl Default for RolloutTelemetryAdvanceOptions {
    fn default() -> Self {
        Self {
            event_limit: DEFAULT_SHADOW_TELEMETRY_LIMIT,
            minimum_event_count: 1,
            automatic_promotion_enabled: false,
            maximum_automatic_weight_bps: Some(500),
            maximum_candidate_latency_p95_ms: None,
        }
    }
}

impl From<RolloutTelemetryGateConfig> for RolloutTelemetryAdvanceOptions {
    fn from(config: RolloutTelemetryGateConfig) -> Self {
        Self {
            event_limit: config.event_limit.max(1),
            minimum_event_count: config.minimum_event_count.max(1),
            automatic_promotion_enabled: config.automatic_promotion_enabled,
            maximum_automatic_weight_bps: config.maximum_automatic_weight_bps,
            maximum_candidate_latency_p95_ms: config.maximum_candidate_latency_p95_ms,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RolloutTelemetryAdvanceStatus {
    NoRuntimeState,
    NoCandidate,
    TerminalStage,
    WaitingForTelemetry,
    PromotionGateBlocked,
    AppliedTransition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RolloutTelemetryAdvanceResult {
    pub status: RolloutTelemetryAdvanceStatus,
    pub observed_event_count: u64,
    pub ignored_event_count: u64,
    pub last_event_sequence: Option<EventSequence>,
    pub applied_transition: Option<AppliedRolloutTransition>,
}

pub fn shadow_telemetry_scorecard_from_event_store<S>(
    state: &RolloutRuntimeState,
    event_store: &S,
    limit: usize,
) -> Result<EvaluationScorecard, sinomo_core::events::EventStoreError>
where
    S: EventStore,
{
    let events = event_store.replay(ReplayQuery {
        direction: ReplayDirection::Reverse,
        limit: Some(limit),
        ..ReplayQuery::by_kind_prefix(MODULE_SHADOW_DECISION_EVENT_KIND)
    })?;
    Ok(shadow_telemetry_scorecard_from_events(state, &events))
}

pub fn shadow_telemetry_scorecard_from_events<'a>(
    state: &RolloutRuntimeState,
    events: impl IntoIterator<Item = &'a StoredEvent>,
) -> EvaluationScorecard {
    let report =
        ModuleShadowTelemetryReport::from_stored_events(state.plan.module_id.clone(), events);
    let baseline_artifact_id = Some(state.loader_state.active_artifact_id.clone());
    let candidate_artifact_id = state
        .loader_state
        .candidate_artifact_id
        .clone()
        .or_else(|| state.loader_state.shadow_artifact_id.clone())
        .or_else(|| Some(state.plan.artifact_id.clone()));
    shadow_telemetry_scorecard_from_report(&report, baseline_artifact_id, candidate_artifact_id)
}

pub fn canary_telemetry_scorecard_from_event_store<S>(
    state: &RolloutRuntimeState,
    event_store: &S,
    limit: usize,
    minimum_candidate_event_count: u64,
    maximum_candidate_latency_p95_ms: Option<u64>,
) -> Result<EvaluationScorecard, sinomo_core::events::EventStoreError>
where
    S: EventStore,
{
    let events = canary_telemetry_events_from_event_store(event_store, None, limit)?;
    Ok(canary_telemetry_scorecard_from_events(
        state,
        &events,
        minimum_candidate_event_count,
        maximum_candidate_latency_p95_ms,
    ))
}

pub fn canary_telemetry_scorecard_from_events<'a>(
    state: &RolloutRuntimeState,
    events: impl IntoIterator<Item = &'a StoredEvent>,
    minimum_candidate_event_count: u64,
    maximum_candidate_latency_p95_ms: Option<u64>,
) -> EvaluationScorecard {
    let candidate_artifact_id = rollout_candidate_artifact_id(state);
    let report = ModuleCanaryTelemetryReport::from_stored_events(
        state.plan.module_id.clone(),
        candidate_artifact_id.as_ref(),
        events,
    );
    canary_telemetry_scorecard_from_report(
        &report,
        Some(state.loader_state.active_artifact_id.clone()),
        candidate_artifact_id,
        minimum_candidate_event_count,
        maximum_candidate_latency_p95_ms,
    )
}

fn shadow_telemetry_scorecard_from_report(
    report: &ModuleShadowTelemetryReport,
    baseline_artifact_id: Option<ArtifactId>,
    candidate_artifact_id: Option<ArtifactId>,
) -> EvaluationScorecard {
    let outcome = shadow_telemetry_outcome(report);
    EvaluationScorecard {
        scorecard_id: sinomo_core::Id::generate(),
        baseline_artifact_id,
        candidate_artifact_id,
        outcome: outcome.clone(),
        gates: vec![sinomo_core::EvaluationGateResult {
            gate: sinomo_core::EvaluationGate::ShadowMode,
            outcome,
            summary: format!(
                "{} live shadow events, {} mismatches, {} primary errors, {} shadow errors",
                report.event_count,
                report.mismatch_count,
                report.primary_error_count,
                report.shadow_error_count
            ),
        }],
        metrics: vec![
            rollout_metric("module_shadow_event_count", report.event_count as i64),
            rollout_metric("module_shadow_matched_count", report.matched_count as i64),
            rollout_metric("module_shadow_mismatch_count", report.mismatch_count as i64),
            rollout_metric(
                "module_shadow_primary_error_count",
                report.primary_error_count as i64,
            ),
            rollout_metric(
                "module_shadow_shadow_error_count",
                report.shadow_error_count as i64,
            ),
            rollout_metric(
                "module_shadow_ignored_event_count",
                report.ignored_event_count as i64,
            ),
        ],
    }
}

fn shadow_telemetry_outcome(report: &ModuleShadowTelemetryReport) -> EvaluationOutcome {
    if report.event_count == 0 {
        return EvaluationOutcome::Inconclusive;
    }
    if report.mismatch_count == 0
        && report.primary_error_count == 0
        && report.shadow_error_count == 0
    {
        EvaluationOutcome::Passed
    } else {
        EvaluationOutcome::Failed
    }
}

fn canary_telemetry_scorecard_from_report(
    report: &ModuleCanaryTelemetryReport,
    baseline_artifact_id: Option<ArtifactId>,
    candidate_artifact_id: Option<ArtifactId>,
    minimum_candidate_event_count: u64,
    maximum_candidate_latency_p95_ms: Option<u64>,
) -> EvaluationScorecard {
    let outcome = canary_telemetry_outcome(
        report,
        minimum_candidate_event_count,
        maximum_candidate_latency_p95_ms,
    );
    let mut metrics = vec![
        rollout_metric("module_canary_event_count", report.event_count as i64),
        rollout_metric(
            "module_canary_candidate_primary_count",
            report.candidate_primary_count as i64,
        ),
        rollout_metric(
            "module_canary_active_primary_count",
            report.active_primary_count as i64,
        ),
        rollout_metric(
            "module_canary_correction_event_count",
            report.correction_event_count as i64,
        ),
        rollout_metric(
            "module_canary_candidate_user_correction_count",
            report.candidate_user_correction_count as i64,
        ),
        rollout_metric(
            "module_canary_active_user_correction_count",
            report.active_user_correction_count as i64,
        ),
        rollout_metric(
            "module_canary_unknown_user_correction_count",
            report.unknown_user_correction_count as i64,
        ),
        rollout_metric(
            "module_canary_candidate_error_count",
            report.candidate_error_count as i64,
        ),
        rollout_metric(
            "module_canary_active_error_count",
            report.active_error_count as i64,
        ),
        rollout_metric(
            "module_canary_candidate_latency_sample_count",
            report.candidate_latency_sample_count as i64,
        ),
        rollout_metric(
            "module_canary_active_latency_sample_count",
            report.active_latency_sample_count as i64,
        ),
        rollout_metric(
            "module_canary_ignored_event_count",
            report.ignored_event_count as i64,
        ),
    ];
    if let Some(latency_ms) = report.candidate_latency_p95_ms {
        metrics.push(rollout_metric(
            "module_canary_candidate_latency_p95_ms",
            latency_ms as i64,
        ));
    }
    if let Some(latency_ms) = report.active_latency_p95_ms {
        metrics.push(rollout_metric(
            "module_canary_active_latency_p95_ms",
            latency_ms as i64,
        ));
    }
    EvaluationScorecard {
        scorecard_id: sinomo_core::Id::generate(),
        baseline_artifact_id,
        candidate_artifact_id,
        outcome: outcome.clone(),
        gates: vec![sinomo_core::EvaluationGateResult {
            gate: sinomo_core::EvaluationGate::CanaryAnalysis,
            outcome,
            summary: format!(
                "{} canary candidate-primary events, {} candidate errors, {} candidate user corrections, candidate p95 latency {} ms, {} active-primary events, {} active errors",
                report.candidate_primary_count,
                report.candidate_error_count,
                report.candidate_user_correction_count,
                report
                    .candidate_latency_p95_ms
                    .map(|latency| latency.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                report.active_primary_count,
                report.active_error_count
            ),
        }],
        metrics,
    }
}

fn canary_telemetry_outcome(
    report: &ModuleCanaryTelemetryReport,
    minimum_candidate_event_count: u64,
    maximum_candidate_latency_p95_ms: Option<u64>,
) -> EvaluationOutcome {
    if report.candidate_user_correction_count > 0 {
        return EvaluationOutcome::Failed;
    }
    if report.candidate_primary_count < minimum_candidate_event_count.max(1) {
        return EvaluationOutcome::Inconclusive;
    }
    if report.candidate_error_count > 0 {
        return EvaluationOutcome::Failed;
    }
    if maximum_candidate_latency_p95_ms.is_some_and(|maximum| {
        report
            .candidate_latency_p95_ms
            .is_some_and(|latency| latency > maximum)
    }) {
        return EvaluationOutcome::Failed;
    }
    EvaluationOutcome::Passed
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ModuleRuntimeFailureReport {
    incident_event_ids: Vec<EventId>,
    rollback_triggers: Vec<RollbackTrigger>,
    panic_count: u64,
    timeout_count: u64,
    policy_blocked_count: u64,
    ignored_event_count: u64,
}

impl ModuleRuntimeFailureReport {
    fn from_stored_events<'a>(
        module_id: &sinomo_core::Id,
        candidate_artifact_id: Option<&ArtifactId>,
        events: impl IntoIterator<Item = &'a StoredEvent>,
    ) -> Self {
        let mut report = Self::default();
        for stored in events {
            let Some(trigger) =
                runtime_failure_trigger_for_event(stored, module_id, candidate_artifact_id)
            else {
                report.ignored_event_count += 1;
                continue;
            };
            if trigger == RollbackTrigger::PolicyIncident
                && !report.incident_event_ids.contains(&stored.event.event_id)
            {
                report
                    .incident_event_ids
                    .push(stored.event.event_id.clone());
            }
            match trigger {
                RollbackTrigger::Panic => report.panic_count += 1,
                RollbackTrigger::Timeout => report.timeout_count += 1,
                RollbackTrigger::PolicyIncident => report.policy_blocked_count += 1,
                _ => {}
            }
            if !report.rollback_triggers.contains(&trigger) {
                report.rollback_triggers.push(trigger);
            }
        }
        report
    }
}

fn append_runtime_failure_metrics(
    scorecard: &mut EvaluationScorecard,
    report: &ModuleRuntimeFailureReport,
) {
    scorecard.metrics.extend([
        EvaluationMetric {
            name: "module_canary_panic_count".to_string(),
            value: MetricValue::Integer(report.panic_count as i64),
            sample_count: None,
        },
        EvaluationMetric {
            name: "module_canary_timeout_count".to_string(),
            value: MetricValue::Integer(report.timeout_count as i64),
            sample_count: None,
        },
        EvaluationMetric {
            name: "module_canary_policy_blocked_count".to_string(),
            value: MetricValue::Integer(report.policy_blocked_count as i64),
            sample_count: None,
        },
    ]);
}

fn rollout_metric(name: &str, value: i64) -> EvaluationMetric {
    EvaluationMetric {
        name: name.to_string(),
        value: MetricValue::Integer(value),
        sample_count: None,
    }
}

fn runtime_failure_trigger_for_event(
    stored: &StoredEvent,
    module_id: &sinomo_core::Id,
    candidate_artifact_id: Option<&ArtifactId>,
) -> Option<RollbackTrigger> {
    if !payload_module_matches(&stored.event.payload, module_id) {
        return None;
    }
    if !runtime_failure_targets_candidate(&stored.event.payload, candidate_artifact_id) {
        return None;
    }
    match stored.event.kind.as_str() {
        MODULE_INVOCATION_DECISION_EVENT_KIND => {
            runtime_failure_trigger_for_invocation_decision(&stored.event.payload)
        }
        MODULE_CRASH_EVENT_KIND => Some(RollbackTrigger::Panic),
        MODULE_TIMEOUT_EVENT_KIND => Some(RollbackTrigger::Timeout),
        MODULE_POLICY_BLOCKED_EVENT_KIND => Some(RollbackTrigger::PolicyIncident),
        _ => None,
    }
}

fn runtime_failure_trigger_for_invocation_decision(
    payload: &serde_json::Value,
) -> Option<RollbackTrigger> {
    match payload
        .get("primary_error_kind")
        .and_then(serde_json::Value::as_str)
    {
        Some("timeout") => return Some(RollbackTrigger::Timeout),
        Some("policy-blocked") => return Some(RollbackTrigger::PolicyIncident),
        _ => {}
    }
    let error = payload
        .get("primary_error")
        .and_then(serde_json::Value::as_str)?
        .trim();
    if error.is_empty() {
        return None;
    }
    let error = error.to_ascii_lowercase();
    if error.contains("timed out") || error.contains("timeout") {
        return Some(RollbackTrigger::Timeout);
    }
    None
}

fn payload_module_matches(payload: &serde_json::Value, module_id: &sinomo_core::Id) -> bool {
    payload.get("module_id").and_then(serde_json::Value::as_str) == Some(module_id.as_str())
}

fn runtime_failure_targets_candidate(
    payload: &serde_json::Value,
    candidate_artifact_id: Option<&ArtifactId>,
) -> bool {
    let lane = payload
        .get("primary_lane")
        .or_else(|| payload.get("lane"))
        .and_then(serde_json::Value::as_str);
    let artifact_id = payload
        .get("artifact_id")
        .or_else(|| payload.get("failed_artifact_id"))
        .or_else(|| payload.get("primary_artifact_id"))
        .and_then(serde_json::Value::as_str);
    if let (Some(candidate), Some(artifact_id)) = (candidate_artifact_id, artifact_id) {
        return artifact_id == candidate.as_str();
    }
    lane == Some("candidate")
}

fn canary_telemetry_events_from_event_store<S>(
    event_store: &S,
    after_sequence: Option<EventSequence>,
    event_limit: usize,
) -> Result<Vec<StoredEvent>, sinomo_core::events::EventStoreError>
where
    S: EventStore,
{
    let limit = event_limit.max(1);
    let mut events = event_store.replay(rollout_telemetry_query(
        MODULE_INVOCATION_DECISION_EVENT_KIND,
        after_sequence,
        limit,
    ))?;
    events.append(&mut event_store.replay(rollout_telemetry_query(
        MODULE_USER_CORRECTION_EVENT_KIND,
        after_sequence,
        limit,
    ))?);
    events.append(&mut event_store.replay(rollout_telemetry_query(
        MODULE_CRASH_EVENT_KIND,
        after_sequence,
        limit,
    ))?);
    events.append(&mut event_store.replay(rollout_telemetry_query(
        MODULE_TIMEOUT_EVENT_KIND,
        after_sequence,
        limit,
    ))?);
    events.append(&mut event_store.replay(rollout_telemetry_query(
        MODULE_POLICY_BLOCKED_EVENT_KIND,
        after_sequence,
        limit,
    ))?);
    Ok(events)
}

fn rollout_telemetry_events_from_event_store<S>(
    event_store: &S,
    telemetry_kind: &str,
    after_sequence: Option<EventSequence>,
    event_limit: usize,
) -> Result<Vec<StoredEvent>, sinomo_core::events::EventStoreError>
where
    S: EventStore,
{
    if telemetry_kind == MODULE_INVOCATION_DECISION_EVENT_KIND {
        return canary_telemetry_events_from_event_store(event_store, after_sequence, event_limit);
    }
    event_store.replay(rollout_telemetry_query(
        telemetry_kind,
        after_sequence,
        event_limit.max(1),
    ))
}

fn rollout_telemetry_query(
    telemetry_kind: &str,
    after_sequence: Option<EventSequence>,
    event_limit: usize,
) -> ReplayQuery {
    let mut query = ReplayQuery::by_kind_prefix(telemetry_kind).limit(event_limit);
    if let Some(after_sequence) = after_sequence {
        query = query.after(after_sequence);
    }
    query
}

/// Returns whether an appended event can change the live rollout telemetry
/// projection. Keep this aligned with the exact replay prefixes above so the
/// daemon only wakes for inputs the controller can consume.
pub fn rollout_telemetry_event_kind_is_relevant(kind: &EventKind) -> bool {
    [
        MODULE_SHADOW_DECISION_EVENT_KIND,
        MODULE_INVOCATION_DECISION_EVENT_KIND,
        MODULE_USER_CORRECTION_EVENT_KIND,
        MODULE_CRASH_EVENT_KIND,
        MODULE_TIMEOUT_EVENT_KIND,
        MODULE_POLICY_BLOCKED_EVENT_KIND,
    ]
    .into_iter()
    .any(|prefix| kind.as_str().starts_with(prefix))
}

fn rollout_candidate_artifact_id(state: &RolloutRuntimeState) -> Option<ArtifactId> {
    state
        .loader_state
        .candidate_artifact_id
        .clone()
        .or_else(|| state.loader_state.shadow_artifact_id.clone())
}

fn rollout_telemetry_checkpoint_matches(
    checkpoint: &RolloutTelemetryAdvanceState,
    state: &RolloutRuntimeState,
    telemetry_kind: &str,
) -> bool {
    checkpoint.rollout_plan_id == state.plan.rollout_plan_id
        && checkpoint.module_id == state.plan.module_id
        && checkpoint.telemetry_kind == telemetry_kind
        && rollout_candidate_artifact_id(state)
            .as_ref()
            .is_some_and(|candidate| checkpoint.candidate_artifact_id == *candidate)
}

fn rollout_stage_is_terminal(stage: &RolloutStage) -> bool {
    matches!(
        stage,
        RolloutStage::Promoted | RolloutStage::RolledBack | RolloutStage::Quarantined
    )
}

fn rollout_telemetry_promotion_is_blocked(
    decision: &RolloutDecision,
    options: &RolloutTelemetryAdvanceOptions,
) -> bool {
    if !matches!(
        decision.action,
        RolloutAction::AdvanceCanary | RolloutAction::Promote | RolloutAction::GuardR1Activation
    ) {
        return false;
    }
    if !options.automatic_promotion_enabled {
        return true;
    }
    options
        .maximum_automatic_weight_bps
        .is_some_and(|maximum| decision.state.current_weight_bps > maximum)
}

fn rollout_telemetry_evidence_ref(
    evidence_kind: &str,
    after_sequence: Option<EventSequence>,
    through_sequence: Option<EventSequence>,
    limit: usize,
    event_count: u64,
) -> String {
    format!(
        "{evidence_kind}:events/event-log.jsonl?after={}&through={}&limit={limit}&events={event_count}",
        after_sequence.map(|sequence| sequence.0).unwrap_or(0),
        through_sequence
            .map(|sequence| sequence.0.to_string())
            .unwrap_or_else(|| "none".to_string())
    )
}

struct RolloutTelemetryEvidenceInputs<'a> {
    evidence_kind: &'a str,
    telemetry_kind: &'a str,
    after_sequence: Option<EventSequence>,
    through_sequence: Option<EventSequence>,
    limit: usize,
    observed_event_count: u64,
    ignored_event_count: u64,
    state: &'a RolloutRuntimeState,
    observation: &'a RolloutObservation,
    active_rollback_triggers: &'a [RollbackTrigger],
}

fn rollout_telemetry_evidence_refs(inputs: RolloutTelemetryEvidenceInputs<'_>) -> Vec<String> {
    let after = inputs
        .after_sequence
        .map(|sequence| sequence.0)
        .unwrap_or(0);
    let through = inputs
        .through_sequence
        .map(|sequence| sequence.0.to_string())
        .unwrap_or_else(|| "none".to_string());
    let mut refs = vec![
        rollout_telemetry_evidence_ref(
            inputs.evidence_kind,
            inputs.after_sequence,
            inputs.through_sequence,
            inputs.limit,
            inputs.observed_event_count,
        ),
        format!("rollout-telemetry://kind/{}", inputs.telemetry_kind),
        format!(
            "rollout-telemetry://window?after={after}&through={through}&limit={}",
            inputs.limit
        ),
        format!(
            "rollout-telemetry://counts?observed={}&ignored={}&incidents={}&user_corrections={}&observed_rollback_triggers={}&active_rollback_triggers={}",
            inputs.observed_event_count,
            inputs.ignored_event_count,
            inputs.observation.incident_event_ids.len(),
            inputs.observation.user_correction_count,
            inputs.observation.rollback_triggers.len(),
            inputs.active_rollback_triggers.len()
        ),
        format!(
            "rollout-telemetry://stage/{}",
            rollout_stage_label(&inputs.state.stage)
        ),
        format!(
            "rollout-telemetry://artifact?active={}&candidate={}",
            inputs.state.loader_state.active_artifact_id,
            inputs
                .state
                .loader_state
                .candidate_artifact_id
                .as_ref()
                .or(inputs.state.loader_state.shadow_artifact_id.as_ref())
                .map(|artifact| artifact.as_str())
                .unwrap_or("none")
        ),
    ];

    for event_id in &inputs.observation.incident_event_ids {
        refs.push(format!("rollout-telemetry://incident-event/{event_id}"));
    }
    if inputs.observation.user_correction_count > 0 {
        refs.push(format!(
            "rollout-telemetry://user-corrections?count={}",
            inputs.observation.user_correction_count
        ));
    }
    for trigger in &inputs.observation.rollback_triggers {
        refs.push(format!(
            "rollout-telemetry://observed-rollback-trigger/{}",
            rollback_trigger_label(trigger)
        ));
    }
    for trigger in inputs.active_rollback_triggers {
        refs.push(format!(
            "rollout-telemetry://active-rollback-trigger/{}",
            rollback_trigger_label(trigger)
        ));
    }
    refs
}

fn rollout_telemetry_result(
    status: RolloutTelemetryAdvanceStatus,
    observed_event_count: u64,
    ignored_event_count: u64,
    last_event_sequence: Option<EventSequence>,
    applied_transition: Option<AppliedRolloutTransition>,
) -> RolloutTelemetryAdvanceResult {
    RolloutTelemetryAdvanceResult {
        status,
        observed_event_count,
        ignored_event_count,
        last_event_sequence,
        applied_transition,
    }
}

fn rollout_timestamp_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RolloutDecision {
    pub action: RolloutAction,
    pub state: RolloutRuntimeState,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RolloutAction {
    StayShadow,
    AdvanceCanary,
    Promote,
    GuardR1Activation,
    RollBack,
    Quarantine,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VerifiedArtifactRolloutRequest {
    pub node_id: NodeId,
    pub artifact: ArtifactManifest,
    pub policy_decision: PolicyDecision,
    pub rollout_plan: RolloutPlan,
    pub release_id: sinomo_core::Id,
    pub release_material_hash: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trusted_signers: Vec<TrustedArtifactSigner>,
    pub active_artifact_id: ArtifactId,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VerifiedArtifactRolloutPreparation {
    pub accepted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_decision: Option<LoadDecision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_state: Option<RolloutRuntimeState>,
    pub release_id: sinomo_core::Id,
    pub release_material_hash: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedRolloutTransition {
    pub transition: RolloutTransitionRecord,
    pub transition_path: PathBuf,
    pub module_state: ModuleLoaderRuntimeState,
    pub module_state_path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct RolloutController {
    node_id: NodeId,
}

impl RolloutController {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    pub fn prepare_verified_artifact_rollout(
        &self,
        request: VerifiedArtifactRolloutRequest,
    ) -> VerifiedArtifactRolloutPreparation {
        if let Some(reason) = verified_artifact_rollout_blocker(&self.node_id, &request) {
            return VerifiedArtifactRolloutPreparation {
                accepted: false,
                load_decision: None,
                initial_state: None,
                release_id: request.release_id,
                release_material_hash: request.release_material_hash,
                evidence_refs: request.evidence_refs,
                reason,
            };
        }

        let release_id = request.release_id.clone();
        let release_material_hash = request.release_material_hash.clone();
        let evidence_refs = request.evidence_refs.clone();
        let load_decision =
            match ModuleLoader::new(self.node_id.clone()).evaluate_load(LoadRequest {
                node_id: request.node_id,
                artifact: request.artifact,
                policy_decision: request.policy_decision,
                rollout_plan: request.rollout_plan.clone(),
            }) {
                Ok(decision) => decision,
                Err(error) => {
                    return VerifiedArtifactRolloutPreparation {
                        accepted: false,
                        load_decision: None,
                        initial_state: None,
                        release_id,
                        release_material_hash,
                        evidence_refs,
                        reason: error.to_string(),
                    };
                }
            };
        let accepted = matches!(
            load_decision.action,
            LoaderAction::AcceptShadow | LoaderAction::AcceptCandidate
        );
        let initial_state = if accepted {
            Some(self.initial_state(request.rollout_plan, request.active_artifact_id))
        } else {
            None
        };
        let reason = if accepted {
            format!(
                "verified release {} prepared for rollout; loader action {:?}",
                release_id, load_decision.action
            )
        } else {
            format!(
                "loader rejected verified release {}: {}",
                release_id, load_decision.reason
            )
        };
        VerifiedArtifactRolloutPreparation {
            accepted,
            load_decision: Some(load_decision),
            initial_state,
            release_id,
            release_material_hash,
            evidence_refs,
            reason,
        }
    }

    pub fn prepare_and_record_verified_artifact_rollout(
        &self,
        paths: &SelfRolloutPaths,
        request: VerifiedArtifactRolloutRequest,
    ) -> Result<VerifiedArtifactRolloutPreparation, crate::RolloutError> {
        let preparation = self.prepare_verified_artifact_rollout(request);
        if let Some(state) = &preparation.initial_state {
            write_rollout_runtime_state(paths, state)?;
        }
        Ok(preparation)
    }

    pub fn initial_state(
        &self,
        plan: RolloutPlan,
        active_artifact_id: ArtifactId,
    ) -> RolloutRuntimeState {
        RolloutRuntimeState {
            loader_state: ModuleLoaderState {
                module_id: plan.module_id.clone(),
                active_artifact_id,
                previous_artifact_id: None,
                shadow_artifact_id: Some(plan.artifact_id.clone()),
                candidate_artifact_id: Some(plan.artifact_id.clone()),
                hot_swap_state: HotSwapState::Shadowing,
            },
            current_weight_bps: plan.initial_weight_bps,
            stage: match plan.strategy {
                RolloutStrategy::Shadow => RolloutStage::Shadow,
                RolloutStrategy::Canary | RolloutStrategy::Rolling | RolloutStrategy::BlueGreen => {
                    RolloutStage::Canary
                }
                RolloutStrategy::Immediate => RolloutStage::Promoted,
            },
            plan,
        }
    }

    pub fn advance(
        &self,
        mut state: RolloutRuntimeState,
        observation: RolloutObservation,
    ) -> RolloutDecision {
        let rollback_triggers = active_rollback_triggers(&state, &observation);
        if !rollback_triggers.is_empty() {
            state.stage = RolloutStage::RolledBack;
            state.current_weight_bps = 0;
            state.loader_state.hot_swap_state = HotSwapState::RollingBack;
            if let Some(restore_artifact_id) =
                state.plan.rollback_policy.restore_artifact_id.clone()
            {
                state.loader_state.active_artifact_id = restore_artifact_id;
            }
            state.loader_state.shadow_artifact_id = None;
            state.loader_state.candidate_artifact_id = None;
            return RolloutDecision {
                action: RolloutAction::RollBack,
                state,
                reason: format!(
                    "rollback policy triggered by {}",
                    rollback_trigger_labels(&rollback_triggers)
                ),
            };
        }

        if state.plan.target_ring == TrustRing::R1TrustedRuntime {
            state.stage = RolloutStage::GuardedR1;
            state.loader_state.hot_swap_state = HotSwapState::ActivationCheckpoint;
            return RolloutDecision {
                action: RolloutAction::GuardR1Activation,
                state,
                reason: "R1 candidate requires guarded activation checkpoint".to_string(),
            };
        }

        if observation
            .scorecard
            .as_ref()
            .is_some_and(|scorecard| scorecard.outcome == EvaluationOutcome::Passed)
        {
            let next_weight =
                next_weight(state.current_weight_bps, &state.plan.promotion_steps_bps);
            if next_weight >= 10_000 || state.stage == RolloutStage::Promoted {
                state.stage = RolloutStage::Promoted;
                state.current_weight_bps = 10_000;
                state.loader_state.previous_artifact_id =
                    Some(state.loader_state.active_artifact_id.clone());
                state.loader_state.active_artifact_id = state.plan.artifact_id.clone();
                state.loader_state.hot_swap_state = HotSwapState::ActiveCandidate;
                return RolloutDecision {
                    action: RolloutAction::Promote,
                    state,
                    reason: "passed evidence reached full rollout".to_string(),
                };
            }

            state.stage = RolloutStage::Canary;
            state.current_weight_bps = next_weight;
            state.loader_state.hot_swap_state = HotSwapState::ActiveCandidate;
            return RolloutDecision {
                action: RolloutAction::AdvanceCanary,
                state,
                reason: "passed evidence advanced canary weight".to_string(),
            };
        }

        RolloutDecision {
            action: RolloutAction::StayShadow,
            state,
            reason: "waiting for passing scorecard evidence".to_string(),
        }
    }

    pub fn decision_event(&self, decision: &RolloutDecision) -> EventEnvelope {
        self.decision_event_with_evidence(decision, &[])
    }

    fn decision_event_with_evidence(
        &self,
        decision: &RolloutDecision,
        triggering_evidence_refs: &[String],
    ) -> EventEnvelope {
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            self.node_id.clone(),
            Source::new("rollout.controller").expect("valid source"),
            EventKind::new(crate::ROLLOUT_TRANSITION_COMPLETED_EVENT_KIND)
                .expect("valid event kind"),
            Salience::new(if decision.action == RolloutAction::RollBack {
                0.9
            } else {
                0.76
            })
            .expect("valid salience"),
            Confidence::new(0.95).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(self.node_id.clone())
        .with_payload(serde_json::json!({
            "rollout_plan_id": decision.state.plan.rollout_plan_id,
            "artifact_id": decision.state.plan.artifact_id,
            "module_id": decision.state.plan.module_id,
            "target_ring": decision.state.plan.target_ring,
            "action": decision.action,
            "stage": decision.state.stage,
            "current_weight_bps": decision.state.current_weight_bps,
            "active_artifact_id": decision.state.loader_state.active_artifact_id,
            "previous_artifact_id": decision.state.loader_state.previous_artifact_id,
            "candidate_artifact_id": decision.state.loader_state.candidate_artifact_id,
            "shadow_artifact_id": decision.state.loader_state.shadow_artifact_id,
            "hot_swap_state": decision.state.loader_state.hot_swap_state,
            "triggering_evidence_refs": triggering_evidence_refs,
            "reason": decision.reason,
        }))
    }

    pub fn record_decision(
        &self,
        paths: &SelfRolloutPaths,
        decision: &RolloutDecision,
        triggering_evidence_refs: impl IntoIterator<Item = String>,
    ) -> Result<RecordedRolloutTransition, crate::RolloutError> {
        let recorded =
            record_rollout_transition(paths, &self.node_id, decision, triggering_evidence_refs)?;
        write_rollout_runtime_state(paths, &decision.state)?;
        append_rollout_transition_event(
            paths,
            self.decision_event_with_evidence(decision, &recorded.record.triggering_evidence_refs),
        )?;
        Ok(recorded)
    }

    pub fn advance_recorded_rollout(
        &self,
        paths: &SelfRolloutPaths,
        observation: RolloutObservation,
        triggering_evidence_refs: impl IntoIterator<Item = String>,
    ) -> Result<RecordedRolloutTransition, crate::RolloutError> {
        let state = read_rollout_runtime_state(paths)?.ok_or_else(|| {
            crate::RolloutError::MissingArtifact {
                artifact: rollout_runtime_state_path(paths).display().to_string(),
            }
        })?;
        let decision = self.advance(state, observation);
        self.record_decision(paths, &decision, triggering_evidence_refs)
    }

    pub fn advance_rollout_from_shadow_telemetry<S>(
        &self,
        data_dir_root: impl AsRef<std::path::Path>,
        paths: &SelfRolloutPaths,
        event_store: &S,
        options: RolloutTelemetryAdvanceOptions,
    ) -> Result<RolloutTelemetryAdvanceResult, crate::RolloutError>
    where
        S: EventStore,
    {
        self.advance_rollout_from_telemetry_kind(
            data_dir_root,
            paths,
            event_store,
            options,
            MODULE_SHADOW_DECISION_EVENT_KIND,
            "module-shadow-telemetry",
        )
    }

    pub fn advance_rollout_from_live_telemetry<S>(
        &self,
        data_dir_root: impl AsRef<std::path::Path>,
        paths: &SelfRolloutPaths,
        event_store: &S,
        options: RolloutTelemetryAdvanceOptions,
    ) -> Result<RolloutTelemetryAdvanceResult, crate::RolloutError>
    where
        S: EventStore,
    {
        let Some(state) = read_rollout_runtime_state(paths)? else {
            return Ok(rollout_telemetry_result(
                RolloutTelemetryAdvanceStatus::NoRuntimeState,
                0,
                0,
                None,
                None,
            ));
        };
        let (telemetry_kind, evidence_kind) = if state.stage == RolloutStage::Canary {
            (
                MODULE_INVOCATION_DECISION_EVENT_KIND,
                "module-canary-telemetry",
            )
        } else {
            (MODULE_SHADOW_DECISION_EVENT_KIND, "module-shadow-telemetry")
        };
        self.advance_rollout_from_telemetry_state(
            data_dir_root,
            paths,
            event_store,
            options,
            state,
            telemetry_kind,
            evidence_kind,
        )
    }

    fn advance_rollout_from_telemetry_kind<S>(
        &self,
        data_dir_root: impl AsRef<std::path::Path>,
        paths: &SelfRolloutPaths,
        event_store: &S,
        options: RolloutTelemetryAdvanceOptions,
        telemetry_kind: &str,
        evidence_kind: &str,
    ) -> Result<RolloutTelemetryAdvanceResult, crate::RolloutError>
    where
        S: EventStore,
    {
        let Some(state) = read_rollout_runtime_state(paths)? else {
            return Ok(rollout_telemetry_result(
                RolloutTelemetryAdvanceStatus::NoRuntimeState,
                0,
                0,
                None,
                None,
            ));
        };
        self.advance_rollout_from_telemetry_state(
            data_dir_root,
            paths,
            event_store,
            options,
            state,
            telemetry_kind,
            evidence_kind,
        )
    }

    fn advance_rollout_from_telemetry_state<S>(
        &self,
        data_dir_root: impl AsRef<std::path::Path>,
        paths: &SelfRolloutPaths,
        event_store: &S,
        options: RolloutTelemetryAdvanceOptions,
        state: RolloutRuntimeState,
        telemetry_kind: &str,
        evidence_kind: &str,
    ) -> Result<RolloutTelemetryAdvanceResult, crate::RolloutError>
    where
        S: EventStore,
    {
        if rollout_stage_is_terminal(&state.stage) {
            return Ok(rollout_telemetry_result(
                RolloutTelemetryAdvanceStatus::TerminalStage,
                0,
                0,
                None,
                None,
            ));
        }
        let Some(candidate_artifact_id) = rollout_candidate_artifact_id(&state) else {
            return Ok(rollout_telemetry_result(
                RolloutTelemetryAdvanceStatus::NoCandidate,
                0,
                0,
                None,
                None,
            ));
        };

        let checkpoint = read_rollout_telemetry_advance_state(paths)?.filter(|checkpoint| {
            rollout_telemetry_checkpoint_matches(checkpoint, &state, telemetry_kind)
        });
        let events = rollout_telemetry_events_from_event_store(
            event_store,
            telemetry_kind,
            checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.last_consumed_sequence),
            options.event_limit.max(1),
        )?;
        let last_event_sequence = events.iter().map(|stored| stored.sequence).max();
        let (
            scorecard,
            observed_event_count,
            ignored_event_count,
            incident_event_ids,
            user_correction_count,
            rollback_triggers,
        ) = if telemetry_kind == MODULE_INVOCATION_DECISION_EVENT_KIND {
            let report = ModuleCanaryTelemetryReport::from_stored_events(
                state.plan.module_id.clone(),
                Some(&candidate_artifact_id),
                &events,
            );
            let failure_report = ModuleRuntimeFailureReport::from_stored_events(
                &state.plan.module_id,
                Some(&candidate_artifact_id),
                &events,
            );
            let mut scorecard = canary_telemetry_scorecard_from_report(
                &report,
                Some(state.loader_state.active_artifact_id.clone()),
                Some(candidate_artifact_id.clone()),
                options.minimum_event_count.max(1),
                options.maximum_candidate_latency_p95_ms,
            );
            append_runtime_failure_metrics(&mut scorecard, &failure_report);
            (
                scorecard,
                report.candidate_primary_count,
                report.ignored_event_count,
                failure_report.incident_event_ids,
                report.candidate_user_correction_count.min(u32::MAX as u64) as u32,
                failure_report.rollback_triggers,
            )
        } else {
            let report = ModuleShadowTelemetryReport::from_stored_events(
                state.plan.module_id.clone(),
                &events,
            );
            (
                shadow_telemetry_scorecard_from_report(
                    &report,
                    Some(state.loader_state.active_artifact_id.clone()),
                    Some(candidate_artifact_id.clone()),
                ),
                report.event_count,
                report.ignored_event_count,
                Vec::new(),
                0,
                Vec::new(),
            )
        };
        if observed_event_count < options.minimum_event_count.max(1)
            && incident_event_ids.is_empty()
            && user_correction_count == 0
            && rollback_triggers.is_empty()
        {
            return Ok(rollout_telemetry_result(
                RolloutTelemetryAdvanceStatus::WaitingForTelemetry,
                observed_event_count,
                ignored_event_count,
                last_event_sequence,
                None,
            ));
        }

        let observation = RolloutObservation {
            scorecard: Some(scorecard),
            incident_event_ids,
            user_correction_count,
            rollback_triggers,
        };
        let active_rollback_triggers = active_rollback_triggers(&state, &observation);
        let evidence_refs = rollout_telemetry_evidence_refs(RolloutTelemetryEvidenceInputs {
            evidence_kind,
            telemetry_kind,
            after_sequence: checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.last_consumed_sequence),
            through_sequence: last_event_sequence,
            limit: options.event_limit.max(1),
            observed_event_count,
            ignored_event_count,
            state: &state,
            observation: &observation,
            active_rollback_triggers: &active_rollback_triggers,
        });
        let decision = self.advance(state.clone(), observation);
        if rollout_telemetry_promotion_is_blocked(&decision, &options) {
            return Ok(rollout_telemetry_result(
                RolloutTelemetryAdvanceStatus::PromotionGateBlocked,
                observed_event_count,
                ignored_event_count,
                last_event_sequence,
                None,
            ));
        }
        let recorded = self.record_decision(paths, &decision, evidence_refs)?;
        let applied = self.apply_recorded_transition(data_dir_root, &recorded)?;
        write_rollout_telemetry_advance_state(
            paths,
            &RolloutTelemetryAdvanceState {
                rollout_plan_id: state.plan.rollout_plan_id.clone(),
                module_id: state.plan.module_id.clone(),
                candidate_artifact_id,
                telemetry_kind: telemetry_kind.to_string(),
                last_consumed_sequence: last_event_sequence,
                last_transition_record_id: Some(applied.transition.record_id.clone()),
                updated_at: rollout_timestamp_now(),
            },
        )?;
        Ok(rollout_telemetry_result(
            RolloutTelemetryAdvanceStatus::AppliedTransition,
            observed_event_count,
            ignored_event_count,
            last_event_sequence,
            Some(applied),
        ))
    }

    pub fn apply_recorded_transition(
        &self,
        data_dir_root: impl AsRef<std::path::Path>,
        recorded: &RecordedRolloutTransition,
    ) -> Result<AppliedRolloutTransition, crate::RolloutError> {
        if recorded.record.node_id != self.node_id {
            return Err(crate::RolloutError::InvalidTransition {
                detail: format!(
                    "rollout transition targets {}, but controller belongs to {}",
                    recorded.record.node_id, self.node_id
                ),
            });
        }
        let data_dir_root = data_dir_root.as_ref();
        let module_state = ModuleLoaderRuntimeState {
            node_id: recorded.record.node_id.clone(),
            activation_id: recorded.record.rollout_plan_id.clone(),
            module_id: recorded.record.module_id.clone(),
            loader_state: ModuleLoaderState {
                module_id: recorded.record.module_id.clone(),
                active_artifact_id: recorded.record.active_artifact_id.clone(),
                previous_artifact_id: recorded.record.previous_artifact_id.clone(),
                shadow_artifact_id: recorded.record.shadow_artifact_id.clone(),
                candidate_artifact_id: recorded.record.candidate_artifact_id.clone(),
                hot_swap_state: recorded.record.hot_swap_state.clone(),
            },
            traffic_weight_bps: applied_traffic_weight_bps(
                &recorded.record.action,
                recorded.record.current_weight_bps,
            ),
            last_loader_action: rollout_action_label(&recorded.record.action).to_string(),
            last_loader_stage: rollout_stage_label(&recorded.record.stage).to_string(),
            last_transition_record_id: recorded.record.record_id.clone(),
            reason: recorded.record.reason.clone(),
            applied_at: module_loader_applied_at_now(),
        };
        let module_state_path = write_module_loader_runtime_state(data_dir_root, &module_state)
            .map_err(|source| crate::RolloutError::Object {
                path: module_loader_state_path(data_dir_root, &module_state.module_id)
                    .display()
                    .to_string(),
                detail: source.to_string(),
            })?;
        Ok(AppliedRolloutTransition {
            transition: recorded.record.clone(),
            transition_path: recorded.path.clone(),
            module_state,
            module_state_path,
        })
    }
}

fn append_rollout_transition_event(
    paths: &SelfRolloutPaths,
    event: EventEnvelope,
) -> Result<(), crate::RolloutError> {
    let data_dir = paths.root.parent().ok_or_else(|| crate::RolloutError::Io {
        path: paths.root.display().to_string(),
        detail: "rollout root has no parent data directory".to_string(),
    })?;
    let path = data_dir.join("events").join("event-log.jsonl");
    let mut store = sinomo_core::events::JsonlEventStore::new(&path);
    store
        .append(event)
        .map_err(|source| crate::RolloutError::Json {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    Ok(())
}

fn verified_artifact_rollout_blocker(
    node_id: &NodeId,
    request: &VerifiedArtifactRolloutRequest,
) -> Option<String> {
    if &request.node_id != node_id {
        return Some(format!(
            "rollout request targets {}, but controller belongs to {}",
            request.node_id, node_id
        ));
    }
    if let Err(error) = verify_artifact_signature(&request.artifact) {
        return Some(format!(
            "artifact signature verification failed for verified artifact rollout: {error}"
        ));
    }
    let signer_authorization =
        authorize_artifact_signature(&request.artifact, &request.trusted_signers);
    if !signer_authorization.is_authorized() {
        return Some(format!(
            "artifact signer is not authorized for verified artifact rollout: {:?}",
            signer_authorization.failed_checks()
        ));
    }
    if request.rollout_plan.artifact_id != request.artifact.artifact_id {
        return Some("rollout plan artifact id does not match artifact manifest".to_string());
    }
    if request.rollout_plan.module_id != request.artifact.module_id {
        return Some("rollout plan module id does not match artifact manifest".to_string());
    }
    if request.rollout_plan.target_ring != request.artifact.target_ring {
        return Some("rollout plan target ring does not match artifact manifest".to_string());
    }
    if !request.rollout_plan.target_nodes.is_empty()
        && !request
            .rollout_plan
            .target_nodes
            .iter()
            .any(|target| target == node_id)
    {
        return Some(format!("rollout plan does not target node {node_id}"));
    }
    if request.policy_decision.target_artifact_id != request.artifact.artifact_id {
        return Some(
            "policy decision target artifact id does not match artifact manifest".to_string(),
        );
    }
    if request.policy_decision.target_ring != request.artifact.target_ring {
        return Some("policy decision target ring does not match artifact manifest".to_string());
    }
    if request.release_material_hash.trim().is_empty() {
        return Some("release material hash is empty".to_string());
    }
    if request.evidence_refs.is_empty() {
        return Some("verified artifact rollout carries no evidence refs".to_string());
    }
    if !artifact_references_release_evidence(&request.artifact, request) {
        return Some(
            "artifact provenance does not reference the verified release evidence".to_string(),
        );
    }
    None
}

fn artifact_references_release_evidence(
    artifact: &ArtifactManifest,
    request: &VerifiedArtifactRolloutRequest,
) -> bool {
    let registry_ref = format!("registry://artifact-evidence/{}", request.release_id);
    artifact.provenance.attestations.iter().any(|attestation| {
        attestation == &registry_ref
            || request
                .evidence_refs
                .iter()
                .any(|reference| attestation == reference)
    })
}

fn active_rollback_triggers(
    state: &RolloutRuntimeState,
    observation: &RolloutObservation,
) -> Vec<RollbackTrigger> {
    let mut observed = observation.rollback_triggers.clone();
    if !observation.incident_event_ids.is_empty() {
        observed.push(RollbackTrigger::PolicyIncident);
    }
    if observation.user_correction_count > 0 {
        observed.push(RollbackTrigger::UserCorrectionSpike);
    }
    if observation
        .scorecard
        .as_ref()
        .is_some_and(|scorecard| scorecard.outcome == EvaluationOutcome::Failed)
    {
        observed.push(RollbackTrigger::ScoreRegression);
    }

    let mut active = Vec::new();
    for trigger in observed {
        if state.plan.rollback_policy.triggers.contains(&trigger) && !active.contains(&trigger) {
            active.push(trigger);
        }
    }
    active
}

fn rollback_trigger_labels(triggers: &[RollbackTrigger]) -> String {
    triggers
        .iter()
        .map(rollback_trigger_label)
        .collect::<Vec<_>>()
        .join(", ")
}

fn rollback_trigger_label(trigger: &RollbackTrigger) -> &'static str {
    match trigger {
        RollbackTrigger::BuildFailure => "build-failure",
        RollbackTrigger::LoadFailure => "load-failure",
        RollbackTrigger::Panic => "panic",
        RollbackTrigger::Timeout => "timeout",
        RollbackTrigger::ResourceBudgetViolation => "resource-budget-violation",
        RollbackTrigger::NewDeniedCapabilityRequest => "new-denied-capability-request",
        RollbackTrigger::PolicyIncident => "policy-incident",
        RollbackTrigger::ScoreRegression => "score-regression",
        RollbackTrigger::EmergencyStop => "emergency-stop",
        RollbackTrigger::UserCorrectionSpike => "user-correction-spike",
        RollbackTrigger::NodeHealthDegradation => "node-health-degradation",
    }
}

fn next_weight(current: u16, steps: &[u16]) -> u16 {
    steps
        .iter()
        .copied()
        .find(|step| *step > current)
        .unwrap_or(10_000)
}

fn applied_traffic_weight_bps(action: &RolloutAction, current_weight_bps: u16) -> u16 {
    match action {
        RolloutAction::StayShadow | RolloutAction::RollBack | RolloutAction::Quarantine => 0,
        RolloutAction::Promote => 10_000,
        RolloutAction::AdvanceCanary | RolloutAction::GuardR1Activation => {
            current_weight_bps.min(10_000)
        }
    }
}

fn rollout_action_label(action: &RolloutAction) -> &'static str {
    match action {
        RolloutAction::StayShadow => "stay-shadow",
        RolloutAction::AdvanceCanary => "advance-canary",
        RolloutAction::Promote => "promote",
        RolloutAction::GuardR1Activation => "guard-r1-activation",
        RolloutAction::RollBack => "rollback",
        RolloutAction::Quarantine => "quarantine",
    }
}

fn rollout_stage_label(stage: &RolloutStage) -> &'static str {
    match stage {
        RolloutStage::Shadow => "shadow",
        RolloutStage::Canary => "canary",
        RolloutStage::GuardedR1 => "guarded-r1",
        RolloutStage::Promoted => "promoted",
        RolloutStage::RolledBack => "rolled-back",
        RolloutStage::Quarantined => "quarantined",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RolloutPlanId, ScorecardId};
    use sinomo_core::events::EventStore;
    use sinomo_core::{
        ActorKind, ActorRef, ArtifactManifest, ArtifactProvenance, CapabilityDiff, EvaluationGate,
        EvaluationGateResult, ModuleManifest, PolicyAction, PolicyDecision, RollbackPolicy,
        RollbackScope,
    };
    use sinomo_module::{
        ModuleId, read_module_loader_runtime_state, sign_artifact_manifest_with_node_identity,
    };

    #[test]
    fn passed_shadow_scorecard_advances_r3_canary() {
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let plan = RolloutPlan {
            rollout_plan_id: RolloutPlanId::from_legacy_named("rollout_candidate")
                .expect("rollout"),
            artifact_id: ArtifactId::from_legacy_named("artifact_candidate").expect("artifact"),
            module_id: ModuleId::from_legacy_named("module.trigger.silence_policy")
                .expect("module"),
            target_ring: TrustRing::R3SandboxedModule,
            strategy: RolloutStrategy::Shadow,
            target_nodes: vec![node.clone()],
            initial_weight_bps: 0,
            promotion_steps_bps: vec![500, 2_500],
            rollback_policy: RollbackPolicy::default(),
        };
        let controller = RolloutController::new(node);
        let state = controller.initial_state(
            plan,
            ArtifactId::from_legacy_named("artifact_active").expect("artifact"),
        );
        let decision = controller.advance(
            state,
            RolloutObservation {
                scorecard: Some(EvaluationScorecard {
                    scorecard_id: ScorecardId::from_legacy_named("score_candidate").expect("score"),
                    baseline_artifact_id: None,
                    candidate_artifact_id: Some(
                        ArtifactId::from_legacy_named("artifact_candidate").expect("artifact"),
                    ),
                    outcome: EvaluationOutcome::Passed,
                    gates: vec![EvaluationGateResult {
                        gate: EvaluationGate::ReplayTests,
                        outcome: EvaluationOutcome::Passed,
                        summary: "ok".to_string(),
                    }],
                    metrics: Vec::new(),
                }),
                incident_event_ids: Vec::new(),
                user_correction_count: 0,
                rollback_triggers: Vec::new(),
            },
        );

        assert_eq!(decision.action, RolloutAction::AdvanceCanary);
        assert_eq!(decision.state.current_weight_bps, 500);
    }

    #[test]
    fn shadow_telemetry_scorecard_from_event_store_summarizes_candidate_events() {
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let controller = RolloutController::new(node.clone());
        let state = controller.initial_state(
            rollout_plan(&node, &artifact_id, &module_id),
            active_artifact_id.clone(),
        );
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        event_store
            .append(module_shadow_decision_event(
                &node,
                &module_id,
                &active_artifact_id,
                &artifact_id,
                true,
            ))
            .expect("append shadow event");

        let scorecard = shadow_telemetry_scorecard_from_event_store(&state, &event_store, 10)
            .expect("scorecard");

        assert_eq!(scorecard.outcome, EvaluationOutcome::Passed);
        assert_eq!(scorecard.baseline_artifact_id, Some(active_artifact_id));
        assert_eq!(scorecard.candidate_artifact_id, Some(artifact_id));
        assert_eq!(scorecard.gates[0].gate, EvaluationGate::ShadowMode);
    }

    #[test]
    fn canary_telemetry_scorecard_from_event_store_summarizes_candidate_primary_events() {
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let controller = RolloutController::new(node.clone());
        let mut state = controller.initial_state(
            rollout_plan(&node, &artifact_id, &module_id),
            active_artifact_id.clone(),
        );
        state.stage = RolloutStage::Canary;
        state.current_weight_bps = 500;
        state.loader_state.hot_swap_state = HotSwapState::ActiveCandidate;
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        event_store
            .append(module_invocation_decision_event(
                &node,
                &module_id,
                &artifact_id,
                "candidate",
                None,
            ))
            .expect("append canary event");

        let scorecard =
            canary_telemetry_scorecard_from_event_store(&state, &event_store, 10, 1, None)
                .expect("scorecard");

        assert_eq!(scorecard.outcome, EvaluationOutcome::Passed);
        assert_eq!(scorecard.baseline_artifact_id, Some(active_artifact_id));
        assert_eq!(scorecard.candidate_artifact_id, Some(artifact_id));
        assert_eq!(scorecard.gates[0].gate, EvaluationGate::CanaryAnalysis);
        assert!(scorecard.metrics.iter().any(|metric| {
            metric.name == "module_canary_candidate_primary_count"
                && metric.value == sinomo_core::MetricValue::Integer(1)
        }));
    }

    #[test]
    fn shadow_telemetry_auto_advance_checkpoints_consumed_events() {
        let root = unique_temp("sinomo-rollout-telemetry-auto-advance");
        let paths = SelfRolloutPaths::new(&root);
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let controller = RolloutController::new(node.clone());
        let state = controller.initial_state(
            rollout_plan(&node, &artifact_id, &module_id),
            active_artifact_id.clone(),
        );
        crate::write_rollout_runtime_state(&paths, &state).expect("runtime state");
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        event_store
            .append(module_shadow_decision_event(
                &node,
                &module_id,
                &active_artifact_id,
                &artifact_id,
                true,
            ))
            .expect("append shadow event");

        let result = controller
            .advance_rollout_from_shadow_telemetry(
                &root,
                &paths,
                &event_store,
                RolloutTelemetryAdvanceOptions {
                    automatic_promotion_enabled: true,
                    ..RolloutTelemetryAdvanceOptions::default()
                },
            )
            .expect("advance from telemetry");

        assert_eq!(
            result.status,
            RolloutTelemetryAdvanceStatus::AppliedTransition
        );
        assert_eq!(result.observed_event_count, 1);
        assert_eq!(
            result
                .applied_transition
                .as_ref()
                .expect("applied")
                .transition
                .action,
            RolloutAction::AdvanceCanary
        );
        let checkpoint = crate::read_rollout_telemetry_advance_state(&paths)
            .expect("read checkpoint")
            .expect("checkpoint");
        assert_eq!(
            checkpoint.last_consumed_sequence,
            result.last_event_sequence
        );
        assert_eq!(checkpoint.rollout_plan_id, state.plan.rollout_plan_id);

        let second = controller
            .advance_rollout_from_shadow_telemetry(
                &root,
                &paths,
                &event_store,
                RolloutTelemetryAdvanceOptions {
                    automatic_promotion_enabled: true,
                    ..RolloutTelemetryAdvanceOptions::default()
                },
            )
            .expect("second telemetry tick");

        assert_eq!(
            second.status,
            RolloutTelemetryAdvanceStatus::WaitingForTelemetry
        );
        assert_eq!(second.observed_event_count, 0);
        assert!(second.applied_transition.is_none());
    }

    #[test]
    fn shadow_telemetry_default_blocks_promotion_until_explicitly_enabled() {
        let root = unique_temp("sinomo-rollout-telemetry-default-blocks-promotion");
        let paths = SelfRolloutPaths::new(&root);
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let controller = RolloutController::new(node.clone());
        let state = controller.initial_state(
            rollout_plan(&node, &artifact_id, &module_id),
            active_artifact_id.clone(),
        );
        crate::write_rollout_runtime_state(&paths, &state).expect("runtime state");
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        event_store
            .append(module_shadow_decision_event(
                &node,
                &module_id,
                &active_artifact_id,
                &artifact_id,
                true,
            ))
            .expect("append shadow event");

        let result = controller
            .advance_rollout_from_shadow_telemetry(
                &root,
                &paths,
                &event_store,
                RolloutTelemetryAdvanceOptions::default(),
            )
            .expect("advance from telemetry");

        assert_eq!(
            result.status,
            RolloutTelemetryAdvanceStatus::PromotionGateBlocked
        );
        assert_eq!(result.observed_event_count, 1);
        assert!(result.applied_transition.is_none());
        assert!(
            crate::read_rollout_telemetry_advance_state(&paths)
                .expect("checkpoint")
                .is_none()
        );
        assert_eq!(
            crate::read_rollout_runtime_state(&paths)
                .expect("runtime state")
                .expect("runtime state"),
            state
        );
    }

    #[test]
    fn live_telemetry_auto_advance_uses_canary_events_in_canary_stage() {
        let root = unique_temp("sinomo-rollout-canary-telemetry-auto-advance");
        let paths = SelfRolloutPaths::new(&root);
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let controller = RolloutController::new(node.clone());
        let mut state = controller.initial_state(
            rollout_plan(&node, &artifact_id, &module_id),
            active_artifact_id,
        );
        state.stage = RolloutStage::Canary;
        state.current_weight_bps = 500;
        state.loader_state.hot_swap_state = HotSwapState::ActiveCandidate;
        crate::write_rollout_runtime_state(&paths, &state).expect("runtime state");
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        event_store
            .append(module_invocation_decision_event(
                &node,
                &module_id,
                &artifact_id,
                "candidate",
                None,
            ))
            .expect("append canary event");

        let result = controller
            .advance_rollout_from_live_telemetry(
                &root,
                &paths,
                &event_store,
                RolloutTelemetryAdvanceOptions {
                    automatic_promotion_enabled: true,
                    maximum_automatic_weight_bps: Some(2_500),
                    ..RolloutTelemetryAdvanceOptions::default()
                },
            )
            .expect("advance from telemetry");

        assert_eq!(
            result.status,
            RolloutTelemetryAdvanceStatus::AppliedTransition
        );
        assert_eq!(result.observed_event_count, 1);
        assert_eq!(
            result
                .applied_transition
                .as_ref()
                .expect("applied")
                .transition
                .action,
            RolloutAction::AdvanceCanary
        );
        assert_eq!(
            result
                .applied_transition
                .as_ref()
                .expect("applied")
                .transition
                .current_weight_bps,
            2_500
        );
        let checkpoint = crate::read_rollout_telemetry_advance_state(&paths)
            .expect("read checkpoint")
            .expect("checkpoint");
        assert_eq!(
            checkpoint.telemetry_kind,
            MODULE_INVOCATION_DECISION_EVENT_KIND
        );
        assert_eq!(
            checkpoint.last_consumed_sequence,
            result.last_event_sequence
        );
    }

    #[test]
    fn live_telemetry_auto_rollback_uses_candidate_user_corrections_in_canary_stage() {
        let root = unique_temp("sinomo-rollout-canary-user-correction-rollback");
        let paths = SelfRolloutPaths::new(&root);
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let controller = RolloutController::new(node.clone());
        let mut state = controller.initial_state(
            rollout_plan(&node, &artifact_id, &module_id),
            active_artifact_id,
        );
        state.stage = RolloutStage::Canary;
        state.current_weight_bps = 500;
        state.loader_state.hot_swap_state = HotSwapState::ActiveCandidate;
        crate::write_rollout_runtime_state(&paths, &state).expect("runtime state");
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        event_store
            .append(module_user_correction_event(
                &node,
                &module_id,
                &artifact_id,
            ))
            .expect("append correction event");

        let result = controller
            .advance_rollout_from_live_telemetry(
                &root,
                &paths,
                &event_store,
                RolloutTelemetryAdvanceOptions {
                    minimum_event_count: 10,
                    maximum_automatic_weight_bps: Some(2_500),
                    ..RolloutTelemetryAdvanceOptions::default()
                },
            )
            .expect("advance from telemetry");

        assert_eq!(
            result.status,
            RolloutTelemetryAdvanceStatus::AppliedTransition
        );
        assert_eq!(result.observed_event_count, 0);
        let applied = result.applied_transition.as_ref().expect("applied");
        assert_eq!(applied.transition.action, RolloutAction::RollBack);
        assert!(applied.transition.reason.contains("user-correction-spike"));
        let evidence_refs = &applied.transition.triggering_evidence_refs;
        assert!(
            evidence_refs
                .iter()
                .any(|reference| reference == "rollout-telemetry://kind/module.invocation.decision")
        );
        assert!(
            evidence_refs
                .iter()
                .any(|reference| reference == "rollout-telemetry://user-corrections?count=1")
        );
        assert!(evidence_refs.iter().any(|reference| reference
            == "rollout-telemetry://active-rollback-trigger/user-correction-spike"));
    }

    #[test]
    fn live_telemetry_auto_rollback_uses_candidate_runtime_failures_in_canary_stage() {
        let root = unique_temp("sinomo-rollout-canary-runtime-failure-rollback");
        let paths = SelfRolloutPaths::new(&root);
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let controller = RolloutController::new(node.clone());
        let mut state = controller.initial_state(
            rollout_plan(&node, &artifact_id, &module_id),
            active_artifact_id,
        );
        state.stage = RolloutStage::Canary;
        state.current_weight_bps = 500;
        state.loader_state.hot_swap_state = HotSwapState::ActiveCandidate;
        crate::write_rollout_runtime_state(&paths, &state).expect("runtime state");
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        event_store
            .append(module_runtime_failure_event(
                &node,
                &module_id,
                &artifact_id,
                "candidate",
                MODULE_TIMEOUT_EVENT_KIND,
            ))
            .expect("append timeout event");

        let result = controller
            .advance_rollout_from_live_telemetry(
                &root,
                &paths,
                &event_store,
                RolloutTelemetryAdvanceOptions {
                    minimum_event_count: 10,
                    maximum_automatic_weight_bps: Some(2_500),
                    ..RolloutTelemetryAdvanceOptions::default()
                },
            )
            .expect("advance from telemetry");

        assert_eq!(
            result.status,
            RolloutTelemetryAdvanceStatus::AppliedTransition
        );
        assert_eq!(result.observed_event_count, 0);
        let applied = result.applied_transition.as_ref().expect("applied");
        assert_eq!(applied.transition.action, RolloutAction::RollBack);
        assert!(applied.transition.reason.contains("timeout"));
        let evidence_refs = &applied.transition.triggering_evidence_refs;
        assert!(
            evidence_refs
                .iter()
                .any(|reference| reference == "rollout-telemetry://kind/module.invocation.decision")
        );
        assert!(
            evidence_refs
                .iter()
                .any(|reference| reference
                    == "rollout-telemetry://observed-rollback-trigger/timeout")
        );
        assert!(
            evidence_refs
                .iter()
                .any(|reference| reference == "rollout-telemetry://active-rollback-trigger/timeout")
        );
        assert!(
            evidence_refs
                .iter()
                .any(|reference| reference.starts_with("rollout-telemetry://counts?observed=0&"))
        );
    }

    #[test]
    fn runtime_failure_report_maps_candidate_events_to_rollback_triggers() {
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        for kind in [
            MODULE_CRASH_EVENT_KIND,
            MODULE_TIMEOUT_EVENT_KIND,
            MODULE_POLICY_BLOCKED_EVENT_KIND,
        ] {
            event_store
                .append(module_runtime_failure_event(
                    &node,
                    &module_id,
                    &candidate_artifact_id,
                    "candidate",
                    kind,
                ))
                .expect("append candidate failure");
        }
        event_store
            .append(module_runtime_failure_event(
                &node,
                &module_id,
                &active_artifact_id,
                "active",
                MODULE_TIMEOUT_EVENT_KIND,
            ))
            .expect("append active failure");
        let events = event_store
            .replay(ReplayQuery {
                direction: ReplayDirection::Reverse,
                limit: Some(10),
                ..ReplayQuery::default()
            })
            .expect("replay events");

        let report = ModuleRuntimeFailureReport::from_stored_events(
            &module_id,
            Some(&candidate_artifact_id),
            &events,
        );

        assert_eq!(report.incident_event_ids.len(), 1);
        assert!(report.rollback_triggers.contains(&RollbackTrigger::Panic));
        assert!(report.rollback_triggers.contains(&RollbackTrigger::Timeout));
        assert!(
            report
                .rollback_triggers
                .contains(&RollbackTrigger::PolicyIncident)
        );
        assert_eq!(report.panic_count, 1);
        assert_eq!(report.timeout_count, 1);
        assert_eq!(report.policy_blocked_count, 1);
        assert_eq!(report.ignored_event_count, 1);
    }

    #[test]
    fn runtime_failure_report_maps_timed_out_candidate_invocation_to_timeout_trigger() {
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        event_store
            .append(module_invocation_decision_event(
                &node,
                &module_id,
                &candidate_artifact_id,
                "candidate",
                Some("local command artifact artifact_candidate timed out after 20ms"),
            ))
            .expect("append invocation failure");
        let events = event_store
            .replay(ReplayQuery {
                direction: ReplayDirection::Reverse,
                limit: Some(10),
                ..ReplayQuery::default()
            })
            .expect("replay events");

        let report = ModuleRuntimeFailureReport::from_stored_events(
            &module_id,
            Some(&candidate_artifact_id),
            &events,
        );

        assert!(report.rollback_triggers.contains(&RollbackTrigger::Timeout));
        assert_eq!(report.timeout_count, 1);
        assert_eq!(report.ignored_event_count, 0);
    }

    #[test]
    fn live_telemetry_auto_rollback_uses_canary_latency_gate() {
        let root = unique_temp("sinomo-rollout-canary-latency-rollback");
        let paths = SelfRolloutPaths::new(&root);
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let controller = RolloutController::new(node.clone());
        let mut state = controller.initial_state(
            rollout_plan(&node, &artifact_id, &module_id),
            active_artifact_id.clone(),
        );
        state.stage = RolloutStage::Canary;
        state.current_weight_bps = 500;
        state.loader_state.hot_swap_state = HotSwapState::ActiveCandidate;
        crate::write_rollout_runtime_state(&paths, &state).expect("runtime state");
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        let mut event =
            module_invocation_decision_event(&node, &module_id, &artifact_id, "candidate", None);
        event.payload["primary_latency_ms"] = serde_json::json!(250);
        event_store.append(event).expect("append canary event");

        let result = controller
            .advance_rollout_from_live_telemetry(
                &root,
                &paths,
                &event_store,
                RolloutTelemetryAdvanceOptions {
                    maximum_candidate_latency_p95_ms: Some(200),
                    maximum_automatic_weight_bps: Some(2_500),
                    ..RolloutTelemetryAdvanceOptions::default()
                },
            )
            .expect("advance from telemetry");

        assert_eq!(
            result.status,
            RolloutTelemetryAdvanceStatus::AppliedTransition
        );
        let applied = result.applied_transition.as_ref().expect("applied");
        assert_eq!(applied.transition.action, RolloutAction::RollBack);
        assert_eq!(applied.transition.current_weight_bps, 0);
        assert_eq!(
            applied.module_state.loader_state.hot_swap_state,
            HotSwapState::RollingBack
        );
        assert_eq!(
            applied.module_state.loader_state.active_artifact_id,
            active_artifact_id
        );
    }

    #[test]
    fn shadow_telemetry_auto_advance_respects_promotion_gate_config() {
        let root = unique_temp("sinomo-rollout-telemetry-gated-advance");
        let paths = SelfRolloutPaths::new(&root);
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let controller = RolloutController::new(node.clone());
        let state = controller.initial_state(
            rollout_plan(&node, &artifact_id, &module_id),
            active_artifact_id.clone(),
        );
        crate::write_rollout_runtime_state(&paths, &state).expect("runtime state");
        let mut event_store = sinomo_core::events::InMemoryEventStore::new();
        event_store
            .append(module_shadow_decision_event(
                &node,
                &module_id,
                &active_artifact_id,
                &artifact_id,
                true,
            ))
            .expect("append shadow event");

        let result = controller
            .advance_rollout_from_shadow_telemetry(
                &root,
                &paths,
                &event_store,
                RolloutTelemetryAdvanceOptions {
                    automatic_promotion_enabled: true,
                    maximum_automatic_weight_bps: Some(250),
                    ..RolloutTelemetryAdvanceOptions::default()
                },
            )
            .expect("advance from telemetry");

        assert_eq!(
            result.status,
            RolloutTelemetryAdvanceStatus::PromotionGateBlocked
        );
        assert_eq!(result.observed_event_count, 1);
        assert!(result.applied_transition.is_none());
        assert_eq!(
            crate::read_rollout_runtime_state(&paths)
                .expect("runtime state")
                .expect("runtime state"),
            state
        );
        assert!(
            crate::read_rollout_telemetry_advance_state(&paths)
                .expect("checkpoint")
                .is_none()
        );
    }

    #[test]
    fn rollback_policy_trigger_rolls_back_and_restores_configured_artifact() {
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let restored_artifact_id =
            ArtifactId::from_legacy_named("artifact_restore").expect("restore");
        let mut plan = rollout_plan(&node, &artifact_id, &module_id);
        plan.rollback_policy = RollbackPolicy {
            scope: RollbackScope::Local,
            triggers: vec![RollbackTrigger::Timeout],
            restore_artifact_id: Some(restored_artifact_id.clone()),
        };
        let controller = RolloutController::new(node);
        let state = controller.initial_state(
            plan,
            ArtifactId::from_legacy_named("artifact_active").expect("active"),
        );

        let decision = controller.advance(
            state,
            RolloutObservation {
                scorecard: None,
                incident_event_ids: Vec::new(),
                user_correction_count: 0,
                rollback_triggers: vec![RollbackTrigger::Timeout],
            },
        );

        assert_eq!(decision.action, RolloutAction::RollBack);
        assert_eq!(
            decision.state.loader_state.active_artifact_id,
            restored_artifact_id
        );
        assert_eq!(decision.state.loader_state.candidate_artifact_id, None);
        assert_eq!(decision.state.loader_state.shadow_artifact_id, None);
        assert!(decision.reason.contains("timeout"));
    }

    #[test]
    fn rollback_policy_ignores_triggers_it_does_not_enable() {
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let mut plan = rollout_plan(&node, &artifact_id, &module_id);
        plan.rollback_policy.triggers = vec![RollbackTrigger::ScoreRegression];
        let controller = RolloutController::new(node);
        let state = controller.initial_state(
            plan,
            ArtifactId::from_legacy_named("artifact_active").expect("active"),
        );

        let decision = controller.advance(
            state,
            RolloutObservation {
                scorecard: None,
                incident_event_ids: Vec::new(),
                user_correction_count: 0,
                rollback_triggers: vec![RollbackTrigger::Timeout],
            },
        );

        assert_eq!(decision.action, RolloutAction::StayShadow);
        assert_eq!(
            decision.state.loader_state.candidate_artifact_id,
            Some(artifact_id)
        );
    }

    #[test]
    fn rollout_controller_persists_promotion_and_rollback_records() {
        let root = unique_temp("sinomo-rollout-transition-records");
        let paths = SelfRolloutPaths::new(&root);
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let plan = RolloutPlan {
            rollout_plan_id: RolloutPlanId::from_legacy_named("rollout_candidate")
                .expect("rollout"),
            artifact_id: artifact_id.clone(),
            module_id: module_id.clone(),
            target_ring: TrustRing::R3SandboxedModule,
            strategy: RolloutStrategy::Canary,
            target_nodes: vec![node.clone()],
            initial_weight_bps: 500,
            promotion_steps_bps: vec![10_000],
            rollback_policy: RollbackPolicy::default(),
        };
        let controller = RolloutController::new(node.clone());
        let mut promotion_state =
            controller.initial_state(plan.clone(), active_artifact_id.clone());
        promotion_state.stage = RolloutStage::Canary;
        let promotion = controller.advance(
            promotion_state,
            RolloutObservation {
                scorecard: Some(scorecard(EvaluationOutcome::Passed, artifact_id.clone())),
                incident_event_ids: Vec::new(),
                user_correction_count: 0,
                rollback_triggers: Vec::new(),
            },
        );

        let recorded_promotion = controller
            .record_decision(
                &paths,
                &promotion,
                vec!["scorecard://promotion-pass".to_string()],
            )
            .expect("promotion record");

        assert_eq!(recorded_promotion.record.action, RolloutAction::Promote);
        assert_eq!(
            recorded_promotion.record.promoted_artifact_id,
            Some(artifact_id.clone())
        );
        assert!(recorded_promotion.path.exists());
        assert!(
            std::fs::read_to_string(&recorded_promotion.path)
                .expect("promotion json")
                .contains("scorecard://promotion-pass")
        );
        assert_eq!(
            crate::read_rollout_runtime_state(&paths).expect("runtime state"),
            Some(promotion.state.clone())
        );

        let rollback_state = controller.initial_state(plan, active_artifact_id.clone());
        let rollback = controller.advance(
            rollback_state,
            RolloutObservation {
                scorecard: Some(scorecard(EvaluationOutcome::Failed, artifact_id.clone())),
                incident_event_ids: Vec::new(),
                user_correction_count: 0,
                rollback_triggers: Vec::new(),
            },
        );
        let recorded_rollback = controller
            .record_decision(
                &paths,
                &rollback,
                vec!["scorecard://rollback-fail".to_string()],
            )
            .expect("rollback record");

        assert_eq!(recorded_rollback.record.action, RolloutAction::RollBack);
        assert_eq!(
            recorded_rollback.record.failed_artifact_id,
            Some(artifact_id)
        );
        assert_eq!(
            recorded_rollback.record.restored_artifact_id,
            Some(active_artifact_id)
        );
        assert!(recorded_rollback.path.exists());
        assert_eq!(
            crate::read_rollout_runtime_state(&paths).expect("runtime state"),
            Some(rollback.state.clone())
        );
        let event_log = root.join("events").join("event-log.jsonl");
        let event_log = std::fs::read_to_string(&event_log).expect("rollout event log");
        assert_eq!(event_log.matches("rollout.transition.completed").count(), 2);
        assert!(event_log.contains("scorecard://promotion-pass"));
        assert!(event_log.contains("scorecard://rollback-fail"));
        assert!(event_log.contains("triggering_evidence_refs"));
        assert!(event_log.contains("active_artifact_id"));
    }

    #[test]
    fn verified_artifact_release_prepares_shadow_rollout() {
        let root = unique_temp("sinomo-rollout-verified-preparation");
        let paths = SelfRolloutPaths::new(&root);
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let release_id = sinomo_core::Id::from_legacy_named("release_verified").expect("release");
        let release_ref = format!("registry://artifact-evidence/{release_id}");
        let (artifact, signer) = signed_artifact_manifest(
            &artifact_id,
            &module_id,
            "blake3:candidate",
            vec![release_ref.clone()],
        );
        let request = VerifiedArtifactRolloutRequest {
            node_id: node.clone(),
            artifact,
            policy_decision: allow_policy(&artifact_id),
            rollout_plan: rollout_plan(&node, &artifact_id, &module_id),
            release_id: release_id.clone(),
            release_material_hash: "blake3:release".to_string(),
            evidence_refs: vec![release_ref],
            trusted_signers: vec![signer],
            active_artifact_id: ArtifactId::from_legacy_named("artifact_active").expect("artifact"),
        };

        let preparation = RolloutController::new(node)
            .prepare_and_record_verified_artifact_rollout(&paths, request)
            .expect("prepare verified rollout");

        assert!(preparation.accepted);
        assert_eq!(preparation.release_id, release_id);
        assert_eq!(preparation.release_material_hash, "blake3:release");
        assert!(
            preparation
                .load_decision
                .as_ref()
                .is_some_and(|decision| decision.action == LoaderAction::AcceptShadow)
        );
        let state = preparation.initial_state.expect("initial state");
        assert_eq!(state.stage, RolloutStage::Shadow);
        assert_eq!(state.plan.artifact_id, artifact_id);
        assert_eq!(
            crate::read_rollout_runtime_state(&paths).expect("runtime state"),
            Some(state)
        );
    }

    #[test]
    fn advance_recorded_rollout_consumes_persisted_runtime_state() {
        let root = unique_temp("sinomo-rollout-advance-recorded");
        let paths = SelfRolloutPaths::new(&root);
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let plan = rollout_plan(&node, &artifact_id, &module_id);
        let controller = RolloutController::new(node);
        let state = controller.initial_state(plan, active_artifact_id);
        crate::write_rollout_runtime_state(&paths, &state).expect("seed runtime state");

        let recorded = controller
            .advance_recorded_rollout(
                &paths,
                RolloutObservation {
                    scorecard: Some(scorecard(EvaluationOutcome::Passed, artifact_id)),
                    incident_event_ids: Vec::new(),
                    user_correction_count: 0,
                    rollback_triggers: Vec::new(),
                },
                vec!["scorecard://recorded-pass".to_string()],
            )
            .expect("recorded advance");

        assert_eq!(recorded.record.action, RolloutAction::AdvanceCanary);
        assert_eq!(recorded.record.current_weight_bps, 500);
        assert_eq!(
            crate::read_rollout_runtime_state(&paths)
                .expect("runtime state")
                .expect("state")
                .current_weight_bps,
            500
        );
        let transition = std::fs::read_to_string(&recorded.path).expect("transition record");
        assert!(transition.contains("scorecard://recorded-pass"));
        let event_log = std::fs::read_to_string(root.join("events").join("event-log.jsonl"))
            .expect("event log");
        assert!(event_log.contains("rollout.transition.completed"));
        assert!(event_log.contains("scorecard://recorded-pass"));
        assert!(event_log.contains("triggering_evidence_refs"));

        let applied = controller
            .apply_recorded_transition(&root, &recorded)
            .expect("apply transition");
        assert_eq!(applied.transition.record_id, recorded.record.record_id);
        assert_eq!(applied.transition_path, recorded.path);
        assert_eq!(applied.module_state.traffic_weight_bps, 500);
        assert_eq!(applied.module_state.last_loader_action, "advance-canary");
        assert_eq!(
            read_module_loader_runtime_state(&root, &applied.module_state.module_id)
                .expect("module state"),
            Some(applied.module_state)
        );
    }

    #[test]
    fn advance_recorded_rollout_requires_persisted_runtime_state() {
        let root = unique_temp("sinomo-rollout-advance-missing-state");
        let paths = SelfRolloutPaths::new(&root);
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let controller = RolloutController::new(node);

        let error = controller
            .advance_recorded_rollout(
                &paths,
                RolloutObservation {
                    scorecard: None,
                    incident_event_ids: Vec::new(),
                    user_correction_count: 0,
                    rollback_triggers: Vec::new(),
                },
                Vec::<String>::new(),
            )
            .expect_err("missing state");

        match error {
            crate::RolloutError::MissingArtifact { artifact } => {
                assert!(artifact.ends_with("rollout/runtime-state.sobj"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn apply_recorded_transition_rejects_wrong_node() {
        let root = unique_temp("sinomo-rollout-apply-wrong-node");
        let paths = SelfRolloutPaths::new(&root);
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let other_node = NodeId::from_legacy_named("node_other").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let controller = RolloutController::new(node.clone());
        let state = controller.initial_state(
            rollout_plan(&node, &artifact_id, &module_id),
            active_artifact_id,
        );
        let decision = controller.advance(
            state,
            RolloutObservation {
                scorecard: Some(scorecard(EvaluationOutcome::Passed, artifact_id)),
                incident_event_ids: Vec::new(),
                user_correction_count: 0,
                rollback_triggers: Vec::new(),
            },
        );
        let recorded = controller
            .record_decision(&paths, &decision, Vec::<String>::new())
            .expect("record decision");

        let error = RolloutController::new(other_node)
            .apply_recorded_transition(&root, &recorded)
            .expect_err("wrong node rejected");

        assert!(matches!(
            error,
            crate::RolloutError::InvalidTransition { .. }
        ));
    }

    #[test]
    fn verified_artifact_rollout_accepts_distinct_artifact_and_release_hashes() {
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let release_id = sinomo_core::Id::from_legacy_named("release_verified").expect("release");
        let release_ref = format!("registry://artifact-evidence/{release_id}");
        let (artifact, signer) = signed_artifact_manifest(
            &artifact_id,
            &module_id,
            "blake3:different",
            vec![release_ref.clone()],
        );
        let request = VerifiedArtifactRolloutRequest {
            node_id: node.clone(),
            artifact,
            policy_decision: allow_policy(&artifact_id),
            rollout_plan: rollout_plan(&node, &artifact_id, &module_id),
            release_id,
            release_material_hash: "blake3:release".to_string(),
            evidence_refs: vec![release_ref],
            trusted_signers: vec![signer],
            active_artifact_id: ArtifactId::from_legacy_named("artifact_active").expect("artifact"),
        };

        let preparation = RolloutController::new(node).prepare_verified_artifact_rollout(request);

        assert!(preparation.accepted, "{}", preparation.reason);
        assert_eq!(preparation.release_material_hash, "blake3:release");
        assert!(
            preparation
                .load_decision
                .as_ref()
                .is_some_and(|decision| decision.action == LoaderAction::AcceptShadow)
        );
    }

    #[test]
    fn verified_artifact_rollout_rejects_unsigned_artifact() {
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let release_id = sinomo_core::Id::from_legacy_named("release_verified").expect("release");
        let release_ref = format!("registry://artifact-evidence/{release_id}");
        let request = VerifiedArtifactRolloutRequest {
            node_id: node.clone(),
            artifact: artifact_manifest(
                &artifact_id,
                &module_id,
                "blake3:candidate",
                vec![release_ref.clone()],
            ),
            policy_decision: allow_policy(&artifact_id),
            rollout_plan: rollout_plan(&node, &artifact_id, &module_id),
            release_id,
            release_material_hash: "blake3:release".to_string(),
            evidence_refs: vec![release_ref],
            trusted_signers: Vec::new(),
            active_artifact_id: ArtifactId::from_legacy_named("artifact_active").expect("artifact"),
        };

        let preparation = RolloutController::new(node).prepare_verified_artifact_rollout(request);

        assert!(!preparation.accepted);
        assert!(preparation.load_decision.is_none());
        assert!(preparation.reason.contains("missing a signature"));
    }

    #[test]
    fn verified_artifact_rollout_rejects_unauthorized_signer() {
        let node = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let release_id = sinomo_core::Id::from_legacy_named("release_verified").expect("release");
        let release_ref = format!("registry://artifact-evidence/{release_id}");
        let (artifact, _signer) = signed_artifact_manifest(
            &artifact_id,
            &module_id,
            "blake3:candidate",
            vec![release_ref.clone()],
        );
        let request = VerifiedArtifactRolloutRequest {
            node_id: node.clone(),
            artifact,
            policy_decision: allow_policy(&artifact_id),
            rollout_plan: rollout_plan(&node, &artifact_id, &module_id),
            release_id,
            release_material_hash: "blake3:release".to_string(),
            evidence_refs: vec![release_ref],
            trusted_signers: Vec::new(),
            active_artifact_id: ArtifactId::from_legacy_named("artifact_active").expect("artifact"),
        };

        let preparation = RolloutController::new(node).prepare_verified_artifact_rollout(request);

        assert!(!preparation.accepted);
        assert!(preparation.load_decision.is_none());
        assert!(preparation.reason.contains("artifact-signer-known"));
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
                    kind: ActorKind::Registry,
                    id: "local-registry".to_string(),
                },
                source_event_id: None,
                build_log_artifact_id: None,
                attestations,
            },
        }
    }

    fn signed_artifact_manifest(
        artifact_id: &ArtifactId,
        module_id: &ModuleId,
        content_hash: &str,
        attestations: Vec<String>,
    ) -> (ArtifactManifest, TrustedArtifactSigner) {
        let (_identity_dir, identity_path) = unique_identity_path("rollout-artifact-signature");
        let identity =
            sinomo_core::node::load_or_create_node_identity(&identity_path).expect("identity");
        let mut artifact = artifact_manifest(artifact_id, module_id, content_hash, attestations);
        artifact.signature =
            Some(sign_artifact_manifest_with_node_identity(&artifact, &identity).expect("sign"));
        let signer =
            TrustedArtifactSigner::local_ed25519("local-test-signer", identity.public_key, "test");
        (artifact, signer)
    }

    fn unique_identity_path(name: &str) -> (sinomo_testdir::TestDir, std::path::PathBuf) {
        let dir = sinomo_testdir::TestDir::new(&format!("sinomo-rollout-{name}"));
        let path = dir.join("identity.sobj");
        (dir, path)
    }

    fn allow_policy(artifact_id: &ArtifactId) -> PolicyDecision {
        PolicyDecision {
            decision_id: sinomo_core::Id::from_legacy_named("policy_allow").expect("policy"),
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
            rollout_plan_id: RolloutPlanId::from_legacy_named("rollout_candidate")
                .expect("rollout"),
            artifact_id: artifact_id.clone(),
            module_id: module_id.clone(),
            target_ring: TrustRing::R3SandboxedModule,
            strategy: RolloutStrategy::Shadow,
            target_nodes: vec![node.clone()],
            initial_weight_bps: 0,
            promotion_steps_bps: vec![500, 2_500],
            rollback_policy: RollbackPolicy::default(),
        }
    }

    fn scorecard(outcome: EvaluationOutcome, artifact_id: ArtifactId) -> EvaluationScorecard {
        EvaluationScorecard {
            scorecard_id: ScorecardId::from_legacy_named("score_candidate").expect("score"),
            baseline_artifact_id: None,
            candidate_artifact_id: Some(artifact_id),
            outcome,
            gates: Vec::new(),
            metrics: Vec::new(),
        }
    }

    fn module_shadow_decision_event(
        node_id: &NodeId,
        module_id: &ModuleId,
        primary_artifact_id: &ArtifactId,
        shadow_artifact_id: &ArtifactId,
        matched: bool,
    ) -> EventEnvelope {
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            node_id.clone(),
            Source::new("module.live").expect("source"),
            EventKind::new(MODULE_SHADOW_DECISION_EVENT_KIND).expect("kind"),
            Salience::new(0.72).expect("salience"),
            Confidence::new(0.95).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(node_id.clone())
        .with_payload(serde_json::json!({
            "route_key": "live-session",
            "telemetry_source": "live",
            "module_id": module_id.as_str(),
            "primary_artifact_id": primary_artifact_id.as_str(),
            "primary_lane": "active",
            "shadow_artifact_id": shadow_artifact_id.as_str(),
            "shadow_matched": matched,
            "primary_output_bytes": 12,
            "shadow_output_bytes": 12,
            "primary_error": null,
            "shadow_error": null,
        }))
    }

    fn module_invocation_decision_event(
        node_id: &NodeId,
        module_id: &ModuleId,
        primary_artifact_id: &ArtifactId,
        primary_lane: &str,
        primary_error: Option<&str>,
    ) -> EventEnvelope {
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            node_id.clone(),
            Source::new("module.live").expect("source"),
            EventKind::new(MODULE_INVOCATION_DECISION_EVENT_KIND).expect("kind"),
            Salience::new(0.72).expect("salience"),
            Confidence::new(0.95).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(node_id.clone())
        .with_payload(serde_json::json!({
            "route_key": "live-session",
            "telemetry_source": "live",
            "module_id": module_id.as_str(),
            "primary_artifact_id": primary_artifact_id.as_str(),
            "primary_lane": primary_lane,
            "traffic_weight_bps": 500,
            "route_bucket_bps": 42,
            "hot_swap_state": "active-candidate",
            "primary_output_bytes": 12,
            "primary_error": primary_error,
        }))
    }

    fn module_user_correction_event(
        node_id: &NodeId,
        module_id: &ModuleId,
        corrected_artifact_id: &ArtifactId,
    ) -> EventEnvelope {
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            node_id.clone(),
            Source::new("user.correction").expect("source"),
            EventKind::new(MODULE_USER_CORRECTION_EVENT_KIND).expect("kind"),
            Salience::new(0.9).expect("salience"),
            Confidence::new(0.95).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(node_id.clone())
        .with_payload(serde_json::json!({
            "module_id": module_id.as_str(),
            "corrected_artifact_id": corrected_artifact_id.as_str(),
            "primary_lane": "candidate",
            "reason": "user corrected candidate output",
        }))
    }

    fn module_runtime_failure_event(
        node_id: &NodeId,
        module_id: &ModuleId,
        artifact_id: &ArtifactId,
        lane: &str,
        kind: &str,
    ) -> EventEnvelope {
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            node_id.clone(),
            Source::new("module.live").expect("source"),
            EventKind::new(kind).expect("kind"),
            Salience::new(0.95).expect("salience"),
            Confidence::new(0.95).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(node_id.clone())
        .with_payload(serde_json::json!({
            "module_id": module_id.as_str(),
            "artifact_id": artifact_id.as_str(),
            "primary_lane": lane,
            "reason": "runtime failure",
        }))
    }

    fn unique_temp(prefix: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(prefix)
    }
}
