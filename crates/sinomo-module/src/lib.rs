//! Module manifest and node-local loader state skeleton.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::signature::{ED25519, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use sinomo_core::{
    ArtifactManifest, Confidence, EvaluationGate, EvaluationGateResult, EvaluationMetric,
    EvaluationOutcome, EvaluationScorecard, EventEnvelope, EventKind, HotSwapState, LatencyClass,
    MetricValue, ModuleLoaderState, PolicyAction, PolicyDecision, PrivacyClass, RolloutPlan,
    RoutingMetadata, RoutingScope, Salience, Source, TrustRing,
};
use sinomo_events::{EventId, EventStore, EventStoreError, StoredEvent};
use sinomo_node::{NodeId, NodeIdentity};
use sinomo_object::{LengthMode, serde_ext};
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub type ArtifactId = sinomo_core::Id;
pub type ModuleId = sinomo_core::Id;
pub type RolloutPlanId = sinomo_core::Id;

pub const ARTIFACT_SIGNATURE_PROTOCOL: &str = "sinomo.artifact-manifest-signature.v1";
pub const LOCAL_ED25519_ARTIFACT_SIGNATURE_SCHEME: &str = "local-ed25519";
pub const MODULE_OBJECT_VERSION: u16 = 1;
pub const DEFAULT_LOCAL_COMMAND_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_LOCAL_COMMAND_OUTPUT_LIMIT_BYTES: usize = 128 * 1024;
pub const SINOMO_MODULE_V1_INTERFACE_VERSION: &str = "sinomo-module-v1";
pub const SINOMO_MODULE_V1_WIT: &str = include_str!("../wit/sinomo-module-v1.wit");
pub const MODULE_INVOCATION_DECISION_EVENT_KIND: &str = "module.invocation.decision";
pub const MODULE_SHADOW_DECISION_EVENT_KIND: &str = "module.shadow.decision";
pub const MODULE_USER_CORRECTION_EVENT_KIND: &str = "module.user.correction";
pub const MODULE_CRASH_EVENT_KIND: &str = "module.crash";
pub const MODULE_TIMEOUT_EVENT_KIND: &str = "module.timeout";
pub const MODULE_POLICY_BLOCKED_EVENT_KIND: &str = "module.policy.blocked";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SinomoModuleV1Context {
    #[serde(rename = "node-id", alias = "node_id")]
    pub node_id: String,
    #[serde(rename = "module-id", alias = "module_id")]
    pub module_id: String,
    #[serde(rename = "module-version", alias = "module_version")]
    pub module_version: String,
    #[serde(rename = "budget-ms", alias = "budget_ms")]
    pub budget_ms: u64,
    #[serde(rename = "capabilities-json", alias = "capabilities_json")]
    pub capabilities_json: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SinomoModuleV1Result {
    pub status: String,
    #[serde(rename = "events-json", alias = "events_json")]
    pub events_json: Vec<String>,
    #[serde(rename = "metrics-json", alias = "metrics_json")]
    pub metrics_json: String,
}

impl SinomoModuleV1Result {
    pub fn from_json_output(output: &str) -> Result<Self, SinomoModuleV1ResultError> {
        let result: Self = serde_json::from_str(output).map_err(|source| {
            SinomoModuleV1ResultError::InvalidJson {
                detail: source.to_string(),
            }
        })?;
        result.validate()?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<(), SinomoModuleV1ResultError> {
        if self.status.trim().is_empty() {
            return Err(SinomoModuleV1ResultError::MissingStatus);
        }
        for (index, event_json) in self.events_json.iter().enumerate() {
            serde_json::from_str::<serde_json::Value>(event_json).map_err(|source| {
                SinomoModuleV1ResultError::InvalidEventJson {
                    index,
                    detail: source.to_string(),
                }
            })?;
        }
        serde_json::from_str::<serde_json::Value>(&self.metrics_json).map_err(|source| {
            SinomoModuleV1ResultError::InvalidMetricsJson {
                detail: source.to_string(),
            }
        })?;
        Ok(())
    }

    pub fn to_json_output(&self) -> Result<String, SinomoModuleV1ResultError> {
        self.validate()?;
        serde_json::to_string(self).map_err(|source| SinomoModuleV1ResultError::Serialize {
            detail: source.to_string(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleLoaderRuntimeState {
    pub node_id: NodeId,
    pub rollout_plan_id: RolloutPlanId,
    pub module_id: ModuleId,
    pub loader_state: ModuleLoaderState,
    pub traffic_weight_bps: u16,
    pub last_rollout_action: String,
    pub last_rollout_stage: String,
    pub last_transition_record_id: sinomo_core::Id,
    pub reason: String,
    pub applied_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct ModuleLocalCommandRegistry {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ModuleLocalCommandArtifact>,
}

impl ModuleLocalCommandRegistry {
    pub fn artifact(&self, artifact_id: &ArtifactId) -> Option<&ModuleLocalCommandArtifact> {
        self.artifacts
            .iter()
            .find(|artifact| artifact.artifact_id == *artifact_id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleLocalCommandArtifact {
    pub artifact_id: ArtifactId,
    pub program: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_bytes: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleRouteRequest {
    pub module_id: ModuleId,
    pub route_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleRouteDecision {
    pub node_id: NodeId,
    pub module_id: ModuleId,
    pub primary_artifact_id: ArtifactId,
    pub primary_lane: ModuleRouteLane,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_artifact_id: Option<ArtifactId>,
    pub traffic_weight_bps: u16,
    pub route_bucket_bps: u16,
    pub hot_swap_state: HotSwapState,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ModuleRouteLane {
    Active,
    Candidate,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleInvocationRequest {
    pub module_id: ModuleId,
    pub route_key: String,
    pub input: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleArtifactInvocation {
    pub node_id: NodeId,
    pub module_id: ModuleId,
    pub artifact_id: ArtifactId,
    pub lane: ModuleInvocationLane,
    pub input: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ModuleInvocationLane {
    Active,
    Candidate,
    Shadow,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleArtifactInvocationResult {
    pub artifact_id: ArtifactId,
    pub lane: ModuleInvocationLane,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleInvocationOutcome {
    pub route: ModuleRouteDecision,
    pub primary: ModuleArtifactInvocationResult,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow: Option<ModuleArtifactInvocationResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModuleReplayFixture {
    pub fixture_id: sinomo_core::Id,
    pub module_id: ModuleId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cases: Vec<ModuleReplayCase>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModuleReplayCase {
    pub case_id: String,
    pub route_key: String,
    pub input: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event: Option<EventEnvelope>,
}

impl ModuleReplayCase {
    pub fn from_event(
        case_id: impl Into<String>,
        route_key: impl Into<String>,
        event: EventEnvelope,
    ) -> Self {
        let input = serde_json::to_string(&event).unwrap_or_else(|_| event.payload.to_string());
        Self {
            case_id: case_id.into(),
            route_key: route_key.into(),
            input,
            source_event: Some(event),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModuleReplayReport {
    pub fixture_id: sinomo_core::Id,
    pub module_id: ModuleId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cases: Vec<ModuleReplayCaseOutcome>,
    pub replay_event_count: u64,
    pub missing_route_count: u64,
    pub primary_error_count: u64,
    pub shadow_error_count: u64,
    pub shadow_mismatch_count: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<EventEnvelope>,
}

impl ModuleReplayReport {
    pub fn outcome(&self) -> EvaluationOutcome {
        if self.cases.is_empty() {
            return EvaluationOutcome::Inconclusive;
        }
        if self.missing_route_count == 0
            && self.primary_error_count == 0
            && self.shadow_error_count == 0
            && self.shadow_mismatch_count == 0
        {
            EvaluationOutcome::Passed
        } else {
            EvaluationOutcome::Failed
        }
    }

    pub fn scorecard(
        &self,
        baseline_artifact_id: Option<ArtifactId>,
        candidate_artifact_id: Option<ArtifactId>,
    ) -> EvaluationScorecard {
        let outcome = self.outcome();
        EvaluationScorecard {
            scorecard_id: sinomo_core::Id::generate(),
            baseline_artifact_id,
            candidate_artifact_id,
            outcome: outcome.clone(),
            gates: vec![
                EvaluationGateResult {
                    gate: EvaluationGate::ReplayTests,
                    outcome: if self.missing_route_count == 0 && !self.cases.is_empty() {
                        EvaluationOutcome::Passed
                    } else {
                        EvaluationOutcome::Failed
                    },
                    summary: format!(
                        "{} replay cases, {} missing routes",
                        self.cases.len(),
                        self.missing_route_count
                    ),
                },
                EvaluationGateResult {
                    gate: EvaluationGate::ShadowMode,
                    outcome,
                    summary: format!(
                        "{} primary errors, {} shadow errors, {} shadow mismatches",
                        self.primary_error_count,
                        self.shadow_error_count,
                        self.shadow_mismatch_count
                    ),
                },
            ],
            metrics: vec![
                replay_metric("module_replay_case_count", self.cases.len() as i64),
                replay_metric("module_replay_event_count", self.replay_event_count as i64),
                replay_metric(
                    "module_replay_missing_route_count",
                    self.missing_route_count as i64,
                ),
                replay_metric(
                    "module_replay_primary_error_count",
                    self.primary_error_count as i64,
                ),
                replay_metric(
                    "module_replay_shadow_error_count",
                    self.shadow_error_count as i64,
                ),
                replay_metric(
                    "module_replay_shadow_mismatch_count",
                    self.shadow_mismatch_count as i64,
                ),
            ],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModuleShadowTelemetryReport {
    pub module_id: ModuleId,
    pub event_count: u64,
    pub matched_count: u64,
    pub mismatch_count: u64,
    pub primary_error_count: u64,
    pub shadow_error_count: u64,
    pub ignored_event_count: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_ids: Vec<EventId>,
}

impl ModuleShadowTelemetryReport {
    pub fn from_stored_events<'a>(
        module_id: ModuleId,
        events: impl IntoIterator<Item = &'a StoredEvent>,
    ) -> Self {
        let mut report = Self {
            module_id,
            event_count: 0,
            matched_count: 0,
            mismatch_count: 0,
            primary_error_count: 0,
            shadow_error_count: 0,
            ignored_event_count: 0,
            event_ids: Vec::new(),
        };
        for stored in events {
            if stored.event.kind.as_str() != MODULE_SHADOW_DECISION_EVENT_KIND {
                report.ignored_event_count += 1;
                continue;
            }
            if stored
                .event
                .payload
                .get("module_id")
                .and_then(serde_json::Value::as_str)
                != Some(report.module_id.as_str())
            {
                report.ignored_event_count += 1;
                continue;
            }
            report.event_count += 1;
            report.event_ids.push(stored.event.event_id.clone());
            match stored
                .event
                .payload
                .get("shadow_matched")
                .and_then(serde_json::Value::as_bool)
            {
                Some(true) => report.matched_count += 1,
                Some(false) => report.mismatch_count += 1,
                None => report.ignored_event_count += 1,
            }
            if payload_field_has_error(&stored.event.payload, "primary_error") {
                report.primary_error_count += 1;
            }
            if payload_field_has_error(&stored.event.payload, "shadow_error") {
                report.shadow_error_count += 1;
            }
        }
        report
    }

    pub fn outcome(&self) -> EvaluationOutcome {
        if self.event_count == 0 {
            return EvaluationOutcome::Inconclusive;
        }
        if self.mismatch_count == 0 && self.primary_error_count == 0 && self.shadow_error_count == 0
        {
            EvaluationOutcome::Passed
        } else {
            EvaluationOutcome::Failed
        }
    }

    pub fn scorecard(
        &self,
        baseline_artifact_id: Option<ArtifactId>,
        candidate_artifact_id: Option<ArtifactId>,
    ) -> EvaluationScorecard {
        let outcome = self.outcome();
        EvaluationScorecard {
            scorecard_id: sinomo_core::Id::generate(),
            baseline_artifact_id,
            candidate_artifact_id,
            outcome: outcome.clone(),
            gates: vec![EvaluationGateResult {
                gate: EvaluationGate::ShadowMode,
                outcome,
                summary: format!(
                    "{} live shadow events, {} mismatches, {} primary errors, {} shadow errors",
                    self.event_count,
                    self.mismatch_count,
                    self.primary_error_count,
                    self.shadow_error_count
                ),
            }],
            metrics: vec![
                replay_metric("module_shadow_event_count", self.event_count as i64),
                replay_metric("module_shadow_matched_count", self.matched_count as i64),
                replay_metric("module_shadow_mismatch_count", self.mismatch_count as i64),
                replay_metric(
                    "module_shadow_primary_error_count",
                    self.primary_error_count as i64,
                ),
                replay_metric(
                    "module_shadow_shadow_error_count",
                    self.shadow_error_count as i64,
                ),
                replay_metric(
                    "module_shadow_ignored_event_count",
                    self.ignored_event_count as i64,
                ),
            ],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModuleCanaryTelemetryReport {
    pub module_id: ModuleId,
    pub event_count: u64,
    pub active_primary_count: u64,
    pub candidate_primary_count: u64,
    pub correction_event_count: u64,
    pub active_user_correction_count: u64,
    pub candidate_user_correction_count: u64,
    pub unknown_user_correction_count: u64,
    pub active_error_count: u64,
    pub candidate_error_count: u64,
    pub active_latency_sample_count: u64,
    pub candidate_latency_sample_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_latency_p95_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_latency_p95_ms: Option<u64>,
    pub ignored_event_count: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_ids: Vec<EventId>,
}

impl ModuleCanaryTelemetryReport {
    pub fn from_stored_events<'a>(
        module_id: ModuleId,
        candidate_artifact_id: Option<&ArtifactId>,
        events: impl IntoIterator<Item = &'a StoredEvent>,
    ) -> Self {
        let mut report = Self {
            module_id,
            event_count: 0,
            active_primary_count: 0,
            candidate_primary_count: 0,
            correction_event_count: 0,
            active_user_correction_count: 0,
            candidate_user_correction_count: 0,
            unknown_user_correction_count: 0,
            active_error_count: 0,
            candidate_error_count: 0,
            active_latency_sample_count: 0,
            candidate_latency_sample_count: 0,
            active_latency_p95_ms: None,
            candidate_latency_p95_ms: None,
            ignored_event_count: 0,
            event_ids: Vec::new(),
        };
        let mut active_latencies = Vec::new();
        let mut candidate_latencies = Vec::new();
        for stored in events {
            match stored.event.kind.as_str() {
                MODULE_INVOCATION_DECISION_EVENT_KIND => {
                    report.ingest_invocation_event(
                        stored,
                        candidate_artifact_id,
                        &mut active_latencies,
                        &mut candidate_latencies,
                    );
                }
                MODULE_USER_CORRECTION_EVENT_KIND => {
                    report.ingest_user_correction_event(stored, candidate_artifact_id);
                }
                _ => report.ignored_event_count += 1,
            }
        }
        report.active_latency_sample_count = active_latencies.len() as u64;
        report.candidate_latency_sample_count = candidate_latencies.len() as u64;
        report.active_latency_p95_ms = percentile_nearest_rank(active_latencies, 95);
        report.candidate_latency_p95_ms = percentile_nearest_rank(candidate_latencies, 95);
        report
    }

    fn ingest_invocation_event(
        &mut self,
        stored: &StoredEvent,
        candidate_artifact_id: Option<&ArtifactId>,
        active_latencies: &mut Vec<u64>,
        candidate_latencies: &mut Vec<u64>,
    ) {
        if !payload_module_matches(&stored.event.payload, &self.module_id) {
            self.ignored_event_count += 1;
            return;
        }
        let Some(primary_lane) = stored
            .event
            .payload
            .get("primary_lane")
            .and_then(serde_json::Value::as_str)
        else {
            self.ignored_event_count += 1;
            return;
        };
        if primary_lane == "candidate"
            && candidate_artifact_id.is_some_and(|candidate| {
                stored
                    .event
                    .payload
                    .get("primary_artifact_id")
                    .and_then(serde_json::Value::as_str)
                    != Some(candidate.as_str())
            })
        {
            self.ignored_event_count += 1;
            return;
        }
        self.event_count += 1;
        self.event_ids.push(stored.event.event_id.clone());
        match primary_lane {
            "active" => {
                self.active_primary_count += 1;
                if payload_field_has_error(&stored.event.payload, "primary_error") {
                    self.active_error_count += 1;
                }
                if let Some(latency_ms) = payload_u64(&stored.event.payload, "primary_latency_ms") {
                    active_latencies.push(latency_ms);
                }
            }
            "candidate" => {
                self.candidate_primary_count += 1;
                if payload_field_has_error(&stored.event.payload, "primary_error") {
                    self.candidate_error_count += 1;
                }
                if let Some(latency_ms) = payload_u64(&stored.event.payload, "primary_latency_ms") {
                    candidate_latencies.push(latency_ms);
                }
            }
            _ => self.ignored_event_count += 1,
        }
    }

    fn ingest_user_correction_event(
        &mut self,
        stored: &StoredEvent,
        candidate_artifact_id: Option<&ArtifactId>,
    ) {
        if !payload_module_matches(&stored.event.payload, &self.module_id) {
            self.ignored_event_count += 1;
            return;
        }
        self.correction_event_count += 1;
        self.event_ids.push(stored.event.event_id.clone());
        match user_correction_lane(&stored.event.payload, candidate_artifact_id) {
            Some("active") => self.active_user_correction_count += 1,
            Some("candidate") => self.candidate_user_correction_count += 1,
            _ => self.unknown_user_correction_count += 1,
        }
    }

    pub fn outcome(
        &self,
        minimum_candidate_event_count: u64,
        maximum_candidate_latency_p95_ms: Option<u64>,
    ) -> EvaluationOutcome {
        if self.candidate_user_correction_count > 0 {
            return EvaluationOutcome::Failed;
        }
        if self.candidate_primary_count < minimum_candidate_event_count.max(1) {
            return EvaluationOutcome::Inconclusive;
        }
        if self.candidate_error_count > 0 {
            return EvaluationOutcome::Failed;
        }
        if maximum_candidate_latency_p95_ms.is_some_and(|maximum| {
            self.candidate_latency_p95_ms
                .is_some_and(|latency| latency > maximum)
        }) {
            return EvaluationOutcome::Failed;
        }
        EvaluationOutcome::Passed
    }

    pub fn scorecard(
        &self,
        baseline_artifact_id: Option<ArtifactId>,
        candidate_artifact_id: Option<ArtifactId>,
        minimum_candidate_event_count: u64,
        maximum_candidate_latency_p95_ms: Option<u64>,
    ) -> EvaluationScorecard {
        let outcome = self.outcome(
            minimum_candidate_event_count,
            maximum_candidate_latency_p95_ms,
        );
        let mut metrics = vec![
            replay_metric("module_canary_event_count", self.event_count as i64),
            replay_metric(
                "module_canary_candidate_primary_count",
                self.candidate_primary_count as i64,
            ),
            replay_metric(
                "module_canary_active_primary_count",
                self.active_primary_count as i64,
            ),
            replay_metric(
                "module_canary_correction_event_count",
                self.correction_event_count as i64,
            ),
            replay_metric(
                "module_canary_candidate_user_correction_count",
                self.candidate_user_correction_count as i64,
            ),
            replay_metric(
                "module_canary_active_user_correction_count",
                self.active_user_correction_count as i64,
            ),
            replay_metric(
                "module_canary_unknown_user_correction_count",
                self.unknown_user_correction_count as i64,
            ),
            replay_metric(
                "module_canary_candidate_error_count",
                self.candidate_error_count as i64,
            ),
            replay_metric(
                "module_canary_active_error_count",
                self.active_error_count as i64,
            ),
            replay_metric(
                "module_canary_candidate_latency_sample_count",
                self.candidate_latency_sample_count as i64,
            ),
            replay_metric(
                "module_canary_active_latency_sample_count",
                self.active_latency_sample_count as i64,
            ),
            replay_metric(
                "module_canary_ignored_event_count",
                self.ignored_event_count as i64,
            ),
        ];
        if let Some(latency_ms) = self.candidate_latency_p95_ms {
            metrics.push(replay_metric(
                "module_canary_candidate_latency_p95_ms",
                latency_ms as i64,
            ));
        }
        if let Some(latency_ms) = self.active_latency_p95_ms {
            metrics.push(replay_metric(
                "module_canary_active_latency_p95_ms",
                latency_ms as i64,
            ));
        }
        EvaluationScorecard {
            scorecard_id: sinomo_core::Id::generate(),
            baseline_artifact_id,
            candidate_artifact_id,
            outcome: outcome.clone(),
            gates: vec![EvaluationGateResult {
                gate: EvaluationGate::CanaryAnalysis,
                outcome,
                summary: format!(
                    "{} canary candidate-primary events, {} candidate errors, {} candidate user corrections, candidate p95 latency {} ms, {} active-primary events, {} active errors",
                    self.candidate_primary_count,
                    self.candidate_error_count,
                    self.candidate_user_correction_count,
                    self.candidate_latency_p95_ms
                        .map(|latency| latency.to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    self.active_primary_count,
                    self.active_error_count
                ),
            }],
            metrics,
        }
    }
}

fn payload_module_matches(payload: &serde_json::Value, module_id: &ModuleId) -> bool {
    payload.get("module_id").and_then(serde_json::Value::as_str) == Some(module_id.as_str())
}

fn user_correction_lane<'a>(
    payload: &'a serde_json::Value,
    candidate_artifact_id: Option<&ArtifactId>,
) -> Option<&'a str> {
    let artifact_id = payload
        .get("corrected_artifact_id")
        .or_else(|| payload.get("artifact_id"))
        .or_else(|| payload.get("primary_artifact_id"))
        .and_then(serde_json::Value::as_str);
    if let (Some(candidate), Some(artifact_id)) = (candidate_artifact_id, artifact_id) {
        if artifact_id == candidate.as_str() {
            return Some("candidate");
        }
        return payload
            .get("primary_lane")
            .or_else(|| payload.get("lane"))
            .and_then(serde_json::Value::as_str)
            .filter(|lane| *lane == "active");
    }
    payload
        .get("primary_lane")
        .or_else(|| payload.get("lane"))
        .and_then(serde_json::Value::as_str)
        .filter(|lane| *lane == "active" || *lane == "candidate")
}

fn payload_u64(payload: &serde_json::Value, field: &str) -> Option<u64> {
    payload.get(field).and_then(serde_json::Value::as_u64)
}

fn payload_field_has_error(payload: &serde_json::Value, field: &str) -> bool {
    payload
        .get(field)
        .is_some_and(|value| !value.is_null() && value.as_str() != Some(""))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModuleReplayCaseOutcome {
    pub case_id: String,
    pub route_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<ModuleInvocationOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_matched: Option<bool>,
}

pub trait ModuleArtifactRunner {
    fn invoke(&self, invocation: &ModuleArtifactInvocation) -> Result<String, String>;
}

#[derive(Clone, Debug)]
pub struct LocalCommandModuleArtifactRunner {
    registry: ModuleLocalCommandRegistry,
    default_cwd: PathBuf,
}

impl LocalCommandModuleArtifactRunner {
    pub fn new(registry: ModuleLocalCommandRegistry, default_cwd: impl Into<PathBuf>) -> Self {
        Self {
            registry,
            default_cwd: default_cwd.into(),
        }
    }

    pub fn from_data_dir(
        data_dir_root: &Path,
        default_cwd: impl Into<PathBuf>,
    ) -> Result<Self, ModuleLoaderStoreError> {
        Ok(Self::new(
            read_module_local_command_registry(data_dir_root)?.unwrap_or_default(),
            default_cwd,
        ))
    }
}

impl ModuleArtifactRunner for LocalCommandModuleArtifactRunner {
    fn invoke(&self, invocation: &ModuleArtifactInvocation) -> Result<String, String> {
        let artifact = self
            .registry
            .artifact(&invocation.artifact_id)
            .ok_or_else(|| {
                format!(
                    "module artifact {} is not registered as a local command",
                    invocation.artifact_id
                )
            })?;
        run_local_command_artifact(artifact, invocation, &self.default_cwd)
    }
}

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
    pub rollout_plan_id: Option<RolloutPlanId>,
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

pub fn module_loader_state_dir(data_dir_root: &Path) -> PathBuf {
    data_dir_root.join("module").join("loader-state")
}

pub fn module_loader_state_path(data_dir_root: &Path, module_id: &ModuleId) -> PathBuf {
    module_loader_state_dir(data_dir_root).join(format!("{module_id}.sobj"))
}

pub fn module_local_command_registry_path(data_dir_root: &Path) -> PathBuf {
    data_dir_root
        .join("module")
        .join("local-command-artifacts.sobj")
}

pub fn read_module_loader_runtime_state(
    data_dir_root: &Path,
    module_id: &ModuleId,
) -> Result<Option<ModuleLoaderRuntimeState>, ModuleLoaderStoreError> {
    let path = module_loader_state_path(data_dir_root, module_id);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
}

pub fn write_module_loader_runtime_state(
    data_dir_root: &Path,
    state: &ModuleLoaderRuntimeState,
) -> Result<PathBuf, ModuleLoaderStoreError> {
    let path = module_loader_state_path(data_dir_root, &state.module_id);
    write_object(&path, state)?;
    Ok(path)
}

pub fn read_module_local_command_registry(
    data_dir_root: &Path,
) -> Result<Option<ModuleLocalCommandRegistry>, ModuleLoaderStoreError> {
    let path = module_local_command_registry_path(data_dir_root);
    if !path.exists() {
        return Ok(None);
    }
    read_object(&path).map(Some)
}

pub fn write_module_local_command_registry(
    data_dir_root: &Path,
    registry: &ModuleLocalCommandRegistry,
) -> Result<PathBuf, ModuleLoaderStoreError> {
    let path = module_local_command_registry_path(data_dir_root);
    write_object(&path, registry)?;
    Ok(path)
}

pub fn module_loader_applied_at_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}

pub fn route_module_invocation(
    data_dir_root: &Path,
    request: &ModuleRouteRequest,
) -> Result<Option<ModuleRouteDecision>, ModuleLoaderStoreError> {
    let Some(state) = read_module_loader_runtime_state(data_dir_root, &request.module_id)? else {
        return Ok(None);
    };
    Ok(Some(route_module_invocation_from_state(
        &state,
        &request.route_key,
    )))
}

pub fn route_module_invocation_from_state(
    state: &ModuleLoaderRuntimeState,
    route_key: &str,
) -> ModuleRouteDecision {
    let bucket = stable_route_bucket_bps(&state.module_id, route_key);
    let weight = state.traffic_weight_bps.min(10_000);
    let candidate_is_primary = matches!(
        state.loader_state.hot_swap_state,
        HotSwapState::ActiveCandidate
    ) && weight > 0
        && weight < 10_000
        && bucket < weight
        && state.loader_state.candidate_artifact_id.is_some();
    let primary_artifact_id = if candidate_is_primary {
        state
            .loader_state
            .candidate_artifact_id
            .clone()
            .expect("checked candidate artifact")
    } else {
        state.loader_state.active_artifact_id.clone()
    };
    let primary_lane = if candidate_is_primary {
        ModuleRouteLane::Candidate
    } else {
        ModuleRouteLane::Active
    };
    let shadow_artifact_id = matches!(state.loader_state.hot_swap_state, HotSwapState::Shadowing)
        .then(|| state.loader_state.shadow_artifact_id.clone())
        .flatten();
    let reason = match (&primary_lane, &shadow_artifact_id) {
        (ModuleRouteLane::Candidate, _) => {
            "canary route selected candidate artifact by stable traffic bucket".to_string()
        }
        (ModuleRouteLane::Active, Some(_)) => {
            "shadow route keeps active artifact primary and mirrors to shadow artifact".to_string()
        }
        (ModuleRouteLane::Active, None) => {
            "route selected active artifact from loader runtime state".to_string()
        }
    };
    ModuleRouteDecision {
        node_id: state.node_id.clone(),
        module_id: state.module_id.clone(),
        primary_artifact_id,
        primary_lane,
        shadow_artifact_id,
        traffic_weight_bps: weight,
        route_bucket_bps: bucket,
        hot_swap_state: state.loader_state.hot_swap_state.clone(),
        reason,
    }
}

pub fn execute_routed_module_invocation<R: ModuleArtifactRunner>(
    data_dir_root: &Path,
    request: &ModuleInvocationRequest,
    runner: &R,
) -> Result<Option<ModuleInvocationOutcome>, ModuleLoaderStoreError> {
    let Some(route) = route_module_invocation(
        data_dir_root,
        &ModuleRouteRequest {
            module_id: request.module_id.clone(),
            route_key: request.route_key.clone(),
        },
    )?
    else {
        return Ok(None);
    };
    Ok(Some(execute_module_route_decision(
        route,
        request.input.clone(),
        runner,
    )))
}

pub fn execute_routed_module_invocation_with_event_store<R, S>(
    data_dir_root: &Path,
    request: &ModuleInvocationRequest,
    runner: &R,
    event_store: &mut S,
) -> Result<Option<ModuleInvocationOutcome>, ModuleInvocationTelemetryError>
where
    R: ModuleArtifactRunner,
    S: EventStore,
{
    let outcome = execute_routed_module_invocation(data_dir_root, request, runner)?;
    if let Some(outcome) = &outcome {
        event_store.append(module_invocation_decision_event_from_outcome(
            None,
            &request.route_key,
            None,
            "live",
            outcome,
        ))?;
        if let Some(event) = module_shadow_decision_event_from_outcome(
            None,
            &request.route_key,
            None,
            "live",
            outcome,
        ) {
            event_store.append(event)?;
        }
    }
    Ok(outcome)
}

pub fn execute_module_route_decision<R: ModuleArtifactRunner>(
    route: ModuleRouteDecision,
    input: String,
    runner: &R,
) -> ModuleInvocationOutcome {
    let primary_lane = match &route.primary_lane {
        ModuleRouteLane::Active => ModuleInvocationLane::Active,
        ModuleRouteLane::Candidate => ModuleInvocationLane::Candidate,
    };
    let primary = invoke_module_artifact(
        runner,
        ModuleArtifactInvocation {
            node_id: route.node_id.clone(),
            module_id: route.module_id.clone(),
            artifact_id: route.primary_artifact_id.clone(),
            lane: primary_lane,
            input: input.clone(),
        },
    );
    let shadow = route
        .shadow_artifact_id
        .as_ref()
        .filter(|artifact_id| **artifact_id != route.primary_artifact_id)
        .map(|artifact_id| {
            invoke_module_artifact(
                runner,
                ModuleArtifactInvocation {
                    node_id: route.node_id.clone(),
                    module_id: route.module_id.clone(),
                    artifact_id: artifact_id.clone(),
                    lane: ModuleInvocationLane::Shadow,
                    input,
                },
            )
        });
    ModuleInvocationOutcome {
        route,
        primary,
        shadow,
    }
}

pub fn run_module_replay_fixture<R: ModuleArtifactRunner>(
    data_dir_root: &Path,
    fixture: &ModuleReplayFixture,
    runner: &R,
) -> Result<ModuleReplayReport, ModuleLoaderStoreError> {
    let mut cases = Vec::with_capacity(fixture.cases.len());
    let mut replay_event_count = 0_u64;
    let mut missing_route_count = 0_u64;
    let mut primary_error_count = 0_u64;
    let mut shadow_error_count = 0_u64;
    let mut shadow_mismatch_count = 0_u64;
    let mut events = Vec::new();

    for case in &fixture.cases {
        replay_event_count += u64::from(case.source_event.is_some());
        let outcome = execute_routed_module_invocation(
            data_dir_root,
            &ModuleInvocationRequest {
                module_id: fixture.module_id.clone(),
                route_key: case.route_key.clone(),
                input: case.input.clone(),
            },
            runner,
        )?;
        if outcome.is_none() {
            missing_route_count += 1;
        }
        if outcome
            .as_ref()
            .is_some_and(|outcome| outcome.primary.error.is_some())
        {
            primary_error_count += 1;
        }
        if outcome
            .as_ref()
            .and_then(|outcome| outcome.shadow.as_ref())
            .is_some_and(|shadow| shadow.error.is_some())
        {
            shadow_error_count += 1;
        }
        let shadow_matched = outcome.as_ref().and_then(shadow_result_matches_primary);
        if shadow_matched == Some(false) {
            shadow_mismatch_count += 1;
        }
        let case_outcome = ModuleReplayCaseOutcome {
            case_id: case.case_id.clone(),
            route_key: case.route_key.clone(),
            source_event_id: case
                .source_event
                .as_ref()
                .map(|event| event.event_id.clone()),
            outcome,
            shadow_matched,
        };
        if let Some(event) = module_shadow_decision_event(&case_outcome) {
            events.push(event);
        }
        cases.push(case_outcome);
    }

    Ok(ModuleReplayReport {
        fixture_id: fixture.fixture_id.clone(),
        module_id: fixture.module_id.clone(),
        cases,
        replay_event_count,
        missing_route_count,
        primary_error_count,
        shadow_error_count,
        shadow_mismatch_count,
        events,
    })
}

fn invoke_module_artifact<R: ModuleArtifactRunner>(
    runner: &R,
    invocation: ModuleArtifactInvocation,
) -> ModuleArtifactInvocationResult {
    let artifact_id = invocation.artifact_id.clone();
    let lane = invocation.lane.clone();
    let started = Instant::now();
    let result = runner.invoke(&invocation);
    let latency_ms = Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64);
    match result {
        Ok(output) => ModuleArtifactInvocationResult {
            artifact_id,
            lane,
            output: Some(output),
            error: None,
            latency_ms,
        },
        Err(error) => ModuleArtifactInvocationResult {
            artifact_id,
            lane,
            output: None,
            error: Some(error),
            latency_ms,
        },
    }
}

fn run_local_command_artifact(
    artifact: &ModuleLocalCommandArtifact,
    invocation: &ModuleArtifactInvocation,
    default_cwd: &Path,
) -> Result<String, String> {
    if artifact.program.trim().is_empty() {
        return Err(format!(
            "local command artifact {} has an empty program",
            artifact.artifact_id
        ));
    }
    let stdout_path = local_command_output_path(&artifact.artifact_id, "stdout");
    let stderr_path = local_command_output_path(&artifact.artifact_id, "stderr");
    let stdout = fs::File::create(&stdout_path).map_err(|source| {
        format!(
            "failed to create stdout capture for artifact {}: {source}",
            artifact.artifact_id
        )
    })?;
    let stderr = match fs::File::create(&stderr_path) {
        Ok(stderr) => stderr,
        Err(source) => {
            cleanup_output_capture(&stdout_path);
            return Err(format!(
                "failed to create stderr capture for artifact {}: {source}",
                artifact.artifact_id
            ));
        }
    };
    let mut command = Command::new(&artifact.program);
    command
        .args(&artifact.args)
        .current_dir(artifact.cwd.as_deref().unwrap_or(default_cwd))
        .env("SINOMO_NODE_ID", invocation.node_id.as_str())
        .env("SINOMO_MODULE_ID", invocation.module_id.as_str())
        .env("SINOMO_ARTIFACT_ID", invocation.artifact_id.as_str())
        .env(
            "SINOMO_INVOCATION_LANE",
            invocation_lane_label(&invocation.lane),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    for (key, value) in &artifact.env {
        command.env(key, value);
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(source) => {
            cleanup_output_capture(&stdout_path);
            cleanup_output_capture(&stderr_path);
            return Err(format!(
                "failed to spawn local command artifact {} (`{}`): {source}",
                artifact.artifact_id, artifact.program
            ));
        }
    };
    if let Some(mut stdin) = child.stdin.take() {
        if let Err(source) = stdin.write_all(invocation.input.as_bytes()) {
            let _ = child.kill();
            let _ = child.wait();
            cleanup_output_capture(&stdout_path);
            cleanup_output_capture(&stderr_path);
            return Err(format!(
                "failed to write stdin for artifact {}: {source}",
                artifact.artifact_id
            ));
        }
    }
    let timeout = Duration::from_millis(
        artifact
            .timeout_ms
            .unwrap_or(DEFAULT_LOCAL_COMMAND_TIMEOUT_MS),
    );
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().map_err(|source| {
            format!(
                "failed waiting for local command artifact {}: {source}",
                artifact.artifact_id
            )
        })? {
            let stdout = read_output_capture(&stdout_path, artifact.max_output_bytes);
            let stderr = read_output_capture(&stderr_path, artifact.max_output_bytes);
            cleanup_output_capture(&stdout_path);
            cleanup_output_capture(&stderr_path);
            if status.success() {
                return Ok(String::from_utf8_lossy(&stdout).to_string());
            }
            return Err(format!(
                "local command artifact {} exited with status {}: stdout={} stderr={}",
                artifact.artifact_id,
                status,
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            ));
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            let stdout = read_output_capture(&stdout_path, artifact.max_output_bytes);
            let stderr = read_output_capture(&stderr_path, artifact.max_output_bytes);
            cleanup_output_capture(&stdout_path);
            cleanup_output_capture(&stderr_path);
            return Err(format!(
                "local command artifact {} timed out after {}ms: stdout={} stderr={}",
                artifact.artifact_id,
                timeout.as_millis(),
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn local_command_output_path(artifact_id: &ArtifactId, stream: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "sinomo-module-{}-{}-{}-{}.log",
        artifact_id,
        stream,
        std::process::id(),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    ))
}

fn read_output_capture(path: &Path, max_output_bytes: Option<usize>) -> Vec<u8> {
    let limit = max_output_bytes.unwrap_or(DEFAULT_LOCAL_COMMAND_OUTPUT_LIMIT_BYTES);
    if limit == 0 {
        return Vec::new();
    }
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(_) => return Vec::new(),
    };
    let length = file.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    let limit_u64 = u64::try_from(limit).unwrap_or(u64::MAX);
    let start = length.saturating_sub(limit_u64);
    if file.seek(SeekFrom::Start(start)).is_err() {
        return Vec::new();
    }
    let mut bytes = Vec::new();
    if file.take(limit_u64).read_to_end(&mut bytes).is_err() {
        return Vec::new();
    }
    bytes
}

fn cleanup_output_capture(path: &Path) {
    let _ = fs::remove_file(path);
}

fn invocation_lane_label(lane: &ModuleInvocationLane) -> &'static str {
    match lane {
        ModuleInvocationLane::Active => "active",
        ModuleInvocationLane::Candidate => "candidate",
        ModuleInvocationLane::Shadow => "shadow",
    }
}

fn shadow_result_matches_primary(outcome: &ModuleInvocationOutcome) -> Option<bool> {
    let shadow = outcome.shadow.as_ref()?;
    Some(
        outcome.primary.output == shadow.output
            && outcome.primary.error == shadow.error
            && shadow.lane == ModuleInvocationLane::Shadow,
    )
}

fn module_shadow_decision_event(case: &ModuleReplayCaseOutcome) -> Option<EventEnvelope> {
    let outcome = case.outcome.as_ref()?;
    module_shadow_decision_event_from_outcome(
        Some(&case.case_id),
        &case.route_key,
        case.source_event_id.as_ref(),
        "replay",
        outcome,
    )
}

pub fn module_invocation_decision_event_from_outcome(
    case_id: Option<&str>,
    route_key: &str,
    source_event_id: Option<&EventId>,
    telemetry_source: &str,
    outcome: &ModuleInvocationOutcome,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        outcome.route.node_id.clone(),
        Source::new(if telemetry_source == "live" {
            "module.live"
        } else {
            "module.replay"
        })
        .expect("valid source"),
        EventKind::new(MODULE_INVOCATION_DECISION_EVENT_KIND).expect("valid event kind"),
        Salience::new(if outcome.primary.error.is_some() {
            0.86
        } else if matches!(&outcome.route.primary_lane, ModuleRouteLane::Candidate) {
            0.7
        } else {
            0.58
        })
        .expect("valid salience"),
        Confidence::new(0.95).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_target_node(outcome.route.node_id.clone())
    .with_payload(serde_json::json!({
    "case_id": case_id,
    "route_key": route_key,
    "source_event_id": source_event_id.map(|id| id.as_str()),
    "telemetry_source": telemetry_source,
    "module_id": outcome.route.module_id.as_str(),
    "primary_artifact_id": outcome.primary.artifact_id.as_str(),
    "primary_lane": outcome.primary.lane.clone(),
    "shadow_artifact_id": outcome.shadow.as_ref().map(|shadow| shadow.artifact_id.as_str()),
        "shadow_matched": shadow_result_matches_primary(outcome),
        "traffic_weight_bps": outcome.route.traffic_weight_bps,
        "route_bucket_bps": outcome.route.route_bucket_bps,
        "hot_swap_state": outcome.route.hot_swap_state.clone(),
        "primary_output_bytes": outcome.primary.output.as_ref().map(|output| output.len()),
        "primary_latency_ms": outcome.primary.latency_ms,
        "shadow_output_bytes": outcome.shadow.as_ref().and_then(|shadow| {
            shadow.output.as_ref().map(|output| output.len())
        }),
        "shadow_latency_ms": outcome.shadow.as_ref().and_then(|shadow| shadow.latency_ms),
        "primary_error": outcome.primary.error.as_deref(),
        "primary_error_kind": module_artifact_error_kind(outcome.primary.error.as_deref()),
        "shadow_error": outcome.shadow.as_ref().and_then(|shadow| shadow.error.as_deref()),
        "shadow_error_kind": module_artifact_error_kind(
            outcome.shadow.as_ref().and_then(|shadow| shadow.error.as_deref()),
        ),
    }))
}

pub fn module_shadow_decision_event_from_outcome(
    case_id: Option<&str>,
    route_key: &str,
    source_event_id: Option<&EventId>,
    telemetry_source: &str,
    outcome: &ModuleInvocationOutcome,
) -> Option<EventEnvelope> {
    let shadow = outcome.shadow.as_ref()?;
    Some(
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            outcome.route.node_id.clone(),
            Source::new(if telemetry_source == "live" {
                "module.live"
            } else {
                "module.replay"
            })
            .expect("valid source"),
            EventKind::new(MODULE_SHADOW_DECISION_EVENT_KIND).expect("valid event kind"),
            Salience::new(if shadow_result_matches_primary(outcome) == Some(false) {
                0.84
            } else {
                0.62
            })
            .expect("valid salience"),
            Confidence::new(0.95).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(outcome.route.node_id.clone())
        .with_payload(serde_json::json!({
            "case_id": case_id,
            "route_key": route_key,
            "source_event_id": source_event_id.map(|id| id.as_str()),
            "telemetry_source": telemetry_source,
            "module_id": outcome.route.module_id.as_str(),
            "primary_artifact_id": outcome.primary.artifact_id.as_str(),
            "primary_lane": outcome.primary.lane.clone(),
            "shadow_artifact_id": shadow.artifact_id.as_str(),
            "shadow_matched": shadow_result_matches_primary(outcome),
            "primary_output_bytes": outcome.primary.output.as_ref().map(|output| output.len()),
            "shadow_output_bytes": shadow.output.as_ref().map(|output| output.len()),
            "primary_latency_ms": outcome.primary.latency_ms,
            "shadow_latency_ms": shadow.latency_ms,
            "primary_error": outcome.primary.error.as_deref(),
            "primary_error_kind": module_artifact_error_kind(outcome.primary.error.as_deref()),
            "shadow_error": shadow.error.as_deref(),
            "shadow_error_kind": module_artifact_error_kind(shadow.error.as_deref()),
        })),
    )
}

fn module_artifact_error_kind(error: Option<&str>) -> Option<&'static str> {
    let error = error?.trim();
    if error.is_empty() {
        return None;
    }
    let normalized = error.to_ascii_lowercase();
    if normalized.contains("timed out") || normalized.contains("timeout") {
        return Some("timeout");
    }
    if normalized.contains("policy") && normalized.contains("blocked") {
        return Some("policy-blocked");
    }
    Some("runtime-error")
}

fn replay_metric(name: &str, value: i64) -> EvaluationMetric {
    EvaluationMetric {
        name: name.to_string(),
        value: MetricValue::Integer(value),
        sample_count: None,
    }
}

fn percentile_nearest_rank(mut values: Vec<u64>, percentile: u64) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let percentile = percentile.clamp(1, 100);
    let rank = (values.len() as u64 * percentile).div_ceil(100);
    let index = rank.saturating_sub(1).min(values.len() as u64 - 1) as usize;
    Some(values[index])
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
        "artifact is missing supported signature or verified registry/candidate-bundle attestation"
            .to_string(),
    )
}

fn stable_route_bucket_bps(module_id: &ModuleId, route_key: &str) -> u16 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in module_id
        .as_str()
        .as_bytes()
        .iter()
        .chain(std::iter::once(&0))
        .chain(route_key.as_bytes())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % 10_000) as u16
}

fn has_supported_content_hash(content_hash: &str) -> bool {
    let Some((algorithm, digest)) = content_hash.split_once(':') else {
        return false;
    };
    matches!(algorithm, "sha256" | "blake3") && !digest.trim().is_empty()
}

fn is_supported_attestation_ref(attestation: &str) -> bool {
    attestation.ends_with("candidate-bundle-registry-record.json")
        || attestation.ends_with("bundle-attestation.json")
        || attestation.starts_with("registry://candidate-bundle/")
        || attestation.starts_with("in-toto:")
}

pub fn sign_artifact_manifest_with_node_identity(
    artifact: &ArtifactManifest,
    identity: &NodeIdentity,
) -> Result<String, ArtifactSignatureError> {
    let key_pair = identity
        .key_pair()
        .map_err(|source| ArtifactSignatureError::SigningKey {
            detail: source.to_string(),
        })?;
    let payload = artifact_signature_payload(artifact)?;
    let signature = URL_SAFE_NO_PAD.encode(key_pair.sign(payload.as_bytes()).as_ref());
    Ok(format!(
        "{LOCAL_ED25519_ARTIFACT_SIGNATURE_SCHEME}:{}:{signature}",
        identity.public_key
    ))
}

pub fn verify_artifact_signature(
    artifact: &ArtifactManifest,
) -> Result<(), ArtifactSignatureError> {
    let parsed = parse_local_ed25519_artifact_signature(artifact)?;
    let public_key = URL_SAFE_NO_PAD
        .decode(parsed.public_key.as_bytes())
        .map_err(|_| ArtifactSignatureError::MalformedPublicKey)?;
    let signature = URL_SAFE_NO_PAD
        .decode(parsed.signature.as_bytes())
        .map_err(|_| ArtifactSignatureError::MalformedSignature)?;
    let payload = artifact_signature_payload(artifact)?;
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(payload.as_bytes(), &signature)
        .map_err(|_| ArtifactSignatureError::Invalid)
}

pub fn artifact_signature_signer_public_key(
    artifact: &ArtifactManifest,
) -> Result<String, ArtifactSignatureError> {
    parse_local_ed25519_artifact_signature(artifact).map(|parsed| parsed.public_key)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParsedArtifactSignature {
    public_key: String,
    signature: String,
}

fn parse_local_ed25519_artifact_signature(
    artifact: &ArtifactManifest,
) -> Result<ParsedArtifactSignature, ArtifactSignatureError> {
    let signature = artifact
        .signature
        .as_deref()
        .ok_or(ArtifactSignatureError::Missing)?;
    let mut parts = signature.split(':');
    let scheme = parts.next().unwrap_or_default();
    if scheme != LOCAL_ED25519_ARTIFACT_SIGNATURE_SCHEME {
        return Err(ArtifactSignatureError::UnsupportedScheme {
            scheme: scheme.to_string(),
        });
    }
    let public_key = parts
        .next()
        .filter(|value| !value.trim().is_empty())
        .ok_or(ArtifactSignatureError::Malformed)?;
    let signature = parts
        .next()
        .filter(|value| !value.trim().is_empty())
        .ok_or(ArtifactSignatureError::Malformed)?;
    if parts.next().is_some() {
        return Err(ArtifactSignatureError::Malformed);
    }
    Ok(ParsedArtifactSignature {
        public_key: public_key.to_string(),
        signature: signature.to_string(),
    })
}

pub fn artifact_signature_payload(
    artifact: &ArtifactManifest,
) -> Result<String, ArtifactSignatureError> {
    let mut unsigned = artifact.clone();
    unsigned.signature = None;
    let target_ring = serde_json::to_string(&unsigned.target_ring).map_err(|source| {
        ArtifactSignatureError::Serialize {
            detail: source.to_string(),
        }
    })?;
    let module_manifest = serde_json::to_string(&unsigned.module_manifest).map_err(|source| {
        ArtifactSignatureError::Serialize {
            detail: source.to_string(),
        }
    })?;
    let provenance = serde_json::to_string(&unsigned.provenance).map_err(|source| {
        ArtifactSignatureError::Serialize {
            detail: source.to_string(),
        }
    })?;
    Ok(format!(
        "{ARTIFACT_SIGNATURE_PROTOCOL}\n{}\n{}\n{}\n{}\n{}\n{}",
        unsigned.artifact_id,
        unsigned.module_id,
        target_ring,
        unsigned.content_hash,
        module_manifest,
        provenance
    ))
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ArtifactSignatureError {
    #[error("artifact is missing a signature")]
    Missing,
    #[error("unsupported artifact signature scheme `{scheme}`")]
    UnsupportedScheme { scheme: String },
    #[error("artifact signature is malformed")]
    Malformed,
    #[error("artifact signature public key is malformed")]
    MalformedPublicKey,
    #[error("artifact signature bytes are malformed")]
    MalformedSignature,
    #[error("artifact signature is invalid")]
    Invalid,
    #[error("artifact signing key could not be loaded: {detail}")]
    SigningKey { detail: String },
    #[error("artifact signature payload could not be serialized: {detail}")]
    Serialize { detail: String },
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ModuleLoadError {
    #[error("load request targets {actual}, but loader belongs to {expected}")]
    WrongNode { expected: NodeId, actual: NodeId },
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SinomoModuleV1ResultError {
    #[error("sinomo-module-v1 result is invalid JSON: {detail}")]
    InvalidJson { detail: String },
    #[error("sinomo-module-v1 result omitted non-empty `status`")]
    MissingStatus,
    #[error("sinomo-module-v1 result events_json[{index}] is invalid JSON: {detail}")]
    InvalidEventJson { index: usize, detail: String },
    #[error("sinomo-module-v1 result metrics_json is invalid JSON: {detail}")]
    InvalidMetricsJson { detail: String },
    #[error("sinomo-module-v1 result could not be serialized: {detail}")]
    Serialize { detail: String },
}

#[derive(Debug, Error)]
pub enum ModuleLoaderStoreError {
    #[error("module loader state I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("module loader state object error at {path}: {detail}")]
    Object { path: String, detail: String },
}

#[derive(Debug, Error)]
pub enum ModuleInvocationTelemetryError {
    #[error(transparent)]
    ModuleStore(#[from] ModuleLoaderStoreError),
    #[error(transparent)]
    EventStore(#[from] EventStoreError),
}

fn read_object<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, ModuleLoaderStoreError> {
    let bytes = fs::read(path).map_err(|source| ModuleLoaderStoreError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_ext::from_bytes(&bytes).map_err(|source| ModuleLoaderStoreError::Object {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn write_object<T: Serialize>(path: &Path, value: &T) -> Result<(), ModuleLoaderStoreError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| ModuleLoaderStoreError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let bytes =
        serde_ext::to_bytes(value, MODULE_OBJECT_VERSION, LengthMode::U32).map_err(|source| {
            ModuleLoaderStoreError::Object {
                path: path.display().to_string(),
                detail: source.to_string(),
            }
        })?;
    fs::write(path, bytes).map_err(|source| ModuleLoaderStoreError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    use sinomo_storage::{STORAGE_CONTRACT_OBJECT_VERSION, StorageRing};

    layout.register_escape_path(
        "sinomo-module",
        StorageRing::R1,
        "module/loader-state",
        "node-local module loader runtime state keyed by module id",
    )?;
    layout.register_sinomo_object(
        "sinomo-module",
        StorageRing::R1,
        "module/loader-state/example.sobj",
        sinomo_storage::ObjectSchema::new(
            "ModuleLoaderRuntimeState",
            STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("node_id", "NodeId")
        .field("rollout_plan_id", "RolloutPlanId")
        .field("module_id", "ModuleId")
        .field("loader_state", "ModuleLoaderState")
        .field("traffic_weight_bps", "u16")
        .field("last_rollout_action", "String")
        .field("last_rollout_stage", "String")
        .field("last_transition_record_id", "Id")
        .field("reason", "String")
        .field("applied_at", "String"),
    )?;
    layout.register_sinomo_object(
        "sinomo-module",
        StorageRing::R1,
        "module/local-command-artifacts.sobj",
        sinomo_storage::ObjectSchema::new(
            "ModuleLocalCommandRegistry",
            STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("artifacts", "Vec<ModuleLocalCommandArtifact>"),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        ActorKind, ActorRef, ArtifactManifest, ArtifactProvenance, CapabilityDiff, Id,
        ModuleManifest, RollbackPolicy, RolloutStrategy,
    };
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    type PolicyDecisionId = Id;

    #[test]
    fn r3_allowed_artifact_loads_in_shadow() {
        let node_id = NodeId::from_legacy_named("node_desktop").expect("node");
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
                attestations: vec!["bundle/candidate-bundle-registry-record.json".to_string()],
            },
        };
        let request = LoadRequest {
            node_id: node_id.clone(),
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
            rollout_plan: RolloutPlan {
                rollout_plan_id: RolloutPlanId::from_legacy_named("rollout_ok").expect("rollout"),
                artifact_id,
                module_id,
                target_ring: TrustRing::R3SandboxedModule,
                strategy: RolloutStrategy::Shadow,
                target_nodes: vec![node_id.clone()],
                initial_weight_bps: 0,
                promotion_steps_bps: Vec::new(),
                rollback_policy: RollbackPolicy::default(),
            },
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
        let artifact = ArtifactManifest {
            artifact_id: artifact_id.clone(),
            module_id: module_id.clone(),
            target_ring: TrustRing::R3SandboxedModule,
            content_hash: "blake3:abc".to_string(),
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
        let request = LoadRequest {
            node_id: node_id.clone(),
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
            rollout_plan: RolloutPlan {
                rollout_plan_id: RolloutPlanId::from_legacy_named("rollout_ok").expect("rollout"),
                artifact_id,
                module_id,
                target_ring: TrustRing::R3SandboxedModule,
                strategy: RolloutStrategy::Shadow,
                target_nodes: vec![node_id.clone()],
                initial_weight_bps: 0,
                promotion_steps_bps: Vec::new(),
                rollback_policy: RollbackPolicy::default(),
            },
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
        let identity_path = unique_identity_path("valid-artifact-signature");
        let identity = sinomo_node::load_or_create_node_identity(&identity_path).expect("identity");
        let mut artifact = ArtifactManifest {
            artifact_id: artifact_id.clone(),
            module_id: module_id.clone(),
            target_ring: TrustRing::R3SandboxedModule,
            content_hash: "blake3:abc".to_string(),
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
        artifact.signature =
            Some(sign_artifact_manifest_with_node_identity(&artifact, &identity).expect("sign"));
        assert_eq!(
            artifact_signature_signer_public_key(&artifact).expect("signer"),
            identity.public_key
        );
        let request = LoadRequest {
            node_id: node_id.clone(),
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
            rollout_plan: RolloutPlan {
                rollout_plan_id: RolloutPlanId::from_legacy_named("rollout_ok").expect("rollout"),
                artifact_id,
                module_id,
                target_ring: TrustRing::R3SandboxedModule,
                strategy: RolloutStrategy::Shadow,
                target_nodes: vec![node_id.clone()],
                initial_weight_bps: 0,
                promotion_steps_bps: Vec::new(),
                rollback_policy: RollbackPolicy::default(),
            },
        };

        let decision = ModuleLoader::new(node_id)
            .evaluate_load(request)
            .expect("load");

        assert_eq!(decision.action, LoaderAction::AcceptShadow);
        let _ = std::fs::remove_file(identity_path);
    }

    #[test]
    fn module_loader_runtime_state_roundtrips_from_data_dir() {
        let root = unique_temp("module-loader-runtime-state");
        let node_id = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let state = ModuleLoaderRuntimeState {
            node_id,
            rollout_plan_id: RolloutPlanId::from_legacy_named("rollout_candidate")
                .expect("rollout"),
            module_id: module_id.clone(),
            loader_state: ModuleLoaderState {
                module_id: module_id.clone(),
                active_artifact_id: artifact_id.clone(),
                previous_artifact_id: None,
                shadow_artifact_id: Some(artifact_id.clone()),
                candidate_artifact_id: Some(artifact_id),
                hot_swap_state: HotSwapState::ActiveCandidate,
            },
            traffic_weight_bps: 500,
            last_rollout_action: "advance-canary".to_string(),
            last_rollout_stage: "canary".to_string(),
            last_transition_record_id: Id::from_legacy_named("rollout_transition")
                .expect("transition"),
            reason: "test apply".to_string(),
            applied_at: module_loader_applied_at_now(),
        };

        let path = write_module_loader_runtime_state(&root, &state).expect("write state");

        assert_eq!(path, module_loader_state_path(&root, &module_id));
        assert_eq!(
            read_module_loader_runtime_state(&root, &module_id).expect("read state"),
            Some(state)
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn module_route_keeps_shadow_artifact_out_of_primary_lane() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&shadow_artifact_id),
            HotSwapState::Shadowing,
            0,
        );

        let route = route_module_invocation_from_state(&state, "session-alpha");

        assert_eq!(route.primary_lane, ModuleRouteLane::Active);
        assert_eq!(route.primary_artifact_id, active_artifact_id);
        assert_eq!(route.shadow_artifact_id, Some(shadow_artifact_id));
        assert_eq!(route.traffic_weight_bps, 0);
    }

    #[test]
    fn module_route_uses_stable_bucket_for_canary_weight() {
        let root = unique_temp("module-route-canary");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&candidate_artifact_id),
            HotSwapState::ActiveCandidate,
            500,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        let candidate_key = route_key_with_bucket(&module_id, 0..500);
        let active_key = route_key_with_bucket(&module_id, 500..10_000);

        let candidate_route = route_module_invocation(
            &root,
            &ModuleRouteRequest {
                module_id: module_id.clone(),
                route_key: candidate_key,
            },
        )
        .expect("route")
        .expect("state");
        let active_route = route_module_invocation(
            &root,
            &ModuleRouteRequest {
                module_id,
                route_key: active_key,
            },
        )
        .expect("route")
        .expect("state");

        assert_eq!(candidate_route.primary_lane, ModuleRouteLane::Candidate);
        assert_eq!(candidate_route.primary_artifact_id, candidate_artifact_id);
        assert_eq!(active_route.primary_lane, ModuleRouteLane::Active);
        assert_eq!(active_route.primary_artifact_id, active_artifact_id);
        assert_eq!(candidate_route.traffic_weight_bps, 500);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn module_route_treats_full_promotion_as_active_lane() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let state = runtime_state(
            &module_id,
            &artifact_id,
            Some(&artifact_id),
            HotSwapState::ActiveCandidate,
            10_000,
        );

        let route = route_module_invocation_from_state(&state, "session-alpha");

        assert_eq!(route.primary_lane, ModuleRouteLane::Active);
        assert_eq!(route.primary_artifact_id, artifact_id);
        assert_eq!(route.traffic_weight_bps, 10_000);
    }

    #[test]
    fn routed_execution_runs_primary_and_shadow_without_overriding_primary() {
        let root = unique_temp("module-routed-execution-shadow");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&shadow_artifact_id),
            HotSwapState::Shadowing,
            0,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("active-output".to_string())),
            (shadow_artifact_id.clone(), Ok("shadow-output".to_string())),
        ]);

        let outcome = execute_routed_module_invocation(
            &root,
            &ModuleInvocationRequest {
                module_id,
                route_key: "session-alpha".to_string(),
                input: "{\"event\":\"example\"}".to_string(),
            },
            &runner,
        )
        .expect("execute")
        .expect("route");

        assert_eq!(outcome.primary.artifact_id, active_artifact_id);
        assert_eq!(outcome.primary.lane, ModuleInvocationLane::Active);
        assert_eq!(outcome.primary.output.as_deref(), Some("active-output"));
        assert_eq!(
            outcome.shadow.as_ref().map(|result| &result.artifact_id),
            Some(&shadow_artifact_id)
        );
        assert_eq!(
            outcome
                .shadow
                .as_ref()
                .and_then(|result| result.output.as_deref()),
            Some("shadow-output")
        );
        let calls = runner.calls.borrow();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].lane, ModuleInvocationLane::Active);
        assert_eq!(calls[1].lane, ModuleInvocationLane::Shadow);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn routed_execution_records_shadow_failure_without_failing_primary() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&shadow_artifact_id),
            HotSwapState::Shadowing,
            0,
        );
        let route = route_module_invocation_from_state(&state, "session-alpha");
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("active-output".to_string())),
            (shadow_artifact_id.clone(), Err("shadow panic".to_string())),
        ]);

        let outcome = execute_module_route_decision(route, "input".to_string(), &runner);

        assert_eq!(outcome.primary.output.as_deref(), Some("active-output"));
        assert_eq!(
            outcome
                .shadow
                .as_ref()
                .and_then(|result| result.error.as_deref()),
            Some("shadow panic")
        );
    }

    #[test]
    fn routed_execution_uses_candidate_primary_for_canary_route() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&candidate_artifact_id),
            HotSwapState::ActiveCandidate,
            500,
        );
        let route_key = route_key_with_bucket(&module_id, 0..500);
        let route = route_module_invocation_from_state(&state, &route_key);
        let runner = StaticModuleRunner::new([
            (active_artifact_id, Ok("active-output".to_string())),
            (
                candidate_artifact_id.clone(),
                Ok("candidate-output".to_string()),
            ),
        ]);

        let outcome = execute_module_route_decision(route, "input".to_string(), &runner);

        assert_eq!(outcome.primary.artifact_id, candidate_artifact_id);
        assert_eq!(outcome.primary.lane, ModuleInvocationLane::Candidate);
        assert_eq!(outcome.primary.output.as_deref(), Some("candidate-output"));
        assert!(outcome.shadow.is_none());
    }

    #[test]
    fn module_replay_fixture_records_matching_shadow_decisions() {
        let root = unique_temp("module-replay-shadow-match");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&shadow_artifact_id),
            HotSwapState::Shadowing,
            0,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("same-output".to_string())),
            (shadow_artifact_id.clone(), Ok("same-output".to_string())),
        ]);
        let fixture = ModuleReplayFixture {
            fixture_id: Id::from_legacy_named("replay_fixture_match").expect("fixture"),
            module_id,
            cases: vec![ModuleReplayCase::from_event(
                "case-1",
                "session-alpha",
                replay_event("event_match"),
            )],
        };

        let report = run_module_replay_fixture(&root, &fixture, &runner).expect("replay");
        let scorecard = report.scorecard(Some(active_artifact_id), Some(shadow_artifact_id));

        assert_eq!(report.replay_event_count, 1);
        assert_eq!(report.missing_route_count, 0);
        assert_eq!(report.primary_error_count, 0);
        assert_eq!(report.shadow_error_count, 0);
        assert_eq!(report.shadow_mismatch_count, 0);
        assert_eq!(report.cases[0].shadow_matched, Some(true));
        assert_eq!(report.events.len(), 1);
        assert_eq!(report.events[0].kind.as_str(), "module.shadow.decision");
        assert_eq!(report.events[0].payload["shadow_matched"], true);
        assert_eq!(scorecard.outcome, EvaluationOutcome::Passed);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn live_routed_execution_appends_shadow_decision_telemetry() {
        let root = unique_temp("module-live-shadow-telemetry");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&shadow_artifact_id),
            HotSwapState::Shadowing,
            0,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("same-output".to_string())),
            (shadow_artifact_id.clone(), Ok("same-output".to_string())),
        ]);
        let mut event_store = sinomo_events::InMemoryEventStore::new();

        let outcome = execute_routed_module_invocation_with_event_store(
            &root,
            &ModuleInvocationRequest {
                module_id: module_id.clone(),
                route_key: "session-alpha".to_string(),
                input: "{\"event\":\"example\"}".to_string(),
            },
            &runner,
            &mut event_store,
        )
        .expect("execute")
        .expect("route");
        let events = event_store
            .replay(sinomo_events::ReplayQuery::by_kind_prefix(
                MODULE_SHADOW_DECISION_EVENT_KIND,
            ))
            .expect("replay events");
        let report = ModuleShadowTelemetryReport::from_stored_events(module_id.clone(), &events);
        let scorecard = report.scorecard(
            Some(active_artifact_id.clone()),
            Some(shadow_artifact_id.clone()),
        );

        assert_eq!(outcome.primary.artifact_id, active_artifact_id);
        assert_eq!(
            outcome.shadow.as_ref().map(|shadow| &shadow.artifact_id),
            Some(&shadow_artifact_id)
        );
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.source.as_str(), "module.live");
        assert_eq!(events[0].event.payload["telemetry_source"], "live");
        assert_eq!(events[0].event.payload["shadow_matched"], true);
        assert_eq!(report.event_count, 1);
        assert_eq!(report.matched_count, 1);
        assert_eq!(report.mismatch_count, 0);
        assert_eq!(scorecard.outcome, EvaluationOutcome::Passed);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn live_routed_execution_appends_canary_invocation_telemetry_without_shadow() {
        let root = unique_temp("module-live-canary-telemetry");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&candidate_artifact_id),
            HotSwapState::ActiveCandidate,
            500,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("active-output".to_string())),
            (
                candidate_artifact_id.clone(),
                Ok("candidate-output".to_string()),
            ),
        ]);
        let mut event_store = sinomo_events::InMemoryEventStore::new();
        let route_key = route_key_with_bucket(&module_id, 0..500);

        let outcome = execute_routed_module_invocation_with_event_store(
            &root,
            &ModuleInvocationRequest {
                module_id: module_id.clone(),
                route_key: route_key.clone(),
                input: "{\"event\":\"example\"}".to_string(),
            },
            &runner,
            &mut event_store,
        )
        .expect("execute")
        .expect("route");
        let invocation_events = event_store
            .replay(sinomo_events::ReplayQuery::by_kind_prefix(
                MODULE_INVOCATION_DECISION_EVENT_KIND,
            ))
            .expect("replay invocation events");
        let shadow_events = event_store
            .replay(sinomo_events::ReplayQuery::by_kind_prefix(
                MODULE_SHADOW_DECISION_EVENT_KIND,
            ))
            .expect("replay shadow events");
        let report = ModuleCanaryTelemetryReport::from_stored_events(
            module_id.clone(),
            Some(&candidate_artifact_id),
            &invocation_events,
        );
        let scorecard = report.scorecard(
            Some(active_artifact_id.clone()),
            Some(candidate_artifact_id.clone()),
            1,
            None,
        );

        assert_eq!(outcome.primary.artifact_id, candidate_artifact_id);
        assert_eq!(outcome.primary.lane, ModuleInvocationLane::Candidate);
        assert!(outcome.shadow.is_none());
        assert_eq!(invocation_events.len(), 1);
        assert_eq!(shadow_events.len(), 0);
        assert_eq!(invocation_events[0].event.source.as_str(), "module.live");
        assert_eq!(invocation_events[0].event.payload["route_key"], route_key);
        assert_eq!(
            invocation_events[0].event.payload["primary_lane"],
            "candidate"
        );
        assert_eq!(
            invocation_events[0].event.payload["traffic_weight_bps"],
            500
        );
        assert!(
            invocation_events[0]
                .event
                .payload
                .get("primary_latency_ms")
                .and_then(serde_json::Value::as_u64)
                .is_some()
        );
        assert_eq!(report.event_count, 1);
        assert_eq!(report.candidate_primary_count, 1);
        assert_eq!(report.active_primary_count, 0);
        assert_eq!(report.candidate_error_count, 0);
        assert_eq!(report.candidate_latency_sample_count, 1);
        assert_eq!(scorecard.outcome, EvaluationOutcome::Passed);
        assert_eq!(scorecard.gates[0].gate, EvaluationGate::CanaryAnalysis);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn invocation_decision_telemetry_classifies_timed_out_candidate_error() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let route_key = route_key_with_bucket(&module_id, 0..500);
        let route = route_module_invocation_from_state(
            &runtime_state(
                &module_id,
                &active_artifact_id,
                Some(&candidate_artifact_id),
                HotSwapState::ActiveCandidate,
                500,
            ),
            &route_key,
        );
        let runner = StaticModuleRunner::new([
            (active_artifact_id, Ok("active-output".to_string())),
            (
                candidate_artifact_id,
                Err("local command artifact timed out after 20ms".to_string()),
            ),
        ]);
        let outcome = execute_module_route_decision(route, "input".to_string(), &runner);
        let event =
            module_invocation_decision_event_from_outcome(None, &route_key, None, "live", &outcome);

        assert_eq!(event.payload["primary_error_kind"], "timeout");
        assert_eq!(event.payload["shadow_error_kind"], serde_json::Value::Null);
    }

    #[test]
    fn canary_telemetry_report_requires_candidate_events_and_fails_candidate_errors() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let route_key = route_key_with_bucket(&module_id, 0..500);
        let route = route_module_invocation_from_state(
            &runtime_state(
                &module_id,
                &active_artifact_id,
                Some(&candidate_artifact_id),
                HotSwapState::ActiveCandidate,
                500,
            ),
            &route_key,
        );
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("active-output".to_string())),
            (
                candidate_artifact_id.clone(),
                Err("candidate panic".to_string()),
            ),
        ]);
        let outcome = execute_module_route_decision(route, "input".to_string(), &runner);
        let event =
            module_invocation_decision_event_from_outcome(None, &route_key, None, "live", &outcome);
        let mut event_store = sinomo_events::InMemoryEventStore::new();
        event_store.append(event).expect("append event");
        let events = event_store
            .replay(sinomo_events::ReplayQuery::by_kind_prefix(
                MODULE_INVOCATION_DECISION_EVENT_KIND,
            ))
            .expect("replay events");
        let report = ModuleCanaryTelemetryReport::from_stored_events(
            module_id,
            Some(&candidate_artifact_id),
            &events,
        );

        assert_eq!(report.candidate_primary_count, 1);
        assert_eq!(report.candidate_error_count, 1);
        assert_eq!(report.outcome(2, None), EvaluationOutcome::Inconclusive);
        assert_eq!(report.outcome(1, None), EvaluationOutcome::Failed);
        assert_eq!(
            report
                .scorecard(
                    Some(active_artifact_id),
                    Some(candidate_artifact_id),
                    1,
                    None
                )
                .gates[0]
                .gate,
            EvaluationGate::CanaryAnalysis
        );
    }

    #[test]
    fn canary_telemetry_report_fails_candidate_latency_gate() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let route_key = route_key_with_bucket(&module_id, 0..500);
        let route = route_module_invocation_from_state(
            &runtime_state(
                &module_id,
                &active_artifact_id,
                Some(&candidate_artifact_id),
                HotSwapState::ActiveCandidate,
                500,
            ),
            &route_key,
        );
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("active-output".to_string())),
            (
                candidate_artifact_id.clone(),
                Ok("candidate-output".to_string()),
            ),
        ]);
        let outcome = execute_module_route_decision(route, "input".to_string(), &runner);
        let mut event =
            module_invocation_decision_event_from_outcome(None, &route_key, None, "live", &outcome);
        event.payload["primary_latency_ms"] = serde_json::json!(250);
        let mut event_store = sinomo_events::InMemoryEventStore::new();
        event_store.append(event).expect("append event");
        let events = event_store
            .replay(sinomo_events::ReplayQuery::by_kind_prefix(
                MODULE_INVOCATION_DECISION_EVENT_KIND,
            ))
            .expect("replay events");
        let report = ModuleCanaryTelemetryReport::from_stored_events(
            module_id,
            Some(&candidate_artifact_id),
            &events,
        );
        let scorecard = report.scorecard(
            Some(active_artifact_id),
            Some(candidate_artifact_id),
            1,
            Some(200),
        );

        assert_eq!(report.candidate_latency_p95_ms, Some(250));
        assert_eq!(report.outcome(1, Some(200)), EvaluationOutcome::Failed);
        assert_eq!(scorecard.outcome, EvaluationOutcome::Failed);
        assert!(scorecard.metrics.iter().any(|metric| {
            metric.name == "module_canary_candidate_latency_p95_ms"
                && metric.value == MetricValue::Integer(250)
        }));
    }

    #[test]
    fn canary_telemetry_report_fails_candidate_user_correction() {
        let node_id = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let candidate_artifact_id =
            ArtifactId::from_legacy_named("artifact_candidate").expect("candidate");
        let correction_event = EventEnvelope::new(
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
        .with_target_node(node_id)
        .with_payload(serde_json::json!({
            "module_id": module_id.as_str(),
            "corrected_artifact_id": candidate_artifact_id.as_str(),
            "primary_lane": "candidate",
            "reason": "user corrected candidate output",
        }));
        let mut event_store = sinomo_events::InMemoryEventStore::new();
        event_store
            .append(correction_event)
            .expect("append correction");
        let events = event_store
            .replay(sinomo_events::ReplayQuery::by_kind_prefix(
                MODULE_USER_CORRECTION_EVENT_KIND,
            ))
            .expect("replay corrections");
        let report = ModuleCanaryTelemetryReport::from_stored_events(
            module_id,
            Some(&candidate_artifact_id),
            &events,
        );
        let scorecard = report.scorecard(
            Some(active_artifact_id),
            Some(candidate_artifact_id),
            10,
            None,
        );

        assert_eq!(report.correction_event_count, 1);
        assert_eq!(report.candidate_user_correction_count, 1);
        assert_eq!(report.candidate_primary_count, 0);
        assert_eq!(report.outcome(10, None), EvaluationOutcome::Failed);
        assert_eq!(scorecard.outcome, EvaluationOutcome::Failed);
        assert!(scorecard.metrics.iter().any(|metric| {
            metric.name == "module_canary_candidate_user_correction_count"
                && metric.value == MetricValue::Integer(1)
        }));
    }

    #[test]
    fn shadow_telemetry_report_fails_live_mismatches() {
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let route = route_module_invocation_from_state(
            &runtime_state(
                &module_id,
                &active_artifact_id,
                Some(&shadow_artifact_id),
                HotSwapState::Shadowing,
                0,
            ),
            "session-alpha",
        );
        let runner = StaticModuleRunner::new([
            (active_artifact_id.clone(), Ok("active-output".to_string())),
            (shadow_artifact_id.clone(), Err("shadow panic".to_string())),
        ]);
        let outcome = execute_module_route_decision(route, "input".to_string(), &runner);
        let event = module_shadow_decision_event_from_outcome(
            None,
            "session-alpha",
            None,
            "live",
            &outcome,
        )
        .expect("shadow event");
        let mut event_store = sinomo_events::InMemoryEventStore::new();
        event_store.append(event).expect("append event");
        let events = event_store
            .replay(sinomo_events::ReplayQuery::by_kind_prefix(
                MODULE_SHADOW_DECISION_EVENT_KIND,
            ))
            .expect("replay events");
        let report = ModuleShadowTelemetryReport::from_stored_events(module_id, &events);
        let scorecard = report.scorecard(Some(active_artifact_id), Some(shadow_artifact_id));

        assert_eq!(report.event_count, 1);
        assert_eq!(report.mismatch_count, 1);
        assert_eq!(report.shadow_error_count, 1);
        assert_eq!(scorecard.outcome, EvaluationOutcome::Failed);
        assert_eq!(scorecard.gates[0].gate, EvaluationGate::ShadowMode);
    }

    #[test]
    fn module_replay_fixture_fails_shadow_mismatches_without_overriding_primary() {
        let root = unique_temp("module-replay-shadow-mismatch");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&shadow_artifact_id),
            HotSwapState::Shadowing,
            0,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        let runner = StaticModuleRunner::new([
            (active_artifact_id, Ok("active-output".to_string())),
            (shadow_artifact_id.clone(), Ok("shadow-output".to_string())),
        ]);
        let fixture = ModuleReplayFixture {
            fixture_id: Id::from_legacy_named("replay_fixture_mismatch").expect("fixture"),
            module_id,
            cases: vec![ModuleReplayCase {
                case_id: "case-1".to_string(),
                route_key: "session-alpha".to_string(),
                input: "{\"event\":\"example\"}".to_string(),
                source_event: None,
            }],
        };

        let report = run_module_replay_fixture(&root, &fixture, &runner).expect("replay");
        let scorecard = report.scorecard(None, Some(shadow_artifact_id));

        assert_eq!(
            report.cases[0]
                .outcome
                .as_ref()
                .expect("outcome")
                .primary
                .output
                .as_deref(),
            Some("active-output")
        );
        assert_eq!(report.shadow_mismatch_count, 1);
        assert_eq!(report.cases[0].shadow_matched, Some(false));
        assert_eq!(report.events[0].payload["shadow_matched"], false);
        assert_eq!(scorecard.outcome, EvaluationOutcome::Failed);
        assert_eq!(scorecard.gates[1].gate, EvaluationGate::ShadowMode);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn module_local_command_registry_roundtrips_from_data_dir() {
        let root = unique_temp("module-local-command-registry");
        let artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let registry = ModuleLocalCommandRegistry {
            artifacts: vec![local_command_artifact(
                artifact_id.clone(),
                "printf '%s' registry",
            )],
        };

        let path = write_module_local_command_registry(&root, &registry).expect("write registry");
        let read_back = read_module_local_command_registry(&root)
            .expect("read registry")
            .expect("registry");

        assert_eq!(path, module_local_command_registry_path(&root));
        assert_eq!(
            read_back
                .artifact(&artifact_id)
                .map(|artifact| artifact.program.as_str()),
            Some("sh")
        );
        assert_eq!(read_back, registry);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn local_command_runner_executes_routed_primary_artifact() {
        let root = unique_temp("module-local-command-primary");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            None,
            HotSwapState::Stable,
            0,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        write_module_local_command_registry(
            &root,
            &ModuleLocalCommandRegistry {
                artifacts: vec![local_command_artifact(
                    active_artifact_id.clone(),
                    "printf '%s:%s' \"$SINOMO_INVOCATION_LANE\" \"$(cat)\"",
                )],
            },
        )
        .expect("write registry");
        let runner = LocalCommandModuleArtifactRunner::from_data_dir(&root, &root)
            .expect("local command runner");

        let outcome = execute_routed_module_invocation(
            &root,
            &ModuleInvocationRequest {
                module_id,
                route_key: "session-alpha".to_string(),
                input: "payload".to_string(),
            },
            &runner,
        )
        .expect("execute")
        .expect("route");

        assert_eq!(outcome.primary.artifact_id, active_artifact_id);
        assert_eq!(outcome.primary.lane, ModuleInvocationLane::Active);
        assert_eq!(outcome.primary.output.as_deref(), Some("active:payload"));
        assert!(outcome.shadow.is_none());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn local_command_runner_mirrors_shadow_artifact_without_overriding_primary() {
        let root = unique_temp("module-local-command-shadow");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let active_artifact_id = ArtifactId::from_legacy_named("artifact_active").expect("active");
        let shadow_artifact_id = ArtifactId::from_legacy_named("artifact_shadow").expect("shadow");
        let state = runtime_state(
            &module_id,
            &active_artifact_id,
            Some(&shadow_artifact_id),
            HotSwapState::Shadowing,
            0,
        );
        write_module_loader_runtime_state(&root, &state).expect("write state");
        write_module_local_command_registry(
            &root,
            &ModuleLocalCommandRegistry {
                artifacts: vec![
                    local_command_artifact(
                        active_artifact_id.clone(),
                        "printf '%s:%s' \"$SINOMO_INVOCATION_LANE\" \"$(cat)\"",
                    ),
                    local_command_artifact(
                        shadow_artifact_id.clone(),
                        "printf '%s:%s' \"$SINOMO_INVOCATION_LANE\" \"$(cat)\"",
                    ),
                ],
            },
        )
        .expect("write registry");
        let runner = LocalCommandModuleArtifactRunner::from_data_dir(&root, &root)
            .expect("local command runner");

        let outcome = execute_routed_module_invocation(
            &root,
            &ModuleInvocationRequest {
                module_id,
                route_key: "session-alpha".to_string(),
                input: "payload".to_string(),
            },
            &runner,
        )
        .expect("execute")
        .expect("route");

        assert_eq!(outcome.primary.artifact_id, active_artifact_id);
        assert_eq!(outcome.primary.output.as_deref(), Some("active:payload"));
        assert_eq!(
            outcome.shadow.as_ref().map(|result| &result.artifact_id),
            Some(&shadow_artifact_id)
        );
        assert_eq!(
            outcome
                .shadow
                .as_ref()
                .and_then(|result| result.output.as_deref()),
            Some("shadow:payload")
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn module_v1_wit_contract_exposes_run_boundary() {
        assert!(SINOMO_MODULE_V1_WIT.contains("package sinomo:module-v1;"));
        assert!(SINOMO_MODULE_V1_WIT.contains("interface module"));
        assert!(
            SINOMO_MODULE_V1_WIT.contains(
                "run: func(context: module-context, input-json: string) -> module-result"
            )
        );
        assert!(SINOMO_MODULE_V1_WIT.contains("world sinomo-module-v1"));
    }

    #[test]
    fn module_v1_result_validates_json_payload_contract() {
        let result = SinomoModuleV1Result {
            status: "ok".to_string(),
            events_json: vec![
                serde_json::json!({
                    "kind": "trigger.decision.created",
                    "action": "stay-silent"
                })
                .to_string(),
            ],
            metrics_json: serde_json::json!({"trigger_decisions": 1}).to_string(),
        };

        let output = result.to_json_output().expect("serialize result");
        let parsed = SinomoModuleV1Result::from_json_output(&output).expect("parse result");

        assert_eq!(parsed, result);
    }

    #[test]
    fn module_v1_result_rejects_invalid_nested_json() {
        let error = SinomoModuleV1Result::from_json_output(
            r#"{"status":"ok","events_json":["not-json"],"metrics_json":"{}"}"#,
        )
        .expect_err("invalid event json");

        assert!(matches!(
            error,
            SinomoModuleV1ResultError::InvalidEventJson { index: 0, .. }
        ));
    }

    #[test]
    fn module_route_returns_none_without_loader_state() {
        let root = unique_temp("module-route-missing");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");

        let route = route_module_invocation(
            &root,
            &ModuleRouteRequest {
                module_id,
                route_key: "session-alpha".to_string(),
            },
        )
        .expect("route");

        assert_eq!(route, None);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn policy_allowed_artifact_rejects_tampered_local_ed25519_signature() {
        let node_id = NodeId::from_legacy_named("node_desktop").expect("node");
        let module_id =
            ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
        let artifact_id = ArtifactId::from_legacy_named("artifact_candidate").expect("artifact");
        let identity_path = unique_identity_path("tampered-artifact-signature");
        let identity = sinomo_node::load_or_create_node_identity(&identity_path).expect("identity");
        let mut artifact = ArtifactManifest {
            artifact_id: artifact_id.clone(),
            module_id: module_id.clone(),
            target_ring: TrustRing::R3SandboxedModule,
            content_hash: "blake3:abc".to_string(),
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
        artifact.signature =
            Some(sign_artifact_manifest_with_node_identity(&artifact, &identity).expect("sign"));
        artifact.content_hash = "blake3:tampered".to_string();
        let request = LoadRequest {
            node_id: node_id.clone(),
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
            rollout_plan: RolloutPlan {
                rollout_plan_id: RolloutPlanId::from_legacy_named("rollout_ok").expect("rollout"),
                artifact_id,
                module_id,
                target_ring: TrustRing::R3SandboxedModule,
                strategy: RolloutStrategy::Shadow,
                target_nodes: vec![node_id.clone()],
                initial_weight_bps: 0,
                promotion_steps_bps: Vec::new(),
                rollback_policy: RollbackPolicy::default(),
            },
        };

        let decision = ModuleLoader::new(node_id)
            .evaluate_load(request)
            .expect("load");

        assert_eq!(decision.action, LoaderAction::Reject);
        assert!(decision.reason.contains("signature verification failed"));
        let _ = std::fs::remove_file(identity_path);
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
                RolloutPlanId::from_legacy_named("rollout_candidate").expect("rollout"),
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

    fn unique_identity_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "sinomo-module-{name}-{}.sobj",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ))
    }

    fn unique_temp(prefix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "{}-{}",
            prefix,
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ))
    }

    fn runtime_state(
        module_id: &ModuleId,
        active_artifact_id: &ArtifactId,
        candidate_artifact_id: Option<&ArtifactId>,
        hot_swap_state: HotSwapState,
        traffic_weight_bps: u16,
    ) -> ModuleLoaderRuntimeState {
        ModuleLoaderRuntimeState {
            node_id: NodeId::from_legacy_named("node_desktop").expect("node"),
            rollout_plan_id: RolloutPlanId::from_legacy_named("rollout_candidate")
                .expect("rollout"),
            module_id: module_id.clone(),
            loader_state: ModuleLoaderState {
                module_id: module_id.clone(),
                active_artifact_id: active_artifact_id.clone(),
                previous_artifact_id: None,
                shadow_artifact_id: candidate_artifact_id.cloned(),
                candidate_artifact_id: candidate_artifact_id.cloned(),
                hot_swap_state,
            },
            traffic_weight_bps,
            last_rollout_action: "advance-canary".to_string(),
            last_rollout_stage: "canary".to_string(),
            last_transition_record_id: Id::from_legacy_named("rollout_transition")
                .expect("transition"),
            reason: "test apply".to_string(),
            applied_at: module_loader_applied_at_now(),
        }
    }

    fn route_key_with_bucket(module_id: &ModuleId, range: std::ops::Range<u16>) -> String {
        (0..20_000)
            .map(|index| format!("session-{index}"))
            .find(|route_key| range.contains(&stable_route_bucket_bps(module_id, route_key)))
            .expect("route key in bucket range")
    }

    fn replay_event(name: &str) -> EventEnvelope {
        EventEnvelope::new(
            EventId::from_legacy_named(name).expect("event"),
            OffsetDateTime::now_utc(),
            NodeId::from_legacy_named("node_desktop").expect("node"),
            Source::new("test.replay").expect("source"),
            EventKind::new("test.replay.input").expect("kind"),
            Salience::new(0.5).expect("salience"),
            Confidence::new(0.9).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::Local,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_payload(serde_json::json!({"name": name}))
    }

    fn local_command_artifact(artifact_id: ArtifactId, script: &str) -> ModuleLocalCommandArtifact {
        ModuleLocalCommandArtifact {
            artifact_id,
            program: "sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            cwd: None,
            env: BTreeMap::new(),
            timeout_ms: Some(2_000),
            max_output_bytes: Some(16 * 1024),
        }
    }

    struct StaticModuleRunner {
        outputs: BTreeMap<ArtifactId, Result<String, String>>,
        calls: RefCell<Vec<ModuleArtifactInvocation>>,
    }

    impl StaticModuleRunner {
        fn new<const N: usize>(outputs: [(ArtifactId, Result<String, String>); N]) -> Self {
            Self {
                outputs: outputs.into_iter().collect(),
                calls: RefCell::new(Vec::new()),
            }
        }
    }

    impl ModuleArtifactRunner for StaticModuleRunner {
        fn invoke(&self, invocation: &ModuleArtifactInvocation) -> Result<String, String> {
            self.calls.borrow_mut().push(invocation.clone());
            self.outputs
                .get(&invocation.artifact_id)
                .cloned()
                .unwrap_or_else(|| Err(format!("missing artifact {}", invocation.artifact_id)))
        }
    }
}
