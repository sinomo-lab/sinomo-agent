use sinomo_runtime::core::{
    EvaluationGate, EvaluationGateResult, EvaluationMetric, EvaluationOutcome, EvaluationScorecard,
    MetricValue,
};
use sinomo_runtime::module::{
    ArtifactId, ModuleCanaryTelemetryReport, ModuleReplayReport, ModuleShadowTelemetryReport,
};

pub fn module_replay_report_scorecard(
    report: &ModuleReplayReport,
    baseline_artifact_id: Option<ArtifactId>,
    candidate_artifact_id: Option<ArtifactId>,
) -> EvaluationScorecard {
    let outcome = module_replay_report_outcome(report);
    EvaluationScorecard {
        scorecard_id: sinomo_runtime::core::Id::generate(),
        baseline_artifact_id,
        candidate_artifact_id,
        outcome: outcome.clone(),
        gates: vec![
            EvaluationGateResult {
                gate: EvaluationGate::ReplayTests,
                outcome: if report.missing_route_count == 0 && !report.cases.is_empty() {
                    EvaluationOutcome::Passed
                } else {
                    EvaluationOutcome::Failed
                },
                summary: format!(
                    "{} replay cases, {} missing routes",
                    report.cases.len(),
                    report.missing_route_count
                ),
            },
            EvaluationGateResult {
                gate: EvaluationGate::ShadowMode,
                outcome,
                summary: format!(
                    "{} primary errors, {} shadow errors, {} shadow mismatches",
                    report.primary_error_count,
                    report.shadow_error_count,
                    report.shadow_mismatch_count
                ),
            },
        ],
        metrics: vec![
            module_evidence_metric("module_replay_case_count", report.cases.len() as i64),
            module_evidence_metric(
                "module_replay_event_count",
                report.replay_event_count as i64,
            ),
            module_evidence_metric(
                "module_replay_missing_route_count",
                report.missing_route_count as i64,
            ),
            module_evidence_metric(
                "module_replay_primary_error_count",
                report.primary_error_count as i64,
            ),
            module_evidence_metric(
                "module_replay_shadow_error_count",
                report.shadow_error_count as i64,
            ),
            module_evidence_metric(
                "module_replay_shadow_mismatch_count",
                report.shadow_mismatch_count as i64,
            ),
        ],
    }
}

pub fn module_replay_report_outcome(report: &ModuleReplayReport) -> EvaluationOutcome {
    if report.cases.is_empty() {
        return EvaluationOutcome::Inconclusive;
    }
    if report.missing_route_count == 0
        && report.primary_error_count == 0
        && report.shadow_error_count == 0
        && report.shadow_mismatch_count == 0
    {
        EvaluationOutcome::Passed
    } else {
        EvaluationOutcome::Failed
    }
}

pub fn module_shadow_telemetry_scorecard(
    report: &ModuleShadowTelemetryReport,
    baseline_artifact_id: Option<ArtifactId>,
    candidate_artifact_id: Option<ArtifactId>,
) -> EvaluationScorecard {
    let outcome = module_shadow_telemetry_outcome(report);
    EvaluationScorecard {
        scorecard_id: sinomo_runtime::core::Id::generate(),
        baseline_artifact_id,
        candidate_artifact_id,
        outcome: outcome.clone(),
        gates: vec![EvaluationGateResult {
            gate: EvaluationGate::ShadowMode,
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
            module_evidence_metric("module_shadow_event_count", report.event_count as i64),
            module_evidence_metric("module_shadow_matched_count", report.matched_count as i64),
            module_evidence_metric("module_shadow_mismatch_count", report.mismatch_count as i64),
            module_evidence_metric(
                "module_shadow_primary_error_count",
                report.primary_error_count as i64,
            ),
            module_evidence_metric(
                "module_shadow_shadow_error_count",
                report.shadow_error_count as i64,
            ),
            module_evidence_metric(
                "module_shadow_ignored_event_count",
                report.ignored_event_count as i64,
            ),
        ],
    }
}

pub fn module_shadow_telemetry_outcome(report: &ModuleShadowTelemetryReport) -> EvaluationOutcome {
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

pub fn module_canary_telemetry_scorecard(
    report: &ModuleCanaryTelemetryReport,
    baseline_artifact_id: Option<ArtifactId>,
    candidate_artifact_id: Option<ArtifactId>,
    minimum_candidate_event_count: u64,
    maximum_candidate_latency_p95_ms: Option<u64>,
) -> EvaluationScorecard {
    let outcome = module_canary_telemetry_outcome(
        report,
        minimum_candidate_event_count,
        maximum_candidate_latency_p95_ms,
    );
    let mut metrics = vec![
        module_evidence_metric("module_canary_event_count", report.event_count as i64),
        module_evidence_metric(
            "module_canary_candidate_primary_count",
            report.candidate_primary_count as i64,
        ),
        module_evidence_metric(
            "module_canary_active_primary_count",
            report.active_primary_count as i64,
        ),
        module_evidence_metric(
            "module_canary_correction_event_count",
            report.correction_event_count as i64,
        ),
        module_evidence_metric(
            "module_canary_candidate_user_correction_count",
            report.candidate_user_correction_count as i64,
        ),
        module_evidence_metric(
            "module_canary_active_user_correction_count",
            report.active_user_correction_count as i64,
        ),
        module_evidence_metric(
            "module_canary_unknown_user_correction_count",
            report.unknown_user_correction_count as i64,
        ),
        module_evidence_metric(
            "module_canary_candidate_error_count",
            report.candidate_error_count as i64,
        ),
        module_evidence_metric(
            "module_canary_active_error_count",
            report.active_error_count as i64,
        ),
        module_evidence_metric(
            "module_canary_candidate_latency_sample_count",
            report.candidate_latency_sample_count as i64,
        ),
        module_evidence_metric(
            "module_canary_active_latency_sample_count",
            report.active_latency_sample_count as i64,
        ),
        module_evidence_metric(
            "module_canary_ignored_event_count",
            report.ignored_event_count as i64,
        ),
    ];
    if let Some(latency_ms) = report.candidate_latency_p95_ms {
        metrics.push(module_evidence_metric(
            "module_canary_candidate_latency_p95_ms",
            latency_ms as i64,
        ));
    }
    if let Some(latency_ms) = report.active_latency_p95_ms {
        metrics.push(module_evidence_metric(
            "module_canary_active_latency_p95_ms",
            latency_ms as i64,
        ));
    }
    EvaluationScorecard {
        scorecard_id: sinomo_runtime::core::Id::generate(),
        baseline_artifact_id,
        candidate_artifact_id,
        outcome: outcome.clone(),
        gates: vec![EvaluationGateResult {
            gate: EvaluationGate::CanaryAnalysis,
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

pub fn module_canary_telemetry_outcome(
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

fn module_evidence_metric(name: &str, value: i64) -> EvaluationMetric {
    EvaluationMetric {
        name: name.to_string(),
        value: MetricValue::Integer(value),
        sample_count: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::events::EventId;
    use sinomo_runtime::module::ModuleReplayCaseOutcome;

    #[test]
    fn replay_report_scorecard_is_evolution_evidence() {
        let report = ModuleReplayReport {
            fixture_id: sinomo_runtime::core::Id::from_legacy_named("fixture_module_replay")
                .expect("fixture"),
            module_id: sinomo_runtime::core::Id::from_legacy_named("module_trigger")
                .expect("module"),
            cases: vec![ModuleReplayCaseOutcome {
                case_id: "case-1".to_string(),
                route_key: "case-1".to_string(),
                source_event_id: None,
                outcome: None,
                shadow_matched: Some(true),
            }],
            replay_event_count: 1,
            missing_route_count: 0,
            primary_error_count: 0,
            shadow_error_count: 0,
            shadow_mismatch_count: 0,
            events: Vec::new(),
        };

        let scorecard = module_replay_report_scorecard(&report, None, None);

        assert_eq!(scorecard.outcome, EvaluationOutcome::Passed);
        assert!(scorecard.gates.iter().any(|gate| {
            gate.gate == EvaluationGate::ReplayTests && gate.outcome == EvaluationOutcome::Passed
        }));
    }

    #[test]
    fn shadow_telemetry_scorecard_fails_mismatch() {
        let report = ModuleShadowTelemetryReport {
            module_id: sinomo_runtime::core::Id::from_legacy_named("module_trigger")
                .expect("module"),
            event_count: 1,
            matched_count: 0,
            mismatch_count: 1,
            primary_error_count: 0,
            shadow_error_count: 1,
            ignored_event_count: 0,
            event_ids: vec![EventId::generate()],
        };

        let scorecard = module_shadow_telemetry_scorecard(&report, None, None);

        assert_eq!(scorecard.outcome, EvaluationOutcome::Failed);
        assert_eq!(scorecard.gates[0].gate, EvaluationGate::ShadowMode);
    }

    #[test]
    fn canary_telemetry_scorecard_checks_candidate_thresholds() {
        let report = ModuleCanaryTelemetryReport {
            module_id: sinomo_runtime::core::Id::from_legacy_named("module_trigger")
                .expect("module"),
            event_count: 1,
            active_primary_count: 0,
            candidate_primary_count: 1,
            correction_event_count: 0,
            active_user_correction_count: 0,
            candidate_user_correction_count: 0,
            unknown_user_correction_count: 0,
            active_error_count: 0,
            candidate_error_count: 0,
            active_latency_sample_count: 0,
            candidate_latency_sample_count: 1,
            active_latency_p95_ms: None,
            candidate_latency_p95_ms: Some(250),
            ignored_event_count: 0,
            event_ids: vec![EventId::generate()],
        };

        let scorecard = module_canary_telemetry_scorecard(&report, None, None, 1, Some(200));

        assert_eq!(scorecard.outcome, EvaluationOutcome::Failed);
        assert!(scorecard.metrics.iter().any(|metric| {
            metric.name == "module_canary_candidate_latency_p95_ms"
                && metric.value == MetricValue::Integer(250)
        }));
    }
}
