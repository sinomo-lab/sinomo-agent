//! Self-evolution runtime test cases.
//!
//! These cases are deterministic local probes that exercise the same
//! orchestration paths used by `sinomo evolve` without invoking a model or a
//! live cluster.

use crate::{
    CandidateWorkspace, EvolutionJob, EvolutionJobStatus, EvolutionOrchestrator, EvolutionRequest,
    LocalCandidateBundleStore, VerificationCommand, WorkspaceAdapter, WorkspaceError,
};
use sinomo_runtime::core::{EvaluationGate, EvaluationOutcome, TrustRing};
use sinomo_runtime::eval::{ExecutingEvalHarness, LocalCommandRunner};
use sinomo_runtime::test::{
    SinomoTestContext, SinomoTestError, SinomoTestOutcome, SinomoTestRegistryBuilder,
    SinomoTestResult,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn register_evolution_test_cases(
    builder: SinomoTestRegistryBuilder,
) -> SinomoTestRegistryBuilder {
    builder.register(evolution_self_dry_run())
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn evolution_self_dry_run(ctx) {
        id: "evolution.self-dry-run",
        summary: "Self-evolution candidate dry run writes a bundle, mutation packet, and verification scorecard without invoking a model",
        tags: ["self-evolution", "agent-readiness", "local-readiness"],
        requirements: [],
        run: {
            run_evolution_self_dry_run(ctx)
        }
    }
}

fn run_evolution_self_dry_run(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let root = self_evolution_test_root(ctx);
    let candidate = root.join("candidate");
    fs::create_dir_all(&candidate).map_err(|source| {
        SinomoTestError::failed(format!(
            "failed to create dry-run candidate workspace {}: {source}",
            candidate.display()
        ))
    })?;

    let orchestrator = EvolutionOrchestrator::new(
        TestWorkspace {
            root: root.clone(),
            candidate: candidate.clone(),
        },
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(root.join("candidates")),
    );

    let report = orchestrator
        .propose(
            EvolutionRequest {
                objective: "agent readiness self-evolution dry run".to_string(),
                target_ring: TrustRing::R2TrustedService,
                intended_paths: vec!["crates/sinomo-agent/**".to_string()],
                verification_commands: vec![VerificationCommand {
                    name: "dry-run-verification".to_string(),
                    command: "exit 0".to_string(),
                    expected_exit_code: 0,
                    gate: EvaluationGate::StaticChecks,
                }],
                benchmark_gates: Vec::new(),
                module_replay_reports: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: true,
            },
            None,
        )
        .map_err(|source| {
            SinomoTestError::failed(format!("self-evolution dry-run proposal failed: {source}"))
        })?;

    ctx.record("job_id", &report.job.job_id);
    ctx.record(
        "candidate_workspace",
        report.candidate_workspace.path.display().to_string(),
    );
    ctx.record("bundle_path", &report.bundle.bundle_path);
    ctx.record("mutation_packet_path", &report.bundle.mutation_packet_path);
    ctx.record("scorecard_path", &report.bundle.scorecard_path);
    ctx.record("command_logs", &report.bundle.command_logs);

    let scorecard = report
        .scorecard
        .as_ref()
        .ok_or_else(|| SinomoTestError::failed("dry-run report did not include a scorecard"))?;
    let mut failures = Vec::new();
    if report.job.status != EvolutionJobStatus::AwaitingApproval {
        failures.push(format!("unexpected job status {:?}", report.job.status));
    }
    if scorecard.outcome != EvaluationOutcome::Passed {
        failures.push(format!(
            "unexpected scorecard outcome {:?}",
            scorecard.outcome
        ));
    }
    for (label, path) in [
        ("bundle", Some(report.bundle.bundle_path.as_str())),
        (
            "mutation packet",
            Some(report.bundle.mutation_packet_path.as_str()),
        ),
        ("changed files", report.bundle.changed_files_path.as_deref()),
        ("diff", report.bundle.diff_path.as_deref()),
        ("scorecard", report.bundle.scorecard_path.as_deref()),
        (
            "resource usage",
            report.bundle.resource_usage_path.as_deref(),
        ),
    ] {
        if !path.is_some_and(|path| Path::new(path).exists()) {
            failures.push(format!("{label} artifact was not written"));
        }
    }
    if report.bundle.agent_run_path.is_some() || report.bundle.model_output_path.is_some() {
        failures.push("dry-run candidate unexpectedly wrote model artifacts".to_string());
    }
    if report.bundle.command_logs.len() != 1 {
        failures.push(format!(
            "expected one verification command log, got {}",
            report.bundle.command_logs.len()
        ));
    }

    if failures.is_empty() {
        Ok(SinomoTestOutcome::passed(format!(
            "self-evolution dry run wrote candidate bundle {} with passed verification",
            report.bundle.bundle_path
        )))
    } else {
        ctx.record("failures", &failures);
        Ok(SinomoTestOutcome::failed(failures.join("; ")))
    }
}

fn self_evolution_test_root(ctx: &SinomoTestContext) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    ctx.data_dir()
        .join("evolution")
        .join("test-runs")
        .join(format!("self-dry-run-{}-{stamp}", std::process::id()))
}

#[derive(Clone, Debug)]
struct TestWorkspace {
    root: PathBuf,
    candidate: PathBuf,
}

impl WorkspaceAdapter for TestWorkspace {
    fn root(&self) -> &Path {
        &self.root
    }

    fn prepare_candidate(&self, job: &EvolutionJob) -> Result<CandidateWorkspace, WorkspaceError> {
        Ok(CandidateWorkspace {
            job_id: job.job_id.clone(),
            path: self.candidate.clone(),
            branch_name: "agent/readiness/self-evolution-dry-run".to_string(),
            source_revision: Some("readiness-test".to_string()),
        })
    }

    fn changed_files(
        &self,
        _workspace: &CandidateWorkspace,
    ) -> Result<Vec<String>, WorkspaceError> {
        Ok(Vec::new())
    }

    fn diff(&self, _workspace: &CandidateWorkspace) -> Result<String, WorkspaceError> {
        Ok(String::new())
    }

    fn status(&self, _workspace: &CandidateWorkspace) -> Result<String, WorkspaceError> {
        Ok(String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::test::{SinomoTestRegistryBuilder, SinomoTestStatus};

    #[test]
    fn evolution_self_dry_run_test_case_is_registered_and_passes() {
        let registry = register_evolution_test_cases(SinomoTestRegistryBuilder::new())
            .try_build()
            .expect("evolution test registry");
        let case = registry
            .get("evolution.self-dry-run")
            .expect("self-evolution dry-run case");
        assert!(
            case.metadata()
                .tags
                .iter()
                .any(|tag| tag == "agent-readiness")
        );

        let root = sinomo_testdir::TestDir::new("sinomo-evolution-self-dry-run-test-case");
        let mut context = SinomoTestContext::new(root.path());
        let report = registry
            .run("evolution.self-dry-run", &mut context)
            .expect("run self-evolution dry-run case");

        assert_eq!(report.status, SinomoTestStatus::Passed, "{report:#?}");
        assert!(report.diagnostics.get("bundle_path").is_some());
        assert!(report.diagnostics.get("scorecard_path").is_some());
    }
}
