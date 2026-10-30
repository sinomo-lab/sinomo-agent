//! Deterministic acceptance driver for the real evolution/approval engine.
//! Replaces only the model; workspace, evaluation, bundle integrity and approval
//! use production paths. Run only against an isolated acceptance DATA directory.
use sinomo_evolution::*;
use sinomo_runtime::{
    core::{EvaluationGate, TrustRing},
    eval::{ExecutingEvalHarness, LocalCommandRunner},
    local::SinomoDataDir,
};
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

fn git(repo: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
struct FixtureModel {
    upstream: Option<PathBuf>,
    fail_startup: bool,
}
impl MutationModel for FixtureModel {
    fn mutate(
        &self,
        _: &MutationPacket,
        workspace: &CandidateWorkspace,
        _: Option<&CandidateBundleManifest>,
    ) -> Result<ModelMutationResult, MutationModelError> {
        let mutate = || -> Result<(), String> {
            if let Some(upstream) = &self.upstream {
                git(
                    &workspace.path,
                    &["fetch", upstream.to_str().ok_or("invalid path")?, "HEAD"],
                )?;
                // The fixture deliberately creates an add/add conflict. The model
                // owns its resolution; individual deployment nodes never merge.
                if git(
                    &workspace.path,
                    &["merge", "--no-commit", "--no-ff", "FETCH_HEAD"],
                )
                .is_ok()
                {
                    return Err("fixture expected a source conflict".into());
                }
                fs::write(
                    workspace.path.join("installer-acceptance.txt"),
                    "local and upstream changes resolved by evolution\n",
                )
                .map_err(|e| e.to_string())?;
                git(&workspace.path, &["add", "installer-acceptance.txt"])?;
            }
            let path = workspace.path.join("crates/sinomo/src/main.rs");
            let mut code = fs::read_to_string(&path).map_err(|e| e.to_string())?;
            if self.fail_startup {
                code = code.replacen("fn main() {", "fn main() { std::process::exit(37);", 1);
            } else {
                code.push_str("\n// Deterministic evolution acceptance candidate.\n");
            }
            fs::write(path, code).map_err(|e| e.to_string())?;
            Ok(())
        };
        mutate().map_err(|detail| MutationModelError::Agent { detail })?;
        serde_json::from_value(serde_json::json!({"provider":"acceptance", "model":"scripted", "raw_output":"resolved and checked candidate"}))
            .map_err(|e| MutationModelError::Serialization { detail: e.to_string() })
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let data = SinomoDataDir::new(sinomo_runtime::local::command_path(&fs::canonicalize(
        args.next().ok_or("DATA required")?,
    )?));
    let failure = args.next().as_deref() == Some("fail-startup");
    let builder = args.next().map(PathBuf::from);
    let repo = sinomo_runtime::rollout::SelfRolloutPaths::new(&data.root()).source_repo;
    for (key, value) in [
        ("user.name", "Acceptance evolution agent"),
        ("commit.gpgsign", "false"),
        ("core.longpaths", "true"),
        ("user.email", "acceptance@invalid"),
    ] {
        git(&repo, &["config", key, value])?;
    }
    let upstream = if failure {
        None
    } else {
        let upstream = data.root().join("acceptance-upstream");
        let status = Command::new("git")
            .args([
                "clone",
                "--config",
                "core.longpaths=true",
                "--config",
                "core.autocrlf=false",
                "--no-hardlinks",
            ])
            .arg(&repo)
            .arg(&upstream)
            .status()?;
        if !status.success() {
            return Err("upstream fixture clone failed".into());
        }
        for (key, value) in [
            ("user.name", "Acceptance upstream"),
            ("commit.gpgsign", "false"),
            ("user.email", "upstream@invalid"),
        ] {
            git(&upstream, &["config", key, value])?;
        }
        for (root, text) in [(&repo, "local change\n"), (&upstream, "upstream change\n")] {
            fs::write(root.join("installer-acceptance.txt"), text)?;
            git(root, &["add", "installer-acceptance.txt"])?;
            git(root, &["commit", "-m", "Divergent acceptance input"])?;
        }
        Some(upstream)
    };
    let paths = EvolutionPaths::new(&data);
    paths.ensure()?;
    let orchestrator = EvolutionOrchestrator::new(
        GitWorkspaceAdapter::new(&repo, &paths.jobs),
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(&paths.jobs),
    );
    let report = orchestrator.propose(
        EvolutionRequest {
            objective: "resolve approved upstream source for installer rollout acceptance".into(),
            target_ring: TrustRing::R1TrustedRuntime,
            intended_paths: vec![
                "crates/sinomo/src/main.rs".into(),
                "installer-acceptance.txt".into(),
            ],
            verification_commands: vec![VerificationCommand {
                name: "check resolved patch".into(),
                command: "git diff --check HEAD".into(),
                expected_exit_code: 0,
                gate: EvaluationGate::StaticChecks,
            }],
            benchmark_gates: vec![],
            module_replay_reports: vec![],
            model_provider: None,
            model: None,
            dry_run: false,
        },
        Some(&FixtureModel {
            upstream,
            fail_startup: failure,
        }),
    )?;
    let record = sinomo_evolution::persistence::StoredEvolutionJob {
        job: report.job,
        candidate_workspace: report.candidate_workspace,
        bundle: report.bundle,
        verification_outcome: report.scorecard.map(|s| s.outcome),
        changed_files: report.changed_files,
        decision: None,
    };
    let approved = decide_stored_evolution_job(
        &paths,
        record,
        CandidateDecisionOutcome::Approved,
        "acceptance reviewer",
        "Approve isolated fixture candidate",
    )?;
    if let Some(builder) = builder {
        // Exercise exact approved-source replacement through the production
        // import owner. Only the evolution workspace performs a merge.
        sinomo_runtime::build::deploy_managed_source_repo(
            &builder,
            &repo,
            sinomo_runtime::build::ManagedSourceDeployOptions::default(),
        )?;
    }
    println!(
        "{}",
        serde_json::json!({"revision":git(&repo, &["rev-parse", "HEAD"])? , "job_id":approved.record.job.job_id, "failure_fixture":failure})
    );
    Ok(())
}
