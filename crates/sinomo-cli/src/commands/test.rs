use super::*;
use serde::Serialize;
#[allow(unused_imports)]
use sinomo_runtime::cluster::ClusterKvPathsExt;
use sinomo_runtime::test::{
    SinomoTestCaseMetadata, SinomoTestContext, SinomoTestError, SinomoTestRegistry,
    SinomoTestReport, SinomoTestStatus,
};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum TestCliCommand {
    Help,
    List(TestListOptions),
    Run(TestRunOptions),
    Smoke(TestSmokeOptions),
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub(crate) struct TestListOptions {
    pub json: bool,
    pub tag: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct TestRunOptions {
    pub id: Option<String>,
    pub tag: Option<String>,
    pub provider: Option<String>,
    pub account: String,
    pub model: Option<String>,
    pub live: bool,
    pub require_cluster: bool,
    pub json: bool,
    pub params: Vec<(String, String)>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct TestSmokeOptions {
    pub provider: String,
    pub account: String,
    pub model: Option<String>,
    pub live: bool,
    pub require_cluster: bool,
    pub json: bool,
}

#[derive(Debug, Serialize)]
struct SinomoTestSuiteReport {
    selector: String,
    status: SinomoTestStatus,
    total: usize,
    passed: usize,
    failed: usize,
    skipped: usize,
    duration_ms: u64,
    reports: Vec<SinomoTestReport>,
}

/// Returns the remote main daemons that the live cluster-health view reports
/// online. Availability is resolved before a case applies `expected-targets`:
/// an intentionally offline or currently unreachable member is not a probe
/// target, while every online member remains in scope so route failures on an
/// online node are still observable by the case itself.
pub(super) fn live_online_remote_main_test_targets(
    data_dir: &SinomoDataDir,
    state: &ClusterBootstrapState,
) -> Result<Vec<NodeId>, SinomoTestError> {
    let (health_state, rows) =
        super::cluster_health_rows_for_data_dir(data_dir).map_err(|error| {
            SinomoTestError::failed(format!(
                "cannot resolve live cluster health before selecting test targets: {error}"
            ))
        })?;
    if health_state.cluster_id != state.cluster_id
        || health_state.local_node_id != state.local_node_id
    {
        return Err(SinomoTestError::failed(format!(
            "live cluster health identity changed while selecting test targets: expected cluster {} local {}, got cluster {} local {}",
            state.cluster_id,
            state.local_node_id,
            health_state.cluster_id,
            health_state.local_node_id,
        )));
    }

    let online = rows
        .into_iter()
        .filter(|row| {
            row.availability == ClusterNodeAvailability::Online && row.live_probe_observed
        })
        .map(|row| row.member.node_id)
        .collect::<BTreeSet<_>>();
    let mut candidates = state
        .members
        .iter()
        .filter(|member| {
            member.node_id != state.local_node_id
                && member.state.is_authorized()
                && cluster_member_expected_daemon_process(member) == Some(DAEMON_MAIN_CLUSTER_ROLE)
        })
        .map(|member| member.node_id.clone())
        .collect::<Vec<_>>();
    candidates.sort();
    Ok(retain_live_online_test_targets(candidates, &online))
}

fn retain_live_online_test_targets(
    candidates: Vec<NodeId>,
    online: &BTreeSet<NodeId>,
) -> Vec<NodeId> {
    candidates
        .into_iter()
        .filter(|target| online.contains(target))
        .collect()
}

pub(super) const fn cluster_test_has_expected_target_count(actual: usize, expected: usize) -> bool {
    actual >= expected
}

pub(crate) fn parse_test_command(args: &[String]) -> Result<TestCliCommand, CliError> {
    match args.split_first() {
        None => Ok(TestCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(TestCliCommand::Help)
        }
        Some((command, rest)) if command == "list" => {
            Ok(TestCliCommand::List(parse_test_list_options(rest)?))
        }
        Some((command, rest)) if command == "run" => {
            Ok(TestCliCommand::Run(parse_test_run_options(rest)?))
        }
        Some((command, rest)) if command == "smoke" => {
            Ok(TestCliCommand::Smoke(parse_test_smoke_options(rest)?))
        }
        _ => Err(CliError::Usage),
    }
}

fn parse_test_list_options(args: &[String]) -> Result<TestListOptions, CliError> {
    let mut options = TestListOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--json" => options.json = true,
            "--tag" => {
                index += 1;
                options.tag = Some(args.get(index).cloned().ok_or_else(|| CliError::Parse {
                    detail: "--tag requires a value".to_string(),
                })?);
            }
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected test list argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    Ok(options)
}

fn parse_test_run_options(args: &[String]) -> Result<TestRunOptions, CliError> {
    let mut id = None;
    let mut tag = None;
    let mut provider = None;
    let mut account = DEFAULT_PROVIDER_ACCOUNT.to_string();
    let mut model = None;
    let mut live = false;
    let mut require_cluster = false;
    let mut json = false;
    let mut params = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--provider" => {
                index += 1;
                provider = Some(args.get(index).cloned().ok_or_else(|| CliError::Parse {
                    detail: "--provider requires a value".to_string(),
                })?);
            }
            "--account" => {
                index += 1;
                account = normalize_provider_account_name(Some(
                    args.get(index)
                        .ok_or_else(|| CliError::Parse {
                            detail: "--account requires a value".to_string(),
                        })?
                        .as_str(),
                ));
            }
            "--model" => {
                index += 1;
                model = Some(args.get(index).cloned().ok_or_else(|| CliError::Parse {
                    detail: "--model requires a value".to_string(),
                })?);
            }
            "--live" => live = true,
            "--require-cluster" => require_cluster = true,
            "--json" => json = true,
            "--tag" => {
                index += 1;
                tag = Some(args.get(index).cloned().ok_or_else(|| CliError::Parse {
                    detail: "--tag requires a value".to_string(),
                })?);
            }
            "--param" => {
                index += 1;
                let raw = args.get(index).ok_or_else(|| CliError::Parse {
                    detail: "--param requires a key=value argument".to_string(),
                })?;
                let (key, value) = raw.split_once('=').ok_or_else(|| CliError::Parse {
                    detail: format!("--param expects key=value, got `{raw}`"),
                })?;
                if key.is_empty() {
                    return Err(CliError::Parse {
                        detail: format!("--param expects a non-empty key, got `{raw}`"),
                    });
                }
                params.push((key.to_string(), value.to_string()));
            }
            value if id.is_none() => id = Some(value.to_string()),
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected test run argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    if id.is_some() && tag.is_some() {
        return Err(CliError::Parse {
            detail: "test run accepts either <test-id> or --tag <tag>, not both".to_string(),
        });
    }
    if id.is_none() && tag.is_none() {
        return Err(CliError::Parse {
            detail: "test run requires <test-id> or --tag <tag>".to_string(),
        });
    }
    Ok(TestRunOptions {
        id,
        tag,
        provider,
        account,
        model,
        live,
        require_cluster,
        json,
        params,
    })
}

fn parse_test_smoke_options(args: &[String]) -> Result<TestSmokeOptions, CliError> {
    let mut provider = DeepSeekProvider::PROVIDER_ID.to_string();
    let mut account = DEFAULT_PROVIDER_ACCOUNT.to_string();
    let mut model = None;
    let mut live = false;
    let mut require_cluster = false;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--provider" => {
                index += 1;
                provider = args.get(index).cloned().ok_or_else(|| CliError::Parse {
                    detail: "--provider requires a value".to_string(),
                })?;
            }
            "--account" => {
                index += 1;
                account = normalize_provider_account_name(Some(
                    args.get(index)
                        .ok_or_else(|| CliError::Parse {
                            detail: "--account requires a value".to_string(),
                        })?
                        .as_str(),
                ));
            }
            "--model" => {
                index += 1;
                model = Some(args.get(index).cloned().ok_or_else(|| CliError::Parse {
                    detail: "--model requires a value".to_string(),
                })?);
            }
            "--live" => live = true,
            "--require-cluster" => require_cluster = true,
            "--json" => json = true,
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected test smoke argument `{value}`"),
                });
            }
        }
        index += 1;
    }
    Ok(TestSmokeOptions {
        provider,
        account,
        model,
        live,
        require_cluster,
        json,
    })
}

pub(crate) fn test_command_requires_cluster_state(command: &TestCliCommand) -> bool {
    match command {
        TestCliCommand::Run(options) => options.require_cluster,
        TestCliCommand::Smoke(options) => options.require_cluster,
        TestCliCommand::Help | TestCliCommand::List(_) => false,
    }
}

pub(crate) fn run_test_command(command: TestCliCommand) -> Result<(), CliError> {
    match command {
        TestCliCommand::Help => {
            print_test_help();
            Ok(())
        }
        TestCliCommand::List(options) => test_list(options),
        TestCliCommand::Run(options) => test_run(options),
        TestCliCommand::Smoke(options) => test_smoke(options),
    }
}

pub(crate) fn test_registry() -> Result<SinomoTestRegistry, CliError> {
    let builder = sinomo_lm::register_lm_test_cases(SinomoTestRegistry::builder());
    let builder = crate::commands::test_cases_network::register_network_cluster_test_cases(builder);
    let builder = crate::commands::test_cases_agent::register_agent_cluster_test_cases(builder);
    let builder =
        crate::commands::test_cases_cluster::register_cluster_migration_test_cases(builder);
    let builder = crate::commands::test_cases_cluster_kv::register_cluster_kv_test_cases(builder);
    let builder = crate::commands::test_cases_dfs::register_dfs_cluster_test_cases(builder);
    let builder = cli_runtime_config()
        .test_registry_hooks
        .iter()
        .fold(builder, |builder, hook| hook(builder));
    builder.try_build().map_err(|source| CliError::Parse {
        detail: source.to_string(),
    })
}

fn test_list(options: TestListOptions) -> Result<(), CliError> {
    let registry = test_registry()?;
    let mut cases = registry.metadata();
    if let Some(tag) = options.tag.as_deref() {
        cases.retain(|case| case.tags.iter().any(|candidate| candidate == tag));
    }
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&cases).map_err(|source| {
                CliError::Parse {
                    detail: source.to_string(),
                }
            })?
        );
        return Ok(());
    }
    if cases.is_empty() {
        println!("No Sinomo tests matched.");
        return Ok(());
    }
    for case in cases {
        print_test_case_metadata(&case);
    }
    Ok(())
}

fn test_run(options: TestRunOptions) -> Result<(), CliError> {
    let registry = test_registry()?;
    let mut context = test_context(
        options.provider.as_deref(),
        &options.account,
        options.model.as_deref(),
        options.live,
        options.require_cluster,
        &options.params,
    )?;
    if let Some(tag) = options.tag.as_deref() {
        let report = test_run_tag(&registry, &mut context, tag)?;
        print_test_suite_report(&report, options.json)?;
        return test_suite_report_result(&report);
    }
    let id = options.id.as_deref().ok_or_else(|| CliError::Parse {
        detail: "test run requires <test-id> or --tag <tag>".to_string(),
    })?;
    let report = registry
        .run(id, &mut context)
        .map_err(|source| CliError::Parse {
            detail: source.to_string(),
        })?;
    print_test_report(&report, options.json)?;
    test_report_result(&report)
}

fn test_run_tag(
    registry: &SinomoTestRegistry,
    context: &mut SinomoTestContext,
    tag: &str,
) -> Result<SinomoTestSuiteReport, CliError> {
    let started = std::time::Instant::now();
    let mut ids = registry
        .metadata()
        .into_iter()
        .filter(|case| case.tags.iter().any(|candidate| candidate == tag))
        .map(|case| case.id)
        .collect::<Vec<_>>();
    ids.sort();
    if ids.is_empty() {
        return Err(CliError::Parse {
            detail: format!("no tests matched tag `{tag}`"),
        });
    }
    let mut reports = Vec::new();
    for id in ids {
        reports.push(
            registry
                .run(&id, context)
                .map_err(|source| CliError::Parse {
                    detail: source.to_string(),
                })?,
        );
    }
    let passed = reports
        .iter()
        .filter(|report| report.status == SinomoTestStatus::Passed)
        .count();
    let failed = reports
        .iter()
        .filter(|report| report.status == SinomoTestStatus::Failed)
        .count();
    let skipped = reports
        .iter()
        .filter(|report| report.status == SinomoTestStatus::Skipped)
        .count();
    let status = if failed > 0 {
        SinomoTestStatus::Failed
    } else if skipped > 0 {
        SinomoTestStatus::Skipped
    } else {
        SinomoTestStatus::Passed
    };
    Ok(SinomoTestSuiteReport {
        selector: format!("tag:{tag}"),
        status,
        total: reports.len(),
        passed,
        failed,
        skipped,
        duration_ms: started.elapsed().as_millis() as u64,
        reports,
    })
}

fn test_smoke(options: TestSmokeOptions) -> Result<(), CliError> {
    let provider = normalize_model_provider(&options.provider).ok_or_else(|| CliError::Parse {
        detail: format!("unknown provider `{}`", options.provider),
    })?;
    let test_id = match provider.as_str() {
        CODEX_PROVIDER_ID => "lm.codex.native-tools",
        DeepSeekProvider::PROVIDER_ID => "lm.deepseek.native-tools",
        OpenAiProvider::PROVIDER_ID => "lm.openai.native-tools",
        other => {
            return Err(CliError::Parse {
                detail: format!("no native-tool smoke test is registered for provider `{other}`"),
            });
        }
    };
    test_run(TestRunOptions {
        id: Some(test_id.to_string()),
        tag: None,
        provider: Some(provider),
        account: options.account,
        model: options.model,
        live: options.live,
        require_cluster: options.require_cluster,
        json: options.json,
        params: Vec::new(),
    })
}

fn test_context(
    provider: Option<&str>,
    account: &str,
    model: Option<&str>,
    live: bool,
    require_cluster: bool,
    params: &[(String, String)],
) -> Result<SinomoTestContext, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let mut context = SinomoTestContext::new(data_dir.root())
        .with_live(live)
        .with_require_cluster(require_cluster)
        .with_parameter("account", account.to_string());
    if let Some(provider) = provider {
        context = context.with_parameter("provider", provider.to_string());
    }
    if let Some(model) = model {
        context = context.with_parameter("model", model.to_string());
    }
    for (key, value) in params {
        context = context.with_parameter(key.clone(), value.clone());
    }
    if model_config_uses_cluster(&data_dir) {
        let paths = ClusterPaths::new(&data_dir);
        let snapshot = model_config_cluster_snapshot(&paths)?;
        context = context.with_cluster_snapshot(snapshot, paths.kv_paths().snapshot);
    } else if require_cluster {
        return Err(CliError::Parse {
            detail: "test requires cluster runtime, but this CLI invocation is in local mode"
                .to_string(),
        });
    }
    Ok(context)
}

fn test_report_result(report: &SinomoTestReport) -> Result<(), CliError> {
    match report.status {
        SinomoTestStatus::Passed | SinomoTestStatus::Skipped => Ok(()),
        SinomoTestStatus::Failed => Err(CliError::Parse {
            detail: format!("test failed: {}", report.summary),
        }),
    }
}

fn test_suite_report_result(report: &SinomoTestSuiteReport) -> Result<(), CliError> {
    match report.status {
        SinomoTestStatus::Passed | SinomoTestStatus::Skipped => Ok(()),
        SinomoTestStatus::Failed => Err(CliError::Parse {
            detail: format!(
                "test suite failed: {} selected, {} failed, {} skipped",
                report.total, report.failed, report.skipped
            ),
        }),
    }
}

fn print_test_case_metadata(case: &SinomoTestCaseMetadata) {
    println!("{}", case.id);
    println!("  {}", case.summary);
    if !case.tags.is_empty() {
        println!("  tags: {}", case.tags.join(", "));
    }
    if !case.requirements.is_empty() {
        let requirements = case
            .requirements
            .iter()
            .map(|requirement| requirement.label())
            .collect::<Vec<_>>()
            .join(", ");
        println!("  requires: {requirements}");
    }
}

fn print_test_report(report: &SinomoTestReport, json: bool) -> Result<(), CliError> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(report).map_err(|source| CliError::Parse {
                detail: source.to_string(),
            })?
        );
        return Ok(());
    }
    println!(
        "{} {} ({} ms): {}",
        match report.status {
            SinomoTestStatus::Passed => "PASS",
            SinomoTestStatus::Failed => "FAIL",
            SinomoTestStatus::Skipped => "SKIP",
        },
        report.case.id,
        report.duration_ms,
        report.summary
    );
    Ok(())
}

fn print_test_suite_report(report: &SinomoTestSuiteReport, json: bool) -> Result<(), CliError> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(report).map_err(|source| CliError::Parse {
                detail: source.to_string(),
            })?
        );
        return Ok(());
    }
    println!(
        "{} {} ({} ms): {} selected, {} passed, {} failed, {} skipped",
        match report.status {
            SinomoTestStatus::Passed => "PASS",
            SinomoTestStatus::Failed => "FAIL",
            SinomoTestStatus::Skipped => "SKIP",
        },
        report.selector,
        report.duration_ms,
        report.total,
        report.passed,
        report.failed,
        report.skipped
    );
    for case_report in &report.reports {
        println!(
            "  {} {} ({} ms): {}",
            match case_report.status {
                SinomoTestStatus::Passed => "PASS",
                SinomoTestStatus::Failed => "FAIL",
                SinomoTestStatus::Skipped => "SKIP",
            },
            case_report.case.id,
            case_report.duration_ms,
            case_report.summary
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn test_report(status: SinomoTestStatus) -> SinomoTestReport {
        SinomoTestReport {
            case: SinomoTestCaseMetadata {
                id: "test.example".to_string(),
                summary: "example test".to_string(),
                tags: Vec::new(),
                requirements: Vec::new(),
            },
            status,
            summary: "example result".to_string(),
            duration_ms: 0,
            diagnostics: serde_json::Value::Object(Default::default()),
            errors: Vec::new(),
        }
    }

    #[test]
    fn live_target_filter_excludes_offline_first_before_expected_count() {
        let offline = NodeId::from_legacy_named("node_acceptance_offline_first").expect("node");
        let carbon = NodeId::from_legacy_named("node_acceptance_carbon").expect("node");
        let oracle = NodeId::from_legacy_named("node_acceptance_oracle").expect("node");
        let candidates = vec![offline.clone(), carbon.clone(), oracle.clone()];
        let online = BTreeSet::from([carbon.clone(), oracle.clone()]);

        let selected = retain_live_online_test_targets(candidates, &online);

        assert_eq!(selected, vec![carbon, oracle]);
        assert!(!selected.contains(&offline));
        assert!(cluster_test_has_expected_target_count(selected.len(), 2));
        assert!(
            !cluster_test_has_expected_target_count(selected.len(), 3),
            "fewer reachable targets than requested must remain a strict failure"
        );
    }

    #[test]
    fn live_target_filter_keeps_every_online_target_for_route_validation() {
        let online_with_broken_route =
            NodeId::from_legacy_named("node_acceptance_online_broken_route").expect("node");
        let selected = retain_live_online_test_targets(
            vec![online_with_broken_route.clone()],
            &BTreeSet::from([online_with_broken_route.clone()]),
        );

        assert_eq!(selected, vec![online_with_broken_route]);
    }

    #[test]
    fn skipped_test_outcomes_do_not_fail_cli_result() {
        assert!(test_report_result(&test_report(SinomoTestStatus::Skipped)).is_ok());

        let suite = SinomoTestSuiteReport {
            selector: "tag:example".to_string(),
            status: SinomoTestStatus::Skipped,
            total: 1,
            passed: 0,
            failed: 0,
            skipped: 1,
            duration_ms: 0,
            reports: vec![test_report(SinomoTestStatus::Skipped)],
        };
        assert!(test_suite_report_result(&suite).is_ok());
    }

    #[test]
    fn run_options_parse_repeatable_params() {
        let options = parse_test_run_options(&args(&[
            "cluster.dfs.write",
            "--require-cluster",
            "--param",
            "path=/services/x.txt",
            "--param",
            "payload=hello=world",
        ]))
        .expect("options");
        assert_eq!(options.id.as_deref(), Some("cluster.dfs.write"));
        assert_eq!(options.tag, None);
        assert!(options.require_cluster);
        assert_eq!(
            options.params,
            vec![
                ("path".to_string(), "/services/x.txt".to_string()),
                ("payload".to_string(), "hello=world".to_string()),
            ]
        );
    }

    #[test]
    fn run_options_accept_tag_selector() {
        let options = parse_test_run_options(&args(&[
            "--tag",
            "agent-readiness",
            "--require-cluster",
            "--json",
        ]))
        .expect("options");
        assert_eq!(options.id, None);
        assert_eq!(options.tag.as_deref(), Some("agent-readiness"));
        assert!(options.require_cluster);
        assert!(options.json);
    }

    #[test]
    fn run_options_reject_malformed_params() {
        assert!(parse_test_run_options(&args(&["x", "--param"])).is_err());
        assert!(parse_test_run_options(&args(&["x", "--param", "no-equals"])).is_err());
        assert!(parse_test_run_options(&args(&["x", "--param", "=value"])).is_err());
        assert!(parse_test_run_options(&args(&["--tag"])).is_err());
        assert!(parse_test_run_options(&args(&["x", "--tag", "agent-readiness"])).is_err());
        assert!(parse_test_run_options(&args(&[])).is_err());
    }

    #[test]
    fn registry_includes_dfs_cluster_cases() {
        let registry = test_registry().expect("registry");
        let ids: Vec<String> = registry
            .metadata()
            .into_iter()
            .map(|case| case.id)
            .collect();
        for id in [
            "cluster.network.ping-benchmark",
            "cluster.network.semantic-attachment-benchmark",
            "cluster.network.arbitrary-message",
            "cluster.network.hygiene",
            "cluster.network.hygiene-dirty-fixture",
            "cluster.kv.quorum-commit-benchmark",
            "cluster.migration.seed-source",
            "cluster.migration.verify-joined",
            "cluster.dfs.lightweight-latency",
            "cluster.dfs.protocol-codec-baseline",
            "cluster.dfs.read-your-write",
            "cluster.dfs.invariants",
            "cluster.dfs.write",
            "cluster.dfs.expect-file",
            "cluster.dfs.expect-absent",
            "cluster.dfs.rename",
            "cluster.dfs.remove",
            "cluster.dfs.small-file-baseline",
            "cluster.dfs.bulk-write",
            "cluster.dfs.bulk-expect",
            "cluster.dfs.disk-baseline",
            "cluster.dfs.large-write",
            "cluster.dfs.large-expect",
        ] {
            assert!(ids.iter().any(|candidate| candidate == id), "missing {id}");
        }
    }

    #[test]
    fn registry_includes_network_dfs_acceptance_suite() {
        let registry = test_registry().expect("registry");
        let acceptance = registry
            .metadata()
            .into_iter()
            .filter(|case| {
                case.tags
                    .iter()
                    .any(|tag| tag == "cluster-network-dfs-acceptance")
            })
            .map(|case| case.id)
            .collect::<Vec<_>>();
        for id in [
            "cluster.network.ping-benchmark",
            "cluster.network.arbitrary-message",
            "cluster.network.hygiene",
            "cluster.dfs.routed-status",
            "cluster.dfs.read-your-write",
            "cluster.dfs.concurrent-file-race",
            "cluster.dfs.concurrent-dir-race",
            "cluster.dfs.failover-current-read",
            "cluster.dfs.invariants",
            "cluster.dfs.lightweight-latency",
        ] {
            assert!(
                acceptance.iter().any(|candidate| candidate == id),
                "missing {id} from cluster-network-dfs-acceptance"
            );
        }
    }

    #[test]
    fn registry_includes_agent_cluster_cases() {
        let registry = test_registry().expect("registry");
        let ids: Vec<String> = registry
            .metadata()
            .into_iter()
            .map(|case| case.id)
            .collect();
        assert!(
            ids.iter()
                .any(|candidate| candidate == "cluster.agent.runtime-routes"),
            "missing cluster.agent.runtime-routes"
        );
        assert!(
            ids.iter()
                .any(|candidate| candidate == "cluster.agent.runtime-persisted-session-routes"),
            "missing cluster.agent.runtime-persisted-session-routes"
        );
        assert!(
            ids.iter()
                .any(|candidate| candidate == "cluster.agent.sandbox-cancel-route"),
            "missing cluster.agent.sandbox-cancel-route"
        );
        assert!(
            ids.iter()
                .any(|candidate| candidate == "cluster.agent.module-native-dylib-cancel-route"),
            "missing cluster.agent.module-native-dylib-cancel-route"
        );
        assert!(
            ids.iter()
                .any(|candidate| candidate == "cluster.agent.module-native-dylib-active-cancel"),
            "missing cluster.agent.module-native-dylib-active-cancel"
        );
        assert!(
            ids.iter()
                .any(|candidate| candidate == "cluster.agent.sandbox-active-cancel"),
            "missing cluster.agent.sandbox-active-cancel"
        );
    }
}
