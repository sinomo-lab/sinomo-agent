use super::*;
use sinomo_runtime::test::{
    SinomoTestCaseMetadata, SinomoTestContext, SinomoTestRegistry, SinomoTestReport,
    SinomoTestStatus,
};

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
    pub id: String,
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
    Ok(TestRunOptions {
        id: id.ok_or_else(|| CliError::Parse {
            detail: "test run requires <test-id>".to_string(),
        })?,
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

fn test_registry() -> Result<SinomoTestRegistry, CliError> {
    let builder = sinomo_lm::register_lm_test_cases(SinomoTestRegistry::builder());
    let builder = crate::commands::test_cases_dfs::register_dfs_cluster_test_cases(builder);
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
    let report = registry
        .run(&options.id, &mut context)
        .map_err(|source| CliError::Parse {
            detail: source.to_string(),
        })?;
    print_test_report(&report, options.json)?;
    test_report_result(&report)
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
        id: test_id.to_string(),
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
        context = context.with_cluster_snapshot(snapshot, paths.kv_snapshot);
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
        SinomoTestStatus::Passed => Ok(()),
        SinomoTestStatus::Skipped => Err(CliError::Parse {
            detail: format!("test skipped: {}", report.summary),
        }),
        SinomoTestStatus::Failed => Err(CliError::Parse {
            detail: format!("test failed: {}", report.summary),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
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
        assert_eq!(options.id, "cluster.dfs.write");
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
    fn run_options_reject_malformed_params() {
        assert!(parse_test_run_options(&args(&["x", "--param"])).is_err());
        assert!(parse_test_run_options(&args(&["x", "--param", "no-equals"])).is_err());
        assert!(parse_test_run_options(&args(&["x", "--param", "=value"])).is_err());
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
            "cluster.dfs.read-your-write",
            "cluster.dfs.invariants",
            "cluster.dfs.write",
            "cluster.dfs.expect-file",
            "cluster.dfs.expect-absent",
            "cluster.dfs.rename",
            "cluster.dfs.remove",
        ] {
            assert!(ids.iter().any(|candidate| candidate == id), "missing {id}");
        }
    }
}
