//! Runtime test selection grammar and registration.
use sinomo_api::lm::{DEEPSEEK_PROVIDER_ID, normalize_provider_account_name};
use sinomo_cli_registry::*;
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TestCliCommand {
    List(TestListOptions),
    Run(TestRunOptions),
    Smoke(TestSmokeOptions),
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct TestListOptions {
    pub json: bool,
    pub tag: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TestRunOptions {
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
pub struct TestSmokeOptions {
    pub provider: String,
    pub account: String,
    pub model: Option<String>,
    pub live: bool,
    pub require_cluster: bool,
    pub json: bool,
}

pub fn test_cli_command_handler<E: Into<CliCommandError> + 'static>(
    run: fn(TestCliCommand) -> Result<(), E>,
) -> DeclarativeCommandHandler<TestCliCommand, E> {
    DeclarativeCommandHandler::new(
        "test",
        CliCommandPolicy::local_capable(),
        test_cli_spec,
        parse_test_matches,
        run,
    )
    .with_parsed_policy(test_cli_policy)
}

fn test_cli_spec() -> CliCommandSpec {
    let common_model_options = || {
        [
            CliOptionSpec::value(
                "--provider",
                "model provider",
                CliValueSpec::new("provider", "provider id"),
            ),
            CliOptionSpec::value(
                "--account",
                "provider account",
                CliValueSpec::new("account", "provider account"),
            ),
            CliOptionSpec::value(
                "--model",
                "model id",
                CliValueSpec::new("model", "model id"),
            ),
            CliOptionSpec::flag("--live", "enable live external integration"),
            CliOptionSpec::flag("--require-cluster", "require cluster state and quorum"),
            CliOptionSpec::flag("--json", "print JSON output"),
        ]
    };
    let mut run_options = common_model_options().to_vec();
    run_options.extend([
        CliOptionSpec::value(
            "--tag",
            "select tests by tag",
            CliValueSpec::new("tag", "test tag"),
        ),
        CliOptionSpec::value(
            "--param",
            "test parameter key=value",
            CliValueSpec::new("key=value", "test parameter"),
        )
        .repeatable(),
    ]);
    CliCommandSpec::new("test", "list and run registered Sinomo tests")
        .help_on_empty()
        .with_children([
            CliCommandSpec::new("list", "list registered tests").with_options([
                CliOptionSpec::flag("--json", "print JSON output"),
                CliOptionSpec::value(
                    "--tag",
                    "filter by tag",
                    CliValueSpec::new("tag", "test tag"),
                ),
            ]),
            CliCommandSpec::new("run", "run a test or tagged test group")
                .with_options(run_options)
                .with_positionals([CliPositionalSpec::optional(CliValueSpec::new(
                    "test-id", "test id",
                ))]),
            CliCommandSpec::new("smoke", "run the model smoke test")
                .with_options(common_model_options()),
        ])
}

fn parse_test_matches(matches: &CliCommandMatches) -> Result<TestCliCommand, CliParseError> {
    match matches.command_name() {
        "list" => Ok(TestCliCommand::List(TestListOptions {
            json: matches.contains("--json"),
            tag: matches.value("--tag").map(str::to_string),
        })),
        "run" => parse_test_run_matches(matches).map(TestCliCommand::Run),
        "smoke" => Ok(TestCliCommand::Smoke(TestSmokeOptions {
            provider: matches
                .value("--provider")
                .unwrap_or(DEEPSEEK_PROVIDER_ID)
                .to_string(),
            account: normalize_provider_account_name(matches.value("--account")),
            model: matches.value("--model").map(str::to_string),
            live: matches.contains("--live"),
            require_cluster: matches.contains("--require-cluster"),
            json: matches.contains("--json"),
        })),
        _ => Err(CliParseError::Parse {
            detail: "test requires a subcommand".to_string(),
        }),
    }
}

fn parse_test_run_matches(matches: &CliCommandMatches) -> Result<TestRunOptions, CliParseError> {
    let id = matches.positionals().first().cloned();
    let tag = matches.value("--tag").map(str::to_string);
    let params = matches
        .values("--param")
        .map(|raw| {
            let (key, value) = raw.split_once('=').ok_or_else(|| CliParseError::Parse {
                detail: format!("--param expects key=value, got `{raw}`"),
            })?;
            if key.is_empty() {
                return Err(CliParseError::Parse {
                    detail: format!("--param expects a non-empty key, got `{raw}`"),
                });
            }
            Ok((key.to_string(), value.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if id.is_some() && tag.is_some() {
        return Err(CliParseError::Parse {
            detail: "test run accepts either <test-id> or --tag <tag>, not both".to_string(),
        });
    }
    if id.is_none() && tag.is_none() {
        return Err(CliParseError::Parse {
            detail: "test run requires <test-id> or --tag <tag>".to_string(),
        });
    }
    Ok(TestRunOptions {
        id,
        tag,
        provider: matches.value("--provider").map(str::to_string),
        account: normalize_provider_account_name(matches.value("--account")),
        model: matches.value("--model").map(str::to_string),
        live: matches.contains("--live"),
        require_cluster: matches.contains("--require-cluster"),
        json: matches.contains("--json"),
        params,
    })
}

fn is_connectivity_diagnostic(command: &TestCliCommand) -> bool {
    matches!(command, TestCliCommand::Run(options)
        if options.tag.is_none() && options.id.as_deref() == Some("cluster.network.connectivity"))
}

pub fn test_command_requires_cluster_state(command: &TestCliCommand) -> bool {
    if is_connectivity_diagnostic(command) {
        return false;
    }
    match command {
        TestCliCommand::Run(options) => options.require_cluster,
        TestCliCommand::Smoke(options) => options.require_cluster,
        TestCliCommand::List(_) => false,
    }
}

fn test_cli_policy(command: &TestCliCommand) -> CliCommandPolicy {
    // This exact read-only diagnostic must remain usable while quorum is lost.
    // It selects candidates from local observations; the serving daemon still
    // authenticates and authorizes every routed ping. Tags and mutation tests
    // retain their ordinary quorum admission.
    if is_connectivity_diagnostic(command) {
        return CliCommandPolicy {
            requires_cluster_state: false,
            supports_degraded_local_mode: false,
        };
    }
    if test_command_requires_cluster_state(command) {
        CliCommandPolicy::cluster_required()
    } else {
        CliCommandPolicy::local_capable()
    }
}
/// Parse this domain's arguments without executing a command.
pub fn parse_test_command(args: &[String]) -> Result<TestCliCommand, CliParseError> {
    parse_command_args(test_cli_spec(), args, parse_test_matches)
}

/// Register the owner's grammar with a host-supplied execution adapter.
pub fn register_cli_commands<E: Into<CliCommandError> + 'static>(
    registry: CliCommandRegistry,
    run: fn(TestCliCommand) -> Result<(), E>,
) -> Result<CliCommandRegistry, String> {
    registry.try_register(test_cli_command_handler(run))
}

#[cfg(test)]
mod diagnostic_policy_tests {
    use super::*;

    #[test]
    fn connectivity_diagnostic_does_not_require_quorum_or_enter_local_mode() {
        let parse = |args: &[&str]| {
            parse_test_command(&args.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap()
        };
        let diagnostic = parse(&["run", "cluster.network.connectivity", "--require-cluster"]);
        assert!(!test_command_requires_cluster_state(&diagnostic));
        let policy = test_cli_policy(&diagnostic);
        assert!(!policy.requires_cluster_state);
        assert!(!policy.supports_degraded_local_mode);
        for args in [
            vec!["run", "cluster.dfs.write", "--require-cluster"],
            vec!["run", "cluster.network.ping-benchmark", "--require-cluster"],
            vec!["run", "--tag", "cluster-network", "--require-cluster"],
        ] {
            assert!(test_cli_policy(&parse(&args)).requires_cluster_state);
        }
    }
}
