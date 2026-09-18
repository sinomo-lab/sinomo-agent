//! Rollout grammar and recovery intent; process and terminal adapters are supplied by the host.
use sinomo_api::{
    SINOMO_DAEMON_DEFAULT_HOST, SINOMO_DAEMON_DEFAULT_PORT, SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
};
use sinomo_cli_registry::*;
use sinomo_core::{Id, RollbackTrigger, events::EventId, node::NodeId};
use sinomo_rollout::DEFAULT_SHADOW_TELEMETRY_LIMIT;
use std::path::PathBuf;
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RolloutCliCommand {
    InitSource(RolloutOptions),
    ImportSource(RolloutOptions),
    ExportSource(RolloutOptions),
    ApplyExternal(RolloutOptions),
    Apply(RolloutOptions),
    Cluster(RolloutOptions),
    Verify(RecoveryOptions),
    Resume(RecoveryOptions),
    Cancel(RecoveryOptions),
    BuildLog(BuildLogOptions),
    PrepareSource {
        repo: PathBuf,
        output: PathBuf,
        targets: Vec<String>,
    },
    ModuleNativeDylib(RolloutModuleNativeDylibOptions),
    Advance(RolloutAdvanceOptions),
    TelemetryGatesShow,
    TelemetryGatesSet(RolloutTelemetryGateSetOptions),
    Diagnostics(RolloutOptions),
    ClearStatus(RolloutOptions),
    Status,
    Desktop(DesktopOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RolloutOptions {
    pub json: bool,
    pub repo: Option<PathBuf>,
    pub binary: Option<PathBuf>,
    pub remote_repo: Option<PathBuf>,
    pub targets: Vec<String>,
    pub host: String,
    /// True only when the operator supplied `--host`. Rollout uses this to
    /// distinguish requested main/shadow listener changes from the loopback
    /// CLI compatibility default.
    pub host_explicit: bool,
    pub port: u16,
    pub shadow_port: u16,
    pub skip_local: bool,
    pub remote_build: bool,
    pub skip_native_modules: bool,
    pub force_source_overwrite: bool,
}

impl Default for RolloutOptions {
    fn default() -> Self {
        Self {
            json: false,
            repo: None,
            binary: None,
            remote_repo: None,
            targets: Vec::new(),
            host: SINOMO_DAEMON_DEFAULT_HOST.to_string(),
            host_explicit: false,
            port: SINOMO_DAEMON_DEFAULT_PORT,
            shadow_port: SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
            skip_local: false,
            remote_build: false,
            skip_native_modules: false,
            force_source_overwrite: false,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RolloutModuleNativeDylibOptions {
    pub module_selector: String,
    pub rollout: RolloutOptions,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct RolloutAdvanceOptions {
    pub node_id: Option<NodeId>,
    pub scorecard: Option<PathBuf>,
    pub module_replay_report: Option<PathBuf>,
    pub shadow_telemetry_limit: Option<usize>,
    pub incident_event_ids: Vec<EventId>,
    pub user_correction_count: u32,
    pub rollback_triggers: Vec<RollbackTrigger>,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct RolloutTelemetryGateSetOptions {
    pub automatic_promotion_enabled: Option<bool>,
    pub event_limit: Option<usize>,
    pub minimum_event_count: Option<u64>,
    pub maximum_automatic_weight_bps: Option<Option<u16>>,
    pub maximum_candidate_latency_p95_ms: Option<Option<u64>>,
}

pub fn rollout_cli_command_handler<E: Into<CliCommandError> + 'static>(
    run: fn(RolloutCliCommand) -> Result<(), E>,
) -> DeclarativeCommandHandler<RolloutCliCommand, E> {
    DeclarativeCommandHandler::new(
        "rollout",
        CliCommandPolicy::default(),
        rollout_cli_spec,
        parse_rollout_matches,
        run,
    )
}

fn rollout_cli_spec() -> CliCommandSpec {
    let option = |name: &'static str, help: &'static str, value: &'static str| {
        CliOptionSpec::value(name, help, CliValueSpec::new(value, help))
    };
    let source_options = || {
        [
            option("--repo", "source repository", "path"),
            CliOptionSpec::flag("--force-source", "allow managed source overwrite")
                .with_aliases(["--force-overwrite"]),
        ]
    };
    let endpoint_options = || {
        [
            option("--host", "local daemon host", "host"),
            option("--port", "local daemon port", "port"),
            option("--shadow-port", "shadow daemon port", "port"),
        ]
    };
    let mut apply_options = source_options().to_vec();
    apply_options.extend(endpoint_options());
    let mut cluster_options = apply_options.clone();
    cluster_options.extend([
        option("--binary", "prebuilt binary path", "path"),
        option("--target", "explicit rollout target", "host:port").repeatable(),
        CliOptionSpec::flag(
            "--remote-build",
            "compatibility alias for build-service scheduling",
        ),
        option("--remote-repo", "remote source repository", "path"),
        CliOptionSpec::flag("--skip-local", "exclude the local node"),
        CliOptionSpec::flag(
            "--skip-native-modules",
            "push only the main binary during bootstrap",
        ),
    ]);
    let mut module_options = endpoint_options().to_vec();
    module_options.extend([
        option("--repo", "source repository", "path"),
        option("--target", "explicit rollout target", "host:port").repeatable(),
        option("--module", "native module selector", "module"),
        CliOptionSpec::flag("--skip-local", "exclude the local node"),
    ]);
    CliCommandSpec::new("rollout", "manage source, binary, and module rollout")
        .help_on_empty()
        .with_children([
            desktop_cli_spec(),
            CliCommandSpec::new("init-source", "initialize the managed source repository")
                .with_options(source_options()),
            CliCommandSpec::new(
                "prepare-source",
                "prepare committed dependency pins in an isolated deployment checkout",
            )
            .with_options([
                option("--repo", "source Git repository", "path"),
                option("--output", "new deployment checkout directory", "path"),
                option(
                    "--target",
                    "Rust target triple for locked metadata validation",
                    "triple",
                )
                .repeatable(),
            ]),
            CliCommandSpec::new(
                "import-source",
                "align managed source to an external committed snapshot",
            )
            .with_aliases(["sync-from-external"])
            .with_options(source_options()),
            CliCommandSpec::new(
                "export-source",
                "merge managed source into an external repo",
            )
            .with_aliases(["sync-to-external"])
            .with_options([option("--repo", "external repository", "path")]),
            CliCommandSpec::new(
                "apply-external",
                "import external source and apply it locally",
            )
            .with_aliases(["from-external"])
            .with_options(apply_options.clone()),
            CliCommandSpec::new("apply", "build and stage managed source locally")
                .with_options(apply_options),
            CliCommandSpec::new("cluster", "roll out to cluster targets")
                .with_aliases(["apply-cluster"])
                .with_options(cluster_options),
            CliCommandSpec::new(
                "verify",
                "verify runtime identity and stable Raft readiness",
            )
            .with_options([
                option("--id", "saved rollout ID (defaults to latest)", "id"),
                option(
                    "--target",
                    "target to verify without a saved plan",
                    "node|host:port",
                )
                .repeatable(),
                option("--commit", "expected runtime source commit", "commit"),
                option(
                    "--repo",
                    "repository for resolving the expected commit",
                    "path",
                ),
            ]),
            CliCommandSpec::new("resume", "reconcile and resume the original saved rollout")
                .with_options([option(
                    "--id",
                    "saved rollout ID (defaults to latest)",
                    "id",
                )]),
            CliCommandSpec::new(
                "cancel",
                "retire a settled plan after proving node readiness",
            )
            .with_options([option(
                "--id",
                "saved rollout ID (defaults to latest)",
                "id",
            )]),
            CliCommandSpec::new(
                "build-log",
                "read or follow one build job's compiler output",
            )
            .with_options([
                option("--job", "build job ID", "id"),
                option("--target", "build server", "node|host:port"),
                CliOptionSpec::flag("--follow", "wait for new output until the job finishes"),
            ]),
            CliCommandSpec::new("module", "roll out native modules").with_children([
                CliCommandSpec::new("native-dylib", "roll out one native dynamic module")
                    .with_options(module_options)
                    .with_positionals([CliPositionalSpec::optional(CliValueSpec::new(
                        "module",
                        "native module selector",
                    ))]),
            ]),
            CliCommandSpec::new("advance", "advance candidate rollout state").with_options([
                option("--node", "candidate node id", "node-id").with_aliases(["--node-id"]),
                option("--scorecard", "evaluation scorecard", "path"),
                option("--module-replay-report", "module replay report", "path"),
                CliOptionSpec::flag("--shadow-telemetry", "ingest default shadow telemetry"),
                option(
                    "--shadow-telemetry-limit",
                    "shadow telemetry event limit",
                    "count",
                ),
                option("--incident", "incident event id", "event-id")
                    .with_aliases(["--incident-event"])
                    .repeatable(),
                option("--user-corrections", "user correction count", "count")
                    .with_aliases(["--user-correction-count"]),
                option("--rollback-trigger", "rollback trigger", "trigger").repeatable(),
                option("--evidence", "evidence reference", "reference").repeatable(),
            ]),
            CliCommandSpec::new("telemetry-gates", "inspect or update promotion gates")
                .with_aliases(["telemetry-gate"])
                .with_children([
                    CliCommandSpec::new("show", "show telemetry promotion gates"),
                    CliCommandSpec::new("set", "update telemetry promotion gates").with_options([
                        CliOptionSpec::flag(
                            "--enable-auto-promotion",
                            "enable automatic promotion",
                        ),
                        CliOptionSpec::flag(
                            "--disable-auto-promotion",
                            "disable automatic promotion",
                        ),
                        option("--event-limit", "telemetry event limit", "count"),
                        option("--min-events", "minimum event count", "count")
                            .with_aliases(["--minimum-event-count"]),
                        option(
                            "--max-auto-weight-bps",
                            "maximum automatic weight",
                            "bps|unlimited",
                        ),
                        option(
                            "--max-candidate-latency-p95-ms",
                            "maximum candidate p95 latency",
                            "milliseconds|unlimited",
                        ),
                    ]),
                ]),
            CliCommandSpec::new("diagnostics", "fetch rollout diagnostics")
                .with_aliases(["diag"])
                .with_options([
                    option("--target", "rollout target", "node|host:port").repeatable(),
                    CliOptionSpec::flag("--json", "structured per-target diagnostics"),
                ]),
            CliCommandSpec::new("clear-status", "clear stale rollout status")
                .with_aliases(["clear-state"])
                .with_options([option("--target", "rollout target", "host:port").repeatable()]),
            CliCommandSpec::new("status", "show rollout paths and state"),
        ])
}

fn rollout_options_from_matches(
    matches: &CliCommandMatches,
) -> Result<RolloutOptions, CliParseError> {
    let defaults = RolloutOptions::default();
    let targets = matches
        .values("--target")
        .map(|value| non_empty_address(value).map_err(semantic_parse_error))
        .collect::<Result<Vec<_>, _>>()?;
    let (host, host_explicit) = matches
        .value("--host")
        .map_or((defaults.host, false), |host| (host.to_string(), true));
    let port = matches
        .value("--port")
        .map(|value| {
            value.parse::<u16>().map_err(|source| CliParseError::Parse {
                detail: format!("invalid rollout daemon port `{value}`: {source}"),
            })
        })
        .transpose()?
        .unwrap_or(defaults.port);
    let shadow_port = matches
        .value("--shadow-port")
        .map(|value| {
            value.parse::<u16>().map_err(|source| CliParseError::Parse {
                detail: format!("invalid rollout shadow daemon port `{value}`: {source}"),
            })
        })
        .transpose()?
        .unwrap_or(defaults.shadow_port);
    Ok(RolloutOptions {
        json: matches.contains("--json"),
        repo: matches.value("--repo").map(PathBuf::from),
        binary: matches.value("--binary").map(PathBuf::from),
        remote_repo: matches.value("--remote-repo").map(PathBuf::from),
        targets,
        host,
        host_explicit,
        port,
        shadow_port,
        skip_local: matches.contains("--skip-local"),
        remote_build: matches.contains("--remote-build"),
        skip_native_modules: matches.contains("--skip-native-modules"),
        force_source_overwrite: matches.contains("--force-source"),
    })
}

fn parse_rollout_matches(matches: &CliCommandMatches) -> Result<RolloutCliCommand, CliParseError> {
    let path = matches
        .path()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    match path.as_slice() {
        ["rollout", "app", _] => Ok(RolloutCliCommand::Desktop(parse_desktop_matches(matches)?)),
        ["rollout", "prepare-source"] => {
            Ok(RolloutCliCommand::PrepareSource {
                repo: PathBuf::from(matches.value("--repo").ok_or_else(|| {
                    CliParseError::Parse {
                        detail: "prepare-source requires --repo".into(),
                    }
                })?),
                output: PathBuf::from(matches.value("--output").ok_or_else(|| {
                    CliParseError::Parse {
                        detail: "prepare-source requires --output".into(),
                    }
                })?),
                targets: matches.values("--target").map(str::to_owned).collect(),
            })
        }
        ["rollout", "verify"] => Ok(RolloutCliCommand::Verify(RecoveryOptions::parse(matches)?)),
        ["rollout", "resume"] => Ok(RolloutCliCommand::Resume(RecoveryOptions::parse(matches)?)),
        ["rollout", "cancel"] => Ok(RolloutCliCommand::Cancel(RecoveryOptions::parse(matches)?)),
        ["rollout", "build-log"] => Ok(RolloutCliCommand::BuildLog(BuildLogOptions::parse(
            matches,
        )?)),
        ["rollout", "init-source"] => {
            rollout_options_from_matches(matches).map(RolloutCliCommand::InitSource)
        }
        ["rollout", "import-source"] => {
            rollout_options_from_matches(matches).map(RolloutCliCommand::ImportSource)
        }
        ["rollout", "export-source"] => {
            rollout_options_from_matches(matches).map(RolloutCliCommand::ExportSource)
        }
        ["rollout", "apply-external"] => {
            rollout_options_from_matches(matches).map(RolloutCliCommand::ApplyExternal)
        }
        ["rollout", "apply"] => rollout_options_from_matches(matches).map(RolloutCliCommand::Apply),
        ["rollout", "cluster"] => {
            let options = rollout_options_from_matches(matches)?;
            validate_cluster_native_module_skip(&options).map_err(semantic_parse_error)?;
            Ok(RolloutCliCommand::Cluster(options))
        }
        ["rollout", "module", "native-dylib"] => {
            let module_selector = matches
                .value("--module")
                .or_else(|| matches.positionals().first().map(String::as_str))
                .ok_or_else(|| CliParseError::Parse {
                    detail: "rollout module native-dylib requires a module selector".to_string(),
                })?;
            Ok(RolloutCliCommand::ModuleNativeDylib(
                RolloutModuleNativeDylibOptions {
                    module_selector: non_empty_rollout_module_selector(module_selector)
                        .map_err(semantic_parse_error)?,
                    rollout: rollout_options_from_matches(matches)?,
                },
            ))
        }
        ["rollout", "advance"] => {
            parse_rollout_advance_matches(matches).map(RolloutCliCommand::Advance)
        }
        ["rollout", "telemetry-gates"] | ["rollout", "telemetry-gates", "show"] => {
            Ok(RolloutCliCommand::TelemetryGatesShow)
        }
        ["rollout", "telemetry-gates", "set"] => {
            parse_rollout_telemetry_gate_matches(matches).map(RolloutCliCommand::TelemetryGatesSet)
        }
        ["rollout", "diagnostics"] => {
            rollout_options_from_matches(matches).map(RolloutCliCommand::Diagnostics)
        }
        ["rollout", "clear-status"] => {
            rollout_options_from_matches(matches).map(RolloutCliCommand::ClearStatus)
        }
        ["rollout", "status"] => Ok(RolloutCliCommand::Status),
        _ => Err(CliParseError::Parse {
            detail: "rollout requires a subcommand".to_string(),
        }),
    }
}

fn parse_rollout_advance_matches(
    matches: &CliCommandMatches,
) -> Result<RolloutAdvanceOptions, CliParseError> {
    let shadow_telemetry_limit = if let Some(value) = matches.value("--shadow-telemetry-limit") {
        let value = value
            .parse::<usize>()
            .map_err(|source| CliParseError::Parse {
                detail: format!("invalid rollout shadow telemetry limit `{value}`: {source}"),
            })?;
        if value == 0 {
            return Err(CliParseError::Parse {
                detail: "rollout shadow telemetry limit must be non-zero".to_string(),
            });
        }
        Some(value)
    } else if matches.contains("--shadow-telemetry") {
        Some(DEFAULT_SHADOW_TELEMETRY_LIMIT)
    } else {
        None
    };
    Ok(RolloutAdvanceOptions {
        node_id: matches
            .value("--node")
            .map(parse_rollout_node_id)
            .transpose()
            .map_err(semantic_parse_error)?,
        scorecard: matches.value("--scorecard").map(PathBuf::from),
        module_replay_report: matches.value("--module-replay-report").map(PathBuf::from),
        shadow_telemetry_limit,
        incident_event_ids: matches
            .values("--incident")
            .map(|value| parse_rollout_event_id(value).map_err(semantic_parse_error))
            .collect::<Result<Vec<_>, _>>()?,
        user_correction_count: matches
            .value("--user-corrections")
            .map(|value| {
                value.parse::<u32>().map_err(|source| CliParseError::Parse {
                    detail: format!("invalid user correction count `{value}`: {source}"),
                })
            })
            .transpose()?
            .unwrap_or_default(),
        rollback_triggers: matches
            .values("--rollback-trigger")
            .map(|value| parse_rollout_rollback_trigger(value).map_err(semantic_parse_error))
            .collect::<Result<Vec<_>, _>>()?,
        evidence_refs: matches
            .values("--evidence")
            .map(|value| {
                if value.trim().is_empty() {
                    Err(CliParseError::Parse {
                        detail: "rollout evidence reference cannot be empty".to_string(),
                    })
                } else {
                    Ok(value.to_string())
                }
            })
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn parse_rollout_telemetry_gate_matches(
    matches: &CliCommandMatches,
) -> Result<RolloutTelemetryGateSetOptions, CliParseError> {
    if matches.contains("--enable-auto-promotion") && matches.contains("--disable-auto-promotion") {
        return Err(CliParseError::Parse {
            detail: "telemetry gates cannot enable and disable auto-promotion together".to_string(),
        });
    }
    Ok(RolloutTelemetryGateSetOptions {
        automatic_promotion_enabled: if matches.contains("--enable-auto-promotion") {
            Some(true)
        } else if matches.contains("--disable-auto-promotion") {
            Some(false)
        } else {
            None
        },
        event_limit: matches
            .value("--event-limit")
            .map(|value| parse_nonzero_usize("rollout telemetry event limit", value))
            .transpose()
            .map_err(semantic_parse_error)?,
        minimum_event_count: matches
            .value("--min-events")
            .map(|value| {
                let parsed = value
                    .parse::<u64>()
                    .map_err(|source| CliParseError::Parse {
                        detail: format!(
                            "invalid rollout telemetry minimum event count `{value}`: {source}"
                        ),
                    })?;
                if parsed == 0 {
                    Err(CliParseError::Parse {
                        detail: "rollout telemetry minimum event count must be non-zero"
                            .to_string(),
                    })
                } else {
                    Ok(parsed)
                }
            })
            .transpose()?,
        maximum_automatic_weight_bps: matches
            .value("--max-auto-weight-bps")
            .map(parse_optional_bps)
            .transpose()
            .map_err(semantic_parse_error)?,
        maximum_candidate_latency_p95_ms: matches
            .value("--max-candidate-latency-p95-ms")
            .map(|value| parse_optional_u64("candidate latency p95", value))
            .transpose()
            .map_err(semantic_parse_error)?,
    })
}

pub fn validate_cluster_native_module_skip(options: &RolloutOptions) -> Result<(), CliParseError> {
    if !options.skip_native_modules {
        return Ok(());
    }
    if options.remote_build {
        return Err(CliParseError::Parse {
            detail: "--skip-native-modules cannot be combined with --remote-build; it is a binary-push bootstrap option"
                .to_string(),
        });
    }
    if options.remote_repo.is_some() {
        return Err(CliParseError::Parse {
            detail: "--skip-native-modules cannot be combined with --remote-repo; it is a binary-push bootstrap option"
                .to_string(),
        });
    }
    Ok(())
}

pub fn reject_native_module_skip(
    options: &RolloutOptions,
    command: &str,
) -> Result<(), CliParseError> {
    if options.skip_native_modules {
        return Err(CliParseError::Parse {
            detail: format!(
                "--skip-native-modules is only supported by `rollout cluster` binary-push rollouts; `{command}` must keep the binary and native module set together"
            ),
        });
    }
    Ok(())
}

fn non_empty_rollout_module_selector(value: &str) -> Result<String, CliParseError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CliParseError::Parse {
            detail: "module selector must not be empty".to_string(),
        });
    }
    Ok(value.to_string())
}

fn parse_nonzero_usize(label: &str, value: &str) -> Result<usize, CliParseError> {
    let value = value
        .parse::<usize>()
        .map_err(|source| CliParseError::Parse {
            detail: format!("invalid {label} `{value}`: {source}"),
        })?;
    if value == 0 {
        return Err(CliParseError::Parse {
            detail: format!("{label} must be non-zero"),
        });
    }
    Ok(value)
}

fn parse_optional_bps(value: &str) -> Result<Option<u16>, CliParseError> {
    if matches!(value, "none" | "off" | "unlimited") {
        return Ok(None);
    }
    let weight = value
        .parse::<u16>()
        .map_err(|source| CliParseError::Parse {
            detail: format!("invalid rollout telemetry max automatic weight `{value}`: {source}"),
        })?;
    if weight > 10_000 {
        return Err(CliParseError::Parse {
            detail: "rollout telemetry max automatic weight must be <= 10000 bps".to_string(),
        });
    }
    Ok(Some(weight))
}

fn parse_optional_u64(label: &str, value: &str) -> Result<Option<u64>, CliParseError> {
    if matches!(value, "none" | "off" | "unlimited") {
        return Ok(None);
    }
    let parsed = value
        .parse::<u64>()
        .map_err(|source| CliParseError::Parse {
            detail: format!("invalid rollout telemetry {label} `{value}`: {source}"),
        })?;
    if parsed == 0 {
        return Err(CliParseError::Parse {
            detail: format!("rollout telemetry {label} must be non-zero"),
        });
    }
    Ok(Some(parsed))
}

fn parse_rollout_node_id(value: &str) -> Result<NodeId, CliParseError> {
    NodeId::new(value.to_string()).map_err(|source| CliParseError::Parse {
        detail: format!("invalid rollout node id `{value}`: {source}"),
    })
}

fn parse_rollout_event_id(value: &str) -> Result<EventId, CliParseError> {
    EventId::new(value.to_string()).map_err(|source| CliParseError::Parse {
        detail: format!("invalid rollout incident event id `{value}`: {source}"),
    })
}

fn parse_rollout_rollback_trigger(value: &str) -> Result<RollbackTrigger, CliParseError> {
    match value {
        "build-failure" => Ok(RollbackTrigger::BuildFailure),
        "load-failure" => Ok(RollbackTrigger::LoadFailure),
        "panic" => Ok(RollbackTrigger::Panic),
        "timeout" => Ok(RollbackTrigger::Timeout),
        "resource-budget-violation" => Ok(RollbackTrigger::ResourceBudgetViolation),
        "new-denied-capability-request" => Ok(RollbackTrigger::NewDeniedCapabilityRequest),
        "policy-incident" => Ok(RollbackTrigger::PolicyIncident),
        "score-regression" => Ok(RollbackTrigger::ScoreRegression),
        "emergency-stop" => Ok(RollbackTrigger::EmergencyStop),
        "user-correction-spike" => Ok(RollbackTrigger::UserCorrectionSpike),
        "node-health-degradation" => Ok(RollbackTrigger::NodeHealthDegradation),
        _ => Err(CliParseError::Parse {
            detail: format!("invalid rollback trigger `{value}`"),
        }),
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecoveryOptions {
    pub id: Option<String>,
    pub targets: Vec<String>,
    pub commit: Option<String>,
    pub repo: Option<PathBuf>,
}

impl RecoveryOptions {
    pub fn parse(matches: &CliCommandMatches) -> Result<Self, CliParseError> {
        let options = Self {
            id: matches.value("--id").map(str::to_owned),
            targets: matches.values("--target").map(str::to_owned).collect(),
            commit: matches.value("--commit").map(str::to_owned),
            repo: matches.value("--repo").map(PathBuf::from),
        };
        if !options.targets.is_empty() && (options.commit.is_none() || options.id.is_some()) {
            return Err(CliParseError::Parse {
                detail: "verify --target requires --commit and cannot be combined with --id".into(),
            });
        }
        if options.targets.is_empty() && (options.commit.is_some() || options.repo.is_some()) {
            return Err(CliParseError::Parse {
                detail: "--commit and --repo require explicit verification targets".into(),
            });
        }
        Ok(options)
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BuildLogOptions {
    pub job_id: Id,
    pub target: Option<String>,
    pub follow: bool,
}

impl BuildLogOptions {
    pub fn parse(matches: &CliCommandMatches) -> Result<Self, CliParseError> {
        let value = matches.value("--job").ok_or_else(|| CliParseError::Parse {
            detail: "build-log requires --job".into(),
        })?;
        Ok(Self {
            job_id: Id::new(value).map_err(|error| CliParseError::Parse {
                detail: error.to_string(),
            })?,
            target: matches.value("--target").map(str::to_owned),
            follow: matches.contains("--follow"),
        })
    }
}
fn non_empty_address(address: &str) -> Result<String, CliParseError> {
    let address = address.trim();
    if address.is_empty() {
        return Err(CliParseError::Parse {
            detail: "cluster introducer address cannot be empty".into(),
        });
    }
    Ok(address.to_string())
}

/// Parse this domain's arguments without executing a command.
pub fn parse_rollout_command(args: &[String]) -> Result<RolloutCliCommand, CliParseError> {
    parse_command_args(rollout_cli_spec(), args, parse_rollout_matches)
}

/// Register the owner's grammar with a host-supplied execution adapter.
pub fn register_cli_commands<E: Into<CliCommandError> + 'static>(
    registry: CliCommandRegistry,
    run: fn(RolloutCliCommand) -> Result<(), E>,
) -> Result<CliCommandRegistry, String> {
    registry.try_register(rollout_cli_command_handler(run))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DesktopOptions {
    pub action: String,
    pub directory: Option<PathBuf>,
    pub repo: Option<PathBuf>,
    pub revision: Option<String>,
    pub target_os: String,
    pub target_arch: String,
    pub targets: Vec<String>,
}

fn desktop_cli_spec() -> CliCommandSpec {
    let directory = || {
        CliOptionSpec::value(
            "--directory",
            "desktop release directory",
            CliValueSpec::new("path", "path"),
        )
    };
    CliCommandSpec::new("app", "build, stage, inspect, or launch a desktop release")
        .help_on_empty()
        .with_children([
            CliCommandSpec::new(
                "build",
                "resolve a desktop bundle through the cluster build service",
            )
            .with_options([
                directory(),
                CliOptionSpec::value(
                    "--target",
                    "node whose desktop platform to build",
                    CliValueSpec::new("node", "node"),
                ),
                CliOptionSpec::value(
                    "--repo",
                    "prepared source checkout (defaults to managed source)",
                    CliValueSpec::new("path", "path"),
                ),
            ]),
            CliCommandSpec::new("package", "manifest already built native desktop binaries")
                .with_options([
                    directory(),
                    CliOptionSpec::value(
                        "--revision",
                        "exact source revision",
                        CliValueSpec::new("commit", "commit"),
                    ),
                    CliOptionSpec::value(
                        "--target-os",
                        "artifact operating system",
                        CliValueSpec::new("os", "os"),
                    ),
                    CliOptionSpec::value(
                        "--target-arch",
                        "artifact architecture",
                        CliValueSpec::new("arch", "arch"),
                    ),
                ]),
            CliCommandSpec::new("stage", "verify a local desktop bundle and notify the app")
                .with_options([
                    directory(),
                    CliOptionSpec::value(
                        "--target",
                        "authorized node to offer the update to",
                        CliValueSpec::new("node", "node"),
                    )
                    .repeatable(),
                ]),
            CliCommandSpec::new("status", "show desktop rollout state"),
            CliCommandSpec::new(
                "launch",
                "launch the installed desktop app with supervision",
            ),
        ])
}

fn parse_desktop_matches(matches: &CliCommandMatches) -> Result<DesktopOptions, CliParseError> {
    let action = matches
        .path()
        .last()
        .cloned()
        .ok_or_else(|| CliParseError::Parse {
            detail: "app action required".into(),
        })?;
    let directory = matches.value("--directory").map(PathBuf::from);
    let revision = matches.value("--revision").map(str::to_owned);
    if matches!(action.as_str(), "stage" | "package") && directory.is_none() {
        return Err(CliParseError::Parse {
            detail: "--directory is required".into(),
        });
    }
    if action == "package" && revision.is_none() {
        return Err(CliParseError::Parse {
            detail: "--revision is required".into(),
        });
    }
    Ok(DesktopOptions {
        targets: matches.values("--target").map(str::to_owned).collect(),
        action,
        directory,
        repo: matches.value("--repo").map(PathBuf::from),
        revision,
        target_os: matches
            .value("--target-os")
            .unwrap_or(std::env::consts::OS)
            .into(),
        target_arch: matches
            .value("--target-arch")
            .unwrap_or(std::env::consts::ARCH)
            .into(),
    })
}
