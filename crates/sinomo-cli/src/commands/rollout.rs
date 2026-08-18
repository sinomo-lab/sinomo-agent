use super::*;
use sinomo_runtime::build::{
    BuildArtifactKind, BuildArtifactRequest, BuildArtifactResponse, BuildArtifactSetManifest,
    BuildCandidate, BuildCapabilitiesRequest, BuildCapabilitiesResponse, BuildJobState,
    BuildKeyInput, BuildOutputRequest, BuildPlan, BuildSource, BuildStatusRequest,
    BuildStatusResponse, BuildSubmitRequest, BuildSubmitResponse, BuildTarget,
    BuildTargetCapability, NativeModuleSelection, build_content_hash,
    discover_native_dylib_outputs, rank_build_candidates,
};
use sinomo_runtime::cli_registry::{
    CliCommandMatches, CliCommandPolicy, CliCommandSpec, CliOptionSpec, CliParseError,
    CliPositionalSpec, CliValueSpec,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum RolloutCliCommand {
    InitSource(RolloutOptions),
    ImportSource(RolloutOptions),
    ExportSource(RolloutOptions),
    ApplyExternal(RolloutOptions),
    Apply(RolloutOptions),
    Cluster(RolloutOptions),
    ModuleNativeDylib(RolloutModuleNativeDylibOptions),
    Advance(RolloutAdvanceOptions),
    TelemetryGatesShow,
    TelemetryGatesSet(RolloutTelemetryGateSetOptions),
    Diagnostics(RolloutOptions),
    ClearStatus(RolloutOptions),
    Status,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RolloutOptions {
    pub(crate) repo: Option<PathBuf>,
    pub(crate) binary: Option<PathBuf>,
    pub(crate) remote_repo: Option<PathBuf>,
    pub(crate) targets: Vec<String>,
    pub(crate) host: String,
    /// True only when the operator supplied `--host`. Rollout uses this to
    /// distinguish requested main/shadow listener changes from the loopback
    /// CLI compatibility default.
    pub(crate) host_explicit: bool,
    pub(crate) port: u16,
    pub(crate) shadow_port: u16,
    pub(crate) skip_local: bool,
    pub(crate) remote_build: bool,
    pub(crate) skip_native_modules: bool,
    pub(crate) force_source_overwrite: bool,
}

impl Default for RolloutOptions {
    fn default() -> Self {
        Self {
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
pub(crate) struct RolloutModuleNativeDylibOptions {
    pub(crate) module_selector: String,
    pub(crate) rollout: RolloutOptions,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct RolloutAdvanceOptions {
    pub(crate) node_id: Option<NodeId>,
    pub(crate) scorecard: Option<PathBuf>,
    pub(crate) module_replay_report: Option<PathBuf>,
    pub(crate) shadow_telemetry_limit: Option<usize>,
    pub(crate) incident_event_ids: Vec<EventId>,
    pub(crate) user_correction_count: u32,
    pub(crate) rollback_triggers: Vec<RollbackTrigger>,
    pub(crate) evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct RolloutTelemetryGateSetOptions {
    pub(crate) automatic_promotion_enabled: Option<bool>,
    pub(crate) event_limit: Option<usize>,
    pub(crate) minimum_event_count: Option<u64>,
    pub(crate) maximum_automatic_weight_bps: Option<Option<u16>>,
    pub(crate) maximum_candidate_latency_p95_ms: Option<Option<u64>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RolloutTarget {
    pub(crate) address: String,
    pub(crate) endpoint: Option<ClusterEndpointRecord>,
    pub(crate) rollout_node_id: Option<NodeId>,
    /// Other authenticated main/shadow endpoints discovered for the same
    /// rollout authority. They remain available while the selected main route
    /// is intentionally restarting.
    restart_endpoints: Vec<ClusterEndpointRecord>,
}

impl RolloutTarget {
    pub(crate) fn address(address: String) -> Self {
        Self {
            address,
            endpoint: None,
            rollout_node_id: None,
            restart_endpoints: Vec::new(),
        }
    }

    pub(crate) fn explicit_endpoint(endpoint: ClusterEndpointRecord) -> Self {
        Self {
            address: endpoint.address.clone(),
            endpoint: Some(endpoint),
            rollout_node_id: None,
            restart_endpoints: Vec::new(),
        }
    }

    pub(crate) fn endpoint_for_node(
        endpoint: ClusterEndpointRecord,
        rollout_node_id: NodeId,
    ) -> Self {
        Self {
            address: endpoint.address.clone(),
            endpoint: Some(endpoint),
            rollout_node_id: Some(rollout_node_id),
            restart_endpoints: Vec::new(),
        }
    }

    pub(crate) fn routed_node(rollout_node_id: NodeId) -> Self {
        Self {
            address: rollout_node_id.to_string(),
            endpoint: None,
            rollout_node_id: Some(rollout_node_id),
            restart_endpoints: Vec::new(),
        }
    }

    pub(crate) fn label(&self) -> &str {
        &self.address
    }

    pub(crate) fn rollout_node_id(&self) -> Option<&NodeId> {
        self.rollout_node_id.as_ref()
    }

    fn with_restart_endpoints(mut self, restart_endpoints: Vec<ClusterEndpointRecord>) -> Self {
        self.restart_endpoints = restart_endpoints;
        self
    }

    #[cfg(test)]
    pub(crate) fn restart_endpoints(&self) -> &[ClusterEndpointRecord] {
        &self.restart_endpoints
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RolloutAuthorityTarget {
    cluster_id: Id,
    node_id: NodeId,
}

#[derive(Clone, Copy, Debug)]
struct RolloutRuntimeSourceIdentity<'a> {
    repo: &'a Path,
    revision: &'a str,
}

fn rollout_cluster_target_from_status(status: &DaemonStatus) -> Option<RolloutAuthorityTarget> {
    let cluster = status.cluster.as_ref()?;
    (cluster.process == DAEMON_MAIN_CLUSTER_ROLE).then(|| RolloutAuthorityTarget {
        cluster_id: cluster.cluster_id.clone(),
        node_id: cluster.node_id.clone(),
    })
}

fn validate_rollout_runtime_source_identity(
    target_label: &str,
    expected: RolloutRuntimeSourceIdentity<'_>,
    status: &DaemonStatus,
) -> Result<(), CliError> {
    let target_identity = status
        .cluster
        .as_ref()
        .map(|cluster| format!("{}/{}", cluster.cluster_id, cluster.node_id))
        .unwrap_or_else(|| target_label.to_string());
    let observed = status.commit.as_deref().ok_or_else(|| CliError::ClusterReadOnly {
        detail: format!(
            "rollout target {target_identity} did not report a runtime commit after activation; expected source revision {:?}",
            expected.revision
        ),
    })?;
    if observed.ends_with("-dirty") {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "rollout target {target_identity} reported dirty runtime commit {observed:?} after a source-snapshot build; expected clean source revision {:?}",
                expected.revision
            ),
        });
    }
    let expected_commit = sinomo_runtime::rollout::resolve_rollout_source_commit(
        expected.repo,
        expected.revision,
    )
    .map_err(|error| CliError::ClusterReadOnly {
        detail: format!(
            "rollout target {target_identity} expected source revision {:?} could not be resolved unambiguously in {}: {error}",
            expected.revision,
            expected.repo.display()
        ),
    })?;
    let observed_commit =
        sinomo_runtime::rollout::resolve_rollout_source_commit(expected.repo, observed).map_err(
            |error| CliError::ClusterReadOnly {
                detail: format!(
                    "rollout target {target_identity} reported runtime commit {observed:?}, which could not be resolved unambiguously in {}: {error}",
                    expected.repo.display()
                ),
            },
        )?;
    if observed_commit != expected_commit {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "rollout target {target_identity} activated runtime commit {observed:?} ({observed_commit}), expected source revision {:?} ({expected_commit})",
                expected.revision
            ),
        });
    }
    Ok(())
}

pub(crate) fn rollout_cli_command_handler() -> DeclarativeCommandHandler<RolloutCliCommand> {
    DeclarativeCommandHandler::new(
        "rollout",
        CliCommandPolicy::default(),
        rollout_cli_spec,
        parse_rollout_matches,
        run_rollout_cli_command,
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
        CliOptionSpec::flag("--remote-build", "build on a remote target"),
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
            CliCommandSpec::new("init-source", "initialize the managed source repository")
                .with_options(source_options()),
            CliCommandSpec::new("import-source", "merge external source into managed source")
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
                .with_options([option("--target", "rollout target", "host:port").repeatable()]),
            CliCommandSpec::new("clear-status", "clear stale rollout status")
                .with_aliases(["clear-state"])
                .with_options([option("--target", "rollout target", "host:port").repeatable()]),
            CliCommandSpec::new("status", "show rollout paths and state"),
        ])
}

fn rollout_options_from_matches(
    matches: &CliCommandMatches,
) -> Result<RolloutOptions, CliParseError> {
    let mut options = RolloutOptions::default();
    options.repo = matches.value("--repo").map(PathBuf::from);
    options.binary = matches.value("--binary").map(PathBuf::from);
    options.remote_repo = matches.value("--remote-repo").map(PathBuf::from);
    options.targets = matches
        .values("--target")
        .map(|value| non_empty_address(value).map_err(semantic_parse_error))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(host) = matches.value("--host") {
        options.host = host.to_string();
        options.host_explicit = true;
    }
    options.port = matches
        .value("--port")
        .map(|value| {
            value.parse::<u16>().map_err(|source| CliParseError::Parse {
                detail: format!("invalid rollout daemon port `{value}`: {source}"),
            })
        })
        .transpose()?
        .unwrap_or(options.port);
    options.shadow_port = matches
        .value("--shadow-port")
        .map(|value| {
            value.parse::<u16>().map_err(|source| CliParseError::Parse {
                detail: format!("invalid rollout shadow daemon port `{value}`: {source}"),
            })
        })
        .transpose()?
        .unwrap_or(options.shadow_port);
    options.skip_local = matches.contains("--skip-local");
    options.remote_build = matches.contains("--remote-build");
    options.skip_native_modules = matches.contains("--skip-native-modules");
    options.force_source_overwrite = matches.contains("--force-source");
    Ok(options)
}

fn parse_rollout_matches(matches: &CliCommandMatches) -> Result<RolloutCliCommand, CliParseError> {
    let path = matches
        .path()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    match path.as_slice() {
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

#[cfg(test)]
pub(crate) fn parse_rollout_schema_for_test(
    args: &[String],
) -> Result<RolloutCliCommand, CliError> {
    parse_schema_command_for_test(rollout_cli_spec(), args, parse_rollout_matches)
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

fn reject_native_module_skip(options: &RolloutOptions, command: &str) -> Result<(), CliError> {
    if options.skip_native_modules {
        return Err(CliError::Parse {
            detail: format!(
                "--skip-native-modules is only supported by `rollout cluster` binary-push rollouts; `{command}` must keep the binary and native module set together"
            ),
        });
    }
    Ok(())
}

fn validate_cluster_native_module_skip(options: &RolloutOptions) -> Result<(), CliError> {
    if !options.skip_native_modules {
        return Ok(());
    }
    if options.remote_build {
        return Err(CliError::Parse {
            detail: "--skip-native-modules cannot be combined with --remote-build; it is a binary-push bootstrap option"
                .to_string(),
        });
    }
    if options.remote_repo.is_some() {
        return Err(CliError::Parse {
            detail: "--skip-native-modules cannot be combined with --remote-repo; it is a binary-push bootstrap option"
                .to_string(),
        });
    }
    Ok(())
}

fn non_empty_rollout_module_selector(value: &str) -> Result<String, CliError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CliError::Parse {
            detail: "module selector must not be empty".to_string(),
        });
    }
    Ok(value.to_string())
}

fn parse_nonzero_usize(label: &str, value: &str) -> Result<usize, CliError> {
    let value = value.parse::<usize>().map_err(|source| CliError::Parse {
        detail: format!("invalid {label} `{value}`: {source}"),
    })?;
    if value == 0 {
        return Err(CliError::Parse {
            detail: format!("{label} must be non-zero"),
        });
    }
    Ok(value)
}

fn parse_optional_bps(value: &str) -> Result<Option<u16>, CliError> {
    if matches!(value, "none" | "off" | "unlimited") {
        return Ok(None);
    }
    let weight = value.parse::<u16>().map_err(|source| CliError::Parse {
        detail: format!("invalid rollout telemetry max automatic weight `{value}`: {source}"),
    })?;
    if weight > 10_000 {
        return Err(CliError::Parse {
            detail: "rollout telemetry max automatic weight must be <= 10000 bps".to_string(),
        });
    }
    Ok(Some(weight))
}

fn parse_optional_u64(label: &str, value: &str) -> Result<Option<u64>, CliError> {
    if matches!(value, "none" | "off" | "unlimited") {
        return Ok(None);
    }
    let parsed = value.parse::<u64>().map_err(|source| CliError::Parse {
        detail: format!("invalid rollout telemetry {label} `{value}`: {source}"),
    })?;
    if parsed == 0 {
        return Err(CliError::Parse {
            detail: format!("rollout telemetry {label} must be non-zero"),
        });
    }
    Ok(Some(parsed))
}

fn parse_rollout_node_id(value: &str) -> Result<NodeId, CliError> {
    NodeId::new(value.to_string()).map_err(|source| CliError::Parse {
        detail: format!("invalid rollout node id `{value}`: {source}"),
    })
}

fn parse_rollout_event_id(value: &str) -> Result<EventId, CliError> {
    EventId::new(value.to_string()).map_err(|source| CliError::Parse {
        detail: format!("invalid rollout incident event id `{value}`: {source}"),
    })
}

fn parse_rollout_rollback_trigger(value: &str) -> Result<RollbackTrigger, CliError> {
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
        _ => Err(CliError::Parse {
            detail: format!("invalid rollback trigger `{value}`"),
        }),
    }
}

pub(crate) fn run_rollout_cli_command(command: RolloutCliCommand) -> Result<(), CliError> {
    match command {
        RolloutCliCommand::InitSource(options) => rollout_init_source(options),
        RolloutCliCommand::ImportSource(options) => rollout_import_source(options),
        RolloutCliCommand::ExportSource(options) => rollout_export_source(options),
        RolloutCliCommand::ApplyExternal(options) => rollout_apply_external(options),
        RolloutCliCommand::Apply(options) => rollout_apply(options),
        RolloutCliCommand::Cluster(options) => rollout_cluster(options),
        RolloutCliCommand::ModuleNativeDylib(options) => rollout_module_native_dylib(options),
        RolloutCliCommand::Advance(options) => rollout_advance(options),
        RolloutCliCommand::TelemetryGatesShow => rollout_telemetry_gates_show(),
        RolloutCliCommand::TelemetryGatesSet(options) => rollout_telemetry_gates_set(options),
        RolloutCliCommand::Diagnostics(options) => rollout_diagnostics(options),
        RolloutCliCommand::ClearStatus(options) => rollout_clear_status(options),
        RolloutCliCommand::Status => rollout_status(),
    }
}

pub(crate) fn rollout_paths_from_defaults() -> Result<SelfRolloutPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(SelfRolloutPaths::new(&data_dir.root()))
}

pub(crate) fn rollout_module_native_dylib(
    options: RolloutModuleNativeDylibOptions,
) -> Result<(), CliError> {
    let targets = rollout_cluster_targets(&options.rollout)?;
    if targets.is_empty() {
        return Err(CliError::Parse {
            detail: "no module rollout targets found; pass --target <host:port> or start a clustered local daemon".to_string(),
        });
    }
    let mut outcomes = ClusterRolloutOutcomeSummary::default();
    let groups = collect_cluster_build_target_groups(targets, &mut outcomes);
    if groups.is_empty() {
        return outcomes.finish();
    }
    if block_cluster_build_groups_after_discovery_failure(&mut outcomes, &groups, "module") {
        return outcomes.finish();
    }
    let source = prepare_cluster_build_source_input(&options.rollout)?;
    let contacts = cluster_build_contacts(&options.rollout, &groups);
    let selection = ClusterBuildOutputSelection::NativeDylibModule(options.module_selector.clone());
    let mut resolved = Vec::new();
    let mut resolution_failed = false;
    for group in groups {
        println!(
            "resolving module {} for {} / {} ({} rollout target(s))",
            options.module_selector,
            group.target.os,
            group.target.arch,
            group.rollout_targets.len()
        );
        match resolve_cluster_build_group(&source, group.clone(), &contacts, &selection) {
            Ok(group) => resolved.push(group),
            Err(error) => {
                resolution_failed = true;
                record_cluster_build_group_failure(&mut outcomes, &group, &error);
            }
        }
    }
    if resolution_failed {
        for group in &resolved {
            for target in &group.group.rollout_targets {
                let detail = "another target platform failed module build resolution; no module was activated"
                    .to_string();
                println!("{}: blocked: {detail}", target.label());
                outcomes.record_failure(target.label(), "blocked", detail);
            }
        }
        return outcomes.finish();
    }

    let requested_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let prepared = resolved
        .into_iter()
        .map(|group| {
            let [artifact] = group.artifacts.native_dylib_modules.as_slice() else {
                return Err(CliError::Parse {
                    detail: format!(
                        "module build {} did not resolve exactly one artifact",
                        group.builder.plan.key
                    ),
                });
            };
            println!(
                "module rollout artifact: {}",
                artifact.built_library.display()
            );
            println!("  module: {}", artifact.module_id);
            println!("  interface: {}", artifact.interface_version);
            println!("  source: {}", artifact.source);
            let mut request = prepare_cluster_rollout_native_dylib_module_request(
                artifact.source.clone(),
                artifact,
                options.rollout.host.clone(),
                options.rollout.port,
                options.rollout.shadow_port,
                requested_at.clone(),
            )?;
            request.host_override = options
                .rollout
                .host_explicit
                .then(|| options.rollout.host.clone());
            Ok((group.group, Arc::new(request)))
        })
        .collect::<Result<Vec<_>, CliError>>()?;
    for (group, request) in prepared {
        for target in &group.rollout_targets {
            let preflight = match read_rollout_target_status_before_cluster_activation(target) {
                Ok(status) => status,
                Err(error) => {
                    println!("{}: failed: {error}", target.label());
                    outcomes.record_failure(target.label(), "failed", error.to_string());
                    return outcomes.finish();
                }
            };
            match send_daemon_rollout_native_dylib_module(target, Arc::clone(&request)) {
                Ok(response) => {
                    println!("{}: {}", target.label(), response.status);
                    if response.status != "accepted" {
                        outcomes.record_failure(
                            target.label(),
                            &response.status,
                            response.detail.clone(),
                        );
                    }
                    if !response.detail.is_empty() {
                        println!("  {}", response.detail);
                    }
                    if let Err(error) = ensure_rollout_target_authority_ready_after_acceptance(
                        target,
                        preflight.authority_target.as_ref(),
                        None,
                        request.host_override.as_deref(),
                        request.port,
                        &response,
                    ) {
                        println!("{}: authority handoff failed: {error}", target.label());
                        outcomes.record_failure(
                            target.label(),
                            "authority-not-ready",
                            error.to_string(),
                        );
                        return outcomes.finish();
                    }
                    if let Err(error) = ensure_cluster_rollout_host_convergence_after_acceptance(
                        target,
                        &preflight.cluster_target,
                        preflight.authority_target.as_ref(),
                        None,
                        request.host_override.as_deref(),
                        &response,
                        &request.requested_at,
                        request.port,
                        request.shadow_port,
                        |restart_target, requested_at| {
                            let mut retry = request.as_ref().clone();
                            retry.requested_at = requested_at;
                            send_daemon_rollout_native_dylib_module(restart_target, Arc::new(retry))
                        },
                    ) {
                        println!("{}: bind convergence failed: {error}", target.label());
                        outcomes.record_failure(
                            target.label(),
                            "bind-not-converged",
                            error.to_string(),
                        );
                        return outcomes.finish();
                    }
                }
                Err(error) => {
                    println!("{}: failed: {error}", target.label());
                    outcomes.record_failure(target.label(), "failed", error.to_string());
                    if rollout_indeterminate_activation_requires_stop(
                        preflight.authority_target.as_ref(),
                    ) {
                        println!(
                            "{}: activation outcome is indeterminate for a Raft voter; refusing to advance to another cluster authority",
                            target.label()
                        );
                        return outcomes.finish();
                    }
                }
            }
        }
    }
    outcomes.finish()
}

pub(crate) fn rollout_init_source(options: RolloutOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    ensure_rollout_source_repo_with_options(
        &paths,
        options.repo.as_deref(),
        rollout_source_deploy_options(&options),
    )?;
    println!("source repo: {}", paths.source_repo.display());
    Ok(())
}

pub(crate) fn rollout_import_source(options: RolloutOptions) -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    ensure_rollout_source_repo_with_options(
        &paths,
        options.repo.as_deref(),
        rollout_source_deploy_options(&options),
    )?;
    println!("imported external source into managed repo");
    println!("  managed source: {}", paths.source_repo.display());
    print_managed_source_status(&paths)?;
    Ok(())
}

pub(crate) fn rollout_export_source(options: RolloutOptions) -> Result<(), CliError> {
    let Some(repo) = options.repo.as_deref() else {
        return Err(CliError::Parse {
            detail: "export-source requires --repo <external-repo>".to_string(),
        });
    };
    let paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&paths)?;
    sync_external_repo_from_managed(&paths, repo)?;
    println!("exported managed source changes into external repo");
    println!("  managed source: {}", paths.source_repo.display());
    println!("  external repo: {}", repo.display());
    Ok(())
}

pub(crate) fn rollout_apply_external(options: RolloutOptions) -> Result<(), CliError> {
    reject_native_module_skip(&options, "rollout apply-external")?;
    let paths = rollout_paths_from_defaults()?;
    rollout_import_source(options.clone())?;
    rollout_apply(rollout_apply_external_managed_options(options, &paths))
}

fn rollout_apply_external_managed_options(
    mut options: RolloutOptions,
    paths: &SelfRolloutPaths,
) -> RolloutOptions {
    // The import above makes the managed repository the exact source of truth
    // for this apply. Leaving `repo` empty would make `rollout_apply` import
    // the caller's current directory and could immediately replace a detached
    // snapshot with an unrelated operator checkout before building it.
    options.repo = Some(paths.source_repo.clone());
    options
}

#[cfg(test)]
#[test]
fn apply_external_builds_the_imported_managed_snapshot_not_the_current_directory() {
    let paths = SelfRolloutPaths::new(Path::new("/tmp/sinomo-apply-external-test"));
    let detached_snapshot = PathBuf::from("/dev/shm/sinomo-rollout/source");
    let options = RolloutOptions {
        repo: Some(detached_snapshot),
        force_source_overwrite: true,
        ..RolloutOptions::default()
    };

    let apply_options = rollout_apply_external_managed_options(options, &paths);

    assert_eq!(
        apply_options.repo.as_deref(),
        Some(paths.source_repo.as_path())
    );
    assert!(apply_options.force_source_overwrite);
}

pub(crate) fn rollout_advance(options: RolloutAdvanceOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let applied = rollout_advance_with_data_dir(&data_dir, options)?;
    println!("rollout action: {:?}", applied.transition.action);
    println!("stage: {:?}", applied.transition.stage);
    println!("weight bps: {}", applied.transition.current_weight_bps);
    println!("record: {}", applied.transition_path.display());
    println!("module state: {}", applied.module_state_path.display());
    Ok(())
}

pub(crate) fn rollout_advance_with_data_dir(
    data_dir: &SinomoDataDir,
    options: RolloutAdvanceOptions,
) -> Result<AppliedRolloutTransition, CliError> {
    data_dir.ensure_root_layout()?;
    let paths = SelfRolloutPaths::new(&data_dir.root());
    let node_id = resolve_rollout_controller_node_id(data_dir, options.node_id.clone())?;
    let runtime_state =
        if options.module_replay_report.is_some() || options.shadow_telemetry_limit.is_some() {
            read_rollout_runtime_state(&paths).map_err(CliError::from)?
        } else {
            None
        };
    let data_dir_root = data_dir.root();
    let (observation, evidence_refs) =
        rollout_observation_from_advance_options(&data_dir_root, options, runtime_state.as_ref())?;
    let controller = RolloutController::new(node_id);
    let recorded = controller
        .advance_recorded_rollout(&paths, observation, evidence_refs)
        .map_err(CliError::from)?;
    controller
        .apply_recorded_transition(data_dir.root(), &recorded)
        .map_err(Into::into)
}

pub(crate) fn rollout_telemetry_gates_show() -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = SelfRolloutPaths::new(&data_dir.root());
    let config = read_rollout_telemetry_gate_config(&paths)?.unwrap_or_default();
    println!(
        "automatic promotion: {}",
        config.automatic_promotion_enabled
    );
    println!("event limit: {}", config.event_limit);
    println!("minimum events: {}", config.minimum_event_count);
    println!(
        "max automatic weight bps: {}",
        config
            .maximum_automatic_weight_bps
            .map(|weight| weight.to_string())
            .unwrap_or_else(|| "unlimited".to_string())
    );
    println!(
        "max candidate latency p95 ms: {}",
        config
            .maximum_candidate_latency_p95_ms
            .map(|latency| latency.to_string())
            .unwrap_or_else(|| "unlimited".to_string())
    );
    Ok(())
}

pub(crate) fn rollout_telemetry_gates_set(
    options: RolloutTelemetryGateSetOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let config = rollout_telemetry_gates_set_with_data_dir(&data_dir, options)?;
    println!("rollout telemetry gates updated");
    println!(
        "automatic promotion: {}",
        config.automatic_promotion_enabled
    );
    println!("event limit: {}", config.event_limit);
    println!("minimum events: {}", config.minimum_event_count);
    println!(
        "max automatic weight bps: {}",
        config
            .maximum_automatic_weight_bps
            .map(|weight| weight.to_string())
            .unwrap_or_else(|| "unlimited".to_string())
    );
    println!(
        "max candidate latency p95 ms: {}",
        config
            .maximum_candidate_latency_p95_ms
            .map(|latency| latency.to_string())
            .unwrap_or_else(|| "unlimited".to_string())
    );
    Ok(())
}

pub(crate) fn rollout_telemetry_gates_set_with_data_dir(
    data_dir: &SinomoDataDir,
    options: RolloutTelemetryGateSetOptions,
) -> Result<RolloutTelemetryGateConfig, CliError> {
    data_dir.ensure_root_layout()?;
    let paths = SelfRolloutPaths::new(&data_dir.root());
    let mut config = read_rollout_telemetry_gate_config(&paths)?.unwrap_or_default();
    if let Some(enabled) = options.automatic_promotion_enabled {
        config.automatic_promotion_enabled = enabled;
    }
    if let Some(limit) = options.event_limit {
        config.event_limit = limit;
    }
    if let Some(count) = options.minimum_event_count {
        config.minimum_event_count = count;
    }
    if let Some(maximum_weight) = options.maximum_automatic_weight_bps {
        config.maximum_automatic_weight_bps = maximum_weight;
    }
    if let Some(maximum_latency) = options.maximum_candidate_latency_p95_ms {
        config.maximum_candidate_latency_p95_ms = maximum_latency;
    }
    write_rollout_telemetry_gate_config(&paths, &config)?;
    Ok(config)
}

fn rollout_observation_from_advance_options(
    data_dir_root: &Path,
    options: RolloutAdvanceOptions,
    runtime_state: Option<&RolloutRuntimeState>,
) -> Result<(RolloutObservation, Vec<String>), CliError> {
    let RolloutAdvanceOptions {
        node_id: _,
        scorecard,
        module_replay_report,
        shadow_telemetry_limit,
        incident_event_ids,
        user_correction_count,
        rollback_triggers,
        mut evidence_refs,
    } = options;
    let evidence_source_count = usize::from(scorecard.is_some())
        + usize::from(module_replay_report.is_some())
        + usize::from(shadow_telemetry_limit.is_some());
    if evidence_source_count > 1 {
        return Err(CliError::Parse {
            detail: "rollout advance accepts only one of --scorecard, --module-replay-report, or --shadow-telemetry".to_string(),
        });
    }
    let scorecard = if let Some(path) = scorecard {
        evidence_refs.push(format!("scorecard:{}", path.display()));
        Some(read_rollout_scorecard(&path)?)
    } else if let Some(path) = module_replay_report {
        evidence_refs.push(format!("module-replay-report:{}", path.display()));
        Some(read_rollout_module_replay_scorecard(&path, runtime_state)?)
    } else if let Some(limit) = shadow_telemetry_limit {
        evidence_refs.push(format!(
            "module-shadow-telemetry:events/event-log.jsonl?limit={limit}"
        ));
        Some(read_rollout_shadow_telemetry_scorecard(
            data_dir_root,
            limit,
            runtime_state,
        )?)
    } else {
        None
    };
    Ok((
        RolloutObservation {
            scorecard,
            incident_event_ids,
            user_correction_count,
            rollback_triggers,
        },
        evidence_refs,
    ))
}

fn read_rollout_scorecard(path: &Path) -> Result<EvaluationScorecard, CliError> {
    let content = fs::read_to_string(path).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| CliError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn read_rollout_module_replay_scorecard(
    path: &Path,
    runtime_state: Option<&RolloutRuntimeState>,
) -> Result<EvaluationScorecard, CliError> {
    let content = fs::read_to_string(path).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let report: ModuleReplayReport =
        serde_json::from_str(&content).map_err(|source| CliError::Json {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    if let Some(state) = runtime_state
        && report.module_id != state.plan.module_id
    {
        return Err(CliError::Parse {
            detail: format!(
                "module replay report {} targets module {}, but rollout state targets {}",
                path.display(),
                report.module_id,
                state.plan.module_id
            ),
        });
    }

    let baseline_artifact_id =
        runtime_state.map(|state| state.loader_state.active_artifact_id.clone());
    let candidate_artifact_id = runtime_state.and_then(|state| {
        state
            .loader_state
            .candidate_artifact_id
            .clone()
            .or_else(|| state.loader_state.shadow_artifact_id.clone())
            .or_else(|| Some(state.plan.artifact_id.clone()))
    });
    Ok(rollout_module_replay_report_scorecard(
        &report,
        baseline_artifact_id,
        candidate_artifact_id,
    ))
}

fn rollout_module_replay_report_scorecard(
    report: &ModuleReplayReport,
    baseline_artifact_id: Option<sinomo_runtime::module::ArtifactId>,
    candidate_artifact_id: Option<sinomo_runtime::module::ArtifactId>,
) -> EvaluationScorecard {
    let outcome = rollout_module_replay_report_outcome(report);
    EvaluationScorecard {
        scorecard_id: sinomo_runtime::core::Id::generate(),
        baseline_artifact_id,
        candidate_artifact_id,
        outcome: outcome.clone(),
        gates: vec![
            sinomo_runtime::core::EvaluationGateResult {
                gate: sinomo_runtime::core::EvaluationGate::ReplayTests,
                outcome: if report.missing_route_count == 0 && !report.cases.is_empty() {
                    sinomo_runtime::core::EvaluationOutcome::Passed
                } else {
                    sinomo_runtime::core::EvaluationOutcome::Failed
                },
                summary: format!(
                    "{} replay cases, {} missing routes",
                    report.cases.len(),
                    report.missing_route_count
                ),
            },
            sinomo_runtime::core::EvaluationGateResult {
                gate: sinomo_runtime::core::EvaluationGate::ShadowMode,
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
            rollout_module_replay_metric("module_replay_case_count", report.cases.len() as i64),
            rollout_module_replay_metric(
                "module_replay_event_count",
                report.replay_event_count as i64,
            ),
            rollout_module_replay_metric(
                "module_replay_missing_route_count",
                report.missing_route_count as i64,
            ),
            rollout_module_replay_metric(
                "module_replay_primary_error_count",
                report.primary_error_count as i64,
            ),
            rollout_module_replay_metric(
                "module_replay_shadow_error_count",
                report.shadow_error_count as i64,
            ),
            rollout_module_replay_metric(
                "module_replay_shadow_mismatch_count",
                report.shadow_mismatch_count as i64,
            ),
        ],
    }
}

fn rollout_module_replay_report_outcome(
    report: &ModuleReplayReport,
) -> sinomo_runtime::core::EvaluationOutcome {
    if report.cases.is_empty() {
        return sinomo_runtime::core::EvaluationOutcome::Inconclusive;
    }
    if report.missing_route_count == 0
        && report.primary_error_count == 0
        && report.shadow_error_count == 0
        && report.shadow_mismatch_count == 0
    {
        sinomo_runtime::core::EvaluationOutcome::Passed
    } else {
        sinomo_runtime::core::EvaluationOutcome::Failed
    }
}

fn rollout_module_replay_metric(name: &str, value: i64) -> sinomo_runtime::core::EvaluationMetric {
    sinomo_runtime::core::EvaluationMetric {
        name: name.to_string(),
        value: sinomo_runtime::core::MetricValue::Integer(value),
        sample_count: None,
    }
}

fn read_rollout_shadow_telemetry_scorecard(
    data_dir_root: &Path,
    limit: usize,
    runtime_state: Option<&RolloutRuntimeState>,
) -> Result<EvaluationScorecard, CliError> {
    let state = runtime_state.ok_or_else(|| CliError::Parse {
        detail: "rollout shadow telemetry requires persisted rollout runtime state".to_string(),
    })?;
    let event_store = JsonlEventStore::new(data_dir_root.join("events").join("event-log.jsonl"));
    Ok(shadow_telemetry_scorecard_from_event_store(
        state,
        &event_store,
        limit,
    )?)
}

fn resolve_rollout_controller_node_id(
    data_dir: &SinomoDataDir,
    explicit_node_id: Option<NodeId>,
) -> Result<NodeId, CliError> {
    if let Some(node_id) = explicit_node_id {
        return Ok(node_id);
    }
    if let Some(status) = daemon_default_main_daemon_status(DAEMON_PROBE_TIMEOUT)?
        && let Some(cluster) = status.cluster
    {
        return Ok(cluster.node_id);
    }
    let cluster_paths = ClusterPaths::new(data_dir);
    if let Ok(state) = read_cluster_state(&cluster_paths) {
        return Ok(state.local_node_id);
    }
    let identity = load_or_create_main_node_identity(&data_dir.root())?;
    Ok(NodeId::derived(
        "sinomo.rollout.local-node",
        identity.public_key.as_bytes(),
    ))
}

pub(crate) fn rollout_status() -> Result<(), CliError> {
    let paths = rollout_paths_from_defaults()?;
    let status = rollout_status_view(&paths)?;
    println!("rollout source: {}", status.source_repo.display());
    println!("main binary: {}", status.main_binary.display());
    println!("shadow binary: {}", status.shadow_binary.display());
    println!("state: {}", status.state.display());
    println!("source repo exists: {}", status.source_repo_exists);
    println!("main binary exists: {}", status.main_binary_exists);
    println!("shadow binary exists: {}", status.shadow_binary_exists);
    print_managed_source_status_view(status.source_status);
    Ok(())
}

pub(crate) fn rollout_diagnostics(options: RolloutOptions) -> Result<(), CliError> {
    let targets = if options.targets.is_empty() {
        vec![RolloutTarget::address(
            default_main_daemon_status()?
                .map(|status| status.address)
                .unwrap_or_else(|| daemon_connect_address(&options.host, options.port)),
        )]
    } else {
        explicit_rollout_targets(options.targets)
    };
    for target in targets {
        println!("== {} ==", target.label());
        match send_daemon_rollout_diagnostics(&target) {
            Ok(response) => print!("{response}"),
            Err(error) => println!("failed: {error}"),
        }
    }
    Ok(())
}

pub(crate) fn rollout_clear_status(options: RolloutOptions) -> Result<(), CliError> {
    if options.targets.is_empty() {
        if let Some(status) = default_main_daemon_status()? {
            match daemon_send_daemon_rollout_clear_status(
                &status.address,
                DAEMON_CLUSTER_KV_TXN_TIMEOUT,
            )
            .map_err(CliError::from)
            {
                Ok(response) => {
                    print!("{response}");
                    return Ok(());
                }
                Err(error) => eprintln!(
                    "warning: could not clear rollout status through local daemon: {error}; clearing local state file directly"
                ),
            }
        }
        let paths = rollout_paths_from_defaults()?;
        if clear_rollout_status(&paths)? {
            println!("cleared rollout status at {}", paths.state.display());
        } else {
            println!("no rollout status to clear at {}", paths.state.display());
        }
        return Ok(());
    }

    for target in unique_rollout_targets(options.targets) {
        let target = explicit_rollout_target(target);
        match send_daemon_rollout_clear_status(&target) {
            Ok(response) => print!("{}: {response}", target.label()),
            Err(error) => println!("{}: failed: {error}", target.label()),
        }
    }
    Ok(())
}

pub(crate) fn print_managed_source_status(paths: &SelfRolloutPaths) -> Result<(), CliError> {
    let status = managed_source_status(paths)?;
    print_managed_source_status_view(status);
    Ok(())
}

pub(crate) fn print_managed_source_status_view(status: ManagedSourceStatus) {
    println!("source git repo: {}", status.is_git_repo);
    if let Some(head) = status.head {
        println!("source head: {head}");
    }
    println!("source external remotes: {}", status.has_external_remotes);
    if !status.remotes.is_empty() {
        println!("source remotes: {}", status.remotes.join(", "));
    }
}

pub(crate) fn rollout_apply(options: RolloutOptions) -> Result<(), CliError> {
    reject_native_module_skip(&options, "rollout apply")?;
    let paths = rollout_paths_from_defaults()?;
    // Local preparation also writes the shared staged binary paths. Acquire
    // before preparation so a second apply rejects before it can overwrite the
    // first candidate. The local build is conservatively kept inside the guard.
    let activation_guard = acquire_rollout_activation_guard(&paths)?;
    let rollout_plan = paths.plan();
    let artifacts = prepare_local_rollout_artifacts_with_options(
        &paths,
        options.repo.as_deref(),
        rollout_source_deploy_options(&options),
    )?;

    let data_dir = SinomoDataDir::from_env_or_home()?;
    let requested_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let mut plan = daemon_prepare_local_rollout_main_activation(
        &data_dir.root(),
        &paths,
        &artifacts,
        &options.host,
        options.host_explicit.then_some(options.host.as_str()),
        options.port,
        options.shadow_port,
        requested_at,
        DAEMON_PROBE_TIMEOUT,
    );
    stage_native_dylib_loader_state_rollback(&data_dir.root(), &paths, &mut plan.activation)?;
    let installed_modules = match rollout_install_local_rollout_native_dylib_modules(
        &data_dir.root(),
        &artifacts.native_dylib_modules,
    ) {
        Ok(installed) => installed,
        Err(error) => {
            if let Err(rollback_error) = restore_native_dylib_loader_state_for_activation(
                &data_dir.root(),
                &paths,
                &plan.activation,
            ) {
                return Err(CliError::Parse {
                    detail: format!(
                        "native module install failed ({error}); restoring previous native loader state failed: {rollback_error}"
                    ),
                });
            }
            return Err(error.into());
        }
    };
    if let Err(error) =
        reconcile_rollout_native_dylib_platforms(&data_dir.root(), &rollout_plan.build_dir)
    {
        if let Err(rollback_error) = restore_native_dylib_loader_state_for_activation(
            &data_dir.root(),
            &paths,
            &plan.activation,
        ) {
            return Err(CliError::Parse {
                detail: format!(
                    "native module platform reconciliation failed ({error}); restoring previous native loader state failed: {rollback_error}"
                ),
            });
        }
        return Err(error.into());
    }
    // Do NOT stop the shadow supervisor here. Stopping it leaves the node
    // unsupervised whenever the shadow is the root of the process tree — PID 1
    // in a container, or a dev box with no external init to respawn it — which
    // is the dead-node rollout failure. Instead keep the shadow running and let
    // it drive the handoff: it restarts the main on the new binary, then
    // re-execs its own process image in place to adopt the new shadow binary
    // (see handle_daemon_finalize_shadow_rollout). The node is supervised the
    // entire time. When no shadow is running, stage_rollout_main_activation
    // takes the main-only bootstrap path and brings a fresh supervisor up.
    let activation_strategy = match stage_rollout_main_activation(
        &data_dir.root(),
        &paths,
        plan.activation.clone(),
    ) {
        Ok(strategy) => strategy,
        Err(error) => {
            if let Err(rollback_error) = restore_native_dylib_loader_state_for_activation(
                &data_dir.root(),
                &paths,
                &plan.activation,
            ) {
                return Err(CliError::Parse {
                    detail: format!(
                        "rollout activation staging failed ({error}); restoring previous native loader state failed: {rollback_error}"
                    ),
                });
            }
            return Err(error);
        }
    };
    drop(activation_guard);
    if let Some(status) = plan.previous_main_status.as_ref() {
        let _ = send_daemon_shutdown_request(&status.address, DAEMON_PROBE_TIMEOUT);
    }
    wait_for_main_daemon_health(
        &options,
        plan.previous_main_status.as_ref().map(|status| status.pid),
    )?;
    if matches!(
        activation_strategy,
        RolloutActivationStrategy::ShadowSupervised
    ) {
        wait_for_local_shadow_rollout_completion(&data_dir, &options)?;
    }
    println!("rollout applied");
    println!("  source: {}", rollout_plan.build_dir.display());
    println!(
        "  main binary: {}",
        rollout_plan.install_main_binary.display()
    );
    println!(
        "  shadow binary: {}",
        rollout_plan.install_shadow_binary.display()
    );
    for module in installed_modules {
        println!(
            "  native module: {} ({})",
            module.state.module_id, module.state.content_hash
        );
    }
    Ok(())
}

pub(crate) fn rollout_cluster(options: RolloutOptions) -> Result<(), CliError> {
    if options.remote_build
        || options.binary.is_some()
        || options.remote_repo.is_some()
        || options.skip_native_modules
        || options.force_source_overwrite
    {
        return rollout_cluster_legacy(options);
    }
    rollout_cluster_via_build_service(options)
}

#[derive(Clone, Debug)]
struct ClusterBuildTargetGroup {
    target: BuildTarget,
    rollout_targets: Vec<RolloutTarget>,
}

#[derive(Clone, Debug)]
struct ClusterBuildSourceInput {
    source: BuildSource,
    source_repo: PathBuf,
    source_bundle: PathBuf,
    lockfile_hash: String,
}

#[derive(Clone, Debug)]
struct ClusterBuilderPlan {
    contact: RolloutTarget,
    capabilities: BuildCapabilitiesResponse,
    plan: BuildPlan,
    cache_hit: bool,
    transfer_cost: u32,
}

#[derive(Clone, Debug)]
enum ClusterBuildOutputSelection {
    FullRollout,
    NativeDylibModule(String),
}

#[derive(Clone, Debug, Default)]
struct DownloadedClusterBuildArtifacts {
    binary: Option<PathBuf>,
    native_dylib_modules: Vec<LocalRolloutNativeDylibModuleArtifact>,
}

#[derive(Clone, Debug)]
struct ResolvedClusterBuildGroup {
    group: ClusterBuildTargetGroup,
    builder: ClusterBuilderPlan,
    artifacts: DownloadedClusterBuildArtifacts,
}

fn build_target_from_daemon_status(
    address: &str,
    status: &DaemonStatus,
) -> Result<BuildTarget, CliError> {
    let target_os = status.target_os.as_deref().ok_or_else(|| CliError::Parse {
        detail: format!(
            "rollout target {address} did not report its operating system; target-aware build selection is unavailable"
        ),
    })?;
    let target_arch = status
        .target_arch
        .as_deref()
        .ok_or_else(|| CliError::Parse {
            detail: format!(
                "rollout target {address} did not report its architecture; target-aware build selection is unavailable"
            ),
        })?;
    BuildTarget::new(target_os, target_arch).map_err(build_service_cli_error)
}

fn collect_cluster_build_target_groups(
    targets: Vec<RolloutTarget>,
    outcomes: &mut ClusterRolloutOutcomeSummary,
) -> Vec<ClusterBuildTargetGroup> {
    let mut classified = Vec::new();
    let mut seen_nodes = BTreeSet::new();
    for target in targets {
        if target
            .rollout_node_id()
            .is_some_and(|node_id| !seen_nodes.insert(node_id.clone()))
        {
            continue;
        }
        outcomes.begin_target();
        let status = match read_rollout_target_status(&target) {
            Ok(status) => status,
            Err(error) => {
                println!("{}: failed: {error}", target.label());
                outcomes.record_failure(target.label(), "failed", error.to_string());
                continue;
            }
        };
        let build_target = match build_target_from_daemon_status(target.label(), &status) {
            Ok(build_target) => build_target,
            Err(error) => {
                println!("{}: skipped: {error}", target.label());
                outcomes.record_failure(target.label(), "skipped", error.to_string());
                continue;
            }
        };
        classified.push((build_target, target));
    }
    group_cluster_build_targets(classified)
}

fn group_cluster_build_targets(
    targets: impl IntoIterator<Item = (BuildTarget, RolloutTarget)>,
) -> Vec<ClusterBuildTargetGroup> {
    let mut groups = BTreeMap::<BuildTarget, Vec<RolloutTarget>>::new();
    for (target, rollout_target) in targets {
        groups.entry(target).or_default().push(rollout_target);
    }
    groups
        .into_iter()
        .map(|(target, rollout_targets)| ClusterBuildTargetGroup {
            target,
            rollout_targets,
        })
        .collect()
}

fn cluster_build_contacts(
    options: &RolloutOptions,
    rollout_groups: &[ClusterBuildTargetGroup],
) -> Vec<RolloutTarget> {
    let mut contacts = Vec::new();
    let mut discovery = options.clone();
    discovery.targets.clear();
    discovery.skip_local = false;
    if let Ok(discovered) = rollout_cluster_targets(&discovery) {
        for target in discovered {
            push_unique_rollout_target(&mut contacts, target);
        }
    }
    for target in rollout_groups
        .iter()
        .flat_map(|group| group.rollout_targets.iter())
    {
        push_unique_rollout_target(&mut contacts, target.clone());
    }
    contacts
}

fn send_daemon_build_capabilities_for_target(
    target: &RolloutTarget,
    request: BuildCapabilitiesRequest,
) -> Result<BuildCapabilitiesResponse, CliError> {
    match resolve_rollout_node_dispatch(target)? {
        RolloutNodeDispatch::LocalMain(status) => {
            sinomo_runtime::daemon::send_daemon_build_capabilities(&status.address, request)
                .map_err(Into::into)
        }
        RolloutNodeDispatch::Routed(node_id) => {
            sinomo_runtime::daemon::send_daemon_build_capabilities_to_target(&node_id, request)
                .map_err(Into::into)
        }
        RolloutNodeDispatch::Direct => {
            if let Some(endpoint) = &target.endpoint {
                sinomo_runtime::daemon::send_daemon_build_capabilities_to_endpoint(
                    endpoint, request,
                )
                .map_err(Into::into)
            } else {
                sinomo_runtime::daemon::send_daemon_build_capabilities(&target.address, request)
                    .map_err(Into::into)
            }
        }
    }
}

fn send_daemon_build_submit_for_target(
    target: &RolloutTarget,
    request: &BuildSubmitRequest,
    source_bundle: Option<&Path>,
) -> Result<BuildSubmitResponse, CliError> {
    match resolve_rollout_node_dispatch(target)? {
        RolloutNodeDispatch::LocalMain(status) => sinomo_runtime::daemon::send_daemon_build_submit(
            &status.address,
            request,
            source_bundle,
        )
        .map_err(Into::into),
        RolloutNodeDispatch::Routed(node_id) => {
            sinomo_runtime::daemon::send_daemon_build_submit_to_target(
                &node_id,
                request,
                source_bundle,
            )
            .map_err(Into::into)
        }
        RolloutNodeDispatch::Direct => {
            if let Some(endpoint) = &target.endpoint {
                sinomo_runtime::daemon::send_daemon_build_submit_to_endpoint(
                    endpoint,
                    request,
                    source_bundle,
                )
                .map_err(Into::into)
            } else {
                sinomo_runtime::daemon::send_daemon_build_submit(
                    &target.address,
                    request,
                    source_bundle,
                )
                .map_err(Into::into)
            }
        }
    }
}

fn send_daemon_build_status_for_target(
    target: &RolloutTarget,
    request: BuildStatusRequest,
) -> Result<BuildStatusResponse, CliError> {
    match resolve_rollout_node_dispatch(target)? {
        RolloutNodeDispatch::LocalMain(status) => {
            sinomo_runtime::daemon::send_daemon_build_status(&status.address, request)
                .map_err(Into::into)
        }
        RolloutNodeDispatch::Routed(node_id) => {
            sinomo_runtime::daemon::send_daemon_build_status_to_target(&node_id, request)
                .map_err(Into::into)
        }
        RolloutNodeDispatch::Direct => {
            if let Some(endpoint) = &target.endpoint {
                sinomo_runtime::daemon::send_daemon_build_status_to_endpoint(endpoint, request)
                    .map_err(Into::into)
            } else {
                sinomo_runtime::daemon::send_daemon_build_status(&target.address, request)
                    .map_err(Into::into)
            }
        }
    }
}

fn send_daemon_build_artifact_for_target(
    target: &RolloutTarget,
    request: &BuildArtifactRequest,
    destination: &Path,
) -> Result<BuildArtifactResponse, CliError> {
    match resolve_rollout_node_dispatch(target)? {
        RolloutNodeDispatch::LocalMain(status) => {
            sinomo_runtime::daemon::send_daemon_build_artifact(
                &status.address,
                request,
                destination,
            )
            .map_err(Into::into)
        }
        RolloutNodeDispatch::Routed(node_id) => {
            sinomo_runtime::daemon::send_daemon_build_artifact_to_target(
                &node_id,
                request,
                destination,
            )
            .map_err(Into::into)
        }
        RolloutNodeDispatch::Direct => {
            if let Some(endpoint) = &target.endpoint {
                sinomo_runtime::daemon::send_daemon_build_artifact_to_endpoint(
                    endpoint,
                    request,
                    destination,
                )
                .map_err(Into::into)
            } else {
                sinomo_runtime::daemon::send_daemon_build_artifact(
                    &target.address,
                    request,
                    destination,
                )
                .map_err(Into::into)
            }
        }
    }
}

fn supporting_build_capability(
    response: &BuildCapabilitiesResponse,
    target: &BuildTarget,
    kinds: &[BuildArtifactKind],
) -> Option<BuildTargetCapability> {
    let mut matches = response
        .capabilities
        .targets
        .iter()
        .filter(|capability| capability.supports(target, kinds))
        .cloned()
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        left.target
            .cmp(&right.target)
            .then_with(|| left.toolchain_fingerprint.cmp(&right.toolchain_fingerprint))
    });
    matches.into_iter().next()
}

const CLUSTER_ROLLOUT_BUILD_PROFILE: &str = "release";

fn build_plan_for_capability(
    source: &ClusterBuildSourceInput,
    capability: &BuildTargetCapability,
    features: &[String],
    outputs: &[BuildOutputRequest],
) -> Result<BuildPlan, CliError> {
    BuildPlan::new(
        source.source.clone(),
        BuildKeyInput {
            source_hash: source.source.bundle_hash.clone(),
            source_revision: source.source.revision.clone(),
            lockfile_hash: source.lockfile_hash.clone(),
            target: capability.target.clone(),
            // Rollout artifacts are runtime candidates, and may be used for
            // performance acceptance. A debug artifact is neither comparable
            // with a raw-device baseline nor suitable for production rollout.
            profile: CLUSTER_ROLLOUT_BUILD_PROFILE.to_string(),
            features: features.to_vec(),
            toolchain_fingerprint: capability.toolchain_fingerprint.clone(),
            outputs: outputs.to_vec(),
        },
    )
    .map_err(build_service_cli_error)
}

fn cluster_build_outputs_for_target(
    source_repo: &Path,
    target: &BuildTarget,
    selection: &ClusterBuildOutputSelection,
) -> Result<Vec<BuildOutputRequest>, CliError> {
    let (module_selection, include_binary) = match selection {
        ClusterBuildOutputSelection::FullRollout => (NativeModuleSelection::Default, true),
        ClusterBuildOutputSelection::NativeDylibModule(_) => (NativeModuleSelection::All, false),
    };
    // A standalone Sinomo source checkout may not have the optional sibling
    // GUI workspace. Plan against the same temporary workspace view used by
    // the remote Cargo executor so output discovery cannot diverge by host.
    let _source_guards = sinomo_runtime::rollout::prepare_rollout_build_source(source_repo)
        .map_err(CliError::from)?;
    let mut modules =
        discover_native_dylib_outputs(Path::new("cargo"), source_repo, target, module_selection)
            .map_err(build_service_cli_error)?;
    if let ClusterBuildOutputSelection::NativeDylibModule(selector) = selection {
        modules.retain(|output| cluster_build_output_matches_selector(output, selector));
        match modules.len() {
            1 => {}
            0 => {
                return Err(CliError::Parse {
                    detail: format!(
                        "no native-dylib module matches `{selector}` for target {}",
                        target.path_key()
                    ),
                });
            }
            _ => {
                return Err(CliError::Parse {
                    detail: format!(
                        "native-dylib module selector `{selector}` matched multiple modules for target {}: {}",
                        target.path_key(),
                        modules
                            .iter()
                            .map(|output| output.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                });
            }
        }
    }
    if include_binary {
        modules.push(BuildOutputRequest::binary("sinomo", "sinomo"));
    }
    modules.sort();
    modules.dedup();
    Ok(modules)
}

fn cluster_build_output_matches_selector(output: &BuildOutputRequest, selector: &str) -> bool {
    let selector_module_id = sinomo_runtime::module::ModuleId::new(selector.to_string())
        .or_else(|_| sinomo_runtime::module::ModuleId::from_legacy_named(selector))
        .ok();
    output.name == selector
        || output.package == selector
        || output.native_dylib.as_ref().is_some_and(|metadata| {
            Some(&metadata.module_id) == selector_module_id.as_ref()
                || metadata.cache_key == selector
                || metadata.source == selector
        })
}

fn prepare_cluster_build_source_input(
    options: &RolloutOptions,
) -> Result<ClusterBuildSourceInput, CliError> {
    let repo = rollout_source_repo_from_options(options)?;
    let rollout_paths = rollout_paths_from_defaults()?;
    ensure_rollout_layout(&rollout_paths)?;
    let (source_bundle, source_head) = rollout_source_bundle(options)?;
    let source_hash = build_content_hash(&source_bundle);
    let source_key = source_hash
        .strip_prefix("blake3:")
        .unwrap_or(source_hash.as_str());
    let source_dir = rollout_paths.root.join("staged").join("build-sources");
    fs::create_dir_all(&source_dir).map_err(|source| CliError::Io {
        path: source_dir.display().to_string(),
        detail: source.to_string(),
    })?;
    let source_bundle_path = source_dir.join(format!("{source_key}.bundle"));
    let existing_matches = fs::read(&source_bundle_path)
        .map(|existing| {
            existing.len() == source_bundle.len() && build_content_hash(&existing) == source_hash
        })
        .unwrap_or(false);
    if !existing_matches {
        fs::write(&source_bundle_path, &source_bundle).map_err(|source| CliError::Io {
            path: source_bundle_path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let lockfile_path = repo.join("Cargo.lock");
    let lockfile = fs::read(&lockfile_path).map_err(|source| CliError::Io {
        path: lockfile_path.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(ClusterBuildSourceInput {
        source: BuildSource {
            label: repo.display().to_string(),
            revision: Some(source_head),
            bundle_hash: source_hash,
            bundle_bytes: source_bundle.len() as u64,
        },
        source_repo: repo,
        source_bundle: source_bundle_path,
        lockfile_hash: build_content_hash(&lockfile),
    })
}

fn rank_cluster_builder_plans(
    requested_target: &BuildTarget,
    artifact_kinds: &[BuildArtifactKind],
    mut plans: Vec<ClusterBuilderPlan>,
) -> Vec<ClusterBuilderPlan> {
    let ranked = rank_build_candidates(
        requested_target,
        artifact_kinds,
        plans.iter().map(|plan| BuildCandidate {
            capabilities: plan.capabilities.capabilities.clone(),
            cache_hit: plan.cache_hit,
            transfer_cost: plan.transfer_cost,
        }),
    );
    let mut ordered = Vec::with_capacity(ranked.len());
    for ranked_candidate in ranked {
        let node_id = &ranked_candidate.candidate.capabilities.node_id;
        if let Some(index) = plans.iter().position(|plan| {
            plan.capabilities.capabilities.node_id == *node_id
                && plan.cache_hit == ranked_candidate.candidate.cache_hit
        }) {
            ordered.push(plans.remove(index));
        }
    }
    ordered
}

fn discover_cluster_builder_plans(
    source: &ClusterBuildSourceInput,
    group: &ClusterBuildTargetGroup,
    contacts: &[RolloutTarget],
    source_repo: &Path,
    selection: &ClusterBuildOutputSelection,
) -> Result<Vec<ClusterBuilderPlan>, CliError> {
    let artifact_kinds = match selection {
        ClusterBuildOutputSelection::FullRollout => vec![
            BuildArtifactKind::Binary,
            BuildArtifactKind::NativeDylibModule,
        ],
        ClusterBuildOutputSelection::NativeDylibModule(_) => {
            vec![BuildArtifactKind::NativeDylibModule]
        }
    };
    let group_node_ids = group
        .rollout_targets
        .iter()
        .filter_map(|target| target.rollout_node_id().cloned())
        .collect::<BTreeSet<_>>();
    let mut plans = Vec::new();
    let mut unavailable = Vec::new();
    for contact in contacts {
        let initial_request = BuildCapabilitiesRequest {
            target: Some(group.target.clone()),
            build_key: None,
            artifact_kinds: artifact_kinds.clone(),
        };
        let initial = match send_daemon_build_capabilities_for_target(contact, initial_request) {
            Ok(response) => response,
            Err(error) => {
                unavailable.push(format!("{}: {error}", contact.label()));
                continue;
            }
        };
        let Some(capability) =
            supporting_build_capability(&initial, &group.target, &artifact_kinds)
        else {
            continue;
        };
        let outputs =
            match cluster_build_outputs_for_target(source_repo, &capability.target, selection) {
                Ok(outputs) => outputs,
                Err(error) => {
                    unavailable.push(format!("{}: {error}", contact.label()));
                    continue;
                }
            };
        let exact_kinds = outputs
            .iter()
            .map(|output| output.kind)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let plan = build_plan_for_capability(source, &capability, &[], &outputs)?;
        let exact_request = BuildCapabilitiesRequest {
            target: Some(capability.target.clone()),
            build_key: Some(plan.key.clone()),
            artifact_kinds: exact_kinds,
        };
        let exact = match send_daemon_build_capabilities_for_target(contact, exact_request) {
            Ok(response) => response,
            Err(error) => {
                unavailable.push(format!("{}: {error}", contact.label()));
                continue;
            }
        };
        if exact.supports_requested_target == Some(false) {
            continue;
        }
        let cache_hit = exact.cache_hit.unwrap_or(false);
        let transfer_cost = if group_node_ids.contains(&exact.capabilities.node_id)
            || group
                .rollout_targets
                .iter()
                .any(|target| target.label() == contact.label())
        {
            0
        } else {
            1
        };
        plans.push(ClusterBuilderPlan {
            contact: contact.clone(),
            capabilities: exact,
            plan,
            cache_hit,
            transfer_cost,
        });
    }
    let plans = rank_cluster_builder_plans(&group.target, &artifact_kinds, plans);
    if plans.is_empty() {
        let detail = if unavailable.is_empty() {
            format!(
                "no build server advertises support for {} / {} and artifact kinds {:?}",
                group.target.os, group.target.arch, artifact_kinds
            )
        } else {
            format!(
                "no compatible build server was reachable for {} / {} ({})",
                group.target.os,
                group.target.arch,
                unavailable.join("; ")
            )
        };
        return Err(CliError::ExternalCommand {
            command: "cluster build discovery".to_string(),
            detail,
        });
    }
    Ok(plans)
}

fn submit_cluster_build_and_wait(
    builder: &ClusterBuilderPlan,
    source_bundle: &Path,
) -> Result<BuildArtifactSetManifest, CliError> {
    println!(
        "build server: {} ({}, {})",
        builder.contact.label(),
        builder.capabilities.capabilities.node_id,
        if builder.cache_hit {
            "exact cache hit"
        } else {
            "cache miss"
        }
    );
    println!(
        "  capacity: {} CPU threads, {} available slot(s), {} queued",
        builder.capabilities.capabilities.resources.cpu_threads,
        builder
            .capabilities
            .capabilities
            .resources
            .available_slots(),
        builder.capabilities.capabilities.resources.queue_depth
    );
    let request = BuildSubmitRequest {
        plan: builder.plan.clone(),
    };
    let first_bundle = (!builder.cache_hit).then_some(source_bundle);
    let submit = match send_daemon_build_submit_for_target(&builder.contact, &request, first_bundle)
    {
        Ok(response) => response,
        Err(_) if builder.cache_hit => {
            send_daemon_build_submit_for_target(&builder.contact, &request, Some(source_bundle))?
        }
        Err(error) => return Err(error),
    };
    println!("  job: {} ({:?})", submit.job_id, submit.state);
    let status_job_id = submit.job_id;
    let mut last_state = None;
    loop {
        let response = send_daemon_build_status_for_target(
            &builder.contact,
            BuildStatusRequest {
                job_id: status_job_id.clone(),
            },
        )?;
        let status = response.status;
        if status.key != builder.plan.key {
            return Err(CliError::Parse {
                detail: format!(
                    "build server {} returned key {} for requested job {}",
                    builder.contact.label(),
                    status.key,
                    builder.plan.key
                ),
            });
        }
        if last_state != Some(status.state) {
            let queue = status
                .queue_position
                .map(|position| format!(", queue position {position}"))
                .unwrap_or_default();
            println!("  build: {:?}{queue} - {}", status.state, status.detail);
            last_state = Some(status.state);
        }
        match status.state {
            BuildJobState::Succeeded => {
                let artifact_set = status.artifact_set.ok_or_else(|| CliError::Parse {
                    detail: format!(
                        "successful build job {} returned no artifact manifest",
                        status.job_id
                    ),
                })?;
                artifact_set.validate().map_err(build_service_cli_error)?;
                if artifact_set.provenance.build_key != builder.plan.key {
                    return Err(CliError::Parse {
                        detail: format!(
                            "build artifact key {} does not match requested key {}",
                            artifact_set.provenance.build_key, builder.plan.key
                        ),
                    });
                }
                return Ok(artifact_set);
            }
            BuildJobState::Failed | BuildJobState::Cancelled => {
                return Err(CliError::ExternalCommand {
                    command: format!("cluster build job {}", status.job_id),
                    detail: status.detail,
                });
            }
            BuildJobState::Queued | BuildJobState::Running => {
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

fn download_cluster_build_artifacts(
    builder: &ClusterBuilderPlan,
    manifest: &BuildArtifactSetManifest,
) -> Result<DownloadedClusterBuildArtifacts, CliError> {
    let rollout_paths = rollout_paths_from_defaults()?;
    let destination_root = rollout_paths
        .root
        .join("staged")
        .join("build-artifacts")
        .join(builder.plan.key.path_key());
    fs::create_dir_all(&destination_root).map_err(|source| CliError::Io {
        path: destination_root.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut downloaded = DownloadedClusterBuildArtifacts::default();
    for artifact in &manifest.artifacts {
        let file_name = Path::new(&artifact.relative_path)
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| CliError::Parse {
                detail: format!(
                    "build artifact {} has no portable file name",
                    artifact.artifact_id
                ),
            })?;
        let destination = destination_root.join(format!("{}-{file_name}", artifact.artifact_id));
        let request = BuildArtifactRequest {
            build_key: builder.plan.key.clone(),
            artifact_id: artifact.artifact_id.clone(),
        };
        let response =
            send_daemon_build_artifact_for_target(&builder.contact, &request, &destination)?;
        if response.artifact != *artifact {
            return Err(CliError::Parse {
                detail: format!(
                    "build server {} returned a different manifest for artifact {}",
                    builder.contact.label(),
                    artifact.artifact_id
                ),
            });
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&destination, fs::Permissions::from_mode(artifact.mode)).map_err(
                |source| CliError::Io {
                    path: destination.display().to_string(),
                    detail: source.to_string(),
                },
            )?;
        }
        match artifact.kind {
            BuildArtifactKind::Binary => {
                if downloaded.binary.replace(destination).is_some() {
                    return Err(CliError::Parse {
                        detail: "build returned more than one binary artifact".to_string(),
                    });
                }
            }
            BuildArtifactKind::NativeDylibModule => {
                let metadata = artifact
                    .native_dylib
                    .as_ref()
                    .ok_or_else(|| CliError::Parse {
                        detail: format!(
                            "native module artifact {} has no loader metadata",
                            artifact.artifact_id
                        ),
                    })?;
                downloaded
                    .native_dylib_modules
                    .push(LocalRolloutNativeDylibModuleArtifact {
                        module_id: metadata.module_id.to_string(),
                        interface_version: metadata.interface_version.clone(),
                        cache_key: metadata.cache_key.clone(),
                        library_name: metadata.library_name.clone(),
                        built_library: destination,
                        source: metadata.source.clone(),
                    });
            }
        }
    }
    downloaded
        .native_dylib_modules
        .sort_by(|left, right| left.module_id.cmp(&right.module_id));
    Ok(downloaded)
}

fn validate_cluster_build_artifact_set(
    builder: &ClusterBuilderPlan,
    manifest: &BuildArtifactSetManifest,
    downloaded: &DownloadedClusterBuildArtifacts,
) -> Result<(), CliError> {
    if manifest.provenance.input.target != builder.plan.input.target {
        return Err(CliError::Parse {
            detail: format!(
                "build artifact target {} differs from planned target {}",
                manifest.provenance.input.target.path_key(),
                builder.plan.input.target.path_key()
            ),
        });
    }
    let mut expected = builder
        .plan
        .input
        .outputs
        .iter()
        .map(|output| {
            (
                output.kind,
                output.name.clone(),
                output.native_dylib.clone(),
            )
        })
        .collect::<Vec<_>>();
    let mut actual = manifest
        .artifacts
        .iter()
        .map(|artifact| {
            (
                artifact.kind,
                artifact.name.clone(),
                artifact.native_dylib.clone(),
            )
        })
        .collect::<Vec<_>>();
    expected.sort();
    actual.sort();
    if actual != expected {
        return Err(CliError::Parse {
            detail: format!(
                "build artifact set does not match the requested outputs: expected {expected:?}, got {actual:?}"
            ),
        });
    }
    let expected_binary_count = builder
        .plan
        .input
        .outputs
        .iter()
        .filter(|output| output.kind == BuildArtifactKind::Binary)
        .count();
    if usize::from(downloaded.binary.is_some()) != expected_binary_count {
        return Err(CliError::Parse {
            detail: format!(
                "downloaded build contained {} binary artifact(s), expected {expected_binary_count}",
                usize::from(downloaded.binary.is_some())
            ),
        });
    }
    let expected_module_count = builder
        .plan
        .input
        .outputs
        .iter()
        .filter(|output| output.kind == BuildArtifactKind::NativeDylibModule)
        .count();
    if downloaded.native_dylib_modules.len() != expected_module_count {
        return Err(CliError::Parse {
            detail: format!(
                "downloaded build contained {} native module artifact(s), expected {expected_module_count}",
                downloaded.native_dylib_modules.len()
            ),
        });
    }
    Ok(())
}

fn cluster_builder_error_allows_retry(error: &CliError) -> bool {
    matches!(error, CliError::Network { .. } | CliError::Io { .. })
}

fn resolve_cluster_build_group(
    source: &ClusterBuildSourceInput,
    group: ClusterBuildTargetGroup,
    contacts: &[RolloutTarget],
    selection: &ClusterBuildOutputSelection,
) -> Result<ResolvedClusterBuildGroup, CliError> {
    let plans =
        discover_cluster_builder_plans(source, &group, contacts, &source.source_repo, selection)?;
    let mut retry_failures = Vec::new();
    for builder in plans {
        let result =
            submit_cluster_build_and_wait(&builder, &source.source_bundle).and_then(|manifest| {
                let artifacts = download_cluster_build_artifacts(&builder, &manifest)?;
                validate_cluster_build_artifact_set(&builder, &manifest, &artifacts)?;
                Ok(ResolvedClusterBuildGroup {
                    group: group.clone(),
                    builder: builder.clone(),
                    artifacts,
                })
            });
        match result {
            Ok(resolved) => return Ok(resolved),
            Err(error) if cluster_builder_error_allows_retry(&error) => {
                eprintln!(
                    "warning: build server {} became unavailable: {error}; trying the next compatible server",
                    builder.contact.label()
                );
                retry_failures.push(format!("{}: {error}", builder.contact.label()));
            }
            Err(error) => return Err(error),
        }
    }
    Err(CliError::ExternalCommand {
        command: "cluster build resolution".to_string(),
        detail: format!(
            "every compatible build server became unavailable ({})",
            retry_failures.join("; ")
        ),
    })
}

fn record_cluster_build_group_failure(
    outcomes: &mut ClusterRolloutOutcomeSummary,
    group: &ClusterBuildTargetGroup,
    error: &CliError,
) {
    for target in &group.rollout_targets {
        println!("{}: build resolution failed: {error}", target.label());
        outcomes.record_failure(target.label(), "build-failed", error.to_string());
    }
}

fn block_cluster_build_groups_after_discovery_failure(
    outcomes: &mut ClusterRolloutOutcomeSummary,
    groups: &[ClusterBuildTargetGroup],
    operation: &str,
) -> bool {
    if !outcomes.has_failures() {
        return false;
    }
    for group in groups {
        for target in &group.rollout_targets {
            let detail = format!(
                "another requested target could not report its build platform; no {operation} build was submitted and no target was activated"
            );
            println!("{}: blocked: {detail}", target.label());
            outcomes.record_failure(target.label(), "blocked", detail);
        }
    }
    true
}

fn build_service_cli_error(error: impl std::fmt::Display) -> CliError {
    CliError::ExternalCommand {
        command: "cluster build service".to_string(),
        detail: error.to_string(),
    }
}

fn rollout_cluster_via_build_service(options: RolloutOptions) -> Result<(), CliError> {
    validate_cluster_native_module_skip(&options)?;
    let targets = rollout_cluster_targets(&options)?;
    if targets.is_empty() {
        return Err(CliError::Parse {
            detail: "no rollout targets found; pass --target <host:port> or start a clustered local daemon"
                .to_string(),
        });
    }
    let mut outcomes = ClusterRolloutOutcomeSummary::default();
    let groups = collect_cluster_build_target_groups(targets, &mut outcomes);
    if groups.is_empty() {
        return outcomes.finish();
    }
    if block_cluster_build_groups_after_discovery_failure(&mut outcomes, &groups, "rollout") {
        return outcomes.finish();
    }
    let source = prepare_cluster_build_source_input(&options)?;
    let contacts = cluster_build_contacts(&options, &groups);
    let selection = ClusterBuildOutputSelection::FullRollout;
    let mut resolved = Vec::new();
    let mut resolution_failed = false;
    for group in groups {
        println!(
            "resolving build for {} / {} ({} rollout target(s))",
            group.target.os,
            group.target.arch,
            group.rollout_targets.len()
        );
        match resolve_cluster_build_group(&source, group.clone(), &contacts, &selection) {
            Ok(group) => {
                println!("  resolved: {}", group.builder.plan.key);
                resolved.push(group);
            }
            Err(error) => {
                resolution_failed = true;
                record_cluster_build_group_failure(&mut outcomes, &group, &error);
            }
        }
    }
    if resolution_failed {
        for group in &resolved {
            for target in &group.group.rollout_targets {
                let detail = "another target platform failed build resolution; no rollout target was activated"
                    .to_string();
                println!("{}: blocked: {detail}", target.label());
                outcomes.record_failure(target.label(), "blocked", detail);
            }
        }
        return outcomes.finish();
    }

    let requested_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let prepared = resolved
        .into_iter()
        .map(|group| {
            let binary = group
                .artifacts
                .binary
                .as_deref()
                .ok_or_else(|| CliError::Parse {
                    detail: format!(
                        "resolved build {} contained no Sinomo binary",
                        group.builder.plan.key
                    ),
                })?;
            let mut prepared = prepare_cluster_binary_rollout_for_target(
                source.source.label.clone(),
                binary,
                &group.artifacts.native_dylib_modules,
                false,
                group.builder.plan.input.target.os.clone(),
                group.builder.plan.input.target.arch.clone(),
                options.host.clone(),
                options.port,
                options.shadow_port,
                requested_at.clone(),
            )?;
            prepared.manifest.host_override = options.host_explicit.then(|| options.host.clone());
            Ok((group.group, prepared))
        })
        .collect::<Result<Vec<_>, CliError>>()?;
    let runtime_source_revision =
        source
            .source
            .revision
            .as_deref()
            .ok_or_else(|| CliError::Parse {
                detail: "cluster build source has no revision for runtime acceptance".to_string(),
            })?;
    let runtime_source = Some(RolloutRuntimeSourceIdentity {
        repo: &source.source_repo,
        revision: runtime_source_revision,
    });
    for (group, prepared) in prepared {
        for target in &group.rollout_targets {
            let preflight = match read_rollout_target_status_before_cluster_activation(target) {
                Ok(status) => status,
                Err(error) => {
                    println!("{}: failed: {error}", target.label());
                    outcomes.record_failure(target.label(), "failed", error.to_string());
                    return outcomes.finish();
                }
            };
            match send_prepared_daemon_rollout_binary(target, &prepared, &preflight.cluster_target)
            {
                Ok(response) => {
                    println!("{}: {}", target.label(), response.status);
                    if response.status != "accepted" {
                        outcomes.record_failure(
                            target.label(),
                            &response.status,
                            response.detail.clone(),
                        );
                    }
                    if !response.detail.is_empty() {
                        println!("  {}", response.detail);
                    }
                    if let Err(error) = ensure_rollout_target_authority_ready_after_acceptance(
                        target,
                        preflight.authority_target.as_ref(),
                        runtime_source,
                        prepared.manifest.host_override.as_deref(),
                        prepared.manifest.port,
                        &response,
                    ) {
                        println!("{}: authority handoff failed: {error}", target.label());
                        outcomes.record_failure(
                            target.label(),
                            "authority-not-ready",
                            error.to_string(),
                        );
                        return outcomes.finish();
                    }
                    if let Err(error) = ensure_cluster_rollout_host_convergence_after_acceptance(
                        target,
                        &preflight.cluster_target,
                        preflight.authority_target.as_ref(),
                        runtime_source,
                        prepared.manifest.host_override.as_deref(),
                        &response,
                        &prepared.manifest.requested_at,
                        prepared.manifest.port,
                        prepared.manifest.shadow_port,
                        |restart_target, requested_at| {
                            let mut retry = prepared.clone();
                            retry.manifest.requested_at = requested_at;
                            retry.manifest.upload_id = host_convergence_upload_id();
                            send_prepared_daemon_rollout_binary(
                                restart_target,
                                &retry,
                                &preflight.cluster_target,
                            )
                        },
                    ) {
                        println!("{}: bind convergence failed: {error}", target.label());
                        outcomes.record_failure(
                            target.label(),
                            "bind-not-converged",
                            error.to_string(),
                        );
                        return outcomes.finish();
                    }
                }
                Err(error) => {
                    println!("{}: failed: {error}", target.label());
                    outcomes.record_failure(target.label(), "failed", error.to_string());
                    if rollout_indeterminate_activation_requires_stop(
                        preflight.authority_target.as_ref(),
                    ) {
                        println!(
                            "{}: activation outcome is indeterminate for a Raft voter; refusing to advance to another cluster authority",
                            target.label()
                        );
                        return outcomes.finish();
                    }
                }
            }
        }
    }
    outcomes.finish()
}

#[cfg(test)]
mod cluster_build_service_tests {
    use super::*;
    use sinomo_runtime::build::{
        BUILD_PROTOCOL, BuildResourceSnapshot, BuildServiceCapabilities,
        NativeDylibArtifactMetadata,
    };

    fn exact_linux_target() -> BuildTarget {
        BuildTarget::new("linux", "aarch64")
            .expect("target")
            .with_family("unix")
            .expect("family")
            .with_rust_target("aarch64-unknown-linux-gnu")
            .expect("triple")
    }

    fn builder_plan(
        name: &str,
        cpu_threads: u16,
        active_jobs: u16,
        queue_depth: u32,
    ) -> ClusterBuilderPlan {
        let target = exact_linux_target();
        let toolchain_fingerprint = "test-toolchain".to_string();
        let source_hash = build_content_hash(b"source");
        let plan = BuildPlan::new(
            BuildSource {
                label: "test-source".to_string(),
                revision: None,
                bundle_hash: source_hash.clone(),
                bundle_bytes: 6,
            },
            BuildKeyInput {
                source_hash,
                source_revision: None,
                lockfile_hash: build_content_hash(b"lockfile"),
                target: target.clone(),
                profile: "dev".to_string(),
                features: Vec::new(),
                toolchain_fingerprint: toolchain_fingerprint.clone(),
                outputs: vec![BuildOutputRequest::binary("sinomo", "sinomo")],
            },
        )
        .expect("plan");
        let capabilities = BuildServiceCapabilities {
            node_id: NodeId::from_legacy_named(name).expect("node id"),
            protocol: BUILD_PROTOCOL.to_string(),
            targets: vec![BuildTargetCapability {
                target,
                artifact_kinds: vec![BuildArtifactKind::Binary],
                toolchain_fingerprint,
            }],
            resources: BuildResourceSnapshot {
                cpu_threads,
                memory_total_mb: Some(u64::from(cpu_threads) * 2_048),
                memory_available_mb: Some(u64::from(cpu_threads) * 1_024),
                cpu_load_milli: Some(0),
                active_jobs,
                max_jobs: 1,
                queue_depth,
            },
        };
        ClusterBuilderPlan {
            contact: RolloutTarget::address(format!("{name}:21353")),
            capabilities: BuildCapabilitiesResponse {
                capabilities,
                supports_requested_target: Some(true),
                cache_hit: Some(false),
            },
            plan,
            cache_hit: false,
            transfer_cost: 1,
        }
    }

    #[test]
    fn cluster_rollout_build_plan_uses_release_profile() {
        let target = exact_linux_target();
        let source_hash = build_content_hash(b"source");
        let source = ClusterBuildSourceInput {
            source: BuildSource {
                label: "test-source".to_string(),
                revision: Some("test-revision".to_string()),
                bundle_hash: source_hash,
                bundle_bytes: 6,
            },
            source_repo: PathBuf::from("test-source"),
            source_bundle: PathBuf::from("test-source.bundle"),
            lockfile_hash: build_content_hash(b"lockfile"),
        };
        let capability = BuildTargetCapability {
            target,
            artifact_kinds: vec![BuildArtifactKind::Binary],
            toolchain_fingerprint: "test-toolchain".to_string(),
        };

        let plan = build_plan_for_capability(
            &source,
            &capability,
            &[],
            &[BuildOutputRequest::binary("sinomo", "sinomo")],
        )
        .expect("build plan");

        assert_eq!(plan.input.profile, CLUSTER_ROLLOUT_BUILD_PROFILE);
        assert_eq!(plan.input.profile, "release");
    }

    #[test]
    fn target_grouping_reuses_one_build_for_matching_os_and_arch() {
        let linux = BuildTarget::new("linux", "aarch64").expect("linux target");
        let windows = BuildTarget::new("windows", "x86_64").expect("windows target");
        let groups = group_cluster_build_targets([
            (
                linux.clone(),
                RolloutTarget::address("pi-a:21353".to_string()),
            ),
            (
                windows.clone(),
                RolloutTarget::address("win:21353".to_string()),
            ),
            (
                linux.clone(),
                RolloutTarget::address("pi-b:21353".to_string()),
            ),
        ]);

        assert_eq!(groups.len(), 2);
        let linux_group = groups
            .iter()
            .find(|group| group.target == linux)
            .expect("linux group");
        assert_eq!(
            linux_group
                .rollout_targets
                .iter()
                .map(RolloutTarget::label)
                .collect::<Vec<_>>(),
            ["pi-a:21353", "pi-b:21353"]
        );
        assert!(groups.iter().any(|group| group.target == windows));
    }

    #[test]
    fn cli_ranking_keeps_busy_powerful_server_ahead_of_idle_pi() {
        let requested = BuildTarget::new("linux", "aarch64").expect("requested target");
        let ranked = rank_cluster_builder_plans(
            &requested,
            &[BuildArtifactKind::Binary],
            vec![
                builder_plan("idle-pi", 4, 0, 0),
                builder_plan("busy-server", 64, 1, 1),
            ],
        );

        assert_eq!(ranked[0].contact.label(), "busy-server:21353");
    }

    #[test]
    fn legacy_named_module_selector_matches_canonical_module_id() {
        let module_id = sinomo_runtime::module::ModuleId::from_legacy_named("module.network-iroh")
            .expect("module id");
        let output = BuildOutputRequest::native_dylib_module(
            module_id.to_string(),
            "sinomo-network-iroh-module",
            "sinomo_network_iroh_module",
            NativeDylibArtifactMetadata::new(
                module_id,
                "v1",
                "module/network/iroh",
                "libsinomo_network_iroh_module.so",
                "rollout:modules/network/iroh",
            ),
        );

        assert!(cluster_build_output_matches_selector(
            &output,
            "module.network-iroh"
        ));
        assert!(cluster_build_output_matches_selector(
            &output,
            "sinomo-network-iroh-module"
        ));
    }

    #[test]
    fn target_discovery_failure_blocks_every_group_before_build_submission() {
        let groups = group_cluster_build_targets([(
            BuildTarget::new("linux", "x86_64").expect("target"),
            RolloutTarget::address("healthy:21353".to_string()),
        )]);
        let mut outcomes = ClusterRolloutOutcomeSummary::default();
        outcomes.begin_target();
        outcomes.record_failure(
            "unreachable:21353",
            "failed",
            "status unavailable".to_string(),
        );
        outcomes.begin_target();

        assert!(block_cluster_build_groups_after_discovery_failure(
            &mut outcomes,
            &groups,
            "rollout"
        ));
        assert_eq!(outcomes.failures.len(), 2);
        assert!(
            outcomes.failures[1]
                .contains("no rollout build was submitted and no target was activated")
        );
    }
}

fn rollout_cluster_legacy(options: RolloutOptions) -> Result<(), CliError> {
    validate_cluster_native_module_skip(&options)?;
    let targets = rollout_cluster_targets(&options)?;
    if targets.is_empty() {
        return Err(CliError::Parse {
            detail: "no rollout targets found; pass --target <host:port> or start a clustered local daemon".to_string(),
        });
    }
    let source = options
        .remote_repo
        .as_ref()
        .or(options.repo.as_ref())
        .or(options.binary.as_ref())
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "remote source repo".to_string());
    let requested_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let mut build_request = prepare_cluster_rollout_build_request(
        source.clone(),
        options.remote_repo.clone(),
        options.force_source_overwrite,
        options.host.clone(),
        options.port,
        options.shadow_port,
        requested_at.clone(),
    );
    build_request.host_override = options.host_explicit.then(|| options.host.clone());
    let mut binary_requests = BTreeMap::<(String, String), PreparedClusterBinaryRollout>::new();
    let mut source_bundle: Option<(Vec<u8>, String)> = None;
    let mut desired_source_head = None;
    if options.targets.is_empty() && options.remote_build && options.remote_repo.is_none() {
        let repo = rollout_source_repo_from_options(&options)?;
        desired_source_head = Some(rollout_source_head(&repo)?);
    }
    if options.targets.is_empty()
        && options.remote_build
        && let Err(error) = publish_cluster_rollout_desired(
            &options,
            &source,
            &requested_at,
            desired_source_head.as_deref(),
        )
    {
        eprintln!(
            "warning: cluster rollout desired-state publication failed: {error}; continuing with direct rollout targets"
        );
    }

    let mut completed_rollout_nodes = std::collections::BTreeSet::new();
    let mut outcomes = ClusterRolloutOutcomeSummary::default();
    for target in targets {
        if target
            .rollout_node_id()
            .is_some_and(|node_id| completed_rollout_nodes.contains(node_id))
        {
            continue;
        }
        outcomes.begin_target();
        let preflight = match read_rollout_target_status_before_cluster_activation(&target) {
            Ok(status) => status,
            Err(error) => {
                println!("{}: failed: {error}", target.label());
                outcomes.record_failure(target.label(), "failed", error.to_string());
                return outcomes.finish();
            }
        };
        let RolloutClusterActivationPreflight {
            status: target_status,
            cluster_target,
            authority_target,
        } = preflight;
        let platform_match = rollout_target_platform_matches_local(&target_status);
        let use_remote_build = !options.skip_native_modules
            && daemon_cluster_rollout_should_use_remote_build(
                target.label(),
                options.remote_build,
                options.binary.is_some(),
                platform_match,
            );
        if use_remote_build {
            let mut request = build_request.clone();
            if request.remote_repo.is_none() {
                if source_bundle.is_none() {
                    source_bundle = Some(rollout_source_bundle(&options)?);
                }
                if let Some((bundle, head)) = source_bundle.as_ref() {
                    request.source_bundle = Some(bundle.clone());
                    request.source_head = Some(head.clone());
                }
            }
            let runtime_source_repo = request
                .source_head
                .as_ref()
                .map(|_| rollout_source_repo_from_options(&options))
                .transpose()?;
            let runtime_source = runtime_source_repo
                .as_deref()
                .zip(request.source_head.as_deref())
                .map(|(repo, revision)| RolloutRuntimeSourceIdentity { repo, revision });
            match send_daemon_rollout_build(&target, &request, &cluster_target) {
                Ok(response) => {
                    let source_head_mismatch = response.status == "accepted"
                        && request.source_head.is_some()
                        && !source_heads_match(
                            response.source_head.as_deref(),
                            request.source_head.as_deref().unwrap_or_default(),
                        );
                    if source_head_mismatch {
                        println!("{}: rejected", target.label());
                        let detail = format!(
                            "remote build did not acknowledge requested source head {}; target reported {}",
                            request.source_head.as_deref().unwrap_or("unknown"),
                            response.source_head.as_deref().unwrap_or("unknown")
                        );
                        println!("  {detail}");
                        outcomes.record_failure(target.label(), "rejected", detail);
                    } else {
                        println!("{}: {}", target.label(), response.status);
                        if response.status == "accepted" {
                            if let Some(node_id) = target.rollout_node_id() {
                                completed_rollout_nodes.insert(node_id.clone());
                            }
                        } else {
                            outcomes.record_failure(
                                target.label(),
                                &response.status,
                                response.detail.clone(),
                            );
                        }
                    }
                    if !response.detail.is_empty() {
                        println!("  {}", response.detail);
                    }
                    if let Err(error) = ensure_rollout_target_authority_ready_after_acceptance(
                        &target,
                        authority_target.as_ref(),
                        runtime_source,
                        request.host_override.as_deref(),
                        request.port,
                        &response,
                    ) {
                        println!("{}: authority handoff failed: {error}", target.label());
                        outcomes.record_failure(
                            target.label(),
                            "authority-not-ready",
                            error.to_string(),
                        );
                        return outcomes.finish();
                    }
                    if !source_head_mismatch
                        && let Err(error) = ensure_cluster_rollout_host_convergence_after_acceptance(
                            &target,
                            &cluster_target,
                            authority_target.as_ref(),
                            runtime_source,
                            request.host_override.as_deref(),
                            &response,
                            &request.requested_at,
                            request.port,
                            request.shadow_port,
                            |restart_target, requested_at| {
                                let mut retry = request.clone();
                                retry.requested_at = requested_at;
                                send_daemon_rollout_build(restart_target, &retry, &cluster_target)
                            },
                        )
                    {
                        println!("{}: bind convergence failed: {error}", target.label());
                        outcomes.record_failure(
                            target.label(),
                            "bind-not-converged",
                            error.to_string(),
                        );
                        return outcomes.finish();
                    }
                }
                Err(error) => {
                    println!("{}: failed: {error}", target.label());
                    outcomes.record_failure(target.label(), "failed", error.to_string());
                    if rollout_indeterminate_activation_requires_stop(authority_target.as_ref()) {
                        println!(
                            "{}: activation outcome is indeterminate for a Raft voter; refusing to advance to another cluster authority",
                            target.label()
                        );
                        return outcomes.finish();
                    }
                }
            }
            continue;
        }

        let explicit_binary = options.binary.is_some();
        if !explicit_binary
            && let Err(error) =
                ensure_rollout_target_platform_from_status(target.label(), &target_status)
        {
            println!("{}: skipped: {error}", target.label());
            outcomes.record_failure(target.label(), "skipped", error.to_string());
            continue;
        }
        let Some(target_os) = target_status.target_os.clone() else {
            let detail = "target daemon did not report its operating system".to_string();
            println!("{}: skipped: {detail}", target.label());
            outcomes.record_failure(target.label(), "skipped", detail);
            continue;
        };
        let Some(target_arch) = target_status.target_arch.clone() else {
            let detail = "target daemon did not report its architecture".to_string();
            println!("{}: skipped: {detail}", target.label());
            outcomes.record_failure(target.label(), "skipped", detail);
            continue;
        };
        let platform = (target_os.clone(), target_arch.clone());
        if !binary_requests.contains_key(&platform) {
            let artifacts = rollout_prepare_dev_artifacts(&options)?;
            println!(
                "cluster rollout binary: {}",
                artifacts.built_binary.display()
            );
            for module in &artifacts.native_dylib_modules {
                println!("cluster rollout module: {}", module.built_library.display());
            }
            let mut prepared = prepare_cluster_binary_rollout_for_target(
                source.clone(),
                &artifacts.built_binary,
                &artifacts.native_dylib_modules,
                options.skip_native_modules,
                target_os,
                target_arch,
                options.host.clone(),
                options.port,
                options.shadow_port,
                requested_at.clone(),
            )?;
            prepared.manifest.host_override = options.host_explicit.then(|| options.host.clone());
            binary_requests.insert(platform.clone(), prepared);
        }
        let request = binary_requests
            .get(&platform)
            .expect("binary rollout request");
        match send_prepared_daemon_rollout_binary(&target, request, &cluster_target) {
            Ok(response) => {
                println!("{}: {}", target.label(), response.status);
                if response.status == "accepted" {
                    if let Some(node_id) = target.rollout_node_id() {
                        completed_rollout_nodes.insert(node_id.clone());
                    }
                } else {
                    outcomes.record_failure(
                        target.label(),
                        &response.status,
                        response.detail.clone(),
                    );
                }
                if !response.detail.is_empty() {
                    println!("  {}", response.detail);
                }
                if let Err(error) = ensure_rollout_target_authority_ready_after_acceptance(
                    &target,
                    authority_target.as_ref(),
                    None,
                    request.manifest.host_override.as_deref(),
                    request.manifest.port,
                    &response,
                ) {
                    println!("{}: authority handoff failed: {error}", target.label());
                    outcomes.record_failure(
                        target.label(),
                        "authority-not-ready",
                        error.to_string(),
                    );
                    return outcomes.finish();
                }
                if let Err(error) = ensure_cluster_rollout_host_convergence_after_acceptance(
                    &target,
                    &cluster_target,
                    authority_target.as_ref(),
                    None,
                    request.manifest.host_override.as_deref(),
                    &response,
                    &request.manifest.requested_at,
                    request.manifest.port,
                    request.manifest.shadow_port,
                    |restart_target, requested_at| {
                        let mut retry = request.clone();
                        retry.manifest.requested_at = requested_at;
                        retry.manifest.upload_id = host_convergence_upload_id();
                        send_prepared_daemon_rollout_binary(restart_target, &retry, &cluster_target)
                    },
                ) {
                    println!("{}: bind convergence failed: {error}", target.label());
                    outcomes.record_failure(
                        target.label(),
                        "bind-not-converged",
                        error.to_string(),
                    );
                    return outcomes.finish();
                }
            }
            Err(error) => {
                println!("{}: failed: {error}", target.label());
                outcomes.record_failure(target.label(), "failed", error.to_string());
                if rollout_indeterminate_activation_requires_stop(authority_target.as_ref()) {
                    println!(
                        "{}: activation outcome is indeterminate for a Raft voter; refusing to advance to another cluster authority",
                        target.label()
                    );
                    return outcomes.finish();
                }
            }
        }
    }
    outcomes.finish()
}

#[derive(Default)]
struct ClusterRolloutOutcomeSummary {
    attempted: usize,
    failures: Vec<String>,
}

impl ClusterRolloutOutcomeSummary {
    fn begin_target(&mut self) {
        self.attempted += 1;
    }

    fn has_failures(&self) -> bool {
        !self.failures.is_empty()
    }

    fn record_failure(&mut self, target: &str, outcome: &str, detail: String) {
        let detail = detail.trim();
        if detail.is_empty() {
            self.failures.push(format!("{target}: {outcome}"));
        } else {
            self.failures
                .push(format!("{target}: {outcome} ({detail})"));
        }
    }

    fn finish(self) -> Result<(), CliError> {
        if self.failures.is_empty() {
            return Ok(());
        }

        let target_noun = if self.attempted == 1 {
            "target"
        } else {
            "targets"
        };
        let summary = if self.failures.len() == self.attempted {
            format!(
                "cluster rollout did not succeed on any of {} {target_noun}",
                self.attempted
            )
        } else {
            format!(
                "cluster rollout did not succeed on {} of {} {target_noun}",
                self.failures.len(),
                self.attempted
            )
        };
        Err(CliError::ExternalCommand {
            command: "rollout cluster".to_string(),
            detail: format!("{summary}: {}", self.failures.join("; ")),
        })
    }
}

#[cfg(test)]
mod cluster_rollout_outcome_tests {
    use super::*;

    #[test]
    fn accepted_targets_keep_the_success_result() {
        let mut outcomes = ClusterRolloutOutcomeSummary::default();
        outcomes.begin_target();
        outcomes.begin_target();

        outcomes.finish().expect("accepted rollout must succeed");
    }

    #[test]
    fn partial_target_failure_returns_an_aggregate_error() {
        let mut outcomes = ClusterRolloutOutcomeSummary::default();
        outcomes.begin_target();
        outcomes.begin_target();
        outcomes.record_failure(
            "node-b:21353",
            "rejected",
            "candidate failed validation".to_string(),
        );

        let error = outcomes.finish().expect_err("partial rollout must fail");

        assert!(matches!(error, CliError::ExternalCommand { .. }));
        assert_eq!(
            error.to_string(),
            "command `rollout cluster` failed: cluster rollout did not succeed on 1 of 2 targets: node-b:21353: rejected (candidate failed validation)"
        );
    }

    #[test]
    fn total_target_failure_returns_every_target_in_the_aggregate_error() {
        let mut outcomes = ClusterRolloutOutcomeSummary::default();
        outcomes.begin_target();
        outcomes.record_failure("node-a:21353", "failed", "connection refused".to_string());
        outcomes.begin_target();
        outcomes.record_failure(
            "node-b:21353",
            "skipped",
            "target platform does not match".to_string(),
        );

        let error = outcomes.finish().expect_err("total rollout must fail");

        assert!(matches!(error, CliError::ExternalCommand { .. }));
        assert_eq!(
            error.to_string(),
            "command `rollout cluster` failed: cluster rollout did not succeed on any of 2 targets: node-a:21353: failed (connection refused); node-b:21353: skipped (target platform does not match)"
        );
    }
}

#[cfg(test)]
mod cluster_binary_rollout_tests {
    use super::*;

    #[test]
    fn binary_only_rollout_omits_native_module_manifest_and_upload_sources() {
        let root = sinomo_testdir::TestDir::new("sinomo-cli-binary-only-rollout");
        let binary = root.join("sinomo");
        std::fs::write(&binary, b"binary-only").expect("write binary");
        let missing_module = root.join("module-must-not-be-read.so");
        let modules = vec![LocalRolloutNativeDylibModuleArtifact {
            module_id: "module.test".to_string(),
            interface_version: "test.v1".to_string(),
            cache_key: "module/test".to_string(),
            library_name: "libmodule_test.so".to_string(),
            built_library: missing_module.clone(),
            source: "test".to_string(),
        }];

        let prepared = prepare_cluster_binary_rollout_for_target(
            "test-source".to_string(),
            &binary,
            &modules,
            true,
            "windows".to_string(),
            "x86_64".to_string(),
            "127.0.0.1".to_string(),
            21_353,
            21_354,
            "now".to_string(),
        )
        .expect("prepare binary-only rollout without reading module artifact");

        assert!(prepared.manifest.native_dylib_modules.is_empty());
        assert_eq!(prepared.manifest.target_os, "windows");
        assert_eq!(prepared.manifest.target_arch, "x86_64");
        assert_eq!(prepared.manifest.total_bytes(), Some(11));
        assert_eq!(prepared.artifacts.len(), 1);
        assert_eq!(prepared.artifacts[0].artifact_id, "binary");
        assert_eq!(prepared.artifacts[0].path, binary);
        assert!(
            !prepared
                .artifacts
                .iter()
                .any(|source| source.path == missing_module)
        );
    }
}

fn publish_cluster_rollout_desired(
    options: &RolloutOptions,
    source: &str,
    requested_at: &str,
    source_head: Option<&str>,
) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    let state = read_cluster_state(&paths)?;
    let desired = sinomo_runtime::r0::ClusterRolloutDesiredRecord {
        cluster_id: state.cluster_id,
        state: "active".to_string(),
        source: Some(source.to_string()),
        mode: Some("cluster-remote-build".to_string()),
        source_head: source_head.map(str::to_string),
        source_bundle: None,
        force_source_overwrite: options.force_source_overwrite,
        remote_repo: options.remote_repo.clone(),
        host: Some(options.host.clone()),
        port: Some(options.port),
        shadow_port: Some(options.shadow_port),
        requested_at: Some(requested_at.to_string()),
        updated_at: OffsetDateTime::now_utc(),
    };
    let Some(status) = daemon_default_main_daemon_status(DAEMON_PROBE_TIMEOUT)? else {
        return Err(CliError::DaemonNotRunning {
            detail: "cluster rollout desired state requires the local daemon".to_string(),
        });
    };
    let txn = KvTxn {
        compares: Vec::new(),
        success: vec![sinomo_runtime::rollout::build_cluster_rollout_desired_put_op(&desired)?],
        failure: Vec::new(),
    };
    let txn_request = KvTxnRequest::generated(txn);
    let payload = sinomo_runtime::kv::cluster_kv_txn_rpc_payload(&txn_request)?;
    let request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &request,
        DAEMON_CLUSTER_KV_TXN_TIMEOUT,
    )?;
    response.error_detail()?;
    let commit = sinomo_runtime::kv::decode_cluster_kv_txn_rpc_body(&response.body)?;
    let txn_response = commit.txn_response.ok_or_else(|| CliError::Parse {
        detail: "cluster KV returned a barrier result for a rollout desired transaction"
            .to_string(),
    })?;
    if !txn_response.succeeded {
        return Err(CliError::Parse {
            detail: format!(
                "rollout desired transaction compare failed at revision {:?}",
                txn_response.revision
            ),
        });
    }
    Ok(())
}

fn rollout_source_deploy_options(options: &RolloutOptions) -> ManagedSourceDeployOptions {
    ManagedSourceDeployOptions {
        force_overwrite: options.force_source_overwrite,
    }
}

pub(crate) fn rollout_source_bundle(
    options: &RolloutOptions,
) -> Result<(Vec<u8>, String), CliError> {
    let repo = rollout_source_repo_from_options(options)?;
    let paths = rollout_paths_from_defaults()?;
    prepare_rollout_source_bundle(&paths, &repo).map_err(Into::into)
}

pub(crate) fn rollout_prepare_dev_artifacts(
    options: &RolloutOptions,
) -> Result<sinomo_runtime::rollout::RolloutBuiltArtifacts, CliError> {
    let repo = rollout_source_repo_from_options(options)?;
    let paths = rollout_paths_from_defaults()?;
    if options.skip_native_modules {
        let built_binary = sinomo_runtime::rollout::prepare_rollout_dev_binary(
            &paths,
            &repo,
            options.binary.as_deref(),
        )?;
        return Ok(sinomo_runtime::rollout::RolloutBuiltArtifacts {
            built_binary,
            native_dylib_modules: Vec::new(),
        });
    }
    prepare_rollout_dev_artifacts(&paths, &repo, options.binary.as_deref()).map_err(Into::into)
}

pub(crate) fn rollout_source_repo_from_options(
    options: &RolloutOptions,
) -> Result<PathBuf, CliError> {
    options.repo.clone().map(Ok).unwrap_or_else(|| {
        env::current_dir().map_err(|source| CliError::Io {
            path: ".".to_string(),
            detail: source.to_string(),
        })
    })
}

pub(crate) fn rollout_cluster_targets(
    options: &RolloutOptions,
) -> Result<Vec<RolloutTarget>, CliError> {
    let local_target = local_rollout_target(options)?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = ClusterPaths::new(&data_dir);
    if !options.targets.is_empty() {
        let state = require_cluster_bootstrap_state().ok();
        let Some(state) = state.as_ref() else {
            return Ok(explicit_rollout_targets(options.targets.clone()));
        };
        let explicitly_targets_local = options
            .targets
            .iter()
            .any(|target| target == state.local_node_id.as_str());
        let local_address = if explicitly_targets_local {
            Some(
                default_main_daemon_status()?
                    .map(|status| status.address)
                    .unwrap_or_else(|| daemon_connect_address(&options.host, options.port)),
            )
        } else {
            None
        };
        return Ok(explicit_rollout_targets_from_state(
            options.targets.clone(),
            state,
            local_address.as_deref(),
        ));
    }
    let state = refreshed_rollout_cluster_state_with(require_cluster_bootstrap_state, || {
        refresh_cluster_kv_from_local_daemon(&paths)
    })?;
    if let Some(state) = state.as_ref() {
        return Ok(rollout_cluster_targets_from_state(
            &paths,
            state,
            local_target,
        ));
    }
    let mut targets = Vec::new();
    if let Some(local_target) = local_target {
        push_unique_rollout_target(&mut targets, local_target);
    }
    Ok(targets)
}

fn refreshed_rollout_cluster_state_with<R, F>(
    mut read_state: R,
    mut refresh_authority: F,
) -> Result<Option<ClusterBootstrapState>, CliError>
where
    R: FnMut() -> Result<ClusterBootstrapState, CliError>,
    F: FnMut() -> Result<(), CliError>,
{
    match read_state() {
        Err(CliError::ClusterOrphan) => return Ok(None),
        Err(error) => return Err(error),
        Ok(_) => {}
    }
    refresh_authority().map_err(|error| CliError::ClusterReadOnly {
        detail: format!(
            "cluster rollout target discovery could not refresh authoritative membership; no target was selected or activated: {error}"
        ),
    })?;
    read_state().map(Some).map_err(|error| CliError::ClusterReadOnly {
        detail: format!(
            "cluster rollout target discovery refreshed authority but could not load the resulting membership; no target was selected or activated: {error}"
        ),
    })
}

#[cfg(test)]
mod rollout_target_discovery_tests {
    use super::*;

    #[test]
    fn authoritative_refresh_failure_never_returns_stale_targets() {
        let stale = ClusterBootstrapState::initialized_locally(
            Id::from_legacy_named("cluster-rollout-stale").expect("cluster id"),
            "stale rollout cluster",
            NodeId::from_legacy_named("node-rollout-stale").expect("node id"),
            "stale node",
            "stale-public-key",
            OffsetDateTime::UNIX_EPOCH,
        );
        let reads = Cell::new(0usize);

        let error = refreshed_rollout_cluster_state_with(
            || {
                reads.set(reads.get() + 1);
                Ok(stale.clone())
            },
            || {
                Err(CliError::Network {
                    address: "127.0.0.1:21353".to_string(),
                    detail: "authoritative membership idle timeout after 1500ms".to_string(),
                })
            },
        )
        .expect_err("stale membership must not select a partial rollout target set");

        assert_eq!(
            reads.get(),
            1,
            "stale state must not be returned after failure"
        );
        let detail = error.to_string();
        assert!(detail.contains("target discovery"));
        assert!(detail.contains("no target was selected or activated"));
        assert!(detail.contains("idle timeout after 1500ms"));
    }

    #[test]
    fn unclustered_target_discovery_does_not_request_cluster_authority() {
        let refreshes = Cell::new(0usize);
        let state = refreshed_rollout_cluster_state_with(
            || Err(CliError::ClusterOrphan),
            || {
                refreshes.set(refreshes.get() + 1);
                Ok(())
            },
        )
        .expect("an unclustered local rollout has no cluster targets");

        assert!(state.is_none());
        assert_eq!(refreshes.get(), 0);
    }
}

fn explicit_rollout_targets(addresses: Vec<String>) -> Vec<RolloutTarget> {
    let mut targets = Vec::new();
    for address in addresses {
        push_unique_rollout_target(&mut targets, explicit_rollout_target(address));
    }
    targets
}

pub(crate) fn explicit_rollout_targets_from_state(
    addresses: Vec<String>,
    state: &ClusterBootstrapState,
    local_address: Option<&str>,
) -> Vec<RolloutTarget> {
    let mut targets = Vec::new();
    for address in addresses {
        let target = state
            .members
            .iter()
            .find(|member| {
                member.node_id.as_str() == address
                    && member.state.is_authorized()
                    && member
                        .capabilities
                        .iter()
                        .any(|capability| capability.as_str() == "daemon.main.run")
            })
            .map(|member| {
                rollout_target_for_cluster_member(
                    &member.node_id,
                    &state.local_node_id,
                    local_address,
                )
            })
            .unwrap_or_else(|| explicit_rollout_target(address));
        push_unique_rollout_target(&mut targets, target);
    }
    targets
}

fn rollout_target_for_cluster_member(
    node_id: &NodeId,
    local_node_id: &NodeId,
    local_address: Option<&str>,
) -> RolloutTarget {
    if node_id == local_node_id
        && let Some(local_address) = local_address
    {
        return RolloutTarget::address(local_address.to_string());
    }
    RolloutTarget::routed_node(node_id.clone())
}

fn explicit_rollout_target(address: String) -> RolloutTarget {
    if address.starts_with("iroh://") {
        RolloutTarget::explicit_endpoint(cluster::invite_introducer_endpoint(
            &NodeId::from_legacy_named("node_rollout_explicit_target")
                .expect("static rollout target node id"),
            address,
        ))
    } else {
        RolloutTarget::address(address)
    }
}

pub(crate) fn rollout_cluster_targets_from_state(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_target: Option<RolloutTarget>,
) -> Vec<RolloutTarget> {
    let mut targets = Vec::new();
    let selector = network::ClusterEndpointSelector::new(paths, state);
    let command_endpoints =
        selector.select(network::ClusterEndpointSelectionPurpose::MainDaemonCommand);
    let mut command_endpoints_by_node = std::collections::BTreeMap::new();
    let mut main_node_ids = Vec::new();
    let mut seen_main_node_ids = std::collections::BTreeSet::new();
    for endpoint in command_endpoints {
        if seen_main_node_ids.insert(endpoint.node_id.clone()) {
            main_node_ids.push(endpoint.node_id.clone());
        }
        command_endpoints_by_node
            .entry(endpoint.node_id.clone())
            .or_insert(endpoint);
    }
    for member in state.members.iter().filter(|member| {
        member.node_id != state.local_node_id
            && member.state.is_authorized()
            && member
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == "daemon.main.run")
    }) {
        if seen_main_node_ids.insert(member.node_id.clone()) {
            main_node_ids.push(member.node_id.clone());
        }
    }

    let rollout_endpoints = selector.select(network::ClusterEndpointSelectionPurpose::Rollout);
    for main_node_id in main_node_ids {
        let command_endpoint = command_endpoints_by_node.get(&main_node_id);
        let mut target_endpoints = selector.rollout_targets_for_main_with_command_endpoint(
            &main_node_id,
            command_endpoint,
            &rollout_endpoints,
        );
        if target_endpoints.is_empty()
            && let Some(command_endpoint) = command_endpoint
        {
            target_endpoints.push(command_endpoint.clone());
        }
        if !target_endpoints.is_empty() {
            let rollout_endpoint = target_endpoints.remove(0);
            push_unique_rollout_target(
                &mut targets,
                RolloutTarget::endpoint_for_node(rollout_endpoint, main_node_id.clone())
                    .with_restart_endpoints(target_endpoints),
            );
        }
    }

    // Keep the currently running local daemon available as the rollout
    // coordinator until every discovered remote target has accepted the new
    // version. Activating it earlier can tear down the only healthy submission
    // path while remote uploads are still in flight.
    if let Some(local_target) = local_target {
        push_unique_rollout_target(&mut targets, local_target);
    }

    targets
}

fn local_rollout_target(options: &RolloutOptions) -> Result<Option<RolloutTarget>, CliError> {
    if !options.targets.is_empty() || options.skip_local {
        return Ok(None);
    }
    if let Some(status) = default_main_daemon_status()? {
        return Ok(Some(RolloutTarget::address(status.address)));
    }
    let shadow_paths = shadow_daemon_paths_from_defaults()?;
    if let DaemonProbeStatus::Running(status) =
        probe_daemon_status(&shadow_paths, DAEMON_PROBE_TIMEOUT)
    {
        return Ok(Some(RolloutTarget::address(status.address)));
    }
    Ok(Some(RolloutTarget::address(daemon_connect_address(
        &options.host,
        options.port,
    ))))
}

#[cfg(test)]
pub(crate) fn rollout_target_endpoint_for_main_with_connectivity(
    paths: &ClusterPaths,
    state: &sinomo_runtime::core::ClusterBootstrapState,
    main_node_id: &NodeId,
    candidates: &[ClusterEndpointRecord],
) -> Option<ClusterEndpointRecord> {
    network::ClusterEndpointSelector::new(paths, state)
        .rollout_targets_for_main_with_command_endpoint(main_node_id, None, candidates)
        .into_iter()
        .next()
}

fn push_unique_rollout_target(targets: &mut Vec<RolloutTarget>, target: RolloutTarget) {
    if !targets
        .iter()
        .any(|candidate| candidate.address == target.address)
    {
        targets.push(target);
    }
}

const ROLLOUT_PROGRESS_POLL_INTERVAL: Duration = Duration::from_millis(750);
const ROLLOUT_TERMINAL_STATUS_GRACE: Duration = Duration::from_secs(45);
const ROLLOUT_ACTIVE_STATUS_ERROR_GRACE: Duration = Duration::from_secs(15 * 60);
const ROLLOUT_STATUS_PROBE_ERROR_GRACE: Duration = Duration::from_secs(2);
const ROLLOUT_PROGRESS_BAR_WIDTH: usize = 24;
const ROLLOUT_AUTHORITY_READINESS_TIMEOUT: Duration = Duration::from_secs(60);
const ROLLOUT_AUTHORITY_READINESS_STABLE_WINDOW: Duration = Duration::from_secs(5);
const ROLLOUT_AUTHORITY_READINESS_POLL_INTERVAL: Duration = Duration::from_secs(1);
// Let the retry loop publish its own terminal diagnostic before the emergency
// wall-clock guard detaches a transport worker that ignored its deadlines.
const ROLLOUT_AUTHORITY_READINESS_GUARD_GRACE: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug)]
struct RolloutAuthorityReadinessTiming {
    timeout: Duration,
    stable_window: Duration,
    poll_interval: Duration,
    probe_timeout: Duration,
}

impl RolloutAuthorityReadinessTiming {
    fn production() -> Self {
        Self {
            timeout: ROLLOUT_AUTHORITY_READINESS_TIMEOUT,
            stable_window: ROLLOUT_AUTHORITY_READINESS_STABLE_WINDOW,
            poll_interval: ROLLOUT_AUTHORITY_READINESS_POLL_INTERVAL,
            // One authority attempt is sequential by design: first obtain a
            // current-term quorum proof, then compare the restarted voter's
            // local view against that proof. Give each phase its own budget so
            // a successful coordinator read at its deadline cannot starve the
            // target-local comparison.
            probe_timeout: DAEMON_CLUSTER_KV_TXN_TIMEOUT.saturating_add(DAEMON_JOIN_TIMEOUT),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RolloutProgressTiming {
    pub(crate) poll_interval: Duration,
    pub(crate) terminal_status_grace: Duration,
    pub(crate) active_status_error_grace: Duration,
    pub(crate) status_probe_error_grace: Duration,
}

impl RolloutProgressTiming {
    fn production() -> Self {
        Self {
            poll_interval: ROLLOUT_PROGRESS_POLL_INTERVAL,
            terminal_status_grace: ROLLOUT_TERMINAL_STATUS_GRACE,
            active_status_error_grace: ROLLOUT_ACTIVE_STATUS_ERROR_GRACE,
            status_probe_error_grace: ROLLOUT_STATUS_PROBE_ERROR_GRACE,
        }
    }
}

pub(crate) fn rollout_progress_bar(status: Option<&ClusterNodeRolloutStatus>) -> String {
    let Some(status) = status else {
        return format!("[{}]   -", ".".repeat(ROLLOUT_PROGRESS_BAR_WIDTH));
    };
    let Some(total) = status.progress_total.filter(|total| *total > 0) else {
        return format!("[{}]   -", ".".repeat(ROLLOUT_PROGRESS_BAR_WIDTH));
    };
    let current = status.progress_current.unwrap_or_default().min(total);
    let filled = ((current as usize * ROLLOUT_PROGRESS_BAR_WIDTH) / total as usize)
        .min(ROLLOUT_PROGRESS_BAR_WIDTH);
    let empty = ROLLOUT_PROGRESS_BAR_WIDTH.saturating_sub(filled);
    let percent = ((current * 100) / total).min(100);
    format!(
        "[{}{}] {:>3}%",
        "=".repeat(filled),
        ".".repeat(empty),
        percent
    )
}

pub(crate) fn format_rollout_progress_line(
    label: &str,
    operation: &str,
    status: Option<&ClusterNodeRolloutStatus>,
    elapsed: Duration,
) -> String {
    let elapsed_secs = elapsed.as_secs();
    let Some(status) = status else {
        return format!(
            "{label}: {} {operation} - waiting for status ({elapsed_secs}s)",
            rollout_progress_bar(None)
        );
    };
    let phase = status
        .phase
        .as_deref()
        .filter(|phase| !phase.is_empty())
        .unwrap_or(status.status.as_str());
    let message = status
        .progress_message
        .as_deref()
        .or(status.detail.as_deref())
        .filter(|message| !message.is_empty())
        .unwrap_or(operation);
    format!(
        "{label}: {} {phase} - {message} ({elapsed_secs}s)",
        rollout_progress_bar(Some(status))
    )
}

fn write_rollout_progress_line(line: &str, last_len: &mut usize) -> Result<(), CliError> {
    let padding = last_len.saturating_sub(line.len());
    let mut output = format!("\r{line}");
    if padding > 0 {
        output.push_str(&" ".repeat(padding));
    }
    write_cli_stdout_bytes(output.as_bytes()).map_err(CliError::stdio)?;
    *last_len = line.len();
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RolloutProgressExpectation {
    pub(crate) source: String,
    pub(crate) requested_at: String,
    pub(crate) source_head: Option<String>,
}

impl RolloutProgressExpectation {
    fn matches(&self, status: &ClusterNodeRolloutStatus) -> bool {
        status.source.as_deref() == Some(self.source.as_str())
            && status.requested_at.as_deref() == Some(self.requested_at.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RolloutRestartStatusCandidate {
    target: RolloutTarget,
    expected_process: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RolloutExpectedRestartEvidence {
    expected: RolloutAuthorityTarget,
    candidates: Vec<RolloutRestartStatusCandidate>,
}

fn rollout_direct_host_from_address(address: &str) -> Option<String> {
    if address.contains("://") {
        return None;
    }
    if let Ok(address) = address.parse::<std::net::SocketAddr>() {
        return Some(address.ip().to_string());
    }
    let (host, port) = address.rsplit_once(':')?;
    port.parse::<u16>().ok()?;
    let host = host.trim().trim_start_matches('[').trim_end_matches(']');
    (!host.is_empty() && !matches!(host, "0.0.0.0" | "::")).then(|| host.to_string())
}

fn rollout_direct_host_from_target(target: &RolloutTarget) -> Option<String> {
    target
        .endpoint
        .as_ref()
        .and_then(|endpoint| rollout_direct_host_from_address(&endpoint.address))
        .or_else(|| rollout_direct_host_from_address(&target.address))
}

fn rollout_restart_endpoint_process(
    endpoint: &ClusterEndpointRecord,
    expected: &RolloutAuthorityTarget,
) -> Option<&'static str> {
    if endpoint.node_id == shadow_daemon_node_id(&expected.node_id)
        || (endpoint.node_id == expected.node_id
            && network::cluster_endpoint_identifies_shadow_daemon_for_node(endpoint))
    {
        Some(sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE)
    } else if endpoint.node_id == expected.node_id
        && network::cluster_endpoint_can_carry_main_daemon_rpc(endpoint)
    {
        Some(DAEMON_MAIN_CLUSTER_ROLE)
    } else {
        None
    }
}

fn push_unique_restart_status_candidate(
    candidates: &mut Vec<RolloutRestartStatusCandidate>,
    candidate: RolloutRestartStatusCandidate,
) {
    if !candidates.iter().any(|existing| {
        existing.expected_process == candidate.expected_process
            && existing.target.address == candidate.target.address
    }) {
        candidates.push(candidate);
    }
}

fn rollout_restart_endpoints_from_state(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    expected: &RolloutAuthorityTarget,
) -> Vec<ClusterEndpointRecord> {
    let selector = network::ClusterEndpointSelector::new(paths, state);
    let candidates = selector.select(network::ClusterEndpointSelectionPurpose::Rollout);
    selector.rollout_targets_for_main_with_command_endpoint(&expected.node_id, None, &candidates)
}

fn rollout_restart_endpoints_for_authority(
    expected: &RolloutAuthorityTarget,
) -> Vec<ClusterEndpointRecord> {
    let Ok(paths) = cluster_paths_from_defaults() else {
        return Vec::new();
    };
    let Ok(state) = cluster::read_cluster_state_without_kv(&paths) else {
        return Vec::new();
    };
    if state.cluster_id != expected.cluster_id {
        return Vec::new();
    }
    rollout_restart_endpoints_from_state(&paths, &state, expected)
}

fn rollout_expected_restart_evidence(
    target: &RolloutTarget,
    expected: &RolloutAuthorityTarget,
    host_override: Option<&str>,
    main_port: u16,
    shadow_port: u16,
) -> RolloutExpectedRestartEvidence {
    let discovered_endpoints = rollout_restart_endpoints_for_authority(expected);
    rollout_expected_restart_evidence_with_endpoints(
        target,
        expected,
        host_override,
        main_port,
        shadow_port,
        &discovered_endpoints,
    )
}

fn rollout_expected_restart_evidence_with_endpoints(
    target: &RolloutTarget,
    expected: &RolloutAuthorityTarget,
    host_override: Option<&str>,
    main_port: u16,
    shadow_port: u16,
    discovered_endpoints: &[ClusterEndpointRecord],
) -> RolloutExpectedRestartEvidence {
    let direct_host = host_override
        .filter(|host| !matches!(*host, "0.0.0.0" | "::"))
        .map(str::to_string)
        .or_else(|| rollout_direct_host_from_target(target));
    let mut candidates = Vec::new();

    // Preserve authenticated, advertised shadow routes before attempting a
    // host/port guess. In particular, an Iroh or relay endpoint need not share
    // either the main daemon's hostname or the requested TCP shadow port.
    for endpoint in target
        .endpoint
        .iter()
        .chain(target.restart_endpoints.iter())
        .chain(discovered_endpoints.iter())
        .filter(|endpoint| {
            rollout_restart_endpoint_process(endpoint, expected)
                == Some(sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE)
        })
    {
        push_unique_restart_status_candidate(
            &mut candidates,
            RolloutRestartStatusCandidate {
                target: RolloutTarget::explicit_endpoint(endpoint.clone()),
                expected_process: sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE,
            },
        );
    }
    if let Some(host) = direct_host.as_deref() {
        push_unique_restart_status_candidate(
            &mut candidates,
            RolloutRestartStatusCandidate {
                target: RolloutTarget::address(rollout_restart_direct_address(host, shadow_port)),
                expected_process: sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE,
            },
        );
    }

    for endpoint in target
        .endpoint
        .iter()
        .chain(target.restart_endpoints.iter())
        .chain(discovered_endpoints.iter())
        .filter(|endpoint| {
            rollout_restart_endpoint_process(endpoint, expected) == Some(DAEMON_MAIN_CLUSTER_ROLE)
        })
    {
        push_unique_restart_status_candidate(
            &mut candidates,
            RolloutRestartStatusCandidate {
                target: RolloutTarget::explicit_endpoint(endpoint.clone()),
                expected_process: DAEMON_MAIN_CLUSTER_ROLE,
            },
        );
    }
    if let Some(host) = direct_host.as_deref() {
        push_unique_restart_status_candidate(
            &mut candidates,
            RolloutRestartStatusCandidate {
                target: RolloutTarget::address(rollout_restart_direct_address(host, main_port)),
                expected_process: DAEMON_MAIN_CLUSTER_ROLE,
            },
        );
    }
    if target.endpoint.is_none() && target.rollout_node_id().is_none() {
        push_unique_restart_status_candidate(
            &mut candidates,
            RolloutRestartStatusCandidate {
                target: target.clone(),
                expected_process: DAEMON_MAIN_CLUSTER_ROLE,
            },
        );
    }
    push_unique_restart_status_candidate(
        &mut candidates,
        RolloutRestartStatusCandidate {
            target: RolloutTarget::routed_node(expected.node_id.clone()),
            expected_process: DAEMON_MAIN_CLUSTER_ROLE,
        },
    );
    RolloutExpectedRestartEvidence {
        expected: expected.clone(),
        candidates,
    }
}

fn rollout_status_is_expected_restart_evidence(
    status: &ClusterNodeRolloutStatus,
    expectation: &RolloutProgressExpectation,
) -> bool {
    expectation.matches(status)
        && (status.status == "main-staged"
            || status.phase.as_deref() == Some("main-staged")
            || rollout_terminal_response(status, expectation).is_some())
}

impl RolloutExpectedRestartEvidence {
    fn read_with<R>(
        &self,
        expectation: &RolloutProgressExpectation,
        budget: Duration,
        mut read_status: R,
    ) -> Result<Option<ClusterNodeRolloutStatus>, CliError>
    where
        R: FnMut(&RolloutTarget, Duration) -> Result<DaemonStatus, CliError>,
    {
        let started = Instant::now();
        self.read_with_elapsed(expectation, budget, || started.elapsed(), &mut read_status)
    }

    fn read_with_elapsed<R, E>(
        &self,
        expectation: &RolloutProgressExpectation,
        budget: Duration,
        mut elapsed: E,
        mut read_status: R,
    ) -> Result<Option<ClusterNodeRolloutStatus>, CliError>
    where
        R: FnMut(&RolloutTarget, Duration) -> Result<DaemonStatus, CliError>,
        E: FnMut() -> Duration,
    {
        let mut failures = Vec::new();
        for candidate in &self.candidates {
            let remaining = budget.saturating_sub(elapsed());
            if remaining.is_zero() {
                failures.push("restart-evidence probe deadline exhausted".to_string());
                break;
            }
            match read_status(&candidate.target, remaining) {
                Ok(status) => {
                    let identity_matches = status.cluster.as_ref().is_some_and(|cluster| {
                        cluster.cluster_id == self.expected.cluster_id
                            && cluster.node_id == self.expected.node_id
                            && cluster.process == candidate.expected_process
                    });
                    if !identity_matches {
                        failures.push(format!(
                            "{} did not report expected {}/{} process {}",
                            candidate.target.label(),
                            self.expected.cluster_id,
                            self.expected.node_id,
                            candidate.expected_process
                        ));
                        continue;
                    }
                    let Some(rollout) = status.rollout else {
                        failures.push(format!(
                            "{} reported no rollout progress",
                            candidate.target.label()
                        ));
                        continue;
                    };
                    if rollout_status_is_expected_restart_evidence(&rollout, expectation) {
                        return Ok(Some(rollout));
                    }
                    failures.push(format!(
                        "{} reported rollout {}/{} for a different or pre-restart activation",
                        candidate.target.label(),
                        rollout.status,
                        rollout.phase.as_deref().unwrap_or("unknown")
                    ));
                }
                Err(error) => failures.push(format!("{}: {error}", candidate.target.label())),
            }
        }
        Err(CliError::Network {
            address: self.expected.node_id.to_string(),
            detail: format!(
                "no authoritative shadow or replacement process reported the expected restart: {}",
                failures.join("; ")
            ),
        })
    }
}

pub(crate) fn rollout_status_matches_expectation(
    status: &ClusterNodeRolloutStatus,
    expectation: &RolloutProgressExpectation,
) -> bool {
    expectation.matches(status)
}

pub(crate) fn rollout_terminal_response(
    status: &ClusterNodeRolloutStatus,
    expectation: &RolloutProgressExpectation,
) -> Option<ClusterRolloutBinaryResponse> {
    if !expectation.matches(status) {
        return None;
    }
    let terminal = status.phase.as_deref() == Some("shadow-finalized")
        || matches!(
            status.status.as_str(),
            "failed" | "rejected" | "rolled-back"
        );
    if !terminal {
        return None;
    }
    Some(ClusterRolloutBinaryResponse {
        status: status.status.clone(),
        detail: status
            .detail
            .clone()
            .or_else(|| status.progress_message.clone())
            .unwrap_or_else(|| {
                status
                    .phase
                    .clone()
                    .unwrap_or_else(|| status.status.clone())
            }),
        main_binary: PathBuf::new(),
        shadow_binary: PathBuf::new(),
        source_head: expectation.source_head.clone(),
    })
}

#[cfg(test)]
pub(crate) fn rollout_pending_error_grace(
    status: Option<&ClusterNodeRolloutStatus>,
    expectation: &RolloutProgressExpectation,
) -> Duration {
    rollout_pending_error_grace_with_timing(
        status,
        expectation,
        RolloutProgressTiming::production(),
    )
}

fn rollout_pending_error_grace_with_timing(
    status: Option<&ClusterNodeRolloutStatus>,
    expectation: &RolloutProgressExpectation,
    timing: RolloutProgressTiming,
) -> Duration {
    let Some(status) = status else {
        return timing.terminal_status_grace;
    };
    if !expectation.matches(status) || rollout_terminal_response(status, expectation).is_some() {
        return timing.terminal_status_grace;
    }
    timing.active_status_error_grace
}

#[cfg(test)]
pub(crate) fn rollout_missing_status_grace(has_seen_matching_status: bool) -> Duration {
    rollout_missing_status_grace_with_timing(
        has_seen_matching_status,
        RolloutProgressTiming::production(),
    )
}

fn rollout_missing_status_grace_with_timing(
    _has_seen_matching_status: bool,
    timing: RolloutProgressTiming,
) -> Duration {
    timing.active_status_error_grace
}

fn send_rollout_request_with_progress<F>(
    target: &RolloutTarget,
    operation: &'static str,
    expectation: RolloutProgressExpectation,
    restart_evidence: RolloutExpectedRestartEvidence,
    send: F,
) -> Result<ClusterRolloutBinaryResponse, CliError>
where
    F: FnOnce(RolloutTarget) -> Result<ClusterRolloutBinaryResponse, CliError> + Send + 'static,
{
    send_rollout_request_with_progress_inner(
        target,
        operation,
        expectation,
        send,
        |target| read_rollout_target_status(target).map(|status| status.rollout),
        move |expectation, budget| {
            restart_evidence.read_with(expectation, budget, read_rollout_target_status_with_timeout)
        },
        RolloutProgressTiming::production(),
        true,
    )
}

fn wait_for_accepted_rollout_response_with_progress(
    target: &RolloutTarget,
    operation: &'static str,
    expectation: RolloutProgressExpectation,
    restart_evidence: RolloutExpectedRestartEvidence,
    response: ClusterRolloutBinaryResponse,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    debug_assert_eq!(response.status, "accepted");
    // A receipt from an earlier identical artifact upload can report the
    // activation response before the persisted handoff reaches finalization.
    // Feed that result into the same observer without issuing another mutation.
    send_rollout_request_with_progress(
        target,
        operation,
        expectation,
        restart_evidence,
        move |_| Ok(response),
    )
}

fn send_rollout_request_with_progress_inner<F, R, H>(
    target: &RolloutTarget,
    operation: &'static str,
    expectation: RolloutProgressExpectation,
    send: F,
    mut read_status: R,
    mut read_restart_status: H,
    timing: RolloutProgressTiming,
    emit_progress: bool,
) -> Result<ClusterRolloutBinaryResponse, CliError>
where
    F: FnOnce(RolloutTarget) -> Result<ClusterRolloutBinaryResponse, CliError> + Send + 'static,
    R: FnMut(&RolloutTarget) -> Result<Option<ClusterNodeRolloutStatus>, CliError>,
    H: FnMut(
        &RolloutProgressExpectation,
        Duration,
    ) -> Result<Option<ClusterNodeRolloutStatus>, CliError>,
{
    let target_for_send = target.clone();
    let target_for_poll = target.clone();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let _ = result_tx.send(send(target_for_send));
    });

    let started = Instant::now();
    let mut last_len = 0usize;
    let mut drew_progress = false;
    let mut pending_error: Option<CliError> = None;
    let mut pending_error_started_at: Option<Instant> = None;
    let mut worker_finished = false;
    let mut accepted_worker_response: Option<ClusterRolloutBinaryResponse> = None;
    let mut has_seen_matching_status = false;
    let mut missing_status_started_at: Option<Instant> = None;
    let mut last_matching_status: Option<ClusterNodeRolloutStatus> = None;
    let mut active_status_error_started_at: Option<Instant> = None;
    loop {
        if !worker_finished && pending_error.is_none() {
            match result_rx.recv_timeout(timing.poll_interval) {
                Ok(Ok(response)) => {
                    worker_finished = true;
                    if response.status == "accepted" {
                        // The activation RPC can acknowledge after the new main
                        // daemon is staged while its shadow is still restarting.
                        // Keep the single request result, but do not advance to
                        // another target until matching durable progress proves
                        // that the handoff reached shadow-finalized.
                        accepted_worker_response = Some(response);
                    } else {
                        if emit_progress && drew_progress {
                            println!();
                        }
                        return Ok(response);
                    }
                }
                Ok(Err(error)) => {
                    worker_finished = true;
                    pending_error = Some(error);
                    pending_error_started_at = Some(Instant::now());
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    worker_finished = true;
                    pending_error = Some(CliError::Network {
                        address: target.label().to_string(),
                        detail: format!("{operation} worker stopped before returning a result"),
                    });
                    pending_error_started_at = Some(Instant::now());
                }
            }
        } else {
            thread::sleep(timing.poll_interval);
        }

        let mut status_probe_error_detail = None;
        let rollout_status = match read_status(&target_for_poll) {
            Ok(status) => {
                let status = status
                    .filter(|status| rollout_status_matches_expectation(status, &expectation));
                if let Some(status) = status.as_ref() {
                    active_status_error_started_at = None;
                    has_seen_matching_status = true;
                    missing_status_started_at = None;
                    last_matching_status = Some(status.clone());
                } else {
                    let missing_started_at =
                        *missing_status_started_at.get_or_insert_with(Instant::now);
                    if missing_started_at.elapsed()
                        >= rollout_missing_status_grace_with_timing(
                            has_seen_matching_status,
                            timing,
                        )
                    {
                        if emit_progress && drew_progress {
                            println!();
                        }
                        return Err(CliError::Network {
                            address: target.label().to_string(),
                            detail: if has_seen_matching_status {
                                format!(
                                    "{operation} progress disappeared before the rollout request returned; run `sinomo rollout diagnostics --target {}`",
                                    target.label()
                                )
                            } else {
                                format!(
                                    "{operation} did not publish progress before timeout; run `sinomo rollout diagnostics --target {}`",
                                    target.label()
                                )
                            },
                        });
                    }
                }
                status
            }
            Err(error) => {
                let last_status = last_matching_status
                    .as_ref()
                    .filter(|status| rollout_terminal_response(status, &expectation).is_none())
                    .cloned();
                let mut effective_error = error;
                let error_started_at =
                    *active_status_error_started_at.get_or_insert_with(Instant::now);
                // The main process can disappear between any two durable
                // progress writes, including after the last primary poll saw
                // a pre-restart phase. Probe the supervisor/replacement on
                // every primary read failure: that path still requires exact
                // process identity, source/request identity, and a durable
                // main-staged or terminal phase before it can advance us.
                let expected_restart = last_status.as_ref().is_none_or(|status| {
                    rollout_status_matches_expectation(status, &expectation)
                        && rollout_terminal_response(status, &expectation).is_none()
                });
                // `status_probe_error_grace` bounds one network attempt. It is
                // not the total replacement-process startup deadline: a
                // main-only rollout may need to bootstrap its shadow, stop the
                // old main, and then publish the replacement main before any
                // remotely reachable restart witness exists. Once exact
                // matching nonterminal progress has been observed, retain it
                // for the existing active handoff grace and make a fresh,
                // bounded authoritative-evidence attempt on every poll.
                let restart_status = if expected_restart
                    && !timing.status_probe_error_grace.is_zero()
                {
                    match read_restart_status(&expectation, timing.status_probe_error_grace) {
                        Ok(Some(status))
                            if rollout_status_is_expected_restart_evidence(
                                &status,
                                &expectation,
                            ) =>
                        {
                            Some(status)
                        }
                        Ok(Some(status)) => {
                            effective_error = CliError::Network {
                                address: target.label().to_string(),
                                detail: format!(
                                    "{effective_error}; expected-restart evidence reported non-authoritative rollout {}/{}",
                                    status.status,
                                    status.phase.as_deref().unwrap_or("unknown")
                                ),
                            };
                            None
                        }
                        Ok(None) => {
                            effective_error = CliError::Network {
                                address: target.label().to_string(),
                                detail: format!(
                                    "{effective_error}; no authoritative shadow or replacement restart evidence was available"
                                ),
                            };
                            None
                        }
                        Err(restart_error) => {
                            effective_error = CliError::Network {
                                address: target.label().to_string(),
                                detail: format!(
                                    "{effective_error}; expected-restart evidence probe failed: {restart_error}"
                                ),
                            };
                            None
                        }
                    }
                } else {
                    None
                };
                if let Some(status) = restart_status {
                    if error_started_at.elapsed() >= timing.active_status_error_grace
                        && rollout_terminal_response(&status, &expectation).is_none()
                    {
                        if emit_progress && drew_progress {
                            println!();
                        }
                        return Err(rollout_status_probe_error(
                            target,
                            operation,
                            &effective_error,
                            Some(&status),
                        ));
                    }
                    has_seen_matching_status = true;
                    missing_status_started_at = None;
                    last_matching_status = Some(status.clone());
                    Some(status)
                } else {
                    let Some(last_status) = last_status else {
                        if emit_progress && drew_progress {
                            println!();
                        }
                        return Err(rollout_status_probe_error(
                            target,
                            operation,
                            &effective_error,
                            None,
                        ));
                    };
                    if error_started_at.elapsed() >= timing.active_status_error_grace {
                        if emit_progress && drew_progress {
                            println!();
                        }
                        return Err(rollout_status_probe_error(
                            target,
                            operation,
                            &effective_error,
                            Some(&last_status),
                        ));
                    }
                    status_probe_error_detail = Some(effective_error.to_string());
                    Some(last_status)
                }
            }
        };
        if emit_progress {
            let mut line = format_rollout_progress_line(
                target.label(),
                operation,
                rollout_status.as_ref(),
                started.elapsed(),
            );
            if let Some(error) = status_probe_error_detail.as_deref() {
                line.push_str(&format!(
                    "; status probe failed ({error}); retrying for at most {}ms because matching active progress was observed (each restart-evidence probe is bounded to {}ms)",
                    timing.active_status_error_grace.as_millis(),
                    timing.status_probe_error_grace.as_millis()
                ));
            }
            write_rollout_progress_line(&line, &mut last_len)?;
            drew_progress = true;
        }
        if let Some(mut response) = rollout_status
            .as_ref()
            .and_then(|status| rollout_terminal_response(status, &expectation))
        {
            if response.status == "accepted"
                && let Some(accepted) = accepted_worker_response.take()
            {
                // Durable status owns the terminal outcome and detail; retain
                // fields that are available only in the RPC response so source
                // acknowledgement checks keep their existing semantics.
                response.main_binary = accepted.main_binary;
                response.shadow_binary = accepted.shadow_binary;
                response.source_head = accepted.source_head;
            }
            if emit_progress {
                println!();
            }
            return Ok(response);
        }
        let pending_error_grace = if rollout_status.is_none() && has_seen_matching_status {
            timing.active_status_error_grace
        } else {
            rollout_pending_error_grace_with_timing(rollout_status.as_ref(), &expectation, timing)
        };
        if pending_error_started_at
            .is_some_and(|started_at| started_at.elapsed() >= pending_error_grace)
        {
            if let Some(error) = pending_error {
                if emit_progress && drew_progress {
                    println!();
                }
                return Err(error);
            }
        }
    }
}

fn rollout_status_probe_error(
    target: &RolloutTarget,
    operation: &str,
    error: &CliError,
    last_status: Option<&ClusterNodeRolloutStatus>,
) -> CliError {
    let evidence = last_status.map_or_else(
        || "no matching active rollout progress was observed".to_string(),
        |status| {
            format!(
                "last matching progress was phase {:?} at {}/{}",
                status.phase.as_deref().unwrap_or(status.status.as_str()),
                status.progress_current.unwrap_or_default(),
                status.progress_total.unwrap_or_default()
            )
        },
    );
    CliError::Network {
        address: target.label().to_string(),
        detail: format!(
            "{operation} status probe failed; {evidence}; refusing to hide the probe failure while the activation outcome is unresolved: {error}; run `sinomo rollout diagnostics --target {}`",
            target.label()
        ),
    }
}

#[cfg(test)]
pub(crate) fn send_rollout_request_with_progress_for_test<F, R>(
    target: &RolloutTarget,
    operation: &'static str,
    expectation: RolloutProgressExpectation,
    send: F,
    read_status: R,
    timing: RolloutProgressTiming,
) -> Result<ClusterRolloutBinaryResponse, CliError>
where
    F: FnOnce(RolloutTarget) -> Result<ClusterRolloutBinaryResponse, CliError> + Send + 'static,
    R: FnMut(&RolloutTarget) -> Result<Option<ClusterNodeRolloutStatus>, CliError>,
{
    send_rollout_request_with_progress_inner(
        target,
        operation,
        expectation,
        send,
        read_status,
        |_, _| Ok(None),
        timing,
        false,
    )
}

#[cfg(test)]
pub(crate) fn send_rollout_request_with_restart_progress_for_test<F, R, H>(
    target: &RolloutTarget,
    operation: &'static str,
    expectation: RolloutProgressExpectation,
    send: F,
    read_status: R,
    read_restart_status: H,
    timing: RolloutProgressTiming,
) -> Result<ClusterRolloutBinaryResponse, CliError>
where
    F: FnOnce(RolloutTarget) -> Result<ClusterRolloutBinaryResponse, CliError> + Send + 'static,
    R: FnMut(&RolloutTarget) -> Result<Option<ClusterNodeRolloutStatus>, CliError>,
    H: FnMut(
        &RolloutProgressExpectation,
        Duration,
    ) -> Result<Option<ClusterNodeRolloutStatus>, CliError>,
{
    send_rollout_request_with_progress_inner(
        target,
        operation,
        expectation,
        send,
        read_status,
        read_restart_status,
        timing,
        false,
    )
}

pub(crate) fn read_rollout_target_status(target: &RolloutTarget) -> Result<DaemonStatus, CliError> {
    read_rollout_target_status_with_timeout(target, DAEMON_JOIN_TIMEOUT)
}

fn read_rollout_target_status_with_timeout(
    target: &RolloutTarget,
    timeout: Duration,
) -> Result<DaemonStatus, CliError> {
    send_rollout_object_rpc_value(
        target,
        sinomo_runtime::daemon::DAEMON_STATUS_ROUTE,
        sinomo_runtime::daemon::DAEMON_RPC_CATEGORY,
        &(),
        timeout,
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RolloutListenerHosts {
    main: String,
    shadow: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolvedRolloutListenerHosts {
    hosts: RolloutListenerHosts,
    main_target: RolloutTarget,
    shadow_finalization_proven: bool,
}

fn rollout_restart_direct_address(host: &str, port: u16) -> String {
    let connect_host = match host {
        "0.0.0.0" => SINOMO_DAEMON_DEFAULT_HOST,
        "::" => "::1",
        host => host,
    };
    connect_host
        .parse::<std::net::IpAddr>()
        .map(|ip| std::net::SocketAddr::new(ip, port).to_string())
        .unwrap_or_else(|_| format!("{connect_host}:{port}"))
}

fn rollout_restart_direct_probe_address(host: &str, port: u16) -> Option<String> {
    // A wildcard is a bind address, not a remotely dialable listener. Mapping
    // it to loopback is valid only inside the restarted node; from a rollout
    // controller it would probe the controller's own daemon instead.
    (!matches!(host, "0.0.0.0" | "::")).then(|| rollout_restart_direct_address(host, port))
}

fn rollout_restart_original_direct_target(
    original_target: &RolloutTarget,
    explicit_host: &str,
) -> Option<RolloutTarget> {
    if !matches!(explicit_host, "0.0.0.0" | "::") {
        return None;
    }
    if let Some(endpoint) = &original_target.endpoint {
        // The endpoint already passed the preflight and first activation. Drop
        // only its node-id routing preference so a transiently unpublished
        // route cannot hide the still-reachable, authenticated endpoint.
        return Some(RolloutTarget::explicit_endpoint(endpoint.clone()));
    }
    original_target
        .rollout_node_id()
        .is_none()
        .then(|| original_target.clone())
}

fn rollout_status_proves_shadow_finalization(status: &DaemonStatus) -> bool {
    status.rollout.as_ref().is_some_and(|rollout| {
        rollout.status == "accepted" && rollout.phase.as_deref() == Some("shadow-finalized")
    })
}

fn read_restarted_rollout_process_status_with<R>(
    cluster_target: &RolloutAuthorityTarget,
    expected_process: &str,
    candidates: Vec<RolloutTarget>,
    mut read_status: R,
) -> Result<(DaemonStatus, RolloutTarget), CliError>
where
    R: FnMut(&RolloutTarget) -> Result<DaemonStatus, CliError>,
{
    let mut failures = Vec::new();
    for candidate in candidates {
        match read_status(&candidate) {
            Ok(status) => {
                let identity_matches = status.cluster.as_ref().is_some_and(|cluster| {
                    cluster.cluster_id == cluster_target.cluster_id
                        && cluster.node_id == cluster_target.node_id
                        && cluster.process == expected_process
                });
                if identity_matches {
                    return Ok((status, candidate));
                }
                failures.push(format!(
                    "{} reported an unexpected cluster/process identity",
                    candidate.label()
                ));
            }
            Err(error) => failures.push(format!("{}: {error}", candidate.label())),
        }
    }
    Err(CliError::Network {
        address: cluster_target.node_id.to_string(),
        detail: format!(
            "could not resolve the restarted {expected_process} using a safe post-restart target: {}",
            if failures.is_empty() {
                "no remotely dialable candidate was available".to_string()
            } else {
                failures.join("; ")
            }
        ),
    })
}

fn read_rollout_listener_hosts_after_restart_with<R>(
    cluster_target: &RolloutAuthorityTarget,
    original_target: &RolloutTarget,
    explicit_host: &str,
    main_port: u16,
    shadow_port: u16,
    mut read_status: R,
) -> Result<ResolvedRolloutListenerHosts, CliError>
where
    R: FnMut(&RolloutTarget) -> Result<DaemonStatus, CliError>,
{
    // For wildcard binds, preserve the concrete target that successfully
    // accepted the first activation. Node-id routing may temporarily disappear
    // while the new main republishes connectivity, whereas that endpoint can
    // already be serving. A concrete host override deliberately excludes the
    // old listener because it may be stale after the requested bind change.
    // Every candidate is checked against the exact cluster/node/process
    // identity before it can drive the compatibility reactivation.
    let mut main_candidates = Vec::new();
    if let Some(original_direct) =
        rollout_restart_original_direct_target(original_target, explicit_host)
    {
        push_unique_rollout_target(&mut main_candidates, original_direct);
    }
    push_unique_rollout_target(
        &mut main_candidates,
        RolloutTarget::routed_node(cluster_target.node_id.clone()),
    );
    if let Some(direct_address) = rollout_restart_direct_probe_address(explicit_host, main_port) {
        push_unique_rollout_target(&mut main_candidates, RolloutTarget::address(direct_address));
    }
    let (main, main_target) = read_restarted_rollout_process_status_with(
        cluster_target,
        DAEMON_MAIN_CLUSTER_ROLE,
        main_candidates,
        &mut read_status,
    )?;
    let shadow_finalization_proven = rollout_status_proves_shadow_finalization(&main);
    // Routed rollout targets are main-daemon-affine. The shadow shares the
    // authority identity and is not a separate cluster member, so routing a
    // derived shadow id through that target can only select the wrong process
    // or fail. Treat the shadow listener as optional telemetry and probe it
    // directly only when the operator supplied a concrete, remotely dialable
    // host. Durable accepted/shadow-finalized status remains the node-local
    // proof used for wildcard and firewalled supervision listeners.
    let shadow = rollout_restart_direct_probe_address(explicit_host, shadow_port)
        .and_then(|direct_address| {
            read_restarted_rollout_process_status_with(
                cluster_target,
                sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE,
                vec![RolloutTarget::address(direct_address)],
                &mut read_status,
            )
            .ok()
        })
        .map(|(status, _)| status.host);
    Ok(ResolvedRolloutListenerHosts {
        hosts: RolloutListenerHosts {
            main: main.host,
            shadow,
        },
        main_target,
        shadow_finalization_proven,
    })
}

fn read_rollout_listener_hosts_after_restart(
    cluster_target: &RolloutAuthorityTarget,
    original_target: &RolloutTarget,
    explicit_host: &str,
    main_port: u16,
    shadow_port: u16,
) -> Result<ResolvedRolloutListenerHosts, CliError> {
    read_rollout_listener_hosts_after_restart_with(
        cluster_target,
        original_target,
        explicit_host,
        main_port,
        shadow_port,
        read_rollout_target_status,
    )
}

struct RolloutClusterActivationPreflight {
    status: DaemonStatus,
    cluster_target: RolloutAuthorityTarget,
    authority_target: Option<RolloutAuthorityTarget>,
}

fn read_rollout_target_status_before_cluster_activation(
    target: &RolloutTarget,
) -> Result<RolloutClusterActivationPreflight, CliError> {
    let status = read_rollout_target_status(target)?;
    let identity = rollout_cluster_target_from_status(&status).ok_or_else(|| CliError::Network {
        address: target.label().to_string(),
        detail: format!(
            "cluster rollout preflight did not reach {DAEMON_MAIN_CLUSTER_ROLE} with a clustered identity; refusing an activation with unknown voter state"
        ),
    })?;
    if let Some(expected_node_id) = target
        .rollout_node_id()
        .or_else(|| target.endpoint.as_ref().map(|endpoint| &endpoint.node_id))
        && expected_node_id != &identity.node_id
    {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "cluster rollout target identity mismatch: routing selected {expected_node_id}, but the main daemon reported {}",
                identity.node_id
            ),
        });
    }
    let paths = cluster_paths_from_defaults()?;
    let state = cluster::read_cluster_state_without_kv(&paths)?;
    if state.cluster_id != identity.cluster_id {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "cluster rollout target belongs to {}, but durable routing state belongs to {}",
                identity.cluster_id, state.cluster_id
            ),
        });
    }
    let member = state
        .members
        .iter()
        .find(|member| member.node_id == identity.node_id)
        .ok_or_else(|| CliError::ClusterReadOnly {
            detail: format!(
                "cluster rollout target {} is absent from durable routing membership; refusing an activation with unknown voter state",
                identity.node_id
            ),
        })?;
    if !member.state.is_authorized() {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "cluster rollout target {} has unauthorized durable membership state {:?}",
                identity.node_id, member.state
            ),
        });
    }
    let authority_target = match &member.role {
        ClusterMemberRole::Voter => Some(identity.clone()),
        ClusterMemberRole::Learner | ClusterMemberRole::NonVoting => None,
    };
    if status
        .cluster
        .as_ref()
        .is_some_and(|cluster| cluster.member_role == ClusterMemberRole::Voter)
        != authority_target.is_some()
    {
        eprintln!(
            "warning: {} reported stale member role {:?}; durable routing membership controls rollout preflight classification",
            target.label(),
            status.cluster.as_ref().map(|cluster| &cluster.member_role)
        );
    }
    Ok(RolloutClusterActivationPreflight {
        status,
        cluster_target: identity,
        authority_target,
    })
}

fn rollout_indeterminate_activation_requires_stop(
    authority_target: Option<&RolloutAuthorityTarget>,
) -> bool {
    authority_target.is_some()
}

fn rejected_rollout_response_requires_authority_stop(
    authority_target: Option<&RolloutAuthorityTarget>,
    response: &ClusterRolloutBinaryResponse,
) -> bool {
    authority_target.is_some() && response.status != "accepted"
}

fn rollout_authority_status_pid(
    expected: &RolloutAuthorityTarget,
    status: &DaemonStatus,
) -> Result<u32, String> {
    let cluster = status.cluster.as_ref().ok_or_else(|| {
        "replacement daemon did not report clustered main-daemon identity".to_string()
    })?;
    if cluster.cluster_id != expected.cluster_id || cluster.node_id != expected.node_id {
        return Err(format!(
            "replacement daemon identity changed from {}/{} to {}/{}",
            expected.cluster_id, expected.node_id, cluster.cluster_id, cluster.node_id
        ));
    }
    if cluster.process != DAEMON_MAIN_CLUSTER_ROLE {
        return Err(format!(
            "readiness probe reached process {:?}, expected {DAEMON_MAIN_CLUSTER_ROLE}",
            cluster.process
        ));
    }
    let rollout = status.rollout.as_ref().ok_or_else(|| {
        "replacement daemon no longer reports the accepted rollout handoff".to_string()
    })?;
    if rollout.status != "accepted" || rollout.phase.as_deref() != Some("shadow-finalized") {
        return Err(format!(
            "replacement daemon rollout handoff is {}/{}, expected accepted/shadow-finalized",
            rollout.status,
            rollout.phase.as_deref().unwrap_or("unknown")
        ));
    }
    Ok(status.pid)
}

fn wait_for_rollout_authority_readiness_with_probes<S, A, N, W>(
    target_label: &str,
    expected: &RolloutAuthorityTarget,
    timing: RolloutAuthorityReadinessTiming,
    mut status_probe: S,
    mut authority_probe: A,
    mut now: N,
    mut wait: W,
) -> Result<(), CliError>
where
    S: FnMut(Duration) -> Result<DaemonStatus, CliError>,
    A: FnMut(Duration) -> Result<(), CliError>,
    N: FnMut() -> Instant,
    W: FnMut(Duration),
{
    let started_at = now();
    let deadline = started_at.checked_add(timing.timeout).unwrap_or(started_at);
    let mut stable_since = None;
    let mut stable_pid = None;
    let mut last_failure = "cluster KV readiness has not been observed".to_string();

    loop {
        let remaining = deadline.saturating_duration_since(now());
        if remaining.is_zero() {
            break;
        }
        let probe_timeout = timing
            .probe_timeout
            .min(remaining)
            .max(Duration::from_millis(1));
        match status_probe(probe_timeout) {
            Ok(status) => match rollout_authority_status_pid(expected, &status) {
                Ok(pid) => {
                    let remaining = deadline.saturating_duration_since(now());
                    if remaining.is_zero() {
                        last_failure =
                            "target status probe exhausted the voter rejoin probe budget"
                                .to_string();
                        continue;
                    }
                    let authority_probe_timeout = timing
                        .probe_timeout
                        .min(remaining)
                        .max(Duration::from_millis(1));
                    match authority_probe(authority_probe_timeout) {
                        Ok(()) => {
                            let healthy_at = now();
                            if stable_pid != Some(pid) {
                                stable_pid = Some(pid);
                                stable_since = Some(healthy_at);
                            }
                            let stable_for = stable_since
                                .map(|since| healthy_at.saturating_duration_since(since))
                                .unwrap_or_default();
                            if stable_for >= timing.stable_window {
                                return Ok(());
                            }
                            last_failure = format!(
                                "voter PID {pid} has been ready for {}ms; {}ms required",
                                stable_for.as_millis(),
                                timing.stable_window.as_millis()
                            );
                        }
                        Err(error) => {
                            stable_since = None;
                            stable_pid = None;
                            last_failure = error.to_string();
                        }
                    }
                }
                Err(detail) => {
                    stable_since = None;
                    stable_pid = None;
                    last_failure = detail;
                }
            },
            Err(error) => {
                stable_since = None;
                stable_pid = None;
                last_failure = error.to_string();
            }
        }

        let remaining = deadline.saturating_duration_since(now());
        if remaining.is_zero() {
            break;
        }
        wait(timing.poll_interval.min(remaining));
    }

    Err(CliError::ClusterReadOnly {
        detail: format!(
            "rollout target {target_label} accepted and shadow-finalized its replacement process, but Raft voter {} did not regain stable cluster KV authority readiness within {}s; refusing to advance to another cluster authority; last probe: {last_failure}",
            expected.node_id,
            timing.timeout.as_secs()
        ),
    })
}

fn rollout_authority_remaining(
    deadline: Instant,
    now: Instant,
    exhausted_detail: impl Into<String>,
) -> Result<Duration, CliError> {
    let remaining = deadline.saturating_duration_since(now);
    if remaining.is_zero() {
        return Err(CliError::ClusterReadOnly {
            detail: exhausted_detail.into(),
        });
    }
    Ok(remaining)
}

fn rollout_main_rpc_options(timeout: Duration) -> network::ClusterObjectRpcOptions {
    network::ClusterObjectRpcOptions::new(network::DaemonRpcTimeouts::connect_and_idle(timeout))
        .with_overall_timeout(timeout)
        .with_destination_process_affinity(network::DestinationProcessAffinity::MainDaemon)
}

fn send_rollout_local_main_object_rpc_with_deadline(
    expected_cluster_id: &Id,
    expected_node_id: &NodeId,
    request: &network::ObjectRpcRequest,
    timeout: Duration,
) -> Result<network::ObjectRpcResponse, CliError> {
    let started_at = Instant::now();
    let deadline = started_at.checked_add(timeout).unwrap_or(started_at);
    let paths = daemon_paths_from_defaults()?;
    let probe_timeout = rollout_authority_remaining(
        deadline,
        Instant::now(),
        "local main-daemon discovery exhausted the rollout authority probe budget",
    )?;
    let status = match probe_daemon_status(&paths, probe_timeout) {
        DaemonProbeStatus::Running(status) => status,
        DaemonProbeStatus::Locked {
            status: Some(status),
        } => status,
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
            return Err(CliError::Network {
                address: expected_node_id.to_string(),
                detail: "rollout authority probe could not resolve the local main daemon"
                    .to_string(),
            });
        }
    };
    let identity = rollout_cluster_target_from_status(&status).ok_or_else(|| CliError::Network {
        address: status.address.clone(),
        detail: "rollout authority probe reached a local process without clustered main-daemon identity"
            .to_string(),
    })?;
    if identity.cluster_id != *expected_cluster_id || identity.node_id != *expected_node_id {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "rollout local main-daemon identity changed to {}/{}, expected {}/{}",
                identity.cluster_id, identity.node_id, expected_cluster_id, expected_node_id
            ),
        });
    }
    let address = status.address;

    rollout_authority_remaining(
        deadline,
        Instant::now(),
        "local main-daemon discovery exhausted the rollout authority RPC budget",
    )?;
    let request = request.clone();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| CliError::Network {
            address: address.clone(),
            detail: format!("could not start rollout authority RPC runtime: {source}"),
        })?;
    let remaining = rollout_authority_remaining(
        deadline,
        Instant::now(),
        "rollout authority RPC runtime startup exhausted the whole-call budget",
    )?;
    runtime.block_on(async move {
        let call = network::ObjectRpcCall::value(request);
        let options = rollout_main_rpc_options(remaining);
        let future = async {
            let runtime = tokio::runtime::Handle::current();
            let result = daemon_send_daemon_rpc_call_async(&address, call, options, &runtime)
                .await
                .map_err(CliError::from)?;
            result
                .into_materialized_value_response(usize::MAX)
                .await
                .map_err(CliError::from)
        };
        tokio::time::timeout(remaining, future)
            .await
            .map_err(|_| CliError::Network {
                address,
                detail: format!(
                    "rollout authority object RPC exceeded its {}ms whole-call deadline",
                    remaining.as_millis()
                ),
            })?
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RolloutMainRpcDestination {
    Local,
    Routed(NodeId),
}

fn rollout_main_rpc_destination(
    state: &sinomo_runtime::core::ClusterBootstrapState,
    expected: &RolloutAuthorityTarget,
) -> RolloutMainRpcDestination {
    if state.local_node_id == expected.node_id {
        RolloutMainRpcDestination::Local
    } else {
        RolloutMainRpcDestination::Routed(expected.node_id.clone())
    }
}

fn send_rollout_main_object_rpc_with_timeout(
    expected: &RolloutAuthorityTarget,
    request: &network::ObjectRpcRequest,
    timeout: Duration,
) -> Result<network::ObjectRpcResponse, CliError> {
    let paths = cluster_paths_from_defaults()?;
    let state = cluster::read_cluster_state_without_kv(&paths)?;
    if state.cluster_id != expected.cluster_id {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "rollout readiness probe belongs to cluster {}, but local routing state belongs to {}",
                expected.cluster_id, state.cluster_id
            ),
        });
    }

    match rollout_main_rpc_destination(&state, expected) {
        RolloutMainRpcDestination::Local => send_rollout_local_main_object_rpc_with_deadline(
            &expected.cluster_id,
            &expected.node_id,
            request,
            timeout,
        ),
        RolloutMainRpcDestination::Routed(node_id) => {
            // A wildcard bind maps to loopback only on the restarted node. A
            // controller-side address fallback could otherwise read the
            // controller's own KV view and falsely prove a remote voter. Keep
            // remote readiness identity-bound and fail closed on route loss.
            sinomo_runtime::daemon::send_object_rpc_to_target_with_options(
                &node_id,
                request,
                rollout_main_rpc_options(timeout),
                true,
            )
            .map_err(Into::into)
        }
    }
}

fn read_rollout_main_target_status(
    expected: &RolloutAuthorityTarget,
    timeout: Duration,
) -> Result<DaemonStatus, CliError> {
    let request = network::ObjectRpcRequest::encode_body(
        sinomo_runtime::daemon::DAEMON_STATUS_ROUTE,
        sinomo_runtime::daemon::DAEMON_RPC_CATEGORY,
        &(),
    )?;
    let response = send_rollout_main_object_rpc_with_timeout(expected, &request, timeout)?;
    response.decode_body().map_err(Into::into)
}

fn send_rollout_coordinator_object_rpc_with_timeout(
    expected_cluster_id: &Id,
    request: &network::ObjectRpcRequest,
    timeout: Duration,
) -> Result<network::ObjectRpcResponse, CliError> {
    let paths = cluster_paths_from_defaults()?;
    let state = cluster::read_cluster_state_without_kv(&paths)?;
    if state.cluster_id != *expected_cluster_id {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "rollout coordinator belongs to cluster {}, expected {}",
                state.cluster_id, expected_cluster_id
            ),
        });
    }
    send_rollout_local_main_object_rpc_with_deadline(
        expected_cluster_id,
        &state.local_node_id,
        request,
        timeout,
    )
}

fn rollout_authoritative_entry_route_is_unsupported(error: &CliError) -> bool {
    let route = sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE;
    let unknown_object_route = format!("unknown object rpc route: {route}");
    let unknown_service_route = format!("unknown service route: {route}");
    let detail_is_unsupported = |detail: &str| {
        let detail = detail
            .strip_prefix("failed to parse command: ")
            .unwrap_or(detail);
        detail == unknown_object_route || detail == unknown_service_route
    };
    match error {
        CliError::Parse { detail } => detail_is_unsupported(detail),
        CliError::Network { address, detail } if address == route => detail_is_unsupported(detail),
        _ => false,
    }
}

type RolloutAuthoritativeRaftConfigRead = (
    KvAuthoritativeRead<Option<sinomo_runtime::kv::KvEntry>>,
    sinomo_runtime::kv::ClusterKvRaftConfigRecord,
    BTreeSet<NodeId>,
);

fn read_rollout_legacy_authoritative_raft_config_entry_with<S>(
    expected_cluster_id: &Id,
    raft_config_key: &str,
    timeout: Duration,
    send: &mut S,
) -> Result<KvAuthoritativeRead<Option<sinomo_runtime::kv::KvEntry>>, CliError>
where
    S: FnMut(
        &Id,
        &network::ObjectRpcRequest,
        Duration,
    ) -> Result<network::ObjectRpcResponse, CliError>,
{
    let payload = sinomo_runtime::kv::cluster_kv_authoritative_range_prefix_rpc_payload(
        raft_config_key.to_string(),
    )?;
    let request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = send(expected_cluster_id, &request, timeout)?;
    response.error_detail()?;
    let range =
        sinomo_runtime::kv::decode_cluster_kv_authoritative_range_prefix_rpc_body(&response.body)?;
    Ok(KvAuthoritativeRead {
        value: range
            .value
            .into_iter()
            .find(|entry| entry.key == raft_config_key),
        authority: range.authority,
    })
}

fn read_rollout_authoritative_raft_config(
    expected: &RolloutAuthorityTarget,
    timeout: Duration,
) -> Result<RolloutAuthoritativeRaftConfigRead, CliError> {
    read_rollout_authoritative_raft_config_with(
        expected,
        timeout,
        send_rollout_coordinator_object_rpc_with_timeout,
        Instant::now,
    )
}

fn read_rollout_authoritative_raft_config_with<S, N>(
    expected: &RolloutAuthorityTarget,
    timeout: Duration,
    mut send: S,
    mut now: N,
) -> Result<RolloutAuthoritativeRaftConfigRead, CliError>
where
    S: FnMut(
        &Id,
        &network::ObjectRpcRequest,
        Duration,
    ) -> Result<network::ObjectRpcResponse, CliError>,
    N: FnMut() -> Instant,
{
    let started_at = now();
    let deadline = started_at.checked_add(timeout).unwrap_or(started_at);
    let raft_config_key = sinomo_runtime::kv::cluster_kv_raft_config_key();
    let payload =
        sinomo_runtime::kv::cluster_kv_authoritative_entry_rpc_payload(raft_config_key.clone())?;
    let request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let exact_timeout = rollout_authority_remaining(
        deadline,
        now(),
        "exact coordinator authoritative KV read exhausted its probe budget",
    )?;
    let exact_response = send(&expected.cluster_id, &request, exact_timeout);
    let authoritative = match exact_response {
        Ok(response) => match response.error_detail() {
            Ok(()) => {
                sinomo_runtime::kv::decode_cluster_kv_authoritative_entry_rpc_body(&response.body)?
            }
            Err(error) => {
                let error = CliError::from(error);
                if !rollout_authoritative_entry_route_is_unsupported(&error) {
                    return Err(error);
                }
                eprintln!(
                    "warning: rollout coordinator does not support exact authoritative KV entry reads; using legacy prefix compatibility for this one mixed-version rollout transition; work may be proportional to retained Raft config-change history"
                );
                let fallback_timeout = rollout_authority_remaining(
                    deadline,
                    now(),
                    "exact coordinator compatibility probe exhausted the legacy authoritative KV read budget",
                )?;
                read_rollout_legacy_authoritative_raft_config_entry_with(
                    &expected.cluster_id,
                    &raft_config_key,
                    fallback_timeout,
                    &mut send,
                )?
            }
        },
        Err(error) if rollout_authoritative_entry_route_is_unsupported(&error) => {
            eprintln!(
                "warning: rollout coordinator does not support exact authoritative KV entry reads; using legacy prefix compatibility for this one mixed-version rollout transition; work may be proportional to retained Raft config-change history"
            );
            let fallback_timeout = rollout_authority_remaining(
                deadline,
                now(),
                "exact coordinator compatibility probe exhausted the legacy authoritative KV read budget",
            )?;
            read_rollout_legacy_authoritative_raft_config_entry_with(
                &expected.cluster_id,
                &raft_config_key,
                fallback_timeout,
                &mut send,
            )?
        }
        Err(error) => return Err(error),
    };
    sinomo_runtime::kv::validate_cluster_kv_current_term_authority_proof(&authoritative.authority)?;
    let entry = authoritative.value.as_ref().ok_or_else(|| CliError::ClusterReadOnly {
        detail: format!(
            "authoritative KV proof did not contain the Raft configuration key {raft_config_key:?}"
        ),
    })?;
    if entry.key != raft_config_key {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "authoritative KV proof returned key {:?} instead of {raft_config_key:?}",
                entry.key
            ),
        });
    }
    let config: sinomo_runtime::kv::ClusterKvRaftConfigRecord = entry.decode()?;
    let unique_voters = config.voters.iter().cloned().collect::<BTreeSet<_>>();
    let configured_quorum = sinomo_runtime::kv::cluster_kv_raft_quorum_size(unique_voters.len());
    let proof_matches_config = matches!(
        authoritative.authority.basis,
        sinomo_runtime::kv::KvAuthorityBasis::CurrentTermQuorum {
            voter_acks,
            required_quorum,
        } if required_quorum == configured_quorum
            && voter_acks <= unique_voters.len()
    );
    if config.cluster_id != expected.cluster_id
        || unique_voters.len() != config.voters.len()
        || entry.create_revision > entry.modify_revision
        || entry.modify_revision > authoritative.authority.revision
        || !proof_matches_config
    {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "authoritative KV proof/config mismatch for rollout target {}: cluster={}, configured voters={}, configured quorum={}, proof={:?}",
                expected.node_id,
                config.cluster_id,
                unique_voters.len(),
                configured_quorum,
                authoritative.authority.basis,
            ),
        });
    }
    Ok((authoritative, config, unique_voters))
}

fn validate_rollout_target_cluster_kv_authority_readiness(
    expected: &RolloutAuthorityTarget,
    raft_config_key: &str,
    authoritative: &KvAuthoritativeRead<Option<sinomo_runtime::kv::KvEntry>>,
    authoritative_config: &sinomo_runtime::kv::ClusterKvRaftConfigRecord,
    unique_voters: &BTreeSet<NodeId>,
    local: &sinomo_runtime::kv::KvLocalRead<Option<sinomo_runtime::kv::KvEntry>>,
) -> Result<(), CliError> {
    if !unique_voters.contains(&expected.node_id) {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "restarted rollout target {} is no longer a voter in the committed Raft configuration",
                expected.node_id
            ),
        });
    }
    let local_entry = local.value.as_ref().ok_or_else(|| CliError::ClusterReadOnly {
        detail: format!(
            "target-local KV view did not contain the Raft configuration key {raft_config_key:?}"
        ),
    })?;
    if local_entry.key != raft_config_key {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "target-local KV view returned key {:?} instead of {raft_config_key:?}",
                local_entry.key
            ),
        });
    }
    let local_config: sinomo_runtime::kv::ClusterKvRaftConfigRecord = local_entry.decode()?;
    let known_leader_is_configured = local
        .known_leader
        .as_ref()
        .is_some_and(|leader| unique_voters.contains(leader));

    if local.observed_term != authoritative.authority.term
        || local.applied_index < authoritative.authority.commit_index
        || local.revision < authoritative.authority.revision
        || !known_leader_is_configured
        || local.value != authoritative.value
        || !sinomo_runtime::kv::cluster_kv_raft_config_record_stable_eq(
            &local_config,
            authoritative_config,
        )
    {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "target has not rejoined current cluster KV replication: local term/index/revision/leader={}/{}/{:?}/{:?}, authoritative term/index/revision={}/{}/{:?}",
                local.observed_term,
                local.applied_index,
                local.revision,
                local.known_leader,
                authoritative.authority.term,
                authoritative.authority.commit_index,
                authoritative.authority.revision,
            ),
        });
    }
    Ok(())
}

fn rollout_coordinator_authority_probe_budget(total: Duration) -> Duration {
    let target_local_reserve = DAEMON_JOIN_TIMEOUT.min(total / 2);
    DAEMON_CLUSTER_KV_TXN_TIMEOUT.min(total.saturating_sub(target_local_reserve))
}

fn read_rollout_target_local_raft_config_entry(
    expected: &RolloutAuthorityTarget,
    raft_config_key: &str,
    timeout: Duration,
) -> Result<sinomo_runtime::kv::KvLocalRead<Option<sinomo_runtime::kv::KvEntry>>, CliError> {
    let local_payload =
        sinomo_runtime::kv::cluster_kv_local_entry_rpc_payload(raft_config_key.to_string())?;
    let local_request = network::ObjectRpcRequest::from_encoded_body(
        local_payload.route,
        local_payload.category,
        local_payload.body,
    );
    let local_response =
        send_rollout_main_object_rpc_with_timeout(expected, &local_request, timeout)?;
    local_response.error_detail()?;
    sinomo_runtime::kv::decode_cluster_kv_local_entry_rpc_body(&local_response.body)
        .map_err(Into::into)
}

fn probe_rollout_target_cluster_kv_authority_readiness(
    expected: &RolloutAuthorityTarget,
    timeout: Duration,
) -> Result<(), CliError> {
    probe_rollout_target_cluster_kv_authority_readiness_with(
        expected,
        timeout,
        |timeout| read_rollout_authoritative_raft_config(expected, timeout),
        |raft_config_key, timeout| {
            read_rollout_target_local_raft_config_entry(expected, raft_config_key, timeout)
        },
        Instant::now,
    )
}

fn probe_rollout_target_cluster_kv_authority_readiness_with<A, L, N>(
    expected: &RolloutAuthorityTarget,
    timeout: Duration,
    mut authoritative_read: A,
    mut local_read: L,
    mut now: N,
) -> Result<(), CliError>
where
    A: FnMut(Duration) -> Result<RolloutAuthoritativeRaftConfigRead, CliError>,
    L: FnMut(
        &str,
        Duration,
    ) -> Result<
        sinomo_runtime::kv::KvLocalRead<Option<sinomo_runtime::kv::KvEntry>>,
        CliError,
    >,
    N: FnMut() -> Instant,
{
    let started_at = now();
    let deadline = started_at.checked_add(timeout).unwrap_or(started_at);
    let raft_config_key = sinomo_runtime::kv::cluster_kv_raft_config_key();
    let coordinator_budget = rollout_coordinator_authority_probe_budget(timeout);
    if coordinator_budget.is_zero() {
        return Err(CliError::ClusterReadOnly {
            detail:
                "voter rejoin probe budget cannot cover both coordinator and target-local KV reads"
                    .to_string(),
        });
    }
    let (authoritative, authoritative_config, unique_voters) =
        authoritative_read(coordinator_budget)?;

    let remaining = rollout_authority_remaining(
        deadline,
        now(),
        "coordinator authoritative KV proof exhausted the voter rejoin probe budget",
    )?;
    let local = local_read(&raft_config_key, remaining)?;
    validate_rollout_target_cluster_kv_authority_readiness(
        expected,
        &raft_config_key,
        &authoritative,
        &authoritative_config,
        &unique_voters,
        &local,
    )
}

fn run_rollout_authority_readiness_with_wall_clock_guard<F>(
    target_label: &str,
    node_id: &NodeId,
    timeout: Duration,
    task: F,
) -> Result<(), CliError>
where
    F: FnOnce() -> Result<(), CliError> + Send + 'static,
{
    let started_at = Instant::now();
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let worker = thread::Builder::new()
        .name("sinomo-rollout-authority-readiness".to_string())
        .spawn(move || {
            let _ = result_tx.send(task());
        })
        .map_err(|source| CliError::ClusterReadOnly {
            detail: format!(
                "rollout target {target_label} accepted its replacement process, but the authority-readiness worker for Raft voter {node_id} could not start: {source}; refusing to advance to another cluster authority"
            ),
        })?;
    let remaining = timeout.saturating_sub(started_at.elapsed());
    match result_rx.recv_timeout(remaining) {
        Ok(result) => {
            if worker.join().is_err() {
                return Err(CliError::ClusterReadOnly {
                    detail: format!(
                        "rollout target {target_label} accepted its replacement process, but the authority-readiness worker for Raft voter {node_id} panicked after returning its result; refusing to advance to another cluster authority"
                    ),
                });
            }
            result
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            // A transport implementation can violate its own deadline while
            // blocked below an async cancellation boundary. Detach exactly
            // this one worker instead of joining it (which would recreate the
            // hang); every caller stops the rollout on this fail-closed error.
            drop(worker);
            Err(CliError::ClusterReadOnly {
                detail: format!(
                    "rollout target {target_label} accepted and shadow-finalized its replacement process, but the authority-readiness worker for Raft voter {node_id} exceeded the {}s wall-clock deadline; refusing to advance to another cluster authority",
                    timeout.as_secs()
                ),
            })
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            let _ = worker.join();
            Err(CliError::ClusterReadOnly {
                detail: format!(
                    "rollout target {target_label} accepted its replacement process, but the authority-readiness worker for Raft voter {node_id} stopped without a result; refusing to advance to another cluster authority"
                ),
            })
        }
    }
}

fn ensure_rollout_target_authority_ready_after_acceptance(
    target: &RolloutTarget,
    authority_target: Option<&RolloutAuthorityTarget>,
    runtime_source: Option<RolloutRuntimeSourceIdentity<'_>>,
    _explicit_host: Option<&str>,
    _main_port: u16,
    response: &ClusterRolloutBinaryResponse,
) -> Result<(), CliError> {
    if response.status == "accepted"
        && let Some(runtime_source) = runtime_source
    {
        let status = read_rollout_target_status(target)?;
        validate_rollout_runtime_source_identity(target.label(), runtime_source, &status)?;
    }
    let Some(expected) = authority_target else {
        return Ok(());
    };
    let expected = expected.clone();
    if rejected_rollout_response_requires_authority_stop(Some(&expected), response) {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "rollout target {} returned terminal status {:?} for Raft voter {}; no positive rollback/no-activation proof is available, so refusing to advance to another cluster authority",
                target.label(),
                response.status,
                expected.node_id
            ),
        });
    }
    let target_label = target.label().to_string();
    let target_label_for_worker = target_label.clone();
    let expected_for_worker = expected.clone();
    run_rollout_authority_readiness_with_wall_clock_guard(
        &target_label,
        &expected.node_id,
        ROLLOUT_AUTHORITY_READINESS_TIMEOUT.saturating_add(ROLLOUT_AUTHORITY_READINESS_GUARD_GRACE),
        move || {
            wait_for_rollout_authority_readiness_with_probes(
                &target_label_for_worker,
                &expected_for_worker,
                RolloutAuthorityReadinessTiming::production(),
                |timeout| {
                    read_rollout_main_target_status(
                        &expected_for_worker,
                        DAEMON_JOIN_TIMEOUT.min(timeout),
                    )
                },
                |timeout| {
                    probe_rollout_target_cluster_kv_authority_readiness(
                        &expected_for_worker,
                        timeout,
                    )
                },
                Instant::now,
                thread::sleep,
            )
        },
    )?;
    println!(
        "{}: Raft voter rejoined cluster KV and remained authority-ready for {}s",
        target.label(),
        ROLLOUT_AUTHORITY_READINESS_STABLE_WINDOW.as_secs()
    );
    Ok(())
}

fn distinct_host_convergence_requested_at(previous: &str) -> String {
    let requested_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown-host-bind-convergence".to_string());
    if requested_at == previous {
        format!("{previous}-host-bind-convergence")
    } else {
        requested_at
    }
}

fn host_convergence_upload_id() -> String {
    format!(
        "host-bind-convergence-{}-{}",
        std::process::id(),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    )
}

fn rollout_listener_hosts_match_explicit_host(
    resolved: &ResolvedRolloutListenerHosts,
    explicit_host: &str,
    allow_local_shadow_finalization_proof: bool,
) -> bool {
    if resolved.hosts.main != explicit_host {
        return false;
    }
    match resolved.hosts.shadow.as_deref() {
        Some(shadow_host) => shadow_host == explicit_host,
        None => allow_local_shadow_finalization_proof && resolved.shadow_finalization_proven,
    }
}

fn converge_explicit_rollout_host_with<R, A>(
    target_label: &str,
    explicit_host: Option<&str>,
    initial_response_status: &str,
    mut read_host: R,
    mut reactivate_once: A,
) -> Result<(), CliError>
where
    R: FnMut() -> Result<ResolvedRolloutListenerHosts, CliError>,
    A: FnMut(&RolloutTarget) -> Result<String, CliError>,
{
    let Some(explicit_host) = explicit_host else {
        return Ok(());
    };
    if initial_response_status != "accepted" {
        return Ok(());
    }

    let first = read_host()?;
    // The first activation may have been planned by an older daemon that did
    // not apply one host override to both processes. Require an independently
    // observed matching shadow before skipping the compatibility reactivation.
    if rollout_listener_hosts_match_explicit_host(&first, explicit_host, false) {
        return Ok(());
    }

    println!(
        "{target_label}: finalized on main/shadow bind hosts {}/{}; applying explicit --host {explicit_host} once against the upgraded daemon",
        first.hosts.main,
        first.hosts.shadow.as_deref().unwrap_or("unreachable")
    );
    let second_status = reactivate_once(&first.main_target)?;
    if second_status != "accepted" {
        return Err(CliError::Network {
            address: target_label.to_string(),
            detail: format!(
                "explicit --host convergence activation returned {second_status:?}, expected accepted"
            ),
        });
    }
    let final_hosts = read_host()?;
    // The retry was planned and finalized by the upgraded daemon. Its durable
    // accepted/shadow-finalized record is therefore valid node-local evidence
    // when a firewalled or wildcard-bound supervisor cannot be probed remotely.
    if !rollout_listener_hosts_match_explicit_host(&final_hosts, explicit_host, true) {
        return Err(CliError::Network {
            address: target_label.to_string(),
            detail: format!(
                "explicit --host convergence finalized on main/shadow bind hosts {}/{}, expected {explicit_host} for both; refusing a third activation",
                final_hosts.hosts.main,
                final_hosts.hosts.shadow.as_deref().unwrap_or("unreachable")
            ),
        });
    }
    println!("{target_label}: explicit main and shadow bind hosts converged to {explicit_host}");
    Ok(())
}

fn ensure_cluster_rollout_host_convergence_after_acceptance<F>(
    target: &RolloutTarget,
    cluster_target: &RolloutAuthorityTarget,
    authority_target: Option<&RolloutAuthorityTarget>,
    runtime_source: Option<RolloutRuntimeSourceIdentity<'_>>,
    explicit_host: Option<&str>,
    initial_response: &ClusterRolloutBinaryResponse,
    original_requested_at: &str,
    main_port: u16,
    shadow_port: u16,
    mut reactivate: F,
) -> Result<(), CliError>
where
    F: FnMut(&RolloutTarget, String) -> Result<ClusterRolloutBinaryResponse, CliError>,
{
    let retry_requested_at = distinct_host_convergence_requested_at(original_requested_at);
    let explicit_host_for_read = explicit_host.unwrap_or_default();
    converge_explicit_rollout_host_with(
        target.label(),
        explicit_host,
        &initial_response.status,
        || {
            read_rollout_listener_hosts_after_restart(
                cluster_target,
                target,
                explicit_host_for_read,
                main_port,
                shadow_port,
            )
        },
        |restart_target| {
            let response = reactivate(restart_target, retry_requested_at.clone())?;
            println!("{}: {}", target.label(), response.status);
            if !response.detail.is_empty() {
                println!("  {}", response.detail);
            }
            ensure_rollout_target_authority_ready_after_acceptance(
                target,
                authority_target,
                runtime_source,
                explicit_host,
                main_port,
                &response,
            )?;
            Ok(response.status)
        },
    )
}

#[cfg(test)]
#[test]
fn explicit_ipv6_wildcard_refresh_uses_ipv6_loopback() {
    assert_eq!(rollout_restart_direct_address("::", 21353), "[::1]:21353");
}

#[cfg(test)]
#[test]
fn explicit_host_convergence_reactivates_legacy_first_pass_exactly_once() {
    let mut observed_hosts = [
        RolloutListenerHosts {
            main: "127.0.0.1".to_string(),
            shadow: None,
        },
        RolloutListenerHosts {
            main: "0.0.0.0".to_string(),
            shadow: Some("0.0.0.0".to_string()),
        },
    ]
    .into_iter();
    let mut reactivations = 0;
    let restart_target = RolloutTarget::address("127.0.0.1:21353".to_string());

    converge_explicit_rollout_host_with(
        "local",
        Some("0.0.0.0"),
        "accepted",
        || {
            Ok(ResolvedRolloutListenerHosts {
                hosts: observed_hosts.next().expect("host observation"),
                main_target: restart_target.clone(),
                shadow_finalization_proven: true,
            })
        },
        |target| {
            assert_eq!(target, &restart_target);
            reactivations += 1;
            Ok("accepted".to_string())
        },
    )
    .expect("host convergence");

    assert_eq!(reactivations, 1);
    assert_eq!(observed_hosts.next(), None);
}

#[cfg(test)]
#[test]
fn explicit_host_convergence_does_not_duplicate_matching_or_default_rollouts() {
    for explicit_host in [None, Some("0.0.0.0")] {
        let mut reads = 0;
        let mut reactivations = 0;
        let restart_target = RolloutTarget::address("127.0.0.1:21353".to_string());
        converge_explicit_rollout_host_with(
            "local",
            explicit_host,
            "accepted",
            || {
                reads += 1;
                Ok(ResolvedRolloutListenerHosts {
                    hosts: RolloutListenerHosts {
                        main: "0.0.0.0".to_string(),
                        shadow: Some("0.0.0.0".to_string()),
                    },
                    main_target: restart_target.clone(),
                    shadow_finalization_proven: true,
                })
            },
            |_| {
                reactivations += 1;
                Ok("accepted".to_string())
            },
        )
        .expect("host already converged");
        assert_eq!(reactivations, 0);
        assert_eq!(reads, usize::from(explicit_host.is_some()));
    }
}

#[cfg(test)]
#[test]
fn explicit_host_convergence_accepts_local_finalization_only_after_upgraded_retry() {
    let mut reads = 0;
    let mut reactivations = 0;
    let restart_target = RolloutTarget::routed_node(
        NodeId::from_legacy_named("explicit-host-shadow-retry").expect("node id"),
    );

    converge_explicit_rollout_host_with(
        "remote",
        Some("0.0.0.0"),
        "accepted",
        || {
            reads += 1;
            Ok(ResolvedRolloutListenerHosts {
                hosts: RolloutListenerHosts {
                    main: "0.0.0.0".to_string(),
                    shadow: None,
                },
                main_target: restart_target.clone(),
                shadow_finalization_proven: true,
            })
        },
        |_| {
            reactivations += 1;
            Ok("accepted".to_string())
        },
    )
    .expect("shadow host convergence");

    assert_eq!(reads, 2);
    assert_eq!(reactivations, 1);
}

#[cfg(test)]
#[test]
fn explicit_host_convergence_retries_without_local_shadow_finalization_proof() {
    let mut shadow_finalization = [false, true].into_iter();
    let mut reactivations = 0;
    let restart_target = RolloutTarget::routed_node(
        NodeId::from_legacy_named("explicit-host-shadow-proof-retry").expect("node id"),
    );

    converge_explicit_rollout_host_with(
        "remote",
        Some("0.0.0.0"),
        "accepted",
        || {
            Ok(ResolvedRolloutListenerHosts {
                hosts: RolloutListenerHosts {
                    main: "0.0.0.0".to_string(),
                    shadow: None,
                },
                main_target: restart_target.clone(),
                shadow_finalization_proven: shadow_finalization
                    .next()
                    .expect("finalization observation"),
            })
        },
        |target| {
            assert_eq!(target, &restart_target);
            reactivations += 1;
            Ok("accepted".to_string())
        },
    )
    .expect("finalization proof converges after one retry");

    assert_eq!(reactivations, 1);
    assert_eq!(shadow_finalization.next(), None);
}

#[cfg(test)]
#[test]
fn explicit_host_convergence_rejects_observable_shadow_mismatch_after_retry() {
    let mut reactivations = 0;
    let restart_target = RolloutTarget::routed_node(
        NodeId::from_legacy_named("explicit-host-shadow-mismatch").expect("node id"),
    );

    let error = converge_explicit_rollout_host_with(
        "remote",
        Some("0.0.0.0"),
        "accepted",
        || {
            Ok(ResolvedRolloutListenerHosts {
                hosts: RolloutListenerHosts {
                    main: "0.0.0.0".to_string(),
                    shadow: Some("127.0.0.1".to_string()),
                },
                main_target: restart_target.clone(),
                shadow_finalization_proven: true,
            })
        },
        |_| {
            reactivations += 1;
            Ok("accepted".to_string())
        },
    )
    .expect_err("observable shadow mismatch must not be hidden by finalization status");

    assert_eq!(reactivations, 1);
    assert!(error.to_string().contains("refusing a third activation"));
}

#[cfg(test)]
mod rollout_authority_readiness_tests {
    use super::*;
    use std::process::Command;

    fn accepted_voter_status(pid: u32) -> DaemonStatus {
        DaemonStatus {
            pid,
            host: "127.0.0.1".to_string(),
            port: SINOMO_DAEMON_DEFAULT_PORT,
            address: format!("127.0.0.1:{SINOMO_DAEMON_DEFAULT_PORT}"),
            version: "test".to_string(),
            commit: Some("test".to_string()),
            target_os: Some(std::env::consts::OS.to_string()),
            target_arch: Some(std::env::consts::ARCH.to_string()),
            hostname: None,
            executable: None,
            rollout: Some(ClusterNodeRolloutStatus {
                status: "accepted".to_string(),
                mode: None,
                detail: None,
                phase: Some("shadow-finalized".to_string()),
                progress_current: None,
                progress_total: None,
                progress_message: None,
                source: None,
                requested_at: None,
                staged_at: None,
                updated_at: None,
                failed_at: None,
                diagnostics: None,
                stdout_log: None,
                stderr_log: None,
                stdout_tail: None,
                stderr_tail: None,
            }),
            started_at: "2026-08-04T00:00:00Z".to_string(),
            data_dir: PathBuf::from("/tmp/sinomo-rollout-authority-test"),
            lock_file: PathBuf::from("/tmp/sinomo-rollout-authority-test/daemon.lock"),
            cluster: Some(DaemonClusterStatus {
                cluster_id: Id::from_legacy_named("cluster-rollout-test").expect("cluster id"),
                node_id: Id::from_legacy_named("node-rollout-test").expect("node id"),
                process: DAEMON_MAIN_CLUSTER_ROLE.to_string(),
                member_role: ClusterMemberRole::Voter,
                member_state: ClusterMemberState::Voter,
            }),
            idle_stats: None,
        }
    }

    fn expected_restart_status(mut status: DaemonStatus, process: &str) -> DaemonStatus {
        status.cluster.as_mut().expect("cluster status").process = process.to_string();
        status.rollout = Some(ClusterNodeRolloutStatus {
            status: "main-staged".to_string(),
            mode: Some("cluster-remote-build".to_string()),
            detail: Some("replacement main is restarting".to_string()),
            phase: Some("main-staged".to_string()),
            progress_current: Some(5),
            progress_total: Some(5),
            progress_message: Some("replacement main is restarting".to_string()),
            source: Some("repo-a".to_string()),
            requested_at: Some("2026-08-04T00:00:01Z".to_string()),
            staged_at: None,
            updated_at: None,
            failed_at: None,
            diagnostics: None,
            stdout_log: None,
            stderr_log: None,
            stdout_tail: None,
            stderr_tail: None,
        });
        status
    }

    fn restart_test_iroh_endpoint(node_id: NodeId, address: &str) -> ClusterEndpointRecord {
        ClusterEndpointRecord {
            endpoint_id: Id::derived("sinomo.test.rollout.restart-endpoint", address),
            node_id,
            transport: ClusterTransportKind::Iroh,
            address: address.to_string(),
            source: sinomo_runtime::core::ClusterEndpointSource::Static,
            privacy: sinomo_runtime::core::ClusterEndpointPrivacy::Relayed,
            priority: 10,
            last_observed_latency_ms: None,
        }
    }

    #[test]
    fn expected_restart_evidence_prefers_the_shadow_and_checks_its_exact_identity() {
        let main = accepted_voter_status(39);
        let expected = rollout_cluster_target_from_status(&main).expect("cluster target");
        let target = RolloutTarget::address("oracle.sinomo.net:21353".to_string());
        let evidence = rollout_expected_restart_evidence(&target, &expected, None, 21_353, 21_354);
        assert_eq!(
            evidence.candidates[0].target.address,
            "oracle.sinomo.net:21354"
        );
        assert_eq!(
            evidence.candidates[0].expected_process,
            sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE
        );
        let expectation = RolloutProgressExpectation {
            source: "repo-a".to_string(),
            requested_at: "2026-08-04T00:00:01Z".to_string(),
            source_head: Some("head-new".to_string()),
        };
        let shadow =
            expected_restart_status(main, sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE);
        let mut probes = 0usize;

        let status = evidence
            .read_with(
                &expectation,
                Duration::from_secs(2),
                |candidate, timeout| {
                    probes += 1;
                    assert_eq!(candidate.address, "oracle.sinomo.net:21354");
                    assert!(timeout <= Duration::from_secs(2));
                    Ok(shadow.clone())
                },
            )
            .expect("the exact shadow process is authoritative restart evidence")
            .expect("matching restart status");

        assert_eq!(probes, 1);
        assert_eq!(status.phase.as_deref(), Some("main-staged"));
    }

    #[test]
    fn expected_restart_evidence_prefers_an_advertised_iroh_shadow_sibling() {
        let main = accepted_voter_status(40);
        let expected = rollout_cluster_target_from_status(&main).expect("cluster target");
        let shadow_node_id = shadow_daemon_node_id(&expected.node_id);
        let main_endpoint = restart_test_iroh_endpoint(
            expected.node_id.clone(),
            "iroh://main-rollout-route?relay=https://main-relay.example/",
        );
        let shadow_endpoint = restart_test_iroh_endpoint(
            shadow_node_id.clone(),
            "iroh://shadow-rollout-route?relay=https://shadow-relay.example/",
        );
        let target = RolloutTarget::endpoint_for_node(main_endpoint, expected.node_id.clone())
            .with_restart_endpoints(vec![shadow_endpoint]);
        let evidence = rollout_expected_restart_evidence(&target, &expected, None, 21_353, 21_354);
        assert_eq!(
            evidence.candidates[0].target.address,
            "iroh://shadow-rollout-route?relay=https://shadow-relay.example/"
        );
        assert_eq!(
            evidence.candidates[0]
                .target
                .endpoint
                .as_ref()
                .map(|endpoint| &endpoint.node_id),
            Some(&shadow_node_id)
        );
        assert_eq!(
            evidence.candidates[0].expected_process,
            sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE
        );
        let expectation = RolloutProgressExpectation {
            source: "repo-a".to_string(),
            requested_at: "2026-08-04T00:00:01Z".to_string(),
            source_head: Some("head-new".to_string()),
        };
        let shadow =
            expected_restart_status(main, sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE);

        let status = evidence
            .read_with(&expectation, Duration::from_secs(2), |candidate, _| {
                assert_eq!(
                    candidate
                        .endpoint
                        .as_ref()
                        .map(|endpoint| &endpoint.node_id),
                    Some(&shadow_node_id)
                );
                Ok(shadow.clone())
            })
            .expect("the advertised Iroh shadow endpoint is authoritative restart evidence")
            .expect("matching restart status");

        assert_eq!(status.phase.as_deref(), Some("main-staged"));
    }

    #[test]
    fn restart_endpoint_process_uses_canonical_folded_shadow_classification() {
        let main = accepted_voter_status(41);
        let expected = rollout_cluster_target_from_status(&main).expect("cluster target");
        let address = "folded-shadow.example.test:22454";
        let mut folded_shadow =
            network::daemon_lan_endpoint(&expected.node_id, address.to_string(), 10);
        folded_shadow.endpoint_id = sinomo_runtime::node::daemon_lan_endpoint_id(
            &shadow_daemon_node_id(&expected.node_id),
            address,
        );

        assert!(network::cluster_endpoint_identifies_shadow_daemon_for_node(
            &folded_shadow
        ));
        assert_eq!(
            rollout_restart_endpoint_process(&folded_shadow, &expected),
            Some(sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE)
        );
    }

    #[test]
    fn authoritative_restart_discovery_enriches_explicit_node_and_raw_targets() {
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-explicit-restart-endpoints");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let cluster_id =
            Id::from_legacy_named("cluster-rollout-explicit-restart").expect("cluster id");
        let local_node_id =
            NodeId::from_legacy_named("node-rollout-explicit-local").expect("local node");
        let main_node_id =
            NodeId::from_legacy_named("node-rollout-explicit-main").expect("main node");
        let shadow_node_id = shadow_daemon_node_id(&main_node_id);
        let now = OffsetDateTime::UNIX_EPOCH;
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "explicit restart discovery",
            local_node_id,
            "local",
            "local-key",
            now,
        );
        let mut main_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            main_node_id.clone(),
            "main",
            "main-key",
            now,
        );
        main_member.capabilities =
            sinomo_runtime::node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        main_member.allowed_capabilities = main_member.capabilities.clone();
        main_member.endpoints.push(network::daemon_lan_endpoint(
            &main_node_id,
            "main.explicit.test:21353".to_string(),
            10,
        ));
        let mut shadow_member = ClusterMemberRecord::local_non_voting(
            cluster_id.clone(),
            shadow_node_id.clone(),
            "shadow",
            "shadow-key",
            now,
        );
        shadow_member.endpoints.push(network::daemon_lan_endpoint(
            &shadow_node_id,
            "shadow.explicit.test:22454".to_string(),
            10,
        ));
        state.members.push(main_member);
        state.members.push(shadow_member);
        let expected = RolloutAuthorityTarget {
            cluster_id,
            node_id: main_node_id.clone(),
        };
        let discovered = rollout_restart_endpoints_from_state(&paths, &state, &expected);
        assert!(discovered.iter().any(|endpoint| {
            endpoint.node_id == shadow_node_id && endpoint.address == "shadow.explicit.test:22454"
        }));

        let node_target =
            explicit_rollout_targets_from_state(vec![main_node_id.to_string()], &state, None)
                .pop()
                .expect("explicit node-id target");
        let raw_target = explicit_rollout_targets_from_state(
            vec!["main.explicit.test:21353".to_string()],
            &state,
            None,
        )
        .pop()
        .expect("explicit raw target");

        for target in [&node_target, &raw_target] {
            let evidence = rollout_expected_restart_evidence_with_endpoints(
                target,
                &expected,
                None,
                21_353,
                21_354,
                &discovered,
            );
            assert_eq!(
                evidence.candidates[0].target.address,
                "shadow.explicit.test:22454"
            );
            assert_eq!(
                evidence.candidates[0].expected_process,
                sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE
            );
        }
    }

    #[test]
    fn expected_restart_evidence_rejects_a_main_identity_on_an_advertised_shadow_endpoint() {
        let main = accepted_voter_status(42);
        let expected = rollout_cluster_target_from_status(&main).expect("cluster target");
        let shadow_node_id = shadow_daemon_node_id(&expected.node_id);
        let target = RolloutTarget::endpoint_for_node(
            restart_test_iroh_endpoint(
                expected.node_id.clone(),
                "iroh://main-rollout-route?relay=https://main-relay.example/",
            ),
            expected.node_id.clone(),
        )
        .with_restart_endpoints(vec![restart_test_iroh_endpoint(
            shadow_node_id,
            "iroh://shadow-rollout-route?relay=https://shadow-relay.example/",
        )]);
        let evidence = rollout_expected_restart_evidence(&target, &expected, None, 21_353, 21_354);
        let expectation = RolloutProgressExpectation {
            source: "repo-a".to_string(),
            requested_at: "2026-08-04T00:00:01Z".to_string(),
            source_head: Some("head-new".to_string()),
        };
        let wrong_process = expected_restart_status(main, DAEMON_MAIN_CLUSTER_ROLE);

        let error = evidence
            .read_with(&expectation, Duration::from_secs(2), |candidate, _| {
                if candidate.address.starts_with("iroh://shadow-rollout-route") {
                    Ok(wrong_process.clone())
                } else {
                    Err(CliError::Network {
                        address: candidate.label().to_string(),
                        detail: "replacement route absent".to_string(),
                    })
                }
            })
            .expect_err("the listener address cannot substitute for process identity");

        let detail = error.to_string();
        assert!(detail.contains("process daemon-shadow"));
        assert!(detail.contains("replacement route absent"));
    }

    #[test]
    fn expected_restart_evidence_shares_one_deadline_across_candidates() {
        let main = accepted_voter_status(43);
        let expected = rollout_cluster_target_from_status(&main).expect("cluster target");
        let target = RolloutTarget::address("oracle.sinomo.net:21353".to_string());
        let evidence = rollout_expected_restart_evidence(&target, &expected, None, 21_353, 21_354);
        let expectation = RolloutProgressExpectation {
            source: "repo-a".to_string(),
            requested_at: "2026-08-04T00:00:01Z".to_string(),
            source_head: Some("head-new".to_string()),
        };
        let elapsed = Cell::new(Duration::ZERO);
        let observed_timeouts = std::cell::RefCell::new(Vec::new());

        let error = evidence
            .read_with_elapsed(
                &expectation,
                Duration::from_secs(2),
                || elapsed.get(),
                |candidate, timeout| {
                    observed_timeouts.borrow_mut().push(timeout);
                    elapsed.set(elapsed.get() + Duration::from_millis(1_500));
                    Err(CliError::Network {
                        address: candidate.label().to_string(),
                        detail: "blackholed restart endpoint".to_string(),
                    })
                },
            )
            .expect_err("lost restart candidates must exhaust one bounded deadline");

        assert_eq!(
            observed_timeouts.into_inner(),
            vec![Duration::from_secs(2), Duration::from_millis(500)],
            "candidate count must not multiply the status-probe error grace"
        );
        assert!(error.to_string().contains("probe deadline exhausted"));
    }

    #[test]
    fn source_snapshot_runtime_acceptance_rejects_dirty_or_unknown_commit_with_identity() {
        let mut status = accepted_voter_status(40);
        let expected = RolloutRuntimeSourceIdentity {
            repo: Path::new("/tmp/sinomo-rollout-runtime-source-test"),
            revision: "0123456789abcdef",
        };
        status.commit = Some("0123456789ab-dirty".to_string());
        let cluster = status.cluster.as_ref().expect("cluster identity");
        let target_identity = format!("{}/{}", cluster.cluster_id, cluster.node_id);

        let dirty = validate_rollout_runtime_source_identity("target.test", expected, &status)
            .expect_err("a clean source-snapshot rollout cannot accept a dirty runtime");
        let dirty_detail = dirty.to_string();
        assert!(dirty_detail.contains(&target_identity));
        assert!(dirty_detail.contains("0123456789ab-dirty"));
        assert!(dirty_detail.contains("0123456789abcdef"));

        status.commit = None;
        let unknown = validate_rollout_runtime_source_identity("target.test", expected, &status)
            .expect_err("a missing runtime commit cannot prove source identity");
        assert!(
            unknown
                .to_string()
                .contains("did not report a runtime commit")
        );
    }

    #[test]
    fn source_snapshot_runtime_acceptance_resolves_length_variants_before_comparing() {
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-runtime-source-identity");
        let repo = root.join("repo");
        fs::create_dir_all(&repo).expect("repo dir");
        let git = |args: &[&str]| {
            let output = Command::new("git")
                .arg("-C")
                .arg(&repo)
                .args(args)
                .output()
                .expect("run git");
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        };
        git(&["init"]);
        git(&["config", "user.email", "test@example.invalid"]);
        git(&["config", "user.name", "Sinomo Test"]);
        fs::write(repo.join("source.txt"), "one\n").expect("source");
        git(&["add", "source.txt"]);
        git(&["commit", "-m", "one"]);
        let first = git(&["rev-parse", "HEAD"]);

        let mut status = accepted_voter_status(39);
        status.commit = Some(first[..12].to_string());
        validate_rollout_runtime_source_identity(
            "target.test",
            RolloutRuntimeSourceIdentity {
                repo: &repo,
                revision: &first[..8],
            },
            &status,
        )
        .expect("8- and 12-character identities resolve to the same unique commit");

        fs::write(repo.join("source.txt"), "two\n").expect("source update");
        git(&["add", "source.txt"]);
        git(&["commit", "-m", "two"]);
        let second = git(&["rev-parse", "HEAD"]);
        status.commit = Some(second[..12].to_string());
        let mismatch = validate_rollout_runtime_source_identity(
            "target.test",
            RolloutRuntimeSourceIdentity {
                repo: &repo,
                revision: &first,
            },
            &status,
        )
        .expect_err("textually valid but different commits must fail closed");
        let detail = mismatch.to_string();
        assert!(detail.contains(&first));
        assert!(detail.contains(&second));
    }

    #[test]
    fn explicit_host_status_refresh_routes_by_identity_then_uses_new_listener() {
        let explicit_host = "10.22.0.9";
        let stale_address = "10.22.0.8:21353";
        let mut main = accepted_voter_status(47);
        main.host = explicit_host.to_string();
        main.address = daemon_connect_address(explicit_host, 21353);
        let cluster_target = rollout_cluster_target_from_status(&main).expect("cluster target");
        let original_endpoint =
            ClusterEndpointRecord::loopback(cluster_target.node_id.clone(), stale_address);
        let original_target =
            RolloutTarget::endpoint_for_node(original_endpoint, cluster_target.node_id.clone());
        let mut shadow = main.clone();
        shadow.host = explicit_host.to_string();
        shadow.port = 21354;
        shadow.address = daemon_connect_address(explicit_host, 21354);
        let shadow_cluster = shadow.cluster.as_mut().expect("shadow cluster identity");
        shadow_cluster.process = sinomo_runtime::node::DAEMON_SHADOW_CLUSTER_ROLE.to_string();
        let mut observed_targets = Vec::new();

        let resolved = read_rollout_listener_hosts_after_restart_with(
            &cluster_target,
            &original_target,
            explicit_host,
            21353,
            21354,
            |target| {
                observed_targets.push(target.clone());
                match target.address.as_str() {
                    address if address == cluster_target.node_id.to_string() => {
                        Err(CliError::Network {
                            address: address.to_string(),
                            detail: "main route has not converged yet".to_string(),
                        })
                    }
                    "10.22.0.9:21353" => Ok(main.clone()),
                    "10.22.0.9:21354" => Ok(shadow.clone()),
                    address => panic!("unexpected post-restart target {address}"),
                }
            },
        )
        .expect("new explicit listeners provide direct fallback");

        assert_eq!(resolved.hosts.main, explicit_host);
        assert_eq!(resolved.hosts.shadow.as_deref(), Some(explicit_host));
        assert_eq!(resolved.main_target.address, "10.22.0.9:21353");
        assert!(resolved.shadow_finalization_proven);
        assert!(
            observed_targets
                .iter()
                .all(|target| target.address != stale_address),
            "post-restart convergence must never reuse the stale pre-rollout listener"
        );
        assert_eq!(
            observed_targets,
            vec![
                RolloutTarget::routed_node(cluster_target.node_id.clone()),
                RolloutTarget::address("10.22.0.9:21353".to_string()),
                RolloutTarget::address("10.22.0.9:21354".to_string()),
            ]
        );
    }

    #[test]
    fn explicit_wildcard_status_refresh_never_dials_the_controller_loopback() {
        let explicit_host = "0.0.0.0";
        let original_address = "10.22.0.8:21353";
        let mut main = accepted_voter_status(48);
        main.host = explicit_host.to_string();
        main.address = daemon_connect_address(explicit_host, 21353);
        let cluster_target = rollout_cluster_target_from_status(&main).expect("cluster target");
        let original_endpoint =
            ClusterEndpointRecord::loopback(cluster_target.node_id.clone(), original_address);
        let original_target = RolloutTarget::endpoint_for_node(
            original_endpoint.clone(),
            cluster_target.node_id.clone(),
        );
        let mut observed_targets = Vec::new();

        let resolved = read_rollout_listener_hosts_after_restart_with(
            &cluster_target,
            &original_target,
            explicit_host,
            21353,
            21354,
            |target| {
                observed_targets.push(target.clone());
                if target.address == original_address && target.rollout_node_id().is_none() {
                    Ok(main.clone())
                } else {
                    panic!("wildcard post-restart probe dialed {}", target.address)
                }
            },
        )
        .expect("the preserved direct endpoint proves the restarted listener");

        assert_eq!(resolved.hosts.main, explicit_host);
        assert_eq!(resolved.hosts.shadow, None);
        assert!(resolved.shadow_finalization_proven);
        assert_eq!(
            observed_targets,
            vec![RolloutTarget::explicit_endpoint(original_endpoint)]
        );
    }

    #[test]
    fn wildcard_status_refresh_rejects_wrong_original_identity_then_uses_node_route() {
        let explicit_host = "0.0.0.0";
        let original_address = "10.22.0.8:21353";
        let mut main = accepted_voter_status(49);
        main.host = explicit_host.to_string();
        main.address = daemon_connect_address(explicit_host, 21353);
        let cluster_target = rollout_cluster_target_from_status(&main).expect("cluster target");
        let original_endpoint =
            ClusterEndpointRecord::loopback(cluster_target.node_id.clone(), original_address);
        let original_target = RolloutTarget::endpoint_for_node(
            original_endpoint.clone(),
            cluster_target.node_id.clone(),
        );
        let mut wrong_identity = main.clone();
        wrong_identity
            .cluster
            .as_mut()
            .expect("cluster identity")
            .node_id = NodeId::from_legacy_named("wrong-rollout-node").expect("wrong node id");
        let mut observed_targets = Vec::new();

        let resolved = read_rollout_listener_hosts_after_restart_with(
            &cluster_target,
            &original_target,
            explicit_host,
            21353,
            21354,
            |target| {
                observed_targets.push(target.clone());
                if target.address == original_address && target.rollout_node_id().is_none() {
                    Ok(wrong_identity.clone())
                } else if target.address == cluster_target.node_id.to_string() {
                    Ok(main.clone())
                } else {
                    panic!("wildcard post-restart probe dialed {}", target.address)
                }
            },
        )
        .expect("wrong endpoint identity must fall through to the node route");

        assert_eq!(
            resolved.main_target,
            RolloutTarget::routed_node(cluster_target.node_id.clone())
        );
        assert_eq!(
            observed_targets,
            vec![
                RolloutTarget::explicit_endpoint(original_endpoint),
                RolloutTarget::routed_node(cluster_target.node_id.clone()),
            ]
        );
    }

    #[test]
    fn wildcard_status_refresh_retains_plain_direct_local_target() {
        let explicit_host = "0.0.0.0";
        let original_target = RolloutTarget::address("127.0.0.1:21353".to_string());
        let mut main = accepted_voter_status(50);
        main.host = explicit_host.to_string();
        main.address = daemon_connect_address(explicit_host, 21353);
        let cluster_target = rollout_cluster_target_from_status(&main).expect("cluster target");
        let mut observed_targets = Vec::new();

        let resolved = read_rollout_listener_hosts_after_restart_with(
            &cluster_target,
            &original_target,
            explicit_host,
            21353,
            21354,
            |target| {
                observed_targets.push(target.clone());
                if target == &original_target {
                    Ok(main.clone())
                } else {
                    panic!(
                        "local wildcard post-restart probe dialed {}",
                        target.address
                    )
                }
            },
        )
        .expect("the local controller keeps using its original loopback target");

        assert_eq!(resolved.main_target, original_target.clone());
        assert_eq!(observed_targets, vec![original_target]);
    }

    fn test_timing(timeout: Duration, stable_window: Duration) -> RolloutAuthorityReadinessTiming {
        RolloutAuthorityReadinessTiming {
            timeout,
            stable_window,
            poll_interval: Duration::from_secs(1),
            probe_timeout: Duration::from_secs(1),
        }
    }

    fn authority_readiness_values(
        expected: &RolloutAuthorityTarget,
    ) -> (
        RolloutAuthoritativeRaftConfigRead,
        sinomo_runtime::kv::KvLocalRead<Option<sinomo_runtime::kv::KvEntry>>,
    ) {
        let leader = NodeId::from_legacy_named("node-rollout-leader").expect("leader");
        let third = NodeId::from_legacy_named("node-rollout-third").expect("third voter");
        let config = sinomo_runtime::kv::ClusterKvRaftConfigRecord {
            cluster_id: expected.cluster_id.clone(),
            term: 7,
            kv_revision: 9,
            voters: vec![expected.node_id.clone(), leader.clone(), third],
            learners: Vec::new(),
            non_voting: Vec::new(),
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
        };
        let key = sinomo_runtime::kv::cluster_kv_raft_config_key();
        let entry = sinomo_runtime::kv::KvEntry {
            key,
            value: sinomo_runtime::kv::encode_kv_value(&config).expect("config value"),
            create_revision: sinomo_runtime::kv::KvRevision(3),
            modify_revision: sinomo_runtime::kv::KvRevision(9),
            version: sinomo_runtime::kv::KvVersion(2),
            lease_id: None,
        };
        let authority = KvAuthoritativeRead {
            value: Some(entry.clone()),
            authority: sinomo_runtime::kv::KvAuthorityProof {
                term: 8,
                commit_index: 12,
                revision: sinomo_runtime::kv::KvRevision(9),
                basis: sinomo_runtime::kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
        };
        let voters = config.voters.iter().cloned().collect::<BTreeSet<_>>();
        let local = sinomo_runtime::kv::KvLocalRead {
            value: Some(entry),
            revision: sinomo_runtime::kv::KvRevision(9),
            observed_term: 8,
            applied_index: 12,
            known_leader: Some(leader),
        };
        ((authority, config, voters), local)
    }

    #[test]
    fn production_authority_attempt_budgets_coordinator_and_target_local_reads_separately() {
        let timing = RolloutAuthorityReadinessTiming::production();
        assert_eq!(
            timing.probe_timeout,
            DAEMON_CLUSTER_KV_TXN_TIMEOUT + DAEMON_JOIN_TIMEOUT
        );
        assert_eq!(
            rollout_coordinator_authority_probe_budget(timing.probe_timeout),
            DAEMON_CLUSTER_KV_TXN_TIMEOUT
        );
    }

    #[test]
    fn slow_successful_coordinator_proof_cannot_starve_target_local_comparison() {
        let status = accepted_voter_status(51);
        let expected = rollout_cluster_target_from_status(&status).expect("cluster target");
        let (authoritative, local) = authority_readiness_values(&expected);
        let start = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let local_timeouts = std::cell::RefCell::new(Vec::new());

        probe_rollout_target_cluster_kv_authority_readiness_with(
            &expected,
            DAEMON_CLUSTER_KV_TXN_TIMEOUT + DAEMON_JOIN_TIMEOUT,
            |timeout| {
                assert_eq!(timeout, DAEMON_CLUSTER_KV_TXN_TIMEOUT);
                elapsed.set(elapsed.get() + timeout);
                Ok(authoritative.clone())
            },
            |_, timeout| {
                local_timeouts.borrow_mut().push(timeout);
                Ok(local.clone())
            },
            || start + elapsed.get(),
        )
        .expect("the reserved target-local phase should compare the restarted voter");

        assert_eq!(local_timeouts.into_inner(), vec![DAEMON_JOIN_TIMEOUT]);
    }

    #[test]
    fn legacy_authority_fallback_receives_only_the_exact_reads_remaining_budget() {
        let status = accepted_voter_status(52);
        let expected = rollout_cluster_target_from_status(&status).expect("cluster target");
        let start = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let observed = std::cell::RefCell::new(Vec::new());
        let exact_route = sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE;

        let error = read_rollout_authoritative_raft_config_with(
            &expected,
            Duration::from_secs(8),
            |_, request, timeout| {
                observed.borrow_mut().push((request.route.clone(), timeout));
                if request.route == exact_route {
                    elapsed.set(elapsed.get() + Duration::from_secs(3));
                    return network::ObjectRpcResponse::encode_error(
                        exact_route,
                        format!("unknown object rpc route: {exact_route}"),
                    )
                    .map_err(Into::into);
                }
                Err(CliError::ClusterReadOnly {
                    detail: "stop after observing the legacy timeout".to_string(),
                })
            },
            || start + elapsed.get(),
        )
        .expect_err("the fixture stops after the compatibility request");

        assert!(error.to_string().contains("stop after observing"));
        assert_eq!(
            observed.into_inner(),
            vec![
                (exact_route.to_string(), Duration::from_secs(8)),
                (
                    sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE.to_string(),
                    Duration::from_secs(5),
                ),
            ]
        );
    }

    #[test]
    fn remote_wildcard_authority_probe_remains_node_identity_routed() {
        let status = accepted_voter_status(53);
        let mut expected = rollout_cluster_target_from_status(&status).expect("cluster target");
        expected.node_id = NodeId::from_legacy_named("node-rollout-remote").expect("remote");
        let state = sinomo_runtime::core::ClusterBootstrapState::initialized_locally(
            expected.cluster_id.clone(),
            "rollout test cluster",
            NodeId::from_legacy_named("node-rollout-controller").expect("controller"),
            "controller",
            "controller-key",
            time::OffsetDateTime::UNIX_EPOCH,
        );

        assert_eq!(rollout_restart_direct_probe_address("0.0.0.0", 21353), None);
        assert_eq!(
            rollout_main_rpc_destination(&state, &expected),
            RolloutMainRpcDestination::Routed(expected.node_id)
        );
    }

    #[test]
    fn routed_rollout_mutations_use_the_explicit_local_service_path_for_the_controller_node() {
        let local_node_id = NodeId::from_legacy_named("node-rollout-controller").expect("local");
        let state = sinomo_runtime::core::ClusterBootstrapState::initialized_locally(
            Id::from_legacy_named("cluster-rollout-test").expect("cluster"),
            "rollout test cluster",
            local_node_id.clone(),
            "controller",
            "controller-key",
            time::OffsetDateTime::UNIX_EPOCH,
        );
        let local = RolloutTarget::routed_node(local_node_id);
        let remote_node_id = NodeId::from_legacy_named("node-rollout-remote").expect("remote");
        let remote = RolloutTarget::routed_node(remote_node_id.clone());

        assert_eq!(
            rollout_node_route_for_state(&state, &local),
            RolloutNodeRoute::LocalMain
        );
        assert_eq!(
            rollout_node_route_for_state(&state, &remote),
            RolloutNodeRoute::Routed(remote_node_id)
        );
        assert_eq!(
            rollout_node_route_for_state(&state, &RolloutTarget::address("127.0.0.1:21353".into())),
            RolloutNodeRoute::Direct
        );
    }

    #[test]
    fn authority_rollout_refuses_to_advance_without_live_leader_or_quorum() {
        let status = accepted_voter_status(41);
        let expected = rollout_cluster_target_from_status(&status).expect("cluster target");
        let start = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let authority_probes = Cell::new(0usize);

        let error = wait_for_rollout_authority_readiness_with_probes(
            "oracle.sinomo.net:21353",
            &expected,
            test_timing(Duration::from_secs(3), Duration::from_secs(1)),
            |_| Ok(status.clone()),
            |_| {
                authority_probes.set(authority_probes.get() + 1);
                Err(CliError::ClusterReadOnly {
                    detail: "no live KV leader or voter quorum after restart".to_string(),
                })
            },
            || start + elapsed.get(),
            |duration| elapsed.set(elapsed.get() + duration),
        )
        .expect_err("rollout must stop before the next authority");

        let detail = error.to_string();
        assert!(detail.contains("refusing to advance to another cluster authority"));
        assert!(detail.contains("no live KV leader or voter quorum after restart"));
        assert!(authority_probes.get() >= 2);
    }

    #[test]
    fn authority_rollout_requires_a_stable_readiness_window() {
        let status = accepted_voter_status(42);
        let expected = rollout_cluster_target_from_status(&status).expect("cluster target");
        let start = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let authority_probes = Cell::new(0usize);

        wait_for_rollout_authority_readiness_with_probes(
            "oracle.sinomo.net:21353",
            &expected,
            test_timing(Duration::from_secs(5), Duration::from_secs(2)),
            |_| Ok(status.clone()),
            |_| {
                authority_probes.set(authority_probes.get() + 1);
                Ok(())
            },
            || start + elapsed.get(),
            |duration| elapsed.set(elapsed.get() + duration),
        )
        .expect("stable target should permit the next authority");

        assert_eq!(elapsed.get(), Duration::from_secs(2));
        assert_eq!(authority_probes.get(), 3);
    }

    #[test]
    fn authority_rollout_bounds_status_calls_by_the_remaining_probe_budget() {
        let status = accepted_voter_status(49);
        let expected = rollout_cluster_target_from_status(&status).expect("cluster target");
        let start = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let observed_timeouts = std::cell::RefCell::new(Vec::new());

        wait_for_rollout_authority_readiness_with_probes(
            "oracle.sinomo.net:21353",
            &expected,
            test_timing(Duration::from_millis(1_500), Duration::from_secs(5)),
            |timeout| {
                observed_timeouts.borrow_mut().push(timeout);
                Ok(status.clone())
            },
            |_| {
                Err(CliError::ClusterReadOnly {
                    detail: "not ready".to_string(),
                })
            },
            || start + elapsed.get(),
            |duration| elapsed.set(elapsed.get() + duration),
        )
        .expect_err("the target cannot satisfy the stability window");

        assert_eq!(
            observed_timeouts.into_inner(),
            vec![Duration::from_secs(1), Duration::from_millis(500)]
        );
    }

    #[test]
    fn authority_rollout_rebases_authority_timeout_after_the_status_probe() {
        let status = accepted_voter_status(50);
        let expected = rollout_cluster_target_from_status(&status).expect("cluster target");
        let start = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let status_timeouts = std::cell::RefCell::new(Vec::new());
        let authority_timeouts = std::cell::RefCell::new(Vec::new());

        wait_for_rollout_authority_readiness_with_probes(
            "oracle.sinomo.net:21353",
            &expected,
            test_timing(Duration::from_millis(1_500), Duration::from_secs(5)),
            |timeout| {
                status_timeouts.borrow_mut().push(timeout);
                elapsed.set(elapsed.get() + Duration::from_millis(750));
                Ok(status.clone())
            },
            |timeout| {
                authority_timeouts.borrow_mut().push(timeout);
                Err(CliError::ClusterReadOnly {
                    detail: "not ready".to_string(),
                })
            },
            || start + elapsed.get(),
            |duration| elapsed.set(elapsed.get() + duration),
        )
        .expect_err("the target cannot satisfy the stability window");

        assert_eq!(status_timeouts.into_inner(), vec![Duration::from_secs(1)]);
        assert_eq!(
            authority_timeouts.into_inner(),
            vec![Duration::from_millis(750)]
        );
    }

    #[test]
    fn authority_identity_does_not_trust_reported_role_and_rejects_shadow_status() {
        let mut non_voter = accepted_voter_status(43);
        let cluster = non_voter.cluster.as_mut().expect("cluster status");
        cluster.member_role = ClusterMemberRole::NonVoting;
        cluster.member_state = ClusterMemberState::NonVoting;
        assert!(rollout_cluster_target_from_status(&non_voter).is_some());

        let mut shadow = accepted_voter_status(44);
        let expected = rollout_cluster_target_from_status(&shadow).expect("cluster target");
        shadow.cluster.as_mut().expect("cluster status").process = "daemon-shadow".to_string();
        let detail = rollout_authority_status_pid(&expected, &shadow)
            .expect_err("shadow status cannot satisfy main-daemon readiness");
        assert!(detail.contains(DAEMON_MAIN_CLUSTER_ROLE));
    }

    #[test]
    fn follower_target_accepts_separate_authority_proof_and_rejects_local_mismatch() {
        let status = accepted_voter_status(45);
        let expected = rollout_cluster_target_from_status(&status).expect("cluster target");
        let leader = NodeId::from_legacy_named("node-rollout-leader").expect("leader");
        let third = NodeId::from_legacy_named("node-rollout-third").expect("third voter");
        let config = sinomo_runtime::kv::ClusterKvRaftConfigRecord {
            cluster_id: expected.cluster_id.clone(),
            term: 7,
            kv_revision: 9,
            voters: vec![expected.node_id.clone(), leader.clone(), third],
            learners: Vec::new(),
            non_voting: Vec::new(),
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
        };
        let key = sinomo_runtime::kv::cluster_kv_raft_config_key();
        let entry = sinomo_runtime::kv::KvEntry {
            key: key.clone(),
            value: sinomo_runtime::kv::encode_kv_value(&config).expect("config value"),
            create_revision: sinomo_runtime::kv::KvRevision(3),
            modify_revision: sinomo_runtime::kv::KvRevision(9),
            version: sinomo_runtime::kv::KvVersion(2),
            lease_id: None,
        };
        let authority = KvAuthoritativeRead {
            value: Some(entry.clone()),
            authority: sinomo_runtime::kv::KvAuthorityProof {
                term: 8,
                commit_index: 12,
                revision: sinomo_runtime::kv::KvRevision(9),
                basis: sinomo_runtime::kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
        };
        let voters = config.voters.iter().cloned().collect::<BTreeSet<_>>();
        let mut local = sinomo_runtime::kv::KvLocalRead {
            value: Some(entry),
            revision: sinomo_runtime::kv::KvRevision(9),
            observed_term: 8,
            applied_index: 12,
            known_leader: Some(leader),
        };

        validate_rollout_target_cluster_kv_authority_readiness(
            &expected, &key, &authority, &config, &voters, &local,
        )
        .expect("a caught-up follower may rely on a different configured leader's proof");

        local.revision = sinomo_runtime::kv::KvRevision(17);
        local.applied_index = 20;
        validate_rollout_target_cluster_kv_authority_readiness(
            &expected, &key, &authority, &config, &voters, &local,
        )
        .expect("unrelated revision churn after the proof must not reject a caught-up follower");

        local.applied_index = 11;
        let error = validate_rollout_target_cluster_kv_authority_readiness(
            &expected, &key, &authority, &config, &voters, &local,
        )
        .expect_err("a target behind the authoritative commit must fail closed");
        assert!(error.to_string().contains("has not rejoined"));
    }

    #[test]
    fn explicit_local_node_id_uses_direct_main_daemon_address() {
        let local = NodeId::from_legacy_named("node-rollout-explicit-local").expect("local");
        let remote = NodeId::from_legacy_named("node-rollout-explicit-remote").expect("remote");

        let local_target =
            rollout_target_for_cluster_member(&local, &local, Some("127.0.0.1:21353"));
        assert_eq!(local_target.address, "127.0.0.1:21353");
        assert!(local_target.endpoint.is_none());
        assert!(local_target.rollout_node_id().is_none());

        let remote_target =
            rollout_target_for_cluster_member(&remote, &local, Some("127.0.0.1:21353"));
        assert_eq!(remote_target.rollout_node_id(), Some(&remote));
    }

    #[test]
    fn legacy_authoritative_entry_fallback_is_unknown_route_only() {
        let route = sinomo_runtime::kv::CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE;
        assert!(rollout_authoritative_entry_route_is_unsupported(
            &CliError::Parse {
                detail: format!("unknown object rpc route: {route}"),
            }
        ));
        assert!(rollout_authoritative_entry_route_is_unsupported(
            &CliError::Network {
                address: route.to_string(),
                detail: format!("unknown service route: {route}"),
            }
        ));
        assert!(rollout_authoritative_entry_route_is_unsupported(
            &CliError::Network {
                address: route.to_string(),
                detail: format!("failed to parse command: unknown object rpc route: {route}"),
            }
        ));
        assert!(rollout_authoritative_entry_route_is_unsupported(
            &CliError::Parse {
                detail: format!("failed to parse command: unknown service route: {route}"),
            }
        ));

        for error in [
            CliError::AuthDenied,
            CliError::ClusterReadOnly {
                detail: "no quorum".to_string(),
            },
            CliError::Network {
                address: route.to_string(),
                detail: "idle timeout".to_string(),
            },
            CliError::Parse {
                detail: "unknown object rpc route: /some/other/route".to_string(),
            },
        ] {
            assert!(
                !rollout_authoritative_entry_route_is_unsupported(&error),
                "must not use legacy prefix fallback for {error}"
            );
        }
    }

    #[test]
    fn authority_readiness_wall_clock_guard_times_out_a_blocked_status_probe() {
        let status = accepted_voter_status(47);
        let expected = rollout_cluster_target_from_status(&status).expect("cluster target");
        let expected_for_worker = expected.clone();
        let started = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started_for_worker = std::sync::Arc::clone(&started);
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = std::sync::mpsc::sync_channel(0);

        let wall_started = Instant::now();
        let error = run_rollout_authority_readiness_with_wall_clock_guard(
            "windows.sinomo.test:21353",
            &expected.node_id,
            Duration::from_millis(50),
            move || {
                let result = wait_for_rollout_authority_readiness_with_probes(
                    "windows.sinomo.test:21353",
                    &expected_for_worker,
                    test_timing(Duration::from_secs(1), Duration::ZERO),
                    |_| {
                        started_for_worker.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        release_rx.recv().expect("release blocked status probe");
                        Ok(status.clone())
                    },
                    |_| Ok(()),
                    Instant::now,
                    thread::sleep,
                );
                let _ = finished_tx.send(());
                result
            },
        )
        .expect_err("the wall-clock guard must bound a blocked status probe");

        assert!(wall_started.elapsed() < Duration::from_secs(1));
        assert!(error.to_string().contains("wall-clock deadline"));
        release_tx.send(()).expect("release status worker");
        finished_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("status worker should exit after release");
        assert_eq!(started.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn authority_readiness_wall_clock_guard_times_out_a_blocked_authority_probe() {
        let status = accepted_voter_status(48);
        let expected = rollout_cluster_target_from_status(&status).expect("cluster target");
        let expected_for_worker = expected.clone();
        let started = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started_for_worker = std::sync::Arc::clone(&started);
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = std::sync::mpsc::sync_channel(0);

        let wall_started = Instant::now();
        let error = run_rollout_authority_readiness_with_wall_clock_guard(
            "windows.sinomo.test:21353",
            &expected.node_id,
            Duration::from_millis(50),
            move || {
                let result = wait_for_rollout_authority_readiness_with_probes(
                    "windows.sinomo.test:21353",
                    &expected_for_worker,
                    test_timing(Duration::from_secs(1), Duration::ZERO),
                    |_| Ok(status.clone()),
                    |_| {
                        started_for_worker.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        release_rx.recv().expect("release blocked authority probe");
                        Ok(())
                    },
                    Instant::now,
                    thread::sleep,
                );
                let _ = finished_tx.send(());
                result
            },
        )
        .expect_err("the wall-clock guard must bound a blocked authority probe");

        assert!(wall_started.elapsed() < Duration::from_secs(1));
        assert!(error.to_string().contains("wall-clock deadline"));
        release_tx.send(()).expect("release authority worker");
        finished_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("authority worker should exit after release");
        assert_eq!(started.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn authority_readiness_wall_clock_guard_propagates_fast_success() {
        let node_id = NodeId::from_legacy_named("node-rollout-guard-success").expect("node id");
        run_rollout_authority_readiness_with_wall_clock_guard(
            "127.0.0.1:21353",
            &node_id,
            Duration::from_secs(1),
            || Ok(()),
        )
        .expect("a timely readiness result should pass through the guard");
    }

    #[test]
    fn authority_readiness_wall_clock_guard_propagates_worker_error() {
        let node_id = NodeId::from_legacy_named("node-rollout-guard-error").expect("node id");
        let error = run_rollout_authority_readiness_with_wall_clock_guard(
            "127.0.0.1:21353",
            &node_id,
            Duration::from_secs(1),
            || {
                Err(CliError::ClusterReadOnly {
                    detail: "worker readiness proof failed".to_string(),
                })
            },
        )
        .expect_err("the worker error should pass through unchanged");
        assert!(error.to_string().contains("worker readiness proof failed"));
    }

    #[test]
    fn indeterminate_send_boundary_stops_before_the_next_voter() {
        let voter = accepted_voter_status(46);
        let authority = rollout_cluster_target_from_status(&voter).expect("cluster target");
        assert!(rollout_indeterminate_activation_requires_stop(Some(
            &authority
        )));
        assert!(!rollout_indeterminate_activation_requires_stop(None));

        let rejected = ClusterRolloutBinaryResponse {
            status: "rolled-back".to_string(),
            detail: "rollback reported without proof".to_string(),
            main_binary: PathBuf::new(),
            shadow_binary: PathBuf::new(),
            source_head: None,
        };
        assert!(rejected_rollout_response_requires_authority_stop(
            Some(&authority),
            &rejected
        ));
        assert!(!rejected_rollout_response_requires_authority_stop(
            None, &rejected
        ));
    }
}

fn send_rollout_object_rpc_value<Req, Resp>(
    target: &RolloutTarget,
    route: &str,
    category: &str,
    request: &Req,
    timeout: Duration,
) -> Result<Resp, CliError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    send_rollout_object_rpc_value_with_options(
        target,
        route,
        category,
        request,
        rollout_object_rpc_options(timeout),
    )
}

fn rollout_object_rpc_options(timeout: Duration) -> network::ClusterObjectRpcOptions {
    let timeouts = network::DaemonRpcTimeouts::connect_and_idle(timeout);
    network::ClusterObjectRpcOptions::new(timeouts).with_overall_timeout(timeout)
}

#[cfg(test)]
#[test]
fn rollout_object_rpc_budget_is_an_absolute_deadline_not_only_an_idle_timeout() {
    let budget = Duration::from_secs(2);
    assert_eq!(
        rollout_object_rpc_options(budget).overall_timeout(),
        Some(budget)
    );
}

fn send_rollout_object_rpc_value_with_options<Req, Resp>(
    target: &RolloutTarget,
    route: &str,
    category: &str,
    request: &Req,
    options: network::ClusterObjectRpcOptions,
) -> Result<Resp, CliError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    let request = network::ObjectRpcRequest::encode_body(route, category, request)?;
    let response = send_rollout_object_rpc_with_options(target, &request, options)?;
    response.decode_body().map_err(CliError::from)
}

fn send_rollout_object_rpc_with_options(
    target: &RolloutTarget,
    request: &network::ObjectRpcRequest,
    options: network::ClusterObjectRpcOptions,
) -> Result<network::ObjectRpcResponse, CliError> {
    match resolve_rollout_node_dispatch(target)? {
        RolloutNodeDispatch::LocalMain(status) => {
            return sinomo_runtime::daemon::send_daemon_rpc_with_options(
                &status.address,
                request,
                options,
            )
            .map_err(Into::into);
        }
        RolloutNodeDispatch::Routed(node_id) => {
            let options = options
                .with_destination_process_affinity(network::DestinationProcessAffinity::MainDaemon);
            sinomo_runtime::daemon::send_object_rpc_to_target_with_options(
                &node_id, request, options, true,
            )
            .map_err(Into::into)
        }
        RolloutNodeDispatch::Direct => {
            if let Some(endpoint) = &target.endpoint {
                sinomo_runtime::daemon::send_daemon_rpc_to_endpoint_with_options(
                    endpoint, request, options,
                )
                .map_err(Into::into)
            } else {
                sinomo_runtime::daemon::send_daemon_rpc_with_options(
                    &target.address,
                    request,
                    options,
                )
                .map_err(Into::into)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RolloutNodeDispatch {
    LocalMain(DaemonStatus),
    Routed(NodeId),
    Direct,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RolloutNodeRoute {
    LocalMain,
    Routed(NodeId),
    Direct,
}

fn rollout_node_route_for_state(
    state: &sinomo_runtime::core::ClusterBootstrapState,
    target: &RolloutTarget,
) -> RolloutNodeRoute {
    match target.rollout_node_id() {
        Some(node_id) if state.local_node_id == *node_id => RolloutNodeRoute::LocalMain,
        Some(node_id) => RolloutNodeRoute::Routed(node_id.clone()),
        None => RolloutNodeRoute::Direct,
    }
}

fn resolve_rollout_node_dispatch(target: &RolloutTarget) -> Result<RolloutNodeDispatch, CliError> {
    if target.rollout_node_id().is_none() {
        return Ok(RolloutNodeDispatch::Direct);
    }
    let paths = cluster_paths_from_defaults()?;
    let state = cluster::read_cluster_state_without_kv(&paths)?;
    let expected_node_id = match rollout_node_route_for_state(&state, target) {
        RolloutNodeRoute::Direct => return Ok(RolloutNodeDispatch::Direct),
        RolloutNodeRoute::Routed(node_id) => return Ok(RolloutNodeDispatch::Routed(node_id)),
        RolloutNodeRoute::LocalMain => target
            .rollout_node_id()
            .expect("local rollout node route has a node id"),
    };
    let status = default_main_daemon_status()?.ok_or_else(|| CliError::Network {
        address: expected_node_id.to_string(),
        detail: "local-node rollout target requires the local main daemon".to_string(),
    })?;
    let cluster = status.cluster.as_ref().ok_or_else(|| CliError::Network {
        address: status.address.clone(),
        detail: "local-node rollout target reached a daemon without cluster identity".to_string(),
    })?;
    if cluster.cluster_id != state.cluster_id
        || cluster.node_id != *expected_node_id
        || cluster.process != DAEMON_MAIN_CLUSTER_ROLE
    {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "local-node rollout target expected main daemon {}/{expected_node_id}, but reached {}/{} ({})",
                state.cluster_id, cluster.cluster_id, cluster.node_id, cluster.process
            ),
        });
    }
    Ok(RolloutNodeDispatch::LocalMain(status))
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedClusterBinaryRollout {
    manifest: ClusterRolloutBinaryManifest,
    artifacts: Vec<RolloutUploadSource>,
}

#[derive(Clone, Debug)]
pub(crate) struct RolloutUploadSource {
    artifact_id: String,
    path: PathBuf,
}

pub(crate) fn rollout_target_platform_matches_local(status: &DaemonStatus) -> Option<bool> {
    sinomo_runtime::rollout::rollout_target_platform_matches_local(
        status.target_os.as_deref(),
        status.target_arch.as_deref(),
    )
}

pub(crate) fn ensure_rollout_target_platform_from_status(
    address: &str,
    status: &DaemonStatus,
) -> Result<(), CliError> {
    sinomo_runtime::rollout::ensure_rollout_target_platform_matches_local(
        address,
        status.target_os.as_deref(),
        status.target_arch.as_deref(),
    )
    .map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_cluster_binary_rollout_for_target(
    source: String,
    binary: &Path,
    native_dylib_modules: &[LocalRolloutNativeDylibModuleArtifact],
    skip_native_modules: bool,
    target_os: String,
    target_arch: String,
    host: String,
    port: u16,
    shadow_port: u16,
    requested_at: String,
) -> Result<PreparedClusterBinaryRollout, CliError> {
    let native_dylib_modules = if skip_native_modules {
        &[]
    } else {
        native_dylib_modules
    };
    let manifest = prepare_cluster_rollout_binary_manifest_for_target(
        source,
        binary,
        native_dylib_modules,
        target_os,
        target_arch,
        host,
        port,
        shadow_port,
        requested_at,
    )?;
    let mut artifacts = vec![RolloutUploadSource {
        artifact_id: manifest.binary.artifact_id.clone(),
        path: binary.to_path_buf(),
    }];
    for (module, spec) in native_dylib_modules
        .iter()
        .zip(&manifest.native_dylib_modules)
    {
        artifacts.push(RolloutUploadSource {
            artifact_id: spec.artifact.artifact_id.clone(),
            path: module.built_library.clone(),
        });
    }
    Ok(PreparedClusterBinaryRollout {
        manifest,
        artifacts,
    })
}

fn send_prepared_daemon_rollout_binary(
    target: &RolloutTarget,
    prepared: &PreparedClusterBinaryRollout,
    expected: &RolloutAuthorityTarget,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    let expectation = RolloutProgressExpectation {
        source: prepared.manifest.source.clone(),
        requested_at: prepared.manifest.requested_at.clone(),
        source_head: None,
    };
    let restart_evidence = rollout_expected_restart_evidence(
        target,
        expected,
        prepared.manifest.host_override.as_deref(),
        prepared.manifest.port,
        prepared.manifest.shadow_port,
    );
    for (index, artifact) in prepared.artifacts.iter().enumerate() {
        let activate = index + 1 == prepared.artifacts.len();
        let request = ClusterRolloutBinaryRequest {
            manifest: prepared.manifest.clone(),
            artifact_id: artifact.artifact_id.clone(),
            activate,
        };
        if activate {
            let path = artifact.path.clone();
            return send_rollout_request_with_progress(
                target,
                "binary rollout",
                expectation,
                restart_evidence.clone(),
                move |target| send_daemon_rollout_binary_direct(&target, &request, &path),
            );
        }
        println!(
            "{}: uploading rollout artifact {}",
            target.label(),
            artifact.artifact_id
        );
        let response = send_daemon_rollout_binary_direct(target, &request, &artifact.path)?;
        if response.status == "accepted" {
            return wait_for_accepted_rollout_response_with_progress(
                target,
                "binary rollout",
                expectation.clone(),
                restart_evidence.clone(),
                response,
            );
        }
        if response.status != "staged" {
            return Ok(response);
        }
    }
    Err(CliError::Network {
        address: target.label().to_string(),
        detail: "rollout manifest contains no artifacts".to_string(),
    })
}

fn send_daemon_rollout_binary_direct(
    target: &RolloutTarget,
    request: &ClusterRolloutBinaryRequest,
    attachment_path: &Path,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    match resolve_rollout_node_dispatch(target)? {
        RolloutNodeDispatch::LocalMain(status) => {
            daemon_send_daemon_rollout_binary(&status.address, request, attachment_path)
                .map_err(Into::into)
        }
        RolloutNodeDispatch::Routed(node_id) => {
            daemon_send_daemon_rollout_binary_to_target(&node_id, request, attachment_path)
                .map_err(Into::into)
        }
        RolloutNodeDispatch::Direct => {
            if let Some(endpoint) = &target.endpoint {
                daemon_send_daemon_rollout_binary_to_endpoint(endpoint, request, attachment_path)
                    .map_err(Into::into)
            } else {
                daemon_send_daemon_rollout_binary(&target.address, request, attachment_path)
                    .map_err(Into::into)
            }
        }
    }
}

pub(crate) fn send_daemon_rollout_native_dylib_module(
    target: &RolloutTarget,
    request: Arc<ClusterRolloutNativeDylibModuleRequest>,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    match resolve_rollout_node_dispatch(target)? {
        RolloutNodeDispatch::LocalMain(status) => daemon_send_daemon_rollout_native_dylib_module(
            &status.address,
            request,
            DAEMON_ROLLOUT_TIMEOUT,
        )
        .map_err(Into::into),
        RolloutNodeDispatch::Routed(node_id) => {
            daemon_send_daemon_rollout_native_dylib_module_to_target(
                &node_id,
                request,
                DAEMON_ROLLOUT_TIMEOUT,
            )
            .map_err(Into::into)
        }
        RolloutNodeDispatch::Direct => {
            if let Some(endpoint) = &target.endpoint {
                daemon_send_daemon_rollout_native_dylib_module_to_endpoint(
                    endpoint,
                    request,
                    DAEMON_ROLLOUT_TIMEOUT,
                )
                .map_err(Into::into)
            } else {
                daemon_send_daemon_rollout_native_dylib_module(
                    &target.address,
                    request,
                    DAEMON_ROLLOUT_TIMEOUT,
                )
                .map_err(Into::into)
            }
        }
    }
}

pub(crate) fn send_daemon_rollout_clear_status(target: &RolloutTarget) -> Result<String, CliError> {
    if target.rollout_node_id().is_some() {
        send_rollout_object_rpc_value(
            target,
            sinomo_runtime::rollout::ROLLOUT_CLEAR_STATUS_ROUTE,
            sinomo_runtime::rollout::ROLLOUT_RPC_CATEGORY,
            &(),
            DAEMON_CLUSTER_KV_TXN_TIMEOUT,
        )
    } else if let Some(endpoint) = &target.endpoint {
        daemon_send_daemon_rollout_clear_status_to_endpoint(endpoint, DAEMON_CLUSTER_KV_TXN_TIMEOUT)
            .map_err(Into::into)
    } else {
        daemon_send_daemon_rollout_clear_status(&target.address, DAEMON_CLUSTER_KV_TXN_TIMEOUT)
            .map_err(Into::into)
    }
}

pub(crate) fn send_daemon_rollout_diagnostics(target: &RolloutTarget) -> Result<String, CliError> {
    if target.rollout_node_id().is_some() {
        send_rollout_object_rpc_value(
            target,
            sinomo_runtime::rollout::ROLLOUT_DIAGNOSTICS_ROUTE,
            sinomo_runtime::rollout::ROLLOUT_RPC_CATEGORY,
            &(),
            CLUSTER_HEALTH_PROBE_TIMEOUT,
        )
    } else if let Some(endpoint) = &target.endpoint {
        daemon_send_daemon_rollout_diagnostics_to_endpoint(endpoint, CLUSTER_HEALTH_PROBE_TIMEOUT)
            .map_err(Into::into)
    } else {
        daemon_send_daemon_rollout_diagnostics(&target.address, CLUSTER_HEALTH_PROBE_TIMEOUT)
            .map_err(Into::into)
    }
}

fn send_daemon_rollout_build(
    target: &RolloutTarget,
    request: &ClusterRolloutBuildRequest,
    expected: &RolloutAuthorityTarget,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    let request = request.clone();
    let expectation = RolloutProgressExpectation {
        source: request.source.clone(),
        requested_at: request.requested_at.clone(),
        source_head: request.source_head.clone(),
    };
    let restart_evidence = rollout_expected_restart_evidence(
        target,
        expected,
        request.host_override.as_deref(),
        request.port,
        request.shadow_port,
    );
    send_rollout_request_with_progress(
        target,
        "remote build",
        expectation,
        restart_evidence,
        move |target| send_daemon_rollout_build_direct(&target, &request),
    )
}

fn send_daemon_rollout_build_direct(
    target: &RolloutTarget,
    request: &ClusterRolloutBuildRequest,
) -> Result<ClusterRolloutBinaryResponse, CliError> {
    match resolve_rollout_node_dispatch(target)? {
        RolloutNodeDispatch::LocalMain(status) => {
            daemon_send_daemon_rollout_build(&status.address, request).map_err(Into::into)
        }
        RolloutNodeDispatch::Routed(node_id) => {
            daemon_send_daemon_rollout_build_to_target(&node_id, request).map_err(Into::into)
        }
        RolloutNodeDispatch::Direct => {
            if let Some(endpoint) = &target.endpoint {
                daemon_send_daemon_rollout_build_to_endpoint(endpoint, request).map_err(Into::into)
            } else {
                daemon_send_daemon_rollout_build(&target.address, request).map_err(Into::into)
            }
        }
    }
}

pub(crate) fn local_rollout_wait_timeout() -> Duration {
    DAEMON_ROLLOUT_TIMEOUT
}

pub(crate) fn wait_for_main_daemon_health(
    options: &RolloutOptions,
    previous_pid: Option<u32>,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let status = daemon_wait_for_main_daemon_health(
        &data_dir.root(),
        &options.host,
        options.port,
        previous_pid,
        local_rollout_wait_timeout(),
        DAEMON_PROBE_TIMEOUT,
    )?;
    println!(
        "main daemon healthy: pid={} bind={} control={}",
        status.pid,
        daemon_bind_address(&status),
        status.address
    );
    Ok(())
}

pub(crate) fn wait_for_local_shadow_rollout_completion(
    data_dir: &SinomoDataDir,
    options: &RolloutOptions,
) -> Result<(), CliError> {
    daemon_wait_for_local_shadow_rollout_completion(
        &data_dir.root(),
        &options.host,
        options.port,
        &options.host,
        options.shadow_port,
        local_rollout_wait_timeout(),
        DAEMON_PROBE_TIMEOUT,
    )
    .map_err(Into::into)
}

use sinomo_runtime::cluster;
