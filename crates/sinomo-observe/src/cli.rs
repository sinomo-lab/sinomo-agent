use crate::prometheus_server::{
    PROMETHEUS_DEFAULT_LISTEN, PROMETHEUS_DEFAULT_REFRESH_SECONDS, PROMETHEUS_MAX_REFRESH_SECONDS,
    PROMETHEUS_MIN_REFRESH_SECONDS, PrometheusCollector, PrometheusHttpServer, PrometheusShutdown,
    install_prometheus_shutdown_signals,
};
use crate::{
    OBSERVE_CONTROL_ROUTE, OBSERVE_CPU_PROFILE_ROUTE, OBSERVE_EVENTS_ROUTE, OBSERVE_LOGS_ROUTE,
    OBSERVE_METRICS_ROUTE, OBSERVE_NETWORK_TIMELINE_ROUTE, OBSERVE_RPC_CATEGORY,
    OBSERVE_TRACES_ROUTE, ObserveControlRequest, ObserveControlResponse, ObserveCpuProfileAction,
    ObserveCpuProfileRequest, ObserveCpuProfileResponse, ObserveEventsRequest,
    ObserveEventsResponse, ObserveLogsRequest, ObserveLogsResponse, ObserveMetricAggregation,
    ObserveMetricDescriptor, ObserveMetricRecord, ObserveMetricSemantics, ObserveMetricUnit,
    ObserveMetricsRequest, ObserveMetricsResponse, ObserveNetworkTimelineAction,
    ObserveNetworkTimelineRequest, ObserveNetworkTimelineResponse, ObserveTracesRequest,
    ObserveTracesResponse, OperationalEventQuery, OperationalEventRecord, OperationalLogQuery,
    OperationalLogRecord, handle_observe_cpu_profile, handle_observe_network_timeline,
    observe_control_snapshot, observe_module_enabled, read_observe_metrics, read_observe_traces,
    read_operational_events, read_operational_logs, update_observe_control,
};
use sinomo_cli_registry::{
    CliAutocompleteOption, CliAutocompleteSpec, CliAutocompleteValue, CliCommand,
    CliCommandContext, CliCommandError, CliCommandHandler, CliCommandHelp, CliCommandMetadata,
    CliCommandPolicy, CliParseError,
};
use sinomo_core::local::SinomoDataDir;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use time::format_description::well_known::Rfc3339;

const OBSERVE_DEFAULT_LIMIT: usize = 40;
const OBSERVE_FANOUT_MAX_WORKERS: usize = 8;
const OBSERVE_POLICY: CliCommandPolicy = CliCommandPolicy::local_capable();

#[derive(Debug, Clone, Eq, PartialEq)]
enum ObserveCliCommand {
    Help,
    Events(ObserveOptions),
    Logs(ObserveOptions),
    Metrics(ObserveOptions),
    Prometheus(ObservePrometheusOptions),
    Traces(ObserveOptions),
    Control(ObserveControlOptions),
    CpuProfile(ObserveCpuProfileOptions),
    NetworkTimeline(ObserveNetworkTimelineOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ObservePrometheusOptions {
    listen: SocketAddr,
    refresh_interval_seconds: u64,
    name_prefix: Option<String>,
    target_node_id: Option<String>,
    target_all: bool,
}

impl Default for ObservePrometheusOptions {
    fn default() -> Self {
        Self {
            listen: PROMETHEUS_DEFAULT_LISTEN
                .parse()
                .expect("default Prometheus exporter address is valid"),
            refresh_interval_seconds: PROMETHEUS_DEFAULT_REFRESH_SECONDS,
            name_prefix: None,
            target_node_id: None,
            target_all: false,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ObserveOptions {
    kind_prefix: Option<String>,
    component: Option<String>,
    operation_id: Option<String>,
    name_prefix: Option<String>,
    target_node_id: Option<String>,
    target_all: bool,
    traceparent: Option<String>,
    after_sequence: Option<u64>,
    capture_id: Option<String>,
    limit: usize,
    json: bool,
    watch_interval_seconds: Option<u64>,
    watch_samples: Option<usize>,
}

impl Default for ObserveOptions {
    fn default() -> Self {
        Self {
            kind_prefix: None,
            component: None,
            operation_id: None,
            name_prefix: None,
            target_node_id: None,
            target_all: false,
            traceparent: None,
            after_sequence: None,
            capture_id: None,
            limit: OBSERVE_DEFAULT_LIMIT,
            json: false,
            watch_interval_seconds: None,
            watch_samples: None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ObserveControlOptions {
    live_log_writing: Option<bool>,
    filter: Option<String>,
    quiet_filter_directives: Vec<String>,
    clear_quiet_filter_directives: bool,
    reset: bool,
    capture_id: Option<String>,
    clear_capture: bool,
    expected_capture_id: Option<String>,
    clear_traces: bool,
    target_node_id: Option<String>,
    target_all: bool,
    json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ObserveCpuProfileOptions {
    request: ObserveCpuProfileRequest,
    target_node_id: Option<String>,
    json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ObserveNetworkTimelineOptions {
    request: ObserveNetworkTimelineRequest,
    target_node_id: Option<String>,
    json: bool,
}

type ObserveCliRpc = Arc<
    dyn Fn(
            Option<&str>,
            sinomo_core::network::ObjectRpcRequest,
        ) -> Result<sinomo_core::network::ObjectRpcResponse, String>
        + Send
        + Sync,
>;
type ObserveCliTargetLister = Arc<dyn Fn() -> Result<Vec<String>, String> + Send + Sync>;

#[derive(Default, Clone)]
pub struct ObserveCliCommandHandler {
    rpc: Option<ObserveCliRpc>,
    target_lister: Option<ObserveCliTargetLister>,
}

pub fn observe_cli_command_handler() -> ObserveCliCommandHandler {
    ObserveCliCommandHandler {
        rpc: None,
        target_lister: None,
    }
}

pub fn observe_cli_command_handler_with_rpc(
    rpc: impl Fn(
        Option<&str>,
        sinomo_core::network::ObjectRpcRequest,
    ) -> Result<sinomo_core::network::ObjectRpcResponse, String>
    + Send
    + Sync
    + 'static,
) -> ObserveCliCommandHandler {
    ObserveCliCommandHandler {
        rpc: Some(Arc::new(rpc)),
        target_lister: None,
    }
}

pub fn observe_cli_command_handler_with_cluster_rpc(
    rpc: impl Fn(
        Option<&str>,
        sinomo_core::network::ObjectRpcRequest,
    ) -> Result<sinomo_core::network::ObjectRpcResponse, String>
    + Send
    + Sync
    + 'static,
    target_lister: impl Fn() -> Result<Vec<String>, String> + Send + Sync + 'static,
) -> ObserveCliCommandHandler {
    ObserveCliCommandHandler {
        rpc: Some(Arc::new(rpc)),
        target_lister: Some(Arc::new(target_lister)),
    }
}

impl CliCommandHandler for ObserveCliCommandHandler {
    fn metadata(&self) -> CliCommandMetadata {
        observe_cli_metadata()
    }

    fn help(&self) -> CliCommandHelp {
        observe_cli_help()
    }

    fn autocomplete(&self) -> CliAutocompleteSpec {
        observe_cli_autocomplete()
    }

    fn parse(&self, args: &[String]) -> Result<Box<dyn CliCommand>, CliParseError> {
        Ok(Box::new(ObserveCliCommandRunner {
            command: parse_observe_command(args)?,
            rpc: self.rpc.clone(),
            target_lister: self.target_lister.clone(),
        }))
    }
}

struct ObserveCliCommandRunner {
    command: ObserveCliCommand,
    rpc: Option<ObserveCliRpc>,
    target_lister: Option<ObserveCliTargetLister>,
}

impl CliCommand for ObserveCliCommandRunner {
    fn name(&self) -> &'static str {
        "observe"
    }

    fn policy(&self) -> CliCommandPolicy {
        OBSERVE_POLICY
    }

    fn is_help(&self) -> bool {
        matches!(self.command, ObserveCliCommand::Help)
    }

    fn help(&self) -> CliCommandHelp {
        observe_cli_help()
    }

    fn autocomplete(&self) -> CliAutocompleteSpec {
        observe_cli_autocomplete()
    }

    fn run(self: Box<Self>, context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError> {
        match self.command {
            ObserveCliCommand::Help => self.help().write(context),
            ObserveCliCommand::Events(options) => observe_events(
                context,
                self.rpc.as_ref(),
                self.target_lister.as_ref(),
                options,
            ),
            ObserveCliCommand::Logs(options) => observe_logs(
                context,
                self.rpc.as_ref(),
                self.target_lister.as_ref(),
                options,
            ),
            ObserveCliCommand::Metrics(options) => observe_metrics(
                context,
                self.rpc.as_ref(),
                self.target_lister.as_ref(),
                options,
            ),
            ObserveCliCommand::Prometheus(options) => observe_prometheus(
                context,
                self.rpc.as_ref(),
                self.target_lister.as_ref(),
                options,
            ),
            ObserveCliCommand::Traces(options) => observe_traces(
                context,
                self.rpc.as_ref(),
                self.target_lister.as_ref(),
                options,
            ),
            ObserveCliCommand::Control(options) => observe_control(
                context,
                self.rpc.as_ref(),
                self.target_lister.as_ref(),
                options,
            ),
            ObserveCliCommand::CpuProfile(options) => {
                observe_cpu_profile(context, self.rpc.as_ref(), options)
            }
            ObserveCliCommand::NetworkTimeline(options) => {
                observe_network_timeline(context, self.rpc.as_ref(), options)
            }
        }
    }
}

fn observe_cli_metadata() -> CliCommandMetadata {
    CliCommandMetadata::new(
        "observe",
        &["obs"],
        "Read and control operational observation.",
        OBSERVE_POLICY,
    )
}

fn observe_cli_help() -> CliCommandHelp {
    CliCommandHelp::new(
        "Observe:",
        [
            "  sinomo observe",
            "      Show observation commands.",
            "  sinomo observe events [--kind <prefix>] [--component <name>] [--operation <id>] [--target <node-id> | --all] [--limit <n>] [--json]",
            "      Read local daemon/shadow operational events from the data directory.",
            "  sinomo observe logs [--component <name>] [--operation <id>] [--target <node-id> | --all] [--limit <n>] [--json]",
            "      Read local daemon/shadow operational logs from the data directory.",
            "  sinomo observe metrics [--name <prefix>] [--target <node-id> | --all] [--limit <n>] [--watch <seconds> [--samples <n>]] [--json]",
            "      Read live runtime metrics.",
            "  sinomo observe prometheus serve [--listen <loopback-ip:port>] [--refresh <seconds>] [--name <prefix>] [--target <node-id> | --all]",
            "      Serve cached live metrics for Prometheus; use Prometheus or Grafana for durable history.",
            "  sinomo observe traces [--target-prefix <prefix>] [--traceparent <id>] [--after-sequence <n>] [--capture <id>] [--target <node-id> | --all] [--limit <n>] [--json]",
            "      Read live bounded trace events.",
            "  sinomo observe control [--live-log-writing | --no-live-log-writing] [--filter <directive>] [--quiet-filter <directive>...] [--capture <id> | --clear-capture --expected-capture <id>] [--clear-traces] [--clear-quiet-filters | --reset] [--target <node-id> | --all] [--json]",
            "      Read or update live tracing; capture-owned filters restore on clear or after the ten-minute lease.",
            "      Routine text writing is opt-in; disabling it never dumps buffered history. Export logs/traces with --json.",
            "  sinomo observe profile cpu start [--capture <id>] [--duration-seconds <1..30>] [--frequency-hz <10..500>] [--target <node-id>] [--json]",
            "  sinomo observe profile cpu status [--target <node-id>] [--json]",
            "  sinomo observe profile cpu stop [--capture <id>] [--target <node-id>] [--json]",
            "      Control one node-local, bounded CPU profile and report its pprof artifact path.",
            "  sinomo observe timeline network start [--capture <id>] [--duration-seconds <1..120>] [--max-events <n>] [--target <node-id>] [--json]",
            "  sinomo observe timeline network status [--target <node-id>] [--json]",
            "  sinomo observe timeline network stop [--capture <id>] [--target <node-id>] [--json]",
            "      Capture one node-local, fixed-memory RPC timeline; stop emits Perfetto-convertible JSON.",
        ],
    )
}

fn observe_cli_autocomplete() -> CliAutocompleteSpec {
    CliAutocompleteSpec::new("observe", "inspect local operational events and logs")
        .with_aliases(["obs"])
        .with_children([
            CliAutocompleteSpec::new("events", "read operational events").with_options([
                CliAutocompleteOption::with_value(
                    "--kind",
                    "event kind prefix",
                    CliAutocompleteValue::new("prefix", "event kind prefix", []),
                ),
                CliAutocompleteOption::with_value(
                    "--component",
                    "component name",
                    CliAutocompleteValue::new("name", "component name", []),
                ),
                CliAutocompleteOption::with_value(
                    "--operation",
                    "operation id",
                    CliAutocompleteValue::new("id", "operation id", []),
                ),
                CliAutocompleteOption::with_value(
                    "--limit",
                    "maximum records to return",
                    CliAutocompleteValue::new("n", "record limit", []),
                ),
                CliAutocompleteOption::with_value(
                    "--watch",
                    "emit interval metric samples",
                    CliAutocompleteValue::new("seconds", "sample interval in seconds", []),
                ),
                CliAutocompleteOption::with_value(
                    "--samples",
                    "stop after this many watch samples",
                    CliAutocompleteValue::new("n", "positive sample count", []),
                ),
                CliAutocompleteOption::flag("--all", "query every cluster node"),
                CliAutocompleteOption::flag("--json", "emit JSON"),
            ]),
            CliAutocompleteSpec::new("logs", "read operational logs").with_options([
                CliAutocompleteOption::with_value(
                    "--component",
                    "component name",
                    CliAutocompleteValue::new("name", "component name", []),
                ),
                CliAutocompleteOption::with_value(
                    "--operation",
                    "operation id",
                    CliAutocompleteValue::new("id", "operation id", []),
                ),
                CliAutocompleteOption::with_value(
                    "--limit",
                    "maximum records to return",
                    CliAutocompleteValue::new("n", "record limit", []),
                ),
                CliAutocompleteOption::flag("--all", "query every cluster node"),
                CliAutocompleteOption::flag("--json", "emit JSON"),
            ]),
            CliAutocompleteSpec::new("metrics", "read live metrics").with_options([
                CliAutocompleteOption::with_value(
                    "--name",
                    "metric name prefix",
                    CliAutocompleteValue::new("prefix", "metric name prefix", []),
                ),
                CliAutocompleteOption::with_value(
                    "--target",
                    "target node id",
                    CliAutocompleteValue::new("node-id", "target node id", []),
                ),
                CliAutocompleteOption::with_value(
                    "--limit",
                    "maximum records to return",
                    CliAutocompleteValue::new("n", "record limit", []),
                ),
                CliAutocompleteOption::flag("--all", "query every cluster node"),
                CliAutocompleteOption::flag("--json", "emit JSON"),
            ]),
            CliAutocompleteSpec::new("prometheus", "serve a Prometheus metrics bridge")
                .with_children([CliAutocompleteSpec::new(
                    "serve",
                    "serve cached OpenMetrics over loopback HTTP",
                )
                .with_options([
                    CliAutocompleteOption::with_value(
                        "--listen",
                        "loopback listen address",
                        CliAutocompleteValue::new("ip:port", "loopback IP address and port", []),
                    ),
                    CliAutocompleteOption::with_value(
                        "--refresh",
                        "metric collection interval",
                        CliAutocompleteValue::new("seconds", "refresh interval in seconds", []),
                    ),
                    CliAutocompleteOption::with_value(
                        "--name",
                        "metric name prefix",
                        CliAutocompleteValue::new("prefix", "metric name prefix", []),
                    ),
                    CliAutocompleteOption::with_value(
                        "--target",
                        "target node id",
                        CliAutocompleteValue::new("node-id", "target node id", []),
                    ),
                    CliAutocompleteOption::flag("--all", "query every cluster node"),
                ])]),
            CliAutocompleteSpec::new("traces", "read live trace events").with_options([
                CliAutocompleteOption::with_value(
                    "--after-sequence",
                    "exclusive process-local trace cursor",
                    CliAutocompleteValue::new("n", "last sequence from the previous response", []),
                ),
                CliAutocompleteOption::with_value(
                    "--target-prefix",
                    "tracing target prefix",
                    CliAutocompleteValue::new("prefix", "tracing target prefix", []),
                ),
                CliAutocompleteOption::with_value(
                    "--traceparent",
                    "traceparent id",
                    CliAutocompleteValue::new("id", "traceparent id", []),
                ),
                CliAutocompleteOption::with_value(
                    "--capture",
                    "cluster-wide capture id",
                    CliAutocompleteValue::new("id", "capture id", []),
                ),
                CliAutocompleteOption::with_value(
                    "--target",
                    "target node id",
                    CliAutocompleteValue::new("node-id", "target node id", []),
                ),
                CliAutocompleteOption::with_value(
                    "--limit",
                    "maximum records to return",
                    CliAutocompleteValue::new("n", "record limit", []),
                ),
                CliAutocompleteOption::flag("--all", "query every cluster node"),
                CliAutocompleteOption::flag("--json", "emit JSON"),
            ]),
            CliAutocompleteSpec::new("control", "read or update observe control").with_options([
                CliAutocompleteOption::flag(
                    "--live-log-writing",
                    "write subsequent routine logs to existing sinks",
                ),
                CliAutocompleteOption::flag(
                    "--no-live-log-writing",
                    "keep routine logs in bounded memory",
                ),
                CliAutocompleteOption::with_value(
                    "--filter",
                    "tracing filter directive",
                    CliAutocompleteValue::new("directive", "tracing filter directive", []),
                ),
                CliAutocompleteOption::with_value(
                    "--quiet-filter",
                    "default noise filter directive",
                    CliAutocompleteValue::new("directive", "quiet filter directive", []),
                ),
                CliAutocompleteOption::with_value(
                    "--target",
                    "target node id",
                    CliAutocompleteValue::new("node-id", "target node id", []),
                ),
                CliAutocompleteOption::flag("--clear-quiet-filters", "clear quiet filters"),
                CliAutocompleteOption::with_value(
                    "--capture",
                    "tag new traces with a capture id",
                    CliAutocompleteValue::new("id", "capture id", []),
                ),
                CliAutocompleteOption::flag("--clear-capture", "stop the active capture"),
                CliAutocompleteOption::with_value(
                    "--expected-capture",
                    "capture id required before clearing",
                    CliAutocompleteValue::new("id", "expected capture id", []),
                ),
                CliAutocompleteOption::flag("--clear-traces", "clear buffered traces"),
                CliAutocompleteOption::flag("--all", "query every cluster node"),
                CliAutocompleteOption::flag("--reset", "reset trace filter"),
                CliAutocompleteOption::flag("--json", "emit JSON"),
            ]),
            CliAutocompleteSpec::new("profile", "control bounded process profiles").with_children(
                [
                    CliAutocompleteSpec::new("cpu", "control a CPU profile").with_children([
                        cpu_profile_autocomplete("start", "start a bounded CPU profile", true),
                        cpu_profile_autocomplete("status", "read CPU profile status", false),
                        cpu_profile_autocomplete("stop", "stop the active CPU profile", true),
                    ]),
                ],
            ),
            CliAutocompleteSpec::new("timeline", "capture bounded RPC timelines").with_children([
                CliAutocompleteSpec::new("network", "capture the object-RPC network path")
                    .with_children([
                        network_timeline_autocomplete("start", "start a bounded timeline", true),
                        network_timeline_autocomplete("status", "read timeline status", false),
                        network_timeline_autocomplete("stop", "stop and emit the timeline", true),
                    ]),
            ]),
        ])
}

fn network_timeline_autocomplete(
    action: &'static str,
    description: &'static str,
    allow_capture: bool,
) -> CliAutocompleteSpec {
    let mut options = vec![
        CliAutocompleteOption::with_value(
            "--target",
            "target node id",
            CliAutocompleteValue::new("node-id", "target node id", []),
        ),
        CliAutocompleteOption::flag("--json", "emit JSON"),
    ];
    if allow_capture {
        options.push(CliAutocompleteOption::with_value(
            "--capture",
            "timeline capture id",
            CliAutocompleteValue::new("id", "capture id", []),
        ));
    }
    if action == "start" {
        options.extend([
            CliAutocompleteOption::with_value(
                "--duration-seconds",
                "bounded capture duration",
                CliAutocompleteValue::new("seconds", "1 to 120 seconds", []),
            ),
            CliAutocompleteOption::with_value(
                "--max-events",
                "preallocated event bound",
                CliAutocompleteValue::new("n", "event capacity", []),
            ),
        ]);
    }
    CliAutocompleteSpec::new(action, description).with_options(options)
}

fn cpu_profile_autocomplete(
    action: &'static str,
    description: &'static str,
    allow_capture: bool,
) -> CliAutocompleteSpec {
    let mut options = vec![
        CliAutocompleteOption::with_value(
            "--target",
            "target node id",
            CliAutocompleteValue::new("node-id", "target node id", []),
        ),
        CliAutocompleteOption::flag("--json", "emit JSON"),
    ];
    if allow_capture {
        options.push(CliAutocompleteOption::with_value(
            "--capture",
            "CPU profile capture id",
            CliAutocompleteValue::new("id", "capture id", []),
        ));
    }
    if action == "start" {
        options.extend([
            CliAutocompleteOption::with_value(
                "--duration-seconds",
                "bounded capture duration",
                CliAutocompleteValue::new("seconds", "1 to 30 seconds", []),
            ),
            CliAutocompleteOption::with_value(
                "--frequency-hz",
                "sampling frequency",
                CliAutocompleteValue::new("hz", "10 to 500 Hz", []),
            ),
        ]);
    }
    CliAutocompleteSpec::new(action, description).with_options(options)
}

fn parse_observe_command(args: &[String]) -> Result<ObserveCliCommand, CliParseError> {
    match args.split_first() {
        None => Ok(ObserveCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(ObserveCliCommand::Help)
        }
        Some((command, rest)) if matches!(command.as_str(), "events" | "event") => Ok(
            ObserveCliCommand::Events(parse_observe_options(rest, true, false, false)?),
        ),
        Some((command, rest)) if matches!(command.as_str(), "logs" | "log") => Ok(
            ObserveCliCommand::Logs(parse_observe_options(rest, false, false, false)?),
        ),
        Some((command, rest)) if matches!(command.as_str(), "metrics" | "metric") => Ok(
            ObserveCliCommand::Metrics(parse_observe_options(rest, false, false, true)?),
        ),
        Some((command, rest)) if command == "prometheus" => Ok(ObserveCliCommand::Prometheus(
            parse_observe_prometheus_options(rest)?,
        )),
        Some((command, rest)) if matches!(command.as_str(), "traces" | "trace") => Ok(
            ObserveCliCommand::Traces(parse_observe_trace_options(rest)?),
        ),
        Some((command, rest)) if matches!(command.as_str(), "control" | "ctl") => Ok(
            ObserveCliCommand::Control(parse_observe_control_options(rest)?),
        ),
        Some((command, rest)) if command == "profile" => Ok(ObserveCliCommand::CpuProfile(
            parse_observe_cpu_profile_options(rest)?,
        )),
        Some((command, rest)) if command == "timeline" => Ok(ObserveCliCommand::NetworkTimeline(
            parse_observe_network_timeline_options(rest)?,
        )),
        _ => Err(CliParseError::Usage),
    }
}

fn parse_observe_prometheus_options(
    args: &[String],
) -> Result<ObservePrometheusOptions, CliParseError> {
    let Some((action, args)) = args.split_first() else {
        return Err(CliParseError::Usage);
    };
    if action != "serve" {
        return Err(CliParseError::Usage);
    }
    let mut options = ObservePrometheusOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--listen" => {
                index += 1;
                let Some(address) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                let address =
                    address
                        .parse::<SocketAddr>()
                        .map_err(|source| CliParseError::Parse {
                            detail: format!(
                                "invalid Prometheus exporter listen address `{address}`: {source}"
                            ),
                        })?;
                if !address.ip().is_loopback() {
                    return Err(CliParseError::Parse {
                        detail: format!(
                            "Prometheus exporter listen address `{address}` is not loopback; expose it through an authenticated reverse proxy"
                        ),
                    });
                }
                options.listen = address;
            }
            "--refresh" | "--refresh-seconds" => {
                index += 1;
                let Some(interval) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                let interval = interval
                    .parse::<u64>()
                    .map_err(|source| CliParseError::Parse {
                        detail: format!(
                            "invalid Prometheus exporter refresh interval `{interval}`: {source}"
                        ),
                    })?;
                if !(PROMETHEUS_MIN_REFRESH_SECONDS..=PROMETHEUS_MAX_REFRESH_SECONDS)
                    .contains(&interval)
                {
                    return Err(CliParseError::Parse {
                        detail: format!(
                            "Prometheus exporter refresh interval must be between {PROMETHEUS_MIN_REFRESH_SECONDS} and {PROMETHEUS_MAX_REFRESH_SECONDS} seconds"
                        ),
                    });
                }
                options.refresh_interval_seconds = interval;
            }
            "--name" | "--name-prefix" => {
                index += 1;
                let Some(name_prefix) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.name_prefix = Some(non_empty_observe_value("--name", name_prefix)?);
            }
            "--target" | "--node" => {
                index += 1;
                let Some(target_node_id) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.target_node_id = Some(non_empty_observe_value("--target", target_node_id)?);
            }
            "--all" => options.target_all = true,
            _ => return Err(CliParseError::Usage),
        }
        index += 1;
    }
    if options.target_all && options.target_node_id.is_some() {
        return Err(CliParseError::Usage);
    }
    Ok(options)
}

fn parse_observe_network_timeline_options(
    args: &[String],
) -> Result<ObserveNetworkTimelineOptions, CliParseError> {
    let Some((timeline_kind, rest)) = args.split_first() else {
        return Err(CliParseError::Usage);
    };
    if timeline_kind != "network" {
        return Err(CliParseError::Usage);
    }
    let (action, rest) = match rest.split_first() {
        None => (ObserveNetworkTimelineAction::Status, rest),
        Some((action, rest)) if matches!(action.as_str(), "status" | "show") => {
            (ObserveNetworkTimelineAction::Status, rest)
        }
        Some((action, rest)) if action == "start" => (ObserveNetworkTimelineAction::Start, rest),
        Some((action, rest)) if action == "stop" => (ObserveNetworkTimelineAction::Stop, rest),
        _ => return Err(CliParseError::Usage),
    };
    let mut request = ObserveNetworkTimelineRequest {
        action,
        ..ObserveNetworkTimelineRequest::default()
    };
    let mut target_node_id = None;
    let mut json = false;
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--capture" if action != ObserveNetworkTimelineAction::Status => {
                index += 1;
                let Some(capture_id) = rest.get(index) else {
                    return Err(CliParseError::Usage);
                };
                request.capture_id = Some(non_empty_observe_value("--capture", capture_id)?);
            }
            "--duration-seconds" | "--duration"
                if action == ObserveNetworkTimelineAction::Start =>
            {
                index += 1;
                let Some(duration) = rest.get(index) else {
                    return Err(CliParseError::Usage);
                };
                request.duration_seconds =
                    Some(
                        duration
                            .parse::<u64>()
                            .map_err(|source| CliParseError::Parse {
                                detail: format!(
                                    "invalid network timeline duration `{duration}`: {source}"
                                ),
                            })?,
                    );
            }
            "--max-events" if action == ObserveNetworkTimelineAction::Start => {
                index += 1;
                let Some(max_events) = rest.get(index) else {
                    return Err(CliParseError::Usage);
                };
                request.max_events =
                    Some(
                        max_events
                            .parse::<usize>()
                            .map_err(|source| CliParseError::Parse {
                                detail: format!(
                                    "invalid network timeline event bound `{max_events}`: {source}"
                                ),
                            })?,
                    );
            }
            "--target" | "--node" => {
                index += 1;
                let Some(target) = rest.get(index) else {
                    return Err(CliParseError::Usage);
                };
                target_node_id = Some(non_empty_observe_value("--target", target)?);
            }
            "--json" => json = true,
            _ => return Err(CliParseError::Usage),
        }
        index += 1;
    }
    Ok(ObserveNetworkTimelineOptions {
        request,
        target_node_id,
        json,
    })
}

fn parse_observe_cpu_profile_options(
    args: &[String],
) -> Result<ObserveCpuProfileOptions, CliParseError> {
    let Some((profile_kind, rest)) = args.split_first() else {
        return Err(CliParseError::Usage);
    };
    if profile_kind != "cpu" {
        return Err(CliParseError::Usage);
    }
    let (action, rest) = match rest.split_first() {
        None => (ObserveCpuProfileAction::Status, rest),
        Some((action, rest)) if matches!(action.as_str(), "status" | "show") => {
            (ObserveCpuProfileAction::Status, rest)
        }
        Some((action, rest)) if action == "start" => (ObserveCpuProfileAction::Start, rest),
        Some((action, rest)) if action == "stop" => (ObserveCpuProfileAction::Stop, rest),
        _ => return Err(CliParseError::Usage),
    };
    let mut request = ObserveCpuProfileRequest {
        action,
        ..ObserveCpuProfileRequest::default()
    };
    let mut target_node_id = None;
    let mut json = false;
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--capture" if action != ObserveCpuProfileAction::Status => {
                index += 1;
                let Some(capture_id) = rest.get(index) else {
                    return Err(CliParseError::Usage);
                };
                request.capture_id = Some(non_empty_observe_value("--capture", capture_id)?);
            }
            "--duration-seconds" | "--duration" if action == ObserveCpuProfileAction::Start => {
                index += 1;
                let Some(duration) = rest.get(index) else {
                    return Err(CliParseError::Usage);
                };
                request.duration_seconds =
                    Some(
                        duration
                            .parse::<u64>()
                            .map_err(|source| CliParseError::Parse {
                                detail: format!(
                                    "invalid CPU profile duration `{duration}`: {source}"
                                ),
                            })?,
                    );
            }
            "--frequency-hz" | "--frequency" if action == ObserveCpuProfileAction::Start => {
                index += 1;
                let Some(frequency) = rest.get(index) else {
                    return Err(CliParseError::Usage);
                };
                request.frequency_hz =
                    Some(
                        frequency
                            .parse::<u32>()
                            .map_err(|source| CliParseError::Parse {
                                detail: format!(
                                    "invalid CPU profile frequency `{frequency}`: {source}"
                                ),
                            })?,
                    );
            }
            "--target" | "--node" => {
                index += 1;
                let Some(target) = rest.get(index) else {
                    return Err(CliParseError::Usage);
                };
                target_node_id = Some(non_empty_observe_value("--target", target)?);
            }
            "--json" => json = true,
            _ => return Err(CliParseError::Usage),
        }
        index += 1;
    }
    Ok(ObserveCpuProfileOptions {
        request,
        target_node_id,
        json,
    })
}

fn parse_observe_trace_options(args: &[String]) -> Result<ObserveOptions, CliParseError> {
    let mut options = parse_observe_options(args, false, true, false)?;
    if !args.iter().any(|argument| argument == "--limit") {
        // Trace capture is a diagnostic artifact: default to the complete
        // bounded ring and make explicit limits opt-in.
        options.limit = 0;
    }
    Ok(options)
}

fn parse_observe_options(
    args: &[String],
    allow_kind: bool,
    allow_capture: bool,
    allow_watch: bool,
) -> Result<ObserveOptions, CliParseError> {
    let mut options = ObserveOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--kind" | "--kind-prefix" if allow_kind => {
                index += 1;
                let Some(kind) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.kind_prefix = Some(non_empty_observe_value("--kind", kind)?);
            }
            "--kind" | "--kind-prefix" => return Err(CliParseError::Usage),
            "--component" => {
                index += 1;
                let Some(component) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.component = Some(non_empty_observe_value("--component", component)?);
            }
            "--operation" | "--operation-id" | "--op" => {
                index += 1;
                let Some(operation_id) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.operation_id = Some(non_empty_observe_value("--operation", operation_id)?);
            }
            "--name" | "--name-prefix" => {
                index += 1;
                let Some(name_prefix) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.name_prefix = Some(non_empty_observe_value("--name", name_prefix)?);
            }
            "--target-prefix" => {
                index += 1;
                let Some(target_prefix) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.component =
                    Some(non_empty_observe_value("--target-prefix", target_prefix)?);
            }
            "--traceparent" => {
                index += 1;
                let Some(traceparent) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.traceparent = Some(non_empty_observe_value("--traceparent", traceparent)?);
            }
            "--after-sequence" if allow_capture => {
                index += 1;
                let value = args.get(index).ok_or(CliParseError::Usage)?;
                options.after_sequence =
                    Some(
                        value
                            .parse::<u64>()
                            .map_err(|source| CliParseError::Parse {
                                detail: format!("invalid trace sequence `{value}`: {source}"),
                            })?,
                    );
            }
            "--capture" if allow_capture => {
                index += 1;
                let Some(capture_id) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.capture_id = Some(non_empty_observe_value("--capture", capture_id)?);
            }
            "--capture" => return Err(CliParseError::Usage),
            "--target" | "--node" => {
                index += 1;
                let Some(target_node_id) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.target_node_id = Some(non_empty_observe_value("--target", target_node_id)?);
            }
            "--all" => options.target_all = true,
            "--limit" => {
                index += 1;
                let Some(limit) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.limit = limit
                    .parse::<usize>()
                    .map_err(|source| CliParseError::Parse {
                        detail: format!("invalid observe limit `{limit}`: {source}"),
                    })?;
            }
            "--watch" if allow_watch => {
                index += 1;
                let Some(interval) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                let interval = interval
                    .parse::<u64>()
                    .map_err(|source| CliParseError::Parse {
                        detail: format!("invalid observe watch interval `{interval}`: {source}"),
                    })?;
                if !(1..=3_600).contains(&interval) {
                    return Err(CliParseError::Parse {
                        detail: "observe watch interval must be between 1 and 3600 seconds"
                            .to_string(),
                    });
                }
                options.watch_interval_seconds = Some(interval);
            }
            "--samples" if allow_watch => {
                index += 1;
                let Some(samples) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                let samples = samples
                    .parse::<usize>()
                    .map_err(|source| CliParseError::Parse {
                        detail: format!("invalid observe watch sample count `{samples}`: {source}"),
                    })?;
                if samples == 0 {
                    return Err(CliParseError::Parse {
                        detail: "observe watch sample count must be positive".to_string(),
                    });
                }
                options.watch_samples = Some(samples);
            }
            "--watch" | "--samples" => return Err(CliParseError::Usage),
            "--json" => options.json = true,
            _ => return Err(CliParseError::Usage),
        }
        index += 1;
    }
    if options.target_all && (options.target_node_id.is_some() || options.after_sequence.is_some())
    {
        return Err(CliParseError::Usage);
    }
    if options.watch_samples.is_some() && options.watch_interval_seconds.is_none() {
        return Err(CliParseError::Usage);
    }
    Ok(options)
}

fn parse_observe_control_options(args: &[String]) -> Result<ObserveControlOptions, CliParseError> {
    let mut options = ObserveControlOptions {
        live_log_writing: None,
        filter: None,
        quiet_filter_directives: Vec::new(),
        clear_quiet_filter_directives: false,
        reset: false,
        capture_id: None,
        clear_capture: false,
        expected_capture_id: None,
        clear_traces: false,
        target_node_id: None,
        target_all: false,
        json: false,
    };
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--live-log-writing" | "--no-live-log-writing" => {
                let enabled = args[index] == "--live-log-writing";
                if options
                    .live_log_writing
                    .is_some_and(|previous| previous != enabled)
                {
                    return Err(CliParseError::Usage);
                }
                options.live_log_writing = Some(enabled);
            }
            "--filter" => {
                index += 1;
                let Some(filter) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.filter = Some(non_empty_observe_value("--filter", filter)?);
            }
            "--quiet-filter" | "--quiet" => {
                index += 1;
                let Some(filter) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options
                    .quiet_filter_directives
                    .push(non_empty_observe_value("--quiet-filter", filter)?);
            }
            "--clear-quiet-filters" | "--clear-quiet" | "--no-quiet-filters" => {
                options.clear_quiet_filter_directives = true;
            }
            "--reset" => options.reset = true,
            "--capture" => {
                index += 1;
                let Some(capture_id) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.capture_id = Some(non_empty_observe_value("--capture", capture_id)?);
            }
            "--clear-capture" => options.clear_capture = true,
            "--expected-capture" => {
                index += 1;
                let Some(capture_id) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.expected_capture_id =
                    Some(non_empty_observe_value("--expected-capture", capture_id)?);
            }
            "--clear-traces" => options.clear_traces = true,
            "--target" | "--node" => {
                index += 1;
                let Some(target_node_id) = args.get(index) else {
                    return Err(CliParseError::Usage);
                };
                options.target_node_id = Some(non_empty_observe_value("--target", target_node_id)?);
            }
            "--all" => options.target_all = true,
            "--json" => options.json = true,
            _ => return Err(CliParseError::Usage),
        }
        index += 1;
    }
    if options.target_all && options.target_node_id.is_some() {
        return Err(CliParseError::Usage);
    }
    if options.clear_quiet_filter_directives && !options.quiet_filter_directives.is_empty() {
        return Err(CliParseError::Usage);
    }
    if options.clear_capture && options.capture_id.is_some() {
        return Err(CliParseError::Usage);
    }
    if options.live_log_writing.is_some() && (options.capture_id.is_some() || options.clear_capture)
    {
        return Err(CliParseError::Usage);
    }
    if options.expected_capture_id.is_some() && !options.clear_capture {
        return Err(CliParseError::Usage);
    }
    if options.clear_capture && options.expected_capture_id.is_none() {
        return Err(CliParseError::Usage);
    }
    if options.clear_capture
        && (options.filter.is_some()
            || !options.quiet_filter_directives.is_empty()
            || options.clear_quiet_filter_directives
            || options.reset)
    {
        return Err(CliParseError::Usage);
    }
    Ok(options)
}

fn non_empty_observe_value(flag: &str, value: &str) -> Result<String, CliParseError> {
    if value.trim().is_empty() {
        return Err(CliParseError::Parse {
            detail: format!("{flag} cannot be empty"),
        });
    }
    Ok(value.to_string())
}

fn observe_events(
    context: &mut CliCommandContext<'_>,
    rpc: Option<&ObserveCliRpc>,
    target_lister: Option<&ObserveCliTargetLister>,
    options: ObserveOptions,
) -> Result<(), CliCommandError> {
    if options.target_all {
        let request = ObserveEventsRequest {
            kind_prefix: options.kind_prefix.clone(),
            component: options.component.clone(),
            operation_id: options.operation_id.clone(),
            limit: options.limit,
        };
        let response = observe_rpc_all::<_, ObserveEventsResponse>(
            rpc,
            target_lister,
            OBSERVE_EVENTS_ROUTE,
            &request,
        )?;
        return print_observe_fanout(
            context,
            response,
            options.json,
            |context, response, json| print_observe_event_records(context, response.records, json),
        );
    }
    if options.target_node_id.is_some() {
        let response: ObserveEventsResponse = observe_rpc(
            rpc,
            options.target_node_id.as_deref(),
            OBSERVE_EVENTS_ROUTE,
            &ObserveEventsRequest {
                kind_prefix: options.kind_prefix.clone(),
                component: options.component.clone(),
                operation_id: options.operation_id.clone(),
                limit: options.limit,
            },
        )?;
        return print_observe_event_records(context, response.records, options.json);
    }
    let data_dir = SinomoDataDir::from_env_or_home().map_err(command_error)?;
    ensure_observe_module_enabled(&data_dir)?;
    let records = read_operational_events(
        data_dir.root(),
        &OperationalEventQuery {
            kind_prefix: options.kind_prefix.clone(),
            component: options.component.clone(),
            operation_id: options.operation_id.clone(),
            limit: options.limit,
        },
    )
    .map_err(command_error)?;
    print_observe_event_records(context, records, options.json)
}

fn print_observe_event_records(
    context: &mut CliCommandContext<'_>,
    records: Vec<OperationalEventRecord>,
    json: bool,
) -> Result<(), CliCommandError> {
    if json {
        let json = serde_json::to_string_pretty(&records).map_err(command_error)?;
        context.stdout_line(format_args!("{json}"))?;
        return Ok(());
    }
    if records.is_empty() {
        context.stdout_line(format_args!("no operational events found"))?;
        return Ok(());
    }
    for record in records {
        print_operational_event_record(context, &record)?;
    }
    Ok(())
}

fn observe_logs(
    context: &mut CliCommandContext<'_>,
    rpc: Option<&ObserveCliRpc>,
    target_lister: Option<&ObserveCliTargetLister>,
    options: ObserveOptions,
) -> Result<(), CliCommandError> {
    if options.target_all {
        let request = ObserveLogsRequest {
            component: options.component.clone(),
            operation_id: options.operation_id.clone(),
            limit: options.limit,
            ..Default::default()
        };
        let response = observe_rpc_all::<_, ObserveLogsResponse>(
            rpc,
            target_lister,
            OBSERVE_LOGS_ROUTE,
            &request,
        )?;
        return print_observe_fanout(context, response, options.json, print_observe_logs_response);
    }
    if options.target_node_id.is_some() || rpc.is_some() {
        let response = observe_rpc::<_, ObserveLogsResponse>(
            rpc,
            options.target_node_id.as_deref(),
            OBSERVE_LOGS_ROUTE,
            &ObserveLogsRequest {
                component: options.component.clone(),
                operation_id: options.operation_id.clone(),
                limit: options.limit,
                ..Default::default()
            },
        );
        match response {
            Ok(response) => return print_observe_logs_response(context, response, options.json),
            Err(error) if options.target_node_id.is_none() => {
                context.stderr_line(format_args!(
                    "daemon log collection unavailable: {error}; reading local operational logs"
                ))?;
            }
            Err(error) => return Err(error),
        }
    }
    let data_dir = SinomoDataDir::from_env_or_home().map_err(command_error)?;
    ensure_observe_module_enabled(&data_dir)?;
    let records = read_operational_logs(
        data_dir.root(),
        &OperationalLogQuery {
            component: options.component.clone(),
            operation_id: options.operation_id.clone(),
            limit: options.limit,
        },
    )
    .map_err(command_error)?;
    print_observe_log_records(context, records, options.json)
}

fn print_observe_logs_response(
    context: &mut CliCommandContext<'_>,
    response: ObserveLogsResponse,
    json: bool,
) -> Result<(), CliCommandError> {
    if !response.runtime_available {
        context.stderr_line(format_args!(
            "runtime log collection unavailable; operational logs only"
        ))?;
    }
    if response.truncated || response.runtime_evicted > 0 {
        context.stderr_line(format_args!(
            "recent log tail; {} runtime records evicted",
            response.runtime_evicted
        ))?;
    }
    for error in &response.errors {
        context.stderr_line(format_args!("log source: {error}"))?;
    }
    print_observe_log_records(context, response.records, json)
}

fn print_observe_log_records(
    context: &mut CliCommandContext<'_>,
    records: Vec<OperationalLogRecord>,
    json: bool,
) -> Result<(), CliCommandError> {
    if json {
        let json = serde_json::to_string_pretty(&records).map_err(command_error)?;
        context.stdout_line(format_args!("{json}"))?;
        return Ok(());
    }
    if records.is_empty() {
        context.stdout_line(format_args!("no logs found"))?;
        return Ok(());
    }
    for record in records {
        print_operational_log_record(context, &record)?;
    }
    Ok(())
}

fn observe_metrics(
    context: &mut CliCommandContext<'_>,
    rpc: Option<&ObserveCliRpc>,
    target_lister: Option<&ObserveCliTargetLister>,
    options: ObserveOptions,
) -> Result<(), CliCommandError> {
    let request = ObserveMetricsRequest {
        name_prefix: options.name_prefix.clone(),
        limit: options.limit,
    };
    if options.target_all {
        let response = observe_metrics_all_once(rpc, target_lister, &request)?;
        if let Some(interval_seconds) = options.watch_interval_seconds {
            return watch_observe_metrics_all(
                context,
                rpc,
                target_lister,
                &request,
                response,
                interval_seconds,
                options.watch_samples,
                options.json,
            );
        }
        return print_observe_fanout(
            context,
            response,
            options.json,
            |context, response, json| print_observe_metric_records(context, response.metrics, json),
        );
    }
    let response = observe_metrics_once(rpc, options.target_node_id.as_deref(), &request)?;
    if let Some(interval_seconds) = options.watch_interval_seconds {
        return watch_observe_metrics(
            context,
            rpc,
            options.target_node_id.as_deref(),
            &request,
            response,
            interval_seconds,
            options.watch_samples,
            options.json,
        );
    }
    print_observe_metric_records(context, response.metrics, options.json)
}

fn observe_prometheus(
    context: &mut CliCommandContext<'_>,
    rpc: Option<&ObserveCliRpc>,
    target_lister: Option<&ObserveCliTargetLister>,
    options: ObservePrometheusOptions,
) -> Result<(), CliCommandError> {
    let server = PrometheusHttpServer::bind(options.listen).map_err(CliCommandError::new)?;
    let listen = server.local_addr().map_err(CliCommandError::new)?;
    let refresh_interval = Duration::from_secs(options.refresh_interval_seconds);
    let rpc = rpc.cloned();
    let target_lister = target_lister.cloned();
    let collector_options = options.clone();
    let (external_shutdown, _signal_guard) =
        install_prometheus_shutdown_signals().map_err(CliCommandError::new)?;
    let collector_shutdown = Arc::clone(&external_shutdown);
    let collector: PrometheusCollector = Arc::new(move || {
        collect_prometheus_metrics(
            rpc.as_ref(),
            target_lister.as_ref(),
            &collector_options,
            Some(&collector_shutdown),
        )
    });

    context.stdout_line(format_args!(
        "Prometheus exporter listening on http://{listen}/metrics (refresh={}s)",
        options.refresh_interval_seconds
    ))?;
    context.stdout_line(format_args!(
        "Short live snapshots only; use Prometheus or Grafana for retention and alerting."
    ))?;
    server
        .serve(
            refresh_interval,
            collector,
            PrometheusShutdown::default(),
            external_shutdown,
        )
        .map_err(CliCommandError::new)
}

fn collect_prometheus_metrics(
    rpc: Option<&ObserveCliRpc>,
    target_lister: Option<&ObserveCliTargetLister>,
    options: &ObservePrometheusOptions,
    cancellation: Option<&AtomicBool>,
) -> Result<String, String> {
    let request = ObserveMetricsRequest {
        name_prefix: options.name_prefix.clone(),
        limit: 0,
    };
    let mut records = Vec::new();
    let mut descriptors = Vec::new();
    let mut collection_complete;
    if options.target_all {
        let response =
            observe_metrics_all_once_with_cancel(rpc, target_lister, &request, cancellation)
                .map_err(|error| error.to_string())?;
        collection_complete = response.errors.is_empty();
        for target in response.targets {
            if target.response.partial {
                collection_complete = false;
            }
            append_prometheus_target_metrics(
                &mut records,
                &mut descriptors,
                &target.target_node_id,
                target.response,
            )?;
            records.push(prometheus_target_up_record(&target.target_node_id, true));
        }
        for error in response.errors {
            records.push(prometheus_target_up_record(&error.target_node_id, false));
        }
    } else {
        let response = observe_metrics_once(rpc, options.target_node_id.as_deref(), &request)
            .map_err(|error| error.to_string())?;
        let target_node_id = options
            .target_node_id
            .clone()
            .or_else(|| response.node_id.clone())
            .unwrap_or_else(|| "local".to_string());
        collection_complete = !response.partial;
        append_prometheus_target_metrics(
            &mut records,
            &mut descriptors,
            &target_node_id,
            response,
        )?;
        records.push(prometheus_target_up_record(&target_node_id, true));
    }
    records.push(ObserveMetricRecord::gauge(
        "sinomo.observe.prometheus.collection_success",
        Default::default(),
        if collection_complete { 1.0 } else { 0.0 },
    ));

    for descriptor in prometheus_bridge_descriptors() {
        descriptors.retain(|existing| existing.name != descriptor.name);
        descriptors.push(descriptor);
    }
    crate::encode_openmetrics(&records, &descriptors).map_err(|error| error.to_string())
}

fn append_prometheus_target_metrics(
    records: &mut Vec<ObserveMetricRecord>,
    descriptors: &mut Vec<ObserveMetricDescriptor>,
    target_node_id: &str,
    response: ObserveMetricsResponse,
) -> Result<(), String> {
    for descriptor in response.descriptors {
        match descriptors
            .iter_mut()
            .find(|existing| existing.name == descriptor.name)
        {
            Some(existing)
                if existing.kind != descriptor.kind || existing.unit != descriptor.unit =>
            {
                return Err(format!(
                    "metric descriptor `{}` changed kind or unit between targets",
                    descriptor.name
                ));
            }
            Some(existing) => {
                // Help text may legitimately drift during a mixed-version
                // rollout. Semantic metadata was added later, so prefer it
                // over an empty legacy block. Conflicting new metadata fails
                // closed for aggregation while preserving known dimensions.
                merge_prometheus_descriptor_semantics(existing, &descriptor);
            }
            None => descriptors.push(descriptor),
        }
    }
    for mut record in response.metrics {
        insert_prometheus_constant_label(&mut record, "node_id", target_node_id)?;
        if let Some(process_role) = response.process_role.as_deref() {
            insert_prometheus_constant_label(&mut record, "process_role", process_role)?;
        }
        records.push(record);
    }
    Ok(())
}

fn merge_prometheus_descriptor_semantics(
    existing: &mut ObserveMetricDescriptor,
    incoming: &ObserveMetricDescriptor,
) {
    existing.semantics.merge_rolling(&incoming.semantics);
}

fn insert_prometheus_constant_label(
    record: &mut ObserveMetricRecord,
    label: &str,
    value: &str,
) -> Result<(), String> {
    if let Some(existing) = record.labels.get(label) {
        if existing == value {
            return Ok(());
        }
        return Err(format!(
            "metric `{}` label `{label}` conflicts with exporter value `{value}`",
            record.name
        ));
    }
    record.labels.insert(label.to_string(), value.to_string());
    Ok(())
}

fn prometheus_target_up_record(target_node_id: &str, up: bool) -> ObserveMetricRecord {
    ObserveMetricRecord::gauge(
        "sinomo.observe.prometheus.target_up",
        [("node_id".to_string(), target_node_id.to_string())]
            .into_iter()
            .collect(),
        if up { 1.0 } else { 0.0 },
    )
}

fn prometheus_bridge_descriptors() -> [ObserveMetricDescriptor; 2] {
    [
        ObserveMetricDescriptor::new(
            "sinomo.observe.prometheus.collection_success",
            crate::ObserveMetricKind::Gauge,
            Some(ObserveMetricUnit::Count),
            "Whether the latest cached cluster observation collection completed without target errors.",
        )
        .with_semantics(ObserveMetricSemantics::new(
            "observe",
            ObserveMetricAggregation::Min,
            std::iter::empty::<&str>(),
        )),
        ObserveMetricDescriptor::new(
            "sinomo.observe.prometheus.target_up",
            crate::ObserveMetricKind::Gauge,
            Some(ObserveMetricUnit::Count),
            "Whether the latest cached observation request reached and decoded the target node.",
        )
        .with_semantics(ObserveMetricSemantics::new(
            "observe",
            ObserveMetricAggregation::Sum,
            ["node_id"],
        )),
    ]
}

fn observe_metrics_once(
    rpc: Option<&ObserveCliRpc>,
    target_node_id: Option<&str>,
    request: &ObserveMetricsRequest,
) -> Result<ObserveMetricsResponse, CliCommandError> {
    if rpc.is_some() {
        observe_rpc::<_, ObserveMetricsResponse>(
            rpc,
            target_node_id,
            OBSERVE_METRICS_ROUTE,
            request,
        )
    } else {
        Ok(read_observe_metrics(request))
    }
}

fn observe_metrics_all_once(
    rpc: Option<&ObserveCliRpc>,
    target_lister: Option<&ObserveCliTargetLister>,
    request: &ObserveMetricsRequest,
) -> Result<ObserveFanoutResponse<ObserveMetricsResponse>, CliCommandError> {
    observe_rpc_all::<_, ObserveMetricsResponse>(rpc, target_lister, OBSERVE_METRICS_ROUTE, request)
}

fn observe_metrics_all_once_with_cancel(
    rpc: Option<&ObserveCliRpc>,
    target_lister: Option<&ObserveCliTargetLister>,
    request: &ObserveMetricsRequest,
    cancellation: Option<&AtomicBool>,
) -> Result<ObserveFanoutResponse<ObserveMetricsResponse>, CliCommandError> {
    observe_rpc_all_with_cancel::<_, ObserveMetricsResponse>(
        rpc,
        target_lister,
        OBSERVE_METRICS_ROUTE,
        request,
        cancellation,
    )
}

#[derive(serde::Serialize)]
struct ObserveMetricWatchSample<T> {
    interval_seconds: f64,
    sample: T,
}

#[allow(clippy::too_many_arguments)]
fn watch_observe_metrics(
    context: &mut CliCommandContext<'_>,
    rpc: Option<&ObserveCliRpc>,
    target_node_id: Option<&str>,
    request: &ObserveMetricsRequest,
    mut previous: ObserveMetricsResponse,
    interval_seconds: u64,
    sample_limit: Option<usize>,
    json: bool,
) -> Result<(), CliCommandError> {
    let interval = Duration::from_secs(interval_seconds);
    let mut previous_at = Instant::now();
    let mut samples = 0_usize;
    loop {
        std::thread::sleep(interval);
        let current = observe_metrics_once(rpc, target_node_id, request)?;
        let now = Instant::now();
        let elapsed = now
            .duration_since(previous_at)
            .as_secs_f64()
            .max(f64::EPSILON);
        let metrics =
            crate::observe_metric_records_interval_delta(&previous.metrics, &current.metrics);
        print_observe_metric_watch_records(context, metrics, elapsed, json)?;
        previous = current;
        previous_at = now;
        samples = samples.saturating_add(1);
        if sample_limit.is_some_and(|limit| samples >= limit) {
            return Ok(());
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn watch_observe_metrics_all(
    context: &mut CliCommandContext<'_>,
    rpc: Option<&ObserveCliRpc>,
    target_lister: Option<&ObserveCliTargetLister>,
    request: &ObserveMetricsRequest,
    mut previous: ObserveFanoutResponse<ObserveMetricsResponse>,
    interval_seconds: u64,
    sample_limit: Option<usize>,
    json: bool,
) -> Result<(), CliCommandError> {
    let interval = Duration::from_secs(interval_seconds);
    let mut previous_at = Instant::now();
    let mut samples = 0_usize;
    loop {
        std::thread::sleep(interval);
        let current = observe_metrics_all_once(rpc, target_lister, request)?;
        let now = Instant::now();
        let elapsed = now
            .duration_since(previous_at)
            .as_secs_f64()
            .max(f64::EPSILON);
        let sample = observe_metric_fanout_interval_delta(&previous, &current);
        if json {
            let json = serde_json::to_string(&ObserveMetricWatchSample {
                interval_seconds: elapsed,
                sample,
            })
            .map_err(command_error)?;
            context.stdout_line(format_args!("{json}"))?;
        } else {
            context.stdout_line(format_args!("interval: {elapsed:.3}s"))?;
            print_observe_fanout(context, sample, false, |context, response, _json| {
                print_observe_metric_interval_records(context, response.metrics, elapsed)
            })?;
        }
        previous = current;
        previous_at = now;
        samples = samples.saturating_add(1);
        if sample_limit.is_some_and(|limit| samples >= limit) {
            return Ok(());
        }
    }
}

fn print_observe_metric_watch_records(
    context: &mut CliCommandContext<'_>,
    metrics: Vec<crate::ObserveMetricRecord>,
    interval_seconds: f64,
    json: bool,
) -> Result<(), CliCommandError> {
    if json {
        let json = serde_json::to_string(&ObserveMetricWatchSample {
            interval_seconds,
            sample: crate::observe_metrics_response_from_metrics(metrics),
        })
        .map_err(command_error)?;
        context.stdout_line(format_args!("{json}"))?;
        return Ok(());
    }
    context.stdout_line(format_args!("interval: {interval_seconds:.3}s"))?;
    print_observe_metric_interval_records(context, metrics, interval_seconds)
}

fn observe_metric_fanout_interval_delta(
    previous: &ObserveFanoutResponse<ObserveMetricsResponse>,
    current: &ObserveFanoutResponse<ObserveMetricsResponse>,
) -> ObserveFanoutResponse<ObserveMetricsResponse> {
    let targets = current
        .targets
        .iter()
        .map(|target| {
            let previous_metrics = previous
                .targets
                .iter()
                .find(|candidate| candidate.target_node_id == target.target_node_id)
                .map(|candidate| candidate.response.metrics.as_slice())
                .unwrap_or_default();
            let mut response = target.response.clone();
            response.metrics = crate::observe_metric_records_interval_delta(
                previous_metrics,
                &target.response.metrics,
            );
            ObserveTargetResponse {
                target_node_id: target.target_node_id.clone(),
                response,
            }
        })
        .collect();
    ObserveFanoutResponse {
        targets,
        errors: current.errors.clone(),
    }
}

fn print_observe_metric_records(
    context: &mut CliCommandContext<'_>,
    metrics: Vec<crate::ObserveMetricRecord>,
    json: bool,
) -> Result<(), CliCommandError> {
    if json {
        let json = serde_json::to_string_pretty(&crate::observe_metrics_response_from_metrics(
            metrics.clone(),
        ))
        .map_err(command_error)?;
        context.stdout_line(format_args!("{json}"))?;
        return Ok(());
    }
    if metrics.is_empty() {
        context.stdout_line(format_args!("no observe metrics found"))?;
        return Ok(());
    }
    for metric in metrics {
        let labels = if metric.labels.is_empty() {
            String::new()
        } else {
            format!(
                " labels={}",
                serde_json::to_string(&metric.labels).map_err(command_error)?
            )
        };
        match metric.kind {
            crate::ObserveMetricKind::Counter | crate::ObserveMetricKind::Gauge => {
                context.stdout_line(format_args!(
                    "{:<9} {:<48} value={}{}",
                    format!("{:?}", metric.kind).to_ascii_lowercase(),
                    metric.name,
                    metric.value.unwrap_or_default(),
                    labels
                ))?;
            }
            crate::ObserveMetricKind::Histogram => {
                let count = metric.count.unwrap_or_default();
                let sum = metric.sum.unwrap_or_default();
                let average = if count == 0 { 0.0 } else { sum / count as f64 };
                context.stdout_line(format_args!(
                    "{:<9} {:<48} count={} avg={:.3} min={} max={} p50<={} p95<={} p99<={}{}",
                    "histogram",
                    metric.name,
                    count,
                    average,
                    metric.min.unwrap_or_default(),
                    metric.max.unwrap_or_default(),
                    metric.histogram_quantile(0.50).unwrap_or_default(),
                    metric.histogram_quantile(0.95).unwrap_or_default(),
                    metric.histogram_quantile(0.99).unwrap_or_default(),
                    labels
                ))?;
            }
        }
    }
    Ok(())
}

fn print_observe_metric_interval_records(
    context: &mut CliCommandContext<'_>,
    metrics: Vec<crate::ObserveMetricRecord>,
    interval_seconds: f64,
) -> Result<(), CliCommandError> {
    if metrics.is_empty() {
        context.stdout_line(format_args!("no observe metrics found"))?;
        return Ok(());
    }
    for metric in metrics {
        let labels = if metric.labels.is_empty() {
            String::new()
        } else {
            format!(
                " labels={}",
                serde_json::to_string(&metric.labels).map_err(command_error)?
            )
        };
        match metric.kind {
            crate::ObserveMetricKind::Gauge => {
                context.stdout_line(format_args!(
                    "{:<9} {:<48} value={}{}",
                    "gauge",
                    metric.name,
                    metric.value.unwrap_or_default(),
                    labels
                ))?;
            }
            crate::ObserveMetricKind::Counter => {
                let delta = metric.value.unwrap_or_default();
                context.stdout_line(format_args!(
                    "{:<9} {:<48} delta={} rate={:.3}/s{}",
                    "counter",
                    metric.name,
                    delta,
                    delta / interval_seconds,
                    labels
                ))?;
            }
            crate::ObserveMetricKind::Histogram => {
                let count = metric.count.unwrap_or_default();
                let sum = metric.sum.unwrap_or_default();
                let average = if count == 0 { 0.0 } else { sum / count as f64 };
                context.stdout_line(format_args!(
                    "{:<9} {:<48} count={} rate={:.3}/s avg={:.3} p50<={} p95<={} p99<={}{}",
                    "histogram",
                    metric.name,
                    count,
                    count as f64 / interval_seconds,
                    average,
                    metric.histogram_quantile(0.50).unwrap_or_default(),
                    metric.histogram_quantile(0.95).unwrap_or_default(),
                    metric.histogram_quantile(0.99).unwrap_or_default(),
                    labels
                ))?;
            }
        }
    }
    Ok(())
}

fn observe_traces(
    context: &mut CliCommandContext<'_>,
    rpc: Option<&ObserveCliRpc>,
    target_lister: Option<&ObserveCliTargetLister>,
    options: ObserveOptions,
) -> Result<(), CliCommandError> {
    let request = ObserveTracesRequest {
        after_sequence: options.after_sequence,
        target_prefix: options.component.clone(),
        traceparent: options.traceparent.clone(),
        capture_id: options.capture_id.clone(),
        limit: options.limit,
    };
    if options.target_all {
        let response = observe_rpc_all::<_, ObserveTracesResponse>(
            rpc,
            target_lister,
            OBSERVE_TRACES_ROUTE,
            &request,
        )?;
        if request.capture_id.is_some() {
            validate_cluster_trace_collection(&response)?;
        }
        return print_observe_fanout(
            context,
            response,
            options.json,
            print_observe_trace_response,
        );
    }
    let response = if rpc.is_some() {
        observe_rpc::<_, ObserveTracesResponse>(
            rpc,
            options.target_node_id.as_deref(),
            OBSERVE_TRACES_ROUTE,
            &request,
        )?
    } else {
        read_observe_traces(&request)
    };
    print_observe_trace_response(context, response, options.json)
}

fn validate_cluster_trace_collection(
    response: &ObserveFanoutResponse<ObserveTracesResponse>,
) -> Result<(), CliCommandError> {
    let mut issues = response
        .errors
        .iter()
        .map(|error| format!("{}: {}", error.target_node_id, error.error))
        .collect::<Vec<_>>();
    issues.extend(
        response
            .targets
            .iter()
            .filter(|&target| target.response.truncated)
            .map(|target| {
                format!(
                    "{}: trace buffer incomplete (dropped_records={})",
                    target.target_node_id, target.response.dropped_records
                )
            }),
    );
    issues.extend(
        response
            .targets
            .iter()
            .filter(|&target| target.response.capture_active != Some(true))
            .map(|target| {
                format!(
                    "{}: requested capture is no longer active",
                    target.target_node_id
                )
            }),
    );
    if issues.is_empty() {
        Ok(())
    } else {
        Err(CliCommandError::new(format!(
            "cluster-wide observe capture collection is incomplete: {}",
            issues.join("; ")
        )))
    }
}

fn print_observe_trace_response(
    context: &mut CliCommandContext<'_>,
    response: ObserveTracesResponse,
    json: bool,
) -> Result<(), CliCommandError> {
    if json {
        let json = serde_json::to_string_pretty(&response).map_err(command_error)?;
        context.stdout_line(format_args!("{json}"))?;
        return Ok(());
    }
    if response.truncated {
        context.stdout_line(format_args!(
            "warning: observe trace response is truncated (dropped_records={})",
            response.dropped_records
        ))?;
    }
    if response.records.is_empty() {
        context.stdout_line(format_args!("no observe traces found"))?;
        return Ok(());
    }
    for record in response.records {
        let time = record.timestamp.format(&Rfc3339).map_err(command_error)?;
        let fields = if record.fields.is_null() {
            String::new()
        } else {
            format!(" fields={}", record.fields)
        };
        let traceparent = record
            .traceparent
            .as_deref()
            .map(|traceparent| format!(" traceparent={traceparent}"))
            .unwrap_or_default();
        let capture = record
            .capture_id
            .as_deref()
            .map(|capture_id| format!(" capture={capture_id}"))
            .unwrap_or_default();
        context.stdout_line(format_args!(
            "{time} {:<5} {:<32} {}{}{}{}",
            record.level, record.target, record.name, traceparent, capture, fields
        ))?;
    }
    Ok(())
}

fn observe_control(
    context: &mut CliCommandContext<'_>,
    rpc: Option<&ObserveCliRpc>,
    target_lister: Option<&ObserveCliTargetLister>,
    options: ObserveControlOptions,
) -> Result<(), CliCommandError> {
    let request = ObserveControlRequest {
        live_log_writing: options.live_log_writing,
        filter: options.filter.clone(),
        quiet_filter_directives: (!options.quiet_filter_directives.is_empty())
            .then_some(options.quiet_filter_directives.clone()),
        clear_quiet_filter_directives: options.clear_quiet_filter_directives,
        reset: options.reset,
        capture_id: options.capture_id.clone(),
        clear_capture: options.clear_capture,
        expected_capture_id: options.expected_capture_id.clone(),
        clear_traces: options.clear_traces,
    };
    if options.target_all {
        let response = observe_rpc_all::<_, ObserveControlResponse>(
            rpc,
            target_lister,
            OBSERVE_CONTROL_ROUTE,
            &request,
        )?;
        if request.capture_id.is_some() || request.clear_capture {
            validate_cluster_capture_control(rpc, &request, &response)?;
        }
        if options.live_log_writing.is_some() {
            let mut issues = response
                .errors
                .iter()
                .map(|error| format!("{}: {}", error.target_node_id, error.error))
                .collect::<Vec<_>>();
            for target in &response.targets {
                if let Err(error) =
                    validate_live_log_writing(options.live_log_writing, &target.response)
                {
                    issues.push(format!("{}: {error}", target.target_node_id));
                }
            }
            if response.targets.is_empty() {
                issues.push("no node acknowledged the requested live-writing mode".into());
            }
            if !issues.is_empty() {
                return Err(CliCommandError::new(format!(
                    "live log writing was not confirmed on every target: {}; successful targets keep their acknowledged setting",
                    issues.join("; ")
                )));
            }
        }
        return print_observe_fanout(
            context,
            response,
            options.json,
            print_observe_control_response,
        );
    }
    let response = if rpc.is_some() {
        observe_rpc::<_, ObserveControlResponse>(
            rpc,
            options.target_node_id.as_deref(),
            OBSERVE_CONTROL_ROUTE,
            &request,
        )?
    } else if request.filter.is_some()
        || request.quiet_filter_directives.is_some()
        || request.clear_quiet_filter_directives
        || request.reset
        || request.capture_id.is_some()
        || request.clear_capture
        || request.clear_traces
        || request.live_log_writing.is_some()
    {
        update_observe_control(request).map_err(command_error)?
    } else {
        observe_control_snapshot()
    };
    validate_live_log_writing(options.live_log_writing, &response)?;
    print_observe_control_response(context, response, options.json)
}

fn validate_live_log_writing(
    requested: Option<bool>,
    response: &ObserveControlResponse,
) -> Result<(), CliCommandError> {
    if let Some(expected) = requested
        && response.live_log_writing != Some(expected)
    {
        return Err(CliCommandError::new(format!(
            "live log writing acknowledgement missing or mismatched (requested={expected}, reported={:?}); an older runtime may not support this control",
            response.live_log_writing
        )));
    }
    Ok(())
}

fn validate_cluster_capture_control(
    rpc: Option<&ObserveCliRpc>,
    request: &ObserveControlRequest,
    response: &ObserveFanoutResponse<ObserveControlResponse>,
) -> Result<(), CliCommandError> {
    let mut issues = response
        .errors
        .iter()
        .map(|error| format!("{}: {}", error.target_node_id, error.error))
        .collect::<Vec<_>>();
    for target in &response.targets {
        let matches = if let Some(capture_id) = request.capture_id.as_deref() {
            target.response.active_capture_id.as_deref() == Some(capture_id)
        } else if request.clear_capture {
            target.response.active_capture_id.is_none()
        } else {
            true
        };
        if !matches {
            issues.push(format!(
                "{}: capture acknowledgement mismatch (active={})",
                target.target_node_id,
                target
                    .response
                    .active_capture_id
                    .as_deref()
                    .unwrap_or("<none>")
            ));
        }
    }
    if issues.is_empty() {
        return Ok(());
    }

    let mut rollback_issues = Vec::new();
    if let Some(capture_id) = request.capture_id.as_ref() {
        let rollback = ObserveControlRequest {
            live_log_writing: None,
            filter: None,
            quiet_filter_directives: None,
            clear_quiet_filter_directives: false,
            reset: false,
            capture_id: None,
            clear_capture: true,
            expected_capture_id: Some(capture_id.clone()),
            clear_traces: false,
        };
        let rollback_targets = response
            .targets
            .iter()
            .map(|target| target.target_node_id.clone())
            .chain(
                response
                    .errors
                    .iter()
                    .map(|error| error.target_node_id.clone()),
            )
            .collect::<std::collections::BTreeSet<_>>();
        for target_node_id in rollback_targets {
            match observe_rpc::<_, ObserveControlResponse>(
                rpc,
                Some(target_node_id.as_str()),
                OBSERVE_CONTROL_ROUTE,
                &rollback,
            ) {
                Ok(response) if response.active_capture_id.is_none() => {}
                Ok(response) => rollback_issues.push(format!(
                    "{}: capture remained active as {}",
                    target_node_id,
                    response.active_capture_id.as_deref().unwrap_or("<none>")
                )),
                Err(error) => rollback_issues.push(format!("{target_node_id}: {error}")),
            }
        }
    }

    let mut detail = format!(
        "cluster-wide observe capture was not acknowledged by every target: {}",
        issues.join("; ")
    );
    if !rollback_issues.is_empty() {
        detail.push_str("; rollback failures: ");
        detail.push_str(&rollback_issues.join("; "));
    }
    Err(CliCommandError::new(detail))
}

fn print_observe_control_response(
    context: &mut CliCommandContext<'_>,
    response: ObserveControlResponse,
    json: bool,
) -> Result<(), CliCommandError> {
    if json {
        let json = serde_json::to_string_pretty(&response).map_err(command_error)?;
        context.stdout_line(format_args!("{json}"))?;
    } else {
        context.stdout_line(format_args!(
            "observe filter: {}{}",
            response.active_filter,
            if response.changed { " (updated)" } else { "" }
        ))?;
        context.stdout_line(format_args!(
            "observe base filter: {}",
            response.base_filter
        ))?;
        let quiet_filters = if response.quiet_filter_directives.is_empty() {
            "<none>".to_string()
        } else {
            response.quiet_filter_directives.join(",")
        };
        context.stdout_line(format_args!("observe quiet filters: {quiet_filters}"))?;
        context.stdout_line(format_args!(
            "observe live log writing: {}",
            match response.live_log_writing {
                Some(true) => "enabled",
                Some(false) => "disabled (routine history in bounded memory)",
                None => "unavailable (unsupported or uninitialized runtime)",
            }
        ))?;
        context.stdout_line(format_args!(
            "observe capture: {}",
            response.active_capture_id.as_deref().unwrap_or("<none>")
        ))?;
        if response.cleared_trace_count > 0 {
            context.stdout_line(format_args!(
                "observe traces cleared: {}",
                response.cleared_trace_count
            ))?;
        }
    }
    Ok(())
}

fn observe_cpu_profile(
    context: &mut CliCommandContext<'_>,
    rpc: Option<&ObserveCliRpc>,
    options: ObserveCpuProfileOptions,
) -> Result<(), CliCommandError> {
    let response = if rpc.is_some() {
        observe_rpc::<_, ObserveCpuProfileResponse>(
            rpc,
            options.target_node_id.as_deref(),
            OBSERVE_CPU_PROFILE_ROUTE,
            &options.request,
        )?
    } else {
        let data_dir = SinomoDataDir::from_env_or_home().map_err(command_error)?;
        ensure_observe_module_enabled(&data_dir)?;
        handle_observe_cpu_profile(data_dir.root(), options.request).map_err(command_error)?
    };
    print_observe_cpu_profile_response(context, response, options.json)
}

fn observe_network_timeline(
    context: &mut CliCommandContext<'_>,
    rpc: Option<&ObserveCliRpc>,
    options: ObserveNetworkTimelineOptions,
) -> Result<(), CliCommandError> {
    let response = if rpc.is_some() {
        observe_rpc::<_, ObserveNetworkTimelineResponse>(
            rpc,
            options.target_node_id.as_deref(),
            OBSERVE_NETWORK_TIMELINE_ROUTE,
            &options.request,
        )?
    } else {
        let data_dir = SinomoDataDir::from_env_or_home().map_err(command_error)?;
        ensure_observe_module_enabled(&data_dir)?;
        handle_observe_network_timeline(options.request).map_err(command_error)?
    };
    print_observe_network_timeline_response(context, response, options.json)
}

fn print_observe_network_timeline_response(
    context: &mut CliCommandContext<'_>,
    response: ObserveNetworkTimelineResponse,
    json: bool,
) -> Result<(), CliCommandError> {
    if json {
        let json = serde_json::to_string_pretty(&response).map_err(command_error)?;
        context.stdout_line(format_args!("{json}"))?;
        return Ok(());
    }
    context.stdout_line(format_args!(
        "network timeline: {}",
        if response.active { "active" } else { "stopped" }
    ))?;
    if let Some(capture_id) = response.capture_id.as_deref() {
        context.stdout_line(format_args!("capture: {capture_id}"))?;
    }
    context.stdout_line(format_args!(
        "events: {}/{} dropped={} overflowed={}",
        response.event_count, response.max_events, response.dropped_events, response.overflowed
    ))?;
    context.stdout_line(format_args!(
        "process: {} pid={} node={}",
        response.process_role,
        response.process_id,
        response.node_id.as_deref().unwrap_or("<none>")
    ))?;
    if let Some(error) = response.error.as_deref() {
        context.stdout_line(format_args!("error: {error}"))?;
    }
    if !response.events.is_empty() {
        context.stdout_line(format_args!(
            "timeline records emitted: {} (use --json for Perfetto conversion)",
            response.events.len()
        ))?;
    }
    Ok(())
}

fn print_observe_cpu_profile_response(
    context: &mut CliCommandContext<'_>,
    response: ObserveCpuProfileResponse,
    json: bool,
) -> Result<(), CliCommandError> {
    if json {
        let json = serde_json::to_string_pretty(&response).map_err(command_error)?;
        context.stdout_line(format_args!("{json}"))?;
        return Ok(());
    }
    context.stdout_line(format_args!(
        "CPU profile: {} (supported={})",
        format!("{:?}", response.state).to_ascii_lowercase(),
        response.supported
    ))?;
    if let Some(capture_id) = response.capture_id.as_deref() {
        context.stdout_line(format_args!("capture: {capture_id}"))?;
    }
    if let (Some(duration), Some(frequency)) = (response.duration_seconds, response.frequency_hz) {
        context.stdout_line(format_args!("capture bounds: {duration}s at {frequency}Hz"))?;
    }
    if let Some(started_at) = response.started_at {
        let started_at = started_at.format(&Rfc3339).map_err(command_error)?;
        context.stdout_line(format_args!("started: {started_at}"))?;
    }
    if let Some(finished_at) = response.finished_at {
        let finished_at = finished_at.format(&Rfc3339).map_err(command_error)?;
        context.stdout_line(format_args!("finished: {finished_at}"))?;
    }
    if let Some(sample_count) = response.sample_count {
        context.stdout_line(format_args!("samples: {sample_count}"))?;
    }
    if let Some(artifact_relative_path) = response.artifact_relative_path.as_deref() {
        context.stdout_line(format_args!(
            "artifact: {artifact_relative_path} ({} bytes)",
            response.artifact_bytes.unwrap_or_default()
        ))?;
    }
    if let Some(manifest_relative_path) = response.manifest_relative_path.as_deref() {
        context.stdout_line(format_args!("manifest: {manifest_relative_path}"))?;
    }
    if let Some(error) = response.error.as_deref() {
        context.stdout_line(format_args!("error: {error}"))?;
    }
    Ok(())
}

#[derive(Clone, serde::Serialize)]
struct ObserveFanoutResponse<Resp> {
    targets: Vec<ObserveTargetResponse<Resp>>,
    errors: Vec<ObserveTargetError>,
}

#[derive(Clone, serde::Serialize)]
struct ObserveTargetResponse<Resp> {
    target_node_id: String,
    response: Resp,
}

#[derive(Clone, serde::Serialize)]
struct ObserveTargetError {
    target_node_id: String,
    error: String,
}

fn observe_rpc_all<Req, Resp>(
    rpc: Option<&ObserveCliRpc>,
    target_lister: Option<&ObserveCliTargetLister>,
    route: &'static str,
    body: &Req,
) -> Result<ObserveFanoutResponse<Resp>, CliCommandError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    observe_rpc_all_with_cancel(rpc, target_lister, route, body, None)
}

fn observe_rpc_all_with_cancel<Req, Resp>(
    rpc: Option<&ObserveCliRpc>,
    target_lister: Option<&ObserveCliTargetLister>,
    route: &'static str,
    body: &Req,
    cancellation: Option<&AtomicBool>,
) -> Result<ObserveFanoutResponse<Resp>, CliCommandError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    let Some(target_lister) = target_lister else {
        return Err(CliCommandError::new(
            "observe --all requires daemon-backed cluster target discovery",
        ));
    };
    let targets = target_lister().map_err(CliCommandError::new)?;
    if targets.is_empty() {
        return Err(CliCommandError::new(
            "observe --all found no authorized cluster targets",
        ));
    }
    let Some(rpc) = rpc else {
        return Err(CliCommandError::new(
            "observe --all requires daemon-backed observe CLI registration",
        ));
    };
    let request =
        sinomo_core::network::ObjectRpcRequest::encode_body(route, OBSERVE_RPC_CATEGORY, body)
            .map_err(command_error)?
            .with_trace_context_if_missing();
    // A broken node must not make cluster-wide diagnostics wait once per target.
    // Each transport call has its own bounded no-progress timeout; issuing them
    // concurrently makes the fan-out bound the slowest target instead of the sum.
    let next_target = std::sync::atomic::AtomicUsize::new(0);
    let target_results =
        std::sync::Mutex::new((0..targets.len()).map(|_| None).collect::<Vec<Option<_>>>());
    std::thread::scope(|scope| -> Result<(), CliCommandError> {
        let worker_count = targets.len().min(OBSERVE_FANOUT_MAX_WORKERS);
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            workers.push(scope.spawn(|| {
                loop {
                    if cancellation.is_some_and(|cancelled| cancelled.load(Ordering::Acquire)) {
                        break;
                    }
                    let index = next_target.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(target_node_id) = targets.get(index).cloned() else {
                        break;
                    };
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        rpc(Some(target_node_id.as_str()), request.clone())
                    }))
                    .unwrap_or_else(|_| Err("observe RPC worker panicked".to_string()));
                    if let Ok(mut results) = target_results.lock() {
                        results[index] = Some((target_node_id, result));
                    }
                }
            }));
        }
        for worker in workers {
            if worker.join().is_err() {
                return Err(CliCommandError::new("observe RPC worker panicked"));
            }
        }
        Ok(())
    })?;
    let target_results = target_results
        .into_inner()
        .map_err(|_| CliCommandError::new("observe fan-out result lock is poisoned"))?
        .into_iter()
        .enumerate()
        .map(|(index, result)| {
            result.unwrap_or_else(|| {
                (
                    targets[index].clone(),
                    Err("observe RPC worker did not return a result".to_string()),
                )
            })
        })
        .collect::<Vec<_>>();
    let mut responses = Vec::new();
    let mut errors = Vec::new();
    for (target_node_id, result) in target_results {
        match result {
            Ok(response) => match response.decode_body() {
                Ok(response) => responses.push(ObserveTargetResponse {
                    target_node_id,
                    response,
                }),
                Err(error) => errors.push(ObserveTargetError {
                    target_node_id,
                    error: error.to_string(),
                }),
            },
            Err(error) => errors.push(ObserveTargetError {
                target_node_id,
                error,
            }),
        }
    }
    Ok(ObserveFanoutResponse {
        targets: responses,
        errors,
    })
}

fn print_observe_fanout<Resp>(
    context: &mut CliCommandContext<'_>,
    response: ObserveFanoutResponse<Resp>,
    json: bool,
    mut print_response: impl FnMut(
        &mut CliCommandContext<'_>,
        Resp,
        bool,
    ) -> Result<(), CliCommandError>,
) -> Result<(), CliCommandError>
where
    Resp: serde::Serialize,
{
    if json {
        let json = serde_json::to_string_pretty(&response).map_err(command_error)?;
        context.stdout_line(format_args!("{json}"))?;
        return Ok(());
    }
    for target in response.targets {
        context.stdout_line(format_args!("target {}:", target.target_node_id))?;
        print_response(context, target.response, false)?;
    }
    for error in response.errors {
        context.stdout_line(format_args!(
            "target {}: error: {}",
            error.target_node_id, error.error
        ))?;
    }
    Ok(())
}

fn observe_rpc<Req, Resp>(
    rpc: Option<&ObserveCliRpc>,
    target_node_id: Option<&str>,
    route: &'static str,
    body: &Req,
) -> Result<Resp, CliCommandError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    let Some(rpc) = rpc else {
        return Err(CliCommandError::new(
            "observe target queries require daemon-backed observe CLI registration",
        ));
    };
    let request =
        sinomo_core::network::ObjectRpcRequest::encode_body(route, OBSERVE_RPC_CATEGORY, body)
            .map_err(command_error)?;
    let response = rpc(target_node_id, request).map_err(CliCommandError::new)?;
    response.decode_body().map_err(command_error)
}

fn ensure_observe_module_enabled(data_dir: &SinomoDataDir) -> Result<(), CliCommandError> {
    if observe_module_enabled(data_dir.root()).map_err(command_error)? {
        Ok(())
    } else {
        Err(CliCommandError::new(
            "sinomo-observe module is disabled; run `sinomo module native-rust enable module.observe`",
        ))
    }
}

fn print_operational_event_record(
    context: &mut CliCommandContext<'_>,
    record: &OperationalEventRecord,
) -> Result<(), CliCommandError> {
    let time = record.appended_at.format(&Rfc3339).map_err(command_error)?;
    let operation = record
        .operation_id
        .as_deref()
        .map(|operation_id| format!(" op={operation_id}"))
        .unwrap_or_default();
    context.stdout_line(format_args!(
        "{time} {:<5} {:<16} {}{} {}",
        record.severity,
        record.component,
        record.event.kind.as_str(),
        operation,
        record.message
    ))
}

fn print_operational_log_record(
    context: &mut CliCommandContext<'_>,
    record: &OperationalLogRecord,
) -> Result<(), CliCommandError> {
    let time = record.timestamp.format(&Rfc3339).map_err(command_error)?;
    let operation = record
        .operation_id
        .as_deref()
        .map(|operation_id| format!(" op={operation_id}"))
        .unwrap_or_default();
    let fields = if record.fields.is_null() {
        String::new()
    } else {
        format!(" fields={}", record.fields)
    };
    context.stdout_line(format_args!(
        "{time} {:<5} {:<16}{} {}{}",
        record.severity, record.component, operation, record.message, fields
    ))
}

fn command_error(error: impl fmt::Display) -> CliCommandError {
    CliCommandError::new(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_log_writing_control_is_explicit_and_separate_from_capture_lifecycle() {
        assert_eq!(
            parse_observe_control_options(&[]).unwrap().live_log_writing,
            None
        );
        for (flag, enabled) in [
            ("--live-log-writing", true),
            ("--no-live-log-writing", false),
        ] {
            let options = parse_observe_control_options(&[flag.into()]).unwrap();
            assert_eq!(options.live_log_writing, Some(enabled));
            assert!(options.filter.is_none());
            assert!(
                parse_observe_control_options(&[
                    flag.into(),
                    "--capture".into(),
                    "capture-a".into()
                ])
                .is_err()
            );
        }
        assert!(
            parse_observe_control_options(&[
                "--live-log-writing".into(),
                "--no-live-log-writing".into()
            ])
            .is_err()
        );
    }

    #[test]
    fn live_log_writing_requires_an_explicit_matching_peer_acknowledgement() {
        let mut response: ObserveControlResponse = serde_json::from_str(
            r#"{"active_filter":"trace","base_filter":"trace","quiet_filter_directives":[],"changed":false}"#,
        ).unwrap();
        assert!(validate_live_log_writing(None, &response).is_ok());
        assert!(validate_live_log_writing(Some(false), &response).is_err());
        assert!(validate_live_log_writing(Some(true), &response).is_err());
        response.live_log_writing = Some(true);
        assert!(validate_live_log_writing(Some(true), &response).is_ok());
        assert!(validate_live_log_writing(Some(false), &response).is_err());
        response.live_log_writing = Some(false);
        assert!(validate_live_log_writing(Some(false), &response).is_ok());
    }

    #[test]
    fn trace_cursor_parses_and_rejects_invalid_or_unrelated_use() {
        let options =
            parse_observe_trace_options(&["--after-sequence".into(), "42".into()]).unwrap();
        assert_eq!(options.after_sequence, Some(42));
        assert!(
            parse_observe_trace_options(&["--after-sequence".into(), "42".into(), "--all".into()])
                .is_err()
        );
        assert!(parse_observe_trace_options(&["--after-sequence".into(), "-1".into()]).is_err());
        assert!(
            parse_observe_options(
                &["--after-sequence".into(), "42".into()],
                false,
                false,
                false
            )
            .is_err()
        );
    }

    #[test]
    fn observe_handler_parses_help_as_local_capable_command() {
        let handler = observe_cli_command_handler();
        let command = handler.parse(&[]).expect("parse help");
        assert_eq!(command.name(), "observe");
        assert_eq!(command.policy(), CliCommandPolicy::local_capable());
    }

    #[test]
    fn observe_handler_accepts_alias_and_options_through_registry() {
        let registry =
            sinomo_cli_registry::CliCommandRegistry::new().register(observe_cli_command_handler());
        let command = registry
            .parse(&[
                "obs".to_string(),
                "events".to_string(),
                "--kind".to_string(),
                "daemon.".to_string(),
                "--limit".to_string(),
                "3".to_string(),
            ])
            .expect("parse")
            .expect("matched");
        assert_eq!(command.name(), "observe");
        assert_eq!(command.policy(), CliCommandPolicy::local_capable());
    }

    #[test]
    fn observe_handler_parses_live_observe_commands() {
        let command = parse_observe_command(&[
            "metrics".to_string(),
            "--name".to_string(),
            "sinomo.object_rpc.".to_string(),
            "--target".to_string(),
            "01hxy6qa9k0t9vr4g9n8d3q5d4".to_string(),
            "--limit".to_string(),
            "8".to_string(),
            "--watch".to_string(),
            "5".to_string(),
            "--samples".to_string(),
            "3".to_string(),
            "--json".to_string(),
        ])
        .expect("parse metrics");
        match command {
            ObserveCliCommand::Metrics(options) => {
                assert_eq!(options.name_prefix.as_deref(), Some("sinomo.object_rpc."));
                assert_eq!(
                    options.target_node_id.as_deref(),
                    Some("01hxy6qa9k0t9vr4g9n8d3q5d4")
                );
                assert_eq!(options.limit, 8);
                assert_eq!(options.watch_interval_seconds, Some(5));
                assert_eq!(options.watch_samples, Some(3));
                assert!(options.json);
            }
            other => panic!("expected metrics command, got {other:?}"),
        }

        let command = parse_observe_command(&[
            "traces".to_string(),
            "--target-prefix".to_string(),
            "sinomo.network".to_string(),
            "--traceparent".to_string(),
            "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01".to_string(),
            "--capture".to_string(),
            "cluster-capture-a".to_string(),
        ])
        .expect("parse traces");
        match command {
            ObserveCliCommand::Traces(options) => {
                assert_eq!(options.component.as_deref(), Some("sinomo.network"));
                assert_eq!(
                    options.traceparent.as_deref(),
                    Some("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
                );
                assert_eq!(options.capture_id.as_deref(), Some("cluster-capture-a"));
                assert_eq!(options.limit, 0);
            }
            other => panic!("expected traces command, got {other:?}"),
        }

        let command = parse_observe_command(&[
            "control".to_string(),
            "--filter".to_string(),
            "warn,sinomo_network=trace".to_string(),
            "--quiet-filter".to_string(),
            "iroh::net_report::report=error".to_string(),
            "--quiet-filter".to_string(),
            "hyper=error".to_string(),
            "--target".to_string(),
            "01hxy6qa9k0t9vr4g9n8d3q5d4".to_string(),
            "--capture".to_string(),
            "cluster-capture-a".to_string(),
            "--clear-traces".to_string(),
        ])
        .expect("parse control");
        match command {
            ObserveCliCommand::Control(options) => {
                assert_eq!(options.filter.as_deref(), Some("warn,sinomo_network=trace"));
                assert_eq!(
                    options.quiet_filter_directives,
                    vec![
                        "iroh::net_report::report=error".to_string(),
                        "hyper=error".to_string()
                    ]
                );
                assert_eq!(
                    options.target_node_id.as_deref(),
                    Some("01hxy6qa9k0t9vr4g9n8d3q5d4")
                );
                assert!(!options.reset);
                assert_eq!(options.capture_id.as_deref(), Some("cluster-capture-a"));
                assert!(options.clear_traces);
            }
            other => panic!("expected control command, got {other:?}"),
        }

        let command = parse_observe_command(&[
            "profile".to_string(),
            "cpu".to_string(),
            "start".to_string(),
            "--capture".to_string(),
            "route-latency".to_string(),
            "--duration-seconds".to_string(),
            "3".to_string(),
            "--frequency-hz".to_string(),
            "200".to_string(),
            "--target".to_string(),
            "node-a".to_string(),
            "--json".to_string(),
        ])
        .expect("parse CPU profile start");
        match command {
            ObserveCliCommand::CpuProfile(options) => {
                assert_eq!(options.request.action, ObserveCpuProfileAction::Start);
                assert_eq!(options.request.capture_id.as_deref(), Some("route-latency"));
                assert_eq!(options.request.duration_seconds, Some(3));
                assert_eq!(options.request.frequency_hz, Some(200));
                assert_eq!(options.target_node_id.as_deref(), Some("node-a"));
                assert!(options.json);
            }
            other => panic!("expected CPU profile command, got {other:?}"),
        }

        let command = parse_observe_command(&["profile".to_string(), "cpu".to_string()])
            .expect("CPU profile defaults to status");
        match command {
            ObserveCliCommand::CpuProfile(options) => {
                assert_eq!(options.request.action, ObserveCpuProfileAction::Status);
            }
            other => panic!("expected CPU profile status command, got {other:?}"),
        }

        let command = parse_observe_command(&[
            "timeline".to_string(),
            "network".to_string(),
            "start".to_string(),
            "--capture".to_string(),
            "direct-rpc".to_string(),
            "--duration-seconds".to_string(),
            "15".to_string(),
            "--max-events".to_string(),
            "4096".to_string(),
            "--target".to_string(),
            "node-a".to_string(),
            "--json".to_string(),
        ])
        .expect("parse network timeline start");
        match command {
            ObserveCliCommand::NetworkTimeline(options) => {
                assert_eq!(options.request.action, ObserveNetworkTimelineAction::Start);
                assert_eq!(options.request.capture_id.as_deref(), Some("direct-rpc"));
                assert_eq!(options.request.duration_seconds, Some(15));
                assert_eq!(options.request.max_events, Some(4096));
                assert_eq!(options.target_node_id.as_deref(), Some("node-a"));
                assert!(options.json);
            }
            other => panic!("expected network timeline command, got {other:?}"),
        }

        assert_eq!(
            parse_observe_command(&[
                "profile".to_string(),
                "cpu".to_string(),
                "start".to_string(),
                "--all".to_string(),
            ]),
            Err(CliParseError::Usage)
        );

        let command = parse_observe_command(&[
            "metrics".to_string(),
            "--name".to_string(),
            "sinomo.".to_string(),
            "--all".to_string(),
        ])
        .expect("parse metrics all");
        match command {
            ObserveCliCommand::Metrics(options) => {
                assert_eq!(options.name_prefix.as_deref(), Some("sinomo."));
                assert!(options.target_all);
                assert!(options.target_node_id.is_none());
            }
            other => panic!("expected metrics command, got {other:?}"),
        }

        let command = parse_observe_command(&[
            "prometheus".to_string(),
            "serve".to_string(),
            "--listen".to_string(),
            "[::1]:19464".to_string(),
            "--refresh".to_string(),
            "30".to_string(),
            "--name".to_string(),
            "sinomo.kv.".to_string(),
            "--all".to_string(),
        ])
        .expect("parse Prometheus exporter");
        match command {
            ObserveCliCommand::Prometheus(options) => {
                assert_eq!(options.listen, "[::1]:19464".parse().expect("address"));
                assert_eq!(options.refresh_interval_seconds, 30);
                assert_eq!(options.name_prefix.as_deref(), Some("sinomo.kv."));
                assert!(options.target_all);
                assert!(options.target_node_id.is_none());
            }
            other => panic!("expected Prometheus command, got {other:?}"),
        }

        let defaults = parse_observe_command(&["prometheus".to_string(), "serve".to_string()])
            .expect("parse Prometheus defaults");
        assert_eq!(
            defaults,
            ObserveCliCommand::Prometheus(ObservePrometheusOptions::default())
        );
        assert_eq!(
            parse_observe_command(&[
                "prometheus".to_string(),
                "serve".to_string(),
                "--listen".to_string(),
                "0.0.0.0:9464".to_string(),
            ]),
            Err(CliParseError::Parse {
                detail: "Prometheus exporter listen address `0.0.0.0:9464` is not loopback; expose it through an authenticated reverse proxy".to_string(),
            })
        );
        assert_eq!(
            parse_observe_command(&[
                "prometheus".to_string(),
                "serve".to_string(),
                "--refresh".to_string(),
                "0".to_string(),
            ]),
            Err(CliParseError::Parse {
                detail: format!(
                    "Prometheus exporter refresh interval must be between {PROMETHEUS_MIN_REFRESH_SECONDS} and {PROMETHEUS_MAX_REFRESH_SECONDS} seconds"
                ),
            })
        );
        assert_eq!(
            parse_observe_command(&[
                "prometheus".to_string(),
                "serve".to_string(),
                "--target".to_string(),
                "node-a".to_string(),
                "--all".to_string(),
            ]),
            Err(CliParseError::Usage)
        );
        assert_eq!(
            parse_observe_command(&["prometheus".to_string()]),
            Err(CliParseError::Usage)
        );

        let result = parse_observe_command(&[
            "traces".to_string(),
            "--all".to_string(),
            "--target".to_string(),
            "01hxy6qa9k0t9vr4g9n8d3q5d4".to_string(),
        ]);
        assert_eq!(result, Err(CliParseError::Usage));

        assert_eq!(
            parse_observe_command(&[
                "metrics".to_string(),
                "--samples".to_string(),
                "2".to_string(),
            ]),
            Err(CliParseError::Usage)
        );

        let result = parse_observe_command(&[
            "control".to_string(),
            "--clear-quiet-filters".to_string(),
            "--quiet-filter".to_string(),
            "hyper=error".to_string(),
        ]);
        assert_eq!(result, Err(CliParseError::Usage));

        let result = parse_observe_command(&[
            "control".to_string(),
            "--capture".to_string(),
            "cluster-capture-a".to_string(),
            "--clear-capture".to_string(),
        ]);
        assert_eq!(result, Err(CliParseError::Usage));

        let result = parse_observe_command(&[
            "control".to_string(),
            "--expected-capture".to_string(),
            "cluster-capture-a".to_string(),
        ]);
        assert_eq!(result, Err(CliParseError::Usage));

        let result = parse_observe_command(&[
            "control".to_string(),
            "--clear-capture".to_string(),
            "--reset".to_string(),
        ]);
        assert_eq!(result, Err(CliParseError::Usage));
    }

    #[test]
    fn metric_interval_delta_reports_counter_and_histogram_window() {
        let previous = vec![
            crate::ObserveMetricRecord::counter("sinomo.test.counter", Default::default(), 10),
            crate::ObserveMetricRecord {
                name: "sinomo.test.duration.us".to_string(),
                kind: crate::ObserveMetricKind::Histogram,
                labels: Default::default(),
                value: None,
                count: Some(4),
                sum: Some(1_000.0),
                min: Some(10.0),
                max: Some(700.0),
                buckets: vec![
                    crate::ObserveHistogramBucket {
                        upper_bound: 100.0,
                        count: 2,
                    },
                    crate::ObserveHistogramBucket {
                        upper_bound: 1_000.0,
                        count: 4,
                    },
                ],
            },
        ];
        let current = vec![
            crate::ObserveMetricRecord::counter("sinomo.test.counter", Default::default(), 16),
            crate::ObserveMetricRecord {
                name: "sinomo.test.duration.us".to_string(),
                kind: crate::ObserveMetricKind::Histogram,
                labels: Default::default(),
                value: None,
                count: Some(7),
                sum: Some(1_650.0),
                min: Some(10.0),
                max: Some(700.0),
                buckets: vec![
                    crate::ObserveHistogramBucket {
                        upper_bound: 100.0,
                        count: 4,
                    },
                    crate::ObserveHistogramBucket {
                        upper_bound: 1_000.0,
                        count: 7,
                    },
                ],
            },
        ];

        let delta = crate::observe_metric_records_interval_delta(&previous, &current);
        assert_eq!(delta[0].value, Some(6.0));
        assert_eq!(delta[1].count, Some(3));
        assert_eq!(delta[1].sum, Some(650.0));
        assert_eq!(delta[1].buckets[0].count, 2);
        assert_eq!(delta[1].buckets[1].count, 3);
        assert_eq!(delta[1].histogram_quantile(0.95), Some(1_000.0));
        assert_eq!(delta[1].min, None);
        assert_eq!(delta[1].max, None);
    }

    #[test]
    fn logs_use_local_daemon_and_never_substitute_local_data_for_a_remote_failure() {
        let rpc: ObserveCliRpc = Arc::new(|target, request| {
            assert_eq!(request.route, OBSERVE_LOGS_ROUTE);
            if target.is_some() {
                return Err("remote unavailable".into());
            }
            sinomo_core::network::ObjectRpcResponse::encode_body(
                OBSERVE_LOGS_ROUTE,
                &ObserveLogsResponse {
                    runtime_available: true,
                    ..Default::default()
                },
            )
            .map_err(|error| error.to_string())
        });
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let mut context = CliCommandContext::new(&mut stdout, &mut stderr);
        observe_logs(&mut context, Some(&rpc), None, ObserveOptions::default()).unwrap();
        let error = observe_logs(
            &mut context,
            Some(&rpc),
            None,
            ObserveOptions {
                target_node_id: Some("remote-node".into()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("remote unavailable"));
        assert_eq!(String::from_utf8(stdout).unwrap().trim(), "no logs found");
        assert!(stderr.is_empty());
    }

    #[test]
    fn observe_all_queries_targets_concurrently_and_preserves_partial_results() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let rpc: ObserveCliRpc = {
            let in_flight = Arc::clone(&in_flight);
            let max_in_flight = Arc::clone(&max_in_flight);
            Arc::new(move |target_node_id, _request| {
                let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                max_in_flight.fetch_max(current, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(30));
                in_flight.fetch_sub(1, Ordering::SeqCst);
                let target_node_id = target_node_id.expect("target node id");
                if target_node_id == "node-b" {
                    return Err("node-b unavailable".to_string());
                }
                sinomo_core::network::ObjectRpcResponse::encode_body(
                    OBSERVE_METRICS_ROUTE,
                    &crate::observe_metrics_response_from_metrics(Vec::new()),
                )
                .map_err(|error| error.to_string())
            })
        };
        let target_lister: ObserveCliTargetLister = Arc::new(|| {
            Ok(vec![
                "node-a".to_string(),
                "node-b".to_string(),
                "node-c".to_string(),
            ])
        });

        let response = observe_rpc_all::<_, ObserveMetricsResponse>(
            Some(&rpc),
            Some(&target_lister),
            OBSERVE_METRICS_ROUTE,
            &ObserveMetricsRequest::default(),
        )
        .expect("fan-out response");

        assert!(max_in_flight.load(Ordering::SeqCst) >= 2);
        assert_eq!(
            response
                .targets
                .iter()
                .map(|target| target.target_node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["node-a", "node-c"]
        );
        assert_eq!(response.errors.len(), 1);
        assert_eq!(response.errors[0].target_node_id, "node-b");
        assert_eq!(response.errors[0].error, "node-b unavailable");
    }

    #[test]
    fn partial_cluster_prometheus_collection_marks_failed_and_partial_targets() {
        let rpc: ObserveCliRpc = Arc::new(|target_node_id, request| {
            let target_node_id = target_node_id.expect("target node id");
            let request: ObserveMetricsRequest = request.decode_body().expect("metrics request");
            assert_eq!(request.name_prefix.as_deref(), Some("sinomo.test."));
            assert_eq!(request.limit, 0);
            if target_node_id == "node-b" {
                return Err("node unavailable".to_string());
            }
            let mut response =
                crate::observe_metrics_response_from_metrics(vec![ObserveMetricRecord::gauge(
                    "sinomo.test.load",
                    Default::default(),
                    0.5,
                )]);
            response.node_id = Some(target_node_id.to_string());
            response.process_role = Some("main".to_string());
            response.partial = true;
            response.descriptors = vec![ObserveMetricDescriptor::new(
                "sinomo.test.load",
                crate::ObserveMetricKind::Gauge,
                Some(ObserveMetricUnit::Ratio),
                "Test load ratio.",
            )];
            sinomo_core::network::ObjectRpcResponse::encode_body(OBSERVE_METRICS_ROUTE, &response)
                .map_err(|error| error.to_string())
        });
        let target_lister: ObserveCliTargetLister =
            Arc::new(|| Ok(vec!["node-a".to_string(), "node-b".to_string()]));
        let options = ObservePrometheusOptions {
            name_prefix: Some("sinomo.test.".to_string()),
            target_all: true,
            ..ObservePrometheusOptions::default()
        };

        let output = collect_prometheus_metrics(Some(&rpc), Some(&target_lister), &options, None)
            .expect("partial OpenMetrics snapshot");

        assert!(output.contains("sinomo_observe_prometheus_target_up{node_id=\"node-a\"} 1\n"));
        assert!(output.contains("sinomo_observe_prometheus_target_up{node_id=\"node-b\"} 0\n"));
        assert!(output.contains("sinomo_observe_prometheus_collection_success 0\n"));
        assert!(
            output
                .contains("sinomo_test_load_ratio{node_id=\"node-a\",process_role=\"main\"} 0.5\n")
        );
    }

    #[test]
    fn observe_all_bounds_worker_count_for_large_membership() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let rpc: ObserveCliRpc = {
            let in_flight = Arc::clone(&in_flight);
            let max_in_flight = Arc::clone(&max_in_flight);
            Arc::new(move |_target_node_id, _request| {
                let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                max_in_flight.fetch_max(current, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(20));
                in_flight.fetch_sub(1, Ordering::SeqCst);
                sinomo_core::network::ObjectRpcResponse::encode_body(
                    OBSERVE_METRICS_ROUTE,
                    &crate::observe_metrics_response_from_metrics(Vec::new()),
                )
                .map_err(|error| error.to_string())
            })
        };
        let target_lister: ObserveCliTargetLister = Arc::new(|| {
            Ok((0..(OBSERVE_FANOUT_MAX_WORKERS * 2 + 1))
                .map(|index| format!("node-{index:02}"))
                .collect())
        });

        let response = observe_rpc_all::<_, ObserveMetricsResponse>(
            Some(&rpc),
            Some(&target_lister),
            OBSERVE_METRICS_ROUTE,
            &ObserveMetricsRequest::default(),
        )
        .expect("fan-out response");

        assert_eq!(response.errors.len(), 0);
        assert_eq!(response.targets.len(), OBSERVE_FANOUT_MAX_WORKERS * 2 + 1);
        assert!(max_in_flight.load(Ordering::SeqCst) >= 2);
        assert!(max_in_flight.load(Ordering::SeqCst) <= OBSERVE_FANOUT_MAX_WORKERS);
    }

    #[test]
    fn partial_cluster_capture_is_rejected_and_acknowledged_nodes_are_rolled_back() {
        let rollbacks = Arc::new(std::sync::Mutex::new(Vec::new()));
        let rpc: ObserveCliRpc = {
            let rollbacks = Arc::clone(&rollbacks);
            Arc::new(move |target_node_id, request| {
                let target_node_id = target_node_id.expect("rollback target").to_string();
                let body: ObserveControlRequest = request.decode_body().expect("control request");
                assert!(body.clear_capture);
                assert_eq!(
                    body.expected_capture_id.as_deref(),
                    Some("cluster-capture-a")
                );
                rollbacks.lock().expect("rollbacks").push(target_node_id);
                sinomo_core::network::ObjectRpcResponse::encode_body(
                    OBSERVE_CONTROL_ROUTE,
                    &ObserveControlResponse {
                        live_log_writing: None,
                        active_filter: "warn,sinomo=info".to_string(),
                        base_filter: "warn,sinomo=info".to_string(),
                        quiet_filter_directives: Vec::new(),
                        active_capture_id: None,
                        cleared_trace_count: 0,
                        changed: true,
                    },
                )
                .map_err(|error| error.to_string())
            })
        };
        let request = ObserveControlRequest {
            live_log_writing: None,
            filter: None,
            quiet_filter_directives: None,
            clear_quiet_filter_directives: false,
            reset: false,
            capture_id: Some("cluster-capture-a".to_string()),
            clear_capture: false,
            expected_capture_id: None,
            clear_traces: true,
        };
        let response = ObserveFanoutResponse {
            targets: vec![
                ObserveTargetResponse {
                    target_node_id: "node-a".to_string(),
                    response: ObserveControlResponse {
                        live_log_writing: None,
                        active_filter: "warn,sinomo=info".to_string(),
                        base_filter: "warn,sinomo=info".to_string(),
                        quiet_filter_directives: Vec::new(),
                        active_capture_id: Some("cluster-capture-a".to_string()),
                        cleared_trace_count: 0,
                        changed: true,
                    },
                },
                ObserveTargetResponse {
                    target_node_id: "node-b".to_string(),
                    response: ObserveControlResponse {
                        live_log_writing: None,
                        active_filter: "warn,sinomo=info".to_string(),
                        base_filter: "warn,sinomo=info".to_string(),
                        quiet_filter_directives: Vec::new(),
                        active_capture_id: None,
                        cleared_trace_count: 0,
                        changed: true,
                    },
                },
            ],
            errors: vec![ObserveTargetError {
                target_node_id: "node-c".to_string(),
                error: "unreachable".to_string(),
            }],
        };

        let error = validate_cluster_capture_control(Some(&rpc), &request, &response)
            .expect_err("partial capture must fail");

        assert!(error.to_string().contains("node-b"));
        assert!(error.to_string().contains("node-c"));
        assert_eq!(
            rollbacks.lock().expect("rollbacks").as_slice(),
            ["node-a", "node-b", "node-c"]
        );
    }

    #[test]
    fn capture_collection_rejects_missing_or_truncated_node_records() {
        let response = ObserveFanoutResponse {
            targets: vec![ObserveTargetResponse {
                target_node_id: "node-a".to_string(),
                response: ObserveTracesResponse {
                    capacity: None,
                    last_sequence: 0,
                    first_available_sequence: None,
                    process: None,
                    records: Vec::new(),
                    capture_active: Some(true),
                    dropped_records: 3,
                    truncated: true,
                },
            }],
            errors: vec![ObserveTargetError {
                target_node_id: "node-b".to_string(),
                error: "unreachable".to_string(),
            }],
        };

        let error = validate_cluster_trace_collection(&response)
            .expect_err("incomplete collection must fail");

        assert!(error.to_string().contains("node-a"));
        assert!(error.to_string().contains("dropped_records=3"));
        assert!(error.to_string().contains("node-b"));
    }

    #[test]
    fn prometheus_descriptor_semantics_merge_rolling_versions_fail_closed() {
        let mut existing = ObserveMetricDescriptor::new(
            "sinomo.test.depth",
            crate::ObserveMetricKind::Gauge,
            Some(ObserveMetricUnit::Count),
            "Depth.",
        );
        let incoming = existing.clone().with_semantics(ObserveMetricSemantics::new(
            "test",
            ObserveMetricAggregation::Sum,
            ["queue"],
        ));
        merge_prometheus_descriptor_semantics(&mut existing, &incoming);
        assert_eq!(existing.semantics, incoming.semantics);

        let conflicting = existing.clone().with_semantics(ObserveMetricSemantics::new(
            "other",
            ObserveMetricAggregation::Max,
            ["priority"],
        ));
        merge_prometheus_descriptor_semantics(&mut existing, &conflicting);
        assert_eq!(existing.semantics.domain, None);
        assert_eq!(
            existing.effective_aggregation(),
            ObserveMetricAggregation::None
        );
        assert_eq!(
            existing.semantics.labels,
            vec!["priority".to_string(), "queue".to_string()]
        );
    }

    #[test]
    fn observe_handler_exposes_help_and_autocomplete() {
        let handler = observe_cli_command_handler();

        let help = handler.help();
        assert_eq!(help.title, "Observe:");
        assert!(
            help.lines
                .iter()
                .any(|line| line.contains("observe events"))
        );

        let autocomplete = handler.autocomplete();
        assert_eq!(autocomplete.word, "observe");
        assert_eq!(autocomplete.aliases, vec!["obs"]);
        let events = autocomplete
            .children
            .iter()
            .find(|child| child.word == "events")
            .expect("events completion");
        assert!(events.options.iter().any(|option| option.name == "--kind"));
    }

    #[test]
    fn observe_handler_rejects_log_kind_filter() {
        let handler = observe_cli_command_handler();
        let result = handler.parse(&[
            "logs".to_string(),
            "--kind".to_string(),
            "daemon".to_string(),
        ]);
        match result {
            Ok(_) => panic!("expected usage error"),
            Err(error) => assert_eq!(error, CliParseError::Usage),
        }
    }
}
