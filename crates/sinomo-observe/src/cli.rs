use crate::{
    OBSERVE_CONTROL_ROUTE, OBSERVE_EVENTS_ROUTE, OBSERVE_LOGS_ROUTE, OBSERVE_METRICS_ROUTE,
    OBSERVE_RPC_CATEGORY, OBSERVE_TRACES_ROUTE, ObserveControlRequest, ObserveControlResponse,
    ObserveEventsRequest, ObserveEventsResponse, ObserveLogsRequest, ObserveLogsResponse,
    ObserveMetricsRequest, ObserveMetricsResponse, ObserveTracesRequest, ObserveTracesResponse,
    OperationalEventQuery, OperationalEventRecord, OperationalLogQuery, OperationalLogRecord,
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
use std::sync::Arc;
use time::format_description::well_known::Rfc3339;

const OBSERVE_DEFAULT_LIMIT: usize = 40;
const OBSERVE_POLICY: CliCommandPolicy = CliCommandPolicy::local_capable();

#[derive(Debug, Clone, Eq, PartialEq)]
enum ObserveCliCommand {
    Help,
    Events(ObserveOptions),
    Logs(ObserveOptions),
    Metrics(ObserveOptions),
    Traces(ObserveOptions),
    Control(ObserveControlOptions),
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
    limit: usize,
    json: bool,
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
            limit: OBSERVE_DEFAULT_LIMIT,
            json: false,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ObserveControlOptions {
    filter: Option<String>,
    quiet_filter_directives: Vec<String>,
    clear_quiet_filter_directives: bool,
    reset: bool,
    target_node_id: Option<String>,
    target_all: bool,
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
            "  sinomo observe metrics [--name <prefix>] [--target <node-id> | --all] [--limit <n>] [--json]",
            "      Read live runtime metrics.",
            "  sinomo observe traces [--target-prefix <prefix>] [--traceparent <id>] [--target <node-id> | --all] [--limit <n>] [--json]",
            "      Read live bounded trace events.",
            "  sinomo observe control [--filter <directive>] [--quiet-filter <directive>...] [--clear-quiet-filters | --reset] [--target <node-id> | --all] [--json]",
            "      Read or update live trace filtering.",
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
            CliAutocompleteSpec::new("traces", "read live trace events").with_options([
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
                CliAutocompleteOption::flag("--all", "query every cluster node"),
                CliAutocompleteOption::flag("--reset", "reset trace filter"),
                CliAutocompleteOption::flag("--json", "emit JSON"),
            ]),
        ])
}

fn parse_observe_command(args: &[String]) -> Result<ObserveCliCommand, CliParseError> {
    match args.split_first() {
        None => Ok(ObserveCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(ObserveCliCommand::Help)
        }
        Some((command, rest)) if matches!(command.as_str(), "events" | "event") => Ok(
            ObserveCliCommand::Events(parse_observe_options(rest, true)?),
        ),
        Some((command, rest)) if matches!(command.as_str(), "logs" | "log") => {
            Ok(ObserveCliCommand::Logs(parse_observe_options(rest, false)?))
        }
        Some((command, rest)) if matches!(command.as_str(), "metrics" | "metric") => Ok(
            ObserveCliCommand::Metrics(parse_observe_options(rest, false)?),
        ),
        Some((command, rest)) if matches!(command.as_str(), "traces" | "trace") => Ok(
            ObserveCliCommand::Traces(parse_observe_options(rest, false)?),
        ),
        Some((command, rest)) if matches!(command.as_str(), "control" | "ctl") => Ok(
            ObserveCliCommand::Control(parse_observe_control_options(rest)?),
        ),
        _ => Err(CliParseError::Usage),
    }
}

fn parse_observe_options(
    args: &[String],
    allow_kind: bool,
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
            "--json" => options.json = true,
            _ => return Err(CliParseError::Usage),
        }
        index += 1;
    }
    if options.target_all && options.target_node_id.is_some() {
        return Err(CliParseError::Usage);
    }
    Ok(options)
}

fn parse_observe_control_options(args: &[String]) -> Result<ObserveControlOptions, CliParseError> {
    let mut options = ObserveControlOptions {
        filter: None,
        quiet_filter_directives: Vec::new(),
        clear_quiet_filter_directives: false,
        reset: false,
        target_node_id: None,
        target_all: false,
        json: false,
    };
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
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
        };
        let response = observe_rpc_all::<_, ObserveLogsResponse>(
            rpc,
            target_lister,
            OBSERVE_LOGS_ROUTE,
            &request,
        )?;
        return print_observe_fanout(
            context,
            response,
            options.json,
            |context, response, json| print_observe_log_records(context, response.records, json),
        );
    }
    if options.target_node_id.is_some() {
        let response: ObserveLogsResponse = observe_rpc(
            rpc,
            options.target_node_id.as_deref(),
            OBSERVE_LOGS_ROUTE,
            &ObserveLogsRequest {
                component: options.component.clone(),
                operation_id: options.operation_id.clone(),
                limit: options.limit,
            },
        )?;
        return print_observe_log_records(context, response.records, options.json);
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
        context.stdout_line(format_args!("no operational logs found"))?;
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
        let response = observe_rpc_all::<_, ObserveMetricsResponse>(
            rpc,
            target_lister,
            OBSERVE_METRICS_ROUTE,
            &request,
        )?;
        return print_observe_fanout(
            context,
            response,
            options.json,
            |context, response, json| print_observe_metric_records(context, response.metrics, json),
        );
    }
    let response = if rpc.is_some() {
        observe_rpc::<_, ObserveMetricsResponse>(
            rpc,
            options.target_node_id.as_deref(),
            OBSERVE_METRICS_ROUTE,
            &request,
        )?
    } else {
        read_observe_metrics(&request)
    };
    print_observe_metric_records(context, response.metrics, options.json)
}

fn print_observe_metric_records(
    context: &mut CliCommandContext<'_>,
    metrics: Vec<crate::ObserveMetricRecord>,
    json: bool,
) -> Result<(), CliCommandError> {
    if json {
        let json = serde_json::to_string_pretty(&ObserveMetricsResponse {
            metrics: metrics.clone(),
        })
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
                context.stdout_line(format_args!(
                    "{:<9} {:<48} count={} sum={} min={} max={}{}",
                    "histogram",
                    metric.name,
                    metric.count.unwrap_or_default(),
                    metric.sum.unwrap_or_default(),
                    metric.min.unwrap_or_default(),
                    metric.max.unwrap_or_default(),
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
        target_prefix: options.component.clone(),
        traceparent: options.traceparent.clone(),
        limit: options.limit,
    };
    if options.target_all {
        let response = observe_rpc_all::<_, ObserveTracesResponse>(
            rpc,
            target_lister,
            OBSERVE_TRACES_ROUTE,
            &request,
        )?;
        return print_observe_fanout(
            context,
            response,
            options.json,
            |context, response, json| print_observe_trace_records(context, response.records, json),
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
    print_observe_trace_records(context, response.records, options.json)
}

fn print_observe_trace_records(
    context: &mut CliCommandContext<'_>,
    records: Vec<crate::ObserveTraceRecord>,
    json: bool,
) -> Result<(), CliCommandError> {
    if json {
        let json = serde_json::to_string_pretty(&ObserveTracesResponse {
            records: records.clone(),
        })
        .map_err(command_error)?;
        context.stdout_line(format_args!("{json}"))?;
        return Ok(());
    }
    if records.is_empty() {
        context.stdout_line(format_args!("no observe traces found"))?;
        return Ok(());
    }
    for record in records {
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
        context.stdout_line(format_args!(
            "{time} {:<5} {:<32} {}{}{}",
            record.level, record.target, record.name, traceparent, fields
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
        filter: options.filter.clone(),
        quiet_filter_directives: (!options.quiet_filter_directives.is_empty())
            .then_some(options.quiet_filter_directives.clone()),
        clear_quiet_filter_directives: options.clear_quiet_filter_directives,
        reset: options.reset,
    };
    if options.target_all {
        let response = observe_rpc_all::<_, ObserveControlResponse>(
            rpc,
            target_lister,
            OBSERVE_CONTROL_ROUTE,
            &request,
        )?;
        return print_observe_fanout(
            context,
            response,
            options.json,
            |context, response, json| print_observe_control_response(context, response, json),
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
    {
        update_observe_control(request).map_err(command_error)?
    } else {
        observe_control_snapshot()
    };
    print_observe_control_response(context, response, options.json)
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
    }
    Ok(())
}

#[derive(serde::Serialize)]
struct ObserveFanoutResponse<Resp> {
    targets: Vec<ObserveTargetResponse<Resp>>,
    errors: Vec<ObserveTargetError>,
}

#[derive(serde::Serialize)]
struct ObserveTargetResponse<Resp> {
    target_node_id: String,
    response: Resp,
}

#[derive(serde::Serialize)]
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
    let mut responses = Vec::new();
    let mut errors = Vec::new();
    for target_node_id in targets {
        match rpc(Some(target_node_id.as_str()), request.clone()) {
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
        ])
        .expect("parse traces");
        match command {
            ObserveCliCommand::Traces(options) => {
                assert_eq!(options.component.as_deref(), Some("sinomo.network"));
                assert_eq!(
                    options.traceparent.as_deref(),
                    Some("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
                );
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
            }
            other => panic!("expected control command, got {other:?}"),
        }

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

        let result = parse_observe_command(&[
            "traces".to_string(),
            "--all".to_string(),
            "--target".to_string(),
            "01hxy6qa9k0t9vr4g9n8d3q5d4".to_string(),
        ]);
        assert_eq!(result, Err(CliParseError::Usage));

        let result = parse_observe_command(&[
            "control".to_string(),
            "--clear-quiet-filters".to_string(),
            "--quiet-filter".to_string(),
            "hyper=error".to_string(),
        ]);
        assert_eq!(result, Err(CliParseError::Usage));
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
