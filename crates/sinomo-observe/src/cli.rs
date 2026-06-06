use crate::{
    OperationalEventQuery, OperationalEventRecord, OperationalLogQuery, OperationalLogRecord,
    observe_module_enabled, read_operational_events, read_operational_logs,
};
use sinomo_cli_registry::{
    CliAutocompleteOption, CliAutocompleteSpec, CliAutocompleteValue, CliCommand,
    CliCommandContext, CliCommandError, CliCommandHandler, CliCommandHelp, CliCommandMetadata,
    CliCommandPolicy, CliParseError,
};
use sinomo_local::SinomoDataDir;
use std::fmt;
use time::format_description::well_known::Rfc3339;

const OBSERVE_DEFAULT_LIMIT: usize = 40;
const OBSERVE_POLICY: CliCommandPolicy = CliCommandPolicy::local_capable();

#[derive(Debug, Clone, Eq, PartialEq)]
enum ObserveCliCommand {
    Help,
    Events(ObserveOptions),
    Logs(ObserveOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ObserveOptions {
    kind_prefix: Option<String>,
    component: Option<String>,
    operation_id: Option<String>,
    limit: usize,
    json: bool,
}

impl Default for ObserveOptions {
    fn default() -> Self {
        Self {
            kind_prefix: None,
            component: None,
            operation_id: None,
            limit: OBSERVE_DEFAULT_LIMIT,
            json: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct ObserveCliCommandHandler;

pub fn observe_cli_command_handler() -> ObserveCliCommandHandler {
    ObserveCliCommandHandler
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
        }))
    }
}

#[derive(Debug)]
struct ObserveCliCommandRunner {
    command: ObserveCliCommand,
}

impl CliCommand for ObserveCliCommandRunner {
    fn name(&self) -> &'static str {
        "observe"
    }

    fn policy(&self) -> CliCommandPolicy {
        OBSERVE_POLICY
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
            ObserveCliCommand::Events(options) => observe_events(context, options),
            ObserveCliCommand::Logs(options) => observe_logs(context, options),
        }
    }
}

fn observe_cli_metadata() -> CliCommandMetadata {
    CliCommandMetadata::new(
        "observe",
        &["obs"],
        "Read local operational events and logs.",
        OBSERVE_POLICY,
    )
}

fn observe_cli_help() -> CliCommandHelp {
    CliCommandHelp::new(
        "Observe:",
        [
            "  sinomo observe",
            "      Show observation commands.",
            "  sinomo observe events [--kind <prefix>] [--component <name>] [--operation <id>] [--limit <n>] [--json]",
            "      Read local daemon/shadow operational events from the data directory.",
            "  sinomo observe logs [--component <name>] [--operation <id>] [--limit <n>] [--json]",
            "      Read local daemon/shadow operational logs from the data directory.",
            "  Observation commands do not require a running daemon, honor SINOMO_DATA_DIR, and require module.observe to be enabled.",
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
    options: ObserveOptions,
) -> Result<(), CliCommandError> {
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
    if options.json {
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
    options: ObserveOptions,
) -> Result<(), CliCommandError> {
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
    if options.json {
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
