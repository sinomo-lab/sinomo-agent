use super::*;
#[cfg(test)]
use sinomo_runtime::cli_registry::{CliCommandMatches, CliParseError};
use sinomo_runtime::cli_registry::{CliCommandPolicy, CliCommandSpec};
pub(crate) use sinomo_runtime::cli_registry::{required_positional, semantic_parse_error};

#[derive(Debug)]
pub(crate) struct TopLevelHelp;

pub(crate) fn help_cli_command_handler() -> DeclarativeCommandHandler<TopLevelHelp> {
    DeclarativeCommandHandler::new(
        "help",
        CliCommandPolicy::default(),
        help_cli_spec,
        |_| Ok(TopLevelHelp),
        |_| {
            print_usage();
            Ok(())
        },
    )
    .with_help_command()
}

fn help_cli_spec() -> CliCommandSpec {
    CliCommandSpec::new("help", "show top-level CLI help").with_aliases(["--help", "-h"])
}

#[cfg(test)]
pub(crate) fn parse_schema_command_for_test<T>(
    specification: CliCommandSpec,
    args: &[String],
    parse: fn(&CliCommandMatches) -> Result<T, CliParseError>,
) -> Result<T, CliError> {
    match specification.parse(args)? {
        sinomo_runtime::cli_registry::CliCommandParseOutcome::Matches(matches) => {
            parse(&matches).map_err(CliError::from)
        }
        sinomo_runtime::cli_registry::CliCommandParseOutcome::Help(_) => Err(CliError::Usage),
    }
}

pub(crate) type DeclarativeCommandHandler<T> =
    sinomo_runtime::cli_registry::DeclarativeCommandHandler<T, CliError>;
