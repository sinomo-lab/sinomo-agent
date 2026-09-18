use super::*;
use sinomo_runtime::cli_registry::{
    CliCommandMatches, CliCommandPolicy, CliCommandSpec, CliParseError,
};

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

pub(crate) fn semantic_parse_error(error: CliError) -> CliParseError {
    CliParseError::Parse {
        detail: error.to_string(),
    }
}

pub(crate) fn required_option<'a>(
    matches: &'a CliCommandMatches,
    option: &str,
) -> Result<&'a str, CliParseError> {
    matches.value(option).ok_or_else(|| CliParseError::Parse {
        detail: format!("declarative parser omitted required option `{option}`"),
    })
}

pub(crate) fn required_positional(
    matches: &CliCommandMatches,
    index: usize,
) -> Result<&str, CliParseError> {
    matches
        .positionals()
        .get(index)
        .map(String::as_str)
        .ok_or_else(|| CliParseError::Parse {
            detail: format!("declarative parser omitted required positional {index}"),
        })
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
