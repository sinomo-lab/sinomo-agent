use super::*;
use sinomo_runtime::cli_registry::{
    CliCommand as RegisteredCommand, CliCommandError, CliCommandHandler, CliCommandHelp,
    CliCommandMatches, CliCommandParseOutcome, CliCommandPolicy, CliCommandSpec, CliParseError,
};

type CommandSpec = fn() -> CliCommandSpec;
type ParseCommand<T> = fn(&CliCommandMatches) -> Result<T, CliParseError>;
type RunCommand<T> = fn(T) -> Result<(), CliError>;
type ParsedPolicy<T> = fn(&T) -> CliCommandPolicy;

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
        CliCommandParseOutcome::Matches(matches) => parse(&matches).map_err(CliError::from),
        CliCommandParseOutcome::Help(_) => Err(CliError::Usage),
    }
}

pub(crate) struct DeclarativeCommandHandler<T> {
    name: &'static str,
    policy: CliCommandPolicy,
    specification: CommandSpec,
    parse: ParseCommand<T>,
    run: RunCommand<T>,
    parsed_policy: Option<ParsedPolicy<T>>,
    always_help: bool,
}

impl<T> DeclarativeCommandHandler<T> {
    pub(crate) const fn new(
        name: &'static str,
        policy: CliCommandPolicy,
        specification: CommandSpec,
        parse: ParseCommand<T>,
        run: RunCommand<T>,
    ) -> Self {
        Self {
            name,
            policy,
            specification,
            parse,
            run,
            parsed_policy: None,
            always_help: false,
        }
    }

    pub(crate) const fn with_parsed_policy(mut self, parsed_policy: ParsedPolicy<T>) -> Self {
        self.parsed_policy = Some(parsed_policy);
        self
    }

    pub(crate) const fn with_help_command(mut self) -> Self {
        self.always_help = true;
        self
    }
}

impl<T> CliCommandHandler for DeclarativeCommandHandler<T>
where
    T: Send + 'static,
{
    fn metadata(&self) -> sinomo_runtime::cli_registry::CliCommandMetadata {
        (self.specification)().metadata(self.policy)
    }

    fn specification(&self) -> Option<CliCommandSpec> {
        Some((self.specification)())
    }

    fn parse(&self, args: &[String]) -> Result<Box<dyn RegisteredCommand>, CliParseError> {
        let action = match (self.specification)().parse(args)? {
            CliCommandParseOutcome::Help(help) => DeclarativeCommandAction::Help(help),
            CliCommandParseOutcome::Matches(matches) => {
                let command = (self.parse)(&matches)?;
                let policy = self
                    .parsed_policy
                    .map(|parsed_policy| parsed_policy(&command))
                    .unwrap_or(self.policy);
                DeclarativeCommandAction::Run { command, policy }
            }
        };
        Ok(Box::new(DeclarativeCommandRunner {
            name: self.name,
            specification: self.specification,
            run: self.run,
            always_help: self.always_help,
            action,
        }))
    }
}

enum DeclarativeCommandAction<T> {
    Help(CliCommandHelp),
    Run {
        command: T,
        policy: CliCommandPolicy,
    },
}

struct DeclarativeCommandRunner<T> {
    name: &'static str,
    specification: CommandSpec,
    run: RunCommand<T>,
    always_help: bool,
    action: DeclarativeCommandAction<T>,
}

impl<T> RegisteredCommand for DeclarativeCommandRunner<T>
where
    T: Send + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn policy(&self) -> CliCommandPolicy {
        match &self.action {
            DeclarativeCommandAction::Help(_) => CliCommandPolicy::default(),
            DeclarativeCommandAction::Run { policy, .. } => *policy,
        }
    }

    fn is_help(&self) -> bool {
        self.always_help || matches!(self.action, DeclarativeCommandAction::Help(_))
    }

    fn help(&self) -> CliCommandHelp {
        (self.specification)().help()
    }

    fn autocomplete(&self) -> sinomo_runtime::cli_registry::CliAutocompleteSpec {
        (self.specification)().autocomplete()
    }

    fn run(self: Box<Self>, context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError> {
        match self.action {
            DeclarativeCommandAction::Help(help) => help.write(context),
            DeclarativeCommandAction::Run { command, .. } => {
                (self.run)(command).map_err(|error| CliCommandError::new(error.to_string()))
            }
        }
    }
}
