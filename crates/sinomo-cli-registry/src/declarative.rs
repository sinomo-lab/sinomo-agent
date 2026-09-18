//! Reusable schema/parser/runner adapter owned by the CLI framework.
use crate::{
    CliCommand as RegisteredCommand, CliCommandContext, CliCommandError, CliCommandHandler,
    CliCommandHelp, CliCommandMatches, CliCommandParseOutcome, CliCommandPolicy, CliCommandSpec,
    CliParseError,
};
type CommandSpec = fn() -> CliCommandSpec;
type ParseCommand<T> = fn(&CliCommandMatches) -> Result<T, CliParseError>;
type RunCommand<T, E> = fn(T) -> Result<(), E>;
type ParsedPolicy<T> = fn(&T) -> CliCommandPolicy;
pub struct DeclarativeCommandHandler<T, E = CliCommandError> {
    name: &'static str,
    policy: CliCommandPolicy,
    specification: CommandSpec,
    parse: ParseCommand<T>,
    run: RunCommand<T, E>,
    parsed_policy: Option<ParsedPolicy<T>>,
    always_help: bool,
}

impl<T, E> DeclarativeCommandHandler<T, E> {
    pub const fn new(
        name: &'static str,
        policy: CliCommandPolicy,
        specification: CommandSpec,
        parse: ParseCommand<T>,
        run: RunCommand<T, E>,
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

    pub const fn with_parsed_policy(mut self, parsed_policy: ParsedPolicy<T>) -> Self {
        self.parsed_policy = Some(parsed_policy);
        self
    }

    pub const fn with_help_command(mut self) -> Self {
        self.always_help = true;
        self
    }
}

impl<T, E: Into<CliCommandError> + 'static> CliCommandHandler for DeclarativeCommandHandler<T, E>
where
    T: Send + 'static,
{
    fn metadata(&self) -> crate::CliCommandMetadata {
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

struct DeclarativeCommandRunner<T, E> {
    name: &'static str,
    specification: CommandSpec,
    run: RunCommand<T, E>,
    always_help: bool,
    action: DeclarativeCommandAction<T>,
}

impl<T, E: Into<CliCommandError> + 'static> RegisteredCommand for DeclarativeCommandRunner<T, E>
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

    fn autocomplete(&self) -> crate::CliAutocompleteSpec {
        (self.specification)().autocomplete()
    }

    fn run(self: Box<Self>, context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError> {
        match self.action {
            DeclarativeCommandAction::Help(help) => help.write(context),
            DeclarativeCommandAction::Run { command, .. } => {
                (self.run)(command).map_err(Into::into)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_exit_status_survives_the_declarative_adapter() {
        let handler = DeclarativeCommandHandler::new(
            "example",
            CliCommandPolicy::default(),
            || CliCommandSpec::new("example", "module command"),
            |_| Ok(()),
            |()| Err(CliCommandError::new("owner error").with_exit_code(7)),
        );
        let command = handler.parse(&[]).unwrap();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let error = command
            .run(&mut CliCommandContext::new(&mut stdout, &mut stderr))
            .unwrap_err();
        assert_eq!(error.exit_code(), 7);
        assert_eq!(error.to_string(), "owner error");
    }
}
