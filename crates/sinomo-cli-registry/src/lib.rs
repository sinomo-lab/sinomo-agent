//! Extensible parser and runner registry for Sinomo CLI commands.
//!
//! This crate deliberately owns only the command-registration seam. Concrete
//! command crates depend on it to expose parser/runner hooks, while `sinomo-cli`
//! composes the registry with its legacy in-crate commands.

use std::error::Error;
use std::fmt;
use std::io::{self, Write};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CliCommandPolicy {
    pub requires_cluster_state: bool,
    pub supports_degraded_local_mode: bool,
}

impl CliCommandPolicy {
    pub const fn local_capable() -> Self {
        Self {
            requires_cluster_state: false,
            supports_degraded_local_mode: true,
        }
    }

    pub const fn cluster_required() -> Self {
        Self {
            requires_cluster_state: true,
            supports_degraded_local_mode: false,
        }
    }

    pub const fn needs_daemon_probe(self) -> bool {
        self.requires_cluster_state || self.supports_degraded_local_mode
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliCommandMetadata {
    pub name: &'static str,
    pub aliases: &'static [&'static str],
    pub help: &'static str,
    pub policy: CliCommandPolicy,
}

impl CliCommandMetadata {
    pub const fn new(
        name: &'static str,
        aliases: &'static [&'static str],
        help: &'static str,
        policy: CliCommandPolicy,
    ) -> Self {
        Self {
            name,
            aliases,
            help,
            policy,
        }
    }

    pub fn matches(&self, value: &str) -> bool {
        self.name == value || self.aliases.contains(&value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliCommandHelp {
    pub title: String,
    pub lines: Vec<String>,
}

impl CliCommandHelp {
    pub fn new(
        title: impl Into<String>,
        lines: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            title: title.into(),
            lines: lines.into_iter().map(Into::into).collect(),
        }
    }

    pub fn from_metadata(metadata: &CliCommandMetadata) -> Self {
        Self::new(metadata.name, [metadata.help])
    }

    pub fn write(&self, context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError> {
        context.stdout_line(format_args!("{}", self.title))?;
        for line in &self.lines {
            context.stdout_line(format_args!("{line}"))?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliAutocompleteSpec {
    pub word: String,
    pub detail: String,
    pub aliases: Vec<String>,
    pub children: Vec<CliAutocompleteSpec>,
    pub options: Vec<CliAutocompleteOption>,
    pub values: Vec<CliAutocompleteValue>,
}

impl CliAutocompleteSpec {
    pub fn new(word: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            word: word.into(),
            detail: detail.into(),
            aliases: Vec::new(),
            children: Vec::new(),
            options: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn from_metadata(metadata: &CliCommandMetadata) -> Self {
        Self {
            word: metadata.name.to_string(),
            detail: metadata.help.to_string(),
            aliases: metadata
                .aliases
                .iter()
                .map(|alias| alias.to_string())
                .collect(),
            children: Vec::new(),
            options: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn with_aliases(mut self, aliases: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.aliases = aliases.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_children(
        mut self,
        children: impl IntoIterator<Item = CliAutocompleteSpec>,
    ) -> Self {
        self.children = children.into_iter().collect();
        self
    }

    pub fn with_options(
        mut self,
        options: impl IntoIterator<Item = CliAutocompleteOption>,
    ) -> Self {
        self.options = options.into_iter().collect();
        self
    }

    pub fn with_values(mut self, values: impl IntoIterator<Item = CliAutocompleteValue>) -> Self {
        self.values = values.into_iter().collect();
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliAutocompleteOption {
    pub name: String,
    pub detail: String,
    pub value: Option<CliAutocompleteValue>,
}

impl CliAutocompleteOption {
    pub fn flag(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            detail: detail.into(),
            value: None,
        }
    }

    pub fn with_value(
        name: impl Into<String>,
        detail: impl Into<String>,
        value: CliAutocompleteValue,
    ) -> Self {
        Self {
            name: name.into(),
            detail: detail.into(),
            value: Some(value),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliAutocompleteValue {
    pub placeholder: String,
    pub detail: String,
    pub choices: Vec<CliAutocompleteChoice>,
}

impl CliAutocompleteValue {
    pub fn new(
        placeholder: impl Into<String>,
        detail: impl Into<String>,
        choices: impl IntoIterator<Item = CliAutocompleteChoice>,
    ) -> Self {
        Self {
            placeholder: placeholder.into(),
            detail: detail.into(),
            choices: choices.into_iter().collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliAutocompleteChoice {
    pub value: String,
    pub detail: String,
}

impl CliAutocompleteChoice {
    pub fn new(value: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            detail: detail.into(),
        }
    }
}

pub trait CliCommandHandler: Send + Sync {
    fn metadata(&self) -> CliCommandMetadata;
    fn help(&self) -> CliCommandHelp {
        CliCommandHelp::from_metadata(&self.metadata())
    }
    fn autocomplete(&self) -> CliAutocompleteSpec {
        CliAutocompleteSpec::from_metadata(&self.metadata())
    }
    fn parse(&self, args: &[String]) -> Result<Box<dyn CliCommand>, CliParseError>;
}

pub trait CliCommand: Send {
    fn name(&self) -> &'static str;
    fn policy(&self) -> CliCommandPolicy;
    fn help(&self) -> CliCommandHelp {
        CliCommandHelp::new(self.name(), std::iter::empty::<String>())
    }
    fn autocomplete(&self) -> CliAutocompleteSpec {
        CliAutocompleteSpec::new(self.name(), "")
    }
    fn run(self: Box<Self>, context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError>;
}

pub struct RegisteredCliCommand {
    inner: Box<dyn CliCommand>,
}

impl RegisteredCliCommand {
    pub fn new(inner: Box<dyn CliCommand>) -> Self {
        Self { inner }
    }

    pub fn name(&self) -> &'static str {
        self.inner.name()
    }

    pub fn policy(&self) -> CliCommandPolicy {
        self.inner.policy()
    }

    pub fn help(&self) -> CliCommandHelp {
        self.inner.help()
    }

    pub fn autocomplete(&self) -> CliAutocompleteSpec {
        self.inner.autocomplete()
    }

    pub fn run(self, context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError> {
        self.inner.run(context)
    }
}

impl fmt::Debug for RegisteredCliCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredCliCommand")
            .field("name", &self.name())
            .field("policy", &self.policy())
            .finish()
    }
}

#[derive(Default)]
pub struct CliCommandRegistry {
    handlers: Vec<Box<dyn CliCommandHandler>>,
}

impl CliCommandRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(mut self, handler: impl CliCommandHandler + 'static) -> Self {
        let metadata = handler.metadata();
        if self.handlers.iter().any(|existing| {
            let existing = existing.metadata();
            existing.name == metadata.name
                || existing.aliases.contains(&metadata.name)
                || metadata.aliases.contains(&existing.name)
                || existing
                    .aliases
                    .iter()
                    .any(|alias| metadata.aliases.contains(alias))
        }) {
            panic!("duplicate CLI command registration for `{}`", metadata.name);
        }
        self.handlers.push(Box::new(handler));
        self
    }

    pub fn parse(&self, args: &[String]) -> Result<Option<RegisteredCliCommand>, CliParseError> {
        let Some((command, rest)) = args.split_first() else {
            return Ok(None);
        };
        for handler in &self.handlers {
            if handler.metadata().matches(command) {
                return handler.parse(rest).map(RegisteredCliCommand::new).map(Some);
            }
        }
        Ok(None)
    }

    pub fn metadata(&self) -> Vec<CliCommandMetadata> {
        self.handlers
            .iter()
            .map(|handler| handler.metadata())
            .collect()
    }

    pub fn help(&self, command: &str) -> Option<CliCommandHelp> {
        self.handlers
            .iter()
            .find(|handler| handler.metadata().matches(command))
            .map(|handler| handler.help())
    }

    pub fn autocomplete_specs(&self) -> Vec<CliAutocompleteSpec> {
        self.handlers
            .iter()
            .map(|handler| handler.autocomplete())
            .collect()
    }
}

pub struct CliCommandContext<'a> {
    stdout: &'a mut dyn Write,
    stderr: &'a mut dyn Write,
}

impl<'a> CliCommandContext<'a> {
    pub fn new(stdout: &'a mut dyn Write, stderr: &'a mut dyn Write) -> Self {
        Self { stdout, stderr }
    }

    pub fn stdout(&mut self) -> &mut dyn Write {
        self.stdout
    }

    pub fn stderr(&mut self) -> &mut dyn Write {
        self.stderr
    }

    pub fn stdout_line(&mut self, args: fmt::Arguments<'_>) -> Result<(), CliCommandError> {
        write_line(self.stdout, args, "stdout")
    }

    pub fn stderr_line(&mut self, args: fmt::Arguments<'_>) -> Result<(), CliCommandError> {
        write_line(self.stderr, args, "stderr")
    }
}

fn write_line(
    writer: &mut dyn Write,
    args: fmt::Arguments<'_>,
    stream: &'static str,
) -> Result<(), CliCommandError> {
    writer
        .write_fmt(args)
        .and_then(|()| writer.write_all(b"\n"))
        .map_err(|source| output_error(stream, source))
}

fn output_error(stream: &'static str, source: io::Error) -> CliCommandError {
    CliCommandError::new(format!("failed to write {stream}: {source}"))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CliParseError {
    Usage,
    Parse { detail: String },
}

impl fmt::Display for CliParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => write!(formatter, "invalid command usage"),
            Self::Parse { detail } => write!(formatter, "{detail}"),
        }
    }
}

impl Error for CliParseError {}

#[derive(Debug)]
pub struct CliCommandError {
    detail: String,
}

impl CliCommandError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl fmt::Display for CliCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl Error for CliCommandError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct TestHandler {
        name: &'static str,
        aliases: &'static [&'static str],
    }

    impl CliCommandHandler for TestHandler {
        fn metadata(&self) -> CliCommandMetadata {
            CliCommandMetadata::new(
                self.name,
                self.aliases,
                "test command",
                CliCommandPolicy::local_capable(),
            )
        }

        fn parse(&self, _args: &[String]) -> Result<Box<dyn CliCommand>, CliParseError> {
            Ok(Box::new(TestCommand { name: self.name }))
        }
    }

    #[derive(Debug)]
    struct TestCommand {
        name: &'static str,
    }

    impl CliCommand for TestCommand {
        fn name(&self) -> &'static str {
            self.name
        }

        fn policy(&self) -> CliCommandPolicy {
            CliCommandPolicy::local_capable()
        }

        fn run(
            self: Box<Self>,
            context: &mut CliCommandContext<'_>,
        ) -> Result<(), CliCommandError> {
            context.stdout_line(format_args!("{}", self.name))
        }
    }

    #[test]
    fn registry_routes_by_name_and_alias() {
        let registry = CliCommandRegistry::new().register(TestHandler {
            name: "observe",
            aliases: &["obs"],
        });
        let by_name = registry
            .parse(&["observe".to_string()])
            .expect("parse")
            .expect("matched");
        let by_alias = registry
            .parse(&["obs".to_string()])
            .expect("parse")
            .expect("matched");
        assert_eq!(by_name.name(), "observe");
        assert_eq!(by_alias.name(), "observe");
    }

    #[test]
    fn registry_returns_none_for_unknown_command() {
        let registry = CliCommandRegistry::new().register(TestHandler {
            name: "observe",
            aliases: &["obs"],
        });
        assert!(
            registry
                .parse(&["missing".to_string()])
                .expect("parse")
                .is_none()
        );
    }

    #[test]
    fn registry_exposes_command_help_and_autocomplete_specs() {
        let registry = CliCommandRegistry::new().register(TestHandler {
            name: "observe",
            aliases: &["obs"],
        });

        let help = registry.help("obs").expect("help");
        assert_eq!(help.title, "observe");
        assert_eq!(help.lines, vec!["test command"]);

        let specs = registry.autocomplete_specs();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].word, "observe");
        assert_eq!(specs[0].aliases, vec!["obs"]);
    }

    #[test]
    #[should_panic(expected = "duplicate CLI command registration")]
    fn registry_rejects_duplicate_aliases() {
        let _registry = CliCommandRegistry::new()
            .register(TestHandler {
                name: "observe",
                aliases: &["obs"],
            })
            .register(TestHandler {
                name: "other",
                aliases: &["obs"],
            });
    }
}
