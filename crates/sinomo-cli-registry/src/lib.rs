//! Extensible parser and runner registry for Sinomo CLI commands.
//!
//! This crate deliberately owns only the command-registration seam. Concrete
//! command crates depend on it to expose parser/runner hooks, while `sinomo-cli`
//! composes the registry with its legacy in-crate commands.

use std::collections::{BTreeMap, HashSet};
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
    pub name: String,
    pub aliases: Vec<String>,
    pub help: String,
    pub policy: CliCommandPolicy,
}

impl CliCommandMetadata {
    pub fn new(
        name: impl Into<String>,
        aliases: &[&str],
        help: impl Into<String>,
        policy: CliCommandPolicy,
    ) -> Self {
        Self {
            name: name.into(),
            aliases: aliases.iter().map(|alias| (*alias).to_string()).collect(),
            help: help.into(),
            policy,
        }
    }

    pub fn from_owned(
        name: impl Into<String>,
        aliases: impl IntoIterator<Item = impl Into<String>>,
        help: impl Into<String>,
        policy: CliCommandPolicy,
    ) -> Self {
        Self {
            name: name.into(),
            aliases: aliases.into_iter().map(Into::into).collect(),
            help: help.into(),
            policy,
        }
    }

    pub fn matches(&self, value: &str) -> bool {
        self.name == value || self.aliases.iter().any(|alias| alias == value)
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
        Self::new(&metadata.name, [&metadata.help])
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
            word: metadata.name.clone(),
            detail: metadata.help.clone(),
            aliases: metadata.aliases.to_vec(),
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliValueSpec {
    pub name: String,
    pub help: String,
    pub choices: Vec<CliAutocompleteChoice>,
    pub restricted: bool,
}

impl CliValueSpec {
    pub fn new(name: impl Into<String>, help: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            help: help.into(),
            choices: Vec::new(),
            restricted: false,
        }
    }

    pub fn with_choices(
        mut self,
        choices: impl IntoIterator<Item = CliAutocompleteChoice>,
    ) -> Self {
        self.choices = choices.into_iter().collect();
        self
    }

    pub fn restricted(mut self) -> Self {
        self.restricted = true;
        self
    }

    fn autocomplete(&self) -> CliAutocompleteValue {
        CliAutocompleteValue::new(&self.name, &self.help, self.choices.iter().cloned())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliOptionSpec {
    pub name: String,
    pub aliases: Vec<String>,
    pub help: String,
    pub value: Option<CliValueSpec>,
    pub required: bool,
    pub repeatable: bool,
}

impl CliOptionSpec {
    pub fn flag(name: impl Into<String>, help: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            aliases: Vec::new(),
            help: help.into(),
            value: None,
            required: false,
            repeatable: false,
        }
    }

    pub fn value(name: impl Into<String>, help: impl Into<String>, value: CliValueSpec) -> Self {
        Self {
            name: name.into(),
            aliases: Vec::new(),
            help: help.into(),
            value: Some(value),
            required: false,
            repeatable: false,
        }
    }

    pub fn with_aliases(mut self, aliases: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.aliases = aliases.into_iter().map(Into::into).collect();
        self
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub fn repeatable(mut self) -> Self {
        self.repeatable = true;
        self
    }

    fn matches(&self, value: &str) -> bool {
        self.name == value || self.aliases.iter().any(|alias| alias == value)
    }

    fn usage(&self) -> String {
        let value = self
            .value
            .as_ref()
            .map(|value| format!(" <{}>", value.name))
            .unwrap_or_default();
        let repeated = if self.repeatable { " ..." } else { "" };
        let option = format!("{}{}{}", self.name, value, repeated);
        if self.required {
            option
        } else {
            format!("[{option}]")
        }
    }

    fn autocomplete(&self) -> impl Iterator<Item = CliAutocompleteOption> + '_ {
        std::iter::once(&self.name)
            .chain(self.aliases.iter())
            .map(|name| match &self.value {
                Some(value) => {
                    CliAutocompleteOption::with_value(name, &self.help, value.autocomplete())
                }
                None => CliAutocompleteOption::flag(name, &self.help),
            })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliPositionalSpec {
    pub value: CliValueSpec,
    pub required: bool,
    pub repeatable: bool,
}

impl CliPositionalSpec {
    pub fn required(value: CliValueSpec) -> Self {
        Self {
            value,
            required: true,
            repeatable: false,
        }
    }

    pub fn optional(value: CliValueSpec) -> Self {
        Self {
            value,
            required: false,
            repeatable: false,
        }
    }

    pub fn repeatable(mut self) -> Self {
        self.repeatable = true;
        self
    }

    fn usage(&self) -> String {
        let repeated = if self.repeatable { " ..." } else { "" };
        if self.required {
            format!("<{}>{repeated}", self.value.name)
        } else {
            format!("[<{}>{repeated}]", self.value.name)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliCommandSpec {
    pub name: String,
    pub aliases: Vec<String>,
    pub about: String,
    pub title: Option<String>,
    pub children: Vec<CliCommandSpec>,
    pub options: Vec<CliOptionSpec>,
    pub positionals: Vec<CliPositionalSpec>,
    pub after_help: Vec<String>,
}

impl CliCommandSpec {
    pub fn new(name: impl Into<String>, about: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            aliases: Vec::new(),
            about: about.into(),
            title: None,
            children: Vec::new(),
            options: Vec::new(),
            positionals: Vec::new(),
            after_help: Vec::new(),
        }
    }

    pub fn with_aliases(mut self, aliases: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.aliases = aliases.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn with_children(mut self, children: impl IntoIterator<Item = CliCommandSpec>) -> Self {
        self.children = children.into_iter().collect();
        self
    }

    pub fn with_options(mut self, options: impl IntoIterator<Item = CliOptionSpec>) -> Self {
        self.options = options.into_iter().collect();
        self
    }

    pub fn with_positionals(
        mut self,
        positionals: impl IntoIterator<Item = CliPositionalSpec>,
    ) -> Self {
        self.positionals = positionals.into_iter().collect();
        self
    }

    pub fn with_after_help(mut self, lines: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.after_help = lines.into_iter().map(Into::into).collect();
        self
    }

    pub fn metadata(&self, policy: CliCommandPolicy) -> CliCommandMetadata {
        CliCommandMetadata::from_owned(
            &self.name,
            self.aliases.iter().cloned(),
            &self.about,
            policy,
        )
    }

    pub fn autocomplete(&self) -> CliAutocompleteSpec {
        CliAutocompleteSpec::new(&self.name, &self.about)
            .with_aliases(self.aliases.iter().cloned())
            .with_children(self.children.iter().map(Self::autocomplete))
            .with_options(self.options.iter().flat_map(CliOptionSpec::autocomplete))
            .with_values(
                self.positionals
                    .iter()
                    .map(|positional| positional.value.autocomplete()),
            )
    }

    pub fn help(&self) -> CliCommandHelp {
        self.help_at(self, std::slice::from_ref(&self.name))
    }

    pub fn validate(&self) -> Result<(), String> {
        self.validate_at(&mut vec![self.name.clone()])
    }

    pub fn parse(&self, args: &[String]) -> Result<CliCommandParseOutcome, CliParseError> {
        self.validate().map_err(|detail| CliParseError::Parse {
            detail: format!("invalid CLI command specification: {detail}"),
        })?;

        let mut node = self;
        let mut path = vec![self.name.clone()];
        let mut index = 0;
        while let Some(token) = args.get(index) {
            if is_help_token(token) {
                return Ok(CliCommandParseOutcome::Help(node.help_at(node, &path)));
            }
            let Some(child) = node.children.iter().find(|child| child.matches(token)) else {
                break;
            };
            node = child;
            path.push(child.name.clone());
            index += 1;
        }

        let mut options = BTreeMap::<String, Vec<String>>::new();
        let mut positionals = Vec::new();
        let mut options_enabled = true;
        while let Some(token) = args.get(index) {
            if options_enabled && is_help_token(token) {
                return Ok(CliCommandParseOutcome::Help(node.help_at(node, &path)));
            }
            if options_enabled && token == "--" {
                options_enabled = false;
                index += 1;
                continue;
            }
            if options_enabled && token.starts_with('-') {
                let (option_name, inline_value) = token
                    .split_once('=')
                    .map_or((token.as_str(), None), |(name, value)| (name, Some(value)));
                let Some(spec) = node
                    .options
                    .iter()
                    .find(|option| option.matches(option_name))
                else {
                    return Err(node.invalid_usage(
                        &path,
                        CliParseErrorKind::UnknownOption,
                        format!("unknown option `{option_name}`"),
                    ));
                };
                let values = options.entry(spec.name.clone()).or_default();
                if !spec.repeatable && !values.is_empty() {
                    return Err(node.invalid_usage(
                        &path,
                        CliParseErrorKind::DuplicateOption,
                        format!("option `{}` may only be provided once", spec.name),
                    ));
                }
                match (&spec.value, inline_value) {
                    (None, Some(_)) => {
                        return Err(node.invalid_usage(
                            &path,
                            CliParseErrorKind::UnexpectedValue,
                            format!("flag `{}` does not accept a value", spec.name),
                        ));
                    }
                    (None, None) => values.push(String::new()),
                    (Some(value_spec), Some(value)) => {
                        validate_choice(node, &path, spec, value_spec, value)?;
                        values.push(value.to_string());
                    }
                    (Some(value_spec), None) => {
                        index += 1;
                        let Some(value) = args.get(index) else {
                            return Err(node.invalid_usage(
                                &path,
                                CliParseErrorKind::MissingValue,
                                format!("option `{}` requires <{}>", spec.name, value_spec.name),
                            ));
                        };
                        if is_help_token(value) {
                            return Ok(CliCommandParseOutcome::Help(node.help_at(node, &path)));
                        }
                        validate_choice(node, &path, spec, value_spec, value)?;
                        values.push(value.clone());
                    }
                }
                index += 1;
                continue;
            }
            if options_enabled && !node.children.is_empty() && node.positionals.is_empty() {
                return Err(node.invalid_usage(
                    &path,
                    CliParseErrorKind::UnknownCommand,
                    format!("unknown subcommand `{token}`"),
                ));
            }
            positionals.push(token.clone());
            index += 1;
        }

        for option in node.options.iter().filter(|option| option.required) {
            if !options.contains_key(&option.name) {
                return Err(node.invalid_usage(
                    &path,
                    CliParseErrorKind::MissingRequiredOption,
                    format!("required option `{}` was not provided", option.name),
                ));
            }
        }
        validate_positionals(node, &path, &positionals)?;
        Ok(CliCommandParseOutcome::Matches(CliCommandMatches {
            path,
            options,
            positionals,
        }))
    }

    fn matches(&self, value: &str) -> bool {
        self.name == value || self.aliases.iter().any(|alias| alias == value)
    }

    fn validate_at(&self, path: &mut Vec<String>) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err(format!("command `{}` has an empty name", path.join(" ")));
        }
        validate_names(
            &self.name,
            &self.aliases,
            &format!("command `{}`", path.join(" ")),
        )?;
        let mut child_names = HashSet::new();
        for child in &self.children {
            for name in std::iter::once(&child.name).chain(child.aliases.iter()) {
                if !child_names.insert(name.as_str()) {
                    return Err(format!(
                        "command `{}` has duplicate subcommand or alias `{name}`",
                        path.join(" ")
                    ));
                }
            }
            path.push(child.name.clone());
            child.validate_at(path)?;
            path.pop();
        }
        let mut option_names = HashSet::new();
        for option in &self.options {
            if !option.name.starts_with('-') {
                return Err(format!(
                    "command `{}` option `{}` must start with `-`",
                    path.join(" "),
                    option.name
                ));
            }
            validate_names(
                &option.name,
                &option.aliases,
                &format!("command `{}` option", path.join(" ")),
            )?;
            if let Some(alias) = option.aliases.iter().find(|alias| !alias.starts_with('-')) {
                return Err(format!(
                    "command `{}` option alias `{alias}` must start with `-`",
                    path.join(" ")
                ));
            }
            for name in std::iter::once(&option.name).chain(option.aliases.iter()) {
                if !option_names.insert(name.as_str()) {
                    return Err(format!(
                        "command `{}` has duplicate option or alias `{name}`",
                        path.join(" ")
                    ));
                }
            }
        }
        if self
            .positionals
            .iter()
            .enumerate()
            .any(|(index, positional)| positional.repeatable && index + 1 != self.positionals.len())
        {
            return Err(format!(
                "command `{}` has a repeatable positional before its final positional",
                path.join(" ")
            ));
        }
        let mut optional_seen = false;
        for positional in &self.positionals {
            if positional.required && optional_seen {
                return Err(format!(
                    "command `{}` has a required positional after an optional positional",
                    path.join(" ")
                ));
            }
            optional_seen |= !positional.required;
        }
        Ok(())
    }

    fn help_at(&self, node: &CliCommandSpec, path: &[String]) -> CliCommandHelp {
        let title = node.title.clone().unwrap_or_else(|| {
            let mut title = path.join(" ");
            if let Some(first) = title.get_mut(0..1) {
                first.make_ascii_uppercase();
            }
            title.push(':');
            title
        });
        let mut lines = Vec::new();
        append_help_lines(node, path, &mut lines);
        if !node.aliases.is_empty() {
            lines.push(format!("  Aliases: {}", node.aliases.join(", ")));
        }
        if !node.after_help.is_empty() {
            lines.push(String::new());
            lines.extend(node.after_help.iter().cloned());
        }
        CliCommandHelp::new(title, lines)
    }

    fn usage_line(&self, path: &[String]) -> String {
        let mut parts = vec!["sinomo".to_string(), path.join(" ")];
        parts.extend(self.options.iter().map(CliOptionSpec::usage));
        parts.extend(self.positionals.iter().map(CliPositionalSpec::usage));
        parts.join(" ")
    }

    fn invalid_usage(
        &self,
        path: &[String],
        kind: CliParseErrorKind,
        detail: String,
    ) -> CliParseError {
        CliParseError::InvalidUsage {
            kind,
            command: path.join(" "),
            detail,
            usage: self.usage_line(path),
        }
    }
}

fn validate_names(name: &str, aliases: &[String], context: &str) -> Result<(), String> {
    if name.trim().is_empty() {
        return Err(format!("{context} has an empty name"));
    }
    let mut names = HashSet::from([name]);
    for alias in aliases {
        if alias.trim().is_empty() {
            return Err(format!("{context} has an empty alias"));
        }
        if !names.insert(alias) {
            return Err(format!("{context} repeats name or alias `{alias}`"));
        }
    }
    Ok(())
}

fn is_help_token(value: &str) -> bool {
    matches!(value, "help" | "--help" | "-h")
}

fn validate_choice(
    node: &CliCommandSpec,
    path: &[String],
    option: &CliOptionSpec,
    value_spec: &CliValueSpec,
    value: &str,
) -> Result<(), CliParseError> {
    if !value_spec.restricted
        || value_spec.choices.is_empty()
        || value_spec
            .choices
            .iter()
            .any(|choice| choice.value == value)
    {
        return Ok(());
    }
    Err(node.invalid_usage(
        path,
        CliParseErrorKind::InvalidValue,
        format!(
            "invalid value `{value}` for `{}`; expected one of: {}",
            option.name,
            value_spec
                .choices
                .iter()
                .map(|choice| choice.value.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ))
}

fn validate_positionals(
    node: &CliCommandSpec,
    path: &[String],
    values: &[String],
) -> Result<(), CliParseError> {
    let required = node
        .positionals
        .iter()
        .filter(|positional| positional.required)
        .count();
    if values.len() < required {
        let missing = &node.positionals[values.len()];
        return Err(node.invalid_usage(
            path,
            CliParseErrorKind::MissingPositional,
            format!(
                "required argument <{}> was not provided",
                missing.value.name
            ),
        ));
    }
    let accepts_extra = node
        .positionals
        .last()
        .is_some_and(|positional| positional.repeatable);
    if !accepts_extra && values.len() > node.positionals.len() {
        return Err(node.invalid_usage(
            path,
            CliParseErrorKind::UnexpectedArgument,
            format!("unexpected argument `{}`", values[node.positionals.len()]),
        ));
    }
    for (index, value) in values.iter().enumerate() {
        let spec = node
            .positionals
            .get(index)
            .or_else(|| node.positionals.last().filter(|spec| spec.repeatable))
            .expect("positional count was validated before value choices");
        if spec.value.restricted
            && !spec.value.choices.is_empty()
            && !spec
                .value
                .choices
                .iter()
                .any(|choice| choice.value == *value)
        {
            return Err(node.invalid_usage(
                path,
                CliParseErrorKind::InvalidValue,
                format!(
                    "invalid value `{value}` for <{}>; expected one of: {}",
                    spec.value.name,
                    spec.value
                        .choices
                        .iter()
                        .map(|choice| choice.value.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
        }
    }
    Ok(())
}

fn append_help_lines(node: &CliCommandSpec, path: &[String], lines: &mut Vec<String>) {
    lines.push(format!("  {}", node.usage_line(path)));
    if !node.about.is_empty() {
        lines.push(format!("      {}", node.about));
    }
    for child in &node.children {
        let mut child_path = path.to_vec();
        child_path.push(child.name.clone());
        append_help_lines(child, &child_path, lines);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CliCommandParseOutcome {
    Help(CliCommandHelp),
    Matches(CliCommandMatches),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliCommandMatches {
    path: Vec<String>,
    options: BTreeMap<String, Vec<String>>,
    positionals: Vec<String>,
}

impl CliCommandMatches {
    pub fn path(&self) -> &[String] {
        &self.path
    }

    pub fn command_name(&self) -> &str {
        self.path.last().map(String::as_str).unwrap_or_default()
    }

    pub fn contains(&self, option: &str) -> bool {
        self.options.contains_key(option)
    }

    pub fn value(&self, option: &str) -> Option<&str> {
        self.options
            .get(option)
            .and_then(|values| values.last())
            .map(String::as_str)
    }

    pub fn values(&self, option: &str) -> impl Iterator<Item = &str> {
        self.options
            .get(option)
            .into_iter()
            .flatten()
            .map(String::as_str)
    }

    pub fn positionals(&self) -> &[String] {
        &self.positionals
    }
}

pub trait CliCommandHandler: Send + Sync {
    fn metadata(&self) -> CliCommandMetadata;
    fn specification(&self) -> Option<CliCommandSpec> {
        None
    }
    fn help(&self) -> CliCommandHelp {
        self.specification()
            .map(|specification| specification.help())
            .unwrap_or_else(|| CliCommandHelp::from_metadata(&self.metadata()))
    }
    fn autocomplete(&self) -> CliAutocompleteSpec {
        self.specification()
            .map(|specification| specification.autocomplete())
            .unwrap_or_else(|| CliAutocompleteSpec::from_metadata(&self.metadata()))
    }
    fn parse(&self, args: &[String]) -> Result<Box<dyn CliCommand>, CliParseError>;
}

pub trait CliCommand: Send {
    fn name(&self) -> &'static str;
    fn policy(&self) -> CliCommandPolicy;
    fn is_help(&self) -> bool {
        false
    }
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

    pub fn is_help(&self) -> bool {
        self.inner.is_help()
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
        validate_names(
            &metadata.name,
            &metadata.aliases,
            "CLI command registration",
        )
        .unwrap_or_else(|detail| panic!("invalid CLI command registration: {detail}"));
        if let Some(specification) = handler.specification() {
            specification.validate().unwrap_or_else(|detail| {
                panic!(
                    "invalid CLI command specification for `{}`: {detail}",
                    metadata.name
                )
            });
            let specification_metadata = specification.metadata(metadata.policy);
            assert_eq!(
                metadata, specification_metadata,
                "CLI command metadata must come from its declarative specification"
            );
        }
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

    pub fn validate_reserved_names(&self, reserved: &[&str]) -> Result<(), String> {
        for handler in &self.handlers {
            let metadata = handler.metadata();
            for name in std::iter::once(&metadata.name).chain(metadata.aliases.iter()) {
                if reserved.contains(&name.as_str()) {
                    return Err(format!(
                        "registered CLI command `{}` conflicts with built-in name `{name}`",
                        metadata.name
                    ));
                }
            }
        }
        Ok(())
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
    force_local: bool,
}

impl<'a> CliCommandContext<'a> {
    pub fn new(stdout: &'a mut dyn Write, stderr: &'a mut dyn Write) -> Self {
        Self {
            stdout,
            stderr,
            force_local: false,
        }
    }

    pub fn with_force_local(mut self, force_local: bool) -> Self {
        self.force_local = force_local;
        self
    }

    pub fn force_local(&self) -> bool {
        self.force_local
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
    Parse {
        detail: String,
    },
    InvalidUsage {
        kind: CliParseErrorKind,
        command: String,
        detail: String,
        usage: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CliParseErrorKind {
    UnknownCommand,
    UnknownOption,
    MissingValue,
    DuplicateOption,
    UnexpectedValue,
    InvalidValue,
    MissingRequiredOption,
    MissingPositional,
    UnexpectedArgument,
}

impl fmt::Display for CliParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => write!(formatter, "invalid command usage"),
            Self::Parse { detail } => write!(formatter, "{detail}"),
            Self::InvalidUsage {
                command,
                detail,
                usage,
                ..
            } => write!(formatter, "{command}: {detail}\nUsage: {usage}"),
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

    fn declarative_test_spec() -> CliCommandSpec {
        CliCommandSpec::new("models", "manage model settings")
            .with_aliases(["model"])
            .with_children([
                CliCommandSpec::new("list", "list models").with_options([CliOptionSpec::value(
                    "--provider",
                    "provider id",
                    CliValueSpec::new("provider", "provider id"),
                )]),
                CliCommandSpec::new("set", "set model options").with_options([
                    CliOptionSpec::value(
                        "--provider",
                        "provider id",
                        CliValueSpec::new("provider", "provider id"),
                    )
                    .required(),
                    CliOptionSpec::value(
                        "--vendor-option",
                        "vendor key=value",
                        CliValueSpec::new("key=value", "vendor option"),
                    )
                    .repeatable(),
                    CliOptionSpec::value(
                        "--device",
                        "runtime device",
                        CliValueSpec::new("cpu|cuda", "runtime device")
                            .with_choices([
                                CliAutocompleteChoice::new("cpu", "CPU"),
                                CliAutocompleteChoice::new("cuda", "CUDA"),
                            ])
                            .restricted(),
                    ),
                ]),
            ])
    }

    #[test]
    fn declarative_spec_drives_parse_help_and_autocomplete() {
        let spec = declarative_test_spec();
        spec.validate().expect("valid specification");

        let CliCommandParseOutcome::Matches(matches) = spec
            .parse(&[
                "set".to_string(),
                "--provider=codex".to_string(),
                "--vendor-option".to_string(),
                "reasoning=high".to_string(),
                "--vendor-option=verbosity=low".to_string(),
                "--device".to_string(),
                "cuda".to_string(),
            ])
            .expect("parse")
        else {
            panic!("expected parsed matches");
        };
        assert_eq!(matches.path(), ["models", "set"]);
        assert_eq!(matches.value("--provider"), Some("codex"));
        assert_eq!(
            matches.values("--vendor-option").collect::<Vec<_>>(),
            ["reasoning=high", "verbosity=low"]
        );

        let CliCommandParseOutcome::Help(help) = spec
            .parse(&["set".to_string(), "--help".to_string()])
            .expect("help")
        else {
            panic!("expected help");
        };
        assert!(
            help.lines
                .iter()
                .any(|line| line.contains("sinomo models set"))
        );
        assert!(
            !help
                .lines
                .iter()
                .any(|line| line.contains("sinomo models list"))
        );

        let autocomplete = spec.autocomplete();
        assert_eq!(autocomplete.word, "models");
        assert_eq!(autocomplete.aliases, ["model"]);
        assert!(
            autocomplete
                .children
                .iter()
                .any(|child| child.word == "set")
        );
    }

    #[test]
    fn declarative_parser_returns_structured_usage_errors() {
        let spec = declarative_test_spec();
        let cases = [
            (
                vec!["set", "--device", "tpu", "--provider", "codex"],
                CliParseErrorKind::InvalidValue,
            ),
            (vec!["set", "--provider"], CliParseErrorKind::MissingValue),
            (
                vec!["set", "--device", "cpu"],
                CliParseErrorKind::MissingRequiredOption,
            ),
            (
                vec!["set", "--provider", "a", "--provider", "b"],
                CliParseErrorKind::DuplicateOption,
            ),
            (vec!["set", "--missing"], CliParseErrorKind::UnknownOption),
        ];
        for (args, expected_kind) in cases {
            let args = args.into_iter().map(str::to_string).collect::<Vec<_>>();
            let error = spec.parse(&args).expect_err("invalid arguments");
            assert!(
                matches!(error, CliParseError::InvalidUsage { kind, .. } if kind == expected_kind),
                "unexpected error: {error}"
            );
            assert!(error.to_string().contains("Usage: sinomo models set"));
        }
    }

    #[test]
    fn declarative_parser_validates_positionals_and_honors_double_dash() {
        let spec = CliCommandSpec::new("run", "run a named mode").with_positionals([
            CliPositionalSpec::required(
                CliValueSpec::new("mode", "execution mode")
                    .with_choices([
                        CliAutocompleteChoice::new("fast", "fast mode"),
                        CliAutocompleteChoice::new("help", "a mode literally named help"),
                    ])
                    .restricted(),
            )
            .repeatable(),
        ]);

        let CliCommandParseOutcome::Matches(matches) = spec
            .parse(&["--".to_string(), "help".to_string(), "fast".to_string()])
            .expect("literal help positional")
        else {
            panic!("expected parsed matches");
        };
        assert_eq!(matches.positionals(), ["help", "fast"]);

        let error = spec
            .parse(&["slow".to_string()])
            .expect_err("restricted positional value");
        assert!(matches!(
            error,
            CliParseError::InvalidUsage {
                kind: CliParseErrorKind::InvalidValue,
                ..
            }
        ));
    }

    #[test]
    fn declarative_spec_rejects_duplicate_names_within_one_command() {
        let error = CliCommandSpec::new("models", "models")
            .with_aliases(["models"])
            .validate()
            .expect_err("duplicate alias");
        assert!(error.contains("repeats name or alias"));
    }

    #[test]
    fn registry_rejects_names_reserved_by_builtin_commands() {
        let registry = CliCommandRegistry::new().register(TestHandler {
            name: "models",
            aliases: &["model"],
        });
        let error = registry
            .validate_reserved_names(&["cluster", "model"])
            .expect_err("reserved alias");
        assert!(error.contains("conflicts with built-in name `model`"));
    }
}
