//! Versioned CLI contributions and invocation values for module-owned parsers.

use crate::{
    ExtensionCompositionPolicy, ExtensionContribution, ExtensionPointDefinition,
    ExtensionVersionRequirement, encode_sobj,
};
use serde::{Deserialize, Serialize};

pub const CLI_COMMANDS_POINT: &str = "sinomo.cli.commands";
pub const CLI_COMMANDS_VERSION: &str = "1.0.0";
pub const CLI_COMMAND_SCHEMA: &str = "sinomo.cli.command/v1";
pub const CLI_INTERFACE_ID: &str = "sinomo.cli.command";
pub const CLI_INTERFACE_VERSION: &str = "1.0.0";
pub const CLI_INVOKE_OPERATION: &str = "cli.invoke";
pub const CLI_REQUEST_SCHEMA: &str = "sinomo.cli.invocation/v1";
pub const CLI_RESPONSE_SCHEMA: &str = "sinomo.cli.result/v1";
pub const CLI_MAX_ARGUMENT_BYTES: usize = 256 * 1024;
pub const CLI_MAX_INPUT_BYTES: usize = 1024 * 1024;
pub const CLI_MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
pub const CLI_MAX_DEADLINE_MS: u64 = 60 * 60 * 1000;
/// These names are needed to recover a host with no usable module catalog.
pub const CLI_RESERVED_NAMES: &[&str] = &[
    "help",
    "version",
    "daemon",
    "cluster",
    "install",
    "uninstall",
    "module",
    "node",
];

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CliCommandDescriptor {
    pub command_id: String,
    pub name: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    pub summary: String,
    /// Help and completion are exported by the parser owner, never maintained by a proxy.
    pub help: Vec<String>,
    /// SOBJ-encoded `sinomo_cli_registry::CliAutocompleteSpec`.
    #[serde(default)]
    pub autocomplete_sobj: Vec<u8>,
    pub service_id: String,
    pub max_deadline_ms: u64,
    #[serde(default)]
    pub accepts_stdin: bool,
    /// Whether the owner treats an empty argument vector as root help.
    #[serde(default)]
    pub help_on_empty: bool,
    #[serde(default)]
    pub requires_cluster_state: bool,
}

impl CliCommandDescriptor {
    pub fn validate(&self) -> Result<(), String> {
        if self.command_id.is_empty()
            || self.command_id.len() > 256
            || self.service_id.is_empty()
            || self.service_id.len() > 256
        {
            return Err("CLI contribution requires bounded command and service identities".into());
        }
        let mut names = std::collections::BTreeSet::new();
        for name in std::iter::once(&self.name).chain(&self.aliases) {
            if name.is_empty()
                || name.len() > 64
                || name.starts_with('-')
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            {
                return Err(format!("invalid CLI command name `{name}`"));
            }
            if CLI_RESERVED_NAMES.contains(&name.as_str()) || !names.insert(name) {
                return Err(format!("reserved or repeated CLI command name `{name}`"));
            }
        }
        if self.max_deadline_ms == 0
            || self.max_deadline_ms > CLI_MAX_DEADLINE_MS
            || self.aliases.len() > 16
            || self.autocomplete_sobj.len() > 128 * 1024
            || self.summary.len() > 4096
            || self.help.len() > 4096
            || self.help.iter().map(String::len).sum::<usize>() > 128 * 1024
        {
            return Err("CLI contribution exceeds its metadata or deadline limits".into());
        }
        Ok(())
    }

    pub fn contribution(&self) -> Result<ExtensionContribution, String> {
        self.validate()?;
        Ok(ExtensionContribution::declarative(
            &self.command_id,
            CLI_COMMANDS_POINT,
            ExtensionVersionRequirement::exact(CLI_COMMANDS_VERSION),
            CLI_COMMAND_SCHEMA,
            encode_sobj(self).map_err(|error| error.to_string())?,
        ))
    }
}

pub fn cli_commands_extension_point() -> ExtensionPointDefinition {
    let mut point = ExtensionPointDefinition::new(
        CLI_COMMANDS_POINT,
        CLI_COMMANDS_VERSION,
        CLI_REQUEST_SCHEMA,
        CLI_COMMAND_SCHEMA,
        CLI_RESPONSE_SCHEMA,
        ExtensionCompositionPolicy::Collect,
    );
    point.max_contributions = 256;
    point.max_response_bytes = CLI_MAX_OUTPUT_BYTES as u64;
    point.max_deadline_ms = CLI_MAX_DEADLINE_MS;
    point
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CliAction {
    #[default]
    Invoke,
    Help,
    Complete,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CliModuleRequest {
    pub command_id: String,
    pub invocation_id: String,
    pub action: CliAction,
    /// Exact argument tokens after the command namespace. No shell joining or re-tokenizing.
    pub args: Vec<String>,
    #[serde(default)]
    pub stdin: Vec<u8>,
    #[serde(default)]
    pub current_dir: Option<String>,
}

impl CliModuleRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.command_id.is_empty()
            || self.command_id.len() > 256
            || self.invocation_id.is_empty()
            || self.invocation_id.len() > 128
            || self.args.len() > 4096
            || self.args.iter().map(String::len).sum::<usize>() > CLI_MAX_ARGUMENT_BYTES
            || self.stdin.len() > CLI_MAX_INPUT_BYTES
            || self
                .current_dir
                .as_ref()
                .is_some_and(|dir| dir.len() > 32 * 1024)
            || self.args.iter().any(|arg| arg.contains('\0'))
        {
            return Err("invalid or oversized CLI invocation".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct CliModuleResponse {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
    #[serde(default)]
    pub completions: Vec<String>,
}

impl CliModuleResponse {
    pub fn failure(exit_code: i32, detail: impl Into<String>) -> Self {
        Self {
            stderr: format!("{}\n", detail.into()).into_bytes(),
            exit_code,
            ..Self::default()
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if !(0..=255).contains(&self.exit_code)
            || self
                .stdout
                .len()
                .saturating_add(self.stderr.len())
                .saturating_add(self.completions.iter().map(String::len).sum::<usize>())
                > CLI_MAX_OUTPUT_BYTES
            || self.completions.len() > 4096
        {
            Err("CLI response exceeds the output limit".into())
        } else {
            Ok(())
        }
    }
}
