//! Contract-only CLI proxy for the independently loaded evolution module.

use std::time::Duration;

use sinomo_app_contract::evolution as contract;
use sinomo_runtime::cli_registry::{
    CliCommand, CliCommandContext, CliCommandError, CliCommandHandler, CliCommandHelp,
    CliCommandMetadata, CliCommandPolicy, CliParseError,
};
use sinomo_runtime::network::ObjectRpcRequest;

use crate::{daemon_default_main_daemon_status, daemon_send_daemon_rpc};

const EVOLUTION_CLI_TIMEOUT: Duration = Duration::from_secs(60 * 60);

#[derive(Clone, Copy, Debug, Default)]
pub struct EvolutionCliCommandHandler;

pub fn evolution_cli_command_handler() -> EvolutionCliCommandHandler {
    EvolutionCliCommandHandler
}

impl CliCommandHandler for EvolutionCliCommandHandler {
    fn metadata(&self) -> CliCommandMetadata {
        CliCommandMetadata::new(
            "evolve",
            &[],
            "Run workflows provided by the active evolution module.",
            CliCommandPolicy::cluster_required(),
        )
    }

    fn help(&self) -> CliCommandHelp {
        evolution_cli_help()
    }

    fn parse(&self, args: &[String]) -> Result<Box<dyn CliCommand>, CliParseError> {
        Ok(Box::new(EvolutionCliCommand {
            args: args.to_vec(),
        }))
    }
}

#[derive(Clone, Debug)]
struct EvolutionCliCommand {
    args: Vec<String>,
}

impl CliCommand for EvolutionCliCommand {
    fn name(&self) -> &'static str {
        "evolve"
    }

    fn policy(&self) -> CliCommandPolicy {
        CliCommandPolicy::cluster_required()
    }

    fn is_help(&self) -> bool {
        self.args.is_empty()
            || matches!(self.args.as_slice(), [arg] if matches!(arg.as_str(), "help" | "--help" | "-h"))
    }

    fn help(&self) -> CliCommandHelp {
        evolution_cli_help()
    }

    fn run(self: Box<Self>, context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError> {
        if self.is_help() {
            return self.help().write(context);
        }
        let status = daemon_default_main_daemon_status(Duration::from_secs(5))
            .map_err(|error| CliCommandError::new(error.to_string()))?
            .ok_or_else(|| {
                CliCommandError::new(
                    "evolution commands require the local daemon and active evolution module",
                )
            })?;
        let request = ObjectRpcRequest::encode_body(
            contract::EVOLUTION_CLI_ROUTE,
            contract::EVOLUTION_RPC_CATEGORY,
            &contract::EvolutionCliRequest { args: self.args },
        )
        .map_err(|error| CliCommandError::new(error.to_string()))?;
        let response: contract::EvolutionCliResponse =
            daemon_send_daemon_rpc(&status.address, &request, EVOLUTION_CLI_TIMEOUT)
                .map_err(|error| CliCommandError::new(error.to_string()))?
                .decode_body()
                .map_err(|error| CliCommandError::new(error.to_string()))?;
        context
            .stdout()
            .write_all(response.stdout.as_bytes())
            .map_err(|error| CliCommandError::new(format!("write evolution output: {error}")))
    }
}

fn evolution_cli_help() -> CliCommandHelp {
    CliCommandHelp::new(
        "Evolve (provided by module.evolution):",
        [
            "  sinomo evolve propose <objective> [options]",
            "  sinomo evolve schedule|iterate|evaluate <campaign> [options]",
            "  sinomo evolve campaign approve|reject <campaign> [options]",
            "  sinomo evolve tick [options]",
            "  sinomo evolve approve|reject|restore [--job <id>] [options]",
            "  sinomo evolve status|show|verify|packet|cleanup [--job <id>]",
            "  sinomo evolve trusted-signers list|publish|hydrate",
            "The active evolution dylib owns parsing, execution, and detailed command errors.",
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_preserves_raw_module_arguments() {
        let handler = evolution_cli_command_handler();
        let command = handler
            .parse(&[
                "propose".to_string(),
                "implement change".to_string(),
                "--dry-run".to_string(),
            ])
            .expect("parse proxy command");
        assert_eq!(command.name(), "evolve");
        assert!(!command.is_help());
    }
}
