//! Generic adapters for commands published by live module owners.
use sinomo_runtime::api::cli::*;
use sinomo_runtime::cli_registry::{
    CliAutocompleteSpec, CliCommand, CliCommandContext, CliCommandError, CliCommandHandler,
    CliCommandHelp, CliCommandMetadata, CliCommandPolicy, CliCommandRegistry, CliParseError,
};
use std::sync::Arc;
use std::time::Duration;

pub trait ModuleCliTransport: Send + Sync {
    fn catalog(&self) -> Result<CliCatalogResponse, String>;
    fn invoke(
        &self,
        request: CliInvokeRequest,
        timeout: Duration,
    ) -> Result<CliInvokeResponse, String>;
}

#[cfg(not(test))]
struct DaemonTransport {
    address: String,
}

#[cfg(not(test))]
impl DaemonTransport {
    async fn cancel(&self, operation_id: &str, reason: &str, runtime: &tokio::runtime::Handle) {
        let Ok(cancel) = sinomo_runtime::network::ObjectRpcRequest::encode_body(
            sinomo_runtime::module::MODULE_NATIVE_DYLIB_CANCEL_ROUTE,
            sinomo_runtime::module::MODULE_RPC_CATEGORY,
            &sinomo_runtime::network::ObjectRpcCancelRequest {
                operation_id: operation_id.into(),
                reason: reason.into(),
            },
        ) else {
            return;
        };
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            sinomo_runtime::api::send_daemon_rpc_async(
                &self.address,
                &cancel,
                sinomo_runtime::network::DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(
                    2,
                )),
                runtime,
            ),
        )
        .await;
    }
}

#[cfg(not(test))]
impl ModuleCliTransport for DaemonTransport {
    fn catalog(&self) -> Result<CliCatalogResponse, String> {
        let request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
            CLI_CATALOG_ROUTE,
            CLI_RPC_CATEGORY,
            &CliCatalogRequest,
        )
        .map_err(|e| e.to_string())?;
        sinomo_runtime::api::send_daemon_rpc(&self.address, &request, Duration::from_millis(750))
            .map_err(|e| e.to_string())?
            .decode_body()
            .map_err(|e| e.to_string())
    }

    fn invoke(
        &self,
        invoke: CliInvokeRequest,
        timeout: Duration,
    ) -> Result<CliInvokeResponse, String> {
        let operation_id = invoke.request.invocation_id.clone();
        let request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
            CLI_INVOKE_ROUTE,
            CLI_RPC_CATEGORY,
            &invoke,
        )
        .map_err(|e| e.to_string())?
        .with_operation_id(&operation_id);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        runtime.block_on(async {
            let owner_runtime = tokio::runtime::Handle::current();
            let timeouts = sinomo_runtime::network::DaemonRpcTimeouts::connect_only(Duration::from_secs(5))
                .with_idle_timeout(Some(timeout));
            tokio::select! {
                response = tokio::time::timeout(timeout, sinomo_runtime::api::send_daemon_rpc_async(&self.address, &request, timeouts, &owner_runtime)) => {
                    match response {
                        Ok(response) => response.map_err(|e| e.to_string())?.decode_body().map_err(|e| e.to_string()),
                        Err(_) => {
                            self.cancel(&operation_id, "CLI deadline elapsed", &owner_runtime).await;
                            Err("module command deadline elapsed".into())
                        }
                    }
                }
                signal = tokio::signal::ctrl_c() => {
                    signal.map_err(|error| format!("could not listen for CLI interruption: {error}"))?;
                    self.cancel(&operation_id, "CLI interrupted", &owner_runtime).await;
                    Ok(CliInvokeResponse::Completed(CliModuleResponse::failure(130, "command interrupted")))
                }
            }
        })
    }
}

pub fn compose_module_commands(
    mut registry: CliCommandRegistry,
    transport: Arc<dyn ModuleCliTransport>,
) -> Result<CliCommandRegistry, String> {
    let catalog = transport.catalog()?;
    for entry in catalog.commands {
        entry.command.validate()?;
        let autocomplete =
            sinomo_runtime::cli_registry::module_command_autocomplete(&entry.command)?;
        registry = registry.try_register(ModuleCommandHandler {
            entry,
            catalog_id: catalog.catalog_id.clone(),
            generation: catalog.generation,
            transport: Arc::clone(&transport),
            autocomplete,
        })?;
    }
    Ok(registry)
}

#[cfg(not(test))]
pub(crate) fn discover(registry: CliCommandRegistry) -> Result<CliCommandRegistry, String> {
    let status = crate::daemon_default_main_daemon_status(Duration::from_millis(750))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "no live daemon command catalog is available".to_string())?;
    compose_module_commands(
        registry,
        Arc::new(DaemonTransport {
            address: status.address,
        }),
    )
}

#[derive(Clone)]
struct ModuleCommandHandler {
    catalog_id: String,
    entry: CliCatalogEntry,
    generation: u64,
    transport: Arc<dyn ModuleCliTransport>,
    autocomplete: CliAutocompleteSpec,
}
impl CliCommandHandler for ModuleCommandHandler {
    fn metadata(&self) -> CliCommandMetadata {
        CliCommandMetadata::from_owned(
            &self.entry.command.name,
            self.entry.command.aliases.clone(),
            &self.entry.command.summary,
            CliCommandPolicy::default(),
        )
    }
    fn help(&self) -> CliCommandHelp {
        CliCommandHelp::new(&self.entry.command.name, self.entry.command.help.clone())
    }
    fn autocomplete(&self) -> CliAutocompleteSpec {
        self.autocomplete.clone()
    }
    fn parse(&self, args: &[String]) -> Result<Box<dyn CliCommand>, CliParseError> {
        Ok(Box::new(ModuleCommand {
            handler: self.clone(),
            args: args.to_vec(),
        }))
    }
}
struct ModuleCommand {
    handler: ModuleCommandHandler,
    args: Vec<String>,
}
impl CliCommand for ModuleCommand {
    fn name(&self) -> &str {
        &self.handler.entry.command.name
    }
    fn policy(&self) -> CliCommandPolicy {
        if !self.is_help() && self.handler.entry.command.requires_cluster_state {
            CliCommandPolicy::cluster_required()
        } else {
            CliCommandPolicy::default()
        }
    }
    fn is_help(&self) -> bool {
        (self.args.is_empty() && self.handler.entry.command.help_on_empty)
            || matches!(self.args.as_slice(), [arg] if matches!(arg.as_str(), "help" | "--help" | "-h"))
    }
    fn help(&self) -> CliCommandHelp {
        self.handler.help()
    }
    fn autocomplete(&self) -> CliAutocompleteSpec {
        self.handler.autocomplete()
    }
    fn run(self: Box<Self>, context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError> {
        let mut stdin = Vec::new();
        if self.handler.entry.command.accepts_stdin && !self.is_help() {
            stdin = context.read_stdin(CLI_MAX_INPUT_BYTES)?;
        }
        let request = CliModuleRequest {
            command_id: self.handler.entry.command.command_id.clone(),
            invocation_id: sinomo_runtime::core::Id::generate().to_string(),
            action: if self.is_help() {
                CliAction::Help
            } else {
                CliAction::Invoke
            },
            args: self.args,
            stdin,
            current_dir: std::env::current_dir()
                .ok()
                .and_then(|path| path.into_os_string().into_string().ok()),
        };
        request.validate().map_err(CliCommandError::new)?;
        let response = self
            .handler
            .transport
            .invoke(
                CliInvokeRequest {
                    catalog_id: self.handler.catalog_id.clone(),
                    generation: self.handler.generation,
                    module_id: self.handler.entry.module_id.clone(),
                    request,
                },
                Duration::from_millis(self.handler.entry.command.max_deadline_ms),
            )
            .map_err(CliCommandError::new)?;
        let response = match response {
            CliInvokeResponse::Completed(response) => response,
            CliInvokeResponse::CatalogChanged => {
                return Err(CliCommandError::new(
                    "command catalog changed before execution; retry after refreshing discovery",
                ));
            }
            CliInvokeResponse::Unavailable { detail }
            | CliInvokeResponse::ExecutionFailed { detail } => {
                return Err(CliCommandError::new(detail));
            }
        };
        response.validate().map_err(CliCommandError::new)?;
        context
            .stdout()
            .write_all(&response.stdout)
            .map_err(|e| CliCommandError::new(e.to_string()))?;
        context
            .stderr()
            .write_all(&response.stderr)
            .map_err(|e| CliCommandError::new(e.to_string()))?;
        if response.exit_code != 0 {
            return Err(CliCommandError::new(format!(
                "module returned exit code {}",
                response.exit_code
            ))
            .with_exit_code(response.exit_code));
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn discover(_registry: CliCommandRegistry) -> Result<CliCommandRegistry, String> {
    Err("module discovery requires an explicitly supplied test transport".into())
}
