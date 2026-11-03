//! Host-side session access for native modules. No provider code is retained by a worker.
use crate::*;
use sinomo_runtime::{
    local::SinomoDataDir,
    module::*,
    module_api::{self, sessions::*},
};
use std::{collections::BTreeMap, path::Path, sync::Arc};

pub struct ModuleSessionHost {
    data_dir: SinomoDataDir,
    sessions: Arc<AgentSessionHost>,
}
impl ModuleSessionHost {
    pub fn new(data_dir: &SinomoDataDir) -> Self {
        Self {
            data_dir: data_dir.clone(),
            sessions: AgentSessionHost::new(Arc::new(FileAgentSessionStore::for_data_dir(
                data_dir,
            ))),
        }
    }
}
impl NativeModuleRequestPreparer for ModuleSessionHost {
    fn id(&self) -> &'static str {
        "sinomo.agent.sessions"
    }
    fn prepare(
        &self,
        _: &Path,
        _: &NativeDylibModuleRouteTarget,
        _: &mut sinomo_runtime::network::ObjectRpcRequest,
    ) -> Result<(), sinomo_runtime::network::NetworkError> {
        Ok(())
    }
    fn host_services(
        &self,
        _: &Path,
        descriptor: &module_api::NativeModuleDescriptor,
        cancellation: &NativeModuleOperationCancellationToken,
    ) -> Result<
        BTreeMap<String, Arc<dyn NativeModuleHostCallbacks>>,
        sinomo_runtime::network::NetworkError,
    > {
        if !descriptor.component.requirements.iter().any(|r| {
            r.interface_id == SESSION_INTERFACE
                && r.interface_version == SESSION_VERSION
                && r.operations.iter().any(|op| op == SESSION_ACCESS)
        }) {
            return Ok(BTreeMap::new());
        }
        let access = ModuleSessions {
            data_dir: self.data_dir.clone(),
            descriptor: descriptor.clone(),
            cancellation: cancellation.clone(),
            sessions: self.sessions.clone(),
        };
        Ok([(
            SESSION_INTERFACE.into(),
            Arc::new(sessions::SessionHostCallbacks {
                sessions: Arc::new(access),
            }) as Arc<dyn NativeModuleHostCallbacks>,
        )]
        .into())
    }
}
struct ModuleSessions {
    cancellation: NativeModuleOperationCancellationToken,
    data_dir: SinomoDataDir,
    descriptor: module_api::NativeModuleDescriptor,
    sessions: Arc<AgentSessionHost>,
}
impl sessions::SessionAccess for ModuleSessions {
    fn request(&self, request: SessionRequest) -> Result<serde_json::Value, String> {
        match request {
            SessionRequest::Start { worker } => {
                let export = self
                    .descriptor
                    .surfaces
                    .agent_definitions
                    .iter()
                    .find(|a| a.id == worker.agent_id)
                    .ok_or("worker is not exported by this module")?;
                if worker.instructions.trim().is_empty()
                    || worker.instructions.len() > 128 * 1024
                    || worker.context.len() > 32
                {
                    return Err("invalid worker input".into());
                }
                let workspace = Path::new(&worker.workspace)
                    .canonicalize()
                    .map_err(|e| e.to_string())?;
                let profile = export
                    .worker
                    .as_ref()
                    .ok_or("agent is not exported as a hosted worker")?;
                let mut admitted = false;
                for root in &profile.workspace_roots {
                    let path = Path::new(root);
                    if path
                        .components()
                        .any(|c| !matches!(c, std::path::Component::Normal(_)))
                        || path.as_os_str().is_empty()
                    {
                        return Err("invalid module workspace grant".into());
                    }
                    let owned_root = self
                        .data_dir
                        .root()
                        .join(path)
                        .canonicalize()
                        .map_err(|e| e.to_string())?;
                    let data_root = self
                        .data_dir
                        .root()
                        .canonicalize()
                        .map_err(|e| e.to_string())?;
                    admitted |= owned_root.starts_with(&data_root)
                        && workspace.starts_with(&owned_root)
                        && workspace != owned_root;
                }
                if !workspace.is_dir() || !admitted {
                    return Err("worker workspace is outside the module grant".into());
                }
                let export: ModuleAgentDefinitionExport = serde_json::from_value(
                    serde_json::to_value(export).map_err(|e| e.to_string())?,
                )
                .map_err(|e| e.to_string())?;
                let mut definition =
                    AgentDefinition::try_from_module_export(&export).map_err(|e| e.to_string())?;
                definition.package = AgentPackageDefinition::from_module(
                    &self.descriptor.module_id,
                    &self.descriptor.module_version,
                );
                let interaction = InteractionSession::new(
                    SessionId::generate(),
                    NodeId::generate(),
                    InteractionSurface::new("module.worker", InteractionSurfaceKind::CommandLine),
                );
                let storage = AgentStoragePaths::from_data_dir(&self.data_dir);
                let workspace_config = local_workspace::LocalWorkspaceAgentConfig::new(&workspace)
                    .allowing_shell_commands(true)
                    .allowing_file_writes(true)
                    .with_agent_storage(&storage, &interaction.session_id);
                let sandbox =
                    sinomo_runtime::sandbox::SandboxLifecycleClient::in_process_at_runtime_root(
                        &workspace_config.sandbox_runtime_root,
                    );
                local_workspace::provision_default_agent_session_sandbox(
                    &sandbox,
                    &workspace_config,
                    vec![],
                )?;
                let tools = AgentToolRegistry::local_workspace_with_config(workspace_config);
                tools.set_local_permission_data_dir(self.data_dir.root());
                tools.set_usage_data_dir(self.data_dir.clone());
                let memory = SifsMemoryNoteBackend::new(&self.data_dir, &worker.agent_id)
                    .map_err(|e| e.to_string())?;
                memory_notes::register_tools(
                    &tools,
                    MemoryNoteAgentToolConfig::from_backend(memory),
                );
                let mut config = AgentConfig::new(&worker.provider, &worker.model)
                    .with_agent_definition(definition)
                    .with_workspace_root(&workspace)
                    .with_context_budget(AgentContextBudget::for_loaded_model(
                        &worker.provider,
                        &worker.model,
                        None,
                    ));
                if let Some(steps) = worker.max_steps {
                    config = config.with_max_steps(steps);
                }
                config.visible_toolsets.push("coding_advanced".into());
                let mut specs = config.definition.tools.clone();
                specs.retain(|t| t.kind != AgentToolKind::SessionHistorySearch);
                specs.push(AgentToolSpec::new(
                    AgentToolKind::ApplyPatch,
                    "apply_patch",
                    "Edit workspace files with a scoped patch",
                    true,
                ));
                let model = ModuleWorkerModel {
                    data_dir: self.data_dir.clone(),
                    provider: worker.provider.clone(),
                    model: worker.model.clone(),
                    workspace,
                };
                let agent = Agent::new(model, &worker.provider, &worker.model)
                    .with_config(config)
                    .with_tool_registry(tools);
                let agent = AgentRuntimeStorage::from_data_dir(&self.data_dir)
                    .apply_to_agent(agent, &interaction.session_id);
                let input = AgentLoopInput {
                    latest_event: user_interaction_input_event(&interaction, &worker.instructions),
                    session: interaction,
                    transient_system_context: None,
                    async_update: None,
                    context_attachments: worker
                        .context
                        .into_iter()
                        .map(serde_json::from_value)
                        .collect::<Result<_, _>>()
                        .map_err(|e| e.to_string())?,
                    tool_specs: specs,
                };
                let snapshot = self
                    .sessions
                    .start(
                        &self.descriptor.module_id,
                        agent,
                        input,
                        AgentCancellationToken::from_parent_signal(self.cancellation.signal()),
                    )
                    .map_err(|e| e.to_string())?;
                serde_json::to_value(snapshot).map_err(|e| e.to_string())
            }
            SessionRequest::Command { command } => self
                .sessions
                .owned_command(&self.descriptor.module_id, command)
                .map_err(|e| e.to_string()),
            read => {
                let roots = self
                    .sessions
                    .owned_roots(&self.descriptor.module_id)
                    .map_err(|e| e.to_string())?;
                crate::session_access::AgentSessionAccess::new(
                    Arc::new(FileAgentSessionStore::for_data_dir(&self.data_dir)),
                    roots,
                )
                .request(read)
            }
        }
    }
}
struct ModuleWorkerModel {
    data_dir: SinomoDataDir,
    provider: String,
    model: String,
    workspace: PathBuf,
}
impl InteractionModel for ModuleWorkerModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.complete_streaming_events_with_cancellation(
            request,
            &AgentCancellationToken::new(),
            &mut |_| {},
        )
    }
    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        token: &AgentCancellationToken,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        sinomo_lm::complete_configured_model_request_streaming_events_with_account_and_cancellation(
            &self.data_dir,
            &self.provider,
            &self.model,
            None,
            Some(self.workspace.clone()),
            request,
            &token.language_model_cancellation(),
            on_event,
        )
    }
}

/// Add the session owner to the host catalog, sharing its lifetime with the component scope.
pub fn with_session_host(
    catalog: &ComponentCatalog,
    data_dir: &SinomoDataDir,
) -> Result<ComponentCatalog, ComponentCatalogError> {
    let host = Arc::new(ModuleSessionHost::new(data_dir));
    let sessions = host.sessions.clone();
    let factory = StaticComponentFactory::new(
        ComponentDescriptor::new("sinomo-agent-sessions", env!("CARGO_PKG_VERSION")),
        move |_, scope| {
            let sessions = sessions.clone();
            scope.on_task_drain("managed sessions", move |deadline| {
                let sessions = sessions.clone();
                async move {
                    tokio::task::spawn_blocking(move || sessions.shutdown(deadline.into_std()))
                        .await
                        .map_err(|e| e.to_string())?
                }
            });
            Ok(())
        },
    )
    .map_err(|e| ComponentCatalogError::InvalidComponent {
        component_id: "sinomo-agent-sessions".into(),
        detail: e.to_string(),
    })?;
    catalog
        .to_builder()
        .register_native_request_preparer(host)?
        .register_factory(Arc::new(factory))?
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use module_api::{decode_sobj, encode_sobj, host::HostServiceRequest};

    #[test]
    fn module_grant_validates_worker_and_workspace_before_starting() {
        let temp = sinomo_testdir::TestDir::new("module-session-grants");
        let data = SinomoDataDir::new(temp.path());
        let host = ModuleSessionHost::new(&data);
        let mut descriptor = sinomo_subagents::descriptor();
        descriptor.module_id = "module.workers".into();
        let mut worker = module_api::NativeModuleAgentDefinition::new(
            "worker",
            "Worker",
            "deliberation",
            "Do the task",
        );
        worker.worker = Some(SessionWorkerProfile {
            workspace_roots: vec!["workers/jobs".into()],
        });
        descriptor.surfaces.agent_definitions = vec![worker];
        let cancellation = NativeModuleOperationCancellationToken::new();
        let grants = host
            .host_services(data.root().as_path(), &descriptor, &cancellation)
            .unwrap();
        let access = &grants[SESSION_INTERFACE];
        let request = |agent: &str, workspace: &Path| HostServiceRequest {
            interface_id: SESSION_INTERFACE.into(),
            interface_version: SESSION_VERSION.into(),
            operation: SESSION_ACCESS.into(),
            payload_sobj: encode_sobj(&SessionRequest::Start {
                worker: SessionWorkerRequest {
                    agent_id: agent.into(),
                    provider: "scripted".into(),
                    model: "scripted".into(),
                    workspace: workspace.display().to_string(),
                    instructions: "do the task".into(),
                    context: vec![],
                    max_steps: Some(2),
                },
            })
            .unwrap(),
        };
        assert!(
            access
                .host_service(&request("other-module-worker", temp.path()))
                .unwrap_err()
                .contains("not exported")
        );
        std::fs::create_dir_all(temp.path().join("workers/jobs/candidate")).unwrap();
        assert!(
            access
                .host_service(&request("worker", temp.path()))
                .unwrap_err()
                .contains("outside")
        );
        let list = HostServiceRequest {
            interface_id: SESSION_INTERFACE.into(),
            interface_version: SESSION_VERSION.into(),
            operation: SESSION_ACCESS.into(),
            payload_sobj: encode_sobj(&SessionRequest::List {
                include_archived: true,
                include_related: true,
            })
            .unwrap(),
        };
        let entries: serde_json::Value = decode_sobj(&access.host_service(&list).unwrap()).unwrap();
        assert_eq!(entries, serde_json::json!([]));
        descriptor.component.requirements.clear();
        assert!(
            host.host_services(data.root().as_path(), &descriptor, &cancellation)
                .unwrap()
                .is_empty()
        );
    }
}
