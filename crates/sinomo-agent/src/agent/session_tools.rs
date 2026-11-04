//! Binding for native tools that operate on host-owned agent sessions.
use super::*;
use sinomo_runtime::module::{ActiveNativeComponent, ActiveNativeComponentResponse};
use sinomo_runtime::module_api::{ServiceRequirement, agent::*};

pub trait AgentSessionToolProvider: Send + Sync {
    fn definition(&self) -> SessionToolDefinition;
    fn provenance(&self) -> Option<(String, String)> {
        None
    }
    fn execute(
        &self,
        input: serde_json::Value,
        sessions: Arc<dyn sinomo_runtime::module::sessions::SessionAccess>,
    ) -> Result<serde_json::Value, AgentError>;
}

pub struct NativeAgentSessionTools {
    provider: Mutex<Option<ActiveNativeComponent>>,
    data_dir: Option<SinomoDataDir>,
    definition: SessionToolDefinition,
    provenance: (String, String),
}
impl NativeAgentSessionTools {
    pub fn requirement() -> ServiceRequirement {
        ServiceRequirement::new(SESSION_TOOLS_INTERFACE, SESSION_TOOLS_VERSION)
            .with_operation(SESSION_TOOLS_DESCRIBE)
            .with_operation(SESSION_TOOLS_EXECUTE)
    }
    pub fn from_component(provider: ActiveNativeComponent) -> Result<Self, AgentError> {
        let definition: SessionToolDefinition = match provider
            .invoke_sobj(SESSION_TOOLS_DESCRIBE, &(), None)
            .map_err(|e| error(e.to_string()))?
        {
            ActiveNativeComponentResponse::Value(value) => value,
            ActiveNativeComponentResponse::Error(e) => return Err(error(e.detail)),
        };
        if definition.name.trim().is_empty()
            || definition.name.len() > 128
            || definition.skill.len() > 128 * 1024
            || !definition.input_schema.is_object()
        {
            return Err(error("invalid native session tool definition"));
        }
        let markdown =
            crate::parse_skill_markdown(&definition.skill).map_err(|e| error(e.to_string()))?;
        if markdown.name() != definition.skill_id {
            return Err(error("native session skill identity mismatch"));
        }
        let provenance = (
            provider.descriptor().module_id.clone(),
            provider.descriptor().module_version.clone(),
        );
        Ok(Self {
            provenance,
            provider: Mutex::new(Some(provider)),
            data_dir: None,
            definition,
        })
    }
    pub fn resolve_shared(data_dir: &SinomoDataDir) -> Result<Arc<Self>, AgentError> {
        Ok(Arc::new(Self::resolve(data_dir)?))
    }
    pub fn resolve(data_dir: &SinomoDataDir) -> Result<Self, AgentError> {
        // Activation already verified policy and provenance; use the shared loader.
        let provider =
            unsafe { ActiveNativeComponent::resolve_shared(data_dir.root(), &Self::requirement()) }
                .map_err(|e| error(e.to_string()))?;
        let mut tools = Self::from_component(provider)?;
        // Tool code is needed only during an invocation. Release the mapping
        // between calls so module
        // withdrawal/replacement never waits for idle agent sessions.
        tools
            .provider
            .get_mut()
            .map_err(|_| error("session tool provider lock poisoned"))?
            .take();
        tools.data_dir = Some(data_dir.clone());
        Ok(tools)
    }
}
impl AgentSessionToolProvider for NativeAgentSessionTools {
    fn provenance(&self) -> Option<(String, String)> {
        Some(self.provenance.clone())
    }
    fn definition(&self) -> SessionToolDefinition {
        self.definition.clone()
    }
    fn execute(
        &self,
        input: serde_json::Value,
        sessions: Arc<dyn sinomo_runtime::module::sessions::SessionAccess>,
    ) -> Result<serde_json::Value, AgentError> {
        let provider = if let Some(data_dir) = &self.data_dir {
            unsafe { ActiveNativeComponent::resolve_shared(data_dir.root(), &Self::requirement()) }
                .map_err(|e| error(e.to_string()))?
        } else {
            self.provider
                .lock()
                .map_err(|_| error("session provider lock poisoned"))?
                .as_ref()
                .cloned()
                .ok_or_else(|| error("session provider unavailable"))?
        };
        let callbacks =
            Arc::new(sinomo_runtime::module::sessions::SessionHostCallbacks { sessions });
        match provider
            .invoke_sobj(SESSION_TOOLS_EXECUTE, &input, Some(callbacks))
            .map_err(|e| error(e.to_string()))?
        {
            ActiveNativeComponentResponse::Value(value) => Ok(value),
            ActiveNativeComponentResponse::Error(e) => Err(error(e.detail)),
        }
    }
}
fn error(detail: impl Into<String>) -> AgentError {
    AgentError::Runtime {
        detail: detail.into(),
    }
}
impl<M: InteractionModel> Agent<M> {
    pub fn with_session_tool_provider(
        mut self,
        provider: Arc<dyn AgentSessionToolProvider>,
    ) -> Self {
        let definition = provider.definition();
        let entry = crate::SkillCatalogEntry::new(
            &definition.skill_id,
            "1.0.0",
            provider
                .provenance()
                .map(|(id, version)| crate::SkillCatalogSource::module(id, version))
                .unwrap_or_else(|| crate::SkillCatalogSource::sinomo("1.0.0")),
            &definition.skill,
        )
        .with_agent_skill_frontmatter(
            &crate::parse_skill_markdown(&definition.skill)
                .expect("validated native session skill")
                .frontmatter,
        );
        let activation = crate::SkillActivationRecord::new(
            self.config.definition.key(),
            &entry,
            "native-session-provider",
            "active native session tool",
        );
        self.config.skill_catalog_texts.insert(
            crate::skill_catalog_entry_text_key(&entry),
            definition.skill,
        );
        self.config
            .skill_catalog
            .upsert_entry(entry)
            .expect("valid native skill");
        self.config
            .skill_catalog
            .set_activation(activation)
            .expect("valid native skill activation");
        self.session_tools = Some(provider);
        self
    }
}

type SessionAccessFactory = dyn Fn(
        &SessionId,
        AgentCancellationToken,
    ) -> Result<Arc<dyn sinomo_runtime::module::sessions::SessionAccess>, AgentError>
    + Send
    + Sync;

/// Generic module adapter: schemas, discovery groups and execution come from
/// the provider. The host supplies only the caller's scoped session access.
pub(super) struct SessionProviderTool {
    provider: Arc<dyn AgentSessionToolProvider>,
    sessions: Arc<SessionAccessFactory>,
}
impl SessionProviderTool {
    pub(super) fn new(
        provider: Arc<dyn AgentSessionToolProvider>,
        sessions: impl Fn(
            &SessionId,
            AgentCancellationToken,
        ) -> Result<
            Arc<dyn sinomo_runtime::module::sessions::SessionAccess>,
            AgentError,
        > + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            provider,
            sessions: Arc::new(sessions),
        }
    }
}
impl AgentTool for SessionProviderTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        let definition = self.provider.definition();
        let mut descriptor = AgentToolDescriptor::new(crate::AgentToolSpec::new(
            crate::AgentToolKind::ModuleService,
            definition.name,
            definition.description,
            false,
        ))
        .with_input_schema(definition.input_schema)
        .with_capabilities([AgentToolCapability::CallModule]);
        if !definition.toolsets.is_empty() {
            descriptor = descriptor.with_toolsets(definition.toolsets);
        }
        descriptor
    }
    fn execute(&self, request: crate::AgentToolRequest<'_>) -> crate::AgentToolResult {
        let result = (self.sessions)(
            &request.session.session_id,
            request.cancellation_token.clone(),
        )
        .and_then(|sessions| self.provider.execute(request.call.input.clone(), sessions));
        match result {
            Ok(value) => {
                crate::AgentToolResult::succeeded("module session operation completed", value)
            }
            Err(error) => crate::AgentToolResult::failed(
                error.to_string(),
                serde_json::json!({"error": error.to_string()}),
            ),
        }
    }
}
