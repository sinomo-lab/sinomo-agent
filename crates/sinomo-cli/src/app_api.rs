//! Stable, GUI-facing surface over the main-agent chat runtime.
//!
//! The graphical app (`sinomo-app`) drives the *same* interaction runtime the
//! terminal UI uses, but needs a small, owned, type-erased API rather than the
//! crate-internal handler types. Everything here returns plain data so the app
//! never depends on `tui` internals.

use sinomo_agent::{
    AgentCatalog, AgentDefinition, InteractionOutput, InteractionSession, InteractionSurface,
    InteractionSurfaceKind,
};
use sinomo_lm::{
    AnthropicConfig, ClaudeCodeConfig, CodexCliConfig, GeminiConfig, GitHubCopilotConfig,
    OpenAiConfig, PROVIDER_DEFAULT_MODEL, ProviderReasoningEffort,
};
use sinomo_runtime::events::{InMemoryEventStore, SessionId};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::node::NodeId;

use crate::error::CliError;
use crate::tui::{
    self, DefaultMainAgentChatModel, InteractionRuntime, RuntimeMainAgentInteractionHandler,
};

type Handler = RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel>;

/// The active agent / model selection, as plain strings.
#[derive(Clone, Debug, Default)]
pub struct ChatRuntimeInfo {
    pub agent: String,
    pub provider: String,
    pub model: String,
    pub account: Option<String>,
    pub reasoning_effort: Option<String>,
}

impl From<tui::AppRuntimeInfo> for ChatRuntimeInfo {
    fn from(info: tui::AppRuntimeInfo) -> Self {
        ChatRuntimeInfo {
            agent: info.agent,
            provider: info.provider,
            model: info.model,
            account: info.account,
            reasoning_effort: info.reasoning_effort,
        }
    }
}

/// A streamed event produced while the agent answers.
#[derive(Clone, Debug)]
pub enum ChatStreamEvent {
    /// A visible answer fragment.
    Token(String),
    /// A transient progress / status note (e.g. "waiting for model runtime").
    Notice(String),
    /// Token accounting for the just-finished turn.
    Usage { provider: String, model: String },
}

/// A selectable agent.
#[derive(Clone, Debug)]
pub struct AgentSummary {
    pub key: String,
    pub name: String,
    pub description: String,
}

/// A selectable language-model provider with a small starter model list.
#[derive(Clone, Debug)]
pub struct ProviderSummary {
    pub id: String,
    pub name: String,
    pub models: Vec<String>,
}

/// One running chat conversation backed by the main-agent runtime.
pub struct AgentChat {
    data_dir: SinomoDataDir,
    daemon_connected: bool,
    runtime: InteractionRuntime<InMemoryEventStore, Handler>,
}

impl AgentChat {
    /// Build a fresh conversation. `daemon_connected` selects cluster vs. local
    /// config resolution for model switching.
    pub fn new(data_dir: SinomoDataDir, daemon_connected: bool) -> Result<Self, CliError> {
        let handler = tui::app_build_chat_handler(&data_dir, None)?;
        let runtime =
            InteractionRuntime::new(InMemoryEventStore::new(), handler, app_session(None));
        Ok(Self {
            data_dir,
            daemon_connected,
            runtime,
        })
    }

    /// Begin a brand-new session (clears the in-memory transcript/context).
    pub fn reset_session(&mut self) {
        self.runtime
            .replace_store_and_session(InMemoryEventStore::new(), app_session(None));
    }

    /// Adopt an existing session id for continued turns (fresh in-memory context;
    /// the visible transcript is restored by the caller from its own store).
    pub fn resume_session(&mut self, session_id: &str) {
        self.runtime
            .replace_store_and_session(InMemoryEventStore::new(), app_session(Some(session_id)));
    }

    pub fn session_id(&self) -> String {
        self.runtime.session().session_id.to_string()
    }

    pub fn runtime_info(&self) -> ChatRuntimeInfo {
        tui::app_runtime_info(self.runtime.handler()).into()
    }

    /// Send a user message and stream the agent's reply through `on_event`.
    pub fn send(
        &mut self,
        text: &str,
        on_event: &mut dyn FnMut(ChatStreamEvent),
    ) -> Result<(), CliError> {
        let mut emit = |output: InteractionOutput| forward_output(output, on_event);
        let trailing = self.runtime.ingest_user_input_streaming(text, &mut emit)?;
        for output in trailing {
            forward_output(output, on_event);
        }
        Ok(())
    }

    /// Switch the active provider/model/account; returns the new selection.
    pub fn set_model(
        &mut self,
        provider: impl Into<String>,
        model: impl Into<String>,
        account: impl Into<String>,
    ) -> Result<ChatRuntimeInfo, CliError> {
        let info = tui::app_switch_model(
            self.runtime.handler_mut(),
            &self.data_dir,
            provider.into(),
            model.into(),
            account.into(),
            self.daemon_connected,
        )?;
        Ok(info.into())
    }

    /// Switch to a named built-in / stored agent definition.
    pub fn set_agent(&mut self, agent_key: &str) -> Result<ChatRuntimeInfo, CliError> {
        let definition =
            resolve_agent(&self.data_dir, agent_key).ok_or_else(|| CliError::Parse {
                detail: format!("unknown agent `{agent_key}`"),
            })?;
        let info = tui::app_switch_agent(
            self.runtime.handler_mut(),
            &self.data_dir,
            definition,
            self.daemon_connected,
        )?;
        Ok(info.into())
    }
}

fn forward_output(output: InteractionOutput, on_event: &mut dyn FnMut(ChatStreamEvent)) {
    match output {
        InteractionOutput::Chunk(chunk) => {
            for event in split_chunk(&chunk) {
                on_event(event);
            }
        }
        InteractionOutput::Line(line) => on_event(ChatStreamEvent::Token(line)),
        InteractionOutput::ModelUsage {
            provider, model, ..
        } => on_event(ChatStreamEvent::Usage { provider, model }),
        InteractionOutput::Exit => {}
    }
}

/// Separate inline `<sinomo_progress>…</sinomo_progress>` status markers from
/// visible answer text.
fn split_chunk(chunk: &str) -> Vec<ChatStreamEvent> {
    if let Some(rest) = chunk.strip_prefix("<sinomo_progress>") {
        if let Some(progress) = rest.strip_suffix("</sinomo_progress>") {
            return vec![ChatStreamEvent::Notice(progress.to_string())];
        }
    }
    vec![ChatStreamEvent::Token(chunk.to_string())]
}

fn app_session(session_id: Option<&str>) -> InteractionSession {
    use std::str::FromStr;
    let node_id = NodeId::from_legacy_named("node_app").expect("valid app node id");
    let session_id = session_id
        .and_then(|id| SessionId::from_str(id).ok())
        .unwrap_or_else(SessionId::generate);
    InteractionSession::new(
        session_id,
        node_id,
        InteractionSurface::new("surface.app", InteractionSurfaceKind::App),
    )
}

/// All agents available for selection (stored definitions first, else built-ins).
pub fn agent_summaries(data_dir: &SinomoDataDir) -> Vec<AgentSummary> {
    let catalog =
        AgentCatalog::load_from_data_dir(data_dir).unwrap_or_else(|_| AgentCatalog::built_ins());
    catalog
        .list()
        .map(|definition| AgentSummary {
            key: definition.key().to_string(),
            name: if definition.name.is_empty() {
                definition.key().to_string()
            } else {
                definition.name.clone()
            },
            description: definition.description.clone(),
        })
        .collect()
}

fn resolve_agent(data_dir: &SinomoDataDir, key: &str) -> Option<AgentDefinition> {
    let catalog =
        AgentCatalog::load_from_data_dir(data_dir).unwrap_or_else(|_| AgentCatalog::built_ins());
    catalog.get(key).cloned()
}

/// Curated provider list with a small starter set of models each.
pub fn provider_summaries() -> Vec<ProviderSummary> {
    vec![
        ProviderSummary {
            id: "anthropic".to_string(),
            name: "Anthropic".to_string(),
            models: dedup(vec![
                AnthropicConfig::default().default_model,
                "claude-opus-4-8".to_string(),
                "claude-sonnet-4-6".to_string(),
                "claude-haiku-4-5".to_string(),
            ]),
        },
        ProviderSummary {
            id: "openai".to_string(),
            name: "OpenAI".to_string(),
            models: dedup(vec![
                OpenAiConfig::default().default_model,
                "gpt-5".to_string(),
                "o4-mini".to_string(),
            ]),
        },
        ProviderSummary {
            id: "gemini".to_string(),
            name: "Google Gemini".to_string(),
            models: dedup(vec![
                GeminiConfig::default().default_model,
                "gemini-2.5-pro".to_string(),
                "gemini-2.5-flash".to_string(),
            ]),
        },
        ProviderSummary {
            id: "claude-code".to_string(),
            name: "Claude Code (CLI)".to_string(),
            models: dedup(vec![ClaudeCodeConfig::default().model]),
        },
        ProviderSummary {
            id: "codex-cli".to_string(),
            name: "Codex (CLI)".to_string(),
            models: dedup(vec![CodexCliConfig::default().model]),
        },
        ProviderSummary {
            id: "github-copilot".to_string(),
            name: "GitHub Copilot".to_string(),
            models: dedup(vec![GitHubCopilotConfig::default().default_model]),
        },
    ]
}

/// Reasoning-effort values, lowest to highest.
pub fn reasoning_efforts() -> Vec<String> {
    ProviderReasoningEffort::VALUES
        .iter()
        .map(|value| value.to_string())
        .collect()
}

/// Default model identifier used when no provider is configured yet.
pub fn fallback_model() -> &'static str {
    PROVIDER_DEFAULT_MODEL
}

fn dedup(values: Vec<String>) -> Vec<String> {
    let mut seen = Vec::new();
    for value in values {
        if !value.is_empty() && !seen.contains(&value) {
            seen.push(value);
        }
    }
    seen
}

// ----------------------------------------------------------------------------
// SIFS browsing
// ----------------------------------------------------------------------------

use crate::file_manager::{FileManagerBackend, FileManagerEntryKind};
use crate::sifs_file_manager::SifsFileManagerBackend;

/// One SIFS directory entry.
#[derive(Clone, Debug)]
pub struct SifsEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    pub is_link: bool,
    pub size: Option<u64>,
}

/// A handle to browse SIFS (the distributed filesystem). In local mode this is
/// backed by the local content store + metadata; with a daemon it spans the
/// cluster.
pub struct SifsBrowser {
    backend: SifsFileManagerBackend,
    node_id: String,
}

impl SifsBrowser {
    pub fn open(data_dir: &SinomoDataDir) -> Result<Self, String> {
        let backend = SifsFileManagerBackend::open(data_dir)?;
        let node_id = backend.node_label();
        Ok(Self { backend, node_id })
    }

    /// The node SIFS objects are placed on locally.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// List a directory (directories first, then files, name-sorted).
    pub fn list(&self, path: &str) -> Result<Vec<SifsEntry>, String> {
        let mut entries: Vec<SifsEntry> = self
            .backend
            .list_dir(path)?
            .into_iter()
            .map(|entry| SifsEntry {
                name: entry.name,
                path: entry.path,
                is_dir: matches!(entry.kind, FileManagerEntryKind::Directory),
                is_link: matches!(entry.kind, FileManagerEntryKind::Symlink),
                size: entry.size,
            })
            .collect();
        entries.sort_by(|a, b| {
            (b.is_dir as u8)
                .cmp(&(a.is_dir as u8))
                .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
        });
        Ok(entries)
    }

    pub fn parent(&self, path: &str) -> Option<String> {
        self.backend.parent_path(path)
    }

    pub fn normalize(&self, path: &str) -> String {
        self.backend
            .normalize_path(path)
            .unwrap_or_else(|_| "/".to_string())
    }
}
