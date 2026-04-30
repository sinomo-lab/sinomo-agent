//! Shared Sinomo command-line handlers.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    CandidateBundleManifest, CandidateDecision, CandidateDecisionOutcome, Confidence,
    EvaluationGate, EvaluationOutcome, EventEnvelope, EventId, EventKind, EvolutionJob,
    EvolutionJobId, EvolutionJobStatus, LatencyClass, MutationPacket, NodeId, PrivacyClass,
    RoutingMetadata, RoutingScope, Salience, SessionId, Source, TrustRing, VerificationCommand,
};
use sinomo_eval::{ExecutingEvalHarness, LocalCommandRunner};
use sinomo_events::{EventStore, InMemoryEventStore, ReplayDirection, ReplayQuery};
use sinomo_evolution::{
    CandidateWorkspace, EvolutionOrchestrator, EvolutionReport, EvolutionRequest,
    GitWorkspaceAdapter, LanguageModelMutationBridge, LocalCandidateBundleStore, MutationModel,
    MutationModelError,
};
use sinomo_lm::{
    AnthropicConfig, AnthropicProvider, CLAUDE_CODE_DEFAULT_MODEL, CODEX_CLI_DEFAULT_MODEL,
    ClaudeCodeConfig, ClaudeCodeProvider, CodexCliConfig, CodexCliProvider, GeminiConfig,
    GeminiProvider, GitHubCopilotCliConfig, GitHubCopilotCliProvider, GitHubCopilotConfig,
    GitHubCopilotProvider, LanguageModelError, LanguageModelMessage, LanguageModelProvider,
    LanguageModelRequest, LanguageModelResponse, LanguageModelResponseFormat, LanguageModelRole,
    OpenAiConfig, OpenAiProvider, PROVIDER_DEFAULT_MODEL,
};
use sinomo_local::SinomoDataDir;
use std::env;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

pub fn run(args: Vec<String>) -> Result<(), CliError> {
    if args.is_empty() {
        return run_cui();
    }

    run_cli_command(&args)
}

fn run_cli_command(args: &[String]) -> Result<(), CliError> {
    if args.first().map(String::as_str) == Some("evolve") {
        return run_evolve_command(&args[1..]);
    }

    if let Some(command) = parse_provider_command(&args) {
        return run_provider_command(command);
    }

    match args {
        [help] if help == "help" || help == "--help" || help == "-h" => {
            print_usage();
            Ok(())
        }
        [] => {
            print_usage();
            Ok(())
        }
        _ => Err(CliError::Usage),
    }
}

pub fn run_cui() -> Result<(), CliError> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    run_cui_with_io(stdin.lock(), stdout.lock())
}

pub fn run_cui_with_io<R: BufRead, W: Write>(mut reader: R, mut writer: W) -> Result<(), CliError> {
    let handler = CliInteractionHandler::from_default_model()?;
    run_cui_with_handler_io(&mut reader, &mut writer, handler)
}

pub fn run_cui_with_handler_io<R: BufRead, W: Write, H: InteractionEventHandler>(
    mut reader: R,
    mut writer: W,
    handler: H,
) -> Result<(), CliError> {
    let session = InteractionSession::new(
        SessionId::generate(),
        NodeId::new("node_cli").expect("valid node id"),
        InteractionSurface::new("surface.cli", InteractionSurfaceKind::CommandLine),
    );
    let mut runtime = InteractionRuntime::new(InMemoryEventStore::new(), handler, session.clone());

    writeln!(writer, "Sinomo CUI").map_err(CliError::stdio)?;
    writeln!(writer, "session: {}", session.session_id).map_err(CliError::stdio)?;
    writeln!(writer, "Type /help for commands or /quit to exit.").map_err(CliError::stdio)?;

    loop {
        write!(writer, "sinomo> ").map_err(CliError::stdio)?;
        writer.flush().map_err(CliError::stdio)?;

        let mut line = String::new();
        if reader.read_line(&mut line).map_err(CliError::stdio)? == 0 {
            break;
        }

        let line = line.trim_end_matches(['\r', '\n']);
        if line.trim().is_empty() {
            continue;
        }

        let outputs = runtime.ingest_user_input(line)?;
        for output in outputs {
            match output {
                InteractionOutput::Line(line) => {
                    writeln!(writer, "{line}").map_err(CliError::stdio)?;
                }
                InteractionOutput::Exit => return Ok(()),
            }
        }
    }

    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct InteractionSession {
    pub session_id: SessionId,
    pub node_id: NodeId,
    pub surface: InteractionSurface,
}

impl InteractionSession {
    pub fn new(session_id: SessionId, node_id: NodeId, surface: InteractionSurface) -> Self {
        Self {
            session_id,
            node_id,
            surface,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct InteractionSurface {
    pub surface_id: String,
    pub kind: InteractionSurfaceKind,
}

impl InteractionSurface {
    pub fn new(surface_id: impl Into<String>, kind: InteractionSurfaceKind) -> InteractionSurface {
        InteractionSurface {
            surface_id: surface_id.into(),
            kind,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum InteractionSurfaceKind {
    CommandLine,
    App,
    Api,
    Voice,
    Remote,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum InteractionInputKind {
    Message,
    Command,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InteractionOutput {
    Line(String),
    Exit,
}

pub trait InteractionEventHandler {
    fn handle_event<S: EventStore>(
        &mut self,
        store: &mut S,
        session: &InteractionSession,
        event: &EventEnvelope,
    ) -> Result<Vec<InteractionOutput>, CliError>;
}

#[derive(Clone, Debug)]
pub struct InteractionRuntime<S, H> {
    store: S,
    handler: H,
    session: InteractionSession,
}

impl<S: EventStore, H: InteractionEventHandler> InteractionRuntime<S, H> {
    pub fn new(store: S, handler: H, session: InteractionSession) -> Self {
        Self {
            store,
            handler,
            session,
        }
    }

    pub fn ingest_user_input(&mut self, text: &str) -> Result<Vec<InteractionOutput>, CliError> {
        let event = user_input_event(&self.session, text);
        self.store.append(event.clone())?;
        self.handler
            .handle_event(&mut self.store, &self.session, &event)
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn session(&self) -> &InteractionSession {
        &self.session
    }
}

pub trait InteractionModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError>;
}

impl<T: LanguageModelProvider> InteractionModel for T {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        LanguageModelProvider::complete(self, request)
    }
}

#[derive(Clone, Debug)]
pub struct ModelBackedInteractionHandler<M> {
    model: M,
    provider: String,
    model_name: String,
}

pub type CliInteractionHandler = ModelBackedInteractionHandler<CliConversationModel>;

impl<M> ModelBackedInteractionHandler<M> {
    pub fn new(model: M, provider: impl Into<String>, model_name: impl Into<String>) -> Self {
        Self {
            model,
            provider: provider.into(),
            model_name: model_name.into(),
        }
    }
}

impl CliInteractionHandler {
    pub fn from_default_model() -> Result<Self, CliError> {
        let model = CliConversationModel::from_defaults()?;
        Ok(Self::new(
            model.clone(),
            model.provider.clone(),
            model.model.clone(),
        ))
    }
}

impl<M: InteractionModel> InteractionEventHandler for ModelBackedInteractionHandler<M> {
    fn handle_event<S: EventStore>(
        &mut self,
        store: &mut S,
        session: &InteractionSession,
        event: &EventEnvelope,
    ) -> Result<Vec<InteractionOutput>, CliError> {
        let text = event
            .payload
            .get("text")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();

        let Some(args) = parse_slash_command(text)? else {
            let message = interaction_event(
                session,
                "interaction.message.received",
                serde_json::json!({
                    "input_event_id": event.event_id,
                    "text": text,
                    "state": "received",
                    "delivery": "event-stream"
                }),
            )
            .caused_by(&event.event_id);
            store.append(message.clone())?;
            let request = conversation_model_request(
                store,
                session,
                &message.event_id,
                text,
                &self.provider,
                &self.model_name,
            )?;
            let requested = interaction_event(
                session,
                "interaction.model.requested",
                serde_json::json!({
                    "message_event_id": message.event_id,
                    "provider": request.provider,
                    "model": request.model,
                    "message_count": request.messages.len()
                }),
            )
            .caused_by(&message.event_id);
            store.append(requested.clone())?;

            match self.model.complete(&request) {
                Ok(response) => {
                    let response_event = interaction_event(
                        session,
                        "interaction.model.responded",
                        serde_json::json!({
                            "message_event_id": message.event_id,
                            "request_event_id": requested.event_id,
                            "provider": response.provider,
                            "model": response.model,
                            "text": response.content,
                            "usage": response.usage
                        }),
                    )
                    .caused_by(&requested.event_id);
                    store.append(response_event.clone())?;
                    return Ok(vec![InteractionOutput::Line(format!(
                        "sinomo: {}",
                        response_event
                            .payload
                            .get("text")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default()
                    ))]);
                }
                Err(error) => {
                    let failure = interaction_event(
                        session,
                        "interaction.model.failed",
                        serde_json::json!({
                            "message_event_id": message.event_id,
                            "request_event_id": requested.event_id,
                            "provider": request.provider,
                            "model": request.model,
                            "error": error.to_string()
                        }),
                    )
                    .caused_by(&requested.event_id);
                    store.append(failure)?;
                    return Err(CliError::LanguageModel(error));
                }
            }
        };

        if matches!(args.first().map(String::as_str), Some("quit" | "exit")) {
            let quit = interaction_event(
                session,
                "interaction.session.ended",
                serde_json::json!({
                    "input_event_id": event.event_id,
                    "reason": "user-requested"
                }),
            )
            .caused_by(&event.event_id);
            store.append(quit)?;
            return Ok(vec![
                InteractionOutput::Line("ending session".to_string()),
                InteractionOutput::Exit,
            ]);
        }

        let command = interaction_event(
            session,
            "interaction.command.requested",
            serde_json::json!({
                "input_event_id": event.event_id,
                "args": args,
                "command": args.join(" ")
            }),
        )
        .caused_by(&event.event_id);
        store.append(command.clone())?;

        let result = run_cli_command(&args);
        let (kind, status, output) = match result {
            Ok(()) => (
                "interaction.command.completed",
                "completed",
                format!("completed command: /{}", args.join(" ")),
            ),
            Err(error) => (
                "interaction.command.failed",
                "failed",
                format!("command failed: {error}"),
            ),
        };
        let outcome = interaction_event(
            session,
            kind,
            serde_json::json!({
                "command_event_id": command.event_id,
                "status": status,
                "args": args
            }),
        )
        .caused_by(&command.event_id);
        store.append(outcome)?;

        Ok(vec![InteractionOutput::Line(output)])
    }
}

#[derive(Clone, Debug)]
pub struct CliConversationModel {
    provider: String,
    model: String,
    data_dir: SinomoDataDir,
}

impl CliConversationModel {
    pub fn from_defaults() -> Result<Self, CliError> {
        let data_dir = SinomoDataDir::from_env_or_home()?;
        data_dir.ensure_root_layout()?;
        let paths = EvolutionPaths::new(&data_dir);
        paths.ensure()?;
        let config = read_config(&paths).ok();
        let provider = config
            .as_ref()
            .and_then(|config| config.default_provider.clone())
            .unwrap_or_else(|| GitHubCopilotProvider::PROVIDER_ID.to_string());
        let provider = normalize_model_provider(&provider).ok_or_else(|| CliError::Parse {
            detail: format!("unknown provider `{provider}`"),
        })?;
        let model = config
            .as_ref()
            .and_then(|config| config.default_model.clone())
            .unwrap_or_else(|| PROVIDER_DEFAULT_MODEL.to_string());
        Ok(Self {
            provider,
            model,
            data_dir,
        })
    }
}

impl InteractionModel for CliConversationModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let request = LanguageModelRequest {
            provider: self.provider.clone(),
            model: self.model.clone(),
            ..request.clone()
        };
        match self.provider.as_str() {
            GitHubCopilotProvider::PROVIDER_ID => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(GitHubCopilotProvider::PROVIDER_ID);
                let provider = GitHubCopilotProvider::new(GitHubCopilotConfig {
                    client_id: env::var("SINOMO_COPILOT_CLIENT_ID")
                        .unwrap_or_else(|_| GitHubCopilotConfig::default().client_id),
                    github_host: env::var("SINOMO_COPILOT_GITHUB_HOST")
                        .unwrap_or_else(|_| GitHubCopilotConfig::default().github_host),
                    api_base_url: env::var("SINOMO_COPILOT_API_BASE_URL")
                        .unwrap_or_else(|_| GitHubCopilotConfig::default().api_base_url),
                    default_model: model_or_default(
                        &self.model,
                        GitHubCopilotConfig::default().default_model,
                    ),
                    token_store_path: Some(credentials_path),
                });
                LanguageModelProvider::complete(&provider, &request)
            }
            GitHubCopilotCliProvider::PROVIDER_ID => {
                let provider = GitHubCopilotCliProvider::new(GitHubCopilotCliConfig {
                    model: model_or_default(&self.model, GitHubCopilotCliConfig::default().model),
                    ..GitHubCopilotCliConfig::default()
                });
                LanguageModelProvider::complete(&provider, &request)
            }
            CodexCliProvider::PROVIDER_ID => {
                let provider = CodexCliProvider::new(CodexCliConfig {
                    model: model_or_default(&self.model, CODEX_CLI_DEFAULT_MODEL.to_string()),
                    working_dir: env::current_dir().ok(),
                    ..CodexCliConfig::default()
                });
                LanguageModelProvider::complete(&provider, &request)
            }
            ClaudeCodeProvider::PROVIDER_ID => {
                let provider = ClaudeCodeProvider::new(ClaudeCodeConfig {
                    model: model_or_default(&self.model, CLAUDE_CODE_DEFAULT_MODEL.to_string()),
                    working_dir: env::current_dir().ok(),
                    ..ClaudeCodeConfig::default()
                });
                LanguageModelProvider::complete(&provider, &request)
            }
            AnthropicProvider::PROVIDER_ID => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(AnthropicProvider::PROVIDER_ID);
                let provider = AnthropicProvider::new(AnthropicConfig {
                    default_model: model_or_default(
                        &self.model,
                        AnthropicConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                    ..AnthropicConfig::default()
                });
                LanguageModelProvider::complete(&provider, &request)
            }
            OpenAiProvider::PROVIDER_ID => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(OpenAiProvider::PROVIDER_ID);
                let provider = OpenAiProvider::new(OpenAiConfig {
                    default_model: model_or_default(
                        &self.model,
                        OpenAiConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                    ..OpenAiConfig::default()
                });
                LanguageModelProvider::complete(&provider, &request)
            }
            GeminiProvider::PROVIDER_ID => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(GeminiProvider::PROVIDER_ID);
                let provider = GeminiProvider::new(GeminiConfig {
                    default_model: model_or_default(
                        &self.model,
                        GeminiConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                    ..GeminiConfig::default()
                });
                LanguageModelProvider::complete(&provider, &request)
            }
            _ => Err(LanguageModelError::ProviderInvocationFailed {
                provider: self.provider.clone(),
                model: self.model.clone(),
                detail: "unknown interaction model provider".to_string(),
            }),
        }
    }
}

trait EventEnvelopeExt {
    fn caused_by(self, event_id: &EventId) -> Self;
}

impl EventEnvelopeExt for EventEnvelope {
    fn caused_by(mut self, event_id: &EventId) -> Self {
        self.links.caused_by.push(event_id.clone());
        self
    }
}

fn user_input_event(session: &InteractionSession, text: &str) -> EventEnvelope {
    let input_kind = if text.trim_start().starts_with('/') {
        InteractionInputKind::Command
    } else {
        InteractionInputKind::Message
    };
    interaction_event(
        session,
        "interaction.input.received",
        serde_json::json!({
            "text": text,
            "input_kind": input_kind,
            "surface": session.surface,
        }),
    )
}

fn interaction_event(
    session: &InteractionSession,
    kind: &str,
    payload: serde_json::Value,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        session.node_id.clone(),
        Source::new("interaction.surface").expect("valid source"),
        EventKind::new(kind).expect("valid event kind"),
        Salience::new(0.62).expect("valid salience"),
        Confidence::new(0.98).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Interactive,
            PrivacyClass::Derived,
        ),
    )
    .with_session(session.session_id.clone())
    .with_payload(payload)
}

fn conversation_model_request<S: EventStore>(
    store: &S,
    session: &InteractionSession,
    message_event_id: &EventId,
    text: &str,
    provider: &str,
    model: &str,
) -> Result<LanguageModelRequest, CliError> {
    let recent = store.replay(ReplayQuery {
        session_id: Some(session.session_id.clone()),
        kind_prefix: Some("interaction.model.responded".to_string()),
        origin_node_id: None,
        after_sequence: None,
        start_time: None,
        end_time: None,
        direction: ReplayDirection::Reverse,
        limit: Some(6),
    })?;
    let mut recent_turns: Vec<String> = recent
        .into_iter()
        .rev()
        .filter_map(|stored| {
            stored
                .event
                .payload
                .get("text")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect();
    if recent_turns.is_empty() {
        recent_turns.push("No prior assistant turns in this session.".to_string());
    }

    Ok(LanguageModelRequest {
        provider: provider.to_string(),
        model: model.to_string(),
        messages: vec![
            LanguageModelMessage {
                role: LanguageModelRole::System,
                content: INTERACTION_SYSTEM_PROMPT.to_string(),
            },
            LanguageModelMessage {
                role: LanguageModelRole::User,
                content: serde_json::to_string_pretty(&serde_json::json!({
                    "session_id": session.session_id,
                    "surface": session.surface,
                    "input_event_id": message_event_id,
                    "recent_assistant_turns": recent_turns,
                    "user_input": text
                }))
                .expect("interaction request serializes"),
            },
        ],
        temperature: Some(0.4),
        max_output_tokens: Some(800),
        response_format: LanguageModelResponseFormat::Text,
        metadata: serde_json::json!({
            "loop": "interactive",
            "node_id": session.node_id,
            "session_id": session.session_id,
            "input_event_id": message_event_id
        }),
    })
}

const INTERACTION_SYSTEM_PROMPT: &str = r#"You are Sinomo's command-line conversation loop.
Respond directly to the user in plain text.
Use the supplied event ids and session metadata as context, but do not expose internal event JSON unless asked.
Slash commands are handled by Sinomo outside the model; ordinary user input reaches you only after being recorded as an interaction event."#;

pub fn parse_slash_command(input: &str) -> Result<Option<Vec<String>>, CliError> {
    let trimmed = input.trim();
    let Some(command) = trimmed.strip_prefix('/') else {
        return Ok(None);
    };
    let args = split_command_line(command)?;
    if args.is_empty() {
        return Err(CliError::Usage);
    }
    Ok(Some(args))
}

fn split_command_line(input: &str) -> Result<Vec<String>, CliError> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut chars = input.chars().peekable();
    let mut quote: Option<char> = None;
    let mut arg_started = false;

    while let Some(character) = chars.next() {
        match (quote, character) {
            (Some(active_quote), value) if value == active_quote => quote = None,
            (Some(_), '\\') => {
                arg_started = true;
                if let Some(escaped) = chars.next() {
                    current.push(escaped);
                } else {
                    current.push('\\');
                }
            }
            (Some(_), value) => {
                arg_started = true;
                current.push(value);
            }
            (None, '"' | '\'') => {
                arg_started = true;
                quote = Some(character);
            }
            (None, value) if value.is_whitespace() => {
                if arg_started {
                    args.push(std::mem::take(&mut current));
                    arg_started = false;
                }
            }
            (None, '\\') => {
                arg_started = true;
                if let Some(escaped) = chars.next() {
                    current.push(escaped);
                } else {
                    current.push('\\');
                }
            }
            (None, value) => {
                arg_started = true;
                current.push(value);
            }
        }
    }

    if let Some(quote) = quote {
        return Err(CliError::Parse {
            detail: format!("unterminated {quote} quote"),
        });
    }

    if arg_started {
        args.push(current);
    }

    Ok(args)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct EvolutionConfig {
    repo: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default_model: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct StoredEvolutionJob {
    job: EvolutionJob,
    candidate_workspace: CandidateWorkspace,
    bundle: CandidateBundleManifest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    verification_outcome: Option<EvaluationOutcome>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    changed_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    decision: Option<CandidateDecision>,
}

#[derive(Clone, Debug)]
struct EvolutionPaths {
    root: PathBuf,
    jobs: PathBuf,
    logs: PathBuf,
    config: PathBuf,
}

impl EvolutionPaths {
    fn new(data_dir: &SinomoDataDir) -> Self {
        let root = data_dir.root().join("evolution");
        Self {
            jobs: root.join("jobs"),
            logs: root.join("logs"),
            config: root.join("config.json"),
            root,
        }
    }

    fn ensure(&self) -> Result<(), CliError> {
        for path in [&self.root, &self.jobs, &self.logs] {
            fs::create_dir_all(path).map_err(|source| CliError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        Ok(())
    }

    fn job_dir(&self, job_id: &EvolutionJobId) -> PathBuf {
        self.jobs.join(job_id.as_str())
    }

    fn job_path(&self, job_id: &EvolutionJobId) -> PathBuf {
        self.job_dir(job_id).join("record.json")
    }

    fn legacy_job_path(&self, job_id: &EvolutionJobId) -> PathBuf {
        self.jobs.join(format!("{job_id}.json"))
    }
}

fn run_evolve_command(args: &[String]) -> Result<(), CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "propose" => evolve_propose(rest),
        Some((command, rest)) if command == "approve" => {
            evolve_decide(rest, CandidateDecisionOutcome::Approved)
        }
        Some((command, rest)) if command == "reject" => {
            evolve_decide(rest, CandidateDecisionOutcome::Rejected)
        }
        Some((command, rest)) if command == "restore" => {
            evolve_decide(rest, CandidateDecisionOutcome::Restored)
        }
        Some((command, rest)) if command == "status" => evolve_status(rest),
        Some((command, rest)) if command == "show" => evolve_show(rest),
        Some((command, rest)) if command == "verify" => evolve_verify(rest),
        Some((command, rest)) if command == "packet" => evolve_show_packet(rest),
        Some((command, rest)) if command == "cleanup" => evolve_cleanup(rest),
        Some((command, rest)) if command == "job" => evolve_job(rest),
        Some((command, rest)) if command == "models" => evolve_models(rest),
        _ => {
            print_usage();
            Err(CliError::Usage)
        }
    }
}

fn evolve_propose(args: &[String]) -> Result<(), CliError> {
    let options = parse_propose_options(args)?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    let config = read_config(&paths).ok();
    let repo = options
        .repo
        .or_else(|| config.as_ref().map(|config| PathBuf::from(&config.repo)))
        .unwrap_or(env::current_dir().map_err(|source| CliError::Io {
            path: ".".to_string(),
            detail: source.to_string(),
        })?);
    let provider = options
        .provider
        .or_else(|| {
            config
                .as_ref()
                .and_then(|config| config.default_provider.clone())
        })
        .unwrap_or_else(|| GitHubCopilotProvider::PROVIDER_ID.to_string());
    let model = options.model.or_else(|| {
        config
            .as_ref()
            .and_then(|config| config.default_model.clone())
    });
    let verification_commands = if options.verify.is_empty() {
        Vec::new()
    } else {
        options
            .verify
            .iter()
            .enumerate()
            .map(|(index, command)| VerificationCommand {
                name: format!("verify-{}", index + 1),
                command: command.clone(),
                expected_exit_code: 0,
                gate: EvaluationGate::StaticChecks,
            })
            .collect()
    };
    let request = EvolutionRequest {
        objective: options.objective,
        target_ring: options.target_ring,
        intended_paths: options.allow,
        verification_commands,
        model_provider: Some(provider.clone()),
        model: model.clone(),
        dry_run: options.dry_run,
    };

    let orchestrator = EvolutionOrchestrator::new(
        GitWorkspaceAdapter::new(&repo, &paths.jobs),
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(&paths.jobs),
    );
    let model_runner = CliMutationModel {
        provider,
        model: model.unwrap_or_else(|| PROVIDER_DEFAULT_MODEL.to_string()),
        data_dir: data_dir.clone(),
    };
    let report = if request.dry_run {
        orchestrator.propose(request, None)?
    } else {
        orchestrator.propose(request, Some(&model_runner))?
    };

    write_report_record(&paths, &report)?;
    println_report(&report);
    Ok(())
}

fn evolve_decide(args: &[String], outcome: CandidateDecisionOutcome) -> Result<(), CliError> {
    let (paths, mut record) = resolve_job_from_args(args, true)?;
    ensure_awaiting_approval(&record)?;
    let note = flag_value(args, "--note").unwrap_or_default();
    let decision = CandidateDecision::new(
        &record.job,
        outcome,
        "cli-supervisor",
        record.candidate_workspace.path.display().to_string(),
        record.bundle.bundle_path.clone(),
        record.verification_outcome.clone(),
        record.changed_files.clone(),
        note,
    );
    let decision_path = PathBuf::from(&record.bundle.bundle_path).join("approval-decision.json");
    write_json(&decision_path, &decision)?;
    record.job.status = decision.resulting_status.clone();
    record.bundle.decision_path = Some(decision_path.display().to_string());
    record.decision = Some(decision);
    write_bundle_manifest(&record)?;
    if let Some(job_path) = &record.bundle.job_path {
        write_json(Path::new(job_path), &record.job)?;
    }
    write_stored_job(&paths, &record)?;
    println!("job: {}", record.job.job_id);
    println!("status: {}", job_status_label(&record.job.status));
    println!("worktree: {}", record.candidate_workspace.path.display());
    println!("bundle: {}", record.bundle.bundle_path);
    println!("decision: {}", decision_path.display());
    Ok(())
}

fn evolve_status(args: &[String]) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    if let Some(job_id) = job_flag(args)? {
        let job_path = paths.job_path(&job_id);
        let record_path = if job_path.exists() {
            job_path
        } else {
            paths.legacy_job_path(&job_id)
        };
        let record = read_stored_job(&record_path)?;
        print_job_status(&record);
        return Ok(());
    }

    let mut records = list_job_records(&paths)?;
    records.sort_by(|left, right| left.job.job_id.cmp(&right.job.job_id));
    if records.is_empty() {
        println!("no evolution jobs");
        return Ok(());
    }
    for record in &records {
        print_job_status(record);
    }
    Ok(())
}

fn evolve_show(args: &[String]) -> Result<(), CliError> {
    let (_paths, record) = resolve_job_from_args(args, false)?;
    let artifact = if has_flag(args, "--diff") {
        record.bundle.diff_path.clone()
    } else if has_flag(args, "--scorecard") {
        record.bundle.scorecard_path.clone()
    } else if has_flag(args, "--decision") {
        record.bundle.decision_path.clone()
    } else if has_flag(args, "--packet") {
        Some(record.bundle.mutation_packet_path.clone())
    } else {
        record.bundle.diff_path.clone()
    }
    .ok_or_else(|| CliError::MissingArtifact {
        artifact: "requested artifact".to_string(),
    })?;
    let content = fs::read_to_string(&artifact).map_err(|source| CliError::Io {
        path: artifact.clone(),
        detail: source.to_string(),
    })?;
    println!("{content}");
    Ok(())
}

fn evolve_show_packet(args: &[String]) -> Result<(), CliError> {
    let mut show_args = args.to_vec();
    show_args.push("--packet".to_string());
    evolve_show(&show_args)
}

fn evolve_verify(args: &[String]) -> Result<(), CliError> {
    let (paths, mut record) = resolve_job_from_args(args, false)?;
    let harness = ExecutingEvalHarness::new(LocalCommandRunner::new());
    let (scorecard, results) = harness
        .run_verification_plan(
            record.job.verification.clone(),
            &record.candidate_workspace.path,
        )
        .map_err(CliError::Eval)?;
    let scorecard_path = PathBuf::from(&record.bundle.bundle_path).join("scorecard.json");
    write_json(&scorecard_path, &scorecard)?;
    for result in &results {
        let safe_name = safe_file_stem(&result.name);
        let path = PathBuf::from(&record.bundle.bundle_path)
            .join("command-logs")
            .join(format!("{safe_name}.txt"));
        fs::write(
            &path,
            format!(
                "name: {}\ncommand: {}\nexpected_exit_code: {}\nexit_code: {}\nduration_ms: {}\n\nstdout:\n{}\n\nstderr:\n{}\n",
                result.name,
                result.command,
                result.expected_exit_code,
                result.exit_code,
                result.duration_ms,
                result.stdout,
                result.stderr
            ),
        )
        .map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    record.verification_outcome = Some(scorecard.outcome.clone());
    record.bundle.scorecard_path = Some(scorecard_path.display().to_string());
    write_stored_job(&paths, &record)?;
    println!("job: {}", record.job.job_id);
    println!("verification: {}", outcome_label(&scorecard.outcome));
    println!("scorecard: {}", scorecard_path.display());
    Ok(())
}

fn evolve_cleanup(args: &[String]) -> Result<(), CliError> {
    let (paths, record) = resolve_job_from_args(args, false)?;
    let path = &record.candidate_workspace.path;
    if path.exists() {
        let output = std::process::Command::new("git")
            .args(["worktree", "remove", "--force"])
            .arg(path)
            .current_dir(&record.job.edit_scope.workspace_root)
            .output();
        match output {
            Ok(output) if output.status.success() => {}
            Ok(output) => {
                fs::remove_dir_all(path).map_err(|source| CliError::Io {
                    path: path.display().to_string(),
                    detail: format!(
                        "git worktree remove failed: {}; remove_dir_all failed: {}",
                        String::from_utf8_lossy(&output.stderr).trim(),
                        source
                    ),
                })?;
            }
            Err(error) => {
                fs::remove_dir_all(path).map_err(|source| CliError::Io {
                    path: path.display().to_string(),
                    detail: format!(
                        "git worktree remove could not start: {}; remove_dir_all failed: {}",
                        error, source
                    ),
                })?;
            }
        }
    }
    let job_dir = paths.job_dir(&record.job.job_id);
    if job_dir.exists() {
        fs::remove_dir_all(&job_dir).map_err(|source| CliError::Io {
            path: job_dir.display().to_string(),
            detail: source.to_string(),
        })?;
    } else {
        let legacy_record = paths.legacy_job_path(&record.job.job_id);
        if legacy_record.exists() {
            fs::remove_file(&legacy_record).map_err(|source| CliError::Io {
                path: legacy_record.display().to_string(),
                detail: source.to_string(),
            })?;
        }
    }
    let bundle_path = PathBuf::from(&record.bundle.bundle_path);
    if bundle_path.exists() && bundle_path != job_dir {
        fs::remove_dir_all(&bundle_path).map_err(|source| CliError::Io {
            path: bundle_path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    println!("removed job: {}", record.job.job_id);
    println!("removed worktree: {}", path.display());
    Ok(())
}

fn evolve_job(args: &[String]) -> Result<(), CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "create" => {
            let mut propose_args = rest.to_vec();
            propose_args.push("--dry-run".to_string());
            evolve_propose(&propose_args)
        }
        _ => Err(CliError::Usage),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ModelDefaultOptions {
    provider: String,
    model: String,
    repo: Option<PathBuf>,
}

fn evolve_models(args: &[String]) -> Result<(), CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "list" => evolve_models_list(rest),
        Some((command, rest)) if command == "default" => evolve_models_default(rest),
        _ => Err(CliError::Usage),
    }
}

fn evolve_models_list(args: &[String]) -> Result<(), CliError> {
    let provider = parse_models_list_provider(args)?;
    let providers = match provider {
        Some(provider) => {
            vec![
                normalize_model_provider(&provider).ok_or_else(|| CliError::Parse {
                    detail: format!("unknown provider `{provider}`"),
                })?,
            ]
        }
        None => default_model_provider_ids()
            .iter()
            .map(|value| value.to_string())
            .collect(),
    };
    for provider in providers {
        println!("{provider}:");
        for model in provider_model_ids(&provider)? {
            println!("  {model}");
        }
    }
    Ok(())
}

fn parse_models_list_provider(args: &[String]) -> Result<Option<String>, CliError> {
    match args {
        [] => Ok(None),
        [flag, provider] if flag == "--provider" => Ok(Some(provider.clone())),
        _ => Err(CliError::Usage),
    }
}

fn evolve_models_default(args: &[String]) -> Result<(), CliError> {
    let options = parse_model_default_options(args)?;
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    let existing = read_config(&paths).ok();
    let repo = options
        .repo
        .or_else(|| existing.as_ref().map(|config| PathBuf::from(&config.repo)))
        .unwrap_or(env::current_dir().map_err(|source| CliError::Io {
            path: ".".to_string(),
            detail: source.to_string(),
        })?);
    let config = EvolutionConfig {
        repo: repo.display().to_string(),
        default_provider: Some(options.provider),
        default_model: Some(options.model),
    };
    write_json(&paths.config, &config)?;
    println!(
        "default provider: {}",
        config.default_provider.as_deref().unwrap_or("")
    );
    println!(
        "default model: {}",
        config.default_model.as_deref().unwrap_or("")
    );
    println!("config: {}", paths.config.display());
    Ok(())
}

fn parse_model_default_options(args: &[String]) -> Result<ModelDefaultOptions, CliError> {
    let provider = flag_value(args, "--provider").ok_or(CliError::Usage)?;
    let provider = normalize_model_provider(&provider).ok_or_else(|| CliError::Parse {
        detail: format!("unknown provider `{provider}`"),
    })?;
    let model = flag_value(args, "--model").ok_or(CliError::Usage)?;
    let repo = flag_value(args, "--repo").map(PathBuf::from);
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--provider" | "--model" | "--repo" => index += 2,
            _ => return Err(CliError::Usage),
        }
    }
    Ok(ModelDefaultOptions {
        provider,
        model,
        repo,
    })
}

fn default_model_provider_ids() -> &'static [&'static str] {
    &[
        GitHubCopilotProvider::PROVIDER_ID,
        GitHubCopilotCliProvider::PROVIDER_ID,
        CodexCliProvider::PROVIDER_ID,
        ClaudeCodeProvider::PROVIDER_ID,
        AnthropicProvider::PROVIDER_ID,
        OpenAiProvider::PROVIDER_ID,
        GeminiProvider::PROVIDER_ID,
    ]
}

fn normalize_model_provider(provider: &str) -> Option<String> {
    match provider {
        GitHubCopilotProvider::PROVIDER_ID => Some(GitHubCopilotProvider::PROVIDER_ID.to_string()),
        GitHubCopilotCliProvider::PROVIDER_ID => {
            Some(GitHubCopilotCliProvider::PROVIDER_ID.to_string())
        }
        CodexCliProvider::PROVIDER_ID | "openai-codex-cli" => {
            Some(CodexCliProvider::PROVIDER_ID.to_string())
        }
        ClaudeCodeProvider::PROVIDER_ID | "anthropic-claude-code" => {
            Some(ClaudeCodeProvider::PROVIDER_ID.to_string())
        }
        AnthropicProvider::PROVIDER_ID | "claude" => {
            Some(AnthropicProvider::PROVIDER_ID.to_string())
        }
        OpenAiProvider::PROVIDER_ID | "openai-api" => Some(OpenAiProvider::PROVIDER_ID.to_string()),
        GeminiProvider::PROVIDER_ID | "google-gemini" => {
            Some(GeminiProvider::PROVIDER_ID.to_string())
        }
        _ => None,
    }
}

fn provider_model_ids(provider: &str) -> Result<Vec<String>, CliError> {
    match provider {
        GitHubCopilotProvider::PROVIDER_ID => {
            let (provider, _) = github_copilot_provider()?;
            provider.list_models().map_err(CliError::LanguageModel)
        }
        GitHubCopilotCliProvider::PROVIDER_ID => Ok(vec![GitHubCopilotCliConfig::default().model]),
        CodexCliProvider::PROVIDER_ID => Ok(vec![CODEX_CLI_DEFAULT_MODEL.to_string()]),
        ClaudeCodeProvider::PROVIDER_ID => Ok(vec![CLAUDE_CODE_DEFAULT_MODEL.to_string()]),
        AnthropicProvider::PROVIDER_ID => {
            Ok(vec![env::var("SINOMO_ANTHROPIC_MODEL").unwrap_or_else(
                |_| AnthropicConfig::default().default_model,
            )])
        }
        OpenAiProvider::PROVIDER_ID => Ok(vec![
            env::var("SINOMO_OPENAI_MODEL")
                .unwrap_or_else(|_| OpenAiConfig::default().default_model),
        ]),
        GeminiProvider::PROVIDER_ID => Ok(vec![
            env::var("SINOMO_GEMINI_MODEL")
                .unwrap_or_else(|_| GeminiConfig::default().default_model),
        ]),
        _ => Err(CliError::Parse {
            detail: format!("unknown provider `{provider}`"),
        }),
    }
}

#[derive(Clone, Debug)]
struct ProposeOptions {
    objective: String,
    provider: Option<String>,
    model: Option<String>,
    repo: Option<PathBuf>,
    allow: Vec<String>,
    verify: Vec<String>,
    target_ring: TrustRing,
    dry_run: bool,
}

fn parse_propose_options(args: &[String]) -> Result<ProposeOptions, CliError> {
    let mut objective = None;
    let mut provider = None;
    let mut model = None;
    let mut repo = None;
    let mut allow = Vec::new();
    let mut verify = Vec::new();
    let mut target_ring = TrustRing::R1TrustedRuntime;
    let mut dry_run = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--provider" => {
                index += 1;
                provider = args.get(index).cloned();
            }
            "--model" => {
                index += 1;
                model = args.get(index).cloned();
            }
            "--repo" => {
                index += 1;
                repo = args.get(index).map(PathBuf::from);
            }
            "--allow" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    allow.push(value.clone());
                }
            }
            "--verify" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    verify.push(value.clone());
                }
            }
            "--target-ring" => {
                index += 1;
                if let Some(value) = args.get(index) {
                    target_ring = parse_trust_ring(value)?;
                }
            }
            "--dry-run" | "--no-model" => {
                dry_run = true;
            }
            value if value.starts_with("--") => return Err(CliError::Usage),
            value => {
                objective = Some(value.to_string());
            }
        }
        index += 1;
    }
    Ok(ProposeOptions {
        objective: objective.ok_or(CliError::Usage)?,
        provider,
        model,
        repo,
        allow,
        verify,
        target_ring,
        dry_run,
    })
}

#[derive(Clone, Debug)]
struct CliMutationModel {
    provider: String,
    model: String,
    data_dir: SinomoDataDir,
}

impl MutationModel for CliMutationModel {
    fn mutate(
        &self,
        packet: &MutationPacket,
        workspace: &CandidateWorkspace,
    ) -> Result<sinomo_evolution::ModelMutationResult, MutationModelError> {
        match self.provider.as_str() {
            GitHubCopilotProvider::PROVIDER_ID => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(GitHubCopilotProvider::PROVIDER_ID);
                let provider = GitHubCopilotProvider::new(GitHubCopilotConfig {
                    client_id: env::var("SINOMO_COPILOT_CLIENT_ID")
                        .unwrap_or_else(|_| GitHubCopilotConfig::default().client_id),
                    github_host: env::var("SINOMO_COPILOT_GITHUB_HOST")
                        .unwrap_or_else(|_| GitHubCopilotConfig::default().github_host),
                    api_base_url: env::var("SINOMO_COPILOT_API_BASE_URL")
                        .unwrap_or_else(|_| GitHubCopilotConfig::default().api_base_url),
                    default_model: model_or_default(
                        &self.model,
                        GitHubCopilotConfig::default().default_model,
                    ),
                    token_store_path: Some(credentials_path),
                });
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            GitHubCopilotCliProvider::PROVIDER_ID => {
                let provider = GitHubCopilotCliProvider::new(GitHubCopilotCliConfig {
                    model: model_or_default(&self.model, GitHubCopilotCliConfig::default().model),
                    ..GitHubCopilotCliConfig::default()
                });
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            CodexCliProvider::PROVIDER_ID | "openai-codex-cli" => {
                let provider = CodexCliProvider::new(CodexCliConfig {
                    model: model_or_default(&self.model, CODEX_CLI_DEFAULT_MODEL.to_string()),
                    working_dir: Some(workspace.path.clone()),
                    ..CodexCliConfig::default()
                });
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            ClaudeCodeProvider::PROVIDER_ID | "anthropic-claude-code" => {
                let provider = ClaudeCodeProvider::new(ClaudeCodeConfig {
                    model: model_or_default(&self.model, CLAUDE_CODE_DEFAULT_MODEL.to_string()),
                    working_dir: Some(workspace.path.clone()),
                    ..ClaudeCodeConfig::default()
                });
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            AnthropicProvider::PROVIDER_ID | "claude" => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(AnthropicProvider::PROVIDER_ID);
                let provider = AnthropicProvider::new(AnthropicConfig {
                    default_model: model_or_default(
                        &self.model,
                        AnthropicConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                    ..AnthropicConfig::default()
                });
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            OpenAiProvider::PROVIDER_ID | "openai-api" => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(OpenAiProvider::PROVIDER_ID);
                let provider = OpenAiProvider::new(OpenAiConfig {
                    default_model: model_or_default(
                        &self.model,
                        OpenAiConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                    ..OpenAiConfig::default()
                });
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            GeminiProvider::PROVIDER_ID | "google-gemini" => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(GeminiProvider::PROVIDER_ID);
                let provider = GeminiProvider::new(GeminiConfig {
                    default_model: model_or_default(
                        &self.model,
                        GeminiConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
                    ..GeminiConfig::default()
                });
                LanguageModelMutationBridge::new(provider, self.model.clone())
                    .mutate(packet, workspace)
            }
            _ => Err(MutationModelError::LanguageModel(
                LanguageModelError::ProviderInvocationFailed {
                    provider: self.provider.clone(),
                    model: self.model.clone(),
                    detail: "unknown evolution model provider".to_string(),
                },
            )),
        }
    }
}

fn model_or_default(model: &str, default_model: String) -> String {
    if model == PROVIDER_DEFAULT_MODEL {
        default_model
    } else {
        model.to_string()
    }
}

fn write_report_record(paths: &EvolutionPaths, report: &EvolutionReport) -> Result<(), CliError> {
    let record = StoredEvolutionJob {
        job: report.job.clone(),
        candidate_workspace: report.candidate_workspace.clone(),
        bundle: report.bundle.clone(),
        verification_outcome: report
            .scorecard
            .as_ref()
            .map(|scorecard| scorecard.outcome.clone()),
        changed_files: report.changed_files.clone(),
        decision: None,
    };
    write_stored_job(paths, &record)?;
    if let Some(job_path) = &record.bundle.job_path {
        write_json(Path::new(job_path), &record.job)?;
    }
    Ok(())
}

fn write_bundle_manifest(record: &StoredEvolutionJob) -> Result<(), CliError> {
    write_json(
        &PathBuf::from(&record.bundle.bundle_path).join("manifest.json"),
        &record.bundle,
    )
}

fn write_stored_job(paths: &EvolutionPaths, record: &StoredEvolutionJob) -> Result<(), CliError> {
    write_json(&paths.job_path(&record.job.job_id), record)
}

fn read_stored_job(path: &Path) -> Result<StoredEvolutionJob, CliError> {
    let content = fs::read_to_string(path).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| CliError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn list_job_records(paths: &EvolutionPaths) -> Result<Vec<StoredEvolutionJob>, CliError> {
    if !paths.jobs.exists() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    for entry in fs::read_dir(&paths.jobs).map_err(|source| CliError::Io {
        path: paths.jobs.display().to_string(),
        detail: source.to_string(),
    })? {
        let entry = entry.map_err(|source| CliError::Io {
            path: paths.jobs.display().to_string(),
            detail: source.to_string(),
        })?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            let record_path = entry_path.join("record.json");
            if record_path.exists() {
                records.push(read_stored_job(&record_path)?);
            }
        } else if entry_path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            records.push(read_stored_job(&entry_path)?);
        }
    }
    Ok(records)
}

fn resolve_job_from_args(
    args: &[String],
    waiting_only: bool,
) -> Result<(EvolutionPaths, StoredEvolutionJob), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure()?;
    if let Some(job_id) = job_flag(args)? {
        let job_path = paths.job_path(&job_id);
        let record_path = if job_path.exists() {
            job_path
        } else {
            paths.legacy_job_path(&job_id)
        };
        let record = read_stored_job(&record_path)?;
        return Ok((paths, record));
    }

    let records = list_job_records(&paths)?;
    let candidates: Vec<_> = records
        .into_iter()
        .filter(|record| {
            if waiting_only {
                record.job.status == EvolutionJobStatus::AwaitingApproval
            } else {
                !is_terminal_status(&record.job.status)
            }
        })
        .collect();
    match candidates.as_slice() {
        [record] => Ok((paths, record.clone())),
        [] => Err(CliError::NoActiveJob),
        _ => Err(CliError::AmbiguousJob {
            job_ids: candidates
                .iter()
                .map(|record| record.job.job_id.to_string())
                .collect(),
        }),
    }
}

fn ensure_awaiting_approval(record: &StoredEvolutionJob) -> Result<(), CliError> {
    if record.job.status == EvolutionJobStatus::AwaitingApproval {
        return Ok(());
    }
    Err(CliError::InvalidJobState {
        job_id: record.job.job_id.to_string(),
        status: job_status_label(&record.job.status).to_string(),
        expected: "awaiting-approval".to_string(),
    })
}

fn read_config(paths: &EvolutionPaths) -> Result<EvolutionConfig, CliError> {
    let content = fs::read_to_string(&paths.config).map_err(|source| CliError::Io {
        path: paths.config.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| CliError::Json {
        path: paths.config.display().to_string(),
        detail: source.to_string(),
    })
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), CliError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let content = serde_json::to_string_pretty(value).map_err(|source| CliError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::write(path, content).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn println_report(report: &EvolutionReport) {
    let verification = report
        .scorecard
        .as_ref()
        .map(|scorecard| outcome_label(&scorecard.outcome))
        .unwrap_or("not-run");
    println!("job: {}", report.job.job_id);
    println!("status: {}", job_status_label(&report.job.status));
    println!("worktree: {}", report.candidate_workspace.path.display());
    println!("bundle: {}", report.bundle.bundle_path);
    println!("verification: {verification}");
}

fn print_job_status(record: &StoredEvolutionJob) {
    println!("job: {}", record.job.job_id);
    println!("status: {}", job_status_label(&record.job.status));
    println!("objective: {}", record.job.objective);
    println!("worktree: {}", record.candidate_workspace.path.display());
    println!("bundle: {}", record.bundle.bundle_path);
    println!(
        "verification: {}",
        record
            .verification_outcome
            .as_ref()
            .map(outcome_label)
            .unwrap_or("unknown")
    );
    if let Some(decision) = &record.decision {
        println!("decision: {}", decision_outcome_label(&decision.outcome));
    }
    println!();
}

fn job_flag(args: &[String]) -> Result<Option<EvolutionJobId>, CliError> {
    flag_value(args, "--job")
        .map(EvolutionJobId::new)
        .transpose()
        .map_err(|source| CliError::Parse {
            detail: source.to_string(),
        })
}

fn flag_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == flag)
        .map(|pair| pair[1].clone())
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

fn parse_trust_ring(value: &str) -> Result<TrustRing, CliError> {
    match value {
        "r0" | "r0-continuity-root" => Ok(TrustRing::R0ContinuityRoot),
        "r1" | "r1-trusted-runtime" => Ok(TrustRing::R1TrustedRuntime),
        "r2" | "r2-trusted-service" => Ok(TrustRing::R2TrustedService),
        "r3" | "r3-sandboxed-module" => Ok(TrustRing::R3SandboxedModule),
        "r4" | "r4-disposable-workspace" => Ok(TrustRing::R4DisposableWorkspace),
        "r5" | "r5-model-provider" => Ok(TrustRing::R5ModelProvider),
        _ => Err(CliError::Parse {
            detail: format!("unknown trust ring `{value}`"),
        }),
    }
}

fn is_terminal_status(status: &EvolutionJobStatus) -> bool {
    matches!(
        status,
        EvolutionJobStatus::Approved
            | EvolutionJobStatus::Rejected
            | EvolutionJobStatus::Restored
            | EvolutionJobStatus::Failed
    )
}

fn job_status_label(status: &EvolutionJobStatus) -> &'static str {
    match status {
        EvolutionJobStatus::AwaitingApproval => "awaiting-approval",
        EvolutionJobStatus::Approved => "approved",
        EvolutionJobStatus::Rejected => "rejected",
        EvolutionJobStatus::Created => "created",
        EvolutionJobStatus::Failed => "failed",
        _ => "in-progress",
    }
}

fn outcome_label(outcome: &EvaluationOutcome) -> &'static str {
    match outcome {
        EvaluationOutcome::Passed => "passed",
        EvaluationOutcome::Failed => "failed",
        EvaluationOutcome::Inconclusive => "inconclusive",
    }
}

fn decision_outcome_label(outcome: &CandidateDecisionOutcome) -> &'static str {
    match outcome {
        CandidateDecisionOutcome::Approved => "approved",
        CandidateDecisionOutcome::Rejected => "rejected",
        CandidateDecisionOutcome::Restored => "restored",
    }
}

fn safe_file_stem(value: &str) -> String {
    let stem: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect();
    let stem = stem.trim_matches('-');
    if stem.is_empty() {
        "command".to_string()
    } else {
        stem.to_string()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderName {
    GitHubCopilot,
    Anthropic,
    OpenAi,
    Gemini,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderCliCommand {
    Auth(ProviderName),
    Test(ProviderName),
}

fn parse_provider_command(args: &[String]) -> Option<ProviderCliCommand> {
    let [providers, action, provider] = args else {
        return None;
    };
    if providers != "providers" {
        return None;
    }

    let provider = match provider.as_str() {
        "github-copilot" => ProviderName::GitHubCopilot,
        "anthropic" => ProviderName::Anthropic,
        "openai" => ProviderName::OpenAi,
        "gemini" => ProviderName::Gemini,
        _ => return None,
    };

    match action.as_str() {
        "auth" => Some(ProviderCliCommand::Auth(provider)),
        "test" => Some(ProviderCliCommand::Test(provider)),
        _ => None,
    }
}

fn run_provider_command(command: ProviderCliCommand) -> Result<(), CliError> {
    match command {
        ProviderCliCommand::Auth(ProviderName::GitHubCopilot) => auth_github_copilot(),
        ProviderCliCommand::Test(ProviderName::GitHubCopilot) => test_github_copilot(),
        ProviderCliCommand::Auth(ProviderName::Anthropic) => auth_anthropic(),
        ProviderCliCommand::Test(ProviderName::Anthropic) => test_anthropic(),
        ProviderCliCommand::Auth(ProviderName::OpenAi) => auth_openai(),
        ProviderCliCommand::Test(ProviderName::OpenAi) => test_openai(),
        ProviderCliCommand::Auth(ProviderName::Gemini) => auth_gemini(),
        ProviderCliCommand::Test(ProviderName::Gemini) => test_gemini(),
    }
}

pub fn auth_github_copilot() -> Result<(), CliError> {
    let (provider, credentials_path) = github_copilot_provider()?;
    let device = provider.device_login_start()?;

    println!("Open this URL in your browser:");
    println!("{}", device.verification_uri);
    println!();
    println!("Enter this code:");
    println!("{}", device.user_code);
    println!();
    println!("Waiting for GitHub authorization...");

    let started = Instant::now();
    let expires_in = if device.expires_in == 0 {
        Duration::from_secs(900)
    } else {
        Duration::from_secs(device.expires_in)
    };
    let mut interval = Duration::from_secs(device.interval.max(5));

    loop {
        if started.elapsed() >= expires_in {
            return Err(CliError::AuthExpired);
        }

        thread::sleep(interval);
        let poll = provider.device_login_poll(&device.device_code)?;
        if poll.access_token.is_some() {
            provider.test_connection()?;
            println!(
                "GitHub Copilot credentials saved to {}",
                credentials_path.display()
            );
            return Ok(());
        }

        match poll.error.as_deref() {
            Some("authorization_pending") | None => {}
            Some("slow_down") => {
                interval += Duration::from_secs(poll.interval.unwrap_or(5));
            }
            Some("expired_token") => return Err(CliError::AuthExpired),
            Some("access_denied") => return Err(CliError::AuthDenied),
            Some(error) => {
                return Err(CliError::AuthProvider {
                    detail: poll.error_description.unwrap_or_else(|| error.to_string()),
                });
            }
        }
    }
}

pub fn test_github_copilot() -> Result<(), CliError> {
    let (provider, credentials_path) = github_copilot_provider()?;
    println!(
        "Testing GitHub Copilot credentials at {}",
        credentials_path.display()
    );
    provider.github_token()?;
    println!("GitHub token: ok");
    provider.test_connection()?;
    println!("Copilot API models endpoint: ok");
    println!("GitHub Copilot provider connection: ok");
    Ok(())
}

pub fn auth_anthropic() -> Result<(), CliError> {
    let (provider, credentials_path) = anthropic_provider()?;
    let (api_key, source) = api_key_from_env_or_prompt(
        "Anthropic",
        &[
            "SINOMO_ANTHROPIC_API_KEY",
            "ANTHROPIC_API_KEY",
            "CLAUDE_API_KEY",
            "SINOMO_CLAUDE_API_KEY",
        ],
    )?;
    provider.save_api_key(&api_key, &source)?;
    println!("Testing Anthropic provider connection...");
    provider.test_connection()?;
    println!(
        "Anthropic credentials saved to {}",
        credentials_path.display()
    );
    Ok(())
}

pub fn test_anthropic() -> Result<(), CliError> {
    let (provider, credentials_path) = anthropic_provider()?;
    println!(
        "Testing Anthropic credentials at {}",
        credentials_path.display()
    );
    provider.api_key()?;
    println!("Anthropic API key: ok");
    provider.test_connection()?;
    println!("Anthropic provider connection: ok");
    Ok(())
}

pub fn auth_openai() -> Result<(), CliError> {
    let (provider, credentials_path) = openai_provider()?;
    let (api_key, source) = api_key_from_env_or_prompt(
        "OpenAI",
        &[
            "SINOMO_OPENAI_API_KEY",
            "OPENAI_API_KEY",
            "CODEX_API_KEY",
            "SINOMO_CODEX_API_KEY",
        ],
    )?;
    provider.save_api_key(&api_key, &source)?;
    println!("Testing OpenAI provider connection...");
    provider.test_connection()?;
    println!("OpenAI credentials saved to {}", credentials_path.display());
    Ok(())
}

pub fn test_openai() -> Result<(), CliError> {
    let (provider, credentials_path) = openai_provider()?;
    println!(
        "Testing OpenAI credentials at {}",
        credentials_path.display()
    );
    provider.api_key()?;
    println!("OpenAI API key: ok");
    provider.test_connection()?;
    println!("OpenAI provider connection: ok");
    Ok(())
}

pub fn auth_gemini() -> Result<(), CliError> {
    let (provider, credentials_path) = gemini_provider()?;
    let (api_key, source) = api_key_from_env_or_prompt(
        "Gemini",
        &[
            "SINOMO_GEMINI_API_KEY",
            "GEMINI_API_KEY",
            "GOOGLE_API_KEY",
            "GOOGLE_AI_API_KEY",
        ],
    )?;
    provider.save_api_key(&api_key, &source)?;
    println!("Testing Gemini provider connection...");
    provider.test_connection()?;
    println!("Gemini credentials saved to {}", credentials_path.display());
    Ok(())
}

pub fn test_gemini() -> Result<(), CliError> {
    let (provider, credentials_path) = gemini_provider()?;
    println!(
        "Testing Gemini credentials at {}",
        credentials_path.display()
    );
    provider.api_key()?;
    println!("Gemini API key: ok");
    provider.test_connection()?;
    println!("Gemini provider connection: ok");
    Ok(())
}

fn github_copilot_provider() -> Result<(GitHubCopilotProvider, PathBuf), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_provider_dir(GitHubCopilotProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(GitHubCopilotProvider::PROVIDER_ID);
    let client_id = env::var("SINOMO_COPILOT_CLIENT_ID")
        .unwrap_or_else(|_| GitHubCopilotConfig::default().client_id);
    let provider = GitHubCopilotProvider::new(GitHubCopilotConfig {
        client_id,
        token_store_path: Some(credentials_path.clone()),
        ..GitHubCopilotConfig::default()
    });
    Ok((provider, credentials_path))
}

fn anthropic_provider() -> Result<(AnthropicProvider, PathBuf), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_provider_dir(AnthropicProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(AnthropicProvider::PROVIDER_ID);
    let provider = AnthropicProvider::new(AnthropicConfig {
        api_base_url: env::var("SINOMO_ANTHROPIC_API_BASE_URL")
            .or_else(|_| env::var("SINOMO_CLAUDE_API_BASE_URL"))
            .unwrap_or_else(|_| AnthropicConfig::default().api_base_url),
        default_model: env::var("SINOMO_ANTHROPIC_MODEL")
            .or_else(|_| env::var("SINOMO_CLAUDE_MODEL"))
            .unwrap_or_else(|_| AnthropicConfig::default().default_model),
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
}

fn openai_provider() -> Result<(OpenAiProvider, PathBuf), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_provider_dir(OpenAiProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(OpenAiProvider::PROVIDER_ID);
    let provider = OpenAiProvider::new(OpenAiConfig {
        api_base_url: env::var("SINOMO_OPENAI_API_BASE_URL")
            .or_else(|_| env::var("SINOMO_CODEX_API_BASE_URL"))
            .unwrap_or_else(|_| OpenAiConfig::default().api_base_url),
        default_model: env::var("SINOMO_OPENAI_MODEL")
            .or_else(|_| env::var("SINOMO_CODEX_MODEL"))
            .unwrap_or_else(|_| OpenAiConfig::default().default_model),
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
}

fn gemini_provider() -> Result<(GeminiProvider, PathBuf), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_provider_dir(GeminiProvider::PROVIDER_ID)?;
    let credentials_path = data_dir.provider_credentials_file(GeminiProvider::PROVIDER_ID);
    let provider = GeminiProvider::new(GeminiConfig {
        api_base_url: env::var("SINOMO_GEMINI_API_BASE_URL")
            .unwrap_or_else(|_| GeminiConfig::default().api_base_url),
        default_model: env::var("SINOMO_GEMINI_MODEL")
            .unwrap_or_else(|_| GeminiConfig::default().default_model),
        credentials_path: Some(credentials_path.clone()),
    });
    Ok((provider, credentials_path))
}

fn api_key_from_env_or_prompt(
    provider_name: &str,
    env_vars: &[&str],
) -> Result<(String, String), CliError> {
    for env_var in env_vars {
        if let Ok(api_key) = env::var(env_var) {
            let api_key = api_key.trim();
            if !api_key.is_empty() {
                return Ok((api_key.to_string(), format!("env:{env_var}")));
            }
        }
    }

    println!("No {provider_name} API key found in environment. Paste an API key to store locally.");
    println!("Input will be visible in this terminal.");
    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .map_err(|source| CliError::AuthProvider {
            detail: format!("failed to read API key: {source}"),
        })?;
    let api_key = line.trim();
    if api_key.is_empty() {
        return Err(CliError::AuthProvider {
            detail: format!("{provider_name} API key was empty"),
        });
    }
    Ok((api_key.to_string(), "prompt".to_string()))
}

pub fn print_usage() {
    println!("Usage:");
    println!("  sinomo");
    println!(
        "  sinomo evolve propose \"<objective>\" [--repo <path>] [--provider github-copilot] [--verify \"cargo check\"]"
    );
    println!("  sinomo evolve approve [--job evojob_...] [--note <note>]");
    println!("  sinomo evolve reject [--job evojob_...] [--note <note>]");
    println!("  sinomo evolve restore [--job evojob_...] [--note <note>]");
    println!("  sinomo evolve status [--job evojob_...]");
    println!("  sinomo evolve show [--diff|--scorecard|--decision|--packet] [--job evojob_...]");
    println!("  sinomo evolve verify|packet|cleanup [--job evojob_...]");
    println!("  sinomo evolve models list [--provider <provider>]");
    println!(
        "  sinomo evolve models default --provider <provider> --model <model> [--repo <path>]"
    );
    println!("  sinomo providers auth github-copilot");
    println!("  sinomo providers test github-copilot");
    println!("  sinomo providers auth anthropic");
    println!("  sinomo providers test anthropic");
    println!("  sinomo providers auth openai");
    println!("  sinomo providers test openai");
    println!("  sinomo providers auth gemini");
    println!("  sinomo providers test gemini");
    println!("  sinomo-app providers auth github-copilot");
    println!("  sinomo-app providers test github-copilot");
    println!("  sinomo-app providers auth anthropic");
    println!("  sinomo-app providers test anthropic");
    println!("  sinomo-app providers auth openai");
    println!("  sinomo-app providers test openai");
    println!("  sinomo-app providers auth gemini");
    println!("  sinomo-app providers test gemini");
    println!();
    println!("CUI:");
    println!("  Run `sinomo` with no arguments to start the command-line user interface.");
    println!("  Inside the CUI, prefix any CLI command with `/`, for example `/evolve status`.");
    println!("  Use `/quit` or `/exit` to end the session.");
    println!();
    println!("Environment:");
    println!("  SINOMO_DATA_DIR             Defaults to ~/.sinomo");
    println!("  ANTHROPIC_API_KEY           Used by `providers auth/test anthropic`");
    println!("  OPENAI_API_KEY              Used by `providers auth/test openai`");
    println!("  GEMINI_API_KEY              Used by `providers auth/test gemini`");
    println!(
        "  SINOMO_ANTHROPIC_MODEL      Defaults to {}",
        AnthropicConfig::default().default_model
    );
    println!(
        "  SINOMO_OPENAI_MODEL         Defaults to {}",
        OpenAiConfig::default().default_model
    );
    println!(
        "  SINOMO_GEMINI_MODEL         Defaults to {}",
        GeminiConfig::default().default_model
    );
    println!(
        "  SINOMO_COPILOT_CLIENT_ID    Defaults to {}",
        GitHubCopilotConfig::default().client_id
    );
    println!(
        "                               GitHub App client IDs are not accepted by Copilot token exchange"
    );
}

#[derive(Debug)]
pub enum CliError {
    Usage,
    AuthDenied,
    AuthExpired,
    AuthProvider {
        detail: String,
    },
    NoActiveJob,
    AmbiguousJob {
        job_ids: Vec<String>,
    },
    InvalidJobState {
        job_id: String,
        status: String,
        expected: String,
    },
    MissingArtifact {
        artifact: String,
    },
    Parse {
        detail: String,
    },
    Io {
        path: String,
        detail: String,
    },
    Json {
        path: String,
        detail: String,
    },
    LocalData(sinomo_local::LocalDataError),
    EventStore(sinomo_events::EventStoreError),
    LanguageModel(LanguageModelError),
    Evolution(sinomo_evolution::EvolutionError),
    Eval(sinomo_eval::EvalError),
}

impl CliError {
    fn stdio(source: io::Error) -> Self {
        Self::Io {
            path: "stdio".to_string(),
            detail: source.to_string(),
        }
    }
}

impl From<sinomo_local::LocalDataError> for CliError {
    fn from(error: sinomo_local::LocalDataError) -> Self {
        Self::LocalData(error)
    }
}

impl From<sinomo_events::EventStoreError> for CliError {
    fn from(error: sinomo_events::EventStoreError) -> Self {
        Self::EventStore(error)
    }
}

impl From<LanguageModelError> for CliError {
    fn from(error: LanguageModelError) -> Self {
        Self::LanguageModel(error)
    }
}

impl From<sinomo_evolution::EvolutionError> for CliError {
    fn from(error: sinomo_evolution::EvolutionError) -> Self {
        Self::Evolution(error)
    }
}

impl From<sinomo_eval::EvalError> for CliError {
    fn from(error: sinomo_eval::EvalError) -> Self {
        Self::Eval(error)
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage => write!(formatter, "unknown command; run `sinomo help`"),
            Self::AuthDenied => write!(formatter, "GitHub device authorization was denied"),
            Self::AuthExpired => write!(formatter, "GitHub device authorization expired"),
            Self::AuthProvider { detail } => write!(formatter, "provider auth failed: {detail}"),
            Self::NoActiveJob => write!(formatter, "no active evolution job"),
            Self::AmbiguousJob { job_ids } => {
                write!(
                    formatter,
                    "multiple active evolution jobs; rerun with --job ({})",
                    job_ids.join(", ")
                )
            }
            Self::InvalidJobState {
                job_id,
                status,
                expected,
            } => write!(
                formatter,
                "evolution job {job_id} is {status}; expected {expected}"
            ),
            Self::MissingArtifact { artifact } => {
                write!(formatter, "{artifact} has not been written")
            }
            Self::Parse { detail } => write!(formatter, "failed to parse command: {detail}"),
            Self::Io { path, detail } => write!(formatter, "filesystem error at {path}: {detail}"),
            Self::Json { path, detail } => write!(formatter, "json error at {path}: {detail}"),
            Self::LocalData(error) => write!(formatter, "{error}"),
            Self::EventStore(error) => write!(formatter, "{error}"),
            Self::LanguageModel(error) => write!(formatter, "{error}"),
            Self::Evolution(error) => write!(formatter, "{error}"),
            Self::Eval(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for CliError {}

#[cfg(test)]
mod tests {
    use super::{
        CliError, EvolutionJobStatus, EvolutionPaths, InteractionModel, InteractionRuntime,
        InteractionSession, InteractionSurface, InteractionSurfaceKind,
        ModelBackedInteractionHandler, ProviderCliCommand, ProviderName, is_terminal_status,
        normalize_model_provider, parse_model_default_options, parse_models_list_provider,
        parse_propose_options, parse_provider_command, parse_slash_command,
        run_cui_with_handler_io, run_evolve_command,
    };
    use sinomo_core::{EvolutionJobId, NodeId, SessionId, TrustRing};
    use sinomo_events::{EventStore, InMemoryEventStore, ReplayQuery};
    use sinomo_lm::{
        LanguageModelError, LanguageModelRequest, LanguageModelResponse, LanguageModelUsage,
    };
    use sinomo_local::SinomoDataDir;
    use std::io::Cursor;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn provider_commands_parse_action_before_provider() {
        assert_eq!(
            parse_provider_command(&[
                "providers".to_string(),
                "auth".to_string(),
                "gemini".to_string(),
            ]),
            Some(ProviderCliCommand::Auth(ProviderName::Gemini))
        );
        assert_eq!(
            parse_provider_command(&[
                "providers".to_string(),
                "test".to_string(),
                "github-copilot".to_string(),
            ]),
            Some(ProviderCliCommand::Test(ProviderName::GitHubCopilot))
        );
    }

    #[test]
    fn provider_commands_reject_provider_before_action_order() {
        assert_eq!(
            parse_provider_command(&[
                "providers".to_string(),
                "gemini".to_string(),
                "auth".to_string(),
            ]),
            None
        );
    }

    #[test]
    fn evolve_propose_parser_accepts_common_flags() {
        let options = parse_propose_options(&[
            "improve scorecards".to_string(),
            "--provider".to_string(),
            "github-copilot".to_string(),
            "--model".to_string(),
            "gpt-5.4".to_string(),
            "--allow".to_string(),
            "crates/sinomo-eval/**".to_string(),
            "--verify".to_string(),
            "cargo test -p sinomo-eval".to_string(),
            "--target-ring".to_string(),
            "r1".to_string(),
            "--dry-run".to_string(),
        ])
        .expect("options");

        assert_eq!(options.objective, "improve scorecards");
        assert_eq!(options.provider.as_deref(), Some("github-copilot"));
        assert_eq!(options.model.as_deref(), Some("gpt-5.4"));
        assert_eq!(options.allow, vec!["crates/sinomo-eval/**"]);
        assert_eq!(options.verify, vec!["cargo test -p sinomo-eval"]);
        assert_eq!(options.target_ring, TrustRing::R1TrustedRuntime);
        assert!(options.dry_run);
    }

    #[test]
    fn slash_commands_reuse_cli_argument_shape() {
        let args = parse_slash_command(
            "/evolve propose \"improve scorecards\" --verify \"cargo check\" --dry-run",
        )
        .expect("parse")
        .expect("slash command");

        assert_eq!(
            args,
            vec![
                "evolve",
                "propose",
                "improve scorecards",
                "--verify",
                "cargo check",
                "--dry-run"
            ]
        );
        assert_eq!(parse_slash_command("hello sinomo").expect("parse"), None);
        assert_eq!(
            parse_slash_command("/evolve approve --note \"\"")
                .expect("parse")
                .expect("slash command"),
            vec!["evolve", "approve", "--note", ""]
        );
    }

    #[test]
    fn interaction_runtime_records_message_input_as_events() {
        let session = InteractionSession::new(
            SessionId::new("sess_cli_test").expect("session"),
            NodeId::new("node_cli_test").expect("node"),
            InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
        );
        let mut runtime = InteractionRuntime::new(
            InMemoryEventStore::new(),
            fixture_handler("hello from fixture"),
            session,
        );

        let outputs = runtime
            .ingest_user_input("start a conversation")
            .expect("input");

        assert_eq!(outputs.len(), 1);
        assert_eq!(
            outputs[0],
            super::InteractionOutput::Line("sinomo: hello from fixture".to_string())
        );
        let events = runtime.store().replay(ReplayQuery::all()).expect("replay");
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].event.kind.as_str(), "interaction.input.received");
        assert_eq!(
            events[1].event.kind.as_str(),
            "interaction.message.received"
        );
        assert_eq!(events[2].event.kind.as_str(), "interaction.model.requested");
        assert_eq!(events[3].event.kind.as_str(), "interaction.model.responded");
        assert_eq!(
            events[1].event.links.caused_by,
            vec![events[0].event.event_id.clone()]
        );
        assert_eq!(
            events[3].event.links.caused_by,
            vec![events[2].event.event_id.clone()]
        );
    }

    #[test]
    fn cui_loop_starts_and_exits_from_quit_command() {
        let mut output = Vec::new();

        run_cui_with_handler_io(
            Cursor::new("/quit\n"),
            &mut output,
            fixture_handler("unused"),
        )
        .expect("cui");

        let output = String::from_utf8(output).expect("utf8");
        assert!(output.contains("Sinomo CUI"));
        assert!(output.contains("ending session"));
    }

    fn fixture_handler(
        response: &'static str,
    ) -> ModelBackedInteractionHandler<FixtureInteractionModel> {
        ModelBackedInteractionHandler::new(
            FixtureInteractionModel { response },
            "fixture",
            "fixture-model",
        )
    }

    #[derive(Clone, Debug)]
    struct FixtureInteractionModel {
        response: &'static str,
    }

    impl InteractionModel for FixtureInteractionModel {
        fn complete(
            &self,
            request: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            assert_eq!(request.provider, "fixture");
            assert_eq!(request.model, "fixture-model");
            assert!(request.messages[1].content.contains("start a conversation"));
            Ok(LanguageModelResponse {
                provider: request.provider.clone(),
                model: request.model.clone(),
                content: self.response.to_string(),
                usage: Some(LanguageModelUsage {
                    input_tokens: Some(1),
                    output_tokens: Some(1),
                }),
            })
        }
    }

    #[test]
    fn model_list_parser_accepts_optional_provider() {
        assert_eq!(parse_models_list_provider(&[]).expect("empty args"), None);
        assert_eq!(
            parse_models_list_provider(&["--provider".to_string(), "github-copilot".to_string()])
                .expect("provider"),
            Some("github-copilot".to_string())
        );
        assert!(matches!(
            parse_models_list_provider(&["github-copilot".to_string()]),
            Err(CliError::Usage)
        ));
    }

    #[test]
    fn model_default_parser_accepts_provider_model_and_repo() {
        let options = parse_model_default_options(&[
            "--provider".to_string(),
            "openai-api".to_string(),
            "--model".to_string(),
            "gpt-5.5".to_string(),
            "--repo".to_string(),
            "/repo".to_string(),
        ])
        .expect("options");

        assert_eq!(options.provider, "openai");
        assert_eq!(options.model, "gpt-5.5");
        assert_eq!(options.repo.as_deref(), Some(std::path::Path::new("/repo")));
    }

    #[test]
    fn model_default_parser_requires_known_provider() {
        let error = parse_model_default_options(&[
            "--provider".to_string(),
            "unknown".to_string(),
            "--model".to_string(),
            "model".to_string(),
        ])
        .expect_err("unknown provider rejected");

        assert!(matches!(error, CliError::Parse { .. }));
    }

    #[test]
    fn model_provider_aliases_normalize_to_supported_provider_ids() {
        assert_eq!(
            normalize_model_provider("google-gemini").as_deref(),
            Some("gemini")
        );
        assert_eq!(
            normalize_model_provider("anthropic-claude-code").as_deref(),
            Some("claude-code")
        );
        assert_eq!(
            normalize_model_provider("github-copilot").as_deref(),
            Some("github-copilot")
        );
    }

    #[test]
    fn active_job_terminal_statuses_are_explicit() {
        assert!(is_terminal_status(&EvolutionJobStatus::Approved));
        assert!(is_terminal_status(&EvolutionJobStatus::Rejected));
        assert!(!is_terminal_status(&EvolutionJobStatus::AwaitingApproval));
    }

    #[test]
    fn evolve_init_command_is_removed() {
        let error = run_evolve_command(&["init".to_string()]).expect_err("init removed");

        assert!(matches!(error, CliError::Usage));
    }

    #[test]
    fn evolution_paths_store_record_inside_job_folder() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-cli-layout-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos()
        ));
        let paths = EvolutionPaths::new(&SinomoDataDir::new(&root));
        let job_id = EvolutionJobId::new("evojob_layout").expect("job id");

        assert_eq!(
            paths.job_path(&job_id),
            root.join("evolution")
                .join("jobs")
                .join("evojob_layout")
                .join("record.json")
        );
        assert_eq!(
            paths.legacy_job_path(&job_id),
            root.join("evolution")
                .join("jobs")
                .join("evojob_layout.json")
        );
    }
}
