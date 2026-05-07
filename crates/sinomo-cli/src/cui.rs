use super::*;

pub fn run_cui() -> Result<(), CliError> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    run_cui_with_io(stdin.lock(), stdout.lock())
}

pub fn run_cui_with_io<R: BufRead, W: Write>(mut reader: R, mut writer: W) -> Result<(), CliError> {
    let handler = CliInteractionHandler::from_default_model()?;
    let daemon = default_main_daemon_status()?;
    run_cui_with_handler_io_and_daemon(&mut reader, &mut writer, handler, daemon)
}

pub fn run_cui_with_handler_io<R: BufRead, W: Write, H: InteractionEventHandler>(
    mut reader: R,
    mut writer: W,
    handler: H,
) -> Result<(), CliError> {
    run_cui_with_handler_io_and_daemon(&mut reader, &mut writer, handler, None)
}

pub(super) fn run_cui_with_handler_io_and_daemon<
    R: BufRead,
    W: Write,
    H: InteractionEventHandler,
>(
    mut reader: R,
    mut writer: W,
    handler: H,
    daemon: Option<DaemonStatus>,
) -> Result<(), CliError> {
    let node_id = daemon
        .as_ref()
        .and_then(|status| status.cluster.as_ref())
        .map(|cluster| cluster.node_id.clone())
        .unwrap_or_else(|| NodeId::from_legacy_named("node_cli").expect("valid node id"));
    let session = InteractionSession::new(
        SessionId::generate(),
        node_id,
        InteractionSurface::new("surface.cli", InteractionSurfaceKind::CommandLine),
    );
    let mut runtime = InteractionRuntime::new(InMemoryEventStore::new(), handler, session.clone());

    writeln!(writer, "Sinomo CUI").map_err(CliError::stdio)?;
    writeln!(writer, "session: {}", session.session_id).map_err(CliError::stdio)?;
    writeln!(writer, "node: {}", session.node_id).map_err(CliError::stdio)?;
    if let Some(status) = &daemon {
        writeln!(
            writer,
            "daemon: connected pid={} address={}",
            status.pid, status.address
        )
        .map_err(CliError::stdio)?;
        if let Some(cluster) = &status.cluster {
            writeln!(writer, "cluster: {}", cluster.cluster_id).map_err(CliError::stdio)?;
        }
    } else {
        writeln!(writer, "daemon: not connected").map_err(CliError::stdio)?;
    }
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
        let evolution_config = read_config(&paths).ok();
        let model_config = read_model_config(&ModelPaths::new(&data_dir)).ok();
        let provider = model_config
            .as_ref()
            .and_then(|config| config.default_provider.clone())
            .or_else(|| {
                evolution_config
                    .as_ref()
                    .and_then(|config| config.default_provider.clone())
            })
            .unwrap_or_else(|| GitHubCopilotProvider::PROVIDER_ID.to_string());
        let provider = normalize_model_provider(&provider).ok_or_else(|| CliError::Parse {
            detail: format!("unknown provider `{provider}`"),
        })?;
        let model = model_config
            .as_ref()
            .and_then(|config| config.default_model.clone())
            .or_else(|| {
                evolution_config
                    .as_ref()
                    .and_then(|config| config.default_model.clone())
            })
            .unwrap_or_else(|| PROVIDER_DEFAULT_MODEL.to_string());
        let model = normalize_model_id_for_provider(&provider, &model);
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
            CODEX_PROVIDER_ID => {
                let provider = codex_provider_for_model(&self.model);
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
            NvidiaProvider::PROVIDER_ID => {
                let credentials_path = self
                    .data_dir
                    .provider_credentials_file(NvidiaProvider::PROVIDER_ID);
                let provider = NvidiaProvider::new(NvidiaConfig {
                    api_base_url: env::var("SINOMO_NVIDIA_API_BASE_URL")
                        .unwrap_or_else(|_| NvidiaConfig::default().api_base_url),
                    default_model: model_or_default(
                        &self.model,
                        NvidiaConfig::default().default_model,
                    ),
                    credentials_path: Some(credentials_path),
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
