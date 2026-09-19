//! Daemon-hosted interactive chat sessions.

use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use sinomo_agent::{
    AgentChannel, AgentChannelAuthor, AgentChannelKind, AgentChannelMessage,
    AgentChannelMessageKind, AgentChannelSource, AgentChannelStore, AgentRuntimeControlHost,
    AgentRuntimeControlRegistry, AgentSessionControlResult, AgentSessionFollowUpRequest,
    AgentSessionInterruptRequest, AgentSessionReattachState, AgentSessionRemoveQueuedInputRequest,
    AgentSessionResumeRequest, AgentSessionResumeResponse, AgentSessionSteerRequest,
    AgentTaskLifecycleStatus, AgentTaskSnapshot, TaskId,
};
use sinomo_runtime::api::app::chat as wire;
use sinomo_runtime::events::SessionId;
use sinomo_runtime::network::ObjectRpcRequest;
use sinomo_runtime::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};

use crate::app_api as local;

mod recovery;
use recovery::CommandJournal;

const CHAT_EVENT_RETENTION: usize = 8 * 1024;

#[derive(Clone)]
struct ChatEventLog {
    state: Arc<(Mutex<ChatEventLogState>, Condvar)>,
}

#[derive(Default)]
struct ChatEventLogState {
    next_sequence: u64,
    generation: String,
    events: VecDeque<wire::SequencedChatEvent>,
}

impl ChatEventLog {
    fn new() -> Self {
        Self {
            state: Arc::new((
                Mutex::new(ChatEventLogState {
                    next_sequence: 1,
                    generation: SessionId::generate().to_string(),
                    events: VecDeque::new(),
                }),
                Condvar::new(),
            )),
        }
    }

    fn push(&self, event: wire::ChatEvent) {
        let (lock, changed) = &*self.state;
        let mut state = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sequence = state.next_sequence;
        state.next_sequence = state.next_sequence.wrapping_add(1).max(1);
        state
            .events
            .push_back(wire::SequencedChatEvent { sequence, event });
        while state.events.len() > CHAT_EVENT_RETENTION {
            state.events.pop_front();
        }
        changed.notify_all();
    }

    fn poll(&self, request: &wire::ChatSessionEventsRequest) -> wire::ChatSessionEventsResponse {
        let limit = request.max_events.clamp(1, wire::CHAT_EVENT_PAGE_MAX);
        let wait = Duration::from_millis(request.wait_ms.min(wire::CHAT_EVENT_WAIT_MAX_MS));
        let (lock, changed) = &*self.state;
        let mut state = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state
            .events
            .iter()
            .any(|event| event.sequence > request.after_sequence)
            && !wait.is_zero()
        {
            let waited = changed.wait_timeout(state, wait);
            state = match waited {
                Ok((state, _)) => state,
                Err(error) => error.into_inner().0,
            };
        }
        let events = state
            .events
            .iter()
            .filter(|event| event.sequence > request.after_sequence)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        let next_sequence = events
            .last()
            .map(|event| event.sequence)
            .unwrap_or(request.after_sequence);
        wire::ChatSessionEventsResponse {
            events,
            next_sequence,
            first_available_sequence: state
                .events
                .front()
                .map(|e| e.sequence)
                .unwrap_or(state.next_sequence),
            generation: state.generation.clone(),
            execution_available: true,
        }
    }
}

enum WorkerCommand {
    Chat(String, wire::ChatSessionCommand),
    Shutdown,
}

struct HostedChatSession {
    command_tx: Sender<WorkerCommand>,
    events: ChatEventLog,
    storage: local::AgentChatSifsStorage,
    journal: CommandJournal,
    ready: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct HostedChatService {
    daemon_address: String,
    runtime_control: AgentRuntimeControlRegistry,
    sessions: Arc<Mutex<HashMap<String, HostedChatSession>>>,
    clients: Arc<Mutex<HashMap<String, String>>>,
    ephemeral_clients: Arc<Mutex<std::collections::BTreeSet<String>>>,
}

impl HostedChatService {
    pub fn new(
        daemon_address: impl Into<String>,
        runtime_control: AgentRuntimeControlRegistry,
    ) -> Self {
        Self {
            daemon_address: daemon_address.into(),
            runtime_control,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            clients: Arc::new(Mutex::new(HashMap::new())),
            ephemeral_clients: Arc::new(Mutex::new(std::collections::BTreeSet::new())),
        }
    }

    fn open(
        &self,
        data_dir: sinomo_runtime::local::SinomoDataDir,
        runtime: tokio::runtime::Handle,
        request: wire::ChatSessionOpenRequest,
    ) -> Result<wire::ChatSessionOpenResponse, String> {
        validate_client_id(&request.client_id)?;
        let ephemeral = request.execution_id.is_none();
        let execution_id = request
            .execution_id
            .clone()
            .unwrap_or_else(|| request.client_id.clone());
        validate_client_id(&execution_id)?;
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| "chat session registry lock poisoned".to_string())?;
        if sessions
            .get(&execution_id)
            .is_some_and(|host| !host.alive.load(Ordering::Acquire))
        {
            sessions.remove(&execution_id);
        }
        let attached = sessions.contains_key(&execution_id);
        if !attached {
            if sessions.len() >= 256 {
                return Err("too many resident chat execution hosts".into());
            }
            let journal = CommandJournal::open(&data_dir.root(), &execution_id)?;
            let events = ChatEventLog::new();
            let ready = Arc::new(AtomicBool::new(false));
            let alive = Arc::new(AtomicBool::new(true));
            let (command_tx, command_rx) = channel();
            let storage = local::AgentChatSifsStorage::new(
                data_dir.clone(),
                self.daemon_address.clone(),
                &runtime,
            );
            spawn_chat_worker(
                ChatWorkerContext {
                    execution_id: execution_id.clone(),
                    data_dir,
                    runtime_control: self.runtime_control.clone(),
                    storage: storage.clone(),
                    events: events.clone(),
                    journal: journal.clone(),
                    ready: ready.clone(),
                    alive: alive.clone(),
                },
                command_rx,
            )?;
            sessions.insert(
                execution_id.clone(),
                HostedChatSession {
                    command_tx,
                    events,
                    storage,
                    journal,
                    ready,
                    alive,
                },
            );
        }
        let runtime_ready = sessions[&execution_id].ready.load(Ordering::Acquire);
        let generation = sessions[&execution_id]
            .events
            .state
            .0
            .lock()
            .map_err(|_| "event log poisoned")?
            .generation
            .clone();
        self.clients
            .lock()
            .map_err(|_| "chat subscription registry poisoned")?
            .insert(request.client_id.clone(), execution_id);
        if ephemeral {
            self.ephemeral_clients
                .lock()
                .map_err(|_| "chat registry poisoned")?
                .insert(request.client_id.clone());
        }
        Ok(wire::ChatSessionOpenResponse {
            client_id: request.client_id,
            next_event_sequence: 0,
            attached,
            generation,
            runtime_ready,
            handoff_protocol: wire::CHAT_HANDOFF_PROTOCOL_VERSION,
        })
    }

    fn command(
        &self,
        request: wire::ChatSessionCommandRequest,
    ) -> Result<wire::ChatSessionCommandResponse, String> {
        let execution_id = match self.execution_id(&request.client_id) {
            Ok(id) => id,
            Err(_) => {
                return Ok(wire::ChatSessionCommandResponse {
                    accepted: false,
                    reattach_required: true,
                });
            }
        };
        let (command_tx, storage, events, journal) = {
            let sessions = self
                .sessions
                .lock()
                .map_err(|_| "chat session registry lock poisoned".to_string())?;
            let session = sessions
                .get(&execution_id)
                .ok_or_else(|| format!("unknown chat client `{}`", request.client_id))?;
            (
                session.command_tx.clone(),
                session.storage.clone(),
                session.events.clone(),
                session.journal.clone(),
            )
        };
        if matches!(request.command, wire::ChatSessionCommand::FetchAgents) {
            let (agents, warning) = match storage.open_agent_catalog_vfs() {
                Ok(vfs) => local::agent_summaries_with_error(&vfs),
                Err(error) => (
                    wire::fallback_agent_summaries(),
                    Some(format!("agent catalog unavailable: {error}")),
                ),
            };
            events.push(wire::ChatEvent::Agents { agents, warning });
            return Ok(wire::ChatSessionCommandResponse {
                accepted: true,
                reattach_required: false,
            });
        }
        if matches!(
            request.command,
            wire::ChatSessionCommand::ControlState { .. }
        ) {
            handle_direct_control(&self.runtime_control, &storage, &events, &request.command)?;
            return Ok(wire::ChatSessionCommandResponse {
                accepted: true,
                reattach_required: false,
            });
        }
        if matches!(
            request.command,
            wire::ChatSessionCommand::FetchModels { .. }
                | wire::ChatSessionCommand::FetchWorkspaces { .. }
        ) {
            command_tx
                .send(WorkerCommand::Chat(String::new(), request.command))
                .map_err(|_| "chat worker stopped")?;
            return Ok(wire::ChatSessionCommandResponse {
                accepted: true,
                reattach_required: false,
            });
        }
        let id = request
            .request_id
            .unwrap_or_else(|| SessionId::generate().to_string());
        if !journal.admit(&id, &request.command)? {
            return Ok(wire::ChatSessionCommandResponse {
                accepted: true,
                reattach_required: false,
            });
        }
        if matches!(
            request.command,
            wire::ChatSessionCommand::Steer { .. }
                | wire::ChatSessionCommand::FollowUp { .. }
                | wire::ChatSessionCommand::RemoveQueuedInput { .. }
                | wire::ChatSessionCommand::Interrupt { .. }
                | wire::ChatSessionCommand::ControlState { .. }
        ) {
            let _work = sinomo_runtime::rollout::acquire_runtime_control(&journal.data_dir())?;
            journal.start(&id)?;
            if let Err(error) =
                handle_direct_control(&self.runtime_control, &storage, &events, &request.command)
            {
                journal.reject(&id, error.clone())?;
                return Err(error);
            }
            journal.complete(&id, None)?;
            return Ok(wire::ChatSessionCommandResponse {
                accepted: true,
                reattach_required: false,
            });
        }
        let _ = command_tx.send(WorkerCommand::Chat(id, request.command));
        Ok(wire::ChatSessionCommandResponse {
            accepted: true,
            reattach_required: false,
        })
    }

    fn events(
        &self,
        request: wire::ChatSessionEventsRequest,
    ) -> Result<wire::ChatSessionEventsResponse, String> {
        let execution_id = self.execution_id(&request.client_id)?;
        let (events, alive) = self
            .sessions
            .lock()
            .map_err(|_| "chat session registry lock poisoned".to_string())?
            .get(&execution_id)
            .map(|session| (session.events.clone(), session.alive.clone()))
            .ok_or_else(|| format!("unknown chat client `{}`", request.client_id))?;
        let mut response = events.poll(&request);
        response.execution_available = alive.load(Ordering::Acquire);
        Ok(response)
    }

    fn close(
        &self,
        request: wire::ChatSessionCloseRequest,
    ) -> Result<wire::ChatSessionCloseResponse, String> {
        let mut sessions = self.sessions.lock().map_err(|_| "chat registry poisoned")?;
        let mut clients = self
            .clients
            .lock()
            .map_err(|_| "chat subscription registry poisoned")?;
        let execution_id = clients.remove(&request.client_id);
        let ephemeral = self
            .ephemeral_clients
            .lock()
            .map_err(|_| "chat registry poisoned")?
            .remove(&request.client_id);
        if ephemeral
            && let Some(id) = &execution_id
            && !clients.values().any(|other| other == id)
        {
            sessions.remove(id);
        }
        // Durable execution outlives a subscribing window. Legacy clients retain their previous
        // close semantics because they never negotiated a stable execution host.
        Ok(wire::ChatSessionCloseResponse {
            closed: execution_id.is_some(),
        })
    }

    pub(crate) fn recover(
        &self,
        data_dir: sinomo_runtime::local::SinomoDataDir,
        runtime: tokio::runtime::Handle,
    ) -> Result<(), String> {
        for execution_id in CommandJournal::discover(&data_dir.root())? {
            let client_id = format!("recovery-{}", SessionId::generate());
            self.open(
                data_dir.clone(),
                runtime.clone(),
                wire::ChatSessionOpenRequest {
                    client_id: client_id.clone(),
                    execution_id: Some(execution_id),
                },
            )?;
            self.close(wire::ChatSessionCloseRequest { client_id })?;
        }
        Ok(())
    }

    fn execution_id(&self, client_id: &str) -> Result<String, String> {
        self.clients
            .lock()
            .map_err(|_| "chat subscription registry poisoned")?
            .get(client_id)
            .cloned()
            .ok_or_else(|| format!("unknown chat client `{client_id}`"))
    }
}

impl Service for HostedChatService {
    fn name(&self) -> &'static str {
        "chat-session"
    }

    fn routes(&self) -> &'static [&'static str] {
        wire::CHAT_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        wire::CHAT_RPC_ROUTES
            .iter()
            .copied()
            .map(|route| {
                let capability = if matches!(
                    route,
                    wire::CHAT_SESSION_EVENTS_ROUTE
                        | wire::CHAT_HISTORY_LIST_ROUTE
                        | wire::CHAT_HISTORY_LOAD_ROUTE
                        | wire::CHAT_HISTORY_CHANNEL_ROUTE
                ) {
                    if route.starts_with("/chat/history/") {
                        wire::CHAT_HISTORY_READ_CAPABILITY
                    } else {
                        wire::CHAT_READ_CAPABILITY
                    }
                } else if route.starts_with("/chat/history/") {
                    wire::CHAT_HISTORY_WRITE_CAPABILITY
                } else {
                    wire::CHAT_WRITE_CAPABILITY
                };
                ServiceRoute::from_route_metadata(
                    route,
                    wire::CHAT_RPC_CATEGORY,
                    wire::CHAT_ROUTE_AUTH_SCOPE,
                    [capability],
                )
            })
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let service = self.clone();
        Box::pin(async move {
            if !ctx.signer.is_local() {
                return Err(ServiceError::Service(
                    "chat sessions are available only to the local app".to_string(),
                ));
            }
            match request.route.as_str() {
                wire::CHAT_SESSION_OPEN_ROUTE => {
                    let payload: wire::ChatSessionOpenRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    let response = tokio::task::spawn_blocking(move || {
                        service.open(data_dir, runtime, payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(wire::CHAT_SESSION_OPEN_ROUTE, response)?;
                }
                wire::CHAT_SESSION_COMMAND_ROUTE => {
                    let payload: wire::ChatSessionCommandRequest =
                        ctx.decode_request_body(&request).await?;
                    let response = tokio::task::spawn_blocking(move || service.command(payload))
                        .await
                        .map_err(|error| ServiceError::Service(error.to_string()))?
                        .map_err(ServiceError::Service)?;
                    ctx.respond_value(wire::CHAT_SESSION_COMMAND_ROUTE, response)?;
                }
                wire::CHAT_SESSION_EVENTS_ROUTE => {
                    let payload: wire::ChatSessionEventsRequest =
                        ctx.decode_request_body(&request).await?;
                    let response = tokio::task::spawn_blocking(move || service.events(payload))
                        .await
                        .map_err(|error| ServiceError::Service(error.to_string()))?
                        .map_err(ServiceError::Service)?;
                    ctx.respond_value(wire::CHAT_SESSION_EVENTS_ROUTE, response)?;
                }
                wire::CHAT_SESSION_CLOSE_ROUTE => {
                    let payload: wire::ChatSessionCloseRequest =
                        ctx.decode_request_body(&request).await?;
                    let response = tokio::task::spawn_blocking(move || service.close(payload))
                        .await
                        .map_err(|error| ServiceError::Service(error.to_string()))?
                        .map_err(ServiceError::Service)?;
                    ctx.respond_value(wire::CHAT_SESSION_CLOSE_ROUTE, response)?;
                }
                wire::CHAT_HISTORY_LIST_ROUTE => {
                    let _: wire::ChatHistoryListRequest = ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    let sessions = tokio::task::spawn_blocking(move || {
                        history_list(&service.daemon_address, data_dir, runtime)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        wire::CHAT_HISTORY_LIST_ROUTE,
                        wire::ChatHistoryListResponse { sessions },
                    )?;
                }
                wire::CHAT_HISTORY_LOAD_ROUTE => {
                    let payload: wire::ChatHistoryLoadRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    let response = tokio::task::spawn_blocking(move || {
                        history_load(&service.daemon_address, data_dir, runtime, payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(wire::CHAT_HISTORY_LOAD_ROUTE, response)?;
                }
                wire::CHAT_HISTORY_CHANNEL_ROUTE => {
                    let payload: wire::ChatHistoryChannelRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    let channel = tokio::task::spawn_blocking(move || {
                        history_channel(
                            &service.daemon_address,
                            data_dir,
                            runtime,
                            &payload.session_id,
                        )
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        wire::CHAT_HISTORY_CHANNEL_ROUTE,
                        wire::ChatHistoryChannelResponse { channel },
                    )?;
                }
                wire::CHAT_HISTORY_CREATE_CHANNEL_ROUTE => {
                    let payload: wire::ChatHistoryCreateChannelRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    tokio::task::spawn_blocking(move || {
                        history_create_channel(&service.daemon_address, data_dir, runtime, payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        wire::CHAT_HISTORY_CREATE_CHANNEL_ROUTE,
                        wire::ChatHistoryMutationResponse { accepted: true },
                    )?;
                }
                wire::CHAT_HISTORY_RENAME_ROUTE => {
                    let payload: wire::ChatHistoryRenameRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    tokio::task::spawn_blocking(move || {
                        history_rename(&service.daemon_address, data_dir, runtime, payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        wire::CHAT_HISTORY_RENAME_ROUTE,
                        wire::ChatHistoryMutationResponse { accepted: true },
                    )?;
                }
                wire::CHAT_HISTORY_DELETE_ROUTE => {
                    let payload: wire::ChatHistoryDeleteRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    tokio::task::spawn_blocking(move || {
                        history_delete(
                            &service.daemon_address,
                            data_dir,
                            runtime,
                            &payload.session_id,
                        )
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        wire::CHAT_HISTORY_DELETE_ROUTE,
                        wire::ChatHistoryMutationResponse { accepted: true },
                    )?;
                }
                wire::CHAT_HISTORY_PARTICIPANT_ROUTE => {
                    let payload: wire::ChatHistoryParticipantRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    tokio::task::spawn_blocking(move || {
                        history_participant(&service.daemon_address, data_dir, runtime, payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        wire::CHAT_HISTORY_PARTICIPANT_ROUTE,
                        wire::ChatHistoryMutationResponse { accepted: true },
                    )?;
                }
                wire::CHAT_HISTORY_APPEND_AGENT_ROUTE => {
                    let payload: wire::ChatHistoryAppendAgentRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    tokio::task::spawn_blocking(move || {
                        history_append_agent(&service.daemon_address, data_dir, runtime, payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        wire::CHAT_HISTORY_APPEND_AGENT_ROUTE,
                        wire::ChatHistoryMutationResponse { accepted: true },
                    )?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_hosted_chat_service(
    builder: ServiceRegistryBuilder,
    daemon_address: impl Into<String>,
    runtime_control: AgentRuntimeControlRegistry,
) -> ServiceRegistryBuilder {
    builder.register(HostedChatService::new(daemon_address, runtime_control))
}

impl Drop for HostedChatSession {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        let _ = self.command_tx.send(WorkerCommand::Shutdown);
    }
}

struct ChatWorkerContext {
    execution_id: String,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime_control: AgentRuntimeControlRegistry,
    storage: local::AgentChatSifsStorage,
    events: ChatEventLog,
    journal: CommandJournal,
    ready: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
}

fn spawn_chat_worker(
    context: ChatWorkerContext,
    command_rx: Receiver<WorkerCommand>,
) -> Result<(), String> {
    let recovered = context.journal.recovery()?;
    std::thread::Builder::new()
        .name(format!("sinomo-chat-{}", context.execution_id))
        .spawn(move || chat_worker(context, command_rx, recovered))
        .map(|_| ())
        .map_err(|e| format!("spawn daemon chat worker: {e}"))
}

fn chat_worker(
    context: ChatWorkerContext,
    command_rx: Receiver<WorkerCommand>,
    recovered: recovery::RecoveredCommands,
) {
    let ChatWorkerContext {
        execution_id,
        data_dir,
        runtime_control,
        storage,
        events,
        journal,
        ready,
        alive,
    } = context;
    struct WorkerLife(Arc<AtomicBool>);
    impl Drop for WorkerLife {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Release);
        }
    }
    let _life = WorkerLife(alive.clone());
    let mut chat = match local::AgentChat::with_runtime_control_registry_and_sifs(
        data_dir.clone(),
        true,
        runtime_control.clone(),
        storage.clone(),
    ) {
        Ok(chat) => chat,
        Err(error) => {
            tracing::warn!(
                execution_id = %execution_id,
                error = %error,
                "daemon chat worker initialization failed"
            );
            events.push(wire::ChatEvent::Error {
                session_id: None,
                error: format!("chat unavailable: {error}"),
            });
            return;
        }
    };
    let (bootstrap, pending, interrupted) = recovered;
    for command in bootstrap {
        let result = match command {
            wire::ChatSessionCommand::Resume { session_id, .. } => {
                chat.resume_session(&session_id, &[])
            }
            wire::ChatSessionCommand::SetModel {
                provider,
                model,
                account,
            } => chat.set_model(provider, model, account).map(|_| ()),
            _ => Err(crate::CliError::Parse {
                detail: "unsupported chat recovery command".into(),
            }),
        };
        if let Err(error) = result {
            events.push(wire::ChatEvent::Error {
                session_id: None,
                error: format!("chat restoration failed: {error}"),
            });
            return;
        }
    }
    ready.store(true, Ordering::Release);
    events.push(wire::ChatEvent::Ready {
        session_id: chat.session_id(),
        info: runtime_info(chat.runtime_info()),
    });
    events.push(wire::ChatEvent::ToolRuntime(tool_runtime(&chat)));
    if let Ok(state) = chat.control_state() {
        events.push(wire::ChatEvent::ControlState(control_state(state)));
    }
    if interrupted {
        events.push(wire::ChatEvent::Notice { session_id: chat.session_id(), notice: "An operation was interrupted by daemon failure. Its result must be reconciled before retrying; inspect the durable tool evidence before retrying.".into() });
    }
    let mut pending = VecDeque::from(pending);
    loop {
        if !alive.load(Ordering::Acquire) {
            break;
        }
        let (id, command) = if let Some(command) = pending.pop_front() {
            command
        } else {
            match command_rx.recv() {
                Ok(WorkerCommand::Chat(id, command)) => (id, command),
                Ok(WorkerCommand::Shutdown) | Err(_) => break,
            }
        };
        let result = (|| {
            let _work =
                sinomo_runtime::rollout::acquire_runtime_work_until(&data_dir.root(), || {
                    !alive.load(Ordering::Acquire)
                })?;
            if !id.is_empty() {
                journal.start(&id)?;
            }
            if !handle_direct_control(&runtime_control, &storage, &events, &command)? {
                run_worker_command(&mut chat, command.clone(), &events);
            }
            if id.is_empty() {
                Ok(())
            } else {
                journal.complete(&id, recovery::resume_command(&command, chat.session_id()))
            }
        })();
        if let Err(error) = result {
            events.push(wire::ChatEvent::Error {
                session_id: Some(chat.session_id()),
                error,
            });
        }
    }
}

fn run_worker_command(
    chat: &mut local::AgentChat,
    command: wire::ChatSessionCommand,
    events: &ChatEventLog,
) {
    match command {
        wire::ChatSessionCommand::Send {
            session_id,
            text,
            attachments,
        } => {
            let session_id = match ensure_session(chat, session_id) {
                Ok(session_id) => session_id,
                Err(error) => {
                    events.push(wire::ChatEvent::Error {
                        session_id: None,
                        error,
                    });
                    return;
                }
            };
            let attachments = attachments
                .into_iter()
                .map(local_attachment)
                .collect::<Vec<_>>();
            let result = chat.send_with_attachments(&text, &attachments, &mut |event| {
                if let Some(event) = stream_event(event, &session_id) {
                    events.push(event);
                }
            });
            if let Err(error) = result {
                events.push(wire::ChatEvent::Error {
                    session_id: Some(session_id.clone()),
                    error: error.to_string(),
                });
            }
            events.push(wire::ChatEvent::TurnDone { session_id });
        }
        wire::ChatSessionCommand::NewSession { session_id } => {
            match chat.resume_session(&session_id, &[]) {
                Ok(()) => {
                    events.push(wire::ChatEvent::SessionReset { session_id });
                    events.push(wire::ChatEvent::ToolRuntime(tool_runtime(chat)));
                }
                Err(error) => events.push(wire::ChatEvent::Error {
                    session_id: Some(session_id),
                    error: format!("new session failed: {error}"),
                }),
            }
        }
        wire::ChatSessionCommand::Resume {
            session_id,
            history,
        } => {
            let history = history
                .into_iter()
                .map(|message| local::ChatHistoryMessage {
                    role: message.role,
                    text: message.text,
                })
                .collect::<Vec<_>>();
            match chat.resume_session(&session_id, &history) {
                Ok(()) => {
                    events.push(wire::ChatEvent::ToolRuntime(tool_runtime(chat)));
                    match chat.control_state() {
                        Ok(state) => {
                            events.push(wire::ChatEvent::ControlState(control_state(state)))
                        }
                        Err(error) => events.push(wire::ChatEvent::Error {
                            session_id: Some(session_id),
                            error: format!("control state failed after resume: {error}"),
                        }),
                    }
                }
                Err(error) => events.push(wire::ChatEvent::Error {
                    session_id: Some(session_id),
                    error: format!("resume failed: {error}"),
                }),
            }
        }
        wire::ChatSessionCommand::SetModel {
            provider,
            model,
            account,
        } => match chat.set_model(provider, model, account) {
            Ok(info) => events.push(wire::ChatEvent::RuntimeInfo(runtime_info(info))),
            Err(error) => events.push(wire::ChatEvent::Error {
                session_id: Some(chat.session_id()),
                error: format!("model switch failed: {error}"),
            }),
        },
        wire::ChatSessionCommand::SetAgent {
            session_id,
            key,
            history: _,
        } => match ensure_session(chat, session_id.clone())
            .and_then(|_| chat.set_agent(&key).map_err(|e| e.to_string()))
        {
            Ok(info) => {
                events.push(wire::ChatEvent::RuntimeInfo(runtime_info(info)));
                events.push(wire::ChatEvent::ToolRuntime(tool_runtime(chat)));
            }
            Err(error) => events.push(wire::ChatEvent::Error {
                session_id: Some(session_id),
                error: format!("agent switch failed: {error}"),
            }),
        },
        wire::ChatSessionCommand::SetToolRuntime { selection } => {
            match chat.set_tool_runtime(local_tool_runtime(selection)) {
                Ok(_) => events.push(wire::ChatEvent::ToolRuntime(tool_runtime(chat))),
                Err(error) => events.push(wire::ChatEvent::Error {
                    session_id: Some(chat.session_id()),
                    error: format!("tool runtime switch failed: {error}"),
                }),
            }
        }
        wire::ChatSessionCommand::SetWorkspace {
            session_id,
            workspace_root,
        } => match ensure_session(chat, session_id.clone()).and_then(|_| {
            chat.replace_workspace_root(workspace_root)
                .map_err(|error| error.to_string())
        }) {
            Ok(_) => events.push(wire::ChatEvent::ToolRuntime(tool_runtime(chat))),
            Err(error) => events.push(wire::ChatEvent::Error {
                session_id: Some(session_id),
                error: format!("workspace switch failed: {error}"),
            }),
        },
        wire::ChatSessionCommand::UpdateWorkspaces {
            session_id,
            request_id,
            change,
        } => {
            let result = if session_id.trim().is_empty() {
                Err("A workspace update requires a conversation id.".into())
            } else {
                ensure_session(chat, session_id.clone()).and_then(|_| {
                    match change {
                        wire::ChatWorkspaceChange::AddFolder { path } => {
                            chat.add_workspace_root(path)
                        }
                        wire::ChatWorkspaceChange::Remove { workspace_id } => {
                            chat.remove_workspace(&workspace_id)
                        }
                        wire::ChatWorkspaceChange::MakeDefault { workspace_id } => {
                            chat.make_workspace_default(&workspace_id)
                        }
                        wire::ChatWorkspaceChange::Clear => chat.clear_workspaces(),
                    }
                    .map(|_| ())
                    .map_err(|error| error.to_string())
                })
            };
            let selection = (chat.session_id() == session_id).then(|| tool_runtime(chat));
            events.push(wire::ChatEvent::WorkspacesUpdated {
                session_id,
                request_id,
                selection,
                error: result.err(),
            });
        }
        wire::ChatSessionCommand::FetchWorkspaces { session_id } => {
            match ensure_session(chat, session_id.clone())
                .and_then(|_| chat.reload_workspaces().map_err(|error| error.to_string()))
            {
                Ok(_) => events.push(wire::ChatEvent::ToolRuntime(tool_runtime(chat))),
                Err(error) => events.push(wire::ChatEvent::Error {
                    session_id: Some(session_id),
                    error: format!("Workspace list unavailable: {error}"),
                }),
            }
        }
        wire::ChatSessionCommand::SetToolRuntimeNode { sandbox_node_id } => {
            match chat.set_tool_runtime_node(sandbox_node_id) {
                Ok(_) => events.push(wire::ChatEvent::ToolRuntime(tool_runtime(chat))),
                Err(error) => events.push(wire::ChatEvent::Error {
                    session_id: Some(chat.session_id()),
                    error: format!("sandbox placement switch failed: {error}"),
                }),
            }
        }
        wire::ChatSessionCommand::FetchModels { provider } => {
            let models = sinomo_lm::provider_available_model_ids(&provider).unwrap_or_default();
            events.push(wire::ChatEvent::Models { provider, models });
        }
        wire::ChatSessionCommand::FetchAgents => {}
        wire::ChatSessionCommand::RealtimeHandoff {
            request_id,
            session_id,
            input,
        } => {
            let result = ensure_session(chat, session_id).and_then(|_| {
                let mut visible = String::new();
                chat.send_with_attachments(&input, &[], &mut |event| {
                    if let local::ChatStreamEvent::Token(text)
                    | local::ChatStreamEvent::AssistantTextDelta { text, .. }
                    | local::ChatStreamEvent::AgentToken { text, .. } = event
                    {
                        visible.push_str(&text);
                    }
                })
                .map_err(|error| error.to_string())?;
                Ok(visible)
            });
            events.push(wire::ChatEvent::RealtimeHandoffResult { request_id, result });
        }
        wire::ChatSessionCommand::Steer { .. }
        | wire::ChatSessionCommand::FollowUp { .. }
        | wire::ChatSessionCommand::RemoveQueuedInput { .. }
        | wire::ChatSessionCommand::Interrupt { .. }
        | wire::ChatSessionCommand::ControlState { .. } => {}
    }
}

fn handle_direct_control(
    registry: &AgentRuntimeControlRegistry,
    storage: &local::AgentChatSifsStorage,
    events: &ChatEventLog,
    command: &wire::ChatSessionCommand,
) -> Result<bool, String> {
    let (requested_session_id, action): (&str, Box<dyn FnOnce(SessionId) -> Result<_, _>>) =
        match command {
            wire::ChatSessionCommand::Steer {
                session_id,
                message,
            } => {
                let registry = registry.clone();
                let message = message.clone();
                (
                    session_id,
                    Box::new(move |session_id| {
                        registry.steer(AgentSessionSteerRequest {
                            session_id,
                            message,
                        })
                    }),
                )
            }
            wire::ChatSessionCommand::FollowUp {
                session_id,
                message,
            } => {
                let registry = registry.clone();
                let message = message.clone();
                (
                    session_id,
                    Box::new(move |session_id| {
                        registry.follow_up(AgentSessionFollowUpRequest {
                            session_id,
                            message,
                        })
                    }),
                )
            }
            wire::ChatSessionCommand::RemoveQueuedInput {
                session_id,
                input_id,
            } => {
                let registry = registry.clone();
                let input_id = TaskId::from_str(input_id).map_err(|error| error.to_string())?;
                (
                    session_id,
                    Box::new(move |session_id| {
                        registry.remove_queued_input(AgentSessionRemoveQueuedInputRequest {
                            session_id,
                            input_id,
                        })
                    }),
                )
            }
            wire::ChatSessionCommand::Interrupt { session_id, reason } => {
                let registry = registry.clone();
                let reason = reason.clone();
                (
                    session_id,
                    Box::new(move |session_id| {
                        registry.interrupt(AgentSessionInterruptRequest { session_id, reason })
                    }),
                )
            }
            wire::ChatSessionCommand::ControlState { session_id } => {
                let session_id =
                    SessionId::from_str(session_id).map_err(|error| error.to_string())?;
                let response = registry
                    .resume(AgentSessionResumeRequest { session_id })
                    .map_err(|error| error.to_string())?;
                events.push(wire::ChatEvent::ControlState(control_resume_state(
                    response,
                )));
                return Ok(true);
            }
            _ => return Ok(false),
        };
    let session_id =
        SessionId::from_str(requested_session_id).map_err(|error| error.to_string())?;
    let result = action(session_id).map_err(|error| error.to_string())?;
    project_control_input(storage, requested_session_id, &result, events);
    events.push(wire::ChatEvent::ControlStatus(control_result(result)));
    Ok(true)
}

fn project_control_input(
    storage: &local::AgentChatSifsStorage,
    stable_channel_id: &str,
    result: &AgentSessionControlResult,
    events: &ChatEventLog,
) {
    if !result.accepted || local::chat_queued_input_from_events(&result.events).is_none() {
        return;
    }
    let Ok(channel_id) = SessionId::from_str(stable_channel_id) else {
        return;
    };
    let projection = storage.open_channel_store().and_then(|store| {
        local::project_accepted_chat_control_input(&store, &channel_id, result)
            .map_err(|error| error.to_string())
    });
    if let Err(error) = projection {
        events.push(wire::ChatEvent::Notice {
            session_id: stable_channel_id.to_string(),
            notice: format!("control accepted, but stable chat history is unavailable: {error}"),
        });
    }
}

fn ensure_session(chat: &mut local::AgentChat, session_id: String) -> Result<String, String> {
    if session_id.trim().is_empty() || chat.session_id() == session_id {
        return Ok(chat.session_id());
    }
    chat.resume_session(&session_id, &[])
        .map_err(|error| format!("resume failed before send: {error}"))?;
    Ok(session_id)
}

fn local_attachment(input: wire::ChatAttachmentInput) -> local::ChatAttachmentInput {
    let source = match input.source {
        wire::ChatAttachmentSource::LocalPath(path) => local::ChatAttachmentSource::LocalPath(path),
        wire::ChatAttachmentSource::LocalSnapshot { provenance, bytes } => {
            local::ChatAttachmentSource::LocalSnapshot {
                provenance: local::ChatLocalAttachmentProvenance {
                    original_path: provenance.original_path,
                    canonical_path: provenance.canonical_path,
                    modified_at_unix_nanos: provenance.modified_at_unix_nanos,
                    unsaved_editor_buffer: provenance.unsaved_editor_buffer,
                },
                bytes,
            }
        }
        wire::ChatAttachmentSource::SifsSnapshot { provenance, bytes } => {
            local::ChatAttachmentSource::SifsSnapshot {
                provenance: local::ChatSifsAttachmentProvenance {
                    path: provenance.path,
                    object_id: provenance.object_id,
                    version: provenance.version,
                    content_hash: provenance.content_hash,
                    policy: provenance.policy,
                    unsaved_editor_buffer: provenance.unsaved_editor_buffer,
                },
                bytes,
            }
        }
    };
    local::ChatAttachmentInput {
        file_name: input.file_name,
        source,
    }
}

fn stream_event(event: local::ChatStreamEvent, session_id: &str) -> Option<wire::ChatEvent> {
    Some(match event {
        local::ChatStreamEvent::Token(text) => wire::ChatEvent::Token {
            session_id: session_id.to_string(),
            text,
        },
        local::ChatStreamEvent::AssistantTextDelta { step_index, text } => {
            wire::ChatEvent::AssistantTextDelta {
                session_id: session_id.to_string(),
                step_index,
                text,
            }
        }
        local::ChatStreamEvent::ReasoningDelta { step_index, delta } => {
            wire::ChatEvent::ReasoningDelta {
                session_id: session_id.to_string(),
                step_index,
                delta,
            }
        }
        local::ChatStreamEvent::ReasoningSummary {
            step_index,
            summary,
        } => wire::ChatEvent::ReasoningSummary {
            session_id: session_id.to_string(),
            step_index,
            summary,
        },
        local::ChatStreamEvent::AgentToken { speaker, text } => wire::ChatEvent::AgentToken {
            session_id: session_id.to_string(),
            speaker: speaker_wire(speaker),
            text,
        },
        local::ChatStreamEvent::Notice(notice) if notice.is_empty() => return None,
        local::ChatStreamEvent::Notice(notice) => wire::ChatEvent::Notice {
            session_id: session_id.to_string(),
            notice,
        },
        local::ChatStreamEvent::ChannelSystem { kind, text, .. } => {
            wire::ChatEvent::SystemMessage {
                session_id: session_id.to_string(),
                kind,
                text,
            }
        }
        local::ChatStreamEvent::Context(context) => wire::ChatEvent::Context {
            session_id: session_id.to_string(),
            context: context_wire(context),
        },
        local::ChatStreamEvent::ToolCallStarted(call) => wire::ChatEvent::ToolCallStarted {
            session_id: session_id.to_string(),
            call: tool_call_wire(call),
        },
        local::ChatStreamEvent::ToolCallDelta {
            step_index,
            call_id,
            tool,
            arguments_delta,
        } => wire::ChatEvent::ToolCallDelta {
            session_id: session_id.to_string(),
            step_index,
            call_id,
            tool,
            arguments_delta,
        },
        local::ChatStreamEvent::ToolCallCompleted(call) => wire::ChatEvent::ToolCallCompleted {
            session_id: session_id.to_string(),
            call: tool_call_wire(call),
        },
        local::ChatStreamEvent::Process(process) => wire::ChatEvent::Process {
            session_id: session_id.to_string(),
            process: process_wire(process),
        },
        local::ChatStreamEvent::Usage(usage) => wire::ChatEvent::Usage {
            session_id: session_id.to_string(),
            usage: usage_wire(usage),
        },
    })
}

fn runtime_info(info: local::ChatRuntimeInfo) -> wire::RuntimeInfo {
    wire::RuntimeInfo {
        agent: info.agent,
        provider: info.provider,
        model: info.model,
        account: info.account,
        effort: info.reasoning_effort,
    }
}

fn tool_runtime(chat: &local::AgentChat) -> wire::ChatToolRuntimeSelection {
    let selection = chat.tool_runtime();
    wire::ChatToolRuntimeSelection {
        workspace_session_id: Some(chat.session_id()),
        workspaces: Some(
            chat.workspaces()
                .into_iter()
                .map(|workspace| wire::ChatWorkspace {
                    path: workspace
                        .sifs_root
                        .map(|path| format!("sifs:{path}"))
                        .or_else(|| workspace.local_root.map(|path| path.display().to_string()))
                        .unwrap_or_else(|| workspace.id.clone()),
                    id: workspace.id,
                })
                .collect(),
        ),
        sandbox_node_id: selection.sandbox_node_id,
        workspace_root: selection.workspace_root,
    }
}

fn local_tool_runtime(
    selection: wire::ChatToolRuntimeSelection,
) -> local::ChatToolRuntimeSelection {
    local::ChatToolRuntimeSelection {
        sandbox_node_id: selection.sandbox_node_id,
        workspace_root: selection.workspace_root,
    }
}

fn speaker_wire(speaker: local::ChatSpeaker) -> wire::ChatSpeaker {
    wire::ChatSpeaker {
        channel_id: speaker.channel_id,
        participant_id: speaker.participant_id,
        agent_key: speaker.agent_key,
        agent_name: speaker.agent_name,
        agent_session_id: speaker.agent_session_id,
    }
}

fn context_wire(context: local::ChatContextSnapshot) -> wire::ChatContextSnapshot {
    wire::ChatContextSnapshot {
        step_index: context.step_index,
        provider: context.provider,
        model: context.model,
        total_bytes: context.total_bytes,
        max_context_bytes: context.max_context_bytes,
        compaction_threshold_bytes: context.compaction_threshold_bytes,
        lanes: context
            .lanes
            .into_iter()
            .map(|lane| wire::ChatContextLane {
                kind: lane.kind,
                label: lane.label,
                bytes: lane.bytes,
                priority: lane.priority,
            })
            .collect(),
        included_count: context.included_count,
        omitted_count: context.omitted_count,
    }
}

fn tool_call_wire(call: local::ChatToolCallSnapshot) -> wire::ChatToolCallSnapshot {
    wire::ChatToolCallSnapshot {
        step_index: call.step_index,
        call_index: call.call_index,
        call_id: call.call_id,
        tool: call.tool,
        input: call.input,
        input_preview: call.input_preview,
    }
}

fn process_wire(process: local::ChatProcessSnapshot) -> wire::ChatProcessSnapshot {
    wire::ChatProcessSnapshot {
        process_id: process.process_id,
        command: process.command,
        status: process.status,
        exit_code: process.exit_code,
        elapsed_ms: process.elapsed_ms,
        stdout: process.stdout,
        stderr: process.stderr,
        stdout_tail: process.stdout_tail,
        stderr_tail: process.stderr_tail,
    }
}

fn usage_wire(usage: local::ChatUsageSnapshot) -> wire::ChatUsageSnapshot {
    wire::ChatUsageSnapshot {
        provider: usage.provider,
        model: usage.model,
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
    }
}

fn control_result(result: AgentSessionControlResult) -> wire::ChatControlStatus {
    let queued_input = local::chat_queued_input_from_events(&result.events).map(|queued| {
        wire::ChatQueuedTurnInput {
            input_id: queued.input_id,
            kind: match queued.kind {
                local::ChatQueuedTurnInputKind::Steering => wire::ChatQueuedTurnInputKind::Steering,
                local::ChatQueuedTurnInputKind::FollowUp => wire::ChatQueuedTurnInputKind::FollowUp,
            },
            message: queued.message,
        }
    });
    wire::ChatControlStatus {
        session_id: result.snapshot.session_id.to_string(),
        accepted: result.accepted,
        runtime_attached: true,
        sequence: result.sequence,
        event_count: result.events.len(),
        active_turn: result.snapshot.active_turn.is_some(),
        task_count: result.snapshot.tasks.len(),
        terminal_result_count: result.terminal_results().len(),
        tasks: result.snapshot.tasks.iter().map(task_wire).collect(),
        queued_input,
        removed_input_id: local::chat_removed_input_id_from_events(&result.events),
        warning: None,
    }
}

fn control_state(state: local::ChatControlState) -> wire::ChatControlState {
    wire::ChatControlState {
        session_id: state.session_id,
        accepted: state.accepted,
        runtime_attached: state.runtime_attached,
        reattach_state: reattach_wire(state.reattach_state),
        reattach_active_turn_persisted: state.reattach_active_turn_persisted,
        reattach_running_task_count: state.reattach_running_task_count,
        reattach_persisted_event_count: state.reattach_persisted_event_count,
        active_turn: state.active_turn,
        next_sequence: state.next_sequence,
        task_count: state.task_count,
        tasks: state.tasks.into_iter().map(local_task_wire).collect(),
        event_cursor_offset: state.event_cursor_offset,
        steering_count: state.steering_count,
        follow_up_count: state.follow_up_count,
        queued_inputs: state
            .queued_inputs
            .into_iter()
            .map(|queued| wire::ChatQueuedTurnInput {
                input_id: queued.input_id,
                kind: match queued.kind {
                    local::ChatQueuedTurnInputKind::Steering => {
                        wire::ChatQueuedTurnInputKind::Steering
                    }
                    local::ChatQueuedTurnInputKind::FollowUp => {
                        wire::ChatQueuedTurnInputKind::FollowUp
                    }
                },
                message: queued.message,
            })
            .collect(),
        detail: state.detail,
    }
}

fn control_resume_state(response: AgentSessionResumeResponse) -> wire::ChatControlState {
    let (active_turn, next_sequence, task_count, tasks) = response
        .control_snapshot
        .as_ref()
        .map(|snapshot| {
            (
                snapshot.active_turn.is_some(),
                snapshot.next_sequence,
                snapshot.tasks.len(),
                snapshot.tasks.iter().map(task_wire).collect(),
            )
        })
        .unwrap_or((false, 0, 0, Vec::new()));
    let (steering_count, follow_up_count, queued_inputs) = response
        .session
        .as_ref()
        .map(|session| {
            (
                session.steering_queue.len(),
                session.follow_up_queue.len(),
                local::chat_queued_inputs_from_session(session)
                    .into_iter()
                    .map(|queued| wire::ChatQueuedTurnInput {
                        input_id: queued.input_id,
                        kind: match queued.kind {
                            local::ChatQueuedTurnInputKind::Steering => {
                                wire::ChatQueuedTurnInputKind::Steering
                            }
                            local::ChatQueuedTurnInputKind::FollowUp => {
                                wire::ChatQueuedTurnInputKind::FollowUp
                            }
                        },
                        message: queued.message,
                    })
                    .collect(),
            )
        })
        .unwrap_or((0, 0, Vec::new()));
    wire::ChatControlState {
        session_id: response.session_id.to_string(),
        accepted: response.accepted,
        runtime_attached: response.runtime_attached,
        reattach_state: reattach_wire(response.reattach.state),
        reattach_active_turn_persisted: response.reattach.active_turn_persisted,
        reattach_running_task_count: response.reattach.running_task_count,
        reattach_persisted_event_count: response.reattach.persisted_event_count,
        active_turn,
        next_sequence,
        task_count,
        tasks,
        event_cursor_offset: response.event_cursor.offset,
        steering_count,
        follow_up_count,
        queued_inputs,
        detail: response.detail,
    }
}

fn reattach_wire(state: AgentSessionReattachState) -> wire::ChatReattachState {
    match state {
        AgentSessionReattachState::LiveRuntimeAttached => {
            wire::ChatReattachState::LiveRuntimeAttached
        }
        AgentSessionReattachState::PersistedMetadataOnly => {
            wire::ChatReattachState::PersistedStateOnly
        }
        AgentSessionReattachState::Missing => wire::ChatReattachState::RuntimeUnavailable,
    }
}

fn task_wire(task: &AgentTaskSnapshot) -> wire::ChatTaskSnapshot {
    wire::ChatTaskSnapshot {
        task_id: task.task.task_id.to_string(),
        kind: task.task.kind.clone(),
        summary: task.task.summary.clone(),
        status: match task.status {
            AgentTaskLifecycleStatus::Pending => "pending",
            AgentTaskLifecycleStatus::Running => "running",
            AgentTaskLifecycleStatus::Completed => "completed",
            AgentTaskLifecycleStatus::Failed => "failed",
            AgentTaskLifecycleStatus::Cancelled => "cancelled",
            AgentTaskLifecycleStatus::TimedOut => "timed-out",
        }
        .to_string(),
        progress_summary: task.progress_summary.clone(),
        result_ref: task.result_ref.clone(),
        active_op_id: task.active_op_id.as_ref().map(ToString::to_string),
    }
}

fn local_task_wire(task: local::ChatTaskSnapshot) -> wire::ChatTaskSnapshot {
    wire::ChatTaskSnapshot {
        task_id: task.task_id,
        kind: task.kind,
        summary: task.summary,
        status: task.status,
        progress_summary: task.progress_summary,
        result_ref: task.result_ref,
        active_op_id: task.active_op_id,
    }
}

fn open_history_store(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: &tokio::runtime::Handle,
) -> Result<local::SifsAgentChannelStore, String> {
    local::AgentChatSifsStorage::new(data_dir, daemon_address, runtime).open_channel_store()
}

fn history_list(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
) -> Result<Vec<wire::ChatHistorySessionSummary>, String> {
    open_history_store(daemon_address, data_dir, &runtime)?
        .list_channel_summaries()
        .map_err(|error| error.to_string())
        .map(|summaries| {
            summaries
                .into_iter()
                .filter(|summary| summary.deleted_at.is_none())
                .map(|summary| wire::ChatHistorySessionSummary {
                    id: summary.channel_id.to_string(),
                    title: summary.title,
                    kind: history_kind(summary.channel_kind),
                    updated_unix: summary.updated_at.unix_timestamp(),
                    participant_count: summary.enabled_participant_count,
                    participant_names: summary.participant_names,
                })
                .collect()
        })
}

fn history_load(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
    request: wire::ChatHistoryLoadRequest,
) -> Result<wire::ChatHistoryLoadResponse, String> {
    let channel_id = SessionId::from_str(&request.session_id).map_err(|error| error.to_string())?;
    let store = open_history_store(daemon_address, data_dir, &runtime)?;
    let (page, discovered_message_ids) = match request.mode {
        wire::ChatHistoryLoadMode::Initial | wire::ChatHistoryLoadMode::ReconcileLatest => {
            let known = request
                .known_message_ids
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>();
            let result = store
                .load_message_reconciliation_page(&channel_id, &known, request.limit)
                .map_err(|error| error.to_string())?;
            (result.page, result.discovered_message_ids)
        }
        wire::ChatHistoryLoadMode::Before => (
            store
                .load_message_page_before(&channel_id, request.sequence.unwrap_or(1), request.limit)
                .map_err(|error| error.to_string())?,
            Vec::new(),
        ),
        wire::ChatHistoryLoadMode::After => (
            store
                .load_message_page_from(
                    &channel_id,
                    request.sequence.unwrap_or(0).saturating_add(1),
                    request.limit,
                )
                .map_err(|error| error.to_string())?,
            Vec::new(),
        ),
    };
    let overview = request
        .include_overview
        .then(|| store.load_timeline_overview(&channel_id))
        .transpose()
        .map_err(|error| error.to_string())?
        .map(|overview| wire::ChatHistoryTimelineOverview {
            total_messages: overview.total_messages,
            oldest_sequence: overview.oldest_sequence,
            newest_sequence: overview.newest_sequence,
            chunk_size: overview.chunk_size,
            segments: overview
                .segments
                .into_iter()
                .map(|segment| wire::ChatHistoryTimelineSegment {
                    start_sequence: segment.start_sequence,
                    end_sequence: segment.end_sequence,
                    message_count: segment.message_count,
                    first_created_unix: segment.first_created_unix,
                    last_created_unix: segment.last_created_unix,
                    preview: segment.preview,
                })
                .collect(),
        });
    Ok(wire::ChatHistoryLoadResponse {
        page: history_page(page, discovered_message_ids),
        overview,
    })
}

fn history_channel(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
    session_id: &str,
) -> Result<Option<wire::ChatHistoryChannel>, String> {
    let channel_id = SessionId::from_str(session_id).map_err(|error| error.to_string())?;
    open_history_store(daemon_address, data_dir, &runtime)?
        .load_channel(&channel_id)
        .map_err(|error| error.to_string())
        .map(|channel| channel.map(history_channel_wire))
}

fn history_create_channel(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
    request: wire::ChatHistoryCreateChannelRequest,
) -> Result<(), String> {
    let channel_id = SessionId::from_str(&request.session_id).map_err(|error| error.to_string())?;
    let mut channel = AgentChannel::new_channel(channel_id, request.title);
    let participant_id = channel.ensure_participant(request.agent_key, request.agent_name);
    if let Some(participant) = channel.participant_mut(&participant_id) {
        participant.tool_runtime.sandbox_node_id = request.sandbox_node_id;
    }
    open_history_store(daemon_address, data_dir, &runtime)?
        .save_channel(&channel)
        .map_err(|error| error.to_string())
}

fn history_rename(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
    request: wire::ChatHistoryRenameRequest,
) -> Result<(), String> {
    let channel_id = SessionId::from_str(&request.session_id).map_err(|error| error.to_string())?;
    let store = open_history_store(daemon_address, data_dir, &runtime)?;
    let mut channel = store
        .load_channel(&channel_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "channel not found".to_string())?;
    channel.channel_kind = match request.kind {
        wire::ChatHistoryConversationKind::Chat => AgentChannelKind::Chat,
        wire::ChatHistoryConversationKind::Channel => AgentChannelKind::Channel,
    };
    channel.rename(request.title);
    store
        .save_channel(&channel)
        .map_err(|error| error.to_string())
}

fn history_delete(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
    session_id: &str,
) -> Result<(), String> {
    let channel_id = SessionId::from_str(session_id).map_err(|error| error.to_string())?;
    let store = open_history_store(daemon_address, data_dir, &runtime)?;
    let mut channel = store
        .load_channel(&channel_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "channel not found".to_string())?;
    channel.tombstone();
    store
        .save_channel(&channel)
        .map_err(|error| error.to_string())
}

fn history_participant(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
    request: wire::ChatHistoryParticipantRequest,
) -> Result<(), String> {
    let session_id = match &request {
        wire::ChatHistoryParticipantRequest::Add { session_id, .. }
        | wire::ChatHistoryParticipantRequest::Remove { session_id, .. } => session_id,
    };
    let channel_id = SessionId::from_str(session_id).map_err(|error| error.to_string())?;
    let store = open_history_store(daemon_address, data_dir, &runtime)?;
    let mut channel = store
        .load_channel(&channel_id)
        .map_err(|error| error.to_string())?
        .unwrap_or_else(|| AgentChannel::new_channel(channel_id, "New channel"));
    channel.channel_kind = AgentChannelKind::Channel;
    match request {
        wire::ChatHistoryParticipantRequest::Add {
            agent_key,
            agent_name,
            sandbox_node_id,
            ..
        } => {
            let participant_id = channel.ensure_participant(agent_key, agent_name);
            if let Some(participant) = channel.participant_mut(&participant_id) {
                participant.tool_runtime.sandbox_node_id = sandbox_node_id;
            }
        }
        wire::ChatHistoryParticipantRequest::Remove { participant_id, .. } => {
            let participant_id =
                TaskId::from_str(&participant_id).map_err(|error| error.to_string())?;
            if !channel.disable_participant(&participant_id) {
                return Err("agent not found".to_string());
            }
        }
    }
    store
        .save_channel(&channel)
        .map_err(|error| error.to_string())
}

fn history_append_agent(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
    request: wire::ChatHistoryAppendAgentRequest,
) -> Result<(), String> {
    let channel_id = SessionId::from_str(&request.session_id).map_err(|error| error.to_string())?;
    let agent_session_id =
        SessionId::from_str(&request.agent_session_id).map_err(|error| error.to_string())?;
    let message_id = TaskId::from_str(&request.message_id).map_err(|error| error.to_string())?;
    let created_at = time::OffsetDateTime::from_unix_timestamp_nanos(request.created_at_unix_nanos)
        .map_err(|error| error.to_string())?;
    let external_channel_id = request.session_id.clone();
    let external_message_id = request.message_id.clone();
    let store = open_history_store(daemon_address, data_dir, &runtime)?;
    local::project_stable_chat_agent_message(
        &store,
        &channel_id,
        &request.agent_key,
        &request.agent_name,
        &agent_session_id,
        message_id,
        created_at,
        &request.content,
        AgentChannelSource {
            adapter: request.source_adapter,
            external_channel_id: Some(external_channel_id),
            external_message_id: Some(external_message_id),
            metadata: serde_json::json!({
                "surface_event": "realtime_output_text_turn",
                "text_source": "provider_neutral_visible_output",
            }),
            ..AgentChannelSource::default()
        },
    )
    .map_err(|error| error.to_string())
}

fn history_page(
    page: local::AgentChannelMessagePage,
    discovered_message_ids: Vec<String>,
) -> wire::ChatHistoryPage {
    wire::ChatHistoryPage {
        messages: page.messages.into_iter().map(history_message).collect(),
        start_sequence: page.start_sequence,
        end_sequence: page.end_sequence,
        has_older: page.has_older,
        has_newer: page.has_newer,
        total_messages: page.total_messages,
        chunk_size: page.chunk_size,
        discovered_message_ids,
    }
}

fn history_message(message: AgentChannelMessage) -> wire::ChatHistoryStoredMessage {
    wire::ChatHistoryStoredMessage {
        message_id: message.message_id.to_string(),
        sequence: message.sequence,
        author: match message.author {
            AgentChannelAuthor::User { display_name, .. } => {
                wire::ChatHistoryAuthor::User { display_name }
            }
            AgentChannelAuthor::Agent { agent_name, .. } => {
                wire::ChatHistoryAuthor::Agent { agent_name }
            }
            AgentChannelAuthor::System => wire::ChatHistoryAuthor::System,
        },
        message_kind: match message.message_kind {
            AgentChannelMessageKind::Normal => wire::ChatHistoryMessageKind::Normal,
            AgentChannelMessageKind::Info => wire::ChatHistoryMessageKind::Info,
            AgentChannelMessageKind::Error => wire::ChatHistoryMessageKind::Error,
        },
        content: message.content,
        created_at_unix: message.created_at.unix_timestamp(),
        attachments: message
            .attachments
            .into_iter()
            .map(|attachment| wire::ChatHistoryAttachment {
                file_name: attachment.file_name,
                stored_path: attachment.stored_path,
                size_bytes: attachment.size_bytes,
            })
            .collect(),
    }
}

fn history_channel_wire(channel: AgentChannel) -> wire::ChatHistoryChannel {
    let participants = channel
        .enabled_participants_in_order()
        .into_iter()
        .map(|participant| wire::ChatHistoryParticipant {
            participant_id: participant.participant_id.to_string(),
            agent_key: participant.agent_key,
            agent_name: participant.agent_name,
            agent_session_id: participant.agent_session_id.to_string(),
            enabled: participant.enabled,
            order: participant.order,
            sandbox_node_id: participant.tool_runtime.sandbox_node_id,
        })
        .collect();
    wire::ChatHistoryChannel {
        id: channel.channel_id.to_string(),
        title: channel.title,
        kind: history_kind(channel.channel_kind),
        participants,
    }
}

fn history_kind(kind: AgentChannelKind) -> wire::ChatHistoryConversationKind {
    match kind {
        AgentChannelKind::Chat => wire::ChatHistoryConversationKind::Chat,
        AgentChannelKind::Channel => wire::ChatHistoryConversationKind::Channel,
    }
}

fn validate_client_id(client_id: &str) -> Result<(), String> {
    if client_id.is_empty()
        || client_id.len() > 256
        || !client_id.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_' | ':')
        })
    {
        return Err("invalid chat client id".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_management_commands_preserve_order_and_persist_exact_grants() {
        let root = sinomo_testdir::TestDir::new("multi-workspace-ui-commands");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(root.join("data"));
        let first = root.join("frontend");
        let second = root.join("shared");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        let first = std::fs::canonicalize(first).unwrap();
        let second = std::fs::canonicalize(second).unwrap();
        let mut chat = local::AgentChat::new(data_dir.clone(), false).unwrap();
        chat.set_agent("coding-agent").unwrap();
        let session_id = chat.session_id();
        let events = ChatEventLog::new();
        let apply = |chat: &mut local::AgentChat, change| {
            run_worker_command(
                chat,
                wire::ChatSessionCommand::UpdateWorkspaces {
                    session_id: session_id.clone(),
                    request_id: "request".into(),
                    change,
                },
                &events,
            );
            let state = events.state.0.lock().unwrap();
            let Some(wire::SequencedChatEvent {
                event:
                    wire::ChatEvent::WorkspacesUpdated {
                        error,
                        selection: Some(selection),
                        ..
                    },
                ..
            }) = state.events.back()
            else {
                panic!("workspace reply")
            };
            assert!(error.is_none(), "{error:?}");
            selection.clone()
        };
        let selected = apply(
            &mut chat,
            wire::ChatWorkspaceChange::AddFolder {
                path: first.clone(),
            },
        );
        let first_id = selected.workspaces.as_ref().unwrap()[0].id.clone();
        let selected = apply(
            &mut chat,
            wire::ChatWorkspaceChange::AddFolder {
                path: second.clone(),
            },
        );
        let second_id = selected.workspaces.as_ref().unwrap()[1].id.clone();
        assert_eq!(selected.workspace_root.as_ref(), Some(&first));
        assert_eq!(selected.workspaces.as_ref().unwrap().len(), 2);
        let duplicated = apply(
            &mut chat,
            wire::ChatWorkspaceChange::AddFolder {
                path: first.clone(),
            },
        );
        assert_eq!(duplicated, selected);
        let selected = apply(
            &mut chat,
            wire::ChatWorkspaceChange::MakeDefault {
                workspace_id: second_id.clone(),
            },
        );
        assert_eq!(selected.workspace_root.as_ref(), Some(&second));
        assert_eq!(selected.workspaces.as_ref().unwrap()[0].id, second_id);
        let mut resumed = local::AgentChat::new(data_dir, false).unwrap();
        resumed.resume_session(&session_id, &[]).unwrap();
        assert_eq!(resumed.workspaces(), chat.workspaces());
        resumed.remove_workspace(&second_id).unwrap();
        run_worker_command(
            &mut chat,
            wire::ChatSessionCommand::FetchWorkspaces {
                session_id: session_id.clone(),
            },
            &events,
        );
        assert_eq!(
            chat.workspaces().len(),
            1,
            "refresh observes another view's revocation"
        );
        let selected = apply(
            &mut chat,
            wire::ChatWorkspaceChange::AddFolder {
                path: first.clone(),
            },
        );
        assert_eq!(
            selected.workspaces.as_ref().unwrap().len(),
            1,
            "adding an existing folder must not revive an old grant"
        );
        let selected = apply(
            &mut chat,
            wire::ChatWorkspaceChange::Remove {
                workspace_id: second_id,
            },
        );
        assert_eq!(selected.workspace_root.as_ref(), Some(&first));
        assert_eq!(selected.workspaces.as_ref().unwrap()[0].id, first_id);
        let selected = apply(&mut chat, wire::ChatWorkspaceChange::Clear);
        assert!(selected.workspaces.unwrap().is_empty());
        assert!(selected.workspace_root.is_none());
        assert!(chat.workspaces().is_empty());
        resumed.add_workspace_root(second).unwrap();
        apply(&mut chat, wire::ChatWorkspaceChange::Clear);
        resumed.reload_workspaces().unwrap();
        assert!(
            resumed.workspaces().is_empty(),
            "clear must observe grants added by another view"
        );
        run_worker_command(
            &mut chat,
            wire::ChatSessionCommand::UpdateWorkspaces {
                session_id,
                request_id: "stale".into(),
                change: wire::ChatWorkspaceChange::MakeDefault {
                    workspace_id: first_id,
                },
            },
            &events,
        );
        assert!(chat.workspaces().is_empty());
        assert!(matches!(
            &events.state.0.lock().unwrap().events.back().unwrap().event,
            wire::ChatEvent::WorkspacesUpdated { error: Some(_), .. }
        ));
    }

    fn notice(value: usize) -> wire::ChatEvent {
        wire::ChatEvent::Notice {
            session_id: "session-1".to_string(),
            notice: value.to_string(),
        }
    }

    fn poll(after_sequence: u64, max_events: usize) -> wire::ChatSessionEventsRequest {
        wire::ChatSessionEventsRequest {
            client_id: "client-1".to_string(),
            after_sequence,
            max_events,
            wait_ms: 0,
        }
    }

    #[test]
    fn event_log_pages_from_an_exclusive_cursor() {
        let events = ChatEventLog::new();
        events.push(notice(1));
        events.push(notice(2));
        events.push(notice(3));

        let first = events.poll(&poll(0, 2));
        assert_eq!(
            first
                .events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(first.next_sequence, 2);

        let second = events.poll(&poll(first.next_sequence, 2));
        assert_eq!(
            second
                .events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![3]
        );
        assert_eq!(second.next_sequence, 3);
    }

    #[test]
    fn closing_a_subscription_keeps_execution_and_reconnect_replays_without_duplicate_commands() {
        let root = sinomo_testdir::TestDir::new("chat-detached-execution");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let service = HostedChatService::new("127.0.0.1:1", AgentRuntimeControlRegistry::new());
        let events = ChatEventLog::new();
        events.push(notice(1));
        let (command_tx, commands) = channel();
        service.sessions.lock().unwrap().insert(
            "durable-host".into(),
            HostedChatSession {
                command_tx,
                events: events.clone(),
                storage: local::AgentChatSifsStorage::new(
                    data_dir.clone(),
                    "127.0.0.1:1",
                    runtime.handle(),
                ),
                journal: CommandJournal::open(&root, "durable-host").unwrap(),
                ready: Arc::new(AtomicBool::new(true)),
                alive: Arc::new(AtomicBool::new(true)),
            },
        );
        service
            .clients
            .lock()
            .unwrap()
            .insert("old-window".into(), "durable-host".into());
        assert!(
            service
                .close(wire::ChatSessionCloseRequest {
                    client_id: "old-window".into()
                })
                .unwrap()
                .closed
        );
        assert!(
            commands.try_recv().is_err(),
            "detach must not send worker shutdown"
        );
        events.push(notice(2));
        let open = service
            .open(
                data_dir,
                runtime.handle().clone(),
                wire::ChatSessionOpenRequest {
                    client_id: "new-window".into(),
                    execution_id: Some("durable-host".into()),
                },
            )
            .unwrap();
        assert!(open.attached);
        let replay = service
            .events(wire::ChatSessionEventsRequest {
                client_id: "new-window".into(),
                after_sequence: 1,
                max_events: 20,
                wait_ms: 0,
            })
            .unwrap();
        assert_eq!(replay.events.len(), 1);
        assert_eq!(replay.events[0].sequence, 2);
        assert_eq!(replay.generation, open.generation);
        let command = wire::ChatSessionCommandRequest {
            client_id: "new-window".into(),
            request_id: Some("same-operation".into()),
            command: wire::ChatSessionCommand::Send {
                session_id: "session".into(),
                text: "continue".into(),
                attachments: vec![],
            },
        };
        assert!(service.command(command.clone()).unwrap().accepted);
        assert!(service.command(command).unwrap().accepted);
        assert!(matches!(
            commands.try_recv().unwrap(),
            WorkerCommand::Chat(_, _)
        ));
        assert!(
            commands.try_recv().is_err(),
            "retry must not execute the command twice"
        );
    }

    #[test]
    fn event_log_retains_a_bounded_tail() {
        let events = ChatEventLog::new();
        for value in 0..(CHAT_EVENT_RETENTION + 3) {
            events.push(notice(value));
        }

        let state = events
            .state
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(state.events.len(), CHAT_EVENT_RETENTION);
        assert_eq!(state.events.front().map(|event| event.sequence), Some(4));
        assert_eq!(
            state.events.back().map(|event| event.sequence),
            Some((CHAT_EVENT_RETENTION + 3) as u64)
        );
    }

    #[test]
    fn client_ids_are_safe_for_worker_names_and_registry_keys() {
        assert!(validate_client_id("app-chat-42:desktop").is_ok());
        assert!(validate_client_id("").is_err());
        assert!(validate_client_id("contains/a/path").is_err());
        assert!(validate_client_id(&"x".repeat(257)).is_err());
    }

    #[test]
    fn chat_routes_are_local_only_and_capability_scoped() {
        let routes = HostedChatService::new("127.0.0.1:23572", AgentRuntimeControlRegistry::new())
            .route_categories();

        assert_eq!(routes.len(), wire::CHAT_RPC_ROUTES.len());
        for route in routes {
            assert_eq!(
                route.auth_scope.as_deref(),
                Some(wire::CHAT_ROUTE_AUTH_SCOPE)
            );
            assert_eq!(route.required_capabilities.len(), 1);
        }
    }
}
