//! Thin client actor for daemon-hosted chat execution.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread;
#[cfg(test)]
use std::thread::JoinHandle;
use std::time::Duration;

use sinomo_client::ChatDaemonClient;
use sinomo_client::chat_contract as wire;

pub use wire::{
    AgentSummary, ChatActivityRef, ChatAttachmentInput, ChatAttachmentSource, ChatContextLane,
    ChatContextSnapshot, ChatControlState, ChatControlStatus, ChatEvent, ChatHistoryMessage,
    ChatLocalAttachmentProvenance, ChatProcessSnapshot, ChatQueuedTurnInput,
    ChatQueuedTurnInputKind, ChatReattachState, ChatSifsAttachmentProvenance, ChatSpeaker,
    ChatTaskSnapshot, ChatToolActivity, ChatToolCallRef, ChatToolCallSnapshot,
    ChatToolRuntimeSelection, ChatUsageSnapshot, ChatWorkTrace, ChatWorkTraceEntry, ChatWorkspace,
    ChatWorkspaceChange, ProviderCapabilities, ProviderSummary, RuntimeInfo,
};

pub type ChatEventWake = Arc<dyn Fn() + Send + Sync + 'static>;
const CHAT_CLIENT_POLL_WAIT: Duration = Duration::from_millis(wire::CHAT_EVENT_WAIT_MAX_MS);
const CHAT_CLIENT_RPC_TIMEOUT: Duration = Duration::from_secs(10);
static CHAT_CLIENT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ChatRole {
    User,
    Agent,
}

#[derive(Clone, Debug)]
pub enum ChatCommand {
    Send {
        session_id: String,
        text: String,
        attachments: Vec<ChatAttachmentInput>,
    },
    Steer {
        session_id: String,
        message: String,
    },
    FollowUp {
        session_id: String,
        message: String,
    },
    RemoveQueuedInput {
        session_id: String,
        input_id: String,
    },
    /// Pause: no new model requests; running tools finish; the next message
    /// resumes the session.
    Interrupt {
        session_id: String,
        reason: String,
    },
    /// Stop everything now, including running tools and processes.
    Cancel {
        session_id: String,
        reason: String,
    },
    ControlState {
        session_id: String,
    },
    NewSession {
        session_id: String,
    },
    Resume {
        session_id: String,
        history: Vec<ChatHistoryMessage>,
    },
    SetModel {
        provider: String,
        model: String,
        account: String,
    },
    SetAgent {
        session_id: String,
        key: String,
        history: Vec<ChatHistoryMessage>,
    },
    SetToolRuntime {
        selection: ChatToolRuntimeSelection,
    },
    SetWorkspace {
        session_id: String,
        workspace_root: Option<std::path::PathBuf>,
    },
    UpdateWorkspaces {
        session_id: String,
        request_id: String,
        change: ChatWorkspaceChange,
    },
    FetchWorkspaces {
        session_id: String,
    },
    SetToolRuntimeNode {
        sandbox_node_id: Option<String>,
    },
    FetchModels {
        provider: String,
    },
    FetchAgents,
    RealtimeHandoff {
        session_id: String,
        input: String,
        response_tx: Sender<Result<String, String>>,
    },
    Checkpoint(Sender<Result<(), String>>),
    Shutdown,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChatConnectionSnapshot {
    pub execution_id: String,
    pub generation: String,
    pub after_sequence: u64,
}
impl Default for ChatConnectionSnapshot {
    fn default() -> Self {
        Self {
            execution_id: format!("app-{}", sinomo_runtime::events::SessionId::generate()),
            generation: String::new(),
            after_sequence: 0,
        }
    }
}
struct ClientEvent {
    event: Option<ChatEvent>,
    cursor: Option<(String, u64)>,
}

#[derive(Clone)]
struct ChatCommandSender {
    inner: Sender<ChatCommand>,
    ready: Arc<tokio::sync::Notify>,
}

impl ChatCommandSender {
    fn send(&self, command: ChatCommand) -> Result<(), std::sync::mpsc::SendError<ChatCommand>> {
        self.inner.send(command)?;
        self.ready.notify_one();
        Ok(())
    }
}

pub struct ChatHandle {
    tx: ChatCommandSender,
    rx: Receiver<ClientEvent>,
    checkpoint: Arc<Mutex<ChatConnectionSnapshot>>,
    connected: Arc<AtomicBool>,
    runtime: Arc<tokio::runtime::Runtime>,
    #[cfg(test)]
    worker: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct ChatRealtimeHandoff {
    tx: ChatCommandSender,
    session_id: String,
}

impl ChatRealtimeHandoff {
    pub fn run(&self, input: String) -> Result<String, String> {
        let (response_tx, response_rx) = channel();
        self.tx
            .send(ChatCommand::RealtimeHandoff {
                session_id: self.session_id.clone(),
                input,
                response_tx,
            })
            .map_err(|_| "daemon chat runtime is unavailable".to_string())?;
        response_rx
            .recv_timeout(Duration::from_secs(10 * 60))
            .map_err(|_| "regular chat handoff timed out".to_string())?
    }
}

impl ChatHandle {
    pub fn spawn(
        runtime: Arc<tokio::runtime::Runtime>,
        daemon_address: Option<String>,
        event_wake: Option<ChatEventWake>,
    ) -> Self {
        Self::spawn_restored(
            runtime,
            daemon_address,
            event_wake,
            ChatConnectionSnapshot::default(),
        )
    }

    pub fn spawn_restored(
        runtime: Arc<tokio::runtime::Runtime>,
        daemon_address: Option<String>,
        event_wake: Option<ChatEventWake>,
        snapshot: ChatConnectionSnapshot,
    ) -> Self {
        let checkpoint = Arc::new(Mutex::new(snapshot));
        let worker_checkpoint = checkpoint.clone();
        let connected = Arc::new(AtomicBool::new(false));
        let worker_connected = connected.clone();
        let (command_tx, command_rx) = channel();
        let command_ready = Arc::new(tokio::sync::Notify::new());
        let command_tx = ChatCommandSender {
            inner: command_tx,
            ready: command_ready.clone(),
        };
        let worker_runtime = runtime.clone();
        let (event_tx, event_rx) = channel();
        let worker = thread::Builder::new()
            .name("sinomo-chat-client".to_string())
            .spawn(move || {
                remote_worker(
                    worker_runtime,
                    command_ready,
                    daemon_address,
                    command_rx,
                    event_tx,
                    event_wake,
                    worker_checkpoint,
                    worker_connected,
                )
            })
            .expect("spawn daemon chat client worker");
        #[cfg(not(test))]
        let _ = worker;
        Self {
            tx: command_tx,
            rx: event_rx,
            checkpoint,
            connected,
            runtime,
            #[cfg(test)]
            worker: Some(worker),
        }
    }

    pub(crate) fn runtime(&self) -> Arc<tokio::runtime::Runtime> {
        self.runtime.clone()
    }

    pub fn send_command(&self, command: ChatCommand) {
        let _ = self.tx.send(command);
    }

    pub fn realtime_handoff(&self, session_id: impl Into<String>) -> ChatRealtimeHandoff {
        ChatRealtimeHandoff {
            tx: self.tx.clone(),
            session_id: session_id.into(),
        }
    }

    pub fn drain(&self) -> Vec<ChatEvent> {
        self.rx
            .try_iter()
            .filter_map(|message| {
                if let Some((generation, after_sequence)) = message.cursor {
                    let mut checkpoint = self
                        .checkpoint
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    checkpoint.generation = generation;
                    checkpoint.after_sequence = after_sequence;
                }
                message.event
            })
            .collect()
    }
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    pub fn connection_snapshot(&self) -> ChatConnectionSnapshot {
        self.checkpoint
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    pub fn prepare_restart(&self) -> Receiver<Result<(), String>> {
        let (tx, rx) = channel();
        let _ = self.tx.send(ChatCommand::Checkpoint(tx));
        rx
    }
}

impl Drop for ChatHandle {
    fn drop(&mut self) {
        let _ = self.tx.send(ChatCommand::Shutdown);
        #[cfg(test)]
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn remote_worker(
    runtime: Arc<tokio::runtime::Runtime>,
    command_ready: Arc<tokio::sync::Notify>,
    daemon_address: Option<String>,
    command_rx: Receiver<ChatCommand>,
    event_tx: Sender<ClientEvent>,
    event_wake: Option<ChatEventWake>,
    checkpoint: Arc<Mutex<ChatConnectionSnapshot>>,
    connected: Arc<AtomicBool>,
) {
    let Some(address) = daemon_address else {
        // Backend initialization owns the user-visible connection failure. A
        // missing address means there is no remote session to open; emitting a
        // second synthetic chat error would overwrite the more precise daemon
        // startup diagnosis already held by the UI. Session reset remains a
        // local presentation transition, so acknowledge it without executing
        // any agent work while disconnected.
        while let Ok(command) = command_rx.recv() {
            match command {
                ChatCommand::NewSession { session_id } => send_event(
                    &event_tx,
                    &event_wake,
                    ChatEvent::SessionReset { session_id },
                ),
                ChatCommand::Checkpoint(tx) => {
                    let _ = tx.send(Err("daemon is disconnected".into()));
                }
                ChatCommand::UpdateWorkspaces {
                    session_id,
                    request_id,
                    ..
                } => send_event(
                    &event_tx,
                    &event_wake,
                    ChatEvent::WorkspacesUpdated {
                        session_id,
                        request_id,
                        selection: None,
                        error: Some("No daemon connection is available.".into()),
                    },
                ),
                ChatCommand::Shutdown => break,
                _ => {}
            }
        }
        return;
    };
    let client = ChatDaemonClient::at_address(address, next_client_id())
        .with_timeout(CHAT_CLIENT_RPC_TIMEOUT);
    runtime.block_on(run_connected_worker(
        client,
        command_rx,
        event_tx,
        event_wake,
        checkpoint,
        connected,
        command_ready,
    ));
}

// The actor remains the sole owner of generation, replay cursor, and stable
// mutation IDs. The transport seam also lets tests hold a real pending read
// while checking command wakeups and reconnect behavior.
trait ChatConnection: Clone + Send + Sync + 'static {
    fn attach(
        &self,
        execution: Option<String>,
    ) -> impl std::future::Future<
        Output = Result<wire::ChatSessionOpenResponse, sinomo_client::api::DaemonApiError>,
    > + Send;
    fn command_with_id(
        &self,
        command: wire::ChatSessionCommand,
        id: Option<String>,
    ) -> impl std::future::Future<
        Output = Result<wire::ChatSessionCommandResponse, sinomo_client::api::DaemonApiError>,
    > + Send;
    fn events(
        &self,
        cursor: u64,
        limit: usize,
        wait: Duration,
    ) -> impl std::future::Future<
        Output = Result<wire::ChatSessionEventsResponse, sinomo_client::api::DaemonApiError>,
    > + Send;
    fn close(
        &self,
    ) -> impl std::future::Future<
        Output = Result<wire::ChatSessionCloseResponse, sinomo_client::api::DaemonApiError>,
    > + Send;
}

struct ChatEventPoll(
    tokio::task::JoinHandle<
        Result<wire::ChatSessionEventsResponse, sinomo_client::api::DaemonApiError>,
    >,
);

impl Drop for ChatEventPoll {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl ChatConnection for ChatDaemonClient {
    async fn attach(
        &self,
        execution: Option<String>,
    ) -> Result<wire::ChatSessionOpenResponse, sinomo_client::api::DaemonApiError> {
        self.attach_async(execution).await
    }
    async fn command_with_id(
        &self,
        command: wire::ChatSessionCommand,
        id: Option<String>,
    ) -> Result<wire::ChatSessionCommandResponse, sinomo_client::api::DaemonApiError> {
        self.command_with_id_async(command, id).await
    }
    async fn events(
        &self,
        cursor: u64,
        limit: usize,
        wait: Duration,
    ) -> Result<wire::ChatSessionEventsResponse, sinomo_client::api::DaemonApiError> {
        self.events_async(cursor, limit, wait).await
    }
    async fn close(
        &self,
    ) -> Result<wire::ChatSessionCloseResponse, sinomo_client::api::DaemonApiError> {
        self.close_async().await
    }
}

async fn run_connected_worker(
    client: impl ChatConnection,
    command_rx: Receiver<ChatCommand>,
    event_tx: Sender<ClientEvent>,
    event_wake: Option<ChatEventWake>,
    checkpoint: Arc<Mutex<ChatConnectionSnapshot>>,
    connected: Arc<AtomicBool>,
    command_ready: Arc<tokio::sync::Notify>,
) {
    let saved = checkpoint
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let mut generation = saved.generation.clone();
    let mut cursor = saved.after_sequence;
    let mut opened = false;
    let stopped = Arc::new(AtomicBool::new(false));
    let mut pending = std::collections::VecDeque::new();
    let mut barriers = Vec::new();
    let mut handoffs = HashMap::<String, Sender<Result<String, String>>>::new();
    let mut request_sequence = 0u64;
    let mut event_poll = None;
    while !stopped.load(Ordering::Acquire) {
        while let Ok(command) = command_rx.try_recv() {
            if matches!(command, ChatCommand::Shutdown) {
                stopped.store(true, Ordering::Release);
                break;
            }
            if let ChatCommand::Checkpoint(tx) = command {
                barriers.push(tx);
                continue;
            }
            let (command, handoff) = wire_command(command, &mut request_sequence);
            if let Some((request_id, response_tx)) = handoff {
                handoffs.insert(request_id, response_tx);
            }
            pending.push_back((
                sinomo_runtime::events::SessionId::generate().to_string(),
                command,
            ));
        }
        if !opened {
            event_poll = None;
            match client.attach(Some(saved.execution_id.clone())).await {
                Ok(response) => {
                    if response.generation.is_empty()
                        || response.handoff_protocol != wire::CHAT_HANDOFF_PROTOCOL_VERSION
                    {
                        send_event(&event_tx, &event_wake, ChatEvent::Error { session_id: None, error: "This daemon does not support durable chat handoff. Update the runtime before using this app release.".into() });
                        break;
                    }
                    let reset = !generation.is_empty() && generation != response.generation;
                    if generation != response.generation {
                        cursor = 0;
                    }
                    generation = response.generation;
                    let _ = event_tx.send(ClientEvent {
                        event: Some(ChatEvent::Reattached { replay_gap: reset }),
                        cursor: Some((generation.clone(), cursor)),
                    });
                    if let Some(wake) = &event_wake {
                        wake();
                    }
                    opened = true;
                    connected.store(response.runtime_ready, Ordering::Release);
                }
                Err(error) => {
                    if stopped.load(Ordering::Acquire) {
                        break;
                    }
                    send_event(
                        &event_tx,
                        &event_wake,
                        ChatEvent::Error {
                            session_id: None,
                            error: format!("chat reconnecting: {error}"),
                        },
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            }
        }
        while let Some((id, command)) = pending.front() {
            match client
                .command_with_id(command.clone(), Some(id.clone()))
                .await
            {
                Ok(response) if response.reattach_required => {
                    opened = false;
                    connected.store(false, Ordering::Release);
                    break;
                }
                Ok(_) => {
                    pending.pop_front();
                }
                Err(
                    sinomo_client::api::DaemonApiError::Network { .. }
                    | sinomo_client::api::DaemonApiError::NetworkDelivery { .. }
                    | sinomo_client::api::DaemonApiError::ForwardNetwork { .. }
                    | sinomo_client::api::DaemonApiError::NotRunning { .. },
                ) => {
                    opened = false;
                    connected.store(false, Ordering::Release);
                    break;
                }
                Err(error) => {
                    if let wire::ChatSessionCommand::UpdateWorkspaces {
                        session_id,
                        request_id,
                        ..
                    } = command
                    {
                        send_event(
                            &event_tx,
                            &event_wake,
                            ChatEvent::WorkspacesUpdated {
                                session_id: session_id.clone(),
                                request_id: request_id.clone(),
                                selection: None,
                                error: Some(format!("Workspace update failed: {error}")),
                            },
                        );
                        pending.pop_front();
                        continue;
                    }
                    send_event(&event_tx, &event_wake, command_error_event(command, &error));
                    pending.pop_front();
                }
            }
        }
        if pending.is_empty() && !barriers.is_empty() {
            let detached = client.close().await.map(|_| ()).map_err(|e| e.to_string());
            for tx in barriers.drain(..) {
                let _ = tx.send(detached.clone());
            }
            if let Some(wake) = &event_wake {
                wake();
            }
            break;
        }
        if stopped.load(Ordering::Acquire) {
            break;
        }
        if !opened {
            // A failed mutation now reattaches without a timed event read in
            // between. Use the existing disconnected-read backoff so repeated
            // non-admission cannot turn that path into a hot retry loop.
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }
        // Drain the read independently even while a command awaits its reply.
        // Keeping this owned task across command wakeups avoids both head-of-line
        // backpressure and cancel/restart churn. Drop aborts it on reconnect/exit.
        let poll = event_poll.get_or_insert_with(|| {
            let client = client.clone();
            ChatEventPoll(tokio::spawn(async move {
                client.events(cursor, 256, CHAT_CLIENT_POLL_WAIT).await
            }))
        });
        let response = tokio::select! {
            response = &mut poll.0 => Some(response.unwrap_or_else(|error| Err(sinomo_client::api::DaemonApiError::Network {
                address: "chat event reader".into(), detail: error.to_string(),
            }))),
            _ = command_ready.notified() => None,
        };
        let Some(response) = response else {
            continue;
        };
        event_poll = None;
        match response {
            Ok(response) => {
                if response.generation != generation {
                    generation = response.generation;
                    cursor = 0;
                    send_event(
                        &event_tx,
                        &event_wake,
                        ChatEvent::Reattached { replay_gap: true },
                    );
                    continue;
                }
                if !response.execution_available {
                    opened = false;
                    connected.store(false, Ordering::Release);
                }
                if response.first_available_sequence > cursor.saturating_add(1) {
                    send_event(
                        &event_tx,
                        &event_wake,
                        ChatEvent::Reattached { replay_gap: true },
                    );
                }
                cursor = response.next_sequence;
                for event in response.events {
                    match event.event {
                        ChatEvent::RealtimeHandoffResult { request_id, result } => {
                            if let Some(response_tx) = handoffs.remove(&request_id) {
                                let _ = response_tx.send(result);
                            }
                        }
                        event_body => {
                            if response.execution_available
                                && matches!(event_body, ChatEvent::Ready { .. })
                            {
                                connected.store(true, Ordering::Release);
                            }
                            let _ = event_tx.send(ClientEvent {
                                event: Some(event_body),
                                cursor: Some((generation.clone(), event.sequence)),
                            });
                            if let Some(wake) = &event_wake {
                                wake();
                            }
                        }
                    }
                }
            }
            Err(error) => {
                opened = false;
                connected.store(false, Ordering::Release);
                send_event(
                    &event_tx,
                    &event_wake,
                    ChatEvent::Error {
                        session_id: None,
                        error: format!("chat event stream disconnected: {error}"),
                    },
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    connected.store(false, Ordering::Release);
    let _ = client.close().await;
    for (_, response_tx) in handoffs {
        let _ = response_tx.send(Err("chat client closed".to_string()));
    }
}

fn send_event(tx: &Sender<ClientEvent>, wake: &Option<ChatEventWake>, event: ChatEvent) {
    if tx
        .send(ClientEvent {
            event: Some(event),
            cursor: None,
        })
        .is_ok()
        && let Some(wake) = wake
    {
        wake();
    }
}

fn wire_command(
    command: ChatCommand,
    request_sequence: &mut u64,
) -> (
    wire::ChatSessionCommand,
    Option<(String, Sender<Result<String, String>>)>,
) {
    let mut handoff = None;
    let command = match command {
        ChatCommand::Send {
            session_id,
            text,
            attachments,
        } => wire::ChatSessionCommand::Send {
            session_id,
            text,
            attachments,
        },
        ChatCommand::Steer {
            session_id,
            message,
        } => wire::ChatSessionCommand::Steer {
            session_id,
            message,
        },
        ChatCommand::FollowUp {
            session_id,
            message,
        } => wire::ChatSessionCommand::FollowUp {
            session_id,
            message,
        },
        ChatCommand::RemoveQueuedInput {
            session_id,
            input_id,
        } => wire::ChatSessionCommand::RemoveQueuedInput {
            session_id,
            input_id,
        },
        ChatCommand::Interrupt { session_id, reason } => {
            wire::ChatSessionCommand::Interrupt { session_id, reason }
        }
        ChatCommand::Cancel { session_id, reason } => {
            wire::ChatSessionCommand::Cancel { session_id, reason }
        }
        ChatCommand::ControlState { session_id } => {
            wire::ChatSessionCommand::ControlState { session_id }
        }
        ChatCommand::NewSession { session_id } => {
            wire::ChatSessionCommand::NewSession { session_id }
        }
        ChatCommand::Resume {
            session_id,
            history,
        } => wire::ChatSessionCommand::Resume {
            session_id,
            history,
        },
        ChatCommand::SetModel {
            provider,
            model,
            account,
        } => wire::ChatSessionCommand::SetModel {
            provider,
            model,
            account,
        },
        ChatCommand::SetAgent {
            session_id,
            key,
            history,
        } => wire::ChatSessionCommand::SetAgent {
            session_id,
            key,
            history,
        },
        ChatCommand::SetToolRuntime { selection } => {
            wire::ChatSessionCommand::SetToolRuntime { selection }
        }
        ChatCommand::SetWorkspace {
            session_id,
            workspace_root,
        } => wire::ChatSessionCommand::SetWorkspace {
            session_id,
            workspace_root,
        },
        ChatCommand::UpdateWorkspaces {
            session_id,
            request_id,
            change,
        } => wire::ChatSessionCommand::UpdateWorkspaces {
            session_id,
            request_id,
            change,
        },
        ChatCommand::FetchWorkspaces { session_id } => {
            wire::ChatSessionCommand::FetchWorkspaces { session_id }
        }
        ChatCommand::SetToolRuntimeNode { sandbox_node_id } => {
            wire::ChatSessionCommand::SetToolRuntimeNode { sandbox_node_id }
        }
        ChatCommand::FetchModels { provider } => wire::ChatSessionCommand::FetchModels { provider },
        ChatCommand::FetchAgents => wire::ChatSessionCommand::FetchAgents,
        ChatCommand::RealtimeHandoff {
            session_id,
            input,
            response_tx,
        } => {
            let request_id = format!("handoff-{}", *request_sequence);
            *request_sequence = request_sequence.wrapping_add(1);
            handoff = Some((request_id.clone(), response_tx));
            wire::ChatSessionCommand::RealtimeHandoff {
                request_id,
                session_id,
                input,
            }
        }
        ChatCommand::Checkpoint(_) | ChatCommand::Shutdown => {
            unreachable!("shutdown handled before wire conversion")
        }
    };
    (command, handoff)
}

fn command_error_event(
    command: &wire::ChatSessionCommand,
    error: &sinomo_client::api::DaemonApiError,
) -> ChatEvent {
    if let wire::ChatSessionCommand::ControlState { session_id } = command {
        tracing::warn!(%session_id, %error, "chat runtime control refresh failed");
        // An observational control query must not interrupt the conversation or
        // become a persisted turn error when an idle session has no live host.
        return ChatEvent::Notice {
            session_id: session_id.clone(),
            notice: "Runtime controls could not be refreshed.".into(),
        };
    }
    ChatEvent::Error {
        session_id: None,
        error: format!("chat command failed: {error}"),
    }
}

fn next_client_id() -> String {
    let sequence = CHAT_CLIENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("app-chat-{}-{sequence}", std::process::id())
}

#[cfg(test)]
mod wait_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejected_control_query_is_a_notice_instead_of_a_turn_error() {
        let event = command_error_event(
            &wire::ChatSessionCommand::ControlState {
                session_id: "empty-chat".into(),
            },
            &sinomo_client::api::DaemonApiError::RemoteService {
                route: wire::CHAT_SESSION_COMMAND_ROUTE.into(),
                detail: "agent session is not registered with this runtime host".into(),
            },
        );
        assert!(matches!(event, ChatEvent::Notice { session_id, notice }
            if session_id == "empty-chat" && notice == "Runtime controls could not be refreshed."));
    }
}
