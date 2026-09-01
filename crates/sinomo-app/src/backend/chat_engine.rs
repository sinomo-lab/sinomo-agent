//! Thin client actor for daemon-hosted chat execution.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;
#[cfg(test)]
use std::thread::JoinHandle;
use std::time::Duration;

use sinomo_client::ChatDaemonClient;
use sinomo_client::chat_contract as wire;

pub use wire::{
    AgentSummary, ChatAttachmentInput, ChatAttachmentSource, ChatContextLane, ChatContextSnapshot,
    ChatControlState, ChatControlStatus, ChatEvent, ChatHistoryMessage,
    ChatLocalAttachmentProvenance, ChatProcessSnapshot, ChatQueuedTurnInput,
    ChatQueuedTurnInputKind, ChatReattachState, ChatSifsAttachmentProvenance, ChatSpeaker,
    ChatTaskSnapshot, ChatToolCallSnapshot, ChatToolRuntimeSelection, ChatUsageSnapshot,
    ProviderCapabilities, ProviderSummary, RuntimeInfo,
};

pub type ChatEventWake = Arc<dyn Fn() + Send + Sync + 'static>;
const CHAT_CLIENT_POLL_WAIT: Duration = Duration::from_millis(100);
const CHAT_CLIENT_RPC_TIMEOUT: Duration = Duration::from_secs(10);
static CHAT_CLIENT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    Interrupt {
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
    Shutdown,
}

pub struct ChatHandle {
    tx: Sender<ChatCommand>,
    rx: Receiver<ChatEvent>,
    #[cfg(test)]
    worker: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct ChatRealtimeHandoff {
    tx: Sender<ChatCommand>,
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
    pub fn spawn(daemon_address: Option<String>, event_wake: Option<ChatEventWake>) -> Self {
        let (command_tx, command_rx) = channel();
        let (event_tx, event_rx) = channel();
        let worker = thread::Builder::new()
            .name("sinomo-chat-client".to_string())
            .spawn(move || remote_worker(daemon_address, command_rx, event_tx, event_wake))
            .expect("spawn daemon chat client worker");
        #[cfg(not(test))]
        let _ = worker;
        Self {
            tx: command_tx,
            rx: event_rx,
            #[cfg(test)]
            worker: Some(worker),
        }
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
        self.rx.try_iter().collect()
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
    daemon_address: Option<String>,
    command_rx: Receiver<ChatCommand>,
    event_tx: Sender<ChatEvent>,
    event_wake: Option<ChatEventWake>,
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
                ChatCommand::Shutdown => break,
                _ => {}
            }
        }
        return;
    };
    let client = ChatDaemonClient::at_address(address, next_client_id())
        .with_timeout(CHAT_CLIENT_RPC_TIMEOUT);
    if let Err(error) = client.open() {
        send_event(
            &event_tx,
            &event_wake,
            ChatEvent::Error {
                session_id: None,
                error: format!("chat unavailable: {error}"),
            },
        );
        return;
    }

    let stopped = Arc::new(AtomicBool::new(false));
    let mut cursor = 0u64;
    let mut handoffs = HashMap::<String, Sender<Result<String, String>>>::new();
    let mut request_sequence = 0u64;
    while !stopped.load(Ordering::Acquire) {
        while let Ok(command) = command_rx.try_recv() {
            if matches!(command, ChatCommand::Shutdown) {
                stopped.store(true, Ordering::Release);
                break;
            }
            let (command, handoff) = wire_command(command, &mut request_sequence);
            if let Some((request_id, response_tx)) = handoff {
                handoffs.insert(request_id, response_tx);
            }
            if let Err(error) = client.command(command) {
                send_event(
                    &event_tx,
                    &event_wake,
                    ChatEvent::Error {
                        session_id: None,
                        error: format!("chat command failed: {error}"),
                    },
                );
            }
        }
        if stopped.load(Ordering::Acquire) {
            break;
        }
        match client.events(cursor, 256, CHAT_CLIENT_POLL_WAIT) {
            Ok(response) => {
                cursor = response.next_sequence;
                for event in response.events {
                    match event.event {
                        ChatEvent::RealtimeHandoffResult { request_id, result } => {
                            if let Some(response_tx) = handoffs.remove(&request_id) {
                                let _ = response_tx.send(result);
                            }
                        }
                        event => send_event(&event_tx, &event_wake, event),
                    }
                }
            }
            Err(error) => {
                send_event(
                    &event_tx,
                    &event_wake,
                    ChatEvent::Error {
                        session_id: None,
                        error: format!("chat event stream disconnected: {error}"),
                    },
                );
                thread::sleep(Duration::from_millis(250));
            }
        }
    }
    let _ = client.close();
    for (_, response_tx) in handoffs {
        let _ = response_tx.send(Err("chat client closed".to_string()));
    }
}

fn send_event(tx: &Sender<ChatEvent>, wake: &Option<ChatEventWake>, event: ChatEvent) {
    if tx.send(event).is_ok()
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
        ChatCommand::Shutdown => unreachable!("shutdown handled before wire conversion"),
    };
    (command, handoff)
}

fn next_client_id() -> String {
    let sequence = CHAT_CLIENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("app-chat-{}-{sequence}", std::process::id())
}
