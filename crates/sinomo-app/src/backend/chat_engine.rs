//! Chat actor.
//!
//! The agent interaction runtime is stateful and its calls block while the model
//! streams. To keep the UI thread responsive we run it on a dedicated worker
//! thread (constructed *inside* that thread, so nothing non-`Send` ever crosses a
//! boundary). The UI sends [`ChatCommand`]s and drains [`ChatEvent`]s each frame.

use std::str::FromStr;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread;
#[cfg(test)]
use std::thread::JoinHandle;

use sinomo_agent::{
    AgentRuntimeControlHost, AgentRuntimeControlRegistry, AgentSessionControlResult,
    AgentSessionFollowUpRequest, AgentSessionInterruptRequest,
    AgentSessionRemoveQueuedInputRequest, AgentSessionResumeRequest, AgentSessionResumeResponse,
    AgentSessionSteerRequest, AgentTaskLifecycleStatus, AgentTaskSnapshot, TaskId,
};
use sinomo_cli::app_api::{
    AgentChat, AgentChatSifsStorage, ChatAttachmentInput, ChatContextSnapshot, ChatControlState,
    ChatControlStatus, ChatHistoryMessage, ChatProcessSnapshot, ChatRuntimeInfo, ChatSpeaker,
    ChatStreamEvent, ChatTaskSnapshot, ChatToolCallSnapshot, ChatToolRuntimeSelection,
    ChatUsageSnapshot, chat_queued_input_from_events, chat_queued_inputs_from_session,
    chat_removed_input_id_from_events,
};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;

pub type ChatEventWake = Arc<dyn Fn() + Send + Sync + 'static>;

#[derive(Clone)]
struct ChatEventSink {
    tx: Sender<ChatEvent>,
    wake: Option<ChatEventWake>,
}

impl ChatEventSink {
    fn new(tx: Sender<ChatEvent>, wake: Option<ChatEventWake>) -> Self {
        Self { tx, wake }
    }

    fn send(&self, event: ChatEvent) {
        if self.tx.send(event).is_ok() {
            if let Some(wake) = &self.wake {
                wake();
            }
        }
    }
}

/// Who authored a chat message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChatRole {
    User,
    Agent,
}

/// Plain selection info shown in the composer.
#[derive(Clone, Debug, Default)]
pub struct RuntimeInfo {
    pub agent: String,
    pub provider: String,
    pub model: String,
    pub account: Option<String>,
    pub effort: Option<String>,
}

impl From<ChatRuntimeInfo> for RuntimeInfo {
    fn from(info: ChatRuntimeInfo) -> Self {
        RuntimeInfo {
            agent: info.agent,
            provider: info.provider,
            model: info.model,
            account: info.account,
            effort: info.reasoning_effort,
        }
    }
}

/// Commands the UI issues to the chat worker.
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
    /// Remove a queued steering/follow-up message (by queue id) before the
    /// runtime consumes it.
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
        sandbox_node_id: Option<String>,
    },
    /// Ask sinomo-lm for the provider's available models (fetched off the worker so the
    /// blocking API call never stalls chat turns); the result arrives as [`ChatEvent::Models`].
    FetchModels {
        provider: String,
    },
    Shutdown,
}

/// Events the worker streams back to the UI.
#[derive(Clone, Debug)]
pub enum ChatEvent {
    Ready {
        session_id: String,
        info: RuntimeInfo,
    },
    Token {
        session_id: String,
        text: String,
    },
    AgentToken {
        session_id: String,
        speaker: ChatSpeaker,
        text: String,
    },
    Notice {
        session_id: String,
        notice: String,
    },
    SystemMessage {
        session_id: String,
        kind: String,
        text: String,
    },
    Context {
        session_id: String,
        context: ChatContextSnapshot,
    },
    ToolCallStarted {
        session_id: String,
        call: ChatToolCallSnapshot,
    },
    ToolCallDelta {
        session_id: String,
        step_index: usize,
        call_id: Option<String>,
        tool: Option<String>,
        arguments_delta: String,
    },
    ToolCallCompleted {
        session_id: String,
        call: ChatToolCallSnapshot,
    },
    Process {
        session_id: String,
        process: ChatProcessSnapshot,
    },
    Usage {
        session_id: String,
        usage: ChatUsageSnapshot,
    },
    TurnDone {
        session_id: String,
    },
    ControlStatus(ChatControlStatus),
    ControlState(ChatControlState),
    RuntimeInfo(RuntimeInfo),
    ToolRuntime(ChatToolRuntimeSelection),
    SessionReset {
        session_id: String,
    },
    /// Available models for `provider`, fetched from sinomo-lm (empty on failure).
    Models {
        provider: String,
        models: Vec<String>,
    },
    Error {
        session_id: Option<String>,
        error: String,
    },
}

impl ChatEvent {
    pub fn session_id(&self) -> Option<&str> {
        match self {
            ChatEvent::Token { session_id, .. }
            | ChatEvent::AgentToken { session_id, .. }
            | ChatEvent::Notice { session_id, .. }
            | ChatEvent::SystemMessage { session_id, .. }
            | ChatEvent::Context { session_id, .. }
            | ChatEvent::ToolCallStarted { session_id, .. }
            | ChatEvent::ToolCallDelta { session_id, .. }
            | ChatEvent::ToolCallCompleted { session_id, .. }
            | ChatEvent::Process { session_id, .. }
            | ChatEvent::Usage { session_id, .. }
            | ChatEvent::TurnDone { session_id }
            | ChatEvent::SessionReset { session_id } => Some(session_id),
            ChatEvent::ControlStatus(status) => Some(&status.session_id),
            ChatEvent::ControlState(state) => Some(&state.session_id),
            ChatEvent::Error {
                session_id: Some(session_id),
                ..
            } => Some(session_id),
            ChatEvent::Ready { .. }
            | ChatEvent::RuntimeInfo(_)
            | ChatEvent::ToolRuntime(_)
            | ChatEvent::Models { .. }
            | ChatEvent::Error {
                session_id: None, ..
            } => None,
        }
    }
}

/// Handle to the chat worker thread.
pub struct ChatHandle {
    tx: Sender<ChatCommand>,
    rx: Receiver<ChatEvent>,
    direct_control: DirectControl,
    #[cfg(test)]
    worker: Option<JoinHandle<()>>,
}

impl ChatHandle {
    /// Spawn the worker. The runtime is built inside the thread.
    pub fn spawn(
        data_dir: SinomoDataDir,
        daemon_connected: bool,
        runtime_control_registry: AgentRuntimeControlRegistry,
        sifs_storage: Option<AgentChatSifsStorage>,
        event_wake: Option<ChatEventWake>,
    ) -> Self {
        let (cmd_tx, cmd_rx) = channel::<ChatCommand>();
        let (evt_tx, evt_rx) = channel::<ChatEvent>();
        let evt_tx = ChatEventSink::new(evt_tx, event_wake);
        let active_session_id = Arc::new(Mutex::new(None));
        let direct_control = DirectControl {
            runtime_control_registry: runtime_control_registry.clone(),
            active_session_id: active_session_id.clone(),
            evt_tx: evt_tx.clone(),
        };
        let worker = thread::Builder::new()
            .name("sinomo-chat".to_string())
            .spawn(move || {
                worker(
                    data_dir,
                    daemon_connected,
                    runtime_control_registry,
                    sifs_storage,
                    active_session_id,
                    cmd_rx,
                    evt_tx,
                )
            })
            .expect("spawn chat worker");
        #[cfg(not(test))]
        let _ = worker;
        Self {
            tx: cmd_tx,
            rx: evt_rx,
            direct_control,
            #[cfg(test)]
            worker: Some(worker),
        }
    }

    pub fn send_command(&self, command: ChatCommand) {
        match command {
            ChatCommand::Steer {
                session_id,
                message,
            } => self.direct_control.steer(session_id, message),
            ChatCommand::FollowUp {
                session_id,
                message,
            } => self.direct_control.follow_up(session_id, message),
            ChatCommand::RemoveQueuedInput {
                session_id,
                input_id,
            } => self
                .direct_control
                .remove_queued_input(session_id, input_id),
            ChatCommand::Interrupt { session_id, reason } => {
                self.direct_control.interrupt(session_id, reason)
            }
            ChatCommand::ControlState { session_id } => {
                self.direct_control.control_state(session_id)
            }
            command => {
                let _ = self.tx.send(command);
            }
        }
    }

    /// Drain all currently-available events without blocking.
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

#[derive(Clone)]
struct DirectControl {
    runtime_control_registry: AgentRuntimeControlRegistry,
    active_session_id: Arc<Mutex<Option<SessionId>>>,
    evt_tx: ChatEventSink,
}

impl DirectControl {
    fn steer(&self, requested_session_id: String, message: String) {
        self.spawn(
            "steer",
            requested_session_id,
            move |registry, session_id| {
                registry
                    .steer(AgentSessionSteerRequest {
                        session_id,
                        message,
                    })
                    .map(|result| ChatEvent::ControlStatus(chat_control_status(result)))
            },
        );
    }

    fn follow_up(&self, requested_session_id: String, message: String) {
        self.spawn(
            "follow-up",
            requested_session_id,
            move |registry, session_id| {
                registry
                    .follow_up(AgentSessionFollowUpRequest {
                        session_id,
                        message,
                    })
                    .map(|result| ChatEvent::ControlStatus(chat_control_status(result)))
            },
        );
    }

    fn remove_queued_input(&self, requested_session_id: String, input_id: String) {
        let input_id = match input_id.parse::<TaskId>() {
            Ok(input_id) => input_id,
            Err(error) => {
                self.evt_tx.send(ChatEvent::Error {
                    session_id: Some(requested_session_id),
                    error: format!("remove queued input failed: invalid input id: {error}"),
                });
                return;
            }
        };
        self.spawn(
            "remove queued input",
            requested_session_id,
            move |registry, session_id| {
                registry
                    .remove_queued_input(AgentSessionRemoveQueuedInputRequest {
                        session_id,
                        input_id,
                    })
                    .map(|result| ChatEvent::ControlStatus(chat_control_status(result)))
            },
        );
    }

    fn interrupt(&self, requested_session_id: String, reason: String) {
        self.spawn(
            "interrupt",
            requested_session_id,
            move |registry, session_id| {
                registry
                    .interrupt(AgentSessionInterruptRequest { session_id, reason })
                    .map(|result| ChatEvent::ControlStatus(chat_control_status(result)))
            },
        );
    }

    fn control_state(&self, requested_session_id: String) {
        self.spawn(
            "control state",
            requested_session_id,
            move |registry, session_id| {
                registry
                    .resume(AgentSessionResumeRequest { session_id })
                    .map(|response| ChatEvent::ControlState(chat_control_state(response)))
            },
        );
    }

    fn spawn(
        &self,
        label: &'static str,
        requested_session_id: String,
        action: impl FnOnce(
            AgentRuntimeControlRegistry,
            SessionId,
        ) -> Result<ChatEvent, sinomo_agent::AgentRuntimeControlError>
        + Send
        + 'static,
    ) {
        let registry = self.runtime_control_registry.clone();
        let active_session_id = self.active_session_id.clone();
        let tx = self.evt_tx.clone();
        thread::Builder::new()
            .name("sinomo-chat-control".to_string())
            .spawn(move || {
                let session_id = if requested_session_id.trim().is_empty() {
                    match active_session_id.lock() {
                        Ok(guard) => guard.clone(),
                        Err(_) => {
                            let _ = tx.send(ChatEvent::Error {
                                session_id: None,
                                error: format!("{label} failed: control lock poisoned"),
                            });
                            return;
                        }
                    }
                } else {
                    match SessionId::from_str(&requested_session_id) {
                        Ok(session_id) => Some(session_id),
                        Err(error) => {
                            let _ = tx.send(ChatEvent::Error {
                                session_id: Some(requested_session_id),
                                error: format!("{label} failed: invalid session id: {error}"),
                            });
                            return;
                        }
                    }
                };
                let Some(session_id) = session_id else {
                    let _ = tx.send(ChatEvent::Error {
                        session_id: None,
                        error: format!("{label} failed: no active chat session"),
                    });
                    return;
                };
                let event_session_id = session_id.to_string();
                match action(registry, session_id) {
                    Ok(event) => {
                        let _ = tx.send(event);
                    }
                    Err(error) => {
                        let _ = tx.send(ChatEvent::Error {
                            session_id: Some(event_session_id),
                            error: format!("{label} failed: agent runtime control: {error}"),
                        });
                    }
                }
            })
            .ok();
    }
}

fn set_active_session_id(
    active_session_id: &Arc<Mutex<Option<SessionId>>>,
    evt_tx: &ChatEventSink,
    session_id: impl AsRef<str>,
) {
    match SessionId::from_str(session_id.as_ref()) {
        Ok(session_id) => {
            if let Ok(mut active) = active_session_id.lock() {
                *active = Some(session_id);
            } else {
                let _ = evt_tx.send(ChatEvent::Error {
                    session_id: None,
                    error: "control state failed: control lock poisoned".to_string(),
                });
            }
        }
        Err(error) => {
            let _ = evt_tx.send(ChatEvent::Error {
                session_id: Some(session_id.as_ref().to_string()),
                error: format!("control state failed: invalid session id: {error}"),
            });
        }
    }
}

fn chat_control_status(result: AgentSessionControlResult) -> ChatControlStatus {
    ChatControlStatus {
        session_id: result.snapshot.session_id.to_string(),
        accepted: result.accepted,
        runtime_attached: true,
        sequence: result.sequence,
        event_count: result.events.len(),
        active_turn: result.snapshot.active_turn.is_some(),
        task_count: result.snapshot.tasks.len(),
        terminal_result_count: result.terminal_results().len(),
        tasks: result
            .snapshot
            .tasks
            .iter()
            .map(chat_task_snapshot)
            .collect(),
        queued_input: chat_queued_input_from_events(&result.events),
        removed_input_id: chat_removed_input_id_from_events(&result.events),
    }
}

fn chat_control_state(response: AgentSessionResumeResponse) -> ChatControlState {
    let (active_turn, next_sequence, task_count) = response
        .control_snapshot
        .as_ref()
        .map(|snapshot| {
            (
                snapshot.active_turn.is_some(),
                snapshot.next_sequence,
                snapshot.tasks.len(),
            )
        })
        .unwrap_or((false, 0, 0));
    let (steering_count, follow_up_count) = response
        .session
        .as_ref()
        .map(|session| (session.steering_queue.len(), session.follow_up_queue.len()))
        .unwrap_or((0, 0));
    let queued_inputs = response
        .session
        .as_ref()
        .map(chat_queued_inputs_from_session)
        .unwrap_or_default();
    ChatControlState {
        session_id: response.session_id.to_string(),
        accepted: response.accepted,
        runtime_attached: response.runtime_attached,
        reattach_state: response.reattach.state.clone(),
        reattach_active_turn_persisted: response.reattach.active_turn_persisted,
        reattach_running_task_count: response.reattach.running_task_count,
        reattach_persisted_event_count: response.reattach.persisted_event_count,
        active_turn,
        next_sequence,
        task_count,
        tasks: response
            .control_snapshot
            .as_ref()
            .map(|snapshot| snapshot.tasks.iter().map(chat_task_snapshot).collect())
            .unwrap_or_default(),
        event_cursor_offset: response.event_cursor.offset,
        steering_count,
        follow_up_count,
        queued_inputs,
        detail: response.detail,
    }
}

fn chat_task_snapshot(task: &AgentTaskSnapshot) -> ChatTaskSnapshot {
    ChatTaskSnapshot {
        task_id: task.task.task_id.to_string(),
        kind: task.task.kind.clone(),
        summary: task.task.summary.clone(),
        status: chat_task_status(&task.status).to_string(),
        progress_summary: task.progress_summary.clone(),
        result_ref: task.result_ref.clone(),
        active_op_id: task.active_op_id.as_ref().map(ToString::to_string),
    }
}

fn chat_task_status(status: &AgentTaskLifecycleStatus) -> &'static str {
    match status {
        AgentTaskLifecycleStatus::Pending => "pending",
        AgentTaskLifecycleStatus::Running => "running",
        AgentTaskLifecycleStatus::Completed => "completed",
        AgentTaskLifecycleStatus::Failed => "failed",
        AgentTaskLifecycleStatus::Cancelled => "cancelled",
        AgentTaskLifecycleStatus::TimedOut => "timed-out",
    }
}

fn worker(
    data_dir: SinomoDataDir,
    daemon_connected: bool,
    runtime_control_registry: AgentRuntimeControlRegistry,
    sifs_storage: Option<AgentChatSifsStorage>,
    active_session_id: Arc<Mutex<Option<SessionId>>>,
    cmd_rx: Receiver<ChatCommand>,
    evt_tx: ChatEventSink,
) {
    let chat_result = match sifs_storage {
        Some(storage) => AgentChat::with_runtime_control_registry_and_sifs(
            data_dir,
            daemon_connected,
            runtime_control_registry,
            storage,
        ),
        None => AgentChat::with_runtime_control_registry(
            data_dir,
            daemon_connected,
            runtime_control_registry,
        ),
    };
    let mut chat = match chat_result {
        Ok(chat) => chat,
        Err(error) => {
            let _ = evt_tx.send(ChatEvent::Error {
                session_id: None,
                error: format!("chat unavailable: {error}"),
            });
            return;
        }
    };
    set_active_session_id(&active_session_id, &evt_tx, chat.session_id());

    let _ = evt_tx.send(ChatEvent::Ready {
        session_id: chat.session_id(),
        info: chat.runtime_info().into(),
    });
    let _ = evt_tx.send(ChatEvent::ToolRuntime(chat.tool_runtime()));

    while let Ok(command) = cmd_rx.recv() {
        match command {
            ChatCommand::Send {
                session_id,
                text,
                attachments,
            } => {
                let session_id = if session_id.trim().is_empty() {
                    chat.session_id()
                } else if chat.session_id() == session_id {
                    session_id
                } else {
                    match chat.resume_session(&session_id, &[]) {
                        Ok(()) => {
                            set_active_session_id(&active_session_id, &evt_tx, &session_id);
                            session_id
                        }
                        Err(error) => {
                            let _ = evt_tx.send(ChatEvent::Error {
                                session_id: Some(session_id),
                                error: format!("resume failed before send: {error}"),
                            });
                            continue;
                        }
                    }
                };
                let tx = evt_tx.clone();
                let event_session_id = session_id.clone();
                let result = chat.send_with_attachments(&text, &attachments, &mut |event| {
                    let mapped = match event {
                        ChatStreamEvent::Token(text) => Some(ChatEvent::Token {
                            session_id: event_session_id.clone(),
                            text,
                        }),
                        ChatStreamEvent::AgentToken { speaker, text } => {
                            Some(ChatEvent::AgentToken {
                                session_id: speaker.channel_id.clone(),
                                speaker,
                                text,
                            })
                        }
                        ChatStreamEvent::Notice(notice) if !notice.is_empty() => {
                            Some(ChatEvent::Notice {
                                session_id: event_session_id.clone(),
                                notice,
                            })
                        }
                        ChatStreamEvent::ChannelSystem {
                            channel_id,
                            kind,
                            text,
                            ..
                        } => Some(ChatEvent::SystemMessage {
                            session_id: channel_id,
                            kind,
                            text,
                        }),
                        ChatStreamEvent::Context(context) => Some(ChatEvent::Context {
                            session_id: event_session_id.clone(),
                            context,
                        }),
                        ChatStreamEvent::ToolCallStarted(call) => {
                            Some(ChatEvent::ToolCallStarted {
                                session_id: event_session_id.clone(),
                                call,
                            })
                        }
                        ChatStreamEvent::ToolCallDelta {
                            step_index,
                            call_id,
                            tool,
                            arguments_delta,
                        } => Some(ChatEvent::ToolCallDelta {
                            session_id: event_session_id.clone(),
                            step_index,
                            call_id,
                            tool,
                            arguments_delta,
                        }),
                        ChatStreamEvent::ToolCallCompleted(call) => {
                            Some(ChatEvent::ToolCallCompleted {
                                session_id: event_session_id.clone(),
                                call,
                            })
                        }
                        ChatStreamEvent::Process(process) => Some(ChatEvent::Process {
                            session_id: event_session_id.clone(),
                            process,
                        }),
                        ChatStreamEvent::Usage(usage) => Some(ChatEvent::Usage {
                            session_id: event_session_id.clone(),
                            usage,
                        }),
                        ChatStreamEvent::Notice(_) => None,
                    };
                    if let Some(event) = mapped {
                        let _ = tx.send(event);
                    }
                });
                if let Err(error) = result {
                    let _ = evt_tx.send(ChatEvent::Error {
                        session_id: Some(session_id.clone()),
                        error: format!("turn failed: {error}"),
                    });
                }
                let _ = evt_tx.send(ChatEvent::TurnDone { session_id });
            }
            ChatCommand::Steer { message, .. } => match chat.steer(message) {
                Ok(status) => {
                    let _ = evt_tx.send(ChatEvent::ControlStatus(status));
                }
                Err(error) => {
                    let _ = evt_tx.send(ChatEvent::Error {
                        session_id: Some(chat.session_id()),
                        error: format!("steer failed: {error}"),
                    });
                }
            },
            ChatCommand::FollowUp { message, .. } => match chat.follow_up(message) {
                Ok(status) => {
                    let _ = evt_tx.send(ChatEvent::ControlStatus(status));
                }
                Err(error) => {
                    let _ = evt_tx.send(ChatEvent::Error {
                        session_id: Some(chat.session_id()),
                        error: format!("follow-up failed: {error}"),
                    });
                }
            },
            ChatCommand::RemoveQueuedInput { input_id, .. } => {
                match input_id
                    .parse::<TaskId>()
                    .map_err(|error| format!("invalid input id: {error}"))
                    .and_then(|input_id| {
                        chat.remove_queued_input(input_id)
                            .map_err(|error| error.to_string())
                    }) {
                    Ok(status) => {
                        let _ = evt_tx.send(ChatEvent::ControlStatus(status));
                    }
                    Err(error) => {
                        let _ = evt_tx.send(ChatEvent::Error {
                            session_id: Some(chat.session_id()),
                            error: format!("remove queued input failed: {error}"),
                        });
                    }
                }
            }
            ChatCommand::Interrupt { reason, .. } => match chat.interrupt(reason) {
                Ok(status) => {
                    let _ = evt_tx.send(ChatEvent::ControlStatus(status));
                }
                Err(error) => {
                    let _ = evt_tx.send(ChatEvent::Error {
                        session_id: Some(chat.session_id()),
                        error: format!("interrupt failed: {error}"),
                    });
                }
            },
            ChatCommand::ControlState { .. } => match chat.control_state() {
                Ok(state) => {
                    let _ = evt_tx.send(ChatEvent::ControlState(state));
                }
                Err(error) => {
                    let _ = evt_tx.send(ChatEvent::Error {
                        session_id: Some(chat.session_id()),
                        error: format!("control state failed: {error}"),
                    });
                }
            },
            ChatCommand::NewSession { session_id } => {
                if let Err(error) = chat.resume_session(&session_id, &[]) {
                    let _ = evt_tx.send(ChatEvent::Error {
                        session_id: Some(session_id),
                        error: format!("new session failed: {error}"),
                    });
                } else {
                    set_active_session_id(&active_session_id, &evt_tx, &session_id);
                    let _ = evt_tx.send(ChatEvent::SessionReset { session_id });
                }
            }
            ChatCommand::Resume {
                session_id,
                history,
            } => {
                if let Err(error) = chat.resume_session(&session_id, &history) {
                    let _ = evt_tx.send(ChatEvent::Error {
                        session_id: Some(session_id),
                        error: format!("resume failed: {error}"),
                    });
                } else {
                    set_active_session_id(&active_session_id, &evt_tx, session_id);
                }
            }
            ChatCommand::SetModel {
                provider,
                model,
                account,
            } => match chat.set_model(provider, model, account) {
                Ok(info) => {
                    let _ = evt_tx.send(ChatEvent::RuntimeInfo(info.into()));
                }
                Err(error) => {
                    let _ = evt_tx.send(ChatEvent::Error {
                        session_id: Some(chat.session_id()),
                        error: format!("model switch failed: {error}"),
                    });
                }
            },
            ChatCommand::SetAgent {
                session_id,
                key,
                history,
            } => {
                let resume_result = if session_id.trim().is_empty() {
                    Ok(())
                } else {
                    chat.resume_session(&session_id, &history)
                };
                match resume_result.and_then(|_| chat.set_agent(&key)) {
                    Ok(info) => {
                        if !session_id.trim().is_empty() {
                            set_active_session_id(&active_session_id, &evt_tx, &session_id);
                        }
                        let _ = evt_tx.send(ChatEvent::RuntimeInfo(info.into()));
                        let _ = evt_tx.send(ChatEvent::ToolRuntime(chat.tool_runtime()));
                    }
                    Err(error) => {
                        let _ = evt_tx.send(ChatEvent::Error {
                            session_id: Some(if session_id.trim().is_empty() {
                                chat.session_id()
                            } else {
                                session_id
                            }),
                            error: format!("agent switch failed: {error}"),
                        });
                    }
                }
            }
            ChatCommand::SetToolRuntime { sandbox_node_id } => {
                match chat.set_tool_runtime_node(sandbox_node_id) {
                    Ok(selection) => {
                        let _ = evt_tx.send(ChatEvent::ToolRuntime(selection));
                    }
                    Err(error) => {
                        let _ = evt_tx.send(ChatEvent::Error {
                            session_id: Some(chat.session_id()),
                            error: format!("tool runtime update failed: {error}"),
                        });
                    }
                }
            }
            ChatCommand::FetchModels { provider } => {
                // Fetch off-thread: the provider API call blocks, and chat turns must not.
                let tx = evt_tx.clone();
                thread::Builder::new()
                    .name("sinomo-models".to_string())
                    .spawn(move || {
                        let models =
                            sinomo_lm::provider_available_model_ids(&provider).unwrap_or_default();
                        let _ = tx.send(ChatEvent::Models { provider, models });
                    })
                    .ok();
            }
            ChatCommand::Shutdown => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    #[test]
    fn chat_worker_routes_runtime_control_commands() {
        let temp = TempDataDir::new("chat-worker-control");
        let runtime_control = AgentRuntimeControlRegistry::new();
        let handle = ChatHandle::spawn(
            temp.data_dir.clone(),
            false,
            runtime_control.clone(),
            None,
            None,
        );
        let ready = wait_for_event(&handle, |event| matches!(event, ChatEvent::Ready { .. }))
            .expect("ready event");
        let ChatEvent::Ready { session_id, .. } = ready else {
            unreachable!("matched ready");
        };
        assert_eq!(
            runtime_control
                .session_count()
                .expect("runtime control session count"),
            1
        );

        handle.send_command(ChatCommand::ControlState {
            session_id: session_id.clone(),
        });
        let initial = wait_for_event(&handle, |event| {
            matches!(event, ChatEvent::ControlState(state) if state.session_id == session_id)
        })
        .expect("initial control state");
        assert!(matches!(
            initial,
            ChatEvent::ControlState(state)
                if state.steering_count == 0 && state.follow_up_count == 0
        ));

        handle.send_command(ChatCommand::Steer {
            session_id: session_id.clone(),
            message: "keep this constraint".to_string(),
        });
        let steer = wait_for_event(&handle, |event| {
            matches!(event, ChatEvent::ControlStatus(status) if status.session_id == session_id)
        })
        .expect("steer status");
        let ChatEvent::ControlStatus(steer_status) = steer else {
            panic!("steer wait returned a non-status event");
        };
        assert!(steer_status.accepted);
        let queued_input = steer_status
            .queued_input
            .expect("steer status carries the runtime queue id");
        assert_eq!(queued_input.message, "keep this constraint");

        handle.send_command(ChatCommand::FollowUp {
            session_id: session_id.clone(),
            message: "also mention tests".to_string(),
        });
        let follow_up = wait_for_event(&handle, |event| {
            matches!(event, ChatEvent::ControlStatus(status) if status.session_id == session_id)
        })
        .expect("follow-up status");
        assert!(matches!(
            follow_up,
            ChatEvent::ControlStatus(status) if status.accepted && status.runtime_attached
        ));

        handle.send_command(ChatCommand::ControlState {
            session_id: session_id.clone(),
        });
        let state = wait_for_event(&handle, |event| {
            matches!(
                event,
                ChatEvent::ControlState(state)
                    if state.session_id == session_id
                        && state.steering_count == 1
                        && state.follow_up_count == 1
                        && state.queued_inputs.len() == 2
            )
        })
        .expect("updated control state");
        assert!(matches!(state, ChatEvent::ControlState(_)));

        // The queued steering message is removable by its runtime id; the
        // follow-up stays queued.
        handle.send_command(ChatCommand::RemoveQueuedInput {
            session_id: session_id.clone(),
            input_id: queued_input.input_id.clone(),
        });
        let removed = wait_for_event(&handle, |event| {
            matches!(
                event,
                ChatEvent::ControlStatus(status)
                    if status.removed_input_id.as_deref() == Some(queued_input.input_id.as_str())
            )
        })
        .expect("remove status");
        assert!(matches!(
            removed,
            ChatEvent::ControlStatus(status) if status.accepted
        ));

        handle.send_command(ChatCommand::ControlState {
            session_id: session_id.clone(),
        });
        wait_for_event(&handle, |event| {
            matches!(
                event,
                ChatEvent::ControlState(state)
                    if state.session_id == session_id
                        && state.steering_count == 0
                        && state.follow_up_count == 1
                        && state.queued_inputs.len() == 1
            )
        })
        .expect("control state after removal");

        let new_session_id = SessionId::from_legacy_named("sess_chat_new_session")
            .expect("session id")
            .to_string();
        handle.send_command(ChatCommand::NewSession {
            session_id: new_session_id.clone(),
        });
        let reset = wait_for_event(&handle, |event| {
            matches!(event, ChatEvent::SessionReset { session_id } if session_id == &new_session_id)
        })
        .expect("new session reset");
        assert!(matches!(
            reset,
            ChatEvent::SessionReset { session_id } if session_id == new_session_id
        ));
    }

    #[test]
    fn chat_worker_switches_agent_for_requested_new_session() {
        let temp = TempDataDir::new("chat-worker-agent-switch");
        let runtime_control = AgentRuntimeControlRegistry::new();
        let handle = ChatHandle::spawn(temp.data_dir.clone(), false, runtime_control, None, None);
        wait_for_event(&handle, |event| matches!(event, ChatEvent::Ready { .. }))
            .expect("ready event");

        let new_session_id = SessionId::from_legacy_named("sess_chat_agent_switch")
            .expect("session id")
            .to_string();
        handle.send_command(ChatCommand::NewSession {
            session_id: new_session_id.clone(),
        });
        wait_for_event(&handle, |event| {
            matches!(event, ChatEvent::SessionReset { session_id } if session_id == &new_session_id)
        })
        .expect("new session reset");

        handle.send_command(ChatCommand::SetAgent {
            session_id: new_session_id,
            key: "coding-agent".to_string(),
            history: Vec::new(),
        });
        let info = wait_for_event(
            &handle,
            |event| matches!(event, ChatEvent::RuntimeInfo(info) if info.agent == "coding-agent"),
        )
        .expect("agent switch runtime info");
        assert!(matches!(
            info,
            ChatEvent::RuntimeInfo(info) if info.agent == "coding-agent"
        ));
    }

    #[test]
    fn chat_handle_routes_live_control_without_worker_queue() {
        let runtime_control = AgentRuntimeControlRegistry::new();
        let session_id =
            SessionId::from_legacy_named("sess_chat_direct_control").expect("session id");
        let session = Arc::new(TestControlSession::new(session_id.clone()));
        runtime_control
            .register_session(session.clone())
            .expect("register fake control session");
        let (cmd_tx, _cmd_rx) = channel::<ChatCommand>();
        let (evt_tx, evt_rx) = channel::<ChatEvent>();
        let handle = ChatHandle {
            tx: cmd_tx,
            rx: evt_rx,
            direct_control: DirectControl {
                runtime_control_registry: runtime_control,
                active_session_id: Arc::new(Mutex::new(Some(session_id.clone()))),
                evt_tx: ChatEventSink::new(evt_tx, None),
            },
            worker: None,
        };

        handle.send_command(ChatCommand::FollowUp {
            session_id: session_id.to_string(),
            message: "also mention live controls".to_string(),
        });
        let follow_up = wait_for_event(&handle, |event| {
            matches!(event, ChatEvent::ControlStatus(status) if status.session_id == session_id.to_string())
        })
        .expect("direct follow-up status");
        assert!(matches!(
            follow_up,
            ChatEvent::ControlStatus(status) if status.accepted
        ));
        assert_eq!(
            session.follow_ups(),
            vec!["also mention live controls".to_string()]
        );

        handle.send_command(ChatCommand::ControlState {
            session_id: session_id.to_string(),
        });
        let state = wait_for_event(&handle, |event| {
            matches!(event, ChatEvent::ControlState(state) if state.session_id == session_id.to_string())
        })
        .expect("direct control state");
        assert!(matches!(
            state,
            ChatEvent::ControlState(state) if state.runtime_attached
        ));
    }

    struct TestControlSession {
        session_id: SessionId,
        sequence: AtomicU64,
        follow_ups: Mutex<Vec<String>>,
    }

    impl TestControlSession {
        fn new(session_id: SessionId) -> Self {
            Self {
                session_id,
                sequence: AtomicU64::new(1),
                follow_ups: Mutex::new(Vec::new()),
            }
        }

        fn snapshot(&self) -> sinomo_agent::AgentSessionControlSnapshot {
            sinomo_agent::AgentSessionControlSnapshot {
                session_id: self.session_id.clone(),
                next_sequence: self.sequence.load(Ordering::SeqCst),
                active_turn: None,
                tasks: Vec::new(),
                buffered_event_count: 0,
            }
        }

        fn result(&self) -> AgentSessionControlResult {
            AgentSessionControlResult {
                sequence: self.sequence.fetch_add(1, Ordering::SeqCst),
                accepted: true,
                events: Vec::new(),
                outcome: None,
                snapshot: self.snapshot(),
            }
        }

        fn follow_ups(&self) -> Vec<String> {
            self.follow_ups.lock().expect("follow-up lock").clone()
        }
    }

    impl sinomo_agent::AgentRuntimeControlSession for TestControlSession {
        fn session_id(&self) -> Result<SessionId, sinomo_agent::AgentRuntimeControlError> {
            Ok(self.session_id.clone())
        }

        fn steer(
            &self,
            _message: String,
        ) -> Result<AgentSessionControlResult, sinomo_agent::AgentRuntimeControlError> {
            Ok(self.result())
        }

        fn follow_up(
            &self,
            message: String,
        ) -> Result<AgentSessionControlResult, sinomo_agent::AgentRuntimeControlError> {
            self.follow_ups
                .lock()
                .expect("follow-up lock")
                .push(message);
            Ok(self.result())
        }

        fn remove_queued_input(
            &self,
            _input_id: TaskId,
        ) -> Result<AgentSessionControlResult, sinomo_agent::AgentRuntimeControlError> {
            Ok(self.result())
        }

        fn interrupt(
            &self,
            _reason: String,
        ) -> Result<AgentSessionControlResult, sinomo_agent::AgentRuntimeControlError> {
            Ok(self.result())
        }

        fn resume(
            &self,
        ) -> Result<AgentSessionResumeResponse, sinomo_agent::AgentRuntimeControlError> {
            let snapshot = self.snapshot();
            Ok(AgentSessionResumeResponse {
                session_id: self.session_id.clone(),
                accepted: true,
                runtime_attached: true,
                session: None,
                control_snapshot: Some(snapshot),
                event_cursor: sinomo_agent::AgentSessionControlEventCursor {
                    offset: 0,
                    last_event_id: None,
                },
                reattach: sinomo_agent::AgentSessionReattachReport {
                    state: sinomo_agent::AgentSessionReattachState::LiveRuntimeAttached,
                    active_turn_persisted: false,
                    running_task_count: 0,
                    persisted_event_count: 0,
                    reason: "test control session".to_string(),
                },
                detail: None,
            })
        }
    }

    fn wait_for_event(
        handle: &ChatHandle,
        matches: impl Fn(&ChatEvent) -> bool,
    ) -> Option<ChatEvent> {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            for event in handle.drain() {
                if matches(&event) {
                    return Some(event);
                }
                if let ChatEvent::Error { error, .. } = &event {
                    panic!("chat worker error: {error}");
                }
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        None
    }

    struct TempDataDir {
        data_dir: SinomoDataDir,
        path: PathBuf,
    }

    impl TempDataDir {
        fn new(slug: &str) -> Self {
            let path = temp_data_dir_path(slug);
            std::fs::create_dir_all(&path).expect("create temp data dir");
            Self {
                data_dir: SinomoDataDir::new(&path),
                path,
            }
        }
    }

    impl Drop for TempDataDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn temp_data_dir_path(slug: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("sinomo-app-{slug}-{}-{unique}", std::process::id()))
    }
}
