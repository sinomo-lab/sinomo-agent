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

use sinomo_agent::{
    AgentRuntimeControlHost, AgentRuntimeControlRegistry, AgentSessionControlResult,
    AgentSessionFollowUpRequest, AgentSessionInterruptRequest, AgentSessionResumeRequest,
    AgentSessionResumeResponse, AgentSessionSteerRequest, AgentTaskLifecycleStatus,
    AgentTaskSnapshot,
};
use sinomo_cli::app_api::{
    AgentChat, ChatContextSnapshot, ChatControlState, ChatControlStatus, ChatHistoryMessage,
    ChatProcessSnapshot, ChatRuntimeInfo, ChatStreamEvent, ChatToolCallSnapshot,
    ChatTaskSnapshot, ChatUsageSnapshot,
};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;

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
    Send(String),
    Steer(String),
    FollowUp(String),
    Interrupt(String),
    ControlState,
    NewSession,
    Resume {
        session_id: String,
        history: Vec<ChatHistoryMessage>,
    },
    SetModel {
        provider: String,
        model: String,
        account: String,
    },
    SetAgent(String),
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
    Token(String),
    Notice(String),
    Context(ChatContextSnapshot),
    ToolCallStarted(ChatToolCallSnapshot),
    ToolCallDelta {
        step_index: usize,
        call_id: Option<String>,
        tool: Option<String>,
        arguments_delta: String,
    },
    ToolCallCompleted(ChatToolCallSnapshot),
    Process(ChatProcessSnapshot),
    Usage(ChatUsageSnapshot),
    TurnDone,
    ControlStatus(ChatControlStatus),
    ControlState(ChatControlState),
    RuntimeInfo(RuntimeInfo),
    SessionReset {
        session_id: String,
    },
    /// Available models for `provider`, fetched from sinomo-lm (empty on failure).
    Models {
        provider: String,
        models: Vec<String>,
    },
    Error(String),
}

/// Handle to the chat worker thread.
pub struct ChatHandle {
    tx: Sender<ChatCommand>,
    rx: Receiver<ChatEvent>,
    direct_control: DirectControl,
}

impl ChatHandle {
    /// Spawn the worker. The runtime is built inside the thread.
    pub fn spawn(
        data_dir: SinomoDataDir,
        daemon_connected: bool,
        runtime_control_registry: AgentRuntimeControlRegistry,
    ) -> Self {
        let (cmd_tx, cmd_rx) = channel::<ChatCommand>();
        let (evt_tx, evt_rx) = channel::<ChatEvent>();
        let active_session_id = Arc::new(Mutex::new(None));
        let direct_control = DirectControl {
            runtime_control_registry: runtime_control_registry.clone(),
            active_session_id: active_session_id.clone(),
            evt_tx: evt_tx.clone(),
        };
        thread::Builder::new()
            .name("sinomo-chat".to_string())
            .spawn(move || {
                worker(
                    data_dir,
                    daemon_connected,
                    runtime_control_registry,
                    active_session_id,
                    cmd_rx,
                    evt_tx,
                )
            })
            .expect("spawn chat worker");
        Self {
            tx: cmd_tx,
            rx: evt_rx,
            direct_control,
        }
    }

    pub fn send_command(&self, command: ChatCommand) {
        match command {
            ChatCommand::Steer(message) => self.direct_control.steer(message),
            ChatCommand::FollowUp(message) => self.direct_control.follow_up(message),
            ChatCommand::Interrupt(reason) => self.direct_control.interrupt(reason),
            ChatCommand::ControlState => self.direct_control.control_state(),
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
    }
}

#[derive(Clone)]
struct DirectControl {
    runtime_control_registry: AgentRuntimeControlRegistry,
    active_session_id: Arc<Mutex<Option<SessionId>>>,
    evt_tx: Sender<ChatEvent>,
}

impl DirectControl {
    fn steer(&self, message: String) {
        self.spawn("steer", move |registry, session_id| {
            registry
                .steer(AgentSessionSteerRequest {
                    session_id,
                    message,
                })
                .map(|result| ChatEvent::ControlStatus(chat_control_status(result)))
        });
    }

    fn follow_up(&self, message: String) {
        self.spawn("follow-up", move |registry, session_id| {
            registry
                .follow_up(AgentSessionFollowUpRequest {
                    session_id,
                    message,
                })
                .map(|result| ChatEvent::ControlStatus(chat_control_status(result)))
        });
    }

    fn interrupt(&self, reason: String) {
        self.spawn("interrupt", move |registry, session_id| {
            registry
                .interrupt(AgentSessionInterruptRequest { session_id, reason })
                .map(|result| ChatEvent::ControlStatus(chat_control_status(result)))
        });
    }

    fn control_state(&self) {
        self.spawn("control state", move |registry, session_id| {
            registry
                .resume(AgentSessionResumeRequest { session_id })
                .map(|response| ChatEvent::ControlState(chat_control_state(response)))
        });
    }

    fn spawn(
        &self,
        label: &'static str,
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
                let session_id = match active_session_id.lock() {
                    Ok(guard) => guard.clone(),
                    Err(_) => {
                        let _ = tx.send(ChatEvent::Error(format!(
                            "{label} failed: control lock poisoned"
                        )));
                        return;
                    }
                };
                let Some(session_id) = session_id else {
                    let _ = tx.send(ChatEvent::Error(format!(
                        "{label} failed: no active chat session"
                    )));
                    return;
                };
                match action(registry, session_id) {
                    Ok(event) => {
                        let _ = tx.send(event);
                    }
                    Err(error) => {
                        let _ = tx.send(ChatEvent::Error(format!(
                            "{label} failed: agent runtime control: {error}"
                        )));
                    }
                }
            })
            .ok();
    }
}

fn set_active_session_id(
    active_session_id: &Arc<Mutex<Option<SessionId>>>,
    evt_tx: &Sender<ChatEvent>,
    session_id: impl AsRef<str>,
) {
    match SessionId::from_str(session_id.as_ref()) {
        Ok(session_id) => {
            if let Ok(mut active) = active_session_id.lock() {
                *active = Some(session_id);
            } else {
                let _ = evt_tx.send(ChatEvent::Error(
                    "control state failed: control lock poisoned".to_string(),
                ));
            }
        }
        Err(error) => {
            let _ = evt_tx.send(ChatEvent::Error(format!(
                "control state failed: invalid session id: {error}"
            )));
        }
    }
}

fn chat_control_status(result: AgentSessionControlResult) -> ChatControlStatus {
    ChatControlStatus {
        session_id: result.snapshot.session_id.to_string(),
        accepted: result.accepted,
        sequence: result.sequence,
        event_count: result.events.len(),
        active_turn: result.snapshot.active_turn.is_some(),
        task_count: result.snapshot.tasks.len(),
        terminal_result_count: result.terminal_results().len(),
        tasks: result.snapshot.tasks.iter().map(chat_task_snapshot).collect(),
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
    active_session_id: Arc<Mutex<Option<SessionId>>>,
    cmd_rx: Receiver<ChatCommand>,
    evt_tx: Sender<ChatEvent>,
) {
    let mut chat = match AgentChat::with_runtime_control_registry(
        data_dir,
        daemon_connected,
        runtime_control_registry,
    ) {
        Ok(chat) => chat,
        Err(error) => {
            let _ = evt_tx.send(ChatEvent::Error(format!("chat unavailable: {error}")));
            return;
        }
    };
    set_active_session_id(&active_session_id, &evt_tx, chat.session_id());

    let _ = evt_tx.send(ChatEvent::Ready {
        session_id: chat.session_id(),
        info: chat.runtime_info().into(),
    });

    while let Ok(command) = cmd_rx.recv() {
        match command {
            ChatCommand::Send(text) => {
                let tx = evt_tx.clone();
                let result = chat.send(&text, &mut |event| {
                    let mapped = match event {
                        ChatStreamEvent::Token(token) => Some(ChatEvent::Token(token)),
                        ChatStreamEvent::Notice(notice) if !notice.is_empty() => {
                            Some(ChatEvent::Notice(notice))
                        }
                        ChatStreamEvent::Context(context) => Some(ChatEvent::Context(context)),
                        ChatStreamEvent::ToolCallStarted(call) => {
                            Some(ChatEvent::ToolCallStarted(call))
                        }
                        ChatStreamEvent::ToolCallDelta {
                            step_index,
                            call_id,
                            tool,
                            arguments_delta,
                        } => Some(ChatEvent::ToolCallDelta {
                            step_index,
                            call_id,
                            tool,
                            arguments_delta,
                        }),
                        ChatStreamEvent::ToolCallCompleted(call) => {
                            Some(ChatEvent::ToolCallCompleted(call))
                        }
                        ChatStreamEvent::Process(process) => Some(ChatEvent::Process(process)),
                        ChatStreamEvent::Usage(usage) => Some(ChatEvent::Usage(usage)),
                        ChatStreamEvent::Notice(_) => None,
                    };
                    if let Some(event) = mapped {
                        let _ = tx.send(event);
                    }
                });
                if let Err(error) = result {
                    let _ = evt_tx.send(ChatEvent::Error(format!("turn failed: {error}")));
                }
                let _ = evt_tx.send(ChatEvent::TurnDone);
            }
            ChatCommand::Steer(message) => match chat.steer(message) {
                Ok(status) => {
                    let _ = evt_tx.send(ChatEvent::ControlStatus(status));
                }
                Err(error) => {
                    let _ = evt_tx.send(ChatEvent::Error(format!("steer failed: {error}")));
                }
            },
            ChatCommand::FollowUp(message) => match chat.follow_up(message) {
                Ok(status) => {
                    let _ = evt_tx.send(ChatEvent::ControlStatus(status));
                }
                Err(error) => {
                    let _ = evt_tx.send(ChatEvent::Error(format!("follow-up failed: {error}")));
                }
            },
            ChatCommand::Interrupt(reason) => match chat.interrupt(reason) {
                Ok(status) => {
                    let _ = evt_tx.send(ChatEvent::ControlStatus(status));
                }
                Err(error) => {
                    let _ = evt_tx.send(ChatEvent::Error(format!("interrupt failed: {error}")));
                }
            },
            ChatCommand::ControlState => match chat.control_state() {
                Ok(state) => {
                    let _ = evt_tx.send(ChatEvent::ControlState(state));
                }
                Err(error) => {
                    let _ = evt_tx.send(ChatEvent::Error(format!("control state failed: {error}")));
                }
            },
            ChatCommand::NewSession => {
                chat.reset_session();
                set_active_session_id(&active_session_id, &evt_tx, chat.session_id());
                let _ = evt_tx.send(ChatEvent::SessionReset {
                    session_id: chat.session_id(),
                });
            }
            ChatCommand::Resume {
                session_id,
                history,
            } => {
                if let Err(error) = chat.resume_session(&session_id, &history) {
                    let _ = evt_tx.send(ChatEvent::Error(format!("resume failed: {error}")));
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
                    let _ = evt_tx.send(ChatEvent::Error(format!("model switch failed: {error}")));
                }
            },
            ChatCommand::SetAgent(key) => match chat.set_agent(&key) {
                Ok(info) => {
                    let _ = evt_tx.send(ChatEvent::RuntimeInfo(info.into()));
                }
                Err(error) => {
                    let _ = evt_tx.send(ChatEvent::Error(format!("agent switch failed: {error}")));
                }
            },
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    #[test]
    fn chat_worker_routes_runtime_control_commands() {
        let data_dir = temp_data_dir("chat-worker-control");
        let runtime_control = AgentRuntimeControlRegistry::new();
        let handle = ChatHandle::spawn(data_dir, false, runtime_control.clone());
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

        handle.send_command(ChatCommand::ControlState);
        let initial = wait_for_event(&handle, |event| {
            matches!(event, ChatEvent::ControlState(state) if state.session_id == session_id)
        })
        .expect("initial control state");
        assert!(matches!(
            initial,
            ChatEvent::ControlState(state)
                if state.steering_count == 0 && state.follow_up_count == 0
        ));

        handle.send_command(ChatCommand::Steer("keep this constraint".to_string()));
        let steer = wait_for_event(&handle, |event| {
            matches!(event, ChatEvent::ControlStatus(status) if status.session_id == session_id)
        })
        .expect("steer status");
        assert!(matches!(
            steer,
            ChatEvent::ControlStatus(status) if status.accepted
        ));

        handle.send_command(ChatCommand::FollowUp("also mention tests".to_string()));
        let follow_up = wait_for_event(&handle, |event| {
            matches!(event, ChatEvent::ControlStatus(status) if status.session_id == session_id)
        })
        .expect("follow-up status");
        assert!(matches!(
            follow_up,
            ChatEvent::ControlStatus(status) if status.accepted
        ));

        handle.send_command(ChatCommand::ControlState);
        let state = wait_for_event(&handle, |event| {
            matches!(
                event,
                ChatEvent::ControlState(state)
                    if state.session_id == session_id
                        && state.steering_count == 1
                        && state.follow_up_count == 1
            )
        })
        .expect("updated control state");
        assert!(matches!(state, ChatEvent::ControlState(_)));
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
                evt_tx,
            },
        };

        handle.send_command(ChatCommand::FollowUp("also mention live controls".to_string()));
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

        handle.send_command(ChatCommand::ControlState);
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
                if let ChatEvent::Error(error) = &event {
                    panic!("chat worker error: {error}");
                }
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        None
    }

    fn temp_data_dir(slug: &str) -> SinomoDataDir {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "sinomo-app-{slug}-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("create temp data dir");
        SinomoDataDir::new(path)
    }
}
