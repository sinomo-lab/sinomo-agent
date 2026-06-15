//! Chat actor.
//!
//! The agent interaction runtime is stateful and its calls block while the model
//! streams. To keep the UI thread responsive we run it on a dedicated worker
//! thread (constructed *inside* that thread, so nothing non-`Send` ever crosses a
//! boundary). The UI sends [`ChatCommand`]s and drains [`ChatEvent`]s each frame.

use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;

use sinomo_agent::AgentRuntimeControlRegistry;
use sinomo_cli::app_api::{
    AgentChat, ChatControlState, ChatControlStatus, ChatHistoryMessage, ChatRuntimeInfo,
    ChatStreamEvent,
};
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
        thread::Builder::new()
            .name("sinomo-chat".to_string())
            .spawn(move || {
                worker(
                    data_dir,
                    daemon_connected,
                    runtime_control_registry,
                    cmd_rx,
                    evt_tx,
                )
            })
            .expect("spawn chat worker");
        Self {
            tx: cmd_tx,
            rx: evt_rx,
        }
    }

    pub fn send_command(&self, command: ChatCommand) {
        let _ = self.tx.send(command);
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

fn worker(
    data_dir: SinomoDataDir,
    daemon_connected: bool,
    runtime_control_registry: AgentRuntimeControlRegistry,
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
                        ChatStreamEvent::Notice(_) | ChatStreamEvent::Usage { .. } => None,
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
