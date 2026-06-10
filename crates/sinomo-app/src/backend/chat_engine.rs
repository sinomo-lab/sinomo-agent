//! Chat actor.
//!
//! The agent interaction runtime is stateful and its calls block while the model
//! streams. To keep the UI thread responsive we run it on a dedicated worker
//! thread (constructed *inside* that thread, so nothing non-`Send` ever crosses a
//! boundary). The UI sends [`ChatCommand`]s and drains [`ChatEvent`]s each frame.

use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;

use sinomo_cli::app_api::{AgentChat, ChatHistoryMessage, ChatRuntimeInfo, ChatStreamEvent};
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
    pub fn spawn(data_dir: SinomoDataDir, daemon_connected: bool) -> Self {
        let (cmd_tx, cmd_rx) = channel::<ChatCommand>();
        let (evt_tx, evt_rx) = channel::<ChatEvent>();
        thread::Builder::new()
            .name("sinomo-chat".to_string())
            .spawn(move || worker(data_dir, daemon_connected, cmd_rx, evt_tx))
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
    cmd_rx: Receiver<ChatCommand>,
    evt_tx: Sender<ChatEvent>,
) {
    let mut chat = match AgentChat::new(data_dir, daemon_connected) {
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
