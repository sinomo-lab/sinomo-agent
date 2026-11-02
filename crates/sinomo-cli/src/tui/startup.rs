use super::*;
use std::sync::mpsc::{self, Receiver, TryRecvError};

type Ready = ReadyTui<RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel>>;

/// The terminal owns presentation immediately. The worker owns all daemon,
/// configuration, catalog and session preparation until it transfers readiness.
struct Startup<T> {
    receiver: Receiver<Result<T, CliError>>,
    error: Option<CliError>,
}

impl<T> Startup<T> {
    fn poll(&mut self, state: &mut TuiState) -> Option<T> {
        if self.error.is_some() {
            return None;
        }
        let error = match self.receiver.try_recv() {
            Ok(Ok(ready)) => return Some(ready),
            Ok(Err(error)) => error,
            Err(TryRecvError::Empty) => return None,
            Err(TryRecvError::Disconnected) => CliError::Parse {
                detail: "startup worker stopped without a result".to_string(),
            },
        };
        state.status = "startup failed; Esc to quit".to_string();
        state.push_message(TuiMessageKind::System, format!("Startup failed: {error}"));
        self.error = Some(error);
        None
    }

    fn exit(self) -> Result<(), CliError> {
        self.error.map_or(Ok(()), Err)
    }

    fn key(&self, key: KeyEvent) -> bool {
        matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat)
            && (key.code == KeyCode::Esc
                || (key.code == KeyCode::Char('c')
                    && key.modifiers.contains(KeyModifiers::CONTROL)))
    }
}

fn loading_state() -> TuiState {
    let session = interaction_session_for_daemon(None, SessionId::generate());
    let mut state = TuiState::new(
        &session,
        None,
        TuiAgentRuntimeInfo {
            provider: "loading".to_string(),
            model: "loading".to_string(),
            ..Default::default()
        },
    );
    state.startup_ready = false;
    state.messages.clear();
    state.push_message(
        TuiMessageKind::System,
        "Initializing; chat is unavailable until startup completes.",
    );
    state.status = "initializing; Esc to quit".to_string();
    state
}

pub(super) fn run() -> Result<(), CliError> {
    let mut terminal = TuiTerminalSession::enter()?;
    let mut state = loading_state();
    terminal.draw(&state)?;
    let (sender, receiver) = mpsc::sync_channel(1);
    let force_local = cli_force_local_mode();
    std::thread::Builder::new()
        .name("sinomo-tui-startup".to_string())
        .spawn(move || {
            let _local = enter_cli_force_local_mode(force_local);
            let result = (|| {
                let (daemon, force_local) = prepare_tui_daemon()?;
                let _local = enter_cli_force_local_mode(force_local);
                let _daemon = daemon.clone().map(enter_cli_default_main_daemon_status);
                let data_dir = SinomoDataDir::from_env_or_home()?;
                let (handler, daemon) =
                    runtime_main_agent_handler_for_tui_with_data_dir(daemon, &data_dir)?;
                prepare_tui_session(handler, daemon, force_local)
            })();
            let _ = sender.send(result);
        })
        .map_err(CliError::stdio)?;
    let mut startup: Startup<Ready> = Startup {
        receiver,
        error: None,
    };
    loop {
        let failed = startup.error.is_some();
        if let Some(ready) = startup.poll(&mut state) {
            // Keys entered while the loading frame was visible must not become
            // a chat submission when the ready session takes over.
            while event::poll(Duration::ZERO).map_err(CliError::stdio)? {
                if let CrosstermEvent::Key(key) = event::read().map_err(CliError::stdio)?
                    && startup.key(key)
                {
                    return startup.exit();
                }
            }
            return run_tui_session(ready, terminal);
        }
        if failed != startup.error.is_some() {
            terminal.draw(&state)?;
        }
        let wait = if startup.error.is_some() { 250 } else { 25 };
        if !event::poll(Duration::from_millis(wait)).map_err(CliError::stdio)? {
            continue;
        }
        match event::read().map_err(CliError::stdio)? {
            CrosstermEvent::Key(key) if startup.key(key) => return startup.exit(),
            CrosstermEvent::Resize(_, _) => terminal.draw(&state)?,
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_and_runtime_settings_share_one_snapshot_read() {
        use sinomo_runtime::kv::{KvEntry, KvOp, KvRevision, KvVersion};
        let models = ModelConfig {
            default_provider: Some("deepseek".to_string()),
            default_model: Some("deepseek-flash".to_string()),
            ..Default::default()
        };
        let runtime = ModelRuntimeConfig {
            pytorch_bridge: sinomo_lm::PytorchBridgeRuntimeConfig {
                endpoint: Some("http://127.0.0.1:42222".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let entries = [
            KvOp::put_serde(LM_PROVIDER_CONFIG_KV_KEY, &models, None).unwrap(),
            KvOp::put_serde(LM_RUNTIME_CONFIG_KV_KEY, &runtime, None).unwrap(),
        ]
        .into_iter()
        .map(|op| {
            let KvOp::Put {
                key,
                value,
                lease_id,
            } = op
            else {
                unreachable!()
            };
            KvEntry {
                key,
                value,
                lease_id,
                create_revision: KvRevision(1),
                modify_revision: KvRevision(1),
                version: KvVersion(1),
            }
        })
        .collect();
        let mut reads = 0;
        let (actual_runtime, actual_models) = read_tui_startup_configs(|| {
            reads += 1;
            Ok(KvSnapshot {
                entries,
                revision: KvRevision(1),
                compacted_revision: KvRevision::ZERO,
                leases: Vec::new(),
                events: Vec::new(),
                committed_requests: Vec::new(),
                expired_requests: Vec::new(),
            })
        })
        .unwrap();
        assert_eq!(reads, 1);
        assert_eq!(actual_runtime, runtime);
        assert_eq!(actual_models, models);
    }

    #[test]
    fn slow_startup_disables_submission_and_allows_exit() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut startup = Startup {
            receiver,
            error: None,
        };
        let mut state = loading_state();
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(120, 24)).unwrap();
        terminal.draw(|frame| render_tui(frame, &state)).unwrap();
        let screen = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(screen.contains("initializing"));
        assert!(screen.contains("Chat unavailable"));
        assert!(!screen.contains("Enter send"));
        assert!(startup.poll(&mut state).is_none());
        state.input = "must not submit".to_string();
        assert!(matches!(state.take_submission(), TuiInputAction::None));
        assert_eq!(state.input, "must not submit");
        assert!(!startup.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)));
        assert!(startup.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)));
        assert!(startup.key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)));
        sender.send(Ok(42)).unwrap();
        assert_eq!(startup.poll(&mut state), Some(42));
    }

    #[test]
    fn skill_failure_stays_visible_and_never_becomes_ready() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut startup: Startup<()> = Startup {
            receiver,
            error: None,
        };
        let mut state = loading_state();
        sender
            .send(Err(CliError::Parse {
                detail: "resident daemon skill-catalog read timed out after 250ms".to_string(),
            }))
            .unwrap();
        assert!(startup.poll(&mut state).is_none());
        assert!(startup.error.is_some());
        assert!(state.status.contains("startup failed"));
        assert!(
            state
                .messages
                .iter()
                .any(|message| message.text.contains("skill-catalog"))
        );
        assert!(startup.poll(&mut state).is_none());
        state.input = "must not submit".to_string();
        assert!(matches!(state.take_submission(), TuiInputAction::None));
        assert_eq!(state.input, "must not submit");
        assert!(!startup.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)));
        let error = startup
            .exit()
            .expect_err("preserve startup failure on exit");
        assert_eq!(error.exit_code(), 2);
        assert!(error.to_string().contains("skill-catalog"));
    }
}
