use super::*;
use crate::chat_activity::{StableChatProjection, background_handoff_notice};
use crate::file_manager::{
    FileManagerEntryKind, FileManagerPreviewer, FileManagerPrompt, FileManagerState,
    format_file_size,
};
use crate::sifs_file_manager::SifsFileManagerBackend;
use crate::sobj_file_preview::SobjFilePreviewer;
#[cfg(feature = "native-models")]
use candle_core::{DType, Device, Tensor};
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event as CrosstermEvent, KeyCode, KeyEvent,
        KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Position, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};
use serde::{Deserialize, Serialize};
#[cfg(feature = "native-models")]
use sinomo_models::minicpm_v46::{
    DEFAULT_MODEL_ID as MINICPM_V46_DEFAULT_MODEL_ID, MiniCpmV46Config,
    MiniCpmV46ForConditionalGeneration, Qwen3_5TextCache,
};
use std::io::IsTerminal;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
#[cfg(feature = "native-models")]
use tokenizers::Tokenizer;

const RUST_MINICPM_V46_PROVIDER_ID: &str = "sinomo-models";
#[cfg(feature = "native-models")]
const DEFAULT_RUST_MINICPM_V46_CACHE_SUBDIR: &str = "openbmb--MiniCPM-V-4_6";
const TUI_SESSION_SCHEMA_VERSION: u32 = 1;
const TUI_WORKSPACE_PROJECTION_VERSION: u32 = 1;
const TUI_MOUSE_SCROLL_LINES: isize = 4;

pub fn run_tui() -> Result<(), CliError> {
    if io::stdin().is_terminal() && io::stdout().is_terminal() {
        let daemon = tui_default_daemon_status()?;
        let (handler, daemon) = runtime_main_agent_handler_for_tui(daemon)?;
        return run_tui_with_handler_and_daemon(handler, daemon);
    }

    let stdin = io::stdin();
    let stdout = io::stdout();
    run_tui_with_io(stdin.lock(), stdout.lock())
}

pub fn run_tui_with_io<R: BufRead, W: Write>(mut reader: R, mut writer: W) -> Result<(), CliError> {
    let daemon = tui_default_daemon_status()?;
    run_tui_with_io_and_daemon_probe(&mut reader, &mut writer, daemon)
}

pub(crate) fn tui_default_daemon_status() -> Result<Option<DaemonStatus>, CliError> {
    if cli_force_local_mode() {
        return Ok(None);
    }
    default_main_daemon_status()
}

pub(super) fn run_tui_with_io_and_daemon_probe<R: BufRead, W: Write>(
    reader: &mut R,
    writer: &mut W,
    daemon: Option<DaemonStatus>,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    let (handler, daemon) = main_agent_handler_for_tui_with_data_dir(daemon, &data_dir)?;
    run_tui_with_handler_io_and_daemon_and_projection(
        reader,
        writer,
        handler,
        daemon,
        Some(&data_dir),
    )
}

fn runtime_main_agent_handler_for_tui(
    daemon: Option<DaemonStatus>,
) -> Result<
    (
        RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel>,
        Option<DaemonStatus>,
    ),
    CliError,
> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    runtime_main_agent_handler_for_tui_with_data_dir(daemon, &data_dir)
}

pub(crate) fn main_agent_handler_for_tui_with_data_dir(
    daemon: Option<DaemonStatus>,
    data_dir: &SinomoDataDir,
) -> Result<
    (
        RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel>,
        Option<DaemonStatus>,
    ),
    CliError,
> {
    runtime_main_agent_handler_for_tui_with_data_dir(daemon, data_dir)
}

pub(crate) fn runtime_main_agent_handler_for_tui_with_data_dir(
    daemon: Option<DaemonStatus>,
    data_dir: &SinomoDataDir,
) -> Result<
    (
        RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel>,
        Option<DaemonStatus>,
    ),
    CliError,
> {
    let daemon_connected = daemon.is_some();
    let (runtime_config, _) = load_tui_model_runtime_config(data_dir, daemon_connected)?;
    let handler = RuntimeMainAgentInteractionHandler::from_default_model_config(
        data_dir,
        runtime_config,
        daemon.as_ref(),
    )?;
    Ok((handler, daemon))
}

pub(crate) fn load_tui_model_runtime_config(
    data_dir: &SinomoDataDir,
    daemon_connected: bool,
) -> Result<(ModelRuntimeConfig, PathBuf), CliError> {
    if daemon_connected || cli_force_local_mode() {
        return load_model_runtime_config_for_node(data_dir);
    }

    let _guard = enter_cli_force_local_mode(true);
    load_model_runtime_config_for_node(data_dir)
}

pub fn run_tui_with_handler_io<R: BufRead, W: Write, H: InteractionEventHandler>(
    mut reader: R,
    mut writer: W,
    handler: H,
) -> Result<(), CliError> {
    run_tui_with_handler_io_and_daemon(&mut reader, &mut writer, handler, None)
}

pub(super) fn run_tui_with_handler_io_and_daemon<
    R: BufRead,
    W: Write,
    H: InteractionEventHandler,
>(
    reader: R,
    writer: W,
    handler: H,
    daemon: Option<DaemonStatus>,
) -> Result<(), CliError> {
    run_tui_with_handler_io_and_daemon_and_projection(reader, writer, handler, daemon, None)
}

fn run_tui_with_handler_io_and_daemon_and_projection<
    R: BufRead,
    W: Write,
    H: InteractionEventHandler,
>(
    mut reader: R,
    mut writer: W,
    handler: H,
    daemon: Option<DaemonStatus>,
    data_dir: Option<&SinomoDataDir>,
) -> Result<(), CliError> {
    let mut runtime = new_interaction_runtime(handler, daemon.as_ref())?;
    let session = runtime.session().clone();
    let projection = data_dir
        .map(|data_dir| StableChatProjection::open(data_dir, &session))
        .transpose()?;

    writeln!(writer, "Sinomo main agent").map_err(CliError::stdio)?;
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
        let is_explicit_skill_turn = parse_slash_command(line)?
            .as_deref()
            .and_then(parse_explicit_chat_skill_command)
            .is_some();
        let is_command = line.trim_start().starts_with('/') && !is_explicit_skill_turn;
        if !is_command {
            writeln!(writer, "sinomo: working...").map_err(CliError::stdio)?;
            writer.flush().map_err(CliError::stdio)?;
        }

        let mut streamed = false;
        let mut emit = |output: InteractionOutput| {
            if let InteractionOutput::Chunk(chunk) = output {
                let _ = write!(writer, "{chunk}");
                let _ = writer.flush();
                streamed = true;
            }
        };
        let stable_input_id = TaskId::generate();
        let stable_input_at = OffsetDateTime::now_utc();
        if !is_command && let Some(projection) = projection.as_ref() {
            projection.record_user_message(
                runtime
                    .handler()
                    .stable_chat_agent_key()
                    .as_deref()
                    .unwrap_or("main-agent"),
                stable_input_id.clone(),
                stable_input_at,
                line,
            )?;
        }
        let interaction = (|| {
            let outputs = if is_command {
                runtime.ingest_user_input_streaming(line, &mut emit)?
            } else {
                runtime.ingest_user_input_with_item_id_and_context_attachments_streaming(
                    line,
                    stable_input_id.clone(),
                    Vec::new(),
                    &mut emit,
                )?
            };
            runtime.handler_mut().join_stable_chat_turn()?;
            let runtime_events = runtime.handler_mut().drain_stable_chat_runtime_events()?;
            Ok::<_, CliError>((outputs, runtime_events))
        })();
        let (outputs, runtime_events) = match interaction {
            Ok(result) => result,
            Err(error) => {
                if !is_command && let Some(projection) = projection.as_ref() {
                    let projection_result = projection.record_system_message(
                        runtime
                            .handler()
                            .stable_chat_agent_key()
                            .as_deref()
                            .unwrap_or("main-agent"),
                        stable_tui_result_id(&stable_input_id, "failed")?,
                        OffsetDateTime::now_utc(),
                        &format!("Agent turn failed: {error}"),
                        true,
                    );
                    if let Err(projection_error) = projection_result {
                        return Err(combine_stable_chat_errors(error, projection_error));
                    }
                }
                return Err(error);
            }
        };
        if let Some(projection) = projection.as_ref() {
            let agent_key = runtime
                .handler()
                .stable_chat_agent_key()
                .unwrap_or_else(|| "main-agent".to_string());
            for event in &runtime_events {
                projection.record_runtime_event(&agent_key, event)?;
            }
            if !is_command {
                for (index, output) in outputs.iter().enumerate() {
                    if let InteractionOutput::Line(line) = output {
                        projection.record_system_message(
                            &agent_key,
                            stable_tui_result_id(&stable_input_id, &format!("output-{index}"))?,
                            OffsetDateTime::now_utc(),
                            line,
                            false,
                        )?;
                    }
                }
            }
        }
        if streamed {
            writeln!(writer).map_err(CliError::stdio)?;
        }
        for output in outputs {
            match output {
                InteractionOutput::Chunk(chunk) => {
                    write!(writer, "{chunk}").map_err(CliError::stdio)?;
                    writer.flush().map_err(CliError::stdio)?;
                }
                InteractionOutput::Line(line) => {
                    writeln!(writer, "{line}").map_err(CliError::stdio)?;
                }
                InteractionOutput::ModelUsage { .. } => {}
                InteractionOutput::Exit => return Ok(()),
            }
        }
        for event in runtime_events {
            for line in stable_chat_runtime_event_lines(&event) {
                writeln!(writer, "{line}").map_err(CliError::stdio)?;
            }
        }
    }

    Ok(())
}

fn new_interaction_runtime<H: InteractionEventHandler>(
    handler: H,
    daemon: Option<&DaemonStatus>,
) -> Result<InteractionRuntime<InMemoryEventStore, H>, CliError> {
    let session = interaction_session_for_daemon(daemon, SessionId::generate());
    let mut runtime = InteractionRuntime::new(InMemoryEventStore::new(), handler, session.clone());
    runtime.append_session_context(daemon_context_event(&session, daemon))?;
    Ok(runtime)
}

fn interaction_session_for_daemon(
    daemon: Option<&DaemonStatus>,
    session_id: SessionId,
) -> InteractionSession {
    let node_id = daemon
        .and_then(|status| status.cluster.as_ref())
        .map(|cluster| cluster.node_id.clone())
        .unwrap_or_else(|| NodeId::from_legacy_named("node_cli").expect("valid node id"));
    InteractionSession::new(
        session_id,
        node_id,
        InteractionSurface::new("surface.cli", InteractionSurfaceKind::CommandLine),
    )
}

fn stable_chat_runtime_event_lines(event: &AgentRuntimeEvent) -> Vec<String> {
    match &event.payload {
        AgentRuntimeEventPayload::TurnFinalDecision {
            decision,
            stop_reason,
            ..
        } => {
            let text = decision.text.trim();
            let mut lines = vec![if !text.is_empty() {
                text.to_string()
            } else if *stop_reason == sinomo_agent::AgentStopReason::Cancelled {
                "Agent turn cancelled.".to_string()
            } else {
                "No agent responded.".to_string()
            }];
            if let Some(handoff) = decision.background_handoff.as_ref() {
                lines.push(background_handoff_notice(handoff));
            }
            lines
        }
        AgentRuntimeEventPayload::WorkerFailed { action, detail, .. } => {
            vec![format!("Agent task failed: {action}: {detail}")]
        }
        AgentRuntimeEventPayload::WorkerStarted { .. }
        | AgentRuntimeEventPayload::WorkerCompleted { .. }
        | AgentRuntimeEventPayload::ModelProgress { .. }
        | AgentRuntimeEventPayload::ToolProgress { .. }
        | AgentRuntimeEventPayload::ControlEvent { .. } => Vec::new(),
    }
}

fn new_tui_interaction_runtime<H: TuiAgentRuntimeControl>(
    handler: H,
    daemon: Option<&DaemonStatus>,
    session_store: &TuiSessionStore,
) -> Result<InteractionRuntime<JsonlEventStore, H>, CliError> {
    let session = interaction_session_for_daemon(daemon, SessionId::generate());
    let store = JsonlEventStore::new(session_store.events_path_for_session(&session.session_id));
    let mut runtime = InteractionRuntime::new(store, handler, session.clone());
    runtime.append_session_context(daemon_context_event(&session, daemon))?;
    Ok(runtime)
}

fn run_tui_with_handler_and_daemon<H: TuiAgentRuntimeControl>(
    handler: H,
    daemon: Option<DaemonStatus>,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let session_store = TuiSessionStore::for_data_dir(&data_dir);
    let mut runtime = new_tui_interaction_runtime(handler, daemon.as_ref(), &session_store)?;
    let mut state = TuiState::new(
        runtime.session(),
        daemon.as_ref(),
        runtime.handler.tui_agent_runtime_info(),
    );
    let mut chat_projection = match daemon.as_ref() {
        Some(daemon) => StableChatProjection::open_daemon_bounded(
            &daemon.address,
            Duration::from_millis(250),
            runtime.session(),
        )
        .unwrap_or_else(|_| StableChatProjection::unavailable(runtime.session())),
        None => StableChatProjection::unavailable(runtime.session()),
    };
    session_store.save_state(runtime.session(), &state)?;
    let mut terminal = TuiTerminalSession::enter()?;

    terminal.draw(&state)?;
    loop {
        if state.exit_requested {
            break;
        }
        if refresh_tui_runtime_events(
            &mut runtime,
            &mut state,
            &session_store,
            daemon.as_ref(),
            &mut chat_projection,
        )? {
            terminal.draw(&state)?;
        }
        if !event::poll(Duration::from_millis(250)).map_err(CliError::stdio)? {
            continue;
        }
        match event::read().map_err(CliError::stdio)? {
            CrosstermEvent::Key(key) => {
                handle_tui_input_action(
                    state.apply_key_event(key),
                    &mut runtime,
                    &mut state,
                    &mut terminal,
                    &data_dir,
                    &session_store,
                    daemon.as_ref(),
                    &mut chat_projection,
                )?;
                terminal.draw(&state)?;
            }
            CrosstermEvent::Mouse(mouse) => {
                let area = terminal.viewport()?;
                let action = state.apply_mouse_event(mouse, area);
                handle_tui_input_action(
                    action,
                    &mut runtime,
                    &mut state,
                    &mut terminal,
                    &data_dir,
                    &session_store,
                    daemon.as_ref(),
                    &mut chat_projection,
                )?;
                terminal.draw(&state)?;
            }
            CrosstermEvent::Paste(text) => {
                state.insert_str(&text);
                terminal.draw(&state)?;
            }
            CrosstermEvent::Resize(_, _) => terminal.draw(&state)?,
            _ => {}
        }
    }

    Ok(())
}

fn handle_tui_input_action<H: TuiAgentRuntimeControl>(
    action: TuiInputAction,
    runtime: &mut InteractionRuntime<JsonlEventStore, H>,
    state: &mut TuiState,
    terminal: &mut TuiTerminalSession,
    data_dir: &SinomoDataDir,
    session_store: &TuiSessionStore,
    daemon: Option<&DaemonStatus>,
    chat_projection: &mut StableChatProjection,
) -> Result<(), CliError> {
    match action {
        TuiInputAction::None => {}
        TuiInputAction::Exit => {
            state.exit_requested = true;
            session_store.save_state(runtime.session(), state)?;
        }
        TuiInputAction::Submit(line) => {
            submit_tui_input(
                runtime,
                state,
                terminal,
                data_dir,
                session_store,
                daemon,
                line,
                chat_projection,
            )?;
        }
        TuiInputAction::InterruptAgent => {
            if runtime
                .handler
                .interrupt_tui_agent("user interrupted from TUI")?
            {
                state.mark_agent_interrupt_requested();
            } else {
                state.apply_output(InteractionOutput::Line(
                    "no active interruptible agent turn".to_string(),
                ));
            }
            session_store.save_state(runtime.session(), state)?;
        }
        TuiInputAction::OpenFiles(target) => {
            state.open_files_target(data_dir, target);
            session_store.save_state(runtime.session(), state)?;
        }
        TuiInputAction::OpenSessions => {
            open_tui_session_list_view(runtime, state, session_store)?;
        }
        TuiInputAction::OpenSettings => {
            open_tui_settings_view(state, data_dir)?;
            session_store.save_state(runtime.session(), state)?;
        }
        TuiInputAction::SaveSetting { key, value } => {
            save_tui_setting(state, data_dir, &key, &value)?;
            session_store.save_state(runtime.session(), state)?;
        }
        TuiInputAction::Resume(session_id) => {
            for output in
                resume_tui_session(runtime, state, session_store, data_dir, daemon, session_id)?
            {
                state.apply_output(output);
            }
            session_store.save_state(runtime.session(), state)?;
        }
    }
    sync_stable_chat_projection(chat_projection, daemon, runtime.session());
    Ok(())
}

fn submit_tui_input<H: TuiAgentRuntimeControl>(
    runtime: &mut InteractionRuntime<JsonlEventStore, H>,
    state: &mut TuiState,
    terminal: &mut TuiTerminalSession,
    data_dir: &SinomoDataDir,
    session_store: &TuiSessionStore,
    daemon: Option<&DaemonStatus>,
    line: String,
    chat_projection: &mut StableChatProjection,
) -> Result<(), CliError> {
    let command = match parse_tui_slash_command(&line) {
        Ok(command) => command,
        Err(error) => {
            state.begin_submission(&line);
            state.apply_output(InteractionOutput::Line(format!("command failed: {error}")));
            state.finish_submission();
            session_store.save_state(runtime.session(), state)?;
            terminal.draw(state)?;
            return Ok(());
        }
    };
    if let Some(command) = command {
        state.begin_submission(&line);
        let outputs =
            apply_tui_slash_command(runtime, state, data_dir, session_store, daemon, command);
        match outputs {
            Ok(outputs) => {
                for output in outputs {
                    state.apply_output(output);
                }
            }
            Err(error) => {
                state.apply_output(InteractionOutput::Line(format!("command failed: {error}")));
            }
        }
        state.finish_submission();
        session_store.save_state(runtime.session(), state)?;
        terminal.draw(state)?;
        return Ok(());
    }

    state.begin_submission(&line);
    terminal.draw(state)?;
    sync_stable_chat_projection(chat_projection, daemon, runtime.session());
    let stable_input_id = TaskId::generate();
    let stable_input_at = OffsetDateTime::now_utc();
    chat_projection.record_user_message(
        &state.runtime_info.agent,
        stable_input_id.clone(),
        stable_input_at,
        &line,
    )?;
    let result = (|| {
        let sandbox_node_id =
            parse_tui_tool_runtime_state_node_id(state.tool_runtime_node_id.as_deref())?;
        let session = runtime.session().clone();
        runtime.handler.configure_tui_tool_runtime(
            data_dir,
            &session,
            sandbox_node_id,
            state.workspace_projection(),
            state.daemon_connected,
        )?;

        let mut render_error = None;
        let mut emit = |output: InteractionOutput| {
            state.apply_output(output);
            if render_error.is_none()
                && let Err(error) = terminal.draw(state)
            {
                render_error = Some(error);
            }
        };
        let outputs = runtime.ingest_user_input_with_item_id_and_context_attachments_streaming(
            &line,
            stable_input_id.clone(),
            Vec::new(),
            &mut emit,
        )?;
        if let Some(error) = render_error {
            return Err(error);
        }
        Ok::<_, CliError>(outputs)
    })();
    match result {
        Ok(outputs) => {
            let completed_at = OffsetDateTime::now_utc();
            for (index, output) in outputs.into_iter().enumerate() {
                if let InteractionOutput::Line(text) = &output {
                    chat_projection.record_system_message(
                        &state.runtime_info.agent,
                        stable_tui_result_id(&stable_input_id, &format!("output-{index}"))?,
                        completed_at,
                        text,
                        false,
                    )?;
                }
                state.apply_output(output);
            }
        }
        Err(error) => {
            let message = format!("agent turn failed: {error}");
            let projection_result = chat_projection.record_system_message(
                &state.runtime_info.agent,
                stable_tui_result_id(&stable_input_id, "failed")?,
                OffsetDateTime::now_utc(),
                &message,
                true,
            );
            if let Err(projection_error) = projection_result {
                return Err(combine_stable_chat_errors(error, projection_error));
            }
            state.apply_output(InteractionOutput::Line(message));
        }
    }
    state.finish_submission();
    session_store.save_state(runtime.session(), state)?;
    Ok(())
}

fn refresh_tui_runtime_events<H: TuiAgentRuntimeControl>(
    runtime: &mut InteractionRuntime<JsonlEventStore, H>,
    state: &mut TuiState,
    session_store: &TuiSessionStore,
    daemon: Option<&DaemonStatus>,
    chat_projection: &mut StableChatProjection,
) -> Result<bool, CliError> {
    sync_stable_chat_projection(chat_projection, daemon, runtime.session());
    let events = runtime.handler.drain_tui_runtime_events()?;
    if events.is_empty() {
        return Ok(false);
    }
    let mut changed = false;
    for event in events {
        chat_projection.record_runtime_event(&state.runtime_info.agent, &event)?;
        changed |= state.apply_runtime_event(event);
    }
    if changed {
        session_store.save_state(runtime.session(), state)?;
    }
    Ok(changed)
}

fn sync_stable_chat_projection(
    projection: &mut StableChatProjection,
    daemon: Option<&DaemonStatus>,
    session: &InteractionSession,
) {
    if !projection.is_for_session(session) {
        *projection = match daemon {
            Some(daemon) => StableChatProjection::open_daemon_bounded(
                &daemon.address,
                Duration::from_millis(250),
                session,
            )
            .unwrap_or_else(|_| StableChatProjection::unavailable(session)),
            None => StableChatProjection::unavailable(session),
        };
    }
}

fn stable_tui_result_id(input_id: &TaskId, kind: &str) -> Result<TaskId, CliError> {
    TaskId::from_legacy_named(format!("cli-chat:{input_id}:{kind}")).map_err(|source| {
        CliError::Parse {
            detail: format!("derive stable TUI result id: {source}"),
        }
    })
}

fn combine_stable_chat_errors(original: CliError, projection: CliError) -> CliError {
    CliError::Parse {
        detail: format!(
            "{original}; recording the visible failure in stable chat history also failed: {projection}"
        ),
    }
}

struct TuiTerminalSession {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
}

impl TuiTerminalSession {
    fn enter() -> Result<Self, CliError> {
        enable_raw_mode().map_err(CliError::stdio)?;
        let mut stdout = io::stdout();
        if let Err(error) = execute!(stdout, EnterAlternateScreen, EnableMouseCapture) {
            let _ = disable_raw_mode();
            return Err(CliError::stdio(error));
        }
        let terminal = match Terminal::new(CrosstermBackend::new(stdout)) {
            Ok(terminal) => terminal,
            Err(error) => {
                let _ = disable_raw_mode();
                let mut stdout = io::stdout();
                let _ = execute!(stdout, DisableMouseCapture, LeaveAlternateScreen);
                return Err(CliError::stdio(error));
            }
        };
        Ok(Self { terminal })
    }

    fn draw(&mut self, state: &TuiState) -> Result<(), CliError> {
        self.terminal
            .draw(|frame| render_tui(frame, state))
            .map(|_| ())
            .map_err(CliError::stdio)
    }

    fn viewport(&self) -> Result<Rect, CliError> {
        let size = self.terminal.size().map_err(CliError::stdio)?;
        Ok(Rect::new(0, 0, size.width, size.height))
    }
}

impl Drop for TuiTerminalSession {
    fn drop(&mut self) {
        let _ = self.terminal.show_cursor();
        let _ = disable_raw_mode();
        let mut stdout = io::stdout();
        let _ = execute!(stdout, DisableMouseCapture, LeaveAlternateScreen);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TuiInputAction {
    None,
    Submit(String),
    InterruptAgent,
    OpenFiles(TuiFilesTarget),
    OpenSessions,
    OpenSettings,
    SaveSetting { key: String, value: String },
    Resume(SessionId),
    Exit,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TuiCompletionState {
    replace_start: usize,
    replace_end: usize,
    selected: Option<usize>,
    items: Vec<SlashCompletionItem>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TuiChatSelection {
    anchor: usize,
    focus: usize,
    dragging: bool,
}

impl TuiChatSelection {
    fn new(line: usize, dragging: bool) -> Self {
        Self {
            anchor: line,
            focus: line,
            dragging,
        }
    }

    fn range(&self) -> (usize, usize) {
        if self.anchor <= self.focus {
            (self.anchor, self.focus)
        } else {
            (self.focus, self.anchor)
        }
    }

    fn line_count(&self) -> usize {
        let (start, end) = self.range();
        end.saturating_sub(start).saturating_add(1)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TuiViewMode {
    Chat,
    Files,
    Sessions,
    Settings,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct TuiSessionRecord {
    schema_version: u32,
    session: InteractionSession,
    /// Conversation-owned ordered workspace grants. The first grant is the
    /// sandbox cwd; the agent session's legacy binding is only a migration
    /// source for records written before this field existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    workspaces: Vec<WorkspaceRef>,
    /// Zero identifies a legacy record where an empty `workspaces` field means
    /// "not projected yet". Current records write this even for an intentional
    /// empty grant set, so revoking the last workspace cannot resurrect the
    /// agent session's compatibility shadow.
    #[serde(default)]
    workspace_projection_version: u32,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
    title: String,
    message_count: usize,
    ended: bool,
    runtime_info: TuiAgentRuntimeInfo,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct TuiSessionTranscript {
    schema_version: u32,
    messages: Vec<TuiMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_model_usage: Option<TuiTokenUsage>,
    #[serde(default, skip_serializing_if = "TuiTokenUsage::is_empty")]
    session_token_usage: TuiTokenUsage,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
struct TuiTokenUsage {
    input_tokens: u64,
    output_tokens: u64,
}

impl TuiTokenUsage {
    fn from_language_model_usage(usage: &sinomo_lm::LanguageModelUsage) -> Option<Self> {
        let input_tokens = usage.input_tokens.unwrap_or(0);
        let output_tokens = usage.output_tokens.unwrap_or(0);
        if input_tokens == 0 && output_tokens == 0 {
            return None;
        }
        Some(Self {
            input_tokens,
            output_tokens,
        })
    }

    fn add(&mut self, usage: Self) {
        self.input_tokens = self.input_tokens.saturating_add(usage.input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(usage.output_tokens);
    }

    fn total_tokens(self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }

    fn is_empty(&self) -> bool {
        self.input_tokens == 0 && self.output_tokens == 0
    }
}

#[derive(Clone, Debug)]
struct TuiSessionStore {
    root: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TuiSessionListState {
    sessions: Vec<TuiSessionRecord>,
    selected: usize,
}

impl TuiSessionListState {
    fn new(sessions: Vec<TuiSessionRecord>, current_session_id: &str) -> Self {
        let selected = sessions
            .iter()
            .position(|session| session.session.session_id.to_string() == current_session_id)
            .unwrap_or(0);
        Self { sessions, selected }
    }

    fn selected_session_id(&self) -> Option<SessionId> {
        self.sessions
            .get(self.selected)
            .map(|record| record.session.session_id.clone())
    }

    fn move_selection(&mut self, delta: isize) {
        if self.sessions.is_empty() {
            self.selected = 0;
            return;
        }
        let last = self.sessions.len() - 1;
        if delta.is_negative() {
            self.selected = self.selected.saturating_sub(delta.unsigned_abs());
        } else {
            self.selected = self.selected.saturating_add(delta as usize).min(last);
        }
    }
}

impl TuiSessionStore {
    fn for_data_dir(data_dir: &SinomoDataDir) -> Self {
        Self {
            root: data_dir.root().join("events").join("tui").join("sessions"),
        }
    }

    fn session_dir(&self, session_id: &SessionId) -> PathBuf {
        self.root.join(session_id.to_string())
    }

    fn record_path_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.session_dir(session_id).join("session.json")
    }

    fn transcript_path_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.session_dir(session_id).join("transcript.json")
    }

    fn events_path_for_session(&self, session_id: &SessionId) -> PathBuf {
        self.session_dir(session_id).join("events.jsonl")
    }

    fn save_state(&self, session: &InteractionSession, state: &TuiState) -> Result<(), CliError> {
        if state.is_empty_session() {
            self.discard_session(&session.session_id)?;
            return Ok(());
        }

        let now = OffsetDateTime::now_utc();
        let existing = self.load_record(&session.session_id)?;
        let created_at = existing
            .as_ref()
            .map(|record| record.created_at)
            .unwrap_or(now);
        let record = TuiSessionRecord {
            schema_version: TUI_SESSION_SCHEMA_VERSION,
            session: session.clone(),
            workspaces: state.workspaces.clone(),
            workspace_projection_version: TUI_WORKSPACE_PROJECTION_VERSION,
            created_at,
            updated_at: now,
            title: tui_session_title(&state.messages),
            message_count: state.messages.len(),
            ended: state.exit_requested,
            runtime_info: state.runtime_info.clone(),
        };
        write_tui_json_atomic(&self.record_path_for_session(&session.session_id), &record)?;
        let transcript = TuiSessionTranscript {
            schema_version: TUI_SESSION_SCHEMA_VERSION,
            messages: state.messages.clone(),
            last_model_usage: state.last_model_usage,
            session_token_usage: state.session_token_usage,
        };
        write_tui_json_atomic(
            &self.transcript_path_for_session(&session.session_id),
            &transcript,
        )
    }

    fn discard_session(&self, session_id: &SessionId) -> Result<(), CliError> {
        let path = self.session_dir(session_id);
        match fs::remove_dir_all(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(CliError::Io {
                path: path.display().to_string(),
                detail: error.to_string(),
            }),
        }
    }

    fn load_record(&self, session_id: &SessionId) -> Result<Option<TuiSessionRecord>, CliError> {
        let path = self.record_path_for_session(session_id);
        if !path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&path).map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        let record: TuiSessionRecord =
            serde_json::from_str(&contents).map_err(|source| CliError::Json {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        if record.schema_version != TUI_SESSION_SCHEMA_VERSION {
            return Err(CliError::Json {
                path: path.display().to_string(),
                detail: format!("unsupported TUI session schema {}", record.schema_version),
            });
        }
        Ok(Some(record))
    }

    fn load_transcript(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<TuiSessionTranscript>, CliError> {
        let path = self.transcript_path_for_session(session_id);
        if !path.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(&path).map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        let transcript: TuiSessionTranscript =
            serde_json::from_str(&contents).map_err(|source| CliError::Json {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        if transcript.schema_version != TUI_SESSION_SCHEMA_VERSION {
            return Err(CliError::Json {
                path: path.display().to_string(),
                detail: format!(
                    "unsupported TUI transcript schema {}",
                    transcript.schema_version
                ),
            });
        }
        Ok(Some(transcript))
    }

    fn load_session(&self, session_id: &SessionId) -> Result<TuiSessionRecord, CliError> {
        self.load_record(session_id)?
            .ok_or_else(|| CliError::Parse {
                detail: format!("unknown session `{session_id}`"),
            })
    }

    fn list_sessions(&self) -> Result<Vec<TuiSessionRecord>, CliError> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let entries = fs::read_dir(&self.root).map_err(|source| CliError::Io {
            path: self.root.display().to_string(),
            detail: source.to_string(),
        })?;
        let mut sessions = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|source| CliError::Io {
                path: self.root.display().to_string(),
                detail: source.to_string(),
            })?;
            if !entry.path().is_dir() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let Ok(session_id) = SessionId::from_str(&name) else {
                continue;
            };
            if let Some(record) = self.load_record(&session_id)? {
                if self
                    .load_transcript(&session_id)?
                    .is_some_and(|transcript| tui_session_messages_are_empty(&transcript.messages))
                {
                    self.discard_session(&session_id)?;
                    continue;
                }
                sessions.push(record);
            }
        }
        sessions.sort_by_key(|session| std::cmp::Reverse(session.updated_at));
        Ok(sessions)
    }
}

#[derive(Clone, Debug, PartialEq)]
struct TuiSettingsEntry {
    definition: sinomo_runtime::config::ConfigDefinition,
    source: Option<sinomo_runtime::config::ConfigValueSource>,
    value: Option<serde_json::Value>,
    error: Option<String>,
}

impl TuiSettingsEntry {
    fn key(&self) -> &str {
        self.definition.key.as_str()
    }

    fn value_preview(&self) -> String {
        if tui_config_key_is_sensitive(self.key()) && self.value.is_some() {
            return "<redacted>".to_string();
        }
        match &self.value {
            Some(value) => compact_tui_json(value),
            None => self.error.clone().unwrap_or_else(|| "unset".to_string()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct TuiSettingsState {
    entries: Vec<TuiSettingsEntry>,
    selected: usize,
    editing_key: Option<String>,
}

impl TuiSettingsState {
    fn load(data_dir: &SinomoDataDir) -> Result<Self, CliError> {
        let store = main_agent_config_store()?;
        let mut entries = store
            .registry()
            .definitions()
            .map(|definition| {
                let resolved = store.get(data_dir, None, definition.key.as_str());
                match resolved {
                    Ok(resolution) => TuiSettingsEntry {
                        definition: resolution.definition,
                        source: Some(resolution.source),
                        value: Some(resolution.value),
                        error: None,
                    },
                    Err(error) => TuiSettingsEntry {
                        definition: definition.clone(),
                        source: None,
                        value: None,
                        error: Some(error.to_string()),
                    },
                }
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.key().cmp(right.key()));
        Ok(Self {
            entries,
            selected: 0,
            editing_key: None,
        })
    }

    fn selected_entry(&self) -> Option<&TuiSettingsEntry> {
        self.entries.get(self.selected)
    }

    fn move_selection(&mut self, delta: isize) {
        if self.entries.is_empty() {
            self.selected = 0;
            return;
        }
        let last = self.entries.len() - 1;
        if delta.is_negative() {
            self.selected = self.selected.saturating_sub(delta.unsigned_abs());
        } else {
            self.selected = self.selected.saturating_add(delta as usize).min(last);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TuiFilesTarget {
    Local(PathBuf),
    Sifs(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TuiSlashCommand {
    Agent(TuiAgentCommand),
    Files { target: TuiFilesTarget },
    Model(TuiModelCommand),
    ToolRuntime(TuiToolRuntimeCommand),
    Settings,
    Sessions,
    Resume { session_id: SessionId },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TuiAgentCommand {
    Status,
    List,
    Use(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TuiModelCommand {
    Status,
    Use(TuiModelSelection),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TuiToolRuntimeCommand {
    Status,
    Local,
    Node(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TuiModelSelection {
    provider: String,
    model: String,
    account: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct TuiAgentRuntimeInfo {
    agent: String,
    provider: String,
    model: String,
    account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reasoning_effort: Option<ProviderReasoningEffort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reasoning_budget_tokens: Option<u32>,
    #[serde(
        default,
        skip_serializing_if = "sinomo_agent::AgentToolRuntimeDefinition::is_default"
    )]
    tool_runtime: sinomo_agent::AgentToolRuntimeDefinition,
}

impl TuiAgentRuntimeInfo {
    fn model_label(&self) -> String {
        match self
            .account
            .as_deref()
            .filter(|account| !account.is_empty())
        {
            Some(account) => format!("{}/{}@{}", self.provider, self.model, account),
            None => format!("{}/{}", self.provider, self.model),
        }
    }

    fn reasoning_label(&self) -> String {
        let effort = self
            .reasoning_effort
            .map(|effort| effort.to_string())
            .unwrap_or_else(|| "default".to_string());
        match self.reasoning_budget_tokens {
            Some(tokens) => format!("{effort}/{}", format_tui_token_count(u64::from(tokens))),
            None => effort,
        }
    }
}

impl Default for TuiAgentRuntimeInfo {
    fn default() -> Self {
        Self {
            agent: "main-agent".to_string(),
            provider: MINICPM_PROVIDER_ID.to_string(),
            model: MINICPM_DEFAULT_MODEL.to_string(),
            account: None,
            reasoning_effort: None,
            reasoning_budget_tokens: None,
            tool_runtime: sinomo_agent::AgentToolRuntimeDefinition::default(),
        }
    }
}

trait TuiAgentRuntimeControl: InteractionEventHandler {
    fn tui_agent_runtime_info(&self) -> TuiAgentRuntimeInfo;

    fn drain_tui_runtime_events(&mut self) -> Result<Vec<AgentRuntimeEvent>, CliError> {
        Ok(Vec::new())
    }

    fn interrupt_tui_agent(&mut self, _reason: &str) -> Result<bool, CliError> {
        Ok(false)
    }

    fn switch_tui_model(
        &mut self,
        data_dir: &SinomoDataDir,
        selection: TuiModelSelection,
        daemon_connected: bool,
    ) -> Result<TuiAgentRuntimeInfo, CliError>;

    fn switch_tui_agent(
        &mut self,
        data_dir: &SinomoDataDir,
        daemon_connected: bool,
        definition: AgentDefinition,
    ) -> Result<TuiAgentRuntimeInfo, CliError>;

    fn switch_tui_agent_for_session(
        &mut self,
        data_dir: &SinomoDataDir,
        session: &InteractionSession,
        workspaces: Option<&[WorkspaceRef]>,
        daemon_connected: bool,
        definition: AgentDefinition,
    ) -> Result<TuiAgentRuntimeInfo, CliError> {
        let _ = session;
        let _ = workspaces;
        self.switch_tui_agent(data_dir, daemon_connected, definition)
    }

    fn prepare_tui_agent(
        &mut self,
        data_dir: &SinomoDataDir,
        daemon_connected: bool,
        definition: AgentDefinition,
    ) -> Result<TuiAgentRuntimeInfo, CliError> {
        self.switch_tui_agent(data_dir, daemon_connected, definition)
    }

    fn snapshot_tui_resume(
        &mut self,
        _session: &InteractionSession,
    ) -> Result<Option<TuiAgentResumeSnapshot>, CliError> {
        Ok(None)
    }

    fn rollback_tui_resume(
        &mut self,
        _data_dir: &SinomoDataDir,
        _snapshot: Option<TuiAgentResumeSnapshot>,
    ) -> Result<(), CliError> {
        Ok(())
    }

    fn load_tui_agent_session(
        &self,
        _session_id: &SessionId,
    ) -> Result<Option<AgentSession>, CliError> {
        Ok(None)
    }

    fn persist_tui_agent_session(
        &mut self,
        _data_dir: &SinomoDataDir,
        _session: &InteractionSession,
    ) -> Result<(), CliError> {
        Ok(())
    }

    fn configure_tui_tool_runtime(
        &mut self,
        _data_dir: &SinomoDataDir,
        _session: &InteractionSession,
        _sandbox_node_id: Option<NodeId>,
        _workspaces: Option<&[WorkspaceRef]>,
        _daemon_connected: bool,
    ) -> Result<(), CliError> {
        Ok(())
    }

    fn restore_tui_agent_session(
        &mut self,
        _data_dir: &SinomoDataDir,
        _session: &InteractionSession,
        _persisted: Option<AgentSession>,
        _transcript: sinomo_agent::AgentTranscript,
    ) -> Result<(), CliError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
enum TuiMessageKind {
    User,
    Command,
    Agent,
    Thinking,
    System,
    Error,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct TuiMessage {
    kind: TuiMessageKind,
    text: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct TuiRuntimeActivity {
    active: bool,
    active_command: Option<String>,
}

#[derive(Clone, Debug)]
struct TuiState {
    session_id: String,
    workspaces: Vec<WorkspaceRef>,
    workspace_projection_version: u32,
    daemon_connected: bool,
    cluster_label: Option<String>,
    runtime_info: TuiAgentRuntimeInfo,
    tool_runtime_node_id: Option<String>,
    tool_runtime_explicit_override: bool,
    runtime_activity: TuiRuntimeActivity,
    last_model_usage: Option<TuiTokenUsage>,
    session_token_usage: TuiTokenUsage,
    view_mode: TuiViewMode,
    file_browser: Option<FileManagerState>,
    file_prompt: Option<FileManagerPrompt>,
    session_list: Option<TuiSessionListState>,
    settings: Option<TuiSettingsState>,
    input: String,
    input_cursor: usize,
    messages: Vec<TuiMessage>,
    stream_message: Option<usize>,
    suppress_structured_model_stream: bool,
    chat_selection: Option<TuiChatSelection>,
    completions: Option<TuiCompletionState>,
    status: String,
    scrollback: u16,
    exit_requested: bool,
}

impl TuiState {
    fn new(
        session: &InteractionSession,
        daemon: Option<&DaemonStatus>,
        runtime_info: TuiAgentRuntimeInfo,
    ) -> Self {
        let cluster_label = daemon
            .and_then(|status| status.cluster.as_ref())
            .map(|cluster| format!("cluster {}", cluster.cluster_id));
        let mut state = Self {
            session_id: session.session_id.to_string(),
            workspaces: initial_tui_conversation_workspaces(),
            workspace_projection_version: TUI_WORKSPACE_PROJECTION_VERSION,
            daemon_connected: daemon.is_some(),
            cluster_label,
            tool_runtime_node_id: runtime_info.tool_runtime.sandbox_node_id.clone(),
            runtime_info,
            tool_runtime_explicit_override: false,
            runtime_activity: TuiRuntimeActivity::default(),
            last_model_usage: None,
            session_token_usage: TuiTokenUsage::default(),
            view_mode: TuiViewMode::Chat,
            file_browser: None,
            file_prompt: None,
            session_list: None,
            settings: None,
            input: String::new(),
            input_cursor: 0,
            messages: Vec::new(),
            stream_message: None,
            suppress_structured_model_stream: false,
            chat_selection: None,
            completions: None,
            status: "listening".to_string(),
            scrollback: 0,
            exit_requested: false,
        };
        state.push_message(TuiMessageKind::System, "main agent session started");
        state
    }

    fn workspace_projection(&self) -> Option<&[WorkspaceRef]> {
        if self.workspace_projection_version > 0 || !self.workspaces.is_empty() {
            Some(&self.workspaces)
        } else {
            None
        }
    }

    fn apply_key_event(&mut self, key: KeyEvent) -> TuiInputAction {
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return TuiInputAction::None;
        }

        match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                if self.runtime_activity.active {
                    TuiInputAction::InterruptAgent
                } else {
                    TuiInputAction::Exit
                }
            }
            KeyCode::F(1) => {
                self.open_chat_tab();
                TuiInputAction::None
            }
            KeyCode::F(2) => self.toggle_file_browser(),
            KeyCode::F(3) => TuiInputAction::OpenSessions,
            KeyCode::F(4) => TuiInputAction::OpenSettings,
            _ if self.view_mode == TuiViewMode::Files => self.apply_file_browser_key_event(key),
            _ if self.view_mode == TuiViewMode::Sessions => self.apply_session_list_key_event(key),
            _ if self.view_mode == TuiViewMode::Settings => self.apply_settings_key_event(key),
            KeyCode::Char('s')
                if self.view_mode == TuiViewMode::Chat
                    && key.modifiers.contains(KeyModifiers::ALT) =>
            {
                self.toggle_chat_selection_mode();
                TuiInputAction::None
            }
            KeyCode::Esc
                if self.view_mode == TuiViewMode::Chat && self.chat_selection.is_some() =>
            {
                self.clear_chat_selection();
                TuiInputAction::None
            }
            _ if self.view_mode == TuiViewMode::Chat && self.chat_selection.is_some() => {
                self.apply_chat_selection_key_event(key)
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.input.clear();
                self.input_cursor = 0;
                self.refresh_completions();
                TuiInputAction::None
            }
            KeyCode::Esc => TuiInputAction::Exit,
            KeyCode::Enter => {
                if self.accept_completion() {
                    TuiInputAction::None
                } else {
                    self.take_submission()
                }
            }
            KeyCode::Tab => {
                self.accept_completion();
                TuiInputAction::None
            }
            KeyCode::Backspace => {
                self.backspace();
                TuiInputAction::None
            }
            KeyCode::Delete => {
                self.delete();
                TuiInputAction::None
            }
            KeyCode::Left => {
                if !self.step_slash_completion_back() {
                    self.move_cursor_left();
                }
                TuiInputAction::None
            }
            KeyCode::Right => {
                if !self.accept_completion() {
                    self.move_cursor_right();
                }
                TuiInputAction::None
            }
            KeyCode::Up => {
                if self.completions.is_some() {
                    self.select_previous_completion();
                }
                TuiInputAction::None
            }
            KeyCode::Down => {
                if self.completions.is_some() {
                    self.select_next_completion();
                }
                TuiInputAction::None
            }
            KeyCode::Home => {
                self.input_cursor = 0;
                self.refresh_completions();
                TuiInputAction::None
            }
            KeyCode::End => {
                self.input_cursor = self.input.len();
                self.refresh_completions();
                TuiInputAction::None
            }
            KeyCode::PageUp => {
                self.scrollback = self.scrollback.saturating_add(8);
                TuiInputAction::None
            }
            KeyCode::PageDown => {
                self.scrollback = self.scrollback.saturating_sub(8);
                TuiInputAction::None
            }
            KeyCode::Char(character)
                if !key.modifiers.contains(KeyModifiers::CONTROL)
                    && !key.modifiers.contains(KeyModifiers::ALT) =>
            {
                self.insert_char(character);
                TuiInputAction::None
            }
            _ => TuiInputAction::None,
        }
    }

    fn apply_mouse_event(&mut self, mouse: MouseEvent, area: Rect) -> TuiInputAction {
        let layout = tui_layout_areas(area, self);
        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                self.apply_mouse_left_click(&layout, mouse.column, mouse.row)
            }
            MouseEventKind::Drag(MouseButton::Left) => {
                self.apply_mouse_left_drag(&layout, mouse.column, mouse.row);
                TuiInputAction::None
            }
            MouseEventKind::Up(MouseButton::Left) => {
                self.finish_mouse_chat_selection();
                TuiInputAction::None
            }
            MouseEventKind::ScrollUp => {
                self.apply_mouse_scroll(&layout, mouse.column, mouse.row, true);
                TuiInputAction::None
            }
            MouseEventKind::ScrollDown => {
                self.apply_mouse_scroll(&layout, mouse.column, mouse.row, false);
                TuiInputAction::None
            }
            _ => TuiInputAction::None,
        }
    }

    fn apply_mouse_left_click(
        &mut self,
        layout: &TuiLayoutAreas,
        column: u16,
        row: u16,
    ) -> TuiInputAction {
        if let Some(tab) = tui_menu_tab_at(layout.menu, column, row) {
            return match tab {
                TuiViewMode::Chat => {
                    self.open_chat_tab();
                    TuiInputAction::None
                }
                TuiViewMode::Files => {
                    if self.view_mode != TuiViewMode::Files {
                        TuiInputAction::OpenFiles(default_tui_files_target())
                    } else {
                        TuiInputAction::None
                    }
                }
                TuiViewMode::Sessions => {
                    if self.view_mode == TuiViewMode::Sessions {
                        TuiInputAction::None
                    } else {
                        TuiInputAction::OpenSessions
                    }
                }
                TuiViewMode::Settings => {
                    if self.view_mode == TuiViewMode::Settings {
                        TuiInputAction::None
                    } else {
                        TuiInputAction::OpenSettings
                    }
                }
            };
        }

        match self.view_mode {
            TuiViewMode::Chat => {
                let (_, transcript_area) = tui_chat_areas(layout.main);
                if self.select_chat_line_at(transcript_area, column, row, true) {
                    return TuiInputAction::None;
                }
                if let Some(completions_area) = layout.completions {
                    self.select_completion_at(completions_area, column, row);
                }
            }
            TuiViewMode::Files => {
                let (entries_area, _) = tui_file_browser_areas(layout.main);
                self.select_file_browser_entry_at(entries_area, column, row);
            }
            TuiViewMode::Sessions => {
                self.select_session_at(layout.main, column, row);
            }
            TuiViewMode::Settings => {
                let (entries_area, _) = tui_settings_areas(layout.main);
                self.select_setting_at(entries_area, column, row);
            }
        }
        TuiInputAction::None
    }

    fn apply_mouse_left_drag(&mut self, layout: &TuiLayoutAreas, column: u16, row: u16) {
        if self.view_mode != TuiViewMode::Chat {
            return;
        }
        let Some(selection) = self.chat_selection else {
            return;
        };
        if !selection.dragging {
            return;
        }
        let (_, transcript_area) = tui_chat_areas(layout.main);
        self.extend_chat_selection_at(transcript_area, column, row, true);
    }

    fn apply_mouse_scroll(&mut self, layout: &TuiLayoutAreas, column: u16, row: u16, up: bool) {
        match self.view_mode {
            TuiViewMode::Chat => {
                if let Some(completions_area) = layout.completions
                    && rect_contains(completions_area, column, row)
                {
                    if up {
                        self.select_previous_completion();
                    } else {
                        self.select_next_completion();
                    }
                    return;
                }
                let (_, transcript_area) = tui_chat_areas(layout.main);
                if rect_contains(transcript_area, column, row) {
                    if up {
                        self.scrollback = self
                            .scrollback
                            .saturating_add(TUI_MOUSE_SCROLL_LINES as u16);
                    } else {
                        self.scrollback = self
                            .scrollback
                            .saturating_sub(TUI_MOUSE_SCROLL_LINES as u16);
                    }
                }
            }
            TuiViewMode::Files => {
                let (entries_area, _) = tui_file_browser_areas(layout.main);
                let entries_inner = tui_inner_rect(entries_area);
                if rect_contains(entries_inner, column, row)
                    && let Some(browser) = self.file_browser.as_mut()
                {
                    browser.move_selection(if up {
                        -TUI_MOUSE_SCROLL_LINES
                    } else {
                        TUI_MOUSE_SCROLL_LINES
                    });
                }
            }
            TuiViewMode::Sessions => {
                let list_inner = tui_inner_rect(layout.main);
                if rect_contains(list_inner, column, row)
                    && let Some(list) = self.session_list.as_mut()
                {
                    list.move_selection(if up {
                        -TUI_MOUSE_SCROLL_LINES
                    } else {
                        TUI_MOUSE_SCROLL_LINES
                    });
                }
            }
            TuiViewMode::Settings => {
                let (entries_area, _) = tui_settings_areas(layout.main);
                let entries_inner = tui_inner_rect(entries_area);
                if rect_contains(entries_inner, column, row)
                    && let Some(settings) = self.settings.as_mut()
                {
                    settings.move_selection(if up {
                        -TUI_MOUSE_SCROLL_LINES
                    } else {
                        TUI_MOUSE_SCROLL_LINES
                    });
                }
            }
        }
    }

    fn select_completion_at(&mut self, area: Rect, column: u16, row: u16) {
        let inner = tui_inner_rect(area);
        if !rect_contains(inner, column, row) {
            return;
        }
        let selected = {
            let Some(completions) = self.completions.as_mut() else {
                return;
            };
            let visible_height = usize::from(inner.height);
            let (visible_start, visible_end) = completion_visible_range(
                completions.items.len(),
                completions.selected,
                visible_height,
            );
            let index = visible_start + usize::from(row - inner.y);
            if index >= visible_end {
                return;
            }
            completions.selected = Some(index);
            true
        };
        if selected {
            self.apply_selected_completion_preview();
        }
    }

    fn select_chat_line_at(&mut self, area: Rect, column: u16, row: u16, dragging: bool) -> bool {
        let Some(line) = self.chat_line_index_at(area, column, row, false) else {
            return false;
        };
        self.chat_selection = Some(TuiChatSelection::new(line, dragging));
        self.completions = None;
        self.update_chat_selection_status();
        true
    }

    fn extend_chat_selection_at(&mut self, area: Rect, column: u16, row: u16, clamp: bool) -> bool {
        let Some(line) = self.chat_line_index_at(area, column, row, clamp) else {
            return false;
        };
        let Some(selection) = self.chat_selection.as_mut() else {
            return false;
        };
        selection.focus = line;
        self.update_chat_selection_status();
        true
    }

    fn chat_line_index_at(&self, area: Rect, column: u16, row: u16, clamp: bool) -> Option<usize> {
        let line_count = tui_message_line_count(&self.messages);
        if line_count == 0 {
            return None;
        }
        let inner = tui_inner_rect(area);
        if inner.width == 0 || inner.height == 0 {
            return None;
        }
        if !clamp && !rect_contains(inner, column, row) {
            return None;
        }
        if clamp && (column < inner.x || column >= inner.x.saturating_add(inner.width)) {
            return None;
        }

        let visible_height = usize::from(inner.height);
        let visible_start =
            tui_transcript_visible_start(line_count, visible_height, self.scrollback);
        let visible_row = if row < inner.y {
            0
        } else if row >= inner.y.saturating_add(inner.height) {
            visible_height.saturating_sub(1)
        } else {
            usize::from(row - inner.y)
        };
        let line = visible_start.saturating_add(visible_row);
        if line < line_count {
            Some(line)
        } else if clamp {
            Some(line_count - 1)
        } else {
            None
        }
    }

    fn select_file_browser_entry_at(&mut self, area: Rect, column: u16, row: u16) {
        let inner = tui_inner_rect(area);
        if !rect_contains(inner, column, row) {
            return;
        }
        let Some(browser) = self.file_browser.as_mut() else {
            return;
        };
        let visible_height = usize::from(inner.height);
        let (visible_start, visible_end) = completion_visible_range(
            browser.entries.len(),
            Some(browser.selected),
            visible_height,
        );
        let index = visible_start + usize::from(row - inner.y);
        if index < visible_end {
            browser.selected = index;
            browser.update_preview();
        }
    }

    fn select_session_at(&mut self, area: Rect, column: u16, row: u16) {
        let inner = tui_inner_rect(area);
        if !rect_contains(inner, column, row) {
            return;
        }
        let Some(list) = self.session_list.as_mut() else {
            return;
        };
        let visible_height = usize::from(inner.height);
        let (visible_start, visible_end) =
            completion_visible_range(list.sessions.len(), Some(list.selected), visible_height);
        let index = visible_start + usize::from(row - inner.y);
        if index < visible_end {
            list.selected = index;
        }
    }

    fn select_setting_at(&mut self, area: Rect, column: u16, row: u16) {
        let inner = tui_inner_rect(area);
        if !rect_contains(inner, column, row) {
            return;
        }
        let Some(settings) = self.settings.as_mut() else {
            return;
        };
        let visible_height = usize::from(inner.height);
        let (visible_start, visible_end) = completion_visible_range(
            settings.entries.len(),
            Some(settings.selected),
            visible_height,
        );
        let index = visible_start + usize::from(row - inner.y);
        if index < visible_end {
            settings.selected = index;
        }
    }

    fn apply_session_list_key_event(&mut self, key: KeyEvent) -> TuiInputAction {
        match key.code {
            KeyCode::Esc => {
                self.close_session_list();
                TuiInputAction::None
            }
            KeyCode::Enter => self
                .session_list
                .as_ref()
                .and_then(TuiSessionListState::selected_session_id)
                .map(TuiInputAction::Resume)
                .unwrap_or(TuiInputAction::None),
            KeyCode::Up => {
                if let Some(list) = self.session_list.as_mut() {
                    list.move_selection(-1);
                }
                TuiInputAction::None
            }
            KeyCode::Down => {
                if let Some(list) = self.session_list.as_mut() {
                    list.move_selection(1);
                }
                TuiInputAction::None
            }
            KeyCode::PageUp => {
                if let Some(list) = self.session_list.as_mut() {
                    list.move_selection(-8);
                }
                TuiInputAction::None
            }
            KeyCode::PageDown => {
                if let Some(list) = self.session_list.as_mut() {
                    list.move_selection(8);
                }
                TuiInputAction::None
            }
            KeyCode::Home => {
                if let Some(list) = self.session_list.as_mut() {
                    list.selected = 0;
                }
                TuiInputAction::None
            }
            KeyCode::End => {
                if let Some(list) = self.session_list.as_mut()
                    && !list.sessions.is_empty()
                {
                    list.selected = list.sessions.len() - 1;
                }
                TuiInputAction::None
            }
            KeyCode::Char('r') if !key.modifiers.contains(KeyModifiers::CONTROL) => self
                .session_list
                .as_ref()
                .and_then(TuiSessionListState::selected_session_id)
                .map(TuiInputAction::Resume)
                .unwrap_or(TuiInputAction::None),
            _ => TuiInputAction::None,
        }
    }

    fn apply_settings_key_event(&mut self, key: KeyEvent) -> TuiInputAction {
        if self
            .settings
            .as_ref()
            .is_some_and(|settings| settings.editing_key.is_some())
        {
            return match key.code {
                KeyCode::Esc => {
                    self.cancel_setting_edit();
                    TuiInputAction::None
                }
                KeyCode::Enter => {
                    let Some(key) = self
                        .settings
                        .as_ref()
                        .and_then(|settings| settings.editing_key.clone())
                    else {
                        return TuiInputAction::None;
                    };
                    let value = std::mem::take(&mut self.input);
                    self.input_cursor = 0;
                    TuiInputAction::SaveSetting { key, value }
                }
                KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.input.clear();
                    self.input_cursor = 0;
                    TuiInputAction::None
                }
                KeyCode::Backspace => {
                    self.backspace();
                    TuiInputAction::None
                }
                KeyCode::Delete => {
                    self.delete();
                    TuiInputAction::None
                }
                KeyCode::Left => {
                    self.move_cursor_left();
                    TuiInputAction::None
                }
                KeyCode::Right => {
                    self.move_cursor_right();
                    TuiInputAction::None
                }
                KeyCode::Home => {
                    self.input_cursor = 0;
                    TuiInputAction::None
                }
                KeyCode::End => {
                    self.input_cursor = self.input.len();
                    TuiInputAction::None
                }
                KeyCode::Char(character)
                    if !key.modifiers.contains(KeyModifiers::CONTROL)
                        && !key.modifiers.contains(KeyModifiers::ALT) =>
                {
                    self.input.insert(self.input_cursor, character);
                    self.input_cursor += character.len_utf8();
                    TuiInputAction::None
                }
                _ => TuiInputAction::None,
            };
        }

        match key.code {
            KeyCode::Esc => {
                self.close_settings();
                TuiInputAction::None
            }
            KeyCode::Enter | KeyCode::Char('e')
                if !key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                self.start_setting_edit();
                TuiInputAction::None
            }
            KeyCode::Char('r') if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                TuiInputAction::OpenSettings
            }
            KeyCode::Up => {
                if let Some(settings) = self.settings.as_mut() {
                    settings.move_selection(-1);
                }
                TuiInputAction::None
            }
            KeyCode::Down => {
                if let Some(settings) = self.settings.as_mut() {
                    settings.move_selection(1);
                }
                TuiInputAction::None
            }
            KeyCode::PageUp => {
                if let Some(settings) = self.settings.as_mut() {
                    settings.move_selection(-8);
                }
                TuiInputAction::None
            }
            KeyCode::PageDown => {
                if let Some(settings) = self.settings.as_mut() {
                    settings.move_selection(8);
                }
                TuiInputAction::None
            }
            KeyCode::Home => {
                if let Some(settings) = self.settings.as_mut() {
                    settings.selected = 0;
                }
                TuiInputAction::None
            }
            KeyCode::End => {
                if let Some(settings) = self.settings.as_mut()
                    && !settings.entries.is_empty()
                {
                    settings.selected = settings.entries.len() - 1;
                }
                TuiInputAction::None
            }
            _ => TuiInputAction::None,
        }
    }

    fn apply_chat_selection_key_event(&mut self, key: KeyEvent) -> TuiInputAction {
        match key.code {
            KeyCode::Esc => self.clear_chat_selection(),
            KeyCode::Up => self.move_chat_selection_focus(-1),
            KeyCode::Down => self.move_chat_selection_focus(1),
            KeyCode::PageUp => {
                self.move_chat_selection_focus(-8);
                self.scrollback = self.scrollback.saturating_add(8);
            }
            KeyCode::PageDown => {
                self.move_chat_selection_focus(8);
                self.scrollback = self.scrollback.saturating_sub(8);
            }
            KeyCode::Home => self.set_chat_selection_focus(0),
            KeyCode::End => {
                if let Some(last) = tui_message_line_count(&self.messages).checked_sub(1) {
                    self.set_chat_selection_focus(last);
                }
            }
            KeyCode::Char('a') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.select_all_chat_lines();
            }
            _ => {}
        }
        TuiInputAction::None
    }

    fn apply_file_browser_key_event(&mut self, key: KeyEvent) -> TuiInputAction {
        if self.file_prompt.is_some() {
            return match key.code {
                KeyCode::Esc => {
                    self.clear_file_prompt();
                    TuiInputAction::None
                }
                KeyCode::Enter => {
                    self.submit_file_prompt();
                    TuiInputAction::None
                }
                KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.input.clear();
                    self.input_cursor = 0;
                    TuiInputAction::None
                }
                KeyCode::Backspace => {
                    self.backspace();
                    TuiInputAction::None
                }
                KeyCode::Delete => {
                    self.delete();
                    TuiInputAction::None
                }
                KeyCode::Left => {
                    self.move_cursor_left();
                    TuiInputAction::None
                }
                KeyCode::Right => {
                    self.move_cursor_right();
                    TuiInputAction::None
                }
                KeyCode::Home => {
                    self.input_cursor = 0;
                    TuiInputAction::None
                }
                KeyCode::End => {
                    self.input_cursor = self.input.len();
                    TuiInputAction::None
                }
                KeyCode::Char(character)
                    if !key.modifiers.contains(KeyModifiers::CONTROL)
                        && !key.modifiers.contains(KeyModifiers::ALT) =>
                {
                    self.input.insert(self.input_cursor, character);
                    self.input_cursor += character.len_utf8();
                    TuiInputAction::None
                }
                _ => TuiInputAction::None,
            };
        }

        match key.code {
            KeyCode::Esc => {
                self.close_file_browser();
                TuiInputAction::None
            }
            KeyCode::Enter => {
                self.with_file_browser(|browser| browser.open_selected());
                TuiInputAction::None
            }
            KeyCode::Backspace => {
                self.with_file_browser(|browser| browser.go_parent());
                TuiInputAction::None
            }
            KeyCode::Up => {
                if let Some(browser) = self.file_browser.as_mut() {
                    browser.move_selection(-1);
                }
                TuiInputAction::None
            }
            KeyCode::Down => {
                if let Some(browser) = self.file_browser.as_mut() {
                    browser.move_selection(1);
                }
                TuiInputAction::None
            }
            KeyCode::PageUp => {
                if let Some(browser) = self.file_browser.as_mut() {
                    browser.move_selection(-8);
                }
                TuiInputAction::None
            }
            KeyCode::PageDown => {
                if let Some(browser) = self.file_browser.as_mut() {
                    browser.move_selection(8);
                }
                TuiInputAction::None
            }
            KeyCode::Home => {
                if let Some(browser) = self.file_browser.as_mut() {
                    browser.select_first();
                }
                TuiInputAction::None
            }
            KeyCode::End => {
                if let Some(browser) = self.file_browser.as_mut() {
                    browser.select_last();
                }
                TuiInputAction::None
            }
            KeyCode::Char('n') if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.start_file_prompt(FileManagerPrompt::NewFile);
                TuiInputAction::None
            }
            KeyCode::Char('m') if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.start_file_prompt(FileManagerPrompt::NewDirectory);
                TuiInputAction::None
            }
            KeyCode::Char('r') if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.start_file_prompt(FileManagerPrompt::Rename);
                TuiInputAction::None
            }
            KeyCode::Char('x') if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.start_file_prompt(FileManagerPrompt::Delete);
                TuiInputAction::None
            }
            _ => TuiInputAction::None,
        }
    }

    fn toggle_file_browser(&mut self) -> TuiInputAction {
        if self.view_mode == TuiViewMode::Files {
            self.close_file_browser();
            TuiInputAction::None
        } else {
            TuiInputAction::OpenFiles(default_tui_files_target())
        }
    }

    fn update_runtime_info(&mut self, runtime_info: TuiAgentRuntimeInfo) {
        if !self.tool_runtime_explicit_override {
            self.tool_runtime_node_id = runtime_info.tool_runtime.sandbox_node_id.clone();
        }
        self.runtime_info = runtime_info;
    }

    fn toggle_chat_selection_mode(&mut self) {
        if self.chat_selection.is_some() {
            self.clear_chat_selection();
            return;
        }
        let Some(last_line) = tui_message_line_count(&self.messages).checked_sub(1) else {
            return;
        };
        self.chat_selection = Some(TuiChatSelection::new(last_line, false));
        self.completions = None;
        self.update_chat_selection_status();
    }

    fn clear_chat_selection(&mut self) {
        self.chat_selection = None;
        if self.view_mode == TuiViewMode::Chat && !self.exit_requested {
            self.status = "listening".to_string();
        }
    }

    fn finish_mouse_chat_selection(&mut self) {
        if let Some(selection) = self.chat_selection.as_mut() {
            selection.dragging = false;
            self.update_chat_selection_status();
        }
    }

    fn move_chat_selection_focus(&mut self, delta: isize) {
        let Some(selection) = self.chat_selection else {
            return;
        };
        let line_count = tui_message_line_count(&self.messages);
        if line_count == 0 {
            self.clear_chat_selection();
            return;
        }
        let focus = if delta.is_negative() {
            selection.focus.saturating_sub(delta.unsigned_abs())
        } else {
            selection
                .focus
                .saturating_add(delta as usize)
                .min(line_count - 1)
        };
        self.set_chat_selection_focus(focus);
    }

    fn set_chat_selection_focus(&mut self, focus: usize) {
        let line_count = tui_message_line_count(&self.messages);
        if line_count == 0 {
            self.clear_chat_selection();
            return;
        }
        if let Some(selection) = self.chat_selection.as_mut() {
            selection.focus = focus.min(line_count - 1);
            selection.dragging = false;
            self.update_chat_selection_status();
        }
    }

    fn select_all_chat_lines(&mut self) {
        let Some(last_line) = tui_message_line_count(&self.messages).checked_sub(1) else {
            return;
        };
        self.chat_selection = Some(TuiChatSelection {
            anchor: 0,
            focus: last_line,
            dragging: false,
        });
        self.completions = None;
        self.update_chat_selection_status();
    }

    fn update_chat_selection_status(&mut self) {
        let Some(selection) = self.chat_selection else {
            return;
        };
        let label = if selection.line_count() == 1 {
            "line"
        } else {
            "lines"
        };
        self.status = format!("selection: {} {label}", selection.line_count());
    }

    fn open_chat_tab(&mut self) {
        match self.view_mode {
            TuiViewMode::Chat => {}
            TuiViewMode::Files => self.close_file_browser(),
            TuiViewMode::Sessions => self.close_session_list(),
            TuiViewMode::Settings => self.close_settings(),
        }
    }

    fn open_file_browser_at(&mut self, target: PathBuf) {
        match FileManagerState::open_local(target).map(with_tui_file_previewers) {
            Ok(browser) => {
                self.install_file_browser(browser);
            }
            Err(error) => {
                self.status = format!("file browser error: {error}");
            }
        }
    }

    fn open_sifs_file_browser_at(&mut self, data_dir: &SinomoDataDir, target: &str) {
        match SifsFileManagerBackend::open(data_dir)
            .and_then(|backend| FileManagerState::open(Arc::new(backend), target))
            .map(with_tui_file_previewers)
        {
            Ok(browser) => self.install_file_browser(browser),
            Err(error) => {
                self.status = format!("file browser error: {error}");
            }
        }
    }

    fn open_files_target(&mut self, data_dir: &SinomoDataDir, target: TuiFilesTarget) {
        match target {
            TuiFilesTarget::Local(path) => self.open_file_browser_at(path),
            TuiFilesTarget::Sifs(path) => self.open_sifs_file_browser_at(data_dir, &path),
        }
    }

    fn install_file_browser(&mut self, browser: FileManagerState) {
        let current_path = browser.current_path().to_string();
        let source = browser.source_label().to_string();
        self.view_mode = TuiViewMode::Files;
        self.chat_selection = None;
        self.file_browser = Some(browser);
        self.file_prompt = None;
        self.session_list = None;
        self.settings = None;
        self.input.clear();
        self.input_cursor = 0;
        self.completions = None;
        self.status = format!("browsing {source} {current_path}");
    }

    fn close_file_browser(&mut self) {
        self.view_mode = TuiViewMode::Chat;
        self.file_prompt = None;
        self.input.clear();
        self.input_cursor = 0;
        self.completions = None;
        self.status = "listening".to_string();
    }

    fn open_session_list(&mut self, sessions: Vec<TuiSessionRecord>) {
        let count = sessions.len();
        self.view_mode = TuiViewMode::Sessions;
        self.file_prompt = None;
        self.input.clear();
        self.input_cursor = 0;
        self.completions = None;
        self.session_list = Some(TuiSessionListState::new(sessions, &self.session_id));
        self.settings = None;
        self.chat_selection = None;
        self.status = format!("showing {count} session(s)");
    }

    fn close_session_list(&mut self) {
        self.view_mode = TuiViewMode::Chat;
        self.session_list = None;
        self.input.clear();
        self.input_cursor = 0;
        self.completions = None;
        self.status = "listening".to_string();
    }

    fn open_settings(&mut self, settings: TuiSettingsState) {
        let count = settings.entries.len();
        self.view_mode = TuiViewMode::Settings;
        self.file_browser = None;
        self.file_prompt = None;
        self.session_list = None;
        self.settings = Some(settings);
        self.input.clear();
        self.input_cursor = 0;
        self.completions = None;
        self.chat_selection = None;
        self.status = format!("showing {count} setting(s)");
    }

    fn close_settings(&mut self) {
        self.view_mode = TuiViewMode::Chat;
        self.settings = None;
        self.input.clear();
        self.input_cursor = 0;
        self.completions = None;
        self.status = "listening".to_string();
    }

    fn start_setting_edit(&mut self) {
        let Some(settings) = self.settings.as_mut() else {
            return;
        };
        let Some(entry) = settings.selected_entry() else {
            self.status = "no setting selected".to_string();
            return;
        };
        if tui_config_key_is_sensitive(entry.key()) {
            self.status = "sensitive settings are not editable here".to_string();
            return;
        }
        let key = entry.key().to_string();
        let Some(value) = entry
            .value
            .clone()
            .or_else(|| entry.definition.default_value.clone())
        else {
            self.status = "setting has no current value".to_string();
            return;
        };
        settings.editing_key = Some(key.clone());
        self.input = compact_tui_json(&value);
        self.input_cursor = self.input.len();
        self.completions = None;
        self.status = format!("editing {key}");
    }

    fn cancel_setting_edit(&mut self) {
        if let Some(settings) = self.settings.as_mut() {
            settings.editing_key = None;
        }
        self.input.clear();
        self.input_cursor = 0;
        self.status = "settings edit cancelled".to_string();
    }

    fn replace_session(
        &mut self,
        session: &InteractionSession,
        workspaces: Vec<WorkspaceRef>,
        workspace_projection_version: u32,
        runtime_info: TuiAgentRuntimeInfo,
        mut messages: Vec<TuiMessage>,
        last_model_usage: Option<TuiTokenUsage>,
        session_token_usage: TuiTokenUsage,
    ) {
        if messages.is_empty() {
            messages.push(TuiMessage {
                kind: TuiMessageKind::System,
                text: "main agent session resumed".to_string(),
            });
        }
        self.session_id = session.session_id.to_string();
        self.workspaces = workspaces;
        self.workspace_projection_version = workspace_projection_version;
        self.runtime_info = runtime_info;
        self.runtime_activity = TuiRuntimeActivity::default();
        self.last_model_usage = last_model_usage;
        self.session_token_usage = session_token_usage;
        self.view_mode = TuiViewMode::Chat;
        self.file_browser = None;
        self.file_prompt = None;
        self.session_list = None;
        self.settings = None;
        self.input.clear();
        self.input_cursor = 0;
        self.completions = None;
        self.chat_selection = None;
        self.messages = messages;
        self.stream_message = None;
        self.scrollback = 0;
        self.exit_requested = false;
        self.status = "session resumed".to_string();
    }

    fn with_file_browser(
        &mut self,
        action: impl FnOnce(&mut FileManagerState) -> Result<String, String>,
    ) {
        let Some(browser) = self.file_browser.as_mut() else {
            self.status = "file browser is not open".to_string();
            return;
        };
        self.status = match action(browser) {
            Ok(message) => message,
            Err(error) => format!("file browser error: {error}"),
        };
    }

    fn start_file_prompt(&mut self, prompt: FileManagerPrompt) {
        if matches!(
            prompt,
            FileManagerPrompt::Rename | FileManagerPrompt::Delete
        ) {
            let Some(entry) = self
                .file_browser
                .as_ref()
                .and_then(FileManagerState::selected_entry)
            else {
                self.status = "no file selected".to_string();
                return;
            };
            if entry.kind == FileManagerEntryKind::Parent {
                self.status = "select a file or directory first".to_string();
                return;
            }
        }
        self.file_prompt = Some(prompt);
        self.input.clear();
        self.input_cursor = 0;
        self.completions = None;
        self.status = match self.file_prompt {
            Some(FileManagerPrompt::NewFile) => "enter new file name".to_string(),
            Some(FileManagerPrompt::NewDirectory) => "enter new directory name".to_string(),
            Some(FileManagerPrompt::Rename) => "enter new name".to_string(),
            Some(FileManagerPrompt::Delete) => "type DELETE to remove selected item".to_string(),
            None => self.status.clone(),
        };
    }

    fn clear_file_prompt(&mut self) {
        self.file_prompt = None;
        self.input.clear();
        self.input_cursor = 0;
        self.status = "browsing files".to_string();
    }

    fn submit_file_prompt(&mut self) {
        let Some(prompt) = self.file_prompt.take() else {
            return;
        };
        let value = std::mem::take(&mut self.input);
        self.input_cursor = 0;
        self.with_file_browser(|browser| match prompt {
            FileManagerPrompt::NewFile => browser.create_file(&value),
            FileManagerPrompt::NewDirectory => browser.create_dir(&value),
            FileManagerPrompt::Rename => browser.rename_selected(&value),
            FileManagerPrompt::Delete => browser.delete_selected(&value),
        });
    }

    fn input_title(&self) -> &'static str {
        if self.view_mode == TuiViewMode::Files {
            return self
                .file_prompt
                .as_ref()
                .map(FileManagerPrompt::title)
                .unwrap_or("Files");
        }
        if self.view_mode == TuiViewMode::Sessions {
            return "Sessions";
        }
        if self.view_mode == TuiViewMode::Settings {
            return "Settings";
        }
        "Message"
    }

    fn accepts_text_input(&self) -> bool {
        self.view_mode == TuiViewMode::Chat
            || self.file_prompt.is_some()
            || self
                .settings
                .as_ref()
                .is_some_and(|settings| settings.editing_key.is_some())
    }

    fn request_exit(&mut self) {
        self.exit_requested = true;
        self.status = "ending session".to_string();
    }

    fn take_submission(&mut self) -> TuiInputAction {
        let line = std::mem::take(&mut self.input);
        self.input_cursor = 0;
        self.completions = None;
        if line.trim().is_empty() {
            return TuiInputAction::None;
        }
        TuiInputAction::Submit(line)
    }

    fn begin_submission(&mut self, line: &str) {
        self.chat_selection = None;
        let kind = if line.trim_start().starts_with('/') {
            TuiMessageKind::Command
        } else {
            TuiMessageKind::User
        };
        self.push_message(kind, line);
        self.stream_message = None;
        self.scrollback = 0;
        self.status = if line.trim_start().starts_with('/') {
            "running command".to_string()
        } else {
            "working".to_string()
        };
    }

    fn finish_submission(&mut self) {
        self.stream_message = None;
        if !self.exit_requested {
            self.status = "listening".to_string();
        }
    }

    fn is_empty_session(&self) -> bool {
        tui_session_messages_are_empty(&self.messages)
    }

    fn apply_output(&mut self, output: InteractionOutput) {
        match output {
            InteractionOutput::Chunk(chunk) => self.append_chunk(&chunk),
            InteractionOutput::Line(line) => self.append_line(&line),
            InteractionOutput::ModelUsage { usage, .. } => self.apply_model_usage(&usage),
            InteractionOutput::Exit => self.request_exit(),
        }
    }

    fn mark_agent_interrupt_requested(&mut self) {
        self.runtime_activity.active = true;
        self.status = "interrupt requested".to_string();
        self.stream_message = None;
        self.push_message(TuiMessageKind::System, "agent interrupt requested");
    }

    fn apply_runtime_event(&mut self, event: AgentRuntimeEvent) -> bool {
        match event.payload {
            AgentRuntimeEventPayload::WorkerStarted { action, .. } => {
                self.runtime_activity.active = true;
                self.status = "working".to_string();
                self.stream_message = None;
                self.push_message(
                    TuiMessageKind::System,
                    format!("agent task started: {action}"),
                );
                true
            }
            AgentRuntimeEventPayload::WorkerCompleted {
                action,
                stop_reason,
                ..
            } => {
                self.runtime_activity = TuiRuntimeActivity::default();
                self.stream_message = None;
                self.status = "listening".to_string();
                self.push_message(
                    TuiMessageKind::System,
                    format!("agent task completed: {action} ({stop_reason:?})"),
                );
                true
            }
            AgentRuntimeEventPayload::WorkerFailed { action, detail, .. } => {
                self.runtime_activity = TuiRuntimeActivity::default();
                self.stream_message = None;
                self.status = "agent task failed".to_string();
                self.push_message(
                    TuiMessageKind::Error,
                    format!("agent task failed: {action}: {detail}"),
                );
                true
            }
            AgentRuntimeEventPayload::TurnFinalDecision {
                decision,
                stop_reason,
                ..
            } => {
                self.apply_turn_final_decision(decision, stop_reason);
                true
            }
            AgentRuntimeEventPayload::ModelProgress { event, .. } => {
                self.apply_model_progress_event(event);
                true
            }
            AgentRuntimeEventPayload::ToolProgress { event, .. } => {
                self.apply_tool_progress_event(event);
                true
            }
            AgentRuntimeEventPayload::ControlEvent { event } => {
                if matches!(
                    event.payload,
                    AgentEventPayload::CancellationRequested { .. }
                ) {
                    self.status = "interrupt requested".to_string();
                    return true;
                }
                false
            }
        }
    }

    fn apply_turn_final_decision(
        &mut self,
        decision: AgentLoopDecision,
        stop_reason: sinomo_agent::AgentStopReason,
    ) {
        self.suppress_structured_model_stream = false;
        let text = decision.text.trim();
        if !text.is_empty() {
            let mut reconciled_stream = false;
            if let Some(index) = self.stream_message.take()
                && let Some(message) = self.messages.get_mut(index)
                && message.kind == TuiMessageKind::Agent
            {
                let streamed = message.text.trim();
                if streamed == text {
                    reconciled_stream = true;
                } else if text.starts_with(streamed) {
                    message.text = text.to_string();
                    reconciled_stream = true;
                }
            }
            if !reconciled_stream {
                let kind = if stop_reason == sinomo_agent::AgentStopReason::Cancelled {
                    TuiMessageKind::System
                } else {
                    TuiMessageKind::Agent
                };
                self.push_message(kind, text);
            }
        }
        if let Some(handoff) = decision.background_handoff.as_ref() {
            self.push_message(TuiMessageKind::System, background_handoff_notice(handoff));
        }
    }

    fn apply_model_progress_event(&mut self, event: AgentModelEvent) {
        match event {
            AgentModelEvent::Started {
                step_index,
                provider,
                model,
            } => {
                self.runtime_activity.active = true;
                self.status = "streaming".to_string();
                self.stream_message = None;
                self.suppress_structured_model_stream = false;
                self.push_message(
                    TuiMessageKind::System,
                    format!("model started: {provider}/{model} step {step_index}"),
                );
            }
            AgentModelEvent::ContextAssembled { report, .. } => {
                self.runtime_activity.active = true;
                self.status = format!(
                    "context ready: {} bytes",
                    report.budget_estimate.total_bytes
                );
            }
            AgentModelEvent::AssistantTextDelta { delta, .. } => {
                self.runtime_activity.active = true;
                if self.suppress_structured_model_stream
                    || starts_structured_agent_json_delta(&delta)
                {
                    self.suppress_structured_model_stream = true;
                    self.status = "streaming".to_string();
                    return;
                }
                self.append_streamed_message(TuiMessageKind::Agent, &delta, "streaming");
            }
            AgentModelEvent::AssistantReasoningDelta { delta, .. } => {
                self.runtime_activity.active = true;
                self.append_streamed_message(TuiMessageKind::Thinking, &delta, "streaming");
            }
            AgentModelEvent::AssistantReasoningSummary { summary, .. } => {
                self.runtime_activity.active = true;
                self.append_streamed_message(TuiMessageKind::Thinking, &summary, "streaming");
            }
            AgentModelEvent::ToolCallStarted { tool, .. } => {
                self.stream_message = None;
                self.push_message(
                    TuiMessageKind::System,
                    format!("model requested tool: {tool}"),
                );
            }
            AgentModelEvent::ToolCallDelta { .. } => {}
            AgentModelEvent::ToolCallCompleted { call, .. } => {
                self.stream_message = None;
                self.push_message(
                    TuiMessageKind::System,
                    format!("model completed tool call: {}", call.tool),
                );
            }
            AgentModelEvent::Usage {
                usage: Some(usage), ..
            } => {
                self.apply_model_usage(&usage);
            }
            AgentModelEvent::Usage { usage: None, .. } => {}
            AgentModelEvent::Finished { .. } => {
                self.status = "working".to_string();
                self.stream_message = None;
                self.suppress_structured_model_stream = false;
            }
            AgentModelEvent::Error { detail, .. } => {
                self.stream_message = None;
                self.suppress_structured_model_stream = false;
                self.status = "model failed".to_string();
                self.push_message(TuiMessageKind::Error, format!("model failed: {detail}"));
            }
            AgentModelEvent::Aborted { reason } => {
                self.stream_message = None;
                self.suppress_structured_model_stream = false;
                self.status = "interrupt requested".to_string();
                self.push_message(TuiMessageKind::System, format!("model aborted: {reason}"));
            }
        }
    }

    fn apply_tool_progress_event(&mut self, event: AgentToolProgressEvent) {
        match event {
            AgentToolProgressEvent::ProcessStarted { process } => {
                self.runtime_activity.active = true;
                self.runtime_activity.active_command = Some(process.command.clone());
                self.stream_message = None;
                self.status = "running command".to_string();
                self.push_message(
                    TuiMessageKind::System,
                    format!("command started: {}", process.command),
                );
            }
            AgentToolProgressEvent::ProcessOutput { process } => {
                self.runtime_activity.active = true;
                self.status = "running command".to_string();
                self.append_runtime_process_output("stdout", &process.stdout);
                self.append_runtime_process_output("stderr", &process.stderr);
            }
            AgentToolProgressEvent::ProcessCompleted { process } => {
                self.runtime_activity.active = false;
                self.runtime_activity.active_command = None;
                self.stream_message = None;
                self.status = if process.cancel_requested {
                    "command cancelled".to_string()
                } else if process.timed_out {
                    "command timed out".to_string()
                } else {
                    "working".to_string()
                };
                self.push_message(
                    TuiMessageKind::System,
                    format!(
                        "command finished: {} ({}, exit {:?})",
                        process.command,
                        process.status.as_str(),
                        process.exit_code
                    ),
                );
            }
            AgentToolProgressEvent::Message { summary, .. } => {
                self.push_message(TuiMessageKind::System, summary);
            }
        }
    }

    fn append_runtime_process_output(&mut self, stream: &str, text: &str) {
        if text.is_empty() {
            return;
        }
        let mut formatted = String::new();
        for line in text.lines() {
            formatted.push_str(stream);
            formatted.push_str("> ");
            formatted.push_str(line);
            formatted.push('\n');
        }
        if formatted.is_empty() {
            formatted.push_str(stream);
            formatted.push_str("> ");
            formatted.push_str(text);
        }
        self.append_streamed_message(TuiMessageKind::Command, &formatted, "running command");
    }

    fn apply_model_usage(&mut self, usage: &sinomo_lm::LanguageModelUsage) {
        let Some(usage) = TuiTokenUsage::from_language_model_usage(usage) else {
            return;
        };
        self.last_model_usage = Some(usage);
        self.session_token_usage.add(usage);
    }

    fn append_chunk(&mut self, chunk: &str) {
        if chunk.is_empty() {
            return;
        }
        let (kind, text) = if let Some(text) = chunk.strip_prefix("thinking> ") {
            (TuiMessageKind::Thinking, text)
        } else if let Some(text) = chunk.strip_prefix("model> ") {
            (TuiMessageKind::System, text)
        } else {
            (TuiMessageKind::Thinking, chunk)
        };
        self.append_streamed_message(kind, text, "streaming");
    }

    fn append_streamed_message(&mut self, kind: TuiMessageKind, text: &str, status: &str) {
        let index = match self.stream_message {
            Some(index)
                if self
                    .messages
                    .get(index)
                    .is_some_and(|message| message.kind == kind) =>
            {
                index
            }
            _ => {
                self.messages.push(TuiMessage {
                    kind,
                    text: String::new(),
                });
                self.messages.len() - 1
            }
        };
        self.messages[index].text.push_str(text);
        self.stream_message = Some(index);
        self.status = status.to_string();
        self.scrollback = 0;
    }

    fn append_line(&mut self, line: &str) {
        self.stream_message = None;
        if let Some(text) = line.strip_prefix("sinomo: ") {
            self.push_message(TuiMessageKind::Agent, text);
        } else if line.starts_with("command failed:") || line.starts_with("model failed:") {
            self.push_message(TuiMessageKind::Error, line);
        } else {
            self.push_message(TuiMessageKind::System, line);
        }
        self.status = if line == "ending session" {
            "ending session".to_string()
        } else {
            "listening".to_string()
        };
    }

    fn push_message(&mut self, kind: TuiMessageKind, text: impl Into<String>) {
        self.messages.push(TuiMessage {
            kind,
            text: text.into(),
        });
        self.scrollback = 0;
    }

    fn insert_str(&mut self, text: &str) {
        if (self.view_mode == TuiViewMode::Files && self.file_prompt.is_none())
            || self.view_mode == TuiViewMode::Sessions
            || (self.view_mode == TuiViewMode::Settings
                && self
                    .settings
                    .as_ref()
                    .is_none_or(|settings| settings.editing_key.is_none()))
        {
            return;
        }
        for character in text.chars() {
            if character != '\n' && character != '\r' {
                self.insert_char(character);
            }
        }
    }

    fn insert_char(&mut self, character: char) {
        self.input.insert(self.input_cursor, character);
        self.input_cursor += character.len_utf8();
        self.refresh_completions();
    }

    fn backspace(&mut self) {
        if self.input_cursor == 0 {
            return;
        }
        if let Some((previous, _)) = self.input[..self.input_cursor].char_indices().next_back() {
            self.input.drain(previous..self.input_cursor);
            self.input_cursor = previous;
            self.refresh_completions();
        }
    }

    fn delete(&mut self) {
        if self.input_cursor >= self.input.len() {
            return;
        }
        let next = self.input[self.input_cursor..]
            .char_indices()
            .nth(1)
            .map(|(index, _)| self.input_cursor + index)
            .unwrap_or(self.input.len());
        self.input.drain(self.input_cursor..next);
        self.refresh_completions();
    }

    fn move_cursor_left(&mut self) {
        if self.input_cursor == 0 {
            return;
        }
        if let Some((previous, _)) = self.input[..self.input_cursor].char_indices().next_back() {
            self.input_cursor = previous;
            self.refresh_completions();
        }
    }

    fn move_cursor_right(&mut self) {
        if self.input_cursor >= self.input.len() {
            return;
        }
        self.input_cursor = self.input[self.input_cursor..]
            .char_indices()
            .nth(1)
            .map(|(index, _)| self.input_cursor + index)
            .unwrap_or(self.input.len());
        self.refresh_completions();
    }

    fn refresh_completions(&mut self) {
        if self.view_mode != TuiViewMode::Chat {
            self.completions = None;
            return;
        }
        self.completions = slash_completions_for_layer(
            &self.input,
            self.input_cursor,
            SlashCommandCompletionLayer::Tui,
        )
        .map(|set| TuiCompletionState {
            replace_start: set.replace_start,
            replace_end: set.replace_end,
            selected: None,
            items: set.items,
        });
    }

    fn select_next_completion(&mut self) {
        let Some(completions) = self.completions.as_mut() else {
            return;
        };
        if completions.items.is_empty() {
            return;
        }
        let selected = completions
            .selected
            .map(|index| (index + 1) % completions.items.len())
            .unwrap_or(0);
        completions.selected = Some(selected);
        self.apply_selected_completion_preview();
    }

    fn select_previous_completion(&mut self) {
        let Some(completions) = self.completions.as_mut() else {
            return;
        };
        if completions.items.is_empty() {
            return;
        }
        let selected = completions
            .selected
            .map(|index| {
                if index == 0 {
                    completions.items.len() - 1
                } else {
                    index - 1
                }
            })
            .unwrap_or_else(|| completions.items.len() - 1);
        completions.selected = Some(selected);
        self.apply_selected_completion_preview();
    }

    fn accept_completion(&mut self) -> bool {
        let Some(selected) = self.selected_fillable_completion() else {
            return false;
        };
        if let Some(completions) = self.completions.as_mut() {
            completions.selected = Some(selected);
        }
        if !self.apply_selected_completion_preview() {
            return false;
        }
        self.refresh_completions();
        true
    }

    fn selected_fillable_completion(&self) -> Option<usize> {
        let completions = self.completions.as_ref()?;
        if let Some(selected) = completions.selected.filter(|selected| {
            completions
                .items
                .get(*selected)
                .is_some_and(|item| item.insert.is_some())
        }) {
            return Some(selected);
        }
        completions
            .items
            .iter()
            .position(|item| item.insert.is_some())
    }

    fn apply_selected_completion_preview(&mut self) -> bool {
        let Some(completions) = self.completions.as_mut() else {
            return false;
        };
        let Some(selected) = completions.selected else {
            return false;
        };
        let Some(insert) = completions
            .items
            .get(selected)
            .and_then(|item| item.insert.as_ref())
            .cloned()
        else {
            return false;
        };
        if completions.replace_start > completions.replace_end
            || completions.replace_end > self.input.len()
        {
            return false;
        }
        self.input
            .replace_range(completions.replace_start..completions.replace_end, &insert);
        completions.replace_end = completions.replace_start + insert.len();
        self.input_cursor = completions.replace_end;
        true
    }

    fn step_slash_completion_back(&mut self) -> bool {
        if self.view_mode != TuiViewMode::Chat
            || !self.input.starts_with('/')
            || self.input_cursor != self.input.len()
        {
            return false;
        }
        let trimmed_end = self.input.trim_end_matches(char::is_whitespace).len();
        if trimmed_end <= 1 {
            return false;
        }
        let command = &self.input[..trimmed_end];
        let previous_level_end = command
            .char_indices()
            .rev()
            .find(|(_, character)| character.is_whitespace())
            .map(|(index, character)| index + character.len_utf8())
            .unwrap_or(1);
        self.input.truncate(previous_level_end.max(1));
        self.input_cursor = self.input.len();
        self.refresh_completions();
        true
    }

    fn input_view(&self, width: u16) -> (String, u16) {
        let width = usize::from(width.max(1));
        let cursor_chars = self.input[..self.input_cursor].chars().count();
        let chars = self.input.chars().collect::<Vec<_>>();
        let start = cursor_chars.saturating_sub(width.saturating_sub(1));
        let end = (start + width).min(chars.len());
        let text = chars[start..end].iter().collect::<String>();
        let cursor_column = cursor_chars
            .saturating_sub(start)
            .min(width.saturating_sub(1));
        (text, cursor_column as u16)
    }
}

fn initial_tui_conversation_workspaces() -> Vec<WorkspaceRef> {
    std::env::current_dir()
        .ok()
        .and_then(|root| fs::canonicalize(root).ok())
        .map(WorkspaceRef::local_path)
        .into_iter()
        .collect()
}

fn tui_file_previewers() -> Vec<Arc<dyn FileManagerPreviewer>> {
    vec![Arc::new(SobjFilePreviewer)]
}

fn with_tui_file_previewers(mut browser: FileManagerState) -> FileManagerState {
    browser.set_file_previewers(tui_file_previewers());
    browser
}

fn parse_tui_slash_command(line: &str) -> Result<Option<TuiSlashCommand>, CliError> {
    let Some(args) = parse_slash_command(line)? else {
        return Ok(None);
    };
    if args.iter().any(|arg| arg == "--local") {
        return Err(CliError::Parse {
            detail:
                "`--local` has been removed; start the main daemon and use cluster-backed commands"
                    .to_string(),
        });
    }
    match args.as_slice() {
        [command, rest @ ..] if command == "agent" => parse_tui_agent_command(rest),
        [command] if command == "files" => Ok(Some(TuiSlashCommand::Files {
            target: default_tui_files_target(),
        })),
        [command, flag] if command == "files" && flag == "--sifs" => {
            Ok(Some(TuiSlashCommand::Files {
                target: default_tui_files_target(),
            }))
        }
        [command, flag, path] if command == "files" && flag == "--sifs" => {
            Ok(Some(TuiSlashCommand::Files {
                target: TuiFilesTarget::Sifs(path.clone()),
            }))
        }
        [command, path] if command == "files" => Ok(Some(TuiSlashCommand::Files {
            target: parse_tui_files_target(path),
        })),
        [command, ..] if command == "files" => Err(CliError::Parse {
            detail: "usage: /files [path|sifs:/path|local:<path>|--sifs [path]]".to_string(),
        }),
        [command, rest @ ..] if command == "model" => parse_tui_model_command(rest),
        [command, rest @ ..] if command == "tool-runtime" => parse_tui_tool_runtime_command(rest),
        [command] if command == "settings" => Ok(Some(TuiSlashCommand::Settings)),
        [command, ..] if command == "settings" => Err(CliError::Parse {
            detail: "usage: /settings".to_string(),
        }),
        [command] if command == "sessions" => Ok(Some(TuiSlashCommand::Sessions)),
        [command, ..] if command == "sessions" => Err(CliError::Parse {
            detail: "usage: /sessions".to_string(),
        }),
        [command, session_id] if command == "resume" => {
            let session_id = SessionId::from_str(session_id).map_err(|source| CliError::Parse {
                detail: format!("invalid session id `{session_id}`: {source}"),
            })?;
            Ok(Some(TuiSlashCommand::Resume { session_id }))
        }
        [command, ..] if command == "resume" => Err(CliError::Parse {
            detail: "usage: /resume <session-id>".to_string(),
        }),
        _ => Ok(None),
    }
}

fn parse_tui_tool_runtime_command(args: &[String]) -> Result<Option<TuiSlashCommand>, CliError> {
    match args {
        [] => Ok(Some(TuiSlashCommand::ToolRuntime(
            TuiToolRuntimeCommand::Status,
        ))),
        [command] if command == "status" => Ok(Some(TuiSlashCommand::ToolRuntime(
            TuiToolRuntimeCommand::Status,
        ))),
        [command] if command == "local" => Ok(Some(TuiSlashCommand::ToolRuntime(
            TuiToolRuntimeCommand::Local,
        ))),
        [command, node_id] if command == "node" => {
            parse_tui_tool_runtime_node_id(node_id)?;
            Ok(Some(TuiSlashCommand::ToolRuntime(
                TuiToolRuntimeCommand::Node(node_id.clone()),
            )))
        }
        _ => Err(CliError::Parse {
            detail: "usage: /tool-runtime [status|local|node <node-id>]".to_string(),
        }),
    }
}

fn parse_tui_tool_runtime_node_id(value: &str) -> Result<NodeId, CliError> {
    NodeId::new(value.to_string())
        .or_else(|_| NodeId::from_legacy_named(value.to_string()))
        .map_err(|source| CliError::Parse {
            detail: format!("invalid sandbox runtime node id `{value}`: {source}"),
        })
}

fn parse_tui_tool_runtime_state_node_id(value: Option<&str>) -> Result<Option<NodeId>, CliError> {
    value.map(parse_tui_tool_runtime_node_id).transpose()
}

fn default_tui_files_target() -> TuiFilesTarget {
    TuiFilesTarget::Sifs("/".to_string())
}

fn parse_tui_files_target(value: &str) -> TuiFilesTarget {
    let lower = value.to_ascii_lowercase();
    if lower == "local" || lower == "local:" || lower == "file" || lower == "file:" {
        return TuiFilesTarget::Local(PathBuf::new());
    }
    if lower.starts_with("local:") || lower.starts_with("file:") {
        let path = value
            .split_once(':')
            .map(|(_, path)| path)
            .unwrap_or_default();
        return TuiFilesTarget::Local(PathBuf::from(path));
    }
    if lower == "sifs" || lower == "sifs:" {
        return TuiFilesTarget::Sifs("/".to_string());
    }
    if lower.starts_with("sifs:") {
        let path = value
            .split_once(':')
            .map(|(_, path)| path)
            .filter(|path| !path.is_empty())
            .unwrap_or("/");
        return TuiFilesTarget::Sifs(path.to_string());
    }
    TuiFilesTarget::Sifs(value.to_string())
}

fn apply_tui_slash_command<H: TuiAgentRuntimeControl>(
    runtime: &mut InteractionRuntime<JsonlEventStore, H>,
    state: &mut TuiState,
    data_dir: &SinomoDataDir,
    session_store: &TuiSessionStore,
    daemon: Option<&DaemonStatus>,
    command: TuiSlashCommand,
) -> Result<Vec<InteractionOutput>, CliError> {
    match command {
        TuiSlashCommand::Agent(TuiAgentCommand::Status) => {
            let info = runtime.handler.tui_agent_runtime_info();
            state.update_runtime_info(info.clone());
            Ok(tui_agent_status_outputs(&info))
        }
        TuiSlashCommand::Agent(TuiAgentCommand::List) => {
            let catalog = load_tui_agent_catalog(daemon)?;
            let mut outputs = Vec::new();
            for definition in catalog
                .list()
                .filter(|definition| definition.supports_interactive_chat())
            {
                outputs.push(InteractionOutput::Line(format!(
                    "{} [{}] - {}",
                    definition.key(),
                    tui_agent_role_label(&definition.role),
                    if definition.description.is_empty() {
                        definition.name.as_str()
                    } else {
                        definition.description.as_str()
                    }
                )));
            }
            if outputs.is_empty() {
                outputs.push(InteractionOutput::Line("no agents registered".to_string()));
            }
            Ok(outputs)
        }
        TuiSlashCommand::Agent(TuiAgentCommand::Use(agent_id)) => {
            let definition = load_tui_agent_definition(daemon, &agent_id)?;
            let session = runtime.session().clone();
            let info = runtime.handler.switch_tui_agent_for_session(
                data_dir,
                &session,
                state.workspace_projection(),
                state.daemon_connected,
                definition,
            )?;
            state.update_runtime_info(info.clone());
            Ok(vec![InteractionOutput::Line(format!(
                "agent switched: {}",
                info.agent
            ))])
        }
        TuiSlashCommand::Files { target } => {
            state.open_files_target(data_dir, target);
            Ok(vec![InteractionOutput::Line(
                "opened file browser".to_string(),
            )])
        }
        TuiSlashCommand::Model(TuiModelCommand::Status) => {
            let info = runtime.handler.tui_agent_runtime_info();
            state.update_runtime_info(info.clone());
            Ok(tui_model_status_outputs(&info))
        }
        TuiSlashCommand::Model(TuiModelCommand::Use(selection)) => {
            let info =
                runtime
                    .handler
                    .switch_tui_model(data_dir, selection, state.daemon_connected)?;
            state.update_runtime_info(info.clone());
            Ok(vec![InteractionOutput::Line(format!(
                "model switched: {} {}",
                info.agent,
                info.model_label()
            ))])
        }
        TuiSlashCommand::ToolRuntime(TuiToolRuntimeCommand::Status) => {
            Ok(vec![InteractionOutput::Line(format!(
                "sandbox runtime node: {}",
                state
                    .tool_runtime_node_id
                    .as_deref()
                    .unwrap_or("local turn node")
            ))])
        }
        TuiSlashCommand::ToolRuntime(TuiToolRuntimeCommand::Local) => {
            state.tool_runtime_node_id = None;
            state.tool_runtime_explicit_override = true;
            let session = runtime.session().clone();
            runtime.handler.configure_tui_tool_runtime(
                data_dir,
                &session,
                None,
                state.workspace_projection(),
                state.daemon_connected,
            )?;
            Ok(vec![InteractionOutput::Line(
                "sandbox runtime node: local turn node".to_string(),
            )])
        }
        TuiSlashCommand::ToolRuntime(TuiToolRuntimeCommand::Node(node_id)) => {
            let parsed_node_id = parse_tui_tool_runtime_node_id(&node_id)?;
            let session = runtime.session().clone();
            runtime.handler.configure_tui_tool_runtime(
                data_dir,
                &session,
                Some(parsed_node_id),
                state.workspace_projection(),
                state.daemon_connected,
            )?;
            state.tool_runtime_node_id = Some(node_id.clone());
            state.tool_runtime_explicit_override = true;
            Ok(vec![InteractionOutput::Line(format!(
                "sandbox runtime node: {node_id}"
            ))])
        }
        TuiSlashCommand::Sessions => {
            let count = open_tui_session_list_view(runtime, state, session_store)?;
            Ok(vec![InteractionOutput::Line(format!(
                "loaded {count} session(s)"
            ))])
        }
        TuiSlashCommand::Settings => {
            let count = open_tui_settings_view(state, data_dir)?;
            Ok(vec![InteractionOutput::Line(format!(
                "loaded {count} setting(s)"
            ))])
        }
        TuiSlashCommand::Resume { session_id } => {
            resume_tui_session(runtime, state, session_store, data_dir, daemon, session_id)
        }
    }
}

pub(crate) fn tui_agent_catalog_vfs(
    daemon: Option<&DaemonStatus>,
) -> Result<sinomo_runtime::vfs::VfsHandle, CliError> {
    let daemon = daemon.ok_or_else(|| CliError::DaemonNotRunning {
        detail: "TUI agent catalog reads require the resident main daemon".to_string(),
    })?;
    sinomo_runtime::vfs::daemon_sifs(daemon.address.clone()).map_err(|source| CliError::Io {
        path: "/".to_string(),
        detail: source.to_string(),
    })
}

fn load_tui_agent_catalog(
    daemon: Option<&DaemonStatus>,
) -> Result<sinomo_agent::AgentCatalog, CliError> {
    load_tui_agent_catalog_from_vfs(tui_agent_catalog_vfs(daemon)?)
}

pub(crate) fn load_tui_agent_catalog_from_vfs(
    vfs: sinomo_runtime::vfs::VfsHandle,
) -> Result<sinomo_agent::AgentCatalog, CliError> {
    sinomo_agent::AgentCatalog::load_from_vfs(vfs).map_err(|source| CliError::Parse {
        detail: source.to_string(),
    })
}

fn load_tui_agent_definition(
    daemon: Option<&DaemonStatus>,
    agent_key: &str,
) -> Result<sinomo_agent::AgentDefinition, CliError> {
    crate::app_api::resolve_agent_definition_with_catalog_vfs(agent_key, || {
        tui_agent_catalog_vfs(daemon)
    })
    .map_err(|detail| CliError::Parse { detail })?
    .ok_or_else(|| CliError::Parse {
        detail: format!("unknown agent `{agent_key}`"),
    })
}

fn open_tui_session_list_view<H: TuiAgentRuntimeControl>(
    runtime: &InteractionRuntime<JsonlEventStore, H>,
    state: &mut TuiState,
    session_store: &TuiSessionStore,
) -> Result<usize, CliError> {
    session_store.save_state(runtime.session(), state)?;
    let sessions = session_store.list_sessions()?;
    let count = sessions.len();
    state.open_session_list(sessions);
    Ok(count)
}

fn open_tui_settings_view(
    state: &mut TuiState,
    data_dir: &SinomoDataDir,
) -> Result<usize, CliError> {
    let settings = TuiSettingsState::load(data_dir)?;
    let count = settings.entries.len();
    state.open_settings(settings);
    Ok(count)
}

fn save_tui_setting(
    state: &mut TuiState,
    data_dir: &SinomoDataDir,
    key: &str,
    value: &str,
) -> Result<(), CliError> {
    let parsed: serde_json::Value =
        serde_json::from_str(value).map_err(|source| CliError::Parse {
            detail: format!("invalid JSON for setting `{key}`: {source}"),
        })?;
    let store = main_agent_config_store()?;
    store
        .set(
            data_dir,
            None,
            key,
            parsed,
            sinomo_runtime::config::ConfigWriteTarget::Auto,
        )
        .map_err(|source| CliError::Parse {
            detail: source.to_string(),
        })?;
    let selected_key = key.to_string();
    let mut settings = TuiSettingsState::load(data_dir)?;
    if let Some(index) = settings
        .entries
        .iter()
        .position(|entry| entry.key() == selected_key)
    {
        settings.selected = index;
    }
    state.open_settings(settings);
    state.status = format!("saved setting {key}");
    Ok(())
}

fn resume_tui_session<H: TuiAgentRuntimeControl>(
    runtime: &mut InteractionRuntime<JsonlEventStore, H>,
    state: &mut TuiState,
    session_store: &TuiSessionStore,
    data_dir: &SinomoDataDir,
    daemon: Option<&DaemonStatus>,
    session_id: SessionId,
) -> Result<Vec<InteractionOutput>, CliError> {
    if runtime.session().session_id == session_id {
        state.close_session_list();
        return Ok(vec![InteractionOutput::Line(format!(
            "session already active: {session_id}"
        ))]);
    }

    let previous_store = runtime.store().clone();
    let previous_session = runtime.session().clone();
    let previous_state = state.clone();
    let handler_snapshot = runtime.handler.snapshot_tui_resume(&previous_session)?;
    match resume_tui_session_inner(runtime, state, session_store, data_dir, daemon, session_id) {
        Ok(outputs) => Ok(outputs),
        Err(error) => {
            let rollback = runtime
                .handler
                .rollback_tui_resume(data_dir, handler_snapshot);
            runtime.replace_store_and_session(previous_store, previous_session);
            *state = previous_state;
            match rollback {
                Ok(()) => Err(error),
                Err(rollback) => Err(CliError::Parse {
                    detail: format!(
                        "resume TUI session failed: {error}; rollback failed: {rollback}"
                    ),
                }),
            }
        }
    }
}

fn resume_tui_session_inner<H: TuiAgentRuntimeControl>(
    runtime: &mut InteractionRuntime<JsonlEventStore, H>,
    state: &mut TuiState,
    session_store: &TuiSessionStore,
    data_dir: &SinomoDataDir,
    daemon: Option<&DaemonStatus>,
    session_id: SessionId,
) -> Result<Vec<InteractionOutput>, CliError> {
    if runtime.session().session_id == session_id {
        state.close_session_list();
        return Ok(vec![InteractionOutput::Line(format!(
            "session already active: {session_id}"
        ))]);
    }

    session_store.save_state(runtime.session(), state)?;
    let current_session = runtime.session().clone();
    runtime
        .handler
        .persist_tui_agent_session(data_dir, &current_session)?;
    let record = session_store.load_session(&session_id)?;
    let mut persisted_agent_session = runtime.handler.load_tui_agent_session(&session_id)?;
    let mut conversation_workspaces = record.workspaces.clone();
    let workspace_projection_is_authoritative =
        record.workspace_projection_version > 0 || !conversation_workspaces.is_empty();
    if !workspace_projection_is_authoritative
        && let Some(legacy_workspace) = persisted_agent_session
            .as_ref()
            .and_then(|session| session.workspace_binding.clone())
    {
        conversation_workspaces.push(legacy_workspace);
    }
    let mut outputs = Vec::new();
    let resume_agent_key = match persisted_agent_session
        .as_ref()
        .and_then(|session| session.agent_definition_key.as_deref())
        .filter(|key| !key.trim().is_empty())
    {
        Some(key) => key.to_string(),
        None => {
            let source = if persisted_agent_session.is_some() {
                "canonical session has no agent definition"
            } else {
                "canonical session is unavailable"
            };
            outputs.push(InteractionOutput::Line(format!(
                "legacy session restore: {source}; using TUI agent metadata `{}`",
                record.runtime_info.agent
            )));
            record.runtime_info.agent.clone()
        }
    };
    let restored_info = restore_tui_session_runtime_info(
        &mut runtime.handler,
        data_dir,
        daemon,
        state.daemon_connected,
        &record.runtime_info,
        &resume_agent_key,
        &mut outputs,
    )?;
    runtime.replace_store_and_session(
        JsonlEventStore::new(session_store.events_path_for_session(&session_id)),
        record.session.clone(),
    );
    runtime.append_session_context(daemon_context_event(&record.session, daemon))?;
    let transcript = session_store.load_transcript(&session_id)?;
    let (messages, last_model_usage, session_token_usage) = match transcript {
        Some(transcript) => (
            transcript.messages,
            transcript.last_model_usage,
            transcript.session_token_usage,
        ),
        None => (Vec::new(), None, TuiTokenUsage::default()),
    };
    if let Some(persisted) = persisted_agent_session.as_mut() {
        // The conversation record is authoritative. Keep the legacy session
        // field synchronized only so older readers see the same primary grant.
        persisted.workspace_binding = conversation_workspaces.first().cloned();
        if persisted
            .agent_definition_key
            .as_deref()
            .is_none_or(|key| key.trim().is_empty())
        {
            persisted.agent_definition_key = Some(resume_agent_key);
        }
    }
    runtime.handler.restore_tui_agent_session(
        data_dir,
        &record.session,
        persisted_agent_session,
        agent_transcript_from_tui_messages(&messages),
    )?;
    state.replace_session(
        &record.session,
        conversation_workspaces,
        TUI_WORKSPACE_PROJECTION_VERSION,
        restored_info,
        messages,
        last_model_usage,
        session_token_usage,
    );
    outputs.insert(
        0,
        InteractionOutput::Line(format!("resumed session: {}", record.session.session_id)),
    );
    Ok(outputs)
}

fn agent_transcript_from_tui_messages(messages: &[TuiMessage]) -> sinomo_agent::AgentTranscript {
    let mut transcript = sinomo_agent::AgentTranscript::new();
    for (step_index, message) in messages.iter().enumerate() {
        if message.text.trim().is_empty() {
            continue;
        }
        match message.kind {
            TuiMessageKind::User | TuiMessageKind::Command => {
                transcript.push(sinomo_agent::AgentItem::UserMessage {
                    item_id: TaskId::generate(),
                    step_index,
                    content: message.text.clone(),
                    event_id: None,
                });
            }
            TuiMessageKind::Agent => transcript.push(sinomo_agent::AgentItem::AssistantText {
                item_id: TaskId::generate(),
                step_index,
                content: message.text.clone(),
            }),
            TuiMessageKind::Thinking | TuiMessageKind::System | TuiMessageKind::Error => {}
        }
    }
    transcript
}

fn restore_tui_session_runtime_info<H: TuiAgentRuntimeControl>(
    handler: &mut H,
    data_dir: &SinomoDataDir,
    daemon: Option<&DaemonStatus>,
    daemon_connected: bool,
    info: &TuiAgentRuntimeInfo,
    agent_key: &str,
    outputs: &mut Vec<InteractionOutput>,
) -> Result<TuiAgentRuntimeInfo, CliError> {
    let provider =
        normalize_tui_model_provider(&info.provider).unwrap_or_else(|_| info.provider.clone());
    let selection = TuiModelSelection {
        provider,
        model: info.model.clone(),
        account: info
            .account
            .clone()
            .unwrap_or_else(|| sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string()),
    };
    let _restored = match handler.switch_tui_model(data_dir, selection, daemon_connected) {
        Ok(info) => info,
        Err(error) => {
            outputs.push(InteractionOutput::Line(format!(
                "model restore failed: {error}"
            )));
            handler.tui_agent_runtime_info()
        }
    };
    let definition = load_tui_agent_definition(daemon, agent_key)?;
    let prepared = handler
        .prepare_tui_agent(data_dir, daemon_connected, definition)
        .map_err(|error| CliError::Parse {
            detail: format!("prepare agent `{agent_key}` for session restore failed: {error}"),
        })?;
    Ok(prepared)
}

fn tui_agent_status_outputs(info: &TuiAgentRuntimeInfo) -> Vec<InteractionOutput> {
    vec![
        InteractionOutput::Line(format!("agent: {}", info.agent)),
        InteractionOutput::Line(format!("model: {}", info.model_label())),
        InteractionOutput::Line(format!("reasoning: {}", info.reasoning_label())),
    ]
}

fn tui_model_status_outputs(info: &TuiAgentRuntimeInfo) -> Vec<InteractionOutput> {
    vec![
        InteractionOutput::Line(format!("agent: {}", info.agent)),
        InteractionOutput::Line(format!("provider: {}", info.provider)),
        InteractionOutput::Line(format!("model: {}", info.model)),
        InteractionOutput::Line(format!(
            "account: {}",
            info.account.as_deref().unwrap_or("default")
        )),
        InteractionOutput::Line(format!("reasoning: {}", info.reasoning_label())),
    ]
}

fn parse_tui_agent_command(args: &[String]) -> Result<Option<TuiSlashCommand>, CliError> {
    match args {
        [] => Ok(Some(TuiSlashCommand::Agent(TuiAgentCommand::Status))),
        [command] if command == "status" => {
            Ok(Some(TuiSlashCommand::Agent(TuiAgentCommand::Status)))
        }
        [command] if command == "list" => Ok(Some(TuiSlashCommand::Agent(TuiAgentCommand::List))),
        [command, agent_id] if command == "use" => Ok(Some(TuiSlashCommand::Agent(
            TuiAgentCommand::Use(agent_id.clone()),
        ))),
        [command] if command == "use" => Err(tui_agent_usage_error()),
        [command, ..] if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Err(tui_agent_usage_error())
        }
        [agent_id] => Ok(Some(TuiSlashCommand::Agent(TuiAgentCommand::Use(
            agent_id.clone(),
        )))),
        _ => Err(tui_agent_usage_error()),
    }
}

fn tui_agent_usage_error() -> CliError {
    CliError::Parse {
        detail: "usage: /agent [status|list|use <agent-id>]".to_string(),
    }
}

fn tui_agent_role_label(role: &sinomo_agent::AgentRole) -> &'static str {
    match role {
        sinomo_agent::AgentRole::Main => "main",
        sinomo_agent::AgentRole::Background => "background",
        sinomo_agent::AgentRole::Deliberation => "deliberation",
        sinomo_agent::AgentRole::Evolution => "evolution",
    }
}

fn parse_tui_model_command(args: &[String]) -> Result<Option<TuiSlashCommand>, CliError> {
    match args {
        [] => Ok(Some(TuiSlashCommand::Model(TuiModelCommand::Status))),
        [command] if command == "status" => {
            Ok(Some(TuiSlashCommand::Model(TuiModelCommand::Status)))
        }
        [command, ..] if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Err(tui_model_usage_error())
        }
        [command, rest @ ..] if command == "use" => Ok(Some(TuiSlashCommand::Model(
            TuiModelCommand::Use(parse_tui_model_selection(rest)?),
        ))),
        _ => Ok(Some(TuiSlashCommand::Model(TuiModelCommand::Use(
            parse_tui_model_selection(args)?,
        )))),
    }
}

fn parse_tui_model_selection(args: &[String]) -> Result<TuiModelSelection, CliError> {
    let mut provider = None;
    let mut model = None;
    let mut account = None;
    let mut named_provider_model = false;
    let mut positional_provider_model = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--provider" => {
                if positional_provider_model {
                    return Err(tui_model_mixed_form_error());
                }
                named_provider_model = true;
                provider = Some(tui_model_option_value(args, index, "--provider")?);
                index += 2;
            }
            "--model" => {
                if positional_provider_model {
                    return Err(tui_model_mixed_form_error());
                }
                named_provider_model = true;
                model = Some(tui_model_option_value(args, index, "--model")?);
                index += 2;
            }
            "--account" => {
                account = Some(tui_model_option_value(args, index, "--account")?);
                index += 2;
            }
            value if value.starts_with("--") => {
                return Err(CliError::Parse {
                    detail: format!("unknown /model option `{value}`"),
                });
            }
            value if provider.is_none() => {
                if named_provider_model {
                    return Err(tui_model_mixed_form_error());
                }
                positional_provider_model = true;
                provider = Some(value.to_string());
                index += 1;
            }
            value if model.is_none() => {
                if named_provider_model {
                    return Err(tui_model_mixed_form_error());
                }
                positional_provider_model = true;
                model = Some(value.to_string());
                index += 1;
            }
            value => {
                return Err(CliError::Parse {
                    detail: format!("unexpected /model argument `{value}`"),
                });
            }
        }
    }
    let provider = provider.ok_or_else(tui_model_usage_error)?;
    let provider = normalize_tui_model_provider(&provider)?;
    let model = model.ok_or_else(tui_model_usage_error)?;
    let model = if provider == MINICPM_PROVIDER_ID || provider == RUST_MINICPM_V46_PROVIDER_ID {
        model
    } else {
        normalize_model_id_for_provider(&provider, &model)
    };
    let account = normalize_provider_account_name(account.as_deref());
    Ok(TuiModelSelection {
        provider,
        model,
        account,
    })
}

fn tui_model_mixed_form_error() -> CliError {
    CliError::Parse {
        detail:
            "usage: do not mix positional /model <provider> <model> with --provider/--model options"
                .to_string(),
    }
}

fn tui_model_option_value(args: &[String], index: usize, option: &str) -> Result<String, CliError> {
    let Some(value) = args.get(index + 1).filter(|value| !value.starts_with("--")) else {
        return Err(CliError::Parse {
            detail: format!("{option} requires a value"),
        });
    };
    Ok(value.clone())
}

fn tui_model_usage_error() -> CliError {
    CliError::Parse {
        detail: "usage: /model <provider> <model> [--account <account>] or /model status"
            .to_string(),
    }
}

fn normalize_tui_model_provider(provider: &str) -> Result<String, CliError> {
    let provider = provider.trim();
    match provider {
        value
            if value.eq_ignore_ascii_case("minicpm")
                || value.eq_ignore_ascii_case(MINICPM_PROVIDER_ID) =>
        {
            Ok(MINICPM_PROVIDER_ID.to_string())
        }
        value if value.eq_ignore_ascii_case(RUST_MINICPM_V46_PROVIDER_ID) => {
            Ok(RUST_MINICPM_V46_PROVIDER_ID.to_string())
        }
        _ => normalize_model_provider(provider).ok_or_else(|| CliError::Parse {
            detail: format!("unknown provider `{provider}`"),
        }),
    }
}

fn tui_session_title(messages: &[TuiMessage]) -> String {
    messages
        .iter()
        .find(|message| message.kind == TuiMessageKind::User && !message.text.trim().is_empty())
        .or_else(|| {
            messages
                .iter()
                .find(|message| !message.text.trim().is_empty())
        })
        .map(|message| truncate_tui_text(message.text.trim(), 80))
        .unwrap_or_else(|| "New TUI session".to_string())
}

fn truncate_tui_text(value: &str, max_chars: usize) -> String {
    let mut output = String::new();
    for (index, character) in value.chars().enumerate() {
        if index >= max_chars {
            output.push_str("...");
            return output;
        }
        output.push(character);
    }
    output
}

fn compact_tui_json(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| value.to_string())
}

fn tui_config_key_is_sensitive(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    lower.contains("credential")
        || lower.contains("api_key")
        || lower.contains("apikey")
        || lower.contains("token")
        || lower.contains("secret")
        || lower.contains("password")
}

fn tui_session_messages_are_empty(messages: &[TuiMessage]) -> bool {
    !messages
        .iter()
        .any(|message| message.kind == TuiMessageKind::User && !message.text.trim().is_empty())
}

fn format_tui_token_count(tokens: u64) -> String {
    if tokens >= 1_000_000 {
        format!("{:.1}M", tokens as f64 / 1_000_000.0)
    } else if tokens >= 1_000 {
        format!("{:.1}k", tokens as f64 / 1_000.0)
    } else {
        tokens.to_string()
    }
}

fn format_tui_context_usage(usage: Option<TuiTokenUsage>) -> String {
    match usage {
        Some(usage) => format!(
            "{} in/{} out",
            format_tui_token_count(usage.input_tokens),
            format_tui_token_count(usage.output_tokens)
        ),
        None => "unknown".to_string(),
    }
}

fn format_tui_session_token_usage(usage: TuiTokenUsage) -> String {
    if usage.is_empty() {
        return "0 tok".to_string();
    }
    format!(
        "{} tok ({} in/{} out)",
        format_tui_token_count(usage.total_tokens()),
        format_tui_token_count(usage.input_tokens),
        format_tui_token_count(usage.output_tokens)
    )
}

fn format_tui_timestamp(value: OffsetDateTime) -> String {
    value.format(&Rfc3339).unwrap_or_else(|_| value.to_string())
}

fn write_tui_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), CliError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let bytes = serde_json::to_vec_pretty(value).map_err(|source| CliError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("session");
    let tmp_path = path.with_file_name(format!(".{file_name}.{}.tmp", SessionId::generate()));
    {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .map_err(|source| CliError::Io {
                path: tmp_path.display().to_string(),
                detail: source.to_string(),
            })?;
        file.write_all(&bytes).map_err(|source| CliError::Io {
            path: tmp_path.display().to_string(),
            detail: source.to_string(),
        })?;
        file.sync_all().map_err(|source| CliError::Io {
            path: tmp_path.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    replace_tui_file(&tmp_path, path)
}

#[cfg(not(windows))]
fn replace_tui_file(source: &Path, target: &Path) -> Result<(), CliError> {
    fs::rename(source, target).map_err(|source_error| CliError::Io {
        path: target.display().to_string(),
        detail: source_error.to_string(),
    })
}

#[cfg(windows)]
fn replace_tui_file(source: &Path, target: &Path) -> Result<(), CliError> {
    match fs::rename(source, target) {
        Ok(()) => Ok(()),
        Err(error) if target.exists() => {
            fs::remove_file(target).map_err(|remove_error| CliError::Io {
                path: target.display().to_string(),
                detail: remove_error.to_string(),
            })?;
            fs::rename(source, target).map_err(|rename_error| CliError::Io {
                path: target.display().to_string(),
                detail: format!(
                    "{error}; retry after removing existing file failed: {rename_error}"
                ),
            })
        }
        Err(error) => Err(CliError::Io {
            path: target.display().to_string(),
            detail: error.to_string(),
        }),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TuiLayoutAreas {
    menu: Rect,
    main: Rect,
    completions: Option<Rect>,
    input: Rect,
    status: Rect,
}

fn tui_layout_areas(area: Rect, state: &TuiState) -> TuiLayoutAreas {
    if matches!(
        state.view_mode,
        TuiViewMode::Files | TuiViewMode::Sessions | TuiViewMode::Settings
    ) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Min(1),
                Constraint::Length(3),
                Constraint::Length(1),
            ])
            .split(area);
        return TuiLayoutAreas {
            menu: chunks[0],
            main: chunks[1],
            completions: None,
            input: chunks[2],
            status: chunks[3],
        };
    }

    let completion_height = tui_completion_height(state);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(completion_height),
            Constraint::Length(3),
            Constraint::Length(1),
        ])
        .split(area);

    TuiLayoutAreas {
        menu: chunks[0],
        main: chunks[1],
        completions: (completion_height > 0).then_some(chunks[2]),
        input: chunks[3],
        status: chunks[4],
    }
}

fn tui_completion_height(state: &TuiState) -> u16 {
    state
        .completions
        .as_ref()
        .map(|completions| (completions.items.len() as u16 + 2).clamp(3, 8))
        .unwrap_or(0)
}

fn tui_file_browser_areas(area: Rect) -> (Rect, Rect) {
    tui_split_list_detail_areas(area)
}

fn tui_settings_areas(area: Rect) -> (Rect, Rect) {
    tui_split_list_detail_areas(area)
}

fn tui_split_list_detail_areas(area: Rect) -> (Rect, Rect) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(38), Constraint::Percentage(62)])
        .split(area);
    (chunks[0], chunks[1])
}

fn tui_inner_rect(area: Rect) -> Rect {
    Rect::new(
        area.x.saturating_add(1),
        area.y.saturating_add(1),
        area.width.saturating_sub(2),
        area.height.saturating_sub(2),
    )
}

fn rect_contains(area: Rect, column: u16, row: u16) -> bool {
    area.width > 0
        && area.height > 0
        && column >= area.x
        && column < area.x.saturating_add(area.width)
        && row >= area.y
        && row < area.y.saturating_add(area.height)
}

fn render_tui(frame: &mut Frame<'_>, state: &TuiState) {
    let areas = tui_layout_areas(frame.area(), state);
    render_tui_menu_bar(frame, areas.menu, state);
    match state.view_mode {
        TuiViewMode::Sessions => render_tui_session_list(frame, areas.main, state),
        TuiViewMode::Settings => render_tui_settings(frame, areas.main, state),
        TuiViewMode::Files => render_tui_file_browser(frame, areas.main, state),
        TuiViewMode::Chat => {
            let (info_area, transcript_area) = tui_chat_areas(areas.main);
            render_tui_chat_info_bar(frame, info_area, state);
            render_tui_transcript(frame, transcript_area, state);
            if let Some(completions) = areas.completions {
                render_tui_completions(frame, completions, state);
            }
        }
    }
    render_tui_input(frame, areas.input, state);
    render_tui_status(frame, areas.status, state);
}

fn render_tui_menu_bar(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    if area.is_empty() {
        return;
    }
    let cluster_label = tui_cluster_status_label(state);
    let cluster_width = cluster_label.chars().count().saturating_add(1) as u16;
    let cluster_width = cluster_width.min(area.width);
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(1), Constraint::Length(cluster_width)])
        .split(area);

    frame.render_widget(Paragraph::new(Line::from(tui_menu_spans(state))), chunks[0]);
    let cluster_style = if state.cluster_label.is_some() {
        Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    frame.render_widget(
        Paragraph::new(cluster_label)
            .alignment(Alignment::Right)
            .style(cluster_style),
        chunks[1],
    );
}

fn tui_menu_spans(state: &TuiState) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    for (index, tab) in tui_menu_tabs(&state.view_mode).into_iter().enumerate() {
        if index > 0 {
            spans.push(Span::raw(" "));
        }
        spans.push(Span::styled(tab.label, tui_menu_tab_style(tab.active)));
    }
    spans
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TuiMenuTab {
    label: &'static str,
    active: bool,
}

fn tui_menu_tabs(view_mode: &TuiViewMode) -> [TuiMenuTab; 4] {
    [
        TuiMenuTab {
            label: " Chat (F1) ",
            active: *view_mode == TuiViewMode::Chat,
        },
        TuiMenuTab {
            label: " Files (F2) ",
            active: *view_mode == TuiViewMode::Files,
        },
        TuiMenuTab {
            label: " Sessions (F3) ",
            active: *view_mode == TuiViewMode::Sessions,
        },
        TuiMenuTab {
            label: " Settings (F4) ",
            active: *view_mode == TuiViewMode::Settings,
        },
    ]
}

fn tui_menu_tab_at(area: Rect, column: u16, row: u16) -> Option<TuiViewMode> {
    if row != area.y || !rect_contains(area, column, row) {
        return None;
    }
    let tabs = [
        (TuiViewMode::Chat, " Chat (F1) "),
        (TuiViewMode::Files, " Files (F2) "),
        (TuiViewMode::Sessions, " Sessions (F3) "),
        (TuiViewMode::Settings, " Settings (F4) "),
    ];
    let mut x = area.x;
    for (mode, label) in tabs {
        let width = label.chars().count() as u16;
        if column >= x && column < x.saturating_add(width) {
            return Some(mode);
        }
        x = x.saturating_add(width).saturating_add(1);
    }
    None
}

fn tui_menu_tab_style(active: bool) -> Style {
    if active {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::White)
    }
}

fn tui_cluster_status_label(state: &TuiState) -> String {
    match &state.cluster_label {
        Some(cluster) => cluster.clone(),
        None => "cluster local".to_string(),
    }
}

fn tui_chat_areas(area: Rect) -> (Rect, Rect) {
    if area.height == 0 {
        return (area, area);
    }
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(0)])
        .split(area);
    (chunks[0], chunks[1])
}

fn render_tui_chat_info_bar(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    if area.is_empty() {
        return;
    }
    let cwd = env::current_dir()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|error| format!("cwd unavailable: {error}"));
    let cwd_budget = usize::from(area.width).saturating_sub(96).clamp(16, 80);
    let line = Line::from(vec![
        Span::styled("model ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            state.runtime_info.model_label(),
            Style::default().fg(Color::Cyan),
        ),
        Span::raw("  "),
        Span::styled("reason ", Style::default().fg(Color::DarkGray)),
        Span::raw(state.runtime_info.reasoning_label()),
        Span::raw("  "),
        Span::styled("context ", Style::default().fg(Color::DarkGray)),
        Span::raw(format_tui_context_usage(state.last_model_usage)),
        Span::raw("  "),
        Span::styled("session ", Style::default().fg(Color::DarkGray)),
        Span::raw(format_tui_session_token_usage(state.session_token_usage)),
        Span::raw("  "),
        Span::styled("cwd ", Style::default().fg(Color::DarkGray)),
        Span::raw(truncate_tui_text(&cwd, cwd_budget)),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

fn render_tui_transcript(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let selection = state.chat_selection.map(|selection| selection.range());
    let lines = tui_message_lines(&state.messages, selection);
    let visible_height = usize::from(area.height.saturating_sub(2));
    let scroll = tui_transcript_visible_start(lines.len(), visible_height, state.scrollback);
    let block = Block::default()
        .title(tui_transcript_title(
            state,
            scroll,
            visible_height,
            lines.len(),
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));
    let transcript = Paragraph::new(Text::from(lines))
        .block(block)
        .wrap(Wrap { trim: false })
        .scroll((scroll as u16, 0));
    frame.render_widget(transcript, area);
}

fn tui_transcript_visible_start(
    line_count: usize,
    visible_height: usize,
    scrollback: u16,
) -> usize {
    let max_scroll = line_count
        .saturating_sub(visible_height)
        .min(usize::from(u16::MAX));
    max_scroll.saturating_sub(usize::from(scrollback).min(max_scroll))
}

fn tui_transcript_title(
    state: &TuiState,
    visible_start: usize,
    visible_height: usize,
    line_count: usize,
) -> String {
    let visible_end = visible_start.saturating_add(visible_height).min(line_count);
    let mut title = if visible_start > 0 || visible_end < line_count {
        format!(
            "Session {}-{}/{}",
            visible_start + 1,
            visible_end,
            line_count
        )
    } else {
        "Session".to_string()
    };
    if let Some(selection) = state.chat_selection {
        title.push_str(&format!("  selected {}", selection.line_count()));
    }
    title
}

fn render_tui_completions(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let Some(completions) = &state.completions else {
        return;
    };
    let visible_height = usize::from(area.height.saturating_sub(2));
    if visible_height == 0 {
        return;
    }
    let (visible_start, visible_end) = completion_visible_range(
        completions.items.len(),
        completions.selected,
        visible_height,
    );
    let lines = completions
        .items
        .iter()
        .enumerate()
        .skip(visible_start)
        .take(visible_end.saturating_sub(visible_start))
        .map(|(index, item)| {
            let selected = completions.selected == Some(index);
            let prefix_style = if selected {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Cyan)
            };
            let detail_style = if selected {
                Style::default().fg(Color::Black).bg(Color::Cyan)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            let marker = if selected { "> " } else { "  " };
            Line::from(vec![
                Span::styled(marker, prefix_style),
                Span::styled(item.label.clone(), prefix_style),
                Span::styled("  ", detail_style),
                Span::styled(item.detail.clone(), detail_style),
            ])
        })
        .collect::<Vec<_>>();
    let title = if visible_start > 0 || visible_end < completions.items.len() {
        format!(
            "Slash Commands {}-{}/{}",
            visible_start + 1,
            visible_end,
            completions.items.len()
        )
    } else {
        "Slash Commands".to_string()
    };
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));
    frame.render_widget(Paragraph::new(Text::from(lines)).block(block), area);
}

fn render_tui_session_list(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let Some(list) = &state.session_list else {
        let block = Block::default()
            .title("Sessions")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Red));
        frame.render_widget(
            Paragraph::new("session list is not loaded").block(block),
            area,
        );
        return;
    };
    let visible_height = usize::from(area.height.saturating_sub(2));
    let (visible_start, visible_end) =
        completion_visible_range(list.sessions.len(), Some(list.selected), visible_height);
    let lines = list
        .sessions
        .iter()
        .enumerate()
        .skip(visible_start)
        .take(visible_end.saturating_sub(visible_start))
        .map(|(index, record)| {
            let selected = list.selected == index;
            let base_style = if selected {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else if record.ended {
                Style::default().fg(Color::DarkGray)
            } else {
                Style::default().fg(Color::White)
            };
            let detail_style = if selected {
                Style::default().fg(Color::Black).bg(Color::Cyan)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            let marker = if selected { "> " } else { "  " };
            let active = if record.session.session_id.to_string() == state.session_id {
                "active"
            } else if record.ended {
                "ended "
            } else {
                "open  "
            };
            Line::from(vec![
                Span::styled(marker, base_style),
                Span::styled(active, detail_style),
                Span::styled(" ", detail_style),
                Span::styled(record.session.session_id.to_string(), base_style),
                Span::styled("  ", detail_style),
                Span::styled(format_tui_timestamp(record.updated_at), detail_style),
                Span::styled("  ", detail_style),
                Span::styled(record.runtime_info.model_label(), detail_style),
                Span::styled("  ", detail_style),
                Span::styled(truncate_tui_text(&record.title, 72), base_style),
            ])
        })
        .collect::<Vec<_>>();
    let title = if visible_start > 0 || visible_end < list.sessions.len() {
        format!(
            "Sessions {}-{}/{}",
            visible_start + 1,
            visible_end,
            list.sessions.len()
        )
    } else {
        "Sessions".to_string()
    };
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));
    frame.render_widget(Paragraph::new(Text::from(lines)).block(block), area);
}

fn render_tui_settings(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let Some(settings) = &state.settings else {
        let block = Block::default()
            .title("Settings")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Red));
        frame.render_widget(Paragraph::new("settings are not loaded").block(block), area);
        return;
    };
    let (entries_area, detail_area) = tui_settings_areas(area);
    render_tui_settings_entries(frame, entries_area, settings);
    render_tui_settings_detail(frame, detail_area, settings);
}

fn render_tui_settings_entries(frame: &mut Frame<'_>, area: Rect, settings: &TuiSettingsState) {
    let visible_height = usize::from(area.height.saturating_sub(2));
    let (visible_start, visible_end) = completion_visible_range(
        settings.entries.len(),
        Some(settings.selected),
        visible_height,
    );
    let lines = settings
        .entries
        .iter()
        .enumerate()
        .skip(visible_start)
        .take(visible_end.saturating_sub(visible_start))
        .map(|(index, entry)| {
            let selected = settings.selected == index;
            let key_style = if selected {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else if entry.error.is_some() {
                Style::default().fg(Color::Yellow)
            } else {
                Style::default().fg(Color::White)
            };
            let detail_style = if selected {
                Style::default().fg(Color::Black).bg(Color::Cyan)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            let marker = if selected { "> " } else { "  " };
            let source = entry
                .source
                .map(|source| format!("{source:?}"))
                .unwrap_or_else(|| "unset".to_string());
            Line::from(vec![
                Span::styled(marker, key_style),
                Span::styled(entry.key().to_string(), key_style),
                Span::styled("  ", detail_style),
                Span::styled(source, detail_style),
                Span::styled("  ", detail_style),
                Span::styled(truncate_tui_text(&entry.value_preview(), 36), detail_style),
            ])
        })
        .collect::<Vec<_>>();
    let title = if visible_start > 0 || visible_end < settings.entries.len() {
        format!(
            "Settings {}-{}/{}",
            visible_start + 1,
            visible_end,
            settings.entries.len()
        )
    } else {
        "Settings".to_string()
    };
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));
    frame.render_widget(Paragraph::new(Text::from(lines)).block(block), area);
}

fn render_tui_settings_detail(frame: &mut Frame<'_>, area: Rect, settings: &TuiSettingsState) {
    let lines = match settings.selected_entry() {
        Some(entry) => tui_settings_detail_lines(entry, settings.editing_key.as_deref()),
        None => vec![Line::from(Span::raw("no settings"))],
    };
    let title = settings
        .selected_entry()
        .map(|entry| entry.key().to_string())
        .unwrap_or_else(|| "Setting".to_string());
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(block)
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn tui_settings_detail_lines(
    entry: &TuiSettingsEntry,
    editing_key: Option<&str>,
) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::from(vec![
            Span::styled("owner ", Style::default().fg(Color::DarkGray)),
            Span::raw(entry.definition.owner.clone()),
        ]),
        Line::from(vec![
            Span::styled("mode ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:?}", entry.definition.cluster_mode)),
        ]),
        Line::from(vec![
            Span::styled("type ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:?}", entry.definition.value_type)),
        ]),
        Line::from(vec![
            Span::styled("source ", Style::default().fg(Color::DarkGray)),
            Span::raw(
                entry
                    .source
                    .map(|source| format!("{source:?}"))
                    .unwrap_or_else(|| "unset".to_string()),
            ),
        ]),
        Line::from(vec![
            Span::styled("local ", Style::default().fg(Color::DarkGray)),
            Span::raw(entry.definition.key.as_str().replace('.', "/") + ".json"),
        ]),
        Line::from(vec![
            Span::styled("cluster ", Style::default().fg(Color::DarkGray)),
            Span::raw(entry.definition.cluster_key()),
        ]),
        Line::from(""),
        Line::from(entry.definition.description.clone()),
        Line::from(""),
        Line::from(vec![Span::styled(
            "value",
            Style::default().fg(Color::DarkGray),
        )]),
    ];
    match (&entry.error, &entry.value) {
        (Some(error), None) => lines.push(Line::from(Span::styled(
            error.clone(),
            Style::default().fg(Color::Yellow),
        ))),
        (_, Some(_)) if tui_config_key_is_sensitive(entry.key()) => {
            lines.push(Line::from("<redacted>"));
        }
        (_, Some(value)) => {
            let pretty =
                serde_json::to_string_pretty(value).unwrap_or_else(|_| compact_tui_json(value));
            lines.extend(pretty.lines().map(|line| Line::from(line.to_string())));
        }
        _ => lines.push(Line::from("unset")),
    }
    if editing_key == Some(entry.key()) {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "editing JSON in input",
            Style::default().fg(Color::Yellow),
        )));
    }
    lines
}

fn render_tui_file_browser(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let Some(browser) = &state.file_browser else {
        let block = Block::default()
            .title("Files")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Red));
        frame.render_widget(
            Paragraph::new("file browser is not open").block(block),
            area,
        );
        return;
    };
    let (entries_area, preview_area) = tui_file_browser_areas(area);
    render_tui_file_entries(frame, entries_area, browser);
    render_tui_file_preview(frame, preview_area, browser);
}

fn render_tui_file_entries(frame: &mut Frame<'_>, area: Rect, browser: &FileManagerState) {
    let visible_height = usize::from(area.height.saturating_sub(2));
    let (visible_start, visible_end) = completion_visible_range(
        browser.entries.len(),
        Some(browser.selected),
        visible_height,
    );
    let lines = browser
        .entries
        .iter()
        .enumerate()
        .skip(visible_start)
        .take(visible_end.saturating_sub(visible_start))
        .map(|(index, entry)| {
            let selected = browser.selected == index;
            let entry_style = if selected {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else if entry.kind == FileManagerEntryKind::Directory {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default().fg(Color::White)
            };
            let detail_style = if selected {
                Style::default().fg(Color::Black).bg(Color::Cyan)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            let marker = if selected { "> " } else { "  " };
            let size = entry.size.map(format_file_size).unwrap_or_else(String::new);
            Line::from(vec![
                Span::styled(marker, entry_style),
                Span::styled(entry.kind.label(), detail_style),
                Span::styled(" ", detail_style),
                Span::styled(entry.name.clone(), entry_style),
                Span::styled(
                    if size.is_empty() {
                        String::new()
                    } else {
                        format!("  {size}")
                    },
                    detail_style,
                ),
            ])
        })
        .collect::<Vec<_>>();
    let title = if visible_start > 0 || visible_end < browser.entries.len() {
        format!(
            "Files ({}) {}-{}/{}  {}",
            browser.source_label(),
            visible_start + 1,
            visible_end,
            browser.entries.len(),
            browser.current_path()
        )
    } else {
        format!(
            "Files ({})  {}",
            browser.source_label(),
            browser.current_path()
        )
    };
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));
    frame.render_widget(Paragraph::new(Text::from(lines)).block(block), area);
}

fn render_tui_file_preview(frame: &mut Frame<'_>, area: Rect, browser: &FileManagerState) {
    let lines = browser
        .preview
        .lines
        .iter()
        .map(|line| Line::from(Span::raw(line.clone())))
        .collect::<Vec<_>>();
    let block = Block::default()
        .title(browser.preview.title.clone())
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(block)
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn completion_visible_range(
    item_count: usize,
    selected: Option<usize>,
    visible_height: usize,
) -> (usize, usize) {
    if item_count == 0 || visible_height == 0 {
        return (0, 0);
    }
    if item_count <= visible_height {
        return (0, item_count);
    }

    let selected = selected.unwrap_or(0).min(item_count - 1);
    let max_start = item_count - visible_height;
    let start = selected.saturating_sub(visible_height / 2).min(max_start);
    (start, start + visible_height)
}

fn render_tui_input(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let inner_width = area.width.saturating_sub(2).max(1);
    let display_width = inner_width.saturating_sub(1).max(1);
    let (input, cursor_column) = state.input_view(display_width);
    let block = Block::default()
        .title(state.input_title())
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Green));
    frame.render_widget(Paragraph::new(input).block(block), area);
    if state.accepts_text_input() && area.width > 2 && area.height > 2 {
        let cursor_x = area
            .x
            .saturating_add(1)
            .saturating_add(cursor_column.min(inner_width.saturating_sub(1)));
        let cursor_y = area.y.saturating_add(1);
        frame.set_cursor_position(Position::new(cursor_x, cursor_y));
    }
}

fn render_tui_status(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let status_style =
        if state.status.starts_with("file browser error:") || state.status == "ending session" {
            Style::default().fg(Color::Red)
        } else {
            match state.status.as_str() {
                "working" | "streaming" | "running command" | "interrupt requested" => {
                    Style::default().fg(Color::Yellow)
                }
                "agent task failed" | "command timed out" | "command cancelled"
                | "model failed" => Style::default().fg(Color::Red),
                _ => Style::default().fg(Color::Green),
            }
        };
    if state.view_mode == TuiViewMode::Files {
        let line = Line::from(vec![
            Span::styled(
                state.status.clone(),
                status_style.add_modifier(Modifier::BOLD),
            ),
            Span::raw("   "),
            Span::styled("Up/Down", Style::default().fg(Color::DarkGray)),
            Span::raw(" move   "),
            Span::styled("Enter", Style::default().fg(Color::DarkGray)),
            Span::raw(" open   "),
            Span::styled("Backspace", Style::default().fg(Color::DarkGray)),
            Span::raw(" parent   "),
            Span::styled("n/m/r/x", Style::default().fg(Color::DarkGray)),
            Span::raw(" new/mkdir/rename/delete   "),
            Span::styled("F1/F2/Esc", Style::default().fg(Color::DarkGray)),
            Span::raw(" chat"),
        ]);
        frame.render_widget(Paragraph::new(line), area);
        return;
    }
    if state.view_mode == TuiViewMode::Sessions {
        let line = Line::from(vec![
            Span::styled(
                state.status.clone(),
                status_style.add_modifier(Modifier::BOLD),
            ),
            Span::raw("   "),
            Span::styled("Up/Down", Style::default().fg(Color::DarkGray)),
            Span::raw(" move   "),
            Span::styled("Enter/r", Style::default().fg(Color::DarkGray)),
            Span::raw(" resume   "),
            Span::styled("Esc/F1", Style::default().fg(Color::DarkGray)),
            Span::raw(" chat"),
        ]);
        frame.render_widget(Paragraph::new(line), area);
        return;
    }
    if state.view_mode == TuiViewMode::Settings {
        let editing = state
            .settings
            .as_ref()
            .is_some_and(|settings| settings.editing_key.is_some());
        let line = if editing {
            Line::from(vec![
                Span::styled(
                    state.status.clone(),
                    status_style.add_modifier(Modifier::BOLD),
                ),
                Span::raw("   "),
                Span::styled("Enter", Style::default().fg(Color::DarkGray)),
                Span::raw(" save   "),
                Span::styled("Esc", Style::default().fg(Color::DarkGray)),
                Span::raw(" cancel   "),
                Span::styled("Ctrl+U", Style::default().fg(Color::DarkGray)),
                Span::raw(" clear"),
            ])
        } else {
            Line::from(vec![
                Span::styled(
                    state.status.clone(),
                    status_style.add_modifier(Modifier::BOLD),
                ),
                Span::raw("   "),
                Span::styled("Up/Down", Style::default().fg(Color::DarkGray)),
                Span::raw(" move   "),
                Span::styled("Enter/e", Style::default().fg(Color::DarkGray)),
                Span::raw(" edit   "),
                Span::styled("r", Style::default().fg(Color::DarkGray)),
                Span::raw(" reload   "),
                Span::styled("Esc/F1", Style::default().fg(Color::DarkGray)),
                Span::raw(" chat"),
            ])
        };
        frame.render_widget(Paragraph::new(line), area);
        return;
    }
    if let Some(selection) = state.chat_selection {
        let line = Line::from(vec![
            Span::styled(
                format!("selection: {} line(s)", selection.line_count()),
                status_style.add_modifier(Modifier::BOLD),
            ),
            Span::raw("   "),
            Span::styled("Mouse drag", Style::default().fg(Color::DarkGray)),
            Span::raw(" extend   "),
            Span::styled("Up/Down", Style::default().fg(Color::DarkGray)),
            Span::raw(" extend   "),
            Span::styled("Ctrl+A", Style::default().fg(Color::DarkGray)),
            Span::raw(" all   "),
            Span::styled("Esc/Alt+S", Style::default().fg(Color::DarkGray)),
            Span::raw(" clear   "),
            Span::styled("Wheel/PgUp/PgDn", Style::default().fg(Color::DarkGray)),
            Span::raw(" scroll"),
        ]);
        frame.render_widget(Paragraph::new(line), area);
        return;
    }
    let line = Line::from(vec![
        Span::styled(
            state.status.clone(),
            status_style.add_modifier(Modifier::BOLD),
        ),
        Span::raw("   "),
        Span::styled("Enter", Style::default().fg(Color::DarkGray)),
        Span::raw(" send   "),
        Span::styled("Up/Down", Style::default().fg(Color::DarkGray)),
        Span::raw(" select   "),
        Span::styled("Alt+S", Style::default().fg(Color::DarkGray)),
        Span::raw(" select mode   "),
        Span::styled("Tab/Right", Style::default().fg(Color::DarkGray)),
        Span::raw(" accept   "),
        Span::styled("Esc", Style::default().fg(Color::DarkGray)),
        Span::raw(" quit   "),
        Span::styled("Ctrl+C", Style::default().fg(Color::DarkGray)),
        Span::raw(" interrupt/quit   "),
        Span::styled("F3", Style::default().fg(Color::DarkGray)),
        Span::raw(" sessions   "),
        Span::styled("PgUp/PgDn", Style::default().fg(Color::DarkGray)),
        Span::raw(" scroll"),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

fn tui_message_lines(
    messages: &[TuiMessage],
    selection: Option<(usize, usize)>,
) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    let mut transcript_line_index = 0;
    for message in messages {
        let prefix = tui_message_prefix(&message.kind);
        let style = tui_message_style(&message.kind);
        if message.text.is_empty() {
            let selected = tui_line_is_selected(transcript_line_index, selection);
            let selected_style = tui_transcript_selection_style(selected);
            lines.push(Line::from(vec![Span::styled(
                prefix,
                selected_style.unwrap_or(style),
            )]));
            transcript_line_index += 1;
            continue;
        }
        for (line_index, text) in message.text.lines().enumerate() {
            let selected = tui_line_is_selected(transcript_line_index, selection);
            let selected_style = tui_transcript_selection_style(selected);
            let display_prefix = if line_index == 0 {
                prefix.to_string()
            } else {
                " ".repeat(prefix.len())
            };
            lines.push(Line::from(vec![
                Span::styled(display_prefix, selected_style.unwrap_or(style)),
                Span::styled(text.to_string(), selected_style.unwrap_or_default()),
            ]));
            transcript_line_index += 1;
        }
    }
    lines
}

fn tui_line_is_selected(line_index: usize, selection: Option<(usize, usize)>) -> bool {
    selection
        .map(|(start, end)| line_index >= start && line_index <= end)
        .unwrap_or(false)
}

fn tui_transcript_selection_style(selected: bool) -> Option<Style> {
    selected.then_some(
        Style::default()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
}

fn tui_message_line_count(messages: &[TuiMessage]) -> usize {
    messages
        .iter()
        .map(|message| {
            if message.text.is_empty() {
                1
            } else {
                message.text.lines().count().max(1)
            }
        })
        .sum()
}

fn tui_message_prefix(kind: &TuiMessageKind) -> &'static str {
    match kind {
        TuiMessageKind::User => "you> ",
        TuiMessageKind::Command => "cmd> ",
        TuiMessageKind::Agent => "sinomo> ",
        TuiMessageKind::Thinking => "think> ",
        TuiMessageKind::System => "system> ",
        TuiMessageKind::Error => "error> ",
    }
}

fn tui_message_style(kind: &TuiMessageKind) -> Style {
    match kind {
        TuiMessageKind::User => Style::default().fg(Color::Blue),
        TuiMessageKind::Command => Style::default().fg(Color::Magenta),
        TuiMessageKind::Agent => Style::default().fg(Color::Green),
        TuiMessageKind::Thinking => Style::default().fg(Color::Yellow),
        TuiMessageKind::System => Style::default().fg(Color::DarkGray),
        TuiMessageKind::Error => Style::default().fg(Color::Red),
    }
}

#[cfg(test)]
mod tui_tests {
    use super::*;

    fn fixture_tui_state() -> TuiState {
        let session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_test").expect("node"),
            InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
        );
        TuiState::new(&session, None, TuiAgentRuntimeInfo::default())
    }

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn press_mod(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    fn mouse(kind: MouseEventKind, column: u16, row: u16) -> MouseEvent {
        MouseEvent {
            kind,
            column,
            row,
            modifiers: KeyModifiers::NONE,
        }
    }

    fn temp_file_browser_root(name: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(&format!("sinomo-tui-file-browser-{name}"))
    }

    fn session_record(session_id: SessionId, title: &str) -> TuiSessionRecord {
        TuiSessionRecord {
            schema_version: TUI_SESSION_SCHEMA_VERSION,
            session: InteractionSession::new(
                session_id,
                NodeId::from_legacy_named("node_mouse").expect("node"),
                InteractionSurface::new("surface.mouse", InteractionSurfaceKind::CommandLine),
            ),
            workspaces: Vec::new(),
            workspace_projection_version: TUI_WORKSPACE_PROJECTION_VERSION,
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            title: title.to_string(),
            message_count: 1,
            ended: false,
            runtime_info: TuiAgentRuntimeInfo::default(),
        }
    }

    fn runtime_event(payload: AgentRuntimeEventPayload) -> AgentRuntimeEvent {
        AgentRuntimeEvent::new(SessionId::generate(), payload)
    }

    struct StableProjectionLineHandler {
        events: Vec<AgentRuntimeEvent>,
    }

    impl InteractionEventHandler for StableProjectionLineHandler {
        fn handle_event<S: EventStore>(
            &mut self,
            _store: &mut S,
            session: &InteractionSession,
            event: &EventEnvelope,
        ) -> Result<Vec<InteractionOutput>, CliError> {
            let text = event
                .payload
                .get("text")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if text.trim_start().starts_with('/') {
                return Ok(vec![InteractionOutput::Exit]);
            }
            self.events.push(AgentRuntimeEvent::new(
                session.session_id.clone(),
                AgentRuntimeEventPayload::TurnFinalDecision {
                    sequence: 1,
                    op_id: TaskId::from_legacy_named("line_projection_op").expect("op id"),
                    decision: AgentLoopDecision {
                        response_mode: sinomo_agent::AgentLoopResponseMode::Answer,
                        text: "typed answer".to_string(),
                        tool_calls: Vec::new(),
                        background_handoff: None,
                        next_state: sinomo_agent::AgentInteractionState::Listening,
                    },
                    stop_reason: sinomo_agent::AgentStopReason::FinalDecision,
                },
            ));
            Ok(Vec::new())
        }

        fn stable_chat_agent_key(&self) -> Option<String> {
            Some("main-agent".to_string())
        }

        fn drain_stable_chat_runtime_events(&mut self) -> Result<Vec<AgentRuntimeEvent>, CliError> {
            Ok(std::mem::take(&mut self.events))
        }
    }

    #[test]
    fn line_mode_projects_typed_user_and_agent_messages_to_sifs_chat() {
        use crate::app_api::SifsAgentChannelStore;
        use sinomo_agent::{AgentChannelAuthor, AgentChannelKind, AgentChannelStore};

        let root = sinomo_testdir::TestDir::new("sinomo-line-chat-projection");
        let data_dir = SinomoDataDir::new(root.to_path_buf());
        let mut output = Vec::new();
        run_tui_with_handler_io_and_daemon_and_projection(
            std::io::Cursor::new("question\n/quit\n"),
            &mut output,
            StableProjectionLineHandler { events: Vec::new() },
            None,
            Some(&data_dir),
        )
        .expect("line mode");

        let channels = SifsAgentChannelStore::open(&data_dir)
            .expect("store")
            .list_channels()
            .expect("channels");
        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].channel_kind, AgentChannelKind::Chat);
        assert_eq!(channels[0].transcript.len(), 2);
        assert_eq!(channels[0].transcript[0].content, "question");
        assert_eq!(channels[0].transcript[1].content, "typed answer");
        assert!(matches!(
            channels[0].transcript[1].author,
            AgentChannelAuthor::Agent { ref agent_key, .. } if agent_key == "main-agent"
        ));
        assert!(
            String::from_utf8(output)
                .expect("utf8")
                .contains("typed answer")
        );
    }

    fn process_snapshot(
        status: sinomo_agent::AgentProcessLifecycleStatus,
        stdout: &str,
        stderr: &str,
    ) -> sinomo_agent::AgentProcessSnapshot {
        sinomo_agent::AgentProcessSnapshot {
            process_id: sinomo_agent::TaskId::generate().to_string(),
            command: "printf progress".to_string(),
            program: "printf".to_string(),
            args: vec!["progress".to_string()],
            cwd: ".".to_string(),
            status,
            exit_code: Some(0),
            timed_out: false,
            cancel_requested: false,
            error: None,
            elapsed_ms: 12,
            timeout_ms: None,
            stdout: stdout.to_string(),
            stderr: stderr.to_string(),
            stdout_offset: stdout.len(),
            stderr_offset: stderr.len(),
            stdout_total_bytes: stdout.len(),
            stderr_total_bytes: stderr.len(),
            stdout_truncated: false,
            stderr_truncated: false,
        }
    }

    struct StaticTuiModel {
        content: String,
    }

    impl StaticTuiModel {
        fn new(content: impl Into<String>) -> Self {
            Self {
                content: content.into(),
            }
        }
    }

    impl InteractionModel for StaticTuiModel {
        fn complete(
            &self,
            request: &sinomo_lm::LanguageModelRequest,
        ) -> Result<sinomo_lm::LanguageModelResponse, sinomo_lm::LanguageModelError> {
            Ok(sinomo_lm::LanguageModelResponse {
                provider: request.provider.clone(),
                model: request.model.clone(),
                content: serde_json::json!({
                    "response_mode": "answer",
                    "text": self.content.as_str(),
                    "tool_calls": [],
                    "background_handoff": null,
                    "next_state": "listening"
                })
                .to_string(),
                items: Vec::new(),
                tool_calls: Vec::new(),
                usage: Some(sinomo_lm::LanguageModelUsage {
                    input_tokens: Some(7),
                    output_tokens: Some(3),
                }),
            })
        }
    }

    struct CapturingTuiModel {
        requests: Arc<Mutex<Vec<sinomo_lm::LanguageModelRequest>>>,
    }

    impl CapturingTuiModel {
        fn new() -> (Self, Arc<Mutex<Vec<sinomo_lm::LanguageModelRequest>>>) {
            let requests = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    requests: Arc::clone(&requests),
                },
                requests,
            )
        }
    }

    impl InteractionModel for CapturingTuiModel {
        fn complete(
            &self,
            request: &sinomo_lm::LanguageModelRequest,
        ) -> Result<sinomo_lm::LanguageModelResponse, sinomo_lm::LanguageModelError> {
            self.requests
                .lock()
                .expect("requests")
                .push(request.clone());
            Ok(sinomo_lm::LanguageModelResponse {
                provider: request.provider.clone(),
                model: request.model.clone(),
                content: serde_json::json!({
                    "response_mode": "answer",
                    "text": "explicit skill used",
                    "tool_calls": [],
                    "background_handoff": null,
                    "next_state": "listening"
                })
                .to_string(),
                items: Vec::new(),
                tool_calls: Vec::new(),
                usage: None,
            })
        }
    }

    #[test]
    fn tui_state_edits_input_at_cursor() {
        let mut state = fixture_tui_state();

        state.insert_str("ab");
        state.move_cursor_left();
        state.insert_char('x');
        state.backspace();
        state.insert_char('y');

        assert_eq!(state.input, "ayb");
        assert_eq!(state.input_cursor, 2);
    }

    #[test]
    fn tui_state_records_submission_and_streamed_outputs() {
        let mut state = fixture_tui_state();

        state.begin_submission("hello");
        state.apply_output(InteractionOutput::Chunk("thinking> checking".to_string()));
        state.apply_output(InteractionOutput::Chunk(" cluster".to_string()));
        state.apply_output(InteractionOutput::Line("sinomo: done".to_string()));
        state.finish_submission();

        assert_eq!(state.messages[1].kind, TuiMessageKind::User);
        assert_eq!(state.messages[1].text, "hello");
        assert_eq!(state.messages[2].kind, TuiMessageKind::Thinking);
        assert_eq!(state.messages[2].text, "checking cluster");
        assert_eq!(state.messages[3].kind, TuiMessageKind::Agent);
        assert_eq!(state.messages[3].text, "done");
        assert_eq!(state.status, "listening");
    }

    #[test]
    fn tui_state_renders_runtime_command_progress_and_interrupts_active_turn() {
        let mut state = fixture_tui_state();

        assert_eq!(
            state.apply_key_event(press_mod(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            TuiInputAction::Exit
        );
        assert!(state.apply_runtime_event(runtime_event(
            AgentRuntimeEventPayload::WorkerStarted {
                sequence: 1,
                op_id: sinomo_agent::TaskId::generate(),
                action: "start_turn".to_string(),
                timeout_ms: None,
            },
        )));
        assert_eq!(
            state.apply_key_event(press_mod(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            TuiInputAction::InterruptAgent
        );
        state.apply_runtime_event(runtime_event(AgentRuntimeEventPayload::ModelProgress {
            sequence: 1,
            op_id: sinomo_agent::TaskId::generate(),
            event: AgentModelEvent::AssistantTextDelta {
                step_index: 0,
                delta: "thinking out loud".to_string(),
            },
        }));

        assert!(
            state
                .messages
                .iter()
                .any(|message| message.kind == TuiMessageKind::Agent
                    && message.text.contains("thinking out loud"))
        );

        state.apply_runtime_event(runtime_event(AgentRuntimeEventPayload::ToolProgress {
            sequence: 1,
            op_id: sinomo_agent::TaskId::generate(),
            event: AgentToolProgressEvent::ProcessStarted {
                process: process_snapshot(
                    sinomo_agent::AgentProcessLifecycleStatus::Running,
                    "",
                    "",
                ),
            },
        }));
        state.apply_runtime_event(runtime_event(AgentRuntimeEventPayload::ToolProgress {
            sequence: 1,
            op_id: sinomo_agent::TaskId::generate(),
            event: AgentToolProgressEvent::ProcessOutput {
                process: process_snapshot(
                    sinomo_agent::AgentProcessLifecycleStatus::Running,
                    "hello\n",
                    "warn\n",
                ),
            },
        }));

        assert!(
            state
                .messages
                .iter()
                .any(|message| message.kind == TuiMessageKind::Command
                    && message.text.contains("stdout> hello")
                    && message.text.contains("stderr> warn"))
        );

        state.apply_runtime_event(runtime_event(AgentRuntimeEventPayload::ToolProgress {
            sequence: 1,
            op_id: sinomo_agent::TaskId::generate(),
            event: AgentToolProgressEvent::ProcessCompleted {
                process: process_snapshot(
                    sinomo_agent::AgentProcessLifecycleStatus::Completed,
                    "",
                    "",
                ),
            },
        }));
        state.apply_runtime_event(runtime_event(AgentRuntimeEventPayload::WorkerCompleted {
            sequence: 1,
            op_id: sinomo_agent::TaskId::generate(),
            action: "start_turn".to_string(),
            stop_reason: sinomo_agent::AgentStopReason::FinalDecision,
        }));

        assert!(!state.runtime_activity.active);
        assert_eq!(
            state.apply_key_event(press_mod(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            TuiInputAction::Exit
        );
    }

    #[test]
    fn tui_state_suppresses_structured_agent_json_model_delta() {
        let mut state = fixture_tui_state();

        state.apply_runtime_event(runtime_event(AgentRuntimeEventPayload::ModelProgress {
            sequence: 1,
            op_id: sinomo_agent::TaskId::generate(),
            event: AgentModelEvent::AssistantTextDelta {
                step_index: 0,
                delta: serde_json::json!({
                    "response_mode": "answer",
                    "text": "ready",
                    "tool_calls": [],
                    "background_handoff": null,
                    "next_state": "listening"
                })
                .to_string(),
            },
        }));
        assert!(
            !state
                .messages
                .iter()
                .any(|message| message.text.contains("response_mode"))
        );

        state.apply_runtime_event(runtime_event(AgentRuntimeEventPayload::TurnFinalDecision {
            sequence: 2,
            op_id: sinomo_agent::TaskId::generate(),
            decision: AgentLoopDecision {
                response_mode: sinomo_agent::AgentLoopResponseMode::Answer,
                text: "ready".to_string(),
                tool_calls: Vec::new(),
                background_handoff: None,
                next_state: sinomo_agent::AgentInteractionState::Listening,
            },
            stop_reason: sinomo_agent::AgentStopReason::FinalDecision,
        }));

        assert!(
            state
                .messages
                .iter()
                .any(|message| message.kind == TuiMessageKind::Agent && message.text == "ready")
        );
    }

    #[test]
    fn tui_keeps_text_and_sanitized_background_handoff_request_visible() {
        let mut state = fixture_tui_state();
        let event = runtime_event(AgentRuntimeEventPayload::TurnFinalDecision {
            sequence: 2,
            op_id: sinomo_agent::TaskId::generate(),
            decision: AgentLoopDecision {
                response_mode: sinomo_agent::AgentLoopResponseMode::Acknowledge,
                text: "I will continue this in the background.".to_string(),
                tool_calls: Vec::new(),
                background_handoff: Some(sinomo_agent::BackgroundHandoffRequest {
                    reason: "observe recovery".to_string(),
                    evidence_ref: "sifs:/private/evidence/secret.json".to_string(),
                    priority: sinomo_agent::BackgroundHandoffPriority::Routine,
                }),
                next_state: sinomo_agent::AgentInteractionState::Delegating,
            },
            stop_reason: sinomo_agent::AgentStopReason::FinalDecision,
        });

        assert_eq!(
            stable_chat_runtime_event_lines(&event),
            vec![
                "I will continue this in the background.".to_string(),
                "Background handoff requested (routine): observe recovery".to_string(),
            ]
        );
        state.apply_runtime_event(event);

        assert!(state.messages.iter().any(|message| {
            message.kind == TuiMessageKind::Agent
                && message.text == "I will continue this in the background."
        }));
        let handoff = state
            .messages
            .iter()
            .find(|message| message.text.starts_with("Background handoff requested"))
            .expect("handoff notice");
        assert_eq!(handoff.kind, TuiMessageKind::System);
        assert_eq!(
            handoff.text,
            "Background handoff requested (routine): observe recovery"
        );
        assert!(!handoff.text.contains("evidence"));
        assert!(!handoff.text.contains("secret"));
    }

    #[test]
    fn runtime_tui_handler_queues_chat_turn_and_streams_terminal_decision() {
        let session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_runtime_tui").expect("node"),
            InteractionSurface::new("surface.runtime-tui", InteractionSurfaceKind::CommandLine),
        );
        let mut store = InMemoryEventStore::new();
        let mut handler = RuntimeMainAgentInteractionHandler::with_thinking(
            StaticTuiModel::new("runtime hello"),
            "fake",
            "fake-model",
            ThinkingControl::default(),
        );
        let event = user_interaction_input_event(&session, "hello");
        store.append(event.clone()).expect("append input");

        let outputs = handler
            .handle_event(&mut store, &session, &event)
            .expect("submit");

        assert!(outputs.is_empty());
        let runtime = handler.ready_runtime().expect("runtime");
        runtime.join_active_turn().expect("join");
        let batch = runtime
            .runtime_events_since(AgentRuntimeEventCursor {
                offset: 0,
                last_event_id: None,
            })
            .expect("events");
        assert!(batch.events.iter().any(|event| matches!(
            event.payload,
            AgentRuntimeEventPayload::WorkerStarted { .. }
        )));
        assert!(batch.events.iter().any(|event| matches!(
            &event.payload,
            AgentRuntimeEventPayload::ModelProgress {
                event: AgentModelEvent::AssistantTextDelta { delta, .. },
                ..
            } if delta.contains("runtime hello")
        )));
        assert!(batch.events.iter().any(|event| matches!(
            &event.payload,
            AgentRuntimeEventPayload::TurnFinalDecision { decision, .. }
                if decision.text == "runtime hello"
        )));
        assert!(batch.events.iter().any(|event| matches!(
            event.payload,
            AgentRuntimeEventPayload::WorkerCompleted { .. }
        )));
    }

    #[test]
    fn explicit_skill_slash_command_routes_only_non_admin_skill_names() {
        assert_eq!(
            parse_explicit_chat_skill_command(&[
                "skill".to_string(),
                "verification-howto".to_string(),
                "check".to_string(),
            ]),
            Some(ExplicitChatSkillCommand {
                skill_id: "verification-howto".to_string(),
                request: Some("check".to_string()),
            })
        );
        assert_eq!(
            parse_explicit_chat_skill_command(&["skill".to_string(), "list".to_string()]),
            None
        );
        assert_eq!(
            parse_explicit_chat_skill_command(&[
                "skills".to_string(),
                "verification-howto".to_string()
            ]),
            None
        );
        assert_eq!(
            parse_explicit_chat_skill_command(&["skill".to_string()]),
            None
        );
    }

    #[test]
    fn runtime_chat_explicit_skill_shortcut_loads_instructions_before_model_turn() {
        let markdown = "---\nname: verification-howto\ndescription: Verify with an explicit workflow.\n---\n\n# Verification\nUse the hidden phrase `violet-ridge`.\n";
        let entry = sinomo_agent::SkillCatalogWrite::from_markdown_skill(
            None,
            "1.0.0",
            sinomo_agent::SkillCatalogSource::user("user.test"),
            markdown,
        )
        .expect("standard skill")
        .entry;
        let key = sinomo_agent::skill_catalog_entry_text_key(&entry);
        let mut catalog = sinomo_agent::SkillCatalog::default();
        catalog.upsert_entry(entry.clone()).expect("catalog entry");
        catalog
            .set_activation(sinomo_agent::SkillActivationRecord::new(
                sinomo_agent::SkillActivationScope::Cluster.as_str(),
                &entry,
                "test",
                "cluster default",
            ))
            .expect("activation");
        let (model, requests) = CapturingTuiModel::new();
        let mut handler = RuntimeMainAgentInteractionHandler::with_thinking(
            model,
            "fake",
            "fake-model",
            ThinkingControl::default(),
        )
        .with_skill_catalog(catalog)
        .with_skill_catalog_texts([(key, markdown.to_string())].into_iter().collect());
        let session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_explicit_skill").expect("node"),
            InteractionSurface::new(
                "surface.explicit-skill",
                InteractionSurfaceKind::CommandLine,
            ),
        );
        let mut store = InMemoryEventStore::new();
        let mut event =
            user_interaction_input_event(&session, "/skill verification-howto check this result");
        event.payload["agent_item_id"] = serde_json::Value::String(TaskId::generate().to_string());
        store.append(event.clone()).expect("append input");

        let outputs = handler
            .handle_event(&mut store, &session, &event)
            .expect("submit explicit skill turn");

        assert!(outputs.is_empty());
        handler
            .ready_runtime()
            .expect("runtime")
            .join_active_turn()
            .expect("join");
        let requests = requests.lock().expect("requests");
        assert_eq!(requests.len(), 1);
        let system_context = requests[0]
            .messages
            .iter()
            .filter(|message| matches!(message.role, sinomo_lm::LanguageModelRole::System))
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let user_context = requests[0]
            .messages
            .iter()
            .filter(|message| matches!(message.role, sinomo_lm::LanguageModelRole::User))
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(system_context.contains("explicit_agent_skill"));
        assert!(system_context.contains("verification-howto"));
        assert!(system_context.contains("violet-ridge"));
        assert!(!system_context.contains("check this result"));
        assert!(user_context.contains("explicit_skill_request"));
        assert!(
            user_context.contains("\"request\": \"check this result\""),
            "{user_context}"
        );
    }

    #[test]
    fn transient_system_context_does_not_accumulate_across_persisted_runtime_turns() {
        let root = sinomo_testdir::TestDir::new("tui-transient-system-context");
        let data_dir = SinomoDataDir::new(&root);
        let session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_transient_context").expect("node"),
            InteractionSurface::new(
                "surface.transient-context",
                InteractionSurfaceKind::CommandLine,
            ),
        );
        let handler = RuntimeMainAgentInteractionHandler::with_thinking(
            StaticTuiModel::new("channel reply"),
            "fake",
            "fake-model",
            ThinkingControl::default(),
        )
        .with_runtime_storage(AgentRuntimeStorage::from_data_dir(&data_dir));
        let mut runtime =
            InteractionRuntime::new(InMemoryEventStore::new(), handler, session.clone());
        let mut durable_session = AgentSession::new(session.session_id.clone(), AgentRole::Main)
            .with_agent_definition_key("main-agent");
        let first_visible = sinomo_agent::AgentItem::UserMessage {
            item_id: TaskId::generate(),
            step_index: 0,
            content: "User: first visible channel message".to_string(),
            event_id: None,
        };
        durable_session.transcript.push(first_visible.clone());
        durable_session.raw_transcript.push(first_visible);
        runtime
            .handler_mut()
            .replace_full_agent_session(&session, durable_session)
            .expect("attach first channel snapshot");

        runtime
            .ingest_transient_system_context_with_attachments_streaming(
                "CHANNEL_CONTROL_TURN_ONE: reply only when useful",
                Vec::new(),
                &mut |_| {},
            )
            .expect("submit first channel turn");
        runtime
            .handler()
            .join_active_tui_turn()
            .expect("join first channel turn");

        let mut durable_session = runtime
            .handler()
            .agent_session_snapshot()
            .expect("first snapshot")
            .expect("attached first snapshot");
        let second_visible = sinomo_agent::AgentItem::UserMessage {
            item_id: TaskId::generate(),
            step_index: 1,
            content: "User: second visible channel message".to_string(),
            event_id: None,
        };
        durable_session.transcript.push(second_visible.clone());
        durable_session.raw_transcript.push(second_visible);
        runtime
            .handler_mut()
            .replace_full_agent_session(&session, durable_session)
            .expect("attach second channel snapshot");
        runtime
            .ingest_transient_system_context_with_attachments_streaming(
                "CHANNEL_CONTROL_TURN_TWO: stay concise",
                Vec::new(),
                &mut |_| {},
            )
            .expect("submit second channel turn");
        runtime
            .handler()
            .join_active_tui_turn()
            .expect("join second channel turn");

        let persisted = FileAgentSessionStore::for_data_dir(&data_dir)
            .load_session(&session.session_id)
            .expect("load persisted session")
            .expect("persisted session");
        for transcript in [&persisted.transcript, &persisted.raw_transcript] {
            let visible_messages = transcript
                .items
                .iter()
                .filter_map(|item| match item {
                    sinomo_agent::AgentItem::UserMessage { content, .. } => Some(content.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(
                visible_messages,
                vec![
                    "User: first visible channel message",
                    "User: second visible channel message"
                ]
            );
            assert!(transcript.items.iter().all(|item| {
                !serde_json::to_string(item)
                    .expect("serialize transcript item")
                    .contains("CHANNEL_CONTROL_TURN_")
            }));
        }
    }

    #[test]
    fn tui_chat_selection_mode_extends_with_keyboard() {
        let mut state = fixture_tui_state();
        state.push_message(TuiMessageKind::User, "one");
        state.push_message(TuiMessageKind::Agent, "two");

        assert_eq!(
            state.apply_key_event(press_mod(KeyCode::Char('s'), KeyModifiers::ALT)),
            TuiInputAction::None
        );
        assert_eq!(state.chat_selection.expect("selection").range(), (2, 2));
        assert_eq!(
            state.apply_key_event(press(KeyCode::Up)),
            TuiInputAction::None
        );
        assert_eq!(state.chat_selection.expect("selection").range(), (1, 2));
        assert_eq!(
            state.apply_key_event(press_mod(KeyCode::Char('a'), KeyModifiers::CONTROL)),
            TuiInputAction::None
        );
        assert_eq!(state.chat_selection.expect("selection").range(), (0, 2));
        assert_eq!(
            state.apply_key_event(press(KeyCode::Esc)),
            TuiInputAction::None
        );
        assert!(state.chat_selection.is_none());
    }

    #[test]
    fn tui_state_tracks_model_usage_without_transcript_message() {
        let mut state = fixture_tui_state();

        state.begin_submission("hello");
        state.apply_output(InteractionOutput::ModelUsage {
            provider: "codex".to_string(),
            model: "gpt-5".to_string(),
            usage: sinomo_lm::LanguageModelUsage {
                input_tokens: Some(1234),
                output_tokens: Some(56),
            },
        });
        state.apply_output(InteractionOutput::Line("sinomo: done".to_string()));
        state.finish_submission();

        assert_eq!(state.messages.len(), 3);
        assert_eq!(
            state.last_model_usage,
            Some(TuiTokenUsage {
                input_tokens: 1234,
                output_tokens: 56,
            })
        );
        assert_eq!(
            state.session_token_usage,
            TuiTokenUsage {
                input_tokens: 1234,
                output_tokens: 56,
            }
        );
        assert_eq!(
            format_tui_context_usage(state.last_model_usage),
            "1.2k in/56 out"
        );
        assert_eq!(
            format_tui_session_token_usage(state.session_token_usage),
            "1.3k tok (1.2k in/56 out)"
        );
    }

    #[test]
    fn tui_state_marks_command_failures_and_exit() {
        let mut state = fixture_tui_state();

        state.begin_submission("/models");
        state.apply_output(InteractionOutput::Line(
            "command failed: missing config".to_string(),
        ));
        state.apply_output(InteractionOutput::Exit);

        assert_eq!(state.messages[1].kind, TuiMessageKind::Command);
        assert_eq!(state.messages[2].kind, TuiMessageKind::Error);
        assert!(state.exit_requested);
        assert_eq!(state.status, "ending session");
    }

    #[test]
    fn tui_state_marks_model_failures_as_errors() {
        let mut state = fixture_tui_state();

        state.begin_submission("hello");
        state.apply_output(InteractionOutput::Line(
            "model failed: backend rejected request".to_string(),
        ));
        state.finish_submission();

        assert_eq!(state.messages[1].kind, TuiMessageKind::User);
        assert_eq!(state.messages[2].kind, TuiMessageKind::Error);
        assert_eq!(
            state.messages[2].text,
            "model failed: backend rejected request"
        );
        assert_eq!(state.status, "listening");
    }

    #[test]
    fn tui_state_updates_completion_list_only_after_typed_edits() {
        let mut state = fixture_tui_state();

        state.insert_char('/');
        let initial_labels = state
            .completions
            .as_ref()
            .expect("completions")
            .items
            .iter()
            .map(|item| item.label.clone())
            .collect::<Vec<_>>();
        assert!(initial_labels.contains(&"cluster".to_string()));
        assert!(initial_labels.contains(&"files".to_string()));

        while state.input != "/cluster " {
            state.select_next_completion();
        }
        let selected_input = state.input.clone();
        let labels_after_selection = state
            .completions
            .as_ref()
            .expect("completions")
            .items
            .iter()
            .map(|item| item.label.clone())
            .collect::<Vec<_>>();
        assert_eq!(labels_after_selection, initial_labels);
        assert!(selected_input.starts_with('/'));
        assert_ne!(selected_input, "/");

        state.accept_completion();
        let labels_after_accept = state
            .completions
            .as_ref()
            .expect("next completions")
            .items
            .iter()
            .map(|item| item.label.clone())
            .collect::<Vec<_>>();
        assert_ne!(labels_after_accept, initial_labels);
    }

    #[test]
    fn tui_state_can_build_option_value_with_completion_selection() {
        let mut state = fixture_tui_state();

        state.insert_str("/dfs pin add --m");
        state.select_next_completion();
        assert_eq!(state.input, "/dfs pin add --mode ");
        state.accept_completion();
        assert!(
            state
                .completions
                .as_ref()
                .expect("value completions")
                .items
                .iter()
                .any(|item| item.label == "metadata")
        );

        state.select_next_completion();
        assert_eq!(state.input, "/dfs pin add --mode metadata ");
    }

    #[test]
    fn tui_state_enter_confirms_completion_and_refreshes_suggestions() {
        let mut state = fixture_tui_state();

        state.insert_char('/');
        while state.input != "/cluster " {
            state.select_next_completion();
        }
        assert!(
            state
                .completions
                .as_ref()
                .expect("top completions")
                .items
                .iter()
                .any(|item| item.label == "files")
        );

        assert_eq!(
            state.apply_key_event(press(KeyCode::Enter)),
            TuiInputAction::None
        );
        let labels = state
            .completions
            .as_ref()
            .expect("cluster completions")
            .items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>();
        assert!(labels.contains(&"role"));
        assert!(labels.contains(&"health"));
        assert!(!labels.contains(&"files"));
    }

    #[test]
    fn tui_state_left_arrow_steps_back_one_slash_completion_level() {
        let mut state = fixture_tui_state();

        state.insert_str("/cluster role ");
        assert_eq!(
            state.apply_key_event(press(KeyCode::Left)),
            TuiInputAction::None
        );
        assert_eq!(state.input, "/cluster ");
        assert!(
            state
                .completions
                .as_ref()
                .expect("cluster completions")
                .items
                .iter()
                .any(|item| item.label == "role")
        );

        assert_eq!(
            state.apply_key_event(press(KeyCode::Left)),
            TuiInputAction::None
        );
        assert_eq!(state.input, "/");
        assert!(
            state
                .completions
                .as_ref()
                .expect("top completions")
                .items
                .iter()
                .any(|item| item.label == "cluster")
        );
    }

    #[test]
    fn completion_visible_range_keeps_selection_in_view() {
        assert_eq!(completion_visible_range(20, None, 6), (0, 6));
        assert_eq!(completion_visible_range(20, Some(0), 6), (0, 6));
        assert_eq!(completion_visible_range(20, Some(7), 6), (4, 10));
        assert_eq!(completion_visible_range(20, Some(19), 6), (14, 20));
        assert_eq!(completion_visible_range(4, Some(3), 6), (0, 4));
    }

    #[test]
    fn tui_menu_tabs_mark_current_view() {
        let chat = tui_menu_tabs(&TuiViewMode::Chat);
        assert_eq!(chat[0].label, " Chat (F1) ");
        assert!(chat[0].active);
        assert!(!chat[1].active);
        assert!(!chat[2].active);
        assert!(!chat[3].active);

        let files = tui_menu_tabs(&TuiViewMode::Files);
        assert!(!files[0].active);
        assert!(files[1].active);
        assert!(!files[2].active);
        assert!(!files[3].active);

        let sessions = tui_menu_tabs(&TuiViewMode::Sessions);
        assert!(!sessions[0].active);
        assert!(!sessions[1].active);
        assert!(sessions[2].active);
        assert!(!sessions[3].active);

        let settings = tui_menu_tabs(&TuiViewMode::Settings);
        assert!(!settings[0].active);
        assert!(!settings[1].active);
        assert!(!settings[2].active);
        assert!(settings[3].active);
    }

    #[test]
    fn tui_state_f1_returns_to_chat_tab() {
        let root = temp_file_browser_root("f1");
        let mut state = fixture_tui_state();

        state.open_file_browser_at(root.to_path_buf());
        assert_eq!(state.view_mode, TuiViewMode::Files);
        assert_eq!(
            state.apply_key_event(press(KeyCode::F(1))),
            TuiInputAction::None
        );
        assert_eq!(state.view_mode, TuiViewMode::Chat);

        state.open_session_list(Vec::new());
        assert_eq!(state.view_mode, TuiViewMode::Sessions);
        assert_eq!(
            state.apply_key_event(press(KeyCode::F(1))),
            TuiInputAction::None
        );
        assert_eq!(state.view_mode, TuiViewMode::Chat);

        state.open_settings(TuiSettingsState {
            entries: Vec::new(),
            selected: 0,
            editing_key: None,
        });
        assert_eq!(state.view_mode, TuiViewMode::Settings);
        assert_eq!(
            state.apply_key_event(press(KeyCode::F(1))),
            TuiInputAction::None
        );
        assert_eq!(state.view_mode, TuiViewMode::Chat);
    }

    #[test]
    fn tui_state_f2_opens_sifs_files_by_default() {
        let mut state = fixture_tui_state();

        assert_eq!(
            state.apply_key_event(press(KeyCode::F(2))),
            TuiInputAction::OpenFiles(TuiFilesTarget::Sifs("/".to_string()))
        );
    }

    #[test]
    fn tui_state_f4_opens_settings() {
        let mut state = fixture_tui_state();

        assert_eq!(
            state.apply_key_event(press(KeyCode::F(4))),
            TuiInputAction::OpenSettings
        );
    }

    #[test]
    fn tui_settings_loads_shared_config_catalog() {
        let root = temp_file_browser_root("settings-catalog");
        let data_dir = SinomoDataDir::new(&root);

        let settings = TuiSettingsState::load(&data_dir).expect("settings");
        let keys = settings
            .entries
            .iter()
            .map(TuiSettingsEntry::key)
            .collect::<Vec<_>>();

        assert!(keys.contains(&"rollout.telemetry.gates"));
        assert!(keys.contains(&"lm.providers"));
        assert!(
            !keys.iter().any(|key| key.starts_with("lm.credentials.")),
            "provider credentials are account data in lm.providers, not catalog entries"
        );
    }

    #[test]
    fn tui_mouse_menu_clicks_switch_views() {
        let root = temp_file_browser_root("mouse-menu");
        let area = Rect::new(0, 0, 100, 24);
        let mut state = fixture_tui_state();
        let menu = tui_layout_areas(area, &state).menu;

        assert_eq!(tui_menu_tab_at(menu, 1, 0), Some(TuiViewMode::Chat));
        assert_eq!(tui_menu_tab_at(menu, 13, 0), Some(TuiViewMode::Files));
        assert_eq!(tui_menu_tab_at(menu, 26, 0), Some(TuiViewMode::Sessions));
        assert_eq!(tui_menu_tab_at(menu, 42, 0), Some(TuiViewMode::Settings));

        assert_eq!(
            state.apply_mouse_event(mouse(MouseEventKind::Down(MouseButton::Left), 13, 0), area),
            TuiInputAction::OpenFiles(TuiFilesTarget::Sifs("/".to_string()))
        );

        assert_eq!(
            state.apply_mouse_event(mouse(MouseEventKind::Down(MouseButton::Left), 26, 0), area),
            TuiInputAction::OpenSessions
        );

        assert_eq!(
            state.apply_mouse_event(mouse(MouseEventKind::Down(MouseButton::Left), 42, 0), area),
            TuiInputAction::OpenSettings
        );

        state.open_file_browser_at(root.to_path_buf());
        assert_eq!(state.view_mode, TuiViewMode::Files);
        assert_eq!(
            state.apply_mouse_event(mouse(MouseEventKind::Down(MouseButton::Left), 1, 0), area),
            TuiInputAction::None
        );
        assert_eq!(state.view_mode, TuiViewMode::Chat);
    }

    #[test]
    fn tui_mouse_selects_completion_file_and_session_rows() {
        let root = temp_file_browser_root("mouse-select");
        fs::write(root.join("a.txt"), "alpha").expect("file a");
        fs::write(root.join("b.txt"), "beta").expect("file b");
        let area = Rect::new(0, 0, 100, 24);
        let mut state = fixture_tui_state();

        state.insert_char('/');
        let completions_area = tui_layout_areas(area, &state)
            .completions
            .expect("completions");
        let completions_inner = tui_inner_rect(completions_area);
        assert_eq!(
            state.apply_mouse_event(
                mouse(
                    MouseEventKind::Down(MouseButton::Left),
                    completions_inner.x,
                    completions_inner.y,
                ),
                area,
            ),
            TuiInputAction::None
        );
        assert_eq!(
            state.completions.as_ref().and_then(|list| list.selected),
            Some(0)
        );

        state.open_file_browser_at(root.to_path_buf());
        let layout = tui_layout_areas(area, &state);
        let (entries_area, _) = tui_file_browser_areas(layout.main);
        let entries_inner = tui_inner_rect(entries_area);
        assert_eq!(
            state.apply_mouse_event(
                mouse(
                    MouseEventKind::Down(MouseButton::Left),
                    entries_inner.x,
                    entries_inner.y.saturating_add(1),
                ),
                area,
            ),
            TuiInputAction::None
        );
        assert_eq!(state.file_browser.as_ref().expect("browser").selected, 1);

        let session_a = SessionId::generate();
        let session_b = SessionId::generate();
        state.open_session_list(vec![
            session_record(session_a, "first"),
            session_record(session_b.clone(), "second"),
        ]);
        let sessions_inner = tui_inner_rect(tui_layout_areas(area, &state).main);
        assert_eq!(
            state.apply_mouse_event(
                mouse(
                    MouseEventKind::Down(MouseButton::Left),
                    sessions_inner.x,
                    sessions_inner.y.saturating_add(1),
                ),
                area,
            ),
            TuiInputAction::None
        );
        assert_eq!(
            state
                .session_list
                .as_ref()
                .expect("sessions")
                .selected_session_id(),
            Some(session_b)
        );
    }

    #[test]
    fn tui_mouse_wheel_scrolls_active_panel() {
        let root = temp_file_browser_root("mouse-scroll");
        fs::write(root.join("a.txt"), "alpha").expect("file a");
        fs::write(root.join("b.txt"), "beta").expect("file b");
        let area = Rect::new(0, 0, 100, 24);
        let mut state = fixture_tui_state();

        for index in 0..30 {
            state.push_message(TuiMessageKind::Agent, format!("line {index}"));
        }
        let transcript = tui_layout_areas(area, &state).main;
        assert_eq!(
            state.apply_mouse_event(
                mouse(
                    MouseEventKind::ScrollUp,
                    transcript.x.saturating_add(1),
                    transcript.y.saturating_add(1),
                ),
                area,
            ),
            TuiInputAction::None
        );
        assert_eq!(state.scrollback, TUI_MOUSE_SCROLL_LINES as u16);
        state.apply_mouse_event(
            mouse(
                MouseEventKind::ScrollDown,
                transcript.x.saturating_add(1),
                transcript.y.saturating_add(1),
            ),
            area,
        );
        assert_eq!(state.scrollback, 0);

        state.open_file_browser_at(root.to_path_buf());
        let (entries_area, _) = tui_file_browser_areas(tui_layout_areas(area, &state).main);
        let entries_inner = tui_inner_rect(entries_area);
        state.apply_mouse_event(
            mouse(MouseEventKind::ScrollDown, entries_inner.x, entries_inner.y),
            area,
        );
        assert_eq!(state.file_browser.as_ref().expect("browser").selected, 2);

        state.open_session_list(vec![
            session_record(SessionId::generate(), "first"),
            session_record(SessionId::generate(), "second"),
            session_record(SessionId::generate(), "third"),
        ]);
        let sessions_inner = tui_inner_rect(tui_layout_areas(area, &state).main);
        state.apply_mouse_event(
            mouse(
                MouseEventKind::ScrollDown,
                sessions_inner.x,
                sessions_inner.y,
            ),
            area,
        );
        assert_eq!(state.session_list.as_ref().expect("sessions").selected, 2);
    }

    #[test]
    fn tui_mouse_selects_chat_transcript_lines() {
        let area = Rect::new(0, 0, 100, 24);
        let mut state = fixture_tui_state();
        state.push_message(TuiMessageKind::User, "first");
        state.push_message(TuiMessageKind::Agent, "second");

        let layout = tui_layout_areas(area, &state);
        let (_, transcript_area) = tui_chat_areas(layout.main);
        let transcript_inner = tui_inner_rect(transcript_area);
        assert_eq!(
            state.apply_mouse_event(
                mouse(
                    MouseEventKind::Down(MouseButton::Left),
                    transcript_inner.x,
                    transcript_inner.y.saturating_add(1),
                ),
                area,
            ),
            TuiInputAction::None
        );
        let selection = state.chat_selection.expect("selection");
        assert_eq!(selection.range(), (1, 1));
        assert!(selection.dragging);

        assert_eq!(
            state.apply_mouse_event(
                mouse(
                    MouseEventKind::Drag(MouseButton::Left),
                    transcript_inner.x,
                    transcript_inner.y.saturating_add(2),
                ),
                area,
            ),
            TuiInputAction::None
        );
        assert_eq!(state.chat_selection.expect("selection").range(), (1, 2));

        assert_eq!(
            state.apply_mouse_event(
                mouse(
                    MouseEventKind::Up(MouseButton::Left),
                    transcript_inner.x,
                    transcript_inner.y.saturating_add(2),
                ),
                area,
            ),
            TuiInputAction::None
        );
        assert!(!state.chat_selection.expect("selection").dragging);
    }

    #[test]
    fn file_browser_loads_directory_and_previews_text_file() {
        let root = temp_file_browser_root("preview");
        fs::create_dir(root.join("src")).expect("dir");
        fs::write(root.join("note.txt"), "hello\nworld\n").expect("file");

        let mut browser = FileManagerState::open_local(&root).expect("browser");
        let parent_index = browser
            .entries
            .iter()
            .position(|entry| entry.kind == FileManagerEntryKind::Parent)
            .expect("parent");
        let dir_index = browser
            .entries
            .iter()
            .position(|entry| entry.name == "src")
            .expect("dir");
        let file_index = browser
            .entries
            .iter()
            .position(|entry| entry.name == "note.txt")
            .expect("file");
        assert!(parent_index < dir_index);
        assert!(dir_index < file_index);

        browser.selected = file_index;
        browser.update_preview();
        assert!(browser.preview.lines.iter().any(|line| line == "hello"));
        assert!(browser.preview.lines.iter().any(|line| line == "world"));
    }

    #[test]
    fn file_browser_previews_sobj_file() {
        let root = temp_file_browser_root("sobj-preview");
        let document = sinomo_runtime::object::Document::dynamic(
            1,
            sinomo_runtime::object::LengthMode::U32,
            sinomo_runtime::object::Value::Object(sinomo_runtime::object::Object::new(vec![
                sinomo_runtime::object::Field::new(
                    "name",
                    sinomo_runtime::object::Value::String("demo".into()),
                ),
                sinomo_runtime::object::Field::new(
                    "enabled",
                    sinomo_runtime::object::Value::Boolean(true),
                ),
                sinomo_runtime::object::Field::new(
                    "payload",
                    sinomo_runtime::object::Value::Bytes(vec![0, 1, 2, 3, 4]),
                ),
            ])),
        );
        fs::write(
            root.join("config.sobj"),
            document.to_bytes().expect("document bytes"),
        )
        .expect("file");

        let mut browser =
            with_tui_file_previewers(FileManagerState::open_local(&root).expect("browser"));
        let file_index = browser
            .entries
            .iter()
            .position(|entry| entry.name == "config.sobj")
            .expect("file");
        browser.selected = file_index;
        browser.update_preview();

        assert_eq!(browser.preview.title, "SOBJ config.sobj");
        assert!(
            browser
                .preview
                .lines
                .iter()
                .any(|line| line == "SOBJ document")
        );
        assert!(
            browser
                .preview
                .lines
                .iter()
                .any(|line| line.contains("\"name\": string(4 chars): \"demo\""))
        );
        assert!(
            browser
                .preview
                .lines
                .iter()
                .any(|line| line.contains("\"enabled\": true"))
        );
        assert!(
            browser
                .preview
                .lines
                .iter()
                .any(|line| line.contains("\"payload\": bytes(5 B): 00 01 02 03 04"))
        );
    }

    #[test]
    fn file_browser_manages_files_with_guarded_delete() {
        let root = temp_file_browser_root("manage");
        let mut browser = FileManagerState::open_local(&root).expect("browser");

        browser.create_file("alpha.txt").expect("create file");
        assert!(root.join("alpha.txt").is_file());
        browser.rename_selected("beta.txt").expect("rename file");
        assert!(!root.join("alpha.txt").exists());
        assert!(root.join("beta.txt").is_file());
        assert!(browser.delete_selected("no").is_err());
        assert!(root.join("beta.txt").exists());
        browser.delete_selected("DELETE").expect("delete file");
        assert!(!root.join("beta.txt").exists());

        browser.create_dir("docs").expect("create dir");
        assert!(root.join("docs").is_dir());
        assert!(browser.create_file("../escape.txt").is_err());
    }

    #[test]
    fn tui_state_file_browser_prompt_creates_file_and_closes() {
        let root = temp_file_browser_root("state");
        let mut state = fixture_tui_state();

        state.open_file_browser_at(root.to_path_buf());
        assert_eq!(state.view_mode, TuiViewMode::Files);
        assert!(state.completions.is_none());

        assert_eq!(
            state.apply_key_event(press(KeyCode::Char('n'))),
            TuiInputAction::None
        );
        assert_eq!(state.file_prompt, Some(FileManagerPrompt::NewFile));
        state.insert_str("made.txt");
        assert_eq!(
            state.apply_key_event(press(KeyCode::Enter)),
            TuiInputAction::None
        );
        assert!(root.join("made.txt").is_file());
        assert!(state.file_prompt.is_none());
        assert!(state.status.contains("created file"));

        assert_eq!(
            state.apply_key_event(press(KeyCode::Esc)),
            TuiInputAction::None
        );
        assert_eq!(state.view_mode, TuiViewMode::Chat);
    }

    #[test]
    fn tui_files_command_accepts_optional_target() {
        assert_eq!(
            parse_tui_slash_command("/files")
                .expect("parse")
                .expect("target"),
            TuiSlashCommand::Files {
                target: TuiFilesTarget::Sifs("/".to_string())
            }
        );
        assert_eq!(
            parse_tui_slash_command("/files \"two words\"")
                .expect("parse")
                .expect("target"),
            TuiSlashCommand::Files {
                target: TuiFilesTarget::Sifs("two words".to_string())
            }
        );
        assert_eq!(
            parse_tui_slash_command("/files \"local:two words\"")
                .expect("parse")
                .expect("target"),
            TuiSlashCommand::Files {
                target: TuiFilesTarget::Local(PathBuf::from("two words"))
            }
        );
        assert_eq!(
            parse_tui_slash_command("/files sifs:/services")
                .expect("parse")
                .expect("target"),
            TuiSlashCommand::Files {
                target: TuiFilesTarget::Sifs("/services".to_string())
            }
        );
        assert_eq!(
            parse_tui_slash_command("/files --sifs /users")
                .expect("parse")
                .expect("target"),
            TuiSlashCommand::Files {
                target: TuiFilesTarget::Sifs("/users".to_string())
            }
        );
        assert!(parse_tui_slash_command("/help").expect("parse").is_none());
        assert!(parse_tui_slash_command("/--local files").is_err());
        assert!(parse_tui_slash_command("/files --local").is_err());
    }

    #[test]
    fn tui_session_and_settings_commands_parse() {
        let session_id = SessionId::generate();
        assert_eq!(
            parse_tui_slash_command("/sessions")
                .expect("parse")
                .expect("sessions"),
            TuiSlashCommand::Sessions
        );
        assert_eq!(
            parse_tui_slash_command("/settings")
                .expect("parse")
                .expect("settings"),
            TuiSlashCommand::Settings
        );
        assert_eq!(
            parse_tui_slash_command(&format!("/resume {session_id}"))
                .expect("parse")
                .expect("resume"),
            TuiSlashCommand::Resume { session_id }
        );
        assert!(parse_tui_slash_command("/resume not-a-session").is_err());
    }

    #[test]
    fn tui_session_store_saves_lists_and_loads_transcript() {
        let root = temp_file_browser_root("sessions");
        let data_dir = SinomoDataDir::new(root.to_path_buf());
        let store = TuiSessionStore::for_data_dir(&data_dir);
        let session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_test").expect("node"),
            InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
        );
        let mut state = TuiState::new(&session, None, TuiAgentRuntimeInfo::default());
        state.workspaces = vec![
            WorkspaceRef::local_path(root.join("workspace-primary")),
            WorkspaceRef::sifs("shared", "/shared/workspace"),
        ];
        state.push_message(TuiMessageKind::User, "continue previous work");
        state.push_message(TuiMessageKind::Agent, "ok");
        state.apply_output(InteractionOutput::ModelUsage {
            provider: "codex".to_string(),
            model: "gpt-5".to_string(),
            usage: sinomo_lm::LanguageModelUsage {
                input_tokens: Some(11),
                output_tokens: Some(7),
            },
        });

        store.save_state(&session, &state).expect("save");
        let sessions = store.list_sessions().expect("list");

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session.session_id, session.session_id);
        assert_eq!(sessions[0].title, "continue previous work");
        assert_eq!(sessions[0].workspaces, state.workspaces);
        let transcript = store
            .load_transcript(&session.session_id)
            .expect("load")
            .expect("transcript");
        assert_eq!(transcript.messages, state.messages);
        assert_eq!(transcript.last_model_usage, state.last_model_usage);
        assert_eq!(transcript.session_token_usage, state.session_token_usage);
    }

    #[test]
    fn tui_session_store_discards_empty_sessions() {
        let root = temp_file_browser_root("empty-sessions");
        let data_dir = SinomoDataDir::new(root.to_path_buf());
        let store = TuiSessionStore::for_data_dir(&data_dir);
        let session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_empty").expect("node"),
            InteractionSurface::new("surface.empty", InteractionSurfaceKind::CommandLine),
        );
        let state = TuiState::new(&session, None, TuiAgentRuntimeInfo::default());

        store.save_state(&session, &state).expect("save");

        assert!(!store.session_dir(&session.session_id).exists());
        assert!(store.list_sessions().expect("list").is_empty());
    }

    #[test]
    fn tui_session_store_discards_command_only_sessions() {
        let root = temp_file_browser_root("command-only-sessions");
        let data_dir = SinomoDataDir::new(root.to_path_buf());
        let store = TuiSessionStore::for_data_dir(&data_dir);
        let session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_command_only").expect("node"),
            InteractionSurface::new("surface.command-only", InteractionSurfaceKind::CommandLine),
        );
        let mut state = TuiState::new(&session, None, TuiAgentRuntimeInfo::default());
        state.begin_submission("/quit");
        state.apply_output(InteractionOutput::Line("ending session".to_string()));
        state.apply_output(InteractionOutput::Exit);
        state.finish_submission();

        store.save_state(&session, &state).expect("save");

        assert!(!store.session_dir(&session.session_id).exists());
        assert!(store.list_sessions().expect("list").is_empty());
    }

    #[test]
    fn tui_session_list_selects_and_resumes_with_enter() {
        let session_a = SessionId::generate();
        let session_b = SessionId::generate();
        let mut state = fixture_tui_state();
        state.open_session_list(vec![
            TuiSessionRecord {
                schema_version: TUI_SESSION_SCHEMA_VERSION,
                session: InteractionSession::new(
                    session_a.clone(),
                    NodeId::from_legacy_named("node_a").expect("node"),
                    InteractionSurface::new("surface.a", InteractionSurfaceKind::CommandLine),
                ),
                workspaces: Vec::new(),
                workspace_projection_version: TUI_WORKSPACE_PROJECTION_VERSION,
                created_at: OffsetDateTime::now_utc(),
                updated_at: OffsetDateTime::now_utc(),
                title: "first".to_string(),
                message_count: 1,
                ended: false,
                runtime_info: TuiAgentRuntimeInfo::default(),
            },
            TuiSessionRecord {
                schema_version: TUI_SESSION_SCHEMA_VERSION,
                session: InteractionSession::new(
                    session_b.clone(),
                    NodeId::from_legacy_named("node_b").expect("node"),
                    InteractionSurface::new("surface.b", InteractionSurfaceKind::CommandLine),
                ),
                workspaces: Vec::new(),
                workspace_projection_version: TUI_WORKSPACE_PROJECTION_VERSION,
                created_at: OffsetDateTime::now_utc(),
                updated_at: OffsetDateTime::now_utc(),
                title: "second".to_string(),
                message_count: 1,
                ended: false,
                runtime_info: TuiAgentRuntimeInfo::default(),
            },
        ]);

        assert_eq!(
            state.apply_key_event(press(KeyCode::Down)),
            TuiInputAction::None
        );
        assert_eq!(
            state.apply_key_event(press(KeyCode::Enter)),
            TuiInputAction::Resume(session_b)
        );
    }

    #[test]
    fn tui_model_command_accepts_status_and_switch_forms() {
        assert_eq!(
            parse_tui_slash_command("/model")
                .expect("parse")
                .expect("status"),
            TuiSlashCommand::Model(TuiModelCommand::Status)
        );
        assert_eq!(
            parse_tui_slash_command("/model status")
                .expect("parse")
                .expect("status"),
            TuiSlashCommand::Model(TuiModelCommand::Status)
        );
        assert_eq!(
            parse_tui_slash_command("/model use codex gpt-5 --account work")
                .expect("parse")
                .expect("selection"),
            TuiSlashCommand::Model(TuiModelCommand::Use(TuiModelSelection {
                provider: "codex".to_string(),
                model: "gpt-5".to_string(),
                account: "work".to_string(),
            }))
        );
        assert_eq!(
            parse_tui_slash_command("/model --provider openai --model gpt-4.1")
                .expect("parse")
                .expect("selection"),
            TuiSlashCommand::Model(TuiModelCommand::Use(TuiModelSelection {
                provider: "openai".to_string(),
                model: "gpt-4.1".to_string(),
                account: "default".to_string(),
            }))
        );
        assert_eq!(
            parse_tui_slash_command("/model use MiniCPM openbmb/MiniCPM-V-4_5")
                .expect("parse")
                .expect("selection"),
            TuiSlashCommand::Model(TuiModelCommand::Use(TuiModelSelection {
                provider: MINICPM_PROVIDER_ID.to_string(),
                model: "openbmb/MiniCPM-V-4_5".to_string(),
                account: "default".to_string(),
            }))
        );
        assert!(parse_tui_slash_command("/model --provider --model gpt-5").is_err());
    }

    #[test]
    fn tui_tool_runtime_command_accepts_status_local_and_node_forms() {
        assert_eq!(
            parse_tui_slash_command("/tool-runtime")
                .expect("parse")
                .expect("status"),
            TuiSlashCommand::ToolRuntime(TuiToolRuntimeCommand::Status)
        );
        assert_eq!(
            parse_tui_slash_command("/tool-runtime local")
                .expect("parse")
                .expect("local"),
            TuiSlashCommand::ToolRuntime(TuiToolRuntimeCommand::Local)
        );
        assert_eq!(
            parse_tui_slash_command("/tool-runtime node worker-a")
                .expect("parse")
                .expect("node"),
            TuiSlashCommand::ToolRuntime(TuiToolRuntimeCommand::Node("worker-a".to_string()))
        );
        assert!(parse_tui_slash_command("/tool-runtime node").is_err());
    }

    #[test]
    fn tui_agent_command_accepts_status_list_and_switch_forms() {
        assert_eq!(
            parse_tui_slash_command("/agent")
                .expect("parse")
                .expect("status"),
            TuiSlashCommand::Agent(TuiAgentCommand::Status)
        );
        assert_eq!(
            parse_tui_slash_command("/agent list")
                .expect("parse")
                .expect("list"),
            TuiSlashCommand::Agent(TuiAgentCommand::List)
        );
        assert_eq!(
            parse_tui_slash_command("/agent use coding-agent")
                .expect("parse")
                .expect("use"),
            TuiSlashCommand::Agent(TuiAgentCommand::Use("coding-agent".to_string()))
        );
        assert_eq!(
            parse_tui_slash_command("/agent background-agent")
                .expect("parse")
                .expect("quick use"),
            TuiSlashCommand::Agent(TuiAgentCommand::Use("background-agent".to_string()))
        );
        assert!(parse_tui_slash_command("/agent use").is_err());
        assert!(parse_tui_slash_command("/agent use coding-agent extra").is_err());
    }

    #[test]
    fn tui_model_command_rejects_malformed_quick_switches() {
        assert!(parse_tui_slash_command("/model list").is_err());
        assert!(parse_tui_slash_command("/model runtime").is_err());
        assert!(parse_tui_slash_command("/model default --provider codex --model gpt-5").is_err());
        assert!(
            parse_tui_slash_command("/models list")
                .expect("parse")
                .is_none()
        );
    }

    #[test]
    fn tui_model_switch_replaces_active_handler_model() {
        let root = temp_file_browser_root("model-switch");
        let data_dir = SinomoDataDir::new(root.to_path_buf());
        let mut handler = RuntimeMainAgentInteractionHandler::from_runtime_config(
            &data_dir,
            ModelRuntimeConfig::default(),
        )
        .expect("handler");

        let info = handler
            .switch_tui_model(
                &data_dir,
                TuiModelSelection {
                    provider: "codex".to_string(),
                    model: "gpt-5".to_string(),
                    account: "default".to_string(),
                },
                false,
            )
            .expect("switch");

        assert_eq!(info.agent, "main-agent");
        assert_eq!(info.provider, "codex");
        assert_eq!(info.model, "gpt-5");
        assert_eq!(info.account.as_deref(), Some("default"));
        assert_eq!(handler.tui_agent_runtime_info(), info);
    }

    #[test]
    fn tui_agent_switch_applies_package_model_preference() {
        let root = temp_file_browser_root("agent-model-policy-switch");
        let data_dir = SinomoDataDir::new(root.to_path_buf());
        let mut handler = RuntimeMainAgentInteractionHandler::from_runtime_config(
            &data_dir,
            ModelRuntimeConfig::default(),
        )
        .expect("handler");
        let mut definition =
            AgentDefinition::custom("review-agent", AgentRole::Deliberation, "Review changes.");
        definition.model_policy.provider = Some("codex".to_string());
        definition.model_policy.model = Some("gpt-5".to_string());

        let info = handler
            .switch_tui_agent(&data_dir, false, definition)
            .expect("switch agent and model");

        assert_eq!(info.agent, "review-agent");
        assert_eq!(info.provider, "codex");
        assert_eq!(info.model, "gpt-5");
    }

    #[test]
    fn daemonless_tui_and_app_runtime_reject_agents_that_require_packaged_skills() {
        let root = temp_file_browser_root("daemonless-agent-skill-catalog");
        let data_dir = SinomoDataDir::new(root.to_path_buf());
        let definition = AgentDefinition::custom(
            "review-agent",
            AgentRole::Deliberation,
            "Review changes with the resident skill package.",
        )
        .with_skill("review.skill");

        let mut tui_handler = RuntimeMainAgentInteractionHandler::from_runtime_config(
            &data_dir,
            ModelRuntimeConfig::default(),
        )
        .expect("TUI handler");
        let tui_error = tui_handler
            .switch_tui_agent(&data_dir, false, definition.clone())
            .expect_err("daemonless TUI must reject packaged skill requirements");
        assert!(
            tui_error
                .to_string()
                .contains("resident daemon SIFS skill catalog")
        );
        assert_eq!(tui_handler.tui_agent_runtime_info().agent, "main-agent");

        let mut app_handler = RuntimeMainAgentInteractionHandler::from_runtime_config(
            &data_dir,
            ModelRuntimeConfig::default(),
        )
        .expect("app handler");
        let app_error = match app_switch_agent(&mut app_handler, &data_dir, definition, false) {
            Ok(_) => panic!("daemonless app must reject packaged skill requirements"),
            Err(error) => error,
        };
        assert!(
            app_error
                .to_string()
                .contains("resident daemon SIFS skill catalog")
        );
        assert_eq!(app_handler.tui_agent_runtime_info().agent, "main-agent");
    }

    #[test]
    fn tui_agent_switch_rejects_roles_without_interactive_execution() {
        let root = temp_file_browser_root("agent-switch");
        let data_dir = SinomoDataDir::new(root.to_path_buf());
        let mut handler = RuntimeMainAgentInteractionHandler::from_runtime_config(
            &data_dir,
            ModelRuntimeConfig::default(),
        )
        .expect("handler");
        let definition =
            AgentDefinition::custom("ops-agent", AgentRole::Background, "Handle ops status.");

        let error = handler
            .switch_tui_agent(&data_dir, false, definition)
            .expect_err("background roles must use the background executor");

        assert!(
            error
                .to_string()
                .contains("not available on interactive chat surfaces")
        );
        assert_eq!(handler.tui_agent_runtime_info().agent, "main-agent");
    }

    #[test]
    fn tui_and_app_agent_switches_atomically_rebind_recall_tool_authority() {
        let root = temp_file_browser_root("agent-switch-recall-authority");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let mut handler = RuntimeMainAgentInteractionHandler::from_runtime_config(
            &data_dir,
            ModelRuntimeConfig::default(),
        )
        .expect("handler");
        let session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_tui_recall_rebind").expect("node"),
            InteractionSurface::new(
                "surface.tui-recall-rebind",
                InteractionSurfaceKind::CommandLine,
            ),
        );
        let path = "switch/authority.md";

        let main_write = execute_tui_workspace_tool(
            &handler,
            &session,
            0,
            "memory_write",
            serde_json::json!({ "path": path, "markdown": "main authority" }),
        );
        assert_eq!(main_write.status, sinomo_agent::AgentToolStatus::Succeeded);

        handler
            .switch_tui_agent(&data_dir, false, AgentDefinition::coding())
            .expect("switch to coding agent");
        let coding_write = execute_tui_workspace_tool(
            &handler,
            &session,
            1,
            "memory_write",
            serde_json::json!({ "path": path, "markdown": "coding authority" }),
        );
        assert_eq!(
            coding_write.status,
            sinomo_agent::AgentToolStatus::Succeeded,
            "a stale main-agent backend would report note_already_exists: {coding_write:?}"
        );
        let coding_read = execute_tui_workspace_tool(
            &handler,
            &session,
            2,
            "memory_read",
            serde_json::json!({ "path": path }),
        );
        assert_eq!(coding_read.output["markdown"], "coding authority");

        handler
            .switch_tui_agent(&data_dir, false, AgentDefinition::main())
            .expect("switch back to main agent");
        let main_read = execute_tui_workspace_tool(
            &handler,
            &session,
            3,
            "memory_read",
            serde_json::json!({ "path": path }),
        );
        assert_eq!(main_read.output["markdown"], "main authority");
    }

    #[test]
    fn tui_agent_switch_persists_identity_and_generic_workspace_immediately() {
        let root = temp_file_browser_root("durable-agent-switch");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let workspace = root.join("plain-workspace");
        let stale_process_workspace = root.join("stale-process-workspace");
        fs::create_dir_all(&workspace).expect("create generic workspace");
        fs::create_dir_all(&stale_process_workspace).expect("create stale process workspace");
        let workspace = fs::canonicalize(workspace).expect("canonical generic workspace");
        let mut handler = RuntimeMainAgentInteractionHandler::from_runtime_config(
            &data_dir,
            ModelRuntimeConfig::default(),
        )
        .expect("handler");
        handler.workspace_root = Some(
            fs::canonicalize(stale_process_workspace).expect("canonical stale process workspace"),
        );
        let session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_tui_durable_switch").expect("node"),
            InteractionSurface::new(
                "surface.tui-durable-switch",
                InteractionSurfaceKind::CommandLine,
            ),
        );
        let recall_path = "switch/session-authority.md";
        let main_write = execute_tui_workspace_tool(
            &handler,
            &session,
            0,
            "memory_write",
            serde_json::json!({
                "path": recall_path,
                "markdown": "main session authority",
            }),
        );
        assert_eq!(main_write.status, sinomo_agent::AgentToolStatus::Succeeded);

        let info = handler
            .switch_tui_agent_for_session(
                &data_dir,
                &session,
                Some(&[WorkspaceRef::local_path(workspace.clone())]),
                false,
                AgentDefinition::coding(),
            )
            .expect("switch coding agent");

        assert_eq!(info.agent, "coding-agent");
        let persisted = FileAgentSessionStore::for_data_dir(&data_dir)
            .load_session(&session.session_id)
            .expect("load canonical session")
            .expect("canonical session");
        assert_eq!(
            persisted.agent_definition_key.as_deref(),
            Some("coding-agent")
        );
        assert_eq!(
            persisted
                .workspace_binding
                .as_ref()
                .and_then(|binding| binding.local_root.as_deref()),
            Some(workspace.as_path())
        );
        assert_eq!(
            handler
                .agent_session_snapshot()
                .expect("live snapshot")
                .expect("live session"),
            persisted
        );
        let coding_write = execute_tui_workspace_tool(
            &handler,
            &session,
            1,
            "memory_write",
            serde_json::json!({
                "path": recall_path,
                "markdown": "coding session authority",
            }),
        );
        assert_eq!(
            coding_write.status,
            sinomo_agent::AgentToolStatus::Succeeded,
            "session-aware switch retained the previous principal: {coding_write:?}"
        );
    }

    #[test]
    fn tui_agent_switch_rolls_back_definition_when_runtime_selection_fails() {
        let root = temp_file_browser_root("agent-switch-rollback");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let mut handler = RuntimeMainAgentInteractionHandler::from_runtime_config(
            &data_dir,
            ModelRuntimeConfig::default(),
        )
        .expect("handler");
        let session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_tui_switch_rollback").expect("node"),
            InteractionSurface::new(
                "surface.tui-switch-rollback",
                InteractionSurfaceKind::CommandLine,
            ),
        );
        handler
            .replace_agent_session(&session, sinomo_agent::AgentTranscript::new())
            .expect("attach main session");
        let runtime_config_path =
            write_model_runtime_config_for_node(&data_dir, &ModelRuntimeConfig::default())
                .expect("write valid runtime config");
        fs::write(&runtime_config_path, b"{not valid json")
            .expect("corrupt runtime config after handler creation");

        handler
            .switch_tui_agent(&data_dir, false, AgentDefinition::coding())
            .expect_err("malformed runtime config must reject switch");

        assert_eq!(handler.tui_agent_runtime_info().agent, "main-agent");
        assert_eq!(
            handler
                .agent_session_snapshot()
                .expect("live snapshot")
                .expect("live session")
                .agent_definition_key
                .as_deref(),
            Some("main-agent")
        );
    }

    #[test]
    fn tui_resume_prefers_canonical_agent_identity_and_conversation_workspaces() {
        let root = temp_file_browser_root("canonical-agent-resume");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let conversation_workspace = root.join("conversation-workspace");
        let stale_session_workspace = root.join("stale-session-workspace");
        fs::create_dir_all(&conversation_workspace).expect("create conversation workspace");
        fs::create_dir_all(&stale_session_workspace).expect("create stale session workspace");
        let conversation_workspace =
            fs::canonicalize(conversation_workspace).expect("canonical conversation workspace");
        let stale_session_workspace =
            fs::canonicalize(stale_session_workspace).expect("canonical stale session workspace");
        let session_store = TuiSessionStore::for_data_dir(&data_dir);
        let target_session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_tui_canonical_resume").expect("node"),
            InteractionSurface::new(
                "surface.tui-canonical-resume",
                InteractionSurfaceKind::CommandLine,
            ),
        );
        let mut target_state = TuiState::new(&target_session, None, TuiAgentRuntimeInfo::default());
        target_state.workspaces = vec![WorkspaceRef::local_path(conversation_workspace.clone())];
        target_state.push_message(TuiMessageKind::User, "resume this coding session");
        session_store
            .save_state(&target_session, &target_state)
            .expect("save stale TUI metadata");
        let mut canonical =
            AgentSession::new(target_session.session_id.clone(), AgentRole::Deliberation)
                .with_agent_definition_key("coding-agent");
        canonical.workspace_binding = Some(WorkspaceRef::local_path(stale_session_workspace));
        FileAgentSessionStore::for_data_dir(&data_dir)
            .save_session(&canonical)
            .expect("save canonical coding session");

        let handler = RuntimeMainAgentInteractionHandler::from_runtime_config(
            &data_dir,
            ModelRuntimeConfig::default(),
        )
        .expect("handler");
        let recall_path = "resume/authority.md";
        let main_recall_write = execute_tui_workspace_tool(
            &handler,
            &target_session,
            0,
            "memory_write",
            serde_json::json!({
                "path": recall_path,
                "markdown": "main session memory",
            }),
        );
        assert_eq!(
            main_recall_write.status,
            sinomo_agent::AgentToolStatus::Succeeded
        );
        let mut runtime =
            new_tui_interaction_runtime(handler, None, &session_store).expect("runtime");
        let mut state = TuiState::new(
            runtime.session(),
            None,
            runtime.handler.tui_agent_runtime_info(),
        );

        resume_tui_session(
            &mut runtime,
            &mut state,
            &session_store,
            &data_dir,
            None,
            target_session.session_id.clone(),
        )
        .expect("resume canonical coding session");

        assert_eq!(state.runtime_info.agent, "coding-agent");
        let resumed = runtime
            .handler
            .agent_session_snapshot()
            .expect("live snapshot")
            .expect("live session");
        assert_eq!(
            resumed.agent_definition_key.as_deref(),
            Some("coding-agent")
        );
        assert_eq!(state.workspaces, target_state.workspaces);
        assert_eq!(
            resumed.workspace_binding,
            Some(WorkspaceRef::local_path(conversation_workspace.clone()))
        );
        let coding_recall_write = execute_tui_workspace_tool(
            &runtime.handler,
            &target_session,
            1,
            "memory_write",
            serde_json::json!({
                "path": recall_path,
                "markdown": "coding session memory",
            }),
        );
        assert_eq!(
            coding_recall_write.status,
            sinomo_agent::AgentToolStatus::Succeeded,
            "resume must replace the previous agent's note backend before returning: {coding_recall_write:?}"
        );
        let resumed_interaction = runtime.session().clone();
        runtime
            .handler
            .configure_tui_tool_runtime_for_session(
                &data_dir,
                &resumed_interaction,
                None,
                state.workspace_projection(),
                false,
            )
            .expect("project conversation workspaces into tool runtime");
        assert_eq!(
            runtime.handler.workspace_root.as_deref(),
            Some(conversation_workspace.as_path())
        );
    }

    #[test]
    fn tui_resume_failure_restores_outer_session_recall_authority_and_route() {
        let root = temp_file_browser_root("resume-transaction-rollback");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let session_store = TuiSessionStore::for_data_dir(&data_dir);
        let target_session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_tui_resume_rollback").expect("node"),
            InteractionSurface::new(
                "surface.tui-resume-rollback",
                InteractionSurfaceKind::CommandLine,
            ),
        );
        let mut target_state = TuiState::new(
            &target_session,
            None,
            TuiAgentRuntimeInfo {
                agent: "coding-agent".to_string(),
                ..TuiAgentRuntimeInfo::default()
            },
        );
        target_state.push_message(TuiMessageKind::User, "corrupt this resume after prepare");
        session_store
            .save_state(&target_session, &target_state)
            .expect("save target state");
        FileAgentSessionStore::for_data_dir(&data_dir)
            .save_session(
                &AgentSession::new(target_session.session_id.clone(), AgentRole::Deliberation)
                    .with_agent_definition_key("coding-agent"),
            )
            .expect("save target canonical session");
        fs::write(
            session_store.transcript_path_for_session(&target_session.session_id),
            b"{not valid json",
        )
        .expect("corrupt target transcript");

        let handler = RuntimeMainAgentInteractionHandler::from_runtime_config(
            &data_dir,
            ModelRuntimeConfig::default(),
        )
        .expect("handler");
        let memory_path = "resume/rollback.md";
        let previous_memory = execute_tui_workspace_tool(
            &handler,
            &target_session,
            0,
            "memory_write",
            serde_json::json!({
                "path": memory_path,
                "markdown": "main authority remains attached",
            }),
        );
        assert_eq!(
            previous_memory.status,
            sinomo_agent::AgentToolStatus::Succeeded
        );
        let mut runtime =
            new_tui_interaction_runtime(handler, None, &session_store).expect("runtime");
        let previous_interaction_session = runtime.session().clone();
        let mut state = TuiState::new(
            runtime.session(),
            None,
            runtime.handler.tui_agent_runtime_info(),
        );

        resume_tui_session(
            &mut runtime,
            &mut state,
            &session_store,
            &data_dir,
            None,
            target_session.session_id.clone(),
        )
        .expect_err("corrupt transcript must reject resume");

        assert_eq!(runtime.session(), &previous_interaction_session);
        assert_eq!(runtime.handler.tui_agent_runtime_info().agent, "main-agent");
        let restored = runtime
            .handler
            .agent_session_snapshot()
            .expect("snapshot")
            .expect("attached session");
        assert_eq!(restored.session_id, previous_interaction_session.session_id);
        assert_eq!(restored.agent_definition_key.as_deref(), Some("main-agent"));
        let registry = runtime.handler.runtime_control_registry();
        assert!(
            registry
                .contains_session(&previous_interaction_session.session_id)
                .expect("previous route")
        );
        assert!(
            !registry
                .contains_session(&target_session.session_id)
                .expect("target route")
        );
        let restored_memory = execute_tui_workspace_tool(
            &runtime.handler,
            &previous_interaction_session,
            1,
            "memory_read",
            serde_json::json!({ "path": memory_path }),
        );
        assert_eq!(
            restored_memory.output["markdown"],
            "main authority remains attached"
        );
    }

    #[test]
    fn tui_resume_backfills_legacy_session_workspace_into_conversation_record() {
        let root = temp_file_browser_root("legacy-workspace-resume");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let workspace = root.join("legacy-workspace");
        fs::create_dir_all(&workspace).expect("create legacy workspace");
        let workspace = fs::canonicalize(workspace).expect("canonical legacy workspace");
        let session_store = TuiSessionStore::for_data_dir(&data_dir);
        let target_session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_tui_legacy_workspace_resume").expect("node"),
            InteractionSurface::new(
                "surface.tui-legacy-workspace-resume",
                InteractionSurfaceKind::CommandLine,
            ),
        );
        let mut target_state = TuiState::new(&target_session, None, TuiAgentRuntimeInfo::default());
        target_state.workspaces.clear();
        target_state.push_message(TuiMessageKind::User, "resume legacy workspace session");
        session_store
            .save_state(&target_session, &target_state)
            .expect("save legacy TUI record");
        let mut legacy_record = session_store
            .load_session(&target_session.session_id)
            .expect("load legacy TUI record");
        legacy_record.workspace_projection_version = 0;
        write_tui_json_atomic(
            &session_store.record_path_for_session(&target_session.session_id),
            &legacy_record,
        )
        .expect("remove workspace projection marker from legacy record");
        let mut canonical =
            AgentSession::new(target_session.session_id.clone(), AgentRole::Deliberation)
                .with_agent_definition_key("coding-agent");
        canonical.workspace_binding = Some(WorkspaceRef::local_path(workspace.clone()));
        FileAgentSessionStore::for_data_dir(&data_dir)
            .save_session(&canonical)
            .expect("save legacy coding session");

        let handler = RuntimeMainAgentInteractionHandler::from_runtime_config(
            &data_dir,
            ModelRuntimeConfig::default(),
        )
        .expect("handler");
        let mut runtime =
            new_tui_interaction_runtime(handler, None, &session_store).expect("runtime");
        let mut state = TuiState::new(
            runtime.session(),
            None,
            runtime.handler.tui_agent_runtime_info(),
        );

        resume_tui_session(
            &mut runtime,
            &mut state,
            &session_store,
            &data_dir,
            None,
            target_session.session_id.clone(),
        )
        .expect("resume legacy workspace session");

        assert_eq!(
            state.workspaces,
            vec![WorkspaceRef::local_path(workspace.clone())]
        );
        session_store
            .save_state(runtime.session(), &state)
            .expect("persist migrated conversation workspace");
        let migrated_record = session_store
            .load_session(&target_session.session_id)
            .expect("load migrated TUI record");
        assert_eq!(
            migrated_record.workspaces,
            vec![WorkspaceRef::local_path(workspace)]
        );
        assert_eq!(
            migrated_record.workspace_projection_version,
            TUI_WORKSPACE_PROJECTION_VERSION
        );
    }

    #[test]
    fn tui_resume_does_not_backfill_authoritative_empty_workspace_grants() {
        let root = temp_file_browser_root("empty-workspace-resume");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let session_store = TuiSessionStore::for_data_dir(&data_dir);
        let target_session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_tui_empty_workspace_resume").expect("node"),
            InteractionSurface::new(
                "surface.tui-empty-workspace-resume",
                InteractionSurfaceKind::CommandLine,
            ),
        );
        let mut target_state = TuiState::new(&target_session, None, TuiAgentRuntimeInfo::default());
        target_state.workspaces.clear();
        target_state.push_message(TuiMessageKind::User, "resume without workspace grants");
        session_store
            .save_state(&target_session, &target_state)
            .expect("save authoritative empty TUI record");
        let mut canonical = AgentSession::new(target_session.session_id.clone(), AgentRole::Main)
            .with_agent_definition_key("main-agent");
        canonical.workspace_binding = Some(WorkspaceRef::local_path("/stale/legacy/workspace"));
        FileAgentSessionStore::for_data_dir(&data_dir)
            .save_session(&canonical)
            .expect("save canonical session with stale shadow");

        let handler = RuntimeMainAgentInteractionHandler::from_runtime_config(
            &data_dir,
            ModelRuntimeConfig::default(),
        )
        .expect("handler");
        let mut runtime =
            new_tui_interaction_runtime(handler, None, &session_store).expect("runtime");
        let mut state = TuiState::new(
            runtime.session(),
            None,
            runtime.handler.tui_agent_runtime_info(),
        );

        resume_tui_session(
            &mut runtime,
            &mut state,
            &session_store,
            &data_dir,
            None,
            target_session.session_id.clone(),
        )
        .expect("resume authoritative empty workspace session");

        assert!(state.workspaces.is_empty());
        assert_eq!(
            runtime
                .handler
                .agent_session_snapshot()
                .expect("snapshot")
                .expect("session")
                .workspace_binding,
            None
        );
    }

    #[test]
    fn tui_resume_migrates_legacy_agent_identity_from_ui_metadata() {
        let root = temp_file_browser_root("legacy-agent-resume");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let session_store = TuiSessionStore::for_data_dir(&data_dir);
        let target_session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_tui_legacy_resume").expect("node"),
            InteractionSurface::new(
                "surface.tui-legacy-resume",
                InteractionSurfaceKind::CommandLine,
            ),
        );
        let coding_info = TuiAgentRuntimeInfo {
            agent: "coding-agent".to_string(),
            ..TuiAgentRuntimeInfo::default()
        };
        let mut target_state = TuiState::new(&target_session, None, coding_info);
        target_state.push_message(TuiMessageKind::User, "resume legacy session");
        session_store
            .save_state(&target_session, &target_state)
            .expect("save legacy TUI metadata");
        FileAgentSessionStore::for_data_dir(&data_dir)
            .save_session(&AgentSession::new(
                target_session.session_id.clone(),
                AgentRole::Deliberation,
            ))
            .expect("save legacy canonical session");

        let handler = RuntimeMainAgentInteractionHandler::from_runtime_config(
            &data_dir,
            ModelRuntimeConfig::default(),
        )
        .expect("handler");
        let mut runtime =
            new_tui_interaction_runtime(handler, None, &session_store).expect("runtime");
        let mut state = TuiState::new(
            runtime.session(),
            None,
            runtime.handler.tui_agent_runtime_info(),
        );

        let outputs = resume_tui_session(
            &mut runtime,
            &mut state,
            &session_store,
            &data_dir,
            None,
            target_session.session_id.clone(),
        )
        .expect("resume legacy session");

        assert!(outputs.iter().any(|output| matches!(
            output,
            InteractionOutput::Line(line) if line.contains("legacy session restore")
        )));
        let migrated = FileAgentSessionStore::for_data_dir(&data_dir)
            .load_session(&target_session.session_id)
            .expect("load migrated session")
            .expect("migrated session");
        assert_eq!(
            migrated.agent_definition_key.as_deref(),
            Some("coding-agent")
        );
        assert_eq!(state.runtime_info.agent, "coding-agent");
    }
}

pub trait InteractionEventHandler {
    fn handle_event<S: EventStore>(
        &mut self,
        store: &mut S,
        session: &InteractionSession,
        event: &EventEnvelope,
    ) -> Result<Vec<InteractionOutput>, CliError>;

    fn handle_event_streaming<S: EventStore>(
        &mut self,
        store: &mut S,
        session: &InteractionSession,
        event: &EventEnvelope,
        emit: &mut dyn FnMut(InteractionOutput),
    ) -> Result<Vec<InteractionOutput>, CliError> {
        let _ = emit;
        self.handle_event(store, session, event)
    }

    fn stable_chat_agent_key(&self) -> Option<String> {
        None
    }

    fn join_stable_chat_turn(&mut self) -> Result<(), CliError> {
        Ok(())
    }

    fn drain_stable_chat_runtime_events(&mut self) -> Result<Vec<AgentRuntimeEvent>, CliError> {
        Ok(Vec::new())
    }
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
        let event = user_interaction_input_event(&self.session, text);
        self.store.append(event.clone())?;
        self.handler
            .handle_event(&mut self.store, &self.session, &event)
    }

    pub fn ingest_user_input_streaming(
        &mut self,
        text: &str,
        emit: &mut dyn FnMut(InteractionOutput),
    ) -> Result<Vec<InteractionOutput>, CliError> {
        let event = user_interaction_input_event(&self.session, text);
        self.store.append(event.clone())?;
        self.handler
            .handle_event_streaming(&mut self.store, &self.session, &event, emit)
    }

    pub fn ingest_user_input_with_context_attachments_streaming(
        &mut self,
        text: &str,
        context_attachments: Vec<sinomo_agent::AgentContextAttachment>,
        emit: &mut dyn FnMut(InteractionOutput),
    ) -> Result<Vec<InteractionOutput>, CliError> {
        let mut event = user_interaction_input_event(&self.session, text);
        if !context_attachments.is_empty() {
            event.payload["context_attachments"] = serde_json::to_value(&context_attachments)
                .map_err(|source| CliError::Parse {
                    detail: format!("serialize context attachments failed: {source}"),
                })?;
        }
        self.store.append(event.clone())?;
        self.handler
            .handle_event_streaming(&mut self.store, &self.session, &event, emit)
    }

    pub fn ingest_user_input_with_item_id_and_context_attachments_streaming(
        &mut self,
        text: &str,
        item_id: sinomo_agent::TaskId,
        context_attachments: Vec<sinomo_agent::AgentContextAttachment>,
        emit: &mut dyn FnMut(InteractionOutput),
    ) -> Result<Vec<InteractionOutput>, CliError> {
        let mut event = user_interaction_input_event(&self.session, text);
        event.payload["agent_item_id"] = serde_json::Value::String(item_id.to_string());
        if !context_attachments.is_empty() {
            event.payload["context_attachments"] = serde_json::to_value(&context_attachments)
                .map_err(|source| CliError::Parse {
                    detail: format!("serialize context attachments failed: {source}"),
                })?;
        }
        self.store.append(event.clone())?;
        self.handler
            .handle_event_streaming(&mut self.store, &self.session, &event, emit)
    }

    /// Run a model turn with surface-owned system context without recording a
    /// second user message in the durable agent session. The visible user
    /// message must already have been reconciled into that session.
    pub fn ingest_transient_system_context_with_attachments_streaming(
        &mut self,
        system_context: &str,
        context_attachments: Vec<sinomo_agent::AgentContextAttachment>,
        emit: &mut dyn FnMut(InteractionOutput),
    ) -> Result<Vec<InteractionOutput>, CliError> {
        let mut event = user_interaction_input_event(&self.session, "");
        event.payload["transient_system_context"] = serde_json::json!(system_context);
        if !context_attachments.is_empty() {
            event.payload["context_attachments"] = serde_json::to_value(&context_attachments)
                .map_err(|source| CliError::Parse {
                    detail: format!("serialize context attachments failed: {source}"),
                })?;
        }
        self.store.append(event.clone())?;
        self.handler
            .handle_event_streaming(&mut self.store, &self.session, &event, emit)
    }

    pub fn append_session_context(&mut self, event: EventEnvelope) -> Result<(), CliError> {
        self.store.append(event)?;
        Ok(())
    }

    pub fn replace_store_and_session(&mut self, store: S, session: InteractionSession) {
        self.store = store;
        self.session = session;
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn handler(&self) -> &H {
        &self.handler
    }

    pub fn handler_mut(&mut self) -> &mut H {
        &mut self.handler
    }

    pub fn session(&self) -> &InteractionSession {
        &self.session
    }
}

pub(crate) enum DefaultMainAgentChatModel {
    #[cfg(feature = "native-models")]
    RustMiniCpmV46(RustMiniCpmV46ChatModel),
    Configured(ConfiguredInteractionModel),
}

impl DefaultMainAgentChatModel {
    fn from_runtime_config(
        data_dir: &SinomoDataDir,
        runtime_config: ModelRuntimeConfig,
        thinking: ThinkingControl,
    ) -> Self {
        if runtime_config.pytorch_bridge.endpoint.is_some() {
            return Self::Configured(ConfiguredInteractionModel {
                data_dir: data_dir.clone(),
                provider: MINICPM_PROVIDER_ID.to_string(),
                model: MINICPM_DEFAULT_MODEL.to_string(),
                account: None,
                runtime_config,
                managed_pytorch: std::sync::Mutex::new(None),
            });
        }
        #[cfg(feature = "native-models")]
        {
            Self::RustMiniCpmV46(RustMiniCpmV46ChatModel::from_runtime_config(
                data_dir,
                runtime_config,
                thinking,
            ))
        }
        #[cfg(not(feature = "native-models"))]
        {
            let _ = thinking;
            Self::Configured(ConfiguredInteractionModel {
                data_dir: data_dir.clone(),
                provider: MINICPM_PROVIDER_ID.to_string(),
                model: MINICPM_DEFAULT_MODEL.to_string(),
                account: None,
                runtime_config,
                managed_pytorch: std::sync::Mutex::new(None),
            })
        }
    }

    fn runtime_info_for_config(runtime_config: &ModelRuntimeConfig) -> TuiAgentRuntimeInfo {
        if runtime_config.pytorch_bridge.endpoint.is_some() {
            return TuiAgentRuntimeInfo {
                agent: "main-agent".to_string(),
                provider: MINICPM_PROVIDER_ID.to_string(),
                model: MINICPM_DEFAULT_MODEL.to_string(),
                account: None,
                reasoning_effort: None,
                reasoning_budget_tokens: None,
                tool_runtime: sinomo_agent::AgentToolRuntimeDefinition::default(),
            };
        }
        TuiAgentRuntimeInfo {
            agent: "main-agent".to_string(),
            provider: default_main_agent_chat_model_provider().to_string(),
            model: default_main_agent_chat_model_id().to_string(),
            account: None,
            reasoning_effort: None,
            reasoning_budget_tokens: None,
            tool_runtime: sinomo_agent::AgentToolRuntimeDefinition::default(),
        }
    }

    fn configured(
        data_dir: &SinomoDataDir,
        provider: String,
        model: String,
        account: Option<sinomo_lm::ProviderAccountConfig>,
        runtime_config: ModelRuntimeConfig,
    ) -> Self {
        Self::Configured(ConfiguredInteractionModel {
            data_dir: data_dir.clone(),
            provider,
            model,
            account,
            runtime_config,
            managed_pytorch: std::sync::Mutex::new(None),
        })
    }
}

impl InteractionModel for DefaultMainAgentChatModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        match self {
            #[cfg(feature = "native-models")]
            Self::RustMiniCpmV46(model) => model.complete(request),
            Self::Configured(model) => model.complete(request),
        }
    }

    fn complete_streaming(
        &self,
        request: &LanguageModelRequest,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        match self {
            #[cfg(feature = "native-models")]
            Self::RustMiniCpmV46(model) => model.complete_streaming(request, on_delta),
            Self::Configured(model) => {
                on_delta("<sinomo_progress>waiting for configured model runtime</sinomo_progress>");
                model.complete_streaming(request, on_delta)
            }
        }
    }

    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation_token: &AgentCancellationToken,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        match self {
            #[cfg(feature = "native-models")]
            Self::RustMiniCpmV46(model) => model.complete_streaming_events_with_cancellation(
                request,
                cancellation_token,
                on_event,
            ),
            Self::Configured(model) => {
                if !cancellation_token.is_cancelled() {
                    on_event(LanguageModelStreamEvent::TextDelta {
                        delta: "<sinomo_progress>waiting for configured model runtime</sinomo_progress>"
                            .to_string(),
                    });
                }
                model.complete_streaming_events_with_cancellation(
                    request,
                    cancellation_token,
                    on_event,
                )
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ThinkingControl {
    enabled: Arc<Mutex<bool>>,
}

impl Default for ThinkingControl {
    fn default() -> Self {
        Self {
            enabled: Arc::new(Mutex::new(true)),
        }
    }
}

impl ThinkingControl {
    pub fn is_enabled(&self) -> bool {
        self.enabled.lock().map(|value| *value).unwrap_or(true)
    }

    pub fn set_enabled(&self, enabled: bool) {
        if let Ok(mut value) = self.enabled.lock() {
            *value = enabled;
        }
    }
}

enum RuntimeMainAgentSlot<M>
where
    M: InteractionModel + Send + Sync + 'static,
{
    Pending(M),
    Ready(AgentSessionRuntime<M>),
}

pub(crate) struct RuntimeMainAgentInteractionHandler<M>
where
    M: InteractionModel + Send + Sync + 'static,
{
    runtime: Option<RuntimeMainAgentSlot<M>>,
    runtime_storage: AgentRuntimeStorage,
    runtime_control_registry: sinomo_agent::AgentRuntimeControlRegistry,
    runtime_cursor: AgentRuntimeEventCursor,
    agent_definition: AgentDefinition,
    skill_catalog: sinomo_agent::SkillCatalog,
    skill_catalog_texts: std::collections::BTreeMap<String, String>,
    skill_catalog_resources:
        std::collections::BTreeMap<String, Vec<sinomo_agent::AgentSkillPackageFile>>,
    module_tool_projection: sinomo_agent::AgentModuleToolProjection,
    tool_registry: sinomo_agent::AgentToolRegistry,
    thinking: ThinkingControl,
    provider: String,
    model_name: String,
    account: Option<String>,
    reasoning_effort: Option<ProviderReasoningEffort>,
    reasoning_budget_tokens: Option<u32>,
    max_context_tokens: Option<u32>,
    auto_compact_token_limit: Option<u32>,
    /// Conversation-projected primary workspace. `None` is a real revocation,
    /// not an instruction to fall back to the process cwd.
    workspace_root: Option<PathBuf>,
}

pub(crate) struct RuntimeMainAgentTransitionSnapshot<M>
where
    M: InteractionModel + Send + Sync + 'static,
{
    runtime: AgentSessionRuntime<M>,
    runtime_session: AgentSession,
    runtime_cursor: AgentRuntimeEventCursor,
    agent_definition: AgentDefinition,
    tool_registry: sinomo_agent::AgentToolRegistry,
    provider: String,
    model_name: String,
    account: Option<String>,
    reasoning_effort: Option<ProviderReasoningEffort>,
    reasoning_budget_tokens: Option<u32>,
    max_context_tokens: Option<u32>,
    auto_compact_token_limit: Option<u32>,
    workspace_root: Option<PathBuf>,
}

type TuiAgentResumeSnapshot = RuntimeMainAgentTransitionSnapshot<DefaultMainAgentChatModel>;

#[cfg(test)]
#[derive(Default)]
pub(crate) struct ThinkingStreamDisplay {
    content: String,
    emitted: usize,
    announced: bool,
    prompt_started_thinking: bool,
}

#[cfg(test)]
impl ThinkingStreamDisplay {
    pub(crate) fn new(prompt_started_thinking: bool) -> Self {
        Self {
            prompt_started_thinking,
            ..Self::default()
        }
    }

    pub(crate) fn push(&mut self, delta: &str) -> Vec<String> {
        if let Some(progress) = progress_stream_delta(delta) {
            return vec![format!("model> {progress}\n")];
        }
        self.content.push_str(delta);
        let Some(thinking) = visible_thinking_prefix(&self.content, self.prompt_started_thinking)
        else {
            return Vec::new();
        };
        if thinking.len() <= self.emitted {
            return Vec::new();
        }
        let mut chunks = Vec::new();
        if !self.announced {
            chunks.push("thinking> ".to_string());
            self.announced = true;
        }
        chunks.push(thinking[self.emitted..].to_string());
        self.emitted = thinking.len();
        chunks
    }
}

#[cfg(test)]
fn progress_stream_delta(delta: &str) -> Option<&str> {
    delta
        .strip_prefix("<sinomo_progress>")
        .and_then(|value| value.strip_suffix("</sinomo_progress>"))
}

#[cfg(test)]
fn visible_thinking_prefix(content: &str, prompt_started_thinking: bool) -> Option<&str> {
    if prompt_started_thinking {
        if content.trim_start().starts_with('{') {
            return None;
        }
        let end = content.find("</think>").unwrap_or(content.len());
        return Some(&content[..end]);
    }
    let start = content.find("<think>")? + "<think>".len();
    let after_start = &content[start..];
    let end = after_start.find("</think>").unwrap_or(after_start.len());
    Some(&after_start[..end])
}

impl<M> RuntimeMainAgentInteractionHandler<M>
where
    M: InteractionModel + Send + Sync + 'static,
{
    pub fn with_thinking(
        model: M,
        provider: impl Into<String>,
        model_name: impl Into<String>,
        thinking: ThinkingControl,
    ) -> Self {
        Self {
            runtime: Some(RuntimeMainAgentSlot::Pending(model)),
            runtime_storage: AgentRuntimeStorage::new(),
            runtime_control_registry: sinomo_agent::AgentRuntimeControlRegistry::new(),
            runtime_cursor: AgentRuntimeEventCursor {
                offset: 0,
                last_event_id: None,
            },
            agent_definition: AgentDefinition::main(),
            skill_catalog: sinomo_agent::SkillCatalog::default(),
            skill_catalog_texts: std::collections::BTreeMap::new(),
            skill_catalog_resources: std::collections::BTreeMap::new(),
            module_tool_projection: sinomo_agent::AgentModuleToolProjection::default(),
            tool_registry: sinomo_agent::AgentToolRegistry::new(),
            thinking,
            provider: provider.into(),
            model_name: model_name.into(),
            account: None,
            reasoning_effort: None,
            reasoning_budget_tokens: None,
            max_context_tokens: None,
            auto_compact_token_limit: None,
            workspace_root: Some(std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))),
        }
    }

    pub fn with_tool_registry(mut self, registry: sinomo_agent::AgentToolRegistry) -> Self {
        self.tool_registry = registry;
        self
    }

    pub fn with_module_tool_projection(
        mut self,
        projection: sinomo_agent::AgentModuleToolProjection,
    ) -> Self {
        self.module_tool_projection = projection;
        self
    }

    pub fn with_skill_catalog(mut self, catalog: sinomo_agent::SkillCatalog) -> Self {
        self.skill_catalog = catalog;
        self
    }

    pub fn with_skill_catalog_texts(
        mut self,
        texts: std::collections::BTreeMap<String, String>,
    ) -> Self {
        self.skill_catalog_texts = texts;
        self
    }

    pub fn with_skill_catalog_resources(
        mut self,
        resources: std::collections::BTreeMap<String, Vec<sinomo_agent::AgentSkillPackageFile>>,
    ) -> Self {
        self.skill_catalog_resources = resources;
        self
    }

    pub fn with_runtime_storage(mut self, storage: AgentRuntimeStorage) -> Self {
        self.runtime_storage = storage;
        self
    }

    pub(crate) fn runtime_control_registry(&self) -> sinomo_agent::AgentRuntimeControlRegistry {
        self.runtime_control_registry.clone()
    }

    pub(crate) fn tool_registry(&self) -> sinomo_agent::AgentToolRegistry {
        self.tool_registry.clone()
    }

    pub(crate) fn module_tool_projection(&self) -> sinomo_agent::AgentModuleToolProjection {
        self.module_tool_projection.clone()
    }

    pub(crate) fn agent_definition(&self) -> &AgentDefinition {
        &self.agent_definition
    }

    pub(crate) fn with_runtime_control_registry(
        mut self,
        registry: sinomo_agent::AgentRuntimeControlRegistry,
    ) -> Self {
        self.runtime_control_registry = registry;
        self
    }

    fn runtime_info(&self) -> TuiAgentRuntimeInfo {
        TuiAgentRuntimeInfo {
            agent: self.agent_definition.key().to_string(),
            provider: self.provider.clone(),
            model: self.model_name.clone(),
            account: self.account.clone(),
            reasoning_effort: self.reasoning_effort,
            reasoning_budget_tokens: self.reasoning_budget_tokens,
            tool_runtime: self.agent_definition.tool_runtime.clone(),
        }
    }

    fn build_runtime(&self, model: M, session: AgentSession) -> AgentSessionRuntime<M> {
        let mut config =
            sinomo_agent::AgentConfig::new(self.provider.clone(), self.model_name.clone())
                .with_agent_definition(self.agent_definition.clone())
                .with_module_tool_projection(self.module_tool_projection.clone())
                .with_model_context_limits(self.max_context_tokens, self.auto_compact_token_limit)
                .with_skill_catalog(self.skill_catalog.clone())
                .with_skill_catalog_texts(self.skill_catalog_texts.clone())
                .with_skill_catalog_resources(self.skill_catalog_resources.clone());
        if let Some(workspace_root) = self.workspace_root.as_ref() {
            config = config.with_workspace_root(workspace_root.clone());
        }
        let agent = Agent::new(model, self.provider.clone(), self.model_name.clone())
            .with_config(config)
            .with_tool_registry(self.tool_registry.clone());
        AgentSessionRuntime::with_storage(agent, session, self.runtime_storage.clone())
    }

    fn ensure_runtime(
        &mut self,
        session: &InteractionSession,
    ) -> Result<&AgentSessionRuntime<M>, CliError> {
        if !matches!(self.runtime, Some(RuntimeMainAgentSlot::Ready(_))) {
            let Some(RuntimeMainAgentSlot::Pending(model)) = self.runtime.take() else {
                return Err(CliError::Parse {
                    detail: "agent runtime is not initialized".to_string(),
                });
            };
            let agent_session = AgentSession::new(
                session.session_id.clone(),
                self.agent_definition.role.clone(),
            )
            .with_agent_definition_key(self.agent_definition.key());
            let runtime = self.build_runtime(model, agent_session);
            self.register_runtime_control(&runtime)?;
            self.runtime = Some(RuntimeMainAgentSlot::Ready(runtime));
            self.runtime_cursor = AgentRuntimeEventCursor {
                offset: 0,
                last_event_id: None,
            };
        }
        match self.runtime.as_ref() {
            Some(RuntimeMainAgentSlot::Ready(runtime)) => Ok(runtime),
            _ => Err(CliError::Parse {
                detail: "agent runtime is not ready".to_string(),
            }),
        }
    }

    fn ready_runtime(&self) -> Option<&AgentSessionRuntime<M>> {
        match self.runtime.as_ref() {
            Some(RuntimeMainAgentSlot::Ready(runtime)) => Some(runtime),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn join_active_tui_turn(&self) -> Result<(), CliError> {
        let Some(runtime) = self.ready_runtime() else {
            return Ok(());
        };
        runtime.join_active_turn()?;
        Ok(())
    }

    pub(crate) fn drain_runtime_events(&mut self) -> Result<Vec<AgentRuntimeEvent>, CliError> {
        let Some(runtime) = self.ready_runtime() else {
            return Ok(Vec::new());
        };
        let batch = runtime.runtime_events_since(self.runtime_cursor.clone())?;
        self.runtime_cursor = batch.next_cursor;
        Ok(batch.events)
    }

    fn replace_model(
        &mut self,
        model: M,
        provider: String,
        model_name: String,
        account: Option<String>,
        options: LanguageModelProviderOptions,
        recall_transition: Option<&AgentRecallToolTransition>,
    ) -> Result<TuiAgentRuntimeInfo, CliError> {
        let previous_runtime = self.runtime.take();
        let previous_control_runtime = match previous_runtime.as_ref() {
            Some(RuntimeMainAgentSlot::Ready(runtime)) => Some((*runtime).clone()),
            Some(RuntimeMainAgentSlot::Pending(_)) | None => None,
        };
        let preserved_session = match previous_runtime.as_ref() {
            Some(RuntimeMainAgentSlot::Ready(runtime)) => {
                let snapshot = (|| {
                    if runtime.active_turn()?.is_some() {
                        let _ = runtime
                            .interrupt_active_turn("model switched while turn was active")?;
                        runtime.join_active_turn()?;
                    }
                    runtime.session_snapshot().map_err(CliError::from)
                })();
                match snapshot {
                    Ok(session) => Some(session),
                    Err(error) => {
                        self.runtime = previous_runtime;
                        return Err(error);
                    }
                }
            }
            Some(RuntimeMainAgentSlot::Pending(_)) | None => None,
        };

        // Detach the externally reachable old runtime before removing its
        // principal-bound executors. New calls fail closed until the target
        // runtime is published and the complete target bundle is installed.
        if recall_transition.is_some()
            && let Some(session) = preserved_session.as_ref()
            && let Err(error) = self.unregister_runtime_control_session(&session.session_id)
        {
            self.runtime = previous_runtime;
            return Err(error);
        }
        if recall_transition.is_some() {
            self.tool_registry
                .remove_registered_tools(AGENT_RECALL_TOOL_NAMES);
        }

        let previous_provider = std::mem::replace(&mut self.provider, provider);
        let previous_model_name = std::mem::replace(&mut self.model_name, model_name);
        let previous_account = std::mem::replace(&mut self.account, account);
        let previous_reasoning_effort =
            std::mem::replace(&mut self.reasoning_effort, options.reasoning_effort);
        let previous_reasoning_budget_tokens = std::mem::replace(
            &mut self.reasoning_budget_tokens,
            options.reasoning_budget_tokens,
        );
        let previous_max_context_tokens =
            std::mem::replace(&mut self.max_context_tokens, options.max_context_tokens);
        let previous_auto_compact_token_limit = std::mem::replace(
            &mut self.auto_compact_token_limit,
            options.auto_compact_token_limit,
        );
        let next_runtime = match preserved_session {
            Some(mut session) => {
                if recall_transition.is_some() {
                    // Only an explicit user agent switch may change the
                    // identity of the currently attached live session.
                    session.role = self.agent_definition.role.clone();
                    session.agent_definition_key = Some(self.agent_definition.key().to_string());
                }
                let runtime = self.build_runtime(model, session);
                if let Err(error) = self.register_runtime_control(&runtime) {
                    if let Some(transition) = recall_transition {
                        self.tool_registry
                            .replace_registered_tools_from(&transition.previous);
                    }
                    let control_rollback = previous_control_runtime
                        .as_ref()
                        .and_then(|runtime| self.register_runtime_control(runtime).err());
                    self.provider = previous_provider;
                    self.model_name = previous_model_name;
                    self.account = previous_account;
                    self.reasoning_effort = previous_reasoning_effort;
                    self.reasoning_budget_tokens = previous_reasoning_budget_tokens;
                    self.max_context_tokens = previous_max_context_tokens;
                    self.auto_compact_token_limit = previous_auto_compact_token_limit;
                    self.runtime = previous_runtime;
                    return Err(match control_rollback {
                        Some(rollback) => CliError::Parse {
                            detail: format!(
                                "replace agent runtime failed: {error}; restore previous runtime-control route failed: {rollback}"
                            ),
                        },
                        None => error,
                    });
                }
                RuntimeMainAgentSlot::Ready(runtime)
            }
            None => RuntimeMainAgentSlot::Pending(model),
        };
        self.runtime = Some(next_runtime);
        if let Some(transition) = recall_transition {
            self.tool_registry
                .replace_registered_tools_from(&transition.next);
        }
        self.runtime_cursor = AgentRuntimeEventCursor {
            offset: 0,
            last_event_id: None,
        };
        Ok(self.runtime_info())
    }

    fn validate_agent_definition(
        &self,
        definition: AgentDefinition,
        daemon_connected: bool,
    ) -> Result<AgentDefinition, CliError> {
        let mut definition = definition;
        if definition.id.is_empty() {
            definition.id = definition.name.clone();
        }
        definition.validate().map_err(|source| CliError::Parse {
            detail: source.to_string(),
        })?;
        if !definition.supports_interactive_chat() {
            return Err(CliError::Parse {
                detail: format!(
                    "agent `{}` has role `{:?}` and is not available on interactive chat surfaces",
                    definition.key(),
                    definition.role
                ),
            });
        }
        if !daemon_connected && !definition.skills.is_empty() {
            return Err(CliError::Parse {
                detail: format!(
                    "agent `{}` requires packaged skills ({}), but the resident daemon SIFS skill catalog is unavailable in daemonless mode",
                    definition.key(),
                    definition.skills.join(", ")
                ),
            });
        }
        Ok(definition)
    }

    #[cfg(test)]
    fn set_agent_definition(&mut self, definition: AgentDefinition) -> Result<(), CliError> {
        let definition = self.validate_agent_definition(definition, true)?;
        self.agent_definition = definition;
        Ok(())
    }

    pub(crate) fn replace_agent_session(
        &mut self,
        interaction_session: &InteractionSession,
        transcript: sinomo_agent::AgentTranscript,
    ) -> Result<(), CliError> {
        let mut agent_session = AgentSession::new(
            interaction_session.session_id.clone(),
            self.agent_definition.role.clone(),
        );
        agent_session.agent_definition_key = Some(self.agent_definition.key().to_string());
        agent_session.transcript = transcript.clone();
        agent_session.raw_transcript = transcript;
        self.replace_attached_agent_session(interaction_session, agent_session)
    }

    pub(crate) fn replace_full_agent_session(
        &mut self,
        interaction_session: &InteractionSession,
        mut agent_session: AgentSession,
    ) -> Result<(), CliError> {
        agent_session.session_id = interaction_session.session_id.clone();
        let persisted_key = agent_session.agent_definition_key.as_deref().ok_or_else(|| {
            CliError::Parse {
                detail: format!(
                    "legacy agent session `{}` has no persisted agent definition; select its agent explicitly before restoring it",
                    agent_session.session_id
                ),
            }
        })?;
        if persisted_key != self.agent_definition.key() {
            return Err(CliError::Parse {
                detail: format!(
                    "agent session `{}` belongs to `{persisted_key}` and cannot be restored as `{}`",
                    agent_session.session_id,
                    self.agent_definition.key()
                ),
            });
        }
        agent_session.role = self.agent_definition.role.clone();
        self.replace_attached_agent_session(interaction_session, agent_session)
    }

    fn replace_attached_agent_session(
        &mut self,
        interaction_session: &InteractionSession,
        agent_session: AgentSession,
    ) -> Result<(), CliError> {
        let runtime = self.ensure_runtime(interaction_session)?.clone();
        let previous_session = runtime.session_snapshot()?;
        self.unregister_runtime_control_session(&previous_session.session_id)?;

        if let Err(error) = runtime.replace_session(agent_session) {
            let route_rollback = self.register_runtime_control(&runtime).err();
            return Err(match route_rollback {
                Some(rollback) => CliError::Parse {
                    detail: format!(
                        "replace agent session failed: {error}; restoring the previous runtime-control route also failed: {rollback}"
                    ),
                },
                None => error.into(),
            });
        }

        let cursor = match runtime.runtime_event_cursor() {
            Ok(cursor) => cursor,
            Err(error) => {
                return Err(self.rollback_attached_agent_session(
                    &runtime,
                    previous_session,
                    error.into(),
                ));
            }
        };
        if let Err(error) = self.register_runtime_control(&runtime) {
            return Err(self.rollback_attached_agent_session(&runtime, previous_session, error));
        }
        self.runtime_cursor = cursor;
        Ok(())
    }

    fn rollback_attached_agent_session(
        &self,
        runtime: &AgentSessionRuntime<M>,
        previous_session: AgentSession,
        error: CliError,
    ) -> CliError {
        let mut rollback_errors = Vec::new();
        if let Err(rollback) = runtime.replace_session(previous_session) {
            rollback_errors.push(format!("restore previous live session: {rollback}"));
        }
        if let Err(rollback) = self.register_runtime_control(runtime) {
            rollback_errors.push(format!(
                "restore previous runtime-control route: {rollback}"
            ));
        }
        if rollback_errors.is_empty() {
            error
        } else {
            CliError::Parse {
                detail: format!(
                    "replace agent session failed: {error}; rollback failed: {}",
                    rollback_errors.join("; ")
                ),
            }
        }
    }

    pub(crate) fn agent_session_snapshot(&self) -> Result<Option<AgentSession>, CliError> {
        let Some(runtime) = self.ready_runtime() else {
            return Ok(None);
        };
        Ok(Some(runtime.session_snapshot()?))
    }

    pub(crate) fn transition_snapshot(
        &mut self,
        interaction_session: &InteractionSession,
    ) -> Result<RuntimeMainAgentTransitionSnapshot<M>, CliError> {
        let (runtime, runtime_session) = {
            let runtime = self.ensure_runtime(interaction_session)?;
            if runtime.active_turn()?.is_some() {
                return Err(CliError::Parse {
                    detail: format!(
                        "cannot begin a session transition while agent session `{}` has an active turn",
                        interaction_session.session_id
                    ),
                });
            }
            runtime.join_active_turn()?;
            (runtime.clone(), runtime.session_snapshot()?)
        };
        if runtime_session.session_id != interaction_session.session_id {
            return Err(CliError::Parse {
                detail: format!(
                    "agent runtime session `{}` does not match transition request for `{}`",
                    runtime_session.session_id, interaction_session.session_id
                ),
            });
        }
        Ok(RuntimeMainAgentTransitionSnapshot {
            runtime,
            runtime_session,
            runtime_cursor: self.runtime_cursor.clone(),
            agent_definition: self.agent_definition.clone(),
            tool_registry: self.tool_registry.detached_snapshot(),
            provider: self.provider.clone(),
            model_name: self.model_name.clone(),
            account: self.account.clone(),
            reasoning_effort: self.reasoning_effort,
            reasoning_budget_tokens: self.reasoning_budget_tokens,
            max_context_tokens: self.max_context_tokens,
            auto_compact_token_limit: self.auto_compact_token_limit,
            workspace_root: self.workspace_root.clone(),
        })
    }

    pub(crate) fn restore_transition_snapshot(
        &mut self,
        data_dir: &SinomoDataDir,
        snapshot: RuntimeMainAgentTransitionSnapshot<M>,
    ) -> Result<(), CliError> {
        let current_runtime = self.ready_runtime().cloned();
        if let Some(runtime) = current_runtime.as_ref() {
            if runtime.active_turn()?.is_some() {
                let _ = runtime.interrupt_active_turn("session transition rolled back")?;
            }
            runtime.join_active_turn()?;
        }

        if let Some(runtime) = current_runtime.as_ref() {
            let current_session_id = runtime.session_snapshot()?.session_id;
            self.unregister_runtime_control_session(&current_session_id)?;
        }
        if current_runtime
            .as_ref()
            .and_then(|runtime| runtime.session_snapshot().ok())
            .is_none_or(|session| session.session_id != snapshot.runtime_session.session_id)
        {
            self.unregister_runtime_control_session(&snapshot.runtime_session.session_id)?;
        }

        // Once both possible routes are detached, remove every executor so a
        // stale runtime handle also fails closed while the old state is restored.
        self.tool_registry.clear();
        if snapshot.runtime.session_snapshot()? != snapshot.runtime_session {
            snapshot
                .runtime
                .replace_session(snapshot.runtime_session.clone())?;
        }

        self.agent_definition = snapshot.agent_definition;
        self.provider = snapshot.provider;
        self.model_name = snapshot.model_name;
        self.account = snapshot.account;
        self.reasoning_effort = snapshot.reasoning_effort;
        self.reasoning_budget_tokens = snapshot.reasoning_budget_tokens;
        self.max_context_tokens = snapshot.max_context_tokens;
        self.auto_compact_token_limit = snapshot.auto_compact_token_limit;
        self.workspace_root = snapshot.workspace_root;
        self.runtime_cursor = snapshot.runtime_cursor;
        self.runtime = Some(RuntimeMainAgentSlot::Ready(snapshot.runtime.clone()));

        if let Err(error) = self.register_runtime_control(&snapshot.runtime) {
            return Err(CliError::Parse {
                detail: format!(
                    "restore previous runtime-control route failed: {error}; previous executors remain detached"
                ),
            });
        }
        self.tool_registry
            .replace_all_registered_from(&snapshot.tool_registry);
        self.persist_attached_agent_session(data_dir, &snapshot.runtime_session.session_id)
    }

    pub(crate) fn load_persisted_agent_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<AgentSession>, CliError> {
        self.runtime_storage
            .load_session(session_id)
            .map_err(|source| CliError::Parse {
                detail: format!("load persisted agent session `{session_id}`: {source}"),
            })
    }

    fn persist_attached_agent_session(
        &self,
        data_dir: &SinomoDataDir,
        expected_session_id: &SessionId,
    ) -> Result<(), CliError> {
        let session = self
            .agent_session_snapshot()?
            .ok_or_else(|| CliError::Parse {
                detail: format!(
                    "agent runtime has no canonical session `{expected_session_id}` to persist"
                ),
            })?;
        if &session.session_id != expected_session_id {
            return Err(CliError::Parse {
                detail: format!(
                    "agent runtime session `{}` does not match persistence request for `{expected_session_id}`",
                    session.session_id
                ),
            });
        }
        FileAgentSessionStore::for_data_dir(data_dir)
            .save_session(&session)
            .map_err(|source| CliError::Parse {
                detail: format!(
                    "persist canonical agent session `{expected_session_id}`: {source}"
                ),
            })
    }

    fn register_runtime_control(&self, runtime: &AgentSessionRuntime<M>) -> Result<(), CliError> {
        self.runtime_control_registry
            .register_runtime(runtime.clone())
            .map(|_| ())
            .map_err(runtime_control_cli_error)
    }

    fn unregister_runtime_control_session(&self, session_id: &SessionId) -> Result<(), CliError> {
        self.runtime_control_registry
            .unregister_session(session_id)
            .map(|_| ())
            .map_err(runtime_control_cli_error)
    }
}

fn runtime_control_cli_error(error: sinomo_agent::AgentRuntimeControlError) -> CliError {
    CliError::Parse {
        detail: format!("agent runtime control registry: {error}"),
    }
}

const AGENT_RECALL_TOOL_NAMES: &[&str] = &[
    "session_history_search",
    "memory_search",
    "memory_read",
    "memory_write",
];

struct AgentRecallToolTransition {
    previous: AgentToolRegistry,
    next: AgentToolRegistry,
}

impl AgentRecallToolTransition {
    fn stage(
        data_dir: &SinomoDataDir,
        previous: &AgentDefinition,
        next: &AgentDefinition,
    ) -> Result<Self, CliError> {
        Ok(Self {
            previous: agent_recall_tool_registry(data_dir, previous)?,
            next: agent_recall_tool_registry(data_dir, next)?,
        })
    }
}

impl RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel> {
    #[cfg(test)]
    pub fn from_runtime_config(
        data_dir: &SinomoDataDir,
        runtime_config: ModelRuntimeConfig,
    ) -> Result<Self, CliError> {
        let thinking = ThinkingControl::default();
        let runtime_info = DefaultMainAgentChatModel::runtime_info_for_config(&runtime_config);
        let model = DefaultMainAgentChatModel::from_runtime_config(
            data_dir,
            runtime_config,
            thinking.clone(),
        );
        let skill_catalog = best_effort_reconciled_skill_catalog(data_dir);
        let skill_catalog_texts = best_effort_skill_catalog_texts(data_dir, &skill_catalog);
        let skill_catalog_resources = best_effort_skill_catalog_resources(data_dir, &skill_catalog);
        Ok(
            Self::with_thinking(model, runtime_info.provider, runtime_info.model, thinking)
                .with_runtime_storage(AgentRuntimeStorage::from_data_dir(data_dir))
                .with_module_tool_projection(crate::agent_module_tool_projection(data_dir))
                .with_skill_catalog(skill_catalog)
                .with_skill_catalog_texts(skill_catalog_texts)
                .with_skill_catalog_resources(skill_catalog_resources)
                .with_tool_registry(main_agent_config_tool_registry(data_dir)?),
        )
    }

    fn from_default_model_config(
        data_dir: &SinomoDataDir,
        runtime_config: ModelRuntimeConfig,
        daemon: Option<&DaemonStatus>,
    ) -> Result<Self, CliError> {
        let daemon_connected = daemon.is_some();
        let runtime_skill_catalog = runtime_skill_catalog(daemon)?;
        Self::from_default_model_config_with_runtime_skill_catalog(
            data_dir,
            runtime_config,
            daemon_connected,
            runtime_skill_catalog,
        )
    }

    fn from_default_model_config_with_runtime_skill_catalog(
        data_dir: &SinomoDataDir,
        runtime_config: ModelRuntimeConfig,
        daemon_connected: bool,
        runtime_skill_catalog: RuntimeSkillCatalog,
    ) -> Result<Self, CliError> {
        let thinking = ThinkingControl::default();
        let tool_registry = main_agent_config_tool_registry(data_dir)?;
        let (config, _) =
            with_tui_model_config_mode(daemon_connected, || load_model_config_for_node(data_dir))?;
        let resolved = resolve_default_model(Some(&config), None)?;
        let (skill_catalog, skill_catalog_texts, skill_catalog_resources) = runtime_skill_catalog;
        let module_tool_projection = crate::agent_module_tool_projection(data_dir);
        if resolved.provider == default_main_agent_chat_model_provider()
            && resolved.model == default_main_agent_chat_model_id()
        {
            let runtime_info = DefaultMainAgentChatModel::runtime_info_for_config(&runtime_config);
            let model = DefaultMainAgentChatModel::from_runtime_config(
                data_dir,
                runtime_config,
                thinking.clone(),
            );
            return Ok(Self::with_thinking(
                model,
                runtime_info.provider,
                runtime_info.model,
                thinking,
            )
            .with_runtime_storage(AgentRuntimeStorage::from_data_dir(data_dir))
            .with_module_tool_projection(module_tool_projection.clone())
            .with_skill_catalog(skill_catalog)
            .with_skill_catalog_texts(skill_catalog_texts)
            .with_skill_catalog_resources(skill_catalog_resources)
            .with_tool_registry(tool_registry));
        }
        let account =
            provider_account_config(&config, &resolved.provider, Some(&resolved.account)).cloned();
        let options = provider_options_for_model(account.as_ref(), &resolved.model);
        Ok(Self::with_thinking(
            DefaultMainAgentChatModel::configured(
                data_dir,
                resolved.provider.clone(),
                resolved.model.clone(),
                account,
                runtime_config,
            ),
            resolved.provider,
            resolved.model,
            thinking,
        )
        .with_runtime_storage(AgentRuntimeStorage::from_data_dir(data_dir))
        .with_module_tool_projection(module_tool_projection)
        .with_skill_catalog(skill_catalog)
        .with_skill_catalog_texts(skill_catalog_texts)
        .with_skill_catalog_resources(skill_catalog_resources)
        .with_tool_registry(tool_registry)
        .with_account_and_options(Some(resolved.account), options))
    }

    fn with_account_and_options(
        mut self,
        account: Option<String>,
        options: LanguageModelProviderOptions,
    ) -> Self {
        self.account = account;
        self.reasoning_effort = options.reasoning_effort;
        self.reasoning_budget_tokens = options.reasoning_budget_tokens;
        self.max_context_tokens = options.max_context_tokens;
        self.auto_compact_token_limit = options.auto_compact_token_limit;
        self
    }

    fn switch_agent_definition(
        &mut self,
        data_dir: &SinomoDataDir,
        definition: AgentDefinition,
        daemon_connected: bool,
        rebind_agent_identity: bool,
    ) -> Result<TuiAgentRuntimeInfo, CliError> {
        let definition = self.validate_agent_definition(definition, daemon_connected)?;
        if let Ok(state) = read_cluster_state(&ClusterPaths::new(data_dir)) {
            validate_agent_local_placement(&definition, &state.local_node_id)?;
        } else if definition.placement.home_server_node_id.is_some()
            || definition.tool_runtime.sandbox_node_id.is_some()
        {
            return Err(CliError::Parse {
                detail: format!(
                    "agent `{}` declares node placement, but the local node identity is unavailable",
                    definition.key()
                ),
            });
        }
        // Build every fallible principal-bound executor before changing the
        // live definition/session. The model switch removes the old bundle
        // while the runtime-control route is detached, then publishes the new
        // runtime before installing the target bundle.
        let recall_transition = rebind_agent_identity
            .then(|| {
                AgentRecallToolTransition::stage(data_dir, &self.agent_definition, &definition)
            })
            .transpose()?;
        let previous_definition = std::mem::replace(&mut self.agent_definition, definition.clone());
        let provider = definition
            .model_policy
            .provider
            .clone()
            .unwrap_or_else(|| self.provider.clone());
        let model = definition.model_policy.model.clone().unwrap_or_else(|| {
            if provider == self.provider {
                self.model_name.clone()
            } else {
                sinomo_lm::PROVIDER_DEFAULT_MODEL.to_string()
            }
        });
        let account = if provider == self.provider {
            self.account
                .clone()
                .unwrap_or_else(|| sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string())
        } else {
            sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string()
        };
        let selection = TuiModelSelection {
            provider,
            model,
            account,
        };
        match self.switch_to_selection_with_agent_rebind(
            data_dir,
            selection,
            daemon_connected,
            recall_transition.as_ref(),
        ) {
            Ok(info) => Ok(info),
            Err(error) => {
                self.agent_definition = previous_definition;
                Err(error)
            }
        }
    }

    fn switch_agent_definition_for_session(
        &mut self,
        data_dir: &SinomoDataDir,
        interaction_session: &InteractionSession,
        workspaces: Option<&[WorkspaceRef]>,
        definition: AgentDefinition,
        daemon_connected: bool,
    ) -> Result<TuiAgentRuntimeInfo, CliError> {
        let definition = self.validate_agent_definition(definition, daemon_connected)?;
        self.ensure_runtime(interaction_session)?;
        let previous_session = self
            .agent_session_snapshot()?
            .ok_or_else(|| CliError::Parse {
                detail: format!(
                    "agent runtime has no canonical session `{}` to switch",
                    interaction_session.session_id
                ),
            })?;
        if previous_session.session_id != interaction_session.session_id {
            return Err(CliError::Parse {
                detail: format!(
                    "agent runtime session `{}` does not match agent switch request for `{}`",
                    previous_session.session_id, interaction_session.session_id
                ),
            });
        }

        let mut candidate = previous_session.clone();
        candidate.role = definition.role.clone();
        candidate.agent_definition_key = Some(definition.key().to_string());
        if let Some(workspaces) = workspaces {
            candidate.workspace_binding = workspaces.first().cloned();
        }
        if agent_definition_has_workspace_mutation_authority(&definition)
            && candidate.workspace_binding.is_none()
            && workspaces.is_none()
            && let Some(workspace_root) = self.workspace_root.as_ref()
        {
            let metadata = std::fs::metadata(workspace_root).map_err(|source| CliError::Io {
                path: workspace_root.display().to_string(),
                detail: format!("workspace directory is unavailable: {source}"),
            })?;
            if !metadata.is_dir() {
                return Err(CliError::Parse {
                    detail: format!(
                        "workspace path is not a directory: {}",
                        workspace_root.display()
                    ),
                });
            }
            candidate.workspace_binding = Some(
                mutating_agent_workspace_binding(data_dir, workspace_root)?
                    .binding()
                    .clone(),
            );
        }

        let store = FileAgentSessionStore::for_data_dir(data_dir);
        if let Err(source) = store.save_session(&candidate) {
            let rollback = store.save_session(&previous_session).err();
            return Err(CliError::Parse {
                detail: match rollback {
                    Some(rollback) => format!(
                        "persist agent switch for session `{}` failed: {source}; restoring the previous canonical session also failed: {rollback}",
                        interaction_session.session_id
                    ),
                    None => format!(
                        "persist agent switch for session `{}` failed: {source}",
                        interaction_session.session_id
                    ),
                },
            });
        }
        // Apply non-principal session changes first, but keep the live role
        // and identity on the old principal until `replace_model` has detached
        // the runtime-control route and failed the recall bundle closed.
        let mut attached_candidate = candidate;
        attached_candidate.role = previous_session.role.clone();
        attached_candidate.agent_definition_key = previous_session.agent_definition_key.clone();
        if let Some(runtime) = self.ready_runtime()
            && let Err(error) = runtime.replace_session(attached_candidate)
        {
            let rollback = store.save_session(&previous_session).err();
            return Err(CliError::Parse {
                detail: match rollback {
                    Some(rollback) => format!(
                        "attach agent switch for session `{}` failed: {error}; restoring the previous canonical session also failed: {rollback}",
                        interaction_session.session_id
                    ),
                    None => format!(
                        "attach agent switch for session `{}` failed: {error}",
                        interaction_session.session_id
                    ),
                },
            });
        }

        match self.switch_agent_definition(data_dir, definition, daemon_connected, true) {
            Ok(info) => Ok(info),
            Err(error) => {
                let mut rollback_errors = Vec::new();
                if let Some(runtime) = self.ready_runtime()
                    && let Err(rollback) = runtime.replace_session(previous_session.clone())
                {
                    rollback_errors.push(format!("restore live session: {rollback}"));
                }
                if let Err(rollback) = store.save_session(&previous_session) {
                    rollback_errors.push(format!("restore canonical session: {rollback}"));
                }
                if rollback_errors.is_empty() {
                    Err(error)
                } else {
                    Err(CliError::Parse {
                        detail: format!(
                            "agent switch failed: {error}; rollback failed: {}",
                            rollback_errors.join("; ")
                        ),
                    })
                }
            }
        }
    }

    fn switch_to_selection(
        &mut self,
        data_dir: &SinomoDataDir,
        selection: TuiModelSelection,
        daemon_connected: bool,
    ) -> Result<TuiAgentRuntimeInfo, CliError> {
        self.switch_to_selection_with_agent_rebind(data_dir, selection, daemon_connected, None)
    }

    fn switch_to_selection_with_agent_rebind(
        &mut self,
        data_dir: &SinomoDataDir,
        selection: TuiModelSelection,
        daemon_connected: bool,
        recall_transition: Option<&AgentRecallToolTransition>,
    ) -> Result<TuiAgentRuntimeInfo, CliError> {
        with_tui_model_config_mode(daemon_connected, || {
            let (runtime_config, _) = load_model_runtime_config_for_node(data_dir)?;
            match selection.provider.as_str() {
                RUST_MINICPM_V46_PROVIDER_ID => {
                    let info = DefaultMainAgentChatModel::runtime_info_for_config(&runtime_config);
                    let model = DefaultMainAgentChatModel::from_runtime_config(
                        data_dir,
                        runtime_config,
                        self.thinking.clone(),
                    );
                    self.replace_model(
                        model,
                        info.provider,
                        info.model,
                        info.account,
                        LanguageModelProviderOptions::default(),
                        recall_transition,
                    )
                }
                MINICPM_PROVIDER_ID => {
                    let model_name = if selection.model == sinomo_lm::PROVIDER_DEFAULT_MODEL {
                        MINICPM_DEFAULT_MODEL.to_string()
                    } else {
                        selection.model
                    };
                    let model = DefaultMainAgentChatModel::configured(
                        data_dir,
                        MINICPM_PROVIDER_ID.to_string(),
                        model_name.clone(),
                        None,
                        runtime_config,
                    );
                    self.replace_model(
                        model,
                        MINICPM_PROVIDER_ID.to_string(),
                        model_name,
                        None,
                        LanguageModelProviderOptions::default(),
                        recall_transition,
                    )
                }
                provider => {
                    let (config, _) = load_model_config_for_node(data_dir)?;
                    let account =
                        provider_account_config(&config, provider, Some(&selection.account))
                            .cloned();
                    let options = provider_options_for_model(account.as_ref(), &selection.model);
                    let model = DefaultMainAgentChatModel::configured(
                        data_dir,
                        provider.to_string(),
                        selection.model.clone(),
                        account,
                        runtime_config,
                    );
                    self.replace_model(
                        model,
                        provider.to_string(),
                        selection.model,
                        Some(selection.account),
                        options,
                        recall_transition,
                    )
                }
            }
        })
    }

    fn configure_tui_tool_runtime_for_session(
        &mut self,
        data_dir: &SinomoDataDir,
        session: &InteractionSession,
        sandbox_node_id: Option<NodeId>,
        workspaces: Option<&[WorkspaceRef]>,
        daemon_connected: bool,
    ) -> Result<(), CliError> {
        // The TUI configures tools before the first model turn, while the
        // handler may still contain only a pending model. Materialize the
        // runtime first so the workspace binding is part of the canonical
        // AgentSession from the beginning.
        self.ensure_runtime(session)?;
        let snapshot = self
            .agent_session_snapshot()?
            .ok_or_else(|| CliError::Parse {
                detail: format!(
                    "agent runtime has no session `{}` to bind",
                    session.session_id
                ),
            })?;
        let requested_workspace_root = if workspaces.is_none()
            && snapshot.workspace_binding.is_none()
            && agent_definition_has_workspace_mutation_authority(&self.agent_definition)
        {
            self.workspace_root.clone()
        } else {
            None
        };
        self.configure_app_tool_runtime_for_session(
            data_dir,
            &session.session_id,
            sandbox_node_id,
            workspaces,
            requested_workspace_root.as_deref(),
            daemon_connected,
        )?;
        Ok(())
    }

    pub(crate) fn configure_app_tool_runtime_for_session(
        &mut self,
        data_dir: &SinomoDataDir,
        session_id: &SessionId,
        sandbox_node_id: Option<NodeId>,
        conversation_workspaces: Option<&[WorkspaceRef]>,
        requested_workspace_root: Option<&Path>,
        daemon_connected: bool,
    ) -> Result<Option<PathBuf>, CliError> {
        let mut session = self
            .agent_session_snapshot()?
            .ok_or_else(|| CliError::Parse {
                detail: format!("agent runtime has no session `{session_id}` to bind"),
            })?;
        if &session.session_id != session_id {
            return Err(CliError::Parse {
                detail: format!(
                    "agent runtime session `{}` does not match workspace request for `{session_id}`",
                    session.session_id
                ),
            });
        }
        if let Some(runtime) = self.ready_runtime()
            && runtime.active_turn()?.is_some()
        {
            return Err(CliError::Parse {
                detail: format!(
                    "cannot reconfigure workspace or sandbox grants for agent session `{session_id}` while a turn is active"
                ),
            });
        }

        let requested_binding = if conversation_workspaces.is_none()
            && session.workspace_binding.is_none()
        {
            requested_workspace_root
                .map(|root| {
                    let metadata = std::fs::metadata(root).map_err(|source| CliError::Io {
                        path: root.display().to_string(),
                        detail: format!("workspace directory is unavailable: {source}"),
                    })?;
                    if !metadata.is_dir() {
                        return Err(CliError::Parse {
                            detail: format!(
                                "workspace path is not a directory: {}",
                                root.display()
                            ),
                        });
                    }
                    if agent_definition_has_workspace_mutation_authority(&self.agent_definition) {
                        mutating_agent_workspace_binding(data_dir, root)
                    } else {
                        workspace_ref_for_selected_root(data_dir, root)
                    }
                })
                .transpose()?
                .map(|workspace| workspace.binding().clone())
        } else {
            None
        };
        let workspace_bindings = if let Some(conversation_workspaces) = conversation_workspaces {
            conversation_workspaces.to_vec()
        } else if let Some(legacy_workspace) = session.workspace_binding.clone() {
            // A legacy conversation has no grant projection yet. Its canonical
            // session shadow belongs to this chat and takes precedence over a
            // process-local selection retained from another chat/device.
            vec![legacy_workspace]
        } else if let Some(requested) = requested_binding {
            vec![requested]
        } else {
            Vec::new()
        };

        // Keep the legacy field as a compatibility snapshot only. It mirrors
        // the conversation's primary grant and never vetoes a conversation
        // workspace change.
        session.workspace_binding = workspace_bindings.first().cloned();

        if session.sandbox_attachment.is_none() {
            let sandbox_server_id = sandbox_node_id
                .as_ref()
                .map(ToString::to_string)
                .or_else(|| {
                    read_cluster_state(&ClusterPaths::new(data_dir))
                        .ok()
                        .map(|state| state.local_node_id.to_string())
                })
                .unwrap_or_else(|| "local-daemon".to_string());
            session
                .attach_sandbox(sinomo_agent::AgentSessionSandboxAttachment {
                    sandbox_id: sinomo_agent::local_workspace::agent_sandbox_id_for_session(
                        session_id,
                    )
                    .to_string(),
                    server_id: sandbox_server_id,
                    attachment_kind: sinomo_agent::AgentSessionSandboxAttachmentKind::Automatic,
                })
                .map_err(|source| CliError::Parse {
                    detail: format!("attach default sandbox to agent session: {source}"),
                })?;
        }

        let resolved_workspaces = workspace_bindings
            .iter()
            .map(|binding| workspace_binding_root_for_data_dir(data_dir, session_id, binding))
            .collect::<Result<Vec<_>, _>>()?;
        let resolved_workspace_roots = resolved_workspaces
            .iter()
            .map(|workspace| workspace.root().to_path_buf())
            .collect::<Vec<_>>();
        let selected_root = resolved_workspace_roots.first().cloned();
        let registry_adapter_root = selected_root
            .clone()
            .map(Ok)
            .unwrap_or_else(|| empty_workspace_context_root(session_id))?;

        let mut previous_workspace_root = None;
        if self.workspace_root != selected_root {
            let previous_root = std::mem::replace(&mut self.workspace_root, selected_root.clone());
            let selection = TuiModelSelection {
                provider: self.provider.clone(),
                model: self.model_name.clone(),
                account: self.account.clone().unwrap_or_default(),
            };
            if let Err(error) = self.switch_to_selection(data_dir, selection, daemon_connected) {
                self.workspace_root = previous_root;
                return Err(error);
            }
            previous_workspace_root = Some(previous_root);
        }
        if let Err(error) = configure_agent_tool_registry_for_workspace_projection(
            &self.tool_registry,
            data_dir,
            &registry_adapter_root,
            &workspace_bindings,
            &resolved_workspace_roots,
            &self.agent_definition,
            Some(session_id),
            sandbox_node_id,
            agent_definition_has_workspace_mutation_authority(&self.agent_definition),
        ) {
            if let Some(previous_root) = previous_workspace_root {
                self.workspace_root = previous_root;
            }
            return Err(error);
        }
        if let Some(runtime) = self.ready_runtime()
            && let Err(error) = runtime.replace_session(session)
        {
            if let Some(previous_root) = previous_workspace_root {
                self.workspace_root = previous_root;
            }
            return Err(error.into());
        }
        self.persist_attached_agent_session(data_dir, session_id)?;
        Ok(selected_root)
    }
}

fn empty_workspace_context_root(session_id: &SessionId) -> Result<PathBuf, CliError> {
    let root = env::temp_dir()
        .join("sinomo-agent")
        .join("empty-workspace-context")
        .join(session_id.to_string());
    fs::create_dir_all(&root).map_err(|source| CliError::Io {
        path: root.display().to_string(),
        detail: format!("create empty workspace context: {source}"),
    })?;
    fs::canonicalize(&root).map_err(|source| CliError::Io {
        path: root.display().to_string(),
        detail: format!("resolve empty workspace context: {source}"),
    })
}

/// Plain-data view of the active runtime selection, for the GUI app.
pub(crate) struct AppRuntimeInfo {
    pub agent: String,
    pub provider: String,
    pub model: String,
    pub account: Option<String>,
    pub reasoning_effort: Option<String>,
}

impl From<&TuiAgentRuntimeInfo> for AppRuntimeInfo {
    fn from(info: &TuiAgentRuntimeInfo) -> Self {
        AppRuntimeInfo {
            agent: info.agent.clone(),
            provider: info.provider.clone(),
            model: info.model.clone(),
            account: info.account.clone(),
            reasoning_effort: info
                .reasoning_effort
                .map(|e| e.as_config_value().to_string()),
        }
    }
}

/// Build the main-agent chat handler the GUI app drives (mirrors the TUI path).
pub(crate) fn app_build_chat_handler(
    data_dir: &SinomoDataDir,
    daemon_connected: bool,
    runtime_skill_catalog_vfs: Option<sinomo_runtime::vfs::VfsHandle>,
) -> Result<RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel>, CliError> {
    let (runtime_config, _) = load_tui_model_runtime_config(data_dir, daemon_connected)?;
    let runtime_skill_catalog = match (daemon_connected, runtime_skill_catalog_vfs) {
        (true, Some(vfs)) => {
            load_runtime_skill_catalog_from_vfs(vfs).map_err(skill_catalog_cli_error)?
        }
        (true, None) => {
            return Err(CliError::DaemonNotRunning {
                detail: "GUI chat skill loading requires the resident main daemon SIFS transport"
                    .to_string(),
            });
        }
        (false, _) => RuntimeSkillCatalog::default(),
    };
    RuntimeMainAgentInteractionHandler::from_default_model_config_with_runtime_skill_catalog(
        data_dir,
        runtime_config,
        daemon_connected,
        runtime_skill_catalog,
    )
}

#[cfg(test)]
pub(crate) fn app_runtime_skill_catalog_for_test(
    handler: &RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel>,
) -> RuntimeSkillCatalog {
    (
        handler.skill_catalog.clone(),
        handler.skill_catalog_texts.clone(),
        handler.skill_catalog_resources.clone(),
    )
}

/// Switch the active provider/model/account on a GUI chat handler.
pub(crate) fn app_switch_model(
    handler: &mut RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel>,
    data_dir: &SinomoDataDir,
    provider: String,
    model: String,
    account: String,
    daemon_connected: bool,
) -> Result<AppRuntimeInfo, CliError> {
    let info = handler.switch_tui_model(
        data_dir,
        TuiModelSelection {
            provider,
            model,
            account,
        },
        daemon_connected,
    )?;
    Ok((&info).into())
}

/// Switch the active agent definition on a GUI chat handler.
pub(crate) fn app_switch_agent(
    handler: &mut RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel>,
    data_dir: &SinomoDataDir,
    definition: AgentDefinition,
    daemon_connected: bool,
) -> Result<AppRuntimeInfo, CliError> {
    let info = handler.switch_tui_agent(data_dir, daemon_connected, definition)?;
    Ok((&info).into())
}

/// Attach a durable GUI session after switching its validated agent identity,
/// and commit that agent's recall principal before returning to the app.
pub(crate) fn app_restore_agent_session(
    handler: &mut RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel>,
    data_dir: &SinomoDataDir,
    session: &InteractionSession,
    persisted: Option<AgentSession>,
    transcript: sinomo_agent::AgentTranscript,
) -> Result<(), CliError> {
    handler.restore_tui_agent_session(data_dir, session, persisted, transcript)
}

/// Read the current runtime selection of a GUI chat handler.
pub(crate) fn app_runtime_info(
    handler: &RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel>,
) -> AppRuntimeInfo {
    (&handler.runtime_info()).into()
}

#[cfg(test)]
pub(crate) fn execute_tui_workspace_tool(
    handler: &RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel>,
    session: &InteractionSession,
    call_index: usize,
    tool: &str,
    input: serde_json::Value,
) -> sinomo_agent::AgentToolOutcome {
    handler.tool_registry.execute(
        session,
        0,
        call_index,
        &sinomo_agent::AgentToolCall {
            call_id: None,
            tool: tool.to_string(),
            input,
            r#async: false,
            summary: format!("test {tool}"),
        },
    )
}

#[cfg(test)]
#[test]
fn tui_grants_workspace_mutation_to_builtin_main_and_coding_agents() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-tui-workspace-mutation-authority");
    let data_dir = SinomoDataDir::new(root.join("data"));
    let mut local_permissions = sinomo_runtime::node::LocalNodePermissions::new();
    local_permissions.set_remote_management_enabled(true);
    sinomo_runtime::node::LocalNodePermissionStore::new(data_dir.root())
        .save(&local_permissions)
        .expect("grant local host-command permission for the workspace-agent fixture");
    let workspace = root.join("workspace");
    fs::create_dir_all(&workspace).expect("create workspace");
    let workspace = fs::canonicalize(workspace).expect("canonical workspace");
    let (mut handler, _) =
        runtime_main_agent_handler_for_tui_with_data_dir(None, &data_dir).expect("TUI handler");
    handler.workspace_root = Some(workspace.clone());
    handler
        .set_agent_definition(AgentDefinition::coding())
        .expect("select coding agent");
    let session = InteractionSession::new(
        SessionId::generate(),
        NodeId::from_legacy_named("node_tui_workspace_mutation_authority").expect("node id"),
        InteractionSurface::new("test.tui", InteractionSurfaceKind::CommandLine),
    );

    handler
        .configure_tui_tool_runtime_for_session(&data_dir, &session, None, None, false)
        .expect("configure coding tools");
    let write = execute_tui_workspace_tool(
        &handler,
        &session,
        0,
        "write_file",
        serde_json::json!({ "path": "written.txt", "content": "written\n" }),
    );
    let patch = execute_tui_workspace_tool(
        &handler,
        &session,
        1,
        "apply_patch",
        serde_json::json!({
            "patch": "*** Begin Patch\n*** Add File: patched.txt\n+patched\n*** End Patch"
        }),
    );
    let command = execute_tui_workspace_tool(
        &handler,
        &session,
        2,
        "run_command",
        serde_json::json!({ "command": "printf shell-ok > shell.txt" }),
    );
    assert_eq!(write.status, sinomo_agent::AgentToolStatus::Succeeded);
    assert_eq!(patch.status, sinomo_agent::AgentToolStatus::Succeeded);
    assert_eq!(command.status, sinomo_agent::AgentToolStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(workspace.join("written.txt")).expect("written file"),
        "written\n"
    );
    assert_eq!(
        fs::read_to_string(workspace.join("patched.txt")).expect("patched file"),
        "patched\n"
    );
    assert_eq!(
        fs::read_to_string(workspace.join("shell.txt")).expect("shell output"),
        "shell-ok"
    );
    let snapshot = handler
        .agent_session_snapshot()
        .expect("session snapshot")
        .expect("attached session");
    assert_eq!(
        snapshot
            .workspace_binding
            .as_ref()
            .and_then(|binding| binding.local_root.as_deref()),
        Some(workspace.as_path())
    );
    let persisted = FileAgentSessionStore::for_data_dir(&data_dir)
        .load_session(&session.session_id)
        .expect("load configured workspace session")
        .expect("configured workspace session");
    assert_eq!(persisted.workspace_binding, snapshot.workspace_binding);

    handler
        .set_agent_definition(AgentDefinition::main())
        .expect("select main agent");
    handler
        .configure_tui_tool_runtime_for_session(&data_dir, &session, None, None, false)
        .expect("configure main-agent tools");
    let main_write = execute_tui_workspace_tool(
        &handler,
        &session,
        3,
        "write_file",
        serde_json::json!({ "path": "main-written.txt", "content": "main\n" }),
    );
    let main_patch = execute_tui_workspace_tool(
        &handler,
        &session,
        4,
        "apply_patch",
        serde_json::json!({
            "patch": "*** Begin Patch\n*** Add File: main-patched.txt\n+main-patched\n*** End Patch"
        }),
    );
    let main_command = execute_tui_workspace_tool(
        &handler,
        &session,
        5,
        "run_command",
        serde_json::json!({ "command": "printf main-shell > main-shell.txt" }),
    );
    assert_eq!(main_write.status, sinomo_agent::AgentToolStatus::Succeeded);
    assert_eq!(main_patch.status, sinomo_agent::AgentToolStatus::Succeeded);
    assert_eq!(
        main_command.status,
        sinomo_agent::AgentToolStatus::Succeeded
    );
    assert_eq!(
        fs::read_to_string(workspace.join("main-written.txt")).expect("main-written file"),
        "main\n"
    );
    assert_eq!(
        fs::read_to_string(workspace.join("main-patched.txt")).expect("main-patched file"),
        "main-patched\n"
    );
    assert_eq!(
        fs::read_to_string(workspace.join("main-shell.txt")).expect("main shell output"),
        "main-shell"
    );

    handler
        .set_agent_definition(AgentDefinition::custom(
            "custom-main",
            AgentRole::Main,
            "Custom main-role agent.",
        ))
        .expect("select custom main-role agent");
    handler
        .configure_tui_tool_runtime_for_session(&data_dir, &session, None, None, false)
        .expect("configure custom main-role tools");
    for (call_index, tool, input) in [
        (
            6,
            "write_file",
            serde_json::json!({ "path": "blocked-write.txt", "content": "blocked" }),
        ),
        (
            7,
            "apply_patch",
            serde_json::json!({
                "patch": "*** Begin Patch\n*** Add File: blocked-patch.txt\n+blocked\n*** End Patch"
            }),
        ),
        (
            8,
            "run_command",
            serde_json::json!({ "command": "printf blocked" }),
        ),
    ] {
        let outcome = execute_tui_workspace_tool(&handler, &session, call_index, tool, input);
        assert_eq!(
            outcome.status,
            sinomo_agent::AgentToolStatus::Blocked,
            "custom main-role agent unexpectedly executed {tool}"
        );
    }
}

#[cfg(test)]
#[test]
fn tui_tool_runtime_legacy_fallback_uses_the_session_workspace_binding() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-tui-resume-workspace");
    let data_dir = SinomoDataDir::new(root.join("data"));
    let original_workspace = root.join("original-workspace");
    let process_workspace = root.join("different-process-workspace");
    fs::create_dir_all(&original_workspace).expect("create original workspace");
    fs::create_dir_all(&process_workspace).expect("create process workspace");
    let original_workspace =
        fs::canonicalize(original_workspace).expect("canonical original workspace");
    let process_workspace =
        fs::canonicalize(process_workspace).expect("canonical process workspace");
    let (mut handler, _) =
        runtime_main_agent_handler_for_tui_with_data_dir(None, &data_dir).expect("TUI handler");
    handler.workspace_root = Some(process_workspace.clone());
    handler
        .set_agent_definition(AgentDefinition::coding())
        .expect("select coding agent");
    let interaction_session = InteractionSession::new(
        SessionId::generate(),
        NodeId::from_legacy_named("node_tui_resume_workspace").expect("node id"),
        InteractionSurface::new("test.tui", InteractionSurfaceKind::CommandLine),
    );
    let mut agent_session = AgentSession::new(
        interaction_session.session_id.clone(),
        AgentRole::Deliberation,
    )
    .with_agent_definition_key("coding-agent");
    agent_session.workspace_binding = Some(WorkspaceRef::local_path(original_workspace.clone()));
    handler
        .replace_full_agent_session(&interaction_session, agent_session)
        .expect("restore workspace-bound session");

    handler
        .configure_app_tool_runtime_for_session(
            &data_dir,
            &interaction_session.session_id,
            None,
            None,
            Some(&process_workspace),
            false,
        )
        .expect("configure resumed coding tools");

    assert_eq!(
        handler.workspace_root.as_deref(),
        Some(original_workspace.as_path())
    );
    let write = execute_tui_workspace_tool(
        &handler,
        &interaction_session,
        0,
        "write_file",
        serde_json::json!({ "path": "resumed.txt", "content": "resumed\n" }),
    );
    assert_eq!(write.status, sinomo_agent::AgentToolStatus::Succeeded);
    assert_eq!(
        fs::read_to_string(original_workspace.join("resumed.txt")).expect("resumed write"),
        "resumed\n"
    );
}

#[cfg(test)]
#[test]
fn tui_tool_runtime_ignores_stale_requested_root_when_conversation_is_authoritative() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-tui-conversation-workspace-authority");
    let data_dir = SinomoDataDir::new(root.join("data"));
    let conversation_workspace = root.join("conversation-workspace");
    let stale_requested_workspace = root.join("missing-stale-workspace");
    fs::create_dir_all(&conversation_workspace).expect("create conversation workspace");
    let conversation_workspace =
        fs::canonicalize(conversation_workspace).expect("canonical conversation workspace");
    let (mut handler, _) =
        runtime_main_agent_handler_for_tui_with_data_dir(None, &data_dir).expect("TUI handler");
    handler
        .set_agent_definition(AgentDefinition::coding())
        .expect("select coding agent");
    let interaction_session = InteractionSession::new(
        SessionId::generate(),
        NodeId::from_legacy_named("node_tui_workspace_authority").expect("node id"),
        InteractionSurface::new("test.tui", InteractionSurfaceKind::CommandLine),
    );
    handler
        .replace_agent_session(&interaction_session, sinomo_agent::AgentTranscript::new())
        .expect("attach agent session");
    let workspaces = vec![WorkspaceRef::local_path(conversation_workspace.clone())];

    let selected = handler
        .configure_app_tool_runtime_for_session(
            &data_dir,
            &interaction_session.session_id,
            None,
            Some(&workspaces),
            Some(&stale_requested_workspace),
            false,
        )
        .expect("conversation workspace must bypass stale requested root");

    assert_eq!(selected.as_deref(), Some(conversation_workspace.as_path()));
    assert_eq!(
        handler.workspace_root.as_deref(),
        Some(conversation_workspace.as_path())
    );
}

#[cfg(test)]
#[test]
fn tui_tool_runtime_projects_ordered_additional_workspace_access() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-tui-ordered-workspaces");
    let data_dir = SinomoDataDir::new(root.join("data"));
    let primary = root.join("primary");
    let secondary = root.join("secondary");
    fs::create_dir_all(&primary).expect("create primary workspace");
    fs::create_dir_all(&secondary).expect("create secondary workspace");
    fs::write(secondary.join("secondary.txt"), "additional workspace\n")
        .expect("write additional workspace file");
    let primary = fs::canonicalize(primary).expect("canonical primary workspace");
    let secondary = fs::canonicalize(secondary).expect("canonical secondary workspace");
    let (mut handler, _) =
        runtime_main_agent_handler_for_tui_with_data_dir(None, &data_dir).expect("TUI handler");
    handler
        .set_agent_definition(AgentDefinition::coding())
        .expect("select coding agent");
    let interaction_session = InteractionSession::new(
        SessionId::generate(),
        NodeId::from_legacy_named("node_tui_ordered_workspaces").expect("node id"),
        InteractionSurface::new("test.tui", InteractionSurfaceKind::CommandLine),
    );
    handler
        .replace_agent_session(&interaction_session, sinomo_agent::AgentTranscript::new())
        .expect("attach agent session");
    let workspaces = vec![
        WorkspaceRef::local_path(primary.clone()),
        WorkspaceRef::local_path(secondary.clone()),
    ];

    handler
        .configure_app_tool_runtime_for_session(
            &data_dir,
            &interaction_session.session_id,
            None,
            Some(&workspaces),
            None,
            false,
        )
        .expect("project ordered workspaces");
    let read = execute_tui_workspace_tool(
        &handler,
        &interaction_session,
        0,
        "read_file",
        serde_json::json!({ "path": secondary.join("secondary.txt") }),
    );

    assert_eq!(handler.workspace_root.as_deref(), Some(primary.as_path()));
    assert_eq!(read.status, sinomo_agent::AgentToolStatus::Succeeded);
    assert_eq!(
        read.output
            .get("content")
            .and_then(serde_json::Value::as_str),
        Some("additional workspace\n")
    );
}

#[cfg(test)]
#[test]
fn tui_tool_runtime_treats_authoritative_empty_workspaces_as_revocation() {
    let root = sinomo_testdir::TestDir::new("sinomo-cli-tui-empty-workspace-authority");
    let data_dir = SinomoDataDir::new(root.join("data"));
    let stale_requested_workspace = root.join("missing-stale-workspace");
    let (mut handler, _) =
        runtime_main_agent_handler_for_tui_with_data_dir(None, &data_dir).expect("TUI handler");
    handler
        .set_agent_definition(AgentDefinition::coding())
        .expect("select coding agent");
    let interaction_session = InteractionSession::new(
        SessionId::generate(),
        NodeId::from_legacy_named("node_tui_empty_workspace_authority").expect("node id"),
        InteractionSurface::new("test.tui", InteractionSurfaceKind::CommandLine),
    );
    let mut agent_session = AgentSession::new(
        interaction_session.session_id.clone(),
        AgentRole::Deliberation,
    )
    .with_agent_definition_key("coding-agent");
    agent_session.workspace_binding = Some(WorkspaceRef::local_path("/legacy/workspace"));
    handler
        .replace_full_agent_session(&interaction_session, agent_session)
        .expect("restore legacy workspace shadow");

    let selected = handler
        .configure_app_tool_runtime_for_session(
            &data_dir,
            &interaction_session.session_id,
            None,
            Some(&[]),
            Some(&stale_requested_workspace),
            false,
        )
        .expect("empty authoritative projection must ignore legacy and requested roots");

    assert_eq!(selected, None);
    assert_eq!(handler.workspace_root, None);
    assert!(
        !handler.tool_registry.contains("read_file"),
        "revoking the last workspace removes workspace tools instead of retaining the old root"
    );
    assert_eq!(
        handler
            .agent_session_snapshot()
            .expect("snapshot")
            .expect("session")
            .workspace_binding,
        None
    );
}

impl TuiAgentRuntimeControl for RuntimeMainAgentInteractionHandler<DefaultMainAgentChatModel> {
    fn tui_agent_runtime_info(&self) -> TuiAgentRuntimeInfo {
        self.runtime_info()
    }

    fn drain_tui_runtime_events(&mut self) -> Result<Vec<AgentRuntimeEvent>, CliError> {
        self.drain_runtime_events()
    }

    fn interrupt_tui_agent(&mut self, reason: &str) -> Result<bool, CliError> {
        let Some(runtime) = self.ready_runtime() else {
            return Ok(false);
        };
        if runtime.active_turn()?.is_none() {
            return Ok(false);
        }
        let result = runtime.interrupt_active_turn(reason)?;
        Ok(result.accepted)
    }

    fn switch_tui_model(
        &mut self,
        data_dir: &SinomoDataDir,
        selection: TuiModelSelection,
        daemon_connected: bool,
    ) -> Result<TuiAgentRuntimeInfo, CliError> {
        self.switch_to_selection(data_dir, selection, daemon_connected)
    }

    fn switch_tui_agent(
        &mut self,
        data_dir: &SinomoDataDir,
        daemon_connected: bool,
        definition: AgentDefinition,
    ) -> Result<TuiAgentRuntimeInfo, CliError> {
        self.switch_agent_definition(data_dir, definition, daemon_connected, true)
    }

    fn switch_tui_agent_for_session(
        &mut self,
        data_dir: &SinomoDataDir,
        session: &InteractionSession,
        workspaces: Option<&[WorkspaceRef]>,
        daemon_connected: bool,
        definition: AgentDefinition,
    ) -> Result<TuiAgentRuntimeInfo, CliError> {
        self.switch_agent_definition_for_session(
            data_dir,
            session,
            workspaces,
            definition,
            daemon_connected,
        )
    }

    fn prepare_tui_agent(
        &mut self,
        data_dir: &SinomoDataDir,
        daemon_connected: bool,
        definition: AgentDefinition,
    ) -> Result<TuiAgentRuntimeInfo, CliError> {
        // Session restore preparation is externally observable through runtime
        // control, so publish the target definition, session identity, and
        // recall bundle together instead of retaining the old principal.
        self.switch_agent_definition(data_dir, definition, daemon_connected, true)
    }

    fn snapshot_tui_resume(
        &mut self,
        session: &InteractionSession,
    ) -> Result<Option<TuiAgentResumeSnapshot>, CliError> {
        self.transition_snapshot(session).map(Some)
    }

    fn rollback_tui_resume(
        &mut self,
        data_dir: &SinomoDataDir,
        snapshot: Option<TuiAgentResumeSnapshot>,
    ) -> Result<(), CliError> {
        match snapshot {
            Some(snapshot) => self.restore_transition_snapshot(data_dir, snapshot),
            None => Ok(()),
        }
    }

    fn load_tui_agent_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<AgentSession>, CliError> {
        self.load_persisted_agent_session(session_id)
    }

    fn persist_tui_agent_session(
        &mut self,
        data_dir: &SinomoDataDir,
        session: &InteractionSession,
    ) -> Result<(), CliError> {
        self.ensure_runtime(session)?;
        self.persist_attached_agent_session(data_dir, &session.session_id)
    }

    fn configure_tui_tool_runtime(
        &mut self,
        data_dir: &SinomoDataDir,
        session: &InteractionSession,
        sandbox_node_id: Option<NodeId>,
        workspaces: Option<&[WorkspaceRef]>,
        daemon_connected: bool,
    ) -> Result<(), CliError> {
        self.configure_tui_tool_runtime_for_session(
            data_dir,
            session,
            sandbox_node_id,
            workspaces,
            daemon_connected,
        )
    }

    fn restore_tui_agent_session(
        &mut self,
        data_dir: &SinomoDataDir,
        session: &InteractionSession,
        persisted: Option<AgentSession>,
        transcript: sinomo_agent::AgentTranscript,
    ) -> Result<(), CliError> {
        // `prepare_tui_agent` has already published the target principal and
        // recall bundle. Stage once more before attaching the durable session
        // so a missing SIFS/history backend fails before its route is changed.
        let recall_tools = agent_recall_tool_registry(data_dir, &self.agent_definition)?;
        match persisted {
            Some(persisted) => self.replace_full_agent_session(session, persisted),
            None => self.replace_agent_session(session, transcript),
        }?;
        self.tool_registry
            .replace_registered_tools_from(&recall_tools);
        self.persist_attached_agent_session(data_dir, &session.session_id)
    }
}

impl<M> InteractionEventHandler for RuntimeMainAgentInteractionHandler<M>
where
    M: InteractionModel + Send + Sync + 'static,
{
    fn handle_event<S: EventStore>(
        &mut self,
        store: &mut S,
        session: &InteractionSession,
        event: &EventEnvelope,
    ) -> Result<Vec<InteractionOutput>, CliError> {
        self.handle_runtime_main_agent_event(store, session, event)
    }

    fn stable_chat_agent_key(&self) -> Option<String> {
        Some(self.agent_definition.key().to_string())
    }

    fn join_stable_chat_turn(&mut self) -> Result<(), CliError> {
        if let Some(runtime) = self.ready_runtime() {
            runtime.join_active_turn()?;
        }
        Ok(())
    }

    fn drain_stable_chat_runtime_events(&mut self) -> Result<Vec<AgentRuntimeEvent>, CliError> {
        self.drain_runtime_events()
    }
}

impl<M> RuntimeMainAgentInteractionHandler<M>
where
    M: InteractionModel + Send + Sync + 'static,
{
    fn handle_runtime_main_agent_event<S: EventStore>(
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
        let mut context_attachments = match event.payload.get("context_attachments") {
            Some(value) => {
                serde_json::from_value::<Vec<sinomo_agent::AgentContextAttachment>>(value.clone())
                    .map_err(|source| CliError::Parse {
                    detail: format!("parse context attachments failed: {source}"),
                })?
            }
            None => Vec::new(),
        };
        let mut transient_system_context = event
            .payload
            .get("transient_system_context")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|context| !context.is_empty())
            .map(str::to_string);
        let mut message_cause_event_id = event.event_id.clone();
        let mut explicit_skill_name = None;

        if let Some(args) = parse_slash_command(text)? {
            if let Some(output) =
                handle_thinking_command(store, session, event, &args, &self.thinking)?
            {
                return Ok(output);
            }
            if let Some(command) = parse_explicit_chat_skill_command(&args) {
                let skill = self
                    .ensure_runtime(session)?
                    .load_explicit_skill_context(&command.skill_id)
                    .map_err(|error| CliError::Parse {
                        detail: error.to_string(),
                    })?;
                transient_system_context = Some(merge_transient_system_context(
                    transient_system_context,
                    render_explicit_skill_turn_context(&skill, command.request.is_some()),
                ));
                if let Some(request) = command.request.as_deref() {
                    context_attachments
                        .push(explicit_skill_request_attachment(&skill.name, request));
                }
                let activated = interaction_event(
                    session,
                    "interaction.skill.activated",
                    serde_json::json!({
                        "input_event_id": event.event_id,
                        "skill": skill.name,
                        "description": skill.description,
                        "resources": skill.resources,
                        "mode": "explicit-slash",
                        "scope": "turn"
                    }),
                )
                .caused_by(&event.event_id);
                store.append(activated.clone())?;
                message_cause_event_id = activated.event_id;
                explicit_skill_name = Some(skill.name);
            } else {
                return handle_slash_command(store, session, event, args);
            }
        }

        let message = interaction_event(
            session,
            "interaction.message.received",
            serde_json::json!({
                "input_event_id": event.event_id,
                "agent_item_id": event.payload.get("agent_item_id").cloned(),
                "text": text,
                "context_attachments": context_attachments.clone(),
                "explicit_skill": explicit_skill_name,
                "state": "received",
                "delivery": "event-stream"
            }),
        )
        .caused_by(&message_cause_event_id);
        store.append(message.clone())?;

        let input = AgentLoopInput {
            session: session.clone(),
            latest_event: message.clone(),
            transient_system_context,
            async_update: None,
            context_attachments,
            tool_specs: Vec::new(),
        };
        let requested = interaction_event(
            session,
            "interaction.main_agent.requested",
            serde_json::json!({
                "message_event_id": message.event_id,
                "input_event_id": event.event_id,
                "explicit_skill": explicit_skill_name,
                "runtime": "agent-session-runtime"
            }),
        )
        .caused_by(&message.event_id);
        store.append(requested.clone())?;

        let runtime = self.ensure_runtime(session)?;
        let result = runtime.submit_op(AgentOp::start_turn(input))?;
        let queued = interaction_event(
            session,
            "interaction.main_agent.queued",
            serde_json::json!({
                "message_event_id": message.event_id,
                "request_event_id": requested.event_id,
                "sequence": result.sequence,
                "accepted": result.accepted,
                "op_id": result
                    .events
                    .first()
                    .map(|event| event.op_id.to_string())
            }),
        )
        .caused_by(&requested.event_id);
        store.append(queued)?;

        if result.accepted {
            Ok(Vec::new())
        } else {
            let reason = result
                .events
                .iter()
                .find_map(|event| match &event.payload {
                    AgentEventPayload::OperationRejected { reason, .. } => Some(reason.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| "agent turn was rejected".to_string());
            Ok(vec![InteractionOutput::Line(format!(
                "agent task rejected: {reason}"
            ))])
        }
    }
}

fn with_tui_model_config_mode<T>(
    daemon_connected: bool,
    f: impl FnOnce() -> Result<T, CliError>,
) -> Result<T, CliError> {
    if daemon_connected || cli_force_local_mode() {
        return f();
    }

    let _guard = enter_cli_force_local_mode(true);
    f()
}

fn default_main_agent_chat_model_provider() -> &'static str {
    #[cfg(feature = "native-models")]
    {
        RUST_MINICPM_V46_PROVIDER_ID
    }
    #[cfg(not(feature = "native-models"))]
    {
        MINICPM_PROVIDER_ID
    }
}

fn default_main_agent_chat_model_id() -> &'static str {
    #[cfg(feature = "native-models")]
    {
        MINICPM_V46_DEFAULT_MODEL_ID
    }
    #[cfg(not(feature = "native-models"))]
    {
        MINICPM_DEFAULT_MODEL
    }
}

#[cfg(feature = "native-models")]
pub struct RustMiniCpmV46ChatModel {
    model_dir: PathBuf,
    max_input_tokens_override: Option<usize>,
    max_output_tokens_override: Option<usize>,
    thinking: ThinkingControl,
    prompt_renderer: MiniCpmV46PromptRenderer,
    loaded: Mutex<Option<LoadedRustMiniCpmV46>>,
}

#[cfg(feature = "native-models")]
struct LoadedRustMiniCpmV46 {
    model: MiniCpmV46ForConditionalGeneration,
    tokenizer: Tokenizer,
    eos_token_ids: Vec<u32>,
    max_position_embeddings: usize,
}

#[cfg(feature = "native-models")]
impl RustMiniCpmV46ChatModel {
    pub fn from_runtime_config(
        data_dir: &SinomoDataDir,
        runtime_config: ModelRuntimeConfig,
        thinking: ThinkingControl,
    ) -> Self {
        let model_dir = configured_rust_minicpm_v46_dir(data_dir, &runtime_config);
        Self {
            model_dir,
            max_input_tokens_override: runtime_config.minicpm_v46.max_input_tokens,
            max_output_tokens_override: runtime_config.minicpm_v46.max_output_tokens,
            thinking,
            prompt_renderer: MiniCpmV46PromptRenderer,
            loaded: Mutex::new(None),
        }
    }

    fn with_loaded<T>(
        &self,
        f: impl FnOnce(&LoadedRustMiniCpmV46) -> Result<T, LanguageModelError>,
    ) -> Result<T, LanguageModelError> {
        let mut guard =
            self.loaded
                .lock()
                .map_err(|error| LanguageModelError::ProviderInvocationFailed {
                    provider: RUST_MINICPM_V46_PROVIDER_ID.to_string(),
                    model: MINICPM_V46_DEFAULT_MODEL_ID.to_string(),
                    detail: format!("MiniCPM-V 4.6 model lock poisoned: {error}"),
                })?;
        if guard.is_none() {
            *guard = Some(load_rust_minicpm_v46(&self.model_dir)?);
        }
        f(guard.as_ref().expect("loaded model initialized"))
    }

    fn generate(
        &self,
        loaded: &LoadedRustMiniCpmV46,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.generate_streaming(loaded, request, None)
    }

    fn generate_streaming(
        &self,
        loaded: &LoadedRustMiniCpmV46,
        request: &LanguageModelRequest,
        on_delta: Option<&mut dyn FnMut(&str)>,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let prompt_request = ChatPromptRequest::from_language_model_request(
            request,
            ThinkingMode::from_enabled(self.thinking.is_enabled()),
        );
        let (content, input_tokens, output_tokens) =
            self.generate_with_prompt_request(loaded, request, prompt_request.as_ref(), on_delta)?;
        Ok(LanguageModelResponse {
            provider: RUST_MINICPM_V46_PROVIDER_ID.to_string(),
            model: MINICPM_V46_DEFAULT_MODEL_ID.to_string(),
            content,
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: Some(sinomo_lm::LanguageModelUsage {
                input_tokens: Some(input_tokens as u64),
                output_tokens: Some(output_tokens as u64),
            }),
        })
    }

    fn generate_with_prompt_request(
        &self,
        loaded: &LoadedRustMiniCpmV46,
        request: &LanguageModelRequest,
        prompt_request: Option<&ChatPromptRequest>,
        on_delta: Option<&mut dyn FnMut(&str)>,
    ) -> Result<(String, usize, usize), LanguageModelError> {
        let prompt = prompt_request
            .as_ref()
            .map(|prompt_request| self.prompt_renderer.render(prompt_request))
            .unwrap_or_else(|| sinomo_lm::render_plain_prompt(request));
        let encoding = loaded.tokenizer.encode(prompt, true).map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: RUST_MINICPM_V46_PROVIDER_ID.to_string(),
                model: MINICPM_V46_DEFAULT_MODEL_ID.to_string(),
                detail: format!("tokenize MiniCPM-V 4.6 prompt: {source}"),
            }
        })?;
        let mut input_ids = encoding.get_ids().to_vec();
        let max_input_tokens = self
            .max_input_tokens_override
            .unwrap_or(loaded.max_position_embeddings);
        if input_ids.len() > max_input_tokens {
            input_ids = input_ids[input_ids.len() - max_input_tokens..].to_vec();
        }
        if input_ids.is_empty() {
            input_ids.push(0);
        }
        let max_output_tokens = self.max_output_tokens_override.unwrap_or(
            loaded
                .max_position_embeddings
                .saturating_sub(input_ids.len())
                .max(1),
        );
        let requested_tokens = request
            .max_output_tokens
            .map(|value| value as usize)
            .unwrap_or(max_output_tokens)
            .min(max_output_tokens);
        let generated = generate_greedy_tokens(loaded, &input_ids, requested_tokens, on_delta)?;
        let content = loaded
            .tokenizer
            .decode(&generated, true)
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: RUST_MINICPM_V46_PROVIDER_ID.to_string(),
                model: MINICPM_V46_DEFAULT_MODEL_ID.to_string(),
                detail: format!("decode MiniCPM-V 4.6 response: {source}"),
            })?;
        let content = prompt_request
            .as_ref()
            .map(|prompt_request| self.prompt_renderer.finalize(prompt_request, &content))
            .unwrap_or_else(|| content.trim().to_string());
        Ok((content, input_ids.len(), generated.len()))
    }
}

#[cfg(feature = "native-models")]
fn configured_rust_minicpm_v46_dir(
    data_dir: &SinomoDataDir,
    runtime_config: &ModelRuntimeConfig,
) -> PathBuf {
    if let Some(model_dir) = runtime_config.minicpm_v46.model_dir.as_deref() {
        return PathBuf::from(model_dir);
    }
    if let Some(cache_dir) = runtime_config.native_models_cache_dir.as_deref() {
        return PathBuf::from(cache_dir).join(DEFAULT_RUST_MINICPM_V46_CACHE_SUBDIR);
    }
    data_dir
        .root()
        .join("models")
        .join(DEFAULT_RUST_MINICPM_V46_CACHE_SUBDIR)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThinkingMode {
    Enabled,
    Disabled,
}

impl ThinkingMode {
    pub fn from_enabled(enabled: bool) -> Self {
        if enabled {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }

    pub fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChatPromptTool {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChatPromptRequest {
    pub system_prompt: String,
    pub user_prompt: String,
    pub tools: Vec<ChatPromptTool>,
    pub thinking: ThinkingMode,
    pub response_format: sinomo_lm::LanguageModelResponseFormat,
}

impl ChatPromptRequest {
    pub fn from_language_model_request(
        request: &LanguageModelRequest,
        thinking: ThinkingMode,
    ) -> Option<Self> {
        if request
            .metadata
            .get("loop")
            .and_then(serde_json::Value::as_str)
            != Some("agent")
        {
            return None;
        }
        agent_chat_prompt_request(request, thinking)
    }
}

pub trait ModelPromptRenderer {
    fn render(&self, request: &ChatPromptRequest) -> String;

    fn finalize(&self, request: &ChatPromptRequest, content: &str) -> String {
        let _ = request;
        content.trim().to_string()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MiniCpmV46PromptRenderer;

impl ModelPromptRenderer for MiniCpmV46PromptRenderer {
    fn render(&self, request: &ChatPromptRequest) -> String {
        minicpm_render_prompt(request)
    }

    fn finalize(&self, request: &ChatPromptRequest, content: &str) -> String {
        finalize_minicpm_response(request, content)
    }
}

#[cfg(test)]
pub(crate) fn minicpm_main_agent_prompt(
    request: &LanguageModelRequest,
    enable_thinking: bool,
) -> Option<String> {
    let request = ChatPromptRequest::from_language_model_request(
        request,
        ThinkingMode::from_enabled(enable_thinking),
    )?;
    Some(MiniCpmV46PromptRenderer.render(&request))
}

fn agent_chat_prompt_request(
    request: &LanguageModelRequest,
    thinking: ThinkingMode,
) -> Option<ChatPromptRequest> {
    let system_prompt = request
        .messages
        .iter()
        .filter(|message| matches!(message.role, sinomo_lm::LanguageModelRole::System))
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    let system_prompt = if system_prompt.trim().is_empty() {
        "You are Sinomo, the user-facing main agent.".to_string()
    } else {
        system_prompt
    };
    let user_prompt = request
        .messages
        .iter()
        .filter(|message| !matches!(message.role, sinomo_lm::LanguageModelRole::System))
        .map(|message| {
            let role = match message.role {
                sinomo_lm::LanguageModelRole::System => "system",
                sinomo_lm::LanguageModelRole::User => "user",
                sinomo_lm::LanguageModelRole::Assistant => "assistant",
            };
            format!("{role}:\n{}", message.content.trim())
        })
        .collect::<Vec<_>>()
        .join("\n");
    if user_prompt.trim().is_empty() {
        return None;
    }
    let tools = chat_prompt_tools_from_language_model_specs(&request.tools);
    Some(ChatPromptRequest {
        system_prompt,
        user_prompt,
        tools,
        thinking,
        response_format: request.response_format.clone(),
    })
}

fn chat_prompt_tools_from_language_model_specs(
    tools: &[sinomo_lm::LanguageModelToolSpec],
) -> Vec<ChatPromptTool> {
    tools
        .iter()
        .take(10)
        .map(|tool| ChatPromptTool {
            name: tool.name.clone(),
            description: tool.description.clone(),
            parameters: if tool.input_schema.is_null() {
                serde_json::json!({
                    "type": "object",
                    "additionalProperties": true
                })
            } else {
                tool.input_schema.clone()
            },
        })
        .collect()
}

fn minicpm_render_prompt(request: &ChatPromptRequest) -> String {
    let tools = minicpm_tool_prompt(&request.tools);
    let thinking_prompt = if request.thinking.is_enabled() {
        "<think>\n"
    } else {
        "<think>\n\n</think>\n\n"
    };
    let system = if tools.is_empty() {
        request.system_prompt.clone()
    } else if request.system_prompt.trim().is_empty() {
        tools
    } else {
        format!("{tools}\n\n{}", request.system_prompt)
    };
    format!(
        "<|im_start|>system\n{}<|im_end|>\n<|im_start|>user\n{}<|im_end|>\n<|im_start|>assistant\n{}",
        system.trim(),
        request.user_prompt.trim(),
        thinking_prompt
    )
}

fn minicpm_tool_prompt(tools: &[ChatPromptTool]) -> String {
    if tools.is_empty() {
        return String::new();
    }
    let mut prompt =
        String::from("# Tools\n\nYou have access to the following functions:\n\n<tools>");
    for tool in tools.iter().take(10) {
        let function = serde_json::json!({
            "type": "function",
            "function": {
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.parameters
            }
        });
        prompt.push('\n');
        prompt.push_str(&function.to_string());
    }
    prompt.push_str(
        "\n</tools>\n\nIf you choose to call a function ONLY reply in the following format with NO suffix:\n\n<tool_call>\n<function=example_function_name>\n<parameter=example_parameter_1>\nvalue_1\n</parameter>\n</function>\n</tool_call>\n\n<IMPORTANT>\nReminder:\n- Function calls MUST follow the specified format: an inner <function=...></function> block must be nested within <tool_call></tool_call> XML tags\n- Required parameters MUST be specified\n- You may provide optional reasoning for your function call in natural language BEFORE the function call, but NOT after\n- If there is no function call available, answer the question like normal with your current knowledge and do not tell the user about function calls\n</IMPORTANT>",
    );
    prompt
}

fn finalize_minicpm_response(request: &ChatPromptRequest, content: &str) -> String {
    let trimmed = trim_generated_chat_turn(content);
    if request.response_format == sinomo_lm::LanguageModelResponseFormat::JsonObject
        && trimmed.contains("<tool_call>")
        && let Some(json) = minicpm_tool_call_to_agent_decision_json(&trimmed)
    {
        return json;
    }
    trimmed
}

pub(crate) fn starts_structured_agent_json_delta(delta: &str) -> bool {
    let trimmed = delta.trim_start();
    if trimmed.starts_with('{') {
        return true;
    }
    trimmed.starts_with("```json")
        && (trimmed.contains('{')
            || trimmed.contains("\"response_mode\"")
            || trimmed.contains("\"tool_calls\"")
            || trimmed.contains("\"next_state\""))
}

pub(crate) fn trim_generated_chat_turn(content: &str) -> String {
    let without_thinking = if let Some((_, after)) = content.rsplit_once("</think>") {
        after
    } else if let Some(after) = content.strip_prefix("<think>") {
        let _ = after;
        return String::new();
    } else {
        content
    };
    let mut end = without_thinking.len();
    for marker in [
        "<|im_end|>",
        "<|im_start|>",
        "\nUser:",
        "\nSinomo:",
        "\nAssistant:",
        "\nSystem:",
        "\nNode:",
        "\nCluster:",
    ] {
        if let Some(index) = without_thinking.find(marker) {
            end = end.min(index);
        }
    }
    without_thinking[..end].trim().to_string()
}

pub(crate) fn minicpm_tool_call_to_agent_decision_json(content: &str) -> Option<String> {
    let function_start = content.find("<function=")? + "<function=".len();
    let function_end = content[function_start..].find('>')? + function_start;
    let tool = content[function_start..function_end].trim();
    let function_close = content[function_end..].find("</function>")? + function_end;
    let function_body = &content[function_end + 1..function_close];
    let mut input = serde_json::Map::new();
    let mut rest = function_body;
    while let Some(parameter_start) = rest.find("<parameter=") {
        rest = &rest[parameter_start + "<parameter=".len()..];
        let Some(parameter_name_end) = rest.find('>') else {
            break;
        };
        let name = rest[..parameter_name_end].trim();
        rest = &rest[parameter_name_end + 1..];
        let Some(parameter_end) = rest.find("</parameter>") else {
            break;
        };
        let value = rest[..parameter_end].trim();
        input.insert(
            name.to_string(),
            serde_json::Value::String(value.to_string()),
        );
        rest = &rest[parameter_end + "</parameter>".len()..];
    }
    let prefix = content
        .split("<tool_call>")
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    serde_json::to_string(&serde_json::json!({
        "response_mode": "defer",
        "text": if prefix.is_empty() {
            format!("I need to use `{tool}`.")
        } else {
            prefix
        },
        "tool_calls": [{
            "tool": tool,
            "input": serde_json::Value::Object(input),
            "async": true,
            "summary": format!("MiniCPM requested `{tool}` through its tool-call format.")
        }],
        "background_handoff": null,
        "next_state": "delegating"
    }))
    .ok()
}

#[cfg(feature = "native-models")]
impl InteractionModel for RustMiniCpmV46ChatModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.with_loaded(|loaded| self.generate(loaded, request))
    }

    fn complete_streaming(
        &self,
        request: &LanguageModelRequest,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        on_delta("<sinomo_progress>loading native MiniCPM-V 4.6</sinomo_progress>");
        self.with_loaded(|loaded| {
            on_delta("<sinomo_progress>running prompt prefill</sinomo_progress>");
            self.generate_streaming(loaded, request, Some(on_delta))
        })
    }
}

#[cfg(feature = "native-models")]
fn load_rust_minicpm_v46(model_dir: &Path) -> Result<LoadedRustMiniCpmV46, LanguageModelError> {
    let config_path = model_dir.join("config.json");
    let safetensors_path = model_dir.join("model.safetensors");
    let tokenizer_path = model_dir.join("tokenizer.json");
    for path in [&config_path, &safetensors_path, &tokenizer_path] {
        if !path.exists() {
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: RUST_MINICPM_V46_PROVIDER_ID.to_string(),
                model: MINICPM_V46_DEFAULT_MODEL_ID.to_string(),
                detail: format!(
                    "missing MiniCPM-V 4.6 asset {}; configure `sinomo models runtime set --minicpm-v46-model-dir <path>` with a directory containing config.json, model.safetensors, and tokenizer.json",
                    path.display()
                ),
            });
        }
    }
    let config = MiniCpmV46Config::from_file(&config_path).map_err(minicpm_load_error)?;
    config
        .validate_native_support()
        .map_err(minicpm_load_error)?;
    let tokenizer = Tokenizer::from_file(&tokenizer_path).map_err(|source| {
        LanguageModelError::ProviderInvocationFailed {
            provider: RUST_MINICPM_V46_PROVIDER_ID.to_string(),
            model: MINICPM_V46_DEFAULT_MODEL_ID.to_string(),
            detail: format!(
                "load MiniCPM-V 4.6 tokenizer {}: {source}",
                tokenizer_path.display()
            ),
        }
    })?;
    let eos_token_ids = ["<|im_end|>", "<|endoftext|>", "</s>", "<|end|>"]
        .into_iter()
        .filter_map(|token| tokenizer.token_to_id(token))
        .collect();
    let device = Device::Cpu;
    let model = unsafe {
        // The checkpoint is loaded read-only from the configured model directory.
        MiniCpmV46ForConditionalGeneration::from_mmaped_safetensors(
            &config,
            &[safetensors_path],
            DType::F32,
            &device,
        )
    }
    .map_err(minicpm_load_error)?;
    Ok(LoadedRustMiniCpmV46 {
        model,
        tokenizer,
        eos_token_ids,
        max_position_embeddings: config.text_config.max_position_embeddings,
    })
}

#[cfg(feature = "native-models")]
fn generate_greedy_tokens(
    loaded: &LoadedRustMiniCpmV46,
    input_ids: &[u32],
    max_output_tokens: usize,
    mut on_delta: Option<&mut dyn FnMut(&str)>,
) -> Result<Vec<u32>, LanguageModelError> {
    let device = Device::Cpu;
    let prompt = Tensor::from_vec(input_ids.to_vec(), (1, input_ids.len()), &device)
        .map_err(minicpm_inference_error)?;
    let (mut logits, mut cache) = loaded
        .model
        .forward_text_only_input_ids_with_cache(&prompt, None::<Qwen3_5TextCache>)
        .map_err(minicpm_inference_error)?;
    if let Some(on_delta) = on_delta.as_deref_mut() {
        on_delta("<sinomo_progress>generating response</sinomo_progress>");
    }
    let mut generated = Vec::new();
    let mut emitted = String::new();
    for _ in 0..max_output_tokens {
        let next_id = next_token_id(&logits)?;
        if loaded.eos_token_ids.contains(&next_id) {
            break;
        }
        generated.push(next_id);
        if let Some(on_delta) = on_delta.as_deref_mut() {
            let decoded = loaded
                .tokenizer
                .decode(&generated, true)
                .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                    provider: RUST_MINICPM_V46_PROVIDER_ID.to_string(),
                    model: MINICPM_V46_DEFAULT_MODEL_ID.to_string(),
                    detail: format!("decode MiniCPM-V 4.6 streaming response: {source}"),
                })?;
            if decoded.len() > emitted.len() && decoded.starts_with(&emitted) {
                on_delta(&decoded[emitted.len()..]);
                emitted = decoded;
            }
        }
        let token =
            Tensor::from_vec(vec![next_id], (1, 1), &device).map_err(minicpm_inference_error)?;
        let (next_logits, next_cache) = loaded
            .model
            .forward_text_only_input_ids_with_cache(&token, Some(cache))
            .map_err(minicpm_inference_error)?;
        logits = next_logits;
        cache = next_cache;
    }
    Ok(generated)
}

#[cfg(feature = "native-models")]
fn next_token_id(logits: &Tensor) -> Result<u32, LanguageModelError> {
    let values = logits.to_vec3::<f32>().map_err(minicpm_inference_error)?;
    let last = values
        .first()
        .and_then(|batch| batch.last())
        .ok_or_else(|| LanguageModelError::ProviderInvocationFailed {
            provider: RUST_MINICPM_V46_PROVIDER_ID.to_string(),
            model: MINICPM_V46_DEFAULT_MODEL_ID.to_string(),
            detail: "MiniCPM-V 4.6 returned empty logits".to_string(),
        })?;
    let (index, _) = last
        .iter()
        .copied()
        .enumerate()
        .max_by(|(_, left), (_, right)| left.total_cmp(right))
        .ok_or_else(|| LanguageModelError::ProviderInvocationFailed {
            provider: RUST_MINICPM_V46_PROVIDER_ID.to_string(),
            model: MINICPM_V46_DEFAULT_MODEL_ID.to_string(),
            detail: "MiniCPM-V 4.6 returned an empty vocabulary axis".to_string(),
        })?;
    Ok(index as u32)
}

#[cfg(feature = "native-models")]
fn minicpm_load_error(error: impl std::fmt::Display) -> LanguageModelError {
    LanguageModelError::ProviderInvocationFailed {
        provider: RUST_MINICPM_V46_PROVIDER_ID.to_string(),
        model: MINICPM_V46_DEFAULT_MODEL_ID.to_string(),
        detail: format!("load MiniCPM-V 4.6 runtime: {error}"),
    }
}

#[cfg(feature = "native-models")]
fn minicpm_inference_error(error: impl std::fmt::Display) -> LanguageModelError {
    LanguageModelError::ProviderInvocationFailed {
        provider: RUST_MINICPM_V46_PROVIDER_ID.to_string(),
        model: MINICPM_V46_DEFAULT_MODEL_ID.to_string(),
        detail: format!("run MiniCPM-V 4.6 runtime: {error}"),
    }
}

fn daemon_context_event(
    session: &InteractionSession,
    daemon: Option<&DaemonStatus>,
) -> EventEnvelope {
    interaction_event(
        session,
        "interaction.cluster.context",
        serde_json::json!({
            "daemon": daemon.map(|status| serde_json::json!({
                "pid": status.pid,
                "address": status.address,
                "version": status.version,
                "cluster": status.cluster.as_ref().map(|cluster| serde_json::json!({
                    "cluster_id": cluster.cluster_id,
                    "node_id": cluster.node_id,
                    "process": cluster.process,
                    "member_role": cluster.member_role,
                    "member_state": cluster.member_state
                }))
            })),
            "routing": "daemon-cluster-status"
        }),
    )
}

fn handle_thinking_command<S: EventStore>(
    store: &mut S,
    session: &InteractionSession,
    event: &EventEnvelope,
    args: &[String],
    thinking: &ThinkingControl,
) -> Result<Option<Vec<InteractionOutput>>, CliError> {
    if args.first().map(String::as_str) != Some("thinking") {
        return Ok(None);
    }
    let output = match args.get(1).map(String::as_str) {
        None | Some("status") => format!(
            "thinking is {}",
            if thinking.is_enabled() { "on" } else { "off" }
        ),
        Some("on") => {
            thinking.set_enabled(true);
            "thinking is on".to_string()
        }
        Some("off") => {
            thinking.set_enabled(false);
            "thinking is off".to_string()
        }
        Some(value) => {
            return Err(CliError::Parse {
                detail: format!(
                    "unexpected /thinking value `{value}`; use /thinking on, /thinking off, or /thinking status"
                ),
            });
        }
    };
    let command = interaction_event(
        session,
        "interaction.thinking.updated",
        serde_json::json!({
            "input_event_id": event.event_id,
            "enabled": thinking.is_enabled()
        }),
    )
    .caused_by(&event.event_id);
    store.append(command)?;
    Ok(Some(vec![InteractionOutput::Line(output)]))
}

const CHAT_SKILL_ADMIN_SUBCOMMANDS: &[&str] = &[
    "activate", "describe", "enable", "export", "help", "import", "inspect", "list", "ls", "show",
    "--help", "-h",
];

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExplicitChatSkillCommand {
    skill_id: String,
    request: Option<String>,
}

fn parse_explicit_chat_skill_command(args: &[String]) -> Option<ExplicitChatSkillCommand> {
    let [command, skill_id, ..] = args else {
        return None;
    };
    let request = args[2..].join(" ");
    (command == "skill"
        && !skill_id.starts_with('-')
        && !CHAT_SKILL_ADMIN_SUBCOMMANDS.contains(&skill_id.as_str()))
    .then(|| ExplicitChatSkillCommand {
        skill_id: skill_id.clone(),
        request: (!request.trim().is_empty()).then_some(request),
    })
}

fn render_explicit_skill_turn_context(
    skill: &sinomo_agent::ExplicitAgentSkillContext,
    has_request: bool,
) -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "explicit_agent_skill": {
            "host_instruction": if has_request {
                "The user explicitly activated this Agent Skill for the current turn. Apply it to the explicit_skill_request context attachment without activating the main skill again. If a listed resource is needed, call skill_activate with this skill name and the relative resource path."
            } else {
                "The user explicitly activated this Agent Skill without an accompanying request. Briefly state what it can help with and ask what they would like to do. Do not activate the main skill again. If a listed resource is needed, call skill_activate with this skill name and the relative resource path."
            },
            "name": skill.name,
            "description": skill.description,
            "instructions": skill.instructions,
            "resources": skill.resources,
        }
    }))
    .expect("explicit Agent Skill turn context serializes")
}

fn explicit_skill_request_attachment(
    skill_name: &str,
    request: &str,
) -> sinomo_agent::AgentContextAttachment {
    sinomo_agent::AgentContextAttachment {
        kind: "explicit_skill_request".to_string(),
        summary: format!("User request for explicitly activated skill `{skill_name}`"),
        payload_json: serde_json::json!({
            "skill": skill_name,
            "request": request,
        }),
        payload_original_bytes: None,
        payload_model_visible_truncated: false,
    }
}

fn merge_transient_system_context(existing: Option<String>, skill_context: String) -> String {
    existing
        .map(|existing| format!("{existing}\n\n{skill_context}"))
        .unwrap_or(skill_context)
}

fn handle_slash_command<S: EventStore>(
    store: &mut S,
    session: &InteractionSession,
    event: &EventEnvelope,
    args: Vec<String>,
) -> Result<Vec<InteractionOutput>, CliError> {
    execute_slash_command(store, session, event, args)
}

fn execute_slash_command<S: EventStore>(
    store: &mut S,
    session: &InteractionSession,
    event: &EventEnvelope,
    args: Vec<String>,
) -> Result<Vec<InteractionOutput>, CliError> {
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

    let execution = run_cli_command_with_capture(&args);
    let mut outputs = captured_cli_command_outputs(&execution.output);
    let command_text = args.join(" ");
    let error_text = execution.result.as_ref().err().map(ToString::to_string);
    let (kind, status) = match &execution.result {
        Ok(()) => ("interaction.command.completed", "completed"),
        Err(_) => ("interaction.command.failed", "failed"),
    };
    if let Some(error) = error_text {
        outputs.push(InteractionOutput::Line(format!("command failed: {error}")));
    } else if outputs.is_empty() {
        outputs.push(InteractionOutput::Line(format!(
            "completed command: /{command_text}"
        )));
    }
    let outcome = interaction_event(
        session,
        kind,
        serde_json::json!({
            "command_event_id": command.event_id,
            "status": status,
            "args": args,
            "stdout_bytes": execution.output.stdout.len(),
            "stderr_bytes": execution.output.stderr.len()
        }),
    )
    .caused_by(&command.event_id);
    store.append(outcome)?;

    Ok(outputs)
}

fn captured_cli_command_outputs(output: &CliCommandOutput) -> Vec<InteractionOutput> {
    let mut outputs = Vec::new();
    push_captured_cli_stream(&mut outputs, None, &output.stdout);
    push_captured_cli_stream(&mut outputs, Some("stderr: "), &output.stderr);
    outputs
}

fn push_captured_cli_stream(
    outputs: &mut Vec<InteractionOutput>,
    prefix: Option<&str>,
    bytes: &[u8],
) {
    let text = String::from_utf8_lossy(bytes);
    for line in text.lines() {
        outputs.push(InteractionOutput::Line(match prefix {
            Some(prefix) => format!("{prefix}{line}"),
            None => line.to_string(),
        }));
    }
}

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
