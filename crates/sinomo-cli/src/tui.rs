use super::*;
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
const TUI_MOUSE_SCROLL_LINES: isize = 4;

pub fn run_tui() -> Result<(), CliError> {
    if io::stdin().is_terminal() && io::stdout().is_terminal() {
        let daemon = tui_default_daemon_status()?;
        let (handler, daemon) = main_agent_handler_for_tui(daemon)?;
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
    let (handler, daemon) = main_agent_handler_for_tui(daemon)?;
    run_tui_with_handler_io_and_daemon(reader, writer, handler, daemon)
}

fn main_agent_handler_for_tui(
    daemon: Option<DaemonStatus>,
) -> Result<
    (
        MainAgentInteractionHandler<DefaultMainAgentChatModel>,
        Option<DaemonStatus>,
    ),
    CliError,
> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    main_agent_handler_for_tui_with_data_dir(daemon, &data_dir)
}

pub(crate) fn main_agent_handler_for_tui_with_data_dir(
    daemon: Option<DaemonStatus>,
    data_dir: &SinomoDataDir,
) -> Result<
    (
        MainAgentInteractionHandler<DefaultMainAgentChatModel>,
        Option<DaemonStatus>,
    ),
    CliError,
> {
    let (runtime_config, _) = load_tui_model_runtime_config(data_dir, daemon.is_some())?;
    let handler = MainAgentInteractionHandler::from_runtime_config(&data_dir, runtime_config)?;
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
    mut reader: R,
    mut writer: W,
    handler: H,
    daemon: Option<DaemonStatus>,
) -> Result<(), CliError> {
    let mut runtime = new_interaction_runtime(handler, daemon.as_ref())?;
    let session = runtime.session().clone();

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
        if !line.trim_start().starts_with('/') {
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
        let outputs = runtime.ingest_user_input_streaming(line, &mut emit)?;
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
    session_store.save_state(runtime.session(), &state)?;
    let mut terminal = TuiTerminalSession::enter()?;

    terminal.draw(&state)?;
    loop {
        if state.exit_requested {
            break;
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
) -> Result<(), CliError> {
    match action {
        TuiInputAction::None => {}
        TuiInputAction::Exit => {
            submit_tui_input(
                runtime,
                state,
                terminal,
                data_dir,
                session_store,
                daemon,
                "/quit".to_string(),
            )?;
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
            )?;
        }
        TuiInputAction::OpenFiles(target) => {
            state.open_files_target(data_dir, target);
            session_store.save_state(runtime.session(), state)?;
        }
        TuiInputAction::OpenSessions => {
            open_tui_session_list_view(runtime, state, session_store)?;
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

    let mut render_error = None;
    let result = {
        let mut emit = |output: InteractionOutput| {
            state.apply_output(output);
            if render_error.is_none() {
                if let Err(error) = terminal.draw(state) {
                    render_error = Some(error);
                }
            }
        };
        runtime.ingest_user_input_streaming(&line, &mut emit)
    };
    if let Some(error) = render_error {
        return Err(error);
    }
    match result {
        Ok(outputs) => {
            for output in outputs {
                state.apply_output(output);
            }
        }
        Err(error) => {
            state.apply_output(InteractionOutput::Line(format!("model failed: {error}")));
        }
    }
    state.finish_submission();
    session_store.save_state(runtime.session(), state)?;
    Ok(())
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
    OpenFiles(TuiFilesTarget),
    OpenSessions,
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
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct TuiSessionRecord {
    schema_version: u32,
    session: InteractionSession,
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
        sessions.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        Ok(sessions)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TuiFilesTarget {
    Local(PathBuf),
    Sifs(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TuiSlashCommand {
    Files { target: TuiFilesTarget },
    Model(TuiModelCommand),
    Sessions,
    Resume { session_id: SessionId },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TuiModelCommand {
    Status,
    Use(TuiModelSelection),
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
        }
    }
}

trait TuiAgentRuntimeControl: InteractionEventHandler {
    fn tui_agent_runtime_info(&self) -> TuiAgentRuntimeInfo;

    fn switch_tui_model(
        &mut self,
        data_dir: &SinomoDataDir,
        selection: TuiModelSelection,
        daemon_connected: bool,
    ) -> Result<TuiAgentRuntimeInfo, CliError>;
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

#[derive(Clone, Debug)]
struct TuiState {
    session_id: String,
    daemon_connected: bool,
    cluster_label: Option<String>,
    runtime_info: TuiAgentRuntimeInfo,
    last_model_usage: Option<TuiTokenUsage>,
    session_token_usage: TuiTokenUsage,
    view_mode: TuiViewMode,
    file_browser: Option<FileManagerState>,
    file_prompt: Option<FileManagerPrompt>,
    session_list: Option<TuiSessionListState>,
    input: String,
    input_cursor: usize,
    messages: Vec<TuiMessage>,
    stream_message: Option<usize>,
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
            daemon_connected: daemon.is_some(),
            cluster_label,
            runtime_info,
            last_model_usage: None,
            session_token_usage: TuiTokenUsage::default(),
            view_mode: TuiViewMode::Chat,
            file_browser: None,
            file_prompt: None,
            session_list: None,
            input: String::new(),
            input_cursor: 0,
            messages: Vec::new(),
            stream_message: None,
            chat_selection: None,
            completions: None,
            status: "listening".to_string(),
            scrollback: 0,
            exit_requested: false,
        };
        state.push_message(TuiMessageKind::System, "main agent session started");
        state
    }

    fn apply_key_event(&mut self, key: KeyEvent) -> TuiInputAction {
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return TuiInputAction::None;
        }

        match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                TuiInputAction::Exit
            }
            KeyCode::F(1) => {
                self.open_chat_tab();
                TuiInputAction::None
            }
            KeyCode::F(2) => self.toggle_file_browser(),
            KeyCode::F(3) => TuiInputAction::OpenSessions,
            _ if self.view_mode == TuiViewMode::Files => self.apply_file_browser_key_event(key),
            _ if self.view_mode == TuiViewMode::Sessions => self.apply_session_list_key_event(key),
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

    fn replace_session(
        &mut self,
        session: &InteractionSession,
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
        self.runtime_info = runtime_info;
        self.last_model_usage = last_model_usage;
        self.session_token_usage = session_token_usage;
        self.view_mode = TuiViewMode::Chat;
        self.file_browser = None;
        self.file_prompt = None;
        self.session_list = None;
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
        "Message"
    }

    fn accepts_text_input(&self) -> bool {
        self.view_mode == TuiViewMode::Chat || self.file_prompt.is_some()
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
        self.status = "streaming".to_string();
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
            detail: "`--local` is a process option; start the TUI with `sinomo --local` instead"
                .to_string(),
        });
    }
    match args.as_slice() {
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
        TuiSlashCommand::Sessions => {
            let count = open_tui_session_list_view(runtime, state, session_store)?;
            Ok(vec![InteractionOutput::Line(format!(
                "loaded {count} session(s)"
            ))])
        }
        TuiSlashCommand::Resume { session_id } => {
            resume_tui_session(runtime, state, session_store, data_dir, daemon, session_id)
        }
    }
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

    session_store.save_state(runtime.session(), state)?;
    let record = session_store.load_session(&session_id)?;
    let mut outputs = Vec::new();
    let restored_info = restore_tui_session_runtime_info(
        &mut runtime.handler,
        data_dir,
        state.daemon_connected,
        &record.runtime_info,
        &mut outputs,
    );
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
    state.replace_session(
        &record.session,
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

fn restore_tui_session_runtime_info<H: TuiAgentRuntimeControl>(
    handler: &mut H,
    data_dir: &SinomoDataDir,
    daemon_connected: bool,
    info: &TuiAgentRuntimeInfo,
    outputs: &mut Vec<InteractionOutput>,
) -> TuiAgentRuntimeInfo {
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
    match handler.switch_tui_model(data_dir, selection, daemon_connected) {
        Ok(info) => info,
        Err(error) => {
            outputs.push(InteractionOutput::Line(format!(
                "model restore failed: {error}"
            )));
            handler.tui_agent_runtime_info()
        }
    }
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
    if matches!(state.view_mode, TuiViewMode::Files | TuiViewMode::Sessions) {
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

fn tui_menu_tabs(view_mode: &TuiViewMode) -> [TuiMenuTab; 3] {
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
                "working" | "streaming" => Style::default().fg(Color::Yellow),
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
                Span::styled(
                    text.to_string(),
                    selected_style.unwrap_or_else(Style::default),
                ),
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

    fn temp_file_browser_root(name: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = env::temp_dir().join(format!(
            "sinomo-tui-file-browser-{name}-{}-{unique}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("temp root");
        root
    }

    fn session_record(session_id: SessionId, title: &str) -> TuiSessionRecord {
        TuiSessionRecord {
            schema_version: TUI_SESSION_SCHEMA_VERSION,
            session: InteractionSession::new(
                session_id,
                NodeId::from_legacy_named("node_mouse").expect("node"),
                InteractionSurface::new("surface.mouse", InteractionSurfaceKind::CommandLine),
            ),
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            title: title.to_string(),
            message_count: 1,
            ended: false,
            runtime_info: TuiAgentRuntimeInfo::default(),
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

        let files = tui_menu_tabs(&TuiViewMode::Files);
        assert!(!files[0].active);
        assert!(files[1].active);
        assert!(!files[2].active);

        let sessions = tui_menu_tabs(&TuiViewMode::Sessions);
        assert!(!sessions[0].active);
        assert!(!sessions[1].active);
        assert!(sessions[2].active);
    }

    #[test]
    fn tui_state_f1_returns_to_chat_tab() {
        let root = temp_file_browser_root("f1");
        let mut state = fixture_tui_state();

        state.open_file_browser_at(root.clone());
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

        fs::remove_dir_all(root).ok();
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
    fn tui_mouse_menu_clicks_switch_views() {
        let root = temp_file_browser_root("mouse-menu");
        let area = Rect::new(0, 0, 100, 24);
        let mut state = fixture_tui_state();
        let menu = tui_layout_areas(area, &state).menu;

        assert_eq!(tui_menu_tab_at(menu, 1, 0), Some(TuiViewMode::Chat));
        assert_eq!(tui_menu_tab_at(menu, 13, 0), Some(TuiViewMode::Files));
        assert_eq!(tui_menu_tab_at(menu, 26, 0), Some(TuiViewMode::Sessions));

        assert_eq!(
            state.apply_mouse_event(mouse(MouseEventKind::Down(MouseButton::Left), 13, 0), area),
            TuiInputAction::OpenFiles(TuiFilesTarget::Sifs("/".to_string()))
        );

        assert_eq!(
            state.apply_mouse_event(mouse(MouseEventKind::Down(MouseButton::Left), 26, 0), area),
            TuiInputAction::OpenSessions
        );

        state.open_file_browser_at(root.clone());
        assert_eq!(state.view_mode, TuiViewMode::Files);
        assert_eq!(
            state.apply_mouse_event(mouse(MouseEventKind::Down(MouseButton::Left), 1, 0), area),
            TuiInputAction::None
        );
        assert_eq!(state.view_mode, TuiViewMode::Chat);

        fs::remove_dir_all(root).ok();
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

        state.open_file_browser_at(root.clone());
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

        fs::remove_dir_all(root).ok();
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

        state.open_file_browser_at(root.clone());
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

        fs::remove_dir_all(root).ok();
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

        fs::remove_dir_all(root).ok();
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

        fs::remove_dir_all(root).ok();
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

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn tui_state_file_browser_prompt_creates_file_and_closes() {
        let root = temp_file_browser_root("state");
        let mut state = fixture_tui_state();

        state.open_file_browser_at(root.clone());
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

        fs::remove_dir_all(root).ok();
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
    fn tui_session_commands_parse_list_and_resume() {
        let session_id = SessionId::generate();
        assert_eq!(
            parse_tui_slash_command("/sessions")
                .expect("parse")
                .expect("sessions"),
            TuiSlashCommand::Sessions
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
        let data_dir = SinomoDataDir::new(root.clone());
        let store = TuiSessionStore::for_data_dir(&data_dir);
        let session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_test").expect("node"),
            InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
        );
        let mut state = TuiState::new(&session, None, TuiAgentRuntimeInfo::default());
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
        let transcript = store
            .load_transcript(&session.session_id)
            .expect("load")
            .expect("transcript");
        assert_eq!(transcript.messages, state.messages);
        assert_eq!(transcript.last_model_usage, state.last_model_usage);
        assert_eq!(transcript.session_token_usage, state.session_token_usage);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn tui_session_store_discards_empty_sessions() {
        let root = temp_file_browser_root("empty-sessions");
        let data_dir = SinomoDataDir::new(root.clone());
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

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn tui_session_store_discards_command_only_sessions() {
        let root = temp_file_browser_root("command-only-sessions");
        let data_dir = SinomoDataDir::new(root.clone());
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

        fs::remove_dir_all(root).ok();
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
        let data_dir = SinomoDataDir::new(root.clone());
        let mut handler = MainAgentInteractionHandler::from_runtime_config(
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

        fs::remove_dir_all(root).ok();
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

    pub fn session(&self) -> &InteractionSession {
        &self.session
    }
}

#[derive(Clone, Debug)]
pub struct ModelBackedInteractionHandler<M> {
    model: M,
    provider: String,
    model_name: String,
}

pub type CliInteractionHandler = ModelBackedInteractionHandler<CliConversationModel>;

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
            };
        }
        TuiAgentRuntimeInfo {
            agent: "main-agent".to_string(),
            provider: default_main_agent_chat_model_provider().to_string(),
            model: default_main_agent_chat_model_id().to_string(),
            account: None,
            reasoning_effort: None,
            reasoning_budget_tokens: None,
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

pub struct MainAgentInteractionHandler<M> {
    agent: MainAgentLoop<M>,
    agent_definition: sinomo_agent::AgentDefinition,
    thinking: ThinkingControl,
    provider: String,
    model_name: String,
    account: Option<String>,
    reasoning_effort: Option<ProviderReasoningEffort>,
    reasoning_budget_tokens: Option<u32>,
}

#[derive(Default)]
pub(crate) struct ThinkingStreamDisplay {
    content: String,
    emitted: usize,
    announced: bool,
    prompt_started_thinking: bool,
}

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

fn progress_stream_delta(delta: &str) -> Option<&str> {
    delta
        .strip_prefix("<sinomo_progress>")
        .and_then(|value| value.strip_suffix("</sinomo_progress>"))
}

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

impl<M: InteractionModel> MainAgentInteractionHandler<M> {
    pub fn new(model: M, provider: impl Into<String>, model_name: impl Into<String>) -> Self {
        let provider = provider.into();
        let model_name = model_name.into();
        let agent_definition = sinomo_agent::AgentDefinition::main();
        Self {
            agent: MainAgentLoop::with_definition(
                model,
                provider.clone(),
                model_name.clone(),
                agent_definition.clone(),
            ),
            agent_definition,
            thinking: ThinkingControl::default(),
            provider,
            model_name,
            account: None,
            reasoning_effort: None,
            reasoning_budget_tokens: None,
        }
    }

    pub fn with_thinking(
        model: M,
        provider: impl Into<String>,
        model_name: impl Into<String>,
        thinking: ThinkingControl,
    ) -> Self {
        let provider = provider.into();
        let model_name = model_name.into();
        let agent_definition = sinomo_agent::AgentDefinition::main();
        Self {
            agent: MainAgentLoop::with_definition(
                model,
                provider.clone(),
                model_name.clone(),
                agent_definition.clone(),
            ),
            agent_definition,
            thinking,
            provider,
            model_name,
            account: None,
            reasoning_effort: None,
            reasoning_budget_tokens: None,
        }
    }

    pub fn with_agent_definition(mut self, definition: sinomo_agent::AgentDefinition) -> Self {
        self.agent = self.agent.with_agent_definition(definition.clone());
        self.agent_definition = definition;
        self
    }

    fn runtime_info(&self) -> TuiAgentRuntimeInfo {
        TuiAgentRuntimeInfo {
            agent: self.agent_definition.name.clone(),
            provider: self.provider.clone(),
            model: self.model_name.clone(),
            account: self.account.clone(),
            reasoning_effort: self.reasoning_effort,
            reasoning_budget_tokens: self.reasoning_budget_tokens,
        }
    }

    fn replace_model(
        &mut self,
        model: M,
        provider: String,
        model_name: String,
        account: Option<String>,
        options: LanguageModelProviderOptions,
    ) -> TuiAgentRuntimeInfo {
        self.agent = MainAgentLoop::with_definition(
            model,
            provider.clone(),
            model_name.clone(),
            self.agent_definition.clone(),
        );
        self.provider = provider;
        self.model_name = model_name;
        self.account = account;
        self.reasoning_effort = options.reasoning_effort;
        self.reasoning_budget_tokens = options.reasoning_budget_tokens;
        self.runtime_info()
    }
}

impl MainAgentInteractionHandler<DefaultMainAgentChatModel> {
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
        Ok(Self::with_thinking(
            model,
            runtime_info.provider,
            runtime_info.model,
            thinking,
        ))
    }

    fn switch_to_selection(
        &mut self,
        data_dir: &SinomoDataDir,
        selection: TuiModelSelection,
        daemon_connected: bool,
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
                    Ok(self.replace_model(
                        model,
                        info.provider,
                        info.model,
                        info.account,
                        LanguageModelProviderOptions::default(),
                    ))
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
                    Ok(self.replace_model(
                        model,
                        MINICPM_PROVIDER_ID.to_string(),
                        model_name,
                        None,
                        LanguageModelProviderOptions::default(),
                    ))
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
                    Ok(self.replace_model(
                        model,
                        provider.to_string(),
                        selection.model,
                        Some(selection.account),
                        options,
                    ))
                }
            }
        })
    }
}

impl TuiAgentRuntimeControl for MainAgentInteractionHandler<DefaultMainAgentChatModel> {
    fn tui_agent_runtime_info(&self) -> TuiAgentRuntimeInfo {
        self.runtime_info()
    }

    fn switch_tui_model(
        &mut self,
        data_dir: &SinomoDataDir,
        selection: TuiModelSelection,
        daemon_connected: bool,
    ) -> Result<TuiAgentRuntimeInfo, CliError> {
        self.switch_to_selection(data_dir, selection, daemon_connected)
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

impl<M: InteractionModel> InteractionEventHandler for MainAgentInteractionHandler<M> {
    fn handle_event<S: EventStore>(
        &mut self,
        store: &mut S,
        session: &InteractionSession,
        event: &EventEnvelope,
    ) -> Result<Vec<InteractionOutput>, CliError> {
        self.handle_main_agent_event(store, session, event, None)
    }

    fn handle_event_streaming<S: EventStore>(
        &mut self,
        store: &mut S,
        session: &InteractionSession,
        event: &EventEnvelope,
        emit: &mut dyn FnMut(InteractionOutput),
    ) -> Result<Vec<InteractionOutput>, CliError> {
        self.handle_main_agent_event(store, session, event, Some(emit))
    }
}

impl<M: InteractionModel> MainAgentInteractionHandler<M> {
    fn handle_main_agent_event<S: EventStore>(
        &mut self,
        store: &mut S,
        session: &InteractionSession,
        event: &EventEnvelope,
        mut emit: Option<&mut dyn FnMut(InteractionOutput)>,
    ) -> Result<Vec<InteractionOutput>, CliError> {
        let text = event
            .payload
            .get("text")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();

        if let Some(args) = parse_slash_command(text)? {
            if let Some(output) =
                handle_thinking_command(store, session, event, &args, &self.thinking)?
            {
                return Ok(output);
            }
            return handle_slash_command(store, session, event, args);
        }

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

        let context = BoundedContextBuilder::new(12).build(store, session.session_id.clone())?;
        let input = AgentLoopInput {
            session: session.clone(),
            latest_event: message.clone(),
            context,
            async_update: None,
            context_attachments: Vec::new(),
            tool_specs: Vec::new(),
        };
        let requested = interaction_event(
            session,
            "interaction.main_agent.requested",
            serde_json::json!({
                "message_event_id": message.event_id,
                "input_event_id": event.event_id
            }),
        )
        .caused_by(&message.event_id);
        store.append(requested.clone())?;

        let mut thinking_stream = ThinkingStreamDisplay::new(self.thinking.is_enabled());
        let decision_result = if let Some(emit) = emit.as_deref_mut() {
            self.agent
                .decide_streaming_with_usage(&input, &mut |delta| {
                    for chunk in thinking_stream.push(delta) {
                        emit(InteractionOutput::Chunk(chunk));
                    }
                })
        } else {
            self.agent.decide_with_usage(&input)
        };

        match decision_result {
            Ok((decision, usage)) => {
                let response_event = interaction_event(
                    session,
                    "interaction.main_agent.responded",
                    serde_json::json!({
                        "message_event_id": message.event_id,
                        "request_event_id": requested.event_id,
                        "response_mode": decision.response_mode,
                        "text": decision.text,
                        "tool_calls": decision.tool_calls,
                        "background_handoff": decision.background_handoff,
                        "next_state": decision.next_state,
                        "usage": usage.clone()
                    }),
                )
                .caused_by(&requested.event_id);
                store.append(response_event.clone())?;
                let text = response_event
                    .payload
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                let mut outputs = vec![InteractionOutput::Line(format!("sinomo: {text}"))];
                if let Some(usage) = usage {
                    outputs.push(InteractionOutput::ModelUsage {
                        provider: self.provider.clone(),
                        model: self.model_name.clone(),
                        usage,
                    });
                }
                Ok(outputs)
            }
            Err(error) => {
                let failure = interaction_event(
                    session,
                    "interaction.main_agent.failed",
                    serde_json::json!({
                        "message_event_id": message.event_id,
                        "request_event_id": requested.event_id,
                        "error": error.to_string()
                    }),
                )
                .caused_by(&requested.event_id);
                store.append(failure)?;
                Err(error.into())
            }
        }
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
            let request = interaction_model_request(
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

        execute_slash_command(store, session, event, args)
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
            != Some("main-agent")
        {
            return None;
        }
        main_agent_chat_prompt_request(request, thinking)
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

fn main_agent_chat_prompt_request(
    request: &LanguageModelRequest,
    thinking: ThinkingMode,
) -> Option<ChatPromptRequest> {
    let user_content = request
        .messages
        .iter()
        .rev()
        .find(|message| matches!(message.role, sinomo_lm::LanguageModelRole::User))?
        .content
        .as_str();
    let input: serde_json::Value = serde_json::from_str(user_content).ok()?;
    let user_text = input
        .pointer("/latest_event/payload/text")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if user_text.trim().is_empty() {
        return None;
    }
    let system_prompt = request
        .messages
        .iter()
        .find(|message| matches!(message.role, sinomo_lm::LanguageModelRole::System))
        .map(|message| message.content.as_str())
        .unwrap_or("You are Sinomo, the user-facing main agent.")
        .lines()
        .take_while(|line| !line.trim_start().starts_with("Return only JSON"))
        .collect::<Vec<_>>()
        .join("\n");
    let node_id = input
        .pointer("/session/node_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let cluster_summary = input
        .pointer("/context/events")
        .and_then(serde_json::Value::as_array)
        .and_then(|events| {
            events.iter().rev().find_map(|event| {
                let kind = event
                    .get("kind")
                    .or_else(|| event.pointer("/event/kind"))
                    .and_then(serde_json::Value::as_str);
                if kind != Some("interaction.cluster.context") {
                    return None;
                }
                let payload = event
                    .get("payload")
                    .or_else(|| event.pointer("/event/payload"))?;
                let daemon = payload.get("daemon")?;
                let address = daemon
                    .get("address")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown");
                let cluster_id = daemon
                    .get("cluster")
                    .and_then(|cluster| cluster.get("cluster_id"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown");
                Some(format!("cluster {cluster_id} through daemon {address}"))
            })
        })
        .unwrap_or_else(|| "cluster context unavailable".to_string());
    let tools = input
        .get("tool_specs")
        .and_then(serde_json::Value::as_array)
        .map(|tools| chat_prompt_tools_from_agent_specs(tools))
        .unwrap_or_default();
    let user_prompt = format!("Node: {node_id}\nCluster: {cluster_summary}\n\n{user_text}");
    Some(ChatPromptRequest {
        system_prompt,
        user_prompt,
        tools,
        thinking,
        response_format: request.response_format.clone(),
    })
}

fn chat_prompt_tools_from_agent_specs(tools: &[serde_json::Value]) -> Vec<ChatPromptTool> {
    tools
        .iter()
        .take(10)
        .map(|tool| ChatPromptTool {
            name: tool
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown_tool")
                .to_string(),
            description: tool
                .get("description")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "additionalProperties": true
            }),
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

#[derive(Clone, Debug)]
pub struct CliConversationModel {
    provider: String,
    account: String,
    model: String,
    model_config: sinomo_lm::ModelConfig,
    data_dir: SinomoDataDir,
}

impl CliConversationModel {
    pub fn from_defaults() -> Result<Self, CliError> {
        let data_dir = SinomoDataDir::from_env_or_home()?;
        data_dir.ensure_root_layout()?;
        let paths = EvolutionPaths::new(&data_dir);
        paths.ensure()?;
        let evolution_config = read_config(&paths).ok();
        let (model_config, _) = load_model_config_for_node(&data_dir)?;
        let evolution_model_config =
            evolution_config
                .as_ref()
                .map(|config| sinomo_lm::ModelConfig {
                    default_provider: config.default_provider.clone(),
                    default_account: None,
                    default_model: config.default_model.clone(),
                    accounts: Vec::new(),
                });
        let resolved = resolve_default_model(Some(&model_config), evolution_model_config.as_ref())?;
        Ok(Self {
            provider: resolved.provider,
            account: resolved.account,
            model: resolved.model,
            model_config,
            data_dir,
        })
    }
}

impl InteractionModel for CliConversationModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let account =
            provider_account_config(&self.model_config, &self.provider, Some(&self.account));
        complete_configured_model_request_with_account(
            &self.data_dir,
            &self.provider,
            &self.model,
            account,
            env::current_dir().ok(),
            request,
        )
    }
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
