use super::*;
#[cfg(feature = "native-models")]
use candle_core::{DType, Device, Tensor};
use crossterm::{
    event::{self, Event as CrosstermEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Position, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};
#[cfg(feature = "native-models")]
use sinomo_models::minicpm_v46::{
    DEFAULT_MODEL_ID as MINICPM_V46_DEFAULT_MODEL_ID, MiniCpmV46Config,
    MiniCpmV46ForConditionalGeneration, Qwen3_5TextCache,
};
use std::io::IsTerminal;
use std::sync::{Arc, Mutex};
#[cfg(feature = "native-models")]
use tokenizers::Tokenizer;

const RUST_MINICPM_V46_PROVIDER_ID: &str = "sinomo-models";
#[cfg(feature = "native-models")]
const DEFAULT_RUST_MINICPM_V46_CACHE_SUBDIR: &str = "openbmb--MiniCPM-V-4_6";

pub fn run_cui() -> Result<(), CliError> {
    if io::stdin().is_terminal() && io::stdout().is_terminal() {
        let daemon = default_main_daemon_status()?;
        let (handler, daemon) = main_agent_handler_for_cui(daemon)?;
        return run_cui_tui_with_handler_and_daemon(handler, daemon);
    }

    let stdin = io::stdin();
    let stdout = io::stdout();
    run_cui_with_io(stdin.lock(), stdout.lock())
}

pub fn run_cui_with_io<R: BufRead, W: Write>(mut reader: R, mut writer: W) -> Result<(), CliError> {
    let daemon = default_main_daemon_status()?;
    run_cui_with_io_and_daemon_probe(&mut reader, &mut writer, daemon)
}

pub(super) fn run_cui_with_io_and_daemon_probe<R: BufRead, W: Write>(
    reader: &mut R,
    writer: &mut W,
    daemon: Option<DaemonStatus>,
) -> Result<(), CliError> {
    let (handler, daemon) = main_agent_handler_for_cui(daemon)?;
    run_cui_with_handler_io_and_daemon(reader, writer, handler, daemon)
}

fn main_agent_handler_for_cui(
    daemon: Option<DaemonStatus>,
) -> Result<
    (
        MainAgentInteractionHandler<DefaultMainAgentChatModel>,
        Option<DaemonStatus>,
    ),
    CliError,
> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    main_agent_handler_for_cui_with_data_dir(daemon, &data_dir)
}

pub(crate) fn main_agent_handler_for_cui_with_data_dir(
    daemon: Option<DaemonStatus>,
    data_dir: &SinomoDataDir,
) -> Result<
    (
        MainAgentInteractionHandler<DefaultMainAgentChatModel>,
        Option<DaemonStatus>,
    ),
    CliError,
> {
    let (runtime_config, _) = load_cui_model_runtime_config(data_dir, daemon.is_some())?;
    let handler = MainAgentInteractionHandler::from_runtime_config(&data_dir, runtime_config)?;
    Ok((handler, daemon))
}

pub(crate) fn load_cui_model_runtime_config(
    data_dir: &SinomoDataDir,
    daemon_connected: bool,
) -> Result<(ModelRuntimeConfig, PathBuf), CliError> {
    if daemon_connected || cli_force_local_mode() {
        return load_model_runtime_config_for_node(data_dir);
    }

    let _guard = enter_cli_force_local_mode(true);
    load_model_runtime_config_for_node(data_dir)
}

pub fn run_cui_with_handler_io<R: BufRead, W: Write, H: InteractionEventHandler>(
    mut reader: R,
    mut writer: W,
    handler: H,
) -> Result<(), CliError> {
    run_cui_with_handler_io_and_daemon(&mut reader, &mut writer, handler, None)
}

pub(super) fn run_cui_with_handler_io_and_daemon<
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
    let node_id = daemon
        .and_then(|status| status.cluster.as_ref())
        .map(|cluster| cluster.node_id.clone())
        .unwrap_or_else(|| NodeId::from_legacy_named("node_cli").expect("valid node id"));
    let session = InteractionSession::new(
        SessionId::generate(),
        node_id,
        InteractionSurface::new("surface.cli", InteractionSurfaceKind::CommandLine),
    );
    let mut runtime = InteractionRuntime::new(InMemoryEventStore::new(), handler, session.clone());
    runtime.append_session_context(daemon_context_event(&session, daemon))?;
    Ok(runtime)
}

fn run_cui_tui_with_handler_and_daemon<H: InteractionEventHandler>(
    handler: H,
    daemon: Option<DaemonStatus>,
) -> Result<(), CliError> {
    let mut runtime = new_interaction_runtime(handler, daemon.as_ref())?;
    let mut state = CuiTuiState::new(runtime.session(), daemon.as_ref());
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
                match state.apply_key_event(key) {
                    TuiInputAction::None => {}
                    TuiInputAction::Exit => {
                        submit_tui_input(
                            &mut runtime,
                            &mut state,
                            &mut terminal,
                            "/quit".to_string(),
                        )?;
                    }
                    TuiInputAction::Submit(line) => {
                        submit_tui_input(&mut runtime, &mut state, &mut terminal, line)?;
                    }
                }
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

fn submit_tui_input<H: InteractionEventHandler>(
    runtime: &mut InteractionRuntime<InMemoryEventStore, H>,
    state: &mut CuiTuiState,
    terminal: &mut TuiTerminalSession,
    line: String,
) -> Result<(), CliError> {
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
    for output in result? {
        state.apply_output(output);
    }
    state.finish_submission();
    Ok(())
}

struct TuiTerminalSession {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
}

impl TuiTerminalSession {
    fn enter() -> Result<Self, CliError> {
        enable_raw_mode().map_err(CliError::stdio)?;
        let mut stdout = io::stdout();
        if let Err(error) = execute!(stdout, EnterAlternateScreen) {
            let _ = disable_raw_mode();
            return Err(CliError::stdio(error));
        }
        let terminal = match Terminal::new(CrosstermBackend::new(stdout)) {
            Ok(terminal) => terminal,
            Err(error) => {
                let _ = disable_raw_mode();
                let mut stdout = io::stdout();
                let _ = execute!(stdout, LeaveAlternateScreen);
                return Err(CliError::stdio(error));
            }
        };
        Ok(Self { terminal })
    }

    fn draw(&mut self, state: &CuiTuiState) -> Result<(), CliError> {
        self.terminal
            .draw(|frame| render_cui_tui(frame, state))
            .map(|_| ())
            .map_err(CliError::stdio)
    }
}

impl Drop for TuiTerminalSession {
    fn drop(&mut self) {
        let _ = self.terminal.show_cursor();
        let _ = disable_raw_mode();
        let mut stdout = io::stdout();
        let _ = execute!(stdout, LeaveAlternateScreen);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TuiInputAction {
    None,
    Submit(String),
    Exit,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TuiCompletionState {
    replace_start: usize,
    replace_end: usize,
    selected: Option<usize>,
    items: Vec<SlashCompletionItem>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TuiMessageKind {
    User,
    Command,
    Agent,
    Thinking,
    System,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TuiMessage {
    kind: TuiMessageKind,
    text: String,
}

#[derive(Clone, Debug)]
struct CuiTuiState {
    session_id: String,
    node_id: String,
    daemon_label: String,
    cluster_label: Option<String>,
    input: String,
    input_cursor: usize,
    messages: Vec<TuiMessage>,
    stream_message: Option<usize>,
    completions: Option<TuiCompletionState>,
    status: String,
    scrollback: u16,
    exit_requested: bool,
}

impl CuiTuiState {
    fn new(session: &InteractionSession, daemon: Option<&DaemonStatus>) -> Self {
        let daemon_label = daemon
            .map(|status| format!("daemon pid={} address={}", status.pid, status.address))
            .unwrap_or_else(|| "daemon not connected".to_string());
        let cluster_label = daemon
            .and_then(|status| status.cluster.as_ref())
            .map(|cluster| format!("cluster {}", cluster.cluster_id));
        let mut state = Self {
            session_id: session.session_id.to_string(),
            node_id: session.node_id.to_string(),
            daemon_label,
            cluster_label,
            input: String::new(),
            input_cursor: 0,
            messages: Vec::new(),
            stream_message: None,
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
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.input.clear();
                self.input_cursor = 0;
                self.refresh_completions();
                TuiInputAction::None
            }
            KeyCode::Esc => TuiInputAction::Exit,
            KeyCode::Enter => self.take_submission(),
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
                self.move_cursor_left();
                TuiInputAction::None
            }
            KeyCode::Right => {
                if self.completions.is_some() {
                    self.accept_completion();
                } else {
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

    fn apply_output(&mut self, output: InteractionOutput) {
        match output {
            InteractionOutput::Chunk(chunk) => self.append_chunk(&chunk),
            InteractionOutput::Line(line) => self.append_line(&line),
            InteractionOutput::Exit => self.request_exit(),
        }
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
        } else if line.starts_with("command failed:") {
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
        self.completions =
            slash_completions(&self.input, self.input_cursor).map(|set| TuiCompletionState {
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

    fn accept_completion(&mut self) {
        if self.completions.is_none() {
            return;
        }
        let needs_preview = self
            .completions
            .as_ref()
            .and_then(|completions| completions.selected)
            .is_none();
        if needs_preview {
            if let Some(completions) = self.completions.as_mut() {
                completions.selected = Some(0);
            }
            self.apply_selected_completion_preview();
        }
        self.refresh_completions();
    }

    fn apply_selected_completion_preview(&mut self) {
        let Some(completions) = self.completions.as_mut() else {
            return;
        };
        let Some(selected) = completions.selected else {
            return;
        };
        let Some(insert) = completions
            .items
            .get(selected)
            .and_then(|item| item.insert.as_ref())
            .cloned()
        else {
            return;
        };
        if completions.replace_start > completions.replace_end
            || completions.replace_end > self.input.len()
        {
            return;
        }
        self.input
            .replace_range(completions.replace_start..completions.replace_end, &insert);
        completions.replace_end = completions.replace_start + insert.len();
        self.input_cursor = completions.replace_end;
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

fn render_cui_tui(frame: &mut Frame<'_>, state: &CuiTuiState) {
    let area = frame.area();
    let completion_height = state
        .completions
        .as_ref()
        .map(|completions| (completions.items.len() as u16 + 2).clamp(3, 8))
        .unwrap_or(0);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Min(1),
            Constraint::Length(completion_height),
            Constraint::Length(3),
            Constraint::Length(1),
        ])
        .split(area);

    render_tui_header(frame, chunks[0], state);
    render_tui_transcript(frame, chunks[1], state);
    if completion_height > 0 {
        render_tui_completions(frame, chunks[2], state);
    }
    render_tui_input(frame, chunks[3], state);
    render_tui_status(frame, chunks[4], state);
}

fn render_tui_header(frame: &mut Frame<'_>, area: Rect, state: &CuiTuiState) {
    let mut runtime_line = vec![
        Span::styled("session ", Style::default().fg(Color::DarkGray)),
        Span::raw(state.session_id.clone()),
        Span::raw("  "),
        Span::styled("node ", Style::default().fg(Color::DarkGray)),
        Span::raw(state.node_id.clone()),
    ];
    if let Some(cluster) = &state.cluster_label {
        runtime_line.push(Span::raw("  "));
        runtime_line.push(Span::styled(
            "cluster ",
            Style::default().fg(Color::DarkGray),
        ));
        runtime_line.push(Span::raw(
            cluster
                .strip_prefix("cluster ")
                .unwrap_or(cluster)
                .to_string(),
        ));
    }
    let text = Text::from(vec![
        Line::from(runtime_line),
        Line::from(vec![
            Span::styled("runtime ", Style::default().fg(Color::DarkGray)),
            Span::raw(state.daemon_label.clone()),
        ]),
    ]);
    let block = Block::default()
        .title("Sinomo")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));
    frame.render_widget(Paragraph::new(text).block(block), area);
}

fn render_tui_transcript(frame: &mut Frame<'_>, area: Rect, state: &CuiTuiState) {
    let lines = tui_message_lines(&state.messages);
    let visible_height = area.height.saturating_sub(2);
    let max_scroll = lines
        .len()
        .saturating_sub(usize::from(visible_height))
        .min(usize::from(u16::MAX)) as u16;
    let scroll = max_scroll.saturating_sub(state.scrollback.min(max_scroll));
    let block = Block::default()
        .title("Session")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));
    let transcript = Paragraph::new(Text::from(lines))
        .block(block)
        .wrap(Wrap { trim: false })
        .scroll((scroll, 0));
    frame.render_widget(transcript, area);
}

fn render_tui_completions(frame: &mut Frame<'_>, area: Rect, state: &CuiTuiState) {
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

fn render_tui_input(frame: &mut Frame<'_>, area: Rect, state: &CuiTuiState) {
    let inner_width = area.width.saturating_sub(2).max(1);
    let display_width = inner_width.saturating_sub(1).max(1);
    let (input, cursor_column) = state.input_view(display_width);
    let block = Block::default()
        .title("Message")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Green));
    frame.render_widget(Paragraph::new(input).block(block), area);
    if area.width > 2 && area.height > 2 {
        let cursor_x = area
            .x
            .saturating_add(1)
            .saturating_add(cursor_column.min(inner_width.saturating_sub(1)));
        let cursor_y = area.y.saturating_add(1);
        frame.set_cursor_position(Position::new(cursor_x, cursor_y));
    }
}

fn render_tui_status(frame: &mut Frame<'_>, area: Rect, state: &CuiTuiState) {
    let status_style = match state.status.as_str() {
        "working" | "streaming" => Style::default().fg(Color::Yellow),
        "ending session" => Style::default().fg(Color::Red),
        _ => Style::default().fg(Color::Green),
    };
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
        Span::styled("Tab/Right", Style::default().fg(Color::DarkGray)),
        Span::raw(" accept   "),
        Span::styled("Esc", Style::default().fg(Color::DarkGray)),
        Span::raw(" quit   "),
        Span::styled("PgUp/PgDn", Style::default().fg(Color::DarkGray)),
        Span::raw(" scroll"),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

fn tui_message_lines(messages: &[TuiMessage]) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    for message in messages {
        let prefix = tui_message_prefix(&message.kind);
        let style = tui_message_style(&message.kind);
        if message.text.is_empty() {
            lines.push(Line::from(vec![Span::styled(prefix, style)]));
            continue;
        }
        for (line_index, text) in message.text.lines().enumerate() {
            let display_prefix = if line_index == 0 {
                prefix.to_string()
            } else {
                " ".repeat(prefix.len())
            };
            lines.push(Line::from(vec![
                Span::styled(display_prefix, style),
                Span::raw(text.to_string()),
            ]));
        }
    }
    lines
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

    fn fixture_tui_state() -> CuiTuiState {
        let session = InteractionSession::new(
            SessionId::generate(),
            NodeId::from_legacy_named("node_test").expect("node"),
            InteractionSurface::new("surface.test", InteractionSurfaceKind::CommandLine),
        );
        CuiTuiState::new(&session, None)
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
    fn completion_visible_range_keeps_selection_in_view() {
        assert_eq!(completion_visible_range(20, None, 6), (0, 6));
        assert_eq!(completion_visible_range(20, Some(0), 6), (0, 6));
        assert_eq!(completion_visible_range(20, Some(7), 6), (4, 10));
        assert_eq!(completion_visible_range(20, Some(19), 6), (14, 20));
        assert_eq!(completion_visible_range(4, Some(3), 6), (0, 4));
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
    thinking: ThinkingControl,
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
        Self {
            agent: MainAgentLoop::new(model, provider, model_name),
            thinking: ThinkingControl::default(),
        }
    }

    pub fn with_thinking(
        model: M,
        provider: impl Into<String>,
        model_name: impl Into<String>,
        thinking: ThinkingControl,
    ) -> Self {
        Self {
            agent: MainAgentLoop::new(model, provider, model_name),
            thinking,
        }
    }
}

impl MainAgentInteractionHandler<DefaultMainAgentChatModel> {
    pub fn from_runtime_config(
        data_dir: &SinomoDataDir,
        runtime_config: ModelRuntimeConfig,
    ) -> Result<Self, CliError> {
        let thinking = ThinkingControl::default();
        let model = DefaultMainAgentChatModel::from_runtime_config(
            data_dir,
            runtime_config,
            thinking.clone(),
        );
        Ok(Self::with_thinking(
            model,
            RUST_MINICPM_V46_PROVIDER_ID,
            default_main_agent_chat_model_id(),
            thinking,
        ))
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
            tool_specs: sinomo_agent::default_main_agent_tools(),
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
            self.agent.decide_streaming(&input, &mut |delta| {
                for chunk in thinking_stream.push(delta) {
                    emit(InteractionOutput::Chunk(chunk));
                }
            })
        } else {
            self.agent.decide(&input)
        };

        match decision_result {
            Ok(decision) => {
                let response_event = interaction_event(
                    session,
                    "interaction.main_agent.responded",
                    serde_json::json!({
                        "message_event_id": message.event_id,
                        "request_event_id": requested.event_id,
                        "response_mode": decision.response_mode,
                        "text": decision.text,
                        "tool_calls": decision.tool_calls,
                        "maid_handoff": decision.maid_handoff,
                        "next_state": decision.next_state
                    }),
                )
                .caused_by(&requested.event_id);
                store.append(response_event.clone())?;
                let text = response_event
                    .payload
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                Ok(vec![InteractionOutput::Line(format!("sinomo: {text}"))])
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
        "maid_handoff": null,
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
