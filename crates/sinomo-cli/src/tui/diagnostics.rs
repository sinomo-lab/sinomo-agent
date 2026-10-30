//! Ephemeral diagnostics stay outside the durable user/model conversation.
use super::terminal_io::{OutputSnapshot, TerminalOutput};
use super::*;
use sinomo_runtime::observe::{
    ObserveLogSource, ObserveLogsRequest, ObserveLogsResponse, OperationalSeverity,
};

#[derive(Clone, Debug, Default)]
pub(super) struct TuiDiagnostics {
    pub open: bool,
    scroll: u16,
    raw_version: u64,
    raw: OutputSnapshot,
    logs: ObserveLogsResponse,
}

impl TuiDiagnostics {
    pub fn refresh(&mut self, output: &TerminalOutput) -> bool {
        let mut changed = false;
        if let Some(raw) = output.snapshot(&mut self.raw_version) {
            self.raw = raw;
            changed = true;
        }
        // Runtime-only collection never opens a file or contacts a daemon.
        let logs = sinomo_runtime::observe::read_observe_logs(
            Path::new(""),
            &ObserveLogsRequest {
                source: Some(ObserveLogSource::Runtime),
                limit: 100,
                ..Default::default()
            },
        );
        if logs.records != self.logs.records
            || logs.runtime_evicted != self.logs.runtime_evicted
            || logs.runtime_available != self.logs.runtime_available
            || logs.truncated != self.logs.truncated
        {
            self.logs = logs;
            changed = true;
        }
        changed
    }

    pub fn warnings(&self) -> usize {
        self.logs
            .records
            .iter()
            .filter(|r| r.severity >= OperationalSeverity::Warn)
            .count()
            + self
                .raw
                .lines
                .iter()
                .filter(|r| r.stream == "stderr")
                .count()
    }

    pub fn toggle(&mut self) {
        self.open = !self.open;
        self.scroll = 0;
    }

    pub fn key(&mut self, key: KeyCode) {
        match key {
            KeyCode::Esc | KeyCode::F(5) => self.open = false,
            KeyCode::Up => self.scroll = self.scroll.saturating_sub(1),
            KeyCode::Down => self.scroll = self.scroll.saturating_add(1),
            KeyCode::PageUp => self.scroll = self.scroll.saturating_sub(10),
            KeyCode::PageDown => self.scroll = self.scroll.saturating_add(10),
            KeyCode::Home => self.scroll = 0,
            _ => {}
        }
    }

    pub fn render(&self, frame: &mut Frame<'_>, area: Rect) {
        let mut lines = Vec::new();
        if !self.logs.runtime_available {
            lines.push(Line::from(
                "Structured collector unavailable; raw process output is still captured.",
            ));
        }
        if self.logs.truncated || self.logs.runtime_evicted > 0 || self.raw.evicted > 0 {
            lines.push(Line::from(format!(
                "Bounded tail; older records omitted (Observe {}, process {}).",
                self.logs.runtime_evicted, self.raw.evicted
            )));
        }
        for record in self.logs.records.iter().rev() {
            let style = match record.severity {
                OperationalSeverity::Error => Style::default().fg(Color::Red),
                OperationalSeverity::Warn => Style::default().fg(Color::Yellow),
                _ => Style::default(),
            };
            let text = format!(
                "{:?} {}: {}{}{}",
                record.severity,
                record.component,
                record.message,
                record
                    .operation_id
                    .as_ref()
                    .map(|id| format!(" [operation {id}]"))
                    .unwrap_or_default(),
                if record.truncated { " [truncated]" } else { "" }
            );
            lines.push(Line::styled(display_text(&text), style));
            if !record.fields.is_null() && record.fields != serde_json::json!({}) {
                lines.push(Line::from(display_text(&record.fields.to_string())));
            }
        }
        if !self.raw.lines.is_empty() {
            lines.push(Line::from("Captured process output (newest first):"));
        }
        for record in self.raw.lines.iter().rev() {
            lines.push(Line::from(display_text(&format!(
                "{}: {}{}",
                record.stream,
                record.text,
                if record.truncated { " [truncated]" } else { "" }
            ))));
        }
        if lines.is_empty() {
            lines.push(Line::from("No diagnostics recorded."));
        }
        let scroll = self.scroll.min(
            lines
                .iter()
                .map(|l| {
                    l.width()
                        .div_ceil(usize::from(area.width.saturating_sub(2).max(1)))
                })
                .sum::<usize>()
                .saturating_sub(1)
                .min(u16::MAX as usize) as u16,
        );
        frame.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: false })
                .scroll((scroll, 0))
                .block(Block::default().borders(Borders::ALL).title(format!(
                    " Diagnostics · {} warnings/stderr · F5/Esc close · ↑/↓ scroll ",
                    self.warnings()
                ))),
            area,
        );
    }
}

fn display_text(text: &str) -> String {
    text.chars()
        .flat_map(|c| {
            if c.is_control() {
                c.escape_default().collect::<Vec<_>>()
            } else {
                vec![c]
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostic_text_cannot_inject_terminal_commands() {
        let text = display_text("message\x1b[2J\r\n\x07界");
        assert!(!text.chars().any(char::is_control));
        assert!(text.contains("message") && text.contains('界'));
    }

    #[test]
    fn diagnostics_view_preserves_error_details_input_and_conversation() {
        let session = interaction_session_for_daemon(None, SessionId::generate());
        let mut state = TuiState::new(&session, None, TuiAgentRuntimeInfo::default());
        state.input = "unsent user text".into();
        let messages = state.messages.clone();
        state.diagnostics.logs.runtime_available = true;
        state
            .diagnostics
            .logs
            .records
            .push(sinomo_runtime::observe::OperationalLogRecord {
                timestamp: OffsetDateTime::now_utc(),
                severity: OperationalSeverity::Error,
                component: "test.worker".into(),
                message: "original error details".into(),
                operation_id: Some("original-operation".into()),
                fields: serde_json::json!({"code": "original-code"}),
                source: ObserveLogSource::Runtime,
                truncated: false,
            });
        assert_eq!(state.diagnostics.warnings(), 1);
        assert_eq!(
            state.apply_key_event(KeyEvent::new(KeyCode::F(5), KeyModifiers::NONE)),
            TuiInputAction::None
        );
        assert!(state.diagnostics.open);
        assert_eq!(
            state.apply_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            TuiInputAction::None
        );
        state.apply_key_event(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE));
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(100, 24)).unwrap();
        terminal.draw(|frame| render_tui(frame, &state)).unwrap();
        let screen = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(
            screen.contains("original error details")
                && screen.contains("original-operation")
                && screen.contains("original-code")
                && screen.contains("F5/Esc close"),
            "{screen}"
        );
        state.apply_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(!state.diagnostics.open);
        assert_eq!(state.input, "unsent user text");
        assert_eq!(state.messages, messages);
    }
}
