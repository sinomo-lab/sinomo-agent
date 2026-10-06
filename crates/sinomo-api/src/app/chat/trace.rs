//! Per-turn record of model reasoning, intermediate output, and tool calls.
//!
//! The daemon folds a turn's stream events into a [`ChatWorkTrace`] and stores
//! it with the message that closed the turn; clients fold the same events with
//! the same reducer while the turn is live. Both sides therefore agree on what
//! a turn did, and history reloads show it without replaying events.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{
    ChatProcessSnapshot, ChatToolCallSnapshot, ChatToolOutputFile, ChatToolOutputSnapshot,
};

/// Per-stream byte budget for stored process output. The live trace keeps
/// everything; the stored copy keeps the tail, where results and errors are.
pub const CHAT_TRACE_STORED_OUTPUT_MAX_BYTES: usize = 64 * 1024;

const INPUT_PREVIEW_CHARS: usize = 120;

/// Identity of the model tool call a progress event belongs to.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatToolCallRef {
    pub step_index: usize,
    pub call_index: Option<usize>,
    pub call_id: Option<String>,
    pub tool: String,
}

/// One tool call as it was requested, executed, and observed.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatToolActivity {
    pub step_index: usize,
    pub call_index: Option<usize>,
    pub call_id: Option<String>,
    pub tool: String,
    pub input: String,
    pub input_preview: String,
    pub status: String,
    pub process_command: Option<String>,
    pub process_status: Option<String>,
    pub exit_code: Option<i32>,
    pub elapsed_ms: Option<u64>,
    pub stdout: String,
    pub stderr: String,
    #[serde(default)]
    pub output_files: Vec<ChatToolOutputFile>,
}

impl ChatToolActivity {
    fn matches(
        &self,
        step_index: usize,
        call_index: Option<usize>,
        call_id: Option<&str>,
        tool: Option<&str>,
    ) -> bool {
        if self.step_index != step_index {
            return false;
        }
        if let (Some(existing), Some(call_id)) = (self.call_id.as_deref(), call_id) {
            return existing == call_id;
        }
        if let (Some(existing), Some(call_index)) = (self.call_index, call_index) {
            return existing == call_index;
        }
        tool.is_some_and(|tool| self.tool == tool)
    }

    /// Providers reveal a call's id and position at different times; keep
    /// whichever parts later events supply so they keep matching.
    fn learn_identity(&mut self, call_index: Option<usize>, call_id: Option<&str>) {
        if self.call_index.is_none() {
            self.call_index = call_index;
        }
        if self.call_id.is_none() {
            self.call_id = call_id.map(str::to_string);
        }
    }

    /// Stable key for this call within its turn.
    pub fn key(&self) -> String {
        if let Some(call_id) = &self.call_id {
            return format!("id:{call_id}");
        }
        if let Some(call_index) = self.call_index {
            return format!("step:{}:call:{call_index}", self.step_index);
        }
        format!("step:{}:tool:{}", self.step_index, self.tool)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChatWorkTraceEntry {
    Reasoning { step_index: usize, text: String },
    Intermediate { step_index: usize, text: String },
    Tool(ChatToolActivity),
}

/// Everything a turn did before its reply, in the order it happened.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChatWorkTrace {
    pub entries: Vec<ChatWorkTraceEntry>,
    /// Assistant text of steps still streaming. It becomes an intermediate
    /// entry if the step goes on to call tools, or the reply if it does not.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pending_step_text: BTreeMap<usize, String>,
}

impl ChatWorkTrace {
    pub fn from_entries(entries: Vec<ChatWorkTraceEntry>) -> Self {
        Self {
            entries,
            pending_step_text: BTreeMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether the trace holds reasoning or intermediate output, not only tools.
    pub fn has_narrative(&self) -> bool {
        self.entries
            .iter()
            .any(|entry| !matches!(entry, ChatWorkTraceEntry::Tool(_)))
    }

    pub fn tool_calls(&self) -> impl DoubleEndedIterator<Item = &ChatToolActivity> {
        self.entries.iter().filter_map(|entry| match entry {
            ChatWorkTraceEntry::Tool(call) => Some(call),
            ChatWorkTraceEntry::Reasoning { .. } | ChatWorkTraceEntry::Intermediate { .. } => None,
        })
    }

    pub fn tool_call_count(&self) -> usize {
        self.tool_calls().count()
    }

    fn tool_calls_mut(&mut self) -> impl DoubleEndedIterator<Item = &mut ChatToolActivity> {
        self.entries.iter_mut().filter_map(|entry| match entry {
            ChatWorkTraceEntry::Tool(call) => Some(call),
            ChatWorkTraceEntry::Reasoning { .. } | ChatWorkTraceEntry::Intermediate { .. } => None,
        })
    }

    pub fn assistant_text_delta(&mut self, step_index: usize, text: &str) {
        if text.is_empty() {
            return;
        }
        self.pending_step_text
            .entry(step_index)
            .or_default()
            .push_str(text);
    }

    /// Move a step's streamed assistant text into the trace because the step
    /// called tools. Returns the moved text so a client can take it back out
    /// of the reply it was streaming into.
    pub fn flush_step_text(&mut self, step_index: usize) -> Option<String> {
        let text = self.pending_step_text.remove(&step_index)?;
        if text.trim().is_empty() {
            return None;
        }
        self.entries.push(ChatWorkTraceEntry::Intermediate {
            step_index,
            text: text.clone(),
        });
        Some(text)
    }

    pub fn reasoning_delta(&mut self, step_index: usize, delta: &str) {
        if delta.is_empty() {
            return;
        }
        if let Some(ChatWorkTraceEntry::Reasoning {
            step_index: existing_step,
            text,
        }) = self.entries.last_mut()
            && *existing_step == step_index
        {
            text.push_str(delta);
            return;
        }
        self.entries.push(ChatWorkTraceEntry::Reasoning {
            step_index,
            text: delta.to_string(),
        });
    }

    pub fn reasoning_summary(&mut self, step_index: usize, summary: &str) {
        let summary = summary.trim();
        if summary.is_empty() {
            return;
        }
        if let Some(ChatWorkTraceEntry::Reasoning {
            step_index: existing_step,
            text,
        }) = self.entries.last_mut()
            && *existing_step == step_index
        {
            if !text.is_empty() {
                text.push_str("\n\n");
            }
            text.push_str(summary);
            return;
        }
        self.entries.push(ChatWorkTraceEntry::Reasoning {
            step_index,
            text: summary.to_string(),
        });
    }

    /// The model started requesting a tool. Returns step text moved out of
    /// the reply, as [`Self::flush_step_text`] does.
    pub fn tool_call_started(&mut self, call: &ChatToolCallSnapshot) -> Option<String> {
        let flushed = self.flush_step_text(call.step_index);
        self.upsert_call(call, "running");
        flushed
    }

    /// The model finished assembling a tool request; execution comes next.
    pub fn tool_call_completed(&mut self, call: &ChatToolCallSnapshot) -> Option<String> {
        let flushed = self.flush_step_text(call.step_index);
        self.upsert_call(call, "requested");
        flushed
    }

    pub fn tool_call_delta(
        &mut self,
        step_index: usize,
        call_id: Option<&str>,
        tool: Option<&str>,
        arguments_delta: &str,
    ) {
        let Some(existing) = self
            .tool_calls_mut()
            .rev()
            .find(|existing| existing.matches(step_index, None, call_id, tool))
        else {
            return;
        };
        existing.learn_identity(None, call_id);
        if let Some(tool) = tool.filter(|tool| !tool.trim().is_empty()) {
            existing.tool = tool.to_string();
        }
        if !arguments_delta.is_empty() {
            existing.input.push_str(arguments_delta);
            existing.input_preview = compact_line(&existing.input, INPUT_PREVIEW_CHARS);
        }
        existing.status = "streaming".to_string();
    }

    pub fn tool_output(&mut self, output: &ChatToolOutputSnapshot) {
        let call = self.upsert_call(&output.call, &output.status);
        call.output_files = output.files.clone();
    }

    /// Fold process progress into the call that ran it. Progress carries
    /// output deltas, so they are appended. Returns whether a call took it.
    pub fn process(&mut self, process: &ChatProcessSnapshot) -> bool {
        let command = process.command.trim();
        let call = match &process.call {
            Some(target) => self.tool_calls_mut().rev().find(|call| {
                call.matches(
                    target.step_index,
                    target.call_index,
                    target.call_id.as_deref(),
                    Some(&target.tool),
                )
            }),
            // Unscoped progress only attaches to a call that names the command.
            None if !command.is_empty() => self.tool_calls_mut().rev().find(|call| {
                call.process_command.as_deref() == Some(command) || call.input.contains(command)
            }),
            None => None,
        };
        let Some(call) = call else {
            return false;
        };
        if let Some(target) = &process.call {
            call.learn_identity(target.call_index, target.call_id.as_deref());
        }
        call.process_command = Some(process.command.clone());
        call.process_status = Some(process.status.clone());
        call.exit_code = process.exit_code;
        call.elapsed_ms = Some(process.elapsed_ms);
        call.stdout.push_str(&process.stdout);
        call.stderr.push_str(&process.stderr);
        true
    }

    /// The turn ended. Text still pending belongs to the reply, not the trace.
    pub fn finish(&mut self) {
        self.pending_step_text.clear();
    }

    /// The finished trace with process output trimmed for durable storage.
    pub fn bounded_for_storage(mut self) -> Self {
        self.finish();
        for call in self.tool_calls_mut() {
            keep_tail(&mut call.stdout, CHAT_TRACE_STORED_OUTPUT_MAX_BYTES);
            keep_tail(&mut call.stderr, CHAT_TRACE_STORED_OUTPUT_MAX_BYTES);
        }
        self
    }

    fn upsert_call(&mut self, call: &ChatToolCallSnapshot, status: &str) -> &mut ChatToolActivity {
        let input = call
            .input
            .clone()
            .or_else(|| call.input_preview.clone())
            .unwrap_or_default();
        let input_preview = call
            .input_preview
            .as_deref()
            .filter(|preview| !preview.trim().is_empty())
            .map(|preview| compact_line(preview, INPUT_PREVIEW_CHARS))
            .unwrap_or_else(|| compact_line(&input, INPUT_PREVIEW_CHARS));
        let index = self.entries.iter().rposition(|entry| {
            matches!(entry, ChatWorkTraceEntry::Tool(existing) if existing.matches(
                call.step_index,
                call.call_index,
                call.call_id.as_deref(),
                Some(&call.tool),
            ))
        });
        let index = index.unwrap_or_else(|| {
            self.entries
                .push(ChatWorkTraceEntry::Tool(ChatToolActivity {
                    step_index: call.step_index,
                    call_index: call.call_index,
                    call_id: call.call_id.clone(),
                    tool: call.tool.clone(),
                    ..ChatToolActivity::default()
                }));
            self.entries.len() - 1
        });
        let ChatWorkTraceEntry::Tool(existing) = &mut self.entries[index] else {
            unreachable!("index points at a tool entry");
        };
        existing.learn_identity(call.call_index, call.call_id.as_deref());
        existing.tool = call.tool.clone();
        if !input_preview.trim().is_empty() {
            existing.input_preview = input_preview;
        }
        if !input.trim().is_empty() {
            existing.input = input;
        }
        existing.status = status.to_string();
        existing
    }
}

fn compact_line(text: &str, max_chars: usize) -> String {
    let line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if line.chars().count() <= max_chars {
        return line;
    }
    let mut out = line
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>();
    out.push('…');
    out
}

fn keep_tail(text: &mut String, max_bytes: usize) {
    if text.len() <= max_bytes {
        return;
    }
    let mut start = text.len() - max_bytes;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    let dropped = start;
    text.replace_range(..start, &format!("[{dropped} earlier bytes omitted]\n"));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(
        step_index: usize,
        call_index: Option<usize>,
        call_id: Option<&str>,
    ) -> ChatToolCallSnapshot {
        ChatToolCallSnapshot {
            step_index,
            call_index,
            call_id: call_id.map(str::to_string),
            tool: "exec_command".to_string(),
            input: None,
            input_preview: None,
        }
    }

    fn process(
        command: &str,
        call: Option<ChatToolCallRef>,
        status: &str,
        stdout: &str,
    ) -> ChatProcessSnapshot {
        ChatProcessSnapshot {
            process_id: "p".to_string(),
            command: command.to_string(),
            status: status.to_string(),
            stdout: stdout.to_string(),
            call,
            ..ChatProcessSnapshot::default()
        }
    }

    #[test]
    fn step_text_before_a_tool_call_becomes_intermediate_and_the_last_step_stays_the_reply() {
        let mut trace = ChatWorkTrace::default();
        trace.reasoning_delta(0, "look ");
        trace.reasoning_delta(0, "first");
        trace.assistant_text_delta(0, "Checking.");
        assert_eq!(
            trace.tool_call_started(&call(0, None, Some("a"))),
            Some("Checking.".to_string())
        );
        trace.assistant_text_delta(1, "Done.");
        trace.finish();

        assert_eq!(
            trace.entries,
            vec![
                ChatWorkTraceEntry::Reasoning {
                    step_index: 0,
                    text: "look first".to_string()
                },
                ChatWorkTraceEntry::Intermediate {
                    step_index: 0,
                    text: "Checking.".to_string()
                },
                ChatWorkTraceEntry::Tool(ChatToolActivity {
                    step_index: 0,
                    call_id: Some("a".to_string()),
                    tool: "exec_command".to_string(),
                    status: "running".to_string(),
                    ..ChatToolActivity::default()
                }),
            ]
        );
    }

    #[test]
    fn calls_merge_identity_across_events_and_keep_every_call() {
        let mut trace = ChatWorkTrace::default();
        for index in 0..20 {
            let id = format!("call-{index}");
            trace.tool_call_started(&call(0, None, Some(&id)));
            trace.tool_call_completed(&call(0, Some(index), Some(&id)));
        }
        assert_eq!(trace.tool_call_count(), 20);
        let last = trace.tool_calls().last().unwrap();
        assert_eq!(last.call_index, Some(19));
        assert_eq!(last.status, "requested");
    }

    #[test]
    fn scoped_process_output_reaches_its_own_call_and_accumulates_deltas() {
        let mut trace = ChatWorkTrace::default();
        trace.tool_call_completed(&call(0, Some(0), None));
        trace.tool_call_completed(&call(0, Some(1), None));
        let first = Some(ChatToolCallRef {
            step_index: 0,
            call_index: Some(0),
            call_id: None,
            tool: "exec_command".to_string(),
        });
        // Both calls run the same command in parallel; scope decides.
        assert!(trace.process(&process("cargo test", first.clone(), "running", "one\n")));
        assert!(trace.process(&process("cargo test", first, "completed", "two\n")));

        let calls = trace.tool_calls().collect::<Vec<_>>();
        assert_eq!(calls[0].stdout, "one\ntwo\n");
        assert_eq!(calls[0].process_status.as_deref(), Some("completed"));
        assert!(calls[1].stdout.is_empty());
        assert!(calls[1].process_command.is_none());
    }

    #[test]
    fn unscoped_process_output_never_falls_back_to_an_unrelated_call() {
        let mut trace = ChatWorkTrace::default();
        trace.tool_call_completed(&call(0, Some(0), None));
        assert!(!trace.process(&process("ls", None, "completed", "x")));
        assert!(trace.tool_calls().next().unwrap().stdout.is_empty());
    }

    #[test]
    fn stored_trace_keeps_the_output_tail_and_drops_pending_reply_text() {
        let mut trace = ChatWorkTrace::default();
        trace.tool_call_completed(&call(0, Some(0), None));
        let scope = Some(ChatToolCallRef {
            step_index: 0,
            call_index: Some(0),
            call_id: None,
            tool: "exec_command".to_string(),
        });
        let long = format!("{}END", "x".repeat(CHAT_TRACE_STORED_OUTPUT_MAX_BYTES));
        trace.process(&process("yes", scope, "completed", &long));
        trace.assistant_text_delta(1, "reply");

        let stored = trace.bounded_for_storage();
        let stdout = &stored.tool_calls().next().unwrap().stdout;
        assert!(stdout.starts_with("[3 earlier bytes omitted]\n"));
        assert!(stdout.ends_with("END"));
        let json = serde_json::to_string(&stored).unwrap();
        assert!(!json.contains("pending_step_text"));
        assert_eq!(
            serde_json::from_str::<ChatWorkTrace>(&json).unwrap(),
            stored
        );
    }
}
