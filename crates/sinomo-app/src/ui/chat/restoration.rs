//! Versioned presentation checkpoint. Runtime journals and SIFS remain authoritative for work.
use super::*;
use crate::backend::chat_engine::ChatConnectionSnapshot;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ChatWorkspaceSnapshot {
    pub connection: ChatConnectionSnapshot,
    session_id: String,
    messages: Vec<ChatMessage>,
    input: String,
    cursor: usize,
    attachments: Vec<PendingChatAttachment>,
    queued_turn_inputs: Vec<QueuedTurnInput>,
    info: RuntimeInfo,
    tool_runtime: ChatToolRuntimeSelection,
    streaming: bool,
    control: ChatControlOverview,
    context: Option<ChatContextSnapshot>,
    usage: Option<ChatUsageSnapshot>,
    timeline: ConversationTimelineState,
    sessions: Vec<SessionRef>,
    scroll: f32,
    stick_bottom: bool,
}

impl ChatState {
    pub(crate) fn workspace_snapshot(&self) -> ChatWorkspaceSnapshot {
        ChatWorkspaceSnapshot {
            connection: self.handle.connection_snapshot(),
            session_id: self.session_id.clone(),
            messages: self
                .messages
                .iter()
                .filter(|message| !message.is_transient_connection_error())
                .cloned()
                .collect(),
            input: self.input.clone(),
            cursor: self.cursor,
            attachments: self.attachments.clone(),
            queued_turn_inputs: self.queued_turn_inputs.clone(),
            info: self.info.clone(),
            tool_runtime: self.tool_runtime.clone(),
            streaming: self.streaming,
            control: self.control.clone(),
            context: self.context.clone(),
            usage: self.usage.clone(),
            timeline: self.timeline.clone(),
            sessions: self.sessions.clone(),
            scroll: self.scroll,
            stick_bottom: self.stick_bottom,
        }
    }

    pub(crate) fn restore_workspace(&mut self, snapshot: ChatWorkspaceSnapshot) {
        self.session_id = snapshot.session_id;
        self.messages = snapshot.messages;
        // Older checkpoints included connection diagnostics as chat messages.
        // Keep runtime-owned records and user/model content intact.
        self.messages
            .retain(|message| !message.is_transient_connection_error());
        self.input = snapshot.input;
        self.cursor = snapshot.cursor.min(self.input.len());
        self.attachments = snapshot.attachments;
        self.queued_turn_inputs = snapshot.queued_turn_inputs;
        self.info = snapshot.info;
        self.tool_runtime = snapshot.tool_runtime;
        self.workspace_path_input = self
            .tool_runtime
            .workspace_root
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        self.streaming = snapshot.streaming;
        self.control = snapshot.control;
        self.context = snapshot.context;
        self.usage = snapshot.usage;
        self.timeline = snapshot.timeline;
        self.sessions = snapshot.sessions;
        self.scroll = snapshot.scroll;
        self.stick_bottom = snapshot.stick_bottom;
        self.sync_selection_from_info();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_checkpoints_exclude_connection_diagnostics_including_legacy_messages() {
        let backend = crate::backend::Backend::local_fallback();
        let mut before = ChatState::new(&backend);
        let diagnostic =
            "Error: chat unavailable: open filesystem provider agent catalog SIFS failed";
        let conversation = vec![
            ChatMessage::user(diagnostic),
            ChatMessage::model(diagnostic),
            ChatMessage::system_error("Error: provider unavailable"),
            ChatMessage::system_error("Error: cluster task failed")
                .with_durable_identity("runtime-message", 7),
        ];
        before.messages = conversation.clone();
        before.messages.push(ChatMessage::system_error(diagnostic));
        before.input = "keep this draft".into();

        let bytes = serde_json::to_vec(&before.workspace_snapshot()).unwrap();
        let mut snapshot: ChatWorkspaceSnapshot = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(snapshot.messages.len(), conversation.len());

        // Simulate an older on-disk snapshot, before diagnostics were excluded.
        snapshot
            .messages
            .push(ChatMessage::system_error(diagnostic));
        let legacy_bytes = serde_json::to_vec(&snapshot).unwrap();
        let legacy_snapshot = serde_json::from_slice(&legacy_bytes).unwrap();
        let mut after = ChatState::new(&backend);
        after.restore_workspace(legacy_snapshot);
        assert_eq!(after.messages.len(), conversation.len());
        for (restored, expected) in after.messages.iter().zip(&conversation) {
            assert_eq!(restored.text, expected.text);
            assert_eq!(restored.source, expected.source);
            assert_eq!(restored.kind, expected.kind);
            assert_eq!(restored.message_id, expected.message_id);
            assert_eq!(restored.sequence, expected.sequence);
        }
        assert_eq!(after.input, "keep this draft");
        assert!(after.notice.is_none());
    }

    #[test]
    fn workspace_handoff_preserves_unsent_input_and_partial_turn_at_the_same_event_cursor() {
        let backend = crate::backend::Backend::local_fallback();
        let mut before = ChatState::new(&backend);
        before.handle = ChatHandle::spawn_restored(
            None,
            None,
            ChatConnectionSnapshot {
                execution_id: "stable-host".into(),
                generation: "daemon-generation".into(),
                after_sequence: 42,
            },
        );
        before.session_id = "same-session".into();
        before.input = "unsent draft".into();
        before.cursor = 6;
        before.streaming = true;
        before.control.active_turn = true;
        before.messages.push(ChatMessage::user("build this"));
        before.attachments.push(PendingChatAttachment {
            source: ChatAttachmentSource::LocalPath("/work/draft.txt".into()),
            file_name: "draft.txt".into(),
            size_bytes: 7,
        });
        before.messages.push(ChatMessage::model(String::new()));
        before.append_step_text_delta(3, "partial answer");
        before.queued_turn_inputs.push(QueuedTurnInput {
            kind: QueuedTurnKind::FollowUp,
            text: "then run tests".into(),
            input_id: Some("queued-id".into()),
        });
        let bytes = serde_json::to_vec(&before.workspace_snapshot()).unwrap();
        let snapshot: ChatWorkspaceSnapshot = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(snapshot.connection.after_sequence, 42);
        assert_eq!(snapshot.connection.execution_id, "stable-host");
        let mut after = ChatState::new(&backend);
        after.restore_workspace(snapshot);
        assert_eq!(after.session_id, "same-session");
        assert_eq!(after.input, "unsent draft");
        assert_eq!(after.cursor, 6);
        assert!(after.streaming && after.control.active_turn);
        assert_eq!(after.attachments[0].file_name, "draft.txt");
        // Restored step text still moves into the trace if the step calls a tool.
        after.apply_event(ChatEvent::ToolCallStarted {
            session_id: "same-session".into(),
            call: ChatToolCallSnapshot {
                step_index: 3,
                tool: "read_file".into(),
                ..Default::default()
            },
        });
        let reply = after.messages.last().unwrap();
        assert!(reply.text.is_empty());
        assert!(matches!(
            &reply.work_trace.as_ref().unwrap().entries[0],
            ChatWorkTraceEntry::Intermediate { step_index: 3, text } if text == "partial answer"
        ));
        assert_eq!(
            after.queued_turn_inputs[0].input_id.as_deref(),
            Some("queued-id")
        );
    }
}
