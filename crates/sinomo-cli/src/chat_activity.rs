use crate::app_api::SifsAgentChannelStore;
use crate::error::CliError;
use sinomo_agent::{
    AGENT_CHANNEL_APP_USER_ID, AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT, AgentChannel,
    AgentChannelAuthor, AgentChannelKind, AgentChannelMessage, AgentChannelMessageKind,
    AgentChannelSource, AgentChannelStore, AgentRuntimeEvent, AgentRuntimeEventPayload,
    AgentStopReason, BackgroundHandoffPriority, BackgroundHandoffRequest, InteractionSession,
    TaskId,
};
use sinomo_runtime::local::SinomoDataDir;
use time::OffsetDateTime;

/// Stable, model-independent projection of one CLI-visible conversation.
///
/// `InteractionSession::session_id` is the stable surface activity id and is
/// reused as the chat/channel id. It is not a provider session handle: model
/// sessions remain private, may be ephemeral or unrelated, and can change
/// format without changing this projection. This writer records only the
/// human-visible chat contract in the same SIFS AgentChannel representation
/// used by the app, so historical recall has one format across UI surfaces.
pub(crate) struct StableChatProjection {
    store: SifsAgentChannelStore,
    session: InteractionSession,
}

impl StableChatProjection {
    pub(crate) fn open(
        data_dir: &SinomoDataDir,
        session: &InteractionSession,
    ) -> Result<Self, CliError> {
        let store = SifsAgentChannelStore::open(data_dir).map_err(|detail| CliError::Parse {
            detail: format!("open stable CLI chat projection: {detail}"),
        })?;
        Ok(Self {
            store,
            session: session.clone(),
        })
    }

    pub(crate) fn record_user_message(
        &self,
        agent_key: &str,
        message_id: TaskId,
        created_at: OffsetDateTime,
        text: &str,
    ) -> Result<(), CliError> {
        self.record_message(
            agent_key,
            message_id,
            created_at,
            AgentChannelAuthor::User {
                user_id: AGENT_CHANNEL_APP_USER_ID.to_string(),
                display_name: "User".to_string(),
            },
            AgentChannelMessageKind::Normal,
            text,
        )
    }

    pub(crate) fn record_agent_message(
        &self,
        agent_key: &str,
        message_id: TaskId,
        created_at: OffsetDateTime,
        text: &str,
    ) -> Result<(), CliError> {
        let (mut channel, participant_id) = self.load_channel_for_agent(agent_key, false)?;
        let participant = channel
            .participant(&participant_id)
            .cloned()
            .ok_or_else(|| CliError::Parse {
                detail: "stable CLI chat participant disappeared".to_string(),
            })?;
        let author = AgentChannelAuthor::Agent {
            participant_id,
            agent_key: participant.agent_key,
            agent_name: participant.agent_name,
            agent_session_id: participant.agent_session_id,
        };
        self.record_message_in_channel(
            &mut channel,
            message_id,
            created_at,
            author,
            AgentChannelMessageKind::Normal,
            text,
        )
    }

    pub(crate) fn record_system_message(
        &self,
        agent_key: &str,
        message_id: TaskId,
        created_at: OffsetDateTime,
        text: &str,
        is_error: bool,
    ) -> Result<(), CliError> {
        self.record_message(
            agent_key,
            message_id,
            created_at,
            AgentChannelAuthor::System,
            if is_error {
                AgentChannelMessageKind::Error
            } else {
                AgentChannelMessageKind::Info
            },
            text,
        )
    }

    /// Record only terminal, user-visible runtime outcomes. Streaming model
    /// deltas, reasoning, tool telemetry, and provider state remain transient;
    /// the typed final decision is the stable assistant message.
    pub(crate) fn record_runtime_event(
        &self,
        agent_key: &str,
        event: &AgentRuntimeEvent,
    ) -> Result<(), CliError> {
        match &event.payload {
            AgentRuntimeEventPayload::TurnFinalDecision {
                decision,
                stop_reason,
                ..
            } => {
                let text = decision.text.trim();
                if text.is_empty() {
                    let notice = if *stop_reason == AgentStopReason::Cancelled {
                        "Agent turn cancelled."
                    } else {
                        "No agent responded."
                    };
                    self.record_system_message(
                        agent_key,
                        event.event_id.clone(),
                        event.emitted_at,
                        notice,
                        false,
                    )
                } else if *stop_reason == AgentStopReason::Cancelled {
                    self.record_system_message(
                        agent_key,
                        event.event_id.clone(),
                        event.emitted_at,
                        text,
                        false,
                    )
                } else {
                    self.record_agent_message(
                        agent_key,
                        event.event_id.clone(),
                        event.emitted_at,
                        text,
                    )
                }?;
                if let Some(handoff) = decision.background_handoff.as_ref() {
                    self.record_background_handoff_request(
                        agent_key,
                        &event.event_id,
                        event.emitted_at,
                        handoff,
                    )?;
                }
                Ok(())
            }
            AgentRuntimeEventPayload::WorkerFailed { action, detail, .. } => self
                .record_system_message(
                    agent_key,
                    event.event_id.clone(),
                    event.emitted_at,
                    &format!("Agent task failed: {action}: {detail}"),
                    true,
                ),
            AgentRuntimeEventPayload::WorkerStarted { .. }
            | AgentRuntimeEventPayload::WorkerCompleted { .. }
            | AgentRuntimeEventPayload::ModelProgress { .. }
            | AgentRuntimeEventPayload::ToolProgress { .. }
            | AgentRuntimeEventPayload::ControlEvent { .. } => Ok(()),
        }
    }

    /// Project a requested handoff as a separate human-visible system message.
    ///
    /// A model decision is not proof that a worker was dispatched, so this
    /// deliberately does not create a background-activity record. The private
    /// evidence reference also remains outside the searchable chat projection.
    pub(crate) fn record_background_handoff_request(
        &self,
        agent_key: &str,
        parent_message_id: &TaskId,
        created_at: OffsetDateTime,
        handoff: &BackgroundHandoffRequest,
    ) -> Result<(), CliError> {
        self.record_system_message(
            agent_key,
            stable_related_message_id(parent_message_id, "background-handoff")?,
            created_at,
            &background_handoff_notice(handoff),
            false,
        )
    }

    pub(crate) fn is_for_session(&self, session: &InteractionSession) -> bool {
        self.session == *session
    }

    fn record_message(
        &self,
        agent_key: &str,
        message_id: TaskId,
        created_at: OffsetDateTime,
        author: AgentChannelAuthor,
        message_kind: AgentChannelMessageKind,
        text: &str,
    ) -> Result<(), CliError> {
        let allow_membership_setup = matches!(&author, AgentChannelAuthor::User { .. });
        let (mut channel, _) = self.load_channel_for_agent(agent_key, allow_membership_setup)?;
        if matches!(&author, AgentChannelAuthor::User { .. })
            && channel.title == "New chat"
            && !text.trim().is_empty()
        {
            channel.title = stable_chat_title(text);
        }
        self.record_message_in_channel(
            &mut channel,
            message_id,
            created_at,
            author,
            message_kind,
            text,
        )
    }

    fn record_message_in_channel(
        &self,
        channel: &mut AgentChannel,
        message_id: TaskId,
        created_at: OffsetDateTime,
        author: AgentChannelAuthor,
        message_kind: AgentChannelMessageKind,
        text: &str,
    ) -> Result<(), CliError> {
        if channel.is_deleted() {
            return Err(CliError::Parse {
                detail: format!("stable CLI chat {} is deleted", self.session.session_id),
            });
        }
        let source = self.source(&message_id);
        if let Some(existing) = self
            .store
            .load_immutable_message_record(&channel.channel_id, &message_id)
            .map_err(|source| projection_store_error("load stable chat message", source))?
        {
            ensure_same_projected_message(
                &existing,
                &author,
                &message_kind,
                text,
                created_at,
                &source,
            )?;
            if !channel
                .transcript
                .iter()
                .any(|message| message.message_id == existing.message_id)
            {
                channel.transcript.push(existing.clone());
                trim_recent_transcript(channel);
            }
            channel.next_sequence = channel
                .next_sequence
                .max(existing.sequence.saturating_add(1));
            channel.updated_at = channel.updated_at.max(existing.created_at);
            self.store
                .save_channel(channel)
                .map_err(|source| projection_store_error("repair stable chat metadata", source))?;
            return Ok(());
        }

        let message = AgentChannelMessage {
            message_id,
            sequence: channel.next_sequence,
            author,
            message_kind,
            content: text.to_string(),
            created_at,
            source,
            attachments: Vec::new(),
        };
        channel.next_sequence = channel.next_sequence.saturating_add(1);
        channel.updated_at = channel.updated_at.max(created_at);
        channel.transcript.push(message.clone());
        trim_recent_transcript(channel);
        self.store
            .append_message(&channel.channel_id, &message)
            .map_err(|source| projection_store_error("append stable chat message", source))?;
        self.store
            .save_channel(channel)
            .map_err(|source| projection_store_error("save stable chat metadata", source))
    }

    fn load_channel_for_agent(
        &self,
        agent_key: &str,
        allow_membership_setup: bool,
    ) -> Result<(AgentChannel, TaskId), CliError> {
        let loaded = self
            .store
            .load_channel(&self.session.session_id)
            .map_err(|source| projection_store_error("load stable CLI chat", source))?;
        let mut channel = match loaded {
            Some(channel) => channel,
            None if allow_membership_setup => {
                AgentChannel::with_default_title(self.session.session_id.clone())
            }
            None => {
                return Err(CliError::Parse {
                    detail: "stable CLI chat has no authorized channel setup".to_string(),
                });
            }
        };
        if channel.channel_kind != AgentChannelKind::Chat {
            return Err(CliError::Parse {
                detail: format!(
                    "activity {} is a channel, not a CLI chat",
                    self.session.session_id
                ),
            });
        }
        let agent_key = normalized_agent_key(agent_key);
        let existing_participant_id = channel
            .participants
            .iter()
            .find(|participant| participant.agent_key == agent_key)
            .map(|participant| participant.participant_id.clone());
        if !allow_membership_setup {
            let participant_id = existing_participant_id.ok_or_else(|| CliError::Parse {
                detail: format!("stable CLI chat agent `{agent_key}` is not a channel participant"),
            })?;
            let participant =
                channel
                    .participant(&participant_id)
                    .ok_or_else(|| CliError::Parse {
                        detail: "stable CLI chat participant disappeared".to_string(),
                    })?;
            if !participant.enabled {
                return Err(CliError::Parse {
                    detail: format!(
                        "stable CLI chat agent `{agent_key}` is not an enabled channel participant"
                    ),
                });
            }
            return Ok((channel, participant_id));
        }
        let participant_id = channel.ensure_participant(agent_key, agent_key);
        let participant_id = if existing_participant_id.is_some() {
            participant_id
        } else {
            let stable_participant_id = stable_participant_id(&channel.channel_id, agent_key)?;
            if let Some(participant) = channel.participant_mut(&participant_id) {
                participant.participant_id = stable_participant_id.clone();
            }
            stable_participant_id
        };
        if let Some(participant) = channel.participant_mut(&participant_id) {
            participant.agent_session_id = self.session.session_id.clone();
        }
        Ok((channel, participant_id))
    }

    fn source(&self, message_id: &TaskId) -> AgentChannelSource {
        AgentChannelSource {
            adapter: "sinomo-cli".to_string(),
            external_channel_id: Some(self.session.session_id.to_string()),
            external_message_id: Some(message_id.to_string()),
            metadata: serde_json::json!({
                "origin_node_id": self.session.node_id.to_string(),
                "origin_surface_id": self.session.surface.surface_id.clone(),
                "origin_surface_kind": self.session.surface.kind.clone(),
            }),
            ..AgentChannelSource::default()
        }
    }
}

/// Stable, human-visible description of a handoff request. The evidence
/// reference is intentionally omitted because it may identify private agent
/// state and a request alone does not establish that any background work ran.
pub(crate) fn background_handoff_notice(handoff: &BackgroundHandoffRequest) -> String {
    let priority = match &handoff.priority {
        BackgroundHandoffPriority::Routine => "routine",
        BackgroundHandoffPriority::Important => "important",
        BackgroundHandoffPriority::Emergency => "emergency",
    };
    let reason = handoff.reason.trim();
    if reason.is_empty() {
        format!("Background handoff requested ({priority}).")
    } else {
        format!("Background handoff requested ({priority}): {reason}")
    }
}

fn stable_related_message_id(message_id: &TaskId, kind: &str) -> Result<TaskId, CliError> {
    TaskId::from_legacy_named(format!("cli-chat:{message_id}:{kind}")).map_err(|source| {
        CliError::Parse {
            detail: format!("derive stable related chat message id: {source}"),
        }
    })
}

fn stable_participant_id(
    channel_id: &sinomo_runtime::events::SessionId,
    agent_key: &str,
) -> Result<TaskId, CliError> {
    TaskId::from_legacy_named(format!("cli-chat:{channel_id}:{agent_key}")).map_err(|source| {
        CliError::Parse {
            detail: format!("derive stable CLI chat participant: {source}"),
        }
    })
}

fn normalized_agent_key(agent_key: &str) -> &str {
    let agent_key = agent_key.trim();
    if agent_key.is_empty() {
        "main-agent"
    } else {
        agent_key
    }
}

fn trim_recent_transcript(channel: &mut AgentChannel) {
    let excess = channel
        .transcript
        .len()
        .saturating_sub(AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT);
    if excess > 0 {
        channel.transcript.drain(0..excess);
    }
}

fn stable_chat_title(text: &str) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut title = compact.chars().take(80).collect::<String>();
    if compact.chars().count() > 80 {
        title.push('…');
    }
    title
}

fn ensure_same_projected_message(
    existing: &AgentChannelMessage,
    author: &AgentChannelAuthor,
    message_kind: &AgentChannelMessageKind,
    text: &str,
    created_at: OffsetDateTime,
    source: &AgentChannelSource,
) -> Result<(), CliError> {
    if &existing.author == author
        && &existing.message_kind == message_kind
        && existing.content == text
        && existing.created_at == created_at
        && &existing.source == source
        && existing.attachments.is_empty()
    {
        return Ok(());
    }
    Err(CliError::Parse {
        detail: format!(
            "stable CLI chat message {} already exists with a different projected identity",
            existing.message_id
        ),
    })
}

fn projection_store_error(action: &str, source: sinomo_agent::AgentChannelStoreError) -> CliError {
    CliError::Parse {
        detail: format!("{action}: {source}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_agent::{AgentChannelStore, InteractionSurface, InteractionSurfaceKind};
    use sinomo_runtime::node::NodeId;

    fn session() -> InteractionSession {
        InteractionSession::new(
            sinomo_runtime::events::SessionId::from_legacy_named("cli_projection_chat")
                .expect("session id"),
            NodeId::from_legacy_named("node_cli_projection").expect("node id"),
            InteractionSurface::new("surface.cli.test", InteractionSurfaceKind::CommandLine),
        )
    }

    #[test]
    fn stable_projection_records_chat_messages_idempotently() {
        let root = sinomo_testdir::TestDir::new("sinomo-cli-chat-projection");
        let data_dir = SinomoDataDir::new(root.to_path_buf());
        let session = session();
        let projection = StableChatProjection::open(&data_dir, &session).expect("projection");
        let user_id = TaskId::from_legacy_named("cli_projection_user").expect("user id");
        let agent_id = TaskId::from_legacy_named("cli_projection_agent").expect("agent id");
        let system_id = TaskId::from_legacy_named("cli_projection_system").expect("system id");
        let user_at = OffsetDateTime::from_unix_timestamp(10).expect("user time");
        let agent_at = OffsetDateTime::from_unix_timestamp(20).expect("agent time");
        let system_at = OffsetDateTime::from_unix_timestamp(30).expect("system time");

        projection
            .record_user_message("main-agent", user_id.clone(), user_at, "hello")
            .expect("user message");
        projection
            .record_agent_message("main-agent", agent_id.clone(), agent_at, "hi")
            .expect("agent message");
        projection
            .record_agent_message("main-agent", agent_id, agent_at, "hi")
            .expect("idempotent agent message");
        projection
            .record_system_message("main-agent", system_id, system_at, "typed failure", true)
            .expect("system error");

        let channel = SifsAgentChannelStore::open(&data_dir)
            .expect("store")
            .load_channel(&session.session_id)
            .expect("load")
            .expect("channel");
        assert_eq!(channel.channel_kind, AgentChannelKind::Chat);
        assert_eq!(channel.transcript.len(), 3);
        assert_eq!(channel.transcript[0].content, "hello");
        assert_eq!(channel.transcript[1].content, "hi");
        assert_eq!(channel.transcript[1].source.adapter, "sinomo-cli");
        assert_eq!(channel.transcript[2].author, AgentChannelAuthor::System);
        assert_eq!(
            channel.transcript[2].message_kind,
            AgentChannelMessageKind::Error
        );
        assert_eq!(
            channel.transcript[2].source.external_channel_id.as_deref(),
            Some(session.session_id.as_str())
        );
        assert!(channel.participants.iter().any(|participant| {
            participant.agent_key == "main-agent"
                && participant.agent_session_id == session.session_id
        }));
    }

    #[test]
    fn stable_projection_keeps_text_and_sanitized_background_handoff_request() {
        let root = sinomo_testdir::TestDir::new("sinomo-cli-chat-projection-handoff");
        let data_dir = SinomoDataDir::new(root.to_path_buf());
        let session = session();
        let projection = StableChatProjection::open(&data_dir, &session).expect("projection");
        let event = AgentRuntimeEvent::new(
            session.session_id.clone(),
            AgentRuntimeEventPayload::TurnFinalDecision {
                sequence: 1,
                op_id: TaskId::from_legacy_named("cli_projection_handoff_op").expect("op id"),
                decision: sinomo_agent::AgentLoopDecision {
                    response_mode: sinomo_agent::AgentLoopResponseMode::Acknowledge,
                    text: "I will continue this in the background.".to_string(),
                    tool_calls: Vec::new(),
                    background_handoff: Some(BackgroundHandoffRequest {
                        reason: "check cluster recovery".to_string(),
                        evidence_ref: "sifs:/private/evidence/secret.json".to_string(),
                        priority: BackgroundHandoffPriority::Important,
                    }),
                    next_state: sinomo_agent::AgentInteractionState::Delegating,
                },
                stop_reason: AgentStopReason::FinalDecision,
            },
        );
        projection
            .record_user_message(
                "main-agent",
                TaskId::from_legacy_named("cli_projection_handoff_user").expect("user message id"),
                event.emitted_at - time::Duration::seconds(1),
                "Please continue in the background.",
            )
            .expect("establish authorized stable chat");

        projection
            .record_runtime_event("main-agent", &event)
            .expect("project final decision");
        projection
            .record_runtime_event("main-agent", &event)
            .expect("idempotent projection");

        let channel = SifsAgentChannelStore::open(&data_dir)
            .expect("store")
            .load_channel(&session.session_id)
            .expect("load")
            .expect("channel");
        assert_eq!(channel.transcript.len(), 3);
        assert_eq!(
            channel.transcript[1].content,
            "I will continue this in the background."
        );
        assert_eq!(
            channel.transcript[1].message_kind,
            AgentChannelMessageKind::Normal
        );
        assert_eq!(channel.transcript[2].author, AgentChannelAuthor::System);
        assert_eq!(
            channel.transcript[2].message_kind,
            AgentChannelMessageKind::Info
        );
        assert_eq!(
            channel.transcript[2].content,
            "Background handoff requested (important): check cluster recovery"
        );
        assert!(!channel.transcript[2].content.contains("evidence"));
        assert!(!channel.transcript[2].content.contains("secret"));
    }

    #[test]
    fn stable_projection_rejects_conflicting_message_identity() {
        let root = sinomo_testdir::TestDir::new("sinomo-cli-chat-projection-conflict");
        let data_dir = SinomoDataDir::new(root.to_path_buf());
        let session = session();
        let projection = StableChatProjection::open(&data_dir, &session).expect("projection");
        let message_id = TaskId::from_legacy_named("cli_projection_conflict").expect("message id");
        let created_at = OffsetDateTime::from_unix_timestamp(30).expect("time");
        projection
            .record_user_message("main-agent", message_id.clone(), created_at, "first")
            .expect("first");

        let error = projection
            .record_user_message("main-agent", message_id, created_at, "different")
            .expect_err("identity conflict");
        assert!(error.to_string().contains("different projected identity"));
    }

    #[test]
    fn delayed_cli_projection_cannot_reenable_revoked_agent() {
        let root = sinomo_testdir::TestDir::new("sinomo-cli-chat-projection-revoked-agent");
        let data_dir = SinomoDataDir::new(root.to_path_buf());
        let session = session();
        let projection = StableChatProjection::open(&data_dir, &session).expect("projection");
        projection
            .record_user_message(
                "main-agent",
                TaskId::generate(),
                OffsetDateTime::from_unix_timestamp(10).expect("time"),
                "authorized setup",
            )
            .expect("initial user message");

        let store = SifsAgentChannelStore::open(&data_dir).expect("store");
        let mut channel = store
            .load_channel(&session.session_id)
            .expect("load channel")
            .expect("channel");
        let participant_id = channel
            .participants
            .iter()
            .find(|participant| participant.agent_key == "main-agent")
            .expect("participant")
            .participant_id
            .clone();
        assert!(channel.disable_participant(&participant_id));
        store.save_channel(&channel).expect("save revocation");

        let error = projection
            .record_agent_message(
                "main-agent",
                TaskId::generate(),
                OffsetDateTime::from_unix_timestamp(20).expect("time"),
                "late agent output",
            )
            .expect_err("late output must not re-enable membership");
        assert!(
            error
                .to_string()
                .contains("not an enabled channel participant")
        );
        let loaded = store
            .load_channel(&session.session_id)
            .expect("reload channel")
            .expect("channel");
        assert!(
            loaded
                .participant(&participant_id)
                .is_some_and(|participant| !participant.enabled)
        );
        assert!(
            !loaded
                .transcript
                .iter()
                .any(|message| message.content == "late agent output")
        );
    }

    #[test]
    fn stable_projection_rejects_replayed_id_from_different_provenance() {
        let root = sinomo_testdir::TestDir::new("sinomo-cli-chat-projection-provenance");
        let data_dir = SinomoDataDir::new(root.to_path_buf());
        let first_session = session();
        let message_id =
            TaskId::from_legacy_named("cli_projection_provenance").expect("message id");
        let created_at = OffsetDateTime::from_unix_timestamp(30).expect("time");
        StableChatProjection::open(&data_dir, &first_session)
            .expect("first projection")
            .record_user_message("main-agent", message_id.clone(), created_at, "same content")
            .expect("first");

        let second_session = InteractionSession::new(
            first_session.session_id,
            NodeId::from_legacy_named("other_node_cli_projection").expect("other node id"),
            InteractionSurface::new("surface.cli.other", InteractionSurfaceKind::CommandLine),
        );
        let error = StableChatProjection::open(&data_dir, &second_session)
            .expect("second projection")
            .record_user_message("main-agent", message_id, created_at, "same content")
            .expect_err("provenance conflict");
        assert!(error.to_string().contains("different projected identity"));
    }
}
