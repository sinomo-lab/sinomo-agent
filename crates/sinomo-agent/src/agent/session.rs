//! Session state, staged session edits, and durable turn lifecycle.

use super::{AgentItemJournal, agent_item_id, latest_compacted_projection_items};

/// Items a manual compaction keeps verbatim when neither the package nor the
/// context budget configures a retained tail.
const DEFAULT_MANUAL_COMPACTION_RETAINED_ITEMS: usize = 12;
use super::{
    Agent, AgentCancellationToken, AgentCompactionRequest, AgentCompactionSummary,
    AgentCompactionTrigger, AgentDefinition, AgentError, AgentEventPayload, AgentItem,
    AgentLoopDecision, AgentLoopInput, AgentModelEvent, AgentModelProgressSink, AgentOp,
    AgentOpAction, AgentOpOutcome, AgentRole, AgentRun, AgentRunModelMetrics,
    AgentSessionControlResult, AgentSessionProjection, AgentSessionStoreRecord,
    AgentSessionStoreRecordKind, AgentStopReason, AgentToolProgressSink, AgentTraceRecord,
    AgentTraceRecordKind, AgentTurnProgress, Deserialize, InteractionModel, PathBuf, Serialize,
    SessionId, TaskId, agent_session_event, compacted_projection_items, compacted_session_items,
    is_false, is_zero_u64, turn_completed_agent_event,
};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSession {
    pub session_id: SessionId,
    pub role: AgentRole,
    /// Monotonic logical revision of private/model execution state. This lets
    /// device-local and roaming snapshots distinguish queue/control updates
    /// that do not append a transcript item.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub state_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_definition_key: Option<String>,
    /// Legacy single-workspace execution snapshot. Chats and channels own the
    /// authoritative ordered workspace list; this field remains readable so
    /// older persisted sessions can resume during the migration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_binding: Option<WorkspaceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_placement: Option<AgentSessionExecutionPlacement>,
    /// Reference to one independently managed persistent sandbox resource.
    /// The agent session never owns the sandbox lifecycle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_attachment: Option<AgentSessionSandboxAttachment>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_bindings: Vec<AgentSessionResourceBinding>,
    pub transcript: AgentTranscript,
    #[serde(default, skip_serializing_if = "AgentTranscript::is_empty")]
    pub raw_transcript: AgentTranscript,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection: Option<AgentSessionProjection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steering_queue: Vec<AgentSteeringMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub follow_up_queue: Vec<AgentFollowUpMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abort: Option<AgentAbortRecord>,
}

impl AgentSession {
    pub fn new(session_id: SessionId, role: AgentRole) -> Self {
        Self {
            session_id,
            role,
            state_revision: 0,
            agent_definition_key: None,
            workspace_binding: None,
            execution_placement: None,
            sandbox_attachment: None,
            resource_bindings: Vec::new(),
            transcript: AgentTranscript::new(),
            raw_transcript: AgentTranscript::new(),
            projection: None,
            steering_queue: Vec::new(),
            follow_up_queue: Vec::new(),
            abort: None,
        }
    }

    pub fn with_agent_definition_key(mut self, key: impl Into<String>) -> Self {
        self.agent_definition_key = Some(key.into());
        self
    }

    /// Select the host for a new session. An agent's configured home server is
    /// preferred; otherwise the daemon processing the creation request owns
    /// the session.
    pub fn place_new_session(
        &mut self,
        definition: &AgentDefinition,
        processing_server_id: impl Into<String>,
    ) {
        self.execution_placement = Some(AgentSessionExecutionPlacement::for_new_session(
            definition,
            processing_server_id,
        ));
        self.advance_state_revision();
    }

    /// Explicitly migrate the session host without attempting to copy
    /// node-local resources or implicitly moving its sandbox.
    pub fn migrate_to_server(
        &mut self,
        target_server_id: impl Into<String>,
    ) -> AgentSessionMigrationReport {
        let target_server_id = target_server_id.into();
        let placement =
            self.execution_placement
                .get_or_insert_with(|| AgentSessionExecutionPlacement {
                    active_server_id: target_server_id.clone(),
                    generation: 0,
                });
        let source_server_id = placement.active_server_id.clone();
        if source_server_id == target_server_id {
            return AgentSessionMigrationReport {
                source_server_id,
                target_server_id,
                placement_generation: placement.generation,
                unavailable_resource_ids: Vec::new(),
            };
        }

        placement.active_server_id = target_server_id.clone();
        placement.generation = placement.generation.saturating_add(1);
        let placement_generation = placement.generation;
        let mut unavailable_resource_ids = Vec::new();
        for binding in &mut self.resource_bindings {
            if binding.mark_unavailable_after_session_migration(&target_server_id) {
                unavailable_resource_ids.push(binding.resource_id.clone());
            }
        }
        self.advance_state_revision();
        AgentSessionMigrationReport {
            source_server_id,
            target_server_id,
            placement_generation,
            unavailable_resource_ids,
        }
    }

    pub fn attach_sandbox(
        &mut self,
        attachment: AgentSessionSandboxAttachment,
    ) -> Result<(), AgentSessionSandboxAttachmentError> {
        if let Some(existing) = self.sandbox_attachment.as_ref() {
            if existing == &attachment {
                return Ok(());
            }
            return Err(AgentSessionSandboxAttachmentError::AlreadyAttached {
                session_id: self.session_id.clone(),
                sandbox_id: existing.sandbox_id.clone(),
            });
        }
        self.sandbox_attachment = Some(attachment);
        self.advance_state_revision();
        Ok(())
    }

    pub fn append_run(&mut self, run: &AgentRun) {
        self.append_turn_items(&run.items, &run.compactions);
    }

    pub(crate) fn append_turn_items(
        &mut self,
        items: &[AgentItem],
        compactions: &[AgentCompactionSummary],
    ) {
        if self.raw_transcript.items.is_empty() && !self.transcript.items.is_empty() {
            self.raw_transcript = self.transcript.clone();
        }
        self.transcript.items.extend(items.iter().cloned());
        self.raw_transcript.items.extend(items.iter().cloned());
        if let Some(projection) = compactions
            .last()
            .map(|compaction| compaction.projection.clone())
        {
            self.projection = Some(projection);
        }
        self.advance_state_revision();
    }

    /// Fold in items a turn journaled that never reached a saved snapshot:
    /// the process stopped mid-turn, or before the completed turn was saved.
    /// Tool calls without a recorded result get a failed result so every call
    /// stays paired, and an error item tells the model what happened.
    pub(crate) fn recover_unsaved_turn_items(&mut self, journaled: Vec<AgentItem>) {
        if journaled.is_empty() {
            return;
        }
        let recovered = journaled.len();
        let results: BTreeSet<(usize, usize)> = journaled
            .iter()
            .filter_map(|item| match item {
                AgentItem::ToolResult {
                    step_index,
                    call_index,
                    ..
                } => Some((*step_index, *call_index)),
                _ => None,
            })
            .collect();
        let mut items = journaled;
        let interrupted_results = items
            .iter()
            .filter_map(|item| match item {
                AgentItem::ToolCall {
                    step_index,
                    call_index,
                    call,
                    ..
                } if !results.contains(&(*step_index, *call_index)) => {
                    let summary = format!(
                        "`{}` was interrupted before its result was recorded; it may or may not have taken effect",
                        call.tool
                    );
                    Some(AgentItem::ToolResult {
                        item_id: TaskId::generate(),
                        step_index: *step_index,
                        call_index: *call_index,
                        call_id: TaskId::generate(),
                        native_call_id: call.call_id.clone(),
                        tool_name: Some(call.tool.clone()),
                        status: crate::AgentToolStatus::Failed,
                        summary: summary.clone(),
                        output: serde_json::json!({ "error": "turn_interrupted" }),
                        evidence: crate::AgentToolEvidence::availability_only(false, summary),
                    })
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let step_index = items
            .iter()
            .filter_map(agent_item_step_index)
            .max()
            .unwrap_or_default();
        items.extend(interrupted_results);
        items.push(AgentItem::Error {
            item_id: TaskId::generate(),
            step_index,
            detail: format!(
                "the previous turn did not finish saving; {recovered} item(s) it recorded were recovered"
            ),
        });
        self.append_turn_items(&items, &[]);
    }

    pub fn advance_state_revision(&mut self) {
        self.state_revision = self.state_revision.saturating_add(1);
    }

    pub fn is_aborted(&self) -> bool {
        self.abort.is_some()
    }

    /// What a manual compaction summarizes: the current model-facing
    /// projection (latest summary plus the items after it), not the full raw
    /// history. Earlier history is already represented by that summary, so
    /// resending it would grow every compaction request without bound.
    pub(super) fn compaction_source_transcript(&mut self) -> AgentTranscript {
        if self.raw_transcript.items.is_empty() && !self.transcript.items.is_empty() {
            self.raw_transcript = self.transcript.clone();
        }
        AgentTranscript {
            items: latest_compacted_projection_items(&self.transcript.items),
        }
    }
}

fn agent_item_step_index(item: &AgentItem) -> Option<usize> {
    match item {
        AgentItem::UserMessage { step_index, .. }
        | AgentItem::AssistantText { step_index, .. }
        | AgentItem::AssistantReasoningSummary { step_index, .. }
        | AgentItem::ToolCall { step_index, .. }
        | AgentItem::ToolResult { step_index, .. }
        | AgentItem::ModelUsage { step_index, .. }
        | AgentItem::Error { step_index, .. }
        | AgentItem::PatchSnapshot { step_index, .. }
        | AgentItem::VerificationResult { step_index, .. }
        | AgentItem::CodingRunState { step_index, .. } => Some(*step_index),
        _ => None,
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AgentTranscript {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<AgentItem>,
}

impl AgentTranscript {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn push(&mut self, item: AgentItem) {
        self.items.push(item);
    }

    /// The most recent `retained_tail_count` items, extended backwards when
    /// needed so a retained tool result always keeps the call that produced
    /// it. Providers reject a result whose call is missing.
    pub fn retained_tail(&self, retained_tail_count: usize) -> Vec<AgentItem> {
        let start = self.items.len().saturating_sub(retained_tail_count);
        self.items[tool_pair_aligned_tail_start(&self.items, start)..].to_vec()
    }
}

/// Move a tail's start back until no tool result in the tail is separated
/// from its call (calls and results pair by step and call index).
pub(super) fn tool_pair_aligned_tail_start(items: &[AgentItem], start: usize) -> usize {
    let mut start = start.min(items.len());
    loop {
        let tail_results = items[start..]
            .iter()
            .filter_map(|item| match item {
                AgentItem::ToolResult {
                    step_index,
                    call_index,
                    ..
                } => Some((*step_index, *call_index)),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let earliest_call = items[..start].iter().position(|item| {
            matches!(
                item,
                AgentItem::ToolCall { step_index, call_index, .. }
                    if tail_results.contains(&(*step_index, *call_index))
            )
        });
        match earliest_call {
            Some(index) => start = index,
            None => return start,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSteeringMessage {
    pub steering_id: TaskId,
    pub message: String,
}

impl AgentSteeringMessage {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            steering_id: TaskId::generate(),
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentFollowUpMessage {
    pub follow_up_id: TaskId,
    pub message: String,
}

impl AgentFollowUpMessage {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            follow_up_id: TaskId::generate(),
            message: message.into(),
        }
    }
}

/// A queued turn input removed from a session queue before the runtime
/// consumed it (queued items are visible **and removable**).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentRemovedQueuedInput {
    Steering { steering: AgentSteeringMessage },
    FollowUp { follow_up: AgentFollowUpMessage },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentAbortRecord {
    pub abort_id: TaskId,
    pub reason: String,
}

impl AgentAbortRecord {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            abort_id: TaskId::generate(),
            reason: reason.into(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentRunContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<AgentTaskRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<WorkspaceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<AgentPlacementMetadata>,
}

impl AgentRunContext {
    pub fn is_empty(&self) -> bool {
        self.task.is_none() && self.workspace.is_none() && self.placement.is_none()
    }

    pub fn with_task(mut self, task: AgentTaskRef) -> Self {
        self.task = Some(task);
        self
    }

    pub fn with_workspace(mut self, workspace: WorkspaceRef) -> Self {
        self.workspace = Some(workspace);
        self
    }

    pub fn with_placement(mut self, placement: AgentPlacementMetadata) -> Self {
        self.placement = Some(placement);
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentTaskRef {
    pub task_id: TaskId,
    pub kind: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_role: Option<AgentRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<TaskId>,
}

impl AgentTaskRef {
    pub fn new(kind: impl Into<String>, summary: impl Into<String>) -> Self {
        Self {
            task_id: TaskId::generate(),
            kind: kind.into(),
            summary: summary.into(),
            owner_role: None,
            parent_task_id: None,
        }
    }

    pub fn with_owner_role(mut self, owner_role: AgentRole) -> Self {
        self.owner_role = Some(owner_role);
        self
    }

    pub fn with_parent_task_id(mut self, parent_task_id: TaskId) -> Self {
        self.parent_task_id = Some(parent_task_id);
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WorkspaceRef {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_root: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sifs_root: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_policy: Option<String>,
}

impl WorkspaceRef {
    pub fn local_path(path: impl Into<PathBuf>) -> Self {
        let local_root = path.into();
        Self {
            id: local_root.display().to_string(),
            local_root: Some(local_root),
            sifs_root: None,
            owner_node: None,
            write_policy: Some("local".to_string()),
        }
    }

    pub fn sifs(id: impl Into<String>, sifs_root: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            local_root: None,
            sifs_root: Some(sifs_root.into()),
            owner_node: None,
            write_policy: None,
        }
    }

    pub fn with_owner_node(mut self, owner_node: impl Into<String>) -> Self {
        self.owner_node = Some(owner_node.into());
        self
    }

    pub fn with_write_policy(mut self, write_policy: impl Into<String>) -> Self {
        self.write_policy = Some(write_policy.into());
        self
    }
}

/// Durable routing record for the process that owns an active agent session.
/// Chat ingress must be routed to `active_server_id`; receiving a message on a
/// different device never changes this record.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSessionExecutionPlacement {
    pub active_server_id: String,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub generation: u64,
}

impl AgentSessionExecutionPlacement {
    pub fn for_new_session(
        definition: &AgentDefinition,
        processing_server_id: impl Into<String>,
    ) -> Self {
        let processing_server_id = processing_server_id.into();
        Self {
            active_server_id: definition
                .placement
                .home_server_node_id
                .clone()
                .unwrap_or(processing_server_id),
            generation: 0,
        }
    }
}

/// A session attachment to a sandbox resource managed through the sandbox
/// service. Multiple sessions may attach the same sandbox, but one session has
/// at most one sandbox attachment.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSessionSandboxAttachment {
    pub sandbox_id: String,
    pub server_id: String,
    #[serde(default)]
    pub attachment_kind: AgentSessionSandboxAttachmentKind,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentSessionSandboxAttachmentKind {
    #[default]
    Automatic,
    Explicit,
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum AgentSessionSandboxAttachmentError {
    #[error("agent session `{session_id}` is already attached to sandbox `{sandbox_id}`")]
    AlreadyAttached {
        session_id: SessionId,
        sandbox_id: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSessionResourceBinding {
    pub resource_id: String,
    pub kind: AgentSessionResourceKind,
    pub location: AgentSessionResourceLocation,
    #[serde(default)]
    pub availability: AgentSessionResourceAvailability,
}

impl AgentSessionResourceBinding {
    pub(super) fn mark_unavailable_after_session_migration(
        &mut self,
        target_server_id: &str,
    ) -> bool {
        let becomes_unavailable = matches!(
            &self.location,
            AgentSessionResourceLocation::SessionServerLocal { server_id, .. }
                if server_id != target_server_id
        );
        if becomes_unavailable {
            self.availability = AgentSessionResourceAvailability::UnavailableAfterMigration;
        }
        becomes_unavailable
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentSessionResourceKind {
    AgentHome,
    Workspace,
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentSessionResourceLocation {
    Sifs { path: String },
    SessionServerLocal { server_id: String, path: PathBuf },
    SandboxLocal { sandbox_id: String, path: String },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentSessionResourceAvailability {
    #[default]
    Available,
    UnavailableAfterMigration,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSessionMigrationReport {
    pub source_server_id: String,
    pub target_server_id: String,
    pub placement_generation: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unavailable_resource_ids: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentPlacementMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_os: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_arch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_class: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub local_only: bool,
}

impl AgentPlacementMetadata {
    pub fn local_current_process() -> Self {
        Self {
            origin_node: None,
            target_node: None,
            target_os: Some(std::env::consts::OS.to_string()),
            target_arch: Some(std::env::consts::ARCH.to_string()),
            sandbox_class: None,
            local_only: true,
        }
    }

    pub fn with_origin_node(mut self, origin_node: impl Into<String>) -> Self {
        self.origin_node = Some(origin_node.into());
        self
    }

    pub fn with_target_node(mut self, target_node: impl Into<String>) -> Self {
        self.target_node = Some(target_node.into());
        self
    }

    pub fn with_sandbox_class(mut self, sandbox_class: impl Into<String>) -> Self {
        self.sandbox_class = Some(sandbox_class.into());
        self
    }
}

impl<M: InteractionModel> Agent<M> {
    pub fn load_session(&self, session_id: &SessionId) -> Result<Option<AgentSession>, AgentError> {
        self.session_store
            .load_session(session_id)
            .map_err(|source| AgentError::SessionStore {
                detail: source.to_string(),
            })
    }

    pub fn save_session(&self, session: &AgentSession) -> Result<(), AgentError> {
        self.session_store
            .save_session(session)
            .map_err(|source| AgentError::SessionStore {
                detail: source.to_string(),
            })
    }

    pub fn handle_op(
        &self,
        session: &mut AgentSession,
        op: AgentOp,
    ) -> Result<AgentOpOutcome, AgentError> {
        self.handle_op_with_cancellation(session, op, AgentCancellationToken::new())
    }

    pub fn handle_op_with_cancellation(
        &self,
        session: &mut AgentSession,
        op: AgentOp,
        cancellation_token: AgentCancellationToken,
    ) -> Result<AgentOpOutcome, AgentError> {
        self.handle_op_with_cancellation_and_progress(
            session,
            op,
            cancellation_token,
            AgentToolProgressSink::default(),
            AgentModelProgressSink::default(),
        )
    }

    pub fn handle_op_with_cancellation_and_progress(
        &self,
        session: &mut AgentSession,
        op: AgentOp,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
        model_progress_sink: AgentModelProgressSink,
    ) -> Result<AgentOpOutcome, AgentError> {
        self.handle_op_with_cancellation_and_progress_and_persistence(
            session,
            op,
            cancellation_token,
            progress_sink,
            model_progress_sink,
            true,
        )
    }

    pub(super) fn handle_op_with_cancellation_and_progress_and_persistence(
        &self,
        session: &mut AgentSession,
        op: AgentOp,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
        model_progress_sink: AgentModelProgressSink,
        persist_completed_turn: bool,
    ) -> Result<AgentOpOutcome, AgentError> {
        let cancellation_token = cancellation_token.with_timeout_ms(op.timeout_ms);
        let op_id = op.op_id;
        let session_id = session.session_id.clone();
        let mut events = Vec::new();
        let mut run = None;

        match op.action {
            AgentOpAction::StartTurn { input } => {
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::TurnStarted {
                        provider: self.config.provider.clone(),
                        model: self.config.model.clone(),
                        role: self.config.definition.role.clone(),
                        surface_id: self.tool_surface.surface_id.clone(),
                        continuation: false,
                        max_steps: self.config.max_steps,
                        run_context: self.config.run_context.clone(),
                    },
                ));
                let completed_run = self
                    .start_turn_with_cancellation_and_progress_and_persistence(
                        session,
                        &input,
                        cancellation_token.clone(),
                        progress_sink.clone(),
                        model_progress_sink.clone(),
                        persist_completed_turn,
                    )?;
                events.push(turn_completed_agent_event(
                    &op_id,
                    &session_id,
                    &completed_run,
                ));
                run = Some(completed_run);
            }
            AgentOpAction::ContinueTurn { input } => {
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::TurnStarted {
                        provider: self.config.provider.clone(),
                        model: self.config.model.clone(),
                        role: self.config.definition.role.clone(),
                        surface_id: self.tool_surface.surface_id.clone(),
                        continuation: true,
                        max_steps: self.config.max_steps,
                        run_context: self.config.run_context.clone(),
                    },
                ));
                let completed_run = self
                    .start_turn_with_cancellation_and_progress_and_persistence(
                        session,
                        &input,
                        cancellation_token.clone(),
                        progress_sink.clone(),
                        model_progress_sink.clone(),
                        persist_completed_turn,
                    )?;
                events.push(turn_completed_agent_event(
                    &op_id,
                    &session_id,
                    &completed_run,
                ));
                run = Some(completed_run);
            }
            AgentOpAction::Steer { message } => {
                let steering =
                    self.steer_with_snapshot(session, message, persist_completed_turn)?;
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::SteeringQueued { steering },
                ));
            }
            AgentOpAction::FollowUp { message } => {
                let follow_up =
                    self.follow_up_with_snapshot(session, message, persist_completed_turn)?;
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::FollowUpQueued { follow_up },
                ));
            }
            AgentOpAction::RemoveQueuedInput { input_id } => {
                let removed = self.remove_queued_input_with_snapshot(
                    session,
                    &input_id,
                    persist_completed_turn,
                )?;
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    match removed {
                        AgentRemovedQueuedInput::Steering { steering } => {
                            AgentEventPayload::SteeringRemoved { steering }
                        }
                        AgentRemovedQueuedInput::FollowUp { follow_up } => {
                            AgentEventPayload::FollowUpRemoved { follow_up }
                        }
                    },
                ));
            }
            AgentOpAction::Compact { reason } => {
                let summary =
                    self.compact_with_snapshot(session, reason, persist_completed_turn)?;
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::Compacted {
                        summary,
                        item_count: session.transcript.items.len(),
                    },
                ));
            }
            AgentOpAction::Abort { reason } => {
                let abort = self.abort_with_snapshot(session, reason, persist_completed_turn)?;
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::Aborted { abort },
                ));
            }
            AgentOpAction::CancelActiveTurn { reason } => {
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::CancellationRequested {
                        reason,
                        active_op_id: None,
                    },
                ));
            }
            AgentOpAction::Snapshot => {
                if persist_completed_turn {
                    self.save_session(session)?;
                }
                events.push(agent_session_event(
                    &op_id,
                    &session_id,
                    AgentEventPayload::Snapshot {
                        item_count: session.transcript.items.len(),
                        steering_count: session.steering_queue.len(),
                        follow_up_count: session.follow_up_queue.len(),
                        aborted: session.is_aborted(),
                    },
                ));
            }
        }

        Ok(AgentOpOutcome {
            op_id,
            session_id,
            events,
            run,
        })
    }

    pub(super) fn trace(
        &self,
        session_id: &SessionId,
        record: AgentTraceRecordKind,
    ) -> Result<(), AgentError> {
        if let AgentTraceRecordKind::Item { item } = &record {
            self.journal_item(session_id, item)?;
        }
        self.trace_store
            .append(&AgentTraceRecord::new(session_id.clone(), record))
            .map_err(|source| AgentError::TraceStore {
                detail: source.to_string(),
            })
    }

    /// Append one item to the session store the moment a session turn
    /// produces it, so a crash mid-turn cannot lose the record of a tool that
    /// already ran. Items outside a journaled session turn are not persisted
    /// here.
    fn journal_item(
        &self,
        trace_session_id: &SessionId,
        item: &AgentItem,
    ) -> Result<(), AgentError> {
        let store_session_id = {
            let journals = self.item_journals.lock().map_err(|_| AgentError::Runtime {
                detail: "agent item journal lock poisoned".to_string(),
            })?;
            match journals.get(trace_session_id) {
                Some(journal) => journal.store_session_id.clone(),
                None => return Ok(()),
            }
        };
        self.session_store_items(&store_session_id, std::slice::from_ref(item))?;
        if let Ok(mut journals) = self.item_journals.lock()
            && let Some(journal) = journals.get_mut(trace_session_id)
        {
            journal.persisted.insert(agent_item_id(item));
        }
        Ok(())
    }

    fn begin_item_journal(&self, trace_session_id: &SessionId, store_session_id: &SessionId) {
        if let Ok(mut journals) = self.item_journals.lock() {
            journals.insert(
                trace_session_id.clone(),
                AgentItemJournal {
                    store_session_id: store_session_id.clone(),
                    persisted: BTreeSet::new(),
                },
            );
        }
    }

    /// Stop journaling and return the ids that were already appended.
    fn finish_item_journal(&self, trace_session_id: &SessionId) -> BTreeSet<TaskId> {
        self.item_journals
            .lock()
            .ok()
            .and_then(|mut journals| journals.remove(trace_session_id))
            .map(|journal| journal.persisted)
            .unwrap_or_default()
    }

    pub(super) fn session_store_record(
        &self,
        session_id: &SessionId,
        record: AgentSessionStoreRecordKind,
    ) -> Result<(), AgentError> {
        self.session_store
            .append_record(&AgentSessionStoreRecord::new(session_id.clone(), record))
            .map_err(|source| AgentError::SessionStore {
                detail: source.to_string(),
            })
    }

    pub(super) fn persist_control_result(
        &self,
        result: &AgentSessionControlResult,
    ) -> Result<(), AgentError> {
        result
            .persist_to_store(self.session_store.as_ref())
            .map_err(|source| AgentError::SessionStore {
                detail: source.to_string(),
            })
    }

    pub(super) fn session_store_items(
        &self,
        session_id: &SessionId,
        items: &[AgentItem],
    ) -> Result<(), AgentError> {
        self.session_store
            .append_items(session_id, items)
            .map_err(|source| AgentError::SessionStore {
                detail: source.to_string(),
            })
    }

    pub(super) fn session_store_projection(
        &self,
        session_id: &SessionId,
        projection: &AgentSessionProjection,
    ) -> Result<(), AgentError> {
        self.session_store
            .append_projection(session_id, projection)
            .map_err(|source| AgentError::SessionStore {
                detail: source.to_string(),
            })
    }

    pub fn start_turn(
        &self,
        session: &mut AgentSession,
        input: &AgentLoopInput,
    ) -> Result<AgentRun, AgentError> {
        self.start_turn_with_cancellation(session, input, AgentCancellationToken::new())
    }

    pub fn start_turn_with_cancellation(
        &self,
        session: &mut AgentSession,
        input: &AgentLoopInput,
        cancellation_token: AgentCancellationToken,
    ) -> Result<AgentRun, AgentError> {
        self.start_turn_with_cancellation_and_progress(
            session,
            input,
            cancellation_token,
            AgentToolProgressSink::default(),
            AgentModelProgressSink::default(),
        )
    }

    pub fn start_turn_with_cancellation_and_progress(
        &self,
        session: &mut AgentSession,
        input: &AgentLoopInput,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
        model_progress_sink: AgentModelProgressSink,
    ) -> Result<AgentRun, AgentError> {
        self.start_turn_with_cancellation_and_progress_and_persistence(
            session,
            input,
            cancellation_token,
            progress_sink,
            model_progress_sink,
            true,
        )
    }

    pub(super) fn start_turn_with_cancellation_and_progress_and_persistence(
        &self,
        session: &mut AgentSession,
        input: &AgentLoopInput,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
        model_progress_sink: AgentModelProgressSink,
        persist_completed_turn: bool,
    ) -> Result<AgentRun, AgentError> {
        if session.is_aborted() {
            return Ok(AgentRun {
                final_decision: AgentLoopDecision::silent(),
                stop_reason: AgentStopReason::Aborted,
                model_metrics: AgentRunModelMetrics::default(),
                run_context: self.config.run_context.clone(),
                transcript: Vec::new(),
                model_turns: Vec::new(),
                tool_results: Vec::new(),
                items: Vec::new(),
                context_reports: Vec::new(),
                model_events: vec![AgentModelEvent::Aborted {
                    reason: session
                        .abort
                        .as_ref()
                        .map(|abort| abort.reason.clone())
                        .unwrap_or_else(|| "session aborted".to_string()),
                }],
                compactions: Vec::new(),
                events: Vec::new(),
                coding_state: None,
                candidate_evidence_bundle: None,
            });
        }
        let source_transcript = AgentTranscript {
            items: compacted_projection_items(
                &session.transcript.items,
                self.config.context_budget.retained_tail_items,
            ),
        };
        let steering = session.steering_queue.clone();
        let follow_ups = session.follow_up_queue.clone();
        let session_projection = session.projection.clone();
        self.session_store_record(
            &session.session_id,
            AgentSessionStoreRecordKind::TurnStarted {
                provider: self.config.provider.clone(),
                model: self.config.model.clone(),
                role: self.config.definition.role.clone(),
                surface_id: self.tool_surface.surface_id.clone(),
                max_steps: self.config.max_steps,
                run_context: self.config.run_context.clone(),
            },
        )?;
        let mut progress = AgentTurnProgress::default();
        self.begin_item_journal(&input.session.session_id, &session.session_id);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.run_with_session_context_and_cancellation(
                input,
                &source_transcript,
                session_projection,
                &steering,
                &follow_ups,
                cancellation_token,
                progress_sink,
                model_progress_sink,
                &mut progress,
            )
        }));
        let journaled = self.finish_item_journal(&input.session.session_id);
        let result = match result {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        let run = match result {
            Ok(run) => run,
            Err(error) => {
                if !progress.items.is_empty() {
                    progress.items.push(AgentItem::Error {
                        item_id: TaskId::generate(),
                        step_index: progress.step_index,
                        detail: error.to_string(),
                    });
                    session.steering_queue.clear();
                    session.follow_up_queue.clear();
                    session.append_turn_items(&progress.items, &progress.compactions);
                    self.persist_turn_items(
                        &session.session_id,
                        &progress.items,
                        &progress.compactions,
                        &journaled,
                    )?;
                    self.session_store_record(
                        &session.session_id,
                        AgentSessionStoreRecordKind::TurnFailed {
                            detail: error.to_string(),
                            item_count: progress.items.len(),
                        },
                    )?;
                    if persist_completed_turn {
                        self.save_session(session)?;
                    }
                }
                return Err(error);
            }
        };
        session.steering_queue.clear();
        session.follow_up_queue.clear();
        session.append_run(&run);
        self.persist_turn_items(
            &session.session_id,
            &run.items,
            &run.compactions,
            &journaled,
        )?;
        self.session_store_record(
            &session.session_id,
            AgentSessionStoreRecordKind::TurnCompleted {
                stop_reason: run.stop_reason.clone(),
                item_count: run.items.len(),
                event_count: run.events.len(),
            },
        )?;
        if persist_completed_turn {
            self.save_session(session)?;
        }
        Ok(run)
    }

    pub(super) fn persist_turn_items(
        &self,
        session_id: &SessionId,
        items: &[AgentItem],
        compactions: &[AgentCompactionSummary],
        already_persisted: &BTreeSet<TaskId>,
    ) -> Result<(), AgentError> {
        let remaining = items
            .iter()
            .filter(|item| !already_persisted.contains(&agent_item_id(item)))
            .cloned()
            .collect::<Vec<_>>();
        self.session_store_items(session_id, &remaining)?;
        for compaction in compactions {
            self.session_store_projection(session_id, &compaction.projection)?;
        }
        Ok(())
    }

    pub fn continue_turn(
        &self,
        session: &mut AgentSession,
        input: &AgentLoopInput,
    ) -> Result<AgentRun, AgentError> {
        self.continue_turn_with_cancellation(session, input, AgentCancellationToken::new())
    }

    pub fn continue_turn_with_cancellation(
        &self,
        session: &mut AgentSession,
        input: &AgentLoopInput,
        cancellation_token: AgentCancellationToken,
    ) -> Result<AgentRun, AgentError> {
        self.start_turn_with_cancellation(session, input, cancellation_token)
    }

    pub fn continue_turn_with_cancellation_and_progress(
        &self,
        session: &mut AgentSession,
        input: &AgentLoopInput,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
        model_progress_sink: AgentModelProgressSink,
    ) -> Result<AgentRun, AgentError> {
        self.start_turn_with_cancellation_and_progress(
            session,
            input,
            cancellation_token,
            progress_sink,
            model_progress_sink,
        )
    }

    pub fn steer(
        &self,
        session: &mut AgentSession,
        message: impl Into<String>,
    ) -> Result<AgentSteeringMessage, AgentError> {
        self.steer_with_snapshot(session, message, true)
    }

    fn steer_with_snapshot(
        &self,
        session: &mut AgentSession,
        message: impl Into<String>,
        save_snapshot: bool,
    ) -> Result<AgentSteeringMessage, AgentError> {
        self.with_staged_session(session, |session| {
            let steering = AgentSteeringMessage::new(message);
            session.steering_queue.push(steering.clone());
            session.advance_state_revision();
            self.session_store_record(
                &session.session_id,
                AgentSessionStoreRecordKind::SteeringQueued {
                    steering: steering.clone(),
                },
            )?;
            if save_snapshot {
                self.save_session(session)?;
            }
            Ok(steering)
        })
    }

    pub fn follow_up(
        &self,
        session: &mut AgentSession,
        message: impl Into<String>,
    ) -> Result<AgentFollowUpMessage, AgentError> {
        self.follow_up_with_snapshot(session, message, true)
    }

    fn follow_up_with_snapshot(
        &self,
        session: &mut AgentSession,
        message: impl Into<String>,
        save_snapshot: bool,
    ) -> Result<AgentFollowUpMessage, AgentError> {
        self.with_staged_session(session, |session| {
            let follow_up = AgentFollowUpMessage::new(message);
            session.follow_up_queue.push(follow_up.clone());
            session.advance_state_revision();
            self.session_store_record(
                &session.session_id,
                AgentSessionStoreRecordKind::FollowUpQueued {
                    follow_up: follow_up.clone(),
                },
            )?;
            if save_snapshot {
                self.save_session(session)?;
            }
            Ok(follow_up)
        })
    }

    /// Remove a not-yet-consumed queued message from either session queue by
    /// its id. A miss is an error so callers can surface the queued-vs-consumed
    /// race honestly instead of pretending the removal took effect.
    pub fn remove_queued_input(
        &self,
        session: &mut AgentSession,
        input_id: &TaskId,
    ) -> Result<AgentRemovedQueuedInput, AgentError> {
        self.remove_queued_input_with_snapshot(session, input_id, true)
    }

    fn remove_queued_input_with_snapshot(
        &self,
        session: &mut AgentSession,
        input_id: &TaskId,
        save_snapshot: bool,
    ) -> Result<AgentRemovedQueuedInput, AgentError> {
        self.with_staged_session(session, |session| {
            if let Some(index) = session
                .steering_queue
                .iter()
                .position(|queued| queued.steering_id == *input_id)
            {
                let steering = session.steering_queue.remove(index);
                session.advance_state_revision();
                self.session_store_record(
                    &session.session_id,
                    AgentSessionStoreRecordKind::SteeringRemoved {
                        steering: steering.clone(),
                    },
                )?;
                if save_snapshot {
                    self.save_session(session)?;
                }
                return Ok(AgentRemovedQueuedInput::Steering { steering });
            }
            if let Some(index) = session
                .follow_up_queue
                .iter()
                .position(|queued| queued.follow_up_id == *input_id)
            {
                let follow_up = session.follow_up_queue.remove(index);
                session.advance_state_revision();
                self.session_store_record(
                    &session.session_id,
                    AgentSessionStoreRecordKind::FollowUpRemoved {
                        follow_up: follow_up.clone(),
                    },
                )?;
                if save_snapshot {
                    self.save_session(session)?;
                }
                return Ok(AgentRemovedQueuedInput::FollowUp { follow_up });
            }
            Err(AgentError::Runtime {
                detail: format!("no queued steering or follow-up message with id {input_id}"),
            })
        })
    }

    pub fn compact(
        &self,
        session: &mut AgentSession,
        reason: impl Into<String>,
    ) -> Result<AgentCompactionSummary, AgentError> {
        self.compact_with_snapshot(session, reason, true)
    }

    fn compact_with_snapshot(
        &self,
        session: &mut AgentSession,
        reason: impl Into<String>,
        save_snapshot: bool,
    ) -> Result<AgentCompactionSummary, AgentError> {
        self.with_staged_session(session, |session| {
            let source_transcript = session.compaction_source_transcript();
            // Without a configured tail a manual compaction would retain
            // everything and only add a summary.
            let retained_tail_count = self
                .compactor
                .retained_tail_count
                .or(self.config.context_budget.retained_tail_items)
                .unwrap_or(DEFAULT_MANUAL_COMPACTION_RETAINED_ITEMS);
            let cancellation_token = AgentCancellationToken::new();
            let summary = self.compact_source_transcript(
                &source_transcript,
                reason.into(),
                source_transcript.items.len(),
                retained_tail_count,
                session.projection.as_ref(),
                &session.steering_queue,
                &session.follow_up_queue,
                AgentCompactionTrigger::Manual,
                &cancellation_token,
            )?;
            let compaction_item = AgentItem::CompactionSummary {
                item_id: TaskId::generate(),
                summary: summary.clone(),
            };
            session.raw_transcript.items.push(compaction_item.clone());
            session.projection = Some(summary.projection.clone());
            session.transcript.items = compacted_session_items(
                &source_transcript,
                compaction_item.clone(),
                summary.retained_tail_count,
            );
            session.advance_state_revision();
            self.session_store_items(&session.session_id, &[compaction_item])?;
            self.session_store_projection(&session.session_id, &summary.projection)?;
            self.session_store_record(
                &session.session_id,
                AgentSessionStoreRecordKind::Compacted {
                    summary: summary.clone(),
                    item_count: session.transcript.items.len(),
                },
            )?;
            if save_snapshot {
                self.save_session(session)?;
            }
            Ok(summary)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn compact_source_transcript(
        &self,
        source_transcript: &AgentTranscript,
        reason: String,
        token_estimate_before: usize,
        retained_tail_count: usize,
        previous_projection: Option<&AgentSessionProjection>,
        steering: &[AgentSteeringMessage],
        follow_ups: &[AgentFollowUpMessage],
        trigger: AgentCompactionTrigger,
        cancellation_token: &AgentCancellationToken,
    ) -> Result<AgentCompactionSummary, AgentError> {
        let engine = self
            .compaction_engine
            .read()
            .map_err(|_| AgentError::Runtime {
                detail: "compaction engine lock poisoned".to_string(),
            })?;
        engine.compact(
            self.model.as_ref(),
            AgentCompactionRequest {
                config: &self.config,
                projection_builder: &self.compactor,
                source_transcript,
                reason,
                trigger,
                token_estimate_before,
                retained_tail_count,
                previous_projection,
                steering,
                follow_ups,
                cancellation_token,
            },
        )
    }

    pub fn abort(
        &self,
        session: &mut AgentSession,
        reason: impl Into<String>,
    ) -> Result<AgentAbortRecord, AgentError> {
        self.abort_with_snapshot(session, reason, true)
    }

    fn abort_with_snapshot(
        &self,
        session: &mut AgentSession,
        reason: impl Into<String>,
        save_snapshot: bool,
    ) -> Result<AgentAbortRecord, AgentError> {
        self.with_staged_session(session, |session| {
            let abort = AgentAbortRecord::new(reason);
            session.abort = Some(abort.clone());
            session.advance_state_revision();
            self.session_store_record(
                &session.session_id,
                AgentSessionStoreRecordKind::Aborted {
                    abort: abort.clone(),
                },
            )?;
            if save_snapshot {
                self.save_session(session)?;
            }
            Ok(abort)
        })
    }

    /// Publish a session edit only after all of its persistence work succeeds.
    /// The snapshot save is the edit's commit point and runs last; callers
    /// that commit through their own snapshot (the session runtime, after it
    /// persists the control result) pass `save_snapshot = false`.
    /// Turn execution deliberately uses a different path: executed tool effects
    /// must remain represented in the session even when the turn fails.
    pub(super) fn with_staged_session<T>(
        &self,
        session: &mut AgentSession,
        update: impl FnOnce(&mut AgentSession) -> Result<T, AgentError>,
    ) -> Result<T, AgentError> {
        let mut staged = session.clone();
        let result = update(&mut staged)?;
        *session = staged;
        Ok(result)
    }
}
