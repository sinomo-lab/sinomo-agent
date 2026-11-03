//! Session control, worker ownership, cancellation, and live-input reconciliation.

use super::subagents::{AgentSubagentScope, AgentSubagents};
use super::{
    Agent, AgentAbortRecord, AgentCancellationToken, AgentCompactionSummary, AgentError,
    AgentFollowUpMessage, AgentLoopDecision, AgentLoopInput, AgentModelEvent, AgentRole, AgentRun,
    AgentRunContext, AgentSession, AgentSessionStore, AgentSessionStoreError,
    AgentSessionStoreRecord, AgentSessionStoreRecordKind, AgentSteeringMessage, AgentStopReason,
    AgentStoragePaths, AgentTaskRef, AgentToolProgressEvent, AgentToolProgressSink, Arc, BTreeSet,
    Deserialize, EventId, ExplicitAgentSkillContext, FileAgentSessionStore, IndexedFileMemoryStore,
    InteractionModel, MemoryRetriever, Mutex, Serialize, SessionId,
    SessionRoutedFileAgentTraceStore, SinomoDataDir, TaskId, agent_session_event, is_false, thread,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentOp {
    pub op_id: TaskId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    pub action: AgentOpAction,
}

impl AgentOp {
    pub fn new(action: AgentOpAction) -> Self {
        Self {
            op_id: TaskId::generate(),
            timeout_ms: None,
            action,
        }
    }

    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    pub fn start_turn(input: AgentLoopInput) -> Self {
        Self::new(AgentOpAction::StartTurn { input })
    }

    pub fn continue_turn(input: AgentLoopInput) -> Self {
        Self::new(AgentOpAction::ContinueTurn { input })
    }

    pub fn steer(message: impl Into<String>) -> Self {
        Self::new(AgentOpAction::Steer {
            message: message.into(),
        })
    }

    pub fn follow_up(message: impl Into<String>) -> Self {
        Self::new(AgentOpAction::FollowUp {
            message: message.into(),
        })
    }

    pub fn remove_queued_input(input_id: TaskId) -> Self {
        Self::new(AgentOpAction::RemoveQueuedInput { input_id })
    }

    pub fn compact(reason: impl Into<String>) -> Self {
        Self::new(AgentOpAction::Compact {
            reason: reason.into(),
        })
    }

    pub fn abort(reason: impl Into<String>) -> Self {
        Self::new(AgentOpAction::Abort {
            reason: reason.into(),
        })
    }

    pub fn cancel_active_turn(reason: impl Into<String>) -> Self {
        Self::new(AgentOpAction::CancelActiveTurn {
            reason: reason.into(),
        })
    }

    pub fn snapshot() -> Self {
        Self::new(AgentOpAction::Snapshot)
    }

    /// Stop starting model requests; running tools and processes continue.
    pub fn pause(reason: impl Into<String>) -> Self {
        Self::new(AgentOpAction::Pause {
            reason: reason.into(),
        })
    }

    pub fn unpause() -> Self {
        Self::new(AgentOpAction::Unpause)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentOpAction {
    StartTurn {
        input: AgentLoopInput,
    },
    ContinueTurn {
        input: AgentLoopInput,
    },
    Steer {
        message: String,
    },
    FollowUp {
        message: String,
    },
    RemoveQueuedInput {
        input_id: TaskId,
    },
    Compact {
        reason: String,
    },
    Abort {
        reason: String,
    },
    CancelActiveTurn {
        reason: String,
    },
    /// Start no further model requests. Unlike cancellation, in-flight tool
    /// calls, processes, and subagents keep running and progress is kept.
    Pause {
        reason: String,
    },
    Unpause,
    Snapshot,
}

impl AgentOpAction {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::StartTurn { .. } => "start_turn",
            Self::ContinueTurn { .. } => "continue_turn",
            Self::Steer { .. } => "steer",
            Self::FollowUp { .. } => "follow_up",
            Self::RemoveQueuedInput { .. } => "remove_queued_input",
            Self::Compact { .. } => "compact",
            Self::Abort { .. } => "abort",
            Self::CancelActiveTurn { .. } => "cancel_active_turn",
            Self::Pause { .. } => "pause",
            Self::Unpause => "unpause",
            Self::Snapshot => "snapshot",
        }
    }

    pub fn is_turn(&self) -> bool {
        matches!(self, Self::StartTurn { .. } | Self::ContinueTurn { .. })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentOpOutcome {
    pub op_id: TaskId,
    pub session_id: SessionId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<AgentEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<AgentRun>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentEvent {
    pub event_id: TaskId,
    pub op_id: TaskId,
    pub session_id: SessionId,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub emitted_at: time::OffsetDateTime,
    pub payload: AgentEventPayload,
}

impl AgentEvent {
    pub fn new(op_id: TaskId, session_id: SessionId, payload: AgentEventPayload) -> Self {
        Self {
            event_id: TaskId::generate(),
            op_id,
            session_id,
            emitted_at: time::OffsetDateTime::now_utc(),
            payload,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentEventPayload {
    OperationQueued {
        sequence: u64,
        action: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_ms: Option<u64>,
    },
    OperationRejected {
        sequence: u64,
        action: String,
        reason: String,
    },
    TurnStarted {
        provider: String,
        model: String,
        role: AgentRole,
        #[serde(alias = "policy_id")]
        surface_id: String,
        continuation: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_steps: Option<usize>,
        #[serde(default, skip_serializing_if = "AgentRunContext::is_empty")]
        run_context: AgentRunContext,
    },
    TurnCompleted {
        stop_reason: AgentStopReason,
        item_count: usize,
        event_count: usize,
        model_turn_count: usize,
        tool_result_count: usize,
        #[serde(default, skip_serializing_if = "AgentRunContext::is_empty")]
        run_context: AgentRunContext,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        emitted_event_ids: Vec<EventId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        candidate_evidence_bundle_id: Option<TaskId>,
    },
    SteeringQueued {
        steering: AgentSteeringMessage,
    },
    FollowUpQueued {
        follow_up: AgentFollowUpMessage,
    },
    SteeringRemoved {
        steering: AgentSteeringMessage,
    },
    FollowUpRemoved {
        follow_up: AgentFollowUpMessage,
    },
    Compacted {
        summary: AgentCompactionSummary,
        item_count: usize,
    },
    Aborted {
        abort: AgentAbortRecord,
    },
    Paused {
        pause: super::AgentPauseRecord,
    },
    Unpaused {
        pause: super::AgentPauseRecord,
    },
    CancellationRequested {
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        active_op_id: Option<TaskId>,
    },
    TurnTimedOut {
        sequence: u64,
        active_op_id: TaskId,
        timeout_ms: u64,
        reason: String,
    },
    /// A turn's host stopped (for example a daemon restart) before the turn
    /// finished. The session is paused; the next message resumes it.
    TurnInterrupted {
        sequence: u64,
        active_op_id: TaskId,
        reason: String,
    },
    TaskStatusChanged {
        snapshot: AgentTaskSnapshot,
    },
    Snapshot {
        item_count: usize,
        steering_count: usize,
        follow_up_count: usize,
        aborted: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentTaskLifecycleStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    /// Stopped by a session pause; the task can continue once unpaused.
    Paused,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentTaskSnapshot {
    pub task: AgentTaskRef,
    /// Host-projected agent identity that owns this activity. This metadata is
    /// part of the control plane rather than model-visible session state so
    /// consumers can authorize persisted task history without loading a raw
    /// agent-session snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_definition_key: Option<String>,
    pub status: AgentTaskLifecycleStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_op_id: Option<TaskId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_ids: Vec<EventId>,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub updated_at: time::OffsetDateTime,
}

impl AgentTaskSnapshot {
    pub fn new(task: AgentTaskRef, status: AgentTaskLifecycleStatus) -> Self {
        Self {
            task,
            agent_definition_key: None,
            status,
            progress_summary: None,
            active_op_id: None,
            result_ref: None,
            event_ids: Vec::new(),
            updated_at: time::OffsetDateTime::now_utc(),
        }
    }

    pub fn with_agent_definition_key(mut self, key: impl Into<String>) -> Self {
        self.agent_definition_key = Some(key.into());
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentActiveTurn {
    pub op_id: TaskId,
    pub sequence: u64,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub started_at: time::OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<TaskId>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub cancel_requested: bool,
    /// The process instance running the turn. A persisted active turn from
    /// another instance (or with no instance) was interrupted by a restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_instance: Option<String>,
}

impl AgentActiveTurn {
    pub fn new(
        op_id: TaskId,
        sequence: u64,
        timeout_ms: Option<u64>,
        task_id: Option<TaskId>,
    ) -> Self {
        Self {
            op_id,
            sequence,
            started_at: time::OffsetDateTime::now_utc(),
            timeout_ms,
            task_id,
            cancel_requested: false,
            host_instance: Some(agent_runtime_host_instance().to_string()),
        }
    }

    /// Whether this turn was started by the current process instance.
    pub fn hosted_here(&self) -> bool {
        self.host_instance.as_deref() == Some(agent_runtime_host_instance())
    }
}

/// Identifies this process as the host of the turns it starts, so a turn left
/// in a persisted control snapshot by an earlier process can be recognised as
/// interrupted rather than still running.
pub fn agent_runtime_host_instance() -> &'static str {
    static INSTANCE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    INSTANCE.get_or_init(|| TaskId::generate().to_string())
}

/// Reason recorded when a restart interrupted a turn.
pub const AGENT_RESTART_INTERRUPTION_REASON: &str =
    "interrupted by a restart before it finished; send a message to continue";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionControlSnapshot {
    pub session_id: SessionId,
    pub next_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn: Option<AgentActiveTurn>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tasks: Vec<AgentTaskSnapshot>,
    pub buffered_event_count: usize,
    /// The session is paused (interrupted): it starts no model requests
    /// until a new message or an explicit unpause resumes it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub paused: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionControlResult {
    pub sequence: u64,
    pub accepted: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<AgentEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<AgentOpOutcome>,
    pub snapshot: AgentSessionControlSnapshot,
}

impl AgentSessionControlResult {
    pub fn terminal_results(&self) -> Vec<AgentTaskTerminalResult> {
        terminal_results_from_tasks(&self.snapshot.tasks)
    }

    pub fn persist_to_store(
        &self,
        store: &dyn AgentSessionStore,
    ) -> Result<(), AgentSessionStoreError> {
        for event in &self.events {
            append_control_event_to_store(store, event)?;
        }
        append_control_snapshot_to_store(store, &self.snapshot)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSessionControlEventCursor {
    pub offset: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_event_id: Option<TaskId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionControlEventBatch {
    pub cursor: AgentSessionControlEventCursor,
    pub next_cursor: AgentSessionControlEventCursor,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<AgentEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_updates: Vec<AgentTaskSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub terminal_results: Vec<AgentTaskTerminalResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentRuntimeEventCursor {
    pub offset: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_event_id: Option<TaskId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentRuntimeEventBatch {
    pub cursor: AgentRuntimeEventCursor,
    pub next_cursor: AgentRuntimeEventCursor,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<AgentRuntimeEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentRuntimeEvent {
    pub event_id: TaskId,
    pub session_id: SessionId,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub emitted_at: time::OffsetDateTime,
    pub payload: AgentRuntimeEventPayload,
}

impl AgentRuntimeEvent {
    pub fn new(session_id: SessionId, payload: AgentRuntimeEventPayload) -> Self {
        Self {
            event_id: TaskId::generate(),
            session_id,
            emitted_at: time::OffsetDateTime::now_utc(),
            payload,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentRuntimeEventPayload {
    WorkerStarted {
        sequence: u64,
        op_id: TaskId,
        action: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_ms: Option<u64>,
    },
    WorkerCompleted {
        sequence: u64,
        op_id: TaskId,
        action: String,
        stop_reason: AgentStopReason,
    },
    WorkerFailed {
        sequence: u64,
        op_id: TaskId,
        action: String,
        detail: String,
    },
    TurnFinalDecision {
        sequence: u64,
        op_id: TaskId,
        decision: AgentLoopDecision,
        stop_reason: AgentStopReason,
    },
    ModelProgress {
        sequence: u64,
        op_id: TaskId,
        event: AgentModelEvent,
    },
    ToolProgress {
        sequence: u64,
        op_id: TaskId,
        event: AgentToolProgressEvent,
    },
    ControlEvent {
        event: AgentEvent,
    },
}

#[derive(Clone, Default)]
pub struct AgentModelProgressSink {
    pub(super) callback: Option<Arc<dyn Fn(AgentModelEvent) + Send + Sync>>,
}

impl AgentModelProgressSink {
    pub fn new(callback: impl Fn(AgentModelEvent) + Send + Sync + 'static) -> Self {
        Self {
            callback: Some(Arc::new(callback)),
        }
    }

    pub fn emit(&self, event: AgentModelEvent) {
        if let Some(callback) = &self.callback {
            callback(event);
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.callback.is_some()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentTaskTerminalResult {
    pub task: AgentTaskRef,
    pub status: AgentTaskLifecycleStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_ids: Vec<EventId>,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub updated_at: time::OffsetDateTime,
}

#[derive(Clone, Debug)]
pub struct AgentSessionControlPlane {
    pub(super) session: AgentSession,
    pub(super) next_sequence: u64,
    pub(super) active_turn: Option<AgentActiveTurn>,
    pub(super) active_turn_cancellation: Option<AgentCancellationToken>,
    /// Sequence of a turn whose cancellation was accepted while its worker was
    /// still running. Its terminal state is already published as cancelled,
    /// and the worker's eventual outcome must not overwrite it.
    pub(super) cancelled_turn_sequence: Option<u64>,
    pub(super) tasks: Vec<AgentTaskSnapshot>,
    pub(super) events: Vec<AgentEvent>,
}

impl AgentSessionControlPlane {
    pub fn new(session: AgentSession) -> Self {
        Self {
            session,
            next_sequence: 0,
            active_turn: None,
            active_turn_cancellation: None,
            cancelled_turn_sequence: None,
            tasks: Vec::new(),
            events: Vec::new(),
        }
    }

    pub fn from_snapshot(
        session: AgentSession,
        snapshot: AgentSessionControlSnapshot,
    ) -> Result<Self, AgentSessionControlRestoreError> {
        if snapshot.session_id != session.session_id {
            return Err(AgentSessionControlRestoreError::SessionMismatch {
                expected: session.session_id,
                actual: snapshot.session_id,
            });
        }
        Ok(Self {
            session,
            next_sequence: snapshot.next_sequence,
            active_turn: snapshot.active_turn,
            active_turn_cancellation: None,
            cancelled_turn_sequence: None,
            tasks: snapshot.tasks,
            events: Vec::new(),
        })
    }

    pub fn session(&self) -> &AgentSession {
        &self.session
    }

    pub fn session_mut(&mut self) -> &mut AgentSession {
        &mut self.session
    }

    pub fn active_turn(&self) -> Option<&AgentActiveTurn> {
        self.active_turn.as_ref()
    }

    pub fn events(&self) -> &[AgentEvent] {
        &self.events
    }

    pub fn events_since(&self, offset: usize) -> &[AgentEvent] {
        self.events.get(offset..).unwrap_or_default()
    }

    pub fn event_cursor(&self) -> AgentSessionControlEventCursor {
        AgentSessionControlEventCursor {
            offset: self.events.len(),
            last_event_id: self.events.last().map(|event| event.event_id.clone()),
        }
    }

    pub fn progress_since(
        &self,
        cursor: AgentSessionControlEventCursor,
    ) -> AgentSessionControlEventBatch {
        agent_session_control_event_batch_from_events(cursor, &self.events)
    }

    pub fn drain_events(&mut self) -> Vec<AgentEvent> {
        std::mem::take(&mut self.events)
    }

    pub fn tasks(&self) -> &[AgentTaskSnapshot] {
        &self.tasks
    }

    pub fn task_snapshot(&self, task_id: &TaskId) -> Option<&AgentTaskSnapshot> {
        self.tasks
            .iter()
            .find(|snapshot| &snapshot.task.task_id == task_id)
    }

    pub fn terminal_task_snapshots(&self) -> impl Iterator<Item = &AgentTaskSnapshot> {
        self.tasks.iter().filter(|snapshot| {
            matches!(
                snapshot.status,
                AgentTaskLifecycleStatus::Completed
                    | AgentTaskLifecycleStatus::Failed
                    | AgentTaskLifecycleStatus::Cancelled
                    | AgentTaskLifecycleStatus::TimedOut
            )
        })
    }

    pub fn terminal_result(&self, task_id: &TaskId) -> Option<AgentTaskTerminalResult> {
        self.task_snapshot(task_id)
            .and_then(AgentTaskTerminalResult::from_snapshot)
    }

    pub fn terminal_results(&self) -> Vec<AgentTaskTerminalResult> {
        terminal_results_from_tasks(&self.tasks)
    }

    pub fn persist_events_to_store(
        &self,
        store: &dyn AgentSessionStore,
        events: &[AgentEvent],
    ) -> Result<(), AgentSessionStoreError> {
        for event in events {
            append_control_event_to_store(store, event)?;
        }
        Ok(())
    }

    pub fn persist_snapshot_to_store(
        &self,
        store: &dyn AgentSessionStore,
    ) -> Result<(), AgentSessionStoreError> {
        append_control_snapshot_to_store(store, &self.snapshot())
    }

    pub fn persist_result_to_store(
        &self,
        store: &dyn AgentSessionStore,
        result: &AgentSessionControlResult,
    ) -> Result<(), AgentSessionStoreError> {
        result.persist_to_store(store)
    }

    pub fn snapshot(&self) -> AgentSessionControlSnapshot {
        AgentSessionControlSnapshot {
            session_id: self.session.session_id.clone(),
            next_sequence: self.next_sequence,
            active_turn: self.active_turn.clone(),
            tasks: self.tasks.clone(),
            buffered_event_count: self.events.len(),
            paused: self.session.is_paused(),
        }
    }

    pub fn submit_op<M: InteractionModel>(
        &mut self,
        agent: &Agent<M>,
        op: AgentOp,
    ) -> Result<AgentSessionControlResult, AgentError> {
        if op.action.is_turn() {
            // A failed turn may already have executed tools. Its partial
            // session and failure events must survive the error.
            return self.submit_op_in_place(agent, op, true);
        }
        let mut staged = self.clone();
        let result = staged.submit_op_in_place(agent, op, true)?;
        *self = staged;
        Ok(result)
    }

    /// `save_snapshot = false` leaves a control edit's session snapshot to the
    /// caller, which saves it as the commit point after persisting the
    /// control result.
    pub(super) fn submit_op_in_place<M: InteractionModel>(
        &mut self,
        agent: &Agent<M>,
        op: AgentOp,
        save_snapshot: bool,
    ) -> Result<AgentSessionControlResult, AgentError> {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        let op_id = op.op_id.clone();
        let action = op.action.kind().to_string();
        let is_turn = op.action.is_turn();
        let abort_reason = match &op.action {
            AgentOpAction::Abort { reason } => Some(reason.clone()),
            _ => None,
        };
        let timeout_ms = op.timeout_ms;
        let cancellation_token =
            is_turn.then(|| AgentCancellationToken::new().with_timeout_ms(timeout_ms));
        let mut events = vec![agent_session_event(
            &op_id,
            &self.session.session_id,
            AgentEventPayload::OperationQueued {
                sequence,
                action: action.clone(),
                timeout_ms,
            },
        )];

        if matches!(&op.action, AgentOpAction::CancelActiveTurn { .. }) {
            let AgentOpAction::CancelActiveTurn { reason } = op.action else {
                unreachable!("cancel action already matched")
            };
            let result = self.cancel_active_turn(sequence, &op_id, &action, reason, events);
            let mut result = result;
            self.events.extend(result.events.iter().cloned());
            result.snapshot = self.snapshot();
            return Ok(result);
        }

        if is_turn && self.active_turn.is_some() {
            events.push(agent_session_event(
                &op_id,
                &self.session.session_id,
                AgentEventPayload::OperationRejected {
                    sequence,
                    action,
                    reason: "session already has an active turn".to_string(),
                },
            ));
            self.events.extend(events.iter().cloned());
            return Ok(AgentSessionControlResult {
                sequence,
                accepted: false,
                events,
                outcome: None,
                snapshot: self.snapshot(),
            });
        }

        let task = if is_turn {
            agent.config.run_context.task.clone()
        } else {
            None
        };
        if is_turn {
            self.active_turn = Some(AgentActiveTurn::new(
                op_id.clone(),
                sequence,
                timeout_ms,
                task.as_ref().map(|task| task.task_id.clone()),
            ));
            self.active_turn_cancellation = cancellation_token.clone();
            if let Some(task) = task.clone() {
                let event = self.set_task_status(
                    &op_id,
                    task,
                    AgentTaskLifecycleStatus::Running,
                    Some("turn running".to_string()),
                    Some(op_id.clone()),
                    None,
                    Vec::new(),
                );
                events.push(event);
            }
        }

        let deadline_probe = cancellation_token.clone();
        let outcome = agent.handle_op_with_cancellation_and_progress_and_persistence(
            &mut self.session,
            op,
            cancellation_token.unwrap_or_else(AgentCancellationToken::new),
            AgentToolProgressSink::default(),
            AgentModelProgressSink::default(),
            is_turn || save_snapshot,
        );
        let timed_out = is_turn
            && deadline_probe
                .as_ref()
                .is_some_and(AgentCancellationToken::deadline_expired);
        if is_turn {
            self.active_turn = None;
            self.active_turn_cancellation = None;
        }
        if timed_out && let Some(timeout_ms) = timeout_ms {
            events.push(agent_session_event(
                &op_id,
                &self.session.session_id,
                AgentEventPayload::TurnTimedOut {
                    sequence,
                    active_op_id: op_id.clone(),
                    timeout_ms,
                    reason: "turn exceeded configured timeout".to_string(),
                },
            ));
        }
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                events.push(agent_session_event(
                    &op_id,
                    &self.session.session_id,
                    AgentEventPayload::OperationRejected {
                        sequence,
                        action,
                        reason: format!("turn execution failed: {error}"),
                    },
                ));
                if let Some(task) = task {
                    events.push(self.set_task_status(
                        &op_id,
                        task,
                        if timed_out {
                            AgentTaskLifecycleStatus::TimedOut
                        } else {
                            AgentTaskLifecycleStatus::Failed
                        },
                        Some(error.to_string()),
                        None,
                        None,
                        Vec::new(),
                    ));
                }
                self.events.extend(events);
                return Err(error);
            }
        };
        events.extend(outcome.events.iter().cloned());
        // Aborting a session stops all of its work, including a running turn.
        if let Some(reason) = abort_reason
            && self.active_turn.is_some()
        {
            let cancelled = self.cancel_active_turn(sequence, &op_id, &action, reason, Vec::new());
            events.extend(cancelled.events);
        }

        if is_turn && let Some(task) = task {
            let status = if timed_out {
                AgentTaskLifecycleStatus::TimedOut
            } else {
                task_status_for_run(&outcome.run)
            };
            let progress_summary = outcome.run.as_ref().map(|run| {
                format!(
                    "turn completed with {:?} after {} model turn(s) and {} tool result(s)",
                    run.stop_reason,
                    run.model_turns.len(),
                    run.tool_results.len()
                )
            });
            let result_ref = outcome
                .run
                .as_ref()
                .and_then(|run| run.candidate_evidence_bundle.as_ref())
                .map(|bundle| bundle.bundle_id.as_str().to_string());
            let event_ids = outcome
                .run
                .as_ref()
                .map(|run| {
                    run.events
                        .iter()
                        .map(|event| event.event_id.clone())
                        .collect()
                })
                .unwrap_or_default();
            events.push(self.set_task_status(
                &op_id,
                task,
                status,
                progress_summary,
                None,
                result_ref,
                event_ids,
            ));
        }

        self.events.extend(events.iter().cloned());
        let result = AgentSessionControlResult {
            sequence,
            accepted: true,
            events,
            outcome: Some(outcome),
            snapshot: self.snapshot(),
        };
        Ok(result)
    }

    pub fn cleanup_timed_out_turn(
        &mut self,
        reason: impl Into<String>,
    ) -> Option<AgentSessionControlResult> {
        let active_turn = self.active_turn.clone()?;
        let timeout_ms = active_turn.timeout_ms?;
        let elapsed_ms = (time::OffsetDateTime::now_utc() - active_turn.started_at)
            .whole_milliseconds()
            .max(0) as u64;
        if elapsed_ms < timeout_ms {
            return None;
        }
        if let Some(token) = &self.active_turn_cancellation {
            token.cancel();
        }
        self.active_turn_cancellation = None;
        self.active_turn = None;
        let op_id = active_turn.op_id.clone();
        let reason = reason.into();
        let mut events = vec![agent_session_event(
            &op_id,
            &self.session.session_id,
            AgentEventPayload::TurnTimedOut {
                sequence: active_turn.sequence,
                active_op_id: active_turn.op_id.clone(),
                timeout_ms,
                reason: reason.clone(),
            },
        )];
        if let Some(task_id) = active_turn.task_id
            && let Some(task) = self
                .tasks
                .iter()
                .find(|snapshot| snapshot.task.task_id == task_id)
                .map(|snapshot| snapshot.task.clone())
        {
            events.push(self.set_task_status(
                &op_id,
                task,
                AgentTaskLifecycleStatus::TimedOut,
                Some(reason),
                None,
                None,
                Vec::new(),
            ));
        }
        self.events.extend(events.iter().cloned());
        let result = AgentSessionControlResult {
            sequence: active_turn.sequence,
            accepted: true,
            events,
            outcome: None,
            snapshot: self.snapshot(),
        };
        Some(result)
    }

    pub(super) fn cancel_active_turn(
        &mut self,
        sequence: u64,
        op_id: &TaskId,
        action: &str,
        reason: String,
        mut events: Vec<AgentEvent>,
    ) -> AgentSessionControlResult {
        if let Some(token) = &self.active_turn_cancellation {
            token.cancel();
        }
        self.active_turn_cancellation = None;
        let Some(active_turn) = self.active_turn.take() else {
            events.push(agent_session_event(
                op_id,
                &self.session.session_id,
                AgentEventPayload::OperationRejected {
                    sequence,
                    action: action.to_string(),
                    reason: "session has no active turn".to_string(),
                },
            ));
            return AgentSessionControlResult {
                sequence,
                accepted: false,
                events,
                outcome: None,
                snapshot: self.snapshot(),
            };
        };

        self.cancelled_turn_sequence = Some(active_turn.sequence);
        events.push(agent_session_event(
            op_id,
            &self.session.session_id,
            AgentEventPayload::CancellationRequested {
                reason: reason.clone(),
                active_op_id: Some(active_turn.op_id.clone()),
            },
        ));
        if let Some(task_id) = active_turn.task_id
            && let Some(task) = self
                .tasks
                .iter()
                .find(|snapshot| snapshot.task.task_id == task_id)
                .map(|snapshot| snapshot.task.clone())
        {
            events.push(self.set_task_status(
                op_id,
                task,
                AgentTaskLifecycleStatus::Cancelled,
                Some(reason),
                None,
                None,
                Vec::new(),
            ));
        }
        AgentSessionControlResult {
            sequence,
            accepted: true,
            events,
            outcome: None,
            snapshot: self.snapshot(),
        }
    }

    pub(super) fn set_task_status(
        &mut self,
        op_id: &TaskId,
        task: AgentTaskRef,
        status: AgentTaskLifecycleStatus,
        progress_summary: Option<String>,
        active_op_id: Option<TaskId>,
        result_ref: Option<String>,
        event_ids: Vec<EventId>,
    ) -> AgentEvent {
        // Ownership is immutable once a task exists. In particular, do not
        // claim a legacy unowned task during a later status update and do not
        // move an in-flight task when the live session switches agents.
        let agent_definition_key = self
            .tasks
            .iter()
            .find(|existing| existing.task.task_id == task.task_id)
            .map(|existing| existing.agent_definition_key.clone())
            .unwrap_or_else(|| self.session.agent_definition_key.clone());
        let snapshot = AgentTaskSnapshot {
            task,
            agent_definition_key,
            status,
            progress_summary,
            active_op_id,
            result_ref,
            event_ids,
            updated_at: time::OffsetDateTime::now_utc(),
        };
        if let Some(existing) = self
            .tasks
            .iter_mut()
            .find(|existing| existing.task.task_id == snapshot.task.task_id)
        {
            *existing = snapshot.clone();
        } else {
            self.tasks.push(snapshot.clone());
        }
        agent_session_event(
            op_id,
            &self.session.session_id,
            AgentEventPayload::TaskStatusChanged { snapshot },
        )
    }
}

pub fn agent_session_control_event_batch_from_events(
    cursor: AgentSessionControlEventCursor,
    events: &[AgentEvent],
) -> AgentSessionControlEventBatch {
    let start = session_event_start_for_cursor(&cursor, events);
    let batch_events: Vec<_> = events[start..].to_vec();
    let task_updates = batch_events
        .iter()
        .filter_map(|event| match &event.payload {
            AgentEventPayload::TaskStatusChanged { snapshot } => Some(snapshot.clone()),
            _ => None,
        })
        .collect();
    let terminal_results = batch_events
        .iter()
        .filter_map(|event| match &event.payload {
            AgentEventPayload::TaskStatusChanged { snapshot } => {
                AgentTaskTerminalResult::from_snapshot(snapshot)
            }
            _ => None,
        })
        .collect();
    AgentSessionControlEventBatch {
        cursor,
        next_cursor: AgentSessionControlEventCursor {
            offset: start + batch_events.len(),
            last_event_id: last_session_event_id_before(start + batch_events.len(), events),
        },
        events: batch_events,
        task_updates,
        terminal_results,
    }
}

pub(super) fn session_event_start_for_cursor(
    cursor: &AgentSessionControlEventCursor,
    events: &[AgentEvent],
) -> usize {
    if let Some(last_event_id) = &cursor.last_event_id
        && let Some(index) = events
            .iter()
            .position(|event| &event.event_id == last_event_id)
    {
        return index + 1;
    }
    cursor.offset.min(events.len())
}

pub(super) fn last_session_event_id_before(offset: usize, events: &[AgentEvent]) -> Option<TaskId> {
    let offset = offset.min(events.len());
    if offset == 0 {
        None
    } else {
        events.get(offset - 1).map(|event| event.event_id.clone())
    }
}

pub struct AgentSessionRuntime<M>
where
    M: InteractionModel + Send + Sync + 'static,
{
    pub(super) agent: Arc<Agent<M>>,
    pub(super) state: Arc<Mutex<AgentSessionRuntimeState>>,
    pub(super) subagent_scope: Arc<AgentSubagentScope>,
}

impl<M> Clone for AgentSessionRuntime<M>
where
    M: InteractionModel + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            agent: Arc::clone(&self.agent),
            state: Arc::clone(&self.state),
            subagent_scope: Arc::clone(&self.subagent_scope),
        }
    }
}

#[derive(Clone, Default)]
pub struct AgentRuntimeStorage {
    pub(super) memory_retriever: Option<Arc<dyn MemoryRetriever>>,
    pub(super) durable_paths: Option<AgentStoragePaths>,
    pub(super) durable_data_dir: Option<SinomoDataDir>,
}

impl AgentRuntimeStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_data_dir(data_dir: &SinomoDataDir) -> Self {
        Self {
            memory_retriever: Some(Arc::new(IndexedFileMemoryStore::from_data_dir(data_dir))),
            durable_paths: Some(AgentStoragePaths::from_data_dir(data_dir)),
            durable_data_dir: Some(data_dir.clone()),
        }
    }

    pub fn with_memory_retriever<T>(mut self, retriever: T) -> Self
    where
        T: MemoryRetriever + 'static,
    {
        self.memory_retriever = Some(Arc::new(retriever));
        self
    }

    pub fn with_shared_memory_retriever(mut self, retriever: Arc<dyn MemoryRetriever>) -> Self {
        self.memory_retriever = Some(retriever);
        self
    }

    pub fn memory_retriever(&self) -> Option<Arc<dyn MemoryRetriever>> {
        self.memory_retriever.clone()
    }

    pub fn load_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<AgentSession>, AgentSessionStoreError> {
        let Some(paths) = &self.durable_paths else {
            return Ok(None);
        };
        let store = self
            .durable_data_dir
            .as_ref()
            .map(FileAgentSessionStore::for_data_dir)
            .unwrap_or_else(|| FileAgentSessionStore::new(paths.clone()));
        store.load_session(session_id)
    }

    pub(super) fn apply_to_agent<M>(&self, mut agent: Agent<M>, _session_id: &SessionId) -> Agent<M>
    where
        M: InteractionModel,
    {
        if agent.session_tools.is_none()
            && let Some(data_dir) = &self.durable_data_dir
        {
            match super::NativeAgentSessionTools::resolve_shared(data_dir) {
                Ok(provider) => agent = agent.with_session_tool_provider(provider),
                Err(error) => tracing::debug!(%error, "native session tools unavailable"),
            }
        }
        if agent.memory_retriever.is_none()
            && let Some(retriever) = &self.memory_retriever
        {
            agent = agent.with_shared_memory_retriever(retriever.clone());
        }
        if let Some(paths) = &self.durable_paths {
            if !agent.session_store_configured {
                let store = self
                    .durable_data_dir
                    .as_ref()
                    .map(FileAgentSessionStore::for_data_dir)
                    .unwrap_or_else(|| FileAgentSessionStore::new(paths.clone()));
                agent = agent.with_session_store(store);
            }
            if !agent.trace_store_configured {
                agent =
                    agent.with_trace_store(SessionRoutedFileAgentTraceStore::new(paths.clone()));
            }
        }
        agent
    }
}

pub(super) struct AgentSessionRuntimeState {
    pub(super) control: AgentSessionControlPlane,
    /// Queue items captured by the active worker. They remain in the durable
    /// live queue until represented in a saved complete or failed turn; a
    /// failure before execution leaves them queued for the next turn.
    pub(super) in_flight_input_ids: BTreeSet<TaskId>,
    pub(super) runtime_events: Vec<AgentRuntimeEvent>,
    pub(super) completed_turns: Vec<AgentOpOutcome>,
    /// Authoritative worker-liveness marker. A joiner temporarily owns and
    /// removes the `JoinHandle`, so handle presence alone cannot fence a new
    /// turn while the previous worker is still exiting.
    pub(super) turn_worker_running: bool,
    pub(super) worker: Option<thread::JoinHandle<Result<(), AgentError>>>,
    /// Completions of detached tool calls waiting for the host to continue
    /// the session with them.
    pub(super) async_updates: Vec<crate::AsyncTaskUpdate>,
    /// Stops the session's detached tool calls.
    pub(super) detached_cancellation: AgentCancellationToken,
}

impl<M> AgentSessionRuntime<M>
where
    M: InteractionModel + Send + Sync + 'static,
{
    pub fn new(agent: Agent<M>, session: AgentSession) -> Self {
        Self::from_arc(Arc::new(agent), session)
    }

    pub fn with_storage(
        agent: Agent<M>,
        session: AgentSession,
        storage: AgentRuntimeStorage,
    ) -> Self {
        let session_id = session.session_id.clone();
        Self::new(storage.apply_to_agent(agent, &session_id), session)
    }

    pub fn with_data_dir_storage(
        agent: Agent<M>,
        session: AgentSession,
        data_dir: &SinomoDataDir,
    ) -> Self {
        Self::with_storage(agent, session, AgentRuntimeStorage::from_data_dir(data_dir))
    }

    pub fn from_arc(agent: Arc<Agent<M>>, session: AgentSession) -> Self {
        let subagent_scope = agent.subagent_scope(session.clone());
        let runtime = Self {
            agent,
            subagent_scope,
            state: Arc::new(Mutex::new(AgentSessionRuntimeState {
                control: AgentSessionControlPlane::new(session),
                in_flight_input_ids: BTreeSet::new(),
                runtime_events: Vec::new(),
                completed_turns: Vec::new(),
                turn_worker_running: false,
                worker: None,
                async_updates: Vec::new(),
                detached_cancellation: AgentCancellationToken::new(),
            })),
        };
        runtime.bind_session_messages();
        runtime
    }

    pub fn submit_op(&self, op: AgentOp) -> Result<AgentSessionControlResult, AgentError> {
        if op.action.is_turn() {
            return self.submit_turn_op(op);
        }
        let mut state = self.lock_state()?;
        if matches!(&op.action, AgentOpAction::Compact { .. })
            && (state.control.active_turn.is_some() || state.turn_worker_running)
        {
            return Err(AgentError::Runtime {
                detail: "cannot compact an agent session while a turn is active".to_string(),
            });
        }
        if let AgentOpAction::RemoveQueuedInput { input_id } = &op.action
            && state.in_flight_input_ids.contains(input_id)
        {
            return Err(AgentError::Runtime {
                detail: format!(
                    "queued input {input_id} is already in flight and cannot be removed"
                ),
            });
        }
        let mut staged = state.control.clone();
        let cancelling_children = matches!(
            &op.action,
            AgentOpAction::Abort { .. } | AgentOpAction::CancelActiveTurn { .. }
        );
        // Cancellation is an effect too: a rejected control transaction must
        // not signal the worker through the token shared by the staged clone.
        let cancellation = if matches!(
            op.action,
            AgentOpAction::CancelActiveTurn { .. } | AgentOpAction::Abort { .. }
        ) {
            staged.active_turn_cancellation.take()
        } else {
            None
        };
        let explicit_snapshot = matches!(op.action, AgentOpAction::Snapshot);
        let pause_signal = match &op.action {
            AgentOpAction::Pause { .. } => Some(true),
            AgentOpAction::Unpause => Some(false),
            _ => None,
        };
        let result = staged.submit_op_in_place(self.agent.as_ref(), op, false)?;
        self.agent.persist_control_result(&result)?;
        // The snapshot is the commit point: until it is saved, a restart
        // resumes the previous session, matching the in-memory rollback.
        if explicit_snapshot || staged.session != state.control.session {
            self.agent.save_session(&staged.session)?;
        }
        if result.accepted && cancelling_children {
            self.subagent_scope.cancel_children();
        }
        if result.accepted
            && let Some(token) = cancellation
        {
            token.cancel();
        }
        // A running turn stops before its next model request; its tools and
        // any subagents keep running. Unpausing lets it carry on.
        if result.accepted
            && let Some(pause) = pause_signal
        {
            if let Some(token) = &staged.active_turn_cancellation {
                if pause {
                    token.pause();
                } else {
                    token.unpause();
                }
            }
            self.subagent_scope.set_children_paused(pause);
        }
        state.control = staged;
        self.subagent_scope
            .manager
            .set_session(&state.control.session);
        state.append_control_runtime_events(&result.events);
        Ok(result)
    }

    /// Interrupt the session, which is the same as pausing it: no further
    /// model request starts, while running tool calls, processes, and
    /// subagents finish and their results are kept. The next message resumes.
    pub fn interrupt_active_turn(
        &self,
        reason: impl Into<String>,
    ) -> Result<AgentSessionControlResult, AgentError> {
        self.submit_op(AgentOp::pause(reason))
    }

    /// Cancel the active turn immediately: the model stream, running tool
    /// calls, processes, and subagents are stopped. Work in progress may be
    /// lost.
    pub fn cancel_active_turn(
        &self,
        reason: impl Into<String>,
    ) -> Result<AgentSessionControlResult, AgentError> {
        self.submit_op(AgentOp::cancel_active_turn(reason))
    }

    pub fn join_active_turn(&self) -> Result<(), AgentError> {
        let worker = {
            let mut state = self.lock_state()?;
            state.worker.take()
        };
        if let Some(worker) = worker {
            join_agent_session_worker(worker)?;
        }
        Ok(())
    }

    pub fn session_snapshot(&self) -> Result<AgentSession, AgentError> {
        Ok(self.lock_state()?.control.session.clone())
    }

    pub fn subagents(&self) -> Arc<AgentSubagents> {
        Arc::clone(&self.subagent_scope.manager)
    }

    pub fn load_explicit_skill_context(
        &self,
        skill_id: &str,
    ) -> Result<ExplicitAgentSkillContext, AgentError> {
        self.agent.load_explicit_skill_context(skill_id)
    }

    pub fn replace_session(&self, session: AgentSession) -> Result<(), AgentError> {
        self.reap_finished_worker()?;
        let mut state = self.lock_state()?;
        if state.control.active_turn.is_some() || state.turn_worker_running {
            return Err(AgentError::Runtime {
                detail: "cannot replace agent session while a turn is active".to_string(),
            });
        }
        self.subagent_scope.replace_session(&session);
        if state.control.session.session_id != session.session_id {
            // Another session's detached calls keep reporting to that
            // session; this one's are restored from its records on attach.
            state.async_updates.clear();
        }
        state.control = AgentSessionControlPlane::new(session);
        state.in_flight_input_ids.clear();
        state.runtime_events.clear();
        state.completed_turns.clear();
        drop(state);
        self.bind_session_messages();
        Ok(())
    }

    pub fn control_snapshot(&self) -> Result<AgentSessionControlSnapshot, AgentError> {
        Ok(self.lock_state()?.control.snapshot())
    }

    /// Identity shared by every handle to this runtime.
    pub fn runtime_identity(&self) -> usize {
        Arc::as_ptr(&self.state) as *const () as usize
    }

    /// Reconcile work that an earlier process left running. When the
    /// persisted control state still shows an active turn from another
    /// process instance, or tasks that never finished, those tasks become
    /// `Paused`, a `TurnInterrupted` event is recorded, and the session is
    /// paused so it starts nothing on its own. The next message resumes it.
    ///
    /// The transcript side (tool calls without results) is already repaired
    /// when the session is loaded. Returns `None` when there was nothing to
    /// reconcile, or when the turn belongs to this process and may be live.
    pub fn recover_interrupted_turn(
        &self,
    ) -> Result<Option<AgentSessionControlResult>, AgentError> {
        self.reap_finished_worker()?;
        self.restore_detached_calls()?;
        let session_id = {
            let state = self.lock_state()?;
            if state.control.active_turn.is_some() || state.turn_worker_running {
                return Ok(None);
            }
            state.control.session.session_id.clone()
        };
        let Some(persisted) = self.agent.load_control_snapshot(&session_id)? else {
            return Ok(None);
        };
        if persisted
            .active_turn
            .as_ref()
            .is_some_and(AgentActiveTurn::hosted_here)
        {
            return Ok(None);
        }
        let unfinished = persisted
            .tasks
            .iter()
            .filter(|task| {
                matches!(
                    task.status,
                    AgentTaskLifecycleStatus::Pending | AgentTaskLifecycleStatus::Running
                )
            })
            .map(|task| task.task.clone())
            .collect::<Vec<_>>();
        if persisted.active_turn.is_none() && unfinished.is_empty() {
            return Ok(None);
        }

        let mut state = self.lock_state()?;
        if state.control.active_turn.is_some()
            || state.turn_worker_running
            || state.control.session.session_id != session_id
        {
            return Ok(None);
        }
        let mut staged = state.control.clone();
        staged.next_sequence = staged.next_sequence.max(persisted.next_sequence);
        for task in &persisted.tasks {
            if !staged
                .tasks
                .iter()
                .any(|existing| existing.task.task_id == task.task.task_id)
            {
                staged.tasks.push(task.clone());
            }
        }
        let sequence = staged.next_sequence;
        staged.next_sequence = staged.next_sequence.saturating_add(1);
        let op_id = persisted
            .active_turn
            .as_ref()
            .map(|turn| turn.op_id.clone())
            .unwrap_or_else(TaskId::generate);
        let mut events = Vec::new();
        if let Some(turn) = &persisted.active_turn {
            events.push(agent_session_event(
                &op_id,
                &session_id,
                AgentEventPayload::TurnInterrupted {
                    sequence: turn.sequence,
                    active_op_id: turn.op_id.clone(),
                    reason: AGENT_RESTART_INTERRUPTION_REASON.to_string(),
                },
            ));
        }
        for task in unfinished {
            events.push(staged.set_task_status(
                &op_id,
                task,
                AgentTaskLifecycleStatus::Paused,
                Some(AGENT_RESTART_INTERRUPTION_REASON.to_string()),
                None,
                None,
                Vec::new(),
            ));
        }
        staged.events.extend(events.iter().cloned());
        let result = AgentSessionControlResult {
            sequence,
            accepted: true,
            events,
            outcome: None,
            snapshot: staged.snapshot(),
        };
        self.agent.persist_control_result(&result)?;
        state.control = staged;
        state.append_control_runtime_events(&result.events);
        let already_paused = state.control.session.is_paused();
        drop(state);
        if already_paused {
            return Ok(Some(result));
        }
        self.submit_op(AgentOp::pause(AGENT_RESTART_INTERRUPTION_REASON))
            .map(Some)
    }

    pub fn active_turn(&self) -> Result<Option<AgentActiveTurn>, AgentError> {
        Ok(self.lock_state()?.control.active_turn.clone())
    }

    /// Whether the session still has work in flight: a turn, including tool
    /// calls a paused turn is finishing, or running subagents.
    pub fn has_running_work(&self) -> Result<bool, AgentError> {
        self.has_work(true)
    }

    /// Work which still executes, rather than a durable completion waiting
    /// for a later model turn. Shutdown must retain the latter for reattach.
    pub fn has_live_work(&self) -> Result<bool, AgentError> {
        self.has_work(false)
    }

    fn has_work(&self, include_ready_updates: bool) -> Result<bool, AgentError> {
        let session_id = {
            let mut state = self.lock_state()?;
            state.collect_detached_completions();
            // Ordinary idle checks include ready continuations. Shutdown joins
            // executing work while leaving those completions for reattach.
            if state.control.active_turn.is_some()
                || state.turn_worker_running
                || detached_calls_running(&state.control.session.session_id) > 0
                || (include_ready_updates
                    && (!state.async_updates.is_empty()
                        || !state.control.session.steering_queue.is_empty()
                        || !state.control.session.follow_up_queue.is_empty()))
            {
                return Ok(true);
            }
            state.control.session.session_id.clone()
        };
        Ok(self
            .subagents()
            .list(&session_id)
            .iter()
            .any(|child| matches!(child.status, super::AgentSubagentStatus::Running)))
    }

    pub fn event_cursor(&self) -> Result<AgentSessionControlEventCursor, AgentError> {
        Ok(self.lock_state()?.control.event_cursor())
    }

    pub fn runtime_event_cursor(&self) -> Result<AgentRuntimeEventCursor, AgentError> {
        Ok(self.lock_state()?.runtime_event_cursor())
    }

    pub fn progress_since(
        &self,
        cursor: AgentSessionControlEventCursor,
    ) -> Result<AgentSessionControlEventBatch, AgentError> {
        Ok(self.lock_state()?.control.progress_since(cursor))
    }

    /// Completions of detached tool calls the session has not continued with
    /// yet, oldest first. The host continues the session with each.
    /// A paused session continues with nothing until it is resumed.
    pub fn take_async_updates(&self) -> Vec<crate::AsyncTaskUpdate> {
        self.state
            .lock()
            .map(|mut state| {
                state.collect_detached_completions();
                if state.control.session.is_paused() {
                    Vec::new()
                } else {
                    std::mem::take(&mut state.async_updates)
                }
            })
            .unwrap_or_default()
    }

    pub fn has_async_updates(&self) -> bool {
        self.state.lock().is_ok_and(|mut state| {
            state.collect_detached_completions();
            !state.async_updates.is_empty() && !state.control.session.is_paused()
        })
    }

    /// Rebuild the attached session's detached calls from its records. A
    /// completion it has not continued with is queued again; a call no
    /// longer running in this process was lost with the process that ran it
    /// and completes as interrupted.
    pub fn restore_detached_calls(&self) -> Result<(), AgentError> {
        let session_id = self.lock_state()?.control.session.session_id.clone();
        let calls = self.agent.load_detached_calls(&session_id)?;
        let mut state = self.lock_state()?;
        if state.control.session.session_id != session_id {
            return Ok(());
        }
        for call in calls {
            let update = match call.completion {
                Some(update) => update,
                // Still running here: it completes into the session's
                // detached work, which the attached runtime collects.
                None if super::tool_execution::detached_call_is_live(&call.task_id) => continue,
                None => {
                    let update = super::tool_execution::interrupted_detached_update(
                        call.task_id,
                        call.tool_call_id,
                        &call.tool,
                    );
                    self.agent.session_store_record(
                        &session_id,
                        AgentSessionStoreRecordKind::DetachedCallCompleted {
                            update: update.clone(),
                        },
                    )?;
                    update
                }
            };
            if !state
                .async_updates
                .iter()
                .any(|queued| queued.task_id == update.task_id)
            {
                state.async_updates.push(update);
            }
        }
        state.collect_detached_completions();
        Ok(())
    }

    /// Stop the session's detached tool calls.
    pub fn cancel_detached_tools(&self) {
        if let Ok(mut state) = self.state.lock() {
            // Existing calls retain the cancelled token; later turns get a
            // fresh scope when this reusable session starts more work.
            std::mem::take(&mut state.detached_cancellation).cancel();
        }
    }

    pub fn runtime_events_since(
        &self,
        cursor: AgentRuntimeEventCursor,
    ) -> Result<AgentRuntimeEventBatch, AgentError> {
        Ok(self.lock_state()?.runtime_events_since(cursor))
    }

    pub fn terminal_result(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<AgentTaskTerminalResult>, AgentError> {
        Ok(self.lock_state()?.control.terminal_result(task_id))
    }

    pub fn turn_result(&self, op_id: &TaskId) -> Result<Option<AgentOpOutcome>, AgentError> {
        Ok(self
            .lock_state()?
            .completed_turns
            .iter()
            .find(|outcome| &outcome.op_id == op_id)
            .cloned())
    }

    pub(super) fn submit_turn_op(
        &self,
        op: AgentOp,
    ) -> Result<AgentSessionControlResult, AgentError> {
        self.submit_turn_op_with_cancellation(op, None)
    }

    /// Bind a turn to the cancellation scope that admitted its input. This
    /// also covers preparation that completes after its caller requested Stop.
    pub fn submit_op_with_cancellation(
        &self,
        op: AgentOp,
        cancellation: AgentCancellationToken,
    ) -> Result<AgentSessionControlResult, AgentError> {
        if !op.action.is_turn() {
            return Err(AgentError::Runtime {
                detail: "an admission cancellation scope requires a turn operation".into(),
            });
        }
        self.submit_turn_op_with_cancellation(op, Some(cancellation))
    }

    fn submit_turn_op_with_cancellation(
        &self,
        op: AgentOp,
        admission: Option<AgentCancellationToken>,
    ) -> Result<AgentSessionControlResult, AgentError> {
        self.reap_finished_worker()?;
        let mut state = self.lock_state()?;
        let previous_control = state.control.clone();
        let previous_in_flight_input_ids = state.in_flight_input_ids.clone();
        let previous_runtime_event_count = state.runtime_events.len();
        let sequence = state.control.next_sequence;
        state.control.next_sequence = state.control.next_sequence.saturating_add(1);
        let op_id = op.op_id.clone();
        let action = op.action.kind().to_string();
        let timeout_ms = op.timeout_ms;
        // A continuation delivers a detached call's completion; it is marked
        // delivered once the session that holds it is saved.
        let delivered_task = match &op.action {
            AgentOpAction::StartTurn { input } | AgentOpAction::ContinueTurn { input } => input
                .async_update
                .as_ref()
                .map(|update| update.task_id.clone()),
            _ => None,
        };
        let mut events = vec![agent_session_event(
            &op_id,
            &state.control.session.session_id,
            AgentEventPayload::OperationQueued {
                sequence,
                action: action.clone(),
                timeout_ms,
            },
        )];

        let busy = state.control.active_turn.is_some() || state.turn_worker_running;
        if busy || state.control.session.is_paused() {
            events.push(agent_session_event(
                &op_id,
                &state.control.session.session_id,
                AgentEventPayload::OperationRejected {
                    sequence,
                    action,
                    reason: if busy {
                        "session already has an active turn worker".to_string()
                    } else {
                        "session is paused; unpause it to run another turn".to_string()
                    },
                },
            ));
            state.control.events.extend(events.iter().cloned());
            state.append_control_runtime_events(&events);
            let result = AgentSessionControlResult {
                sequence,
                accepted: false,
                events,
                outcome: None,
                snapshot: state.control.snapshot(),
            };
            if let Err(error) = self.agent.persist_control_result(&result) {
                state.control = previous_control;
                state.in_flight_input_ids = previous_in_flight_input_ids;
                state.runtime_events.truncate(previous_runtime_event_count);
                return Err(error);
            }
            return Ok(result);
        }

        // The continuation consumes its completion. A copy restored from the
        // session's records (a rebuilt runtime restores those not yet
        // delivered) must not continue the session again.
        if let Some(task_id) = &delivered_task {
            state.collect_detached_completions();
            state
                .async_updates
                .retain(|update| &update.task_id != task_id);
        }
        let task = self.agent.config.run_context.task.clone();
        let cancellation_token = admission
            .as_ref()
            .map(AgentCancellationToken::child_token)
            .unwrap_or_default()
            .with_timeout_ms(timeout_ms);
        state.control.active_turn = Some(AgentActiveTurn::new(
            op_id.clone(),
            sequence,
            timeout_ms,
            task.as_ref().map(|task| task.task_id.clone()),
        ));
        state.control.active_turn_cancellation = Some(cancellation_token.clone());
        if let Some(task) = task.clone() {
            let event = state.control.set_task_status(
                &op_id,
                task,
                AgentTaskLifecycleStatus::Running,
                Some("turn running".to_string()),
                Some(op_id.clone()),
                None,
                Vec::new(),
            );
            events.push(event);
        }
        let session_for_turn = state.control.session.clone();
        let base_state_revision = session_for_turn.state_revision;
        state.in_flight_input_ids = session_for_turn
            .steering_queue
            .iter()
            .map(|input| input.steering_id.clone())
            .chain(
                session_for_turn
                    .follow_up_queue
                    .iter()
                    .map(|input| input.follow_up_id.clone()),
            )
            .collect();
        state.control.events.extend(events.iter().cloned());
        state.append_control_runtime_events(&events);
        state.push_runtime_event(AgentRuntimeEventPayload::WorkerStarted {
            sequence,
            op_id: op_id.clone(),
            action: action.clone(),
            timeout_ms,
        });
        let result = AgentSessionControlResult {
            sequence,
            accepted: true,
            events,
            outcome: None,
            snapshot: state.control.snapshot(),
        };
        if let Err(error) = self.agent.persist_control_result(&result) {
            // A turn must never begin if its queued/running state cannot be
            // made durable. Restore the complete pre-submit control view and
            // leave no worker behind for the caller to discover later.
            state.control = previous_control;
            state.in_flight_input_ids = previous_in_flight_input_ids;
            state.runtime_events.truncate(previous_runtime_event_count);
            return Err(error);
        }
        state.turn_worker_running = true;
        self.subagent_scope.manager.set_session(&session_for_turn);

        let worker_state = Arc::clone(&self.state);
        let progress_state = Arc::clone(&self.state);
        let model_progress_state = Arc::clone(&self.state);
        let worker_agent = Arc::clone(&self.agent);
        let worker_op_id = op_id.clone();
        let persist_worker_op_id = worker_op_id.clone();
        let progress_op_id = op_id.clone();
        let model_progress_op_id = op_id.clone();
        let worker_action = action;
        let persist_worker_action = worker_action.clone();
        let worker_task = task;
        let detached_cancellation = state.detached_cancellation.clone();
        let detached_agent = Arc::clone(&self.agent);
        let detached_session_id = session_for_turn.session_id.clone();
        let progress_sink = AgentToolProgressSink::new(move |event| {
            if let Ok(mut state) = progress_state.lock() {
                // Detached calls outlive the turn, the runtime, and possibly
                // the process: their lifecycle is recorded in the session
                // that started them, and their completion goes to whichever
                // runtime is attached to that session.
                match &event {
                    AgentToolProgressEvent::DetachedStarted {
                        task_id,
                        tool_call_id,
                        tool,
                        ..
                    } => {
                        record_detached(
                            &detached_agent,
                            &detached_session_id,
                            AgentSessionStoreRecordKind::DetachedCallStarted {
                                task_id: task_id.clone(),
                                tool_call_id: tool_call_id.clone(),
                                tool: tool.clone(),
                            },
                        );
                        detached_call_started(&detached_session_id);
                    }
                    AgentToolProgressEvent::DetachedCompleted { update } => {
                        record_detached(
                            &detached_agent,
                            &detached_session_id,
                            AgentSessionStoreRecordKind::DetachedCallCompleted {
                                update: update.clone(),
                            },
                        );
                        detached_call_completed(&detached_session_id, update.clone());
                    }
                    _ => {}
                }
                state.push_runtime_event(AgentRuntimeEventPayload::ToolProgress {
                    sequence,
                    op_id: progress_op_id.clone(),
                    event,
                });
            }
        })
        .with_detached_tools(detached_cancellation);
        let model_progress_sink = AgentModelProgressSink::new(move |event| {
            if let Ok(mut state) = model_progress_state.lock() {
                state.push_runtime_event(AgentRuntimeEventPayload::ModelProgress {
                    sequence,
                    op_id: model_progress_op_id.clone(),
                    event,
                });
            }
        });
        let deadline_probe = cancellation_token.clone();
        let worker = thread::spawn(move || -> Result<(), AgentError> {
            let mut completed_session = session_for_turn;
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                worker_agent.handle_op_with_cancellation_and_progress_and_persistence(
                    &mut completed_session,
                    op,
                    cancellation_token,
                    progress_sink,
                    model_progress_sink,
                    false,
                )
            }))
            .unwrap_or_else(|_| {
                Err(AgentError::Runtime {
                    detail: "agent session runtime worker panicked".to_string(),
                })
            });
            let mut state = worker_state
                .lock()
                .expect("agent session runtime lock poisoned");
            let completed_revision_changed =
                outcome.is_ok() || completed_session.state_revision != base_state_revision;
            let session_persistence = if completed_revision_changed {
                let in_flight = std::mem::take(&mut state.in_flight_input_ids);
                state.replace_session_preserving_live_inputs(
                    completed_session,
                    base_state_revision,
                    &in_flight,
                );
                worker_agent.save_session(&state.control.session)
            } else {
                Ok(())
            };
            let outcome = match &session_persistence {
                Ok(()) => outcome,
                Err(error) => Err(AgentError::SessionStore {
                    detail: format!("persist merged completed agent session failed: {error}"),
                }),
            };
            if session_persistence.is_ok()
                && completed_revision_changed
                && let Some(task_id) = delivered_task
            {
                record_detached(
                    &worker_agent,
                    &state.control.session.session_id.clone(),
                    AgentSessionStoreRecordKind::DetachedCallDelivered { task_id },
                );
            }
            let control_result = state.finish_turn(AgentSessionRuntimeTurnCompletion {
                sequence,
                op_id: worker_op_id,
                action: worker_action,
                task: worker_task,
                timeout_ms,
                deadline_expired: deadline_probe.deadline_expired(),
                outcome: &outcome,
            });
            let control_persistence = worker_agent.persist_control_result(&control_result);
            if let Err(error) = &control_persistence {
                state.push_runtime_event(AgentRuntimeEventPayload::WorkerFailed {
                    sequence,
                    op_id: persist_worker_op_id.clone(),
                    action: persist_worker_action.clone(),
                    detail: format!("persist agent control history failed: {error}"),
                });
            }
            drop(state);
            session_persistence?;
            control_persistence?;
            outcome.map(|_| ())
        });
        state.worker = Some(worker);
        Ok(result)
    }

    pub(super) fn reap_finished_worker(&self) -> Result<(), AgentError> {
        let worker = {
            let mut state = self.lock_state()?;
            if state
                .worker
                .as_ref()
                .is_some_and(|worker| worker.is_finished())
            {
                state.worker.take()
            } else {
                None
            }
        };
        if let Some(worker) = worker {
            // A returned failure belongs to the previous operation and has
            // already been published in its runtime events. Reaping it must
            // not reject an unrelated new turn. Explicit joins still return
            // the operation error; unexpected thread panics remain errors.
            let _reported_outcome = worker.join().map_err(|_| AgentError::Runtime {
                detail: "agent session runtime worker panicked".to_string(),
            })?;
        }
        Ok(())
    }

    pub(super) fn lock_state(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, AgentSessionRuntimeState>, AgentError> {
        self.state.lock().map_err(|_| AgentError::Runtime {
            detail: "agent session runtime lock poisoned".to_string(),
        })
    }
}

pub(super) struct AgentSessionRuntimeTurnCompletion<'a> {
    pub(super) sequence: u64,
    pub(super) op_id: TaskId,
    pub(super) action: String,
    pub(super) task: Option<AgentTaskRef>,
    pub(super) timeout_ms: Option<u64>,
    pub(super) deadline_expired: bool,
    pub(super) outcome: &'a Result<AgentOpOutcome, AgentError>,
}

impl AgentSessionRuntimeState {
    /// Queue the completions of the attached session's detached calls,
    /// whichever runtime started them.
    fn collect_detached_completions(&mut self) {
        for update in take_detached_completions(&self.control.session.session_id) {
            if !self
                .async_updates
                .iter()
                .any(|queued| queued.task_id == update.task_id)
            {
                self.async_updates.push(update);
            }
        }
    }

    pub(super) fn runtime_event_cursor(&self) -> AgentRuntimeEventCursor {
        AgentRuntimeEventCursor {
            offset: self.runtime_events.len(),
            last_event_id: self
                .runtime_events
                .last()
                .map(|event| event.event_id.clone()),
        }
    }

    pub(super) fn runtime_events_since(
        &self,
        cursor: AgentRuntimeEventCursor,
    ) -> AgentRuntimeEventBatch {
        let start = cursor.offset.min(self.runtime_events.len());
        let events = self.runtime_events[start..].to_vec();
        AgentRuntimeEventBatch {
            cursor,
            next_cursor: self.runtime_event_cursor(),
            events,
        }
    }

    pub(super) fn push_runtime_event(&mut self, payload: AgentRuntimeEventPayload) {
        self.runtime_events.push(AgentRuntimeEvent::new(
            self.control.session.session_id.clone(),
            payload,
        ));
    }

    pub(super) fn append_control_runtime_events(&mut self, events: &[AgentEvent]) {
        self.runtime_events
            .extend(events.iter().cloned().map(|event| {
                AgentRuntimeEvent::new(
                    event.session_id.clone(),
                    AgentRuntimeEventPayload::ControlEvent { event },
                )
            }));
    }

    pub(super) fn finish_turn(
        &mut self,
        completion: AgentSessionRuntimeTurnCompletion<'_>,
    ) -> AgentSessionControlResult {
        self.turn_worker_running = false;
        self.control.active_turn = None;
        self.control.active_turn_cancellation = None;
        let sequence = completion.sequence;
        let mut events = Vec::new();
        // An accepted cancel already published this turn's terminal state.
        // Whatever the worker returns afterwards is the cancelled turn
        // winding down, not a new failure or completion.
        let cancel_accepted = self
            .control
            .cancelled_turn_sequence
            .take_if(|cancelled| *cancelled == sequence)
            .is_some();
        let timed_out = !cancel_accepted && completion.deadline_expired;
        if timed_out && let Some(timeout_ms) = completion.timeout_ms {
            events.push(agent_session_event(
                &completion.op_id,
                &self.control.session.session_id,
                AgentEventPayload::TurnTimedOut {
                    sequence,
                    active_op_id: completion.op_id.clone(),
                    timeout_ms,
                    reason: "turn exceeded configured timeout".to_string(),
                },
            ));
        }
        match completion.outcome {
            Ok(outcome) => {
                events.extend(outcome.events.iter().cloned());
                let stop_reason = outcome
                    .run
                    .as_ref()
                    .map(|run| run.stop_reason.clone())
                    .unwrap_or(AgentStopReason::Aborted);
                if let Some(run) = &outcome.run {
                    self.push_runtime_event(AgentRuntimeEventPayload::TurnFinalDecision {
                        sequence: completion.sequence,
                        op_id: completion.op_id.clone(),
                        decision: run.final_decision.clone(),
                        stop_reason: stop_reason.clone(),
                    });
                }
                if let Some(task) = completion.task
                    && !cancel_accepted
                {
                    self.append_terminal_task_status(
                        &mut events,
                        &completion.op_id,
                        timed_out,
                        task,
                        outcome,
                    );
                }
                self.completed_turns.push(outcome.clone());
                self.push_runtime_event(AgentRuntimeEventPayload::WorkerCompleted {
                    sequence: completion.sequence,
                    op_id: completion.op_id.clone(),
                    action: completion.action.clone(),
                    stop_reason,
                });
            }
            Err(_) if cancel_accepted => {
                self.in_flight_input_ids.clear();
                self.push_runtime_event(AgentRuntimeEventPayload::WorkerCompleted {
                    sequence: completion.sequence,
                    op_id: completion.op_id.clone(),
                    action: completion.action.clone(),
                    stop_reason: AgentStopReason::Cancelled,
                });
            }
            Err(error) => {
                self.in_flight_input_ids.clear();
                let detail = error.to_string();
                events.push(agent_session_event(
                    &completion.op_id,
                    &self.control.session.session_id,
                    AgentEventPayload::OperationRejected {
                        sequence: completion.sequence,
                        action: completion.action.clone(),
                        reason: format!("turn execution failed: {detail}"),
                    },
                ));
                if let Some(task) = completion.task {
                    events.push(self.control.set_task_status(
                        &completion.op_id,
                        task,
                        if timed_out {
                            AgentTaskLifecycleStatus::TimedOut
                        } else {
                            AgentTaskLifecycleStatus::Failed
                        },
                        Some(detail.clone()),
                        None,
                        None,
                        Vec::new(),
                    ));
                }
                self.push_runtime_event(AgentRuntimeEventPayload::WorkerFailed {
                    sequence: completion.sequence,
                    op_id: completion.op_id.clone(),
                    action: completion.action.clone(),
                    detail,
                });
            }
        }
        self.control.events.extend(events.iter().cloned());
        self.append_control_runtime_events(&events);
        AgentSessionControlResult {
            sequence,
            accepted: true,
            events,
            outcome: None,
            snapshot: self.control.snapshot(),
        }
    }

    pub(super) fn replace_session_preserving_live_inputs(
        &mut self,
        mut completed_session: AgentSession,
        base_state_revision: u64,
        in_flight_input_ids: &BTreeSet<TaskId>,
    ) {
        let live_session = &mut self.control.session;
        let live_state_revision = live_session.state_revision;
        // A failure before execution leaves captured inputs queued. Once
        // recorded in a partial or complete turn, they are consumed exactly
        // as on success. New inputs still belong to the live control plane.
        let unconsumed_inputs: BTreeSet<_> = completed_session
            .steering_queue
            .iter()
            .map(|input| &input.steering_id)
            .chain(
                completed_session
                    .follow_up_queue
                    .iter()
                    .map(|input| &input.follow_up_id),
            )
            .collect();
        let consumed =
            |id: &TaskId| in_flight_input_ids.contains(id) && !unconsumed_inputs.contains(id);
        let queued_steering = std::mem::take(&mut live_session.steering_queue)
            .into_iter()
            .filter(|input| !consumed(&input.steering_id))
            .collect();
        let queued_follow_ups = std::mem::take(&mut live_session.follow_up_queue)
            .into_iter()
            .filter(|input| !consumed(&input.follow_up_id))
            .collect();
        let live_abort = live_session.abort.clone();
        // Pause and unpause are live control edits made while the turn ran.
        let live_pause = live_session.pause.clone();
        completed_session.steering_queue = queued_steering;
        completed_session.follow_up_queue = queued_follow_ups;
        if live_abort.is_some() {
            completed_session.abort = live_abort;
        }
        completed_session.pause = live_pause;
        // Lifecycle metadata is host-owned and can be archived during a turn.
        // A late turn snapshot must not resurrect a discarded child.
        completed_session.subagent = live_session.subagent.clone();
        if live_state_revision > base_state_revision {
            completed_session.state_revision = completed_session
                .state_revision
                .max(live_state_revision)
                .saturating_add(1);
        }
        self.control.session = completed_session;
    }

    pub(super) fn append_terminal_task_status(
        &mut self,
        events: &mut Vec<AgentEvent>,
        op_id: &TaskId,
        timed_out: bool,
        task: AgentTaskRef,
        outcome: &AgentOpOutcome,
    ) {
        let status = if timed_out {
            AgentTaskLifecycleStatus::TimedOut
        } else {
            task_status_for_run(&outcome.run)
        };
        let progress_summary = outcome.run.as_ref().map(|run| {
            format!(
                "turn completed with {:?} after {} model turn(s) and {} tool result(s)",
                run.stop_reason,
                run.model_turns.len(),
                run.tool_results.len()
            )
        });
        let result_ref = outcome
            .run
            .as_ref()
            .and_then(|run| run.candidate_evidence_bundle.as_ref())
            .map(|bundle| bundle.bundle_id.as_str().to_string());
        let event_ids = outcome
            .run
            .as_ref()
            .map(|run| {
                run.events
                    .iter()
                    .map(|event| event.event_id.clone())
                    .collect()
            })
            .unwrap_or_default();
        events.push(self.control.set_task_status(
            op_id,
            task,
            status,
            progress_summary,
            None,
            result_ref,
            event_ids,
        ));
    }
}

pub(super) fn join_agent_session_worker(
    worker: thread::JoinHandle<Result<(), AgentError>>,
) -> Result<(), AgentError> {
    worker.join().map_err(|_| AgentError::Runtime {
        detail: "agent session runtime worker panicked".to_string(),
    })?
}

impl AgentTaskTerminalResult {
    pub fn from_snapshot(snapshot: &AgentTaskSnapshot) -> Option<Self> {
        if !is_terminal_task_status(&snapshot.status) {
            return None;
        }
        Some(Self {
            task: snapshot.task.clone(),
            status: snapshot.status.clone(),
            progress_summary: snapshot.progress_summary.clone(),
            result_ref: snapshot.result_ref.clone(),
            event_ids: snapshot.event_ids.clone(),
            updated_at: snapshot.updated_at,
        })
    }
}

pub(super) fn terminal_results_from_tasks(
    tasks: &[AgentTaskSnapshot],
) -> Vec<AgentTaskTerminalResult> {
    tasks
        .iter()
        .filter_map(AgentTaskTerminalResult::from_snapshot)
        .collect()
}

pub(super) fn is_terminal_task_status(status: &AgentTaskLifecycleStatus) -> bool {
    matches!(
        status,
        AgentTaskLifecycleStatus::Completed
            | AgentTaskLifecycleStatus::Failed
            | AgentTaskLifecycleStatus::Cancelled
            | AgentTaskLifecycleStatus::TimedOut
    )
}

pub(super) fn append_control_event_to_store(
    store: &dyn AgentSessionStore,
    event: &AgentEvent,
) -> Result<(), AgentSessionStoreError> {
    store.append_record(&AgentSessionStoreRecord::new(
        event.session_id.clone(),
        AgentSessionStoreRecordKind::ControlEventPublished {
            event: event.clone(),
        },
    ))?;
    if let AgentEventPayload::TaskStatusChanged { snapshot } = &event.payload {
        store.append_record(&AgentSessionStoreRecord::new(
            event.session_id.clone(),
            AgentSessionStoreRecordKind::TaskStatusChanged {
                snapshot: snapshot.clone(),
            },
        ))?;
    }
    Ok(())
}

pub(super) fn append_control_snapshot_to_store(
    store: &dyn AgentSessionStore,
    snapshot: &AgentSessionControlSnapshot,
) -> Result<(), AgentSessionStoreError> {
    store.append_record(&AgentSessionStoreRecord::new(
        snapshot.session_id.clone(),
        AgentSessionStoreRecordKind::ControlSnapshotSaved {
            snapshot: snapshot.clone(),
        },
    ))
}

#[derive(Debug, thiserror::Error, Clone, Eq, PartialEq)]
pub enum AgentSessionControlRestoreError {
    #[error("session control snapshot belongs to {actual}, expected {expected}")]
    SessionMismatch {
        expected: SessionId,
        actual: SessionId,
    },
}

pub(super) fn task_status_for_run(run: &Option<AgentRun>) -> AgentTaskLifecycleStatus {
    match run.as_ref().map(|run| &run.stop_reason) {
        Some(AgentStopReason::Aborted | AgentStopReason::Cancelled) => {
            AgentTaskLifecycleStatus::Cancelled
        }
        Some(AgentStopReason::MaxStepsReached) => AgentTaskLifecycleStatus::Failed,
        Some(AgentStopReason::Paused) => AgentTaskLifecycleStatus::Paused,
        Some(
            AgentStopReason::FinalDecision
            | AgentStopReason::SilentAsyncUpdate
            | AgentStopReason::CodingStopRecommended,
        ) => AgentTaskLifecycleStatus::Completed,
        None => AgentTaskLifecycleStatus::Failed,
    }
}

/// Detached calls in this process, per session. A host can rebuild a
/// session's runtime (for a model, agent, or workspace change) while its
/// detached calls run, so their count and completions belong to the session
/// rather than to the runtime that started them. The session's records stay
/// the durable source; this carries only what happens in this process.
#[derive(Default)]
struct DetachedSessionWork {
    running: usize,
    completions: Vec<crate::AsyncTaskUpdate>,
}

static DETACHED_WORK: std::sync::LazyLock<
    Mutex<std::collections::HashMap<SessionId, DetachedSessionWork>>,
> = std::sync::LazyLock::new(Default::default);

fn with_detached_work<T>(
    update: impl FnOnce(&mut std::collections::HashMap<SessionId, DetachedSessionWork>) -> T,
) -> T {
    let mut work = DETACHED_WORK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    update(&mut work)
}

fn detached_call_started(session_id: &SessionId) {
    with_detached_work(|work| work.entry(session_id.clone()).or_default().running += 1);
}

fn detached_call_completed(session_id: &SessionId, update: crate::AsyncTaskUpdate) {
    with_detached_work(|work| {
        let session = work.entry(session_id.clone()).or_default();
        session.running = session.running.saturating_sub(1);
        session.completions.push(update);
    });
}

/// Deliver host-originated session updates without changing the count of
/// independently executing detached tool calls.
pub(super) fn queue_session_update(session_id: &SessionId, update: crate::AsyncTaskUpdate) {
    with_detached_work(|work| {
        work.entry(session_id.clone())
            .or_default()
            .completions
            .push(update)
    });
}

pub(super) fn detached_calls_running(session_id: &SessionId) -> usize {
    with_detached_work(|work| work.get(session_id).map_or(0, |session| session.running))
}

fn take_detached_completions(session_id: &SessionId) -> Vec<crate::AsyncTaskUpdate> {
    with_detached_work(|work| {
        let Some(session) = work.get_mut(session_id) else {
            return Vec::new();
        };
        let completions = std::mem::take(&mut session.completions);
        if session.running == 0 {
            work.remove(session_id);
        }
        completions
    })
}

/// Best effort: a failed record leaves the call to the next attach's
/// recovery rather than failing the tool or the turn that reported it.
fn record_detached<M: InteractionModel>(
    agent: &Agent<M>,
    session_id: &SessionId,
    record: AgentSessionStoreRecordKind,
) {
    if let Err(error) = agent.session_store_record(session_id, record) {
        tracing::warn!(%session_id, %error, "record detached tool call failed");
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;
    use crate::{
        Agent, AgentRole, AgentRuntimeControlRegistry, AsyncTaskNextAction, AsyncTaskStatus,
        AsyncTaskUpdate,
    };

    struct NoModelCalls;
    impl InteractionModel for NoModelCalls {
        fn complete(
            &self,
            _: &sinomo_lm::LanguageModelRequest,
        ) -> Result<sinomo_lm::LanguageModelResponse, sinomo_lm::LanguageModelError> {
            panic!("shutdown must not start a model turn");
        }
    }

    #[test]
    fn session_notifications_do_not_complete_unrelated_detached_tools() {
        let id = SessionId::generate();
        let update = AsyncTaskUpdate {
            kind: "agent.subagent.completed".into(),
            task_id: TaskId::generate(),
            tool_call_id: None,
            status: AsyncTaskStatus::Completed,
            summary: "child finished".into(),
            needs_user_attention: false,
            result_ref: None,
            next_action_hint: AsyncTaskNextAction::InspectResult,
        };
        detached_call_started(&id);
        queue_session_update(&id, update.clone());
        assert_eq!(detached_calls_running(&id), 1);
        assert_eq!(take_detached_completions(&id), vec![update.clone()]);
        assert_eq!(detached_calls_running(&id), 1);
        detached_call_completed(&id, update.clone());
        assert_eq!(detached_calls_running(&id), 0);
        assert_eq!(take_detached_completions(&id), vec![update]);
    }

    #[test]
    fn shutdown_live_work_query_preserves_idle_and_completed_continuations() {
        let session_id = SessionId::generate();
        let runtime = AgentSessionRuntime::new(
            Agent::new(NoModelCalls, "fixture", "fixture"),
            AgentSession::new(session_id.clone(), AgentRole::Main),
        );
        let registry = AgentRuntimeControlRegistry::new();
        registry.register_runtime(runtime.clone()).unwrap();
        let before = runtime.session_snapshot().unwrap();
        registry.cancel_live_work("shutdown idle fixture").unwrap();
        assert_eq!(runtime.session_snapshot().unwrap(), before);
        let update = AsyncTaskUpdate {
            kind: "completed-tool".into(),
            task_id: TaskId::generate(),
            tool_call_id: Some(TaskId::generate()),
            status: AsyncTaskStatus::Completed,
            summary: "stored completion still needs its original continuation".into(),
            needs_user_attention: false,
            result_ref: Some("original-result".into()),
            next_action_hint: AsyncTaskNextAction::InspectResult,
        };
        runtime
            .lock_state()
            .unwrap()
            .async_updates
            .push(update.clone());
        assert!(
            runtime.has_running_work().unwrap(),
            "ordinary idle policy must still see pending work"
        );
        assert!(
            !runtime.has_live_work().unwrap(),
            "a completed result must not force a model turn during shutdown"
        );
        registry
            .cancel_live_work("shutdown completed fixture")
            .unwrap();
        assert_eq!(runtime.session_snapshot().unwrap(), before);
        assert_eq!(
            runtime.take_async_updates(),
            vec![update],
            "original task/call/result identities were retained"
        );
        runtime.lock_state().unwrap().turn_worker_running = true;
        assert!(
            runtime.has_live_work().unwrap(),
            "a live worker cannot disappear from shutdown accounting"
        );
        runtime.lock_state().unwrap().turn_worker_running = false;
    }
}
