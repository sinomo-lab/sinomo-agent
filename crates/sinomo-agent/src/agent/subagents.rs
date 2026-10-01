//! Temporary child execution with inherited access and parent-owned lifetimes.

use super::*;
use crate::{AgentToolKind, AgentToolRequest, AgentToolResult, AgentToolSpec};
use std::sync::{Condvar, Weak};
use std::time::{Duration, Instant};

const MAX_WAIT_MS: u64 = 60_000;
const MAX_RESULT_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentSubagentStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSubagentSnapshot {
    pub id: TaskId,
    pub parent_session_id: SessionId,
    pub session_id: SessionId,
    pub name: String,
    pub task: String,
    pub status: AgentSubagentStatus,
    pub cancel_requested: bool,
    pub result: Option<String>,
    pub result_truncated: bool,
    pub stop_reason: Option<AgentStopReason>,
    pub error: Option<String>,
}

struct Child {
    snapshot: Mutex<AgentSubagentSnapshot>,
    changed: Condvar,
    cancellation: AgentCancellationToken,
    session: AgentSession,
}

#[derive(Clone)]
struct Parent {
    session: AgentSession,
    input: Option<AgentLoopInput>,
    owners: usize,
}

#[derive(Default)]
struct SubagentState {
    parents: BTreeMap<SessionId, Parent>,
    children: BTreeMap<TaskId, Arc<Child>>,
}

type ChildWork = dyn FnOnce(AgentCancellationToken) -> Result<AgentRun, AgentError> + Send;
type ChildFactory =
    dyn Fn(AgentSession, AgentLoopInput) -> Result<Box<ChildWork>, AgentError> + Send + Sync;

/// Live child handles are scoped to their parent session. They are never agent
/// catalog entries, background handoffs, or resumable process handles.
pub struct AgentSubagents {
    state: Mutex<SubagentState>,
    factory: Arc<ChildFactory>,
}

impl AgentSubagents {
    fn lock(&self) -> std::sync::MutexGuard<'_, SubagentState> {
        self.state.lock().expect("subagents lock poisoned")
    }

    pub(super) fn set_session(&self, session: &AgentSession) {
        if let Some(parent) = self.lock().parents.get_mut(&session.session_id) {
            parent.session = parent_context_session(session);
        }
    }

    pub(super) fn set_input(&self, input: &AgentLoopInput) {
        if let Some(parent) = self.lock().parents.get_mut(&input.session.session_id) {
            parent.input = Some(input.clone());
        }
    }

    pub fn spawn(
        &self,
        parent_session_id: &SessionId,
        name: &str,
        task: &str,
        cancellation: &AgentCancellationToken,
    ) -> Result<AgentSubagentSnapshot, AgentError> {
        if task.trim().is_empty() {
            return Err(subagent_error("subagent task must not be empty"));
        }
        if cancellation.is_cancelled() {
            return Err(subagent_error("parent operation is cancelled"));
        }
        let mut state = self.lock();
        let parent = state
            .parents
            .get(parent_session_id)
            .ok_or_else(|| subagent_error("parent session is no longer live"))?;
        if parent.session.is_aborted() {
            return Err(subagent_error("parent session is aborted"));
        }
        let mut input = parent
            .input
            .clone()
            .ok_or_else(|| subagent_error("parent session has no turn context"))?;
        let session_id = SessionId::generate();
        let session = inherited_session(&parent.session, session_id.clone());
        let id = TaskId::generate();
        let snapshot = AgentSubagentSnapshot {
            id: id.clone(),
            parent_session_id: parent_session_id.clone(),
            session_id: session_id.clone(),
            name: name.to_string(),
            task: task.to_string(),
            status: AgentSubagentStatus::Running,
            cancel_requested: false,
            result: None,
            result_truncated: false,
            stop_reason: None,
            error: None,
        };
        input.session.session_id = session_id;
        let mut event = crate::user_interaction_input_event(&input.session, task);
        event
            .links
            .caused_by
            .push(input.latest_event.event_id.clone());
        event.payload["subagent"] = serde_json::json!({
            "id": id, "name": name, "parent_session_id": parent_session_id,
        });
        input.latest_event = event;
        // Surface participation instructions belong to the parent's turn.
        // Child task text stays user input, with its resources in attachments.
        input.transient_system_context = None;
        input.async_update = None;
        let child = Arc::new(Child {
            snapshot: Mutex::new(snapshot.clone()),
            changed: Condvar::new(),
            cancellation: cancellation.child_token(),
            session: session.clone(),
        });
        let worker_child = Arc::clone(&child);
        let work = (self.factory)(session, input)?;
        // Hold the parent admission lock until the worker is published, so
        // parent disposal cannot miss a newly launched child.
        let worker = thread::Builder::new()
            .name(format!("subagent-{id}"))
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    work(worker_child.cancellation.clone())
                }))
                .unwrap_or_else(|_| Err(subagent_error("subagent worker panicked")));
                let mut snapshot = worker_child
                    .snapshot
                    .lock()
                    .expect("subagent snapshot lock poisoned");
                if worker_child.cancellation.is_cancelled() {
                    snapshot.cancel_requested = true;
                    snapshot.status = AgentSubagentStatus::Cancelled;
                } else {
                    match &result {
                        Ok(run)
                            if matches!(
                                run.stop_reason,
                                AgentStopReason::Cancelled | AgentStopReason::Aborted
                            ) =>
                        {
                            snapshot.status = AgentSubagentStatus::Cancelled;
                        }
                        Ok(run) if run.stop_reason == AgentStopReason::MaxStepsReached => {
                            snapshot.status = AgentSubagentStatus::Failed;
                            snapshot.error =
                                Some("subagent reached its inherited step limit".to_string());
                        }
                        Ok(_) => snapshot.status = AgentSubagentStatus::Completed,
                        Err(_) => snapshot.status = AgentSubagentStatus::Failed,
                    }
                }
                match result {
                    Ok(run) => {
                        let mut text = run.final_decision.text;
                        if text.len() > MAX_RESULT_BYTES {
                            let mut end = MAX_RESULT_BYTES;
                            while !text.is_char_boundary(end) {
                                end -= 1;
                            }
                            text.truncate(end);
                            snapshot.result_truncated = true;
                        }
                        snapshot.result = Some(text);
                        snapshot.stop_reason = Some(run.stop_reason);
                    }
                    Err(error) => snapshot.error = Some(error.to_string()),
                }
                worker_child.changed.notify_all();
            })
            .map_err(|error| subagent_error(format!("could not start subagent: {error}")))?;
        // The worker owns execution resources until it really exits. Disposal
        // signals cooperative cancellation; it never claims a forced stop.
        drop(worker);
        state.children.insert(id, child);
        Ok(snapshot)
    }

    fn child(&self, parent_session_id: &SessionId, id: &TaskId) -> Result<Arc<Child>, AgentError> {
        let state = self.lock();
        let child = state
            .children
            .get(id)
            .filter(|child| {
                child
                    .snapshot
                    .lock()
                    .expect("subagent snapshot lock poisoned")
                    .parent_session_id
                    == *parent_session_id
            })
            .ok_or_else(|| subagent_error("subagent does not belong to this parent session"))?;
        Ok(Arc::clone(child))
    }

    pub fn inspect(
        &self,
        parent_session_id: &SessionId,
        id: &TaskId,
    ) -> Result<AgentSubagentSnapshot, AgentError> {
        Ok(self
            .child(parent_session_id, id)?
            .snapshot
            .lock()
            .expect("subagent snapshot lock poisoned")
            .clone())
    }

    pub fn session_snapshot(
        &self,
        parent_session_id: &SessionId,
        id: &TaskId,
    ) -> Result<AgentSession, AgentError> {
        Ok(self.child(parent_session_id, id)?.session.clone())
    }

    pub fn list(&self, parent_session_id: &SessionId) -> Vec<AgentSubagentSnapshot> {
        self.lock()
            .children
            .values()
            .filter_map(|child| {
                let snapshot = child
                    .snapshot
                    .lock()
                    .expect("subagent snapshot lock poisoned");
                (snapshot.parent_session_id == *parent_session_id).then(|| snapshot.clone())
            })
            .collect()
    }

    pub fn wait(
        &self,
        parent_session_id: &SessionId,
        id: &TaskId,
        timeout_ms: u64,
        cancellation: &AgentCancellationToken,
    ) -> Result<AgentSubagentSnapshot, AgentError> {
        let child = self.child(parent_session_id, id)?;
        let deadline = Instant::now() + Duration::from_millis(timeout_ms.min(MAX_WAIT_MS));
        let mut snapshot = child
            .snapshot
            .lock()
            .expect("subagent snapshot lock poisoned");
        while snapshot.status == AgentSubagentStatus::Running && !cancellation.is_cancelled() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            snapshot = child
                .changed
                .wait_timeout(snapshot, remaining.min(Duration::from_millis(25)))
                .expect("subagent snapshot lock poisoned")
                .0;
        }
        Ok(snapshot.clone())
    }

    pub fn cancel(
        &self,
        parent_session_id: &SessionId,
        id: &TaskId,
    ) -> Result<AgentSubagentSnapshot, AgentError> {
        let child = self.child(parent_session_id, id)?;
        child.cancellation.cancel();
        child
            .snapshot
            .lock()
            .expect("subagent snapshot lock poisoned")
            .cancel_requested = true;
        child.changed.notify_all();
        self.inspect(parent_session_id, id)
    }

    pub fn dispose(&self, parent_session_id: &SessionId, id: &TaskId) -> Result<(), AgentError> {
        let child = self.child(parent_session_id, id)?;
        child.cancellation.cancel();
        self.lock().children.remove(id);
        Ok(())
    }

    fn cancel_children(&self, parent_session_id: &SessionId) {
        for child in self.lock().children.values() {
            let mut snapshot = child
                .snapshot
                .lock()
                .expect("subagent snapshot lock poisoned");
            if snapshot.parent_session_id == *parent_session_id
                && snapshot.status == AgentSubagentStatus::Running
            {
                snapshot.cancel_requested = true;
                child.cancellation.cancel();
                child.changed.notify_all();
            }
        }
    }
}

fn subagent_error(detail: impl Into<String>) -> AgentError {
    AgentError::Runtime {
        detail: detail.into(),
    }
}

fn inherited_session(parent: &AgentSession, session_id: SessionId) -> AgentSession {
    let mut child = AgentSession::new(session_id, parent.role.clone());
    child.agent_definition_key = parent.agent_definition_key.clone();
    child.workspace_binding = parent.workspace_binding.clone();
    child.execution_placement = parent.execution_placement.clone();
    child.sandbox_attachment = parent.sandbox_attachment.clone();
    child.resource_bindings = parent.resource_bindings.clone();
    child
}

fn parent_context_session(session: &AgentSession) -> AgentSession {
    let mut context = inherited_session(session, session.session_id.clone());
    context.abort = session.abort.clone();
    context
}

pub(super) struct AgentSubagentScope {
    pub(super) manager: Arc<AgentSubagents>,
    session_id: Mutex<SessionId>,
}

impl AgentSubagentScope {
    pub(super) fn cancel_children(&self) {
        let session_id = self
            .session_id
            .lock()
            .expect("subagent scope lock poisoned")
            .clone();
        self.manager.cancel_children(&session_id);
    }

    pub(super) fn replace_session(&self, session: &AgentSession) {
        let mut session_id = self
            .session_id
            .lock()
            .expect("subagent scope lock poisoned");
        let mut state = self.manager.lock();
        if *session_id != session.session_id {
            release_parent(&mut state, &session_id);
            let parent = state
                .parents
                .entry(session.session_id.clone())
                .or_insert(Parent {
                    session: parent_context_session(session),
                    input: None,
                    owners: 0,
                });
            parent.owners += 1;
            *session_id = session.session_id.clone();
        } else if let Some(parent) = state.parents.get_mut(&session.session_id) {
            parent.session = parent_context_session(session);
            parent.input = None;
        }
    }
}

impl Drop for AgentSubagentScope {
    fn drop(&mut self) {
        let session_id = self
            .session_id
            .lock()
            .expect("subagent scope lock poisoned");
        let mut state = self.manager.lock();
        release_parent(&mut state, &session_id);
    }
}

fn release_parent(state: &mut SubagentState, session_id: &SessionId) {
    if let Some(parent) = state.parents.get_mut(session_id) {
        parent.owners -= 1;
        if parent.owners > 0 {
            return;
        }
    }
    state.parents.remove(session_id);
    state.children.retain(|_, child| {
        if child
            .snapshot
            .lock()
            .expect("subagent snapshot lock poisoned")
            .parent_session_id
            == *session_id
        {
            child.cancellation.cancel();
            child.changed.notify_all();
            false
        } else {
            true
        }
    });
}

impl<M: InteractionModel + Send + Sync + 'static> Agent<M> {
    fn fork_subagent(&self) -> Self {
        Self {
            model: Arc::clone(&self.model),
            config: self.config.clone(),
            tool_surface: self.tool_surface.clone(),
            tools: self.tools.detached_snapshot(),
            context_assembler: ContextAssembler,
            memory_retriever: self.memory_retriever.clone(),
            compactor: self.compactor.clone(),
            compaction_engine: Arc::clone(&self.compaction_engine),
            trace_store: Arc::clone(&self.trace_store),
            session_store: Box::new(NoopAgentSessionStore),
            trace_store_configured: self.trace_store_configured,
            session_store_configured: false,
            subagents: OnceLock::new(),
        }
    }

    pub(super) fn subagent_scope(
        self: &Arc<Self>,
        session: AgentSession,
    ) -> Arc<AgentSubagentScope> {
        let manager = self
            .subagents
            .get_or_init(|| {
                let agent = Arc::downgrade(self);
                Arc::new(AgentSubagents {
                    state: Mutex::new(SubagentState::default()),
                    factory: Arc::new(move |session, input| {
                        let parent = agent
                            .upgrade()
                            .ok_or_else(|| subagent_error("parent agent was released"))?;
                        let mut child = parent.fork_subagent();
                        let mut task =
                            AgentTaskRef::new("subagent", latest_event_prompt_text(&input));
                        if let Some(id) = input
                            .latest_event
                            .payload
                            .pointer("/subagent/id")
                            .and_then(serde_json::Value::as_str)
                        {
                            task.task_id = id
                                .parse()
                                .map_err(|_| subagent_error("invalid child task identity"))?;
                        }
                        task.parent_task_id = parent
                            .config
                            .run_context
                            .task
                            .as_ref()
                            .map(|task| task.task_id.clone());
                        // Configuration and access are inherited; only task identity changes.
                        child.config.run_context.task = Some(task);
                        let child = Arc::new(child);
                        Ok(Box::new(move |cancellation| {
                            let _scope = child.subagent_scope(session);
                            child.run_with_session_context_and_cancellation(
                                &input,
                                &AgentTranscript::new(),
                                None,
                                &[],
                                &[],
                                cancellation,
                                AgentToolProgressSink::default(),
                                AgentModelProgressSink::default(),
                                &mut AgentTurnProgress::default(),
                            )
                        }))
                    }),
                })
            })
            .clone();
        self.tools.register(SubagentsTool {
            manager: Arc::downgrade(&manager),
        });
        let session_id = session.session_id.clone();
        {
            let mut state = manager.lock();
            let parent = state.parents.entry(session_id.clone()).or_insert(Parent {
                session: parent_context_session(&session),
                input: None,
                owners: 0,
            });
            parent.owners += 1;
        }
        Arc::new(AgentSubagentScope {
            manager,
            session_id: Mutex::new(session_id),
        })
    }
}

pub fn subagents_tool_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(AgentToolSpec::new(
        AgentToolKind::Subagents, "subagents",
        "Spawn temporary agents for parallel or focused tasks with your current permissions and resources. Inspect, wait, cancel, or dispose your children by id.", false,
    )).with_input_schema(serde_json::json!({
        "type": "object", "properties": {
            "action": {"type": "string", "enum": ["spawn", "list", "inspect", "wait", "cancel", "dispose"]},
            "task": {"type": "string", "description": "Required for spawn. Give a self-contained task."},
            "name": {"type": "string"}, "id": {"type": "string", "description": "Required for inspect, wait, cancel, and dispose."},
            "timeout_ms": {"type": "integer", "minimum": 0, "maximum": MAX_WAIT_MS}
        }, "required": ["action"], "additionalProperties": false
    }))
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum SubagentsAction {
    Spawn {
        task: String,
        #[serde(default)]
        name: String,
    },
    List,
    Inspect {
        id: TaskId,
    },
    Wait {
        id: TaskId,
        #[serde(default = "default_wait_ms")]
        timeout_ms: u64,
    },
    Cancel {
        id: TaskId,
    },
    Dispose {
        id: TaskId,
    },
}

fn default_wait_ms() -> u64 {
    1000
}

struct SubagentsTool {
    manager: Weak<AgentSubagents>,
}

impl AgentTool for SubagentsTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        subagents_tool_descriptor()
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        let result = (|| -> Result<serde_json::Value, AgentError> {
            let manager = self
                .manager
                .upgrade()
                .ok_or_else(|| subagent_error("subagent runtime was released"))?;
            let parent = &request.session.session_id;
            let action = serde_json::from_value::<SubagentsAction>(request.call.input.clone())
                .map_err(|error| subagent_error(format!("invalid subagents request: {error}")))?;
            let snapshot = match action {
                SubagentsAction::Spawn { task, name } => {
                    manager.spawn(parent, &name, &task, &request.cancellation_token)?
                }
                SubagentsAction::List => {
                    return Ok(serde_json::json!({"subagents": manager.list(parent)}));
                }
                SubagentsAction::Inspect { id } => manager.inspect(parent, &id)?,
                SubagentsAction::Wait { id, timeout_ms } => {
                    manager.wait(parent, &id, timeout_ms, &request.cancellation_token)?
                }
                SubagentsAction::Cancel { id } => manager.cancel(parent, &id)?,
                SubagentsAction::Dispose { id } => {
                    manager.dispose(parent, &id)?;
                    return Ok(serde_json::json!({"disposed": true, "id": id}));
                }
            };
            Ok(serde_json::json!({"subagent": snapshot}))
        })();
        match result {
            Ok(output) => AgentToolResult::succeeded("subagents operation completed", output),
            Err(error) => AgentToolResult::failed(
                error.to_string(),
                serde_json::json!({"error": error.to_string()}),
            ),
        }
    }
}
