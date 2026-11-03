//! Host-owned child sessions. Native providers supply tool policy, never a model loop.
use super::*;
use crate::{AsyncTaskNextAction, AsyncTaskStatus, AsyncTaskUpdate};
use sinomo_runtime::module_api::agent::SessionCommand;
use std::collections::VecDeque;
use std::sync::{Condvar, Weak};
use std::time::{Duration, Instant};

const MAX_WAIT_MS: u64 = 60_000;
const MAX_RESULT_BYTES: usize = 64 * 1024;
const IDLE_SECONDS: u64 = 24 * 60 * 60;
fn now() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

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
    pub root_session_id: SessionId,
    pub name: String,
    pub task: String,
    pub prompt: String,
    pub status: AgentSubagentStatus,
    pub cancel_requested: bool,
    pub result: Option<String>,
    pub result_truncated: bool,
    pub stop_reason: Option<AgentStopReason>,
    pub error: Option<String>,
    pub archived: bool,
    pub last_activity_unix: i64,
    pub idle_timeout_secs: u64,
}

trait ChildRuntime: Send + Sync {
    fn submit(&self, op: AgentOp) -> Result<AgentSessionControlResult, AgentError>;
    fn start(
        &self,
        op: AgentOp,
        cancellation: AgentCancellationToken,
    ) -> Result<AgentSessionControlResult, AgentError>;
    fn updates(&self) -> Vec<AsyncTaskUpdate>;
    fn detached_work(&self) -> bool;
    fn cancel_detached(&self);
    fn join(&self) -> Result<(), AgentError>;
    fn session(&self) -> Result<AgentSession, AgentError>;
    fn result(&self, id: &TaskId) -> Result<Option<AgentOpOutcome>, AgentError>;
    fn save_metadata(&self, snapshot: &AgentSubagentSnapshot) -> Result<(), AgentError>;
}
impl<M: InteractionModel + Send + Sync + 'static> ChildRuntime for AgentSessionRuntime<M> {
    fn submit(&self, op: AgentOp) -> Result<AgentSessionControlResult, AgentError> {
        self.submit_op(op)
    }
    fn start(
        &self,
        op: AgentOp,
        cancellation: AgentCancellationToken,
    ) -> Result<AgentSessionControlResult, AgentError> {
        self.submit_op_with_cancellation(op, cancellation)
    }
    fn updates(&self) -> Vec<AsyncTaskUpdate> {
        self.take_async_updates()
    }
    fn cancel_detached(&self) {
        self.cancel_detached_tools();
    }
    fn detached_work(&self) -> bool {
        self.session_snapshot()
            .is_ok_and(|session| super::control::detached_calls_running(&session.session_id) > 0)
    }
    fn join(&self) -> Result<(), AgentError> {
        self.join_active_turn()
    }
    fn session(&self) -> Result<AgentSession, AgentError> {
        self.session_snapshot()
    }
    fn result(&self, id: &TaskId) -> Result<Option<AgentOpOutcome>, AgentError> {
        self.turn_result(id)
    }
    fn save_metadata(&self, snapshot: &AgentSubagentSnapshot) -> Result<(), AgentError> {
        let mut state = self.lock_state()?;
        let mut session = state.control.session().clone();
        session.subagent = Some(snapshot.clone());
        session.relationship = Some(sinomo_runtime::module_api::sessions::SessionRelationship {
            parent_session_id: snapshot.parent_session_id.to_string(),
            kind: "delegation".into(),
        });
        session.state_revision = session.state_revision.saturating_add(1);
        self.agent.save_session(&session)?;
        *state.control.session_mut() = session;
        Ok(())
    }
}

type ChildFactory =
    dyn Fn(AgentSession, &str) -> Result<Arc<dyn ChildRuntime>, AgentError> + Send + Sync;
type ParentMessage = dyn Fn(&str) -> Result<bool, AgentError> + Send + Sync;
type ParentInspect = dyn Fn() -> Result<AgentSession, AgentError> + Send + Sync;
struct Parent {
    session: AgentSession,
    input: Option<AgentLoopInput>,
    factory: Arc<ChildFactory>,
    message: Option<Arc<ParentMessage>>,
    inspect: Option<Arc<ParentInspect>>,
    cancellation_generation: u64,
}
struct ChildState {
    snapshot: AgentSubagentSnapshot,
    session: AgentSession,
    runtime: Option<Arc<dyn ChildRuntime>>,
    input: AgentLoopInput,
    pending: bool,
    updates: VecDeque<AsyncTaskUpdate>,
    admission: AgentCancellationToken,
    driver: bool,
}
struct Child {
    state: Mutex<ChildState>,
    changed: Condvar,
}
#[derive(Default)]
struct SubagentState {
    restored: BTreeSet<SessionId>,
    parents: BTreeMap<SessionId, Parent>,
    children: BTreeMap<TaskId, Arc<Child>>,
}
pub struct AgentSubagents {
    state: Mutex<SubagentState>,
    store: Arc<dyn AgentSessionStore>,
}
impl AgentSubagents {
    fn lock(&self) -> std::sync::MutexGuard<'_, SubagentState> {
        self.state.lock().expect("session registry poisoned")
    }
    pub(super) fn set_session(&self, session: &AgentSession) {
        if let Some(parent) = self.lock().parents.get_mut(&session.session_id) {
            parent.session = session.clone();
        }
    }
    pub(super) fn set_input(self: &Arc<Self>, input: &AgentLoopInput) {
        if let Some(parent) = self.lock().parents.get_mut(&input.session.session_id) {
            parent.input = Some(input.clone());
        }
        if let Err(error) = self.restore_children(input) {
            tracing::error!(%error, "restore child sessions");
        }
    }
    fn restore_children(self: &Arc<Self>, input: &AgentLoopInput) -> Result<(), AgentError> {
        let caller = &input.session.session_id;
        let root = self.root(caller)?;
        if caller != &root || self.lock().restored.contains(&root) {
            return Ok(());
        }
        let sessions = self
            .store
            .list_sessions()
            .map_err(|e| subagent_error(e.to_string()))?;
        let factory = self.lock().parents.get(caller).unwrap().factory.clone();
        for mut session in sessions {
            let Some(mut snapshot) = session.subagent.clone() else {
                continue;
            };
            if snapshot.root_session_id != root || self.lock().children.contains_key(&snapshot.id) {
                continue;
            }
            if snapshot.status == AgentSubagentStatus::Running {
                snapshot.status = AgentSubagentStatus::Failed;
                snapshot.error =
                    Some("execution interrupted by process restart; work was not replayed".into());
            }
            if now() >= snapshot.last_activity_unix + snapshot.idle_timeout_secs as i64 {
                snapshot.archived = true;
            }
            session.subagent = Some(snapshot.clone());
            session.relationship =
                Some(sinomo_runtime::module_api::sessions::SessionRelationship {
                    parent_session_id: snapshot.parent_session_id.to_string(),
                    kind: "delegation".into(),
                });
            self.store
                .save_session(&session)
                .map_err(|e| subagent_error(e.to_string()))?;
            let runtime = if snapshot.archived {
                None
            } else {
                Some(factory(session.clone(), &snapshot.prompt)?)
            };
            let mut child_input = input.clone();
            child_input.session.session_id = snapshot.session_id.clone();
            child_input.latest_event = crate::user_interaction_input_event(
                &child_input.session,
                "Continue with queued messages.",
            );
            child_input.latest_event.payload["subagent"] = serde_json::json!({
                "id": snapshot.id, "name": snapshot.name,
                "parent_session_id": snapshot.parent_session_id,
            });
            child_input.transient_system_context = None;
            child_input.async_update = None;
            let child = Arc::new(Child {
                state: Mutex::new(ChildState {
                    snapshot: snapshot.clone(),
                    session,
                    runtime,
                    input: child_input,
                    pending: false,
                    updates: VecDeque::new(),
                    admission: AgentCancellationToken::new(),
                    driver: false,
                }),
                changed: Condvar::new(),
            });
            self.lock().children.insert(snapshot.id, child.clone());
            if !snapshot.archived {
                self.start_driver(child)?;
            }
        }
        self.lock().restored.insert(root);
        Ok(())
    }
    fn root(&self, caller: &SessionId) -> Result<SessionId, AgentError> {
        let state = self.lock();
        let parent = state
            .parents
            .get(caller)
            .ok_or_else(|| subagent_error("caller session is not live"))?;
        Ok(parent
            .session
            .subagent
            .as_ref()
            .map(|s| s.root_session_id.clone())
            .unwrap_or_else(|| caller.clone()))
    }
    fn child(&self, caller: &SessionId, id: &TaskId) -> Result<Arc<Child>, AgentError> {
        let root = self.root(caller)?;
        let child = self
            .lock()
            .children
            .get(id)
            .cloned()
            .ok_or_else(|| subagent_error("unknown agent id"))?;
        if child.state.lock().unwrap().snapshot.root_session_id != root {
            return Err(subagent_error("agent belongs to another session tree"));
        }
        Ok(child)
    }
    pub fn spawn(
        self: &Arc<Self>,
        caller: &SessionId,
        name: &str,
        task: &str,
        cancellation: &AgentCancellationToken,
    ) -> Result<AgentSubagentSnapshot, AgentError> {
        self.spawn_with_prompt(caller, name, task, "", IDLE_SECONDS, cancellation)
    }
    fn spawn_with_prompt(
        self: &Arc<Self>,
        caller: &SessionId,
        name: &str,
        task: &str,
        prompt: &str,
        idle_timeout_secs: u64,
        cancellation: &AgentCancellationToken,
    ) -> Result<AgentSubagentSnapshot, AgentError> {
        if task.trim().is_empty()
            || task.len() > 128 * 1024
            || prompt.len() > 64 * 1024
            || name.len() > 1024
        {
            return Err(subagent_error("invalid child instructions, prompt or name"));
        }
        if cancellation.is_cancelled() {
            return Err(subagent_error("parent operation is cancelled"));
        }
        let root = self.root(caller)?;
        let (mut input, mut session, factory, inherited_prompt, cancellation_generation) = {
            let state = self.lock();
            let parent = state
                .parents
                .get(caller)
                .ok_or_else(|| subagent_error("parent session was released"))?;
            if parent.session.is_aborted() {
                return Err(subagent_error("parent session is aborted"));
            }
            (
                parent
                    .input
                    .clone()
                    .ok_or_else(|| subagent_error("parent session has no turn context"))?,
                inherited_session(&parent.session, SessionId::generate()),
                parent.factory.clone(),
                parent
                    .session
                    .subagent
                    .as_ref()
                    .map(|s| s.prompt.clone())
                    .unwrap_or_default(),
                parent.cancellation_generation,
            )
        };
        let snapshot = AgentSubagentSnapshot {
            id: TaskId::generate(),
            parent_session_id: caller.clone(),
            session_id: session.session_id.clone(),
            root_session_id: root,
            name: name.into(),
            task: task.into(),
            prompt: format!("{inherited_prompt}\n{prompt}"),
            status: AgentSubagentStatus::Running,
            cancel_requested: false,
            result: None,
            result_truncated: false,
            stop_reason: None,
            error: None,
            archived: false,
            last_activity_unix: now(),
            idle_timeout_secs: idle_timeout_secs.clamp(1, IDLE_SECONDS),
        };
        session.subagent = Some(snapshot.clone());
        session.relationship = Some(sinomo_runtime::module_api::sessions::SessionRelationship {
            parent_session_id: snapshot.parent_session_id.to_string(),
            kind: "delegation".into(),
        });
        input.session.session_id = session.session_id.clone();
        let mut event = crate::user_interaction_input_event(&input.session, task);
        event
            .links
            .caused_by
            .push(input.latest_event.event_id.clone());
        let attribution = sinomo_lm::usage::current_attribution();
        event.payload["subagent"] = serde_json::json!({
            "id": snapshot.id, "name": name, "parent_session_id": caller,
            "parent_operation": attribution.operation,
            "parent_tool_call": attribution.tool_call,
        });
        input.latest_event = event;
        input.transient_system_context = None;
        input.async_update = None;
        self.store
            .save_session(&session)
            .map_err(|e| subagent_error(e.to_string()))?;
        let runtime = factory(session.clone(), prompt)?;
        let child = Arc::new(Child {
            state: Mutex::new(ChildState {
                snapshot: snapshot.clone(),
                session,
                runtime: Some(runtime),
                input,
                pending: true,
                updates: VecDeque::new(),
                admission: cancellation.child_token(),
                driver: false,
            }),
            changed: Condvar::new(),
        });
        {
            let mut registry = self.lock();
            if registry.parents.get(caller).is_none_or(|parent| {
                parent.cancellation_generation != cancellation_generation
                    || parent.session.is_aborted()
            }) {
                drop(registry);
                let mut state = child.state.lock().unwrap();
                state.snapshot.status = AgentSubagentStatus::Cancelled;
                state.snapshot.archived = true;
                state.snapshot.cancel_requested = true;
                state.session.subagent = Some(state.snapshot.clone());
                self.store
                    .save_session(&state.session)
                    .map_err(|e| subagent_error(e.to_string()))?;
                return Err(subagent_error("parent cancelled child admission"));
            }
            registry.children.insert(snapshot.id.clone(), child.clone());
        }
        self.start_driver(child)?;
        Ok(snapshot)
    }
    fn start_driver(self: &Arc<Self>, child: Arc<Child>) -> Result<(), AgentError> {
        let mut state = child.state.lock().unwrap();
        if state.driver {
            return Ok(());
        }
        state.driver = true;
        let manager = Arc::downgrade(self);
        let worker = child.clone();
        if let Err(e) = thread::Builder::new()
            .name(format!("agent-session-{}", state.snapshot.id))
            .spawn(move || {
                Self::drive(manager, worker);
            })
        {
            state.driver = false;
            return Err(subagent_error(e.to_string()));
        }
        Ok(())
    }
    fn drive(manager: Weak<Self>, child: Arc<Child>) {
        loop {
            let (runtime, input, admission) = {
                let mut state = child.state.lock().unwrap();
                while !state.pending && !state.snapshot.archived {
                    if let Some(runtime) = state.runtime.clone() {
                        state.updates.extend(runtime.updates());
                        if !state.updates.is_empty() {
                            state.pending = true;
                            break;
                        }
                        if runtime.detached_work() {
                            state.snapshot.last_activity_unix = now();
                        }
                    }
                    let remaining = (state.snapshot.last_activity_unix
                        + state.snapshot.idle_timeout_secs as i64
                        - now())
                    .max(0) as u64;
                    if remaining == 0 {
                        state.snapshot.archived = true;
                        break;
                    }
                    state = child
                        .changed
                        .wait_timeout(state, Duration::from_secs(remaining.min(1)))
                        .unwrap()
                        .0;
                }
                if state.snapshot.archived {
                    if let Some(runtime) = &state.runtime
                        && let Ok(session) = runtime.session()
                    {
                        state.session = session;
                    }
                    state.session.subagent = Some(state.snapshot.clone());
                    if let Some(manager) = manager.upgrade()
                        && let Err(error) = manager.store.save_session(&state.session)
                    {
                        state.snapshot.error = Some(format!("archive persistence failed: {error}"));
                        // Keep the live handle and retry instead of losing the archive.
                        state.snapshot.archived = false;
                        state.snapshot.last_activity_unix = now();
                        continue;
                    }
                    let runtime = state.runtime.take();
                    state.driver = false;
                    child.changed.notify_all();
                    drop(state);
                    drop(runtime);
                    return;
                }
                if let Some(runtime) = state.runtime.clone() {
                    state.updates.extend(runtime.updates());
                }
                let update = state.updates.pop_front();
                state.pending = !state.updates.is_empty();
                state.snapshot.status = AgentSubagentStatus::Running;
                state.snapshot.cancel_requested = false;
                state.snapshot.last_activity_unix = now();
                let mut input = state.input.clone();
                input.async_update = update;
                state.input.async_update = None;
                let delegation = state.input.latest_event.payload.get("subagent").cloned();
                state.input.latest_event = crate::user_interaction_input_event(
                    &state.input.session,
                    "Continue with the queued messages and updates.",
                );
                if let Some(delegation) = delegation {
                    state.input.latest_event.payload["subagent"] = delegation;
                }
                (
                    state.runtime.as_ref().unwrap().clone(),
                    input,
                    state.admission.clone(),
                )
            };
            let op = AgentOp::continue_turn(input);
            let id = op.op_id.clone();
            let result = (|| {
                let accepted = runtime.start(op, admission.clone())?;
                if !accepted.accepted {
                    return Err(subagent_error("child turn admission rejected"));
                }
                runtime.join()?;
                runtime.result(&id)
            })();
            let snapshot = {
                let mut state = child.state.lock().unwrap();
                if let Ok(session) = runtime.session() {
                    state.session = session;
                }
                state.snapshot.last_activity_unix = now();
                state.snapshot.cancel_requested |= admission.is_cancelled();
                state.admission = AgentCancellationToken::new();
                state.snapshot.result = None;
                state.snapshot.result_truncated = false;
                state.snapshot.stop_reason = None;
                state.snapshot.error = None;
                match result {
                    Ok(Some(outcome)) => {
                        if let Some(run) = outcome.run {
                            let mut text = run.final_decision.text.clone();
                            state.snapshot.result_truncated = text.len() > MAX_RESULT_BYTES;
                            if text.len() > MAX_RESULT_BYTES {
                                let mut end = MAX_RESULT_BYTES;
                                while !text.is_char_boundary(end) {
                                    end -= 1;
                                }
                                text.truncate(end);
                            }
                            state.snapshot.result = Some(text);
                            state.snapshot.stop_reason = Some(run.stop_reason);
                        }
                        state.snapshot.status = if state.snapshot.cancel_requested {
                            AgentSubagentStatus::Cancelled
                        } else if state.snapshot.stop_reason
                            == Some(AgentStopReason::MaxStepsReached)
                        {
                            state.snapshot.error =
                                Some("subagent reached its step limit before completing".into());
                            AgentSubagentStatus::Failed
                        } else {
                            AgentSubagentStatus::Completed
                        };
                    }
                    Ok(None) => {
                        state.snapshot.status = AgentSubagentStatus::Failed;
                        state.snapshot.error = Some("child completed without a turn result".into());
                    }
                    Err(error) => {
                        state.snapshot.status = if state.snapshot.cancel_requested {
                            AgentSubagentStatus::Cancelled
                        } else {
                            AgentSubagentStatus::Failed
                        };
                        state.snapshot.error = Some(error.to_string());
                    }
                }
                if state.snapshot.status == AgentSubagentStatus::Completed
                    && runtime.detached_work()
                {
                    state.snapshot.status = AgentSubagentStatus::Running;
                }
                if !state.session.follow_up_queue.is_empty()
                    || !state.session.steering_queue.is_empty()
                {
                    state.pending = true;
                }
                if let Err(error) = runtime.save_metadata(&state.snapshot) {
                    state.snapshot.error = Some(error.to_string());
                    state.snapshot.status = AgentSubagentStatus::Failed;
                }
                state.session.subagent = Some(state.snapshot.clone());
                child.changed.notify_all();
                state.snapshot.clone()
            };
            if snapshot.status != AgentSubagentStatus::Running
                && let Some(manager) = manager.upgrade()
                && let Err(error) = manager.notify_parent(&snapshot)
            {
                child.state.lock().unwrap().snapshot.error =
                    Some(format!("parent notification failed: {error}"));
            }
        }
    }
    fn notify_parent(&self, snapshot: &AgentSubagentSnapshot) -> Result<(), AgentError> {
        let update = AsyncTaskUpdate {
            kind: "agent.subagent.completed".into(),
            task_id: TaskId::generate(),
            tool_call_id: None,
            status: match snapshot.status {
                AgentSubagentStatus::Completed => AsyncTaskStatus::Completed,
                AgentSubagentStatus::Cancelled => AsyncTaskStatus::Cancelled,
                _ => AsyncTaskStatus::Failed,
            },
            summary: format!(
                "Subagent {} ({}) finished: {}",
                snapshot.name,
                snapshot.id,
                snapshot
                    .result
                    .as_deref()
                    .or(snapshot.error.as_deref())
                    .unwrap_or("no output")
            ),
            needs_user_attention: false,
            result_ref: Some(snapshot.session_id.to_string()),
            next_action_hint: AsyncTaskNextAction::InspectResult,
        };
        self.store
            .append_record(&AgentSessionStoreRecord::new(
                snapshot.parent_session_id.clone(),
                AgentSessionStoreRecordKind::DetachedCallStarted {
                    task_id: update.task_id.clone(),
                    tool_call_id: update.task_id.clone(),
                    tool: "agent.subagent".into(),
                },
            ))
            .map_err(|e| subagent_error(e.to_string()))?;
        self.store
            .append_record(&AgentSessionStoreRecord::new(
                snapshot.parent_session_id.clone(),
                AgentSessionStoreRecordKind::DetachedCallCompleted {
                    update: update.clone(),
                },
            ))
            .map_err(|e| subagent_error(e.to_string()))?;
        super::control::queue_session_update(&snapshot.parent_session_id, update.clone());
        let children: Vec<_> = self.lock().children.values().cloned().collect();
        for parent in children {
            let mut state = parent.state.lock().unwrap();
            if state.snapshot.session_id == snapshot.parent_session_id && !state.snapshot.archived {
                // The existing runtime completion queue owns delivery and
                // replay. Wake the driver without copying or overwriting it.
                state.pending = true;
                parent.changed.notify_all();
                break;
            }
        }
        Ok(())
    }
    pub fn inspect(
        &self,
        caller: &SessionId,
        id: &TaskId,
    ) -> Result<AgentSubagentSnapshot, AgentError> {
        Ok(self
            .child(caller, id)?
            .state
            .lock()
            .unwrap()
            .snapshot
            .clone())
    }
    pub fn session_snapshot(
        &self,
        caller: &SessionId,
        id: &TaskId,
    ) -> Result<AgentSession, AgentError> {
        let child = self.child(caller, id)?;
        let state = child.state.lock().unwrap();
        match &state.runtime {
            Some(runtime) => runtime.session(),
            None => Ok(state.session.clone()),
        }
    }
    pub fn list(&self, caller: &SessionId) -> Vec<AgentSubagentSnapshot> {
        self.search(caller, false)
    }
    pub fn search(&self, caller: &SessionId, archived: bool) -> Vec<AgentSubagentSnapshot> {
        let Ok(root) = self.root(caller) else {
            return vec![];
        };
        let children: Vec<_> = self.lock().children.values().cloned().collect();
        children
            .into_iter()
            .filter_map(|c| {
                let state = c.state.lock().unwrap();
                (state.snapshot.root_session_id == root && state.snapshot.archived == archived)
                    .then(|| state.snapshot.clone())
            })
            .collect()
    }
    pub fn wait(
        &self,
        caller: &SessionId,
        id: &TaskId,
        timeout_ms: u64,
        cancellation: &AgentCancellationToken,
    ) -> Result<AgentSubagentSnapshot, AgentError> {
        let child = self.child(caller, id)?;
        let deadline = Instant::now() + Duration::from_millis(timeout_ms.min(MAX_WAIT_MS));
        let mut state = child.state.lock().unwrap();
        while state.snapshot.status == AgentSubagentStatus::Running && !cancellation.is_cancelled()
        {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            state = child
                .changed
                .wait_timeout(state, remaining.min(Duration::from_millis(25)))
                .unwrap()
                .0;
        }
        Ok(state.snapshot.clone())
    }
    pub fn cancel(
        &self,
        caller: &SessionId,
        id: &TaskId,
    ) -> Result<AgentSubagentSnapshot, AgentError> {
        let child = self.child(caller, id)?;
        let runtime = {
            let mut state = child.state.lock().unwrap();
            if state.snapshot.parent_session_id != *caller {
                return Err(subagent_error("only the parent can cancel a child"));
            }
            state.snapshot.cancel_requested = true;
            state.admission.cancel();
            state.runtime.clone()
        };
        if let Some(runtime) = runtime {
            runtime.cancel_detached();
            runtime.submit(AgentOp::cancel_active_turn("parent cancelled child"))?;
        }
        let state = child.state.lock().unwrap();
        child.changed.notify_all();
        Ok(state.snapshot.clone())
    }
    pub fn dispose(&self, caller: &SessionId, id: &TaskId) -> Result<(), AgentError> {
        self.cancel(caller, id)?;
        let child = self.child(caller, id)?;
        let mut state = child.state.lock().unwrap();
        state.snapshot.archived = true;
        state.pending = false;
        if let Some(runtime) = &state.runtime {
            runtime.save_metadata(&state.snapshot)?;
        } else {
            state.session.subagent = Some(state.snapshot.clone());
            self.store
                .save_session(&state.session)
                .map_err(|e| subagent_error(e.to_string()))?;
        }
        child.changed.notify_all();
        Ok(())
    }
    pub fn message(
        self: &Arc<Self>,
        caller: &SessionId,
        id: &str,
        message: &str,
    ) -> Result<(), AgentError> {
        if message.trim().is_empty() || message.len() > 128 * 1024 {
            return Err(subagent_error("invalid message"));
        }
        let parent_target = {
            let state = self.lock();
            let parent = state
                .parents
                .get(caller)
                .ok_or_else(|| subagent_error("caller session is not live"))?;
            if id == "parent" {
                parent
                    .session
                    .subagent
                    .as_ref()
                    .map(|s| s.parent_session_id.clone())
            } else {
                None
            }
        };
        let text = format!("Message from agent session {caller}:\n{message}");
        if let Some(target) = parent_target {
            let parent_id = self
                .lock()
                .parents
                .get(&target)
                .and_then(|p| p.session.subagent.as_ref())
                .map(|s| s.id.clone());
            if let Some(id) = parent_id {
                return self.message(caller, id.as_str(), message);
            }
            let send = self
                .lock()
                .parents
                .get(&target)
                .and_then(|p| p.message.clone())
                .ok_or_else(|| subagent_error("parent session is unavailable"))?;
            if send(&text)? {
                return Ok(());
            }
            let mut session = self
                .store
                .load_session(&target)
                .map_err(|e| subagent_error(e.to_string()))?
                .ok_or_else(|| subagent_error("parent session is unavailable"))?;
            session
                .follow_up_queue
                .push(AgentFollowUpMessage::new(text));
            session.state_revision = session.state_revision.saturating_add(1);
            self.store
                .save_session(&session)
                .map_err(|e| subagent_error(e.to_string()))?;
            return Ok(());
        }
        let id: TaskId = id.parse().map_err(|_| subagent_error("invalid agent id"))?;
        let child = self.child(caller, &id)?;
        let runtime = {
            let state = child.state.lock().unwrap();
            if state.snapshot.archived {
                return Err(subagent_error("agent is archived"));
            }
            state
                .runtime
                .clone()
                .ok_or_else(|| subagent_error("agent runtime is unavailable"))?
        };
        // Runtime control can propagate to descendants. Never hold a child
        // registry lock while entering that control plane.
        if runtime.session()?.is_paused() {
            runtime.submit(AgentOp::unpause())?;
        }
        runtime.submit(AgentOp::follow_up(text))?;
        {
            let mut state = child.state.lock().unwrap();
            if state.snapshot.archived {
                return Err(subagent_error("agent archived during message admission"));
            }
            if state.snapshot.status != AgentSubagentStatus::Running {
                state.admission = AgentCancellationToken::new();
            }
            state.pending = true;
            state.snapshot.status = AgentSubagentStatus::Running;
            state.snapshot.last_activity_unix = now();
            child.changed.notify_all();
        }
        self.start_driver(child)
    }
    fn cancel_children(&self, caller: &SessionId) {
        if let Some(parent) = self.lock().parents.get_mut(caller) {
            parent.cancellation_generation = parent.cancellation_generation.wrapping_add(1);
        }
        for child in self.list(caller) {
            if child.parent_session_id == *caller {
                let _ = self.cancel(caller, &child.id);
            }
        }
    }
    fn set_children_paused(&self, caller: &SessionId, paused: bool) {
        let children: Vec<_> = self.lock().children.values().cloned().collect();
        for child in children {
            let runtime = {
                let state = child.state.lock().unwrap();
                (state.snapshot.parent_session_id == *caller)
                    .then(|| state.runtime.clone())
                    .flatten()
            };
            if let Some(runtime) = runtime {
                let _ = runtime.submit(if paused {
                    AgentOp::pause("parent paused")
                } else {
                    AgentOp::unpause()
                });
            }
        }
    }
}
fn subagent_error(detail: impl Into<String>) -> AgentError {
    AgentError::Runtime {
        detail: detail.into(),
    }
}
fn inherited_session(parent: &AgentSession, id: SessionId) -> AgentSession {
    let mut child = AgentSession::new(id, parent.role.clone());
    child.agent_definition_key = parent.agent_definition_key.clone();
    child.workspace_binding = parent.workspace_binding.clone();
    child.execution_placement = parent.execution_placement.clone();
    child.sandbox_attachment = parent.sandbox_attachment.clone();
    child.resource_bindings = parent.resource_bindings.clone();
    child
}

pub(super) struct AgentSubagentScope {
    pub(super) manager: Arc<AgentSubagents>,
    session_id: Mutex<SessionId>,
}
impl AgentSubagentScope {
    pub(super) fn cancel_children(&self) {
        self.manager
            .cancel_children(&self.session_id.lock().unwrap());
    }
    pub(super) fn set_children_paused(&self, paused: bool) {
        self.manager
            .set_children_paused(&self.session_id.lock().unwrap(), paused);
    }
    pub(super) fn replace_session(&self, session: &AgentSession) {
        let mut id = self.session_id.lock().unwrap();
        let mut state = self.manager.lock();
        if *id != session.session_id {
            if let Some(parent) = state.parents.remove(&id) {
                state.parents.insert(
                    session.session_id.clone(),
                    Parent {
                        session: session.clone(),
                        input: None,
                        ..parent
                    },
                );
            }
            *id = session.session_id.clone();
        } else if let Some(parent) = state.parents.get_mut(&id) {
            parent.session = session.clone();
        }
    }
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
            compaction_engine: self.compaction_engine.clone(),
            trace_store: self.trace_store.clone(),
            session_store: self.session_store.clone(),
            trace_store_configured: self.trace_store_configured,
            session_store_configured: self.session_store_configured,
            subagents: self
                .subagents
                .get()
                .cloned()
                .map(OnceLock::from)
                .unwrap_or_default(),
            session_tools: self.session_tools.clone(),
            item_journals: Mutex::new(BTreeMap::new()),
        }
    }
    pub(super) fn subagent_scope(
        self: &Arc<Self>,
        session: AgentSession,
    ) -> Arc<AgentSubagentScope> {
        type RegistryKey = (PathBuf, SessionId);
        static REGISTRIES: OnceLock<Mutex<BTreeMap<RegistryKey, Weak<AgentSubagents>>>> =
            OnceLock::new();
        let root_id = session
            .subagent
            .as_ref()
            .map(|s| s.root_session_id.clone())
            .unwrap_or_else(|| session.session_id.clone());
        let key = self
            .session_store
            .session_namespace()
            .map(|namespace| (namespace, root_id));
        let mut registries = REGISTRIES
            .get_or_init(Default::default)
            .lock()
            .expect("session registries poisoned");
        registries.retain(|_, manager| manager.strong_count() > 0);
        let manager = self
            .subagents
            .get_or_init(|| {
                key.as_ref()
                    .and_then(|key| registries.get(key))
                    .and_then(Weak::upgrade)
                    .unwrap_or_else(|| {
                        Arc::new(AgentSubagents {
                            state: Mutex::new(SubagentState::default()),
                            store: self.session_store.clone(),
                        })
                    })
            })
            .clone();
        if let Some(key) = key {
            registries.insert(key, Arc::downgrade(&manager));
        }
        drop(registries);
        let agent = Arc::downgrade(self);
        let factory = Arc::new(
            move |session: AgentSession,
                  prompt: &str|
                  -> Result<Arc<dyn ChildRuntime>, AgentError> {
                let parent = agent
                    .upgrade()
                    .ok_or_else(|| subagent_error("parent agent was released"))?;
                let mut child = parent.fork_subagent();
                child
                    .config
                    .definition
                    .system_prompt
                    .push_str(&format!("\n\n{prompt}"));
                if let Some(snapshot) = &session.subagent {
                    let mut task = AgentTaskRef::new("subagent", &snapshot.task);
                    task.task_id = snapshot.id.clone();
                    task.parent_task_id = parent
                        .config
                        .run_context
                        .task
                        .as_ref()
                        .map(|task| task.task_id.clone());
                    child.config.run_context.task = Some(task);
                }
                Ok(Arc::new(AgentSessionRuntime::new(child, session)))
            },
        );
        if let Some(provider) = &self.session_tools {
            let sessions = Arc::downgrade(&manager);
            self.tools
                .register(super::session_tools::SessionProviderTool::new(
                    provider.clone(),
                    move |caller, cancellation| {
                        sessions
                            .upgrade()
                            .ok_or_else(|| subagent_error("session runtime released"))?
                            .tool_session_access(caller, cancellation)
                    },
                ));
        }
        let id = session.session_id.clone();
        {
            let mut state = manager.lock();
            let parent = state.parents.entry(id.clone()).or_insert_with(|| Parent {
                session: session.clone(),
                input: None,
                factory: factory.clone(),
                message: None,
                inspect: None,
                cancellation_generation: 0,
            });
            parent.session = session;
            parent.factory = factory;
        }
        Arc::new(AgentSubagentScope {
            manager,
            session_id: Mutex::new(id),
        })
    }
}

impl<M: InteractionModel + Send + Sync + 'static> AgentSessionRuntime<M> {
    pub(super) fn bind_session_messages(&self) {
        let weak_state = Arc::downgrade(&self.state);
        let weak_agent = Arc::downgrade(&self.agent);
        let weak_scope = Arc::downgrade(&self.subagent_scope);
        let message = Arc::new(move |text: &str| {
            let (Some(state), Some(agent), Some(subagent_scope)) = (
                weak_state.upgrade(),
                weak_agent.upgrade(),
                weak_scope.upgrade(),
            ) else {
                return Ok(false);
            };
            let runtime = AgentSessionRuntime {
                state,
                agent,
                subagent_scope,
            };
            runtime.submit_op(AgentOp::follow_up(text))?;
            Ok(true)
        });
        let weak_state = Arc::downgrade(&self.state);
        let inspect = Arc::new(move || {
            let state = weak_state
                .upgrade()
                .ok_or_else(|| subagent_error("parent session detached"))?;
            let session = state
                .lock()
                .map_err(|_| subagent_error("parent session poisoned"))?
                .control
                .session()
                .clone();
            Ok(session)
        });
        let id = self.session_snapshot().expect("session").session_id;
        if let Some(parent) = self.subagents().lock().parents.get_mut(&id) {
            parent.message = Some(message);
            parent.inspect = Some(inspect);
        }
    }
}

impl AgentSubagents {
    fn tool_session_access(
        self: &Arc<Self>,
        caller: &SessionId,
        cancellation: AgentCancellationToken,
    ) -> Result<Arc<dyn sinomo_runtime::module::sessions::SessionAccess>, AgentError> {
        let caller = caller.clone();
        let root = self.root(&caller)?;
        let manager = self.clone();
        let access = crate::session_access::AgentSessionAccess::new(
            self.store.clone(),
            [(root.to_string(), "Agent".into())].into_iter().collect(),
        )
        .with_cancellation(cancellation.clone())
        .with_commands(move |command| {
            manager
                .execute_command(&caller, command, &cancellation)
                .map_err(|e| e.to_string())
        });
        Ok(Arc::new(access))
    }

    fn execute_command(
        self: &Arc<Self>,
        caller: &SessionId,
        command: SessionCommand,
        cancellation: &AgentCancellationToken,
    ) -> Result<serde_json::Value, AgentError> {
        let parse = |id: String| -> Result<TaskId, AgentError> {
            let id = id
                .parse::<TaskId>()
                .map_err(|_| subagent_error("invalid session or agent id"))?;
            if self.lock().children.contains_key(&id) {
                return Ok(id);
            }
            // Reuse the existing owner registry; session IDs are the generic
            // API identity and task IDs remain supported tool aliases.
            let children: Vec<_> = self
                .lock()
                .children
                .iter()
                .map(|(id, child)| (id.clone(), child.clone()))
                .collect();
            Ok(children
                .into_iter()
                .find(|(_, child)| child.state.lock().unwrap().snapshot.session_id == id)
                .map(|(id, _)| id)
                .unwrap_or(id))
        };
        let snapshot = match command {
            SessionCommand::Create {
                name,
                instructions,
                prompt,
                idle_timeout_secs,
            } => self.spawn_with_prompt(
                caller,
                &name,
                &instructions,
                &prompt,
                idle_timeout_secs,
                cancellation,
            )?,
            SessionCommand::List { archived } => {
                return Ok(serde_json::json!({"subagents": self.search(caller, archived)}));
            }
            SessionCommand::Inspect { id, session } => {
                if id == "parent" {
                    let inspect = {
                        let state = self.lock();
                        let parent_id = state
                            .parents
                            .get(caller)
                            .and_then(|p| p.session.subagent.as_ref())
                            .map(|s| &s.parent_session_id)
                            .ok_or_else(|| subagent_error("no parent"))?;
                        state
                            .parents
                            .get(parent_id)
                            .and_then(|p| p.inspect.clone())
                            .ok_or_else(|| subagent_error("parent unavailable"))?
                    };
                    return Ok(serde_json::json!({"session": inspect()?}));
                }
                let id = parse(id)?;
                if session {
                    return Ok(serde_json::json!({"session": self.session_snapshot(caller, &id)?}));
                }
                self.inspect(caller, &id)?
            }
            SessionCommand::Message { id, message } => {
                let parent = self
                    .lock()
                    .parents
                    .get(caller)
                    .and_then(|parent| parent.session.subagent.as_ref())
                    .map(|child| child.parent_session_id.to_string());
                let (target, session_id) = if id == "parent" || parent.as_ref() == Some(&id) {
                    ("parent".to_string(), parent)
                } else {
                    let target = parse(id)?;
                    let session = self.inspect(caller, &target)?.session_id.to_string();
                    (target.to_string(), Some(session))
                };
                self.message(caller, &target, &message)?;
                return Ok(serde_json::json!({"accepted":true,"session_id":session_id}));
            }
            SessionCommand::Wait { id, timeout_ms } => {
                self.wait(caller, &parse(id)?, timeout_ms, cancellation)?
            }
            SessionCommand::Cancel { id } => self.cancel(caller, &parse(id)?)?,
            SessionCommand::Archive { id } => {
                let id = parse(id)?;
                let session_id = self.inspect(caller, &id)?.session_id;
                self.dispose(caller, &id)?;
                return Ok(
                    serde_json::json!({"accepted":true,"session_id":session_id,"archived":true}),
                );
            }
        };
        Ok::<_, AgentError>(
            serde_json::json!({"accepted":true,"session_id":snapshot.session_id,"status":snapshot.status,"archived":snapshot.archived,"subagent":snapshot}),
        )
    }
}
