//! Host-owned managed sessions. Native providers supply tool policy, never a model loop.
use super::*;
use crate::{AsyncTaskNextAction, AsyncTaskStatus, AsyncTaskUpdate};
use sinomo_runtime::module_api::sessions::SessionCommand;
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
pub enum ManagedSessionStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ManagedSessionSnapshot {
    pub id: TaskId,
    pub parent_session_id: Option<SessionId>,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub operation_id: Option<TaskId>,
    pub session_id: SessionId,
    pub root_session_id: SessionId,
    pub name: String,
    pub task: String,
    pub prompt: String,
    pub status: ManagedSessionStatus,
    pub cancel_requested: bool,
    pub result: Option<String>,
    pub result_truncated: bool,
    pub stop_reason: Option<AgentStopReason>,
    pub error: Option<String>,
    pub archived: bool,
    pub last_activity_unix: i64,
    pub idle_timeout_secs: u64,
}

impl ManagedSessionSnapshot {
    pub fn archived_at(&self, unix_seconds: i64) -> bool {
        self.archived
            || (self.status != ManagedSessionStatus::Running
                && unix_seconds
                    >= self
                        .last_activity_unix
                        .saturating_add(self.idle_timeout_secs as i64))
    }
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
    fn pending_work(&self) -> bool;
    fn cancel_detached(&self);
    fn join(&self) -> Result<(), AgentError>;
    fn session(&self) -> Result<AgentSession, AgentError>;
    fn result(&self, id: &TaskId) -> Result<Option<AgentOpOutcome>, AgentError>;
    fn save_metadata(&self, snapshot: &ManagedSessionSnapshot) -> Result<(), AgentError>;
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
    fn pending_work(&self) -> bool {
        self.has_pending_turn_work().unwrap_or(false)
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
    fn save_metadata(&self, snapshot: &ManagedSessionSnapshot) -> Result<(), AgentError> {
        let mut state = self.lock_state()?;
        let mut session = state.control.session().clone();
        session.managed = Some(snapshot.clone());
        session.relationship = snapshot.parent_session_id.as_ref().map(|parent| {
            sinomo_runtime::module_api::sessions::SessionRelationship {
                parent_session_id: parent.to_string(),
                kind: "delegation".into(),
            }
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
    snapshot: ManagedSessionSnapshot,
    session: AgentSession,
    runtime: Option<Arc<dyn ChildRuntime>>,
    input: AgentLoopInput,
    pending: bool,
    pending_operation_id: Option<TaskId>,
    updates: VecDeque<AsyncTaskUpdate>,
    admission: AgentCancellationToken,
    driver: bool,
}
struct Child {
    state: Mutex<ChildState>,
    changed: Condvar,
}
#[derive(Default)]
struct SessionHostState {
    closing: bool,
    restored: BTreeSet<SessionId>,
    parents: BTreeMap<SessionId, Parent>,
    children: BTreeMap<TaskId, Arc<Child>>,
}
pub struct AgentSessionHost {
    state: Mutex<SessionHostState>,
    store: Arc<dyn AgentSessionStore>,
}
impl AgentSessionHost {
    pub fn recover_interrupted(&self) -> Result<(), String> {
        if !self.lock().children.is_empty() {
            return Err("session recovery must precede admission".into());
        }
        let sessions = self.store.list_sessions().map_err(|e| e.to_string())?;
        let roots: BTreeSet<_> = sessions
            .iter()
            .filter_map(|s| {
                s.managed
                    .as_ref()
                    .filter(|m| m.owner.is_some())
                    .map(|m| m.root_session_id.clone())
            })
            .collect();
        for mut session in sessions {
            let Some(managed) = &mut session.managed else {
                continue;
            };
            if !roots.contains(&managed.root_session_id) {
                continue;
            }
            if managed.archived
                || (managed.status != ManagedSessionStatus::Running && !managed.archived_at(now()))
            {
                continue;
            }
            if managed.status == ManagedSessionStatus::Running {
                managed.status = ManagedSessionStatus::Failed;
                managed.error = Some("session host restarted; work was not replayed".into());
                if let Some(active_op_id) = managed.operation_id.clone() {
                    let event = AgentEvent::new(
                        TaskId::generate(),
                        session.session_id.clone(),
                        AgentEventPayload::TurnInterrupted {
                            sequence: 0,
                            active_op_id,
                            reason: "session host restarted; work was not replayed".into(),
                        },
                    );
                    self.store
                        .append_record(&AgentSessionStoreRecord::new(
                            session.session_id.clone(),
                            AgentSessionStoreRecordKind::ControlEventPublished { event },
                        ))
                        .map_err(|e| e.to_string())?;
                }
            }
            if now() >= managed.last_activity_unix + managed.idle_timeout_secs as i64 {
                managed.archived = true;
            }
            self.store
                .save_session(&session)
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    pub fn shutdown(&self, deadline: Instant) -> Result<(), String> {
        let children: Vec<_> = {
            let mut state = self.lock();
            state.closing = true;
            state.children.values().cloned().collect()
        };
        for child in &children {
            let runtime = {
                let mut state = child.state.lock().unwrap();
                state.snapshot.archived = true;
                if state.pending && state.pending_operation_id.is_some() {
                    state.snapshot.status = ManagedSessionStatus::Cancelled;
                    state.snapshot.cancel_requested = true;
                    state.snapshot.last_activity_unix = now();
                }
                state.admission.cancel();
                state.runtime.clone()
            };
            if let Some(runtime) = runtime {
                runtime.cancel_detached();
                let _ = runtime.submit(AgentOp::cancel_active_turn("session host stopped"));
            }
            child.changed.notify_all();
        }
        for child in children {
            let mut state = child.state.lock().unwrap();
            while state.driver {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err("managed sessions are still draining".into());
                }
                state = child.changed.wait_timeout(state, remaining).unwrap().0;
            }
        }
        Ok(())
    }
    pub fn new(store: Arc<dyn AgentSessionStore>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SessionHostState::default()),
            store,
        })
    }

    /// Adopt a host-created root using the same driver and lifecycle as delegated sessions.
    pub fn start<M: InteractionModel + Send + Sync + 'static>(
        self: &Arc<Self>,
        owner: &str,
        mut agent: Agent<M>,
        input: AgentLoopInput,
        cancellation: AgentCancellationToken,
        idle_timeout_secs: u64,
        start_paused: bool,
    ) -> Result<ManagedSessionSnapshot, AgentError> {
        if self.lock().closing || cancellation.is_cancelled() {
            return Err(session_error("session admission stopped"));
        }
        let session_id = input.session.session_id.clone();
        let snapshot = ManagedSessionSnapshot {
            id: TaskId::generate(),
            parent_session_id: None,
            owner: Some(owner.into()),
            operation_id: Some(TaskId::generate()),
            session_id: session_id.clone(),
            root_session_id: session_id.clone(),
            name: agent.config.definition.name.clone(),
            task: input.latest_event.payload.to_string(),
            prompt: String::new(),
            status: ManagedSessionStatus::Running,
            cancel_requested: false,
            result: None,
            result_truncated: false,
            stop_reason: None,
            error: None,
            archived: false,
            last_activity_unix: now(),
            idle_timeout_secs: idle_timeout_secs.clamp(1, IDLE_SECONDS),
        };
        let mut session = AgentSession::new(session_id, agent.config.definition.role.clone());
        session.managed = Some(snapshot.clone());
        if start_paused {
            session.pause = Some(AgentPauseRecord::new("awaiting owner admission"));
        }
        session.workspace_binding = agent.config.run_context.workspace.clone();
        session.agent_definition_key = Some(agent.config.definition.key().to_string());
        agent.session_store = self.store.clone();
        agent.session_store_configured = true;
        agent.session_host = OnceLock::from(self.clone());
        let runtime = Arc::new(AgentSessionRuntime::new(agent, session.clone()));
        let child = Arc::new(Child {
            state: Mutex::new(ChildState {
                snapshot: snapshot.clone(),
                session: session.clone(),
                runtime: Some(runtime),
                input,
                pending: true,
                pending_operation_id: snapshot.operation_id.clone(),
                updates: VecDeque::new(),
                admission: cancellation,
                driver: false,
            }),
            changed: Condvar::new(),
        });
        {
            let mut state = self.lock();
            if state.closing {
                return Err(session_error("session host is stopping"));
            }
            self.store
                .save_session(&session)
                .map_err(|e| session_error(e.to_string()))?;
            state.children.insert(snapshot.id.clone(), child.clone());
            self.start_driver(child)?;
        }
        Ok(snapshot)
    }

    pub fn owned_roots(&self, owner: &str) -> Result<BTreeMap<String, String>, AgentError> {
        Ok(self
            .store
            .list_sessions()
            .map_err(|e| session_error(e.to_string()))?
            .into_iter()
            .filter_map(|s| {
                s.managed
                    .filter(|m| m.owner.as_deref() == Some(owner) && m.parent_session_id.is_none())
                    .map(|m| (m.session_id.to_string(), m.name))
            })
            .collect())
    }

    /// Validate a directly addressed session without scanning unrelated histories.
    pub fn owned_session(&self, owner: &str, id: &str) -> Result<AgentSession, AgentError> {
        let id = id
            .parse()
            .map_err(|_| session_error("invalid session id"))?;
        let session = self
            .store
            .load_session(&id)
            .map_err(|e| session_error(e.to_string()))?
            .ok_or_else(|| session_error("session unavailable"))?;
        let root_id = &session
            .managed
            .as_ref()
            .ok_or_else(|| session_error("unmanaged session"))?
            .root_session_id;
        let root = self
            .store
            .load_session(root_id)
            .map_err(|e| session_error(e.to_string()))?
            .ok_or_else(|| session_error("session root unavailable"))?;
        if !root
            .managed
            .as_ref()
            .is_some_and(|m| m.owner.as_deref() == Some(owner) && m.parent_session_id.is_none())
        {
            return Err(session_error("session outside module grant"));
        }
        Ok(session)
    }

    pub fn owned_command(
        self: &Arc<Self>,
        owner: &str,
        command: SessionCommand,
    ) -> Result<serde_json::Value, AgentError> {
        let id = match &command {
            SessionCommand::Inspect { id, .. }
            | SessionCommand::Message { id, .. }
            | SessionCommand::Wait { id, .. }
            | SessionCommand::Cancel { id }
            | SessionCommand::Archive { id }
            | SessionCommand::Result { id, .. }
            | SessionCommand::Pause { id }
            | SessionCommand::Resume { id } => id,
            _ => return Err(session_error("module commands require a session identity")),
        };
        let session = self.owned_session(owner, id)?;
        let root = session
            .managed
            .as_ref()
            .expect("owned session")
            .root_session_id
            .clone();
        if matches!(command, SessionCommand::Archive { .. })
            && !self.lock().parents.contains_key(&root)
        {
            let mut session = session;
            if let Some(managed) = &mut session.managed {
                managed.archived = true;
            }
            self.store
                .save_session(&session)
                .map_err(|e| session_error(e.to_string()))?;
            return Ok(
                serde_json::json!({"accepted":true,"session_id":session.session_id,"archived":true}),
            );
        }
        self.execute_command(&root, command, &AgentCancellationToken::new())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SessionHostState> {
        self.state.lock().expect("session registry poisoned")
    }
    pub(super) fn set_session(&self, session: &AgentSession) {
        if let Some(parent) = self.lock().parents.get_mut(&session.session_id) {
            if session.state_revision >= parent.session.state_revision {
                parent.session = session.clone();
            }
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
            .map_err(|e| session_error(e.to_string()))?;
        let factory = self.lock().parents.get(caller).unwrap().factory.clone();
        for mut session in sessions {
            let Some(mut snapshot) = session.managed.clone() else {
                continue;
            };
            if snapshot.root_session_id != root || self.lock().children.contains_key(&snapshot.id) {
                continue;
            }
            if snapshot.status == ManagedSessionStatus::Running {
                snapshot.status = ManagedSessionStatus::Failed;
                snapshot.error =
                    Some("execution interrupted by process restart; work was not replayed".into());
            }
            if now() >= snapshot.last_activity_unix + snapshot.idle_timeout_secs as i64 {
                snapshot.archived = true;
            }
            session.managed = Some(snapshot.clone());
            session.relationship = snapshot.parent_session_id.as_ref().map(|parent| {
                sinomo_runtime::module_api::sessions::SessionRelationship {
                    parent_session_id: parent.to_string(),
                    kind: "delegation".into(),
                }
            });
            self.store
                .save_session(&session)
                .map_err(|e| session_error(e.to_string()))?;
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
                    pending_operation_id: None,
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
            .ok_or_else(|| session_error("caller session is not live"))?;
        Ok(parent
            .session
            .managed
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
            .ok_or_else(|| session_error("unknown agent id"))?;
        if child.state.lock().unwrap().snapshot.root_session_id != root {
            return Err(session_error("agent belongs to another session tree"));
        }
        Ok(child)
    }
    pub fn spawn(
        self: &Arc<Self>,
        caller: &SessionId,
        name: &str,
        task: &str,
        cancellation: &AgentCancellationToken,
    ) -> Result<ManagedSessionSnapshot, AgentError> {
        self.spawn_with_prompt(caller, name, task, "", IDLE_SECONDS, false, cancellation)
    }
    #[allow(clippy::too_many_arguments)]
    fn spawn_with_prompt(
        self: &Arc<Self>,
        caller: &SessionId,
        name: &str,
        task: &str,
        prompt: &str,
        idle_timeout_secs: u64,
        start_paused: bool,
        cancellation: &AgentCancellationToken,
    ) -> Result<ManagedSessionSnapshot, AgentError> {
        if task.trim().is_empty()
            || task.len() > 128 * 1024
            || prompt.len() > 64 * 1024
            || name.len() > 1024
        {
            return Err(session_error("invalid child instructions, prompt or name"));
        }
        if cancellation.is_cancelled() {
            return Err(session_error("parent operation is cancelled"));
        }
        let root = self.root(caller)?;
        let (mut input, mut session, factory, inherited_prompt, cancellation_generation) = {
            let state = self.lock();
            let parent = state
                .parents
                .get(caller)
                .ok_or_else(|| session_error("parent session was released"))?;
            if parent.session.is_aborted() {
                return Err(session_error("parent session is aborted"));
            }
            (
                parent
                    .input
                    .clone()
                    .ok_or_else(|| session_error("parent session has no turn context"))?,
                inherited_session(&parent.session, SessionId::generate()),
                parent.factory.clone(),
                parent
                    .session
                    .managed
                    .as_ref()
                    .map(|s| s.prompt.clone())
                    .unwrap_or_default(),
                parent.cancellation_generation,
            )
        };
        let snapshot = ManagedSessionSnapshot {
            id: TaskId::generate(),
            parent_session_id: Some(caller.clone()),
            owner: None,
            operation_id: Some(TaskId::generate()),
            session_id: session.session_id.clone(),
            root_session_id: root,
            name: name.into(),
            task: task.into(),
            prompt: format!("{inherited_prompt}\n{prompt}"),
            status: ManagedSessionStatus::Running,
            cancel_requested: false,
            result: None,
            result_truncated: false,
            stop_reason: None,
            error: None,
            archived: false,
            last_activity_unix: now(),
            idle_timeout_secs: idle_timeout_secs.clamp(1, IDLE_SECONDS),
        };
        session.managed = Some(snapshot.clone());
        if start_paused {
            session.pause = Some(AgentPauseRecord::new("awaiting owner admission"));
        }
        session.relationship = snapshot.parent_session_id.as_ref().map(|parent| {
            sinomo_runtime::module_api::sessions::SessionRelationship {
                parent_session_id: parent.to_string(),
                kind: "delegation".into(),
            }
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
            .map_err(|e| session_error(e.to_string()))?;
        let runtime = factory(session.clone(), prompt)?;
        let child = Arc::new(Child {
            state: Mutex::new(ChildState {
                snapshot: snapshot.clone(),
                session,
                runtime: Some(runtime),
                input,
                pending: true,
                pending_operation_id: snapshot.operation_id.clone(),
                updates: VecDeque::new(),
                admission: cancellation.child_token(),
                driver: false,
            }),
            changed: Condvar::new(),
        });
        {
            let mut registry = self.lock();
            if registry.closing
                || registry.parents.get(caller).is_none_or(|parent| {
                    parent.cancellation_generation != cancellation_generation
                        || parent.session.is_aborted()
                })
            {
                drop(registry);
                let mut state = child.state.lock().unwrap();
                state.snapshot.status = ManagedSessionStatus::Cancelled;
                state.snapshot.archived = true;
                state.snapshot.cancel_requested = true;
                state.session.managed = Some(state.snapshot.clone());
                self.store
                    .save_session(&state.session)
                    .map_err(|e| session_error(e.to_string()))?;
                return Err(session_error("parent cancelled child admission"));
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
            return Err(session_error(e.to_string()));
        }
        Ok(())
    }
    fn drive(manager: Weak<Self>, child: Arc<Child>) {
        loop {
            let (runtime, input, admission, operation_id) = {
                let mut state = child.state.lock().unwrap();
                while !state.snapshot.archived
                    && (!state.pending
                        || state
                            .runtime
                            .as_ref()
                            .is_some_and(|runtime| runtime.session().is_ok_and(|s| s.is_paused())))
                {
                    if state.pending && state.admission.is_cancelled() {
                        state.pending = false;
                        state.pending_operation_id = None;
                        state.snapshot.status = ManagedSessionStatus::Cancelled;
                        state.snapshot.cancel_requested = true;
                        if let Some(runtime) = &state.runtime {
                            let _ = runtime.save_metadata(&state.snapshot);
                        }
                        child.changed.notify_all();
                    }
                    if let Some(runtime) = state.runtime.clone() {
                        state.updates.extend(runtime.updates());
                        if !state.updates.is_empty()
                            && !runtime.session().is_ok_and(|s| s.is_paused())
                        {
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
                    state.session.managed = Some(state.snapshot.clone());
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
                state.snapshot.status = ManagedSessionStatus::Running;
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
                    state.pending_operation_id.take(),
                )
            };
            let mut op = AgentOp::continue_turn(input);
            if let Some(id) = operation_id {
                op.op_id = id;
            }
            let id = op.op_id.clone();
            let metadata = {
                let mut state = child.state.lock().unwrap();
                state.snapshot.operation_id = Some(id.clone());
                state.snapshot.clone()
            };
            let result = (|| {
                runtime.save_metadata(&metadata)?;
                let accepted = runtime.start(op, admission.clone())?;
                if !accepted.accepted {
                    return Err(session_error("child turn admission rejected"));
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
                            ManagedSessionStatus::Cancelled
                        } else if state.snapshot.stop_reason
                            == Some(AgentStopReason::MaxStepsReached)
                        {
                            state.snapshot.error =
                                Some("subagent reached its step limit before completing".into());
                            ManagedSessionStatus::Failed
                        } else {
                            ManagedSessionStatus::Completed
                        };
                    }
                    Ok(None) => {
                        state.snapshot.status = ManagedSessionStatus::Failed;
                        state.snapshot.error = Some("child completed without a turn result".into());
                    }
                    Err(error) => {
                        state.snapshot.status = if state.snapshot.cancel_requested {
                            ManagedSessionStatus::Cancelled
                        } else {
                            ManagedSessionStatus::Failed
                        };
                        state.snapshot.error = Some(error.to_string());
                    }
                }
                // Reuse the core pending-work check: a completed detached call
                // still belongs to this worker until its continuation is consumed.
                let pending = runtime.pending_work();
                state.updates.extend(runtime.updates());
                if !state.updates.is_empty() {
                    state.pending = true;
                }
                if state.snapshot.status == ManagedSessionStatus::Completed
                    && (pending || state.pending || !state.updates.is_empty())
                {
                    state.snapshot.status = ManagedSessionStatus::Running;
                }
                if !state.session.follow_up_queue.is_empty()
                    || !state.session.steering_queue.is_empty()
                {
                    state.pending = true;
                }
                if let Err(error) = runtime.save_metadata(&state.snapshot) {
                    state.snapshot.error = Some(error.to_string());
                    state.snapshot.status = ManagedSessionStatus::Failed;
                }
                state.session.managed = Some(state.snapshot.clone());
                child.changed.notify_all();
                state.snapshot.clone()
            };
            if snapshot.status != ManagedSessionStatus::Running
                && let Some(manager) = manager.upgrade()
                && let Err(error) = manager.notify_parent(&snapshot)
            {
                child.state.lock().unwrap().snapshot.error =
                    Some(format!("parent notification failed: {error}"));
            }
        }
    }
    fn notify_parent(&self, snapshot: &ManagedSessionSnapshot) -> Result<(), AgentError> {
        let Some(parent_id) = &snapshot.parent_session_id else {
            return Ok(());
        };
        let update = AsyncTaskUpdate {
            kind: "agent.managed.completed".into(),
            task_id: TaskId::generate(),
            tool_call_id: None,
            status: match snapshot.status {
                ManagedSessionStatus::Completed => AsyncTaskStatus::Completed,
                ManagedSessionStatus::Cancelled => AsyncTaskStatus::Cancelled,
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
                parent_id.clone(),
                AgentSessionStoreRecordKind::DetachedCallStarted {
                    task_id: update.task_id.clone(),
                    tool_call_id: update.task_id.clone(),
                    tool: "agent.managed".into(),
                },
            ))
            .map_err(|e| session_error(e.to_string()))?;
        self.store
            .append_record(&AgentSessionStoreRecord::new(
                parent_id.clone(),
                AgentSessionStoreRecordKind::DetachedCallCompleted {
                    update: update.clone(),
                },
            ))
            .map_err(|e| session_error(e.to_string()))?;
        super::control::queue_session_update(parent_id, update.clone());
        let children: Vec<_> = self.lock().children.values().cloned().collect();
        for parent in children {
            let mut state = parent.state.lock().unwrap();
            if &state.snapshot.session_id == parent_id && !state.snapshot.archived {
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
    ) -> Result<ManagedSessionSnapshot, AgentError> {
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
    pub fn list(&self, caller: &SessionId) -> Vec<ManagedSessionSnapshot> {
        self.search(caller, false)
    }
    pub fn search(&self, caller: &SessionId, archived: bool) -> Vec<ManagedSessionSnapshot> {
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
    ) -> Result<ManagedSessionSnapshot, AgentError> {
        let child = self.child(caller, id)?;
        let deadline = Instant::now() + Duration::from_millis(timeout_ms.min(MAX_WAIT_MS));
        let mut state = child.state.lock().unwrap();
        while state.snapshot.status == ManagedSessionStatus::Running && !cancellation.is_cancelled()
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
    fn control(
        &self,
        caller: &SessionId,
        id: &TaskId,
        op: AgentOp,
    ) -> Result<ManagedSessionSnapshot, AgentError> {
        let child = self.child(caller, id)?;
        let runtime = {
            let state = child.state.lock().unwrap();
            if state
                .snapshot
                .parent_session_id
                .as_ref()
                .unwrap_or(&state.snapshot.session_id)
                != caller
            {
                return Err(session_error("only the owner can control a session"));
            }
            state
                .runtime
                .clone()
                .ok_or_else(|| session_error("session is archived"))?
        };
        let result = runtime.submit(op)?;
        if !result.accepted {
            return Err(session_error("runtime rejected session control"));
        }
        child.changed.notify_all();
        self.inspect(caller, id)
    }
    pub fn cancel(
        &self,
        caller: &SessionId,
        id: &TaskId,
    ) -> Result<ManagedSessionSnapshot, AgentError> {
        let child = self.child(caller, id)?;
        let runtime = {
            let mut state = child.state.lock().unwrap();
            if state
                .snapshot
                .parent_session_id
                .as_ref()
                .unwrap_or(&state.snapshot.session_id)
                != caller
            {
                return Err(session_error("only the parent can cancel a child"));
            }
            state.snapshot.cancel_requested = true;
            state.admission.cancel();
            state.runtime.clone()
        };
        let propagated = if let Some(runtime) = runtime {
            runtime.cancel_detached();
            runtime
                .submit(AgentOp::cancel_active_turn("parent cancelled child"))?
                .accepted
        } else {
            false
        };
        if !propagated {
            // A retained idle parent can still own running descendants.
            let session_id = child.state.lock().unwrap().snapshot.session_id.clone();
            self.cancel_children(&session_id);
        }
        let mut state = child.state.lock().unwrap();
        if state.pending && state.pending_operation_id.is_some() {
            state.pending = false;
            state.pending_operation_id = None;
            state.snapshot.status = ManagedSessionStatus::Cancelled;
            state.snapshot.last_activity_unix = now();
            if let Some(runtime) = &state.runtime {
                runtime.save_metadata(&state.snapshot)?;
            }
        }
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
            state.session.managed = Some(state.snapshot.clone());
            self.store
                .save_session(&state.session)
                .map_err(|e| session_error(e.to_string()))?;
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
            return Err(session_error("invalid message"));
        }
        let parent_target = {
            let state = self.lock();
            let parent = state
                .parents
                .get(caller)
                .ok_or_else(|| session_error("caller session is not live"))?;
            if id == "parent" {
                parent
                    .session
                    .managed
                    .as_ref()
                    .and_then(|s| s.parent_session_id.clone())
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
                .and_then(|p| p.session.managed.as_ref())
                .map(|s| s.id.clone());
            if let Some(id) = parent_id {
                return self.message(caller, id.as_str(), message);
            }
            let send = self
                .lock()
                .parents
                .get(&target)
                .and_then(|p| p.message.clone())
                .ok_or_else(|| session_error("parent session is unavailable"))?;
            if send(&text)? {
                return Ok(());
            }
            let mut session = self
                .store
                .load_session(&target)
                .map_err(|e| session_error(e.to_string()))?
                .ok_or_else(|| session_error("parent session is unavailable"))?;
            session
                .follow_up_queue
                .push(AgentFollowUpMessage::new(text));
            session.state_revision = session.state_revision.saturating_add(1);
            self.store
                .save_session(&session)
                .map_err(|e| session_error(e.to_string()))?;
            return Ok(());
        }
        let id: TaskId = id.parse().map_err(|_| session_error("invalid agent id"))?;
        let child = self.child(caller, &id)?;
        let runtime = {
            let state = child.state.lock().unwrap();
            if state.snapshot.archived {
                return Err(session_error("agent is archived"));
            }
            state
                .runtime
                .clone()
                .ok_or_else(|| session_error("agent runtime is unavailable"))?
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
                return Err(session_error("agent archived during message admission"));
            }
            if state.snapshot.status != ManagedSessionStatus::Running {
                state.admission = AgentCancellationToken::new();
            }
            state.pending = true;
            state.snapshot.status = ManagedSessionStatus::Running;
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
            if child.parent_session_id.as_ref() == Some(caller) {
                let _ = self.cancel(caller, &child.id);
            }
        }
    }
    fn set_children_paused(&self, caller: &SessionId, paused: bool) {
        let children: Vec<_> = self.lock().children.values().cloned().collect();
        for child in children {
            let runtime = {
                let state = child.state.lock().unwrap();
                (state.snapshot.parent_session_id.as_ref() == Some(caller))
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
fn session_error(detail: impl Into<String>) -> AgentError {
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

pub(super) struct AgentSessionScope {
    pub(super) manager: Arc<AgentSessionHost>,
    session_id: Mutex<SessionId>,
}
impl AgentSessionScope {
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
            session_host: self
                .session_host
                .get()
                .cloned()
                .map(OnceLock::from)
                .unwrap_or_default(),
            session_tools: self.session_tools.clone(),
            item_journals: Mutex::new(BTreeMap::new()),
        }
    }
    pub(super) fn session_scope(self: &Arc<Self>, session: AgentSession) -> Arc<AgentSessionScope> {
        type RegistryKey = (PathBuf, SessionId);
        static REGISTRIES: OnceLock<Mutex<BTreeMap<RegistryKey, Weak<AgentSessionHost>>>> =
            OnceLock::new();
        let root_id = session
            .managed
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
            .session_host
            .get_or_init(|| {
                key.as_ref()
                    .and_then(|key| registries.get(key))
                    .and_then(Weak::upgrade)
                    .unwrap_or_else(|| {
                        Arc::new(AgentSessionHost {
                            state: Mutex::new(SessionHostState::default()),
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
                    .ok_or_else(|| session_error("parent agent was released"))?;
                let mut child = parent.fork_subagent();
                child
                    .config
                    .definition
                    .system_prompt
                    .push_str(&format!("\n\n{prompt}"));
                if let Some(snapshot) = &session.managed {
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
                            .ok_or_else(|| session_error("session runtime released"))?
                            .session_access(caller, cancellation)
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
        Arc::new(AgentSessionScope {
            manager,
            session_id: Mutex::new(id),
        })
    }
}

impl<M: InteractionModel + Send + Sync + 'static> AgentSessionRuntime<M> {
    pub(super) fn bind_session_messages(&self) {
        let weak_state = Arc::downgrade(&self.state);
        let weak_agent = Arc::downgrade(&self.agent);
        let weak_scope = Arc::downgrade(&self.session_scope);
        let message = Arc::new(move |text: &str| {
            let (Some(state), Some(agent), Some(session_scope)) = (
                weak_state.upgrade(),
                weak_agent.upgrade(),
                weak_scope.upgrade(),
            ) else {
                return Ok(false);
            };
            let runtime = AgentSessionRuntime {
                state,
                agent,
                session_scope,
            };
            runtime.submit_op(AgentOp::follow_up(text))?;
            Ok(true)
        });
        let weak_state = Arc::downgrade(&self.state);
        let inspect = Arc::new(move || {
            let state = weak_state
                .upgrade()
                .ok_or_else(|| session_error("parent session detached"))?;
            let session = state
                .lock()
                .map_err(|_| session_error("parent session poisoned"))?
                .control
                .session()
                .clone();
            Ok(session)
        });
        let id = self.session_snapshot().expect("session").session_id;
        if let Some(parent) = self.sessions().lock().parents.get_mut(&id) {
            parent.message = Some(message);
            parent.inspect = Some(inspect);
        }
    }
}

impl AgentSessionHost {
    pub fn session_access(
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

    pub(crate) fn execute_command(
        self: &Arc<Self>,
        caller: &SessionId,
        command: SessionCommand,
        cancellation: &AgentCancellationToken,
    ) -> Result<serde_json::Value, AgentError> {
        let parse = |id: String| -> Result<TaskId, AgentError> {
            let id = id
                .parse::<TaskId>()
                .map_err(|_| session_error("invalid session or agent id"))?;
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
                start_paused,
                worker: None,
            } => self.spawn_with_prompt(
                caller,
                &name,
                &instructions,
                &prompt,
                idle_timeout_secs,
                start_paused,
                cancellation,
            )?,
            SessionCommand::Create {
                worker: Some(_), ..
            } => {
                return Err(session_error(
                    "worker profiles require a module-owned session grant",
                ));
            }
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
                            .and_then(|p| p.session.managed.as_ref())
                            .and_then(|s| s.parent_session_id.as_ref())
                            .ok_or_else(|| session_error("no parent"))?;
                        state
                            .parents
                            .get(parent_id)
                            .and_then(|p| p.inspect.clone())
                            .ok_or_else(|| session_error("parent unavailable"))?
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
                    .and_then(|parent| parent.session.managed.as_ref())
                    .and_then(|child| child.parent_session_id.as_ref().map(ToString::to_string));
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
            SessionCommand::Result { id, operation_id } => {
                let child = self.child(caller, &parse(id)?)?;
                let state = child.state.lock().unwrap();
                let operation_id = operation_id
                    .parse::<TaskId>()
                    .map_err(|_| session_error("invalid operation id"))?;
                let result = state
                    .runtime
                    .as_ref()
                    .map(|r| r.result(&operation_id))
                    .transpose()?
                    .flatten();
                return serde_json::to_value(result).map_err(|e| session_error(e.to_string()));
            }
            SessionCommand::Pause { id } => {
                self.control(caller, &parse(id)?, AgentOp::pause("owner paused session"))?
            }
            SessionCommand::Resume { id } => {
                self.control(caller, &parse(id)?, AgentOp::unpause())?
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
            serde_json::json!({"accepted":true,"session_id":snapshot.session_id,"operation_id":snapshot.operation_id,"error":snapshot.error,"status":snapshot.status,"archived":snapshot.archived,"subagent":snapshot}),
        )
    }
}
