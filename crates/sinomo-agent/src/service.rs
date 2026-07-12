use crate::{
    AgentError, AgentEvent, AgentOp, AgentSession, AgentSessionControlEventBatch,
    AgentSessionControlEventCursor, AgentSessionControlResult, AgentSessionControlSnapshot,
    AgentSessionRuntime, AgentSessionStore, AgentSessionStoreError, AgentTaskLifecycleStatus,
    AgentTaskSnapshot, AgentTaskTerminalResult, FileAgentSessionStore, InteractionModel, TaskId,
    agent_session_control_event_batch_from_events,
};
use serde::{Deserialize, Serialize};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::network::{ObjectRpcRequest, ObjectRpcResponse};
use sinomo_runtime::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

pub const AGENT_RPC_CATEGORY: &str = "agent";
pub const AGENT_SESSION_INSPECT_ROUTE: &str = "/agent/session/inspect";
pub const AGENT_SESSION_PROGRESS_ROUTE: &str = "/agent/session/progress";
pub const AGENT_SESSION_TERMINAL_RESULTS_ROUTE: &str = "/agent/session/terminal-results";
pub const AGENT_SESSION_STEER_ROUTE: &str = "/agent/session/steer";
pub const AGENT_SESSION_FOLLOW_UP_ROUTE: &str = "/agent/session/follow-up";
pub const AGENT_SESSION_REMOVE_QUEUED_INPUT_ROUTE: &str = "/agent/session/remove-queued-input";
pub const AGENT_SESSION_INTERRUPT_ROUTE: &str = "/agent/session/interrupt";
pub const AGENT_SESSION_RESUME_ROUTE: &str = "/agent/session/resume";
pub const AGENT_OBJECT_RPC_ROUTES: &[&str] = &[
    AGENT_SESSION_INSPECT_ROUTE,
    AGENT_SESSION_PROGRESS_ROUTE,
    AGENT_SESSION_TERMINAL_RESULTS_ROUTE,
];
pub const AGENT_RUNTIME_CONTROL_OBJECT_RPC_ROUTES: &[&str] = &[
    AGENT_SESSION_STEER_ROUTE,
    AGENT_SESSION_FOLLOW_UP_ROUTE,
    AGENT_SESSION_REMOVE_QUEUED_INPUT_ROUTE,
    AGENT_SESSION_INTERRUPT_ROUTE,
    AGENT_SESSION_RESUME_ROUTE,
];

const AGENT_READ_ROUTE_AUTH_SCOPE: &str = "local-or-cluster-member";
const AGENT_CONTROL_ROUTE_AUTH_SCOPE: &str = "local-or-cluster-member";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSessionInspectRequest {
    pub session_id: SessionId,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionInspectResponse {
    pub session_id: SessionId,
    pub session: Option<AgentSession>,
    pub control_snapshot: Option<AgentSessionControlSnapshot>,
    pub event_cursor: AgentSessionControlEventCursor,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub terminal_results: Vec<AgentTaskTerminalResult>,
    pub persisted_event_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSessionProgressRequest {
    pub session_id: SessionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<AgentSessionControlEventCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionProgressResponse {
    pub session_id: SessionId,
    pub batch: AgentSessionControlEventBatch,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSessionTerminalResultsRequest {
    pub session_id: SessionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<TaskId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionTerminalResultsResponse {
    pub session_id: SessionId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub terminal_results: Vec<AgentTaskTerminalResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSessionSteerRequest {
    pub session_id: SessionId,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSessionFollowUpRequest {
    pub session_id: SessionId,
    pub message: String,
}

/// Remove a queued steering/follow-up message (by its queue id) before the
/// runtime consumes it.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSessionRemoveQueuedInputRequest {
    pub session_id: SessionId,
    pub input_id: TaskId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSessionInterruptRequest {
    pub session_id: SessionId,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSessionResumeRequest {
    pub session_id: SessionId,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionControlResponse {
    pub session_id: SessionId,
    pub result: AgentSessionControlResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionResumeResponse {
    pub session_id: SessionId,
    pub accepted: bool,
    #[serde(default)]
    pub runtime_attached: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<AgentSession>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_snapshot: Option<AgentSessionControlSnapshot>,
    pub event_cursor: AgentSessionControlEventCursor,
    #[serde(default)]
    pub reattach: AgentSessionReattachReport,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentSessionReattachState {
    LiveRuntimeAttached,
    PersistedMetadataOnly,
    Missing,
}

impl Default for AgentSessionReattachState {
    fn default() -> Self {
        Self::Missing
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentSessionReattachReport {
    pub state: AgentSessionReattachState,
    pub active_turn_persisted: bool,
    pub running_task_count: usize,
    pub persisted_event_count: usize,
    pub reason: String,
}

impl AgentSessionReattachReport {
    pub fn live_runtime_attached(
        snapshot: &AgentSessionControlSnapshot,
        persisted_event_count: usize,
    ) -> Self {
        Self {
            state: AgentSessionReattachState::LiveRuntimeAttached,
            active_turn_persisted: snapshot.active_turn.is_some(),
            running_task_count: running_task_count(snapshot),
            persisted_event_count,
            reason: "live runtime attached".to_string(),
        }
    }

    pub fn persisted_metadata_only(
        snapshot: &Option<AgentSessionControlSnapshot>,
        persisted_event_count: usize,
        live_error: impl Into<String>,
    ) -> Self {
        Self {
            state: AgentSessionReattachState::PersistedMetadataOnly,
            active_turn_persisted: snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.active_turn.is_some()),
            running_task_count: snapshot.as_ref().map(running_task_count).unwrap_or(0),
            persisted_event_count,
            reason: format!(
                "live runtime unavailable; in-flight task handles are host-owned and cannot be reattached from persisted metadata alone: {}",
                live_error.into()
            ),
        }
    }

    pub fn missing(live_error: impl Into<String>) -> Self {
        Self {
            state: AgentSessionReattachState::Missing,
            active_turn_persisted: false,
            running_task_count: 0,
            persisted_event_count: 0,
            reason: format!(
                "live runtime unavailable and no persisted session metadata was found: {}",
                live_error.into()
            ),
        }
    }
}

impl Default for AgentSessionReattachReport {
    fn default() -> Self {
        Self {
            state: AgentSessionReattachState::Missing,
            active_turn_persisted: false,
            running_task_count: 0,
            persisted_event_count: 0,
            reason: "reattach report not recorded".to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentRuntimeControlError {
    detail: String,
}

impl AgentRuntimeControlError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for AgentRuntimeControlError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for AgentRuntimeControlError {}

pub trait AgentRuntimeControlHost: Send + Sync {
    fn steer(
        &self,
        request: AgentSessionSteerRequest,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError>;

    fn follow_up(
        &self,
        request: AgentSessionFollowUpRequest,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError>;

    fn remove_queued_input(
        &self,
        request: AgentSessionRemoveQueuedInputRequest,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError>;

    fn interrupt(
        &self,
        request: AgentSessionInterruptRequest,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError>;

    fn resume(
        &self,
        request: AgentSessionResumeRequest,
    ) -> Result<AgentSessionResumeResponse, AgentRuntimeControlError>;
}

pub trait AgentRuntimeControlSession: Send + Sync {
    fn session_id(&self) -> Result<SessionId, AgentRuntimeControlError>;

    fn steer(&self, message: String)
    -> Result<AgentSessionControlResult, AgentRuntimeControlError>;

    fn follow_up(
        &self,
        message: String,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError>;

    fn remove_queued_input(
        &self,
        input_id: TaskId,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError>;

    fn interrupt(
        &self,
        reason: String,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError>;

    fn resume(&self) -> Result<AgentSessionResumeResponse, AgentRuntimeControlError>;
}

impl<M> AgentRuntimeControlSession for AgentSessionRuntime<M>
where
    M: InteractionModel + Send + Sync + 'static,
{
    fn session_id(&self) -> Result<SessionId, AgentRuntimeControlError> {
        self.session_snapshot()
            .map(|session| session.session_id)
            .map_err(agent_runtime_error)
    }

    fn steer(
        &self,
        message: String,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
        self.submit_op(AgentOp::steer(message))
            .map_err(agent_runtime_error)
    }

    fn follow_up(
        &self,
        message: String,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
        self.submit_op(AgentOp::follow_up(message))
            .map_err(agent_runtime_error)
    }

    fn remove_queued_input(
        &self,
        input_id: TaskId,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
        self.submit_op(AgentOp::remove_queued_input(input_id))
            .map_err(agent_runtime_error)
    }

    fn interrupt(
        &self,
        reason: String,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
        self.interrupt_active_turn(reason)
            .map_err(agent_runtime_error)
    }

    fn resume(&self) -> Result<AgentSessionResumeResponse, AgentRuntimeControlError> {
        let session = self.session_snapshot().map_err(agent_runtime_error)?;
        let session_id = session.session_id.clone();
        let control_snapshot = self.control_snapshot().map_err(agent_runtime_error)?;
        let event_cursor = self.event_cursor().map_err(agent_runtime_error)?;
        let reattach = AgentSessionReattachReport::live_runtime_attached(
            &control_snapshot,
            event_cursor.offset,
        );
        Ok(AgentSessionResumeResponse {
            session_id,
            accepted: true,
            runtime_attached: true,
            session: Some(session),
            control_snapshot: Some(control_snapshot),
            event_cursor,
            reattach,
            detail: None,
        })
    }
}

#[derive(Clone, Default)]
pub struct AgentRuntimeControlRegistry {
    sessions: Arc<Mutex<BTreeMap<SessionId, Arc<dyn AgentRuntimeControlSession>>>>,
}

impl AgentRuntimeControlRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_session(
        &self,
        session: Arc<dyn AgentRuntimeControlSession>,
    ) -> Result<SessionId, AgentRuntimeControlError> {
        let session_id = session.session_id()?;
        self.sessions
            .lock()
            .map_err(|_| AgentRuntimeControlError::new("agent runtime registry lock poisoned"))?
            .insert(session_id.clone(), session);
        Ok(session_id)
    }

    pub fn register_runtime<M>(
        &self,
        runtime: AgentSessionRuntime<M>,
    ) -> Result<SessionId, AgentRuntimeControlError>
    where
        M: InteractionModel + Send + Sync + 'static,
    {
        self.register_session(Arc::new(runtime))
    }

    pub fn unregister_session(
        &self,
        session_id: &SessionId,
    ) -> Result<bool, AgentRuntimeControlError> {
        self.sessions
            .lock()
            .map_err(|_| AgentRuntimeControlError::new("agent runtime registry lock poisoned"))
            .map(|mut sessions| sessions.remove(session_id).is_some())
    }

    pub fn contains_session(
        &self,
        session_id: &SessionId,
    ) -> Result<bool, AgentRuntimeControlError> {
        self.sessions
            .lock()
            .map_err(|_| AgentRuntimeControlError::new("agent runtime registry lock poisoned"))
            .map(|sessions| sessions.contains_key(session_id))
    }

    pub fn session_count(&self) -> Result<usize, AgentRuntimeControlError> {
        self.sessions
            .lock()
            .map_err(|_| AgentRuntimeControlError::new("agent runtime registry lock poisoned"))
            .map(|sessions| sessions.len())
    }

    fn session(
        &self,
        session_id: &SessionId,
    ) -> Result<Arc<dyn AgentRuntimeControlSession>, AgentRuntimeControlError> {
        self.sessions
            .lock()
            .map_err(|_| AgentRuntimeControlError::new("agent runtime registry lock poisoned"))?
            .get(session_id)
            .cloned()
            .ok_or_else(|| {
                AgentRuntimeControlError::new(format!(
                    "agent session {session_id} is not registered with this runtime host"
                ))
            })
    }
}

impl AgentRuntimeControlHost for AgentRuntimeControlRegistry {
    fn steer(
        &self,
        request: AgentSessionSteerRequest,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
        self.session(&request.session_id)?.steer(request.message)
    }

    fn follow_up(
        &self,
        request: AgentSessionFollowUpRequest,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
        self.session(&request.session_id)?
            .follow_up(request.message)
    }

    fn remove_queued_input(
        &self,
        request: AgentSessionRemoveQueuedInputRequest,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
        self.session(&request.session_id)?
            .remove_queued_input(request.input_id)
    }

    fn interrupt(
        &self,
        request: AgentSessionInterruptRequest,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
        self.session(&request.session_id)?.interrupt(request.reason)
    }

    fn resume(
        &self,
        request: AgentSessionResumeRequest,
    ) -> Result<AgentSessionResumeResponse, AgentRuntimeControlError> {
        self.session(&request.session_id)?.resume()
    }
}

#[derive(Default)]
pub struct AgentRuntimeService;

impl AgentRuntimeService {
    pub fn new() -> Self {
        Self
    }
}

pub fn register_services(builder: ServiceRegistryBuilder) -> ServiceRegistryBuilder {
    builder.register(AgentRuntimeService::new())
}

pub fn register_runtime_control_services(
    builder: ServiceRegistryBuilder,
    host: std::sync::Arc<dyn AgentRuntimeControlHost>,
) -> ServiceRegistryBuilder {
    builder.register(AgentRuntimeControlService::new(host))
}

pub fn register_services_with_runtime_control(
    builder: ServiceRegistryBuilder,
    host: std::sync::Arc<dyn AgentRuntimeControlHost>,
) -> ServiceRegistryBuilder {
    register_runtime_control_services(register_services(builder), host)
}

impl Service for AgentRuntimeService {
    fn name(&self) -> &'static str {
        "agent-runtime"
    }

    fn routes(&self) -> &'static [&'static str] {
        AGENT_OBJECT_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        AGENT_OBJECT_RPC_ROUTES
            .iter()
            .copied()
            .map(agent_service_route)
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        Box::pin(async move {
            let store = FileAgentSessionStore::for_data_dir(ctx.data_dir.as_ref());
            let response = match request.route.as_str() {
                AGENT_SESSION_INSPECT_ROUTE => {
                    let request: AgentSessionInspectRequest = request.decode_body()?;
                    let response = inspect_session(&store, request)?;
                    ObjectRpcResponse::encode_body(AGENT_SESSION_INSPECT_ROUTE, &response)?
                }
                AGENT_SESSION_PROGRESS_ROUTE => {
                    let request: AgentSessionProgressRequest = request.decode_body()?;
                    let response = session_progress(&store, request)?;
                    ObjectRpcResponse::encode_body(AGENT_SESSION_PROGRESS_ROUTE, &response)?
                }
                AGENT_SESSION_TERMINAL_RESULTS_ROUTE => {
                    let request: AgentSessionTerminalResultsRequest = request.decode_body()?;
                    let response = terminal_results(&store, request)?;
                    ObjectRpcResponse::encode_body(AGENT_SESSION_TERMINAL_RESULTS_ROUTE, &response)?
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub struct AgentRuntimeControlService {
    host: std::sync::Arc<dyn AgentRuntimeControlHost>,
}

impl AgentRuntimeControlService {
    pub fn new(host: std::sync::Arc<dyn AgentRuntimeControlHost>) -> Self {
        Self { host }
    }
}

impl Service for AgentRuntimeControlService {
    fn name(&self) -> &'static str {
        "agent-runtime-control"
    }

    fn routes(&self) -> &'static [&'static str] {
        AGENT_RUNTIME_CONTROL_OBJECT_RPC_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        AGENT_RUNTIME_CONTROL_OBJECT_RPC_ROUTES
            .iter()
            .copied()
            .map(agent_control_service_route)
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let host = std::sync::Arc::clone(&self.host);
        Box::pin(async move {
            let response = match request.route.as_str() {
                AGENT_SESSION_STEER_ROUTE => {
                    let request: AgentSessionSteerRequest = request.decode_body()?;
                    let session_id = request.session_id.clone();
                    let result = host.steer(request).map_err(agent_control_error)?;
                    ObjectRpcResponse::encode_body(
                        AGENT_SESSION_STEER_ROUTE,
                        &AgentSessionControlResponse { session_id, result },
                    )?
                }
                AGENT_SESSION_FOLLOW_UP_ROUTE => {
                    let request: AgentSessionFollowUpRequest = request.decode_body()?;
                    let session_id = request.session_id.clone();
                    let result = host.follow_up(request).map_err(agent_control_error)?;
                    ObjectRpcResponse::encode_body(
                        AGENT_SESSION_FOLLOW_UP_ROUTE,
                        &AgentSessionControlResponse { session_id, result },
                    )?
                }
                AGENT_SESSION_REMOVE_QUEUED_INPUT_ROUTE => {
                    let request: AgentSessionRemoveQueuedInputRequest = request.decode_body()?;
                    let session_id = request.session_id.clone();
                    let result = host
                        .remove_queued_input(request)
                        .map_err(agent_control_error)?;
                    ObjectRpcResponse::encode_body(
                        AGENT_SESSION_REMOVE_QUEUED_INPUT_ROUTE,
                        &AgentSessionControlResponse { session_id, result },
                    )?
                }
                AGENT_SESSION_INTERRUPT_ROUTE => {
                    let request: AgentSessionInterruptRequest = request.decode_body()?;
                    let session_id = request.session_id.clone();
                    let result = host.interrupt(request).map_err(agent_control_error)?;
                    ObjectRpcResponse::encode_body(
                        AGENT_SESSION_INTERRUPT_ROUTE,
                        &AgentSessionControlResponse { session_id, result },
                    )?
                }
                AGENT_SESSION_RESUME_ROUTE => {
                    let request: AgentSessionResumeRequest = request.decode_body()?;
                    let response = match host.resume(request.clone()) {
                        Ok(response) => response,
                        Err(error) => {
                            let store = FileAgentSessionStore::for_data_dir(ctx.data_dir.as_ref());
                            resume_session_from_store(&store, request, error.to_string())?
                        }
                    };
                    ObjectRpcResponse::encode_body(AGENT_SESSION_RESUME_ROUTE, &response)?
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            };
            ctx.respond(response)?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

fn inspect_session(
    store: &FileAgentSessionStore,
    request: AgentSessionInspectRequest,
) -> Result<AgentSessionInspectResponse, ServiceError> {
    let session = store
        .load_session(&request.session_id)
        .map_err(agent_store_error)?;
    let control_snapshot = store
        .load_control_snapshot(&request.session_id)
        .map_err(agent_store_error)?;
    let events = store
        .load_control_events(&request.session_id)
        .map_err(agent_store_error)?;
    let terminal_results = terminal_results_from_snapshot_or_events(&control_snapshot, &events);
    Ok(AgentSessionInspectResponse {
        session_id: request.session_id,
        session,
        control_snapshot,
        event_cursor: event_cursor_for_events(&events),
        terminal_results,
        persisted_event_count: events.len(),
    })
}

fn session_progress(
    store: &FileAgentSessionStore,
    request: AgentSessionProgressRequest,
) -> Result<AgentSessionProgressResponse, ServiceError> {
    let events = store
        .load_control_events(&request.session_id)
        .map_err(agent_store_error)?;
    let cursor = request.cursor.unwrap_or(AgentSessionControlEventCursor {
        offset: 0,
        last_event_id: None,
    });
    let batch = agent_session_control_event_batch_from_events(cursor, &events);
    Ok(AgentSessionProgressResponse {
        session_id: request.session_id,
        batch,
    })
}

fn terminal_results(
    store: &FileAgentSessionStore,
    request: AgentSessionTerminalResultsRequest,
) -> Result<AgentSessionTerminalResultsResponse, ServiceError> {
    let snapshot = store
        .load_control_snapshot(&request.session_id)
        .map_err(agent_store_error)?;
    let events = store
        .load_control_events(&request.session_id)
        .map_err(agent_store_error)?;
    let mut terminal_results = terminal_results_from_snapshot_or_events(&snapshot, &events);
    if let Some(task_id) = request.task_id {
        terminal_results.retain(|result| result.task.task_id == task_id);
    }
    Ok(AgentSessionTerminalResultsResponse {
        session_id: request.session_id,
        terminal_results,
    })
}

pub(crate) fn resume_session_from_store(
    store: &FileAgentSessionStore,
    request: AgentSessionResumeRequest,
    live_error: String,
) -> Result<AgentSessionResumeResponse, ServiceError> {
    let session = store
        .load_session(&request.session_id)
        .map_err(agent_store_error)?;
    let control_snapshot = store
        .load_control_snapshot(&request.session_id)
        .map_err(agent_store_error)?;
    let events = store
        .load_control_events(&request.session_id)
        .map_err(agent_store_error)?;
    let has_persisted_metadata =
        session.is_some() || control_snapshot.is_some() || !events.is_empty();
    let reattach = if has_persisted_metadata {
        AgentSessionReattachReport::persisted_metadata_only(
            &control_snapshot,
            events.len(),
            live_error.clone(),
        )
    } else {
        AgentSessionReattachReport::missing(live_error.clone())
    };
    let detail = if has_persisted_metadata {
        format!("live runtime unavailable; returning persisted session metadata: {live_error}")
    } else {
        format!(
            "live runtime unavailable and no persisted session metadata was found: {live_error}"
        )
    };
    Ok(AgentSessionResumeResponse {
        session_id: request.session_id,
        accepted: false,
        runtime_attached: false,
        session,
        control_snapshot,
        event_cursor: event_cursor_for_events(&events),
        reattach,
        detail: Some(detail),
    })
}

fn terminal_results_from_snapshot_or_events(
    snapshot: &Option<AgentSessionControlSnapshot>,
    events: &[AgentEvent],
) -> Vec<AgentTaskTerminalResult> {
    let mut terminal_results = snapshot
        .as_ref()
        .map(|snapshot| terminal_results_from_tasks(&snapshot.tasks))
        .unwrap_or_default();
    let event_terminal_results = agent_session_control_event_batch_from_events(
        AgentSessionControlEventCursor {
            offset: 0,
            last_event_id: None,
        },
        events,
    )
    .terminal_results;

    for event_result in event_terminal_results {
        let task_is_in_snapshot = snapshot.as_ref().is_some_and(|snapshot| {
            snapshot
                .tasks
                .iter()
                .any(|task| task.task.task_id == event_result.task.task_id)
        });
        let task_is_already_reported = terminal_results
            .iter()
            .any(|result| result.task.task_id == event_result.task.task_id);
        if !task_is_in_snapshot && !task_is_already_reported {
            terminal_results.push(event_result);
        }
    }
    terminal_results
}

fn terminal_results_from_tasks(tasks: &[AgentTaskSnapshot]) -> Vec<AgentTaskTerminalResult> {
    tasks
        .iter()
        .filter_map(AgentTaskTerminalResult::from_snapshot)
        .collect()
}

fn running_task_count(snapshot: &AgentSessionControlSnapshot) -> usize {
    snapshot
        .tasks
        .iter()
        .filter(|task| task.status == AgentTaskLifecycleStatus::Running)
        .count()
}

fn event_cursor_for_events(events: &[AgentEvent]) -> AgentSessionControlEventCursor {
    AgentSessionControlEventCursor {
        offset: events.len(),
        last_event_id: events.last().map(|event| event.event_id.clone()),
    }
}

fn agent_service_route(route: &'static str) -> ServiceRoute {
    let capability = match route {
        AGENT_SESSION_INSPECT_ROUTE => "agent.session.inspect",
        AGENT_SESSION_PROGRESS_ROUTE => "agent.session.progress.read",
        AGENT_SESSION_TERMINAL_RESULTS_ROUTE => "agent.session.results.read",
        _ => "agent.session.read",
    };
    ServiceRoute::new(route, AGENT_RPC_CATEGORY)
        .with_auth_scope(AGENT_READ_ROUTE_AUTH_SCOPE)
        .require_capability(capability)
        .with_concurrency_limit(8)
}

fn agent_control_service_route(route: &'static str) -> ServiceRoute {
    let capability = match route {
        AGENT_SESSION_STEER_ROUTE => "agent.session.steer",
        AGENT_SESSION_FOLLOW_UP_ROUTE => "agent.session.follow_up",
        AGENT_SESSION_REMOVE_QUEUED_INPUT_ROUTE => "agent.session.remove_queued_input",
        AGENT_SESSION_INTERRUPT_ROUTE => "agent.session.interrupt",
        AGENT_SESSION_RESUME_ROUTE => "agent.session.resume",
        _ => "agent.session.control",
    };
    ServiceRoute::new(route, AGENT_RPC_CATEGORY)
        .with_auth_scope(AGENT_CONTROL_ROUTE_AUTH_SCOPE)
        .require_capability(capability)
        .with_concurrency_limit(4)
}

fn agent_store_error(error: AgentSessionStoreError) -> ServiceError {
    ServiceError::Service(format!("agent session store: {error}"))
}

fn agent_control_error(error: AgentRuntimeControlError) -> ServiceError {
    ServiceError::Service(format!("agent runtime control: {error}"))
}

fn agent_runtime_error(error: AgentError) -> AgentRuntimeControlError {
    AgentRuntimeControlError::new(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Agent, AgentEventPayload, AgentRole, AgentSession, AgentSessionRuntime,
        AgentSessionStoreRecord, AgentSessionStoreRecordKind, AgentTaskLifecycleStatus,
        AgentTaskRef, AgentTaskSnapshot,
    };
    use sinomo_lm::{LanguageModelError, LanguageModelRequest, LanguageModelResponse};
    use sinomo_runtime::local::SinomoDataDir;
    use sinomo_runtime::network::{ObjectRpcIncomingBody, ObjectRpcSessionResponse};
    use sinomo_runtime::service::{ResponseSink, ServiceRegistry, ServiceSigner};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    #[derive(Clone, Default)]
    struct TestSink {
        frames: Arc<Mutex<Vec<ObjectRpcResponse>>>,
    }

    impl TestSink {
        fn frames(&self) -> Vec<ObjectRpcResponse> {
            self.frames.lock().expect("frames lock").clone()
        }
    }

    impl ResponseSink for TestSink {
        fn send(self: Box<Self>, response: ObjectRpcSessionResponse) -> Result<(), ServiceError> {
            if !response.body.is_empty() {
                return Err(ServiceError::Service(
                    "agent test sink received an unexpected response body".to_string(),
                ));
            }
            self.frames
                .lock()
                .expect("frames lock")
                .push(response.response);
            Ok(())
        }
    }

    struct TestInteractionModel;

    impl InteractionModel for TestInteractionModel {
        fn complete(
            &self,
            _request: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            panic!("runtime control registry tests should not sample the model")
        }
    }

    #[derive(Default)]
    struct TestControlHost {
        calls: Mutex<Vec<&'static str>>,
    }

    impl TestControlHost {
        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().expect("calls lock").clone()
        }

        fn record(&self, call: &'static str) {
            self.calls.lock().expect("calls lock").push(call);
        }
    }

    impl AgentRuntimeControlHost for TestControlHost {
        fn steer(
            &self,
            request: AgentSessionSteerRequest,
        ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
            self.record("steer");
            Ok(test_control_result(request.session_id, 1))
        }

        fn follow_up(
            &self,
            request: AgentSessionFollowUpRequest,
        ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
            self.record("follow_up");
            Ok(test_control_result(request.session_id, 2))
        }

        fn remove_queued_input(
            &self,
            request: AgentSessionRemoveQueuedInputRequest,
        ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
            self.record("remove_queued_input");
            Ok(test_control_result(request.session_id, 5))
        }

        fn interrupt(
            &self,
            request: AgentSessionInterruptRequest,
        ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
            self.record("interrupt");
            Ok(test_control_result(request.session_id, 3))
        }

        fn resume(
            &self,
            request: AgentSessionResumeRequest,
        ) -> Result<AgentSessionResumeResponse, AgentRuntimeControlError> {
            self.record("resume");
            let snapshot = test_control_snapshot(request.session_id.clone(), 4);
            let reattach = AgentSessionReattachReport::live_runtime_attached(&snapshot, 0);
            Ok(AgentSessionResumeResponse {
                session_id: request.session_id.clone(),
                accepted: true,
                runtime_attached: true,
                session: Some(AgentSession::new(request.session_id, AgentRole::Main)),
                control_snapshot: Some(snapshot),
                event_cursor: AgentSessionControlEventCursor {
                    offset: 0,
                    last_event_id: None,
                },
                reattach,
                detail: None,
            })
        }
    }

    #[test]
    fn agent_runtime_service_reads_persisted_session_progress() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-runtime-service");
        let data_dir = SinomoDataDir::new(&root);
        let store = FileAgentSessionStore::for_data_dir(&data_dir);
        let session_id = SessionId::from_legacy_named("sess_agent_service").expect("session id");
        let session = AgentSession::new(session_id.clone(), AgentRole::Main);
        store.save_session(&session).expect("save session");

        let archived_op_id = TaskId::from_legacy_named("op_agent_service_archived").expect("op id");
        let archived_task = AgentTaskRef::new("coding", "archived persisted agent session")
            .with_owner_role(AgentRole::Main);
        let mut archived_snapshot =
            AgentTaskSnapshot::new(archived_task.clone(), AgentTaskLifecycleStatus::Completed);
        archived_snapshot.progress_summary =
            Some("completed before the latest snapshot".to_string());
        let archived_event = crate::AgentEvent::new(
            archived_op_id,
            session_id.clone(),
            AgentEventPayload::TaskStatusChanged {
                snapshot: archived_snapshot,
            },
        );
        store
            .append_record(&AgentSessionStoreRecord::new(
                session_id.clone(),
                AgentSessionStoreRecordKind::ControlEventPublished {
                    event: archived_event,
                },
            ))
            .expect("append archived event");

        let op_id = TaskId::from_legacy_named("op_agent_service").expect("op id");
        let task = AgentTaskRef::new("coding", "inspect persisted agent session")
            .with_owner_role(AgentRole::Main);
        let mut snapshot =
            AgentTaskSnapshot::new(task.clone(), AgentTaskLifecycleStatus::Completed);
        snapshot.progress_summary = Some("completed through persisted events".to_string());
        let event = crate::AgentEvent::new(
            op_id,
            session_id.clone(),
            AgentEventPayload::TaskStatusChanged {
                snapshot: snapshot.clone(),
            },
        );
        store
            .append_record(&AgentSessionStoreRecord::new(
                session_id.clone(),
                AgentSessionStoreRecordKind::ControlEventPublished {
                    event: event.clone(),
                },
            ))
            .expect("append event");
        store
            .append_record(&AgentSessionStoreRecord::new(
                session_id.clone(),
                AgentSessionStoreRecordKind::ControlSnapshotSaved {
                    snapshot: AgentSessionControlSnapshot {
                        session_id: session_id.clone(),
                        next_sequence: 1,
                        active_turn: None,
                        tasks: vec![snapshot],
                        buffered_event_count: 0,
                    },
                },
            ))
            .expect("append snapshot");

        let registry = register_services(ServiceRegistry::builder()).build();
        assert!(registry.owns(AGENT_SESSION_INSPECT_ROUTE));
        let sink = TestSink::default();
        let ctx = ServiceContext::new(
            Arc::new(data_dir),
            None,
            ServiceSigner::Local,
            ObjectRpcIncomingBody::empty(),
            Box::new(sink.clone()),
        );
        let request = ObjectRpcRequest::encode_body(
            AGENT_SESSION_INSPECT_ROUTE,
            AGENT_RPC_CATEGORY,
            &AgentSessionInspectRequest {
                session_id: session_id.clone(),
            },
        )
        .expect("request");

        block_on_ready(registry.dispatch_direct(ctx, request)).expect("dispatch");

        let frames = sink.frames();
        assert_eq!(frames.len(), 1);
        let response: AgentSessionInspectResponse =
            frames[0].decode_body().expect("inspect response");
        assert_eq!(response.session_id, session_id);
        assert!(response.session.is_some());
        assert_eq!(response.persisted_event_count, 2);
        assert_eq!(response.event_cursor.offset, 2);
        assert_eq!(response.terminal_results.len(), 2);
        assert!(
            response
                .terminal_results
                .iter()
                .any(|result| result.task.task_id == task.task_id)
        );
        assert!(
            response
                .terminal_results
                .iter()
                .any(|result| result.task.task_id == archived_task.task_id)
        );
    }

    #[test]
    fn default_agent_runtime_service_registers_read_only_routes() {
        let registry = register_services(ServiceRegistry::builder()).build();

        assert!(registry.owns(AGENT_SESSION_INSPECT_ROUTE));
        assert!(registry.owns(AGENT_SESSION_PROGRESS_ROUTE));
        assert!(registry.owns(AGENT_SESSION_TERMINAL_RESULTS_ROUTE));
        assert!(!registry.owns(AGENT_SESSION_STEER_ROUTE));
        assert!(!registry.owns(AGENT_SESSION_FOLLOW_UP_ROUTE));
        assert!(!registry.owns(AGENT_SESSION_INTERRUPT_ROUTE));
        assert!(!registry.owns(AGENT_SESSION_RESUME_ROUTE));
    }

    #[test]
    fn runtime_control_service_dispatches_to_injected_host() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-runtime-control-service");
        let data_dir = SinomoDataDir::new(&root);
        let host = Arc::new(TestControlHost::default());
        let registry =
            register_services_with_runtime_control(ServiceRegistry::builder(), host.clone())
                .build();
        assert!(registry.owns(AGENT_SESSION_INSPECT_ROUTE));
        assert!(registry.owns(AGENT_SESSION_STEER_ROUTE));
        assert!(registry.owns(AGENT_SESSION_FOLLOW_UP_ROUTE));
        assert!(registry.owns(AGENT_SESSION_INTERRUPT_ROUTE));
        assert!(registry.owns(AGENT_SESSION_RESUME_ROUTE));

        let session_id = SessionId::from_legacy_named("sess_agent_control").expect("session id");
        let sink = TestSink::default();
        let ctx = ServiceContext::new(
            Arc::new(data_dir),
            None,
            ServiceSigner::Local,
            ObjectRpcIncomingBody::empty(),
            Box::new(sink.clone()),
        );
        let request = ObjectRpcRequest::encode_body(
            AGENT_SESSION_STEER_ROUTE,
            AGENT_RPC_CATEGORY,
            &AgentSessionSteerRequest {
                session_id: session_id.clone(),
                message: "prefer concise output".to_string(),
            },
        )
        .expect("request");

        block_on_ready(registry.dispatch_direct(ctx, request)).expect("dispatch");

        assert_eq!(host.calls(), vec!["steer"]);
        let frames = sink.frames();
        assert_eq!(frames.len(), 1);
        let response: AgentSessionControlResponse =
            frames[0].decode_body().expect("control response");
        assert_eq!(response.session_id, session_id);
        assert!(response.result.accepted);
        assert_eq!(response.result.sequence, 1);
    }

    #[test]
    fn runtime_control_service_dispatches_resume_to_injected_host() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-runtime-resume-service");
        let data_dir = SinomoDataDir::new(&root);
        let host = Arc::new(TestControlHost::default());
        let registry =
            register_runtime_control_services(ServiceRegistry::builder(), host.clone()).build();
        let session_id = SessionId::from_legacy_named("sess_agent_resume").expect("session id");
        let sink = TestSink::default();
        let ctx = ServiceContext::new(
            Arc::new(data_dir),
            None,
            ServiceSigner::Local,
            ObjectRpcIncomingBody::empty(),
            Box::new(sink.clone()),
        );
        let request = ObjectRpcRequest::encode_body(
            AGENT_SESSION_RESUME_ROUTE,
            AGENT_RPC_CATEGORY,
            &AgentSessionResumeRequest {
                session_id: session_id.clone(),
            },
        )
        .expect("request");

        block_on_ready(registry.dispatch_direct(ctx, request)).expect("dispatch");

        assert_eq!(host.calls(), vec!["resume"]);
        let frames = sink.frames();
        assert_eq!(frames.len(), 1);
        let response: AgentSessionResumeResponse =
            frames[0].decode_body().expect("resume response");
        assert_eq!(response.session_id, session_id);
        assert!(response.accepted);
        assert!(response.runtime_attached);
        assert_eq!(
            response.reattach.state,
            AgentSessionReattachState::LiveRuntimeAttached
        );
        assert!(response.session.is_some());
        assert!(response.control_snapshot.is_some());
    }

    #[test]
    fn runtime_control_resume_falls_back_to_persisted_metadata_when_live_runtime_is_missing() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-runtime-resume-fallback");
        let data_dir = SinomoDataDir::new(&root);
        let store = FileAgentSessionStore::for_data_dir(&data_dir);
        let session_id =
            SessionId::from_legacy_named("sess_agent_resume_fallback").expect("session id");
        let session = AgentSession::new(session_id.clone(), AgentRole::Main);
        store.save_session(&session).expect("save session");
        let task =
            AgentTaskRef::new("coding", "restart-resume task").with_owner_role(AgentRole::Main);
        let mut task_snapshot =
            AgentTaskSnapshot::new(task.clone(), AgentTaskLifecycleStatus::Running);
        let active_op_id = TaskId::from_legacy_named("op_agent_resume_fallback").expect("op id");
        task_snapshot.active_op_id = Some(active_op_id.clone());
        store
            .append_record(&AgentSessionStoreRecord::new(
                session_id.clone(),
                AgentSessionStoreRecordKind::ControlSnapshotSaved {
                    snapshot: AgentSessionControlSnapshot {
                        session_id: session_id.clone(),
                        next_sequence: 1,
                        active_turn: Some(crate::AgentActiveTurn::new(
                            active_op_id,
                            0,
                            None,
                            Some(task.task_id.clone()),
                        )),
                        tasks: vec![task_snapshot],
                        buffered_event_count: 0,
                    },
                },
            ))
            .expect("append snapshot");

        let host = Arc::new(AgentRuntimeControlRegistry::new());
        let registry = register_runtime_control_services(ServiceRegistry::builder(), host).build();
        let sink = TestSink::default();
        let ctx = ServiceContext::new(
            Arc::new(data_dir),
            None,
            ServiceSigner::Local,
            ObjectRpcIncomingBody::empty(),
            Box::new(sink.clone()),
        );
        let request = ObjectRpcRequest::encode_body(
            AGENT_SESSION_RESUME_ROUTE,
            AGENT_RPC_CATEGORY,
            &AgentSessionResumeRequest {
                session_id: session_id.clone(),
            },
        )
        .expect("request");

        block_on_ready(registry.dispatch_direct(ctx, request)).expect("dispatch");

        let frames = sink.frames();
        assert_eq!(frames.len(), 1);
        let response: AgentSessionResumeResponse =
            frames[0].decode_body().expect("resume response");
        assert_eq!(response.session_id, session_id);
        assert!(!response.accepted);
        assert!(!response.runtime_attached);
        assert_eq!(
            response.reattach.state,
            AgentSessionReattachState::PersistedMetadataOnly
        );
        assert!(response.reattach.active_turn_persisted);
        assert_eq!(response.reattach.running_task_count, 1);
        assert!(
            response
                .reattach
                .reason
                .contains("cannot be reattached from persisted metadata alone")
        );
        assert!(response.session.is_some());
        let snapshot = response.control_snapshot.expect("persisted snapshot");
        assert!(snapshot.active_turn.is_some());
        assert_eq!(snapshot.tasks.len(), 1);
        assert_eq!(snapshot.tasks[0].status, AgentTaskLifecycleStatus::Running);
        assert_eq!(response.event_cursor.offset, 0);
        assert!(
            response
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("live runtime unavailable"))
        );
    }

    #[test]
    fn runtime_control_registry_routes_service_calls_to_live_runtime() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-runtime-control-registry");
        let data_dir = SinomoDataDir::new(&root);
        let session_id =
            SessionId::from_legacy_named("sess_agent_control_registry").expect("session id");
        let agent = Agent::new(TestInteractionModel, "fake", "fake");
        let runtime = AgentSessionRuntime::new(
            agent,
            AgentSession::new(session_id.clone(), AgentRole::Main),
        );
        let control_registry = AgentRuntimeControlRegistry::new();
        assert_eq!(
            control_registry
                .register_runtime(runtime.clone())
                .expect("register runtime"),
            session_id
        );

        let registry = register_runtime_control_services(
            ServiceRegistry::builder(),
            Arc::new(control_registry.clone()),
        )
        .build();
        let sink = TestSink::default();
        let ctx = ServiceContext::new(
            Arc::new(data_dir),
            None,
            ServiceSigner::Local,
            ObjectRpcIncomingBody::empty(),
            Box::new(sink.clone()),
        );
        let request = ObjectRpcRequest::encode_body(
            AGENT_SESSION_STEER_ROUTE,
            AGENT_RPC_CATEGORY,
            &AgentSessionSteerRequest {
                session_id: session_id.clone(),
                message: "keep the current constraint".to_string(),
            },
        )
        .expect("request");

        block_on_ready(registry.dispatch_direct(ctx, request)).expect("dispatch");

        let frames = sink.frames();
        assert_eq!(frames.len(), 1);
        let response: AgentSessionControlResponse =
            frames[0].decode_body().expect("control response");
        assert_eq!(response.session_id, session_id);
        assert!(response.result.accepted);
        assert!(response.result.events.iter().any(|event| matches!(
            &event.payload,
            AgentEventPayload::SteeringQueued { steering }
                if steering.message == "keep the current constraint"
        )));
        let live_session = runtime.session_snapshot().expect("session snapshot");
        assert_eq!(live_session.steering_queue.len(), 1);
        assert_eq!(
            live_session.steering_queue[0].message,
            "keep the current constraint"
        );
    }

    #[test]
    fn runtime_control_registry_removes_queued_input_over_route() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-runtime-control-remove");
        let data_dir = SinomoDataDir::new(&root);
        let session_id =
            SessionId::from_legacy_named("sess_agent_control_remove").expect("session id");
        let agent = Agent::new(TestInteractionModel, "fake", "fake");
        let runtime = AgentSessionRuntime::new(
            agent,
            AgentSession::new(session_id.clone(), AgentRole::Main),
        );
        let control_registry = AgentRuntimeControlRegistry::new();
        control_registry
            .register_runtime(runtime.clone())
            .expect("register runtime");

        let queued = control_registry
            .steer(AgentSessionSteerRequest {
                session_id: session_id.clone(),
                message: "prefer AV1, skip the HEVC pass".to_string(),
            })
            .expect("steer");
        let input_id = queued
            .events
            .iter()
            .find_map(|event| match &event.payload {
                AgentEventPayload::SteeringQueued { steering } => {
                    Some(steering.steering_id.clone())
                }
                _ => None,
            })
            .expect("queued steering id");

        let registry = register_runtime_control_services(
            ServiceRegistry::builder(),
            Arc::new(control_registry.clone()),
        )
        .build();
        let sink = TestSink::default();
        let ctx = ServiceContext::new(
            Arc::new(data_dir),
            None,
            ServiceSigner::Local,
            ObjectRpcIncomingBody::empty(),
            Box::new(sink.clone()),
        );
        let request = ObjectRpcRequest::encode_body(
            AGENT_SESSION_REMOVE_QUEUED_INPUT_ROUTE,
            AGENT_RPC_CATEGORY,
            &AgentSessionRemoveQueuedInputRequest {
                session_id: session_id.clone(),
                input_id: input_id.clone(),
            },
        )
        .expect("request");

        block_on_ready(registry.dispatch_direct(ctx, request)).expect("dispatch");

        let frames = sink.frames();
        assert_eq!(frames.len(), 1);
        let response: AgentSessionControlResponse =
            frames[0].decode_body().expect("control response");
        assert_eq!(response.session_id, session_id);
        assert!(response.result.accepted);
        assert!(response.result.events.iter().any(|event| matches!(
            &event.payload,
            AgentEventPayload::SteeringRemoved { steering }
                if steering.steering_id == input_id
        )));
        let live_session = runtime.session_snapshot().expect("session snapshot");
        assert!(live_session.steering_queue.is_empty());

        // Removing the same id again misses: the queued-vs-consumed race is
        // surfaced as an error rather than a silent success.
        let missing = control_registry.remove_queued_input(AgentSessionRemoveQueuedInputRequest {
            session_id: session_id.clone(),
            input_id,
        });
        assert!(missing.is_err());
    }

    #[test]
    fn runtime_control_registry_resume_returns_live_runtime_snapshot() {
        let session_id =
            SessionId::from_legacy_named("sess_agent_control_registry_resume").expect("session id");
        let agent = Agent::new(TestInteractionModel, "fake", "fake");
        let runtime = AgentSessionRuntime::new(
            agent,
            AgentSession::new(session_id.clone(), AgentRole::Main),
        );
        let control_registry = AgentRuntimeControlRegistry::new();
        control_registry
            .register_runtime(runtime.clone())
            .expect("register runtime");

        let response = control_registry
            .resume(AgentSessionResumeRequest {
                session_id: session_id.clone(),
            })
            .expect("resume");

        assert!(response.accepted);
        assert_eq!(response.session_id, session_id);
        assert!(response.session.is_some());
        assert_eq!(
            response.control_snapshot.expect("snapshot").next_sequence,
            0
        );
        assert_eq!(response.event_cursor.offset, 0);
    }

    fn test_control_result(session_id: SessionId, sequence: u64) -> AgentSessionControlResult {
        AgentSessionControlResult {
            sequence,
            accepted: true,
            events: Vec::new(),
            outcome: None,
            snapshot: test_control_snapshot(session_id, sequence.saturating_add(1)),
        }
    }

    fn test_control_snapshot(
        session_id: SessionId,
        next_sequence: u64,
    ) -> AgentSessionControlSnapshot {
        AgentSessionControlSnapshot {
            session_id,
            next_sequence,
            active_turn: None,
            tasks: Vec::new(),
            buffered_event_count: 0,
        }
    }

    fn block_on_ready<F: Future>(future: F) -> F::Output {
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        loop {
            match Pin::new(&mut future).poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    fn noop_waker() -> Waker {
        unsafe { Waker::from_raw(noop_raw_waker()) }
    }

    fn noop_raw_waker() -> RawWaker {
        RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)
    }

    static NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        noop_waker_clone,
        noop_waker_wake,
        noop_waker_wake,
        noop_waker_drop,
    );

    unsafe fn noop_waker_clone(_: *const ()) -> RawWaker {
        noop_raw_waker()
    }

    unsafe fn noop_waker_wake(_: *const ()) {}

    unsafe fn noop_waker_drop(_: *const ()) {}
}
