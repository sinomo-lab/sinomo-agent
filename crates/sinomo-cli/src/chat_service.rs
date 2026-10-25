//! Daemon-hosted interactive chat sessions.

use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use sinomo_agent::{
    AgentChannel, AgentChannelAuthor, AgentChannelKind, AgentChannelMessage,
    AgentChannelMessageKind, AgentChannelSource, AgentChannelStore, AgentRuntimeControlHost,
    AgentRuntimeControlRegistry, AgentSessionCancelRequest, AgentSessionControlResult,
    AgentSessionFollowUpRequest, AgentSessionInterruptRequest, AgentSessionReattachState,
    AgentSessionRemoveQueuedInputRequest, AgentSessionResumeRequest, AgentSessionResumeResponse,
    AgentSessionSteerRequest, AgentTaskLifecycleStatus, AgentTaskSnapshot, TaskId,
};
use sinomo_runtime::api::app::chat as wire;
use sinomo_runtime::events::SessionId;
use sinomo_runtime::network::ObjectRpcRequest;
use sinomo_runtime::node::NodeId;
use sinomo_runtime::service::{
    Service, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};

use crate::app_api as local;

pub(crate) mod activity;
mod cluster;
mod recovery;
use cluster::{CHAT_OWNER_TTL, ChatCluster, ChatOwner};
use recovery::CommandJournal;

const CHAT_EVENT_RETENTION: usize = 8 * 1024;
/// Most execution hosts the daemon keeps resident at once.
const MAX_RESIDENT_CHAT_HOSTS: usize = 256;
/// A host no window is attached to is released after this long without
/// activity, once its session has no work in flight. Reopening the chat
/// restores it from durable state.
const DETACHED_CHAT_HOST_IDLE_TTL: Duration = Duration::from_secs(30 * 60);
/// A route following another node's chat with no window here is dropped
/// after this long.
const REMOTE_CHAT_ROUTE_IDLE_TTL: Duration = Duration::from_secs(60);
/// How long a deleted chat's content is kept before it is purged.
const DELETED_CHAT_RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);
/// Also the cadence of chat ownership renewal, well inside its TTL.
const CHAT_HOST_SWEEP_INTERVAL: Duration = Duration::from_secs(10);
const DELETED_CHAT_PURGE_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone)]
struct ChatEventLog {
    state: Arc<(Mutex<ChatEventLogState>, Condvar)>,
}

#[derive(Default)]
struct ChatEventLogState {
    next_sequence: u64,
    generation: String,
    events: VecDeque<wire::SequencedChatEvent>,
}

impl ChatEventLog {
    fn new() -> Self {
        Self {
            state: Arc::new((
                Mutex::new(ChatEventLogState {
                    next_sequence: 1,
                    generation: SessionId::generate().to_string(),
                    events: VecDeque::new(),
                }),
                Condvar::new(),
            )),
        }
    }

    fn push(&self, event: wire::ChatEvent) {
        let (lock, changed) = &*self.state;
        let mut state = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sequence = state.next_sequence;
        state.next_sequence = state.next_sequence.wrapping_add(1).max(1);
        state
            .events
            .push_back(wire::SequencedChatEvent { sequence, event });
        while state.events.len() > CHAT_EVENT_RETENTION {
            state.events.pop_front();
        }
        changed.notify_all();
    }

    fn poll(&self, request: &wire::ChatSessionEventsRequest) -> wire::ChatSessionEventsResponse {
        let limit = request.max_events.clamp(1, wire::CHAT_EVENT_PAGE_MAX);
        let wait = Duration::from_millis(request.wait_ms.min(wire::CHAT_EVENT_WAIT_MAX_MS));
        let (lock, changed) = &*self.state;
        let mut state = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state
            .events
            .iter()
            .any(|event| event.sequence > request.after_sequence)
            && !wait.is_zero()
        {
            let waited = changed.wait_timeout(state, wait);
            state = match waited {
                Ok((state, _)) => state,
                Err(error) => error.into_inner().0,
            };
        }
        let events = state
            .events
            .iter()
            .filter(|event| event.sequence > request.after_sequence)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        let next_sequence = events
            .last()
            .map(|event| event.sequence)
            .unwrap_or(request.after_sequence);
        wire::ChatSessionEventsResponse {
            events,
            next_sequence,
            first_available_sequence: state
                .events
                .front()
                .map(|e| e.sequence)
                .unwrap_or(state.next_sequence),
            generation: state.generation.clone(),
            execution_available: true,
        }
    }
}

enum HostCommand {
    Run {
        /// The view that issued the command; it already shows its own input.
        origin: String,
        /// The issuing view's journal entry, when the command was admitted.
        journal: Option<(CommandJournal, String)>,
        command: wire::ChatSessionCommand,
    },
    Shutdown,
}

/// Something a chat host or view can deliver chat events to.
trait ChatEventSink {
    fn push(&self, event: wire::ChatEvent);
}

impl ChatEventSink for ChatEventLog {
    fn push(&self, event: wire::ChatEvent) {
        ChatEventLog::push(self, event);
    }
}

/// Delivers one chat's events to every view subscribed to it on this node,
/// keeps the latest runtime state so a view that attaches later (even during
/// a running turn) is brought up to date immediately, and keeps a sequenced
/// tail that windows on other nodes follow.
#[derive(Clone)]
struct ChatFanout {
    state: Arc<(Mutex<ChatFanoutState>, Condvar)>,
}

struct ChatFanoutState {
    subscribers: std::collections::BTreeMap<String, ChatEventLog>,
    ready: Option<wire::ChatEvent>,
    tool_runtime: Option<wire::ChatEvent>,
    control_state: Option<wire::ChatEvent>,
    generation: String,
    next_sequence: u64,
    peer_events: VecDeque<wire::ChatPeerEvent>,
}

impl Default for ChatFanout {
    fn default() -> Self {
        Self {
            state: Arc::new((
                Mutex::new(ChatFanoutState {
                    subscribers: std::collections::BTreeMap::new(),
                    ready: None,
                    tool_runtime: None,
                    control_state: None,
                    generation: SessionId::generate().to_string(),
                    next_sequence: 1,
                    peer_events: VecDeque::new(),
                }),
                Condvar::new(),
            )),
        }
    }
}

impl ChatFanout {
    fn lock(&self) -> std::sync::MutexGuard<'_, ChatFanoutState> {
        self.state
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn snapshot(state: &ChatFanoutState) -> Vec<wire::ChatEvent> {
        [&state.ready, &state.tool_runtime, &state.control_state]
            .into_iter()
            .flatten()
            .cloned()
            .collect()
    }

    /// Subscribe a view and replay the latest state to it.
    fn subscribe(&self, view: &str, log: ChatEventLog) {
        let mut state = self.lock();
        for event in Self::snapshot(&state) {
            log.push(event);
        }
        state.subscribers.insert(view.to_string(), log);
    }

    fn unsubscribe(&self, view: &str) {
        self.lock().subscribers.remove(view);
    }

    fn subscriber_count(&self) -> usize {
        self.lock().subscribers.len()
    }

    fn take_subscribers(&self) -> Vec<(String, ChatEventLog)> {
        std::mem::take(&mut self.lock().subscribers)
            .into_iter()
            .collect()
    }

    fn is_ready(&self) -> bool {
        self.lock().ready.is_some()
    }

    /// Deliver an event. A user message is not echoed to the view that sent
    /// it, which already shows it.
    fn push_from(&self, origin: Option<&str>, event: wire::ChatEvent) {
        let mut state = self.lock();
        match &event {
            wire::ChatEvent::Ready { .. } => state.ready = Some(event.clone()),
            wire::ChatEvent::RuntimeInfo(info) => {
                if let Some(wire::ChatEvent::Ready { info: ready, .. }) = state.ready.as_mut() {
                    *ready = info.clone();
                }
            }
            wire::ChatEvent::ToolRuntime(_) => state.tool_runtime = Some(event.clone()),
            wire::ChatEvent::ControlState(_) => state.control_state = Some(event.clone()),
            _ => {}
        }
        let skip_origin = matches!(event, wire::ChatEvent::UserMessage { .. });
        for (view, log) in &state.subscribers {
            if skip_origin && origin == Some(view.as_str()) {
                continue;
            }
            log.push(event.clone());
        }
        let sequence = state.next_sequence;
        state.next_sequence += 1;
        state.peer_events.push_back(wire::ChatPeerEvent {
            sequence,
            origin: origin.map(str::to_string),
            event,
        });
        while state.peer_events.len() > CHAT_EVENT_RETENTION {
            state.peer_events.pop_front();
        }
        self.state.1.notify_all();
    }

    /// Events after `after_sequence` for a window on another node. Following
    /// from the start includes the chat's current state.
    fn poll_peer(&self, request: &wire::ChatPeerEventsRequest) -> wire::ChatPeerEventsResponse {
        let limit = request.max_events.clamp(1, wire::CHAT_EVENT_PAGE_MAX);
        let wait = Duration::from_millis(request.wait_ms.min(wire::CHAT_EVENT_WAIT_MAX_MS));
        let mut state = self.lock();
        if request.after_sequence != 0
            && !state
                .peer_events
                .iter()
                .any(|event| event.sequence > request.after_sequence)
            && !wait.is_zero()
        {
            state = match self.state.1.wait_timeout(state, wait) {
                Ok((state, _)) => state,
                Err(error) => error.into_inner().0,
            };
        }
        let (snapshot, after) = if request.after_sequence == 0 {
            // Start from the current state rather than replaying the tail.
            (Self::snapshot(&state), state.next_sequence - 1)
        } else {
            (Vec::new(), request.after_sequence)
        };
        let events = state
            .peer_events
            .iter()
            .filter(|event| event.sequence > after)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        let next_sequence = events.last().map(|event| event.sequence).unwrap_or(after);
        wire::ChatPeerEventsResponse {
            generation: state.generation.clone(),
            snapshot,
            events,
            next_sequence,
            not_owner: false,
        }
    }
}

impl ChatEventSink for ChatFanout {
    fn push(&self, event: wire::ChatEvent) {
        self.push_from(None, event);
    }
}

/// The single live runtime for one chat in the cluster, hosted on this node.
/// Every window viewing the chat, here or on another node, reaches it, so
/// they share one conversation, one turn, and one control route.
struct ChatHost {
    command_tx: Sender<HostCommand>,
    fanout: ChatFanout,
    ready: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
    activity: HostActivity,
    /// When ownership was last confirmed; a host that cannot renew stops.
    renewed_at: Mutex<Instant>,
    /// Recent admission outcomes for this host. A failed or uncertain effect
    /// must not become an acknowledgement (or execute again) on a peer retry.
    peer_requests: Mutex<VecDeque<(String, Result<(), String>)>>,
}

impl Drop for ChatHost {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        let _ = self.command_tx.send(HostCommand::Shutdown);
    }
}

impl ChatHost {
    /// Run an admitted effect once on this host. The caller holds the route
    /// registry through the effect; only later event/history projection may
    /// outlive that ownership guard. Duplicate successes have no new output.
    fn with_peer_request<T>(
        &self,
        request_id: &str,
        effect: impl FnOnce() -> Result<T, String>,
    ) -> Result<Option<T>, String> {
        let mut seen = self
            .peer_requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((_, outcome)) = seen.iter().find(|(id, _)| id == request_id) {
            return outcome.clone().map(|()| None);
        }
        let result = effect();
        seen.push_back((
            request_id.to_string(),
            result.as_ref().map(|_| ()).map_err(Clone::clone),
        ));
        while seen.len() > 1024 {
            seen.pop_front();
        }
        result.map(Some)
    }
}

/// A chat owned by another node. Its events are followed into a local
/// fan-out for this node's views, and commands are forwarded to the owner.
struct RemoteChat {
    owner: NodeId,
    fanout: ChatFanout,
    alive: Arc<AtomicBool>,
    last_active: Mutex<Instant>,
}

impl Drop for RemoteChat {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
    }
}

/// How this node reaches a chat.
enum ChatRoute {
    Local(ChatHost),
    Remote(RemoteChat),
}

impl ChatRoute {
    fn fanout(&self) -> &ChatFanout {
        match self {
            Self::Local(host) => &host.fanout,
            Self::Remote(remote) => &remote.fanout,
        }
    }

    fn alive(&self) -> bool {
        match self {
            Self::Local(host) => host.alive.load(Ordering::Acquire),
            Self::Remote(remote) => remote.alive.load(Ordering::Acquire),
        }
    }

    fn ready(&self) -> bool {
        match self {
            Self::Local(host) => host.ready.load(Ordering::Acquire),
            Self::Remote(remote) => remote.fanout.is_ready(),
        }
    }

    fn touch(&self) {
        match self {
            Self::Local(host) => host.activity.touch(),
            Self::Remote(remote) => {
                *remote
                    .last_active
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Instant::now()
            }
        }
    }

    fn idle_for(&self, runtime_control: &AgentRuntimeControlRegistry) -> Option<Duration> {
        match self {
            Self::Local(host) => host.activity.idle_for(runtime_control),
            Self::Remote(remote) => Some(
                remote
                    .last_active
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .elapsed(),
            ),
        }
    }
}

/// One window tab's view: its own event stream and command journal, and the
/// chat it currently shows. Views are independent; closing or switching one
/// never affects another view of the same chat.
struct ChatView {
    events: ChatEventLog,
    storage: local::AgentChatSifsStorage,
    journal: CommandJournal,
    chat: Arc<Mutex<Option<String>>>,
    last_active: Arc<Mutex<Instant>>,
}

impl ChatView {
    fn chat_id(&self) -> Option<String> {
        self.chat
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn touch(&self) {
        *self
            .last_active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.last_active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .elapsed()
    }
}

/// What the worker is doing, so idle hosts can be told apart from busy ones.
#[derive(Clone)]
struct HostActivity {
    state: Arc<Mutex<HostActivityState>>,
}

struct HostActivityState {
    busy: bool,
    last_active: Instant,
    session_id: Option<SessionId>,
    /// Channel participants have work in flight (their sessions are not the
    /// chat's, so the control registry check does not see them).
    participant_work: bool,
}

impl HostActivity {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(HostActivityState {
                busy: false,
                last_active: Instant::now(),
                session_id: None,
                participant_work: false,
            })),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HostActivityState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn touch(&self) {
        self.lock().last_active = Instant::now();
    }

    fn begin(&self) {
        let mut state = self.lock();
        state.busy = true;
        state.last_active = Instant::now();
    }

    fn set_participant_work(&self, participant_work: bool) {
        self.lock().participant_work = participant_work;
    }

    fn end(&self, session_id: Option<SessionId>) {
        let mut state = self.lock();
        state.busy = false;
        state.last_active = Instant::now();
        if session_id.is_some() {
            state.session_id = session_id;
        }
    }

    /// How long the host has been idle, or `None` while it is processing a
    /// command or its session still has work in flight (a turn, tools a
    /// paused turn is finishing, or subagents).
    fn idle_for(&self, runtime_control: &AgentRuntimeControlRegistry) -> Option<Duration> {
        let (busy, last_active, session_id) = {
            let state = self.lock();
            (
                state.busy || state.participant_work,
                state.last_active,
                state.session_id.clone(),
            )
        };
        if busy {
            return None;
        }
        if let Some(session_id) = session_id {
            match runtime_control.has_running_work(&session_id) {
                Ok(Some(false) | None) => {}
                Ok(Some(true)) | Err(_) => return None,
            }
        }
        Some(last_active.elapsed())
    }
}

/// Where hosts are started: the daemon's data directory and runtime, and
/// this node's place in the cluster.
#[derive(Clone)]
struct HostEnvironment {
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
    cluster: ChatCluster,
}

#[derive(Clone)]
pub struct HostedChatService {
    daemon_address: String,
    runtime_control: AgentRuntimeControlRegistry,
    views: Arc<Mutex<HashMap<String, ChatView>>>,
    hosts: Arc<Mutex<HashMap<String, ChatRoute>>>,
    clients: Arc<Mutex<HashMap<String, String>>>,
    ephemeral_clients: Arc<Mutex<std::collections::BTreeSet<String>>>,
    environment: Arc<std::sync::OnceLock<HostEnvironment>>,
    /// Starts hosts in place of the agent runtime worker (tests).
    host_factory: Option<Arc<dyn Fn(&str) -> ChatHost + Send + Sync>>,
}

impl HostedChatService {
    pub fn new(
        daemon_address: impl Into<String>,
        runtime_control: AgentRuntimeControlRegistry,
    ) -> Self {
        Self {
            daemon_address: daemon_address.into(),
            runtime_control,
            views: Arc::new(Mutex::new(HashMap::new())),
            hosts: Arc::new(Mutex::new(HashMap::new())),
            clients: Arc::new(Mutex::new(HashMap::new())),
            ephemeral_clients: Arc::new(Mutex::new(std::collections::BTreeSet::new())),
            environment: Arc::new(std::sync::OnceLock::new()),
            host_factory: None,
        }
    }

    fn environment(&self) -> Result<HostEnvironment, String> {
        self.environment
            .get()
            .cloned()
            .ok_or_else(|| "chat service has not been opened yet".to_string())
    }

    fn init_environment(
        &self,
        data_dir: &sinomo_runtime::local::SinomoDataDir,
        runtime: &tokio::runtime::Handle,
    ) -> HostEnvironment {
        self.environment
            .get_or_init(|| HostEnvironment {
                data_dir: data_dir.clone(),
                runtime: runtime.clone(),
                cluster: ChatCluster::for_data_dir(data_dir, &self.daemon_address),
            })
            .clone()
    }

    fn open(
        &self,
        data_dir: sinomo_runtime::local::SinomoDataDir,
        runtime: tokio::runtime::Handle,
        request: wire::ChatSessionOpenRequest,
    ) -> Result<wire::ChatSessionOpenResponse, String> {
        validate_client_id(&request.client_id)?;
        self.init_environment(&data_dir, &runtime);
        let ephemeral = request.execution_id.is_none();
        let execution_id = request
            .execution_id
            .clone()
            .unwrap_or_else(|| request.client_id.clone());
        validate_client_id(&execution_id)?;
        let attached = {
            let mut views = self
                .views
                .lock()
                .map_err(|_| "chat view registry lock poisoned".to_string())?;
            let attached = views.contains_key(&execution_id);
            if !attached {
                if views.len() >= MAX_RESIDENT_CHAT_HOSTS {
                    // Make room by releasing the longest-idle detached view.
                    self.evict_idle_views(&mut views, Some(Duration::ZERO), 1)?;
                }
                if views.len() >= MAX_RESIDENT_CHAT_HOSTS {
                    return Err("too many resident chat views".into());
                }
                let journal = CommandJournal::open(&data_dir.root(), &execution_id)?;
                views.insert(
                    execution_id.clone(),
                    ChatView {
                        events: ChatEventLog::new(),
                        storage: local::AgentChatSifsStorage::new(
                            data_dir.clone(),
                            self.daemon_address.clone(),
                            &runtime,
                        ),
                        journal,
                        chat: Arc::new(Mutex::new(None)),
                        last_active: Arc::new(Mutex::new(Instant::now())),
                    },
                );
            }
            attached
        };
        if !attached {
            self.restore_view(&execution_id)?;
        }
        let (runtime_ready, generation) = {
            let views = self
                .views
                .lock()
                .map_err(|_| "chat view registry poisoned")?;
            let view = views
                .get(&execution_id)
                .ok_or_else(|| format!("chat view `{execution_id}` disappeared"))?;
            let generation = view
                .events
                .state
                .0
                .lock()
                .map_err(|_| "event log poisoned")?
                .generation
                .clone();
            (self.view_route_ready(view), generation)
        };
        self.clients
            .lock()
            .map_err(|_| "chat subscription registry poisoned")?
            .insert(request.client_id.clone(), execution_id);
        if ephemeral {
            self.ephemeral_clients
                .lock()
                .map_err(|_| "chat registry poisoned")?
                .insert(request.client_id.clone());
        }
        Ok(wire::ChatSessionOpenResponse {
            client_id: request.client_id,
            next_event_sequence: 0,
            attached,
            generation,
            runtime_ready,
            handoff_protocol: wire::CHAT_HANDOFF_PROTOCOL_VERSION,
        })
    }

    fn view_route_ready(&self, view: &ChatView) -> bool {
        let Some(chat_id) = view.chat_id() else {
            return false;
        };
        self.hosts
            .lock()
            .map(|hosts| hosts.get(&chat_id).is_some_and(ChatRoute::ready))
            .unwrap_or(false)
    }

    /// Bring a newly opened view back to where its journal left it: the chat
    /// it showed (or a new chat), the model it selected, and any commands that
    /// were admitted but not finished.
    fn restore_view(&self, execution_id: &str) -> Result<(), String> {
        let (journal, events) = {
            let views = self
                .views
                .lock()
                .map_err(|_| "chat view registry poisoned")?;
            let view = views
                .get(execution_id)
                .ok_or_else(|| format!("chat view `{execution_id}` disappeared"))?;
            (view.journal.clone(), view.events.clone())
        };
        let (bootstrap, pending, interrupted) = journal.recovery()?;
        let mut chat_id = None;
        let mut models = Vec::new();
        for command in bootstrap {
            match command {
                wire::ChatSessionCommand::Resume { session_id, .. } => chat_id = Some(session_id),
                command @ wire::ChatSessionCommand::SetModel { .. } => models.push(command),
                _ => {
                    events.push(wire::ChatEvent::Error {
                        session_id: None,
                        error: "chat restoration failed: unsupported chat recovery command".into(),
                    });
                }
            }
        }
        let chat_id = chat_id.unwrap_or_else(|| SessionId::generate().to_string());
        self.attach_view(execution_id, &chat_id, &[])?;
        for command in models {
            self.dispatch(execution_id, &chat_id, None, command)?;
        }
        if interrupted {
            events.push(wire::ChatEvent::Notice { session_id: chat_id.clone(), notice: "An operation was interrupted by daemon failure. Its result must be reconciled before retrying; inspect the durable tool evidence before retrying.".into() });
        }
        for (id, command) in pending {
            let target = command_chat_id(&command)
                .filter(|target| !target.trim().is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| chat_id.clone());
            self.dispatch(execution_id, &target, Some((journal.clone(), id)), command)?;
        }
        Ok(())
    }

    /// Show `chat_id` in a view: subscribe it to the chat's route, starting
    /// it if needed, and leave the chat it showed before.
    fn attach_view(
        &self,
        execution_id: &str,
        chat_id: &str,
        history: &[wire::ChatHistoryMessage],
    ) -> Result<(), String> {
        let (events, chat) = {
            let views = self
                .views
                .lock()
                .map_err(|_| "chat view registry poisoned")?;
            let view = views
                .get(execution_id)
                .ok_or_else(|| format!("chat view `{execution_id}` disappeared"))?;
            (view.events.clone(), view.chat.clone())
        };
        let hosts = self.ensure_route(chat_id, history)?;
        let previous = chat
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(chat_id.to_string());
        if let Some(previous) = previous.filter(|previous| previous != chat_id)
            && let Some(route) = hosts.get(&previous)
        {
            route.fanout().unsubscribe(execution_id);
        }
        if let Some(route) = hosts.get(chat_id) {
            route.fanout().subscribe(execution_id, events);
        }
        Ok(())
    }

    /// Make sure this node can reach `chat_id`: an existing live route, or
    /// a new one. The cluster decides whether this node hosts the chat or
    /// follows the node that does. Views of a route that died move to the
    /// new one.
    ///
    /// Settling the owner talks to the cluster, so it happens without holding
    /// the route registry; the registry is returned locked with the route in
    /// place.
    fn ensure_route(
        &self,
        chat_id: &str,
        history: &[wire::ChatHistoryMessage],
    ) -> Result<std::sync::MutexGuard<'_, HashMap<String, ChatRoute>>, String> {
        let lock = || {
            self.hosts
                .lock()
                .map_err(|_| "chat host registry poisoned".to_string())
        };
        {
            let hosts = lock()?;
            if hosts.get(chat_id).is_some_and(ChatRoute::alive) {
                return Ok(hosts);
            }
        }
        let environment = self.environment()?;
        let route = match environment.cluster.ownership.claim(chat_id)? {
            ChatOwner::Local => {
                ChatRoute::Local(self.start_host(&environment, chat_id, history)?)
            }
            ChatOwner::Remote(owner) => {
                ChatRoute::Remote(self.follow_remote(&environment, chat_id, owner)?)
            }
        };
        let mut hosts = lock()?;
        // Another request may have settled the route meanwhile; keep it.
        if hosts.get(chat_id).is_some_and(ChatRoute::alive) {
            return Ok(hosts);
        }
        let orphaned = hosts
            .remove(chat_id)
            .map(|route| route.fanout().take_subscribers())
            .unwrap_or_default();
        if hosts.len() >= MAX_RESIDENT_CHAT_HOSTS {
            self.evict_idle_hosts(&mut hosts, Some(Duration::ZERO), 1)?;
        }
        if hosts.len() >= MAX_RESIDENT_CHAT_HOSTS {
            return Err("too many resident chat hosts".into());
        }
        for (view, log) in orphaned {
            route.fanout().subscribe(&view, log);
        }
        hosts.insert(chat_id.to_string(), route);
        Ok(hosts)
    }

    fn start_host(
        &self,
        environment: &HostEnvironment,
        chat_id: &str,
        history: &[wire::ChatHistoryMessage],
    ) -> Result<ChatHost, String> {
        if let Some(factory) = &self.host_factory {
            return Ok(factory(chat_id));
        }
        let fanout = ChatFanout::default();
        let ready = Arc::new(AtomicBool::new(false));
        let alive = Arc::new(AtomicBool::new(true));
        let activity = HostActivity::new();
        let (command_tx, command_rx) = channel();
        let storage = local::AgentChatSifsStorage::new(
            environment.data_dir.clone(),
            self.daemon_address.clone(),
            &environment.runtime,
        );
        let context = ChatHostContext {
            chat_id: chat_id.to_string(),
            history: history.to_vec(),
            data_dir: environment.data_dir.clone(),
            runtime_control: self.runtime_control.clone(),
            storage,
            fanout: fanout.clone(),
            ready: ready.clone(),
            alive: alive.clone(),
            activity: activity.clone(),
        };
        std::thread::Builder::new()
            .name(format!("sinomo-chat-{chat_id}"))
            .spawn(move || chat_host_worker(context, command_rx))
            .map_err(|e| format!("spawn daemon chat host: {e}"))?;
        Ok(ChatHost {
            command_tx,
            fanout,
            ready,
            alive,
            activity,
            renewed_at: Mutex::new(Instant::now()),
            peer_requests: Mutex::new(VecDeque::new()),
        })
    }

    /// Follow a chat hosted by `owner`: its events stream into a local
    /// fan-out until the owner stops answering or no longer owns the chat.
    fn follow_remote(
        &self,
        environment: &HostEnvironment,
        chat_id: &str,
        owner: NodeId,
    ) -> Result<RemoteChat, String> {
        let fanout = ChatFanout::default();
        let alive = Arc::new(AtomicBool::new(true));
        let follower = RemoteFollower {
            chat_id: chat_id.to_string(),
            owner: owner.clone(),
            cluster: environment.cluster.clone(),
            fanout: fanout.clone(),
            alive: alive.clone(),
        };
        std::thread::Builder::new()
            .name(format!("sinomo-chat-follow-{chat_id}"))
            .spawn(move || follower.run())
            .map_err(|e| format!("follow remote chat: {e}"))?;
        Ok(RemoteChat {
            owner,
            fanout,
            alive,
            last_active: Mutex::new(Instant::now()),
        })
    }

    /// Run a command on the chat's host: here, or forwarded to the node that
    /// owns the chat.
    fn dispatch(
        &self,
        origin: &str,
        chat_id: &str,
        journal: Option<(CommandJournal, String)>,
        command: wire::ChatSessionCommand,
    ) -> Result<(), String> {
        for _ in 0..2 {
            let owner = {
                let hosts = self.ensure_route(chat_id, &[])?;
                let route = hosts
                    .get(chat_id)
                    .ok_or_else(|| format!("chat host `{chat_id}` disappeared"))?;
                route.touch();
                match route {
                    ChatRoute::Local(host) => {
                        return host
                            .command_tx
                            .send(HostCommand::Run {
                                origin: origin.to_string(),
                                journal,
                                command,
                            })
                            .map_err(|_| "chat host stopped".to_string());
                    }
                    ChatRoute::Remote(remote) => remote.owner.clone(),
                }
            };
            if self.forward(origin, chat_id, &owner, journal.as_ref(), &command)? {
                return Ok(());
            }
            // The owner changed; re-resolve the route and try once more.
            if let Ok(hosts) = self.hosts.lock()
                && let Some(ChatRoute::Remote(remote)) = hosts.get(chat_id)
            {
                remote.alive.store(false, Ordering::Release);
            }
        }
        Err(format!("chat `{chat_id}` has no reachable host"))
    }

    /// Forward a command to the owning node. `Ok(false)` when that node no
    /// longer owns the chat.
    fn forward(
        &self,
        origin: &str,
        chat_id: &str,
        owner: &NodeId,
        journal: Option<&(CommandJournal, String)>,
        command: &wire::ChatSessionCommand,
    ) -> Result<bool, String> {
        let environment = self.environment()?;
        let request_id = journal
            .map(|(_, id)| id.clone())
            .unwrap_or_else(|| SessionId::generate().to_string());
        if let Some((journal, id)) = journal {
            journal.start(id)?;
        }
        let result = environment.cluster.peers.command(
            owner,
            wire::ChatPeerCommandRequest {
                chat_id: chat_id.to_string(),
                origin: environment.cluster.qualify_origin(origin),
                request_id,
                command: command.clone(),
            },
        );
        match result {
            Ok(response) if response.not_owner && !response.accepted => {
                if let Some((journal, id)) = journal {
                    journal.retry_after_not_owner(id)?;
                }
                Ok(false)
            }
            Ok(response) if response.accepted && !response.not_owner => {
                if let Some((journal, id)) = journal {
                    journal.complete(id, recovery::resume_command(command, chat_id.to_string()))?;
                }
                Ok(true)
            }
            Ok(_) => {
                let error = "peer returned no unambiguous command admission".to_string();
                if let Some((journal, id)) = journal {
                    journal.reject(id, error.clone())?;
                }
                Err(error)
            }
            Err(error) => {
                if let Some((journal, id)) = journal {
                    let _ = journal.reject(id, error.clone());
                }
                Err(format!(
                    "forward to the chat's host node `{owner}` failed: {error}"
                ))
            }
        }
    }

    /// Run a control command (steer, interrupt, cancel, ...) where the chat
    /// is hosted. Its result is shown to every view of the chat.
    fn control(
        &self,
        origin: &str,
        storage: &local::AgentChatSifsStorage,
        view_events: &ChatEventLog,
        chat_id: &str,
        journal: Option<(CommandJournal, String)>,
        command: wire::ChatSessionCommand,
    ) -> Result<(), String> {
        let local_fanout = {
            let hosts = if chat_id.trim().is_empty() {
                self.hosts
                    .lock()
                    .map_err(|_| "chat host registry poisoned")?
            } else {
                self.ensure_route(chat_id, &[])?
            };
            match hosts.get(chat_id) {
                Some(ChatRoute::Remote(_)) => None,
                Some(ChatRoute::Local(host)) => Some(host.fanout.clone()),
                None => Some(ChatFanout::default()),
            }
        };
        let Some(fanout) = local_fanout else {
            return self.dispatch(origin, chat_id, journal, command);
        };
        let sink: Box<dyn ChatEventSink> = if fanout.subscriber_count() == 0 {
            Box::new(view_events.clone())
        } else {
            Box::new(fanout)
        };
        if let Some((journal, id)) = &journal {
            journal.start(id)?;
        }
        match handle_direct_control(&self.runtime_control, storage, sink.as_ref(), &command) {
            Ok(_) => {
                if let Some((journal, id)) = &journal {
                    journal.complete(id, None)?;
                }
                Ok(())
            }
            Err(error) => {
                if let Some((journal, id)) = &journal {
                    journal.reject(id, error.clone())?;
                }
                Err(error)
            }
        }
    }

    fn command(
        &self,
        request: wire::ChatSessionCommandRequest,
    ) -> Result<wire::ChatSessionCommandResponse, String> {
        let accepted = Ok(wire::ChatSessionCommandResponse {
            accepted: true,
            reattach_required: false,
        });
        let execution_id = match self.execution_id(&request.client_id) {
            Ok(id) => id,
            Err(_) => {
                return Ok(wire::ChatSessionCommandResponse {
                    accepted: false,
                    reattach_required: true,
                });
            }
        };
        let (storage, events, journal, current_chat) = {
            let views = self
                .views
                .lock()
                .map_err(|_| "chat view registry lock poisoned".to_string())?;
            let view = views
                .get(&execution_id)
                .ok_or_else(|| format!("unknown chat client `{}`", request.client_id))?;
            view.touch();
            (
                view.storage.clone(),
                view.events.clone(),
                view.journal.clone(),
                view.chat_id(),
            )
        };
        match &request.command {
            wire::ChatSessionCommand::FetchAgents => {
                let (agents, warning) = match storage.open_agent_catalog_vfs() {
                    Ok(vfs) => local::agent_summaries_with_error(&vfs),
                    Err(error) => (
                        wire::fallback_agent_summaries(),
                        Some(format!("agent catalog unavailable: {error}")),
                    ),
                };
                events.push(wire::ChatEvent::Agents { agents, warning });
                return accepted;
            }
            wire::ChatSessionCommand::FetchModels { provider } => {
                let models = sinomo_lm::provider_available_model_ids(provider).unwrap_or_default();
                events.push(wire::ChatEvent::Models {
                    provider: provider.clone(),
                    models,
                });
                return accepted;
            }
            wire::ChatSessionCommand::ControlState { session_id } => {
                self.control(
                    &execution_id,
                    &storage,
                    &events,
                    session_id,
                    None,
                    request.command.clone(),
                )?;
                return accepted;
            }
            wire::ChatSessionCommand::FetchWorkspaces { session_id } => {
                let chat_id = self.command_target(&execution_id, session_id, current_chat)?;
                self.dispatch(&execution_id, &chat_id, None, request.command)?;
                return accepted;
            }
            _ => {}
        }
        let id = request
            .request_id
            .unwrap_or_else(|| SessionId::generate().to_string());
        if !journal.admit(&id, &request.command)? {
            return accepted;
        }
        match &request.command {
            wire::ChatSessionCommand::Steer { session_id, .. }
            | wire::ChatSessionCommand::FollowUp { session_id, .. }
            | wire::ChatSessionCommand::RemoveQueuedInput { session_id, .. }
            | wire::ChatSessionCommand::Interrupt { session_id, .. }
            | wire::ChatSessionCommand::Cancel { session_id, .. }
            | wire::ChatSessionCommand::ControlState { session_id } => {
                let _work = sinomo_runtime::rollout::acquire_runtime_control(&journal.data_dir())?;
                self.control(
                    &execution_id,
                    &storage,
                    &events,
                    session_id,
                    Some((journal, id)),
                    request.command.clone(),
                )?;
            }
            wire::ChatSessionCommand::Resume {
                session_id,
                history,
            } => {
                journal.start(&id)?;
                self.show_chat(&execution_id, session_id, history, &events)?;
                journal.complete(
                    &id,
                    recovery::resume_command(&request.command, session_id.clone()),
                )?;
            }
            wire::ChatSessionCommand::NewSession { session_id } => {
                journal.start(&id)?;
                self.attach_view(&execution_id, session_id, &[])?;
                events.push(wire::ChatEvent::SessionReset {
                    session_id: session_id.clone(),
                });
                journal.complete(
                    &id,
                    recovery::resume_command(&request.command, session_id.clone()),
                )?;
            }
            command => {
                let requested = command_chat_id(command).unwrap_or_default().to_string();
                let chat_id = self.command_target(&execution_id, &requested, current_chat)?;
                self.dispatch(
                    &execution_id,
                    &chat_id,
                    Some((journal, id)),
                    request.command,
                )?;
            }
        }
        accepted
    }

    /// The chat a command acts on: the one it names, or the view's current
    /// chat. Naming a different chat moves the view to it.
    fn command_target(
        &self,
        execution_id: &str,
        requested: &str,
        current: Option<String>,
    ) -> Result<String, String> {
        let requested = requested.trim();
        if requested.is_empty() {
            if let Some(current) = current {
                return Ok(current);
            }
            let chat_id = SessionId::generate().to_string();
            self.attach_view(execution_id, &chat_id, &[])?;
            return Ok(chat_id);
        }
        if current.as_deref() != Some(requested) {
            self.attach_view(execution_id, requested, &[])?;
        }
        Ok(requested.to_string())
    }

    /// Resume: show the chat and its current control state in this view.
    fn show_chat(
        &self,
        execution_id: &str,
        chat_id: &str,
        history: &[wire::ChatHistoryMessage],
        events: &ChatEventLog,
    ) -> Result<(), String> {
        self.attach_view(execution_id, chat_id, history)?;
        if let Ok(session_id) = SessionId::from_str(chat_id)
            && let Ok(response) = self
                .runtime_control
                .resume(AgentSessionResumeRequest { session_id })
        {
            events.push(wire::ChatEvent::ControlState(control_resume_state(
                response,
            )));
        }
        Ok(())
    }

    /// Delete a chat on the node that hosts it, where its live work and its
    /// sessions' records (including which sandboxes they own) are. A chat no
    /// node hosts, or whose host is unreachable, is deleted here.
    fn delete_chat_where_hosted(
        &self,
        data_dir: sinomo_runtime::local::SinomoDataDir,
        runtime: tokio::runtime::Handle,
        chat_id: &str,
    ) -> Result<(), String> {
        let environment = self.init_environment(&data_dir, &runtime);
        if let Ok(Some(ChatOwner::Remote(owner))) =
            environment.cluster.ownership.current_owner(chat_id)
        {
            match environment.cluster.peers.delete(
                &owner,
                wire::ChatPeerDeleteRequest {
                    chat_id: chat_id.to_string(),
                },
            ) {
                Ok(()) => return Ok(()),
                Err(error) => tracing::warn!(
                    %chat_id,
                    %owner,
                    %error,
                    "the chat's host node is unreachable; deleting the chat here"
                ),
            }
        }
        history_delete(
            &self.daemon_address,
            &self.runtime_control,
            data_dir,
            runtime,
            chat_id,
        )
    }

    /// A command from a window on another node for a chat this node hosts.
    fn peer_command(
        &self,
        request: wire::ChatPeerCommandRequest,
    ) -> Result<wire::ChatPeerCommandResponse, String> {
        let not_owner = Ok(wire::ChatPeerCommandResponse {
            accepted: false,
            not_owner: true,
        });
        if let Some(target) = command_chat_id(&request.command)
            && !target.trim().is_empty()
            && target != request.chat_id
        {
            return Err(format!(
                "peer command for chat `{target}` names host `{}`",
                request.chat_id
            ));
        }
        // Resolve any storage context before the ownership guard. Control's
        // durable history projection happens after the guarded local effect.
        let storage = if is_control_command(&request.command) {
            let environment = self.environment()?;
            Some(local::AgentChatSifsStorage::new(
                environment.data_dir,
                self.daemon_address.clone(),
                &environment.runtime,
            ))
        } else {
            None
        };
        let (fanout, control) = {
            let hosts = self.ensure_route(&request.chat_id, &[])?;
            let Some(ChatRoute::Local(host)) = hosts.get(&request.chat_id) else {
                return not_owner;
            };
            if !host.alive.load(Ordering::Acquire) {
                return not_owner;
            }
            host.activity.touch();
            let control = host
                .with_peer_request(&request.request_id, || {
                    if storage.is_some() {
                        apply_direct_control(&self.runtime_control, &request.command)
                    } else {
                        // Enqueue on the exact host just admitted, while retirement
                        // and route replacement are excluded by `hosts`. Calling
                        // dispatch here would re-resolve and possibly forward.
                        host.command_tx
                            .send(HostCommand::Run {
                                origin: request.origin,
                                journal: None,
                                command: request.command,
                            })
                            .map_err(|_| "chat host stopped".to_string())?;
                        Ok(None)
                    }
                })?
                .flatten();
            (host.fanout.clone(), control)
        };
        if let (Some(storage), Some(control)) = (storage, control) {
            publish_direct_control(&storage, &fanout, control);
        }
        Ok(wire::ChatPeerCommandResponse {
            accepted: true,
            not_owner: false,
        })
    }

    /// Events for a window on another node following a chat hosted here.
    fn peer_events(
        &self,
        request: wire::ChatPeerEventsRequest,
    ) -> Result<wire::ChatPeerEventsResponse, String> {
        let fanout = {
            let hosts = self.ensure_route(&request.chat_id, &[])?;
            match hosts.get(&request.chat_id) {
                Some(ChatRoute::Local(host)) => {
                    host.activity.touch();
                    host.fanout.clone()
                }
                _ => {
                    return Ok(wire::ChatPeerEventsResponse {
                        generation: String::new(),
                        snapshot: Vec::new(),
                        events: Vec::new(),
                        next_sequence: 0,
                        not_owner: true,
                    });
                }
            }
        };
        Ok(fanout.poll_peer(&request))
    }

    fn events(
        &self,
        request: wire::ChatSessionEventsRequest,
    ) -> Result<wire::ChatSessionEventsResponse, String> {
        let execution_id = self.execution_id(&request.client_id)?;
        let events = self
            .views
            .lock()
            .map_err(|_| "chat view registry lock poisoned".to_string())?
            .get(&execution_id)
            .map(|view| view.events.clone())
            .ok_or_else(|| format!("unknown chat client `{}`", request.client_id))?;
        Ok(events.poll(&request))
    }

    fn close(
        &self,
        request: wire::ChatSessionCloseRequest,
    ) -> Result<wire::ChatSessionCloseResponse, String> {
        let mut views = self.views.lock().map_err(|_| "chat registry poisoned")?;
        let mut clients = self
            .clients
            .lock()
            .map_err(|_| "chat subscription registry poisoned")?;
        let execution_id = clients.remove(&request.client_id);
        let ephemeral = self
            .ephemeral_clients
            .lock()
            .map_err(|_| "chat registry poisoned")?
            .remove(&request.client_id);
        if ephemeral
            && let Some(id) = &execution_id
            && !clients.values().any(|other| other == id)
            && let Some(view) = views.remove(id)
        {
            drop(clients);
            self.detach_view(id, &view);
        }
        // A durable view outlives a subscribing window. Legacy clients retain
        // their previous close semantics because they never negotiated a
        // stable view.
        Ok(wire::ChatSessionCloseResponse {
            closed: execution_id.is_some(),
        })
    }

    fn detach_view(&self, execution_id: &str, view: &ChatView) {
        if let Some(chat_id) = view.chat_id()
            && let Ok(hosts) = self.hosts.lock()
            && let Some(route) = hosts.get(&chat_id)
        {
            route.fanout().unsubscribe(execution_id);
        }
    }

    /// Release views no window is attached to that have been idle for at
    /// least `min_idle` (`None` uses the detached TTL), longest-idle first,
    /// up to `limit`. A released view is restored from its journal when its
    /// window returns.
    fn evict_idle_views(
        &self,
        views: &mut HashMap<String, ChatView>,
        min_idle: Option<Duration>,
        limit: usize,
    ) -> Result<usize, String> {
        let min_idle = min_idle.unwrap_or(DETACHED_CHAT_HOST_IDLE_TTL);
        let attached = self
            .clients
            .lock()
            .map_err(|_| "chat subscription registry poisoned")?
            .values()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let mut idle = views
            .iter()
            .filter(|(execution_id, _)| !attached.contains(*execution_id))
            .map(|(execution_id, view)| (view.idle_for(), execution_id.clone()))
            .filter(|(idle, _)| *idle >= min_idle)
            .collect::<Vec<_>>();
        idle.sort_by_key(|(idle, _)| std::cmp::Reverse(*idle));
        let mut evicted = 0;
        for (_, execution_id) in idle.into_iter().take(limit) {
            if let Some(view) = views.remove(&execution_id) {
                self.detach_view(&execution_id, &view);
                evicted += 1;
            }
        }
        Ok(evicted)
    }

    /// Release chat routes no view shows that have been idle for at least
    /// `min_idle` (`None` uses the detached TTL), longest-idle first, up to
    /// `limit`. A host is never released while a view shows its chat, while
    /// it is processing a command, or while its session has work in flight.
    /// A released host gives up its ownership of the chat.
    fn evict_idle_hosts(
        &self,
        hosts: &mut HashMap<String, ChatRoute>,
        min_idle: Option<Duration>,
        limit: usize,
    ) -> Result<usize, String> {
        let mut idle = hosts
            .iter()
            .filter(|(_, route)| route.fanout().subscriber_count() == 0)
            .filter_map(|(chat_id, route)| {
                // Following another node's chat costs a poll loop; stop soon
                // after the last window leaves.
                let threshold = min_idle.unwrap_or(match route {
                    ChatRoute::Local(_) => DETACHED_CHAT_HOST_IDLE_TTL,
                    ChatRoute::Remote(_) => REMOTE_CHAT_ROUTE_IDLE_TTL,
                });
                route
                    .idle_for(&self.runtime_control)
                    .filter(|idle| *idle >= threshold)
                    .map(|idle| (idle, chat_id.clone()))
            })
            .collect::<Vec<_>>();
        idle.sort_by_key(|(idle, _)| std::cmp::Reverse(*idle));
        let mut evicted = 0;
        for (_, chat_id) in idle.into_iter().take(limit) {
            // Dropping a host stops its worker, which releases the runtime.
            if let Some(ChatRoute::Local(_)) = hosts.remove(&chat_id)
                && let Ok(environment) = self.environment()
            {
                environment.cluster.ownership.release(&chat_id);
            }
            evicted += 1;
        }
        Ok(evicted)
    }

    /// Keep this node's chat ownership current: renew every hosted chat, and
    /// stop a host whose ownership moved elsewhere or could not be renewed in
    /// time, so two nodes never run one chat. Views of a stopped or dead
    /// route move to the chat's current host.
    fn maintain_routes(&self) -> Result<(), String> {
        let Ok(environment) = self.environment() else {
            return Ok(());
        };
        let chats = self
            .hosts
            .lock()
            .map_err(|_| "chat host registry poisoned")?
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for chat_id in chats {
            let local = matches!(
                self.hosts
                    .lock()
                    .map_err(|_| "chat host registry poisoned")?
                    .get(&chat_id),
                Some(ChatRoute::Local(_))
            );
            if local {
                let renewed = environment.cluster.ownership.renew(&chat_id);
                let hosts = self
                    .hosts
                    .lock()
                    .map_err(|_| "chat host registry poisoned")?;
                let Some(ChatRoute::Local(host)) = hosts.get(&chat_id) else {
                    continue;
                };
                let mut renewed_at = host
                    .renewed_at
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let lost = match renewed {
                    Ok(true) => {
                        *renewed_at = Instant::now();
                        false
                    }
                    Ok(false) => true,
                    // Fence ourselves before another node may take over.
                    Err(_) => renewed_at.elapsed() >= CHAT_OWNER_TTL / 2,
                };
                drop(renewed_at);
                if lost {
                    tracing::warn!(%chat_id, "chat ownership lost; stopping its host here");
                    if let Ok(session_id) = SessionId::from_str(&chat_id) {
                        let _ =
                            self.runtime_control
                                .cancel(sinomo_agent::AgentSessionCancelRequest {
                                    session_id,
                                    reason: "chat ownership moved to another node".into(),
                                });
                    }
                    host.alive.store(false, Ordering::Release);
                }
            }
            let orphaned = self
                .hosts
                .lock()
                .map_err(|_| "chat host registry poisoned")?
                .get(&chat_id)
                .is_some_and(|route| !route.alive() && route.fanout().subscriber_count() > 0);
            if orphaned && let Err(error) = self.ensure_route(&chat_id, &[]) {
                tracing::warn!(%chat_id, %error, "re-route chat views failed");
            }
        }
        Ok(())
    }

    fn sweep_idle_hosts(&self) -> Result<usize, String> {
        let evicted_views = {
            let mut views = self.views.lock().map_err(|_| "chat registry poisoned")?;
            self.evict_idle_views(&mut views, None, usize::MAX)?
        };
        let mut hosts = self.hosts.lock().map_err(|_| "chat registry poisoned")?;
        Ok(evicted_views + self.evict_idle_hosts(&mut hosts, None, usize::MAX)?)
    }

    /// Periodically keep chat ownership current, release idle detached views
    /// and hosts, and purge deleted chats whose retention window has passed.
    /// Runs until the runtime stops.
    fn start_maintenance(
        &self,
        data_dir: sinomo_runtime::local::SinomoDataDir,
        runtime: tokio::runtime::Handle,
    ) {
        let service = self.clone();
        let blocking = runtime.clone();
        runtime.spawn(async move {
            let mut next_purge = Instant::now();
            loop {
                let sweep = service.clone();
                let purge = (Instant::now() >= next_purge).then(|| {
                    next_purge = Instant::now() + DELETED_CHAT_PURGE_INTERVAL;
                    (
                        service.daemon_address.clone(),
                        data_dir.clone(),
                        blocking.clone(),
                    )
                });
                let _ = tokio::task::spawn_blocking(move || {
                    if let Err(error) = sweep.maintain_routes() {
                        tracing::warn!(%error, "chat ownership maintenance failed");
                    }
                    if let Err(error) = sweep.sweep_idle_hosts() {
                        tracing::warn!(%error, "chat host sweep failed");
                    }
                    if let Some((daemon_address, data_dir, runtime)) = purge {
                        match purge_deleted_chats(
                            &daemon_address,
                            data_dir,
                            &runtime,
                            DELETED_CHAT_RETENTION,
                        ) {
                            Ok(0) => {}
                            Ok(purged) => tracing::info!(purged, "purged deleted chats"),
                            Err(error) => tracing::warn!(%error, "deleted chat purge failed"),
                        }
                    }
                })
                .await;
                tokio::time::sleep(CHAT_HOST_SWEEP_INTERVAL).await;
            }
        });
    }

    pub(crate) fn recover(
        &self,
        data_dir: sinomo_runtime::local::SinomoDataDir,
        runtime: tokio::runtime::Handle,
    ) -> Result<(), String> {
        self.init_environment(&data_dir, &runtime);
        self.start_maintenance(data_dir.clone(), runtime.clone());
        for execution_id in CommandJournal::discover(&data_dir.root())? {
            let client_id = format!("recovery-{}", SessionId::generate());
            self.open(
                data_dir.clone(),
                runtime.clone(),
                wire::ChatSessionOpenRequest {
                    client_id: client_id.clone(),
                    execution_id: Some(execution_id),
                },
            )?;
            self.close(wire::ChatSessionCloseRequest { client_id })?;
        }
        Ok(())
    }

    fn execution_id(&self, client_id: &str) -> Result<String, String> {
        self.clients
            .lock()
            .map_err(|_| "chat subscription registry poisoned")?
            .get(client_id)
            .cloned()
            .ok_or_else(|| format!("unknown chat client `{client_id}`"))
    }
}

/// Follows a remote chat's events into this node's fan-out.
struct RemoteFollower {
    chat_id: String,
    owner: NodeId,
    cluster: ChatCluster,
    fanout: ChatFanout,
    alive: Arc<AtomicBool>,
}

impl RemoteFollower {
    fn run(self) {
        const MAX_FAILURES: u32 = 3;
        let mut after_sequence = 0;
        let mut generation: Option<String> = None;
        let mut failures = 0;
        while self.alive.load(Ordering::Acquire) {
            let response = self.cluster.peers.events(
                &self.owner,
                wire::ChatPeerEventsRequest {
                    chat_id: self.chat_id.clone(),
                    after_sequence,
                    max_events: wire::CHAT_EVENT_PAGE_MAX,
                    wait_ms: wire::CHAT_EVENT_WAIT_MAX_MS,
                },
            );
            let response = match response {
                Ok(response) if !response.not_owner => {
                    failures = 0;
                    response
                }
                Ok(_) => break,
                Err(error) => {
                    failures += 1;
                    if failures >= MAX_FAILURES {
                        tracing::warn!(chat_id = %self.chat_id, owner = %self.owner, %error, "lost the chat's host node");
                        break;
                    }
                    std::thread::sleep(Duration::from_secs(1));
                    continue;
                }
            };
            if generation.as_deref() != Some(response.generation.as_str()) {
                let replaced = generation.is_some();
                generation = Some(response.generation.clone());
                if replaced {
                    // The owner's host restarted: follow it from its start.
                    after_sequence = 0;
                    self.fanout
                        .push(wire::ChatEvent::Reattached { replay_gap: true });
                    continue;
                }
            }
            for event in response.snapshot {
                self.fanout.push(event);
            }
            for event in response.events {
                let origin = event
                    .origin
                    .as_deref()
                    .and_then(|origin| self.cluster.local_origin(origin));
                self.fanout.push_from(origin, event.event);
            }
            after_sequence = response.next_sequence;
        }
        // Views of this chat move to its current host on the next sweep or
        // command.
        self.alive.store(false, Ordering::Release);
    }
}

fn is_control_command(command: &wire::ChatSessionCommand) -> bool {
    matches!(
        command,
        wire::ChatSessionCommand::Steer { .. }
            | wire::ChatSessionCommand::FollowUp { .. }
            | wire::ChatSessionCommand::RemoveQueuedInput { .. }
            | wire::ChatSessionCommand::Interrupt { .. }
            | wire::ChatSessionCommand::Cancel { .. }
            | wire::ChatSessionCommand::ControlState { .. }
    )
}

/// The chat a command names, if any.
fn command_chat_id(command: &wire::ChatSessionCommand) -> Option<&str> {
    use wire::ChatSessionCommand::*;
    match command {
        Send { session_id, .. }
        | Steer { session_id, .. }
        | FollowUp { session_id, .. }
        | RemoveQueuedInput { session_id, .. }
        | Interrupt { session_id, .. }
        | Cancel { session_id, .. }
        | ControlState { session_id }
        | NewSession { session_id }
        | Resume { session_id, .. }
        | SetAgent { session_id, .. }
        | SetWorkspace { session_id, .. }
        | UpdateWorkspaces { session_id, .. }
        | FetchWorkspaces { session_id }
        | RealtimeHandoff { session_id, .. } => Some(session_id.as_str()),
        SetModel { .. }
        | SetToolRuntime { .. }
        | SetToolRuntimeNode { .. }
        | FetchModels { .. }
        | FetchAgents => None,
    }
}

impl Service for HostedChatService {
    fn name(&self) -> &'static str {
        "chat-session"
    }

    fn routes(&self) -> &'static [&'static str] {
        wire::CHAT_SERVICE_ROUTES
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        let peer_routes = wire::CHAT_PEER_RPC_ROUTES.iter().copied().map(|route| {
            let capability = if route == wire::CHAT_PEER_EVENTS_ROUTE {
                wire::CHAT_READ_CAPABILITY
            } else {
                wire::CHAT_WRITE_CAPABILITY
            };
            ServiceRoute::from_route_metadata(
                route,
                wire::CHAT_RPC_CATEGORY,
                wire::CHAT_PEER_ROUTE_AUTH_SCOPE,
                [capability],
            )
        });
        wire::CHAT_RPC_ROUTES
            .iter()
            .copied()
            .map(|route| {
                let capability = if matches!(
                    route,
                    wire::CHAT_SESSION_EVENTS_ROUTE
                        | wire::CHAT_SESSION_ACTIVITY_ROUTE
                        | wire::CHAT_HISTORY_LIST_ROUTE
                        | wire::CHAT_HISTORY_LOAD_ROUTE
                        | wire::CHAT_HISTORY_CHANNEL_ROUTE
                ) {
                    if route.starts_with("/chat/history/") {
                        wire::CHAT_HISTORY_READ_CAPABILITY
                    } else {
                        wire::CHAT_READ_CAPABILITY
                    }
                } else if route.starts_with("/chat/history/") {
                    wire::CHAT_HISTORY_WRITE_CAPABILITY
                } else {
                    wire::CHAT_WRITE_CAPABILITY
                };
                ServiceRoute::from_route_metadata(
                    route,
                    wire::CHAT_RPC_CATEGORY,
                    wire::CHAT_ROUTE_AUTH_SCOPE,
                    [capability],
                )
            })
            .chain(peer_routes)
            .collect()
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let service = self.clone();
        Box::pin(async move {
            let peer_route = wire::CHAT_PEER_RPC_ROUTES.contains(&request.route.as_str());
            // Peer routes serve other cluster members; everything else is the
            // local app's.
            if !peer_route && !ctx.signer.is_local() {
                return Err(ServiceError::Service(
                    "chat sessions are available only to the local app".to_string(),
                ));
            }
            match request.route.as_str() {
                wire::CHAT_PEER_COMMAND_ROUTE => {
                    let payload: wire::ChatPeerCommandRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    let response = tokio::task::spawn_blocking(move || {
                        service.init_environment(&data_dir, &runtime);
                        service.peer_command(payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(wire::CHAT_PEER_COMMAND_ROUTE, response)?;
                }
                wire::CHAT_PEER_DELETE_ROUTE => {
                    let payload: wire::ChatPeerDeleteRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    tokio::task::spawn_blocking(move || {
                        history_delete(
                            &service.daemon_address,
                            &service.runtime_control,
                            data_dir,
                            runtime,
                            &payload.chat_id,
                        )
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        wire::CHAT_PEER_DELETE_ROUTE,
                        wire::ChatHistoryMutationResponse { accepted: true },
                    )?;
                }
                wire::CHAT_PEER_EVENTS_ROUTE => {
                    let payload: wire::ChatPeerEventsRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    let response = tokio::task::spawn_blocking(move || {
                        service.init_environment(&data_dir, &runtime);
                        service.peer_events(payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(wire::CHAT_PEER_EVENTS_ROUTE, response)?;
                }
                wire::CHAT_SESSION_OPEN_ROUTE => {
                    let payload: wire::ChatSessionOpenRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    let response = tokio::task::spawn_blocking(move || {
                        service.open(data_dir, runtime, payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(wire::CHAT_SESSION_OPEN_ROUTE, response)?;
                }
                wire::CHAT_SESSION_COMMAND_ROUTE => {
                    let payload: wire::ChatSessionCommandRequest =
                        ctx.decode_request_body(&request).await?;
                    let response = tokio::task::spawn_blocking(move || service.command(payload))
                        .await
                        .map_err(|error| ServiceError::Service(error.to_string()))?
                        .map_err(ServiceError::Service)?;
                    ctx.respond_value(wire::CHAT_SESSION_COMMAND_ROUTE, response)?;
                }
                wire::CHAT_SESSION_EVENTS_ROUTE => {
                    let payload: wire::ChatSessionEventsRequest =
                        ctx.decode_request_body(&request).await?;
                    let response = tokio::task::spawn_blocking(move || service.events(payload))
                        .await
                        .map_err(|error| ServiceError::Service(error.to_string()))?
                        .map_err(ServiceError::Service)?;
                    ctx.respond_value(wire::CHAT_SESSION_EVENTS_ROUTE, response)?;
                }
                wire::CHAT_SESSION_CLOSE_ROUTE => {
                    let payload: wire::ChatSessionCloseRequest =
                        ctx.decode_request_body(&request).await?;
                    let response = tokio::task::spawn_blocking(move || service.close(payload))
                        .await
                        .map_err(|error| ServiceError::Service(error.to_string()))?
                        .map_err(ServiceError::Service)?;
                    ctx.respond_value(wire::CHAT_SESSION_CLOSE_ROUTE, response)?;
                }
                wire::CHAT_HISTORY_LIST_ROUTE => {
                    let _: wire::ChatHistoryListRequest = ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    let sessions = tokio::task::spawn_blocking(move || {
                        history_list(&service.daemon_address, data_dir, runtime)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        wire::CHAT_HISTORY_LIST_ROUTE,
                        wire::ChatHistoryListResponse { sessions },
                    )?;
                }
                wire::CHAT_HISTORY_LOAD_ROUTE => {
                    let payload: wire::ChatHistoryLoadRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    let response = tokio::task::spawn_blocking(move || {
                        history_load(&service.daemon_address, data_dir, runtime, payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(wire::CHAT_HISTORY_LOAD_ROUTE, response)?;
                }
                wire::CHAT_SESSION_ACTIVITY_ROUTE => {
                    let payload: wire::ChatSessionActivityRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let response = tokio::task::spawn_blocking(move || {
                        activity::session_activity(&data_dir, payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?;
                    ctx.respond_value(wire::CHAT_SESSION_ACTIVITY_ROUTE, response)?;
                }
                wire::CHAT_HISTORY_CHANNEL_ROUTE => {
                    let payload: wire::ChatHistoryChannelRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    let channel = tokio::task::spawn_blocking(move || {
                        history_channel(
                            &service.daemon_address,
                            data_dir,
                            runtime,
                            &payload.session_id,
                        )
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        wire::CHAT_HISTORY_CHANNEL_ROUTE,
                        wire::ChatHistoryChannelResponse { channel },
                    )?;
                }
                wire::CHAT_HISTORY_CREATE_CHANNEL_ROUTE => {
                    let payload: wire::ChatHistoryCreateChannelRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    tokio::task::spawn_blocking(move || {
                        history_create_channel(&service.daemon_address, data_dir, runtime, payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        wire::CHAT_HISTORY_CREATE_CHANNEL_ROUTE,
                        wire::ChatHistoryMutationResponse { accepted: true },
                    )?;
                }
                wire::CHAT_HISTORY_RENAME_ROUTE => {
                    let payload: wire::ChatHistoryRenameRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    tokio::task::spawn_blocking(move || {
                        history_rename(&service.daemon_address, data_dir, runtime, payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        wire::CHAT_HISTORY_RENAME_ROUTE,
                        wire::ChatHistoryMutationResponse { accepted: true },
                    )?;
                }
                wire::CHAT_HISTORY_DELETE_ROUTE => {
                    let payload: wire::ChatHistoryDeleteRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    tokio::task::spawn_blocking(move || {
                        service.delete_chat_where_hosted(data_dir, runtime, &payload.session_id)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        wire::CHAT_HISTORY_DELETE_ROUTE,
                        wire::ChatHistoryMutationResponse { accepted: true },
                    )?;
                }
                wire::CHAT_HISTORY_PARTICIPANT_ROUTE => {
                    let payload: wire::ChatHistoryParticipantRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    tokio::task::spawn_blocking(move || {
                        history_participant(&service.daemon_address, data_dir, runtime, payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        wire::CHAT_HISTORY_PARTICIPANT_ROUTE,
                        wire::ChatHistoryMutationResponse { accepted: true },
                    )?;
                }
                wire::CHAT_HISTORY_APPEND_AGENT_ROUTE => {
                    let payload: wire::ChatHistoryAppendAgentRequest =
                        ctx.decode_request_body(&request).await?;
                    let data_dir = ctx.data_dir.as_ref().clone();
                    let runtime = tokio::runtime::Handle::current();
                    tokio::task::spawn_blocking(move || {
                        history_append_agent(&service.daemon_address, data_dir, runtime, payload)
                    })
                    .await
                    .map_err(|error| ServiceError::Service(error.to_string()))?
                    .map_err(ServiceError::Service)?;
                    ctx.respond_value(
                        wire::CHAT_HISTORY_APPEND_AGENT_ROUTE,
                        wire::ChatHistoryMutationResponse { accepted: true },
                    )?;
                }
                other => return Err(ServiceError::UnknownRoute(other.to_string())),
            }
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_hosted_chat_service(
    builder: ServiceRegistryBuilder,
    daemon_address: impl Into<String>,
    runtime_control: AgentRuntimeControlRegistry,
) -> ServiceRegistryBuilder {
    builder.register(HostedChatService::new(daemon_address, runtime_control))
}

struct ChatHostContext {
    chat_id: String,
    history: Vec<wire::ChatHistoryMessage>,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime_control: AgentRuntimeControlRegistry,
    storage: local::AgentChatSifsStorage,
    fanout: ChatFanout,
    ready: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
    activity: HostActivity,
}

/// Runs one chat: the only runtime for it on this node. Commands from every
/// view of the chat execute here in order, and events go to all of them.
fn chat_host_worker(context: ChatHostContext, command_rx: Receiver<HostCommand>) {
    let ChatHostContext {
        chat_id,
        history,
        data_dir,
        runtime_control,
        storage,
        fanout,
        ready,
        alive,
        activity,
    } = context;
    struct WorkerLife(Arc<AtomicBool>);
    impl Drop for WorkerLife {
        fn drop(&mut self) {
            self.0.store(false, Ordering::Release);
        }
    }
    let _life = WorkerLife(alive.clone());
    let history = history
        .into_iter()
        .map(|message| local::ChatHistoryMessage {
            role: message.role,
            text: message.text,
        })
        .collect::<Vec<_>>();
    let Some(mut chat) = open_chat_host(&ChatHostOpening {
        chat_id: &chat_id,
        history: &history,
        data_dir: &data_dir,
        runtime_control: &runtime_control,
        storage: &storage,
        fanout: &fanout,
        alive: &alive,
    }) else {
        return;
    };
    ready.store(true, Ordering::Release);
    activity.end(SessionId::from_str(&chat.session_id()).ok());
    fanout.push(wire::ChatEvent::Ready {
        session_id: chat.session_id(),
        info: runtime_info(chat.runtime_info()),
    });
    fanout.push(wire::ChatEvent::ToolRuntime(tool_runtime(&chat)));
    if let Ok(state) = chat.control_state() {
        fanout.push(wire::ChatEvent::ControlState(control_state(state)));
    }
    loop {
        if !alive.load(Ordering::Acquire) {
            break;
        }
        let (origin, journal, command) = match command_rx.recv_timeout(ASYNC_CONTINUATION_POLL) {
            Ok(HostCommand::Run {
                origin,
                journal,
                command,
            }) => (origin, journal, command),
            Ok(HostCommand::Shutdown) | Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {
                activity.set_participant_work(chat.has_participant_work());
                // Background tool calls that completed while the chat was
                // idle continue the session: the agent posts on its own.
                if chat.has_async_updates() {
                    activity.begin();
                    let result = sinomo_runtime::rollout::acquire_runtime_work_until(
                        &data_dir.root(),
                        || !alive.load(Ordering::Acquire),
                    )
                    .map(|_work| run_async_continuation(&mut chat, &fanout));
                    activity.end(SessionId::from_str(&chat.session_id()).ok());
                    if let Err(error) = result {
                        fanout.push(wire::ChatEvent::Error {
                            session_id: Some(chat.session_id()),
                            error,
                        });
                    }
                }
                continue;
            }
        };
        activity.begin();
        let result = (|| {
            let _work =
                sinomo_runtime::rollout::acquire_runtime_work_until(&data_dir.root(), || {
                    !alive.load(Ordering::Acquire)
                })?;
            if let Some((journal, id)) = &journal {
                journal.start(id)?;
            }
            if let Some(requested) = command_chat_id(&command)
                && !requested.trim().is_empty()
                && requested != chat_id
            {
                return Err(format!(
                    "command for chat `{requested}` reached the host of chat `{chat_id}`"
                ));
            }
            // Other views of the chat show the message another view sent.
            if let wire::ChatSessionCommand::Send { text, .. } = &command {
                fanout.push_from(
                    Some(&origin),
                    wire::ChatEvent::UserMessage {
                        session_id: chat_id.clone(),
                        text: text.clone(),
                    },
                );
            }
            if !handle_direct_control(&runtime_control, &storage, &fanout, &command)? {
                run_worker_command(&mut chat, command.clone(), &fanout);
            }
            match &journal {
                Some((journal, id)) => {
                    journal.complete(id, recovery::resume_command(&command, chat.session_id()))
                }
                None => Ok(()),
            }
        })();
        activity.end(SessionId::from_str(&chat.session_id()).ok());
        if let Err(error) = result {
            if let Some((journal, id)) = &journal {
                let _ = journal.reject(id, error.clone());
            }
            fanout.push(wire::ChatEvent::Error {
                session_id: Some(chat.session_id()),
                error,
            });
        }
    }
    // The host is gone: stop routing control to its runtime so the runtime,
    // its tool executors, and its subagents are released with the worker.
    if let Err(error) = chat.release_runtime_control() {
        tracing::warn!(chat_id = %chat_id, %error, "release chat runtime control failed");
    }
}

/// How long a chat host waits for cluster services that are still starting
/// before it reports the chat unavailable.
const CHAT_HOST_STARTUP_WAIT: Duration = Duration::from_secs(30);
const CHAT_HOST_STARTUP_RETRY: Duration = Duration::from_millis(250);
/// Starts the error of a cluster store whose Raft node another process still
/// holds, as while a replacement daemon takes over from its predecessor.
const RAFT_NODE_BUSY_ERROR: &str = "raft node is busy";

struct ChatHostOpening<'a> {
    chat_id: &'a str,
    history: &'a [local::ChatHistoryMessage],
    data_dir: &'a sinomo_runtime::local::SinomoDataDir,
    runtime_control: &'a sinomo_agent::AgentRuntimeControlRegistry,
    storage: &'a local::AgentChatSifsStorage,
    fanout: &'a ChatFanout,
    alive: &'a AtomicBool,
}

/// Open the chat and restore its session. While the daemon is still starting
/// (its cluster services unpublished, or its Raft node held by the process it
/// replaces) a chat would fail for a moment; the host waits for them instead,
/// telling viewers once, and reports only a failure that outlasts the wait.
fn open_chat_host(opening: &ChatHostOpening<'_>) -> Option<local::AgentChat> {
    let deadline = Instant::now() + CHAT_HOST_STARTUP_WAIT;
    let mut waiting = false;
    loop {
        let (error, session_id) = match local::AgentChat::with_runtime_control_registry_and_sifs(
            opening.data_dir.clone(),
            true,
            opening.runtime_control.clone(),
            opening.storage.clone(),
        ) {
            Ok(mut chat) => match chat.resume_session(opening.chat_id, opening.history) {
                Ok(()) => return Some(chat),
                Err(error) => (
                    format!("chat restoration failed: {error}"),
                    Some(opening.chat_id.to_string()),
                ),
            },
            Err(error) => (format!("chat unavailable: {error}"), None),
        };
        if chat_startup_failure_is_transient(&error)
            && Instant::now() < deadline
            && opening.alive.load(Ordering::Acquire)
        {
            if !waiting {
                waiting = true;
                tracing::info!(chat_id = %opening.chat_id, %error, "chat host waiting for cluster services");
                opening.fanout.push(wire::ChatEvent::Notice {
                    session_id: opening.chat_id.to_string(),
                    notice: "waiting for cluster services to start".to_string(),
                });
            }
            std::thread::sleep(CHAT_HOST_STARTUP_RETRY);
            continue;
        }
        tracing::warn!(chat_id = %opening.chat_id, %error, "daemon chat host initialization failed");
        opening
            .fanout
            .push(wire::ChatEvent::Error { session_id, error });
        return None;
    }
}

fn chat_startup_failure_is_transient(error: &str) -> bool {
    error.contains(local::NO_SIFS_PROVIDER_ERROR) || error.contains(RAFT_NODE_BUSY_ERROR)
}

/// How often an idle chat host checks for completed background work.
const ASYNC_CONTINUATION_POLL: Duration = Duration::from_millis(250);

/// Run the turns that continue a chat after its background tool calls
/// completed, framed like a user turn so every view shows it streaming.
fn run_async_continuation(chat: &mut local::AgentChat, events: &dyn ChatEventSink) {
    let session_id = chat.session_id();
    events.push(wire::ChatEvent::TurnStarted {
        session_id: session_id.clone(),
    });
    let result = chat.continue_async_updates(&mut |event| {
        if let Some(event) = stream_event(event, &session_id) {
            events.push(event);
        }
    });
    if let Err(error) = result {
        events.push(wire::ChatEvent::Error {
            session_id: Some(session_id.clone()),
            error: error.to_string(),
        });
    }
    events.push(wire::ChatEvent::TurnDone { session_id });
}

fn run_worker_command(
    chat: &mut local::AgentChat,
    command: wire::ChatSessionCommand,
    events: &dyn ChatEventSink,
) {
    match command {
        wire::ChatSessionCommand::Send {
            session_id,
            text,
            attachments,
        } => {
            let session_id = match ensure_session(chat, session_id) {
                Ok(session_id) => session_id,
                Err(error) => {
                    events.push(wire::ChatEvent::Error {
                        session_id: None,
                        error,
                    });
                    return;
                }
            };
            let attachments = attachments
                .into_iter()
                .map(local_attachment)
                .collect::<Vec<_>>();
            let result = chat.send_with_attachments(&text, &attachments, &mut |event| {
                if let Some(event) = stream_event(event, &session_id) {
                    events.push(event);
                }
            });
            if let Err(error) = result {
                events.push(wire::ChatEvent::Error {
                    session_id: Some(session_id.clone()),
                    error: error.to_string(),
                });
            }
            events.push(wire::ChatEvent::TurnDone { session_id });
        }
        wire::ChatSessionCommand::NewSession { session_id } => {
            match chat.resume_session(&session_id, &[]) {
                Ok(()) => {
                    events.push(wire::ChatEvent::SessionReset { session_id });
                    events.push(wire::ChatEvent::ToolRuntime(tool_runtime(chat)));
                }
                Err(error) => events.push(wire::ChatEvent::Error {
                    session_id: Some(session_id),
                    error: format!("new session failed: {error}"),
                }),
            }
        }
        wire::ChatSessionCommand::Resume {
            session_id,
            history,
        } => {
            let history = history
                .into_iter()
                .map(|message| local::ChatHistoryMessage {
                    role: message.role,
                    text: message.text,
                })
                .collect::<Vec<_>>();
            match chat.resume_session(&session_id, &history) {
                Ok(()) => {
                    events.push(wire::ChatEvent::ToolRuntime(tool_runtime(chat)));
                    match chat.control_state() {
                        Ok(state) => {
                            events.push(wire::ChatEvent::ControlState(control_state(state)))
                        }
                        Err(error) => events.push(wire::ChatEvent::Error {
                            session_id: Some(session_id),
                            error: format!("control state failed after resume: {error}"),
                        }),
                    }
                }
                Err(error) => events.push(wire::ChatEvent::Error {
                    session_id: Some(session_id),
                    error: format!("resume failed: {error}"),
                }),
            }
        }
        wire::ChatSessionCommand::SetModel {
            provider,
            model,
            account,
        } => match chat.set_model(provider, model, account) {
            Ok(info) => events.push(wire::ChatEvent::RuntimeInfo(runtime_info(info))),
            Err(error) => events.push(wire::ChatEvent::Error {
                session_id: Some(chat.session_id()),
                error: format!("model switch failed: {error}"),
            }),
        },
        wire::ChatSessionCommand::SetAgent {
            session_id,
            key,
            history: _,
        } => match ensure_session(chat, session_id.clone())
            .and_then(|_| chat.set_agent(&key).map_err(|e| e.to_string()))
        {
            Ok(info) => {
                events.push(wire::ChatEvent::RuntimeInfo(runtime_info(info)));
                events.push(wire::ChatEvent::ToolRuntime(tool_runtime(chat)));
            }
            Err(error) => events.push(wire::ChatEvent::Error {
                session_id: Some(session_id),
                error: format!("agent switch failed: {error}"),
            }),
        },
        wire::ChatSessionCommand::SetToolRuntime { selection } => {
            match chat.set_tool_runtime(local_tool_runtime(selection)) {
                Ok(_) => events.push(wire::ChatEvent::ToolRuntime(tool_runtime(chat))),
                Err(error) => events.push(wire::ChatEvent::Error {
                    session_id: Some(chat.session_id()),
                    error: format!("tool runtime switch failed: {error}"),
                }),
            }
        }
        wire::ChatSessionCommand::SetWorkspace {
            session_id,
            workspace_root,
        } => match ensure_session(chat, session_id.clone()).and_then(|_| {
            chat.replace_workspace_root(workspace_root)
                .map_err(|error| error.to_string())
        }) {
            Ok(_) => events.push(wire::ChatEvent::ToolRuntime(tool_runtime(chat))),
            Err(error) => events.push(wire::ChatEvent::Error {
                session_id: Some(session_id),
                error: format!("workspace switch failed: {error}"),
            }),
        },
        wire::ChatSessionCommand::UpdateWorkspaces {
            session_id,
            request_id,
            change,
        } => {
            let result = if session_id.trim().is_empty() {
                Err("A workspace update requires a conversation id.".into())
            } else {
                ensure_session(chat, session_id.clone()).and_then(|_| {
                    match change {
                        wire::ChatWorkspaceChange::AddFolder { path } => {
                            chat.add_workspace_root(path)
                        }
                        wire::ChatWorkspaceChange::Remove { workspace_id } => {
                            chat.remove_workspace(&workspace_id)
                        }
                        wire::ChatWorkspaceChange::MakeDefault { workspace_id } => {
                            chat.make_workspace_default(&workspace_id)
                        }
                        wire::ChatWorkspaceChange::Clear => chat.clear_workspaces(),
                    }
                    .map(|_| ())
                    .map_err(|error| error.to_string())
                })
            };
            let selection = (chat.session_id() == session_id).then(|| tool_runtime(chat));
            events.push(wire::ChatEvent::WorkspacesUpdated {
                session_id,
                request_id,
                selection,
                error: result.err(),
            });
        }
        wire::ChatSessionCommand::FetchWorkspaces { session_id } => {
            match ensure_session(chat, session_id.clone())
                .and_then(|_| chat.reload_workspaces().map_err(|error| error.to_string()))
            {
                Ok(_) => events.push(wire::ChatEvent::ToolRuntime(tool_runtime(chat))),
                Err(error) => events.push(wire::ChatEvent::Error {
                    session_id: Some(session_id),
                    error: format!("Workspace list unavailable: {error}"),
                }),
            }
        }
        wire::ChatSessionCommand::SetToolRuntimeNode { sandbox_node_id } => {
            match chat.set_tool_runtime_node(sandbox_node_id) {
                Ok(_) => events.push(wire::ChatEvent::ToolRuntime(tool_runtime(chat))),
                Err(error) => events.push(wire::ChatEvent::Error {
                    session_id: Some(chat.session_id()),
                    error: format!("sandbox placement switch failed: {error}"),
                }),
            }
        }
        wire::ChatSessionCommand::FetchModels { provider } => {
            let models = sinomo_lm::provider_available_model_ids(&provider).unwrap_or_default();
            events.push(wire::ChatEvent::Models { provider, models });
        }
        wire::ChatSessionCommand::FetchAgents => {}
        wire::ChatSessionCommand::RealtimeHandoff {
            request_id,
            session_id,
            input,
        } => {
            let result = ensure_session(chat, session_id).and_then(|_| {
                let mut visible = String::new();
                chat.send_with_attachments(&input, &[], &mut |event| {
                    if let local::ChatStreamEvent::Token(text)
                    | local::ChatStreamEvent::AssistantTextDelta { text, .. }
                    | local::ChatStreamEvent::AgentToken { text, .. } = event
                    {
                        visible.push_str(&text);
                    }
                })
                .map_err(|error| error.to_string())?;
                Ok(visible)
            });
            events.push(wire::ChatEvent::RealtimeHandoffResult { request_id, result });
        }
        wire::ChatSessionCommand::Steer { .. }
        | wire::ChatSessionCommand::FollowUp { .. }
        | wire::ChatSessionCommand::RemoveQueuedInput { .. }
        | wire::ChatSessionCommand::Interrupt { .. }
        | wire::ChatSessionCommand::Cancel { .. }
        | wire::ChatSessionCommand::ControlState { .. } => {}
    }
}

fn handle_direct_control(
    registry: &AgentRuntimeControlRegistry,
    storage: &local::AgentChatSifsStorage,
    events: &dyn ChatEventSink,
    command: &wire::ChatSessionCommand,
) -> Result<bool, String> {
    let Some(control) = apply_direct_control(registry, command)? else {
        return Ok(false);
    };
    publish_direct_control(storage, events, control);
    Ok(true)
}

enum DirectControlEffect {
    State(AgentSessionResumeResponse),
    Input(String, AgentSessionControlResult),
}

/// Apply to the local runtime before releasing the selected host's ownership.
/// No routed lookup or history I/O is allowed at this effect boundary.
fn apply_direct_control(
    registry: &AgentRuntimeControlRegistry,
    command: &wire::ChatSessionCommand,
) -> Result<Option<DirectControlEffect>, String> {
    let (requested_session_id, action): (&str, Box<dyn FnOnce(SessionId) -> Result<_, _>>) =
        match command {
            wire::ChatSessionCommand::Steer {
                session_id,
                message,
            } => {
                let registry = registry.clone();
                let message = message.clone();
                (
                    session_id,
                    Box::new(move |session_id| {
                        registry.steer(AgentSessionSteerRequest {
                            session_id,
                            message,
                        })
                    }),
                )
            }
            wire::ChatSessionCommand::FollowUp {
                session_id,
                message,
            } => {
                let registry = registry.clone();
                let message = message.clone();
                (
                    session_id,
                    Box::new(move |session_id| {
                        registry.follow_up(AgentSessionFollowUpRequest {
                            session_id,
                            message,
                        })
                    }),
                )
            }
            wire::ChatSessionCommand::RemoveQueuedInput {
                session_id,
                input_id,
            } => {
                let registry = registry.clone();
                let input_id = TaskId::from_str(input_id).map_err(|error| error.to_string())?;
                (
                    session_id,
                    Box::new(move |session_id| {
                        registry.remove_queued_input(AgentSessionRemoveQueuedInputRequest {
                            session_id,
                            input_id,
                        })
                    }),
                )
            }
            wire::ChatSessionCommand::Interrupt { session_id, reason } => {
                let registry = registry.clone();
                let reason = reason.clone();
                (
                    session_id,
                    Box::new(move |session_id| {
                        registry.interrupt(AgentSessionInterruptRequest { session_id, reason })
                    }),
                )
            }
            wire::ChatSessionCommand::Cancel { session_id, reason } => {
                let registry = registry.clone();
                let reason = reason.clone();
                (
                    session_id,
                    Box::new(move |session_id| {
                        registry
                            .cancel(sinomo_agent::AgentSessionCancelRequest { session_id, reason })
                    }),
                )
            }
            wire::ChatSessionCommand::ControlState { session_id } => {
                let session_id =
                    SessionId::from_str(session_id).map_err(|error| error.to_string())?;
                let response = registry
                    .resume(AgentSessionResumeRequest { session_id })
                    .map_err(|error| error.to_string())?;
                return Ok(Some(DirectControlEffect::State(response)));
            }
            _ => return Ok(None),
        };
    let session_id =
        SessionId::from_str(requested_session_id).map_err(|error| error.to_string())?;
    let resumes = matches!(
        command,
        wire::ChatSessionCommand::Steer { .. } | wire::ChatSessionCommand::FollowUp { .. }
    );
    let mut result = action(session_id.clone()).map_err(|error| error.to_string())?;
    // A new message resumes an interrupted (paused) session. The queued
    // message is then consumed by the continuing turn.
    if resumes && result.accepted && result.snapshot.paused {
        let unpaused = registry
            .unpause(sinomo_agent::AgentSessionUnpauseRequest { session_id })
            .map_err(|error| error.to_string())?;
        result.snapshot = unpaused.snapshot;
    }
    Ok(Some(DirectControlEffect::Input(
        requested_session_id.to_string(),
        result,
    )))
}

fn publish_direct_control(
    storage: &local::AgentChatSifsStorage,
    events: &dyn ChatEventSink,
    control: DirectControlEffect,
) {
    match control {
        DirectControlEffect::State(response) => {
            events.push(wire::ChatEvent::ControlState(control_resume_state(
                response,
            )));
        }
        DirectControlEffect::Input(session, result) => {
            project_control_input(storage, &session, &result, events);
            events.push(wire::ChatEvent::ControlStatus(control_result(result)));
        }
    }
}

fn project_control_input(
    storage: &local::AgentChatSifsStorage,
    stable_channel_id: &str,
    result: &AgentSessionControlResult,
    events: &dyn ChatEventSink,
) {
    if !result.accepted || local::chat_queued_input_from_events(&result.events).is_none() {
        return;
    }
    let Ok(channel_id) = SessionId::from_str(stable_channel_id) else {
        return;
    };
    let projection = storage.open_channel_store().and_then(|store| {
        local::project_accepted_chat_control_input(&store, &channel_id, result)
            .map_err(|error| error.to_string())
    });
    if let Err(error) = projection {
        events.push(wire::ChatEvent::Notice {
            session_id: stable_channel_id.to_string(),
            notice: format!("control accepted, but stable chat history is unavailable: {error}"),
        });
    }
}

fn ensure_session(chat: &mut local::AgentChat, session_id: String) -> Result<String, String> {
    if session_id.trim().is_empty() || chat.session_id() == session_id {
        return Ok(chat.session_id());
    }
    chat.resume_session(&session_id, &[])
        .map_err(|error| format!("resume failed before send: {error}"))?;
    Ok(session_id)
}

fn local_attachment(input: wire::ChatAttachmentInput) -> local::ChatAttachmentInput {
    let source = match input.source {
        wire::ChatAttachmentSource::LocalPath(path) => local::ChatAttachmentSource::LocalPath(path),
        wire::ChatAttachmentSource::LocalSnapshot { provenance, bytes } => {
            local::ChatAttachmentSource::LocalSnapshot {
                provenance: local::ChatLocalAttachmentProvenance {
                    original_path: provenance.original_path,
                    canonical_path: provenance.canonical_path,
                    modified_at_unix_nanos: provenance.modified_at_unix_nanos,
                    unsaved_editor_buffer: provenance.unsaved_editor_buffer,
                },
                bytes,
            }
        }
        wire::ChatAttachmentSource::SifsSnapshot { provenance, bytes } => {
            local::ChatAttachmentSource::SifsSnapshot {
                provenance: local::ChatSifsAttachmentProvenance {
                    path: provenance.path,
                    object_id: provenance.object_id,
                    version: provenance.version,
                    content_hash: provenance.content_hash,
                    policy: provenance.policy,
                    unsaved_editor_buffer: provenance.unsaved_editor_buffer,
                },
                bytes,
            }
        }
    };
    local::ChatAttachmentInput {
        file_name: input.file_name,
        source,
    }
}

fn stream_event(event: local::ChatStreamEvent, session_id: &str) -> Option<wire::ChatEvent> {
    Some(match event {
        local::ChatStreamEvent::Token(text) => wire::ChatEvent::Token {
            session_id: session_id.to_string(),
            text,
        },
        local::ChatStreamEvent::AssistantTextDelta { step_index, text } => {
            wire::ChatEvent::AssistantTextDelta {
                session_id: session_id.to_string(),
                step_index,
                text,
            }
        }
        local::ChatStreamEvent::ReasoningDelta { step_index, delta } => {
            wire::ChatEvent::ReasoningDelta {
                session_id: session_id.to_string(),
                step_index,
                delta,
            }
        }
        local::ChatStreamEvent::ReasoningSummary {
            step_index,
            summary,
        } => wire::ChatEvent::ReasoningSummary {
            session_id: session_id.to_string(),
            step_index,
            summary,
        },
        local::ChatStreamEvent::AgentToken { speaker, text } => wire::ChatEvent::AgentToken {
            session_id: session_id.to_string(),
            speaker: speaker_wire(speaker),
            text,
        },
        local::ChatStreamEvent::SpeakerStarted(speaker) => wire::ChatEvent::SpeakerStarted {
            session_id: session_id.to_string(),
            speaker: speaker_wire(speaker),
        },
        local::ChatStreamEvent::Notice(notice) if notice.is_empty() => return None,
        local::ChatStreamEvent::Notice(notice) => wire::ChatEvent::Notice {
            session_id: session_id.to_string(),
            notice,
        },
        local::ChatStreamEvent::ChannelSystem { kind, text, .. } => {
            wire::ChatEvent::SystemMessage {
                session_id: session_id.to_string(),
                kind,
                text,
            }
        }
        local::ChatStreamEvent::Context(context) => wire::ChatEvent::Context {
            session_id: session_id.to_string(),
            context: context_wire(context),
        },
        local::ChatStreamEvent::ToolCallStarted(call) => wire::ChatEvent::ToolCallStarted {
            session_id: session_id.to_string(),
            call,
        },
        local::ChatStreamEvent::ToolCallDelta {
            step_index,
            call_id,
            tool,
            arguments_delta,
        } => wire::ChatEvent::ToolCallDelta {
            session_id: session_id.to_string(),
            step_index,
            call_id,
            tool,
            arguments_delta,
        },
        local::ChatStreamEvent::ToolCallCompleted(call) => wire::ChatEvent::ToolCallCompleted {
            session_id: session_id.to_string(),
            call,
        },
        local::ChatStreamEvent::ToolArtifacts(_) | local::ChatStreamEvent::OpStarted { .. } => {
            return None;
        }
        local::ChatStreamEvent::ToolOutput(output) => wire::ChatEvent::ToolOutput {
            session_id: session_id.to_string(),
            output,
        },
        local::ChatStreamEvent::Process(process) => wire::ChatEvent::Process {
            session_id: session_id.to_string(),
            process,
        },
        local::ChatStreamEvent::Usage(usage) => wire::ChatEvent::Usage {
            session_id: session_id.to_string(),
            usage: usage_wire(usage),
        },
    })
}

fn runtime_info(info: local::ChatRuntimeInfo) -> wire::RuntimeInfo {
    wire::RuntimeInfo {
        agent: info.agent,
        provider: info.provider,
        model: info.model,
        account: info.account,
        effort: info.reasoning_effort,
    }
}

fn tool_runtime(chat: &local::AgentChat) -> wire::ChatToolRuntimeSelection {
    let selection = chat.tool_runtime();
    wire::ChatToolRuntimeSelection {
        workspace_session_id: Some(chat.session_id()),
        workspaces: Some(
            chat.workspaces()
                .into_iter()
                .map(|workspace| wire::ChatWorkspace {
                    path: workspace
                        .sifs_root
                        .map(|path| format!("sifs:{path}"))
                        .or_else(|| workspace.local_root.map(|path| path.display().to_string()))
                        .unwrap_or_else(|| workspace.id.clone()),
                    id: workspace.id,
                })
                .collect(),
        ),
        sandbox_node_id: selection.sandbox_node_id,
        workspace_root: selection.workspace_root,
    }
}

fn local_tool_runtime(
    selection: wire::ChatToolRuntimeSelection,
) -> local::ChatToolRuntimeSelection {
    local::ChatToolRuntimeSelection {
        sandbox_node_id: selection.sandbox_node_id,
        workspace_root: selection.workspace_root,
    }
}

fn speaker_wire(speaker: local::ChatSpeaker) -> wire::ChatSpeaker {
    wire::ChatSpeaker {
        channel_id: speaker.channel_id,
        participant_id: speaker.participant_id,
        agent_key: speaker.agent_key,
        agent_name: speaker.agent_name,
        agent_session_id: speaker.agent_session_id,
    }
}

fn context_wire(context: local::ChatContextSnapshot) -> wire::ChatContextSnapshot {
    wire::ChatContextSnapshot {
        step_index: context.step_index,
        provider: context.provider,
        model: context.model,
        total_bytes: context.total_bytes,
        max_context_bytes: context.max_context_bytes,
        compaction_threshold_bytes: context.compaction_threshold_bytes,
        lanes: context
            .lanes
            .into_iter()
            .map(|lane| wire::ChatContextLane {
                kind: lane.kind,
                label: lane.label,
                bytes: lane.bytes,
                priority: lane.priority,
            })
            .collect(),
        included_count: context.included_count,
        omitted_count: context.omitted_count,
    }
}

fn usage_wire(usage: local::ChatUsageSnapshot) -> wire::ChatUsageSnapshot {
    wire::ChatUsageSnapshot {
        provider: usage.provider,
        model: usage.model,
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
    }
}

fn control_result(result: AgentSessionControlResult) -> wire::ChatControlStatus {
    let queued_input = local::chat_queued_input_from_events(&result.events).map(|queued| {
        wire::ChatQueuedTurnInput {
            input_id: queued.input_id,
            kind: match queued.kind {
                local::ChatQueuedTurnInputKind::Steering => wire::ChatQueuedTurnInputKind::Steering,
                local::ChatQueuedTurnInputKind::FollowUp => wire::ChatQueuedTurnInputKind::FollowUp,
            },
            message: queued.message,
        }
    });
    wire::ChatControlStatus {
        session_id: result.snapshot.session_id.to_string(),
        accepted: result.accepted,
        runtime_attached: true,
        sequence: result.sequence,
        event_count: result.events.len(),
        active_turn: result.snapshot.active_turn.is_some(),
        task_count: result.snapshot.tasks.len(),
        terminal_result_count: result.terminal_results().len(),
        tasks: result.snapshot.tasks.iter().map(task_wire).collect(),
        queued_input,
        removed_input_id: local::chat_removed_input_id_from_events(&result.events),
        warning: None,
        paused: result.snapshot.paused,
    }
}

fn control_state(state: local::ChatControlState) -> wire::ChatControlState {
    wire::ChatControlState {
        session_id: state.session_id,
        accepted: state.accepted,
        runtime_attached: state.runtime_attached,
        reattach_state: reattach_wire(state.reattach_state),
        reattach_active_turn_persisted: state.reattach_active_turn_persisted,
        reattach_running_task_count: state.reattach_running_task_count,
        reattach_persisted_event_count: state.reattach_persisted_event_count,
        active_turn: state.active_turn,
        next_sequence: state.next_sequence,
        task_count: state.task_count,
        tasks: state.tasks.into_iter().map(local_task_wire).collect(),
        event_cursor_offset: state.event_cursor_offset,
        steering_count: state.steering_count,
        follow_up_count: state.follow_up_count,
        queued_inputs: state
            .queued_inputs
            .into_iter()
            .map(|queued| wire::ChatQueuedTurnInput {
                input_id: queued.input_id,
                kind: match queued.kind {
                    local::ChatQueuedTurnInputKind::Steering => {
                        wire::ChatQueuedTurnInputKind::Steering
                    }
                    local::ChatQueuedTurnInputKind::FollowUp => {
                        wire::ChatQueuedTurnInputKind::FollowUp
                    }
                },
                message: queued.message,
            })
            .collect(),
        detail: state.detail,
        paused: state.paused,
    }
}

fn control_resume_state(response: AgentSessionResumeResponse) -> wire::ChatControlState {
    let (active_turn, next_sequence, task_count, tasks) = response
        .control_snapshot
        .as_ref()
        .map(|snapshot| {
            (
                snapshot.active_turn.is_some(),
                snapshot.next_sequence,
                snapshot.tasks.len(),
                snapshot.tasks.iter().map(task_wire).collect(),
            )
        })
        .unwrap_or((false, 0, 0, Vec::new()));
    let (steering_count, follow_up_count, queued_inputs) = response
        .session
        .as_ref()
        .map(|session| {
            (
                session.steering_queue.len(),
                session.follow_up_queue.len(),
                local::chat_queued_inputs_from_session(session)
                    .into_iter()
                    .map(|queued| wire::ChatQueuedTurnInput {
                        input_id: queued.input_id,
                        kind: match queued.kind {
                            local::ChatQueuedTurnInputKind::Steering => {
                                wire::ChatQueuedTurnInputKind::Steering
                            }
                            local::ChatQueuedTurnInputKind::FollowUp => {
                                wire::ChatQueuedTurnInputKind::FollowUp
                            }
                        },
                        message: queued.message,
                    })
                    .collect(),
            )
        })
        .unwrap_or((0, 0, Vec::new()));
    wire::ChatControlState {
        session_id: response.session_id.to_string(),
        accepted: response.accepted,
        runtime_attached: response.runtime_attached,
        reattach_state: reattach_wire(response.reattach.state),
        reattach_active_turn_persisted: response.reattach.active_turn_persisted,
        reattach_running_task_count: response.reattach.running_task_count,
        reattach_persisted_event_count: response.reattach.persisted_event_count,
        active_turn,
        next_sequence,
        task_count,
        tasks,
        event_cursor_offset: response.event_cursor.offset,
        steering_count,
        follow_up_count,
        queued_inputs,
        paused: response
            .session
            .as_ref()
            .is_some_and(|session| session.is_paused()),
        detail: response.detail,
    }
}

fn reattach_wire(state: AgentSessionReattachState) -> wire::ChatReattachState {
    match state {
        AgentSessionReattachState::LiveRuntimeAttached => {
            wire::ChatReattachState::LiveRuntimeAttached
        }
        AgentSessionReattachState::PersistedMetadataOnly => {
            wire::ChatReattachState::PersistedStateOnly
        }
        AgentSessionReattachState::Missing => wire::ChatReattachState::RuntimeUnavailable,
    }
}

fn task_wire(task: &AgentTaskSnapshot) -> wire::ChatTaskSnapshot {
    wire::ChatTaskSnapshot {
        task_id: task.task.task_id.to_string(),
        kind: task.task.kind.clone(),
        summary: task.task.summary.clone(),
        status: match task.status {
            AgentTaskLifecycleStatus::Pending => "pending",
            AgentTaskLifecycleStatus::Running => "running",
            AgentTaskLifecycleStatus::Completed => "completed",
            AgentTaskLifecycleStatus::Failed => "failed",
            AgentTaskLifecycleStatus::Cancelled => "cancelled",
            AgentTaskLifecycleStatus::TimedOut => "timed-out",
            AgentTaskLifecycleStatus::Paused => "paused",
        }
        .to_string(),
        progress_summary: task.progress_summary.clone(),
        result_ref: task.result_ref.clone(),
        active_op_id: task.active_op_id.as_ref().map(ToString::to_string),
    }
}

fn local_task_wire(task: local::ChatTaskSnapshot) -> wire::ChatTaskSnapshot {
    wire::ChatTaskSnapshot {
        task_id: task.task_id,
        kind: task.kind,
        summary: task.summary,
        status: task.status,
        progress_summary: task.progress_summary,
        result_ref: task.result_ref,
        active_op_id: task.active_op_id,
    }
}

fn open_history_store(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: &tokio::runtime::Handle,
) -> Result<local::SifsAgentChannelStore, String> {
    local::AgentChatSifsStorage::new(data_dir, daemon_address, runtime).open_channel_store()
}

fn history_list(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
) -> Result<Vec<wire::ChatHistorySessionSummary>, String> {
    open_history_store(daemon_address, data_dir, &runtime)?
        .list_channel_summaries()
        .map_err(|error| error.to_string())
        .map(|summaries| {
            summaries
                .into_iter()
                .filter(|summary| summary.deleted_at.is_none())
                .map(|summary| wire::ChatHistorySessionSummary {
                    id: summary.channel_id.to_string(),
                    title: summary.title,
                    kind: history_kind(summary.channel_kind),
                    updated_unix: summary.updated_at.unix_timestamp(),
                    participant_count: summary.enabled_participant_count,
                    participant_names: summary.participant_names,
                })
                .collect()
        })
}

fn history_load(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
    request: wire::ChatHistoryLoadRequest,
) -> Result<wire::ChatHistoryLoadResponse, String> {
    let channel_id = SessionId::from_str(&request.session_id).map_err(|error| error.to_string())?;
    let store = open_history_store(daemon_address, data_dir, &runtime)?;
    let (page, discovered_message_ids) = match request.mode {
        wire::ChatHistoryLoadMode::Initial | wire::ChatHistoryLoadMode::ReconcileLatest => {
            let known = request
                .known_message_ids
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>();
            let result = store
                .load_message_reconciliation_page(&channel_id, &known, request.limit)
                .map_err(|error| error.to_string())?;
            (result.page, result.discovered_message_ids)
        }
        wire::ChatHistoryLoadMode::Before => (
            store
                .load_message_page_before(&channel_id, request.sequence.unwrap_or(1), request.limit)
                .map_err(|error| error.to_string())?,
            Vec::new(),
        ),
        wire::ChatHistoryLoadMode::After => (
            store
                .load_message_page_from(
                    &channel_id,
                    request.sequence.unwrap_or(0).saturating_add(1),
                    request.limit,
                )
                .map_err(|error| error.to_string())?,
            Vec::new(),
        ),
    };
    let overview = request
        .include_overview
        .then(|| store.load_timeline_overview(&channel_id))
        .transpose()
        .map_err(|error| error.to_string())?
        .map(|overview| wire::ChatHistoryTimelineOverview {
            total_messages: overview.total_messages,
            oldest_sequence: overview.oldest_sequence,
            newest_sequence: overview.newest_sequence,
            chunk_size: overview.chunk_size,
            segments: overview
                .segments
                .into_iter()
                .map(|segment| wire::ChatHistoryTimelineSegment {
                    start_sequence: segment.start_sequence,
                    end_sequence: segment.end_sequence,
                    message_count: segment.message_count,
                    first_created_unix: segment.first_created_unix,
                    last_created_unix: segment.last_created_unix,
                    preview: segment.preview,
                })
                .collect(),
        });
    Ok(wire::ChatHistoryLoadResponse {
        page: history_page(page, discovered_message_ids),
        overview,
    })
}

fn history_channel(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
    session_id: &str,
) -> Result<Option<wire::ChatHistoryChannel>, String> {
    let channel_id = SessionId::from_str(session_id).map_err(|error| error.to_string())?;
    open_history_store(daemon_address, data_dir, &runtime)?
        .load_channel(&channel_id)
        .map_err(|error| error.to_string())
        .map(|channel| channel.map(history_channel_wire))
}

fn history_create_channel(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
    request: wire::ChatHistoryCreateChannelRequest,
) -> Result<(), String> {
    let channel_id = SessionId::from_str(&request.session_id).map_err(|error| error.to_string())?;
    let mut channel = AgentChannel::new_channel(channel_id, request.title);
    let participant_id = channel.ensure_participant(request.agent_key, request.agent_name);
    if let Some(participant) = channel.participant_mut(&participant_id) {
        participant.tool_runtime.sandbox_node_id = request.sandbox_node_id;
    }
    open_history_store(daemon_address, data_dir, &runtime)?
        .save_channel(&channel)
        .map_err(|error| error.to_string())
}

fn history_rename(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
    request: wire::ChatHistoryRenameRequest,
) -> Result<(), String> {
    let channel_id = SessionId::from_str(&request.session_id).map_err(|error| error.to_string())?;
    let store = open_history_store(daemon_address, data_dir, &runtime)?;
    let mut channel = store
        .load_channel(&channel_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "channel not found".to_string())?;
    channel.channel_kind = match request.kind {
        wire::ChatHistoryConversationKind::Chat => AgentChannelKind::Chat,
        wire::ChatHistoryConversationKind::Channel => AgentChannelKind::Channel,
    };
    channel.rename(request.title);
    store
        .save_channel(&channel)
        .map_err(|error| error.to_string())
}

/// Agent sessions that belong to a chat: the chat's own session and each
/// participant's session.
fn chat_agent_session_ids(channel: &AgentChannel) -> Vec<SessionId> {
    let mut ids = vec![channel.channel_id.clone()];
    for participant in &channel.participants {
        if !ids.contains(&participant.agent_session_id) {
            ids.push(participant.agent_session_id.clone());
        }
    }
    ids
}

/// Delete a chat. Its live work is cancelled first so nothing writes after
/// the delete, the chat is tombstoned, and each session's automatic sandbox is
/// released. Transcripts and other content are kept for the retention window
/// and purged afterwards by [`purge_deleted_chats`].
fn history_delete(
    daemon_address: &str,
    runtime_control: &AgentRuntimeControlRegistry,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
    session_id: &str,
) -> Result<(), String> {
    let channel_id = SessionId::from_str(session_id).map_err(|error| error.to_string())?;
    let store = open_history_store(daemon_address, data_dir.clone(), &runtime)?;
    let local_node_id = crate::read_cluster_state(&crate::ClusterPaths::new(&data_dir))
        .ok()
        .map(|state| state.local_node_id);
    let sandboxes = sinomo_runtime::sandbox::SandboxLifecycleClient::daemon(
        data_dir.clone(),
        daemon_address,
        local_node_id,
    );
    delete_chat(&store, runtime_control, &sandboxes, &data_dir, &channel_id)
}

fn delete_chat(
    store: &local::SifsAgentChannelStore,
    runtime_control: &AgentRuntimeControlRegistry,
    sandboxes: &sinomo_runtime::sandbox::SandboxLifecycleClient,
    data_dir: &sinomo_runtime::local::SinomoDataDir,
    channel_id: &SessionId,
) -> Result<(), String> {
    let mut channel = store
        .load_channel(channel_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "channel not found".to_string())?;
    let session_ids = chat_agent_session_ids(&channel);
    for session_id in &session_ids {
        cancel_live_session(runtime_control, session_id);
    }
    channel.tombstone();
    store
        .save_channel(&channel)
        .map_err(|error| error.to_string())?;
    for session_id in &session_ids {
        match release_session_sandbox(sandboxes, data_dir, session_id) {
            Ok(true) => tracing::info!(%session_id, "released deleted chat session sandbox"),
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(%session_id, %error, "release deleted chat session sandbox failed")
            }
        }
    }
    Ok(())
}

fn cancel_live_session(runtime_control: &AgentRuntimeControlRegistry, session_id: &SessionId) {
    if !runtime_control
        .contains_session(session_id)
        .unwrap_or(false)
    {
        return;
    }
    if let Err(error) = runtime_control.cancel(AgentSessionCancelRequest {
        session_id: session_id.clone(),
        reason: "chat deleted".to_string(),
    }) {
        tracing::warn!(%session_id, %error, "cancel deleted chat session failed");
    }
}

/// Release the sandbox a deleted session owns: an automatic attachment to the
/// sandbox derived from this session that no other session attaches, on
/// whichever node hosts it. Explicit and shared sandboxes are left alone.
fn release_session_sandbox(
    sandboxes: &sinomo_runtime::sandbox::SandboxLifecycleClient,
    data_dir: &sinomo_runtime::local::SinomoDataDir,
    session_id: &SessionId,
) -> Result<bool, String> {
    use sinomo_agent::AgentSessionStore as _;
    let store = sinomo_agent::FileAgentSessionStore::for_data_dir(data_dir);
    let Some(session) = store
        .load_session(session_id)
        .map_err(|error| error.to_string())?
    else {
        return Ok(false);
    };
    let Some(attachment) = session.sandbox_attachment else {
        return Ok(false);
    };
    let owned = sinomo_agent::local_workspace::agent_sandbox_id_for_session(session_id);
    if attachment.attachment_kind != sinomo_agent::AgentSessionSandboxAttachmentKind::Automatic
        || attachment.sandbox_id != owned.as_str()
    {
        return Ok(false);
    }
    // "local-daemon" is the attachment written before the node joined a
    // cluster; it always means this node.
    let node = if attachment.server_id == "local-daemon" {
        None
    } else {
        Some(
            sinomo_runtime::node::NodeId::from_str(&attachment.server_id)
                .map_err(|error| format!("invalid sandbox server id: {error}"))?,
        )
    };
    if !store
        .sessions_attached_to_sandbox(&attachment.sandbox_id, session_id)
        .map_err(|error| error.to_string())?
        .is_empty()
    {
        return Ok(false);
    }
    sinomo_agent::local_workspace::release_default_agent_session_sandbox(
        sandboxes,
        node.as_ref(),
        session_id,
    )
}

/// Purge chats deleted longer than `retention` ago: each session's local
/// data (transcript, events, traces, evidence) and the chat's shared content
/// (messages, attachments, checkpoints). The deletion tombstone is kept.
/// Returns how many chats had content purged.
fn purge_deleted_chats(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: &tokio::runtime::Handle,
    retention: Duration,
) -> Result<usize, String> {
    let store = open_history_store(daemon_address, data_dir.clone(), runtime)?;
    purge_deleted_chats_in(&store, &data_dir, retention)
}

fn purge_deleted_chats_in(
    store: &local::SifsAgentChannelStore,
    data_dir: &sinomo_runtime::local::SinomoDataDir,
    retention: Duration,
) -> Result<usize, String> {
    let cutoff = time::OffsetDateTime::now_utc()
        - time::Duration::try_from(retention).map_err(|error| error.to_string())?;
    let sessions = sinomo_agent::FileAgentSessionStore::for_data_dir(data_dir);
    let mut purged = 0;
    for summary in store
        .list_deleted_channel_summaries()
        .map_err(|error| error.to_string())?
    {
        if summary
            .deleted_at
            .is_none_or(|deleted_at| deleted_at > cutoff)
        {
            continue;
        }
        let Some(channel) = store
            .load_channel_metadata_only(&summary.channel_id)
            .map_err(|error| error.to_string())?
        else {
            continue;
        };
        let mut removed = false;
        for session_id in chat_agent_session_ids(&channel) {
            removed |= sessions
                .purge_session(&session_id)
                .map_err(|error| error.to_string())?;
        }
        removed |= store.purge_deleted_channel_content(&summary.channel_id)?;
        if removed {
            purged += 1;
        }
    }
    Ok(purged)
}

fn history_participant(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
    request: wire::ChatHistoryParticipantRequest,
) -> Result<(), String> {
    let session_id = match &request {
        wire::ChatHistoryParticipantRequest::Add { session_id, .. }
        | wire::ChatHistoryParticipantRequest::Remove { session_id, .. } => session_id,
    };
    let channel_id = SessionId::from_str(session_id).map_err(|error| error.to_string())?;
    let store = open_history_store(daemon_address, data_dir, &runtime)?;
    let mut channel = store
        .load_channel(&channel_id)
        .map_err(|error| error.to_string())?
        .unwrap_or_else(|| AgentChannel::new_channel(channel_id, "New channel"));
    channel.channel_kind = AgentChannelKind::Channel;
    match request {
        wire::ChatHistoryParticipantRequest::Add {
            agent_key,
            agent_name,
            sandbox_node_id,
            ..
        } => {
            let participant_id = channel.ensure_participant(agent_key, agent_name);
            if let Some(participant) = channel.participant_mut(&participant_id) {
                participant.tool_runtime.sandbox_node_id = sandbox_node_id;
            }
        }
        wire::ChatHistoryParticipantRequest::Remove { participant_id, .. } => {
            let participant_id =
                TaskId::from_str(&participant_id).map_err(|error| error.to_string())?;
            if !channel.disable_participant(&participant_id) {
                return Err("agent not found".to_string());
            }
        }
    }
    store
        .save_channel(&channel)
        .map_err(|error| error.to_string())
}

fn history_append_agent(
    daemon_address: &str,
    data_dir: sinomo_runtime::local::SinomoDataDir,
    runtime: tokio::runtime::Handle,
    request: wire::ChatHistoryAppendAgentRequest,
) -> Result<(), String> {
    let channel_id = SessionId::from_str(&request.session_id).map_err(|error| error.to_string())?;
    let agent_session_id =
        SessionId::from_str(&request.agent_session_id).map_err(|error| error.to_string())?;
    let message_id = TaskId::from_str(&request.message_id).map_err(|error| error.to_string())?;
    let created_at = time::OffsetDateTime::from_unix_timestamp_nanos(request.created_at_unix_nanos)
        .map_err(|error| error.to_string())?;
    let external_channel_id = request.session_id.clone();
    let external_message_id = request.message_id.clone();
    let store = open_history_store(daemon_address, data_dir, &runtime)?;
    local::project_stable_chat_agent_message(
        &store,
        &channel_id,
        &request.agent_key,
        &request.agent_name,
        &agent_session_id,
        message_id,
        created_at,
        &request.content,
        AgentChannelSource {
            adapter: request.source_adapter,
            external_channel_id: Some(external_channel_id),
            external_message_id: Some(external_message_id),
            metadata: serde_json::json!({
                "surface_event": "realtime_output_text_turn",
                "text_source": "provider_neutral_visible_output",
            }),
            ..AgentChannelSource::default()
        },
    )
    .map_err(|error| error.to_string())
}

fn history_page(
    page: local::AgentChannelMessagePage,
    discovered_message_ids: Vec<String>,
) -> wire::ChatHistoryPage {
    wire::ChatHistoryPage {
        messages: page.messages.into_iter().map(history_message).collect(),
        start_sequence: page.start_sequence,
        end_sequence: page.end_sequence,
        has_older: page.has_older,
        has_newer: page.has_newer,
        total_messages: page.total_messages,
        chunk_size: page.chunk_size,
        discovered_message_ids,
    }
}

fn history_message(message: AgentChannelMessage) -> wire::ChatHistoryStoredMessage {
    wire::ChatHistoryStoredMessage {
        message_id: message.message_id.to_string(),
        sequence: message.sequence,
        author: match message.author {
            AgentChannelAuthor::User { display_name, .. } => {
                wire::ChatHistoryAuthor::User { display_name }
            }
            AgentChannelAuthor::Agent { agent_name, .. } => {
                wire::ChatHistoryAuthor::Agent { agent_name }
            }
            AgentChannelAuthor::System => wire::ChatHistoryAuthor::System,
        },
        message_kind: match message.message_kind {
            AgentChannelMessageKind::Normal => wire::ChatHistoryMessageKind::Normal,
            AgentChannelMessageKind::Info => wire::ChatHistoryMessageKind::Info,
            AgentChannelMessageKind::Error => wire::ChatHistoryMessageKind::Error,
        },
        content: message.content,
        created_at_unix: message.created_at.unix_timestamp(),
        attachments: message
            .attachments
            .into_iter()
            .map(|attachment| wire::ChatHistoryAttachment {
                file_name: attachment.file_name,
                stored_path: attachment.stored_path,
                size_bytes: attachment.size_bytes,
            })
            .collect(),
        produced_by: message
            .produced_by
            .map(|produced_by| wire::ChatActivityRef {
                participant_id: produced_by.participant_id,
                session_id: produced_by.session_id.to_string(),
                op_id: produced_by.op_id.to_string(),
            }),
    }
}

fn history_channel_wire(channel: AgentChannel) -> wire::ChatHistoryChannel {
    let participants = channel
        .enabled_participants_in_order()
        .into_iter()
        .map(|participant| wire::ChatHistoryParticipant {
            participant_id: participant.participant_id.to_string(),
            agent_key: participant.agent_key,
            agent_name: participant.agent_name,
            agent_session_id: participant.agent_session_id.to_string(),
            enabled: participant.enabled,
            order: participant.order,
            sandbox_node_id: participant.tool_runtime.sandbox_node_id,
        })
        .collect();
    wire::ChatHistoryChannel {
        id: channel.channel_id.to_string(),
        title: channel.title,
        kind: history_kind(channel.channel_kind),
        participants,
    }
}

fn history_kind(kind: AgentChannelKind) -> wire::ChatHistoryConversationKind {
    match kind {
        AgentChannelKind::Chat => wire::ChatHistoryConversationKind::Chat,
        AgentChannelKind::Channel => wire::ChatHistoryConversationKind::Channel,
    }
}

fn validate_client_id(client_id: &str) -> Result<(), String> {
    if client_id.is_empty()
        || client_id.len() > 256
        || !client_id.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_' | ':')
        })
    {
        return Err("invalid chat client id".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_chat_host_waits_out_only_daemon_startup_failures() {
        for starting in [
            "chat unavailable: failed to parse command: open filesystem provider agent catalog SIFS failed: no cluster service instance provides sifs.path.access (catalog revision 6, 0 entries, 0 decoded instances; services: [none]; provider capabilities: [none])",
            "chat restoration failed: failed to parse command: configure resumed chat tool runtime failed: failed to parse command: load attached sandbox `x`: daemon service error at /sandbox/status: failed to parse command: network serialization error: invalid DFS runtime operation: distributed DFS error: raft node is busy at D:\\data\\cluster\\kv-raft\\node",
        ] {
            assert!(chat_startup_failure_is_transient(starting), "{starting}");
        }
        for lasting in [
            "chat unavailable: failed to parse command: open agent channel SIFS store failed: device authorization was denied",
            "chat restoration failed: agent session `x` references unknown agent definition `gone`",
        ] {
            assert!(!chat_startup_failure_is_transient(lasting), "{lasting}");
        }
    }

    #[test]
    fn workspace_management_commands_preserve_order_and_persist_exact_grants() {
        let root = sinomo_testdir::TestDir::new("multi-workspace-ui-commands");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(root.join("data"));
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let first = root.join("frontend");
        let second = root.join("shared");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        let first = std::fs::canonicalize(first).unwrap();
        let second = std::fs::canonicalize(second).unwrap();
        let mut chat = local::AgentChat::new(data_dir.clone(), false).unwrap();
        chat.set_agent("coding-agent").unwrap();
        let session_id = chat.session_id();
        let events = ChatEventLog::new();
        let apply = |chat: &mut local::AgentChat, change| {
            run_worker_command(
                chat,
                wire::ChatSessionCommand::UpdateWorkspaces {
                    session_id: session_id.clone(),
                    request_id: "request".into(),
                    change,
                },
                &events,
            );
            let state = events.state.0.lock().unwrap();
            let Some(wire::SequencedChatEvent {
                event:
                    wire::ChatEvent::WorkspacesUpdated {
                        error,
                        selection: Some(selection),
                        ..
                    },
                ..
            }) = state.events.back()
            else {
                panic!("workspace reply")
            };
            assert!(error.is_none(), "{error:?}");
            selection.clone()
        };
        let selected = apply(
            &mut chat,
            wire::ChatWorkspaceChange::AddFolder {
                path: first.clone(),
            },
        );
        let first_id = selected.workspaces.as_ref().unwrap()[0].id.clone();
        let selected = apply(
            &mut chat,
            wire::ChatWorkspaceChange::AddFolder {
                path: second.clone(),
            },
        );
        let second_id = selected.workspaces.as_ref().unwrap()[1].id.clone();
        assert_eq!(selected.workspace_root.as_ref(), Some(&first));
        assert_eq!(selected.workspaces.as_ref().unwrap().len(), 2);
        let duplicated = apply(
            &mut chat,
            wire::ChatWorkspaceChange::AddFolder {
                path: first.clone(),
            },
        );
        assert_eq!(duplicated, selected);
        let selected = apply(
            &mut chat,
            wire::ChatWorkspaceChange::MakeDefault {
                workspace_id: second_id.clone(),
            },
        );
        assert_eq!(selected.workspace_root.as_ref(), Some(&second));
        assert_eq!(selected.workspaces.as_ref().unwrap()[0].id, second_id);
        let mut resumed = local::AgentChat::new(data_dir, false).unwrap();
        resumed.resume_session(&session_id, &[]).unwrap();
        assert_eq!(resumed.workspaces(), chat.workspaces());
        resumed.remove_workspace(&second_id).unwrap();
        run_worker_command(
            &mut chat,
            wire::ChatSessionCommand::FetchWorkspaces {
                session_id: session_id.clone(),
            },
            &events,
        );
        assert_eq!(
            chat.workspaces().len(),
            1,
            "refresh observes another view's revocation"
        );
        let selected = apply(
            &mut chat,
            wire::ChatWorkspaceChange::AddFolder {
                path: first.clone(),
            },
        );
        assert_eq!(
            selected.workspaces.as_ref().unwrap().len(),
            1,
            "adding an existing folder must not revive an old grant"
        );
        let selected = apply(
            &mut chat,
            wire::ChatWorkspaceChange::Remove {
                workspace_id: second_id,
            },
        );
        assert_eq!(selected.workspace_root.as_ref(), Some(&first));
        assert_eq!(selected.workspaces.as_ref().unwrap()[0].id, first_id);
        let selected = apply(&mut chat, wire::ChatWorkspaceChange::Clear);
        assert!(selected.workspaces.unwrap().is_empty());
        assert!(selected.workspace_root.is_none());
        assert!(chat.workspaces().is_empty());
        resumed.add_workspace_root(second).unwrap();
        apply(&mut chat, wire::ChatWorkspaceChange::Clear);
        resumed.reload_workspaces().unwrap();
        assert!(
            resumed.workspaces().is_empty(),
            "clear must observe grants added by another view"
        );
        run_worker_command(
            &mut chat,
            wire::ChatSessionCommand::UpdateWorkspaces {
                session_id,
                request_id: "stale".into(),
                change: wire::ChatWorkspaceChange::MakeDefault {
                    workspace_id: first_id,
                },
            },
            &events,
        );
        assert!(chat.workspaces().is_empty());
        assert!(matches!(
            &events.state.0.lock().unwrap().events.back().unwrap().event,
            wire::ChatEvent::WorkspacesUpdated { error: Some(_), .. }
        ));
    }

    fn notice(value: usize) -> wire::ChatEvent {
        wire::ChatEvent::Notice {
            session_id: "session-1".to_string(),
            notice: value.to_string(),
        }
    }

    fn poll(after_sequence: u64, max_events: usize) -> wire::ChatSessionEventsRequest {
        wire::ChatSessionEventsRequest {
            client_id: "client-1".to_string(),
            after_sequence,
            max_events,
            wait_ms: 0,
        }
    }

    #[test]
    fn event_log_pages_from_an_exclusive_cursor() {
        let events = ChatEventLog::new();
        events.push(notice(1));
        events.push(notice(2));
        events.push(notice(3));

        let first = events.poll(&poll(0, 2));
        assert_eq!(
            first
                .events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(first.next_sequence, 2);

        let second = events.poll(&poll(first.next_sequence, 2));
        assert_eq!(
            second
                .events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![3]
        );
        assert_eq!(second.next_sequence, 3);
    }

    fn fake_view(
        root: &std::path::Path,
        runtime: &tokio::runtime::Runtime,
        execution_id: &str,
    ) -> ChatView {
        ChatView {
            events: ChatEventLog::new(),
            storage: local::AgentChatSifsStorage::new(
                sinomo_runtime::local::SinomoDataDir::new(root),
                "127.0.0.1:1",
                runtime.handle(),
            ),
            journal: CommandJournal::open(root, execution_id).unwrap(),
            chat: Arc::new(Mutex::new(None)),
            last_active: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// A host whose worker is replaced by the returned command receiver.
    fn fake_host(idle_for: Duration) -> (ChatHost, Receiver<HostCommand>) {
        let (command_tx, commands) = channel();
        let activity = HostActivity::new();
        activity.lock().last_active = Instant::now() - idle_for;
        let host = ChatHost {
            command_tx,
            fanout: ChatFanout::default(),
            ready: Arc::new(AtomicBool::new(true)),
            alive: Arc::new(AtomicBool::new(true)),
            activity,
            renewed_at: Mutex::new(Instant::now()),
            peer_requests: Mutex::new(VecDeque::new()),
        };
        (host, commands)
    }

    fn service_with_environment(
        root: &std::path::Path,
        runtime: &tokio::runtime::Runtime,
        registry: AgentRuntimeControlRegistry,
    ) -> HostedChatService {
        let service = HostedChatService::new("127.0.0.1:1", registry);
        let _ = service.environment.set(HostEnvironment {
            data_dir: sinomo_runtime::local::SinomoDataDir::new(root),
            runtime: runtime.handle().clone(),
            cluster: ChatCluster::standalone(),
        });
        service
    }

    fn send(
        client: &str,
        request: &str,
        chat: &str,
        text: &str,
    ) -> wire::ChatSessionCommandRequest {
        wire::ChatSessionCommandRequest {
            client_id: client.into(),
            request_id: Some(request.into()),
            command: wire::ChatSessionCommand::Send {
                session_id: chat.into(),
                text: text.into(),
                attachments: vec![],
            },
        }
    }

    fn peer_send(chat: &str, id: &str) -> wire::ChatPeerCommandRequest {
        wire::ChatPeerCommandRequest {
            chat_id: chat.into(),
            origin: "peer/view".into(),
            request_id: id.into(),
            command: wire::ChatSessionCommand::Send {
                session_id: chat.into(),
                text: "owned command".into(),
                attachments: vec![],
            },
        }
    }

    struct DecliningPeer {
        calls: Arc<Mutex<Vec<String>>>,
        uncertain: bool,
        contradictory: bool,
    }

    impl cluster::ChatPeers for DecliningPeer {
        fn command(
            &self,
            _: &NodeId,
            request: wire::ChatPeerCommandRequest,
        ) -> Result<wire::ChatPeerCommandResponse, String> {
            self.calls.lock().unwrap().push(request.request_id);
            if self.uncertain {
                Err("original delivery outcome unknown".into())
            } else {
                Ok(wire::ChatPeerCommandResponse {
                    accepted: self.contradictory,
                    not_owner: true,
                })
            }
        }
        fn events(
            &self,
            _: &NodeId,
            _: wire::ChatPeerEventsRequest,
        ) -> Result<wire::ChatPeerEventsResponse, String> {
            Err("no event forwarding in this control".into())
        }
        fn delete(&self, _: &NodeId, _: wire::ChatPeerDeleteRequest) -> Result<(), String> {
            Err("no deletion in this control".into())
        }
    }

    #[test]
    fn peer_dispatch_reroutes_only_explicit_non_admission_with_the_original_journal_id() {
        for (uncertain, contradictory) in [(false, false), (true, false), (false, true)] {
            let root = sinomo_testdir::TestDir::new("peer-dispatch-original-request");
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let mut service =
                HostedChatService::new("127.0.0.1:1", AgentRuntimeControlRegistry::new());
            let calls = Arc::new(Mutex::new(Vec::new()));
            service
                .environment
                .set(HostEnvironment {
                    data_dir: sinomo_runtime::local::SinomoDataDir::new(&root),
                    runtime: runtime.handle().clone(),
                    cluster: ChatCluster {
                        peers: Arc::new(DecliningPeer {
                            calls: calls.clone(),
                            uncertain,
                            contradictory,
                        }),
                        ..ChatCluster::standalone()
                    },
                })
                .ok()
                .unwrap();
            let receivers = Arc::new(Mutex::new(Vec::new()));
            let collected = receivers.clone();
            service.host_factory = Some(Arc::new(move |_| {
                let (host, receiver) = fake_host(Duration::ZERO);
                collected.lock().unwrap().push(receiver);
                host
            }));
            service.hosts.lock().unwrap().insert(
                "chat".into(),
                ChatRoute::Remote(RemoteChat {
                    owner: NodeId::from_legacy_named("declining-owner").unwrap(),
                    fanout: ChatFanout::default(),
                    alive: Arc::new(AtomicBool::new(true)),
                    last_active: Mutex::new(Instant::now()),
                }),
            );
            let journal = CommandJournal::open(&root, "origin").unwrap();
            let command = peer_send("chat", "original-request").command;
            journal.admit("original-request", &command).unwrap();
            let result = service.dispatch(
                "view",
                "chat",
                Some((journal.clone(), "original-request".into())),
                command.clone(),
            );
            assert_eq!(calls.lock().unwrap().as_slice(), ["original-request"]);
            if uncertain || contradictory {
                let error = if uncertain {
                    "original delivery outcome unknown"
                } else {
                    "peer returned no unambiguous command admission"
                };
                assert!(result.unwrap_err().contains(error));
                assert!(
                    receivers.lock().unwrap().is_empty(),
                    "uncertain effects must not reroute"
                );
                assert_eq!(
                    journal.admit("original-request", &command).unwrap_err(),
                    error
                );
            } else {
                result.unwrap();
                let receivers = receivers.lock().unwrap();
                assert_eq!(receivers.len(), 1);
                let HostCommand::Run {
                    journal: Some((owned, id)),
                    command: received,
                    ..
                } = receivers[0].try_recv().unwrap()
                else {
                    panic!("missing bound handoff");
                };
                assert_eq!(id, "original-request");
                assert_eq!(received, command);
                // This is the real worker's durable start boundary. It must
                // still own the original Pending entry after non-admission.
                owned
                    .start(&id)
                    .expect("known non-admission remains retryable with the same ID");
                owned.complete(&id, None).unwrap();
                assert!(!journal.admit(&id, &command).unwrap());
            }
        }
    }

    #[test]
    fn peer_dispatch_delivers_once_to_the_selected_local_host() {
        let service = HostedChatService::new("127.0.0.1:1", AgentRuntimeControlRegistry::new());
        let (host, commands) = fake_host(Duration::ZERO);
        service
            .hosts
            .lock()
            .unwrap()
            .insert("chat".into(), ChatRoute::Local(host));
        let request = peer_send("chat", "same-request");
        for _ in 0..2 {
            let response = service.peer_command(request.clone()).unwrap();
            assert!(response.accepted && !response.not_owner);
        }
        assert!(
            matches!(commands.try_recv().unwrap(), HostCommand::Run { origin, journal: None, command: wire::ChatSessionCommand::Send { session_id, .. } }
            if origin == "peer/view" && session_id == "chat")
        );
        assert!(
            commands.try_recv().is_err(),
            "same request must not queue twice"
        );
    }

    #[test]
    fn peer_dispatch_failed_enqueue_is_not_acknowledged_by_a_retry() {
        let service = HostedChatService::new("127.0.0.1:1", AgentRuntimeControlRegistry::new());
        let (host, commands) = fake_host(Duration::ZERO);
        drop(commands);
        service
            .hosts
            .lock()
            .unwrap()
            .insert("chat".into(), ChatRoute::Local(host));
        let request = peer_send("chat", "failed-request");
        let first = service.peer_command(request.clone()).unwrap_err();
        assert_eq!(service.peer_command(request).unwrap_err(), first);
    }

    #[test]
    fn peer_dispatch_rejects_a_command_for_another_chat_before_effects() {
        let root = sinomo_testdir::TestDir::new("peer-dispatch-chat-binding");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let registry = AgentRuntimeControlRegistry::new();
        let service = service_with_environment(&root, &runtime, registry.clone());
        let chat = SessionId::generate().to_string();
        let other = SessionId::generate();
        let cancelled = Arc::new(Mutex::new(Vec::new()));
        registry
            .register_session(Arc::new(CancelRecorder(
                other.clone(),
                cancelled.clone(),
                None,
            )))
            .unwrap();
        let (host, commands) = fake_host(Duration::ZERO);
        service
            .hosts
            .lock()
            .unwrap()
            .insert(chat.clone(), ChatRoute::Local(host));
        let mut request = peer_send(&chat, "mismatched-request");
        request.command = wire::ChatSessionCommand::Cancel {
            session_id: other.to_string(),
            reason: "wrong chat".into(),
        };
        assert!(service.peer_command(request).is_err());
        assert!(
            cancelled.lock().unwrap().is_empty(),
            "must not cancel a different chat on the same node"
        );
        assert!(commands.try_recv().is_err());
    }

    #[test]
    fn peer_dispatch_partial_control_failure_is_not_reexecuted_or_acknowledged() {
        let root = sinomo_testdir::TestDir::new("peer-dispatch-control-outcome");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let registry = AgentRuntimeControlRegistry::new();
        let service = service_with_environment(&root, &runtime, registry.clone());
        let chat = SessionId::generate();
        let cancelled = Arc::new(Mutex::new(Vec::new()));
        registry
            .register_session(Arc::new(CancelRecorder(
                chat.clone(),
                cancelled.clone(),
                None,
            )))
            .unwrap();
        let (host, commands) = fake_host(Duration::ZERO);
        service
            .hosts
            .lock()
            .unwrap()
            .insert(chat.to_string(), ChatRoute::Local(host));
        // Leave normal work queued; control must not wait behind that work.
        service
            .peer_command(peer_send(&chat.to_string(), "queued-turn"))
            .unwrap();
        let mut request = peer_send(&chat.to_string(), "cancel-request");
        request.command = wire::ChatSessionCommand::Cancel {
            session_id: chat.to_string(),
            reason: "cancel directly".into(),
        };
        let first = service.peer_command(request.clone()).unwrap_err();
        assert_eq!(first, "recorded");
        assert_eq!(service.peer_command(request).unwrap_err(), first);
        assert_eq!(cancelled.lock().unwrap().as_slice(), ["cancel directly"]);
        assert!(matches!(
            commands.try_recv(),
            Ok(HostCommand::Run {
                command: wire::ChatSessionCommand::Send { .. },
                ..
            })
        ));
        assert!(
            commands.try_recv().is_err(),
            "control never joins the worker queue"
        );
    }

    #[test]
    fn peer_dispatch_rejects_remote_ownership_without_forwarding() {
        // There is deliberately no environment/peer transport to fall back to.
        let service = HostedChatService::new("127.0.0.1:1", AgentRuntimeControlRegistry::new());
        service.hosts.lock().unwrap().insert(
            "chat".into(),
            ChatRoute::Remote(RemoteChat {
                owner: NodeId::from_legacy_named("other-node").unwrap(),
                fanout: ChatFanout::default(),
                alive: Arc::new(AtomicBool::new(true)),
                last_active: Mutex::new(Instant::now()),
            }),
        );
        let response = service
            .peer_command(peer_send("chat", "remote-request"))
            .unwrap();
        assert!(!response.accepted && response.not_owner);
    }

    #[test]
    fn peer_dispatch_does_not_replace_a_host_that_stops_during_selection() {
        let root = sinomo_testdir::TestDir::new("peer-dispatch-retired-owner");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let mut service =
            service_with_environment(&root, &runtime, AgentRuntimeControlRegistry::new());
        let receivers = Arc::new(Mutex::new(Vec::new()));
        let collected = receivers.clone();
        service.host_factory = Some(Arc::new(move |_| {
            let (host, receiver) = fake_host(Duration::ZERO);
            // The selected worker can fail before selection returns. Do not
            // route this same admission to a replacement worker or another node.
            host.alive.store(false, Ordering::Release);
            collected.lock().unwrap().push(receiver);
            host
        }));
        let response = service
            .peer_command(peer_send("chat", "retired-request"))
            .unwrap();
        assert!(!response.accepted && response.not_owner);
        let receivers = receivers.lock().unwrap();
        assert_eq!(receivers.len(), 1, "no second owner lookup or replacement");
        assert!(
            receivers[0].try_recv().is_err(),
            "retired owner receives no effect"
        );
    }

    #[test]
    fn peer_dispatch_binds_owner_through_the_control_effect() {
        let root = sinomo_testdir::TestDir::new("peer-dispatch-owner-effect");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let registry = AgentRuntimeControlRegistry::new();
        let service = service_with_environment(&root, &runtime, registry.clone());
        let chat = SessionId::generate();
        let (host, commands) = fake_host(Duration::ZERO);
        service
            .hosts
            .lock()
            .unwrap()
            .insert(chat.to_string(), ChatRoute::Local(host));
        let cancelled = Arc::new(Mutex::new(Vec::new()));
        let replaced = Arc::new(AtomicBool::new(false));
        let (hosts, attempted, selected) =
            (service.hosts.clone(), replaced.clone(), chat.to_string());
        let try_retire = Arc::new(move || {
            // Exercise replacement at the actual runtime effect callback, not
            // at an earlier status probe. The admitted owner must still hold.
            if let Ok(mut hosts) = hosts.try_lock() {
                hosts.remove(&selected);
                attempted.store(true, Ordering::Release);
            }
        });
        registry
            .register_session(Arc::new(CancelRecorder(
                chat.clone(),
                cancelled.clone(),
                Some(try_retire),
            )))
            .unwrap();
        let mut request = peer_send(&chat.to_string(), "bound-control");
        request.command = wire::ChatSessionCommand::Cancel {
            session_id: chat.to_string(),
            reason: "owned cancel".into(),
        };
        assert_eq!(service.peer_command(request).unwrap_err(), "recorded");
        assert!(
            !replaced.load(Ordering::Acquire),
            "route changed across local admission and effect"
        );
        assert_eq!(cancelled.lock().unwrap().as_slice(), ["owned cancel"]);
        assert!(commands.try_recv().is_err());
        // The guard does not leak beyond the effect; ordinary retirement works.
        service.hosts.lock().unwrap().remove(&chat.to_string());
        assert!(matches!(commands.try_recv(), Ok(HostCommand::Shutdown)));
    }

    #[test]
    fn closing_a_subscription_keeps_the_view_and_reconnect_replays_without_duplicate_commands() {
        let root = sinomo_testdir::TestDir::new("chat-detached-execution");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let service = service_with_environment(&root, &runtime, AgentRuntimeControlRegistry::new());
        let view = fake_view(&root, &runtime, "durable-view");
        *view.chat.lock().unwrap() = Some("chat".into());
        let events = view.events.clone();
        events.push(notice(1));
        service
            .views
            .lock()
            .unwrap()
            .insert("durable-view".into(), view);
        let (host, commands) = fake_host(Duration::ZERO);
        host.fanout.subscribe("durable-view", events.clone());
        service
            .hosts
            .lock()
            .unwrap()
            .insert("chat".into(), ChatRoute::Local(host));
        service
            .clients
            .lock()
            .unwrap()
            .insert("old-window".into(), "durable-view".into());
        assert!(
            service
                .close(wire::ChatSessionCloseRequest {
                    client_id: "old-window".into()
                })
                .unwrap()
                .closed
        );
        assert!(
            commands.try_recv().is_err(),
            "detach must not stop the chat host"
        );
        events.push(notice(2));
        let open = service
            .open(
                data_dir,
                runtime.handle().clone(),
                wire::ChatSessionOpenRequest {
                    client_id: "new-window".into(),
                    execution_id: Some("durable-view".into()),
                },
            )
            .unwrap();
        assert!(open.attached);
        assert!(open.runtime_ready);
        let replay = service
            .events(wire::ChatSessionEventsRequest {
                client_id: "new-window".into(),
                after_sequence: 1,
                max_events: 20,
                wait_ms: 0,
            })
            .unwrap();
        assert_eq!(replay.events.len(), 1);
        assert_eq!(replay.events[0].sequence, 2);
        assert_eq!(replay.generation, open.generation);
        let command = send("new-window", "same-operation", "chat", "continue");
        assert!(service.command(command.clone()).unwrap().accepted);
        assert!(service.command(command).unwrap().accepted);
        assert!(matches!(
            commands.try_recv().unwrap(),
            HostCommand::Run { ref origin, .. } if origin == "durable-view"
        ));
        assert!(
            commands.try_recv().is_err(),
            "retry must not execute the command twice"
        );
    }

    #[test]
    fn two_windows_on_one_chat_share_its_host_and_stay_independent() {
        let root = sinomo_testdir::TestDir::new("chat-two-windows");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let service = service_with_environment(&root, &runtime, AgentRuntimeControlRegistry::new());
        let (shared, shared_commands) = fake_host(Duration::ZERO);
        let (other, other_commands) = fake_host(Duration::ZERO);
        shared.fanout.push(wire::ChatEvent::Ready {
            session_id: "shared".into(),
            info: wire::RuntimeInfo::default(),
        });
        service
            .hosts
            .lock()
            .unwrap()
            .insert("shared".into(), ChatRoute::Local(shared));
        service
            .hosts
            .lock()
            .unwrap()
            .insert("other".into(), ChatRoute::Local(other));
        for (view, client) in [("view-a", "window-a"), ("view-b", "window-b")] {
            service
                .views
                .lock()
                .unwrap()
                .insert(view.into(), fake_view(&root, &runtime, view));
            service
                .clients
                .lock()
                .unwrap()
                .insert(client.into(), view.into());
            service.attach_view(view, "shared", &[]).unwrap();
        }
        let poll = |client: &str| {
            service
                .events(wire::ChatSessionEventsRequest {
                    client_id: client.into(),
                    after_sequence: 0,
                    max_events: 50,
                    wait_ms: 0,
                })
                .unwrap()
                .events
                .into_iter()
                .map(|event| event.event)
                .collect::<Vec<_>>()
        };
        // A window that attaches later is brought up to date immediately.
        assert!(matches!(
            poll("window-b").first(),
            Some(wire::ChatEvent::Ready { .. })
        ));

        // Both windows send to the one host for the chat; one runtime runs.
        service
            .command(send("window-a", "a-1", "shared", "from a"))
            .unwrap();
        service
            .command(send("window-b", "b-1", "", "from b"))
            .unwrap();
        let origins = [shared_commands.try_recv(), shared_commands.try_recv()].map(|command| {
            match command.unwrap() {
                HostCommand::Run { origin, .. } => origin,
                HostCommand::Shutdown => panic!("unexpected shutdown"),
            }
        });
        assert_eq!(origins, ["view-a".to_string(), "view-b".to_string()]);

        // Another window's message is shown in the other view, not echoed.
        service.hosts.lock().unwrap()["shared"].fanout().push_from(
            Some("view-a"),
            wire::ChatEvent::UserMessage {
                session_id: "shared".into(),
                text: "from a".into(),
            },
        );
        let user_messages = |client| {
            poll(client)
                .into_iter()
                .filter(|event| matches!(event, wire::ChatEvent::UserMessage { .. }))
                .count()
        };
        assert_eq!(user_messages("window-a"), 0);
        assert_eq!(user_messages("window-b"), 1);

        // Switching one window to another chat leaves the other attached.
        service
            .command(wire::ChatSessionCommandRequest {
                client_id: "window-b".into(),
                request_id: Some("b-2".into()),
                command: wire::ChatSessionCommand::Resume {
                    session_id: "other".into(),
                    history: vec![],
                },
            })
            .unwrap();
        let hosts = service.hosts.lock().unwrap();
        assert_eq!(hosts["shared"].fanout().subscriber_count(), 1);
        assert_eq!(hosts["other"].fanout().subscriber_count(), 1);
        drop(hosts);
        // Closing a window does not stop the shared chat.
        service
            .close(wire::ChatSessionCloseRequest {
                client_id: "window-a".into(),
            })
            .unwrap();
        assert!(shared_commands.try_recv().is_err());
        assert!(other_commands.try_recv().is_err());
    }

    /// A live session whose turn is still running.
    struct BusySession(SessionId);

    impl sinomo_agent::AgentRuntimeControlSession for BusySession {
        fn session_id(&self) -> Result<SessionId, sinomo_agent::AgentRuntimeControlError> {
            Ok(self.0.clone())
        }
        fn steer(
            &self,
            _: String,
        ) -> Result<AgentSessionControlResult, sinomo_agent::AgentRuntimeControlError> {
            Err(sinomo_agent::AgentRuntimeControlError::new("unused"))
        }
        fn follow_up(
            &self,
            _: String,
        ) -> Result<AgentSessionControlResult, sinomo_agent::AgentRuntimeControlError> {
            Err(sinomo_agent::AgentRuntimeControlError::new("unused"))
        }
        fn remove_queued_input(
            &self,
            _: TaskId,
        ) -> Result<AgentSessionControlResult, sinomo_agent::AgentRuntimeControlError> {
            Err(sinomo_agent::AgentRuntimeControlError::new("unused"))
        }
        fn interrupt(
            &self,
            _: String,
        ) -> Result<AgentSessionControlResult, sinomo_agent::AgentRuntimeControlError> {
            Err(sinomo_agent::AgentRuntimeControlError::new("unused"))
        }
        fn resume(
            &self,
        ) -> Result<AgentSessionResumeResponse, sinomo_agent::AgentRuntimeControlError> {
            Err(sinomo_agent::AgentRuntimeControlError::new("unused"))
        }
        fn has_running_work(&self) -> Result<bool, sinomo_agent::AgentRuntimeControlError> {
            Ok(true)
        }
    }

    #[test]
    fn idle_unviewed_hosts_and_detached_views_are_released() {
        let root = sinomo_testdir::TestDir::new("chat-host-eviction");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let registry = AgentRuntimeControlRegistry::new();
        let service = service_with_environment(&root, &runtime, registry.clone());
        let long_ago = DETACHED_CHAT_HOST_IDLE_TTL + Duration::from_secs(60);

        let (idle, idle_commands) = fake_host(long_ago);
        let (recent, _recent_commands) = fake_host(Duration::from_secs(5));
        let (viewed, _viewed_commands) = fake_host(long_ago);
        viewed.fanout.subscribe("window-view", ChatEventLog::new());
        let (processing, _processing_commands) = fake_host(long_ago);
        processing.activity.lock().busy = true;
        let (running, _running_commands) = fake_host(long_ago);
        let running_session = SessionId::generate();
        running.activity.lock().session_id = Some(running_session.clone());
        registry
            .register_session(Arc::new(BusySession(running_session)))
            .unwrap();
        {
            let mut hosts = service.hosts.lock().unwrap();
            hosts.insert("idle".into(), ChatRoute::Local(idle));
            hosts.insert("recent".into(), ChatRoute::Local(recent));
            hosts.insert("viewed".into(), ChatRoute::Local(viewed));
            hosts.insert("processing".into(), ChatRoute::Local(processing));
            hosts.insert("running".into(), ChatRoute::Local(running));
        }
        // A detached, long-idle view is released; an attached one is kept.
        let detached = fake_view(&root, &runtime, "detached-view");
        *detached.last_active.lock().unwrap() = Instant::now() - long_ago;
        let attached = fake_view(&root, &runtime, "window-view");
        *attached.last_active.lock().unwrap() = Instant::now() - long_ago;
        service
            .views
            .lock()
            .unwrap()
            .insert("detached-view".into(), detached);
        service
            .views
            .lock()
            .unwrap()
            .insert("window-view".into(), attached);
        service
            .clients
            .lock()
            .unwrap()
            .insert("window".into(), "window-view".into());

        assert_eq!(service.sweep_idle_hosts().unwrap(), 2);
        let remaining = service
            .hosts
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            remaining,
            ["processing", "recent", "running", "viewed"]
                .into_iter()
                .map(String::from)
                .collect()
        );
        assert!(
            matches!(idle_commands.try_recv(), Ok(HostCommand::Shutdown)),
            "a released host's worker is told to stop"
        );
        assert!(service.views.lock().unwrap().contains_key("window-view"));
        assert!(!service.views.lock().unwrap().contains_key("detached-view"));

        // At capacity, the longest-idle unviewed host makes room even before
        // its TTL has passed.
        let mut hosts = service.hosts.lock().unwrap();
        assert_eq!(
            service
                .evict_idle_hosts(&mut hosts, Some(Duration::ZERO), 1)
                .unwrap(),
            1
        );
        assert!(!hosts.contains_key("recent"));
        assert!(hosts.contains_key("viewed"));
    }

    /// A live session that records cancellation requests.
    struct CancelRecorder(
        SessionId,
        Arc<Mutex<Vec<String>>>,
        Option<Arc<dyn Fn() + Send + Sync>>,
    );

    impl sinomo_agent::AgentRuntimeControlSession for CancelRecorder {
        fn session_id(&self) -> Result<SessionId, sinomo_agent::AgentRuntimeControlError> {
            Ok(self.0.clone())
        }
        fn steer(
            &self,
            _: String,
        ) -> Result<AgentSessionControlResult, sinomo_agent::AgentRuntimeControlError> {
            Err(sinomo_agent::AgentRuntimeControlError::new("unused"))
        }
        fn follow_up(
            &self,
            _: String,
        ) -> Result<AgentSessionControlResult, sinomo_agent::AgentRuntimeControlError> {
            Err(sinomo_agent::AgentRuntimeControlError::new("unused"))
        }
        fn remove_queued_input(
            &self,
            _: TaskId,
        ) -> Result<AgentSessionControlResult, sinomo_agent::AgentRuntimeControlError> {
            Err(sinomo_agent::AgentRuntimeControlError::new("unused"))
        }
        fn interrupt(
            &self,
            _: String,
        ) -> Result<AgentSessionControlResult, sinomo_agent::AgentRuntimeControlError> {
            Err(sinomo_agent::AgentRuntimeControlError::new("unused"))
        }
        fn cancel(
            &self,
            reason: String,
        ) -> Result<AgentSessionControlResult, sinomo_agent::AgentRuntimeControlError> {
            if let Some(before_effect) = &self.2 {
                before_effect();
            }
            self.1.lock().unwrap().push(reason);
            Err(sinomo_agent::AgentRuntimeControlError::new("recorded"))
        }
        fn resume(
            &self,
        ) -> Result<AgentSessionResumeResponse, sinomo_agent::AgentRuntimeControlError> {
            Err(sinomo_agent::AgentRuntimeControlError::new("unused"))
        }
    }

    #[test]
    fn deleting_a_chat_stops_its_work_releases_its_sandbox_and_purges_after_retention() {
        use sinomo_agent::{AgentChannelStore as _, AgentSessionStore as _};
        let root = sinomo_testdir::TestDir::new("chat-delete-lifecycle");
        let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
        let _native_data_dir = crate::native_test_fixture::NativeCliFixture::start(&data_dir);
        let store = local::SifsAgentChannelStore::open(&data_dir).unwrap();
        let sessions = sinomo_agent::FileAgentSessionStore::for_data_dir(&data_dir);
        let runtime_root = root.join("sandbox").join("runtime");

        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::with_default_title(channel_id.clone());
        let message = channel.append_user_message(
            "user",
            "User",
            "private request",
            sinomo_agent::AgentChannelSource::app(),
        );
        store.save_channel(&channel).unwrap();
        store.append_message(&channel_id, &message).unwrap();

        // The chat's session owns an automatic sandbox and has a live turn.
        let mut session =
            sinomo_agent::AgentSession::new(channel_id.clone(), sinomo_agent::AgentRole::Main);
        let sandbox_id = sinomo_agent::local_workspace::agent_sandbox_id_for_session(&channel_id);
        session
            .attach_sandbox(sinomo_agent::AgentSessionSandboxAttachment {
                sandbox_id: sandbox_id.to_string(),
                server_id: "local-daemon".into(),
                attachment_kind: sinomo_agent::AgentSessionSandboxAttachmentKind::Automatic,
            })
            .unwrap();
        sessions.save_session(&session).unwrap();
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let sandboxes =
            sinomo_runtime::sandbox::SandboxLifecycleClient::in_process(data_dir.clone());
        sinomo_agent::local_workspace::provision_default_agent_session_sandbox(
            &sandboxes,
            &sinomo_agent::local_workspace::LocalWorkspaceAgentConfig::new(&workspace)
                .with_sandbox_runtime_root(&runtime_root)
                .with_sandbox_instance_id(sandbox_id.clone()),
            Vec::new(),
        )
        .unwrap();
        let registry = AgentRuntimeControlRegistry::new();
        let cancels = Arc::new(Mutex::new(Vec::new()));
        registry
            .register_session(Arc::new(CancelRecorder(
                channel_id.clone(),
                cancels.clone(),
                None,
            )))
            .unwrap();

        assert!(
            store.purge_deleted_channel_content(&channel_id).is_err(),
            "a chat that is not deleted is never purged"
        );
        delete_chat(&store, &registry, &sandboxes, &data_dir, &channel_id).unwrap();

        assert_eq!(cancels.lock().unwrap().as_slice(), ["chat deleted"]);
        let deleted = store.load_channel(&channel_id).unwrap().unwrap();
        assert!(deleted.deleted_at.is_some());
        assert!(
            !sinomo_agent::local_workspace::release_default_agent_session_sandbox(
                &sandboxes,
                None,
                &channel_id
            )
            .unwrap(),
            "the session's sandbox was already released"
        );
        // Within the retention window the content is kept.
        assert_eq!(
            purge_deleted_chats_in(&store, &data_dir, DELETED_CHAT_RETENTION).unwrap(),
            0
        );
        assert!(sessions.load_session(&channel_id).unwrap().is_some());
        assert!(
            store
                .load_message(&channel_id, &message.message_id)
                .unwrap()
                .is_some()
        );

        // After it, the session data and chat content are purged, but the
        // tombstone remains so the chat stays deleted.
        assert_eq!(
            purge_deleted_chats_in(&store, &data_dir, Duration::ZERO).unwrap(),
            1
        );
        assert!(sessions.load_session(&channel_id).unwrap().is_none());
        assert!(
            store
                .load_message(&channel_id, &message.message_id)
                .unwrap()
                .is_none()
        );
        let tombstone = store.load_channel(&channel_id).unwrap().unwrap();
        assert!(tombstone.deleted_at.is_some());
        assert_eq!(
            purge_deleted_chats_in(&store, &data_dir, Duration::ZERO).unwrap(),
            0,
            "purging is idempotent"
        );
    }

    /// Chat services on different nodes, joined in process.
    #[derive(Default)]
    struct LoopbackPeers {
        services: Mutex<HashMap<String, HostedChatService>>,
        deletes: Mutex<Vec<(String, String)>>,
    }

    impl cluster::ChatPeers for LoopbackPeers {
        fn command(
            &self,
            node: &NodeId,
            request: wire::ChatPeerCommandRequest,
        ) -> Result<wire::ChatPeerCommandResponse, String> {
            let service = self
                .services
                .lock()
                .unwrap()
                .get(&node.to_string())
                .cloned();
            service
                .ok_or_else(|| format!("node {node} is unreachable"))?
                .peer_command(request)
        }

        fn events(
            &self,
            node: &NodeId,
            request: wire::ChatPeerEventsRequest,
        ) -> Result<wire::ChatPeerEventsResponse, String> {
            let service = self
                .services
                .lock()
                .unwrap()
                .get(&node.to_string())
                .cloned();
            service
                .ok_or_else(|| format!("node {node} is unreachable"))?
                .peer_events(request)
        }

        fn delete(
            &self,
            node: &NodeId,
            request: wire::ChatPeerDeleteRequest,
        ) -> Result<(), String> {
            self.deletes
                .lock()
                .unwrap()
                .push((node.to_string(), request.chat_id));
            Ok(())
        }
    }

    type HostCommands = Arc<Mutex<HashMap<String, Receiver<HostCommand>>>>;

    /// A service on `node` whose hosts are fakes; their command receivers
    /// are collected by chat id, along with each host's fan-out.
    fn node_service(
        root: &std::path::Path,
        runtime: &tokio::runtime::Runtime,
        node: &str,
        kv: &Arc<cluster::tests::MemoryKv>,
        peers: &Arc<LoopbackPeers>,
        registry: AgentRuntimeControlRegistry,
    ) -> (
        HostedChatService,
        HostCommands,
        Arc<Mutex<HashMap<String, ChatFanout>>>,
    ) {
        let commands: HostCommands = Arc::default();
        let fanouts: Arc<Mutex<HashMap<String, ChatFanout>>> = Arc::default();
        let mut service = HostedChatService::new("127.0.0.1:1", registry);
        let (collected, collected_fanouts) = (commands.clone(), fanouts.clone());
        service.host_factory = Some(Arc::new(move |chat_id: &str| {
            let (host, receiver) = fake_host(Duration::ZERO);
            collected
                .lock()
                .unwrap()
                .insert(chat_id.to_string(), receiver);
            collected_fanouts
                .lock()
                .unwrap()
                .insert(chat_id.to_string(), host.fanout.clone());
            host
        }));
        let node_id = NodeId::from_legacy_named(node).unwrap();
        let _ = service.environment.set(HostEnvironment {
            data_dir: sinomo_runtime::local::SinomoDataDir::new(root.join(node)),
            runtime: runtime.handle().clone(),
            cluster: ChatCluster {
                node_id: Some(node_id.clone()),
                ownership: Arc::new(cluster::tests::ownership(kv, &node_id.to_string(), node)),
                peers: peers.clone(),
            },
        });
        peers
            .services
            .lock()
            .unwrap()
            .insert(node_id.to_string(), service.clone());
        (service, commands, fanouts)
    }

    fn add_window(
        service: &HostedChatService,
        root: &std::path::Path,
        runtime: &tokio::runtime::Runtime,
        view: &str,
        client: &str,
    ) {
        std::fs::create_dir_all(root).unwrap();
        service
            .views
            .lock()
            .unwrap()
            .insert(view.into(), fake_view(root, runtime, view));
        service
            .clients
            .lock()
            .unwrap()
            .insert(client.into(), view.into());
    }

    /// Wait until `client` has received an event matching `found`.
    fn wait_for_event(
        service: &HostedChatService,
        client: &str,
        found: impl Fn(&wire::ChatEvent) -> bool,
    ) -> Vec<wire::ChatEvent> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let events = service
                .events(wire::ChatSessionEventsRequest {
                    client_id: client.into(),
                    after_sequence: 0,
                    max_events: 500,
                    wait_ms: 100,
                })
                .unwrap()
                .events
                .into_iter()
                .map(|event| event.event)
                .collect::<Vec<_>>();
            if events.iter().any(&found) {
                return events;
            }
            assert!(
                Instant::now() < deadline,
                "{client} never received the event: {events:?}"
            );
        }
    }

    #[test]
    fn a_chat_runs_on_one_node_and_windows_on_other_nodes_use_it() {
        let root = sinomo_testdir::TestDir::new("chat-two-nodes");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let kv = Arc::new(cluster::tests::MemoryKv::default());
        let peers = Arc::new(LoopbackPeers::default());
        let registry_a = AgentRuntimeControlRegistry::new();
        let (node_a, commands_a, fanouts_a) =
            node_service(&root, &runtime, "node_a", &kv, &peers, registry_a.clone());
        let (node_b, commands_b, fanouts_b) = node_service(
            &root,
            &runtime,
            "node_b",
            &kv,
            &peers,
            AgentRuntimeControlRegistry::new(),
        );
        let chat = SessionId::generate().to_string();
        let a_root = root.join("node_a");
        let b_root = root.join("node_b");
        add_window(&node_a, &a_root, &runtime, "view-a", "window-a");
        add_window(&node_b, &b_root, &runtime, "view-b", "window-b");

        // Node A opens the chat first and hosts it; node B follows A.
        node_a.attach_view("view-a", &chat, &[]).unwrap();
        fanouts_a.lock().unwrap()[&chat].push(wire::ChatEvent::Ready {
            session_id: chat.clone(),
            info: wire::RuntimeInfo::default(),
        });
        node_b.attach_view("view-b", &chat, &[]).unwrap();
        assert!(matches!(
            node_b.hosts.lock().unwrap().get(&chat),
            Some(ChatRoute::Remote(_))
        ));
        assert!(
            commands_b.lock().unwrap().is_empty(),
            "node B runs no runtime"
        );
        wait_for_event(&node_b, "window-b", |event| {
            matches!(event, wire::ChatEvent::Ready { .. })
        });

        // A message sent from node B runs on node A's host.
        node_b
            .command(send("window-b", "b-1", &chat, "from node b"))
            .unwrap();
        let origin = match commands_a.lock().unwrap()[&chat].try_recv().unwrap() {
            HostCommand::Run { origin, .. } => origin,
            HostCommand::Shutdown => panic!("unexpected shutdown"),
        };
        assert!(origin.ends_with("/view-b"), "{origin}");

        // Its turn streams to both nodes; the sender's own message is not
        // echoed back to it, but node A's window shows it.
        let fanout_a = fanouts_a.lock().unwrap()[&chat].clone();
        fanout_a.push_from(
            Some(&origin),
            wire::ChatEvent::UserMessage {
                session_id: chat.clone(),
                text: "from node b".into(),
            },
        );
        fanout_a.push(wire::ChatEvent::Token {
            session_id: chat.clone(),
            text: "reply".into(),
        });
        let on_b = wait_for_event(&node_b, "window-b", |event| {
            matches!(event, wire::ChatEvent::Token { .. })
        });
        assert!(
            !on_b
                .iter()
                .any(|event| matches!(event, wire::ChatEvent::UserMessage { .. }))
        );
        wait_for_event(&node_a, "window-a", |event| {
            matches!(event, wire::ChatEvent::UserMessage { .. })
        });

        // Deleting the chat from node B happens on node A, which hosts it.
        node_b
            .delete_chat_where_hosted(
                sinomo_runtime::local::SinomoDataDir::new(&b_root),
                runtime.handle().clone(),
                &chat,
            )
            .unwrap();
        assert_eq!(
            peers.deletes.lock().unwrap().as_slice(),
            [(
                NodeId::from_legacy_named("node_a").unwrap().to_string(),
                chat.clone()
            )]
        );

        // Control from node B reaches the runtime on node A.
        let cancels = Arc::new(Mutex::new(Vec::new()));
        registry_a
            .register_session(Arc::new(CancelRecorder(
                SessionId::from_str(&chat).unwrap(),
                cancels.clone(),
                None,
            )))
            .unwrap();
        let _ = node_b.command(wire::ChatSessionCommandRequest {
            client_id: "window-b".into(),
            request_id: Some("b-cancel".into()),
            command: wire::ChatSessionCommand::Cancel {
                session_id: chat.clone(),
                reason: "stop from node b".into(),
            },
        });
        assert_eq!(cancels.lock().unwrap().as_slice(), ["stop from node b"]);

        // Node A goes away and stops renewing: node B takes the chat over and
        // its window moves to the new host there.
        peers
            .services
            .lock()
            .unwrap()
            .remove(&NodeId::from_legacy_named("node_a").unwrap().to_string());
        kv.put_record(
            &chat,
            &cluster::ChatOwnerRecord {
                node_id: NodeId::from_legacy_named("node_a").unwrap().to_string(),
                instance: "node_a".into(),
                renewed_at_unix_ms: cluster::now_unix_ms()
                    - cluster::CHAT_OWNER_TTL.as_millis() as u64
                    - 1_000,
            },
        );
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            node_b.maintain_routes().unwrap();
            if matches!(
                node_b.hosts.lock().unwrap().get(&chat),
                Some(ChatRoute::Local(_))
            ) {
                break;
            }
            assert!(Instant::now() < deadline, "node B never took the chat over");
            std::thread::sleep(Duration::from_millis(200));
        }
        assert!(commands_b.lock().unwrap().contains_key(&chat));
        assert_eq!(
            node_b.hosts.lock().unwrap()[&chat]
                .fanout()
                .subscriber_count(),
            1,
            "node B's window follows the chat to its new host"
        );
        fanouts_b.lock().unwrap()[&chat].push(wire::ChatEvent::Notice {
            session_id: chat.clone(),
            notice: "hosted on node b".into(),
        });
        wait_for_event(
            &node_b,
            "window-b",
            |event| matches!(event, wire::ChatEvent::Notice { notice, .. } if notice == "hosted on node b"),
        );

        // Node A notices it lost the chat and stops its host there.
        node_a.maintain_routes().unwrap();
        assert!(matches!(
            node_a.hosts.lock().unwrap().get(&chat),
            Some(ChatRoute::Remote(_))
        ));
    }

    #[test]
    fn event_log_retains_a_bounded_tail() {
        let events = ChatEventLog::new();
        for value in 0..(CHAT_EVENT_RETENTION + 3) {
            events.push(notice(value));
        }

        let state = events
            .state
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(state.events.len(), CHAT_EVENT_RETENTION);
        assert_eq!(state.events.front().map(|event| event.sequence), Some(4));
        assert_eq!(
            state.events.back().map(|event| event.sequence),
            Some((CHAT_EVENT_RETENTION + 3) as u64)
        );
    }

    #[test]
    fn client_ids_are_safe_for_worker_names_and_registry_keys() {
        assert!(validate_client_id("app-chat-42:desktop").is_ok());
        assert!(validate_client_id("").is_err());
        assert!(validate_client_id("contains/a/path").is_err());
        assert!(validate_client_id(&"x".repeat(257)).is_err());
    }

    #[test]
    fn app_routes_are_local_only_and_peer_routes_serve_cluster_members() {
        let routes = HostedChatService::new("127.0.0.1:23572", AgentRuntimeControlRegistry::new())
            .route_categories();

        assert_eq!(routes.len(), wire::CHAT_SERVICE_ROUTES.len());
        for route in routes {
            let expected = if wire::CHAT_PEER_RPC_ROUTES.contains(&route.route.as_str()) {
                wire::CHAT_PEER_ROUTE_AUTH_SCOPE
            } else {
                wire::CHAT_ROUTE_AUTH_SCOPE
            };
            assert_eq!(
                route.auth_scope.as_deref(),
                Some(expected),
                "{}",
                route.route
            );
            assert_eq!(route.required_capabilities.len(), 1);
        }
    }
}
