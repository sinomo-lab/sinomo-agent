//! View lifetime is independent of execution ownership. The per-chat gate
//! serializes teardown, lease renewal and cold activation; it grants no authority.
use super::*;

// Instant may exclude suspend on mobile. A wall-clock discontinuity can expire
// a local cache/confirmation early; it must never extend execution authority.
#[derive(Clone, Copy)]
pub(super) struct HostTime {
    steady: Instant,
    wall: std::time::SystemTime,
}
impl HostTime {
    pub(super) fn now() -> Self {
        Self {
            steady: Instant::now(),
            wall: std::time::SystemTime::now(),
        }
    }
    pub(super) fn elapsed(self) -> Duration {
        self.steady
            .elapsed()
            .max(self.wall.elapsed().unwrap_or(Duration::MAX))
    }
    #[cfg(test)]
    pub(super) fn ago(duration: Duration) -> Self {
        Self {
            steady: Instant::now() - duration,
            wall: std::time::SystemTime::now() - duration,
        }
    }
}

#[derive(Clone)]
pub(super) struct HostCompletion(Arc<(Mutex<bool>, Condvar)>);

impl Default for HostCompletion {
    fn default() -> Self {
        Self(Arc::new((Mutex::new(false), Condvar::new())))
    }
}

impl HostCompletion {
    #[cfg(test)]
    pub(super) fn finished() -> Self {
        let completion = Self::default();
        completion.finish();
        completion
    }

    pub(super) fn finish(&self) {
        *self
            .0
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.0.1.notify_all();
    }

    fn wait(&self) -> Result<(), String> {
        let done = self.0.0.lock().map_err(|_| "chat completion poisoned")?;
        let (done, _) = self
            .0
            .1
            .wait_timeout_while(done, CHAT_HOST_STARTUP_WAIT, |done| !*done)
            .map_err(|_| "chat completion poisoned")?;
        if *done {
            Ok(())
        } else {
            Err("previous chat runtime is still draining".into())
        }
    }
}

#[derive(Clone)]
pub(super) struct DormantChat {
    pub(super) fanout: ChatFanout,
    pub(super) lifecycle: Arc<Mutex<()>>,
    pub(super) peer_requests: Arc<Mutex<VecDeque<(String, Result<(), String>)>>>,
    pub(super) since: Instant,
    // Deferred projection of the existing durable view bootstrap, not authority.
    pub(super) recovery_model: Option<wire::ChatSessionCommand>,
}

impl Default for DormantChat {
    fn default() -> Self {
        Self {
            fanout: ChatFanout::default(),
            lifecycle: Arc::new(Mutex::new(())),
            peer_requests: Arc::new(Mutex::new(VecDeque::new())),
            since: Instant::now(),
            recovery_model: None,
        }
    }
}

impl DormantChat {
    pub(super) fn from_route(route: &ChatRoute) -> Self {
        match route {
            ChatRoute::Dormant(chat) => chat.clone(),
            ChatRoute::Local(host) => Self {
                fanout: host.fanout.clone(),
                lifecycle: host.lifecycle.clone(),
                peer_requests: host.peer_requests.clone(),
                since: Instant::now(),
                recovery_model: None,
            },
            ChatRoute::Remote(remote) => Self {
                fanout: remote.fanout.clone(),
                lifecycle: remote.lifecycle.clone(),
                peer_requests: remote.peer_requests.clone(),
                since: Instant::now(),
                recovery_model: None,
            },
        }
    }

    pub(super) fn model(&self) -> Option<wire::ChatSessionCommand> {
        if let Some(wire::ChatEvent::Ready { info, .. }) = &self.fanout.lock().ready {
            return Some(wire::ChatSessionCommand::SetModel {
                provider: info.provider.clone(),
                model: info.model.clone(),
                account: info.account.clone().unwrap_or_default(),
            });
        }
        self.recovery_model.clone()
    }
}

fn execution_ready(route: &ChatRoute) -> bool {
    match route {
        ChatRoute::Local(host) => {
            host.alive.load(Ordering::Acquire)
                && host
                    .renewed_at
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .elapsed()
                    < CHAT_OWNER_TTL / 2
        }
        ChatRoute::Remote(remote) => remote.alive.load(Ordering::Acquire),
        ChatRoute::Dormant(_) => false,
    }
}

impl HostedChatService {
    /// Attaching and inspecting a view can follow an existing owner, but never
    /// claims execution or creates a local runtime.
    pub(super) fn observe_route(
        &self,
        chat_id: &str,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<String, ChatRoute>>, String> {
        {
            let hosts = self
                .hosts
                .lock()
                .map_err(|_| "chat host registry poisoned")?;
            self.require_running()?;
            if hosts.contains_key(chat_id) {
                return Ok(hosts);
            }
        }
        let environment = self.environment()?;
        let retained = DormantChat::default();
        let route = match environment.cluster.ownership.route_hint(chat_id)? {
            Some(ChatOwner::Remote(owner)) => {
                ChatRoute::Remote(self.follow_remote(&environment, chat_id, owner, &retained)?)
            }
            _ => ChatRoute::Dormant(retained),
        };
        let mut hosts = self
            .hosts
            .lock()
            .map_err(|_| "chat host registry poisoned")?;
        self.require_running()?;
        if !hosts.contains_key(chat_id) {
            if hosts.len() >= MAX_RESIDENT_CHAT_HOSTS {
                self.evict_idle_hosts(&mut hosts, Some(Duration::ZERO), 1)?;
            }
            if hosts.len() >= MAX_RESIDENT_CHAT_HOSTS {
                return Err("too many resident chat views".into());
            }
            hosts.insert(chat_id.to_string(), route);
        }
        Ok(hosts)
    }

    pub(super) fn activate_route(
        &self,
        chat_id: &str,
        history: &[wire::ChatHistoryMessage],
    ) -> Result<std::sync::MutexGuard<'_, HashMap<String, ChatRoute>>, String> {
        loop {
            let retained = {
                let hosts = self.observe_route(chat_id)?;
                let route = hosts.get(chat_id).ok_or("chat view disappeared")?;
                if execution_ready(route) {
                    return Ok(hosts);
                }
                DormantChat::from_route(route)
            };
            let _activation = retained
                .lifecycle
                .lock()
                .map_err(|_| "chat lifecycle poisoned")?;
            let (completion, confirmation) = {
                let hosts = self
                    .hosts
                    .lock()
                    .map_err(|_| "chat host registry poisoned")?;
                self.require_running()?;
                let Some(route) = hosts.get(chat_id) else {
                    continue;
                };
                let current = DormantChat::from_route(route);
                if !Arc::ptr_eq(&current.lifecycle, &retained.lifecycle) {
                    continue;
                }
                if execution_ready(route) {
                    return Ok(hosts);
                }
                match route {
                    ChatRoute::Local(host) if host.alive.load(Ordering::Acquire) => {
                        (None, Some((host.alive.clone(), host.renewed_at.clone())))
                    }
                    ChatRoute::Local(host) => (Some(host.completion.clone()), None),
                    _ => (None, None),
                }
            };
            if let Some((alive, renewed_at)) = confirmation {
                let environment = self.environment()?;
                let renewed = environment.cluster.ownership.renew(chat_id);
                let hosts = self
                    .hosts
                    .lock()
                    .map_err(|_| "chat host registry poisoned")?;
                let Some(ChatRoute::Local(host)) = hosts.get(chat_id) else {
                    continue;
                };
                if !Arc::ptr_eq(&host.alive, &alive) {
                    continue;
                }
                if matches!(renewed, Ok(true)) {
                    *renewed_at
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = HostTime::now();
                    return Ok(hosts);
                }
                host.stop();
                if let Ok(session_id) = SessionId::from_str(chat_id) {
                    let _ = self.runtime_control.cancel(AgentSessionCancelRequest {
                        session_id,
                        reason: "chat ownership confirmation expired".into(),
                    });
                }
                return Err(
                    "chat ownership could not be confirmed; command was not admitted".into(),
                );
            }
            if let Some(completion) = completion {
                completion.wait()?;
            }
            let environment = self.environment()?;
            // claim() must confirm a fresh CAS even if a previous release was
            // uncertain and the persisted owner still names this process.
            let route = match environment.cluster.ownership.claim(chat_id)? {
                ChatOwner::Local => {
                    ChatRoute::Local(self.start_host(&environment, chat_id, history, &retained)?)
                }
                ChatOwner::Remote(owner) => ChatRoute::Remote(self.follow_remote(
                    &environment,
                    chat_id,
                    owner,
                    &retained,
                )?),
            };
            let mut hosts = self
                .hosts
                .lock()
                .map_err(|_| "chat host registry poisoned")?;
            self.require_running()?;
            hosts.insert(chat_id.to_string(), route);
            return Ok(hosts);
        }
    }

    /// Called by the idle actor while holding its lifecycle gate. A sender
    /// enqueues and touches activity under the same route registry guard, so an
    /// admitted command cannot be overtaken by the idle transition.
    pub(super) fn begin_dormancy(
        &self,
        chat_id: &str,
        alive: &Arc<AtomicBool>,
    ) -> Option<ChatHost> {
        let mut hosts = self.hosts.lock().ok()?;
        self.require_running().ok()?;
        let Some(ChatRoute::Local(host)) = hosts.get(chat_id) else {
            return None;
        };
        if !Arc::ptr_eq(&host.alive, alive)
            || !host.alive.load(Ordering::Acquire)
            || !host
                .activity
                .idle_for(&self.runtime_control)
                .is_some_and(|idle| idle >= CHAT_HOST_WARM_IDLE)
        {
            return None;
        }
        let retained = DormantChat::from_route(hosts.get(chat_id)?);
        let Some(ChatRoute::Local(host)) =
            hosts.insert(chat_id.to_string(), ChatRoute::Dormant(retained))
        else {
            unreachable!()
        };
        host.stop();
        Some(host)
    }

    pub(super) fn abort_dormancy(&self, chat_id: &str, host: ChatHost) {
        if let Ok(mut hosts) = self.hosts.lock()
            && self.require_running().is_ok()
            && matches!(hosts.get(chat_id), Some(ChatRoute::Dormant(chat)) if Arc::ptr_eq(&chat.lifecycle, &host.lifecycle))
        {
            host.alive.store(true, Ordering::Release);
            hosts.insert(chat_id.to_string(), ChatRoute::Local(host));
        }
    }

    pub(super) fn observe_control(
        &self,
        chat_id: &str,
        storage: &local::AgentChatSifsStorage,
        events: &ChatEventLog,
        command: wire::ChatSessionCommand,
    ) -> Result<(), String> {
        let remote = {
            let hosts = self.observe_route(chat_id)?;
            match hosts.get(chat_id) {
                Some(ChatRoute::Remote(remote)) if remote.alive.load(Ordering::Acquire) => {
                    Some(remote.owner.clone())
                }
                _ => None,
            }
        };
        if let Some(owner) = remote {
            let environment = self.environment()?;
            let response = environment.cluster.peers.command(
                &owner,
                wire::ChatPeerCommandRequest {
                    chat_id: chat_id.into(),
                    origin: String::new(),
                    request_id: SessionId::generate().to_string(),
                    command,
                },
            )?;
            if response.accepted {
                return Ok(());
            }
            return Err("chat observation host is unavailable".into());
        }
        self.publish_observation(chat_id, storage, events, command)
    }

    pub(super) fn publish_observation(
        &self,
        chat_id: &str,
        storage: &local::AgentChatSifsStorage,
        events: &ChatEventLog,
        command: wire::ChatSessionCommand,
    ) -> Result<(), String> {
        match command {
            wire::ChatSessionCommand::ControlState { session_id } => {
                let request = AgentSessionResumeRequest {
                    session_id: SessionId::from_str(&session_id).map_err(|e| e.to_string())?,
                };
                let environment = self.environment()?;
                let response = self
                    .runtime_control
                    .resume_or_persisted(
                        &sinomo_agent::FileAgentSessionStore::for_data_dir(&environment.data_dir),
                        request,
                    )
                    .map_err(|e| e.to_string())?;
                events.push(wire::ChatEvent::ControlState(control_resume_state(
                    response,
                )));
            }
            wire::ChatSessionCommand::FetchWorkspaces { .. } => {
                let channel = storage
                    .open_channel_store()?
                    .load_channel(&SessionId::from_str(chat_id).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
                let mut selection = self
                    .hosts
                    .lock()
                    .map_err(|_| "chat host registry poisoned")?
                    .get(chat_id)
                    .and_then(|route| match &route.fanout().lock().tool_runtime {
                        Some(wire::ChatEvent::ToolRuntime(selection)) => Some(selection.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();
                selection.workspace_session_id = Some(chat_id.into());
                if let Some(channel) = channel {
                    selection.workspace_root = channel
                        .workspaces
                        .first()
                        .and_then(|workspace| workspace.local_root.clone());
                    selection.workspaces = Some(
                        channel
                            .workspaces
                            .into_iter()
                            .map(|workspace| wire::ChatWorkspace {
                                path: workspace
                                    .sifs_root
                                    .map(|path| format!("sifs:{path}"))
                                    .or_else(|| {
                                        workspace.local_root.map(|path| path.display().to_string())
                                    })
                                    .unwrap_or_else(|| workspace.id.clone()),
                                id: workspace.id,
                            })
                            .collect(),
                    );
                }
                events.push(wire::ChatEvent::ToolRuntime(selection));
            }
            _ => return Err("command is not observational".into()),
        }
        Ok(())
    }
}

// The same guard accounts for queued actor commands and concurrent controls.
// Keeping it in the queued message protects admission even if a mobile process
// is suspended for longer than the grace interval before the worker receives it.
pub(super) struct HostAdmission {
    activity: HostActivity,
    turn: Option<Arc<AdmittedTurn>>,
}
impl HostAdmission {
    pub(super) fn cancellation(&self) -> Option<sinomo_agent::AgentCancellationToken> {
        self.turn.as_ref().map(|turn| turn.cancellation.clone())
    }
}
impl Drop for HostAdmission {
    fn drop(&mut self) {
        let mut state = self.activity.lock();
        state.admissions -= 1;
        state.admitted_turns.retain(|turn| {
            turn.strong_count() > 0
                && self
                    .turn
                    .as_ref()
                    .is_none_or(|mine| turn.as_ptr() != Arc::as_ptr(mine))
        });
        state.last_active = HostTime::now();
        drop(state);
        // A concurrent control ends here: the worker recomputes its idle
        // deadline and checks what the control changed.
        self.activity.wake();
    }
}
impl HostActivity {
    pub(super) fn admit(&self) -> HostAdmission {
        self.admit_inner(None)
    }

    pub(super) fn admit_command(
        &self,
        command: &wire::ChatSessionCommand,
        journal: Option<&(CommandJournal, String)>,
    ) -> HostAdmission {
        let turn = matches!(command, wire::ChatSessionCommand::Send { .. }).then(|| {
            Arc::new(AdmittedTurn {
                cancellation: sinomo_agent::AgentCancellationToken::new(),
                journal: journal.cloned(),
            })
        });
        self.admit_inner(turn)
    }

    fn admit_inner(&self, turn: Option<Arc<AdmittedTurn>>) -> HostAdmission {
        let mut state = self.lock();
        state.admissions += 1;
        if let Some(turn) = &turn {
            state.admitted_turns.push(Arc::downgrade(turn));
        }
        state.last_active = HostTime::now();
        HostAdmission {
            activity: self.clone(),
            turn,
        }
    }

    pub(super) fn cancel_admitted_turns(&self) -> Result<bool, String> {
        let turns = self
            .lock()
            .admitted_turns
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .collect::<Vec<_>>();
        for turn in &turns {
            turn.cancellation.cancel();
            if let Some((journal, id)) = &turn.journal {
                journal.reject_if_pending(id, "chat input cancelled before execution")?;
            }
        }
        Ok(!turns.is_empty())
    }
}

impl ChatHost {
    pub(super) fn apply_control(
        &self,
        registry: &AgentRuntimeControlRegistry,
        command: &wire::ChatSessionCommand,
    ) -> Result<Option<DirectControlEffect>, String> {
        if let wire::ChatSessionCommand::Cancel { session_id, .. } = command {
            SessionId::from_str(session_id).map_err(|error| error.to_string())?;
        }
        let pending_cancel = matches!(command, wire::ChatSessionCommand::Cancel { .. })
            && self.activity.cancel_admitted_turns()?;
        if pending_cancel && !self.ready.load(Ordering::Acquire) {
            return Ok(Some(DirectControlEffect::PendingCancellation(
                command_chat_id(command).unwrap_or_default().to_string(),
            )));
        }
        let effect = apply_direct_control(registry, command)?;
        if pending_cancel
            && matches!(&effect, Some(DirectControlEffect::Input(_, result)) if !result.accepted)
        {
            return Ok(Some(DirectControlEffect::PendingCancellation(
                command_chat_id(command).unwrap_or_default().to_string(),
            )));
        }
        Ok(effect)
    }
}

impl HostedChatService {
    pub(super) fn execute_control(
        &self,
        origin: &str,
        storage: &local::AgentChatSifsStorage,
        view_events: &ChatEventLog,
        chat_id: &str,
        journal: Option<(CommandJournal, String)>,
        command: wire::ChatSessionCommand,
    ) -> Result<(), String> {
        let (alive, _admission) = {
            let hosts = match self.ensure_route(chat_id, &[]) {
                Ok(hosts) => hosts,
                Err(error) => {
                    if let Some((journal, id)) = &journal {
                        journal.reject(id, error.clone())?;
                    }
                    return Err(error);
                }
            };
            match hosts.get(chat_id) {
                Some(ChatRoute::Remote(_)) => {
                    drop(hosts);
                    return self.dispatch(origin, chat_id, journal, command);
                }
                Some(ChatRoute::Local(host)) => {
                    host.activity.touch();
                    if !host.ready.load(Ordering::Acquire)
                        && !matches!(command, wire::ChatSessionCommand::Cancel { .. })
                    {
                        return host.enqueue(origin.into(), journal, command);
                    }
                    (host.alive.clone(), host.activity.admit())
                }
                _ => return Err("chat host disappeared".into()),
            }
        };
        if let Some((journal, id)) = &journal {
            journal.start(id)?;
        }
        let result: Result<_, String> = (|| {
            let hosts = self
                .hosts
                .lock()
                .map_err(|_| "chat host registry poisoned")?;
            let Some(ChatRoute::Local(host)) = hosts.get(chat_id) else {
                return Err("chat host stopped before control admission".into());
            };
            if !Arc::ptr_eq(&host.alive, &alive) || !host.alive.load(Ordering::Acquire) {
                return Err("chat host stopped before control admission".into());
            }
            let effect = host.apply_control(&self.runtime_control, &command)?;
            Ok((host.fanout.clone(), effect))
        })();
        match result {
            Ok((fanout, effect)) => {
                if let Some(effect) = effect {
                    let sink: &dyn ChatEventSink = if fanout.subscriber_count() == 0 {
                        view_events
                    } else {
                        &fanout
                    };
                    publish_direct_control(storage, sink, effect);
                }
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
}

impl HostedChatService {
    /// Rebind only event following from the resident placement projection. An
    /// expired record is a routing hint, never permission to create a runtime.
    pub(super) fn refresh_dormant_routes(&self) -> Result<(), String> {
        let environment = self.environment()?;
        let candidates = self
            .hosts
            .lock()
            .map_err(|_| "chat host registry poisoned")?
            .iter()
            .filter(|(_, route)| !route.alive() && route.fanout().has_views())
            .map(|(chat, route)| (chat.clone(), DormantChat::from_route(route)))
            .collect::<Vec<_>>();
        for (chat_id, retained) in candidates {
            let hint = environment.cluster.ownership.route_hint(&chat_id)?;
            let Ok(_gate) = retained.lifecycle.try_lock() else {
                continue;
            };
            {
                let hosts = self
                    .hosts
                    .lock()
                    .map_err(|_| "chat host registry poisoned")?;
                let Some(route) = hosts.get(&chat_id) else {
                    continue;
                };
                if route.alive()
                    || !Arc::ptr_eq(
                        &DormantChat::from_route(route).lifecycle,
                        &retained.lifecycle,
                    )
                {
                    continue;
                }
                if let ChatRoute::Local(host) = route
                    && !*host
                        .completion
                        .0
                        .0
                        .lock()
                        .map_err(|_| "chat completion poisoned")?
                {
                    continue;
                }
            }
            let route = match hint {
                Some(ChatOwner::Remote(owner)) => ChatRoute::Remote(self.follow_remote(
                    &environment,
                    &chat_id,
                    owner,
                    &retained,
                )?),
                _ => continue,
            };
            let mut hosts = self
                .hosts
                .lock()
                .map_err(|_| "chat host registry poisoned")?;
            self.require_running()?;
            hosts.insert(chat_id, route);
        }
        Ok(())
    }
}

impl HostedChatService {
    /// Admission can wait for this chat's teardown or for a KV self-RPC. Keep
    /// that wait in the component's existing owned thread registry, not the
    /// bounded Tokio blocking pool that serves the nested KV/storage request.
    pub(super) async fn owned_request<T: Send + 'static>(
        &self,
        run: impl FnOnce() -> Result<T, String> + Send + 'static,
    ) -> Result<T, ServiceError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let span = tracing::Span::current();
        let runtime = tokio::runtime::Handle::current();
        self.spawn_worker("sinomo-chat-admission".into(), move || {
            let _runtime = runtime.enter();
            let _entered = span.enter();
            let _ = tx.send(run());
        })
        .map_err(ServiceError::Service)?;
        rx.await
            .map_err(|error| {
                ServiceError::Service(format!("chat admission worker stopped: {error}"))
            })?
            .map_err(ServiceError::Service)
    }
}

impl HostedChatService {
    /// Resuming a view must recover already-admitted work, without warming an
    /// ordinary quiescent session. In particular a completed Send can still own
    /// a detached tool continuation after a daemon restart.
    pub(super) fn restore_unfinished_work(
        &self,
        chat_id: &str,
        storage: &local::AgentChatSifsStorage,
    ) -> Result<(), String> {
        use sinomo_agent::AgentSessionStore;
        {
            let hosts = self
                .hosts
                .lock()
                .map_err(|_| "chat host registry poisoned")?;
            if hosts.get(chat_id).is_some_and(ChatRoute::alive) {
                return Ok(());
            }
        }
        let environment = self.environment()?;
        let store = sinomo_agent::FileAgentSessionStore::for_data_dir(&environment.data_dir);
        let session_id = SessionId::from_str(chat_id).map_err(|e| e.to_string())?;
        let pending = |session: &SessionId| -> Result<bool, String> {
            if !store
                .load_detached_calls(session)
                .map_err(|e| e.to_string())?
                .is_empty()
            {
                return Ok(true);
            }
            if store
                .load_session(session)
                .map_err(|e| e.to_string())?
                .is_some_and(|session| {
                    !session.steering_queue.is_empty() || !session.follow_up_queue.is_empty()
                })
            {
                return Ok(true);
            }
            Ok(store
                .load_control_snapshot(session)
                .map_err(|e| e.to_string())?
                .is_some_and(|snapshot| snapshot.active_turn.is_some()))
        };
        let mut recover = pending(&session_id)?;
        if !recover
            && let Some(channel) = storage
                .open_channel_store()?
                .load_channel(&session_id)
                .map_err(|e| e.to_string())?
        {
            for participant in channel.enabled_participants_in_order() {
                if participant.agent_session_id != session_id
                    && pending(&participant.agent_session_id)?
                {
                    recover = true;
                    break;
                }
            }
        }
        if recover {
            drop(self.ensure_route(chat_id, &[])?);
        }
        Ok(())
    }
}

#[cfg(test)]
mod clock_tests {
    use super::*;
    #[test]
    fn suspend_and_clock_rollback_cannot_extend_idle_or_lease_confirmation() {
        let slept = HostTime {
            steady: Instant::now(),
            wall: std::time::SystemTime::now() - CHAT_OWNER_TTL,
        };
        assert!(slept.elapsed() >= CHAT_OWNER_TTL);
        let rollback = HostTime {
            steady: Instant::now(),
            wall: std::time::SystemTime::now() + Duration::from_secs(60),
        };
        assert_eq!(rollback.elapsed(), Duration::MAX);
    }
}
