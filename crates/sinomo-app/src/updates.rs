//! Event-driven desktop update presentation. The daemon owns release state and the launcher
//! owns replacement; this module only saves presentation state and requests a restart.
use crate::state::Shared;
use sinomo_client::{AppUpdateClient, api::app::update::*};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver},
    },
    time::{Duration, Instant},
};

const RESTORATION_STABILITY_WINDOW: Duration = Duration::from_secs(2);

/// A pending update transition owns its wake-up. An idle app must not depend
/// on chat traffic or user input to acknowledge restoration or retry a reply.
#[derive(Default)]
struct DeadlineWake {
    pending: Option<(Instant, Arc<AtomicBool>)>,
}

impl DeadlineWake {
    fn schedule(&mut self, deadline: Option<Instant>, wake: Option<Arc<dyn Fn() + Send + Sync>>) {
        if self.pending.as_ref().map(|(at, _)| *at) == deadline {
            return;
        }
        if let Some((_, cancelled)) = self.pending.take() {
            cancelled.store(true, Ordering::Release);
        }
        let (Some(deadline), Some(wake)) = (deadline, wake) else {
            return;
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        self.pending = Some((deadline, cancelled.clone()));
        std::thread::spawn(move || {
            std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
            if !cancelled.load(Ordering::Acquire) {
                wake();
            }
        });
    }
}

impl Drop for DeadlineWake {
    fn drop(&mut self) {
        if let Some((_, cancelled)) = self.pending.take() {
            cancelled.store(true, Ordering::Release);
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct UpdateView {
    pub status: AppUpdateStatus,
    pub busy: bool,
    pub when_idle: bool,
    pub dismissed: bool,
    pub error: Option<String>,
}

#[derive(Default)]
pub(crate) struct AppUpdates {
    pub view: UpdateView,
    address: Option<String>,
    receiver: Option<Receiver<Result<AppUpdateStatus, String>>>,
    restart: Option<Receiver<Result<AppUpdateStatus, String>>>,
    checkpoint: Option<Vec<Receiver<Result<(), String>>>>,
    acknowledged: bool,
    restart_intent: Option<String>,
    retry_restart_after: Option<std::time::Instant>,
    pub rendered_ready: AtomicBool,
    ready_since: Option<std::time::Instant>,
    retry_ack_after: Option<std::time::Instant>,
    ack: Option<Receiver<Result<AppUpdateStatus, String>>>,
    cancel: Arc<AtomicBool>,
    deadline_wake: DeadlineWake,
    readiness_observation: Option<(usize, usize, bool)>,
}

impl Drop for AppUpdates {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Release);
    }
}

impl AppUpdates {
    fn acknowledgement_deadline(&self) -> Option<Instant> {
        if self.acknowledged || self.ack.is_some() {
            return None;
        }
        let stable_at = self.ready_since? + RESTORATION_STABILITY_WINDOW;
        Some(
            self.retry_ack_after
                .map_or(stable_at, |retry| retry.max(stable_at)),
        )
    }

    fn connect(&mut self, address: Option<String>, wake: Option<sui::UiHandle>) {
        if self.address == address {
            return;
        }
        self.cancel.store(true, Ordering::Release);
        self.cancel = Arc::new(AtomicBool::new(false));
        self.address = address.clone();
        self.acknowledged = false;
        let Some(address) = address else {
            self.receiver = None;
            return;
        };
        let cancel = self.cancel.clone();
        let (tx, rx) = mpsc::channel();
        self.receiver = Some(rx);
        std::thread::spawn(move || {
            let client = AppUpdateClient::at_address(address);
            let mut revision = None;
            while !cancel.load(Ordering::Acquire) {
                let response = client.status(revision, 25_000).map_err(|e| e.to_string());
                let failed = response.is_err();
                if let Ok(status) = &response {
                    if revision == Some(status.revision) {
                        continue;
                    }
                    revision = Some(status.revision);
                }
                if tx.send(response).is_err() {
                    break;
                }
                if let Some(wake) = &wake {
                    wake.wake();
                }
                if failed {
                    std::thread::sleep(Duration::from_secs(5));
                }
            }
        });
    }
}

pub(crate) fn request_restart(shared: &Shared, when_idle: bool) {
    if !shared.is_ready() {
        return;
    }
    let chats = shared.chat_scopes();
    let checkpoint_allowed = chats.iter().all(|scope| {
        let chat = scope.chat();
        chat.handle.is_connected() && chat.can_checkpoint_workspace()
    }) && std::iter::once(shared.root_scope())
        .chain(shared.workspace_scopes())
        .all(|scope| {
            scope.workspace_kind() == Some(crate::state::WorkspaceTabKind::Chat)
                || scope.files().can_checkpoint_workspace()
        });
    let media_active = chats.iter().any(|scope| scope.chat().media_call.active);
    let mut app = shared.borrow_mut();
    if app.updates.view.busy {
        return;
    }
    if when_idle {
        app.updates.view.when_idle = true;
        return;
    }
    if !checkpoint_allowed {
        app.updates.view.error = Some(
            "Wait for workspace loading and open file dialogs to finish before restarting.".into(),
        );
        return;
    }
    if std::env::var_os(APP_LAUNCH_TOKEN_ENV).is_none() {
        app.updates.view.error =
            Some("Launch the installed desktop app to enable supervised updates.".into());
        return;
    }
    if media_active {
        app.updates.view.error = Some("Finish the voice or video call before restarting.".into());
        return;
    }
    app.updates.view.busy = true;
    app.updates.restart_intent = app.updates.view.status.candidate_release.clone();
    app.updates.view.error = None;
    drop(app);
    let checkpoints = chats
        .iter()
        .map(|scope| scope.chat().handle.prepare_restart())
        .collect();
    shared.borrow_mut().updates.checkpoint = Some(checkpoints);
}

pub(crate) fn pump(shared: &Shared) -> bool {
    if cfg!(target_os = "android") {
        return false;
    }
    if !shared.is_ready() {
        return false;
    }
    let chats = shared.chat_scopes();
    let (address, waker, connected, idle, restore_error) = {
        let app = shared.borrow();
        let ready = app.ready.as_ref().expect("ready");
        (
            ready.backend.connection.daemon_address().map(str::to_owned),
            app.waker.clone(),
            chats.iter().all(|scope| {
                let chat = scope.chat();
                chat.handle.is_connected() && chat.restoration_complete()
            }),
            chats.iter().all(|scope| {
                let chat = scope.chat();
                !chat.streaming && !chat.control.active_turn && !chat.media_call.active
            }),
            chats
                .iter()
                .find_map(|scope| scope.chat().restoration_error.clone()),
        )
    };
    let observation = (
        chats
            .iter()
            .filter(|scope| !scope.chat().handle.is_connected())
            .count(),
        chats
            .iter()
            .filter(|scope| !scope.chat().restoration_complete())
            .count(),
        shared
            .borrow()
            .updates
            .rendered_ready
            .load(Ordering::Acquire),
    );
    let was_busy = shared.borrow().updates.view.busy;
    let mut checkpoint_ready = false;
    let mut exit = false;
    let mut install_idle = false;
    {
        let mut app = shared.borrow_mut();
        let updates = &mut app.updates;
        updates.connect(address.clone(), waker.clone());
        if !updates.acknowledged && updates.readiness_observation != Some(observation) {
            updates.readiness_observation = Some(observation);
            tracing::info!(
                disconnected_chats = observation.0,
                pending_restorations = observation.1,
                rendered = observation.2,
                "desktop restoration readiness changed"
            );
        }
        if connected && restore_error.is_none() && updates.rendered_ready.load(Ordering::Acquire) {
            updates
                .ready_since
                .get_or_insert_with(std::time::Instant::now);
        } else {
            updates.ready_since = None;
        }
        if let Some(rx) = &updates.receiver {
            for result in rx.try_iter() {
                match result {
                    Ok(status) => {
                        if status.candidate_release != updates.view.status.candidate_release {
                            updates.view.dismissed = false;
                        }
                        updates.view.status = status;
                    }
                    Err(error) => {
                        tracing::debug!(%error, "desktop update observation unavailable");
                    }
                }
            }
        }
        if !updates.acknowledged
            && updates
                .ready_since
                .is_some_and(|ready| ready.elapsed() >= RESTORATION_STABILITY_WINDOW)
            && updates.ack.is_none()
            && updates
                .retry_ack_after
                .is_none_or(|deadline| std::time::Instant::now() >= deadline)
            && connected
            && restore_error.is_none()
            && let (Some(address), Ok(token)) =
                (address.clone(), std::env::var(APP_LAUNCH_TOKEN_ENV))
        {
            updates.ack = Some(background(waker.clone(), move || {
                AppUpdateClient::at_address(address)
                    .ready(token)
                    .map_err(|e| e.to_string())
            }));
        }
        if let Some(result) = updates.ack.as_ref().and_then(|rx| rx.try_recv().ok()) {
            updates.ack = None;
            match result {
                Ok(_) => {
                    updates.acknowledged = true;
                    tracing::info!("desktop restoration acknowledged");
                }
                Err(error) => {
                    tracing::warn!(%error, "desktop restoration acknowledgement will retry");
                    updates.view.error = Some(error);
                    updates.retry_ack_after =
                        Some(std::time::Instant::now() + Duration::from_secs(5));
                }
            }
        }
        if let Some(checkpoints) = &mut updates.checkpoint {
            let mut error = None;
            checkpoints.retain(|rx| match rx.try_recv() {
                Ok(Ok(())) => false,
                Ok(Err(message)) => {
                    error = Some(message);
                    false
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    error =
                        Some("A chat stopped before its workspace checkpoint completed.".into());
                    false
                }
                Err(mpsc::TryRecvError::Empty) => true,
            });
            if let Some(error) = error {
                updates.checkpoint = None;
                updates.view.error = Some(error);
                updates.view.busy = false;
            } else if checkpoints.is_empty() {
                updates.checkpoint = None;
                checkpoint_ready = true;
            }
        }
        if let Some(result) = updates.restart.as_ref().and_then(|rx| rx.try_recv().ok()) {
            updates.restart = None;
            match result {
                Ok(_) => exit = true,
                Err(error) => {
                    updates.view.error = Some(error);
                    updates.view.busy = false;
                    updates.retry_restart_after =
                        Some(std::time::Instant::now() + Duration::from_secs(5));
                }
            }
        }
        if !updates.view.busy
            && updates.view.status.phase == AppUpdatePhase::RestartRequested
            && updates.restart_intent.is_some()
            && updates.restart_intent == updates.view.status.candidate_release
            && updates
                .retry_restart_after
                .is_none_or(|deadline| std::time::Instant::now() >= deadline)
        {
            install_idle = true;
        }
        if updates.view.when_idle
            && idle
            && connected
            && !updates.view.busy
            && updates.view.status.phase == AppUpdatePhase::Ready
        {
            updates.view.when_idle = false;
            install_idle = true;
        }
        let acknowledgement = (address.is_some()
            && std::env::var_os(APP_LAUNCH_TOKEN_ENV).is_some())
        .then(|| updates.acknowledgement_deadline())
        .flatten();
        let restart = (!updates.view.busy
            && updates.view.status.phase == AppUpdatePhase::RestartRequested
            && updates.restart_intent.is_some())
        .then_some(updates.retry_restart_after)
        .flatten();
        let deadline = acknowledgement
            .into_iter()
            .chain(restart.filter(|at| *at > Instant::now()))
            .min();
        let wake = waker
            .clone()
            .map(|waker| Arc::new(move || waker.wake()) as Arc<dyn Fn() + Send + Sync>);
        updates.deadline_wake.schedule(deadline, wake);
    }
    if checkpoint_ready {
        // The composer is now covered by the restarting surface. Incoming runtime events can
        // continue; anything after this exact cursor is replayed by the replacement process.
        match crate::restoration::save(shared) {
            Ok(()) => {
                let candidate = shared
                    .borrow()
                    .updates
                    .view
                    .status
                    .candidate_release
                    .clone();
                let token = std::env::var(APP_LAUNCH_TOKEN_ENV).ok();
                if let (Some(address), Some(candidate), Some(token)) = (address, candidate, token) {
                    shared.borrow_mut().updates.restart = Some(background(waker, move || {
                        AppUpdateClient::at_address(address)
                            .restart(candidate, token)
                            .map_err(|e| e.to_string())
                    }));
                } else {
                    let mut app = shared.borrow_mut();
                    app.updates.view.busy = false;
                    app.updates.view.error = Some("No supervised update is ready.".into());
                }
            }
            Err(error) => {
                let mut app = shared.borrow_mut();
                app.updates.view.busy = false;
                app.updates.view.error = Some(format!("Workspace could not be saved: {error}"));
            }
        }
    }
    if was_busy && !shared.borrow().updates.view.busy {
        for scope in shared.chat_scopes() {
            scope.chat_mut().reconnect_after_update_failure();
        }
    }
    if install_idle {
        request_restart(shared, false);
    }
    if exit {
        std::process::exit(APP_RESTART_EXIT_CODE);
    }
    let view = shared.borrow().updates.view.clone();
    let changed = shared.signals().app_update.get() != view;
    shared.signals().app_update.set(view);
    changed
}

fn background<T: Send + 'static>(
    wake: Option<sui::UiHandle>,
    work: impl FnOnce() -> T + Send + 'static,
) -> Receiver<T> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(work());
        if let Some(wake) = wake {
            wake.wake();
        }
    });
    rx
}

pub(crate) fn banner(shared: Shared) -> impl sui::Widget {
    use sui::{Button, Flex, Label, RebuildOnChange, WidgetPod};
    RebuildOnChange::new_observable(shared.signals().theme.clone(), move |theme| {
        let shared = shared.clone();
        let theme = *theme;
        WidgetPod::new(RebuildOnChange::new_observable(
            shared.signals().app_update.clone(),
            move |view| {
                let label = |text: &str| {
                    Label::new(text.to_owned()).style(theme.text_style(theme.palette.text))
                };
                let mut row = Flex::horizontal()
                    .main_gap(8.0)
                    .align_items(sui::Alignment::Center)
                    .wrap(sui::FlexWrap::Wrap);
                if let Some(error) = &view.error {
                    row = row.with_child(label(error));
                }
                if view.busy {
                    row = row.with_child(label("Saving your workspace and restarting…"));
                } else if view.status.phase == AppUpdatePhase::RestartRequested {
                    let shared = shared.clone();
                    row = row
                        .with_child(label("Finishing the requested app update…"))
                        .with_child(
                            Button::new("Retry restart")
                                .theme(theme)
                                .on_press(move || request_restart(&shared, false)),
                        );
                } else if view.status.phase == AppUpdatePhase::Ready {
                    if view.dismissed {
                        let shared = shared.clone();
                        row =
                            row.with_child(Button::new("Update available").theme(theme).on_press(
                                move || {
                                    shared.borrow_mut().updates.view.dismissed = false;
                                },
                            ));
                    } else {
                        let now = shared.clone();
                        let idle = shared.clone();
                        let later = shared.clone();
                        row = row
                            .with_child(label(if view.when_idle {
                                "Update ready — waiting for idle"
                            } else {
                                "App update ready"
                            }))
                            .with_child(
                                Button::new("Restart and restore")
                                    .theme(theme)
                                    .on_press(move || request_restart(&now, false)),
                            )
                            .with_child(
                                Button::new("When idle")
                                    .theme(theme)
                                    .on_press(move || request_restart(&idle, true)),
                            )
                            .with_child(Button::new("Later").theme(theme).on_press(move || {
                                let mut app = later.borrow_mut();
                                app.updates.view.dismissed = true;
                                app.updates.view.when_idle = false;
                                app.updates.restart_intent = None;
                            }));
                    }
                } else if view.status.phase == AppUpdatePhase::RolledBack {
                    row = row.with_child(label(&view.status.detail));
                }
                WidgetPod::new(row)
            },
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_restoration_deadline_wakes_once_and_cancellation_retires_the_wake() {
        let (tx, rx) = mpsc::channel();
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let _ = tx.send(());
        });
        let mut timer = DeadlineWake::default();
        let deadline = Instant::now() + Duration::from_millis(25);
        timer.schedule(Some(deadline), Some(wake.clone()));
        timer.schedule(Some(deadline), Some(wake.clone()));
        rx.recv_timeout(Duration::from_secs(2))
            .expect("idle update owns its deadline wake");
        assert!(
            rx.try_recv().is_err(),
            "repeated pumps must not duplicate timers"
        );
        timer.schedule(Some(Instant::now() + Duration::from_millis(25)), Some(wake));
        timer.schedule(None, None);
        assert!(
            rx.recv_timeout(Duration::from_millis(80)).is_err(),
            "retired deadline must not wake the app"
        );
    }

    #[test]
    fn acknowledgement_deadline_preserves_stability_retry_and_inflight_boundaries() {
        let start = Instant::now();
        let mut updates = AppUpdates::default();
        assert_eq!(updates.acknowledgement_deadline(), None);
        updates.ready_since = Some(start);
        assert_eq!(
            updates.acknowledgement_deadline(),
            Some(start + RESTORATION_STABILITY_WINDOW)
        );
        updates.retry_ack_after = Some(start + Duration::from_secs(5));
        assert_eq!(updates.acknowledgement_deadline(), updates.retry_ack_after);
        // Losing restoration readiness cancels the scheduled acknowledgement.
        updates.ready_since = None;
        assert_eq!(updates.acknowledgement_deadline(), None);
        updates.ready_since = Some(start + Duration::from_secs(7));
        assert_eq!(
            updates.acknowledgement_deadline(),
            Some(start + Duration::from_secs(9))
        );
        let (_tx, rx) = mpsc::channel();
        updates.ack = Some(rx);
        assert_eq!(updates.acknowledgement_deadline(), None);
        updates.ack = None;
        updates.acknowledged = true;
        assert_eq!(updates.acknowledgement_deadline(), None);
    }

    #[test]
    fn desktop_update_notice_is_accessible_and_can_be_deferred_and_reopened() -> sui::Result<()> {
        let mut state = crate::state::AppState::ready(crate::backend::Backend::local_fallback());
        state.theme_mode = crate::theme::AppThemeMode::Dark;
        state.updates.view.status.phase = AppUpdatePhase::Ready;
        state.updates.view.status.candidate_release = Some("candidate".into());
        let shared = Shared::new(state);
        let runtime = sui::App::new()
            .window(sui::Window::new("Desktop update").root(banner(shared.clone())))
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(sui::Event::Window(sui::WindowEvent::Resized(
                sui::Size::new(320.0, 240.0),
            )))?;
        app.run_until_idle()?;
        let snapshot = window.snapshot()?;
        assert!(
            snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("Restart and restore"))
        );
        for button in snapshot
            .accessibility
            .nodes
            .iter()
            .filter(|n| n.role == sui::SemanticsRole::Button)
        {
            assert!(
                button.bounds.max_x() <= 320.5,
                "update action clipped: {:?}",
                button.bounds
            );
        }
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Later")
            .click()?;
        assert!(shared.borrow().updates.view.dismissed);
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Update available")
            .click()?;
        assert!(!shared.borrow().updates.view.dismissed);
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("When idle")
            .click()?;
        assert!(shared.borrow().updates.view.when_idle);
        Ok(())
    }
}
