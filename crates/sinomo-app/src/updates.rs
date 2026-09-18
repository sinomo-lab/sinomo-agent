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
    time::Duration,
};

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
    checkpoint: Option<Receiver<Result<(), String>>>,
    acknowledged: bool,
    restart_intent: Option<String>,
    retry_restart_after: Option<std::time::Instant>,
    pub rendered_ready: AtomicBool,
    ready_since: Option<std::time::Instant>,
    retry_ack_after: Option<std::time::Instant>,
    ack: Option<Receiver<Result<AppUpdateStatus, String>>>,
    cancel: Arc<AtomicBool>,
}

impl Drop for AppUpdates {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Release);
    }
}

impl AppUpdates {
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
    let mut app = shared.borrow_mut();
    if app.updates.view.busy {
        return;
    }
    if when_idle {
        app.updates.view.when_idle = true;
        return;
    }
    let chat = &app.ready.as_ref().expect("ready").chat;
    if !chat.handle.is_connected() || !chat.can_checkpoint_workspace() {
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
    if app.ready.as_ref().is_some_and(|r| r.chat.media_call.active) {
        app.updates.view.error = Some("Finish the voice or video call before restarting.".into());
        return;
    }
    app.updates.view.busy = true;
    app.updates.restart_intent = app.updates.view.status.candidate_release.clone();
    app.updates.view.error = None;
    app.updates.checkpoint = Some(
        app.ready
            .as_ref()
            .expect("ready")
            .chat
            .handle
            .prepare_restart(),
    );
}

pub(crate) fn pump(shared: &Shared) -> bool {
    if cfg!(target_os = "android") {
        return false;
    }
    if !shared.is_ready() {
        return false;
    }
    let (address, waker, connected, idle, restore_error) = {
        let app = shared.borrow();
        let ready = app.ready.as_ref().expect("ready");
        (
            ready.backend.connection.daemon_address().map(str::to_owned),
            app.waker.clone(),
            ready.chat.handle.is_connected() && ready.chat.restoration_complete(),
            !ready.chat.streaming
                && !ready.chat.control.active_turn
                && !ready.chat.media_call.active,
            ready.chat.restoration_error.clone(),
        )
    };
    let was_busy = shared.borrow().updates.view.busy;
    let mut checkpoint_ready = false;
    let mut exit = false;
    let mut install_idle = false;
    {
        let mut app = shared.borrow_mut();
        let updates = &mut app.updates;
        updates.connect(address.clone(), waker.clone());
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
                .is_some_and(|ready| ready.elapsed() >= Duration::from_secs(2))
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
                Ok(_) => updates.acknowledged = true,
                Err(error) => {
                    updates.view.error = Some(error);
                    updates.retry_ack_after =
                        Some(std::time::Instant::now() + Duration::from_secs(5));
                }
            }
        }
        if let Some(result) = updates
            .checkpoint
            .as_ref()
            .and_then(|rx| rx.try_recv().ok())
        {
            updates.checkpoint = None;
            match result {
                Ok(()) => checkpoint_ready = true,
                Err(error) => {
                    updates.view.error = Some(error);
                    updates.view.busy = false;
                }
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
        shared.chat_mut().reconnect_after_update_failure();
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
