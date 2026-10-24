//! Private OS worker ownership. No media encoding or network transport.
#[cfg(target_os = "windows")]
#[path = "windows.rs"]
mod platform;
#[cfg(not(target_os = "windows"))]
#[path = "unsupported.rs"]
mod platform;
use sinomo_core::node::LocalNodePermissionStore;
use sinomo_device::NativeVideoSample;
use sinomo_device::desktop::*;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
pub fn capabilities() -> DesktopCapabilities {
    platform::capabilities()
}
pub fn enumerate_targets(
    permissions: &LocalNodePermissionStore,
    windows: bool,
) -> DesktopResult<Vec<DesktopTargetInfo>> {
    authorize_host(permissions)?;
    platform::enumerate_targets(windows)
}
pub fn enumerate_child_windows(
    permissions: &LocalNodePermissionStore,
    parent: &DesktopTarget,
) -> DesktopResult<Vec<DesktopTargetInfo>> {
    authorize_host(permissions)?;
    platform::validate_target(parent)?;
    platform::enumerate_child_windows(parent)
}
/// A held-input lease. Dropping it releases only inputs injected by this owner.
/// Cleanup is allowed after revocation so keys cannot remain stuck.
struct InputState {
    target: DesktopTarget,
    permissions: LocalNodePermissionStore,
    input: platform::Input,
    keys: BTreeSet<(u16, bool)>,
    buttons: BTreeSet<MouseButton>,
    cancelled: Arc<AtomicBool>,
    deadline: Instant,
}
impl InputState {
    fn open(
        target: DesktopTarget,
        permissions: LocalNodePermissionStore,
        cancelled: Arc<AtomicBool>,
        deadline: Instant,
    ) -> DesktopResult<Self> {
        authorize(&permissions, &target, "input")?;
        platform::validate_target(&target)?;
        Ok(Self {
            input: platform::Input::new(&target)?,
            target,
            permissions,
            keys: BTreeSet::new(),
            buttons: BTreeSet::new(),
            cancelled,
            deadline,
        })
    }
    pub fn send(&mut self, events: &[DesktopInput]) -> DesktopResult<usize> {
        validate_inputs(&self.target, events)?;
        for (index, event) in events.iter().enumerate() {
            if matches!(event, DesktopInput::ReleaseAll) {
                self.release_all()?;
                continue;
            }
            let active =
                if self.cancelled.load(Ordering::Acquire) || Instant::now() >= self.deadline {
                    Err(DesktopError::Closed("input lease ended".into()))
                } else {
                    Ok(())
                };
            if let Err(error) = active
                .and_then(|_| authorize(&self.permissions, &self.target, "input"))
                .and_then(|_| platform::validate_target(&self.target))
                .and_then(|_| self.input.send(event))
            {
                let _ = self.release_all();
                return Err(if index == 0 {
                    error
                } else {
                    DesktopError::PartialInput {
                        applied: index,
                        detail: error.to_string(),
                    }
                });
            }
            match event {
                DesktopInput::Key {
                    code,
                    extended,
                    down,
                } => {
                    if *down {
                        self.keys.insert((*code, *extended));
                    } else {
                        self.keys.remove(&(*code, *extended));
                    }
                }
                DesktopInput::Button { button, down } => {
                    if *down {
                        self.buttons.insert(*button);
                    } else {
                        self.buttons.remove(button);
                    }
                }
                _ => {}
            }
        }
        Ok(events.len())
    }
    pub fn release_all(&mut self) -> DesktopResult<()> {
        let mut failure = None;
        // Monitor removal must not strand global held keys. A destroyed HWND
        // has no global input state and must never receive cleanup via a new owner.
        if !self.target.is_window() || platform::validate_target(&self.target).is_ok() {
            for (code, extended) in self.keys.clone() {
                match self.input.send(&DesktopInput::Key {
                    code,
                    extended,
                    down: false,
                }) {
                    Ok(()) => {
                        self.keys.remove(&(code, extended));
                    }
                    Err(error) => {
                        failure.get_or_insert(error);
                    }
                }
            }
            for button in self.buttons.clone() {
                match self.input.send(&DesktopInput::Button {
                    button,
                    down: false,
                }) {
                    Ok(()) => {
                        self.buttons.remove(&button);
                    }
                    Err(error) => {
                        failure.get_or_insert(error);
                    }
                }
            }
        }
        self.input.release_gamepad();
        failure.map_or(Ok(()), Err)
    }
}
impl Drop for InputState {
    fn drop(&mut self) {
        let _ = self.release_all();
    }
}

enum InputCommand {
    Send(Vec<DesktopInput>, mpsc::SyncSender<DesktopResult<usize>>),
    Stop,
}
/// Input is executed on an owning OS thread, including WinRT gamepad lifetime.
/// Idle leases expire after 30 seconds; callers renew by sending input.
pub struct DesktopInputSession {
    commands: mpsc::Sender<InputCommand>,
    worker: Option<JoinHandle<()>>,
}
impl DesktopInputSession {
    pub fn open_with_lease(
        target: DesktopTarget,
        permissions: LocalNodePermissionStore,
        duration: Duration,
        cancelled: Arc<AtomicBool>,
    ) -> DesktopResult<Self> {
        authorize(&permissions, &target, "input")?;
        let deadline = Instant::now() + duration.min(Duration::from_secs(300));
        let (commands, receiver) = mpsc::channel();
        let (ready, started) = mpsc::sync_channel(1);
        let worker = std::thread::Builder::new()
            .name("desktop-input".into())
            .spawn(move || {
                let _thread = match platform::initialize_input_thread() {
                    Ok(thread) => thread,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                let mut state = match InputState::open(target, permissions, cancelled, deadline) {
                    Ok(state) => state,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                let _ = ready.send(Ok(()));
                let mut last_input = Instant::now();
                loop {
                    match receiver.recv_timeout(Duration::from_millis(100)) {
                        Ok(InputCommand::Send(events, reply)) => {
                            let result = state.send(&events);
                            last_input = Instant::now();
                            let _ = reply.send(result);
                        }
                        Ok(InputCommand::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    if last_input.elapsed() >= Duration::from_secs(30)
                        || Instant::now() >= state.deadline
                        || state.cancelled.load(Ordering::Acquire)
                        || authorize(&state.permissions, &state.target, "input").is_err()
                    {
                        break;
                    }
                }
            })
            .map_err(|e| DesktopError::Platform(e.to_string()))?;
        let session = Self {
            commands,
            worker: Some(worker),
        };
        started
            .recv_timeout(Duration::from_secs(10))
            .map_err(|e| DesktopError::Platform(format!("input startup: {e}")))??;
        Ok(session)
    }
    pub fn send(&mut self, events: &[DesktopInput]) -> DesktopResult<usize> {
        let (reply, response) = mpsc::sync_channel(1);
        self.commands
            .send(InputCommand::Send(events.to_vec(), reply))
            .map_err(|_| DesktopError::Closed("input lease ended".into()))?;
        response
            .recv()
            .map_err(|_| DesktopError::Closed("input worker ended".into()))?
    }
    pub fn stop(&mut self) {
        let _ = self.commands.send(InputCommand::Stop);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
impl Drop for DesktopInputSession {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Default)]
struct CaptureState {
    sample: Option<NativeVideoSample>,
    error: Option<DesktopError>,
    closed: bool,
}
#[derive(Default)]
pub(super) struct CaptureMailbox {
    state: Mutex<CaptureState>,
    changed: Condvar,
}
impl CaptureMailbox {
    pub(super) fn publish(&self, sample: NativeVideoSample) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).sample = Some(sample);
        self.changed.notify_all();
    }
    fn close(&self, error: Option<DesktopError>) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.sample = None;
        state.error = error;
        state.closed = true;
        self.changed.notify_all();
    }
}

/// Continuous compositor capture with a single latest-frame slot. Consumers
/// select their own cadence. No encoding, polling screenshots, or unbounded queue.
pub struct DesktopCapture {
    target: DesktopTarget,
    permissions: LocalNodePermissionStore,
    mailbox: Arc<CaptureMailbox>,
    stop: Option<mpsc::Sender<()>>,
    worker: Option<JoinHandle<()>>,
}
impl DesktopCapture {
    pub fn start(
        config: DesktopCaptureConfig,
        permissions: LocalNodePermissionStore,
    ) -> DesktopResult<Self> {
        if !(1..=120).contains(&config.frames_per_second) {
            return Err(DesktopError::Invalid("capture fps must be 1..120".into()));
        }
        authorize(&permissions, &config.target, "capture")?;
        platform::validate_target(&config.target)?;
        let mailbox = Arc::new(CaptureMailbox::default());
        let output = mailbox.clone();
        let authority = permissions.clone();
        let target = config.target.clone();
        let (stop, stopping) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let worker = std::thread::Builder::new()
            .name("desktop-capture".into())
            .spawn(move || {
                let result = platform::capture(config, authority, &output, stopping, ready_tx);
                output.close(result.err());
            })
            .map_err(|e| DesktopError::Platform(e.to_string()))?;
        let mut capture = Self {
            target,
            permissions,
            mailbox,
            stop: Some(stop),
            worker: Some(worker),
        };
        match ready_rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Ok(())) => Ok(capture),
            Ok(Err(error)) => {
                capture.stop();
                Err(error)
            }
            Err(error) => {
                capture.stop();
                Err(DesktopError::Platform(format!("capture startup: {error}")))
            }
        }
    }
    pub fn next(
        &self,
        after_sequence: u64,
        timeout: Duration,
    ) -> DesktopResult<Option<NativeVideoSample>> {
        authorize(&self.permissions, &self.target, "capture")?;
        let state = self.mailbox.state.lock().unwrap_or_else(|e| e.into_inner());
        let (state, _) = self
            .mailbox
            .changed
            .wait_timeout_while(state, timeout.min(Duration::from_secs(5)), |s| {
                !s.closed
                    && s.sample
                        .as_ref()
                        .is_none_or(|sample| sample.sequence <= after_sequence)
            })
            .unwrap_or_else(|e| e.into_inner());
        // Do not return a cached frame across permission revocation.
        authorize(&self.permissions, &self.target, "capture")?;
        if let Some(error) = &state.error {
            return Err(error.clone());
        }
        if state.closed {
            return Err(DesktopError::Closed("capture stopped".into()));
        }
        Ok(state
            .sample
            .as_ref()
            .filter(|s| s.sequence > after_sequence)
            .cloned())
    }
    pub fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
impl Drop for DesktopCapture {
    fn drop(&mut self) {
        self.stop();
    }
}
