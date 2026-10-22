//! Driver-free desktop devices. Pixels remain native video samples; this layer
//! owns neither encoding nor transport. Every effect rechecks local consent.

#[cfg(target_os = "windows")]
#[path = "desktop/windows.rs"]
mod platform;
#[cfg(not(target_os = "windows"))]
#[path = "desktop/unsupported.rs"]
mod platform;

use crate::NativeVideoSample;
use serde::{Deserialize, Serialize};
use sinomo_core::node::LocalNodePermissionStore;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub const MAX_INPUT_EVENTS: usize = 256;
pub const MAX_FRAME_BYTES: usize = 128 * 1024 * 1024;
pub const MAX_TARGETS: usize = 1024;

#[derive(Clone, Debug, thiserror::Error, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "code", content = "detail", rename_all = "snake_case")]
pub enum DesktopError {
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("target unavailable: {0}")]
    TargetUnavailable(String),
    #[error("platform operation failed: {0}")]
    Platform(String),
    #[error("{applied} input events applied before failure: {detail}; do not replay the batch")]
    PartialInput { applied: usize, detail: String },
    #[error("capture closed: {0}")]
    Closed(String),
}
pub type DesktopResult<T> = Result<T, DesktopError>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DesktopTarget {
    Monitor {
        device_name: String,
    },
    Window {
        handle: u64,
        process_id: u32,
        thread_id: u32,
        process_started: u64,
    },
}
impl DesktopTarget {
    pub fn resource_id(&self) -> String {
        match self {
            Self::Monitor { device_name } => format!("display:{device_name}"),
            Self::Window {
                handle,
                process_id,
                process_started,
                ..
            } => format!("window:{process_id}:{process_started}:{handle}"),
        }
    }
    pub fn is_window(&self) -> bool {
        matches!(self, Self::Window { .. })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DesktopTargetInfo {
    pub target: DesktopTarget,
    pub resource_id: String,
    pub title: String,
    /// Physical screen coordinates; app mouse input uses client coordinates.
    pub bounds: [i32; 4],
    /// Physical screen rectangle of the client area, for mapping captured
    /// whole-window pixels to app input coordinates. Absent for monitors.
    pub client_bounds: Option<[i32; 4]>,
    pub minimized: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DesktopCapabilities {
    pub platform: String,
    pub capture: bool,
    pub keyboard_mouse: bool,
    pub background_window_input: bool,
    pub gamepad: String,
    pub limitations: Vec<String>,
}
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

pub fn authorize_host(store: &LocalNodePermissionStore) -> DesktopResult<()> {
    let permission = store
        .load()
        .map_err(|e| DesktopError::PermissionDenied(e.to_string()))?;
    if !permission.remote_management_enabled() {
        return Err(DesktopError::PermissionDenied(
            "node-local remote-management consent is required".into(),
        ));
    }
    Ok(())
}
pub fn authorize(
    store: &LocalNodePermissionStore,
    target: &DesktopTarget,
    operation: &str,
) -> DesktopResult<()> {
    let permission = store
        .load()
        .map_err(|e| DesktopError::PermissionDenied(e.to_string()))?;
    if !permission.remote_management_enabled()
        || !permission.resource_operation_exposed(&target.resource_id(), operation)
    {
        return Err(DesktopError::PermissionDenied(format!(
            "remote-management and {} / {operation} exposure are required",
            target.resource_id()
        )));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum MouseButton {
    Left,
    Middle,
    Right,
    X1,
    X2,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DesktopInput {
    /// Set-1 scan code for computer use; virtual-key code for app use.
    Key {
        code: u16,
        down: bool,
        #[serde(default)]
        extended: bool,
    },
    Text {
        text: String,
    },
    /// Physical virtual-desktop pixels for computer use; client pixels for app use.
    Move {
        x: i32,
        y: i32,
    },
    RelativeMove {
        dx: i32,
        dy: i32,
    },
    Button {
        button: MouseButton,
        down: bool,
    },
    Wheel {
        delta: i16,
        #[serde(default)]
        horizontal: bool,
    },
    /// Complete state; axes [-1,1], triggers [0,1], Windows GamepadButtons mask.
    Gamepad {
        buttons: u32,
        left_x: f64,
        left_y: f64,
        right_x: f64,
        right_y: f64,
        left_trigger: f64,
        right_trigger: f64,
    },
    ReleaseAll,
}
pub fn validate_inputs(target: &DesktopTarget, events: &[DesktopInput]) -> DesktopResult<()> {
    if events.is_empty() || events.len() > MAX_INPUT_EVENTS {
        return Err(DesktopError::Invalid(
            "input batch must contain 1..256 events".into(),
        ));
    }
    for event in events {
        match event {
            DesktopInput::Key { code, .. } if *code == 0 || *code > 255 => {
                return Err(DesktopError::Invalid("key code must be 1..255".into()));
            }
            DesktopInput::Text { text }
                if text.is_empty() || text.encode_utf16().count() > 4096 =>
            {
                return Err(DesktopError::Invalid(
                    "text must contain 1..4096 UTF-16 units".into(),
                ));
            }
            DesktopInput::Move { x, y }
                if target.is_window()
                    && (i16::try_from(*x).is_err() || i16::try_from(*y).is_err()) =>
            {
                return Err(DesktopError::Invalid(
                    "app client coordinates must fit signed 16 bits".into(),
                ));
            }
            DesktopInput::RelativeMove { .. } | DesktopInput::Gamepad { .. }
                if target.is_window() =>
            {
                return Err(DesktopError::Unsupported(
                    "app use supports window messages, not relative hardware motion or gamepads"
                        .into(),
                ));
            }
            DesktopInput::Gamepad {
                buttons,
                left_x,
                left_y,
                right_x,
                right_y,
                left_trigger,
                right_trigger,
            } if *buttons & !0x3fff != 0
                || [left_x, left_y, right_x, right_y]
                    .into_iter()
                    .any(|v| !(-1.0..=1.0).contains(v))
                || [left_trigger, right_trigger]
                    .into_iter()
                    .any(|v| !(0.0..=1.0).contains(v)) =>
            {
                return Err(DesktopError::Invalid(
                    "invalid gamepad button mask or axes".into(),
                ));
            }
            _ => {}
        }
    }
    Ok(())
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
    pub fn open(
        target: DesktopTarget,
        permissions: LocalNodePermissionStore,
    ) -> DesktopResult<Self> {
        Self::open_with_lease(
            target,
            permissions,
            Duration::from_secs(300),
            Arc::new(AtomicBool::new(false)),
        )
    }
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
    pub fn release_all(&mut self) {
        let _ = self.send(&[DesktopInput::ReleaseAll]);
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DesktopCaptureConfig {
    pub target: DesktopTarget,
    #[serde(default = "default_fps")]
    pub frames_per_second: u32,
    #[serde(default = "default_cursor")]
    pub include_cursor: bool,
}
fn default_fps() -> u32 {
    30
}
fn default_cursor() -> bool {
    true
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn input_validation_rejects_partial_invalid_batches_and_hardware_app_events() {
        let target = DesktopTarget::Window {
            handle: 1,
            process_id: 1,
            thread_id: 1,
            process_started: 1,
        };
        assert!(
            validate_inputs(
                &target,
                &[
                    DesktopInput::Text { text: "ok".into() },
                    DesktopInput::Key {
                        code: 0,
                        down: true,
                        extended: false
                    }
                ]
            )
            .is_err()
        );
        assert!(validate_inputs(&target, &[DesktopInput::RelativeMove { dx: 1, dy: 1 }]).is_err());
        assert!(validate_inputs(&target, &[DesktopInput::Move { x: 32768, y: 0 }]).is_err());
    }
    #[test]
    fn mailbox_keeps_only_the_latest_frame() {
        let mailbox = CaptureMailbox::default();
        for sequence in 1..100 {
            mailbox.publish(NativeVideoSample {
                sequence,
                format: crate::NativeVideoSampleFormat::Gray8,
                width: 1,
                height: 1,
                timestamp_ns: sequence,
                duration_ns: 1,
                captured_at_unix_ms: 1,
                data: Arc::from([1]),
                planes: Arc::from([crate::NativeVideoPlane {
                    offset: 0,
                    len: 1,
                    width: 1,
                    height: 1,
                    stride: 1,
                }]),
            });
        }
        assert_eq!(
            mailbox
                .state
                .lock()
                .unwrap()
                .sample
                .as_ref()
                .unwrap()
                .sequence,
            99
        );
        mailbox.close(None);
        assert!(mailbox.state.lock().unwrap().sample.is_none());
    }
}
