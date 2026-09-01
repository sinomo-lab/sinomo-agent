//! Node-local audio/video setup around realtime calls.
//!
//! Device inventory, permission probes, and test streams stay in Sinomo. SUI
//! only renders the resulting snapshots.

use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(test))]
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
#[cfg(not(test))]
use std::thread;
#[cfg(not(test))]
use std::time::{Duration, Instant};

use sinomo_runtime::device::{
    DeviceKind, NativeDeviceDescriptor, NativeDeviceFailureKind, NativeDeviceInventory,
};
#[cfg(not(test))]
use sinomo_runtime::device::{enumerate_native_devices, enumerate_native_devices_without_camera};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::node::NodeId;

use crate::backend::app_config::{
    AppMediaPreferences, RememberedNativeDevice, load_media_preferences, save_media_preferences,
};

#[cfg(not(test))]
const SETUP_AUDIO_SAMPLE_RATE_HZ: u32 = 24_000;
const SETUP_CAMERA_FRAME_INTERVAL_MS: u64 = sinomo_video::NATIVE_VIDEO_PREVIEW_FRAME_INTERVAL_MS;
const MAX_CONSECUTIVE_CAMERA_FRAME_FAILURES: u8 = 3;
#[cfg(not(test))]
const SPEAKER_TEST_DURATION: Duration = Duration::from_millis(650);
#[cfg(not(test))]
const SETUP_DEVICE_OPEN_TIMEOUT: Duration = Duration::from_secs(65);

pub(crate) fn camera_frame_failure_is_terminal(failures: &mut u8) -> bool {
    *failures = failures.saturating_add(1);
    *failures >= MAX_CONSECUTIVE_CAMERA_FRAME_FAILURES
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MediaDeviceOption {
    pub native_id: Option<String>,
    pub display_name: String,
    pub is_default: bool,
}

impl MediaDeviceOption {
    fn system_default() -> Self {
        Self {
            native_id: None,
            display_name: "System default".to_string(),
            is_default: true,
        }
    }

    pub fn label(&self) -> String {
        if self.native_id.is_some() && self.is_default {
            format!("{} (default)", self.display_name)
        } else {
            self.display_name.clone()
        }
    }

    fn remembered(&self) -> Option<RememberedNativeDevice> {
        self.native_id.as_ref().map(|native_id| {
            RememberedNativeDevice::new(native_id.clone(), self.display_name.clone())
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MediaAccessKind {
    Microphone,
    Speaker,
    Camera,
}

impl MediaAccessKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Microphone => "Microphone",
            Self::Speaker => "Speaker",
            Self::Camera => "Camera",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MediaAccessState {
    Idle,
    Checking,
    Ready,
    PermissionDenied(String),
    Busy(String),
    Unavailable(String),
    Unsupported(String),
}

impl MediaAccessState {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Idle => "Not checked",
            Self::Checking => "Checking",
            Self::Ready => "Ready",
            Self::PermissionDenied(_) => "Permission needed",
            Self::Busy(_) => "In use",
            Self::Unavailable(_) => "Unavailable",
            Self::Unsupported(_) => "Unsupported",
        }
    }

    pub fn detail(&self) -> Option<&str> {
        match self {
            Self::PermissionDenied(detail)
            | Self::Busy(detail)
            | Self::Unavailable(detail)
            | Self::Unsupported(detail) => Some(detail),
            Self::Idle | Self::Checking | Self::Ready => None,
        }
    }

    pub fn needs_recovery(&self) -> bool {
        matches!(
            self,
            Self::PermissionDenied(_) | Self::Busy(_) | Self::Unavailable(_)
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpeakerTestState {
    Idle,
    Testing,
    Passed,
    Failed(String),
}

impl SpeakerTestState {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Idle => "Test speaker",
            Self::Testing => "Playing…",
            Self::Passed => "Played",
            Self::Failed(_) => "Try again",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MediaDeviceSelection {
    pub microphone: Option<String>,
    pub speaker: Option<String>,
    pub camera: Option<String>,
}

#[cfg_attr(test, allow(dead_code))]
struct MediaSetupRuntime {
    control_tx: Sender<MediaSetupControl>,
    event_rx: Receiver<MediaSetupEvent>,
    snapshots: Arc<Mutex<MediaSetupSnapshots>>,
    cancelled: Arc<AtomicBool>,
    microphone_enabled: Arc<AtomicBool>,
    camera_enabled: Arc<AtomicBool>,
    #[cfg(not(test))]
    worker: Option<thread::JoinHandle<()>>,
}

impl Drop for MediaSetupRuntime {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        let _ = self.control_tx.send(MediaSetupControl::Stop);
    }
}

pub(crate) struct MediaSetupShutdown {
    #[cfg(not(test))]
    worker: Option<thread::JoinHandle<()>>,
}

impl MediaSetupShutdown {
    #[cfg(not(test))]
    pub(crate) fn wait(mut self) {
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[derive(Default)]
struct MediaSetupSnapshots {
    microphone_level: Option<f32>,
    camera_surface_revision: Option<u64>,
}

/// Classifies the invalidation needed after draining media worker state.
///
/// Camera previews and microphone levels are paint-only. Inventory, access,
/// and lifecycle events rebuild controls or their semantics and therefore
/// require a full layout pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MediaPumpChange {
    pub(crate) paint: bool,
    pub(crate) semantics: bool,
    pub(crate) layout: bool,
}

impl MediaPumpChange {
    pub(crate) fn mark_paint(&mut self) {
        self.paint = true;
    }

    pub(crate) fn mark_semantics(&mut self) {
        self.paint = true;
        self.semantics = true;
    }

    pub(crate) fn mark_layout(&mut self) {
        self.paint = true;
        self.semantics = true;
        self.layout = true;
    }

    pub(crate) fn changed(self) -> bool {
        self.paint || self.semantics || self.layout
    }
}

impl MediaSetupSnapshots {
    fn take(&mut self) -> Self {
        Self {
            microphone_level: self.microphone_level.take(),
            camera_surface_revision: self.camera_surface_revision.take(),
        }
    }

    fn is_empty(&self) -> bool {
        self.microphone_level.is_none() && self.camera_surface_revision.is_none()
    }
}

#[cfg_attr(test, allow(dead_code))]
enum MediaSetupControl {
    Stop,
    TestSpeaker,
    SetMicrophoneEnabled(bool),
    SetCameraEnabled(bool),
}

#[cfg_attr(test, allow(dead_code))]
enum MediaSetupEvent {
    Inventory(NativeDeviceInventory),
    Access(MediaAccessKind, MediaAccessState),
    SpeakerTest(SpeakerTestState),
}

pub struct MediaSetupState {
    data_dir: Arc<SinomoDataDir>,
    node_id: NodeId,
    #[cfg_attr(test, allow(dead_code))]
    waker: Option<sui::UiHandle>,
    preferences: AppMediaPreferences,
    pub microphones: Vec<MediaDeviceOption>,
    pub speakers: Vec<MediaDeviceOption>,
    pub cameras: Vec<MediaDeviceOption>,
    pub microphone_index: usize,
    pub speaker_index: usize,
    pub camera_index: usize,
    pub microphone_access: MediaAccessState,
    pub speaker_access: MediaAccessState,
    pub camera_access: MediaAccessState,
    pub microphone_level: f32,
    pub(crate) camera_surface: Arc<sinomo_video::NativeVideoSurface>,
    pub speaker_test: SpeakerTestState,
    pub inventory_loading: bool,
    pub inventory_issues: Vec<String>,
    pub preference_error: Option<String>,
    microphone_enabled: bool,
    camera_enabled: bool,
    running: bool,
    runtime: Option<MediaSetupRuntime>,
}

impl MediaSetupState {
    pub fn new(
        data_dir: Arc<SinomoDataDir>,
        node_id: NodeId,
        waker: Option<sui::UiHandle>,
    ) -> Self {
        let preferences = load_media_preferences(&data_dir);
        Self {
            data_dir,
            node_id,
            waker,
            preferences,
            microphones: vec![MediaDeviceOption::system_default()],
            speakers: vec![MediaDeviceOption::system_default()],
            cameras: vec![MediaDeviceOption::system_default()],
            microphone_index: 0,
            speaker_index: 0,
            camera_index: 0,
            microphone_access: MediaAccessState::Idle,
            speaker_access: MediaAccessState::Idle,
            camera_access: MediaAccessState::Idle,
            microphone_level: 0.0,
            camera_surface: Arc::new(sinomo_video::NativeVideoSurface::new()),
            speaker_test: SpeakerTestState::Idle,
            inventory_loading: false,
            inventory_issues: Vec::new(),
            preference_error: None,
            microphone_enabled: true,
            // Inventory may discover cameras, but capture starts only after an explicit UI
            // toggle. This also leaves room for screen sharing as a peer video source.
            camera_enabled: false,
            running: false,
            runtime: None,
        }
    }

    pub fn is_running(&self) -> bool {
        self.running
    }

    pub fn start(&mut self) {
        #[cfg_attr(test, allow(unused_variables))]
        let previous_shutdown = self.begin_shutdown();
        self.running = true;
        self.inventory_loading = true;
        self.inventory_issues.clear();
        self.microphone_access = if self.microphone_enabled {
            MediaAccessState::Checking
        } else {
            MediaAccessState::Idle
        };
        self.speaker_access = MediaAccessState::Checking;
        self.camera_access = if self.camera_enabled {
            MediaAccessState::Checking
        } else {
            MediaAccessState::Idle
        };
        self.microphone_level = 0.0;
        self.camera_surface.clear();
        self.speaker_test = SpeakerTestState::Idle;

        #[cfg(test)]
        {
            self.inventory_loading = false;
            self.microphone_access = if self.microphone_enabled {
                MediaAccessState::Ready
            } else {
                MediaAccessState::Idle
            };
            self.speaker_access = MediaAccessState::Ready;
            self.camera_access = if self.camera_enabled {
                MediaAccessState::Ready
            } else {
                MediaAccessState::Idle
            };
        }
        #[cfg(not(test))]
        {
            let (event_tx, event_rx) = mpsc::channel();
            let (control_tx, control_rx) = mpsc::channel();
            let snapshots = Arc::new(Mutex::new(MediaSetupSnapshots::default()));
            let worker_snapshots = snapshots.clone();
            let cancelled = Arc::new(AtomicBool::new(false));
            let worker_cancelled = cancelled.clone();
            let node_id = self.node_id.clone();
            let preferences = self.preferences.clone();
            let microphone_enabled = self.microphone_enabled;
            let camera_enabled = self.camera_enabled;
            let microphone_enabled_signal = Arc::new(AtomicBool::new(microphone_enabled));
            let worker_microphone_enabled_signal = microphone_enabled_signal.clone();
            let camera_enabled_signal = Arc::new(AtomicBool::new(camera_enabled));
            let worker_camera_enabled_signal = camera_enabled_signal.clone();
            let camera_surface = self.camera_surface.clone();
            let waker = self.waker.clone();
            match thread::Builder::new()
                .name("sinomo-app-media-setup".to_string())
                .spawn(move || {
                    if let Some(shutdown) = previous_shutdown {
                        shutdown.wait();
                    }
                    run_media_setup_worker(
                        node_id,
                        preferences,
                        microphone_enabled,
                        camera_enabled,
                        event_tx,
                        control_rx,
                        worker_snapshots,
                        worker_cancelled,
                        worker_microphone_enabled_signal,
                        worker_camera_enabled_signal,
                        camera_surface,
                        waker,
                    )
                }) {
                Ok(worker) => {
                    self.runtime = Some(MediaSetupRuntime {
                        control_tx,
                        event_rx,
                        snapshots,
                        cancelled,
                        microphone_enabled: microphone_enabled_signal,
                        camera_enabled: camera_enabled_signal,
                        worker: Some(worker),
                    });
                }
                Err(error) => {
                    self.running = false;
                    self.inventory_loading = false;
                    let detail = format!("media setup worker failed to start: {error}");
                    self.microphone_access = MediaAccessState::Unavailable(detail.clone());
                    self.speaker_access = MediaAccessState::Unavailable(detail.clone());
                    self.camera_access = MediaAccessState::Unavailable(detail);
                }
            }
        }
    }

    pub fn stop(&mut self) {
        let _ = self.begin_shutdown();
        self.inventory_loading = false;
        self.microphone_level = 0.0;
        self.camera_surface.clear();
        self.speaker_test = SpeakerTestState::Idle;
    }

    pub(crate) fn begin_shutdown(&mut self) -> Option<MediaSetupShutdown> {
        self.running = false;
        #[cfg_attr(test, allow(unused_mut))]
        let mut runtime = self.runtime.take()?;
        runtime.cancelled.store(true, Ordering::Release);
        let _ = runtime.control_tx.send(MediaSetupControl::Stop);
        Some(MediaSetupShutdown {
            #[cfg(not(test))]
            worker: runtime.worker.take(),
        })
    }

    pub fn retry_access(&mut self) {
        self.start();
    }

    pub fn set_microphone_enabled(&mut self, enabled: bool) {
        self.microphone_enabled = enabled;
        if !enabled {
            self.microphone_level = 0.0;
            self.microphone_access = MediaAccessState::Idle;
        } else if self.running {
            self.microphone_access = MediaAccessState::Checking;
        }

        if let Some(runtime) = &self.runtime {
            runtime.microphone_enabled.store(enabled, Ordering::Release);
            if runtime
                .control_tx
                .send(MediaSetupControl::SetMicrophoneEnabled(enabled))
                .is_err()
            {
                self.microphone_access = MediaAccessState::Unavailable(
                    "microphone setup worker stopped; retry access".to_string(),
                );
            }
        }

        #[cfg(test)]
        if enabled && self.running {
            self.microphone_access = MediaAccessState::Ready;
        }
    }

    pub fn set_camera_enabled(&mut self, enabled: bool) {
        self.camera_enabled = enabled;
        if !enabled {
            self.camera_surface.clear();
            self.camera_access = MediaAccessState::Idle;
        } else if self.running {
            self.camera_access = MediaAccessState::Checking;
        }

        if let Some(runtime) = &self.runtime {
            runtime.camera_enabled.store(enabled, Ordering::Release);
            if runtime
                .control_tx
                .send(MediaSetupControl::SetCameraEnabled(enabled))
                .is_err()
            {
                self.camera_access = MediaAccessState::Unavailable(
                    "camera setup worker stopped; retry access".to_string(),
                );
            }
        }

        #[cfg(test)]
        if enabled && self.running {
            self.camera_access = MediaAccessState::Ready;
        }
    }

    #[cfg(test)]
    pub(crate) fn microphone_enabled(&self) -> bool {
        self.microphone_enabled
    }

    #[cfg(test)]
    pub(crate) fn camera_enabled(&self) -> bool {
        self.camera_enabled
    }

    pub fn test_speaker(&mut self) {
        if self.runtime.is_none() {
            self.start();
        }
        if let Some(runtime) = &self.runtime {
            self.speaker_test = SpeakerTestState::Testing;
            if runtime
                .control_tx
                .send(MediaSetupControl::TestSpeaker)
                .is_err()
            {
                let detail = "speaker test worker stopped".to_string();
                self.speaker_test = SpeakerTestState::Failed(detail.clone());
                self.speaker_access = MediaAccessState::Unavailable(detail);
            }
        }
    }

    pub fn microphone_labels(&self) -> Vec<String> {
        self.microphones
            .iter()
            .map(MediaDeviceOption::label)
            .collect()
    }

    pub fn speaker_labels(&self) -> Vec<String> {
        self.speakers.iter().map(MediaDeviceOption::label).collect()
    }

    pub fn camera_labels(&self) -> Vec<String> {
        self.cameras.iter().map(MediaDeviceOption::label).collect()
    }

    pub fn selection(&self) -> MediaDeviceSelection {
        MediaDeviceSelection {
            microphone: selected_native_id(&self.microphones, self.microphone_index),
            speaker: selected_native_id(&self.speakers, self.speaker_index),
            camera: selected_native_id(&self.cameras, self.camera_index),
        }
    }

    pub fn select_microphone(&mut self, index: usize) {
        self.microphone_index = index.min(self.microphones.len().saturating_sub(1));
        self.preferences.microphone = self.microphones[self.microphone_index].remembered();
        self.save_and_restart();
    }

    pub fn select_speaker(&mut self, index: usize) {
        self.speaker_index = index.min(self.speakers.len().saturating_sub(1));
        self.preferences.speaker = self.speakers[self.speaker_index].remembered();
        self.save_and_restart();
    }

    pub fn select_camera(&mut self, index: usize) {
        self.camera_index = index.min(self.cameras.len().saturating_sub(1));
        self.preferences.camera = self.cameras[self.camera_index].remembered();
        self.save_and_restart();
    }

    pub fn recovery_detail(&self) -> Option<String> {
        let mut details = [
            (MediaAccessKind::Microphone, &self.microphone_access),
            (MediaAccessKind::Speaker, &self.speaker_access),
            (MediaAccessKind::Camera, &self.camera_access),
        ]
        .into_iter()
        .filter_map(|(kind, state)| {
            state
                .detail()
                .map(|detail| format!("{}: {detail}", kind.label()))
        })
        .collect::<Vec<_>>();
        details.extend(self.inventory_issues.iter().cloned());
        if let Some(error) = &self.preference_error {
            details.push(error.clone());
        }
        if let SpeakerTestState::Failed(error) = &self.speaker_test {
            details.push(format!("Speaker test: {error}"));
        }
        (!details.is_empty()).then(|| details.join(" · "))
    }

    pub fn access_check_complete(&self) -> bool {
        !self.inventory_loading
            && !matches!(self.microphone_access, MediaAccessState::Checking)
            && !matches!(self.speaker_access, MediaAccessState::Checking)
            && !matches!(self.camera_access, MediaAccessState::Checking)
    }

    pub fn has_recoverable_failure(&self) -> bool {
        self.microphone_access.needs_recovery()
            || self.speaker_access.needs_recovery()
            || self.camera_access.needs_recovery()
    }

    pub fn permission_settings_available(&self) -> bool {
        matches!(
            self.microphone_access,
            MediaAccessState::PermissionDenied(_)
        ) || matches!(self.camera_access, MediaAccessState::PermissionDenied(_))
    }

    pub fn open_permission_settings(&mut self) {
        self.preference_error = open_media_privacy_settings().err();
    }

    pub(crate) fn pump(&mut self) -> MediaPumpChange {
        let Some(runtime) = self.runtime.as_mut() else {
            return MediaPumpChange::default();
        };
        let mut events = Vec::new();
        let mut disconnected = false;
        let snapshots = runtime
            .snapshots
            .lock()
            .map(|mut snapshots| snapshots.take())
            .unwrap_or_default();
        loop {
            match runtime.event_rx.try_recv() {
                Ok(event) => events.push(event),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }
        let snapshots_changed = !snapshots.is_empty();
        let structural_changed = !events.is_empty() || disconnected;
        if !snapshots_changed && !structural_changed {
            return MediaPumpChange::default();
        }
        for event in events {
            self.apply_event(event);
        }
        if let Some(level) = snapshots.microphone_level {
            self.microphone_level = level.clamp(0.0, 1.0);
        }
        if disconnected {
            self.running = false;
            self.runtime = None;
            self.microphone_level = 0.0;
            self.camera_surface.clear();
            for state in [
                &mut self.microphone_access,
                &mut self.speaker_access,
                &mut self.camera_access,
            ] {
                if matches!(
                    &*state,
                    MediaAccessState::Ready | MediaAccessState::Checking
                ) {
                    *state = MediaAccessState::Unavailable(
                        "media setup worker stopped; retry access".to_string(),
                    );
                }
            }
            if matches!(self.speaker_test, SpeakerTestState::Testing) {
                self.speaker_test =
                    SpeakerTestState::Failed("speaker test worker stopped".to_string());
            }
        }
        let mut change = MediaPumpChange::default();
        if snapshots_changed {
            change.mark_paint();
        }
        if structural_changed {
            change.mark_layout();
        }
        change
    }

    fn save_and_restart(&mut self) {
        self.preference_error = save_media_preferences(&self.data_dir, &self.preferences).err();
        if self.runtime.is_some() {
            self.start();
        }
    }

    fn apply_event(&mut self, event: MediaSetupEvent) {
        match event {
            MediaSetupEvent::Inventory(inventory) => self.apply_inventory(inventory),
            MediaSetupEvent::Access(kind, state) => match kind {
                MediaAccessKind::Microphone => self.microphone_access = state,
                MediaAccessKind::Speaker => self.speaker_access = state,
                MediaAccessKind::Camera => self.camera_access = state,
            },
            MediaSetupEvent::SpeakerTest(state) => {
                if let SpeakerTestState::Failed(detail) = &state
                    && matches!(self.speaker_access, MediaAccessState::Ready)
                {
                    self.speaker_access = media_access_failure(detail.clone());
                }
                self.speaker_test = state;
            }
        }
    }

    fn apply_inventory(&mut self, inventory: NativeDeviceInventory) {
        self.inventory_loading = false;
        self.inventory_issues = inventory.issues;
        self.microphones = device_options(&inventory.devices, DeviceKind::Microphone);
        self.speakers = device_options(&inventory.devices, DeviceKind::Speaker);
        self.cameras = device_options(&inventory.devices, DeviceKind::Camera);
        self.microphone_index =
            remembered_index(&self.microphones, self.preferences.microphone.as_ref());
        self.speaker_index = remembered_index(&self.speakers, self.preferences.speaker.as_ref());
        self.camera_index = remembered_index(&self.cameras, self.preferences.camera.as_ref());
        append_missing_preference_issue(
            &mut self.inventory_issues,
            "microphone",
            &self.preferences.microphone,
            &self.microphones,
            self.microphone_index,
        );
        append_missing_preference_issue(
            &mut self.inventory_issues,
            "speaker",
            &self.preferences.speaker,
            &self.speakers,
            self.speaker_index,
        );
        if self.camera_enabled {
            append_missing_preference_issue(
                &mut self.inventory_issues,
                "camera",
                &self.preferences.camera,
                &self.cameras,
                self.camera_index,
            );
        }
    }
}

fn selected_native_id(options: &[MediaDeviceOption], index: usize) -> Option<String> {
    options
        .get(index)
        .and_then(|option| option.native_id.clone())
}

fn device_options(
    descriptors: &[NativeDeviceDescriptor],
    kind: DeviceKind,
) -> Vec<MediaDeviceOption> {
    let mut options = descriptors
        .iter()
        .filter(|descriptor| descriptor.profile.kind == kind)
        .map(|descriptor| MediaDeviceOption {
            native_id: Some(descriptor.native_id.clone()),
            display_name: descriptor.profile.display_name.clone(),
            is_default: descriptor.is_default,
        })
        .collect::<Vec<_>>();
    options.sort_by(|left, right| {
        right
            .is_default
            .cmp(&left.is_default)
            .then_with(|| left.display_name.cmp(&right.display_name))
    });
    options.insert(0, MediaDeviceOption::system_default());
    options
}

fn remembered_index(
    options: &[MediaDeviceOption],
    remembered: Option<&RememberedNativeDevice>,
) -> usize {
    let Some(remembered) = remembered else {
        return 0;
    };
    options
        .iter()
        .position(|option| option.native_id.as_deref() == Some(remembered.native_id.as_str()))
        .or_else(|| {
            let mut matches = options.iter().enumerate().filter(|(_, option)| {
                option.native_id.is_some() && option.display_name == remembered.display_name
            });
            let first = matches.next().map(|(index, _)| index)?;
            matches.next().is_none().then_some(first)
        })
        .unwrap_or(0)
}

fn selection_for_inventory(
    inventory: &NativeDeviceInventory,
    preferences: &AppMediaPreferences,
) -> MediaDeviceSelection {
    fn selected_for_kind(
        inventory: &NativeDeviceInventory,
        kind: DeviceKind,
        remembered: Option<&RememberedNativeDevice>,
    ) -> Option<String> {
        let options = device_options(&inventory.devices, kind);
        selected_native_id(&options, remembered_index(&options, remembered))
    }

    MediaDeviceSelection {
        microphone: selected_for_kind(
            inventory,
            DeviceKind::Microphone,
            preferences.microphone.as_ref(),
        ),
        speaker: selected_for_kind(inventory, DeviceKind::Speaker, preferences.speaker.as_ref()),
        camera: selected_for_kind(inventory, DeviceKind::Camera, preferences.camera.as_ref()),
    }
}

fn append_missing_preference_issue(
    issues: &mut Vec<String>,
    kind: &str,
    remembered: &Option<RememberedNativeDevice>,
    options: &[MediaDeviceOption],
    selected_index: usize,
) {
    let Some(remembered) = remembered else {
        return;
    };
    if selected_index == 0
        && !options.iter().any(|option| {
            option.native_id.as_deref() == Some(remembered.native_id.as_str())
                || option.display_name == remembered.display_name
        })
    {
        issues.push(format!(
            "saved {kind} `{}` is unavailable; using the system default",
            remembered.display_name
        ));
    }
}

#[cfg(not(test))]
fn await_media_setup_operation<T, F, W>(
    label: &'static str,
    cancelled: &AtomicBool,
    device_requested: Option<&AtomicBool>,
    open: F,
    mut on_wait: W,
) -> Result<Option<T>, String>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
    W: FnMut(),
{
    on_wait();
    if cancelled.load(Ordering::Acquire) {
        return Ok(None);
    }
    if device_requested.is_some_and(|requested| !requested.load(Ordering::Acquire)) {
        return Ok(None);
    }
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    if let Err(error) = thread::Builder::new()
        .name(format!("sinomo-app-setup-{label}-open"))
        .spawn(move || {
            let _ = result_tx.send(open());
        })
    {
        if cancelled.load(Ordering::Acquire)
            || device_requested.is_some_and(|requested| !requested.load(Ordering::Acquire))
        {
            return Ok(None);
        }
        return Err(format!("{label} setup worker failed to start: {error}"));
    }

    let started = Instant::now();
    loop {
        on_wait();
        if cancelled.load(Ordering::Acquire) {
            return Ok(None);
        }
        if device_requested.is_some_and(|requested| !requested.load(Ordering::Acquire)) {
            return Ok(None);
        }
        match result_rx.recv_timeout(Duration::from_millis(10)) {
            Ok(result) => {
                on_wait();
                if cancelled.load(Ordering::Acquire)
                    || device_requested.is_some_and(|requested| !requested.load(Ordering::Acquire))
                {
                    drop(result);
                    return Ok(None);
                }
                return Ok(Some(result));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(format!("{label} setup worker stopped"));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if started.elapsed() >= SETUP_DEVICE_OPEN_TIMEOUT {
            return Err(format!("{label} setup timed out"));
        }
    }
}

#[cfg(not(test))]
fn release_setup_microphone_if_unrequested(
    requested: &AtomicBool,
    microphone: &mut Option<sinomo_audio::NativeAudioCapture>,
    event_tx: &Sender<MediaSetupEvent>,
    snapshots: &Arc<Mutex<MediaSetupSnapshots>>,
    waker: &Option<sui::UiHandle>,
) {
    if requested.load(Ordering::Acquire) || microphone.take().is_none() {
        return;
    }
    publish_media_setup_snapshot(snapshots, waker, |snapshots| {
        snapshots.microphone_level = Some(0.0);
    });
    let _ = send_media_setup_event(
        event_tx,
        waker,
        MediaSetupEvent::Access(MediaAccessKind::Microphone, MediaAccessState::Idle),
    );
}

#[cfg(not(test))]
fn release_setup_camera_if_unrequested(
    requested: &AtomicBool,
    camera: &mut Option<sinomo_video::NativeVideoCapture>,
    event_tx: &Sender<MediaSetupEvent>,
    snapshots: &Arc<Mutex<MediaSetupSnapshots>>,
    camera_surface: &sinomo_video::NativeVideoSurface,
    waker: &Option<sui::UiHandle>,
) {
    if requested.load(Ordering::Acquire) || camera.take().is_none() {
        return;
    }
    let revision = camera_surface.clear();
    publish_media_setup_snapshot(snapshots, waker, |snapshots| {
        snapshots.camera_surface_revision = Some(revision);
    });
    let _ = send_media_setup_event(
        event_tx,
        waker,
        MediaSetupEvent::Access(MediaAccessKind::Camera, MediaAccessState::Idle),
    );
}

#[cfg(not(test))]
fn open_media_setup_device<T, F, W>(
    kind: MediaAccessKind,
    label: &'static str,
    cancelled: &AtomicBool,
    device_requested: Option<&AtomicBool>,
    event_tx: &Sender<MediaSetupEvent>,
    waker: &Option<sui::UiHandle>,
    open: F,
    on_wait: W,
) -> Option<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
    W: FnMut(),
{
    let _ = send_media_setup_event(
        event_tx,
        waker,
        MediaSetupEvent::Access(kind, MediaAccessState::Checking),
    );
    match await_media_setup_operation(label, cancelled, device_requested, open, on_wait) {
        Ok(None) => {
            if !cancelled.load(Ordering::Acquire) {
                let _ = send_media_setup_event(
                    event_tx,
                    waker,
                    MediaSetupEvent::Access(kind, MediaAccessState::Idle),
                );
            }
            None
        }
        Ok(Some(Ok(device))) => {
            let _ = send_media_setup_event(
                event_tx,
                waker,
                MediaSetupEvent::Access(kind, MediaAccessState::Ready),
            );
            Some(device)
        }
        Ok(Some(Err(detail))) | Err(detail) => {
            let _ = send_media_setup_event(
                event_tx,
                waker,
                MediaSetupEvent::Access(kind, media_access_failure(detail)),
            );
            None
        }
    }
}

#[cfg(not(test))]
fn open_media_setup_microphone(
    config: sinomo_audio::NativeAudioCaptureConfig,
) -> Result<sinomo_audio::NativeAudioCapture, String> {
    sinomo_audio::start_native_microphone_capture(config).map_err(|error| error.to_string())
}

#[cfg(not(test))]
fn open_media_setup_camera(
    config: sinomo_video::NativeVideoCaptureConfig,
) -> Result<sinomo_video::NativeVideoCapture, String> {
    sinomo_video::start_native_camera_capture(config).map_err(|error| error.to_string())
}

#[cfg(not(test))]
fn open_media_setup_speaker(
    config: sinomo_audio::NativeSpeakerPlaybackConfig,
) -> Result<sinomo_audio::NativeSpeakerPlayback, String> {
    sinomo_audio::start_native_speaker_playback(config).map_err(|error| error.to_string())
}

#[cfg(not(test))]
fn run_media_setup_worker(
    node_id: NodeId,
    preferences: AppMediaPreferences,
    mut microphone_enabled: bool,
    mut camera_enabled: bool,
    event_tx: Sender<MediaSetupEvent>,
    control_rx: Receiver<MediaSetupControl>,
    snapshots: Arc<Mutex<MediaSetupSnapshots>>,
    cancelled: Arc<AtomicBool>,
    microphone_requested: Arc<AtomicBool>,
    camera_requested: Arc<AtomicBool>,
    camera_surface: Arc<sinomo_video::NativeVideoSurface>,
    waker: Option<sui::UiHandle>,
) {
    if media_setup_stop_requested(&control_rx, &mut microphone_enabled, &mut camera_enabled) {
        return;
    }
    let inventory_node = node_id.clone();
    let inventory_result = if camera_enabled {
        await_media_setup_operation(
            "device-inventory",
            &cancelled,
            Some(&camera_requested),
            move || enumerate_native_devices(inventory_node),
            || {},
        )
    } else {
        await_media_setup_operation(
            "audio-device-inventory",
            &cancelled,
            None,
            move || enumerate_native_devices_without_camera(inventory_node),
            || {},
        )
    };
    let inventory = match inventory_result {
        Ok(Some(inventory)) => inventory,
        Ok(None) if cancelled.load(Ordering::Acquire) => return,
        Ok(None) => {
            let audio_inventory_node = node_id.clone();
            match await_media_setup_operation(
                "audio-device-inventory",
                &cancelled,
                None,
                move || enumerate_native_devices_without_camera(audio_inventory_node),
                || {},
            ) {
                Ok(Some(inventory)) => inventory,
                Ok(None) => return,
                Err(detail) => {
                    let _ = send_media_setup_event(
                        &event_tx,
                        &waker,
                        MediaSetupEvent::Access(
                            MediaAccessKind::Microphone,
                            MediaAccessState::Unavailable(detail.clone()),
                        ),
                    );
                    let _ = send_media_setup_event(
                        &event_tx,
                        &waker,
                        MediaSetupEvent::Access(
                            MediaAccessKind::Speaker,
                            MediaAccessState::Unavailable(detail),
                        ),
                    );
                    return;
                }
            }
        }
        Err(detail) => {
            for kind in [
                MediaAccessKind::Microphone,
                MediaAccessKind::Speaker,
                MediaAccessKind::Camera,
            ] {
                let _ = send_media_setup_event(
                    &event_tx,
                    &waker,
                    MediaSetupEvent::Access(kind, MediaAccessState::Unavailable(detail.clone())),
                );
            }
            return;
        }
    };
    let selection = selection_for_inventory(&inventory, &preferences);
    if !send_media_setup_event(&event_tx, &waker, MediaSetupEvent::Inventory(inventory)) {
        return;
    }
    if media_setup_stop_requested(&control_rx, &mut microphone_enabled, &mut camera_enabled) {
        return;
    }

    let mut mic_config = sinomo_audio::NativeAudioCaptureConfig::new(
        SETUP_AUDIO_SAMPLE_RATE_HZ,
        sinomo_audio::NativeAudioSampleFormat::Pcm16Le,
    );
    mic_config.device = selection.microphone;
    let mut microphone = if microphone_enabled {
        let open_config = mic_config.clone();
        open_media_setup_device(
            MediaAccessKind::Microphone,
            "microphone",
            &cancelled,
            Some(&microphone_requested),
            &event_tx,
            &waker,
            move || open_media_setup_microphone(open_config),
            || {},
        )
    } else {
        None
    };
    if cancelled.load(Ordering::Acquire) {
        return;
    }

    let mut speaker_config = sinomo_audio::NativeSpeakerPlaybackConfig::new(
        SETUP_AUDIO_SAMPLE_RATE_HZ,
        sinomo_audio::NativeAudioSampleFormat::Pcm16Le,
    );
    speaker_config.device = selection.speaker;
    let mut speaker = open_media_setup_device(
        MediaAccessKind::Speaker,
        "speaker",
        &cancelled,
        None,
        &event_tx,
        &waker,
        move || open_media_setup_speaker(speaker_config),
        || {
            release_setup_microphone_if_unrequested(
                &microphone_requested,
                &mut microphone,
                &event_tx,
                &snapshots,
                &waker,
            );
        },
    );
    if cancelled.load(Ordering::Acquire) {
        return;
    }

    let mut camera_config = sinomo_video::NativeVideoCaptureConfig {
        device: selection.camera,
        frame_interval_ms: SETUP_CAMERA_FRAME_INTERVAL_MS,
        ..Default::default()
    };
    let mut camera = if camera_enabled {
        let open_config = camera_config.clone();
        open_media_setup_device(
            MediaAccessKind::Camera,
            "camera",
            &cancelled,
            Some(&camera_requested),
            &event_tx,
            &waker,
            move || open_media_setup_camera(open_config),
            || {
                release_setup_microphone_if_unrequested(
                    &microphone_requested,
                    &mut microphone,
                    &event_tx,
                    &snapshots,
                    &waker,
                );
            },
        )
    } else {
        None
    };
    if cancelled.load(Ordering::Acquire) {
        return;
    }

    let mut speaker_test_deadline = None;
    let mut camera_frame_failures = 0_u8;
    loop {
        loop {
            match control_rx.try_recv() {
                Ok(MediaSetupControl::Stop) | Err(TryRecvError::Disconnected) => return,
                Ok(MediaSetupControl::TestSpeaker) => {
                    let mut speaker_failure = None;
                    let state = match speaker.as_ref() {
                        Some(speaker) => {
                            let tone = sinomo_audio::speaker_test_tone(
                                SETUP_AUDIO_SAMPLE_RATE_HZ,
                                sinomo_audio::NativeAudioSampleFormat::Pcm16Le,
                                SPEAKER_TEST_DURATION,
                            );
                            match speaker.write(&tone) {
                                Ok(()) => {
                                    speaker_test_deadline =
                                        Some(Instant::now() + SPEAKER_TEST_DURATION);
                                    SpeakerTestState::Testing
                                }
                                Err(error) => {
                                    speaker_test_deadline = None;
                                    let detail = error.to_string();
                                    speaker_failure = Some(detail.clone());
                                    SpeakerTestState::Failed(detail)
                                }
                            }
                        }
                        None => SpeakerTestState::Failed("speaker is unavailable".to_string()),
                    };
                    let _ = send_media_setup_event(
                        &event_tx,
                        &waker,
                        MediaSetupEvent::SpeakerTest(state),
                    );
                    if let Some(detail) = speaker_failure {
                        let _ = send_media_setup_event(
                            &event_tx,
                            &waker,
                            MediaSetupEvent::Access(
                                MediaAccessKind::Speaker,
                                media_access_failure(detail),
                            ),
                        );
                        speaker = None;
                    }
                }
                Ok(MediaSetupControl::SetMicrophoneEnabled(enabled)) => {
                    microphone_enabled = enabled;
                    if enabled {
                        if microphone.is_none() {
                            let open_config = mic_config.clone();
                            microphone = open_media_setup_device(
                                MediaAccessKind::Microphone,
                                "microphone",
                                &cancelled,
                                Some(&microphone_requested),
                                &event_tx,
                                &waker,
                                move || open_media_setup_microphone(open_config),
                                || {
                                    release_setup_camera_if_unrequested(
                                        &camera_requested,
                                        &mut camera,
                                        &event_tx,
                                        &snapshots,
                                        &camera_surface,
                                        &waker,
                                    );
                                },
                            );
                            if cancelled.load(Ordering::Acquire) {
                                return;
                            }
                        }
                    } else {
                        microphone = None;
                        publish_media_setup_snapshot(&snapshots, &waker, |snapshots| {
                            snapshots.microphone_level = Some(0.0);
                        });
                        let _ = send_media_setup_event(
                            &event_tx,
                            &waker,
                            MediaSetupEvent::Access(
                                MediaAccessKind::Microphone,
                                MediaAccessState::Idle,
                            ),
                        );
                    }
                }
                Ok(MediaSetupControl::SetCameraEnabled(enabled)) => {
                    camera_enabled = enabled;
                    camera_frame_failures = 0;
                    if enabled {
                        if camera.is_none() {
                            let inventory_node = node_id.clone();
                            let inventory = match await_media_setup_operation(
                                "camera-device-inventory",
                                &cancelled,
                                Some(&camera_requested),
                                move || enumerate_native_devices(inventory_node),
                                || {
                                    release_setup_microphone_if_unrequested(
                                        &microphone_requested,
                                        &mut microphone,
                                        &event_tx,
                                        &snapshots,
                                        &waker,
                                    );
                                },
                            ) {
                                Ok(Some(inventory)) => inventory,
                                Ok(None) if cancelled.load(Ordering::Acquire) => return,
                                Ok(None) => {
                                    let _ = send_media_setup_event(
                                        &event_tx,
                                        &waker,
                                        MediaSetupEvent::Access(
                                            MediaAccessKind::Camera,
                                            MediaAccessState::Idle,
                                        ),
                                    );
                                    continue;
                                }
                                Err(detail) => {
                                    let _ = send_media_setup_event(
                                        &event_tx,
                                        &waker,
                                        MediaSetupEvent::Access(
                                            MediaAccessKind::Camera,
                                            media_access_failure(detail),
                                        ),
                                    );
                                    continue;
                                }
                            };
                            let refreshed_selection =
                                selection_for_inventory(&inventory, &preferences);
                            camera_config.device = refreshed_selection.camera;
                            if !send_media_setup_event(
                                &event_tx,
                                &waker,
                                MediaSetupEvent::Inventory(inventory),
                            ) {
                                return;
                            }
                            let open_config = camera_config.clone();
                            camera = open_media_setup_device(
                                MediaAccessKind::Camera,
                                "camera",
                                &cancelled,
                                Some(&camera_requested),
                                &event_tx,
                                &waker,
                                move || open_media_setup_camera(open_config),
                                || {
                                    release_setup_microphone_if_unrequested(
                                        &microphone_requested,
                                        &mut microphone,
                                        &event_tx,
                                        &snapshots,
                                        &waker,
                                    );
                                },
                            );
                            if cancelled.load(Ordering::Acquire) {
                                return;
                            }
                        }
                    } else {
                        camera = None;
                        let revision = camera_surface.clear();
                        publish_media_setup_snapshot(&snapshots, &waker, |snapshots| {
                            snapshots.camera_surface_revision = Some(revision);
                        });
                        let _ = send_media_setup_event(
                            &event_tx,
                            &waker,
                            MediaSetupEvent::Access(
                                MediaAccessKind::Camera,
                                MediaAccessState::Idle,
                            ),
                        );
                    }
                }
                Err(TryRecvError::Empty) => break,
            }
        }

        let mut microphone_failed = false;
        if microphone_enabled && let Some(capture) = microphone.as_ref() {
            match capture.recv_timeout(Duration::ZERO) {
                Ok(Some(chunk)) => {
                    let level = sinomo_audio::normalized_audio_meter_level(
                        &chunk.audio,
                        chunk.sample_format,
                    );
                    publish_media_setup_snapshot(&snapshots, &waker, |snapshots| {
                        snapshots.microphone_level = Some(level);
                    });
                }
                Ok(None) => {}
                Err(error) => {
                    microphone_failed = true;
                    publish_media_setup_snapshot(&snapshots, &waker, |snapshots| {
                        snapshots.microphone_level = Some(0.0);
                    });
                    let _ = send_media_setup_event(
                        &event_tx,
                        &waker,
                        MediaSetupEvent::Access(
                            MediaAccessKind::Microphone,
                            media_access_failure(error.to_string()),
                        ),
                    );
                }
            }
        }
        if microphone_failed {
            microphone = None;
        }

        let mut camera_failed = false;
        if camera_enabled && let Some(capture) = camera.as_ref() {
            match capture.recv_sample_timeout(Duration::ZERO) {
                Ok(Some(sample)) => match camera_surface.publish_sample(&sample) {
                    Ok(frame) => {
                        camera_frame_failures = 0;
                        publish_media_setup_snapshot(&snapshots, &waker, |snapshots| {
                            snapshots.camera_surface_revision = Some(frame.revision);
                        });
                    }
                    Err(error) => {
                        if camera_frame_failure_is_terminal(&mut camera_frame_failures) {
                            camera_failed = true;
                            let revision = camera_surface.clear();
                            publish_media_setup_snapshot(&snapshots, &waker, |snapshots| {
                                snapshots.camera_surface_revision = Some(revision);
                            });
                            let _ = send_media_setup_event(
                                &event_tx,
                                &waker,
                                MediaSetupEvent::Access(
                                    MediaAccessKind::Camera,
                                    MediaAccessState::Unavailable(format!(
                                        "camera produced {camera_frame_failures} invalid frames: {error}"
                                    )),
                                ),
                            );
                        }
                    }
                },
                Ok(None) => {}
                Err(error) => {
                    camera_failed = true;
                    let revision = camera_surface.clear();
                    publish_media_setup_snapshot(&snapshots, &waker, |snapshots| {
                        snapshots.camera_surface_revision = Some(revision);
                    });
                    let _ = send_media_setup_event(
                        &event_tx,
                        &waker,
                        MediaSetupEvent::Access(
                            MediaAccessKind::Camera,
                            media_access_failure(error.to_string()),
                        ),
                    );
                }
            }
        }
        if camera_failed {
            camera = None;
        }

        if speaker_test_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            speaker_test_deadline = None;
            let mut speaker_failure = None;
            let state = match speaker.as_ref() {
                Some(speaker) => match speaker.check_health() {
                    Ok(()) => SpeakerTestState::Passed,
                    Err(error) => {
                        let detail = error.to_string();
                        speaker_failure = Some(detail.clone());
                        let _ = send_media_setup_event(
                            &event_tx,
                            &waker,
                            MediaSetupEvent::Access(
                                MediaAccessKind::Speaker,
                                media_access_failure(detail.clone()),
                            ),
                        );
                        SpeakerTestState::Failed(detail)
                    }
                },
                None => SpeakerTestState::Failed("speaker is unavailable".to_string()),
            };
            let _ = send_media_setup_event(&event_tx, &waker, MediaSetupEvent::SpeakerTest(state));
            if speaker_failure.is_some() {
                speaker = None;
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn media_setup_stop_requested(
    control_rx: &Receiver<MediaSetupControl>,
    microphone_enabled: &mut bool,
    camera_enabled: &mut bool,
) -> bool {
    loop {
        match control_rx.try_recv() {
            Ok(MediaSetupControl::Stop) | Err(TryRecvError::Disconnected) => return true,
            Ok(MediaSetupControl::TestSpeaker) => {}
            Ok(MediaSetupControl::SetMicrophoneEnabled(enabled)) => {
                *microphone_enabled = enabled;
            }
            Ok(MediaSetupControl::SetCameraEnabled(enabled)) => {
                *camera_enabled = enabled;
            }
            Err(TryRecvError::Empty) => return false,
        }
    }
}

#[cfg(not(test))]
fn send_media_setup_event(
    event_tx: &Sender<MediaSetupEvent>,
    waker: &Option<sui::UiHandle>,
    event: MediaSetupEvent,
) -> bool {
    let sent = event_tx.send(event).is_ok();
    if sent && let Some(waker) = waker {
        waker.wake();
    }
    sent
}

#[cfg(not(test))]
fn publish_media_setup_snapshot(
    snapshots: &Arc<Mutex<MediaSetupSnapshots>>,
    waker: &Option<sui::UiHandle>,
    update: impl FnOnce(&mut MediaSetupSnapshots),
) {
    if let Ok(mut snapshots) = snapshots.lock() {
        update(&mut snapshots);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

fn media_access_failure(detail: String) -> MediaAccessState {
    match NativeDeviceFailureKind::classify_detail(&detail) {
        NativeDeviceFailureKind::PermissionDenied => MediaAccessState::PermissionDenied(detail),
        NativeDeviceFailureKind::Busy => MediaAccessState::Busy(detail),
        NativeDeviceFailureKind::Unsupported => MediaAccessState::Unsupported(detail),
        NativeDeviceFailureKind::NotFound | NativeDeviceFailureKind::Other => {
            MediaAccessState::Unavailable(detail)
        }
    }
}

#[cfg(target_os = "windows")]
fn open_media_privacy_settings() -> Result<(), String> {
    std::process::Command::new("explorer.exe")
        .arg("ms-settings:privacy")
        .spawn()
        .map(|_| ())
        .map_err(|error| format!("open Windows privacy settings: {error}"))
}

#[cfg(target_os = "macos")]
fn open_media_privacy_settings() -> Result<(), String> {
    std::process::Command::new("open")
        .arg("x-apple.systempreferences:com.apple.preference.security?Privacy")
        .spawn()
        .map(|_| ())
        .map_err(|error| format!("open macOS privacy settings: {error}"))
}

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
fn open_media_privacy_settings() -> Result<(), String> {
    Err(
        "Open your system privacy settings, allow microphone/camera access, then retry."
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::device::{DeviceProfile, NativeDeviceBackend, NativeDeviceDescriptor};
    use std::sync::mpsc;

    fn descriptor(
        node_id: &NodeId,
        id: &str,
        label: &str,
        kind: DeviceKind,
        is_default: bool,
    ) -> NativeDeviceDescriptor {
        NativeDeviceDescriptor {
            native_id: id.to_string(),
            backend: NativeDeviceBackend::Cpal,
            profile: DeviceProfile::new(node_id.clone(), label, kind),
            is_default,
            detail: None,
        }
    }

    fn temp_state(slug: &str) -> MediaSetupState {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let data_dir = Arc::new(SinomoDataDir::new(std::env::temp_dir().join(format!(
            "sinomo-media-setup-{slug}-{}-{unique}",
            std::process::id()
        ))));
        MediaSetupState::new(
            data_dir,
            NodeId::from_legacy_named("node_media_setup").expect("node"),
            None,
        )
    }

    #[test]
    fn media_setup_camera_uses_the_responsive_native_preview_cadence() {
        assert_eq!(SETUP_CAMERA_FRAME_INTERVAL_MS, 33);
        assert_eq!(
            SETUP_CAMERA_FRAME_INTERVAL_MS,
            sinomo_video::NativeVideoCaptureConfig::default().frame_interval_ms
        );
    }

    #[test]
    fn inventory_groups_devices_and_prefers_explicit_system_default() {
        let mut state = temp_state("inventory");
        let node_id = state.node_id.clone();
        state.apply_inventory(NativeDeviceInventory {
            node_id: node_id.clone(),
            devices: vec![
                descriptor(
                    &node_id,
                    "cpal:input:1:usb",
                    "USB microphone",
                    DeviceKind::Microphone,
                    true,
                ),
                descriptor(
                    &node_id,
                    "cpal:output:0:speaker",
                    "Speakers",
                    DeviceKind::Speaker,
                    true,
                ),
                descriptor(&node_id, "camera:0", "Camera", DeviceKind::Camera, true),
            ],
            issues: Vec::new(),
        });

        assert_eq!(state.microphones[0].label(), "System default");
        assert_eq!(state.microphones[1].label(), "USB microphone (default)");
        assert_eq!(state.speakers.len(), 2);
        assert_eq!(state.cameras.len(), 2);
        assert_eq!(state.selection(), MediaDeviceSelection::default());
    }

    #[test]
    fn remembered_id_and_unique_display_name_restore_selection() {
        let options = vec![
            MediaDeviceOption::system_default(),
            MediaDeviceOption {
                native_id: Some("new-id".to_string()),
                display_name: "Desk mic".to_string(),
                is_default: false,
            },
        ];
        assert_eq!(
            remembered_index(
                &options,
                Some(&RememberedNativeDevice::new("new-id", "Desk mic"))
            ),
            1
        );
        assert_eq!(
            remembered_index(
                &options,
                Some(&RememberedNativeDevice::new("stale-id", "Desk mic"))
            ),
            1
        );
    }

    #[test]
    fn setup_probe_resolves_preferences_against_fresh_inventory() {
        let state = temp_state("probe-selection");
        let node_id = state.node_id.clone();
        let inventory = NativeDeviceInventory {
            node_id: node_id.clone(),
            devices: vec![
                descriptor(
                    &node_id,
                    "mic-new-id",
                    "Desk mic",
                    DeviceKind::Microphone,
                    false,
                ),
                descriptor(
                    &node_id,
                    "speaker-id",
                    "Speakers",
                    DeviceKind::Speaker,
                    true,
                ),
            ],
            issues: Vec::new(),
        };
        let preferences = AppMediaPreferences {
            microphone: Some(RememberedNativeDevice::new("mic-stale-id", "Desk mic")),
            speaker: Some(RememberedNativeDevice::new(
                "missing-speaker",
                "Missing speaker",
            )),
            camera: None,
        };

        assert_eq!(
            selection_for_inventory(&inventory, &preferences),
            MediaDeviceSelection {
                microphone: Some("mic-new-id".to_string()),
                speaker: None,
                camera: None,
            }
        );
    }

    #[test]
    fn camera_reenable_refresh_restores_remembered_selection() {
        let mut state = temp_state("camera-reenable-selection");
        let node_id = state.node_id.clone();
        let audio_only = NativeDeviceInventory {
            node_id: node_id.clone(),
            devices: vec![descriptor(
                &node_id,
                "mic-id",
                "Desk mic",
                DeviceKind::Microphone,
                true,
            )],
            issues: Vec::new(),
        };
        let refreshed = NativeDeviceInventory {
            node_id: node_id.clone(),
            devices: vec![descriptor(
                &node_id,
                "camera-remembered",
                "Desk camera",
                DeviceKind::Camera,
                false,
            )],
            issues: Vec::new(),
        };
        let preferences = AppMediaPreferences {
            camera: Some(RememberedNativeDevice::new(
                "camera-remembered",
                "Desk camera",
            )),
            ..Default::default()
        };
        state.preferences = preferences.clone();
        state.camera_enabled = false;
        state.apply_inventory(audio_only.clone());

        assert_eq!(
            selection_for_inventory(&audio_only, &preferences).camera,
            None
        );
        assert!(
            state
                .inventory_issues
                .iter()
                .all(|issue| !issue.contains("saved camera"))
        );
        state.camera_enabled = true;
        state.apply_inventory(refreshed.clone());
        assert_eq!(
            selection_for_inventory(&refreshed, &preferences)
                .camera
                .as_deref(),
            Some("camera-remembered")
        );
        assert_eq!(
            state.selection().camera.as_deref(),
            Some("camera-remembered")
        );
    }

    #[test]
    fn selecting_device_persists_native_id_and_default_clears_it() {
        let mut state = temp_state("persist");
        state.microphones.push(MediaDeviceOption {
            native_id: Some("mic-id".to_string()),
            display_name: "Desk mic".to_string(),
            is_default: false,
        });

        state.select_microphone(1);
        assert_eq!(
            load_media_preferences(&state.data_dir)
                .microphone
                .as_ref()
                .map(|device| device.native_id.as_str()),
            Some("mic-id")
        );

        state.select_microphone(0);
        assert_eq!(load_media_preferences(&state.data_dir).microphone, None);
    }

    #[test]
    fn access_failures_distinguish_permission_busy_and_unsupported() {
        assert!(matches!(
            media_access_failure("access denied by privacy settings".to_string()),
            MediaAccessState::PermissionDenied(_)
        ));
        assert!(matches!(
            media_access_failure("device is already in use".to_string()),
            MediaAccessState::Busy(_)
        ));
        assert!(matches!(
            media_access_failure("camera backend unsupported".to_string()),
            MediaAccessState::Unsupported(_)
        ));
    }

    #[test]
    fn privacy_settings_recovery_is_offered_only_for_permission_denials() {
        let mut state = temp_state("permission-recovery");
        state.microphone_access = MediaAccessState::Busy("in use".to_string());
        assert!(!state.permission_settings_available());

        state.camera_access =
            MediaAccessState::PermissionDenied("camera access denied".to_string());
        assert!(state.permission_settings_available());
    }

    #[test]
    fn access_check_completes_only_after_inventory_and_probes_settle() {
        let mut state = temp_state("access-complete");
        state.inventory_loading = true;
        state.microphone_access = MediaAccessState::Checking;
        assert!(!state.access_check_complete());

        state.inventory_loading = false;
        state.microphone_access = MediaAccessState::Ready;
        state.speaker_access = MediaAccessState::Unavailable("no output".to_string());
        state.camera_access = MediaAccessState::PermissionDenied("allow camera".to_string());
        assert!(state.access_check_complete());
    }

    #[test]
    fn logical_setup_lifecycle_is_visible_without_native_test_workers() {
        let mut state = temp_state("logical-lifecycle");

        state.start();
        assert!(state.is_running());
        state.stop();
        assert!(!state.is_running());
    }

    #[test]
    fn continuous_camera_stream_tolerates_isolated_bad_frames() {
        let mut failures = 0;
        assert!(!camera_frame_failure_is_terminal(&mut failures));
        assert!(!camera_frame_failure_is_terminal(&mut failures));
        assert!(camera_frame_failure_is_terminal(&mut failures));

        failures = 0;
        assert!(!camera_frame_failure_is_terminal(&mut failures));
    }

    #[test]
    fn speaker_test_failure_is_included_in_recovery_detail() {
        let mut state = temp_state("speaker-failure");
        state.speaker_access = MediaAccessState::Ready;
        state.apply_event(MediaSetupEvent::SpeakerTest(SpeakerTestState::Failed(
            "output stream stopped".to_string(),
        )));

        assert!(
            state
                .recovery_detail()
                .is_some_and(|detail| detail.contains("Speaker test: output stream stopped"))
        );
        assert!(!matches!(state.speaker_access, MediaAccessState::Ready));
    }

    #[test]
    fn latest_levels_and_surface_revisions_replace_previous_snapshots() {
        let mut state = temp_state("latest");
        let mut snapshots = MediaSetupSnapshots {
            microphone_level: Some(0.25),
            ..MediaSetupSnapshots::default()
        };
        snapshots.microphone_level = Some(0.75);
        let first = state
            .camera_surface
            .publish(sinomo_video::NativeVideoSurfaceFrame {
                revision: 0,
                sequence: 1,
                width: 1,
                height: 1,
                timestamp_ns: 0,
                duration_ns: 33_333_333,
                captured_at_unix_ms: 1,
                rgba: Arc::<[u8]>::from([0, 0, 0, 255]),
            });
        snapshots.camera_surface_revision = Some(first.revision);
        let second = state
            .camera_surface
            .publish(sinomo_video::NativeVideoSurfaceFrame {
                revision: 0,
                sequence: 2,
                width: 1,
                height: 1,
                timestamp_ns: 33_333_333,
                duration_ns: 33_333_333,
                captured_at_unix_ms: 34,
                rgba: Arc::<[u8]>::from([255, 255, 255, 255]),
            });
        snapshots.camera_surface_revision = Some(second.revision);

        let snapshots = snapshots.take();
        state.microphone_level = snapshots.microphone_level.expect("latest level");

        assert_eq!(state.microphone_level, 0.75);
        assert_eq!(
            state.camera_surface.latest().map(|frame| frame.sequence),
            Some(2)
        );
        assert_eq!(snapshots.camera_surface_revision, Some(second.revision));

        let mut snapshots = MediaSetupSnapshots {
            microphone_level: Some(0.0),
            camera_surface_revision: Some(state.camera_surface.clear()),
        };
        let taken = snapshots.take();
        assert_eq!(taken.microphone_level, Some(0.0));
        assert!(taken.camera_surface_revision.is_some());
        assert!(state.camera_surface.latest().is_none());
        assert!(snapshots.is_empty());
    }

    #[test]
    fn queued_preflight_controls_update_capture_intent_before_devices_open() {
        let (control_tx, control_rx) = mpsc::channel();
        control_tx
            .send(MediaSetupControl::SetMicrophoneEnabled(false))
            .expect("queue microphone mute");
        control_tx
            .send(MediaSetupControl::SetCameraEnabled(false))
            .expect("queue camera pause");
        let mut microphone_enabled = true;
        let mut camera_enabled = true;

        assert!(!media_setup_stop_requested(
            &control_rx,
            &mut microphone_enabled,
            &mut camera_enabled,
        ));
        assert!(!microphone_enabled);
        assert!(!camera_enabled);

        control_tx
            .send(MediaSetupControl::Stop)
            .expect("queue setup stop");
        assert!(media_setup_stop_requested(
            &control_rx,
            &mut microphone_enabled,
            &mut camera_enabled,
        ));
    }
}
